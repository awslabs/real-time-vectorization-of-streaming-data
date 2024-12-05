package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.datastreamvectorization.integrationtests.model.IntegTestCaseInput;
import com.amazonaws.datastreamvectorization.integrationtests.model.MskClusterConfig;
import com.amazonaws.datastreamvectorization.integrationtests.model.OpenSearchClusterConfig;
import com.amazonaws.services.cloudformation.model.Stack;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.opensearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.SINK_ORIGINAL_DATA_NAME;
import static com.amazonaws.datastreamvectorization.integrationtests.constants.IntegTestConstants.BLUEPRINT_CDK_TEMPLATE_URL;
import static com.amazonaws.datastreamvectorization.integrationtests.constants.IntegTestConstants.BlueprintParameterKeys.PARAM_APP_NAME;
import static com.amazonaws.datastreamvectorization.integrationtests.constants.IntegTestConstants.BlueprintParameterKeys.PARAM_ROLE_NAME;

/**
 * Integration Test Base class that contains steps for running one integration test case.
 */
@Slf4j
public class IntegTestBase {
    private final static int MAX_QUERY_OPENSEARCH_RETRIES = 5;
    private final static Long QUERY_OPENSEARCH_DELAY = 60000L; // 1 minute

    // TODO: test case needs to do certain cleanup even if test fails in the middle
    //  For example, clean up MSK topic and OpenSearch index if stack deployment fails
    public void runTestCase(IntegTestCaseInput testCaseInput) {
        String testID = this.generateTestID(testCaseInput.getTestName());
        MskClusterConfig mskClusterConfig = testCaseInput.getMskCluster();
        OpenSearchClusterConfig osClusterConfig = testCaseInput.getOpenSearchCluster();

        // create MSK clients
        MSKHelper mskHelper = new MSKHelper();
        String mskClusterBootstrapBrokerString = mskHelper.getBootstrapBrokers(mskClusterConfig.getArn());
        KafkaClients kafkaClients = new KafkaClients(mskClusterBootstrapBrokerString);
        AdminClient adminClient = kafkaClients.createKafkaAdminClient(testID);

        // create OpenSearch clients
        OpenSearchHelper osHelper = new OpenSearchHelper();
        OpenSearchRestClient osRestClient = new OpenSearchRestClient();

        // create other service clients
        CloudFormationHelper cfnHelper = new CloudFormationHelper();
        MSFHelper msfHelper = new MSFHelper();
        BedrockHelper bedrockHelper = new BedrockHelper();

        // create new MSK topic for test
        String mskTestTopicName = mskHelper.buildTestTopicName(testID);
        adminClient.createTopics(List.of(new NewTopic(mskTestTopicName, 3, (short) 3)));

        // create new OpenSearch index for test
        EmbeddingModel embeddingModel = bedrockHelper.getSupportedEmbeddingModel();
        String osTestIndexName = osHelper.buildTestVectorIndexName(testID);
        osRestClient.createVectorIndex(
                osClusterConfig.getEndpointUrl(),
                osClusterConfig.getOpenSearchClusterType(),
                osTestIndexName,
                embeddingModel);

        // deploy the blueprint stack
        String blueprintCDKTemplateURL = System.getProperty(BLUEPRINT_CDK_TEMPLATE_URL);
        Stack blueprintStack = cfnHelper.createBlueprintStack(
                blueprintCDKTemplateURL,
                testCaseInput.getMskCluster(),
                testCaseInput.getOpenSearchCluster(),
                testID);

        // add blueprint stack IAM role as master user to the OpenSearch cluster
        if (osClusterConfig.getOpenSearchClusterType().equals(OpenSearchType.PROVISIONED)) {
            String iamRoleName = cfnHelper.getParameterValue(blueprintStack, PARAM_ROLE_NAME);
            osHelper.addMasterUserIAMRole(osClusterConfig.getName(), iamRoleName);
        }

        // update MSF app config
        String msfAppName = cfnHelper.getParameterValue(blueprintStack, PARAM_APP_NAME);
        msfHelper.updateMSFAppDefault(msfAppName);

        // start the MSF app
        msfHelper.startMSFApp(msfAppName);

        // produce to the MSK cluster
        List<String> testRecords = new ArrayList<>();
        testRecords.add(testID + " integ-test-record-1");
        testRecords.add(testID + " integ-test-record-2");
        testRecords.add(testID + " integ-test-record-3");
        testRecords.add(testID + " integ-test-record-4");
        KafkaProducer<String, String> kafkaProducer = kafkaClients.createKafkaStringProducer(testID);
        List<ProducerRecord<String, String>> mskRecords = testRecords
                .stream()
                .map(record -> new ProducerRecord<String, String>(mskTestTopicName, record))
                .collect(Collectors.toList());
        for (ProducerRecord<String, String> record : mskRecords) {
            kafkaProducer.send(record);
        }

        // query for OpenSearch records
        validateOpenSearchRecords(osRestClient, osClusterConfig, osTestIndexName, testRecords);

        // stop MSF app
        msfHelper.stopMSFApp(msfAppName, true);

        // at step deleting stack (and deleting VPC endpoints)
        cfnHelper.deleteBlueprintStack(blueprintStack.getStackName());

        // delete the MSK test topic
        adminClient.deleteTopics(List.of(mskTestTopicName));
        adminClient.close();

        // delete the OpenSearch test index
        osRestClient.deleteIndex(
                osClusterConfig.getEndpointUrl(),
                osClusterConfig.getOpenSearchClusterType(),
                osTestIndexName);
    }

    /**
     * Generate a test ID string for a single test case run.
     *
     * @param testName Name of the integration test case
     * @return Test ID string
     */
    private String generateTestID(String testName) {
        String currentTimestamp = Long.toString(System.currentTimeMillis());
        return String.join("-", testName, currentTimestamp);
    }

    /**
     * Validate that the queried OpenSearch records match the original data that was produced to the MSK cluster.
     *
     * @param osRestClient OpenSearch REST client to use to query
     * @param osClusterConfig OpenSearch cluster config
     * @param osTestIndexName OpenSearch index to query from
     * @param expectedOriginalDataList The list of original data that was produced to the MSK cluster
     */
    private void validateOpenSearchRecords(OpenSearchRestClient osRestClient, 
                                           OpenSearchClusterConfig osClusterConfig, 
                                           String osTestIndexName,
                                           List<String> expectedOriginalDataList) {
        int retryCount = 0;
        SearchHit[] hits = {};
        try {
            // query OpenSearch records in a retry loop until number of expected records is found or timeout is reached
            while (retryCount++ <= MAX_QUERY_OPENSEARCH_RETRIES) {
                hits = osRestClient.queryIndexRecords(
                        osClusterConfig.getEndpointUrl(),
                        osClusterConfig.getOpenSearchClusterType(),
                        osTestIndexName);
                if (hits.length >= expectedOriginalDataList.size()) {
                    break;
                }
                Thread.sleep(QUERY_OPENSEARCH_DELAY);
            }
            // assert records are as expected
            Assertions.assertEquals(expectedOriginalDataList.size(), hits.length);
            List<String> resultOriginalDataList = Arrays.stream(hits)
                    .map(hit -> {
                        Map<String, Object> sourceMap = hit.getSourceAsMap();
                        Assertions.assertTrue(sourceMap.containsKey(SINK_ORIGINAL_DATA_NAME));
                        return sourceMap.get(SINK_ORIGINAL_DATA_NAME).toString();
                    })
                    .sorted()
                    .collect(Collectors.toList());
            Collections.sort(expectedOriginalDataList);
            Assertions.assertEquals(expectedOriginalDataList, resultOriginalDataList);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred when querying and validating OpenSearch records for "
                    + osClusterConfig.getName(), e);
        }
    }
}
