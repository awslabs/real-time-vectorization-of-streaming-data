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

import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.datastreamvectorization.integrationtests.constants.IntegTestConstants.BLUEPRINT_CDK_TEMPLATE_URL;
import static com.amazonaws.datastreamvectorization.integrationtests.constants.IntegTestConstants.BlueprintParameterKeys.PARAM_APP_NAME;
import static com.amazonaws.datastreamvectorization.integrationtests.constants.IntegTestConstants.BlueprintParameterKeys.PARAM_ROLE_NAME;

/**
 * Integration Test Base class that contains steps for running one integration test case.
 */
@Slf4j
public class IntegTestBase {

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
        // TODO: wait and validate that MSF app is running
        //  for now, just sleeping
        try {
            Thread.sleep(300000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // produce to the MSK cluster
        List<String> testRecords = List.of(
                testID + " integ-test-value-1",
                testID + " integ-test-value-2",
                testID + " integ-test-value-3"
        );
        KafkaProducer<String, String> kafkaProducer = kafkaClients.createKafkaStringProducer(testID);
        List<ProducerRecord<String, String>> mskRecords = testRecords
                .stream()
                .map(record -> new ProducerRecord<String, String>(mskTestTopicName, record))
                .collect(Collectors.toList());
        for (ProducerRecord<String, String> record : mskRecords) {
            kafkaProducer.send(record);
        }

        // query for OpenSearch records
        // TODO: wait and validate in a loop that OpenSearch records exist
        //  for now, just sleeping
        try {
            Thread.sleep(120000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        osRestClient.queryIndexRecords(
                osClusterConfig.getEndpointUrl(),
                osClusterConfig.getOpenSearchClusterType(),
                osTestIndexName);

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
}
