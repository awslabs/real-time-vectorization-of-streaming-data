/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.services.cloudformation.model.CreateStackResult;
import com.amazonaws.services.cloudformation.model.DeleteStackResult;
import com.amazonaws.services.cloudformation.model.Stack;
import com.amazonaws.services.kafka.AWSKafka;
import com.amazonaws.services.kafka.AWSKafkaClientBuilder;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersRequest;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersResult;

import com.amazonaws.services.kinesisanalyticsv2.model.UpdateApplicationResult;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import org.json.JSONObject;

class BlueprintIT {

    @Test
    void testPrototype() {
        System.out.println("MADE IT TO BlueprintIT testGetConfig()");
        String currentTimestamp = Long.toString(System.currentTimeMillis());
        System.out.println("CURRENT TIMESTAMP: " + currentTimestamp);

        // TODO: prototype reading in test inputs JSON file
        System.out.println("AT STEP: prototype reading in test inputs JSON file");
        String testInputFile = System.getProperty("integTestInputsFile");
        JSONObject testInputJson;
        try {
            InputStream is = new FileInputStream(testInputFile);
            String jsonTxt = IOUtils.toString(is, StandardCharsets.UTF_8);
            testInputJson = new JSONObject(jsonTxt);
        } catch (IOException e) {
            throw new RuntimeException("Could not read test input file " + testInputFile, e);
        }

        // TODO: prototype get MSK cluster info
        System.out.println("AT STEP: prototype get MSK cluster info");
        JSONObject mskServerlessPrivateVPC = (JSONObject) testInputJson.get("MSKServerlessPrivateVPC");
        String mskClusterArn = (String) mskServerlessPrivateVPC.get("MSKClusterArn");
        AWSKafka mskClient = AWSKafkaClientBuilder.defaultClient();
        GetBootstrapBrokersRequest bookstrapBrokersRequest = new GetBootstrapBrokersRequest();
        GetBootstrapBrokersResult bookstrapBrokersResult = mskClient.getBootstrapBrokers(bookstrapBrokersRequest.withClusterArn(mskClusterArn));
        System.out.println("MSK cluster bootstrap server result: " + bookstrapBrokersResult);
        String mskClusterBootstrapBrokerString = bookstrapBrokersResult.getBootstrapBrokerStringSaslIam();
        System.out.println("MSK cluster bootstrap server string: " + mskClusterBootstrapBrokerString);

        // TODO: prototype creating a topic on the MSK cluster
        System.out.println("AT STEP: prototype creating a topic on the MSK cluster");
        MSKHelper mskHelper = new MSKHelper(currentTimestamp);
        String mskTestTopicName = mskHelper.buildTestTopicName();
        KafkaClients kafkaClients = new KafkaClients(mskClusterBootstrapBrokerString);
        AdminClient adminClient = kafkaClients.createKafkaAdminClient(currentTimestamp);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(List.of(new NewTopic(mskTestTopicName, 3, (short) 3)));
        System.out.println(createTopicsResult);

//        // TODO: prototype get OpenSearch cluster info
//        System.out.println("AT STEP: prototype get OpenSearch cluster info");
//        JSONObject openSearchCluster = (JSONObject) testInputJson.get("OpenSearchCluster");
//        String openSearchClusterName = (String) openSearchCluster.get("Name");
//        String openSearchClusterType = (String) openSearchCluster.get("Type");
//        String openSearchClusterEndpointUrl = (String) openSearchCluster.get("EndpointUrl");
//        System.out.println("OpenSearch cluster endpoint URL: " + openSearchClusterEndpointUrl);
//        OpenSearchType openSearchType;
//        if (openSearchClusterType.equals("PROVISIONED")) {
//            openSearchType = OpenSearchType.PROVISIONED;
//        } else if (openSearchClusterType.equals("SERVERLESS")) {
//            openSearchType = OpenSearchType.SERVERLESS;
//        } else {
//            throw new RuntimeException("Unsupported OpenSearch cluster type " + openSearchClusterType);
//        }

//        // TODO: prototype creating an index in the OpenSearch cluster
//        System.out.println("AT STEP: prototype creating an index in the OpenSearch cluster");
//        OpenSearchRestClient osRestClient = new OpenSearchRestClient();
//
//        BedrockHelper bedrockHelper = new BedrockHelper();
//        EmbeddingModel embeddingModel = bedrockHelper.getSupportedEmbeddingModel();
//
//        String opensearchIndexName = "integ-os-index-" + currentTimestamp;
//
//        osRestClient.createIndex(
//                openSearchClusterEndpointUrl,
//                openSearchType,
//                opensearchIndexName,
//                embeddingModel);

        // TODO: prototype producing to the MSK cluster
        System.out.println("AT STEP: prototype producing to the MSK cluster");
        List<String> testRecords = List.of(
                currentTimestamp + " integ-test-value-1",
                currentTimestamp + " integ-test-value-2",
                currentTimestamp + " integ-test-value-3"
        );
        KafkaProducer<String, String> kafkaProducer = kafkaClients.createKafkaStringProducer(currentTimestamp);
        List<ProducerRecord<String, String>> mskRecords = testRecords
                .stream()
                .map(record -> new ProducerRecord<String, String>(mskTestTopicName, record))
                .collect(Collectors.toList());
        for (ProducerRecord<String, String> record : mskRecords) {
            kafkaProducer.send(record);
        }

//        // TODO: prototype deploying blueprint stack
//        System.out.println("AT STEP: prototype deploying blueprint stack");
//        String blueprintCDKTemplateURL = System.getProperty("blueprintCDKTemplateURL");
//        CloudFormationHelper cfnHelper = new CloudFormationHelper(currentTimestamp);
//        Stack blueprintStack = cfnHelper.createBlueprintStack(blueprintCDKTemplateURL, mskClusterArn, openSearchClusterName, openSearchType);
//        System.out.println("Stack creation succeeded: " + blueprintStack);
//
//        // TODO: prototype adding blueprint IAM role as OpenSearch master user
//        System.out.println("AT STEP: prototype adding blueprint IAM role as OpenSearch master user");
//        OpenSearchHelper osHelper = new OpenSearchHelper();
//        osHelper.addMasterUserIAMRole(openSearchClusterName, openSearchType, cfnHelper.buildStackRoleName());
//
//        // TODO: prototype updating MSF app config
//        System.out.println("AT STEP: prototype updating MSF app config");
//        MSFHelper msfHelper = new MSFHelper();
//        String msfAppName = cfnHelper.buildStackAppName();
//        msfHelper.updateMSFAppDefault(msfAppName);
//
//        // TODO: prototype starting MSF app
//        System.out.println("AT STEP: prototype starting MSF app");
//        msfHelper.startMSFApp(msfAppName);
//
//        // TODO: prototype producing to the MSK cluster
//        System.out.println("AT STEP: prototype producing to the MSK cluster");
//        List<String> testRecords = List.of(
//                currentTimestamp + " integ-test-value-1",
//                currentTimestamp + " integ-test-value-2",
//                currentTimestamp + " integ-test-value-3"
//        );
//        KafkaProducer<String, String> kafkaProducer = kafkaClients.createKafkaProducer(currentTimestamp);
//        List<ProducerRecord<String, String>> mskRecords = testRecords
//                .stream()
//                .map(record -> new ProducerRecord<String, String>(mskTestTopicName, record))
//                .collect(Collectors.toList());
//        for (ProducerRecord<String, String> record : mskRecords) {
//            kafkaProducer.send(record);
//        }
//
//        // TODO: prototype checking OpenSearch records
//        System.out.println("AT STEP: prototype checking OpenSearch records");
//        List<OpenSearchIndexDocument> searchResult = osRestClient.queryIndexRecords(openSearchType, opensearchIndexName, openSearchClusterEndpointUrl);
//
//        osRestClient.validateOpenSearchRecords(testRecords, searchResult);
//
//        // TODO: prototype stopping MSF app
//        System.out.println("AT STEP: prototype stopping MSF app");
//        msfHelper.stopMSFApp(msfAppName, true);
//
//        // TODO: prototype deleting stack (and deleting VPC endpoints)
//        System.out.println("AT STEP: prototype deleting stack (and deleting VPC endpoints)");
//        cfnHelper.deleteBlueprintStack(blueprintStack.getStackName());

        // TODO: prototype deleting created topic from the MSK cluster
        System.out.println("AT STEP: prototype deleting created topic from the MSK cluster");
        adminClient.deleteTopics(List.of(mskTestTopicName));
        adminClient.close();

//        // TODO: prototype deleting created index from the OpenSearch cluster
//        System.out.println("AT STEP: prototype deleting created index from the OpenSearch cluster");
//        osRestClient.deleteIndex(openSearchClusterEndpointUrl, openSearchType, opensearchIndexName);
    }
}
