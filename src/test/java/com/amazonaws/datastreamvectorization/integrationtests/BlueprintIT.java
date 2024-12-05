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

import com.amazonaws.services.kafka.AWSKafka;
import com.amazonaws.services.kafka.AWSKafkaClientBuilder;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersRequest;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersResult;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.json.JSONObject;

class BlueprintIT {

    @Test
    void testPrototype() {
        System.out.println("MADE IT TO BlueprintIT testGetConfig()");
        long currentTimestamp = System.currentTimeMillis();
        System.out.println("CURRENT TIMESTAMP: " + currentTimestamp);

        // TODO: prototype reading in test inputs JSON file
        System.out.println("AT STEP: prototype reading in test inputs JSON file");
        String testInputFileName = System.getProperty("integTestInputsFileName");
        JSONObject testInputJson;
        try {
            InputStream is = new FileInputStream(testInputFileName);
            String jsonTxt = IOUtils.toString(is, StandardCharsets.UTF_8);
            testInputJson = new JSONObject(jsonTxt);
        } catch (IOException e) {
            throw new RuntimeException("Could not read test input file " + testInputFileName, e);
        }

        // TODO: prototype get MSK cluster info
        System.out.println("AT STEP: prototype get MSK cluster info");
        JSONObject mskServerlessPrivateVPC = (JSONObject) testInputJson.get("MSKServerlessPrivateVPC");
        String mskClusterArn = (String) mskServerlessPrivateVPC.get("MSKClusterArn");
        AWSKafka mskClient = AWSKafkaClientBuilder.defaultClient();
        GetBootstrapBrokersRequest request = new GetBootstrapBrokersRequest();
        GetBootstrapBrokersResult bookstrapBrokersResult = mskClient.getBootstrapBrokers(request.withClusterArn(mskClusterArn));
        System.out.println("MSK cluster bootstrap server result: " + bookstrapBrokersResult);
        String mskClusterBootstrapBrokerString = bookstrapBrokersResult.getBootstrapBrokerString();
        System.out.println("MSK cluster bootstrap server string: " + mskClusterBootstrapBrokerString);

        // TODO: prototype creating a topic on the MSK cluster
        System.out.println("AT STEP: prototype creating a topic on the MSK cluster");
        String mskTestTopicName = "nexus-integ-test-topic-" + currentTimestamp;
        KafkaClients kafkaClients = new KafkaClients(mskClusterBootstrapBrokerString);
        AdminClient adminClient = kafkaClients.createKafkaAdminClient();
        adminClient.createTopics(List.of(new NewTopic(mskTestTopicName, 3, (short) 3)));

        // TODO: prototype creating an index in the OpenSearch cluster

        // TODO: prototype deploying blueprint stack

        // TODO: prototype adding blueprint IAM role as OpenSearch master user

        // TODO: prototype updating MSF app config

        // TODO: prototype starting MSF app

        // TODO: prototype producing to the MSK cluster

        // TODO: prototype checking OpenSearch records

        // TODO: prototype stopping MSF app

        // TODO: prototype deleting VPC endpoints

        // TODO: prototype deleting stack

        // TODO: prototype deleting created topic from the MSK cluster
        System.out.println("AT STEP: prototype deleting created topic from the MSK cluster");
        adminClient.deleteTopics(List.of(mskTestTopicName));
        adminClient.close();

    }
}
