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
package com.amazonaws.datastreamvectorization.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


class DataSourceValidationUtilsTest {

    @ParameterizedTest
    @ValueSource(strings = {"", "  ", "urn:some:uri:1.2.3", "telnet://aws.jam", "https:boot.amazon.com", "https::amazon.com",
            "ftp://amazon.com"})
    void validBootstrapServers_ShouldReturnFalse(String input) {
        assertFalse(DataSourceValidationUtils.validBootstrapServers(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"boot-rdjnphld.c1.kafka-serverless.us-east-1.amazonaws.com:9098",
            "b-1.provionedmskcluster.n5ksvt.c5.kafka.us-east-1.amazonaws.com:9092,b-2.provionedmskcluster.n5ksvt.c5.kafka.us-east-1.amazonaws.com:9092"
    })
    void validBootstrapServers_ShouldReturnTrue(String input) {
        assertTrue(DataSourceValidationUtils.validBootstrapServers(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"group*", "group@id", "group id", "group+id"})
    void isValidKafkaConsumerGroupId_InvalidName(String groupId) {
        assertFalse(DataSourceValidationUtils.isValidKafkaConsumerGroupId(groupId));
    }

    @ParameterizedTest
    @ValueSource(strings = {"groupid", "group-id", "GROUP_ID", "group.123"})
    void isValidOpenSearchIndexName_ValidName(String groupId) {
        assertTrue(DataSourceValidationUtils.isValidKafkaConsumerGroupId(groupId));
    }

    @ParameterizedTest
    @ValueSource(strings = {"topic*", "topicN@me", "topic name", "topic+name"})
    void isValidKafkaTopic_InvalidName(String topicName) {
        assertFalse(DataSourceValidationUtils.isValidKafkaTopic(topicName));
    }

    @ParameterizedTest
    @ValueSource(strings = {"topic", "topic-name", "TOPIC_NAME", "topic.123"})
    void isValidKafkaTopic_ValidName(String topicName) {
        assertTrue(DataSourceValidationUtils.isValidKafkaTopic(topicName));
    }


}
