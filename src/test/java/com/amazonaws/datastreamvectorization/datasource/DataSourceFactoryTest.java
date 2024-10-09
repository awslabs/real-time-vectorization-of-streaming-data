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
package com.amazonaws.datastreamvectorization.datasource;

import com.amazonaws.datastreamvectorization.datasource.model.MskAuthType;
import com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration;
import com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration.MskDataSourceConfigurationBuilder;
import com.amazonaws.datastreamvectorization.datasource.model.StartingOffset;
import com.amazonaws.datastreamvectorization.datasource.model.StreamDataType;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_BOOTSTRAP_SERVERS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_TOPIC_NAMES;
import static com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration.TOPIC_PATTERN_INCLUDE_ALL;
import static com.amazonaws.datastreamvectorization.datasource.model.StartingOffset.COMMITTED;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;

class DataSourceFactoryTest {

    private static final String TEST_BOOTSTRAP_SERVER = "bootstrap.server:1234";
    private static final String TEST_TOPIC_NAME_1 = "sample-topic-1";
    private static final String TEST_TOPIC_NAME_2 = "sample-topic-1";
    private static final String TEST_TOPIC_NAME_3 = "sample-topic-1";
    private static final String TEST_GROUP_ID = "group-id-1";
    private static final String TEST_PROPS_KEY_1 = "props-key-1";
    private static final String TEST_PROPS_VALUE_1 = "props-value-1";
    private static final String TEST_PROPS_KEY_2 = "props-key-2";
    private static final String TEST_PROPS_VALUE_2 = "props-value-2";
    private static final StartingOffset TEST_STARTING_OFFSET = COMMITTED;

    @Test
    public void testMskStringDataSourceFactoryWithMinimalConfigs() {
        MskDataSourceConfiguration configuration = getBasicMskDataSourceConfigurationBuilder().build();
        Source response = DataSourceFactory.getDataSource(configuration);

        Assertions.assertNotNull(response);
        assertThat(response).isInstanceOf(KafkaSource.class);
        KafkaSource<String> actualKafkaSource = (KafkaSource<String>) response;
        assertEquals(new SimpleStringSchema().getProducedType(), actualKafkaSource.getProducedType());
    }

    @Test
    public void testMskStringDataSourceFactoryWithAllConfigs() {
        MskDataSourceConfiguration configuration = getBasicMskDataSourceConfigurationBuilder()
                .groupId(TEST_GROUP_ID)
                .startingOffset(TEST_STARTING_OFFSET)
                .kafkaProperties(getKafkaProperties())
                .authType(MskAuthType.IAM)
                .build();
        Source response = DataSourceFactory.getDataSource(configuration);

        Assertions.assertNotNull(response);
        assertThat(response).isInstanceOf(KafkaSource.class);
        KafkaSource<String> actualKafkaSource = (KafkaSource<String>) response;
        assertEquals(new SimpleStringSchema().getProducedType(), actualKafkaSource.getProducedType());
    }

    @Test
    public void testMskStringDataSourceFactoryWithAllConfigsAndTopicPattern() {
        Properties kafkaProperties = getKafkaProperties();
        kafkaProperties.put(PROPERTY_BOOTSTRAP_SERVERS, TEST_BOOTSTRAP_SERVER);

        MskDataSourceConfiguration configuration = MskDataSourceConfiguration.parseFrom(kafkaProperties)
                .topicPattern(TOPIC_PATTERN_INCLUDE_ALL)
                .groupId(TEST_GROUP_ID)
                .startingOffset(TEST_STARTING_OFFSET)
                .kafkaProperties(getKafkaProperties())
                .authType(MskAuthType.IAM)
                .build();
        Source response = DataSourceFactory.getDataSource(configuration);

        Assertions.assertNotNull(response);
        assertThat(response).isInstanceOf(KafkaSource.class);
        KafkaSource<String> actualKafkaSource = (KafkaSource<String>) response;
        assertEquals(new SimpleStringSchema().getProducedType(), actualKafkaSource.getProducedType());
    }

    @Test
    public void testMskJsonDataSourceFactoryWithMinimalConfigs() {
        MskDataSourceConfiguration configuration = getBasicMskDataSourceConfigurationBuilder()
                .streamDataType(StreamDataType.JSON)
                .build();
        Source response = DataSourceFactory.getDataSource(configuration);

        Assertions.assertNotNull(response);
        assertThat(response).isInstanceOf(KafkaSource.class);
        KafkaSource<JSONObject> actualKafkaSource = (KafkaSource<JSONObject>) response;
        assertEquals(new JSONObjectDeserializationSchema().getProducedType(), actualKafkaSource.getProducedType());
    }

    @Test
    public void testMskJsonDataSourceFactoryWithAllConfigs() {
        MskDataSourceConfiguration configuration = getBasicMskDataSourceConfigurationBuilder()
                .streamDataType(StreamDataType.JSON)
                .groupId(TEST_GROUP_ID)
                .startingOffset(TEST_STARTING_OFFSET)
                .kafkaProperties(getKafkaProperties())
                .authType(MskAuthType.IAM)
                .build();
        Source response = DataSourceFactory.getDataSource(configuration);

        Assertions.assertNotNull(response);
        assertThat(response).isInstanceOf(KafkaSource.class);
        KafkaSource<JSONObject> actualKafkaSource = (KafkaSource<JSONObject>) response;
        assertEquals(new JSONObjectDeserializationSchema().getProducedType(), actualKafkaSource.getProducedType());
    }

    private MskDataSourceConfigurationBuilder getBasicMskDataSourceConfigurationBuilder() {
        Properties kafkaProperties = getKafkaProperties();
        kafkaProperties.put(PROPERTY_BOOTSTRAP_SERVERS, TEST_BOOTSTRAP_SERVER);
        kafkaProperties.put(PROPERTY_TOPIC_NAMES, TEST_TOPIC_NAME_1 + "," + TEST_TOPIC_NAME_2 + "," + TEST_TOPIC_NAME_3);
        return MskDataSourceConfiguration.parseFrom(kafkaProperties);
    }

    private Properties getKafkaProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(TEST_PROPS_KEY_1, TEST_PROPS_VALUE_1);
        kafkaProperties.put(TEST_PROPS_KEY_2, TEST_PROPS_VALUE_2);
        return kafkaProperties;
    }
}
