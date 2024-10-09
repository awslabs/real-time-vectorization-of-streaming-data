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

import com.amazonaws.datastreamvectorization.datasink.model.DataSinkConfiguration;
import com.amazonaws.datastreamvectorization.datasink.model.DataSinkType;
import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchDataSinkConfiguration;
import com.amazonaws.datastreamvectorization.datasource.model.DataSourceConfiguration;
import com.amazonaws.datastreamvectorization.datasource.model.DataSourceType;
import com.amazonaws.datastreamvectorization.datasource.model.MskAuthType;
import com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration;
import com.amazonaws.datastreamvectorization.datasource.model.StartingOffset;
import com.amazonaws.datastreamvectorization.datasource.model.StreamDataType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import com.amazonaws.datastreamvectorization.exceptions.UnsupportedEmbeddingModelException;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.DIMENSIONS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.EMBEDDING_TYPES;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.INPUT_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.NORMALIZE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.OUTPUT_EMBEDDING_LENGTH;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.TRUNCATE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_BOOTSTRAP_SERVERS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_CHARSET;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_CONFIG;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_JSON_FIELDS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_ID;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_OVERRIDES;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_GROUP_ID;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_ENDPOINT;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_INDEX;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_SOURCE_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_STARTING_OFFSET;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_STREAM_DATA_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_TOPIC_NAMES;
import static com.amazonaws.datastreamvectorization.datasink.model.DataSinkConfiguration.PROPERTY_SINK_TYPE;
import static com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType.SERVERLESS;
import static com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration.DEFAULT_GROUP_ID;
import static com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration.DEFAULT_STARTING_OFFSET;
import static com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration.DEFAULT_STREAM_DATA_TYPE;
import static com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration.TOPIC_PATTERN_INCLUDE_ALL;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel.AMAZON_TITAN_MULTIMODAL_G1;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel.AMAZON_TITAN_TEXT_G1;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel.AMAZON_TITAN_TEXT_V2;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel.COHERE_EMBED_ENGLISH;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel.COHERE_EMBED_MULTILINGUAL;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PropertiesUtilsTest {

    private static final String TEST_SOURCE_TYPE = DataSourceType.MSK.name();
    private static final String TEST_SOURCE_BOOTSTRAP_SERVERS = "localhost:9098";
    private static final String TEST_EMBED_MODEL_ID = EmbeddingModel.AMAZON_TITAN_TEXT_G1.getModelId();
    private static final String TEST_SINK_TYPE = DataSinkType.OPENSEARCH.name();
    private static final String TEST_OS_TYPE = SERVERLESS.name();
    private static final String TEST_OS_DOMAIN = "https://some-domain.test-value.com";
    private static final String TEST_OS_CUSTOM_INDEX = "test-index";
    private static final String TEST_REGION = "us-east-1";


    @Test
    void getProperties() {
        Properties applicationProperties = new Properties();
        applicationProperties.put("a.b.c", "1");
        applicationProperties.put("a.b.d", "2");
        applicationProperties.put("a.e.f", "3");
        applicationProperties.put("g.h.i", "4");

        Properties properties = PropertiesUtils.getProperties(applicationProperties, "a.b.");

        assertEquals(2, properties.size());
        assertEquals("1", properties.get("c"));
        assertEquals("2", properties.get("d"));
    }

    @Test
    void getPropertiesMap() {
        Properties applicationProperties = new Properties();
        applicationProperties.put("a.b.c", "1");
        applicationProperties.put("a.b.d", "2");
        applicationProperties.put("a.e.f", "3");
        applicationProperties.put("g.h.i", "4");

        Map<String, Object> propertiesMap = PropertiesUtils.getPropertiesMap(applicationProperties, "a.b.");

        assertEquals(2, propertiesMap.size());
        assertEquals("1", propertiesMap.get("c"));
        assertEquals("2", propertiesMap.get("d"));
    }

    @Test
    void getPropertiesMapWhenEmpty() {
        Properties applicationProperties = new Properties();
        applicationProperties.put("a.b.c", "1");
        applicationProperties.put("a.b.d", "2");
        applicationProperties.put("a.e.f", "3");
        applicationProperties.put("g.h.i", "4");

        Map<String, Object> propertiesMap = PropertiesUtils.getPropertiesMap(applicationProperties, "x.y.");
        assertEquals(0, propertiesMap.size());
    }

    private static Stream<Arguments> provideModels() {
        return Stream.of(Arguments.of(AMAZON_TITAN_TEXT_G1), Arguments.of(AMAZON_TITAN_TEXT_V2),
                Arguments.of(AMAZON_TITAN_MULTIMODAL_G1), Arguments.of(COHERE_EMBED_ENGLISH),
                Arguments.of(COHERE_EMBED_MULTILINGUAL)
        );
    }

    @ParameterizedTest
    @MethodSource("provideModels")
    void getEmbeddingModelOverrides_validConfigs(EmbeddingModel embeddingModel) {
        Map<String, Object> outputMap = PropertiesUtils.getEmbeddingModelOverrides(embeddingModel,
                getValidProperties());
        assertEquals(embeddingModel.getConfigurationKeyDataTypeMap().size(), outputMap.size());
        embeddingModel.getConfigurationKeyDataTypeMap().forEach((key, expectedClass) -> {
            // validate value for the key
            assertEquals(getValidProperties().getProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + key),
                    outputMap.get(key).toString());
            // validate expected object type
            assertEquals(expectedClass, outputMap.get(key).getClass());
        });
    }

    private static Stream<Arguments> provideInvalidConfigs() {
        return Stream.of(Arguments.of(AMAZON_TITAN_TEXT_V2, DIMENSIONS, "abc",
                        DIMENSIONS + " must be a valid integer value."),
                Arguments.of(AMAZON_TITAN_TEXT_V2, DIMENSIONS, "-12",
                        DIMENSIONS + " must be a positive value."),
                Arguments.of(AMAZON_TITAN_TEXT_V2, DIMENSIONS, " ",
                        DIMENSIONS + " must be a non-empty positive integer value."),
                Arguments.of(AMAZON_TITAN_MULTIMODAL_G1, OUTPUT_EMBEDDING_LENGTH, "123",
                        "Invalid value 123 found for configuration " + OUTPUT_EMBEDDING_LENGTH
                                + ". Please refer documentation for allowed values."),
                Arguments.of(COHERE_EMBED_ENGLISH, INPUT_TYPE, "someType",
                        "Invalid value someType found for configuration " + INPUT_TYPE
                                + ". Please refer documentation for allowed values."),
                Arguments.of(COHERE_EMBED_ENGLISH, TRUNCATE, "truncate",
                        "Invalid value truncate found for configuration " + TRUNCATE
                                + ". Please refer documentation for allowed values."),
                Arguments.of(COHERE_EMBED_MULTILINGUAL, EMBEDDING_TYPES, "someEmbeddingType",
                        "Invalid value someEmbeddingType found for configuration " + EMBEDDING_TYPES
                                + ". Please refer documentation for allowed values.")
        );
    }

    @ParameterizedTest
    @MethodSource("provideInvalidConfigs")
    void getEmbeddingModelOverrides_configValidationFails(EmbeddingModel embeddingModel, String keyName,
                                                          String invalidConfigValue, String expectedExceptionMessage) {
        Properties applicationProperties = getValidProperties();
        applicationProperties.put(PROPERTY_EMBEDDING_MODEL_OVERRIDES + keyName, invalidConfigValue);

        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class,
                () -> PropertiesUtils.getEmbeddingModelOverrides(embeddingModel,
                applicationProperties));
        assertEquals(expectedExceptionMessage, exception.getMessage());

    }


    @Test
    public void testGetEmbeddingConfiguration() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_MULTIMODAL_G1.getModelId());
        inputProperties.put(PROPERTY_EMBEDDING_CHARSET, "UTF-8");
        inputProperties.put(PROPERTY_EMBEDDING_MODEL_OVERRIDES + OUTPUT_EMBEDDING_LENGTH, "1024");
        inputProperties.put(PROPERTY_EMBEDDING_INPUT_CONFIG + PROPERTY_EMBEDDING_INPUT_JSON_FIELDS, "description");

        EmbeddingConfiguration expectedConfig = EmbeddingConfiguration.internalBuilder()
                .embeddingModel(EmbeddingModel.AMAZON_TITAN_MULTIMODAL_G1)
                .charset("UTF-8")
                .embeddingModelOverrideConfig(Map.of(OUTPUT_EMBEDDING_LENGTH, 1024))
                .embeddingInputConfig(Map.of(PROPERTY_EMBEDDING_INPUT_JSON_FIELDS, "description"))
                .build();

        final EmbeddingConfiguration result = PropertiesUtils.getEmbeddingConfiguration(inputProperties);
        Assert.assertEquals(expectedConfig, result);
    }

    @Test
    public void testGetEmbeddingConfiguration_InvalidEmbeddingModel() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_EMBEDDING_MODEL_ID, "somemodel");
        inputProperties.put(PROPERTY_EMBEDDING_CHARSET, "UTF-8");
        inputProperties.put(PROPERTY_EMBEDDING_MODEL_OVERRIDES + OUTPUT_EMBEDDING_LENGTH, "1024");
        inputProperties.put(PROPERTY_EMBEDDING_INPUT_CONFIG + PROPERTY_EMBEDDING_INPUT_JSON_FIELDS, "description");

        Exception exception = assertThrows(UnsupportedEmbeddingModelException.class, () -> {
            PropertiesUtils.getEmbeddingConfiguration(inputProperties);
        });
        Assert.assertEquals("somemodel not supported. Use a valid embedding model.", exception.getMessage());
    }

    @Test
    public void testGetDataSinkConfiguration() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS, "10");

        OpenSearchDataSinkConfiguration expectedConfig = OpenSearchDataSinkConfiguration.internalBuilder()
                .region(TEST_REGION)
                .index(TEST_OS_CUSTOM_INDEX)
                .endpoint(TEST_OS_DOMAIN)
                .openSearchType(SERVERLESS)
                .bulkFlushIntervalMillis(10)
                .build();

        final DataSinkConfiguration result = PropertiesUtils.getDataSinkConfiguration(TEST_REGION, inputProperties);

        assertTrue(result instanceof OpenSearchDataSinkConfiguration);
        Assert.assertEquals(expectedConfig, result);
    }

    @Test
    public void testGetDataSinkConfiguration_UnsupportedSink() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_SINK_TYPE, "someSink");

        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class, () -> {
            PropertiesUtils.getDataSinkConfiguration(TEST_REGION, inputProperties);
        });
        Assert.assertEquals("someSink is not a valid value for DataSinkType enum", exception.getMessage());
    }

    @Test
    public void testGetDataSinkConfiguration_InvalidEndpoint() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_OS_ENDPOINT, "someEndpoint");

        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class, () -> {
            PropertiesUtils.getDataSinkConfiguration(TEST_REGION, inputProperties);
        });
        Assert.assertEquals("OpenSearch endpoint should be a valid url.", exception.getMessage());
    }

    @Test
    public void testGetDataSinkConfiguration_InvalidIndexName() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_OS_INDEX, "_invalid+index");

        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class, () -> {
            PropertiesUtils.getDataSinkConfiguration(TEST_REGION, inputProperties);
        });
        Assert.assertEquals("OpenSearch index name is invalid.", exception.getMessage());
    }

    @Test
    public void testGetDataSourceConfiguration_topicNameProvided() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_TOPIC_NAMES, "someTopic1, someTopic2, someTopic3");
        inputProperties.put(PROPERTY_GROUP_ID, "someGroup");
        inputProperties.put(PROPERTY_STREAM_DATA_TYPE, StreamDataType.STRING.name());
        inputProperties.put(PROPERTY_STARTING_OFFSET, StartingOffset.COMMITTED.name());

        MskDataSourceConfiguration expectedConfig = MskDataSourceConfiguration.internalBuilder()
                .topicNames(List.of("someTopic1", "someTopic2", "someTopic3"))
                .authType(MskAuthType.IAM)
                .groupId("someGroup")
                .streamDataType(StreamDataType.STRING)
                .startingOffset(StartingOffset.COMMITTED)
                .bootstrapServers(TEST_SOURCE_BOOTSTRAP_SERVERS)
                .kafkaProperties(new Properties())
                .build();

        final DataSourceConfiguration result = PropertiesUtils.getDataSourceConfiguration(inputProperties);

        assertTrue(result instanceof MskDataSourceConfiguration);
        Assert.assertEquals(expectedConfig, result);
    }

    @Test
    public void testGetDataSourceConfiguration_topicNameNotProvided() {
        Properties inputProperties = getValidProperties();
        inputProperties.remove(PROPERTY_TOPIC_NAMES);

        MskDataSourceConfiguration expectedConfig = MskDataSourceConfiguration.internalBuilder()
                .topicPattern(TOPIC_PATTERN_INCLUDE_ALL)
                .authType(MskAuthType.IAM)
                .groupId(DEFAULT_GROUP_ID)
                .streamDataType(DEFAULT_STREAM_DATA_TYPE)
                .startingOffset(DEFAULT_STARTING_OFFSET)
                .bootstrapServers(TEST_SOURCE_BOOTSTRAP_SERVERS)
                .kafkaProperties(new Properties())
                .build();

        final DataSourceConfiguration result = PropertiesUtils.getDataSourceConfiguration(inputProperties);

        assertTrue(result instanceof MskDataSourceConfiguration);
        Assert.assertEquals(expectedConfig, result);
    }

    @Test
    public void testGetDataSourceConfiguration_topicPatternProvided() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_TOPIC_NAMES, ".*");

        MskDataSourceConfiguration expectedConfig = MskDataSourceConfiguration.internalBuilder()
                .topicPattern(TOPIC_PATTERN_INCLUDE_ALL)
                .authType(MskAuthType.IAM)
                .groupId(DEFAULT_GROUP_ID)
                .streamDataType(DEFAULT_STREAM_DATA_TYPE)
                .startingOffset(DEFAULT_STARTING_OFFSET)
                .bootstrapServers(TEST_SOURCE_BOOTSTRAP_SERVERS)
                .kafkaProperties(new Properties())
                .build();

        final DataSourceConfiguration result = PropertiesUtils.getDataSourceConfiguration(inputProperties);

        assertTrue(result instanceof MskDataSourceConfiguration);
        Assert.assertEquals(expectedConfig, result);
    }

    @Test
    public void testGetDataSourceConfiguration_UnsupportedSourceType() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_SOURCE_TYPE, "someSource");

        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class, () -> {
            PropertiesUtils.getDataSourceConfiguration(inputProperties);
        });
        Assert.assertEquals("someSource is not a valid value for DataSourceType enum", exception.getMessage());
    }

    @Test
    public void testGetDataSourceConfiguration_InvalidBootstrapServer() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_BOOTSTRAP_SERVERS, "someServer");

        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class, () -> {
            PropertiesUtils.getDataSourceConfiguration(inputProperties);
        });
        Assert.assertEquals("MSK bootstrap servers not valid.", exception.getMessage());
    }

    @Test
    public void testGetDataSourceConfiguration_InvalidTpicName() {
        Properties inputProperties = getValidProperties();
        inputProperties.put(PROPERTY_TOPIC_NAMES, "topic*");

        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class, () -> {
            PropertiesUtils.getDataSourceConfiguration(inputProperties);
        });
        Assert.assertEquals("Topic name topic* is not valid. Valid characters for topics are the ASCII Alphanumeric "
                + "characters, ‘.’, ‘_’, and '-'.", exception.getMessage());
    }

    private Properties getValidProperties() {
        Properties applicationProperties = new Properties();
        applicationProperties.put(PROPERTY_SOURCE_TYPE, TEST_SOURCE_TYPE);
        applicationProperties.put(PROPERTY_BOOTSTRAP_SERVERS, TEST_SOURCE_BOOTSTRAP_SERVERS);
        applicationProperties.put(PROPERTY_EMBEDDING_MODEL_ID, TEST_EMBED_MODEL_ID);
        applicationProperties.put(PROPERTY_SINK_TYPE, TEST_SINK_TYPE);
        applicationProperties.put(PROPERTY_OS_ENDPOINT, TEST_OS_DOMAIN);
        applicationProperties.put(PROPERTY_OS_INDEX, TEST_OS_CUSTOM_INDEX);
        applicationProperties.put(PROPERTY_OS_TYPE, TEST_OS_TYPE);
        applicationProperties.put(PROPERTY_EMBEDDING_MODEL_OVERRIDES + NORMALIZE, "true");
        applicationProperties.put(PROPERTY_EMBEDDING_MODEL_OVERRIDES + DIMENSIONS, "128");
        applicationProperties.put(PROPERTY_EMBEDDING_MODEL_OVERRIDES + OUTPUT_EMBEDDING_LENGTH, "1024");
        applicationProperties.put(PROPERTY_EMBEDDING_MODEL_OVERRIDES + INPUT_TYPE, "classification");
        applicationProperties.put(PROPERTY_EMBEDDING_MODEL_OVERRIDES + TRUNCATE, "START");
        applicationProperties.put(PROPERTY_EMBEDDING_MODEL_OVERRIDES + EMBEDDING_TYPES, "uint8");
        return applicationProperties;
    }
}
