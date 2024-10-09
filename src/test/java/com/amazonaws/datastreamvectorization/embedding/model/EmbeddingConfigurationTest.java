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
package com.amazonaws.datastreamvectorization.embedding.model;

import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import com.amazonaws.datastreamvectorization.exceptions.UnsupportedEmbeddingModelException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.DIMENSIONS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.NORMALIZE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.OUTPUT_EMBEDDING_LENGTH;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_CHARSET;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_CONFIG;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_JSON_FIELDS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_ID;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_OVERRIDES;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel.AMAZON_TITAN_MULTIMODAL_G1;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel.AMAZON_TITAN_TEXT_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(Parameterized.class)
class EmbeddingConfigurationTest {

    private static Stream<Arguments> provideModelIds() {
        return Stream.of(
                Arguments.of("amazon.titan-embed-text-v1", EmbeddingModel.AMAZON_TITAN_TEXT_G1),
                Arguments.of("amazon.titan-embed-text-v2:0", AMAZON_TITAN_TEXT_V2),
                Arguments.of("amazon.titan-embed-image-v1", AMAZON_TITAN_MULTIMODAL_G1),
                Arguments.of("cohere.embed-english-v3", EmbeddingModel.COHERE_EMBED_ENGLISH),
                Arguments.of("cohere.embed-multilingual-v3", EmbeddingModel.COHERE_EMBED_MULTILINGUAL)
        );
    }

    @ParameterizedTest
    @MethodSource("provideModelIds")
    public void testSupportedModels(String modelId, EmbeddingModel expectedModel) {
        EmbeddingConfiguration config = new EmbeddingConfiguration(modelId, Collections.EMPTY_MAP);
        Assert.assertEquals(expectedModel, config.getEmbeddingModel());
    }

    @Test
    public void testUnsupportedModelsThrowException() {
        Assert.assertThrows(UnsupportedEmbeddingModelException.class, () ->
                new EmbeddingConfiguration("unsupportedModel", Collections.EMPTY_MAP));
    }

    @Test
    public void testParseFrom_validConfigs() {
       Properties properties =ParameterTool.fromMap(
                Map.of(PROPERTY_EMBEDDING_MODEL_ID, AMAZON_TITAN_TEXT_V2.getModelId(),
                        PROPERTY_EMBEDDING_CHARSET, StandardCharsets.US_ASCII.name(),
                        PROPERTY_EMBEDDING_MODEL_OVERRIDES + NORMALIZE, "true",
                        PROPERTY_EMBEDDING_MODEL_OVERRIDES + DIMENSIONS, "128"
                )
        ).getProperties();
        EmbeddingConfiguration expectedConfig = EmbeddingConfiguration.internalBuilder()
                .embeddingModel(AMAZON_TITAN_TEXT_V2)
                .charset(StandardCharsets.US_ASCII.name())
                .embeddingModelOverrideConfig(Map.of(NORMALIZE, true, DIMENSIONS, 128))
                .embeddingInputConfig(Collections.emptyMap())
                .build();
        EmbeddingConfiguration actualConfig = EmbeddingConfiguration.parseFrom(properties).build();
        Assert.assertEquals(expectedConfig.getEmbeddingModel(), actualConfig.getEmbeddingModel());
        Assert.assertEquals(expectedConfig.getCharset(), actualConfig.getCharset());
        Assert.assertEquals(expectedConfig.getEmbeddingModelOverrideConfig(), actualConfig.getEmbeddingModelOverrideConfig());
        Assert.assertEquals(expectedConfig.getEmbeddingInputConfig(), actualConfig.getEmbeddingInputConfig());
    }

   @Test
    public void testParseFrom_invalidConfig_throwException() {
       Properties properties =ParameterTool.fromMap(
                Map.of(PROPERTY_EMBEDDING_MODEL_ID, AMAZON_TITAN_MULTIMODAL_G1.getModelId(),
                        PROPERTY_EMBEDDING_CHARSET, StandardCharsets.US_ASCII.name(),
                        PROPERTY_EMBEDDING_MODEL_OVERRIDES + OUTPUT_EMBEDDING_LENGTH, "12"
                )
        ).getProperties();
        EmbeddingConfiguration expectedConfig = EmbeddingConfiguration.internalBuilder()
                .embeddingModel(AMAZON_TITAN_MULTIMODAL_G1)
                .charset(StandardCharsets.US_ASCII.name())
                .embeddingModelOverrideConfig(Collections.emptyMap())
                .embeddingInputConfig(Collections.emptyMap())
                .build();

       Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class,
               () -> EmbeddingConfiguration.parseFrom(properties).build());
       assertEquals("Invalid value 12 found for configuration " + OUTPUT_EMBEDDING_LENGTH
               + ". Please refer documentation for allowed values.", exception.getMessage());
    }

    @Test
    public void testConfigs() {
        Map<String, Object> expectedConfigMap = Map.of(
                "key1", "value1",
                "key2", 20.000,
                "key3", true);
        EmbeddingConfiguration config = new EmbeddingConfiguration("amazon.titan-embed-text-v1",
                expectedConfigMap, "UTF-8");
        Assert.assertEquals(EmbeddingModel.AMAZON_TITAN_TEXT_G1, config.getEmbeddingModel());
        Assert.assertEquals(expectedConfigMap, config.getEmbeddingModelOverrideConfig());
    }

    private static Stream<Arguments> provideInvalidConfigurations() {
        return Stream.of(
                Arguments.of(EmbeddingConfiguration.parseFrom(ParameterTool.fromMap(
                                        Map.of(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_G1.getModelId()
                                                , PROPERTY_EMBEDDING_CHARSET, "")
                                ).getProperties())
                                .build(),
                        "Input stream Charset is required."
                ),

                Arguments.of(EmbeddingConfiguration.parseFrom(ParameterTool.fromMap(
                                        Map.of(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_G1.getModelId()
                                                , PROPERTY_EMBEDDING_CHARSET, "xyz")
                                ).getProperties())
                                .build(),
                        "Input stream Charset is not supported."
                )
        );
    }

    @ParameterizedTest
    @MethodSource("provideInvalidConfigurations")
    public void testInvalidConfigs(EmbeddingConfiguration config, String expectedExceptionMessage) {
        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class, config::validate);
        assertEquals(expectedExceptionMessage, exception.getMessage());
    }

    private static Stream<Arguments> provideValidConfigurations() {
        return Stream.of(
                Arguments.of(EmbeddingConfiguration.parseFrom(ParameterTool.fromMap(
                                Map.of(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_G1.getModelId()
                                )).getProperties())
                        .build()
                ),
                Arguments.of(EmbeddingConfiguration.parseFrom(ParameterTool.fromMap(
                                        Map.of(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_G1.getModelId(),
                                                PROPERTY_EMBEDDING_CHARSET, StandardCharsets.US_ASCII.name())
                                ).getProperties())
                                .build()
                ),
                Arguments.of(EmbeddingConfiguration.parseFrom(ParameterTool.fromMap(
                                        Map.of(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_G1.getModelId(),
                                                PROPERTY_EMBEDDING_CHARSET, StandardCharsets.US_ASCII.name(),
                                                PROPERTY_EMBEDDING_MODEL_OVERRIDES, "some_value")
                                ).getProperties())
                                .build()
                ),
                Arguments.of(EmbeddingConfiguration.parseFrom(ParameterTool.fromMap(
                                        Map.of(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_G1.getModelId(),
                                                PROPERTY_EMBEDDING_CHARSET, StandardCharsets.US_ASCII.name(),
                                                PROPERTY_EMBEDDING_MODEL_OVERRIDES, "some_value",
                                                PROPERTY_EMBEDDING_INPUT_CONFIG + PROPERTY_EMBEDDING_INPUT_JSON_FIELDS,
                                                "[\"some\", \"fields\", \"to\", \"embed\",,]"
                                                )
                                ).getProperties())
                                .build()
                ),
                Arguments.of(new EmbeddingConfiguration("amazon.titan-embed-text-v1",
                        Map.of("key1", "value1"))
                ),
                Arguments.of(new EmbeddingConfiguration("amazon.titan-embed-text-v1",
                        Map.of("key1", "value1"), StandardCharsets.US_ASCII.name())
                )
        );
    }

    @ParameterizedTest
    @MethodSource("provideValidConfigurations")
    public void testValidConfigs(EmbeddingConfiguration config) {
        config.validate();
    }
}
