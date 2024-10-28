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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_CHARSET;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_CONFIG;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_ID;
import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.getEmbeddingModelOverrides;
import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.getPropertiesMap;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;

/**
 * Class containing the embedding model and client provided embedding configs.
 */
@Slf4j
@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@Builder(builderMethodName = "internalBuilder")
public class EmbeddingConfiguration implements Serializable {
    public static final int DEFAULT_EMBEDDING_ASYNC_TIMEOUT = 15000;
    public static final TimeUnit DEFAULT_EMBEDDING_ASYNC_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
    public static final int DEFAULT_EMBEDDING_ASYNC_MAX_IO = 1000;
    public static final String DEFAULT_EMBEDDING_CHARSET = "UTF-8";
    public static final ChunkingType DEFAULT_EMBEDDING_CHUNKING_TYPE = ChunkingType.SPLIT_BY_WORD;
    public static final int DEFAULT_EMBEDDING_CHUNKING_MAX_OVERLAP_SIZE = 0;

    @NonNull
    private final EmbeddingModel embeddingModel;
    @Setter
    private Map<String, Object> embeddingModelOverrideConfig;
    private Map<String, Object> embeddingInputConfig;
    /*
    TODO: Change String to Charset type. Context:
    java.nio.charset.Charset object was giving errors in serialization when calling RichAsyncFunction through
    Flink DataStreams API. For now, we have kept this String type but need to dig more to move it to Charset
     */
    private String charset;

    /**
     * Constructor to use when we have a model ID instead of {@link EmbeddingModel}
     *
     * @param embeddingModelId     model ID for embedding model
     * @param embeddingModelOverrideConfig client provided configs for the embedding model
     * @param charset              input stream charset
     */
    public EmbeddingConfiguration(@NonNull final String embeddingModelId,
                                  @NonNull final Map<String, Object> embeddingModelOverrideConfig,
                                  @NonNull final String charset) {
        this.embeddingModel = getEmbeddingModel(embeddingModelId);
        this.embeddingModelOverrideConfig = embeddingModelOverrideConfig;
        this.charset = charset;
    }

    /**
     * Constructor to use when we have a model ID instead of {@link EmbeddingModel}
     *
     * @param embeddingModelId     model ID for embedding model
     * @param embeddingModelOverrideConfig client provided configs for the embedding model
     */
    public EmbeddingConfiguration(@NonNull final String embeddingModelId,
                                  @NonNull final Map<String, Object> embeddingModelOverrideConfig) {
        this.embeddingModel = getEmbeddingModel(embeddingModelId);
        this.embeddingModelOverrideConfig = embeddingModelOverrideConfig;
        this.charset = DEFAULT_EMBEDDING_CHARSET;
    }

    public static EmbeddingConfigurationBuilder parseFrom(@NonNull final Properties properties) {
        EmbeddingModel embeddingModel = getEmbeddingModel(properties.getProperty(PROPERTY_EMBEDDING_MODEL_ID));
        return internalBuilder()
                .embeddingModel(embeddingModel)
                .charset(properties.getProperty(PROPERTY_EMBEDDING_CHARSET, DEFAULT_EMBEDDING_CHARSET))
                .embeddingModelOverrideConfig(getEmbeddingModelOverrides(embeddingModel, properties))
                .embeddingInputConfig(getPropertiesMap(properties, PROPERTY_EMBEDDING_INPUT_CONFIG));
    }

    /**
     * Method to validate Embedding model.
     */
    public void validate() {
        if (isEmpty(this.embeddingModel)) {
            throw new MissingOrIncorrectConfigurationException("Embedding model is required.");
        }
        if (isEmpty(this.embeddingModel.getModelId())) {
            throw new MissingOrIncorrectConfigurationException("Embedding model ID is required.");
        }
        // check to avoid overriding default charset to null or empty string.
        if (isEmpty(this.charset)) {
            throw new MissingOrIncorrectConfigurationException("Input stream Charset is required.");
        }
        if (!Charset.isSupported(this.charset)) {
            throw new MissingOrIncorrectConfigurationException("Input stream Charset is not supported.");
        }
    }

    private static EmbeddingModel getEmbeddingModel(final String modelId) {
        for (EmbeddingModel model : EmbeddingModel.values()) {
            if (model.getModelId().equals(modelId)) {
                return model;
            }
        }
        throw new UnsupportedEmbeddingModelException(modelId + " not supported. Use a valid embedding model.");
    }
}
