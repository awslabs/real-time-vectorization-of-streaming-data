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

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.json.JSONArray;

import java.util.Collections;
import java.util.Map;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.DIMENSIONS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.EMBEDDING_TYPES;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.INPUT_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.INPUT_TYPE_SEARCH_DOCUMENT;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.NORMALIZE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.OUTPUT_EMBEDDING_LENGTH;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.TRUNCATE;

/**
 * Enum containing list of embedding models and their corresponding model IDs and default configurations.
 * <a href="https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-embed.html">Refer Link</a>
 */
@Getter
@AllArgsConstructor
public enum EmbeddingModel {

    AMAZON_TITAN_TEXT_G1("amazon.titan-embed-text-v1", "inputText", String.class, "embedding",
            Collections.emptyMap(), Collections.emptyMap(), 50000, 16000, 1536),
    AMAZON_TITAN_TEXT_V2("amazon.titan-embed-text-v2:0", "inputText", String.class, "embedding",
            Collections.emptyMap(),
            Map.of(NORMALIZE, Boolean.class,
                    DIMENSIONS, Integer.class
    ), 50000, 16000, 1024),
    AMAZON_TITAN_MULTIMODAL_G1("amazon.titan-embed-image-v1", "inputText", String.class, "embedding",
            Collections.emptyMap(), Map.of(
            OUTPUT_EMBEDDING_LENGTH, Integer.class
    ), 100000, 50000, 1024),
    /*
    For Cohere models, we add a default input_type since this is a required field. In search use-cases,
    search_document is used when you encode documents for embeddings that you store in a vector database.
    See: https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-embed.html
    */
    COHERE_EMBED_ENGLISH("cohere.embed-english-v3", "texts", JSONArray.class, "embeddings",
            Map.of(INPUT_TYPE, INPUT_TYPE_SEARCH_DOCUMENT), Map.of(
            INPUT_TYPE, String.class,
            TRUNCATE, String.class,
            EMBEDDING_TYPES, String.class
    ), 2048, 1000, 1024),
    COHERE_EMBED_MULTILINGUAL("cohere.embed-multilingual-v3", "texts", JSONArray.class, "embeddings",
            Map.of(INPUT_TYPE, INPUT_TYPE_SEARCH_DOCUMENT), Map.of(
            INPUT_TYPE, String.class,
            TRUNCATE, String.class,
            EMBEDDING_TYPES, String.class
    ), 2048, 1000, 1024);

    /**
     * Bedrock model ID.
     */
    private final String modelId;

    /**
     * Bedrock model input text key for request body and its type
     */
    private final String inputKey;
    private final Class inputType;

    /**
     * Bedrock model embedding text key for response body
     */
    private final String embeddingKey;

    /**
     * Map of default configurations associated with the model.
     */
    private final Map<String, String> defaultConfigs;

    /**
     * Map of configuration keys and their expected data types that are supported by the model.
     * @see EmbeddingConfiguration
     */
    private final Map<String, Class> configurationKeyDataTypeMap;

    /**
     * Bedrock model max character limit of the text to embed in a request.
     */
    private final int modelMaxCharacterLimit;

    /**
     * Bedrock model default character limit of the text to embed in a request.
     */
    private final int modelDefaultCharacterLimit;

    /**
     * Default output dimension for the model
     */
    private final int modelDefaultDimensions;
}
