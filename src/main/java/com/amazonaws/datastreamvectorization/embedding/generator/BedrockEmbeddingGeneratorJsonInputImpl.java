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
package com.amazonaws.datastreamvectorization.embedding.generator;

import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_INPUT_TEXT_KEY_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_JSON_FIELDS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.PROPERTY_VALUES_DELIMITER;

/**
 * This class generates embeddings for JSON input type using provided Bedrock model
 * and returns a JSONObject containing embeddings.
 */
@Slf4j
public class BedrockEmbeddingGeneratorJsonInputImpl extends BedrockEmbeddingGenerator<JSONObject, JSONObject> {

    public BedrockEmbeddingGeneratorJsonInputImpl(@NonNull final String region,
                                                  @NonNull final EmbeddingConfiguration config) {
        super(region, config);
    }

    /**
     * Asynchronously invoke the embedding model and complete the ResultFuture with the result.
     * @param jsonInput
     * @param resultFuture
     */
    @Override
    public void asyncInvoke(@NonNull final JSONObject jsonInput, @NonNull final ResultFuture<JSONObject> resultFuture) {
        long startTime = System.currentTimeMillis();
        CompletableFuture<JSONObject> embeddingFuture = CompletableFuture.supplyAsync(() -> {
            JSONObject jsonToEmbed = prepareForEmbedding(jsonInput);
            String input = jsonToEmbed.toString();
            log.info("Embedding input: {}", input);
            Optional<JSONObject> responseOptional = getEmbeddingJSON(input);
            if (responseOptional.isEmpty()) {
                log.warn("No embedding generated for input: [{}]", input);
                return new JSONObject();
            }
            // TODO: See if we need to add additional fields to the response
            /*
             Add the JSON input object provided for embedding back to the response.
             This means, if only selected fields were requested to be embedded, only those fields will be present
             in the response. If no specific fields were specified, the entire original JSON input is returned.
             */
            JSONObject response = responseOptional.get();
            response.put(EMBED_INPUT_TEXT_KEY_NAME, jsonToEmbed.toString());
            reportLatencyMetric(startTime);
            return response;
        });

        embeddingFuture.thenAccept((JSONObject result) -> resultFuture.complete(Collections.singleton(result)))
                .exceptionally(throwable -> {
                    /*
                    fail fast gracefully to prevent stale/out of order messages to be embedded.
                    TODO: consider moving the failed messages to a DLQ instead of just logging the errors.
                     */
                    resultFuture.completeExceptionally(throwable.getCause());
                    return null;
                });
    }

    private JSONObject prepareForEmbedding(final JSONObject inputJsonObject) {
        if (shouldFilterJsonFields()) {
            Map<String, Object> embeddingInputConfig = embeddingConfiguration.getEmbeddingInputConfig();
            String fieldsToEmbed = embeddingInputConfig.get(PROPERTY_EMBEDDING_INPUT_JSON_FIELDS).toString();
            List<String> fieldsToEmbedList =  Arrays.stream(fieldsToEmbed.trim().split(PROPERTY_VALUES_DELIMITER))
                    // remove extra spaces and empty strings
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            log.info("Filtering JSON input to only embed the following: {}", fieldsToEmbedList);
            JSONObject output = new JSONObject();

            for (String field : fieldsToEmbedList) {
                output.put(field, inputJsonObject.get(field));
            }
            log.info("Transformed JSON object to embed: {}", output);
            return output;
        }
        // If there is nothing to remove from the JSON, return the original input
        log.info("No fields to filter for embedding. Returning original input.");
        return inputJsonObject;
    }

    private boolean shouldFilterJsonFields() {
        return embeddingConfiguration.getEmbeddingInputConfig() != null
                && embeddingConfiguration.getEmbeddingInputConfig()
                .containsKey(PROPERTY_EMBEDDING_INPUT_JSON_FIELDS)
                && !embeddingConfiguration.getEmbeddingInputConfig().get(PROPERTY_EMBEDDING_INPUT_JSON_FIELDS)
                .equals("*"); // indicates including all fields
    }
}
