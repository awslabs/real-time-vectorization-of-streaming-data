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
package com.amazonaws.datastreamvectorization.embedding.preprocessor;

import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingJsonStrictness;
import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_JSON_FIELDS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_JSON_STRICTNESS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.PROPERTY_VALUES_DELIMITER;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingJsonStrictness.FAIL_FAST;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingJsonStrictness.MISSING_FIELDS_EMBED_ALL;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingJsonStrictness.MISSING_FIELDS_IGNORE;
import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.isPresent;

@Slf4j
@AllArgsConstructor
public class JSONPreprocessor extends RichAsyncFunction<JSONObject, JSONObject> {

    private static final EmbeddingJsonStrictness PROPERTY_STRICTNESS_DEFAULT = FAIL_FAST;
    final EmbeddingConfiguration embeddingConfiguration;

    @Override
    public void asyncInvoke(final JSONObject inputJsonObject, final ResultFuture<JSONObject> resultFuture) {
        CompletableFuture<JSONObject> embeddingFuture = CompletableFuture.supplyAsync(() ->
                prepareForEmbedding(inputJsonObject));

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

    @VisibleForTesting
    JSONObject prepareForEmbedding(final JSONObject inputJsonObject) {
        log.info("Received embedding config: {}", embeddingConfiguration);
        if (shouldFilterJsonFields(embeddingConfiguration)) {
            Map<String, Object> embeddingInputConfig = embeddingConfiguration.getEmbeddingInputConfig();
            String fieldsToEmbed = embeddingInputConfig.get(PROPERTY_EMBEDDING_INPUT_JSON_FIELDS).toString();
            List<String> fieldsToEmbedList =  Arrays.stream(fieldsToEmbed.trim().split(PROPERTY_VALUES_DELIMITER))
                    // remove extra spaces and empty strings
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            log.info("Filtering JSON input to only embed the following: {}", fieldsToEmbedList);
            JSONObject output = new JSONObject();
            EmbeddingJsonStrictness strictness = getStrictnessConfig(embeddingConfiguration);
            for (String field : fieldsToEmbedList) {
                log.info("Strictness: {} Checking for field {}", strictness, field);
                if (inputJsonObject.has(field)) {
                    log.debug("Strictness: {} Found field {} ", strictness, field);
                    output.put(field, inputJsonObject.get(field));
                } else {
                    log.debug("Strictness: {} field {} not found", strictness, field);
                    if (FAIL_FAST.equals(strictness)) {
                        throw new MissingOrIncorrectConfigurationException("Input JSON does not contain the "
                                + "field to be embedded: " + field + " strictness: " + strictness);
                    } else {
                        log.info("Input JSON does not contain the field to be embedded: {}. Ignoring field.", field);
                    }
                }
            }
            if (output.isEmpty()) {
                if (MISSING_FIELDS_EMBED_ALL.equals(strictness)) {
                    log.info("No field added to object. Returning original JSON object. Strictness: {}", strictness);
                    return inputJsonObject;
                }
                if (MISSING_FIELDS_IGNORE.equals(strictness)) {
                    log.info("No field added to object. Ignoring message. Strictness: {}", strictness);
                    return new JSONObject();
                }
            }
            log.info("Transformed JSON object to embed: {}", output);
            return output;
        }
        // If there is nothing to remove from the JSON, return the original input
        log.info("No fields to filter for embedding. Returning original input.");
        return inputJsonObject;
    }

    private static boolean shouldFilterJsonFields(final EmbeddingConfiguration embeddingConfiguration) {
        return isPresent(embeddingConfiguration.getEmbeddingInputConfig(), PROPERTY_EMBEDDING_INPUT_JSON_FIELDS)
                && !embeddingConfiguration.getEmbeddingInputConfig().get(PROPERTY_EMBEDDING_INPUT_JSON_FIELDS)
                .equals(".*"); // indicates including all fields
    }

    /**
     * Method that returns strictness level based on configuration. If not specified, default is FAIL_FAST
     * @return
     */
    private static EmbeddingJsonStrictness getStrictnessConfig(final EmbeddingConfiguration embeddingConfiguration) {
        if (isPresent(embeddingConfiguration.getEmbeddingInputConfig(), PROPERTY_EMBEDDING_INPUT_JSON_STRICTNESS)) {
            String strictness = embeddingConfiguration.getEmbeddingInputConfig()
                    .get(PROPERTY_EMBEDDING_INPUT_JSON_STRICTNESS).toString();
            log.debug("Running with Strictness: {}", strictness);
            return parseEmbeddingJsonStrictness(strictness);
        }
        return PROPERTY_STRICTNESS_DEFAULT;
    }

    private static EmbeddingJsonStrictness parseEmbeddingJsonStrictness(final String strictness) {
        try {
            return EmbeddingJsonStrictness.valueOf(strictness.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            // If the input string is not a valid enum value, use default value
            log.warn("Invalid strictness value: {}. Using default value: {}", strictness, PROPERTY_STRICTNESS_DEFAULT);
            return PROPERTY_STRICTNESS_DEFAULT;
        }
    }
}
