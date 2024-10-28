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
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingInput;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.json.JSONObject;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_INPUT_CHUNK_TEXT_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_INPUT_CHUNK_KEY_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_INPUT_ORIGINAL_TEXT_NAME;

/**
 * This class generates embeddings for String input type using provided Bedrock model and
 * returns a JSONObject containing embeddings.
 */
@Slf4j
public class BedrockEmbeddingGeneratorStringInputImpl extends BedrockEmbeddingGenerator<EmbeddingInput, JSONObject> {

    public BedrockEmbeddingGeneratorStringInputImpl(@NonNull final String region,
                                                    @NonNull final EmbeddingConfiguration config) {
        super(region, config);
    }

    /**
     * Asynchronously invoke the embedding model and complete the ResultFuture with the result.
     *
     * @param input
     * @param resultFuture
     */
    @Override
    public void asyncInvoke(@NonNull final EmbeddingInput input, @NonNull final ResultFuture<JSONObject> resultFuture) {
        long startTime = System.currentTimeMillis();
        CompletableFuture<JSONObject> embeddingFuture = CompletableFuture.supplyAsync(() -> {
            Optional<JSONObject> responseOptional = getEmbeddingJSON(input);
            if (responseOptional.isEmpty()) {
                log.warn("No embedding generated for input: [{}]", input.getStringToEmbed());
                return new JSONObject();
            }
            // TODO: See if we need to add additional fields to the response
            JSONObject response = responseOptional.get();
            response.put(EMBED_INPUT_ORIGINAL_TEXT_NAME, input.getOriginalData());
            if (!StringUtils.isEmpty(input.getChunkData())) {
                response.put(EMBED_INPUT_CHUNK_TEXT_NAME, input.getChunkData());
            }
            if (!StringUtils.isEmpty(input.getChunkKey())) {
                response.put(EMBED_INPUT_CHUNK_KEY_NAME, input.getChunkKey());
            }
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
}
