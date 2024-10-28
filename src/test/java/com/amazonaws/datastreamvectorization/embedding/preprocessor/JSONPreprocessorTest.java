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
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_CONFIG;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_JSON_FIELDS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_JSON_STRICTNESS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_ID;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingJsonStrictness.FAIL_FAST;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingJsonStrictness.MISSING_FIELDS_EMBED_ALL;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingJsonStrictness.MISSING_FIELDS_IGNORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(MockitoExtension.class)
public class JSONPreprocessorTest {

    @Test
    public void testSuccessInvocation() throws Exception {
        // Setup
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_V2.getModelId());
        EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
        JSONPreprocessor sut = new JSONPreprocessor(embeddingConfig);

        // invocation
        CompletableFuture<Collection<JSONObject>> resultFuture = new CompletableFuture<>();
        sut.asyncInvoke(getInputJson(), new AsyncResultFuture<>(resultFuture));

        // validate results
        Collection<JSONObject> responses = resultFuture.get(5, TimeUnit.SECONDS);
        assertNotNull(responses);
        assertEquals(1, responses.size());
        JSONObject response = responses.iterator().next();
        assertEquals(getInputJson().keySet().size(), response.keySet().size());
        getInputJson().keySet().forEach(key -> assertNotNull(response.get(key)));
    }

    private static Stream<Arguments> provideFieldsToEmbedWithStrictness() {
        return Stream.of(
                // valid fields, strict check: FAIL_FAST
                Arguments.of("name,title,version", List.of("name", "title", "version"),
                        FAIL_FAST, false),
                Arguments.of("name,,title", List.of("name", "title"),
                        FAIL_FAST, false),
                Arguments.of("name, title, version, ", List.of("name", "title", "version"),
                        FAIL_FAST, false),
                Arguments.of(".*", new ArrayList<>(getInputJson().keySet()),
                        FAIL_FAST, false),
                // valid fields, strict check: MISSING_FIELDS_EMBED_ALL
                Arguments.of("name,title,version", List.of("name", "title", "version"),
                        MISSING_FIELDS_EMBED_ALL, false),
                Arguments.of("name,,title", List.of("name", "title"),
                        MISSING_FIELDS_EMBED_ALL, false),
                Arguments.of("name, title, version, ", List.of("name", "title", "version"),
                        MISSING_FIELDS_EMBED_ALL, false),
                Arguments.of(".*", new ArrayList<>(getInputJson().keySet()),
                        MISSING_FIELDS_EMBED_ALL, false),
                // invalid fields, strict check: FAIL_FAST
                Arguments.of("name,title,vs", Collections.EMPTY_LIST, FAIL_FAST, true),
                Arguments.of("name123,title123", Collections.EMPTY_LIST, FAIL_FAST, true),
                // invalid fields, strict check: MISSING_FIELDS_IGNORE
                Arguments.of("name, title, abc123", List.of("name", "title"),
                        MISSING_FIELDS_IGNORE, false),
                Arguments.of("name123, title123", Collections.emptyList(),
                        MISSING_FIELDS_IGNORE, false),
                // invalid fields, strict check: MISSING_FIELDS_EMBED_ALL
                Arguments.of("name, title, abc123", List.of("name", "title"), MISSING_FIELDS_EMBED_ALL, false),
                Arguments.of("name123, title123", new ArrayList<>(getInputJson().keySet()),
                        MISSING_FIELDS_EMBED_ALL, false)
        );
    }

    @ParameterizedTest
    @MethodSource("provideFieldsToEmbedWithStrictness")
    public void testSuccessWithFieldsAndStrictness(String fieldsToEmbed, List<String> expectedFields,
                                                   EmbeddingJsonStrictness strictness, boolean expectException)
            throws ExecutionException, InterruptedException, TimeoutException {
        // Setup
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_V2.getModelId());
        properties.setProperty(PROPERTY_EMBEDDING_INPUT_CONFIG + PROPERTY_EMBEDDING_INPUT_JSON_STRICTNESS,
                strictness.toString());
        properties.setProperty(PROPERTY_EMBEDDING_INPUT_CONFIG + PROPERTY_EMBEDDING_INPUT_JSON_FIELDS,
                fieldsToEmbed);
        EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
        JSONPreprocessor sut = new JSONPreprocessor(embeddingConfig);

        // invocation
        CompletableFuture<Collection<JSONObject>> resultFuture = new CompletableFuture<>();
        sut.asyncInvoke(getInputJson(), new AsyncResultFuture<>(resultFuture));

        // validate results
        if (expectException) {
            try {
                resultFuture.get(5, TimeUnit.SECONDS);
                fail("Expected Exception with EmbeddingGenerationException cause to be thrown");
            } catch (Exception e) {
                assertTrue(e.getCause() instanceof MissingOrIncorrectConfigurationException);
            }
            return;
        }
        Collection<JSONObject> responses = resultFuture.get(5, TimeUnit.SECONDS);
        assertNotNull(responses);
        assertEquals(1, responses.size());

        if (!expectedFields.isEmpty()) {
            JSONObject response = responses.iterator().next();
            assertEquals(expectedFields.size(), response.keySet().size());
            expectedFields.forEach(key -> assertNotNull(response.get(key)));
        } else {
            JSONObject response = responses.iterator().next();
            assertTrue(response.isEmpty());
        }
    }

    /**
     * Small result future class to store result future.
     *
     * @param <T>
     */
    private static class AsyncResultFuture<T> implements ResultFuture<T> {
        private final CompletableFuture<Collection<T>> resultFuture;

        AsyncResultFuture(CompletableFuture<Collection<T>> resultFuture) {
            this.resultFuture = resultFuture;
        }

        @Override
        public void complete(Collection<T> result) {
            resultFuture.complete(result);
        }

        @Override
        public void completeExceptionally(Throwable throwable) {
            resultFuture.completeExceptionally(throwable);
        }
    }

    private static JSONObject getInputJson() {
        JSONObject inputJson = new JSONObject();
        inputJson.put("name", "someName");
        inputJson.put("title", "someTitle");
        inputJson.put("description", "someDescription");
        inputJson.put("version", 1);
        inputJson.put("metadata", "someMetadata");
        inputJson.put("isValid", true);
        return inputJson;
    }
}


