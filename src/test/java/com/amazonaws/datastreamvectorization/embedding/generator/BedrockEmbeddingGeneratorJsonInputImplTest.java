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
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.skyscreamer.jsonassert.JSONAssert;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.AccessDeniedException;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_INPUT_TEXT_KEY_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_OUTPUT_TIMESTAMP_KEY_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_CONFIG;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_JSON_FIELDS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BedrockEmbeddingGeneratorJsonInputImplTest {

    @Mock
    private static RuntimeContext mockRuntimeContext;
    @Mock
    private static OperatorMetricGroup mockMetricGroup;
    @Mock
    private BedrockRuntimeAsyncClient mockBedrockClient;
    @Mock
    private CompletableFuture<InvokeModelResponse> invokeModelFuture;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(mockRuntimeContext.getMetricGroup()).thenReturn(mockMetricGroup);
        when(mockMetricGroup.addGroup(anyString())).thenReturn(mockMetricGroup);
        when(mockMetricGroup.addGroup(anyString(), anyString())).thenReturn(mockMetricGroup);
    }

    @Test
    public void testSuccessInvocation() throws Exception {
        // Setup
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_V2.getModelId());
        EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
        final String mockResponseJson = "{\n" +
                "    \"embedding\": [123.34, 234.56, 456.78],\n" +
                "    \"inputTextTokenCount\": 2\n" +
                "}";
        final InvokeModelResponse mockResponse = InvokeModelResponse.builder()
                .body(SdkBytes.fromUtf8String(mockResponseJson))
                .build();
        BedrockEmbeddingGeneratorJsonInputImpl sut = new BedrockEmbeddingGeneratorJsonInputImpl(
                "us-east-1", embeddingConfig);
        // Setup mocks
        when(invokeModelFuture.join()).thenReturn(mockResponse);
        when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(invokeModelFuture);
        sut.setBedrockClient(mockBedrockClient);
        sut.setRuntimeContext(mockRuntimeContext);

        // invocation
        CompletableFuture<Collection<JSONObject>> resultFuture = new CompletableFuture<>();
        sut.asyncInvoke(getInputJson(), new AsyncResultFuture<>(resultFuture));

        // validate results
        JSONObject expectedResponse = new JSONObject(mockResponseJson);
        expectedResponse.put(EMBED_INPUT_TEXT_KEY_NAME, getInputJson());
        expectedResponse.put(EMBED_OUTPUT_TIMESTAMP_KEY_NAME, DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

        Collection<JSONObject> responses = resultFuture.get(5, TimeUnit.SECONDS);
        assertNotNull(responses);
        assertEquals(1, responses.size());
        JSONObject response = responses.iterator().next();
        assertEquals(4, response.keySet().size());
        expectedResponse.keySet().forEach(key -> assertNotNull(response.get(key)));
        assertEquals(3, response.getJSONArray("embedding").length());
        assertEquals(expectedResponse.getInt("inputTextTokenCount"), response.getInt("inputTextTokenCount"));
        String inputTextJsonString = response.getString(EMBED_INPUT_TEXT_KEY_NAME);
        JSONAssert.assertEquals(new JSONObject(inputTextJsonString),  getInputJson(), true);
    }


    private static Stream<Arguments> provideFieldsToEmbed() {
        return Stream.of(
                Arguments.of("name,title,version", List.of("name", "title", "version")),
                Arguments.of("name,,title", List.of("name", "title")),
                Arguments.of("name, title, version, ", List.of("name", "title", "version")),
                Arguments.of("*", new ArrayList<>(getInputJson().keySet()))
        );
    }

    @ParameterizedTest
    @MethodSource("provideFieldsToEmbed")
    public void testSuccessInvocationWithFilteredFields(String fieldsToEmbed, List<String> expectedFields) throws Exception {
        // Setup
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_V2.getModelId());
        properties.setProperty(PROPERTY_EMBEDDING_INPUT_CONFIG + PROPERTY_EMBEDDING_INPUT_JSON_FIELDS,
                fieldsToEmbed);
        EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
        final String mockResponseJson = "{\n" +
                "    \"embedding\": [123.34, 234.56, 456.78],\n" +
                "    \"inputTextTokenCount\": 2\n" +
                "}";
        final InvokeModelResponse mockResponse = InvokeModelResponse.builder()
                .body(SdkBytes.fromUtf8String(mockResponseJson))
                .build();
        BedrockEmbeddingGeneratorJsonInputImpl sut = new BedrockEmbeddingGeneratorJsonInputImpl(
                "us-east-1", embeddingConfig);
        // Setup mocks
        when(invokeModelFuture.join()).thenReturn(mockResponse);
        when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(invokeModelFuture);

        sut.setBedrockClient(mockBedrockClient);
        sut.setRuntimeContext(mockRuntimeContext);

        // invocation
        CompletableFuture<Collection<JSONObject>> resultFuture = new CompletableFuture<>();
        sut.asyncInvoke(getInputJson(), new AsyncResultFuture<>(resultFuture));

        // validate results
        JSONObject expectedResponse = new JSONObject(mockResponseJson);
        JSONObject trimmedJson = new JSONObject();
        expectedFields.forEach(key -> trimmedJson.put(key, getInputJson().get(key)));
        expectedResponse.put(EMBED_INPUT_TEXT_KEY_NAME, trimmedJson);
        expectedResponse.put(EMBED_OUTPUT_TIMESTAMP_KEY_NAME, DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

        Collection<JSONObject> responses = resultFuture.get(5, TimeUnit.SECONDS);
        assertNotNull(responses);
        assertEquals(1, responses.size());

        JSONObject response = responses.iterator().next();
        assertEquals(4, response.keySet().size());
        expectedResponse.keySet().forEach(key -> assertNotNull(response.get(key)));
        assertEquals(3, response.getJSONArray("embedding").length());
        assertEquals(expectedResponse.getInt("inputTextTokenCount"), response.getInt("inputTextTokenCount"));
        String inputTextJsonString = response.getString(EMBED_INPUT_TEXT_KEY_NAME);
        JSONAssert.assertEquals(new JSONObject(inputTextJsonString),  trimmedJson, true);
    }

    @Test
    public void testInvocationThrowsException() {
        // Setup
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_V2.getModelId());
        EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
        BedrockEmbeddingGeneratorJsonInputImpl sut = new BedrockEmbeddingGeneratorJsonInputImpl(
                "us-east-1",
                embeddingConfig);
        // Setup mocks
        when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenThrow(AccessDeniedException.class);
        sut.setBedrockClient(mockBedrockClient);
        sut.setRuntimeContext(mockRuntimeContext);

        // invocation
        CompletableFuture<Collection<JSONObject>> resultFuture = new CompletableFuture<>();
        sut.asyncInvoke(getInputJson(), new AsyncResultFuture<>(resultFuture));
        sut.asyncInvoke(getInputJson(), new AsyncResultFuture<>(resultFuture));

        try {
            resultFuture.get(5, TimeUnit.SECONDS);
            fail("Expected Exception with EmbeddingGenerationException cause to be thrown");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof AccessDeniedException);
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


