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
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.AccessDeniedException;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_INPUT_TEXT_KEY_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.DIMENSIONS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.EMBEDDING_CONFIG;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.EMBEDDING_TYPES;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.INPUT_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.NORMALIZE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.OUTPUT_EMBEDDING_LENGTH;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.TRUNCATE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_ID;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_OVERRIDES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BedrockEmbeddingGeneratorStringInputImplTest {
    private static final Instant MOCK_INSTANT = Instant.now();

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
        BedrockEmbeddingGeneratorStringInputImpl sut = new BedrockEmbeddingGeneratorStringInputImpl(
                "us-east-1",
                embeddingConfig);
        // Setup mocks
        when(invokeModelFuture.join()).thenReturn(mockResponse);
        when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(invokeModelFuture);
        sut.setBedrockClient(mockBedrockClient);
        sut.setRuntimeContext(mockRuntimeContext);

        // invocation
        CompletableFuture<Collection<JSONObject>> resultFuture = new CompletableFuture<>();
        sut.asyncInvoke("text", new AsyncResultFuture<>(resultFuture));

        // validate results
        JSONObject expectedResponse = new JSONObject(mockResponseJson);
        expectedResponse.put("inputText", "text");
        expectedResponse.put("@timestamp", new Date().toString());

        Collection<JSONObject> responses = resultFuture.get(5, TimeUnit.SECONDS);
        assertNotNull(responses);
        assertEquals(1, responses.size());
        JSONObject response = responses.iterator().next();
        assertEquals(expectedResponse.getString("inputText"), response.getString("inputText"));
        assertEquals(expectedResponse.getInt("inputTextTokenCount"), response.getInt("inputTextTokenCount"));
        assertNotNull(response.getString("@timestamp"));
        assertEquals(3, expectedResponse.getJSONArray("embedding").length());
    }

    @Test
    public void testInvocationThrowsException() {
        // Setup
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_V2.getModelId());
        EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
        BedrockEmbeddingGeneratorStringInputImpl sut = new BedrockEmbeddingGeneratorStringInputImpl(
                "us-east-1",
                embeddingConfig);
        // Setup mocks
        when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenThrow(AccessDeniedException.class);
        sut.setBedrockClient(mockBedrockClient);
        sut.setRuntimeContext(mockRuntimeContext);

        // invocation
        CompletableFuture<Collection<JSONObject>> resultFuture = new CompletableFuture<>();
        sut.asyncInvoke("text", new AsyncResultFuture<>(resultFuture));
        sut.asyncInvoke("text", new AsyncResultFuture<>(resultFuture));

        try {
            resultFuture.get(5, TimeUnit.SECONDS);
            fail("Expected Exception with EmbeddingGenerationException cause to be thrown");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof AccessDeniedException);
        }
    }

    @Test
    public void testGetEmbeddingJSONSuccess() {
        try (final MockedStatic<Instant> mockedStaticInstant = Mockito.mockStatic(Instant.class)) {
            // Setup mocks
            mockedStaticInstant.when(Instant::now).thenReturn(MOCK_INSTANT);
            final String mockEmbeddingResponse = "{\n" +
                    "    \"embedding\": [123.34, 234.56, 456.78],\n" +
                    "    \"inputTextTokenCount\": 2\n" +
                    "}";
            JSONObject mockEmbeddingResponseJSONObject = new JSONObject(mockEmbeddingResponse);
            final InvokeModelResponse mockResponse = InvokeModelResponse.builder()
                    .body(SdkBytes.fromUtf8String(mockEmbeddingResponse))
                    .build();
            when(invokeModelFuture.join()).thenReturn(mockResponse);
            when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(invokeModelFuture);

            // Setup
            Properties properties = new Properties();
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_V2.getModelId());
            EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
            BedrockEmbeddingGeneratorStringInputImpl sut = new BedrockEmbeddingGeneratorStringInputImpl(
                    "us-east-1",
                    embeddingConfig);
            sut.setBedrockClient(mockBedrockClient);
            sut.setRuntimeContext(mockRuntimeContext);

            // invocation
            String inputText = "text";
            Optional<JSONObject> embeddingResult = sut.getEmbeddingJSON(inputText);

            // validate results
            assertTrue(embeddingResult.isPresent());
            assertEquals(3, mockEmbeddingResponseJSONObject.getJSONArray("embedding").length());
            assertEquals(mockEmbeddingResponseJSONObject.getInt("inputTextTokenCount"),
                    embeddingResult.get().getInt("inputTextTokenCount"));
        }
    }

    @Test
    public void testGetEmbeddingJSONEmptyInput() {
        try (final MockedStatic<Instant> mockedStaticInstant = Mockito.mockStatic(Instant.class)) {
            // Setup mocks
            mockedStaticInstant.when(Instant::now).thenReturn(MOCK_INSTANT);
            // Setup
            Properties properties = new Properties();
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_V2.getModelId());
            EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
            BedrockEmbeddingGeneratorStringInputImpl sut = new BedrockEmbeddingGeneratorStringInputImpl(
                    "us-east-1",
                    embeddingConfig);
            sut.setBedrockClient(mockBedrockClient);
            sut.setRuntimeContext(mockRuntimeContext);

            // invocation
            Optional<JSONObject> embeddingResult = sut.getEmbeddingJSON("");

            // validate results
            assertFalse(embeddingResult.isPresent());
            verifyNoInteractions(mockBedrockClient, mockRuntimeContext);
        }
    }

    @Test
    public void testConstructInputBody_AddProps() {
        try (final MockedStatic<Instant> mockedStaticInstant = Mockito.mockStatic(Instant.class)) {
            // Setup mocks
            mockedStaticInstant.when(Instant::now).thenReturn(MOCK_INSTANT);
            // Setup
            Properties properties = new Properties();
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.COHERE_EMBED_ENGLISH.getModelId());
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + INPUT_TYPE, "classification");
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + TRUNCATE, "START");
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + EMBEDDING_TYPES, "int8");
            EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
            BedrockEmbeddingGeneratorStringInputImpl sut = new BedrockEmbeddingGeneratorStringInputImpl(
                    "us-east-1",
                    embeddingConfig);
            sut.setBedrockClient(mockBedrockClient);
            sut.setRuntimeContext(mockRuntimeContext);

            // invocation
            JSONObject result = sut.constructInputBody("sample-text");

            // validate results
            assertEquals("sample-text", result.getString(EMBED_INPUT_TEXT_KEY_NAME));
            assertEquals(properties.getProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + INPUT_TYPE),
                    result.getString(INPUT_TYPE));
            assertEquals(properties.getProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + TRUNCATE),
                    result.getString(TRUNCATE));
            assertEquals(properties.getProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + EMBEDDING_TYPES),
                    result.getString(EMBEDDING_TYPES));
        }
    }

    @Test
    public void testConstructInputBody_SkipUnwantedConfigs() {
        try (final MockedStatic<Instant> mockedStaticInstant = Mockito.mockStatic(Instant.class)) {
            // Setup mocks
            mockedStaticInstant.when(Instant::now).thenReturn(MOCK_INSTANT);
            // Setup
            Properties properties = new Properties();
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_TEXT_V2.getModelId());
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + NORMALIZE, "true");
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + DIMENSIONS, "128");
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + OUTPUT_EMBEDDING_LENGTH, "8");
            EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
            BedrockEmbeddingGeneratorStringInputImpl sut = new BedrockEmbeddingGeneratorStringInputImpl(
                    "us-east-1",
                    embeddingConfig);
            sut.setBedrockClient(mockBedrockClient);
            sut.setRuntimeContext(mockRuntimeContext);

            // invocation
            JSONObject result = sut.constructInputBody("sample-text");

            // validate results
            assertEquals("sample-text", result.getString(EMBED_INPUT_TEXT_KEY_NAME));
            assertFalse(result.has(OUTPUT_EMBEDDING_LENGTH));
            assertTrue((Boolean) result.get(NORMALIZE));
            assertEquals(128, result.getInt(DIMENSIONS));
        }
    }

   @Test
    public void testConstructInputBody_WithOutputEmbeddingLength() {
        try (final MockedStatic<Instant> mockedStaticInstant = Mockito.mockStatic(Instant.class)) {
            // Setup mocks
            mockedStaticInstant.when(Instant::now).thenReturn(MOCK_INSTANT);
            // Setup
            Properties properties = new Properties();
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_ID, EmbeddingModel.AMAZON_TITAN_MULTIMODAL_G1.getModelId());
            properties.setProperty(PROPERTY_EMBEDDING_MODEL_OVERRIDES + OUTPUT_EMBEDDING_LENGTH, "1024");
            EmbeddingConfiguration embeddingConfig = EmbeddingConfiguration.parseFrom(properties).build();
            BedrockEmbeddingGeneratorStringInputImpl sut = new BedrockEmbeddingGeneratorStringInputImpl(
                    "us-east-1",
                    embeddingConfig);
            sut.setBedrockClient(mockBedrockClient);
            sut.setRuntimeContext(mockRuntimeContext);

            // invocation
            JSONObject result = sut.constructInputBody("sample-text");

            // validate results
            assertEquals("sample-text", result.getString(EMBED_INPUT_TEXT_KEY_NAME));
            assertTrue(result.has(EMBEDDING_CONFIG));
            assertTrue(result.getJSONObject(EMBEDDING_CONFIG).has(OUTPUT_EMBEDDING_LENGTH));
            assertEquals(1024, result.getJSONObject(EMBEDDING_CONFIG).getInt(OUTPUT_EMBEDDING_LENGTH));
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
}


