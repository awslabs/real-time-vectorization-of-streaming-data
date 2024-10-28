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
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.OrRetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnExceptionsCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;
import software.amazon.awssdk.services.bedrockruntime.model.ThrottlingException;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_OUTPUT_TIMESTAMP_KEY_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.EMBEDDING_CONFIG;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.OUTPUT_EMBEDDING_LENGTH;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.METRIC_GROUP_KEY_OPERATION;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.METRIC_GROUP_KEY_SERVICE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.METRIC_GROUP_KINESIS_ANALYTICS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.METRIC_GROUP_VALUE_SERVICE;
import static java.time.Duration.ofMillis;
import static org.apache.commons.lang3.ObjectUtils.isNotEmpty;
import static software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting.MAX_BACKOFF;

/**
 * This class opens a connection to Bedrock client and implements common methods needed to call Bedrock.
 * This abstract class can be extended by classes that use different input/output data types.
 */
@Slf4j
public abstract class BedrockEmbeddingGenerator<IN, OUT> extends RichAsyncFunction<IN, OUT>
        implements EmbeddingGenerator {

    @Setter
    @VisibleForTesting
    transient BedrockRuntimeAsyncClient bedrockClient;
    final String region;
    final EmbeddingConfiguration embeddingConfiguration;
    private transient double latencyInMillis;
    private transient boolean reportLatency = false;
    private transient int tokenCount;
    private transient boolean reportTokenCount = false;

    private static final String RESPONSE_KEY_TOKEN_COUNT = "inputTextTokenCount";
    private static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
    private static final String ACCEPT_MIME_TYPE = "*/*";
    private static final int BASE_DELAY_MILLISECONDS = 1200;
    private static final int MAX_RETRIES = 4;

    private static final String METRIC_GROUP_VALUE_OPERATION = "EmbeddingGeneration";
    private static final String METRIC_NAME_LATENCY_MS = "BedrockEmbeddingGenerationLatencyMs";
    private static final String METRIC_NAME_TOKEN_COUNT = "BedrockTitanEmbeddingTokenCount";

    public BedrockEmbeddingGenerator(@NonNull final String region, @NonNull final EmbeddingConfiguration config) {
        this.region = region;
        this.embeddingConfiguration = config;
        this.embeddingConfiguration.validate();
    }

    /**
     * RichAsyncFunction method that opens a connection to Bedrock Async client
     * @param parameters
     */
    @Override
    public void open(final Configuration parameters) {
        bedrockClient = BedrockRuntimeAsyncClient
                .builder()
                .region(Region.of(region))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(getRetryPolicy())
                        .build())
                .build();
        /*
        By design, for all Flink metrics, if there are no new data points, the gauge's value remains unchanged,
        effectively retaining the last reported value. This means, if the last reported latency was 100ms, the gauge
        will continue to report 1000ms until a new message arrives in the stream and changes the value for the metric.
        Hence, we introduce a boolean to reset the gauge when no metric needs to be reported.
         */
        getEnrichedMetricGroup().gauge(METRIC_NAME_LATENCY_MS, (Gauge<Double>) () -> {
            if (reportLatency) {
                reportLatency = false;
            } else {
                latencyInMillis = 0.0;
            }
            return latencyInMillis;
        });
        getEnrichedMetricGroup().gauge(METRIC_NAME_TOKEN_COUNT, (Gauge<Integer>) () -> {
            if (reportTokenCount) {
                reportTokenCount = false;
            } else {
                tokenCount = 0;
            }
            return tokenCount;
        });
    }

    /**
     * RichAsyncFunction method that closes the connection.
     */
    @Override
    public void close() {
        if (bedrockClient != null) {
            bedrockClient.close();
        }
    }

    /**
     * Method to construct input body from input string.
     * @param input
     * @return
     */
    JSONObject constructInputBody(@NonNull final String input) {
        JSONObject jsonBody = new JSONObject();
        jsonBody.put(this.embeddingConfiguration.getEmbeddingModel().getInputKey(),
                getEmbeddingInputFromString(input, this.embeddingConfiguration.getEmbeddingModel()));

        // Insert configs provided by client into the input body
        if (isNotEmpty(embeddingConfiguration.getEmbeddingModelOverrideConfig())) {
            for (Entry<String, Object> entry : embeddingConfiguration.getEmbeddingModelOverrideConfig().entrySet()) {
                /*
                Special handling for outputEmbeddingLength as it needs to be in a nested JSON.
                https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-titan-embed-mm.html
                TODO: need to find a better way to doing this.
                 */
                if (OUTPUT_EMBEDDING_LENGTH.equals(entry.getKey())) {
                    JSONObject outputEmbedLengthJsonObject = new JSONObject();
                    outputEmbedLengthJsonObject.put(OUTPUT_EMBEDDING_LENGTH, entry.getValue());
                    jsonBody.put(EMBEDDING_CONFIG, outputEmbedLengthJsonObject);
                } else {
                    jsonBody.put(entry.getKey(), entry.getValue());
                }
            }
        }

        /*
         Insert default configs if they were not already provided by the client.
         This should never overwrite client provided configs.
         */
        if (embeddingConfiguration.getEmbeddingModel().getDefaultConfigs() != null) {
            for (Entry<String, String> entry : embeddingConfiguration.getEmbeddingModel()
                    .getDefaultConfigs().entrySet()) {
                if (!jsonBody.has(entry.getKey())) {
                    jsonBody.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return jsonBody;
    }

    /**
     * Prepare model invocation request.
     * <a href="https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_InvokeModel.html">Ref</a>
     * @param body The prompt and inference parameters in the format specified in the contentType in the header.
     *             The body must be in JSON format
     * @return InvokeModelRequest
     */
    InvokeModelRequest prepareInvokeModelRequest(@NonNull final SdkBytes body) {
        return InvokeModelRequest.builder()
                .modelId(embeddingConfiguration.getEmbeddingModel().getModelId())
                .contentType(CONTENT_TYPE_APPLICATION_JSON)
                .accept(ACCEPT_MIME_TYPE)
                .body(body)
                .build();
    }


    /**
     * Get the embedding for the input text.
     *
     * @param input input text to generate embedding for
     * @return JSONObject containing the embedding
     */
    Optional<JSONObject> getEmbeddingJSON(EmbeddingInput input) {
        if (input.isEmpty()) {
            log.info("Received empty input. Skipping embedding generation.");
            return Optional.empty();
        }
        log.info("Embedding raw message: {}", input.getStringToEmbed());
        JSONObject jsonBody = constructInputBody(input.getStringToEmbed());
        log.info("Constructed JSON input body for Bedrock embedding: {}", jsonBody);
        Charset charset = Charset.forName(embeddingConfiguration.getCharset());
        SdkBytes body = SdkBytes.fromString(jsonBody.toString(), charset);
            /*
            TODO: handle exceptions from Bedrock
            Reference for errors: https://tinyurl.com/3re28xbt
            */
        InvokeModelRequest request = prepareInvokeModelRequest(body);
        JSONObject response = getEmbeddingResult(request);
        response.put(EMBED_OUTPUT_TIMESTAMP_KEY_NAME, DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
        if (response.has(RESPONSE_KEY_TOKEN_COUNT)) {
            tokenCount = response.getInt(RESPONSE_KEY_TOKEN_COUNT);
            reportTokenCount = true;
        }
        return Optional.of(response);
    }

    /**
     * Since the format specified in the contentType header of the request is always JSON,
     * we will always get a JSONObject in response. The response will be encoded in UTF8 since
     * charset is not specified in the request contentType header.
     * See:  https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-titan-text.html
     * @param request InvokeModelRequest
     * @return JSONObject containing response.
     */
    JSONObject getEmbeddingResult(@NonNull final InvokeModelRequest request) {
        log.info("Invoking Bedrock model request: {}", request);
        CompletableFuture<InvokeModelResponse> futureResponse = bedrockClient.invokeModel(request);
        return new JSONObject(futureResponse.join().body().asUtf8String());
    }

    /**
     * STANDARD uses FullJitterBackoffStrategy with 100ms base delay, 20s max backoff, and up to 2 retries
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/retry/RetryMode.html
     * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/retry-strategy.html
     * We override the retry policy to adjust retry and base delay. We can tweak these values further based on
     * performance and load testing.
     * We also added Throttling exception in retry condition so that we retry with backoff when throttled.
     * @return
     */
    private RetryPolicy getRetryPolicy() {
        RetryCondition retryCondition = OrRetryCondition.create(
                RetryCondition.defaultRetryCondition(),
                RetryOnExceptionsCondition.create(
                        ThrottlingException.class
                )
        );
        return RetryPolicy.builder(RetryMode.STANDARD)
                .numRetries(MAX_RETRIES)
                .retryCondition(retryCondition)
                .backoffStrategy(FullJitterBackoffStrategy.builder()
                        .baseDelay(ofMillis(BASE_DELAY_MILLISECONDS))
                        .maxBackoffTime(MAX_BACKOFF)
                        .build())
                .build();
    }

    void reportLatencyMetric(final long startTime) {
        long endTime = System.currentTimeMillis();
        long latency = endTime - startTime;
        // round off to 3 decimal places
        latencyInMillis = Math.ceil(latency * 1000.0) / 1000;
        log.info("Bedrock embedding generation latency in millis:{}", latencyInMillis);
        reportLatency = true;
    }

    private MetricGroup getEnrichedMetricGroup() {
        return getRuntimeContext()
                .getMetricGroup()
                .addGroup(METRIC_GROUP_KINESIS_ANALYTICS)
                .addGroup(METRIC_GROUP_KEY_SERVICE, METRIC_GROUP_VALUE_SERVICE)
                .addGroup(METRIC_GROUP_KEY_OPERATION, METRIC_GROUP_VALUE_OPERATION);
    }

    /**
     * Get the object expected to be the embedding input based on the embedding model.
     * For models with input type array, a single element array with the embedding input string is created.
     *
     * @param embeddingInput String to be embedded
     * @param embeddingModel Bedrock model to use to create the embedding
     * @return Embedding input object of the type the model is expecting
     */
    public Object getEmbeddingInputFromString(String embeddingInput, EmbeddingModel embeddingModel) {
        if (embeddingModel.getInputType().equals(JSONArray.class)) {
            JSONArray jsonArray = new JSONArray();
            jsonArray.put(embeddingInput);
            return jsonArray;
        }
        return embeddingInput;
    }
}
