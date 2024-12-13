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
package com.amazonaws.datastreamvectorization.datasink.opensearch;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchDataSinkConfiguration;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import com.amazonaws.datastreamvectorization.exceptions.UnsupportedEmbeddingResultException;
import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.opensearch.sink.OpensearchSink;
import org.apache.flink.connector.opensearch.sink.OpensearchSinkBuilder;
import org.apache.flink.connector.opensearch.sink.RestClientFactory;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Requests;
import org.opensearch.common.xcontent.XContentType;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_OUTPUT_TIMESTAMP_KEY_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_INPUT_ORIGINAL_TEXT_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_INPUT_CHUNK_TEXT_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EMBED_INPUT_CHUNK_KEY_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.SINK_EMBEDDED_DATA_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.SINK_ORIGINAL_DATA_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.SINK_CHUNK_DATA_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.SINK_CHUNK_KEY_NAME;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.SINK_DATE_NAME;
import static com.amazonaws.datastreamvectorization.datasink.model.OpenSearchDataSinkConfiguration.DEFAULT_OS_BULK_FLUSH_INTERVAL_MILLIS;
import static org.apache.flink.connector.opensearch.sink.FlushBackoffType.EXPONENTIAL;

/**
 * This class provides methods to create an OpenSearch data sink.
 * The sink is used to index documents in OpenSearch.
 * The documents are indexed in the specified index in OpenSearch.
 */
@Slf4j
public class OpenSearchSinkBuilder {

    public static final int BACKOFF_MAX_RETRIES = 5;
    public static final int RETRY_INITIAL_DELAY_MILLIS = 1000;

    private static IndexRequest createIndexRequest(JSONObject element, String index, EmbeddingModel model) {
        Map<String, Object> json = new HashMap<>();
        if (!element.has(model.getEmbeddingKey()) || !element.has(EMBED_INPUT_ORIGINAL_TEXT_NAME)
                || !element.has(EMBED_OUTPUT_TIMESTAMP_KEY_NAME)) {
            throw new MissingOrIncorrectConfigurationException("Invalid JSON found when sending to Sink: " + element);
        }

        json.put(SINK_EMBEDDED_DATA_NAME, getEmbeddingResultFromJSONArray(element, model));
        json.put(SINK_ORIGINAL_DATA_NAME, element.get(EMBED_INPUT_ORIGINAL_TEXT_NAME));
        json.put(SINK_DATE_NAME, element.get(EMBED_OUTPUT_TIMESTAMP_KEY_NAME));
        if (element.has(EMBED_INPUT_CHUNK_TEXT_NAME)) {
            json.put(SINK_CHUNK_DATA_NAME, element.get(EMBED_INPUT_CHUNK_TEXT_NAME));
        }
        if (element.has(EMBED_INPUT_CHUNK_KEY_NAME)) {
            json.put(SINK_CHUNK_KEY_NAME, element.get(EMBED_INPUT_CHUNK_KEY_NAME));
        }
        log.info("Indexing document: {}", json);
        return Requests.indexRequest()
                .index(index)
                .source(json, XContentType.JSON);
    }

    public static RestClientFactory getRestClientFactory(String service, String region) {
        return (restClientBuilder, restClientConfig) -> {
            HttpRequestInterceptor interceptor = new AwsRequestSigningApacheInterceptor(
                    service,
                    Aws4Signer.create(),
                    DefaultCredentialsProvider.create(),
                    Region.of(region));
            restClientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder -> httpAsyncClientBuilder.addInterceptorLast(interceptor));
        };
    }

    private static OpensearchSinkBuilder<JSONObject> getOSBuilder(long sinkBulkFlushInterval,
                                                                  String endpointUrl,
                                                                  String index,
                                                                  String serviceName,
                                                                  String region,
                                                                  EmbeddingModel model) {
        return new OpensearchSinkBuilder<JSONObject>()
                .setBulkFlushInterval(sinkBulkFlushInterval)
                .setHosts(HttpHost.create(endpointUrl))
                .setEmitter((element, context, indexer) -> indexer.add(
                        createIndexRequest(element, index, model)))
                // Exponential backoff retry mechanism, with a max of 5 retries and an initial delay of 1000 msecs
                .setBulkFlushBackoffStrategy(EXPONENTIAL, BACKOFF_MAX_RETRIES, RETRY_INITIAL_DELAY_MILLIS)
                .setRestClientFactory(getRestClientFactory(serviceName, region));
    }

    /**
     * getDataSink method creates an OpenSearch data sink.
     *
     * @param osConfig OpenSearch data sink configuration
     * @return OpenSearch data sink
     */
    public OpensearchSink<JSONObject> getDataSink(OpenSearchDataSinkConfiguration osConfig,
                                                  EmbeddingConfiguration embeddingConfig) {
        long sinkBulkFlushInterval = osConfig.getBulkFlushIntervalMillis() != 0 ? osConfig.getBulkFlushIntervalMillis()
                : DEFAULT_OS_BULK_FLUSH_INTERVAL_MILLIS;
        OpensearchSinkBuilder<JSONObject> osBuilder = getOSBuilder(sinkBulkFlushInterval,
                osConfig.getEndpoint(),
                osConfig.getIndex(),
                osConfig.getOpenSearchType().getServiceName(),
                osConfig.getRegion(),
                embeddingConfig.getEmbeddingModel());
        return osBuilder.build();
    }

    /**
     * Get the unnested JSONArray from a nested JSONArray embeddingResult to be stored in the sink.
     * When the input type to the model is an array, the embedding result will be a nested array. We only create
     * embedding input arrays of one element (see above method) so the embedding result is expected to only have one
     * nested array if the input type is JSONArray.
     *
     * @param embeddingResult Embedding result from Bedrock
     * @param embeddingModel The Bedrock model used to create the embedding
     * @return Original embedding result if input was a single-level array, else the one nested array in a nested array.
     */
    private static JSONArray getEmbeddingResultFromJSONArray(JSONObject embeddingResult,
                                                             EmbeddingModel embeddingModel) {
        JSONArray embeddingResultArray = embeddingResult.getJSONArray(embeddingModel.getEmbeddingKey());
        if (embeddingModel.getInputType().equals(JSONArray.class)) {
            if (embeddingResultArray.length() == 1) {
                Object embeddings = embeddingResultArray.get(0);
                if (embeddings instanceof JSONArray) {
                    return (JSONArray) embeddings;
                }
            }
            // Throwing error because we currently don't support storing multiple sink records per embedding record
            throw new UnsupportedEmbeddingResultException(
                    "Found " + embeddingResultArray.length() + " elements in embedding result for embedding input of "
                        + "type array. Only embedding result arrays with one nested array are supported.");
        }
        return embeddingResultArray;
    }
}
