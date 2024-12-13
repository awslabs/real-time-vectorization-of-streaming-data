package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.common.settings.Settings;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.Map;

public class OpenSearchRestClient {
    private final String region;

    public OpenSearchRestClient() {
        AwsRegionProvider regionProvider = new DefaultAwsRegionProviderChain();
        region = regionProvider.getRegion();
    }

    public CreateIndexResponse createIndex(String openSearchEndpoint, OpenSearchType openSearchType, String indexName, EmbeddingModel embeddingModel) {
        try (RestHighLevelClient client = getRestHighLevelClient(openSearchEndpoint, openSearchType)) {
            // create mappings for index
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("ef_construction", 128);
            parameters.put("m", 24);

            Map<String, Object> method = new HashMap<>();
            method.put("name", "hnsw");
            method.put("engine", "nmslib");
            method.put("parameters", parameters);

            Map<String, Object> embeddedData = new HashMap<>();
            embeddedData.put("type", "knn_vector");
            embeddedData.put("dimension", embeddingModel.getModelDefaultDimensions());
            embeddedData.put("method", method);

            // build the create index request
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            createIndexRequest.settings(Settings.builder()
                    .put("index.knn", true)
            );
            createIndexRequest.mapping(embeddedData);

            // send the create index request
            return client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException("Error when creating the OpenSearch index" + indexName + ": ", e);
        }

    }

    public void queryIndexRecords(String indexName, int startTimestamp) {

    }

    private RestHighLevelClient getRestHighLevelClient(String openSearchEndpoint, OpenSearchType openSearchType) {
        HttpRequestInterceptor interceptor = new AwsRequestSigningApacheInterceptor(
                openSearchType.getServiceName(),
                Aws4Signer.create(),
                DefaultCredentialsProvider.create(),
                Region.of(this.region));

        RestClientBuilder restClientBuilder = RestClient.builder(
                        HttpHost.create(openSearchEndpoint))
                .setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)
                );

        return new RestHighLevelClient(restClientBuilder);
    }
}
