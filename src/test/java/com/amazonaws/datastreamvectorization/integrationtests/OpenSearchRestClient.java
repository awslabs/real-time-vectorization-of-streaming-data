package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchHit;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class OpenSearchRestClient {
    private final String region;

    public OpenSearchRestClient() {
        AwsRegionProvider regionProvider = new DefaultAwsRegionProviderChain();
        region = regionProvider.getRegion();
    }

    /**
     * Creates a new vector index with dimensions matching vectors produced by the given embedding model.
     *
     * @param openSearchEndpointUrl The endpoint URL of the OpenSearch cluster to connect to
     * @param openSearchType The type of the OpenSearch cluster
     * @param indexName The name of the index to create
     * @param embeddingModel The embedding model that will produce the vectors to store in this vector index
     * @return CreateIndexResponse
     */
    public CreateIndexResponse createVectorIndex(String openSearchEndpointUrl,
                                                 OpenSearchType openSearchType,
                                                 String indexName,
                                                 EmbeddingModel embeddingModel) {
        try (RestHighLevelClient client = getRestHighLevelClient(openSearchEndpointUrl, openSearchType)) {
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

            Map<String, Object> properties = new HashMap<>();
            properties.put("embedded_data", embeddedData);

            Map<String, Object> mappings = new HashMap<>();
            mappings.put("properties", properties);

            // build the create index request
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            createIndexRequest.settings(Settings.builder()
                    .put("index.knn", true)
            );
            createIndexRequest.mapping(mappings);

            // send the create index request
            return client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException("Error when creating the OpenSearch index " + indexName + ": ", e);
        }
    }

    /**
     * Deletes the given index from the OpenSearch cluster
     *
     * @param openSearchEndpointUrl The endpoint URL of the OpenSearch cluster to connect to
     * @param openSearchType The type of the OpenSearch cluster
     * @param indexName The name of the index to delete
     * @return AcknowledgedResponse
     */
    public AcknowledgedResponse deleteIndex(String openSearchEndpointUrl,
                                            OpenSearchType openSearchType,
                                            String indexName) {
        try (RestHighLevelClient client = getRestHighLevelClient(openSearchEndpointUrl, openSearchType)) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("indexName");
            return client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException("Error when deleting the OpenSearch index " + indexName + ": ", e);
        }
    }

    /**
     * Queries the OpenSearc
     * @param openSearchEndpointUrl
     * @param openSearchType
     * @param indexName
     */
    public void queryIndexRecords(String openSearchEndpointUrl,
                                  OpenSearchType openSearchType,
                                  String indexName) {
        try (RestHighLevelClient client = getRestHighLevelClient(openSearchEndpointUrl, openSearchType)) {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                System.out.println(hit.field("original_data"));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error when querying the OpenSearch index " + indexName + ": ", e);
        }
    }

    private RestHighLevelClient getRestHighLevelClient(String openSearchEndpointUrl, OpenSearchType openSearchType) {
        HttpRequestInterceptor interceptor = new AwsRequestSigningApacheInterceptor(
                openSearchType.getServiceName(),
                Aws4Signer.create(),
                DefaultCredentialsProvider.create(),
                Region.of(this.region));

        RestClientBuilder restClientBuilder = RestClient.builder(
                        new HttpHost(openSearchEndpointUrl, 443, "https"))
                .setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)
                );

        return new RestHighLevelClient(restClientBuilder);
    }
}
