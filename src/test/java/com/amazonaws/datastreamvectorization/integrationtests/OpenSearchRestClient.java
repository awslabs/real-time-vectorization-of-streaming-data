package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.junit.jupiter.api.Assertions;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
//import org.opensearch.client.json.JsonData;
//import org.opensearch.client.opensearch.OpenSearchClient;
//import org.opensearch.client.opensearch._types.mapping.*;
//import org.opensearch.client.opensearch.core.SearchResponse;
//import org.opensearch.client.opensearch.indices.*;
//import org.opensearch.client.transport.aws.AwsSdk2Transport;
//import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import java.util.*;
import java.util.stream.Collectors;

import static com.amazonaws.services.cloudfront.util.SignerUtils.Protocol.https;

@Slf4j
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

//    public DeleteIndexResponse deleteIndex(String openSearchEndpoint, OpenSearchType openSearchType, String indexName, EmbeddingModel embeddingModel) {
//        try (RestHighLevelClient client = getRestHighLevelClient(openSearchEndpoint, openSearchType)) {
//            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
//
//            // send the create index request
//            return client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
//        } catch (Exception e) {
//            throw new RuntimeException("Error when creating the OpenSearch index " + indexName + ": ", e);
//        }
//    }

//    public CreateIndexResponse createIndex(String openSearchEndpoint, OpenSearchType openSearchType, String indexName, EmbeddingModel embeddingModel) {
//        try (SdkHttpClient httpClient = createHttpClient()) {
//            // create settings for index
//            IndexSettings settings = new IndexSettings.Builder()
//                    .knn(true)
//                    .build();
//
//            // https://github.com/opensearch-project/opensearch-java/blob/main/java-client/src/test/java/org/opensearch/client/opensearch/_types/mapping/KnnVectorMethodTest.java
//            Map<String, JsonData> parameters = new HashMap<>();
//            parameters.put("ef_construction", JsonData.of(128));
//            parameters.put("m", JsonData.of(24));
//
//            KnnVectorMethod method = new KnnVectorMethod.Builder()
//                    .name("hnsw")
//                    .engine("nmslib")
//                    .parameters(parameters)
//                    .build();
//            KnnVectorProperty embeddedData = new KnnVectorProperty.Builder()
//                    .dimension(embeddingModel.getModelDefaultDimensions())
//                    .method(method)
//                    .build();
//
//            Map<String, Property> properties = new HashMap<>();
//            properties.put("embedded_data", new Property(embeddedData));
//
//            TypeMapping mappings = new TypeMapping.Builder()
//                    .properties(properties)
//                    .build();
//
//            // build the create index request
//            CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
//                    .index(indexName)
//                    .settings(settings)
//                    .mappings(mappings)
//                    .build();
//
//            // send the create index request
//            OpenSearchClient client = createOpenSearchClient(httpClient, openSearchEndpoint, openSearchType);
//            return client.indices().create(createIndexRequest);
//        } catch (Exception e) {
//            throw new RuntimeException("Error when creating the OpenSearch index" + indexName + ": ", e);
//        }
//    }

//    public DeleteIndexResponse deleteIndex(String openSearchEndpoint, OpenSearchType openSearchType, String indexName) {
//        try (SdkHttpClient httpClient = createHttpClient()) {
//            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder()
//                    .index(indexName)
//                    .build();
//            OpenSearchClient client = createOpenSearchClient(httpClient, openSearchEndpoint, openSearchType);
//            return client.indices().delete(deleteIndexRequest);
//        } catch (Exception e) {
//            throw new RuntimeException("Error when deleting the OpenSearch index" + indexName + ": ", e);
//        }
//    }

//    public List<OpenSearchIndexDocument> queryIndexRecords(OpenSearchType openSearchType, String indexName, String openSearchEndpoint) {
//        // TODO: https://opensearch.org/docs/latest/clients/java-rest-high-level/
//        //      The OpenSearch Java high-level REST client is deprecated.
//        //      Support will be removed in OpenSearch version 3.0.0.
//        //      We recommend switching to the Java client instead.
//        //  SWITCH create index to below method with Java client
//
//        // https://opensearch.org/docs/latest/clients/java/#connecting-to-amazon-opensearch-service
//        try (SdkHttpClient httpClient = createHttpClient()) {
//            OpenSearchClient client = createOpenSearchClient(httpClient, openSearchEndpoint, openSearchType);
//                    // https://opensearch.org/docs/latest/clients/java/#searching-for-documents
//                    SearchResponse<OpenSearchIndexDocument> searchResponse = client.search(
//                    s -> s.index(indexName), OpenSearchIndexDocument.class);
//            return searchResponse.documents();
//        } catch (Exception e) {
//            throw new RuntimeException("Error when searching the index " + indexName + ": ", e);
//        }
//    }

    public void queryIndexRecords(OpenSearchType openSearchType, String indexName, String openSearchEndpoint) {
        try (RestHighLevelClient client = getRestHighLevelClient(openSearchEndpoint, openSearchType)) {
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

    private SdkHttpClient createHttpClient() {
        return ApacheHttpClient.builder().build();
    }

//    private OpenSearchClient createOpenSearchClient(SdkHttpClient httpClient, String openSearchEndpoint, OpenSearchType openSearchType) {
//        return new OpenSearchClient(
//                new AwsSdk2Transport(
//                        httpClient,
//                        openSearchEndpoint,
//                        openSearchType.getServiceName(),
//                        Region.of(this.region),
//                        AwsSdk2TransportOptions.builder().build()
//                )
//        );
//    }

    // TODO: think about using the following instead (above method):
    //  https://opensearch.org/docs/latest/clients/java/#connecting-to-amazon-opensearch-service
    //  https://opensearch.org/docs/latest/clients/java/#connecting-to-amazon-opensearch-serverless
    private RestHighLevelClient getRestHighLevelClient(String openSearchEndpoint, OpenSearchType openSearchType) {
        HttpRequestInterceptor interceptor = new AwsRequestSigningApacheInterceptor(
                openSearchType.getServiceName(),
                Aws4Signer.create(),
                DefaultCredentialsProvider.create(),
                Region.of(this.region));

        RestClientBuilder restClientBuilder = RestClient.builder(
                        new HttpHost(openSearchEndpoint, 443, "https"))
                .setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)
                );

        return new RestHighLevelClient(restClientBuilder);
    }

    // TODO: remove later from here
    public void validateOpenSearchRecords(List<String> expectedOriginalDataList, List<OpenSearchIndexDocument> searchResult) {
        if (searchResult.size() != expectedOriginalDataList.size()) {
            log.info("Did not find expected number of records in OpenSearch search result. " +
                    "Expected {} records but got {}", expectedOriginalDataList.size(), searchResult.size());
        }
        List<String> resultOriginalDataList = searchResult
                .stream()
                .map(OpenSearchIndexDocument::getOriginal_data)
                .sorted().
                collect(Collectors.toList());

        Collections.sort(expectedOriginalDataList);
        Assertions.assertEquals(expectedOriginalDataList, resultOriginalDataList);
    }
}
