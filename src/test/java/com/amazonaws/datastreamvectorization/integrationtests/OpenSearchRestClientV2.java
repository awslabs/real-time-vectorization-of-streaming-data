package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.datastreamvectorization.integrationtests.model.OpenSearchIndexDocument;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.*;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.indices.*;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class uses the following new Java client for OpenSearch:
 *  https://opensearch.org/docs/latest/clients/java/#connecting-to-amazon-opensearch-service
 *  https://opensearch.org/docs/latest/clients/java/#connecting-to-amazon-opensearch-serverless
 * The OpenSearch Java high-level REST client is deprecated:
 *  https://opensearch.org/docs/latest/clients/java-rest-high-level/
 */
@Slf4j
public class OpenSearchRestClientV2 {
    private final String region;

    public OpenSearchRestClientV2() {
        AwsRegionProvider regionProvider = new DefaultAwsRegionProviderChain();
        region = regionProvider.getRegion();
    }

    public CreateIndexResponse createIndex(String openSearchEndpoint, OpenSearchType openSearchType, String indexName, EmbeddingModel embeddingModel) {
        try (SdkHttpClient httpClient = createHttpClient()) {
            // create settings for index
            IndexSettings settings = new IndexSettings.Builder()
                    .knn(true)
                    .build();

            // https://github.com/opensearch-project/opensearch-java/blob/main/java-client/src/test/java/org/opensearch/client/opensearch/_types/mapping/KnnVectorMethodTest.java
            Map<String, JsonData> parameters = new HashMap<>();
            parameters.put("ef_construction", JsonData.of(128));
            parameters.put("m", JsonData.of(24));

            KnnVectorMethod method = new KnnVectorMethod.Builder()
                    .name("hnsw")
                    .engine("nmslib")
                    .parameters(parameters)
                    .build();
            KnnVectorProperty embeddedData = new KnnVectorProperty.Builder()
                    .dimension(embeddingModel.getModelDefaultDimensions())
                    .method(method)
                    .build();

            Map<String, Property> properties = new HashMap<>();
            properties.put("embedded_data", new Property(embeddedData));

            TypeMapping mappings = new TypeMapping.Builder()
                    .properties(properties)
                    .build();

            // build the create index request
            CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
                    .index(indexName)
                    .settings(settings)
                    .mappings(mappings)
                    .build();

            // send the create index request
            OpenSearchClient client = createOpenSearchClient(httpClient, openSearchEndpoint, openSearchType);
            return client.indices().create(createIndexRequest);
        } catch (Exception e) {
            throw new RuntimeException("Error when creating the OpenSearch index" + indexName + ": ", e);
        }
    }

    public DeleteIndexResponse deleteIndex(String openSearchEndpoint, OpenSearchType openSearchType, String indexName) {
        try (SdkHttpClient httpClient = createHttpClient()) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder()
                    .index(indexName)
                    .build();
            OpenSearchClient client = createOpenSearchClient(httpClient, openSearchEndpoint, openSearchType);
            return client.indices().delete(deleteIndexRequest);
        } catch (Exception e) {
            throw new RuntimeException("Error when deleting the OpenSearch index" + indexName + ": ", e);
        }
    }

    public List<OpenSearchIndexDocument> queryIndexRecords(OpenSearchType openSearchType, String indexName, String openSearchEndpoint) {
        // TODO: https://opensearch.org/docs/latest/clients/java-rest-high-level/
        //      The OpenSearch Java high-level REST client is deprecated.
        //      Support will be removed in OpenSearch version 3.0.0.
        //      We recommend switching to the Java client instead.
        //  SWITCH create index to below method with Java client

        // https://opensearch.org/docs/latest/clients/java/#connecting-to-amazon-opensearch-service
        try (SdkHttpClient httpClient = createHttpClient()) {
            OpenSearchClient client = createOpenSearchClient(httpClient, openSearchEndpoint, openSearchType);
                    // https://opensearch.org/docs/latest/clients/java/#searching-for-documents
                    SearchResponse<OpenSearchIndexDocument> searchResponse = client.search(
                    s -> s.index(indexName), OpenSearchIndexDocument.class);
            return searchResponse.documents();
        } catch (Exception e) {
            throw new RuntimeException("Error when searching the index " + indexName + ": ", e);
        }
    }

    private SdkHttpClient createHttpClient() {
        return ApacheHttpClient.builder().build();
    }

    private OpenSearchClient createOpenSearchClient(SdkHttpClient httpClient, String openSearchEndpoint, OpenSearchType openSearchType) {
        return new OpenSearchClient(
                new AwsSdk2Transport(
                        httpClient,
                        openSearchEndpoint,
                        openSearchType.getServiceName(),
                        Region.of(this.region),
                        AwsSdk2TransportOptions.builder().build()
                )
        );
    }

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
