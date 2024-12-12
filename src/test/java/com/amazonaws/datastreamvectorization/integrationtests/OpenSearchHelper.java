package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.services.opensearch.AmazonOpenSearch;
import com.amazonaws.services.opensearch.AmazonOpenSearchClientBuilder;
import com.amazonaws.services.opensearch.model.DescribeDomainRequest;
import com.amazonaws.services.opensearch.model.DescribeDomainResult;

import com.amazonaws.services.opensearchserverless.AWSOpenSearchServerless;
import com.amazonaws.services.opensearchserverless.AWSOpenSearchServerlessClientBuilder;
import com.amazonaws.services.opensearchserverless.model.BatchGetCollectionRequest;
import com.amazonaws.services.opensearchserverless.model.BatchGetCollectionResult;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class OpenSearchHelper {
    AmazonOpenSearch osProvisionedClient;
    AWSOpenSearchServerless osServerlessClient;
    String testId;

    public OpenSearchHelper(String testId) {
//        String region = AmazonOpenSearchClientBuilder.standard().getRegion();

        osProvisionedClient = AmazonOpenSearchClientBuilder.defaultClient();
//        osServerlessClient = AWSOpenSearchServerlessClientBuilder.defaultClient();
        this.testId = testId;
    }

    public OpenSearchClusterData getOpenSearchClusterData(String osClusterName, OpenSearchType osClusterType) {
        String openSearchEndpointURL = "";
        if (osClusterType == OpenSearchType.PROVISIONED) {
            DescribeDomainRequest describeDomainRequest = new DescribeDomainRequest().withDomainName(osClusterName);
            DescribeDomainResult describeDomainResult = osProvisionedClient.describeDomain(describeDomainRequest);
            String publicOSEndpoint = describeDomainResult.getDomainStatus().getEndpoint();
            Map<String, String> vpcOSEndpointsMap = describeDomainResult.getDomainStatus().getEndpoints();
            if (publicOSEndpoint != null) {
                openSearchEndpointURL = publicOSEndpoint;
            } else if (vpcOSEndpointsMap != null && vpcOSEndpointsMap.containsKey("vpc")) {
                openSearchEndpointURL = vpcOSEndpointsMap.get("vpc");
            } else {
                throw new RuntimeException("Cannot find endpoint URL for OpenSearch provisioned cluster: " + osClusterName);
            }
        } else if (osClusterType == OpenSearchType.SERVERLESS) {
//            BatchGetCollectionRequest batchGetCollectionRequest = new BatchGetCollectionRequest().withNames(List.of(osClusterName));
//            BatchGetCollectionResult batchGetCollectionResult = osServerlessClient.batchGetCollection(batchGetCollectionRequest);
//            try {
//                openSearchEndpointURL = batchGetCollectionResult.getCollectionDetails().get(0).getCollectionEndpoint();
//            } catch (NoSuchElementException e) {
//                throw new RuntimeException("Provided OpenSearch cluster " + osClusterName + " does not exist:", e);
//            }
            System.out.println("osClusterType is SERVERLESS");
        } else {
            throw new RuntimeException("Got unrecognized OpenSearch cluster type: " + osClusterType);
        }

        return OpenSearchClusterData.builder()
                .OpenSearchCollectionName(osClusterName)
                .OpenSearchType(osClusterType.toString())
                .OpenSearchEndpointURL(openSearchEndpointURL)
                .OpenSearchVectorIndexName(this.buildTestVectorName())
                .build();
    }

    private String buildTestVectorName() {
        return "integ-test-index-" + testId;
    }
}
