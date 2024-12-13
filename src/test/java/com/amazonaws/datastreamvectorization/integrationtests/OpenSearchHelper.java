package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.datasink.opensearch.OpenSearchSinkBuilder;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.opensearch.AmazonOpenSearch;
import com.amazonaws.services.opensearch.AmazonOpenSearchClientBuilder;
import com.amazonaws.services.opensearch.model.*;

import com.amazonaws.services.opensearchserverless.AWSOpenSearchServerless;
import com.amazonaws.services.opensearchserverless.AWSOpenSearchServerlessClientBuilder;
import com.amazonaws.services.opensearchserverless.model.BatchGetCollectionRequest;
import com.amazonaws.services.opensearchserverless.model.BatchGetCollectionResult;
import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import org.apache.flink.connector.opensearch.sink.RestClientFactory;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.regions.Region;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class OpenSearchHelper {
    AmazonOpenSearch osProvisionedClient;
    AWSOpenSearchServerless osServerlessClient;
    String region;

    public OpenSearchHelper() {
        AwsRegionProvider regionProvider = new DefaultAwsRegionProviderChain();
        region = regionProvider.getRegion();

        osProvisionedClient = AmazonOpenSearchClientBuilder.defaultClient();
//        osServerlessClient = AWSOpenSearchServerlessClientBuilder.defaultClient();
    }

    public OpenSearchClusterData getOpenSearchClusterData(String osClusterName, OpenSearchType osClusterType, String testId) {
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
                .OpenSearchVectorIndexName(this.buildTestVectorName(testId))
                .build();
    }

    public String getCrossVpcEndpoint(String osClusterName, OpenSearchType osClusterType, String crossVpcId) {
        if (osClusterType != OpenSearchType.PROVISIONED) {
            throw new RuntimeException("Can only get cross VPC endpoint for PROVISIONED OpenSearch clusters.");
        }
        try {
            ListVpcEndpointsForDomainRequest listVpcEndpointsForDomainRequest = new ListVpcEndpointsForDomainRequest()
                    .withDomainName(osClusterName);
            ListVpcEndpointsForDomainResult listVpcEndpointsForDomainResult = osProvisionedClient.listVpcEndpointsForDomain(listVpcEndpointsForDomainRequest);
            List<VpcEndpointSummary> vpcEndpointSummaries = listVpcEndpointsForDomainResult.getVpcEndpointSummaryList();
            for (VpcEndpointSummary vpceSummary : vpcEndpointSummaries) {
                String vpceId = vpceSummary.getVpcEndpointId();
                DescribeVpcEndpointsRequest describeVpcEndpointsRequest = new DescribeVpcEndpointsRequest().withVpcEndpointIds(vpceId);
                DescribeVpcEndpointsResult describeVpcEndpointsResult = osProvisionedClient.describeVpcEndpoints(describeVpcEndpointsRequest);
                VpcEndpoint vpcEndpoint = describeVpcEndpointsResult.getVpcEndpoints().get(0);
                if (vpcEndpoint.getVpcOptions().getVPCId().equals(crossVpcId)) {
                    return vpcEndpoint.getEndpoint();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error looking for cross VPC endpoint belonging to " + osClusterType + ": ", e);
        }
        throw new RuntimeException("Did not find a cross VPC endpoint belonging to " + osClusterName +
                " for VPC " + crossVpcId);
    }

    private String buildTestVectorName(String testId) {
        return "integ-test-index-" + testId;
    }



    public void createIndex(OpenSearchType openSearchType, String indexName) {

//        HttpRequestInterceptor interceptor = new AwsRequestSigningApacheInterceptor(
//                openSearchType.getServiceName(),
//                Aws4Signer.create(),
//                DefaultCredentialsProvider.create(),
//                Region.of(this.region));
//
//        RestClientBuilder restClientBuilder = new RestClientBuilder.HttpClientConfigCallback();
//
//        RestClientBuilder.setHttpClientConfigCallback(
//                httpAsyncClientBuilder -> httpAsyncClientBuilder.addInterceptorLast(interceptor));
//
//        RestClientFactory restClientFactory = OpenSearchSinkBuilder
//                .getRestClientFactory(openSearchType.getServiceName(), this.region);
//
//        restClientFactory.configureRestClientBuilder();
//
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new AWSOpen);
//
//        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "https"))
//                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                    @Override
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    }
//                });
//        RestHighLevelClient client = new RestHighLevelClient(builder);
//        client.se
    }

    public void addMasterUserIAMRole(String iamRoleName) {
//        this.osProvisionedClient.
    }

    public void queryIndexRecords(String indexName, int startTimestamp) {

    }


}
