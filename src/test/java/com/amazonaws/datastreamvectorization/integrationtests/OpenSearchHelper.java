package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.services.opensearch.AmazonOpenSearch;
import com.amazonaws.services.opensearch.AmazonOpenSearchClientBuilder;
import com.amazonaws.services.opensearch.model.*;

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

    public OpenSearchHelper() {
//        String region = AmazonOpenSearchClientBuilder.standard().getRegion();

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
}
