package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleResult;
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

    public OpenSearchHelper() {}

    /**
     * Prepares the OpenSearch cluster data needed to deploy the blueprint CDK stack.
     *
     * @param osClusterName Name of the OpenSearch cluster
     * @param osClusterType Type of the OpenSearch cluster (PROVISIONED vs. SERVERLESS)
     * @param testId ID string for the test
     * @return OpenSearchClusterBlueprintData containing required blueprint OpenSearch parameters
     */
    public OpenSearchClusterBlueprintData getOpenSearchClusterBlueprintData(String osClusterName, OpenSearchType osClusterType, String testId) {
        String openSearchEndpointURL;
        // gets the OpenSearchEndpointURL parameter for blueprint deployment
        if (osClusterType == OpenSearchType.PROVISIONED) {
            AmazonOpenSearch osProvisionedClient = AmazonOpenSearchClientBuilder.defaultClient();
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
            AWSOpenSearchServerless osServerlessClient = AWSOpenSearchServerlessClientBuilder.defaultClient();
            BatchGetCollectionRequest batchGetCollectionRequest = new BatchGetCollectionRequest().withNames(List.of(osClusterName));
            BatchGetCollectionResult batchGetCollectionResult = osServerlessClient.batchGetCollection(batchGetCollectionRequest);
            try {
                openSearchEndpointURL = batchGetCollectionResult.getCollectionDetails().get(0).getCollectionEndpoint();
            } catch (NoSuchElementException e) {
                throw new RuntimeException("Provided OpenSearch cluster " + osClusterName + " does not exist:", e);
            }
        } else {
            throw new RuntimeException("Got unrecognized OpenSearch cluster type: " + osClusterType);
        }

        // build and return OpenSearchClusterBlueprintData
        return OpenSearchClusterBlueprintData.builder()
                .OpenSearchCollectionName(osClusterName)
                .OpenSearchType(osClusterType.toString())
                .OpenSearchEndpointURL(openSearchEndpointURL)
                .OpenSearchVectorIndexName(this.buildTestVectorIndexName(testId))
                .build();
    }

    /**
     * Finds the VPC endpoint belonging to the given OpenSearch domain that connects to the
     * given VPC ID. Purpose of this method is to find VPC endpoints for a VPC that is different
     * from the VPC the OpenSearch domain is in.
     * Only intended for provisioned OpenSearch domain. Will fail for serverless OpenSearch collections.
     *
     * @param osClusterName Name of the OpenSearch domain that the VPC endpoint belongs to.
     * @param crossVpcId VPC ID of the VPC that the VPC endpoint should connect to.
     * @return VPC endpoint ID if found, else throws error.
     */
    public String getCrossVpcEndpoint(String osClusterName, String crossVpcId) {
        try {
            AmazonOpenSearch osProvisionedClient = AmazonOpenSearchClientBuilder.defaultClient();
            // get all VPC endpoints for the OpenSearch domain
            ListVpcEndpointsForDomainRequest listVpcEndpointsForDomainRequest = new ListVpcEndpointsForDomainRequest()
                    .withDomainName(osClusterName);
            ListVpcEndpointsForDomainResult listVpcEndpointsForDomainResult = osProvisionedClient
                    .listVpcEndpointsForDomain(listVpcEndpointsForDomainRequest);
            List<VpcEndpointSummary> vpcEndpointSummaries = listVpcEndpointsForDomainResult.getVpcEndpointSummaryList();
            // go through the found VPCE's and return the one with VPC ID matching crossVpcId
            for (VpcEndpointSummary vpceSummary : vpcEndpointSummaries) {
                String vpceId = vpceSummary.getVpcEndpointId();
                DescribeVpcEndpointsRequest describeVpcEndpointsRequest = new DescribeVpcEndpointsRequest()
                        .withVpcEndpointIds(vpceId);
                DescribeVpcEndpointsResult describeVpcEndpointsResult = osProvisionedClient
                        .describeVpcEndpoints(describeVpcEndpointsRequest);
                VpcEndpoint vpcEndpoint = describeVpcEndpointsResult.getVpcEndpoints().get(0);
                if (vpcEndpoint.getVpcOptions().getVPCId().equals(crossVpcId)) {
                    return vpcEndpoint.getEndpoint();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error looking for cross VPC endpoint belonging to " + osClusterName + ": ", e);
        }
        throw new RuntimeException("Did not find a cross VPC endpoint belonging to " + osClusterName +
                " for VPC " + crossVpcId);
    }

    /**
     * Adds an IAM role as a master user to the provided OpenSearch domain.
     *
     * @param osDomainName The OpenSearch provisioned domain name
     * @param iamRoleName The IAM role name
     */
    public void addMasterUserIAMRole(String osDomainName, String iamRoleName) {
        // get the IAM role ARN to add as master user
        AmazonIdentityManagement iamClient = AmazonIdentityManagementClientBuilder.defaultClient();
        GetRoleRequest getRoleRequest = new GetRoleRequest().withRoleName(iamRoleName);
        GetRoleResult getRoleResult = iamClient.getRole(getRoleRequest);
        String iamRoleARN = getRoleResult.getRole().getArn();

        // add the master user
        AmazonOpenSearch osProvisionedClient = AmazonOpenSearchClientBuilder.defaultClient();
        MasterUserOptions masterUserOptions = new MasterUserOptions().withMasterUserARN(iamRoleARN);
        AdvancedSecurityOptionsInput advancedSecurityOptionsInput = new AdvancedSecurityOptionsInput()
                .withMasterUserOptions(masterUserOptions);
        UpdateDomainConfigRequest updateDomainConfigRequest = new UpdateDomainConfigRequest()
                .withDomainName(osDomainName)
                .withAdvancedSecurityOptions(advancedSecurityOptionsInput);
        osProvisionedClient.updateDomainConfig(updateDomainConfigRequest);
    }

    /**
     * Builds the OpenSearch vector index name to use for the test.
     *
     * @param testId ID string for the test
     * @return Index name string
     */
    private String buildTestVectorIndexName(String testId) {
        return "integ-test-index-" + testId;
    }
}
