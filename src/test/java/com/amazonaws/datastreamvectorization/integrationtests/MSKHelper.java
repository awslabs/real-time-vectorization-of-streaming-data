package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.integrationtests.model.MSKClusterBlueprintParameters;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.kafka.AWSKafka;
import com.amazonaws.services.kafka.AWSKafkaClientBuilder;
import com.amazonaws.services.kafka.model.*;

import java.util.List;

/**
 * Helper class to interact with Amazon MSK
 */
public class MSKHelper {
    AWSKafka mskClient;

    public MSKHelper() {
        this.mskClient = AWSKafkaClientBuilder.defaultClient();
    }

    /**
     * Get bootstrap brokers for the provided MSK cluster
     *
     * @param mskClusterArn ARN of the MSK cluster
     * @return Bootstrap brokers string for connecting to the cluster
     */
    public String getBootstrapBrokers(String mskClusterArn) {
        GetBootstrapBrokersRequest bookstrapBrokersRequest = new GetBootstrapBrokersRequest();
        GetBootstrapBrokersResult bookstrapBrokersResult = this.mskClient.getBootstrapBrokers(bookstrapBrokersRequest.withClusterArn(mskClusterArn));
        return bookstrapBrokersResult.getBootstrapBrokerStringSaslIam();
    }

    /**
     * Prepares the MSK cluster data needed to deploy the blueprint CDK stack.
     * @param mskClusterArn MSK cluster ARN
     * @param testID ID string for the test
     * @return MSKClusterBlueprintParameters containing required blueprint MSK parameters
     */
    public MSKClusterBlueprintParameters getMSKClusterBlueprintParameters(String mskClusterArn, String testID) {
        DescribeClusterV2Request describeClusterV2Request = new DescribeClusterV2Request().withClusterArn(mskClusterArn);
        DescribeClusterV2Result describeClusterV2Result = this.mskClient.describeClusterV2(describeClusterV2Request);

        Cluster clusterInfo = describeClusterV2Result.getClusterInfo();
        String mskClusterName = clusterInfo.getClusterName();

        Provisioned provisionedClusterInfo = describeClusterV2Result.getClusterInfo().getProvisioned();
        Serverless serverlessClusterInfo = describeClusterV2Result.getClusterInfo().getServerless();
        List<String> mskSubnetIDs;
        List<String> mskSecurityGroupIDs;
        String mskVpcId;

        if (provisionedClusterInfo != null) {
            mskSubnetIDs = provisionedClusterInfo.getBrokerNodeGroupInfo().getClientSubnets();
            mskSecurityGroupIDs = provisionedClusterInfo.getBrokerNodeGroupInfo().getSecurityGroups();
            mskVpcId = this.getVpcIdFromSubnets(mskSubnetIDs);
        } else if (serverlessClusterInfo != null) {
            mskSubnetIDs = serverlessClusterInfo.getVpcConfigs().get(0).getSubnetIds();
            mskSecurityGroupIDs = serverlessClusterInfo.getVpcConfigs().get(0).getSecurityGroupIds();
            mskVpcId = this.getVpcIdFromSubnets(mskSubnetIDs);
        } else {
            throw new RuntimeException("MSK cluster not of type provisioned or serverless. " +
                    "DescribeClusterV2Result for cluster " + mskClusterArn + "was: " + describeClusterV2Result);
        }

        return MSKClusterBlueprintParameters.builder()
                .MSKClusterArn(mskClusterArn)
                .MSKClusterName(mskClusterName)
                .MSKTopics(this.buildTestTopicName(testID))
                .MSKVpcId(mskVpcId)
                .MSKClusterSubnetIds(String.join(",", mskSubnetIDs))
                .MSKClusterSecurityGroupIds(String.join(",", mskSecurityGroupIDs))
                .build();
    }

    /**
     * Builds the MSK topic name to use for the test.
     *
     * @param testID ID string for the test
     * @return Topic name for the test
     */
    public String buildTestTopicName(String testID) {
        return "integ-test-topic-" + testID;
    }

    /**
     * Gets the VPC ID from a list of subnets.
     *
     * @param subnetIds List of subnet IDs
     * @return VPC ID
     */
    private String getVpcIdFromSubnets(List<String> subnetIds) {
        AmazonEC2 ec2Client = AmazonEC2ClientBuilder.defaultClient();
        DescribeSubnetsRequest describeSubnetsRequest = new DescribeSubnetsRequest().withSubnetIds(subnetIds);
        DescribeSubnetsResult describeSubnetsResult = ec2Client.describeSubnets(describeSubnetsRequest);
        List<Subnet> subnets = describeSubnetsResult.getSubnets();
        return subnets.get(0).getVpcId();
    }
}
