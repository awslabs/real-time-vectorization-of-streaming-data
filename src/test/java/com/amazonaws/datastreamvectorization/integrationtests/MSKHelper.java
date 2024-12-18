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

    public MSKClusterBlueprintParameters getMSKClusterBlueprintParameters(String mskClusterArn, String testId) {
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
                .MSKTopics(this.buildTestTopicName(testId))
                .MSKVpcId(mskVpcId)
                .MSKClusterSubnetIds(String.join(",", mskSubnetIDs))
                .MSKClusterSecurityGroupIds(String.join(",", mskSecurityGroupIDs))
                .build();
    }

    public String buildTestTopicName(String testId) {
        return "integ-test-topic-" + testId;
    }

    private String getVpcIdFromSubnets(List<String> subnetIds) {
        AmazonEC2 ec2Client = AmazonEC2ClientBuilder.defaultClient();
        DescribeSubnetsRequest describeSubnetsRequest = new DescribeSubnetsRequest().withSubnetIds(subnetIds);
        DescribeSubnetsResult describeSubnetsResult = ec2Client.describeSubnets(describeSubnetsRequest);
        List<Subnet> subnets = describeSubnetsResult.getSubnets();
        return subnets.get(0).getVpcId(); // TODO: error handling if empty / not all subnets have same VPC
    }
}
