package com.amazonaws.datastreamvectorization.integrationtests;

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
    String testId;

    public MSKHelper(String testId) {
        this.mskClient = AWSKafkaClientBuilder.defaultClient();
        this.testId = testId;
    }

    public String getBootstrapBrokers(String mskClusterArn) {
        GetBootstrapBrokersRequest bookstrapBrokersRequest = new GetBootstrapBrokersRequest();
        GetBootstrapBrokersResult bookstrapBrokersResult = this.mskClient.getBootstrapBrokers(bookstrapBrokersRequest.withClusterArn(mskClusterArn));
        System.out.println("MSK cluster bootstrap server result: " + bookstrapBrokersResult);
        String mskClusterBootstrapBrokerString = bookstrapBrokersResult.getBootstrapBrokerStringSaslIam();
        System.out.println("MSK cluster bootstrap server string: " + mskClusterBootstrapBrokerString);
        return mskClusterBootstrapBrokerString;
    }

    public MSKClusterData getMSKClusterData(String mskClusterArn) {
        DescribeClusterV2Request describeClusterV2Request = new DescribeClusterV2Request().withClusterArn(mskClusterArn);
        DescribeClusterV2Result describeClusterV2Result = this.mskClient.describeClusterV2(describeClusterV2Request);

        Cluster clusterInfo = describeClusterV2Result.getClusterInfo();
        String mskClusterName = clusterInfo.getClusterName();

        Provisioned provisionedClusterInfo = describeClusterV2Result.getClusterInfo().getProvisioned();

        List<String> mskSubnetIDs = provisionedClusterInfo.getBrokerNodeGroupInfo().getClientSubnets();
        List<String> mskSecurityGroupIDs = provisionedClusterInfo.getBrokerNodeGroupInfo().getSecurityGroups();
        String mskVpcId = this.getVpcIdFromSubnets(mskSubnetIDs);

        // TODO: SERVERLESS version. Consider passing VPC information from the test inputs instead
//        Serverless serverlessClusterInfo = describeClusterV2Result.getClusterInfo().getServerless();
//
//        List<String> mskSubnetIDs = serverlessClusterInfo.getVpcConfigs().getFirst().getSubnetIds();
//        List<String> mskSecurityGroupIDs = serverlessClusterInfo.getVpcConfigs().getFirst().getSecurityGroupIds();
//        String mskVpcId = this.getVpcIdFromSubnets(mskSubnetIDs);


        return MSKClusterData.builder()
                .MSKClusterArn(mskClusterArn)
                .MSKClusterName(mskClusterName)
                .MSKTopics(this.buildTestTopicNames())
                .MSKVpcId(mskVpcId)
                .MSKClusterSubnetIds(String.join(",", mskSubnetIDs))
                .MSKClusterSecurityGroupIds(String.join(",", mskSecurityGroupIDs))
                .build();
    }

    public String buildTestTopicNames() {
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
