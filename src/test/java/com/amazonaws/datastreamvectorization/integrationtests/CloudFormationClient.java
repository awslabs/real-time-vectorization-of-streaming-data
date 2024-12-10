package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.*;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointsRequest;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointsResult;
import com.amazonaws.services.opensearch.AmazonOpenSearch;
import com.amazonaws.services.opensearch.AmazonOpenSearchClientBuilder;
import com.amazonaws.services.opensearch.model.DeleteVpcEndpointRequest;
import com.amazonaws.services.opensearch.model.DeleteVpcEndpointResult;
import com.amazonaws.services.opensearchserverless.AWSOpenSearchServerless;
import com.amazonaws.services.opensearchserverless.AWSOpenSearchServerlessClientBuilder;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CloudFormationClient {
    AmazonCloudFormation cfnClient;

    public CloudFormationClient() {
        cfnClient = AmazonCloudFormationClientBuilder.defaultClient();
    }

    public CreateStackResult createStack(String templateURL, String testName) {
        String stackName = "datastream-vec-integ-test-" + testName;
        CreateStackRequest createStackRequest = new CreateStackRequest()
                .withTemplateURL(templateURL)
                .withStackName(stackName);
        return cfnClient.createStack(createStackRequest);
    }

//    public DeleteStackResult deleteStack(String stackName) {
//        this.stackCleanup(stackName);
//        DeleteStackRequest deleteStackRequest = new DeleteStackRequest().withStackName(stackName);
//        return cfnClient.deleteStack(deleteStackRequest);
//    }
//
//    public void stackCleanup(String stackName) {
//        DescribeStacksRequest describeStacksRequest = new DescribeStacksRequest().withStackName(stackName);
//        DescribeStacksResult describeStacksResults = cfnClient.describeStacks(describeStacksRequest);
//        List<Stack> stacks = describeStacksResults.getStacks();
//        if (stacks.size() != 1) {
//            throw new RuntimeException("Expected number of stacks is 1, got: " + stacks.size());
//        }
//        Stack stackToCleanup = stacks.getFirst();
//
//        // TODO: get parameter describing OpenSearch cluster type, or take it in as input
////        List<Parameter> parameters = stackToCleanup.();
////        Parameter test = parameters.getFirst();
////        test.getParameterKey()
//        String opensearchType = "PROVISIONED";
//
//        List<Output> outputs = stackToCleanup.getOutputs();
//        for (Output output : outputs) {
//            String outputKey = output.getOutputKey();
//            String outputValue = output.getOutputValue();
//            String vpceId = "";
//
//            if (outputKey.equals("BedrockVpcEndpoint")) {
//                vpceId = getBedrockVpceToDelete(outputValue);
//
//                // TODO: delete if Bedrock endpoint
//                AmazonEC2 ec2Client = AmazonEC2ClientBuilder.defaultClient();
//                DeleteVpcEndpointsRequest deleteVpcEndpointsRequest = new DeleteVpcEndpointsRequest().withVpcEndpointIds(vpceId);
//                DeleteVpcEndpointsResult deleteVpcEndpointsResult = ec2Client.deleteVpcEndpoints(deleteVpcEndpointsRequest);
//            } else if (outputKey.equals("OpenSearchVpcEndpoint")) {
//                vpceId = getOpenSearchVpceToDelete(outputValue);
//
//                if (opensearchType.equals("PROVISIONED")) {
//                    // TODO: delete if Opensearch Provisioned endpoint
//                    AmazonOpenSearch opensearchClient = AmazonOpenSearchClientBuilder.defaultClient();
//                    DeleteVpcEndpointRequest deleteVpcEndpointRequest = new DeleteVpcEndpointRequest().withVpcEndpointId(vpceId);
//                    DeleteVpcEndpointResult deleteVpcEndpointResult = opensearchClient.deleteVpcEndpoint(deleteVpcEndpointRequest);
//                } else {
//                    // TODO: delete if Opensearch Serverless endpoint
//                    AWSOpenSearchServerless openSearchClient = AWSOpenSearchServerlessClientBuilder.defaultClient();
//                    com.amazonaws.services.opensearchserverless.model.DeleteVpcEndpointRequest deleteVpcEndpointRequest =
//                            new com.amazonaws.services.opensearchserverless.model.DeleteVpcEndpointRequest().withId(vpceId);
//                    com.amazonaws.services.opensearchserverless.model.DeleteVpcEndpointResult deleteVpcEndpointResult =
//                            openSearchClient.deleteVpcEndpoint(deleteVpcEndpointRequest);
//                }
//            }
//        }
//    }
//
//    private String getBedrockVpceToDelete(String outputValue) {
//        Pattern pattern = Pattern.compile("Bedrock VPC endpoint ID on stack creation: (?<vpceID>[a-z0-9\\-]+) \\| Was created by this stack: (?<vpceBoolean>(True|False))");
//        Matcher matcher = pattern.matcher(outputValue);
//
//        if (matcher.find()) {
//            String vpceID = matcher.group("vpceID");
//            boolean vpceBoolean = Boolean.getBoolean(matcher.group("vpceBoolean"));
//            // TODO: Remove prints later
//            System.out.println("BEDROCK vpceID: " + vpceID);
//            System.out.println("BEDROCK vpceBoolean: " + vpceBoolean);
//            if (vpceBoolean) {
//                return vpceID;
//            } else {
//                return "";
//            }
//        } else {
//            throw new RuntimeException("Unexpected output value structure when parsing stack output for Bedrock VPCEs: " + outputValue);
//        }
//    }
//
//    private String getOpenSearchVpceToDelete(String outputValue) {
//        Pattern pattern = Pattern.compile("OpenSearch VPC endpoint ID(s) on stack creation: (?<vpceID>[a-z0-9\\-,]+) \\| Was created by this stack: (?<vpceBoolean>(True|False))");
//        Matcher matcher = pattern.matcher(outputValue);
//
//        if (matcher.find()) {
//            String vpceID = matcher.group("vpceID");
//            boolean vpceBoolean = Boolean.getBoolean(matcher.group("vpceBoolean"));
//            // TODO: Remove prints later
//            System.out.println("OPENSEARCH vpceID: " + vpceID);
//            System.out.println("OPENSEARCH vpceBoolean: " + vpceBoolean);
//            if (vpceBoolean) {
//                return vpceID;
//            } else {
//                return "";
//            }
//        } else {
//            throw new RuntimeException("Unexpected output value structure when parsing stack output for OpenSearch VPCEs: " + outputValue);
//        }
//    }

}
