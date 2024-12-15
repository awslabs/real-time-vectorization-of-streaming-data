package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
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

public class CloudFormationHelper {
    AmazonCloudFormation cfnClient;
    String testId;
    private final int MAX_POLL_STACK_STATUS_RETRIES = 15;
    private final Long POLL_STACK_STATUS_DELAY = 60000L; // 1 minute
    private final List<StackStatus> TERMINAL_STACK_STATUSES = List.of(
            StackStatus.CREATE_COMPLETE,
            StackStatus.CREATE_FAILED,
            StackStatus.DELETE_COMPLETE,
            StackStatus.DELETE_FAILED,
            StackStatus.ROLLBACK_COMPLETE,
            StackStatus.ROLLBACK_FAILED
    );

    public CloudFormationHelper(String testId) {
        cfnClient = AmazonCloudFormationClientBuilder.defaultClient();
        this.testId = testId;
    }

    public String buildStackAppName() {
        return "integ-test-app-" + testId;
    }

    public String buildStackRoleName() {
        return "integ-test-app-" + testId + "-role";
    }

    private String buildStackLogGroupName() {
        return "integ-test-app-" + testId + "-log-group";
    }

    private String buildStackLogStreamName() {
        return "integ-test-app-" + testId + "-log-stream";
    }

    private String buildStackAssetBucketName() {
        return "integ-test-app-" + testId + "-bucket";
    }

    private List<Parameter> getBlueprintParameters(String mskClusterArn, String osClusterName, OpenSearchType osClusterType) {
        MSKHelper mskHelper = new MSKHelper(testId);
        MSKClusterData mskClusterData = mskHelper.getMSKClusterData(mskClusterArn);
        System.out.println("mskClusterData:");
        System.out.println(mskClusterData);

        OpenSearchHelper osHelper = new OpenSearchHelper();
        OpenSearchClusterData osClusterData = osHelper.getOpenSearchClusterData(osClusterName, osClusterType, testId);
        System.out.println("osClusterData:");
        System.out.println(osClusterData);

        BedrockHelper bedrockHelper = new BedrockHelper();
        EmbeddingModel embeddingModel = bedrockHelper.getSupportedEmbeddingModel();
        System.out.println("embeddingModel:");
        System.out.println(embeddingModel.getModelId());

        return List.of(
            new Parameter().withParameterKey("SourceType").withParameterValue("MSK"),
            new Parameter().withParameterKey("SourceDataType").withParameterValue("STRING"),
            new Parameter().withParameterKey("SinkType").withParameterValue("OPENSEARCH"),
            new Parameter().withParameterKey("MSKClusterName").withParameterValue(mskClusterData.MSKClusterName),
            new Parameter().withParameterKey("MSKClusterArn").withParameterValue(mskClusterData.MSKClusterArn),
            new Parameter().withParameterKey("MSKClusterSubnetIds").withParameterValue(mskClusterData.MSKClusterSubnetIds),
            new Parameter().withParameterKey("MSKClusterSecurityGroupIds").withParameterValue(mskClusterData.MSKClusterSecurityGroupIds),
            new Parameter().withParameterKey("MSKTopics").withParameterValue(mskClusterData.MSKTopics),
            new Parameter().withParameterKey("MSKVpcId").withParameterValue(mskClusterData.MSKVpcId),
            new Parameter().withParameterKey("OpenSearchCollectionName").withParameterValue(osClusterData.OpenSearchCollectionName),
            new Parameter().withParameterKey("OpenSearchEndpointURL").withParameterValue(osClusterData.OpenSearchEndpointURL),
            new Parameter().withParameterKey("OpenSearchType").withParameterValue(osClusterData.OpenSearchType),
            new Parameter().withParameterKey("OpenSearchVectorIndexName").withParameterValue(osClusterData.OpenSearchVectorIndexName),
            new Parameter().withParameterKey("EmbeddingModelName").withParameterValue(embeddingModel.getModelId()),
            new Parameter().withParameterKey("JsonKeysToEmbed").withParameterValue(".*"),
            new Parameter().withParameterKey("AppName").withParameterValue(this.buildStackAppName()),
            new Parameter().withParameterKey("RuntimeEnvironment").withParameterValue("FLINK-1_19"),
            new Parameter().withParameterKey("RoleName").withParameterValue(this.buildStackRoleName()),
            new Parameter().withParameterKey("CloudWatchLogGroupName").withParameterValue(this.buildStackLogGroupName()),
            new Parameter().withParameterKey("CloudWatchLogStreamName").withParameterValue(this.buildStackLogStreamName()),
            new Parameter().withParameterKey("AssetBucket").withParameterValue(this.buildStackAssetBucketName()),
            new Parameter().withParameterKey("JarFile").withParameterValue("data-stream-vectorization-1.0-SNAPSHOT.jar"),
            new Parameter().withParameterKey("AssetList").withParameterValue("https://github.com/awslabs/real-time-vectorization-of-streaming-data/releases/download/0.1-SNAPSHOT/data-stream-vectorization-1.0-SNAPSHOT.jar")
        );
    }

    public Stack pollBlueprintStatusStatus(String stackName) throws InterruptedException {
        int retryCount = 0;
        String stackStatus;
        Stack stack = null;
        while (retryCount++ <= MAX_POLL_STACK_STATUS_RETRIES) {
            DescribeStacksRequest describeStacksRequest = new DescribeStacksRequest().withStackName(stackName);
            DescribeStacksResult describeStacksResult = cfnClient.describeStacks(describeStacksRequest);
            stack = describeStacksResult.getStacks().get(0);
            stackStatus = stack.getStackStatus();
            if (TERMINAL_STACK_STATUSES.contains(StackStatus.fromValue(stackStatus))) {
                return stack;
            }
            Thread.sleep(POLL_STACK_STATUS_DELAY);
        }
        return stack;
    }

    public Stack createBlueprintStack(String templateURL, String mskClusterArn, String osClusterName, OpenSearchType osClusterType) {
        String stackName = "datastream-vec-integ-test-" + testId;

        try {
            System.out.println("Stack template URL: " + templateURL);
            List<Parameter> stackParameters = getBlueprintParameters(mskClusterArn, osClusterName, osClusterType);

            CreateStackRequest createStackRequest = new CreateStackRequest()
                    .withTemplateURL(templateURL)
                    .withStackName(stackName)
                    .withParameters(stackParameters)
                    .withCapabilities(Capability.CAPABILITY_NAMED_IAM);
            cfnClient.createStack(createStackRequest);

            Stack stack = this.pollBlueprintStatusStatus(stackName);
            if (stack == null) {
                throw new RuntimeException("Failed to create blueprint stack " + stackName);
            }
            if (!stack.getStackStatus().equals(StackStatus.CREATE_COMPLETE.toString())) {
                throw new RuntimeException("Create blueprint stack ended with unsuccessful status: " + stack);
            }
            return stack;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create blueprint stack " + stackName, e);
        }
    }

    public void deleteBlueprintStack(String stackName) {
        this.blueprintStackCleanup(stackName);
        DeleteStackRequest deleteStackRequest = new DeleteStackRequest().withStackName(stackName);
        cfnClient.deleteStack(deleteStackRequest);
    }

    public void blueprintStackCleanup(String stackName) {
        DescribeStacksRequest describeStacksRequest = new DescribeStacksRequest().withStackName(stackName);
        DescribeStacksResult describeStacksResults = cfnClient.describeStacks(describeStacksRequest);
        List<Stack> stacks = describeStacksResults.getStacks();
        if (stacks.size() != 1) {
            throw new RuntimeException("Expected number of stacks is 1, got: " + stacks.size());
        }
        Stack stackToCleanup = stacks.get(0);

        // TODO: get parameter describing OpenSearch cluster type, or take it in as input
//        List<Parameter> parameters = stackToCleanup.();
//        Parameter test = parameters.getFirst();
//        test.getParameterKey()
        String opensearchType = "PROVISIONED";

        List<Output> outputs = stackToCleanup.getOutputs();
        for (Output output : outputs) {
            String outputKey = output.getOutputKey();
            String outputValue = output.getOutputValue();
            String vpceId = "";

            if (outputKey.equals("BedrockVpcEndpoint")) {
                vpceId = getBedrockVpceToDelete(outputValue);

                // TODO: delete if Bedrock endpoint
                AmazonEC2 ec2Client = AmazonEC2ClientBuilder.defaultClient();
                DeleteVpcEndpointsRequest deleteVpcEndpointsRequest = new DeleteVpcEndpointsRequest().withVpcEndpointIds(vpceId);
                DeleteVpcEndpointsResult deleteVpcEndpointsResult = ec2Client.deleteVpcEndpoints(deleteVpcEndpointsRequest);
            } else if (outputKey.equals("OpenSearchVpcEndpoint")) {
                vpceId = getOpenSearchVpceToDelete(outputValue);

                if (opensearchType.equals("PROVISIONED")) {
                    // TODO: delete if Opensearch Provisioned endpoint
                    AmazonOpenSearch opensearchClient = AmazonOpenSearchClientBuilder.defaultClient();
                    DeleteVpcEndpointRequest deleteVpcEndpointRequest = new DeleteVpcEndpointRequest().withVpcEndpointId(vpceId);
                    DeleteVpcEndpointResult deleteVpcEndpointResult = opensearchClient.deleteVpcEndpoint(deleteVpcEndpointRequest);
                } else {
                    // TODO: delete if Opensearch Serverless endpoint
                    AWSOpenSearchServerless openSearchClient = AWSOpenSearchServerlessClientBuilder.defaultClient();
                    com.amazonaws.services.opensearchserverless.model.DeleteVpcEndpointRequest deleteVpcEndpointRequest =
                            new com.amazonaws.services.opensearchserverless.model.DeleteVpcEndpointRequest().withId(vpceId);
                    com.amazonaws.services.opensearchserverless.model.DeleteVpcEndpointResult deleteVpcEndpointResult =
                            openSearchClient.deleteVpcEndpoint(deleteVpcEndpointRequest);
                }
            }
        }
    }

    private String getBedrockVpceToDelete(String outputValue) {
        Pattern pattern = Pattern.compile("Bedrock VPC endpoint ID on stack creation: (?<vpceID>[a-z0-9\\-]+) \\| Was created by this stack: (?<vpceBoolean>(True|False))");
        Matcher matcher = pattern.matcher(outputValue);

        if (matcher.find()) {
            String vpceID = matcher.group("vpceID");
            boolean vpceBoolean = Boolean.getBoolean(matcher.group("vpceBoolean"));
            // TODO: Remove prints later
            System.out.println("BEDROCK vpceID: " + vpceID);
            System.out.println("BEDROCK vpceBoolean: " + vpceBoolean);
            if (vpceBoolean) {
                return vpceID;
            } else {
                return "";
            }
        } else {
            throw new RuntimeException("Unexpected output value structure when parsing stack output for Bedrock VPCEs: " + outputValue);
        }
    }

    private String getOpenSearchVpceToDelete(String outputValue) {
        Pattern pattern = Pattern.compile("OpenSearch VPC endpoint ID(s) on stack creation: (?<vpceID>[a-z0-9\\-,]+) \\| Was created by this stack: (?<vpceBoolean>(True|False))");
        Matcher matcher = pattern.matcher(outputValue);

        if (matcher.find()) {
            String vpceID = matcher.group("vpceID");
            boolean vpceBoolean = Boolean.getBoolean(matcher.group("vpceBoolean"));
            // TODO: Remove prints later
            System.out.println("OPENSEARCH vpceID: " + vpceID);
            System.out.println("OPENSEARCH vpceBoolean: " + vpceBoolean);
            if (vpceBoolean) {
                return vpceID;
            } else {
                return "";
            }
        } else {
            throw new RuntimeException("Unexpected output value structure when parsing stack output for OpenSearch VPCEs: " + outputValue);
        }
    }

}
