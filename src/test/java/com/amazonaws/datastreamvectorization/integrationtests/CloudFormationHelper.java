package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.datastreamvectorization.integrationtests.model.MSKClusterBlueprintParameters;
import com.amazonaws.datastreamvectorization.integrationtests.model.MskClusterConfig;
import com.amazonaws.datastreamvectorization.integrationtests.model.OpenSearchClusterBlueprintParameters;
import com.amazonaws.datastreamvectorization.integrationtests.model.OpenSearchClusterConfig;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.*;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointsRequest;
import com.amazonaws.services.opensearch.AmazonOpenSearch;
import com.amazonaws.services.opensearch.AmazonOpenSearchClientBuilder;
import com.amazonaws.services.opensearchserverless.AWSOpenSearchServerless;
import com.amazonaws.services.opensearchserverless.AWSOpenSearchServerlessClientBuilder;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.datastreamvectorization.integrationtests.constants.IntegTestConstants.BlueprintParameterKeys.*;

/**
 * Helper class to interact with Amazon CloudFormation
 */
public class CloudFormationHelper {
    AmazonCloudFormation cfnClient;
    private final int MAX_POLL_STACK_STATUS_RETRIES = 15;
    private final Long POLL_STACK_STATUS_DELAY = 60000L; // 1 minute
    private final String BEDROCK_VPC_ENDPOINT_OUTPUT_KEY = "BedrockVpcEndpoint";
    private final String OPENSEARCH_ENDPOINT_OUTPUT_KEY = "OpenSearchVpcEndpoint";
    private final List<StackStatus> TERMINAL_STACK_STATUSES = List.of(
            StackStatus.CREATE_COMPLETE,
            StackStatus.CREATE_FAILED,
            StackStatus.DELETE_COMPLETE,
            StackStatus.DELETE_FAILED,
            StackStatus.ROLLBACK_COMPLETE,
            StackStatus.ROLLBACK_FAILED
    );

    public CloudFormationHelper() {
        cfnClient = AmazonCloudFormationClientBuilder.defaultClient();
    }

    /**
     * Find the value of a parameter by key from the given stack.
     *
     * @param stack Stack to look for parameters from
     * @param parameterKey The key of the parameter to look for
     * @return String value of the parameter associated with the given key
     */
    public String getParameterValue(Stack stack, String parameterKey) {
        int parameterIndex = stack.getParameters().indexOf(new Parameter().withParameterKey(parameterKey));
        if (parameterIndex < 0) {
            throw new RuntimeException("Did not find parameter " + parameterKey + "in stack " + stack.getStackName());
        }
        return stack.getParameters().get(parameterIndex).getParameterValue();
    }

    /**
     * Creates (deploys) the blueprint stack. Will wait for a terminal stack status to be reached before returning,
     * such as CREATE_COMPLETE and CREATE_FAILED, until a max timeout is reached.
     *
     * @param templateURL The S3 template URL of the blueprint CDK template to deploy
     * @param mskClusterConfig MskClusterConfig MSK cluster information
     * @param osClusterConfig OpenSearchClusterConfig OpenSearch cluster information
     * @param testID ID string for the test
     * @return Stack that was deployed
     */
    public Stack createBlueprintStack(String templateURL,
                                      MskClusterConfig mskClusterConfig,
                                      OpenSearchClusterConfig osClusterConfig,
                                      String testID) {
        String stackName = buildStackName(testID);
        try {
            // URL encode the template URL string
            System.out.println("Stack template URL raw: " + templateURL);
            String s3URLPrefix = "https://s3";
            String encodedTemplateURL = s3URLPrefix + templateURL.substring(s3URLPrefix.length())
                    .replace(":", "%3A");
            System.out.println("Stack template URL: " + encodedTemplateURL);

            // get parameters and deploy the stack
            List<Parameter> stackParameters = getBlueprintParameters(mskClusterConfig, osClusterConfig, testID);
            CreateStackRequest createStackRequest = new CreateStackRequest()
                    .withTemplateURL(encodedTemplateURL)
                    .withStackName(stackName)
                    .withParameters(stackParameters)
                    .withCapabilities(Capability.CAPABILITY_NAMED_IAM);
            cfnClient.createStack(createStackRequest);

            // wait for the stack to reach a terminal status
            Stack stack = this.pollBlueprintStatusStatus(stackName);
            if (stack == null) {
                throw new RuntimeException("Failed to create blueprint stack " + stackName);
            }
            if (stack.getStackStatus().equals(StackStatus.CREATE_IN_PROGRESS.toString())) {
                throw new RuntimeException("Blueprint stack " + stackName + " is still in CREATE_IN_PROGRESS state " +
                        "and did not complete deployment within test timeout");
            }
            if (!stack.getStackStatus().equals(StackStatus.CREATE_COMPLETE.toString())) {
                throw new RuntimeException("Create blueprint stack ended with unsuccessful status: " + stack.getStackStatus());
            }
            return stack;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create blueprint stack " + stackName, e);
        }
    }

    /**
     * Get the parameters to deploy the blueprint stack
     *
     * @param mskClusterConfig MskClusterConfig MSK cluster information
     * @param osClusterConfig OpenSearchClusterConfig OpenSearch cluster information
     * @param testID ID string for the test
     * @return List of Parameters to provide to the create stack request
     */
    private List<Parameter> getBlueprintParameters(MskClusterConfig mskClusterConfig,
                                                   OpenSearchClusterConfig osClusterConfig,
                                                   String testID) {
        // get MSK parameters
        MSKHelper mskHelper = new MSKHelper();
        MSKClusterBlueprintParameters mskClusterParams = mskHelper.getMSKClusterBlueprintParameters(
                mskClusterConfig.getArn(),
                testID);

        // get OpenSearch parameters
        OpenSearchHelper osHelper = new OpenSearchHelper();
        OpenSearchClusterBlueprintParameters osClusterParams = osHelper.getOpenSearchClusterBlueprintParameters(
                osClusterConfig.getName(),
                osClusterConfig.getOpenSearchClusterType(),
                testID);

        // get embedding model
        BedrockHelper bedrockHelper = new BedrockHelper();
        EmbeddingModel embeddingModel = bedrockHelper.getSupportedEmbeddingModel();

        return List.of(
            new Parameter().withParameterKey(PARAM_SOURCE_TYPE).withParameterValue("MSK"),
            new Parameter().withParameterKey(PARAM_SOURCE_DATA_TYPE).withParameterValue("STRING"),
            new Parameter().withParameterKey(PARAM_SINK_TYPE).withParameterValue("OPENSEARCH"),
            new Parameter().withParameterKey(PARAM_MSK_CLUSTER_NAME).withParameterValue(mskClusterParams.getMSKClusterName()),
            new Parameter().withParameterKey(PARAM_MSK_CLUSTER_ARN).withParameterValue(mskClusterParams.getMSKClusterArn()),
            new Parameter().withParameterKey(PARAM_MSK_CLUSTER_SUBNET_IDS).withParameterValue(mskClusterParams.getMSKClusterSubnetIds()),
            new Parameter().withParameterKey(PARAM_MSK_CLUSTER_SECURITY_GROUP_IDS).withParameterValue(mskClusterParams.getMSKClusterSecurityGroupIds()),
            new Parameter().withParameterKey(PARAM_MSK_TOPICS).withParameterValue(mskClusterParams.getMSKTopics()),
            new Parameter().withParameterKey(PARAM_MSK_VPC_ID).withParameterValue(mskClusterParams.getMSKVpcId()),
            new Parameter().withParameterKey(PARAM_OPEN_SEARCH_COLLECTION_NAME).withParameterValue(osClusterParams.getOpenSearchCollectionName()),
            new Parameter().withParameterKey(PARAM_OPEN_SEARCH_ENDPOINT_URL).withParameterValue(osClusterParams.getOpenSearchEndpointURL()),
            new Parameter().withParameterKey(PARAM_OPEN_SEARCH_TYPE).withParameterValue(osClusterParams.getOpenSearchType()),
            new Parameter().withParameterKey(PARAM_OPEN_SEARCH_INDEX_NAME).withParameterValue(osClusterParams.getOpenSearchVectorIndexName()),
            new Parameter().withParameterKey(PARAM_EMBEDDING_MODEL_NAME).withParameterValue(embeddingModel.getModelId()),
            new Parameter().withParameterKey(PARAM_JSON_KEYS_TO_EMBED).withParameterValue(".*"),
            new Parameter().withParameterKey(PARAM_APP_NAME).withParameterValue(this.buildStackAppName(testID)),
            new Parameter().withParameterKey(PARAM_RUNTIME_ENVIRONMENT).withParameterValue("FLINK-1_19"),
            new Parameter().withParameterKey(PARAM_ROLE_NAME).withParameterValue(this.buildStackRoleName(testID)),
            new Parameter().withParameterKey(PARAM_CLOUD_WATCH_LOG_GROUP_NAME).withParameterValue(this.buildStackLogGroupName(testID)),
            new Parameter().withParameterKey(PARAM_CLOUDWATCH_LOG_STREAM_NAME).withParameterValue(this.buildStackLogStreamName(testID)),
            new Parameter().withParameterKey(PARAM_ASSET_BUCKET).withParameterValue(this.buildStackAssetBucketName(testID)),
            new Parameter().withParameterKey(PARAM_JAR_FILE).withParameterValue("data-stream-vectorization-1.0-SNAPSHOT.jar"),
            new Parameter().withParameterKey(PARAM_ASSET_LIST).withParameterValue("https://github.com/awslabs/real-time-vectorization-of-streaming-data/releases/download/0.1-SNAPSHOT/data-stream-vectorization-1.0-SNAPSHOT.jar")
        );
    }

    /**
     * Poll the stack deployment status by periodically checking on the stack status.
     *
     * @param stackName The name of the stack to check
     * @return The deployed stack
     */
    public Stack pollBlueprintStatusStatus(String stackName) {
        int retryCount = 0;
        String stackStatus;
        Stack stack = null;
        try {
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
        } catch (Exception e) {
            throw new RuntimeException("Error occurred when polling stack status for " + stackName, e);
        }
    }

    /**
     * Delete the blueprint stack and first cleaning up any created stack-external resources, such as VPC endpoints.
     * Will wait for a terminal stack status to be reached before returning, such as CREATE_COMPLETE and CREATE_FAILED,
     * until a max timeout is reached.
     *
     * @param stackName The name of the stack to delete
     * @return Stack that was deleted
     */
    public Stack deleteBlueprintStack(String stackName) {
        // first clean up external stack resources
        this.blueprintStackCleanup(stackName);
        // delete the stack
        DeleteStackRequest deleteStackRequest = new DeleteStackRequest().withStackName(stackName);
        cfnClient.deleteStack(deleteStackRequest);
        // wait for the stack to reach a terminal status
        Stack stack = this.pollBlueprintStatusStatus(stackName);
        if (stack == null) {
            throw new RuntimeException("Failed to get the deleting blueprint stack " + stackName);
        }
        if (stack.getStackStatus().equals(StackStatus.DELETE_IN_PROGRESS.toString())) {
            throw new RuntimeException("Blueprint stack " + stackName + " is still in DELETE_IN_PROGRESS state " +
                    "and did not complete deletion within test timeout");
        }
        if (!stack.getStackStatus().equals(StackStatus.DELETE_COMPLETE.toString())) {
            throw new RuntimeException("Delete blueprint stack ended with unsuccessful status: " + stack.getStackStatus());
        }
        return stack;
    }

    /**
     * Clean up the external stack resources. Checks for stack outputs and deletes VPC endpoints if the outputs
     * indicate that they were created by the stack.
     *
     * @param stackName The name of the stack to delete external resources for
     */
    public void blueprintStackCleanup(String stackName) {
        // get the stack to clean up
        DescribeStacksRequest describeStacksRequest = new DescribeStacksRequest().withStackName(stackName);
        DescribeStacksResult describeStacksResults = cfnClient.describeStacks(describeStacksRequest);
        List<Stack> stacks = describeStacksResults.getStacks();
        if (stacks.size() != 1) {
            throw new RuntimeException("Expected number of stacks is 1, got: " + stacks.size());
        }
        Stack stackToCleanup = stacks.get(0);

        // get the OpenSearch cluster type
        String opensearchType = getParameterValue(stackToCleanup, PARAM_OPEN_SEARCH_TYPE);

        List<Output> outputs = stackToCleanup.getOutputs();
        for (Output output : outputs) {
            String outputKey = output.getOutputKey();
            String outputValue = output.getOutputValue();
            String vpceId;

            if (outputKey.equals(BEDROCK_VPC_ENDPOINT_OUTPUT_KEY)) {
                vpceId = getBedrockVpceToDelete(outputValue);
                if (!vpceId.isEmpty()) {
                    // delete the Bedrock VPC endpoint
                    AmazonEC2 ec2Client = AmazonEC2ClientBuilder.defaultClient();
                    DeleteVpcEndpointsRequest deleteVpcEndpointsRequest = new DeleteVpcEndpointsRequest()
                            .withVpcEndpointIds(vpceId);
                    ec2Client.deleteVpcEndpoints(deleteVpcEndpointsRequest);
                }
            } else if (outputKey.equals(OPENSEARCH_ENDPOINT_OUTPUT_KEY)) {
                vpceId = getOpenSearchVpceToDelete(outputValue);
                if (!vpceId.isEmpty()) {
                    if (opensearchType.equals(OpenSearchType.PROVISIONED.toString())) {
                        // delete the OpenSearch Provisioned VPC endpoint
                        AmazonOpenSearch opensearchClient = AmazonOpenSearchClientBuilder.defaultClient();
                        com.amazonaws.services.opensearch.model.DeleteVpcEndpointRequest deleteVpcEndpointRequest =
                                new com.amazonaws.services.opensearch.model.DeleteVpcEndpointRequest().withVpcEndpointId(vpceId);
                        opensearchClient.deleteVpcEndpoint(deleteVpcEndpointRequest);
                    } else if (opensearchType.equals(OpenSearchType.SERVERLESS.toString())) {
                        // delete the OpenSearch Serverless VPC endpoint
                        AWSOpenSearchServerless openSearchClient = AWSOpenSearchServerlessClientBuilder.defaultClient();
                        com.amazonaws.services.opensearchserverless.model.DeleteVpcEndpointRequest deleteVpcEndpointRequest =
                                new com.amazonaws.services.opensearchserverless.model.DeleteVpcEndpointRequest().withId(vpceId);
                        com.amazonaws.services.opensearchserverless.model.DeleteVpcEndpointResult deleteVpcEndpointResult =
                                openSearchClient.deleteVpcEndpoint(deleteVpcEndpointRequest);
                    } else {
                        throw new RuntimeException("Unknown OpenSearchType when cleaning up stack: " + opensearchType);
                    }
                }
            }
        }
    }

    /**
     * Get the VPC endpoint ID of the Bedrock VPC endpoint if it was created by the stack
     *
     * @param outputValue Bedrock output value string from stack output
     * @return Bedrock VPC endpoint ID
     */
    private String getBedrockVpceToDelete(String outputValue) {
        Pattern pattern = Pattern.compile("Bedrock VPC endpoint ID on stack creation: " +
                "(?<vpceID>[a-z0-9\\-]+) \\| Was created by this stack: (?<vpceBoolean>(True|False))");
        Matcher matcher = pattern.matcher(outputValue);

        if (matcher.find()) {
            String vpceID = matcher.group("vpceID");
            boolean vpceBoolean = Boolean.getBoolean(matcher.group("vpceBoolean"));
            if (vpceBoolean) {
                return vpceID;
            } else {
                return "";
            }
        } else {
            throw new RuntimeException("Unexpected output value structure when parsing stack output for " +
                    "Bedrock VPCEs: " + outputValue);
        }
    }

    /**
     * Get the VPC endpoint ID of the OpenSearch VPC endpoint if it was created by the stack
     *
     * @param outputValue OpenSearch output value string from stack output
     * @return OpenSearch VPC endpoint ID
     */
    private String getOpenSearchVpceToDelete(String outputValue) {
        Pattern pattern = Pattern.compile("OpenSearch VPC endpoint ID(s) on stack creation: " +
                "(?<vpceID>[a-z0-9\\-,]+) \\| Was created by this stack: (?<vpceBoolean>(True|False))");
        Matcher matcher = pattern.matcher(outputValue);

        if (matcher.find()) {
            String vpceID = matcher.group("vpceID");
            boolean vpceBoolean = Boolean.getBoolean(matcher.group("vpceBoolean"));
            if (vpceBoolean) {
                return vpceID;
            } else {
                return "";
            }
        } else {
            throw new RuntimeException("Unexpected output value structure when parsing stack output for " +
                    "OpenSearch VPCEs: " + outputValue);
        }
    }

    /**
     * Build the StackName parameter value for a test
     *
     * @param testID ID string for the test
     * @return StackName parameter value
     */
    private String buildStackName(String testID) {
        return "datastream-vec-integ-test-" + testID;
    }

    /**
     * Build the StackAppName parameter value for a test
     *
     * @param testID ID string for the test
     * @return StackAppName parameter value
     */
    private String buildStackAppName(String testID) {
        return "integ-test-app-" + testID;
    }

    /**
     * Build the StackRoleName parameter value for a test
     *
     * @param testID ID string for the test
     * @return StackRoleName parameter value
     */
    private String buildStackRoleName(String testID) {
        return "integ-test-app-" + testID + "-role";
    }

    /**
     * Build the StackLogGroupName parameter value for a test
     *
     * @param testID ID string for the test
     * @return StackLogGroupName parameter value
     */
    private String buildStackLogGroupName(String testID) {
        return "integ-test-app-" + testID + "-log-group";
    }

    /**
     * Build the StackLogStreamName parameter value for a test
     *
     * @param testID ID string for the test
     * @return StackLogStreamName parameter value
     */
    private String buildStackLogStreamName(String testID) {
        return "integ-test-app-" + testID + "-log-stream";
    }

    /**
     * Build the StackAssetBucketName parameter value for a test
     *
     * @param testID ID string for the test
     * @return StackAssetBucketName parameter value
     */
    private String buildStackAssetBucketName(String testID) {
        return "integ-test-app-" + testID + "-bucket";
    }

}
