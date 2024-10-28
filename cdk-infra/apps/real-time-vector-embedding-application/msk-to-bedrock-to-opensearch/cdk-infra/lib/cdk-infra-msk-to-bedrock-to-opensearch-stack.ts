/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Apache-2.0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import * as cdk from "aws-cdk-lib";
import { CfnOutput, Fn, StackProps } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as iam from "aws-cdk-lib/aws-iam";
import { aws_logs as logs } from "aws-cdk-lib";
import {
  MsfJavaApp,
} from "../../../../../shared/lib/msf-java-app-construct";
import { CopyAssetsLambdaConstruct } from "../../../../../shared/lib/copy-assets-lambda-construct";
import { OpenSearchServerlessAccessPolicyConstruct } from "../../../../../shared/lib/os-serverless-access-policy-construct";
import { MSKGetBootstrapBrokerStringConstruct } from "../../../../../shared/lib/msk-get-bootstrap-broker-string";
import { VpcEndpointCheckerConstruct } from "../../../../../shared/lib/vpc-endpoint-checker";

export interface GlobalProps extends StackProps {
  assetBucket: string;
  assetList: string;
  assetKey: string;
  sourceType: string;
  sourceDataType: string;
  sinkType: string;
  mskClusterName: string;
  mskClusterArn: string;
  mskClusterSubnetIds: string;
  mskClusterSecurityGroupIds: string;
  mskTopics: string;
  mskVpcId: string;
  openSearchType: string;
  openSearchCollectionName: string;
  openSearchEndpointURL: string;
  openSearchVectorIndexName: string;
  embeddingModelName: string;
  jsonKeysToEmbed: string;
}

/**
 * Stack with resources for the Real-time vector embedding app
 */
export class CdkInfraMskToOpenSearchStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: GlobalProps) {
    super(scope, id, props);

    // we'll be generating a CFN script so we need CFN params
    let cfnParams = this.getParams();

    // this construct creates an S3 bucket then copies all the assets passed
    const copyAssetsLambdaFn = new CopyAssetsLambdaConstruct(
      this,
      "CopyAssetsLambda",
      {
        account: this.account,
        region: this.region,
        AssetBucket: cfnParams.get("AssetBucket")!.valueAsString,
        AssetList: cfnParams.get("AssetList")!.valueAsString,
      }
    );

    // our Real-time vector embedding app needs access to read application jar from S3
    const msfAppS3BucketAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: [
            `arn:aws:s3:::${cfnParams.get("AssetBucket")!.valueAsString}/*`,
          ],
          actions: ["s3:ListBucket", "s3:GetObject"],
          conditions: {
            'ForAnyValue:StringEquals': {
              's3:ResourceAccount': this.account,
            },
          }
        }),
      ],
    });

    // create CW log group and log stream so it can be used when creating the Real-time vector embedding app
    const msfAppLogGroup = new logs.LogGroup(this, "RealTimeVecEmbed-MSFAppLogGroup", {
      logGroupName: cfnParams.get("CloudWatchLogGroupName")!.valueAsString,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const msfAppLogStream = new logs.LogStream(this, "RealTimeVecEmbed-MSFAppLogStream", {
      logStreamName: cfnParams.get("CloudWatchLogStreamName")!.valueAsString,
      logGroup: msfAppLogGroup,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    msfAppLogStream.node.addDependency(msfAppLogGroup);

    // our MSF app needs to be able to log
    const msfAppCWLogsAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: [msfAppLogGroup.logGroupArn],
          actions: [
            "logs:PutLogEvents",
            "logs:DescribeLogGroups",
            "logs:DescribeLogStreams",
          ],
        }),
      ],
    });

    // our Real-time vector embedding app needs to be able to write metrics
    const msfAppCWMetricsAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ["*"],
          actions: ["cloudwatch:PutMetricData"],
          conditions: {
            'ForAnyValue:StringEquals': {
              'cloudwatch:namespace': 'AWS/KinesisAnalytics',
            },
          }
        }),
      ],
    });

    // our Real-time vector embedding app needs access to describe kinesisanalytics
    const msfAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: [
            `arn:aws:kinesisanalytics:${this.region}:${
              this.account
            }:application/${cfnParams.get("AppName")!.valueAsString}`,
          ],
          actions: ["kinesisanalytics:DescribeApplication"],
        }),
      ],
    });

    const mskClusterName = cfnParams.get("MSKClusterName")!.valueAsString;

    // our Real-time vector embedding app needs the following permissions against MSK
    const msfAppMSKClusterAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: [
            `arn:aws:kafka:${this.region}:${this.account}:cluster/${mskClusterName}/*`,
            `arn:aws:kafka:${this.region}:${this.account}:topic/${mskClusterName}/*`,
            `arn:aws:kafka:${this.region}:${this.account}:group/${mskClusterName}/*`,
          ],
          actions: [
            "kafka-cluster:Connect",
            "kafka-cluster:DescribeTopic",
            "kafka-cluster:DescribeGroup",
            "kafka-cluster:AlterGroup",
            "kafka-cluster:ReadData",
          ],
        }),
      ],
    });

    // our Real-time vector embedding app needs to be the following permissions against Bedrock
    // - InvokeModel (to get vector embeddings of MSK stream data)
    const msfAppBedrockAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: [
            `arn:aws:bedrock:${this.region}::foundation-model/*`,
          ],
          actions: ["bedrock:GetFoundationModel", "bedrock:InvokeModel"],
        }),
      ],
    });

    // our Real-time vector embedding app needs to be the following permissions against AOSS and ES
    // - AOSS ApiAllAccess
    // - ES HTTP GET, POST, PUT
    const msfAppOpenSearchAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: [
            `arn:aws:aoss:${this.region}:${this.account}:collection/*`,
            `arn:aws:aoss:${this.region}:${this.account}:index/*`,
          ],
          actions: ["aoss:APIAccessAll"],
        }),
        new iam.PolicyStatement({
          resources: [
            `arn:aws:es:${this.region}:${this.account}:domain/*`,
          ],
          actions: [
            "es:ESHttpGet",
            "es:ESHttpPost",
            "es:ESHttpPut",
          ],
        }),
      ],
    });

    // our Real-time vector embedding app needs access to perform VPC actions
    const accessVPCPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ["*"],
          actions: [
            "ec2:DescribeNetworkInterfaces",
            "ec2:DescribeVpcs",
            "ec2:DescribeDhcpOptions",
            "ec2:DescribeSubnets",
            "ec2:DescribeSecurityGroups",
            "ec2:CreateNetworkInterface",
            "ec2:CreateNetworkInterfacePermission",
            "ec2:DeleteNetworkInterface",
          ],
        }),
      ],
    });

    // our Real-time vector embedding app needs a role with created access policies
    const msfAppIAMRole = new iam.Role(this, "real-time-vec-embed-app-role", {
      roleName: cfnParams.get("RoleName")!.valueAsString,
      assumedBy: new iam.ServicePrincipal("kinesisanalytics.amazonaws.com"),
      description: "Real-time vector embedding MSF app role",
      inlinePolicies: {
        AccessS3Policy: msfAppS3BucketAccessPolicy,
        AccessCWLogsPolicy: msfAppCWLogsAccessPolicy,
        AccessCWMetricsPolicy: msfAppCWMetricsAccessPolicy,
        MSFAccessPolicy: msfAccessPolicy,
        AccessMSKPolicy: msfAppMSKClusterAccessPolicy,
        AccessBedrockPolicy: msfAppBedrockAccessPolicy,
        AccessVPCPolicy: accessVPCPolicy,
        AccessOpenSearchPolicy: msfAppOpenSearchAccessPolicy,
      },
    });

    // our Real-time vector embedding app needs to write permissions against Amazon OpenSearch collection & index
    const opensearchServerlessAccessPolicy =
      new OpenSearchServerlessAccessPolicyConstruct(
        this,
        "OpenSearchAccessPolicy",
        {
          collectionName: cfnParams.get("OpenSearchCollectionName")!
            .valueAsString,
          vectorIndexName: cfnParams.get("OpenSearchVectorIndexName")!
            .valueAsString,
          fullAccessRoleArns: [msfAppIAMRole.roleArn],
        }
      );

    // our Real-time vector embedding app needs VPCE endpoints for Bedrock and OpenSearch if they don't already exist
    const getVpcEndpointCheckerLambda = new VpcEndpointCheckerConstruct(
      this,
      'VpceCheckLambda', {
        vpcId: cfnParams.get("MSKVpcId")!.valueAsString,
        region: this.region,
      });

    // custom resource to check if Bedrock VPC Endpoint already exists and create if it doesn't
    const getBedrockVpcEndpointChecker = new cdk.CustomResource(this, 'BrVpceChecker', {
      serviceToken: getVpcEndpointCheckerLambda.getVpcEndpointCheckerFunction.functionArn,
      properties: {
        endpointType: "BEDROCK",
        subnetIds: cfnParams.get("MSKClusterSubnetIds")!.valueAsString,
        securityGroups: cfnParams.get("MSKClusterSecurityGroupIds")!.valueAsList,
        createIfNotExists: true,
      }
    });

    // custom resource to check if Opensearch VPC Endpoint already exists and create if it doesn't
    const getOpensearchVpcEndpointChecker = new cdk.CustomResource(this, 'OsVpceChecker', {
      serviceToken: getVpcEndpointCheckerLambda.getVpcEndpointCheckerFunction.functionArn,
      properties: {
        endpointType: Fn.join("-", ["OPENSEARCH", cfnParams.get("OpenSearchType")!.valueAsString]),
        subnetIds: cfnParams.get("MSKClusterSubnetIds")!.valueAsString,
        securityGroups: cfnParams.get("MSKClusterSecurityGroupIds")!.valueAsList,
        createIfNotExists: true,
        domainName: cfnParams.get("OpenSearchCollectionName")!.valueAsString,
      }
    });

    // add Bedrock VPC endpoint information in stack output to track created VPC endpoint
    new CfnOutput(this, 'BedrockVpcEndpoint', {
      description: "Bedrock VPC endpoint ID",
      value: Fn.sub("Bedrock VPC endpoint ID on stack creation: ${vpceId} | Was created by this stack: ${createdByStack}", {
        vpceId: getBedrockVpcEndpointChecker.getAttString("VpcEndpointId"),
        createdByStack: getBedrockVpcEndpointChecker.getAttString("CreatedVpcEndpoints"),
      }),
    });

    // add OpenSearch VPC endpoint information in stack output to track created VPC endpoint
    new CfnOutput(this, 'OpenSearchVpcEndpoint', {
      description: "OpenSearch VPC endpoint ID",
      value: Fn.sub("OpenSearch VPC endpoint ID(s) on stack creation: ${vpceId} | Was created by this stack: ${createdByStack}", {
        vpceId: getOpensearchVpcEndpointChecker.getAttString("VpcEndpointId"),
        createdByStack: getOpensearchVpcEndpointChecker.getAttString("CreatedVpcEndpoints"),
      }),
    });

    // our Real-time vector embedding app needs the bootstrap broker string to connect to the MSK cluster
    const getBootstrapBrokersLambda = new MSKGetBootstrapBrokerStringConstruct(this, 'GetBootstrapStringLambda', {
      mskClusterArn: cfnParams.get("MSKClusterArn")!.valueAsString
    });

    // custom resource to get bootstrap brokers for our MSK cluster
    const getBootstrapBrokers = new cdk.CustomResource(this, 'BootstrapBrokersLookup', {
      serviceToken: getBootstrapBrokersLambda.getBootstrapBrokerFn.functionArn
    });

    // condition to add Json keys runtime property only if the source data type is JSON
    const shouldUseJsonKeys = new cdk.CfnCondition(this, 'shouldUseJsonKeys', {
      expression: cdk.Fn.conditionEquals(cfnParams.get("SourceDataType")!.valueAsString, "JSON"),
    });

    // Runtime properties needed by the Real-time vector embedding app
    const msfAppProperties = {
      "stack.arn": this.stackId,
      "source.type": cfnParams.get("SourceType")!.valueAsString,
      "sink.type": cfnParams.get("SinkType")!.valueAsString,
      "source.msk.cluster.arn": cfnParams.get("MSKClusterArn")!.valueAsString,
      "source.msk.bootstrap.servers": getBootstrapBrokers.getAttString("BootstrapBrokerString"),
      "source.msk.topic.names": cfnParams.get("MSKTopics")!.valueAsString,
      "source.msk.data.type": cfnParams.get("SourceDataType")!.valueAsString,
      "embed.model.id": cfnParams.get("EmbeddingModelName")!.valueAsString,
      "embed.input.config.json.fieldsToEmbed": cdk.Fn.conditionIf(
        shouldUseJsonKeys.logicalId,
        cfnParams.get("JsonKeysToEmbed")!.valueAsString,
        cdk.Fn.ref('AWS::NoValue')),
      "sink.os.type": cfnParams.get("OpenSearchType")!.valueAsString,
      "sink.os.name": cfnParams.get("OpenSearchCollectionName")!.valueAsString,
      "sink.os.endpoint": cfnParams.get("OpenSearchEndpointURL")!.valueAsString,
      "sink.os.vector.index": cfnParams.get("OpenSearchVectorIndexName")!.valueAsString,
    };

    // custom resource to create the Real-time vector embedding MSF app
    const msfApp = new MsfJavaApp(this, "real-time-vec-embed-java-app", {
      account: this.account,
      region: this.region,
      partition: this.partition,
      appName: cfnParams.get("AppName")!.valueAsString,
      runtimeEnvironment: cfnParams.get("RuntimeEnvironment")!.valueAsString,
      serviceExecutionRole: msfAppIAMRole.roleArn,
      bucketName: cfnParams.get("AssetBucket")!.valueAsString,
      jarFile: cfnParams.get("JarFile")!.valueAsString,
      logStreamName: msfAppLogStream.logStreamName,
      logGroupName: msfAppLogGroup.logGroupName,
      applicationPropertiesGroupId: "FlinkApplicationProperties",
      applicationProperties: msfAppProperties,
      subnets: Fn.split(",", cfnParams.get("MSKClusterSubnetIds")!.valueAsString),
      securityGroups: cfnParams.get("MSKClusterSecurityGroupIds")!.valueAsList,
    });

    msfApp.node.addDependency(copyAssetsLambdaFn);
    msfApp.node.addDependency(msfAppIAMRole);
    msfApp.node.addDependency(msfAppLogGroup);
    msfApp.node.addDependency(opensearchServerlessAccessPolicy);
  } // constructor

  getParams(): Map<string, cdk.CfnParameter> {
    let params = new Map<string, cdk.CfnParameter>();

    params.set(
      "SourceType",
      new cdk.CfnParameter(this, "SourceType", {
        type: "String",
        description: "The type of data streaming source (e.g. MSK)",
      })
    );

    params.set(
      "SourceDataType",
      new cdk.CfnParameter(this, "SourceDataType", {
        type: "String",
        description: "The type of data from the streaming source (e.g. STRING)",
      })
    );

    params.set(
      "SinkType",
      new cdk.CfnParameter(this, "SinkType", {
        type: "String",
        description: "The type of data sink source (e.g. OPENSEARCH)",
      })
    );

    params.set(
      "MSKClusterName",
      new cdk.CfnParameter(this, "MSKClusterName", {
        type: "String",
        description: "The MSK Serverless cluster name",
      })
    );

    params.set(
      "MSKClusterArn",
      new cdk.CfnParameter(this, "MSKClusterArn", {
        type: "String",
        description: "The MSK Serverless cluster ARN",
      })
    );

    params.set(
      "MSKClusterSubnetIds",
      new cdk.CfnParameter(this, "MSKClusterSubnetIds", {
        type: "String",
        description: "The list of MSK cluster SubnetIds.",
      })
    );

    params.set(
      "MSKClusterSecurityGroupIds",
      new cdk.CfnParameter(this, "MSKClusterSecurityGroupIds", {
        type: "CommaDelimitedList",
        description: "The MSK cluster SecurityGroupIds.",
      })
    );

    params.set(
      "MSKTopics",
      new cdk.CfnParameter(this, "MSKTopics", {
        type: "String",
        description:
          "The list of topics in MSK to read streaming data to vectorize.",
      })
    );

    params.set(
      "MSKVpcId",
      new cdk.CfnParameter(this, "MSKVpcId", {
        type: "String",
        description: "The MSK VPC ID",
      })
    );

    params.set(
      "OpenSearchCollectionName",
      new cdk.CfnParameter(this, "OpenSearchCollectionName", {
        type: "String",
        description: "The OpenSearch Serverless collection name",
      })
    );
    params.set(
      "OpenSearchEndpointURL",
      new cdk.CfnParameter(this, "OpenSearchEndpointURL", {
        type: "String",
        description: "OpenSearch collection endpoint url",
      })
    );
    params.set(
      "OpenSearchType",
      new cdk.CfnParameter(this, "OpenSearchType", {
        type: "String",
        description: "OpenSearch flavor (e.g. PROVISIONED or SERVERLESS)",
      })
    );
    params.set(
      "OpenSearchVectorIndexName",
      new cdk.CfnParameter(this, "OpenSearchVectorIndexName", {
        type: "String",
        description:
          "The name of the Vector index in the OpenSearch collection.",
      })
    );

    params.set(
      "EmbeddingModelName",
      new cdk.CfnParameter(this, "EmbeddingModelName", {
        type: "String",
        description: "The embedding model used for vectorizing streaming data.",
      })
    );

    params.set(
      "JsonKeysToEmbed",
      new cdk.CfnParameter(this, "JsonKeysToEmbed", {
        type: "String",
        description: "The keys in the JSON to be embedded if the source data type is JSON.",
      })
    );

    params.set(
      "AppName",
      new cdk.CfnParameter(this, "AppName", {
        type: "String",
        description: "Flink application name",
      })
    );

    params.set(
      "RuntimeEnvironment",
      new cdk.CfnParameter(this, "RuntimeEnvironment", {
        type: "String",
        description: "Flink runtime environment",
      })
    );

    params.set(
      "RoleName",
      new cdk.CfnParameter(this, "RoleName", {
        type: "String",
        description: "Name of IAM role used to run the Flink application",
      })
    );

    params.set(
      "CloudWatchLogGroupName",
      new cdk.CfnParameter(this, "CloudWatchLogGroupName", {
        type: "String",
        description: "The log group name for the Flink application",
      })
    );

    params.set(
      "CloudWatchLogStreamName",
      new cdk.CfnParameter(this, "CloudWatchLogStreamName", {
        type: "String",
        description: "The log stream name for the Flink application",
      })
    );

    params.set(
      "AssetBucket",
      new cdk.CfnParameter(this, "AssetBucket", {
        type: "String",
        description:
          "The name of the Amazon S3 bucket where uploaded jar function will be stored.",
      })
    );

    params.set(
      "JarFile",
      new cdk.CfnParameter(this, "JarFile", {
        type: "String",
        description:
          "S3 key for .jar file containing the app (must exist in bucket specified in BucketName parameter)",
      })
    );

    params.set(
      "AssetList",
      new cdk.CfnParameter(this, "AssetList", {
        type: "String",
        description:
          "The list of assets for MSF app which are uploaded to S3 bucket.",
      })
    );

    return params;
  }
} // class
