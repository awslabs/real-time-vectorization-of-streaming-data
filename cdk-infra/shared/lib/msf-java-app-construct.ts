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

import { readFileSync } from 'fs';
import { StackProps } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';

export interface MsfJavaAppProps extends StackProps {
  account: string;
  region: string;
  partition: string;
  appName: string;
  runtimeEnvironment: string,
  serviceExecutionRole: string;
  bucketName: string;
  jarFile: string;
  logStreamName: string;
  logGroupName: string;
  subnets?: string[];
  securityGroups?: string[];
  parallelism?: Number;
  parallelismPerKpu?: Number;
  autoscalingEnabled?: Boolean;
  checkpointInterval?: Number;
  minPauseBetweenCheckpoints?: Number;
  applicationPropertiesGroupId?: string;
  applicationProperties?: Object;
}

// MsfJavaApp construct is used to create a new Java blueprint application.
// This construct is used instead of official CDK construct because official
// CDK construct does not support configuring CW logs during creation.
// Configuring CW logs with official CDK construct results in an update
// to the application which changes its initial version to 2. This is not 
// desired for blueprints functionality in AWS console.
export class MsfJavaApp extends Construct {
  constructor(scope: Construct, id: string, props: MsfJavaAppProps) {
    super(scope, id);

    const fn = new lambda.SingletonFunction(this, 'RealTimeVecEmbedMsfJavaAppCustomResourceHandler', {
      uuid: 'c4e1d42d-595a-4bd6-99e9-c299b61f2358',
      lambdaPurpose: "Deploy a Real-time vector embedding MSF app created with Java",
      code: lambda.Code.fromInline(readFileSync(`${__dirname}/../../python/msf_java_app_custom_resource_handler.py`, "utf-8")),
      handler: "index.handler",
      initialPolicy: [
        new iam.PolicyStatement(
          {
            actions: [
              'iam:PassRole',
              'kinesisanalytics:DeleteApplicationVpcConfiguration',
            ],
            resources: [props.serviceExecutionRole],
            conditions: {
              StringEqualsIfExists: {
                "iam:PassedToService": "kinesisanalytics.amazonaws.com"
              },
              ArnEqualsIfExists: {
                "iam:AssociatedResourceARN": `arn:${props.partition}:kinesisanalytics:${props.region}:${props.account}:application/${props.appName}`
              }
            }
          }),
      ],
      timeout: cdk.Duration.seconds(360),
      runtime: lambda.Runtime.PYTHON_3_9,
      memorySize: 1024,
    });

    fn.addToRolePolicy(new iam.PolicyStatement(
      {
        actions: [
          'kinesisanalytics:DescribeApplication',
          'kinesisanalytics:CreateApplication',
          'kinesisAnalyticsv2:UpdateApplication',
          'kinesisanalytics:DeleteApplicationVpcConfiguration',
          'kinesisanalytics:DeleteApplication',
        ],
        resources: ['arn:aws:kinesisanalytics:' + props.region + ':' + props.account + ':application/' + props.appName]
      }));

    const defaultProps = {
      parallelism: 2,
      parallelismPerKpu: 1,
      autoscalingEnabled: false,
      checkpointInterval: 60000,
      minPauseBetweenCheckpoints: 5000,
      applicationProperties: {}
    };

    props = {...defaultProps, ...props};

    const logStreamArn = `arn:${props.partition}:logs:${props.region}:${props.account}:log-group:${props.logGroupName}:log-stream:${props.logStreamName}`;
    const bucketArn = `arn:${props.partition}:s3:::${props.bucketName}`;
    new cdk.CustomResource(this, `MSFJavaApp${id}`, {
      serviceToken: fn.functionArn,
      properties:
        {
          AppName: props.appName,
          RuntimeEnvironment: props.runtimeEnvironment,
          ServiceExecutionRole: props.serviceExecutionRole,
          BucketArn: bucketArn,
          FileKey: props.jarFile,
          LogStreamArn: logStreamArn,
          Subnets: props.subnets,
          SecurityGroups: props.securityGroups,
          Parallelism: props.parallelism,
          ParallelismPerKpu: props.parallelismPerKpu,
          AutoscalingEnabled: props.autoscalingEnabled,
          CheckpointInterval: props.checkpointInterval,
          MinPauseBetweenCheckpoints: props.minPauseBetweenCheckpoints,
          ApplicationPropertiesGroupId: props.applicationPropertiesGroupId ?? 'BlueprintMetadata',
          ApplicationProperties: props.applicationProperties
        }
    });
  }
}
