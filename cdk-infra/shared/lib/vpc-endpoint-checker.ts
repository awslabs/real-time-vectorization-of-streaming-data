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

import { StackProps } from 'aws-cdk-lib';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { readFileSync } from "fs";

export interface VpcEndpointCheckerProps extends StackProps {
  vpcId: string,
  region: string,
}

// Lambda construct that creates a VPC endpoint for either Bedrock, OpenSearch (ES), or OpenSearch Serverless (AOSS)
export class VpcEndpointCheckerConstruct extends Construct {
  public getVpcEndpointCheckerFunction: lambda.SingletonFunction;

  constructor(scope: Construct, id: string, props: VpcEndpointCheckerProps) {
    super(scope, id);

    // Run topic creation lambda
    this.getVpcEndpointCheckerFunction = new lambda.SingletonFunction(this, id, {
      uuid: 'e28123c0-1b6b-11ee-ae56-0242ac120002',
      lambdaPurpose: id,
      code: lambda.Code.fromInline(readFileSync(`${__dirname}/../../python/lambda_check_vpc_endpoints.py`, "utf-8")),
      handler: "index.handler",
      initialPolicy: [
        new iam.PolicyStatement(
          {
            actions: [
              'ec2:DescribeVpcs',
              'ec2:DescribeVpcEndpoints',
              'ec2:CreateVpcEndpoint',
              'ec2:CreateTags',
              'aoss:CreateVpcEndpoint',
              'ec2:DescribeSubnets',
              'ec2:DescribeSecurityGroups',
              "es:CreateVpcEndpoint",
              "es:DescribeDomain",
              "route53:*",
            ],
            resources: ['*']
          })
      ],
      timeout: cdk.Duration.seconds(300),
      runtime: lambda.Runtime.PYTHON_3_9,
      memorySize: 256,
      environment:
        {
          vpc_id: props.vpcId,
          region: props.region,
        }
    });

    // deletes SGs and such before deleting lambda (with dependencies)
    const fixVpcDeletion = (handler: lambda.IFunction): void => {
      if (!handler.isBoundToVpc) {
        return
      }
      handler.connections.securityGroups.forEach(sg => {
        if (handler.role) {
          handler.role.node.children.forEach(child => {
            if (
              child.node.defaultChild &&
              (child.node.defaultChild as iam.CfnPolicy).cfnResourceType === 'AWS::IAM::Policy'
            ) {
              sg.node.addDependency(child);
            }
          });
        }
      });
    };

    fixVpcDeletion(this.getVpcEndpointCheckerFunction)
  }
}
