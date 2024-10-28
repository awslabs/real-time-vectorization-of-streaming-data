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

import {
  CfnAccessPolicy,
  CfnCollection,
} from "aws-cdk-lib/aws-opensearchserverless";
import { Construct } from "constructs";
import { CustomResource } from "aws-cdk-lib";
import { Fn } from "aws-cdk-lib";
import { DisambiguitorLambdaConstruct } from "./disambiguator-lambda-construct";

export interface OpenSearchServerlessProps {
  collectionName: string;
  vectorIndexName: string;
  fullAccessRoleArns: string[];
}

// Construct for OpenSearch access policy
export class OpenSearchServerlessAccessPolicyConstruct extends Construct {
  readonly collection: CfnCollection;

  constructor(scope: Construct, id: string, props: OpenSearchServerlessProps) {
    super(scope, id);

    const getDisambuguitorLambda = new DisambiguitorLambdaConstruct(this, 'DisambuguitorLambda');

    const getDisambuguitor = new CustomResource(this, 'GetDisambuguitor', {
      serviceToken: getDisambuguitorLambda.getLambdaDisambuguitorFunction.functionArn,
      properties: {
        disambiguitorLength: 5,
      },
    });

    const dataAccessPolicy = new CfnAccessPolicy(this, "Policy", {
      name: Fn.join("-", [id.toLowerCase(), getDisambuguitor.getAttString("Disambiguitor")]),
      type: "data",
      description: `Data Access Policy for ${props.collectionName}`,
      policy: "generated",
    });
    dataAccessPolicy.policy = JSON.stringify([
      {
        Description: "Full Data Access",
        Rules: [
          {
            Permission: [
              "aoss:CreateCollectionItems",
              "aoss:DeleteCollectionItems",
              "aoss:UpdateCollectionItems",
              "aoss:DescribeCollectionItems",
            ],
            ResourceType: "collection",
            Resource: [`collection/${props.collectionName}`],
          },
          {
            Permission: [
              "aoss:CreateIndex",
              "aoss:DeleteIndex",
              "aoss:UpdateIndex",
              "aoss:DescribeIndex",
              "aoss:ReadDocument",
              "aoss:WriteDocument",
            ],
            ResourceType: "index",
            Resource: [`index/${props.collectionName}/*`],
          },
        ],
        Principal: props.fullAccessRoleArns,
      },
    ]);
  }
}