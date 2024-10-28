#!/usr/bin/env node
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

import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { BootstraplessStackSynthesizer } from "cdk-bootstrapless-synthesizer";
import { CFN_PARAMS } from "../lib/constants";
import { CdkInfraMskToOpenSearchStack } from "../lib/cdk-infra-msk-to-bedrock-to-opensearch-stack";

const app = new cdk.App();

const stackName = app.node.tryGetContext("stackName")
  ? app.node.tryGetContext("stackName")
  : "BootstrapCdkStack";

// BOOTSTRAP PORTION
const AssetBucket = app.node.tryGetContext(CFN_PARAMS.AssetBucket);
// list of links (assets) to download
const AssetList = app.node.tryGetContext(CFN_PARAMS.AssetList);
const AssetKey = app.node.tryGetContext(CFN_PARAMS.AssetKey);
// END OF BOOTSTRAP PORTION

// REAL-TIME-VECTOR EMBEDDING APPLICATION PORTION
const SourceType = app.node.tryGetContext(CFN_PARAMS.SourceType);
const SourceDataType = app.node.tryGetContext(CFN_PARAMS.SourceDataType);
const SinkType = app.node.tryGetContext(CFN_PARAMS.SinkType);
const MSKClusterName = app.node.tryGetContext(CFN_PARAMS.MSKClusterName);
const MSKClusterARN = app.node.tryGetContext(CFN_PARAMS.MSKClusterArn);
const MSKClusterSubnetIds = app.node.tryGetContext(
  CFN_PARAMS.MSKClusterSubnetIds
);
const MSKClusterSecurityGroupIds = app.node.tryGetContext(
  CFN_PARAMS.MSKClusterSecurityGroupIds
);
const MSKTopics = app.node.tryGetContext(CFN_PARAMS.MSKTopics);
const MSKVpcId = app.node.tryGetContext(CFN_PARAMS.MSKVpcId);
const OpenSearchType = app.node.tryGetContext(
  CFN_PARAMS.OpenSearchType
);
const OpenSearchCollectionName = app.node.tryGetContext(
  CFN_PARAMS.OpenSearchCollectionName
);
const OpenSearchEndpointURL = app.node.tryGetContext(
  CFN_PARAMS.OpenSearchEndpointURL
);
const OpenSearchIndexName = app.node.tryGetContext(
  CFN_PARAMS.OpenSearchIndexName
);
const EmbeddingModelName = app.node.tryGetContext(
  CFN_PARAMS.EmbeddingModelName
);
const JsonKeysToEmbed = app.node.tryGetContext(
  CFN_PARAMS.JsonKeysToEmbed
);
// END OF REAL-TIME-VECTOR EMBEDDING APPLICATION PORTION

new CdkInfraMskToOpenSearchStack(app, stackName, {
  synthesizer: new BootstraplessStackSynthesizer({
    templateBucketName: "cfn-template-bucket",
    fileAssetBucketName: "file-asset-bucket-${AWS::Region}",
    fileAssetRegionSet: ["us-west-1", "us-west-2"],
    fileAssetPrefix: "file-asset-prefix/latest/",
  }),
  assetBucket: AssetBucket,
  assetList: AssetList,
  assetKey: AssetKey,
  sourceType: SourceType,
  sourceDataType: SourceDataType,
  sinkType: SinkType,
  mskClusterName: MSKClusterName,
  mskClusterArn: MSKClusterARN,
  mskClusterSubnetIds: MSKClusterSubnetIds,
  mskClusterSecurityGroupIds: MSKClusterSecurityGroupIds,
  mskTopics: MSKTopics,
  mskVpcId: MSKVpcId,
  openSearchType: OpenSearchType,
  openSearchCollectionName: OpenSearchCollectionName,
  openSearchEndpointURL: OpenSearchEndpointURL,
  openSearchVectorIndexName: OpenSearchIndexName,
  embeddingModelName: EmbeddingModelName,
  jsonKeysToEmbed: JsonKeysToEmbed
});
