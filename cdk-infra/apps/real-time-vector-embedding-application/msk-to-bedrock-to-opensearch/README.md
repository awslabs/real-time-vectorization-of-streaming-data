# Real-time Vector Embedding Application


## Prerequisites

Follow the installation instructions [here](../../../notes/installation.md) to complete required prerequisites.


## High-level deployment steps

1. Build app and copy resulting JAR to S3 location
2. Deploy associated infra using CDK
    - If using existing resources, you can simply update app properties in MSF.
3. Start your MSF application from the AWS console.
4. Produce to your MSK topic(s) you configured for the MSF application


## MSF application code development

1. Make changes to the Java source code if desired.
2. Build Java Flink application locally to create a new JAR.
   ```
   mvn clean package
   ```
2. Deploy the blueprint stack by following the [Launching via SAM with pre-synthesized templates](#launching-via-SAM-with-pre-synthesized-templates) instructions
3. Upload the new JAR to an S3 bucket in your account. Then in the MSF console, configure the deployed application's "Application code location" to point to the new JAR that you uploaded to S3.


## Launching via SAM with pre-synthesized templates:

1. Make changes to the CDK source code if desired.
2. In the `cdk-infra/apps/real-time-vector-embedding-application/msk-to-bedrock-to-opensearch/cdk-infra` folder, run:
   ```
   cdk synth
   # Can run below in non-guided mode by removing "--guided" if a config was saved and you would like to use the same values
   sam deploy -t cdk.out/BootstrapCdkStack.template.json --capabilities CAPABILITY_NAMED_IAM --guided
   ```

   In the repo's current state, when running `sam deploy` in `guided` mode, follow the guidelines for parameter values:
   ```
   Stack Name []: <your name>
   AWS Region []: <your-region>
   Parameter SourceType []: MSK
   Parameter SourceDataType []: <STRING or JSON>
   Parameter SinkType []: OPENSEARCH
   Parameter MSKClusterName []: <your MSK cluster name>
   Parameter MSKClusterArn []: <your MSK cluster ARN>
   Parameter MSKClusterSubnetIds []: <comma-separated list of your MSK cluster subnet IDs>
   Parameter MSKClusterSecurityGroupIds []: <comma-separated list of your MSK cluster security group IDs>
   Parameter MSKTopics []: <comma-separated list of existing topic names or .* for all topics>
   Parameter MSKVpcId []: <your MSK cluster VPC ID>
   Parameter OpenSearchCollectionName []: <your OpenSearch domain (if type PROVISIONED) or collection (if type SERVERLESS) name>
   Parameter OpenSearchEndpointURL []: <your OpenSearch domain/collection endpoint URL>
   Parameter OpenSearchType []: <PROVISIONED or SERVERLESS>
   Parameter OpenSearchVectorIndexName []: <an existing index name in your OpenSearch domain/collection>
   Parameter EmbeddingModelName []: <one of: amazon.titan-embed-text-v1, amazon.titan-embed-text-v2:0, amazon.titan-embed-image-v1, cohere.embed-english-v3, cohere.embed-multilingual-v3>
   Parameter JsonKeysToEmbed []: <comma-separated list of JSON keys to embed or .* for all keys>
   Parameter AppName []: <your new MSF app name>
   Parameter RuntimeEnvironment []: FLINK-1_19
   Parameter RoleName []: <your new MSF role name>
   Parameter CloudWatchLogGroupName []: <your new MSF Cloudwatch log group name>
   Parameter CloudWatchLogStreamName []: <your new MSF Cloudwatch log stream name>
   Parameter AssetBucket []: <your S3 bucket name where MSF assets are stored>
   Parameter JarFile []: <MSF app jar file name>
   Parameter AssetList []: <Github URL to download MSF app jar file from>
   ```

Now the blueprint stack will be deployed in your account.