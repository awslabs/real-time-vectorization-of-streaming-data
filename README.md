<div style="text-align: center">
<h1>Real-time Vector Embedding Blueprint</h1>

Real-time Vector Embedding Blueprint is an Amazon Managed Service for Apache Flink (MSF) blueprint which deploys an MSF app and other needed infrastructure for vectorizing incoming stream data and persisting the vectorized data in a vector DB. The MSF app consumes from an Amazon MSK cluster, creates embeddings of these messages with a supported Amazon Bedrock model, and stores the embeddings to an Amazon OpenSearch domain or collection.
</div>

## Get started with Real-time Vector Embedding

### Installation

Follow the installation instructions [here](cdk-infra/notes/installation.md) to install the shared libraries and begin developing.

### Deploying
Follow the steps [here](cdk-infra/apps/real-time-vector-embedding-application/msk-to-bedrock-to-opensearch/README.md) to build, deploy, and run the application.