# Installation

### Prerequisites
You must first install the prerequisites below to start synthesizing and testing templates on your local machine.

- [Install Java](https://www.java.com/en/download/help/download_options.html)
- [Install Maven](https://maven.apache.org/install.html)
- [Install Node.js](https://nodejs.org/en/download/)
- [Install and Bootstrap CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html)
- [Install Git](https://github.com/git-guides/install-git)
- [Install AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- [Install AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html)
- [Install AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)

### Clone this repository

```
git clone https://github.com/awslabs/real-time-vectorization-of-streaming-data
```

### Install/Update npm packages associated with CDK

In the `cdk-infra/shared` folder, run:
```
npm install
npm run build
```

In the `cdk-infra/apps/real-time-vector-embedding-application/msk-to-bedrock-to-opensearch/cdk-infra` folder, run:
```
npm install
npm run build
```