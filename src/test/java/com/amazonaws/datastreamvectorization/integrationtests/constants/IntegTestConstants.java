package com.amazonaws.datastreamvectorization.integrationtests.constants;

public final class IntegTestConstants {
    public static final String INTEG_TEST_INPUTS_FILE = "integTestInputsFile";
    public static final String BLUEPRINT_CDK_TEMPLATE_URL = "blueprintCDKTemplateURL";

    public static final class BlueprintParameterKeys {
        // Parameter keys for deploying the blueprint stack
        public static final String PARAM_SOURCE_TYPE = "SourceType";
        public static final String PARAM_SOURCE_DATA_TYPE = "SourceDataType";
        public static final String PARAM_SINK_TYPE = "SinkType";
        public static final String PARAM_MSK_CLUSTER_NAME = "MSKClusterName";
        public static final String PARAM_MSK_CLUSTER_ARN = "MSKClusterArn";
        public static final String PARAM_MSK_CLUSTER_SUBNET_IDS = "MSKClusterSubnetIds";
        public static final String PARAM_MSK_CLUSTER_SECURITY_GROUP_IDS = "MSKClusterSecurityGroupIds";
        public static final String PARAM_MSK_TOPICS = "MSKTopics";
        public static final String PARAM_MSK_VPC_ID = "MSKVpcId";
        public static final String PARAM_OPEN_SEARCH_COLLECTION_NAME = "OpenSearchCollectionName";
        public static final String PARAM_OPEN_SEARCH_ENDPOINT_URL = "OpenSearchEndpointURL";
        public static final String PARAM_OPEN_SEARCH_TYPE = "OpenSearchType";
        public static final String PARAM_OPEN_SEARCH_INDEX_NAME = "OpenSearchVectorIndexName";
        public static final String PARAM_EMBEDDING_MODEL_NAME = "EmbeddingModelName";
        public static final String PARAM_JSON_KEYS_TO_EMBED = "JsonKeysToEmbed";
        public static final String PARAM_APP_NAME = "AppName";
        public static final String PARAM_RUNTIME_ENVIRONMENT = "RuntimeEnvironment";
        public static final String PARAM_ROLE_NAME = "RoleName";
        public static final String PARAM_CLOUD_WATCH_LOG_GROUP_NAME = "CloudWatchLogGroupName";
        public static final String PARAM_CLOUDWATCH_LOG_STREAM_NAME = "CloudWatchLogStreamName";
        public static final String PARAM_ASSET_BUCKET = "AssetBucket";
        public static final String PARAM_JAR_FILE = "JarFile";
        public static final String PARAM_ASSET_LIST = "AssetList";

    }
}
