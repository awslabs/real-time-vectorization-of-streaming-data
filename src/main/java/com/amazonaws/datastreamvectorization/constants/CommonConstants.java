/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
package com.amazonaws.datastreamvectorization.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CommonConstants {
    public static final String PROPERTY_VALUES_DELIMITER = ",";
    public static final String EMBED_OUTPUT_EMBEDDING = "embedding";
    public static final String EMBED_INPUT_TEXT_KEY_NAME = "inputText";
    public static final String EMBED_OUTPUT_TIMESTAMP_KEY_NAME = "@timestamp";

    public static final String METRIC_GROUP_KINESIS_ANALYTICS = "KinesisAnalytics";
    public static final String METRIC_GROUP_KEY_SERVICE = "Service";
    public static final String METRIC_GROUP_VALUE_SERVICE = "StreamingDataVectorization";
    public static final String METRIC_GROUP_KEY_OPERATION = "Operation";

    /**
     * Class to hold names of constants configured through Flink runtime properties.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class FlinkApplicationProperties {

        // Source property name constants
        public static final String PROPERTY_SOURCE_TYPE = "source.type";
        public static final String PROPERTY_BOOTSTRAP_SERVERS = "source.msk.bootstrap.servers";
        public static final String PROPERTY_TOPIC_NAMES = "source.msk.topic.names";
        public static final String PROPERTY_GROUP_ID = "source.msk.group.id";
        public static final String PROPERTY_STARTING_OFFSET = "source.msk.starting.offset";
        public static final String PROPERTY_AUTH_TYPE = "source.msk.auth.type";
        public static final String PROPERTY_STREAM_DATA_TYPE = "source.msk.data.type";
        public static final String PROPERTY_MSK_PREFIX = "source.msk.kafka";

        // Embedding property name constants
        public static final String PROPERTY_EMBEDDING_MODEL_ID = "embed.model.id";
        public static final String PROPERTY_EMBEDDING_CHARSET = "embed.model.charset";
        public static final String PROPERTY_EMBEDDING_MODEL_OVERRIDES = "embed.model.overrides.";
        public static final String PROPERTY_EMBEDDING_INPUT_CONFIG = "embed.input.config.";
        public static final String PROPERTY_EMBEDDING_INPUT_JSON_FIELDS = "json.fieldsToEmbed";

        // Sink property name constants
        public static final String PROPERTY_OS_ENDPOINT = "sink.os.endpoint";
        public static final String PROPERTY_OS_INDEX = "sink.os.vector.index";
        public static final String PROPERTY_OS_TYPE = "sink.os.type";
        public static final String PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS = "sink.os.bulkFlushIntervalMillis";

    }

    /**
     * Class to hold names of constants for embedding model properties.
     * Ref: https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-titan-embed-mm.html
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class EmbeddingModelConfigurations {

        // Embedding configurations name constants
        public static final String NORMALIZE = "normalize";
        public static final String DIMENSIONS = "dimensions";
        public static final String OUTPUT_EMBEDDING_LENGTH = "outputEmbeddingLength";
        public static final String EMBEDDING_CONFIG = "embeddingConfig";
        public static final String INPUT_TYPE = "input_type";
        public static final String TRUNCATE = "truncate";
        public static final String EMBEDDING_TYPES = "embeddingTypes";

        public static final String INPUT_TYPE_SEARCH_DOCUMENT = "search_document";

        public static final List<String> OUTPUT_EMBEDDING_LENGTH_VALID_VALUES = List.of("256", "384", "1024");
        public static final List<String> INPUT_TYPE_VALID_VALUES = List.of("search_document", "search_query",
                "classification", "clustering");
   public static final List<String> TRUNCATE_VALID_VALUES = List.of("NONE", "START", "END");
   public static final List<String> EMBEDDING_TYPES_VALID_VALUES = List.of("None", "float", "int8", "uint8", "binary",
           "ubinary");

        public static final Map<String, List<String>> OVERRIDE_VALUES_TO_VALIDATIONS_MAP = Map.of(
                OUTPUT_EMBEDDING_LENGTH, OUTPUT_EMBEDDING_LENGTH_VALID_VALUES,
                INPUT_TYPE, INPUT_TYPE_VALID_VALUES,
                TRUNCATE, TRUNCATE_VALID_VALUES,
                EMBEDDING_TYPES, EMBEDDING_TYPES_VALID_VALUES
        );
    }
}
