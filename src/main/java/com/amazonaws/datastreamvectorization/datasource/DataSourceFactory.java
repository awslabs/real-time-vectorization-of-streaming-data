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
package com.amazonaws.datastreamvectorization.datasource;

import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import lombok.NonNull;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;

import com.amazonaws.datastreamvectorization.datasource.model.DataSourceConfiguration;
import com.amazonaws.datastreamvectorization.datasource.model.MskAuthType;
import com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration;
import com.amazonaws.datastreamvectorization.exceptions.UnsupportedInputStreamDataTypeException;

import java.util.regex.Pattern;

/**
 * Factory class for providing a client needed to set up a data source.
 */
public class DataSourceFactory {

    public static Source getDataSource(@NonNull final DataSourceConfiguration config) {
        if (config instanceof MskDataSourceConfiguration) {
            final MskDataSourceConfiguration configuration = (MskDataSourceConfiguration) config;
            KafkaSourceBuilder builder = getMinimalConfigBuilder(configuration);
            populateTopicNameForBuilder(configuration, builder);
            populateConfigForBuilder(configuration, builder);
            populateConfigForAuth(configuration, builder);

            switch (configuration.getStreamDataType()) {
                case JSON:
                    builder.setValueOnlyDeserializer(new JSONObjectDeserializationSchema());
                    break;
                case STRING:
                    builder.setValueOnlyDeserializer(new SimpleStringSchema());
                    break;
                default:
                    throw new UnsupportedInputStreamDataTypeException("Unsupported stream data type. "
                            + "Only STRING or JSON are supported.");
            }
            return builder.build();
        }
        throw new UnsupportedInputStreamDataTypeException("Unsupported data source configuration. "
                + "Only MSK is supported.");
    }

    /**
     * Method to create {@link  KafkaSourceBuilder} with all required configs.
     *
     * @param configuration MskDataSourceConfiguration
     * @return
     */
    private static KafkaSourceBuilder getMinimalConfigBuilder(final MskDataSourceConfiguration configuration) {
        return KafkaSource.builder()
                .setBootstrapServers(configuration.getBootstrapServers())
                // https://tinyurl.com/3s9h6waz
                .setProperty("partition.discovery.interval.ms", "60000"); // discover new partitions per minute
    }

    /**
     * Method to set topic names fields for MSK config builder.
     *
     * @param configuration
     */
    private static void populateTopicNameForBuilder(final MskDataSourceConfiguration configuration,
                                                 final KafkaSourceBuilder builder) {
        if (ObjectUtils.isNotEmpty(configuration.getTopicNames())) {
            builder.setTopics(configuration.getTopicNames());
        } else if (ObjectUtils.isNotEmpty(configuration.getTopicPattern())) {
            Pattern topicPattern = Pattern.compile(configuration.getTopicPattern());
            builder.setTopicPattern(topicPattern);
        } else {
            throw new MissingOrIncorrectConfigurationException("Need at least topic name or topic pattern "
                    + "to read from the input stream.");
        }
    }

    /**
     * Method to set additional fields for MSK config builder.
     *
     * @param configuration
     */
    private static void populateConfigForBuilder(final MskDataSourceConfiguration configuration,
                                                 final KafkaSourceBuilder builder) {
        if (ObjectUtils.isNotEmpty(configuration.getGroupId())) {
            builder.setGroupId(configuration.getGroupId());
        }
        if (ObjectUtils.isNotEmpty(configuration.getStartingOffset())) {
            builder.setStartingOffsets(configuration.getStartingOffset().getOffsetsInitializer());
        }
        if (ObjectUtils.isNotEmpty(configuration.getKafkaProperties())) {
            builder.setProperties(configuration.getKafkaProperties());
        }
    }

    /**
     * Method to set auth-related fields for MSK config builder.
     * Ref: https://github.com/aws/aws-msk-iam-auth/blob/main/README.md
     *
     * @param configuration
     * @param builder
     */
    private static void populateConfigForAuth(final MskDataSourceConfiguration configuration,
                                              final KafkaSourceBuilder builder) {
        if (ObjectUtils.isNotEmpty(configuration.getAuthType())
                && MskAuthType.IAM.equals(configuration.getAuthType())) {
            builder.setProperty("security.protocol", "SASL_SSL")
                    .setProperty("sasl.mechanism", "AWS_MSK_IAM")
                    .setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
                    .setProperty("sasl.client.callback.handler.class",
                            "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        }
    }
}
