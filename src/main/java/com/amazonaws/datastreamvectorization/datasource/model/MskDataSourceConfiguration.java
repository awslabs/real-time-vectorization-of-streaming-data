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
package com.amazonaws.datastreamvectorization.datasource.model;

import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_AUTH_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_BOOTSTRAP_SERVERS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_GROUP_ID;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_MSK_PREFIX;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_STARTING_OFFSET;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_STREAM_DATA_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_TOPIC_NAMES;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.PROPERTY_VALUES_DELIMITER;
import static com.amazonaws.datastreamvectorization.utils.DataSourceValidationUtils.isValidKafkaConsumerGroupId;
import static com.amazonaws.datastreamvectorization.utils.DataSourceValidationUtils.isValidKafkaTopic;
import static com.amazonaws.datastreamvectorization.utils.DataSourceValidationUtils.validBootstrapServers;
import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.getProperties;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;
import static org.apache.commons.lang3.ObjectUtils.isNotEmpty;

/**
 * POJO for storing all MSK configurations needed to set up AWS MSK as data source.
 */
@Data
@Builder(builderMethodName = "internalBuilder")
public class MskDataSourceConfiguration implements DataSourceConfiguration {
    public static final StreamDataType DEFAULT_STREAM_DATA_TYPE = StreamDataType.STRING;
    public static final StartingOffset DEFAULT_STARTING_OFFSET = StartingOffset.EARLIEST;
    public static final MskAuthType DEFAULT_AUTH_TYPE = MskAuthType.IAM;
    public static final String DEFAULT_GROUP_ID = "msk-msf-group";
    public static final String TOPIC_PATTERN_INCLUDE_ALL = ".*";

    @NonNull
    private String bootstrapServers;
    private List<String> topicNames;
    private String topicPattern;
    private String groupId;
    private StartingOffset startingOffset;
    private Properties kafkaProperties;
    private MskAuthType authType;
    private StreamDataType streamDataType;

    public static MskDataSourceConfigurationBuilder parseFrom(Properties properties) {
        MskDataSourceConfigurationBuilder builder = internalBuilder();
        builder.bootstrapServers(properties.getProperty(PROPERTY_BOOTSTRAP_SERVERS));
        populateTopicConfiguration(builder, properties);
        builder.groupId(properties.getProperty(PROPERTY_GROUP_ID, DEFAULT_GROUP_ID));
        builder.authType(MskAuthType.valueOf(
                properties.getProperty(PROPERTY_AUTH_TYPE, DEFAULT_AUTH_TYPE.toString())));
        builder.streamDataType(StreamDataType.valueOf(
                properties.getProperty(PROPERTY_STREAM_DATA_TYPE, DEFAULT_STREAM_DATA_TYPE.toString())));
        builder.startingOffset(StartingOffset.valueOf(
                (properties.getProperty(PROPERTY_STARTING_OFFSET, DEFAULT_STARTING_OFFSET.toString()))));
        builder.kafkaProperties(getProperties(properties, PROPERTY_MSK_PREFIX));
        return builder;
    }

    public void validate() {
        if (!validBootstrapServers(bootstrapServers)) {
            throw new MissingOrIncorrectConfigurationException("MSK bootstrap servers not valid.");
        }
        if (isEmpty(topicNames) && isEmpty(topicPattern)) {
            throw new MissingOrIncorrectConfigurationException(
                    "Either MSK topic names or topic pattern should be provided.");
        }
        if (isNotEmpty(topicNames)) {
            this.topicNames.forEach(topicName -> {
                if (isEmpty(topicName) || isEmpty(topicName.trim())) {
                    throw new MissingOrIncorrectConfigurationException("Topic name should be a non-empty string.");
                }
                if (!isValidKafkaTopic(topicName)) {
                    throw new MissingOrIncorrectConfigurationException("Topic name " + topicName + " is not valid. "
                            + "Valid characters for topics are the ASCII Alphanumeric characters, ‘.’, ‘_’, and '-'.");
                }
            });
        }
        if (isNotEmpty(groupId) && !isValidKafkaConsumerGroupId(groupId)) {
            throw new MissingOrIncorrectConfigurationException("Group ID " + groupId + " is not valid. "
                    + "Valid characters for consumer groups are the ASCII alphanumeric characters, '.', '_' and '-'.");
        }
    }

    private static void populateTopicConfiguration(final MskDataSourceConfigurationBuilder builder,
                                                   final Properties properties) {
        if (properties.containsKey(PROPERTY_TOPIC_NAMES)) {
            String topicNamesValue = properties.getProperty(PROPERTY_TOPIC_NAMES).trim();
            if (TOPIC_PATTERN_INCLUDE_ALL.equals(topicNamesValue)) {
                builder.topicPattern(TOPIC_PATTERN_INCLUDE_ALL);
                return;
            }
            List<String> topicNames = Arrays.stream(properties.getProperty(PROPERTY_TOPIC_NAMES).trim()
                    .split(PROPERTY_VALUES_DELIMITER))
                    // remove extra spaces and empty strings
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            if (isNotEmpty(topicNames)) {
                builder.topicNames(topicNames);
            }
        } else {
            builder.topicPattern(TOPIC_PATTERN_INCLUDE_ALL);
        }
    }
}
