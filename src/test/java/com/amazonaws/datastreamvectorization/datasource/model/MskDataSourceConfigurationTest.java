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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_BOOTSTRAP_SERVERS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_GROUP_ID;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_TOPIC_NAMES;
import static com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration.TOPIC_PATTERN_INCLUDE_ALL;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MskDataSourceConfigurationTest {


    private static Properties getValidProperties() {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_BOOTSTRAP_SERVERS,
                "boot-vieujqnz.c2.kafka-serverless.us-east-1.amazonaws.com:9098");
        return properties;
    }

    private static Stream<Arguments> provideInvalidConfigurations() {
        Properties propertiesWithNonEmptyTopicName = getValidProperties();
        propertiesWithNonEmptyTopicName.setProperty(PROPERTY_TOPIC_NAMES, " ");

        String invalidTopicNames = "topic*";
        Properties propertiesWithIncorrectTopicName = getValidProperties();
        propertiesWithIncorrectTopicName.setProperty(PROPERTY_TOPIC_NAMES, invalidTopicNames);

        String invalidGroupId = "k@fk@+group";
        Properties propertiesWithInvalidGroupId = getValidProperties();
        propertiesWithInvalidGroupId.setProperty(PROPERTY_TOPIC_NAMES, "validTopicName");
        propertiesWithInvalidGroupId.setProperty(PROPERTY_GROUP_ID, invalidGroupId);

        MskDataSourceConfiguration mskConfigWithoutTopicNameOrPattern =
                MskDataSourceConfiguration.internalBuilder().bootstrapServers("localhost:9098").build();
        MskDataSourceConfiguration mskConfigWithIncorrectBrokerName =
                MskDataSourceConfiguration.internalBuilder().bootstrapServers("abc123.com").build();
        return Stream.of(
                Arguments.of(mskConfigWithIncorrectBrokerName,
                        "MSK bootstrap servers not valid."),

                Arguments.of(MskDataSourceConfiguration.parseFrom(propertiesWithInvalidGroupId).build(),
                        "Group ID " + invalidGroupId + " is not valid. Valid characters for consumer groups are "
                                + "the ASCII alphanumeric characters, '.', '_' and '-'."),

                // invalid topic name configs
                Arguments.of(MskDataSourceConfiguration.parseFrom(propertiesWithNonEmptyTopicName).build(),
                        "Either MSK topic names or topic pattern should be provided."),
                Arguments.of(MskDataSourceConfiguration.parseFrom(propertiesWithIncorrectTopicName).build(),
                        "Topic name " + invalidTopicNames + " is not valid. Valid characters for topics are "
                                + "the ASCII Alphanumeric characters, ‘.’, ‘_’, and '-'."),
                Arguments.of(mskConfigWithoutTopicNameOrPattern,
                        "Either MSK topic names or topic pattern should be provided.")
        );
    }

    private static Stream<Arguments> provideValidConfigurations() {
        Properties properties = getValidProperties();

        Properties propertiesWithExtraSpacesInTopicName = getValidProperties();
        propertiesWithExtraSpacesInTopicName.setProperty(PROPERTY_TOPIC_NAMES, " topic1 , ,topic2 ");

        return Stream.of(
                Arguments.of(MskDataSourceConfiguration.parseFrom(properties).build()),
                Arguments.of(MskDataSourceConfiguration.parseFrom(propertiesWithExtraSpacesInTopicName).build())
        );
    }

    @ParameterizedTest
    @MethodSource("provideInvalidConfigurations")
    void validate_ThrowsOnInvalidConfig(MskDataSourceConfiguration config, String expectedExceptionMessage) {
        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class, config::validate);
        assertEquals(expectedExceptionMessage, exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("provideValidConfigurations")
    void validate_SuccessOnValidConfig(MskDataSourceConfiguration config) {
        config.validate();
    }

    @Test
    public void testParse_WithTopicName_IncludeTopics() {
        Properties properties = getValidProperties();
        properties.setProperty(PROPERTY_TOPIC_NAMES, "topic1, topic2");
        MskDataSourceConfiguration config = MskDataSourceConfiguration.parseFrom(properties).build();
        assertEquals(List.of("topic1", "topic2"), config.getTopicNames());
        assertTrue(isEmpty(config.getTopicPattern()));
    }

    @Test
    public void testParse_WithoutTopicName_IncludeAllTopicsPattern() {
        MskDataSourceConfiguration config = MskDataSourceConfiguration.parseFrom(getValidProperties()).build();
        assertEquals(TOPIC_PATTERN_INCLUDE_ALL, config.getTopicPattern());
        assertTrue(isEmpty(config.getTopicNames()));
    }

    @Test
    public void testParse_WithIncludeAllTopicName_IncludeAllTopicsPattern() {
        Properties properties = getValidProperties();
        properties.setProperty(PROPERTY_TOPIC_NAMES, TOPIC_PATTERN_INCLUDE_ALL);
        MskDataSourceConfiguration config = MskDataSourceConfiguration.parseFrom(getValidProperties()).build();
        assertEquals(TOPIC_PATTERN_INCLUDE_ALL, config.getTopicPattern());
        assertTrue(isEmpty(config.getTopicNames()));
    }
}
