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
package com.amazonaws.datastreamvectorization.datasink.model;

import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Properties;
import java.util.stream.Stream;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_ENDPOINT;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_INDEX;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OpenSearchDataSinkConfigurationTest {

    private static Properties provideValidPropertiesMap() {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_OS_ENDPOINT, "https://ad77zixjklwz3asd0dti.us-east-1.aoss.amazonaws.com");
        properties.setProperty(PROPERTY_OS_INDEX, "index");
        properties.setProperty(PROPERTY_OS_TYPE, OpenSearchType.SERVERLESS.name());
        return properties;
    }

    private static Stream<Arguments> provideInvalidConfigurations() {
        Properties propertiesWithInvalidUrl = provideValidPropertiesMap();
        propertiesWithInvalidUrl.setProperty(PROPERTY_OS_ENDPOINT, "ftp:/invalid_url/");
       Properties propertiesWithInvalidIndexName = provideValidPropertiesMap();
        propertiesWithInvalidIndexName.setProperty(PROPERTY_OS_INDEX, "_invalidIndex");
        return Stream.of(Arguments.of(
                OpenSearchDataSinkConfiguration.parseFrom("us-east-1", propertiesWithInvalidUrl)
                        .build(),
                "OpenSearch endpoint should be a valid url.",
                OpenSearchDataSinkConfiguration.parseFrom("us-east-1", propertiesWithInvalidIndexName).build(),
                "OpenSearch index name is invalid."));
    }

    private static Stream<Arguments> provideValidConfigurations() {
        return Stream.of(Arguments.of(OpenSearchDataSinkConfiguration
                .parseFrom("us-east-1", provideValidPropertiesMap()).build()));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidConfigurations")
    void validate_ThrowsOnInvalidConfig(OpenSearchDataSinkConfiguration config, String expectedExceptionMessage) {
        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class, config::validate);
        assertEquals(expectedExceptionMessage, exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("provideValidConfigurations")
    void validate_SuccessOnValidConfig(OpenSearchDataSinkConfiguration config) {
        config.validate();
    }
}
