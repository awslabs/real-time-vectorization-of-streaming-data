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
package com.amazonaws.datastreamvectorization.utils;

import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Properties;
import java.util.stream.Stream;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataSinkValidationUtilsTest {

    private static Stream<Arguments> provideInvalidIndexNames() {
        return Stream.of(Arguments.of(" ", "_index", "-index", "index name", "index,name", "index:name",
                "index\"name", "index*", "index+name", "index/name", "index\\name", "index|name", "index-name?",
                "index-name#", "index->name", "index<-name"
        ));
    }

    private static Stream<Arguments> provideValidIndexNames() {
        return Stream.of(Arguments.of("index", "index-name", "index_name", "index123"));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidIndexNames")
    void isValidOpenSearchIndexName_InvalidName(String indexName) {
        assertFalse(DataSinkValidationUtils.isValidOpenSearchIndexName(indexName));
    }

    @ParameterizedTest
    @MethodSource("provideValidIndexNames")
    void isValidOpenSearchIndexName_ValidName(String indexName) {
        assertTrue(DataSinkValidationUtils.isValidOpenSearchIndexName(indexName));
    }


    private static Stream<Arguments> provideInvalidFlushIntervals() {
        Properties propertiesWithEmptyFlushInterval = new Properties();
        propertiesWithEmptyFlushInterval.setProperty(PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS, StringUtils.EMPTY);

        Properties propertiesWithNegativeFlushInterval = new Properties();
        propertiesWithNegativeFlushInterval.setProperty(PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS, "-100");

        Properties propertiesWithInvalidFlushInterval = new Properties();
        propertiesWithInvalidFlushInterval.setProperty(PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS, "invalidInterval");

        return Stream.of(Arguments.of(
                propertiesWithEmptyFlushInterval,
                "OpenSearch bulk flush interval must be a non-empty positive long value.",
                propertiesWithNegativeFlushInterval,
                "OpenSearch bulk flush interval must be a positive value.",
                propertiesWithEmptyFlushInterval,
                "OpenSearch bulk flush interval must be a valid long value."
        ));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidFlushIntervals")
    void validate_ThrowsOnInvalidFlushInterval(Properties properties, String expectedExceptionMessage) {
        Exception exception = assertThrows(MissingOrIncorrectConfigurationException.class, () ->
                DataSinkValidationUtils.validateFlushInterval(properties));
        assertEquals(expectedExceptionMessage, exception.getMessage());
    }

    @Test
    void validateFlushInterval_ValidInterval() {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS, "100");
        DataSinkValidationUtils.validateFlushInterval(properties);
    }

}
