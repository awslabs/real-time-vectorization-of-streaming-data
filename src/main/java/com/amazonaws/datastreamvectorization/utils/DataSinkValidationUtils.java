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
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;

/*
Data source validation utilities.
 */
@Slf4j
public class DataSinkValidationUtils {

    private static final String INVALID_CHARACTERS = ":, \"*+/\\|?#><";
    /**
     * Checks if the given index name is valid for OpenSearch.
     * OpenSearch indices have the following naming restrictions:
     *     All letters must be lowercase.
     *     Index names can’t begin with underscores (_) or hyphens (-).
     *     Index names can’t contain spaces, commas, or the following characters:
     *     :, ", *, +, /, \, |, ?, #, >, or <
     *     https://docs.aws.amazon.com/opensearch-service/latest/developerguide/indexing.html
     * @param indexName The index name to validate.
     * @return true if the index name is valid, false otherwise.
     */
    public static boolean isValidOpenSearchIndexName(final String indexName) {
        if (isEmpty(indexName)) {
            log.warn("Index name cannot be empty.");
            return false;
        }
        if (indexName.startsWith("_") || indexName.startsWith("-")) {
            log.warn("Index name cannot start with an underscore or hyphen.");
            return false;
        }
        for (char c : INVALID_CHARACTERS.toCharArray()) {
            if (indexName.contains(String.valueOf(c))) {
                log.warn("Index name contains invalid characters.");
                return false;
            }
        }
        if (!indexName.equals(indexName.toLowerCase())) {
            log.warn("Index name must be in lowercase.");
            return false;
        }
        return true;
    }

    /**
     * Validates the OpenSearch bulk flush interval property.
     * @throws MissingOrIncorrectConfigurationException if the property is missing or invalid
     * @param properties
     */
    public static void validateFlushInterval(@NonNull final Properties properties) {
        if (!properties.containsKey(PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS)) {
            // nothing to validate
            return;
        }
        try {
            String flushIntervalString = properties.getProperty(PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS);
            if (StringUtils.isEmpty(flushIntervalString)) {
                throw new MissingOrIncorrectConfigurationException(
                        "OpenSearch bulk flush interval must be a non-empty positive long value.");
            }
            long flushInterval = Long.parseLong(flushIntervalString);
            if (flushInterval < 0) {
                throw new MissingOrIncorrectConfigurationException(
                        "OpenSearch bulk flush interval must be a positive value.");
            }
        } catch (ClassCastException | NumberFormatException e) {
            throw new MissingOrIncorrectConfigurationException(
                    "OpenSearch bulk flush interval must be a valid long value.", e);
        }
    }


}
