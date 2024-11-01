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
import com.amazonaws.datastreamvectorization.utils.ConfigurationUtils;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_ENDPOINT;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_INDEX;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_TYPE;
import static com.amazonaws.datastreamvectorization.utils.DataSinkValidationUtils.isValidOpenSearchIndexName;
import static com.amazonaws.datastreamvectorization.utils.DataSinkValidationUtils.validateFlushInterval;
import static com.amazonaws.datastreamvectorization.utils.ValidationUtils.hasValidProtocol;
import static com.amazonaws.datastreamvectorization.utils.ValidationUtils.isValidUrl;

/**
 * Configuration for OpenSearch data sink.
 */
@Slf4j
@Data
@Builder(builderMethodName = "internalBuilder")
public class OpenSearchDataSinkConfiguration implements DataSinkConfiguration {
    public static final long DEFAULT_OS_BULK_FLUSH_INTERVAL_MILLIS = 1;
    public static final String DEFAULT_ENDPOINT_PROTOCOL = "https://";

    @NonNull
    private String endpoint;
    @NonNull
    private String index;
    @NonNull
    private String region;
    @NonNull
    private OpenSearchType openSearchType;
    private long bulkFlushIntervalMillis;

    public static OpenSearchDataSinkConfigurationBuilder parseFrom(final String region, final Properties properties) {
        validateFlushInterval(properties);
        return internalBuilder()
                .region(region)
                .endpoint(getEndpoint(properties))
                .index(properties.getProperty(PROPERTY_OS_INDEX))
                .openSearchType(ConfigurationUtils.getRequiredEnum(OpenSearchType.class,
                        properties.getProperty(PROPERTY_OS_TYPE)))
                .bulkFlushIntervalMillis(Long.parseLong(properties.getProperty(PROPERTY_OS_BULK_FLUSH_INTERVAL_MILLIS,
                        String.valueOf(DEFAULT_OS_BULK_FLUSH_INTERVAL_MILLIS))));
    }

    private static String getEndpoint(Properties properties) {
        String endpointUrl = properties.getProperty(PROPERTY_OS_ENDPOINT);
        if (!hasValidProtocol(endpointUrl)) {
            String newEndpointUrl = DEFAULT_ENDPOINT_PROTOCOL + endpointUrl;
            log.info("Provided OpenSearch endpoint {} does not start with a protocol. Adding {} to the start of "
                    + "the OpenSearch endpoint URL: {}", endpointUrl, DEFAULT_ENDPOINT_PROTOCOL, newEndpointUrl);
            return newEndpointUrl;
        }
        return endpointUrl;
    }

    /**
     * Validates the configuration
     *
     * @throws MissingOrIncorrectConfigurationException if the config is invalid
     */
    public void validate() {
        if (!isValidUrl(this.endpoint)) {
            throw new MissingOrIncorrectConfigurationException("OpenSearch endpoint should be a valid url.");
        }
        if (!isValidOpenSearchIndexName(this.index)) {
            throw new MissingOrIncorrectConfigurationException("OpenSearch index name is invalid.");
        }
    }
}
