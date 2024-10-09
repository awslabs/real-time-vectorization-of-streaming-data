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
package com.amazonaws.datastreamvectorization.datasink;

import lombok.NonNull;
import org.apache.flink.api.connector.sink2.Sink;
import org.json.JSONObject;

import com.amazonaws.datastreamvectorization.datasink.model.DataSinkConfiguration;
import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchDataSinkConfiguration;
import com.amazonaws.datastreamvectorization.datasink.opensearch.OpenSearchSinkBuilder;
import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;

/**
 * Factory class for creating data sink instances based on the provided configuration.
 */
public class DataSinkFactory {

    /**
     * Returns a data sink based on the configuration provided. Currently, only OpenSearch is supported.
     *
     * @param config The configuration for the data sink
     * @return The data sink instance
     * @throws MissingOrIncorrectConfigurationException If the configuration is not supported
     */
    public Sink<JSONObject> getDataSink(@NonNull final DataSinkConfiguration config) {
        if (config instanceof OpenSearchDataSinkConfiguration) {
            return new OpenSearchSinkBuilder().getDataSink((OpenSearchDataSinkConfiguration) config);
        }
        throw new MissingOrIncorrectConfigurationException("Unsupported data sink configuration");
    }

}
