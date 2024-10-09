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
package com.amazonaws.datastreamvectorization.wrappers;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Properties;

import com.amazonaws.datastreamvectorization.exceptions.ConfigurationLoadingException;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import static org.apache.commons.lang3.ObjectUtils.isEmpty;

/**
 * Wrapper class for providing the setup for Flink application
 */
@Slf4j
public class FlinkSetupProvider {
    public static final String APPLICATION_PROPERTIES_GROUP_NAME = "FlinkApplicationProperties";

    public static Properties loadApplicationProperties(@NonNull final String[] args,
                                                       @NonNull final StreamExecutionEnvironment environment)
            throws ConfigurationLoadingException {
        if (environment instanceof LocalStreamEnvironment) {
            return ParameterTool.fromArgs(args).getProperties();
        }

        Map<String, Properties> applicationProperties;
        try {
            applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        } catch (Exception e) {
            throw new ConfigurationLoadingException("Failed to load application properties from MSF application.", e);
        }

        Properties flinkProperties = applicationProperties.get(APPLICATION_PROPERTIES_GROUP_NAME);
        if (isEmpty(flinkProperties)) {
            throw new ConfigurationLoadingException("Unable to load runtime properties from MSF application.");
        }
        return flinkProperties;
    }
}
