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

import com.amazonaws.datastreamvectorization.datasink.model.DataSinkConfiguration;
import com.amazonaws.datastreamvectorization.datasink.model.DataSinkType;
import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchDataSinkConfiguration;
import com.amazonaws.datastreamvectorization.datasource.model.DataSourceConfiguration;
import com.amazonaws.datastreamvectorization.datasource.model.DataSourceType;
import com.amazonaws.datastreamvectorization.datasource.model.MskDataSourceConfiguration;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.EmbeddingModelConfigurations.OVERRIDE_VALUES_TO_VALIDATIONS_MAP;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_OVERRIDES;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_SOURCE_TYPE;
import static com.amazonaws.datastreamvectorization.datasink.model.DataSinkConfiguration.PROPERTY_SINK_TYPE;

@Slf4j
@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class PropertiesUtils {

    /**
     * Get properties which starts with given prefix from application properties.
     *
     * @param applicationProperties - application properties
     * @param startsWith            - prefix to filter properties
     * @return properties which starts with given prefix or empty properties if no properties found
     */
    public static @NonNull Properties getProperties(@NonNull Properties applicationProperties,
                                                    @NonNull String startsWith) {
        Properties properties = new Properties();
        applicationProperties.forEach((key, value) -> {
            Optional.ofNullable(key).map(Object::toString).filter(k -> k.startsWith(startsWith))
                    .ifPresent(k -> {
                        properties.put(k.substring(startsWith.length()), value);
                    });
        });
        return properties;
    }

    /**
     * Get properties map which starts with given prefix from application properties.
     *
     * @param applicationProperties - application properties
     * @param startsWith            - prefix to filter properties
     * @return properties map which starts with given prefix or empty map if no properties found
     */
    public static Map<String, Object> getPropertiesMap(@NonNull Properties applicationProperties,
                                                       @NonNull String startsWith) {
        return getProperties(applicationProperties, startsWith)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
    }

    /**
     * Get embedding model overrides from properties.
     * @param embeddingModel
     * @param properties
     * @return
     */
    public static Map<String, Object>  getEmbeddingModelOverrides(final EmbeddingModel embeddingModel,
                                                                  final Properties properties) {
        Map<String, Object> clientOverridesMap = getPropertiesMap(properties, PROPERTY_EMBEDDING_MODEL_OVERRIDES);
        Map<String, Object> resultMap = new HashMap<>();

        embeddingModel.getConfigurationKeyDataTypeMap().forEach((key, expectedClass) -> {
            if (clientOverridesMap.containsKey(key)) {
                String value = clientOverridesMap.get(key).toString();
                if (!validateEmbeddingModelConfig(key, value)) {
                    throw new MissingOrIncorrectConfigurationException("Invalid value " + value
                            + " found for configuration " + key + ". Please refer documentation for allowed values.");
                } else if (expectedClass == String.class) {
                    resultMap.put(key, clientOverridesMap.get(key).toString());
                } else if (expectedClass == Integer.class) {
                    validatePositiveInteger(key, clientOverridesMap);
                    resultMap.put(key, Integer.parseInt(value));
                } else if (expectedClass == Boolean.class) {
                    resultMap.put(key, Boolean.parseBoolean(value));
                }
            }
        });
        return resultMap;
    }

    /**
     * Get embedding configuration from properties.
     * @param applicationProperties
     * @return
     */
    public static EmbeddingConfiguration getEmbeddingConfiguration(Properties applicationProperties) {
        EmbeddingConfiguration embeddingConfiguration =
                EmbeddingConfiguration.parseFrom(applicationProperties).build();
        embeddingConfiguration.validate();
        return embeddingConfiguration;
    }

    /**
     * Get data sink configuration from properties.
     * @param region
     * @param applicationProperties
     * @return
     */
    public static DataSinkConfiguration getDataSinkConfiguration(String region, Properties applicationProperties) {
        DataSinkType dataSinkType = ConfigurationUtils.getRequiredEnum(DataSinkType.class,
                applicationProperties.getProperty(PROPERTY_SINK_TYPE));
        if (dataSinkType == DataSinkType.OPENSEARCH) {
            OpenSearchDataSinkConfiguration openSearchConfig =
                    OpenSearchDataSinkConfiguration.parseFrom(region, applicationProperties).build();
            openSearchConfig.validate();
            return openSearchConfig;
        }
        throw new MissingOrIncorrectConfigurationException("Unsupported data sink type: " + dataSinkType);
    }

    /**
     * Get data source configuration from properties.
     * @param applicationProperties
     * @return
     */
    public static DataSourceConfiguration getDataSourceConfiguration(Properties applicationProperties) {
        DataSourceType dataSourceType = ConfigurationUtils.getRequiredEnum(DataSourceType.class,
                applicationProperties.getProperty(PROPERTY_SOURCE_TYPE));
        if (dataSourceType == DataSourceType.MSK) {
            MskDataSourceConfiguration mskConfig = MskDataSourceConfiguration.parseFrom(applicationProperties).build();
            mskConfig.validate();
            return mskConfig;
        }
        throw new MissingOrIncorrectConfigurationException("Unsupported data source type: " + dataSourceType);
    }

    /**
     * Validate if the value for a given key in properties map is a positive integer.
     * @param integerKey
     * @param propertiesMap
     */
    private static void validatePositiveInteger(final String integerKey, final Map<String, Object> propertiesMap) {
        if (!propertiesMap.containsKey(integerKey)) {
            // nothing to validate
            return;
        }
        try {
            String integerString = propertiesMap.get(integerKey).toString().trim();
            if (StringUtils.isEmpty(integerString)) {
                throw new MissingOrIncorrectConfigurationException(
                        integerKey + " must be a non-empty positive integer value.");
            }
            int integerValue = Integer.parseInt(integerString);
            if (integerValue < 0) {
                throw new MissingOrIncorrectConfigurationException(integerKey +  " must be a positive value.");
            }
        } catch (ClassCastException | NumberFormatException e) {
            throw new MissingOrIncorrectConfigurationException(integerKey + " must be a valid integer value.", e);
        }
    }

    /**
     * Ref: https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-embed.html
     * @param configName
     * @param configValue
     * @return
     */
    private static boolean validateEmbeddingModelConfig(final String configName, final String configValue) {
        if (OVERRIDE_VALUES_TO_VALIDATIONS_MAP.containsKey(configName)) {
            return OVERRIDE_VALUES_TO_VALIDATIONS_MAP.get(configName).stream().anyMatch(v -> v.equals(configValue));
        }
         return true;
    }

}