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

import lombok.NonNull;
import org.apache.commons.lang3.EnumUtils;

import java.util.Properties;

import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;

import static org.apache.commons.lang3.ObjectUtils.isEmpty;

public class ConfigurationUtils {

    /**
     * Gets the required enum constant from the given enum class.
     *
     * @param <E>       The enum type
     * @param enumClass The enum class to get the enum constant from
     * @param value     The value of the enum constant to get
     * @return The enum constant with the given value
     * @throws MissingOrIncorrectConfigurationException If the value is not a valid enum constant
     */
    public static <E extends Enum<E>> E getRequiredEnum(@NonNull final Class<E> enumClass, final String value) {
        if (!EnumUtils.isValidEnum(enumClass, value)) {
            throw new MissingOrIncorrectConfigurationException(
                    String.format("%s is not a valid value for %s enum", value, enumClass.getSimpleName()));
        }

        return EnumUtils.getEnum(enumClass, value);
    }

    /**
     * Gets the required property value from the given properties.
     *
     * @param key        The key of the property to get the value for
     * @param properties The properties to get the value from
     * @return The value of the property with the given key
     * @throws MissingOrIncorrectConfigurationException If the property is missing or empty
     */
    public static String getRequiredProperty(@NonNull final Properties properties, final String key) {
        final String value = properties.getProperty(key);
        if (isEmpty(value)) {
            throw new MissingOrIncorrectConfigurationException(key + " is required.");
        }
        return value;
    }
}
