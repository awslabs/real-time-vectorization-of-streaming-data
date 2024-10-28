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

import org.junit.jupiter.api.Test;

import java.util.Properties;

import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigurationUtilsTest {

    @Test
    void getRequiredEnum_ThrowsForInvalidValue() {
        assertThrows(MissingOrIncorrectConfigurationException.class,
                () -> ConfigurationUtils.getRequiredEnum(TestEnum.class, "INVALID_VALUE"));
    }

    @Test
    void getRequiredEnum_ReturnsForValidValue() {
        final TestEnum testEnum = ConfigurationUtils.getRequiredEnum(TestEnum.class, TestEnum.VALUE1.name());
        assert testEnum != null;
    }

    @Test
    void getRequiredProperty_ThrowsForMissingProperty() {
        assertThrows(MissingOrIncorrectConfigurationException.class,
                () -> ConfigurationUtils.getRequiredProperty(new Properties(), "missingKey"));
    }

    @Test
    void getRequiredProperty_ReturnsForPresentProperty() {
        final Properties properties = new Properties();
        properties.setProperty("presentKey", "value");
        final String value = ConfigurationUtils.getRequiredProperty(properties, "presentKey");
        assert value != null;
    }

    private enum TestEnum {
        VALUE1,
        VALUE2
    }
}
