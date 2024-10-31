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

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidationUtilsTest {

    @ParameterizedTest
    @ValueSource(strings = {StringUtils.EMPTY, "  ", "urn:some:uri:1.2.3", "telnet://aws.jam", "https:amazon.com", "https::amazon.com",
            "ftp://amazon.com", "https://", "http://", "http://abc"})
    void isValidUrl_ShouldReturnFalse(String input) {
        assertFalse(ValidationUtils.isValidUrl(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://ad77zixjklwz3asd0dti.us-east-1.aoss.amazonaws.com",
            "https://vpc-test-7a27u4iagoq5cthd2m6upmenlu.us-east-1.es.amazonaws.com",
            "https://dashboards.us-east-1.aoss.amazonaws.com/_login/?collectionId=ad77zixjklwz3asd0dti"})
    void isValidUrl_ShouldReturnTrue(String input) {
        assertTrue(ValidationUtils.isValidUrl(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"htt://ad77zixjklwz3asd0dti.us-east-1.aoss.amazonaws.com",
            "http:/vpc-test-7a27u4iagoq5cthd2m6upmenlu.us-east-1.es.amazonaws.com",
            "abc", "https//vpc-test-7a27u4iagoq5cthd2m6upmenlu.us-east-1.es.amazonaws.com",
            "ahttps://ad77zixjklwz3asd0dti.us-east-1.aoss.amazonaws.com", ""})
    void hasValidProtocol_ShouldReturnFalse(String input) {
        assertFalse(ValidationUtils.hasValidProtocol(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://ad77zixjklwz3asd0dti.us-east-1.aoss.amazonaws.com",
            "http://vpc-test-7a27u4iagoq5cthd2m6upmenlu.us-east-1.es.amazonaws.com",
            "https:///ad77zixjklwz3asd0dti.us-east-1.aoss.amazonaws.com",
            "https://", "http://", "http://abc"})
    void hasValidProtocol_ShouldReturnTrue(String input) {
        assertTrue(ValidationUtils.hasValidProtocol(input));
    }
}
