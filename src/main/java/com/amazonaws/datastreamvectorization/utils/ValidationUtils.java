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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A utility class for validation.
 */
@Slf4j
public class ValidationUtils {

    public static final Pattern VALID_URL_PATTERN =
            Pattern.compile("[-a-zA-Z0-9/@%._~&+$]+\\.[a-zA-Z]{2,6}\\b[-a-zA-Z0-9~/=+:?#_%&$(),]*");
    private static final Pattern VALID_PROTOCOL_PATTERN = Pattern.compile("^((http|https)://).*");
    private static final Pattern VALID_HTTP_URL_PATTERN =
            Pattern.compile("((http|https)://)" + VALID_URL_PATTERN);
    public static final Pattern VALID_IP_PATTERN =
            Pattern.compile("^\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}$|localhost");

    /**
     * Checks if a url is valid.
     *
     * @param url
     * @return true if valid, false otherwise.
     */
    public static boolean isValidUrl(final String url) {
        if (StringUtils.isEmpty(url)) {
            return false;
        }
        final Matcher matcher = VALID_HTTP_URL_PATTERN.matcher(url);
        return matcher.matches();
    }

    /**
     * Checks if a URL starts with a valid protocol
     *
     * @param url
     * @return True if the URL starts with a valid protocol
     */
    public static boolean hasValidProtocol(final String url) {
        final Matcher matcher = VALID_PROTOCOL_PATTERN.matcher(url);
        return matcher.matches();
    }
}
