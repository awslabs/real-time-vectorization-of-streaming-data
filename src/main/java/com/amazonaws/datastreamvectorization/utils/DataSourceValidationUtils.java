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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.PROPERTY_VALUES_DELIMITER;
import static com.amazonaws.datastreamvectorization.utils.ValidationUtils.VALID_IP_PATTERN;
import static com.amazonaws.datastreamvectorization.utils.ValidationUtils.VALID_URL_PATTERN;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;

/*
Data source validation utilities.
 */
@Slf4j
public class DataSourceValidationUtils {

    public static final Pattern VALID_KAFKA_TOPIC_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]*$");
    public static final Pattern VALID_KAFKA_GROUP_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]{1,255}$");


    /**
     * Valid characters for Kafka consumer group ids are the ASCII alphanumeric characters, '.', '_' and '-'.
     * Ref: https://www.ibm.com/docs/en/app-connect/11.0.0?topic=enterprise-consuming-messages-from-kafka-topics
     * @param groupId
     * @return
     */
    public static boolean isValidKafkaConsumerGroupId(final String groupId) {
        return VALID_KAFKA_GROUP_ID_PATTERN.matcher(groupId).matches();
    }

    /**
     * Valid characters for Kafka topics are the ASCII Alphanumeric characters, ‘.’, ‘_’, and '-'.
     * Ref: https://www.ibm.com/docs/en/app-connect/11.0.0?topic=enterprise-consuming-messages-from-kafka-topics
     * @param topic
     * @return
     */
    public static boolean isValidKafkaTopic(final String topic) {
        return VALID_KAFKA_TOPIC_PATTERN.matcher(topic).matches();
    }

    /**
     * Validate bootstrap servers. Valid bootstrap server format: <host>:<port>
     * @param bootstrapServers
     * @return
     */
    public static boolean validBootstrapServers(String bootstrapServers) {
        List<String> servers = Arrays.stream(bootstrapServers.split(PROPERTY_VALUES_DELIMITER))
                .map(String::trim)
                .collect(Collectors.toList());

        for (String server : servers) {
            if (!isValidBootstrapServer(server)) {
                return false;
            }
        }

        return true;
    }

    private static boolean isValidBootstrapServer(final String bootstrapServer) {
        try {
            if (isEmpty(bootstrapServer)) {
                return false;
            }
            String[] hostPort = bootstrapServer.split(":");
            if (hostPort.length != 2) {
                log.warn("Expected bootstrap server format: <boostrap-server>:<host>");
                return false;
            }

            String host = hostPort[0];
            int port = Integer.parseInt(hostPort[1]);

            if (!isValidHost(host)) {
                return false;
            }

            if (port < 1 || port > 65535) {
                return false;
            }
        } catch (Exception e) {
            log.error("Invalid bootstrap server: {}", bootstrapServer, e);
            return false;
        }

        return true;
    }

    private static boolean isValidHost(final String host) {
        Matcher domainMatcher = VALID_URL_PATTERN.matcher(host);
        Matcher ipMatcher = VALID_IP_PATTERN.matcher(host);
        return domainMatcher.matches() || ipMatcher.matches();
    }

}
