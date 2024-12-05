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
package com.amazonaws.datastreamvectorization.integrationtests;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Class that creates Kafka admin and producer clients that connect to an MSK cluster
 */
class KafkaClients {
    private static final String CLIENT_ID = "test-kafka-helper-id";
    private final Properties securityProperties;
    private final Properties producerClientProperties;
    private final Properties adminClientProperties;

    public KafkaClients(String bootstrapBrokers) {
        this.securityProperties = getSecurityProperties();
        this.producerClientProperties = getProducerClientProperties(bootstrapBrokers);
        this.adminClientProperties = getAdminClientProperties(bootstrapBrokers);
    }

    /**
     * Create a Kafka producer with String key/value serializers
     *
     * @param testID ID string for the test
     * @return KafkaProducer<String, String>
     */
    public KafkaProducer<String, String> createKafkaStringProducer(String testID) {
        Serializer<String> serializer = new StringSerializer();
        return createKafkaProducer(testID, serializer, serializer);
    }

    /**
     * Creates a Kafka producer with customizable key/value serializers
     *
     * @param testID ID string for the test
     * @param keySerializer Key serializer for produced records
     * @param valueSerializer Value serializer for produced records
     * @return KafkaProducer<K, V>
     * @param <K> Key serializer data type
     * @param <V> Value serializer data type
     */
    public <K, V> KafkaProducer<K, V> createKafkaProducer(String testID,
                                                          Serializer<K> keySerializer,
                                                          Serializer<V> valueSerializer) {
        Properties producerProps = new Properties();
        producerProps.putAll(this.securityProperties);
        producerProps.putAll(this.producerClientProperties);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID + "-producer-" + testID);

        try {
            return new KafkaProducer<>(producerProps, keySerializer, valueSerializer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create a kafka producer.", e);
        }
    }

    /**
     * Create a Kafka admin client
     *
     * @param testID ID string for the test
     * @return AdminClient
     */
    public AdminClient createKafkaAdminClient(String testID) {
        Properties adminClientProps = new Properties();
        adminClientProps.putAll(this.securityProperties);
        adminClientProps.putAll(this.adminClientProperties);
        adminClientProps.put(AdminClientConfig.CLIENT_ID_CONFIG, CLIENT_ID + "-admin-" + testID);

        AdminClient adminClient;
        try {
            adminClient = AdminClient.create(adminClientProps);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create a kafka admin client.", e);
        }
        return adminClient;
    }

    private Properties getSecurityProperties() {
        Properties securityProperties = new Properties();
        // Sets up TLS for encryption and SASL for authN.
        securityProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        // Identifies the SASL mechanism to use.
        securityProperties.put(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
        // Binds SASL client implementation.
        securityProperties.put(SaslConfigs.SASL_JAAS_CONFIG,
                "software.amazon.msk.auth.iam.IAMLoginModule required;");
        // Encapsulates constructing a SigV4 signature based on extracted credentials.
        securityProperties.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return securityProperties;
    }

    private Properties getProducerClientProperties(String bootstrapBrokers) {
        Properties producerClientProperties = new Properties();
        producerClientProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        return producerClientProperties;
    }

    private Properties getAdminClientProperties(String bootstrapBrokers) {
        Properties adminClientProperties = new Properties();
        adminClientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        adminClientProperties.put(AdminClientConfig.RETRIES_CONFIG, 5);
        return adminClientProperties;
    }
}
