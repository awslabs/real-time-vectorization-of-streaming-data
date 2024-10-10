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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.datastreamvectorization.datasink.model.DataSinkType;
import com.amazonaws.datastreamvectorization.datasource.model.DataSourceType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import static com.amazonaws.datastreamvectorization.datasink.model.DataSinkConfiguration.PROPERTY_SINK_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_ENDPOINT;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_INDEX;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_SOURCE_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_BOOTSTRAP_SERVERS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_ID;
import static com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType.SERVERLESS;
import static com.amazonaws.datastreamvectorization.wrappers.FlinkSetupProvider.APPLICATION_PROPERTIES_GROUP_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class FlinkSetupProviderTest {
    private static final String TEST_SOURCE_TYPE = DataSourceType.MSK.name();
    private static final String TEST_SOURCE_BOOTSTRAP_SERVERS = "localhost:9098";
    private static final String TEST_EMBED_MODEL_ID = EmbeddingModel.AMAZON_TITAN_TEXT_G1.getModelId();
    private static final String TEST_SINK_TYPE = DataSinkType.OPENSEARCH.name();
    private static final String TEST_OS_TYPE = SERVERLESS.name();
    private static final String TEST_OS_DOMAIN = "https://some-domain.test-value.com";
    private static final String TEST_OS_CUSTOM_INDEX = "test-index";

    private static final String[] APP_PROPERTIES_ARGS = new String[] {
            "--" + PROPERTY_SOURCE_TYPE, TEST_SOURCE_TYPE,
            "--" + PROPERTY_BOOTSTRAP_SERVERS, TEST_SOURCE_BOOTSTRAP_SERVERS,
            "--" + PROPERTY_EMBEDDING_MODEL_ID, TEST_EMBED_MODEL_ID,
            "--" + PROPERTY_SINK_TYPE, TEST_SINK_TYPE,
            "--" + PROPERTY_OS_ENDPOINT, TEST_OS_DOMAIN,
            "--" + PROPERTY_OS_INDEX, TEST_OS_CUSTOM_INDEX,
            "--" + PROPERTY_OS_TYPE, TEST_OS_TYPE
    };
    private static final Properties APP_PROPERTIES = ParameterTool.fromMap(
            Map.of(PROPERTY_SOURCE_TYPE, TEST_SOURCE_TYPE,
                    PROPERTY_BOOTSTRAP_SERVERS, TEST_SOURCE_BOOTSTRAP_SERVERS,
                    PROPERTY_EMBEDDING_MODEL_ID, TEST_EMBED_MODEL_ID,
                    PROPERTY_SINK_TYPE, TEST_SINK_TYPE,
                    PROPERTY_OS_ENDPOINT, TEST_OS_DOMAIN,
                    PROPERTY_OS_INDEX, TEST_OS_CUSTOM_INDEX,
                    PROPERTY_OS_TYPE, TEST_OS_TYPE
            )
    ).getProperties();

    @Mock
    private StreamExecutionEnvironment mockStreamingExecutionEnvironment;
    private StreamExecutionEnvironment spyExecutionEnvironment;

    @BeforeEach
    public void setUp() throws Exception {
        spyExecutionEnvironment = Mockito.spy(LocalStreamEnvironment.getExecutionEnvironment());
        doReturn(null).when(spyExecutionEnvironment).execute(anyString());
    }

    private static void assertApplicationProperties(Properties testParameters) {
        Assert.assertNotNull(testParameters);
        assertThat(testParameters.getProperty(PROPERTY_SOURCE_TYPE), is(TEST_SOURCE_TYPE));
        assertThat(testParameters.getProperty(PROPERTY_BOOTSTRAP_SERVERS), is(TEST_SOURCE_BOOTSTRAP_SERVERS));
        assertThat(testParameters.getProperty(PROPERTY_EMBEDDING_MODEL_ID), is(TEST_EMBED_MODEL_ID));
        assertThat(testParameters.getProperty(PROPERTY_SINK_TYPE), is(TEST_SINK_TYPE));
        assertThat(testParameters.getProperty(PROPERTY_OS_ENDPOINT), is(TEST_OS_DOMAIN));
        assertThat(testParameters.getProperty(PROPERTY_OS_INDEX), is(TEST_OS_CUSTOM_INDEX));
        assertThat(testParameters.getProperty(PROPERTY_OS_TYPE), is(TEST_OS_TYPE));
    }

    @Test
    public void testLoadApplicationProperties() {
        try (MockedStatic<KinesisAnalyticsRuntime> mockKinesisAnalyticsRuntime = mockStatic(
                KinesisAnalyticsRuntime.class)) {
            final Map<String, Properties> mockApplicationProperties = new HashMap<>();
            mockApplicationProperties.put(APPLICATION_PROPERTIES_GROUP_NAME, APP_PROPERTIES);
            mockKinesisAnalyticsRuntime.when(KinesisAnalyticsRuntime::getApplicationProperties)
                    .thenReturn(mockApplicationProperties);
            final Properties testParameters =
                    FlinkSetupProvider.loadApplicationProperties(new String[] {}, mockStreamingExecutionEnvironment);

            assertApplicationProperties(testParameters);
        }
    }

    @Test
    public void testLoadLocalApplicationParameters() {
        final Properties testParameters =
                FlinkSetupProvider.loadApplicationProperties(APP_PROPERTIES_ARGS,
                        LocalStreamEnvironment.getExecutionEnvironment());
        assertApplicationProperties(testParameters);
    }
}
