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
package com.amazonaws.datastreamvectorization;

import com.amazonaws.datastreamvectorization.datasink.model.DataSinkType;
import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.datasource.model.DataSourceType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import com.amazonaws.datastreamvectorization.wrappers.FlinkSetupProvider;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_BOOTSTRAP_SERVERS;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_MODEL_ID;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_ENDPOINT;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_INDEX;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_SOURCE_TYPE;
import static com.amazonaws.datastreamvectorization.datasink.model.DataSinkConfiguration.PROPERTY_SINK_TYPE;
import static com.amazonaws.datastreamvectorization.wrappers.FlinkSetupProvider.APPLICATION_PROPERTIES_GROUP_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
public class DataStreamVectorizationJobTest {
    private static final String TEST_SOURCE_TYPE = DataSourceType.MSK.name();
    private static final String TEST_SOURCE_BOOTSTRAP_SERVERS = "localhost:9098";
    private static final String TEST_EMBED_MODEL_ID = EmbeddingModel.AMAZON_TITAN_TEXT_G1.getModelId();
    private static final String TEST_SINK_TYPE = DataSinkType.OPENSEARCH.name();
    private static final String TEST_OS_TYPE = OpenSearchType.SERVERLESS.name();
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


    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private StreamExecutionEnvironment mockStreamingExecutionEnvironment;
    private StreamExecutionEnvironment spyExecutionEnvironment;

    @Before
    public void setUp() throws Exception {
        spyExecutionEnvironment = Mockito.spy(LocalStreamEnvironment.getExecutionEnvironment());
        doReturn(null).when(spyExecutionEnvironment).execute(anyString());
    }

    @Test
    public void testUnsupportedFlinkArgsThrowException() {
        Assert.assertThrows(IllegalArgumentException.class, () ->
                DataStreamVectorizationJob.main(new String[] {"1, 2, 3, 4"}));
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

    @Test
    public void testGetExecutionEnvironment() {
        Assert.assertTrue(DataStreamVectorizationJob.getEnvironment() instanceof LocalStreamEnvironment);
    }

    @Test
    public void testMissingParametersThrowsConfigurationException() {
        String[] bad_args = new String[]{"--bad.arg.1", "arg1", "--bad.arg.2", "arg2", "--bad.arg.3", "arg3"};
        Assert.assertThrows(MissingOrIncorrectConfigurationException.class, () ->
                DataStreamVectorizationJob.main(bad_args));
    }
}
