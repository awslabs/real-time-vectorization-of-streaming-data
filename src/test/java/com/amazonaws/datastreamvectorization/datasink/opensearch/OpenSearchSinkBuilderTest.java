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
package com.amazonaws.datastreamvectorization.datasink.opensearch;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchDataSinkConfiguration;
import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import org.apache.flink.connector.opensearch.sink.OpensearchSink;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_ENDPOINT;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_INDEX;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_OS_TYPE;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class OpenSearchSinkBuilderTest {

    @Test
    void testGetDataSink() {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_OS_ENDPOINT, "https://ad77zixjklwz3asd0dti.us-east-1.aoss.amazonaws.com");
        properties.setProperty(PROPERTY_OS_INDEX, "index");
        properties.setProperty(PROPERTY_OS_TYPE, OpenSearchType.SERVERLESS.name());
        OpenSearchDataSinkConfiguration testOSConfig = OpenSearchDataSinkConfiguration
                .parseFrom("us-east-1", properties).build();
        EmbeddingConfiguration testEmbedConfig = new EmbeddingConfiguration(
                EmbeddingModel.AMAZON_TITAN_TEXT_V2.getModelId(), Collections.emptyMap());
        OpensearchSink<JSONObject> dataSink = new OpenSearchSinkBuilder().getDataSink(testOSConfig, testEmbedConfig);
        assertNotNull(dataSink);
    }
}
