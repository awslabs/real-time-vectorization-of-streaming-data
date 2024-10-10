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

import com.amazonaws.datastreamvectorization.datasink.DataSinkFactory;
import com.amazonaws.datastreamvectorization.datasink.model.DataSinkConfiguration;
import com.amazonaws.datastreamvectorization.datasource.DataSourceFactory;
import com.amazonaws.datastreamvectorization.datasource.model.DataSourceConfiguration;
import com.amazonaws.datastreamvectorization.datasource.model.StreamDataType;
import com.amazonaws.datastreamvectorization.embedding.EmbeddingGeneratorFactory;
import com.amazonaws.datastreamvectorization.embedding.generator.EmbeddingGenerator;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.json.JSONObject;

import java.util.Properties;

import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration.DEFAULT_EMBEDDING_ASYNC_MAX_IO;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration.DEFAULT_EMBEDDING_ASYNC_TIMEOUT;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration.DEFAULT_EMBEDDING_ASYNC_TIMEOUT_UNIT;
import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.getDataSinkConfiguration;
import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.getDataSourceConfiguration;
import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.getEmbeddingConfiguration;
import static com.amazonaws.datastreamvectorization.wrappers.FlinkSetupProvider.loadApplicationProperties;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;

/**
 * Main class for the Data Stream Vectorization Blueprint Flink Application
 */
@Slf4j
public class DataStreamVectorizationJob {

    @Getter
    private static StreamExecutionEnvironment environment;
    private static Properties applicationProperties;
    private static DataSourceConfiguration sourceConfiguration;
    private static EmbeddingConfiguration embeddingConfiguration;
    private static DataSinkConfiguration sinkConfiguration;

    /**
     * The main entry point for the application.
     *
     * @param args Command line arguments (received from the console)
     * @throws Exception if an exception occurs during execution.
     */
    public static void main(@NonNull final String[] args) throws Exception {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        applicationProperties = loadApplicationProperties(args, environment);
        log.info("Loaded application properties: {}", applicationProperties);

        // Get the AWS region
        AwsRegionProvider regionProvider = new DefaultAwsRegionProviderChain();
        String region = regionProvider.getRegion();

        //source
        sourceConfiguration = getDataSourceConfiguration(applicationProperties);
        log.info("Source Configuration: {}", sourceConfiguration);
        Source source = DataSourceFactory.getDataSource(sourceConfiguration);
        DataStreamSource sourceDataStream =
                environment.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");
        log.info("Source data stream added to environment.");

        DataStream filteredMessages;
        // Filter out empty strings in input stream
        if (StreamDataType.STRING.equals(sourceConfiguration.getStreamDataType())) {
            log.info("Filtering for empty string messages.");
            filteredMessages = sourceDataStream.filter(object -> {
                log.info("String is Empty: {}", ObjectUtils.isEmpty(object));
                if (ObjectUtils.isEmpty(object)) {
                    log.info("Filtered empty string message in stream.");
                }
                // filter function retains only those element for which the function returns true.
                return !ObjectUtils.isEmpty(object);

            }).uid("empty-string-message-filter");
        } else if (StreamDataType.JSON.equals(sourceConfiguration.getStreamDataType())) {
            log.info("Filtering for empty JSON messages.");
            filteredMessages = sourceDataStream.filter(jsonObject -> {
                boolean isEmpty = isEmpty(jsonObject) || ((JSONObject) jsonObject).isEmpty();
                log.info("JSON isEmpty: {}", isEmpty);
                if (isEmpty) {
                    log.info("Filtered empty json message in stream.");
                }
                // filter function retains only those element for which the function returns true.
                return !isEmpty;
            }).uid("empty-json-message-filter");
        } else {
            throw new MissingOrIncorrectConfigurationException("Unsupported data type for source stream. "
                    + sourceConfiguration.getStreamDataType());
        }

        // TODO: Chunking
        embeddingConfiguration = getEmbeddingConfiguration(applicationProperties);
        log.info("Embedding Configuration: {}", embeddingConfiguration);
        EmbeddingGenerator embeddingGenerator = new EmbeddingGeneratorFactory(region)
                .getEmbeddingGenerator(sourceConfiguration.getStreamDataType().getClazz(), embeddingConfiguration);
        DataStream<JSONObject> embeddingDataStream = AsyncDataStream.unorderedWait(
                filteredMessages,
                (RichAsyncFunction) embeddingGenerator,
                DEFAULT_EMBEDDING_ASYNC_TIMEOUT,
                DEFAULT_EMBEDDING_ASYNC_TIMEOUT_UNIT,
                DEFAULT_EMBEDDING_ASYNC_MAX_IO
        ).uid("custom-message-bedrock-async-function");
        log.info("Embedding function added.");

        // Filter out empty JSON objects from embeddings.
        DataStream<JSONObject> filteredEmbeddingResults = embeddingDataStream.filter(jsonObject -> {
            log.info("Should exclude JSON: {}", jsonObject.isEmpty());
            return !jsonObject.isEmpty();
        });
        log.info("Filtered embedding output stream.");

        //sink
        sinkConfiguration = getDataSinkConfiguration(region, applicationProperties);
        log.info("Sink Configuration: {}", sinkConfiguration);
        Sink sink = new DataSinkFactory().getDataSink(sinkConfiguration);
        filteredEmbeddingResults.sinkTo(sink);
        log.info("Sink added to embedded stream.");

        //process
        environment.execute("MSK Flink Bedrock Application");
    }
}