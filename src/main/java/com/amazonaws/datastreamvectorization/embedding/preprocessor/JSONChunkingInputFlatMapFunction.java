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

package com.amazonaws.datastreamvectorization.embedding.preprocessor;

import com.amazonaws.datastreamvectorization.embedding.model.ChunkingInput;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.getChunkingMaxSegmentSize;

/**
 * Flatmap function for splitting a JSON object into ChunkingInput objects.
 */
@Slf4j
public class JSONChunkingInputFlatMapFunction implements FlatMapFunction<JSONObject, ChunkingInput> {

    private final int maxSegmentSizeInChars;

    public JSONChunkingInputFlatMapFunction(EmbeddingConfiguration embeddingConfig) {
        this.maxSegmentSizeInChars = getChunkingMaxSegmentSize(embeddingConfig);
    }

    @Override
    public void flatMap(final JSONObject inputJson, final Collector<ChunkingInput> collector) {
        if (inputJson == JSONObject.NULL || inputJson.isEmpty()) {
            // don't add to collector if JSON is empty
            return;
        }

        String originalJsonString = inputJson.toString();
        if (originalJsonString.length() <= maxSegmentSizeInChars) {
            collector.collect(new ChunkingInput(originalJsonString, StringUtils.EMPTY, StringUtils.EMPTY));
            return;
        }

        collectFlattenedJson(originalJsonString, StringUtils.EMPTY, inputJson, collector);
    }

    private void collectFlattenedJson(String originalJsonString, String parentKey, JSONObject jsonObject,
                                      Collector<ChunkingInput> collector) {
        if (jsonObject == JSONObject.NULL || jsonObject.isEmpty()) {
            return;
        }
        jsonObject.keySet().forEach(key -> {
            Object jsonValue = jsonObject.get(key);
            String fullKey = StringUtils.isEmpty(parentKey) ? key : parentKey + "." + key;
            if (jsonValue instanceof JSONObject) {
                JSONObject jsonValueAsJsonObject = (JSONObject) jsonValue;
                String jsonValueString = jsonValueAsJsonObject.toString();
                if (jsonValueString.length() <= maxSegmentSizeInChars) {
                    collector.collect(new ChunkingInput(originalJsonString, fullKey, jsonValueString));
                } else {
                    collectFlattenedJson(originalJsonString, fullKey, jsonValueAsJsonObject, collector);
                }
            } else {
                collector.collect(new ChunkingInput(originalJsonString, fullKey, jsonValue.toString()));
            }
        });
    }
}