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
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Map function for creating a ChunkingInput out of a string.
 */
public class StringChunkingInputMapFunction implements MapFunction<String, ChunkingInput> {
    @Override
    public ChunkingInput map(String inputString) {
        return new ChunkingInput(inputString, StringUtils.EMPTY, StringUtils.EMPTY);
    }
}
