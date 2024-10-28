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

package com.amazonaws.datastreamvectorization.embedding.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;


/**
 * EmbeddingInput which holds the original string the data to embed is from and the chunk of the original
 * string to embed. Chunk data is intended to be empty if no chunking was applied, so the original data is
 * the string to embed instead. Chunk key is populated with the JSON key that the chunk was from if the
 * original data is a JSON string.
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class EmbeddingInput {
    private final String originalData;
    private final String chunkKey;
    private final String chunkData;

    public boolean isEmpty() {
        return StringUtils.isEmpty(originalData) && StringUtils.isEmpty(chunkData);
    }

    public String getStringToEmbed() {
        if (!StringUtils.isEmpty(chunkData)) {
            return chunkData;
        }
        return originalData;
    }

    // Needed for testing to print out readable object contents if assertions fail
    @Override
    public String toString() {
        return "{originalData: " + originalData + ", chunkKey: " + chunkKey + ", chunkData: " + chunkData + "}";
    }
}
