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
package com.amazonaws.datastreamvectorization.embedding;

import com.amazonaws.datastreamvectorization.embedding.generator.BedrockEmbeddingGeneratorStringInputImpl;
import com.amazonaws.datastreamvectorization.embedding.generator.EmbeddingGenerator;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import com.amazonaws.datastreamvectorization.exceptions.UnsupportedInputStreamDataTypeException;
import lombok.NonNull;
import org.json.JSONObject;

/**
 * Factory class to provide embedding generator for a given input data type.
 */
public class EmbeddingGeneratorFactory {

    private final String region;

    public EmbeddingGeneratorFactory(@NonNull final String region) {
        this.region = region;
    }

    /**
     * Method to get embedding generator based on input data type.
     * In the future, we might want to add another parameter for determining whether we need to call Bedrock or
     * some other embedding generator.
     * @param inputDataType class of input data type
     * @param embeddingConfiguration corresponding embedding generator.
     * @return
     */
    public EmbeddingGenerator getEmbeddingGenerator(@NonNull final Class<?> inputDataType,
                                                    @NonNull final EmbeddingConfiguration embeddingConfiguration) {
        if (String.class == inputDataType || JSONObject.class == inputDataType) {
            return new BedrockEmbeddingGeneratorStringInputImpl(region, embeddingConfiguration);
        }

        // TODO: add embedding generator for JSON input data type
        throw new UnsupportedInputStreamDataTypeException("Only String or JSON input data types are supported.");
    }
}
