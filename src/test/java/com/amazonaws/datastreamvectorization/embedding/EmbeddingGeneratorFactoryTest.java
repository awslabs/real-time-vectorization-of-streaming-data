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
import org.json.JSONObject;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@ExtendWith(MockitoExtension.class)
class EmbeddingGeneratorFactoryTest {

    @Mock
    EmbeddingConfiguration mockEmbeddingConfig;

    EmbeddingGeneratorFactory sut = new EmbeddingGeneratorFactory("us-east-1");

    private static Stream<Arguments> provideParameters() {
        return Stream.of(
                Arguments.of(String.class, BedrockEmbeddingGeneratorStringInputImpl.class),
                Arguments.of(JSONObject.class, BedrockEmbeddingGeneratorStringInputImpl.class)
        );
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    public void testExpectedInputType(Class<Object> inputTypeClass, Class<Object> expectedOutputTypeClass) {
        EmbeddingGenerator output = sut.getEmbeddingGenerator(inputTypeClass, mockEmbeddingConfig);
        assertInstanceOf(expectedOutputTypeClass, output);
    }
}
