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
import com.amazonaws.datastreamvectorization.embedding.model.ChunkingType;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingInput;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_CHUNKING_TYPE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_CHUNKING_MAX_SIZE;
import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.PROPERTY_EMBEDDING_INPUT_CHUNKING_MAX_OVERLAP;

import static com.amazonaws.datastreamvectorization.embedding.model.ChunkingType.*;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration.DEFAULT_EMBEDDING_CHARSET;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel.AMAZON_TITAN_TEXT_G1;

public class ChunkingTest {
        private static Stream<Arguments> provideSuccessStringArguments() {
        String input1 = "A string to be chunked";
        String input2 = "Split this by word but split by character when encountering long word like abcdefghijklmnopqrstuvwxyz1234567890";
        String input3 = "Split by sentence. Short sent1? Short sent2! But split by word if the sentence is very long.";
        String input4 = "Multi-line string to be chunked.\n Chunking should occur at\n new lines if possible";
        String input5 = "Multi-paragraph\n string\n\n to be chunked.\n\n Chunking occurs at\n new paragraphs if possible, \n  else split at new line";

        return Stream.of(
                Arguments.of(input1,
                    List.of(
                        new EmbeddingInput(input1, StringUtils.EMPTY, "A string t"),
                        new EmbeddingInput(input1, StringUtils.EMPTY, "o be chunk"),
                        new EmbeddingInput(input1, StringUtils.EMPTY, "ed")
                    ),
                    SPLIT_BY_CHARACTER, 10, 0),
                Arguments.of(
                    input2,
                    List.of(
                        new EmbeddingInput(input2, StringUtils.EMPTY, "Split this by word but"),
                        new EmbeddingInput(input2, StringUtils.EMPTY, "split by character when"),
                        new EmbeddingInput(input2, StringUtils.EMPTY, "encountering long word"),
                        new EmbeddingInput(input2, StringUtils.EMPTY, "like"),
                        new EmbeddingInput(input2, StringUtils.EMPTY, "abcdefghijklmnopqrstuvwxy"),
                        new EmbeddingInput(input2, StringUtils.EMPTY, "z1234567890")
                    ),
                    SPLIT_BY_WORD, 25, 0),
                Arguments.of(
                    input3,
                    List.of(
                        new EmbeddingInput(input3, StringUtils.EMPTY, "Split by sentence. Short sent1?"),
                        new EmbeddingInput(input3, StringUtils.EMPTY,"Short sent2!"),
                        new EmbeddingInput(input3, StringUtils.EMPTY,"But split by word if the sentence"),
                        new EmbeddingInput(input3, StringUtils.EMPTY,"is very long.")
                    ),
                    SPLIT_BY_SENTENCE, 35, 0),
                Arguments.of(
                    input4,
                    List.of(
                        new EmbeddingInput(input4, StringUtils.EMPTY,"Multi-line string to be chunked."),
                        new EmbeddingInput(input4, StringUtils.EMPTY,"Chunking should occur at"),
                        new EmbeddingInput(input4, StringUtils.EMPTY,"new lines if possible")
                    ),
                    SPLIT_BY_LINE, 35, 0),
                Arguments.of(
                    input5,
                    List.of(
                        new EmbeddingInput(input5, StringUtils.EMPTY,"Multi-paragraph\n string"),
                        new EmbeddingInput(input5, StringUtils.EMPTY,"to be chunked."),
                        new EmbeddingInput(input5, StringUtils.EMPTY,"Chunking occurs at new"),
                        new EmbeddingInput(input5, StringUtils.EMPTY,"paragraphs if possible, else"),
                        new EmbeddingInput(input5, StringUtils.EMPTY,"split at new line")
                    ),
                    SPLIT_BY_PARAGRAPH, 30, 0)
        );
    }

    private static Stream<Arguments> provideSuccessJsonArguments() {
        JSONObject shortJsonInput = new JSONObject();
        shortJsonInput.put("title", "My Test JSON Object ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");
        shortJsonInput.put("metadata", "Contains  sentences with spaces! And contains \n random new line character within a sentence");
        shortJsonInput.put("version", 540);
        shortJsonInput.put("isValid", true);
        String shortJsonString = shortJsonInput.toString();

        JSONObject longJsonInput = new JSONObject();
        longJsonInput.put("title", "My Test JSON Object ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");
        longJsonInput.put("metadata", "This is some metadata for my JSON object: Contains  sentences with spaces! " +
                "And contains \n random new line character within a sentence");
        longJsonInput.put("description", "My JSON object description. " +
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n\n " +
                "Ut enim ad minim veniam. Duis aute irure dolor in reprehenderit in voluptate velit. \n" +
                "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");
        longJsonInput.put("version", 540);
        longJsonInput.put("isValid", true);
        String longJsonString = longJsonInput.toString();

        return Stream.of(
                Arguments.of(
                        shortJsonInput,
                        List.of(
                                new EmbeddingInput(shortJsonString, "metadata", "Contains  sentences with spaces! And contains \n ra"),
                                new EmbeddingInput(shortJsonString, "metadata", "ndom new line character within a sentence"),
                                new EmbeddingInput(shortJsonString, "isValid", "true"),
                                new EmbeddingInput(shortJsonString, "title", "My Test JSON Object ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"),
                                new EmbeddingInput(shortJsonString, "title", "567890"),
                                new EmbeddingInput(shortJsonString, "version", "540")
                        ),
                        SPLIT_BY_CHARACTER, 50, 0),
                Arguments.of(
                        shortJsonInput,
                        List.of(
                                new EmbeddingInput(shortJsonString, "metadata", "Contains sentences with"),
                                new EmbeddingInput(shortJsonString, "metadata", "spaces! And contains random"),
                                new EmbeddingInput(shortJsonString, "metadata", "new line character within a"),
                                new EmbeddingInput(shortJsonString, "metadata", "sentence"),
                                new EmbeddingInput(shortJsonString, "isValid", "true"),
                                new EmbeddingInput(shortJsonString, "title", "My Test JSON Object"),
                                new EmbeddingInput(shortJsonString, "title", "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"),
                                new EmbeddingInput(shortJsonString, "title", "567890"),
                                new EmbeddingInput(shortJsonString, "version", "540")
                        ),
                        SPLIT_BY_WORD, 30, 0),
                Arguments.of(
                        longJsonInput,
                        List.of(
                                new EmbeddingInput(longJsonString, "metadata", "This is some metadata for my JSON object: Contains  sentences with spaces!"),
                                new EmbeddingInput(longJsonString, "metadata", "And contains \n random new line character within a sentence"),
                                new EmbeddingInput(longJsonString, "isValid", "true"),
                                new EmbeddingInput(longJsonString, "description", "My JSON object description."),
                                new EmbeddingInput(longJsonString, "description", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor"),
                                new EmbeddingInput(longJsonString, "description", "incididunt ut labore et dolore magna aliqua."),
                                new EmbeddingInput(longJsonString, "description", "Ut enim ad minim veniam."),
                                new EmbeddingInput(longJsonString, "description", "Duis aute irure dolor in reprehenderit in voluptate velit."),
                                new EmbeddingInput(longJsonString, "description", "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia"),
                                new EmbeddingInput(longJsonString, "description", "deserunt mollit anim id est laborum."),
                                new EmbeddingInput(longJsonString, "title", "My Test JSON Object ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"),
                                new EmbeddingInput(longJsonString, "version", "540")
                        ),
                        SPLIT_BY_SENTENCE, 80, 0),
                Arguments.of(
                        longJsonInput,
                        List.of(
                                new EmbeddingInput(longJsonString, "metadata", "This is some metadata for my JSON object: Contains  sentences with spaces! And contains"),
                                new EmbeddingInput(longJsonString, "metadata", "random new line character within a sentence"),
                                new EmbeddingInput(longJsonString, "isValid", "true"),
                                new EmbeddingInput(longJsonString, "description", "My JSON object description."),
                                new EmbeddingInput(longJsonString, "description", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore"),
                                new EmbeddingInput(longJsonString, "description", "et dolore magna aliqua."),
                                new EmbeddingInput(longJsonString, "description", "Ut enim ad minim veniam. Duis aute irure dolor in reprehenderit in voluptate velit."),
                                new EmbeddingInput(longJsonString, "description", "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id"),
                                new EmbeddingInput(longJsonString, "description", "est laborum."),
                                new EmbeddingInput(longJsonString, "title", "My Test JSON Object ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"),
                                new EmbeddingInput(longJsonString, "version", "540")
                        ),
                        SPLIT_BY_LINE, 100, 0),
                Arguments.of(
                        longJsonInput,
                        List.of(
                                new EmbeddingInput(longJsonString, "metadata", "This is some metadata for my JSON object: Contains  sentences with spaces!"),
                                new EmbeddingInput(longJsonString, "metadata", "And contains \n random new line character within a sentence"),
                                new EmbeddingInput(longJsonString, "isValid", "true"),
                                new EmbeddingInput(longJsonString, "description", "My JSON object description."),
                                new EmbeddingInput(longJsonString, "description", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."),
                                new EmbeddingInput(longJsonString, "description", "Ut enim ad minim veniam. Duis aute irure dolor in reprehenderit in voluptate velit."),
                                new EmbeddingInput(longJsonString, "description", "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."),
                                new EmbeddingInput(longJsonString, "title", "My Test JSON Object ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"),
                                new EmbeddingInput(longJsonString, "version", "540")
                        ),
                        SPLIT_BY_PARAGRAPH, 130, 0)
        );
    }

    private static Stream<Arguments> provideSuccessNestedJsonArguments() {
        // short nested Json to not be chunked
        JSONObject nestedShortJsonInput = new JSONObject();
        JSONObject innerAJsonObject = new JSONObject();
        innerAJsonObject.put("Aa", "A string in a nested JSON.");
        innerAJsonObject.put("Ab", "Another string in a nested JSON.");
        nestedShortJsonInput.put("A", innerAJsonObject);
        nestedShortJsonInput.put("B", "Some metadata");
        String nestedShortJsonString = nestedShortJsonInput.toString();

        // long nested Json that has an array in it too
        JSONObject nestedLongJsonInput = new JSONObject();

        JSONObject innerCcJsonObject = new JSONObject();
        innerCcJsonObject.put("Cc1", "Ut enim ad minim veniam. Duis aute irure dolor in reprehenderit in voluptate.");
        innerCcJsonObject.put("Cc2", "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");
        innerCcJsonObject.put("Cc3", new JSONObject("{\"content\":\"A double-nested JSON. This JSON needs to be chunked because it is long.\",\"metadata\":\"Some metadata\"}"));

        List<Object> innerCdJsonObjectList = List.of(
                new JSONObject("{\"name\": \"Element 1\", \"version\": 2}"),
                new JSONObject("{\"name\": \"Element 2\", \"version\": 1}"),
                new JSONObject("{\"name\": \"Element 3\", \"version\": 40}"),
                "A string element",
                123
        );

        JSONObject innerCJsonObject = new JSONObject();
        innerCJsonObject.put("Ca", "A string in a nested JSON.");
        innerCJsonObject.put("Cb", "Another string in a nested JSON. This string needs to be chunked because it is long.");
        innerCJsonObject.put("Cc", innerCcJsonObject);
        innerCJsonObject.put("Cd", innerCdJsonObjectList);

        JSONObject innerDJsonObject = new JSONObject();
        innerDJsonObject.put("Da", "Short object");
        innerDJsonObject.put("Db", 1);

        nestedLongJsonInput.put("C", innerCJsonObject);
        nestedLongJsonInput.put("D", innerDJsonObject);
        String nestedLongJsonString = nestedLongJsonInput.toString();

        return Stream.of(
                Arguments.of(
                        nestedShortJsonInput,
                        List.of(
                                new EmbeddingInput(nestedShortJsonString, StringUtils.EMPTY, StringUtils.EMPTY)
                        ),
                        SPLIT_BY_WORD, 100000, 0),
                Arguments.of(
                        nestedLongJsonInput,
                        List.of(
                                new EmbeddingInput(nestedLongJsonString, "C.Cc.Cc1", "Ut enim ad minim veniam. Duis aute irure dolor in reprehenderit in voluptate."),
                                new EmbeddingInput(nestedLongJsonString, "C.Cc.Cc3.metadata", "Some metadata"),
                                new EmbeddingInput(nestedLongJsonString, "C.Cc.Cc3.content", "A double-nested JSON. This JSON needs to be chunked because it is long."),
                                new EmbeddingInput(nestedLongJsonString, "C.Cc.Cc2", "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia"),
                                new EmbeddingInput(nestedLongJsonString, "C.Cc.Cc2", "deserunt mollit anim id est laborum."),
                                new EmbeddingInput(nestedLongJsonString, "C.Cd", "[{\"name\":\"Element 1\",\"version\":2},{\"name\":\"Element"),
                                new EmbeddingInput(nestedLongJsonString, "C.Cd", "2\",\"version\":1},{\"name\":\"Element 3\",\"version\":40},\"A string element\",123]"),
                                new EmbeddingInput(nestedLongJsonString, "C.Ca", "A string in a nested JSON."),
                                new EmbeddingInput(nestedLongJsonString, "C.Cb", "Another string in a nested JSON. This string needs to be chunked because it is"),
                                new EmbeddingInput(nestedLongJsonString, "C.Cb", "long."),
                                new EmbeddingInput(nestedLongJsonString, "D", "{\"Da\":\"Short object\",\"Db\":1}")
                        ),
                        SPLIT_BY_WORD, 80, 0)
        );
    }

    @ParameterizedTest
    @MethodSource("provideSuccessStringArguments")
    public void testSuccessForChunkingStrings(String stringInput, List<EmbeddingInput> expectedChunks,
                                              ChunkingType chunkingType, int maxSegmentSizeInChars,
                                              int maxOverlapSizeInChars) {
        List<EmbeddingInput> chunkingResult = getChunkingResultFromString(stringInput, chunkingType,
                maxSegmentSizeInChars, maxOverlapSizeInChars);
        Assertions.assertEquals(expectedChunks, chunkingResult);
    }

    @ParameterizedTest
    @MethodSource("provideSuccessJsonArguments")
    public void testSuccessForChunkingJson(JSONObject jsonInput, List<EmbeddingInput> expectedChunks,
                                              ChunkingType chunkingType, int maxSegmentSizeInChars,
                                              int maxOverlapSizeInChars) {
        List<EmbeddingInput> chunkingResult = getChunkingResultFromJson(jsonInput, chunkingType,
                maxSegmentSizeInChars, maxOverlapSizeInChars);
        Assertions.assertEquals(expectedChunks, chunkingResult);
    }

    @ParameterizedTest
    @MethodSource("provideSuccessNestedJsonArguments")
    public void testSuccessForChunkingNestedJson(JSONObject jsonInput, List<EmbeddingInput> expectedChunks,
                                           ChunkingType chunkingType, int maxSegmentSizeInChars,
                                           int maxOverlapSizeInChars) {
        List<EmbeddingInput> chunkingResult = getChunkingResultFromJson(jsonInput, chunkingType,
                maxSegmentSizeInChars, maxOverlapSizeInChars);
        Assertions.assertEquals(expectedChunks, chunkingResult);
    }

    @Test
    public void testSuccessForNoSubChunkingJSON() {
        JSONObject jsonInput = new JSONObject();
        jsonInput.put("title", "My title");
        jsonInput.put("metadata", "My metadata");
        String jsonString = jsonInput.toString();

        List<EmbeddingInput> expectedChunksForJson = List.of(
                new EmbeddingInput(jsonString, "metadata", "My metadata"),
                new EmbeddingInput(jsonString, "title", "My title")
        );
        List<EmbeddingInput> chunkingResultForJson = getChunkingResultFromJson(jsonInput, SPLIT_BY_CHARACTER, 12, 0);
        Assertions.assertEquals(expectedChunksForJson, chunkingResultForJson);
    }

    @Test
    public void testSuccessForNoChunkingNeeded() {
        // test string
        String stringInput = "A string that will not be chunked.";
        List<EmbeddingInput> expectedChunksForString = List.of(new EmbeddingInput(stringInput, StringUtils.EMPTY, StringUtils.EMPTY));
        List<EmbeddingInput> chunkingResultForString = getChunkingResultFromString(stringInput, SPLIT_BY_LINE,
                10000, 100);
        Assertions.assertEquals(expectedChunksForString, chunkingResultForString);

        // test JSON
        JSONObject jsonInput = new JSONObject();
        jsonInput.put("title", "My Test JSON Object ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");
        jsonInput.put("version", 540);
        jsonInput.put("isValid", true);
        String jsonString = jsonInput.toString();
        List<EmbeddingInput> expectedChunksForJson = List.of(new EmbeddingInput(jsonString, StringUtils.EMPTY, StringUtils.EMPTY));
        List<EmbeddingInput> chunkingResultForJson = getChunkingResultFromJson(jsonInput, SPLIT_BY_LINE,
                10000, 100);
        Assertions.assertEquals(expectedChunksForJson, chunkingResultForJson);
    }

    @Test
    public void testSuccessForEmptyInput() {
        // test string
        String stringInput = StringUtils.EMPTY;
        List<EmbeddingInput> chunkingResultForString = getChunkingResultFromString(stringInput, SPLIT_BY_WORD,
                10, 5);
        Assertions.assertTrue(chunkingResultForString.isEmpty());

        // test JSON
        JSONObject jsonInput = new JSONObject();
        List<EmbeddingInput> chunkingResultForJson = getChunkingResultFromJson(jsonInput, SPLIT_BY_CHARACTER,
                10, 5);
        Assertions.assertTrue(chunkingResultForJson.isEmpty());
    }

    @Test
    public void testSuccessForNonZeroOverlapSize() {
        // test string
        String stringInput = "Unit testing chunking function. Trying a non-zero overlap size.";
        List<EmbeddingInput> expectedChunksForString = List.of(
                new EmbeddingInput(stringInput, StringUtils.EMPTY, "Unit testing chunking function. Trying a non-zero"),
                new EmbeddingInput(stringInput, StringUtils.EMPTY, "Trying a non-zero overlap size.")
        );
        List<EmbeddingInput> chunkingResultForString = getChunkingResultFromString(stringInput, SPLIT_BY_WORD,
                50, 40);
        Assertions.assertEquals(expectedChunksForString, chunkingResultForString);

        // test JSON
        JSONObject jsonInput = new JSONObject();
        jsonInput.put("Key 1", "Chunking is needed. Trying a non-zero overlap size.");
        jsonInput.put("Key 2", "Another key that is not chunked.");
        String jsonString = jsonInput.toString();
        List<EmbeddingInput> expectedChunksForJson = List.of(
                new EmbeddingInput(jsonString, "Key 1", "Chunking is needed. Trying a non-zero"),
                new EmbeddingInput(jsonString, "Key 1", "Trying a non-zero overlap size."),
                new EmbeddingInput(jsonString, "Key 2", "Another key that is not chunked.")
        );
        List<EmbeddingInput> chunkingResultForJson = getChunkingResultFromJson(jsonInput, SPLIT_BY_WORD,
                40, 30);
        Assertions.assertEquals(expectedChunksForJson, chunkingResultForJson);
    }

    private List<EmbeddingInput> getChunkingResultFromJson(JSONObject input, ChunkingType chunkingType,
                                                         int maxSegmentSizeInChars, int maxOverlapSizeInChars) {
        EmbeddingConfiguration embeddingConfig = getEmbeddingConfig(chunkingType, maxSegmentSizeInChars,
                    maxOverlapSizeInChars);
        JSONChunkingInputFlatMapFunction jsonChunkingInputFlatMapFunction = new JSONChunkingInputFlatMapFunction(
                embeddingConfig);
        ChunkingFlatMapFunction chunkingFlatMapFunction = new ChunkingFlatMapFunction(embeddingConfig);

        List<ChunkingInput> chunkingInputResult = new ArrayList<>();
        ListCollector<ChunkingInput> chunkingInputCollector = new ListCollector<>(chunkingInputResult);
        List<EmbeddingInput> chunkingResult = new ArrayList<>();
        ListCollector<EmbeddingInput> chunkingCollector = new ListCollector<>(chunkingResult);

        jsonChunkingInputFlatMapFunction.flatMap(input, chunkingInputCollector);
        chunkingInputResult.forEach(
                chunkingInput -> chunkingFlatMapFunction.flatMap(chunkingInput, chunkingCollector)
        );
        return chunkingResult;
    }

    private List<EmbeddingInput> getChunkingResultFromString(String input, ChunkingType chunkingType,
                                                         int maxSegmentSizeInChars, int maxOverlapSizeInChars) {
        EmbeddingConfiguration embeddingConfig = getEmbeddingConfig(chunkingType, maxSegmentSizeInChars,
                maxOverlapSizeInChars);
        StringChunkingInputMapFunction stringChunkingInputFlatMapFunction = new StringChunkingInputMapFunction();
        ChunkingFlatMapFunction chunkingFlatMapFunction = new ChunkingFlatMapFunction(embeddingConfig);

        List<EmbeddingInput> chunkingResult = new ArrayList<>();
        ListCollector<EmbeddingInput> chunkingCollector = new ListCollector<>(chunkingResult);

        ChunkingInput chunkingInput = stringChunkingInputFlatMapFunction.map(input);
        chunkingFlatMapFunction.flatMap(chunkingInput, chunkingCollector);
        return chunkingResult;
    }

    private EmbeddingConfiguration getEmbeddingConfig(ChunkingType chunkingType, int maxSegmentSizeInChars,
                                                      int maxOverlapSizeInChars) {
        Map<String, Object> embeddingInputConfig = new HashMap<>();
        embeddingInputConfig.put(PROPERTY_EMBEDDING_INPUT_CHUNKING_TYPE, chunkingType);
        embeddingInputConfig.put(PROPERTY_EMBEDDING_INPUT_CHUNKING_MAX_SIZE, maxSegmentSizeInChars);
        embeddingInputConfig.put(PROPERTY_EMBEDDING_INPUT_CHUNKING_MAX_OVERLAP, maxOverlapSizeInChars);

        return new EmbeddingConfiguration(AMAZON_TITAN_TEXT_G1, new HashMap<>(), embeddingInputConfig, DEFAULT_EMBEDDING_CHARSET);
    }
}
