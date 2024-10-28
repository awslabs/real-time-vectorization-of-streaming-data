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
import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import dev.langchain4j.data.document.splitter.HierarchicalDocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentByCharacterSplitter;
import dev.langchain4j.data.document.splitter.DocumentByWordSplitter;
import dev.langchain4j.data.document.splitter.DocumentBySentenceSplitter;
import dev.langchain4j.data.document.splitter.DocumentByLineSplitter;
import dev.langchain4j.data.document.splitter.DocumentByParagraphSplitter;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.segment.TextSegment;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.getChunkingMaxSegmentSize;
import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.getChunkingMaxOverlapSize;
import static com.amazonaws.datastreamvectorization.utils.PropertiesUtils.getChunkingType;

/**
 * Map function for chunking a string into EmbeddingInput objects.
 */
@Slf4j
public class ChunkingFlatMapFunction implements FlatMapFunction<ChunkingInput, EmbeddingInput> {

    private final ChunkingType chunkingType;
    private final int maxSegmentSizeInChars;
    private final int maxOverlapSizeInChars;

    public ChunkingFlatMapFunction(EmbeddingConfiguration embeddingConfig) {
        this.chunkingType = getChunkingType(embeddingConfig);
        this.maxSegmentSizeInChars = getChunkingMaxSegmentSize(embeddingConfig);
        this.maxOverlapSizeInChars = getChunkingMaxOverlapSize(embeddingConfig);
    }

    @Override
    public void flatMap(final ChunkingInput chunkingInput, final Collector<EmbeddingInput> collector) {
        if (chunkingInput.isEmpty()) {
            // don't add to collector if chunking input is empty
            return;
        }

        if (chunkingInput.getStringToChunk().length() <= maxSegmentSizeInChars) {
            // don't chunk
            log.info("Input length within {} characters, not chunking: {}", maxSegmentSizeInChars,
                    chunkingInput.getStringToChunk());
            collector.collect(new EmbeddingInput(chunkingInput.getOriginalData(), chunkingInput.getChunkKey(),
                    chunkingInput.getDataToChunk()));
            return;
        }

        log.info("Chunking input with strategy {}", chunkingType);
        log.debug("Chunking input: {}", chunkingInput.getStringToChunk());
        List<TextSegment> splitText = getSplitter().split(Document.from(chunkingInput.getStringToChunk()));

        splitText.stream()
                .map(textSegment -> new EmbeddingInput(chunkingInput.getOriginalData(), chunkingInput.getChunkKey(),
                        textSegment.text()))
                .forEach(collector::collect);
    }

    private HierarchicalDocumentSplitter getSplitter() {
        switch (chunkingType) {
            case SPLIT_BY_CHARACTER:
                return new DocumentByCharacterSplitter(maxSegmentSizeInChars, maxOverlapSizeInChars);
            case SPLIT_BY_WORD:
                return new DocumentByWordSplitter(maxSegmentSizeInChars, maxOverlapSizeInChars);
            case SPLIT_BY_SENTENCE:
                return new DocumentBySentenceSplitter(maxSegmentSizeInChars, maxOverlapSizeInChars);
            case SPLIT_BY_LINE:
                return new DocumentByLineSplitter(maxSegmentSizeInChars, maxOverlapSizeInChars);
            case SPLIT_BY_PARAGRAPH:
                return new DocumentByParagraphSplitter(maxSegmentSizeInChars, maxOverlapSizeInChars);
            default:
                throw new MissingOrIncorrectConfigurationException("Unsupported chunking type: "
                        + chunkingType);
        }
    }
}