package com.amazonaws.datastreamvectorization.integrationtests.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * Model for expected record structure from OpenSearch vector index
 */
@AllArgsConstructor
@Getter
@Setter
public class OpenSearchIndexDocument {
    private String original_data;
    private String embedded_data;
    private String chunk_data;
    private String chunk_key;

    @Override
    public String toString() {
        return String.format("OpenSearchIndexDocument{original_data='%s', embedded_data='%s', " +
                        "chunk_data='%s', chunk_key='%s'}", original_data, embedded_data, chunk_data, chunk_key);
    }
}
