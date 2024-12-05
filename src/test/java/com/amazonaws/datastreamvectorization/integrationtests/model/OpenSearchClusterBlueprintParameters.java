package com.amazonaws.datastreamvectorization.integrationtests.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Model containing OpenSearch related blueprint stack parameter values
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
public class OpenSearchClusterBlueprintParameters {
    String OpenSearchCollectionName;
    String OpenSearchEndpointURL;
    String OpenSearchType;
    String OpenSearchVectorIndexName;
}
