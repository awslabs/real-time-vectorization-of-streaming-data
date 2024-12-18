package com.amazonaws.datastreamvectorization.integrationtests;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
public class OpenSearchClusterBlueprintParameters {
    String OpenSearchCollectionName;
    String OpenSearchEndpointURL;
    String OpenSearchType;
    String OpenSearchVectorIndexName;
}
