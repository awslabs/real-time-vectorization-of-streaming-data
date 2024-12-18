package com.amazonaws.datastreamvectorization.integrationtests.model;

import com.amazonaws.datastreamvectorization.datasink.model.OpenSearchType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class OpenSearchClusterConfig {
    private String Name;
    private String Type;
    private String EndpointUrl;

    public OpenSearchType getOpenSearchClusterType() {
        try {
            return OpenSearchType.valueOf(Type.toUpperCase().trim());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Unsupported OpenSearch cluster type " + Type);
        }
    }
}
