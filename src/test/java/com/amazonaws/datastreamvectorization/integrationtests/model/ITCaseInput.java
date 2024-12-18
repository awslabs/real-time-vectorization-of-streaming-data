package com.amazonaws.datastreamvectorization.integrationtests.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class ITCaseInput {
    private String TestName;
    private MskClusterConfig MskCluster;
    private OpenSearchClusterConfig OpenSearchCluster;
}
