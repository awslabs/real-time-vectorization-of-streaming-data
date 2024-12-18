package com.amazonaws.datastreamvectorization.integrationtests.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class ITCaseInput {
    private String TestName;
    private MskClusterConfig MskCluster;
    private OpenSearchClusterConfig OpenSearchCluster;
}
