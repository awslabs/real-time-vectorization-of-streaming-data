package com.amazonaws.datastreamvectorization.integrationtests.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Config for a single integration test case
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class IntegTestCaseInput {
    private String TestName;
    private MskClusterConfig MskCluster;
    private OpenSearchClusterConfig OpenSearchCluster;
}
