package com.amazonaws.datastreamvectorization.integrationtests.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Model containing MSK cluster information needed for an integration test case
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class MskClusterConfig {
    private String Arn;
}
