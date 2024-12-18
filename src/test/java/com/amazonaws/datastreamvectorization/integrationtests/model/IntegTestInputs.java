package com.amazonaws.datastreamvectorization.integrationtests.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Config for a list of integration test cases
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class IntegTestInputs {
    private IntegTestCaseInput[] TestCases;
}
