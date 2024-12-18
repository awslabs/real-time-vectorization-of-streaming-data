package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.integrationtests.model.IntegTestCaseInput;
import com.amazonaws.datastreamvectorization.integrationtests.model.IntegTestInputs;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.PropertyNamingStrategy;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static com.amazonaws.datastreamvectorization.integrationtests.constants.IntegTestConstants.INTEG_TEST_INPUTS_FILE;

public class BlueprintIT {
    @Test
    void runTests() {
        // TODO: handle test parallelization later
        IntegTestCaseInput[] testInputs = this.readTestConfigFile().getTestCases();
        for (IntegTestCaseInput testCase : testInputs) {
            IntegTestBase integrationTest = new IntegTestBase();
            integrationTest.runTestCase(testCase);
        }
    }

    /**
     * Reads in the local test config file with test case inputs
     *
     * @return IntegTestInputs
     */
    private IntegTestInputs readTestConfigFile() {
        // read in the local test inputs file
        String testInputFile = System.getProperty(INTEG_TEST_INPUTS_FILE);
        JSONObject testInputJson;
        try {
            InputStream is = new FileInputStream(testInputFile);
            String jsonTxt = IOUtils.toString(is, StandardCharsets.UTF_8);
            testInputJson = new JSONObject(jsonTxt);
        } catch (IOException e) {
            throw new RuntimeException("Could not read test input file " + testInputFile, e);
        }
        // read test inputs JSON into IntegTestInputs class object
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.UPPER_CAMEL_CASE);
            return objectMapper.readValue(testInputJson.toString(), IntegTestInputs.class);
        } catch (Exception e) {
            throw new RuntimeException("Error reading test input JSON into IntegTestInputs class: ", e);
        }
    }
}
