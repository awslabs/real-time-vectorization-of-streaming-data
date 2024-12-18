package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.integrationtests.model.ITCaseInput;
import com.amazonaws.datastreamvectorization.integrationtests.model.ITTestInputs;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static com.amazonaws.datastreamvectorization.integrationtests.constants.ITConstants.INTEG_TEST_INPUTS_FILE;

public class BlueprintIT {
    @Test
    void runTests() {
        // TODO: handle test parallelization later
        ITCaseInput[] testInputs = this.readTestConfigFile().getTestCases();
        for (ITCaseInput testCase : testInputs) {
            ITBase integrationTest = new ITBase();
            integrationTest.runTestCase(testCase);
        }
    }

    private ITTestInputs readTestConfigFile() {
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
        // read test inputs JSON into ITTestInputs class object
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(testInputJson.toString(), ITTestInputs.class);
        } catch (Exception e) {
            throw new RuntimeException("Error reading test input JSON into ITTestInputs class: ", e);
        }
    }
}
