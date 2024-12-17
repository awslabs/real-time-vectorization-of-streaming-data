package com.amazonaws.datastreamvectorization.integrationtests;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Integration Test Base class that contains steps for running one integration test case.
 */
@Slf4j
public class ITBase {

    public void runTest(List<String> recordsToProduce) {
        readTestConfigFile();
    }

    // TODO: return new "TestInputs" class object
    private void readTestConfigFile() {

    }

    private String generateTestID(String testName) {
        String currentTimestamp = Long.toString(System.currentTimeMillis());
        return String.join("-", testName, currentTimestamp);
    }


    /**
     * Validates that the original data fields from OpenSearch records queried match the expected
     * original data that was produced to the MSK cluster.
     * @param expectedOriginalDataList
     * @param searchResult
     */
    private void validateOpenSearchRecords(List<String> expectedOriginalDataList, List<OpenSearchIndexDocument> searchResult) {
        if (searchResult.size() != expectedOriginalDataList.size()) {
            log.info("Did not find expected number of records in OpenSearch search result. " +
                    "Expected {} records but got {}", expectedOriginalDataList.size(), searchResult.size());
        }
        List<String> resultOriginalDataList = searchResult
                .stream()
                .map(OpenSearchIndexDocument::getOriginal_data)
                .sorted().
                collect(Collectors.toList());

        Collections.sort(expectedOriginalDataList);
        Assertions.assertEquals(expectedOriginalDataList, resultOriginalDataList);
    }
}
