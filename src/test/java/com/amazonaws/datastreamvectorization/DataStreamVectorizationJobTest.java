/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
package com.amazonaws.datastreamvectorization;

import com.amazonaws.datastreamvectorization.exceptions.MissingOrIncorrectConfigurationException;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
public class DataStreamVectorizationJobTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Test
    public void testUnsupportedFlinkArgsThrowException() {
        Assert.assertThrows(IllegalArgumentException.class, () ->
                DataStreamVectorizationJob.main(new String[] {"1, 2, 3, 4"}));
    }

    @Test
    public void testGetExecutionEnvironment() {
        Assert.assertTrue(DataStreamVectorizationJob.getEnvironment() instanceof LocalStreamEnvironment);
    }

    @Test
    public void testMissingParametersThrowsConfigurationException() {
        String[] bad_args = new String[]{"--bad.arg.1", "arg1", "--bad.arg.2", "arg2", "--bad.arg.3", "arg3"};
        Assert.assertThrows(MissingOrIncorrectConfigurationException.class, () ->
                DataStreamVectorizationJob.main(bad_args));
    }
}
