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
package com.amazonaws.datastreamvectorization.datasource;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JSONObjectDeserializationSchemaTest {

    DeserializationSchema<JSONObject> schema;

    @BeforeEach
    public void setUp() {
       schema = new JSONObjectDeserializationSchema();
    }

    @Test
    public void testIntegrity() throws IOException {
        // create json element
        JSONObject expectedResponse = new JSONObject();
        expectedResponse.put("id", 1);
        expectedResponse.put("name", "testName");
        expectedResponse.put("version", 1.0);

        // deserialize json element
        JSONObject deserializedElement = schema.deserialize(expectedResponse.toString().getBytes());
        assertNotNull(deserializedElement);
        assertEquals(expectedResponse.toString(), deserializedElement.toString(),
                "Deserialized JSON string does not match");
    }

    @Test
    public void testFailureIntegrity_NullInput() throws IOException {
        // deserialize json element
        assertEquals("{}", schema.deserialize(null).toString());
    }

    @Test
    public void testFailureIntegrity_EmptyString() throws IOException {
        // deserialize json element
        assertEquals("{}", schema.deserialize(StringUtils.EMPTY.getBytes()).toString());
    }

    @Test
    public void testFailureIntegrity_EmptyJson() throws IOException {
        // deserialize json element
        assertEquals("{}", schema.deserialize("{}".getBytes()).toString());
    }

    @Test
    public void testProducedTypeIntegrity() {
        // Validate produced type
        assertEquals(schema.getProducedType(), TypeInformation.of(JSONObject.class), "Incorrect TypeInformation");
    }

    @Test
    public void testEndOfStreamIntegrity() {
        // validate reading continuous stream
        assertFalse(schema.isEndOfStream(new JSONObject()), "End of stream error.");
    }
}
