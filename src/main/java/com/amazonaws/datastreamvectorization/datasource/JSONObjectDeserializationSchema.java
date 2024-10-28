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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

import static org.apache.commons.lang3.ObjectUtils.isEmpty;

/**
 * {@link JSONObject} deserialization schema.
 */
@Slf4j
public class JSONObjectDeserializationSchema implements DeserializationSchema<JSONObject> {

    @Override
    public void open(InitializationContext context) {
    }

    /**
     * Deserializes the byte record as {@link JSONObject}.
     *
     * @param recordValue The record, as a byte array.
     * @return The deserialized recordValue as an {@link JSONObject} object (null if the record cannot be deserialized).
     */
    @Override
    public JSONObject deserialize(byte[] recordValue) {
        log.info("Deserializing record: {}", recordValue);
        if (isEmpty(recordValue)) {
            log.warn("Empty record received; nothing to deserialize.");
            return new JSONObject();
        }
        String jsonString = new String(recordValue, StandardCharsets.UTF_8);
        log.info("Deserialized String: {}", jsonString);
        return new JSONObject(jsonString);
    }

    /**
     * Method to decide whether the {@link JSONObject} element signals the end of the stream. If
     * true is returned the element won't be emitted.
     *
     * @param nextElement The {@link JSONObject} object to test for the end-of-stream signal.
     * @return false, to signal continuous stream.
     */
    @Override
    public boolean isEndOfStream(final JSONObject nextElement) {
        return false;
    }

    /**
     * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
     *
     * @return TypeInformation of {@link JSONObject}.
     */
    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }
}
