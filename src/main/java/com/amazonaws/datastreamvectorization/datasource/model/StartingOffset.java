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
package com.amazonaws.datastreamvectorization.datasource.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Enum to define starting offsets for {@link org.apache.flink.connector.kafka.source.KafkaSource}
 * {@link OffsetsInitializer}
 */
@Getter
@AllArgsConstructor
public enum StartingOffset {
    /*
     * Map of default configurations associated with the model.
     */
    COMMITTED(OffsetsInitializer.committedOffsets()),
    EARLIEST(OffsetsInitializer.earliest()),
    LATEST(OffsetsInitializer.latest());

    private final OffsetsInitializer offsetsInitializer;
}