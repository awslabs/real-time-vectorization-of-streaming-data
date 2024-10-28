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

package com.amazonaws.datastreamvectorization.embedding.model;

public enum EmbeddingJsonStrictness {
    // check all embedding json configs, fail fast if any condition does not match
    FAIL_FAST,
    // check all embedding json configs, if any condition does not match, ignore the condition and
    // if none match, ignore the JSON message
    MISSING_FIELDS_IGNORE,
    // check all embedding json configs, if any condition does not match, ignore the condition and
    // if none match, embed the entire JSON
    MISSING_FIELDS_EMBED_ALL;
}
