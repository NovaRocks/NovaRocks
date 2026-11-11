// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::commit::preparation::{CapturedMergePolicy, ManifestMergePolicy};
use crate::iceberg::spec::TableMetadata;

const ENABLED: &str = "commit.manifest-merge.enabled";
const MIN_COUNT: &str = "commit.manifest.min-count-to-merge";
const TARGET_SIZE: &str = "commit.manifest.target-size-bytes";

impl ManifestMergePolicy {
    pub(crate) fn capture(metadata: &TableMetadata) -> CapturedMergePolicy {
        Self::from_properties(metadata.properties())
    }

    pub(super) fn from_properties(
        properties: &std::collections::HashMap<String, String>,
    ) -> CapturedMergePolicy {
        let enabled = properties
            .get(ENABLED)
            .is_none_or(|value| value.eq_ignore_ascii_case("true"));
        let min_count = properties
            .get(MIN_COUNT)
            .map(|value| {
                value.parse::<i32>()
                    .map_err(|_| "Iceberg property commit.manifest.min-count-to-merge must be a 32-bit integer")
            })
            .transpose()?
            .unwrap_or(100);
        let target_bytes = properties
            .get(TARGET_SIZE)
            .map(|value| {
                value.parse::<i64>()
                    .map_err(|_| "Iceberg property commit.manifest.target-size-bytes must be a 64-bit integer")
            })
            .transpose()?
            .unwrap_or(8 * 1024 * 1024);

        // Java's signed properties allow negative layout parameters. No positive
        // bin is below a nonpositive count or fits a nonpositive byte target.
        Ok(Self {
            enabled,
            min_count: min_count.max(0) as usize,
            target_bytes: target_bytes.max(0) as u64,
        })
    }
}
