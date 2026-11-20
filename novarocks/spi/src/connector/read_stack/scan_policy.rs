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

use std::num::NonZeroU32;

/// Output granularity for producers of complete rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanOutputPolicy {
    /// Maximum number of rows in an output, including a zero-column output.
    pub max_rows: NonZeroU32,
    /// Target for newly constructed column storage. A single representable
    /// row may exceed this target to let the producer make progress.
    pub soft_target_new_column_bytes: u64,
}

/// Engine-owned output granularity, following Trino's PageProcessor row
/// limit and PageBuilderStatus page size. Readers own their decode sizing.
// Design: ADR-0245 (docs/adr/ADR-0245-parquet-reader-sizing-and-scan-output-policy.md)
pub const ENGINE_OUTPUT_POLICY: ScanOutputPolicy = ScanOutputPolicy {
    max_rows: NonZeroU32::new(8192).expect("output row limit must be positive"),
    soft_target_new_column_bytes: 1024 * 1024,
};
