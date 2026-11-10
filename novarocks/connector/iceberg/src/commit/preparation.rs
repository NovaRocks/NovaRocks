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

/// The layout policy is captured once from the first preparation base.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ManifestMergePolicy {
    pub(crate) enabled: bool,
    pub(crate) min_count: usize,
    pub(crate) target_bytes: u64,
}

// Explicit-layout and metadata-only steps do not consume the ordinary layout.
pub(crate) type CapturedMergePolicy = std::result::Result<ManifestMergePolicy, &'static str>;

/// Per-operation preparation inputs. FileIO acquires the actual permits.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparationContext {
    pub(crate) merge_policy: CapturedMergePolicy,
    pub(crate) io_window_width: usize,
}
