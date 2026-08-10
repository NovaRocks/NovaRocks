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
//! Join-build sink state abstraction.
//!
//! Responsibilities:
//! - Defines the contract for collecting build-side rows and producing probe-ready artifacts.
//! - Allows broadcast and partitioned hash-join build paths to share sink orchestration logic.
//!
//! Key exported interfaces:
//! - Types: `JoinBuildSinkState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use super::build_artifact::JoinBuildArtifact;

/// Contract for join build-state implementations that ingest chunks and publish probe-ready artifacts.
pub(crate) trait JoinBuildSinkState: Send + Sync {
    fn partition_for_driver(&self, driver_id: i32) -> usize;
    fn dep_name(&self, partition: usize) -> &str;
    fn set_build(&self, partition: usize, artifact: Arc<JoinBuildArtifact>) -> Result<(), String>;
}
