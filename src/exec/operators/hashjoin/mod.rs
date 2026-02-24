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
//! Hash-join operator module exports.
//!
//! Responsibilities:
//! - Registers broadcast and partitioned hash-join operators and their shared state types.
//! - Exposes probe/build factories used by pipeline graph construction.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

mod broadcast_join_probe_processor;
pub(crate) mod broadcast_join_shared;
mod build_artifact;
pub(crate) mod build_state;
mod hash_join_build_sink;
mod hash_join_probe_core;
mod join_hash_table;
pub(crate) mod join_probe_utils;
mod partitioned_join_probe_processor;
pub(crate) mod partitioned_join_shared;

// Re-export all public types
pub use broadcast_join_probe_processor::BroadcastJoinProbeProcessorFactory;
pub use hash_join_build_sink::HashJoinBuildSinkFactory;
pub use partitioned_join_probe_processor::PartitionedJoinProbeProcessorFactory;
