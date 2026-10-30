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

pub(crate) mod broadcast_join_shared;
mod build_artifact;
mod build_capture;
mod build_collection;
mod build_csr;
mod build_finalize;
mod build_match_bitmap;
mod build_pages;
mod build_projection;
mod build_requirements;
pub(crate) mod build_state;
pub(crate) mod compiled_hash_join;
mod csr_slots;
mod join_completion;
mod join_hash_map;
mod join_hash_table;
mod key_build_worker;
pub(crate) mod native_runtime_filter;
mod tail_ranges;

pub(crate) use join_hash_map::gather::{
    JoinGatherError, TypedJoinBatch, filter_probe_typed, gather_join_batch_typed,
    gather_left_with_null_right_typed,
};
#[cfg(test)]
pub(crate) use join_hash_map::gather::{
    MAX_JOIN_OUTPUT_ROWS_PER_BATCH, gather_probe_build_batches_typed,
};

pub(crate) use join_completion::JoinParticipants;
