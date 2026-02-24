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
//! Materialized build artifact for hash-join probing.
//!
//! Responsibilities:
//! - Packages hash tables, row references, and build-side schema artifacts for probe operators.
//! - Separates build-time materialization from probe-time read access semantics.
//!
//! Key exported interfaces:
//! - Types: `JoinBuildArtifact`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use super::join_hash_table::JoinHashTable;
use crate::exec::chunk::Chunk;
use crate::exec::runtime_filter::LocalRuntimeFilterSet;

#[derive(Clone)]
/// Materialized build-side artifact consumed by join probe operators.
pub(crate) struct JoinBuildArtifact {
    pub(crate) build_batches: Arc<Vec<Chunk>>,
    pub(crate) build_table: Option<Arc<JoinHashTable>>,
    pub(crate) build_row_count: usize,
    pub(crate) build_has_null_key: bool,
    pub(crate) build_null_key_rows: Option<Arc<Vec<Vec<u32>>>>,
    pub(crate) runtime_filters: Option<Arc<LocalRuntimeFilterSet>>,
}

impl JoinBuildArtifact {
    pub(crate) fn new(
        build_batches: Vec<Chunk>,
        build_table: Option<JoinHashTable>,
        build_row_count: usize,
        build_has_null_key: bool,
        build_null_key_rows: Option<Arc<Vec<Vec<u32>>>>,
        runtime_filters: Option<Arc<LocalRuntimeFilterSet>>,
    ) -> Self {
        Self {
            build_batches: Arc::new(build_batches),
            build_table: build_table.map(Arc::new),
            build_row_count,
            build_has_null_key,
            build_null_key_rows,
            runtime_filters,
        }
    }
}
