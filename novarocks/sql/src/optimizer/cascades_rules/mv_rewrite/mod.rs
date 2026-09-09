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

//! Transparent MV query rewrite (single-table SPJG + aggregate rollup).
//!
//! Design spec: docs/design/specs/2026-06-10-mv-query-rewrite-design.md
//! StarRocks counterparts: MaterializedViewRewriter / AggregatedMaterializedViewRewriter.

pub(crate) mod aggregate_rollup;
pub(crate) mod column_mapping;
pub(crate) mod descriptor;
pub(crate) mod predicate_split;
pub(crate) mod rule;

use crate::compiler::SqlMvRewriteSelectionFacts;
use crate::optimizer::scalar::ScalarArena;
use crate::optimizer::stats_input::StatsRef;
use crate::planner::table::TableDef;
use descriptor::SpjgDescriptor;

pub(crate) const RULE_NAME: &str = "MvRewrite";

/// One usable MV candidate, fully prepared by the engine layer
/// (`src/mv/rewrite_prep.rs`). Everything the optimizer rule needs;
/// no engine/catalog handles cross this boundary.
#[derive(Clone, Debug)]
pub(crate) struct MvRewriteCandidate {
    /// MV name, for logging and the EXPLAIN annotation.
    pub mv_name: String,
    /// SPJG decomposition of the MV defining query, expressed over the
    /// base table's ColumnIds (allocated in the shared ColumnRefFactory).
    pub mv: SpjgDescriptor,
    /// Scalar arena that owns every ScalarId stored in `mv`.
    pub mv_scalars: ScalarArena,
    /// Database (namespace) of the MV target table, for ScanOp.
    pub target_database: String,
    /// SQL table definition of the MV target table, resolved through its
    /// request-local current-snapshot binding.
    pub target_table: TableDef,
    /// Query-scoped statistics ref for the MV target scan injected by the rule.
    pub target_stats_ref: StatsRef,
    pub selection: Option<SqlMvRewriteSelectionFacts>,
}
