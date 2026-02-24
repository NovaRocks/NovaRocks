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
//! Local runtime-filter registry.
//!
//! Responsibilities:
//! - Maintains runtime filters available inside one fragment/executor boundary.
//! - Tracks filter availability and probe eligibility metadata per expression.
//!
//! Key exported interfaces:
//! - Types: `LocalRuntimeFilterSet`, `RuntimeFilterMembership`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::filter_record_batch;
use hashbrown::HashSet;

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::hash_table::key_builder::{
    GroupKeyArrayView, build_group_key_hashes, build_group_key_views,
};
use crate::exec::node::join::JoinRuntimeFilterSpec;

use super::apply;

#[derive(Clone, Debug)]
/// Local registry containing runtime membership filters keyed by expression id.
pub(crate) struct LocalRuntimeFilterSet {
    hash_seed: u64,
    filters: Vec<LocalRuntimeFilter>,
}

#[derive(Clone, Debug)]
/// Probe-side runtime filter membership metadata for one expression.
pub(crate) struct RuntimeFilterMembership {
    hash_seed: u64,
    hashes: HashSet<u64>,
}

#[derive(Clone, Debug)]
struct LocalRuntimeFilter {
    filter_id: i32,
    expr_order: usize,
    hashes: HashSet<u64>,
}

pub(in crate::exec::runtime_filter) fn row_has_null(
    views: &[GroupKeyArrayView<'_>],
    row: usize,
) -> bool {
    for view in views {
        let is_null = match view {
            GroupKeyArrayView::Int(view) => view.value_at(row).is_none(),
            GroupKeyArrayView::Float(view) => view.value_at(row).is_none(),
            GroupKeyArrayView::Boolean(arr) => arr.is_null(row),
            GroupKeyArrayView::Utf8(arr) => arr.is_null(row),
            GroupKeyArrayView::Date32(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampSecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampMillisecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampMicrosecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampNanosecond(arr) => arr.is_null(row),
            GroupKeyArrayView::Decimal128(arr) => arr.is_null(row),
            GroupKeyArrayView::Decimal256(arr) => arr.is_null(row),
            GroupKeyArrayView::LargeIntBinary(arr) => arr.is_null(row),
            GroupKeyArrayView::ListUtf8 { list, .. } => list.is_null(row),
            GroupKeyArrayView::ListInt32 { list, .. } => list.is_null(row),
            GroupKeyArrayView::Complex(arr) => arr.is_null(row),
        };
        if is_null {
            return true;
        }
    }
    false
}

impl LocalRuntimeFilterSet {
    pub(crate) fn new(specs: &[JoinRuntimeFilterSpec], hash_seed: u64) -> Self {
        let mut filters = Vec::with_capacity(specs.len());
        for spec in specs {
            filters.push(LocalRuntimeFilter {
                filter_id: spec.filter_id,
                expr_order: spec.expr_order,
                hashes: HashSet::new(),
            });
        }
        Self { hash_seed, filters }
    }

    pub(crate) fn add_build_arrays(&mut self, key_arrays: &[ArrayRef]) -> Result<(), String> {
        if self.filters.is_empty() {
            return Ok(());
        }
        for filter in &mut self.filters {
            let Some(array) = key_arrays.get(filter.expr_order) else {
                return Err(format!(
                    "runtime filter {} expects build key index {} but only {} keys are available",
                    filter.filter_id,
                    filter.expr_order,
                    key_arrays.len()
                ));
            };
            let arrays = [Arc::clone(array)];
            let views = build_group_key_views(&arrays)?;
            let hashes = build_group_key_hashes(&views, array.len(), self.hash_seed)?;
            for row in 0..array.len() {
                if row_has_null(&views, row) {
                    continue;
                }
                filter.hashes.insert(hashes[row]);
            }
        }
        Ok(())
    }

    pub(crate) fn filter_probe_chunk(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        chunk: Chunk,
    ) -> Result<Option<Chunk>, String> {
        if self.filters.is_empty() || chunk.is_empty() {
            return Ok(Some(chunk));
        }
        if probe_keys.is_empty() {
            return Ok(Some(chunk));
        }
        let mut key_arrays = Vec::with_capacity(probe_keys.len());
        for expr in probe_keys {
            let array = arena.eval(*expr, &chunk).map_err(|e| e.to_string())?;
            key_arrays.push(array);
        }

        let len = chunk.len();
        let mut keep = vec![true; len];
        for filter in &self.filters {
            let Some(array) = key_arrays.get(filter.expr_order) else {
                return Err(format!(
                    "runtime filter {} expects probe key index {} but only {} keys are available",
                    filter.filter_id,
                    filter.expr_order,
                    key_arrays.len()
                ));
            };
            let arrays = [Arc::clone(array)];
            let views = build_group_key_views(&arrays)?;
            let hashes = build_group_key_hashes(&views, len, self.hash_seed)?;
            for row in 0..len {
                if !keep[row] {
                    continue;
                }
                if row_has_null(&views, row) || !filter.hashes.contains(&hashes[row]) {
                    keep[row] = false;
                }
            }
        }

        if keep.iter().all(|v| *v) {
            return Ok(Some(chunk));
        }
        if keep.iter().all(|v| !*v) {
            return Ok(None);
        }

        let mask = BooleanArray::from(keep);
        let filtered_batch = filter_record_batch(&chunk.batch, &mask).map_err(|e| e.to_string())?;
        Ok(Some(Chunk::new(filtered_batch)))
    }
}

impl RuntimeFilterMembership {
    pub(in crate::exec::runtime_filter) fn hash_seed(&self) -> u64 {
        self.hash_seed
    }

    pub(in crate::exec::runtime_filter) fn contains_hash(&self, hash: u64) -> bool {
        self.hashes.contains(&hash)
    }

    #[allow(dead_code)]
    pub(crate) fn filter_chunk_by_slot(
        &self,
        chunk: Chunk,
        slot_id: SlotId,
    ) -> Result<Option<Chunk>, String> {
        apply::filter_chunk_by_memberships(std::slice::from_ref(self), chunk, slot_id)
    }
}
