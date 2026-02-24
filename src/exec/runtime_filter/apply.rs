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
//! Runtime-filter application helpers.
//!
//! Responsibilities:
//! - Applies membership and IN filters to chunks with optional expression-driven slot mapping.
//! - Builds row-selection masks and returns filtered chunks for probe-side pruning.
//!
//! Key exported interfaces:
//! - Functions: `filter_chunk_by_memberships`, `filter_chunk_by_in_filters`, `filter_chunk_by_in_filters_with_exprs`, `filter_chunk_by_membership_filters`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::hash_table::key_builder::{build_group_key_hashes, build_group_key_views};

use super::{RuntimeFilterMembership, RuntimeInFilter, RuntimeMembershipFilter};

/// Apply membership filters to a chunk and return the filtered chunk.
pub(crate) fn filter_chunk_by_memberships(
    filters: &[RuntimeFilterMembership],
    chunk: Chunk,
    slot_id: SlotId,
) -> Result<Option<Chunk>, String> {
    if filters.is_empty() {
        return Ok(Some(chunk));
    }
    if chunk.is_empty() {
        return Ok(Some(chunk));
    }
    if !chunk.slot_id_to_index().contains_key(&slot_id) {
        return Ok(Some(chunk));
    }
    let array = chunk.column_by_slot_id(slot_id)?;
    let arrays = [array];
    let views = build_group_key_views(&arrays)?;
    let len = chunk.len();
    let mut keep = vec![false; len];
    for filter in filters {
        let hashes = build_group_key_hashes(&views, len, filter.hash_seed())?;
        for row in 0..len {
            if keep[row] {
                continue;
            }
            if super::row_has_null(&views, row) {
                continue;
            }
            if filter.contains_hash(hashes[row]) {
                keep[row] = true;
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

/// Apply IN filters to a chunk and return the filtered chunk.
pub(crate) fn filter_chunk_by_in_filters(
    filters: &[Arc<RuntimeInFilter>],
    chunk: Chunk,
) -> Result<Option<Chunk>, String> {
    if filters.is_empty() {
        return Ok(Some(chunk));
    }
    if chunk.is_empty() {
        return Ok(Some(chunk));
    }
    let len = chunk.len();
    let mut keep = vec![true; len];
    for filter in filters {
        let filter = filter.as_ref();
        if filter.is_empty() {
            continue;
        }
        if !chunk.slot_id_to_index().contains_key(&filter.slot_id()) {
            continue;
        }
        let array = chunk.column_by_slot_id(filter.slot_id())?;
        for row in 0..len {
            if !keep[row] {
                continue;
            }
            if array.is_null(row) {
                keep[row] = false;
                continue;
            }
            if !filter.contains(&array, row)? {
                keep[row] = false;
            }
        }
        if keep.iter().all(|v| !*v) {
            return Ok(None);
        }
    }
    if keep.iter().all(|v| *v) {
        return Ok(Some(chunk));
    }
    let mask = BooleanArray::from(keep);
    let filtered_batch = filter_record_batch(&chunk.batch, &mask).map_err(|e| e.to_string())?;
    Ok(Some(Chunk::new(filtered_batch)))
}

/// Apply IN filters using expression mappings and return the filtered chunk.
pub(crate) fn filter_chunk_by_in_filters_with_exprs(
    arena: &ExprArena,
    exprs: &HashMap<i32, ExprId>,
    filters: &[Arc<RuntimeInFilter>],
    chunk: Chunk,
) -> Result<Option<Chunk>, String> {
    if filters.is_empty() {
        return Ok(Some(chunk));
    }
    let mut current = Some(chunk);
    for filter in filters {
        let filter_ref = filter.as_ref();
        let Some(chunk) = current else {
            return Ok(None);
        };
        let Some(expr_id) = exprs.get(&filter_ref.filter_id()) else {
            current = filter_chunk_by_in_filters(std::slice::from_ref(filter), chunk)?;
            continue;
        };
        let array = match arena.eval(*expr_id, &chunk) {
            Ok(array) => array,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("slot id") && msg.contains("not found in chunk") {
                    current = Some(chunk);
                    continue;
                }
                return Err(msg);
            }
        };
        current = filter_ref.filter_chunk_with_array(&array, chunk)?;
    }
    Ok(current)
}

#[allow(dead_code)]
/// Apply membership-filter wrappers to a chunk and return the filtered chunk.
pub(crate) fn filter_chunk_by_membership_filters(
    filters: &[Arc<RuntimeMembershipFilter>],
    chunk: Chunk,
) -> Result<Option<Chunk>, String> {
    if filters.is_empty() {
        return Ok(Some(chunk));
    }
    let mut current = Some(chunk);
    for filter in filters {
        let filter = filter.as_ref();
        let Some(chunk) = current else {
            return Ok(None);
        };
        current = filter.filter_chunk(chunk)?;
    }
    Ok(current)
}

/// Apply membership filters with expression mappings and return the filtered chunk.
pub(crate) fn filter_chunk_by_membership_filters_with_exprs(
    arena: &ExprArena,
    exprs: &HashMap<i32, ExprId>,
    filters: &[Arc<RuntimeMembershipFilter>],
    chunk: Chunk,
) -> Result<Option<Chunk>, String> {
    if filters.is_empty() {
        return Ok(Some(chunk));
    }
    let mut current = Some(chunk);
    for filter in filters {
        let filter = filter.as_ref();
        let Some(chunk) = current else {
            return Ok(None);
        };
        if let Some(expr_id) = exprs.get(&filter.filter_id()) {
            let array = match arena.eval(*expr_id, &chunk) {
                Ok(array) => array,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("slot id") && msg.contains("not found in chunk") {
                        current = Some(chunk);
                        continue;
                    }
                    return Err(msg);
                }
            };
            current = filter.filter_chunk_with_array(&array, chunk)?;
        } else {
            current = filter.filter_chunk(chunk)?;
        }
    }
    Ok(current)
}
