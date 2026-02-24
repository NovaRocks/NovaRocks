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
//! Runtime-filter partial-result merger.
//!
//! Responsibilities:
//! - Merges partial IN filter payloads produced by multiple build-side drivers.
//! - Materializes final runtime membership filters when merge thresholds are satisfied.
//!
//! Key exported interfaces:
//! - Types: `RuntimeMembershipFilterBuildParam`, `PartialRuntimeInFilterMerger`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use arrow::array::ArrayRef;

use crate::common::ids::SlotId;

use super::bloom::RuntimeBloomFilter;
use super::in_filter::RuntimeInFilter;
use super::membership::{RuntimeEmptyFilter, RuntimeMembershipFilter};
use super::min_max::RuntimeMinMaxFilter;

#[derive(Clone, Debug)]
/// Build parameters used when constructing runtime membership filters.
pub(crate) struct RuntimeMembershipFilterBuildParam {
    filter_id: i32,
    slot_id: SlotId,
    ltype: crate::types::TPrimitiveType,
    join_mode: i8,
    arrays: Vec<ArrayRef>,
}

impl RuntimeMembershipFilterBuildParam {
    pub(crate) fn new(
        filter_id: i32,
        slot_id: SlotId,
        ltype: crate::types::TPrimitiveType,
        join_mode: i8,
    ) -> Self {
        Self {
            filter_id,
            slot_id,
            ltype,
            join_mode,
            arrays: Vec::new(),
        }
    }

    pub(crate) fn add_array(&mut self, array: ArrayRef) {
        self.arrays.push(array);
    }

    pub(crate) fn filter_id(&self) -> i32 {
        self.filter_id
    }

    pub(crate) fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub(crate) fn ltype(&self) -> crate::types::TPrimitiveType {
        self.ltype
    }

    pub(crate) fn join_mode(&self) -> i8 {
        self.join_mode
    }

    pub(crate) fn arrays(&self) -> &[ArrayRef] {
        &self.arrays
    }
}

#[derive(Debug)]
/// Accumulator that merges partial IN-filter payloads into final probe filters.
pub(crate) struct PartialRuntimeInFilterMerger {
    max_conditions: usize,
    expected_partitions: usize,
    state: std::sync::Mutex<PartialRuntimeInFilterState>,
}

#[derive(Debug)]
struct PartialRuntimeInFilterState {
    received: usize,
    received_membership: usize,
    ht_row_counts: Vec<usize>,
    partial_filters: Vec<Option<Vec<RuntimeInFilter>>>,
    partial_membership_params: Vec<Option<Vec<RuntimeMembershipFilterBuildParam>>>,
}

pub(crate) const MAX_RUNTIME_IN_FILTER_CONDITIONS: usize = 1024;

impl PartialRuntimeInFilterMerger {
    pub(crate) fn new(expected_partitions: usize, max_conditions: usize) -> Self {
        let expected_partitions = expected_partitions.max(1);
        let state = PartialRuntimeInFilterState {
            received: 0,
            received_membership: 0,
            ht_row_counts: vec![0; expected_partitions],
            partial_filters: vec![None; expected_partitions],
            partial_membership_params: vec![None; expected_partitions],
        };
        Self {
            max_conditions,
            expected_partitions,
            state: std::sync::Mutex::new(state),
        }
    }

    pub(crate) fn add_partial(
        &self,
        partition: usize,
        ht_row_count: usize,
        filters: Vec<RuntimeInFilter>,
    ) -> Result<Option<Vec<RuntimeInFilter>>, String> {
        if partition >= self.expected_partitions {
            return Err("runtime in-filter partition out of bounds".to_string());
        }
        let mut guard = self.state.lock().expect("runtime in-filter merge lock");
        guard.ht_row_counts[partition] = ht_row_count;
        guard.partial_filters[partition] = Some(filters);
        guard.received = guard.received.saturating_add(1);
        if guard.received < self.expected_partitions {
            return Ok(None);
        }

        let mut can_merge = true;
        let mut max_rows = 0;
        let mut active_indices = Vec::new();
        for (idx, rows) in guard.ht_row_counts.iter().copied().enumerate() {
            if rows == 0 {
                continue;
            }
            max_rows = max_rows.max(rows);
            let Some(filters) = guard.partial_filters[idx].as_ref() else {
                return Err("runtime in-filter missing partial filters".to_string());
            };
            if filters.is_empty() {
                can_merge = false;
                break;
            }
            active_indices.push(idx);
        }

        if !can_merge || max_rows > self.max_conditions || active_indices.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let mut total = guard.partial_filters[active_indices[0]]
            .as_ref()
            .ok_or_else(|| "runtime in-filter missing partial filters".to_string())?
            .clone();

        for idx in active_indices.iter().skip(1) {
            let filters = guard.partial_filters[*idx]
                .as_ref()
                .ok_or_else(|| "runtime in-filter missing partial filters".to_string())?;
            if filters.len() != total.len() {
                return Err("runtime in-filter size mismatch".to_string());
            }
            for (target, source) in total.iter_mut().zip(filters.iter()) {
                target.merge_from(source)?;
            }
        }
        Ok(Some(total))
    }

    pub(crate) fn add_partial_membership(
        &self,
        partition: usize,
        params: Vec<RuntimeMembershipFilterBuildParam>,
    ) -> Result<Option<Vec<RuntimeMembershipFilter>>, String> {
        if partition >= self.expected_partitions {
            return Err("runtime membership filter partition out of bounds".to_string());
        }
        let mut guard = self
            .state
            .lock()
            .expect("runtime membership filter merge lock");
        let first_insert = guard.partial_membership_params[partition].is_none();
        guard.partial_membership_params[partition] = Some(params);
        if first_insert {
            guard.received_membership = guard.received_membership.saturating_add(1);
        }
        if guard.received_membership < self.expected_partitions {
            return Ok(None);
        }

        let mut parts = Vec::with_capacity(guard.partial_membership_params.len());
        for slot in guard.partial_membership_params.iter() {
            let Some(params) = slot.as_ref() else {
                return Ok(None);
            };
            parts.push(params);
        }
        if parts.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let expected_len = parts[0].len();
        if expected_len == 0 {
            return Ok(Some(Vec::new()));
        }
        for params in parts.iter().skip(1) {
            if params.len() != expected_len {
                return Err("runtime membership filter size mismatch".to_string());
            }
        }

        let total_rows: u64 = guard.ht_row_counts.iter().copied().sum::<usize>() as u64;
        let mut merged = Vec::with_capacity(expected_len);
        for idx in 0..expected_len {
            let meta = &parts[0][idx];
            for params in parts.iter().skip(1) {
                let other = &params[idx];
                if meta.filter_id() != other.filter_id()
                    || meta.slot_id() != other.slot_id()
                    || meta.ltype() != other.ltype()
                    || meta.join_mode() != other.join_mode()
                {
                    return Err("runtime membership filter metadata mismatch".to_string());
                }
            }
            let mut min_max = RuntimeMinMaxFilter::empty_range(meta.ltype())?;
            for params in parts.iter() {
                let part = &params[idx];
                let part_min_max = RuntimeMinMaxFilter::from_arrays(meta.ltype(), part.arrays())?;
                min_max.merge_from(&part_min_max)?;
            }
            if total_rows == 0 {
                merged.push(RuntimeMembershipFilter::Empty(RuntimeEmptyFilter::new(
                    meta.filter_id(),
                    meta.slot_id(),
                    meta.ltype(),
                    false,
                    meta.join_mode(),
                    0,
                    min_max,
                )));
                continue;
            }
            let mut bloom = RuntimeBloomFilter::with_capacity(
                meta.filter_id(),
                meta.slot_id(),
                meta.ltype(),
                meta.join_mode(),
                total_rows,
                min_max,
            );
            for params in parts.iter() {
                let param = &params[idx];
                for array in param.arrays() {
                    bloom.insert_array(array)?;
                }
            }
            merged.push(RuntimeMembershipFilter::Bloom(bloom));
        }

        Ok(Some(merged))
    }
}
