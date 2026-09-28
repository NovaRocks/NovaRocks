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

//! What a scan does to every chunk it read before handing it downstream:
//! its conjuncts, then its runtime filters, then its LIMIT, in that order.

use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;

use crate::exec::chunk::{Chunk, hydrate_dictionary_columns_except};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::scan::ScanNode;
use crate::exec::operators::FilterEncodingPolicy;
use crate::exec::operators::runtime_filter::{
    NativeOrderedLiveConsumerSet, RuntimeFilterConsumerSet,
};
use crate::runtime::fragment::io::FragmentEventSink;
use crate::runtime::profile::{OperatorProfiles, ProfileUnit};

// These counters describe the core-owned residual scan conjunct. They are
// intentionally separate from runtime-filter counters because a connector may
// use the same source predicate for pruning while the core still evaluates it
// for correctness.
pub(super) const SCAN_CONJUNCT_INPUT_ROWS: &str = "ScanConjunctInputRows";
pub(super) const SCAN_CONJUNCT_OUTPUT_ROWS: &str = "ScanConjunctOutputRows";
const ROWS_READ: &str = "RowsRead";

/// The filters a scan applies to what it read: its conjuncts, then live
/// ordered runtime filters, then blocking runtime filters.
#[derive(Clone)]
pub(super) struct ScanOutputFilter {
    conjunct_predicate: Option<ExprId>,
    conjunct_encoding_policy: Option<FilterEncodingPolicy>,
    arena: Arc<ExprArena>,
    ordered_live: Option<NativeOrderedLiveConsumerSet>,
    blocking: Option<RuntimeFilterConsumerSet>,
}

impl ScanOutputFilter {
    pub(super) fn new(
        scan: &ScanNode,
        arena: Arc<ExprArena>,
        blocking: Option<RuntimeFilterConsumerSet>,
        ordered_live: Option<NativeOrderedLiveConsumerSet>,
    ) -> Self {
        let conjunct_predicate = scan.conjunct_predicate();
        let conjunct_encoding_policy = conjunct_predicate
            .map(|predicate| FilterEncodingPolicy::from_predicate(&arena, predicate));
        Self {
            conjunct_predicate,
            conjunct_encoding_policy,
            arena,
            ordered_live,
            blocking,
        }
    }

    pub(super) fn ordered_live(&self) -> Option<&NativeOrderedLiveConsumerSet> {
        self.ordered_live.as_ref()
    }

    pub(super) fn blocking(&self) -> Option<&RuntimeFilterConsumerSet> {
        self.blocking.as_ref()
    }

    /// Picks up newly published live ordered filters before a chunk is
    /// filtered.
    pub(super) fn poll_live_updates(&self) -> Result<(), String> {
        match self.ordered_live.as_ref() {
            Some(consumers) => consumers.poll_updates(),
            None => Ok(()),
        }
    }

    /// Applies every filter in order; `None` when no row is left.
    pub(super) fn apply(
        &self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
        event_sink: &Arc<dyn FragmentEventSink>,
    ) -> Result<Option<Chunk>, String> {
        let Some(chunk) = self.apply_conjunct_predicate(chunk, profiles)? else {
            return Ok(None);
        };
        let Some(chunk) = (match self.ordered_live.as_ref() {
            Some(consumers) => consumers.apply_latest_chunk_observed(chunk, Some(event_sink))?,
            None => Some(chunk),
        }) else {
            return Ok(None);
        };
        let Some(chunk) = (match self.blocking.as_ref() {
            Some(consumers) => consumers.apply_chunk_observed(chunk, Some(event_sink))?,
            None => Some(chunk),
        }) else {
            return Ok(None);
        };
        Ok((!chunk.is_empty()).then_some(chunk))
    }

    fn apply_conjunct_predicate(
        &self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
    ) -> Result<Option<Chunk>, String> {
        let Some(predicate) = self.conjunct_predicate else {
            return Ok(Some(chunk));
        };
        if chunk.is_empty() {
            return Ok(Some(chunk));
        }

        let input_rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);

        let chunk = if let Some(policy) = self.conjunct_encoding_policy.as_ref() {
            hydrate_dictionary_columns_except(&chunk, |slot_id, data_type| {
                policy.accepts_encoded_column(slot_id, data_type)
            })?
        } else {
            chunk
        };

        let predicate_array = self
            .arena
            .eval(predicate, &chunk)
            .map_err(|e| e.to_string())?;
        let filter_mask = predicate_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "scan conjunct predicate must return boolean array".to_string())?;
        let filtered_batch = filter_record_batch(&chunk.batch, filter_mask)
            .map_err(|e| format!("scan conjunct filter failed: {}", e))?;
        if let Some(profiles) = profiles {
            profiles
                .common
                .counter_add(SCAN_CONJUNCT_INPUT_ROWS, ProfileUnit::Unit, input_rows);
            profiles.common.counter_add(
                SCAN_CONJUNCT_OUTPUT_ROWS,
                ProfileUnit::Unit,
                i64::try_from(filtered_batch.num_rows()).unwrap_or(i64::MAX),
            );
        }
        if filtered_batch.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(Chunk::new_like(filtered_batch, &chunk)))
    }
}

/// Counts a chunk the scan hands downstream.
pub(super) fn record_rows_read(profiles: Option<&OperatorProfiles>, rows: usize) {
    if let Some(profiles) = profiles {
        profiles.unique.counter_add(
            ROWS_READ,
            ProfileUnit::Unit,
            i64::try_from(rows).unwrap_or(i64::MAX),
        );
    }
}

/// What the scan LIMIT allows for a filtered chunk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ScanLimitDecision {
    /// Hand the chunk downstream and keep reading.
    Emit,
    /// Hand the chunk downstream and read no more: it reaches the limit.
    /// Rows beyond the limit are cut by the plan's limit operator.
    EmitThenStop,
    /// The limit was already reached: drop the chunk and read no more.
    Stop,
}

/// Decides a chunk of `rows` rows against the scan's `limit`, given the rows
/// the scan emitted before it.
pub(super) fn scan_limit_decision(
    limit: Option<usize>,
    rows_before: usize,
    rows: usize,
) -> ScanLimitDecision {
    match limit {
        None => ScanLimitDecision::Emit,
        Some(limit) if rows_before >= limit => ScanLimitDecision::Stop,
        Some(limit) if rows_before.saturating_add(rows) >= limit => ScanLimitDecision::EmitThenStop,
        Some(_) => ScanLimitDecision::Emit,
    }
}

#[cfg(test)]
mod tests {
    use super::{ScanLimitDecision, scan_limit_decision};

    #[test]
    fn the_scan_limit_emits_the_chunk_that_reaches_it_and_then_stops() {
        assert_eq!(scan_limit_decision(None, 100, 5), ScanLimitDecision::Emit);
        assert_eq!(scan_limit_decision(Some(10), 0, 5), ScanLimitDecision::Emit);
        assert_eq!(
            scan_limit_decision(Some(10), 5, 5),
            ScanLimitDecision::EmitThenStop
        );
        assert_eq!(
            scan_limit_decision(Some(10), 8, 5),
            ScanLimitDecision::EmitThenStop
        );
        assert_eq!(
            scan_limit_decision(Some(10), 10, 1),
            ScanLimitDecision::Stop
        );
    }
}
