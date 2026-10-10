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
    CompiledRuntimeFilterConsumers, CompiledRuntimeFilterKeys, NativeOrderedLiveConsumerSet,
    RuntimeFilterConsumerSet, RuntimeFilterConsumerState,
};
use crate::runtime::fragment::ExecutionResult;
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
/// ordered runtime filters, then blocking runtime filters. A factory keeps
/// one and hands each driver its own [`ScanDriverFilter`].
#[derive(Clone)]
pub(super) struct ScanOutputFilter {
    conjunct: Option<ScanConjunct>,
    ordered_live: Option<NativeOrderedLiveConsumerSet>,
    blocking: Option<ScanBlockingConsumers>,
}

/// A scan's blocking membership runtime filters and where their keys come
/// from. Either way every driver of the scan shares one consumer state.
#[derive(Clone)]
enum ScanBlockingConsumers {
    /// Keys are the plan's ExprArena expressions.
    Arena(RuntimeFilterConsumerSet),
    /// Keys are the compiled scan's `RuntimeFilter` roots; each driver
    /// evaluates them through its own instances.
    Compiled(Arc<CompiledRuntimeFilterConsumers>),
}

/// The filters one driver applies: its scan's filters, and the driver's own
/// instances of compiled runtime-filter keys.
pub(super) struct ScanDriverFilter {
    filter: ScanOutputFilter,
    compiled_keys: Option<CompiledRuntimeFilterKeys>,
}

/// A legacy scan conjunct and the arena it is evaluated against.
#[derive(Clone)]
struct ScanConjunct {
    predicate: ExprId,
    encoding_policy: FilterEncodingPolicy,
    arena: Arc<ExprArena>,
}

impl ScanOutputFilter {
    pub(super) fn new(
        scan: &ScanNode,
        arena: Arc<ExprArena>,
        blocking: Option<RuntimeFilterConsumerSet>,
        ordered_live: Option<NativeOrderedLiveConsumerSet>,
    ) -> Self {
        let conjunct = scan.conjunct_predicate().map(|predicate| ScanConjunct {
            predicate,
            encoding_policy: FilterEncodingPolicy::from_predicate(&arena, predicate),
            arena,
        });
        Self {
            conjunct,
            ordered_live,
            blocking: blocking.map(ScanBlockingConsumers::Arena),
        }
    }

    /// The filter of a compiled scan: no conjunct and no expression arena,
    /// only its blocking membership consumers, if it has any. Every other
    /// chunk is handed downstream as read.
    pub(super) fn compiled(consumers: Option<Arc<CompiledRuntimeFilterConsumers>>) -> Self {
        Self {
            conjunct: None,
            ordered_live: None,
            blocking: consumers.map(ScanBlockingConsumers::Compiled),
        }
    }

    /// The filter one driver applies: the consumer states stay shared with
    /// every other driver, while compiled key instances are the driver's own.
    pub(super) fn for_driver(&self) -> ScanDriverFilter {
        let compiled_keys = match &self.blocking {
            Some(ScanBlockingConsumers::Compiled(consumers)) => Some(consumers.driver_keys()),
            Some(ScanBlockingConsumers::Arena(_)) | None => None,
        };
        ScanDriverFilter {
            filter: self.clone(),
            compiled_keys,
        }
    }

    /// The consumer state of the scan's blocking runtime filters.
    pub(super) fn blocking(&self) -> Option<&RuntimeFilterConsumerState> {
        self.blocking.as_ref().map(|blocking| match blocking {
            ScanBlockingConsumers::Arena(consumers) => consumers.state(),
            ScanBlockingConsumers::Compiled(consumers) => consumers.state(),
        })
    }
}

impl ScanDriverFilter {
    pub(super) fn bind_runtime_state(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) {
        if let Some(keys) = &mut self.compiled_keys {
            keys.bind_runtime_state(state);
        }
    }

    pub(super) fn bind_mem_tracker(
        &mut self,
        tracker: std::sync::Arc<crate::runtime::mem_tracker::MemTracker>,
    ) {
        if let Some(keys) = &mut self.compiled_keys {
            keys.bind_mem_tracker(tracker);
        }
    }
    pub(super) fn ordered_live(&self) -> Option<&NativeOrderedLiveConsumerSet> {
        self.filter.ordered_live.as_ref()
    }

    /// The consumer state of the scan's blocking runtime filters.
    pub(super) fn blocking(&self) -> Option<&RuntimeFilterConsumerState> {
        self.filter.blocking()
    }

    /// Picks up newly published live ordered filters before a chunk is
    /// filtered.
    pub(super) fn poll_live_updates(&self) -> Result<(), String> {
        match self.filter.ordered_live.as_ref() {
            Some(consumers) => consumers.poll_updates(),
            None => Ok(()),
        }
    }

    /// Applies every filter in order; `None` when no row is left.
    pub(super) fn apply(
        &mut self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
        event_sink: &Arc<dyn FragmentEventSink>,
    ) -> ExecutionResult<Option<Chunk>> {
        let filter = &self.filter;
        let Some(chunk) = filter.apply_conjunct_predicate(chunk, profiles)? else {
            return Ok(None);
        };
        let Some(chunk) = (match filter.ordered_live.as_ref() {
            Some(consumers) => consumers.apply_latest_chunk_observed(chunk, Some(event_sink))?,
            None => Some(chunk),
        }) else {
            return Ok(None);
        };
        let Some(chunk) = (match (filter.blocking.as_ref(), self.compiled_keys.as_mut()) {
            (Some(ScanBlockingConsumers::Arena(consumers)), _) => {
                consumers.apply_chunk_observed(chunk, Some(event_sink))?
            }
            (Some(ScanBlockingConsumers::Compiled(consumers)), Some(keys)) => consumers
                .state()
                .apply_chunk_observed(chunk, keys, Some(event_sink))?,
            (Some(ScanBlockingConsumers::Compiled(_)), None) => {
                return Err("compiled scan runtime filters have no driver key instances".into());
            }
            (None, _) => Some(chunk),
        }) else {
            return Ok(None);
        };
        Ok((!chunk.is_empty()).then_some(chunk))
    }
}

impl ScanOutputFilter {
    fn apply_conjunct_predicate(
        &self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
    ) -> Result<Option<Chunk>, String> {
        let Some(conjunct) = self.conjunct.as_ref() else {
            return Ok(Some(chunk));
        };
        if chunk.is_empty() {
            return Ok(Some(chunk));
        }

        let input_rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);

        let chunk = hydrate_dictionary_columns_except(&chunk, |slot_id, data_type| {
            conjunct
                .encoding_policy
                .accepts_encoded_column(slot_id, data_type)
        })?;

        let predicate_array = conjunct
            .arena
            .eval(conjunct.predicate, &chunk)
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
