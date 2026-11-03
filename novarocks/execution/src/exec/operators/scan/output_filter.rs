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
#[path = "compiled_ordered_output.rs"]
mod compiled_ordered_output;
use compiled_ordered_output::CompiledOrderedOutputEvaluator;
pub(super) use compiled_ordered_output::CompiledOrderedOutputFactory;

use crate::exec::chunk::Chunk;
use crate::exec::operators::runtime_filter::{
    CompiledRuntimeFilterConsumers, CompiledRuntimeFilterKeys, RuntimeFilterConsumerState,
};
use crate::runtime::profile::{OperatorProfiles, ProfileUnit};
use crate::runtime::task::io::TaskEventSink;
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

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
    compiled_ordered: Option<CompiledOrderedOutputFactory>,
    blocking: Option<Arc<CompiledRuntimeFilterConsumers>>,
}

/// The filters one driver applies: its scan's filters, and the driver's own
/// instances of compiled runtime-filter keys.
pub(super) struct ScanDriverFilter {
    filter: ScanOutputFilter,
    compiled_keys: Option<CompiledRuntimeFilterKeys>,
    compiled_ordered: Option<CompiledOrderedOutputEvaluator>,
}

impl ScanOutputFilter {
    /// The filter of a compiled scan: no conjunct and no expression arena,
    /// only its blocking membership consumers, if it has any. Every other
    /// chunk is handed downstream as read.
    pub(super) fn compiled(consumers: Option<Arc<CompiledRuntimeFilterConsumers>>) -> Self {
        Self {
            compiled_ordered: None,
            blocking: consumers,
        }
    }

    pub(super) fn compiled_ordered(
        consumers: Option<Arc<CompiledRuntimeFilterConsumers>>,
        ordered: CompiledOrderedOutputFactory,
    ) -> Self {
        let mut filter = Self::compiled(consumers);
        filter.compiled_ordered = Some(ordered);
        filter
    }

    /// The filter one driver applies: the consumer states stay shared with
    /// every other driver, while compiled key instances are the driver's own.
    pub(super) fn for_driver(&self) -> ScanDriverFilter {
        let compiled_keys = self
            .blocking
            .as_ref()
            .map(|consumers| match &self.compiled_ordered {
                Some(ordered) => consumers.driver_keys_controlled(ordered.host.scalar_allocator()),
                None => consumers.driver_keys(),
            });
        ScanDriverFilter {
            filter: self.clone(),
            compiled_keys,
            compiled_ordered: self
                .compiled_ordered
                .as_ref()
                .map(|factory| factory.for_driver()),
        }
    }

    /// The consumer state of the scan's blocking runtime filters.
    pub(super) fn blocking(&self) -> Option<&RuntimeFilterConsumerState> {
        self.blocking.as_ref().map(|consumers| consumers.state())
    }
}

impl ScanDriverFilter {
    pub(super) fn bind_runtime_state(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) -> ExecutionResult<()> {
        if let Some(keys) = &mut self.compiled_keys {
            keys.bind_runtime_state(state);
        }
        if let Some(ordered) = &mut self.compiled_ordered {
            ordered.bind(state)?;
        }
        Ok(())
    }

    pub(super) fn bind_mem_tracker(
        &mut self,
        tracker: std::sync::Arc<crate::runtime::mem_tracker::MemTracker>,
    ) {
        if let Some(keys) = &mut self.compiled_keys {
            keys.bind_mem_tracker(Arc::clone(&tracker));
        }
        if let Some(ordered) = &mut self.compiled_ordered {
            ordered.bind_mem_tracker(tracker);
        }
    }
    pub(super) fn bind_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        if let Some(keys) = &mut self.compiled_keys {
            keys.bind_construction_factory(Arc::clone(&factory));
        }
        if let Some(ordered) = &mut self.compiled_ordered {
            ordered.bind_construction_factory(factory);
        }
    }
    /// The ordered composition borrows the real input before its filter/slice
    /// creators retain it. Its original whole-source handoff must precede that
    /// borrow; a provider token is preserved by the existing handoff author.
    pub(super) fn requires_input_accounting(&self) -> bool {
        self.compiled_ordered.is_some()
    }

    pub(super) fn restrict_limit(
        &mut self,
        chunk: Chunk,
        limit: Option<usize>,
        rows_before: usize,
    ) -> ExecutionResult<Option<Chunk>> {
        match &mut self.compiled_ordered {
            Some(ordered) => ordered.restrict_limit(chunk, limit, rows_before),
            None => Ok(Some(chunk)),
        }
    }

    /// The consumer state of the scan's blocking runtime filters.
    pub(super) fn blocking(&self) -> Option<&RuntimeFilterConsumerState> {
        self.filter.blocking()
    }

    /// Applies every filter in order; `None` when no row is left.
    pub(super) fn apply(
        &mut self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
        event_sink: &Arc<dyn TaskEventSink>,
    ) -> ExecutionResult<Option<Chunk>> {
        let filter = &self.filter;
        let Some(chunk) = (match self.compiled_ordered.as_mut() {
            Some(ordered) => ordered.apply(chunk, profiles, event_sink)?,
            None => Some(chunk),
        }) else {
            return Ok(None);
        };
        let Some(chunk) = (match (filter.blocking.as_ref(), self.compiled_keys.as_mut()) {
            (Some(consumers), Some(keys)) => {
                consumers
                    .state()
                    .apply_chunk_observed(chunk, keys, Some(event_sink))?
            }
            (Some(_), None) => {
                return Err(scan_contract(
                    "compiled scan runtime filters have no driver key instances",
                ));
            }
            (None, _) => Some(chunk),
        }) else {
            return Ok(None);
        };
        Ok((!chunk.is_empty()).then_some(chunk))
    }
}

impl ScanOutputFilter {}

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

#[derive(Debug)]
enum ScanOutputFailure {
    Contract(&'static str),
}
impl std::fmt::Display for ScanOutputFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => f.write_str(detail),
        }
    }
}
impl std::error::Error for ScanOutputFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(_) => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ScanOutputFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let (class, reason) = match self {
            Self::Contract(_) => (FailureClass::Invariant, ExecutionReason::ScanOutputContract),
        };
        FailureFact::new(class, reason.reason())
    }
}
fn scan_contract(detail: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_domain(ScanOutputFailure::Contract(detail))
}
