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
use compiled_ordered_output::CompiledOrderedOutputDriver;
pub(super) use compiled_ordered_output::CompiledOrderedOutputFactory;

use arrow::array::BooleanArray;

use crate::exec::chunk::Chunk;
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
    compiled_ordered: Option<CompiledOrderedOutputFactory>,
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
    compiled_ordered: Option<CompiledOrderedOutputDriver>,
    evaluation_factory: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    evaluation_control: Option<super::super::compiled_expression::RuntimeKernelControl>,
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
            compiled_ordered: None,
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
            compiled_ordered: None,
            conjunct: None,
            ordered_live: None,
            blocking: consumers.map(ScanBlockingConsumers::Compiled),
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
        let compiled_keys = match &self.blocking {
            Some(ScanBlockingConsumers::Compiled(consumers)) => {
                Some(match &self.compiled_ordered {
                    Some(ordered) => {
                        consumers.driver_keys_controlled(ordered.host.scalar_allocator())
                    }
                    None => consumers.driver_keys(),
                })
            }
            Some(ScanBlockingConsumers::Arena(_)) | None => None,
        };
        ScanDriverFilter {
            filter: self.clone(),
            compiled_keys,
            evaluation_factory: None,
            evaluation_control: None,
            compiled_ordered: self
                .compiled_ordered
                .as_ref()
                .map(|factory| factory.for_driver()),
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
    ) -> ExecutionResult<()> {
        self.evaluation_control =
            Some(super::super::compiled_expression::RuntimeKernelControl::new(state.error_state()));
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
        self.evaluation_factory = Some(Arc::clone(&factory));
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
        let Some(chunk) = filter.apply_conjunct_predicate_created(
            chunk,
            profiles,
            self.evaluation_factory.as_deref(),
            self.evaluation_control.as_ref(),
        )?
        else {
            return Ok(None);
        };
        let Some(chunk) = (match self.compiled_ordered.as_mut() {
            Some(ordered) => ordered.apply(chunk, profiles, event_sink)?,
            None => Some(chunk),
        }) else {
            return Ok(None);
        };
        let Some(chunk) = (match filter.ordered_live.as_ref() {
            Some(consumers) => consumers.apply_latest_chunk_created(
                chunk,
                self.evaluation_factory
                    .as_deref()
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?,
                self.evaluation_control
                    .as_ref()
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?,
                Some(event_sink),
            )?,
            None => Some(chunk),
        }) else {
            return Ok(None);
        };
        let Some(chunk) = (match (filter.blocking.as_ref(), self.compiled_keys.as_mut()) {
            (Some(ScanBlockingConsumers::Arena(consumers)), _) => consumers.apply_chunk_created(
                chunk,
                self.evaluation_factory
                    .as_deref()
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?,
                self.evaluation_control
                    .as_ref()
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?,
                Some(event_sink),
            )?,
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
    fn apply_conjunct_predicate_created(
        &self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
        factory: Option<&dyn crate::exec::pipeline::operator::InputConstructionFactory>,
        control: Option<&super::super::compiled_expression::RuntimeKernelControl>,
    ) -> ExecutionResult<Option<Chunk>> {
        let Some(conjunct) = self.conjunct.as_ref() else {
            return Ok(Some(chunk));
        };
        if chunk.is_empty() {
            return Ok(Some(chunk));
        }

        let input_rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);

        let factory =
            factory.ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let hydration_owner = factory.begin_input_construction(&chunk)?;
        let chunk =
            chunk.hydrate_for_evaluation(hydration_owner.as_ref(), |slot_id, data_type| {
                conjunct
                    .encoding_policy
                    .accepts_encoded_column(slot_id, data_type)
            })?;
        let owner = factory.begin_input_construction(&chunk)?;
        let evaluation = crate::exec::expr::ExprEvaluationContext::new(
            &conjunct.arena,
            owner.as_ref(),
            control.ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?,
        );
        let predicate_array = evaluation.eval(conjunct.predicate, &chunk)?;
        let filter_mask = predicate_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "scan conjunct predicate must return boolean array".to_string())?;
        let filtered_chunk = chunk.filter_for_evaluation(filter_mask, owner.as_ref())?;
        if let Some(profiles) = profiles {
            profiles
                .common
                .counter_add(SCAN_CONJUNCT_INPUT_ROWS, ProfileUnit::Unit, input_rows);
            profiles.common.counter_add(
                SCAN_CONJUNCT_OUTPUT_ROWS,
                ProfileUnit::Unit,
                i64::try_from(filtered_chunk.len()).unwrap_or(i64::MAX),
            );
        }
        if filtered_chunk.len() == 0 {
            return Ok(None);
        }
        Ok(Some(filtered_chunk))
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
