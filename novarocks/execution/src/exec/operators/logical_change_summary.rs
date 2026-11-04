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

//! Task-local logical contributions and receiver-authored summary provenance.
//! A contributor finishes on input exhaustion, independently of writer exit.
use crate::{
    exec::{
        chunk::{Chunk, ChunkSchemaRef},
        pipeline::{
            operator::{
                ConstructionFailure, InputConstructionFactory, Operator, OperatorLifecycle,
            },
            operator_factory::OperatorFactory,
            shape::{Source, SourceKernel},
            step::{NextStep, StepResult, WaitFact, WaitReason},
            step_context::StepContext,
        },
    },
    runtime::{
        exchange::{ExchangeReceiverHandle, SourcedExchangePopResult},
        fragment::{ExecutionFailure, ExecutionReason, ExecutionResult},
        mem_tracker::{MemTracker, TrackedBytes},
        observable::Observable,
    },
};
use arrow::{
    array::{Array, ArrayRef, Int64Array},
    datatypes::DataType,
};
use novarocks_execution_contract::ExchangeSource;
use novarocks_local_program::{LogicalChangeCounts, StaticLayout};
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub(crate) enum SummaryFailure {
    Contributors,
    DuplicateContributor,
    MissingContributor,
    OutputClaimed,
    Count(novarocks_result_contract::LogicalChangeCountError),
    Schema,
    Source,
    DuplicateSource,
    MissingSource,
    WithdrawnWriter,
}
impl std::fmt::Display for SummaryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contributors => f.write_str("logical summary requires the actual nonempty contributor set"),
            Self::DuplicateContributor => f.write_str("logical summary contributor was claimed or finished twice"),
            Self::MissingContributor => f.write_str("logical summary contributor exited before input exhaustion"),
            Self::OutputClaimed => f.write_str("logical summary has more than one output owner"),
            Self::Count(error) => error.fmt(f),
            Self::Schema => f.write_str("logical summary must be one row of three required Int64 counts under its exact declaration"),
            Self::Source => f.write_str("logical summary source differs from the frozen receiver source set"),
            Self::DuplicateSource => f.write_str("logical summary source contributed more than once"),
            Self::MissingSource => f.write_str("normal logical summary EOS has incomplete source coverage"),
            Self::WithdrawnWriter => f.write_str("writer-results input withdrew before normal exhaustion"),
        }
    }
}
impl std::error::Error for SummaryFailure {}
impl novarocks_failure_contract::FailureProjection for SummaryFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            if matches!(self, Self::Count(_)) {
                ExecutionReason::LogicalChangeCount
            } else {
                ExecutionReason::LogicalSummaryContract
            }
            .reason(),
        )
    }
}
pub(crate) fn summary_failure(cause: SummaryFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(cause)
}
fn add(
    left: LogicalChangeCounts,
    right: LogicalChangeCounts,
) -> ExecutionResult<LogicalChangeCounts> {
    left.checked_add(right)
        .map_err(|error| summary_failure(SummaryFailure::Count(error)))
}
struct Reduction {
    // 0 = unclaimed, 1 = live, 2 = normally exhausted. No per-row ledger.
    contributors: Vec<u8>,
    remaining: usize,
    counts: LogicalChangeCounts,
    failure: Option<ExecutionFailure>,
    output_claimed: bool,
}
#[derive(Clone)]
pub(crate) struct LogicalSummaryState {
    inner: Arc<Mutex<Reduction>>,
    observable: Arc<Observable>,
    _accounting: Arc<TrackedBytes>,
}
impl LogicalSummaryState {
    pub(crate) fn new(contributors: usize, tracker: Arc<MemTracker>) -> ExecutionResult<Self> {
        if contributors == 0 {
            return Err(summary_failure(SummaryFailure::Contributors));
        }
        let bytes = contributors
            .checked_add(std::mem::size_of::<Reduction>())
            .ok_or(ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            ))?;
        let accounting =
            TrackedBytes::try_new(bytes, tracker).map_err(ExecutionFailure::from_domain)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Reduction {
                contributors: vec![0; contributors],
                remaining: contributors,
                counts: LogicalChangeCounts::ZERO,
                failure: None,
                output_claimed: false,
            })),
            observable: Arc::new(Observable::new()),
            _accounting: Arc::new(accounting),
        })
    }
    pub(crate) fn validate_geometry(&self, dop: i32) -> ExecutionResult<()> {
        if usize::try_from(dop).ok()
            != Some(
                self.inner
                    .lock()
                    .expect("logical summary lock")
                    .contributors
                    .len(),
            )
        {
            return Err(summary_failure(SummaryFailure::Contributors));
        }
        Ok(())
    }
    pub(crate) fn contributor(&self, ordinal: usize) -> ExecutionResult<LogicalSummaryContributor> {
        let mut state = self.inner.lock().expect("logical summary lock");
        let slot = state
            .contributors
            .get_mut(ordinal)
            .ok_or_else(|| summary_failure(SummaryFailure::Contributors))?;
        if *slot != 0 {
            return Err(summary_failure(SummaryFailure::DuplicateContributor));
        }
        *slot = 1;
        Ok(LogicalSummaryContributor {
            state: self.clone(),
            ordinal,
            counts: LogicalChangeCounts::ZERO,
            finished: false,
        })
    }
    fn claim_output(&self) -> ExecutionResult<()> {
        let mut state = self.inner.lock().expect("logical summary lock");
        if state.output_claimed {
            return Err(summary_failure(SummaryFailure::OutputClaimed));
        }
        state.output_claimed = true;
        Ok(())
    }
    fn result(&self) -> ExecutionResult<Option<LogicalChangeCounts>> {
        let state = self.inner.lock().expect("logical summary lock");
        if let Some(error) = &state.failure {
            return Err(error.clone());
        }
        Ok((state.remaining == 0).then_some(state.counts))
    }
}
pub(crate) struct LogicalSummaryContributor {
    state: LogicalSummaryState,
    ordinal: usize,
    counts: LogicalChangeCounts,
    finished: bool,
}
impl LogicalSummaryContributor {
    pub(crate) fn accept(&mut self, counts: LogicalChangeCounts) -> ExecutionResult<()> {
        if self.finished {
            return Err(summary_failure(SummaryFailure::DuplicateContributor));
        }
        self.counts = add(self.counts, counts)?;
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> ExecutionResult<()> {
        if self.finished {
            return Err(summary_failure(SummaryFailure::DuplicateContributor));
        }
        let notify = self.state.observable.defer_notify();
        let mut state = self.state.inner.lock().expect("logical summary lock");
        match add(state.counts, self.counts) {
            Ok(counts) => state.counts = counts,
            Err(error) => {
                state.failure.get_or_insert(error.clone());
                notify.arm();
                return Err(error);
            }
        }
        state.contributors[self.ordinal] = 2;
        state.remaining -= 1;
        self.finished = true;
        notify.arm();
        Ok(())
    }
}
impl Drop for LogicalSummaryContributor {
    fn drop(&mut self) {
        if !self.finished {
            let notify = self.state.observable.defer_notify();
            self.state
                .inner
                .lock()
                .expect("logical summary lock")
                .failure
                .get_or_insert_with(|| summary_failure(SummaryFailure::MissingContributor));
            notify.arm();
        }
    }
}
pub(crate) struct LogicalSummarySourceFactory {
    state: LogicalSummaryState,
    layout: StaticLayout,
}
impl LogicalSummarySourceFactory {
    pub(crate) fn new(state: LogicalSummaryState, layout: StaticLayout) -> ExecutionResult<Self> {
        validate_layout(&layout)?;
        Ok(Self { state, layout })
    }
}
fn validate_layout(layout: &StaticLayout) -> ExecutionResult<()> {
    if layout.slots().len() != 3
        || layout
            .schema()
            .fields()
            .iter()
            .any(|field| field.data_type() != &DataType::Int64 || field.is_nullable())
    {
        return Err(summary_failure(SummaryFailure::Schema));
    }
    Ok(())
}
impl OperatorFactory for LogicalSummarySourceFactory {
    fn name(&self) -> &str {
        "LogicalChangeSummary"
    }
    fn is_source(&self) -> bool {
        true
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let failure = if dop != 1 || driver_id != 0 {
            Some(summary_failure(SummaryFailure::OutputClaimed))
        } else {
            self.state.claim_output().err()
        };
        Box::new(Source::new(SummarySource {
            state: self.state.clone(),
            layout: self.layout.clone(),
            schema: None,
            factory: None,
            emitted: false,
            failure,
            #[cfg(novarocks_dev_controls)]
            diagnostics: None,
        }))
    }
}
struct SummarySource {
    state: LogicalSummaryState,
    layout: StaticLayout,
    schema: Option<ChunkSchemaRef>,
    factory: Option<Arc<dyn InputConstructionFactory>>,
    emitted: bool,
    failure: Option<ExecutionFailure>,
    #[cfg(novarocks_dev_controls)]
    diagnostics: Option<crate::runtime::writer_diagnostics::WriterDiagnostics>,
}
impl OperatorLifecycle for SummarySource {
    fn name(&self) -> &str {
        "LogicalChangeSummary"
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn bind_runtime_state(
        &mut self,
        _state: &crate::runtime::runtime_state::RuntimeState,
    ) -> ExecutionResult<()> {
        #[cfg(novarocks_dev_controls)]
        {
            self.diagnostics =
                crate::runtime::writer_diagnostics::WriterDiagnostics::from_runtime(_state);
        }
        Ok(())
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.factory = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        self.schema = Some(
            self.factory
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?
                .create_compiled_chunk_schema(&self.layout)?,
        );
        Ok(())
    }
}
impl SourceKernel for SummarySource {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.state.observable.clone())
    }
    fn poll_output(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        ctx.checkpoint()?;
        #[cfg(novarocks_dev_controls)]
        if let Some(scope) = &self.diagnostics {
            scope.emit("summary_source_poll", || {
                let reduction = self
                    .state
                    .inner
                    .try_lock()
                    .ok()
                    .map(|state| {
                        format!(
                            "remaining={} contributors={:?} failed={} output_claimed={}",
                            state.remaining,
                            state.contributors,
                            state.failure.is_some(),
                            state.output_claimed
                        )
                    })
                    .unwrap_or_else(|| "unavailable".into());
                format!(
                    "emitted={} generation={} reduction={reduction}",
                    self.emitted,
                    self.state.observable.generation()
                )
            });
        }
        if self.emitted {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        let generation = self.state.observable.generation();
        let Some(counts) = self.state.result()? else {
            return Ok(StepResult::new(
                None,
                NextStep::Wait(WaitFact {
                    reason: WaitReason::Input,
                    observable: self.state.observable.clone(),
                    observed_generation: generation,
                    deadline: None,
                }),
                0,
            ));
        };
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let output = summary_chunk(
            counts,
            self.schema
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?,
            self.factory
                .as_deref()
                .ok_or(ConstructionFailure::MissingOwner)?,
        )?;
        self.emitted = true;
        #[cfg(novarocks_dev_controls)]
        if let Some(scope) = &self.diagnostics {
            scope.emit("summary_source_emitted", || format!("counts={counts:?}"));
        }
        Ok(StepResult::new(Some(output), NextStep::Finished, 0))
    }
}
fn copy_failure(
    error: novarocks_functions::selected_copy::CopyConstructionError<ConstructionFailure>,
) -> ExecutionFailure {
    use novarocks_functions::selected_copy::CopyConstructionError;
    match error {
        CopyConstructionError::Creator(e) => e.into(),
        CopyConstructionError::Resource(e) => ConstructionFailure::Resource(e).into(),
        CopyConstructionError::Copy(e) => e.into_kernel().into(),
    }
}
pub(crate) fn summary_chunk(
    counts: LogicalChangeCounts,
    schema: &ChunkSchemaRef,
    factory: &dyn InputConstructionFactory,
) -> ExecutionResult<Chunk> {
    use novarocks_functions::selected_copy;
    use novarocks_type_contract::ValueSchemaCreation;
    let owner = factory.begin_typed_construction(schema)?;
    owner.request(Layout::array::<ArrayRef>(3).map_err(|_| {
        ConstructionFailure::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?)?;
    let mut columns = Vec::with_capacity(3);
    for value in counts.into_wire() {
        // Exact value buffer plus the public Arrow host's metadata invoice.
        owner.request(Layout::new::<i64>())?;
        let mut request = |layout| owner.request(layout);
        let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
        selected_copy::preflight_flat_array_structure_construction(
            schema.slots()[columns.len()].typed_binding().layout(),
            false,
            &mut creation,
            &mut |boundary| factory.observe_input_work(usize::from(!boundary)),
        )
        .map_err(copy_failure)?;
        columns.push(Arc::new(Int64Array::from(vec![value])) as ArrayRef);
    }
    Ok(Chunk::try_new_typed_with_columns_owned_observed::<
        ConstructionFailure,
        _,
    >(
        schema.clone(),
        columns,
        1,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )?)
}

/// This value can only be produced from the receiver queue's original source.
/// Payloads contain counts only, never an identity chosen by their sender.
pub(crate) struct SourceBoundSummary {
    source: ExchangeSource,
    counts: LogicalChangeCounts,
}
pub(crate) enum SummaryRead {
    Summary(SourceBoundSummary),
    Pending(WaitFact),
    End,
}
/// One immutable index shared by ingress validation and finish coverage.
/// The descriptor's original order does not become a semantic source ordinal.
pub(crate) struct SummarySourceIndex {
    sorted: Vec<ExchangeSource>,
    _accounting: TrackedBytes,
}
impl SummarySourceIndex {
    fn new(
        expected: &[ExchangeSource],
        tracker: Arc<MemTracker>,
        factory: &dyn InputConstructionFactory,
    ) -> ExecutionResult<Arc<Self>> {
        if expected.is_empty() {
            return Err(summary_failure(SummaryFailure::Source));
        }
        let bytes = Layout::array::<ExchangeSource>(expected.len()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?.size()
            .checked_add(std::mem::size_of::<Self>()).ok_or(ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?;
        let accounting =
            TrackedBytes::try_new(bytes, tracker).map_err(ExecutionFailure::from_domain)?;
        let mut sorted = Vec::with_capacity(expected.len());
        for (ordinal, source) in expected.iter().enumerate() {
            if ordinal % 256 == 0 {
                factory.observe_input_work(256.min(expected.len() - ordinal))?;
            }
            sorted.push(*source);
        }
        // One bounded library sort of fixed-width, descriptor-admitted keys;
        // preflight its work and recheck control before publishing the index.
        factory.observe_input_work(
            expected
                .len()
                .saturating_mul(expected.len().ilog2() as usize + 1),
        )?;
        sorted.sort_unstable();
        factory.observe_input_work(0)?;
        for (ordinal, pair) in sorted.windows(2).enumerate() {
            if ordinal % 256 == 0 {
                factory.observe_input_work(256.min(sorted.len() - ordinal))?;
            }
            if pair[0] == pair[1] {
                return Err(summary_failure(SummaryFailure::Source));
            }
        }
        Ok(Arc::new(Self {
            sorted,
            _accounting: accounting,
        }))
    }
    fn ordinal(&self, source: &ExchangeSource) -> Option<usize> {
        self.sorted.binary_search(source).ok()
    }
}
pub(crate) struct SourceBoundSummaryInput {
    receiver: ExchangeReceiverHandle,
    expected: Arc<[ExchangeSource]>,
    index: Option<Arc<SummarySourceIndex>>,
    layout: StaticLayout,
    schema: Option<ChunkSchemaRef>,
    ended: bool,
    #[cfg(novarocks_dev_controls)]
    diagnostics: Option<crate::runtime::writer_diagnostics::WriterDiagnostics>,
}
impl SourceBoundSummaryInput {
    pub(crate) fn new(
        receiver: ExchangeReceiverHandle,
        expected: Arc<[ExchangeSource]>,
        layout: StaticLayout,
    ) -> ExecutionResult<Self> {
        validate_layout(&layout)?;
        if expected.is_empty() {
            return Err(summary_failure(SummaryFailure::Source));
        }
        Ok(Self {
            receiver,
            expected,
            index: None,
            layout,
            schema: None,
            ended: false,
            #[cfg(novarocks_dev_controls)]
            diagnostics: None,
        })
    }
    pub(crate) fn prepare(
        &mut self,
        factory: &dyn InputConstructionFactory,
        tracker: Arc<MemTracker>,
    ) -> ExecutionResult<()> {
        self.schema = Some(factory.create_compiled_chunk_schema(&self.layout)?);
        self.index = Some(SummarySourceIndex::new(&self.expected, tracker, factory)?);
        Ok(())
    }
    pub(crate) fn layout(&self) -> &StaticLayout {
        &self.layout
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn bind_diagnostics(
        &mut self,
        scope: Option<crate::runtime::writer_diagnostics::WriterDiagnostics>,
    ) {
        self.diagnostics = scope;
    }
    pub(crate) fn sources(&self) -> ExecutionResult<Arc<SummarySourceIndex>> {
        self.index
            .clone()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }
    pub(crate) fn observable(&self) -> Arc<Observable> {
        self.receiver.observable()
    }
    pub(crate) fn ready(&self) -> bool {
        self.ended || self.receiver.has_output_or_finished(self.expected.len())
    }
    pub(crate) fn poll(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<SummaryRead> {
        ctx.checkpoint()?;
        if self.ended {
            return Ok(SummaryRead::End);
        }
        let event = self.receiver.observable();
        let generation = event.generation();
        match self
            .receiver
            .try_pop_sourced_with_stats(self.expected.len())
            .map_err(|e| e.into_execution())?
        {
            Some(SourcedExchangePopResult::Chunk(item)) => {
                let (source, chunk) = item.into_parts();
                let index = self
                    .index
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?;
                let source = source
                    .filter(|source| index.ordinal(source).is_some())
                    .ok_or_else(|| summary_failure(SummaryFailure::Source))?;
                let schema = self
                    .schema
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?;
                if chunk.len() != 1 || chunk.chunk_schema() != schema.as_ref() {
                    return Err(summary_failure(SummaryFailure::Schema));
                }
                let mut wire = [0; 3];
                for (ordinal, value) in wire.iter_mut().enumerate() {
                    let a = chunk.columns()[ordinal]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .filter(|a| a.null_count() == 0)
                        .ok_or_else(|| summary_failure(SummaryFailure::Schema))?;
                    *value = a.value(0);
                }
                let counts = LogicalChangeCounts::try_from_wire(wire[0], wire[1], wire[2])
                    .map_err(|e| summary_failure(SummaryFailure::Count(e)))?;
                #[cfg(novarocks_dev_controls)]
                if let Some(scope) = &self.diagnostics {
                    scope.emit("summary_input_handoff", || {
                        format!(
                            "source={source:?} counts={counts:?} expected={:?}",
                            self.expected
                        )
                    });
                }
                Ok(SummaryRead::Summary(SourceBoundSummary { source, counts }))
            }
            Some(SourcedExchangePopResult::Finished(_)) => {
                self.receiver
                    .complete_input_consumption(self.expected.len())
                    .map_err(|e| e.into_execution())?;
                self.ended = true;
                #[cfg(novarocks_dev_controls)]
                if let Some(scope) = &self.diagnostics {
                    scope.emit("summary_input_consumed", || {
                        format!("expected={:?}", self.expected)
                    });
                }
                Ok(SummaryRead::End)
            }
            None => {
                #[cfg(novarocks_dev_controls)]
                if let Some(scope) = &self.diagnostics {
                    scope.emit("summary_input_wait", || {
                        format!("generation={generation} expected={:?}", self.expected)
                    });
                }
                Ok(SummaryRead::Pending(WaitFact {
                    reason: WaitReason::Input,
                    observable: event,
                    observed_generation: generation,
                    deadline: None,
                }))
            }
        }
    }
}
/// Finish-owned coverage, independent of writer target ordinals and profiles.
pub(crate) struct SummaryCoverage {
    expected: Arc<SummarySourceIndex>,
    remaining: usize,
    seen: Vec<bool>,
    counts: LogicalChangeCounts,
    _accounting: TrackedBytes,
}
impl SummaryCoverage {
    pub(crate) fn new(
        expected: Arc<SummarySourceIndex>,
        tracker: Arc<MemTracker>,
    ) -> ExecutionResult<Self> {
        let accounting = TrackedBytes::try_new(expected.sorted.len(), tracker)
            .map_err(ExecutionFailure::from_domain)?;
        Ok(Self {
            seen: vec![false; expected.sorted.len()],
            remaining: expected.sorted.len(),
            expected,
            counts: LogicalChangeCounts::ZERO,
            _accounting: accounting,
        })
    }
    pub(crate) fn accept(&mut self, summary: SourceBoundSummary) -> ExecutionResult<()> {
        let ordinal = self
            .expected
            .ordinal(&summary.source)
            .ok_or_else(|| summary_failure(SummaryFailure::Source))?;
        if self.seen[ordinal] {
            return Err(summary_failure(SummaryFailure::DuplicateSource));
        }
        let next = add(self.counts, summary.counts)?;
        self.counts = next;
        self.seen[ordinal] = true;
        self.remaining -= 1;
        Ok(())
    }
    pub(crate) fn finish(&self) -> ExecutionResult<LogicalChangeCounts> {
        if self.remaining != 0 {
            return Err(summary_failure(SummaryFailure::MissingSource));
        }
        Ok(self.counts)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::exec::pipeline::step_context::TurnWork;
    use crate::runtime::{
        exchange::{ExchangeKey, ExecutionExchangeRegistry, encode_chunks},
        runtime_state::RuntimeState,
    };
    use novarocks_execution_contract::ExchangeStreamIdentity;
    use novarocks_types::{SlotId, UniqueId};
    pub(crate) fn summary_layout() -> StaticLayout {
        crate::exec::chunk::typed_fixture::layout(
            Arc::new(arrow::datatypes::Schema::new(
                (0..3)
                    .map(|i| {
                        arrow::datatypes::Field::new(format!("count{i}"), DataType::Int64, false)
                    })
                    .collect::<Vec<_>>(),
            )),
            &[SlotId(801), SlotId(802), SlotId(803)],
            &std::array::from_fn::<_, 3, _>(|_| {
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, false)
            }),
        )
    }
    pub(crate) fn creator(state: &RuntimeState) -> Arc<dyn InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            MemTracker::new_root("logical result construction"),
            state.error_state(),
        )
        .unwrap()
    }
    pub(crate) struct IngressFixture {
        pub(crate) registry: Arc<ExecutionExchangeRegistry>,
        pub(crate) key: ExchangeKey,
        pub(crate) streams: Vec<ExchangeStreamIdentity>,
        pub(crate) layout: StaticLayout,
        pub(crate) schema: ChunkSchemaRef,
        pub(crate) factory: Arc<dyn InputConstructionFactory>,
    }
    impl IngressFixture {
        /// Install the explicit original Task for this operator fixture while
        /// preserving its configured runtime, query options and tracker.
        pub(crate) fn task_state(state: &RuntimeState) -> Arc<RuntimeState> {
            let owner = crate::runtime::task_control::test_support::Owner::new();
            let control = Arc::new(crate::runtime::runtime_state::RuntimeErrorState::for_task(
                Arc::new(owner.view(novarocks_execution_contract::TaskFailurePhase::Execution)),
            ));
            let runtime = state
                .execution_runtime()
                .cloned()
                .map(Arc::new)
                .unwrap_or_else(crate::runtime::execution_runtime::test_execution_runtime);
            Arc::new(RuntimeState::new(
                state.query_options().cloned(),
                state.cache_options().cloned(),
                None,
                Some(UniqueId::new(946801, 946802)),
                state.backend_num(),
                state.mem_tracker(),
                Some(runtime),
                control,
            ))
        }
        pub(crate) fn new(sources: usize, state: &RuntimeState) -> Self {
            let target = state
                .error_state()
                .task_identity()
                .expect("summary ingress fixture requires its original Task");
            let registry = state
                .execution_runtime()
                .expect("summary ingress fixture requires its actual execution runtime")
                .exchange_registry();
            let key = ExchangeKey {
                finst_id_hi: 946801,
                finst_id_lo: 946802,
                node_id: 31,
            };
            let layout = summary_layout();
            let factory = creator(state);
            let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
            registry
                .register_expected_chunk_schema(
                    key,
                    sources,
                    (schema.clone()).clone(),
                    schema.clone(),
                    factory.clone(),
                )
                .unwrap();
            let streams: Vec<_> = (0..sources)
                .map(|i| {
                    let source = novarocks_execution_contract::TaskIdentity::new(
                        target.query_execution_id(),
                        novarocks_types::identity::StageId::new(99).unwrap(),
                        novarocks_types::identity::TaskId::new(u32::try_from(i + 1).unwrap())
                            .unwrap(),
                        target.backend_process_id(),
                    );
                    ExchangeStreamIdentity::try_new(
                        novarocks_execution_contract::ExchangeSource::new(
                            source,
                            UniqueId::new(946803, 946804 + i as i64),
                            u32::try_from(i).unwrap(),
                        ),
                        target,
                        UniqueId::new(key.finst_id_hi, key.finst_id_lo),
                        novarocks_execution_contract::FragmentNodeId::new(key.node_id),
                    )
                    .unwrap()
                })
                .collect();
            registry.bind_unit_receiver(key, streams.clone(), state.error_state(), novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES as u64).unwrap();
            Self {
                registry,
                key,
                streams,
                layout,
                schema,
                factory,
            }
        }
        pub(crate) fn input(&self) -> SourceBoundSummaryInput {
            SourceBoundSummaryInput::new(
                self.registry
                    .get_receiver_handle(self.key, self.streams.len())
                    .unwrap(),
                self.streams
                    .iter()
                    .map(|s| s.source())
                    .collect::<Vec<_>>()
                    .into(),
                self.layout.clone(),
            )
            .unwrap()
        }
        pub(crate) fn send(
            &self,
            source: usize,
            sequence: u64,
            counts: Option<LogicalChangeCounts>,
            eos: bool,
        ) {
            let stream = self.streams[source];
            let payload = counts
                .map(|c| {
                    encode_chunks(
                        &[summary_chunk(c, &self.schema, self.factory.as_ref()).unwrap()],
                        true,
                    )
                    .unwrap()
                })
                .unwrap_or_default();
            use crate::runtime::fragment::io::exchange_unit::{
                ExchangeDemandEntry, ExchangeDemandOperation, ExchangeUnitDemand, ExchangeUnitItem,
                ExchangeUnitReceiverFrame, ExchangeUnitSequence,
            };
            let sequence = ExchangeUnitSequence::try_new(sequence).unwrap();
            let item = if counts.is_some() {
                let demand = ExchangeUnitDemand {
                    sequence,
                    wire_bytes: payload.len() as u64,
                };
                self.registry
                    .register_unit_demand(
                        self.key,
                        ExchangeDemandEntry {
                            stream,
                            operation: ExchangeDemandOperation::Register(demand),
                        },
                    )
                    .unwrap();
                ExchangeUnitItem::Unit {
                    demand,
                    end_after_unit: eos,
                }
            } else {
                assert!(eos, "an empty summary request is an explicit End");
                ExchangeUnitItem::End(sequence)
            };
            self.registry
                .receive_unit(
                    self.key,
                    ExchangeUnitReceiverFrame {
                        stream,
                        sender_count: u32::try_from(self.streams.len()).unwrap(),
                        sender_id: source as i32,
                        backend_number: 0,
                        item,
                        payload: bytes::Bytes::from(payload),
                        next_demand: None,
                    },
                )
                .unwrap();
        }
    }
    #[test]
    fn logical_change_summary_reduces_actual_contributors_once_and_zero_is_a_real_row() {
        let tracker = MemTracker::new_root("summary state");
        for empty in [true, false] {
            let state = LogicalSummaryState::new(3, tracker.clone()).unwrap();
            let mut contributors = (0..3)
                .map(|i| state.contributor(i).unwrap())
                .collect::<Vec<_>>();
            assert!(state.contributor(0).is_err());
            assert!(state.result().unwrap().is_none());
            if !empty {
                for c in &mut contributors {
                    c.accept(LogicalChangeCounts::try_new(2, 3, 4).unwrap())
                        .unwrap();
                }
            }
            contributors[2].finish().unwrap();
            contributors[0].finish().unwrap();
            assert!(state.result().unwrap().is_none());
            contributors[1].finish().unwrap();
            assert!(contributors[1].finish().is_err());
            let runtime = RuntimeState::default();
            let mut source = LogicalSummarySourceFactory::new(state.clone(), summary_layout())
                .unwrap()
                .create(1, 0);
            source.set_construction_factory(creator(&runtime));
            source.prepare().unwrap();
            let output = source.drive_for_test(&runtime).unwrap().output.unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(
                output.columns()[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                if empty { 0 } else { 6 }
            );
            assert!(source.drive_for_test(&runtime).unwrap().output.is_none());
            assert!(
                LogicalSummarySourceFactory::new(state, summary_layout())
                    .unwrap()
                    .create(1, 0)
                    .prepare()
                    .is_err()
            );
        }
    }
    #[test]
    fn logical_change_summary_missing_contributor_and_cross_driver_overflow_refuse_output() {
        let tracker = MemTracker::new_root("summary failures");
        let state = LogicalSummaryState::new(1, tracker.clone()).unwrap();
        drop(state.contributor(0).unwrap());
        assert!(state.result().is_err());
        let state = LogicalSummaryState::new(2, tracker).unwrap();
        let mut first = state.contributor(0).unwrap();
        let mut second = state.contributor(1).unwrap();
        first
            .accept(LogicalChangeCounts::try_new(i64::MAX as u64, 0, 0).unwrap())
            .unwrap();
        second.accept(LogicalChangeCounts::DELETE).unwrap();
        first.finish().unwrap();
        assert!(second.finish().is_err());
        assert!(state.result().is_err());
    }
    #[test]
    fn logical_change_summary_wait_outlives_transport_deadline_and_real_data_end_complete() {
        let owner = crate::runtime::fragment::io::exchange::test_support::Fixture::with_runtime(
            crate::exec::operators::exchange_source::test_support::runtime_with_transport_timeout(
                std::time::Duration::from_millis(10),
            ),
        );
        let runtime = owner.state(UniqueId::new(946801, 946802));
        let fixture = IngressFixture::new(1, &runtime);
        let mut input = fixture.input();
        input
            .prepare(
                fixture.factory.as_ref(),
                MemTracker::new_root("long-wait summary source index"),
            )
            .unwrap();
        let mut coverage = SummaryCoverage::new(
            input.sources().unwrap(),
            MemTracker::new_root("long-wait summary coverage"),
        )
        .unwrap();
        let errors = runtime.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&errors, &mut work, &mut conversions);
        let SummaryRead::Pending(before) = input.poll(&mut ctx).unwrap() else {
            panic!("summary input must actually wait")
        };
        assert!(before.deadline.is_none());
        assert!(!input.ready());
        assert!(
            coverage.finish().is_err(),
            "idle input cannot invent a zero summary"
        );
        std::thread::sleep(
            owner
                .runtime
                .exchange_flow_limits()
                .transport_timeout()
                .saturating_mul(20),
        );
        assert!(
            !input.ready(),
            "elapsed time cannot make an empty summary receiver ready"
        );
        let SummaryRead::Pending(after) = input.poll(&mut ctx).unwrap() else {
            panic!("summary input must still wait")
        };
        assert!(after.deadline.is_none());
        assert!(Arc::ptr_eq(&before.observable, &after.observable));
        assert_eq!(after.observable.generation(), before.observed_generation);
        assert!(runtime.error().is_none());
        fixture.send(
            0,
            1,
            Some(LogicalChangeCounts::try_new(2, 3, 4).unwrap()),
            false,
        );
        assert!(after.observable.generation() > after.observed_generation);
        assert!(input.ready());
        let SummaryRead::Summary(summary) = input.poll(&mut ctx).unwrap() else {
            panic!("actual summary DATA")
        };
        assert_eq!(summary.source, fixture.streams[0].source());
        coverage.accept(summary).unwrap();
        assert_eq!(coverage.finish().unwrap().affected_rows(), 9);
        let SummaryRead::Pending(before_end) = input.poll(&mut ctx).unwrap() else {
            panic!("summary DATA is not End")
        };
        assert!(before_end.deadline.is_none());
        assert!(!input.ready());
        fixture.send(0, 2, None, true);
        assert!(before_end.observable.generation() > before_end.observed_generation);
        assert!(input.ready());
        assert!(matches!(input.poll(&mut ctx).unwrap(), SummaryRead::End));
        assert!(runtime.error().is_none());
        assert!(
            fixture
                .registry
                .stream_close_facts(fixture.key, &fixture.streams)
                .unwrap()
                .iter()
                .all(|fact| matches!(
                    fact.disposition,
                    novarocks_execution_contract::ExchangeCloseDisposition::Consumed { .. }
                ))
        );
    }

    #[test]
    fn logical_change_summary_pending_input_preserves_original_task_failure() {
        let runtime = IngressFixture::task_state(&RuntimeState::default());
        let fixture = IngressFixture::new(1, &runtime);
        let mut input = fixture.input();
        input
            .prepare(
                fixture.factory.as_ref(),
                MemTracker::new_root("failed-task summary input"),
            )
            .unwrap();
        let errors = runtime.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&errors, &mut work, &mut conversions);
        let SummaryRead::Pending(wait) = input.poll(&mut ctx).unwrap() else {
            panic!("summary must be pending")
        };
        assert!(wait.deadline.is_none());
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new(
                "original Task failure while waiting for a logical summary",
            ),
        ));
        errors.set_error(original.clone());
        let failure = match input.poll(&mut ctx) {
            Err(error) => error,
            Ok(_) => panic!("Task failure must terminate the summary input wait"),
        };
        assert!(failure.same_occurrence(&original));
        assert!(runtime.error().unwrap().same_occurrence(&original));
    }

    #[test]
    fn logical_change_summary_sources_are_receiver_bound_and_end_is_not_a_summary_row() {
        let runtime = IngressFixture::task_state(&RuntimeState::default());
        let fixture = IngressFixture::new(2, &runtime);
        let mut input = fixture.input();
        input
            .prepare(
                fixture.factory.as_ref(),
                MemTracker::new_root("summary source index"),
            )
            .unwrap();
        let mut coverage = SummaryCoverage::new(
            input.sources().unwrap(),
            MemTracker::new_root("finish coverage"),
        )
        .unwrap();
        let errors = runtime.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&errors, &mut work, &mut conversions);
        fixture.send(
            1,
            1,
            Some(LogicalChangeCounts::try_new(2, 3, 4).unwrap()),
            true,
        );
        let SummaryRead::Summary(summary) = input.poll(&mut ctx).unwrap() else {
            panic!("actual sourced DATA")
        };
        coverage.accept(summary).unwrap();
        assert!(coverage.finish().is_err());
        assert!(matches!(
            input.poll(&mut ctx).unwrap(),
            SummaryRead::Pending(_)
        ));
        fixture.send(0, 1, Some(LogicalChangeCounts::ZERO), false);
        let SummaryRead::Summary(summary) = input.poll(&mut ctx).unwrap() else {
            panic!("zero sourced DATA")
        };
        coverage.accept(summary).unwrap();
        assert_eq!(coverage.finish().unwrap().affected_rows(), 9);
        assert!(
            matches!(input.poll(&mut ctx).unwrap(), SummaryRead::Pending(_)),
            "summary coverage alone is not EOS"
        );
        fixture.send(0, 2, None, true);
        assert!(matches!(input.poll(&mut ctx).unwrap(), SummaryRead::End));
        assert!(
            fixture
                .registry
                .stream_close_facts(fixture.key, &fixture.streams)
                .unwrap()
                .iter()
                .all(|f| matches!(
                    f.disposition,
                    novarocks_execution_contract::ExchangeCloseDisposition::Consumed { .. }
                ))
        );
    }
    #[test]
    fn logical_change_summary_exact_coverage_rejects_duplicate_foreign_missing_and_overflow() {
        let runtime = IngressFixture::task_state(&RuntimeState::default());
        let fixture = IngressFixture::new(3, &runtime);
        let expected: Arc<[_]> = fixture.streams[..2]
            .iter()
            .map(|s| s.source())
            .collect::<Vec<_>>()
            .into();
        let mut coverage = SummaryCoverage::new(
            SummarySourceIndex::new(
                &expected,
                MemTracker::new_root("coverage index"),
                fixture.factory.as_ref(),
            )
            .unwrap(),
            MemTracker::new_root("coverage failures"),
        )
        .unwrap();
        coverage
            .accept(SourceBoundSummary {
                source: fixture.streams[0].source(),
                counts: LogicalChangeCounts::try_new(i64::MAX as u64, 0, 0).unwrap(),
            })
            .unwrap();
        assert!(
            coverage
                .accept(SourceBoundSummary {
                    source: fixture.streams[0].source(),
                    counts: LogicalChangeCounts::ZERO
                })
                .is_err()
        );
        assert!(
            coverage
                .accept(SourceBoundSummary {
                    source: fixture.streams[2].source(),
                    counts: LogicalChangeCounts::ZERO
                })
                .is_err()
        );
        assert!(
            coverage
                .accept(SourceBoundSummary {
                    source: fixture.streams[1].source(),
                    counts: LogicalChangeCounts::DELETE
                })
                .is_err()
        );
        assert!(coverage.finish().is_err());
    }
    #[test]
    fn logical_change_summary_source_index_accepts_unordered_identity_sets_and_keeps_query_governance()
     {
        let runtime = IngressFixture::task_state(&RuntimeState::default());
        let fixture = IngressFixture::new(64, &runtime);
        let mut sources = fixture
            .streams
            .iter()
            .map(|s| s.source())
            .collect::<Vec<_>>();
        sources.reverse();
        let tracker = MemTracker::new_root("source index ownership");
        let index =
            SummarySourceIndex::new(&sources, tracker.clone(), fixture.factory.as_ref()).unwrap();
        assert!(tracker.current() > 0);
        for source in &sources {
            assert!(index.ordinal(source).is_some());
        }
        let mut coverage =
            SummaryCoverage::new(index.clone(), MemTracker::new_root("source coverage")).unwrap();
        for source in &sources {
            coverage
                .accept(SourceBoundSummary {
                    source: *source,
                    counts: LogicalChangeCounts::INSERT,
                })
                .unwrap();
        }
        assert_eq!(coverage.finish().unwrap().affected_rows(), 64);
        sources.push(sources[0]);
        assert!(
            SummarySourceIndex::new(&sources, tracker.clone(), fixture.factory.as_ref()).is_err()
        );
        drop(coverage);
        drop(index);
        assert_eq!(tracker.current(), 0);
        let limited = MemTracker::new_root("source index original query limit");
        limited.install_limit_once(1).unwrap();
        assert!(
            SummarySourceIndex::new(&sources, limited.clone(), fixture.factory.as_ref()).is_err()
        );
        assert_eq!(limited.current(), 0);
    }
}
