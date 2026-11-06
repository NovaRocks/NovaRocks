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
            driver_step::DriverStep,
            operator::{
                ConstructionFailure, InputConstructionFactory, Operator, OperatorLifecycle,
            },
            operator_factory::OperatorFactory,
            shape::{Source, SourceKernel},
            step::{NextStep, StepResult, WaitFact, WaitReason},
        },
    },
    runtime::{
        exchange::{ExchangeReceiverPin, SourcedExchangePopResult},
        mem_tracker::{MemTracker, TrackedBytes},
        observable::Observable,
        task::{ExecutionFailure, ExecutionReason, ExecutionResult},
    },
};
use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, Int64Array};
use novarocks_execution_contract::ExchangeSource;
use novarocks_local_program::StaticLayout;
use novarocks_result_contract::{
    DeleteVerificationCounts, LogicalChangeCounts, SummaryKind, SummaryMergeError, SummaryValue,
};
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub(crate) enum SummaryFailure {
    Author(SummaryKind),
    Contributors(SummaryKind),
    DuplicateContributor(SummaryKind),
    MissingContributor(SummaryKind),
    OutputClaimed(SummaryKind),
    Count {
        kind: SummaryKind,
        cause: SummaryMergeError,
    },
    Schema(SummaryKind),
    Source(SummaryKind),
    DuplicateSource(SummaryKind),
    MissingSource(SummaryKind),
    WithdrawnWriter,
}
impl std::fmt::Display for SummaryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (kind, message) = match self {
            Self::Author(kind) => (
                *kind,
                "requires its exact author to declare units once before driver creation",
            ),
            Self::Contributors(kind) => (*kind, "requires the exact nonempty author unit set"),
            Self::DuplicateContributor(kind) => (*kind, "unit was claimed or finished twice"),
            Self::MissingContributor(kind) => (*kind, "unit exited before strict input exhaustion"),
            Self::OutputClaimed(kind) => (*kind, "has more than one output owner"),
            Self::Schema(kind) => (
                *kind,
                "requires one row under its exact ordered non-null field contract",
            ),
            Self::Source(kind) => (*kind, "source differs from the frozen receiver source set"),
            Self::DuplicateSource(kind) => (*kind, "source contributed more than once"),
            Self::MissingSource(kind) => (*kind, "normal EOS has incomplete source coverage"),
            Self::Count { kind, cause } => return write!(f, "{kind} summary: {cause}"),
            Self::WithdrawnWriter => {
                return f.write_str("writer-results input withdrew before normal exhaustion");
            }
        };
        write!(f, "{kind} summary {message}")
    }
}
impl std::error::Error for SummaryFailure {}
impl novarocks_failure_contract::FailureProjection for SummaryFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self {
            Self::Count {
                cause: SummaryMergeError::WriteCounts(_) | SummaryMergeError::DeleteVerification(_),
                ..
            } => ExecutionReason::SummaryCount,
            _ => ExecutionReason::SummaryContract,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
pub(crate) fn summary_failure(cause: SummaryFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(cause)
}
fn add(left: SummaryValue, right: SummaryValue) -> ExecutionResult<SummaryValue> {
    left.checked_merge(right).map_err(|cause| {
        summary_failure(SummaryFailure::Count {
            kind: left.kind(),
            cause,
        })
    })
}
struct SummaryAuthorEntry {
    author: novarocks_local_program::StaticSummaryAuthor,
    claimed: bool,
    state: Option<TaskSummaryState>,
}
/// Task-local preparation obligation. It is checked by `seal`, with no Drop
/// panic or replacement of an earlier construction failure. Its frozen author
/// facts grant no memory capacity; the actual author supplies the Task tracker.
pub(crate) struct SummaryAuthorAssemblyObligation {
    entries: novarocks_result_contract::PerKind<SummaryAuthorEntry>,
}
impl SummaryAuthorAssemblyObligation {
    pub(crate) fn new(
        outputs: &[novarocks_local_program::StaticSummaryBranch],
    ) -> ExecutionResult<Self> {
        let mut entries = novarocks_result_contract::PerKind::empty();
        for output in outputs {
            if entries
                .insert(
                    output.kind(),
                    SummaryAuthorEntry {
                        author: output.author(),
                        claimed: false,
                        state: None,
                    },
                )
                .is_some()
            {
                return Err(summary_failure(SummaryFailure::Author(output.kind())));
            }
        }
        Ok(Self { entries })
    }
    pub(crate) fn claim(
        &mut self,
        kind: SummaryKind,
        author: novarocks_local_program::StaticSummaryAuthor,
    ) -> ExecutionResult<SummaryAuthorObligation<'_>> {
        let entry = self
            .entries
            .get_mut(kind)
            .ok_or_else(|| summary_failure(SummaryFailure::Author(kind)))?;
        if entry.author != author || entry.claimed {
            return Err(summary_failure(SummaryFailure::Author(kind)));
        }
        entry.claimed = true;
        Ok(SummaryAuthorObligation {
            kind,
            slot: &mut entry.state,
        })
    }
    pub(crate) fn declares(
        &self,
        kind: SummaryKind,
        author: novarocks_local_program::StaticSummaryAuthor,
    ) -> bool {
        self.entries
            .get(kind)
            .is_some_and(|entry| entry.author == author)
    }
    pub(crate) fn seal(
        mut self,
    ) -> ExecutionResult<novarocks_result_contract::PerKind<TaskSummaryState>> {
        let mut states = novarocks_result_contract::PerKind::empty();
        for kind in SummaryKind::ALL {
            if let Some(entry) = self.entries.get_mut(kind) {
                let state = entry
                    .state
                    .take()
                    .ok_or_else(|| summary_failure(SummaryFailure::Author(kind)))?;
                states.insert(kind, state);
            }
        }
        Ok(states)
    }
}
/// The selected factory must consume this preparation obligation by declaring
/// its structural unit count. No Drop check; the original assembly's seal is
/// the check, so a failed factory retains its original cause.
pub(crate) struct SummaryAuthorObligation<'a> {
    kind: SummaryKind,
    slot: &'a mut Option<TaskSummaryState>,
}
impl SummaryAuthorObligation<'_> {
    pub(crate) fn declare_units(
        self,
        units: usize,
        tracker: Arc<MemTracker>,
    ) -> ExecutionResult<TaskSummaryState> {
        let tracker = MemTracker::new_child(
            match self.kind {
                SummaryKind::WriteCounts => "WriteCountsSummary",
                SummaryKind::DeleteVerification => "DeleteVerificationSummary",
            },
            &tracker,
        );
        let state = TaskSummaryState::new(self.kind, units, tracker)?;
        *self.slot = Some(state.clone());
        Ok(state)
    }
}
struct Reduction {
    // 0 = unclaimed, 1 = live, 2 = normally exhausted. No per-row ledger.
    contributors: Vec<u8>,
    remaining: usize,
    value: SummaryValue,
    failure: Option<ExecutionFailure>,
    output_claimed: bool,
}
#[derive(Clone)]
pub(crate) struct TaskSummaryState {
    kind: SummaryKind,
    inner: Arc<Mutex<Reduction>>,
    observable: Arc<Observable>,
    _accounting: Arc<TrackedBytes>,
}
impl TaskSummaryState {
    pub(crate) fn new(
        kind: SummaryKind,
        contributors: usize,
        tracker: Arc<MemTracker>,
    ) -> ExecutionResult<Self> {
        if contributors == 0 {
            return Err(summary_failure(SummaryFailure::Contributors(kind)));
        }
        let bytes = contributors
            .checked_add(std::mem::size_of::<Reduction>())
            .ok_or(ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            ))?;
        let accounting =
            TrackedBytes::try_new(bytes, tracker).map_err(ExecutionFailure::from_domain)?;
        Ok(Self {
            kind,
            inner: Arc::new(Mutex::new(Reduction {
                contributors: vec![0; contributors],
                remaining: contributors,
                value: SummaryValue::zero(kind),
                failure: None,
                output_claimed: false,
            })),
            observable: Arc::new(Observable::new()),
            _accounting: Arc::new(accounting),
        })
    }
    pub(crate) const fn kind(&self) -> SummaryKind {
        self.kind
    }
    pub(crate) fn validate_units(&self, units: usize) -> ExecutionResult<()> {
        if self.inner.lock().expect("summary lock").contributors.len() != units {
            return Err(summary_failure(SummaryFailure::Contributors(self.kind)));
        }
        Ok(())
    }
    pub(crate) fn contributor(&self, ordinal: usize) -> ExecutionResult<SummaryUnitObligation> {
        let mut state = self.inner.lock().expect("logical summary lock");
        let slot = state
            .contributors
            .get_mut(ordinal)
            .ok_or_else(|| summary_failure(SummaryFailure::Contributors(self.kind)))?;
        if *slot != 0 {
            return Err(summary_failure(SummaryFailure::DuplicateContributor(
                self.kind,
            )));
        }
        *slot = 1;
        Ok(SummaryUnitObligation {
            state: self.clone(),
            ordinal,
            value: SummaryValue::zero(self.kind),
            finished: false,
        })
    }
    fn validate_claimed_units(&self) -> ExecutionResult<()> {
        if self
            .inner
            .lock()
            .expect("summary lock")
            .contributors
            .contains(&0)
        {
            return Err(summary_failure(SummaryFailure::MissingContributor(
                self.kind,
            )));
        }
        Ok(())
    }
    fn claim_output(&self) -> ExecutionResult<()> {
        let mut state = self.inner.lock().expect("logical summary lock");
        if state.output_claimed {
            return Err(summary_failure(SummaryFailure::OutputClaimed(self.kind)));
        }
        state.output_claimed = true;
        Ok(())
    }
    fn result(&self) -> ExecutionResult<Option<SummaryValue>> {
        let state = self.inner.lock().expect("logical summary lock");
        if let Some(error) = &state.failure {
            return Err(error.clone());
        }
        Ok((state.remaining == 0).then_some(state.value))
    }
}
/// Must be finished once after this unit's complete input reaches strict EOS.
/// Drop is checked against the same Task state and prevents summary publication.
pub(crate) struct SummaryUnitObligation {
    state: TaskSummaryState,
    ordinal: usize,
    value: SummaryValue,
    finished: bool,
}
impl SummaryUnitObligation {
    pub(crate) fn accept(&mut self, value: SummaryValue) -> ExecutionResult<()> {
        if self.finished {
            return Err(summary_failure(SummaryFailure::DuplicateContributor(
                self.state.kind(),
            )));
        }
        self.value = add(self.value, value)?;
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> ExecutionResult<()> {
        if self.finished {
            return Err(summary_failure(SummaryFailure::DuplicateContributor(
                self.state.kind(),
            )));
        }
        let notify = self.state.observable.defer_notify();
        let mut state = self.state.inner.lock().expect("logical summary lock");
        match add(state.value, self.value) {
            Ok(counts) => state.value = counts,
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
impl Drop for SummaryUnitObligation {
    fn drop(&mut self) {
        if !self.finished {
            let notify = self.state.observable.defer_notify();
            self.state
                .inner
                .lock()
                .expect("logical summary lock")
                .failure
                .get_or_insert_with(|| {
                    summary_failure(SummaryFailure::MissingContributor(self.state.kind()))
                });
            notify.arm();
        }
    }
}
pub(crate) struct SummarySourceFactory {
    state: TaskSummaryState,
    layout: StaticLayout,
}
impl SummarySourceFactory {
    pub(crate) fn new(state: TaskSummaryState, layout: StaticLayout) -> ExecutionResult<Self> {
        validate_layout(state.kind(), &layout)?;
        Ok(Self { state, layout })
    }
}
fn validate_layout(kind: SummaryKind, layout: &StaticLayout) -> ExecutionResult<()> {
    novarocks_local_program::validate_summary_layout(kind, layout)
        .map_err(|_| summary_failure(SummaryFailure::Schema(kind)))
}
impl OperatorFactory for SummarySourceFactory {
    fn name(&self) -> &str {
        "TypedSummary"
    }
    fn is_source(&self) -> bool {
        true
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let failure = if dop != 1 || driver_id != 0 {
            Some(summary_failure(SummaryFailure::OutputClaimed(
                self.state.kind(),
            )))
        } else {
            self.state
                .claim_output()
                .and_then(|()| self.state.validate_claimed_units())
                .err()
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
    state: TaskSummaryState,
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
        "TypedSummary"
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
    fn poll_output(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
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
                    "kind={} emitted={} generation={} reduction={reduction}",
                    self.state.kind(),
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
    value: SummaryValue,
    schema: &ChunkSchemaRef,
    factory: &dyn InputConstructionFactory,
) -> ExecutionResult<Chunk> {
    use novarocks_functions::selected_copy;
    use novarocks_type_contract::ValueSchemaCreation;
    let owner = factory.begin_typed_construction(schema)?;
    owner.request(
        Layout::array::<ArrayRef>(value.kind().fields().len()).map_err(|_| {
            ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?,
    )?;
    let mut columns = Vec::with_capacity(value.kind().fields().len());
    let (wire, fingerprint) = match value {
        SummaryValue::WriteCounts(counts) => (counts.into_wire(), None),
        SummaryValue::DeleteVerification(counts) => {
            let (wire, bytes) = counts.into_wire();
            (wire, Some(bytes))
        }
    };
    for value in wire {
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
    if let Some(bytes) = fingerprint {
        owner.request(Layout::new::<[u8; 16]>())?;
        let mut request = |layout| owner.request(layout);
        let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
        selected_copy::preflight_flat_array_structure_construction(
            schema.slots()[3].typed_binding().layout(),
            false,
            &mut creation,
            &mut |boundary| factory.observe_input_work(usize::from(!boundary)),
        )
        .map_err(copy_failure)?;
        columns.push(Arc::new(FixedSizeBinaryArray::new(
            16,
            arrow::buffer::Buffer::from_vec(bytes.to_vec()),
            None,
        )) as ArrayRef);
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
    value: SummaryValue,
}
pub(crate) enum SummaryRead {
    Summary(SourceBoundSummary),
    Pending(WaitFact),
    End,
}
/// One immutable index shared by ingress validation and finish coverage.
/// The descriptor's original order does not become a semantic source ordinal.
pub(crate) struct SummarySourceIndex {
    kind: SummaryKind,
    sorted: Vec<ExchangeSource>,
    _accounting: TrackedBytes,
}
impl SummarySourceIndex {
    fn new(
        kind: SummaryKind,
        expected: &[ExchangeSource],
        tracker: Arc<MemTracker>,
        factory: &dyn InputConstructionFactory,
    ) -> ExecutionResult<Arc<Self>> {
        if expected.is_empty() {
            return Err(summary_failure(SummaryFailure::Source(kind)));
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
                return Err(summary_failure(SummaryFailure::Source(kind)));
            }
        }
        Ok(Arc::new(Self {
            kind,
            sorted,
            _accounting: accounting,
        }))
    }
    fn ordinal(&self, source: &ExchangeSource) -> Option<usize> {
        self.sorted.binary_search(source).ok()
    }
}
pub(crate) struct SourceBoundSummaryInput {
    kind: SummaryKind,
    receiver: ExchangeReceiverPin,
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
        kind: SummaryKind,
        receiver: ExchangeReceiverPin,
        expected: Arc<[ExchangeSource]>,
        layout: StaticLayout,
    ) -> ExecutionResult<Self> {
        validate_layout(kind, &layout)?;
        if expected.is_empty() {
            return Err(summary_failure(SummaryFailure::Source(kind)));
        }
        Ok(Self {
            kind,
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
        self.index = Some(SummarySourceIndex::new(
            self.kind,
            &self.expected,
            tracker,
            factory,
        )?);
        Ok(())
    }
    pub(crate) const fn kind(&self) -> SummaryKind {
        self.kind
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
    pub(crate) fn poll(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<SummaryRead> {
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
                    .ok_or_else(|| summary_failure(SummaryFailure::Source(self.kind)))?;
                let schema = self
                    .schema
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?;
                let value = decode_summary_value(self.kind, &chunk, schema)?;
                #[cfg(novarocks_dev_controls)]
                if let Some(scope) = &self.diagnostics {
                    scope.emit("summary_input_handoff", || {
                        format!(
                            "source={source:?} value={value:?} expected={:?}",
                            self.expected
                        )
                    });
                }
                Ok(SummaryRead::Summary(SourceBoundSummary { source, value }))
            }
            Some(SourcedExchangePopResult::Finished(_)) => {
                self.receiver
                    .complete_input_consumption(self.expected.len())
                    .map_err(|e| e.into_execution())?;
                self.ended = true;
                #[cfg(novarocks_dev_controls)]
                if let Some(scope) = &self.diagnostics {
                    scope.emit("summary_input_consumed", || {
                        format!("kind={} expected={:?}", self.kind, self.expected)
                    });
                }
                Ok(SummaryRead::End)
            }
            None => {
                #[cfg(novarocks_dev_controls)]
                if let Some(scope) = &self.diagnostics {
                    scope.emit("summary_input_wait", || {
                        format!(
                            "kind={} generation={generation} expected={:?}",
                            self.kind, self.expected
                        )
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
fn decode_summary_value(
    kind: SummaryKind,
    chunk: &Chunk,
    schema: &ChunkSchemaRef,
) -> ExecutionResult<SummaryValue> {
    if chunk.len() != 1 || chunk.chunk_schema() != schema.as_ref() {
        return Err(summary_failure(SummaryFailure::Schema(kind)));
    }
    let mut wire = [0; 3];
    for (ordinal, value) in wire.iter_mut().enumerate() {
        let a = chunk.columns()[ordinal]
            .as_any()
            .downcast_ref::<Int64Array>()
            .filter(|a| a.null_count() == 0)
            .ok_or_else(|| summary_failure(SummaryFailure::Schema(kind)))?;
        *value = a.value(0);
    }
    let value = match kind {
        SummaryKind::WriteCounts => SummaryValue::WriteCounts(
            LogicalChangeCounts::try_from_wire(wire[0], wire[1], wire[2]).map_err(|error| {
                summary_failure(SummaryFailure::Count {
                    kind,
                    cause: SummaryMergeError::WriteCounts(error),
                })
            })?,
        ),
        SummaryKind::DeleteVerification => {
            let fingerprint = chunk.columns()[3]
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .filter(|array| array.value_length() == 16 && array.null_count() == 0)
                .ok_or_else(|| summary_failure(SummaryFailure::Schema(kind)))?;
            let fingerprint: [u8; 16] = fingerprint
                .value(0)
                .try_into()
                .map_err(|_| summary_failure(SummaryFailure::Schema(kind)))?;
            SummaryValue::DeleteVerification(
                DeleteVerificationCounts::try_from_wire(wire[0], wire[1], wire[2], fingerprint)
                    .map_err(|error| {
                        summary_failure(SummaryFailure::Count {
                            kind,
                            cause: SummaryMergeError::DeleteVerification(error),
                        })
                    })?,
            )
        }
    };
    Ok(value)
}
/// Finish-owned coverage, independent of writer target ordinals and profiles.
pub(crate) struct SummaryCoverage {
    expected: Arc<SummarySourceIndex>,
    remaining: usize,
    seen: Vec<bool>,
    value: SummaryValue,
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
            value: SummaryValue::zero(expected.kind),
            expected,
            _accounting: accounting,
        })
    }
    pub(crate) fn accept(&mut self, summary: SourceBoundSummary) -> ExecutionResult<()> {
        let ordinal = self
            .expected
            .ordinal(&summary.source)
            .ok_or_else(|| summary_failure(SummaryFailure::Source(self.expected.kind)))?;
        if self.seen[ordinal] {
            return Err(summary_failure(SummaryFailure::DuplicateSource(
                self.expected.kind,
            )));
        }
        let next = add(self.value, summary.value)?;
        self.value = next;
        self.seen[ordinal] = true;
        self.remaining -= 1;
        Ok(())
    }
    pub(crate) fn finish(&self) -> ExecutionResult<SummaryValue> {
        if self.remaining != 0 {
            return Err(summary_failure(SummaryFailure::MissingSource(
                self.expected.kind,
            )));
        }
        Ok(self.value)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::exec::pipeline::driver_step::TurnWork;
    use crate::runtime::{
        exchange::{ExchangeKey, ExecutionExchangeRegistry, online_unit},
        runtime_state::RuntimeState,
    };
    use arrow::datatypes::DataType;
    use novarocks_execution_contract::ExchangeStreamIdentity;
    use novarocks_types::{SlotId, UniqueId};
    pub(crate) fn summary_layout() -> StaticLayout {
        summary_layout_for(SummaryKind::WriteCounts)
    }
    pub(crate) fn summary_layout_for(kind: SummaryKind) -> StaticLayout {
        let types = kind
            .fields()
            .iter()
            .map(|field| match field.wire_type() {
                novarocks_result_contract::SummaryWireType::Int64 => DataType::Int64,
                novarocks_result_contract::SummaryWireType::FixedSizeBinary(width) => {
                    DataType::FixedSizeBinary(i32::from(width))
                }
            })
            .collect::<Vec<_>>();
        crate::exec::chunk::typed_fixture::layout(
            Arc::new(arrow::datatypes::Schema::new(
                kind.fields()
                    .iter()
                    .zip(&types)
                    .map(|(field, ty)| {
                        arrow::datatypes::Field::new(field.name(), ty.clone(), false)
                    })
                    .collect::<Vec<_>>(),
            )),
            &(0..types.len())
                .map(|i| SlotId(801 + i as u32))
                .collect::<Vec<_>>(),
            &types
                .into_iter()
                .map(|ty| novarocks_type_contract::FunctionValueType::new(ty, false))
                .collect::<Vec<_>>(),
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
        kind: SummaryKind,
        pub(crate) registry: Arc<ExecutionExchangeRegistry>,
        pub(crate) key: ExchangeKey,
        pub(crate) streams: Vec<ExchangeStreamIdentity>,
        pub(crate) layout: StaticLayout,
        pub(crate) schema: ChunkSchemaRef,
        pub(crate) factory: Arc<dyn InputConstructionFactory>,
        control: crate::exec::operators::compiled_expression::RuntimeKernelControl,
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
            Self::new_kind(sources, state, SummaryKind::WriteCounts)
        }
        pub(crate) fn new_kind(sources: usize, state: &RuntimeState, kind: SummaryKind) -> Self {
            let target = state
                .error_state()
                .task_identity()
                .expect("summary ingress fixture requires its original Task");
            let registry = state
                .execution_runtime()
                .expect("summary ingress fixture requires its actual execution runtime")
                .exchange_registry();
            let key = ExchangeKey {
                task_key_hi: 946801,
                task_key_lo: 946802,
                node_id: match kind {
                    SummaryKind::WriteCounts => 31,
                    SummaryKind::DeleteVerification => 32,
                },
            };
            let layout = summary_layout_for(kind);
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
                        UniqueId::new(key.task_key_hi, key.task_key_lo),
                        novarocks_execution_contract::FragmentNodeId::new(key.node_id),
                    )
                    .unwrap()
                })
                .collect();
            registry.bind_unit_receiver(key, streams.clone(), state.error_state(), novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES as u64).unwrap();
            Self {
                kind,
                registry,
                key,
                streams,
                layout,
                schema,
                factory,
                control: crate::exec::operators::compiled_expression::RuntimeKernelControl::new(
                    state.error_state(),
                ),
            }
        }
        pub(crate) fn input(&self) -> SourceBoundSummaryInput {
            SourceBoundSummaryInput::new(
                self.kind,
                self.registry
                    .get_receiver_pin(self.key, self.streams.len())
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
            self.send_value(source, sequence, counts.map(SummaryValue::WriteCounts), eos);
        }
        pub(crate) fn send_value(
            &self,
            source: usize,
            sequence: u64,
            value: Option<SummaryValue>,
            eos: bool,
        ) {
            assert!(value.is_none_or(|value| value.kind() == self.kind));
            let stream = self.streams[source];
            let payload = value
                .map(|c| {
                    let recipe =
                        online_unit::prepare_recipe(&self.schema, &[], self.factory.as_ref())
                            .unwrap();
                    let chunk = summary_chunk(c, &self.schema, self.factory.as_ref()).unwrap();
                    online_unit::encode(&recipe, &chunk, self.factory.as_ref(), &self.control)
                        .unwrap()
                })
                .unwrap_or_default();
            use crate::runtime::task::io::exchange_unit::{
                ExchangeDemandEntry, ExchangeDemandOperation, ExchangeUnitDemand, ExchangeUnitItem,
                ExchangeUnitReceiverFrame, ExchangeUnitSequence,
            };
            let sequence = ExchangeUnitSequence::try_new(sequence).unwrap();
            let item = if value.is_some() {
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
    fn declared_output(
        kind: SummaryKind,
        author: novarocks_local_program::StaticSummaryAuthor,
    ) -> novarocks_local_program::StaticSummaryBranch {
        use novarocks_local_program::*;
        let layout = summary_layout_for(kind);
        let branch = StaticStreamBranch::try_new(
            31,
            DataStreamPartitionType::Unpartitioned,
            vec![],
            layout.slots().to_vec(),
            None,
            StaticStreamContract {
                role: ProgramOutputRole::Summary(kind),
                delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain,
                routing: StaticStreamRouting::Ordinary,
            },
        )
        .unwrap();
        StaticSummaryBranch::try_new(kind, author, branch, layout).unwrap()
    }
    #[test]
    fn node_author_declares_one_unit_for_two_probe_groups_and_keeps_fingerprint_wrapping() {
        use novarocks_local_program::{ProgramNodeId, StaticSummaryAuthor};
        let kind = SummaryKind::DeleteVerification;
        let author = StaticSummaryAuthor::Node(ProgramNodeId::new(0));
        let mut assembly =
            SummaryAuthorAssemblyObligation::new(&[declared_output(kind, author)]).unwrap();
        assert!(
            assembly
                .claim(kind, StaticSummaryAuthor::RouterSink)
                .is_err()
        );
        let state = assembly
            .claim(kind, author)
            .unwrap()
            .declare_units(1, MemTracker::new_root("node summary units"))
            .unwrap();
        assert!(assembly.claim(kind, author).is_err());
        let states = assembly.seal().unwrap();
        assert!(states.write_counts.is_none());
        let mut unit = state.contributor(0).unwrap();
        // Two independent probe groups contribute through the original author.
        // Its one obligation remains live until both complete their full input.
        unit.accept(SummaryValue::DeleteVerification(
            DeleteVerificationCounts::try_new(3, 2, 1, u128::MAX).unwrap(),
        ))
        .unwrap();
        assert!(state.result().unwrap().is_none());
        unit.accept(SummaryValue::DeleteVerification(
            DeleteVerificationCounts::try_new(4, 3, 2, 2).unwrap(),
        ))
        .unwrap();
        assert!(state.result().unwrap().is_none());
        unit.finish().unwrap();
        assert_eq!(
            state.result().unwrap(),
            Some(SummaryValue::DeleteVerification(
                DeleteVerificationCounts::try_new(7, 5, 3, 1).unwrap()
            ))
        );
    }
    #[test]
    fn every_declared_author_and_unit_must_be_handed_off_before_source_preparation() {
        use novarocks_local_program::StaticSummaryAuthor;
        for kind in SummaryKind::ALL {
            let output = declared_output(kind, StaticSummaryAuthor::RouterSink);
            assert!(
                SummaryAuthorAssemblyObligation::new(std::slice::from_ref(&output))
                    .unwrap()
                    .seal()
                    .is_err()
            );
            let mut assembly = SummaryAuthorAssemblyObligation::new(&[output]).unwrap();
            let state = assembly
                .claim(kind, StaticSummaryAuthor::RouterSink)
                .unwrap()
                .declare_units(2, MemTracker::new_root("exact summary handoff"))
                .unwrap();
            assembly.seal().unwrap();
            let _one = state.contributor(0).unwrap();
            assert!(
                SummarySourceFactory::new(state, summary_layout_for(kind))
                    .unwrap()
                    .create(1, 0)
                    .prepare()
                    .is_err(),
                "an unclaimed unit cannot leave a running Source waiting forever"
            );
        }
    }
    #[test]
    fn verification_zero_input_emits_one_exact_raw_fingerprint_row_and_missing_unit_refuses() {
        let kind = SummaryKind::DeleteVerification;
        let state =
            TaskSummaryState::new(kind, 1, MemTracker::new_root("verification zero")).unwrap();
        state.contributor(0).unwrap().finish().unwrap();
        let runtime = RuntimeState::default();
        let mut source = SummarySourceFactory::new(state, summary_layout_for(kind))
            .unwrap()
            .create(1, 0);
        source.set_construction_factory(creator(&runtime));
        source.prepare().unwrap();
        let output = source.drive_for_test(&runtime).unwrap().output.unwrap();
        assert_eq!(output.len(), 1);
        for column in &output.columns()[..3] {
            assert_eq!(
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                0
            );
        }
        assert_eq!(
            output.columns()[3]
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            &[0; 16]
        );
        assert!(source.drive_for_test(&runtime).unwrap().output.is_none());
        let missing =
            TaskSummaryState::new(kind, 1, MemTracker::new_root("verification missing")).unwrap();
        drop(missing.contributor(0).unwrap());
        let error = missing.result().unwrap_err();
        assert!(error.to_string().contains("delete-verification"));
    }
    #[test]
    fn per_kind_coverage_refuses_duplicate_foreign_and_missing_sources_independently() {
        for kind in SummaryKind::ALL {
            let runtime = IngressFixture::task_state(&RuntimeState::default());
            let fixture = IngressFixture::new_kind(3, &runtime, kind);
            let sources = fixture.streams[..2]
                .iter()
                .map(|stream| stream.source())
                .collect::<Vec<_>>();
            let expected = SummarySourceIndex::new(
                kind,
                &sources,
                MemTracker::new_root("typed coverage index"),
                fixture.factory.as_ref(),
            )
            .unwrap();
            let mut coverage =
                SummaryCoverage::new(expected, MemTracker::new_root("typed coverage")).unwrap();
            coverage
                .accept(SourceBoundSummary {
                    source: sources[0],
                    value: SummaryValue::zero(kind),
                })
                .unwrap();
            assert!(
                coverage
                    .accept(SourceBoundSummary {
                        source: sources[0],
                        value: SummaryValue::zero(kind)
                    })
                    .is_err()
            );
            assert!(
                coverage
                    .accept(SourceBoundSummary {
                        source: fixture.streams[2].source(),
                        value: SummaryValue::zero(kind)
                    })
                    .is_err()
            );
            assert!(
                coverage
                    .finish()
                    .unwrap_err()
                    .to_string()
                    .contains(&kind.to_string())
            );
            coverage
                .accept(SourceBoundSummary {
                    source: sources[1],
                    value: SummaryValue::zero(kind),
                })
                .unwrap();
            assert_eq!(coverage.finish().unwrap(), SummaryValue::zero(kind));
        }
    }
    #[test]
    fn summary_wire_negative_fields_and_each_verification_overflow_are_typed_refusals() {
        let runtime = RuntimeState::default();
        let factory = creator(&runtime);
        for kind in SummaryKind::ALL {
            let schema = factory
                .create_compiled_chunk_schema(&summary_layout_for(kind))
                .unwrap();
            let zero = summary_chunk(SummaryValue::zero(kind), &schema, factory.as_ref()).unwrap();
            for ordinal in 0..3 {
                let mut columns = zero.columns().to_vec();
                columns[ordinal] = Arc::new(Int64Array::from(vec![-1]));
                let corrupt = Chunk::try_new_with_columns(schema.clone(), columns).unwrap();
                assert!(
                    decode_summary_value(kind, &corrupt, &schema)
                        .unwrap_err()
                        .to_string()
                        .contains(&kind.to_string())
                );
            }
        }
        for ordinal in 0..3 {
            let mut large = [0; 3];
            large[ordinal] = i64::MAX as u64;
            let mut delta = [0; 3];
            delta[ordinal] = 1;
            let state = TaskSummaryState::new(
                SummaryKind::DeleteVerification,
                2,
                MemTracker::new_root("verification checked fields"),
            )
            .unwrap();
            let mut first = state.contributor(0).unwrap();
            let mut second = state.contributor(1).unwrap();
            first
                .accept(SummaryValue::DeleteVerification(
                    DeleteVerificationCounts::try_new(large[0], large[1], large[2], 0).unwrap(),
                ))
                .unwrap();
            second
                .accept(SummaryValue::DeleteVerification(
                    DeleteVerificationCounts::try_new(delta[0], delta[1], delta[2], 0).unwrap(),
                ))
                .unwrap();
            first.finish().unwrap();
            assert!(
                second
                    .finish()
                    .unwrap_err()
                    .to_string()
                    .contains("delete-verification")
            );
            assert!(state.result().is_err());
        }
    }
    #[test]
    fn verification_author_unit_patterns_reduce_two_original_sources_without_using_pipeline_width()
    {
        use novarocks_local_program::{ProgramNodeId, StaticSummaryAuthor};
        for per_driver in [false, true] {
            let runtime = IngressFixture::task_state(&RuntimeState::default());
            let ingress = IngressFixture::new_kind(2, &runtime, SummaryKind::DeleteVerification);
            let mut input = ingress.input();
            input
                .prepare(
                    ingress.factory.as_ref(),
                    MemTracker::new_root("verification input"),
                )
                .unwrap();
            let mut coverage = SummaryCoverage::new(
                input.sources().unwrap(),
                MemTracker::new_root("verification coverage"),
            )
            .unwrap();
            for source_ordinal in 0..2 {
                let kind = SummaryKind::DeleteVerification;
                let author = StaticSummaryAuthor::Node(ProgramNodeId::new(0));
                let mut assembly =
                    SummaryAuthorAssemblyObligation::new(&[declared_output(kind, author)]).unwrap();
                // This test author's three input groups may share one unit or
                // each own a unit. Neither case reads a pipeline profile.
                let units = if per_driver { 3 } else { 1 };
                let state = assembly
                    .claim(kind, author)
                    .unwrap()
                    .declare_units(units, MemTracker::new_root("original author Task"))
                    .unwrap();
                assembly.seal().unwrap();
                let mut contributors = (0..units)
                    .map(|ordinal| state.contributor(ordinal).unwrap())
                    .collect::<Vec<_>>();
                let mut source = SummarySourceFactory::new(state, summary_layout_for(kind))
                    .unwrap()
                    .create(1, 0);
                source.set_construction_factory(creator(&runtime));
                source.prepare().unwrap();
                for group in 0..3 {
                    let unit = if per_driver { group } else { 0 };
                    contributors[unit]
                        .accept(SummaryValue::DeleteVerification(
                            DeleteVerificationCounts::try_new(2, 1, 1, source_ordinal as u128 + 1)
                                .unwrap(),
                        ))
                        .unwrap();
                    if per_driver {
                        contributors[unit].finish().unwrap();
                    }
                    if group != 2 {
                        assert!(
                            source.drive_for_test(&runtime).unwrap().output.is_none(),
                            "a partial set of group EOS cannot publish"
                        );
                    }
                }
                if !per_driver {
                    contributors[0].finish().unwrap();
                }
                let output = source.drive_for_test(&runtime).unwrap().output.unwrap();
                let value =
                    decode_summary_value(kind, &output, &output.chunk_schema_ref()).unwrap();
                ingress.send_value(source_ordinal, 1, Some(value), true);
                assert!(source.drive_for_test(&runtime).unwrap().output.is_none());
            }
            let errors = runtime.error_state();
            let mut work = TurnWork::new(std::num::NonZeroU64::new(128).unwrap(), 128);
            let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
            for _ in 0..2 {
                work.begin_turn(128).unwrap();
                let SummaryRead::Summary(value) = input
                    .poll(&mut DriverStep::new(&errors, &mut work, &mut conversions))
                    .unwrap()
                else {
                    panic!("one row per original source")
                };
                coverage.accept(value).unwrap();
            }
            work.begin_turn(128).unwrap();
            assert!(matches!(
                input
                    .poll(&mut DriverStep::new(&errors, &mut work, &mut conversions))
                    .unwrap(),
                SummaryRead::End
            ));
            assert_eq!(
                coverage.finish().unwrap(),
                SummaryValue::DeleteVerification(
                    DeleteVerificationCounts::try_new(12, 6, 6, 9).unwrap()
                )
            );
        }
    }
    #[test]
    fn verification_author_abandoned_unit_never_publishes_for_either_unit_pattern() {
        use novarocks_local_program::{ProgramNodeId, StaticSummaryAuthor};
        for units in [1, 3] {
            let kind = SummaryKind::DeleteVerification;
            let author = StaticSummaryAuthor::Node(ProgramNodeId::new(0));
            let mut assembly =
                SummaryAuthorAssemblyObligation::new(&[declared_output(kind, author)]).unwrap();
            let state = assembly
                .claim(kind, author)
                .unwrap()
                .declare_units(
                    units,
                    MemTracker::new_root("canceling original author Task"),
                )
                .unwrap();
            assembly.seal().unwrap();
            let mut contributors = (0..units)
                .map(|ordinal| state.contributor(ordinal).unwrap())
                .collect::<Vec<_>>();
            let runtime = RuntimeState::default();
            let mut source = SummarySourceFactory::new(state, summary_layout_for(kind))
                .unwrap()
                .create(1, 0);
            source.set_construction_factory(creator(&runtime));
            source.prepare().unwrap();
            let abandoned = contributors.pop().unwrap();
            for unit in &mut contributors {
                unit.finish().unwrap();
            }
            drop(abandoned);
            assert!(
                source
                    .drive_for_test(&runtime)
                    .unwrap_err()
                    .to_string()
                    .contains("delete-verification")
            );
        }
    }
    #[test]
    fn logical_change_summary_reduces_actual_contributors_once_and_zero_is_a_real_row() {
        let tracker = MemTracker::new_root("summary state");
        for empty in [true, false] {
            let state =
                TaskSummaryState::new(SummaryKind::WriteCounts, 3, tracker.clone()).unwrap();
            let mut contributors = (0..3)
                .map(|i| state.contributor(i).unwrap())
                .collect::<Vec<_>>();
            assert!(state.contributor(0).is_err());
            assert!(state.result().unwrap().is_none());
            if !empty {
                for c in &mut contributors {
                    c.accept(SummaryValue::WriteCounts(
                        LogicalChangeCounts::try_new(2, 3, 4).unwrap(),
                    ))
                    .unwrap();
                }
            }
            contributors[2].finish().unwrap();
            contributors[0].finish().unwrap();
            assert!(state.result().unwrap().is_none());
            contributors[1].finish().unwrap();
            assert!(contributors[1].finish().is_err());
            let runtime = RuntimeState::default();
            let mut source = SummarySourceFactory::new(state.clone(), summary_layout())
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
                SummarySourceFactory::new(state, summary_layout())
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
        let state = TaskSummaryState::new(SummaryKind::WriteCounts, 1, tracker.clone()).unwrap();
        drop(state.contributor(0).unwrap());
        assert!(state.result().is_err());
        let state = TaskSummaryState::new(SummaryKind::WriteCounts, 2, tracker).unwrap();
        let mut first = state.contributor(0).unwrap();
        let mut second = state.contributor(1).unwrap();
        first
            .accept(SummaryValue::WriteCounts(
                LogicalChangeCounts::try_new(i64::MAX as u64, 0, 0).unwrap(),
            ))
            .unwrap();
        second
            .accept(SummaryValue::WriteCounts(LogicalChangeCounts::DELETE))
            .unwrap();
        first.finish().unwrap();
        assert!(second.finish().is_err());
        assert!(state.result().is_err());
    }
    #[test]
    fn logical_change_summary_wait_outlives_transport_deadline_and_real_data_end_complete() {
        let owner = crate::runtime::task::io::exchange::test_support::Fixture::with_runtime(
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
        let mut ctx = DriverStep::new(&errors, &mut work, &mut conversions);
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
        assert_eq!(
            match coverage.finish().unwrap() {
                SummaryValue::WriteCounts(counts) => counts.affected_rows(),
                _ => panic!("wrong kind"),
            },
            9
        );
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
        let mut ctx = DriverStep::new(&errors, &mut work, &mut conversions);
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
        let mut ctx = DriverStep::new(&errors, &mut work, &mut conversions);
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
        assert_eq!(
            match coverage.finish().unwrap() {
                SummaryValue::WriteCounts(counts) => counts.affected_rows(),
                _ => panic!("wrong kind"),
            },
            9
        );
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
                SummaryKind::WriteCounts,
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
                value: SummaryValue::WriteCounts(
                    LogicalChangeCounts::try_new(i64::MAX as u64, 0, 0).unwrap(),
                ),
            })
            .unwrap();
        assert!(
            coverage
                .accept(SourceBoundSummary {
                    source: fixture.streams[0].source(),
                    value: SummaryValue::WriteCounts(LogicalChangeCounts::ZERO)
                })
                .is_err()
        );
        assert!(
            coverage
                .accept(SourceBoundSummary {
                    source: fixture.streams[2].source(),
                    value: SummaryValue::WriteCounts(LogicalChangeCounts::ZERO)
                })
                .is_err()
        );
        assert!(
            coverage
                .accept(SourceBoundSummary {
                    source: fixture.streams[1].source(),
                    value: SummaryValue::WriteCounts(LogicalChangeCounts::DELETE)
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
        let index = SummarySourceIndex::new(
            SummaryKind::WriteCounts,
            &sources,
            tracker.clone(),
            fixture.factory.as_ref(),
        )
        .unwrap();
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
                    value: SummaryValue::WriteCounts(LogicalChangeCounts::INSERT),
                })
                .unwrap();
        }
        assert_eq!(
            match coverage.finish().unwrap() {
                SummaryValue::WriteCounts(counts) => counts.affected_rows(),
                _ => panic!("wrong kind"),
            },
            64
        );
        sources.push(sources[0]);
        assert!(
            SummarySourceIndex::new(
                SummaryKind::WriteCounts,
                &sources,
                tracker.clone(),
                fixture.factory.as_ref()
            )
            .is_err()
        );
        drop(coverage);
        drop(index);
        assert_eq!(tracker.current(), 0);
        let limited = MemTracker::new_root("source index original query limit");
        limited.install_limit_once(1).unwrap();
        assert!(
            SummarySourceIndex::new(
                SummaryKind::WriteCounts,
                &sources,
                limited.clone(),
                fixture.factory.as_ref()
            )
            .is_err()
        );
        assert_eq!(limited.current(), 0);
    }
}
