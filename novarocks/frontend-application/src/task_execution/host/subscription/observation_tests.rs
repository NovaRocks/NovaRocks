// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#[path = "observation_tests/liveness.rs"]
mod liveness;

use super::*;
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    actor_turn::ActorRuntime,
    coalescing_input::{CoalescingInput, Merge},
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{ActorId, Generation, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::{OwnedSlotKind, OwnedSlotNeed},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use std::{
    future::{Future, pending, ready},
    pin::Pin,
    sync::Weak,
};

#[derive(Debug, Default)]
pub(in crate::task_execution::host) struct ManualClock(Mutex<Duration>);
impl MonotonicClock for ManualClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(*self.0.lock().unwrap())
    }
}
impl ManualClock {
    fn at(&self, millis: u64) {
        *self.0.lock().unwrap() = Duration::from_millis(millis);
    }
}
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn key() -> StreamKey {
    StreamKey {
        participation: ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        ),
        generation: NonZeroU64::new(1).unwrap(),
    }
}
fn token(n: u64) -> AttemptRequestToken {
    AttemptRequestToken::new(NonZeroU64::new(n).unwrap())
}
fn bookmark(key: StreamKey, sequence: u64) -> ObservationEvent {
    ObservationEvent::Fact {
        source_revision: None,
        fact: ObservationFact::Bookmark {
            generation: key.generation.get(),
            sequence,
            covered_prefix: sequence,
            source_cut: sequence,
        },
    }
}
#[derive(Debug, PartialEq)]
enum Seen {
    Observations(Vec<u64>),
    Control,
    Snapshot(ReadStateSnapshot),
    Progress(ReadProgressSnapshot),
    Phase(MonotonicInstant, Option<MonotonicInstant>),
}
#[derive(Debug)]
struct Recorder(
    Arc<Mutex<Vec<(InputId, Seen)>>>,
    Arc<Mutex<Vec<ObservationOpening>>>,
    Option<liveness::Capture>,
);
impl Actor for Recorder {
    type Input = AttemptInput;
    type Effect = ();
    fn timer_input(_: Tick) -> AttemptInput {
        unreachable!("test has no deadline")
    }
    fn handle(
        &mut self,
        _: Tick,
        input: InputEnvelope,
        value: AttemptInput,
        _: &mut ActorStep<'_, ()>,
    ) {
        if let Some(capture) = &self.2 {
            assert!(capture.lock().unwrap().replace((input, value)).is_none());
            return;
        }
        let summary = match value {
            AttemptInput::Executor(ExecutorFact::Observations(batch)) => Seen::Observations(
                batch
                    .into_items()
                    .into_vec()
                    .into_iter()
                    .map(|v| {
                        self.1.lock().unwrap().push(v.opening);
                        match v.event {
                            ObservationEvent::Fact {
                                fact: ObservationFact::Bookmark { sequence, .. },
                                ..
                            } => sequence,
                            ObservationEvent::Failed(_) => u64::MAX,
                            ObservationEvent::Closed => u64::MAX - 1,
                            _ => 0,
                        }
                    })
                    .collect(),
            ),
            AttemptInput::Port(_) => Seen::Control,
            AttemptInput::Executor(ExecutorFact::SubscriptionProgress(phase)) => {
                Seen::Phase(phase.opening_started_at, phase.opened_at)
            }
            AttemptInput::Executor(ExecutorFact::ReadState(snapshot)) => Seen::Snapshot(snapshot),
            AttemptInput::Executor(ExecutorFact::ReadProgress(progress)) => {
                Seen::Progress(progress)
            }
            _ => unreachable!("unregistered test input"),
        };
        self.0.lock().unwrap().push((input.input, summary));
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {
        unreachable!()
    }
}
#[derive(Clone, Copy)]
struct TestReadProgress(Option<ReadProgressSnapshot>);
impl Merge for TestReadProgress {
    fn merge(&mut self, newer: Self) {
        if let Some(newer) = newer.0 {
            if let Some(old) = self.0 {
                assert_eq!(old.participation, newer.participation);
                if (newer.generation, newer.resume_revision) < (old.generation, old.resume_revision)
                {
                    return;
                }
            }
            self.0 = Some(newer);
        }
    }
    fn unit() -> Self {
        Self(None)
    }
}
struct BankState {
    data: Vec<OwnedPositionObligation>,
    control: Vec<OwnedPositionObligation>,
    used: usize,
    control_used: usize,
    waits: BTreeMap<StreamKey, Waker>,
    next_claim: u64,
    claims: BTreeMap<u64, Option<StreamKey>>,
}
type ResumeCallback = Box<dyn FnOnce(ReadProgressSnapshot) + Send>;

/// Test author uses the SAME existing Source byte window plus real bound CR
/// positions. It adds NO ledger byte dimension or transport capacity claim.
pub(in crate::task_execution::host) struct Bank {
    budget: ObservationBudget,
    state: Mutex<BankState>,
    original: Weak<Bank>,
    on_return: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    binding: RootControlBinding,
    canonical: Mutex<BTreeMap<AttemptRequestToken, LogicalToAttempt<AttemptFailure>>>,
    projections: Mutex<usize>,
    metadata: Mutex<TestMetadataRecord>,
    progress: Mutex<Option<CoalescingInput<Recorder, TestReadProgress>>>,
    on_resume: Mutex<Option<ResumeCallback>>,
    progress_debts: Mutex<std::collections::BTreeSet<StreamKey>>,
    progress_failed: std::sync::atomic::AtomicBool,
}
impl Bank {
    pub(in crate::task_execution::host) fn make(
        budget: ObservationBudget,
        mut slots: Vec<OwnedPositionObligation>,
        binding: RootControlBinding,
    ) -> Arc<Self> {
        let control = vec![slots.pop().unwrap()];
        Arc::new_cyclic(|weak| Self {
            budget,
            original: weak.clone(),
            on_return: Mutex::new(None),
            binding,
            canonical: Mutex::new(BTreeMap::new()),
            projections: Mutex::new(0),
            metadata: Mutex::new(TestMetadataRecord::default()),
            progress: Mutex::new(None),
            on_resume: Mutex::new(None),
            progress_debts: Mutex::new(std::collections::BTreeSet::new()),
            progress_failed: std::sync::atomic::AtomicBool::new(false),
            state: Mutex::new(BankState {
                data: slots,
                control,
                used: 0,
                control_used: 0,
                waits: BTreeMap::new(),
                next_claim: 0,
                claims: BTreeMap::new(),
            }),
        })
    }

    pub(in crate::task_execution::host) fn refuse_metadata_once(&self) {
        self.metadata.lock().unwrap().refuse_acquisition_once = true;
    }

    pub(in crate::task_execution::host) fn retire_positions(&self) {
        assert!(self.metadata.lock().unwrap().retired);
        assert!(!self.metadata.lock().unwrap().unknown);
        let mut bank = self.state.lock().unwrap();
        assert_eq!(bank.used, 0);
        assert_eq!(bank.control_used, 0);
        assert!(bank.claims.is_empty());
        assert_eq!(
            bank.data.len() + bank.control.len(),
            self.budget.total_positions
        );
        let mut slots = std::mem::take(&mut bank.data);
        slots.extend(std::mem::take(&mut bank.control));
        drop(bank);
        for slot in slots {
            slot.retire_after_exit().unwrap();
        }
    }
    fn seal(&self) -> RootControlRef {
        let token = token(1);
        self.canonical.lock().unwrap().insert(
            token,
            LogicalToAttempt {
                activation: self.binding.0.activation,
                body: LogicalToAttemptBody::SealRequest { token },
            },
        );
        self.binding.reference(token)
    }
    fn credit(&self, control: bool, stream: Option<StreamKey>) -> Option<ObservationCredit> {
        let reserved = if control {
            self.budget.control_bytes
        } else {
            self.budget.max_frame_bytes
        };
        let (position, claim) = {
            let mut state = self.state.lock().unwrap();
            let position = if control {
                let p = state.control.pop()?;
                state.control_used += reserved;
                p
            } else {
                if state.used
                    > (self.budget.total_bytes - self.budget.control_bytes).saturating_sub(reserved)
                {
                    return None;
                }
                let p = state.data.pop()?;
                state.used += reserved;
                p
            };
            state.next_claim += 1;
            let claim = state.next_claim;
            state.claims.insert(claim, stream);
            (position, claim)
        };
        let owner = self.original.upgrade().unwrap();
        let refund = owner.clone();
        Some(ObservationCredit::from_source(
            InputSlotLoan::from_source(position, move |position| {
                let wakes = {
                    let mut state = refund.state.lock().unwrap();
                    if control {
                        state.control.push(position);
                    } else {
                        state.data.push(position);
                    }
                    std::mem::take(&mut state.waits)
                        .into_values()
                        .collect::<Vec<_>>()
                };
                for wake in wakes {
                    wake.wake();
                }
            }),
            Box::new(Bytes {
                bank: owner.clone(),
                reserved,
                control,
            }),
            move || {
                let original = owner.state.lock().unwrap().claims.remove(&claim);
                assert!(
                    original.is_some(),
                    "one exact original claim return receipt"
                );
            },
        ))
    }
}
struct Bytes {
    bank: Arc<Bank>,
    reserved: usize,
    control: bool,
}
impl Bytes {
    fn refund(&mut self, amount: usize) {
        let wakes = {
            let mut s = self.bank.state.lock().unwrap();
            if self.control {
                s.control_used -= amount;
            } else {
                s.used -= amount;
            }
            std::mem::take(&mut s.waits)
                .into_values()
                .collect::<Vec<_>>()
        };
        self.reserved -= amount;
        for wake in wakes {
            wake.wake();
        }
    }
}
impl ObservationByteLoan for Bytes {
    fn reserved_bytes(&self) -> usize {
        self.reserved
    }
    fn shrink_to(&mut self, actual: usize) -> Result<(), ObservationError> {
        if actual > self.reserved {
            return Err(ObservationError::Oversized {
                charged: actual,
                reserved: self.reserved,
            });
        }
        self.refund(self.reserved - actual);
        Ok(())
    }
}
impl Drop for Bytes {
    fn drop(&mut self) {
        self.refund(self.reserved);
        let callback = self.bank.on_return.lock().unwrap().take();
        if let Some(callback) = callback {
            callback();
        }
    }
}
// This fixture checks the lifetime handshake against the original test owner.
// It does not prove production allocation funding: P10a must supply the actual
// metadata author for the real Entry/container/ActorInput backing.
#[derive(Default)]
struct TestMetadataRecord {
    refuse_acquisition_once: bool,
    required: Option<ObservationMetadataLayout>,
    retired: bool,
    refuse_once: bool,
    unknown: bool,
}
struct TestMetadata {
    original: Arc<Bank>,
    retired: bool,
}
impl Drop for TestMetadata {
    fn drop(&mut self) {
        if !self.retired {
            self.original.metadata.lock().unwrap().unknown = true;
        }
    }
}
impl ObservationMetadataObligation for TestMetadata {
    fn retire_after_exit(mut self: Box<Self>) -> Result<(), MetadataRetirementRefusal> {
        let refused = {
            let mut record = self.original.metadata.lock().unwrap();
            std::mem::take(&mut record.refuse_once)
        };
        if refused {
            return Err(MetadataRetirementRefusal {
                error: ObservationError::Busy,
                original: self,
            });
        }
        assert!(self.original.state.lock().unwrap().claims.is_empty());
        assert!(
            self.original.progress.lock().unwrap().is_none(),
            "original Source role must actually close before metadata release"
        );
        self.original.metadata.lock().unwrap().retired = true;
        self.retired = true;
        Ok(())
    }
}
impl ObservationSourcePool for Bank {
    fn fixed_root_control_binding(&self) -> RootControlBinding {
        self.binding.clone()
    }
    fn close_root_controls(&self) -> Result<(), ObservationError> {
        Ok(())
    }
    fn fence_root_controls_for_close(&self) -> bool {
        true
    }
    fn retain_metadata(
        &self,
        required: ObservationMetadataLayout,
    ) -> Result<Box<dyn ObservationMetadataObligation>, ObservationError> {
        let mut record = self.metadata.lock().unwrap();
        if std::mem::take(&mut record.refuse_acquisition_once) {
            return Err(ObservationError::Budget);
        }
        assert!(record.required.replace(required).is_none());
        Ok(Box::new(TestMetadata {
            original: self.original.upgrade().unwrap(),
            retired: false,
        }))
    }
    fn register_stream(&self, _: StreamKey) -> Result<(), ObservationError> {
        Ok(())
    }
    fn retire_stream(&self, key: StreamKey) -> Result<(), ObservationError> {
        self.stream_claims_exited(key)
            .then_some(())
            .ok_or(ObservationError::Busy)
    }
    fn project_root_control(
        &self,
        reference: &RootControlRef,
    ) -> Result<LogicalToAttempt<AttemptFailure>, ObservationError> {
        if !reference.belongs_to(&self.binding) {
            return Err(ObservationError::InvalidRootControl);
        }
        let original = self
            .canonical
            .lock()
            .unwrap()
            .get(&reference.token())
            .cloned()
            .ok_or(ObservationError::InvalidRootControl)?;
        *self.projections.lock().unwrap() += 1;
        Ok(original)
    }
    fn budget(&self) -> ObservationBudget {
        self.budget
    }
    fn poll_read(
        &self,
        key: StreamKey,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ObservationCredit, ObservationError>> {
        // Register before checking the real original pool/window.
        self.state
            .lock()
            .unwrap()
            .waits
            .insert(key, cx.waker().clone());
        match self.credit(false, Some(key)) {
            Some(credit) => {
                self.state.lock().unwrap().waits.remove(&key);
                Poll::Ready(Ok(credit))
            }
            None => Poll::Pending,
        }
    }
    fn cancel_read_wait(&self, key: StreamKey) {
        self.state.lock().unwrap().waits.remove(&key);
    }
    fn read_resumed(&self, progress: ReadProgressSnapshot) -> Result<(), ObservationError> {
        if let Some(callback) = self.on_resume.lock().unwrap().take() {
            callback(progress);
        }
        // Borrow the actual original prepaid Ordinary standing; no fabricated
        // success and no bool/ref substituted for the publisher's obligation.
        let original = self.progress.lock().unwrap().as_ref().unwrap().clone();
        self.progress_debts.lock().unwrap().insert(StreamKey {
            participation: progress.participation,
            generation: progress.generation,
        });
        let published = original
            .notify(TestReadProgress(Some(progress)))
            .map(|_| ())
            .map_err(ObservationError::Runtime);
        if published.is_err() {
            self.progress_failed
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        published
    }
    fn try_control(
        &self,
        reference: &RootControlRef,
    ) -> Result<ObservationCredit, ObservationError> {
        if !reference.belongs_to(&self.binding)
            || !self
                .canonical
                .lock()
                .unwrap()
                .contains_key(&reference.token())
        {
            return Err(ObservationError::InvalidRootControl);
        }
        self.credit(true, None).ok_or(ObservationError::Busy)
    }
    fn stream_claims_exited(&self, key: StreamKey) -> bool {
        !self
            .state
            .lock()
            .unwrap()
            .claims
            .values()
            .any(|stream| *stream == Some(key))
            && !self.progress_debts.lock().unwrap().contains(&key)
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    work: WorkObligation,
    capacity: ReservationObligation,
    runtime: ActorRuntime<Recorder>,
    registration: novarocks_control_runtime::actor_turn::ActorRegistration,
    input: OrderedObservationInput<Recorder>,
    bank: Arc<Bank>,
    clock: Arc<ManualClock>,
    seen: Arc<Mutex<Vec<(InputId, Seen)>>>,
    openings: Arc<Mutex<Vec<ObservationOpening>>>,
    key: StreamKey,
}
impl Fixture {
    fn new(queue: usize, byte_frames: usize) -> Self {
        Self::make(queue, byte_frames, true)
    }
    fn unopened(queue: usize, byte_frames: usize) -> Self {
        Self::make(queue, byte_frames, false)
    }
    fn make(queue: usize, byte_frames: usize, opened: bool) -> Self {
        Self::make_with_trace(queue, byte_frames, opened, None)
    }

    fn make_with_trace(
        queue: usize,
        byte_frames: usize,
        opened: bool,
        trace: Option<liveness::Trace>,
    ) -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let budget = ObservationBudget {
            queued_frames: queue,
            total_positions: queue + 2,
            max_frame_bytes: 2048,
            control_bytes: 512,
            total_bytes: 2048 * byte_frames + 512,
        };
        let (mut work, mut capacity) = scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        inputs: budget.total_positions,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        let mut slots = Vec::new();
        for _ in 0..budget.total_positions {
            let mut slot = capacity.take_position(OwnedSlotKind::Input).unwrap();
            slot.bind().unwrap();
            slots.push(slot);
        }
        let seen = Arc::new(Mutex::new(Vec::new()));
        let openings = Arc::new(Mutex::new(Vec::new()));
        let (runtime, key, capture, progress_source) = if let Some(trace) = trace {
            (
                ActorRuntime::with_history(trace.history),
                trace.key,
                Some(trace.capture),
                ActorId::new(3).unwrap(),
            )
        } else {
            (ActorRuntime::new(), key(), None, ActorId::new(5).unwrap())
        };
        let registration = runtime
            .register_bound(
                actor(),
                scope.clone(),
                Recorder(seen.clone(), openings.clone(), capture),
                ActorConfig {
                    mailbox: [8; 4],
                    max_batches: 8,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap();
        let binding = RootControlBinding::for_registered_source(
            registration.clone(),
            AttemptActivationIdentity::from_parts(1, key.participation.query_execution_id(), 1),
            TaskIdentity::new(
                key.participation.query_execution_id(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                key.participation.backend_process_id(),
            ),
        )
        .unwrap();
        assert_eq!(binding.registration().actor(), actor());
        let bank = Bank::make(budget, slots, binding);
        let standing = |class| {
            runtime
                .reserve_admitted_input(actor(), ActorId::new(2).unwrap(), class, &work)
                .unwrap()
                .into_standing_input()
                .unwrap()
        };
        let progress = runtime
            .reserve_admitted_input(actor(), progress_source, InputClass::Ordinary, &work)
            .unwrap()
            .into_standing_input()
            .unwrap();
        *bank.progress.lock().unwrap() = Some(CoalescingInput::new(
            progress,
            |update: &TestReadProgress| {
                AttemptInput::Executor(ExecutorFact::ReadProgress(
                    update.0.expect("actual original resume fact"),
                ))
            },
        ));
        let clock = Arc::new(ManualClock::default());
        let input = OrderedObservationInput::new(
            standing(InputClass::Lifecycle),
            standing(InputClass::Control),
            bank.clone(),
            bank.fixed_root_control_binding(),
            clock.clone(),
            |input| input,
            Waker::noop().clone(),
        )
        .unwrap();
        input.register_stream(key).unwrap();
        let fixture = Self {
            ledger,
            scope,
            work,
            capacity,
            runtime,
            registration,
            input,
            bank,
            clock,
            seen,
            openings,
            key,
        };
        if opened {
            fixture.open_stream(key);
        }
        fixture
    }
    fn open_stream(&self, key: StreamKey) {
        // Model a real successful first open poll against the original Source,
        // not a registeredAt fallback. Original preread credit is actually held.
        let mut credit = Some(self.bank.credit(false, Some(key)).unwrap());
        let mut opened = ready(Ok::<(), ObservationFailure>(()));
        let poll = self
            .input
            .poll_open(
                key,
                &mut credit,
                Pin::new(&mut opened),
                &mut Context::from_waker(Waker::noop()),
            )
            .unwrap();
        assert!(matches!(poll.result, Poll::Ready(Ok(()))));
        drop(credit);
    }
    fn phase(
        &self,
        generation: u64,
    ) -> Arc<crate::task_execution::host::subscriptions::SubscriptionPhasePublisher<Recorder>> {
        let input = self
            .runtime
            .reserve_admitted_input(
                actor(),
                ActorId::new(4).unwrap(),
                InputClass::Lifecycle,
                &self.work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap();
        crate::task_execution::host::subscriptions::SubscriptionPhasePublisher::new(
            input,
            StreamKey {
                generation: NonZeroU64::new(generation).unwrap(),
                ..self.key
            },
            |input| input,
        )
    }
    fn drain_phase(
        &self,
        phase: &crate::task_execution::host::subscriptions::SubscriptionPhasePublisher<Recorder>,
    ) {
        while let Some(id) = phase.current_input().unwrap() {
            self.runtime.poll_one(Tick(1)).unwrap();
            self.runtime.retire_input(actor(), id).unwrap();
            phase.input_retired(id).unwrap();
        }
    }
    fn drain_progress(&self) {
        let progress = self.bank.progress.lock().unwrap().as_ref().cloned();
        if let Some(progress) = progress {
            while let Some(id) = progress.in_flight().unwrap() {
                self.runtime.poll_one(Tick(1)).unwrap();
                self.runtime.retire_input(actor(), id).unwrap();
                progress.after_retire().unwrap();
            }
            assert!(
                !self
                    .bank
                    .progress_failed
                    .load(std::sync::atomic::Ordering::SeqCst),
                "unresolved real publisher debt cannot be refunded"
            );
            self.bank.progress_debts.lock().unwrap().clear();
        }
    }
    fn close_progress(&self) {
        self.drain_progress();
        if let Some(progress) = self.bank.progress.lock().unwrap().take() {
            progress.close_after_exit().unwrap();
        }
    }
    fn confirmation(&self) -> CausalInputObligation<Recorder> {
        CausalInputObligation::reserve_admitted(
            &self.runtime,
            &self.registration,
            ActorId::new(3).unwrap(),
            InputClass::Lifecycle,
            &self.work,
        )
        .unwrap()
    }
    fn frame(&self, sequence: u64) {
        let mut cx = Context::from_waker(Waker::noop());
        let credit = match self.bank.poll_read(self.key, &mut cx) {
            Poll::Ready(Ok(c)) => c,
            _ => panic!("test original credit unavailable"),
        };
        self.input
            .accept(self.key, bookmark(self.key, sequence), 0, credit)
            .unwrap();
    }
    fn consume(&self, root_processed: bool) -> InputId {
        let id = self
            .input
            .0
            .state
            .lock()
            .unwrap()
            .in_flight
            .as_ref()
            .unwrap()
            .input;
        self.runtime.poll_one(Tick(1)).unwrap();
        let control = self
            .seen
            .lock()
            .unwrap()
            .iter()
            .find(|(input, _)| *input == id)
            .is_some_and(|(_, v)| *v == Seen::Control);
        if control && root_processed {
            self.input.root_control_processed(id).unwrap();
        }
        self.runtime.retire_input(actor(), id).unwrap();
        self.input.input_retired(id).unwrap();
        id
    }
    fn finish(mut self) {
        while self.input.0.state.lock().unwrap().in_flight.is_some() {
            self.consume(true);
        }
        self.drain_progress();
        if self
            .input
            .0
            .state
            .lock()
            .unwrap()
            .streams
            .contains_key(&self.key)
        {
            self.input.job_closed(self.key).unwrap();
            self.input.retire_stream(self.key).unwrap();
        }
        self.close_progress();
        self.input.close_after_exit().unwrap();
        drop(self.input);
        self.bank.retire_positions();
        self.work.body_exited().unwrap();
        self.work.captures_exited().unwrap();
        self.capacity.finish().unwrap();
        self.ledger
            .reap_scope_ready(self.scope.id(), self.bank.budget.total_positions + 8)
            .unwrap();
        self.runtime.close_actor_admission(actor()).unwrap();
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        self.runtime.seal_actor(actor()).unwrap();
        self.scope.close_external().unwrap();
        self.runtime.remove_actor(actor()).unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}

#[test]
fn real_runtime_batches_cut_at_256_and_root_control_waits_for_first_complete_processing() {
    let f = Fixture::new(300, 512);
    f.frame(1);
    for n in 2..=300 {
        f.frame(n);
    }
    f.input.root_control(f.bank.seal()).unwrap();
    f.frame(301);
    {
        let state = f.input.0.state.lock().unwrap();
        assert_eq!(state.queue.len(), 301);
        assert!(state.queue.capacity() <= f.bank.budget.total_positions);
    }
    f.consume(false);
    f.consume(false);
    f.consume(false);
    let control = f.consume(false);
    assert!(
        f.input
            .0
            .state
            .lock()
            .unwrap()
            .in_flight
            .as_ref()
            .unwrap()
            .retired
    );
    assert!(matches!(
        f.seen.lock().unwrap().last(),
        Some((_, Seen::Control))
    ));
    // The root's runtime input already retired, but later facts still cannot
    // pass until the first FULL processing callback. A final reply is absent.
    f.runtime.poll_one(Tick(2)).unwrap();
    assert_eq!(f.seen.lock().unwrap().len(), 4);
    f.input.root_control_processed(control).unwrap();
    f.consume(false);
    let seen = f.seen.lock().unwrap();
    assert!(matches!(&seen[0].1, Seen::Observations(v) if v == &[1]));
    assert!(matches!(&seen[1].1, Seen::Observations(v) if v.len() == 256));
    assert!(matches!(&seen[2].1, Seen::Observations(v) if v.len() == 43));
    assert!(matches!(&seen[4].1, Seen::Observations(v) if v == &[301]));
    drop(seen);
    f.finish();
}
#[test]
fn ready_before_decode_is_incomplete_and_its_failure_keeps_the_original_control_cut() {
    let f = Fixture::new(8, 4);
    let mut credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
    f.clock.at(2000);
    let mut ready = ready(());
    let receipt = match f.input.poll_receive(
        f.key,
        &mut credit,
        Pin::new(&mut ready),
        &mut Context::from_waker(Waker::noop()),
    ) {
        Poll::Ready(Ok((receipt, ()))) => receipt,
        _ => panic!("actual Ready not recorded"),
    };
    f.clock.at(3000);
    f.input.root_control(f.bank.seal()).unwrap();
    let snapshot = f
        .input
        .read_state(f.key, token(2), MonotonicInstant::ORIGIN)
        .unwrap();
    assert_eq!(
        snapshot.received_through_snapshot,
        ReceptionCoverage::Incomplete
    );
    assert!(f.input.0.state.lock().unwrap().in_flight.is_none());
    let failure = novarocks_attempt_protocol::input::ObservationFailure {
        kind: novarocks_attempt_protocol::input::ObservationFailureKind::ProtocolViolation,
        cause: novarocks_attempt_protocol::failure::HostProjectedFailure {
            original: novarocks_attempt_protocol::failure::HostFailureRef(
                NonZeroU64::new(1).unwrap(),
            ),
            kind: novarocks_attempt_protocol::failure::HostFailureKind::NativeTransport,
            class: novarocks_coordination_contract::AttemptFailureClass::ContractViolation,
            site: novarocks_attempt_protocol::failure::AttemptFailureSite::Participation(
                f.key.participation,
            ),
        },
    };
    f.input
        .complete_receive(&receipt, ObservationEvent::Failed(failure), 0)
        .unwrap();
    f.consume(false);
    f.consume(true);
    assert!(matches!(&f.seen.lock().unwrap()[0].1, Seen::Observations(v) if v == &[u64::MAX]));
    assert_eq!(f.seen.lock().unwrap()[1].1, Seen::Control);
    assert_eq!(
        f.input
            .read_state(f.key, token(3), MonotonicInstant::ORIGIN)
            .unwrap()
            .received_through_snapshot,
        ReceptionCoverage::Complete
    );
    f.finish();
}
#[test]
fn ordinary_long_poll_pending_does_not_make_every_confirmation_incomplete() {
    let f = Fixture::new(8, 4);
    let mut credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
    let mut future = pending::<()>();
    assert!(
        f.input
            .poll_receive(
                f.key,
                &mut credit,
                Pin::new(&mut future),
                &mut Context::from_waker(Waker::noop())
            )
            .is_pending()
    );
    assert_eq!(
        f.input
            .read_state(f.key, token(2), MonotonicInstant::ORIGIN)
            .unwrap()
            .received_through_snapshot,
        ReceptionCoverage::Complete
    );
    drop(credit);
    f.finish();
}
#[test]
fn snapshot_clips_pause_to_one_instant_and_only_new_same_generation_markers_renew() {
    let mut s = ReadState::new(MonotonicInstant::ORIGIN);
    let key = key();
    let at = |n| MonotonicInstant::from_origin(Duration::from_secs(n));
    s.pause(at(1)).unwrap();
    let one = s.snapshot(key, token(1), at(3)).unwrap();
    assert_eq!(one.pauses.paused, Duration::from_secs(2));
    s.resume(at(5)).unwrap();
    s.receive(key, at(6), &bookmark(key, 1)).unwrap();
    s.pause(at(7)).unwrap();
    s.receive(key, at(8), &bookmark(key, 1)).unwrap();
    let old_key = StreamKey {
        generation: NonZeroU64::new(2).unwrap(),
        ..key
    };
    s.receive(key, at(9), &bookmark(old_key, 2)).unwrap();
    let two = s.snapshot(key, token(2), at(10)).unwrap();
    assert_eq!(two.last_liveness_at, at(6));
    assert_eq!(
        two.pauses,
        ReadPauseCoverage {
            from: at(6),
            through: at(10),
            paused: Duration::from_secs(3)
        }
    );
    s.receive(key, at(11), &bookmark(key, 2)).unwrap();
    let three = s.snapshot(key, token(3), at(12)).unwrap();
    assert_eq!(three.last_liveness_at, at(11));
    assert_eq!(three.pauses.paused, Duration::from_secs(1));
    assert_eq!(two.snapshot_at, at(10));
    assert_eq!(two.pauses.paused, Duration::from_secs(3));
}

#[test]
fn short_network_poll_retains_root_control_and_ready_placeholder_precedes_it() {
    let f = Fixture::new(8, 4);
    let mut sampled = None;
    let mut credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
    let mut network_poll = std::future::poll_fn(|_| {
        // Reenter the actual author during Future::poll. Holding its mutex
        // across the network poll would deadlock at this exact boundary.
        f.input.root_control(f.bank.seal()).unwrap();
        sampled = Some(
            f.input
                .read_state(f.key, token(7), MonotonicInstant::ORIGIN)
                .unwrap(),
        );
        assert!(f.input.0.state.lock().unwrap().in_flight.is_none());
        Poll::Ready(())
    });
    let receipt = match f.input.poll_receive(
        f.key,
        &mut credit,
        Pin::new(&mut network_poll),
        &mut Context::from_waker(Waker::noop()),
    ) {
        Poll::Ready(Ok((receipt, ()))) => receipt,
        _ => panic!("network Ready was not recorded"),
    };
    drop(network_poll);
    assert_eq!(
        sampled.unwrap().received_through_snapshot,
        ReceptionCoverage::Incomplete
    );
    f.input
        .complete_receive(&receipt, bookmark(f.key, 1), 0)
        .unwrap();
    f.consume(false);
    f.consume(true);
    assert_eq!(f.seen.lock().unwrap()[0].1, Seen::Observations(vec![1]));
    assert_eq!(f.seen.lock().unwrap()[1].1, Seen::Control);
    f.finish();
}
#[test]
fn full_source_window_does_not_block_the_independently_prepaid_confirmation() {
    let f = Fixture::new(8, 1);
    let mut credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
    let mut network = ready(());
    f.clock.at(1000);
    let receipt = match f.input.poll_receive(
        f.key,
        &mut credit,
        Pin::new(&mut network),
        &mut Context::from_waker(Waker::noop()),
    ) {
        Poll::Ready(Ok((receipt, ()))) => receipt,
        _ => panic!("original network receipt unavailable"),
    };
    assert!(
        f.bank
            .poll_read(f.key, &mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let registration = &f.registration;
    let mut independent = CausalInputObligation::reserve_admitted(
        &f.runtime,
        registration,
        ActorId::new(3).unwrap(),
        InputClass::Lifecycle,
        &f.work,
    )
    .unwrap();
    f.clock.at(3000);
    let mut confirming = Box::pin(f.input.confirm(
        f.key,
        token(8),
        MonotonicInstant::from_origin(Duration::from_secs(2)),
        &mut independent,
    ));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(confirming.as_mut().poll(&mut cx).is_pending());
    assert!(
        !f.seen
            .lock()
            .unwrap()
            .iter()
            .any(|(_, seen)| matches!(seen, Seen::Snapshot(_)))
    );
    f.bank.cancel_read_wait(f.key);
    f.input
        .complete_receive(&receipt, bookmark(f.key, 1), 0)
        .unwrap();
    let confirmation = match confirming.as_mut().poll(&mut cx) {
        Poll::Ready(Ok(id)) => id,
        _ => panic!("same original confirmation did not complete after decode"),
    };
    drop(confirming);
    // Common observation remains actually unretired/full; the independently
    // prepaid confirmation can still fill and be consumed exactly once.
    f.runtime.poll_one(Tick(1)).unwrap();
    f.runtime.poll_one(Tick(1)).unwrap();
    f.runtime.retire_input(actor(), confirmation).unwrap();
    let snapshots = f
        .seen
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, seen)| {
            if let Seen::Snapshot(snapshot) = seen {
                Some(*snapshot)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(
        snapshots[0].received_through_snapshot,
        ReceptionCoverage::Complete
    );
    independent.close_after_exit().unwrap();
    let snapshot = f
        .input
        .read_state(f.key, token(9), MonotonicInstant::ORIGIN)
        .unwrap();
    assert_eq!(
        snapshot.last_liveness_at,
        MonotonicInstant::from_origin(Duration::from_secs(1))
    );
    assert_eq!(
        snapshot.snapshot_at,
        MonotonicInstant::from_origin(Duration::from_secs(3))
    );
    assert_eq!(
        snapshot.received_through_snapshot,
        ReceptionCoverage::Complete
    );
    f.consume(false);
    f.finish();
}

#[test]
fn refund_reentry_cannot_reap_a_generation_before_all_original_loans_return() {
    let f = Fixture::new(8, 4);
    f.frame(1);
    f.input.job_closed(f.key).unwrap();
    f.input.root_control(f.bank.seal()).unwrap();
    let ingress = f.input.clone();
    let key = f.key;
    *f.bank.on_return.lock().unwrap() = Some(Box::new(move || {
        // The byte claim has refunded, but the original InputSlotLoan has not
        // returned yet. Even a synchronous refund wake cannot reap this Source.
        assert_eq!(ingress.retire_stream(key), Err(ObservationError::Busy));
        ingress.drive().unwrap();
        let state = ingress.0.state.lock().unwrap();
        assert!(state.in_flight.is_none());
        assert_eq!(state.queue.len(), 1);
    }));
    f.consume(false);
    assert_eq!(
        f.input.0.state.lock().unwrap().streams[&f.key].accepted_unretired,
        0
    );
    f.consume(true);
    f.finish();
}
#[test]
fn refund_panic_keeps_original_generation_retained_as_unknown() {
    let f = Fixture::new(8, 4);
    f.frame(1);
    f.input.job_closed(f.key).unwrap();
    *f.bank.on_return.lock().unwrap() =
        Some(Box::new(|| panic!("original byte claim refund panic")));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.consume(false)));
    assert!(panic.is_err());
    assert_eq!(
        f.input.0.state.lock().unwrap().streams[&f.key].accepted_unretired,
        1
    );
    assert_eq!(f.input.retire_stream(f.key), Err(ObservationError::Busy));
    // This mirrors a failure custodian retaining an UNKNOWN original Source.
    // It must never run normal positive cleanup merely to satisfy a fixture.
    std::mem::forget(f);
}

#[test]
fn one_wc_source_child_survives_generation_exit_until_original_jobs_and_frames_reap() {
    use crate::query_execution::native_execution_adapter::source_cohort::tests as wc;
    use crate::task_execution::host::subscriptions::SubscriptionSource;
    use novarocks_attempt_protocol::observation::SubscriptionRequest;
    use novarocks_control_runtime::{
        completion::ReportSlot,
        exit_custody::{ExitCustody, ExitCustodyPhase},
        exit_work::{PanicSink, run_future},
        reported_work::ReportedWork,
    };
    use novarocks_control_runtime_core::{completion::ConsumerKind, identity::ConsumerId};
    use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
    let f = Fixture::new(8, 4);
    let (control, mut driver, capacity) = wc::fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    wc::bind(&scope, &mut driver);
    let grant = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let cohort = wc::cohort(&scope, 1, &mut driver);
    let source =
        SubscriptionSource::from_cohort(f.key.participation, &scope, &cohort, f.input.clone())
            .unwrap();
    assert_eq!(cohort.remaining(), 0);
    let request = |generation| SubscriptionRequest {
        participation: f.key.participation,
        generation: NonZeroU64::new(generation).unwrap(),
        status_cursors: Box::new([]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([]),
    };
    for generation in [2, 3] {
        let job = source
            .prepare(Arc::new(request(generation)), &f.phase(generation))
            .unwrap();
        let (work, mut parcel) = f
            .scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::Derived(f.work.id()),
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let report = ReportSlot::reserve_next(
            &mut parcel,
            Generation::new(1).unwrap(),
            1,
            [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
        )
        .unwrap();
        let (custody, exit) =
            ExitCustody::from_admitted(ReportedWork::try_new(work, parcel, report).unwrap());
        let future = run_future(
            exit,
            async move {
                std::future::pending::<()>().await;
                drop(job);
            },
            PanicSink::new(),
        );
        assert!(source.close_after_jobs().is_err());
        drop(future);
        assert_eq!(custody.phase(), ExitCustodyPhase::Exited);
        assert!(custody.try_retire().unwrap());
        f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
        source
            .reap_generation(NonZeroU64::new(generation).unwrap())
            .unwrap();
        assert_eq!(
            cohort.remaining(),
            0,
            "generation rollover must not take a second paid WC child"
        );
    }
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    drop(source);
    drop((cohort, grant, capacity, scope));
    wc::finish(control, root, driver);
    f.finish();
}

#[test]
fn generation_keeps_phase_rpc_exit_and_actual_claim_return_as_independent_facts() {
    use crate::query_execution::native_execution_adapter::source_cohort::tests as wc;
    use crate::task_execution::host::subscriptions::*;
    use novarocks_attempt_protocol::{
        failure::{AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure},
        input::{ObservationFailure, ObservationFailureKind},
        observation::SubscriptionRequest,
    };
    use novarocks_control_runtime::{
        completion::ReportSlot,
        exit_custody::ExitCustody,
        exit_work::{PanicSink, run_future},
        reported_work::ReportedWork,
    };
    use novarocks_control_runtime_core::{completion::ConsumerKind, identity::ConsumerId};
    use novarocks_proto_codec::ProtocolError;
    use novarocks_proto_models::novarocks::TaskStatusStreamEvent;
    use novarocks_task_codec::operation::{
        CoveredObservationBookmark, CoveredStatusStreamEvent, CoveredStatusStreamFact,
        DecodedCoveredSubscription, encode_covered_status_event,
    };
    use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Io {
        frames: Arc<Mutex<VecDeque<TaskStatusStreamEvent>>>,
        opens: AtomicUsize,
        clock: Arc<ManualClock>,
        hanging: bool,
    }
    struct Stream(Arc<Mutex<VecDeque<TaskStatusStreamEvent>>>);
    impl CoveredSubscriptionIo for Io {
        fn open(
            &self,
            _: DecodedCoveredSubscription,
        ) -> SubscriptionIoFuture<'_, Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure>>
        {
            self.opens.fetch_add(1, Ordering::SeqCst);
            let frames = self.frames.clone();
            let clock = self.clock.clone();
            if self.hanging {
                return Box::pin(std::future::pending());
            }
            Box::pin(async move {
                clock.at(3000);
                Ok(Box::new(Stream(frames)) as Box<dyn CoveredSubscriptionStream>)
            })
        }
    }
    impl CoveredSubscriptionStream for Stream {
        fn next(
            &mut self,
        ) -> SubscriptionIoFuture<'_, Result<Option<TaskStatusStreamEvent>, ObservationFailure>>
        {
            let frame = self.0.lock().unwrap().pop_front();
            Box::pin(async move { Ok(frame) })
        }
    }
    #[derive(Default)]
    struct Failures {
        malformed: Mutex<Vec<ProtocolError>>,
        conflicting: Mutex<Vec<crate::native::task_transport::wire::CoveredEventValidationError>>,
        ingress: Mutex<Vec<ObservationError>>,
    }
    fn fact(key: StreamKey) -> ObservationFailure {
        ObservationFailure {
            kind: ObservationFailureKind::ProtocolViolation,
            cause: HostProjectedFailure {
                original: HostFailureRef(NonZeroU64::new(1).unwrap()),
                kind: HostFailureKind::NativeTransport,
                class: novarocks_coordination_contract::AttemptFailureClass::ContractViolation,
                site: AttemptFailureSite::Participation(key.participation),
            },
        }
    }
    impl SubscriptionFailureCustody for Failures {
        fn malformed(&self, key: StreamKey, original: ProtocolError) -> ObservationFailure {
            self.malformed.lock().unwrap().push(original);
            fact(key)
        }
        fn conflicting(
            &self,
            key: StreamKey,
            original: crate::native::task_transport::wire::CoveredEventValidationError,
        ) -> ObservationFailure {
            self.conflicting.lock().unwrap().push(original);
            fact(key)
        }
        fn ingress_rejected(
            &self,
            key: StreamKey,
            original: ObservationError,
        ) -> ObservationFailure {
            self.ingress.lock().unwrap().push(original);
            fact(key)
        }
    }
    for mode in 0..4 {
        let malformed = mode == 1;
        let hanging = mode >= 2;
        let refund_panic = mode == 3;
        let f = Fixture::new(8, 4);
        let (control, mut driver, capacity) = wc::fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        wc::bind(&scope, &mut driver);
        let grant = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        let cohort = wc::cohort(&scope, 1, &mut driver);
        let source =
            SubscriptionSource::from_cohort(f.key.participation, &scope, &cohort, f.input.clone())
                .unwrap();
        let request = SubscriptionRequest {
            participation: f.key.participation,
            generation: NonZeroU64::new(2).unwrap(),
            status_cursors: Box::new([]),
            task_convergence_cursors: Box::new([]),
            participation_cursor: None,
            quiesce_cursor: None,
            required_identities: Box::new([]),
        };
        let phase = f.phase(2);
        let job = source.prepare(Arc::new(request), &phase).unwrap();
        let wire = if malformed {
            TaskStatusStreamEvent::default()
        } else {
            encode_covered_status_event(&CoveredStatusStreamEvent {
                source_revision: None,
                fact: CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                    generation: 2,
                    sequence: 9,
                    covered_prefix: 0,
                    source_cut: 0,
                }),
            })
            .unwrap()
        };
        let io = Arc::new(Io {
            frames: Arc::new(Mutex::new(VecDeque::from([wire]))),
            opens: AtomicUsize::new(0),
            clock: f.clock.clone(),
            hanging,
        });
        let failures = Arc::new(Failures::default());
        let (work, mut parcel) = f
            .scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::Derived(f.work.id()),
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut parcel,
            Generation::new(1).unwrap(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (custody, exit) = ExitCustody::from_admitted(
            ReportedWork::try_new(work, parcel, report.clone()).unwrap(),
        );
        f.clock.at(1000);
        let panic_sink = PanicSink::new();
        let mut running = Box::pin(run_future(
            exit,
            job.run(io.clone(), failures.clone()),
            panic_sink.clone(),
        ));
        let first = running
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        let opening_snapshot = f
            .input
            .read_state(
                StreamKey {
                    generation: NonZeroU64::new(2).unwrap(),
                    ..f.key
                },
                token(8),
                MonotonicInstant::ORIGIN,
            )
            .unwrap();
        assert_eq!(
            opening_snapshot.opening_started_at,
            Some(MonotonicInstant::from_origin(Duration::from_secs(1)))
        );
        assert_eq!(
            opening_snapshot.opened_at,
            if hanging {
                None
            } else {
                Some(MonotonicInstant::from_origin(Duration::from_secs(3)))
            }
        );
        let end = if hanging {
            assert!(first.is_pending());
            assert_eq!(
                custody.phase(),
                novarocks_control_runtime::exit_custody::ExitCustodyPhase::Started
            );
            f.drain_phase(&phase);
            f.clock.at(100_000);
            assert!(
                running
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending(),
                "host must not manufacture an opening timeout/retry policy"
            );
            if refund_panic {
                *f.bank.on_return.lock().unwrap() =
                    Some(Box::new(|| panic!("original preread refund panic")));
            }
            source
                .close_generation(NonZeroU64::new(2).unwrap())
                .unwrap();
            if refund_panic {
                let panic = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    running
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                })) {
                    Err(panic) => panic,
                    Ok(_) => panic!("preread refund panic was not propagated"),
                };
                assert!(panic.is::<novarocks_control_runtime::exit_work::ForwardedWorkPanic>());
                drop(running);
                assert!(panic_sink.has_panic());
                // The internal async-body Drop panic has an actual exited F;
                // that positive report cannot bless its unacknowledged claim.
                assert_eq!(
                    custody.phase(),
                    novarocks_control_runtime::exit_custody::ExitCustodyPhase::Exited
                );
                report.consume(consumer).unwrap();
                assert!(custody.try_retire().unwrap());
                f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
                let key = StreamKey {
                    generation: NonZeroU64::new(2).unwrap(),
                    ..f.key
                };
                assert_eq!(
                    f.bank.state.lock().unwrap().used,
                    0,
                    "occupancy alone cannot prove return"
                );
                assert!(!f.bank.stream_claims_exited(key));
                source.close_new_jobs();
                assert!(source.reap_generation(key.generation).is_err());
                assert!(source.close_after_jobs().is_err());
                assert!(
                    source.scope().is_ok(),
                    "the SAME WC child must remain retained"
                );
                // Production's failure custodian retains these exact originals;
                // normal cleanup would falsely discharge the unknown loan.
                std::mem::forget((
                    f, source, phase, cohort, grant, capacity, scope, root, control, driver,
                    panic_sink,
                ));
                continue;
            }
            match running
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                Poll::Ready(Ok(Ok(end))) => end,
                _ => panic!("explicit core close did not stop opening"),
            }
        } else {
            match first {
                Poll::Ready(Ok(Ok(end))) => end,
                _ => panic!("finite single-generation executor failed"),
            }
        };
        drop(running);
        assert_eq!(
            end,
            if hanging {
                SubscriptionEnd::CoreClosed
            } else if malformed {
                SubscriptionEnd::Failed
            } else {
                SubscriptionEnd::RemoteClosed
            }
        );
        assert_eq!(
            io.opens.load(Ordering::SeqCst),
            1,
            "only AP may request another RPC/generation"
        );
        assert_eq!(
            failures.malformed.lock().unwrap().len(),
            usize::from(malformed)
        );
        if !hanging {
            assert!(
                source.reap_generation(NonZeroU64::new(2).unwrap()).is_err(),
                "RPC exit cannot retire still-live phase/frame loans"
            );
        }
        f.drain_phase(&phase);
        if hanging {
            assert!(f.seen.lock().unwrap().iter().any(|(_, seen)| *seen
                == Seen::Phase(MonotonicInstant::from_origin(Duration::from_secs(1)), None)));
        }
        if !hanging {
            assert!(f.seen.lock().unwrap().iter().any(|(_, seen)| *seen
                == Seen::Phase(
                    MonotonicInstant::from_origin(Duration::from_secs(1)),
                    Some(MonotonicInstant::from_origin(Duration::from_secs(3)))
                )));
        }
        if !hanging {
            f.consume(false);
            if !malformed {
                f.consume(false);
            }
        }
        report.consume(consumer).unwrap();
        assert!(custody.try_retire().unwrap());
        f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
        f.drain_progress();
        source.reap_generation(NonZeroU64::new(2).unwrap()).unwrap();
        source.close_new_jobs();
        source.close_after_jobs().unwrap();
        drop(source);
        drop((cohort, grant, capacity, scope));
        wc::finish(control, root, driver);
        f.finish();
    }
}

#[test]
fn two_streams_decode_out_of_order_but_fill_their_original_ready_order_before_control() {
    let f = Fixture::new(8, 4);
    let second = StreamKey {
        participation: ParticipationRef::new(
            f.key.participation.query_execution_id(),
            f.key.participation.frontend_process_id(),
            BackendProcessId::new_v7(),
        ),
        generation: f.key.generation,
    };
    f.input.register_stream(second).unwrap();
    f.open_stream(second);
    let mut first_credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
    let mut second_credit = Some(f.bank.credit(false, Some(second)).unwrap());
    let mut first_read = ready(());
    let mut second_read = ready(());
    let first = match f.input.poll_receive(
        f.key,
        &mut first_credit,
        Pin::new(&mut first_read),
        &mut Context::from_waker(Waker::noop()),
    ) {
        Poll::Ready(Ok((receipt, ()))) => receipt,
        _ => panic!("first Ready was not registered"),
    };
    let later = match f.input.poll_receive(
        second,
        &mut second_credit,
        Pin::new(&mut second_read),
        &mut Context::from_waker(Waker::noop()),
    ) {
        Poll::Ready(Ok((receipt, ()))) => receipt,
        _ => panic!("second Ready was not registered"),
    };
    f.input
        .complete_receive(&later, bookmark(second, 2), 0)
        .unwrap();
    f.input.root_control(f.bank.seal()).unwrap();
    assert!(
        f.input.0.state.lock().unwrap().in_flight.is_none(),
        "later decode cannot fill past the earlier Ready"
    );
    f.input
        .complete_receive(&first, bookmark(f.key, 1), 0)
        .unwrap();
    f.consume(false);
    f.consume(true);
    assert_eq!(f.seen.lock().unwrap()[0].1, Seen::Observations(vec![1, 2]));
    assert_eq!(f.seen.lock().unwrap()[1].1, Seen::Control);
    f.input.job_closed(second).unwrap();
    f.input.retire_stream(second).unwrap();
    f.finish();
}

#[test]
fn compact_controls_project_only_at_fill_and_metadata_refusal_keeps_original() {
    let f = Fixture::new(4, 4);
    let layout = f.bank.metadata.lock().unwrap().required.unwrap();
    assert_eq!(layout.entry, Layout::new::<Entry>());
    assert_eq!(layout.root_reference, Layout::new::<RootControlRef>());
    assert_eq!(layout.actor_input, Layout::new::<AttemptInput>());
    assert_eq!(layout.ap_projection, Layout::new::<AttemptInput>());
    assert_eq!(
        layout.logical_projection,
        Layout::new::<LogicalToAttempt<AttemptFailure>>()
    );
    assert_eq!(layout.batch_item, Layout::new::<ReceivedObservation>());
    assert_eq!(layout.in_flight, Layout::new::<InFlight>());
    assert_eq!(
        layout.failed_fill,
        Layout::new::<FailedFill<AttemptInput>>()
    );
    assert_eq!(layout.read_state, Layout::new::<ReadState>());
    assert_eq!(
        layout.read_confirmation,
        Layout::new::<ReadStateConfirmation<'static, Recorder>>()
    );
    assert_eq!(
        layout.confirmation_waiter,
        Layout::new::<Notified<'static>>()
    );
    assert_eq!(layout.read_progress, Layout::new::<ReadProgressSnapshot>());
    assert_eq!(layout.total_positions, f.bank.budget.total_positions);
    assert_eq!(
        layout.queue_peak_bytes,
        2 * layout.total_positions * Layout::new::<Entry>().size()
    );
    assert_eq!(layout.max_batch_items, 256);
    assert!(layout.root_reference.size() < layout.logical_projection.size());
    f.frame(1);
    f.input.root_control(f.bank.seal()).unwrap();
    assert_eq!(*f.bank.projections.lock().unwrap(), 0);
    assert_eq!(
        f.bank.state.lock().unwrap().control_used,
        f.bank.budget.control_bytes
    );
    f.consume(true);
    assert_eq!(*f.bank.projections.lock().unwrap(), 1);
    f.consume(true);
    f.input.job_closed(f.key).unwrap();
    f.input.retire_stream(f.key).unwrap();
    f.close_progress();
    f.bank.metadata.lock().unwrap().refuse_once = true;
    assert!(matches!(
        f.input.close_after_exit(),
        Err(ObservationError::Busy)
    ));
    assert!(f.input.0.metadata.lock().unwrap().is_some());
    assert!(!f.bank.metadata.lock().unwrap().retired);
    assert!(!f.bank.metadata.lock().unwrap().unknown);
    // The original obligation and already-closed standing roles survive retry.
    f.input.close_after_exit().unwrap();
    f.finish();
}

#[test]
fn foreign_source_control_ref_does_not_borrow_credit_or_project() {
    let f = Fixture::new(2, 2);
    let foreign = Fixture::new(2, 2);
    assert!(matches!(
        f.input.root_control(foreign.bank.seal()),
        Err(ObservationError::InvalidRootControl)
    ));
    assert_eq!(f.bank.state.lock().unwrap().control_used, 0);
    assert_eq!(*f.bank.projections.lock().unwrap(), 0);
    f.finish();
    foreign.finish();
}

#[test]
fn canonical_projection_refusal_retains_control_claim_until_real_close() {
    let f = Fixture::new(2, 2);
    f.frame(1);
    f.input.root_control(f.bank.seal()).unwrap();
    f.bank.canonical.lock().unwrap().clear();
    let input = f
        .input
        .0
        .state
        .lock()
        .unwrap()
        .in_flight
        .as_ref()
        .unwrap()
        .input;
    f.runtime.poll_one(Tick(1)).unwrap();
    f.runtime.retire_input(actor(), input).unwrap();
    assert!(matches!(
        f.input.input_retired(input),
        Err(ObservationError::InvalidRootControl)
    ));
    assert!(f.input.0.state.lock().unwrap().failed_control.is_some());
    assert_eq!(
        f.bank.state.lock().unwrap().control_used,
        f.bank.budget.control_bytes
    );
    assert!(!f.bank.metadata.lock().unwrap().retired);
    assert_eq!(*f.bank.projections.lock().unwrap(), 0);
    f.finish();
}

#[test]
fn opening_poll_snapshot_is_atomic_while_phase_delivery_can_be_late() {
    let f = Fixture::unopened(4, 4);
    let before = f
        .input
        .read_state(f.key, token(2), MonotonicInstant::ORIGIN)
        .unwrap();
    assert_eq!(before.opening_started_at, None);
    assert_eq!(before.opened_at, None);
    assert!(!before.had_liveness);
    let mut credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
    let mut during = Vec::new();
    let mut calls = 0;
    f.clock.at(1000);
    let mut opening = std::future::poll_fn(|_| {
        calls += 1;
        if calls == 2 {
            f.clock.at(7000);
        }
        during.push(
            f.input
                .read_state(f.key, token(3), MonotonicInstant::ORIGIN)
                .unwrap(),
        );
        if calls == 1 {
            Poll::Pending
        } else {
            // This real Source accepts the move-only request during the short
            // poll, retaining it until successful opening registration finishes.
            f.input.root_control(f.bank.seal()).unwrap();
            assert!(f.input.0.state.lock().unwrap().in_flight.is_none());
            Poll::Ready(Ok::<_, ObservationFailure>(()))
        }
    });
    let first = f
        .input
        .poll_open(
            f.key,
            &mut credit,
            Pin::new(&mut opening),
            &mut Context::from_waker(Waker::noop()),
        )
        .unwrap();
    assert!(first.result.is_pending());
    assert_eq!(
        first.started_at,
        MonotonicInstant::from_origin(Duration::from_secs(1))
    );
    assert_eq!(first.opened_at, None);
    let waiting = f
        .input
        .read_state(f.key, token(4), MonotonicInstant::ORIGIN)
        .unwrap();
    assert_eq!(waiting.opening_started_at, Some(first.started_at));
    assert_eq!(waiting.opened_at, None);
    assert_eq!(
        waiting.received_through_snapshot,
        ReceptionCoverage::Complete
    );
    let second = f
        .input
        .poll_open(
            f.key,
            &mut credit,
            Pin::new(&mut opening),
            &mut Context::from_waker(Waker::noop()),
        )
        .unwrap();
    assert!(matches!(second.result, Poll::Ready(Ok(()))));
    drop(opening);
    assert_eq!(during.len(), 2);
    for snapshot in during {
        assert_eq!(snapshot.opening_started_at, Some(first.started_at));
        assert_eq!(snapshot.opened_at, None);
        assert_eq!(
            snapshot.received_through_snapshot,
            ReceptionCoverage::Incomplete
        );
    }
    // No phase publisher was serviced: this independent snapshot still knows
    // the exact successful response. No five-second host policy is applied.
    let actual = f
        .input
        .read_state(f.key, token(5), MonotonicInstant::ORIGIN)
        .unwrap();
    assert_eq!(actual.opening_started_at, Some(first.started_at));
    assert_eq!(
        actual.opened_at,
        Some(MonotonicInstant::from_origin(Duration::from_secs(7)))
    );
    assert_eq!(
        actual.received_through_snapshot,
        ReceptionCoverage::Complete
    );
    assert!(!actual.had_liveness);
    f.input
        .accept(f.key, bookmark(f.key, 1), 0, credit.take().unwrap())
        .unwrap();
    f.consume(true);
    f.consume(false);
    assert_eq!(
        f.openings.lock().unwrap().as_slice(),
        &[ObservationOpening {
            started_at: first.started_at,
            opened_at: actual.opened_at
        }]
    );
    f.finish();
}

#[test]
fn failed_open_ready_keeps_original_fifo_position_before_reentrant_control() {
    use novarocks_attempt_protocol::failure::{
        AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure,
    };
    use novarocks_attempt_protocol::input::ObservationFailureKind;
    let f = Fixture::unopened(4, 4);
    let failure = ObservationFailure {
        kind: ObservationFailureKind::ProtocolViolation,
        cause: HostProjectedFailure {
            original: HostFailureRef(NonZeroU64::new(1).unwrap()),
            kind: HostFailureKind::NativeTransport,
            class: novarocks_coordination_contract::AttemptFailureClass::ContractViolation,
            site: AttemptFailureSite::Participation(f.key.participation),
        },
    };
    let mut credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
    let mut opening = std::future::poll_fn(|_| {
        f.input.root_control(f.bank.seal()).unwrap();
        Poll::Ready(Err::<(), _>(failure))
    });
    let polled = f
        .input
        .poll_open(
            f.key,
            &mut credit,
            Pin::new(&mut opening),
            &mut Context::from_waker(Waker::noop()),
        )
        .unwrap();
    let Poll::Ready(Err((receipt, original))) = polled.result else {
        panic!("opening failure lost its original Ready receipt");
    };
    assert_eq!(original, failure);
    assert!(credit.is_none());
    assert!(f.input.0.state.lock().unwrap().in_flight.is_none());
    let snapshot = f
        .input
        .read_state(f.key, token(2), MonotonicInstant::ORIGIN)
        .unwrap();
    assert_eq!(snapshot.opening_started_at, Some(MonotonicInstant::ORIGIN));
    assert_eq!(snapshot.opened_at, None);
    assert_eq!(
        snapshot.received_through_snapshot,
        ReceptionCoverage::Incomplete
    );
    f.input
        .complete_receive(&receipt, ObservationEvent::Failed(original), 0)
        .unwrap();
    f.consume(false);
    f.consume(true);
    assert_eq!(
        f.seen.lock().unwrap()[0].1,
        Seen::Observations(vec![u64::MAX])
    );
    assert_eq!(f.seen.lock().unwrap()[1].1, Seen::Control);
    assert_eq!(
        f.openings.lock().unwrap().as_slice(),
        &[ObservationOpening {
            started_at: MonotonicInstant::ORIGIN,
            opened_at: None
        }]
    );
    f.finish();
}

#[test]
fn valid_marker_at_the_same_open_tick_sets_had_liveness() {
    for catch_up in [false, true] {
        let f = Fixture::unopened(4, 4);
        let mut credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
        let mut opening = ready(Ok::<(), ObservationFailure>(()));
        let polled = f
            .input
            .poll_open(
                f.key,
                &mut credit,
                Pin::new(&mut opening),
                &mut Context::from_waker(Waker::noop()),
            )
            .unwrap();
        assert!(matches!(polled.result, Poll::Ready(Ok(()))));
        let before = f
            .input
            .read_state(f.key, token(2), MonotonicInstant::ORIGIN)
            .unwrap();
        assert!(!before.had_liveness);
        let event = if catch_up {
            ObservationEvent::Fact {
                source_revision: None,
                fact: ObservationFact::CatchUpComplete {
                    generation: f.key.generation.get(),
                    initial_cut: 0,
                },
            }
        } else {
            bookmark(f.key, 1)
        };
        f.input
            .accept(f.key, event, 0, credit.take().unwrap())
            .unwrap();
        let after = f
            .input
            .read_state(f.key, token(3), MonotonicInstant::ORIGIN)
            .unwrap();
        assert!(after.had_liveness);
        assert_eq!(after.last_liveness_at, after.opened_at.unwrap());
        f.consume(false);
        f.finish();
    }
}

#[derive(Default)]
struct ConfirmationWake(std::sync::atomic::AtomicUsize);
impl std::task::Wake for ConfirmationWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn incomplete_confirmation_waits_for_real_pending_poll_change_without_any_frame() {
    let f = Fixture::new(4, 1);
    let mut original_result = f.confirmation();
    let mut confirming = Box::pin(f.input.confirm(
        f.key,
        token(1),
        MonotonicInstant::ORIGIN,
        &mut original_result,
    ));
    let wakes = Arc::new(ConfirmationWake::default());
    let waker = Waker::from(wakes.clone());
    let mut confirmation_cx = Context::from_waker(&waker);
    let mut credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
    let original_claims = f.bank.state.lock().unwrap().claims.len();
    let mut network = std::future::poll_fn(|_| {
        assert!(confirming.as_mut().poll(&mut confirmation_cx).is_pending());
        assert!(f.seen.lock().unwrap().is_empty());
        Poll::<()>::Pending
    });
    assert!(
        f.input
            .poll_receive(
                f.key,
                &mut credit,
                Pin::new(&mut network),
                &mut Context::from_waker(Waker::noop())
            )
            .is_pending()
    );
    drop(network);
    assert!(
        wakes.0.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "clearing the actual short poll wakes the same original confirmation"
    );
    assert_eq!(
        f.bank.state.lock().unwrap().claims.len(),
        original_claims,
        "confirmation cannot borrow a second frame loan"
    );
    let input = match confirming.as_mut().poll(&mut confirmation_cx) {
        Poll::Ready(Ok(input)) => input,
        _ => panic!("actual source change did not complete original confirmation"),
    };
    drop(confirming);
    f.runtime.poll_one(Tick(1)).unwrap();
    f.runtime.retire_input(actor(), input).unwrap();
    let seen = f.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    let Seen::Snapshot(snapshot) = &seen[0].1 else {
        panic!("confirmation was replaced by an unrelated input");
    };
    assert_eq!(
        snapshot.received_through_snapshot,
        ReceptionCoverage::Complete
    );
    drop(seen);
    original_result.close_after_exit().unwrap();
    drop(credit);
    f.finish();
}

fn live_controller(
    f: &Fixture,
    error_budget: u32,
) -> novarocks_attempt_protocol::observation::controller::CoveredSubscription {
    use novarocks_attempt_protocol::{
        ids::IdSeed, input::SubscriptionPhaseSnapshot, observation::controller::*,
    };
    let mut controller =
        CoveredSubscription::new(f.key.participation, IdSeed::new(10, 1), error_budget).unwrap();
    controller.activate();
    assert_eq!(
        controller
            .drive(MonotonicInstant::ORIGIN, f.key.generation, None)
            .unwrap(),
        Some(SubscriptionDirective::Open {
            generation: f.key.generation
        })
    );
    controller
        .observe_phase(SubscriptionPhaseSnapshot {
            participation: f.key.participation,
            generation: f.key.generation,
            opening_started_at: MonotonicInstant::ORIGIN,
            opened_at: Some(MonotonicInstant::ORIGIN),
        })
        .unwrap();
    controller
}

#[test]
fn actual_long_pause_resume_uses_paid_progress_and_same_tick_revision() {
    use novarocks_attempt_protocol::observation::controller::SubscriptionDirective;
    let f = Fixture::new(4, 1);
    let held = f.bank.credit(false, Some(f.key)).unwrap();
    f.clock.at(4900);
    let mut reserving = Box::pin(f.input.reserve_read(f.key));
    let wakes = Arc::new(ConfirmationWake::default());
    let waker = Waker::from(wakes.clone());
    assert!(
        reserving
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    f.clock.at(60_000);
    let now = MonotonicInstant::from_origin(Duration::from_secs(60));
    let snapshot = f.input.read_state(f.key, token(1), now).unwrap();
    assert!(snapshot.reading_paused);
    assert_eq!(snapshot.resume_revision, 0);
    assert_eq!(snapshot.pauses.paused, Duration::from_millis(55_100));
    let mut paused_first = live_controller(&f, 3);
    let mut progress_first = live_controller(&f, 3);
    for controller in [&mut paused_first, &mut progress_first] {
        assert!(matches!(
            controller
                .drive(now, f.key.generation, Some(token(1)))
                .unwrap(),
            Some(SubscriptionDirective::Confirm(_))
        ));
    }
    paused_first.observe_read_state(snapshot).unwrap();
    assert_eq!(
        paused_first.next_deadline(now),
        Some(now.saturating_add(Duration::from_secs(5)))
    );
    let ingress = f.input.clone();
    let key = f.key;
    *f.bank.on_resume.lock().unwrap() = Some(Box::new(move |progress| {
        let current = ingress
            .read_state(key, token(9), MonotonicInstant::ORIGIN)
            .unwrap();
        assert!(
            !current.reading_paused,
            "real transition must commit before calling the foreign publisher"
        );
        assert_eq!(current.resume_revision, progress.resume_revision.get());
    }));
    drop(held);
    assert!(wakes.0.load(std::sync::atomic::Ordering::SeqCst) > 0);
    let credit = match reserving.as_mut().poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(Ok(credit)) => credit,
        _ => panic!("original read credit did not resume"),
    };
    drop(reserving);
    drop(credit);
    assert!(f.bank.state.lock().unwrap().claims.is_empty());
    assert!(
        !f.bank.stream_claims_exited(f.key),
        "same-generation progress debt is not a returned read claim"
    );
    f.drain_progress();
    let progresses = f
        .seen
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, seen)| {
            if let Seen::Progress(progress) = seen {
                Some(*progress)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(progresses.len(), 1);
    let progress = progresses[0];
    assert_eq!(progress.resumed_at, now);
    assert_eq!(progress.resume_revision.get(), 1);
    assert!(f.bank.stream_claims_exited(f.key));
    paused_first.observe_read_progress(progress).unwrap();
    progress_first.observe_read_progress(progress).unwrap();
    progress_first.observe_read_state(snapshot).unwrap();
    let measurement = now.saturating_add(Duration::from_millis(100));
    assert_eq!(paused_first.next_deadline(now), Some(measurement));
    assert_eq!(
        progress_first.next_deadline(now),
        Some(measurement),
        "older paused snapshot cannot erase same-tick actual resume"
    );
    let current = f.input.read_state(f.key, token(2), now).unwrap();
    assert!(!current.reading_paused);
    assert_eq!(current.resume_revision, progress.resume_revision.get());
    f.finish();
}

#[test]
fn closed_confirmation_waits_for_ordered_marker_and_eof_before_retry_settlement() {
    use novarocks_attempt_protocol::observation::controller::{
        ObservationDisposition, SubscriptionDirective, SubscriptionStatus,
    };
    let f = Fixture::new(4, 4);
    let mut controller = live_controller(&f, 1);
    f.input.root_control(f.bank.seal()).unwrap();
    let barrier = f.consume(false);
    f.clock.at(5000);
    f.frame(1);
    f.input
        .accept(
            f.key,
            ObservationEvent::Closed,
            0,
            f.bank.credit(false, Some(f.key)).unwrap(),
        )
        .unwrap();
    let frames = f
        .input
        .0
        .state
        .lock()
        .unwrap()
        .queue
        .iter()
        .filter_map(|entry| {
            if let EntryValue::Observation(frame) = &entry.value {
                Some(frame.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(frames.len(), 2);
    let now = MonotonicInstant::from_origin(Duration::from_secs(5));
    assert!(matches!(
        controller
            .drive(now, f.key.generation, Some(token(1)))
            .unwrap(),
        Some(SubscriptionDirective::Confirm(_))
    ));
    let mut result = f.confirmation();
    let mut confirming = Box::pin(f.input.confirm(f.key, token(1), now, &mut result));
    let confirmation = match confirming
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Ok(id)) => id,
        _ => panic!("independent closed confirmation waited for the common barrier"),
    };
    drop(confirming);
    f.runtime.poll_one(Tick(1)).unwrap();
    f.runtime.retire_input(actor(), confirmation).unwrap();
    let snapshot = f
        .seen
        .lock()
        .unwrap()
        .iter()
        .find_map(|(_, seen)| {
            if let Seen::Snapshot(snapshot) = seen {
                Some(*snapshot)
            } else {
                None
            }
        })
        .unwrap();
    assert!(snapshot.closed && snapshot.had_liveness);
    controller.observe_read_state(snapshot).unwrap();
    assert_eq!(
        controller.status(),
        SubscriptionStatus::Live,
        "Closed is not ordinary retry-budget settlement"
    );
    assert_eq!(
        controller
            .drive(now, f.key.generation, Some(token(2)))
            .unwrap(),
        None
    );
    result.close_after_exit().unwrap();
    f.input.root_control_processed(barrier).unwrap();
    f.consume(false);
    assert!(matches!(
        controller.observe(frames[0].clone()).unwrap(),
        ObservationDisposition::ApplyEntity(_)
    ));
    assert_eq!(controller.status(), SubscriptionStatus::Live);
    assert_eq!(
        controller.observe(frames[1].clone()).unwrap(),
        ObservationDisposition::Ignore
    );
    assert_eq!(
        controller.status(),
        SubscriptionStatus::BudgetExhausted,
        "only the original ordered EOF spends the one failure budget"
    );
    f.finish();
}

#[test]
fn frontend_host_retires_ordered_control_before_releasing_its_suffix() {
    run_frontend_ordered_control(None);
}

#[test]
fn ordered_host_runs_pending_seal_cut_through_the_attempt_core() {
    for finished_first in [true, false] {
        run_frontend_ordered_control(Some(finished_first));
    }
}

struct ProtocolUnits(usize);

impl novarocks_attempt_protocol::capacity::StepContext for ProtocolUnits {
    fn charge(&mut self, units: usize) -> bool {
        let Some(remaining) = self.0.checked_sub(units) else {
            return false;
        };
        self.0 = remaining;
        true
    }

    fn reserve_queue_stage(
        &mut self,
        _: novarocks_attempt_protocol::config::DispatchLane,
        _: bool,
        _: BackendProcessId,
        _: usize,
        _: usize,
    ) -> novarocks_attempt_protocol::capacity::QueueStage {
        panic!("the ordered-only fixture has no Native dispatch")
    }
}

#[derive(Debug, Eq, PartialEq)]
enum ProtocolObservation {
    Batch(InputId, Vec<(ParticipationRef, u64)>),
    FirstSealProcessing(InputId),
    SealResult(bool),
}

struct ProtocolOrder {
    core: novarocks_attempt_protocol::Core,
    sources: novarocks_attempt_protocol::start::AttemptSources,
    continuation: u64,
    observed: Arc<Mutex<Vec<ProtocolObservation>>>,
}

impl ProtocolOrder {
    fn apply(
        &mut self,
        now: MonotonicInstant,
        source: novarocks_attempt_protocol::input::AttemptSourceId,
        input: novarocks_attempt_protocol::input::AttemptInputId,
        value: AttemptInput,
        observed_tail: Option<novarocks_attempt_protocol::input::AttemptInputId>,
    ) -> novarocks_attempt_protocol::AttemptStep {
        use novarocks_attempt_protocol::{
            AttemptCore,
            input::{AttemptInputClaim, AttemptSourceCut},
        };
        self.core
            .handle(
                now,
                AttemptInputClaim {
                    source,
                    input,
                    native_cut: Some(AttemptSourceCut {
                        source: self.sources.native,
                        tail: None,
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: self.sources.observation,
                        tail: observed_tail,
                    }),
                },
                value,
                &mut ProtocolUnits(64),
            )
            .unwrap()
    }

    fn next_continuation(&mut self) -> novarocks_attempt_protocol::input::AttemptInputId {
        self.continuation += 1;
        novarocks_attempt_protocol::input::AttemptInputId(
            NonZeroU64::new(self.continuation).unwrap(),
        )
    }
}

impl crate::control_runtime::actor::TestActor for ProtocolOrder {
    fn handle(
        &mut self,
        at: Tick,
        claim: InputEnvelope,
        input: crate::control_runtime::actor::FrontendInput,
        step: &mut ActorStep<'_, crate::control_runtime::actor::FrontendEffect>,
    ) {
        use crate::control_runtime::actor::{FrontendEffect, FrontendInput};
        use novarocks_attempt_protocol::{
            InputSelection,
            effect::{EffectBody, Publication},
            input::{AttemptInputId, PublicationApplied},
        };
        use novarocks_control_runtime_core::effect::EffectKind;
        use novarocks_coordination_contract::AttemptToLogicalBody;

        // The ordered observations and Seal run through Core. The fixture has
        // already bootstrapped it and records only custody of the neutral hints.
        let input = match input {
            FrontendInput::AttemptStartup(_) => {
                assert_eq!(claim.class, InputClass::Lifecycle);
                return;
            }
            FrontendInput::Membership(update) => {
                assert_eq!(claim.class, InputClass::Ordinary);
                assert_eq!(update.0.revision, 1);
                assert_eq!(update.0.processes.len(), 2);
                assert!(update.0.processes.values().all(|fact| {
                    *fact == novarocks_attempt_protocol::input::MembershipObservation::ExactOriginal
                }));
                return;
            }
            FrontendInput::CapacityReleased => {
                assert_eq!(claim.class, InputClass::Ordinary);
                return;
            }
            FrontendInput::Attempt(input) => input,
            _ => panic!("the fixture consumes the original ordered input"),
        };
        if let AttemptInput::Executor(ExecutorFact::ReadProgress(progress)) = input.as_ref() {
            assert_eq!(claim.class, InputClass::Ordinary);
            assert_eq!(progress.resume_revision, NonZeroU64::MIN);
            return;
        }
        let input_id = AttemptInputId(NonZeroU64::new(claim.input.get()).unwrap());
        if let AttemptInput::Executor(ExecutorFact::Observations(batch)) = input.as_ref() {
            let keys = batch
                .items()
                .iter()
                .map(|frame| {
                    let sequence = match &frame.event {
                        ObservationEvent::Fact {
                            fact: ObservationFact::Bookmark { sequence, .. },
                            ..
                        } => *sequence,
                        ObservationEvent::Fact {
                            fact: ObservationFact::Status(_),
                            ..
                        } => 0,
                        _ => panic!("unexpected ordered fixture observation"),
                    };
                    (frame.participation, sequence)
                })
                .collect();
            self.observed
                .lock()
                .unwrap()
                .push(ProtocolObservation::Batch(claim.input, keys));
        }
        let now = MonotonicInstant::from_origin(Duration::from_nanos(at.0));
        let mut result = self.apply(
            now,
            self.sources.observation,
            input_id,
            *input,
            Some(input_id),
        );
        if let Some(processed) = result.root_control_processed {
            assert_eq!(processed, input_id);
            self.observed
                .lock()
                .unwrap()
                .push(ProtocolObservation::FirstSealProcessing(claim.input));
            let author = step.input_author().unwrap().clone();
            step.effect_from_author(
                &author,
                crate::task_execution::host::adapter::descriptor(
                    result.notice_action.expect("original Core notice").get(),
                    author.work(),
                    EffectKind::Settlement,
                ),
                FrontendEffect::AttemptRootControlProcessed(claim.input),
            )
            .unwrap();
        }
        // Model external publication delivery and the Core timer only. The
        // production ordered writer and Host must still retire the control.
        for _ in 0..64 {
            let applied = result.effect.take().and_then(|effect| {
                let applied = match effect.body {
                    EffectBody::Publish(Publication::ProtocolBatch(_)) => {
                        PublicationApplied::ProtocolBatch { published: true }
                    }
                    EffectBody::Publish(Publication::RootStatus(_)) => {
                        PublicationApplied::RootStatus
                    }
                    EffectBody::Port(port) => {
                        if let AttemptToLogicalBody::SealResult {
                            token: reply,
                            result,
                        } = port.body
                        {
                            assert_eq!(reply, token(1));
                            self.observed
                                .lock()
                                .unwrap()
                                .push(ProtocolObservation::SealResult(result.is_ok()));
                            return Some((effect.id, PublicationApplied::SealResult));
                        }
                        assert!(matches!(
                            port.body,
                            AttemptToLogicalBody::AttemptTerminal(_)
                        ));
                        return None;
                    }
                    EffectBody::AdjustReadWindow(_) | EffectBody::ReleaseCreationPayload { .. } => {
                        return None;
                    }
                    other => panic!("unexpected ordered fixture effect: {other:?}"),
                };
                Some((effect.id, applied))
            });
            let (source, value) = match (result.selection, applied) {
                (InputSelection::Only(source), Some((effect, applied)))
                    if source == self.sources.publication =>
                {
                    (
                        source,
                        AttemptInput::Executor(ExecutorFact::PublicationApplied {
                            effect,
                            result: Ok(applied),
                        }),
                    )
                }
                (InputSelection::Only(source), None) if source == self.sources.actor => {
                    (source, AttemptInput::Timer)
                }
                (InputSelection::Only(source), None) if source == self.sources.observation => {
                    return;
                }
                (InputSelection::Prefer(source), None) if source == self.sources.native => return,
                other => panic!("unexpected continuation after ordered fixture input: {other:?}"),
            };
            let id = self.next_continuation();
            result = self.apply(now, source, id, value, None);
        }
        panic!("the original ordered fixture input did not finish");
    }
}

fn ordered_protocol_fixture(
    observed: Arc<Mutex<Vec<ProtocolObservation>>>,
    sources: novarocks_attempt_protocol::start::AttemptSources,
) -> (
    ProtocolOrder,
    AttemptActivationIdentity,
    TaskIdentity,
    TaskIdentity,
    [StreamKey; 2],
) {
    use crate::task_execution::host::adapter::tests::start;
    use novarocks_attempt_protocol::{AttemptCore, Core, start::PayloadRef};
    use novarocks_coordination_contract::AttemptActionId;
    let mut start = start();
    let root = start.plan.graph.root;
    let root_task = start.plan.graph.tasks[&root.task_id()].clone();
    let backend = BackendProcessId::new_v7();
    let sibling = TaskIdentity::new(
        root.query_execution_id(),
        StageId::new(root.stage_id().get() + 1).unwrap(),
        TaskId::new(root.task_id().get() + 1).unwrap(),
        backend,
    );
    let sibling_participation = ParticipationRef::new(
        root.query_execution_id(),
        root_task.participation.frontend_process_id(),
        backend,
    );
    let root_backend = root.backend_process_id();
    let mut sibling_task = root_task.clone();
    sibling_task.identity = sibling;
    sibling_task.participation = sibling_participation;
    sibling_task.creation.resource = PayloadRef(NonZeroU64::new(3).unwrap());
    start
        .plan
        .graph
        .tasks
        .insert(sibling.task_id(), sibling_task);
    start
        .plan
        .graph
        .stages
        .insert(sibling.stage_id(), Box::new([sibling]));
    let mut material = start.plan.establish_material[&root_task.participation];
    material.material = PayloadRef(NonZeroU64::new(4).unwrap());
    start
        .plan
        .establish_material
        .insert(sibling_participation, material);
    start
        .plan
        .admission_epochs
        .insert(backend, start.plan.admission_epochs[&root_backend]);
    start
        .plan
        .preparation_positions
        .insert(backend, start.plan.preparation_positions[&root_backend]);
    start.sources = sources;

    let activation = start.activation.identity;
    let mut core = <Core as AttemptCore>::new(start).unwrap();
    let streams = [root_task.participation, sibling_participation].map(|participation| {
        let generation = core
            .open_subscription_for_test(
                participation,
                AttemptActionId::new(NonZeroU64::new(99).unwrap()),
                token(99),
                &mut ProtocolUnits(64),
            )
            .unwrap();
        StreamKey {
            participation,
            generation,
        }
    });
    let mut protocol = ProtocolOrder {
        core,
        sources,
        observed,
        continuation: 1 << 32,
    };
    let id = protocol.next_continuation();
    protocol.apply(
        MonotonicInstant::ORIGIN,
        sources.actor,
        id,
        AttemptInput::Bootstrap,
        None,
    );
    (protocol, activation, root, sibling, streams)
}

fn run_frontend_ordered_control(finished_first: Option<bool>) {
    use crate::control_runtime::{
        actor::{FrontendActor, FrontendActorKind, FrontendEffect, FrontendInput, TestActor},
        host::{
            EffectExecutor, EffectSource, FrontendLanePolicy, HostBinding, HostRegistry,
            PendingHandoff, PreparedEffectJob, SourceProgress,
        },
        membership::{
            AttemptMembershipSource, FrozenMember, MembershipRegistry, MembershipTarget,
            membership_input,
        },
        metrics::FrontendHostObservations,
    };
    use crate::{
        native::transport_capacity::{
            NativeTransportCapacity, TransportCapacityLane, TransportCapacityRefusal,
        },
        task_execution::host::{
            adapter::{AttemptCapacitySource, descriptor},
            geometry::AttemptHostGeometry,
            observations::{host_turn::OrderedObservationHost, read_progress::ReadProgressInput},
            startup::AttemptStartupSource,
            transport::send_queue_resources,
        },
    };
    use novarocks_attempt_protocol::config::CoordinationBudgets;
    use novarocks_control_runtime::{
        actor_turn::{ActorHostTurn, EffectTicket, InputPhase, ProcessClock},
        coalescing_input::CoalescingInput,
        transaction::RuntimeError,
    };
    use novarocks_control_runtime_core::{capacity::CapacityWaitTarget, effect::EffectKind};
    use novarocks_execution_contract::RuntimeEndpoint;
    use novarocks_query_application::api::{
        BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
    };
    use std::num::NonZeroUsize;

    struct CurrentProcesses([(BackendProcessId, RuntimeEndpoint); 2]);

    impl BackendProcessObservationPort for CurrentProcesses {
        fn observe_process_at_endpoint(
            &self,
            process: BackendProcessId,
            endpoint: &RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, BackendTopologyError> {
            assert!(
                self.0
                    .iter()
                    .any(|(expected, original)| *expected == process && original == endpoint)
            );
            Ok(BackendProcessObservation::Current)
        }
    }

    struct Core(Arc<Mutex<Vec<&'static str>>>);
    impl TestActor for Core {
        fn handle(
            &mut self,
            _: Tick,
            claim: InputEnvelope,
            input: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            let FrontendInput::Attempt(input) = input else {
                panic!("original attempt input");
            };
            match *input {
                AttemptInput::Executor(ExecutorFact::Observations(batch)) => {
                    let mut seen = self.0.lock().unwrap();
                    for frame in batch.into_items() {
                        let ObservationEvent::Fact {
                            fact: ObservationFact::Bookmark { sequence, .. },
                            ..
                        } = frame.event
                        else {
                            panic!("original bookmark");
                        };
                        seen.push(if sequence == 1 { "prefix" } else { "suffix" });
                    }
                }
                AttemptInput::Executor(ExecutorFact::ReadProgress(progress)) => {
                    self.0
                        .lock()
                        .unwrap()
                        .push(match progress.resume_revision.get() {
                            1 => "first resume",
                            2 => "coalesced resume",
                            _ => panic!("unexpected read resume"),
                        });
                }
                AttemptInput::Port(LogicalToAttempt {
                    body: LogicalToAttemptBody::SealRequest { .. },
                    ..
                }) => {
                    self.0.lock().unwrap().push("first seal decision");
                    let author = step.input_author().unwrap().clone();
                    step.effect_from_author(
                        &author,
                        descriptor(1, author.work(), EffectKind::Settlement),
                        FrontendEffect::AttemptRootControlProcessed(claim.input),
                    )
                    .unwrap();
                }
                _ => panic!("unexpected attempt input"),
            }
        }
    }

    struct Source(bool);
    impl EffectSource for Source {
        fn prepay_result(
            &mut self,
            _: &FrontendEffect,
        ) -> Option<CausalInputObligation<FrontendActor>> {
            panic!("a control processing notice needs no job or result loan");
        }

        fn bind_job(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &ActorHostTurn<FrontendActor>,
            _: &EffectTicket<FrontendEffect>,
            _: CausalInputObligation<FrontendActor>,
        ) -> PreparedEffectJob {
            unreachable!()
        }

        fn return_unclaimed_result(&mut self, _: CausalInputObligation<FrontendActor>) {
            unreachable!()
        }

        fn core_consumed(&mut self, _: &[InputId]) {}

        fn owner_failed_pending(&mut self, _: PendingHandoff) {
            unreachable!()
        }

        fn close_external(&mut self) {
            self.0 = true;
        }

        fn result_admission_closed(&self) -> bool {
            self.0
        }

        fn progress_retirement(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &ActorHostTurn<FrontendActor>,
            _: Tick,
            _: usize,
        ) -> SourceProgress {
            SourceProgress {
                units: 0,
                progressed: false,
                more: false,
                retired: self.0,
            }
        }
    }

    struct NoJobs;
    impl EffectExecutor for NoJobs {
        fn handoff(&self, _: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
            panic!("ordered observations need no job handoff");
        }
    }

    use novarocks_control_runtime::history::HistoryCapture;
    use novarocks_control_runtime_core::history::{EffectHistoryPhase, HistoryFact};
    let runtime_history = HistoryCapture::new(4096);
    let layout = finished_first.map(|_| {
        NativeTransportCapacity::from_transport(
            novarocks_task_codec::TransportBudget::new(
                1,
                128,
                64,
                2,
                1024,
                4,
                4096,
                1,
                2,
                Duration::from_secs(1),
            )
            .unwrap(),
        )
        .unwrap()
    });
    let ledger = match &layout {
        Some(layout) => RuntimeLedger::with_keyed_families_and_observation(
            layout.process_limits(),
            layout.keyed_families(),
            [],
            Some(runtime_history.clone()),
            Default::default(),
        )
        .unwrap(),
        None => RuntimeLedger::with_history(ResourceVector::default(), runtime_history.clone()),
    };
    let generation = Generation::new(1).unwrap();
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let runtime = ActorRuntime::with_history(runtime_history.clone());
    let sources = layout.as_ref().map(|layout| {
        AttemptHostGeometry::derive(
            scope.id(),
            NonZeroUsize::new(2).unwrap(),
            CoordinationBudgets::DEFAULT.dispatch,
            layout,
            &[],
            false,
        )
        .unwrap()
        .sources
    });
    let mailbox = sources.map_or([4; 4], |sources| sources.mailbox);
    assert_eq!(mailbox[InputClass::Ordinary.index()], 4);
    let config = ActorConfig {
        mailbox,
        // A single committed batch lets the test inspect both sides of the
        // real Host retirement before the next actor step is admitted.
        max_batches: if sources.is_some() { 1 } else { 4 },
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: if sources.is_some() { 16 } else { 8 },
        max_step_work: 1,
    };
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let (test_actor, activation, root, key, sibling) = if let Some(sources) = sources {
        let (protocol, activation, root, sibling, [first, second]) =
            ordered_protocol_fixture(observed.clone(), sources);
        (
            FrontendActor::Test(Box::new(protocol)),
            activation,
            root,
            first,
            Some((sibling, second)),
        )
    } else {
        let key = key();
        let query = key.participation.query_execution_id();
        let activation = AttemptActivationIdentity::from_parts(1, query, 1);
        let root = TaskIdentity::new(
            query,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            key.participation.backend_process_id(),
        );
        (
            FrontendActor::Test(Box::new(Core(seen.clone()))),
            activation,
            root,
            key,
            None,
        )
    };
    let source_id = |source: novarocks_attempt_protocol::input::AttemptSourceId| {
        ActorId::new(source.0.get()).unwrap()
    };
    let source = sources.map_or_else(|| ActorId::new(2).unwrap(), |s| source_id(s.observation));
    let subscription_source = sources.map_or(source, |s| source_id(s.subscription));
    let registration = runtime
        .register_bound(actor(), scope.clone(), test_actor, config)
        .unwrap_or_else(|_| panic!("original ordered actor"));
    let budget = if sibling.is_some() {
        ObservationBudget {
            queued_frames: 260,
            total_positions: 262,
            max_frame_bytes: 2048,
            total_bytes: 2048 * 262 + 512,
            control_bytes: 512,
        }
    } else {
        ObservationBudget {
            queued_frames: 3,
            total_positions: 5,
            max_frame_bytes: 1024,
            total_bytes: 4608,
            control_bytes: 512,
        }
    };
    let (mut work, mut reservation) = scope
        .reserve_next(
            generation,
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    inputs: budget.total_positions,
                    ..Default::default()
                },
            ),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    work.start().unwrap();
    let positions = (0..budget.total_positions)
        .map(|_| {
            let mut original = reservation.take_position(OwnedSlotKind::Input).unwrap();
            original.bind().unwrap();
            original
        })
        .collect();
    let binding =
        RootControlBinding::for_registered_source(registration.clone(), activation, root).unwrap();
    let bank = Bank::make(budget, positions, binding.clone());
    let standing = |source, class| {
        runtime
            .reserve_admitted_input_for(&registration, source, class, &work)
            .unwrap()
            .into_standing_input()
            .unwrap()
    };
    let ingress = OrderedObservationInput::new(
        standing(source, InputClass::Lifecycle),
        standing(source, InputClass::Control),
        bank.clone(),
        binding,
        Arc::new(ManualClock::default()),
        |input| FrontendInput::Attempt(Box::new(input)),
        Waker::noop().clone(),
    )
    .unwrap();
    let streams = std::iter::once(key).chain(sibling.map(|(_, key)| key));
    let progress_roles = streams
        .clone()
        .map(|key| {
            ReadProgressInput::new(
                &registration,
                key.participation,
                standing(subscription_source, InputClass::Ordinary),
            )
        })
        .collect::<Vec<_>>();
    let read_progress = &progress_roles[0];
    let membership_registry = Arc::new(MembershipRegistry::new());
    let endpoints = [19091, 19092].map(|port| RuntimeEndpoint::new("127.0.0.1", port).unwrap());
    let mut startup_witness = None;
    let mut capacity_wait = None;
    let mut membership_slot = None;
    let mut capacity_slot = None;
    let (startup, capacity, membership) = if let Some(sources) = sources {
        let (startup, witness) = AttemptStartupSource::new(
            registration.clone(),
            standing(source_id(sources.actor), InputClass::Lifecycle),
        );
        startup_witness = Some(witness);
        let registrant = Arc::new(scope.capacity_registrant(Waker::noop().clone()).unwrap());
        capacity_wait = Some(registrant.clone());
        assert_eq!(source_id(sources.actor), actor());
        let capacity_input = standing(source_id(sources.capacity), InputClass::Ordinary);
        capacity_slot = Some(capacity_input.id());
        let capacity = AttemptCapacitySource::new(registration.clone(), capacity_input, registrant);
        let membership_input_slot = standing(source_id(sources.membership), InputClass::Ordinary);
        membership_slot = Some(membership_input_slot.id());
        let members = streams
            .zip(endpoints.iter().cloned())
            .map(|(key, endpoint)| FrozenMember {
                participation: key.participation,
                endpoint,
            })
            .collect::<Vec<_>>()
            .into();
        let member = MembershipTarget::register(
            registration.clone(),
            members,
            CoalescingInput::new(membership_input_slot, membership_input),
        );
        (
            Some(startup),
            Some(capacity),
            Some((
                AttemptMembershipSource::new(membership_registry.clone(), member.target.clone()),
                member,
            )),
        )
    } else {
        (None, None, None)
    };
    // The logical-side registration owns the original detailed membership errors.
    let (membership, _membership_registration) = match membership {
        Some((source, registration)) => (Some(source), Some(registration)),
        None => (None, None),
    };
    let host = HostRegistry::new(
        runtime.clone(),
        ledger.clone(),
        Arc::new(FrontendHostObservations::new(Arc::new(
            ProcessClock::default(),
        ))),
    );
    let exit = host
        .register(HostBinding {
            scope: scope.clone(),
            registration: registration.clone(),
            kind: FrontendActorKind::Test,
            config,
            policy: FrontendLanePolicy::attempt(1),
            source: Box::new(Source(false)),
            executor: Arc::new(NoJobs),
            transport: None,
            startup,
            capacity,
            membership,
            owner_controls: None,
            ordered_observations: Some(
                OrderedObservationHost::new(ingress.clone())
                    .with_read_progress(progress_roles.iter().cloned()),
            ),
        })
        .unwrap_or_else(|_| panic!("original observation host"));
    let drive = || {
        let mut turns = 0;
        while let Some(turn) = runtime.claim_host_actor() {
            turns += 1;
            assert!(turns < 16, "ordered host progress must be finite");
            host.drive(Tick(1), turn);
        }
    };
    if let Some(witness) = startup_witness {
        drive();
        for _ in 0..2 {
            assert!(runtime.poll_one(Tick(1)).unwrap());
            drive();
        }
        assert!(witness.ready_for(&registration));
        assert!(!membership_registry.is_empty());
    }
    let open = |key| {
        ingress.register_stream(key).unwrap();
        let mut credit = Some(bank.credit(false, Some(key)).unwrap());
        let mut opened = ready(Ok::<(), ObservationFailure>(()));
        assert!(matches!(
            ingress
                .poll_open(
                    key,
                    &mut credit,
                    Pin::new(&mut opened),
                    &mut Context::from_waker(Waker::noop()),
                )
                .unwrap()
                .result,
            Poll::Ready(Ok(()))
        ));
    };
    open(key);

    let queue_parent = reservation.id();
    let mut finish = || {
        for role in &progress_roles {
            role.close_after_exit().unwrap();
        }

        ingress.job_closed(key).unwrap();
        ingress.retire_stream(key).unwrap();
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        reservation.finish().unwrap();
        scope.close_external().unwrap();
        bank.metadata.lock().unwrap().refuse_once = true;
        assert_eq!(ingress.close_after_exit(), Err(ObservationError::Busy));
        scope.reap_ready(budget.total_positions + 8).unwrap();
        if layout.is_none() {
            assert!(scope.join_ready().unwrap());
        }
        exit.request_close().unwrap();
        drive();
        assert!(!host.is_empty());
        ingress.close_after_exit().unwrap();
        bank.retire_positions();
        scope.reap_ready(budget.total_positions + 8).unwrap();
        runtime.notify_host_actor(&registration).unwrap();
        drive();
        assert!(membership_registry.is_empty());
        assert!(ledger.try_keyed_usage_snapshot().unwrap().is_empty());
        assert_eq!(ledger.capacity_waiter_count(), 0);
        assert!(host.is_empty() && runtime.is_empty() && ledger.is_empty());
    };

    if let (Some(finished_first), Some((sibling, other))) = (finished_first, sibling) {
        use novarocks_execution_contract::{
            AbortCause, TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion,
            TerminationDetail,
        };
        open(other);
        let accept = |key, event| {
            ingress
                .accept(key, event, 32, bank.credit(false, Some(key)).unwrap())
                .unwrap();
        };
        // The status suffix is still part of the initial covered transfer.
        // These markers advance liveness without claiming a covered prefix.
        let marker = |key: StreamKey, sequence| ObservationEvent::Fact {
            source_revision: None,
            fact: ObservationFact::Bookmark {
                generation: key.generation.get(),
                sequence,
                covered_prefix: 0,
                source_cut: 0,
            },
        };
        accept(key, marker(key, 1));
        let read = |key| {
            let mut frame = ready(());
            let mut credit = Some(bank.credit(false, Some(key)).unwrap());
            match ingress.poll_receive(
                key,
                &mut credit,
                Pin::new(&mut frame),
                &mut Context::from_waker(Waker::noop()),
            ) {
                Poll::Ready(Ok((receipt, ()))) => receipt,
                _ => panic!("the original receipt did not enter the ordered writer"),
            }
        };
        let earlier = read(key);
        let later = read(other);
        ingress
            .complete_receive(&later, marker(other, 3), 32)
            .unwrap();
        ingress
            .complete_receive(&earlier, marker(key, 2), 32)
            .unwrap();
        for sequence in 4..=258 {
            let key = if sequence % 2 == 0 { key } else { other };
            accept(key, marker(key, sequence));
        }
        let mut progress_receipts = Vec::new();
        for (role, key) in progress_roles.iter().zip([key, other]) {
            progress_receipts.push(
                role.publish(ReadProgressSnapshot {
                    participation: key.participation,
                    generation: key.generation,
                    resumed_at: MonotonicInstant::ORIGIN,
                    resume_revision: NonZeroU64::MIN,
                })
                .unwrap(),
            );
        }
        let target = key.participation.backend_process_id();
        let [root_endpoint, sibling_endpoint] = endpoints;
        let membership_processes = CurrentProcesses([
            (target, root_endpoint),
            (other.participation.backend_process_id(), sibling_endpoint),
        ]);
        membership_registry.publish_for_test(1, &membership_processes);
        let layout = layout.as_ref().unwrap();
        let registrant = capacity_wait.as_ref().unwrap();
        let queue_resources = |items| {
            send_queue_resources(layout, target, TransportCapacityLane::Ordinary, items, 32)
                .unwrap()
        };
        let blocker = scope
            .reserve_next(
                generation,
                queue_resources(2),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap();
        let wait_target = match scope.reserve_child_next_or_wait(
            generation,
            queue_parent,
            queue_resources(1),
            WorkKind::Business,
            registrant,
        ) {
            Err(RuntimeError::CapacityDenied(denial)) => {
                assert!(matches!(
                    NativeTransportCapacity::classify_denial(denial).unwrap(),
                    TransportCapacityRefusal::TargetFull(backend) if backend == target
                ));
                denial.wait_target()
            }
            _ => panic!("the exact backend queue must deny a third item"),
        };
        assert!(matches!(wait_target, CapacityWaitTarget::Keyed { .. }));
        assert_eq!(ledger.capacity_waiter_count(), 1);
        drop(blocker);
        assert_eq!(ledger.capacity_waiter_count(), 0);

        let ordinary_inventory = || {
            runtime
                .standing_input_inventory(actor(), None, 16)
                .unwrap()
                .into_iter()
                .filter(|row| row.class == InputClass::Ordinary)
                .collect::<Vec<_>>()
        };
        let filled_ordinary = || {
            let roles = ordinary_inventory();
            let sources = sources.unwrap();
            let expected = [
                (progress_roles[0].standing_id(), subscription_source),
                (progress_roles[1].standing_id(), subscription_source),
                (membership_slot.unwrap(), source_id(sources.membership)),
                (capacity_slot.unwrap(), source_id(sources.capacity)),
            ];
            assert_eq!(roles.len(), mailbox[InputClass::Ordinary.index()]);
            for (standing, source) in expected {
                let row = roles
                    .iter()
                    .find(|row| row.standing_input_id == standing)
                    .unwrap();
                assert_eq!(row.source, source);
                let input = row.in_flight.expect("the original Ordinary role is filled");
                let custody = runtime.input_custody(actor(), input).unwrap().unwrap();
                assert_eq!(custody.standing_input_id, Some(standing));
                assert_eq!(custody.phase, InputPhase::Filled);
                assert_eq!(custody.class, InputClass::Ordinary);
                assert_eq!(custody.source, source);
                assert!(!custody.runtime_metadata);
            }
            roles
        };
        let ordinary = filled_ordinary();
        ingress.root_control(bank.seal()).unwrap();
        assert_eq!(filled_ordinary(), ordinary);
        let status = |task, state, termination| ObservationEvent::Fact {
            source_revision: None,
            fact: ObservationFact::Status(
                TaskStatus::try_new_with_installed(
                    task,
                    TaskStatusVersion::FIRST,
                    state,
                    termination,
                    TaskOutputFacts::new(state == TaskState::Finished),
                    true,
                )
                .unwrap(),
            ),
        };
        let finished = (key, status(root, TaskState::Finished, None));
        let failed = (
            other,
            status(
                sibling,
                TaskState::Aborted,
                Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
            ),
        );
        for (key, event) in if finished_first {
            [finished, failed]
        } else {
            [failed, finished]
        } {
            accept(key, event);
        }

        for _ in 0..3 {
            assert!(runtime.poll_one(Tick(1)).unwrap());
            drive();
        }
        assert!(runtime.poll_one(Tick(1)).unwrap());
        let prefix = observed.lock().unwrap();
        let [
            ProtocolObservation::Batch(first_id, first),
            ProtocolObservation::Batch(batch_id, batch),
            ProtocolObservation::Batch(last_id, last),
            ProtocolObservation::FirstSealProcessing(control),
        ] = prefix.as_slice()
        else {
            panic!("the Core did not first process the pending seal after the 256-frame cut");
        };
        assert_eq!(first.as_slice(), [(key.participation, 1)]);
        assert_eq!(batch.len(), 256);
        assert_eq!(
            batch[..2],
            [(key.participation, 2), (other.participation, 3)]
        );
        assert_eq!(last.as_slice(), [(key.participation, 258)]);
        let mut expected_inputs = vec![*first_id, *batch_id, *last_id, *control];
        let control = *control;
        drop(prefix);
        assert!(runtime.input_custody(actor(), control).unwrap().is_some());
        assert_eq!(filled_ordinary(), ordinary);
        {
            let writer = ingress.0.state.lock().unwrap();
            let original = writer.in_flight.as_ref().unwrap();
            assert_eq!(original.input, control);
            assert!(original.control && !original.finished());
            assert_eq!(writer.queue.len(), 2);
        }
        drive();
        assert!(runtime.input_custody(actor(), control).unwrap().is_none());
        assert_eq!(filled_ordinary(), ordinary);
        assert!(
            runtime.poll_one(Tick(1)).unwrap(),
            "pending Seal cannot block its evidence"
        );
        drive();
        let history = observed.lock().unwrap();
        let [
            ProtocolObservation::Batch(suffix_id, suffix),
            ProtocolObservation::SealResult(success),
        ] = &history[4..]
        else {
            panic!("expected one suffix batch and one decision from the actual Core: {history:?}");
        };
        assert_eq!(suffix.len(), 2);
        assert_eq!(*success, finished_first);
        expected_inputs.push(*suffix_id);
        drop(history);
        assert_eq!(filled_ordinary(), ordinary);
        for _ in &ordinary {
            assert!(runtime.poll_one(Tick(1)).unwrap());
            drive();
        }
        assert!(progress_receipts.iter().all(|receipt| receipt.retired()));
        let idle_roles = ordinary
            .iter()
            .copied()
            .map(|mut role| {
                role.in_flight = None;
                role
            })
            .collect::<Vec<_>>();
        assert_eq!(ordinary_inventory(), idle_roles);
        let ordinary_inputs = ordinary
            .iter()
            .map(|row| row.in_flight.unwrap())
            .collect::<Vec<_>>();
        for input in &ordinary_inputs {
            assert!(runtime.input_custody(actor(), *input).unwrap().is_none());
        }
        let history = runtime_history.drain(4096);
        assert!(
            history.complete && history.remaining == 0,
            "{:?}",
            history.error
        );
        let consumed = history
            .entries
            .iter()
            .filter_map(|entry| match &entry.fact {
                HistoryFact::InputConsumed { input, .. } if expected_inputs.contains(input) => {
                    Some(*input)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(consumed, expected_inputs);
        let control_completion = history
            .entries
            .iter()
            .filter_map(|entry| match &entry.fact {
                HistoryFact::Effect {
                    input,
                    phase: EffectHistoryPhase::Completed,
                    ..
                } if *input == control => Some(entry.ordinal),
                _ => None,
            })
            .collect::<Vec<_>>();
        let suffix_consumption = history
            .entries
            .iter()
            .find_map(|entry| match &entry.fact {
                HistoryFact::InputConsumed { input, .. }
                    if Some(input) == expected_inputs.last() =>
                {
                    Some(entry.ordinal)
                }
                _ => None,
            })
            .expect("the original suffix was consumed");
        let [completed] = control_completion.as_slice() else {
            panic!("the Host must consume the first-processing notice once");
        };
        let control_consumption = history
            .entries
            .iter()
            .find_map(|entry| match &entry.fact {
                HistoryFact::InputConsumed { input, .. } if *input == control => {
                    Some(entry.ordinal)
                }
                _ => None,
            })
            .unwrap();
        assert!(control_consumption < *completed && *completed < suffix_consumption);
        let mut ordinary_consumed = history
            .entries
            .iter()
            .filter_map(|entry| match &entry.fact {
                HistoryFact::InputConsumed { input, target } if ordinary_inputs.contains(input) => {
                    assert_eq!(target.actor, actor());
                    assert_eq!(target.scope_origin, scope.id());
                    assert!(entry.ordinal > suffix_consumption);
                    Some(*input)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        ordinary_consumed.sort_unstable();
        let mut expected_ordinary = ordinary_inputs;
        expected_ordinary.sort_unstable();
        assert_eq!(ordinary_consumed, expected_ordinary);
        let wait_history = history
            .entries
            .iter()
            .filter_map(|entry| match &entry.fact {
                HistoryFact::CapacityWaitRegistered {
                    registrant: original,
                    target,
                } if *original == registrant.id() => {
                    assert_eq!(*target, wait_target);
                    Some((entry.ordinal, true))
                }
                HistoryFact::CapacityWaitNotified {
                    registrant: original,
                    target,
                } if *original == registrant.id() => {
                    assert_eq!(*target, wait_target);
                    Some((entry.ordinal, false))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let [(registered, true), (released, false)] = wait_history.as_slice() else {
            panic!("the original keyed wait must be registered and notified once");
        };
        assert!(registered < released && *released < control_consumption);
        ingress.job_closed(other).unwrap();
        ingress.retire_stream(other).unwrap();
        finish();
        return;
    }

    let accept = |sequence| {
        ingress
            .accept(
                key,
                bookmark(key, sequence),
                32,
                bank.credit(false, Some(key)).unwrap(),
            )
            .unwrap();
    };
    accept(1);
    ingress.root_control(bank.seal()).unwrap();
    accept(2);

    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert_eq!(*seen.lock().unwrap(), ["prefix"]);
    drive();
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert_eq!(*seen.lock().unwrap(), ["prefix", "first seal decision"]);
    let root_input = runtime.committed_batches(actor(), None, 4).unwrap()[0].1;
    assert!(
        runtime
            .input_custody(actor(), root_input)
            .unwrap()
            .is_some()
    );
    assert!(!runtime.poll_one(Tick(1)).unwrap());
    drive();
    assert!(
        runtime
            .input_custody(actor(), root_input)
            .unwrap()
            .is_none()
    );
    assert_eq!(*seen.lock().unwrap(), ["prefix", "first seal decision"]);
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert_eq!(
        *seen.lock().unwrap(),
        ["prefix", "first seal decision", "suffix"]
    );
    drive();

    let publish_resume = |revision| {
        read_progress
            .publish(ReadProgressSnapshot {
                participation: key.participation,
                generation: key.generation,
                resumed_at: MonotonicInstant::from_origin(Duration::from_millis(revision)),
                resume_revision: NonZeroU64::new(revision).unwrap(),
            })
            .unwrap()
    };
    let first = publish_resume(1);
    let coalesced = publish_resume(2);
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert_eq!(seen.lock().unwrap().last(), Some(&"first resume"));
    assert!(!first.retired());
    drive();
    assert!(first.retired());
    assert!(!coalesced.retired());
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert_eq!(seen.lock().unwrap().last(), Some(&"coalesced resume"));
    assert!(!coalesced.retired());
    drive();
    assert!(coalesced.retired());
    drop((first, coalesced));
    finish();
}

#[test]
fn failed_tail_keeps_a_ready_credit_until_the_original_publisher_exits() {
    let f = Fixture::new(2, 2);
    let mut credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
    let mut read = ready(Box::new([42u8; 16]));
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok((receipt, wire))) =
        f.input
            .poll_receive(f.key, &mut credit, Pin::new(&mut read), &mut cx)
    else {
        panic!("the original Ready owns its preread position");
    };
    assert!(credit.is_none());
    let held = f.input.borrow_read_confirmation(f.key).unwrap();
    let mut confirmation = Box::pin(held.snapshot(token(1), MonotonicInstant::ORIGIN));
    let wakes = Arc::new(ConfirmationWake::default());
    let waker = Waker::from(wakes.clone());
    let mut confirmation_cx = Context::from_waker(&waker);
    assert!(
        confirmation
            .as_mut()
            .poll(&mut confirmation_cx)
            .is_pending()
    );
    f.input.fence_failed_writer();
    assert!(wakes.0.load(std::sync::atomic::Ordering::SeqCst) != 0);
    assert!(matches!(
        confirmation.as_mut().poll(&mut confirmation_cx),
        Poll::Ready(Err(ObservationError::Closed))
    ));
    drop(confirmation);
    drop(held);
    {
        let mut disposition = Box::pin(f.input.dispose_failed_tail());
        assert!(disposition.as_mut().poll(&mut cx).is_pending());
        assert_eq!(f.input.close_after_exit(), Err(ObservationError::Busy));
        f.input.job_closed(f.key).unwrap();
        assert!(disposition.as_mut().poll(&mut cx).is_pending());
        assert!(!f.bank.stream_claims_exited(f.key));
        assert_eq!(f.input.retire_stream(f.key), Err(ObservationError::Busy));

        // The publisher owns the raw payload outside the writer's FIFO.
        // Closing the stream above does not prove its destruction.
        drop((wire, receipt, read));
        f.input.publisher_exited(f.key);
        assert_eq!(disposition.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    }
    assert!(f.bank.stream_claims_exited(f.key));
    f.input.retire_stream(f.key).unwrap();
    f.finish();
}
