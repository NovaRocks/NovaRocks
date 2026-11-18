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

use super::*;
use novarocks_control_runtime::{
    exit_work::run_future,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::{Consumption, SlotState},
    identity::{Generation, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::{
    collections::VecDeque,
    future::{Ready, poll_fn, ready},
    sync::atomic::{AtomicUsize, Ordering},
};
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
}
impl Fixture {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        Self { ledger, scope }
    }
    fn reported(&self, parent: Option<WorkId>) -> ReportedWork {
        let (work, mut capacity) = self
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
                parent.map_or(WorkAdmission::External, WorkAdmission::Derived),
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            Generation::new(1).unwrap(),
            1,
            [(observer(), ConsumerKind::Observer)],
        )
        .unwrap();
        ReportedWork::try_new(work, capacity, report).unwrap()
    }
    fn finish(self) {
        self.ledger.reap_scope_ready(self.scope.id(), 128).unwrap();
        self.scope.close_external().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
fn observer() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}

#[tokio::test]
async fn prepared_transport_handoff_wakes_source_and_waits_for_actual_join() {
    use std::task::Wake;
    struct WakeCount(AtomicUsize);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let fixture = Fixture::new();
    let original = fixture.reported(None);
    let report = original.report().clone();
    let panic = PanicSink::new();
    let (mut role, exit, handoff) =
        OriginalTransportJoinObligation::prepare(original, observer(), panic.clone())
            .unwrap_or_else(|e| panic!("{}", e.error));
    let (foreign, foreign_exit) = ExitCustody::from_admitted(fixture.reported(None));
    assert!(handoff.owns_exit(&exit));
    assert!(!handoff.owns_exit(&foreign_exit));
    foreign_exit.revoke_unused_and_exit().unwrap();
    assert!(foreign.try_retire().unwrap());

    let counter = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(role.poll_shared_completion(&mut cx).is_pending());
    assert!(!role.try_retire().unwrap());
    let (ready, wait) = tokio::sync::oneshot::channel();
    let original_registration = role.registration();
    handoff
        .spawn(&Handle::current(), exit, |exit, registration| {
            assert!(Weak::ptr_eq(
                &original_registration.original,
                &registration.original
            ));
            let future = run_future(exit, async { wait.await.unwrap() }, panic);
            async { future.await.unwrap() }
        })
        .unwrap();
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert!(role.poll_shared_completion(&mut cx).is_pending());
    assert_eq!(report.publication_state().unwrap(), SlotState::Reserved);
    assert!(!role.try_retire().unwrap());
    ready.send(()).unwrap();
    assert!(
        poll_fn(|cx| role.poll_shared_completion(cx))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        report.consumption(observer()).unwrap(),
        Consumption::Consumed
    );
    assert!(role.try_retire().unwrap());
    fixture.finish();
}

#[tokio::test]
async fn transport_local_join_precedes_full_child_tree_retirement() {
    let fixture = Fixture::new();
    let parent = fixture.reported(None);
    let report = parent.report().clone();
    let child = fixture.reported(Some(parent.work().id()));
    let child_report = child.report().clone();
    let (child_custody, child_exit) = ExitCustody::from_admitted(child);
    let panic = PanicSink::new();
    let inner = panic.clone();
    let mut parent = OriginalTransportJoinObligation::spawn(
        &Handle::current(),
        parent,
        observer(),
        panic,
        move |exit, _| {
            let original = run_future(exit, async {}, inner);
            async move {
                original.await.unwrap();
            }
        },
    )
    .unwrap_or_else(|e| panic!("{}", e.error));
    let complete = poll_fn(|cx| parent.poll_shared_completion(cx))
        .await
        .unwrap();
    let local = parent.role.custody.body_exited() && parent.role.custody.captures_exited();
    let parent_waited = !parent.try_retire().unwrap();
    let before = report.publication_state().unwrap();
    let consumed = report.consumption(observer()).unwrap();
    child_exit.revoke_unused_and_exit().unwrap();
    let unused = child_report.publication_state().unwrap();
    assert!(child_custody.try_retire().unwrap());
    // ExitCustody returns the child's capacity; the original bounded runtime
    // reaper separately retires its header and removes the parent child link.
    let parent_waited_for_reaper = !parent.try_retire().unwrap();
    let reaped = fixture
        .ledger
        .reap_scope_ready(fixture.scope.id(), 1)
        .unwrap();
    assert!(parent.try_retire().unwrap());
    fixture.finish();
    assert!(complete.is_none() && local && parent_waited && parent_waited_for_reaper);
    assert_eq!(reaped, 1);
    assert_eq!(before, SlotState::Filled);
    assert_eq!(consumed, Consumption::Consumed);
    assert_eq!(unused, SlotState::RevokedUnused);
}

#[tokio::test]
async fn unused_transport_report_is_revoked_without_fabricated_consumer_ack() {
    let fixture = Fixture::new();
    let original = fixture.reported(None);
    let report = original.report().clone();
    let panic = PanicSink::new();
    let inner = panic.clone();
    let mut role = OriginalTransportJoinObligation::spawn(
        &Handle::current(),
        original,
        observer(),
        panic,
        move |exit, _| {
            drop(run_future(exit, std::future::pending::<()>(), inner));
            ready(())
        },
    )
    .unwrap_or_else(|e| panic!("{}", e.error));
    let result = poll_fn(|cx| role.poll_shared_completion(cx)).await.unwrap();
    let publication = report.publication_state().unwrap();
    let consumption = report.consumption(observer()).unwrap();
    assert!(role.try_retire().unwrap());
    fixture.finish();
    assert!(result.is_none());
    assert_eq!(publication, SlotState::RevokedUnused);
    assert_eq!(consumption, Consumption::Pending);
}

struct OriginalPanic(Arc<AtomicUsize>);
impl Drop for OriginalPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn factory_panic_returns_owned_failed_role_and_same_raw_cause_without_join_proof() {
    let fixture = Fixture::new();
    let original = fixture.reported(None);
    let dropped = Arc::new(AtomicUsize::new(0));
    let captured = dropped.clone();
    let panic = PanicSink::new();
    let inner = panic.clone();
    let mut role = OriginalTransportJoinObligation::spawn(
        &Handle::current(),
        original,
        observer(),
        panic,
        move |exit, _| -> Ready<()> {
            drop(run_future(exit, std::future::pending::<()>(), inner));
            std::panic::panic_any(OriginalPanic(captured));
        },
    )
    .unwrap_or_else(|e| panic!("{}", e.error));
    let observed = poll_fn(|cx| role.poll_shared_completion(cx)).await;
    let retained = dropped.load(Ordering::SeqCst) == 0;
    let cause = role.take_handoff_failure().unwrap();
    let same = cause.is::<OriginalPanic>();
    let no_duplicate = role.take_handoff_failure().is_none();
    drop(cause);
    assert!(role.retire_unused_without_join().unwrap());
    fixture.finish();
    assert!(matches!(observed, Err(CustodyError::OriginalHandoffFailed)));
    assert!(retained && same && no_duplicate);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_observer_returns_original_reported_work_before_factory_runs() {
    let fixture = Fixture::new();
    let original = fixture.reported(None);
    let report = original.report().clone();
    let ran = Arc::new(AtomicUsize::new(0));
    let actual = ran.clone();
    let result = OriginalTransportJoinObligation::spawn(
        &Handle::current(),
        original,
        ConsumerId::new(99).unwrap(),
        PanicSink::new(),
        move |_, _| {
            actual.fetch_add(1, Ordering::SeqCst);
            ready(())
        },
    );
    let refused = match result {
        Err(refused) => refused,
        Ok(_) => panic!("foreign observer cannot own this role"),
    };
    let same = refused.original.report().same_slot(&report);
    let (custody, exit) = ExitCustody::from_admitted(refused.original);
    exit.revoke_unused_and_exit().unwrap();
    assert!(custody.try_retire().unwrap());
    fixture.finish();
    assert!(same);
    assert_eq!(ran.load(Ordering::SeqCst), 0);
}

use crate::control_runtime::actor::{FrontendActor, FrontendEffect, FrontendInput, TestActor};
use novarocks_attempt_protocol::input::{AttemptInput, ExecutorFact};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    actor_turn::{ActorRegistration, ActorRuntime},
    causal_input::CausalInputObligation,
};
use novarocks_control_runtime_core::{
    identity::{ActorId, InputId, Tick},
    mailbox::{InputClass, InputEnvelope},
};

type ObservedWithdrawals = Arc<Mutex<Vec<(InputEnvelope, AttemptInput)>>>;
type TransportCommand = (
    Arc<crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope>,
    Option<novarocks_control_runtime::obligation::StepRegistration>,
);
struct WithdrawalTarget {
    observed: ObservedWithdrawals,
    commands: Arc<Mutex<VecDeque<TransportCommand>>>,
    handle_attempt_hints: bool,
}
impl WithdrawalTarget {
    const SELECT_COMMAND_SOURCE: u64 = 2;
    const REOPEN_SOURCES: u64 = 3;
}

impl TestActor for WithdrawalTarget {
    fn handle(
        &mut self,
        _: Tick,
        envelope: InputEnvelope,
        value: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        if self.handle_attempt_hints {
            match &value {
                FrontendInput::AttemptStartup(_) => {
                    assert_eq!(envelope.class, InputClass::Lifecycle);
                    return;
                }
                FrontendInput::CapacityReleased | FrontendInput::Membership(_) => {
                    assert_eq!(envelope.class, InputClass::Ordinary);
                    return;
                }
                FrontendInput::Attempt(value)
                    if matches!(
                        value.as_ref(),
                        AttemptInput::Executor(ExecutorFact::ReadProgress(_))
                        | AttemptInput::Port(novarocks_coordination_contract::LogicalToAttempt {
                            body: novarocks_coordination_contract::LogicalToAttemptBody::PreReadyReplacementChanged { .. }, ..
                        })
                    ) =>
                {
                    assert_eq!(envelope.class, InputClass::Ordinary);
                    assert_eq!(crate::task_execution::host::adapter::input_class(value),envelope.class);
                    return;
                }
                _ => {}
            }
        }
        if let FrontendInput::Test(value) = value {
            assert_ne!(value, u64::MAX, "transport fixture Core failure");
            match value {
                Self::SELECT_COMMAND_SOURCE => {
                    step.select_input_source(Some(envelope.source));
                    return;
                }
                Self::REOPEN_SOURCES => {
                    step.select_input_source(None);
                    return;
                }
                _ => {}
            }
            let (effect, guard) = self
                .commands
                .lock()
                .unwrap()
                .pop_front()
                .expect("one original transport command");
            let work = match guard {
                Some(guard) => {
                    let work = novarocks_control_runtime_core::identity::ScopedWorkRef {
                        scope: guard.scope().id(),
                        work: guard.work_id(),
                    };
                    assert!(step.reservation(guard).is_ok());
                    work
                }
                None => step.input_work().expect("original admission author"),
            };
            let kind = match effect.protocol.class {
                novarocks_attempt_protocol::effect::EffectClass::Business => {
                    novarocks_control_runtime_core::effect::EffectKind::Business
                }
                novarocks_attempt_protocol::effect::EffectClass::Settlement => {
                    novarocks_control_runtime_core::effect::EffectKind::Settlement
                }
                novarocks_attempt_protocol::effect::EffectClass::Publication => {
                    novarocks_control_runtime_core::effect::EffectKind::Publication
                }
                novarocks_attempt_protocol::effect::EffectClass::Reply => {
                    novarocks_control_runtime_core::effect::EffectKind::Reply
                }
                novarocks_attempt_protocol::effect::EffectClass::Cleanup => {
                    novarocks_control_runtime_core::effect::EffectKind::Cleanup
                }
            };
            step.effect(
                crate::task_execution::host::adapter::descriptor(
                    effect.protocol.id.get(),
                    work,
                    kind,
                ),
                FrontendEffect::Attempt(effect),
            )
            .unwrap();
            return;
        }
        let FrontendInput::Attempt(value) = value else {
            panic!("expected an original attempt input");
        };
        self.observed.lock().unwrap().push((envelope, *value));
    }
}

pub(crate) struct WithdrawalInbox {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    source: ActorId,
    observed: ObservedWithdrawals,
    commands: Arc<Mutex<VecDeque<TransportCommand>>>,
}
impl WithdrawalInbox {
    pub(crate) fn new(scope: &ScopeHandle) -> Self {
        Self::with_effect(scope, None)
    }
    const CONFIG: ActorConfig = ActorConfig {
        mailbox: [4; 4],
        max_batches: 4,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 8,
        max_step_work: 1,
    };

    fn with_effect(
        scope: &ScopeHandle,
        effects: impl IntoIterator<
            Item = Arc<crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope>,
        >,
    ) -> Self {
        Self::with_commands(
            scope,
            effects.into_iter().map(|effect| (effect, None)),
            Self::CONFIG.mailbox,
        )
    }

    pub(crate) fn with_commands(
        scope: &ScopeHandle,
        commands: impl IntoIterator<Item = TransportCommand>,
        mailbox: [usize; 4],
    ) -> Self {
        Self::with_failure_work(scope, commands, mailbox, None)
    }

    pub(crate) fn with_failure_work(
        scope: &ScopeHandle,
        commands: impl IntoIterator<Item = TransportCommand>,
        mailbox: [usize; 4],
        failure: Option<&novarocks_control_runtime::obligation::WorkObligation>,
    ) -> Self {
        Self::register(
            scope,
            commands,
            ActorConfig {
                mailbox,
                ..Self::CONFIG
            },
            failure,
            None,
        )
    }

    pub(crate) fn with_attempt_geometry_and_history(
        scope: &ScopeHandle,
        commands: impl IntoIterator<Item = TransportCommand>,
        config: ActorConfig,
        sources: novarocks_attempt_protocol::start::AttemptSources,
        history: novarocks_control_runtime::history::HistoryCapture,
    ) -> Self {
        assert_eq!(config.mailbox, sources.mailbox);
        Self::register(scope, commands, config, None, Some((sources, history)))
    }

    fn register(
        scope: &ScopeHandle,
        commands: impl IntoIterator<Item = TransportCommand>,
        config: ActorConfig,
        failure: Option<&novarocks_control_runtime::obligation::WorkObligation>,
        geometry: Option<(
            novarocks_attempt_protocol::start::AttemptSources,
            novarocks_control_runtime::history::HistoryCapture,
        )>,
    ) -> Self {
        let handle_attempt_hints = geometry.is_some();
        let (runtime, actor, source) = match geometry {
            Some((sources, history)) => (
                ActorRuntime::with_history(history),
                ActorId::new(sources.actor.0.get()).unwrap(),
                ActorId::new(sources.subscription.0.get()).unwrap(),
            ),
            None => (
                ActorRuntime::new(),
                ActorId::new(1).unwrap(),
                ActorId::new(2).unwrap(),
            ),
        };
        let observed: ObservedWithdrawals = Arc::default();
        let commands = Arc::new(Mutex::new(commands.into_iter().collect()));
        let target = FrontendActor::Test(Box::new(WithdrawalTarget {
            observed: observed.clone(),
            commands: commands.clone(),
            handle_attempt_hints,
        }));
        let registration = match failure {
            Some(work) => runtime.register_supervised_bound_with_source_cut(
                actor,
                scope.clone(),
                target,
                config,
                work,
                source,
            ),
            None => {
                runtime.register_bound_with_source_cut(actor, scope.clone(), target, config, source)
            }
        }
        .unwrap_or_else(|(error, _)| panic!("{error}"));
        Self {
            runtime,
            registration,
            source,
            observed,
            commands,
        }
    }
    pub(crate) fn push_transport_command(
        &self,
        effect: Arc<crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope>,
        guard: Option<novarocks_control_runtime::obligation::StepRegistration>,
    ) {
        self.commands.lock().unwrap().push_back((effect, guard));
    }

    pub(crate) fn reserve(&self, work: &ReportedWork) -> CausalInputObligation<FrontendActor> {
        self.reserve_result(work.work())
    }

    pub(crate) fn reserve_result(
        &self,
        work: &novarocks_control_runtime::obligation::WorkObligation,
    ) -> CausalInputObligation<FrontendActor> {
        CausalInputObligation::reserve_admitted(
            &self.runtime,
            &self.registration,
            self.source,
            InputClass::Lifecycle,
            work,
        )
        .unwrap()
    }

    pub(crate) fn registration(&self) -> &ActorRegistration {
        &self.registration
    }
    pub(crate) fn source(&self) -> ActorId {
        self.source
    }

    pub(crate) fn runtime(&self) -> &ActorRuntime<FrontendActor> {
        &self.runtime
    }

    pub(crate) fn command(
        &self,
        work: &novarocks_control_runtime::obligation::WorkObligation,
        action: novarocks_coordination_contract::AttemptActionId,
    ) -> novarocks_control_runtime::actor_turn::EffectTicket<FrontendEffect> {
        let input = self.submit_command(work);
        assert!(self.runtime.poll_one(Tick(1)).unwrap());
        self.runtime
            .take_effect(
                self.registration.actor(),
                input,
                novarocks_control_runtime_core::identity::ActionId::new(action.get()).unwrap(),
            )
            .unwrap()
    }

    pub(crate) fn submit_command(
        &self,
        work: &novarocks_control_runtime::obligation::WorkObligation,
    ) -> InputId {
        self.reserve_command(work)
            .fill(FrontendInput::Test(1))
            .unwrap()
    }
    pub(crate) fn select_command_source_for_test(
        &self,
        work: &novarocks_control_runtime::obligation::WorkObligation,
        selected: bool,
    ) {
        let command = if selected {
            WithdrawalTarget::SELECT_COMMAND_SOURCE
        } else {
            WithdrawalTarget::REOPEN_SOURCES
        };
        let input = self
            .reserve_command(work)
            .fill(FrontendInput::Test(command))
            .unwrap();
        assert!(self.runtime.poll_one(Tick(1)).unwrap());
        self.runtime
            .retire_input(self.registration.actor(), input)
            .unwrap();
    }

    pub(crate) fn reserve_command(
        &self,
        work: &novarocks_control_runtime::obligation::WorkObligation,
    ) -> novarocks_control_runtime::actor_turn::ReservedInputObligation<FrontendActor> {
        self.runtime
            .reserve_admitted_input_for(
                &self.registration,
                self.source,
                InputClass::Lifecycle,
                work,
            )
            .unwrap()
    }
    pub(crate) fn reserve_standing(
        &self,
        work: &novarocks_control_runtime::obligation::WorkObligation,
    ) -> novarocks_control_runtime::actor_turn::StandingInputObligation<FrontendActor> {
        self.runtime
            .reserve_admitted_input_for(
                &self.registration,
                self.source,
                InputClass::Lifecycle,
                work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap()
    }
    pub(crate) fn reserve_native_receipt(
        &self,
        work: &novarocks_control_runtime::obligation::WorkObligation,
        key: crate::task_execution::host::transport::SendKey,
        kind: novarocks_execution_contract::OperationKind,
    ) -> crate::task_execution::host::transport::NativeReceiptInput {
        crate::task_execution::host::transport::NativeReceiptInput::new(
            key,
            kind,
            self.reserve_standing(work),
        )
    }

    pub(crate) fn assert_no_result(&self) {
        assert_eq!(
            self.runtime
                .snapshot(self.registration.actor())
                .unwrap()
                .inputs,
            0
        );
    }
    pub(crate) fn consume(&self, input: InputId) -> ExecutorFact {
        let fact = self.commit(input);
        self.runtime
            .retire_input(self.registration.actor(), input)
            .unwrap();
        fact
    }
    pub(crate) fn commit(&self, input: InputId) -> ExecutorFact {
        let (actual, fact) = self.try_commit_next().expect("same original input");
        assert_eq!(actual, input);
        fact
    }

    pub(crate) fn try_commit_next(&self) -> Option<(InputId, ExecutorFact)> {
        let (input, value) = self.try_commit_attempt()?;
        let AttemptInput::Executor(fact) = value else {
            panic!("expected an original executor fact");
        };
        Some((input, fact))
    }

    fn try_commit_attempt(&self) -> Option<(InputId, AttemptInput)> {
        self.try_commit_attempt_from(self.source)
    }

    fn try_commit_attempt_from(&self, source: ActorId) -> Option<(InputId, AttemptInput)> {
        self.try_commit_from(source, InputClass::Lifecycle)
    }

    fn try_commit_from(
        &self,
        source: ActorId,
        class: InputClass,
    ) -> Option<(InputId, AttemptInput)> {
        let (envelope, fact) = self.try_commit_envelope()?;
        assert_eq!(envelope.source, source);
        assert_eq!(envelope.class, class);
        Some((envelope.input, fact))
    }

    pub(in crate::task_execution::host) fn try_commit_envelope(
        &self,
    ) -> Option<(
        novarocks_control_runtime_core::mailbox::InputEnvelope,
        AttemptInput,
    )> {
        if self.observed.lock().unwrap().is_empty() {
            self.runtime.poll_one(Tick(1)).unwrap();
        }
        let mut observed = self.observed.lock().unwrap();
        if observed.is_empty() {
            return None;
        }
        Some(observed.remove(0))
    }
    pub(crate) fn finish(self) {
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        self.runtime
            .remove_actor(self.registration.actor())
            .unwrap();
        assert!(self.runtime.is_empty());
    }
}

#[tokio::test]
async fn withdrawn_admission_publishes_the_original_generation_after_join() {
    assert_withdrawn_admission_result(false).await;
    assert_withdrawn_admission_result(true).await;
}

async fn assert_withdrawn_admission_result(fail_scope: bool) {
    use crate::{
        control_runtime::transport_withdrawal::AttemptEffectEnvelope,
        task_execution::host::transport::BodyOutcome,
    };
    use novarocks_attempt_protocol::{
        effect::{AttemptEffect, EffectBody, EffectClass, OperationIntent, QueuedInputPayload},
        input::SendGeneration,
    };
    use novarocks_control_runtime::exit_work::{GatedNotStarted, GatedOutcome, GatedWork};
    use novarocks_coordination_contract::AttemptActionId;
    use novarocks_execution_contract::{CancelReason, CancelTask, TaskIdentity, TaskOperationId};
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroU64;

    let fixture = Fixture::new();
    let inbox = WithdrawalInbox::new(&fixture.scope);
    let original = fixture.reported(None);
    let result = inbox.reserve(&original);
    let author = result.event_author().unwrap();
    let foreign_result = inbox.reserve(&original);
    let foreign_author = foreign_result.event_author().unwrap();
    let id = AttemptActionId::new(NonZeroU64::MIN);
    let send_generation = SendGeneration::new(id);
    let operation_id = TaskOperationId::new_v7();
    let operation = OperationIntent::CancelTask(CancelTask::new(
        operation_id,
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(0x7a5c, 6), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        ),
        CancelReason::UpstreamNoLongerNeeded,
    ));
    let effect = AttemptEffectEnvelope::new(AttemptEffect {
        id,
        class: EffectClass::Settlement,
        body: EffectBody::AdmitQueuedInput {
            lane: operation.lane(),
            operation,
            generation: send_generation,
            payload: QueuedInputPayload::AlreadyRetained,
        },
    });
    let arbitration = effect.handoff().unwrap().clone();
    arbitration.withdraw();
    let panic = PanicSink::new();
    let (mut owner, exit, handoff) =
        OriginalTransportJoinObligation::prepare(original, observer(), panic.clone())
            .unwrap_or_else(|e| panic!("{}", e.error));
    let mut foreign_result = handoff
        .bind_actor_result(effect.clone(), &author, foreign_result, None)
        .expect_err("another position of the same Work cannot substitute");
    assert!(
        foreign_result
            .event_author()
            .unwrap()
            .same_binding(&foreign_author)
    );
    assert!(
        handoff
            .bind_actor_result(effect.clone(), &author, result, None)
            .is_ok()
    );
    let (staged, observe) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    handoff
        .spawn(&Handle::current(), exit, |exit, port| async move {
            let outcome = GatedWork::new(exit, (), arbitration, panic)
                .run(
                    Generation::new(1).unwrap(),
                    ready(()),
                    |(), ()| -> Ready<()> {
                        panic!("withdrawn admission must not start");
                    },
                )
                .await
                .unwrap();
            let GatedOutcome::NotStarted(reason) = outcome else {
                panic!("original admission gate must report unused");
            };
            assert!(
                port.stage_actor_outcome(BodyOutcome::Unused(reason))
                    .is_ok()
            );
            assert!(
                port.stage_actor_outcome(BodyOutcome::ReceiptsRecorded)
                    .is_err()
            );
            staged.send(()).unwrap();
            wait.await.unwrap();
        })
        .unwrap();
    observe.await.unwrap();
    assert!(owner.take_actor_result().is_none());
    inbox.assert_no_result();
    assert!(!owner.try_retire().unwrap());
    release.send(()).unwrap();
    assert!(
        poll_fn(|cx| owner.poll_shared_completion(cx))
            .await
            .unwrap()
            .is_none()
    );
    assert!(!owner.try_retire().unwrap());
    if fail_scope {
        fixture.scope.fail().unwrap();
    }
    let mut returned = owner.take_actor_result().expect("actual original result");
    assert!(Arc::ptr_eq(&returned.effect, &effect));
    assert!(
        returned
            .original
            .event_author()
            .unwrap()
            .same_binding(&author)
    );
    assert!(matches!(
        returned.outcome,
        Some(BodyOutcome::Unused(GatedNotStarted::Withdrawn))
    ));
    assert!(owner.take_actor_result().is_none());
    let fact = match returned.withdrawal.take().unwrap() {
        Ok(input) if !fail_scope => inbox.consume(input),
        Err(failure) if fail_scope => {
            assert_eq!(
                failure.error,
                novarocks_control_runtime::actor_turn::ActorTurnError::ActorFailed
            );
            assert!(failure.predecessor.is_none() && failure.predecessor_input.is_none());
            inbox.assert_no_result();
            let FrontendInput::Attempt(value) = failure.event else {
                panic!("refusal returns the exact original attempt event");
            };
            let AttemptInput::Executor(fact) = *value else {
                panic!("refusal returns the exact original executor fact");
            };
            fact
        }
        Ok(_) => panic!("failed Scope must refuse the original event"),
        Err(failure) => panic!("{}", failure.error),
    };
    assert!(matches!(
        fact,
        ExecutorFact::QueuedInputWithdrawn { operation, generation }
            if operation == operation_id && generation == send_generation
    ));
    returned.original.close_after_exit().unwrap();
    foreign_result.close_after_exit().unwrap();
    assert!(owner.try_retire().unwrap());
    fixture.scope.close_external().unwrap();
    fixture
        .ledger
        .reap_scope_ready(fixture.scope.id(), 128)
        .unwrap();
    inbox.finish();
    fixture.finish();
}

use crate::{
    control_runtime::host::{EffectSource, PendingHandoff, PreparedEffectJob, SourceProgress},
    task_execution::host::transport::AttemptCallSource,
};
use novarocks_control_runtime::actor_turn::{ActorHostTurn, EffectTicket};
use novarocks_coordination_contract::AttemptActionId;

struct CallFixtureSource {
    source: Arc<Mutex<AttemptCallSource>>,
    process: Option<Arc<Mutex<AttemptCallSource>>>,
    process_pending: bool,
    scope: ScopeHandle,
    published: Arc<AtomicUsize>,
    closed: bool,
}
impl EffectSource for CallFixtureSource {
    fn prepay_result(
        &mut self,
        effect: &FrontendEffect,
    ) -> Option<CausalInputObligation<FrontendActor>> {
        let FrontendEffect::Attempt(effect) = effect else {
            unreachable!()
        };
        self.process_pending = effect.is_process_call();
        let source = if self.process_pending {
            self.process.as_ref().expect("original Process test source")
        } else {
            &self.source
        };
        source.lock().unwrap().prepay(effect).unwrap()
    }
    fn bind_job(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: CausalInputObligation<FrontendActor>,
    ) -> PreparedEffectJob {
        let source = if self.process_pending {
            self.process.as_ref().expect("original Process test source")
        } else {
            &self.source
        };
        source.lock().unwrap().bind(ticket, result)
    }
    fn return_unclaimed_result(&mut self, result: CausalInputObligation<FrontendActor>) {
        let source = if self.process_pending {
            self.process.as_ref().expect("original Process test source")
        } else {
            &self.source
        };
        source.lock().unwrap().return_unclaimed(result);
    }
    fn core_consumed(&mut self, _: &[InputId]) {}
    fn transport_event(&mut self, effect: AttemptActionId, event: TransportSourceEvent) {
        let source = if effect.get() == 6 {
            self.process.as_ref().expect("original Process test source")
        } else {
            &self.source
        };
        let mut source = source.lock().unwrap();
        match event {
            TransportSourceEvent::Result(original) => {
                source
                    .accept(original)
                    .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)));
                self.published.fetch_add(1, Ordering::SeqCst);
            }
            TransportSourceEvent::Retired => source.transport_retired(effect),
            original => std::panic::resume_unwind(Box::new(original)),
        }
    }
    fn owner_failed_pending(&mut self, pending: PendingHandoff) {
        std::panic::resume_unwind(Box::new(pending));
    }
    fn close_external(&mut self) {
        self.closed = true;
        self.scope.close_external().unwrap();
    }
    fn result_admission_closed(&self) -> bool {
        self.closed
    }
    fn progress_retirement(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        _: Tick,
        budget: usize,
    ) -> SourceProgress {
        let mut progressed = false;
        let mut units = 0;
        for source in std::iter::once(&self.source)
            .chain(self.process.iter())
            .take(budget)
        {
            units += 1;
            progressed |= source.lock().unwrap().retire_result().unwrap();
        }
        SourceProgress {
            units,
            progressed,
            more: false,
            retired: self.closed && self.scope.join_ready().unwrap(),
        }
    }
}

#[tokio::test]
async fn publication_host_returns_original_result_and_typed_failure() {
    use crate::{
        control_runtime::{
            actor::FrontendActorKind,
            host::{FrontendLanePolicy, HostBinding, HostRegistry},
            metrics::FrontendHostObservations,
            transport_withdrawal::AttemptEffectEnvelope,
        },
        task_execution::host::{
            publication::{AttemptPublicationOwner, OriginalHostFailure, PublicationError, tests},
            transport::{ActorTransportExecutor, AttemptCallOwner},
        },
    };
    use novarocks_attempt_protocol::{
        effect::{AttemptEffect, EffectBody, EffectClass, Publication},
        failure::AttemptFailure,
        input::{FinalTaskInfoRef, PublicationApplied},
    };
    use novarocks_control_runtime::actor_turn::ProcessClock;
    use novarocks_coordination_contract::AttemptRequestToken;
    use std::{num::NonZeroU64, time::Duration};

    let fixture = Fixture::new();
    let generation = Generation::new(1).unwrap();
    let (mut parent, mut payer) = fixture
        .scope
        .reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let (record, mut receiver, root, plan) = tests::fixture();
    let owner = AttemptPublicationOwner::new(record, None);
    let activation = owner.activation();
    let id = |n| AttemptActionId::new(NonZeroU64::new(n).unwrap());
    let first = id(1);
    let second = id(2);
    let command = |id, publication| {
        AttemptEffectEnvelope::new(AttemptEffect {
            id,
            class: EffectClass::Publication,
            body: EffectBody::Publish(publication),
        })
    };
    let reference = FinalTaskInfoRef::new(AttemptRequestToken::new(NonZeroU64::MIN), root);
    let inbox = WithdrawalInbox::with_effect(
        &fixture.scope,
        [
            command(first, tests::batch(0, Vec::new())),
            command(second, Publication::FinalTaskInfo(reference)),
        ],
    );
    let source = Arc::new(Mutex::new(AttemptCallSource::new(
        &parent,
        &payer,
        generation,
        observer(),
        AttemptCallOwner::Publication(owner),
        inbox.reserve_result(&parent),
    )));
    let published = Arc::new(AtomicUsize::new(0));
    let host = HostRegistry::new(
        inbox.runtime.clone(),
        fixture.ledger.clone(),
        Arc::new(FrontendHostObservations::new(Arc::new(
            ProcessClock::default(),
        ))),
    );
    let handle = Handle::current();
    let mut exit = host
        .register(HostBinding {
            scope: fixture.scope.clone(),
            registration: inbox.registration.clone(),
            kind: FrontendActorKind::Test,
            config: WithdrawalInbox::CONFIG,
            policy: FrontendLanePolicy::attempt(1),
            executor: Arc::new(ActorTransportExecutor(handle.clone())),
            source: Box::new(CallFixtureSource {
                source: source.clone(),
                process: None,
                process_pending: false,
                scope: fixture.scope.clone(),
                published: published.clone(),
                closed: false,
            }),
            transport: Some(TransportRetirements::new(
                inbox.runtime.clone(),
                inbox.registration.clone(),
                handle,
                1,
            )),
            startup: None,
            capacity: None,
            membership: None,

            local_exit: None,
            ordered_observations: None,
        })
        .unwrap_or_else(|_| panic!("original publication host"));
    let drive_host = || {
        while let Some(turn) = inbox.runtime.claim_host_actor() {
            host.drive(Tick(1), turn);
        }
    };
    let commands = [inbox.submit_command(&parent), inbox.submit_command(&parent)];
    assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
    for (index, expected) in [first, second].into_iter().enumerate() {
        tokio::time::timeout(Duration::from_secs(2), async {
            while published.load(Ordering::SeqCst) <= index {
                drive_host();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(published.load(Ordering::SeqCst), index + 1);
        assert!(!source.lock().unwrap().retire_result().unwrap());
        let (_, ExecutorFact::PublicationApplied { effect, result }) =
            inbox.try_commit_next().unwrap()
        else {
            panic!("same original publication result");
        };
        assert_eq!(effect, expected);
        if expected == first {
            assert_eq!(
                result.unwrap(),
                PublicationApplied::ProtocolBatch { published: true }
            );
            assert!(
                receiver
                    .take_changes(activation, root, plan)
                    .unwrap()
                    .unwrap()
                    .is_initial()
            );
        } else {
            let Err(AttemptFailure::Host(failure)) = result else {
                panic!("original typed publication failure");
            };
            assert_eq!(failure.original.0.get(), effect.get());
            assert!(matches!(
                source.lock().unwrap().take_failure(failure.original),
                Some(OriginalHostFailure::Publication(
                    PublicationError::MissingProfileObserver
                ))
            ));
        }
        inbox
            .runtime
            .retire_input(inbox.registration.actor(), commands[index])
            .unwrap();
        drive_host();
    }
    let publisher = {
        let mut original = source.lock().unwrap();
        assert!(original.close_idle().unwrap());
        original
            .take_drained_owner()
            .expect("original publication role retired")
    };
    drop(publisher);
    parent.body_exited().unwrap();
    parent.captures_exited().unwrap();
    payer.finish().unwrap();
    fixture.scope.reap_ready(128).unwrap();
    exit.request_close().unwrap();
    drive_host();
    tokio::time::timeout(Duration::from_secs(1), exit.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(host.is_empty() && inbox.runtime.is_empty() && fixture.ledger.is_empty());
}

#[tokio::test]
async fn read_confirmation_host_holds_the_exact_stream_through_executor_capture_exit() {
    use crate::{
        control_runtime::{
            actor::FrontendActorKind,
            host::{FrontendLanePolicy, HostBinding, HostRegistry},
            metrics::FrontendHostObservations,
            transport_withdrawal::AttemptEffectEnvelope,
        },
        task_execution::host::{
            observations::{
                ObservationBudget, ObservationError, OrderedObservationInput, RootControlBinding,
                StreamKey,
                tests::{Bank, ManualClock},
            },
            transport::{ActorTransportExecutor, AttemptCallOwner},
        },
    };
    use novarocks_attempt_protocol::{
        effect::{AttemptEffect, EffectBody, EffectClass},
        input::ReceptionCoverage,
    };
    use novarocks_control_runtime::actor_turn::ProcessClock;
    use novarocks_control_runtime_core::owned_storage::OwnedSlotKind;
    use novarocks_coordination_contract::{
        AttemptActivationIdentity, AttemptRequestToken, MonotonicInstant,
    };
    use novarocks_execution_contract::{ParticipationRef, TaskIdentity};
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::{num::NonZeroU64, time::Duration};

    let fixture = Fixture::new();
    let generation = Generation::new(1).unwrap();
    let budget = ObservationBudget {
        queued_frames: 1,
        total_positions: 3,
        max_frame_bytes: 1024,
        total_bytes: 1536,
        control_bytes: 512,
    };
    let (mut parent, mut payer) = fixture
        .scope
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
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    parent.start().unwrap();
    let positions = (0..budget.total_positions)
        .map(|_| {
            let mut original = payer.take_position(OwnedSlotKind::Input).unwrap();
            original.bind().unwrap();
            original
        })
        .collect();
    let execution = QueryExecutionId::new(QueryId::new(11, 1), AttemptId::new(1).unwrap()).unwrap();
    let backend = BackendProcessId::new_v7();
    let participation = ParticipationRef::new(execution, FrontendProcessId::new_v7(), backend);
    let key = StreamKey {
        participation,
        generation: NonZeroU64::MIN,
    };
    let token = AttemptRequestToken::new(NonZeroU64::MIN);
    let now = MonotonicInstant::from_origin(Duration::ZERO);
    let effect = AttemptEffectEnvelope::new(AttemptEffect {
        id: AttemptActionId::new(NonZeroU64::MIN),
        class: EffectClass::Settlement,
        body: EffectBody::ConfirmReadState {
            token,
            participation,
            generation: key.generation,
            requested_at: now,
        },
    });
    let inbox = WithdrawalInbox::with_effect(&fixture.scope, [effect]);
    let root = TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        backend,
    );
    let binding = RootControlBinding::for_registered_source(
        inbox.registration.clone(),
        AttemptActivationIdentity::from_parts(1, execution, 1),
        root,
    )
    .unwrap();
    let bank = Bank::make(budget, positions, binding.clone());
    let observation_source = ActorId::new(4).unwrap();
    let standing = |class| {
        inbox
            .runtime
            .reserve_admitted_input_for(&inbox.registration, observation_source, class, &parent)
            .unwrap()
            .into_standing_input()
            .unwrap()
    };
    let ingress = OrderedObservationInput::new(
        standing(InputClass::Lifecycle),
        standing(InputClass::Control),
        bank.clone(),
        binding,
        Arc::new(ManualClock::default()),
        |input| FrontendInput::Attempt(Box::new(input)),
        Waker::noop().clone(),
    )
    .unwrap();
    ingress.register_stream(key).unwrap();
    let source = Arc::new(Mutex::new(AttemptCallSource::new(
        &parent,
        &payer,
        generation,
        observer(),
        AttemptCallOwner::ReadConfirmation(ingress.clone()),
        inbox.reserve_result(&parent),
    )));
    let published = Arc::new(AtomicUsize::new(0));
    let host = HostRegistry::new(
        inbox.runtime.clone(),
        fixture.ledger.clone(),
        Arc::new(FrontendHostObservations::new(Arc::new(
            ProcessClock::default(),
        ))),
    );
    let handle = Handle::current();
    let mut exit = host
        .register(HostBinding {
            scope: fixture.scope.clone(),
            registration: inbox.registration.clone(),
            kind: FrontendActorKind::Test,
            config: WithdrawalInbox::CONFIG,
            policy: FrontendLanePolicy::attempt(1),
            executor: Arc::new(ActorTransportExecutor(handle.clone())),
            source: Box::new(CallFixtureSource {
                source: source.clone(),
                process: None,
                process_pending: false,
                scope: fixture.scope.clone(),
                published: published.clone(),
                closed: false,
            }),
            transport: Some(TransportRetirements::new(
                inbox.runtime.clone(),
                inbox.registration.clone(),
                handle,
                1,
            )),
            startup: None,
            capacity: None,
            membership: None,

            local_exit: None,
            ordered_observations: None,
        })
        .unwrap_or_else(|_| panic!("original confirmation host"));
    let drive = || {
        while let Some(turn) = inbox.runtime.claim_host_actor() {
            host.drive(Tick(1), turn);
        }
    };
    let command = inbox.submit_command(&parent);
    assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
    drive();
    ingress.job_closed(key).unwrap();
    assert_eq!(ingress.retire_stream(key), Err(ObservationError::Busy));
    inbox
        .runtime
        .retire_input(inbox.registration.actor(), command)
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        while published.load(Ordering::SeqCst) == 0 {
            drive();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (_, ExecutorFact::ReadState(snapshot)) = inbox.try_commit_next().unwrap() else {
        panic!("same original confirmation");
    };
    assert_eq!(
        (
            snapshot.participation,
            snapshot.generation,
            snapshot.request
        ),
        (participation, key.generation, token)
    );
    assert_eq!(snapshot.snapshot_at, now);
    assert_eq!(
        snapshot.received_through_snapshot,
        ReceptionCoverage::Complete
    );
    assert!(snapshot.closed);

    tokio::time::timeout(Duration::from_secs(2), async {
        while !source.lock().unwrap().close_idle().unwrap() {
            drive();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    ingress.retire_stream(key).unwrap();
    ingress.close_after_exit().unwrap();
    bank.retire_positions();
    let original = source.lock().unwrap().take_drained_owner().unwrap();
    drop(original);
    parent.body_exited().unwrap();
    parent.captures_exited().unwrap();
    payer.finish().unwrap();
    fixture.scope.reap_ready(128).unwrap();
    exit.request_close().unwrap();
    drive();
    tokio::time::timeout(Duration::from_secs(2), exit.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(host.is_empty() && inbox.runtime.is_empty() && fixture.ledger.is_empty());
}

#[tokio::test]
async fn qa_host_serializes_admission_and_establish_through_one_reply_credit() {
    use crate::{
        control_runtime::{
            actor::FrontendActorKind,
            host::{FrontendLanePolicy, HostBinding, HostRegistry},
            membership::FrozenMember,
            metrics::FrontendHostObservations,
            transport_withdrawal::AttemptEffectEnvelope,
        },
        query_execution::lifecycle_plan::QueryCredentialLeases,
        task_execution::{
            host::transport::{
                ActorTransportExecutor, AttemptCallOwner, AttemptCallSource,
                AttemptEstablishMaterial, AttemptProcessObserver,
            },
            sources::AttemptEstablishFacts,
        },
    };
    use novarocks_attempt_protocol::effect::{
        AttemptEffect, EffectBody, EffectClass, OperationIntent,
    };
    use novarocks_attempt_protocol::port_output::{
        AttemptAuthorization, AttemptTerminal, AttemptToLogical, AttemptToLogicalBody,
    };
    use novarocks_control_runtime::actor_turn::{Clock, ProcessClock};
    use novarocks_coordination_contract::{
        AttemptActionId, AttemptAuthorizationDecision, AttemptAuthorizationRef,
        AttemptConvergenceClosure, AttemptIssueSettlement, AttemptParticipationConvergence,
        AttemptRequestToken, LogicalToAttemptBody, MonotonicInstant,
    };
    use novarocks_execution_contract::{
        AcquireParticipationAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
        LeaseSequence, LeaseValidFor, OperationEnvelope, OperationKind, OperationOutcome,
        ParticipationAdmissionTicketReceipt, ParticipationRef, RuntimeEndpoint, TaskOperationId,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
        QueryId,
    };
    use std::{num::NonZeroU64, time::Duration};

    use novarocks_query_application::api::{
        BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
    };

    struct FrozenProcess {
        original: ParticipationRef,
        endpoint: RuntimeEndpoint,
        replacement: BackendProcessId,
    }
    impl BackendProcessObservationPort for FrozenProcess {
        fn observe_process_at_endpoint(
            &self,
            expected: BackendProcessId,
            endpoint: &RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, BackendTopologyError> {
            assert_eq!(expected, self.original.backend_process_id());
            assert_eq!(endpoint, &self.endpoint);
            Ok(BackendProcessObservation::Replaced {
                current_process: self.replacement,
            })
        }
    }
    struct SampleClock;
    impl Clock for SampleClock {
        fn now(&self) -> Tick {
            Tick(1)
        }
    }

    use crate::control_runtime::host::{EffectBindRefusal, EffectExecutor, PreparedEffectBinding};
    use crate::control_runtime::logical::adapter::{LogicalEffectEnvelope, LogicalHostEffect};
    use crate::task_execution::host::port::{
        facts::{AttemptFactBank, FactPositionLoanObligation, FactRole},
        requests::AttemptRequests,
    };
    use novarocks_control_runtime_core::{
        effect::{EffectDescriptor, EffectKind},
        identity::ActionId,
    };
    use novarocks_coordination_contract::logical_port::{
        AttemptToLogical as CanonicalMessage, AttemptToLogicalBody as CanonicalBody,
        LogicalToAttempt as CanonicalReply,
    };
    use std::{num::NonZeroUsize, task::Wake};

    struct HostWake {
        runtime: ActorRuntime<FrontendActor>,
        registration: ActorRegistration,
    }
    impl Wake for HostWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self.runtime.notify_host_actor(&self.registration);
        }
    }
    // The receiving actor is deliberately a scripted carrier, not a second
    // logical policy engine. Every reply uses an actual same-LP effect ticket.
    struct LogicalReceiver {
        attempt: ActorId,
        seen: Arc<Mutex<Vec<CanonicalMessage>>>,
        next: Arc<Mutex<Option<FrontendEffect>>>,
    }
    impl TestActor for LogicalReceiver {
        fn handle(
            &mut self,
            _: Tick,
            envelope: InputEnvelope,
            input: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            assert!(step.charge(1));
            if matches!(input, FrontendInput::Test(_)) {
                assert_eq!(envelope.class, InputClass::Lifecycle);
                let effect = self
                    .next
                    .lock()
                    .unwrap()
                    .take()
                    .expect("one original script reply");
                let FrontendEffect::Logical(original) = &effect else {
                    unreachable!()
                };
                let LogicalHostEffect::Protocol(original) = &original.body else {
                    unreachable!()
                };
                step.effect(
                    EffectDescriptor {
                        id: ActionId::new(original.action.get()).unwrap(),
                        work: step
                            .input_work()
                            .expect("same original LP financial author"),
                        kind: EffectKind::Settlement,
                        dependencies: Vec::new(),
                    },
                    effect,
                )
                .unwrap();
                return;
            }
            assert_eq!(envelope.source, self.attempt);
            let FrontendInput::Logical(input) = input else {
                panic!("original final LP inlet")
            };
            let novarocks_logical_execution_protocol::input::LogicalInput::AttemptPort(message) =
                *input
            else {
                unreachable!()
            };
            let expected_class = match &message.body {
                CanonicalBody::AuthorizationRequest { .. }
                | CanonicalBody::EstablishSettlement { .. }
                | CanonicalBody::PreReadyReplacementQuery { .. } => InputClass::Ordinary,
                _ => InputClass::Lifecycle,
            };
            assert_eq!(envelope.class, expected_class);
            self.seen.lock().unwrap().push(*message);
        }
    }
    struct LogicalReceivingSource {
        scope: ScopeHandle,
        closing: bool,
    }
    impl EffectSource for LogicalReceivingSource {
        fn prepay_result(
            &mut self,
            _: &FrontendEffect,
        ) -> Option<CausalInputObligation<FrontendActor>> {
            unreachable!("the original script ticket is completed before Host indexing")
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
            self.closing = true;
            self.scope.close_external().unwrap();
        }
        fn result_admission_closed(&self) -> bool {
            self.closing
        }
        fn progress_retirement(
            &mut self,
            runtime: &ActorRuntime<FrontendActor>,
            turn: &ActorHostTurn<FrontendActor>,
            _: Tick,
            budget: usize,
        ) -> SourceProgress {
            SourceProgress {
                units: usize::from(budget != 0),
                progressed: false,
                more: budget == 0 && self.closing,
                retired: budget != 0
                    && self.closing
                    && runtime
                        .snapshot(turn.registration().actor())
                        .unwrap()
                        .inputs
                        == 0
                    && self.scope.join_ready().unwrap(),
            }
        }
    }
    // Three final families have their original paid roles. Admission and
    // Establish share exactly ONE Authorization call/result credit.
    struct Row {
        token: Option<AttemptRequestToken>,
        job: Option<PreparedEffectJob>,
    }
    struct PortCalls {
        scope: ScopeHandle,
        calls: [Arc<Mutex<AttemptCallSource>>; 3],
        process: Arc<Mutex<AttemptCallSource>>,
        requests: Arc<AttemptRequests>,
        bank: Arc<AttemptFactBank>,
        material: Arc<AttemptEstablishMaterial>,
        compatibility: NativeCompatibilityId,
        executor: ActorTransportExecutor,
        published: Arc<AtomicUsize>,
        loan: Option<FactPositionLoanObligation>,
        selected: Option<FactRole>,
        rows: [Option<Row>; 3],
        next: usize,
        closed: bool,
    }
    impl EffectSource for PortCalls {
        fn prepay_result(
            &mut self,
            effect: &FrontendEffect,
        ) -> Option<CausalInputObligation<FrontendActor>> {
            let FrontendEffect::Attempt(effect) = effect else {
                unreachable!()
            };
            self.selected = FactRole::for_protocol_effect(&effect.protocol.body);
            let Some(role) = self.selected else {
                return self.process.lock().unwrap().prepay(effect).unwrap();
            };
            if self.rows[role.index()].is_some() || !self.requests.has_capacity() {
                return None;
            }
            let loan = self.bank.try_loan(role)?;
            let result = self.calls[role.index()]
                .lock()
                .unwrap()
                .prepay(effect)
                .unwrap()?;
            assert!(self.loan.replace(loan).is_none());
            Some(result)
        }
        fn bind_job(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &ActorHostTurn<FrontendActor>,
            ticket: &EffectTicket<FrontendEffect>,
            result: CausalInputObligation<FrontendActor>,
        ) -> PreparedEffectJob {
            assert!(self.selected.is_none());
            self.process.lock().unwrap().bind(ticket, result)
        }
        fn try_bind_effect(
            &mut self,
            runtime: &ActorRuntime<FrontendActor>,
            turn: &ActorHostTurn<FrontendActor>,
            ticket: &EffectTicket<FrontendEffect>,
            result: CausalInputObligation<FrontendActor>,
        ) -> Result<PreparedEffectBinding, EffectBindRefusal> {
            let Some(role) = self.selected else {
                return Ok(PreparedEffectBinding::Job(
                    self.bind_job(runtime, turn, ticket, result),
                ));
            };
            let FrontendEffect::Attempt(effect) = &ticket.effect else {
                unreachable!()
            };
            let original = match &effect.protocol.body {
                EffectBody::EstablishAuthorization { operation, request } => {
                    let AttemptToLogicalBody::AuthorizationRequest {
                        request: authorization,
                        ..
                    } = request.body
                    else {
                        unreachable!()
                    };
                    self.material
                        .for_authorization(operation, authorization)
                        .unwrap();
                    request
                }
                EffectBody::Port(original) => original,
                _ => unreachable!(),
            };
            let message = novarocks_attempt_protocol::port_projection::project_attempt_message(
                original,
                &mut |_| None,
                &mut |p, op| self.material.authorization_facts(p, op, self.compatibility),
                &mut |_, _, _| None,
            )
            .unwrap();
            let request = match message.body {
                CanonicalBody::AuthorizationRequest { token, .. }
                | CanonicalBody::PreReadyReplacementQuery { token, .. }
                | CanonicalBody::EstablishSettlement { token, .. } => Some(token),
                _ => None,
            };
            let mut job = self.calls[role.index()]
                .lock()
                .unwrap()
                .bind(ticket, result);
            let transport = job.transport.take();
            let row = if request.is_some() {
                self.requests.insert(&message, job).unwrap();
                Row {
                    token: request,
                    job: None,
                }
            } else {
                Row {
                    token: None,
                    job: Some(job),
                }
            };
            assert!(self.rows[role.index()].replace(row).is_none());
            self.loan
                .take()
                .unwrap()
                .publish_now(runtime, ticket, message)
                .unwrap();
            // The complete SAME job is retained before current-ticket commit.
            // The original Host tracks its Join before this Source can launch it.
            Ok(PreparedEffectBinding::Retained { transport })
        }
        fn return_unclaimed_result(&mut self, result: CausalInputObligation<FrontendActor>) {
            drop(self.loan.take());
            match self.selected {
                Some(role) => self.calls[role.index()]
                    .lock()
                    .unwrap()
                    .return_unclaimed(result),
                None => self.process.lock().unwrap().return_unclaimed(result),
            }
        }
        fn core_consumed(&mut self, _: &[InputId]) {}
        fn transport_event(&mut self, action: AttemptActionId, event: TransportSourceEvent) {
            let call = if action.get() == 6 {
                &self.process
            } else {
                &self.calls[if action.get() <= 4 {
                    FactRole::Authorization
                } else if action.get() == 5 {
                    FactRole::PreReady
                } else {
                    FactRole::Lifecycle
                }
                .index()]
            };
            let mut call = call.lock().unwrap();
            match event {
                TransportSourceEvent::Result(original) => {
                    call.accept(original)
                        .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)));
                    if action.get() >= 6 {
                        self.published.fetch_add(1, Ordering::SeqCst);
                    }
                }
                TransportSourceEvent::Retired => call.transport_retired(action),
                original => std::panic::resume_unwind(Box::new(original)),
            }
        }
        fn owner_failed_pending(&mut self, original: PendingHandoff) {
            std::panic::resume_unwind(Box::new(original));
        }
        fn close_external(&mut self) {
            self.closed = true;
            self.scope.close_external().unwrap();
        }
        fn result_admission_closed(&self) -> bool {
            self.closed
        }
        fn progress_retirement(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &ActorHostTurn<FrontendActor>,
            _: Tick,
            budget: usize,
        ) -> SourceProgress {
            let mut units = 0;
            let mut progressed = false;
            while units < budget.min(4) {
                let index = self.next;
                self.next = (self.next + 1) % 4;
                units += 1;
                if index < 3 {
                    if let Some(row) = &mut self.rows[index] {
                        if let Some(token) = row.token
                            && let Some(original) = self.requests.take_replied(token)
                        {
                            assert_eq!(original.token, token);
                            row.token = None;
                            row.job = Some(original.job);
                            progressed = true;
                        }
                        if let Some(job) = row.job.take() {
                            match self.executor.handoff(job) {
                                Ok(()) => {
                                    self.rows[index] = None;
                                    progressed = true;
                                }
                                Err(job) => row.job = Some(job),
                            }
                        }
                    }
                    progressed |= self.calls[index].lock().unwrap().retire_result().unwrap();
                } else {
                    progressed |= self.process.lock().unwrap().retire_result().unwrap();
                }
            }
            SourceProgress {
                units,
                progressed,
                more: progressed,
                retired: self.closed && self.scope.join_ready().unwrap(),
            }
        }
    }

    let fixture = Fixture::new();
    let generation = Generation::new(1).unwrap();
    let (mut parent, mut payer) = fixture
        .scope
        .reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let participation = ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(9, 11), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    );
    let compatibility = NativeCompatibilityId::new([31; 32]);
    let lease = LeaseValidFor::new(Duration::from_secs(30)).unwrap();
    let admission = AcquireParticipationAdmissionTicket::new(
        TaskOperationId::new_v7(),
        participation,
        lease,
        compatibility,
        AdmissionEpochCapability::try_from_bytes([9; 16]).unwrap(),
    );
    let ticket = AdmissionTicketId::try_from_bytes([31; 16]).unwrap();
    let facts = AttemptEstablishFacts::freeze(
        Default::default(),
        [(participation.backend_process_id(), Default::default())],
        Default::default(),
        &QueryCredentialLeases::empty(),
    )
    .unwrap();
    let mut next_payload = NonZeroU64::MIN;
    let (quotes, material) =
        AttemptEstablishMaterial::project(&facts, [participation], &mut next_payload).unwrap();
    let operation_id = TaskOperationId::new_v7();
    let quote = quotes[&participation];
    let activation = novarocks_coordination_contract::AttemptActivationIdentity::from_parts(
        1,
        participation.query_execution_id(),
        1,
    );
    let id = |number| NonZeroU64::new(number).unwrap();
    let port = |number, body| AttemptToLogical {
        activation,
        action: AttemptActionId::new(id(number)),
        body,
    };
    let admission_issue = AttemptAuthorizationRef::new(id(1));
    let authorization = AttemptAuthorizationRef::new(id(3));
    let token = |number| AttemptRequestToken::new(id(number));
    let admission_effect = |number, class, request| {
        AttemptEffectEnvelope::new(AttemptEffect {
            id: AttemptActionId::new(id(number)),
            class,
            body: EffectBody::Port(port(
                number,
                AttemptToLogicalBody::AuthorizationRequest {
                    token: token(number),
                    request,
                },
            )),
        })
    };
    let begin = admission_effect(
        1,
        EffectClass::Business,
        AttemptAuthorization::BeginAdmission(admission),
    );
    let grant = admission_effect(
        2,
        EffectClass::Settlement,
        AttemptAuthorization::SettleAdmission {
            issue: admission_issue,
            participation,
            operation_id: admission.envelope().operation_id(),
            outcome: Some(OperationOutcome::Accepted),
            receipt: Some(ParticipationAdmissionTicketReceipt::new(
                ticket,
                participation,
                lease,
            )),
            prior_transport_unknown: false,
        },
    );
    let first = AttemptEffectEnvelope::new(AttemptEffect {
        id: AttemptActionId::new(id(3)),
        class: EffectClass::Business,
        body: EffectBody::EstablishAuthorization {
            operation: OperationIntent::EstablishParticipation {
                envelope: OperationEnvelope::with_default_wait(
                    operation_id,
                    OperationKind::UpdateParticipation,
                ),
                participation,
                ticket,
                initial_lease_sequence: LeaseSequence::INITIAL,
                initial_lease_valid_for: lease,
                material: quote.material,
                queued_bytes: quote.queued_bytes,
            },
            request: port(
                3,
                AttemptToLogicalBody::AuthorizationRequest {
                    token: token(3),
                    request: AttemptAuthorization::Establish {
                        participation,
                        operation_id,
                    },
                },
            ),
        },
    });
    let second = AttemptEffectEnvelope::new(AttemptEffect {
        id: AttemptActionId::new(id(4)),
        class: EffectClass::Settlement,
        body: EffectBody::Port(port(
            4,
            AttemptToLogicalBody::EstablishSettlement {
                token: token(4),
                authorization,
                participation,
                operation_id,
                settlement: AttemptIssueSettlement::DefinitelyUnsent,
            },
        )),
    });
    assert!(
        [&begin, &grant, &first, &second]
            .into_iter()
            .all(|effect| effect.handoff().is_none())
    );
    let refusal = novarocks_attempt_protocol::failure::AttemptFailure::TaskProtocol(Box::new(
        novarocks_attempt_protocol::error::TaskExecutionError::PreReadyEstablishRejected {
            participation,
            operation_id,
            rejection: novarocks_execution_contract::operation::OperationResult::Verdict {
                outcome: OperationOutcome::IdentityMismatch,
                detail: None,
                acknowledgement: None,
            },
        },
    ));
    let permission = AttemptEffectEnvelope::new(AttemptEffect {
        id: AttemptActionId::new(id(5)),
        class: EffectClass::Settlement,
        body: EffectBody::Port(port(
            5,
            AttemptToLogicalBody::PreReadyReplacementQuery {
                token: token(5),
                refusal: refusal.clone(),
            },
        )),
    });
    let sample = AttemptEffectEnvelope::new(AttemptEffect {
        id: AttemptActionId::new(id(6)),
        class: EffectClass::Settlement,
        body: EffectBody::ObserveProcess {
            request: token(6),
            participation,
        },
    });
    let terminal = AttemptEffectEnvelope::new(AttemptEffect {
        id: AttemptActionId::new(id(7)),
        class: EffectClass::Settlement,
        body: EffectBody::Port(port(
            7,
            AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(refusal)),
        )),
    });
    let closure = Arc::new(AttemptConvergenceClosure::Evidence(vec![(
        participation,
        AttemptParticipationConvergence::NeverEstablished,
    )]));
    let progress = |id, local_exited, closure, lifecycle_runtime_filters| {
        AttemptEffectEnvelope::new(AttemptEffect {
            id: AttemptActionId::new(NonZeroU64::new(id).unwrap()),
            class: EffectClass::Reply,
            body: EffectBody::Port(port(
                id,
                AttemptToLogicalBody::ConvergenceProgress {
                    local_exited,
                    closure,
                    lifecycle_runtime_filters,
                },
            )),
        })
    };
    let selection = Arc::new(novarocks_coordination_contract::RuntimeFilterDiagnostic {
        complete: false,
        contributions: Vec::new(),
    });
    let remote = progress(8, false, Some(closure.clone()), None);
    let local = progress(9, true, None, Some(selection.clone()));
    let mailbox = [10, 8, 8, 8];
    let inbox = WithdrawalInbox::with_commands(
        &fixture.scope,
        [
            begin, grant, first, second, permission, sample, terminal, remote, local,
        ]
        .map(|effect| (effect, None)),
        mailbox,
    );
    let runtime = &inbox.runtime;
    let logical_scope = fixture.ledger.open_scope_next(None, generation).unwrap();
    let (mut logical_work, mut logical_payer) = logical_scope
        .reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    logical_work.start().unwrap();
    let logical_seen = Arc::new(Mutex::new(Vec::new()));
    let next_reply = Arc::new(Mutex::new(None));
    let config = ActorConfig {
        mailbox: [16; 4],
        max_batches: 16,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 12,
        max_step_work: 2,
    };
    let logical = runtime
        .register_bound(
            ActorId::new(31).unwrap(),
            logical_scope.clone(),
            FrontendActor::Test(Box::new(LogicalReceiver {
                attempt: inbox.registration.actor(),
                seen: logical_seen.clone(),
                next: next_reply.clone(),
            })),
            config,
        )
        .unwrap_or_else(|(error, _)| panic!("same actual LP carrier actor: {error:?}"));
    let ap_wake = Waker::from(Arc::new(HostWake {
        runtime: runtime.clone(),
        registration: inbox.registration.clone(),
    }));
    let lp_wake = Waker::from(Arc::new(HostWake {
        runtime: runtime.clone(),
        registration: logical.clone(),
    }));
    let bank = AttemptFactBank::reserve(
        runtime,
        &logical,
        &inbox.registration,
        activation,
        &logical_work,
        ap_wake.clone(),
        lp_wake,
    )
    .unwrap();
    let requests = AttemptRequests::new(
        activation,
        inbox.registration.clone(),
        logical.clone(),
        NonZeroUsize::MIN,
        ap_wake,
    );
    let material = Arc::new(material);
    let calls = FactRole::ALL.map(|role| {
        Arc::new(Mutex::new(AttemptCallSource::new(
            &parent,
            &payer,
            generation,
            observer(),
            AttemptCallOwner::LogicalPort(role),
            CausalInputObligation::reserve_admitted(
                runtime,
                &inbox.registration,
                logical.actor(),
                InputClass::Lifecycle,
                &parent,
            )
            .unwrap(),
        )))
    });
    let source = calls[FactRole::Authorization.index()].clone();
    let endpoint = RuntimeEndpoint::new("127.0.0.1", 1701).unwrap();
    let replacement = BackendProcessId::new_v7();
    let processes = Arc::new(FrozenProcess {
        original: participation,
        endpoint: endpoint.clone(),
        replacement,
    });
    let members: Arc<[FrozenMember]> = Arc::from([FrozenMember {
        participation,
        endpoint,
    }]);
    let process_source = ActorId::new(3).unwrap();
    let process = Arc::new(Mutex::new(AttemptCallSource::new(
        &parent,
        &payer,
        generation,
        observer(),
        AttemptCallOwner::Process(Arc::new(AttemptProcessObserver::new(
            processes.clone(),
            members.clone(),
            Arc::new(SampleClock),
        ))),
        CausalInputObligation::reserve_admitted(
            &inbox.runtime,
            &inbox.registration,
            process_source,
            InputClass::Lifecycle,
            &parent,
        )
        .unwrap(),
    )));
    let published = Arc::new(AtomicUsize::new(0));
    let host = HostRegistry::new(
        inbox.runtime.clone(),
        fixture.ledger.clone(),
        Arc::new(FrontendHostObservations::new(Arc::new(
            ProcessClock::default(),
        ))),
    );
    let handle = Handle::current();
    let mut host_exit = host
        .register(HostBinding {
            scope: fixture.scope.clone(),
            registration: inbox.registration.clone(),
            kind: FrontendActorKind::Test,
            config: ActorConfig {
                mailbox,
                ..WithdrawalInbox::CONFIG
            },
            policy: FrontendLanePolicy::attempt(1),
            executor: Arc::new(ActorTransportExecutor(handle.clone())),
            source: Box::new(PortCalls {
                scope: fixture.scope.clone(),
                calls: calls.clone(),
                process: process.clone(),
                requests: requests.clone(),
                bank: bank.clone(),
                material: material.clone(),
                compatibility,
                executor: ActorTransportExecutor(handle.clone()),
                published: published.clone(),
                loan: None,
                selected: None,
                rows: std::array::from_fn(|_| None),
                next: 0,
                closed: false,
            }),
            transport: Some(TransportRetirements::new(
                inbox.runtime.clone(),
                inbox.registration.clone(),
                handle.clone(),
                1,
            )),
            startup: None,
            capacity: None,
            membership: None,

            local_exit: None,
            ordered_observations: None,
        })
        .unwrap_or_else(|_| panic!("original QA host registration"));
    let actual_ap_exit = host_exit.observe_actual_exit();
    let mut logical_exit = host
        .register(HostBinding {
            scope: logical_scope.clone(),
            registration: logical.clone(),
            kind: FrontendActorKind::Test,
            config,
            policy: FrontendLanePolicy::logical(1),
            source: Box::new(LogicalReceivingSource {
                scope: logical_scope.clone(),
                closing: false,
            }),
            executor: Arc::new(ActorTransportExecutor(handle)),
            transport: None,
            startup: None,
            capacity: None,
            membership: None,
            local_exit: None,
            ordered_observations: None,
        })
        .unwrap_or_else(|_| panic!("actual LP receiving Host and Source"));
    let reply_triggers = Mutex::new(Vec::new());
    let reply = |number, body| {
        let message = CanonicalReply { activation, body };
        let original = FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope {
            reservation: logical_payer.id(),
            body: LogicalHostEffect::Protocol(Box::new(
                novarocks_logical_execution_protocol::effect::LogicalEffect {
                    action: novarocks_logical_execution_protocol::LogicalActionId::new(id(number)),
                    body:
                        novarocks_logical_execution_protocol::effect::LogicalEffectBody::AttemptPort(
                            Box::new(message.clone()),
                        ),
                },
            )),
        }));
        assert!(next_reply.lock().unwrap().replace(original).is_none());
        let position = runtime
            .reserve_admitted_input_for(
                &logical,
                logical.actor(),
                InputClass::Lifecycle,
                &logical_work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|_| panic!("same original LP reply trigger"));
        let input = position.fill(FrontendInput::Test(1)).unwrap();
        for _ in 0..16 {
            runtime.poll_one(Tick(1)).unwrap();
            if runtime
                .effect_inventory(logical.actor(), input, None, 1)
                .is_ok_and(|v| !v.is_empty())
            {
                break;
            }
        }
        let ticket = runtime
            .take_effect(logical.actor(), input, ActionId::new(number).unwrap())
            .unwrap();
        let original_input = requests
            .deliver_reply_now(runtime, &ticket, message)
            .unwrap();
        assert!(
            runtime
                .input_custody(inbox.registration.actor(), original_input)
                .unwrap()
                .is_some()
        );
        runtime.effect_completed(ticket).unwrap();
        assert_eq!(
            runtime
                .effect_inventory(logical.actor(), input, None, 1)
                .unwrap()[0]
                .1,
            novarocks_control_runtime_core::effect::EffectState::Completed
        );
        // The synchronous original script delivered its SAME current ticket.
        // Retire that completed input before this passive LP Host indexes it;
        // keep its original idle role until the final explicit close below.
        assert!(position.retire_ready_after_exit().unwrap());
        published.fetch_add(1, Ordering::SeqCst);
        reply_triggers.lock().unwrap().push(position);
    };
    let submit_pair = || {
        let commands = [inbox.submit_command(&parent), inbox.submit_command(&parent)];
        assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
        commands
    };
    let retire_commands = |commands: &[InputId]| {
        for input in commands {
            inbox
                .runtime
                .retire_input(inbox.registration.actor(), *input)
                .unwrap();
        }
    };
    let commit_reply = || {
        let (_, AttemptInput::Port(reply)) =
            inbox.try_commit_attempt_from(logical.actor()).unwrap()
        else {
            panic!("same original QA reply");
        };
        reply.body
    };
    let drive_host = || {
        bank.progress(3, false, false).unwrap();
        while inbox.runtime.poll_one(Tick(1)).unwrap() {}
        while let Some(turn) = inbox.runtime.claim_host_actor() {
            host.drive(Tick(1), turn);
        }
    };
    let await_reply = |count| {
        let published = &published;
        let drive_host = &drive_host;
        async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                while published.load(Ordering::SeqCst) < count {
                    drive_host();
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    };
    let await_fact = |number: u64| {
        let drive_host = &drive_host;
        let seen = &logical_seen;
        async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    drive_host();
                    if let Some(original) = seen
                        .lock()
                        .unwrap()
                        .iter()
                        .find(|m| m.action.get() == number)
                        .cloned()
                    {
                        break original;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap()
        }
    };
    let commands = submit_pair();
    let first = await_fact(1).await;
    assert!(
        matches!(first.body, CanonicalBody::AuthorizationRequest { token: t,
        request: novarocks_coordination_contract::logical_port::AttemptAuthorization::BeginAdmission(original),
    } if t == token(1) && original.envelope() == admission.envelope()
        && original.participation() == participation && original.valid_for() == lease
        && original.native_compatibility_id() == compatibility
        && original.admission_epoch_capability() == admission.admission_epoch_capability())
    );
    drive_host();
    assert!(
        logical_seen
            .lock()
            .unwrap()
            .iter()
            .all(|m| m.action.get() != 2),
        "second original authorization cannot overwrite the original outstanding reply"
    );
    assert!(!source.lock().unwrap().retire_result().unwrap());
    assert_eq!(published.load(Ordering::SeqCst), 0);
    // Explicit script protocol references confer no WC resource grant.
    reply(
        1,
        LogicalToAttemptBody::AuthorizationReply {
            token: token(1),
            decision: Ok(AttemptAuthorizationDecision::AdmissionStarted(
                admission_issue,
            )),
        },
    );
    await_reply(1).await;
    assert!(
        matches!(commit_reply(), LogicalToAttemptBody::AuthorizationReply {
        token: reply_token, decision: Ok(AttemptAuthorizationDecision::AdmissionStarted(issue)),
    } if reply_token == token(1) && issue == admission_issue)
    );
    let granted = await_fact(2).await;
    assert!(
        matches!(granted.body, CanonicalBody::AuthorizationRequest { token: t,
        request: novarocks_coordination_contract::logical_port::AttemptAuthorization::SettleAdmission {
            issue, participation: p, operation_id: op, outcome: Some(OperationOutcome::Accepted), receipt: Some(r), prior_transport_unknown: false,
        },
    } if t == token(2) && issue == admission_issue && p == participation
        && op == admission.envelope().operation_id() && r.ticket_id() == ticket)
    );
    reply(
        2,
        LogicalToAttemptBody::AuthorizationReply {
            token: token(2),
            decision: Ok(AttemptAuthorizationDecision::AdmissionGranted),
        },
    );
    await_reply(2).await;
    assert!(
        matches!(commit_reply(), LogicalToAttemptBody::AuthorizationReply {
        token: reply_token, decision: Ok(AttemptAuthorizationDecision::AdmissionGranted),
    } if reply_token == token(2))
    );
    retire_commands(&commands);
    drive_host();

    let commands = submit_pair();
    let establish = await_fact(3).await;
    let CanonicalBody::AuthorizationRequest {
        token: t,
        request:
            novarocks_coordination_contract::logical_port::AttemptAuthorization::Establish {
                participation: p,
                operation_id: op,
                ticket_id,
                semantics,
                native_compatibility_id,
            },
    } = establish.body
    else {
        panic!("same original complete Establish metadata")
    };
    let expected = material
        .authorization_facts(participation, operation_id, compatibility)
        .unwrap();
    assert_eq!(
        (t, p, op, ticket_id, semantics, native_compatibility_id),
        (
            token(3),
            participation,
            operation_id,
            ticket,
            expected.semantics,
            compatibility
        )
    );
    drive_host();
    assert!(
        logical_seen
            .lock()
            .unwrap()
            .iter()
            .all(|m| m.action.get() != 4)
    );
    assert!(!source.lock().unwrap().retire_result().unwrap());
    reply(
        3,
        LogicalToAttemptBody::AuthorizationReply {
            token: token(3),
            decision: Ok(AttemptAuthorizationDecision::EstablishAuthorized(
                authorization,
            )),
        },
    );
    await_reply(3).await;
    assert!(
        matches!(commit_reply(), LogicalToAttemptBody::AuthorizationReply {
        token: reply_token, decision: Ok(AttemptAuthorizationDecision::EstablishAuthorized(guard)),
    } if reply_token == token(3) && guard == authorization)
    );
    let settled = await_fact(4).await;
    assert!(
        matches!(settled.body, CanonicalBody::EstablishSettlement { token: t,
        authorization: original, participation: p, operation_id: op,
        settlement: AttemptIssueSettlement::DefinitelyUnsent,
    } if t == token(4) && original == authorization && p == participation && op == operation_id)
    );
    reply(
        4,
        LogicalToAttemptBody::EstablishSettlementReply {
            token: token(4),
            authorization,
            result: Ok(()),
        },
    );
    await_reply(4).await;
    assert!(
        matches!(commit_reply(), LogicalToAttemptBody::EstablishSettlementReply {
        token: reply_token, authorization: guard, result: Ok(()),
    } if reply_token == token(4) && guard == authorization)
    );
    retire_commands(&commands);
    drive_host();
    // QA permission and the independent residual convergence process read
    // have different source authors and neither can stand in for the other.
    let permission_command = inbox.submit_command(&parent);
    assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
    let permission = await_fact(5).await;
    let CanonicalBody::PreReadyReplacementQuery {
        token: exact,
        refusal: original_refusal,
    } = permission.body
    else {
        unreachable!()
    };
    assert_eq!(exact, token(5));
    assert!(original_refusal.pre_ready_replacement().is_none());
    reply(
        5,
        LogicalToAttemptBody::PreReadyReplacementReply {
            token: token(5),
            may_observe: false,
        },
    );
    await_reply(5).await;
    assert!(
        matches!(commit_reply(), LogicalToAttemptBody::PreReadyReplacementReply {
        token: reply_token, may_observe: false,
    } if reply_token == token(5))
    );
    inbox
        .runtime
        .retire_input(inbox.registration.actor(), permission_command)
        .unwrap();
    let sample_command = inbox.submit_command(&parent);
    assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
    await_reply(6).await;
    assert!(matches!(inbox.try_commit_attempt_from(process_source),
        Some((_, AttemptInput::Executor(ExecutorFact::ProcessSnapshot {
            request, participation: exact, sampled_at,
            observation: novarocks_attempt_protocol::input::MembershipObservation::ExactReplacement(observed),
        }))) if request == token(6) && exact == participation && observed == replacement
            && sampled_at == MonotonicInstant::from_origin(Duration::from_nanos(1))));
    inbox
        .runtime
        .retire_input(inbox.registration.actor(), sample_command)
        .unwrap();
    drive_host();
    let terminal_command = inbox.submit_command(&parent);
    assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
    await_reply(7).await;
    let terminal_fact = await_fact(7).await;
    let CanonicalBody::AttemptTerminal(
        novarocks_coordination_contract::logical_port::AttemptTerminal::Failed(failure),
    ) = terminal_fact.body
    else {
        panic!("original typed refusal actual handoff")
    };
    assert!(failure.pre_ready_replacement().is_none());
    assert_eq!(
        failure.input().site(),
        novarocks_query_application::execution_error::ExecutionFailureSite::Operation {
            participation,
            operation_id,
            kind: OperationKind::UpdateParticipation
        }
    );
    inbox
        .runtime
        .retire_input(inbox.registration.actor(), terminal_command)
        .unwrap();
    drive_host();
    inbox.assert_no_result();
    let remote_command = inbox.submit_command(&parent);
    assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
    await_reply(8).await;
    let original_remote = await_fact(8).await;
    assert!(
        matches!(&original_remote.body, CanonicalBody::ConvergenceProgress {
        local_exited: false, closure: Some(original), lifecycle_runtime_filters: None,
    } if Arc::ptr_eq(original, &closure))
    );
    assert!(
        !*actual_ap_exit.borrow(),
        "positive remote metadata is not actual Source exit"
    );
    retire_commands(&[remote_command]);
    let local_command = inbox.submit_command(&parent);
    assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
    await_reply(9).await;
    let original_local = await_fact(9).await;
    assert!(
        matches!(&original_local.body, CanonicalBody::ConvergenceProgress {
        local_exited: true, closure: None, lifecycle_runtime_filters: Some(original),
    } if Arc::ptr_eq(original, &selection))
    );
    assert!(
        !*actual_ap_exit.borrow(),
        "original parent/capture boundary remains independent"
    );
    retire_commands(&[local_command]);
    drive_host();
    inbox.assert_no_result();
    let process_owner = {
        let mut sample_owner = process.lock().unwrap();
        assert!(sample_owner.close_idle().unwrap());
        sample_owner
            .take_drained_owner()
            .expect("original Process owner retired")
    };
    drop(process_owner);
    for call in &calls {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                drive_host();
                if call.lock().unwrap().close_idle().unwrap() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let original = call
            .lock()
            .unwrap()
            .take_drained_owner()
            .expect("same final call owner after actual Join");
        drop(original);
    }
    assert!(requests.is_empty());
    host_exit.request_close().unwrap();
    drive_host();
    {
        let mut waiting = Box::pin(host_exit.wait());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drive_host();
        assert!(futures::poll!(waiting.as_mut()).is_pending());
    }
    assert!(!*actual_ap_exit.borrow());
    parent.body_exited().unwrap();
    parent.captures_exited().unwrap();
    payer.finish().unwrap();
    fixture.scope.reap_ready(128).unwrap();
    inbox
        .runtime
        .notify_host_actor(&inbox.registration)
        .unwrap();
    drive_host();
    tokio::time::timeout(Duration::from_secs(1), host_exit.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(*actual_ap_exit.borrow());
    assert!(
        matches!(&original_remote.body, CanonicalBody::ConvergenceProgress { closure: Some(original), .. } if Arc::ptr_eq(original, &closure))
    );
    assert!(
        matches!(&original_local.body, CanonicalBody::ConvergenceProgress { lifecycle_runtime_filters: Some(original), .. } if Arc::ptr_eq(original, &selection))
    );
    requests.close_admission();
    bank.original_producer_exited();
    let mut closed = false;
    for _ in 0..16 {
        while runtime.poll_one(Tick(1)).unwrap() {}
        while let Some(turn) = runtime.claim_host_actor() {
            host.drive(Tick(1), turn);
        }
        closed = bank.progress(3, true, false).unwrap().complete;
        if closed {
            break;
        }
    }
    assert!(closed && bank.is_closed());
    for mut position in reply_triggers.into_inner().unwrap() {
        assert!(position.retire_ready_after_exit().unwrap());
        position.close_after_exit().unwrap();
    }
    drop((requests, bank));
    logical_work.body_exited().unwrap();
    logical_work.captures_exited().unwrap();
    logical_payer.finish().unwrap();
    logical_scope.close_external().unwrap();
    fixture
        .ledger
        .reap_scope_ready(logical_scope.id(), 128)
        .unwrap();
    logical_exit.request_close().unwrap();
    while let Some(turn) = runtime.claim_host_actor() {
        host.drive(Tick(1), turn);
    }
    tokio::time::timeout(Duration::from_secs(1), logical_exit.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(host.is_empty() && runtime.is_empty() && fixture.ledger.is_empty());
}

#[tokio::test]
async fn original_admission_installs_wc_grant_and_payload_pin_before_publishing() {
    use crate::{
        control_runtime::{
            actor::FrontendActorKind,
            host::{
                EffectSource, FrontendLanePolicy, HostBinding, HostRegistry, PendingHandoff,
                PreparedEffectJob, SourceProgress,
            },
            metrics::FrontendHostObservations,
            transport_withdrawal::AttemptEffectEnvelope,
        },
        task_execution::{
            creation::{
                PricedTaskCreation, creation_owner_header_bytes, tests::original_creation_seed,
            },
            host::transport::{
                self, ActorTransportExecutor, AdmissionSource, FrozenSendRequests,
                PreparedSendBatch, SendBindingError, SendKey,
            },
        },
    };
    use novarocks_attempt_protocol::{
        effect::{
            AttemptEffect, DispatchBatch, EffectBody, EffectClass, OperationIntent,
            QueuedInputPayload,
        },
        input::SendGeneration,
        start::PayloadRef,
    };
    use novarocks_coordination_contract::{AttemptActionId, MonotonicInstant};
    use novarocks_execution_contract::{OperationEnvelope, OperationKind, TaskOperationId};
    use std::{num::NonZeroU64, time::Duration};

    let fixture = Fixture::new();
    let mut wc = transport::GovernedQueuedInputFixture::new();
    let generation = Generation::new(1).unwrap();
    let (mut parent, mut payer) = fixture
        .scope
        .reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let id = AttemptActionId::new(NonZeroU64::MIN);
    let key = SendKey {
        operation: TaskOperationId::new_v7(),
        generation: SendGeneration::new(id),
    };
    let payload = PayloadRef(NonZeroU64::MIN);
    let seed = original_creation_seed();
    let task = seed.identity();
    let (quote, original) = PricedTaskCreation::quote(seed, payload).unwrap();
    let backing_bytes = creation_owner_header_bytes().unwrap() + original.sizing.new_backing_bytes;
    let operation = OperationIntent::CreateTask {
        envelope: OperationEnvelope::with_default_wait(key.operation, OperationKind::CreateTask),
        identity: task,
        creation: payload,
        queued_bytes: quote.queued_bytes,
    };
    let withdrawn = key;
    let key = SendKey {
        generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::new(2).unwrap())),
        ..key
    };
    let replay = SendKey {
        generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::new(3).unwrap())),
        ..key
    };
    let effect = |send: SendKey, payload| {
        AttemptEffectEnvelope::new(AttemptEffect {
            id: send.generation.admission_effect(),
            class: EffectClass::Business,
            body: EffectBody::AdmitQueuedInput {
                lane: operation.lane(),
                operation: operation.clone(),
                generation: send.generation,
                payload,
            },
        })
    };
    struct Source {
        scope: ScopeHandle,
        admission: Arc<Mutex<AdmissionSource>>,
        requests: Arc<Mutex<FrozenSendRequests>>,
        completed: Arc<Mutex<VecDeque<InputId>>>,
        retired: Arc<AtomicUsize>,
        closing: bool,
    }
    impl EffectSource for Source {
        fn prepay_result(
            &mut self,
            effect: &FrontendEffect,
        ) -> Option<CausalInputObligation<FrontendActor>> {
            let FrontendEffect::Attempt(effect) = effect else {
                unreachable!()
            };
            self.admission
                .lock()
                .unwrap()
                .prepay(
                    effect,
                    &self.requests.lock().unwrap(),
                    novarocks_task_codec::TransportBudget::DEFAULT,
                )
                .unwrap()
        }
        fn bind_job(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &novarocks_control_runtime::actor_turn::ActorHostTurn<FrontendActor>,
            ticket: &novarocks_control_runtime::actor_turn::EffectTicket<FrontendEffect>,
            result: CausalInputObligation<FrontendActor>,
        ) -> PreparedEffectJob {
            self.admission.lock().unwrap().bind(ticket, result)
        }
        fn return_unclaimed_result(&mut self, result: CausalInputObligation<FrontendActor>) {
            self.admission.lock().unwrap().return_unclaimed(result);
        }
        fn core_consumed(&mut self, _: &[InputId]) {}
        fn transport_event(
            &mut self,
            effect: AttemptActionId,
            original: super::TransportSourceEvent,
        ) {
            match original {
                super::TransportSourceEvent::Result(result) => {
                    assert_eq!(result.effect.protocol.id, effect);
                    let input = self
                        .admission
                        .lock()
                        .unwrap()
                        .accept(result, &mut self.requests.lock().unwrap())
                        .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)))
                        .expect("original admitted input");
                    self.completed.lock().unwrap().push_back(input);
                }
                super::TransportSourceEvent::Retired => {
                    assert!(self.admission.lock().unwrap().transport_retired(effect));
                    self.retired.fetch_add(1, Ordering::SeqCst);
                }
                original => std::panic::resume_unwind(Box::new(original)),
            }
        }
        fn owner_failed_pending(&mut self, pending: PendingHandoff) {
            std::panic::resume_unwind(Box::new(pending));
        }
        fn close_external(&mut self) {
            self.closing = true;
            self.scope.close_external().unwrap();
        }
        fn result_admission_closed(&self) -> bool {
            self.closing
        }
        fn progress_retirement(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &novarocks_control_runtime::actor_turn::ActorHostTurn<FrontendActor>,
            _: Tick,
            budget: usize,
        ) -> SourceProgress {
            let mut admission = self.admission.lock().unwrap();
            let mut units = 0;
            let mut progressed = false;
            if budget != 0 && admission.awaiting_result_retirement() != 0 {
                progressed = admission.retire_result().unwrap();
                units += 1;
            }
            if self.closing {
                if units < budget && admission.close_one_idle().unwrap() {
                    units += 1;
                    progressed = true;
                }
                let reaped = self.scope.reap_ready(budget - units).unwrap();
                units += reaped;
                progressed |= reaped != 0;
            }
            let retired = self.closing && self.scope.join_ready().unwrap();
            SourceProgress {
                units,
                progressed,
                more: progressed && self.closing && !retired,
                retired,
            }
        }
    }
    let refused = effect(withdrawn, QueuedInputPayload::NewCreation(payload));
    let inbox = WithdrawalInbox::with_effect(
        &fixture.scope,
        [
            refused.clone(),
            effect(key, QueuedInputPayload::NewCreation(payload)),
            effect(replay, QueuedInputPayload::AlreadyRetained),
        ],
    );
    let position = CausalInputObligation::reserve_admitted(
        &inbox.runtime,
        &inbox.registration,
        inbox.source,
        InputClass::Lifecycle,
        &parent,
    )
    .unwrap();
    let command = || {
        inbox
            .runtime
            .reserve_admitted_input_for(
                &inbox.registration,
                inbox.source,
                InputClass::Lifecycle,
                &parent,
            )
            .unwrap()
            .fill(FrontendInput::Test(1))
            .unwrap()
    };
    let first_command = command();
    let mut admission = AdmissionSource::new(
        &parent,
        &payer,
        generation,
        observer(),
        wc.authority(),
        [VecDeque::from([position]), VecDeque::new()],
    )
    .unwrap_or_else(|_| panic!("original admission payer"));
    admission
        .install_creation(payload, original)
        .unwrap_or_else(|_| panic!("original creation"));
    let requests = Arc::new(Mutex::new(FrozenSendRequests::default()));
    let admission = Arc::new(Mutex::new(admission));
    let completed = Arc::new(Mutex::new(VecDeque::new()));
    let retired = Arc::new(AtomicUsize::new(0));
    let handle = Handle::current();
    let host = HostRegistry::new(
        inbox.runtime.clone(),
        fixture.ledger.clone(),
        Arc::new(FrontendHostObservations::new(Arc::new(
            novarocks_control_runtime::actor_turn::ProcessClock::default(),
        ))),
    );
    host.register(HostBinding {
        scope: fixture.scope.clone(),
        registration: inbox.registration.clone(),
        kind: FrontendActorKind::Test,
        config: WithdrawalInbox::CONFIG,
        policy: FrontendLanePolicy::attempt(1),
        executor: Arc::new(ActorTransportExecutor(handle.clone())),
        source: Box::new(Source {
            scope: fixture.scope.clone(),
            admission: admission.clone(),
            requests: requests.clone(),
            completed: completed.clone(),
            retired: retired.clone(),
            closing: false,
        }),
        transport: Some(super::TransportRetirements::new(
            inbox.runtime.clone(),
            inbox.registration.clone(),
            handle,
            1,
        )),
        startup: None,
        capacity: None,
        membership: None,

        local_exit: None,
        ordered_observations: None,
    })
    .unwrap_or_else(|_| panic!("same original host route"));

    assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
    let mut first_turns = 0;
    while let Some(turn) = inbox.runtime.claim_host_actor() {
        host.drive(Tick(1), turn);
        first_turns += 1;
        assert!(first_turns <= 4, "a pending Join must let the host sleep");
    }
    assert!(first_turns > 0);
    assert!(completed.lock().unwrap().is_empty());
    inbox
        .runtime
        .retire_input(inbox.registration.actor(), first_command)
        .unwrap();
    inbox.assert_no_result();
    assert!(!admission.lock().unwrap().can_release_creation(payload));
    assert!(
        admission
            .lock()
            .unwrap()
            .take_unclaimed_creation()
            .is_none()
    );
    refused.handoff().unwrap().withdraw();

    let drive = |wc: &mut transport::GovernedQueuedInputFixture| {
        wc.progress();
        while let Some(turn) = inbox.runtime.claim_host_actor() {
            host.drive(Tick(1), turn);
        }
        completed.lock().unwrap().pop_front()
    };
    let input = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(input) = drive(&mut wc) {
                break input;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        inbox.commit(input),
        ExecutorFact::QueuedInputWithdrawn { operation, generation }
            if operation == withdrawn.operation && generation == withdrawn.generation
    ));
    assert!(admission.lock().unwrap().can_release_creation(payload));
    assert!(
        requests
            .lock()
            .unwrap()
            .creation_for_test(key.operation)
            .is_none()
    );
    let create_command = command();
    assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
    let input = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(input) = drive(&mut wc) {
                break input;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !admission.lock().unwrap().retire_result().unwrap(),
        "uncommitted result still owns its paid role"
    );
    assert!(admission.lock().unwrap().take_unclaimed_grant().is_none());
    let frozen = requests
        .lock()
        .unwrap()
        .creation_for_test(key.operation)
        .expect("the frozen request is installed before Core consumes the grant");
    let ExecutorFact::QueuedInputGranted {
        operation: admitted_operation,
        grant,
        queued_bytes,
    } = inbox.commit(input)
    else {
        panic!("same queued input grant");
    };
    assert_eq!(
        (admitted_operation, queued_bytes, grant.generation()),
        (key.operation, quote.queued_bytes, key.generation)
    );
    let send = |operation: OperationIntent| {
        AttemptEffectEnvelope::new(AttemptEffect {
            id: AttemptActionId::new(NonZeroU64::new(4).unwrap()),
            class: EffectClass::Business,
            body: EffectBody::SendBatch {
                batch: DispatchBatch {
                    backend: task.backend_process_id(),
                    lane: operation.lane(),
                    control_progress: false,
                    operations: Box::new([operation]),
                    queued_bytes,
                    queued_at: MonotonicInstant::ORIGIN,
                    queued_inputs: Box::new([grant]),
                },
                reservation: novarocks_attempt_protocol::capacity::ReservationToken(
                    NonZeroU64::MIN,
                ),
            },
        })
    };
    {
        let requests = requests.lock().unwrap();
        let prepared = PreparedSendBatch::resolve(send(operation.clone()), &requests).unwrap();
        let [crate::task_execution::intent::OperationIntent::CreateTask(original)] =
            prepared.operations()
        else {
            panic!("resolved the exact first creation");
        };
        assert!(original.same_original(&frozen));
        let mut foreign = operation.clone();
        let OperationIntent::CreateTask { creation, .. } = &mut foreign else {
            unreachable!()
        };
        *creation = PayloadRef(NonZeroU64::new(2).unwrap());
        assert!(matches!(
            PreparedSendBatch::resolve(send(foreign), &requests),
            Err(SendBindingError::WrongRequest)
        ));
    }
    inbox
        .runtime
        .retire_input(inbox.registration.actor(), create_command)
        .unwrap();
    let second_command = command();
    assert!(inbox.runtime.poll_one(Tick(1)).unwrap());
    let input = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(input) = drive(&mut wc) {
                break input;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let ExecutorFact::QueuedInputGranted { grant: second, .. } = inbox.commit(input) else {
        panic!("same paid result role admitted a fresh send generation");
    };
    assert_eq!(second.generation(), replay.generation);
    assert_ne!(grant, second);
    inbox
        .runtime
        .retire_input(inbox.registration.actor(), second_command)
        .unwrap();
    assert!(drive(&mut wc).is_none());
    assert_eq!(retired.load(Ordering::SeqCst), 3);
    {
        let mut admission = admission.lock().unwrap();
        assert!(admission.take_grant(grant, replay).is_none());
        assert!(admission.take_grant(second, key).is_none());
        let first = admission.take_grant(grant, key).expect("same first grant");
        assert!(admission.take_unclaimed_creation().is_none());
        let replay_grant = admission
            .take_unclaimed_grant()
            .expect("same unbound replay grant");
        assert_eq!(replay_grant.key(), replay);
        assert!(admission.take_unclaimed_grant().is_none());
        assert_eq!(
            first.retain_payload().unwrap().capacity_bytes().unwrap(),
            backing_bytes as u64
        );
        assert_eq!(
            replay_grant
                .retain_payload()
                .unwrap()
                .capacity_bytes()
                .unwrap(),
            0
        );
        drop((first, replay_grant));
        let (original_task, original_payload, seed) = admission
            .take_unclaimed_creation()
            .expect("original source row");
        assert_eq!((original_task, original_payload), (task, payload));
        assert!(admission.take_unclaimed_creation().is_none());
        assert!(
            seed.lock().unwrap().is_none(),
            "the seed was frozen exactly once"
        );
        let Some(crate::task_execution::intent::OperationIntent::CreateTask(original)) =
            requests.lock().unwrap().retire_creation(task, payload)
        else {
            panic!("publication installed the frozen request");
        };
        assert_eq!(original.envelope().operation_id(), key.operation);
        assert_eq!(original.queued_bytes(), quote.queued_bytes);
        assert!(original.same_original(&frozen));
        drop((original, frozen));
    }
    fixture.scope.reap_ready(128).unwrap();
    parent.body_exited().unwrap();
    parent.captures_exited().unwrap();
    payer.finish().unwrap();
    host.close_registration();
    while let Some(turn) = inbox.runtime.claim_host_actor() {
        host.drive(Tick(1), turn);
    }
    assert!(host.is_empty() && inbox.runtime.is_empty() && fixture.ledger.is_empty());
    assert!(wc.finish());
}

/// Passive test observation of the SAME retained Join, including its actual
/// completed fact. It grants no start, cancellation, result or exit authority.
pub(crate) fn original_transport_join_id(
    original: &OriginalTransportJoinObligation,
) -> Option<tokio::task::Id> {
    let state = original
        .role
        .joined
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    state
        .join
        .as_ref()
        .map(tokio::task::JoinHandle::id)
        .or_else(|| state.facts.map(|facts| facts.id))
}
