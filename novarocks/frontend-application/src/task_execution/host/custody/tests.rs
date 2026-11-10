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

type ObservedWithdrawals = Arc<Mutex<Vec<(InputEnvelope, ExecutorFact)>>>;
struct WithdrawalTarget(ObservedWithdrawals);
impl TestActor for WithdrawalTarget {
    fn handle(
        &mut self,
        _: Tick,
        envelope: InputEnvelope,
        value: FrontendInput,
        _: &mut ActorStep<'_, FrontendEffect>,
    ) {
        let FrontendInput::Attempt(value) = value else {
            panic!("expected an original attempt input");
        };
        let AttemptInput::Executor(fact) = *value else {
            panic!("expected an original executor fact");
        };
        self.0.lock().unwrap().push((envelope, fact));
    }
}

pub(crate) struct WithdrawalInbox {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    source: ActorId,
    observed: ObservedWithdrawals,
}
impl WithdrawalInbox {
    pub(crate) fn new(scope: &ScopeHandle) -> Self {
        let runtime = ActorRuntime::new();
        let actor = ActorId::new(1).unwrap();
        let source = ActorId::new(2).unwrap();
        let observed: ObservedWithdrawals = Arc::default();
        let registration = runtime
            .register_bound_with_source_cut(
                actor,
                scope.clone(),
                FrontendActor::Test(Box::new(WithdrawalTarget(observed.clone()))),
                ActorConfig {
                    mailbox: [4; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
                source,
            )
            .unwrap_or_else(|(error, _)| panic!("{error}"));
        Self {
            runtime,
            registration,
            source,
            observed,
        }
    }
    pub(crate) fn reserve(&self, work: &ReportedWork) -> CausalInputObligation<FrontendActor> {
        CausalInputObligation::reserve_admitted(
            &self.runtime,
            &self.registration,
            self.source,
            InputClass::Lifecycle,
            work.work(),
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
        assert!(self.runtime.poll_one(Tick(1)).unwrap());
        let mut observed = self.observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        let (envelope, fact) = observed.pop().unwrap();
        assert_eq!(envelope.source, self.source);
        assert_eq!(envelope.class, InputClass::Lifecycle);
        assert_eq!(envelope.input, input);
        self.runtime
            .retire_input(self.registration.actor(), input)
            .unwrap();
        fact
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
        .err()
        .expect("another position of the same Work cannot substitute");
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

#[tokio::test]
async fn original_admission_installs_wc_grant_and_payload_pin_before_publishing() {
    use super::actor_result::AdmissionPublicationError;
    use crate::{
        control_runtime::{host::PreparedEffectJob, transport_withdrawal::AttemptEffectEnvelope},
        task_execution::host::transport::{
            self, ActorTransportHandoff, ActorTransportWork, BoundActorTransport, BoundQueue,
            GrantedInputs, SendKey, SourceLayouts,
        },
    };
    use novarocks_attempt_protocol::{
        effect::{AttemptEffect, EffectBody, EffectClass, OperationIntent, QueuedInputPayload},
        input::SendGeneration,
        start::PayloadRef,
    };
    use novarocks_coordination_contract::AttemptActionId;
    use novarocks_execution_contract::{
        OperationEnvelope, OperationKind, TaskIdentity, TaskOperationId,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::{num::NonZeroU64, time::Duration};

    let fixture = Fixture::new();
    let inbox = WithdrawalInbox::new(&fixture.scope);
    let mut wc = transport::GovernedQueuedInputFixture::new();
    let original = fixture.reported(None);
    let result = inbox.reserve(&original);
    let result_author = result.event_author().unwrap();
    let id = AttemptActionId::new(NonZeroU64::MIN);
    let key = SendKey {
        operation: TaskOperationId::new_v7(),
        generation: SendGeneration::new(id),
    };
    let payload = PayloadRef(NonZeroU64::MIN);
    let operation = OperationIntent::CreateTask {
        envelope: OperationEnvelope::with_default_wait(key.operation, OperationKind::CreateTask),
        identity: TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(0x7a5c, 6), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        ),
        creation: payload,
        queued_bytes: 8192,
    };
    let effect = AttemptEffectEnvelope::new(AttemptEffect {
        id,
        class: EffectClass::Business,
        body: EffectBody::AdmitQueuedInput {
            lane: operation.lane(),
            operation,
            generation: key.generation,
            payload: QueuedInputPayload::NewCreation(payload),
        },
    });
    let panic = PanicSink::new();
    let (mut owner, exit, launch) =
        OriginalTransportJoinObligation::prepare(original, observer(), panic.clone())
            .unwrap_or_else(|e| panic!("{}", e.error));
    let layouts = transport::actor_transport_layouts(false).unwrap();
    let quote = transport::queued_input_quote(8192, SourceLayouts { original: &layouts }).unwrap();
    let job = PreparedEffectJob {
        effect: FrontendEffect::Attempt(effect.clone()),
        exit,
        result,
        captures: Box::new(BoundActorTransport {
            effect,
            work: ActorTransportWork::Admit(Box::new(BoundQueue {
                key,
                scope_generation: Generation::new(1).unwrap(),
                control: false,
                quote,
                authority: wc.authority(),
                original_captures: Box::new(()),
            })),
            launch,
            panic,
            result_author,
        }),
    };
    assert!(matches!(
        transport::handoff_actor_transport(&Handle::current(), job),
        Ok(ActorTransportHandoff::Spawned)
    ));
    assert!(owner.take_actor_result().is_none());
    inbox.assert_no_result();
    let joined = tokio::time::timeout(
        Duration::from_secs(2),
        poll_fn(|cx| {
            wc.progress();
            let result = owner.poll_shared_completion(cx);
            if result.is_pending() {
                cx.waker().wake_by_ref();
            }
            result
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(joined.is_none());
    let mut result = owner.take_actor_result().unwrap();
    assert!(result.withdrawal.is_none());
    let mut inputs = GrantedInputs::default();
    assert!(matches!(
        result.publish_admission(&mut inputs, None),
        Err(AdmissionPublicationError::MissingPayloadRetention)
    ));
    inbox.assert_no_result();
    let mut payload_retention = None;
    let published = result
        .publish_admission(&mut inputs, Some(&mut payload_retention))
        .unwrap()
        .unwrap();
    assert!(payload_retention.is_some());
    assert!(inputs.contains(published.grant));
    assert!(
        result
            .publish_admission(&mut inputs, None)
            .unwrap()
            .is_none()
    );
    let input = published
        .delivery
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    assert!(matches!(
        inbox.consume(input),
        ExecutorFact::QueuedInputGranted { operation, grant, queued_bytes: 8192 }
            if operation == key.operation && grant == published.grant && grant.generation() == key.generation
    ));
    let wrong = SendKey {
        operation: TaskOperationId::new_v7(),
        ..key
    };
    assert!(inputs.take(published.grant, wrong).is_none());
    let replay = SendKey {
        generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::new(2).unwrap())),
        ..key
    };
    assert!(inputs.take(published.grant, replay).is_none());
    let original_grant = inputs
        .take(published.grant, key)
        .expect("same original WC grant");
    assert_eq!(original_grant.key(), key);
    assert!(inputs.take(published.grant, key).is_none());
    drop(original_grant);
    drop(payload_retention);
    result.original.close_after_exit().unwrap();
    assert!(owner.try_retire().unwrap());
    fixture.scope.close_external().unwrap();
    fixture
        .ledger
        .reap_scope_ready(fixture.scope.id(), 128)
        .unwrap();
    inbox.finish();
    fixture.finish();
    assert!(wc.finish());
}
