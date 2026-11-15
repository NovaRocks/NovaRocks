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

//! Actual Ledger/ActorRuntime work, causal positions and Tokio Join custody.
//! These tests do not substitute the separate logical Source end-to-end tests.
use super::*;
use crate::control_runtime::{
    actor::{FrontendEffect, TestActor},
    logical::adapter::LogicalEffectEnvelope,
};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    actor_turn::{FailureSourceState, TimerSourceState},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ActorId, DimensionId, InputId, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_logical_execution_protocol::{
    effect::{LogicalEffect, LogicalEffectBody, NativeEffect},
    input::NativeFact,
};
use std::{
    future::poll_fn,
    num::NonZeroU64,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;

fn dimension() -> DimensionId {
    DimensionId::new(71).unwrap()
}
fn action() -> LogicalActionId {
    LogicalActionId::new(NonZeroU64::new(41).unwrap())
}
#[derive(Debug)]
struct OriginalCause(&'static str);
#[derive(Clone, Copy)]
enum Script {
    Fact,
    Metadata,
    FactoryPanic,
    PollPanic,
    Pending,
}
struct Executor {
    script: Script,
    calls: AtomicUsize,
    polls: Arc<AtomicUsize>,
    cause: Arc<OriginalCause>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}
impl Executor {
    fn new(script: Script) -> (Arc<Self>, oneshot::Sender<()>) {
        let (send, receive) = oneshot::channel();
        (
            Arc::new(Self {
                script,
                calls: AtomicUsize::new(0),
                polls: Arc::new(AtomicUsize::new(0)),
                cause: Arc::new(OriginalCause("original logical job failure")),
                release: Mutex::new(Some(receive)),
            }),
            send,
        )
    }
}
impl LogicalJobExecutor for Executor {
    fn execute(&self, _: LogicalHostEffect) -> LogicalFuture<'_, Option<ActionResult>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if matches!(self.script, Script::FactoryPanic) {
            std::panic::panic_any(self.cause.clone());
        }
        let script = self.script;
        let polls = self.polls.clone();
        let cause = self.cause.clone();
        let release = if matches!(script, Script::Pending) {
            self.release.lock().unwrap().take()
        } else {
            None
        };
        Box::pin(async move {
            polls.fetch_add(1, Ordering::SeqCst);
            match script {
                Script::Metadata => None,
                Script::PollPanic => std::panic::panic_any(cause),
                Script::Pending => {
                    release.unwrap().await.unwrap();
                    Some(ActionResult::Native(NativeFact::Applied))
                }
                Script::Fact => Some(ActionResult::Native(NativeFact::Applied)),
                Script::FactoryPanic => unreachable!(),
            }
        })
    }
}
struct Capture {
    drops: Arc<AtomicUsize>,
    panic: Option<Arc<OriginalCause>>,
}
impl Drop for Capture {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(cause) = self.panic.take() {
            std::panic::panic_any(cause);
        }
    }
}
type Received = Arc<Mutex<Vec<(InputId, FrontendInput)>>>;
type Failures = Arc<Mutex<Vec<RawFailure>>>;
struct Receiver(Received);
impl TestActor for Receiver {
    fn handle(
        &mut self,
        _: Tick,
        envelope: InputEnvelope,
        input: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        self.0.lock().unwrap().push((envelope.input, input));
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    owner: Arc<LogicalWorkObligation>,
    job: Option<PreparedEffectJob>,
    exit_position: Option<CausalInputObligation<FrontendActor>>,
    failures: Failures,
    received: Received,
    executor: Option<Arc<Executor>>,
    drops: Arc<AtomicUsize>,
    report: ReportSlot,
}
impl Fixture {
    fn new(metadata: bool, cleanup: Option<Arc<OriginalCause>>) -> Self {
        let quota = ResourceVector::new([(dimension(), 7)]).unwrap();
        let ledger = RuntimeLedger::new(quota.clone());
        let scope = ledger.open_scope_next(None, GENERATION).unwrap();
        let (work, mut capacity) = scope
            .reserve_next(
                GENERATION,
                ReservationResources::new(
                    quota,
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        let reservation = capacity.id();
        let runtime = ActorRuntime::new();
        let received = Arc::new(Mutex::new(Vec::new()));
        let registration = runtime
            .register_bound(
                ActorId::new(1).unwrap(),
                scope.clone(),
                FrontendActor::Test(Box::new(Receiver(received.clone()))),
                ActorConfig {
                    mailbox: [16; 4],
                    max_batches: 16,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap_or_else(|_| panic!("original logical test actor registration"));
        let result = CausalInputObligation::reserve_admitted(
            &runtime,
            &registration,
            registration.actor(),
            InputClass::Ordinary,
            &work,
        )
        .unwrap();
        let exit_position = CausalInputObligation::reserve_admitted(
            &runtime,
            &registration,
            registration.actor(),
            InputClass::Lifecycle,
            &work,
        )
        .unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            GENERATION,
            1,
            [(OBSERVER, ConsumerKind::Observer)],
        )
        .unwrap();
        let failures = Arc::new(Mutex::new(Vec::new()));
        let original_failures = failures.clone();
        let (owner, exit) = LogicalWorkObligation::prepare(
            ReportedWork::try_new(work, capacity, report.clone()).unwrap(),
            Waker::noop().clone(),
            Arc::new(move |error| original_failures.lock().unwrap().push(error)),
        );
        let body = if metadata {
            LogicalHostEffect::JoinReadiness(false)
        } else {
            LogicalHostEffect::Protocol(Box::new(LogicalEffect {
                action: action(),
                body: LogicalEffectBody::Native(NativeEffect::RetirePreparation),
            }))
        };
        let drops = Arc::new(AtomicUsize::new(0));
        let effect = FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope { body, reservation }));
        let job = PreparedEffectJob {
            effect,
            exit,
            result,
            captures: Box::new(Capture {
                drops: drops.clone(),
                panic: cleanup,
            }),
            transport: None,
        };
        assert_eq!(ledger.used(dimension()), 7);
        Self {
            ledger,
            scope,
            runtime,
            registration,
            owner,
            job: Some(job),
            exit_position: Some(exit_position),
            failures,
            received,
            executor: None,
            drops,
            report,
        }
    }
    fn launch(&mut self, executor: Arc<Executor>) {
        self.executor = Some(executor.clone());
        self.owner
            .launch(&Handle::current(), self.job.take().unwrap(), executor);
    }
    async fn join(&self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            poll_fn(|cx| {
                if self.owner.poll_join(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("original logical work Join completes");
    }
    fn consume_position(&self, position: &CausalInputObligation<FrontendActor>) {
        for _ in 0..8 {
            if !self.runtime.poll_one(Tick(0)).unwrap() {
                break;
            }
        }
        for id in position.in_flight().unwrap().into_iter().flatten() {
            self.runtime
                .retire_input(self.registration.actor(), id)
                .unwrap();
        }
    }
    fn send_actual_exit(&mut self) {
        assert!(self.owner.actual_exit());
        assert!(self.owner.state.lock().unwrap().joined);
        let value = self
            .owner
            .result_action()
            .map_or(FrontendInput::LogicalHostSettled, |action| {
                FrontendInput::Logical(Box::new(LogicalInput::ActionExited { action }))
            });
        self.exit_position
            .as_mut()
            .unwrap()
            .fill_with_receipt(None, value)
            .unwrap_or_else(|_| panic!("original independent exit position"));
    }
    fn retire_known_exit(&mut self, send_exit: bool) {
        assert!(self.owner.actual_exit());
        let mut result = self
            .owner
            .take_result()
            .expect("same original result loan returned");
        // The Source withholds custody retirement until both original causal
        // positions retire. Physical exit alone is not that caller proof.
        assert_eq!(
            self.ledger.used(dimension()),
            7,
            "the original Source has not retired either input position"
        );
        if send_exit {
            self.send_actual_exit();
        }
        self.consume_position(&result);
        let mut exit = self.exit_position.take().unwrap();
        self.consume_position(&exit);
        result.close_after_exit().unwrap();
        exit.close_after_exit().unwrap();
        assert!(
            self.owner
                .retire(&self.runtime, &self.registration)
                .unwrap()
        );
        self.scope.reap_ready(32).unwrap();
        assert_eq!(self.ledger.used(dimension()), 0);
        assert_eq!(self.owner.custody.phase(), ExitCustodyPhase::Retired);
        self.close_original_actor();
    }
    fn close_original_actor(&mut self) {
        let actor = self.registration.actor();
        self.runtime.close_actor_admission(actor).unwrap();
        assert_eq!(
            self.runtime.close_timer_source(actor).unwrap(),
            TimerSourceState::ClosedDrained
        );
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        // Sealing withdraws the actual unused builtin failure source only
        // after all original standing inputs and batches have retired.
        self.runtime.seal_actor(actor).unwrap();
        assert_eq!(
            self.runtime.failure_source_state(actor).unwrap(),
            FailureSourceState::ClosedDrained
        );
        self.scope.close_external().unwrap();
        drop(self.runtime.remove_actor(actor).unwrap());
        self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.runtime.is_empty());
        assert!(self.ledger.is_empty());
    }
    fn retain_unknown(self) {
        assert!(!self.owner.actual_exit());
        assert_eq!(self.owner.custody.phase(), ExitCustodyPhase::Unresolved);
        assert_eq!(self.ledger.used(dimension()), 7);
        assert!(!self.runtime.is_empty());
        assert!(!self.ledger.is_empty());
        // The process fixture retains the complete SAME owner, result/exit
        // positions, actor, ledger and raw causes. There is no positive exit
        // or fabricated close for an unresolved original capture destructor.
        static ORIGINAL_UNKNOWN: OnceLock<Mutex<Vec<Fixture>>> = OnceLock::new();
        ORIGINAL_UNKNOWN
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .push(self);
    }
    fn has_original(&self, cause: &Arc<OriginalCause>) -> bool {
        self.failures.lock().unwrap().iter().any(|failure| {
            failure
                .downcast_ref::<Arc<OriginalCause>>()
                .is_some_and(|original| Arc::ptr_eq(original, cause))
        })
    }
}

#[tokio::test]
async fn original_action_result_and_independent_exit_retire_the_same_work_and_funding() {
    let mut f = Fixture::new(false, None);
    let (executor, _) = Executor::new(Script::Fact);
    f.launch(executor.clone());
    f.join().await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(executor.polls.load(Ordering::SeqCst), 1);
    assert!(f.owner.result_filled());
    assert_eq!(f.owner.result_action(), Some(action()));
    assert!(f.owner.actual_exit());
    assert_eq!(f.drops.load(Ordering::SeqCst), 1);
    assert!(f.failures.lock().unwrap().is_empty());
    assert_eq!(f.report.publication_state().unwrap(), SlotState::Filled);
    f.retire_known_exit(true);
    let received = f.received.lock().unwrap();
    assert_eq!(received.len(), 2);
    assert!(
        matches!(&received[0].1,FrontendInput::Logical(input)if matches!(&**input,LogicalInput::ActionResult{action:a,result:ActionResult::Native(NativeFact::Applied)}if *a==action()))
    );
    assert!(
        matches!(&received[1].1,FrontendInput::Logical(input)if matches!(&**input,LogicalInput::ActionExited{action:a}if *a==action()))
    );
    assert_ne!(received[0].0, received[1].0);
}
#[tokio::test]
async fn metadata_work_has_real_host_facts_but_never_invents_a_protocol_action_exit() {
    let mut f = Fixture::new(true, None);
    let (executor, _) = Executor::new(Script::Metadata);
    f.launch(executor);
    f.join().await;
    assert!(f.owner.result_filled());
    assert_eq!(f.owner.result_action(), None);
    assert!(f.owner.actual_exit());
    f.retire_known_exit(true);
    let received = f.received.lock().unwrap();
    assert_eq!(received.len(), 2);
    assert!(
        received
            .iter()
            .all(|(_, input)| matches!(input, FrontendInput::LogicalHostSettled))
    );
}
#[tokio::test]
async fn pending_join_loan_stays_with_original_slot_and_cannot_supply_actual_exit() {
    let mut f = Fixture::new(false, None);
    let (executor, release) = Executor::new(Script::Pending);
    f.launch(executor.clone());
    tokio::time::timeout(Duration::from_secs(5), async {
        while executor.polls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..3 {
        assert!(!f.owner.poll_join(&mut cx));
        assert!(f.owner.state.lock().unwrap().join.is_some());
    }
    assert!(!f.owner.actual_exit());
    assert!(!f.owner.result_filled());
    assert!(f.owner.take_result().is_none());
    assert_eq!(f.drops.load(Ordering::SeqCst), 0);
    assert_eq!(f.ledger.used(dimension()), 7);
    assert!(!f.owner.retire(&f.runtime, &f.registration).unwrap());
    release.send(()).unwrap();
    f.join().await;
    f.retire_known_exit(true);
}
#[tokio::test]
async fn factory_and_poll_panics_keep_the_original_cause_without_fabricating_a_result() {
    for script in [Script::FactoryPanic, Script::PollPanic] {
        let mut f = Fixture::new(false, None);
        let (executor, _) = Executor::new(script);
        let original = executor.cause.clone();
        f.launch(executor);
        f.join().await;
        assert!(f.has_original(&original));
        assert_eq!(original.0, "original logical job failure");
        assert!(!f.owner.result_filled());
        assert_eq!(f.owner.result_action(), None);
        assert!(f.owner.actual_exit());
        assert_eq!(f.drops.load(Ordering::SeqCst), 1);
        f.retire_known_exit(false);
        assert!(f.received.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn primary_and_capture_drop_panics_keep_both_originals_and_leave_exit_unknown() {
    let cleanup = Arc::new(OriginalCause("original capture cleanup failure"));
    let mut f = Fixture::new(false, Some(cleanup.clone()));
    let (executor, _) = Executor::new(Script::PollPanic);
    let primary = executor.cause.clone();
    f.launch(executor);
    f.join().await;
    assert!(f.has_original(&primary));
    assert!(f.has_original(&cleanup));
    assert!(!f.owner.actual_exit());
    assert!(!f.owner.result_filled());
    assert_eq!(f.owner.result_action(), None);
    assert_eq!(f.owner.custody.phase(), ExitCustodyPhase::Unresolved);
    assert_eq!(f.drops.load(Ordering::SeqCst), 1);
    assert!(!f.owner.retire(&f.runtime, &f.registration).unwrap());
    assert_eq!(f.ledger.used(dimension()), 7);
    assert_eq!(f.scope.snapshot().unwrap().unresolved, 1);
    assert!(f.received.lock().unwrap().is_empty());
    f.retain_unknown();
}
#[tokio::test]
async fn aborting_the_original_pending_join_preserves_capture_drop_unknown_without_a_result() {
    let cleanup = Arc::new(OriginalCause(
        "original aborted-future capture cleanup failure",
    ));
    let mut f = Fixture::new(false, Some(cleanup.clone()));
    let (executor, _release) = Executor::new(Script::Pending);
    f.launch(executor.clone());
    tokio::time::timeout(Duration::from_secs(5), async {
        while executor.polls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Inject cancellation into the same original Join; never replace its Exit.
    f.owner.state.lock().unwrap().join.as_ref().unwrap().abort();
    f.join().await;
    assert!(f.has_original(&cleanup));
    assert!(!f.owner.result_filled());
    assert_eq!(f.owner.result_action(), None);
    assert!(!f.owner.actual_exit());
    assert_eq!(f.owner.custody.phase(), ExitCustodyPhase::Unresolved);
    assert_eq!(f.ledger.used(dimension()), 7);
    assert!(!f.owner.retire(&f.runtime, &f.registration).unwrap());
    f.retain_unknown();
}
#[tokio::test]
async fn generation_fence_disposes_original_captures_without_running_or_publishing_success() {
    let mut f = Fixture::new(false, None);
    let (executor, _) = Executor::new(Script::Fact);
    assert_eq!(f.scope.fail().unwrap(), GENERATION.next().unwrap());
    f.launch(executor.clone());
    f.join().await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(executor.polls.load(Ordering::SeqCst), 0);
    assert!(!f.owner.result_filled());
    assert_eq!(f.owner.result_action(), None);
    assert_eq!(f.drops.load(Ordering::SeqCst), 1);
    assert!(f.failures.lock().unwrap().iter().any(|error| matches!(
        error.downcast_ref::<ExitWorkError>(),
        Some(ExitWorkError::NotStarted(_))
    )));
    // RevokeUnused is the actual never-run wrapper disposal, not success.
    assert!(f.owner.actual_exit());
    assert_eq!(
        f.report.publication_state().unwrap(),
        SlotState::RevokedUnused
    );
    f.retire_known_exit(false);
    assert!(f.received.lock().unwrap().is_empty());
}
