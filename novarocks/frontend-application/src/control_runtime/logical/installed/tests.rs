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

//! Installed Registry probes use the actual paid standing input and original
//! caller future. No HostExit or Scope removal is invented by these tests.
use super::*;
use crate::control_runtime::{
    actor::{FrontendActor, FrontendEffect, FrontendInput, TestActor},
    logical::inlet::{InletProgress, LogicalInletObligation},
};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    actor_turn::{ActorRegistration, ActorRuntime, StandingInputId},
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{ActorId, DimensionId, Generation, InputId, ScopedWorkRef, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_logical_execution_protocol::input::LogicalInput;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::Wake,
};

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl WakeCount {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegistryFact {
    input: InputId,
    author: ScopedWorkRef,
    source: LogicalSourceId,
    token: LogicalRequestToken,
    close: bool,
}
#[derive(Default)]
struct Recorded {
    probes: Vec<RegistryFact>,
    failures: Vec<(InputId, ScopedWorkRef)>,
    fail_marker: bool,
}
struct Recorder(Arc<Mutex<Recorded>>);
impl TestActor for Recorder {
    fn handle(
        &mut self,
        _: Tick,
        envelope: InputEnvelope,
        input: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        let author = step.input_work().expect("same actual admitted Work author");
        match input {
            FrontendInput::Logical(input) => {
                let LogicalInput::RegistryJoinReadiness {
                    source,
                    token,
                    close_ingress,
                } = *input
                else {
                    panic!("original Registry readiness input expected");
                };
                self.0.lock().unwrap().probes.push(RegistryFact {
                    input: envelope.input,
                    author,
                    source,
                    token,
                    close: close_ingress,
                });
            }
            FrontendInput::LogicalInfrastructureFailed => {
                let fail = self.0.lock().unwrap().fail_marker;
                assert!(!fail, "controlled original installed logical failure");
                self.0
                    .lock()
                    .unwrap()
                    .failures
                    .push((envelope.input, author));
            }
            _ => panic!("no replacement fact or runtime owner is introduced"),
        }
    }
}
fn source() -> LogicalSourceId {
    LogicalSourceId::new(NonZeroU64::new(17).unwrap())
}
fn token(n: u64) -> LogicalRequestToken {
    LogicalRequestToken::new(NonZeroU64::new(n).unwrap())
}
fn dimension() -> DimensionId {
    DimensionId::new(401).unwrap()
}
fn once<F: Future + ?Sized>(mut future: Pin<&mut F>, wake: &Waker) -> Poll<F::Output> {
    future.as_mut().poll(&mut Context::from_waker(wake))
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    work: WorkObligation,
    payer: ReservationObligation,
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    inlet: LogicalInletObligation,
    registry_position: StandingInputId,
    marker_position: StandingInputId,
    sender: SenderCell,
    installed: Arc<LogicalInstalledState>,
    failure: Arc<LogicalInfrastructureFailure>,
    recorded: Arc<Mutex<Recorded>>,
    host: Arc<WakeCount>,
}
impl Fixture {
    fn new() -> Self {
        let epoch = Generation::new(1).unwrap();
        let quota = ResourceVector::new([(dimension(), 3)]).unwrap();
        let ledger = RuntimeLedger::new(quota.clone());
        let scope = ledger.open_scope_next(None, epoch).unwrap();
        let (mut work, payer) = scope
            .reserve_next(
                epoch,
                ReservationResources::new(
                    quota,
                    OwnedSlotNeed {
                        work_headers: 1,
                        inputs: 2,
                        batches: 4,
                        effects: 1,
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
        let runtime = ActorRuntime::new();
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let registration = runtime
            .register_supervised_bound(
                ActorId::new(1).unwrap(),
                scope.clone(),
                FrontendActor::Test(Box::new(Recorder(recorded.clone()))),
                ActorConfig {
                    mailbox: [4; 4],
                    max_batches: 8,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 16,
                    max_step_work: 2,
                },
                &work,
            )
            .unwrap_or_else(|_| panic!("actual paid installed-state test actor"));
        let reserve = |source, class| {
            runtime
                .reserve_admitted_input_for(
                    &registration,
                    ActorId::new(source).unwrap(),
                    class,
                    &work,
                )
                .unwrap()
                .into_standing_input()
                .unwrap_or_else(|_| panic!("same original paid standing input"))
        };
        let registry = reserve(17, InputClass::Ordinary);
        let registry_position = registry.id();
        let marker = reserve(18, InputClass::Lifecycle);
        let marker_position = marker.id();
        let host = Arc::new(WakeCount::default());
        let wake = Waker::from(host.clone());
        let failure = LogicalInfrastructureFailure::new(marker, wake.clone());
        let owner_failure = failure.clone();
        let mut inlet = LogicalInletObligation::new(
            registry,
            source(),
            LogicalSourceKind::Registry,
            wake.clone(),
            Arc::new(move |original| owner_failure.retain(Box::new(original))),
        );
        let sender = Arc::new(AsyncMutex::new(inlet.take_sender().unwrap()));
        let installed = LogicalInstalledState::new(wake, failure.clone());
        installed.bind_failure_actor(&runtime, &registration);
        installed.bind_source(LogicalSourceKind::Registry, source(), &sender);
        installed.bind_registry_close(inlet.ingress_close_observer());
        assert_eq!(ledger.used(dimension()), 3);
        Self {
            ledger,
            scope,
            work,
            payer,
            runtime,
            registration,
            inlet,
            registry_position,
            marker_position,
            sender,
            installed,
            failure,
            recorded,
            host,
        }
    }
    fn position_input(&self, position: StandingInputId) -> Option<InputId> {
        self.runtime
            .standing_input_inventory(self.registration.actor(), None, 8)
            .unwrap()
            .into_iter()
            .find(|p| p.standing_input_id == position)
            .expect("same original standing still exists")
            .in_flight
    }
    fn assert_same_registry(&self, original_input: InputId) {
        assert_eq!(
            self.position_input(self.registry_position),
            Some(original_input)
        );
        let custody = self
            .runtime
            .input_custody(self.registration.actor(), original_input)
            .unwrap()
            .unwrap();
        assert_eq!(custody.standing_input_id, Some(self.registry_position));
        assert_eq!(custody.source, ActorId::new(17).unwrap());
        let original = self
            .installed
            .replies
            .lock()
            .unwrap()
            .registry
            .as_ref()
            .unwrap()
            .1
            .upgrade()
            .unwrap();
        assert!(Arc::ptr_eq(&original, &self.sender));
        assert_eq!(
            self.runtime
                .snapshot(self.registration.actor())
                .unwrap()
                .standing_inputs,
            2
        );
        assert_eq!(
            self.ledger.used(dimension()),
            3,
            "a borrowed wait never refunds original paid Work"
        );
    }
    fn consume(&self) {
        for _ in 0..8 {
            if !self.runtime.poll_one(Tick(1)).unwrap() {
                return;
            }
        }
        panic!("finite actual test inputs did not drain");
    }
    fn retire_registry_input(&mut self) {
        assert_eq!(
            self.inlet.progress(false, false).unwrap(),
            InletProgress::Changed
        );
        assert_eq!(self.position_input(self.registry_position), None);
    }
    fn last_probe(&self) -> RegistryFact {
        *self.recorded.lock().unwrap().probes.last().unwrap()
    }
    fn finish(mut self) {
        assert!(self.installed.replies.lock().unwrap().flight.is_none());
        assert!(self.installed.replies.lock().unwrap().probe.is_none());
        assert!(
            self.sender.try_lock().is_ok(),
            "the original send frame returned"
        );
        self.consume();
        self.inlet.close();
        self.work.body_exited().unwrap();
        self.work.captures_exited().unwrap();
        assert_eq!(
            self.inlet.progress(true, false).unwrap(),
            InletProgress::Closed
        );
        assert!(self.failure.close_marker_after_exit(false).unwrap());
        self.ledger.reap_scope_ready(self.scope.id(), 16).unwrap();
        self.payer
            .release_quota(&self.payer.resources().unwrap().quota)
            .unwrap();
        self.payer.finish().unwrap();
        self.scope.close_external().unwrap();
        while let Some(host) = self.runtime.claim_host_actor() {
            host.release(false).unwrap();
        }
        self.runtime
            .remove_actor(self.registration.actor())
            .unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert_eq!(self.ledger.used(dimension()), 0);
        assert!(self.ledger.is_empty());
    }
}

#[tokio::test]
async fn cancelled_registry_probe_reuses_same_original_inlet_occurrence_and_reply() {
    let mut f = Fixture::new();
    let state = f.installed.clone();
    let view = state.source(LogicalSourceKind::Registry).unwrap();
    assert_eq!(view.id(), source());
    assert!(matches!(
        state.source(LogicalSourceKind::Registry),
        Err(LogicalSourceRefusal::AlreadyTaken)
    ));
    let mut first = state.join_readiness(false);
    assert!(once(first.as_mut(), Waker::noop()).is_pending());
    let original = f.position_input(f.registry_position).unwrap();
    f.assert_same_registry(original);
    drop(first);
    for _ in 0..3 {
        let mut cancelled_again = state.join_readiness(false);
        assert!(once(cancelled_again.as_mut(), Waker::noop()).is_pending());
        f.assert_same_registry(original);
        assert_eq!(state.replies.lock().unwrap().next, NonZeroU64::new(2));
        assert_eq!(
            state.replies.lock().unwrap().probe.as_ref().unwrap().token,
            token(1)
        );
        drop(cancelled_again);
    }
    let mut resumed = state.join_readiness(false);
    assert!(once(resumed.as_mut(), Waker::noop()).is_pending());
    f.consume();
    let fact = f.last_probe();
    assert_eq!(fact.input, original);
    assert_eq!(fact.author, f.work.reference());
    assert_eq!(
        (fact.source, fact.token, fact.close),
        (source(), token(1), false)
    );
    state.registry_reply(source(), token(1), false);
    assert!(
        once(resumed.as_mut(), Waker::noop()).is_pending(),
        "a reply cannot replace original input retirement"
    );
    f.assert_same_registry(original);
    f.retire_registry_input();
    assert!(matches!(
        once(resumed.as_mut(), Waker::noop()),
        Poll::Ready(Ok(LogicalJoinReadiness::WaitingForConvergence))
    ));
    drop(resumed);
    let mut next = state.join_readiness(false);
    assert!(once(next.as_mut(), Waker::noop()).is_pending());
    let second = f.position_input(f.registry_position).unwrap();
    assert_ne!(original, second);
    f.assert_same_registry(second);
    f.consume();
    assert_eq!(
        f.recorded.lock().unwrap().probes.len(),
        2,
        "the original cancelled fact was not resubmitted"
    );
    assert_eq!(f.last_probe().token, token(2));
    state.registry_reply(source(), token(2), true);
    f.retire_registry_input();
    assert!(matches!(
        once(next.as_mut(), Waker::noop()),
        Poll::Ready(Ok(LogicalJoinReadiness::Ready))
    ));
    drop(next);
    assert!(!f.failure.failed());
    drop(view);
    f.finish();
}

#[tokio::test]
async fn stronger_close_after_cancelled_probe_settles_original_before_issuing_one_new_fact() {
    let mut f = Fixture::new();
    let state = f.installed.clone();
    let mut borrowed = state.join_readiness(false);
    assert!(once(borrowed.as_mut(), Waker::noop()).is_pending());
    let original = f.position_input(f.registry_position).unwrap();
    drop(borrowed);
    let mut stronger = state.join_readiness(true);
    assert!(once(stronger.as_mut(), Waker::noop()).is_pending());
    f.assert_same_registry(original);
    assert_eq!(state.replies.lock().unwrap().next, NonZeroU64::new(2));
    f.consume();
    assert!(!f.last_probe().close);
    state.registry_reply(source(), token(1), false);
    f.retire_registry_input();
    assert!(once(stronger.as_mut(), Waker::noop()).is_pending());
    let close = f.position_input(f.registry_position).unwrap();
    assert_ne!(original, close);
    f.assert_same_registry(close);
    assert_eq!(f.recorded.lock().unwrap().probes.len(), 1);
    f.consume();
    assert_eq!(
        (
            f.last_probe().source,
            f.last_probe().token,
            f.last_probe().close
        ),
        (source(), token(2), true)
    );
    assert_eq!(f.recorded.lock().unwrap().probes.len(), 2);
    state.registry_reply(source(), token(2), true);
    f.retire_registry_input();
    assert!(matches!(
        once(stronger.as_mut(), Waker::noop()),
        Poll::Ready(Ok(LogicalJoinReadiness::Ready))
    ));
    drop(stronger);
    f.finish();
}

#[tokio::test]
async fn cancelled_wait_after_actual_handoff_keeps_pending_reply_without_another_fact() {
    let mut f = Fixture::new();
    let state = f.installed.clone();
    let mut first = state.join_readiness(false);
    assert!(once(first.as_mut(), Waker::noop()).is_pending());
    f.consume();
    let original = f.last_probe();
    f.retire_registry_input();
    assert!(once(first.as_mut(), Waker::noop()).is_pending());
    drop(first);
    assert!(state.replies.lock().unwrap().flight.is_some());
    assert_eq!(
        state.replies.lock().unwrap().probe.as_ref().unwrap().token,
        original.token
    );
    let mut resumed = state.join_readiness(false);
    assert!(once(resumed.as_mut(), Waker::noop()).is_pending());
    assert_eq!(f.position_input(f.registry_position), None);
    assert_eq!(f.recorded.lock().unwrap().probes.len(), 1);
    assert_eq!(state.replies.lock().unwrap().next, NonZeroU64::new(2));
    state.registry_reply(original.source, original.token, false);
    assert!(matches!(
        once(resumed.as_mut(), Waker::noop()),
        Poll::Ready(Ok(LogicalJoinReadiness::WaitingForConvergence))
    ));
    drop(resumed);
    f.finish();
}

#[tokio::test]
async fn false_readiness_never_wakes_exit_waiter_and_committed_true_progress_cannot_fake_exit() {
    let mut f = Fixture::new();
    let state = f.installed.clone();
    let exit = Arc::new(WakeCount::default());
    let wake = Waker::from(exit.clone());
    assert!(
        state
            .poll_exit(&mut Context::from_waker(&wake))
            .is_pending()
    );
    for ready in [false, true] {
        let mut query = state.join_readiness(false);
        assert!(once(query.as_mut(), Waker::noop()).is_pending());
        f.consume();
        let probe = f.last_probe();
        state.registry_reply(probe.source, probe.token, ready);
        f.retire_registry_input();
        assert!(matches!(
            once(query.as_mut(), Waker::noop()),
            Poll::Ready(Ok(_))
        ));
        drop(query);
        if !ready {
            assert_eq!(
                exit.count(),
                0,
                "a false Registry reply cannot wake actual-exit observation"
            );
            for _ in 0..32 {
                state.readiness_changed(false);
            }
            assert_eq!(exit.count(), 0);
        }
    }
    state.readiness_changed(true);
    assert_eq!(exit.count(), 1);
    assert!(
        state
            .poll_exit(&mut Context::from_waker(&wake))
            .is_pending(),
        "Ready is not actual HostExit/Scope removal"
    );
    assert_eq!(exit.count(), 1);
    assert!(!state.retirable.load(Ordering::Acquire));
    assert!(!f.failure.failed());
    f.finish();
}

#[tokio::test]
async fn wrong_registry_reply_retains_same_owner_failure_marker_and_does_not_consume_original_probe()
 {
    for wrong_source in [false, true] {
        let mut f = Fixture::new();
        let state = f.installed.clone();
        assert!(Arc::ptr_eq(&state.failure, &f.failure));
        let mut query = state.join_readiness(false);
        assert!(once(query.as_mut(), Waker::noop()).is_pending());
        let original = f.position_input(f.registry_position).unwrap();
        let bad_source = if wrong_source {
            LogicalSourceId::new(NonZeroU64::new(18).unwrap())
        } else {
            source()
        };
        let bad_token = if wrong_source { token(1) } else { token(9) };
        state.registry_reply(bad_source, bad_token, true);
        assert!(f.failure.failed());
        assert_eq!(
            f.failure
                .error()
                .execution_failure()
                .unwrap()
                .fact()
                .reason()
                .code(),
            84
        );
        let marker = f
            .position_input(f.marker_position)
            .expect("same prepaid failure marker filled");
        assert_ne!(marker, original);
        assert_eq!(
            state.replies.lock().unwrap().probe.as_ref().unwrap().token,
            token(1)
        );
        assert_eq!(
            state.replies.lock().unwrap().probe.as_ref().unwrap().source,
            source()
        );
        assert!(once(query.as_mut(), Waker::noop()).is_pending());
        f.assert_same_registry(original);
        state.registry_reply(bad_source, bad_token, false);
        assert_eq!(
            f.position_input(f.marker_position),
            Some(marker),
            "wrong replies do not mint another failure position"
        );
        f.consume();
        let recorded = f.recorded.lock().unwrap();
        assert_eq!(recorded.probes.len(), 1);
        assert_eq!(
            recorded.failures.as_slice(),
            &[(marker, f.work.reference())]
        );
        drop(recorded);
        state.registry_reply(source(), token(1), false);
        f.retire_registry_input();
        assert!(matches!(
            once(query.as_mut(), Waker::noop()),
            Poll::Ready(Ok(LogicalJoinReadiness::WaitingForConvergence))
        ));
        drop(query);
        assert!(f.failure.failed());
        assert!(
            state
                .poll_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "the owner failure marker is not a fabricated actual exit"
        );
        assert!(f.host.count() > 0);
        f.finish();
    }
}

#[tokio::test]
async fn failed_owner_registry_close_settles_original_probe_before_actual_host_exit() {
    use novarocks_control_runtime::actor_turn::FailureSourceState;
    let mut f = std::mem::ManuallyDrop::new(Fixture::new());
    assert_eq!(
        f.installed.begin_owner_failed_registry_close(),
        Err(LogicalSourceClosed),
        "a typed outer failure or install-only role return cannot replace the real kernel fence"
    );
    assert!(!f.installed.replies.lock().unwrap().owner_failed_closing);
    let state = f.installed.clone();
    let mut original = Box::pin(state.join_readiness(true));
    assert!(matches!(
        once(original.as_mut(), Waker::noop()),
        Poll::Pending
    ));
    let input = f.position_input(f.registry_position).unwrap();
    f.recorded.lock().unwrap().fail_marker = true;
    f.failure.retain(Box::new("controlled original marker"));
    assert!(f.runtime.poll_one(Tick(1)).unwrap());
    f.runtime.failure_proof_for(&f.registration).unwrap();
    f.installed.begin_owner_failed_registry_close().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        f.installed.poll_owner_failed_registry_closed(&mut cx),
        Poll::Pending
    ));
    f.assert_same_registry(input);
    assert!(
        f.recorded.lock().unwrap().probes.is_empty(),
        "the failed Core did not consume the Registry fact"
    );
    assert_eq!(
        f.inlet.progress(false, true).unwrap(),
        InletProgress::Changed
    );
    assert!(matches!(
        f.installed.poll_owner_failed_registry_closed(&mut cx),
        Poll::Pending
    ));
    assert_eq!(
        f.position_input(f.registry_position),
        Some(input),
        "payload disposal alone is not input retirement"
    );
    assert_eq!(
        f.inlet.progress(false, true).unwrap(),
        InletProgress::Changed
    );

    assert!(matches!(
        f.installed.poll_owner_failed_registry_closed(&mut cx),
        Poll::Ready(Ok(()))
    ));
    assert!(matches!(
        once(original.as_mut(), Waker::noop()),
        Poll::Ready(Err(LogicalSourceClosed))
    ));
    drop(original);
    assert!(f.sender.try_lock().is_ok());
    assert_eq!(f.position_input(f.registry_position), None);
    assert!(
        !f.inlet.is_closed(),
        "ingress close never closes the original producer Work or role"
    );
    assert!(matches!(f.installed.poll_exit(&mut cx), Poll::Pending));
    assert!(f.runtime.validate_registration(&f.registration).is_ok());
    assert_eq!(f.ledger.used(dimension()), 3);
    let FailureSourceState::ClosedPending(supervisor) = f
        .runtime
        .failure_source_state(f.registration.actor())
        .unwrap()
    else {
        panic!("same original failed supervisor");
    };
    let cleanup = f
        .runtime
        .take_effect(
            f.registration.actor(),
            supervisor,
            novarocks_control_runtime_core::identity::ActionId::new(1).unwrap(),
        )
        .unwrap();
    assert!(matches!(cleanup.effect, FrontendEffect::OwnerFailed { .. }));
    let session = f.runtime.claim_failure_publication(&cleanup).unwrap();
    session.finish().unwrap();
    f.runtime.effect_completed(cleanup).unwrap();
    f.runtime
        .retire_input(f.registration.actor(), supervisor)
        .unwrap();
    let marker = f.position_input(f.marker_position).unwrap();
    f.runtime
        .retire_input(f.registration.actor(), marker)
        .unwrap();
    drop(
        f.runtime
            .take_failure_payload(f.registration.actor())
            .unwrap(),
    );
    std::mem::ManuallyDrop::into_inner(f).finish();
}
