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

use super::super::funding::GENERATION;
use super::*;
use crate::control_runtime::actor::{FrontendEffect, FrontendInput, TestActor};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    actor_turn::{FailureSourceState, TimerSourceState},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ActorId, DimensionId, Tick},
    mailbox::InputEnvelope,
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::{
    future::poll_fn,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{runtime::Builder, sync::oneshot, task::JoinError};
fn dimension() -> DimensionId {
    DimensionId::new(87).unwrap()
}
#[derive(Debug)]
struct OriginalCause(&'static str);
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
struct Receiver(Arc<AtomicUsize>);
impl TestActor for Receiver {
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        _: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Fixture {
    ledger: novarocks_control_runtime::transaction::RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<FrontendActor>,
    actor: ActorRegistration,
    original: Option<ReportedWork>,
    owner: Option<Arc<AuxiliaryWorkObligation>>,
    failures: Arc<Mutex<Vec<super::super::work::RawFailure>>>,
    received: Arc<AtomicUsize>,
}
impl Fixture {
    fn new() -> Self {
        Self::with_inputs(0)
    }
    fn with_inputs(inputs: usize) -> Self {
        let quota = ResourceVector::new([(dimension(), 7)]).unwrap();
        let ledger = novarocks_control_runtime::transaction::RuntimeLedger::new(quota.clone());
        let scope = ledger.open_scope_next(None, GENERATION).unwrap();
        let (work, mut payer) = scope
            .reserve_next(
                GENERATION,
                ReservationResources::new(
                    quota,
                    OwnedSlotNeed {
                        work_headers: 1,
                        inputs,
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
        let report = ReportSlot::reserve_next(
            &mut payer,
            GENERATION,
            1,
            [(OBSERVER, ConsumerKind::Observer)],
        )
        .unwrap();
        let original = ReportedWork::try_new(work, payer, report).unwrap();
        let runtime = ActorRuntime::new();
        let received = Arc::new(AtomicUsize::new(0));
        let actor = runtime
            .register_bound(
                ActorId::new(1).unwrap(),
                scope.clone(),
                FrontendActor::Test(Box::new(Receiver(received.clone()))),
                ActorConfig {
                    mailbox: [4; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap_or_else(|_| panic!("original auxiliary actor"));
        Self {
            ledger,
            scope,
            runtime,
            actor,
            original: Some(original),
            owner: None,
            failures: Arc::new(Mutex::new(Vec::new())),
            received,
        }
    }
    fn start(&mut self, handle: Handle, make: AuxiliaryFactory) {
        let failures = self.failures.clone();
        self.owner = Some(AuxiliaryWorkObligation::start(AuxiliaryWorkStart {
            handle,
            original: self.original.take().unwrap(),
            make,
            wake: Waker::noop().clone(),
            failure: Arc::new(move |error| failures.lock().unwrap().push(error)),
            retirement: AuxiliaryHeaderOwner {
                runtime: self.runtime.clone(),
                actor: self.actor.clone(),
            },
        }));
    }
    fn owner(&self) -> &Arc<AuxiliaryWorkObligation> {
        self.owner.as_ref().unwrap()
    }
    async fn complete(&self) -> Result<(), AuxiliaryWorkError> {
        tokio::time::timeout(
            Duration::from_secs(5),
            poll_fn(|cx| self.owner().poll_complete(cx)),
        )
        .await
        .expect("original auxiliary completes its own Join/header")
    }
    fn has_cause(&self, cause: &Arc<OriginalCause>) -> bool {
        self.failures.lock().unwrap().iter().any(|raw| {
            raw.downcast_ref::<Arc<OriginalCause>>()
                .is_some_and(|original| Arc::ptr_eq(original, cause))
        })
    }
    fn assert_shared_join_error(&self) {
        let Err(outcome) = self
            .owner()
            .take_join_outcome()
            .expect("actual original Join outcome")
        else {
            panic!("actual failed Join required");
        };
        let original = outcome
            .downcast_ref::<JoinError>()
            .expect("same real Tokio JoinError");
        assert!(self.failures.lock().unwrap().iter().any(|raw| {
            raw.downcast_ref::<Arc<JoinError>>()
                .is_some_and(|reported| std::ptr::eq(reported.as_ref(), original))
        }));
        assert!(self.owner().take_join_outcome().is_none());
    }
    fn close(self) {
        self.close_with_explicit_inputs(0);
    }
    fn close_with_explicit_inputs(self, explicit: usize) {
        assert!(self.owner().observer.is_retired());
        assert_eq!(self.ledger.used(dimension()), 0);
        assert_eq!(
            self.received.load(Ordering::SeqCst),
            explicit,
            "auxiliary work must not synthesize Core input"
        );
        let actor = self.actor.actor();
        self.runtime.close_actor_admission(actor).unwrap();
        assert_eq!(
            self.runtime.close_timer_source(actor).unwrap(),
            TimerSourceState::ClosedDrained
        );
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
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
        assert!(!self.owner().actual_exit());
        assert!(!self.owner().observer.is_retired());
        assert_eq!(self.ledger.used(dimension()), 7);
        assert_eq!(self.received.load(Ordering::SeqCst), 0);
        static ORIGINAL_UNKNOWN: OnceLock<Mutex<Vec<Fixture>>> = OnceLock::new();
        ORIGINAL_UNKNOWN
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .push(self);
    }
}
struct PollPanicFuture {
    cause: Arc<OriginalCause>,
    _capture: Capture,
}
impl Future for PollPanicFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        std::panic::panic_any(self.cause.clone());
    }
}
struct ReadyFuture {
    _capture: Capture,
}
impl Future for ReadyFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}

#[tokio::test]
async fn normal_original_auxiliary_reaps_its_header_and_has_no_core_input() {
    let mut f = Fixture::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let original_calls = calls.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let capture = Capture {
        drops: drops.clone(),
        panic: None,
    };
    f.start(
        Handle::current(),
        Box::new(move || {
            std::hint::black_box(&capture);
            original_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        }),
    );
    assert_eq!(f.complete().await, Ok(()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(f.owner().actual_exit());
    assert!(matches!(f.owner().take_join_outcome(), Some(Ok(()))));
    assert!(f.owner().take_join_outcome().is_none());
    assert!(f.failures.lock().unwrap().is_empty());
    f.close();
}
#[tokio::test]
async fn pending_auxiliary_join_loan_cannot_supply_exit_or_retirement() {
    let mut f = Fixture::new();
    let (send, receive) = oneshot::channel();
    let mut receive = Some(receive);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    f.start(
        Handle::current(),
        Box::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
            let receive = receive.take().unwrap();
            Box::pin(async move {
                receive.await.unwrap();
            })
        }),
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..3 {
        assert!(f.owner().poll_complete(&mut cx).is_pending());
        assert!(f.owner().state.lock().unwrap().join.is_some());
    }
    assert!(!f.owner().actual_exit());
    assert_eq!(f.ledger.used(dimension()), 7);
    assert!(f.owner().take_join_outcome().is_none());
    send.send(()).unwrap();
    assert_eq!(f.complete().await, Ok(()));
    f.close();
}
#[tokio::test]
async fn borrowed_factory_panic_and_factory_drop_keep_both_original_causes_unknown() {
    let mut f = Fixture::new();
    let primary = Arc::new(OriginalCause("factory original"));
    let cleanup = Arc::new(OriginalCause("factory F-Drop original"));
    let body_primary = primary.clone();
    let capture = Capture {
        drops: Arc::new(AtomicUsize::new(0)),
        panic: Some(cleanup.clone()),
    };
    f.start(
        Handle::current(),
        Box::new(move || {
            std::hint::black_box(&capture);
            std::panic::panic_any(body_primary.clone());
        }),
    );
    assert_eq!(
        f.complete().await,
        Err(AuxiliaryWorkError::OriginalCapturesUnresolved)
    );
    assert!(f.has_cause(&primary));
    assert!(f.has_cause(&cleanup));
    assert_eq!(primary.0, "factory original");
    f.assert_shared_join_error();
    f.retain_unknown();
}
#[tokio::test]
async fn poll_and_future_drop_panics_preserve_primary_cleanup_and_same_join_error() {
    let mut f = Fixture::new();
    let primary = Arc::new(OriginalCause("poll original"));
    let cleanup = Arc::new(OriginalCause("future F-Drop original"));
    let mut original = Some(PollPanicFuture {
        cause: primary.clone(),
        _capture: Capture {
            drops: Arc::new(AtomicUsize::new(0)),
            panic: Some(cleanup.clone()),
        },
    });
    f.start(
        Handle::current(),
        Box::new(move || Box::pin(original.take().unwrap())),
    );
    assert_eq!(
        f.complete().await,
        Err(AuxiliaryWorkError::OriginalCapturesUnresolved)
    );
    assert!(f.has_cause(&primary));
    assert!(f.has_cause(&cleanup));
    f.assert_shared_join_error();
    f.retain_unknown();
}
#[tokio::test]
async fn ready_future_drop_failure_is_not_a_successful_exit() {
    let mut f = Fixture::new();
    let cleanup = Arc::new(OriginalCause("completed future F-Drop original"));
    let mut original = Some(ReadyFuture {
        _capture: Capture {
            drops: Arc::new(AtomicUsize::new(0)),
            panic: Some(cleanup.clone()),
        },
    });
    f.start(
        Handle::current(),
        Box::new(move || Box::pin(original.take().unwrap())),
    );
    assert_eq!(
        f.complete().await,
        Err(AuxiliaryWorkError::OriginalCapturesUnresolved)
    );
    assert!(f.has_cause(&cleanup));
    f.assert_shared_join_error();
    f.retain_unknown();
}
#[test]
fn cancelled_never_polled_launch_keeps_same_exit_and_uncalled_factory_in_stable_slot() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let mut f = Fixture::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let capture = Capture {
        drops: drops.clone(),
        panic: None,
    };
    f.start(
        runtime.handle().clone(),
        Box::new(move || {
            std::hint::black_box(&capture);
            count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        }),
    );
    drop(runtime);
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        f.owner().poll_complete(&mut cx),
        Poll::Ready(Err(AuxiliaryWorkError::OriginalCapturesUnresolved))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(f.owner().state.lock().unwrap().launch.is_some());
    assert!(!f.owner().body.lock().unwrap().called);
    assert!(f.owner().body.lock().unwrap().factory.is_some());
    assert_eq!(f.owner().custody.phase(), ExitCustodyPhase::Admitted);
    f.assert_shared_join_error();
    f.retain_unknown();
}
#[tokio::test]
async fn original_prepaid_settlement_still_runs_after_scope_failure_without_new_permission() {
    let mut f = Fixture::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    assert_eq!(f.scope.fail().unwrap(), GENERATION.next().unwrap());
    f.start(
        Handle::current(),
        Box::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        }),
    );
    assert_eq!(f.complete().await, Ok(()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(f.owner().take_join_outcome(), Some(Ok(()))));
    f.close();
}
#[tokio::test]
async fn factory_panic_with_clean_teardown_keeps_real_join_error_but_reaps_own_work() {
    let mut f = Fixture::new();
    let primary = Arc::new(OriginalCause("factory failure with real clean exit"));
    let cause = primary.clone();
    f.start(
        Handle::current(),
        Box::new(move || std::panic::panic_any(cause.clone())),
    );
    assert_eq!(f.complete().await, Err(AuxiliaryWorkError::JoinFailed));
    assert!(f.has_cause(&primary));
    f.assert_shared_join_error();
    assert!(f.owner().actual_exit());
    f.close();
}

#[tokio::test]
async fn original_work_retires_before_its_own_input_header_pin_is_released() {
    let mut f = std::mem::ManuallyDrop::new(Fixture::with_inputs(1));
    let mut role = std::mem::ManuallyDrop::new(
        f.runtime
            .reserve_admitted_input_for(
                &f.actor,
                f.actor.actor(),
                novarocks_control_runtime_core::mailbox::InputClass::Lifecycle,
                f.original.as_ref().unwrap().work(),
            )
            .unwrap()
            .into_standing_input()
            .unwrap(),
    );
    role.fill(FrontendInput::Test(37)).unwrap();
    assert!(f.runtime.poll_one(Tick(1)).unwrap());
    f.start(Handle::current(), Box::new(|| Box::pin(async {})));
    let retired = tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| f.owner().poll_work_retirement(cx)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(retired && f.owner().actual_exit());
    assert!(
        !f.owner().observer.is_retired(),
        "the same original input still pins its header"
    );
    assert!(!f.owner().retired());
    assert!(
        f.owner()
            .poll_complete(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(role.retire_ready_after_exit().unwrap());
    role.close_after_exit().unwrap();
    assert_eq!(f.complete().await, Ok(()));
    assert!(f.owner().observer.is_retired());
    drop(std::mem::ManuallyDrop::into_inner(role));
    std::mem::ManuallyDrop::into_inner(f).close_with_explicit_inputs(1);
}
