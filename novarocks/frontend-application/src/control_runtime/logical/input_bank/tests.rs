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

//! Same paid input inventory, original AP standing roles and actual teardown.
use super::super::{
    failure::LogicalInfrastructureFailure,
    funding::{LogicalFundingObligation, SourceFundingObligation},
    geometry::LogicalGeometry,
    installed::LogicalInstalledState,
};
use super::*;
use crate::control_runtime::actor::{FrontendActor, FrontendEffect, FrontendInput, TestActor};
use novarocks_control_runtime::{
    actor::ActorStep,
    actor_turn::{ActorRegistration, ActorRuntime, StandingInputObligation},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, ScopedWorkRef, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::WorkKind,
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, ExecutionEffect, IdSeed, MonotonicInstant, RecoveryMode,
};
use novarocks_logical_execution_protocol::start::{
    InitialAttempt, LogicalBudgets, LogicalFinishBoundary, LogicalOutputMode, LogicalPolicy,
    LogicalStart, ObservationBudgets,
};
use novarocks_query_application::api::{
    InstalledLogicalExecution, LogicalHostFact, LogicalSourceKind,
};
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
use std::{
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    panic::{AssertUnwindSafe, catch_unwind, panic_any},
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Wake},
    time::Duration,
};

#[derive(Default)]
struct Probe(AtomicUsize);
impl Probe {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
    fn reset(&self) {
        self.0.store(0, Ordering::SeqCst);
    }
}
impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Receiver;
impl TestActor for Receiver {
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        _: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        panic!("bank loans must not synthesize a business input");
    }
}
fn start() -> LogicalStart {
    let activation = AttemptActivationIdentity::from_parts(
        71,
        QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(1).unwrap()).unwrap(),
        12,
    );
    LogicalStart {
        identity: activation.execution().query_id(),
        actor_instance: activation.actor(),
        policy: LogicalPolicy {
            recovery: RecoveryMode::NoRecovery,
            effect: ExecutionEffect::None,
            output: LogicalOutputMode::CompletionOnly,
            finish: LogicalFinishBoundary::TaskCompletion,
            retain_diagnostics: false,
            root_carrier: None,
            internal_plan: None,
        },
        budgets: LogicalBudgets {
            max_attempts: NonZeroU32::new(3).unwrap(),
            replacement_reservation_valid_for: None,
            remote_cleanup_timeout: Duration::from_millis(100),
            admission_issues_per_participation: NonZeroUsize::new(2).unwrap(),
            establish_per_participation: NonZeroUsize::new(2).unwrap(),
            abort_per_participation: NonZeroUsize::new(2).unwrap(),
            observation: ObservationBudgets {
                diagnostics: Duration::from_millis(20),
                convergence: Duration::from_millis(90),
            },
        },
        first_attempt: InitialAttempt {
            activation,
            participations: Box::new([]),
        },
        statement_deadline: None,
        id_seed: IdSeed::new(7, 100),
        started_at: MonotonicInstant::ORIGIN,
    }
}

struct Fixture {
    ledger: RuntimeLedger,
    funding: LogicalFundingObligation,
    marker_author: Option<SourceFundingObligation>,
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    failure: Arc<LogicalInfrastructureFailure>,
    installed: Arc<LogicalInstalledState>,
    bank: Arc<LogicalInputBank>,
    roles: Vec<StandingInputObligation<FrontendActor>>,
    original_ap: ScopedWorkRef,
    host: Arc<Probe>,
}
impl Fixture {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let funding = LogicalFundingObligation::reserve(&ledger).unwrap();
        let start = start();
        let geometry = LogicalGeometry::derive(funding.scope().id(), &start).unwrap();
        let runtime = ActorRuntime::new();
        let registration = runtime
            .register_bound(
                geometry.actor,
                funding.scope().clone(),
                FrontendActor::Test(Box::new(Receiver)),
                geometry.config,
            )
            .unwrap_or_else(|_| panic!("same original bank test actor"));
        let marker_author = SourceFundingObligation::start(
            funding
                .reserve_step(
                    WorkKind::Settlement,
                    OwnedSlotNeed {
                        inputs: 1,
                        work_headers: 1,
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
        let marker = runtime
            .reserve_admitted_input_for(
                &registration,
                geometry
                    .source(geometry.purposes + geometry.attempts + 6)
                    .unwrap(),
                InputClass::Lifecycle,
                marker_author.work(),
            )
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|_| panic!("original paid failure marker"));
        let host = Arc::new(Probe::default());
        let wake = Waker::from(Arc::clone(&host));
        let failure = LogicalInfrastructureFailure::new(marker, wake.clone());
        let installed = LogicalInstalledState::new(wake.clone(), Arc::clone(&failure));
        let mut inputs = LogicalInputs::reserve(
            &runtime,
            registration.clone(),
            &funding,
            &geometry,
            &start,
            Arc::clone(&installed),
            Arc::clone(&failure),
            wake.clone(),
        );
        let author = inputs.ap_author(0).unwrap();
        let original_ap = author.reference();
        let mut roles = Vec::new();
        for class in [
            InputClass::Lifecycle,
            InputClass::Ordinary,
            InputClass::Ordinary,
        ] {
            roles.push(
                runtime
                    .reserve_admitted_input_for(
                        &registration,
                        ActorId::new(91).unwrap(),
                        class,
                        author,
                    )
                    .unwrap()
                    .into_standing_input()
                    .unwrap_or_else(|_| panic!("original AP role from the same prepaid Work")),
            );
        }
        let bank = LogicalInputBank::new(inputs, wake);
        Self {
            ledger,
            funding,
            marker_author: Some(marker_author),
            runtime,
            registration,
            failure,
            installed,
            bank,
            roles,
            original_ap,
            host,
        }
    }
    fn close_ap_roles(&mut self) {
        for role in &mut self.roles {
            role.close_after_exit().unwrap();
        }
        self.roles.clear();
    }
    fn finish(mut self) {
        self.close_ap_roles();
        {
            let mut original = self
                .bank
                .try_loan()
                .expect("same inventory returned before teardown");
            original.close();
            original.ap_inputs_closed_after_producer_exit(0).unwrap();
            let progress = original
                .progress(&self.runtime, true, false, usize::MAX)
                .unwrap();
            assert!(
                progress.complete,
                "all original input Work must actually retire"
            );
            original.mark_retired();
        }
        assert!(!self.failure.failed());
        assert!(self.failure.close_marker_after_exit(false).unwrap());
        assert!(
            self.marker_author
                .as_mut()
                .unwrap()
                .progress(&self.runtime, &self.registration)
                .unwrap()
        );
        self.marker_author = None;
        self.funding.scope().close_external().unwrap();
        assert!(
            self.funding
                .progress(&self.runtime, &self.registration)
                .unwrap()
        );
        while let Some(host) = self.runtime.claim_host_actor() {
            host.release(false).unwrap();
        }
        self.runtime
            .remove_actor(self.registration.actor())
            .unwrap();
        self.ledger.remove_scope(self.funding.scope().id()).unwrap();
        assert!(
            self.runtime.is_empty(),
            "same actor and standing-role inventory must exit"
        );
        assert!(
            self.ledger.is_empty(),
            "same original AP/source/root headers and Scope must exit"
        );
    }
}

#[tokio::test]
async fn busy_host_waits_for_one_actual_return_of_the_same_paid_inventory() {
    let f = Fixture::new();
    let mut held = f.bank.loan().await.unwrap();
    assert_eq!(held.ap_author(0).unwrap().reference(), f.original_ap);
    f.host.reset();
    assert!(f.bank.try_loan().is_none());
    assert!(f.bank.try_loan().is_none());
    assert_eq!(f.host.count(), 0, "Busy never schedules a retry poll");
    drop(held);
    assert_eq!(
        f.host.count(),
        1,
        "one original availability transition serves both requests"
    );
    {
        let mut same = f.bank.try_loan().unwrap();
        assert_eq!(same.ap_author(0).unwrap().reference(), f.original_ap);
        assert_eq!(same.ap_count(), 3);
    }
    assert_eq!(
        f.host.count(),
        1,
        "unrequested returns do not wake the host"
    );
    f.finish();
}

#[tokio::test]
async fn cancelling_a_borrowed_ap_wait_preserves_the_original_for_the_next_waiter() {
    let f = Fixture::new();
    let held = f.bank.try_loan().unwrap();
    let cancelled = Arc::new(Probe::default());
    let cancelled_waker = Waker::from(Arc::clone(&cancelled));
    {
        let mut waiting = Box::pin(f.bank.loan());
        let mut cx = Context::from_waker(&cancelled_waker);
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert_eq!(
            cancelled.count(),
            0,
            "the waiter does not self-poll on Busy"
        );
    }
    let survivor = Arc::new(Probe::default());
    let survivor_waker = Waker::from(Arc::clone(&survivor));
    let mut waiting = Box::pin(f.bank.loan());
    let mut cx = Context::from_waker(&survivor_waker);
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    drop(held);
    assert_eq!(
        cancelled.count(),
        0,
        "a cancelled wait owns neither inventory nor notification"
    );
    assert_eq!(
        survivor.count(),
        1,
        "the surviving wait observes the real return"
    );
    let Poll::Ready(Ok(mut original)) = waiting.as_mut().poll(&mut cx) else {
        panic!("the same original inventory must be available after its return");
    };
    assert_eq!(original.ap_author(0).unwrap().reference(), f.original_ap);
    assert!(
        f.bank.try_loan().is_none(),
        "the async borrower has the only inventory"
    );
    drop(original);
    drop(waiting);
    f.finish();
}

#[test]
fn unwinding_a_loan_returns_the_same_paid_ap_author_before_waking_the_host() {
    let f = Fixture::new();
    let original_panic = Arc::new(AtomicUsize::new(73));
    f.host.reset();
    let caught = catch_unwind(AssertUnwindSafe(|| {
        let mut held = f.bank.try_loan().unwrap();
        assert_eq!(held.ap_author(0).unwrap().reference(), f.original_ap);
        assert!(f.bank.try_loan().is_none());
        panic_any(Arc::clone(&original_panic));
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        caught.downcast_ref::<Arc<AtomicUsize>>().unwrap(),
        &original_panic
    ));
    assert_eq!(f.host.count(), 1);
    {
        let mut returned = f
            .bank
            .try_loan()
            .expect("unwind returned the original, not a replacement");
        assert_eq!(returned.ap_author(0).unwrap().reference(), f.original_ap);
    }
    f.finish();
}

#[tokio::test]
async fn external_close_preserves_loans_until_all_original_input_authors_actually_retire() {
    let mut f = Fixture::new();
    let mut weak = f.installed.source(LogicalSourceKind::Cancellation).unwrap();
    {
        let mut original = f.bank.loan().await.unwrap();
        f.host.reset();
        f.bank.close_admission_gate();
        assert_eq!(
            f.host.count(),
            0,
            "gate closure is no fabricated inventory availability"
        );
        assert_eq!(
            weak.send(LogicalHostFact::ConsumerClosed).await,
            Err(LogicalSourceClosed)
        );
        assert!(
            !f.failure.failed(),
            "the closed gate rejects WrongKind before its owner callback"
        );
        assert!(
            !original.is_retired(),
            "closing the projection does not exit any original source"
        );
        assert_eq!(
            original.ap_author(0).unwrap().reference(),
            f.original_ap,
            "a late issued binding still sees only its same original prepaid author"
        );
        original.close();
        assert!(
            !original
                .progress(&f.runtime, true, false, usize::MAX)
                .unwrap()
                .complete,
            "exposed AP Work remains live while its original three roles remain"
        );
        assert!(!original.is_retired());
        assert!(
            catch_unwind(AssertUnwindSafe(|| original.mark_retired())).is_err(),
            "the bank cannot manufacture actual input retirement"
        );
    }
    {
        let mut same = f
            .bank
            .loan()
            .await
            .expect("external close is not bank retirement");
        assert_eq!(
            same.ap_count(),
            3,
            "the original inventory remains inspectable"
        );
        assert!(
            same.ap_author(0).is_none(),
            "a bank loan grants no new authority past Inputs close"
        );
        assert!(!same.is_retired());
    }
    let bank = Arc::clone(&f.bank);
    let mut original = bank.try_loan().unwrap();
    let waiter = Arc::new(Probe::default());
    let waiter_waker = Waker::from(Arc::clone(&waiter));
    let mut pending = Box::pin(bank.loan());
    let mut cx = Context::from_waker(&waiter_waker);
    assert!(pending.as_mut().poll(&mut cx).is_pending());
    assert_eq!(waiter.count(), 0);
    f.close_ap_roles();
    original.ap_inputs_closed_after_producer_exit(0).unwrap();
    assert!(
        original
            .progress(&f.runtime, true, false, usize::MAX)
            .unwrap()
            .complete
    );
    assert!(original.is_retired());
    original.mark_retired();
    assert_eq!(
        waiter.count(),
        1,
        "real retirement wakes the waiting AP loan with Closed"
    );
    assert!(matches!(
        pending.as_mut().poll(&mut cx),
        Poll::Ready(Err(LogicalSourceClosed))
    ));
    drop(pending);
    drop(original);
    assert!(matches!(bank.loan().await, Err(LogicalSourceClosed)));
    {
        let retired = bank
            .try_loan()
            .expect("host may inspect the same retired inventory");
        assert!(retired.is_retired());
        assert_eq!(retired.ap_count(), 3);
    }
    f.finish();
}
