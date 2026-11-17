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
use crate::control_runtime::actor::{FrontendEffect, FrontendInput, TestActor};
use crate::control_runtime::{
    actor::FrontendActorKind,
    host::{
        EffectExecutor, EffectSource, FrontendLanePolicy, HostBinding, HostRegistry,
        PendingHandoff, PreparedEffectJob, SourceProgress,
    },
    metrics::FrontendHostObservations,
};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    exit_custody::ExitCustody,
};
use novarocks_control_runtime::{
    actor_turn::{ActorHostTurn, EffectTicket, ProcessClock},
    causal_input::CausalInputObligation,
};
use novarocks_control_runtime_core::{identity::InputId, mailbox::InputClass};
use novarocks_control_runtime_core::{
    identity::{ActorId, Tick},
    mailbox::InputEnvelope,
    scope::WorkAdmission,
};
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::Wake,
};
struct Receiver;
impl TestActor for Receiver {
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        _: FrontendInput,
        _: &mut ActorStep<'_, FrontendEffect>,
    ) {
        panic!("link custody publishes no synthetic Core input");
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    scope: novarocks_control_runtime::transaction::ScopeHandle,
    parent: ReportedWork,
    links: Arc<LogicalAttemptLinks>,
    activation: AttemptActivationIdentity,
}
impl Fixture {
    fn new() -> Self {
        Self::with_wake(Waker::noop().clone())
    }
    fn with_wake(wake: Waker) -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger.open_scope_next(None, GENERATION).unwrap();
        let guard = scope
            .reserve_next(
                GENERATION,
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap();
        let parent = super::super::funding::reported_step(guard, WorkKind::Business);
        let runtime = ActorRuntime::new();
        let registration = runtime
            .register_bound(
                ActorId::new(1).unwrap(),
                scope.clone(),
                FrontendActor::Test(Box::new(Receiver)),
                ActorConfig {
                    mailbox: [4; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap_or_else(|_| panic!("original link test actor"));
        let activation = AttemptActivationIdentity::from_parts(
            9,
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            1,
        );
        Self {
            ledger,
            runtime,
            registration,
            scope,
            parent,
            links: LogicalAttemptLinks::new(
                2,
                wake,
                Arc::new(|original| std::panic::resume_unwind(original)),
            ),
            activation,
        }
    }
    fn issue(&self) {
        self.links.issue(0, self.activation, &self.parent).unwrap();
    }
    fn progress(&self) -> bool {
        self.links
            .progress(0, &self.runtime, &self.registration)
            .unwrap()
    }
    fn close(self) {
        let observer = self.parent.work().retirement_observer();
        let (custody, exit) = ExitCustody::from_admitted(self.parent);
        exit.revoke_unused_and_exit().unwrap();
        assert!(custody.try_retire().unwrap());
        assert!(self.scope.reap_observed_header(&observer).unwrap());
        let actor = self.registration.actor();
        self.runtime.close_actor_admission(actor).unwrap();
        self.runtime.close_timer_source(actor).unwrap();
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        self.runtime.seal_actor(actor).unwrap();
        self.scope.close_external().unwrap();
        drop(self.runtime.remove_actor(actor).unwrap());
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.runtime.is_empty() && self.ledger.is_empty());
    }
}
#[test]
fn identity_cannot_create_or_duplicate_an_original_loan_and_return_does_not_prove_exit() {
    let f = Fixture::new();
    assert!(f.links.borrow(f.activation).is_none());
    f.issue();
    let loan = f.links.borrow(f.activation).unwrap();
    assert!(f.links.borrow(f.activation).is_none());
    assert!(!f.progress());
    drop(loan);
    assert!(!f.progress(), "loan return is not Native activation exit");
    let loan = f.links.borrow(f.activation).unwrap();
    f.links.activation_exited(0);
    assert!(
        !f.progress(),
        "the actual borrowed original still owns custody"
    );
    drop(loan);
    assert!(f.progress());
    assert!(f.links.borrow(f.activation).is_none());
    f.close();
}
#[test]
fn actual_unpublished_child_rollback_and_activation_exit_are_both_required() {
    let f = Fixture::new();
    f.issue();
    let loan = f.links.borrow(f.activation).unwrap();
    let child = loan
        .reserve_attempt(&f.ledger, GENERATION, ReservationResources::work_only())
        .unwrap();
    assert_ne!(child.scope().id(), f.scope.id());
    assert!(f.ledger.remove_scope(f.scope.id()).is_err());
    f.links.activation_exited(0);
    assert!(!f.progress());
    loan.retire_unpublished_child(child, &f.ledger);
    assert!(
        !f.progress(),
        "rollback does not return the still-borrowed original"
    );
    drop(loan);
    assert!(f.progress());
    f.close();
}
#[test]
fn already_issued_link_derives_same_settlement_storage_in_actual_failed_scope_epoch() {
    for opened_before_failure in [false, true] {
        let f = Fixture::new();
        f.issue();
        let loan = f.links.borrow(f.activation).unwrap();
        let before = f.scope.snapshot().unwrap().generation;
        let child = opened_before_failure.then(|| {
            loan.reserve_attempt(&f.ledger, GENERATION, ReservationResources::work_only())
                .unwrap()
        });
        let failed = f.scope.fail().unwrap();
        assert_ne!(failed, before);
        let storage = loan.reserve_port_storage().unwrap();
        assert_eq!(storage.work().scope().id(), f.scope.id());
        assert_eq!(storage.work().kind().unwrap(), WorkKind::Settlement);
        let header = storage.work().retirement_observer();
        let (custody, exit) = ExitCustody::from_admitted(storage);
        exit.revoke_unused_and_exit().unwrap();
        assert!(custody.try_retire().unwrap());
        assert!(f.scope.reap_observed_header(&header).unwrap());
        // An existing child is independent; a newly opened child inherits
        // failure. Neither case fabricates an AP registration or Core panic.
        let child = child.unwrap_or_else(|| {
            loan.reserve_attempt(&f.ledger, GENERATION, ReservationResources::work_only())
                .unwrap()
        });
        assert_eq!(
            child.scope().gates().unwrap().failed,
            !opened_before_failure
        );
        assert_eq!(
            child.scope().snapshot().unwrap().generation == GENERATION,
            opened_before_failure
        );
        loan.retire_unpublished_child(child, &f.ledger);
        f.links.activation_exited(0);
        drop(loan);
        assert!(f.progress());
        f.close();
    }
}

#[test]
fn original_stop_refuses_new_business_links_without_minting_an_activation() {
    let f = Fixture::new();
    f.scope.request_stop().unwrap();
    assert!(f.links.issue(0, f.activation, &f.parent).is_err());
    assert!(f.links.borrow(f.activation).is_none());
    assert!(f.progress());
    f.close();
}

struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct NoExecutor;
impl EffectExecutor for NoExecutor {
    fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        Err(job)
    }
}
/// No effect or synthetic exit input is needed: the installed host must close
/// these same real funded sources and remove its actual Actor and Scope.
struct ChildSource {
    funding: AttemptHostFunding,
    ledger: RuntimeLedger,
    closing: bool,
}
impl EffectSource for ChildSource {
    fn prepay_result(
        &mut self,
        _: &FrontendEffect,
    ) -> Option<CausalInputObligation<FrontendActor>> {
        unreachable!("the child fixture produces no effects")
    }
    fn bind_job(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        _: &EffectTicket<FrontendEffect>,
        _: CausalInputObligation<FrontendActor>,
    ) -> PreparedEffectJob {
        unreachable!("the child fixture produces no effect jobs")
    }
    fn return_unclaimed_result(&mut self, _: CausalInputObligation<FrontendActor>) {
        unreachable!("the child fixture has no result loans")
    }
    fn core_consumed(&mut self, _: &[InputId]) {}
    fn owner_failed_pending(&mut self, _: PendingHandoff) {
        unreachable!("the child fixture has no pending handoff")
    }
    fn close_external(&mut self) {
        self.closing = true;
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
        if !self.closing || budget == 0 {
            return SourceProgress {
                units: 0,
                progressed: false,
                more: false,
                retired: false,
            };
        }
        let progress = self
            .funding
            .progress(&self.ledger, runtime, &turn.registration())
            .unwrap();
        use crate::task_execution::host::funding::FundingProgress;
        SourceProgress {
            units: 1,
            progressed: matches!(progress, FundingProgress::More | FundingProgress::Retired),
            more: matches!(progress, FundingProgress::More),
            retired: matches!(progress, FundingProgress::Retired),
        }
    }
}
fn installed_child(
    f: &Fixture,
    loan: &AttemptActivationLinkLoan,
) -> (HostRegistry, ActorRegistration, HostExit) {
    let mut funding = loan
        .reserve_attempt(&f.ledger, GENERATION, ReservationResources::work_only())
        .unwrap();
    // The observation Source's real backing reservation has no remaining
    // readers in this fixture. Retire it through its actual original author.
    let backing = {
        let (observation, payer, _) = funding.observation_and_root_control();
        observation
            .scope()
            .reserve_child_next(
                GENERATION,
                payer.id(),
                ReservationResources::work_only(),
                WorkKind::Settlement,
            )
            .unwrap()
    };
    let (mut work, mut payer) = backing.bind(WorkKind::Settlement).unwrap();
    work.start().unwrap();
    let backing_exit = payer.retirement_observer();
    let backing_header = work.retirement_observer();
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    payer.finish().unwrap();
    assert!(work.scope().reap_observed_header(&backing_header).unwrap());
    assert!(backing_exit.is_retired());
    funding.bind_observation_exit(backing_exit);
    let scope = funding.scope().clone();
    let config = ActorConfig {
        mailbox: [4; 4],
        max_batches: 4,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 8,
        max_step_work: 1,
    };
    let registration = f
        .runtime
        .register_bound(
            ActorId::new(2).unwrap(),
            scope.clone(),
            FrontendActor::Test(Box::new(Receiver)),
            config,
        )
        .unwrap_or_else(|_| panic!("the actual child Actor registration"));
    let host = HostRegistry::new(
        f.runtime.clone(),
        f.ledger.clone(),
        Arc::new(FrontendHostObservations::new(Arc::new(
            ProcessClock::default(),
        ))),
    );
    let exit = host
        .register(HostBinding {
            scope,
            registration: registration.clone(),
            kind: FrontendActorKind::Test,
            config,
            policy: FrontendLanePolicy::attempt(1),
            source: Box::new(ChildSource {
                funding,
                ledger: f.ledger.clone(),
                closing: false,
            }),
            executor: Arc::new(NoExecutor),
            transport: None,
            ordered_observations: None,
            startup: None,
            capacity: None,
            membership: None,

            local_exit: None,
        })
        .unwrap_or_else(|_| panic!("the original installed child HostExit"));
    assert!(!loan.has_bound_host_exit(f.activation, &registration, &exit));
    observe_inventory_change(f, || loan.bind_registered(&registration, &exit), true);
    assert!(loan.has_bound_host_exit(f.activation, &registration, &exit));
    (host, registration, exit)
}
fn drive_child(f: &Fixture, host: &HostRegistry, child: &ActorRegistration) {
    for _ in 0..16 {
        let Some(turn) = f.runtime.claim_host_actor() else {
            return;
        };
        if turn.registration().same_registration(child) {
            host.drive(Tick(0), turn);
        } else {
            turn.release(false).unwrap();
        }
    }
    panic!("the finite child host closure did not settle");
}
#[test]
fn same_exit_receiving_receipt_survives_the_original_wait_future_loan() {
    let f = Fixture::new();
    f.issue();
    let loan = f.links.borrow(f.activation).unwrap();
    let (host, child, exit) = installed_child(&f, &loan);
    let wait = loan.slot.state.lock().unwrap().child_exit.take().unwrap();
    assert!(loan.has_bound_host_exit(f.activation, &child, &exit));
    assert!(!loan.has_bound_host_exit(f.activation, &f.registration, &exit));
    assert!(
        !f.links.actual_child_exited(f.activation),
        "an installed receipt is not physical removal"
    );
    {
        let mut state = loan.slot.state.lock().unwrap();
        assert!(state.child_exit.is_none());
        state.child_exit = Some(wait);
    }
    exit.request_close().unwrap();
    drive_child(&f, &host, &child);
    assert!(host.is_empty() && *exit.observe_actual_exit().borrow());
    assert!(loan.has_bound_host_exit(f.activation, &child, &exit));
    f.links.activation_exited(0);
    drop(loan);
    assert!(f.progress());
    f.close();
}
#[test]
fn registered_child_scope_removal_wakes_original_link_and_cannot_be_replaced_by_activation_exit() {
    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let f = Fixture::with_wake(Waker::from(wake.clone()));
    f.issue();
    let loan = f.links.borrow(f.activation).unwrap();
    let (host, child, exit) = installed_child(&f, &loan);
    let actual_exit = exit.observe_actual_exit();
    f.links.activation_exited(0);
    drop(loan);
    assert!(!f.progress(), "actual child Scope is still installed");
    wake.0.store(0, Ordering::SeqCst);
    drive_child(&f, &host, &child);
    assert!(!*actual_exit.borrow());
    assert!(!f.progress());
    exit.request_close().unwrap();
    drive_child(&f, &host, &child);
    assert!(host.is_empty());
    assert!(f.runtime.snapshot(child.actor()).is_err());
    assert!(
        *actual_exit.borrow(),
        "only the original HostRegistry writes removal"
    );
    assert!(
        wake.0.load(Ordering::SeqCst) > 0,
        "the pending actual HostExit woke its link Source"
    );
    assert!(f.progress());
    f.close();
}
#[test]
fn child_removal_and_loan_return_still_wait_for_the_same_original_header_pin() {
    let f = Fixture::new();
    f.issue();
    let loan = f.links.borrow(f.activation).unwrap();
    let (host, child, exit) = installed_child(&f, &loan);
    let mut pin = f
        .runtime
        .reserve_admitted_input_for(
            &f.registration,
            f.registration.actor(),
            InputClass::Control,
            loan.work(),
        )
        .unwrap()
        .into_standing_input()
        .unwrap_or_else(|_| panic!("the original link header pin"));
    exit.request_close().unwrap();
    drive_child(&f, &host, &child);
    assert!(*exit.observe_actual_exit().borrow());
    f.links.activation_exited(0);
    assert!(
        !f.progress(),
        "the real loan remains checked out after child removal"
    );
    drop(loan);
    assert!(
        !f.progress(),
        "the original paid input author still pins the link header"
    );
    pin.close_after_exit().unwrap();
    let turn = f
        .runtime
        .claim_host_actor()
        .expect("the original header readiness wake");
    assert!(turn.registration().same_registration(&f.registration));
    assert!(!turn.take_header_readiness(8).is_empty());
    turn.release(false).unwrap();
    assert!(f.progress());
    f.close();
}

/// A real observer callback may inspect the inventory immediately. This also
/// checks that notifications never execute while either custody lock is held.
struct InventoryWake {
    links: std::sync::Weak<LogicalAttemptLinks>,
    count: AtomicUsize,
}
impl Wake for InventoryWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let links = self.links.upgrade().expect("live original inventory");
        assert!(links.routes.try_lock().is_ok(), "routing lock released");
        for slot in &links.slots {
            assert!(slot.state.try_lock().is_ok(), "custody lock released");
        }
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}
fn observe_inventory_change(f: &Fixture, action: impl FnOnce(), changed: bool) {
    let changes = f.links.changes();
    let wake = Arc::new(InventoryWake {
        links: Arc::downgrade(&f.links),
        count: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    let mut notified = std::pin::pin!(changes.notified());
    notified.as_mut().enable();
    assert!(notified.as_mut().poll(&mut context).is_pending());
    action();
    assert_eq!(notified.as_mut().poll(&mut context).is_ready(), changed);
    assert_eq!(wake.count.load(Ordering::SeqCst) > 0, changed);
}

#[test]
fn original_inventory_changes_are_passive_and_borrow_scans_do_not_self_wake() {
    let f = Fixture::new();
    assert!(Arc::ptr_eq(&f.links.changes(), &f.links.changes()));
    observe_inventory_change(&f, || f.issue(), true);
    let mut loan = None;
    observe_inventory_change(&f, || loan = f.links.borrow(f.activation), false);
    observe_inventory_change(
        &f,
        || {
            assert!(f.links.borrow(f.activation).is_none());
            assert!(f.links.endpoint(f.activation).is_none());
            assert!(!f.progress());
        },
        false,
    );
    observe_inventory_change(&f, || drop(loan.take()), true);
    observe_inventory_change(&f, || assert!(!f.progress()), false);
    observe_inventory_change(&f, || f.links.activation_exited(0), true);
    observe_inventory_change(&f, || f.links.activation_exited(0), false);
    observe_inventory_change(&f, || f.links.input_author_closed(0), true);
    observe_inventory_change(&f, || f.links.input_author_closed(0), false);
    observe_inventory_change(&f, || assert!(f.progress()), true);
    observe_inventory_change(&f, || assert!(f.progress()), false);
    f.close();
}

#[test]
fn enabled_observer_catches_transition_between_scan_and_first_poll() {
    let f = Fixture::new();
    let changes = f.links.changes();
    let mut notified = std::pin::pin!(changes.notified());
    notified.as_mut().enable();
    assert!(f.links.borrow(f.activation).is_none());
    f.issue();
    assert!(
        notified
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready(),
        "issue after the scan is retained by the enabled waiter"
    );
    // notify_waiters does not leave a permit that could spin the next scan.
    observe_inventory_change(&f, || assert!(!f.progress()), false);
    f.links.activation_exited(0);
    assert!(f.progress());
    f.close();
}

#[test]
fn original_child_binding_rollback_and_actual_removal_notify_without_faking_exit() {
    let f = Fixture::new();
    f.issue();
    let loan = f.links.borrow(f.activation).unwrap();
    let mut child = None;
    observe_inventory_change(
        &f,
        || {
            child = Some(
                loan.reserve_attempt(&f.ledger, GENERATION, ReservationResources::work_only())
                    .unwrap(),
            );
        },
        true,
    );
    observe_inventory_change(
        &f,
        || loan.retire_unpublished_child(child.take().unwrap(), &f.ledger),
        true,
    );
    let mut installed = None;
    observe_inventory_change(&f, || installed = Some(installed_child(&f, &loan)), true);
    let (host, child, exit) = installed.unwrap();
    f.links.activation_exited(0);
    drop(loan);
    observe_inventory_change(&f, || assert!(!f.progress()), false);
    exit.request_close().unwrap();
    drive_child(&f, &host, &child);
    assert!(*exit.observe_actual_exit().borrow());
    observe_inventory_change(&f, || assert!(f.progress()), true);
    observe_inventory_change(&f, || assert!(f.progress()), false);
    f.close();
}

#[test]
fn unwinding_loan_returns_same_original_and_notifies_without_proving_producer_exit() {
    let f = Fixture::new();
    f.issue();
    observe_inventory_change(
        &f,
        || {
            let original = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _loan = f.links.borrow(f.activation).unwrap();
                panic!("original activation installation panic");
            }));
            assert!(original.is_err());
        },
        true,
    );
    assert!(
        !f.progress(),
        "returned custody is not actual producer exit"
    );
    let loan = f
        .links
        .borrow(f.activation)
        .expect("same original returned");
    f.links.activation_exited(0);
    drop(loan);
    assert!(f.progress());
    f.close();
}
