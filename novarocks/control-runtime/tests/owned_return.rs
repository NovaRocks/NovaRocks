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

#![cfg(not(loom))]

use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase, ExitObligation},
    owned_return::{OwnedReturnReader, OwnedReturnSender},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::{CompletionError, ConsumerKind, Consumption, ReturnState},
    identity::{ConsumerId, DimensionId, Generation},
    owned_storage::OwnedSlotNeed,
    scope::{ScopeError, WorkAdmission, WorkKind},
};
use std::{
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn consumer() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}
fn resources(n: u64) -> ResourceVector {
    ResourceVector::new([(dimension(), n)]).unwrap()
}
fn reported_resources() -> ReservationResources {
    ReservationResources::new(
        resources(1),
        OwnedSlotNeed {
            work_headers: 1,
            completions: 1,
            captures: 1,
            ..OwnedSlotNeed::default()
        },
    )
}
struct Fixture<T: Send + 'static> {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    exit_custody: ExitCustody,
    authority: Option<ExitObligation>,
    report: ReportSlot,
    sender: Option<OwnedReturnSender<T>>,
    reader: Option<OwnedReturnReader<T>>,
}
impl<T: Send + 'static> Fixture<T> {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(resources(2));
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (work, mut capacity) = scope
            .reserve_next(
                generation(),
                reported_resources(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(consumer(), ConsumerKind::Observer)],
        )
        .unwrap();
        let (sender, reader) = report.claim_owned_return().unwrap();
        let (exit_custody, authority) = ExitCustody::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                work,
                capacity,
                report.clone(),
            )
            .unwrap(),
        );
        Self {
            ledger,
            scope,
            exit_custody,
            authority: Some(authority),
            report,
            sender: Some(sender),
            reader: Some(reader),
        }
    }
    fn exit_and_consume(&mut self) {
        if let Some(authority) = self.authority.take() {
            authority.exited().unwrap();
        }
        if self.report.consumption(consumer()).unwrap() == Consumption::Pending {
            self.report.consume(consumer()).unwrap();
        }
    }
    fn finish(mut self) {
        self.exit_and_consume();
        assert!(self.exit_custody.try_retire().unwrap());
        assert_eq!(self.ledger.reap_scope_ready(self.scope.id(), 4).unwrap(), 1);
        self.scope.close_external().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
#[test]
fn one_original_slot_claim_and_take_transfer_the_same_value_without_exit_or_ack() {
    let mut f = Fixture::<Box<usize>>::new();
    assert!(f.report.same_slot(f.sender.as_ref().unwrap().report_slot()));
    assert!(f.report.same_slot(f.reader.as_ref().unwrap().report_slot()));
    assert!(matches!(
        f.report.clone().claim_owned_return::<u64>(),
        Err(RuntimeError::Completion(
            CompletionError::ReturnAlreadyClaimed
        ))
    ));
    let original = Box::new(17);
    let pointer = std::ptr::from_ref(original.as_ref());
    f.sender.take().unwrap().publish(original).unwrap();
    assert_eq!(
        f.reader.as_ref().unwrap().state().unwrap(),
        ReturnState::Filled
    );
    let value = f.reader.as_mut().unwrap().try_take().unwrap().unwrap();
    assert_eq!(std::ptr::from_ref(value.as_ref()), pointer);
    assert!(f.reader.as_mut().unwrap().try_take().unwrap().is_none());
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Admitted);
    assert!(!f.exit_custody.body_exited());
    assert!(!f.exit_custody.captures_exited());
    assert_eq!(
        f.report.consumption(consumer()).unwrap(),
        Consumption::Pending
    );
    assert!(!f.report.try_retire().unwrap());
    drop(value);
    f.finish();
}
#[test]
fn live_payload_blocks_report_and_capacity_retirement_after_all_other_legs() {
    let mut f = Fixture::<String>::new();
    f.sender
        .take()
        .unwrap()
        .publish("same original value".into())
        .unwrap();
    f.exit_and_consume();
    assert!(f.exit_custody.actual_exit_ready().unwrap());
    assert!(!f.report.try_retire().unwrap());
    assert!(!f.exit_custody.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    assert_eq!(f.scope.snapshot().unwrap().storage, 1);
    assert_eq!(
        f.reader.as_mut().unwrap().try_take().unwrap().as_deref(),
        Some("same original value")
    );
    assert!(f.exit_custody.try_retire().unwrap());
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 4).unwrap(), 1);
    assert!(
        f.reader.as_mut().unwrap().try_take().unwrap().is_none(),
        "a verified local Taken witness survives report retirement"
    );
    assert_eq!(
        f.reader.as_ref().unwrap().state().unwrap(),
        ReturnState::Taken
    );
    f.scope.close_external().unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
    assert!(f.ledger.is_empty());
}
#[test]
fn claim_requires_the_original_bound_not_started_producer_and_reserved_report() {
    for exited in [false, true] {
        let ledger = RuntimeLedger::new(resources(1));
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let mut guard = scope
            .reserve_next(
                generation(),
                reported_resources(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap();
        let report = ReportSlot::reserve_next(
            &mut guard,
            generation(),
            1,
            [(consumer(), ConsumerKind::Observer)],
        )
        .unwrap();
        assert!(matches!(
            report.claim_owned_return::<()>(),
            Err(RuntimeError::Scope(ScopeError::InvalidTransition))
        ));
        let (work, capacity) = guard.bind(WorkKind::Cleanup).unwrap();
        let (exit_custody, authority) = ExitCustody::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                work,
                capacity,
                report.clone(),
            )
            .unwrap(),
        );
        let mut authority = Some(authority);
        authority.as_mut().unwrap().start().unwrap();
        if exited {
            authority.take().unwrap().exited().unwrap();
        }
        assert!(matches!(
            report.claim_owned_return::<()>(),
            Err(RuntimeError::Scope(ScopeError::InvalidTransition))
        ));
        if let Some(authority) = authority {
            authority.exited().unwrap();
        }
        report.consume(consumer()).unwrap();
        assert!(exit_custody.try_retire().unwrap());
        ledger.reap_scope_ready(scope.id(), 1).unwrap();
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
}
#[test]
fn old_admission_can_publish_after_close_or_failure_without_inventing_any_exit() {
    for fail in [false, true] {
        let mut f = Fixture::<usize>::new();
        f.scope.close_external().unwrap();
        if fail {
            f.scope.fail().unwrap();
        }
        let before = f.scope.snapshot().unwrap();
        f.sender.take().unwrap().publish(73).unwrap();
        assert_eq!(f.reader.as_mut().unwrap().try_take().unwrap(), Some(73));
        assert_eq!(f.scope.snapshot().unwrap().work, before.work);
        assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Admitted);
        assert_eq!(f.ledger.used(dimension()), 1);
        assert_eq!(
            f.report.consumption(consumer()).unwrap(),
            Consumption::Pending
        );
        f.finish();
    }
}
#[test]
fn unresolved_original_work_can_return_a_fault_without_positive_exit_or_consumption() {
    let mut f = Fixture::<Result<(), usize>>::new();
    drop(f.authority.take());
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Unresolved);
    assert_eq!(f.scope.snapshot().unwrap().unresolved, 1);
    f.scope.fail().unwrap();
    f.sender.take().unwrap().publish(Err(19)).unwrap();
    assert_eq!(
        f.reader.as_mut().unwrap().try_take().unwrap(),
        Some(Err(19))
    );
    assert!(!f.exit_custody.body_exited());
    assert!(!f.exit_custody.captures_exited());
    assert_eq!(
        f.report.consumption(consumer()).unwrap(),
        Consumption::Pending
    );
    assert!(!f.exit_custody.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    // Returning the actual fault value cannot supply the missing Work exit.
    let _original = ManuallyDrop::new(f);
}
#[test]
fn unused_return_revocation_neither_exits_the_original_role_nor_consumes_its_report() {
    let mut f = Fixture::<usize>::new();
    f.sender.take().unwrap().revoke_unused().unwrap();
    assert_eq!(
        f.reader.as_ref().unwrap().state().unwrap(),
        ReturnState::RevokedUnused
    );
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Admitted);
    assert_eq!(
        f.report.consumption(consumer()).unwrap(),
        Consumption::Pending
    );
    assert!(!f.report.try_retire().unwrap());
    // A reader need not poll before an explicitly unused report retires.
    let reader = f.reader.take().unwrap();
    f.finish();
    drop(reader);
}
#[test]
fn numeric_slot_equality_from_another_real_ledger_is_not_the_same_slot() {
    let mut first = Fixture::<usize>::new();
    let mut second = Fixture::<usize>::new();
    assert_eq!(first.report.id(), second.report.id());
    assert!(!first.report.same_slot(&second.report));
    first.sender.take().unwrap().revoke_unused().unwrap();
    second.sender.take().unwrap().revoke_unused().unwrap();
    first.reader.as_mut().unwrap().try_take().unwrap();
    second.reader.as_mut().unwrap().try_take().unwrap();
    first.finish();
    second.finish();
}
#[derive(Debug)]
struct Counted(Arc<AtomicUsize>);
impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn lost_reader_preserves_the_filled_opaque_payload_and_paid_report_as_unknown() {
    let mut f = Fixture::<Counted>::new();
    let drops = Arc::new(AtomicUsize::new(0));
    f.sender
        .take()
        .unwrap()
        .publish(Counted(drops.clone()))
        .unwrap();
    drop(f.reader.take());
    f.exit_and_consume();
    assert_eq!(f.report.owned_return_state().unwrap(), ReturnState::Unknown);
    assert!(!f.exit_custody.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(f.scope.snapshot().unwrap().storage, 1);
    // Unknown retains this original report/cell/role; no fake positive cleanup.
    let _original = ManuallyDrop::new(f);
}
#[test]
fn lost_producer_is_sticky_and_late_publish_returns_the_same_value_and_sender() {
    let mut f = Fixture::<Counted>::new();
    drop(f.reader.take());
    let drops = Arc::new(AtomicUsize::new(0));
    let value = Counted(drops.clone());
    let refusal = f.sender.take().unwrap().publish(value).unwrap_err();
    assert_eq!(
        refusal.error,
        RuntimeError::Completion(CompletionError::InvalidReturnState(ReturnState::Unknown))
    );
    assert!(refusal.sender.report_slot().same_slot(&f.report));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    // The caller now owns the refused value; only its own explicit Drop runs.
    drop(refusal.value);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(refusal.sender);
    f.exit_and_consume();
    assert!(!f.exit_custody.try_retire().unwrap());
    let _original = ManuallyDrop::new(f);

    let mut unpublished = Fixture::<usize>::new();
    drop(unpublished.sender.take());
    assert_eq!(
        unpublished.reader.as_ref().unwrap().state().unwrap(),
        ReturnState::Unknown
    );
    assert!(unpublished.reader.as_mut().unwrap().try_take().is_err());
    unpublished.exit_and_consume();
    assert!(!unpublished.exit_custody.try_retire().unwrap());
    let _unpublished = ManuallyDrop::new(unpublished);
}
struct BlockingDrop {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}
impl Drop for BlockingDrop {
    fn drop(&mut self) {
        self.entered.send(()).unwrap();
        self.release.recv().unwrap();
    }
}
fn advance_peer(ledger: &RuntimeLedger) -> Result<(), RuntimeError> {
    let scope = ledger.open_scope_next(None, generation())?;
    let (mut work, mut capacity) = scope
        .reserve_next(
            generation(),
            ReservationResources::for_work(resources(1)),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )?
        .bind(WorkKind::Cleanup)?;
    work.start()?;
    work.body_exited()?;
    work.captures_exited()?;
    capacity.release_quota(&capacity.resources()?.quota)?;
    capacity.finish()?;
    ledger.reap_scope_ready(scope.id(), 1)?;
    scope.close_external()?;
    ledger.remove_scope(scope.id())?;
    Ok(())
}
#[test]
fn blocking_opaque_drop_is_on_the_callers_stack_and_another_scope_can_progress() {
    let mut f = Fixture::<BlockingDrop>::new();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    f.sender
        .take()
        .unwrap()
        .publish(BlockingDrop {
            entered: entered_tx,
            release: release_rx,
        })
        .unwrap();
    let value = f.reader.as_mut().unwrap().try_take().unwrap().unwrap();
    let dropping = thread::spawn(move || drop(value));
    let entered = entered_rx.recv_timeout(Duration::from_secs(5));
    let (done_tx, done_rx) = mpsc::channel();
    let peer_ledger = f.ledger.clone();
    let peer = thread::spawn(move || {
        let result = advance_peer(&peer_ledger);
        done_tx.send(result).unwrap();
    });
    let progressed = done_rx.recv_timeout(Duration::from_secs(5));
    let _ = release_tx.send(());
    let dropped = dropping.join();
    let joined = peer.join();
    assert!(entered.is_ok());
    assert!(dropped.is_ok());
    assert!(joined.is_ok());
    progressed
        .expect("peer progressed while the opaque destructor was blocked")
        .unwrap();
    f.finish();
}
struct PanicDrop {
    ledger: RuntimeLedger,
    drops: Arc<AtomicUsize>,
}
impl Drop for PanicDrop {
    fn drop(&mut self) {
        assert_eq!(
            self.ledger.used(dimension()),
            1,
            "opaque Drop can reenter the original ledger"
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("original opaque destructor panic");
    }
}
#[test]
fn opaque_destructor_panic_occurs_only_after_take_unlocked_the_original_custody() {
    let mut f = Fixture::<PanicDrop>::new();
    let drops = Arc::new(AtomicUsize::new(0));
    f.sender
        .take()
        .unwrap()
        .publish(PanicDrop {
            ledger: f.ledger.clone(),
            drops: drops.clone(),
        })
        .unwrap();
    let value = f.reader.as_mut().unwrap().try_take().unwrap().unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| drop(value))).is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Admitted);
    advance_peer(&f.ledger).unwrap();
    // Consumer-side destruction does not assert original producer capture exit.
    f.finish();
}
