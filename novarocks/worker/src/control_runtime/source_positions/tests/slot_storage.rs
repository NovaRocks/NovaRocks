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

//! Actual owned Source roles, generation reports, aliases and Work reaping.
//! Neutral bank rows survive a generation without pinning its old Work.
use super::*;
use novarocks_control_runtime_core::{
    capacity::ReservationResources, owned_storage::OwnedSlotNeed,
};

struct PaidFixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    payer: WorkObligation,
    payment: ReservationObligation,
    bank: SourcePositions,
    task: TaskIdentity,
    pending: Arc<Pending>,
    health: Arc<ExecutionHealth>,
}
impl PaidFixture {
    fn new(source_roles: usize) -> Self {
        Self::with_limit(1, source_roles)
    }
    fn with_limit(limit: usize, source_roles: usize) -> Self {
        let paid = SOURCE_SLOT_STORAGE
            .checked_mul(source_roles)
            .unwrap()
            .resources()
            .unwrap();
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut payer, payment) = scope
            .reserve_next(
                generation,
                ReservationResources::new(
                    paid.quota,
                    paid.owned_slots
                        .checked_add(OwnedSlotNeed::work_only())
                        .unwrap(),
                ),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        payer.start().unwrap();
        let task = task();
        let pending = Arc::new(Pending(AtomicUsize::new(0)));
        let health = Arc::new(ExecutionHealth::default());
        let bank = SourcePositions::new(task, limit, pending.clone(), health.clone());
        Self {
            ledger,
            scope,
            payer,
            payment,
            bank,
            task,
            pending,
            health,
        }
    }
    fn reported(&mut self) -> ReportedWork {
        let role = super::super::super::reported_work_admission::reserve_reported_work(
            &self.scope,
            &mut self.payment,
            &self.payer,
            SOURCE_SLOT_STORAGE.resources().unwrap(),
            WorkKind::Cleanup,
            Some((self.scope.id(), Generation::new(1).unwrap())),
        )
        .unwrap();
        super::super::super::reported_work_admission::adopt_reported_work(
            role.work,
            role.capacity,
            role.report,
        )
    }
    fn admission(&mut self) -> SourceSlotAdmission {
        SourceSlotAdmission::from_admitted(self.reported()).unwrap()
    }
    fn work_count(&self) -> usize {
        self.scope.snapshot().unwrap().work
    }
    fn unused_reported(&self, role: ReportedWork) {
        let (custody, permit) = SourceCustody::from_reported(
            self.task,
            role,
            self.pending.clone(),
            self.health.clone(),
        );
        drop(permit);
        assert!(custody.try_retire().unwrap());
        self.ledger.reap_scope_ready(self.scope.id(), 16).unwrap();
    }
    fn reap_unused(&mut self, key: &SourcePositionKey) -> ReapedPosition {
        self.bank.take_unused(key).unwrap().exit();
        assert!(key.position.custody.try_retire().unwrap());
        self.ledger.reap_scope_ready(self.scope.id(), 16).unwrap();
        self.bank
            .retirement_probe_for(key)
            .unwrap()
            .observe_reaped()
            .unwrap()
    }
    fn finish(mut self) {
        assert_eq!(
            self.bank.len(),
            0,
            "the test must detach every actual paid slot"
        );
        self.scope.close_external().unwrap();
        self.payer.body_exited().unwrap();
        self.payer.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 16).unwrap();
        self.payment.finish().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
        assert!(self.health.failure().is_none());
    }
}

#[test]
fn original_slot_retires_each_real_generation_before_rearm_and_final_metadata_detach() {
    let mut f = PaidFixture::new(3);
    let admission = f.admission();
    let version = f.ledger.change_version();
    let key = f.bank.append_admitted(admission).unwrap();
    let append_metadata_only = f.ledger.change_version() == version;
    let custody = key.position.custody.clone();
    let dropped = Arc::new(AtomicUsize::new(0));
    let original = ExecutionFailure::from_domain(Original {
        dropped: dropped.clone(),
        position: Arc::downgrade(&key.position),
        panic: false,
    });
    let alias = original.clone();
    let bound = f
        .bank
        .claim(&key, SourceId(9001), TaskFailurePhase::Execution)
        .unwrap()
        .bind(original);
    f.bank.install(bound).unwrap();
    drop(f.bank.take_source(SourceId(9001)).unwrap());
    let alias_blocks_witness = f
        .bank
        .retirement_probe_for(&key)
        .unwrap()
        .observe_reaped()
        .is_err();
    let before_alias = f.work_count();
    drop(alias);
    let real_destructor_returned = dropped.load(Ordering::SeqCst) == 1;
    custody.report().consume(core()).unwrap();
    let observer_blocks = !custody.try_retire().unwrap()
        && f.bank
            .retirement_probe_for(&key)
            .unwrap()
            .observe_reaped()
            .is_err();
    custody.report().consume(host()).unwrap();
    let generation_retired = custody.try_retire().unwrap();
    f.ledger.reap_scope_ready(f.scope.id(), 16).unwrap();
    let reaped = f
        .bank
        .retirement_probe_for(&key)
        .unwrap()
        .observe_reaped()
        .unwrap();
    let stale = f
        .bank
        .retirement_probe_for(&key)
        .unwrap()
        .observe_reaped()
        .unwrap();
    let version = f.ledger.change_version();
    f.bank.mark_generation_retired(reaped).unwrap();
    let mark_metadata_only = f.ledger.change_version() == version;
    let after_mark = f.work_count();
    let remains_resident =
        f.bank.len() == 1 && f.bank.inspect(&key).unwrap().phase == SourcePositionPhase::Retired;
    let reaped = f
        .bank
        .retirement_probe_for(&key)
        .unwrap()
        .observe_reaped()
        .unwrap();
    let role = f.admission();
    let version = f.ledger.change_version();
    let replacement = f.bank.replace_after_reap(reaped, role).unwrap();
    let rearm_metadata_only = f.ledger.change_version() == version;
    let after_rearm = f.work_count();
    let old_rejected = matches!(
        f.bank.mark_generation_retired(stale),
        Err((SourcePositionError::WrongGeneration, _))
    );
    let reaped = f.reap_unused(&replacement);
    let stale_final = f
        .bank
        .retirement_probe_for(&replacement)
        .unwrap()
        .observe_reaped()
        .unwrap();
    let gate = Mutex::new(());
    let version = f.ledger.change_version();
    let detached = {
        let _handoff = gate.lock().unwrap();
        f.bank.release_reaped(reaped).unwrap()
    };
    let detach_metadata_only = f.ledger.change_version() == version;
    let after_detach = f.work_count();
    let no_new_witness = f
        .bank
        .retirement_probe_for(&replacement)
        .unwrap()
        .observe_reaped()
        .is_err();
    let incoming = f.admission();
    let (error, incoming) = f
        .bank
        .replace_after_reap(stale_final, incoming)
        .unwrap_err();
    let cannot_revive = error == SourcePositionError::SlotDetached && f.bank.len() == 0;
    f.unused_reported(incoming.into_reported());
    let before_drop = f.work_count();
    drop(detached);
    let after_drop = f.work_count();
    let no_exit_from_metadata_drop = before_drop == after_drop;
    f.finish();

    assert!(
        append_metadata_only && alias_blocks_witness && real_destructor_returned && observer_blocks
    );
    assert!(generation_retired && remains_resident && mark_metadata_only && rearm_metadata_only);
    assert_eq!(
        before_alias, 2,
        "the live alias still pins its actual source Work"
    );
    assert_eq!(
        after_mark, 1,
        "actual source header retires while its neutral address stays resident"
    );
    assert_eq!(
        after_rearm, 2,
        "a replacement carries its own real bound header"
    );
    assert!(old_rejected && detach_metadata_only && no_new_witness && cannot_revive);
    assert_eq!(
        after_detach, 1,
        "detaching metadata creates or retires no Work"
    );
    assert_eq!(after_drop, 1);
    assert!(no_exit_from_metadata_drop);
}

#[test]
fn a_foreign_generation_with_matching_numeric_scope_cannot_rebind_the_original_slot() {
    let mut f = PaidFixture::new(1);
    let mut other = PaidFixture::new(1);
    let original = f.reported();
    let original_work = original.work().id();
    let admission = SourceSlotAdmission::from_admitted(original).unwrap();
    let key = f.bank.append_admitted(admission).unwrap();
    let reaped = f.reap_unused(&key);
    f.bank.mark_generation_retired(reaped).unwrap();
    let witness = f
        .bank
        .retirement_probe_for(&key)
        .unwrap()
        .observe_reaped()
        .unwrap();
    let incoming = other.admission();
    let work = incoming.reported().work().id();
    let report = incoming.reported().report().clone();
    let same_scope_number = incoming.reported().work().scope().id() == f.scope.id();
    let same_work_number = work == original_work;
    let version = f.ledger.change_version();
    let (error, incoming) = f.bank.replace_after_reap(witness, incoming).unwrap_err();
    let untouched = error == SourcePositionError::ForeignInventory
        && incoming.reported().work().id() == work
        && incoming.reported().report().same_slot(&report)
        && f.ledger.change_version() == version
        && f.work_count() == 1
        && key.position.generation == 1
        && key.position.custody.reusable();
    other.unused_reported(incoming.into_reported());
    let witness = f
        .bank
        .retirement_probe_for(&key)
        .unwrap()
        .observe_reaped()
        .unwrap();
    let detached = f.bank.release_reaped(witness).unwrap();
    drop(detached);
    f.finish();
    other.finish();
    assert!(same_scope_number && same_work_number && untouched);
}

#[test]
fn a_full_bank_returns_the_same_paid_whole_admission_for_real_unexposed_cleanup() {
    let mut f = PaidFixture::new(2);
    let first = f.admission();
    let key = f.bank.append_admitted(first).unwrap();
    let second = f.admission();
    let second_work = second.reported().work().reference();
    let second_report = second.reported().report().clone();
    let version = f.ledger.change_version();
    let (error, second) = f.bank.append_admitted(second).unwrap_err();
    assert_eq!(second.reported().work().reference(), second_work);
    assert!(second.reported().report().same_slot(&second_report));
    let unchanged = error == SourcePositionError::Full
        && f.ledger.change_version() == version
        && f.work_count() == 3
        && f.bank.len() == 1;
    let report = second.reported().report().clone();
    let original_work = second.reported().work().id();
    assert_eq!(
        second.reported().bound_storage_profile().unwrap(),
        SOURCE_SLOT_STORAGE.resources().unwrap().owned_slots
    );
    assert!(original_work.get() > f.payer.id().get());
    assert!(!report.same_slot(key.position.custody.report()));
    f.unused_reported(second.into_reported());
    assert!(matches!(
        report.owned_return_state(),
        Err(RuntimeError::UnknownSlot)
    ));
    let witness = f.reap_unused(&key);
    let detached = f.bank.release_reaped(witness).unwrap();
    drop(detached);
    f.finish();
    assert!(unchanged);
}

#[test]
fn a_foreign_second_pair_generation_returns_both_roles_without_rebinding_either_original() {
    let mut f = PaidFixture::with_limit(2, 5);
    let mut other = PaidFixture::new(1);
    let first = f.admission();
    let second = f.admission();
    let pair = f.bank.append_pair([first, second]).unwrap();
    let keys = pair.keys().map(Clone::clone);
    let witnesses = keys.each_ref().map(|key| f.reap_unused(key));
    let incoming = [f.admission(), other.admission()];
    let ids = incoming.each_ref().map(|role| role.reported().work().id());
    let reports = incoming
        .each_ref()
        .map(|role| role.reported().report().clone());
    let same_scope_number = incoming[1].reported().work().scope().id() == f.scope.id();
    let version = f.ledger.change_version();
    let (error, pair, witnesses, returned) =
        f.bank.rearm_pair(pair, witnesses, incoming).unwrap_err();
    let intact = error == SourcePositionError::ForeignInventory
        && returned.each_ref().map(|role| role.reported().work().id()) == ids
        && returned
            .iter()
            .zip(&reports)
            .all(|(role, report)| role.reported().report().same_slot(report))
        && f.ledger.change_version() == version
        && f.work_count() == 2
        && keys
            .iter()
            .all(|key| key.position.generation == 1 && key.position.custody.reusable());
    let [local, foreign] = returned;
    f.unused_reported(local.into_reported());
    other.unused_reported(foreign.into_reported());
    // The same pair and witnesses remain usable with two complete same-origin
    // roles. Each generation owns its actual independent bound storage.
    let incoming = [f.admission(), f.admission()];
    let version = f.ledger.change_version();
    let replacement = f.bank.rearm_pair(pair, witnesses, incoming).unwrap();
    let metadata_only = f.ledger.change_version() == version && f.work_count() == 3;
    for key in replacement.keys() {
        let witness = f.reap_unused(key);
        let storage = f.bank.release_reaped(witness).unwrap();
        drop(storage);
    }
    f.finish();
    other.finish();
    assert!(same_scope_number && intact && metadata_only);
}
