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

//! Actual paid source slots, generation reports, aliases and Work reaping.
//! A detached payment is explicitly returned only after leaving the handoff
//! gate. This does not prove a Native SourceIO or S8 owner loop.
use super::*;
use crate::control_admission::Dimension;

struct PaidFixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    payer: WorkLease,
    payment: CapacityBundle,
    bank: SourcePositions,
    task: TaskIdentity,
    pending: Arc<Pending>,
    health: Arc<ExecutionHealth>,
}
impl PaidFixture {
    fn new() -> Self {
        Self::with_limit(1)
    }
    fn with_limit(limit: usize) -> Self {
        let paid = SOURCE_SLOT_STORAGE
            .checked_mul(3)
            .unwrap()
            .resources()
            .unwrap();
        let ledger = RuntimeLedger::new(paid.clone(), 32, 1);
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation, 16, 16).unwrap();
        let (mut payer, payment) = scope
            .reserve_next(
                generation,
                paid,
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
    fn reported(&self) -> ReportedWork {
        reported(
            super::super::super::admitted_role::reserve_reported_role(
                &self.scope,
                self.payment.id(),
                WorkKind::Cleanup,
                Some(Generation::new(1).unwrap()),
            )
            .unwrap(),
        )
    }
    fn admission(&mut self) -> SourceSlotAdmission {
        let role = self.reported();
        let payment = self
            .payment
            .split_next(&SOURCE_SLOT_STORAGE.resources().unwrap())
            .unwrap();
        SourceSlotAdmission::from_paid(role, payment).unwrap()
    }
    fn charges(&self) -> [u64; 3] {
        [Dimension::Work, Dimension::Completions, Dimension::Captures]
            .map(|dimension| self.ledger.used(dimension.id()))
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
        self.payment
            .release(&self.payment.resources().unwrap())
            .unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
        assert!(self.health.failure().is_none());
    }
}

#[test]
fn paid_slot_keeps_one_fee_through_true_generation_reaping_and_rearm_until_final_detach_release() {
    let mut f = PaidFixture::new();
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
    let before_alias = f.charges();
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
    let after_mark = f.charges();
    let remains_resident =
        f.bank.len() == 1 && f.bank.inspect(&key).unwrap().phase == SourcePositionPhase::Retired;
    let reaped = f
        .bank
        .retirement_probe_for(&key)
        .unwrap()
        .observe_reaped()
        .unwrap();
    let role = f.reported();
    let version = f.ledger.change_version();
    let replacement = f.bank.replace_after_reap(reaped, role).unwrap();
    let rearm_metadata_only = f.ledger.change_version() == version;
    let after_rearm = f.charges();
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
    let mut detached = {
        let _handoff = gate.lock().unwrap();
        f.bank.release_reaped(reaped).unwrap()
    };
    let detach_metadata_only = f.ledger.change_version() == version;
    let after_detach = f.charges();
    let no_new_witness = f
        .bank
        .retirement_probe_for(&replacement)
        .unwrap()
        .observe_reaped()
        .is_err();
    let incoming = f.reported();
    let (error, incoming) = f
        .bank
        .replace_after_reap(stale_final, incoming)
        .unwrap_err();
    let cannot_revive = error == SourcePositionError::SlotDetached && f.bank.len() == 0;
    f.unused_reported(incoming);
    detached.release().unwrap();
    let refunded = f.charges();
    detached.release().unwrap();
    let once = f.charges() == refunded;
    f.finish();

    assert!(
        append_metadata_only && alias_blocks_witness && real_destructor_returned && observer_blocks
    );
    assert!(generation_retired && remains_resident && mark_metadata_only && rearm_metadata_only);
    assert_eq!(before_alias, [3; 3]);
    assert_eq!(after_mark, before_alias);
    assert_eq!(after_rearm, before_alias);
    assert!(old_rejected && detach_metadata_only && no_new_witness && cannot_revive);
    assert_eq!(
        after_detach, before_alias,
        "detach transfers the original fee without refunding under the gate"
    );
    assert_eq!(refunded, [2; 3]);
    assert!(once);
}

#[test]
fn a_foreign_generation_with_matching_numeric_scope_cannot_take_the_original_slot_fee() {
    let mut f = PaidFixture::new();
    let other = PaidFixture::new();
    let original = f.reported();
    let original_work = original.work().id();
    let payment = f
        .payment
        .split_next(&SOURCE_SLOT_STORAGE.resources().unwrap())
        .unwrap();
    let admission = SourceSlotAdmission::from_paid(original, payment).unwrap();
    let key = f.bank.append_admitted(admission).unwrap();
    let reaped = f.reap_unused(&key);
    f.bank.mark_generation_retired(reaped).unwrap();
    let witness = f
        .bank
        .retirement_probe_for(&key)
        .unwrap()
        .observe_reaped()
        .unwrap();
    let incoming = other.reported();
    let work = incoming.work().id();
    let report = incoming.report().clone();
    let same_scope_number = incoming.work().scope().id() == f.scope.id();
    let same_work_number = work == original_work;
    let version = f.ledger.change_version();
    let (error, incoming) = f.bank.replace_after_reap(witness, incoming).unwrap_err();
    let untouched = error == SourcePositionError::ForeignInventory
        && incoming.work().id() == work
        && incoming.report().same_slot(&report)
        && f.ledger.change_version() == version
        && f.charges() == [3; 3]
        && key.position.generation == 1
        && key.position.storage.lock().unwrap().is_some();
    other.unused_reported(incoming);
    let witness = f
        .bank
        .retirement_probe_for(&key)
        .unwrap()
        .observe_reaped()
        .unwrap();
    let mut detached = f.bank.release_reaped(witness).unwrap();
    detached.release().unwrap();
    f.finish();
    other.finish();
    assert!(same_scope_number && same_work_number && untouched);
}

#[test]
fn a_full_bank_returns_the_same_paid_whole_admission_for_real_unexposed_cleanup() {
    let mut f = PaidFixture::new();
    let first = f.admission();
    let key = f.bank.append_admitted(first).unwrap();
    let second = f.admission();
    let version = f.ledger.change_version();
    let (error, second) = f.bank.append_admitted(second).unwrap_err();
    let unchanged = error == SourcePositionError::Full
        && f.ledger.change_version() == version
        && f.charges() == [3; 3]
        && f.bank.len() == 1;
    let (reported, payment) = second.into_originals();
    f.unused_reported(reported);
    let mut payment = payment.expect("the original paid admission keeps its unique source fee");
    payment.release(&payment.resources().unwrap()).unwrap();
    let witness = f.reap_unused(&key);
    let mut detached = f.bank.release_reaped(witness).unwrap();
    detached.release().unwrap();
    f.finish();
    assert!(unchanged);
}

#[test]
fn a_foreign_second_pair_generation_returns_both_roles_without_moving_either_slot_fee() {
    let mut f = PaidFixture::with_limit(2);
    let other = PaidFixture::new();
    let first = f.admission();
    let second = f.admission();
    let pair = f.bank.append_pair([first, second]).unwrap();
    let keys = pair.keys().map(Clone::clone);
    let witnesses = keys.each_ref().map(|key| f.reap_unused(key));
    let incoming = [f.reported(), other.reported()];
    let ids = incoming.each_ref().map(|role| role.work().id());
    let reports = incoming.each_ref().map(|role| role.report().clone());
    let same_scope_number = incoming[1].work().scope().id() == f.scope.id();
    let version = f.ledger.change_version();
    let (error, pair, witnesses, returned) =
        f.bank.rearm_pair(pair, witnesses, incoming).unwrap_err();
    let intact = error == SourcePositionError::ForeignInventory
        && returned.each_ref().map(|role| role.work().id()) == ids
        && returned
            .iter()
            .zip(&reports)
            .all(|(role, report)| role.report().same_slot(report))
        && f.ledger.change_version() == version
        && f.charges() == [3; 3]
        && keys.iter().all(|key| {
            key.position.generation == 1 && key.position.storage.lock().unwrap().is_some()
        });
    let [local, foreign] = returned;
    f.unused_reported(local);
    other.unused_reported(foreign);
    // The same original pair and witnesses remain usable with two genuine
    // same-origin zero-resource generations; no second slot payment is minted.
    let incoming = [f.reported(), f.reported()];
    let version = f.ledger.change_version();
    let replacement = f.bank.rearm_pair(pair, witnesses, incoming).unwrap();
    let metadata_only = f.ledger.change_version() == version && f.charges() == [3; 3];
    for key in replacement.keys() {
        let witness = f.reap_unused(key);
        let mut storage = f.bank.release_reaped(witness).unwrap();
        storage.release().unwrap();
    }
    f.finish();
    other.finish();
    assert!(same_scope_number && intact && metadata_only);
}
