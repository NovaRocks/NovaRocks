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

//! Released rows leave the resident bound without making an old original
//! reusable twice. These probes retain actual original reaper observations.
use super::*;

fn exit_used_source(f: &mut Fixture, key: &SourcePositionKey, source_id: SourceId) {
    let (original, dropped) = source(f, key.address(), false);
    let bound = f
        .bank_mut()
        .claim(key, source_id, TaskFailurePhase::Execution)
        .unwrap()
        .bind(original);
    f.bank_mut().install(bound).unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(f.bank_mut().take_source(source_id).unwrap());
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "the original source destructor actually returned"
    );
}

// One old pair leg remains resident after its real report and Work have gone.
// This differs deliberately from Fixture::retire, which also removes its row.
fn reap_without_releasing(f: &Fixture, key: &SourcePositionKey) -> ReapedPosition {
    let custody = f.bank().inspect(key).unwrap().custody;
    assert_eq!(custody.phase(), ExitRolePhase::Exited);
    assert!(!custody.try_retire().unwrap());
    custody.report().consume(core()).unwrap();
    assert!(
        !custody.try_retire().unwrap(),
        "the host leg is independently unconsumed"
    );
    custody.report().consume(host()).unwrap();
    assert!(custody.try_retire().unwrap());
    assert!(
        !custody.reusable(),
        "report retirement does not reap the original Work"
    );
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    f.bank()
        .retirement_probe_for(key)
        .unwrap()
        .observe_reaped()
        .unwrap()
}

#[test]
fn a_second_old_witness_cannot_revive_a_replacement_after_final_slot_detach() {
    let mut f = Fixture::new(1);
    let id = f.append();
    let old_key = f.key(id);
    exit_used_source(&mut f, &old_key, SourceId(8101));
    let first = reap_without_releasing(&f, &old_key);
    let stale = f
        .bank()
        .retirement_probe_for(&old_key)
        .unwrap()
        .observe_reaped()
        .unwrap();
    f.bank_mut().mark_generation_retired(first).unwrap();
    assert_eq!(f.bank().len(), 1, "a reusable slot retains its row and fee");
    let first = f
        .bank()
        .retirement_probe_for(&old_key)
        .unwrap()
        .observe_reaped()
        .unwrap();
    let role = f.role();
    let replacement_report = role.report.clone();
    let replacement = f
        .bank_mut()
        .replace_after_reap(first, reported(role))
        .unwrap_or_else(|_| panic!("one real retired generation retains its paid slot"));
    assert_eq!(replacement.address(), old_key.address());
    assert!(!replacement.same_original(&old_key));
    assert!(
        f.bank()
            .inspect(&replacement)
            .unwrap()
            .custody
            .report()
            .same_slot(&replacement_report)
    );
    exit_used_source(&mut f, &replacement, SourceId(8102));
    f.retire(replacement.address());
    assert_eq!(f.bank().len(), 0);
    let role = f.role();
    let work = role.work.id();
    let reservation = role.capacity.id();
    let report = role.report.clone();
    let retired = role.work.retirement_observer();
    let before = f.scope.snapshot().unwrap();
    let version = f.ledger.change_version();
    let (error, returned) = f
        .bank_mut()
        .replace_after_reap(stale, reported(role))
        .unwrap_err();
    let returned = raw_reported(returned);
    let refused = error == SourcePositionError::WrongGeneration
        && returned.work.id() == work
        && returned.capacity.id() == reservation
        && returned.report.same_slot(&report)
        && !retired.is_retired();
    let unchanged = f.bank().len() == 0
        && f.scope.snapshot().unwrap() == before
        && f.ledger.change_version() == version;
    f.dispose_role(returned);
    f.finish();
    assert!(refused && unchanged && retired.is_retired());
}

#[test]
fn a_pair_with_one_finally_detached_leg_returns_all_originals_even_when_residency_is_available() {
    let mut f = Fixture::new(2);
    let roles = [f.role(), f.role()];
    let pair = f.bank_mut().append_pair(roles.map(model)).unwrap();
    let addresses = pair.positions();
    let keys = pair.keys().map(Clone::clone);
    exit_used_source(&mut f, &keys[0], SourceId(8201));
    let resident = reap_without_releasing(&f, &keys[0]);
    exit_used_source(&mut f, &keys[1], SourceId(8202));
    let old = reap_without_releasing(&f, &keys[1]);
    let detached = f
        .bank()
        .retirement_probe_for(&keys[1])
        .unwrap()
        .observe_reaped()
        .unwrap();
    let mut storage = f.bank_mut().release_reaped(detached).unwrap();
    storage.release().unwrap();
    let new_probe_refused = f
        .bank()
        .retirement_probe_for(&keys[1])
        .unwrap()
        .observe_reaped()
        .is_err();
    assert_eq!(f.bank().len(), 1);
    let roles = [f.role(), f.role()];
    let work_ids = roles.each_ref().map(|role| role.work.id());
    let reservations = roles.each_ref().map(|role| role.capacity.id());
    let reports = roles.each_ref().map(|role| role.report.clone());
    let before = f.scope.snapshot().unwrap();
    let version = f.ledger.change_version();
    let (error, pair, _witnesses, returned) = f
        .bank_mut()
        .rearm_pair(pair, [resident, old], roles.map(reported))
        .unwrap_err();
    let returned = returned.map(raw_reported);
    let rejected = error == SourcePositionError::SlotDetached
        && pair.positions() == addresses
        && returned.each_ref().map(|role| role.work.id()) == work_ids
        && returned.each_ref().map(|role| role.capacity.id()) == reservations
        && returned
            .iter()
            .zip(&reports)
            .all(|(role, report)| role.report.same_slot(report));
    let unchanged = f.scope.snapshot().unwrap() == before
        && f.ledger.change_version() == version
        && f.bank().len() == 1;
    for role in returned {
        f.dispose_role(role);
    }
    // Only a fresh whole admission can acquire the free resident address.
    let fresh = f.append();
    let monotonic = !addresses.contains(&fresh);
    f.finish();
    assert!(new_probe_refused && rejected && unchanged && monotonic);
}
