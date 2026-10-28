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
fn a_second_old_witness_cannot_revive_a_replacement_after_its_row_was_released() {
    let mut f = Fixture::new(1);
    let id = f.append();
    let old_key = f.key(id);
    // Distinct non-Clone probes observe the same actual original, not an ID
    // synthesized after its resident row disappeared.
    let probes =
        std::array::from_fn::<_, 2, _>(|_| f.bank().retirement_probe_for(&old_key).unwrap());
    exit_used_source(&mut f, &old_key, SourceId(8101));
    f.retire(id);
    assert_eq!(f.bank().len(), 0);
    let [first_witness, second_witness] = probes.map(|probe| probe.observe_reaped().unwrap());
    let role = f.role();
    let replacement_report = role.report.clone();
    let replacement = f
        .bank_mut()
        .replace_after_reap(first_witness, role)
        .unwrap_or_else(|_| panic!("the original reaped role may be replaced once"));
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
    assert_eq!(f.bank().len(), 1);

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
        .replace_after_reap(second_witness, role)
        .unwrap_err();
    assert_eq!(error, SourcePositionError::WrongGeneration);
    assert_eq!(f.bank().len(), 0, "no retired original Arc was resurrected");
    assert_eq!(returned.work.id(), work);
    assert_eq!(returned.capacity.id(), reservation);
    assert!(returned.report.same_slot(&report));
    assert!(!retired.is_retired());
    assert_eq!(f.scope.snapshot().unwrap(), before);
    assert_eq!(f.ledger.change_version(), version);
    f.dispose_role(returned);
    assert!(retired.is_retired());
    f.finish();
}

#[test]
fn a_mixed_resident_pair_returns_whole_originals_on_full_and_retries_after_real_release() {
    let mut f = Fixture::new(2);
    let roles = [f.role(), f.role()];
    let pair = f
        .bank_mut()
        .append_pair(roles)
        .unwrap_or_else(|_| panic!("original paid pair"));
    let addresses = pair.positions();
    let keys = pair.keys().map(Clone::clone);
    let released_probe = f.bank().retirement_probe_for(&keys[1]).unwrap();
    exit_used_source(&mut f, &keys[0], SourceId(8201));
    let resident_witness = reap_without_releasing(&f, &keys[0]);
    exit_used_source(&mut f, &keys[1], SourceId(8202));
    f.retire(keys[1].address());
    let released_witness = released_probe.observe_reaped().unwrap();
    assert_eq!(f.bank().len(), 1);
    assert_eq!(
        f.bank().inspect(&keys[0]).unwrap().phase,
        SourcePositionPhase::Taken
    );
    assert_eq!(
        f.bank().inspect(&keys[1]).unwrap().phase,
        SourcePositionPhase::Retired
    );
    let blocker = f.append();
    assert!(
        !addresses.contains(&blocker),
        "fresh residency has its own monotonic address"
    );
    assert_eq!(f.bank().len(), 2);

    let roles = [f.role(), f.role()];
    let work_ids = roles.each_ref().map(|role| role.work.id());
    let reservations = roles.each_ref().map(|role| role.capacity.id());
    let reports = roles.each_ref().map(|role| role.report.clone());
    let retired = roles.each_ref().map(|role| role.work.retirement_observer());
    let before = f.scope.snapshot().unwrap();
    let version = f.ledger.change_version();
    let (error, pair, witnesses, returned) = f
        .bank_mut()
        .rearm_pair(pair, [resident_witness, released_witness], roles)
        .unwrap_err();
    assert_eq!(error, SourcePositionError::Full);
    assert_eq!(pair.positions(), addresses);
    for (key, original) in pair.keys().into_iter().zip(&keys) {
        assert!(key.same_original(original));
    }
    assert_eq!(returned.each_ref().map(|role| role.work.id()), work_ids);
    assert_eq!(
        returned.each_ref().map(|role| role.capacity.id()),
        reservations
    );
    for (role, report) in returned.iter().zip(&reports) {
        assert!(role.report.same_slot(report));
    }
    assert!(retired.iter().all(|observer| !observer.is_retired()));
    assert_eq!(f.scope.snapshot().unwrap(), before);
    assert_eq!(f.ledger.change_version(), version);
    assert_eq!(f.bank().len(), 2);

    // Remove the actual competing resident only after its unused permit,
    // report and Work genuinely exit. Keep the complete refused pair,
    // witnesses and newly admitted roles intact for this same retry.
    f.take_unused(blocker).unwrap().exit();
    f.retire(blocker);
    assert_eq!(f.bank().len(), 1);
    let pair = f
        .bank_mut()
        .rearm_pair(pair, witnesses, returned)
        .unwrap_or_else(|_| {
            panic!("one missing row fits; Full consumed neither original replacement")
        });
    assert_eq!(pair.positions(), addresses);
    assert_eq!(
        f.bank().len(),
        2,
        "only the missing leg adds a resident row"
    );
    for ((key, old_key), report) in pair.keys().into_iter().zip(&keys).zip(&reports) {
        assert!(!key.same_original(old_key));
        assert!(
            f.bank()
                .inspect(key)
                .unwrap()
                .custody
                .report()
                .same_slot(report)
        );
    }
    assert!(retired.iter().all(|observer| !observer.is_retired()));
    for key in pair.keys() {
        f.bank_mut().take_unused(key).unwrap().exit();
        f.retire(key.address());
    }
    assert!(retired.iter().all(|observer| observer.is_retired()));
    assert_eq!(f.bank().len(), 0);
    f.finish();
}
