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

//! Original availability events across finite Host ready-pass budget cuts.
use super::*;

fn install_original_transport_waker(f: &Fixture) -> std::task::Waker {
    let retirement = TransportRetirements::new(
        f.runtime.clone(),
        f.registration.clone(),
        tokio::runtime::Handle::current(),
        f.count,
    );
    let wake = retirement.host_waker();
    let entry = f.host.state.lock().unwrap().entries[&f.scope.id()].clone();
    let mut machine = entry.machine.lock().unwrap();
    let previous = machine.as_mut().unwrap().transport.replace(retirement);
    // This is the actual registration's empty transport owner. No transport
    // Work, row, exit, or Native result is manufactured by this test.
    assert!(previous.is_none());
    wake
}

fn return_original_credit_without_source_progress(f: &Fixture, value: u64) {
    let mut bank = f.bank.lock().unwrap();
    let role = bank.held.remove(&value).unwrap();
    let previous = bank.originals.get_mut(&value).unwrap().role.replace(role);
    assert!(previous.is_none());
    // Unlike Fixture::release_credit, availability is reported by a separate
    // owner's original HostWake, without a local Bank.changed bridge. There
    // is no new input, completion, timer, or fictional retirement progress.
}

fn scan_cut(f: &Fixture) -> (Option<usize>, bool, usize, bool) {
    let entry = f.host.state.lock().unwrap().entries[&f.scope.id()].clone();
    let machine = entry.machine.lock().unwrap();
    let machine = machine.as_ref().unwrap();
    (
        machine.scan_remaining,
        machine.rescan_requested,
        machine.dispatch.ready_positions(),
        machine.dispatch.has_unindexed(&f.runtime).unwrap(),
    )
}

#[tokio::test]
async fn an_external_credit_wake_owes_one_rescan_after_the_old_full_pass() {
    let full = (1..=16).collect::<Vec<_>>();
    let f = Fixture::new(
        full.iter()
            .map(|&value| vec![(value, value, vec![])])
            .collect(),
        &full,
    );
    let wake = install_original_transport_waker(&f);
    for batch in 0..16 {
        f.send(batch);
    }
    let (_, first_idle) = f.to_idle();
    let old_visits = f.captured().0.visits.len();

    // Begin a genuine finite pass over already-indexed, still-Full heads.
    f.runtime.notify_host_actor(&f.registration).unwrap();
    let started = f.drive_one();
    let cut = scan_cut(&f);
    let visits_before_credit = f.captured().0.visits.clone();
    // Fair purpose rotation can change the first visited identity. Return a
    // position that this same pass ACTUALLY visited while it was still Full.
    // Keep the fallback within the original inventory so setup failures are
    // diagnosed only after the fixture's actual teardown below.
    let returned = visits_before_credit.get(old_visits).copied().unwrap_or(1);
    return_original_credit_without_source_progress(&f, returned);
    wake.wake_by_ref();
    let (_, idle) = f.to_idle();
    let (observed, accepted) = f.captured();
    let quiet = (0..8).all(|_| !f.drive_one());
    drop(wake);
    let cleanup = f.finish(false);

    // Expected red oracles follow actual captures/roles/report/header/actor/
    // Scope teardown, so a missed wake cannot trigger a second Drop panic.
    assert_eq!(cleanup, (16, true));
    assert!(first_idle && started && idle && quiet);
    assert_eq!(cut, (Some(6), false, 16, false));
    assert_eq!(visits_before_credit.len() - old_visits, 10);
    assert!(visits_before_credit[old_visits..].contains(&returned));
    assert_eq!(observed.availability_changes, 0);
    assert_eq!(
        accepted,
        vec![returned],
        "the original external wake must revisit the earlier Full head"
    );
    assert_eq!(observed.borrowed, vec![returned]);
    assert_eq!(observed.bound, vec![returned]);
    assert!(observed.origins.iter().all(|same| *same));
}

#[tokio::test]
async fn an_external_credit_wake_survives_exact_pending_retirement_budget_before_first_scan() {
    // Ten independently paid original custodies, but ONE committed effect.
    let full = (1..=10).collect::<Vec<_>>();
    let f = Fixture::new(
        full.iter()
            .map(|&value| vec![(value, value, vec![])])
            .collect(),
        &full,
    );
    let wake = install_original_transport_waker(&f);
    f.send(0);
    let (_, first_idle) = f.to_idle();
    let before = f.captured().0;
    let cut = scan_cut(&f);
    let budget = f.host.state.lock().unwrap().entries[&f.scope.id()].budget;
    return_original_credit_without_source_progress(&f, 1);
    f.bank.lock().unwrap().inspect_pending_retirement = true;
    wake.wake_by_ref();
    let woke = f.drive_one();
    let after_pending_checks = f.captured().0;
    let after_cut = scan_cut(&f);
    // Only this real finite Pending inspection occupies the allowance. Later
    // turns have their normal budgets; no second availability wake is issued.
    f.bank.lock().unwrap().inspect_pending_retirement = false;
    let (_, idle) = f.to_idle();
    let (observed, accepted) = f.captured();
    let quiet = (0..8).all(|_| !f.drive_one());
    drop(wake);
    let cleanup = f.finish(false);

    assert_eq!(cleanup, (10, true));
    assert!(first_idle && woke && idle && quiet);
    assert_eq!(budget, 10);
    assert_eq!(cut, (None, false, 1, false));
    assert_eq!(
        after_pending_checks.retirement_checks - before.retirement_checks,
        budget
    );
    assert_eq!(after_pending_checks.visits, before.visits);
    assert_eq!(after_cut.2, 1);
    assert!(!after_cut.3);
    assert_eq!(observed.availability_changes, 0);
    assert_eq!(
        accepted,
        vec![1],
        "the consumed external credit event still owes its first ready scan"
    );
    assert_eq!(observed.borrowed, vec![1]);
    assert_eq!(observed.bound, vec![1]);
    assert!(observed.origins.iter().all(|same| *same));
}

#[test]
fn a_pure_host_continuation_and_pending_checks_do_not_create_another_ready_pass() {
    let full = (1..=10).collect::<Vec<_>>();
    let f = Fixture::new(
        full.iter()
            .map(|&value| vec![(value, value, vec![])])
            .collect(),
        &full,
    );
    for batch in 0..10 {
        f.send(batch);
    }
    let (_, first_idle) = f.to_idle();
    let before = f.captured().0;
    let before_cut = scan_cut(&f);
    let budget = f.host.state.lock().unwrap().entries[&f.scope.id()].budget;

    // Construct only the real Kernel continuation boundary. Consume the
    // fixture's scheduling notification without claiming a credit change,
    // then requeue the SAME registration through release(true). No input,
    // availability return, exit, or Source progress is manufactured here.
    f.runtime.notify_host_actor(&f.registration).unwrap();
    let notified = f.runtime.claim_host_actor().unwrap();
    let original_notified = notified.was_notified();
    let notified_origin = notified.registration().same_registration(&f.registration);
    notified.release(true).unwrap();
    let continuation = f.runtime.claim_host_actor().unwrap();
    let continuation_notified = continuation.was_notified();
    let continuation_origin = continuation
        .registration()
        .same_registration(&f.registration);
    f.bank.lock().unwrap().inspect_pending_retirement = true;
    f.host.drive(Tick(1), continuation);
    let after_cut = scan_cut(&f);
    // No new notification or credit follows this inspection. A manufactured
    // debt must fail the unchanged scan-cut, visit-count, or idle oracle.
    let (extra_turns, idle) = f.to_idle();
    let (observed, accepted) = f.captured();
    let quiet = (0..8).all(|_| !f.drive_one());
    let cleanup = f.finish(false);

    assert_eq!(cleanup, (10, true));
    assert!(first_idle && original_notified && notified_origin);
    assert!(!continuation_notified && continuation_origin);
    assert_eq!(budget, 10);
    assert_eq!(before_cut, (None, false, 10, false));
    assert_eq!(after_cut, before_cut);
    assert_eq!(extra_turns, 0);
    assert!(idle && quiet);
    assert_eq!(
        observed.retirement_checks - before.retirement_checks,
        budget
    );
    assert_eq!(observed.visits, before.visits);
    assert_eq!(observed.availability_changes, 0);
    assert!(accepted.is_empty());
    assert!(observed.borrowed.is_empty() && observed.bound.is_empty());
}
