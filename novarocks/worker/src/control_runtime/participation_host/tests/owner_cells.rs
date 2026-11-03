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

//! The real Participation cell and directory retain one bootstrap inventory. These
//! checks neither create a host instance nor infer Native/WorkerNodeHost progress.

use super::*;
use std::{
    sync::mpsc::{self, RecvTimeoutError},
    time::Duration,
};

fn charges(fixture: &Harness) -> [u64; 4] {
    actual_admission_usage(fixture)
}

#[test]
fn directory_lookup_does_not_hold_the_participation_cell_and_removed_aliases_cannot_borrow_it() {
    let mut fixture = Harness::new(PARTICIPATION_BOOTSTRAP_STORAGE);
    let materialize = fixture.materialize();
    let binding = fixture.binding(participation_actor(), &materialize);
    fixture
        .service
        .adopt_participation(&materialize, binding)
        .unwrap();
    fixture.complete_materialize_handoff(materialize);
    let directory = fixture.service.directory();
    let address = directory.participation(fixture.identity).unwrap();
    let stale_alias = address.clone();
    let guard = address.lock().unwrap();
    let scope = guard.scope().clone();
    let baseline = scope.snapshot().unwrap();
    let paid = charges(&fixture);
    let registration = guard.registration().clone();
    // A pointer value is only a test observation of the same resident record;
    // it is never dereferenced or used to construct an owner/Work authority.
    let original_record = std::ptr::from_ref(&*guard).addr();
    let original_lifetime = guard.resources.metadata_resources[LIFETIME].work.id();
    let identity = fixture.identity;
    let worker_directory = directory.clone();
    let (looked_up, lookup_rx) = mpsc::sync_channel(1);
    let (acquired, acquire_rx) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let original_address = worker_directory.participation(identity);
        // This notice precedes any attempt to acquire the Participation cell. It
        // must arrive while the main thread still owns that cell's guard.
        let _ = looked_up.send(original_address.is_some());
        let observation = original_address.and_then(|address| {
            let guard = address.lock()?;
            Some((
                std::ptr::from_ref(&*guard).addr(),
                guard.scope().clone(),
                guard.resources.metadata_resources[LIFETIME].work.id(),
            ))
        });
        let _ = acquired.send(observation);
    });
    let lookup_completed_while_guard_held =
        matches!(lookup_rx.recv_timeout(Duration::from_secs(5)), Ok(true));
    let early = acquire_rx.recv_timeout(Duration::from_millis(20));
    let excluded_while_guard_held = matches!(&early, Err(RecvTimeoutError::Timeout));
    // Release before inspecting any failure oracle or joining the actual
    // thread, even when the directory lookup unexpectedly timed out.
    drop(guard);
    let acquired_after_release = match early {
        Ok(observation) => Some(observation),
        Err(RecvTimeoutError::Timeout) => acquire_rx.recv_timeout(Duration::from_secs(5)).ok(),
        Err(RecvTimeoutError::Disconnected) => None,
    };
    let worker_exited = worker.join().is_ok();
    let same_original =
        acquired_after_release
            .flatten()
            .is_some_and(|(record, actual_scope, lifetime)| {
                record == original_record
                    && actual_scope.same_scope(&scope)
                    && lifetime == original_lifetime
            });
    let address_clones_uncharged =
        scope.snapshot().unwrap() == baseline && charges(&fixture) == paid;

    // The same Common host-turn registration resolves to this actual cell;
    // Root metadata turns have no entry in the Participation directory.
    fixture.runtime.notify_host_actor(&registration).unwrap();
    let mut original_host_turn_found = false;
    let mut matching_turn_resolved = false;
    let mut unrelated_turn_rejected = true;
    for _ in 0..4 {
        let Some(turn) = fixture.runtime.claim_host_actor() else {
            break;
        };
        if turn.matches_registration(&registration) {
            original_host_turn_found = true;
            matching_turn_resolved = directory.host_turn(&turn).is_some_and(|address| {
                address
                    .lock()
                    .is_some_and(|guard| std::ptr::from_ref(&*guard).addr() == original_record)
            });
        } else {
            unrelated_turn_rejected &= directory.host_turn(&turn).is_none();
        }
        turn.release(false).unwrap();
    }
    let ledger = fixture.ledger.clone();
    let runtime = fixture.runtime.clone();
    // This existing path really disposes the bootstrap inventory, removes
    // its actor/Scope, and transfers the original Host through RemoveHost.
    // It calls dispose_service_bootstrap; no synthetic removal bit is used.
    let (host, retirement) = fixture.actual_model_refusal_retirement();
    let directory_removed = directory.participation(identity).is_none();
    let old_address_cannot_borrow = stale_alias.lock().is_none() && address.lock().is_none();
    fixture.finish_retired_participation(host, retirement);
    let old_alias_stays_empty = stale_alias.lock().is_none();

    assert!(lookup_completed_while_guard_held);
    assert!(excluded_while_guard_held && worker_exited && same_original);
    assert!(address_clones_uncharged);
    assert!(original_host_turn_found && matching_turn_resolved && unrelated_turn_rejected);
    assert!(directory_removed && old_address_cannot_borrow && old_alias_stays_empty);
    assert!(runtime.is_empty() && ledger.is_empty());
}
