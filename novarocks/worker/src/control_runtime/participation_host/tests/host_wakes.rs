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

//! The real Participation notice wakes its original Common host owner. No second
//! ready index, host instance, Native execution or physical exit is simulated.

use super::*;

fn charges(fixture: &Harness) -> [u64; 4] {
    actual_admission_usage(fixture)
}

#[test]
fn original_notice_wakes_and_coalesces_without_retiring_work_and_ends_only_after_scope_removal() {
    let mut fixture = Harness::new(PARTICIPATION_BOOTSTRAP_STORAGE);
    let materialize = fixture.materialize();
    let binding = fixture.binding(participation_actor(), &materialize);
    fixture
        .service
        .adopt_participation(&materialize, binding)
        .unwrap();
    fixture.complete_materialize_handoff(materialize);
    Harness::release_drained_effect_claims(&fixture.runtime);
    let address = fixture.service.participation(fixture.identity).unwrap();
    let host = address.lock().unwrap();
    let notice = host.resources.notice.as_ref().unwrap().clone();
    let registration = host.registration().clone();
    let scope = host.scope().clone();
    drop(host);
    let same_original_notice = notice.same_origin(&notice.clone());
    let live_scope_refused = notice.unregister_removed().is_err();
    // Intake closure and a real stop request still leave the original
    // metadata Work and attachment alive; neither authorizes unregister.
    scope.close_external().unwrap();
    scope.request_stop().unwrap();
    let stopped_scope_refused = notice.unregister_removed().is_err();
    let baseline = scope.snapshot().unwrap();
    let funded = charges(&fixture);
    Harness::release_drained_effect_claims(&fixture.runtime);
    let first_notice = notice.clone();
    let first_notifier = std::thread::spawn(move || first_notice.notify());
    let first_thread_exited = first_notifier.join().is_ok();
    let mut first_is_original = false;
    let mut repeated_thread_exited = false;
    let mut held_turn_exclusive = false;
    let mut repeated_mark_survived_release = false;
    let mut repetitions_coalesced = false;
    if let Some(turn) = fixture.runtime.claim_host_actor() {
        first_is_original = turn.matches_registration(&registration);
        let repeated = notice.clone();
        let notifier = std::thread::spawn(move || {
            for _ in 0..8 {
                repeated.notify();
            }
        });
        repeated_thread_exited = notifier.join().is_ok();
        held_turn_exclusive = match fixture.runtime.claim_host_actor() {
            Some(unexpected) => {
                unexpected.release(false).unwrap();
                false
            }
            None => true,
        };
        turn.release(false).unwrap();
        if let Some(next) = fixture.runtime.claim_host_actor() {
            repeated_mark_survived_release = next.matches_registration(&registration);
            next.release(false).unwrap();
        }
        repetitions_coalesced = match fixture.runtime.claim_host_actor() {
            Some(unexpected) => {
                unexpected.release(false).unwrap();
                false
            }
            None => true,
        };
    }
    let no_work_or_fee_exit = scope.snapshot().unwrap() == baseline && charges(&fixture) == funded;
    let runtime = fixture.runtime.clone();
    let ledger = fixture.ledger.clone();
    // This existing path really disposes all original bootstrap roles,
    // removes the child Scope/actor and transfers the original host record.
    let (host, retirement) = fixture.actual_model_refusal_retirement();
    let child_scope_really_removed = matches!(
        scope.gates(),
        Err(novarocks_control_runtime::transaction::RuntimeError::UnknownScope)
    );
    let notice_may_unregister = notice.unregister_removed().is_ok();
    fixture.finish_retired_participation(host, retirement);
    notice.notify();
    let retired_actor_not_woken = match runtime.claim_host_actor() {
        Some(unexpected) => {
            unexpected.release(false).unwrap();
            false
        }
        None => true,
    };
    let removal_check_stays_idempotent = notice.unregister_removed().is_ok();

    assert!(same_original_notice && live_scope_refused && stopped_scope_refused);
    assert!(first_thread_exited && first_is_original && repeated_thread_exited);
    assert!(held_turn_exclusive && repeated_mark_survived_release && repetitions_coalesced);
    assert!(no_work_or_fee_exit);
    assert!(child_scope_really_removed && notice_may_unregister && removal_check_stays_idempotent);
    assert!(retired_actor_not_woken);
    assert!(runtime.is_empty() && ledger.is_empty());
}
