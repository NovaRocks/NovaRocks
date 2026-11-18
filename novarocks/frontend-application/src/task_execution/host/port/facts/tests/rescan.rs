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

//! Actual paid bank roles and a real consumer index, without LP policy claims.
//! This verifies the new API; the invalid inner Source prototype is not a RED.
use super::*;
use crate::control_runtime::host::FrontendLanePolicy;
use novarocks_control_runtime::{actor_turn::InputPhase, effect_dispatch::EffectDispatch};

#[test]
fn original_consumer_index_rescan_survives_the_partial_fact_pass_and_coalesces() {
    for request_rescan in [false, true] {
        let mut f = Fixture::new();
        let (bank, wake) = bank(&f);
        let original_roles = {
            let state = bank.lock();
            state
                .slots
                .each_ref()
                .map(|slot| slot.position.as_ref().unwrap().id())
        };
        let ticket = f.emit(false, terminal_effect(731));
        let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
        let input = loan
            .publish_now(&f.runtime, &ticket, terminal(731))
            .unwrap();
        drop(loan);
        f.runtime.effect_completed(ticket).unwrap();
        let originally_filled = f
            .runtime
            .input_custody(f.logical.registration.actor(), input)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::Filled);
        // The real original producer declares Full and registers its waiter.
        let full_before_consume = bank.try_loan(FactRole::Lifecycle).is_none();
        let life = bank.progress(1, false, false).unwrap();
        let auth = bank.progress(1, false, false).unwrap();
        let old_frontier = {
            let state = bank.lock();
            (state.cursor, state.remaining, state.rescan_requested)
        };
        f.consume();
        let committed_empty = f
            .runtime
            .input_custody(f.logical.registration.actor(), input)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::Committed)
            && f.runtime
                .effect_inventory(f.logical.registration.actor(), input, None, 1)
                .is_ok_and(|effects| effects.is_empty());
        let mut dispatch = EffectDispatch::new(
            f.logical.registration.actor(),
            32,
            FrontendLanePolicy::logical(1),
        );
        let indexed = dispatch.index_committed(&f.runtime, 1).unwrap();
        let wake_before_hint = wake.0.load(Ordering::SeqCst);
        if request_rescan {
            for _ in 0..8 {
                bank.request_rescan();
            }
        }
        let hint_does_not_wake = wake.0.load(Ordering::SeqCst) == wake_before_hint;
        // This visit is still the previous pass's PreReady, not Lifecycle.
        let old_tail = bank.progress(1, false, false).unwrap();
        let tail_frontier = {
            let state = bank.lock();
            (state.cursor, state.remaining, state.rescan_requested)
        };
        let still_original = f
            .runtime
            .input_custody(f.logical.registration.actor(), input)
            .unwrap()
            .is_some();
        let mut followup = Vec::new();
        let mut more = old_tail.more;
        for _ in 0..8 {
            if !more {
                break;
            }
            let next = bank.progress(1, false, false).unwrap();
            more = next.more;
            followup.push(next);
        }
        let actual_retired = f
            .runtime
            .input_custody(f.logical.registration.actor(), input)
            .unwrap()
            .is_none();
        let role_loan = bank.try_loan(FactRole::Lifecycle);
        let reusable = role_loan.is_some();
        let same_role = role_loan
            .as_ref()
            .is_some_and(|loan| loan.position.as_ref().unwrap().id() == original_roles[0]);
        drop(role_loan);
        let final_frontier = {
            let state = bank.lock();
            (state.cursor, state.remaining, state.rescan_requested)
        };
        let same_inventory = {
            let state = bank.lock();
            state
                .slots
                .each_ref()
                .map(|slot| slot.position.as_ref().unwrap().id())
                == original_roles
        };
        let actual_credit_wakes = wake.0.load(Ordering::SeqCst) - wake_before_hint;

        // The no-hint branch intentionally parks. Original producer exit and
        // explicit Source close below are separate cleanup events, never proof
        // that the earlier captured credit frontier progressed by itself.
        bank.close_admission();
        let closed = bank.progress(3, true, false).unwrap();
        let closed_wakes = wake.0.load(Ordering::SeqCst);
        for _ in 0..8 {
            bank.request_rescan();
        }
        let ignored_after_close = {
            let state = bank.lock();
            state.live == 0 && !state.rescan_requested
        };
        let closed_hint_does_not_wake = wake.0.load(Ordering::SeqCst) == closed_wakes;
        let closed_probe = bank.progress(1, true, false).unwrap();
        drop(dispatch);
        f.finish(); // Actual input/role/header/actor/Scope/ledger teardown first.

        assert!(originally_filled && full_before_consume && committed_empty);
        assert_eq!(indexed.inputs, [input]);
        assert_eq!(
            indexed.units, 1,
            "the real empty batch still has its original index charge"
        );
        assert_eq!(life.units, 1);
        assert_eq!(auth.units, 1);
        assert!(!life.progressed && !auth.progressed && life.more && auth.more);
        assert_eq!(old_frontier, (2, 1, false));
        assert!(hint_does_not_wake && same_inventory);
        assert_eq!(old_tail.units, 1);
        assert!(!old_tail.progressed && still_original);
        assert_eq!(old_tail.more, request_rescan);
        assert_eq!(
            tail_frontier,
            (0, if request_rescan { 3 } else { 0 }, false)
        );
        assert_eq!(
            followup.len(),
            if request_rescan { 3 } else { 0 },
            "eight index hints coalesce into exactly one original three-role pass"
        );
        assert!(!more);
        assert_eq!(final_frontier, (0, 0, false));
        assert_eq!(actual_retired, request_rescan);
        assert_eq!(reusable, request_rescan);
        assert_eq!(same_role, request_rescan);
        assert_eq!(actual_credit_wakes, usize::from(request_rescan));
        assert!(closed.complete && bank.is_closed());
        assert!(ignored_after_close && closed_hint_does_not_wake);
        assert!(closed_probe.complete && !closed_probe.more && closed_probe.units == 0);
    }
}
