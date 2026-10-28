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

//! Metadata-only probes. These IDs address slots, not admitted Work or funds.
use super::*;

fn index(limit: usize) -> ControlRequestReady {
    ControlRequestReady::new(NonZeroUsize::new(limit).unwrap(), ActorRuntime::new(1))
}
fn input(id: u64) -> InputId {
    InputId::new(id).unwrap()
}
fn scope_id(id: u64) -> ScopeId {
    ScopeId::new(id).unwrap()
}
fn work(id: u64) -> WorkId {
    WorkId::new(id).unwrap()
}

#[test]
fn both_domains_share_one_limit_but_keep_distinct_numeric_namespaces() {
    let ready = index(2);
    assert_eq!(ready.limit().get(), 2);
    assert!(ready.same_owner(&ready.clone()));
    let control = ready.register_control(input(1)).unwrap();
    let context = ready.register_context(scope_id(1), work(1)).unwrap();
    assert_eq!(ready.registered(), 2);
    assert!(matches!(
        ready.register_control(input(1)),
        Err(ControlRequestError::DuplicateReadySlot)
    ));
    assert!(matches!(
        ready.register_context(scope_id(1), work(1)),
        Err(ControlRequestError::DuplicateReadySlot)
    ));
    assert!(matches!(
        ready.register_control(input(2)),
        Err(ControlRequestError::ReadyFull)
    ));
    assert!(matches!(
        ready.register_context(scope_id(1), work(2)),
        Err(ControlRequestError::ReadyFull)
    ));
    ready.unregister(&context).unwrap();
    let next = ready.register_control(input(2)).unwrap();
    assert_eq!(ready.registered(), 2);
    ready.unregister(&control).unwrap();
    ready.unregister(&next).unwrap();
    assert_eq!(ready.registered(), 0);
}

#[test]
fn coalesced_notifications_never_consume_the_other_domain() {
    let ready = index(2);
    let control = ready.register_control(input(1)).unwrap();
    let context = ready.register_context(scope_id(1), work(1)).unwrap();
    let observed = ready.change_version();
    control.notify();
    control.notify();
    context.notify();
    context.notify();
    assert!(ready.change_version() > observed);
    assert_eq!(ready.next_ready(), Some(input(1)));
    assert_eq!(ready.next_ready(), None);
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(1))));
    assert_eq!(ready.next_context_ready(), None);
    context.notify();
    control.notify();
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(1))));
    assert_eq!(ready.next_context_ready(), None);
    assert_eq!(ready.next_ready(), Some(input(1)));
    ready.unregister(&control).unwrap();
    ready.unregister(&context).unwrap();
}

#[test]
fn each_domain_wraps_fairly_past_a_requeued_lower_address() {
    let ready = index(4);
    let controls = [
        ready.register_control(input(1)).unwrap(),
        ready.register_control(input(2)).unwrap(),
    ];
    let contexts = [
        ready.register_context(scope_id(1), work(1)).unwrap(),
        ready.register_context(scope_id(1), work(2)).unwrap(),
    ];
    for slot in controls.iter().chain(contexts.iter()) {
        slot.notify();
    }
    assert_eq!(ready.next_ready(), Some(input(1)));
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(1))));
    controls[0].notify();
    contexts[0].notify();
    assert_eq!(ready.next_ready(), Some(input(2)));
    assert_eq!(ready.next_ready(), Some(input(1)));
    assert_eq!(ready.next_ready(), None);
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(2))));
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(1))));
    assert_eq!(ready.next_context_ready(), None);
    for slot in controls.iter().chain(contexts.iter()) {
        ready.unregister(slot).unwrap();
    }
}

#[test]
fn foreign_and_replaced_slots_cannot_remove_or_notify_the_original_address() {
    let ready = index(2);
    let foreign = index(2);
    let original = ready.register_control(input(1)).unwrap();
    let other = foreign.register_control(input(1)).unwrap();
    assert!(original.belongs_to(&ready));
    assert!(!other.belongs_to(&ready));
    assert!(!ready.same_owner(&foreign));
    assert!(matches!(
        ready.unregister(&other),
        Err(ControlRequestError::UnknownReadySlot)
    ));
    ready.unregister(&original).unwrap();
    let replacement = ready.register_control(input(1)).unwrap();
    let observed = ready.change_version();
    original.notify();
    assert_eq!(ready.change_version(), observed);
    assert!(matches!(
        ready.unregister(&original),
        Err(ControlRequestError::UnknownReadySlot)
    ));
    other.notify();
    assert_eq!(ready.next_ready(), None);
    assert_eq!(foreign.next_ready(), Some(input(1)));
    replacement.notify();
    assert_eq!(ready.next_ready(), Some(input(1)));
    assert_eq!(ready.registered(), 1);
    ready.unregister(&replacement).unwrap();
    foreign.unregister(&other).unwrap();
}

#[test]
fn a_retained_slot_does_not_retain_the_index_or_its_runtime() {
    let ready = index(1);
    let slot = ready.register_context(scope_id(1), work(1)).unwrap();
    let actual_index = Arc::downgrade(&ready.shared);
    drop(ready);
    assert!(actual_index.upgrade().is_none());
    slot.notify();
}

#[test]
fn external_end_is_idempotent_and_notifies_only_after_the_phase_unlocks() {
    use novarocks_control_runtime::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::{capacity::ResourceVector, identity::Generation};
    let ready = index(1);
    let slot = ready.register_context(scope_id(1), work(1)).unwrap();
    let leg = RequestExternalLeg::new(slot.clone());
    assert!(Arc::ptr_eq(leg.slot(), &slot));
    assert!(!leg.is_ended());
    let ledger = RuntimeLedger::new(ResourceVector::default(), 1, 1);
    let scope = ledger
        .open_scope_next(None, Generation::new(1).unwrap(), 1, 1)
        .unwrap();
    let reply = PublishedReply::<u8>::new(&scope);
    assert_eq!(leg.try_take(&reply), None);
    assert!(*leg.phase.lock().unwrap() == ExternalPhase::Waiting);
    let observed = ready.change_version();
    leg.end();
    assert!(leg.is_ended());
    assert!(leg.phase.try_lock().is_ok());
    assert!(ready.change_version() > observed);
    let ended = ready.change_version();
    leg.end();
    assert_eq!(ready.change_version(), ended);
    assert_eq!(leg.try_take(&reply), None);
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(1))));
    leg.notify();
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(1))));
    ready.unregister(leg.slot()).unwrap();
    drop(leg);
    drop(reply);
    scope.close_external().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}

#[test]
fn equal_work_numbers_in_different_scopes_are_independent_fair_addresses() {
    let ready = index(3);
    let control = ready.register_control(input(7)).unwrap();
    let first = ready.register_context(scope_id(1), work(7)).unwrap();
    let second = ready.register_context(scope_id(2), work(7)).unwrap();
    assert_eq!(ready.registered(), 3);
    assert!(matches!(
        ready.register_context(scope_id(1), work(7)),
        Err(ControlRequestError::DuplicateReadySlot)
    ));
    assert!(matches!(
        ready.register_context(scope_id(3), work(7)),
        Err(ControlRequestError::ReadyFull)
    ));
    assert!(matches!(
        ready.register_control(input(8)),
        Err(ControlRequestError::ReadyFull)
    ));
    first.notify();
    second.notify();
    control.notify();
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(7))));
    first.notify();
    assert_eq!(ready.next_context_ready(), Some((scope_id(2), work(7))));
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(7))));
    assert_eq!(ready.next_context_ready(), None);
    assert_eq!(ready.next_ready(), Some(input(7)));
    ready.unregister(&first).unwrap();
    let replacement = ready.register_context(scope_id(1), work(7)).unwrap();
    let observed = ready.change_version();
    first.notify();
    assert_eq!(ready.change_version(), observed);
    assert!(matches!(
        ready.unregister(&first),
        Err(ControlRequestError::UnknownReadySlot)
    ));
    second.notify();
    assert_eq!(ready.next_context_ready(), Some((scope_id(2), work(7))));
    assert_eq!(ready.next_context_ready(), None);
    replacement.notify();
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(7))));
    ready.unregister(&control).unwrap();
    ready.unregister(&second).unwrap();
    ready.unregister(&replacement).unwrap();
    assert_eq!(ready.registered(), 0);
}

#[test]
fn fresh_lower_scope_work_cannot_overtake_an_already_ready_other_scope() {
    let ready = index(2);
    let lower = ready.register_context(scope_id(1), work(1)).unwrap();
    let waiting = ready.register_context(scope_id(2), work(1)).unwrap();
    lower.notify();
    waiting.notify();
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(1))));
    ready.unregister(&lower).unwrap();
    let fresh = ready.register_context(scope_id(1), work(2)).unwrap();
    fresh.notify();
    assert_eq!(
        ready.next_context_ready(),
        Some((scope_id(2), work(1))),
        "registration churn in a lower scope cannot jump the real ready queue"
    );
    assert_eq!(ready.next_context_ready(), Some((scope_id(1), work(2))));
    ready.unregister(&waiting).unwrap();
    ready.unregister(&fresh).unwrap();
}

#[test]
fn an_old_slot_becoming_ready_later_cannot_be_starved_by_new_registration_churn() {
    let ready = index(2);
    let old = ready.register_context(scope_id(1), work(1)).unwrap();
    let previous = ready.register_context(scope_id(2), work(1)).unwrap();
    previous.notify();
    assert_eq!(ready.next_context_ready(), Some((scope_id(2), work(1))));
    old.notify();
    ready.unregister(&previous).unwrap();
    let fresh = ready.register_context(scope_id(2), work(2)).unwrap();
    fresh.notify();
    assert_eq!(
        ready.next_context_ready(),
        Some((scope_id(1), work(1))),
        "ready entry time, not address or registration time, determines FIFO"
    );
    assert_eq!(ready.next_context_ready(), Some((scope_id(2), work(2))));
    ready.unregister(&old).unwrap();
    ready.unregister(&fresh).unwrap();
}

#[test]
fn control_addresses_also_follow_actual_ready_order_and_unregistration_removes_queued_marks() {
    let ready = index(2);
    let first = ready.register_control(input(9)).unwrap();
    let second = ready.register_control(input(1)).unwrap();
    first.notify();
    second.notify();
    assert_eq!(ready.next_ready(), Some(input(9)));
    first.notify();
    assert_eq!(ready.next_ready(), Some(input(1)));
    ready.unregister(&first).unwrap();
    assert_eq!(ready.next_ready(), None);
    ready.unregister(&second).unwrap();
}
