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

//! Small deterministic correctness cases. IDs and expected traces are explicit;
//! no clock, thread, executor, database fixture or cleanup is required.

use novarocks_control_runtime_core::{
    capacity::{
        CapacityError, CapacityLedger, ReservationRequest, ReservationResources, ResourceVector,
    },
    completion::{CompletionSlot, ConsumerKind, Consumption},
    effect::{EffectBatchState, EffectDescriptor, EffectKind, EffectState},
    fair_queue::FairWorkQueue,
    history::{HistoryBuffer, HistoryError, HistoryFact},
    identity::*,
    mailbox::{InputClass, MailboxError, MailboxState},
    owned_storage::{OwnedSlotNeed, OwnedStorageError},
    scheduler::{ReadyQueue, SchedulerError, TurnBudget},
    scope::{ScopeError, ScopeState, WorkAdmission, WorkKind},
    timer::{TimerError, TimerIndex},
};

fn scope(id: u64) -> ScopeState {
    ScopeState::new(ScopeId::new(id).unwrap(), None, generation())
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn work(id: u64) -> WorkId {
    WorkId::new(id).unwrap()
}
fn actor(id: u64) -> ActorId {
    ActorId::new(id).unwrap()
}
fn input(id: u64) -> InputId {
    InputId::new(id).unwrap()
}
fn dimension(id: u64) -> DimensionId {
    DimensionId::new(id).unwrap()
}
fn vector(values: &[(u64, u64)]) -> ResourceVector {
    ResourceVector::new(values.iter().map(|(id, amount)| (dimension(*id), *amount))).unwrap()
}
fn quota_partition(values: &[(u64, u64)]) -> ReservationResources {
    ReservationResources::new(vector(values), OwnedSlotNeed::default())
}
fn request(id: u64, values: &[(u64, u64)]) -> ReservationRequest {
    ReservationRequest {
        id: ReservationId::new(id).unwrap(),
        generation: generation(),
        guard: work(id),
        resources: ReservationResources::for_work(vector(values)),
        admission: WorkAdmission::External,
        kind: WorkKind::Business,
    }
}
fn finish(scope: &mut ScopeState, id: WorkId) {
    scope.body_exited(id).unwrap();
    scope.captures_exited(id).unwrap();
    scope.retire_work(id).unwrap();
}

#[test]
fn bundle_failure_does_not_charge_any_dimension_or_publish_a_guard() {
    let mut ledger = CapacityLedger::new(vector(&[(1, 2), (2, 10)]));
    let mut a = scope(1);
    let id = ledger
        .try_reserve_bundle(&mut a, request(1, &[(1, 1), (2, 9)]))
        .unwrap();
    let mut b = scope(2);
    assert_eq!(
        ledger.try_reserve_bundle(&mut b, request(2, &[(1, 1), (2, 2)])),
        Err(CapacityError::Exhausted(dimension(2)))
    );
    assert_eq!(
        (
            ledger.used(dimension(1)),
            ledger.used(dimension(2)),
            b.work_count()
        ),
        (1, 9, 0)
    );
    a.close_external();
    assert!(
        !a.join_ready(),
        "unbound transaction guard is already scope-visible"
    );
    ledger.rollback_unbound(&mut a, id).unwrap();
    assert!(a.join_ready());
    assert_eq!(
        (ledger.used(dimension(1)), ledger.used(dimension(2))),
        (0, 0)
    );
}

#[test]
fn bound_queued_payload_cannot_rollback_or_release_work_on_terminal() {
    let mut ledger = CapacityLedger::new(vector(&[(1, 1), (2, 100)]));
    let mut s = scope(1);
    let id = ledger
        .try_reserve_bundle(&mut s, request(1, &[(1, 1), (2, 100)]))
        .unwrap();
    ledger.bind(&mut s, id, WorkKind::Business).unwrap();
    s.request_stop();
    assert_eq!(
        ledger.rollback_unbound(&mut s, id),
        Err(CapacityError::BoundReservation)
    );
    assert!(!s.join_ready());
    assert_eq!(ledger.used(dimension(2)), 100);
    s.body_exited(work(1)).unwrap();
    assert_eq!(s.retire_work(work(1)), Err(ScopeError::ExitUnobserved));
    ledger.release_quota(id, &vector(&[(1, 1)])).unwrap();
    assert_eq!(
        (ledger.used(dimension(1)), ledger.used(dimension(2))),
        (0, 100)
    );
    s.captures_exited(work(1)).unwrap();
    ledger.release_quota(id, &vector(&[(2, 100)])).unwrap();
    s.retire_work(work(1)).unwrap();
    ledger.finish(&s, id).unwrap();
    assert!(s.join_ready());
}

#[test]
fn splitting_and_transfer_preserve_total_charge_and_exact_holder() {
    let mut ledger = CapacityLedger::new(vector(&[(1, 2), (2, 100)]));
    let mut a = scope(1);
    let mut b = scope(2);
    let id = ledger
        .try_reserve_bundle(&mut a, request(1, &[(1, 1), (2, 100)]))
        .unwrap();
    ledger.bind(&mut a, id, WorkKind::Business).unwrap();
    let retained = ReservationId::new(2).unwrap();
    ledger
        .split(id, retained, &quota_partition(&[(2, 100)]))
        .unwrap();
    b.admit(
        generation(),
        work(2),
        WorkKind::Cleanup,
        WorkAdmission::External,
    )
    .unwrap();
    ledger.transfer(retained, &b, work(2)).unwrap();
    assert_eq!(ledger.reservation(retained).unwrap().scope, b.id());
    assert_eq!(ledger.used(dimension(2)), 100);
    ledger.release_quota(id, &vector(&[(1, 1)])).unwrap();
    finish(&mut a, work(1));
    ledger.finish(&a, id).unwrap();
    assert_eq!(ledger.used(dimension(2)), 100);
    ledger
        .release_quota(retained, &vector(&[(2, 100)]))
        .unwrap();
    finish(&mut b, work(2));
    ledger.finish(&b, retained).unwrap();
    assert_eq!(ledger.reservation_count(), 0);
}

#[test]
fn close_allows_existing_parent_drain_but_stop_allows_only_cleanup() {
    let mut s = scope(1);
    s.admit(
        generation(),
        work(1),
        WorkKind::Business,
        WorkAdmission::External,
    )
    .unwrap();
    s.close_external();
    assert_eq!(
        s.admit(
            generation(),
            work(2),
            WorkKind::Business,
            WorkAdmission::External
        ),
        Err(ScopeError::Closed)
    );
    s.admit(
        generation(),
        work(2),
        WorkKind::Business,
        WorkAdmission::Derived(work(1)),
    )
    .unwrap();
    s.request_stop();
    assert_eq!(
        s.admit(
            generation(),
            work(3),
            WorkKind::Business,
            WorkAdmission::Derived(work(1))
        ),
        Err(ScopeError::Stopped)
    );
    s.admit(
        generation(),
        work(3),
        WorkKind::Cleanup,
        WorkAdmission::Derived(work(1)),
    )
    .unwrap();
    s.body_exited(work(1)).unwrap();
    s.captures_exited(work(1)).unwrap();
    assert_eq!(s.retire_work(work(1)), Err(ScopeError::ChildrenRemain));
    finish(&mut s, work(2));
    finish(&mut s, work(3));
    assert!(s.work_exited(work(1)).unwrap());
    s.retire_work(work(1)).unwrap();
    assert!(s.join_ready());
    assert_eq!(
        s.admit(
            generation(),
            work(4),
            WorkKind::Cleanup,
            WorkAdmission::Derived(work(1))
        ),
        Err(ScopeError::UnknownWork)
    );
}

#[test]
fn stopped_cleanup_bundle_cannot_be_rebound_as_business() {
    let mut s = scope(1);
    s.admit(
        generation(),
        work(1),
        WorkKind::Business,
        WorkAdmission::External,
    )
    .unwrap();
    s.request_stop();
    let mut ledger = CapacityLedger::new(vector(&[(1, 2)]));
    let mut r = request(2, &[(1, 1)]);
    r.admission = WorkAdmission::Derived(work(1));
    r.kind = WorkKind::Cleanup;
    let id = ledger.try_reserve_bundle(&mut s, r).unwrap();
    assert_eq!(
        ledger.bind(&mut s, id, WorkKind::Business),
        Err(CapacityError::Scope(ScopeError::InvalidTransition))
    );
    ledger.bind(&mut s, id, WorkKind::Cleanup).unwrap();
    s.start(work(2)).unwrap();
    assert!(!s.join_ready());
}

#[test]
fn retired_ids_do_not_resurrect_old_work_or_slots() {
    let mut s = scope(1);
    s.admit(
        generation(),
        work(1),
        WorkKind::Business,
        WorkAdmission::External,
    )
    .unwrap();
    finish(&mut s, work(1));
    assert_eq!(
        s.admit(
            generation(),
            work(1),
            WorkKind::Business,
            WorkAdmission::External
        ),
        Err(ScopeError::DuplicateWork)
    );
    let slot = SlotId::new(1).unwrap();
    s.reserve_slot(slot).unwrap();
    s.retire_slot(slot).unwrap();
    assert_eq!(s.reserve_slot(slot), Err(ScopeError::DuplicateSlot));
    assert!(Generation::new(u64::MAX).unwrap().next().is_none());
}

#[test]
fn failed_owner_retires_only_its_consumer_and_never_the_capture() {
    let sid = ScopeId::new(1).unwrap();
    let dead = ConsumerId::new(1).unwrap();
    let observer = ConsumerId::new(2).unwrap();
    let mut slot = CompletionSlot::new(
        SlotId::new(1).unwrap(),
        sid,
        generation(),
        work(1),
        2,
        [
            (
                dead,
                ConsumerKind::Owner {
                    scope: sid,
                    generation: generation(),
                },
            ),
            (observer, ConsumerKind::Observer),
        ],
    )
    .unwrap();
    slot.fill().unwrap();
    slot.observe_body_exit();
    slot.abandon_owner(dead, sid, generation()).unwrap();
    assert_eq!(
        slot.consumption(dead),
        Some(Consumption::AbandonedByOwnerFailure)
    );
    assert!(!slot.retirable());
    slot.consume(observer).unwrap();
    assert!(!slot.retirable());
    slot.observe_captures_exit();
    assert!(slot.retirable());
}

#[test]
fn work_join_does_not_claim_report_storage_retirement() {
    let mut s = scope(1);
    let slot = SlotId::new(1).unwrap();
    s.reserve_slot(slot).unwrap();
    s.close_external();
    assert!(s.join_ready());
    assert!(!s.retirement_ready());
    s.retire_slot(slot).unwrap();
    assert!(s.retirement_ready());
}

#[test]
fn revoked_unused_completion_still_waits_for_actual_exit() {
    let mut slot = CompletionSlot::new(
        SlotId::new(1).unwrap(),
        ScopeId::new(1).unwrap(),
        generation(),
        work(1),
        0,
        [],
    )
    .unwrap();
    slot.revoke_unused().unwrap();
    assert!(!slot.retirable());
    slot.observe_body_exit();
    assert!(!slot.retirable());
    slot.observe_captures_exit();
    assert!(slot.retirable());
}

#[test]
fn ready_claim_coalesces_wakes_and_preserves_wake_during_turn_release() {
    let mut q = ReadyQueue::new();
    q.register(actor(1)).unwrap();
    q.register(actor(2)).unwrap();
    for _ in 0..10 {
        q.notify(actor(1)).unwrap();
    }
    assert_eq!(q.ready_count(), 1);
    assert_eq!(q.claim(), Some(actor(1)));
    q.notify(actor(1)).unwrap();
    q.notify(actor(2)).unwrap();
    assert_eq!(q.claim(), Some(actor(2)));
    assert_eq!(q.claim(), None);
    q.release(actor(1), false).unwrap();
    q.release(actor(2), false).unwrap();
    assert_eq!(q.claim(), Some(actor(1)));
    q.release(actor(1), false).unwrap();
    assert_eq!(q.claim(), None);
    // Actor metadata follows actual registration rather than a global count.
    for id in 3..=6 {
        q.register(actor(id)).unwrap();
        q.notify(actor(id)).unwrap();
    }
    assert_eq!(q.actor_count(), 6);
    assert_eq!(q.register(actor(3)), Err(SchedulerError::DuplicateActor));
    for id in 3..=6 {
        assert_eq!(q.claim(), Some(actor(id)));
        q.release(actor(id), false).unwrap();
    }
    assert_eq!(q.claim(), None);
    // An independent metadata owner may cancel a queued middle address, but
    // ordinary retirement still rejects it until the explicit cancellation.
    for id in 3..=5 {
        q.notify(actor(id)).unwrap();
    }
    assert_eq!(q.unregister(actor(4)), Err(SchedulerError::Busy));
    q.cancel_pending(actor(4)).unwrap();
    q.unregister(actor(4)).unwrap();
    assert_eq!(q.ready_count(), 2);
    assert_eq!(q.actor_count(), 5);
    assert_eq!(q.claim(), Some(actor(3)));
    q.notify(actor(3)).unwrap();
    assert_eq!(
        q.cancel_pending(actor(3)),
        Err(SchedulerError::AlreadyClaimed)
    );
    q.release(actor(3), false).unwrap();
    assert_eq!(q.claim(), Some(actor(5)));
    q.release(actor(5), false).unwrap();
    assert_eq!(q.claim(), Some(actor(3)));
    q.release(actor(3), false).unwrap();
    assert_eq!(q.claim(), None);
    assert_eq!(
        q.cancel_pending(actor(9)),
        Err(SchedulerError::UnknownActor)
    );
    q.register(actor(4)).unwrap();
    q.notify(actor(4)).unwrap();
    q.cancel_pending(actor(4)).unwrap();
    assert_eq!(q.claim(), None);
    q.notify(actor(4)).unwrap();
    assert_eq!(q.claim(), Some(actor(4)));
    q.release(actor(4), false).unwrap();
    for id in 1..=6 {
        q.unregister(actor(id)).unwrap();
    }
    assert_eq!(q.actor_count(), 0);
}

#[test]
fn classified_inputs_do_not_overtake_a_same_source_predecessor() {
    let mut m = MailboxState::new([2, 2, 2, 1]);
    m.push(InputClass::Ordinary, actor(1), input(1)).unwrap();
    m.push(InputClass::Control, actor(1), input(2)).unwrap();
    m.push(InputClass::Control, actor(2), input(3)).unwrap();
    assert_eq!(m.pop(InputClass::Control).unwrap().input, input(3));
    assert!(!m.has_ready(InputClass::Control));
    assert_eq!(m.pop(InputClass::Ordinary).unwrap().input, input(1));
    assert_eq!(m.pop(InputClass::Control).unwrap().input, input(2));
    m.push(InputClass::Timer, actor(1), input(4)).unwrap();
    assert_eq!(
        m.push(InputClass::Timer, actor(2), input(5)),
        Err(MailboxError::Full(InputClass::Timer))
    );
}

#[test]
fn turn_guarantees_every_class_before_round_robin_excess() {
    let mut b = TurnBudget::new(8).unwrap();
    let actual = (0..8)
        .map(|_| b.next([true; 4]).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        [
            InputClass::Lifecycle,
            InputClass::Control,
            InputClass::Ordinary,
            InputClass::Timer,
            InputClass::Lifecycle,
            InputClass::Control,
            InputClass::Ordinary,
            InputClass::Timer
        ]
    );
    assert_eq!(b.next([true; 4]), None);
}

#[test]
fn preparation_is_fifo_with_one_body_per_group_and_group_fairness() {
    let a = ScopeId::new(1).unwrap();
    let b = ScopeId::new(2).unwrap();
    let mut q = FairWorkQueue::new(4, 3);
    q.enqueue(a, work(1)).unwrap();
    q.enqueue(a, work(2)).unwrap();
    q.enqueue(b, work(3)).unwrap();
    assert_eq!(q.pop(), Some((a, work(1))));
    assert_eq!(q.pop(), Some((b, work(3))));
    assert_eq!(q.pop(), None);
    q.enqueue(b, work(4)).unwrap();
    q.finish(a, work(1)).unwrap();
    q.finish(b, work(3)).unwrap();
    assert_eq!(q.pop(), Some((a, work(2))));
    assert_eq!(q.pop(), Some((b, work(4))));
    q.finish(a, work(2)).unwrap();
    q.finish(b, work(4)).unwrap();
    assert!(q.is_empty());
}

#[test]
fn indexed_queued_cancellation_preserves_remaining_fifo() {
    let s = ScopeId::new(1).unwrap();
    let mut q = FairWorkQueue::new(4, 4);
    for id in 1..=4 {
        q.enqueue(s, work(id)).unwrap();
    }
    q.remove_queued(s, work(2)).unwrap();
    q.remove_queued(s, work(4)).unwrap();
    assert_eq!(q.pop(), Some((s, work(1))));
    q.finish(s, work(1)).unwrap();
    assert_eq!(q.pop(), Some((s, work(3))));
    q.finish(s, work(3)).unwrap();
    assert!(q.is_empty());
}

#[test]
fn mutable_timer_nodes_and_pending_ticks_are_coalesced_and_budgeted() {
    let mut t = TimerIndex::new();
    for deadline in 1..=100 {
        t.set(actor(1), Tick(deadline)).unwrap();
    }
    assert_eq!(t.deadline_count(), 1);
    t.set(actor(2), Tick(100)).unwrap();
    assert_eq!(t.advance(Tick(99), 1).unwrap(), []);
    assert_eq!(t.advance(Tick(100), 1).unwrap(), [actor(1)]);
    assert_eq!(t.advance(Tick(100), 1).unwrap(), [actor(2)]);
    t.set(actor(1), Tick(100)).unwrap();
    assert_eq!(t.advance(Tick(100), 1).unwrap(), []);
    assert_eq!(t.pending_count(), 2);
    assert!(t.acknowledge_tick(actor(1)));
    assert_eq!(t.advance(Tick(99), 1), Err(TimerError::TimeWentBackwards));
    // More distinct actors add one node each; per-turn work and a pending
    // Tick remain bounded independently of that metadata population.
    for id in 3..=6 {
        t.set(actor(id), Tick(101)).unwrap();
    }
    assert_eq!(t.advance(Tick(101), 2).unwrap(), [actor(1), actor(3)]);
    assert_eq!(t.advance(Tick(101), 2).unwrap(), [actor(4), actor(5)]);
    assert_eq!(t.advance(Tick(101), 2).unwrap(), [actor(6)]);
    assert_eq!(t.pending_count(), 6);
    assert_eq!(t.advance(Tick(101), 2).unwrap(), []);
    for id in 1..=6 {
        assert!(t.acknowledge_tick(actor(id)));
    }
    assert_eq!(t.pending_count(), 0);
    assert_eq!(t.deadline_count(), 0);
}

#[test]
fn effects_wait_for_commit_and_publication_and_started_effects_survive_fencing() {
    let sid = ScopeId::new(1).unwrap();
    let a = ActionId::new(1).unwrap();
    let b = ActionId::new(2).unwrap();
    let mut batch = EffectBatchState::new(sid, generation(), input(1), 3, 1);
    batch
        .push(EffectDescriptor {
            id: a,
            work: ScopedWorkRef {
                scope: ScopeId::new(1).unwrap(),
                work: work(1),
            },
            kind: EffectKind::Publication,
            dependencies: vec![],
        })
        .unwrap();
    batch
        .push(EffectDescriptor {
            id: b,
            work: ScopedWorkRef {
                scope: sid,
                work: work(2),
            },
            kind: EffectKind::Settlement,
            dependencies: vec![a],
        })
        .unwrap();
    assert!(batch.start(a, generation()).is_err());
    batch.commit().unwrap();
    assert_eq!(batch.poll_ready(2, generation()).unwrap(), [a]);
    batch.start(a, generation()).unwrap();
    batch.published(a).unwrap();
    let newer = generation().next().unwrap();
    assert!(batch.abandon_unstarted(a, newer).is_err());
    assert!(!batch.ready(b, newer).unwrap());
    batch.dependency_published(b, a).unwrap();
    batch.start(b, newer).unwrap();
    batch.complete(a).unwrap();
    batch.complete(b).unwrap();
    assert!(batch.all_terminal());
}

#[test]
fn pending_business_effect_cannot_start_under_a_failed_generation() {
    let mut batch = EffectBatchState::new(ScopeId::new(1).unwrap(), generation(), input(1), 1, 0);
    let id = ActionId::new(1).unwrap();
    batch
        .push(EffectDescriptor {
            id,
            work: ScopedWorkRef {
                scope: ScopeId::new(1).unwrap(),
                work: work(1),
            },
            kind: EffectKind::Business,
            dependencies: vec![],
        })
        .unwrap();
    batch.commit().unwrap();
    let newer = generation().next().unwrap();
    assert!(!batch.ready(id, newer).unwrap());
    batch.abandon_unstarted(id, newer).unwrap();
    assert_eq!(
        batch.record(id).unwrap().state(),
        EffectState::AbandonedByOwnerFailure
    );
}

#[test]
fn history_is_bounded_and_preserves_complete_order_without_affecting_state() {
    let target = |n| novarocks_control_runtime_core::history::HistoryActorTarget {
        actor: actor(n),
        attachment: SlotId::new(n).unwrap(),
        scope_origin: ScopeId::new(1).unwrap(),
    };
    let mut h = HistoryBuffer::new(1);
    h.record(Tick(1), HistoryFact::Claimed { target: target(1) })
        .unwrap();
    assert_eq!(
        h.record(Tick(2), HistoryFact::Claimed { target: target(2) }),
        Err(HistoryError::Full)
    );
    assert_eq!(h.pop().unwrap().ordinal, 1);
    h.record(Tick(2), HistoryFact::Claimed { target: target(2) })
        .unwrap();
    assert_eq!(h.pop().unwrap().ordinal, 2);
}

#[test]
fn effect_work_cannot_spend_other_classes_guaranteed_units() {
    let mut budget = TurnBudget::new(8).unwrap();
    assert_eq!(budget.next([true; 4]), Some(InputClass::Lifecycle));
    assert!(budget.charge(4));
    assert!(!budget.charge(1));
    assert_eq!(budget.next([true; 4]), Some(InputClass::Control));
    assert_eq!(budget.next([true; 4]), Some(InputClass::Ordinary));
    assert_eq!(budget.next([true; 4]), Some(InputClass::Timer));
    assert_eq!(budget.remaining(), 0);
}

#[test]
fn overflow_rejection_keeps_all_previous_units_and_scope_work_unchanged() {
    let mut ledger = CapacityLedger::new(vector(&[(1, u64::MAX), (2, 2)]));
    let mut a = scope(1);
    ledger
        .try_reserve_bundle(&mut a, request(1, &[(1, u64::MAX), (2, 1)]))
        .unwrap();
    let mut b = scope(2);
    assert_eq!(
        ledger.try_reserve_bundle(&mut b, request(2, &[(1, 1), (2, 1)])),
        Err(CapacityError::Overflow)
    );
    assert_eq!(ledger.used(dimension(1)), u64::MAX);
    assert_eq!(ledger.used(dimension(2)), 1);
    assert_eq!(b.work_count(), 0);
}

#[test]
fn owner_failure_fences_new_business_and_preserves_existing_work_until_exit() {
    let mut s = scope(1);
    s.admit(
        generation(),
        work(1),
        WorkKind::Business,
        WorkAdmission::External,
    )
    .unwrap();
    s.start(work(1)).unwrap();
    let failed_generation = s.fail().unwrap();
    assert_eq!(failed_generation, Generation::new(2).unwrap());
    assert_eq!(s.fail().unwrap(), failed_generation);
    assert_eq!(
        s.admit(
            generation(),
            work(2),
            WorkKind::Business,
            WorkAdmission::External
        ),
        Err(ScopeError::GenerationMismatch)
    );
    s.admit(
        failed_generation,
        work(2),
        WorkKind::Cleanup,
        WorkAdmission::Derived(work(1)),
    )
    .unwrap();
    s.body_exited(work(1)).unwrap();
    assert!(!s.join_ready());
    finish(&mut s, work(2));
    assert!(!s.join_ready());
    s.captures_exited(work(1)).unwrap();
    s.retire_work(work(1)).unwrap();
    assert!(s.join_ready());
}

#[test]
fn newer_deadline_due_during_pending_tick_is_rearmed_after_ack() {
    let mut timers = TimerIndex::new();
    timers.set(actor(1), Tick(10)).unwrap();
    assert_eq!(timers.advance(Tick(10), 1).unwrap(), [actor(1)]);
    timers.set(actor(1), Tick(20)).unwrap();
    assert_eq!(timers.advance(Tick(20), 1).unwrap(), []);
    assert_eq!(timers.deadline_count(), 1);
    assert_eq!(timers.pending_count(), 1);
    assert_eq!(timers.next_deadline(), None);
    assert!(timers.acknowledge_tick(actor(1)));
    assert_eq!(timers.next_deadline(), Some(Tick(20)));
    assert_eq!(timers.advance(Tick(20), 1).unwrap(), [actor(1)]);
    assert_eq!(timers.deadline_count(), 0);
    assert!(timers.acknowledge_tick(actor(1)));
    assert_eq!(timers.advance(Tick(20), 1).unwrap(), []);
}

#[test]
fn deferred_due_timer_does_not_block_other_actors_and_latest_update_wins() {
    let mut timers = TimerIndex::new();
    timers.set(actor(1), Tick(1)).unwrap();
    assert_eq!(timers.advance(Tick(1), 1).unwrap(), [actor(1)]);
    timers.set(actor(1), Tick(2)).unwrap();
    timers.set(actor(2), Tick(3)).unwrap();
    assert_eq!(timers.advance(Tick(3), 2).unwrap(), [actor(2)]);
    assert_eq!(timers.deadline_count(), 1);
    timers.set(actor(1), Tick(10)).unwrap();
    assert!(timers.acknowledge_tick(actor(1)));
    assert_eq!(timers.next_deadline(), Some(Tick(10)));
    assert_eq!(timers.advance(Tick(9), 2).unwrap(), []);
    assert_eq!(timers.advance(Tick(10), 2).unwrap(), [actor(1)]);
}

#[test]
fn canceling_deferred_due_deadline_preserves_only_the_existing_tick() {
    let mut timers = TimerIndex::new();
    timers.set(actor(1), Tick(1)).unwrap();
    assert_eq!(timers.advance(Tick(1), 1).unwrap(), [actor(1)]);
    timers.set(actor(1), Tick(2)).unwrap();
    assert_eq!(timers.advance(Tick(2), 1).unwrap(), []);
    timers.cancel(actor(1));
    assert_eq!(timers.deadline_count(), 0);
    assert_eq!(timers.pending_count(), 1);
    assert!(timers.acknowledge_tick(actor(1)));
    assert_eq!(timers.advance(Tick(2), 1).unwrap(), []);
    timers.set(actor(2), Tick(3)).unwrap();
    assert_eq!(timers.advance(Tick(3), 1).unwrap(), [actor(2)]);
}

#[test]
fn reservation_owner_index_follows_split_transfer_rollback_and_final_release() {
    let mut ledger = CapacityLedger::new(vector(&[(1, 8)]));
    let mut a = scope(1);
    let mut b = scope(2);
    let id = ledger
        .try_reserve_bundle(&mut a, request(1, &[(1, 4)]))
        .unwrap();
    assert_eq!(ledger.reservation_count_for(a.id()), 1);
    ledger.bind(&mut a, id, WorkKind::Business).unwrap();
    let split = ReservationId::new(2).unwrap();
    ledger
        .split(id, split, &quota_partition(&[(1, 1)]))
        .unwrap();
    assert_eq!(ledger.reservation_count_for(a.id()), 2);
    b.admit(
        generation(),
        work(1),
        WorkKind::Business,
        WorkAdmission::External,
    )
    .unwrap();
    ledger.transfer(split, &b, work(1)).unwrap();
    assert_eq!(ledger.reservation_count_for(a.id()), 1);
    assert_eq!(ledger.reservation_count_for(b.id()), 1);
    ledger.release_quota(id, &vector(&[(1, 2)])).unwrap();
    assert_eq!(ledger.reservation_count_for(a.id()), 1);
    ledger.release_quota(id, &vector(&[(1, 1)])).unwrap();
    assert_eq!(ledger.reservation_count_for(a.id()), 1);
    finish(&mut a, work(1));
    ledger.finish(&a, id).unwrap();
    assert_eq!(ledger.reservation_count_for(a.id()), 0);
    ledger.release_quota(split, &vector(&[(1, 1)])).unwrap();
    finish(&mut b, work(1));
    ledger.finish(&b, split).unwrap();
    assert_eq!(ledger.reservation_count_for(b.id()), 0);
    let unbound = ledger
        .try_reserve_bundle(&mut a, request(3, &[(1, 1)]))
        .unwrap();
    ledger.rollback_unbound(&mut a, unbound).unwrap();
    assert_eq!(ledger.reservation_count_for(a.id()), 0);
    assert_eq!(ledger.reservation_count(), 0);
}

#[test]
fn independent_scope_work_ordinals_never_collide_or_cancel_each_other() {
    let a = ScopeId::new(1).unwrap();
    let b = ScopeId::new(2).unwrap();
    let mut queue = FairWorkQueue::new(4, 2);
    for scope in [a, b] {
        queue.enqueue(scope, work(1)).unwrap();
        queue.enqueue(scope, work(2)).unwrap();
    }
    assert_eq!(queue.len(), 4);
    assert_eq!(queue.pop(), Some((a, work(1))));
    assert_eq!(queue.pop(), Some((b, work(1))));
    queue.remove_queued(a, work(2)).unwrap();
    assert_eq!(queue.len(), 3);
    queue.finish(a, work(1)).unwrap();
    assert_eq!(queue.running(b), Some(work(1)));
    assert_eq!(
        queue.pop(),
        None,
        "the other context still owns its body slot"
    );
    queue.finish(b, work(1)).unwrap();
    assert_eq!(queue.pop(), Some((b, work(2))));
    queue.finish(b, work(2)).unwrap();
    assert!(queue.is_empty());
}

#[test]
fn indexed_owner_usage_follows_split_partial_release_and_cross_scope_transfer() {
    let mut ledger =
        CapacityLedger::with_owner_usage_dimensions(vector(&[(1, 8), (2, 100)]), [dimension(1)])
            .unwrap();
    assert_eq!(ledger.owner_usage_metadata_entries(dimension(1)), Some(1));
    assert_eq!(ledger.max_owner_usage(dimension(2)), None);
    let mut a = scope(1);
    let mut b = scope(2);
    let first = ledger
        .try_reserve_bundle(&mut a, request(1, &[(1, 4), (2, 20)]))
        .unwrap();
    ledger.bind(&mut a, first, WorkKind::Business).unwrap();
    assert_eq!(ledger.owner_usage_metadata_entries(dimension(1)), Some(3));
    let second = ledger
        .try_reserve_bundle(&mut b, request(2, &[(1, 2), (2, 10)]))
        .unwrap();
    ledger.bind(&mut b, second, WorkKind::Business).unwrap();
    assert_eq!(ledger.owner_usage_metadata_entries(dimension(1)), Some(5));
    assert_eq!(
        (
            ledger.used(dimension(1)),
            ledger.max_owner_usage(dimension(1))
        ),
        (6, Some(4))
    );
    let split = ReservationId::new(3).unwrap();
    ledger
        .split(first, split, &quota_partition(&[(1, 1), (2, 5)]))
        .unwrap();
    assert_eq!(ledger.max_owner_usage(dimension(1)), Some(4));
    assert_eq!(ledger.owner_usage_metadata_entries(dimension(1)), Some(5));
    b.admit(
        generation(),
        work(3),
        WorkKind::Cleanup,
        WorkAdmission::External,
    )
    .unwrap();
    ledger.transfer(split, &b, work(3)).unwrap();
    assert_eq!(ledger.owner_usage_metadata_entries(dimension(1)), Some(4));
    assert_eq!(
        (
            ledger.used(dimension(1)),
            ledger.max_owner_usage(dimension(1))
        ),
        (6, Some(3))
    );
    ledger.release_quota(first, &vector(&[(1, 2)])).unwrap();
    assert_eq!(ledger.owner_usage_metadata_entries(dimension(1)), Some(5));
    assert_eq!(ledger.max_owner_usage(dimension(1)), Some(3));
    ledger
        .release_quota(second, &vector(&[(1, 2), (2, 10)]))
        .unwrap();
    assert_eq!(ledger.owner_usage_metadata_entries(dimension(1)), Some(4));
    finish(&mut b, work(2));
    ledger.finish(&b, second).unwrap();
    assert_eq!(
        (
            ledger.used(dimension(1)),
            ledger.max_owner_usage(dimension(1))
        ),
        (2, Some(1))
    );
    ledger
        .release_quota(first, &vector(&[(1, 1), (2, 15)]))
        .unwrap();
    assert_eq!(ledger.owner_usage_metadata_entries(dimension(1)), Some(3));
    finish(&mut a, work(1));
    ledger.finish(&a, first).unwrap();
    ledger
        .release_quota(split, &vector(&[(1, 1), (2, 5)]))
        .unwrap();
    finish(&mut b, work(3));
    ledger.finish(&b, split).unwrap();
    assert_eq!(ledger.max_owner_usage(dimension(1)), Some(0));
    assert_eq!(ledger.reservation_count(), 0);
    assert_eq!(ledger.owner_usage_metadata_entries(dimension(1)), Some(1));
}

#[test]
fn indexed_owner_usage_survives_refused_transactions_and_real_unbound_rollback() {
    let mut ledger =
        CapacityLedger::with_owner_usage_dimensions(vector(&[(1, 2), (2, 10)]), [dimension(1)])
            .unwrap();
    let mut a = scope(1);
    let mut b = scope(2);
    let original = ledger
        .try_reserve_bundle(&mut a, request(1, &[(1, 1), (2, 8)]))
        .unwrap();
    assert_eq!(
        ledger.try_reserve_bundle(&mut b, request(2, &[(1, 1), (2, 3)])),
        Err(CapacityError::Exhausted(dimension(2)))
    );
    assert_eq!(
        (
            ledger.reservation_count(),
            b.work_count(),
            ledger.max_owner_usage(dimension(1))
        ),
        (1, 0, Some(1))
    );
    ledger.rollback_unbound(&mut a, original).unwrap();
    assert_eq!(ledger.max_owner_usage(dimension(1)), Some(0));
    let original = ledger
        .try_reserve_bundle(&mut a, request(2, &[(1, 2)]))
        .unwrap();
    assert_eq!(
        ledger.bind(&mut b, original, WorkKind::Business),
        Err(CapacityError::WrongOwner)
    );
    ledger.bind(&mut a, original, WorkKind::Business).unwrap();
    assert!(
        ledger
            .split(
                original,
                ReservationId::new(3).unwrap(),
                &quota_partition(&[(1, 3)])
            )
            .is_err()
    );
    assert_eq!(
        ledger.transfer(original, &b, work(99)),
        Err(CapacityError::Scope(ScopeError::UnknownWork))
    );
    assert_eq!(
        (
            ledger.used(dimension(1)),
            ledger.max_owner_usage(dimension(1))
        ),
        (2, Some(2))
    );
    b.admit(
        generation(),
        work(1),
        WorkKind::Cleanup,
        WorkAdmission::External,
    )
    .unwrap();
    assert_eq!(
        ledger.transfer(original, &b, work(1)),
        Err(CapacityError::OwnedStorage(
            OwnedStorageError::BoundPosition
        ))
    );
    let moved = ReservationId::new(3).unwrap();
    ledger
        .split(original, moved, &quota_partition(&[(1, 2)]))
        .unwrap();
    ledger.transfer(moved, &b, work(1)).unwrap();
    ledger.release_quota(moved, &vector(&[(1, 1)])).unwrap();
    let last = ledger
        .try_reserve_bundle(&mut a, request(4, &[(1, 1)]))
        .unwrap();
    ledger.bind(&mut a, last, WorkKind::Business).unwrap();
    assert_eq!(
        (
            ledger.used(dimension(1)),
            ledger.max_owner_usage(dimension(1))
        ),
        (2, Some(1))
    );
    ledger.release_quota(moved, &vector(&[(1, 1)])).unwrap();
    finish(&mut b, work(1));
    ledger.finish(&b, moved).unwrap();
    finish(&mut a, work(2));
    ledger.finish(&a, original).unwrap();
    ledger.release_quota(last, &vector(&[(1, 1)])).unwrap();
    finish(&mut a, work(4));
    ledger.finish(&a, last).unwrap();
    assert_eq!(
        (
            ledger.reservation_count(),
            ledger.max_owner_usage(dimension(1))
        ),
        (0, Some(0))
    );
}

#[test]
fn keyed_families_preflight_process_first_and_retire_only_after_last_charge() {
    use novarocks_control_runtime_core::capacity::KeyedQuota;
    let family = FamilyId::new(1).unwrap();
    let key = CapacityKey(7);
    let keyed = |amount| KeyedQuota {
        family,
        key,
        quota: vector(&[(2, amount)]),
    };
    let mut ledger =
        CapacityLedger::with_keyed_families(vector(&[(1, 2)]), [(family, vector(&[(2, 3)]))])
            .unwrap();
    let mut owner = scope(1);
    let mut first = request(1, &[(1, 1)]);
    first.resources = first.resources.with_keyed(keyed(2));
    ledger.try_reserve_bundle(&mut owner, first).unwrap();
    let mut rejected = request(2, &[(1, 2)]);
    rejected.resources = rejected.resources.with_keyed(keyed(2));
    assert_eq!(
        ledger.try_reserve_bundle(&mut owner, rejected),
        Err(CapacityError::Exhausted(dimension(1)))
    );
    let mut rejected = request(2, &[(1, 1)]);
    rejected.resources = rejected.resources.with_keyed(keyed(2));
    assert_eq!(
        ledger.try_reserve_bundle(&mut owner, rejected),
        Err(CapacityError::ExhaustedKeyed {
            family,
            key,
            dimension: dimension(2)
        })
    );
    assert_eq!(ledger.used(dimension(1)), 1);
    assert_eq!(owner.work_count(), 1);
    let id = ReservationId::new(1).unwrap();
    ledger.bind(&mut owner, id, WorkKind::Business).unwrap();
    ledger
        .release_quota_with_keyed(id, &vector(&[]), Some(&keyed(1)))
        .unwrap();
    assert_eq!(ledger.keyed_usage_snapshot()[0].used.get(dimension(2)), 1);
    let wrong = KeyedQuota {
        key: CapacityKey(8),
        ..keyed(1)
    };
    assert_eq!(
        ledger.release_quota_with_keyed(id, &vector(&[(1, 1)]), Some(&wrong)),
        Err(CapacityError::WrongKey)
    );
    assert_eq!(
        ledger.used(dimension(1)),
        1,
        "invalid keyed release must not refund process quota"
    );
    ledger
        .release_quota_with_keyed(id, &vector(&[(1, 1)]), Some(&keyed(1)))
        .unwrap();
    assert!(ledger.keyed_usage_snapshot().is_empty());
    finish(&mut owner, work(1));
    ledger.finish(&owner, id).unwrap();
}

#[test]
fn keyed_split_transfer_reserve_from_and_rollback_preserve_original_key() {
    use novarocks_control_runtime_core::capacity::KeyedQuota;
    let family = FamilyId::new(1).unwrap();
    let key = CapacityKey(99);
    let keyed = |amount| KeyedQuota {
        family,
        key,
        quota: vector(&[(2, amount)]),
    };
    let mut ledger =
        CapacityLedger::with_keyed_families(vector(&[(1, 8)]), [(family, vector(&[(2, 8)]))])
            .unwrap();
    let mut source = scope(1);
    let mut first = request(1, &[(1, 4)]);
    first.resources.owned_slots.work_headers = 2;
    first.resources = first.resources.with_keyed(keyed(4));
    ledger.try_reserve_bundle(&mut source, first).unwrap();
    let original = ReservationId::new(1).unwrap();
    ledger
        .bind(&mut source, original, WorkKind::Business)
        .unwrap();
    let partition =
        ReservationResources::new(vector(&[(1, 1)]), OwnedSlotNeed::default()).with_keyed(keyed(1));
    let split = ReservationId::new(2).unwrap();
    ledger.split(original, split, &partition).unwrap();
    let mut target = scope(2);
    ledger
        .try_reserve_bundle(&mut target, request(3, &[]))
        .unwrap();
    ledger
        .bind(
            &mut target,
            ReservationId::new(3).unwrap(),
            WorkKind::Business,
        )
        .unwrap();
    ledger.transfer(split, &target, work(3)).unwrap();
    assert_eq!(
        ledger
            .reservation(split)
            .unwrap()
            .keyed
            .as_ref()
            .unwrap()
            .key,
        key
    );
    let mut child = request(4, &[(1, 1)]);
    child.admission = WorkAdmission::Derived(work(1));
    child.resources = child.resources.with_keyed(keyed(1));
    ledger.reserve_from(&mut source, original, child).unwrap();
    assert_eq!(
        ledger.keyed_dimension_usage(family, key, dimension(2)),
        Some((4, 8))
    );
    ledger
        .rollback_unbound(&mut source, ReservationId::new(4).unwrap())
        .unwrap();
    assert_eq!(
        ledger.keyed_dimension_usage(family, key, dimension(2)),
        Some((3, 8))
    );
    assert_eq!(
        ledger
            .reservation(original)
            .unwrap()
            .keyed
            .as_ref()
            .unwrap()
            .quota
            .get(dimension(2)),
        2
    );
    ledger
        .release_quota_with_keyed(split, &partition.quota, partition.keyed.as_ref())
        .unwrap();
    ledger.finish(&target, split).unwrap();
    let remainder = ledger.reservation(original).unwrap().resources();
    ledger
        .release_quota_with_keyed(original, &remainder.quota, remainder.keyed.as_ref())
        .unwrap();
    finish(&mut source, work(1));
    ledger.finish(&source, original).unwrap();
    finish(&mut target, work(3));
    ledger
        .finish(&target, ReservationId::new(3).unwrap())
        .unwrap();
    assert!(ledger.keyed_usage_snapshot().is_empty());
}
