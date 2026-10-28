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

//! Real registered owners qualify these metadata addresses. The Control
//! namespace below uses a Context actor too; no Root lifecycle is simulated.
//! FIFO is local to each owner; Common schedules fairness between actors.

use super::*;
use crate::{
    control_admission::{AdmissionStorage, Dimension},
    control_runtime::actors::{ContextActor, tests::initial_establishment},
};
use novarocks_control_runtime::{
    actor::ActorConfig,
    driver::{ActorRegistration, DriverError},
    lease::{CapacityBundle, WorkLease},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, Generation},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::QueryContextRef;
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use novarocks_worker_protocol::{
    context::{ContextConfig, ContextCore},
    lease::LeaseBounds,
};
use std::{
    sync::Barrier,
    time::{Duration, Instant},
};

fn input(n: u64) -> InputId {
    InputId::new(n).unwrap()
}
struct Owner {
    scope: ScopeHandle,
    registration: ActorRegistration,
    roles: Vec<(WorkLease, CapacityBundle)>,
}
impl Owner {
    fn work(&self, index: usize) -> WorkId {
        self.roles[index].0.id()
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    runtime: ActorRuntime<WorkerActor>,
    owners: Vec<Owner>,
}
impl Fixture {
    fn new(count: usize) -> Self {
        let generation = Generation::new(1).unwrap();
        let ledger = RuntimeLedger::new(
            ResourceVector::new([(Dimension::Work.id(), u64::try_from(count * 3).unwrap())])
                .unwrap(),
            count * 3,
            count,
        );
        let runtime = ActorRuntime::new(count);
        let mut owners = Vec::with_capacity(count);
        for n in 0..count {
            let scope = ledger.open_scope_next(None, generation, 3, 1).unwrap();
            let mut roles = Vec::with_capacity(3);
            for kind in [WorkKind::Cleanup, WorkKind::Step, WorkKind::Step] {
                let (mut work, capacity) = scope
                    .reserve_next(
                        generation,
                        ResourceVector::new([(Dimension::Work.id(), 1)]).unwrap(),
                        kind,
                        WorkAdmission::External,
                    )
                    .unwrap()
                    .bind(kind)
                    .unwrap();
                if kind != WorkKind::Cleanup {
                    work.start().unwrap();
                }
                roles.push((work, capacity));
            }
            let identity = QueryContextRef::new(
                QueryExecutionId::new(QueryId::new(8101, 8102), AttemptId::new(1).unwrap())
                    .unwrap(),
                FrontendProcessId::new_v7(),
                BackendProcessId::new_v7(),
            );
            let core = ContextCore::new(
                identity,
                generation,
                initial_establishment(),
                ContextConfig {
                    max_tasks: 1,
                    max_creation_waiters: 2,
                    max_pending_requests: 2,
                    max_preparing: 1,
                    max_task_bytes: 16,
                    step_budget: ContextCore::MAX_STEP_WORK,
                    lease_bounds: LeaseBounds::DEFAULT,
                    metric_publish_interval: Duration::from_millis(100),
                },
            )
            .unwrap();
            let actor = ContextActor::new(
                core,
                scope.clone(),
                AdmissionStorage {
                    loading: ResourceVector::default(),
                    accepted: ResourceVector::default(),
                    context: ResourceVector::default(),
                    ticket: ResourceVector::default(),
                },
                roles[1].0.id(),
                roles[2].0.id(),
            );
            let registration = runtime
                .register_supervised_bound(
                    ActorId::new(u64::try_from(n + 1).unwrap()).unwrap(),
                    scope.clone(),
                    WorkerActor::Context(Box::new(actor)),
                    ActorConfig {
                        mailbox: [4; 4],
                        max_batches: 4,
                        max_effects_per_step: ContextCore::MAX_STEP_WORK,
                        max_dependencies_per_effect: ContextCore::MAX_STEP_WORK,
                        turn_budget: 64,
                        max_step_work: ContextCore::MAX_STEP_WORK,
                    },
                    &roles[0].0,
                )
                .unwrap_or_else(|(error, _core)| panic!("original Context registration: {error}"));
            owners.push(Owner {
                scope,
                registration,
                roles,
            });
        }
        Self {
            ledger,
            runtime,
            owners,
        }
    }
    fn index(&self, limit: usize) -> ControlRequestReady {
        ControlRequestReady::new(NonZeroUsize::new(limit).unwrap(), self.runtime.clone())
    }
    fn drain_host_marks(&self) {
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
    }
    fn finish(mut self) {
        self.drain_host_marks();
        for owner in &mut self.owners {
            self.runtime
                .close_actor_admission(owner.registration.actor())
                .unwrap();
            self.runtime
                .close_timer_source(owner.registration.actor())
                .unwrap();
            owner.scope.close_external().unwrap();
            for (work, _) in &mut owner.roles {
                work.body_exited().unwrap();
                work.captures_exited().unwrap();
            }
            self.ledger.reap_scope_ready(owner.scope.id(), 3).unwrap();
        }
        for mut owner in self.owners {
            drop(
                self.runtime
                    .remove_actor(owner.registration.actor())
                    .unwrap(),
            );
            for (_, capacity) in &mut owner.roles {
                capacity.release(&capacity.resources().unwrap()).unwrap();
            }
            self.ledger.remove_scope(owner.scope.id()).unwrap();
        }
        assert!(self.runtime.is_empty() && self.ledger.is_empty());
    }
}

#[test]
fn both_namespaces_share_one_limit_without_issuing_work_or_accepting_duplicate_slots() {
    let f = Fixture::new(1);
    let owner = &f.owners[0];
    let ready = f.index(2);
    let baseline = owner.scope.snapshot().unwrap();
    let paid = f.ledger.used(Dimension::Work.id());
    let control = ready
        .register_control(input(1), &owner.registration)
        .unwrap();
    let context = ready
        .register_context(&owner.scope, owner.work(1), &owner.registration)
        .unwrap();
    let distinct_namespaces = ready.registered() == 2;
    let duplicate_control = matches!(
        ready.register_control(input(1), &owner.registration),
        Err(ControlRequestError::DuplicateReadySlot)
    );
    let duplicate_context = matches!(
        ready.register_context(&owner.scope, owner.work(1), &owner.registration),
        Err(ControlRequestError::DuplicateReadySlot)
    );
    let full_control = matches!(
        ready.register_control(input(2), &owner.registration),
        Err(ControlRequestError::ReadyFull)
    );
    let full_context = matches!(
        ready.register_context(&owner.scope, owner.work(2), &owner.registration),
        Err(ControlRequestError::ReadyFull)
    );
    ready.unregister(&context).unwrap();
    let replacement = ready
        .register_control(input(2), &owner.registration)
        .unwrap();
    let still_bounded = ready.registered() == 2;
    let unchanged_inventory =
        owner.scope.snapshot().unwrap() == baseline && f.ledger.used(Dimension::Work.id()) == paid;
    let same_index = ready.same_owner(&ready.clone()) && ready.limit().get() == 2;
    replacement.notify();
    control.notify();
    let control_first = ready.next_ready() == Some(input(2));
    replacement.notify();
    let control_requeue_fifo = ready.next_ready() == Some(input(1))
        && ready.next_ready() == Some(input(2))
        && ready.next_ready().is_none();
    control.notify();
    ready.unregister(&control).unwrap();
    let queued_unregister_removed = ready.next_ready().is_none();
    ready.unregister(&replacement).unwrap();
    let emptied = ready.registered() == 0;
    f.finish();
    assert!(distinct_namespaces && duplicate_control && duplicate_context);
    assert!(full_control && full_context && still_bounded && unchanged_inventory);
    assert!(same_index && emptied);
    assert!(control_first && control_requeue_fifo && queued_unregister_removed);
}

#[test]
fn each_context_owner_keeps_ready_fifo_and_cannot_consume_another_owner() {
    let f = Fixture::new(2);
    let a = &f.owners[0];
    let b = &f.owners[1];
    let ready = f.index(5);
    let control = ready.register_control(input(7), &a.registration).unwrap();
    let a_slots = [
        ready
            .register_context(&a.scope, a.work(1), &a.registration)
            .unwrap(),
        ready
            .register_context(&a.scope, a.work(2), &a.registration)
            .unwrap(),
    ];
    let b_slots = [
        ready
            .register_context(&b.scope, b.work(1), &b.registration)
            .unwrap(),
        ready
            .register_context(&b.scope, b.work(2), &b.registration)
            .unwrap(),
    ];
    a_slots[1].notify();
    a_slots[1].notify();
    b_slots[1].notify();
    a_slots[0].notify();
    b_slots[0].notify();
    control.notify();
    let first_a = ready.next_context_ready(&a.registration);
    a_slots[1].notify();
    let rest_a = [
        ready.next_context_ready(&a.registration),
        ready.next_context_ready(&a.registration),
        ready.next_context_ready(&a.registration),
    ];
    let untouched_b = [
        ready.next_context_ready(&b.registration),
        ready.next_context_ready(&b.registration),
        ready.next_context_ready(&b.registration),
    ];
    let control_independent = ready.next_ready() == Some(input(7)) && ready.next_ready().is_none();
    for slot in a_slots
        .iter()
        .chain(b_slots.iter())
        .chain(std::iter::once(&control))
    {
        ready.unregister(slot).unwrap();
    }
    let correct_a =
        first_a == Some(a.work(2)) && rest_a == [Some(a.work(1)), Some(a.work(2)), None];
    let correct_b = untouched_b == [Some(b.work(2)), Some(b.work(1)), None];
    f.finish();
    assert!(correct_a && correct_b && control_independent);
}

#[test]
fn foreign_owners_and_old_slot_aliases_cannot_mutate_or_consume_the_original_queue() {
    let f = Fixture::new(1);
    let foreign_fixture = Fixture::new(1);
    let a = &f.owners[0];
    let foreign_owner = &foreign_fixture.owners[0];
    let ready = f.index(2);
    let foreign = foreign_fixture.index(2);
    let original = ready.register_control(input(1), &a.registration).unwrap();
    let other = foreign
        .register_control(input(1), &foreign_owner.registration)
        .unwrap();
    let foreign_control = matches!(
        ready.register_control(input(2), &foreign_owner.registration),
        Err(ControlRequestError::Driver(
            DriverError::EffectTicketOriginMismatch
        ))
    );
    let foreign_context = matches!(
        ready.register_context(&a.scope, a.work(1), &foreign_owner.registration),
        Err(ControlRequestError::Driver(
            DriverError::EffectTicketOriginMismatch
        ))
    );
    let foreign_scope = matches!(
        ready.register_context(&foreign_owner.scope, foreign_owner.work(1), &a.registration),
        Err(ControlRequestError::Driver(
            DriverError::EffectTicketOriginMismatch
        ))
    );
    let foreign_index = matches!(
        ready.unregister(&other),
        Err(ControlRequestError::UnknownReadySlot)
    );
    let belongs =
        original.belongs_to(&ready) && !other.belongs_to(&ready) && !ready.same_owner(&foreign);
    ready.unregister(&original).unwrap();
    let replacement = ready.register_control(input(1), &a.registration).unwrap();
    let observed = ready.change_version();
    original.notify();
    let old_notify_noop = ready.change_version() == observed && ready.next_ready().is_none();
    let old_remove_refused = matches!(
        ready.unregister(&original),
        Err(ControlRequestError::UnknownReadySlot)
    );
    let context = ready
        .register_context(&a.scope, a.work(1), &a.registration)
        .unwrap();
    context.notify();
    let same_numbers = a.scope.id() == foreign_owner.scope.id()
        && a.work(1) == foreign_owner.work(1)
        && a.registration.actor() == foreign_owner.registration.actor()
        && a.registration.attachment() == foreign_owner.registration.attachment();
    let foreign_pop_noop = ready
        .next_context_ready(&foreign_owner.registration)
        .is_none();
    let original_pop = ready.next_context_ready(&a.registration) == Some(a.work(1));
    other.notify();
    let foreign_queue_independent =
        ready.next_ready().is_none() && foreign.next_ready() == Some(input(1));
    replacement.notify();
    let replacement_works = ready.next_ready() == Some(input(1));
    ready.unregister(&replacement).unwrap();
    ready.unregister(&context).unwrap();
    let context_replacement = ready
        .register_context(&a.scope, a.work(1), &a.registration)
        .unwrap();
    let before_old_context_notify = ready.change_version();
    context.notify();
    let old_context_noop = ready.change_version() == before_old_context_notify
        && ready.next_context_ready(&a.registration).is_none();
    context_replacement.notify();
    let replacement_context_works = ready.next_context_ready(&a.registration) == Some(a.work(1));
    ready.unregister(&context_replacement).unwrap();
    foreign.unregister(&other).unwrap();
    f.finish();
    foreign_fixture.finish();
    assert!(foreign_control && foreign_context && foreign_scope && foreign_index && belongs);
    assert!(
        old_notify_noop && old_remove_refused && same_numbers && foreign_pop_noop && original_pop
    );
    assert!(foreign_queue_independent && replacement_works);
    assert!(old_context_noop && replacement_context_works);
}

#[test]
fn original_slot_notification_wakes_its_host_and_survives_a_racing_release() {
    let f = Fixture::new(2);
    let a = &f.owners[0];
    let b = &f.owners[1];
    let ready = f.index(2);
    let first = ready
        .register_context(&a.scope, a.work(1), &a.registration)
        .unwrap();
    let second = ready
        .register_context(&b.scope, b.work(1), &b.registration)
        .unwrap();
    first.notify();
    let turn = f.runtime.claim_host_actor().unwrap();
    let original_claim = turn.matches_registration(&a.registration);
    let first_address = ready.next_context_ready(&a.registration);
    let race = Arc::new(Barrier::new(2));
    let worker_race = race.clone();
    let worker_slot = first.clone();
    let worker_second = second.clone();
    let worker = std::thread::spawn(move || {
        worker_slot.notify();
        worker_second.notify();
        worker_race.wait();
        worker_slot.notify();
    });
    race.wait();
    turn.release(false).unwrap();
    let joined = worker.join().is_ok();
    let next = f.runtime.claim_host_actor().unwrap();
    let other_owner_progressed = next.matches_registration(&b.registration);
    next.release(false).unwrap();
    let again = f.runtime.claim_host_actor().unwrap();
    let original_mark_retained = again.matches_registration(&a.registration);
    again.release(false).unwrap();
    let coalesced_a = ready.next_context_ready(&a.registration) == Some(a.work(1))
        && ready.next_context_ready(&a.registration).is_none();
    let coalesced_b = ready.next_context_ready(&b.registration) == Some(b.work(1))
        && ready.next_context_ready(&b.registration).is_none();
    let original_address = first_address == Some(a.work(1));
    ready.unregister(&first).unwrap();
    ready.unregister(&second).unwrap();
    f.finish();
    assert!(original_claim && original_address && joined && other_owner_progressed);
    assert!(original_mark_retained && coalesced_a && coalesced_b);
}

#[test]
fn a_retained_weak_slot_cannot_keep_the_index_alive_or_wake_its_former_owner() {
    let f = Fixture::new(1);
    let a = &f.owners[0];
    let ready = f.index(1);
    let slot = ready
        .register_context(&a.scope, a.work(1), &a.registration)
        .unwrap();
    let actual_index = Arc::downgrade(&ready.shared);
    let baseline = a.scope.snapshot().unwrap();
    drop(ready);
    let index_gone = actual_index.upgrade().is_none();
    slot.notify();
    let no_owner_wake = match f.runtime.claim_host_actor() {
        Some(unexpected) => {
            unexpected.release(false).unwrap();
            false
        }
        None => true,
    };
    let unchanged_inventory = a.scope.snapshot().unwrap() == baseline;
    f.finish();
    assert!(index_gone && no_owner_wake && unchanged_inventory);
}

#[test]
fn external_end_publishes_its_phase_before_the_original_host_notice_and_is_idempotent() {
    let f = Fixture::new(1);
    let a = &f.owners[0];
    let ready = f.index(1);
    let slot = ready
        .register_context(&a.scope, a.work(1), &a.registration)
        .unwrap();
    let leg = Arc::new(RequestExternalLeg::new(slot.clone()));
    let reply = PublishedReply::<u8>::new(&a.scope);
    let waiting =
        leg.try_take(&reply).is_none() && *leg.phase.lock().unwrap() == ExternalPhase::Waiting;
    let original_slot = Arc::ptr_eq(leg.slot(), &slot);
    let observed = ready.change_version();
    // Hold the real ready index so end's actual notify cannot finish. The
    // phase must already be Ended and unlocked before that notification can
    // acquire the index; try_lock keeps the negative path releasable.
    let index_guard = ready.shared.state.lock().unwrap();
    let worker_leg = leg.clone();
    let worker = std::thread::spawn(move || worker_leg.end());
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut phase_unlocked_before_notification = false;
    while Instant::now() < deadline {
        if let Ok(phase) = leg.phase.try_lock()
            && *phase == ExternalPhase::Ended
        {
            phase_unlocked_before_notification = true;
            break;
        }
        std::thread::yield_now();
    }
    drop(index_guard);
    let joined = worker.join().is_ok();
    let turn = f.runtime.claim_host_actor().unwrap();
    let actual_host_notice = turn.matches_registration(&a.registration);
    let ended_before_notice = leg.is_ended() && leg.phase.try_lock().is_ok();
    turn.release(false).unwrap();
    let first_end_notified = ready.change_version() > observed;
    let ended = ready.change_version();
    leg.end();
    let second_end_noop = ready.change_version() == ended;
    let ready_once = ready.next_context_ready(&a.registration) == Some(a.work(1))
        && ready.next_context_ready(&a.registration).is_none();
    leg.notify();
    let explicit_progress_notice = ready.next_context_ready(&a.registration) == Some(a.work(1));
    let no_pending_reply = leg.try_take(&reply).is_none();
    ready.unregister(leg.slot()).unwrap();
    drop(leg);
    drop(reply);
    f.finish();
    assert!(waiting && original_slot && joined && actual_host_notice && ended_before_notice);
    assert!(phase_unlocked_before_notification);
    assert!(first_end_notified && second_end_noop && ready_once && explicit_progress_notice);
    assert!(no_pending_reply);
}
