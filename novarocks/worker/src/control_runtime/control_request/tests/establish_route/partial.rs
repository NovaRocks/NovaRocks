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

//! Paid metadata fixtures exercise actual partial route retirement. They do
//! not stand in for Native materialization or a credential comparison source.

use super::*;
use crate::control_runtime::context_io::CONTEXT_IO_CALLBACKS;
use crate::control_runtime::establish_route::{CONTEXT_ROUTE_ESCROW_STORAGE, EstablishRouteError};
use novarocks_control_runtime::{
    lease::{CapacityBundle, WorkLease, WorkRetirementObserver},
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::scope::{WorkAdmission, WorkKind};

struct PaidMetadata {
    scope: ScopeHandle,
    work: WorkLease,
    capacity: CapacityBundle,
    retirement: WorkRetirementObserver,
}
impl PaidMetadata {
    fn admit(f: &mut RouteFixture, need: StorageNeed) -> Self {
        let context = f.f.context;
        let original_address = f.f.root.service().context(context).unwrap();
        let mut original_context = original_address.lock().unwrap();
        let resources = original_context.resources_for_test();
        let scope = resources.scope.as_ref().unwrap().clone();
        let parent = resources.roles[0].work.id();
        let (mut work, capacity) = scope
            .reserve_next(
                scope.gates().unwrap().generation,
                need.resources().unwrap(),
                WorkKind::Settlement,
                WorkAdmission::Derived(parent),
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        let retirement = work.retirement_observer();
        Self {
            scope,
            work,
            capacity,
            retirement,
        }
    }
    fn exit(&mut self) {
        // This owner has finished its pure metadata body and released every
        // input capture before observing these two actual exits.
        self.work.body_exited().unwrap();
        self.work.captures_exited().unwrap();
    }
    fn release(mut self) {
        assert!(
            self.retirement.is_retired(),
            "the exact paid metadata header must retire before refund"
        );
        self.capacity
            .release(&self.capacity.resources().unwrap())
            .unwrap();
    }
}

fn header_storage() -> StorageNeed {
    StorageNeed {
        work: 1,
        captures: 1,
        ..StorageNeed::default()
    }
}

#[test]
fn a_paid_full_context_input_returns_its_partial_report_and_survives_repeated_retirement() {
    // Context policy contributes two Control positions, the two actual Root R
    // positions contribute two more, and M/R have their independent positions.
    // Every position occupied by this fixture has its own actual payment.
    let control_positions = 4 + CONTEXT_IO_CALLBACKS;
    let occupier_need = StorageNeed {
        inputs: control_positions,
        batches: control_positions,
        effects: crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS * control_positions,
        ..header_storage()
    };
    let mut f = RouteFixture::with_geometry(
        2,
        occupier_need.checked_add(header_storage()).unwrap(),
        false,
    );
    let mut occupier = PaidMetadata::admit(&mut f, occupier_need);
    let mut backlog = PaidMetadata::admit(&mut f, header_storage());
    let scope = occupier.scope.clone();
    let baseline = scope.snapshot().unwrap();
    let mut positions = Vec::new();
    for _ in 0..control_positions {
        positions.push(
            f.f.runtime
                .reserve_admitted_input(
                    context_actor(),
                    context_actor(),
                    InputClass::Control,
                    &occupier.work,
                )
                .unwrap(),
        );
    }
    let all_control_positions_occupied =
        f.f.runtime.snapshot(context_actor()).unwrap().inputs == control_positions;
    backlog.exit();
    let RouteStarted {
        input,
        mut handle,
        getters,
        drops,
        refusal,
    } = f.start_route(op(11), 10, None);
    let input_capacity_refusal = matches!(
        refusal,
        Some(ControlRequestError::Route(EstablishRouteError::Driver(
            DriverError::InputCapacity
        )))
    );
    let partial_has_report = scope.snapshot().unwrap().storage == baseline.storage + 1;

    // The original Route effect already completed despite construction
    // refusal. Its real Root deadline can therefore dispatch Cancel/Timeout.
    let timer_count = f.f.runtime.advance_time(Tick(10), 2).unwrap();
    f.f.runtime.poll_one(Tick(10)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let cancel = f.f.root.take_control_effect(64).unwrap().unwrap();
    let timer_input = cancel.input;
    let cancelled_original = matches!(&cancel.effect,
        WorkerAction::Control { effect: ControlEffect::CancelEstablishRoute { request, .. }, .. }
        if request.token == input);
    f.f.root.handoff_establish_route_cancel(cancel).unwrap();
    let premature = f.f.root.take_retired_route_disposal(input);
    let source_not_yet_disposable = premature.is_none();
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    let timeout_reply = matches!(&reply.effect, WorkerAction::ControlReply(reply)
        if reply.request.token == input && reply.result == RequestResult::TimeoutWhileAccepted);
    f.f.root.publish_request_reply(reply).unwrap();
    let delivery = handle.try_take().unwrap();
    drop(delivery);
    drop(handle);
    f.f.runtime.retire_input(actor(), timer_input).unwrap();

    for position in positions {
        position.revoke_unused().unwrap();
    }
    occupier.exit();
    // Only the earlier occupier header is removed here. The next earlier
    // backlog header remains ready to consume the route's first reap budget.
    let occupier_headers = f.f.ledger.reap_scope_ready(scope.id(), 1).unwrap();
    occupier.release();
    let first =
        f.f.root
            .service()
            .context(f.f.context)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(input)
            .unwrap();
    let partial_report_retired = scope.snapshot().unwrap().storage == baseline.storage;
    let second =
        f.f.root
            .service()
            .context(f.f.context)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(input)
            .unwrap();
    backlog.release();
    let disposal = premature
        .or_else(|| f.f.root.take_retired_route_disposal(input))
        .expect("the cancelled original source after actual partial retirement");
    let source_exited = thread::spawn(move || disposal.run()).join().unwrap();
    let retired = f.f.root.reap_ready_requests(32).unwrap();
    let context_restored = scope.snapshot().unwrap().work == baseline.work - 2
        && scope.snapshot().unwrap().storage == baseline.storage;
    f.finish_model_refusal();

    assert!(all_control_positions_occupied);
    assert!(input_capacity_refusal);
    assert!(partial_has_report);
    assert_eq!(timer_count, 1);
    assert!(cancelled_original);
    assert!(timeout_reply);
    assert!(source_not_yet_disposable);
    assert_eq!(occupier_headers, 1);
    assert!(!first);
    assert!(partial_report_retired);
    assert!(second);
    assert!(source_exited);
    assert_eq!(retired, 1);
    assert!(context_restored);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn a_route_keeps_every_escrow_unit_until_its_exact_context_header_retires() {
    let mut f = RouteFixture::with_geometry(2, header_storage(), false);
    let mut backlog = PaidMetadata::admit(&mut f, header_storage());
    let scope = backlog.scope.clone();
    let metadata_work_with_backlog = scope.snapshot().unwrap().work;
    backlog.exit();
    let (input, mut handle, getters, drops) = f.begin_route(op(12), 100, None);
    let result = f.probe(input);
    let escrow = CONTEXT_ROUTE_ESCROW_STORAGE.resources().unwrap();
    let before: Vec<_> = escrow
        .iter()
        .map(|(dimension, count)| (dimension, count, f.f.ledger.used(dimension)))
        .collect();
    let first =
        f.f.root
            .service()
            .context(f.f.context)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(input)
            .unwrap();
    let escrow_held = before
        .iter()
        .all(|(dimension, _, held)| f.f.ledger.used(*dimension) == *held);
    let second =
        f.f.root
            .service()
            .context(f.f.context)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(input)
            .unwrap();
    let exact_refund = before.iter().all(|(dimension, count, held)| {
        held.checked_sub(*count) == Some(f.f.ledger.used(*dimension))
    });
    let root_return_still_filled =
        f.f.runtime
            .input_inventory(actor(), None, 16)
            .unwrap()
            .iter()
            .filter(|record| {
                record.source == context_actor()
                    && record.phase == InputPhase::Filled
                    && record.role.is_some()
            })
            .count()
            == 1;
    let root_reply_not_consumed = handle.try_take().is_none();
    let root_held = f.f.root.reap_ready_requests(32).unwrap() == 0;
    backlog.release();
    let only_context_metadata = scope.snapshot().unwrap().work == metadata_work_with_backlog - 1;
    f.dispose_context_metadata();

    f.f.runtime.poll_one(Tick(4)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    let original_reply = matches!(&reply.effect, WorkerAction::ControlReply(reply)
        if reply.request.token == input && reply.result == RequestResult::Outcome(OperationOutcome::ContextConflict));
    let disposal =
        f.f.root
            .begin_rejected_ingress_disposal(&reply)
            .unwrap()
            .unwrap();
    f.f.root.publish_request_reply(reply).unwrap();
    let delivery = handle.try_take().unwrap();
    drop(delivery);
    drop(handle);
    let source_exited = thread::spawn(move || disposal.run()).join().unwrap();
    let retired = f.f.root.reap_ready_requests(32).unwrap();
    f.finish_model_refusal();

    assert!(matches!(
        result,
        EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ContextConflict
    ));
    assert!(!first);
    assert!(escrow_held);
    assert!(second);
    assert!(exact_refund);
    assert!(root_return_still_filled);
    assert!(root_reply_not_consumed);
    assert!(root_held);
    assert!(only_context_metadata);
    assert!(original_reply);
    assert!(source_exited);
    assert_eq!(retired, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}
