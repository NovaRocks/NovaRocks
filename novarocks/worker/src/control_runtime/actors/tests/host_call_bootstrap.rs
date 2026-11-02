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

//! The elected Task retains its original host/source holders through actual
//! unused revocation. No getter, observer or execution callback is invoked.

use super::*;
use crate::control_runtime::{
    source_positions::SOURCE_SLOT_STORAGE,
    task::{TaskHostBinding, TaskHostRecord},
    task_host_call::{HOST_CALL_STORAGE, TaskHostCallKind},
};
use novarocks_control_runtime::exit_role::ExitRolePhase;
use std::sync::Arc;

#[test]
fn original_host_reports_retire_independently_before_the_same_task_returns_its_payment() {
    let mut f = Fixture::with_policy_byte_limits(100);
    let policy = f.policy.unwrap();
    let task = task(f.identity, 1);
    let original = f.scope.snapshot().unwrap();
    assert_eq!(original.storage, 1);
    assert_eq!(original.unresolved, 0);
    let create = f.send(
        ContextInput::Request(ContextCommand::Create {
            task,
            payload: PayloadId(1),
        }),
        Some(operation(1)),
        InputClass::Ordinary,
        2,
    );
    f.runtime.poll_one(Tick(1)).unwrap();
    let action = f.runtime.ready_effects(actor(), create, 15).unwrap()[0];
    let ticket = f.runtime.take_effect(actor(), create, action).unwrap();
    let reservation = match ticket.effect {
        WorkerAction::Context(ContextEffect::LoadInput { reservation, .. }) => reservation,
        _ => panic!("the original elected Task's actual LoadInput effect"),
    };
    let task_work = ticket.descriptor.work;
    let elected = f.scope.snapshot().unwrap();
    assert_eq!(elected.work, original.work + 1);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    let host = TaskHostRecord::adopt_loading(
        &f.runtime,
        &ticket,
        &TaskHostBinding {
            registration: f.registration.clone(),
            context: f.identity,
            task_source: ActorId::new(2).unwrap(),
            protocol_generation: generation(),
            max_cuts: 1,
            pending: Arc::new(CausalPending),
            health: Arc::new(
                novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
            ),
        },
        &f.roles[0].0,
    )
    .unwrap_or_else(|refusal| panic!("the original host bootstrap succeeds: {:?}", refusal.error));
    let mut resources = host.into_resources();
    assert_eq!(resources.lifetime.work().reference(), task_work);
    assert_eq!(resources.active.id(), reservation);
    assert!(resources.source_partials.is_empty());
    assert!(resources.loader.is_none());
    assert!(resources.loading_events.is_none());
    assert!(resources.loading_return.is_none());
    assert!(resources.pending_loading.is_none());
    assert!(resources.preparation.is_none());
    assert!(resources.preparation_remainder.is_none());
    assert!(resources.accepted_role_storage.is_none());
    assert!(resources.pending_preparation_adoption.is_none());
    assert!(resources.completion.is_none());
    assert!(resources.completion_action.is_none());
    assert!(resources.lifecycle_inputs.is_empty());
    assert!(resources.statistics.is_none());
    assert!(resources.dynamic_filters.is_none());
    assert!(resources.open_summary.is_none());
    assert!(resources.causal.is_empty());
    assert!(resources.production.is_none());
    for (call, kind) in resources
        .host_calls
        .iter()
        .zip([TaskHostCallKind::Stop, TaskHostCallKind::RetireBorrowing])
    {
        assert_eq!(call.kind(), kind);
        assert_eq!(call.phase(), ExitRolePhase::Admitted);
        assert_eq!(call.key().0, f.scope.id());
        assert!(!call.is_retired());
        assert!(call.poll_return().unwrap().is_none());
    }
    let sources = SOURCE_SLOT_STORAGE.checked_mul(4).unwrap();
    let host_fees = HOST_CALL_STORAGE.checked_mul(2).unwrap();
    let mut retained_active = policy
        .loading
        .checked_sub(sources)
        .unwrap()
        .checked_sub(host_fees)
        .unwrap()
        .checked_sub(crate::control_runtime::task_disposal::TASK_DISPOSAL_STORAGE)
        .unwrap()
        .resources()
        .unwrap()
        .checked_add(&ReservationResources::new(
            ResourceVector::new([(Dimension::ActiveTasks.id(), 1)]).unwrap(),
            Default::default(),
        ))
        .unwrap();
    retained_active.owned_slots = retained_active
        .owned_slots
        .checked_sub(novarocks_control_runtime_core::owned_storage::OwnedSlotNeed::work_only())
        .unwrap();
    assert_eq!(resources.active.resources().unwrap(), retained_active);
    assert_eq!(f.scope.snapshot().unwrap().storage, original.storage + 7);
    assert_eq!(f.scope.snapshot().unwrap().work, elected.work + 7);
    assert_eq!(f.scope.snapshot().unwrap().unresolved, 0);
    assert!(f.ledger.unresolved_work().is_empty());
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    let lifetime = resources.lifetime.retirement_observer();
    assert!(!lifetime.is_retired());
    assert!(!resources.lifetime.actual_exit_ready().unwrap());
    assert!(matches!(
        f.runtime.take_step_reservation(&ticket, reservation),
        Err(DriverError::UnknownReservation)
    ));
    f.runtime.effect_completed(ticket).unwrap();
    f.drain(create);

    // Each exact unexposed host role revokes its own report and waits for its
    // real Work reaper before that original role's payment is returned.
    for (index, call) in resources.host_calls.iter().enumerate() {
        let mut retired = false;
        for _ in 0..64 {
            if call.retire_unused(&f.ledger).unwrap() {
                retired = true;
                break;
            }
        }
        assert!(retired && call.is_retired());
        assert!(call.retire_unused(&f.ledger).unwrap());
        assert_eq!(call.phase(), ExitRolePhase::Retired);
        assert_eq!(resources.active.resources().unwrap(), retained_active);
        assert_eq!(
            f.scope.snapshot().unwrap().storage,
            original.storage + 6 - index
        );
        assert_eq!(f.scope.snapshot().unwrap().work, elected.work + 6 - index);
        assert!(!lifetime.is_retired());
        assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    }
    let mut disposal_retired = false;
    for _ in 0..64 {
        if resources.disposal.retire_unused(&f.ledger).unwrap() {
            disposal_retired = true;
            break;
        }
    }
    assert!(disposal_retired && resources.disposal.is_retired());
    assert_eq!(f.scope.snapshot().unwrap().storage, original.storage + 4);
    assert_eq!(f.scope.snapshot().unwrap().work, elected.work + 4);
    finish_source_routes(&f, resources.port.ingress());
    assert_eq!(f.scope.snapshot().unwrap().storage, original.storage);
    assert_eq!(f.scope.snapshot().unwrap().work, elected.work);
    assert_eq!(resources.active.resources().unwrap(), retained_active);
    assert!(!lifetime.is_retired());
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    drop(resources.port);
    resources.lifetime.body_exited().unwrap();
    assert!(!resources.lifetime.actual_exit_ready().unwrap());
    resources.lifetime.captures_exited().unwrap();
    assert!(resources.lifetime.actual_exit_ready().unwrap());
    assert!(!lifetime.is_retired());
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    assert!(lifetime.is_retired());
    // Reaping Work does not return the original resource reservation.
    assert_eq!(resources.active.resources().unwrap(), retained_active);
    resources
        .active
        .release_quota(&resources.active.resources().unwrap().quota)
        .unwrap();
    resources.active.finish().unwrap();
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 0);
    assert_eq!(f.scope.snapshot().unwrap().work, original.work);
    assert_eq!(f.scope.snapshot().unwrap().unresolved, 0);
    assert!(f.ledger.unresolved_work().is_empty());
    f.finish();
}
