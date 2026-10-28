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

//! Bootstrap refusal retains the elected Task's actual paid host/source roles.
//! No Native port, getter, observer or execution callback has been exposed.

use super::*;
use crate::control_runtime::{
    source_positions::SOURCE_SLOT_STORAGE,
    task::{TaskHostBinding, TaskHostError, TaskHostRecord},
    task_host_call::{HOST_CALL_STORAGE, TaskHostCallError, TaskHostCallKind},
};
use novarocks_control_runtime::{exit_role::ExitRolePhase, transaction::RuntimeError};
use novarocks_control_runtime_core::scope::ScopeError;
use std::sync::Arc;

#[test]
fn second_host_report_refusal_keeps_the_same_task_and_all_paid_originals_until_unused_exit() {
    // The actual Actor attachment and four Source reports consume five slots.
    // Stop fits as slot six; RetireBorrowing's report must really be refused.
    let mut f = Fixture::with_policy_scope_slot_limit(100, Some(6));
    let policy = f.policy.unwrap();
    let task = task(f.identity, 1);
    let original = f.scope.snapshot().unwrap();
    assert_eq!(original.storage, 1);
    assert_eq!(original.unresolved, 0);
    f.assert_storage_paid(StorageNeed::default());
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
    f.assert_storage_paid(policy.loading);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    let refusal = TaskHostRecord::adopt_loading(
        &f.runtime,
        &ticket,
        TaskHostBinding {
            context: f.identity,
            task_source: ActorId::new(2).unwrap(),
            protocol_generation: generation(),
            max_cuts: 1,
            source_storage_model: false,
            pending: Arc::new(CausalPending),
            health: Arc::new(
                novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
            ),
        },
    )
    .err()
    .expect("the second host report exceeds the actual six-slot Scope limit");
    assert!(matches!(
        refusal.error,
        TaskHostError::HostCall(TaskHostCallError::Runtime(RuntimeError::Scope(
            ScopeError::SlotCapacity
        )))
    ));
    let mut unpublished = refusal
        .unpublished
        .expect("bootstrap refusal returns the same whole elected Task");
    assert_eq!(unpublished.task, task);
    assert_eq!(unpublished.lifetime.id(), task_work);
    assert_eq!(unpublished.active.id(), reservation);
    assert_eq!(unpublished.host_calls.len(), 1);
    assert_eq!(unpublished.host_calls[0].kind(), TaskHostCallKind::Stop);
    assert_eq!(unpublished.host_calls[0].phase(), ExitRolePhase::Admitted);
    assert_eq!(unpublished.host_calls[0].key().0, f.scope.id());
    assert!(!unpublished.host_calls[0].is_retired());
    assert!(unpublished.host_calls[0].poll_return().unwrap().is_none());
    assert_eq!(unpublished.host_call_payments.len(), 1);
    assert_eq!(
        unpublished.host_call_payments[0].resources().unwrap(),
        HOST_CALL_STORAGE.resources().unwrap()
    );
    assert_eq!(unpublished.source_partials.len(), 1);
    assert!(!unpublished.source_partials[0].is_empty());
    assert_eq!(unpublished.source_partials[0].scope().id(), f.scope.id());
    let sources = SOURCE_SLOT_STORAGE.checked_mul(4).unwrap();
    let host_fees = HOST_CALL_STORAGE.checked_mul(2).unwrap();
    let retained_active = policy
        .loading
        .checked_sub(sources)
        .unwrap()
        .checked_sub(host_fees)
        .unwrap();
    assert_eq!(
        unpublished.active.resources().unwrap(),
        retained_active
            .resources()
            .unwrap()
            .checked_add(&ResourceVector::new([(Dimension::ActiveTasks.id(), 1)]).unwrap())
            .unwrap()
    );
    let partial = f.scope.snapshot().unwrap();
    assert_eq!(partial.storage, original.storage + 5);
    assert_eq!(partial.work, elected.work + 5);
    assert_eq!(partial.unresolved, 0);
    assert!(f.ledger.unresolved_work().is_empty());
    // Moving split payments and reported Work into unpublished custody does
    // not refund any of the original Task.loading payment.
    f.assert_storage_paid(policy.loading);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    let lifetime = unpublished.lifetime.retirement_observer();
    assert!(!lifetime.is_retired());
    assert!(!unpublished.lifetime.actual_exit_ready().unwrap());
    assert!(matches!(
        f.runtime.take_step_reservation(&ticket, reservation),
        Err(DriverError::UnknownReservation)
    ));
    f.runtime.effect_completed(ticket).unwrap();
    f.drain(create);
    f.assert_storage_paid(policy.loading);

    // These are the same never-exposed empty roles. Their genuine unused
    // revocation/report retirement/Work reaping precede either host-fee return.
    let mut retired = false;
    for _ in 0..64 {
        if unpublished.retire_host_calls(&f.ledger).unwrap() {
            retired = true;
            break;
        }
    }
    assert!(retired);
    assert!(unpublished.retire_host_calls(&f.ledger).unwrap());
    assert!(unpublished.host_calls[0].is_retired());
    assert_eq!(unpublished.host_calls[0].phase(), ExitRolePhase::Retired);
    assert!(unpublished.host_call_payments.is_empty());
    f.assert_storage_paid(policy.loading.checked_sub(host_fees).unwrap());
    assert_eq!(f.scope.snapshot().unwrap().storage, original.storage + 4);
    assert_eq!(f.scope.snapshot().unwrap().work, elected.work + 4);
    assert!(!unpublished.source_partials[0].is_empty());
    assert!(!lifetime.is_retired());
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);

    // All four original Source holders still have their original paid storage.
    // Revoke and reap them before the Task lifetime can actually finish.
    retire_source_partials(&f, &mut unpublished.source_partials);
    assert_eq!(f.scope.snapshot().unwrap().storage, original.storage);
    assert_eq!(f.scope.snapshot().unwrap().work, elected.work);
    f.assert_storage_paid(retained_active);
    assert!(!lifetime.is_retired());
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    unpublished.lifetime.body_exited().unwrap();
    assert!(!unpublished.lifetime.actual_exit_ready().unwrap());
    unpublished.lifetime.captures_exited().unwrap();
    assert!(unpublished.lifetime.actual_exit_ready().unwrap());
    assert!(!lifetime.is_retired());
    f.assert_storage_paid(retained_active);
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    assert!(lifetime.is_retired());
    // Work reaping is still not the explicit return of the original capacity.
    f.assert_storage_paid(retained_active);
    let resources = unpublished.active.resources().unwrap();
    unpublished.active.release(&resources).unwrap();
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 0);
    f.assert_storage_paid(StorageNeed::default());
    assert_eq!(f.scope.snapshot().unwrap().work, original.work);
    assert_eq!(f.scope.snapshot().unwrap().unresolved, 0);
    assert!(f.ledger.unresolved_work().is_empty());
    drop(unpublished);
    f.finish();
}
