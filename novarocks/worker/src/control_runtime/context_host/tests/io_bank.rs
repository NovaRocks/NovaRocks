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

//! The original Context funds these unused metadata roles. No host instance,
//! M/R callback, Native materialization or successful Core fact is fabricated.

use super::*;
use crate::control_runtime::context_io::{CONTEXT_IO_CALLBACKS, CONTEXT_IO_ROLES, ContextIoError};

fn adopted_metadata_context() -> Harness {
    let mut fixture = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
    let materialize = fixture.materialize();
    let binding = fixture.binding(context_actor(), &materialize);
    fixture
        .service
        .adopt_context(&materialize, binding)
        .unwrap();
    fixture.complete_materialize_handoff(materialize);
    fixture
}

fn bank_charges(fixture: &Harness) -> [u64; 3] {
    [
        fixture.ledger.used(Dimension::Work.id()),
        fixture.ledger.used(Dimension::Captures.id()),
        fixture.ledger.used(Dimension::Completions.id()),
    ]
}

#[test]
fn an_original_context_binds_and_withdraws_its_prepaid_io_bank_without_new_charge() {
    let mut fixture = adopted_metadata_context();
    let scope = fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    let baseline = scope.snapshot().unwrap();
    let original_callbacks = fixture.runtime.snapshot(context_actor()).unwrap();
    let paid = bank_charges(&fixture);
    fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_io()
        .unwrap();
    let admitted = scope.snapshot().unwrap();
    let callbacks = fixture.runtime.snapshot(context_actor()).unwrap();
    let unchanged_charge_on_admit = bank_charges(&fixture) == paid;
    let duplicate_refused = matches!(
        fixture
            .service
            .context(fixture.identity)
            .unwrap()
            .lock()
            .unwrap()
            .admit_io(),
        Err(ContextHostError::Io(ContextIoError::AlreadyAdmitted))
    );
    let duplicate_unchanged = scope.snapshot().unwrap() == admitted;
    let inventory = fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .lock()
        .unwrap()
        .resources
        .io
        .take()
        .unwrap();
    inventory.withdraw_unused();
    let withdrawn = scope.snapshot().unwrap();
    let callbacks_after_withdrawal = fixture.runtime.snapshot(context_actor()).unwrap();
    let withdrawn_bank_cannot_rebind = matches!(
        fixture
            .service
            .context(fixture.identity)
            .unwrap()
            .lock()
            .unwrap()
            .admit_io(),
        Err(ContextHostError::Io(ContextIoError::AlreadyAdmitted))
    );
    let withdrawal_retry_unchanged = scope.snapshot().unwrap() == withdrawn;
    let reaped = fixture
        .ledger
        .reap_scope_ready(scope.id(), CONTEXT_IO_ROLES)
        .unwrap();
    let restored = scope.snapshot().unwrap();
    let bank_still_paid = bank_charges(&fixture) == paid;
    let (host, retirement) = fixture.actual_model_refusal_retirement();
    fixture.finish_retired_context(host, retirement);

    assert_eq!(admitted.work, baseline.work + CONTEXT_IO_ROLES);
    assert_eq!(admitted.storage, baseline.storage + CONTEXT_IO_ROLES);
    assert_eq!(
        callbacks.roles,
        original_callbacks.roles + CONTEXT_IO_CALLBACKS
    );
    assert_eq!(callbacks.inputs, 0);
    assert!(unchanged_charge_on_admit);
    assert!(duplicate_refused);
    assert!(duplicate_unchanged);
    assert_eq!(withdrawn.work, admitted.work);
    assert_eq!(withdrawn.storage, baseline.storage);
    assert_eq!(callbacks_after_withdrawal.roles, original_callbacks.roles);
    assert_eq!(callbacks_after_withdrawal.inputs, 0);
    assert!(withdrawn_bank_cannot_rebind);
    assert!(withdrawal_retry_unchanged);
    assert_eq!(reaped, CONTEXT_IO_ROLES);
    assert_eq!(restored.work, baseline.work);
    assert_eq!(restored.storage, baseline.storage);
    assert!(bank_still_paid);
}

#[test]
fn a_sealed_context_refuses_callbacks_and_retires_all_three_unused_partial_roles() {
    let mut fixture = adopted_metadata_context();
    let scope = fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    let baseline = scope.snapshot().unwrap();
    let paid = bank_charges(&fixture);
    fixture
        .runtime
        .close_actor_admission(context_actor())
        .unwrap();
    // This stopped component driver withdraws its one original unused parent
    // source before sealing; an empty input queue alone cannot revoke it.
    assert!(
        fixture
            .service
            .context(fixture.identity)
            .unwrap()
            .lock()
            .unwrap()
            .resources
            .owner_control
            .close_for_teardown()
            .unwrap()
    );
    assert!(
        fixture
            .service
            .context(fixture.identity)
            .unwrap()
            .lock()
            .unwrap()
            .resources
            .internal
            .close_for_teardown()
            .unwrap()
    );
    fixture.runtime.seal_actor(context_actor()).unwrap();
    let callback_refused = matches!(
        fixture
            .service
            .context(fixture.identity)
            .unwrap()
            .lock()
            .unwrap()
            .admit_io(),
        Err(ContextHostError::Io(ContextIoError::Driver(
            DriverError::Closed
        )))
    );
    let partial = scope.snapshot().unwrap();
    let no_inventory = fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .lock()
        .unwrap()
        .resources
        .io
        .is_none();
    let callbacks = fixture.runtime.snapshot(context_actor()).unwrap();
    let unchanged_charge_on_refusal = bank_charges(&fixture) == paid;
    let failed_bank_cannot_rebind = matches!(
        fixture
            .service
            .context(fixture.identity)
            .unwrap()
            .lock()
            .unwrap()
            .admit_io(),
        Err(ContextHostError::Io(ContextIoError::AlreadyAdmitted))
    );
    let refusal_retry_unchanged = scope.snapshot().unwrap() == partial;
    let reaped = fixture
        .ledger
        .reap_scope_ready(scope.id(), CONTEXT_IO_ROLES)
        .unwrap();
    let restored = scope.snapshot().unwrap();
    let bank_still_paid = bank_charges(&fixture) == paid;
    // The actual seal can be repeated by the ordinary metadata cleanup path.
    // Its success grants neither an Active fact nor a materialization result.
    let resealed = fixture.runtime.seal_actor(context_actor()).is_ok();
    let (host, retirement) = fixture.actual_model_refusal_retirement();
    fixture.finish_retired_context(host, retirement);

    assert!(callback_refused);
    assert!(no_inventory);
    assert_eq!(partial.work, baseline.work + CONTEXT_IO_ROLES);
    assert_eq!(partial.storage, baseline.storage);
    assert_eq!(callbacks.inputs, 0);
    assert_eq!(callbacks.roles, 0);
    assert!(unchanged_charge_on_refusal);
    assert!(failed_bank_cannot_rebind);
    assert!(refusal_retry_unchanged);
    assert_eq!(reaped, CONTEXT_IO_ROLES);
    assert_eq!(restored.work, baseline.work);
    assert_eq!(restored.storage, baseline.storage);
    assert!(bank_still_paid);
    assert!(resealed);
}

#[test]
fn an_actual_holder_cleanup_retires_with_only_its_host_observer_and_no_core_callback() {
    use crate::{
        control_runtime::{
            admitted_role::{adopt_reported_work, dispose_unused_completion},
            context_io::ContextIoParts,
        },
        task_completion_roles::AdmittedCompletionRole,
    };
    use novarocks_control_runtime::exit_role::ExitRole;
    use novarocks_control_runtime_core::identity::ConsumerId;
    use std::thread;

    let mut fixture = adopted_metadata_context();
    let scope = fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    let baseline = scope.snapshot().unwrap();
    let original_callbacks = fixture.runtime.snapshot(context_actor()).unwrap();
    let paid = bank_charges(&fixture);
    fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_io()
        .unwrap();
    let ContextIoParts {
        materialization,
        release,
        holder,
        mut materialization_input,
        mut release_input,
    } = fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .lock()
        .unwrap()
        .resources
        .io
        .take()
        .unwrap()
        .into_parts();
    let no_core_input_before = fixture
        .runtime
        .input_inventory(context_actor(), None, 8)
        .unwrap()
        .is_empty();
    let AdmittedCompletionRole {
        work,
        capacity,
        report,
    } = holder;
    let holder_report = report.clone();
    let reported = adopt_reported_work(work, capacity, report);
    let (holder, mut authority) = ExitRole::from_admitted(reported);
    let holder = thread::spawn(move || {
        authority.start().unwrap();
        // This original holder has no instance or opaque capture yet. Its
        // actual host invocation ends here, independently of either M/R role.
        authority.exited().unwrap();
        holder
    })
    .join()
    .unwrap();
    assert!(holder.actual_exit_ready().unwrap());
    holder_report.consume(ConsumerId::new(2).unwrap()).unwrap();
    let retired_without_core = holder.try_retire().unwrap();
    if !retired_without_core {
        // Preserve the real oracle above. If the constructor still declared a
        // phantom Core consumer, acknowledge it only for diagnostic teardown,
        // so strict Scope Drop cannot replace the intended ordinary assertion.
        holder_report.consume(ConsumerId::new(1).unwrap()).unwrap();
        assert!(holder.try_retire().unwrap());
    }
    let no_core_input_after = fixture
        .runtime
        .input_inventory(context_actor(), None, 8)
        .unwrap()
        .is_empty();
    drop(holder_report);
    drop(holder);
    dispose_unused_completion(materialization);
    dispose_unused_completion(release);
    materialization_input.close_after_exit().unwrap();
    release_input.close_after_exit().unwrap();
    let report_storage_restored = scope.snapshot().unwrap().storage == baseline.storage;
    let reaped = fixture
        .ledger
        .reap_scope_ready(scope.id(), CONTEXT_IO_ROLES)
        .unwrap();
    let restored = scope.snapshot().unwrap();
    let callbacks = fixture.runtime.snapshot(context_actor()).unwrap();
    let bank_still_paid = bank_charges(&fixture) == paid;
    let (host, retirement) = fixture.actual_model_refusal_retirement();
    fixture.finish_retired_context(host, retirement);

    assert!(
        retired_without_core,
        "the physical holder must not wait for a Core callback it never owns"
    );
    assert!(no_core_input_before);
    assert!(no_core_input_after);
    assert!(report_storage_restored);
    assert_eq!(reaped, CONTEXT_IO_ROLES);
    assert_eq!(restored.work, baseline.work);
    assert_eq!(restored.storage, baseline.storage);
    assert_eq!(callbacks.roles, original_callbacks.roles);
    assert_eq!(callbacks.inputs, 0);
    assert!(bank_still_paid);
}
