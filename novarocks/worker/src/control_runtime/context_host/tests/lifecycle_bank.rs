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

//! A partial Root lifecycle bank remains with its original Context payer.
//! No host body is captured and no successful M/R or HostRemoved is invented.

use super::*;

fn funded_charges(fixture: &Harness) -> [u64; 8] {
    [
        Dimension::Work,
        Dimension::Captures,
        Dimension::Completions,
        Dimension::Inputs,
        Dimension::Batches,
        Dimension::Effects,
        Dimension::Contexts,
        Dimension::Tickets,
    ]
    .map(|dimension| fixture.ledger.used(dimension.id()))
}

#[test]
fn a_partial_root_lifecycle_bank_stays_owned_and_withdraws_without_reissuing_or_refunding() {
    let mut fixture = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
    let materialize = fixture.materialize();
    let binding = fixture.binding(context_actor(), &materialize);
    fixture
        .service
        .adopt_context(&materialize, binding)
        .unwrap();
    fixture.complete_materialize_handoff(materialize);
    let child = fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .scope()
        .clone();
    let baseline = child.snapshot().unwrap();
    let root_before = fixture.runtime.snapshot(control_actor()).unwrap();
    let context_before = fixture.runtime.snapshot(context_actor()).unwrap();
    let paid = funded_charges(&fixture);

    // This actual Root has one Control position. Context IO first binds its
    // own three roles/two callbacks, then the first lifecycle role binds to
    // Root and the second lifecycle reservation meets actual InputCapacity.
    let partial_refused = matches!(
        fixture
            .service
            .context(fixture.identity)
            .unwrap()
            .prepare_host_jobs(),
        Err(ContextHostError::HostJob(ContextHostJobError::Driver(
            DriverError::InputCapacity
        )))
    );
    let partial = child.snapshot().unwrap();
    let root_partial = fixture.runtime.snapshot(control_actor()).unwrap();
    let context_partial = fixture.runtime.snapshot(context_actor()).unwrap();
    let (io_kept, lifecycle_kept, no_captured_host_jobs) = {
        let resources = &fixture.service.context(fixture.identity).unwrap().resources;
        (
            resources.io.is_some(),
            resources.lifecycle.is_some(),
            resources.host_jobs.is_none(),
        )
    };
    let paid_after_refusal = funded_charges(&fixture) == paid;
    let duplicate_refused = matches!(
        fixture
            .service
            .context(fixture.identity)
            .unwrap()
            .prepare_host_jobs(),
        Err(ContextHostError::Io(ContextIoError::AlreadyAdmitted))
    );
    let duplicate_child_unchanged = child.snapshot().unwrap() == partial;
    let root_duplicate = fixture.runtime.snapshot(control_actor()).unwrap();
    let context_duplicate = fixture.runtime.snapshot(context_actor()).unwrap();
    let duplicate_charge_unchanged = funded_charges(&fixture) == paid;

    fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .resources
        .lifecycle
        .as_mut()
        .unwrap()
        .withdraw_unused()
        .unwrap();
    let root_withdrawn = fixture.runtime.snapshot(control_actor()).unwrap();
    fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .resources
        .lifecycle
        .as_mut()
        .unwrap()
        .withdraw_unused()
        .unwrap();
    let root_withdrawn_again = fixture.runtime.snapshot(control_actor()).unwrap();
    let lifecycle_still_owned = fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .resources
        .lifecycle
        .is_some();
    let inventory = fixture
        .service
        .context(fixture.identity)
        .unwrap()
        .resources
        .io
        .take()
        .unwrap();
    inventory.withdraw_unused();
    let withdrawn = child.snapshot().unwrap();
    let context_withdrawn = fixture.runtime.snapshot(context_actor()).unwrap();
    let paid_after_withdrawal = funded_charges(&fixture) == paid;

    // Withdrawing original positions is not permission to create a new bank.
    // Keep the same partial lifecycle object in the Context's real resources.
    let retry_refused = matches!(
        fixture
            .service
            .context(fixture.identity)
            .unwrap()
            .prepare_host_jobs(),
        Err(ContextHostError::Io(ContextIoError::AlreadyAdmitted))
    );
    let retry_child_unchanged = child.snapshot().unwrap() == withdrawn;
    let root_retry = fixture.runtime.snapshot(control_actor()).unwrap();
    let context_retry = fixture.runtime.snapshot(context_actor()).unwrap();
    let retry_charge_unchanged = funded_charges(&fixture) == paid;
    let reaped = fixture
        .ledger
        .reap_scope_ready(child.id(), CONTEXT_IO_ROLES)
        .unwrap();
    let restored = child.snapshot().unwrap();
    let original_context_fee_still_paid = funded_charges(&fixture) == paid;

    // Neither bank ever handed off a Context fact or a host payload. Only now
    // use the existing actual metadata-model refusal and real Scope teardown.
    let (host, retirement) = fixture.actual_model_refusal_retirement();
    fixture.finish_retired_context(host, retirement);

    assert!(partial_refused);
    assert!(io_kept);
    assert!(lifecycle_kept);
    assert!(no_captured_host_jobs);
    assert_eq!(partial.work, baseline.work + CONTEXT_IO_ROLES);
    assert_eq!(partial.storage, baseline.storage + CONTEXT_IO_ROLES);
    assert_eq!(root_partial.roles, root_before.roles + 1);
    assert_eq!(root_partial.inputs, root_before.inputs);
    assert_eq!(
        context_partial.roles,
        context_before.roles + CONTEXT_IO_CALLBACKS
    );
    assert_eq!(context_partial.inputs, context_before.inputs);
    assert!(paid_after_refusal);
    assert!(duplicate_refused);
    assert!(duplicate_child_unchanged);
    assert_eq!(root_duplicate.roles, root_partial.roles);
    assert_eq!(root_duplicate.inputs, root_partial.inputs);
    assert_eq!(context_duplicate.roles, context_partial.roles);
    assert_eq!(context_duplicate.inputs, context_partial.inputs);
    assert!(duplicate_charge_unchanged);
    assert_eq!(root_withdrawn.roles, root_before.roles);
    assert_eq!(root_withdrawn.inputs, root_before.inputs);
    assert_eq!(root_withdrawn_again.roles, root_withdrawn.roles);
    assert_eq!(root_withdrawn_again.inputs, root_withdrawn.inputs);
    assert!(lifecycle_still_owned);
    assert_eq!(withdrawn.work, partial.work);
    assert_eq!(withdrawn.storage, baseline.storage);
    assert_eq!(context_withdrawn.roles, context_before.roles);
    assert_eq!(context_withdrawn.inputs, context_before.inputs);
    assert!(paid_after_withdrawal);
    assert!(retry_refused);
    assert!(retry_child_unchanged);
    assert_eq!(root_retry.roles, root_before.roles);
    assert_eq!(root_retry.inputs, root_before.inputs);
    assert_eq!(context_retry.roles, context_before.roles);
    assert_eq!(context_retry.inputs, context_before.inputs);
    assert!(retry_charge_unchanged);
    assert_eq!(reaped, CONTEXT_IO_ROLES);
    assert_eq!(restored.work, baseline.work);
    assert_eq!(restored.storage, baseline.storage);
    assert!(original_context_fee_still_paid);
}
