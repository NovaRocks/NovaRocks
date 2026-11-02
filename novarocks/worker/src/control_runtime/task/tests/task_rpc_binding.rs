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

//! Actual LoadInput adoption and finite original roles; this empty-vector
//! protocol fixture proves neither production R funding nor Native/RF exit.

use super::*;
use crate::control_runtime::{
    actors::tests::{Fixture, finish_source_routes, operation, task},
    causal::{CausalSourceRetirement, RpcAuthorAdmission},
};
use novarocks_control_runtime_core::completion::ConsumerKind;
use novarocks_control_runtime_core::identity::ConsumerId;
use novarocks_worker_protocol::lease::MonotonicInstant;
use std::time::Duration;

fn original_rpc_roles(
    scope: &ScopeHandle,
    parent: &WorkLease,
) -> (AdmittedCompletionRole, [SourceSlotAdmission; 2]) {
    let generation = scope.gates().unwrap().generation;
    let (work, mut capacity) = scope
        .reserve_next(
            generation,
            StorageNeed::role(false).resources().unwrap(),
            WorkKind::Settlement,
            novarocks_control_runtime_core::scope::WorkAdmission::Derived(parent.id()),
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    let report = ReportSlot::reserve_next(
        &mut capacity,
        generation,
        2,
        [
            (
                ConsumerId::new(1).unwrap(),
                ConsumerKind::Owner {
                    scope: scope.id(),
                    generation: crate::control_runtime::actors::tests::generation(),
                },
            ),
            (ConsumerId::new(2).unwrap(), ConsumerKind::Observer),
        ],
    )
    .unwrap();
    // The original R owns its own capture and prepaid Source2. Issuing the
    // two children first leaves precisely R's actual capture to adopt.
    let sources = reserve_source_slots::<2>(
        scope,
        &work,
        (
            scope.id(),
            crate::control_runtime::actors::tests::generation(),
        ),
        SourceSlotPayer::Retained(&mut capacity),
    )
    .unwrap();
    (
        AdmittedCompletionRole {
            work,
            capacity,
            report,
        },
        sources,
    )
}

#[test]
fn closing_task_rpc_admission_returns_new_originals_and_keeps_the_existing_cut_alive() {
    let mut f = Fixture::new();
    let identity = task(f.identity, 1);
    let mut host = f.adopt_task_host(identity, 4);
    let original_scope = f.scope.clone();
    let initial_open = host.request_admission_open() && host.task_id() == identity;
    let (admitted, sources) = original_rpc_roles(&f.scope, host.resources.lifetime.work());
    let admission = CutSourceAdmission::Fresh(sources);
    let request = RequestMetadata {
        operation: operation(92),
        deadline: MonotonicInstant::from_origin(Duration::from_nanos(99)),
    };
    let RpcAuthorAdmission {
        cut,
        role,
        authority,
        sender,
        input_author: _,
    } = host
        .bind_request_author(admitted, request, InputClass::Control, 1, admission)
        .unwrap();
    let before_close = f.scope.snapshot().unwrap();
    host.close_request_admission();
    host.close_request_admission();
    let only_admission_closed = !host.request_admission_open()
        && f.scope.snapshot().unwrap() == before_close
        && host.causal().cut_inventory(None, 4) == vec![cut]
        && role.phase() == novarocks_control_runtime::exit_role::ExitRolePhase::Admitted;

    let (rejected, source_roles) = original_rpc_roles(&f.scope, host.resources.lifetime.work());
    let work = rejected.work.id();
    let capacity = rejected.capacity.id();
    let report = rejected.report.clone();
    let sources = source_roles.each_ref().map(|source| {
        (
            source.reported().work().id(),
            source.reported().report().clone(),
        )
    });
    let before_reject = f.scope.snapshot().unwrap();
    let refused = host.bind_request_author(
        rejected,
        request,
        InputClass::Ordinary,
        1,
        CutSourceAdmission::Fresh(source_roles),
    );
    let (error, rejected, admission) = match refused {
        Err(RpcAuthorAdmissionRefusal::BeforeCut {
            error,
            admitted,
            admission,
        }) => (error, admitted, admission),
        _ => panic!("new Task R requires its original open admission gate"),
    };
    let whole_returned = rejected.work.id() == work
        && rejected.capacity.id() == capacity
        && rejected.report.same_slot(&report);
    let CutSourceAdmission::Fresh(source_roles) = admission else {
        unreachable!()
    };
    let sources_returned =
        source_roles
            .each_ref()
            .iter()
            .zip(&sources)
            .all(|(source, (work, report))| {
                source.reported().work().id() == *work
                    && source.reported().report().same_slot(report)
            });
    let no_new_cut_or_stop = f.scope.snapshot().unwrap() == before_reject
        && host.causal().cut_inventory(None, 4) == vec![cut];
    let mut unused =
        crate::control_runtime::source_slot_issuer::SourceSlotIssuePartial::from_admissions(
            &f.scope,
            source_roles,
        );
    let mut unused_retired = false;
    for _ in 0..128 {
        if unused.try_retire(&f.ledger).unwrap() {
            unused_retired = true;
            break;
        }
    }
    assert!(unused_retired);
    dispose_unused_completion(rejected);

    // The original already-admitted sender still really returns after closure.
    // This neither exits its main R nor fabricates a Core event/consumption.
    let returned = sender.withdraw();
    host.causal().observe_rpc_author_return(returned).unwrap();
    let still_no_event = host.causal().rpc_delivery(cut).unwrap().is_none();
    host.causal().observe_effects_exited(cut).unwrap();
    authority.withdraw_unpublished_and_exit().unwrap();
    assert!(
        !role.try_retire().unwrap(),
        "R's original Source2 children still join their actual parent"
    );
    let mut reaped_sources = None;
    for _ in 0..128 {
        let progress = host
            .causal()
            .advance_source_pair_retirement(cut, &f.scope)
            .unwrap();
        if let Some(reaped) = progress.reaped {
            reaped_sources = Some(reaped);
            break;
        }
    }
    let reaped_sources =
        reaped_sources.expect("same R's two unused Source children actually exit and reap");
    let original_retired = role.try_retire().unwrap();
    let retired = host
        .causal()
        .retire_after_exit(cut, CausalSourceRetirement::Reaped(reaped_sources))
        .unwrap();
    let ingress = host.resources.port.ingress().clone();
    for holder in ingress
        .take_unused_owned_pair(&retired.pair)
        .unwrap()
        .into_iter()
        .flatten()
    {
        holder.exit();
    }
    for observation in ingress.owned_pair_inventory(&retired.pair).unwrap() {
        observation.custody.try_retire().unwrap();
    }
    f.ledger.reap_scope_ready(f.scope.id(), 128).unwrap();
    let reaped = ingress
        .prepare_owned_pair_reap(&retired.pair)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    let detached = ingress.release_owned_pair(&retired.pair, reaped).unwrap();
    drop(detached);
    drop(retired);
    let mut resources = host.into_resources();
    finish_source_routes(&f, &ingress);
    drop(ingress);
    crate::control_runtime::task::retire_unused_host_calls_for_test(
        &resources.host_calls,
        &f.ledger,
    );
    // This unissued disposal is the original Context child, outside Task join.
    let mut disposal_retired = false;
    for _ in 0..64 {
        if resources.disposal.retire_unused(&f.ledger).unwrap() {
            disposal_retired = true;
            break;
        }
    }
    assert!(disposal_retired && resources.disposal.is_retired());
    resources.lifetime.body_exited().unwrap();
    resources.lifetime.captures_exited().unwrap();
    let original_lifetime_exited = resources.lifetime.actual_exit_ready().unwrap();
    resources
        .active
        .release_quota(&resources.active.resources().unwrap().quota)
        .unwrap();
    resources.active.finish().unwrap();
    f.ledger.reap_scope_ready(original_scope.id(), 128).unwrap();
    drop(resources);
    f.finish();

    assert!(initial_open && only_admission_closed);
    assert!(matches!(
        error,
        RpcAuthorAdmissionError::Causal(CausalError::RequestAdmissionClosed)
    ));
    assert!(whole_returned && sources_returned && no_new_cut_or_stop);
    assert!(still_no_event && original_retired && original_lifetime_exited);
}
