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
    actors::tests::{Fixture, finish_source_routes, generation, operation, task},
    causal::{CausalSourceRetirement, RpcAuthorAdmission},
};
use novarocks_worker_protocol::lease::MonotonicInstant;
use std::time::Duration;

#[test]
fn closing_task_rpc_admission_returns_new_originals_and_keeps_the_existing_cut_alive() {
    let mut f = Fixture::new();
    let identity = task(f.identity, 1);
    let mut host = f.adopt_task_host(identity, 4);
    let original_scope = f.scope.clone();
    let parent = host.resources.active.id();
    let initial_open = host.request_admission_open() && host.task_id() == identity;
    let admitted =
        reserve_reported_role(&f.scope, parent, WorkKind::Settlement, Some(generation())).unwrap();
    let admission =
        CutSourceAdmission::Fresh(test_model_source_slots::<2>(&f.scope, parent, generation()));
    let request = RequestMetadata {
        operation: operation(92),
        deadline: MonotonicInstant::from_origin(Duration::from_nanos(99)),
    };
    let RpcAuthorAdmission {
        cut,
        role,
        authority,
        sender,
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

    let rejected =
        reserve_reported_role(&f.scope, parent, WorkKind::Settlement, Some(generation())).unwrap();
    let work = rejected.work.id();
    let capacity = rejected.capacity.id();
    let report = rejected.report.clone();
    let source_roles = test_model_source_slots::<2>(&f.scope, parent, generation());
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
    dispose_unused_completion(rejected);
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

    // The original already-admitted sender still really returns after closure.
    // This neither exits its main R nor fabricates a Core event/consumption.
    let returned = sender.withdraw();
    host.causal().observe_rpc_author_return(returned).unwrap();
    let still_no_event = host.causal().rpc_delivery(cut).unwrap().is_none();
    host.causal().observe_effects_exited(cut).unwrap();
    authority.withdraw_unpublished_and_exit().unwrap();
    f.ledger.reap_scope_ready(f.scope.id(), 128).unwrap();
    let original_retired = role.try_retire().unwrap();
    let retired = host
        .causal()
        .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
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
    for mut storage in ingress.release_owned_pair(&retired.pair, reaped).unwrap() {
        storage.release().unwrap();
    }
    drop(retired);
    let mut resources = host.into_resources();
    finish_source_routes(&f, &ingress);
    drop(ingress);
    resources.lifetime.body_exited().unwrap();
    resources.lifetime.captures_exited().unwrap();
    let original_lifetime_exited = resources.lifetime.actual_exit_ready().unwrap();
    resources
        .active
        .release(&resources.active.resources().unwrap())
        .unwrap();
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
