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

//! Worker admission produces one original Work/bundle/report binding. A
//! mismatch at these trusted composition points is a shared invariant failure,
//! not a query refusal. Abort before unwinding can drop opaque input on a
//! control poller; normal Common callers can instead retain the full refusal.

use crate::task_completion_set::AdmittedCompletionTransfer;
use novarocks_control_runtime::{
    completion::ReportSlot,
    obligation::{ReservationObligation, WorkChildAuthority, WorkObligation},
    reported_work::ReportedWork,
    transaction::{DeclaredInput, RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    completion::ConsumerKind,
    identity::{ConsumerId, Generation, ScopeId},
    scope::WorkKind,
};

pub(crate) fn adopt_reported_work(
    work: WorkObligation,
    capacity: ReservationObligation,
    report: ReportSlot,
) -> ReportedWork {
    match ReportedWork::try_new(work, capacity, report) {
        Ok(admitted) => admitted,
        Err(refusal) => {
            std::mem::forget(refusal);
            std::process::abort();
        }
    }
}

/// Financial admission and the consuming Core are separate original owners.
/// `core_owner` names that Core's Scope and initial actor epoch; it grants no
/// Work and never replaces the financial Scope's current generation.
pub(super) fn reserve_reported_work(
    scope: &ScopeHandle,
    payer: &mut ReservationObligation,
    actual_parent: &WorkObligation,
    resources: ReservationResources,
    kind: WorkKind,
    core_owner: Option<(ScopeId, Generation)>,
) -> Result<AdmittedCompletionTransfer, RuntimeError> {
    reserve_reported_work_with_parent(
        scope,
        payer,
        ReportedParentKind::Work(actual_parent),
        resources,
        kind,
        core_owner,
    )
}

/// The same unique payer funds a child of an independently held original
/// baseline. Authority revalidation and position binding share Common's gate.
pub(super) fn reserve_reported_work_from_authority(
    scope: &ScopeHandle,
    payer: &mut ReservationObligation,
    actual_parent: &WorkChildAuthority,
    resources: ReservationResources,
    kind: WorkKind,
    core_owner: Option<(ScopeId, Generation)>,
) -> Result<AdmittedCompletionTransfer, RuntimeError> {
    reserve_reported_work_with_parent(
        scope,
        payer,
        ReportedParentKind::Authority(actual_parent),
        resources,
        kind,
        core_owner,
    )
}

enum ReportedParentKind<'a> {
    Work(&'a WorkObligation),
    Authority(&'a WorkChildAuthority),
}
fn reserve_reported_work_with_parent(
    scope: &ScopeHandle,
    payer: &mut ReservationObligation,
    actual_parent: ReportedParentKind<'_>,
    resources: ReservationResources,
    kind: WorkKind,
    core_owner: Option<(ScopeId, Generation)>,
) -> Result<AdmittedCompletionTransfer, RuntimeError> {
    // Financial admission follows this original Scope. Report consumption
    // targets the explicitly supplied Core, independently of the parent.
    let generation = scope.gates()?.generation;
    let mut consumers = Vec::with_capacity(2);
    if let Some((core_scope, core_generation)) = core_owner {
        consumers.push((
            ConsumerId::new(1).unwrap(),
            ConsumerKind::Owner {
                scope: core_scope,
                generation: core_generation,
            },
        ));
    }
    consumers.push((ConsumerId::new(2).unwrap(), ConsumerKind::Observer));
    let declared_input = DeclaredInput {
        generation,
        kind,
        resources,
        consumers,
    };
    let reported = match actual_parent {
        ReportedParentKind::Work(parent) => scope.reserve_child_from(payer, parent, declared_input),
        ReportedParentKind::Authority(parent) => {
            scope.reserve_child_from_authority(payer, parent, declared_input)
        }
    }?;
    let (work, capacity, report) = reported.into_parts();
    Ok(AdmittedCompletionTransfer {
        work,
        capacity,
        report,
    })
}

fn revoke_report(report: &ReportSlot) {
    report.revoke_unused().expect("unbound original report");
    report.body_exited().expect("unbound report producer exit");
    report
        .captures_exited()
        .expect("unbound report has no captures");
    assert!(report.try_retire().expect("unbound report retirement"));
}
pub(super) fn dispose_unused_completion(mut completion_transfer: AdmittedCompletionTransfer) {
    // No capability/capture was handed to the Task factory. This exact
    // unused holder exits now; shared bounded reaping handles its parent link.
    revoke_report(&completion_transfer.report);
    completion_transfer
        .work
        .body_exited()
        .expect("unused original work body");
    completion_transfer
        .work
        .captures_exited()
        .expect("unused original work captures");
    completion_transfer
        .capacity
        .finish()
        .expect("unused original reservation storage exit");
}
