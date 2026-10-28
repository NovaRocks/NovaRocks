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

use crate::task_completion_roles::AdmittedCompletionRole;
use novarocks_control_runtime::{
    completion::ReportSlot,
    lease::{CapacityBundle, WorkLease},
    reported_work::ReportedWork,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::ConsumerKind,
    identity::{ConsumerId, Generation, ReservationId},
    scope::WorkKind,
};

pub(crate) fn adopt_reported_work(
    work: WorkLease,
    capacity: CapacityBundle,
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

pub(super) fn reserve_reported_role(
    scope: &ScopeHandle,
    parent: ReservationId,
    kind: WorkKind,
    core_generation: Option<Generation>,
) -> Result<AdmittedCompletionRole, RuntimeError> {
    let generation = scope.gates()?.generation;
    let guard = scope.reserve_child_next(generation, parent, ResourceVector::default(), kind)?;
    let mut consumers = Vec::with_capacity(2);
    if let Some(core_generation) = core_generation {
        consumers.push((
            ConsumerId::new(1).unwrap(),
            ConsumerKind::Owner {
                scope: scope.id(),
                generation: core_generation,
            },
        ));
    }
    consumers.push((ConsumerId::new(2).unwrap(), ConsumerKind::Observer));
    let report = ReportSlot::reserve_next(scope, generation, guard.work_id(), 2, consumers)?;
    let (work, capacity) = match guard.bind(kind) {
        Ok(bound) => bound,
        Err(error) => {
            revoke_report(&report);
            return Err(error);
        }
    };
    Ok(AdmittedCompletionRole {
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
pub(super) fn dispose_unused_completion(mut role: AdmittedCompletionRole) {
    // No capability/capture was handed to the fragment factory. This exact
    // unused holder exits now; shared bounded reaping handles its parent link.
    revoke_report(&role.report);
    role.work.body_exited().expect("unused original role body");
    role.work
        .captures_exited()
        .expect("unused original role captures");
    role.capacity
        .release(&ResourceVector::default())
        .expect("zero-resource original header release");
}
