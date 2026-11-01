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

//! The pure protocol's synchronous capacity port. There is one ledger; this
//! adapter keeps only its still-unbound guards, never another set of counters.

use crate::control_runtime_policy::{PolicyError, StorageNeed};
use novarocks_control_runtime::{
    lease::StepGuard,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{DimensionId, Generation, ReservationId, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{OperationOutcome, QueryContextRef};
use novarocks_worker_protocol::creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal};
use std::collections::BTreeMap;

/// Actual Worker process quotas. Object positions are owned storage, not
/// process capacity dimensions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub(crate) enum Dimension {
    ActiveTasks = 1,
    PreparingTasks,
    PreparingBytes,
    /// One actually admitted external request, independent of reply cells.
    Requests = 16,
}
impl Dimension {
    pub(crate) const fn id(self) -> DimensionId {
        DimensionId::new(self as u64).unwrap()
    }
}

impl StorageNeed {
    pub(crate) fn resources(self) -> Result<ReservationResources, PolicyError> {
        let quota = ResourceVector::new([(
            Dimension::Requests.id(),
            u64::try_from(self.requests).map_err(|_| PolicyError::Overflow)?,
        )])
        .map_err(|_| PolicyError::Overflow)?;
        Ok(ReservationResources::new(
            quota,
            OwnedSlotNeed {
                inputs: self.inputs,
                batches: self.batches,
                effects: self.effects,
                cuts: self.cuts,
                replies: self.replies,
                completions: self.completions,
                captures: self.captures,
                work_headers: self.work,
            },
        ))
    }
}

/// The composition owner derives these from its admitted role inventory.
/// Creating and Accepted have separate storage vectors; neither can borrow
/// positions that belong to another Task or to a returned ingress permit.
#[derive(Clone, Debug)]
pub(crate) struct AdmissionStorage {
    pub loading: ReservationResources,
    pub accepted: ReservationResources,
    pub context: ReservationResources,
}
impl AdmissionStorage {
    fn request(&self, need: AdmissionNeed) -> ReservationResources {
        let (storage, resources) = match need {
            AdmissionNeed::ActiveTask { .. } => {
                (&self.loading, vec![(Dimension::ActiveTasks.id(), 1)])
            }
            AdmissionNeed::Preparation { bytes, .. } => (
                &self.accepted,
                vec![
                    (Dimension::PreparingTasks.id(), 1),
                    (Dimension::PreparingBytes.id(), bytes),
                ],
            ),
            AdmissionNeed::Context { .. } => (&self.context, Vec::new()),
        };
        ReservationResources::new(
            ResourceVector::new(resources)
                .and_then(|resources| resources.checked_add(&storage.quota))
                .expect("validated fixed Worker admission quota"),
            storage.owned_slots,
        )
    }
}

/// One actor's exact synchronous port. Only the actor mutates this map. The
/// guards remain visible in their runtime scope before the port returns.
pub(crate) struct ScopeAdmissionPort {
    scope: ScopeHandle,
    generation: Generation,
    context: Option<QueryContextRef>,
    storage: AdmissionStorage,
    guards: BTreeMap<ReservationId, StepGuard>,
    admission: WorkAdmission,
}
impl ScopeAdmissionPort {
    pub(crate) fn new(
        scope: ScopeHandle,
        generation: Generation,
        context: Option<QueryContextRef>,
        storage: AdmissionStorage,
    ) -> Self {
        Self {
            scope,
            generation,
            context,
            storage,
            guards: BTreeMap::new(),
            admission: WorkAdmission::External,
        }
    }
    /// Existing context responsibility admits its drain work even after the
    /// process closes external intake. The actual ledger validates this parent.
    pub(crate) fn with_parent(mut self, parent: WorkId) -> Self {
        self.admission = WorkAdmission::Derived(parent);
        self
    }

    /// Transfers the same reservations to the step/batch custodian. These
    /// cannot be dropped as unused after a queue/capture has been bound.
    pub(crate) fn take_guards(&mut self) -> Vec<StepGuard> {
        std::mem::take(&mut self.guards).into_values().collect()
    }

    fn refusal(need: AdmissionNeed) -> AdmissionRefusal {
        if matches!(need, AdmissionNeed::Preparation { .. }) {
            OperationOutcome::PreparationBusy.into()
        } else {
            OperationOutcome::ResourceExhausted.into()
        }
    }
}
impl AdmissionPort for ScopeAdmissionPort {
    fn try_reserve(&mut self, need: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        let context = match need {
            AdmissionNeed::ActiveTask { context, .. }
            | AdmissionNeed::Preparation { context, .. }
            | AdmissionNeed::Context { context, .. } => context,
        };
        if let Some(bound) = self.context {
            assert_eq!(bound, context, "scope admission exact context binding");
        }
        let resources = self.storage.request(need);
        let reserved = match need {
            AdmissionNeed::Preparation { active, .. } => self.scope.reserve_child_next(
                self.generation,
                active,
                resources,
                WorkKind::Business,
            ),
            _ => self.scope.reserve_next(
                self.generation,
                resources,
                WorkKind::Business,
                self.admission,
            ),
        };
        let guard = match reserved {
            Ok(guard) => guard,
            Err(RuntimeError::CapacityDenied(_)) => return Err(Self::refusal(need)),
            Err(error) => {
                // A normal close is decided by the core before closing its
                // scope gate; a dead owner cannot execute another core step.
                // Other failures here indicate broken composition/ledger
                // invariants, not application capacity or a retryable verdict.
                tracing::error!(
                    ?error,
                    "control admission lost its live scope or capacity invariant"
                );
                std::process::abort();
            }
        };
        let id = guard.reservation_id();
        assert!(self.guards.insert(id, guard).is_none());
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_control_runtime::transaction::RuntimeLedger;
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn context() -> QueryContextRef {
        QueryContextRef::new(
            QueryExecutionId::new(QueryId::new(391, 392), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }
    fn task(context: QueryContextRef) -> TaskIdentity {
        TaskIdentity::new(
            context.query_execution_id(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            context.backend_process_id(),
        )
    }
    fn generation() -> Generation {
        Generation::new(1).unwrap()
    }
    fn limits(preparing: u64) -> ResourceVector {
        ResourceVector::new(
            [
                Dimension::ActiveTasks,
                Dimension::PreparingTasks,
                Dimension::PreparingBytes,
                Dimension::Requests,
            ]
            .map(|d| {
                (
                    d.id(),
                    match d {
                        Dimension::PreparingTasks => preparing,
                        _ => 100,
                    },
                )
            }),
        )
        .unwrap()
    }
    fn storage() -> AdmissionStorage {
        let resource = StorageNeed::role(true).resources().unwrap();
        AdmissionStorage {
            loading: resource.clone(),
            accepted: resource.clone(),
            context: resource,
        }
    }
    fn port(ledger: &RuntimeLedger, context: QueryContextRef) -> ScopeAdmissionPort {
        ScopeAdmissionPort::new(
            ledger.open_scope_next(None, generation()).unwrap(),
            generation(),
            Some(context),
            storage(),
        )
    }
    fn preparing(context: QueryContextRef, active: ReservationId) -> AdmissionNeed {
        AdmissionNeed::Preparation {
            context,
            task: task(context),
            active,
            generation: generation(),
            preparing_in_context: 0,
            bytes: 14,
        }
    }

    fn active_task(
        port: &mut ScopeAdmissionPort,
        context: QueryContextRef,
    ) -> (
        ReservationId,
        novarocks_control_runtime::lease::WorkLease,
        novarocks_control_runtime::lease::ReservationBundle,
    ) {
        let id = port
            .try_reserve(AdmissionNeed::ActiveTask {
                context,
                task: task(context),
                generation: generation(),
            })
            .unwrap();
        let mut guards = port.take_guards();
        assert_eq!(guards.len(), 1);
        let (mut work, capacity) = guards.pop().unwrap().bind(WorkKind::Business).unwrap();
        work.start().unwrap();
        (id, work, capacity)
    }
    fn finish_parent(
        ledger: &RuntimeLedger,
        mut work: novarocks_control_runtime::lease::WorkLease,
        mut capacity: novarocks_control_runtime::lease::ReservationBundle,
    ) {
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        ledger.reap_scope_ready(work.scope().id(), 16).unwrap();
        let resources = capacity.resources().unwrap();
        capacity.release_quota(&resources.quota).unwrap();
        capacity.finish().unwrap();
    }

    #[test]
    fn a_successful_guard_is_scope_visible_before_return_and_bound_storage_waits_for_real_exit() {
        let ledger = RuntimeLedger::new(limits(2));
        let context = context();
        let mut port = port(&ledger, context);
        let (active, parent, parent_capacity) = active_task(&mut port, context);
        port.try_reserve(preparing(context, active)).unwrap();
        assert_eq!(port.scope.snapshot().unwrap().work, 2);
        let mut guards = port.take_guards();
        let (mut work, mut bundle) = guards.pop().unwrap().bind(WorkKind::Business).unwrap();
        let resources = bundle.resources().unwrap();
        work.start().unwrap();
        port.scope.close_external().unwrap();
        work.body_exited().unwrap();
        assert!(!port.scope.snapshot().unwrap().join_ready);
        work.captures_exited().unwrap();
        ledger.reap_ready(1).unwrap();
        assert!(!port.scope.snapshot().unwrap().join_ready);
        assert!(!port.scope.snapshot().unwrap().retirement_ready);
        assert_eq!(ledger.used(Dimension::PreparingTasks.id()), 1);
        bundle.release_quota(&resources.quota).unwrap();
        bundle.finish().unwrap();
        finish_parent(&ledger, parent, parent_capacity);
        assert!(port.scope.snapshot().unwrap().retirement_ready);
        ledger.remove_scope(port.scope.id()).unwrap();
    }

    #[test]
    fn competing_contexts_share_one_synchronous_quota_and_contention_is_not_a_refusal() {
        let ledger = RuntimeLedger::new(limits(1));
        let first = context();
        let second = QueryContextRef::new(
            QueryExecutionId::new(QueryId::new(393, 394), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            first.backend_process_id(),
        );
        let mut a = port(&ledger, first);
        let mut b = port(&ledger, second);
        let (aid, aw, ac) = active_task(&mut a, first);
        let (bid, bw, bc) = active_task(&mut b, second);
        let gate = std::sync::Arc::new(std::sync::Barrier::new(3));
        let ga = gate.clone();
        let gb = gate.clone();
        let a = std::thread::spawn(move || {
            ga.wait();
            let result = a.try_reserve(preparing(first, aid));
            (a, result)
        });
        let b = std::thread::spawn(move || {
            gb.wait();
            let result = b.try_reserve(preparing(second, bid));
            (b, result)
        });
        gate.wait();
        let (mut a, ar) = a.join().unwrap();
        let (mut b, br) = b.join().unwrap();
        assert_eq!(usize::from(ar.is_ok()) + usize::from(br.is_ok()), 1);
        let refused = ar.err().or_else(|| br.err()).unwrap();
        assert_eq!(
            refused,
            AdmissionRefusal::Verdict(OperationOutcome::PreparationBusy)
        );
        assert_eq!(ledger.used(Dimension::PreparingTasks.id()), 1);
        drop(a.take_guards());
        drop(b.take_guards());
        assert_eq!(ledger.used(Dimension::PreparingTasks.id()), 0);
        finish_parent(&ledger, aw, ac);
        finish_parent(&ledger, bw, bc);
        for port in [a, b] {
            port.scope.close_external().unwrap();
            ledger.remove_scope(port.scope.id()).unwrap();
        }
        assert!(ledger.is_empty());
    }

    #[test]
    fn external_requests_in_two_scopes_share_one_physical_request_dimension() {
        use crate::control_runtime::control_request::CONTROL_REQUEST_STORAGE;
        // Keep every other storage dimension slack. Only the one actual
        // process-wide request position can refuse the competing scope.
        let budget = StorageNeed {
            requests: 1,
            ..CONTROL_REQUEST_STORAGE.checked_mul(2).unwrap()
        };
        let ledger = RuntimeLedger::new(budget.resources().unwrap().quota);
        let first = ledger.open_scope_next(None, generation()).unwrap();
        let second = ledger.open_scope_next(None, generation()).unwrap();
        let resources = CONTROL_REQUEST_STORAGE.resources().unwrap();
        let (mut work, mut capacity) = first
            .reserve_next(
                generation(),
                resources.clone(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        assert_eq!(ledger.used(Dimension::Requests.id()), 1);
        let before = second.snapshot().unwrap();
        let denied = match second.reserve_next(
            generation(),
            resources.clone(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        ) {
            Err(error) => error,
            Ok(_) => panic!("the same process request dimension must refuse this scope"),
        };
        assert!(
            matches!(denied, RuntimeError::CapacityDenied(denial) if denial.dimension == Dimension::Requests.id() && denial.requested == 1 && denial.committed == 1 && denial.limit == 1)
        );
        assert_eq!(second.snapshot().unwrap(), before);
        // This is a quota-container probe, not a full RPC/report lifecycle.
        // Even genuine Work reaping cannot silently refund the held bundle.
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        assert_eq!(ledger.reap_scope_ready(first.id(), 1).unwrap(), 1);
        assert_eq!(ledger.used(Dimension::Requests.id()), 1);
        let denied = match second.reserve_next(
            generation(),
            resources.clone(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        ) {
            Err(error) => error,
            Ok(_) => panic!("the same process request dimension must refuse this scope"),
        };
        assert!(
            matches!(denied, RuntimeError::CapacityDenied(denial) if denial.dimension == Dimension::Requests.id())
        );
        capacity.release_quota(&resources.quota).unwrap();
        capacity.finish().unwrap();
        assert_eq!(ledger.used(Dimension::Requests.id()), 0);
        let (mut work, mut capacity) = second
            .reserve_next(
                generation(),
                resources.clone(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        assert_eq!(ledger.used(Dimension::Requests.id()), 1);
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        assert_eq!(ledger.reap_scope_ready(second.id(), 1).unwrap(), 1);
        capacity.release_quota(&resources.quota).unwrap();
        capacity.finish().unwrap();
        for scope in [first, second] {
            scope.close_external().unwrap();
            ledger.remove_scope(scope.id()).unwrap();
        }
        assert!(ledger.is_empty());
    }
}
