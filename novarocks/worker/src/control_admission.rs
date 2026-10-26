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
    transaction::{CapacityDenial, RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ResourceVector},
    identity::{DimensionId, Generation, ReservationId, WorkId},
    scope::{ScopeError, WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{
    OperationDomainFailure, OperationOutcome, QueryContextRef, SafeDetail,
    failure::{FailureClass, FailureFact, ResourceDimension, ResourceFacts, ResourceScope},
    operation::DomainFailureOutcome,
};
use novarocks_worker_protocol::creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal};
use std::collections::BTreeMap;

/// Worker-owned accounting vocabulary. Storage and active/preparing work are
/// independent dimensions with different actual holder retirement conditions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub(crate) enum Dimension {
    ActiveTasks = 1,
    PreparingTasks,
    PreparingBytes,
    InboundRecords,
    InboundBytes,
    Contexts,
    Tickets,
    Inputs,
    Batches,
    Effects,
    Cuts,
    Replies,
    Completions,
    Captures,
    Work,
}
impl Dimension {
    pub(crate) const fn id(self) -> DimensionId {
        DimensionId::new(self as u64).unwrap()
    }
}

impl StorageNeed {
    pub(crate) fn resources(self) -> Result<ResourceVector, PolicyError> {
        let pairs = [
            (Dimension::Inputs, self.inputs),
            (Dimension::Batches, self.batches),
            (Dimension::Effects, self.effects),
            (Dimension::Cuts, self.cuts),
            (Dimension::Replies, self.replies),
            (Dimension::Completions, self.completions),
            (Dimension::Captures, self.captures),
            (Dimension::Work, self.work),
        ]
        .into_iter()
        .map(|(dimension, value)| {
            Ok((
                dimension.id(),
                u64::try_from(value).map_err(|_| PolicyError::Overflow)?,
            ))
        })
        .collect::<Result<Vec<_>, PolicyError>>()?;
        ResourceVector::new(pairs).map_err(|_| PolicyError::Overflow)
    }
}

/// The composition owner derives these from its admitted role inventory.
/// Creating and Accepted have separate storage vectors; neither can borrow
/// positions that belong to another Task or to a returned ingress permit.
#[derive(Clone, Debug)]
pub(crate) struct AdmissionStorage {
    pub loading: ResourceVector,
    pub accepted: ResourceVector,
    pub context: ResourceVector,
    pub ticket: ResourceVector,
}
impl AdmissionStorage {
    fn request(&self, need: AdmissionNeed) -> ResourceVector {
        let (storage, resources) = match need {
            AdmissionNeed::ActiveTask { .. } => {
                (&self.loading, vec![(Dimension::ActiveTasks.id(), 1)])
            }
            AdmissionNeed::Preparation {
                bytes,
                inbound_records,
                inbound_bytes,
                ..
            } => (
                &self.accepted,
                vec![
                    (Dimension::PreparingTasks.id(), 1),
                    (Dimension::PreparingBytes.id(), bytes),
                    (Dimension::InboundRecords.id(), inbound_records),
                    (Dimension::InboundBytes.id(), inbound_bytes),
                ],
            ),
            AdmissionNeed::Context { .. } => (&self.context, vec![(Dimension::Contexts.id(), 1)]),
            AdmissionNeed::Ticket { .. } => (&self.ticket, vec![(Dimension::Tickets.id(), 1)]),
        };
        ResourceVector::new(resources)
            .and_then(|resources| resources.checked_add(storage))
            .expect("validated fixed Worker admission vector")
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

    fn refusal(need: AdmissionNeed, denial: CapacityDenial) -> AdmissionRefusal {
        if let AdmissionNeed::Preparation { .. } = need {
            let reason = if denial.dimension == Dimension::InboundRecords.id() {
                Some((
                    crate::failure_reason::WorkerFailureReason::InboundRecordCount,
                    ResourceDimension::Items,
                ))
            } else if denial.dimension == Dimension::InboundBytes.id() {
                Some((
                    crate::failure_reason::WorkerFailureReason::InboundRecordBytes,
                    ResourceDimension::Bytes,
                ))
            } else {
                None
            };
            if let Some((reason, dimension)) = reason {
                let fact = FailureFact::new(FailureClass::Resource, reason.reason()).with_resource(
                    ResourceFacts {
                        request_scope: Some(ResourceScope::Task),
                        constraint_scope: Some(ResourceScope::Process),
                        dimension,
                        requested: Some(denial.requested),
                        required_growth: Some(denial.requested),
                        limit: Some(denial.limit),
                        live: None,
                        committed: Some(denial.committed),
                    },
                );
                return AdmissionRefusal::Domain(
                    DomainFailureOutcome::ResourceExhausted,
                    OperationDomainFailure::new(
                        fact,
                        SafeDetail::truncating(
                            "inbound normal-close reservation exceeds process capacity",
                        ),
                    ),
                );
            }
            return OperationOutcome::PreparationBusy.into();
        }
        OperationOutcome::ResourceExhausted.into()
    }
}
impl AdmissionPort for ScopeAdmissionPort {
    fn try_reserve(&mut self, need: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        let context = match need {
            AdmissionNeed::ActiveTask { context, .. }
            | AdmissionNeed::Preparation { context, .. }
            | AdmissionNeed::Context { context, .. }
            | AdmissionNeed::Ticket { context } => context,
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
            Err(RuntimeError::CapacityDenied(denial)) => return Err(Self::refusal(need, denial)),
            Err(RuntimeError::Capacity(CapacityError::Scope(
                ScopeError::WorkCapacity | ScopeError::SlotCapacity,
            )))
            | Err(RuntimeError::Scope(ScopeError::WorkCapacity | ScopeError::SlotCapacity)) => {
                return Err(if matches!(need, AdmissionNeed::Preparation { .. }) {
                    OperationOutcome::PreparationBusy.into()
                } else {
                    OperationOutcome::ResourceExhausted.into()
                });
            }
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
    fn limits(preparing: u64, inbound_records: u64) -> ResourceVector {
        ResourceVector::new(
            [
                Dimension::ActiveTasks,
                Dimension::PreparingTasks,
                Dimension::PreparingBytes,
                Dimension::InboundRecords,
                Dimension::InboundBytes,
                Dimension::Contexts,
                Dimension::Tickets,
                Dimension::Inputs,
                Dimension::Batches,
                Dimension::Effects,
                Dimension::Cuts,
                Dimension::Replies,
                Dimension::Completions,
                Dimension::Captures,
                Dimension::Work,
            ]
            .map(|d| {
                (
                    d.id(),
                    match d {
                        Dimension::PreparingTasks => preparing,
                        Dimension::InboundRecords => inbound_records,
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
            context: resource.clone(),
            ticket: resource,
        }
    }
    fn port(ledger: &RuntimeLedger, context: QueryContextRef) -> ScopeAdmissionPort {
        ScopeAdmissionPort::new(
            ledger.open_scope_next(None, generation(), 16, 16).unwrap(),
            generation(),
            Some(context),
            storage(),
        )
    }
    fn preparing(
        context: QueryContextRef,
        active: ReservationId,
        inbound_records: u64,
    ) -> AdmissionNeed {
        AdmissionNeed::Preparation {
            context,
            task: task(context),
            active,
            generation: generation(),
            preparing_in_context: 0,
            bytes: 14,
            inbound_records,
            inbound_bytes: 9,
        }
    }

    fn active_task(
        port: &mut ScopeAdmissionPort,
        context: QueryContextRef,
    ) -> (
        ReservationId,
        novarocks_control_runtime::lease::WorkLease,
        novarocks_control_runtime::lease::CapacityBundle,
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
        mut capacity: novarocks_control_runtime::lease::CapacityBundle,
    ) {
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        ledger.reap_scope_ready(work.scope().id(), 16).unwrap();
        let resources = capacity.resources().unwrap();
        capacity.release(&resources).unwrap();
    }

    #[test]
    fn failed_inbound_dimension_keeps_typed_resource_facts_and_reserves_no_other_dimension() {
        let ledger = RuntimeLedger::new(limits(2, 2), 16, 1);
        let context = context();
        let mut port = port(&ledger, context);
        let (active, work, capacity) = active_task(&mut port, context);
        let refusal = port.try_reserve(preparing(context, active, 3)).unwrap_err();
        let AdmissionRefusal::Domain(outcome, failure) = refusal else {
            panic!("preserve typed inbound rejection");
        };
        assert_eq!(outcome, DomainFailureOutcome::ResourceExhausted);
        assert_eq!(
            failure.fact().reason(),
            crate::failure_reason::WorkerFailureReason::InboundRecordCount.reason()
        );
        let resource = failure.fact().resource().unwrap();
        assert_eq!(resource.dimension, ResourceDimension::Items);
        assert_eq!(resource.requested, Some(3));
        assert_eq!(resource.committed, Some(0));
        assert_eq!(resource.limit, Some(2));
        assert_eq!(ledger.used(Dimension::PreparingTasks.id()), 0);
        assert_eq!(ledger.used(Dimension::PreparingBytes.id()), 0);
        assert_eq!(ledger.used(Dimension::InboundBytes.id()), 0);
        assert_eq!(ledger.used(Dimension::ActiveTasks.id()), 1);
        assert_eq!(port.scope.snapshot().unwrap().work, 1);
        assert!(port.take_guards().is_empty());
        finish_parent(&ledger, work, capacity);
        assert_eq!(ledger.used(Dimension::ActiveTasks.id()), 0);
        port.scope.close_external().unwrap();
        ledger.remove_scope(port.scope.id()).unwrap();
    }

    #[test]
    fn a_successful_guard_is_scope_visible_before_return_and_bound_storage_waits_for_real_exit() {
        let ledger = RuntimeLedger::new(limits(2, 2), 16, 1);
        let context = context();
        let mut port = port(&ledger, context);
        let (active, parent, parent_capacity) = active_task(&mut port, context);
        port.try_reserve(preparing(context, active, 1)).unwrap();
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
        bundle.release(&resources).unwrap();
        finish_parent(&ledger, parent, parent_capacity);
        assert!(port.scope.snapshot().unwrap().retirement_ready);
        ledger.remove_scope(port.scope.id()).unwrap();
    }

    #[test]
    fn competing_contexts_share_one_synchronous_quota_and_contention_is_not_a_refusal() {
        let ledger = RuntimeLedger::new(limits(1, 2), 16, 2);
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
            let result = a.try_reserve(preparing(first, aid, 1));
            (a, result)
        });
        let b = std::thread::spawn(move || {
            gb.wait();
            let result = b.try_reserve(preparing(second, bid, 1));
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
}
