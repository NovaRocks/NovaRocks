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

//! Fixed Worker host composition on the common actor pollers. The same
//! original Root and per-Participation owners consume their committed effects;
//! foreign calls remain in the admitted physical host executors.

use super::{
    actors::{WorkerAction, WorkerActor},
    control_request::{ControlRequestHandle, ControlRequestReady},
    establish_executors::EstablishExecutorHandles,
    participation_executors::ParticipationExecutorHandles,
    participation_host::{ParticipationHostBinding, ParticipationHostRecord, TaskIoPorts},
    process_control::{ControlHostCell, ProcessControlOwner},
    service::ParticipationDirectory,
};
use crate::{
    ParticipationHostFactory, control_admission::AdmissionStorage,
    control_runtime_policy::ControlRuntimePolicy,
};
use novarocks_control_runtime::{
    actor_turn::{
        ActorHostTurn, ActorRegistration, ActorRuntime, Clock, PollerObservation, PollerPool,
    },
    publication::PublishedSnapshot,
};
use novarocks_control_runtime_core::identity::{ActorId, Generation, ReservationId, Tick};
use novarocks_execution_contract::{ParticipationRef, ParticipationState, TaskOperationId};
use novarocks_worker_protocol::{
    control::{ControlCore, ControlEffect, ControlInput},
    lease::MonotonicInstant,
    participation::{ParticipationConfig, ParticipationCore},
};
use std::{
    any::Any,
    collections::{BTreeMap, VecDeque},
    num::NonZeroU64,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

/// Explicit process identity input. Numeric equality is never registration
/// authority; every handoff still validates its actual runtime registration.
pub(crate) struct NodeActorIds(AtomicU64);
impl NodeActorIds {
    pub(crate) fn new(first: NonZeroU64) -> Self {
        Self(AtomicU64::new(first.get()))
    }
    fn next(&self) -> ActorId {
        let id = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .unwrap_or_else(|_| std::process::abort());
        ActorId::new(id).expect("nonzero explicit actor identity")
    }
}
struct NodeTaskNotice(super::participation_ready::ParticipationReadyNotice);
impl crate::task_port::TaskPendingNotice for NodeTaskNotice {
    fn pending(&self) {
        self.0.notify();
    }
}
pub(crate) struct NodeParticipationConfig {
    pub core: ParticipationConfig,
    pub policy: ControlRuntimePolicy,
    pub storage: AdmissionStorage,
    pub max_live_tasks: usize,
    pub max_live_roots: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ParticipationRoute {
    pub state: ParticipationState,
    pub generation: Generation,
}
/// The actual neutral route cell and, after physical child removal, its
/// original remaining parent owner. There is one row per elected Participation.
struct ParticipationRow {
    reservation: ReservationId,
    route: PublishedSnapshot<ParticipationRoute>,
    removed: Option<ParticipationHostRecord>,
    retire_requested: bool,
}
#[derive(Default)]
struct ParticipationRows {
    participations: BTreeMap<ParticipationRef, ParticipationRow>,
    reservations: BTreeMap<ReservationId, ParticipationRef>,
    removed_ready: VecDeque<ParticipationRef>,
}
pub(crate) enum NodeTaskReadHandle {
    Live(
        super::participation_host::TaskRequestHandle,
        Option<Arc<super::task_read_payload::TaskReadPayloadShared>>,
    ),
    Sealed(super::participation_host::ParticipationRequestHandle),
}
pub(crate) enum NodeCancelHandle {
    Task(super::participation_host::TaskRequestHandle),
    Participation(super::participation_host::ParticipationRequestHandle),
    Refused(novarocks_worker_protocol::participation::TaskControlRefusal),
}
pub(crate) enum NodeCreateHandle {
    Participation(super::participation_host::ParticipationRequestHandle),
    BeforeParticipation(ControlRequestHandle),
}

/// A refusal owns the original uncompleted ticket/captures. Returning this
/// value cannot acknowledge an action, close an author or release its charge.
#[must_use = "Retain the original refused operation and its actual resources"]
pub(crate) struct NodeHostRefusal {
    pub operation: &'static str,
    original: Box<dyn Any + Send>,
}
impl std::fmt::Debug for NodeHostRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("NodeHostRefusal");
        debug.field("operation", &self.operation);
        if let Some((_, original)) = self
            .original
            .downcast_ref::<(ActorHostTurn<WorkerActor>, NodeHostRefusal)>()
        {
            debug.field("cause", original);
        }
        if let Some((error, ticket)) = self.original.downcast_ref::<(
            super::effect_dispatch::DispatchError,
            novarocks_control_runtime::actor_turn::EffectTicket<WorkerAction>,
        )>() {
            // Original dispatch facts only; never format the effect payload.
            debug
                .field("error", error)
                .field("actor", &ticket.actor)
                .field("input", &ticket.input)
                .field("descriptor", &ticket.descriptor);
        }
        if let Some(error) = self
            .original
            .downcast_ref::<super::effect_dispatch::DispatchError>()
        {
            debug.field("error", error);
        }
        if let Some(error) = self
            .original
            .downcast_ref::<super::process_control::ControlRequestAdvanceError>()
        {
            // Only closed runtime, route and source-phase facts are present.
            // This does not format the original request, host or rich source.
            debug.field("error", error);
        }
        if let Some((refusal, ticket)) = self.original.downcast_ref::<(
            super::participation_removal::ParticipationRemovalRefusal,
            novarocks_control_runtime::actor_turn::EffectTicket<WorkerAction>,
        )>() {
            debug
                .field("error", &refusal.error)
                .field("retains_partial", &refusal.partial.is_some())
                .field("actor", &ticket.actor)
                .field("input", &ticket.input);
        }
        if let Some((error, turn)) = self.original.downcast_ref::<(
            super::participation_host::ParticipationIoHandoffError,
            ActorHostTurn<WorkerActor>,
        )>() {
            // This enum contains only typed runtime/owner error metadata.
            // No retained payload, Task source or host Debug is formatted.
            debug.field("actor", &turn.actor()).field("error", error);
        }
        debug.finish_non_exhaustive()
    }
}
impl NodeHostRefusal {
    fn new<T: Any + Send>(operation: &'static str, original: T) -> Self {
        Self {
            operation,
            original: Box::new(original),
        }
    }
}
/// The original pre-R admission error after its rich input and Native
/// responsibility have moved to the existing paid Root cleanup owner.
/// This value contains no opaque capture, partial R or ingress responsibility.
#[derive(Debug)]
pub(crate) struct NeutralNodeRequestRefusal {
    operation: &'static str,
    _original_error: super::participation_host::requests::ParticipationRequestError,
}
impl NeutralNodeRequestRefusal {
    pub(crate) fn stage(&self) -> &'static str {
        self.operation
    }
    #[cfg(test)]
    pub(crate) fn into_original_for_test(self) -> NodeHostRefusal {
        NodeHostRefusal::new(
            self.operation,
            super::participation_host::requests::ParticipationRequestAdmissionRefusal {
                error: self._original_error,
                partial: None,
            },
        )
    }
}
/// A supported control submission can be refused before it owns any R.
/// Actor turn invariants retain their complete original custody separately.
#[derive(Debug)]
pub(crate) enum NodeRequestRefusal {
    OwnerFailed,
    Neutral(NeutralNodeRequestRefusal),
    Invariant(NodeHostRefusal),
}
impl From<NodeHostRefusal> for NodeRequestRefusal {
    fn from(original: NodeHostRefusal) -> Self {
        Self::Invariant(original)
    }
}
#[derive(Debug, Default)]
pub(crate) struct NodeHostProgress {
    pub root_effects: usize,
    pub runnable: bool,
}

/// There is no user callback table and no second host scheduler. Root's cell
/// is released before any per-Participation lock or actual host submission is taken.
pub(crate) struct WorkerNodeHost {
    clock: Arc<dyn Clock>,
    runtime: ActorRuntime<WorkerActor>,
    root_registration: ActorRegistration,
    control: ControlHostCell,
    metrics_ledger: novarocks_control_runtime::transaction::RuntimeLedger,
    poller_observation: OnceLock<PollerObservation>,
    root_ready_scan: AtomicUsize,
    draining: AtomicBool,
    directory: ParticipationDirectory,
    requests: ControlRequestReady,
    participations: ParticipationExecutorHandles,
    tasks: TaskIoPorts,
    establish: EstablishExecutorHandles,
    factory: Arc<dyn ParticipationHostFactory>,
    config: NodeParticipationConfig,
    actor_ids: NodeActorIds,
    rows: Mutex<ParticipationRows>,
}
impl WorkerNodeHost {
    #[cfg(test)]
    pub(crate) fn hold_normal_retire_handoff_for_test(
        &self,
        participation: ParticipationRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> bool {
        if task.verify_participation(participation).is_err() {
            return false;
        }
        let Some(address) = self.directory.participation(participation) else {
            return false;
        };
        let Some(mut host) = address.lock() else {
            return false;
        };
        host.hold_normal_retire_handoff_for_test(task)
    }
    #[cfg(test)]
    pub(crate) fn release_normal_retire_handoff_for_test(
        &self,
        participation: ParticipationRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> bool {
        if task.verify_participation(participation).is_err() {
            return false;
        }
        let Some(address) = self.directory.participation(participation) else {
            return false;
        };
        let Some(mut host) = address.lock() else {
            return false;
        };
        host.release_normal_retire_handoff_for_test(task)
    }
    #[cfg(test)]
    pub(crate) fn finalized_task_closing_for_test(
        &self,
        participation: ParticipationRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<super::participation_host::task_io::FinalizedTaskClosingForTest> {
        task.verify_participation(participation).ok()?;
        let address = self.directory.participation(participation)?;
        let mut host = address.lock()?;
        host.finalized_task_closing_for_test(task)
    }
    #[cfg(test)]
    pub(crate) fn closing_task_retirement_for_test(
        &self,
        participation: ParticipationRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<(
        novarocks_worker_protocol::creation::PayloadId,
        Generation,
        bool,
        bool,
        bool,
        bool,
    )> {
        task.verify_participation(participation).ok()?;
        let address = self.directory.participation(participation)?;
        let mut host = address.lock()?;
        host.closing_task_retirement_for_test(task)
    }
    #[cfg(test)]
    pub(crate) fn task_domain_refusal_is_closed_for_test(
        &self,
        task: novarocks_execution_contract::TaskIdentity,
        refusal: &NodeHostRefusal,
    ) -> bool {
        use super::participation_host::requests::{
            ParticipationRequestAdmissionRefusal, ParticipationRequestError,
        };
        let Some((admission, request, _)) = refusal.original.downcast_ref::<(
            ParticipationRequestAdmissionRefusal,
            novarocks_execution_contract::UpdateTask,
            super::node_api::WorkerIngressResponsibility,
        )>() else {
            return false;
        };
        refusal.operation == "Task domain original R admission"
            && request.identity() == task
            && admission.partial.is_none()
            && matches!(
                admission.error,
                ParticipationRequestError::Causal(
                    super::causal::CausalError::RequestAdmissionClosed
                )
            )
    }
    #[cfg(test)]
    pub(crate) fn retired_task_contract_for_test(
        &self,
        participation: ParticipationRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<(bool, bool)> {
        let address = self.directory.participation(participation)?;
        let mut host = address.lock()?;
        Some((host.task(task).is_some(), host.has_retired_task_read(task)))
    }
    #[cfg(test)]
    pub(crate) fn ordinary_participation_admission_closed_for_test(
        &self,
        participation: ParticipationRef,
    ) -> bool {
        let Some(address) = self.directory.participation(participation) else {
            return false;
        };
        let Some(host) = address.lock() else {
            return false;
        };
        self.runtime
            .snapshot(host.actor())
            .is_ok_and(|snapshot| !snapshot.failed && !snapshot.external_open)
    }
    #[cfg(test)]
    pub(crate) fn closed_participation_domain_refusal_for_test(
        &self,
        participation: ParticipationRef,
        operation: novarocks_execution_contract::TaskOperationId,
        refusal: &NodeHostRefusal,
    ) -> Option<bool> {
        use super::participation_host::requests::{
            ParticipationRequestAdmissionRefusal, ParticipationRequestError,
        };
        if refusal.operation != "Participation domain original R admission" {
            return None;
        }
        let closed = |admission: &ParticipationRequestAdmissionRefusal| {
            admission.partial.is_none()
                && matches!(admission.error, ParticipationRequestError::AdmissionClosed)
        };
        if let Some((admission, request, _)) = refusal.original.downcast_ref::<(
            ParticipationRequestAdmissionRefusal,
            novarocks_execution_contract::AdvanceParticipationDomain,
            super::node_api::WorkerIngressResponsibility,
        )>() {
            return (closed(admission)
                && request.participation() == participation
                && request.envelope().operation_id() == operation)
                .then_some(true);
        }
        refusal
            .original
            .downcast_ref::<ParticipationRequestAdmissionRefusal>()
            .filter(|admission| closed(admission))
            .map(|_| false)
    }
    #[cfg(test)]
    pub(crate) fn dispose_closed_participation_domain_refusal_for_test(
        &self,
        participation: ParticipationRef,
        operation: novarocks_execution_contract::TaskOperationId,
        refusal: NodeHostRefusal,
    ) -> Result<(), NodeHostRefusal> {
        use super::participation_host::requests::ParticipationRequestAdmissionRefusal;
        use super::process_control::rejected_ingress::RejectedIngressPayload;
        let Some(has_original) =
            self.closed_participation_domain_refusal_for_test(participation, operation, &refusal)
        else {
            return Err(refusal);
        };
        let NodeHostRefusal {
            operation: stage,
            original,
        } = refusal;
        if !has_original {
            let neutral = original
                .downcast::<ParticipationRequestAdmissionRefusal>()
                .unwrap_or_else(|_| unreachable!("the inspected original neutral refusal"));
            drop(neutral);
            return Ok(());
        }
        let (admission, request, ingress) = *original
            .downcast::<(
                ParticipationRequestAdmissionRefusal,
                novarocks_execution_contract::AdvanceParticipationDomain,
                super::node_api::WorkerIngressResponsibility,
            )>()
            .unwrap_or_else(|_| unreachable!("the inspected original rich refusal"));
        match self.control.lock().adopt_rejected_ingress(
            participation,
            RejectedIngressPayload::ParticipationDomain(request),
            ingress,
        ) {
            Ok(()) => Ok(()),
            Err(original) => Err(NodeHostRefusal::new(stage, (admission, original))),
        }
    }
    /// Return a pre-R fixture refusal to the existing original Root disposal
    /// owner. The test must observe real cleanup before asserting its mismatch.
    #[cfg(test)]
    pub(crate) fn dispose_task_domain_refusal_for_test(
        &self,
        participation: ParticipationRef,
        refusal: NodeHostRefusal,
    ) -> Result<(), NodeHostRefusal> {
        use super::node_api::WorkerIngressResponsibility;
        use super::participation_host::requests::ParticipationRequestAdmissionRefusal;
        use super::process_control::rejected_ingress::RejectedIngressPayload;
        use novarocks_execution_contract::UpdateTask;
        if refusal.operation != "Task domain original R admission" {
            return Err(refusal);
        }
        let NodeHostRefusal {
            operation,
            original,
        } = refusal;
        let original = match original.downcast::<(
            ParticipationRequestAdmissionRefusal,
            UpdateTask,
            WorkerIngressResponsibility,
        )>() {
            Ok(original) => *original,
            Err(original) => {
                return Err(NodeHostRefusal {
                    operation,
                    original,
                });
            }
        };
        let (admission, request, ingress) = original;
        if admission.partial.is_some()
            || request
                .identity()
                .verify_participation(participation)
                .is_err()
        {
            return Err(NodeHostRefusal::new(
                operation,
                (admission, request, ingress),
            ));
        }
        match self.control.lock().adopt_rejected_ingress(
            participation,
            RejectedIngressPayload::TaskDomain(request),
            ingress,
        ) {
            Ok(()) => Ok(()),
            Err(original) => Err(NodeHostRefusal::new(operation, (admission, original))),
        }
    }
    #[cfg(test)]
    pub(crate) fn original_request_inventory_for_test(
        &self,
        participation: ParticipationRef,
    ) -> Option<Vec<String>> {
        let address = self.directory.participation(participation)?;
        let host = address.lock()?;
        Some(host.original_request_inventory_for_test())
    }
    #[cfg(test)]
    pub(crate) fn original_task_request_for_test(
        &self,
        participation: ParticipationRef,
        work: novarocks_control_runtime_core::identity::ScopedWorkRef,
    ) -> Option<(
        novarocks_worker_protocol::creation::RequestReplyTag,
        novarocks_execution_contract::TaskIdentity,
        bool,
        bool,
    )> {
        let address = self.directory.participation(participation)?;
        let host = address.lock()?;
        host.original_task_request_for_test(work)
    }
    /// RPC sources share the same checked process identity origin as Contexts
    /// and Tasks. This number is a routing address, never admission authority.
    pub(crate) fn next_source(&self) -> ActorId {
        self.actor_ids.next()
    }

    pub(crate) fn next_payload(&self) -> novarocks_worker_protocol::creation::PayloadId {
        novarocks_worker_protocol::creation::PayloadId(self.actor_ids.next().get())
    }

    pub(crate) fn admission_epoch_capability(
        &self,
    ) -> novarocks_execution_contract::AdmissionEpochCapability {
        self.control
            .lock()
            .resources()
            .admission_snapshot
            .borrow()
            .epoch
    }

    pub(crate) fn task_participation(
        &self,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<ParticipationRef> {
        self.directory.task_participation(task)
    }
    pub(crate) fn released_evidence(
        &self,
        participation: ParticipationRef,
    ) -> Result<crate::ReleasedParticipationEvidence, NodeHostRefusal> {
        let address = self.directory.participation(participation).ok_or_else(|| {
            NodeHostRefusal::new("Release evidence Participation address", participation)
        })?;
        let host = address.lock().ok_or_else(|| {
            NodeHostRefusal::new("Release evidence original Participation", participation)
        })?;
        host.host_jobs()
            .and_then(|jobs| jobs.released_evidence())
            .ok_or_else(|| {
                NodeHostRefusal::new("Release evidence original host result", participation)
            })
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn live_task_execution_control_for_test(
        &self,
        identity: novarocks_execution_contract::TaskIdentity,
    ) -> Option<Arc<novarocks_execution::runtime::task_control::TaskExecutionControl>> {
        let participation = self.directory.task_participation(identity)?;
        let address = self.directory.participation(participation)?;
        let mut host = address.lock()?;
        let owner = host.task(identity)?;
        if !owner.request_admission_open() {
            return None;
        }
        Some(Arc::new(
            novarocks_execution::runtime::task_control::TaskExecutionControl::new(
                identity,
                owner.port().execution_port(),
                novarocks_execution_contract::TaskFailurePhase::Execution,
            )
            .expect("same original held Task execution view"),
        ))
    }

    pub(crate) fn submit_task_read(
        &self,
        participation: ParticipationRef,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: novarocks_worker_protocol::participation::ParticipationCommand,
    ) -> Result<NodeTaskReadHandle, NodeHostRefusal> {
        use novarocks_worker_protocol::participation::ParticipationCommand;
        if !matches!(
            &command,
            ParticipationCommand::GetFinalInfo { .. }
                | ParticipationCommand::GetDynamicFilters { .. }
        ) {
            return Err(NodeHostRefusal::new("Task read command identity", command));
        }
        let task = command.task().expect("actual read command Task");
        if task.verify_participation(participation).is_err() {
            return Err(NodeHostRefusal::new(
                "Task read Participation identity",
                command,
            ));
        }
        let Some(address) = self.directory.participation(participation) else {
            return Err(NodeHostRefusal::new(
                "Task read Participation address",
                command,
            ));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new(
                "Task read Participation owner",
                command,
            ));
        };
        let awaiting_seal = host.task_read_awaits_seal(task);
        if host.has_retired_task_read(task) || awaiting_seal {
            let (work, handle) = host
                .admit_request(
                    &self.requests,
                    self.next_source(),
                    novarocks_control_runtime_core::mailbox::InputClass::Control,
                    operation,
                    deadline,
                )
                .map_err(|original| {
                    NodeHostRefusal::new("Task sealed read R admission", original)
                })?;
            let submission = if awaiting_seal {
                host.submit_deferred_task_read(work, command)
            } else {
                host.submit_retired_task_command(work, command)
            };
            match submission {
                Ok(()) => Ok(NodeTaskReadHandle::Sealed(handle)),
                Err(original) => Err(NodeHostRefusal::new(
                    "Task sealed read original handoff",
                    (original, handle),
                )),
            }
        } else {
            // No inferred retirement: this still requires the live original
            // Task's actual source/cut and its whole funding transfer.
            let (work, handle) = host
                .admit_task_request(
                    &self.requests,
                    task,
                    novarocks_control_runtime_core::mailbox::InputClass::Control,
                    operation,
                    deadline,
                )
                .map_err(|original| {
                    NodeHostRefusal::new("Task causal read R admission", original)
                })?;
            let read_payload = if matches!(&command, ParticipationCommand::GetDynamicFilters { .. })
            {
                Some(
                    host.attach_dynamic_filter_read(work, task)
                        .map_err(|original| {
                            NodeHostRefusal::new("Task RF original R read cell", original)
                        })?,
                )
            } else {
                None
            };
            match host.submit_request(work, command) {
                Ok(()) => Ok(NodeTaskReadHandle::Live(handle, read_payload)),
                Err(original) => Err(NodeHostRefusal::new(
                    "Task causal read original handoff",
                    (original, handle),
                )),
            }
        }
    }

    fn reject_failed_ingress(
        &self,
        participation: ParticipationRef,
        payload: super::process_control::rejected_ingress::RejectedIngressPayload,
        ingress: super::node_api::WorkerIngressResponsibility,
    ) -> NodeRequestRefusal {
        // The caller releases its Participation guard before this Root handoff.
        // Only the admitted original payload and Native responsibility move;
        // no Root RPC, reply cell, or second R is created.
        match self
            .control
            .lock()
            .adopt_rejected_ingress(participation, payload, ingress)
        {
            Ok(()) => NodeRequestRefusal::OwnerFailed,
            Err(original) => {
                NodeHostRefusal::new("failed ingress original disposal handoff", original).into()
            }
        }
    }
    fn refuse_closed_ingress(
        &self,
        participation: ParticipationRef,
        stage: &'static str,
        admission: super::participation_host::requests::ParticipationRequestAdmissionRefusal,
        payload: super::process_control::rejected_ingress::RejectedIngressPayload,
        ingress: super::node_api::WorkerIngressResponsibility,
    ) -> NodeRequestRefusal {
        use super::participation_host::requests::ParticipationRequestError;
        // No Participation R exists on this exact permission refusal. Do not revive
        // admission or project OwnerFailed: only the paid process cleanup
        // adopts the original rich input and Native arrival responsibility.
        if admission.partial.is_some()
            || !matches!(admission.error, ParticipationRequestError::AdmissionClosed)
        {
            return NodeHostRefusal::new(stage, (admission, payload, ingress)).into();
        }
        // Every caller has released its Participation guard before the Root handoff.
        let handoff = self
            .control
            .lock()
            .adopt_rejected_ingress(participation, payload, ingress);
        match handoff {
            Ok(()) => NodeRequestRefusal::Neutral(NeutralNodeRequestRefusal {
                operation: stage,
                _original_error: admission.error,
            }),
            Err(original) => NodeHostRefusal::new(stage, (admission, original)).into(),
        }
    }
    fn reject_failed_admission_partial(
        host: &mut super::participation_host::ParticipationHostRecord,
        admission: super::participation_host::requests::ParticipationRequestAdmissionRefusal,
        payload: super::process_control::rejected_ingress::RejectedIngressPayload,
        ingress: super::node_api::WorkerIngressResponsibility,
    ) -> NodeRequestRefusal {
        let Some(work) = admission.partial else {
            return NodeHostRefusal::new(
                "Rejected admission has no original partial",
                (admission, payload, ingress),
            )
            .into();
        };
        match host.capture_failed_admission_partial(work, payload, ingress) {
            Ok(()) => NodeRequestRefusal::OwnerFailed,
            Err(original) => NodeHostRefusal::new(
                "Rejected admission original partial capture",
                (admission, original),
            )
            .into(),
        }
    }
    #[cfg(test)]
    pub(crate) fn task_update_retirement_for_test(
        &self,
        participation: ParticipationRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<(bool, bool)> {
        task.verify_participation(participation).ok()?;
        let address = self.directory.participation(participation)?;
        let mut host = address.lock()?;
        Some((host.task(task).is_some(), host.has_retired_task_read(task)))
    }
    pub(crate) fn submit_task_domain(
        &self,
        participation: ParticipationRef,
        original: novarocks_execution_contract::UpdateTask,
        deadline: MonotonicInstant,
        ingress: super::node_api::WorkerIngressResponsibility,
    ) -> Result<super::participation_host::TaskRequestHandle, NodeRequestRefusal> {
        use super::participation_host::requests::ParticipationRequestError;
        use super::process_control::rejected_ingress::RejectedIngressPayload;
        if self.draining.load(Ordering::Acquire) || !self.tasks.intake_open() {
            return Err(
                NodeHostRefusal::new("Task domain intake is closed", (original, ingress)).into(),
            );
        }
        if original
            .identity()
            .verify_participation(participation)
            .is_err()
        {
            return Err(NodeHostRefusal::new(
                "Task domain Participation identity",
                (original, ingress),
            )
            .into());
        }
        let Some(address) = self.directory.participation(participation) else {
            return Err(NodeHostRefusal::new(
                "Task domain Participation address",
                (original, ingress),
            )
            .into());
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new(
                "Task domain Participation owner",
                (original, ingress),
            )
            .into());
        };
        let snapshot = match self.runtime.snapshot(host.actor()) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Task domain original owner gate",
                    (error, original, ingress),
                )
                .into());
            }
        };
        if snapshot.failed {
            drop(host);
            return Err(self.reject_failed_ingress(
                participation,
                RejectedIngressPayload::TaskDomain(original),
                ingress,
            ));
        }
        let live = match host.task_domain_uses_live_route(original.identity()) {
            Ok(live) => live,
            Err(ParticipationRequestError::OwnerFailed) => {
                drop(host);
                return Err(self.reject_failed_ingress(
                    participation,
                    RejectedIngressPayload::TaskDomain(original),
                    ingress,
                ));
            }
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Task domain original route",
                    (error, original, ingress),
                )
                .into());
            }
        };
        if !live {
            let task = original.identity();
            let (work, handle) = match host.admit_task_update_inspection(
                &self.requests,
                self.next_source(),
                task,
                original.envelope().operation_id(),
                deadline,
            ) {
                Ok(admitted) => admitted,
                Err(error)
                    if error.partial.is_none()
                        && matches!(error.error, ParticipationRequestError::OwnerFailed) =>
                {
                    drop(host);
                    return Err(self.reject_failed_ingress(
                        participation,
                        RejectedIngressPayload::TaskDomain(original),
                        ingress,
                    ));
                }
                Err(error) if error.partial.is_some() => {
                    return Err(Self::reject_failed_admission_partial(
                        &mut host,
                        error,
                        RejectedIngressPayload::TaskDomain(original),
                        ingress,
                    ));
                }
                Err(error)
                    if error.partial.is_none()
                        && matches!(error.error, ParticipationRequestError::AdmissionClosed) =>
                {
                    drop(host);
                    return Err(self.refuse_closed_ingress(
                        participation,
                        "Task domain original inspection R admission",
                        error,
                        RejectedIngressPayload::TaskDomain(original),
                        ingress,
                    ));
                }
                Err(error) => {
                    return Err(NodeHostRefusal::new(
                        "Task domain original inspection R admission",
                        (error, original, ingress),
                    )
                    .into());
                }
            };
            if let Err((error, original, ingress)) =
                host.capture_task_update_inspection(work, original, ingress)
            {
                return Err(NodeHostRefusal::new(
                    "Task domain original inspection capture",
                    (error, original, ingress, handle),
                )
                .into());
            }
            return match host.submit_task_update_inspection(work, task) {
                Ok(()) => Ok(handle),
                Err(original) => Err(NodeHostRefusal::new(
                    "Task domain original inspection handoff",
                    (original, handle),
                )
                .into()),
            };
        }
        let (work, handle) = match host.admit_task_domain_request(
            &self.requests,
            original.identity(),
            original.envelope().operation_id(),
            deadline,
        ) {
            Ok(admitted) => admitted,
            Err(error)
                if error.partial.is_none()
                    && matches!(error.error, ParticipationRequestError::OwnerFailed) =>
            {
                drop(host);
                return Err(self.reject_failed_ingress(
                    participation,
                    RejectedIngressPayload::TaskDomain(original),
                    ingress,
                ));
            }
            Err(error) if error.partial.is_some() => {
                return Err(Self::reject_failed_admission_partial(
                    &mut host,
                    error,
                    RejectedIngressPayload::TaskDomain(original),
                    ingress,
                ));
            }
            Err(error)
                if error.partial.is_none()
                    && matches!(error.error, ParticipationRequestError::AdmissionClosed) =>
            {
                drop(host);
                return Err(self.refuse_closed_ingress(
                    participation,
                    "Task domain original R admission",
                    error,
                    RejectedIngressPayload::TaskDomain(original),
                    ingress,
                ));
            }
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Task domain original R admission",
                    (error, original, ingress),
                )
                .into());
            }
        };
        match host.capture_task_domain(
            work,
            original,
            self.tasks
                .execution
                .as_ref()
                .expect("open Task domain execution port")
                .clone(),
            self.next_payload(),
        ) {
            Ok(()) => Ok(handle),
            Err((error, original)) => {
                match host.capture_refused_task_domain(work, original, ingress) {
                    Ok(()) => Ok(handle),
                    Err(original) => Err(NodeHostRefusal::new(
                        "Task domain original source capture",
                        (error, original, handle),
                    )
                    .into()),
                }
            }
        }
    }
    pub(crate) fn submit_participation_domain(
        &self,
        original: novarocks_execution_contract::AdvanceParticipationDomain,
        deadline: MonotonicInstant,
        ingress: super::node_api::WorkerIngressResponsibility,
    ) -> Result<super::participation_host::ParticipationRequestHandle, NodeRequestRefusal> {
        use super::participation_host::requests::ParticipationRequestError;
        use super::process_control::rejected_ingress::RejectedIngressPayload;
        if self.draining.load(Ordering::Acquire) || !self.tasks.intake_open() {
            return Err(NodeHostRefusal::new(
                "Participation domain intake is closed",
                (original, ingress),
            )
            .into());
        }
        let participation = original.participation();
        let Some(address) = self.directory.participation(participation) else {
            return Err(
                NodeHostRefusal::new("Participation domain address", (original, ingress)).into(),
            );
        };
        let Some(mut host) = address.lock() else {
            return Err(
                NodeHostRefusal::new("Participation domain owner", (original, ingress)).into(),
            );
        };
        let snapshot = match self.runtime.snapshot(host.actor()) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Participation domain original owner gate",
                    (error, original, ingress),
                )
                .into());
            }
        };
        if snapshot.failed {
            drop(host);
            return Err(self.reject_failed_ingress(
                participation,
                RejectedIngressPayload::ParticipationDomain(original),
                ingress,
            ));
        }
        let (work, handle) = match host.admit_request(
            &self.requests,
            self.next_source(),
            novarocks_control_runtime_core::mailbox::InputClass::Ordinary,
            original.envelope().operation_id(),
            deadline,
        ) {
            Ok(admitted) => admitted,
            Err(error)
                if error.partial.is_none()
                    && matches!(error.error, ParticipationRequestError::OwnerFailed) =>
            {
                drop(host);
                return Err(self.reject_failed_ingress(
                    participation,
                    RejectedIngressPayload::ParticipationDomain(original),
                    ingress,
                ));
            }
            Err(error) if error.partial.is_some() => {
                return Err(Self::reject_failed_admission_partial(
                    &mut host,
                    error,
                    RejectedIngressPayload::ParticipationDomain(original),
                    ingress,
                ));
            }
            Err(error)
                if error.partial.is_none()
                    && matches!(error.error, ParticipationRequestError::AdmissionClosed) =>
            {
                drop(host);
                return Err(self.refuse_closed_ingress(
                    participation,
                    "Participation domain original R admission",
                    error,
                    RejectedIngressPayload::ParticipationDomain(original),
                    ingress,
                ));
            }
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Participation domain original R admission",
                    (error, original, ingress),
                )
                .into());
            }
        };
        match host.capture_participation_domain(work, original, self.next_payload()) {
            Ok(()) => Ok(handle),
            Err((error, original)) => {
                match host.capture_refused_participation_domain(work, original, ingress) {
                    Ok(()) => Ok(handle),
                    Err(original) => Err(NodeHostRefusal::new(
                        "Participation domain original source capture",
                        (error, original, handle),
                    )
                    .into()),
                }
            }
        }
    }
    /// Task-identity cancellation is routed by the actual Worker directory.
    /// Missing ownership is a protocol refusal before R/Work admission, never
    /// a Cancel tombstone or a fabricated successful Task delivery.
    pub(crate) fn submit_cancel_task(
        &self,
        request: novarocks_execution_contract::CancelTask,
        deadline: MonotonicInstant,
    ) -> Result<NodeCancelHandle, NodeRequestRefusal> {
        use super::participation_host::requests::ParticipationRequestError;
        use novarocks_worker_protocol::participation::{ParticipationCommand, TaskControlRefusal};
        let task = request.identity();
        let Some(participation) = self.directory.task_participation(task) else {
            return Ok(NodeCancelHandle::Refused(TaskControlRefusal::NotOwned));
        };
        let Some(address) = self.directory.participation(participation) else {
            return Ok(NodeCancelHandle::Refused(TaskControlRefusal::NotOwned));
        };
        let Some(mut host) = address.lock() else {
            // This exact cell has already moved to its actual removed owner.
            return Ok(NodeCancelHandle::Refused(TaskControlRefusal::NotOwned));
        };
        let operation = request.envelope().operation_id();
        let command = ParticipationCommand::Cancel {
            task,
            reason: request.reason(),
        };
        if host.has_retired_task_read(task) {
            // The original physical retirement proof replaces the departed
            // producer; the same Participation Core still owns its replay answer.
            let (work, handle) = match host.admit_request(
                &self.requests,
                self.next_source(),
                novarocks_control_runtime_core::mailbox::InputClass::Control,
                operation,
                deadline,
            ) {
                Ok(original) => original,
                Err(original)
                    if original.partial.is_none()
                        && matches!(original.error, ParticipationRequestError::OwnerFailed) =>
                {
                    return Err(NodeRequestRefusal::OwnerFailed);
                }
                Err(original) => {
                    return Err(NodeHostRefusal::new(
                        "Cancel retained Task R admission",
                        (original, command),
                    )
                    .into());
                }
            };
            return match host.submit_retired_task_command(work, command) {
                Ok(()) => Ok(NodeCancelHandle::Participation(handle)),
                Err(original) => Err(NodeHostRefusal::new(
                    "Cancel retained Task original handoff",
                    (original, handle),
                )
                .into()),
            };
        }
        let (work, handle) = match host.admit_task_request(
            &self.requests,
            task,
            novarocks_control_runtime_core::mailbox::InputClass::Control,
            operation,
            deadline,
        ) {
            Ok(original) => original,
            Err(original)
                if original.partial.is_none()
                    && matches!(original.error, ParticipationRequestError::UnknownTask) =>
            {
                return Ok(NodeCancelHandle::Refused(TaskControlRefusal::NotOwned));
            }
            Err(original)
                if original.partial.is_none()
                    && matches!(original.error, ParticipationRequestError::OwnerFailed) =>
            {
                return Err(NodeRequestRefusal::OwnerFailed);
            }
            Err(original) => {
                return Err(NodeHostRefusal::new(
                    "Cancel Task original R admission",
                    (original, command),
                )
                .into());
            }
        };
        match host.submit_request(work, command) {
            Ok(()) => Ok(NodeCancelHandle::Task(handle)),
            Err(original) => Err(NodeHostRefusal::new(
                "Cancel Task original causal handoff",
                (original, handle),
            )
            .into()),
        }
    }

    #[cfg(test)]
    pub(crate) fn submit_task_command(
        &self,
        participation: ParticipationRef,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: novarocks_worker_protocol::participation::ParticipationCommand,
    ) -> Result<super::participation_host::TaskRequestHandle, NodeHostRefusal> {
        use novarocks_worker_protocol::participation::ParticipationCommand;
        if !matches!(
            &command,
            ParticipationCommand::Cancel { .. }
                | ParticipationCommand::GetFinalInfo { .. }
                | ParticipationCommand::GetDynamicFilters { .. }
        ) {
            return Err(NodeHostRefusal::new(
                "Task command requires its original source",
                command,
            ));
        }
        let task = command.task().expect("qualified Task command");
        if task.verify_participation(participation).is_err() {
            return Err(NodeHostRefusal::new(
                "Task command participation identity",
                command,
            ));
        }
        let Some(address) = self.directory.participation(participation) else {
            return Err(NodeHostRefusal::new(
                "Task command Participation address",
                command,
            ));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new(
                "Task command Participation owner",
                command,
            ));
        };
        let (work, handle) = match host.admit_task_request(
            &self.requests,
            task,
            novarocks_control_runtime_core::mailbox::InputClass::Control,
            operation,
            deadline,
        ) {
            Ok(original) => original,
            Err(original) => {
                return Err(NodeHostRefusal::new(
                    "Task command R admission",
                    (original, command),
                ));
            }
        };
        match host.submit_request(work, command) {
            Ok(()) => Ok(handle),
            Err(original) => Err(NodeHostRefusal::new(
                "Task command original causal handoff",
                (original, handle),
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn print_neutral_task_inventory(
        &self,
        participation: ParticipationRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) {
        if let Some(address) = self.directory.participation(participation)
            && let Some(mut host) = address.lock()
        {
            host.print_original_execution_inventory();
            host.print_neutral_task_io_inventory(task);
            if let Some(owner) = host.task(task) {
                owner.print_neutral_production_inventory();
            }
        }
    }
    /// Physical pool owners stay with process composition until all these
    /// original producers drain. Handles alone confer no new admission.
    pub(crate) fn from_started(
        mut root: ProcessControlOwner,
        participations: ParticipationExecutorHandles,
        tasks: TaskIoPorts,
        establish: EstablishExecutorHandles,
        factory: Arc<dyn ParticipationHostFactory>,
        config: NodeParticipationConfig,
        actor_ids: NodeActorIds,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, NodeHostRefusal> {
        if config.max_live_tasks != config.core.max_tasks
            || config.core.step_budget != ParticipationCore::MAX_STEP_WORK
            || tasks.lifecycle.is_none()
            || tasks.capacity.is_none()
            || tasks.observation.is_none()
            || tasks.protocol_events.is_none()
        {
            return Err(NodeHostRefusal::new(
                "node geometry",
                (
                    root,
                    participations,
                    tasks,
                    establish,
                    factory,
                    config,
                    actor_ids,
                    clock,
                ),
            ));
        }
        if let Some(signal) = &tasks.capacity {
            let original = match root.task_capacity_signal() {
                Ok(original) => original,
                Err(error) => {
                    return Err(NodeHostRefusal::new(
                        "Task capacity source",
                        (
                            error,
                            root,
                            participations,
                            tasks,
                            establish,
                            factory,
                            config,
                            actor_ids,
                            clock,
                        ),
                    ));
                }
            };
            if !original.same_signal(signal)
                || !original.same_signal(participations.capacity_signal())
            {
                return Err(NodeHostRefusal::new(
                    "Task capacity source origin",
                    (
                        root,
                        participations,
                        tasks,
                        establish,
                        factory,
                        config,
                        actor_ids,
                        clock,
                    ),
                ));
            }
        }
        if let Err(error) = root.enable_internal_source() {
            return Err(NodeHostRefusal::new(
                "Root internal source",
                (
                    error,
                    root,
                    participations,
                    tasks,
                    establish,
                    factory,
                    config,
                    actor_ids,
                    clock,
                ),
            ));
        }
        Ok(Self {
            clock,
            runtime: root.runtime().clone(),
            root_registration: root.registration().clone(),
            directory: root.service().directory(),
            requests: root.request_ready().clone(),
            metrics_ledger: root.ledger().clone(),
            poller_observation: OnceLock::new(),
            control: ControlHostCell::new(root),
            root_ready_scan: AtomicUsize::new(0),
            draining: AtomicBool::new(false),
            participations,
            tasks,
            establish,
            factory,
            config,
            actor_ids,
            rows: Mutex::new(ParticipationRows::default()),
        })
    }
    pub(crate) fn route(
        &self,
        participation: ParticipationRef,
    ) -> Option<PublishedSnapshot<ParticipationRoute>> {
        self.rows
            .lock()
            .expect("original Participation route rows")
            .participations
            .get(&participation)
            .map(|row| row.route.clone())
    }
    pub(crate) fn begin_drain(&self) -> Result<(), NodeHostRefusal> {
        self.control
            .lock()
            .begin_drain()
            .map_err(|original| NodeHostRefusal::new("Root admission drain", original))?;
        self.draining.store(true, Ordering::Release);
        self.runtime
            .notify_host_actor(&self.root_registration)
            .map_err(|original| NodeHostRefusal::new("Root drain wake", original))
    }
    pub(crate) fn confirm_ingress_closed(&self) -> Result<(), NodeHostRefusal> {
        self.control
            .lock()
            .confirm_ingress_closed()
            .map_err(|original| NodeHostRefusal::new("Root physical ingress closure", original))?;
        self.runtime
            .notify_host_actor(&self.root_registration)
            .map_err(|original| NodeHostRefusal::new("Root ingress closure wake", original))
    }
    /// An already admitted neutral request moves directly to the same Root.
    pub(crate) fn observe_ticket(
        &self,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        ticket: novarocks_execution_contract::AdmissionTicketId,
        participation: ParticipationRef,
        preflight: bool,
    ) -> Result<ControlRequestHandle, NodeHostRefusal> {
        let mut root = self.control.lock();
        let (input, handle) = root
            .admit_request(self.next_source(), operation, deadline)
            .map_err(|original| NodeHostRefusal::new("ticket observation admission", original))?;
        match root.submit_ticket_observation(input, ticket, participation, preflight) {
            Ok(()) => Ok(handle),
            Err(original) => Err(NodeHostRefusal::new(
                "ticket observation original handoff",
                (original, handle),
            )),
        }
    }

    pub(crate) fn submit_control(
        &self,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: ControlInput,
    ) -> Result<ControlRequestHandle, NodeHostRefusal> {
        let mut root = self.control.lock();
        let (input, handle) = match root.admit_request(source, operation, deadline) {
            Ok(admitted) => admitted,
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Root request admission",
                    (error, command),
                ));
            }
        };
        match root.submit_request(input, command) {
            Ok(()) => Ok(handle),
            Err(error) => Err(NodeHostRefusal::new(
                "Root request submission",
                (error, handle),
            )),
        }
    }
    pub(crate) fn begin_status_subscription(
        &self,
        participation: ParticipationRef,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        generation: u64,
        position: novarocks_worker_protocol::observation::CoveredSubscriptionRequest,
    ) -> Result<
        super::participation_host::requests::observation::ParticipationObservationStream,
        NodeHostRefusal,
    > {
        let Some(address) = self.directory.participation(participation) else {
            return Err(NodeHostRefusal::new(
                "status subscription Participation address",
                position,
            ));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new(
                "status subscription Participation owner",
                position,
            ));
        };
        let (work, handle) = host
            .begin_status_subscription(
                &self.requests,
                source,
                operation,
                deadline,
                generation,
                position,
            )
            .map_err(|original| {
                NodeHostRefusal::new("status subscription original R handoff", original)
            })?;
        drop(host);
        Ok(
            super::participation_host::requests::observation::ParticipationObservationStream::from_request(
                address, work, handle,
            ),
        )
    }

    pub(crate) fn submit_participation(
        &self,
        participation: ParticipationRef,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: novarocks_worker_protocol::participation::ParticipationCommand,
    ) -> Result<super::participation_host::ParticipationRequestHandle, NodeRequestRefusal> {
        self.submit_participation_selected(
            participation,
            source,
            operation,
            deadline,
            command,
            false,
        )
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn submit_participation_owner_panic(
        &self,
        participation: ParticipationRef,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: novarocks_worker_protocol::participation::ParticipationCommand,
    ) -> Result<super::participation_host::ParticipationRequestHandle, NodeRequestRefusal> {
        if !matches!(
            &command,
            novarocks_worker_protocol::participation::ParticipationCommand::Renew { .. }
        ) {
            return Err(NodeHostRefusal::new(
                "declared owner panic requires its original Renew",
                command,
            )
            .into());
        }
        self.submit_participation_selected(
            participation,
            source,
            operation,
            deadline,
            command,
            true,
        )
    }
    fn submit_participation_selected(
        &self,
        participation: ParticipationRef,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: novarocks_worker_protocol::participation::ParticipationCommand,
        _panic_on_renew: bool,
    ) -> Result<super::participation_host::ParticipationRequestHandle, NodeRequestRefusal> {
        use novarocks_worker_protocol::participation::ParticipationCommand;
        if !matches!(
            &command,
            ParticipationCommand::Renew { .. }
                | ParticipationCommand::Quiesce
                | ParticipationCommand::Abort { .. }
                | ParticipationCommand::Release
        ) {
            // Task-causal commands and rich updates enter through their
            // original paid source/cut owners, not this neutral control port.
            return Err(NodeHostRefusal::new(
                "Participation control command requires its original source",
                command,
            )
            .into());
        }
        let Some(address) = self.directory.participation(participation) else {
            return Err(NodeHostRefusal::new("Participation request address", command).into());
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new("Participation request owner", command).into());
        };
        let (work, handle) = match host.admit_request(
            &self.requests,
            source,
            novarocks_control_runtime_core::mailbox::InputClass::Control,
            operation,
            deadline,
        ) {
            Ok(original) => original,
            Err(error) => {
                if matches!(
                    error.error,
                    super::participation_host::ParticipationRequestError::OwnerFailed
                ) && error.partial.is_none()
                {
                    // Only these four plain control commands enter this port.
                    // No R, rich input or partial was admitted on this branch.
                    drop(host);
                    drop(command);
                    return Err(NodeRequestRefusal::OwnerFailed);
                }
                return Err(NodeHostRefusal::new(
                    "Participation request admission",
                    (error, command),
                )
                .into());
            }
        };
        #[cfg(novarocks_dev_controls)]
        let submitted = if _panic_on_renew {
            host.submit_request_owner_panic(work, command)
        } else {
            host.submit_request(work, command)
        };
        #[cfg(not(novarocks_dev_controls))]
        let submitted = host.submit_request(work, command);
        match submitted {
            Ok(()) => Ok(handle),
            Err(error) => {
                // A real Core failure can race the original admitted fill.
                // The same R already retains that refused neutral message;
                // its failure supervisor publishes and drains the same cell.
                if error.command.is_none()
                    && host
                        .retains_refused_participation_control(work)
                        .map_err(|error| {
                            NodeHostRefusal::new(
                                "Participation original refused control input",
                                error,
                            )
                        })?
                {
                    return Ok(handle);
                }
                Err(
                    NodeHostRefusal::new("Participation request submission", (error, handle))
                        .into(),
                )
            }
        }
    }
    /// The original Create payload is captured by its admitted R before the
    /// pure Core can elect a loader. No process-global payload lookup exists.
    pub(crate) fn submit_task_create(
        &self,
        source: ActorId,
        deadline: MonotonicInstant,
        payload: super::loading::TaskCreatePayload,
        ingress: super::node_api::WorkerIngressResponsibility,
    ) -> Result<NodeCreateHandle, NodeRequestRefusal> {
        use super::participation_host::requests::{
            ParticipationCreationInspectionRefusal, ParticipationRequestError,
        };
        use super::process_control::rejected_ingress::RejectedIngressPayload;
        if self.draining.load(Ordering::Acquire) || !self.tasks.intake_open() {
            return Err(NodeHostRefusal::new("Task intake is not open", (payload, ingress)).into());
        }
        let participation = payload.request.participation();
        let operation = payload.request.envelope().operation_id();
        let Some(address) = self.directory.participation(participation) else {
            return self
                .create_before_participation(source, deadline, payload)
                .map_err(Into::into);
        };
        let Some(mut host) = address.lock() else {
            return self
                .create_before_participation(source, deadline, payload)
                .map_err(Into::into);
        };
        let admission = match self.runtime.snapshot(host.actor()) {
            Ok(snapshot) => snapshot,
            Err(original) => {
                return Err(NodeHostRefusal::new(
                    "Create original Participation gate",
                    (original, payload, ingress),
                )
                .into());
            }
        };
        if admission.failed {
            drop(host);
            return Err(self.reject_failed_ingress(
                participation,
                RejectedIngressPayload::Create {
                    payload,
                    binding: None,
                },
                ingress,
            ));
        }
        if !admission.external_open {
            let inspection = host.inspect_creation(
                &self.requests,
                source,
                operation,
                deadline,
                payload.request.identity(),
            );
            return match inspection {
                Ok(handle) => {
                    // The actual Native caller still owns this unadmitted
                    // carrier. Its foreign Drop runs after the owner unlocks.
                    drop(host);
                    drop(payload);
                    Ok(NodeCreateHandle::Participation(handle))
                }
                Err(ParticipationCreationInspectionRefusal::Admission(error))
                    if error.partial.is_none()
                        && matches!(error.error, ParticipationRequestError::OwnerFailed) =>
                {
                    drop(host);
                    Err(self.reject_failed_ingress(
                        participation,
                        RejectedIngressPayload::Create {
                            payload,
                            binding: None,
                        },
                        ingress,
                    ))
                }
                Err(ParticipationCreationInspectionRefusal::Admission(error))
                    if error.partial.is_some() =>
                {
                    Err(Self::reject_failed_admission_partial(
                        &mut host,
                        error,
                        RejectedIngressPayload::Create {
                            payload,
                            binding: None,
                        },
                        ingress,
                    ))
                }
                Err(original) => Err(NodeHostRefusal::new(
                    "Create closed inspection",
                    (original, payload, ingress),
                )
                .into()),
            };
        }
        let protocol_generation = match host.scope().gates() {
            Ok(gates) => gates.generation,
            Err(original) => {
                return Err(NodeHostRefusal::new(
                    "Create scope generation",
                    (original, payload, ingress),
                )
                .into());
            }
        };
        let binding = super::task::TaskHostBinding {
            participation,
            registration: host.registration().clone(),
            task_source: self.actor_ids.next(),
            protocol_generation,
            max_cuts: self.config.policy.max_task_cuts,
            pending: Arc::new(NodeTaskNotice(host.ready_notice().clone())),
            health: self
                .tasks
                .protocol_events
                .as_ref()
                .expect("original role execution ports were validated at Node startup")
                .execution_health(),
        };
        let (work, handle) = match host.admit_request(
            &self.requests,
            source,
            novarocks_control_runtime_core::mailbox::InputClass::Ordinary,
            operation,
            deadline,
        ) {
            Ok(original) => original,
            Err(error)
                if error.partial.is_none()
                    && matches!(error.error, ParticipationRequestError::OwnerFailed) =>
            {
                drop(host);
                return Err(self.reject_failed_ingress(
                    participation,
                    RejectedIngressPayload::Create {
                        payload,
                        binding: Some(binding),
                    },
                    ingress,
                ));
            }
            Err(error) if error.partial.is_some() => {
                return Err(Self::reject_failed_admission_partial(
                    &mut host,
                    error,
                    RejectedIngressPayload::Create {
                        payload,
                        binding: Some(binding),
                    },
                    ingress,
                ));
            }
            Err(error) => {
                tracing::warn!(error = ?error.error, "Create original request admission refused");
                return Err(NodeHostRefusal::new(
                    "Create request admission",
                    (error, payload, binding, ingress),
                )
                .into());
            }
        };
        match host.submit_task_create(work, payload, binding) {
            Ok(()) => Ok(NodeCreateHandle::Participation(handle)),
            Err(original) => {
                if original.originals.is_none() {
                    match host.retains_refused_create_input(work) {
                        Ok(true) => {
                            return match host.capture_refused_create_ingress(work, ingress) {
                                Ok(()) => Ok(NodeCreateHandle::Participation(handle)),
                                Err((error, ingress)) => Err(NodeHostRefusal::new(
                                    "Create original refused ingress capture",
                                    (error, original, handle, ingress),
                                )
                                .into()),
                            };
                        }
                        Ok(false) => {}
                        Err(error) => {
                            return Err(NodeHostRefusal::new(
                                "Create original refused input proof",
                                (error, original, handle, ingress),
                            )
                            .into());
                        }
                    }
                }
                tracing::warn!(error = ?original.error, "Create original source handoff refused");
                Err(NodeHostRefusal::new(
                    "Create original source handoff",
                    (original, handle, ingress),
                )
                .into())
            }
        }
    }
    fn create_before_participation(
        &self,
        source: ActorId,
        deadline: MonotonicInstant,
        payload: super::loading::TaskCreatePayload,
    ) -> Result<NodeCreateHandle, NodeHostRefusal> {
        let participation = payload.request.participation();
        let task = payload.request.identity();
        let operation = payload.request.envelope().operation_id();
        tracing::debug!(participation = ?participation, "Create awaits its original Participation address");
        // No Task has accepted this decoded input. The original caller owns
        // its physical disposal, outside all owner locks. Only the actual
        // Root R decides whether the request can retry or the owner is closed.
        drop(payload);
        self.submit_control(
            source,
            operation,
            deadline,
            ControlInput::CreateWithoutParticipationAddress {
                participation,
                task,
            },
        )
        .map(NodeCreateHandle::BeforeParticipation)
    }
    /// Resolve the exact original Participation, then admit the data holder and R
    /// before lending the channel. The returned read executes outside this
    /// metadata guard; Root's channel lock orders it against real sealing.
    pub(crate) fn begin_root_read(
        &self,
        participation: ParticipationRef,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        request: &novarocks_execution_contract::root_result::RootResultRead,
    ) -> Result<super::participation_host::NodeRootRead, super::participation_host::RootReadError>
    {
        let Some(address) = self.directory.participation(participation) else {
            return Err(super::participation_host::RootReadError::UnknownParticipation);
        };
        let Some(mut host) = address.lock() else {
            return Err(super::participation_host::RootReadError::ParticipationRemoved);
        };
        host.begin_root_read(&self.requests, source, operation, deadline, request)
    }
    /// The raw source enters the same original R before any getter is called.
    /// Its physical getter return drives submission; the poller never calls it.
    pub(crate) fn submit_establish(
        &self,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        raw: novarocks_execution_contract::EstablishParticipation,
        payload: novarocks_worker_protocol::creation::PayloadId,
    ) -> Result<ControlRequestHandle, NodeHostRefusal> {
        let mut root = self.control.lock();
        let (input, handle) = match root.admit_establish_request(source, operation, deadline) {
            Ok(admitted) => admitted,
            Err(error) => return Err(NodeHostRefusal::new("Establish admission", (error, raw))),
        };
        let job = match root.begin_establish_ingress(input, raw, payload) {
            Ok(job) => job,
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Establish source custody",
                    (error, handle),
                ));
            }
        };
        if let Err(original) = root.stage_establish_getter(input, job) {
            return Err(NodeHostRefusal::new(
                "Establish getter staging",
                (original, handle),
            ));
        }
        Ok(handle)
    }
    pub(crate) fn start_pollers(
        self: &Arc<Self>,
        workers: usize,
    ) -> Result<PollerPool<WorkerActor>, std::io::Error> {
        let node = self.clone();
        PollerPool::start_with_host(
            self.runtime.clone(),
            self.clock.clone(),
            workers,
            move |now, turn| {
                if let Err(original) = node.advance(now, turn, 8) {
                    tracing::error!(failure = ?original, "Worker host driver invariant failed");
                    // Preserve the fatal boundary in standalone integration
                    // tests too, where no tracing subscriber may be installed.
                    eprintln!("Worker host driver invariant failed: {original:?}");
                    // Reaching this boundary is a fixed driver invariant failure.
                    // Preserve the original refused captures across process exit.
                    std::mem::forget(original);
                    std::process::abort();
                }
            },
        )
    }
    pub(crate) fn advance(
        &self,
        _now: Tick,
        turn: ActorHostTurn<WorkerActor>,
        budget: usize,
    ) -> Result<NodeHostProgress, NodeHostRefusal> {
        if turn.matches_registration(&self.root_registration) {
            for notice in turn.take_header_readiness(budget) {
                self.control
                    .lock()
                    .notify_request_header_readiness(&notice)
                    .map_err(|error| {
                        NodeHostRefusal::new("Root original header readiness", error)
                    })?;
            }
            match self.advance_root(&turn, budget) {
                Ok(mut progress) => {
                    let rows_empty = self
                        .rows
                        .lock()
                        .expect("original Participation route rows")
                        .participations
                        .is_empty();
                    if self.draining.load(Ordering::Acquire) && rows_empty {
                        let drain = self.control.lock().advance_drain().map_err(|original| {
                            NodeHostRefusal::new("Root metadata drain", original)
                        })?;
                        progress.runnable |= drain.runnable;
                        if drain.ready {
                            let pure =
                                self.control.lock().finish_drain(turn).map_err(|original| {
                                    NodeHostRefusal::new(
                                        "Root physical metadata teardown",
                                        original,
                                    )
                                })?;
                            drop(pure);
                            // Root removal precedes the last Scope removal.
                            // Publish a change after both actual inventories
                            // have exited so a draining join cannot miss it.
                            self.runtime.notify_pollers();
                            return Ok(progress);
                        }
                    }
                    turn.release(progress.runnable).map_err(|original| {
                        NodeHostRefusal::new("Root host turn release", original)
                    })?;
                    Ok(progress)
                }
                Err(original) => Err(NodeHostRefusal::new("Root host turn", (turn, original))),
            }
        } else {
            self.advance_participation(turn, budget)
        }
    }
    fn advance_root(
        &self,
        turn: &ActorHostTurn<WorkerActor>,
        budget: usize,
    ) -> Result<NodeHostProgress, NodeHostRefusal> {
        let mut progress = NodeHostProgress::default();
        if budget == 0 {
            return Ok(progress);
        }
        let partials = self
            .control
            .lock()
            .advance_request_partials(budget)
            .map_err(|original| {
                NodeHostRefusal::new("Root original request partial retirement", original)
            })?;
        progress.runnable |= partials.runnable;
        let indexed = self
            .control
            .lock()
            .index_control_effects((ControlCore::MAX_STEP_WORK + 1).saturating_mul(budget))
            .map_err(|original| NodeHostRefusal::new("Root effect index", original))?;
        // Only the original Common Root turn updates this scan. The count is
        // routing metadata, never an action/Work/exit authority. A bounded
        // NotReady prefix must neither hide its ready tail nor spin forever.
        let mut remaining = self.root_ready_scan.load(Ordering::Relaxed);
        if indexed.units > 0 || remaining == 0 {
            remaining = self.control.lock().resources().dispatch.ready_positions();
        }
        for _ in 0..budget {
            let positions = self.control.lock().resources().dispatch.ready_positions();
            let allowance = remaining.min(positions).min(ControlCore::MAX_STEP_WORK);
            if allowance == 0 {
                remaining = 0;
                break;
            }
            let ticket = self
                .control
                .lock()
                .take_control_effect(allowance)
                .map_err(|original| NodeHostRefusal::new("Root effect dispatch", original))?;
            let Some(ticket) = ticket else {
                remaining -= allowance;
                break;
            };
            self.handoff_root_effect(ticket)?;
            progress.root_effects += 1;
            remaining = self.control.lock().resources().dispatch.ready_positions();
        }
        self.root_ready_scan.store(remaining, Ordering::Relaxed);
        let io = self
            .control
            .advance_ready(
                turn,
                &self.establish,
                self.tasks.protocol_events.as_ref(),
                budget,
            )
            .map_err(|original| NodeHostRefusal::new("Root source/route progress", original))?;
        let mut root = self.control.lock();
        root.advance_internal()
            .map_err(|original| NodeHostRefusal::new("Root internal progress", original))?;
        progress.runnable |= remaining > 0
            || progress.root_effects == budget
            || io.runnable
            || root
                .resources()
                .dispatch
                .has_unindexed(&self.runtime)
                .map_err(|original| NodeHostRefusal::new("Root committed tail", original))?;
        drop(root);
        progress.runnable |= self.advance_parent_rows(budget)?;
        if let Some(signal) = &self.tasks.capacity {
            progress.runnable |= signal.drain_pending(budget).map_err(|original| {
                NodeHostRefusal::new("Task physical capacity progress", original)
            })?;
        }
        if let Some(input) = self
            .runtime
            .pending_timer_input(self.root_registration.actor())
            .map_err(|original| NodeHostRefusal::new("Root original timer input", original))?
        {
            match self
                .runtime
                .retire_input(self.root_registration.actor(), input)
            {
                Ok(())
                | Err(
                    novarocks_control_runtime::actor_turn::ActorTurnError::InputStillQueued
                    | novarocks_control_runtime::actor_turn::ActorTurnError::EffectsRemain,
                ) => {}
                Err(original) => {
                    return Err(NodeHostRefusal::new("Root timer retirement", original));
                }
            }
        }
        Ok(progress)
    }
    fn complete_root(
        &self,
        ticket: novarocks_control_runtime::actor_turn::EffectTicket<WorkerAction>,
    ) -> Result<(), NodeHostRefusal> {
        let ordered = match self
            .control
            .lock()
            .resources()
            .dispatch
            .requires_completion(&ticket)
        {
            Ok(ordered) => ordered,
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Root original dispatch policy",
                    (error, ticket),
                ));
            }
        };
        let completed = self
            .runtime
            .effect_completed_with_receipt(ticket)
            .map_err(|original| NodeHostRefusal::new("Root effect completion", original))?;
        let mut root = self.control.lock();
        if ordered {
            root.control_completion_observed_with_receipt(&completed)
                .map_err(|original| NodeHostRefusal::new("Root ordered completion", original))?;
        } else {
            root.note_control_input_progress(completed.input());
        }
        Ok(())
    }
    fn handoff_root_effect(
        &self,
        ticket: novarocks_control_runtime::actor_turn::EffectTicket<WorkerAction>,
    ) -> Result<(), NodeHostRefusal> {
        // Only frozen neutral facts are cloned; no rich source enters this switch.
        match ticket.effect.clone() {
            WorkerAction::AdmissionEpoch(epoch) => {
                let projection = self.control.lock().resources().admission_snapshot.clone();
                if let Err((error, original)) =
                    self.runtime
                        .commit_snapshot(&ticket, &projection, Arc::new(epoch))
                {
                    return Err(NodeHostRefusal::new(
                        "admission epoch publication",
                        (error, original, ticket),
                    ));
                }
                self.complete_root(ticket)
            }
            WorkerAction::ControlReply(_) => self
                .control
                .lock()
                .publish_request_reply(ticket)
                .map_err(|original| NodeHostRefusal::new("Root reply publication", original)),
            WorkerAction::Control {
                effect:
                    ControlEffect::AdmissionReply { .. }
                    | ControlEffect::DomainFailureReply { .. }
                    | ControlEffect::Reply { .. },
                admitted: None,
            } => {
                // The Core owns ticket metadata. This marker borrows its
                // original neutral receipt; the following ControlReply owns
                // delivery, without another ticket Work or holder inventory.
                self.complete_root(ticket)
            }
            WorkerAction::Control {
                effect:
                    ControlEffect::Materialize {
                        participation,
                        establishment,
                        generation,
                        reservation,
                    },
                ..
            } => {
                let core = match ParticipationCore::new(
                    participation,
                    generation,
                    establishment,
                    self.config.core,
                ) {
                    Ok(core) => core,
                    Err(error) => {
                        return Err(NodeHostRefusal::new(
                            "Participation configuration",
                            (error, ticket),
                        ));
                    }
                };
                let initial_route = PublishedSnapshot::new(
                    &self.control.lock().resources().scope,
                    Arc::new(ParticipationRoute {
                        state: ParticipationState::Establishing,
                        generation,
                    }),
                );
                {
                    let mut rows = self.rows.lock().expect("original Participation route rows");
                    if rows.participations.contains_key(&participation)
                        || rows.reservations.contains_key(&reservation)
                    {
                        return Err(NodeHostRefusal::new("Participation row geometry", ticket));
                    }
                    // Materialize is the core's original Establishing election.
                    // Subsequent state changes require actual Route publication.
                    rows.participations.insert(
                        participation,
                        ParticipationRow {
                            reservation,
                            removed: None,
                            retire_requested: false,
                            route: initial_route,
                        },
                    );
                    rows.reservations.insert(reservation, participation);
                }
                if let Err(error) = self.control.lock().adopt_participation(
                    &ticket,
                    ParticipationHostBinding {
                        actor: self.actor_ids.next(),
                        core,
                        policy: self.config.policy,
                        storage: self.config.storage.clone(),
                        max_live_tasks: self.config.max_live_tasks,
                        max_live_roots: self.config.max_live_roots,
                    },
                ) {
                    return Err(NodeHostRefusal::new(
                        "Participation bootstrap",
                        (error, ticket),
                    ));
                }
                let target = self
                    .directory
                    .participation(participation)
                    .expect("same adopted Participation address");
                tracing::debug!(participation = ?participation, "Worker Node adopted original Participation address");
                self.control
                    .handoff_materialization(ticket, target, self.factory.clone())
                    .map_err(|original| {
                        NodeHostRefusal::new("Participation materialization handoff", original)
                    })
            }
            WorkerAction::Control {
                effect:
                    ControlEffect::Route {
                        participation,
                        state,
                        generation,
                    },
                ..
            } => {
                let route = self
                    .route(participation)
                    .expect("Route retains its original paid row");
                if let Err(original) = self.runtime.commit_snapshot(
                    &ticket,
                    &route,
                    Arc::new(ParticipationRoute { state, generation }),
                ) {
                    return Err(NodeHostRefusal::new(
                        "Participation route publication",
                        (original, ticket),
                    ));
                }
                if let Err(error) = self.control.lock().control_publication_committed(&ticket) {
                    return Err(NodeHostRefusal::new(
                        "Participation route ordering",
                        (error, ticket),
                    ));
                }
                self.complete_root(ticket)
            }
            WorkerAction::Control {
                effect: ControlEffect::RouteEstablish { .. },
                ..
            } => self
                .control
                .lock()
                .handoff_establish_route(ticket)
                .map_err(|original| NodeHostRefusal::new("Establish route handoff", original)),
            WorkerAction::Control {
                effect: ControlEffect::CancelEstablishRoute { .. },
                ..
            } => self
                .control
                .lock()
                .handoff_establish_route_cancel(ticket)
                .map_err(|original| NodeHostRefusal::new("Establish route cancellation", original)),
            WorkerAction::Control {
                effect: ControlEffect::WakeInternal,
                ..
            } => self
                .control
                .lock()
                .handoff_internal_wake(ticket)
                .map_err(|original| NodeHostRefusal::new("Root continuation handoff", original)),
            WorkerAction::Control {
                effect: ControlEffect::AbortParticipation { .. },
                ..
            } => {
                if let Err(error) = self.directory.handoff_owner_fence(&ticket) {
                    return Err(NodeHostRefusal::new(
                        "Participation owner fence",
                        (error, ticket),
                    ));
                }
                self.complete_root(ticket)
            }
            WorkerAction::Control {
                effect: ControlEffect::RemoveHost { participation },
                ..
            } => {
                let Some(target) = self.directory.participation(participation) else {
                    return Err(NodeHostRefusal::new(
                        "Participation removal address",
                        ticket,
                    ));
                };
                let notice = {
                    let host = target.lock().ok_or_else(|| {
                        NodeHostRefusal::new("Participation removal original owner", participation)
                    })?;
                    host.ready_notice().clone()
                };
                let Some(execution) = self.tasks.execution.as_ref().cloned() else {
                    return Err(NodeHostRefusal::new(
                        "Participation removal execution host",
                        ticket,
                    ));
                };
                let original = {
                    let root = self.control.lock();
                    super::participation_removal::ParticipationRemovalOwner::from_actual_effect(
                        self.runtime.clone(),
                        self.root_registration.clone(),
                        &root.resources().metadata_resources[0].work,
                        &ticket,
                        participation,
                        notice,
                        execution,
                    )
                };
                let (owner, job) = match original {
                    Ok(original) => original,
                    Err(refusal) => {
                        return Err(NodeHostRefusal::new(
                            "Participation removal original admission",
                            (refusal, ticket),
                        ));
                    }
                };
                let mut host = target.lock().ok_or_else(|| {
                    NodeHostRefusal::new("Participation removal owner disappeared", participation)
                })?;
                if let Err(error) = host.authorize_owner_control_removal(&ticket) {
                    return Err(NodeHostRefusal::new(
                        "Participation removal intake fence",
                        (error, owner, job, ticket),
                    ));
                }
                if let Err(original) = host.stage_removal(owner, job) {
                    return Err(NodeHostRefusal::new(
                        "Participation removal custody handoff",
                        (original, ticket),
                    ));
                }
                self.runtime
                    .close_actor_admission(target.registration().actor())
                    .map_err(|e| NodeHostRefusal::new("Participation removal mailbox fence", e))?;
                self.runtime
                    .close_timer_source(target.registration().actor())
                    .map_err(|e| NodeHostRefusal::new("Participation removal timer closure", e))?;
                drop(host);
                self.complete_root(ticket)
            }
            WorkerAction::Control {
                effect: ControlEffect::RetireReservation(reservation),
                ..
            } => {
                let participation = self
                    .rows
                    .lock()
                    .expect("original Participation route rows")
                    .reservations
                    .get(&reservation)
                    .copied();
                if let Some(participation) = participation {
                    // Never wait for a Participation while holding the route rows.
                    // The actual child-to-parent transfer holds its cell
                    // through insertion, so None here implies the original
                    // has already reached that row rather than vanished.
                    let live = self
                        .directory
                        .participation(participation)
                        .and_then(|target| {
                            target
                                .lock()
                                .map(|mut host| host.authorize_parent_retirement(&ticket))
                        });
                    let mut rows = self.rows.lock().expect("original Participation route rows");
                    let row = rows
                        .participations
                        .get_mut(&participation)
                        .expect("original reservation row");
                    let result = match live {
                        Some(result) => result,
                        None => {
                            let Some(host) = row.removed.as_mut() else {
                                return Err(NodeHostRefusal::new(
                                    "Participation parent retirement owner",
                                    ticket,
                                ));
                            };
                            host.authorize_parent_retirement(&ticket)
                        }
                    };
                    if let Err(error) = result {
                        return Err(NodeHostRefusal::new(
                            "Participation active charge retirement",
                            (error, ticket),
                        ));
                    }
                    row.retire_requested = true;
                } else {
                    return Err(NodeHostRefusal::new(
                        "Participation reservation retirement owner missing",
                        ticket,
                    ));
                }
                self.complete_root(ticket)
            }
            // Continuation hints are consumed by the pure actor projection.
            // A Participation action or Root failure is never a default host ACK.
            _ => Err(NodeHostRefusal::new("unexpected Root action", ticket)),
        }
    }
    fn advance_parent_rows(&self, budget: usize) -> Result<bool, NodeHostRefusal> {
        // This finite Root turn acquires its owner before the routing rows.
        // Removed Participation tails contain no live Participation lock or foreign call.
        let mut root = self.control.lock();
        let mut rows = self.rows.lock().expect("original Participation route rows");
        let mut runnable = false;
        // Freeze this pass's actual length; a not-yet-decided Root input
        // cannot keep a poller spinning around the same metadata address.
        let visits = rows.removed_ready.len().min(budget);
        for _ in 0..visits {
            let participation = rows
                .removed_ready
                .pop_front()
                .expect("actual removed owner");
            let row = rows
                .participations
                .get_mut(&participation)
                .expect("same removed Participation row");
            if !row.retire_requested {
                rows.removed_ready.push_back(participation);
                continue;
            }
            let retired = row
                .removed
                .as_mut()
                .expect("original parent owner")
                .advance_removed_root_tail(&mut root)
                .map_err(|original| {
                    NodeHostRefusal::new("Participation original removal tail", original)
                })?;
            if retired {
                let row = rows
                    .participations
                    .remove(&participation)
                    .expect("same completed parent");
                assert_eq!(
                    rows.reservations.remove(&row.reservation),
                    Some(participation)
                );
                // Its rich holder and original children have already exited.
                drop(row);
            } else {
                rows.removed_ready.push_back(participation);
                runnable = true;
            }
        }
        Ok(runnable || rows.removed_ready.len() > visits)
    }
    fn advance_participation(
        &self,
        mut turn: ActorHostTurn<WorkerActor>,
        budget: usize,
    ) -> Result<NodeHostProgress, NodeHostRefusal> {
        let Some(address) = self.directory.host_turn(&turn) else {
            return Err(NodeHostRefusal::new(
                "original Participation host turn",
                turn,
            ));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new(
                "removed Participation host turn",
                turn,
            ));
        };
        for notice in turn.take_header_readiness(budget) {
            host.notify_header_readiness(&notice);
        }
        let cold_proof = host.cold_failure_admission_needed().map_err(|original| {
            NodeHostRefusal::new("Participation actual cold failure proof", original)
        })?;
        if let Some(proof) = cold_proof {
            drop(host);
            let admission = {
                let root = self.control.lock();
                super::participation_host::failure::cold::ColdFailureAdmission::admit(
                    &self.runtime,
                    &self.root_registration,
                    root.lifetime_author(),
                    &proof,
                )
            }
            .map_err(|original| {
                NodeHostRefusal::new("Participation original cold failure Cleanup", original)
            })?;
            host = match address.lock() {
                Some(host) => host,
                None => {
                    return Err(NodeHostRefusal::new(
                        "Participation cold failure install disappeared",
                        admission,
                    ));
                }
            };
            if let Err(original) = host.install_cold_failure(admission) {
                return Err(NodeHostRefusal::new(
                    "Participation original cold failure install",
                    original,
                ));
            }
        }
        let metadata_runnable =
            host.advance_lifecycle_metadata(&mut turn, budget)
                .map_err(|original| {
                    NodeHostRefusal::new("Participation original lifecycle metadata", original)
                })?;
        let removal_runnable = host.advance_removal(&self.tasks).map_err(|original| {
            NodeHostRefusal::new("Participation actual forget progress", original)
        })?;
        let advanced = match host.advance_io_handoff(
            &self.participations,
            &self.requests,
            &self.tasks,
            budget,
        ) {
            Ok(advanced) => advanced,
            Err(original) => {
                return Err(NodeHostRefusal::new(
                    "Participation production turn",
                    (original, turn),
                ));
            }
        };
        let detached = host
            .try_detach_execution(&mut turn, self.clock.as_ref())
            .map_err(|original| {
                NodeHostRefusal::new("Participation actual execution detachment", original)
            })?;
        // Only neutral proof/fact/notice leave this Participation borrow. Root's
        // original internal Source serializes delivery without a second R.
        let intent = if self
            .runtime
            .snapshot(address.registration().actor())
            .map_err(|original| {
                NodeHostRefusal::new("Participation custodian failure state", original)
            })?
            .failed
        {
            let proof = self
                .runtime
                .failure_proof_for(address.registration())
                .map_err(|original| {
                    NodeHostRefusal::new("Participation custodian original proof", original)
                })?;
            let notice = host.ready_notice().clone();
            let now =
                MonotonicInstant::from_origin(std::time::Duration::from_nanos(self.clock.now().0));
            let fact = host
                .failed_lifecycle_intent(&proof, now)
                .map_err(|original| {
                    NodeHostRefusal::new("Participation custodian lifecycle intent", original)
                })?;
            fact.map(|fact| (proof, fact, notice))
        } else {
            None
        };
        if let Some((proof, fact, notice)) = intent {
            drop(host);
            let receipt = self
                .control
                .lock()
                .enqueue_failed_lifecycle(&proof, fact, notice)
                .map_err(|original| {
                    NodeHostRefusal::new("Root original failed lifecycle Source", original)
                })?;
            host = match address.lock() {
                Some(host) => host,
                None => {
                    return Err(NodeHostRefusal::new(
                        "Participation failed lifecycle receipt disappeared",
                        receipt,
                    ));
                }
            };
            host.install_failed_lifecycle_receipt(&proof, fact, receipt)
                .map_err(|original| {
                    NodeHostRefusal::new(
                        "Participation original failed lifecycle receipt",
                        original,
                    )
                })?;
        }
        let parent_runnable = if host.parent_retirement_ready() {
            !host.advance_parent_teardown().map_err(|original| {
                NodeHostRefusal::new("Participation hot parent retirement", original)
            })?
        } else {
            false
        };
        let progress = NodeHostProgress {
            runnable: advanced.runnable
                || metadata_runnable
                || detached
                || parent_runnable
                || removal_runnable,
            ..Default::default()
        };
        if advanced.metadata_teardown_ready {
            let pure = host.finish_metadata_teardown(turn).map_err(|original| {
                NodeHostRefusal::new("Participation physical metadata teardown", original)
            })?;
            let mut rows = self.rows.lock().expect("original Participation route rows");
            let row = rows
                .participations
                .get_mut(&address.participation())
                .expect("same original Participation row");
            assert!(row.removed.replace(host.take_for_parent()).is_none());
            drop(rows);
            drop(host);
            drop(pure);
            self.directory.remove_original(&address);
            self.rows
                .lock()
                .expect("original Participation route rows")
                .removed_ready
                .push_back(address.participation());
            self.runtime
                .notify_host_actor(&self.root_registration)
                .map_err(|original| {
                    NodeHostRefusal::new("Root parent retirement wake", original)
                })?;
        } else {
            drop(host);
            turn.release(progress.runnable).map_err(|original| {
                NodeHostRefusal::new("Participation host turn release", original)
            })?;
        }
        Ok(progress)
    }
}

impl WorkerNodeHost {
    pub(crate) fn trace_drain_inventory(&self) {
        if !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }
        let participation = self
            .rows
            .lock()
            .expect("original Participation route rows")
            .participations
            .first_key_value()
            .map(|(&participation, _)| participation);
        if let Some(address) =
            participation.and_then(|participation| self.directory.participation(participation))
            && let Some(host) = address.lock()
        {
            // A bounded diagnostic prefix, never an exit predicate or a
            // request-path global owner scan.
            host.trace_drain_inventory(8);
        }
    }
    pub(crate) fn try_preparation_snapshot(
        &self,
    ) -> Result<
        Option<super::node_metrics::WorkerPreparationSnapshot>,
        super::node_metrics::WorkerPreparationObservationError,
    > {
        let original =
            self.tasks.preparation.as_ref().ok_or(
                super::node_metrics::WorkerPreparationObservationError::MissingPhysicalOwner,
            )?;
        super::node_metrics::WorkerPreparationSnapshot::sample(
            &self.metrics_ledger,
            original,
            self.config.core.max_preparing,
        )
    }
    pub(crate) fn metrics_snapshot(&self) -> super::node_metrics::WorkerNodeMetricsSnapshot {
        // Resource units and the metadata row count come from their original
        // owners; neither observation grants capacity or proves actual exit.
        super::node_metrics::WorkerNodeMetricsSnapshot::sample(
            &self.metrics_ledger,
            self.directory.len(),
        )
    }
}

impl WorkerNodeHost {
    /// Original registered inventory over a bounded observation interval.
    /// Logical channel fields share their own lock; physical atomics and
    /// different Contexts are independent samples, never an exit receipt.
    pub(crate) fn try_root_ownership_snapshot(
        &self,
    ) -> Option<crate::root_result_channel::RootOwnershipSnapshot> {
        let addresses = self.directory.try_census_addresses()?;
        let mut total = crate::root_result_channel::RootOwnershipSnapshot::default();
        for address in &addresses {
            let host = address.try_lock()?;
            total.try_add(host.try_root_ownership_snapshot()?)?;
        }
        self.directory
            .census_addresses_unchanged(&addresses)
            .then_some(total)
    }
}

impl WorkerNodeHost {
    /// The sole bootstrap owner binds its actual pool before exposing a facade.
    /// Refusal returns only neutral metadata; the caller still owns its pool.
    pub(crate) fn bind_control_plane_observation(
        &self,
        observation: PollerObservation,
    ) -> Result<(), PollerObservation> {
        self.poller_observation.set(observation)
    }
    pub(crate) fn try_control_plane_snapshot(
        &self,
    ) -> Option<super::node_metrics::WorkerControlPlaneSnapshot> {
        self.poller_observation.get()?.snapshot()
    }
    pub(crate) fn runtime_observation_snapshot(
        &self,
    ) -> Option<super::node_metrics::WorkerRuntimeObservationSnapshot> {
        self.runtime.observation_snapshot()
    }
    pub(crate) fn try_runtime_inventory_page(
        &self,
        budget: usize,
    ) -> Result<
        Option<super::node_metrics::WorkerRuntimeInventoryPage>,
        super::node_metrics::WorkerRuntimeObservationError,
    > {
        self.metrics_ledger.try_observation_inventory(budget)
    }
}

impl WorkerNodeHost {
    /// Read only original neutral metadata. Busy, poison or missing samples
    /// are unavailable, never a default zero or an inferred ticket closure.
    pub(crate) fn try_admission_snapshot(
        &self,
    ) -> Option<super::node_metrics::WorkerAdmissionSnapshot> {
        let logical_live_grants = {
            let root = self.control.try_lock()?;
            let admission = root.resources().admission_snapshot.try_borrow()?;
            admission.logical_live_grants
        };
        Some(super::node_metrics::WorkerAdmissionSnapshot {
            logical_live_grants: u64::try_from(logical_live_grants).ok()?,
        })
    }

    pub(crate) fn try_execution_quiescence_snapshot(
        &self,
    ) -> Option<super::node_metrics::WorkerExecutionQuiescenceSnapshot> {
        let addresses = self.directory.try_census_addresses()?;
        let mut roots = crate::root_result_channel::RootOwnershipSnapshot::default();
        let mut active_participations = 0u64;
        for address in &addresses {
            let host = address.try_lock()?;
            let (idle, root) = host.try_execution_observation()?;
            active_participations = active_participations.checked_add(u64::from(!idle))?;
            roots.try_add(root)?;
        }
        let sample = super::node_metrics::WorkerExecutionQuiescenceSnapshot::sample(
            &self.metrics_ledger,
            active_participations,
            addresses.len(),
            roots,
        )?;
        self.directory
            .census_addresses_unchanged(&addresses)
            .then_some(sample)
    }
}

// Strictly cfg(test): no role, scope, report, receipt or cleanup authority escapes.
#[cfg(test)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PreparationFixtureInventory {
    pub scope: novarocks_control_runtime::transaction::ScopeSnapshot,
    pub input_custodies: Vec<novarocks_control_runtime::actor_turn::StandingInputCustody>,
    pub preparation_key: Option<(
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    )>,
    pub body_exited: bool,
    pub child_body_exited: bool,
    pub host_borrowers_exited: bool,
    pub body_report_retired: bool,
    pub whole_report_retired: bool,
    pub stopped: bool,
    pub stop_call_exited: bool,
    pub activation: (bool, usize, usize, usize),
    pub installed: bool,
    pub physical_pending: usize,
    pub lifecycle_ids: Vec<(
        crate::control_runtime_policy::TaskLifecycleInputKind,
        novarocks_control_runtime_core::identity::ScopedWorkRef,
        novarocks_worker_protocol::proposal_summary::CutId,
        novarocks_control_runtime::exit_custody::ExitCustodyPhase,
    )>,
    pub settlement_work: Option<novarocks_control_runtime_core::identity::ScopedWorkRef>,
}
#[cfg(test)]
impl WorkerNodeHost {
    pub(crate) fn preparation_fixture_inventory(
        &self,
        participation: ParticipationRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<PreparationFixtureInventory> {
        let address = self.directory.participation(participation)?;
        let mut host = address.lock()?;
        let scope = host.scope().snapshot().ok()?;
        let budget = self.runtime.snapshot(host.actor()).ok()?.standing_inputs;
        let input_custodies = self
            .runtime
            .standing_input_inventory(host.actor(), None, budget)
            .ok()?;
        let physical_pending = host.original_physical_pending_for_test();
        let owner = host.task(task)?;
        let resources = owner.resources_for_retirement();
        let preparation = owner.preparation();
        Some(PreparationFixtureInventory {
            scope,
            input_custodies,
            preparation_key: preparation.map(|p| p.key()),
            body_exited: preparation.is_some_and(|p| p.body_and_captures_exited()),
            child_body_exited: preparation
                .and_then(|p| p.host_children())
                .is_some_and(|child| child.body_has_exited()),
            host_borrowers_exited: preparation.is_some_and(|p| p.host_borrowers_exited()),
            body_report_retired: preparation.is_some_and(|p| p.body_report_retired()),
            whole_report_retired: preparation.is_some_and(|p| p.exit_report_retired()),
            stopped: resources.port.is_stopped(),
            stop_call_exited: owner
                .host_call(crate::control_runtime::task_host_call::TaskHostCallKind::Stop)
                .actual_call_exited(),
            activation: owner.activation_fixture_state(),
            installed: resources.port.current().installed(),
            physical_pending,
            lifecycle_ids: resources
                .lifecycle_inputs
                .iter()
                .map(|(kind, input)| (*kind, input.work_id(), input.cut(), input.phase()))
                .collect(),
            settlement_work: resources
                .completion
                .as_ref()
                .map(|completion| completion.settlement_work()),
        })
    }

    /// This fixture holds the original Participation projection after the actual
    /// opaque host has consumed its roles. No status/exit fact is injected.
    pub(crate) fn with_original_task_projection_locked_for_test<T>(
        &self,
        participation: ParticipationRef,
        task: novarocks_execution_contract::TaskIdentity,
        observe: impl FnOnce(bool) -> T,
    ) -> T {
        let address = self.directory.participation(participation).unwrap();
        let mut host = address.lock().unwrap();
        let installed = host
            .task(task)
            .unwrap()
            .resources_for_retirement()
            .port
            .current()
            .installed();
        observe(installed)
    }

    /// Rescue only this fixture's proven uninvoked original action. No new
    /// role, identity, factory or Core fact is made; callbacks run unlocked.
    pub(crate) fn resume_original_uninvoked_activation_for_test(
        &self,
        participation: ParticipationRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> bool {
        let original = {
            let address = self.directory.participation(participation).unwrap();
            let mut host = address.lock().unwrap();
            host.task(task)
                .unwrap()
                .original_uninvoked_activation_for_rescue()
                .unwrap()
        };
        original.1.activate(&original.0)
    }
}

impl WorkerNodeHost {
    pub(crate) fn try_protocol_retention_snapshot(
        &self,
    ) -> Option<super::node_metrics::WorkerProtocolRetentionSnapshot> {
        let projection = {
            let root = self.control.try_lock()?;
            let snapshot = root.resources().admission_snapshot.try_borrow()?;
            *snapshot
        };
        let addresses = self.directory.try_census_addresses()?;
        let mut participations = super::node_metrics::WorkerParticipationRetentionSnapshot::empty();
        for address in &addresses {
            participations.try_add(address.try_lock()?.retention_snapshot()?)?;
        }
        self.directory
            .census_addresses_unchanged(&addresses)
            .then_some(super::node_metrics::WorkerProtocolRetentionSnapshot {
                participations,
                admission: projection.admission_retention,
                control: projection.control_retention,
            })
    }
}
