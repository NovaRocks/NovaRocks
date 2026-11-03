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
//! original Root and per-Context owners consume their committed effects;
//! foreign calls remain in the admitted physical host executors.

use super::{
    actors::{WorkerAction, WorkerActor},
    context_executors::ContextExecutorHandles,
    context_host::{ContextHostBinding, ContextHostRecord, TaskDriverPorts},
    control_request::{ControlRequestHandle, ControlRequestReady},
    establish_executors::EstablishExecutorHandles,
    process_control::{ProcessControlOwner, RootHostCell},
    service::ContextDirectory,
};
use crate::{
    QueryContextHostFactory, control_admission::AdmissionStorage,
    control_runtime_policy::ControlRuntimePolicy,
};
use novarocks_control_runtime::{
    driver::{
        ActorHostTurn, ActorRegistration, ActorRuntime, Clock, PollerObservation, PollerPool,
    },
    publication::PublishedSnapshot,
};
use novarocks_control_runtime_core::identity::{ActorId, Generation, ReservationId, Tick};
use novarocks_execution_contract::{QueryContextRef, QueryContextState, TaskOperationId};
use novarocks_worker_protocol::{
    context::{ContextConfig, ContextCore},
    control::{ControlCore, ControlEffect, ControlInput},
    lease::MonotonicInstant,
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
struct NodeTaskNotice(super::context_ready::ContextReadyNotice);
impl crate::task_port::TaskPendingNotice for NodeTaskNotice {
    fn pending(&self) {
        self.0.notify();
    }
}
pub(crate) struct NodeContextConfig {
    pub core: ContextConfig,
    pub policy: ControlRuntimePolicy,
    pub storage: AdmissionStorage,
    pub max_live_tasks: usize,
    pub max_live_roots: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ContextRoute {
    pub state: QueryContextState,
    pub generation: Generation,
}
/// The actual neutral route cell and, after physical child removal, its
/// original remaining parent owner. There is one row per elected Context.
struct ContextRow {
    reservation: ReservationId,
    route: PublishedSnapshot<ContextRoute>,
    removed: Option<ContextHostRecord>,
    retire_requested: bool,
}
#[derive(Default)]
struct ContextRows {
    contexts: BTreeMap<QueryContextRef, ContextRow>,
    reservations: BTreeMap<ReservationId, QueryContextRef>,
    removed_ready: VecDeque<QueryContextRef>,
}
pub(crate) enum NodeTaskReadHandle {
    Live(
        super::context_host::TaskContextRequestHandle,
        Option<Arc<super::task_read_payload::TaskReadPayloadShared>>,
    ),
    Sealed(super::context_host::ContextRequestHandle),
}
pub(crate) enum NodeCancelHandle {
    Task(super::context_host::TaskContextRequestHandle),
    Context(super::context_host::ContextRequestHandle),
    Refused(novarocks_worker_protocol::context::TaskControlRefusal),
}
pub(crate) enum NodeCreateHandle {
    Context(super::context_host::ContextRequestHandle),
    BeforeContext(ControlRequestHandle),
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
            novarocks_control_runtime::driver::EffectTicket<WorkerAction>,
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
            .downcast_ref::<super::process_control::RootRequestAdvanceError>()
        {
            // Only closed runtime, route and source-phase facts are present.
            // This does not format the original request, host or rich source.
            debug.field("error", error);
        }
        if let Some((refusal, ticket)) = self.original.downcast_ref::<(
            super::context_removal::ContextRemovalRefusal,
            novarocks_control_runtime::driver::EffectTicket<WorkerAction>,
        )>() {
            debug
                .field("error", &refusal.error)
                .field("retains_partial", &refusal.partial.is_some())
                .field("actor", &ticket.actor)
                .field("input", &ticket.input);
        }
        if let Some((error, turn)) = self.original.downcast_ref::<(
            super::context_host::ContextDriverError,
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
/// A supported control submission can be refused before it owns any R.
/// Driver invariants retain their complete original custody separately.
#[derive(Debug)]
pub(crate) enum NodeRequestRefusal {
    OwnerFailed,
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
/// is released before any per-Context lock or actual host submission is taken.
pub(crate) struct WorkerNodeHost {
    clock: Arc<dyn Clock>,
    runtime: ActorRuntime<WorkerActor>,
    root_registration: ActorRegistration,
    root: RootHostCell,
    metrics_ledger: novarocks_control_runtime::transaction::RuntimeLedger,
    poller_observation: OnceLock<PollerObservation>,
    root_ready_scan: AtomicUsize,
    draining: AtomicBool,
    directory: ContextDirectory,
    requests: ControlRequestReady,
    contexts: ContextExecutorHandles,
    tasks: TaskDriverPorts,
    establish: EstablishExecutorHandles,
    factory: Arc<dyn QueryContextHostFactory>,
    config: NodeContextConfig,
    actor_ids: NodeActorIds,
    rows: Mutex<ContextRows>,
}
impl WorkerNodeHost {
    #[cfg(test)]
    pub(crate) fn original_request_inventory_for_test(
        &self,
        context: QueryContextRef,
    ) -> Option<Vec<String>> {
        let address = self.directory.context(context)?;
        let host = address.lock()?;
        Some(host.original_request_inventory_for_test())
    }
    #[cfg(test)]
    pub(crate) fn original_task_request_for_test(
        &self,
        context: QueryContextRef,
        work: novarocks_control_runtime_core::identity::ScopedWorkRef,
    ) -> Option<(
        novarocks_worker_protocol::creation::RequestContext,
        novarocks_execution_contract::TaskIdentity,
        bool,
        bool,
    )> {
        let address = self.directory.context(context)?;
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
        self.root
            .lock()
            .resources()
            .admission_snapshot
            .borrow()
            .epoch
    }

    pub(crate) fn task_context(
        &self,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<QueryContextRef> {
        self.directory.task_context(task)
    }
    pub(crate) fn released_evidence(
        &self,
        context: QueryContextRef,
    ) -> Result<crate::ReleasedContextEvidence, NodeHostRefusal> {
        let address = self
            .directory
            .context(context)
            .ok_or_else(|| NodeHostRefusal::new("Release evidence Context address", context))?;
        let host = address
            .lock()
            .ok_or_else(|| NodeHostRefusal::new("Release evidence original Context", context))?;
        host.host_jobs()
            .and_then(|jobs| jobs.released_evidence())
            .ok_or_else(|| NodeHostRefusal::new("Release evidence original host result", context))
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn live_task_execution_control_for_test(
        &self,
        identity: novarocks_execution_contract::TaskIdentity,
    ) -> Option<Arc<novarocks_execution::runtime::task_control::TaskExecutionControl>> {
        let context = self.directory.task_context(identity)?;
        let address = self.directory.context(context)?;
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
        context: QueryContextRef,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: novarocks_worker_protocol::context::ContextCommand,
    ) -> Result<NodeTaskReadHandle, NodeHostRefusal> {
        use novarocks_worker_protocol::context::ContextCommand;
        if !matches!(
            &command,
            ContextCommand::GetFinalInfo { .. } | ContextCommand::GetDynamicFilters { .. }
        ) {
            return Err(NodeHostRefusal::new("Task read command identity", command));
        }
        let task = command.task().expect("actual read command Task");
        if task.verify_query_context(context).is_err() {
            return Err(NodeHostRefusal::new("Task read Context identity", command));
        }
        let Some(address) = self.directory.context(context) else {
            return Err(NodeHostRefusal::new("Task read Context address", command));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new("Task read Context owner", command));
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
            let read_payload = if matches!(&command, ContextCommand::GetDynamicFilters { .. }) {
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
        context: QueryContextRef,
        payload: super::process_control::rejected_ingress::RejectedIngressPayload,
        ingress: super::node_api::WorkerIngressResponsibility,
    ) -> NodeRequestRefusal {
        // The caller releases its Context guard before this Root handoff.
        // Only the admitted original payload and Native responsibility move;
        // no Root RPC, reply cell, or second R is created.
        match self
            .root
            .lock()
            .adopt_rejected_ingress(context, payload, ingress)
        {
            Ok(()) => NodeRequestRefusal::OwnerFailed,
            Err(original) => {
                NodeHostRefusal::new("failed ingress original disposal handoff", original).into()
            }
        }
    }
    fn reject_failed_admission_partial(
        host: &mut super::context_host::ContextHostRecord,
        admission: super::context_host::requests::ContextRequestAdmissionRefusal,
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
        context: QueryContextRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<(bool, bool)> {
        task.verify_query_context(context).ok()?;
        let address = self.directory.context(context)?;
        let mut host = address.lock()?;
        Some((host.task(task).is_some(), host.has_retired_task_read(task)))
    }
    pub(crate) fn submit_task_domain(
        &self,
        context: QueryContextRef,
        original: novarocks_execution_contract::UpdateTask,
        deadline: MonotonicInstant,
        ingress: super::node_api::WorkerIngressResponsibility,
    ) -> Result<super::context_host::TaskContextRequestHandle, NodeRequestRefusal> {
        use super::context_host::requests::ContextRequestError;
        use super::process_control::rejected_ingress::RejectedIngressPayload;
        if self.draining.load(Ordering::Acquire) || !self.tasks.intake_open() {
            return Err(
                NodeHostRefusal::new("Task domain intake is closed", (original, ingress)).into(),
            );
        }
        if original.identity().verify_query_context(context).is_err() {
            return Err(
                NodeHostRefusal::new("Task domain Context identity", (original, ingress)).into(),
            );
        }
        let Some(address) = self.directory.context(context) else {
            return Err(
                NodeHostRefusal::new("Task domain Context address", (original, ingress)).into(),
            );
        };
        let Some(mut host) = address.lock() else {
            return Err(
                NodeHostRefusal::new("Task domain Context owner", (original, ingress)).into(),
            );
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
                context,
                RejectedIngressPayload::TaskDomain(original),
                ingress,
            ));
        }
        let live = match host.task_domain_uses_live_route(original.identity()) {
            Ok(live) => live,
            Err(ContextRequestError::OwnerFailed) => {
                drop(host);
                return Err(self.reject_failed_ingress(
                    context,
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
                        && matches!(error.error, ContextRequestError::OwnerFailed) =>
                {
                    drop(host);
                    return Err(self.reject_failed_ingress(
                        context,
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
                    && matches!(error.error, ContextRequestError::OwnerFailed) =>
            {
                drop(host);
                return Err(self.reject_failed_ingress(
                    context,
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
    pub(crate) fn submit_context_domain(
        &self,
        original: novarocks_execution_contract::AdvanceQueryContextDomain,
        deadline: MonotonicInstant,
        ingress: super::node_api::WorkerIngressResponsibility,
    ) -> Result<super::context_host::ContextRequestHandle, NodeRequestRefusal> {
        use super::context_host::requests::ContextRequestError;
        use super::process_control::rejected_ingress::RejectedIngressPayload;
        if self.draining.load(Ordering::Acquire) || !self.tasks.intake_open() {
            return Err(NodeHostRefusal::new(
                "Context domain intake is closed",
                (original, ingress),
            )
            .into());
        }
        let context = original.context();
        let Some(address) = self.directory.context(context) else {
            return Err(NodeHostRefusal::new("Context domain address", (original, ingress)).into());
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new("Context domain owner", (original, ingress)).into());
        };
        let snapshot = match self.runtime.snapshot(host.actor()) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Context domain original owner gate",
                    (error, original, ingress),
                )
                .into());
            }
        };
        if snapshot.failed {
            drop(host);
            return Err(self.reject_failed_ingress(
                context,
                RejectedIngressPayload::ContextDomain(original),
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
                    && matches!(error.error, ContextRequestError::OwnerFailed) =>
            {
                drop(host);
                return Err(self.reject_failed_ingress(
                    context,
                    RejectedIngressPayload::ContextDomain(original),
                    ingress,
                ));
            }
            Err(error) if error.partial.is_some() => {
                return Err(Self::reject_failed_admission_partial(
                    &mut host,
                    error,
                    RejectedIngressPayload::ContextDomain(original),
                    ingress,
                ));
            }
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Context domain original R admission",
                    (error, original, ingress),
                )
                .into());
            }
        };
        match host.capture_context_domain(work, original, self.next_payload()) {
            Ok(()) => Ok(handle),
            Err((error, original)) => {
                match host.capture_refused_context_domain(work, original, ingress) {
                    Ok(()) => Ok(handle),
                    Err(original) => Err(NodeHostRefusal::new(
                        "Context domain original source capture",
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
        use super::context_host::requests::ContextRequestError;
        use novarocks_worker_protocol::context::{ContextCommand, TaskControlRefusal};
        let task = request.identity();
        let Some(context) = self.directory.task_context(task) else {
            return Ok(NodeCancelHandle::Refused(TaskControlRefusal::NotOwned));
        };
        let Some(address) = self.directory.context(context) else {
            return Ok(NodeCancelHandle::Refused(TaskControlRefusal::NotOwned));
        };
        let Some(mut host) = address.lock() else {
            // This exact cell has already moved to its actual removed owner.
            return Ok(NodeCancelHandle::Refused(TaskControlRefusal::NotOwned));
        };
        let operation = request.envelope().operation_id();
        let command = ContextCommand::Cancel {
            task,
            reason: request.reason(),
        };
        if host.has_retired_task_read(task) {
            // The original physical retirement proof replaces the departed
            // producer; the same Context Core still owns its replay answer.
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
                        && matches!(original.error, ContextRequestError::OwnerFailed) =>
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
                Ok(()) => Ok(NodeCancelHandle::Context(handle)),
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
                    && matches!(original.error, ContextRequestError::UnknownTask) =>
            {
                return Ok(NodeCancelHandle::Refused(TaskControlRefusal::NotOwned));
            }
            Err(original)
                if original.partial.is_none()
                    && matches!(original.error, ContextRequestError::OwnerFailed) =>
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
        context: QueryContextRef,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: novarocks_worker_protocol::context::ContextCommand,
    ) -> Result<super::context_host::TaskContextRequestHandle, NodeHostRefusal> {
        use novarocks_worker_protocol::context::ContextCommand;
        if !matches!(
            &command,
            ContextCommand::Cancel { .. }
                | ContextCommand::GetFinalInfo { .. }
                | ContextCommand::GetDynamicFilters { .. }
        ) {
            return Err(NodeHostRefusal::new(
                "Task command requires its original source",
                command,
            ));
        }
        let task = command.task().expect("qualified Task command");
        if task.verify_query_context(context).is_err() {
            return Err(NodeHostRefusal::new(
                "Task command context identity",
                command,
            ));
        }
        let Some(address) = self.directory.context(context) else {
            return Err(NodeHostRefusal::new(
                "Task command Context address",
                command,
            ));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new("Task command Context owner", command));
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
        context: QueryContextRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) {
        if let Some(address) = self.directory.context(context)
            && let Some(mut host) = address.lock()
        {
            host.print_original_execution_inventory();
            host.print_neutral_task_driver_inventory(task);
            if let Some(owner) = host.task(task) {
                owner.print_neutral_production_inventory();
            }
        }
    }
    /// Physical pool owners stay with process composition until all these
    /// original producers drain. Handles alone confer no new admission.
    pub(crate) fn from_started(
        mut root: ProcessControlOwner,
        contexts: ContextExecutorHandles,
        tasks: TaskDriverPorts,
        establish: EstablishExecutorHandles,
        factory: Arc<dyn QueryContextHostFactory>,
        config: NodeContextConfig,
        actor_ids: NodeActorIds,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, NodeHostRefusal> {
        if config.max_live_tasks != config.core.max_tasks
            || config.core.step_budget != ContextCore::MAX_STEP_WORK
            || tasks.lifecycle.is_none()
            || tasks.capacity.is_none()
            || tasks.observation.is_none()
            || tasks.protocol_events.is_none()
        {
            return Err(NodeHostRefusal::new(
                "node geometry",
                (
                    root, contexts, tasks, establish, factory, config, actor_ids, clock,
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
                            error, root, contexts, tasks, establish, factory, config, actor_ids,
                            clock,
                        ),
                    ));
                }
            };
            if !original.same_signal(signal) || !original.same_signal(contexts.capacity_signal()) {
                return Err(NodeHostRefusal::new(
                    "Task capacity source origin",
                    (
                        root, contexts, tasks, establish, factory, config, actor_ids, clock,
                    ),
                ));
            }
        }
        if let Err(error) = root.enable_internal_source() {
            return Err(NodeHostRefusal::new(
                "Root internal source",
                (
                    error, root, contexts, tasks, establish, factory, config, actor_ids, clock,
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
            root: RootHostCell::new(root),
            root_ready_scan: AtomicUsize::new(0),
            draining: AtomicBool::new(false),
            contexts,
            tasks,
            establish,
            factory,
            config,
            actor_ids,
            rows: Mutex::new(ContextRows::default()),
        })
    }
    pub(crate) fn route(
        &self,
        context: QueryContextRef,
    ) -> Option<PublishedSnapshot<ContextRoute>> {
        self.rows
            .lock()
            .expect("original Context route rows")
            .contexts
            .get(&context)
            .map(|row| row.route.clone())
    }
    pub(crate) fn begin_drain(&self) -> Result<(), NodeHostRefusal> {
        self.root
            .lock()
            .begin_drain()
            .map_err(|original| NodeHostRefusal::new("Root admission drain", original))?;
        self.draining.store(true, Ordering::Release);
        self.runtime
            .notify_host_actor(&self.root_registration)
            .map_err(|original| NodeHostRefusal::new("Root drain wake", original))
    }
    pub(crate) fn confirm_ingress_closed(&self) -> Result<(), NodeHostRefusal> {
        self.root
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
        context: QueryContextRef,
        preflight: bool,
    ) -> Result<ControlRequestHandle, NodeHostRefusal> {
        let mut root = self.root.lock();
        let (input, handle) = root
            .admit_request(self.next_source(), operation, deadline)
            .map_err(|original| NodeHostRefusal::new("ticket observation admission", original))?;
        match root.submit_ticket_observation(input, ticket, context, preflight) {
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
        let mut root = self.root.lock();
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
        context: QueryContextRef,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        generation: u64,
        position: novarocks_worker_protocol::observation::CoveredSubscriptionRequest,
    ) -> Result<super::context_host::requests::observation::ContextObservationStream, NodeHostRefusal>
    {
        let Some(address) = self.directory.context(context) else {
            return Err(NodeHostRefusal::new(
                "status subscription Context address",
                position,
            ));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new(
                "status subscription Context owner",
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
            super::context_host::requests::observation::ContextObservationStream::from_request(
                address, work, handle,
            ),
        )
    }

    pub(crate) fn submit_context(
        &self,
        context: QueryContextRef,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: novarocks_worker_protocol::context::ContextCommand,
    ) -> Result<super::context_host::ContextRequestHandle, NodeRequestRefusal> {
        self.submit_context_selected(context, source, operation, deadline, command, false)
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn submit_context_owner_panic(
        &self,
        context: QueryContextRef,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: novarocks_worker_protocol::context::ContextCommand,
    ) -> Result<super::context_host::ContextRequestHandle, NodeRequestRefusal> {
        if !matches!(
            &command,
            novarocks_worker_protocol::context::ContextCommand::Renew { .. }
        ) {
            return Err(NodeHostRefusal::new(
                "declared owner panic requires its original Renew",
                command,
            )
            .into());
        }
        self.submit_context_selected(context, source, operation, deadline, command, true)
    }
    fn submit_context_selected(
        &self,
        context: QueryContextRef,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        command: novarocks_worker_protocol::context::ContextCommand,
        _panic_on_renew: bool,
    ) -> Result<super::context_host::ContextRequestHandle, NodeRequestRefusal> {
        use novarocks_worker_protocol::context::ContextCommand;
        if !matches!(
            &command,
            ContextCommand::Renew { .. }
                | ContextCommand::Quiesce
                | ContextCommand::Abort { .. }
                | ContextCommand::Release
        ) {
            // Task-causal commands and rich updates enter through their
            // original paid source/cut owners, not this neutral control port.
            return Err(NodeHostRefusal::new(
                "Context control command requires its original source",
                command,
            )
            .into());
        }
        let Some(address) = self.directory.context(context) else {
            return Err(NodeHostRefusal::new("Context request address", command).into());
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new("Context request owner", command).into());
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
                    super::context_host::ContextRequestError::OwnerFailed
                ) && error.partial.is_none()
                {
                    // Only these four plain control commands enter this port.
                    // No R, rich input or partial was admitted on this branch.
                    drop(host);
                    drop(command);
                    return Err(NodeRequestRefusal::OwnerFailed);
                }
                return Err(
                    NodeHostRefusal::new("Context request admission", (error, command)).into(),
                );
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
                        .retains_refused_context_control(work)
                        .map_err(|error| {
                            NodeHostRefusal::new("Context original refused control input", error)
                        })?
                {
                    return Ok(handle);
                }
                Err(NodeHostRefusal::new("Context request submission", (error, handle)).into())
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
        use super::context_host::requests::{
            ContextCreationInspectionRefusal, ContextRequestError,
        };
        use super::process_control::rejected_ingress::RejectedIngressPayload;
        if self.draining.load(Ordering::Acquire) || !self.tasks.intake_open() {
            return Err(NodeHostRefusal::new("Task intake is not open", (payload, ingress)).into());
        }
        let context = payload.request.context();
        let operation = payload.request.envelope().operation_id();
        let Some(address) = self.directory.context(context) else {
            return self
                .create_before_context(source, deadline, payload)
                .map_err(Into::into);
        };
        let Some(mut host) = address.lock() else {
            return self
                .create_before_context(source, deadline, payload)
                .map_err(Into::into);
        };
        let admission = match self.runtime.snapshot(host.actor()) {
            Ok(snapshot) => snapshot,
            Err(original) => {
                return Err(NodeHostRefusal::new(
                    "Create original Context gate",
                    (original, payload, ingress),
                )
                .into());
            }
        };
        if admission.failed {
            drop(host);
            return Err(self.reject_failed_ingress(
                context,
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
                    Ok(NodeCreateHandle::Context(handle))
                }
                Err(ContextCreationInspectionRefusal::Admission(error))
                    if error.partial.is_none()
                        && matches!(error.error, ContextRequestError::OwnerFailed) =>
                {
                    drop(host);
                    Err(self.reject_failed_ingress(
                        context,
                        RejectedIngressPayload::Create {
                            payload,
                            binding: None,
                        },
                        ingress,
                    ))
                }
                Err(ContextCreationInspectionRefusal::Admission(error))
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
            context,
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
                    && matches!(error.error, ContextRequestError::OwnerFailed) =>
            {
                drop(host);
                return Err(self.reject_failed_ingress(
                    context,
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
            Ok(()) => Ok(NodeCreateHandle::Context(handle)),
            Err(original) => {
                if original.originals.is_none() {
                    match host.retains_refused_create_input(work) {
                        Ok(true) => {
                            return match host.capture_refused_create_ingress(work, ingress) {
                                Ok(()) => Ok(NodeCreateHandle::Context(handle)),
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
    fn create_before_context(
        &self,
        source: ActorId,
        deadline: MonotonicInstant,
        payload: super::loading::TaskCreatePayload,
    ) -> Result<NodeCreateHandle, NodeHostRefusal> {
        let context = payload.request.context();
        let task = payload.request.identity();
        let operation = payload.request.envelope().operation_id();
        tracing::debug!(context = ?context, "Create awaits its original Context address");
        // No Task has accepted this decoded input. The original caller owns
        // its physical disposal, outside all owner locks. Only the actual
        // Root R decides whether the request can retry or the owner is closed.
        drop(payload);
        self.submit_control(
            source,
            operation,
            deadline,
            ControlInput::CreateWithoutContextAddress { context, task },
        )
        .map(NodeCreateHandle::BeforeContext)
    }
    /// Resolve the exact original Context, then admit the data holder and R
    /// before lending the channel. The returned read executes outside this
    /// metadata guard; Root's channel lock orders it against real sealing.
    pub(crate) fn begin_root_read(
        &self,
        context: QueryContextRef,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        request: &novarocks_execution_contract::root_result::RootResultRead,
    ) -> Result<super::context_host::NodeRootRead, super::context_host::RootReadError> {
        let Some(address) = self.directory.context(context) else {
            return Err(super::context_host::RootReadError::UnknownContext);
        };
        let Some(mut host) = address.lock() else {
            return Err(super::context_host::RootReadError::ContextRemoved);
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
        raw: novarocks_execution_contract::EstablishQueryContext,
        payload: novarocks_worker_protocol::creation::PayloadId,
    ) -> Result<ControlRequestHandle, NodeHostRefusal> {
        let mut root = self.root.lock();
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
                self.root
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
                        .expect("original Context route rows")
                        .contexts
                        .is_empty();
                    if self.draining.load(Ordering::Acquire) && rows_empty {
                        let drain = self.root.lock().advance_drain().map_err(|original| {
                            NodeHostRefusal::new("Root metadata drain", original)
                        })?;
                        progress.runnable |= drain.runnable;
                        if drain.ready {
                            let pure = self.root.lock().finish_drain(turn).map_err(|original| {
                                NodeHostRefusal::new("Root physical metadata teardown", original)
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
            self.advance_context(turn, budget)
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
            .root
            .lock()
            .advance_request_partials(budget)
            .map_err(|original| {
                NodeHostRefusal::new("Root original request partial retirement", original)
            })?;
        progress.runnable |= partials.runnable;
        let indexed = self
            .root
            .lock()
            .index_control_effects((ControlCore::MAX_STEP_WORK + 1).saturating_mul(budget))
            .map_err(|original| NodeHostRefusal::new("Root effect index", original))?;
        // Only the original Common Root turn updates this scan. The count is
        // routing metadata, never an action/Work/exit authority. A bounded
        // NotReady prefix must neither hide its ready tail nor spin forever.
        let mut remaining = self.root_ready_scan.load(Ordering::Relaxed);
        if indexed.units > 0 || remaining == 0 {
            remaining = self.root.lock().resources().dispatch.ready_positions();
        }
        for _ in 0..budget {
            let positions = self.root.lock().resources().dispatch.ready_positions();
            let allowance = remaining.min(positions).min(ControlCore::MAX_STEP_WORK);
            if allowance == 0 {
                remaining = 0;
                break;
            }
            let ticket = self
                .root
                .lock()
                .take_control_effect(allowance)
                .map_err(|original| NodeHostRefusal::new("Root effect dispatch", original))?;
            let Some(ticket) = ticket else {
                remaining -= allowance;
                break;
            };
            self.handoff_root_effect(ticket)?;
            progress.root_effects += 1;
            remaining = self.root.lock().resources().dispatch.ready_positions();
        }
        self.root_ready_scan.store(remaining, Ordering::Relaxed);
        let io = self
            .root
            .advance_ready(
                turn,
                &self.establish,
                self.tasks.protocol_events.as_ref(),
                budget,
            )
            .map_err(|original| NodeHostRefusal::new("Root source/route progress", original))?;
        let mut root = self.root.lock();
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
                    novarocks_control_runtime::driver::DriverError::InputStillQueued
                    | novarocks_control_runtime::driver::DriverError::EffectsRemain,
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
        ticket: novarocks_control_runtime::driver::EffectTicket<WorkerAction>,
    ) -> Result<(), NodeHostRefusal> {
        let ordered = match self
            .root
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
        let mut root = self.root.lock();
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
        ticket: novarocks_control_runtime::driver::EffectTicket<WorkerAction>,
    ) -> Result<(), NodeHostRefusal> {
        // Only frozen neutral facts are cloned; no rich source enters this switch.
        match ticket.effect.clone() {
            WorkerAction::AdmissionEpoch(epoch) => {
                let projection = self.root.lock().resources().admission_snapshot.clone();
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
                .root
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
                        context,
                        establishment,
                        generation,
                        reservation,
                    },
                ..
            } => {
                let core =
                    match ContextCore::new(context, generation, establishment, self.config.core) {
                        Ok(core) => core,
                        Err(error) => {
                            return Err(NodeHostRefusal::new(
                                "Context configuration",
                                (error, ticket),
                            ));
                        }
                    };
                let initial_route = PublishedSnapshot::new(
                    &self.root.lock().resources().scope,
                    Arc::new(ContextRoute {
                        state: QueryContextState::Establishing,
                        generation,
                    }),
                );
                {
                    let mut rows = self.rows.lock().expect("original Context route rows");
                    if rows.contexts.contains_key(&context)
                        || rows.reservations.contains_key(&reservation)
                    {
                        return Err(NodeHostRefusal::new("Context row geometry", ticket));
                    }
                    // Materialize is the core's original Establishing election.
                    // Subsequent state changes require actual Route publication.
                    rows.contexts.insert(
                        context,
                        ContextRow {
                            reservation,
                            removed: None,
                            retire_requested: false,
                            route: initial_route,
                        },
                    );
                    rows.reservations.insert(reservation, context);
                }
                if let Err(error) = self.root.lock().adopt_context(
                    &ticket,
                    ContextHostBinding {
                        actor: self.actor_ids.next(),
                        core,
                        policy: self.config.policy,
                        storage: self.config.storage.clone(),
                        max_live_tasks: self.config.max_live_tasks,
                        max_live_roots: self.config.max_live_roots,
                    },
                ) {
                    return Err(NodeHostRefusal::new("Context bootstrap", (error, ticket)));
                }
                let target = self
                    .directory
                    .context(context)
                    .expect("same adopted Context address");
                tracing::debug!(context = ?context, "Worker Node adopted original Context address");
                self.root
                    .handoff_materialization(ticket, target, self.factory.clone())
                    .map_err(|original| {
                        NodeHostRefusal::new("Context materialization handoff", original)
                    })
            }
            WorkerAction::Control {
                effect:
                    ControlEffect::Route {
                        context,
                        state,
                        generation,
                    },
                ..
            } => {
                let route = self
                    .route(context)
                    .expect("Route retains its original paid row");
                if let Err(original) = self.runtime.commit_snapshot(
                    &ticket,
                    &route,
                    Arc::new(ContextRoute { state, generation }),
                ) {
                    return Err(NodeHostRefusal::new(
                        "Context route publication",
                        (original, ticket),
                    ));
                }
                if let Err(error) = self.root.lock().control_publication_committed(&ticket) {
                    return Err(NodeHostRefusal::new(
                        "Context route ordering",
                        (error, ticket),
                    ));
                }
                self.complete_root(ticket)
            }
            WorkerAction::Control {
                effect: ControlEffect::RouteEstablish { .. },
                ..
            } => self
                .root
                .lock()
                .handoff_establish_route(ticket)
                .map_err(|original| NodeHostRefusal::new("Establish route handoff", original)),
            WorkerAction::Control {
                effect: ControlEffect::CancelEstablishRoute { .. },
                ..
            } => self
                .root
                .lock()
                .handoff_establish_route_cancel(ticket)
                .map_err(|original| NodeHostRefusal::new("Establish route cancellation", original)),
            WorkerAction::Control {
                effect: ControlEffect::WakeInternal,
                ..
            } => self
                .root
                .lock()
                .handoff_internal_wake(ticket)
                .map_err(|original| NodeHostRefusal::new("Root continuation handoff", original)),
            WorkerAction::Control {
                effect: ControlEffect::AbortContext { .. },
                ..
            } => {
                if let Err(error) = self.directory.handoff_owner_fence(&ticket) {
                    return Err(NodeHostRefusal::new("Context owner fence", (error, ticket)));
                }
                self.complete_root(ticket)
            }
            WorkerAction::Control {
                effect: ControlEffect::RemoveHost { context },
                ..
            } => {
                let Some(target) = self.directory.context(context) else {
                    return Err(NodeHostRefusal::new("Context removal address", ticket));
                };
                let notice = {
                    let host = target.lock().ok_or_else(|| {
                        NodeHostRefusal::new("Context removal original owner", context)
                    })?;
                    host.ready_notice().clone()
                };
                let Some(execution) = self.tasks.execution.as_ref().cloned() else {
                    return Err(NodeHostRefusal::new(
                        "Context removal execution host",
                        ticket,
                    ));
                };
                let original = {
                    let root = self.root.lock();
                    super::context_removal::ContextRemovalOwner::from_actual_effect(
                        self.runtime.clone(),
                        self.root_registration.clone(),
                        &root.resources().roles[0].work,
                        &ticket,
                        context,
                        notice,
                        execution,
                    )
                };
                let (owner, job) = match original {
                    Ok(original) => original,
                    Err(refusal) => {
                        return Err(NodeHostRefusal::new(
                            "Context removal original admission",
                            (refusal, ticket),
                        ));
                    }
                };
                let mut host = target.lock().ok_or_else(|| {
                    NodeHostRefusal::new("Context removal owner disappeared", context)
                })?;
                if let Err(error) = host.authorize_owner_control_removal(&ticket) {
                    return Err(NodeHostRefusal::new(
                        "Context removal intake fence",
                        (error, owner, job, ticket),
                    ));
                }
                if let Err(original) = host.stage_removal(owner, job) {
                    return Err(NodeHostRefusal::new(
                        "Context removal custody handoff",
                        (original, ticket),
                    ));
                }
                self.runtime
                    .close_actor_admission(target.registration().actor())
                    .map_err(|e| NodeHostRefusal::new("Context removal mailbox fence", e))?;
                self.runtime
                    .close_timer_source(target.registration().actor())
                    .map_err(|e| NodeHostRefusal::new("Context removal timer closure", e))?;
                drop(host);
                self.complete_root(ticket)
            }
            WorkerAction::Control {
                effect: ControlEffect::RetireReservation(reservation),
                ..
            } => {
                let context = self
                    .rows
                    .lock()
                    .expect("original Context route rows")
                    .reservations
                    .get(&reservation)
                    .copied();
                if let Some(context) = context {
                    // Never wait for a Context while holding the route rows.
                    // The actual child-to-parent transfer holds its cell
                    // through insertion, so None here implies the original
                    // has already reached that row rather than vanished.
                    let live = self.directory.context(context).and_then(|target| {
                        target
                            .lock()
                            .map(|mut host| host.authorize_parent_retirement(&ticket))
                    });
                    let mut rows = self.rows.lock().expect("original Context route rows");
                    let row = rows
                        .contexts
                        .get_mut(&context)
                        .expect("original reservation row");
                    let result = match live {
                        Some(result) => result,
                        None => {
                            let Some(host) = row.removed.as_mut() else {
                                return Err(NodeHostRefusal::new(
                                    "Context parent retirement owner",
                                    ticket,
                                ));
                            };
                            host.authorize_parent_retirement(&ticket)
                        }
                    };
                    if let Err(error) = result {
                        return Err(NodeHostRefusal::new(
                            "Context active charge retirement",
                            (error, ticket),
                        ));
                    }
                    row.retire_requested = true;
                } else {
                    return Err(NodeHostRefusal::new(
                        "Context reservation retirement owner missing",
                        ticket,
                    ));
                }
                self.complete_root(ticket)
            }
            // Continuation hints are consumed by the pure actor projection.
            // A Context action or Root failure is never a default host ACK.
            _ => Err(NodeHostRefusal::new("unexpected Root action", ticket)),
        }
    }
    fn advance_parent_rows(&self, budget: usize) -> Result<bool, NodeHostRefusal> {
        // This finite Root turn acquires its owner before the routing rows.
        // Removed Context tails contain no live Context lock or foreign call.
        let mut root = self.root.lock();
        let mut rows = self.rows.lock().expect("original Context route rows");
        let mut runnable = false;
        // Freeze this pass's actual length; a not-yet-decided Root input
        // cannot keep a poller spinning around the same metadata address.
        let visits = rows.removed_ready.len().min(budget);
        for _ in 0..visits {
            let context = rows
                .removed_ready
                .pop_front()
                .expect("actual removed owner");
            let row = rows
                .contexts
                .get_mut(&context)
                .expect("same removed Context row");
            if !row.retire_requested {
                rows.removed_ready.push_back(context);
                continue;
            }
            let retired = row
                .removed
                .as_mut()
                .expect("original parent owner")
                .advance_removed_root_tail(&mut root)
                .map_err(|original| {
                    NodeHostRefusal::new("Context original removal tail", original)
                })?;
            if retired {
                let row = rows
                    .contexts
                    .remove(&context)
                    .expect("same completed parent");
                assert_eq!(rows.reservations.remove(&row.reservation), Some(context));
                // Its rich holder and original children have already exited.
                drop(row);
            } else {
                rows.removed_ready.push_back(context);
                runnable = true;
            }
        }
        Ok(runnable || rows.removed_ready.len() > visits)
    }
    fn advance_context(
        &self,
        mut turn: ActorHostTurn<WorkerActor>,
        budget: usize,
    ) -> Result<NodeHostProgress, NodeHostRefusal> {
        let Some(address) = self.directory.host_turn(&turn) else {
            return Err(NodeHostRefusal::new("original Context host turn", turn));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new("removed Context host turn", turn));
        };
        for notice in turn.take_header_readiness(budget) {
            host.notify_header_readiness(&notice);
        }
        let cold_proof = host.cold_failure_admission_needed().map_err(|original| {
            NodeHostRefusal::new("Context actual cold failure proof", original)
        })?;
        if let Some(proof) = cold_proof {
            drop(host);
            let admission = {
                let root = self.root.lock();
                super::context_host::failure::cold::ColdFailureAdmission::admit(
                    &self.runtime,
                    &self.root_registration,
                    root.lifetime_author(),
                    &proof,
                )
            }
            .map_err(|original| {
                NodeHostRefusal::new("Context original cold failure Cleanup", original)
            })?;
            host = match address.lock() {
                Some(host) => host,
                None => {
                    return Err(NodeHostRefusal::new(
                        "Context cold failure install disappeared",
                        admission,
                    ));
                }
            };
            if let Err(original) = host.install_cold_failure(admission) {
                return Err(NodeHostRefusal::new(
                    "Context original cold failure install",
                    original,
                ));
            }
        }
        let metadata_runnable =
            host.advance_lifecycle_metadata(&mut turn, budget)
                .map_err(|original| {
                    NodeHostRefusal::new("Context original lifecycle metadata", original)
                })?;
        let removal_runnable = host
            .advance_removal(&self.tasks)
            .map_err(|original| NodeHostRefusal::new("Context actual forget progress", original))?;
        let advanced =
            match host.advance_driver(&self.contexts, &self.requests, &self.tasks, budget) {
                Ok(advanced) => advanced,
                Err(original) => {
                    return Err(NodeHostRefusal::new(
                        "Context production turn",
                        (original, turn),
                    ));
                }
            };
        let detached = host
            .try_detach_execution(&mut turn, self.clock.as_ref())
            .map_err(|original| {
                NodeHostRefusal::new("Context actual execution detachment", original)
            })?;
        // Only neutral proof/fact/notice leave this Context borrow. Root's
        // original internal Source serializes delivery without a second R.
        let intent = if self
            .runtime
            .snapshot(address.registration().actor())
            .map_err(|original| NodeHostRefusal::new("Context custodian failure state", original))?
            .failed
        {
            let proof = self
                .runtime
                .failure_proof_for(address.registration())
                .map_err(|original| {
                    NodeHostRefusal::new("Context custodian original proof", original)
                })?;
            let notice = host.ready_notice().clone();
            let now =
                MonotonicInstant::from_origin(std::time::Duration::from_nanos(self.clock.now().0));
            let fact = host
                .failed_lifecycle_intent(&proof, now)
                .map_err(|original| {
                    NodeHostRefusal::new("Context custodian lifecycle intent", original)
                })?;
            fact.map(|fact| (proof, fact, notice))
        } else {
            None
        };
        if let Some((proof, fact, notice)) = intent {
            drop(host);
            let receipt = self
                .root
                .lock()
                .enqueue_failed_lifecycle(&proof, fact, notice)
                .map_err(|original| {
                    NodeHostRefusal::new("Root original failed lifecycle Source", original)
                })?;
            host = match address.lock() {
                Some(host) => host,
                None => {
                    return Err(NodeHostRefusal::new(
                        "Context failed lifecycle receipt disappeared",
                        receipt,
                    ));
                }
            };
            host.install_failed_lifecycle_receipt(&proof, fact, receipt)
                .map_err(|original| {
                    NodeHostRefusal::new("Context original failed lifecycle receipt", original)
                })?;
        }
        let parent_runnable = if host.parent_retirement_ready() {
            !host.advance_parent_teardown().map_err(|original| {
                NodeHostRefusal::new("Context hot parent retirement", original)
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
                NodeHostRefusal::new("Context physical metadata teardown", original)
            })?;
            let mut rows = self.rows.lock().expect("original Context route rows");
            let row = rows
                .contexts
                .get_mut(&address.context())
                .expect("same original Context row");
            assert!(row.removed.replace(host.take_for_parent()).is_none());
            drop(rows);
            drop(host);
            drop(pure);
            self.directory.remove_original(&address);
            self.rows
                .lock()
                .expect("original Context route rows")
                .removed_ready
                .push_back(address.context());
            self.runtime
                .notify_host_actor(&self.root_registration)
                .map_err(|original| {
                    NodeHostRefusal::new("Root parent retirement wake", original)
                })?;
        } else {
            drop(host);
            turn.release(progress.runnable)
                .map_err(|original| NodeHostRefusal::new("Context host turn release", original))?;
        }
        Ok(progress)
    }
}

impl WorkerNodeHost {
    pub(crate) fn trace_drain_inventory(&self) {
        if !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }
        let context = self
            .rows
            .lock()
            .expect("original Context route rows")
            .contexts
            .first_key_value()
            .map(|(&context, _)| context);
        if let Some(address) = context.and_then(|context| self.directory.context(context))
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
            let root = self.root.try_lock()?;
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
        let mut active_contexts = 0u64;
        for address in &addresses {
            let host = address.try_lock()?;
            let (idle, root) = host.try_execution_observation()?;
            active_contexts = active_contexts.checked_add(u64::from(!idle))?;
            roots.try_add(root)?;
        }
        let sample = super::node_metrics::WorkerExecutionQuiescenceSnapshot::sample(
            &self.metrics_ledger,
            active_contexts,
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
    pub roles: Vec<novarocks_control_runtime::driver::InputRoleCustody>,
    pub preparation_key: Option<(
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    )>,
    pub body_exited: bool,
    pub host_borrowers_exited: bool,
    pub body_report_retired: bool,
    pub whole_report_retired: bool,
    pub stopped: bool,
    pub stop_call_exited: bool,
    pub lifecycle_ids: Vec<(
        crate::control_runtime_policy::TaskLifecycleRole,
        novarocks_control_runtime_core::identity::ScopedWorkRef,
        novarocks_worker_protocol::proposal_summary::CutId,
        novarocks_control_runtime::exit_role::ExitRolePhase,
    )>,
    pub settlement_work: Option<novarocks_control_runtime_core::identity::ScopedWorkRef>,
}
#[cfg(test)]
impl WorkerNodeHost {
    pub(crate) fn preparation_fixture_inventory(
        &self,
        context: QueryContextRef,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<PreparationFixtureInventory> {
        let address = self.directory.context(context)?;
        let mut host = address.lock()?;
        let scope = host.scope().snapshot().ok()?;
        let budget = self.runtime.snapshot(host.actor()).ok()?.roles;
        let roles = self
            .runtime
            .role_inventory(host.actor(), None, budget)
            .ok()?;
        let owner = host.task(task)?;
        let resources = owner.resources_for_retirement();
        let preparation = owner.preparation();
        Some(PreparationFixtureInventory {
            scope,
            roles,
            preparation_key: preparation.map(|p| p.key()),
            body_exited: preparation.is_some_and(|p| p.body_and_captures_exited()),
            host_borrowers_exited: preparation.is_some_and(|p| p.host_borrowers_exited()),
            body_report_retired: preparation.is_some_and(|p| p.body_report_retired()),
            whole_report_retired: preparation.is_some_and(|p| p.exit_report_retired()),
            stopped: resources.port.is_stopped(),
            stop_call_exited: owner
                .host_call(crate::control_runtime::task_host_call::TaskHostCallKind::Stop)
                .actual_call_exited(),
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
}

impl WorkerNodeHost {
    pub(crate) fn try_protocol_retention_snapshot(
        &self,
    ) -> Option<super::node_metrics::WorkerProtocolRetentionSnapshot> {
        let projection = {
            let root = self.root.try_lock()?;
            let snapshot = root.resources().admission_snapshot.try_borrow()?;
            *snapshot
        };
        let addresses = self.directory.try_census_addresses()?;
        let mut contexts = super::node_metrics::WorkerContextRetentionSnapshot::empty();
        for address in &addresses {
            contexts.try_add(address.try_lock()?.retention_snapshot()?)?;
        }
        self.directory
            .census_addresses_unchanged(&addresses)
            .then_some(super::node_metrics::WorkerProtocolRetentionSnapshot {
                contexts,
                admission: projection.admission_retention,
                control: projection.control_retention,
            })
    }
}
