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
    effect_dispatch::EffectRef,
    establish_executors::EstablishExecutorHandles,
    process_control::{ProcessControlOwner, RootHostCell},
    service::ContextDirectory,
};
use crate::{
    QueryContextHostFactory, control_admission::AdmissionStorage,
    control_runtime_policy::ControlRuntimePolicy,
};
use novarocks_control_runtime::{
    driver::{ActorHostTurn, ActorRegistration, ActorRuntime, Clock, PollerPool},
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
        Arc, Mutex,
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
    pub max_contexts: usize,
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
#[derive(Debug, Default)]
pub(crate) struct NodeHostProgress {
    pub root_effects: usize,
    pub context_effects: usize,
    pub runnable: bool,
}

/// There is no user callback table and no second host scheduler. Root's cell
/// is released before any per-Context lock or actual host submission is taken.
pub(crate) struct WorkerNodeHost {
    runtime: ActorRuntime<WorkerActor>,
    root_registration: ActorRegistration,
    root: RootHostCell,
    metrics_ledger: novarocks_control_runtime::transaction::RuntimeLedger,
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
        *self.root.lock().resources().admission_epoch.borrow()
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
        if host.has_retired_task_read(task) {
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
            match host.submit_retired_task_read(work, command) {
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

    pub(crate) fn submit_task_domain(
        &self,
        context: QueryContextRef,
        original: novarocks_execution_contract::UpdateTask,
        deadline: MonotonicInstant,
    ) -> Result<super::context_host::TaskContextRequestHandle, NodeHostRefusal> {
        if self.draining.load(Ordering::Acquire) || !self.tasks.intake_open() {
            return Err(NodeHostRefusal::new(
                "Task domain intake is closed",
                original,
            ));
        }
        if original.identity().verify_query_context(context).is_err() {
            return Err(NodeHostRefusal::new(
                "Task domain Context identity",
                original,
            ));
        }
        let Some(address) = self.directory.context(context) else {
            return Err(NodeHostRefusal::new(
                "Task domain Context address",
                original,
            ));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new("Task domain Context owner", original));
        };
        let (work, handle) = match host.admit_task_request(
            &self.requests,
            original.identity(),
            novarocks_control_runtime_core::mailbox::InputClass::Ordinary,
            original.envelope().operation_id(),
            deadline,
        ) {
            Ok(admitted) => admitted,
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Task domain original R admission",
                    (error, original),
                ));
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
            Err(original) => Err(NodeHostRefusal::new(
                "Task domain original source capture",
                (original, handle),
            )),
        }
    }
    pub(crate) fn submit_context_domain(
        &self,
        original: novarocks_execution_contract::AdvanceQueryContextDomain,
        deadline: MonotonicInstant,
    ) -> Result<super::context_host::ContextRequestHandle, NodeHostRefusal> {
        if self.draining.load(Ordering::Acquire) || !self.tasks.intake_open() {
            return Err(NodeHostRefusal::new(
                "Context domain intake is closed",
                original,
            ));
        }
        let Some(address) = self.directory.context(original.context()) else {
            return Err(NodeHostRefusal::new("Context domain address", original));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new("Context domain owner", original));
        };
        let (work, handle) = match host.admit_request(
            &self.requests,
            self.next_source(),
            novarocks_control_runtime_core::mailbox::InputClass::Ordinary,
            original.envelope().operation_id(),
            deadline,
        ) {
            Ok(admitted) => admitted,
            Err(error) => {
                return Err(NodeHostRefusal::new(
                    "Context domain original R admission",
                    (error, original),
                ));
            }
        };
        match host.capture_context_domain(work, original, self.next_payload()) {
            Ok(()) => Ok(handle),
            Err(original) => Err(NodeHostRefusal::new(
                "Context domain original source capture",
                (original, handle),
            )),
        }
    }
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
    ) -> Result<Self, NodeHostRefusal> {
        if config.max_contexts != root.service().directory_capacity().get()
            || config.max_live_tasks != config.core.max_tasks
            || config.core.step_budget != ContextCore::MAX_STEP_WORK
            || tasks.lifecycle.is_none()
            || tasks.capacity.is_none()
            || tasks.observation.is_none()
            || tasks.protocol_events.is_none()
        {
            return Err(NodeHostRefusal::new(
                "node geometry",
                (root, contexts, tasks, establish, factory, config, actor_ids),
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
                        ),
                    ));
                }
            };
            if !original.same_signal(signal) {
                return Err(NodeHostRefusal::new(
                    "Task capacity source origin",
                    (root, contexts, tasks, establish, factory, config, actor_ids),
                ));
            }
        }
        if let Err(error) = root.enable_internal_source() {
            return Err(NodeHostRefusal::new(
                "Root internal source",
                (
                    error, root, contexts, tasks, establish, factory, config, actor_ids,
                ),
            ));
        }
        Ok(Self {
            runtime: root.runtime().clone(),
            root_registration: root.registration().clone(),
            directory: root.service().directory(),
            requests: root.request_ready().clone(),
            metrics_ledger: root.ledger().clone(),
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
    ) -> Result<super::context_host::ContextRequestHandle, NodeHostRefusal> {
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
    ) -> Result<super::context_host::ContextRequestHandle, NodeHostRefusal> {
        if !matches!(
            &command,
            novarocks_worker_protocol::context::ContextCommand::Renew { .. }
        ) {
            return Err(NodeHostRefusal::new(
                "declared owner panic requires its original Renew",
                command,
            ));
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
    ) -> Result<super::context_host::ContextRequestHandle, NodeHostRefusal> {
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
            ));
        }
        let Some(address) = self.directory.context(context) else {
            return Err(NodeHostRefusal::new("Context request address", command));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new("Context request owner", command));
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
                return Err(NodeHostRefusal::new(
                    "Context request admission",
                    (error, command),
                ));
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
            Err(error) => Err(NodeHostRefusal::new(
                "Context request submission",
                (error, handle),
            )),
        }
    }
    /// The original Create payload is captured by its admitted R before the
    /// pure Core can elect a loader. No process-global payload lookup exists.
    pub(crate) fn submit_task_create(
        &self,
        source: ActorId,
        deadline: MonotonicInstant,
        payload: super::loading::TaskCreatePayload,
    ) -> Result<NodeCreateHandle, NodeHostRefusal> {
        if self.draining.load(Ordering::Acquire) || !self.tasks.intake_open() {
            return Err(NodeHostRefusal::new("Task intake is not open", payload));
        }
        let context = payload.request.context();
        let operation = payload.request.envelope().operation_id();
        let Some(address) = self.directory.context(context) else {
            return self.create_before_context(source, deadline, payload);
        };
        let Some(mut host) = address.lock() else {
            return self.create_before_context(source, deadline, payload);
        };
        let admission = match self.runtime.snapshot(host.actor()) {
            Ok(snapshot) => snapshot,
            Err(original) => {
                return Err(NodeHostRefusal::new(
                    "Create original Context gate",
                    (original, payload),
                ));
            }
        };
        if !admission.external_open {
            let inspection = host.inspect_creation(
                &self.requests,
                source,
                operation,
                deadline,
                payload.request.identity(),
            );
            drop(host);
            return match inspection {
                Ok(handle) => {
                    // The actual Native caller still owns this unadmitted
                    // carrier. Its foreign Drop runs after the owner unlocks.
                    drop(payload);
                    Ok(NodeCreateHandle::Context(handle))
                }
                Err(original) => Err(NodeHostRefusal::new(
                    "Create closed inspection",
                    (original, payload),
                )),
            };
        }
        let protocol_generation = match host.scope().gates() {
            Ok(gates) => gates.generation,
            Err(original) => {
                return Err(NodeHostRefusal::new(
                    "Create scope generation",
                    (original, payload),
                ));
            }
        };
        let binding = super::task::TaskHostBinding {
            context,
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
            #[cfg(test)]
            source_storage_model: false,
        };
        let (work, handle) = match host.admit_request(
            &self.requests,
            source,
            novarocks_control_runtime_core::mailbox::InputClass::Ordinary,
            operation,
            deadline,
        ) {
            Ok(original) => original,
            Err(error) => {
                tracing::warn!(error = ?error.error, "Create original request admission refused");
                return Err(NodeHostRefusal::new(
                    "Create request admission",
                    (error, payload, binding),
                ));
            }
        };
        match host.submit_task_create(work, payload, binding) {
            Ok(()) => Ok(NodeCreateHandle::Context(handle)),
            Err(original) => {
                tracing::warn!(error = ?original.error, "Create original source handoff refused");
                Err(NodeHostRefusal::new(
                    "Create original source handoff",
                    (original, handle),
                ))
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
        clock: Arc<dyn Clock>,
        workers: usize,
    ) -> Result<PollerPool<WorkerActor>, std::io::Error> {
        let node = self.clone();
        PollerPool::start_with_host(self.runtime.clone(), clock, workers, move |now, turn| {
            if let Err(original) = node.advance(now, turn, 8) {
                tracing::error!(failure = ?original, "Worker host driver invariant failed");
                #[cfg(test)]
                eprintln!("Worker host driver invariant failed: {original:?}");
                // Reaching this boundary is a fixed driver invariant failure.
                // Preserve the original refused captures across process exit.
                std::mem::forget(original);
                std::process::abort();
            }
        })
    }
    pub(crate) fn advance(
        &self,
        _now: Tick,
        turn: ActorHostTurn<WorkerActor>,
        budget: usize,
    ) -> Result<NodeHostProgress, NodeHostRefusal> {
        if turn.matches_registration(&self.root_registration) {
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
        progress.runnable = remaining > 0
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
        ordered: bool,
    ) -> Result<(), NodeHostRefusal> {
        let reference = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        self.runtime
            .effect_completed(ticket)
            .map_err(|original| NodeHostRefusal::new("Root effect completion", original))?;
        let mut root = self.root.lock();
        if ordered {
            root.control_completion_observed(reference)
                .map_err(|original| NodeHostRefusal::new("Root ordered completion", original))?;
        } else {
            root.note_control_input_progress(reference.input);
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
                let projection = self.root.lock().resources().admission_epoch.clone();
                if let Err((error, original)) =
                    self.runtime
                        .commit_snapshot(&ticket, &projection, Arc::new(epoch))
                {
                    return Err(NodeHostRefusal::new(
                        "admission epoch publication",
                        (error, original, ticket),
                    ));
                }
                self.complete_root(ticket, true)
            }
            WorkerAction::ControlReply(_) => self
                .root
                .lock()
                .publish_request_reply(ticket)
                .map_err(|original| NodeHostRefusal::new("Root reply publication", original)),
            WorkerAction::Control {
                effect: ControlEffect::AdmissionReply { result: Ok(_), .. },
                ..
            } => {
                if let Err(error) = self.root.lock().adopt_ticket(&ticket) {
                    return Err(NodeHostRefusal::new("ticket adoption", (error, ticket)));
                }
                self.complete_root(ticket, true)
            }
            WorkerAction::Control {
                effect:
                    ControlEffect::AdmissionReply { result: Err(_), .. }
                    | ControlEffect::DomainFailureReply { .. }
                    | ControlEffect::Reply { .. },
                ..
            } => {
                // These neutral markers are followed by the independently
                // addressed full ControlReply. They invoke no host operation.
                self.complete_root(ticket, true)
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
                {
                    let mut rows = self.rows.lock().expect("original Context route rows");
                    if rows.contexts.len() >= self.config.max_contexts
                        || rows.contexts.contains_key(&context)
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
                            route: PublishedSnapshot::new(
                                &self.root.lock().resources().scope,
                                Arc::new(ContextRoute {
                                    state: QueryContextState::Establishing,
                                    generation,
                                }),
                            ),
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
                self.complete_root(ticket, false)
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
                self.complete_root(ticket, false)
            }
            WorkerAction::Control {
                effect: ControlEffect::RemoveHost { context },
                ..
            } => {
                let Some(target) = self.directory.context(context) else {
                    return Err(NodeHostRefusal::new("Context removal address", ticket));
                };
                self.root
                    .handoff_context_removal(ticket, target)
                    .map_err(|original| NodeHostRefusal::new("Context holder removal", original))
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
                } else if let Err(error) = self.root.lock().retire_ticket(&ticket) {
                    return Err(NodeHostRefusal::new(
                        "ticket holder retirement",
                        (error, ticket),
                    ));
                }
                self.complete_root(ticket, true)
            }
            // Continuation hints are consumed by the pure actor projection.
            // A Context action or Root failure is never a default host ACK.
            _ => Err(NodeHostRefusal::new("unexpected Root action", ticket)),
        }
    }
    fn advance_parent_rows(&self, budget: usize) -> Result<bool, NodeHostRefusal> {
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
                .advance_parent_teardown()
                .map_err(|original| {
                    NodeHostRefusal::new("Context original parent teardown", original)
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
        turn: ActorHostTurn<WorkerActor>,
        budget: usize,
    ) -> Result<NodeHostProgress, NodeHostRefusal> {
        let Some(address) = self.directory.host_turn(&turn) else {
            return Err(NodeHostRefusal::new("original Context host turn", turn));
        };
        let Some(mut host) = address.lock() else {
            return Err(NodeHostRefusal::new("removed Context host turn", turn));
        };
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
        let progress = NodeHostProgress {
            context_effects: advanced.effects,
            runnable: advanced.runnable,
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
            rows.removed_ready.push_back(address.context());
            drop(rows);
            drop(host);
            drop(pure);
            self.directory.remove_original(&address);
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
        if let Some(address) = context.and_then(|context| self.directory.context(context)) {
            if let Some(host) = address.lock() {
                // A bounded diagnostic prefix, never an exit predicate or a
                // request-path global owner scan.
                host.trace_drain_inventory(8);
            }
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
        // This is the same ledger retained at original composition, without
        // a Root/Context inventory lock, enumeration or foreign callback.
        super::node_metrics::WorkerNodeMetricsSnapshot::sample(&self.metrics_ledger)
    }
}
