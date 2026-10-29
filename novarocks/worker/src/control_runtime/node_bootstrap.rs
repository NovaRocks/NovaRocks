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

//! The one actual process Node startup and its owned physical joins.
//! No request facade escapes before every original pool and Common poller
//! starts. Refusal retains the same partially started owners and captures.

use super::{
    actors::{ControlActor, WorkerActor},
    context_executors::{ContextExecutorShutdownError, ContextExecutors},
    context_host::{TaskDriverPorts, task_driver::TaskDriverHostJob},
    establish_executors::EstablishExecutors,
    host_executor::{HostExecutor, HostExecutorLimits, HostPairShutdownError, HostShutdownError},
    loading_host_job::LoadingHostJob,
    node::{NodeActorIds, NodeContextConfig, NodeHostRefusal, WorkerNodeHost},
    node_geometry::{NodeGeometry, NodeGeometryError},
    process_control::{ProcessControlBinding, ProcessControlOwner},
};
use crate::{QueryContextHostFactory, TaskExecutionHost, TaskExecutionPorts};
use novarocks_control_runtime::{
    actor::ActorConfig,
    driver::{ActorRuntime, Clock, PollerPool},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::identity::{ActorId, Generation};
use novarocks_execution_contract::AdmissionEpochCapability;
use novarocks_types::{BackendProcessId, NativeCompatibilityId};
use novarocks_worker_protocol::{
    admission::AdmissionTicketConfig,
    context::{ContextConfig, ContextCore},
    control::{ControlConfig, ControlCore},
    lease::MonotonicInstant,
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

type LoadingPool = HostExecutor<LoadingHostJob, fn(LoadingHostJob)>;
type TaskPool = HostExecutor<TaskDriverHostJob, fn(TaskDriverHostJob)>;

#[derive(Clone, Copy, Debug)]
pub struct NodeWorkers {
    pub pollers: NonZeroUsize,
    pub context_materialization: NonZeroUsize,
    pub context_cleanup: NonZeroUsize,
    pub establish_getters: NonZeroUsize,
    pub establish_cleanup: NonZeroUsize,
    pub loading: NonZeroUsize,
    pub preparation: NonZeroUsize,
    pub lifecycle: NonZeroUsize,
}

/// All identities and clocks come from the actual role composition. The
/// capability supplies this process's issuer; its later published epoch comes
/// from the same Root's real committed projection, never this initial value.
pub(crate) struct NodeBootstrapConfig {
    pub observation: super::context_host::observation::ObservationProjection,
    pub backend: BackendProcessId,
    pub compatibility: NativeCompatibilityId,
    pub admission_epoch: AdmissionEpochCapability,
    pub root_actor: ActorId,
    pub next_actor: NonZeroU64,
    pub generation: Generation,
    pub clock: Arc<dyn Clock>,
    pub context_factory: Arc<dyn QueryContextHostFactory>,
    pub execution: Arc<dyn TaskExecutionHost>,
    pub execution_ports: TaskExecutionPorts,
    pub control: ControlConfig,
    pub context: ContextConfig,
    pub tickets: AdmissionTicketConfig,
    pub geometry: NodeGeometry,
    pub workers: NodeWorkers,
    pub cadence: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NodeBootstrapPhase {
    Validate,
    Root,
    EstablishPools,
    ContextPools,
    CapacitySource,
    LoadingPool,
    PreparationPool,
    LifecyclePool,
    Composition,
    Pollers,
}
#[derive(Debug)]
pub(crate) enum NodeBootstrapError {
    Geometry(NodeGeometryError),
    CapacityIndex(novarocks_control_runtime::transaction::RuntimeError),
    InvalidConfiguration(&'static str),
    Core(novarocks_worker_protocol::context::CoreError),
    OriginalOwner,
    Spawn(std::io::Error),
}

/// This carrier does not destroy partially started workers, original Root
/// inventory, refused callbacks or supplied foreign ports on an implicit Drop.
/// The application retains it for its actual startup-failure policy.
#[must_use = "Retain every original partially started Node owner"]
pub(crate) struct NodeBootstrapRefusal {
    pub phase: NodeBootstrapPhase,
    pub error: NodeBootstrapError,
    original: ManuallyDrop<Box<NodeStartupState>>,
}
impl std::fmt::Debug for NodeBootstrapRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeBootstrapRefusal")
            .field("phase", &self.phase)
            .field("error", &self.error)
            .field("retains_root", &self.original.root.is_some())
            .field("retains_node", &self.original.node.is_some())
            .field("retains_loading", &self.original.loading.is_some())
            .field("retains_preparation", &self.original.preparation.is_some())
            .field("retains_lifecycle", &self.original.lifecycle.is_some())
            .finish_non_exhaustive()
    }
}
struct NodeStartupState {
    config: NodeBootstrapConfig,
    runtime: Option<ActorRuntime<WorkerActor>>,
    ledger: Option<RuntimeLedger>,
    root: Option<ProcessControlOwner>,
    establish: Option<EstablishExecutors>,
    contexts: Option<ContextExecutors>,
    loading: Option<LoadingPool>,
    preparation: Option<TaskPool>,
    lifecycle: Option<TaskPool>,
    node: Option<Arc<WorkerNodeHost>>,
    pollers: Option<PollerPool<WorkerActor>>,
    // Exact failure originals, never a replacement owner or diagnostic copy.
    refused: Option<Box<dyn Any + Send>>,
}
impl NodeStartupState {
    fn refuse(self, phase: NodeBootstrapPhase, error: NodeBootstrapError) -> NodeBootstrapRefusal {
        NodeBootstrapRefusal {
            phase,
            error,
            original: ManuallyDrop::new(Box::new(self)),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TaskPhysicalGeometry {
    pub loading: HostExecutorLimits,
    pub preparation: HostExecutorLimits,
    pub lifecycle: HostExecutorLimits,
}
fn add(a: usize, b: usize) -> Result<usize, NodeGeometryError> {
    a.checked_add(b).ok_or(NodeGeometryError::Overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, NodeGeometryError> {
    a.checked_mul(b).ok_or(NodeGeometryError::Overflow)
}
fn nz(value: usize) -> Result<NonZeroUsize, NodeGeometryError> {
    NonZeroUsize::new(value).ok_or(NodeGeometryError::InvalidBootstrapManifest)
}
impl TaskPhysicalGeometry {
    fn checked(config: &NodeBootstrapConfig) -> Result<Self, NodeGeometryError> {
        let g = &config.geometry;
        let c = g.inputs.contexts.get();
        let t = g.inputs.tasks_per_context.get();
        let a = g.inputs.active_tasks.get();
        let p = g.inputs.preparing_tasks.get();
        let r = g.request_positions.get();
        let loading = HostExecutorLimits {
            contexts: nz(add(c, config.workers.loading.get())?)?,
            jobs: nz(add(a, config.workers.loading.get())?)?,
            // Task identities are cumulative in the exact original Context;
            // the old loader tail already has one of those T identities.
            per_context: nz(t)?,
            workers: config.workers.loading,
        };
        let per_preparing = p.min(config.context.max_preparing).min(t);
        let preparation = HostExecutorLimits {
            contexts: nz(add(c, config.workers.preparation.get())?)?,
            jobs: nz(add(p, config.workers.preparation.get())?)?,
            // Preparation membership includes the actual running body tail.
            // Its later activation uses the paid lifecycle continuation slot.
            per_context: nz(t.min(add(per_preparing, config.workers.preparation.get())?))?,
            workers: config.workers.preparation,
        };
        // One projection-disposal job per actual paid effect ticket. This is
        // an output-storage bound, not CPU work multiplied by a guess. Fixed
        // cleanup holders may also run without a normal Core effect in S8:
        // Stop/Retire(2), Settlement(1), Task Root seal(1), Task disposal(1),
        // and the original preparation's continuation(1): Activate or provider
        // rollback. The same owner cannot issue both: activation requires its
        // runnable, while rollback requires that no runnable was adopted.
        let fixed_task_jobs = add(super::task_host_call::HOST_CALL_KINDS.len(), 4)?;
        let projection_jobs = g.storage.effects;
        // Each R has independent notification and protocol-observation
        // captures. Both can await their actual physical exit concurrently.
        // Never-loaded Create disposal uses the same original R only after
        // both captures are idle and its external delivery has actually ended;
        // it cannot add a third concurrent queued capture for that R.
        let request_jobs = mul(r, 2)?;
        let global_jobs = add(
            add(
                add(projection_jobs, mul(fixed_task_jobs, a)?)?,
                request_jobs,
            )?,
            c,
        )?;
        let context_projection = add(
            add(
                mul(g.inputs.policy.task_base.effects, t)?,
                super::context_host::CONTEXT_BOOTSTRAP_STORAGE.effects,
            )?,
            mul(g.request_per_position.effects, r)?,
        )?;
        let local_jobs = add(
            add(
                add(
                    context_projection.min(projection_jobs),
                    mul(fixed_task_jobs, a.min(t))?,
                )?,
                request_jobs,
            )?,
            1,
        )?;
        let lifecycle = HostExecutorLimits {
            contexts: nz(add(c, config.workers.lifecycle.get())?)?,
            // R has its notification and observation bodies; a retained-root sweep has
            // one Context effect/capture. N accounts for real running tails
            // after their logical holder/role can already have retired.
            jobs: nz(add(global_jobs, config.workers.lifecycle.get())?)?,
            per_context: nz(add(local_jobs, config.workers.lifecycle.get())?)?,
            workers: config.workers.lifecycle,
        };
        Ok(Self {
            loading,
            preparation,
            lifecycle,
        })
    }
}

fn run_loading(job: LoadingHostJob) {
    if let Err(original) = job.run() {
        // The original paid return cannot ordinarily reject its own sender.
        // Preserve its actual custody across a shared-invariant process stop.
        std::mem::forget(original);
        std::process::abort();
    }
}
fn run_task(job: TaskDriverHostJob) {
    job.run_on_host();
}

/// Owns every actual physical executor and Common poller, alongside the one
/// Node authority. The application must begin drain, keep servicing its actual
/// timers/exits, then explicitly close/join only after runtime+ledger empty.
#[must_use = "The actual Node must drain and explicitly join its original pools"]
pub(crate) struct OwnedNodeRuntime {
    state: ManuallyDrop<Box<NodeStartupState>>,
    physical: TaskPhysicalGeometry,
}
#[derive(Debug)]
pub(crate) enum NodeShutdownError {
    StillLive,
    Loading(HostShutdownError),
    Preparation(HostShutdownError),
    Lifecycle(HostShutdownError),
    Context(ContextExecutorShutdownError),
    Establish(HostPairShutdownError),
    PollersStillLive,
}
impl OwnedNodeRuntime {
    pub(crate) fn start(config: NodeBootstrapConfig) -> Result<Self, NodeBootstrapRefusal> {
        let mut state = NodeStartupState {
            config,
            runtime: None,
            ledger: None,
            root: None,
            establish: None,
            contexts: None,
            loading: None,
            preparation: None,
            lifecycle: None,
            node: None,
            pollers: None,
            refused: None,
        };
        let g = &state.config.geometry;
        let invalid = if state.config.control.max_contexts != g.inputs.contexts.get()
            || state.config.context.max_tasks != g.inputs.tasks_per_context.get()
            || state.config.control.max_waiters != g.request_positions.get()
            || state.config.control.max_pending_requests != g.request_positions.get()
            || state.config.context.max_pending_requests != g.request_positions.get()
            || state.config.control.step_budget != ControlCore::MAX_STEP_WORK
            || state.config.context.step_budget != ContextCore::MAX_STEP_WORK
            || state.config.context.max_preparing == 0
            || state.config.context.max_preparing > state.config.context.max_tasks
            || state.config.tickets.max_reservations() != g.inputs.ticket_live.get()
            || state.config.tickets.max_records() != g.inputs.ticket_records.get()
            || state.config.next_actor.get() <= state.config.root_actor.get()
            || state.config.cadence.is_zero()
        {
            Some("Node configuration differs from its checked geometry")
        } else {
            None
        };
        if let Some(error) = invalid {
            return Err(state.refuse(
                NodeBootstrapPhase::Validate,
                NodeBootstrapError::InvalidConfiguration(error),
            ));
        }
        let physical = match TaskPhysicalGeometry::checked(&state.config) {
            Ok(geometry) => geometry,
            Err(error) => {
                return Err(state.refuse(
                    NodeBootstrapPhase::Validate,
                    NodeBootstrapError::Geometry(error),
                ));
            }
        };
        let now = MonotonicInstant::from_origin(Duration::from_nanos(state.config.clock.now().0));
        let core = match ControlCore::new(
            state.config.control,
            state.config.tickets,
            state.config.admission_epoch.issuer(),
            now,
        ) {
            Ok(core) => core,
            Err(error) => {
                return Err(state.refuse(
                    NodeBootstrapPhase::Validate,
                    NodeBootstrapError::Core(error),
                ));
            }
        };
        let runtime = ActorRuntime::new(state.config.geometry.max_actors);
        let ledger = match RuntimeLedger::with_owner_usage_dimensions(
            state.config.geometry.limits.clone(),
            state.config.geometry.reservations.total,
            state.config.geometry.max_scopes,
            [crate::control_admission::Dimension::PreparingTasks.id()],
        ) {
            Ok(ledger) => ledger,
            Err(error) => {
                return Err(state.refuse(
                    NodeBootstrapPhase::Validate,
                    NodeBootstrapError::CapacityIndex(error),
                ));
            }
        };
        if ledger
            .owner_usage_metadata_entries(crate::control_admission::Dimension::PreparingTasks.id())
            != Some(state.config.geometry.capacity_owner_usage_entries)
        {
            return Err(state.refuse(
                NodeBootstrapPhase::Validate,
                NodeBootstrapError::InvalidConfiguration(
                    "preparation owner index differs from actual Node geometry",
                ),
            ));
        }
        state.runtime = Some(runtime.clone());
        state.ledger = Some(ledger.clone());
        let r = state.config.geometry.request_positions.get();
        let root_config = (|| -> Result<ActorConfig, NodeGeometryError> {
            let step = ControlActor::MAX_STEP_WORK;
            Ok(ActorConfig {
                mailbox: [1, 1, r, 1],
                max_batches: add(r, 3)?,
                max_effects_per_step: ControlActor::MAX_STEP_ACTIONS,
                max_dependencies_per_effect: ControlActor::MAX_STEP_ACTIONS,
                turn_budget: mul(add(step, 1)?, 4)?,
                max_step_work: step,
            })
        })();
        let actor_config = match root_config {
            Ok(config) => config,
            Err(error) => {
                return Err(state.refuse(
                    NodeBootstrapPhase::Root,
                    NodeBootstrapError::Geometry(error),
                ));
            }
        };
        match ProcessControlOwner::bootstrap(
            &runtime,
            &ledger,
            core,
            ProcessControlBinding {
                actor: state.config.root_actor,
                generation: state.config.generation,
                actor_config,
                max_work: state.config.geometry.root_scope.max_work,
                max_slots: state.config.geometry.root_scope.max_slots,
                request_positions: state.config.geometry.request_positions,
                admission_storage: state.config.geometry.admission.clone(),
            },
        ) {
            Ok(root) => state.root = Some(root),
            Err(original) => {
                state.refused = Some(Box::new(original));
                return Err(
                    state.refuse(NodeBootstrapPhase::Root, NodeBootstrapError::OriginalOwner)
                );
            }
        }
        match state.root.as_mut().unwrap().start_establish_executors(
            state.config.workers.establish_getters,
            state.config.workers.establish_cleanup,
        ) {
            Ok(owner) => state.establish = Some(owner),
            Err(original) => {
                state.refused = Some(Box::new(original));
                return Err(state.refuse(
                    NodeBootstrapPhase::EstablishPools,
                    NodeBootstrapError::OriginalOwner,
                ));
            }
        }
        match state
            .root
            .as_mut()
            .unwrap()
            .service()
            .start_context_executors(
                state.config.workers.context_materialization,
                state.config.workers.context_cleanup,
            ) {
            Ok(owner) => state.contexts = Some(owner),
            Err(original) => {
                state.refused = Some(Box::new(original));
                return Err(state.refuse(
                    NodeBootstrapPhase::ContextPools,
                    NodeBootstrapError::OriginalOwner,
                ));
            }
        }
        let signal = match state.root.as_mut().unwrap().task_capacity_signal() {
            Ok(signal) => signal,
            Err(original) => {
                state.refused = Some(Box::new(original));
                return Err(state.refuse(
                    NodeBootstrapPhase::CapacitySource,
                    NodeBootstrapError::OriginalOwner,
                ));
            }
        };
        let runner: fn(LoadingHostJob) = run_loading;
        match HostExecutor::start_with_waker(physical.loading, runner, signal.waker()) {
            Ok(owner) => state.loading = Some(owner),
            Err((error, owner)) => {
                state.loading = Some(owner);
                return Err(state.refuse(
                    NodeBootstrapPhase::LoadingPool,
                    NodeBootstrapError::Spawn(error),
                ));
            }
        }
        let runner: fn(TaskDriverHostJob) = run_task;
        match HostExecutor::start_eligible_fifo_with_waker(
            physical.preparation,
            runner,
            signal.waker(),
        ) {
            Ok(owner) => state.preparation = Some(owner),
            Err((error, owner)) => {
                state.preparation = Some(owner);
                return Err(state.refuse(
                    NodeBootstrapPhase::PreparationPool,
                    NodeBootstrapError::Spawn(error),
                ));
            }
        }
        match HostExecutor::start_with_waker(physical.lifecycle, runner, signal.waker()) {
            Ok(owner) => state.lifecycle = Some(owner),
            Err((error, owner)) => {
                state.lifecycle = Some(owner);
                return Err(state.refuse(
                    NodeBootstrapPhase::LifecyclePool,
                    NodeBootstrapError::Spawn(error),
                ));
            }
        }
        let establish = state
            .establish
            .as_ref()
            .and_then(EstablishExecutors::handles)
            .expect("complete original source pools");
        let contexts = state
            .contexts
            .as_ref()
            .and_then(ContextExecutors::handles)
            .expect("complete original Context pools");
        let ports = TaskDriverPorts {
            protocol_events: Some(state.config.execution_ports.clone()),
            observation: Some(state.config.observation),
            execution: Some(state.config.execution.clone()),
            loading: Some(state.loading.as_ref().unwrap().handle()),
            preparation: Some(state.preparation.as_ref().unwrap().handle()),
            lifecycle: Some(state.lifecycle.as_ref().unwrap().handle()),
            cadence: state.config.cadence,
            capacity: Some(signal),
        };
        let node = WorkerNodeHost::from_started(
            state.root.take().unwrap(),
            contexts,
            ports,
            establish,
            state.config.context_factory.clone(),
            NodeContextConfig {
                core: state.config.context,
                policy: state.config.geometry.inputs.policy,
                storage: state.config.geometry.admission.clone(),
                max_live_tasks: state.config.geometry.inputs.tasks_per_context.get(),
                max_live_roots: state.config.geometry.inputs.tasks_per_context.get(),
                max_contexts: state.config.geometry.inputs.contexts.get(),
            },
            NodeActorIds::new(state.config.next_actor),
        );
        let node = match node {
            Ok(node) => Arc::new(node),
            Err(original) => {
                state.refused = Some(Box::new(original));
                return Err(state.refuse(
                    NodeBootstrapPhase::Composition,
                    NodeBootstrapError::OriginalOwner,
                ));
            }
        };
        state.node = Some(node.clone());
        match node.start_pollers(
            state.config.clock.clone(),
            state.config.workers.pollers.get(),
        ) {
            Ok(owner) => {
                let observation = owner.observation();
                // Store the original stop/join owner before the metadata bind.
                // Even a composition refusal retains its actual pool cleanup.
                state.pollers = Some(owner);
                if node.bind_control_plane_observation(observation).is_err() {
                    return Err(state.refuse(
                        NodeBootstrapPhase::Pollers,
                        NodeBootstrapError::OriginalOwner,
                    ));
                }
            }
            Err(error) => {
                return Err(state.refuse(
                    NodeBootstrapPhase::Pollers,
                    NodeBootstrapError::Spawn(error),
                ));
            }
        }
        Ok(Self {
            state: ManuallyDrop::new(Box::new(state)),
            physical,
        })
    }
    pub(crate) fn node(&self) -> &Arc<WorkerNodeHost> {
        self.state
            .node
            .as_ref()
            .expect("fully started original Node")
    }
    pub(crate) fn runtime(&self) -> &ActorRuntime<WorkerActor> {
        self.state.runtime.as_ref().expect("original runtime")
    }
    pub(crate) fn ledger(&self) -> &RuntimeLedger {
        self.state.ledger.as_ref().expect("original ledger")
    }
    pub(crate) fn backend(&self) -> BackendProcessId {
        self.state.config.backend
    }
    pub(crate) fn compatibility(&self) -> NativeCompatibilityId {
        self.state.config.compatibility
    }
    pub(crate) fn execution_ports(&self) -> &TaskExecutionPorts {
        &self.state.config.execution_ports
    }
    pub(crate) fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_nanos(self.state.config.clock.now().0))
    }
    pub(crate) fn begin_drain(&self) -> Result<(), NodeHostRefusal> {
        self.node().begin_drain()
    }
    pub(crate) fn is_drained(&self) -> bool {
        self.runtime().is_empty() && self.ledger().is_empty()
    }
    /// Nonblocking while logical/physical owners remain. The actual caller
    /// keeps this same runtime and its standard change source; no polling loop
    /// or timeout is substituted for original Work/source exit.
    pub(crate) fn try_shutdown(mut self) -> Result<(), (NodeShutdownError, Self)> {
        if !self.is_drained() {
            return Err((NodeShutdownError::StillLive, self));
        }
        if let Some(owner) = self.state.loading.take() {
            match owner.shutdown() {
                Ok(runner) => drop(runner),
                Err((error, owner)) => {
                    self.state.loading = Some(owner);
                    return Err((NodeShutdownError::Loading(error), self));
                }
            }
        }
        if let Some(owner) = self.state.preparation.take() {
            match owner.shutdown() {
                Ok(runner) => drop(runner),
                Err((error, owner)) => {
                    self.state.preparation = Some(owner);
                    return Err((NodeShutdownError::Preparation(error), self));
                }
            }
        }
        if let Some(owner) = self.state.contexts.take() {
            if let Err((error, owner)) = owner.shutdown() {
                self.state.contexts = Some(owner);
                return Err((NodeShutdownError::Context(error), self));
            }
        }
        if let Some(owner) = self.state.establish.take() {
            if let Err((error, owner)) = owner.shutdown() {
                self.state.establish = Some(owner);
                return Err((NodeShutdownError::Establish(error), self));
            }
        }
        // Cleanup remains available until all business/source pools joined.
        if let Some(owner) = self.state.lifecycle.take() {
            match owner.shutdown() {
                Ok(runner) => drop(runner),
                Err((error, owner)) => {
                    self.state.lifecycle = Some(owner);
                    return Err((NodeShutdownError::Lifecycle(error), self));
                }
            }
        }
        if let Some(owner) = self.state.pollers.take() {
            if let Err(owner) = owner.shutdown() {
                self.state.pollers = Some(owner);
                return Err((NodeShutdownError::PollersStillLive, self));
            }
        }
        // Only actual empty inventories and real physical joins reach here.
        // Remaining supplied ports/factory captures are dropped by this caller,
        // never by a Common poller or while a service metadata lock is held.
        drop(ManuallyDrop::into_inner(self.state));
        Ok(())
    }
}

// Native configuration uses neutral protocol values, never Common identities,
// runtime registrations or a caller-created ledger.
pub use novarocks_worker_protocol::{
    admission::AdmissionTicketConfig as NodeAdmissionTicketConfig,
    context::ContextConfig as NodeContextCoreConfig, control::ControlConfig as NodeControlConfig,
};
pub const NODE_CONTROL_STEP_BUDGET: usize = ControlCore::MAX_STEP_WORK;
pub const NODE_CONTEXT_STEP_BUDGET: usize = ContextCore::MAX_STEP_WORK;

/// The actual role composition supplies its original clock and foreign ports.
/// Checked geometry carries no runtime authority and is revalidated at start.
pub struct WorkerNodeConfig {
    pub observation: super::context_host::observation::ObservationProjection,
    pub backend: BackendProcessId,
    pub compatibility: NativeCompatibilityId,
    pub initial_admission_epoch: AdmissionEpochCapability,
    pub clock: Arc<dyn crate::WorkerMonotonicClock>,
    pub context_factory: Arc<dyn QueryContextHostFactory>,
    pub execution: Arc<dyn TaskExecutionHost>,
    pub execution_ports: TaskExecutionPorts,
    pub control: NodeControlConfig,
    pub context: NodeContextCoreConfig,
    pub tickets: NodeAdmissionTicketConfig,
    pub geometry: NodeGeometry,
    pub workers: NodeWorkers,
    pub cadence: Duration,
}

struct NodeClock(Arc<dyn crate::WorkerMonotonicClock>);
impl Clock for NodeClock {
    fn now(&self) -> novarocks_control_runtime_core::identity::Tick {
        // A different origin would silently reinterpret every actual deadline.
        // Overflow cannot be represented by this protocol's nanosecond Tick.
        let nanos = u64::try_from(self.0.now().since_origin().as_nanos())
            .unwrap_or_else(|_| std::process::abort());
        novarocks_control_runtime_core::identity::Tick(nanos)
    }
}

enum WorkerNodeStartOriginal {
    Configuration {
        error: NodeGeometryError,
        _original: Box<WorkerNodeConfig>,
    },
    Startup(NodeBootstrapRefusal),
}
/// Retains the actual supplied configuration or partially started owners.
/// An implicit Drop cannot destroy foreign captures or detached live workers.
#[must_use = "Retain the actual refused Node startup owners"]
pub struct WorkerNodeStartRefusal {
    original: ManuallyDrop<WorkerNodeStartOriginal>,
}
impl WorkerNodeStartRefusal {
    pub fn stage(&self) -> &'static str {
        match &*self.original {
            WorkerNodeStartOriginal::Configuration { .. } => "geometry",
            WorkerNodeStartOriginal::Startup(original) => match original.phase {
                NodeBootstrapPhase::Validate => "validate",
                NodeBootstrapPhase::Root => "root",
                NodeBootstrapPhase::EstablishPools => "establish pools",
                NodeBootstrapPhase::ContextPools => "context pools",
                NodeBootstrapPhase::CapacitySource => "capacity source",
                NodeBootstrapPhase::LoadingPool => "loading pool",
                NodeBootstrapPhase::PreparationPool => "preparation pool",
                NodeBootstrapPhase::LifecyclePool => "lifecycle pool",
                NodeBootstrapPhase::Composition => "node composition",
                NodeBootstrapPhase::Pollers => "pollers",
            },
        }
    }
}
impl std::fmt::Debug for WorkerNodeStartRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("WorkerNodeStartRefusal");
        d.field("stage", &self.stage());
        match &*self.original {
            WorkerNodeStartOriginal::Configuration { error, .. } => d.field("error", error),
            WorkerNodeStartOriginal::Startup(original) => d.field("original", original),
        };
        d.finish_non_exhaustive()
    }
}
impl std::fmt::Display for WorkerNodeStartRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Worker Node startup refused at {}: {self:?}",
            self.stage()
        )
    }
}
impl std::error::Error for WorkerNodeStartRefusal {}

#[derive(Debug)]
pub struct WorkerNodeShutdownError(NodeShutdownError);
impl std::fmt::Display for WorkerNodeShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Worker Node shutdown remains incomplete: {:?}", self.0)
    }
}
impl std::error::Error for WorkerNodeShutdownError {}

/// The sole process-owned Node and all of its original physical joins.
/// The control handle is cloneable; cloning it does not clone this owner.
#[must_use = "Drain the actual Node and explicitly join its physical workers"]
pub struct WorkerNodeRuntime {
    original: OwnedNodeRuntime,
    clock: Arc<dyn crate::WorkerMonotonicClock>,
}
impl WorkerNodeRuntime {
    pub fn start(config: WorkerNodeConfig) -> Result<Self, WorkerNodeStartRefusal> {
        // Recompute from the actual original manifest rather than trusting a
        // configuration-time aggregate or accepting a supplied RuntimeLedger.
        let geometry = match NodeGeometry::try_new(config.geometry.inputs) {
            Ok(geometry) => geometry,
            Err(error) => {
                return Err(WorkerNodeStartRefusal {
                    original: ManuallyDrop::new(WorkerNodeStartOriginal::Configuration {
                        error,
                        _original: Box::new(config),
                    }),
                });
            }
        };
        let clock = config.clock;
        let original = OwnedNodeRuntime::start(NodeBootstrapConfig {
            observation: config.observation,
            backend: config.backend,
            compatibility: config.compatibility,
            admission_epoch: config.initial_admission_epoch,
            // This owner creates a fresh actual runtime and ledger. The first
            // Root and generation, and its next-ID source, belong to it.
            root_actor: ActorId::new(1).expect("fresh Root identity"),
            next_actor: NonZeroU64::new(2).expect("fresh next actor identity"),
            generation: Generation::new(1).expect("fresh Root generation"),
            clock: Arc::new(NodeClock(clock.clone())),
            context_factory: config.context_factory,
            execution: config.execution,
            execution_ports: config.execution_ports,
            control: config.control,
            context: config.context,
            tickets: config.tickets,
            geometry,
            workers: config.workers,
            cadence: config.cadence,
        })
        .map_err(|original| WorkerNodeStartRefusal {
            original: ManuallyDrop::new(WorkerNodeStartOriginal::Startup(original)),
        })?;
        Ok(Self { original, clock })
    }
    pub fn task_control(&self) -> crate::WorkerTaskControl {
        crate::WorkerTaskControl::from_node(self.original.node().clone(), self.clock.clone())
    }
    pub fn now(&self) -> crate::MonotonicInstant {
        self.clock.now()
    }
    pub fn backend(&self) -> BackendProcessId {
        self.original.backend()
    }
    pub fn compatibility(&self) -> NativeCompatibilityId {
        self.original.compatibility()
    }
    pub fn execution_ports(&self) -> &TaskExecutionPorts {
        self.original.execution_ports()
    }
    pub fn begin_drain(&self) -> Result<(), crate::WorkerRequestError> {
        self.original.begin_drain().map_err(Into::into)
    }
    /// The role owner calls this only after every actual Native listener joined.
    /// It ends replay retention, never the original physical Work obligations.
    pub fn confirm_ingress_closed(&self) -> Result<(), crate::WorkerRequestError> {
        self.original
            .node()
            .confirm_ingress_closed()
            .map_err(Into::into)
    }
    pub fn is_drained(&self) -> bool {
        self.original.is_drained()
    }
    /// Wait on the original runtime change source. The caller closes admission
    /// first; only actual inventory exit satisfies this predicate.
    pub fn drain_and_join(self) -> Result<(), (WorkerNodeShutdownError, Self)> {
        loop {
            let version = self.original.runtime().change_version();
            if self.is_drained() {
                return self.try_shutdown();
            }
            tracing::debug!(
                process_id = std::process::id(),
                version,
                runtime_empty = self.original.runtime().is_empty(),
                ledger_empty = self.original.ledger().is_empty(),
                resources = ?self.original.node().metrics_snapshot(),
                "Worker Node awaiting original inventory exit"
            );
            self.original.node().trace_drain_inventory();
            self.original.runtime().wait_after(version);
        }
    }
    /// While any original owner remains live, return this same runtime. Once
    /// actually empty, close and join each original pool and Common poller.
    pub fn try_shutdown(self) -> Result<(), (WorkerNodeShutdownError, Self)> {
        let Self { original, clock } = self;
        original
            .try_shutdown()
            .map_err(|(error, original)| (WorkerNodeShutdownError(error), Self { original, clock }))
    }
}
