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
    establish_executors::EstablishExecutors,
    host_executor::{HostExecutor, HostExecutorLimits, HostPairShutdownError, HostShutdownError},
    loading_host_job::LoadingHostJob,
    node::{NodeActorIds, NodeHostRefusal, NodeParticipationConfig, WorkerNodeHost},
    node_geometry::{NodeGeometry, NodeGeometryError},
    participation_executors::{ParticipationExecutorShutdownError, ParticipationExecutors},
    participation_host::{TaskIoPorts, task_io::TaskIoHostJob},
    process_control::{ProcessControlBinding, ProcessControlOwner},
};
use crate::{ParticipationHostFactory, TaskExecutionHost, TaskExecutionPorts};
use novarocks_control_runtime::{
    actor::ActorConfig,
    actor_turn::{ActorRuntime, Clock, PollerPool},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::identity::{ActorId, Generation};
use novarocks_execution_contract::AdmissionEpochCapability;
use novarocks_types::{BackendProcessId, NativeCompatibilityId};
use novarocks_worker_protocol::{
    admission::AdmissionTicketConfig,
    control::{ControlConfig, ControlCore},
    lease::MonotonicInstant,
    participation::{ParticipationConfig, ParticipationCore},
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

type LoadingPool = HostExecutor<LoadingHostJob, fn(LoadingHostJob)>;
type TaskPool = HostExecutor<TaskIoHostJob, fn(TaskIoHostJob)>;

#[derive(Clone, Copy, Debug)]
pub struct NodeWorkers {
    pub pollers: NonZeroUsize,
    pub participation_materialization: NonZeroUsize,
    pub participation_cleanup: NonZeroUsize,
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
    pub observation: super::participation_host::observation::ObservationProjection,
    pub history: Option<novarocks_control_runtime::history::HistoryCapture>,
    pub backend: BackendProcessId,
    pub compatibility: NativeCompatibilityId,
    pub admission_epoch: AdmissionEpochCapability,
    pub root_actor: ActorId,
    pub next_actor: NonZeroU64,
    pub generation: Generation,
    pub clock: Arc<dyn Clock>,
    pub participation_factory: Arc<dyn ParticipationHostFactory>,
    pub execution: Arc<dyn TaskExecutionHost>,
    pub execution_ports: TaskExecutionPorts,
    pub control: ControlConfig,
    pub participation: ParticipationConfig,
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
    ParticipationPools,
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
    Core(novarocks_worker_protocol::participation::CoreError),
    OriginalOwner,
    Spawn(std::io::Error),
}
impl std::fmt::Display for NodeBootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Geometry(error) => write!(f, "Worker Node geometry is invalid: {error}"),
            Self::CapacityIndex(error) => write!(
                f,
                "Worker Node capacity index initialization failed: {error}"
            ),
            Self::InvalidConfiguration(reason) => {
                write!(f, "Worker Node configuration is invalid: {reason}")
            }
            Self::Core(error) => write!(
                f,
                "Worker Node protocol Core initialization failed: {error:?}"
            ),
            Self::OriginalOwner => {
                f.write_str("Worker Node startup could not transfer its original owner")
            }
            Self::Spawn(error) => write!(f, "Worker Node thread creation failed: {error}"),
        }
    }
}
impl std::error::Error for NodeBootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Geometry(error) => Some(error),
            Self::CapacityIndex(error) => Some(error),
            Self::Spawn(error) => Some(error),
            Self::InvalidConfiguration(_) | Self::Core(_) | Self::OriginalOwner => None,
        }
    }
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
    participations: Option<ParticipationExecutors>,
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
pub(crate) struct NodeExecutorGeometry {
    pub participation_materialization: HostExecutorLimits,
    pub participation_cleanup: HostExecutorLimits,
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
impl NodeExecutorGeometry {
    fn checked(config: &NodeBootstrapConfig) -> Result<Self, NodeGeometryError> {
        let g = &config.geometry;
        let t = g.inputs.tasks_per_participation.get();
        let a = g.inputs.active_tasks.get();
        let p = g.inputs.preparing_tasks.get();
        let r = g.request_positions.get();
        // Participation queues share the configured ingress window, not a limit on
        // live or retained directory records. Full retains the same pending
        // job. N covers actual running tails after their original report.
        let participation_limits = |workers: NonZeroUsize, roles: usize| {
            Ok::<_, NodeGeometryError>(HostExecutorLimits {
                jobs: nz(add(mul(r, roles)?, workers.get())?)?,
                per_group: nz(add(roles, workers.get())?)?,
                workers,
            })
        };
        let participation_materialization =
            participation_limits(config.workers.participation_materialization, 1)?;
        let participation_cleanup = participation_limits(config.workers.participation_cleanup, 2)?;
        let loading = HostExecutorLimits::for_loading(t, a, config.workers.loading.get())?;
        let per_preparing = p.min(config.participation.max_preparing).min(t);
        let preparation = HostExecutorLimits {
            jobs: nz(add(p, config.workers.preparation.get())?)?,
            // Preparation membership includes the actual running body tail.
            // Its later activation uses the paid lifecycle continuation slot.
            per_group: nz(t.min(add(per_preparing, config.workers.preparation.get())?))?,
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
        let projection_jobs = add(
            mul(g.inputs.policy.task_base.effects, a)?,
            mul(g.request_per_position.effects, r)?,
        )?;
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
            r,
        )?;
        let participation_projection = add(
            add(
                mul(g.inputs.policy.task_base.effects, t)?,
                super::participation_host::PARTICIPATION_BOOTSTRAP_STORAGE.effects,
            )?,
            mul(g.request_per_position.effects, r)?,
        )?;
        let local_jobs = add(
            add(
                add(
                    participation_projection.min(projection_jobs),
                    mul(fixed_task_jobs, a.min(t))?,
                )?,
                request_jobs,
            )?,
            1,
        )?;
        let lifecycle = HostExecutorLimits {
            // R has its notification and observation bodies; a retained-root sweep has
            // one Participation effect/capture. N accounts for real running tails
            // after their logical holder/role can already have retired.
            jobs: nz(add(global_jobs, config.workers.lifecycle.get())?)?,
            per_group: nz(add(local_jobs, config.workers.lifecycle.get())?)?,
            workers: config.workers.lifecycle,
        };
        Ok(Self {
            participation_materialization,
            participation_cleanup,
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
fn run_task(job: TaskIoHostJob) {
    job.run_on_host();
}

/// Owns every actual physical executor and Common poller, alongside the one
/// Node authority. The application must begin drain, keep servicing its actual
/// timers/exits, then explicitly close/join only after runtime+ledger empty.
#[must_use = "The actual Node must drain and explicitly join its original pools"]
pub(crate) struct OwnedNodeRuntime {
    state: ManuallyDrop<Box<NodeStartupState>>,
}
#[derive(Debug)]
pub(crate) enum NodeShutdownError {
    StillLive,
    Loading(HostShutdownError),
    Preparation(HostShutdownError),
    Lifecycle(HostShutdownError),
    Participation(ParticipationExecutorShutdownError),
    Establish(HostPairShutdownError),
    PollersStillLive,
}
impl std::fmt::Display for NodeShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StillLive => {
                f.write_str("Worker Node still owns live execution or control inventory")
            }
            Self::Loading(error) => {
                write!(f, "Worker Node loading pool shutdown failed: {error:?}")
            }
            Self::Preparation(error) => {
                write!(f, "Worker Node preparation pool shutdown failed: {error:?}")
            }
            Self::Lifecycle(error) => {
                write!(f, "Worker Node lifecycle pool shutdown failed: {error:?}")
            }
            Self::Participation(error) => {
                write!(
                    f,
                    "Worker Node Participation pools shutdown failed: {error:?}"
                )
            }
            Self::Establish(error) => write!(
                f,
                "Worker Node establishment pools shutdown failed: {error:?}"
            ),
            Self::PollersStillLive => f.write_str("Worker Node pollers have not physically exited"),
        }
    }
}
impl std::error::Error for NodeShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StillLive
            | Self::Loading(_)
            | Self::Preparation(_)
            | Self::Lifecycle(_)
            | Self::Participation(_)
            | Self::Establish(_)
            | Self::PollersStillLive => None,
        }
    }
}

impl OwnedNodeRuntime {
    pub(crate) fn start(config: NodeBootstrapConfig) -> Result<Self, NodeBootstrapRefusal> {
        let mut state = NodeStartupState {
            config,
            runtime: None,
            ledger: None,
            root: None,
            establish: None,
            participations: None,
            loading: None,
            preparation: None,
            lifecycle: None,
            node: None,
            pollers: None,
            refused: None,
        };
        let g = &state.config.geometry;
        let invalid = if state.config.participation.max_tasks
            != g.inputs.tasks_per_participation.get()
            || state.config.control.max_pending_requests != g.request_positions.get()
            || state.config.participation.max_pending_requests != g.request_positions.get()
            || state.config.control.step_budget != ControlCore::MAX_STEP_WORK
            || state.config.participation.step_budget != ParticipationCore::MAX_STEP_WORK
            || state.config.participation.max_preparing == 0
            || state.config.participation.max_preparing > state.config.participation.max_tasks
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
        let physical = match NodeExecutorGeometry::checked(&state.config) {
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
            state.config.backend,
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
        let observation = novarocks_control_runtime::observation::RuntimeObservation::new(Some(
            state.config.clock.clone(),
        ));
        let runtime =
            ActorRuntime::with_observation(state.config.history.clone(), observation.clone());
        let ledger = match RuntimeLedger::with_owner_usage_dimensions_and_observation(
            state.config.geometry.limits.clone(),
            [crate::control_admission::Dimension::PreparingTasks.id()],
            state.config.history.clone(),
            observation,
        ) {
            Ok(ledger) => ledger,
            Err(error) => {
                return Err(state.refuse(
                    NodeBootstrapPhase::Validate,
                    NodeBootstrapError::CapacityIndex(error),
                ));
            }
        };
        // Owner-index storage follows actual reservations. Its current row
        // count is observation, not a prepaid admission quota or startup gate.
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
        match state
            .root
            .as_mut()
            .unwrap()
            .service()
            .start_participation_executors(
                physical.participation_materialization,
                physical.participation_cleanup,
                signal.clone(),
            ) {
            Ok(owner) => state.participations = Some(owner),
            Err(original) => {
                state.refused = Some(Box::new(original));
                return Err(state.refuse(
                    NodeBootstrapPhase::ParticipationPools,
                    NodeBootstrapError::OriginalOwner,
                ));
            }
        }
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
        let runner: fn(TaskIoHostJob) = run_task;
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
        let participations = state
            .participations
            .as_ref()
            .and_then(ParticipationExecutors::handles)
            .expect("complete original Participation pools");
        let ports = TaskIoPorts {
            #[cfg(test)]
            close_handoff_gate: None,
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
            participations,
            ports,
            establish,
            state.config.participation_factory.clone(),
            NodeParticipationConfig {
                core: state.config.participation,
                policy: state.config.geometry.inputs.policy,
                storage: state.config.geometry.admission.clone(),
                max_live_tasks: state.config.geometry.inputs.tasks_per_participation.get(),
                max_live_roots: state.config.geometry.inputs.tasks_per_participation.get(),
            },
            NodeActorIds::new(state.config.next_actor),
            state.config.clock.clone(),
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
        match node.start_pollers(state.config.workers.pollers.get()) {
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
        if let Some(owner) = self.state.participations.take()
            && let Err((error, owner)) = owner.shutdown()
        {
            self.state.participations = Some(owner);
            return Err((NodeShutdownError::Participation(error), self));
        }
        if let Some(owner) = self.state.establish.take()
            && let Err((error, owner)) = owner.shutdown()
        {
            self.state.establish = Some(owner);
            return Err((NodeShutdownError::Establish(error), self));
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
        if let Some(owner) = self.state.pollers.take()
            && let Err(owner) = owner.shutdown()
        {
            self.state.pollers = Some(owner);
            return Err((NodeShutdownError::PollersStillLive, self));
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
    control::ControlConfig as NodeControlConfig,
    participation::ParticipationConfig as NodeParticipationCoreConfig,
};
pub const NODE_CONTROL_STEP_BUDGET: usize = ControlCore::MAX_STEP_WORK;
pub const NODE_PARTICIPATION_STEP_BUDGET: usize = ParticipationCore::MAX_STEP_WORK;

/// The actual role composition supplies its original clock and foreign ports.
/// Checked geometry carries no runtime authority and is revalidated at start.
pub struct WorkerNodeConfig {
    pub observation: super::participation_host::observation::ObservationProjection,
    pub backend: BackendProcessId,
    pub compatibility: NativeCompatibilityId,
    pub initial_admission_epoch: AdmissionEpochCapability,
    pub clock: Arc<dyn crate::WorkerMonotonicClock>,
    pub participation_factory: Arc<dyn ParticipationHostFactory>,
    pub execution: Arc<dyn TaskExecutionHost>,
    pub execution_ports: TaskExecutionPorts,
    pub control: NodeControlConfig,
    pub participation: NodeParticipationCoreConfig,
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
                NodeBootstrapPhase::ParticipationPools => "participation pools",
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
        write!(f, "Worker Node startup refused at {}: ", self.stage())?;
        match &*self.original {
            WorkerNodeStartOriginal::Configuration { error, .. } => write!(f, "{error}"),
            WorkerNodeStartOriginal::Startup(original) => write!(f, "{}", original.error),
        }
    }
}
impl std::error::Error for WorkerNodeStartRefusal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &*self.original {
            WorkerNodeStartOriginal::Configuration { error, .. } => Some(error),
            WorkerNodeStartOriginal::Startup(original) => Some(&original.error),
        }
    }
}

#[derive(Debug)]
pub struct WorkerNodeShutdownError(NodeShutdownError);
impl std::fmt::Display for WorkerNodeShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Worker Node shutdown remains incomplete: {}", self.0)
    }
}
impl std::error::Error for WorkerNodeShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// The sole process-owned Node and all of its original physical joins.
/// The control handle is cloneable; cloning it does not clone this owner.
#[must_use = "Drain the actual Node and explicitly join its physical workers"]
pub struct WorkerNodeRuntime {
    original: OwnedNodeRuntime,
    clock: Arc<dyn crate::WorkerMonotonicClock>,
}
impl WorkerNodeRuntime {
    pub fn start(config: WorkerNodeConfig) -> Result<Self, WorkerNodeStartRefusal> {
        Self::start_observed(config, None)
    }
    /// The observer retains a clone and drains this same bounded capture.
    /// Driver and original ledger publish into it; the normal constructor
    /// allocates no history. Overflow marks the history incomplete and never
    /// changes Task decisions, work admission or physical shutdown.
    pub fn start_with_history(
        config: WorkerNodeConfig,
        history: novarocks_control_runtime::history::HistoryCapture,
    ) -> Result<Self, WorkerNodeStartRefusal> {
        Self::start_observed(config, Some(history))
    }
    fn start_observed(
        config: WorkerNodeConfig,
        history: Option<novarocks_control_runtime::history::HistoryCapture>,
    ) -> Result<Self, WorkerNodeStartRefusal> {
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
            history,
            backend: config.backend,
            compatibility: config.compatibility,
            admission_epoch: config.initial_admission_epoch,
            // This owner creates a fresh actual runtime and ledger. The first
            // Root and generation, and its next-ID source, belong to it.
            root_actor: ActorId::new(1).expect("fresh Root identity"),
            next_actor: NonZeroU64::new(2).expect("fresh next actor identity"),
            generation: Generation::new(1).expect("fresh Root generation"),
            clock: Arc::new(NodeClock(clock.clone())),
            participation_factory: config.participation_factory,
            execution: config.execution,
            execution_ports: config.execution_ports,
            control: config.control,
            participation: config.participation,
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
        crate::WorkerTaskControl::from_node(
            self.original.node().clone(),
            self.clock.clone(),
            self.original.backend(),
        )
    }
    pub fn now(&self) -> crate::MonotonicInstant {
        self.clock.now()
    }
    /// The owner of a replaceable clock signals after advancing its timeline.
    /// This only wakes the original pollers: they read that same clock and
    /// author their own due timer inputs. It grants no work or expiry fact.
    pub fn notify_clock_changed(&self) {
        self.original.runtime().notify_pollers();
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
