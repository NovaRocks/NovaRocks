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

//! The original Context admission owns its isolated actor scope. Bootstrap
//! only transfers the existing guard and establishes finite metadata roles;
//! it does not run materialization, publish Active, or infer any host exit.

mod driver;
pub(super) mod failure;
pub(super) mod failure_tail;
pub(crate) mod observation;
pub(crate) use driver::ContextDriverError;
pub(crate) mod root_read;
pub(crate) use root_read::{NodeRootDelivery, NodeRootRead, RootReadError};
pub(super) mod task_driver;
pub(crate) use task_driver::TaskDriverPorts;
mod internal;
mod owner_control;
mod teardown;
pub(crate) use internal::ContextInternalAdvance;
mod execution_observation;
mod progress;
mod root_census;
pub(crate) use owner_control::OwnerFenceDelivery;
pub(crate) use progress::{ContextHostProgress, ContextHostProgressError};

use super::{
    actors::{ContextActor, WorkerAction, WorkerActor},
    context_host_jobs::{ContextHostJobBinding, ContextHostJobError, ContextHostJobs},
    context_io::{CONTEXT_IO_CALLBACKS, CONTEXT_IO_STORAGE, ContextIoError, ContextIoInventory},
    context_lifecycle::{CONTEXT_LIFECYCLE_STORAGE, ContextLifecycle},
    context_ready::{ContextReadyError, ContextReadyNotice},
    effect_dispatch::{EffectDispatch, EffectRef},
    establish_route::{EstablishRouteCustody, EstablishRouteError},
    task::{TaskHostBinding, TaskHostError, TaskHostOwner, TaskHostRecord},
};
use crate::{
    control_admission::AdmissionStorage,
    control_runtime_policy::{ControlRuntimePolicy, StorageNeed},
};
use novarocks_control_runtime::{
    driver::{ActorRegistration, ActorRuntime, DriverError, EffectTicket},
    lease::{ReservationBundle, WorkChildAuthority, WorkLease},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    identity::{ActorId, InputId, ReservationId},
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{QueryContextRef, TaskIdentity};
use novarocks_worker_protocol::{context::ContextCore, control::ControlEffect};
use std::{collections::BTreeMap, sync::Arc};

// The actual child-scope metadata holders. The original admission and the
// child-scope parent link are the other two Work records, in the Control scope.
const LOCAL_ROLES: [WorkKind; 3] = [WorkKind::Step, WorkKind::Step, WorkKind::Cleanup];
const LIFETIME: usize = 0;
const TIMER: usize = 1;
const FAILURE: usize = 2;

/// Exactly this bootstrap's metadata, independent of Task/request/root pools.
/// Includes the fixed M/R/opaque-holder inventory. Jobs must bind those actual
/// roles and both callbacks before capture; shared-domain U is still separate.
pub(crate) const CONTEXT_BOOTSTRAP_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 1
        + novarocks_control_runtime::driver::ISOLATED_FAILURE_INPUT_POSITIONS
        + CONTEXT_IO_STORAGE.inputs
        + CONTEXT_LIFECYCLE_STORAGE.inputs
        + owner_control::OWNER_CONTROL_STORAGE.inputs
        + internal::CONTEXT_INTERNAL_STORAGE.inputs,
    batches: 1
        + novarocks_control_runtime::driver::ISOLATED_FAILURE_INPUT_POSITIONS
        + CONTEXT_IO_STORAGE.batches
        + CONTEXT_LIFECYCLE_STORAGE.batches
        + owner_control::OWNER_CONTROL_STORAGE.batches
        + internal::CONTEXT_INTERNAL_STORAGE.batches,
    effects: (1 + novarocks_control_runtime::driver::ISOLATED_FAILURE_INPUT_POSITIONS)
        * crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS
        + CONTEXT_IO_STORAGE.effects
        + CONTEXT_LIFECYCLE_STORAGE.effects
        + owner_control::OWNER_CONTROL_STORAGE.effects
        + internal::CONTEXT_INTERNAL_STORAGE.effects,
    cuts: 0,
    replies: 0,
    completions: CONTEXT_IO_STORAGE.completions,
    // The same failure Work serially retains one original projection,
    // abandoned neutral body or final panic and its physical return cell.
    captures: CONTEXT_IO_STORAGE.captures + failure_tail::FAILURE_TAIL_CAPTURES,
    work: LOCAL_ROLES.len() + 2 + CONTEXT_IO_STORAGE.work,
};

pub(crate) struct ContextHostBinding {
    pub actor: ActorId,
    pub core: ContextCore,
    pub policy: ControlRuntimePolicy,
    pub storage: AdmissionStorage,
    pub max_live_tasks: usize,
    pub max_live_roots: usize,
}

#[derive(Debug)]
pub(crate) enum ContextHostError {
    WrongEffect,
    WrongRuntime,
    WrongContext,
    WrongEstablishment,
    InvalidGeometry,
    InvalidStoragePolicy,
    BootstrapUnfunded,
    TaskAlreadyOwned,
    TaskTableFull,
    TaskNotOwned,
    TaskStillOwned,
    MissingLifecycleExecutor,
    MissingCapacitySignal,
    RemovalSubmit(super::host_executor::HostSubmitError),
    Capacity(super::task_capacity::TaskCapacityError),
    Driver(DriverError),
    Runtime(RuntimeError),
    AdmissionBindInvariant(RuntimeError),
    Task(TaskHostError),
    Io(ContextIoError),
    HostJob(ContextHostJobError),
    Ready(ContextReadyError),
    RootRead(root_read::RootReadError),
}
impl std::fmt::Display for ContextHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongEffect => f.write_str("Context host received the wrong effect"),
            Self::WrongRuntime => f.write_str("Context host belongs to another runtime"),
            Self::WrongContext => f.write_str("Context host identity does not match"),
            Self::WrongEstablishment => {
                f.write_str("Context host establishment does not match its original request")
            }
            Self::InvalidGeometry => f.write_str("Context host storage geometry is invalid"),
            Self::InvalidStoragePolicy => f.write_str("Context host storage policy is invalid"),
            Self::BootstrapUnfunded => {
                f.write_str("Context host bootstrap has no original funding")
            }
            Self::TaskAlreadyOwned => f.write_str("Context already owns the original Task"),
            Self::TaskTableFull => f.write_str("Context Task positions are full"),
            Self::TaskNotOwned => f.write_str("Context does not own this Task"),
            Self::TaskStillOwned => f.write_str("Context still owns the original Task inventory"),
            Self::MissingLifecycleExecutor => f.write_str("Context lifecycle executor is missing"),
            Self::MissingCapacitySignal => {
                f.write_str("Context capacity notification source is missing")
            }
            Self::RemovalSubmit(error) => write!(f, "Context removal submission failed: {error:?}"),
            Self::Capacity(error) => write!(f, "Context capacity notification failed: {error:?}"),
            Self::Driver(error) => write!(
                f,
                "Context host input or publication operation failed: {error}"
            ),
            Self::Runtime(error) => write!(f, "Context host role operation failed: {error}"),
            Self::AdmissionBindInvariant(error) => write!(
                f,
                "Context committed admission binding invariant failed: {error}"
            ),
            Self::Task(error) => write!(f, "Context Task ownership operation failed: {error}"),
            Self::Io(error) => write!(f, "Context I/O ownership operation failed: {error}"),
            Self::HostJob(error) => write!(f, "Context host job failed: {error}"),
            Self::Ready(error) => write!(f, "Context readiness notification failed: {error:?}"),
            Self::RootRead(original) => {
                // Only the closed boundary tag is inspected. Channel causes
                // may retain foreign failure objects whose formatter is not
                // safe to invoke from this metadata diagnostic.
                use root_read::RootReadError;
                f.write_str(match original {
                    RootReadError::ForeignProcess => "Root read targets another backend process",
                    RootReadError::UnknownContext => "Context Root read targets an unknown Context",
                    RootReadError::ContextRemoved => "Context Root read targets a removed Context",
                    RootReadError::Request(_) => {
                        "Context Root read request admission failed; original typed cause retained"
                    }
                    RootReadError::Task(_) => {
                        "Context Root Task read failed; original typed cause retained"
                    }
                    RootReadError::Channel(_) => {
                        "Context Root channel read failed; original typed cause retained"
                    }
                    RootReadError::UnknownTask => "Context Root read targets an unknown Task",
                    RootReadError::Capacity(_) => {
                        "Context Root read capacity was refused; original facts retained"
                    }
                    RootReadError::Closed { .. } => "Context Root read channel is sealed",
                    RootReadError::WrongOwner => "Context Root read belongs to another owner",
                })
            }
        }
    }
}
impl std::error::Error for ContextHostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Driver(error) => Some(error),
            Self::Runtime(error) | Self::AdmissionBindInvariant(error) => Some(error),
            Self::Task(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::HostJob(error) => Some(error),
            Self::WrongEffect
            | Self::WrongRuntime
            | Self::WrongContext
            | Self::WrongEstablishment
            | Self::InvalidGeometry
            | Self::InvalidStoragePolicy
            | Self::BootstrapUnfunded
            | Self::TaskAlreadyOwned
            | Self::TaskTableFull
            | Self::TaskNotOwned
            | Self::TaskStillOwned
            | Self::MissingLifecycleExecutor
            | Self::MissingCapacitySignal
            | Self::RemovalSubmit(_)
            | Self::Capacity(_)
            | Self::Ready(_)
            | Self::RootRead(_) => None,
        }
    }
}

pub(crate) struct ContextMetadataRole {
    pub work: WorkLease,
    pub capacity: ReservationBundle,
}

/// This is the original, possibly partial bootstrap inventory. Failure returns
/// it intact to the live host owner; it never drops a bound admission as unused
/// or reports a half-registered Context as successfully materialized.
#[must_use = "The host must retain the original Context payer and every admitted role"]
pub(crate) struct ContextHostResources {
    pub(super) observation: Option<observation::ContextObservation>,
    pub(super) observation_positions: usize,
    pub(super) observation_projection: Option<observation::ObservationProjection>,
    pub(in crate::control_runtime) root_reads: root_read::RootReadState,
    teardown: teardown::ContextTeardownState,
    pub(super) control_admission_closed: bool,
    pub(super) execution_close_epoch: Option<novarocks_control_runtime_core::identity::Generation>,
    pub admission: ContextMetadataRole,
    pub scope: Option<ScopeHandle>,
    pub roles: Vec<ContextMetadataRole>,
    pub registered: bool,
    registration: Option<ActorRegistration>,
    pub routes: BTreeMap<InputId, Arc<EstablishRouteCustody>>,
    pub owner_control: owner_control::OwnerControl,
    pub(in crate::control_runtime) internal: internal::ContextInternal,
    pub io: Option<ContextIoInventory>,
    pub host_jobs: Option<ContextHostJobs>,
    pending_host_jobs: progress::PendingHostJobs,
    pub lifecycle: Option<ContextLifecycle>,
    pub(super) removal: Option<super::context_removal::ContextRemovalOwner>,
    pub(super) removal_job: Option<task_driver::TaskDriverHostJob>,
    pub notice: Option<ContextReadyNotice>,
    io_attempted: bool,
    pub requests: BTreeMap<
        novarocks_control_runtime_core::identity::ScopedWorkRef,
        requests::ContextRequestCustody,
    >,
    failure: Option<failure::ScopeFailureOwner>,
    pub request_partials: BTreeMap<
        novarocks_control_runtime_core::identity::ScopedWorkRef,
        requests::ContextRequestAdmissionPartial,
    >,
}

pub(crate) struct ContextHostRefusal {
    pub error: ContextHostError,
    pub resources: Option<Box<ContextHostResources>>,
}
impl std::fmt::Debug for ContextHostRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextHostRefusal")
            .field("error", &self.error)
            .field("retains_original_admission", &self.resources.is_some())
            .finish()
    }
}

/// The Context's actual host-side owner. This table contains the original
/// Task custodians, rather than copies of their protocol state or quota counts.
pub(crate) struct ContextHostRecord {
    identity: QueryContextRef,
    actor: ActorId,
    control: ActorId,
    control_registration: ActorRegistration,
    // The original Materialize ticket was validated by this exact runtime.
    // A caller-supplied runtime or matching numeric IDs cannot replace it.
    control_runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    /// Original process ingress Scope; retained requests never derive Work
    /// from the Context execution lifetime or its parent-link.
    request_scope: ScopeHandle,
    /// Issued only by this process Root's actual baseline Work. This does not
    /// keep a Context execution owner or a caller's R alive.
    process_source_parent: WorkChildAuthority,
    establishment: novarocks_worker_protocol::context::ContextEstablishment,
    initial_generation: novarocks_control_runtime_core::identity::Generation,
    request_horizon: novarocks_worker_protocol::lease::RequestHorizon,
    resources: ContextHostResources,
    tasks: BTreeMap<TaskIdentity, TaskHostOwner>,
    retired_task_reads: BTreeMap<TaskIdentity, Arc<super::lifecycle_read::TaskReadSnapshot>>,
    task_read_waiters: requests::deferred_read::TaskReadWaiters,
    task_driver: task_driver::TaskDriverState,
    root_issuance_sealed: bool,
    max_live_tasks: usize,
    max_routes: usize,
    pub dispatch: EffectDispatch,
    request_inputs: BTreeMap<InputId, novarocks_control_runtime_core::identity::ScopedWorkRef>,
    task_request_cuts: BTreeMap<
        (
            TaskIdentity,
            novarocks_worker_protocol::proposal_summary::CutId,
        ),
        novarocks_control_runtime_core::identity::ScopedWorkRef,
    >,
}
impl ContextHostRecord {
    #[cfg(test)]
    pub(crate) fn adopt(
        runtime: &ActorRuntime<WorkerActor>,
        ledger: &RuntimeLedger,
        ticket: &EffectTicket<WorkerAction>,
        binding: ContextHostBinding,
        process_source_parent: WorkChildAuthority,
    ) -> Result<Self, ContextHostRefusal> {
        Self::adopt_with_routes(runtime, ledger, ticket, binding, 0, process_source_parent)
    }
    pub(crate) fn adopt_with_routes(
        runtime: &ActorRuntime<WorkerActor>,
        ledger: &RuntimeLedger,
        ticket: &EffectTicket<WorkerAction>,
        binding: ContextHostBinding,
        max_routes: usize,
        process_source_parent: WorkChildAuthority,
    ) -> Result<Self, ContextHostRefusal> {
        let no_owner = |error| ContextHostRefusal {
            error,
            resources: None,
        };
        let control_registration = runtime
            .registration_from_effect(ticket)
            .map_err(|error| no_owner(ContextHostError::Driver(error)))?;
        let (context, reservation, generation, establishment) = match &ticket.effect {
            WorkerAction::Control {
                effect:
                    ControlEffect::Materialize {
                        context,
                        reservation,
                        generation,
                        establishment,
                        ..
                    },
                admitted: Some(original),
            } if original == reservation => (*context, *reservation, *generation, *establishment),
            _ => return Err(no_owner(ContextHostError::WrongEffect)),
        };
        if binding.core.identity() != context || binding.core.generation() != generation {
            return Err(no_owner(ContextHostError::WrongContext));
        }
        if binding.core.establishment() != establishment {
            return Err(no_owner(ContextHostError::WrongEstablishment));
        }
        let request_horizon = binding.core.request_horizon();
        let loading = binding
            .policy
            .loading
            .resources()
            .map_err(|_| no_owner(ContextHostError::InvalidGeometry))?;
        let accepted = binding
            .policy
            .accepted
            .resources()
            .map_err(|_| no_owner(ContextHostError::InvalidGeometry))?;
        if binding.storage.loading != loading || binding.storage.accepted != accepted {
            return Err(no_owner(ContextHostError::InvalidStoragePolicy));
        }
        let mut actor_config = binding
            .policy
            .context_actor(binding.max_live_tasks)
            .map_err(|_| no_owner(ContextHostError::InvalidGeometry))?;
        actor_config.mailbox[InputClass::Control.index()] = actor_config.mailbox
            [InputClass::Control.index()]
        .checked_add(max_routes)
        .and_then(|n| n.checked_add(CONTEXT_IO_CALLBACKS))
        .and_then(|n| n.checked_add(owner_control::OWNER_CONTROL_STORAGE.inputs))
        .and_then(|n| n.checked_add(internal::CONTEXT_INTERNAL_STORAGE.inputs))
        .ok_or_else(|| no_owner(ContextHostError::InvalidGeometry))?;
        actor_config.max_batches = actor_config
            .max_batches
            .checked_add(max_routes)
            .and_then(|n| n.checked_add(CONTEXT_IO_CALLBACKS))
            .and_then(|n| n.checked_add(owner_control::OWNER_CONTROL_STORAGE.batches))
            .and_then(|n| n.checked_add(internal::CONTEXT_INTERNAL_STORAGE.batches))
            .ok_or_else(|| no_owner(ContextHostError::InvalidGeometry))?;
        let effect_limit = actor_config
            .max_batches
            .checked_mul(crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS)
            .ok_or_else(|| no_owner(ContextHostError::InvalidGeometry))?;
        let need = CONTEXT_BOOTSTRAP_STORAGE
            .resources()
            .map_err(|_| no_owner(ContextHostError::InvalidGeometry))?;
        let guard = runtime
            .take_step_reservation(ticket, reservation)
            .map_err(|error| no_owner(ContextHostError::Driver(error)))?;
        let (work, capacity) = guard
            .bind(WorkKind::Business)
            .map_err(|error| no_owner(ContextHostError::AdmissionBindInvariant(error)))?;
        let mut resources = ContextHostResources {
            observation: None,
            observation_positions: binding.policy.request_pool.requests,
            observation_projection: None,
            root_reads: root_read::RootReadState::new(
                binding.max_live_roots,
                binding.policy.ingress.root_read_holders,
            ),
            teardown: Default::default(),
            control_admission_closed: false,
            execution_close_epoch: None,
            failure: None,
            admission: ContextMetadataRole { work, capacity },
            scope: None,
            roles: Vec::with_capacity(LOCAL_ROLES.len()),
            registered: false,
            registration: None,
            routes: BTreeMap::new(),
            owner_control: owner_control::OwnerControl::default(),
            internal: internal::ContextInternal::default(),
            io: None,
            host_jobs: None,
            pending_host_jobs: Default::default(),
            lifecycle: None,
            removal: None,
            removal_job: None,
            notice: None,
            io_attempted: false,
            requests: BTreeMap::new(),
            request_partials: BTreeMap::new(),
        };
        let result = (|| -> Result<(), ContextHostError> {
            let paid = resources
                .admission
                .capacity
                .resources()
                .map_err(ContextHostError::Runtime)?;
            // Binding consumed this same admission's actual Work header.
            // The remaining object inventory is storage, not a Context quota.
            if need
                .quota
                .iter()
                .any(|(dimension, count)| paid.quota.get(dimension) < count)
                || paid
                    .owned_slots
                    .checked_add(
                        novarocks_control_runtime_core::owned_storage::OwnedSlotNeed::work_only(),
                    )
                    .ok()
                    != Some(need.owned_slots)
            {
                return Err(ContextHostError::BootstrapUnfunded);
            }
            let (scope, root) = ledger
                .open_child_scope_with_root(
                    &resources.admission.work,
                    generation,
                    LOCAL_ROLES[LIFETIME],
                    ReservationResources::work_only(),
                )
                .map_err(ContextHostError::Runtime)?;
            resources.scope = Some(scope.clone());
            let (work, capacity) = root
                .bind(LOCAL_ROLES[LIFETIME])
                .map_err(ContextHostError::Runtime)?;
            resources.roles.push(ContextMetadataRole { work, capacity });
            for kind in LOCAL_ROLES.into_iter().skip(1) {
                let guard = scope
                    .reserve_next(
                        scope.gates().map_err(ContextHostError::Runtime)?.generation,
                        ReservationResources::work_only(),
                        kind,
                        WorkAdmission::Derived(resources.roles[LIFETIME].work.id()),
                    )
                    .map_err(ContextHostError::Runtime)?;
                let (work, capacity) = guard.bind(kind).map_err(ContextHostError::Runtime)?;
                resources.roles.push(ContextMetadataRole { work, capacity });
            }
            resources.roles[TIMER]
                .work
                .start()
                .map_err(ContextHostError::Runtime)?;
            let actor = ContextActor::new(
                binding.core,
                scope.clone(),
                binding.storage,
                resources.roles[TIMER].work.reference(),
                resources.roles[LIFETIME].work.reference(),
            );
            let registration = runtime
                .register_supervised_bound(
                    binding.actor,
                    scope,
                    WorkerActor::Context(Box::new(actor)),
                    actor_config,
                    &resources.roles[FAILURE].work,
                )
                .map_err(|(error, _pure_actor)| ContextHostError::Driver(error))?;
            resources.registration = Some(registration);
            resources.registered = true;
            resources
                .owner_control
                .admit(
                    runtime,
                    resources
                        .registration
                        .as_ref()
                        .expect("original Context registration"),
                    ticket.actor,
                    context,
                    &resources.admission.work,
                )
                .map_err(ContextHostError::Driver)?;
            resources
                .internal
                .admit(
                    runtime,
                    resources
                        .registration
                        .as_ref()
                        .expect("original Context registration"),
                    ticket.actor,
                    context,
                    &resources.roles[TIMER].work,
                )
                .map_err(ContextHostError::Driver)?;
            Ok(())
        })();
        if let Err(error) = result {
            return Err(ContextHostRefusal {
                error,
                resources: Some(Box::new(resources)),
            });
        }
        Ok(Self {
            identity: context,
            actor: binding.actor,
            control: ticket.actor,
            control_registration,
            control_runtime: runtime.clone(),
            ledger: ledger.clone(),
            request_scope: resources.admission.work.scope().clone(),
            process_source_parent,
            establishment,
            initial_generation: generation,
            request_horizon,
            resources,
            tasks: BTreeMap::new(),
            retired_task_reads: BTreeMap::new(),
            task_read_waiters: Default::default(),
            task_driver: task_driver::TaskDriverState::new(binding.max_live_tasks),
            root_issuance_sealed: false,
            max_live_tasks: binding.max_live_tasks,
            max_routes,
            dispatch: EffectDispatch::context(runtime.clone(), binding.actor, effect_limit),
            request_inputs: BTreeMap::new(),
            task_request_cuts: BTreeMap::new(),
        })
    }
    pub(crate) fn identity(&self) -> QueryContextRef {
        self.identity
    }
    pub(crate) fn actor(&self) -> ActorId {
        self.actor
    }
    pub(super) fn registration(&self) -> &ActorRegistration {
        self.resources
            .registration
            .as_ref()
            .expect("registered original Context")
    }
    pub(crate) fn request_scope(&self) -> &ScopeHandle {
        &self.request_scope
    }
    pub(crate) fn scope(&self) -> &ScopeHandle {
        self.resources
            .scope
            .as_ref()
            .expect("registered Context owns its original scope")
    }
    #[cfg(test)]
    pub(in crate::control_runtime) fn resources_for_test(&mut self) -> &mut ContextHostResources {
        &mut self.resources
    }
    pub(crate) fn admission(&self) -> ReservationId {
        self.resources.admission.capacity.id()
    }
    pub(super) fn register_ready(&mut self) -> Result<(), ContextReadyError> {
        if self.resources.notice.is_some() {
            return Err(ContextReadyError::Duplicate);
        }
        self.resources.notice = Some(ContextReadyNotice::for_registered(
            &self.control_runtime,
            self.identity,
            self.scope(),
            self.registration(),
        )?);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn establish_route(&self, request: InputId) -> Option<&Arc<EstablishRouteCustody>> {
        self.resources.routes.get(&request)
    }
    pub(super) fn ready_notice(&self) -> &ContextReadyNotice {
        self.resources
            .notice
            .as_ref()
            .expect("published original Context host notice")
    }
    /// Bind the fixed original Context bank before asking Root for its source
    /// loan or handing any factory/instance to an executor.
    pub(crate) fn admit_io(&mut self) -> Result<(), ContextHostError> {
        if self.resources.io_attempted {
            return Err(ContextHostError::Io(ContextIoError::AlreadyAdmitted));
        }
        // This elected Context gets one original fixed bank. Even a partial
        // refusal leaves ready headers with the common reaper; an empty host
        // Option is not authority to issue a second bank over those headers.
        self.resources.io_attempted = true;
        let scope = self.scope().clone();
        let generation = scope.gates().map_err(ContextHostError::Runtime)?.generation;
        // Only unused positions cross the Scope boundary. M/R/H are born in
        // this Context under its actual lifetime; no Work is reparented.
        let mut payer = self
            .resources
            .admission
            .capacity
            .split_next(
                &CONTEXT_IO_STORAGE
                    .resources()
                    .expect("fixed original Context IO inventory"),
            )
            .map_err(ContextHostError::Runtime)?;
        if let Err(error) = payer.transfer(&self.resources.roles[LIFETIME].work) {
            payer.finish().expect("unexposed original IO parcel");
            return Err(ContextHostError::Runtime(error));
        }
        let result = ContextIoInventory::admit(
            &self.control_runtime,
            self.actor,
            &scope,
            &mut payer,
            &self.resources.roles[LIFETIME].work,
            generation,
        );
        // An incomplete pre-capture admission withdrew its actual roles. Only
        // never-bound positions remain in this parcel; it owns no Work header.
        payer.finish().map_err(ContextHostError::Runtime)?;
        self.resources.io = Some(result.map_err(ContextHostError::Io)?);
        Ok(())
    }
    pub(crate) fn prepare_host_jobs(&mut self) -> Result<(), ContextHostError> {
        if self.resources.host_jobs.is_some() {
            return Err(ContextHostError::Io(ContextIoError::AlreadyAdmitted));
        }
        let notice = self
            .resources
            .notice
            .as_ref()
            .ok_or(ContextHostError::HostJob(ContextHostJobError::WrongPhase))?
            .clone();
        if self.resources.io.is_none() {
            self.admit_io()?;
        }
        self.admit_lifecycle()?;
        let parts = self
            .resources
            .io
            .take()
            .expect("original admitted Context IO bank")
            .into_parts();
        self.resources.host_jobs = Some(ContextHostJobs::from_admitted(
            ContextHostJobBinding {
                runtime: self.control_runtime.clone(),
                ledger: self.ledger.clone(),
                actor: self.actor,
                control: self.control,
                context: self.identity,
                generation: self.initial_generation,
                establishment: self.establishment,
                admission: self.resources.admission.capacity.id(),
                admission_work: self.resources.admission.work.reference(),
                notice,
            },
            parts,
        ));
        Ok(())
    }
    fn admit_lifecycle(&mut self) -> Result<(), ContextHostError> {
        if self.resources.lifecycle.is_some() {
            return Err(ContextHostError::Io(ContextIoError::AlreadyAdmitted));
        }
        let mut owner = ContextLifecycle::new(
            self.control_runtime.clone(),
            self.control_registration.clone(),
            self.registration().clone(),
            self.identity,
            self.scope().clone(),
            &self.resources.admission.work,
            self.request_horizon,
        );
        let result = owner.initialize(&self.resources.admission.work);
        // A partial original Context callback stays with its process payer.
        self.resources.lifecycle = Some(owner);
        result.map_err(ContextHostError::HostJob)
    }

    pub(crate) fn host_jobs(&self) -> Option<&ContextHostJobs> {
        self.resources.host_jobs.as_ref()
    }
    /// Transfer the actual Core release intent to R's original prepaid owner.
    /// Waiting for M's instance publication stays with that owner, never with
    /// this source effect/input or a physical cleanup worker.
    pub(crate) fn handoff_host_release(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (ContextHostJobError, EffectTicket<WorkerAction>)> {
        let Some(jobs) = &self.resources.host_jobs else {
            return Err((ContextHostJobError::WrongPhase, ticket));
        };
        if let Err(error) = jobs.request_release(&ticket) {
            return Err((error, ticket));
        }
        self.complete_host_effect(ticket)
            .map_err(|refusal| (ContextHostJobError::Driver(refusal.error), refusal.ticket))?;
        Ok(())
    }
    pub(crate) fn retire_lifecycle(&mut self) -> Result<bool, ContextHostJobError> {
        self.resources
            .lifecycle
            .as_mut()
            .ok_or(ContextHostJobError::WrongPhase)?
            .retire_callback()
    }
    pub(crate) fn handoff_establishment_decision(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (ContextHostJobError, EffectTicket<WorkerAction>)> {
        let Some(jobs) = &self.resources.host_jobs else {
            return Err((ContextHostJobError::WrongPhase, ticket));
        };
        if let Err(e) = jobs.forward_establishment(&ticket) {
            return Err((e, ticket));
        }
        let reference = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        self.complete_host_effect(ticket)
            .map_err(|r| (ContextHostJobError::Driver(r.error), r.ticket))?;
        if self.dispatch.completion_observed(reference).is_err() {
            std::process::abort();
        }
        Ok(())
    }
    pub(crate) fn register_establish_route(
        &mut self,
        route: Arc<EstablishRouteCustody>,
    ) -> Result<(), EstablishRouteError> {
        if !route.target_matches(
            &self.control_runtime,
            self.registration(),
            self.identity,
            self.control,
        ) {
            return Err(EstablishRouteError::WrongOwner);
        }
        // A captured address is not a lifecycle pin. A failed or sealed actor
        // cannot accept a new leg. Root keeps this original R for its existing
        // failure/closure decision; unavailable does not manufacture Gone.
        match self.control_runtime.snapshot(self.actor) {
            Ok(snapshot)
                if snapshot.failed
                    || snapshot.sealed
                    || self.resources.control_admission_closed =>
            {
                return Err(EstablishRouteError::TargetRemoved);
            }
            Ok(_) => {}
            Err(DriverError::UnknownActor | DriverError::EffectTicketOriginMismatch) => {
                return Err(EstablishRouteError::TargetRemoved);
            }
            Err(error) => return Err(EstablishRouteError::Driver(error)),
        }
        if self.resources.routes.contains_key(&route.request().token) {
            return Err(EstablishRouteError::WrongPhase);
        }
        if self.resources.routes.len() >= self.max_routes {
            return Err(EstablishRouteError::Capacity);
        }
        let request = route.request();
        // The jobs retain their original execution identity after actual
        // Scope removal. Validate against that same lifecycle identity; the
        // new replay leg is already funded by its original process/R payer.
        let original_scope = self
            .resources
            .scope
            .as_ref()
            .or_else(|| {
                self.resources
                    .lifecycle
                    .as_ref()
                    .map(ContextLifecycle::original_execution_scope)
            })
            .ok_or(EstablishRouteError::WrongPhase)?;
        // Store this alias before the Context's first Work/report/input.
        self.resources.routes.insert(request.token, route.clone());
        route.bind_host_jobs(
            self.resources.host_jobs.clone(),
            original_scope,
            self.ready_notice().clone(),
        )?;
        let initialized = route.initialize_context();
        // Cancellation can precede insertion; retain an actual cleanup wake.
        self.notify_host_progress();
        initialized
    }
    pub(crate) fn handoff_establish_probe(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (EstablishRouteError, EffectTicket<WorkerAction>)> {
        let WorkerAction::Context(
            novarocks_worker_protocol::context::ContextEffect::EstablishProbed { request, .. },
        ) = &ticket.effect
        else {
            return Err((EstablishRouteError::WrongEffect, ticket));
        };
        let Some(route) = self.resources.routes.get(&request.token).cloned() else {
            return Err((EstablishRouteError::WrongOwner, ticket));
        };
        if let Err(error) = route.accept_probe(&ticket) {
            return Err((error, ticket));
        }
        let effect = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        self.complete_host_effect(ticket)
            .map_err(|refusal| (EstablishRouteError::Driver(refusal.error), refusal.ticket))?;
        // A completed ticket in this actor's original metadata lane cannot
        // lose its routing index. The original return leg remains retained.
        if self.dispatch.completion_observed(effect).is_err() {
            std::process::abort();
        }
        route.notify_progress();
        Ok(())
    }
    pub(crate) fn advance_establish_route(
        &mut self,
        request: InputId,
    ) -> Result<bool, EstablishRouteError> {
        let route = self
            .resources
            .routes
            .get(&request)
            .ok_or(EstablishRouteError::WrongOwner)?;
        route.advance_replay()?;
        if !route.try_retire_context()? {
            return Ok(false);
        }
        self.resources.routes.remove(&request);
        Ok(true)
    }

    /// The original Root decision authorizes the admission's own retirement.
    /// Actual callback/batch tails still exit before its header and storage.
    pub(crate) fn authorize_parent_retirement(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ContextHostError> {
        self.control_runtime
            .validate_effect_ticket(ticket)
            .map_err(ContextHostError::Driver)?;
        if ticket.actor != self.control
            || !matches!(&ticket.effect, WorkerAction::Control {
                effect: ControlEffect::RetireReservation(id), ..
            } if *id == self.admission())
        {
            return Err(ContextHostError::WrongEffect);
        }
        self.resources.owner_control.authorize_parent_retirement();
        self.resources.teardown.parent_retire_authorized = true;
        // The Core-consumption receipt may already have woken and parked the
        // Context before this separate Root effect is handed over.
        self.notify_host_progress();
        Ok(())
    }
    pub(crate) fn adopt_task(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
        binding: &TaskHostBinding,
    ) -> Result<(), ContextHostError> {
        if !self.control_runtime.same_runtime(runtime) {
            return Err(ContextHostError::WrongRuntime);
        }
        let task = match &ticket.effect {
            WorkerAction::Context(
                novarocks_worker_protocol::context::ContextEffect::LoadInput { task, .. },
            ) if ticket.actor == self.actor && binding.context == self.identity => *task,
            _ => return Err(ContextHostError::WrongEffect),
        };
        if self.tasks.contains_key(&task) || self.retired_task_reads.contains_key(&task) {
            return Err(ContextHostError::TaskAlreadyOwned);
        }
        if self
            .tasks
            .len()
            .saturating_add(self.retired_task_reads.len())
            >= self.max_live_tasks
        {
            return Err(ContextHostError::TaskTableFull);
        }
        let source = binding.task_source;
        let mut host = match TaskHostRecord::adopt_loading(
            runtime,
            ticket,
            binding,
            &self.resources.roles[LIFETIME].work,
        ) {
            Ok(host) => host,
            Err(refusal) => {
                if let Some(unpublished) = refusal.unpublished {
                    assert!(
                        self.tasks
                            .insert(task, TaskHostOwner::Unpublished(unpublished))
                            .is_none()
                    );
                    // This row owns actual unused admissions even though no
                    // Task port exists to provide the usual first notice.
                    self.task_driver.queue_owned_task(task);
                    self.notify_host_progress();
                }
                return Err(ContextHostError::Task(refusal.error));
            }
        };
        // Retain the original elected Task even if its paid notification
        // position cannot transfer. No host port/job is published on refusal;
        // the actual failure owner still has this complete bound inventory.
        assert!(self.task_driver.sources.insert(source, task).is_none());
        let result = host.admit_open_summary(runtime);
        let owner = if result.is_ok() {
            TaskHostOwner::Ready(Box::new(host))
        } else {
            TaskHostOwner::Initializing(Box::new(host))
        };
        assert!(self.tasks.insert(task, owner).is_none());
        result.map_err(ContextHostError::Task)
    }
    /// Only the running original failure stage may reach this still-owned
    /// constructor partial. Normal request/loader admission stays Ready-only.
    pub(super) fn failed_task_for_cleanup(
        &mut self,
        stage: &failure::ScopePublicationsDrained<'_>,
        task: TaskIdentity,
    ) -> Result<&mut TaskHostRecord, ContextHostError> {
        stage
            .validate(&self.control_runtime, self.actor)
            .map_err(ContextHostError::Driver)?;
        let owner = match self.tasks.get_mut(&task) {
            Some(TaskHostOwner::Ready(owner)) => owner.as_mut(),
            Some(TaskHostOwner::Initializing(owner)) => {
                if !owner.loader_factory_not_entered() {
                    return Err(ContextHostError::TaskStillOwned);
                }
                owner.as_mut()
            }
            Some(TaskHostOwner::Unpublished(_)) => return Err(ContextHostError::TaskStillOwned),
            None => return Err(ContextHostError::TaskNotOwned),
        };
        stage
            .validate_scope(
                &self.control_runtime,
                self.actor,
                owner.resources_for_retirement().lifetime.scope(),
            )
            .map_err(ContextHostError::Driver)?;
        Ok(owner)
    }
    pub(crate) fn task(&mut self, task: TaskIdentity) -> Option<&mut TaskHostRecord> {
        match self.tasks.get_mut(&task)? {
            TaskHostOwner::Ready(host) => Some(host),
            TaskHostOwner::Initializing(_) | TaskHostOwner::Unpublished(_) => None,
        }
    }
    /// A Task remains in this Context's failure-publication inventory until
    /// its real producers, captures and consumers have all closed. Moving an
    /// active Task to an unindexed cleanup owner would lose Scope-wide duties.
    pub(crate) fn take_task(
        &mut self,
        task: TaskIdentity,
    ) -> Result<TaskHostOwner, ContextHostError> {
        self.take_task_with_failure_stage(task, false)
    }

    pub(super) fn take_failed_task(
        &mut self,
        task: TaskIdentity,
        stage: &failure::ScopePublicationsDrained<'_>,
    ) -> Result<TaskHostOwner, ContextHostError> {
        stage
            .validate(&self.control_runtime, self.actor)
            .map_err(ContextHostError::Driver)?;
        self.take_task_with_failure_stage(task, true)
    }

    fn take_task_with_failure_stage(
        &mut self,
        task: TaskIdentity,
        failed_stage: bool,
    ) -> Result<TaskHostOwner, ContextHostError> {
        if self
            .scope()
            .gates()
            .map_err(ContextHostError::Runtime)?
            .failed
            && !failed_stage
            && !self.failure_publications_drained()
        {
            return Err(ContextHostError::TaskStillOwned);
        }
        self.ensure_task_unrouted(task)?;
        if self.retired_task_reads.len() >= self.max_live_tasks
            && !self.retired_task_reads.contains_key(&task)
        {
            return Err(ContextHostError::TaskTableFull);
        }
        let sealed_read = match self.tasks.get(&task) {
            Some(TaskHostOwner::Ready(host) | TaskHostOwner::Initializing(host)) => host
                .resources_for_retirement()
                .seal_read_observation()
                .map_err(|_| ContextHostError::TaskStillOwned)?,
            _ => return Err(ContextHostError::TaskStillOwned),
        };
        if sealed_read.status.identity() != task {
            return Err(ContextHostError::WrongContext);
        }
        // The installed Root was already moved into this Context's original
        // slot. Only this real closed Task prefix permits direct RootEndAcknowledged.
        self.resources.root_reads.mark_task_retired(task);
        assert!(
            self.retired_task_reads
                .insert(task, sealed_read.clone())
                .is_none(),
            "one actual sealed read handoff per original Task"
        );
        // Only this actual seal transfer wakes the original waiting reads.
        // A terminal status and a closed hot request gate grant no such proof.
        self.task_read_waiters.sealed(task, sealed_read);
        self.notify_host_progress();
        Ok(self.remove_task_owner(task))
    }
    pub(crate) fn has_retired_task_read(&self, task: TaskIdentity) -> bool {
        self.retired_task_reads
            .get(&task)
            .is_some_and(|original| original.status.identity() == task)
    }
    fn remove_task_owner(&mut self, task: TaskIdentity) -> TaskHostOwner {
        let owner = self.tasks.remove(&task).expect("same original Task");
        if let TaskHostOwner::Ready(host) | TaskHostOwner::Initializing(host) = &owner {
            assert_eq!(
                self.task_driver.sources.remove(&host.source_address()),
                Some(task)
            );
        }
        owner
    }
    fn ensure_task_unrouted(&mut self, task: TaskIdentity) -> Result<(), ContextHostError> {
        if self
            .resources
            .failure
            .as_ref()
            .is_some_and(|failure| !failure.is_drained())
        {
            return Err(ContextHostError::TaskStillOwned);
        }
        if self
            .task_request_cuts
            .range((task, novarocks_worker_protocol::proposal_summary::CutId(0))..)
            .take_while(|((identity, _), _)| *identity == task)
            .any(|(_, work)| {
                self.resources
                    .requests
                    .get(work)
                    .expect("same original request cut index")
                    .task_route_attached()
            })
        {
            return Err(ContextHostError::TaskStillOwned);
        }
        match self.tasks.get_mut(&task) {
            Some(TaskHostOwner::Ready(host) | TaskHostOwner::Initializing(host)) => {
                if host.request_admission_open() || !host.causal().is_empty() {
                    return Err(ContextHostError::TaskStillOwned);
                }
            }
            Some(TaskHostOwner::Unpublished(_)) => {}
            None => return Err(ContextHostError::TaskNotOwned),
        }
        Ok(())
    }
    /// Component fixture teardown moves the actual still-paid originals only
    /// after it has stopped driving this Context. This grants no production
    /// removal, failed-owner stage, physical exit or resource refund.
    #[cfg(test)]
    pub(super) fn take_task_for_teardown(
        &mut self,
        task: TaskIdentity,
    ) -> Result<TaskHostOwner, ContextHostError> {
        self.ensure_task_unrouted(task)?;
        Ok(self.remove_task_owner(task))
    }
    pub(crate) fn into_resources(
        self,
    ) -> (ContextHostResources, BTreeMap<TaskIdentity, TaskHostOwner>) {
        (self.resources, self.tasks)
    }
}

pub(crate) mod requests;
pub(crate) use requests::{
    CONTEXT_REQUEST_STORAGE, ContextRequestError, ContextRequestHandle, TaskContextRequestHandle,
};

#[cfg(test)]
mod tests {
    mod host_wakes;
    mod io_bank;
    mod lifecycle_bank;
    mod owner_cells;
    #[path = "../requests/tests.rs"]
    mod requests;
    use super::*;
    use crate::control_admission::Dimension;
    use crate::control_runtime::actors::{
        ControlActor, ControlMessage, RequestMetadata, WorkerMessage,
    };
    use crate::control_runtime::service::WorkerService;
    use crate::control_runtime_policy::IngressRoleLimits;
    use novarocks_control_runtime::{
        actor::ActorConfig,
        driver::InputRole,
        publication::{PublishedReply, PublishedSnapshot},
    };
    use novarocks_control_runtime_core::capacity::ResourceVector;
    use novarocks_control_runtime_core::{
        effect::EffectState,
        identity::{Generation, InputId, Tick},
        mailbox::InputClass,
    };
    use novarocks_execution_contract::{
        AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
        ContentFingerprint, CredentialEpoch, CredentialLeaseId, LeaseValidFor, OperationOutcome,
        QueryContextState, TaskOperationId,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
        QueryId,
    };
    use novarocks_worker_protocol::{
        admission::AdmissionTicketConfig,
        context::{ContextConfig, RequestResult},
        control::{ControlConfig, ControlCore, ControlInput, EstablishSemanticFacts},
        creation::PayloadId,
        lease::{LeaseBounds, MonotonicInstant},
    };
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    fn actual_component_inventory(fixture: &Harness) -> [u64; 2] {
        let root = fixture.scope.snapshot().unwrap();
        let child = fixture
            .service
            .context(fixture.identity)
            .and_then(|address| {
                let host = address.lock()?;
                host.resources
                    .scope
                    .as_ref()
                    .map(|scope| scope.snapshot().unwrap())
            });
        [
            u64::try_from(root.work + child.map_or(0, |s| s.work)).unwrap(),
            u64::try_from(root.storage + child.map_or(0, |s| s.storage)).unwrap(),
        ]
    }
    fn actual_admission_usage(fixture: &Harness) -> [u64; 4] {
        [
            Dimension::Requests,
            Dimension::ActiveTasks,
            Dimension::PreparingTasks,
            Dimension::PreparingBytes,
        ]
        .map(|dimension| fixture.ledger.used(dimension.id()))
    }
    fn finish_unused_bundle(bundle: &mut ReservationBundle) {
        let quota = bundle.resources().unwrap().quota;
        bundle.release_quota(&quota).unwrap();
        bundle.finish().unwrap();
    }
    fn generation() -> Generation {
        Generation::new(1).unwrap()
    }
    fn control_actor() -> ActorId {
        ActorId::new(1).unwrap()
    }
    fn context_actor() -> ActorId {
        ActorId::new(2).unwrap()
    }
    fn operation(value: u8) -> TaskOperationId {
        let mut bytes = [value; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        TaskOperationId::try_from_bytes(bytes).unwrap()
    }
    fn exit(role: &mut ContextMetadataRole) {
        role.work.body_exited().unwrap();
        role.work.captures_exited().unwrap();
    }
    fn release(role: &mut ContextMetadataRole) {
        assert!(
            role.work.actual_exit_ready().unwrap(),
            "the original fixture parent still owns live work: scope={:?}, work={:?}, lifecycle={:?}, inventory={:?}",
            role.work.scope().id(),
            role.work.id(),
            role.work.scope().work_lifecycle(role.work.id()).unwrap(),
            role.work.scope().snapshot().unwrap(),
        );
        let quota = role.capacity.resources().unwrap().quota;
        role.capacity.release_quota(&quota).unwrap();
        role.capacity.finish().unwrap();
    }

    // The real ControlActor issues the original metadata receipt and Context
    // guard. This fixture owns the original roles; no EffectTicket or
    // protocol Core is fabricated by an adapter.
    struct Harness {
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        runtime: ActorRuntime<WorkerActor>,
        service: WorkerService,
        roles: Vec<ContextMetadataRole>,
        input: InputRole<WorkerActor>,
        route: PublishedSnapshot<QueryContextState>,
        replies: BTreeMap<TaskOperationId, PublishedReply<RequestResult>>,
        completed: Vec<(TaskOperationId, RequestResult)>,
        admission_snapshot:
            PublishedSnapshot<super::super::node_metrics::ControlAdmissionProjection>,
        context_owner: Option<ContextMetadataRole>,
        policy: ControlRuntimePolicy,
        storage: AdmissionStorage,
        identity: QueryContextRef,
        capability: AdmissionEpochCapability,
        now: Tick,
        model_establishment_decision:
            Option<novarocks_worker_protocol::context::ContextEstablishmentDecision>,
        capture_control: bool,
        retirement_identity:
            Option<novarocks_worker_protocol::retention::ContextRetirementIdentity>,
    }
    impl Harness {
        fn new(context_storage: StorageNeed) -> Self {
            Self::new_with_request_capacity(context_storage, 0)
        }
        fn new_with_request_capacity(context_storage: StorageNeed, requests: usize) -> Self {
            Self::new_with_context_identity(context_storage, requests, 0, None)
        }
        fn new_with_context_identity(
            context_storage: StorageNeed,
            requests: usize,
            task_positions: usize,
            identity: Option<QueryContextRef>,
        ) -> Self {
            let policy = ControlRuntimePolicy::try_new(IngressRoleLimits {
                ordinary_running: 1,
                ordinary_waiting: 0,
                control_running: 1,
                control_waiting: 0,
                max_batch_items: 1,
                root_read_holders: 1,
            })
            .unwrap();
            // Control owns its three actual roles, a timer and one sequential
            // return/request position. Only the real Context pays separately.
            let limits = ResourceVector::new([
                (Dimension::Requests.id(), u64::try_from(requests).unwrap()),
                (
                    Dimension::ActiveTasks.id(),
                    u64::try_from(task_positions.max(1)).unwrap(),
                ),
                (Dimension::PreparingTasks.id(), 1),
                (Dimension::PreparingBytes.id(), 1024),
            ])
            .unwrap();
            let ledger = RuntimeLedger::new(limits);
            let scope = ledger.open_scope_next(None, generation()).unwrap();
            let mut roles = Vec::new();
            for (index, kind) in [WorkKind::Cleanup, WorkKind::Step, WorkKind::Step]
                .into_iter()
                .enumerate()
            {
                let mut need = StorageNeed {
                    work: 1,
                    ..StorageNeed::default()
                };
                if index != 0 {
                    need.inputs = 1;
                    need.batches = 1;
                    need.effects = ControlActor::MAX_STEP_ACTIONS;
                }
                if index == 2 {
                    need.replies = 2;
                }
                let (mut work, capacity) = scope
                    .reserve_next(
                        generation(),
                        need.resources().unwrap(),
                        kind,
                        WorkAdmission::External,
                    )
                    .unwrap()
                    .bind(kind)
                    .unwrap();
                if kind == WorkKind::Step {
                    work.start().unwrap();
                }
                roles.push(ContextMetadataRole { work, capacity });
            }
            let identity = identity.unwrap_or_else(|| {
                QueryContextRef::new(
                    QueryExecutionId::new(QueryId::new(8701, 8702), AttemptId::new(1).unwrap())
                        .unwrap(),
                    FrontendProcessId::new_v7(),
                    BackendProcessId::new_v7(),
                )
            });
            let storage = AdmissionStorage {
                loading: policy.loading.resources().unwrap(),
                accepted: policy.accepted.resources().unwrap(),
                context: context_storage.resources().unwrap(),
            };
            let core = ControlCore::new(
                ControlConfig {
                    max_pending_requests: requests + 4,
                    step_budget: ControlCore::MAX_STEP_WORK,
                },
                AdmissionTicketConfig::new(Duration::from_secs(5), Duration::from_secs(10))
                    .unwrap(),
                [7; 8],
                MonotonicInstant::ORIGIN,
            )
            .unwrap();
            let capability = core.admission().capability();
            let actor = ControlActor::new(
                core,
                scope.clone(),
                storage.clone(),
                roles[1].work.reference(),
                roles[0].work.reference(),
            );
            let runtime = ActorRuntime::new();
            runtime
                .register_supervised_next(
                    control_actor(),
                    scope.clone(),
                    WorkerActor::Control(Box::new(actor)),
                    ActorConfig {
                        mailbox: [1, 1, 1, 1],
                        max_batches: 4,
                        max_effects_per_step:
                            crate::control_runtime::actors::ControlActor::MAX_STEP_ACTIONS,
                        max_dependencies_per_effect:
                            crate::control_runtime::actors::ControlActor::MAX_STEP_ACTIONS,
                        turn_budget: crate::control_runtime::actors::ControlActor::TURN_BUDGET,
                        max_step_work: crate::control_runtime::actors::ControlActor::MAX_STEP_WORK,
                    },
                    &roles[0].work,
                )
                .unwrap_or_else(|(error, _)| {
                    panic!("actual Control actor registration: {error:?}")
                });
            let input = runtime
                .reserve_admitted_input(
                    control_actor(),
                    ActorId::new(90).unwrap(),
                    InputClass::Lifecycle,
                    &roles[2].work,
                )
                .unwrap()
                .into_role()
                .unwrap();
            let service = WorkerService::from_admitted_control(
                runtime.clone(),
                ledger.clone(),
                control_actor(),
                &roles[0].work,
            );
            Self {
                ledger,
                scope: scope.clone(),
                service,
                runtime,
                roles,
                input,
                route: PublishedSnapshot::new(&scope, Arc::new(QueryContextState::Absent)),
                replies: BTreeMap::new(),
                completed: Vec::new(),
                admission_snapshot: PublishedSnapshot::new(
                    &scope,
                    Arc::new(super::super::node_metrics::ControlAdmissionProjection {
                        epoch: capability,
                        logical_live_grants: 0,
                        admission_retention: Default::default(),
                        control_retention: Default::default(),
                    }),
                ),
                context_owner: None,
                policy,
                storage,
                identity,
                capability,
                now: Tick(1),
                model_establishment_decision: None,
                capture_control: false,
                retirement_identity: None,
            }
        }
        fn binding(
            &self,
            actor: ActorId,
            materialize: &EffectTicket<WorkerAction>,
        ) -> ContextHostBinding {
            let establishment = match &materialize.effect {
                WorkerAction::Control {
                    effect: ControlEffect::Materialize { establishment, .. },
                    ..
                } => *establishment,
                _ => panic!("Context binding requires its actual Materialize facts"),
            };
            self.binding_with_establishment(actor, establishment)
        }
        fn binding_with_establishment(
            &self,
            actor: ActorId,
            establishment: novarocks_worker_protocol::context::ContextEstablishment,
        ) -> ContextHostBinding {
            ContextHostBinding {
                actor,
                core: ContextCore::new(
                    self.identity,
                    generation(),
                    establishment,
                    ContextConfig {
                        max_tasks: 1,
                        max_creation_waiters: 1,
                        max_pending_requests: 4,
                        max_preparing: 1,
                        max_task_bytes: 1024,
                        step_budget: ContextCore::MAX_STEP_WORK,
                        lease_bounds: LeaseBounds::DEFAULT,
                        metric_publish_interval: Duration::from_millis(100),
                        request_horizon: crate::RequestHorizon::DEFAULT,
                    },
                )
                .unwrap(),
                policy: self.policy,
                storage: self.storage.clone(),
                max_live_tasks: 1,
                max_live_roots: 1,
            }
        }
        fn command(
            &mut self,
            command: ControlInput,
            request: Option<TaskOperationId>,
        ) -> Option<EffectTicket<WorkerAction>> {
            if let Some(request) = request {
                assert!(
                    self.replies
                        .insert(request, PublishedReply::new(&self.scope))
                        .is_none()
                );
            }
            let input = self
                .input
                .fill(WorkerMessage::Control(ControlMessage::Command {
                    work: self.roles[2].work.reference(),
                    request: request.map(|operation| RequestMetadata {
                        operation,
                        deadline: MonotonicInstant::from_origin(Duration::from_secs(100)),
                    }),
                    command,
                }))
                .unwrap();
            assert!(self.runtime.poll_one(self.now).unwrap());
            let materialize = self.drain(input);
            if materialize.is_none() {
                self.runtime.retire_input(control_actor(), input).unwrap();
            }
            materialize
        }
        fn drain(&mut self, input: InputId) -> Option<EffectTicket<WorkerAction>> {
            let mut materialize = None;
            loop {
                let inventory = self
                    .runtime
                    .effect_inventory(control_actor(), input, None, ControlCore::MAX_STEP_WORK)
                    .unwrap();
                let states: BTreeMap<_, _> = inventory
                    .iter()
                    .map(|(descriptor, state)| (descriptor.id, *state))
                    .collect();
                for (descriptor, state) in inventory {
                    if state == EffectState::Pending {
                        for predecessor in descriptor.dependencies {
                            if states[&predecessor] == EffectState::Completed {
                                self.runtime
                                    .acknowledge_dependency(
                                        control_actor(),
                                        input,
                                        descriptor.id,
                                        predecessor,
                                    )
                                    .unwrap();
                            }
                        }
                    }
                }
                let ready = self
                    .runtime
                    .ready_effects(control_actor(), input, ControlCore::MAX_STEP_WORK)
                    .unwrap();
                if ready.is_empty() {
                    break;
                }
                for action in ready {
                    let ticket = self
                        .runtime
                        .take_effect(control_actor(), input, action)
                        .unwrap();
                    match &ticket.effect {
                        WorkerAction::Control {
                            effect: ControlEffect::Materialize { .. },
                            ..
                        } => {
                            assert!(materialize.replace(ticket).is_none());
                            continue;
                        }
                        WorkerAction::AdmissionEpoch(projection) => {
                            let previous = self
                                .runtime
                                .commit_snapshot(
                                    &ticket,
                                    &self.admission_snapshot,
                                    Arc::new(*projection),
                                )
                                .unwrap();
                            // The same Root advances its published capability
                            // after recording this decision. Follow that actual
                            // commit; the bootstrap capability is not timeless.
                            assert_eq!(previous.epoch, self.capability);
                            self.capability = projection.epoch;
                        }
                        WorkerAction::Control {
                            effect:
                                ControlEffect::AdmissionReply {
                                    result: Ok(receipt),
                                    ..
                                },
                            admitted: None,
                        } => {
                            assert_eq!(receipt.context(), self.identity);
                            assert_eq!(self.admission_snapshot.read().logical_live_grants, 1);
                            assert_eq!(self.admission_snapshot.read().epoch, self.capability);
                        }
                        WorkerAction::Control {
                            effect: ControlEffect::Route { context, state, .. },
                            ..
                        } => {
                            assert_eq!(*context, self.identity);
                            if let Some(decision) = &self.model_establishment_decision {
                                if *state == QueryContextState::Active {
                                    assert_eq!(decision.state, QueryContextState::Active);
                                    assert_eq!(decision.outcome, Ok(OperationOutcome::Accepted));
                                    let original = decision.acknowledgement.as_ref().expect(
                                        "Active follows the actual Context acknowledgement",
                                    );
                                    assert_eq!(original.outcome, OperationOutcome::Accepted);
                                    assert_eq!(original.receipt.context(), self.identity);
                                    assert_eq!(original.receipt.state(), QueryContextState::Active);
                                    assert!(original.receipt.lease().is_some());
                                }
                            } else {
                                assert_ne!(
                                    *state,
                                    QueryContextState::Active,
                                    "metadata bootstrap is not materialization"
                                );
                            }
                            drop(
                                self.runtime
                                    .commit_snapshot(&ticket, &self.route, Arc::new(*state))
                                    .unwrap(),
                            );
                        }
                        WorkerAction::Control {
                            effect: ControlEffect::RemoveHost { .. },
                            ..
                        } => {
                            if self.capture_control {
                                return Some(ticket);
                            }
                            if let Some(host) = self.service.take_removing_context(&ticket).unwrap()
                            {
                                let (resources, tasks) = host.into_resources();
                                assert!(tasks.is_empty());
                                assert!(resources.scope.is_none());
                                assert!(!resources.registered);
                                assert!(
                                    resources
                                        .roles
                                        .iter()
                                        .all(|role| role.capacity.is_released())
                                );
                                assert!(self.context_owner.replace(resources.admission).is_none());
                            }
                        }
                        WorkerAction::Control {
                            effect: ControlEffect::RetireReservation(id),
                            ..
                        } => {
                            if self.capture_control {
                                return Some(ticket);
                            }
                            let address = self
                                .service
                                .context(self.identity)
                                .expect("same original Context");
                            let mut host = address.lock().unwrap();
                            assert_eq!(host.admission(), *id);
                            host.authorize_parent_retirement(&ticket).unwrap();
                        }
                        WorkerAction::Control {
                            effect: ControlEffect::WakeInternal,
                            ..
                        } => {}
                        WorkerAction::Control {
                            effect: ControlEffect::Reply { request, outcome },
                            ..
                        } => {
                            assert_eq!(*request, operation(2));
                            assert_eq!(
                                Ok(*outcome),
                                self.model_establishment_decision
                                    .as_ref()
                                    .expect("reply follows the actual same Core decision")
                                    .outcome
                            );
                        }
                        WorkerAction::ControlReply(settlement) => {
                            if settlement.request.operation == operation(2)
                                && settlement.result
                                    == RequestResult::Outcome(OperationOutcome::Accepted)
                            {
                                let original = self
                                    .model_establishment_decision
                                    .as_ref()
                                    .expect("Accepted follows the original Context decision")
                                    .acknowledgement
                                    .as_ref()
                                    .expect("Accepted retains the original acknowledgement");
                                let novarocks_worker_protocol::control::ControlReplyBody::Context(
                                    actual,
                                ) = &settlement.body
                                else {
                                    panic!(
                                        "Accepted Establish returns its original Context acknowledgement"
                                    );
                                };
                                assert_eq!(actual, original);
                                assert!(Arc::ptr_eq(&actual.receipt, &original.receipt));
                                assert_eq!(*self.route.read(), QueryContextState::Active);
                            }
                            let reply = self
                                .replies
                                .remove(&settlement.request.operation)
                                .expect("original bounded reply outbox");
                            self.runtime
                                .commit_reply(&ticket, &reply, Arc::new(settlement.result))
                                .unwrap();
                            assert_eq!(*reply.take_once().unwrap(), settlement.result);
                            self.completed
                                .push((settlement.request.operation, settlement.result));
                        }
                        other => panic!("unexpected bootstrap protocol effect: {other:?}"),
                    }
                    self.runtime.effect_completed(ticket).unwrap();
                }
            }
            materialize
        }
        fn materialize(&mut self) -> EffectTicket<WorkerAction> {
            let ticket = AdmissionTicketId::try_from_bytes([7; 16]).unwrap();
            assert!(
                self.command(
                    ControlInput::Acquire {
                        request: AcquireQueryContextAdmissionTicket::new(
                            operation(1),
                            self.identity,
                            LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
                            NativeCompatibilityId::new([3; 32]),
                            self.capability,
                        ),
                        ticket,
                    },
                    Some(operation(1)),
                )
                .is_none()
            );

            self.command(
                ControlInput::Establish {
                    request: operation(2),
                    context: self.identity,
                    ticket,
                    payload: PayloadId(1),
                    semantics: EstablishSemanticFacts {
                        catalog: ContentFingerprint::from_bytes([1; 16]),
                        shared_filter: ContentFingerprint::from_bytes([2; 16]),
                        query_options: ContentFingerprint::from_bytes([3; 16]),
                        credential_lease: CredentialLeaseId::new(1),
                        credential_epoch: CredentialEpoch::new(1).unwrap(),
                        initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30))
                            .unwrap(),
                    },
                    generation: generation(),
                },
                Some(operation(2)),
            )
            .expect("real Control Materialize effect carries the original Context guard")
        }
        fn complete_materialize_handoff(&mut self, ticket: EffectTicket<WorkerAction>) {
            let input = ticket.input;
            self.runtime.effect_completed(ticket).unwrap();
            assert!(self.drain(input).is_none());
            self.runtime.retire_input(control_actor(), input).unwrap();
        }
        fn dispose_inventory(
            runtime: &ActorRuntime<WorkerActor>,
            ledger: &RuntimeLedger,
            parent: &ScopeHandle,
            resources: &mut ContextHostResources,
            actor: ActorId,
        ) {
            assert!(resources.owner_control.close_for_teardown().unwrap());
            assert!(resources.internal.close_for_teardown().unwrap());
            Self::release_drained_effect_claims(runtime);
            if resources.registered {
                runtime.close_actor_admission(actor).unwrap();
                runtime.close_timer_source(actor).unwrap();
                runtime.seal_actor(actor).unwrap();
            }
            if let Some(scope) = resources.scope.take() {
                scope.close_external().unwrap();
                for role in resources.roles.iter_mut().rev() {
                    exit(role);
                }
                ledger.reap_scope_ready(scope.id(), 32).unwrap();
                for role in &mut resources.roles {
                    release(role);
                }
                if resources.registered {
                    drop(runtime.remove_actor(actor).unwrap());
                    resources.registered = false;
                }
                assert!(scope.snapshot().unwrap().retirement_ready);
                ledger.remove_scope(scope.id()).unwrap();
                ledger.reap_scope_ready(parent.id(), 32).unwrap();
            }
        }
        fn release_drained_effect_claims(runtime: &ActorRuntime<WorkerActor>) {
            while let Some(original_host_turn) = runtime.claim_host_actor() {
                let actor = original_host_turn.actor();
                assert!(runtime.pending_batches(actor, None, 1).unwrap().is_empty());
                original_host_turn.release(false).unwrap();
            }
        }
        fn close_model_core(&mut self) {
            let Some(address) = self.service.context(self.identity) else {
                return;
            };
            let host = address.lock().unwrap();
            if host.resources.scope.is_none()
                || host.resources.execution_close_epoch.is_some()
                || self.runtime.snapshot(host.actor()).unwrap().failed
                || host
                    .resources
                    .scope
                    .as_ref()
                    .is_some_and(|scope| scope.gates().unwrap().failed)
            {
                return;
            }
            drop(host);
            // The external Root waiter may expire independently. Only the
            // actual Context decision determines whether materialization ended.
            let fact = if self.model_establishment_decision.is_some() {
                novarocks_worker_protocol::context::ContextInput::OwnerFenced
            } else {
                novarocks_worker_protocol::context::ContextInput::MaterializationFinished {
                    result: Err(
                        novarocks_worker_protocol::creation::AdmissionRefusal::Verdict(
                            OperationOutcome::OwnerFailed,
                        ),
                    ),
                }
            };
            let input = requests::model_fact(self, fact, self.now.0);
            requests::complete_model_effects(self, input);
            self.runtime.retire_input(context_actor(), input).unwrap();
            for _ in 0..8 {
                let input = requests::model_fact(
                    self,
                    novarocks_worker_protocol::context::ContextInput::NextInternal,
                    self.now.0,
                );
                requests::complete_model_effects(self, input);
                self.runtime.retire_input(context_actor(), input).unwrap();
            }
        }
        fn dispose_service_bootstrap(&mut self, actor: ActorId) {
            self.close_model_core();
            let host_address = self.service.context(self.identity).unwrap();
            let mut host = host_address.lock().unwrap();
            if let Ok(proof) = self.runtime.failure_proof_for(host.registration()) {
                self.retirement_identity = Some(
                    novarocks_worker_protocol::retention::ContextRetirementIdentity::OwnerFailure {
                        actor: proof.registration().actor(),
                        initial_epoch: proof.initial_epoch(),
                        failed_epoch: proof.failed_epoch(),
                    },
                );
            } else if let Some(close_epoch) = host.resources.execution_close_epoch {
                self.retirement_identity = Some(
                    novarocks_worker_protocol::retention::ContextRetirementIdentity::Protocol {
                        close_epoch,
                    },
                );
            }
            Self::dispose_inventory(
                &self.runtime,
                &self.ledger,
                &self.scope,
                &mut host.resources,
                actor,
            );
        }
        fn dispose_bootstrap(&mut self, mut resources: ContextHostResources, actor: ActorId) {
            Self::dispose_inventory(
                &self.runtime,
                &self.ledger,
                &self.scope,
                &mut resources,
                actor,
            );
            assert!(self.context_owner.replace(resources.admission).is_none());
        }
        fn one_control_effect(&mut self, command: ControlInput) -> EffectTicket<WorkerAction> {
            self.capture_control = true;
            let result = self.command(command, None);
            self.capture_control = false;
            result.expect("actual original Root retirement/removal effect")
        }
        fn complete_control_effect(&mut self, ticket: EffectTicket<WorkerAction>) {
            let input = ticket.input;
            self.runtime.effect_completed(ticket).unwrap();
            // Capturing one original removal/retirement effect does not retire
            // the same batch's remaining admission snapshot or continuation.
            assert!(self.drain(input).is_none());
            self.runtime.retire_input(control_actor(), input).unwrap();
        }
        fn actual_model_refusal_removal(&mut self) -> EffectTicket<WorkerAction> {
            self.dispose_service_bootstrap(context_actor());
            let retirement = self
                .retirement_identity
                .expect("actual Core close or Actor failure proof");
            assert!(
                self.command(
                    ControlInput::ExecutionRetired {
                        context: self.identity,
                        retirement
                    },
                    None
                )
                .is_none()
            );
            self.capture_control = true;
            let mut removal = self.command(ControlInput::ProcessDrain, None);
            if removal.is_none() {
                removal = self.command(ControlInput::ProcessIngressClosed, None);
            }
            for _ in 0..16 {
                if removal.is_some() {
                    break;
                }
                removal = self.command(ControlInput::NextInternal, None);
            }
            self.capture_control = false;
            let removal =
                removal.expect("actual process drain removes only the physically retired Context");
            assert!(
                matches!(&removal.effect, WorkerAction::Control { effect: ControlEffect::RemoveHost { context }, .. } if *context == self.identity)
            );
            removal
        }
        fn actual_model_refusal_retirement(
            &mut self,
        ) -> (ContextHostRecord, EffectTicket<WorkerAction>) {
            self.dispose_service_bootstrap(context_actor());
            let retirement = self
                .retirement_identity
                .expect("actual Core close or Actor failure proof");
            let ticket = self.one_control_effect(ControlInput::ExecutionRetired {
                context: self.identity,
                retirement,
            });
            let address = self.service.context(self.identity).unwrap();
            let host = {
                let mut original = address.lock().unwrap();
                assert!(original.resources.scope.is_none());
                assert!(!original.resources.registered);
                original.take_for_parent()
            };
            self.service.directory().remove_original(&address);
            assert!(host.resources.scope.is_none());
            assert!(!host.resources.registered);
            assert!(
                matches!(&ticket.effect, WorkerAction::Control { effect: ControlEffect::RetireReservation(id), .. } if *id == host.admission())
            );
            (host, ticket)
        }
        fn finish_retired_context(
            mut self,
            mut host: ContextHostRecord,
            retirement: EffectTicket<WorkerAction>,
        ) {
            host.authorize_parent_retirement(&retirement).unwrap();
            self.complete_control_effect(retirement);
            let (resources, tasks) = host.into_resources();
            assert!(tasks.is_empty());
            assert!(resources.scope.is_none());
            assert!(!resources.registered);
            assert!(self.context_owner.replace(resources.admission).is_none());
            self.finish_removed();
        }
        fn finish_model_refusal(mut self) {
            if let Some(address) = self.service.context(self.identity) {
                let host = {
                    let mut original = address.lock().unwrap();
                    assert!(original.resources.scope.is_none());
                    assert!(!original.resources.registered);
                    original.take_for_parent()
                };
                assert!(host.resources.scope.is_none());
                assert!(!host.resources.registered);
                self.service.directory().remove_original(&address);
                let (resources, tasks) = host.into_resources();
                assert!(tasks.is_empty());
                assert!(self.context_owner.replace(resources.admission).is_none());
            }
            self.finish_removed();
        }
        fn finish_removed(mut self) {
            if !self.replies.is_empty() {
                self.now = Tick(101_000_000_000);
                assert!(self.command(ControlInput::Tick, None).is_none());
            }
            assert!(self.replies.is_empty());

            assert_eq!(
                usize::from(self.service.context(self.identity).is_some()),
                0
            );
            let mut admission = self.context_owner.take().unwrap();
            assert!(admission.capacity.resources().unwrap().quota.is_empty());
            assert!(!admission.work.retirement_observer().is_retired());
            exit(&mut admission);
            self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
            release(&mut admission);
            self.input.close_after_exit().unwrap();
            self.runtime.close_actor_admission(control_actor()).unwrap();
            Self::release_drained_effect_claims(&self.runtime);
            self.runtime.seal_actor(control_actor()).unwrap();
            self.scope.close_external().unwrap();
            for role in &mut self.roles {
                exit(role);
            }
            self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
            for role in &mut self.roles {
                release(role);
            }
            drop(self.runtime.remove_actor(control_actor()).unwrap());
            self.ledger.remove_scope(self.scope.id()).unwrap();
            assert!(self.runtime.is_empty());
            assert!(self.ledger.is_empty());
            assert!(self.replies.is_empty());
        }
    }

    #[test]
    fn actual_control_materialize_guard_pays_original_bootstrap_and_io_bank_once() {
        let mut fixture = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
        let ticket = fixture.materialize();
        let original = match &ticket.effect {
            WorkerAction::Control {
                effect: ControlEffect::Materialize { reservation, .. },
                ..
            } => *reservation,
            _ => unreachable!(),
        };
        let root_work_before_adoption = fixture.scope.snapshot().unwrap().work;
        let binding = fixture.binding(context_actor(), &ticket);
        assert_eq!(binding.core.state(), QueryContextState::Establishing);
        let actual_deadline = binding.core.next_deadline();
        let expected_deadline = match &ticket.effect {
            WorkerAction::Control {
                effect: ControlEffect::Materialize { establishment, .. },
                ..
            } => establishment
                .elected_at
                .saturating_add(Duration::from_secs(30)),
            _ => unreachable!(),
        };
        fixture.service.adopt_context(&ticket, binding).unwrap();
        let host_address = fixture.service.context(fixture.identity).unwrap();
        let host = host_address.lock().unwrap();
        assert_eq!(host.admission(), original);
        assert!(
            fixture
                .scope
                .work_lifecycle(host.resources.admission.work.id())
                .unwrap()
                .is_some(),
            "the same original bootstrap author remains owned while its child Scope exists"
        );
        assert_eq!(host.identity(), fixture.identity);
        assert_eq!(host.actor(), context_actor());
        assert_ne!(host.scope().id(), fixture.scope.id());
        assert_eq!(host.scope().snapshot().unwrap().work, 3);
        assert_eq!(
            fixture.scope.snapshot().unwrap().work,
            root_work_before_adoption + 1,
            "the child Scope adds exactly one real parent-link Work beside the original Materialize payment"
        );
        assert!(
            fixture.scope.snapshot().unwrap().work >= 4,
            "the original bootstrap parent remains physically owned"
        );
        assert_eq!(*fixture.route.read(), QueryContextState::Absent);
        assert!(matches!(
            fixture.runtime.take_step_reservation(&ticket, original),
            Err(DriverError::UnknownReservation)
        ));
        drop(host);
        fixture.complete_materialize_handoff(ticket);
        fixture.dispose_service_bootstrap(context_actor());
        fixture.finish_model_refusal();
        assert_eq!(
            actual_deadline,
            Some(expected_deadline),
            "the actual creator's election starts the initial lease"
        );
    }

    #[test]
    fn foreign_root_retirement_cannot_release_a_matching_numeric_context_permit() {
        let mut first = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
        let mut second = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
        for fixture in [&mut first, &mut second] {
            let materialize = fixture.materialize();
            let binding = fixture.binding(context_actor(), &materialize);
            fixture
                .service
                .adopt_context(&materialize, binding)
                .unwrap();
            fixture.complete_materialize_handoff(materialize);
        }
        let (first_host, first_retirement) = first.actual_model_refusal_retirement();
        let second_host_address = second.service.context(second.identity).unwrap();
        let mut second_host = second_host_address.lock().unwrap();
        assert_eq!(first_host.admission(), second_host.admission());
        assert_eq!(first_retirement.actor, second_host.control);
        let original = second_host
            .resources
            .admission
            .capacity
            .resources()
            .unwrap();
        let scope_work = second.scope.snapshot().unwrap().work;
        let result = second_host.authorize_parent_retirement(&first_retirement);
        let after = second_host
            .resources
            .admission
            .capacity
            .resources()
            .unwrap();
        let quota = usize::from(second.service.context(second.identity).is_some());
        let after_work = second.scope.snapshot().unwrap().work;
        // The original source ticket still retires its own actual holder. A
        // broken target rule has already released B's unit; B's own genuine
        // retirement is idempotent and still permits honest cleanup below.
        first.finish_retired_context(first_host, first_retirement);
        drop(second_host);
        let (second_host, second_retirement) = second.actual_model_refusal_retirement();
        second.finish_retired_context(second_host, second_retirement);
        assert!(matches!(
            result,
            Err(ContextHostError::Driver(
                DriverError::EffectTicketOriginMismatch
            ))
        ));
        assert_eq!(after, original);
        assert_eq!(quota, 1);
        assert_eq!(after_work, scope_work);
    }

    #[test]
    fn foreign_root_remove_host_cannot_move_the_original_service_inventory() {
        use crate::control_runtime::service::ServiceHostError;
        let mut first = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
        let mut second = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
        // Equal complete protocol coordinates in independent actual runtimes
        // still do not confer ownership of the other Service's original map.
        second.identity = first.identity;
        for fixture in [&mut first, &mut second] {
            let materialize = fixture.materialize();
            let binding = fixture.binding(context_actor(), &materialize);
            fixture
                .service
                .adopt_context(&materialize, binding)
                .unwrap();
            fixture.complete_materialize_handoff(materialize);
        }
        let removal = first.actual_model_refusal_removal();
        let original = {
            let host_address = second.service.context(second.identity).unwrap();
            let host = host_address.lock().unwrap();
            (host.admission(), host.scope().id(), host.tasks.len())
        };
        let fee = usize::from(second.service.context(second.identity).is_some());
        let outcome = second.service.take_removing_context(&removal);
        let retained = second.service.context(second.identity).map(|address| {
            let host = address.lock().unwrap();
            (host.admission(), host.scope().id(), host.tasks.len())
        });
        let fee_after = usize::from(second.service.context(second.identity).is_some());
        let (error, moved) = match outcome {
            Err(error) => (Some(error), None),
            Ok(owner) => (None, owner),
        };
        // The genuine source effect still moves its own original owner.
        let first_host = first
            .service
            .take_removing_context(&removal)
            .unwrap()
            .unwrap();
        first.complete_control_effect(removal);
        assert!(
            first
                .command(
                    ControlInput::HostRemoved {
                        context: first.identity
                    },
                    None
                )
                .is_none()
        );
        let (resources, tasks) = first_host.into_resources();
        assert!(tasks.is_empty() && resources.scope.is_none() && !resources.registered);
        assert!(first.context_owner.replace(resources.admission).is_none());
        first.finish_removed();
        if let Some(mut wrong_owner) = moved {
            // A broken gate has moved the ACTUAL B owner; keep it and perform
            // its real physical cleanup, never reconstruct or reinsert it.
            Harness::dispose_inventory(
                &second.runtime,
                &second.ledger,
                &second.scope,
                &mut wrong_owner.resources,
                context_actor(),
            );
            let (resources, tasks) = wrong_owner.into_resources();
            assert!(tasks.is_empty());
            assert!(second.context_owner.replace(resources.admission).is_none());
        } else {
            second.dispose_service_bootstrap(context_actor());
        }
        // B's own real RemoveHost is still observed; if the wrong rule already
        // moved it, the same actual payer above remains its cleanup owner.
        second.finish_model_refusal();
        assert!(matches!(
            error,
            Some(ServiceHostError::Driver(
                DriverError::EffectTicketOriginMismatch
            ))
        ));
        assert_eq!(retained, Some(original));
        assert_eq!(fee_after, fee);
    }

    #[test]
    fn admitted_context_bootstrap_attaches_after_ledger_close_without_reopening_business() {
        let mut fixture = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
        let ticket = fixture.materialize();
        fixture.ledger.close_admission();
        let binding = fixture.binding(context_actor(), &ticket);
        assert_eq!(binding.core.state(), QueryContextState::Establishing);
        fixture.service.adopt_context(&ticket, binding).unwrap();
        let host_address = fixture.service.context(fixture.identity).unwrap();
        let host = host_address.lock().unwrap();
        let scope = host.scope();
        assert!(!scope.gates().unwrap().external_open);
        assert!(matches!(
            scope.reserve_next(
                generation(),
                ReservationResources::work_only(),
                WorkKind::Business,
                WorkAdmission::External
            ),
            Err(RuntimeError::Scope(
                novarocks_control_runtime_core::scope::ScopeError::Closed
            ))
        ));
        let lifetime = host.resources.roles[LIFETIME].capacity.id();
        // External admission is closed, but a real pre-existing owner may
        // derive during drain. Storage is prepaid per holder, not a metadata
        // Context count/byte cap. This is no Task or Accepted permission.
        let before_child = scope.snapshot().unwrap();
        let (mut child, mut capacity) = scope
            .reserve_child_next(
                generation(),
                lifetime,
                fixture.policy.loading.resources().unwrap(),
                WorkKind::Business,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        assert_eq!(scope.snapshot().unwrap().work, before_child.work + 1);
        assert!(!scope.gates().unwrap().external_open);
        child.body_exited().unwrap();
        child.captures_exited().unwrap();
        fixture.ledger.reap_scope_ready(scope.id(), 32).unwrap();
        assert!(child.actual_exit_ready().unwrap());
        assert!(capacity.resources().unwrap().quota.is_empty());
        capacity.finish().unwrap();
        assert_eq!(scope.snapshot().unwrap(), before_child);
        scope.request_stop().unwrap();
        assert!(matches!(
            scope.reserve_child_next(
                generation(),
                lifetime,
                fixture.policy.loading.resources().unwrap(),
                WorkKind::Business
            ),
            Err(RuntimeError::Capacity(
                novarocks_control_runtime_core::capacity::CapacityError::Scope(
                    novarocks_control_runtime_core::scope::ScopeError::Stopped
                )
            ))
        ));
        assert_eq!(scope.snapshot().unwrap().work, 3);
        assert!(
            fixture.scope.snapshot().unwrap().work >= 4,
            "the original bootstrap parent remains physically owned"
        );
        drop(host);
        fixture.complete_materialize_handoff(ticket);
        fixture.dispose_service_bootstrap(context_actor());
        fixture.finish_model_refusal();
    }

    #[test]
    fn storage_policy_refusal_keeps_original_materialize_guard_for_corrected_binding() {
        for loading in [false, true] {
            let mut fixture = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
            let ticket = fixture.materialize();
            let guards = fixture
                .runtime
                .step_reservations(ticket.actor, ticket.input, None, ControlCore::MAX_STEP_WORK)
                .unwrap();
            assert_eq!(guards.len(), 1);
            assert_eq!(guards[0].1, ticket.descriptor.work.work);
            let work_before = fixture.scope.snapshot().unwrap().work;
            let mut binding = fixture.binding(context_actor(), &ticket);
            if loading {
                binding.storage.loading = ReservationResources::default();
            } else {
                binding.storage.accepted = ReservationResources::default();
            }
            let refusal = ContextHostRecord::adopt(
                &fixture.runtime,
                &fixture.ledger,
                &ticket,
                binding,
                fixture.roles[0].work.child_authority().unwrap(),
            )
            .err()
            .expect("deviating from the actual policy must refuse before ownership transfer");
            assert!(matches!(
                refusal.error,
                ContextHostError::InvalidStoragePolicy
            ));
            assert!(refusal.resources.is_none());
            assert_eq!(fixture.runtime.step_reservations(
                ticket.actor, ticket.input, None, ControlCore::MAX_STEP_WORK,
            ).unwrap(), guards);
            assert_eq!(fixture.scope.snapshot().unwrap().work, work_before);
            assert!(
                fixture.service.context(fixture.identity).is_none(),
                "a rejected direct adoption cannot publish a Service host"
            );

            // Reuse this exact original ticket/guard, with no second Core
            // decision, Context reservation or invented Materialized fact.
            let binding = fixture.binding(context_actor(), &ticket);
            fixture.service.adopt_context(&ticket, binding).unwrap();
            assert_eq!(
                fixture
                    .service
                    .context(fixture.identity)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .admission(),
                guards[0].0
            );
            assert!(fixture.runtime.step_reservations(
                ticket.actor, ticket.input, None, ControlCore::MAX_STEP_WORK,
            ).unwrap().is_empty());
            assert!(
                fixture.scope.snapshot().unwrap().work >= 4,
                "the original bootstrap parent remains physically owned"
            );
            fixture.complete_materialize_handoff(ticket);
            fixture.dispose_service_bootstrap(context_actor());
            fixture.finish_model_refusal();
        }
    }

    #[test]
    fn different_initial_establish_facts_cannot_take_the_original_context_bootstrap_guard() {
        for field in 0..10 {
            let mut fixture = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
            let ticket = fixture.materialize();
            let mut initial = match &ticket.effect {
                WorkerAction::Control {
                    effect: ControlEffect::Materialize { establishment, .. },
                    ..
                } => *establishment,
                _ => unreachable!(),
            };
            match field {
                0 => initial.operation = operation(8),
                1 => initial.ticket = AdmissionTicketId::try_from_bytes([8; 16]).unwrap(),
                2 => initial.payload = PayloadId(8),
                3 => initial.semantics.catalog = ContentFingerprint::from_bytes([8; 16]),
                4 => initial.semantics.shared_filter = ContentFingerprint::from_bytes([8; 16]),
                5 => initial.semantics.query_options = ContentFingerprint::from_bytes([8; 16]),
                6 => initial.semantics.credential_lease = CredentialLeaseId::new(8),
                7 => initial.semantics.credential_epoch = CredentialEpoch::new(8).unwrap(),
                8 => {
                    initial.semantics.initial_lease_valid_for =
                        LeaseValidFor::new(Duration::from_secs(31)).unwrap()
                }
                _ => initial.elected_at = MonotonicInstant::from_origin(Duration::from_secs(8)),
            }
            let before = fixture
                .runtime
                .step_reservations(ticket.actor, ticket.input, None, ControlCore::MAX_STEP_WORK)
                .unwrap();
            let work_before = fixture.scope.snapshot().unwrap().work;
            let binding = fixture.binding_with_establishment(context_actor(), initial);
            // Exact Context and runtime generation are insufficient: the
            // actual creator's original operation, ticket and facts must agree.
            assert_eq!(binding.core.identity(), fixture.identity);
            assert_eq!(binding.core.generation(), generation());
            let refusal = ContextHostRecord::adopt(
                &fixture.runtime,
                &fixture.ledger,
                &ticket,
                binding,
                fixture.roles[0].work.child_authority().unwrap(),
            )
            .err()
            .expect("a different original creation must not take the guard");
            let refused = matches!(refusal.error, ContextHostError::WrongEstablishment)
                && refusal.resources.is_none();
            let original_guard = fixture
                .runtime
                .step_reservations(ticket.actor, ticket.input, None, ControlCore::MAX_STEP_WORK)
                .unwrap()
                == before;
            let no_work = fixture.scope.snapshot().unwrap().work == work_before;
            let binding = fixture.binding(context_actor(), &ticket);
            fixture.service.adopt_context(&ticket, binding).unwrap();
            fixture.complete_materialize_handoff(ticket);
            fixture.dispose_service_bootstrap(context_actor());
            fixture.finish_model_refusal();
            assert!(refused, "initial fact {field}");
            assert!(original_guard);
            assert!(no_work);
        }
    }

    #[test]
    fn insufficient_fifth_work_returns_original_bound_context_payer_without_a_child() {
        let mut storage = CONTEXT_BOOTSTRAP_STORAGE;
        storage.work -= 1;
        let mut fixture = Harness::new(storage);
        let ticket = fixture.materialize();
        let refusal = ContextHostRecord::adopt(
            &fixture.runtime,
            &fixture.ledger,
            &ticket,
            fixture.binding(context_actor(), &ticket),
            fixture.roles[0].work.child_authority().unwrap(),
        )
        .err()
        .unwrap();
        assert!(matches!(refusal.error, ContextHostError::BootstrapUnfunded));
        let resources = *refusal
            .resources
            .expect("same bound original payer survives refusal");
        assert!(resources.scope.is_none());
        assert!(resources.roles.is_empty());
        assert!(!resources.registered);
        assert!(
            fixture.scope.snapshot().unwrap().work >= 4,
            "the original bootstrap parent remains physically owned"
        );
        fixture.complete_materialize_handoff(ticket);
        fixture.dispose_bootstrap(resources, context_actor());
        fixture.finish_model_refusal();
    }

    #[test]
    fn actor_registration_refusal_preserves_all_three_child_roles_and_original_funding() {
        let mut fixture = Harness::new(CONTEXT_BOOTSTRAP_STORAGE);
        let ticket = fixture.materialize();
        let refusal = ContextHostRecord::adopt(
            &fixture.runtime,
            &fixture.ledger,
            &ticket,
            fixture.binding(control_actor(), &ticket),
            fixture.roles[0].work.child_authority().unwrap(),
        )
        .err()
        .unwrap();
        assert!(matches!(refusal.error, ContextHostError::Driver(_)));
        let resources = *refusal
            .resources
            .expect("bound partial Context inventory returned intact");
        assert!(resources.scope.is_some());
        assert_eq!(resources.roles.len(), 3);
        assert!(!resources.registered);
        assert_eq!(
            resources.scope.as_ref().unwrap().snapshot().unwrap().work,
            3
        );
        assert!(
            fixture.scope.snapshot().unwrap().work >= 4,
            "the original bootstrap parent remains physically owned"
        );
        fixture.complete_materialize_handoff(ticket);
        fixture.dispose_bootstrap(resources, context_actor());
        fixture.finish_model_refusal();
    }
}
