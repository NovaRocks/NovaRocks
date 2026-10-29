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
pub(crate) mod observation;
pub(crate) use driver::{ContextDriverError, ContextDriverProgress};
pub(crate) mod root_read;
pub(crate) use root_read::{NodeRootDelivery, NodeRootRead, RootReadError};
pub(super) mod task_driver;
pub(crate) use task_driver::{
    TaskDriverError, TaskDriverPorts, TaskDriverProgress, TaskDriverRefusal,
};
mod teardown;
pub(crate) use teardown::ContextTeardownRefusal;
mod internal;
mod owner_control;
pub(crate) use internal::ContextInternalAdvance;
mod progress;
pub(crate) use owner_control::OwnerFenceDelivery;
pub(crate) use progress::{ContextHostProgress, ContextHostProgressError};

use super::{
    actors::{ContextActor, WorkerAction, WorkerActor},
    context_host_jobs::{
        ContextHostJob, ContextHostJobBinding, ContextHostJobError, ContextHostJobs,
        MaterializationCaptureRefusal,
    },
    context_io::{
        CONTEXT_IO_CALLBACKS, CONTEXT_IO_ROLES, CONTEXT_IO_STORAGE, ContextIoError,
        ContextIoInventory,
    },
    context_lifecycle::{CONTEXT_LIFECYCLE_STORAGE, ContextLifecycle},
    context_ready::{ContextReadyError, ContextReadyNotice},
    effect_dispatch::{EffectDispatch, EffectRef},
    establish_ingress::ValidatedMaterializationSource,
    establish_route::{EstablishRouteCustody, EstablishRouteError},
    task::{TaskHostBinding, TaskHostError, TaskHostOwner, TaskHostRecord},
};
use crate::{
    control_admission::{AdmissionStorage, Dimension},
    control_runtime_policy::{ControlRuntimePolicy, StorageNeed},
};
use novarocks_control_runtime::{
    driver::{ActorRegistration, ActorRuntime, DriverError, EffectTicket},
    lease::{CapacityBundle, WorkLease},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, InputId, ReservationId},
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{QueryContextRef, TaskIdentity};
use novarocks_worker_protocol::{context::ContextCore, control::ControlEffect};
use std::{collections::BTreeMap, sync::Arc};

// The actual child-scope metadata holders. The original admission and the
// child-scope parent link are the other two Work records, in the Control scope.
const LOCAL_ROLES: [WorkKind; 3] = [WorkKind::Business, WorkKind::Step, WorkKind::Cleanup];
const LIFETIME: usize = 0;
const TIMER: usize = 1;
const FAILURE: usize = 2;

/// Exactly this bootstrap's metadata, independent of Task/request/root pools.
/// Includes the fixed M/R/opaque-holder inventory. Jobs must bind those actual
/// roles and both callbacks before capture; shared-domain U is still separate.
pub(crate) const CONTEXT_BOOTSTRAP_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 1
        + CONTEXT_IO_STORAGE.inputs
        + CONTEXT_LIFECYCLE_STORAGE.inputs
        + owner_control::OWNER_CONTROL_STORAGE.inputs
        + internal::CONTEXT_INTERNAL_STORAGE.inputs,
    batches: 1
        + CONTEXT_IO_STORAGE.batches
        + CONTEXT_LIFECYCLE_STORAGE.batches
        + owner_control::OWNER_CONTROL_STORAGE.batches
        + internal::CONTEXT_INTERNAL_STORAGE.batches,
    effects: crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS
        + CONTEXT_IO_STORAGE.effects
        + CONTEXT_LIFECYCLE_STORAGE.effects
        + owner_control::OWNER_CONTROL_STORAGE.effects
        + internal::CONTEXT_INTERNAL_STORAGE.effects,
    cuts: 0,
    replies: 0,
    completions: CONTEXT_IO_STORAGE.completions,
    captures: CONTEXT_IO_STORAGE.captures,
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
    Driver(DriverError),
    Runtime(RuntimeError),
    AdmissionBindInvariant(RuntimeError),
    Task(TaskHostError),
    Io(ContextIoError),
    HostJob(ContextHostJobError),
    Ready(ContextReadyError),
    RootRead(root_read::RootReadError),
}

pub(crate) struct ContextMetadataRole {
    pub work: WorkLease,
    pub capacity: CapacityBundle,
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
    pub(super) teardown: teardown::ContextTeardownState,
    pub(super) control_admission_closed: bool,
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
    pub notice: Option<ContextReadyNotice>,
    io_attempted: bool,
    pub requests:
        BTreeMap<novarocks_control_runtime_core::identity::WorkId, requests::ContextRequestCustody>,
    failure: Option<failure::ScopeFailureOwner>,
    pub request_partials: BTreeMap<
        novarocks_control_runtime_core::identity::WorkId,
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
    // The original Materialize ticket was validated by this exact runtime.
    // A caller-supplied runtime or matching numeric IDs cannot replace it.
    control_runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    establishment: novarocks_worker_protocol::context::ContextEstablishment,
    initial_generation: novarocks_control_runtime_core::identity::Generation,
    resources: ContextHostResources,
    tasks: BTreeMap<TaskIdentity, TaskHostOwner>,
    retired_task_reads: BTreeMap<TaskIdentity, Arc<super::lifecycle_read::TaskReadSnapshot>>,
    pub(super) task_driver: task_driver::TaskDriverState,
    root_issuance_sealed: bool,
    max_live_tasks: usize,
    max_live_roots: usize,
    max_routes: usize,
    pub dispatch: EffectDispatch,
    request_inputs: BTreeMap<InputId, novarocks_control_runtime_core::identity::WorkId>,
    task_request_cuts: BTreeMap<
        (
            TaskIdentity,
            novarocks_worker_protocol::proposal_summary::CutId,
        ),
        novarocks_control_runtime_core::identity::WorkId,
    >,
}
impl ContextHostRecord {
    #[cfg(test)]
    pub(crate) fn adopt(
        runtime: &ActorRuntime<WorkerActor>,
        ledger: &RuntimeLedger,
        ticket: &EffectTicket<WorkerAction>,
        binding: ContextHostBinding,
    ) -> Result<Self, ContextHostRefusal> {
        Self::adopt_with_routes(runtime, ledger, ticket, binding, 0)
    }
    pub(crate) fn adopt_with_routes(
        runtime: &ActorRuntime<WorkerActor>,
        ledger: &RuntimeLedger,
        ticket: &EffectTicket<WorkerAction>,
        binding: ContextHostBinding,
        max_routes: usize,
    ) -> Result<Self, ContextHostRefusal> {
        let no_owner = |error| ContextHostRefusal {
            error,
            resources: None,
        };
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
        let task_work = binding
            .policy
            .task_base
            .work
            .checked_mul(binding.max_live_tasks);
        let root_work = binding
            .policy
            .root_read_pool_per_root
            .work
            .checked_mul(binding.max_live_roots);
        let max_work = task_work
            .and_then(|n| n.checked_add(root_work?))
            .and_then(|n| n.checked_add(binding.policy.request_pool.work))
            .and_then(|n| n.checked_add(LOCAL_ROLES.len()))
            .and_then(|n| n.checked_add(max_routes))
            .and_then(|n| n.checked_add(CONTEXT_IO_ROLES))
            .ok_or_else(|| no_owner(ContextHostError::InvalidGeometry))?;
        let task_reports = binding
            .policy
            .task_base
            .completions
            .checked_mul(binding.max_live_tasks);
        let root_reports = binding
            .policy
            .root_read_pool_per_root
            .completions
            .checked_mul(binding.max_live_roots);
        let max_slots = task_reports
            .and_then(|n| n.checked_add(root_reports?))
            .and_then(|n| n.checked_add(binding.policy.request_pool.completions))
            .and_then(|n| n.checked_add(1)) // This actor's original attachment.
            .and_then(|n| n.checked_add(max_routes))
            .and_then(|n| n.checked_add(CONTEXT_IO_ROLES))
            .ok_or_else(|| no_owner(ContextHostError::InvalidGeometry))?;
        let effect_limit = actor_config
            .max_batches
            .checked_mul(crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS)
            .ok_or_else(|| no_owner(ContextHostError::InvalidGeometry))?;
        let need = CONTEXT_BOOTSTRAP_STORAGE
            .resources()
            .and_then(|need| {
                need.checked_add(
                    &ResourceVector::new([(Dimension::Contexts.id(), 1)])
                        .expect("one fixed Context admission dimension"),
                )
                .map_err(|_| crate::control_runtime_policy::PolicyError::Overflow)
            })
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
            if need
                .iter()
                .any(|(dimension, count)| paid.get(dimension) < count)
            {
                return Err(ContextHostError::BootstrapUnfunded);
            }
            let (scope, root) = ledger
                .open_child_scope_with_root(
                    &resources.admission.work,
                    generation,
                    max_work,
                    max_slots,
                    LOCAL_ROLES[LIFETIME],
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
                        ResourceVector::default(),
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
                resources.roles[TIMER].work.id(),
                resources.roles[LIFETIME].work.id(),
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
                    &resources.roles[TIMER].work,
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
            control_runtime: runtime.clone(),
            ledger: ledger.clone(),
            establishment,
            initial_generation: generation,
            resources,
            tasks: BTreeMap::new(),
            retired_task_reads: BTreeMap::new(),
            task_driver: task_driver::TaskDriverState::default(),
            root_issuance_sealed: false,
            max_live_tasks: binding.max_live_tasks,
            max_live_roots: binding.max_live_roots,
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
    pub(super) fn ready_notice(&self) -> &ContextReadyNotice {
        self.resources
            .notice
            .as_ref()
            .expect("published original Context host notice")
    }
    pub(crate) fn matches_ready(&self, notice: &ContextReadyNotice) -> bool {
        self.resources
            .notice
            .as_ref()
            .is_some_and(|original| original.same_origin(notice))
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
        self.resources.io = Some(
            ContextIoInventory::admit(
                &self.control_runtime,
                self.actor,
                self.scope(),
                self.resources.roles[LIFETIME].capacity.id(),
                self.scope()
                    .gates()
                    .map_err(ContextHostError::Runtime)?
                    .generation,
            )
            .map_err(ContextHostError::Io)?,
        );
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
                admission_work: self.resources.admission.work.id(),
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
            self.control,
            self.actor,
            self.identity,
            self.scope().clone(),
            &self.resources.admission.work,
        );
        let result = owner.initialize(&self.resources.admission.work);
        // Even a partial Root input bank remains with this same actual payer.
        self.resources.lifecycle = Some(owner);
        result.map_err(ContextHostError::HostJob)
    }
    pub(crate) fn capture_materialization(
        &self,
        source: ValidatedMaterializationSource,
        factory: Arc<dyn crate::QueryContextHostFactory>,
    ) -> Result<ContextHostJob, MaterializationCaptureRefusal> {
        match &self.resources.host_jobs {
            Some(jobs) => jobs.capture_materialization(source, factory),
            None => Err(MaterializationCaptureRefusal {
                error: ContextHostJobError::WrongPhase,
                source,
                factory,
            }),
        }
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
    pub(crate) fn handoff_retention(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (ContextHostJobError, EffectTicket<WorkerAction>)> {
        let Some(lifecycle) = &mut self.resources.lifecycle else {
            return Err((ContextHostJobError::WrongPhase, ticket));
        };
        let Some(normal_close) = self
            .resources
            .host_jobs
            .as_ref()
            .and_then(ContextHostJobs::normal_close_observation)
        else {
            return Err((ContextHostJobError::WrongPhase, ticket));
        };
        if let Err(error) = lifecycle.forward_retained(&ticket, normal_close) {
            return Err((error, ticket));
        }
        let reference = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        self.complete_host_effect(ticket)
            .map_err(|refusal| (ContextHostJobError::Driver(refusal.error), refusal.ticket))?;
        if self.dispatch.completion_observed(reference).is_err() {
            std::process::abort();
        }
        Ok(())
    }
    pub(crate) fn publish_host_removed(&mut self) -> Result<bool, ContextHostJobError> {
        let jobs = self
            .resources
            .host_jobs
            .as_ref()
            .ok_or(ContextHostJobError::WrongPhase)?;
        self.resources
            .lifecycle
            .as_mut()
            .ok_or(ContextHostJobError::WrongPhase)?
            .publish_removed(
                jobs,
                self.resources
                    .notice
                    .as_ref()
                    .ok_or(ContextHostJobError::WrongPhase)?,
            )
    }
    pub(crate) fn retire_lifecycle(&mut self) -> Result<bool, ContextHostJobError> {
        self.resources
            .lifecycle
            .as_mut()
            .ok_or(ContextHostJobError::WrongPhase)?
            .retire_after_removed()
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
        // A captured address is not a lifecycle pin. A sealed/removed actor
        // cannot admit a new Context leg after its metadata sources exit.
        match self.control_runtime.snapshot(self.actor) {
            Ok(snapshot) if snapshot.sealed || self.resources.control_admission_closed => {
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
        // Store this alias before the Context's first Work/report/input.
        self.resources.routes.insert(request.token, route.clone());
        let initialized = route.initialize_context(&self.resources.roles[LIFETIME].work);
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
        let Some(route) = self.resources.routes.get(&request.token) else {
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
        if !route.try_retire_context()? {
            return Ok(false);
        }
        self.resources.routes.remove(&request);
        Ok(true)
    }
    pub(crate) fn establish_route(&self, request: InputId) -> Option<&Arc<EstablishRouteCustody>> {
        self.resources.routes.get(&request)
    }

    /// The original Root decision authorizes parent teardown. Keep both its
    /// Context slot and metadata payment until the parent records really exit.
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
        self.resources.teardown.parent_retire_authorized = true;
        Ok(())
    }
    pub(crate) fn adopt_task(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
        binding: TaskHostBinding,
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
        let mut host = match TaskHostRecord::adopt_loading(runtime, ticket, binding) {
            Ok(host) => host,
            Err(refusal) => {
                if let Some(unpublished) = refusal.unpublished {
                    assert!(
                        self.tasks
                            .insert(task, TaskHostOwner::Unpublished(unpublished))
                            .is_none()
                    );
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
        if self
            .scope()
            .gates()
            .map_err(ContextHostError::Runtime)?
            .failed
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
        // slot. Only this real closed Task prefix permits direct RootDrained.
        self.resources.root_reads.mark_task_retired(task);
        assert!(
            self.retired_task_reads.insert(task, sealed_read).is_none(),
            "one actual sealed read handoff per original Task"
        );
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
            .next()
            .is_some_and(|((identity, _), _)| *identity == task)
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
    CONTEXT_REQUEST_STORAGE, ContextRequestAdmissionRefusal, ContextRequestError,
    ContextRequestHandle, ContextRequestPublishRefusal, ContextRequestSubmitRefusal,
    TaskContextRequestHandle,
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
        role.capacity
            .release(&role.capacity.resources().unwrap())
            .unwrap();
    }

    // The real ControlActor issues both the ticket and Context guard. This
    // fixture owns the original bounded metadata roles; no EffectTicket or
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
        ticket: Option<ContextMetadataRole>,
        context_owner: Option<ContextMetadataRole>,
        policy: ControlRuntimePolicy,
        storage: AdmissionStorage,
        identity: QueryContextRef,
        capability: AdmissionEpochCapability,
        context_storage: StorageNeed,
        now: Tick,
    }
    impl Harness {
        fn new(context_storage: StorageNeed) -> Self {
            Self::new_with_request_capacity(context_storage, 0)
        }
        fn new_with_request_capacity(context_storage: StorageNeed, requests: usize) -> Self {
            Self::new_with_request_profile(
                context_storage,
                requests,
                super::requests::CONTEXT_REQUEST_STORAGE,
            )
        }
        fn new_with_request_profile(
            context_storage: StorageNeed,
            requests: usize,
            profile: StorageNeed,
        ) -> Self {
            Self::new_with_request_and_task_capacity(context_storage, requests, profile, 0)
        }
        fn new_with_request_and_task_capacity(
            context_storage: StorageNeed,
            requests: usize,
            profile: StorageNeed,
            task_positions: usize,
        ) -> Self {
            Self::new_with_context_identity(
                context_storage,
                requests,
                profile,
                task_positions,
                None,
            )
        }
        fn new_with_context_identity(
            context_storage: StorageNeed,
            requests: usize,
            profile: StorageNeed,
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
            let request_storage = profile
                .checked_mul(requests)
                .unwrap()
                .checked_add(policy.task_base.checked_mul(task_positions).unwrap())
                .unwrap();
            // Control owns its three actual roles, a timer and one sequential
            // return/request position. The ticket and Context pay separately.
            let mut limits = Vec::new();
            for dimension in [
                Dimension::Requests,
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
            ] {
                let count = match dimension {
                    Dimension::Requests => requests,
                    Dimension::Contexts | Dimension::Tickets => 1,
                    Dimension::Inputs | Dimension::Batches => {
                        2 + context_storage.inputs + request_storage.inputs
                    }
                    Dimension::Effects => {
                        2 * ControlCore::MAX_STEP_WORK
                            + context_storage.effects
                            + request_storage.effects
                    }
                    Dimension::Replies => 2 + request_storage.replies,
                    Dimension::Work => 3 + 1 + context_storage.work + request_storage.work,
                    _ => 1024,
                };
                limits.push((dimension.id(), u64::try_from(count).unwrap()));
            }
            let ledger = RuntimeLedger::new(ResourceVector::new(limits).unwrap(), 64, 4);
            let scope = ledger.open_scope_next(None, generation(), 32, 32).unwrap();
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
                    need.effects = ControlCore::MAX_STEP_WORK;
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
                ticket: StorageNeed {
                    work: 1,
                    ..StorageNeed::default()
                }
                .resources()
                .unwrap(),
            };
            let core = ControlCore::new(
                ControlConfig {
                    max_contexts: 1,
                    max_waiters: 1,
                    max_pending_requests: 4,
                    step_budget: ControlCore::MAX_STEP_WORK,
                    retained_contexts: 1,
                    gone_fences: 1,
                    retention: Duration::from_secs(10),
                },
                AdmissionTicketConfig::new(1, Duration::from_secs(5), Duration::from_secs(10), 4)
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
                roles[1].work.id(),
                roles[0].work.id(),
            );
            let runtime = ActorRuntime::new(2);
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
                .reserve_input(
                    control_actor(),
                    ActorId::new(90).unwrap(),
                    InputClass::Lifecycle,
                )
                .unwrap()
                .into_role()
                .unwrap();
            let service = WorkerService::from_admitted_control(
                runtime.clone(),
                ledger.clone(),
                control_actor(),
                NonZeroUsize::new(1).unwrap(),
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
                ticket: None,
                context_owner: None,
                policy,
                storage,
                identity,
                capability,
                context_storage,
                now: Tick(1),
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
                    work: self.roles[2].work.id(),
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
                        WorkerAction::Control {
                            effect: ControlEffect::AdmissionReply { result: Ok(_), .. },
                            admitted: Some(reservation),
                        } => {
                            let guard = self
                                .runtime
                                .take_step_reservation(&ticket, *reservation)
                                .unwrap();
                            let (mut work, capacity) = guard.bind(WorkKind::Business).unwrap();
                            work.start().unwrap();
                            assert!(
                                self.ticket
                                    .replace(ContextMetadataRole { work, capacity })
                                    .is_none()
                            );
                        }
                        WorkerAction::Control {
                            effect: ControlEffect::Route { state, .. },
                            ..
                        } => {
                            assert_ne!(
                                *state,
                                QueryContextState::Active,
                                "metadata bootstrap is not materialization"
                            );
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
                            if self
                                .ticket
                                .as_ref()
                                .is_some_and(|role| role.capacity.id() == *id)
                            {
                                let mut role = self.ticket.take().unwrap();
                                exit(&mut role);
                                self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
                                release(&mut role);
                            } else {
                                let role = self.context_owner.as_mut().expect(
                                    "original Context payer retained until Control HostRemoved",
                                );
                                assert_eq!(role.capacity.id(), *id);
                                role.capacity
                                    .release(
                                        &ResourceVector::new([(Dimension::Contexts.id(), 1)])
                                            .unwrap(),
                                    )
                                    .unwrap();
                                assert_eq!(
                                    role.capacity.resources().unwrap(),
                                    self.context_storage.resources().unwrap()
                                );
                            }
                        }
                        WorkerAction::Control {
                            effect: ControlEffect::Reply { outcome, .. },
                            ..
                        } => {
                            assert_eq!(*outcome, OperationOutcome::OwnerFailed);
                        }
                        WorkerAction::ControlReply(settlement) => {
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
            assert_eq!(self.ledger.used(Dimension::Tickets.id()), 1);
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
        fn dispose_service_bootstrap(&mut self, actor: ActorId) {
            let host_address = self.service.context(self.identity).unwrap();
            let mut host = host_address.lock().unwrap();
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
            let input = self
                .input
                .fill(WorkerMessage::Control(ControlMessage::Command {
                    work: self.roles[2].work.id(),
                    request: None,
                    command,
                }))
                .unwrap();
            assert!(self.runtime.poll_one(self.now).unwrap());
            let ready = self
                .runtime
                .ready_effects(control_actor(), input, ControlCore::MAX_STEP_WORK)
                .unwrap();
            assert_eq!(ready.len(), 1);
            self.runtime
                .take_effect(control_actor(), input, ready[0])
                .unwrap()
        }
        fn complete_control_effect(&self, ticket: EffectTicket<WorkerAction>) {
            let input = ticket.input;
            self.runtime.effect_completed(ticket).unwrap();
            self.runtime.retire_input(control_actor(), input).unwrap();
        }
        fn actual_model_refusal_removal(&mut self) -> EffectTicket<WorkerAction> {
            self.dispose_service_bootstrap(context_actor());
            assert!(
                self.command(
                    ControlInput::ContextBootstrapFailed {
                        context: self.identity,
                        refusal: novarocks_worker_protocol::creation::AdmissionRefusal::Verdict(
                            OperationOutcome::OwnerFailed
                        ),
                    },
                    None
                )
                .is_none()
            );
            assert!(
                self.command(
                    ControlInput::Retained {
                        context: self.identity,
                    },
                    None
                )
                .is_none()
            );
            self.now = Tick(11_000_000_000);
            let removal = self.one_control_effect(ControlInput::Tick);
            assert!(matches!(&removal.effect, WorkerAction::Control {
                effect: ControlEffect::RemoveHost { context }, ..
            } if *context == self.identity));
            removal
        }
        fn actual_model_refusal_retirement(
            &mut self,
        ) -> (ContextHostRecord, EffectTicket<WorkerAction>) {
            let removal = self.actual_model_refusal_removal();
            let host = self
                .service
                .take_removing_context(&removal)
                .unwrap()
                .unwrap();
            assert!(host.resources.scope.is_none());
            assert!(!host.resources.registered);
            self.complete_control_effect(removal);
            let retirement = self.one_control_effect(ControlInput::HostRemoved {
                context: self.identity,
            });
            assert!(matches!(&retirement.effect, WorkerAction::Control {
                effect: ControlEffect::RetireReservation(reservation), ..
            } if *reservation == host.admission()));
            (host, retirement)
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
            // No materialization body or foreign Context resource was started.
            // The original metadata/actor holders have actually exited above;
            // report that real model refusal, never a fictitious Active fact.
            assert!(
                self.command(
                    ControlInput::ContextBootstrapFailed {
                        context: self.identity,
                        refusal: novarocks_worker_protocol::creation::AdmissionRefusal::Verdict(
                            OperationOutcome::OwnerFailed
                        ),
                    },
                    None
                )
                .is_none()
            );
            assert_eq!(*self.route.read(), QueryContextState::Aborting);
            assert!(
                self.completed
                    .iter()
                    .any(|(operation_id, result)| *operation_id == operation(2)
                        && *result == RequestResult::Outcome(OperationOutcome::OwnerFailed))
            );
            assert_eq!(self.ledger.used(Dimension::Contexts.id()), 1);
            assert!(
                self.command(
                    ControlInput::Retained {
                        context: self.identity
                    },
                    None
                )
                .is_none()
            );
            assert_eq!(self.ledger.used(Dimension::Tickets.id()), 0);
            assert_eq!(self.ledger.used(Dimension::Contexts.id()), 1);
            assert_eq!(*self.route.read(), QueryContextState::TerminalRetained);
            // The real retention deadline creates RemoveHost. Service moves
            // its original owner only on that actual Control effect.
            self.now = Tick(11_000_000_000);
            assert!(self.command(ControlInput::Tick, None).is_none());
            assert!(self.service.context(self.identity).is_none());
            assert!(
                self.command(
                    ControlInput::HostRemoved {
                        context: self.identity
                    },
                    None
                )
                .is_none()
            );
            self.finish_removed();
        }
        fn finish_removed(mut self) {
            assert_eq!(self.ledger.used(Dimension::Contexts.id()), 0);
            let mut admission = self.context_owner.take().unwrap();
            assert_eq!(
                admission.capacity.resources().unwrap(),
                self.context_storage.resources().unwrap()
            );
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
        assert_eq!(host.identity(), fixture.identity);
        assert_eq!(host.actor(), context_actor());
        assert_ne!(host.scope().id(), fixture.scope.id());
        assert_eq!(host.scope().snapshot().unwrap().work, 3);
        assert_eq!(fixture.scope.snapshot().unwrap().work, 6);
        assert_eq!(
            fixture.ledger.used(Dimension::Work.id()),
            4 + u64::try_from(fixture.context_storage.work).unwrap()
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
        let quota = second.ledger.used(Dimension::Contexts.id());
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
        let fee = second.ledger.used(Dimension::Contexts.id());
        let outcome = second.service.take_removing_context(&removal);
        let retained = second.service.context(second.identity).map(|address| {
            let host = address.lock().unwrap();
            (host.admission(), host.scope().id(), host.tasks.len())
        });
        let fee_after = second.ledger.used(Dimension::Contexts.id());
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
        let first_retirement = first.one_control_effect(ControlInput::HostRemoved {
            context: first.identity,
        });
        first.finish_retired_context(first_host, first_retirement);
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
                ResourceVector::default(),
                WorkKind::Business,
                WorkAdmission::External
            ),
            Err(RuntimeError::Scope(
                novarocks_control_runtime_core::scope::ScopeError::Closed
            ))
        ));
        let lifetime = host.resources.roles[LIFETIME].capacity.id();
        // Existing responsibility may derive during drain, but a real Task's
        // nonzero loading inventory is not part of this metadata-only payer.
        assert!(matches!(
            scope.reserve_child_next(
                generation(),
                lifetime,
                fixture.policy.loading.resources().unwrap(),
                WorkKind::Business
            ),
            Err(RuntimeError::CapacityDenied(_))
        ));
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
        assert_eq!(
            fixture.ledger.used(Dimension::Work.id()),
            4 + u64::try_from(fixture.context_storage.work).unwrap()
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
            assert_eq!(guards[0].1, ticket.descriptor.work);
            let work_before = fixture.scope.snapshot().unwrap().work;
            let mut binding = fixture.binding(context_actor(), &ticket);
            if loading {
                binding.storage.loading = ResourceVector::default();
            } else {
                binding.storage.accepted = ResourceVector::default();
            }
            let refusal =
                ContextHostRecord::adopt(&fixture.runtime, &fixture.ledger, &ticket, binding)
                    .err()
                    .expect(
                        "deviating from the actual policy must refuse before ownership transfer",
                    );
            assert!(matches!(
                refusal.error,
                ContextHostError::InvalidStoragePolicy
            ));
            assert!(refusal.resources.is_none());
            assert_eq!(fixture.runtime.step_reservations(
                ticket.actor, ticket.input, None, ControlCore::MAX_STEP_WORK,
            ).unwrap(), guards);
            assert_eq!(fixture.scope.snapshot().unwrap().work, work_before);
            assert_eq!(fixture.ledger.used(Dimension::Contexts.id()), 1);

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
            assert_eq!(
                fixture.ledger.used(Dimension::Work.id()),
                4 + u64::try_from(fixture.context_storage.work).unwrap()
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
            let refusal =
                ContextHostRecord::adopt(&fixture.runtime, &fixture.ledger, &ticket, binding)
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
        assert_eq!(
            fixture.ledger.used(Dimension::Work.id()),
            4 + u64::try_from(fixture.context_storage.work).unwrap()
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
        assert_eq!(
            fixture.ledger.used(Dimension::Work.id()),
            4 + u64::try_from(fixture.context_storage.work).unwrap()
        );
        fixture.complete_materialize_handoff(ticket);
        fixture.dispose_bootstrap(resources, context_actor());
        fixture.finish_model_refusal();
    }
}
