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

//! Actual process Control bootstrap ownership. This pays only the fixed
//! lifetime, timer and failure metadata roles and the actor's timer position.
//! Request, ticket, materialization and transport inventories are independent
//! owners; this constructor does not expose a ready Native request facade.

mod host_cell;
mod ready_routes;
pub(in crate::control_runtime) mod rejected_ingress;
mod request_partial;
mod source_io;
mod teardown;
pub(crate) use host_cell::RootHostCell;
use ready_routes::ReadyRoutes;
pub(crate) use source_io::RootRequestAdvanceError;

#[cfg(test)]
use super::effect_dispatch::EffectRef;
use super::{
    actors::{ControlActor, WorkerActor},
    context_host::{ContextHostBinding, ContextMetadataRole},
    context_host_jobs::ContextHostJobs,
    control_internal::{
        ControlInternal, ControlInternalError, ControlInternalPartial, InternalAdvance,
    },
    control_request::{
        ControlRequestAdmissionRefusal, ControlRequestCustody, ControlRequestError,
        ControlRequestHandle, ControlRequestPublishRefusal, ControlRequestReady,
        ControlRequestSubmitRefusal, PreparedEstablishRoute,
    },
    effect_dispatch::{DispatchError, EffectDispatch, IndexedBatches},
    establish_executors::{
        EstablishExecutorStartError, EstablishExecutorStartRefusal, EstablishExecutors,
    },
    service::{ServiceHostError, WorkerService},
};
use crate::{control_admission::AdmissionStorage, control_runtime_policy::StorageNeed};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorFailurePolicy},
    driver::{
        ActorRegistration, ActorRuntime, CompletedEffect, DriverError, EffectCompletionRefusal,
        EffectTicket, InputRoleId,
    },
    header_readiness::HeaderReadyNotice,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    identity::{ActorId, Generation, InputId, ScopedWorkRef},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{QueryContextRef, TaskOperationId};
use novarocks_worker_protocol::{
    control::{ControlCore, ControlInput},
    lease::MonotonicInstant,
};
use std::{collections::BTreeMap, num::NonZeroUsize};

const KINDS: [WorkKind; 3] = [WorkKind::Business, WorkKind::Step, WorkKind::Cleanup];
const LIFETIME: usize = 0;
const TIMER: usize = 1;
const FAILURE: usize = 2;

pub(crate) const PROCESS_CONTROL_BOOTSTRAP_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 1,
    batches: 1,
    effects: ControlActor::MAX_STEP_ACTIONS,
    cuts: 0,
    replies: 0,
    completions: 0,
    captures: 0,
    work: KINDS.len(),
};

pub(crate) struct ProcessControlBinding {
    /// From the composition's single explicit actor identity source.
    pub actor: ActorId,
    pub generation: Generation,
    pub actor_config: ActorConfig,
    /// Actual process ingress positions, including admitted waiting calls.
    /// This bounds the ready index; each request still pays its own role.
    pub request_positions: NonZeroUsize,
    pub admission_storage: AdmissionStorage,
}
#[derive(Debug)]
pub(crate) enum ProcessBootstrapError {
    InvalidGeometry,
    Runtime(RuntimeError),
    Driver(DriverError),
    Control(ControlRequestError),
}
impl std::fmt::Display for ProcessBootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidGeometry => f.write_str("Process control bootstrap geometry is invalid"),
            Self::Runtime(error) => write!(
                f,
                "Process control bootstrap runtime operation failed: {error}"
            ),
            Self::Driver(error) => write!(f, "Process control actor registration failed: {error}"),
            Self::Control(error) => write!(f, "Process control request setup failed: {error}"),
        }
    }
}
impl std::error::Error for ProcessBootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidGeometry => None,
            Self::Runtime(error) => Some(error),
            Self::Driver(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}

#[must_use = "A refused wake retains its original effect ticket"]
pub(crate) enum InternalWakeRefusal {
    Rejected {
        error: ControlInternalError,
        ticket: EffectTicket<super::actors::WorkerAction>,
    },
    Completion(EffectCompletionRefusal<super::actors::WorkerAction>),
}
impl std::fmt::Debug for InternalWakeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected { error, ticket } => f
                .debug_struct("Rejected")
                .field("error", error)
                .field("input", &ticket.input)
                .field("action", &ticket.descriptor.id)
                .finish_non_exhaustive(),
            Self::Completion(refusal) => f
                .debug_struct("Completion")
                .field("error", &refusal.error)
                .field("input", &refusal.ticket.input)
                .field("action", &refusal.ticket.descriptor.id)
                .finish_non_exhaustive(),
        }
    }
}

#[must_use = "A refused route handoff retains its original effect"]
pub(crate) enum ControlRouteHandoffRefusal {
    Rejected {
        error: ControlRequestError,
        ticket: EffectTicket<super::actors::WorkerAction>,
    },
    Completion(EffectCompletionRefusal<super::actors::WorkerAction>),
}
impl std::fmt::Debug for ControlRouteHandoffRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected { error, ticket } => f
                .debug_struct("Rejected")
                .field("error", error)
                .field("input", &ticket.input)
                .field("action", &ticket.descriptor.id)
                .finish_non_exhaustive(),
            Self::Completion(refusal) => f
                .debug_struct("Completion")
                .field("error", &refusal.error)
                .field("input", &refusal.ticket.input)
                .field("action", &refusal.ticket.descriptor.id)
                .finish_non_exhaustive(),
        }
    }
}

/// Partial bootstrap returns these same holders, including a possibly empty
/// scope. No original role or paid bundle is dropped as an unused reservation.
#[must_use = "Retain the process root until every original owner actually exits"]
pub(crate) struct ProcessControlResources {
    pub admission_snapshot: novarocks_control_runtime::publication::PublishedSnapshot<
        super::node_metrics::ControlAdmissionProjection,
    >,
    pub scope: ScopeHandle,
    pub roles: Vec<ContextMetadataRole>,
    pub registered: bool,
    pub request_ready: ControlRequestReady,
    pub requests: BTreeMap<InputId, ControlRequestCustody>,
    // One neutral routing row per live, fully admitted original Root R.
    // This neither records exit nor creates a request or intermediate Work.
    request_header_routes: BTreeMap<ScopedWorkRef, InputId>,
    request_partials: request_partial::RootRequestPartials,
    rejected_ingress: Option<rejected_ingress::RejectedIngressLane>,
    pub dispatch: EffectDispatch,
    pub internal: Option<ControlInternal>,
    pub internal_partial: Option<ControlInternalPartial>,
    teardown: teardown::ProcessControlTeardown,
    task_capacity: Option<super::task_capacity::TaskCapacitySignal>,
    // Addresses of the original R owners, never decisions or obligations.
    // Keys follow actual metadata handoff order, not request token order.
    route_waiters: BTreeMap<QueryContextRef, BTreeMap<u64, InputId>>,
    ready_routes: ReadyRoutes,
    next_route_order: u64,
    // Original paid return-role addresses. Repeated occurrence InputIds are
    // not RPC tokens; even an empty callback batch must wake its same R.
    return_routes: BTreeMap<InputRoleId, InputId>,
}

/// Construction evidence from the actual successful registration. It is not
/// clonable or constructible by a raw actor identity, and grants no new quota.
pub(super) struct RegisteredControl {
    runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    actor: ActorId,
    requests: NonZeroUsize,
    process_source_parent: novarocks_control_runtime::lease::WorkChildAuthority,
}
impl RegisteredControl {
    pub(super) fn into_parts(
        self,
    ) -> (
        ActorRuntime<WorkerActor>,
        RuntimeLedger,
        ActorId,
        NonZeroUsize,
        novarocks_control_runtime::lease::WorkChildAuthority,
    ) {
        (
            self.runtime,
            self.ledger,
            self.actor,
            self.requests,
            self.process_source_parent,
        )
    }
}
pub(crate) struct ProcessBootstrapRefusal {
    pub error: ProcessBootstrapError,
    pub resources: Option<ProcessControlResources>,
}
impl std::fmt::Debug for ProcessBootstrapRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessBootstrapRefusal")
            .field("error", &self.error)
            .field("scope", &self.resources.as_ref().map(|r| r.scope.id()))
            .field("roles", &self.resources.as_ref().map(|r| r.roles.len()))
            .finish()
    }
}

/// Service is constructed only after this same Control registration succeeds.
/// Directory metadata follows actual admitted Context objects. The host owns
/// the original root outside the pure actor, so a
/// failed Core cannot destroy or recreate its cleanup authority.
#[must_use = "The actual process owner must drain before its resources can retire"]
pub(crate) struct ProcessControlOwner {
    actor: ActorId,
    registration: ActorRegistration,
    service: WorkerService,
    resources: ProcessControlResources,
    source_executors_started: bool,
}
impl ProcessControlOwner {
    pub(crate) fn bootstrap(
        runtime: &ActorRuntime<WorkerActor>,
        ledger: &RuntimeLedger,
        core: ControlCore,
        mut binding: ProcessControlBinding,
    ) -> Result<Self, ProcessBootstrapRefusal> {
        let invalid = || ProcessBootstrapRefusal {
            error: ProcessBootstrapError::InvalidGeometry,
            resources: None,
        };
        if binding.actor_config.max_step_work < ControlActor::MAX_STEP_WORK
            || binding.actor_config.max_effects_per_step < ControlActor::MAX_STEP_ACTIONS
            || binding.actor_config.max_dependencies_per_effect < ControlActor::MAX_STEP_ACTIONS
            || binding.actor_config.validate().is_err()
        {
            return Err(invalid());
        }
        binding.actor_config.mailbox
            [novarocks_control_runtime_core::mailbox::InputClass::Control.index()] = binding
            .actor_config
            .mailbox[novarocks_control_runtime_core::mailbox::InputClass::Control.index()]
        .checked_add(binding.request_positions.get())
        .ok_or_else(invalid)?;
        binding.actor_config.max_batches = binding
            .actor_config
            .max_batches
            .checked_add(binding.request_positions.get())
            .ok_or_else(invalid)?;
        let effect_positions = binding
            .actor_config
            .max_batches
            .checked_mul(binding.actor_config.max_effects_per_step)
            .ok_or_else(invalid)?;
        let need = PROCESS_CONTROL_BOOTSTRAP_STORAGE
            .resources()
            .map_err(|_| invalid())?;
        let scope = ledger
            .open_scope_next(None, binding.generation)
            .map_err(|error| ProcessBootstrapRefusal {
                error: ProcessBootstrapError::Runtime(error),
                resources: None,
            })?;
        let mut resources = ProcessControlResources {
            admission_snapshot: novarocks_control_runtime::publication::PublishedSnapshot::new(
                &scope,
                std::sync::Arc::new(super::node_metrics::ControlAdmissionProjection {
                    epoch: core.admission().capability(),
                    logical_live_grants: core.admission().logical_live_grants(),
                    admission_retention: core.admission().retention_inventory(),
                    control_retention: core.retention_inventory(),
                }),
            ),
            scope: scope.clone(),
            roles: Vec::with_capacity(KINDS.len()),
            registered: false,
            request_ready: ControlRequestReady::new(binding.request_positions, runtime.clone()),
            requests: BTreeMap::new(),
            request_header_routes: BTreeMap::new(),
            request_partials: request_partial::RootRequestPartials::default(),
            rejected_ingress: None,
            dispatch: EffectDispatch::control(runtime.clone(), binding.actor, effect_positions),
            internal: None,
            internal_partial: None,
            teardown: teardown::ProcessControlTeardown::default(),
            task_capacity: None,
            route_waiters: BTreeMap::new(),
            ready_routes: ReadyRoutes::new(binding.request_positions),
            next_route_order: 0,
            return_routes: BTreeMap::new(),
        };
        let admitted = (|| -> Result<ActorRegistration, ProcessBootstrapError> {
            let root = scope
                .reserve_next(
                    binding.generation,
                    need,
                    KINDS[LIFETIME],
                    WorkAdmission::External,
                )
                .map_err(ProcessBootstrapError::Runtime)?;
            let (work, capacity) = match root.bind(KINDS[LIFETIME]) {
                Ok(bound) => bound,
                Err(_error) => {
                    // This fresh guard has no foreign captures. Its failure
                    // still indicates shared admission corruption, not quota.
                    std::process::abort();
                }
            };
            resources.roles.push(ContextMetadataRole { work, capacity });
            for kind in KINDS.into_iter().skip(1) {
                let guard = scope
                    .reserve_next(
                        binding.generation,
                        ReservationResources::work_only(),
                        kind,
                        WorkAdmission::Derived(resources.roles[LIFETIME].work.id()),
                    )
                    .map_err(ProcessBootstrapError::Runtime)?;
                let (work, capacity) = match guard.bind(kind) {
                    Ok(bound) => bound,
                    Err(_error) => {
                        std::process::abort();
                    }
                };
                resources.roles.push(ContextMetadataRole { work, capacity });
            }
            resources.roles[TIMER]
                .work
                .start()
                .map_err(ProcessBootstrapError::Runtime)?;
            let actor = ControlActor::new(
                core,
                scope.clone(),
                binding.admission_storage,
                resources.roles[TIMER].work.reference(),
                resources.roles[LIFETIME].work.reference(),
            );
            let registration = runtime
                .register_supervised_bound_with_policy(
                    binding.actor,
                    scope.clone(),
                    WorkerActor::Control(Box::new(actor)),
                    binding.actor_config,
                    &resources.roles[FAILURE].work,
                    ActorFailurePolicy::ProcessFailStop,
                )
                .map_err(|(error, _pure_actor)| ProcessBootstrapError::Driver(error))?;
            resources.registered = true;
            Ok(registration)
        })();
        let registration = match admitted {
            Ok(registration) => registration,
            Err(error) => {
                return Err(ProcessBootstrapRefusal {
                    error,
                    resources: Some(resources),
                });
            }
        };
        // Pre-R rejected ingress can be the first Root activity. Bind the
        // existing cleanup capacity wake before any request slot is registered.
        if let Err(error) = resources
            .request_ready
            .bind_source_capacity_owner(&registration)
        {
            return Err(ProcessBootstrapRefusal {
                error: ProcessBootstrapError::Control(error),
                resources: Some(resources),
            });
        }
        resources.rejected_ingress = Some(rejected_ingress::RejectedIngressLane::new(
            runtime.clone(),
            registration.clone(),
            ledger.clone(),
            scope,
            resources.request_ready.clone(),
        ));
        let process_source_parent = match resources.roles[LIFETIME].work.child_authority() {
            Ok(authority) => authority,
            Err(error) => {
                return Err(ProcessBootstrapRefusal {
                    error: ProcessBootstrapError::Runtime(error),
                    resources: Some(resources),
                });
            }
        };
        Ok(Self {
            actor: binding.actor,
            registration,
            service: WorkerService::from_registered_control(RegisteredControl {
                runtime: runtime.clone(),
                ledger: ledger.clone(),
                actor: binding.actor,
                requests: binding.request_positions,
                process_source_parent,
            }),
            resources,
            source_executors_started: false,
        })
    }
    /// One process composition shares this same signal between physical Task
    /// pools and the original Root turn. Each registered actor owns at most
    /// one neutral address node; it carries no task or resource authority.
    pub(crate) fn task_capacity_signal(
        &mut self,
    ) -> Result<super::task_capacity::TaskCapacitySignal, super::task_capacity::TaskCapacityError>
    {
        if let Some(original) = &self.resources.task_capacity {
            return Ok(original.clone());
        }
        let original = super::task_capacity::TaskCapacitySignal::new(
            self.runtime().clone(),
            self.registration.clone(),
        )?;
        self.resources.task_capacity = Some(original.clone());
        Ok(original)
    }
    pub(crate) fn registration(&self) -> &ActorRegistration {
        &self.registration
    }
    pub(crate) fn actor(&self) -> ActorId {
        self.actor
    }
    pub(crate) fn start_establish_executors(
        &mut self,
        getter_workers: NonZeroUsize,
        cleanup_workers: NonZeroUsize,
    ) -> Result<EstablishExecutors, EstablishExecutorStartRefusal> {
        if std::mem::replace(&mut self.source_executors_started, true) {
            return Err(EstablishExecutorStartRefusal {
                error: EstablishExecutorStartError::AlreadyStarted,
                owner: None,
            });
        }
        EstablishExecutors::start(
            self.resources.request_ready.clone(),
            getter_workers,
            cleanup_workers,
        )
    }
    /// Mechanical projection of the same original source role. In particular,
    /// physical executor drain cannot turn Unresolved into observed raw exit.
    #[cfg(test)]
    pub(crate) fn establish_source_phase(
        &self,
        input: InputId,
    ) -> Result<Option<novarocks_control_runtime::exit_role::ExitRolePhase>, ControlRequestError>
    {
        self.resources
            .requests
            .get(&input)
            .map(ControlRequestCustody::ingress_source_phase)
            .ok_or(ControlRequestError::WrongRequest)
    }
    pub(super) fn runtime(&self) -> &ActorRuntime<WorkerActor> {
        self.service.runtime()
    }
    pub(super) fn ledger(&self) -> &RuntimeLedger {
        self.service.ledger()
    }
    pub(super) fn request_ready(&self) -> &ControlRequestReady {
        &self.resources.request_ready
    }
    pub(super) fn lifetime_work(&self) -> novarocks_control_runtime_core::identity::WorkId {
        self.resources.roles[LIFETIME].work.id()
    }
    pub(in crate::control_runtime) fn lifetime_author(
        &self,
    ) -> &novarocks_control_runtime::lease::WorkLease {
        &self.resources.roles[LIFETIME].work
    }
    /// This independent source is paid before any production facade opens.
    /// The metadata bootstrap alone never promised an internally ready host.
    pub(crate) fn enable_internal_source(&mut self) -> Result<(), ControlInternalError> {
        if self.resources.internal.is_some() || self.resources.internal_partial.is_some() {
            return Err(ControlInternalError::AlreadyAdmitted);
        }
        match ControlInternal::admit(self) {
            Ok(source) => {
                self.resources.internal = Some(source);
                Ok(())
            }
            Err(refusal) => {
                self.resources.internal_partial = refusal.partial;
                Err(refusal.error)
            }
        }
    }
    #[cfg(test)]
    fn accept_internal_wake(
        &mut self,
        ticket: &EffectTicket<super::actors::WorkerAction>,
    ) -> Result<(), ControlInternalError> {
        self.resources
            .internal
            .as_mut()
            .ok_or(ControlInternalError::NotEnabled)?
            .accept_wake(ticket)
    }
    pub(crate) fn handoff_internal_wake(
        &mut self,
        ticket: EffectTicket<super::actors::WorkerAction>,
    ) -> Result<(), InternalWakeRefusal> {
        let Some(source) = self.resources.internal.as_mut() else {
            return Err(InternalWakeRefusal::Rejected {
                error: ControlInternalError::NotEnabled,
                ticket,
            });
        };
        if let Err(error) = source.accept_wake(&ticket) {
            return Err(InternalWakeRefusal::Rejected { error, ticket });
        }
        let completed = self
            .runtime()
            .effect_completed_with_receipt(ticket)
            .map_err(InternalWakeRefusal::Completion)?;
        if self
            .control_completion_observed_with_receipt(&completed)
            .is_err()
        {
            std::process::abort();
        }
        Ok(())
    }
    pub(crate) fn advance_internal(&mut self) -> Result<InternalAdvance, ControlInternalError> {
        if self.resources.teardown.internal_closed {
            return Ok(InternalAdvance::Idle);
        }
        self.resources
            .internal
            .as_mut()
            .ok_or(ControlInternalError::NotEnabled)?
            .advance()
    }
    pub(crate) fn enqueue_failed_lifecycle(
        &mut self,
        proof: &novarocks_control_runtime::driver::ActorFailureProof,
        fact: super::control_internal::RootLifecycleFact,
        notice: super::context_ready::ContextReadyNotice,
    ) -> Result<super::control_internal::RootLifecycleReceipt, ControlInternalError> {
        if self.resources.teardown.internal_closed {
            return Err(ControlInternalError::Closed);
        }
        self.resources
            .internal
            .as_mut()
            .ok_or(ControlInternalError::NotEnabled)?
            .enqueue_failed_lifecycle(proof, fact, notice)
    }
    /// The original removed-host owner keeps this receipt until the Root's
    /// existing Source has consumed and retired the same HostRemoved input.
    pub(crate) fn enqueue_host_removed(
        &mut self,
        original: &novarocks_control_runtime::lease::WorkLease,
        context: QueryContextRef,
    ) -> Result<super::control_internal::RootLifecycleReceipt, ControlInternalError> {
        if self.resources.teardown.internal_closed {
            return Err(ControlInternalError::Closed);
        }
        self.resources
            .internal
            .as_mut()
            .ok_or(ControlInternalError::NotEnabled)?
            .enqueue_host_removed(original, context)
    }
    /// Store the actual request before any command capture. Ready's R bound
    /// is the single address bound; this map contains the original owners,
    /// never a copy of the Core's operation decisions or reply bodies.
    pub(crate) fn admit_request(
        &mut self,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(InputId, ControlRequestHandle), ControlRequestAdmissionRefusal> {
        let (custody, handle) =
            match ControlRequestCustody::admit(self, source, operation, deadline) {
                Ok(original) => original,
                Err(refusal) => return Err(self.retain_request_partial(refusal)),
            };
        let input = self.insert_admitted_request(custody);
        Ok((input, handle))
    }
    fn insert_admitted_request(&mut self, custody: ControlRequestCustody) -> InputId {
        let input = custody.input_id();
        let work = custody.header_work();
        if self.resources.requests.contains_key(&input)
            || self.resources.request_header_routes.contains_key(&work)
        {
            // Never replace or unwind through either original owner.
            std::process::abort();
        }
        self.resources.request_header_routes.insert(work, input);
        self.resources.requests.insert(input, custody);
        input
    }

    /// The caller holds this actual Root host turn. Numeric Work equality is
    /// only a lookup: the original registration must match the same cell.
    pub(crate) fn notify_request_header_readiness(
        &self,
        notice: &HeaderReadyNotice,
    ) -> Result<bool, ControlRequestError> {
        self.runtime()
            .validate_registration(self.registration())
            .map_err(ControlRequestError::Driver)?;
        let Some(input) = self.resources.request_header_routes.get(&notice.work()) else {
            return Ok(false);
        };
        let original = self
            .resources
            .requests
            .get(input)
            .ok_or(ControlRequestError::WrongRequest)?;
        Ok(original.notify_header_readiness(notice))
    }

    pub(super) fn remove_retired_request(&mut self, input: InputId) {
        let original = self
            .resources
            .requests
            .get(&input)
            .expect("same original retired Root R");
        assert!(
            original.is_retired(),
            "Root R removal requires its original true retirement"
        );
        let work = original.header_work();
        assert_eq!(
            self.resources.request_header_routes.remove(&work),
            Some(input)
        );
        self.resources.requests.remove(&input);
    }

    pub(crate) fn submit_ticket_observation(
        &mut self,
        input: InputId,
        ticket: novarocks_execution_contract::AdmissionTicketId,
        context: QueryContextRef,
        preflight: bool,
    ) -> Result<(), ControlRequestError> {
        self.resources
            .requests
            .get_mut(&input)
            .ok_or(ControlRequestError::WrongRequest)?
            .submit_ticket_observation(ticket, context, preflight)
    }
    pub(crate) fn admit_establish_request(
        &mut self,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(InputId, ControlRequestHandle), ControlRequestAdmissionRefusal> {
        let (custody, handle) =
            match ControlRequestCustody::admit_establish(self, source, operation, deadline) {
                Ok(original) => original,
                Err(refusal) => return Err(self.retain_request_partial(refusal)),
            };
        let input = self.insert_admitted_request(custody);
        Ok((input, handle))
    }
    pub(crate) fn begin_establish_ingress(
        &mut self,
        input: InputId,
        request: novarocks_execution_contract::EstablishQueryContext,
        payload: novarocks_worker_protocol::creation::PayloadId,
    ) -> Result<
        super::establish_ingress::EstablishIngressJob,
        super::control_request::EstablishIngressBeginRefusal,
    > {
        let Some(custody) = self.resources.requests.get_mut(&input) else {
            return Err(super::control_request::EstablishIngressBeginRefusal::new(
                ControlRequestError::WrongRequest,
                request,
            ));
        };
        custody.begin_establish_ingress(request, payload)
    }
    #[cfg(test)]
    pub(crate) fn take_establish_ingress(
        &mut self,
        input: InputId,
    ) -> Result<Option<super::establish_ingress::EstablishIngressOutcome>, ControlRequestError>
    {
        self.resources
            .requests
            .get_mut(&input)
            .ok_or(ControlRequestError::WrongRequest)?
            .take_establish_ingress()
    }
    #[cfg(test)]
    pub(crate) fn begin_unsubmitted_withdrawal(
        &mut self,
        input: InputId,
    ) -> Result<Option<super::establish_ingress::EstablishDisposalJob>, ControlRequestError> {
        self.resources
            .requests
            .get_mut(&input)
            .ok_or(ControlRequestError::WrongRequest)?
            .begin_unsubmitted_withdrawal()
    }
    /// Bind the original Root return without borrowing the Context owner.
    /// The caller prepared these same Jobs under its separate Context cell.
    pub(crate) fn bind_materialization_return(
        &mut self,
        ticket: &EffectTicket<super::actors::WorkerAction>,
        jobs: ContextHostJobs,
    ) -> Result<(), ControlRequestError> {
        let custody = self
            .resources
            .requests
            .get_mut(&ticket.input)
            .ok_or(ControlRequestError::WrongRequest)?;
        let result = custody.bind_creator_return(ticket, jobs, &self.resources.roles[FAILURE].work);
        if let Some(role) = custody.route_return_role() {
            match self.resources.return_routes.entry(role) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(ticket.input);
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    assert_eq!(*entry.get(), ticket.input)
                }
            }
        }
        result
    }
    /// Borrow the elected original R only after the Context issuer has
    /// admitted its independent M call and all of its return positions.
    pub(crate) fn take_materialization_source(
        &self,
        ticket: &EffectTicket<super::actors::WorkerAction>,
    ) -> Result<super::establish_ingress::ValidatedMaterializationSource, ControlRequestError> {
        self.resources
            .requests
            .get(&ticket.input)
            .ok_or(ControlRequestError::WrongRequest)?
            .take_materialization_source(ticket)
    }
    #[cfg(test)]
    pub(crate) fn begin_rejected_ingress_disposal(
        &self,
        ticket: &EffectTicket<super::actors::WorkerAction>,
    ) -> Result<Option<super::establish_ingress::EstablishDisposalJob>, ControlRequestError> {
        let super::actors::WorkerAction::ControlReply(reply) = &ticket.effect else {
            return Err(ControlRequestError::WrongEffect);
        };
        self.resources
            .requests
            .get(&reply.request.token)
            .ok_or(ControlRequestError::WrongRequest)?
            .begin_rejected_ingress_disposal(ticket)
    }
    #[cfg(test)]
    pub(crate) fn submit_establish_ingress_failure(
        &mut self,
        input: InputId,
    ) -> Result<(), ControlRequestError> {
        self.resources
            .requests
            .get_mut(&input)
            .ok_or(ControlRequestError::WrongRequest)?
            .submit_ingress_owner_failure()
            .map_err(|refusal| refusal.error)
    }
    pub(crate) fn submit_request(
        &mut self,
        input: InputId,
        command: ControlInput,
    ) -> Result<(), ControlRequestSubmitRefusal> {
        let Some(custody) = self.resources.requests.get_mut(&input) else {
            return Err(ControlRequestSubmitRefusal {
                error: ControlRequestError::WrongRequest,
                command: Some(command),
            });
        };
        custody.submit(command)
    }
    /// Completing this metadata handoff never waits for a Context response.
    /// A construction refusal after registration returns its retained partial
    /// to the live cleanup inventory and still releases this ordering lane.
    pub(crate) fn handoff_establish_route(
        &mut self,
        ticket: EffectTicket<super::actors::WorkerAction>,
    ) -> Result<(), ControlRouteHandoffRefusal> {
        use super::actors::WorkerAction;
        use novarocks_worker_protocol::control::ControlEffect;
        let WorkerAction::Control {
            effect: ControlEffect::RouteEstablish { request, .. },
            ..
        } = &ticket.effect
        else {
            return Err(ControlRouteHandoffRefusal::Rejected {
                error: ControlRequestError::WrongEffect,
                ticket,
            });
        };
        let Some(custody) = self.resources.requests.get_mut(&request.request.token) else {
            return Err(ControlRouteHandoffRefusal::Rejected {
                error: ControlRequestError::WrongRequest,
                ticket,
            });
        };
        let Some(order) = self.resources.next_route_order.checked_add(1) else {
            std::process::abort();
        };
        let context = match custody.prepare_establish_route(&ticket, order) {
            Ok(context) => context,
            Err(error) => return Err(ControlRouteHandoffRefusal::Rejected { error, ticket }),
        };
        self.resources.next_route_order = order;
        assert!(
            self.resources
                .route_waiters
                .entry(context)
                .or_default()
                .insert(order, request.request.token)
                .is_none()
        );
        // Always bind this pair's head, including when a newer R encounters
        // an actor that appeared after an older R had already been deferred.
        if self.service.context(context).is_some() {
            self.resources.ready_routes.mark(context);
            self.runtime()
                .notify_host_actor(&self.registration)
                .unwrap_or_else(|_| std::process::abort());
        }
        let completed = self
            .runtime()
            .effect_completed_with_receipt(ticket)
            .map_err(ControlRouteHandoffRefusal::Completion)?;
        if self
            .control_completion_observed_with_receipt(&completed)
            .is_err()
        {
            std::process::abort();
        }
        Ok(())
    }
    fn prepare_context_route(
        &mut self,
        context: QueryContextRef,
    ) -> Result<Option<PreparedEstablishRoute>, ControlRequestError> {
        let Some((&order, &input)) = self
            .resources
            .route_waiters
            .get(&context)
            .and_then(|rows| rows.first_key_value())
        else {
            self.resources.ready_routes.forget(context);
            return Ok(None);
        };
        let Some(address) = self.service.context(context) else {
            // Await actual publication. No poller waits for a Context mutex.
            self.resources.ready_routes.forget(context);
            return Ok(None);
        };
        let custody = self
            .resources
            .requests
            .get_mut(&input)
            .expect("route address retains its original R owner");
        assert_eq!(custody.pending_route(), Some((context, order)));
        let transfer = custody
            .prepare_pending_establish_route(address, &self.resources.roles[FAILURE].work)?;
        if let Some(role) = custody.route_return_role() {
            assert!(self.resources.return_routes.insert(role, input).is_none());
        }
        let rows = self
            .resources
            .route_waiters
            .get_mut(&context)
            .expect("original route address");
        rows.remove(&order);
        if rows.is_empty() {
            self.resources.route_waiters.remove(&context);
            self.resources.ready_routes.forget(context);
        } else {
            self.resources.ready_routes.mark(context);
        }
        Ok(Some(transfer))
    }
    /// Take one original neutral transfer. Its Root return is registered
    /// before the caller releases Root; binding never borrows this owner.
    pub(crate) fn take_ready_route_binding(
        &mut self,
    ) -> Result<Option<PreparedEstablishRoute>, ControlRequestError> {
        let Some(context) = self.resources.ready_routes.pop() else {
            return Ok(None);
        };
        self.prepare_context_route(context)
    }
    pub(crate) fn adopt_context(
        &mut self,
        ticket: &EffectTicket<super::actors::WorkerAction>,
        binding: ContextHostBinding,
    ) -> Result<(), ServiceHostError> {
        let context = binding.core.identity();
        self.service.adopt_context(ticket, binding)?;
        if self.resources.route_waiters.contains_key(&context) {
            self.resources.ready_routes.mark(context);
            self.runtime()
                .notify_host_actor(&self.registration)
                .unwrap_or_else(|_| std::process::abort());
        }
        Ok(())
    }
    pub(crate) fn handoff_establish_route_cancel(
        &mut self,
        ticket: EffectTicket<super::actors::WorkerAction>,
    ) -> Result<(), ControlRouteHandoffRefusal> {
        use super::actors::WorkerAction;
        use novarocks_worker_protocol::control::ControlEffect;
        let WorkerAction::Control {
            effect: ControlEffect::CancelEstablishRoute { request, .. },
            ..
        } = &ticket.effect
        else {
            return Err(ControlRouteHandoffRefusal::Rejected {
                error: ControlRequestError::WrongEffect,
                ticket,
            });
        };
        let Some(custody) = self.resources.requests.get_mut(&request.token) else {
            return Err(ControlRouteHandoffRefusal::Rejected {
                error: ControlRequestError::WrongRequest,
                ticket,
            });
        };
        let pending = custody.pending_route();
        if let Err(error) = custody.cancel_establish_route(&ticket) {
            return Err(ControlRouteHandoffRefusal::Rejected { error, ticket });
        }
        if let Some((context, order)) = pending {
            let rows = self
                .resources
                .route_waiters
                .get_mut(&context)
                .expect("cancel retains original route address");
            assert_eq!(rows.remove(&order), Some(request.token));
            if rows.is_empty() {
                self.resources.route_waiters.remove(&context);
                self.resources.ready_routes.forget(context);
            }
        }
        let completed = self
            .runtime()
            .effect_completed_with_receipt(ticket)
            .map_err(ControlRouteHandoffRefusal::Completion)?;
        if self
            .control_completion_observed_with_receipt(&completed)
            .is_err()
        {
            std::process::abort();
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn take_retired_route_disposal(
        &self,
        request: InputId,
    ) -> Option<super::establish_ingress::EstablishDisposalJob> {
        self.resources
            .requests
            .get(&request)?
            .take_retired_route_disposal()
    }
    /// Drain only actual ready addresses. False retirement waits for an
    /// original completion/delivery wake; no request thread scans the table.
    #[cfg(test)]
    pub(crate) fn reap_ready_requests(
        &mut self,
        budget: usize,
    ) -> Result<usize, ControlRequestError> {
        let mut retired = 0;
        for _ in 0..budget {
            let Some(input) = self.resources.request_ready.next_ready() else {
                break;
            };
            let Some(custody) = self.resources.requests.get_mut(&input) else {
                std::process::abort();
            };
            if custody.try_retire()? {
                if let Some(role) = custody.route_return_role() {
                    assert_eq!(self.resources.return_routes.remove(&role), Some(input));
                }
                self.remove_retired_request(input);
                retired += 1;
            }
        }
        Ok(retired)
    }
    pub(crate) fn index_control_effects(
        &mut self,
        budget: usize,
    ) -> Result<IndexedBatches, DispatchError> {
        let runtime = self.runtime().clone();
        let indexed = self.resources.dispatch.index_committed(&runtime, budget)?;
        for input in &indexed.inputs {
            self.note_control_input_progress(*input);
        }
        Ok(indexed)
    }
    pub(crate) fn take_control_effect(
        &mut self,
        budget: usize,
    ) -> Result<Option<EffectTicket<super::actors::WorkerAction>>, DispatchError> {
        let runtime = self.runtime().clone();
        self.resources.dispatch.take_ready(&runtime, budget)
    }
    pub(crate) fn publish_request_reply(
        &mut self,
        ticket: EffectTicket<super::actors::WorkerAction>,
    ) -> Result<(), ControlRequestPublishRefusal> {
        let token = match &ticket.effect {
            super::actors::WorkerAction::ControlReply(reply) => reply.request.token,
            _ => {
                return Err(ControlRequestPublishRefusal::Rejected {
                    error: ControlRequestError::WrongEffect,
                    ticket,
                });
            }
        };
        let Some(custody) = self.resources.requests.get_mut(&token) else {
            return Err(ControlRequestPublishRefusal::Rejected {
                error: ControlRequestError::WrongRequest,
                ticket,
            });
        };
        if let Err(error) = custody.stage_reply_disposal(&ticket) {
            return Err(ControlRequestPublishRefusal::Rejected { error, ticket });
        }
        let completed = custody.publish_with_receipt(ticket)?;
        // The original effect has actually completed. Missing its own root
        // ordering entry is a shared invariant, not a retriable RPC refusal.
        if self
            .control_completion_observed_with_receipt(&completed)
            .is_err()
        {
            std::process::abort();
        }
        Ok(())
    }
    pub(crate) fn control_publication_committed(
        &mut self,
        ticket: &EffectTicket<super::actors::WorkerAction>,
    ) -> Result<(), DispatchError> {
        let runtime = self.runtime().clone();
        self.resources
            .dispatch
            .publication_committed(&runtime, ticket)
    }
    #[cfg(test)]
    pub(crate) fn control_completion_observed(
        &mut self,
        effect: EffectRef,
    ) -> Result<(), DispatchError> {
        self.resources.dispatch.completion_observed(effect)?;
        self.note_control_input_progress(effect.input);
        Ok(())
    }
    pub(crate) fn control_completion_observed_with_receipt(
        &mut self,
        completed: &CompletedEffect<WorkerActor>,
    ) -> Result<(), DispatchError> {
        self.resources
            .dispatch
            .completion_observed_with_receipt(completed)?;
        self.note_control_input_progress(completed.input());
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn take_protocol_observation_for_test(
        &mut self,
        input: InputId,
        ports: &crate::TaskExecutionPorts,
    ) -> Result<Option<super::request_observation::RequestObservationJob>, ControlRequestError>
    {
        Ok(self
            .resources
            .requests
            .get_mut(&input)
            .ok_or(ControlRequestError::WrongRequest)?
            .take_protocol_observation_for_test(ports))
    }
    pub(crate) fn note_control_input_progress(&self, input: InputId) {
        if let Some(custody) = self.resources.requests.get(&input) {
            custody.note_progress();
            return;
        }
        // Look up the real still-resident input header, not a guessed token
        // or a scan of requests. Absence supplies no retirement/exit fact.
        if let Ok(Some(header)) = self.runtime().input_custody(self.actor, input)
            && let Some(role) = header.role
            && let Some(original) = self.resources.return_routes.get(&role)
            && let Some(custody) = self.resources.requests.get(original)
        {
            custody.note_progress();
        }
    }
    pub(crate) fn service(&mut self) -> &mut WorkerService {
        &mut self.service
    }
    pub(crate) fn resources(&self) -> &ProcessControlResources {
        &self.resources
    }
    /// Move the whole original inventory into its explicit process drain owner.
    /// This proves no publication, holder exit, report or physical executor drain.
    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (WorkerService, ProcessControlResources) {
        (self.service, self.resources)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_admission::Dimension;
    use novarocks_worker_protocol::admission::AdmissionTicketConfig;
    use novarocks_worker_protocol::{control::ControlConfig, lease::MonotonicInstant};
    use std::time::Duration;

    fn generation() -> Generation {
        Generation::new(1).unwrap()
    }
    fn actor() -> ActorId {
        ActorId::new(1).unwrap()
    }
    fn core() -> ControlCore {
        ControlCore::new(
            ControlConfig {
                max_pending_requests: 1,
                step_budget: ControlCore::MAX_STEP_WORK,
            },
            AdmissionTicketConfig::new(Duration::from_secs(5), Duration::from_secs(10)).unwrap(),
            [9; 8],
            MonotonicInstant::ORIGIN,
        )
        .unwrap()
    }
    fn binding() -> ProcessControlBinding {
        ProcessControlBinding {
            actor: actor(),
            generation: generation(),
            request_positions: NonZeroUsize::new(1).unwrap(),
            actor_config: ActorConfig {
                mailbox: [1; 4],
                max_batches: 4,
                max_effects_per_step:
                    crate::control_runtime::actors::ControlActor::MAX_STEP_ACTIONS,
                max_dependencies_per_effect:
                    crate::control_runtime::actors::ControlActor::MAX_STEP_ACTIONS,
                turn_budget: crate::control_runtime::actors::ControlActor::TURN_BUDGET,
                max_step_work: crate::control_runtime::actors::ControlActor::MAX_STEP_WORK,
            },
            // This bootstrap submits no request, ticket or Context effect.
            // It cannot claim any such unpaid capability from these vectors.
            admission_storage: AdmissionStorage {
                loading: Default::default(),
                accepted: Default::default(),
                context: Default::default(),
            },
        }
    }
    fn finish(
        ledger: &RuntimeLedger,
        runtime: &ActorRuntime<WorkerActor>,
        mut resources: ProcessControlResources,
    ) {
        resources.scope.close_external().unwrap();
        assert_eq!(resources.request_ready.registered(), 0);
        assert!(resources.requests.is_empty());
        assert!(resources.dispatch.is_empty());
        assert!(resources.internal_partial.is_none());
        if let Some(internal) = &mut resources.internal {
            internal.close_after_drain().unwrap();
        }
        ledger.reap_scope_ready(resources.scope.id(), 4).unwrap();
        if resources.registered {
            runtime.close_actor_admission(actor()).unwrap();
            while let Some(original_host_turn) = runtime.claim_host_actor() {
                original_host_turn.release(false).unwrap();
            }
            runtime.seal_actor(actor()).unwrap();
        }
        // Sealing first cancels the actual timer source and forbids any new
        // input. These pure metadata owners then end; their payer remains live
        // until the real actor attachment has detached as well.
        for role in resources.roles.iter_mut().rev() {
            role.work.body_exited().unwrap();
            role.work.captures_exited().unwrap();
        }
        ledger
            .reap_scope_ready(resources.scope.id(), KINDS.len())
            .unwrap();
        assert_eq!(resources.scope.snapshot().unwrap().work, 0);
        if resources.registered {
            match runtime.remove_actor(actor()) {
                Ok(pure_core) => drop(pure_core),
                Err(error) => panic!("actual empty Control registration must retire: {error:?}"),
            }
            resources.registered = false;
        }
        for role in resources.roles.iter_mut().rev() {
            role.capacity
                .release_quota(&role.capacity.resources().unwrap().quota)
                .unwrap();
            role.capacity.finish().unwrap();
        }
        ledger.remove_scope(resources.scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
    #[test]
    fn original_nonzero_root_bundle_owns_the_actual_control_registration_and_roles() {
        let paid = PROCESS_CONTROL_BOOTSTRAP_STORAGE.resources().unwrap();
        let ledger = RuntimeLedger::new(paid.quota.clone());
        let runtime = ActorRuntime::new();
        let mut owner =
            ProcessControlOwner::bootstrap(&runtime, &ledger, core(), binding()).unwrap();
        assert_eq!(owner.actor(), actor());
        assert!(!owner.service().runtime().snapshot(actor()).unwrap().failed);
        assert_eq!(owner.resources().roles.len(), KINDS.len());
        assert_eq!(
            owner.resources().scope.snapshot().unwrap().work,
            KINDS.len()
        );
        assert_eq!(owner.resources().scope.snapshot().unwrap().storage, 1);
        for (dimension, count) in paid.quota.iter() {
            assert_eq!(ledger.used(dimension), count);
        }
        let (service, resources) = owner.into_parts();
        drop(service);
        finish(&ledger, &runtime, resources);
    }
    #[test]
    fn the_paid_internal_wake_completes_before_reusing_its_own_input_role() {
        use crate::control_runtime::{
            actors::{ControlMessage, WorkerAction, WorkerMessage},
            control_internal::{CONTROL_INTERNAL_STORAGE, InternalAdvance},
        };
        use novarocks_control_runtime::driver::TimerSourceState;
        use novarocks_control_runtime_core::{identity::Tick, mailbox::InputClass};
        use novarocks_types::FrontendProcessId;
        use novarocks_worker_protocol::control::ControlEffect;

        let seed_storage = StorageNeed {
            inputs: 1,
            batches: 1,
            effects: ControlCore::MAX_STEP_WORK,
            work: 1,
            ..StorageNeed::default()
        };
        let limits = PROCESS_CONTROL_BOOTSTRAP_STORAGE
            .checked_add(CONTROL_INTERNAL_STORAGE)
            .unwrap()
            .checked_add(seed_storage)
            .unwrap()
            .resources()
            .unwrap();
        let ledger = RuntimeLedger::new(limits.quota);
        let runtime = ActorRuntime::new();
        let config = binding();
        let mut owner = ProcessControlOwner::bootstrap(&runtime, &ledger, core(), config).unwrap();
        owner.enable_internal_source().unwrap();
        assert_eq!(owner.advance_internal().unwrap(), InternalAdvance::Idle);
        let (mut seed_work, mut seed_capacity) = owner
            .resources()
            .scope
            .reserve_next(
                generation(),
                seed_storage.resources().unwrap(),
                WorkKind::Step,
                WorkAdmission::Derived(owner.lifetime_work()),
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        seed_work.start().unwrap();
        let mut seed_role = runtime
            .reserve_input(actor(), actor(), InputClass::Lifecycle)
            .unwrap()
            .into_role()
            .unwrap();
        for _ in 0..2 {
            let input = seed_role
                .fill(WorkerMessage::Control(ControlMessage::Command {
                    work: seed_work.reference(),
                    request: None,
                    command: ControlInput::FenceFrontend {
                        frontend: FrontendProcessId::new_v7(),
                    },
                }))
                .unwrap();
            assert!(runtime.poll_one(Tick(1)).unwrap());
            owner.index_control_effects(64).unwrap();
            let mut wake = None;
            for _ in 0..crate::control_runtime::actors::ControlActor::MAX_STEP_ACTIONS {
                let ticket = owner.take_control_effect(64).unwrap().unwrap();
                if let WorkerAction::AdmissionEpoch(projection) = &ticket.effect {
                    // Commit the actual preceding admission projection before
                    // handling the wake from this same fenced Root step.
                    runtime
                        .commit_snapshot(
                            &ticket,
                            &owner.resources().admission_snapshot,
                            std::sync::Arc::new(*projection),
                        )
                        .unwrap_or_else(|_| panic!("original admission projection commits"));
                    let reference = EffectRef {
                        input: ticket.input,
                        action: ticket.descriptor.id,
                    };
                    runtime.effect_completed(ticket).unwrap();
                    owner.control_completion_observed(reference).unwrap();
                } else {
                    wake = Some(ticket);
                    break;
                }
            }
            let wake = wake.expect("the same bounded Root step publishes its internal wake");
            assert!(matches!(
                &wake.effect,
                WorkerAction::Control {
                    effect: ControlEffect::WakeInternal,
                    admitted: None
                }
            ));
            owner.handoff_internal_wake(wake).unwrap();
            runtime.retire_input(actor(), input).unwrap();
        }
        seed_work.body_exited().unwrap();
        seed_work.captures_exited().unwrap();
        seed_role.close_after_exit().unwrap();
        seed_capacity
            .release_quota(&seed_capacity.resources().unwrap().quota)
            .unwrap();
        seed_capacity.finish().unwrap();
        ledger
            .reap_scope_ready(owner.resources().scope.id(), 5)
            .unwrap();
        let first = match owner.advance_internal().unwrap() {
            InternalAdvance::Submitted(input) => input,
            other => panic!("paid pending wake must fill its own idle role: {other:?}"),
        };
        assert_eq!(
            owner.advance_internal().unwrap(),
            InternalAdvance::Waiting(first)
        );
        assert!(runtime.poll_one(Tick(2)).unwrap());
        owner.index_control_effects(64).unwrap();
        let wake = owner.take_control_effect(64).unwrap().unwrap();
        assert_eq!(wake.input, first);
        owner.accept_internal_wake(&wake).unwrap();
        assert_eq!(
            owner.advance_internal().unwrap(),
            InternalAdvance::Waiting(first)
        );
        let reference = EffectRef {
            input: wake.input,
            action: wake.descriptor.id,
        };
        runtime.effect_completed(wake).unwrap();
        owner.control_completion_observed(reference).unwrap();
        let second = match owner.advance_internal().unwrap() {
            InternalAdvance::Submitted(input) => input,
            other => panic!("the original wake effect must not pin its own next input: {other:?}"),
        };
        assert_ne!(first, second);
        assert!(runtime.poll_one(Tick(3)).unwrap());
        owner.index_control_effects(64).unwrap();
        assert!(owner.take_control_effect(64).unwrap().is_none());
        assert_eq!(owner.advance_internal().unwrap(), InternalAdvance::Idle);
        assert_eq!(owner.resources().scope.snapshot().unwrap().work, 4);
        assert_eq!(runtime.snapshot(actor()).unwrap().roles, 1);
        assert!(
            owner.resources().internal.is_some()
                || owner
                    .resources()
                    .internal_partial
                    .as_ref()
                    .is_some_and(|partial| partial
                        .capacity
                        .resources()
                        .unwrap()
                        .owned_slots
                        .captures
                        == 1)
        );
        assert!(matches!(
            owner
                .resources
                .internal
                .as_mut()
                .unwrap()
                .close_after_drain(),
            Err(ControlInternalError::TimerStillOpen)
        ));
        assert_eq!(
            runtime.close_timer_source(actor()).unwrap(),
            TimerSourceState::ClosedDrained
        );
        owner
            .resources
            .internal
            .as_mut()
            .unwrap()
            .close_after_drain()
            .unwrap();
        ledger
            .reap_scope_ready(owner.resources().scope.id(), 4)
            .unwrap();
        assert_eq!(owner.resources().scope.snapshot().unwrap().work, 3);
        assert!(owner.resources().internal_partial.is_none());
        let (service, resources) = owner.into_parts();
        drop(service);
        finish(&ledger, &runtime, resources);
    }
    #[test]
    fn internal_input_refusal_retains_the_original_nonzero_unused_work() {
        use crate::control_runtime::control_internal::CONTROL_INTERNAL_STORAGE;
        use novarocks_control_runtime_core::mailbox::InputClass;
        let config = binding();
        let control_positions = config.actor_config.mailbox[InputClass::Control.index()]
            + config.request_positions.get();
        let unused = StorageNeed {
            inputs: control_positions,
            work: 1,
            ..StorageNeed::default()
        };
        let limits = PROCESS_CONTROL_BOOTSTRAP_STORAGE
            .checked_add(CONTROL_INTERNAL_STORAGE)
            .unwrap()
            .checked_add(unused)
            .unwrap()
            .resources()
            .unwrap();
        let ledger = RuntimeLedger::new(limits.quota);
        let runtime = ActorRuntime::new();
        let mut owner = ProcessControlOwner::bootstrap(&runtime, &ledger, core(), config).unwrap();
        let (mut occupier, mut capacity) = owner
            .resources()
            .scope
            .reserve_next(
                generation(),
                unused.resources().unwrap(),
                WorkKind::Step,
                WorkAdmission::Derived(owner.lifetime_work()),
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        let reserved: Vec<_> = (0..control_positions)
            .map(|_| {
                runtime
                    .reserve_input(actor(), actor(), InputClass::Control)
                    .unwrap()
            })
            .collect();
        assert!(matches!(
            owner.enable_internal_source(),
            Err(ControlInternalError::Driver(DriverError::InputCapacity))
        ));
        assert!(owner.resources().internal.is_none());
        assert!(owner.resources().internal_partial.is_some());
        assert_eq!(owner.resources().scope.snapshot().unwrap().work, 5);
        assert!(
            owner.resources().internal.is_some()
                || owner
                    .resources()
                    .internal_partial
                    .as_ref()
                    .is_some_and(|partial| partial
                        .capacity
                        .resources()
                        .unwrap()
                        .owned_slots
                        .captures
                        == 1)
        );
        assert!(matches!(
            owner.enable_internal_source(),
            Err(ControlInternalError::AlreadyAdmitted)
        ));
        let mut partial = owner.resources.internal_partial.take().unwrap();
        assert!(partial.permit.is_none());
        assert!(partial.role.is_none());
        // The refused source never captured a command. These exact bound
        // metadata holders end unused; no input consumption is fabricated.
        partial.work.body_exited().unwrap();
        partial.work.captures_exited().unwrap();
        partial
            .capacity
            .release_quota(&partial.capacity.resources().unwrap().quota)
            .and_then(|()| partial.capacity.finish())
            .unwrap();
        for permit in reserved {
            permit.revoke_unused().unwrap();
        }
        occupier.body_exited().unwrap();
        occupier.captures_exited().unwrap();
        capacity
            .release_quota(&capacity.resources().unwrap().quota)
            .unwrap();
        capacity.finish().unwrap();
        ledger
            .reap_scope_ready(owner.resources().scope.id(), 4)
            .unwrap();
        assert_eq!(owner.resources().scope.snapshot().unwrap().work, 3);
        assert!(owner.resources().internal_partial.is_none());
        drop((partial, occupier, capacity));
        let (service, resources) = owner.into_parts();
        drop(service);
        finish(&ledger, &runtime, resources);
    }
    #[test]
    fn ticket_replay_and_expiry_preserve_the_original_receipt_without_work_holders() {
        exercise_ticket_replay_and_expiry(false);
    }
    #[test]
    fn later_expiry_publication_cannot_overtake_the_original_ticket_reply_body() {
        exercise_ticket_replay_and_expiry(true);
    }
    fn exercise_ticket_replay_and_expiry(expire_before_reply_body: bool) {
        use crate::control_runtime::{
            actors::WorkerAction,
            control_request::{CONTROL_REQUEST_STORAGE, ControlReplyBody},
            effect_dispatch::EffectRef,
        };
        use novarocks_control_runtime_core::identity::Tick;
        use novarocks_execution_contract::{
            AcquireQueryContextAdmissionTicket, AdmissionTicketId, LeaseValidFor, QueryContextRef,
            TaskOperationId,
        };
        use novarocks_types::{
            AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId,
            QueryExecutionId, QueryId,
        };
        use novarocks_worker_protocol::control::{ControlEffect, ControlInput};

        // Only the actual process and one external R own Work/storage.
        // Ticket history is Core metadata and holds no extra payer.
        let limits = PROCESS_CONTROL_BOOTSTRAP_STORAGE
            .checked_add(CONTROL_REQUEST_STORAGE)
            .unwrap()
            .resources()
            .unwrap();
        let ledger = RuntimeLedger::new(limits.quota);
        let runtime = ActorRuntime::new();
        let core = core();
        let capability = core.admission().capability();
        let mut owner = ProcessControlOwner::bootstrap(&runtime, &ledger, core, binding()).unwrap();
        fn complete_marker(
            owner: &mut ProcessControlOwner,
            runtime: &ActorRuntime<WorkerActor>,
            marker: EffectTicket<WorkerAction>,
        ) {
            if let WorkerAction::AdmissionEpoch(projection) = &marker.effect {
                runtime
                    .commit_snapshot(
                        &marker,
                        &owner.resources().admission_snapshot,
                        std::sync::Arc::new(*projection),
                    )
                    .unwrap_or_else(|_| panic!("original admission projection commits"));
            }
            let reference = EffectRef {
                input: marker.input,
                action: marker.descriptor.id,
            };
            runtime.effect_completed(marker).unwrap();
            owner.control_completion_observed(reference).unwrap();
        }
        let mut advanced_expiry = None;
        let context = QueryContextRef::new(
            QueryExecutionId::new(QueryId::new(7101, 7102), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let mut bytes = [17; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        let operation = TaskOperationId::try_from_bytes(bytes).unwrap();
        let mut original = None;
        for ordinal in 1..=if expire_before_reply_body { 1 } else { 2 } {
            let (input, mut external) = owner
                .admit_request(
                    ActorId::new(2).unwrap(),
                    operation,
                    MonotonicInstant::from_origin(Duration::from_secs(10)),
                )
                .unwrap();
            owner
                .submit_request(
                    input,
                    ControlInput::Acquire {
                        request: AcquireQueryContextAdmissionTicket::new(
                            operation,
                            context,
                            LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
                            NativeCompatibilityId::new([3; 32]),
                            capability,
                        ),
                        // A new issuer candidate is not immutable request semantics;
                        // the second RPC must receive the first receipt.
                        ticket: AdmissionTicketId::try_from_bytes([ordinal; 16]).unwrap(),
                    },
                )
                .unwrap();
            assert!(runtime.poll_one(Tick(ordinal.into())).unwrap());
            owner
                .index_control_effects(ControlCore::MAX_STEP_WORK)
                .unwrap();
            let mut body = owner
                .take_control_effect(ControlCore::MAX_STEP_WORK)
                .unwrap()
                .unwrap();
            assert_eq!(body.input, input);
            if matches!(body.effect, WorkerAction::AdmissionEpoch(_)) {
                complete_marker(&mut owner, &runtime, body);
                assert_eq!(
                    owner
                        .resources()
                        .admission_snapshot
                        .borrow()
                        .logical_live_grants,
                    1
                );
                body = owner
                    .take_control_effect(ControlCore::MAX_STEP_WORK)
                    .unwrap()
                    .unwrap();
            }
            assert_eq!(body.input, input);
            let receipt = match &body.effect {
                WorkerAction::Control {
                    effect:
                        ControlEffect::AdmissionReply {
                            result: Ok(receipt),
                            ..
                        },
                    admitted: None,
                } => *receipt,
                other => panic!("the original metadata-only ticket grant: {other:?}"),
            };
            assert_eq!(*original.get_or_insert(receipt), receipt);
            if expire_before_reply_body {
                let expiry = runtime.next_deadline().unwrap();
                assert_eq!(runtime.advance_time(expiry, 1).unwrap(), 1);
                assert!(runtime.poll_one(expiry).unwrap());
                owner
                    .index_control_effects(ControlCore::MAX_STEP_WORK)
                    .unwrap();
                // The later pure expiry is committed. Its new logical count
                // cannot publish before the original grant/reply metadata lane
                // completes; no ticket holder or retirement Work is involved.
                let timer = runtime.input_inventory(actor(), Some(input), 1).unwrap()[0].input;
                assert_eq!(
                    runtime
                        .ready_effects(actor(), timer, ControlCore::MAX_STEP_WORK)
                        .unwrap()
                        .len(),
                    1
                );
                assert!(
                    owner
                        .take_control_effect(ControlCore::MAX_STEP_WORK)
                        .unwrap()
                        .is_none()
                );
                assert_eq!(
                    owner
                        .resources()
                        .admission_snapshot
                        .borrow()
                        .logical_live_grants,
                    1
                );
                advanced_expiry = Some(expiry);
            }
            complete_marker(&mut owner, &runtime, body);
            let reply = owner
                .take_control_effect(ControlCore::MAX_STEP_WORK)
                .unwrap()
                .unwrap();
            assert_eq!(reply.input, input);
            owner.publish_request_reply(reply).unwrap();
            assert_eq!(owner.reap_ready_requests(1).unwrap(), 0);
            let delivery = external.try_take().unwrap();
            assert_eq!(delivery.body(), &ControlReplyBody::Admission(Ok(receipt)));
            assert_eq!(owner.reap_ready_requests(1).unwrap(), 0);
            assert_eq!(owner.resources().requests.len(), 1);
            drop(delivery);
            assert_eq!(owner.reap_ready_requests(1).unwrap(), 1);
            assert!(owner.resources().requests.is_empty());
            drop(external);
            ledger
                .reap_scope_ready(owner.resources().scope.id(), 5)
                .unwrap();
            assert_eq!(owner.request_ready().registered(), 0);
            assert_eq!(ledger.used(Dimension::Requests.id()), 0);
            assert_eq!(
                owner.resources().scope.snapshot().unwrap().work,
                3,
                "only original process roles remain after this actual R exits"
            );
        }
        if advanced_expiry.is_none() {
            let expiry = runtime.next_deadline().unwrap();
            assert_eq!(runtime.advance_time(expiry, 1).unwrap(), 1);
            assert!(runtime.poll_one(expiry).unwrap());
            owner
                .index_control_effects(ControlCore::MAX_STEP_WORK)
                .unwrap();
        }
        let inputs = runtime.input_inventory(actor(), None, 2).unwrap();
        assert_eq!(inputs.len(), 1);
        let timer = inputs[0].input;
        let expired = owner
            .take_control_effect(ControlCore::MAX_STEP_WORK)
            .unwrap()
            .unwrap();
        assert_eq!(expired.input, timer);
        assert!(
            matches!(expired.effect, WorkerAction::AdmissionEpoch(projection) if projection.logical_live_grants == 0)
        );
        complete_marker(&mut owner, &runtime, expired);
        assert_eq!(
            owner
                .resources()
                .admission_snapshot
                .borrow()
                .logical_live_grants,
            0
        );
        assert!(
            owner
                .take_control_effect(ControlCore::MAX_STEP_WORK)
                .unwrap()
                .is_none(),
            "ticket expiry has no retirement reservation or Work effect"
        );
        runtime.retire_input(actor(), timer).unwrap();
        let before_forget = *owner.resources().admission_snapshot.borrow();
        assert_eq!(before_forget.admission_retention.ticket_records, 1);
        assert_eq!(before_forget.admission_retention.replay_decisions, 1);
        let horizon = runtime.next_deadline().unwrap();
        assert_eq!(runtime.advance_time(horizon, 1).unwrap(), 1);
        assert!(runtime.poll_one(horizon).unwrap());
        owner
            .index_control_effects(ControlCore::MAX_STEP_WORK)
            .unwrap();
        let forgotten = owner
            .take_control_effect(ControlCore::MAX_STEP_WORK)
            .unwrap()
            .unwrap();
        let horizon_input = forgotten.input;
        let WorkerAction::AdmissionEpoch(projection) = &forgotten.effect else {
            panic!("the original replay-horizon inventory publication");
        };
        assert_eq!(projection.epoch, before_forget.epoch);
        assert_eq!(
            projection.logical_live_grants,
            before_forget.logical_live_grants
        );
        assert_eq!(projection.admission_retention.ticket_records, 0);
        assert_eq!(projection.admission_retention.acquisition_decisions, 0);
        assert_eq!(projection.admission_retention.replay_decisions, 0);
        complete_marker(&mut owner, &runtime, forgotten);
        assert_eq!(
            owner
                .resources()
                .admission_snapshot
                .borrow()
                .admission_retention
                .ticket_records,
            0
        );
        assert!(
            owner
                .take_control_effect(ControlCore::MAX_STEP_WORK)
                .unwrap()
                .is_none()
        );
        runtime.retire_input(actor(), horizon_input).unwrap();
        ledger
            .reap_scope_ready(owner.resources().scope.id(), 5)
            .unwrap();
        assert_eq!(owner.resources().scope.snapshot().unwrap().work, 3);
        assert!(owner.resources().dispatch.is_empty());
        let (service, resources) = owner.into_parts();
        drop(service);
        finish(&ledger, &runtime, resources);
    }
    #[test]
    fn closed_runtime_registration_returns_the_same_complete_bootstrap_inventory() {
        let ledger =
            RuntimeLedger::new(PROCESS_CONTROL_BOOTSTRAP_STORAGE.resources().unwrap().quota);
        let runtime = ActorRuntime::new();
        runtime.close_admission();
        let refusal = ProcessControlOwner::bootstrap(&runtime, &ledger, core(), binding())
            .err()
            .unwrap();
        assert!(matches!(
            refusal.error,
            ProcessBootstrapError::Driver(DriverError::Closed)
        ));
        let resources = refusal.resources.unwrap();
        assert!(!resources.registered);
        assert_eq!(resources.roles.len(), KINDS.len());
        assert_eq!(resources.scope.snapshot().unwrap().storage, 0);
        assert_eq!(resources.scope.snapshot().unwrap().work, KINDS.len());
        finish(&ledger, &runtime, resources);
    }
    #[test]
    fn invalid_dependency_geometry_is_rejected_before_any_root_scope_or_payer_exists() {
        let ledger =
            RuntimeLedger::new(PROCESS_CONTROL_BOOTSTRAP_STORAGE.resources().unwrap().quota);
        let runtime = ActorRuntime::new();
        let mut invalid = binding();
        invalid.actor_config.max_dependencies_per_effect = 0;
        let refusal = ProcessControlOwner::bootstrap(&runtime, &ledger, core(), invalid)
            .err()
            .unwrap();
        assert!(matches!(
            refusal.error,
            ProcessBootstrapError::InvalidGeometry
        ));
        assert!(refusal.resources.is_none());
        assert!(ledger.is_empty());
        assert!(matches!(
            runtime.snapshot(actor()),
            Err(DriverError::UnknownActor)
        ));
    }
    #[cfg(unix)]
    #[test]
    fn an_actual_control_core_panic_uses_the_process_policy_with_a_separately_paid_input() {
        use super::super::actors::{ControlMessage, WorkerMessage};
        use novarocks_control_runtime_core::{identity::Tick, mailbox::InputClass};
        use novarocks_execution_contract::QueryContextRef;
        use novarocks_types::{
            AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
        };
        use novarocks_worker_protocol::control::ControlInput;
        use std::os::unix::process::ExitStatusExt;
        const CHILD: &str = "NOVAROCKS_PROCESS_CONTROL_PANIC_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let request_need = StorageNeed {
                inputs: 1,
                batches: 1,
                effects: ControlCore::MAX_STEP_WORK,
                work: 1,
                ..StorageNeed::default()
            };
            let all = PROCESS_CONTROL_BOOTSTRAP_STORAGE
                .checked_add(request_need)
                .unwrap();
            let ledger = RuntimeLedger::new(all.resources().unwrap().quota);
            let runtime = ActorRuntime::new();
            let with_request = binding();
            let owner =
                ProcessControlOwner::bootstrap(&runtime, &ledger, core(), with_request).unwrap();
            let scope = &owner.resources().scope;
            let (mut request, _capacity) = scope
                .reserve_next(
                    generation(),
                    request_need.resources().unwrap(),
                    WorkKind::Step,
                    WorkAdmission::Derived(owner.resources().roles[LIFETIME].work.id()),
                )
                .unwrap()
                .bind(WorkKind::Step)
                .unwrap();
            request.start().unwrap();
            let unknown = QueryContextRef::new(
                QueryExecutionId::new(QueryId::new(9901, 9902), AttemptId::new(1).unwrap())
                    .unwrap(),
                FrontendProcessId::new_v7(),
                BackendProcessId::new_v7(),
            );
            runtime
                .reserve_input(actor(), ActorId::new(2).unwrap(), InputClass::Ordinary)
                .unwrap()
                .fill(WorkerMessage::Control(ControlMessage::Command {
                    work: request.reference(),
                    request: None,
                    // An impossible internal host report models a Core bug.
                    // It is not a supported I/O refusal or a query failure.
                    command: ControlInput::ExecutionRetired {
                        context: unknown,
                        retirement: novarocks_worker_protocol::retention::ContextRetirementIdentity::Protocol {
                            close_epoch: generation(),
                        },
                    },
                }))
                .unwrap();
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runtime.poll_one(Tick(1))
            })) {
                // Exit without dropping live fault custody. A secondary Drop
                // panic must not impersonate the intended fail-stop signal.
                Ok(_) => std::process::exit(75),
                Err(payload) => {
                    std::mem::forget(payload);
                    std::process::exit(77);
                }
            }
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("control_runtime::process_control::tests::an_actual_control_core_panic_uses_the_process_policy_with_a_separately_paid_input")
            .arg("--nocapture")
            .env(CHILD, "1").env("RUST_BACKTRACE", "0").output().unwrap();
        assert_eq!(
            output.status.signal(),
            Some(6),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
