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

use super::{
    actors::{ControlActor, WorkerActor},
    context_host::{ContextHostBinding, ContextMetadataRole},
    context_host_jobs::ContextHostJobError,
    context_lifecycle::CONTEXT_LIFECYCLE_INPUTS,
    control_internal::{
        ControlInternal, ControlInternalError, ControlInternalPartial, InternalAdvance,
    },
    control_request::{
        ControlRequestAdmissionRefusal, ControlRequestCustody, ControlRequestError,
        ControlRequestHandle, ControlRequestPublishRefusal, ControlRequestReady,
        ControlRequestSubmitRefusal,
    },
    control_ticket::{ControlTicketError, ControlTicketInventory},
    effect_dispatch::{DispatchError, EffectDispatch, EffectRef, IndexedBatches},
    service::{ServiceHostError, WorkerService},
};
use crate::{control_admission::AdmissionStorage, control_runtime_policy::StorageNeed};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorFailurePolicy},
    driver::{ActorRuntime, DriverError, EffectCompletionRefusal, EffectTicket, InputRoleId},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, Generation, InputId},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{
    QueryContextAdmissionTicketReceipt, QueryContextRef, TaskOperationId,
};
use novarocks_worker_protocol::{
    control::{ControlCore, ControlInput},
    lease::MonotonicInstant,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
};

const KINDS: [WorkKind; 3] = [WorkKind::Business, WorkKind::Step, WorkKind::Cleanup];
const LIFETIME: usize = 0;
const TIMER: usize = 1;
const FAILURE: usize = 2;

pub(crate) const PROCESS_CONTROL_BOOTSTRAP_STORAGE: StorageNeed = StorageNeed {
    inputs: 1,
    batches: 1,
    effects: ControlCore::MAX_STEP_WORK,
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
    pub max_work: usize,
    pub max_slots: usize,
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
}
#[must_use = "A refused wake retains its original effect ticket"]
#[derive(Debug)]
pub(crate) enum InternalWakeRefusal {
    Rejected {
        error: ControlInternalError,
        ticket: EffectTicket<super::actors::WorkerAction>,
    },
    Completion(EffectCompletionRefusal<super::actors::WorkerAction>),
}
#[must_use = "A refused route handoff retains its original effect"]
#[derive(Debug)]
pub(crate) enum ControlRouteHandoffRefusal {
    Rejected {
        error: ControlRequestError,
        ticket: EffectTicket<super::actors::WorkerAction>,
    },
    Completion(EffectCompletionRefusal<super::actors::WorkerAction>),
}
#[must_use = "A refused Context cleanup handoff retains its original Root effect"]
#[derive(Debug)]
pub(crate) enum ControlContextHandoffRefusal {
    Rejected {
        error: ContextHostJobError,
        ticket: EffectTicket<super::actors::WorkerAction>,
    },
    Completion(EffectCompletionRefusal<super::actors::WorkerAction>),
}

/// Partial bootstrap returns these same holders, including a possibly empty
/// scope. No original role or paid bundle is dropped as an unused reservation.
#[must_use = "Retain the process root until every original owner actually exits"]
pub(crate) struct ProcessControlResources {
    pub scope: ScopeHandle,
    pub roles: Vec<ContextMetadataRole>,
    pub registered: bool,
    pub request_ready: ControlRequestReady,
    pub requests: BTreeMap<InputId, ControlRequestCustody>,
    pub tickets: ControlTicketInventory,
    pub dispatch: EffectDispatch,
    pub internal: Option<ControlInternal>,
    pub internal_partial: Option<ControlInternalPartial>,
    // Addresses of the original R owners, never decisions or obligations.
    // Keys follow actual metadata handoff order, not request token order.
    route_waiters: BTreeMap<QueryContextRef, BTreeMap<u64, InputId>>,
    ready_routes: BTreeSet<QueryContextRef>,
    route_cursor: Option<QueryContextRef>,
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
    directory: NonZeroUsize,
    requests: NonZeroUsize,
}
impl RegisteredControl {
    pub(super) fn into_parts(
        self,
    ) -> (
        ActorRuntime<WorkerActor>,
        RuntimeLedger,
        ActorId,
        NonZeroUsize,
        NonZeroUsize,
    ) {
        (
            self.runtime,
            self.ledger,
            self.actor,
            self.directory,
            self.requests,
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
/// Its directory capacity is read from the actual Core rather than supplied a
/// second time. The host owns the original root outside the pure actor, so a
/// failed Core cannot destroy or recreate its cleanup authority.
#[must_use = "The actual process owner must drain before its resources can retire"]
pub(crate) struct ProcessControlOwner {
    actor: ActorId,
    service: WorkerService,
    resources: ProcessControlResources,
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
        if binding.max_work < KINDS.len()
            || binding.max_slots == 0
            || binding.actor_config.max_step_work != ControlCore::MAX_STEP_WORK
            || binding.actor_config.max_effects_per_step != ControlCore::MAX_STEP_WORK
            || binding.actor_config.max_dependencies_per_effect < ControlCore::MAX_STEP_WORK
            || binding.actor_config.validate().is_err()
        {
            return Err(invalid());
        }
        let directory = NonZeroUsize::new(core.directory_capacity()).ok_or_else(invalid)?;
        let lifecycle_inputs = directory
            .get()
            .checked_mul(CONTEXT_LIFECYCLE_INPUTS)
            .ok_or_else(invalid)?;
        binding.actor_config.mailbox
            [novarocks_control_runtime_core::mailbox::InputClass::Control.index()] = binding
            .actor_config
            .mailbox[novarocks_control_runtime_core::mailbox::InputClass::Control.index()]
        .checked_add(binding.request_positions.get())
        .and_then(|n| n.checked_add(lifecycle_inputs))
        .ok_or_else(invalid)?;
        binding.actor_config.max_batches = binding
            .actor_config
            .max_batches
            .checked_add(binding.request_positions.get())
            .and_then(|n| n.checked_add(lifecycle_inputs))
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
            .open_scope_next(
                None,
                binding.generation,
                binding.max_work,
                binding.max_slots,
            )
            .map_err(|error| ProcessBootstrapRefusal {
                error: ProcessBootstrapError::Runtime(error),
                resources: None,
            })?;
        let mut resources = ProcessControlResources {
            scope: scope.clone(),
            roles: Vec::with_capacity(KINDS.len()),
            registered: false,
            request_ready: ControlRequestReady::new(binding.request_positions, runtime.clone()),
            requests: BTreeMap::new(),
            tickets: ControlTicketInventory::new(),
            dispatch: EffectDispatch::control(runtime.clone(), binding.actor, effect_positions),
            internal: None,
            internal_partial: None,
            route_waiters: BTreeMap::new(),
            ready_routes: BTreeSet::new(),
            route_cursor: None,
            next_route_order: 0,
            return_routes: BTreeMap::new(),
        };
        let admitted = (|| -> Result<(), ProcessBootstrapError> {
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
                        ResourceVector::default(),
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
                resources.roles[TIMER].work.id(),
            );
            runtime
                .register_supervised_next_with_policy(
                    binding.actor,
                    scope.clone(),
                    WorkerActor::Control(Box::new(actor)),
                    binding.actor_config,
                    &resources.roles[FAILURE].work,
                    ActorFailurePolicy::ProcessFailStop,
                )
                .map_err(|(error, _pure_actor)| ProcessBootstrapError::Driver(error))?;
            resources.registered = true;
            Ok(())
        })();
        if let Err(error) = admitted {
            return Err(ProcessBootstrapRefusal {
                error,
                resources: Some(resources),
            });
        }
        Ok(Self {
            actor: binding.actor,
            service: WorkerService::from_registered_control(RegisteredControl {
                runtime: runtime.clone(),
                ledger: ledger.clone(),
                actor: binding.actor,
                directory,
                requests: binding.request_positions,
            }),
            resources,
        })
    }
    pub(crate) fn actor(&self) -> ActorId {
        self.actor
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
        let reference = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        self.runtime()
            .effect_completed(ticket)
            .map_err(InternalWakeRefusal::Completion)?;
        if self.control_completion_observed(reference).is_err() {
            std::process::abort();
        }
        Ok(())
    }
    pub(crate) fn advance_internal(&mut self) -> Result<InternalAdvance, ControlInternalError> {
        self.resources
            .internal
            .as_mut()
            .ok_or(ControlInternalError::NotEnabled)?
            .advance()
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
        let (custody, handle) = ControlRequestCustody::admit(self, source, operation, deadline)?;
        let input = custody.input_id();
        if self.resources.requests.contains_key(&input) {
            // A duplicate actual input identity is a shared runtime invariant.
            // Preserve both original owners until process fail-stop.
            std::process::abort();
        }
        self.resources.requests.insert(input, custody);
        Ok((input, handle))
    }
    pub(crate) fn admit_establish_request(
        &mut self,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(InputId, ControlRequestHandle), ControlRequestAdmissionRefusal> {
        let (custody, handle) =
            ControlRequestCustody::admit_establish(self, source, operation, deadline)?;
        let input = custody.input_id();
        if self.resources.requests.insert(input, custody).is_some() {
            std::process::abort();
        }
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
    pub(crate) fn bind_materialization_return(
        &mut self,
        ticket: &EffectTicket<super::actors::WorkerAction>,
    ) -> Result<(), ControlRequestError> {
        let super::actors::WorkerAction::Control {
            effect: novarocks_worker_protocol::control::ControlEffect::Materialize { context, .. },
            ..
        } = &ticket.effect
        else {
            return Err(ControlRequestError::WrongEffect);
        };
        let jobs = self
            .service
            .context(*context)
            .and_then(|h| h.host_jobs())
            .cloned()
            .ok_or(ControlRequestError::WrongPhase)?;
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
    pub(crate) fn take_consumed_creator_disposal(
        &self,
        input: InputId,
    ) -> Result<Option<super::establish_ingress::EstablishDisposalJob>, ControlRequestError> {
        self.resources
            .requests
            .get(&input)
            .ok_or(ControlRequestError::WrongRequest)?
            .take_consumed_creator_disposal()
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
    pub(crate) fn submit_establish_ingress_failure(
        &mut self,
        input: InputId,
    ) -> Result<(), ControlRequestError> {
        let failure_work = self.resources.roles[FAILURE].work.id();
        self.resources
            .requests
            .get_mut(&input)
            .ok_or(ControlRequestError::WrongRequest)?
            .submit_ingress_owner_failure(failure_work)
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
    ) -> Result<Option<ControlRequestError>, ControlRouteHandoffRefusal> {
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
        let refusal = self.advance_context_route(context).err();
        let effect = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        self.runtime()
            .effect_completed(ticket)
            .map_err(ControlRouteHandoffRefusal::Completion)?;
        if self.control_completion_observed(effect).is_err() {
            std::process::abort();
        }
        Ok(refusal)
    }
    fn advance_context_route(
        &mut self,
        context: QueryContextRef,
    ) -> Result<bool, ControlRequestError> {
        let Some((&order, &input)) = self
            .resources
            .route_waiters
            .get(&context)
            .and_then(|rows| rows.first_key_value())
        else {
            self.resources.ready_routes.remove(&context);
            return Ok(false);
        };
        let Some(host) = self.service.context(context) else {
            self.resources.ready_routes.remove(&context);
            return Ok(false);
        };
        let custody = self
            .resources
            .requests
            .get_mut(&input)
            .expect("route address retains its original R owner");
        assert_eq!(custody.pending_route(), Some((context, order)));
        let initialization =
            custody.bind_pending_establish_route(host, &self.resources.roles[FAILURE].work)?;
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
            self.resources.ready_routes.remove(&context);
        } else {
            self.resources.ready_routes.insert(context);
        }
        initialization.map_err(ControlRequestError::Route)?;
        Ok(true)
    }
    /// Each unit binds one existing original R through a coalesced target
    /// index. No request thread scans Contexts or recreates an obligation.
    pub(crate) fn advance_route_bindings(
        &mut self,
        budget: usize,
    ) -> Result<usize, ControlRequestError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let mut units = 0;
        while units < budget {
            let context = self
                .resources
                .route_cursor
                .and_then(|cursor| {
                    self.resources
                        .ready_routes
                        .range((Excluded(cursor), Unbounded))
                        .next()
                        .copied()
                })
                .or_else(|| self.resources.ready_routes.first().copied());
            let Some(context) = context else {
                break;
            };
            self.resources.route_cursor = Some(context);
            self.advance_context_route(context)?;
            units += 1;
        }
        Ok(units)
    }
    pub(crate) fn adopt_context(
        &mut self,
        ticket: &EffectTicket<super::actors::WorkerAction>,
        binding: ContextHostBinding,
    ) -> Result<(), ServiceHostError> {
        let context = binding.core.identity();
        self.service.adopt_context(ticket, binding)?;
        if self.resources.route_waiters.contains_key(&context) {
            self.resources.ready_routes.insert(context);
            self.runtime().notify_pollers();
        }
        Ok(())
    }
    pub(crate) fn handoff_context_removal(
        &mut self,
        ticket: EffectTicket<super::actors::WorkerAction>,
    ) -> Result<(), ControlContextHandoffRefusal> {
        use super::actors::WorkerAction;
        use novarocks_worker_protocol::control::ControlEffect;
        let context = match &ticket.effect {
            WorkerAction::Control {
                effect: ControlEffect::RemoveHost { context },
                ..
            } if ticket.actor == self.actor => *context,
            _ => {
                return Err(ControlContextHandoffRefusal::Rejected {
                    error: ContextHostJobError::WrongOwner,
                    ticket,
                });
            }
        };
        let result = self
            .service
            .context(context)
            .and_then(|host| host.host_jobs())
            .ok_or(ContextHostJobError::WrongOwner)
            .and_then(|jobs| jobs.request_holder_disposal(&ticket));
        if let Err(error) = result {
            return Err(ControlContextHandoffRefusal::Rejected { error, ticket });
        }
        let input = ticket.input;
        self.runtime()
            .effect_completed(ticket)
            .map_err(ControlContextHandoffRefusal::Completion)?;
        // RemoveHost releases the Root metadata lane on transfer/start. This
        // completion releases the original input, never H's physical custody.
        self.note_control_input_progress(input);
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
                self.resources.ready_routes.remove(&context);
            }
        }
        let effect = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        self.runtime()
            .effect_completed(ticket)
            .map_err(ControlRouteHandoffRefusal::Completion)?;
        if self.control_completion_observed(effect).is_err() {
            std::process::abort();
        }
        Ok(())
    }
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
    pub(crate) fn reap_ready_requests(
        &mut self,
        budget: usize,
    ) -> Result<usize, ControlRequestError> {
        let mut retired = 0;
        let route_units = self.advance_route_bindings(budget)?;
        for _ in route_units..budget {
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
                self.resources.requests.remove(&input);
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
        let reference = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        custody.publish(ticket)?;
        // The original effect has actually completed. Missing its own root
        // ordering entry is a shared invariant, not a retriable RPC refusal.
        if self.control_completion_observed(reference).is_err() {
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
    pub(crate) fn control_completion_observed(
        &mut self,
        effect: EffectRef,
    ) -> Result<(), DispatchError> {
        self.resources.dispatch.completion_observed(effect)?;
        self.note_control_input_progress(effect.input);
        Ok(())
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
    pub(crate) fn adopt_ticket(
        &mut self,
        ticket: &EffectTicket<super::actors::WorkerAction>,
    ) -> Result<QueryContextAdmissionTicketReceipt, ControlTicketError> {
        let runtime = self.runtime().clone();
        self.resources.tickets.adopt(&runtime, self.actor, ticket)
    }
    pub(crate) fn retire_ticket(
        &mut self,
        ticket: &EffectTicket<super::actors::WorkerAction>,
    ) -> Result<bool, ControlTicketError> {
        let runtime = self.runtime().clone();
        self.resources.tickets.retire(&runtime, self.actor, ticket)
    }
    pub(crate) fn service(&mut self) -> &mut WorkerService {
        &mut self.service
    }
    pub(crate) fn resources(&self) -> &ProcessControlResources {
        &self.resources
    }
    /// Move the whole original inventory into its explicit process drain owner.
    /// This proves no publication, holder exit, report or physical executor drain.
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
        core_with_fence_limit(1)
    }
    fn core_with_fence_limit(gone_fences: usize) -> ControlCore {
        ControlCore::new(
            ControlConfig {
                max_contexts: 2,
                max_waiters: 1,
                max_pending_requests: 1,
                step_budget: ControlCore::MAX_STEP_WORK,
                retained_contexts: 1,
                gone_fences,
                retention: Duration::from_secs(10),
            },
            AdmissionTicketConfig::new(1, Duration::from_secs(5), Duration::from_secs(10), 1)
                .unwrap(),
            [9; 8],
            MonotonicInstant::ORIGIN,
        )
        .unwrap()
    }
    fn binding() -> ProcessControlBinding {
        ProcessControlBinding {
            actor: actor(),
            generation: generation(),
            max_work: KINDS.len(),
            max_slots: 1,
            request_positions: NonZeroUsize::new(1).unwrap(),
            actor_config: ActorConfig {
                mailbox: [1; 4],
                max_batches: 4,
                max_effects_per_step: ControlCore::MAX_STEP_WORK,
                max_dependencies_per_effect: ControlCore::MAX_STEP_WORK,
                turn_budget: 64,
                max_step_work: ControlCore::MAX_STEP_WORK,
            },
            // This bootstrap submits no request, ticket or Context effect.
            // It cannot claim any such unpaid capability from these vectors.
            admission_storage: AdmissionStorage {
                loading: ResourceVector::default(),
                accepted: ResourceVector::default(),
                context: ResourceVector::default(),
                ticket: ResourceVector::default(),
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
        assert!(resources.tickets.is_empty());
        assert!(resources.internal_partial.is_none());
        if let Some(internal) = &mut resources.internal {
            internal.close_after_drain().unwrap();
        }
        ledger.reap_scope_ready(resources.scope.id(), 4).unwrap();
        if resources.registered {
            runtime.close_actor_admission(actor()).unwrap();
            while let Some(ready) = runtime.claim_effect_actor() {
                runtime.release_effect_actor(ready, false).unwrap();
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
                .release(&role.capacity.resources().unwrap())
                .unwrap();
        }
        ledger.remove_scope(resources.scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
    #[test]
    fn original_nonzero_root_bundle_owns_the_actual_control_registration_and_roles() {
        let paid = PROCESS_CONTROL_BOOTSTRAP_STORAGE.resources().unwrap();
        let ledger = RuntimeLedger::new(paid.clone(), KINDS.len(), 1);
        let runtime = ActorRuntime::new(1);
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
        for (dimension, count) in paid.iter() {
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
        let ledger = RuntimeLedger::new(limits, 5, 1);
        let runtime = ActorRuntime::new(1);
        let mut config = binding();
        config.max_work = 5;
        let mut owner =
            ProcessControlOwner::bootstrap(&runtime, &ledger, core_with_fence_limit(2), config)
                .unwrap();
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
                    work: seed_work.id(),
                    request: None,
                    command: ControlInput::FenceFrontend {
                        frontend: FrontendProcessId::new_v7(),
                    },
                }))
                .unwrap();
            assert!(runtime.poll_one(Tick(1)).unwrap());
            owner.index_control_effects(64).unwrap();
            let wake = owner.take_control_effect(64).unwrap().unwrap();
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
            .release(&seed_capacity.resources().unwrap())
            .unwrap();
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
        assert_eq!(ledger.used(Dimension::Work.id()), 4);
        assert_eq!(ledger.used(Dimension::Inputs.id()), 2);
        assert_eq!(ledger.used(Dimension::Captures.id()), 1);
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
        assert_eq!(ledger.used(Dimension::Work.id()), 3);
        assert_eq!(ledger.used(Dimension::Captures.id()), 0);
        let (service, resources) = owner.into_parts();
        drop(service);
        finish(&ledger, &runtime, resources);
    }
    #[test]
    fn internal_input_refusal_retains_the_original_nonzero_unused_work() {
        use crate::control_runtime::control_internal::CONTROL_INTERNAL_STORAGE;
        use novarocks_control_runtime_core::mailbox::InputClass;
        let mut config = binding();
        let control_positions = config.actor_config.mailbox[InputClass::Control.index()]
            + config.request_positions.get()
            + core().directory_capacity() * CONTEXT_LIFECYCLE_INPUTS;
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
        let ledger = RuntimeLedger::new(limits, 5, 1);
        let runtime = ActorRuntime::new(1);
        config.max_work = 5;
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
        assert_eq!(ledger.used(Dimension::Work.id()), 5);
        assert_eq!(ledger.used(Dimension::Captures.id()), 1);
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
            .release(&partial.capacity.resources().unwrap())
            .unwrap();
        for permit in reserved {
            permit.revoke_unused().unwrap();
        }
        occupier.body_exited().unwrap();
        occupier.captures_exited().unwrap();
        capacity.release(&capacity.resources().unwrap()).unwrap();
        assert_eq!(ledger.used(Dimension::Work.id()), 3);
        assert_eq!(ledger.used(Dimension::Captures.id()), 0);
        drop((partial, occupier, capacity));
        let (service, resources) = owner.into_parts();
        drop(service);
        finish(&ledger, &runtime, resources);
    }
    #[test]
    fn real_ticket_replay_and_expiry_keep_the_original_holder_after_each_rpc_exits() {
        exercise_ticket_replay_and_expiry(false);
    }
    #[test]
    fn a_later_expiry_cannot_overtake_the_original_unadopted_ticket_body() {
        exercise_ticket_replay_and_expiry(true);
    }
    fn exercise_ticket_replay_and_expiry(expire_before_adoption: bool) {
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

        // One actual external RPC at a time, independently of the live ticket.
        let ticket_storage = StorageNeed {
            work: 1,
            ..StorageNeed::default()
        };
        let limits = PROCESS_CONTROL_BOOTSTRAP_STORAGE
            .checked_add(CONTROL_REQUEST_STORAGE)
            .unwrap()
            .checked_add(ticket_storage)
            .unwrap()
            .resources()
            .unwrap()
            .checked_add(&ResourceVector::new([(Dimension::Tickets.id(), 1)]).unwrap())
            .unwrap();
        let ledger = RuntimeLedger::new(limits, 5, 1);
        let runtime = ActorRuntime::new(1);
        let core = core();
        let capability = core.admission().capability();
        let mut config = binding();
        config.max_work = 5;
        config.max_slots = 2;
        config.admission_storage.ticket = ticket_storage.resources().unwrap();
        let mut owner = ProcessControlOwner::bootstrap(&runtime, &ledger, core, config).unwrap();
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
        for ordinal in 1..=if expire_before_adoption { 1 } else { 2 } {
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
                        // the second RPC must receive the first receipt and holder.
                        ticket: AdmissionTicketId::try_from_bytes([ordinal; 16]).unwrap(),
                    },
                )
                .unwrap();
            assert!(runtime.poll_one(Tick(ordinal.into())).unwrap());
            owner
                .index_control_effects(ControlCore::MAX_STEP_WORK)
                .unwrap();
            let body = owner
                .take_control_effect(ControlCore::MAX_STEP_WORK)
                .unwrap()
                .unwrap();
            assert_eq!(body.input, input);
            if expire_before_adoption {
                let expiry = runtime.next_deadline().unwrap();
                assert_eq!(runtime.advance_time(expiry, 1).unwrap(), 1);
                assert!(runtime.poll_one(expiry).unwrap());
                owner
                    .index_control_effects(ControlCore::MAX_STEP_WORK)
                    .unwrap();
                // The later timer is genuinely committed and its retirement
                // is ready in Common. Root metadata order still retains the
                // first original unbound grant until its actual adoption.
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
                assert!(owner.resources().tickets.is_empty());
                assert_eq!(ledger.used(Dimension::Tickets.id()), 1);
                advanced_expiry = Some(expiry);
            }
            let admitted = matches!(
                &body.effect,
                WorkerAction::Control {
                    admitted: Some(_),
                    ..
                }
            );
            assert_eq!(admitted, ordinal == 1);
            let receipt = owner.adopt_ticket(&body).unwrap();
            assert_eq!(*original.get_or_insert(receipt), receipt);
            assert_eq!(owner.resources().tickets.len(), 1);
            assert_eq!(ledger.used(Dimension::Tickets.id()), 1);
            let completed = body.descriptor.id;
            runtime.effect_completed(body).unwrap();
            owner
                .control_completion_observed(EffectRef {
                    input,
                    action: completed,
                })
                .unwrap();
            for (descriptor, _) in runtime
                .effect_inventory(actor(), input, None, ControlCore::MAX_STEP_WORK)
                .unwrap()
            {
                if descriptor.dependencies.contains(&completed) {
                    runtime
                        .acknowledge_dependency(actor(), input, descriptor.id, completed)
                        .unwrap();
                }
            }
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
            assert_eq!(ledger.used(Dimension::Replies.id()), 0);
            assert_eq!(ledger.used(Dimension::Tickets.id()), 1);
            assert_eq!(ledger.used(Dimension::Work.id()), 4);
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
        let retired = owner
            .take_control_effect(ControlCore::MAX_STEP_WORK)
            .unwrap()
            .unwrap();
        assert_eq!(retired.input, timer);
        assert!(matches!(
            &retired.effect,
            WorkerAction::Control {
                effect: ControlEffect::RetireReservation(_),
                admitted: None,
            }
        ));
        assert!(owner.retire_ticket(&retired).unwrap());
        assert!(owner.resources().tickets.is_empty());
        let reference = EffectRef {
            input: timer,
            action: retired.descriptor.id,
        };
        runtime.effect_completed(retired).unwrap();
        owner.control_completion_observed(reference).unwrap();
        runtime.retire_input(actor(), timer).unwrap();
        ledger
            .reap_scope_ready(owner.resources().scope.id(), 5)
            .unwrap();
        assert_eq!(ledger.used(Dimension::Tickets.id()), 0);
        assert_eq!(ledger.used(Dimension::Work.id()), 3);
        assert!(owner.resources().dispatch.is_empty());
        let (service, resources) = owner.into_parts();
        drop(service);
        finish(&ledger, &runtime, resources);
    }
    #[test]
    fn partial_root_role_refusal_retains_the_same_nonzero_payer_until_actual_metadata_exit() {
        let ledger =
            RuntimeLedger::new(PROCESS_CONTROL_BOOTSTRAP_STORAGE.resources().unwrap(), 2, 1);
        let runtime = ActorRuntime::new(1);
        let refusal = ProcessControlOwner::bootstrap(&runtime, &ledger, core(), binding())
            .err()
            .unwrap();
        assert!(matches!(refusal.error, ProcessBootstrapError::Runtime(_)));
        let resources = refusal.resources.unwrap();
        assert!(!resources.registered);
        assert_eq!(resources.roles.len(), 2);
        assert_eq!(ledger.used(Dimension::Work.id()), KINDS.len() as u64);
        assert_eq!(resources.scope.snapshot().unwrap().work, 2);
        finish(&ledger, &runtime, resources);
    }
    #[test]
    fn closed_runtime_registration_returns_the_same_complete_bootstrap_inventory() {
        let ledger = RuntimeLedger::new(
            PROCESS_CONTROL_BOOTSTRAP_STORAGE.resources().unwrap(),
            KINDS.len(),
            1,
        );
        let runtime = ActorRuntime::new(1);
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
        assert_eq!(ledger.used(Dimension::Work.id()), KINDS.len() as u64);
        finish(&ledger, &runtime, resources);
    }
    #[test]
    fn invalid_dependency_geometry_is_rejected_before_any_root_scope_or_payer_exists() {
        let ledger = RuntimeLedger::new(
            PROCESS_CONTROL_BOOTSTRAP_STORAGE.resources().unwrap(),
            KINDS.len(),
            1,
        );
        let runtime = ActorRuntime::new(1);
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
    #[test]
    fn real_root_quota_denial_returns_its_empty_scope_without_manufacturing_a_work_exit() {
        let ledger = RuntimeLedger::new(ResourceVector::default(), KINDS.len(), 1);
        let runtime = ActorRuntime::new(1);
        let refusal = ProcessControlOwner::bootstrap(&runtime, &ledger, core(), binding())
            .err()
            .unwrap();
        assert!(matches!(refusal.error, ProcessBootstrapError::Runtime(_)));
        let resources = refusal.resources.unwrap();
        assert!(resources.roles.is_empty());
        assert!(!resources.registered);
        assert_eq!(resources.scope.snapshot().unwrap().work, 0);
        assert_eq!(ledger.used(Dimension::Work.id()), 0);
        finish(&ledger, &runtime, resources);
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
            let ledger = RuntimeLedger::new(all.resources().unwrap(), KINDS.len() + 1, 1);
            let runtime = ActorRuntime::new(1);
            let mut with_request = binding();
            with_request.max_work += 1;
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
                    work: request.id(),
                    request: None,
                    // An impossible internal host report models a Core bug.
                    // It is not a supported I/O refusal or a query failure.
                    command: ControlInput::ContextBootstrapFailed {
                        context: unknown,
                        refusal: novarocks_worker_protocol::creation::AdmissionRefusal::Verdict(
                            novarocks_execution_contract::OperationOutcome::ResourceExhausted,
                        ),
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
