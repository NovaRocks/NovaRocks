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

//! Original process-root request and external reply custody. Neutral body facts
//! may be shared between actual waiters; publication and exit authority cannot.

mod source_io;
pub(crate) use source_io::{RequestIoError, RequestIoProgress};

pub(crate) use super::request_ready::ControlRequestReady;
use super::request_ready::{RequestExternalLeg, RequestReadySlot};
use super::{
    actors::{ControlMessage, RequestMetadata, WorkerAction, WorkerActor, WorkerMessage},
    context_host::ContextHostRecord,
    context_host_jobs::{ContextHostJobError, ContextHostJobs},
    establish_ingress::{
        EstablishDisposalJob, EstablishIngressJob, EstablishIngressOutcome, EstablishIngressOwner,
        EstablishIngressPartial, EstablishIngressReturned, ValidatedMaterializationSource,
    },
    establish_route::{
        CONTEXT_ROUTE_ESCROW_STORAGE, EstablishRouteCustody, EstablishRouteError,
        ROOT_ROUTE_RETURN_STORAGE,
    },
    process_control::ProcessControlOwner,
};
use crate::control_runtime_policy::{PolicyError, StorageNeed};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{
        ActorRuntime, DriverError, EffectCompletionRefusal, EffectTicket, InputPermit, InputRoleId,
    },
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::{CapacityBundle, WorkLease, WorkRetirementObserver},
    owned_return::OwnedReturnReader,
    publication::PublishedReply,
    reported_work::{ReportedWork, ReportedWorkBindingError},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    completion::ConsumerKind,
    identity::{ActionId, ActorId, ConsumerId, InputId, WorkId},
    mailbox::InputClass,
    scope::WorkAdmission,
    scope::WorkKind,
};
use novarocks_execution_contract::{
    EstablishQueryContext, LeaseValidFor, OperationOutcome, QueryContextRef, TaskOperationId,
};
use novarocks_worker_protocol::{
    context::{EstablishReplayRequest, RequestResult},
    control::{ControlCore, ControlEffect, ControlInput},
    creation::{PayloadId, RequestContext},
    lease::MonotonicInstant,
};
use std::sync::Arc;

pub(crate) const CONTROL_REQUEST_STORAGE: StorageNeed = StorageNeed {
    inputs: 1,
    batches: 1,
    effects: ControlCore::MAX_STEP_WORK,
    cuts: 0,
    replies: 1,
    completions: 1,
    captures: 1,
    work: 1,
};
/// The same RPC position pays for its source child, one Context probe and
/// its independent Root return. Context materialization and opaque current-
/// credential comparison require their own actual roles and storage.
pub(crate) const ESTABLISH_REQUEST_STORAGE: StorageNeed = StorageNeed {
    inputs: CONTROL_REQUEST_STORAGE.inputs
        + CONTEXT_ROUTE_ESCROW_STORAGE.inputs
        + ROOT_ROUTE_RETURN_STORAGE.inputs,
    batches: CONTROL_REQUEST_STORAGE.batches
        + CONTEXT_ROUTE_ESCROW_STORAGE.batches
        + ROOT_ROUTE_RETURN_STORAGE.batches,
    effects: CONTROL_REQUEST_STORAGE.effects
        + CONTEXT_ROUTE_ESCROW_STORAGE.effects
        + ROOT_ROUTE_RETURN_STORAGE.effects,
    work: 3 + CONTEXT_ROUTE_ESCROW_STORAGE.work,
    completions: 2 + CONTEXT_ROUTE_ESCROW_STORAGE.completions,
    captures: 2 + CONTEXT_ROUTE_ESCROW_STORAGE.captures,
    ..CONTROL_REQUEST_STORAGE
};
fn core_consumer() -> ConsumerId {
    ConsumerId::new(1).expect("nonzero original Core consumer")
}
fn external_consumer() -> ConsumerId {
    ConsumerId::new(2).expect("nonzero original external consumer")
}

#[derive(Debug)]
pub(crate) enum ControlRequestError {
    Policy(PolicyError),
    Runtime(RuntimeError),
    Driver(DriverError),
    Binding(ReportedWorkBindingError),
    Exit(ExitRoleError),
    ReadyFull,
    DuplicateReadySlot,
    UnknownReadySlot,
    WrongRoot,
    WrongOperation,
    WrongRequest,
    WrongEffect,
    WrongPhase,
    MissingBody,
    BodyResultMismatch,
    AdmissionReceiptMismatch,
    SubmissionUnresolved,
    IngressNotEnabled,
    IngressPending,
    IngressUnresolved,
    SourceUnresolved { input: InputId },
    Route(EstablishRouteError),
    HostJob(ContextHostJobError),
}

/// A refusal before command capture retains each actually created original.
/// In particular, a scope error does not authorize an unused-exit inference.
#[must_use = "Retain or actually dispose every original partial admission"]
pub(crate) struct ControlRequestPartial {
    pub work: WorkLease,
    pub capacity: CapacityBundle,
    pub report: Option<ReportSlot>,
    pub permit: Option<InputPermit<WorkerActor>>,
    pub route_escrow: Option<CapacityBundle>,
}
#[must_use = "A refused request may retain a complete original partial inventory"]
pub(crate) struct ControlRequestAdmissionRefusal {
    pub error: ControlRequestError,
    pub partial: Option<ControlRequestPartial>,
}
impl std::fmt::Debug for ControlRequestAdmissionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlRequestAdmissionRefusal")
            .field("error", &self.error)
            .field("work", &self.partial.as_ref().map(|p| p.work.id()))
            .finish()
    }
}
#[must_use = "A refused neutral command remains with its caller or original custody"]
pub(crate) struct ControlRequestSubmitRefusal {
    pub error: ControlRequestError,
    /// None means the failed fill's complete neutral message remains in custody.
    pub command: Option<ControlInput>,
}
impl std::fmt::Debug for ControlRequestSubmitRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlRequestSubmitRefusal")
            .field("error", &self.error)
            .field("command_returned", &self.command.is_some())
            .finish()
    }
}
#[must_use = "Every refusal retains the original unique effect ticket"]
pub(crate) enum ControlRequestPublishRefusal {
    Rejected {
        error: ControlRequestError,
        ticket: EffectTicket<WorkerAction>,
    },
    Publication {
        error: DriverError,
        ticket: EffectTicket<WorkerAction>,
        body: Arc<ControlReplyBody>,
    },
    Completion(EffectCompletionRefusal<WorkerAction>),
}
impl std::fmt::Debug for ControlRequestPublishRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected { error, ticket } => f
                .debug_struct("RejectedControlReply")
                .field("error", error)
                .field("ticket", ticket)
                .finish(),
            Self::Publication { error, ticket, .. } => f
                .debug_struct("RefusedControlReplyPublication")
                .field("error", error)
                .field("ticket", ticket)
                .finish_non_exhaustive(),
            Self::Completion(refusal) => std::fmt::Debug::fmt(refusal, f),
        }
    }
}

pub(crate) use novarocks_worker_protocol::control::ControlReplyBody;

struct Endpoint {
    request: RequestContext,
    ready: ControlRequestReady,
    reply: PublishedReply<ControlReplyBody>,
    external: RequestExternalLeg,
}
impl Endpoint {
    fn end_external(this: &Arc<Self>) {
        this.external.end();
    }
    fn external_ended(&self) -> bool {
        self.external.is_ended()
    }
}
/// This endpoint cannot be cloned. Cancellation ends only the external leg.
#[must_use = "Take the original reply or end only this external wait"]
pub(crate) struct ControlRequestHandle {
    endpoint: Arc<Endpoint>,
    active: bool,
}
impl ControlRequestHandle {
    pub(crate) fn request(&self) -> RequestContext {
        self.endpoint.request
    }
    pub(crate) fn try_take(&mut self) -> Option<ControlRequestDelivery> {
        if !self.active {
            return None;
        }
        let body = self.endpoint.external.try_take(&self.endpoint.reply)?;
        self.active = false;
        Some(ControlRequestDelivery {
            endpoint: self.endpoint.clone(),
            body: Some(body),
        })
    }
}
/// Concrete coalesced metadata wake; it executes no application callback.
#[derive(Clone)]
pub(super) struct ControlRequestNotice(Arc<RequestReadySlot>);
impl ControlRequestNotice {
    pub(super) fn belongs_to(&self, ready: &ControlRequestReady) -> bool {
        self.0.belongs_to(ready)
    }
    pub(super) fn notify(&self) {
        self.0.notify();
    }
}

#[must_use = "The original request remains owned on ingress refusal"]
pub(crate) struct EstablishIngressBeginRefusal {
    pub error: ControlRequestError,
    request: std::mem::ManuallyDrop<EstablishQueryContext>,
}
impl EstablishIngressBeginRefusal {
    pub(super) fn new(error: ControlRequestError, request: EstablishQueryContext) -> Self {
        Self {
            error,
            request: std::mem::ManuallyDrop::new(request),
        }
    }
    pub(crate) fn into_request(self) -> EstablishQueryContext {
        std::mem::ManuallyDrop::into_inner(self.request)
    }
}
impl std::fmt::Debug for EstablishIngressBeginRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EstablishIngressBeginRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl Drop for ControlRequestHandle {
    fn drop(&mut self) {
        if self.active {
            Endpoint::end_external(&self.endpoint);
        }
    }
}
/// Taking the outbox transfers the same endpoint's physical external exit
/// obligation here. The body is disposed before that exit fact is published.
#[must_use = "The actual sender retains delivery custody until it really ends"]
pub(crate) struct ControlRequestDelivery {
    endpoint: Arc<Endpoint>,
    body: Option<Arc<ControlReplyBody>>,
}
impl ControlRequestDelivery {
    pub(crate) fn request(&self) -> RequestContext {
        self.endpoint.request
    }
    pub(crate) fn body(&self) -> &ControlReplyBody {
        self.body.as_deref().expect("live original reply delivery")
    }
}
impl Drop for ControlRequestDelivery {
    fn drop(&mut self) {
        drop(self.body.take());
        Endpoint::end_external(&self.endpoint);
    }
}

#[derive(Clone, Copy)]
enum RequestKind {
    Acquire {
        context: QueryContextRef,
        valid_for: LeaseValidFor,
    },
    Establish,
}
/// Neutral responsibility transferred by a validated original Root effect.
/// Private fields prevent inventing a deferred route from numeric identities.
pub(super) struct ValidatedEstablishRoute {
    root: ActorId,
    context: QueryContextRef,
    request: EstablishReplayRequest,
    order: u64,
}
impl ValidatedEstablishRoute {
    pub(super) fn root(&self) -> ActorId {
        self.root
    }
    pub(super) fn context(&self) -> QueryContextRef {
        self.context
    }
    pub(super) fn request(&self) -> RequestContext {
        self.request.request
    }
}
#[must_use = "The actual Root must retain the request until its original retirement"]
pub(crate) struct ControlRequestCustody {
    runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    actor: ActorId,
    work: WorkId,
    work_retirement: WorkRetirementObserver,
    endpoint: Arc<Endpoint>,
    permit: Option<InputPermit<WorkerActor>>,
    role: ExitRole,
    authority: Option<ExitAuthority>,
    report: ReportSlot,
    kind: Option<RequestKind>,
    submitted: bool,
    started: bool,
    failed_submission: Option<WorkerMessage>,
    published: Option<(InputId, ActionId)>,
    reply_completed: bool,
    initial_retired: bool,
    exit_observed: bool,
    core_consumed: bool,
    external_consumed: bool,
    retired: bool,
    ingress_enabled: bool,
    ingress_issued: bool,
    ingress_outcome: Option<EstablishIngressOutcome>,
    pending_getter: Option<EstablishIngressJob>,
    pending_disposal: Option<EstablishDisposalJob>,
    ingress_owner: Option<EstablishIngressOwner>,
    ingress_partial: Option<EstablishIngressPartial>,
    ingress_return: Option<OwnedReturnReader<EstablishIngressReturned>>,
    route_escrow: Option<CapacityBundle>,
    route: Option<Arc<EstablishRouteCustody>>,
    creator: Option<ContextHostJobs>,
    route_cancelled: bool,
    route_issued: bool,
    pending_route: Option<ValidatedEstablishRoute>,
    withdrawal: bool,
}
impl ControlRequestCustody {
    pub(crate) fn admit(
        root: &ProcessControlOwner,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(Self, ControlRequestHandle), ControlRequestAdmissionRefusal> {
        Self::admit_with_storage(root, source, operation, deadline, false)
    }
    pub(crate) fn admit_establish(
        root: &ProcessControlOwner,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(Self, ControlRequestHandle), ControlRequestAdmissionRefusal> {
        Self::admit_with_storage(root, source, operation, deadline, true)
    }
    fn admit_with_storage(
        root: &ProcessControlOwner,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        ingress_enabled: bool,
    ) -> Result<(Self, ControlRequestHandle), ControlRequestAdmissionRefusal> {
        let empty = |error| ControlRequestAdmissionRefusal {
            error,
            partial: None,
        };
        let scope = &root.resources().scope;
        let generation = scope
            .gates()
            .map_err(|e| empty(ControlRequestError::Runtime(e)))?
            .generation;
        let guard = scope
            .reserve_next(
                generation,
                (if ingress_enabled {
                    ESTABLISH_REQUEST_STORAGE
                } else {
                    CONTROL_REQUEST_STORAGE
                })
                .resources()
                .map_err(|e| empty(ControlRequestError::Policy(e)))?,
                WorkKind::Step,
                WorkAdmission::External,
            )
            .map_err(|e| empty(ControlRequestError::Runtime(e)))?;
        // There are still no foreign captures. Failed bind rolls back only the
        // actual pure unbound guard, never a previously submitted request.
        let (work, mut capacity) = match guard.bind(WorkKind::Step) {
            Ok(bound) => bound,
            // A fresh same-kind bind has no ordinary admission refusal after
            // reservation. No command or external payload has been captured;
            // shared admission corruption cannot become a request verdict.
            Err(_error) => std::process::abort(),
        };
        let report = match ReportSlot::reserve_next(
            scope,
            generation,
            work.id(),
            2,
            [
                (core_consumer(), ConsumerKind::Observer),
                (external_consumer(), ConsumerKind::Observer),
            ],
        ) {
            Ok(report) => report,
            Err(error) => {
                return Err(ControlRequestAdmissionRefusal {
                    error: ControlRequestError::Runtime(error),
                    partial: Some(ControlRequestPartial {
                        work,
                        capacity,
                        report: None,
                        permit: None,
                        route_escrow: None,
                    }),
                });
            }
        };
        let permit = match root
            .runtime()
            .reserve_input(root.actor(), source, InputClass::Ordinary)
        {
            Ok(permit) => permit,
            Err(error) => {
                return Err(ControlRequestAdmissionRefusal {
                    error: ControlRequestError::Driver(error),
                    partial: Some(ControlRequestPartial {
                        work,
                        capacity,
                        report: Some(report),
                        permit: None,
                        route_escrow: None,
                    }),
                });
            }
        };
        let request = RequestContext {
            token: permit.id(),
            operation,
            deadline,
        };
        let work_id = work.id();
        let work_retirement = work.retirement_observer();
        let route_escrow = if ingress_enabled {
            match capacity.split_next(
                &CONTEXT_ROUTE_ESCROW_STORAGE
                    .resources()
                    .expect("fixed Context route escrow"),
            ) {
                Ok(escrow) => Some(escrow),
                Err(error) => {
                    return Err(ControlRequestAdmissionRefusal {
                        error: ControlRequestError::Runtime(error),
                        partial: Some(ControlRequestPartial {
                            work,
                            capacity,
                            report: Some(report),
                            permit: Some(permit),
                            route_escrow: None,
                        }),
                    });
                }
            }
        } else {
            None
        };
        let reported = match ReportedWork::try_new(work, capacity, report.clone()) {
            Ok(reported) => reported,
            Err(refusal) => {
                return Err(ControlRequestAdmissionRefusal {
                    error: ControlRequestError::Binding(refusal.error),
                    partial: Some(ControlRequestPartial {
                        work: refusal.work,
                        capacity: refusal.capacity,
                        report: Some(refusal.report),
                        permit: Some(permit),
                        route_escrow,
                    }),
                });
            }
        };
        let slot = match root.request_ready().register_control(request.token) {
            Ok(slot) => slot,
            Err(error) => {
                let (work, capacity, report) = reported.into_parts();
                return Err(ControlRequestAdmissionRefusal {
                    error,
                    partial: Some(ControlRequestPartial {
                        work,
                        capacity,
                        report: Some(report),
                        permit: Some(permit),
                        route_escrow,
                    }),
                });
            }
        };
        let endpoint = Arc::new(Endpoint {
            request,
            ready: root.request_ready().clone(),
            reply: PublishedReply::new(scope),
            external: RequestExternalLeg::new(slot),
        });
        let (role, authority) = ExitRole::from_admitted(reported);
        Ok((
            Self {
                runtime: root.runtime().clone(),
                ledger: root.ledger().clone(),
                scope: scope.clone(),
                actor: root.actor(),
                work: work_id,
                work_retirement,
                endpoint: endpoint.clone(),
                permit: Some(permit),
                role,
                authority: Some(authority),
                report,
                kind: None,
                submitted: false,
                started: false,
                failed_submission: None,
                published: None,
                reply_completed: false,
                initial_retired: false,
                exit_observed: false,
                core_consumed: false,
                external_consumed: false,
                retired: false,
                ingress_enabled,
                ingress_issued: false,
                ingress_outcome: None,
                pending_getter: None,
                pending_disposal: None,
                ingress_owner: None,
                ingress_partial: None,
                ingress_return: None,
                route_escrow,
                route: None,
                creator: None,
                route_cancelled: false,
                route_issued: false,
                pending_route: None,
                withdrawal: false,
            },
            ControlRequestHandle {
                endpoint,
                active: true,
            },
        ))
    }
    pub(crate) fn begin_establish_ingress(
        &mut self,
        request: EstablishQueryContext,
        payload: PayloadId,
    ) -> Result<EstablishIngressJob, EstablishIngressBeginRefusal> {
        // A refusal or invariant must never unwind an opaque DTO on a poller.
        let request = std::mem::ManuallyDrop::new(request);
        if !self.ingress_enabled {
            return Err(EstablishIngressBeginRefusal {
                error: ControlRequestError::IngressNotEnabled,
                request,
            });
        }
        if self.submitted || self.ingress_issued || self.started || self.authority.is_none() {
            return Err(EstablishIngressBeginRefusal {
                error: ControlRequestError::WrongPhase,
                request,
            });
        }
        if request.envelope().operation_id() != self.request().operation {
            return Err(EstablishIngressBeginRefusal {
                error: ControlRequestError::WrongOperation,
                request,
            });
        }
        // Claim while the original R producer is still bound and Queued.
        let (sender, reader) = match self.report.claim_owned_return() {
            Ok(pair) => pair,
            Err(error) => {
                return Err(EstablishIngressBeginRefusal {
                    error: ControlRequestError::Runtime(error),
                    request,
                });
            }
        };
        self.ingress_issued = true;
        self.ingress_return = Some(reader);
        let generation = self.scope.gates().expect("original Root scope").generation;
        let owner = match EstablishIngressOwner::bootstrap(
            self.authority.as_mut().expect("original RPC authority"),
            generation,
            &self.ledger,
            self.scope.id(),
            ControlRequestNotice(self.endpoint.external.slot().clone()),
        ) {
            Ok(owner) => owner,
            Err(refusal) => {
                self.ingress_partial = refusal.partial;
                if let Err(refusal) = sender.revoke_unused() {
                    std::mem::forget(refusal);
                    std::process::abort();
                }
                return Err(EstablishIngressBeginRefusal {
                    error: ControlRequestError::Exit(refusal.error),
                    request,
                });
            }
        };
        self.ingress_owner = Some(owner.clone());
        Ok(owner.capture(
            std::mem::ManuallyDrop::into_inner(request),
            payload,
            self.authority.take().expect("original RPC authority"),
            sender,
        ))
    }
    pub(crate) fn take_establish_ingress(
        &mut self,
    ) -> Result<Option<EstablishIngressOutcome>, ControlRequestError> {
        let Some(reader) = &mut self.ingress_return else {
            return Ok(None);
        };
        let Some(returned) = reader.try_take().map_err(ControlRequestError::Runtime)? else {
            return Ok(None);
        };
        if self.authority.is_some() {
            std::mem::forget(returned);
            std::process::abort();
        }
        self.authority = returned.authority;
        self.ingress_outcome = Some(returned.outcome);
        self.started = matches!(
            returned.outcome,
            EstablishIngressOutcome::Projected(_) | EstablishIngressOutcome::GetterPanicked
        );
        Ok(Some(returned.outcome))
    }
    fn ingress_projection(&self) -> Option<super::establish_ingress::EstablishProjection> {
        match self.ingress_outcome {
            Some(EstablishIngressOutcome::Projected(projection)) => Some(projection),
            _ => None,
        }
    }
    fn ingress_panicked(&self) -> bool {
        matches!(
            self.ingress_outcome,
            Some(EstablishIngressOutcome::GetterPanicked)
        )
    }
    pub(super) fn prepare_establish_route(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
        order: u64,
    ) -> Result<QueryContextRef, ControlRequestError> {
        self.runtime
            .validate_effect_ticket(ticket)
            .map_err(ControlRequestError::Driver)?;
        let WorkerAction::Control {
            effect: ControlEffect::RouteEstablish { context, request },
            admitted: None,
        } = &ticket.effect
        else {
            return Err(ControlRequestError::WrongEffect);
        };
        if ticket.actor != self.actor
            || ticket.descriptor.work != self.work
            || request.request != self.request()
            || !self.submitted
            || self.route_issued
            || self.creator.is_some()
            || !self.ingress_enabled
        {
            return Err(ControlRequestError::WrongRequest);
        }
        let projection = self
            .ingress_projection()
            .ok_or(ControlRequestError::IngressPending)?;
        if projection.context != *context
            || projection.operation != request.request.operation
            || projection.ticket != request.ticket
            || projection.payload != request.payload
            || projection.semantics != request.semantics
        {
            return Err(ControlRequestError::WrongRequest);
        }
        self.pending_route = Some(ValidatedEstablishRoute {
            root: self.actor,
            context: *context,
            request: *request,
            order,
        });
        self.route_issued = true;
        Ok(*context)
    }
    /// The elected creator lends its own raw input to the Context host job.
    /// The ticket proves selection only; the Context issuer must separately
    /// admit its actual M Work, capture, return cell and input author before
    /// capturing this loan. Root R and Context M retain independent lifetimes.
    pub(super) fn take_materialization_source(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<ValidatedMaterializationSource, ControlRequestError> {
        self.runtime
            .validate_effect_ticket(ticket)
            .map_err(ControlRequestError::Driver)?;
        let WorkerAction::Control {
            effect:
                ControlEffect::Materialize {
                    context,
                    establishment,
                    reservation,
                    generation,
                },
            admitted: Some(original),
        } = &ticket.effect
        else {
            return Err(ControlRequestError::WrongEffect);
        };
        let projection = self
            .ingress_projection()
            .ok_or(ControlRequestError::IngressPending)?;
        if ticket.actor != self.actor
            || ticket.input != self.input_id()
            || reservation != original
            || !self.submitted
            || self.withdrawal
            || self.route_issued
            || projection.context != *context
            || projection.operation != establishment.operation
            || projection.ticket != establishment.ticket
            || projection.payload != establishment.payload
            || projection.semantics != establishment.semantics
        {
            return Err(ControlRequestError::WrongRequest);
        }
        // This exact runtime-issued M descriptor names its new Context
        // admission Work. It must not equal the original R author: taking the
        // committed guard independently validates that M Work binding.
        let loan = self
            .ingress_owner
            .as_ref()
            .and_then(EstablishIngressOwner::take_materialization)
            .ok_or(ControlRequestError::WrongPhase)?;
        Ok(ValidatedMaterializationSource::new(
            self.runtime.clone(),
            self.actor,
            *generation,
            *establishment,
            self.request(),
            loan,
        ))
    }
    pub(super) fn pending_route(&self) -> Option<(QueryContextRef, u64)> {
        self.pending_route
            .as_ref()
            .map(|seed| (seed.context, seed.order))
    }
    pub(super) fn route_return_role(&self) -> Option<InputRoleId> {
        self.route
            .as_ref()
            .and_then(|route| route.return_role())
            .or_else(|| {
                self.creator
                    .as_ref()
                    .and_then(ContextHostJobs::creator_role)
            })
    }
    pub(super) fn bind_creator_return(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
        jobs: ContextHostJobs,
        author: &WorkLease,
    ) -> Result<(), ControlRequestError> {
        self.runtime
            .validate_effect_ticket(ticket)
            .map_err(ControlRequestError::Driver)?;
        let WorkerAction::Control {
            effect:
                ControlEffect::Materialize {
                    context,
                    establishment,
                    generation,
                    reservation,
                },
            admitted: Some(original),
        } = &ticket.effect
        else {
            return Err(ControlRequestError::WrongEffect);
        };
        let p = self
            .ingress_projection()
            .ok_or(ControlRequestError::IngressPending)?;
        if ticket.actor != self.actor
            || ticket.input != self.input_id()
            || reservation != original
            || !self.submitted
            || self.route_issued
            || self.creator.is_some()
            || self.withdrawal
            || p.context != *context
            || p.operation != establishment.operation
            || p.ticket != establishment.ticket
            || p.payload != establishment.payload
            || p.semantics != establishment.semantics
            || !jobs.matches_owner(
                &self.runtime,
                self.actor,
                *context,
                *generation,
                *establishment,
            )
        {
            return Err(ControlRequestError::WrongRequest);
        }
        self.creator = Some(jobs.clone());
        jobs.bind_creator(
            ticket,
            self.request(),
            self.work,
            author,
            ControlRequestNotice(self.endpoint.external.slot().clone()),
        )
        .map_err(ControlRequestError::HostJob)?;
        Ok(())
    }
    pub(crate) fn take_consumed_creator_disposal(
        &self,
    ) -> Result<Option<EstablishDisposalJob>, ControlRequestError> {
        let Some(creator) = &self.creator else {
            return Ok(None);
        };
        if !creator
            .source_consumed()
            .map_err(ControlRequestError::HostJob)?
        {
            return Ok(None);
        }
        Ok(self
            .ingress_owner
            .as_ref()
            .and_then(EstablishIngressOwner::take_disposal))
    }
    pub(super) fn bind_pending_establish_route(
        &mut self,
        host: &mut ContextHostRecord,
        root_author: &WorkLease,
    ) -> Result<Result<(), EstablishRouteError>, ControlRequestError> {
        let seed = self
            .pending_route
            .as_ref()
            .ok_or(ControlRequestError::WrongPhase)?;
        host.validate_establish_route(&self.runtime, seed)
            .map_err(ControlRequestError::Route)?;
        let escrow = self
            .route_escrow
            .take()
            .ok_or(ControlRequestError::WrongPhase)?;
        let seed = self
            .pending_route
            .take()
            .expect("validated original route seed");
        let route = EstablishRouteCustody::new(
            self.runtime.clone(),
            self.ledger.clone(),
            self.actor,
            self.work,
            seed.context,
            host.actor(),
            host.scope().clone(),
            seed.request,
            escrow,
            ControlRequestNotice(self.endpoint.external.slot().clone()),
        );
        // Register both aliases before the first Context Work, input or
        // backchannel is constructed. Initialization refusal retains the same
        // original partial in these actual failure inventories.
        self.route = Some(route.clone());
        Ok(host.register_establish_route(route, root_author))
    }
    pub(super) fn cancel_establish_route(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ControlRequestError> {
        self.runtime
            .validate_effect_ticket(ticket)
            .map_err(ControlRequestError::Driver)?;
        let WorkerAction::Control {
            effect: ControlEffect::CancelEstablishRoute { context, request },
            ..
        } = &ticket.effect
        else {
            return Err(ControlRequestError::WrongEffect);
        };
        if ticket.actor != self.actor
            || *request != self.request()
            || !self.route_issued
            || self
                .ingress_projection()
                .is_none_or(|projection| projection.context != *context)
        {
            return Err(ControlRequestError::WrongRequest);
        }
        if let Some(route) = &self.route {
            route.cancel();
        }
        self.pending_route = None;
        self.route_cancelled = true;
        Ok(())
    }
    /// Only a cancelled replay with a really retired Context leg can claim
    /// its exclusive source cell. Creator timeouts cannot use this proof.
    pub(crate) fn take_retired_route_disposal(&self) -> Option<EstablishDisposalJob> {
        if !self.route_cancelled
            || self.pending_route.is_some()
            || self
                .route
                .as_ref()
                .is_some_and(|route| !route.context_retired())
        {
            return None;
        }
        self.ingress_owner
            .as_ref()
            .and_then(EstablishIngressOwner::take_disposal)
    }
    fn close_route(&mut self) -> Result<bool, ControlRequestError> {
        if let Some(creator) = &self.creator
            && !creator
                .retire_creator()
                .map_err(ControlRequestError::HostJob)?
        {
            return Ok(false);
        }
        if self.pending_route.is_some() {
            return Ok(false);
        }
        if let Some(route) = &self.route
            && !route
                .try_retire_root()
                .map_err(ControlRequestError::Route)?
        {
            return Ok(false);
        }
        if let Some(escrow) = self.route_escrow.as_mut() {
            escrow
                .release(&escrow.resources().map_err(ControlRequestError::Runtime)?)
                .map_err(ControlRequestError::Runtime)?;
            self.route_escrow = None;
        }
        Ok(true)
    }
    pub(crate) fn begin_unsubmitted_withdrawal(
        &mut self,
    ) -> Result<Option<EstablishDisposalJob>, ControlRequestError> {
        if self.submitted
            || self.failed_submission.is_some()
            || self.authority.is_none()
            || self.ingress_panicked()
        {
            return Err(ControlRequestError::WrongPhase);
        }
        if let Some(reader) = &self.ingress_return {
            use novarocks_control_runtime_core::completion::ReturnState;
            if !matches!(
                reader.state().map_err(ControlRequestError::Runtime)?,
                ReturnState::Taken | ReturnState::RevokedUnused
            ) {
                return Err(ControlRequestError::IngressPending);
            }
        }
        self.withdrawal = true;
        self.note_progress();
        Ok(self
            .ingress_owner
            .as_ref()
            .and_then(EstablishIngressOwner::take_disposal))
    }
    pub(crate) fn begin_rejected_ingress_disposal(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<Option<EstablishDisposalJob>, ControlRequestError> {
        self.runtime
            .validate_effect_ticket(ticket)
            .map_err(ControlRequestError::Driver)?;
        if ticket.actor != self.actor {
            return Err(ControlRequestError::WrongRoot);
        }
        if !self.submitted || !matches!(self.kind, Some(RequestKind::Establish)) || self.withdrawal
        {
            return Err(ControlRequestError::WrongPhase);
        }
        let WorkerAction::ControlReply(reply) = &ticket.effect else {
            return Err(ControlRequestError::WrongEffect);
        };
        if reply.request != self.request()
            || matches!(
                reply.result,
                RequestResult::TimeoutWhileAccepted
                    | RequestResult::Outcome(
                        OperationOutcome::Accepted | OperationOutcome::Idempotent
                    )
            )
        {
            return Err(ControlRequestError::WrongEffect);
        }
        Ok(self
            .ingress_owner
            .as_ref()
            .and_then(EstablishIngressOwner::take_disposal))
    }
    pub(super) fn ingress_source_phase(&self) -> Option<ExitRolePhase> {
        self.ingress_owner
            .as_ref()
            .map(EstablishIngressOwner::phase)
    }
    fn close_ingress(&mut self) -> Result<bool, ControlRequestError> {
        if let Some(partial) = self.ingress_partial.take()
            && let Err(original) = partial.close(&self.ledger, self.scope.id())
        {
            self.ingress_partial = Some(original);
            return Err(ControlRequestError::IngressUnresolved);
        }
        if let Some(owner) = &self.ingress_owner {
            return owner.try_close().map_err(ControlRequestError::Exit);
        }
        Ok(true)
    }
    fn retire_original_role(&self) -> Result<bool, ControlRequestError> {
        if !self.work_retirement.is_retired() {
            self.ledger
                .reap_scope_ready(self.scope.id(), 1)
                .map_err(ControlRequestError::Runtime)?;
            if !self.work_retirement.is_retired() {
                // All actual exits were observed by the caller. Only original
                // ready metadata (including the child link) remains to reap.
                self.note_progress();
                return Ok(false);
            }
        }
        self.role.try_retire().map_err(ControlRequestError::Exit)
    }
    pub(crate) fn request(&self) -> RequestContext {
        self.endpoint.request
    }
    pub(crate) fn input_id(&self) -> InputId {
        self.request().token
    }
    /// The Root first inserts this same custody by input_id in its actual
    /// table. Only then may it fill this original permit and expose a command.
    /// Failure before semantic admission settles only this original RPC.
    /// Observed call failure preserves the original authority. A genuine lost
    /// producer remains Unknown. Neither failure publication supplies any
    /// missing raw/capture/child/input/report/delivery exit.
    pub(super) fn submit_ingress_owner_failure(
        &mut self,
        failure_work: WorkId,
    ) -> Result<(), ControlRequestSubmitRefusal> {
        let command = ControlInput::IngressOwnerFailed {
            request: self.request().operation,
        };
        let eligible = self.ingress_enabled
            && self.ingress_issued
            && !self.submitted
            && !self.withdrawal
            && self.failed_submission.is_none()
            && self.permit.is_some();
        if !eligible {
            return Err(ControlRequestSubmitRefusal {
                error: ControlRequestError::WrongPhase,
                command: Some(command),
            });
        }
        use novarocks_control_runtime_core::completion::ReturnState;
        let returned =
            self.report
                .owned_return_state()
                .map_err(|error| ControlRequestSubmitRefusal {
                    error: ControlRequestError::Runtime(error),
                    command: Some(ControlInput::IngressOwnerFailed {
                        request: self.request().operation,
                    }),
                })?;
        if returned == ReturnState::Filled {
            return Err(ControlRequestSubmitRefusal {
                error: ControlRequestError::IngressPending,
                command: Some(command),
            });
        }
        let observed_panic = self.ingress_panicked()
            && self.authority.is_some()
            && self.started
            && self.role.phase() == ExitRolePhase::Started
            && returned == ReturnState::Taken;
        let lost = self.authority.is_none()
            && self.role.phase() == ExitRolePhase::Unresolved
            && matches!(returned, ReturnState::Taken | ReturnState::Unknown);
        if !observed_panic && !lost {
            return Err(ControlRequestSubmitRefusal {
                error: ControlRequestError::WrongPhase,
                command: Some(command),
            });
        }
        if let Err(error) = self.permit.as_ref().expect("original RPC permit").bind() {
            return Err(ControlRequestSubmitRefusal {
                error: ControlRequestError::Driver(error),
                command: Some(command),
            });
        }
        self.kind = Some(RequestKind::Establish);
        self.fill_original_command(command, failure_work)
    }
    pub(crate) fn submit(
        &mut self,
        command: ControlInput,
    ) -> Result<(), ControlRequestSubmitRefusal> {
        let reject = |error, command| ControlRequestSubmitRefusal {
            error,
            command: Some(command),
        };
        if self.submitted
            || self.withdrawal
            || self.failed_submission.is_some()
            || self.permit.is_none()
        {
            return Err(reject(ControlRequestError::WrongPhase, command));
        }
        if self.ingress_enabled {
            let Some(projection) = self.ingress_projection() else {
                return Err(reject(ControlRequestError::IngressPending, command));
            };
            let matches_projection = matches!(&command,
                ControlInput::Establish { request, context, ticket, payload, semantics, .. }
                if *request == projection.operation && *context == projection.context && *ticket == projection.ticket
                    && *payload == projection.payload && *semantics == projection.semantics);
            if self.authority.is_none() || !self.started || !matches_projection {
                return Err(reject(ControlRequestError::WrongRequest, command));
            }
        }
        let kind = match &command {
            ControlInput::Acquire { request, .. }
                if request.envelope().operation_id() == self.request().operation =>
            {
                RequestKind::Acquire {
                    context: request.context(),
                    valid_for: request.valid_for(),
                }
            }
            ControlInput::Establish { request, .. } if *request == self.request().operation => {
                RequestKind::Establish
            }
            _ => return Err(reject(ControlRequestError::WrongOperation, command)),
        };
        if !self.started {
            if let Err(error) = self
                .authority
                .as_mut()
                .expect("original request authority")
                .start()
            {
                return Err(reject(ControlRequestError::Exit(error), command));
            }
            self.started = true;
        }
        if let Err(error) = self
            .permit
            .as_ref()
            .expect("original reserved input")
            .bind()
        {
            return Err(reject(ControlRequestError::Driver(error), command));
        }
        self.kind = Some(kind);
        self.fill_original_command(command, self.work)
    }
    fn fill_original_command(
        &mut self,
        command: ControlInput,
        work: WorkId,
    ) -> Result<(), ControlRequestSubmitRefusal> {
        let message = WorkerMessage::Control(ControlMessage::Command {
            work,
            request: Some(RequestMetadata {
                operation: self.request().operation,
                deadline: self.request().deadline,
            }),
            command,
        });
        match self
            .permit
            .take()
            .expect("one original input fill")
            .fill(message)
        {
            Ok(input) => {
                assert_eq!(input, self.input_id(), "same original request input");
                self.submitted = true;
                Ok(())
            }
            Err((error, original)) => {
                // Common currently returns no InputPermit on a failed fill.
                // Keep its exact identity and neutral message unresolved; never
                // manufacture unused revocation, a replacement ID or an ACK.
                self.failed_submission = Some(original);
                Err(ControlRequestSubmitRefusal {
                    error: ControlRequestError::Driver(error),
                    command: None,
                })
            }
        }
    }
    pub(crate) fn publish(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), ControlRequestPublishRefusal> {
        let reject = |error, ticket| ControlRequestPublishRefusal::Rejected { error, ticket };
        let (settlement, decided_body) = match &ticket.effect {
            WorkerAction::ControlReply(reply) if reply.request == self.request() => {
                (reply.settlement, reply.body.clone())
            }
            _ => return Err(reject(ControlRequestError::WrongRequest, ticket)),
        };
        if ticket.actor != self.actor {
            return Err(reject(ControlRequestError::WrongRoot, ticket));
        }
        if !self.submitted || self.reply_completed {
            return Err(reject(ControlRequestError::WrongPhase, ticket));
        }
        let key = (ticket.input, ticket.descriptor.id);
        if let Some(original) = self.published {
            if original != key {
                return Err(reject(ControlRequestError::WrongEffect, ticket));
            }
            // A completion refusal returned this unique original ticket. Its
            // origin-only completion gate remains valid after scope failure;
            // re-running publication would incorrectly require Started again.
        } else {
            if let Err(error) = self.runtime.validate_effect_ticket(&ticket) {
                return Err(reject(ControlRequestError::Driver(error), ticket));
            }
            if !decided_body.matches(settlement.result) {
                return Err(reject(ControlRequestError::BodyResultMismatch, ticket));
            }
            match (self.kind, &decided_body) {
                (
                    Some(RequestKind::Acquire { context, valid_for }),
                    ControlReplyBody::Admission(Ok(receipt)),
                ) if receipt.context() != context || receipt.valid_for() != valid_for => {
                    return Err(reject(
                        ControlRequestError::AdmissionReceiptMismatch,
                        ticket,
                    ));
                }
                (
                    Some(RequestKind::Acquire { .. }),
                    ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::Accepted)),
                ) => {
                    return Err(reject(ControlRequestError::MissingBody, ticket));
                }
                (Some(RequestKind::Establish), ControlReplyBody::Admission(_)) => {
                    return Err(reject(ControlRequestError::WrongEffect, ticket));
                }
                _ => {}
            }
            let body = Arc::new(decided_body);
            if let Err((error, body)) =
                self.runtime
                    .commit_reply(&ticket, &self.endpoint.reply, body)
            {
                return Err(ControlRequestPublishRefusal::Publication {
                    error,
                    ticket,
                    body,
                });
            }
            self.published = Some(key);
        }
        match self.runtime.effect_completed(ticket) {
            Ok(()) => {
                self.reply_completed = true;
                self.endpoint.external.notify();
                Ok(())
            }
            Err(refusal) => Err(ControlRequestPublishRefusal::Completion(refusal)),
        }
    }
    pub(crate) fn note_progress(&self) {
        self.endpoint.external.notify();
    }
    /// Initial input retirement and final RPC completion are independent. A
    /// delayed Establish may retire its first step while its RPC still waits.
    pub(crate) fn try_retire(&mut self) -> Result<bool, ControlRequestError> {
        if self.retired {
            return Ok(true);
        }
        if self.failed_submission.is_some() {
            return Err(ControlRequestError::SubmissionUnresolved);
        }
        if self.pending_getter.is_some() || self.pending_disposal.is_some() {
            return Ok(false);
        }
        if self.withdrawal {
            if !self.endpoint.external_ended() || !self.close_ingress()? || !self.close_route()? {
                return Ok(false);
            }
            if !self.initial_retired {
                if let Err(_error) = self
                    .permit
                    .take()
                    .expect("original never-filled permit")
                    .revoke_unused()
                {
                    std::process::abort();
                }
                self.initial_retired = true;
            }
            if !self.exit_observed {
                self.authority
                    .take()
                    .ok_or(ControlRequestError::WrongPhase)?
                    .withdraw_unpublished_and_exit()
                    .map_err(ControlRequestError::Exit)?;
                self.exit_observed = true;
            }
            if !self.retire_original_role()? {
                return Ok(false);
            }
            self.endpoint
                .ready
                .unregister(self.endpoint.external.slot())?;
            self.retired = true;
            return Ok(true);
        }
        if !self.submitted {
            return Ok(false);
        }
        if !self.initial_retired {
            match self.runtime.retire_input(self.actor, self.input_id()) {
                Ok(()) => self.initial_retired = true,
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain,
                ) => return Ok(false),
                Err(error) => return Err(ControlRequestError::Driver(error)),
            }
        }
        if !self.reply_completed || !self.endpoint.external_ended() {
            return Ok(false);
        }
        if !self.close_ingress()? || !self.close_route()? {
            return Ok(false);
        }
        if self.role.phase() == ExitRolePhase::Unresolved {
            // All known output/source work may drain, while this original
            // failed physical call is still conservatively Unknown.
            return Ok(false);
        }
        // Cancellation can leave a committed neutral body untaken. Dispose it
        // outside all internal locks before observing actual capture exit.
        drop(self.endpoint.reply.take_once());
        if !self.exit_observed {
            self.authority
                .take()
                .ok_or(ControlRequestError::WrongPhase)?
                .exited()
                .map_err(ControlRequestError::Exit)?;
            self.exit_observed = true;
        }
        if !self.core_consumed {
            self.report
                .consume(core_consumer())
                .map_err(ControlRequestError::Runtime)?;
            self.core_consumed = true;
        }
        if !self.external_consumed {
            self.report
                .consume(external_consumer())
                .map_err(ControlRequestError::Runtime)?;
            self.external_consumed = true;
        }
        if !self.retire_original_role()? {
            return Ok(false);
        }
        self.endpoint
            .ready
            .unregister(self.endpoint.external.slot())?;
        self.retired = true;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    mod establish_route;
    use super::*;
    use crate::{
        control_admission::{AdmissionStorage, Dimension},
        control_runtime::process_control::{
            PROCESS_CONTROL_BOOTSTRAP_STORAGE, ProcessControlBinding,
        },
    };
    use novarocks_control_runtime::{actor::ActorConfig, transaction::RuntimeLedger};
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        completion::Consumption,
        identity::{Generation, Tick},
        scope::ScopeError,
    };
    use novarocks_execution_contract::{
        AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
        QueryId,
    };
    use novarocks_worker_protocol::{admission::AdmissionTicketConfig, control::ControlConfig};
    use std::{collections::BTreeMap, num::NonZeroUsize, time::Duration};

    fn generation() -> Generation {
        Generation::new(1).unwrap()
    }
    fn actor() -> ActorId {
        ActorId::new(1).unwrap()
    }
    fn operation() -> TaskOperationId {
        let mut bytes = [11; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        TaskOperationId::try_from_bytes(bytes).unwrap()
    }
    struct Fixture {
        runtime: ActorRuntime<WorkerActor>,
        ledger: RuntimeLedger,
        root: ProcessControlOwner,
        requests: BTreeMap<InputId, ControlRequestCustody>,
        capability: AdmissionEpochCapability,
        context: QueryContextRef,
    }
    struct HostInput {
        input: InputId,
        work: WorkLease,
        capacity: CapacityBundle,
    }
    impl Fixture {
        fn new(positions: usize, funded: usize, slots: usize) -> Self {
            Self::with_control_storage(
                positions,
                funded,
                slots,
                AdmissionStorage {
                    loading: ResourceVector::default(),
                    accepted: ResourceVector::default(),
                    context: ResourceVector::default(),
                    ticket: ResourceVector::default(),
                },
                ResourceVector::default(),
            )
        }
        fn with_control_storage(
            positions: usize,
            funded: usize,
            slots: usize,
            admission_storage: AdmissionStorage,
            additional: ResourceVector,
        ) -> Self {
            Self::with_scopes(positions, funded, slots, admission_storage, additional, 1)
        }
        fn with_scopes(
            positions: usize,
            funded: usize,
            slots: usize,
            admission_storage: AdmissionStorage,
            additional: ResourceVector,
            scopes: usize,
        ) -> Self {
            Self::with_geometry(
                positions,
                funded,
                slots,
                admission_storage,
                additional,
                scopes,
                1,
            )
        }
        fn with_geometry(
            positions: usize,
            funded: usize,
            slots: usize,
            admission_storage: AdmissionStorage,
            additional: ResourceVector,
            scopes: usize,
            actors: usize,
        ) -> Self {
            Self::with_geometry_records(
                positions,
                funded,
                slots,
                admission_storage,
                additional,
                scopes,
                actors,
                16,
            )
        }
        fn with_geometry_records(
            positions: usize,
            funded: usize,
            slots: usize,
            admission_storage: AdmissionStorage,
            additional: ResourceVector,
            scopes: usize,
            actors: usize,
            reservations: usize,
        ) -> Self {
            let mut budget = PROCESS_CONTROL_BOOTSTRAP_STORAGE;
            for _ in 0..funded {
                budget = budget.checked_add(CONTROL_REQUEST_STORAGE).unwrap();
            }
            let ledger = RuntimeLedger::new(
                budget
                    .resources()
                    .unwrap()
                    .checked_add(&additional)
                    .unwrap(),
                reservations,
                scopes,
            );
            let runtime = ActorRuntime::new(actors);
            let core = ControlCore::new(
                ControlConfig {
                    max_contexts: 2,
                    max_waiters: 4,
                    max_pending_requests: 4,
                    step_budget: ControlCore::MAX_STEP_WORK,
                    retained_contexts: 1,
                    gone_fences: 1,
                    retention: Duration::from_secs(10),
                },
                AdmissionTicketConfig::new(1, Duration::from_secs(5), Duration::from_secs(10), 1)
                    .unwrap(),
                [9; 8],
                MonotonicInstant::ORIGIN,
            )
            .unwrap();
            let capability = core.admission().capability();
            let root = ProcessControlOwner::bootstrap(
                &runtime,
                &ledger,
                core,
                ProcessControlBinding {
                    actor: actor(),
                    generation: generation(),
                    max_work: 16,
                    max_slots: slots,
                    request_positions: NonZeroUsize::new(positions).unwrap(),
                    actor_config: ActorConfig {
                        mailbox: [4, 4, 4, 1],
                        max_batches: 8,
                        max_effects_per_step: ControlCore::MAX_STEP_WORK,
                        max_dependencies_per_effect: ControlCore::MAX_STEP_WORK,
                        turn_budget: 64,
                        max_step_work: ControlCore::MAX_STEP_WORK,
                    },
                    admission_storage,
                },
            )
            .unwrap();
            let context = QueryContextRef::new(
                QueryExecutionId::new(QueryId::new(7001, 7002), AttemptId::new(1).unwrap())
                    .unwrap(),
                FrontendProcessId::new_v7(),
                BackendProcessId::new_v7(),
            );
            Self {
                runtime,
                ledger,
                root,
                requests: BTreeMap::new(),
                capability,
                context,
            }
        }
        fn admit(&mut self) -> (InputId, ControlRequestHandle) {
            self.admit_operation(operation())
        }
        fn admit_operation(
            &mut self,
            operation: TaskOperationId,
        ) -> (InputId, ControlRequestHandle) {
            let (custody, handle) = ControlRequestCustody::admit(
                &self.root,
                ActorId::new(2).unwrap(),
                operation,
                MonotonicInstant::from_origin(Duration::from_secs(10)),
            )
            .unwrap();
            let input = custody.input_id();
            assert!(self.requests.insert(input, custody).is_none());
            (input, handle)
        }
        fn command(&self) -> ControlInput {
            ControlInput::Acquire {
                request: AcquireQueryContextAdmissionTicket::new(
                    operation(),
                    self.context,
                    // Core's five-second ticket limit produces the genuine
                    // neutral AdmissionReply without an unpaid ticket holder.
                    LeaseValidFor::new(Duration::from_secs(6)).unwrap(),
                    NativeCompatibilityId::new([3; 32]),
                    self.capability,
                ),
                ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
            }
        }
        fn submit(&mut self, input: InputId) {
            let command = self.command();
            self.requests
                .get_mut(&input)
                .unwrap()
                .submit(command)
                .unwrap();
        }
        fn body(&mut self, input: InputId) {
            let ready = self
                .runtime
                .ready_effects(actor(), input, ControlCore::MAX_STEP_WORK)
                .unwrap();
            assert_eq!(ready.len(), 1);
            let ticket = self.runtime.take_effect(actor(), input, ready[0]).unwrap();
            self.complete_body(ticket);
        }
        fn complete_body(&self, ticket: EffectTicket<WorkerAction>) {
            let input = ticket.input;
            let completed = ticket.descriptor.id;
            self.runtime.effect_completed(ticket).unwrap();
            for (descriptor, _) in self
                .runtime
                .effect_inventory(actor(), input, None, ControlCore::MAX_STEP_WORK)
                .unwrap()
            {
                if descriptor.dependencies.contains(&completed) {
                    self.runtime
                        .acknowledge_dependency(actor(), input, descriptor.id, completed)
                        .unwrap();
                }
            }
        }
        fn reply(&self, input: InputId) -> EffectTicket<WorkerAction> {
            let ready = self
                .runtime
                .ready_effects(actor(), input, ControlCore::MAX_STEP_WORK)
                .unwrap();
            assert_eq!(ready.len(), 1);
            self.runtime.take_effect(actor(), input, ready[0]).unwrap()
        }
        fn ordered_reply(
            &self,
            dispatch: &mut crate::control_runtime::effect_dispatch::EffectDispatch,
            input: InputId,
        ) -> EffectTicket<WorkerAction> {
            dispatch.index_committed(&self.runtime, 64).unwrap();
            let ticket = dispatch.take_ready(&self.runtime, 64).unwrap().unwrap();
            assert_eq!(ticket.input, input);
            ticket
        }
        fn complete_ordered_body(
            &self,
            dispatch: &mut crate::control_runtime::effect_dispatch::EffectDispatch,
            ticket: EffectTicket<WorkerAction>,
        ) {
            let reference = crate::control_runtime::effect_dispatch::EffectRef {
                input: ticket.input,
                action: ticket.descriptor.id,
            };
            self.complete_body(ticket);
            dispatch.completion_observed(reference).unwrap();
        }
        fn retire(&mut self, input: InputId) -> bool {
            let retired = self.requests.get_mut(&input).unwrap().try_retire().unwrap();
            if retired {
                drop(self.requests.remove(&input));
            }
            retired
        }
        fn host_input(&self, command: ControlInput, storage: StorageNeed) -> HostInput {
            let scope = &self.root.resources().scope;
            let (mut work, capacity) = scope
                .reserve_next(
                    scope.gates().unwrap().generation,
                    storage.resources().unwrap(),
                    WorkKind::Step,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(WorkKind::Step)
                .unwrap();
            work.start().unwrap();
            let permit = self
                .runtime
                .reserve_input(actor(), ActorId::new(3).unwrap(), InputClass::Control)
                .unwrap();
            permit.bind().unwrap();
            let input = permit
                .fill(WorkerMessage::Control(ControlMessage::Command {
                    work: work.id(),
                    request: None,
                    command,
                }))
                .unwrap();
            HostInput {
                input,
                work,
                capacity,
            }
        }
        fn finish_host_input(&self, mut host: HostInput) {
            self.runtime.retire_input(actor(), host.input).unwrap();
            // This actual neutral producer and its consumed batch have ended.
            // No timer, foreign capture or asynchronous child is inferred gone.
            host.work.body_exited().unwrap();
            host.work.captures_exited().unwrap();
            host.capacity
                .release(&host.capacity.resources().unwrap())
                .unwrap();
            self.ledger
                .reap_scope_ready(self.root.resources().scope.id(), 16)
                .unwrap();
        }
        fn publish_route(
            &self,
            dispatch: &mut crate::control_runtime::effect_dispatch::EffectDispatch,
            ticket: EffectTicket<WorkerAction>,
            route: &novarocks_control_runtime::publication::PublishedSnapshot<
                novarocks_execution_contract::QueryContextState,
            >,
        ) {
            let state = match &ticket.effect {
                WorkerAction::Control {
                    effect: ControlEffect::Route { context, state, .. },
                    ..
                } if *context == self.context => *state,
                _ => panic!("the actual directory route must precede its reply"),
            };
            drop(
                self.runtime
                    .commit_snapshot(&ticket, route, Arc::new(state))
                    .unwrap(),
            );
            dispatch
                .publication_committed(&self.runtime, &ticket)
                .unwrap();
            self.complete_body(ticket);
        }
        fn finish(self) {
            assert!(self.requests.is_empty());
            assert!(self.root.resources().requests.is_empty());
            assert_eq!(self.root.request_ready().registered(), 0);
            let (_service, mut resources) = self.root.into_parts();
            assert!(resources.tickets.is_empty());
            resources.scope.close_external().unwrap();
            self.runtime.close_actor_admission(actor()).unwrap();
            while let Some(ready) = self.runtime.claim_effect_actor() {
                self.runtime.release_effect_actor(ready, false).unwrap();
            }
            self.runtime.seal_actor(actor()).unwrap();
            for role in resources.roles.iter_mut().rev() {
                role.work.body_exited().unwrap();
                role.work.captures_exited().unwrap();
            }
            self.ledger
                .reap_scope_ready(resources.scope.id(), 32)
                .unwrap();
            drop(self.runtime.remove_actor(actor()).unwrap());
            for role in resources.roles.iter_mut().rev() {
                role.capacity
                    .release(&role.capacity.resources().unwrap())
                    .unwrap();
            }
            self.ledger.remove_scope(resources.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
        }
    }

    #[test]
    fn actual_once_reply_keeps_its_fee_until_the_delivery_guard_really_exits() {
        let mut f = Fixture::new(1, 1, 2);
        let (input, mut handle) = f.admit();
        assert_eq!(handle.request().token, input);
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 1);
        assert!(!f.retire(input));
        f.submit(input);
        f.runtime.poll_one(Tick(1)).unwrap();
        f.body(input);
        let reply = f.reply(input);
        f.requests.get_mut(&input).unwrap().publish(reply).unwrap();
        assert!(!f.retire(input));
        assert!(f.runtime.input_custody(actor(), input).unwrap().is_none());
        assert_eq!(
            f.requests[&input]
                .report
                .consumption(core_consumer())
                .unwrap(),
            Consumption::Pending
        );
        let delivery = handle.try_take().unwrap();
        assert!(matches!(
            delivery.body(),
            ControlReplyBody::Admission(Err(OperationOutcome::InvalidStateOrRequest))
        ));
        assert!(handle.try_take().is_none());
        drop(handle);
        assert!(
            !f.retire(input),
            "taking moves the original external obligation into delivery"
        );
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 1);
        let observed_runtime = f.runtime.change_version();
        drop(delivery);
        assert_ne!(f.runtime.change_version(), observed_runtime);
        assert!(f.retire(input));
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 0);
        f.finish();
    }

    #[test]
    fn external_cancel_does_not_retire_the_internal_rpc_or_finance_another_request() {
        let mut f = Fixture::new(2, 1, 3);
        let (input, handle) = f.admit();
        let observed = f.root.request_ready().change_version();
        let observed_runtime = f.runtime.change_version();
        drop(handle);
        assert_ne!(f.root.request_ready().change_version(), observed);
        assert_ne!(f.runtime.change_version(), observed_runtime);
        assert_eq!(f.root.request_ready().next_ready(), Some(input));
        assert_eq!(f.root.request_ready().next_ready(), None);
        assert!(!f.retire(input));
        let refusal = ControlRequestCustody::admit(
            &f.root,
            ActorId::new(2).unwrap(),
            operation(),
            MonotonicInstant::from_origin(Duration::from_secs(10)),
        )
        .err()
        .unwrap();
        assert!(matches!(refusal.error, ControlRequestError::Runtime(_)));
        assert!(
            refusal.partial.is_none(),
            "actual ledger quota denial preceded every new holder"
        );
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 1);
        f.submit(input);
        f.runtime.poll_one(Tick(1)).unwrap();
        f.body(input);
        let reply = f.reply(input);
        f.requests.get_mut(&input).unwrap().publish(reply).unwrap();
        assert!(
            f.retire(input),
            "unread neutral outbox is disposed after the actual final effect"
        );
        assert_eq!(f.root.request_ready().registered(), 0);
        f.finish();
    }

    #[test]
    fn an_immutable_same_operation_result_never_authorizes_another_waiters_reply() {
        let mut f = Fixture::new(2, 2, 3);
        let (first, mut handle_a) = f.admit();
        let (second, mut handle_b) = f.admit();
        assert_ne!(first, second);
        f.submit(first);
        f.submit(second);
        f.runtime.poll_one(Tick(1)).unwrap();
        f.body(first);
        f.body(second);
        let reply_a = f.reply(first);
        let refused = f
            .requests
            .get_mut(&second)
            .unwrap()
            .publish(reply_a)
            .err()
            .unwrap();
        let ticket = match refused {
            ControlRequestPublishRefusal::Rejected {
                error: ControlRequestError::WrongRequest,
                ticket,
            } => ticket,
            other => {
                panic!("the same operation must not replace an original waiter token: {other:?}")
            }
        };
        f.requests.get_mut(&first).unwrap().publish(ticket).unwrap();
        let reply_b = f.reply(second);
        f.requests
            .get_mut(&second)
            .unwrap()
            .publish(reply_b)
            .unwrap();
        let delivery_a = handle_a.try_take().unwrap();
        let delivery_b = handle_b.try_take().unwrap();
        assert_eq!(delivery_a.body(), delivery_b.body());
        drop(delivery_a);
        assert!(f.retire(first));
        assert!(!f.retire(second));
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 1);
        drop(delivery_b);
        assert!(f.retire(second));
        drop((handle_a, handle_b));
        f.finish();
    }

    #[test]
    fn slot_denial_returns_the_actual_nonzero_partial_work_without_fake_consumption() {
        let f = Fixture::new(1, 1, 1);
        let refusal = ControlRequestCustody::admit(
            &f.root,
            ActorId::new(2).unwrap(),
            operation(),
            MonotonicInstant::from_origin(Duration::from_secs(10)),
        )
        .err()
        .unwrap();
        assert!(matches!(
            refusal.error,
            ControlRequestError::Runtime(RuntimeError::Scope(ScopeError::SlotCapacity))
        ));
        let mut partial = refusal.partial.unwrap();
        assert!(partial.report.is_none());
        assert!(partial.permit.is_none());
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 1);
        assert_eq!(f.root.request_ready().registered(), 0);
        // This original never-started work captured no command or external
        // endpoint. Observe its actual unused holder exit, then return its fee.
        partial.work.body_exited().unwrap();
        partial.work.captures_exited().unwrap();
        partial
            .capacity
            .release(&partial.capacity.resources().unwrap())
            .unwrap();
        f.ledger
            .reap_scope_ready(f.root.resources().scope.id(), 16)
            .unwrap();
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 0);
        drop(partial);
        f.finish();
    }

    #[test]
    fn matching_numeric_actor_operation_and_input_do_not_authorize_a_foreign_root_reply() {
        let mut first = Fixture::new(1, 1, 2);
        let mut second = Fixture::new(1, 1, 2);
        let (input_a, mut handle_a) = first.admit();
        let (input_b, mut handle_b) = second.admit();
        assert_eq!(input_a, input_b);
        first.submit(input_a);
        second.submit(input_b);
        first.runtime.poll_one(Tick(1)).unwrap();
        second.runtime.poll_one(Tick(1)).unwrap();
        first.body(input_a);
        second.body(input_b);
        let reply_a = first.reply(input_a);
        let refusal = second
            .requests
            .get_mut(&input_b)
            .unwrap()
            .publish(reply_a)
            .err()
            .unwrap();
        let ticket = match refusal {
            ControlRequestPublishRefusal::Rejected {
                error: ControlRequestError::Driver(DriverError::EffectTicketOriginMismatch),
                ticket,
            } => ticket,
            other => {
                panic!("a foreign root cannot publish through equal numeric identities: {other:?}")
            }
        };
        first
            .requests
            .get_mut(&input_a)
            .unwrap()
            .publish(ticket)
            .unwrap();
        let reply_b = second.reply(input_b);
        second
            .requests
            .get_mut(&input_b)
            .unwrap()
            .publish(reply_b)
            .unwrap();
        drop(handle_a.try_take().unwrap());
        drop(handle_b.try_take().unwrap());
        assert!(first.retire(input_a));
        assert!(second.retire(input_b));
        drop((handle_a, handle_b));
        first.finish();
        second.finish();
    }

    #[test]
    fn a_full_address_index_returns_the_original_reserved_input_and_report_for_unused_cleanup() {
        let mut f = Fixture::new(1, 2, 3);
        let (input, handle) = f.admit();
        let refusal = ControlRequestCustody::admit(
            &f.root,
            ActorId::new(2).unwrap(),
            operation(),
            MonotonicInstant::from_origin(Duration::from_secs(10)),
        )
        .err()
        .unwrap();
        assert!(matches!(refusal.error, ControlRequestError::ReadyFull));
        let mut partial = refusal.partial.unwrap();
        let permit = partial.permit.take().unwrap();
        let unused_input = permit.id();
        assert_ne!(unused_input, input);
        assert_eq!(f.root.request_ready().registered(), 1);
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 2);
        permit.revoke_unused().unwrap();
        let report = partial.report.take().unwrap();
        report.revoke_unused().unwrap();
        partial.work.body_exited().unwrap();
        partial.work.captures_exited().unwrap();
        report.body_exited().unwrap();
        report.captures_exited().unwrap();
        assert!(report.try_retire().unwrap());
        partial
            .capacity
            .release(&partial.capacity.resources().unwrap())
            .unwrap();
        f.ledger
            .reap_scope_ready(f.root.resources().scope.id(), 16)
            .unwrap();
        drop(partial);
        assert!(
            f.runtime
                .input_custody(actor(), unused_input)
                .unwrap()
                .is_none()
        );
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 1);
        drop(handle);
        f.submit(input);
        f.runtime.poll_one(Tick(1)).unwrap();
        f.body(input);
        let reply = f.reply(input);
        f.requests.get_mut(&input).unwrap().publish(reply).unwrap();
        assert!(f.retire(input));
        f.finish();
    }

    #[test]
    fn a_later_bootstrap_failure_input_cannot_retire_the_request_before_its_started_materialize_effect()
     {
        use crate::control_runtime::effect_dispatch::{EffectDispatch, EffectRef};
        use novarocks_control_runtime::publication::PublishedSnapshot;
        use novarocks_control_runtime_core::effect::EffectState;
        use novarocks_execution_contract::{
            ContentFingerprint, CredentialEpoch, CredentialLeaseId, QueryContextState,
        };
        use novarocks_worker_protocol::{control::EstablishSemanticFacts, creation::PayloadId};

        let ticket_storage = StorageNeed {
            captures: 1,
            work: 1,
            ..StorageNeed::default()
        };
        let context_storage = StorageNeed {
            inputs: 1,
            batches: 1,
            effects: ControlCore::MAX_STEP_WORK,
            captures: 1,
            work: 1,
            ..StorageNeed::default()
        };
        let host_storage = StorageNeed {
            inputs: 1,
            batches: 1,
            effects: ControlCore::MAX_STEP_WORK,
            work: 1,
            ..StorageNeed::default()
        };
        // Each actual owner is funded independently of the original RPC.
        // ScopeCapacity below is a real materialization refusal, not a made-up
        // grant or an unexecuted host callback reported as complete.
        let additional = ticket_storage
            .resources()
            .unwrap()
            .checked_add(&context_storage.resources().unwrap())
            .unwrap()
            .checked_add(&host_storage.resources().unwrap())
            .unwrap()
            .checked_add(
                &ResourceVector::new([(Dimension::Tickets.id(), 1), (Dimension::Contexts.id(), 1)])
                    .unwrap(),
            )
            .unwrap();
        let mut f = Fixture::with_control_storage(
            2,
            2,
            3,
            AdmissionStorage {
                loading: ResourceVector::default(),
                accepted: ResourceVector::default(),
                context: context_storage.resources().unwrap(),
                ticket: ticket_storage.resources().unwrap(),
            },
            additional,
        );
        let mut dispatch =
            EffectDispatch::control(f.runtime.clone(), actor(), 8 * ControlCore::MAX_STEP_WORK);
        let (acquire, mut acquire_handle) = f.admit();
        let command = ControlInput::Acquire {
            request: AcquireQueryContextAdmissionTicket::new(
                operation(),
                f.context,
                LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
                NativeCompatibilityId::new([3; 32]),
                f.capability,
            ),
            ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
        };
        f.requests
            .get_mut(&acquire)
            .unwrap()
            .submit(command)
            .unwrap();
        f.runtime.poll_one(Tick(1)).unwrap();
        let grant = f.ordered_reply(&mut dispatch, acquire);
        let receipt = f.root.adopt_ticket(&grant).unwrap();
        assert_eq!(receipt.context(), f.context);
        f.complete_ordered_body(&mut dispatch, grant);
        let reply = f.ordered_reply(&mut dispatch, acquire);
        let reference = EffectRef {
            input: reply.input,
            action: reply.descriptor.id,
        };
        f.requests
            .get_mut(&acquire)
            .unwrap()
            .publish(reply)
            .unwrap();
        dispatch.completion_observed(reference).unwrap();
        let delivered = acquire_handle.try_take().unwrap();
        assert!(
            matches!(delivered.body(), ControlReplyBody::Admission(Ok(actual)) if *actual == receipt)
        );
        drop(delivered);
        assert!(f.retire(acquire));
        drop(acquire_handle);
        assert_eq!(f.ledger.used(Dimension::Tickets.id()), 1);
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 0);

        let mut bytes = [12; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        let operation = TaskOperationId::try_from_bytes(bytes).unwrap();
        let (establish, external) = f.admit_operation(operation);
        let context = f.context;
        let establish_command = || ControlInput::Establish {
            request: operation,
            context,
            ticket: receipt.ticket_id(),
            payload: PayloadId(1),
            semantics: EstablishSemanticFacts {
                catalog: ContentFingerprint::from_bytes([1; 16]),
                shared_filter: ContentFingerprint::from_bytes([2; 16]),
                query_options: ContentFingerprint::from_bytes([3; 16]),
                credential_lease: CredentialLeaseId::new(1),
                credential_epoch: CredentialEpoch::new(1).unwrap(),
                initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
            },
            generation: generation(),
        };
        f.requests
            .get_mut(&establish)
            .unwrap()
            .submit(establish_command())
            .unwrap();
        drop(external);
        f.runtime.poll_one(Tick(2)).unwrap();
        let materialize = f.ordered_reply(&mut dispatch, establish);
        let reservation = match &materialize.effect {
            WorkerAction::Control {
                effect:
                    ControlEffect::Materialize {
                        context,
                        reservation,
                        ..
                    },
                admitted: Some(original),
            } if *context == f.context && reservation == original => *reservation,
            _ => panic!("the real establish must issue its own materialization guard"),
        };
        let guard = f
            .runtime
            .take_step_reservation(&materialize, reservation)
            .unwrap();
        let (mut context_work, mut context_capacity) = guard.bind(WorkKind::Business).unwrap();
        context_work.start().unwrap();
        let refusal = f
            .ledger
            .open_child_scope_with_root(&context_work, generation(), 1, 1, WorkKind::Business)
            .err()
            .expect("the actual single-scope fixture refuses a new Context scope");
        assert_eq!(refusal, RuntimeError::ScopeCapacity);
        let route = PublishedSnapshot::new(
            &f.root.resources().scope,
            Arc::new(QueryContextState::Absent),
        );
        let materialized = f.host_input(
            ControlInput::ContextBootstrapFailed {
                context: f.context,
                refusal: novarocks_worker_protocol::creation::AdmissionRefusal::Verdict(
                    OperationOutcome::ResourceExhausted,
                ),
            },
            host_storage,
        );
        assert_ne!(materialized.input, establish);
        f.runtime.poll_one(Tick(3)).unwrap();
        let route_ticket = f.ordered_reply(&mut dispatch, materialized.input);
        // A later external waiter arrives after the pure result is ready but
        // before its original route publication. It owns a separate paid RPC.
        let (joined, joined_external) = f.admit_operation(operation);
        f.requests
            .get_mut(&joined)
            .unwrap()
            .submit(establish_command())
            .unwrap();
        drop(joined_external);
        assert!(f.runtime.poll_one(Tick(3)).unwrap());
        dispatch.index_committed(&f.runtime, 64).unwrap();
        assert!(dispatch.take_ready(&f.runtime, 64).unwrap().is_none());
        assert_eq!(*route.read(), QueryContextState::Absent);
        f.publish_route(&mut dispatch, route_ticket, &route);
        assert_eq!(*route.read(), QueryContextState::Aborting);
        let body = f.ordered_reply(&mut dispatch, materialized.input);
        f.complete_ordered_body(&mut dispatch, body);
        let final_reply = f.ordered_reply(&mut dispatch, materialized.input);
        let reference = EffectRef {
            input: final_reply.input,
            action: final_reply.descriptor.id,
        };
        assert_eq!(final_reply.input, materialized.input);
        assert!(
            matches!(&final_reply.effect, WorkerAction::ControlReply(reply)
            if reply.request.token == establish && reply.request.operation == operation)
        );
        f.requests
            .get_mut(&establish)
            .unwrap()
            .publish(final_reply)
            .unwrap();
        dispatch.completion_observed(reference).unwrap();
        f.finish_host_input(materialized);
        let joined_body = f.ordered_reply(&mut dispatch, joined);
        let routed_request = match &joined_body.effect {
            WorkerAction::Control {
                effect: ControlEffect::RouteEstablish { context, request },
                ..
            } if *context == f.context => request.request,
            _ => panic!("the new RPC must route, not inherit the creation failure"),
        };
        // This fixture's actual Context-scope bootstrap failed above. The
        // neutral route is handed off before the creator's independent
        // unavailable-entity verdict returns; no Context or I/O exit is faked.
        f.complete_ordered_body(&mut dispatch, joined_body);
        assert!(!f.retire(joined));
        let routed_result = f.host_input(
            ControlInput::RoutedEstablishFinished {
                context: f.context,
                request: routed_request,
                result: Ok(OperationOutcome::InvalidStateOrRequest),
                decided_at: MonotonicInstant::from_origin(Duration::from_nanos(3)),
            },
            host_storage,
        );
        f.runtime.poll_one(Tick(3)).unwrap();
        let joined_reply = f.ordered_reply(&mut dispatch, routed_result.input);
        assert!(
            matches!(&joined_reply.effect, WorkerAction::ControlReply(reply)
            if reply.request == routed_request && reply.result == RequestResult::Outcome(OperationOutcome::InvalidStateOrRequest))
        );
        let reference = EffectRef {
            input: joined_reply.input,
            action: joined_reply.descriptor.id,
        };
        f.requests
            .get_mut(&joined)
            .unwrap()
            .publish(joined_reply)
            .unwrap();
        dispatch.completion_observed(reference).unwrap();
        f.finish_host_input(routed_result);
        assert!(f.retire(joined));

        assert!(f.requests[&establish].endpoint.reply.is_committed());
        assert!(
            f.runtime
                .effect_inventory(actor(), establish, None, ControlCore::MAX_STEP_WORK)
                .unwrap()
                .iter()
                .any(
                    |(descriptor, state)| descriptor.id == materialize.descriptor.id
                        && *state == EffectState::Started
                )
        );
        assert!(
            !f.retire(establish),
            "final Reply and external cancel cannot finish the original Started effect"
        );
        assert!(
            f.runtime
                .input_custody(actor(), establish)
                .unwrap()
                .is_some()
        );
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 1);
        assert_eq!(
            f.requests[&establish]
                .report
                .consumption(core_consumer())
                .unwrap(),
            Consumption::Pending
        );
        assert_eq!(
            f.requests[&establish]
                .report
                .consumption(external_consumer())
                .unwrap(),
            Consumption::Pending
        );
        while let Some(ready) = f.root.request_ready().next_ready() {
            assert_eq!(ready, establish);
        }
        let observed = f.root.request_ready().change_version();
        f.runtime.effect_completed(materialize).unwrap();
        f.requests[&establish].note_progress();
        assert_ne!(f.root.request_ready().change_version(), observed);
        assert_eq!(f.root.request_ready().next_ready(), Some(establish));
        assert!(f.retire(establish));
        assert!(
            f.runtime
                .input_custody(actor(), establish)
                .unwrap()
                .is_none()
        );
        assert_eq!(f.ledger.used(Dimension::Replies.id()), 0);

        // No Context scope was created. The actual failed host has no child
        // left to stop, so its retained/removal facts can retire only the real
        // ticket and metadata holders elected by their corresponding effects.
        let retained = f.host_input(ControlInput::Retained { context: f.context }, host_storage);
        f.runtime.poll_one(Tick(4)).unwrap();
        let retire_ticket = f.ordered_reply(&mut dispatch, retained.input);
        assert!(f.root.retire_ticket(&retire_ticket).unwrap());
        f.complete_ordered_body(&mut dispatch, retire_ticket);
        let retained_route = f.ordered_reply(&mut dispatch, retained.input);
        f.publish_route(&mut dispatch, retained_route, &route);
        assert_eq!(*route.read(), QueryContextState::TerminalRetained);
        f.finish_host_input(retained);
        assert_eq!(f.ledger.used(Dimension::Tickets.id()), 0);
        drop(route);
        let removed = f.host_input(
            ControlInput::HostRemoved { context: f.context },
            host_storage,
        );
        f.runtime.poll_one(Tick(5)).unwrap();
        let retire_context = f.ordered_reply(&mut dispatch, removed.input);
        f.runtime.validate_effect_ticket(&retire_context).unwrap();
        assert!(matches!(&retire_context.effect, WorkerAction::Control {
            effect: ControlEffect::RetireReservation(actual), admitted: None,
        } if *actual == context_capacity.id()));
        context_work.body_exited().unwrap();
        context_work.captures_exited().unwrap();
        assert!(context_work.actual_exit_ready().unwrap());
        context_capacity
            .release(&context_capacity.resources().unwrap())
            .unwrap();
        f.complete_ordered_body(&mut dispatch, retire_context);
        f.finish_host_input(removed);
        assert_eq!(f.ledger.used(Dimension::Contexts.id()), 0);
        drop((context_work, context_capacity));
        assert!(dispatch.is_empty());
        f.finish();
    }
    mod establish_ingress;
}
