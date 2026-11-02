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

use super::request_endpoint::{RequestDelivery, RequestEndpoint, RequestHandle};
pub(crate) use super::request_ready::ControlRequestReady;
use super::request_ready::RequestExternalLeg;
use super::{
    actors::{ControlMessage, RequestMetadata, WorkerAction, WorkerActor, WorkerMessage},
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
    service::ContextHostAddress,
};
use crate::control_runtime_policy::{PolicyError, StorageNeed};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{
        ActorRegistration, ActorRuntime, DriverError, EffectCompletionRefusal, EffectTicket,
        InputAuthor, InputPermit, InputRoleId,
    },
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    header_readiness::{
        HeaderReadyError, HeaderReadyNotice, HeaderReadyRegistration, HeaderWaitState,
    },
    lease::{ReservationBundle, WorkLease, WorkRetirementObserver},
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
    requests: 1,
    inputs: 1,
    batches: 1,
    effects: ControlCore::MAX_STEP_WORK,
    cuts: 0,
    replies: 1,
    completions: 1,
    captures: 4 + super::request_observation::REQUEST_OBSERVATION_CAPTURES,
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
    captures: CONTROL_REQUEST_STORAGE.captures + 1 + CONTEXT_ROUTE_ESCROW_STORAGE.captures,
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
    HeaderReady(HeaderReadyError),
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
    MissingObservationPort,
    BodyResultMismatch,
    AdmissionReceiptMismatch,
    SubmissionUnresolved,
    IngressNotEnabled,
    IngressPending,
    IngressUnresolved,
    Route(EstablishRouteError),
    HostJob(ContextHostJobError),
}
impl std::fmt::Display for ControlRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Policy(error) => write!(f, "Control request storage policy is invalid: {error}"),
            Self::Runtime(error) => write!(f, "Control request role operation failed: {error}"),
            Self::Driver(error) => write!(
                f,
                "Control request input or publication operation failed: {error}"
            ),
            Self::HeaderReady(error) => write!(
                f,
                "Control request original header readiness failed: {error}"
            ),
            Self::Binding(error) => write!(
                f,
                "Control request original reported work binding failed: {error}"
            ),
            Self::Exit(error) => write!(
                f,
                "Control request physical exit observation failed: {error}"
            ),
            Self::ReadyFull => f.write_str("Control request ready positions are full"),
            Self::DuplicateReadySlot => {
                f.write_str("Control request ready position is already registered")
            }
            Self::UnknownReadySlot => f.write_str("Control request ready position is unknown"),
            Self::WrongRoot => f.write_str("Control request does not belong to the original Root"),
            Self::WrongOperation => {
                f.write_str("Control request operation identity does not match")
            }
            Self::WrongRequest => {
                f.write_str("Control request original request identity does not match")
            }
            Self::WrongEffect => f.write_str("Control request received the wrong effect"),
            Self::WrongPhase => f.write_str("Control request is in the wrong lifecycle phase"),
            Self::MissingBody => f.write_str("Control request original body is missing"),
            Self::MissingObservationPort => {
                f.write_str("Control request observation port is missing")
            }
            Self::BodyResultMismatch => {
                f.write_str("Control request body result differs from its original receipt")
            }
            Self::AdmissionReceiptMismatch => {
                f.write_str("Control request admission receipt does not match")
            }
            Self::SubmissionUnresolved => {
                f.write_str("Control request submission remains unresolved")
            }
            Self::IngressNotEnabled => f.write_str("Control request ingress is not enabled"),
            Self::IngressPending => {
                f.write_str("Control request ingress still has a pending invocation")
            }
            Self::IngressUnresolved => f.write_str("Control request ingress remains unresolved"),
            Self::Route(error) => {
                write!(f, "Control request establishment route failed: {error:?}")
            }
            Self::HostJob(error) => write!(f, "Control request Context host job failed: {error}"),
        }
    }
}
impl std::error::Error for ControlRequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Policy(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Driver(error) => Some(error),
            Self::HeaderReady(error) => Some(error),
            Self::Binding(error) => Some(error),
            Self::Exit(error) => Some(error),
            Self::HostJob(error) => Some(error),
            Self::ReadyFull
            | Self::DuplicateReadySlot
            | Self::UnknownReadySlot
            | Self::WrongRoot
            | Self::WrongOperation
            | Self::WrongRequest
            | Self::WrongEffect
            | Self::WrongPhase
            | Self::MissingBody
            | Self::MissingObservationPort
            | Self::BodyResultMismatch
            | Self::AdmissionReceiptMismatch
            | Self::SubmissionUnresolved
            | Self::IngressNotEnabled
            | Self::IngressPending
            | Self::IngressUnresolved
            | Self::Route(_) => None,
        }
    }
}

/// A refusal before command capture retains each actually created original.
/// In particular, a scope error does not authorize an unused-exit inference.
#[must_use = "Retain or actually dispose every original partial admission"]
pub(crate) struct ControlRequestPartial {
    pub work: WorkLease,
    pub capacity: ReservationBundle,
    pub report: Option<ReportSlot>,
    pub permit: Option<InputPermit<WorkerActor>>,
    pub route_escrow: Option<ReservationBundle>,
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
        _original_body: Arc<ControlReplyBody>,
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

type Endpoint = RequestEndpoint<ControlReplyBody>;
pub(crate) type ControlRequestHandle = RequestHandle<ControlReplyBody>;
pub(crate) type ControlRequestDelivery = RequestDelivery<ControlReplyBody>;
pub(super) use super::request_endpoint::RequestNotice as ControlRequestNotice;

#[must_use = "The original request remains owned on ingress refusal"]
pub(crate) struct EstablishIngressBeginRefusal {
    pub error: ControlRequestError,
    _original_request: std::mem::ManuallyDrop<EstablishQueryContext>,
}
impl EstablishIngressBeginRefusal {
    pub(super) fn new(error: ControlRequestError, request: EstablishQueryContext) -> Self {
        Self {
            error,
            _original_request: std::mem::ManuallyDrop::new(request),
        }
    }
}
impl std::fmt::Debug for EstablishIngressBeginRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EstablishIngressBeginRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy)]
enum RequestKind {
    ObserveTicket,
    CreateWithoutContext,
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
/// Root already retains the same original route and return position. This
/// transfer owns only the target address and neutral Context half; the driver
/// releases its Root inventory guard before binding the target inventory.
#[must_use = "Bind the same original transfer outside the Root inventory lock"]
pub(crate) struct PreparedEstablishRoute {
    address: ContextHostAddress,
    route: Arc<EstablishRouteCustody>,
    initialization: Result<(), EstablishRouteError>,
}
impl PreparedEstablishRoute {
    pub(crate) fn bind(self) -> Result<(), ControlRequestError> {
        self.initialization.map_err(ControlRequestError::Route)?;
        let mut host = self.address.lock().ok_or(ControlRequestError::Route(
            EstablishRouteError::TargetRemoved,
        ))?;
        host.register_establish_route(self.route)
            .map_err(ControlRequestError::Route)
    }
}

#[must_use = "The actual Root must retain the request until its original retirement"]
pub(crate) struct ControlRequestCustody {
    runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    actor: ActorId,
    work: WorkId,
    author: InputAuthor,
    work_retirement: WorkRetirementObserver,
    // Minted once from this actual fully admitted Work before ExitRole takes
    // custody. Lazy arm and exact notices grant no Work or physical exit.
    header_ready: HeaderReadyRegistration,
    header_owner: ActorRegistration,
    endpoint: Arc<Endpoint>,
    notification: novarocks_control_runtime::reply_notification::ReplyNotification<WorkerActor>,
    pending_notification: Option<super::request_endpoint::RequestNotificationJob>,
    protocol_observation: super::request_observation::RequestObservation,
    pending_protocol_observation: Option<super::request_observation::RequestObservationJob>,
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
    pending_comparison: Option<super::establish_route::EstablishComparisonJob>,
    ingress_owner: Option<EstablishIngressOwner>,
    ingress_partial: Option<EstablishIngressPartial>,
    ingress_return: Option<OwnedReturnReader<EstablishIngressReturned>>,
    route_escrow: Option<ReservationBundle>,
    route: Option<Arc<EstablishRouteCustody>>,
    creator: Option<ContextHostJobs>,
    creator_retirement: Option<WorkRetirementObserver>,
    creator_return_role: Option<InputRoleId>,
    creator_source_consumed: bool,
    route_cancelled: bool,
    route_issued: bool,
    pending_route: Option<ValidatedEstablishRoute>,
    withdrawal: bool,
}
impl ControlRequestCustody {
    /// Observe the original pure ticket owner through this same admitted R.
    /// This read neither enters the operation replay table nor redeems a grant.
    pub(crate) fn submit_ticket_observation(
        &mut self,
        ticket: novarocks_execution_contract::AdmissionTicketId,
        context: QueryContextRef,
        preflight: bool,
    ) -> Result<(), ControlRequestError> {
        if self.submitted || self.started || self.ingress_enabled || self.permit.is_none() {
            return Err(ControlRequestError::WrongPhase);
        }
        self.authority
            .as_mut()
            .expect("original observation R authority")
            .start()
            .map_err(ControlRequestError::Exit)?;
        self.started = true;
        self.permit
            .as_ref()
            .expect("original observation R input")
            .bind()
            .map_err(ControlRequestError::Driver)?;
        self.kind = Some(RequestKind::ObserveTicket);
        let message = WorkerMessage::Control(ControlMessage::ObserveTicket {
            work: self.author.work(),
            request: RequestMetadata {
                operation: self.request().operation,
                deadline: self.request().deadline,
            },
            ticket,
            context,
            preflight,
        });
        match self
            .permit
            .take()
            .expect("same original observation input")
            .fill(message)
        {
            Ok(input) => {
                assert_eq!(input, self.input_id());
                self.submitted = true;
                Ok(())
            }
            Err((error, original)) => {
                self.failed_submission = Some(original);
                Err(ControlRequestError::Driver(error))
            }
        }
    }
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
        let (mut work, mut capacity) = match guard.bind(WorkKind::Step) {
            Ok(bound) => bound,
            // A fresh same-kind bind has no ordinary admission refusal after
            // reservation. No command or external payload has been captured;
            // shared admission corruption cannot become a request verdict.
            Err(_error) => std::process::abort(),
        };
        let report = match ReportSlot::reserve_next(
            &mut capacity,
            generation,
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
        let permit = match root.runtime().reserve_admitted_input_for(
            root.registration(),
            source,
            InputClass::Ordinary,
            &work,
        ) {
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
        let author = permit.author().unwrap_or_else(|_| std::process::abort());
        let request = RequestContext {
            author: author.work(),
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
        if ingress_enabled {
            // Establish supervision starts at this actual admission boundary.
            // No ready address or raw getter is exposed yet. A concurrent stop
            // returns every original holder to the existing partial owner.
            if let Err(error) = work.start_in_generation(author.generation()) {
                return Err(ControlRequestAdmissionRefusal {
                    error: ControlRequestError::Runtime(error),
                    partial: Some(ControlRequestPartial {
                        work,
                        capacity,
                        report: Some(report),
                        permit: Some(permit),
                        route_escrow,
                    }),
                });
            }
        }
        let mut reported = match ReportedWork::try_new(work, capacity, report.clone()) {
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
        let slot = match root
            .request_ready()
            .register_control(request.token, root.registration())
        {
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
        let (notification, observer) =
            novarocks_control_runtime::reply_notification::ReplyNotification::for_admitted_work(
                root.runtime(),
                root.registration(),
                reported.work(),
            )
            .unwrap_or_else(|_| std::process::abort());
        let endpoint = Arc::new(Endpoint {
            request,
            ready: root.request_ready().clone(),
            reply: PublishedReply::new_for_input(&author),
            external: RequestExternalLeg::new(slot),
        });
        // All ordinary refusal points have returned before this unique mint.
        // The original bound Work is unexposed, body/captures are still live,
        // and no previous readiness registration could have been issued.
        // Failure here is shared admission corruption, not a new R verdict.
        let header_ready = reported
            .header_readiness_registration()
            .unwrap_or_else(|_| std::process::abort());
        let (role, authority) = ExitRole::from_admitted(reported);
        Ok((
            Self {
                runtime: root.runtime().clone(),
                ledger: root.ledger().clone(),
                scope: scope.clone(),
                actor: root.actor(),
                work: work_id,
                author,
                work_retirement,
                header_ready,
                header_owner: root.registration().clone(),
                endpoint: endpoint.clone(),
                notification,
                pending_notification: None,
                protocol_observation: Default::default(),
                pending_protocol_observation: None,
                permit: Some(permit),
                role,
                authority: Some(authority),
                report,
                kind: None,
                submitted: false,
                started: ingress_enabled,
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
                pending_comparison: None,
                ingress_owner: None,
                ingress_partial: None,
                ingress_return: None,
                route_escrow,
                route: None,
                creator: None,
                creator_retirement: None,
                creator_return_role: None,
                creator_source_consumed: false,
                route_cancelled: false,
                route_issued: false,
                pending_route: None,
                withdrawal: false,
            },
            ControlRequestHandle {
                endpoint,
                active: true,
                observer: Some(observer),
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
                _original_request: request,
            });
        }
        if self.submitted
            || self.ingress_issued
            || !self.started
            || self.authority.is_none()
            || self.role.phase() != ExitRolePhase::Started
        {
            return Err(EstablishIngressBeginRefusal {
                error: ControlRequestError::WrongPhase,
                _original_request: request,
            });
        }
        if request.envelope().operation_id() != self.request().operation {
            return Err(EstablishIngressBeginRefusal {
                error: ControlRequestError::WrongOperation,
                _original_request: request,
            });
        }
        // This already-started supervisor may derive its original Cleanup
        // source after stopping. The child's atomic parent gate still decides
        // whether the delayed getter may actually begin.
        self.ingress_issued = true;
        let generation = self.author.generation();
        let owner = match EstablishIngressOwner::bootstrap(
            self.authority.as_mut().expect("original RPC authority"),
            generation,
            &self.ledger,
            &self.scope,
            ControlRequestNotice(self.endpoint.external.slot().clone()),
        ) {
            Ok(owner) => owner,
            Err(refusal) => {
                self.ingress_partial = refusal.partial;
                return Err(EstablishIngressBeginRefusal {
                    error: ControlRequestError::Exit(refusal.error),
                    _original_request: request,
                });
            }
        };
        self.ingress_owner = Some(owner.clone());
        let (job, reader) = owner.capture(std::mem::ManuallyDrop::into_inner(request), payload);
        self.ingress_return = Some(reader);
        Ok(job)
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
        let outcome = self
            .ingress_owner
            .as_ref()
            .expect("original ingress child")
            .restore_getter_return(returned);
        self.ingress_outcome = Some(outcome);
        Ok(Some(outcome))
    }
    /// The actual child-owned getter return is independent of R's final report.
    pub(super) fn ingress_return_state(
        &self,
    ) -> Result<Option<novarocks_control_runtime_core::completion::ReturnState>, ControlRequestError>
    {
        self.ingress_return
            .as_ref()
            .map(|reader| reader.state().map_err(ControlRequestError::Runtime))
            .transpose()
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
            || ticket.descriptor.work != self.author.work()
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
                self.creator_return_role.or_else(|| {
                    self.creator
                        .as_ref()
                        .and_then(ContextHostJobs::creator_role)
                })
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
                    generation: _,
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
            || !jobs.matches_materialization(&self.runtime, ticket)
        {
            return Err(ControlRequestError::WrongRequest);
        }
        self.creator_retirement = Some(
            jobs.materialization_retirement(ticket)
                .map_err(ControlRequestError::HostJob)?,
        );
        self.creator = Some(jobs.clone());
        let role = jobs
            .bind_creator(
                ticket,
                self.request(),
                self.author.clone(),
                author,
                ControlRequestNotice(self.endpoint.external.slot().clone()),
            )
            .map_err(ControlRequestError::HostJob)?;
        self.creator_return_role = Some(role);
        Ok(())
    }
    pub(crate) fn take_consumed_creator_disposal(
        &self,
    ) -> Result<Option<EstablishDisposalJob>, ControlRequestError> {
        let consumed = self.creator_source_consumed
            || self
                .creator
                .as_ref()
                .map(ContextHostJobs::source_consumed)
                .transpose()
                .map_err(ControlRequestError::HostJob)?
                .unwrap_or(false);
        if !consumed {
            return Ok(None);
        }
        Ok(self
            .ingress_owner
            .as_ref()
            .and_then(EstablishIngressOwner::take_disposal))
    }
    pub(super) fn prepare_pending_establish_route(
        &mut self,
        address: ContextHostAddress,
        root_author: &WorkLease,
    ) -> Result<PreparedEstablishRoute, ControlRequestError> {
        let seed = self
            .pending_route
            .as_ref()
            .ok_or(ControlRequestError::WrongPhase)?;
        if address.context() != seed.context || seed.root != self.actor {
            return Err(ControlRequestError::WrongRequest);
        }
        let ingress = self
            .ingress_owner
            .as_ref()
            .ok_or(ControlRequestError::WrongPhase)?
            .clone();
        let escrow = self
            .route_escrow
            .take()
            .ok_or(ControlRequestError::WrongPhase)?;
        let route = match EstablishRouteCustody::new(
            self.runtime.clone(),
            self.ledger.clone(),
            self.actor,
            self.author.clone(),
            seed.context,
            address.registration().clone(),
            seed.request,
            escrow,
            ControlRequestNotice(self.endpoint.external.slot().clone()),
            ingress,
        ) {
            Ok(route) => route,
            Err((error, original)) => {
                self.route_escrow = Some(original);
                return Err(ControlRequestError::Route(match error {
                    EstablishRouteError::Driver(
                        DriverError::UnknownActor | DriverError::EffectTicketOriginMismatch,
                    ) => EstablishRouteError::TargetRemoved,
                    error => error,
                }));
            }
        };
        self.route = Some(route.clone());
        self.pending_route = None;
        let initialization = route.initialize_root(root_author);
        Ok(PreparedEstablishRoute {
            address,
            route,
            initialization,
        })
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
            // A never-bound leg admitted no Context Work or capture. An
            // admitted Context leg still exits through its actual owner.
            route
                .try_retire_unbound_context()
                .map_err(ControlRequestError::Route)?;
        }
        self.pending_route = None;
        self.route_cancelled = true;
        Ok(())
    }
    /// A replay can dispose its exclusive source only after its actual Context
    /// leg retires, including every comparison loan. A reply alone cannot.
    pub(crate) fn take_retired_route_disposal(&self) -> Option<EstablishDisposalJob> {
        if !self.route_issued
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
    fn advance_creator_retirement(&mut self) -> Result<bool, ControlRequestError> {
        if let Some(creator) = &self.creator {
            self.creator_source_consumed |= creator
                .source_consumed()
                .map_err(ControlRequestError::HostJob)?;
            let returned = creator
                .retire_creator()
                .map_err(ControlRequestError::HostJob)?;
            let retired = self
                .creator_retirement
                .as_ref()
                .is_some_and(WorkRetirementObserver::is_retired);
            if !returned || !retired {
                return Ok(false);
            }
            self.creator = None;
            self.creator_retirement = None;
            self.note_progress();
        }
        Ok(true)
    }
    fn close_route(&mut self) -> Result<bool, ControlRequestError> {
        if !self.advance_creator_retirement()? {
            return Ok(false);
        }
        if self.pending_route.is_some() {
            return Ok(false);
        }
        if let Some(route) = &self.route {
            route
                .try_retire_unbound_context()
                .map_err(ControlRequestError::Route)?;
        }
        if let Some(route) = &self.route
            && !route
                .try_retire_root()
                .map_err(ControlRequestError::Route)?
        {
            return Ok(false);
        }
        Ok(true)
    }
    #[cfg(test)]
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
    fn retire_original_role(&mut self) -> Result<bool, ControlRequestError> {
        // The upstream owner has observed its actual body/capture exit. The
        // canonical original-header gate also includes remaining children and
        // pins; arm before waiting on either, rather than losing their wake.
        if !self.work_retirement.is_retired()
            && !self
                .ledger
                .reap_observed_header(&self.work_retirement)
                .map_err(ControlRequestError::Runtime)?
        {
            match self
                .header_ready
                .arm(&self.runtime, &self.header_owner, &self.work_retirement)
                .map_err(ControlRequestError::HeaderReady)?
            {
                HeaderWaitState::Waiting => return Ok(false),
                HeaderWaitState::ReadyToReap => {
                    // Another original holder can race the observation. The
                    // armed cell already retains one exact retry; a failed
                    // reap creates neither an exit fact nor a busy loop.
                    if !self
                        .ledger
                        .reap_observed_header(&self.work_retirement)
                        .map_err(ControlRequestError::Runtime)?
                    {
                        return Ok(false);
                    }
                }
                HeaderWaitState::Retired => {}
            }
        }
        // Withdraw original readiness metadata before ExitRole can refund
        // Requests or the owner can remove its actual R address.
        self.header_ready.cancel();
        if let Some(escrow) = &mut self.route_escrow {
            escrow
                .release_quota(
                    &escrow
                        .resources()
                        .map_err(ControlRequestError::Runtime)?
                        .quota,
                )
                .and_then(|()| escrow.finish())
                .map_err(ControlRequestError::Runtime)?;
            self.route_escrow = None;
        }
        self.role.try_retire().map_err(ControlRequestError::Exit)
    }
    pub(super) fn header_work(&self) -> novarocks_control_runtime_core::identity::ScopedWorkRef {
        self.header_ready.work()
    }
    pub(super) fn notify_header_readiness(&self, notice: &HeaderReadyNotice) -> bool {
        if !self.header_ready.matches(notice) {
            return false;
        }
        self.note_progress();
        true
    }
    pub(super) fn is_retired(&self) -> bool {
        self.retired
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
        let returned = self
            .ingress_return_state()
            .map_err(|error| ControlRequestSubmitRefusal {
                error,
                command: Some(ControlInput::IngressOwnerFailed {
                    request: self.request().operation,
                }),
            })?
            .ok_or_else(|| ControlRequestSubmitRefusal {
                error: ControlRequestError::WrongPhase,
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
        let lost = self.authority.is_some()
            && self.started
            && self.role.phase() == ExitRolePhase::Started
            && self.ingress_source_phase() == Some(ExitRolePhase::Unresolved)
            && returned == ReturnState::Unknown;
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
        // Failure is the outcome of this same admitted request. Its original
        // permit remains authored by R, including when the getter was lost;
        // a process cleanup role cannot replace that financial identity.
        self.fill_original_command(command, self.author.work())
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
            ControlInput::CreateWithoutContextAddress { .. } => RequestKind::CreateWithoutContext,
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
        self.fill_original_command(command, self.author.work())
    }
    fn fill_original_command(
        &mut self,
        command: ControlInput,
        work: novarocks_control_runtime_core::identity::ScopedWorkRef,
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
    #[cfg(test)]
    pub(crate) fn publish(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), ControlRequestPublishRefusal> {
        self.publish_with_receipt(ticket).map(|_| ())
    }
    pub(crate) fn publish_with_receipt(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<
        novarocks_control_runtime::driver::CompletedEffect<WorkerActor>,
        ControlRequestPublishRefusal,
    > {
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
                    Some(RequestKind::CreateWithoutContext),
                    ControlReplyBody::CreateContext(_)
                    | ControlReplyBody::Verdict(RequestResult::Outcome(
                        OperationOutcome::OwnerFailed,
                    )),
                ) => {}
                (Some(RequestKind::CreateWithoutContext), _)
                | (_, ControlReplyBody::CreateContext(_)) => {
                    return Err(reject(ControlRequestError::WrongEffect, ticket));
                }
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
            let event = if matches!(self.kind, Some(RequestKind::Establish)) {
                super::request_observation::establish_event(self.request().operation, &decided_body)
            } else {
                None
            };
            let body = Arc::new(decided_body);
            if let Err((error, body)) =
                self.runtime
                    .commit_reply(&ticket, &self.endpoint.reply, body)
            {
                return Err(ControlRequestPublishRefusal::Publication {
                    error,
                    ticket,
                    _original_body: body,
                });
            }
            self.protocol_observation.stage(event);
            self.published = Some(key);
            self.notification.published();
        }
        match self.runtime.effect_completed_with_receipt(ticket) {
            Ok(completed) => {
                self.reply_completed = true;
                self.notification.published();
                self.endpoint.external.notify();
                Ok(completed)
            }
            Err(refusal) => Err(ControlRequestPublishRefusal::Completion(refusal)),
        }
    }
    #[cfg(test)]
    pub(crate) fn take_protocol_observation_for_test(
        &mut self,
        ports: &crate::TaskExecutionPorts,
    ) -> Option<super::request_observation::RequestObservationJob> {
        self.protocol_observation.take_job(
            ports,
            self.scope.id(),
            self.work,
            super::request_endpoint::RequestNotice(self.endpoint.external.slot().clone()),
        )
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
        if self.protocol_observation.failed() || self.notification.snapshot().failure.is_some() {
            return Err(ControlRequestError::SubmissionUnresolved);
        }
        if self.failed_submission.is_some() {
            return Err(ControlRequestError::SubmissionUnresolved);
        }
        if !self.advance_creator_retirement()? {
            return Ok(false);
        }
        if self.pending_getter.is_some() || self.pending_disposal.is_some() {
            return Ok(false);
        }
        if self.withdrawal {
            if !self.endpoint.external_ended()
                || !self.notification.is_idle()
                || !self.close_ingress()?
                || !self.close_route()?
            {
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
        if !self.reply_completed
            || !self.endpoint.external_ended()
            || !self.notification.is_idle()
            || !self.protocol_observation.is_idle()
        {
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
        capacity::{ReservationResources, ResourceVector},
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
        positions: usize,
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
        capacity: ReservationBundle,
    }
    impl Fixture {
        fn new(positions: usize, funded: usize) -> Self {
            Self::with_control_storage(
                positions,
                funded,
                AdmissionStorage {
                    loading: Default::default(),
                    accepted: Default::default(),
                    context: Default::default(),
                },
                ReservationResources::default(),
            )
        }
        fn with_control_storage(
            positions: usize,
            funded: usize,
            admission_storage: AdmissionStorage,
            additional: ReservationResources,
        ) -> Self {
            Self::with_control_storage_and_usage(
                positions,
                funded,
                admission_storage,
                additional,
                [],
            )
        }
        fn with_control_storage_and_usage(
            positions: usize,
            funded: usize,
            admission_storage: AdmissionStorage,
            additional: ReservationResources,
            owner_usage: impl IntoIterator<Item = novarocks_control_runtime_core::identity::DimensionId>,
        ) -> Self {
            let mut budget = PROCESS_CONTROL_BOOTSTRAP_STORAGE;
            for _ in 0..funded {
                budget = budget.checked_add(CONTROL_REQUEST_STORAGE).unwrap();
            }
            let ledger = RuntimeLedger::with_owner_usage_dimensions(
                budget
                    .resources()
                    .unwrap()
                    .quota
                    .checked_add(&additional.quota)
                    .unwrap(),
                owner_usage,
            )
            .unwrap();
            let runtime = ActorRuntime::new();
            let core = ControlCore::new(
                ControlConfig {
                    max_pending_requests: positions,
                    step_budget: ControlCore::MAX_STEP_WORK,
                },
                AdmissionTicketConfig::new(Duration::from_secs(5), Duration::from_secs(10))
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
                    request_positions: NonZeroUsize::new(positions).unwrap(),
                    actor_config: ActorConfig {
                        mailbox: [4, 4, 4, 1],
                        max_batches: 8,
                        max_effects_per_step:
                            crate::control_runtime::actors::ControlActor::MAX_STEP_ACTIONS,
                        max_dependencies_per_effect:
                            crate::control_runtime::actors::ControlActor::MAX_STEP_ACTIONS,
                        turn_budget: crate::control_runtime::actors::ControlActor::TURN_BUDGET,
                        max_step_work: crate::control_runtime::actors::ControlActor::MAX_STEP_WORK,
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
                positions,
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
            for _ in 0..ControlCore::MAX_STEP_WORK {
                let ticket = self.reply(input);
                let projection = matches!(&ticket.effect, WorkerAction::AdmissionEpoch(_));
                self.complete_body(ticket);
                if !projection {
                    return;
                }
            }
            panic!("the original bounded Root step must expose its actual body");
        }
        fn complete_body(&self, ticket: EffectTicket<WorkerAction>) {
            if let WorkerAction::AdmissionEpoch(projection) = &ticket.effect {
                self.runtime
                    .commit_snapshot(
                        &ticket,
                        &self.root.resources().admission_snapshot,
                        Arc::new(*projection),
                    )
                    .unwrap();
            }

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
        fn ordered_reply(&mut self, input: InputId) -> EffectTicket<WorkerAction> {
            self.root.index_control_effects(64).unwrap();
            let ticket = self.next_control_body();
            assert_eq!(ticket.input, input);
            ticket
        }
        fn next_control_body(&mut self) -> EffectTicket<WorkerAction> {
            for _ in 0..ControlCore::MAX_STEP_WORK {
                let ticket = self.root.take_control_effect(64).unwrap().unwrap();
                if matches!(&ticket.effect, WorkerAction::AdmissionEpoch(_)) {
                    let reference = crate::control_runtime::effect_dispatch::EffectRef {
                        input: ticket.input,
                        action: ticket.descriptor.id,
                    };
                    self.complete_body(ticket);
                    self.root.control_completion_observed(reference).unwrap();
                } else {
                    return ticket;
                }
            }
            panic!("the bounded Root publication prefix must expose its actual body");
        }
        fn complete_ordered_body(&mut self, ticket: EffectTicket<WorkerAction>) {
            let reference = crate::control_runtime::effect_dispatch::EffectRef {
                input: ticket.input,
                action: ticket.descriptor.id,
            };
            let requires_completion = self
                .root
                .resources()
                .dispatch
                .requires_completion(&ticket)
                .unwrap();
            self.complete_body(ticket);
            if requires_completion {
                self.root.control_completion_observed(reference).unwrap();
            }
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
                    work: work.reference(),
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
        fn finish_ordered_host_input(&mut self, host: HostInput) {
            self.root.index_control_effects(64).unwrap();
            for _ in 0..ControlCore::MAX_STEP_WORK {
                let original = self
                    .runtime
                    .effect_inventory(actor(), host.input, None, ControlCore::MAX_STEP_WORK)
                    .unwrap();
                if original.iter().all(|(_, state)| {
                    *state == novarocks_control_runtime_core::effect::EffectState::Completed
                }) {
                    self.finish_host_input(host);
                    return;
                }
                let ticket = self
                    .root
                    .take_control_effect(ControlCore::MAX_STEP_WORK)
                    .unwrap()
                    .expect("the original HostInput still owns its actual effect");
                match &ticket.effect {
                    WorkerAction::AdmissionEpoch(_) => self.complete_ordered_body(ticket),
                    WorkerAction::Control {
                        effect: ControlEffect::WakeInternal,
                        admitted: None,
                    } => {
                        assert_eq!(
                            ticket.input, host.input,
                            "this original batch's coalesced maintenance hint"
                        );
                        self.root.handoff_internal_wake(ticket).unwrap();
                    }
                    _ => panic!(
                        "original HostInput {:?} inventory={original:?}; actual taken ticket input={:?}, descriptor={:?}, effect={:?}",
                        host.input, ticket.input, ticket.descriptor, ticket.effect
                    ),
                }
            }
            panic!("the original HostInput's finite effect prefix did not retire");
        }
        fn drain_original_internal_source(&mut self, now: Tick) {
            use crate::control_runtime::control_internal::InternalAdvance;
            assert_eq!(
                self.runtime.close_timer_source(actor()).unwrap(),
                novarocks_control_runtime::driver::TimerSourceState::ClosedDrained
            );
            // The real source selects the Core's own coalesced continuation.
            // This fixture never constructs a NextInternal command or cursor.
            for _ in 0..ControlCore::MAX_STEP_WORK {
                match self.root.advance_internal().unwrap() {
                    InternalAdvance::Idle => return,
                    InternalAdvance::Submitted(input) => {
                        assert!(self.runtime.poll_one(now).unwrap());
                        self.root.index_control_effects(64).unwrap();
                        while let Some(ticket) = self.root.take_control_effect(64).unwrap() {
                            assert_eq!(ticket.input, input);
                            match &ticket.effect {
                                WorkerAction::AdmissionEpoch(_) => {
                                    self.complete_ordered_body(ticket)
                                }
                                WorkerAction::Control {
                                    effect: ControlEffect::WakeInternal,
                                    admitted: None,
                                } => self.root.handoff_internal_wake(ticket).unwrap(),
                                _ => panic!(
                                    "the original maintenance source emitted an unhandled actual effect: {:?}",
                                    ticket.effect
                                ),
                            }
                        }
                    }
                    InternalAdvance::Waiting(input) => panic!(
                        "the original maintenance input still owns an actual obligation: {input:?}"
                    ),
                }
            }
            panic!("the original finite maintenance source did not reach idle");
        }
        fn finish_host_input(&self, mut host: HostInput) {
            self.runtime.retire_input(actor(), host.input).unwrap();
            // This actual neutral producer and its consumed batch have ended.
            // No timer, foreign capture or asynchronous child is inferred gone.
            host.work.body_exited().unwrap();
            host.work.captures_exited().unwrap();
            host.capacity
                .release_quota(&host.capacity.resources().unwrap().quota)
                .unwrap();
            host.capacity.finish().unwrap();
            self.ledger
                .reap_scope_ready(self.root.resources().scope.id(), 16)
                .unwrap();
        }
        fn publish_route(
            &mut self,
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
            self.root.control_publication_committed(&ticket).unwrap();
            self.complete_body(ticket);
        }
        fn finish(self) {
            assert!(self.requests.is_empty());
            assert!(self.root.resources().requests.is_empty());
            assert_eq!(self.root.request_ready().registered(), 0);
            let (_service, mut resources) = self.root.into_parts();
            resources.scope.close_external().unwrap();
            if let Some(source) = resources.internal.as_mut() {
                assert_eq!(
                    self.runtime.close_timer_source(actor()).unwrap(),
                    novarocks_control_runtime::driver::TimerSourceState::ClosedDrained
                );
                source.close_after_drain().unwrap();
            }
            self.runtime.close_actor_admission(actor()).unwrap();
            while let Some(original_host_turn) = self.runtime.claim_host_actor() {
                original_host_turn.release(false).unwrap();
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
                    .release_quota(&role.capacity.resources().unwrap().quota)
                    .unwrap();
                role.capacity.finish().unwrap();
            }
            self.ledger.remove_scope(resources.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
        }
    }

    #[test]
    fn actual_once_reply_keeps_its_fee_until_the_delivery_guard_really_exits() {
        let mut f = Fixture::new(1, 1);
        let (input, mut handle) = f.admit();
        assert_eq!(handle.request().token, input);
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
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
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
        let observed_runtime = f.runtime.change_version();
        drop(delivery);
        assert_ne!(f.runtime.change_version(), observed_runtime);
        assert!(f.retire(input));
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
        f.finish();
    }

    #[test]
    fn external_cancel_does_not_retire_the_internal_rpc_or_finance_another_request() {
        let mut f = Fixture::new(2, 1);
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
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
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
        let mut f = Fixture::new(2, 2);
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
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
        drop(delivery_b);
        assert!(f.retire(second));
        drop((handle_a, handle_b));
        f.finish();
    }

    #[test]
    fn request_capacity_denial_is_atomic_before_any_partial_holder_or_consumption() {
        let mut f = Fixture::new(2, 1);
        let (input, external) = f.admit();
        let before = f.root.resources().scope.snapshot().unwrap();
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
            ControlRequestError::Runtime(RuntimeError::CapacityDenied(denial))
                if denial.dimension == Dimension::Requests.id()
                    && denial.requested == 1
                    && denial.committed == 1
                    && denial.limit == 1
        ));
        assert!(refusal.partial.is_none());
        assert_eq!(f.root.resources().scope.snapshot().unwrap(), before);
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
        assert_eq!(f.root.request_ready().registered(), 1);
        drop(external);
        f.submit(input);
        f.runtime.poll_one(Tick(1)).unwrap();
        f.body(input);
        let reply = f.reply(input);
        f.requests.get_mut(&input).unwrap().publish(reply).unwrap();
        assert!(f.retire(input));
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
        assert_eq!(f.root.request_ready().registered(), 0);
        f.finish();
    }

    #[test]
    fn matching_numeric_actor_operation_and_input_do_not_authorize_a_foreign_root_reply() {
        let mut first = Fixture::new(1, 1);
        let mut second = Fixture::new(1, 1);
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
        let mut f = Fixture::new(1, 2);
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
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 2);
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
            .release_quota(&partial.capacity.resources().unwrap().quota)
            .and_then(|()| partial.capacity.finish())
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
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
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
        use crate::control_runtime::control_internal::CONTROL_INTERNAL_STORAGE;
        use crate::control_runtime::effect_dispatch::EffectRef;
        use novarocks_control_runtime::publication::PublishedSnapshot;
        use novarocks_control_runtime_core::effect::EffectState;
        use novarocks_execution_contract::{
            ContentFingerprint, CredentialEpoch, CredentialLeaseId, QueryContextState,
        };
        use novarocks_worker_protocol::{control::EstablishSemanticFacts, creation::PayloadId};
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
        // The unpublished bootstrap Scope below actually stops and joins;
        // neither a grant nor an unexecuted host callback supplies that exit.
        let additional = context_storage
            .resources()
            .unwrap()
            .checked_add(&host_storage.resources().unwrap())
            .unwrap()
            .checked_add(&CONTROL_INTERNAL_STORAGE.resources().unwrap())
            .unwrap();
        let mut f = Fixture::with_control_storage(
            2,
            2,
            AdmissionStorage {
                loading: Default::default(),
                accepted: Default::default(),
                context: context_storage.resources().unwrap(),
            },
            additional,
        );
        // Exactly one process-owned source pays its original one Work,
        // input/batch, fixed effect inventory and capture; Requests stays two.
        f.root.enable_internal_source().unwrap();
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
        let grant = f.ordered_reply(acquire);
        let receipt = match &grant.effect {
            WorkerAction::Control {
                effect:
                    ControlEffect::AdmissionReply {
                        result: Ok(receipt),
                        ..
                    },
                admitted: None,
            } => *receipt,
            other => panic!("the original metadata-only ticket receipt: {other:?}"),
        };
        assert_eq!(receipt.context(), f.context);
        f.complete_ordered_body(grant);
        let reply = f.ordered_reply(acquire);
        let reference = EffectRef {
            input: reply.input,
            action: reply.descriptor.id,
        };
        f.requests
            .get_mut(&acquire)
            .unwrap()
            .publish(reply)
            .unwrap();
        f.root.control_completion_observed(reference).unwrap();
        let delivered = acquire_handle.try_take().unwrap();
        assert!(
            matches!(delivered.body(), ControlReplyBody::Admission(Ok(actual)) if *actual == receipt)
        );
        drop(delivered);
        assert!(f.retire(acquire));
        drop(acquire_handle);

        assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);

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
        let materialize = f.ordered_reply(establish);
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
        let (bootstrap_scope, bootstrap_root) = f
            .ledger
            .open_child_scope_with_root(
                &context_work,
                generation(),
                WorkKind::Business,
                ReservationResources::work_only(),
            )
            .unwrap();
        let (mut bootstrap_work, mut bootstrap_capacity) =
            bootstrap_root.bind(WorkKind::Business).unwrap();
        bootstrap_work.start().unwrap();
        bootstrap_scope.request_stop().unwrap();
        let refusal = f
            .ledger
            .open_child_scope_with_root(
                &bootstrap_work,
                generation(),
                WorkKind::Business,
                ReservationResources::work_only(),
            )
            .err()
            .expect("the actual stopped Context bootstrap cannot derive new Business work");
        assert_eq!(refusal, RuntimeError::Scope(ScopeError::Stopped));
        bootstrap_work.body_exited().unwrap();
        bootstrap_work.captures_exited().unwrap();
        assert!(bootstrap_work.actual_exit_ready().unwrap());
        bootstrap_capacity
            .release_quota(&bootstrap_capacity.resources().unwrap().quota)
            .unwrap();
        bootstrap_capacity.finish().unwrap();
        f.ledger.reap_scope_ready(bootstrap_scope.id(), 16).unwrap();
        assert!(bootstrap_scope.snapshot().unwrap().retirement_ready);
        f.ledger.remove_scope(bootstrap_scope.id()).unwrap();
        f.ledger
            .reap_scope_ready(f.root.resources().scope.id(), 16)
            .unwrap();
        assert!(!context_work.actual_exit_ready().unwrap());
        drop((bootstrap_work, bootstrap_capacity, bootstrap_scope));
        let route = PublishedSnapshot::new(
            &f.root.resources().scope,
            Arc::new(QueryContextState::Absent),
        );
        let materialized = f.host_input(
            ControlInput::EstablishmentDecided {
                context: f.context,
                decision: novarocks_worker_protocol::context::ContextEstablishmentDecision {
                    state: QueryContextState::Aborting,
                    outcome: Ok(OperationOutcome::OwnerFailed),
                    acknowledgement: None,
                    decided_at: MonotonicInstant::from_origin(Duration::from_nanos(3)),
                },
            },
            host_storage,
        );
        assert_ne!(materialized.input, establish);
        f.runtime.poll_one(Tick(3)).unwrap();
        let route_ticket = f.ordered_reply(materialized.input);
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
        f.root.index_control_effects(64).unwrap();
        assert!(f.root.take_control_effect(64).unwrap().is_none());
        assert_eq!(*route.read(), QueryContextState::Absent);
        f.publish_route(route_ticket, &route);
        assert_eq!(*route.read(), QueryContextState::Aborting);
        let body = f.ordered_reply(materialized.input);
        f.complete_ordered_body(body);
        let final_reply = f.ordered_reply(materialized.input);
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
        f.root.control_completion_observed(reference).unwrap();
        f.finish_ordered_host_input(materialized);
        let joined_body = f.ordered_reply(joined);
        let routed_request = match &joined_body.effect {
            WorkerAction::Control {
                effect: ControlEffect::RouteEstablish { context, request },
                ..
            } if *context == f.context => request.request,
            _ => panic!("the new RPC must route, not inherit the creation failure"),
        };
        // This fixture's actual Context-scope bootstrap stopped and joined.
        // The neutral route is handed off before the creator's independent
        // unavailable-entity verdict returns; no Context or I/O exit is faked.
        f.complete_ordered_body(joined_body);
        assert!(!f.retire(joined));
        let routed_result = f.host_input(
            ControlInput::RoutedEstablishFinished {
                context: f.context,
                request: routed_request,
                result: Ok(OperationOutcome::InvalidStateOrRequest),
                acknowledgement: None,
                decided_at: MonotonicInstant::from_origin(Duration::from_nanos(3)),
            },
            host_storage,
        );
        f.runtime.poll_one(Tick(3)).unwrap();
        let joined_reply = f.ordered_reply(routed_result.input);
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
        f.root.control_completion_observed(reference).unwrap();
        f.finish_ordered_host_input(routed_result);
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
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
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
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);

        // The unpublished bootstrap Scope actually joined above. The failed
        // host has no child left to stop; its retained/removal facts retire the
        // Context metadata holder elected by its corresponding effect.
        let retained = f.host_input(
            ControlInput::ExecutionRetired {
                context: f.context,
                retirement:
                    novarocks_worker_protocol::retention::ContextRetirementIdentity::Protocol {
                        close_epoch: generation(),
                    },
            },
            host_storage,
        );
        f.runtime.poll_one(Tick(4)).unwrap();
        let retained_route = f.ordered_reply(retained.input);
        f.publish_route(retained_route, &route);
        assert_eq!(*route.read(), QueryContextState::TerminalRetained);

        let retire_context = f.ordered_reply(retained.input);
        f.runtime.validate_effect_ticket(&retire_context).unwrap();
        assert!(matches!(&retire_context.effect, WorkerAction::Control {
            effect: ControlEffect::RetireReservation(actual), admitted: None,
        } if *actual == context_capacity.id()));
        context_work.body_exited().unwrap();
        context_work.captures_exited().unwrap();
        assert!(context_work.actual_exit_ready().unwrap());
        context_capacity
            .release_quota(&context_capacity.resources().unwrap().quota)
            .unwrap();
        context_capacity.finish().unwrap();
        f.complete_ordered_body(retire_context);
        f.finish_ordered_host_input(retained);
        let expired = f.host_input(
            ControlInput::ContextExpired {
                context: f.context,
                retirement:
                    novarocks_worker_protocol::retention::ContextRetirementIdentity::Protocol {
                        close_epoch: generation(),
                    },
            },
            host_storage,
        );
        f.runtime.poll_one(Tick(11_000_000_000)).unwrap();
        let remove_host = f.ordered_reply(expired.input);
        assert!(
            matches!(&remove_host.effect, WorkerAction::Control { effect: ControlEffect::RemoveHost { context }, .. } if *context == f.context)
        );
        f.complete_ordered_body(remove_host);
        f.finish_ordered_host_input(expired);
        let removed = f.host_input(
            ControlInput::HostRemoved { context: f.context },
            host_storage,
        );
        f.runtime.poll_one(Tick(11_000_000_001)).unwrap();
        let removed_route = f.ordered_reply(removed.input);
        f.publish_route(removed_route, &route);
        assert_eq!(*route.read(), QueryContextState::Gone);
        f.finish_ordered_host_input(removed);
        drop(route);
        assert!(context_capacity.is_released());
        drop((context_work, context_capacity));
        f.drain_original_internal_source(Tick(11_000_000_001));
        assert!(f.root.resources().dispatch.is_empty());
        f.finish();
    }
    mod establish_ingress;
}
