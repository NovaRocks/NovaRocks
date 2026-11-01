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

//! Original direct Context RPC custody. A command handoff, Core consumption,
//! reply publication and the actual external delivery exit are separate facts.

pub(super) mod create_disposal;
pub(crate) mod domain;
pub(super) mod domain_driver;
mod endpoint;
pub(crate) mod observation;
pub(super) mod rejected_capture;
pub(super) mod rejected_domain_capture;
mod task_funding;
mod task_route;
use super::root_read::{
    DeferredRootRequest, RootReadData, RootReadDataGuard, RootReadError, RootReadShared,
};
pub(crate) use rejected_capture::SameRRawRefusal;
pub(super) use task_funding::TaskRequestFunding;

use endpoint::ContextRequestEndpoint;
use task_route::TaskRequestRoute;

use super::ContextHostRecord;
use crate::control_runtime::source_slot_issuer::{
    SourceSlotIssueError, SourceSlotIssuePartial, UnusedRetirementProgress,
};
use crate::control_runtime::{
    actors::{ContextMessage, RequestMetadata, WorkerAction, WorkerActor, WorkerMessage},
    causal::CausalError,
    control_request::ControlRequestError,
    request_endpoint::{RequestEndpoint, RequestHandle},
    request_ready::{ControlRequestReady, RequestExternalLeg},
    task::TaskHostRecord,
};
use crate::control_runtime_policy::{PolicyError, StorageNeed};
use novarocks_control_runtime::header_readiness::{
    HeaderReadyError, HeaderReadyNotice, HeaderReadyRegistration, HeaderWaitState,
};
use novarocks_control_runtime::{
    completion::{ActorReportConsumer, ReportSlot},
    driver::{
        ActorRuntime, DriverError, EffectCompletionRefusal, EffectTicket, InputPermit, InputPhase,
    },
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::{ReservationBundle, WorkLease, WorkRetirementObserver},
    publication::{FailurePublicationSession, PublishedReply},
    reported_work::{ReportedWork, ReportedWorkBindingError},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    completion::ConsumerKind,
    identity::{ActionId, ActorId, ConsumerId, InputId, ScopedWorkRef},
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{OperationOutcome, TaskIdentity, TaskOperationId};
use novarocks_worker_protocol::{
    context::{ContextCommand, ContextInput, ContextReplyBody, RequestResult},
    creation::RequestContext,
    lease::MonotonicInstant,
};
use std::sync::Arc;

/// Direct RPCs have one actual input and no Task cut. Task-causal RPCs must
/// separately fund their original prefix/event and Source2 escrow.
pub(crate) const CONTEXT_REQUEST_STORAGE: StorageNeed = StorageNeed {
    requests: 1,
    inputs: 1,
    batches: 1,
    effects: crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
    cuts: 0,
    replies: 1,
    completions: 1,
    captures: 4,
    work: 1,
};

fn core_consumer() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}
fn external_consumer() -> ConsumerId {
    ConsumerId::new(2).unwrap()
}

pub(crate) type ContextRequestHandle = RequestHandle<ContextReplyBody>;
pub(crate) type TaskContextRequestHandle = RequestHandle<ContextReplyBody, RequestMetadata>;

#[derive(Debug)]
pub(crate) enum ContextRequestError {
    Policy(PolicyError),
    Runtime(RuntimeError),
    Driver(DriverError),
    Exit(ExitRoleError),
    Binding(ReportedWorkBindingError),
    Ready(ControlRequestError),
    WrongRuntime,
    InvalidClass,
    UnknownRequest,
    WrongRequest,
    WrongPhase,
    OwnerFailed,
    AdmissionClosed,
    MissingCoreReceipt,
    MissingReleaseObservation,
    SubmissionUnresolved,
    RejectedCapture(rejected_capture::RejectedCaptureFailure),
    TaskPredecessorRequired,
    Causal(CausalError),
    SourceSlots(SourceSlotIssueError),
    TaskNotReady,
    TaskUnpublished,
    UnknownTask,
    BodyResultMismatch,
    MissingNotificationPort,
    Observation(super::observation::ContextObservationError),
    NotificationSubmit(crate::control_runtime::host_executor::HostSubmitError),
    Domain(domain::DomainHostError),
    DynamicFilter(crate::control_runtime::dynamic_filters::DynamicFilterError),
    ReadPayload(crate::control_runtime::task_read_payload::TaskReadPayloadError),
    HeaderReady(HeaderReadyError),
}
impl std::fmt::Display for ContextRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HeaderReady(error) => write!(f, "Request header readiness failed: {error}"),
            Self::Policy(error) => write!(f, "Context request storage policy is invalid: {error}"),
            Self::Runtime(error) => write!(f, "Context request role operation failed: {error}"),
            Self::Driver(error) => write!(
                f,
                "Context request input or publication operation failed: {error}"
            ),
            Self::Exit(error) => write!(
                f,
                "Context request physical exit observation failed: {error}"
            ),
            Self::Binding(error) => write!(
                f,
                "Context request original reported work binding failed: {error}"
            ),
            Self::Ready(error) => write!(f, "Context request ready position failed: {error}"),
            Self::WrongRuntime => f.write_str("Context request belongs to another runtime"),
            Self::InvalidClass => f.write_str("Context request input class is invalid"),
            Self::UnknownRequest => f.write_str("Context request original owner is unknown"),
            Self::WrongRequest => f.write_str("Context request identity does not match"),
            Self::WrongPhase => f.write_str("Context request is in the wrong lifecycle phase"),
            Self::OwnerFailed => f.write_str("Context request actor owner has failed"),
            Self::AdmissionClosed => f.write_str("Context request admission is closed"),
            Self::MissingCoreReceipt => {
                f.write_str("Context request original Core receipt is missing")
            }
            Self::MissingReleaseObservation => {
                f.write_str("Context request original release observation is missing")
            }
            Self::SubmissionUnresolved => {
                f.write_str("Context request submission remains unresolved")
            }
            Self::RejectedCapture(error) => write!(
                f,
                "Context rejected request capture remains unresolved: {error}"
            ),
            Self::TaskPredecessorRequired => {
                f.write_str("Context request still requires its original Task predecessor")
            }
            Self::Causal(error) => write!(f, "Context request causal handoff failed: {error:?}"),
            Self::SourceSlots(error) => write!(
                f,
                "Context request Source positions could not be issued: {error:?}"
            ),
            Self::TaskNotReady => f.write_str("Context request Task is not ready"),
            Self::TaskUnpublished => f.write_str("Context request Task has not been published"),
            Self::UnknownTask => f.write_str("Context request Task is unknown"),
            Self::BodyResultMismatch => {
                f.write_str("Context request body result differs from its original receipt")
            }
            Self::MissingNotificationPort => {
                f.write_str("Context request notification port is missing")
            }
            Self::Observation(error) => write!(f, "Context request observation failed: {error}"),
            Self::NotificationSubmit(error) => write!(
                f,
                "Context request notification submission failed: {error:?}"
            ),
            Self::Domain(error) => write!(f, "Context request domain operation failed: {error}"),
            Self::DynamicFilter(error) => write!(
                f,
                "Context request runtime-filter operation failed: {error:?}"
            ),
            Self::ReadPayload(error) => write!(
                f,
                "Context request read payload remains unavailable: {error:?}"
            ),
        }
    }
}
impl std::error::Error for ContextRequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::HeaderReady(error) => Some(error),
            Self::Policy(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Driver(error) => Some(error),
            Self::Exit(error) => Some(error),
            Self::Binding(error) => Some(error),
            Self::Ready(error) => Some(error),
            Self::RejectedCapture(error) => Some(error),
            Self::Observation(error) => Some(error),
            Self::Domain(error) => Some(error),
            Self::WrongRuntime
            | Self::InvalidClass
            | Self::UnknownRequest
            | Self::WrongRequest
            | Self::WrongPhase
            | Self::OwnerFailed
            | Self::AdmissionClosed
            | Self::MissingCoreReceipt
            | Self::MissingReleaseObservation
            | Self::SubmissionUnresolved
            | Self::TaskPredecessorRequired
            | Self::Causal(_)
            | Self::SourceSlots(_)
            | Self::TaskNotReady
            | Self::TaskUnpublished
            | Self::UnknownTask
            | Self::BodyResultMismatch
            | Self::MissingNotificationPort
            | Self::NotificationSubmit(_)
            | Self::DynamicFilter(_)
            | Self::ReadPayload(_) => None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ContextRequestAdmissionRefusal {
    pub error: ContextRequestError,
    /// The actual Context retains this partial original; this address is not
    /// a caller-owned replacement Work or a successful RPC admission.
    pub partial: Option<ScopedWorkRef>,
}
/// This mode is private to the atomic issuer: no unfilled post-close R is
/// handed to a caller that could replace its read-only neutral command.
#[derive(Clone, Copy)]
enum RequestAdmissionKind {
    Business(InputClass),
    CreationInspection,
}
pub(crate) enum ContextCreationInspectionRefusal {
    Admission(ContextRequestAdmissionRefusal),
    Submission {
        original: Box<ContextRequestSubmitRefusal>,
        _original_handle: ContextRequestHandle,
    },
}
impl std::fmt::Debug for ContextCreationInspectionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(original) => f
                .debug_tuple("CreationInspectionAdmission")
                .field(original)
                .finish(),
            Self::Submission {
                original,
                _original_handle: _,
            } => f
                .debug_struct("CreationInspectionSubmission")
                .field("error", &format_args!("{}", original.error))
                .field("neutral_command_retained", &original.command.is_some())
                .field("retains_external_handle", &true)
                .finish(),
        }
    }
}
impl std::fmt::Display for ContextCreationInspectionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(original) => write!(
                f,
                "Creation inspection admission failed: {}",
                original.error
            ),
            Self::Submission {
                original,
                _original_handle: _,
            } => write!(
                f,
                "Creation inspection original submission failed: {}",
                original.error
            ),
        }
    }
}
impl std::error::Error for ContextCreationInspectionRefusal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission(original) => Some(&original.error),
            Self::Submission {
                original,
                _original_handle: _,
            } => Some(&original.error),
        }
    }
}
#[derive(Debug)]
pub(crate) struct ContextRequestSubmitRefusal {
    pub error: ContextRequestError,
    /// A failed fill retains the exact message in its original custodian.
    pub command: Option<ContextCommand>,
}
#[derive(Debug)]
pub(crate) enum ContextRequestPublishRefusal {
    Rejected {
        error: ContextRequestError,
        ticket: EffectTicket<WorkerAction>,
    },
    Publication {
        error: DriverError,
        ticket: EffectTicket<WorkerAction>,
        body: Arc<ContextReplyBody>,
    },
    Completion(EffectCompletionRefusal<WorkerAction>),
}

/// Only this admission path constructs these originals, before capturing any
/// command or invoking a body. Failed revocation remains unresolved.
pub(crate) struct ContextRequestPartial {
    ledger: RuntimeLedger,
    work: WorkLease,
    capacity: ReservationBundle,
    report: Option<ReportSlot>,
    permit: Option<InputPermit<WorkerActor>>,
    work_retirement: WorkRetirementObserver,
    report_revoked: bool,
    exited: bool,
    unresolved: bool,
    outer: Option<ReservationBundle>,
    sources: Option<SourceSlotIssuePartial>,
}

pub(crate) enum ContextRequestAdmissionPartial {
    Unused(Box<ContextRequestPartial>),
    RejectedCapture(Box<rejected_capture::RejectedAdmissionPartial>),
    BoundTask(Box<TaskRequestBindPartial>),
}
impl ContextRequestAdmissionPartial {
    fn try_retire(&mut self) -> Result<bool, ContextRequestError> {
        match self {
            Self::Unused(partial) => partial.try_retire(),
            Self::RejectedCapture(partial) => partial.try_retire(),
            Self::BoundTask(_) => Err(ContextRequestError::SubmissionUnresolved),
        }
    }
}
/// An unexpected whole-binding refusal can already own a real cut/sender.
/// Retain it for the actual failed-owner inventory; never revoke it as an
/// untouched source or invent a replacement main role to make cleanup pass.
pub(crate) struct TaskRequestBindPartial {
    _original_refusal: crate::control_runtime::causal::RpcAuthorAdmissionRefusal,
    _original_outer: ReservationBundle,
}
impl std::fmt::Debug for TaskRequestBindPartial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The original bound refusal may own a cut/sender. Diagnostic
        // formatting never observes or retires those actual resources.
        f.debug_struct("TaskRequestBindPartial")
            .field("original_refusal", &"retained")
            .field("original_outer_reservation", &"retained")
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for ContextRequestAdmissionPartial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unused(_) => f
                .debug_struct("UnusedRequestAdmission")
                .finish_non_exhaustive(),
            Self::RejectedCapture(_) => f
                .debug_struct("RejectedRequestCaptureAdmission")
                .finish_non_exhaustive(),
            Self::BoundTask(original) => f
                .debug_tuple("BoundTaskRequestAdmission")
                .field(original)
                .finish(),
        }
    }
}

impl ContextRequestPartial {
    fn try_retire(&mut self) -> Result<bool, ContextRequestError> {
        self.try_retire_progress().map(|progress| progress.retired)
    }
    fn try_retire_progress(&mut self) -> Result<UnusedRetirementProgress, ContextRequestError> {
        if self.unresolved {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        let mut progressed = false;
        if let Some(sources) = &mut self.sources {
            let source_progress = sources
                .try_retire_progress(&self.ledger)
                .map_err(ContextRequestError::SourceSlots)?;
            progressed |= source_progress.progressed;
            if !source_progress.retired {
                return Ok(UnusedRetirementProgress {
                    retired: false,
                    progressed,
                });
            }
        }
        if let Some(permit) = self.permit.take() {
            if let Err(error) = permit.revoke_unused() {
                self.unresolved = true;
                return Err(ContextRequestError::Driver(error));
            }
            progressed = true;
        }
        if !self.report_revoked {
            if let Some(report) = &self.report {
                report
                    .revoke_unused()
                    .map_err(ContextRequestError::Runtime)?;
            }
            self.report_revoked = true;
            progressed = true;
        }
        if !self.exited {
            self.work
                .body_exited()
                .map_err(ContextRequestError::Runtime)?;
            self.work
                .captures_exited()
                .map_err(ContextRequestError::Runtime)?;
            if let Some(report) = &self.report {
                report.body_exited().map_err(ContextRequestError::Runtime)?;
                report
                    .captures_exited()
                    .map_err(ContextRequestError::Runtime)?;
            }
            self.exited = true;
            progressed = true;
        }
        if let Some(report) = &self.report
            && !report.try_retire().map_err(ContextRequestError::Runtime)?
        {
            return Ok(UnusedRetirementProgress {
                retired: false,
                progressed,
            });
        }
        // The report handle is never queried after its actual retirement.
        if self.report.is_some() {
            self.report = None;
            progressed = true;
        }
        if !self.work_retirement.is_retired() {
            let retired = self
                .ledger
                .reap_observed_header(&self.work_retirement)
                .map_err(ContextRequestError::Runtime)?;
            if !retired {
                return Ok(UnusedRetirementProgress {
                    retired: false,
                    progressed,
                });
            }
            progressed = true;
        }
        if !self.capacity.is_released() {
            let remaining = self
                .capacity
                .resources()
                .map_err(ContextRequestError::Runtime)?;
            self.capacity
                .release_quota(&remaining.quota)
                .and_then(|()| self.capacity.finish())
                .map_err(ContextRequestError::Runtime)?;
            progressed = true;
        }
        if let Some(outer) = &mut self.outer
            && !outer.is_released()
        {
            let remaining = outer.resources().map_err(ContextRequestError::Runtime)?;
            outer
                .release_quota(&remaining.quota)
                .and_then(|()| outer.finish())
                .map_err(ContextRequestError::Runtime)?;
            progressed = true;
        }
        Ok(UnusedRetirementProgress {
            retired: true,
            progressed,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CoreReceipt {
    Pending,
    Consumed,
    Abandoned,
}

// The same rejected input and its actual error remain with the original R.
// The failed-owner stage consumes only the exact neutral Root ACK or a
// supported control command, after authenticating its actual original R.
struct RefusedRequestInput {
    error: DriverError,
    message: WorkerMessage,
}

#[must_use = "The actual Context retains its request until every original leg retires"]
pub(crate) struct ContextRequestCustody {
    runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    /// Financial Work/reaping belongs to the original admitted request.
    scope: ScopeHandle,
    /// The actual Core consumer remains bound to its original actor even
    /// after that actor's execution Scope has been removed.
    core_consumer: ActorReportConsumer,
    actor: ActorId,
    work: ScopedWorkRef,
    endpoint: ContextRequestEndpoint,
    request: Option<RequestContext>,
    task_route: Option<TaskRequestRoute>,
    domain: Option<domain_driver::DomainRequest>,
    permit: Option<InputPermit<WorkerActor>>,
    role: Option<ExitRole>,
    authority: Option<ExitAuthority>,
    report: ReportSlot,
    work_retirement: WorkRetirementObserver,
    header_ready: HeaderReadyRegistration,
    expected_task: Option<TaskIdentity>,
    submitted: bool,
    started: bool,
    failed_message: Option<RefusedRequestInput>,
    #[cfg(novarocks_dev_controls)]
    owner_panic_on_renew: bool,
    published: Option<(InputId, ActionId)>,
    reply_completed: bool,
    input_retired: bool,
    core_receipt: CoreReceipt,
    exit_observed: bool,
    core_acknowledged: bool,
    external_acknowledged: bool,
    unresolved_revocation: bool,
    create: Option<TaskCreateCapture>,
    rejected_domain: Option<rejected_domain_capture::RejectedDomainCapture>,
    retired_task_read: Option<Arc<super::super::lifecycle_read::TaskReadSnapshot>>,
    read_payload: Option<Arc<crate::control_runtime::task_read_payload::TaskReadPayloadShared>>,
    deferred_root: Option<DeferredRootRequest>,
    observation: Option<observation::ObservationRequest>,
    notification: novarocks_control_runtime::reply_notification::ReplyNotification<WorkerActor>,
    pending_notification: Option<crate::control_runtime::request_endpoint::RequestNotificationJob>,
    protocol_observation: crate::control_runtime::request_observation::RequestObservation,
    pending_protocol_observation:
        Option<crate::control_runtime::request_observation::RequestObservationJob>,
    protocol_observation_kind: crate::control_runtime::request_observation::ContextObservationKind,
    release_observation: Option<crate::RuntimeFilterReleaseObservation>,
}
/// Test-only neutral observations of one actual R. No Work, Scope, report,
/// completion consumer or unique authority escapes through this snapshot.
#[cfg(test)]
pub(in crate::control_runtime::context_host) struct RequestCustodyObservation {
    pub work: ScopedWorkRef,
    pub authority_present: bool,
    pub started: bool,
    pub submitted: bool,
    pub role: Option<RequestRoleObservation>,
    pub header: WorkRetirementObserver,
    pub return_state: novarocks_control_runtime_core::completion::ReturnState,
    pub domain: Option<RequestDomainObservation>,
}
#[cfg(test)]
pub(in crate::control_runtime::context_host) struct RequestRoleObservation {
    pub phase: ExitRolePhase,
    pub body_exited: bool,
    pub captures_exited: bool,
}
#[cfg(test)]
pub(in crate::control_runtime::context_host) struct RequestDomainObservation {
    pub uncaptured_unused: bool,
    pub retired: bool,
}
impl ContextRequestCustody {
    #[cfg(test)]
    pub(in crate::control_runtime::context_host) fn observation_for_test(
        &self,
    ) -> Result<RequestCustodyObservation, RuntimeError> {
        Ok(RequestCustodyObservation {
            work: self.work,
            authority_present: self.authority.is_some(),
            started: self.started,
            submitted: self.submitted,
            role: self.role.as_ref().map(|role| RequestRoleObservation {
                phase: role.phase(),
                body_exited: role.body_exited(),
                captures_exited: role.captures_exited(),
            }),
            header: self.work_retirement.clone(),
            return_state: self.report.owned_return_state()?,
            domain: self.domain.as_ref().map(|domain| RequestDomainObservation {
                uncaptured_unused: domain.is_uncaptured_unused(),
                retired: domain.retired,
            }),
        })
    }
    pub(super) fn task_route_attached(&self) -> bool {
        self.task_route
            .as_ref()
            .is_some_and(TaskRequestRoute::is_attached)
    }
    pub(super) fn note_input_progress(&self) {
        self.endpoint.external().notify();
    }
    pub(crate) fn trace_drain_inventory(&self) {
        let input = self.request.map(|request| {
            (
                self.runtime.input_custody(self.actor, request.token),
                self.runtime
                    .effect_inventory(self.actor, request.token, None, 16),
            )
        });
        let root = self.deferred_root.as_ref().map(|root| {
            (
                root.task,
                root.retained,
                root.cut_waiting,
                root.funding.is_some(),
                root.data.as_ref().map(|data| data.role.phase()),
            )
        });
        tracing::debug!(pid = std::process::id(), work = ?self.work,
            request = ?self.request, task = ?self.expected_task, ?input, ?root,
            task_route = self.task_route.is_some(), observation = self.observation.is_some(),
            domain = self.domain.is_some(), create = self.create.as_ref().map(|c| c.transferred),
            role = ?self.role.as_ref().map(|role| role.phase()), authority = self.authority.is_some(),
            submitted = self.submitted, started = self.started,
            published = ?self.published, reply_completed = self.reply_completed,
            input_retired = self.input_retired, core_receipt = ?self.core_receipt,
            external_ended = self.endpoint.external_ended(), exit_observed = self.exit_observed,
            core_acknowledged = self.core_acknowledged, external_acknowledged = self.external_acknowledged,
            notification = ?self.notification.snapshot(), observer_idle = self.protocol_observation.is_idle(),
            "Worker original request inventory at process drain");
    }
    fn admit(
        host: &ContextHostRecord,
        ready: &ControlRequestReady,
        source: ActorId,
        class: InputClass,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(Self, ContextRequestHandle), (ContextRequestError, Option<ContextRequestPartial>)>
    {
        if !ready.same_runtime(&host.control_runtime) {
            return Err((ContextRequestError::WrongRuntime, None));
        }
        if !matches!(class, InputClass::Ordinary | InputClass::Control) {
            return Err((ContextRequestError::InvalidClass, None));
        }
        let scope = host.request_scope();
        let generation = scope
            .gates()
            .map_err(|e| (ContextRequestError::Runtime(e), None))?
            .generation;
        let guard = scope
            .reserve_next(
                generation,
                CONTEXT_REQUEST_STORAGE
                    .resources()
                    .map_err(|e| (ContextRequestError::Policy(e), None))?,
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .map_err(|e| (ContextRequestError::Runtime(e), None))?;
        let (work, capacity) = match guard.bind(WorkKind::Settlement) {
            Ok(originals) => originals,
            Err(_) => std::process::abort(),
        };
        let observer = work.retirement_observer();
        let mut partial = ContextRequestPartial {
            ledger: host.ledger.clone(),
            work,
            capacity,
            report: None,
            permit: None,
            work_retirement: observer,
            report_revoked: false,
            exited: false,
            unresolved: false,
            outer: None,
            sources: None,
        };
        let report = match ReportSlot::reserve_next(
            &mut partial.capacity,
            generation,
            2,
            [
                (
                    core_consumer(),
                    ConsumerKind::Owner {
                        scope: host.registration().scope_id(),
                        generation: host.registration().initial_epoch(),
                    },
                ),
                (external_consumer(), ConsumerKind::Observer),
            ],
        ) {
            Ok(report) => report,
            Err(error) => return Err((ContextRequestError::Runtime(error), Some(partial))),
        };
        partial.report = Some(report.clone());
        let permit = match host.control_runtime.reserve_admitted_input_for(
            host.registration(),
            source,
            class,
            &partial.work,
        ) {
            Ok(permit) => permit,
            Err(error) => return Err((ContextRequestError::Driver(error), Some(partial))),
        };
        let request = RequestContext {
            token: permit.id(),
            author: partial.work.reference(),
            operation,
            deadline,
        };
        let reply = PublishedReply::new_for_input(
            &permit
                .author()
                .expect("the original admitted R input author"),
        );
        partial.permit = Some(permit);
        let slot = match ready.register_context(&partial.work, host.registration()) {
            Ok(slot) => slot,
            Err(error) => return Err((ContextRequestError::Ready(error), Some(partial))),
        };
        let work = partial.work.reference();
        let permit = partial.permit.take();
        let mut reported =
            match ReportedWork::try_new(partial.work, partial.capacity, report.clone()) {
                Ok(reported) => reported,
                Err(refusal) => {
                    // Unregister only the actual never-exposed original slot.
                    ready
                        .unregister(&slot)
                        .expect("original unexposed request slot");
                    return Err((
                        ContextRequestError::Binding(refusal.error),
                        Some(ContextRequestPartial {
                            ledger: host.ledger.clone(),
                            work: refusal.work,
                            capacity: refusal.capacity,
                            report: Some(refusal.report),
                            permit,
                            work_retirement: partial.work_retirement,
                            report_revoked: false,
                            exited: false,
                            unresolved: false,
                            outer: None,
                            sources: None,
                        }),
                    ));
                }
            };
        let (notification, observer) =
            novarocks_control_runtime::reply_notification::ReplyNotification::for_admitted_work(
                &host.control_runtime,
                host.registration(),
                reported.work(),
            )
            .unwrap_or_else(|_| std::process::abort());
        let endpoint = Arc::new(RequestEndpoint {
            request,
            ready: ready.clone(),
            reply,
            external: RequestExternalLeg::new(slot),
        });
        let header_ready = reported
            .header_readiness_registration()
            .unwrap_or_else(|_| std::process::abort());
        let (role, authority) = ExitRole::from_admitted(reported);
        let handle = RequestHandle {
            endpoint: endpoint.clone(),
            active: true,
            observer: Some(observer),
        };
        Ok((
            Self {
                runtime: host.control_runtime.clone(),
                ledger: host.ledger.clone(),
                scope: scope.clone(),
                core_consumer: report
                    .bind_actor_consumer(
                        &host.control_runtime,
                        host.registration(),
                        core_consumer(),
                    )
                    .unwrap_or_else(|_| std::process::abort()),
                actor: host.actor,
                work,
                endpoint: ContextRequestEndpoint::Direct(endpoint),
                request: Some(request),
                task_route: None,
                domain: None,
                permit,
                role: Some(role),
                authority: Some(authority),
                report,
                work_retirement: partial.work_retirement,
                header_ready,
                expected_task: None,
                submitted: false,
                started: false,
                failed_message: None,
                #[cfg(novarocks_dev_controls)]
                owner_panic_on_renew: false,
                published: None,
                reply_completed: false,
                input_retired: false,
                core_receipt: CoreReceipt::Pending,
                exit_observed: false,
                core_acknowledged: false,
                external_acknowledged: false,
                unresolved_revocation: false,
                create: None,
                rejected_domain: None,
                retired_task_read: None,
                read_payload: None,
                deferred_root: None,
                observation: None,
                notification,
                pending_notification: None,
                protocol_observation: Default::default(),
                pending_protocol_observation: None,
                protocol_observation_kind: Default::default(),
                release_observation: None,
            },
            handle,
        ))
    }
    pub(crate) fn request(&self) -> RequestContext {
        self.request
            .expect("the original RPC has its actual input receipt")
    }
    pub(crate) fn work(&self) -> ScopedWorkRef {
        self.work
    }
    fn submit(&mut self, command: ContextCommand) -> Result<(), ContextRequestSubmitRefusal> {
        if self.task_route.is_some() {
            return Err(ContextRequestSubmitRefusal {
                error: ContextRequestError::TaskPredecessorRequired,
                command: Some(command),
            });
        }
        let reject = |error, command| ContextRequestSubmitRefusal {
            error,
            command: Some(command),
        };
        if self.submitted
            || self.failed_message.is_some()
            || self.permit.is_none()
            || self.unresolved_revocation
        {
            return Err(reject(ContextRequestError::WrongPhase, command));
        }
        let sealed_command = matches!(&command,
            ContextCommand::Cancel { task, .. } | ContextCommand::GetFinalInfo { task } | ContextCommand::GetDynamicFilters { task, .. }
                if self.retired_task_read.as_ref().is_some_and(|original| original.status.identity() == *task));
        if !sealed_command
            && matches!(
                command,
                ContextCommand::Cancel { .. }
                    | ContextCommand::UpdateTask { .. }
                    | ContextCommand::GetFinalInfo { .. }
                    | ContextCommand::GetDynamicFilters { .. }
            )
        {
            return Err(reject(
                ContextRequestError::TaskPredecessorRequired,
                command,
            ));
        }
        if !self.started {
            if let Err(error) = self
                .authority
                .as_mut()
                .expect("original request authority")
                .start()
            {
                return Err(reject(ContextRequestError::Exit(error), command));
            }
            self.started = true;
        }
        if let Err(error) = self.permit.as_ref().expect("original input").bind() {
            return Err(reject(ContextRequestError::Driver(error), command));
        }
        self.expected_task = command.task();
        self.protocol_observation_kind = match &command {
            ContextCommand::Renew { .. } => {
                crate::control_runtime::request_observation::ContextObservationKind::Renew
            }
            ContextCommand::Abort { .. } => {
                crate::control_runtime::request_observation::ContextObservationKind::Abort
            }
            _ => crate::control_runtime::request_observation::ContextObservationKind::Other,
        };
        let metadata = RequestMetadata {
            operation: self.request().operation,
            deadline: self.request().deadline,
        };
        let message = ContextMessage::Command {
            work: self.work,
            request: Some(metadata),
            command: ContextInput::Request(command),
        };
        #[cfg(novarocks_dev_controls)]
        let message = if std::mem::take(&mut self.owner_panic_on_renew) {
            let ContextMessage::Command {
                work,
                request: Some(request),
                command,
            } = message
            else {
                unreachable!("the same admitted Renew retains its metadata");
            };
            ContextMessage::TestOwnerPanicOnRenew {
                work,
                request,
                command,
            }
        } else {
            message
        };
        let message = WorkerMessage::Context(message);
        match self.permit.take().expect("one original fill").fill(message) {
            Ok(actual) => {
                assert_eq!(actual, self.request().token);
                self.submitted = true;
                Ok(())
            }
            Err((error, original)) => {
                self.failed_message = Some(RefusedRequestInput {
                    error,
                    message: original,
                });
                Err(ContextRequestSubmitRefusal {
                    error: ContextRequestError::Driver(error),
                    command: None,
                })
            }
        }
    }
    pub(crate) fn publish(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), ContextRequestPublishRefusal> {
        let reject = |error, ticket| ContextRequestPublishRefusal::Rejected { error, ticket };
        let (body, result) = match &ticket.effect {
            WorkerAction::Reply(reply)
                if reply.request == self.request() && reply.task == self.expected_task =>
            {
                if !reply.body.matches(reply.result) {
                    return Err(reject(ContextRequestError::BodyResultMismatch, ticket));
                }
                (reply.body.clone(), reply.result)
            }
            _ => return Err(reject(ContextRequestError::WrongRequest, ticket)),
        };
        if ticket.actor != self.actor || !self.submitted || self.reply_completed {
            return Err(reject(ContextRequestError::WrongPhase, ticket));
        }
        if let Some(stream) = &mut self.observation
            && !body.matches(RequestResult::Outcome(OperationOutcome::Accepted))
        {
            // A Core intake refusal emitted no Subscribe effect.
            stream.handed_off = true;
        }
        let key = (ticket.input, ticket.descriptor.id);
        if let Some(original) = self.published {
            if original != key {
                return Err(reject(ContextRequestError::WrongRequest, ticket));
            }
        } else {
            if matches!(
                &*body,
                ContextReplyBody::Release {
                    release: novarocks_execution_contract::ReleaseOutcome::Released,
                    ..
                }
            ) && self.release_observation.is_none()
            {
                return Err(reject(
                    ContextRequestError::MissingReleaseObservation,
                    ticket,
                ));
            }
            let event = crate::control_runtime::request_observation::context_event(
                self.endpoint.metadata().operation,
                self.protocol_observation_kind,
                &body,
                result,
                self.release_observation,
            );
            if let Err((error, body)) =
                self.runtime
                    .commit_reply(&ticket, self.endpoint.reply(), body)
            {
                return Err(ContextRequestPublishRefusal::Publication {
                    error,
                    ticket,
                    body,
                });
            }
            self.protocol_observation.stage(event);
            self.published = Some(key);
            self.notification.published();
        }
        self.runtime
            .effect_completed(ticket)
            .map_err(ContextRequestPublishRefusal::Completion)?;
        self.reply_completed = true;
        self.notification.published();
        self.endpoint.external().notify();
        Ok(())
    }
    /// S8 supplies the actual same-scope failure publication session. An
    /// already published commitment is kept, not replaced with OwnerFailed.
    pub(crate) fn publish_owner_failure(
        &mut self,
        session: &mut FailurePublicationSession<WorkerActor>,
    ) -> Result<bool, ContextRequestError> {
        if self.endpoint.reply().is_committed() {
            // The immutable first commitment may already have been taken by
            // its external reader. Failure cannot replace it, but the original
            // R must retain its positive publication disposition.
            self.reply_completed = true;
            self.notification.published();
            self.endpoint.external().notify();
            return Ok(false);
        }
        let body = Arc::new(ContextReplyBody::Verdict(RequestResult::Outcome(
            OperationOutcome::OwnerFailed,
        )));
        session
            .commit_reply(self.endpoint.reply(), body)
            .map_err(|(error, _)| ContextRequestError::Driver(error))?;
        self.reply_completed = true;
        self.notification.published();
        self.endpoint.external().notify();
        Ok(true)
    }
    fn is_refused_root_ack(&self, error: DriverError, message: &WorkerMessage) -> bool {
        if !matches!(
            error,
            DriverError::ActorFailed
                | DriverError::Closed
                | DriverError::FailurePublicationRequired
        ) || self.submitted
            || !self.started
            || self.task_route.is_some()
        {
            return false;
        }
        let Some(root) = self.deferred_root.as_ref() else {
            return false;
        };
        let Some(original) = self.request else {
            return false;
        };
        let metadata = self.endpoint.metadata();
        root.retained
            && root.funding.is_none()
            && root.shared.end_acknowledged()
            && self.expected_task == Some(root.task)
            && original.operation == metadata.operation
            && original.deadline == metadata.deadline
            && matches!(message, WorkerMessage::Context(ContextMessage::Command {
                work,
                request: Some(request),
                command: ContextInput::RootEndAcknowledged { task },
            }) if *work == self.work && *task == root.task
                && request.operation == metadata.operation && request.deadline == metadata.deadline)
    }

    fn is_refused_context_control(&self, error: DriverError, message: &WorkerMessage) -> bool {
        if !matches!(
            error,
            DriverError::ActorFailed
                | DriverError::Closed
                | DriverError::FailurePublicationRequired
        ) || self.submitted
            || !self.started
            || self.task_route.is_some()
            || self.domain.is_some()
            || self.create.is_some()
            || self.deferred_root.is_some()
            || self.observation.is_some()
            || self.expected_task.is_some()
        {
            return false;
        }
        let Some(original) = self.request else {
            return false;
        };
        let ordinary = matches!(message,WorkerMessage::Context(ContextMessage::Command {
            work,request:Some(metadata),command:ContextInput::Request(
                ContextCommand::Renew{..}|ContextCommand::Quiesce|ContextCommand::Abort{..}|ContextCommand::Release),
        }) if *work==self.work && metadata.operation==original.operation && metadata.deadline==original.deadline);
        #[cfg(novarocks_dev_controls)]
        let ordinary = ordinary
            || matches!(message,WorkerMessage::Context(ContextMessage::TestOwnerPanicOnRenew {
            work,request:metadata,command:ContextInput::Request(ContextCommand::Renew{..}),
        }) if *work==self.work && metadata.operation==original.operation && metadata.deadline==original.deadline);
        ordinary
    }
    fn is_refused_neutral_input(&self, error: DriverError, message: &WorkerMessage) -> bool {
        self.is_refused_root_ack(error, message)
            || self.is_refused_context_control(error, message)
            || self.is_refused_create_input(error, message)
    }
    fn is_refused_create_input(&self, error: DriverError, message: &WorkerMessage) -> bool {
        if error != DriverError::ActorFailed
            || self.submitted
            || !self.started
            || self.task_route.is_some()
            || self.domain.is_some()
            || self.deferred_root.is_some()
            || self.observation.is_some()
        {
            return false;
        }
        let (Some(create), Some(original)) = (&self.create, self.request) else {
            return false;
        };
        if create.transferred || self.expected_task != Some(create.task) {
            return false;
        }
        matches!(message, WorkerMessage::Context(ContextMessage::Command {
            work, request: Some(metadata),
            command: ContextInput::Request(ContextCommand::Create { task, payload }),
        }) if *work == self.work && original.author == self.work
            && metadata.operation == original.operation && metadata.deadline == original.deadline
            && *task == create.task && *payload == create.payload_id)
    }
    /// The stage authenticates this original failed Core and committed outbox.
    /// Consume only the exact neutral ACK/control variant after saving its
    /// actual abandoned header. Other variants retain their complete owner.
    fn consume_refused_neutral_input(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
    ) -> Result<bool, ContextRequestError> {
        let Some(RefusedRequestInput { error, message }) = &self.failed_message else {
            return Ok(false);
        };
        if !self.is_refused_neutral_input(*error, message) {
            return Ok(false);
        }
        stage
            .validate(&self.runtime, self.actor)
            .map_err(ContextRequestError::Driver)?;
        if !self.reply_completed || !self.endpoint.reply().is_committed() {
            return Ok(false);
        }
        let input = self.request().token;
        let actual = self
            .runtime
            .input_custody(self.actor, input)
            .map_err(ContextRequestError::Driver)?
            .ok_or(ContextRequestError::MissingCoreReceipt)?;
        let refused_create = self.is_refused_create_input(*error, message);
        let expected_class = if refused_create {
            InputClass::Ordinary
        } else {
            InputClass::Control
        };
        if (self.is_refused_root_ack(*error, message) && actual.source != self.actor)
            || actual.class != expected_class
            || (refused_create && actual.runtime_metadata)
            || actual.role.is_some()
            || !matches!(
                actual.phase,
                InputPhase::Bound | InputPhase::AbandonedByOwnerFailure
            )
        {
            return Err(ContextRequestError::WrongRequest);
        }
        if self
            .permit
            .as_ref()
            .is_some_and(|permit| permit.id() != input)
        {
            return Err(ContextRequestError::WrongRequest);
        }
        self.runtime
            .abandon_failed_input(self.actor, input)
            .map_err(ContextRequestError::Driver)?;
        let abandoned = self
            .runtime
            .input_custody(self.actor, input)
            .map_err(ContextRequestError::Driver)?
            .ok_or(ContextRequestError::MissingCoreReceipt)?;
        if abandoned.phase != InputPhase::AbandonedByOwnerFailure {
            return Err(ContextRequestError::MissingCoreReceipt);
        }
        // A deferred ACK can fail while binding its reply, before fill takes
        // the original permit. Release that neutral handle only after the
        // same real input has an authenticated abandonment receipt.
        self.permit = None;
        self.core_receipt = CoreReceipt::Abandoned;
        // submitted stays false: no command ever entered the mailbox/Core.
        let refused = self
            .failed_message
            .take()
            .expect("same refused neutral input");
        match refused.message {
            WorkerMessage::Context(ContextMessage::Command {
                work,
                request: Some(metadata),
                command: ContextInput::Request(ContextCommand::Create { task, payload }),
            }) if refused_create => {
                // Only neutral IDs leave. The same immutable capture binding,
                // opaque raw, return and Work stay in this original R.
                let _original_fields = (work, metadata, task, payload, refused.error);
            }
            WorkerMessage::Context(ContextMessage::Command {
                work,
                request: Some(metadata),
                command: ContextInput::RootEndAcknowledged { task },
            }) => {
                // Every field of this exact variant is Copy neutral metadata;
                // none owns a callback, payload, guard, Work or Source. This
                // selected message has no custom Drop in the current types.
                let _original_fields = (work, metadata, task, refused.error);
            }
            WorkerMessage::Context(ContextMessage::Command {
                work,
                request: Some(metadata),
                command: ContextInput::Request(command),
            }) if matches!(
                &command,
                ContextCommand::Renew { .. }
                    | ContextCommand::Quiesce
                    | ContextCommand::Abort { .. }
                    | ContextCommand::Release
            ) =>
            {
                // The selected command vocabulary contains only ordinary
                // protocol values; all actual Work/endpoint custody stays here.
                let _original_fields = (work, metadata, command, refused.error);
            }
            #[cfg(novarocks_dev_controls)]
            WorkerMessage::Context(ContextMessage::TestOwnerPanicOnRenew {
                work,
                request: metadata,
                command:
                    ContextInput::Request(ContextCommand::Renew {
                        sequence,
                        valid_for,
                    }),
            }) => {
                let _original_fields = (work, metadata, sequence, valid_for, refused.error);
            }
            message => {
                self.failed_message = Some(RefusedRequestInput {
                    error: refused.error,
                    message,
                });
                return Err(ContextRequestError::WrongRequest);
            }
        }
        self.endpoint.external().notify();
        Ok(true)
    }

    fn observe_core(&mut self) -> Result<bool, ContextRequestError> {
        if self.core_receipt != CoreReceipt::Pending {
            return Ok(true);
        }
        let actual = self
            .runtime
            .input_custody(self.actor, self.request().token)
            .map_err(ContextRequestError::Driver)?
            .ok_or(ContextRequestError::MissingCoreReceipt)?;
        self.core_receipt = match actual.phase {
            InputPhase::Committed => CoreReceipt::Consumed,
            InputPhase::AbandonedByOwnerFailure => CoreReceipt::Abandoned,
            _ => return Ok(false),
        };
        Ok(true)
    }
    fn retire_original_header(&mut self) -> Result<bool, ContextRequestError> {
        if !self
            .ledger
            .reap_observed_header(&self.work_retirement)
            .map_err(ContextRequestError::Runtime)?
        {
            match self
                .header_ready
                .arm(
                    &self.runtime,
                    self.core_consumer.registration(),
                    &self.work_retirement,
                )
                .map_err(ContextRequestError::HeaderReady)?
            {
                HeaderWaitState::Waiting => return Ok(false),
                HeaderWaitState::ReadyToReap => {
                    if !self
                        .ledger
                        .reap_observed_header(&self.work_retirement)
                        .map_err(ContextRequestError::Runtime)?
                    {
                        // A genuine new pin can win after the first probe.
                        // Keep the same armed source; no self-polling notice.
                        return Ok(false);
                    }
                }
                HeaderWaitState::Retired => {}
            }
        }
        // Subscription cancellation changes no Work/report/exit fact and must
        // precede the original role's metadata/Requests refund.
        self.header_ready.cancel();
        Ok(true)
    }
    fn try_retire(
        &mut self,
        mut task: Option<&mut TaskHostRecord>,
    ) -> Result<bool, ContextRequestError> {
        if let Some(done) = self.prepare_deferred_root(task.as_deref_mut())? {
            return Ok(done);
        }
        let mut route = self.task_route.take();
        let result = self.try_retire_with_route(route.as_mut(), task);
        self.task_route = route;
        result
    }
    fn try_retire_with_route(
        &mut self,
        mut route: Option<&mut TaskRequestRoute>,
        mut task: Option<&mut TaskHostRecord>,
    ) -> Result<bool, ContextRequestError> {
        if let Some(capture) = &mut self.rejected_domain {
            if let Some(authority) = capture.take_authority()? {
                assert!(
                    self.authority.is_none(),
                    "same original domain R authority returns once"
                );
                self.authority = Some(authority);
                self.started = true;
            }
            if !capture.returned() {
                return Ok(false);
            }
        }
        if let Some(route) = route.as_mut() {
            route.prepare(self, task.as_deref_mut())?;
            if let Some(task) = task.as_deref_mut() {
                route.try_detach_final_info_tail(self, task)?;
            }
            if self.submitted && self.request.is_none() && !route.abandoned_before_fill() {
                return Ok(false);
            }
        }
        let abandoned_before_fill = route
            .as_ref()
            .is_some_and(|route| route.abandoned_before_fill());
        if let Some(shared) = &self.read_payload {
            if shared.is_unresolved() {
                return Err(ContextRequestError::SubmissionUnresolved);
            }
            if self.endpoint.external_ended()
                && let Some(authority) = shared.take_unlent_after_external_end()
            {
                assert!(self.authority.is_none(), "one original read authority");
                self.authority = Some(authority);
            }
            if let Some(authority) = shared.take_returned() {
                assert!(
                    self.authority.is_none(),
                    "same original read authority returns once"
                );
                self.authority = Some(authority);
            }
            // A lent payload remains part of this actual R's body/captures.
            // External wait cancellation is not its actual physical return.
            if !self.exit_observed && self.authority.is_none() && self.started {
                return Ok(false);
            }
        }
        if self.domain.as_ref().is_some_and(|domain| !domain.retired) {
            return Ok(false);
        }
        if let Some(create) = &mut self.create
            && !create.transferred
        {
            if create
                .returned
                .try_take()
                .map_err(ContextRequestError::Runtime)?
                .is_some()
            {
                create.transferred = true;
            } else {
                return Ok(false);
            }
        }
        if self.protocol_observation.failed() || self.notification.snapshot().failure.is_some() {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        if self.unresolved_revocation {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        if let Some(refused) = &self.failed_message {
            if self.is_refused_neutral_input(refused.error, &refused.message)
                && self
                    .runtime
                    .snapshot(self.actor)
                    .map_err(ContextRequestError::Driver)?
                    .failed
            {
                // The exact R waits for its stage-authenticated abandonment.
                return Ok(false);
            }
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        if !self.submitted && self.core_receipt != CoreReceipt::Abandoned {
            if !self.endpoint.external_ended() || !self.notification.is_idle() {
                return Ok(false);
            }
            if let Some(permit) = self.permit.take()
                && let Err(error) = permit.revoke_unused()
            {
                self.unresolved_revocation = true;
                return Err(ContextRequestError::Driver(error));
            }
            if !self.exit_observed {
                drop(self.retired_task_read.take());
                self.authority
                    .take()
                    .expect("original unused authority")
                    .withdraw_unpublished_and_exit()
                    .map_err(ContextRequestError::Exit)?;
                self.exit_observed = true;
            }
        } else {
            if !self.input_retired && !abandoned_before_fill {
                if !self.observe_core()? {
                    return Ok(false);
                }
                match self.runtime.retire_input(self.actor, self.request().token) {
                    Ok(()) => self.input_retired = true,
                    Err(
                        DriverError::InputStillQueued
                        | DriverError::EffectsRemain
                        | DriverError::ReservationsRemain,
                    ) => return Ok(false),
                    Err(error) => return Err(ContextRequestError::Driver(error)),
                }
            }
            if !self.reply_completed
                || !self.endpoint.external_ended()
                || !self.notification.is_idle()
                || !self.protocol_observation.is_idle()
            {
                return Ok(false);
            }
            if !self.retire_root_data()? {
                return Ok(false);
            }
            drop(self.endpoint.reply().take_once());
            if !self.exit_observed {
                drop(self.retired_task_read.take());
                self.authority
                    .take()
                    .expect("original request authority")
                    .exited()
                    .map_err(ContextRequestError::Exit)?;
                self.exit_observed = true;
            }
            if !self.core_acknowledged {
                match self.core_receipt {
                    CoreReceipt::Consumed => self
                        .core_consumer
                        .consume()
                        .map_err(ContextRequestError::Runtime)?,
                    CoreReceipt::Abandoned => {
                        let proof = self
                            .runtime
                            .failure_proof_for(self.core_consumer.registration())
                            .map_err(ContextRequestError::Driver)?;
                        self.core_consumer
                            .abandon(&proof)
                            .map_err(ContextRequestError::Driver)?;
                    }
                    CoreReceipt::Pending => return Err(ContextRequestError::MissingCoreReceipt),
                }
                self.core_acknowledged = true;
            }
            if !self.external_acknowledged {
                self.report
                    .consume(external_consumer())
                    .map_err(ContextRequestError::Runtime)?;
                self.external_acknowledged = true;
            }
        }
        // Source2 are actual R children. Their already-consumed cuts,
        // physical holders and report legs retire before the parent header;
        // waiting for that header first would wait on our own children.
        if let Some(route) = route.as_deref_mut()
            && !route.finish(self, task)?
        {
            return Ok(false);
        }
        if !self.retire_original_header()? {
            return Ok(false);
        }
        let role = self
            .role
            .as_ref()
            .expect("the submitted original R owns its role");
        if role.phase() != ExitRolePhase::Retired
            && !role.try_retire().map_err(ContextRequestError::Exit)?
        {
            return Ok(false);
        }
        if let Some(route) = route {
            route.release_retired_outer()?;
        }
        if let Some(root) = &mut self.deferred_root {
            if let Some(outer) = &mut root.direct_outer {
                outer
                    .release_quota(
                        &outer
                            .resources()
                            .map_err(ContextRequestError::Runtime)?
                            .quota,
                    )
                    .and_then(|()| outer.finish())
                    .map_err(ContextRequestError::Runtime)?;
            }
            root.direct_outer = None;
        }
        if let Some(create) = &mut self.create
            && let Some(ingress) = create.rejected_ingress.take()
        {
            // Only this direct R's actual raw return/report/header have ended.
            // This pure Native bookkeeping was never attached to Source/H.
            drop(std::mem::ManuallyDrop::into_inner(ingress));
        }
        if let Some(capture) = &mut self.rejected_domain {
            capture.release_retention();
        }
        self.endpoint
            .ready()
            .unregister(self.endpoint.external().slot())
            .map_err(ContextRequestError::Ready)?;
        Ok(true)
    }
}

impl ContextHostRecord {
    pub(crate) fn notify_header_readiness(&self, notice: &HeaderReadyNotice) -> bool {
        let Some(original) = self.resources.requests.get(&notice.work()) else {
            // Common has already routed this actual header cell to the same
            // actor incarnation. Other host owners, including replay legs,
            // advance through their own finite inventory on this real event.
            // The hint is never a header-reaped or resource-exit receipt.
            self.notify_host_progress();
            return false;
        };
        if !original.header_ready.matches(notice) {
            return false;
        }
        original.note_input_progress();
        true
    }
    /// Wake the exact original R authors whose input header pins just ended.
    /// This neither scans another request nor issues a replacement address.
    pub(super) fn notify_retired_input_authors(&self, authors: &[ScopedWorkRef]) {
        for author in authors {
            if let Some(original) = self.resources.requests.get(author) {
                original.note_input_progress();
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn original_request_inventory_for_test(&self) -> Vec<String> {
        self.resources.requests.values().take(8).map(|r| {
            format!(
                "work={:?} request={:?} task={:?} submitted={} started={} reply={} published={:?} input_retired={} core={:?} external_ended={} exit_observed={} core_ack={} external_ack={} authority={} role={:?} header_retired={} header_holders={:?} published_input_status={:?} report_ready={:?} return={:?} notify={:?} protocol_idle={} create={:?}",
                r.work, r.request, r.expected_task, r.submitted, r.started,
                r.reply_completed, r.published, r.input_retired, r.core_receipt,
                r.endpoint.external_ended(), r.exit_observed, r.core_acknowledged,
                r.external_acknowledged, r.authority.is_some(),
                r.role.as_ref().map(|role| (role.phase(), role.body_exited(), role.captures_exited())),
                r.work_retirement.is_retired(), r.work_retirement.header_holders(),
                r.published.map(|(input, _)| (input, r.runtime.input_retirement_status(r.actor, input), r.runtime.input_header_authors(r.actor, input, 8))),
                r.report.retirement_ready(),
                r.report.owned_return_state(), r.notification.snapshot(),
                r.protocol_observation.is_idle(),
                r.create.as_ref().map(|c| (c.transferred, c.returned.state(), c.payload.is_some(), c.binding.is_some(), c.sender.is_some(), c.disposal.is_some()))
            )
        }).collect()
    }
    /// Original saved request facts only; no admission or exit authority.
    #[cfg(test)]
    pub(crate) fn original_task_request_for_test(
        &self,
        work: ScopedWorkRef,
    ) -> Option<(RequestContext, TaskIdentity, bool, bool)> {
        let original = self.resources.requests.get(&work)?;
        let route = original.task_route.as_ref()?;
        Some((
            original.request?,
            original.expected_task?,
            route.is_attached(),
            self.resources.scope.is_some(),
        ))
    }
    /// The same runtime receipt has just become Abandoned. Wake only the R
    /// indexed by this exact original token, not a scan or a synthetic return.
    pub(super) fn notify_failed_request_input(&self, input: InputId) {
        if let Some(work) = self.request_inputs.get(&input) {
            self.resources
                .requests
                .get(work)
                .expect("same original R input index")
                .endpoint
                .external()
                .notify();
        }
    }
    fn retain_request_partial(
        &mut self,
        error: ContextRequestError,
        partial: Option<ContextRequestPartial>,
    ) -> ContextRequestAdmissionRefusal {
        let partial = partial.map(|partial| self.retain_original_request_partial(partial));
        ContextRequestAdmissionRefusal { error, partial }
    }
    /// The same Context owns this complete partial before any error leaves.
    /// Its address is diagnostic metadata, not an external cleanup authority.
    fn retain_original_request_partial(&mut self, partial: ContextRequestPartial) -> ScopedWorkRef {
        let work = partial.work.reference();
        assert!(
            self.resources
                .request_partials
                .insert(
                    work,
                    ContextRequestAdmissionPartial::Unused(Box::new(partial)),
                )
                .is_none()
        );
        if let Some(notice) = &self.resources.notice {
            notice.notify();
        }
        work
    }
    /// The actual Context owns each admitted R before its command can be
    /// filled. The process-wide ready index grants no substitute scope/payer.
    pub(crate) fn admit_request(
        &mut self,
        ready: &ControlRequestReady,
        source: ActorId,
        class: InputClass,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(ScopedWorkRef, ContextRequestHandle), ContextRequestAdmissionRefusal> {
        self.admit_request_kind(
            ready,
            source,
            RequestAdmissionKind::Business(class),
            operation,
            deadline,
        )
    }
    /// One original ordinary R reads the Core's closed creation decision.
    /// No Task payload is captured and no Control lane/position is borrowed.
    pub(crate) fn inspect_creation(
        &mut self,
        ready: &ControlRequestReady,
        source: ActorId,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        task: TaskIdentity,
    ) -> Result<ContextRequestHandle, ContextCreationInspectionRefusal> {
        let (work, handle) = self
            .admit_request_kind(
                ready,
                source,
                RequestAdmissionKind::CreationInspection,
                operation,
                deadline,
            )
            .map_err(ContextCreationInspectionRefusal::Admission)?;
        match self.submit_request(work, ContextCommand::InspectCreation { task }) {
            Ok(()) => Ok(handle),
            Err(original) => Err(ContextCreationInspectionRefusal::Submission {
                original: Box::new(original),
                _original_handle: handle,
            }),
        }
    }
    fn admit_request_kind(
        &mut self,
        ready: &ControlRequestReady,
        source: ActorId,
        kind: RequestAdmissionKind,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(ScopedWorkRef, ContextRequestHandle), ContextRequestAdmissionRefusal> {
        let class = match kind {
            RequestAdmissionKind::Business(class) => class,
            RequestAdmissionKind::CreationInspection => InputClass::Ordinary,
        };
        if self.resources.failure.is_some() {
            return Err(ContextRequestAdmissionRefusal {
                error: ContextRequestError::OwnerFailed,
                partial: None,
            });
        }
        let snapshot = self.control_runtime.snapshot(self.actor).map_err(|error| {
            ContextRequestAdmissionRefusal {
                error: ContextRequestError::Driver(error),
                partial: None,
            }
        })?;
        if snapshot.failed {
            return Err(ContextRequestAdmissionRefusal {
                error: ContextRequestError::OwnerFailed,
                partial: None,
            });
        }
        if snapshot.external_open && matches!(kind, RequestAdmissionKind::CreationInspection) {
            return Err(ContextRequestAdmissionRefusal {
                error: ContextRequestError::WrongPhase,
                partial: None,
            });
        }
        if snapshot.sealed
            || self.resources.control_admission_closed
            || (matches!(kind, RequestAdmissionKind::Business(InputClass::Ordinary))
                && !snapshot.external_open)
        {
            return Err(ContextRequestAdmissionRefusal {
                error: ContextRequestError::AdmissionClosed,
                partial: None,
            });
        }
        match ContextRequestCustody::admit(self, ready, source, class, operation, deadline) {
            Ok((custody, handle)) => {
                let work = custody.work();
                assert!(
                    self.request_inputs
                        .insert(custody.request().token, work)
                        .is_none()
                );
                assert!(self.resources.requests.insert(work, custody).is_none());
                Ok((work, handle))
            }
            Err((error, partial)) => Err(self.retain_request_partial(error, partial)),
        }
    }
    pub(crate) fn submit_request(
        &mut self,
        work: ScopedWorkRef,
        command: ContextCommand,
    ) -> Result<(), ContextRequestSubmitRefusal> {
        if self.resources.failure.is_some() {
            return Err(ContextRequestSubmitRefusal {
                error: ContextRequestError::OwnerFailed,
                command: Some(command),
            });
        }
        match self.resources.requests.get_mut(&work) {
            Some(original) => {
                if let Some(mut route) = original.task_route.take() {
                    let task = match self.tasks.get_mut(&route.task()) {
                        Some(
                            super::TaskHostOwner::Ready(host)
                            | super::TaskHostOwner::Initializing(host),
                        ) => host.as_mut(),
                        _ => {
                            original.task_route = Some(route);
                            return Err(ContextRequestSubmitRefusal {
                                error: ContextRequestError::UnknownTask,
                                command: Some(command),
                            });
                        }
                    };
                    let result = route.submit(original, task, command);
                    original.task_route = Some(route);
                    result
                } else {
                    if matches!(&command, ContextCommand::Subscribe { .. }) {
                        if original.observation.is_some() {
                            return Err(ContextRequestSubmitRefusal {
                                error: ContextRequestError::WrongPhase,
                                command: Some(command),
                            });
                        }
                        original.observation = Some(Default::default());
                    }
                    original.submit(command)
                }
            }
            None => Err(ContextRequestSubmitRefusal {
                error: ContextRequestError::UnknownRequest,
                command: Some(command),
            }),
        }
    }
    /// The adapter has consumed an exact declared session action. Bind its
    /// neutral marker before filling this already admitted original R input.
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn submit_request_owner_panic(
        &mut self,
        work: ScopedWorkRef,
        command: ContextCommand,
    ) -> Result<(), ContextRequestSubmitRefusal> {
        if !matches!(&command, ContextCommand::Renew { .. }) {
            return Err(ContextRequestSubmitRefusal {
                error: ContextRequestError::WrongRequest,
                command: Some(command),
            });
        }
        let Some(original) = self.resources.requests.get_mut(&work) else {
            return Err(ContextRequestSubmitRefusal {
                error: ContextRequestError::UnknownRequest,
                command: Some(command),
            });
        };
        if original.submitted || original.task_route.is_some() {
            return Err(ContextRequestSubmitRefusal {
                error: ContextRequestError::WrongPhase,
                command: Some(command),
            });
        }
        original.owner_panic_on_renew = true;
        self.submit_request(work, command)
    }
    pub(crate) fn publish_request_reply(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), ContextRequestPublishRefusal> {
        let producer = match self.effect_producer_task(&ticket) {
            Ok(task) => task,
            Err(error) => {
                return Err(ContextRequestPublishRefusal::Rejected {
                    error: ContextRequestError::Driver(error),
                    ticket,
                });
            }
        };
        let work = match &ticket.effect {
            WorkerAction::Reply(reply) => self.request_inputs.get(&reply.request.token).copied(),
            _ => None,
        };
        let released_observation = self
            .resources
            .host_jobs
            .as_ref()
            .and_then(|jobs| jobs.released_observation());
        let Some(original) = work.and_then(|work| self.resources.requests.get_mut(&work)) else {
            return Err(ContextRequestPublishRefusal::Rejected {
                error: ContextRequestError::UnknownRequest,
                ticket,
            });
        };
        original.release_observation = released_observation;
        original.publish(ticket)?;
        self.notify_effect_producer_task(producer);
        Ok(())
    }
    /// Finish the same original Reply action after the live failure owner has
    /// preserved the immutable first outbox commitment. This settles action
    /// metadata only, never the external delivery or any physical R holder.
    pub(crate) fn finish_failed_reply_effect(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (ContextRequestError, EffectTicket<WorkerAction>)> {
        if let Err(error) = stage.validate(&self.control_runtime, self.actor) {
            return Err((ContextRequestError::Driver(error), ticket));
        }
        let producer = match self.effect_producer_task(&ticket) {
            Ok(task) => task,
            Err(error) => return Err((ContextRequestError::Driver(error), ticket)),
        };
        let reply = match &ticket.effect {
            WorkerAction::Reply(reply) if ticket.actor == self.actor => reply,
            _ => return Err((ContextRequestError::WrongRequest, ticket)),
        };
        let Some(work) = self.request_inputs.get(&reply.request.token).copied() else {
            return Err((ContextRequestError::UnknownRequest, ticket));
        };
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .expect("original R reply index");
        if original.request != Some(reply.request)
            || original.expected_task != reply.task
            || !reply.body.matches(reply.result)
            || !original.endpoint.reply().is_committed()
            || original
                .published
                .is_some_and(|key| key != (ticket.input, ticket.descriptor.id))
        {
            return Err((ContextRequestError::WrongRequest, ticket));
        }
        if let Err(refusal) = self.control_runtime.effect_completed(ticket) {
            return Err((ContextRequestError::Driver(refusal.error), refusal.ticket));
        }
        original.reply_completed = true;
        original.notification.published();
        original.endpoint.external().notify();
        self.notify_effect_producer_task(producer);
        Ok(())
    }
    pub(crate) fn retains_refused_context_control(
        &self,
        work: ScopedWorkRef,
    ) -> Result<bool, ContextRequestError> {
        if !self
            .control_runtime
            .snapshot(self.actor)
            .map_err(ContextRequestError::Driver)?
            .failed
        {
            return Ok(false);
        }
        let Some(original) = self.resources.requests.get(&work) else {
            return Ok(false);
        };
        Ok(original.failed_message.as_ref().is_some_and(|refused| {
            original.is_refused_context_control(refused.error, &refused.message)
        }))
    }
    pub(crate) fn retains_refused_create_input(
        &self,
        work: ScopedWorkRef,
    ) -> Result<bool, ContextRequestError> {
        if !self
            .control_runtime
            .snapshot(self.actor)
            .map_err(ContextRequestError::Driver)?
            .failed
        {
            return Ok(false);
        }
        Ok(self.resources.requests.get(&work).is_some_and(|original| {
            original.failed_message.as_ref().is_some_and(|refused| {
                original.is_refused_create_input(refused.error, &refused.message)
            })
        }))
    }
    pub(crate) fn capture_refused_create_ingress(
        &mut self,
        work: ScopedWorkRef,
        ingress: crate::control_runtime::node_api::WorkerIngressResponsibility,
    ) -> Result<
        (),
        (
            ContextRequestError,
            crate::control_runtime::node_api::WorkerIngressResponsibility,
        ),
    > {
        match self.retains_refused_create_input(work) {
            Ok(true) => {}
            Ok(false) => return Err((ContextRequestError::WrongRequest, ingress)),
            Err(error) => return Err((error, ingress)),
        }
        let create = self
            .resources
            .requests
            .get_mut(&work)
            .expect("same retained original Create R")
            .create
            .as_mut()
            .expect("strict refused Create capture");
        if create.rejected_ingress.is_some() {
            return Err((ContextRequestError::WrongPhase, ingress));
        }
        create.rejected_ingress = Some(std::mem::ManuallyDrop::new(ingress));
        Ok(())
    }
    /// Inspect only a bounded slice of original R rows. The exact neutral
    /// ACK/control input can end here; rich messages keep their own owners.
    pub(super) fn advance_refused_neutral_inputs(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        after: &mut Option<ScopedWorkRef>,
        budget: usize,
    ) -> Result<bool, ContextRequestError> {
        use std::ops::Bound::{Excluded, Unbounded};
        if budget == 0 {
            return Ok(false);
        }
        stage
            .validate(&self.control_runtime, self.actor)
            .map_err(ContextRequestError::Driver)?;
        let mut progressed = false;
        for _ in 0..budget {
            let Some((&work, original)) = self
                .resources
                .requests
                .range_mut((after.map_or(Unbounded, Excluded), Unbounded))
                .next()
            else {
                *after = None;
                return Ok(progressed);
            };
            *after = Some(work);
            progressed |= original.consume_refused_neutral_input(stage)?;
        }
        let more = self
            .resources
            .requests
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .next()
            .is_some();
        if !more {
            *after = None;
        }
        Ok(progressed || more)
    }
    /// Withdraw only this never-submitted original Task sender under the
    /// actual running failure publisher. No external-end wait or fake input
    /// receipt is involved; physical raw/authority retirement stays separate.
    pub(super) fn withdraw_failed_unfilled_request(
        &mut self,
        work: ScopedWorkRef,
        session: &FailurePublicationSession<WorkerActor>,
    ) -> Result<bool, ContextRequestError> {
        let Some(original) = self.resources.requests.get_mut(&work) else {
            return Ok(false);
        };
        if original.submitted || !original.task_route_attached() {
            return Ok(false);
        }
        let mut route = original
            .task_route
            .take()
            .expect("same attached original Task R");
        let result = match self.tasks.get_mut(&route.task()) {
            Some(super::TaskHostOwner::Ready(task) | super::TaskHostOwner::Initializing(task)) => {
                route.withdraw_failed_unfilled(original, task, session)
            }
            _ => Err(ContextRequestError::UnknownTask),
        };
        original.task_route = Some(route);
        result
    }
    /// The same actual input's request wake, if this effect belongs to an R.
    /// No endpoint/body/Work authority is reconstructed from numeric identity.
    pub(super) fn request_notice_for_input(
        &self,
        input: InputId,
    ) -> Option<crate::control_runtime::request_endpoint::RequestNotice> {
        self.request_inputs
            .get(&input)
            .and_then(|work| self.resources.requests.get(work))
            .map(|original| {
                crate::control_runtime::request_endpoint::RequestNotice(
                    original.endpoint.external().slot().clone(),
                )
            })
    }
    /// One original ready address per call, without scanning another Context.
    pub(crate) fn advance_request(
        &mut self,
        work: ScopedWorkRef,
    ) -> Result<bool, ContextRequestError> {
        self.advance_observation_request(work, 1)?;
        if let Some(original) = self.resources.requests.get_mut(&work) {
            if original.observation.is_some() && !original.endpoint.external_ended() {
                return Ok(false);
            }
            if original.observation.is_some() {
                return Ok(false);
            }
            if let Some(root) = original.deferred_root.as_mut()
                && self.resources.root_reads.has_retained(root.task)
            {
                // This is the same private whole-owner token transferred by
                // actual Task retirement, never a terminal-status inference.
                root.retained = true;
            }
            let task_identity = original
                .task_route
                .as_ref()
                .map(|route| route.task())
                .or_else(|| original.deferred_root.as_ref().map(|root| root.task));
            let task = match task_identity {
                Some(task) => match self.tasks.get_mut(&task) {
                    Some(
                        super::TaskHostOwner::Ready(host)
                        | super::TaskHostOwner::Initializing(host),
                    ) => Some(host.as_mut()),
                    _ if original
                        .task_route
                        .as_ref()
                        .is_some_and(|route| !route.is_attached()) =>
                    {
                        None
                    }
                    _ if original
                        .deferred_root
                        .as_ref()
                        .is_some_and(|root| root.retained) =>
                    {
                        None
                    }
                    _ => return Err(ContextRequestError::UnknownTask),
                },
                None => None,
            };
            let retired = original.try_retire(task).map_err(|error| {
                tracing::error!(
                    actor = ?original.actor,
                    work = ?original.work,
                    request = ?original.request,
                    task = ?original.expected_task,
                    deferred_root = original.deferred_root.is_some(),
                    root_retained = original.deferred_root.as_ref().map(|root| root.retained),
                    task_route = original.task_route.is_some(),
                    submitted = original.submitted,
                    input_retired = original.input_retired,
                    core_receipt = ?original.core_receipt,
                    ?error,
                    "Worker original request retirement refused"
                );
                error
            })?;
            if let Some(root) = &original.deferred_root {
                self.resources
                    .root_reads
                    .set_cut_waiting(root.task, work, root.cut_waiting);
            }
            if let Some(request) = original.request
                && let Some(old) = self.request_inputs.insert(request.token, work)
            {
                assert_eq!(old, work);
            }
            if let Some(route) = &original.task_route {
                let key = (route.task(), route.cut());
                if let Some(old) = self.task_request_cuts.insert(key, work) {
                    assert_eq!(old, work);
                }
            }
            if !retired {
                return Ok(false);
            }
            let root_task = original.deferred_root.as_ref().map(|root| root.task);
            let input = original.request.map(|request| request.token);
            let cut = original.task_route.as_ref().map(|route| route.cut());
            self.resources.requests.remove(&work);
            if let Some(task) = task_identity {
                self.remove_domain_request_index(task, work);
            }
            if let Some(task) = root_task {
                self.resources.root_reads.release(task, work);
            }
            if let Some(input) = input {
                self.request_inputs.remove(&input);
            }
            if let (Some(task), Some(cut)) = (task_identity, cut) {
                if self.task_request_cuts.remove(&(task, cut)).is_some()
                    && let Some(
                        super::TaskHostOwner::Ready(host)
                        | super::TaskHostOwner::Initializing(host),
                    ) = self.tasks.get(&task)
                {
                    // The actual last routing index can disappear after this
                    // turn's failed-Task sweep. No physical producer need wake
                    // again; retain one retry on this same still-owned Task.
                    host.pending_notice().pending();
                }
                // The same route just returned its original cut/source funding.
                // Wake only its Task's genuinely parked Root R positions.
                self.resources.root_reads.source_progress(task);
            }
            // The failed/normal Context tail can have parked earlier in this
            // same turn on its original R inventory. Its real removal is a
            // positive owner transition even when no Task or I/O remains.
            self.notify_host_progress();
            return Ok(true);
        }
        if let Some(original) = self.resources.request_partials.get_mut(&work) {
            if !original.try_retire()? {
                // A bounded reaper may have retired a different ready sibling.
                // This original partial has no request-ready slot of its own.
                let wake = !matches!(original, ContextRequestAdmissionPartial::RejectedCapture(_));
                if wake && let Some(notice) = &self.resources.notice {
                    notice.notify();
                }
                return Ok(false);
            }
            self.resources.request_partials.remove(&work);
            self.notify_host_progress();
            return Ok(true);
        }
        Err(ContextRequestError::UnknownRequest)
    }
}

/// The original admitted R retains the opaque Create capture until a real
/// loader takes it, or a physical cleanup body returns in its same paid slot.
struct TaskCreateCapture {
    task: TaskIdentity,
    payload_id: novarocks_worker_protocol::creation::PayloadId,
    // Only an authenticated failed-fill direct Create transfers this marker.
    // Unknown capture exit retains the original transport admission backing.
    rejected_ingress: Option<
        std::mem::ManuallyDrop<crate::control_runtime::node_api::WorkerIngressResponsibility>,
    >,
    payload: Option<std::mem::ManuallyDrop<crate::control_runtime::loading::TaskCreatePayload>>,
    binding: Option<crate::control_runtime::task::TaskHostBinding>,
    sender: Option<novarocks_control_runtime::owned_return::OwnedReturnSender<()>>,
    returned: novarocks_control_runtime::owned_return::OwnedReturnReader<()>,
    transferred: bool,
    disposal: Option<create_disposal::TaskCreateDisposalJob>,
}
pub(crate) struct TaskCreateSubmitRefusal {
    pub error: ContextRequestError,
    /// None means the original R already owns the capture.
    pub originals: Option<(
        crate::control_runtime::loading::TaskCreatePayload,
        crate::control_runtime::task::TaskHostBinding,
    )>,
}
impl ContextHostRecord {
    pub(crate) fn submit_task_create(
        &mut self,
        work: ScopedWorkRef,
        payload: crate::control_runtime::loading::TaskCreatePayload,
        binding: crate::control_runtime::task::TaskHostBinding,
    ) -> Result<(), TaskCreateSubmitRefusal> {
        let reject = |error, payload, binding| TaskCreateSubmitRefusal {
            error,
            originals: Some((payload, binding)),
        };
        if binding.context != self.identity
            || !binding.registration.same_registration(self.registration())
        {
            return Err(reject(ContextRequestError::WrongRequest, payload, binding));
        }
        let Some(original) = self.resources.requests.get_mut(&work) else {
            return Err(reject(
                ContextRequestError::UnknownRequest,
                payload,
                binding,
            ));
        };
        if original.submitted || original.started || original.create.is_some() {
            return Err(reject(ContextRequestError::WrongPhase, payload, binding));
        }
        let (sender, returned) = match original.report.claim_owned_return::<()>() {
            Ok(parts) => parts,
            Err(error) => {
                return Err(reject(
                    ContextRequestError::Runtime(error),
                    payload,
                    binding,
                ));
            }
        };
        let command = ContextCommand::Create {
            task: payload.request.identity(),
            payload: payload.id,
        };
        original.create = Some(TaskCreateCapture {
            task: payload.request.identity(),
            payload_id: payload.id,
            rejected_ingress: None,
            payload: Some(std::mem::ManuallyDrop::new(payload)),
            binding: Some(binding),
            sender: Some(sender),
            returned,
            transferred: false,
            disposal: None,
        });
        original
            .submit(command)
            .map_err(|refusal| TaskCreateSubmitRefusal {
                error: refusal.error,
                originals: None,
            })
    }
    pub(super) fn take_create_for_load(
        &mut self,
        token: InputId,
        task: TaskIdentity,
        payload: novarocks_worker_protocol::creation::PayloadId,
    ) -> Result<
        (
            ScopedWorkRef,
            crate::control_runtime::loading::TaskCreatePayload,
            crate::control_runtime::task::TaskHostBinding,
        ),
        ContextRequestError,
    > {
        let work = *self
            .request_inputs
            .get(&token)
            .ok_or(ContextRequestError::UnknownRequest)?;
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .ok_or(ContextRequestError::UnknownRequest)?;
        let capture = original
            .create
            .as_mut()
            .ok_or(ContextRequestError::WrongRequest)?;
        let source = capture
            .payload
            .as_ref()
            .ok_or(ContextRequestError::WrongPhase)?;
        if source.id != payload || source.request.identity() != task {
            return Err(ContextRequestError::WrongRequest);
        }
        Ok((
            work,
            std::mem::ManuallyDrop::into_inner(capture.payload.take().expect("same capture")),
            capture.binding.take().expect("same binding"),
        ))
    }
    pub(super) fn restore_create_after_load_refusal(
        &mut self,
        work: ScopedWorkRef,
        payload: crate::control_runtime::loading::TaskCreatePayload,
        binding: crate::control_runtime::task::TaskHostBinding,
    ) {
        let capture = self
            .resources
            .requests
            .get_mut(&work)
            .expect("original R")
            .create
            .as_mut()
            .expect("original capture");
        assert!(capture.payload.is_none() && capture.binding.is_none());
        capture.payload = Some(std::mem::ManuallyDrop::new(payload));
        capture.binding = Some(binding);
    }
    /// Authenticate the same never-loaded raw/binding pair after its original
    /// bootstrap refusal. Plain IDs select this already-owned R, not authority.
    pub(super) fn validate_failed_unpublished_create(
        &self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        input: InputId,
        task: TaskIdentity,
        payload: novarocks_worker_protocol::creation::PayloadId,
    ) -> Result<(), ContextRequestError> {
        stage
            .validate(&self.control_runtime, self.actor)
            .map_err(ContextRequestError::Driver)?;
        let work = self
            .request_inputs
            .get(&input)
            .ok_or(ContextRequestError::UnknownRequest)?;
        let original = self
            .resources
            .requests
            .get(work)
            .ok_or(ContextRequestError::UnknownRequest)?;
        let request = original.request.ok_or(ContextRequestError::WrongRequest)?;
        let capture = original
            .create
            .as_ref()
            .ok_or(ContextRequestError::WrongRequest)?;
        let raw = capture
            .payload
            .as_ref()
            .ok_or(ContextRequestError::WrongPhase)?;
        let binding = capture
            .binding
            .as_ref()
            .ok_or(ContextRequestError::WrongPhase)?;
        if original.actor != self.actor
            || !original.runtime.same_runtime(&self.control_runtime)
            || original.work != *work
            || request.token != input
            || request.author != *work
            || !original.started
            || !original.submitted
            || original.task_route.is_some()
            || original.failed_message.is_some()
            || original.unresolved_revocation
            || original.expected_task != Some(task)
            || capture.task != task
            || capture.payload_id != payload
            || capture.transferred
            || capture.disposal.is_some()
            || capture.sender.is_none()
            || capture.rejected_ingress.is_some()
            || raw.id != payload
            || raw.request.identity() != task
            || raw.request.context() != self.identity
            || raw.request.envelope().operation_id() != request.operation
            || !binding.registration.same_registration(stage.registration())
            || binding.context != self.identity
            || !original.reply_completed
            || !original.endpoint.reply().is_committed()
        {
            return Err(ContextRequestError::WrongRequest);
        }
        Ok(())
    }
    /// Release only the original pure mailbox/batch custody. Root's raw,
    /// return, authority and external leg stay in this same R until real exit.
    pub(super) fn retire_failed_unpublished_create_input(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        input: InputId,
        task: TaskIdentity,
        payload: novarocks_worker_protocol::creation::PayloadId,
    ) -> Result<bool, ContextRequestError> {
        self.validate_failed_unpublished_create(stage, input, task, payload)?;
        let work = *self
            .request_inputs
            .get(&input)
            .expect("validated original Create input");
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .expect("validated original Create R");
        if original.input_retired {
            return Ok(true);
        }
        let custody = self
            .control_runtime
            .input_custody(self.actor, input)
            .map_err(ContextRequestError::Driver)?
            .ok_or(ContextRequestError::MissingCoreReceipt)?;
        if custody.class != InputClass::Ordinary
            || custody.role.is_some()
            || custody.runtime_metadata
        {
            return Err(ContextRequestError::WrongRequest);
        }
        if !original.observe_core()? {
            return Ok(false);
        }
        match self.control_runtime.retire_input(self.actor, input) {
            Ok(()) => {
                original.input_retired = true;
                original.endpoint.external().notify();
                self.notify_host_progress();
                Ok(true)
            }
            Err(
                DriverError::InputStillQueued
                | DriverError::EffectsRemain
                | DriverError::ReservationsRemain,
            ) => Ok(false),
            Err(error) => Err(ContextRequestError::Driver(error)),
        }
    }
    pub(super) fn create_transferred_to_loader(
        &mut self,
        work: ScopedWorkRef,
    ) -> Result<(), ContextRequestError> {
        let capture = self
            .resources
            .requests
            .get_mut(&work)
            .ok_or(ContextRequestError::UnknownRequest)?
            .create
            .as_mut()
            .ok_or(ContextRequestError::WrongRequest)?;
        if capture.payload.is_some() || capture.transferred {
            return Err(ContextRequestError::WrongPhase);
        }
        if let Err(refusal) = capture
            .sender
            .take()
            .ok_or(ContextRequestError::WrongPhase)?
            .revoke_unused()
        {
            capture.sender = Some(refusal.sender);
            return Err(ContextRequestError::Runtime(refusal.error));
        }
        capture.transferred = true;
        Ok(())
    }
}

impl ContextHostRecord {
    /// Fund this original R and its Source2 before any Root channel access.
    /// No Task cut or actor input exists until the actual EOF continuation.
    pub(super) fn admit_deferred_root_request(
        &mut self,
        ready: &ControlRequestReady,
        read: &novarocks_execution_contract::root_result::RootResultRead,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(ScopedWorkRef, TaskContextRequestHandle, Arc<RootReadShared>), RootReadError> {
        let task = read.root_task();
        let reject = |error| {
            RootReadError::Request(ContextRequestAdmissionRefusal {
                error,
                partial: None,
            })
        };
        if !ready.same_runtime(&self.control_runtime) {
            return Err(reject(ContextRequestError::WrongRuntime));
        }
        let snapshot = self
            .control_runtime
            .snapshot(self.actor)
            .map_err(|error| reject(ContextRequestError::Driver(error)))?;
        if snapshot.failed || self.resources.failure.is_some() {
            return Err(reject(ContextRequestError::OwnerFailed));
        }
        if let Some(accepted_consumed) = self.resources.root_reads.closed_read_frontier(read)? {
            return Err(RootReadError::Closed { accepted_consumed });
        }
        let (task_owner, task_request_admission_open) = match self.tasks.get(&task) {
            Some(super::TaskHostOwner::Ready(host)) => {
                ("Ready", Some(host.request_admission_open()))
            }
            Some(super::TaskHostOwner::Initializing(host)) => {
                ("Initializing", Some(host.request_admission_open()))
            }
            Some(super::TaskHostOwner::Unpublished(_)) => ("Unpublished", None),
            None => ("Absent", None),
        };
        let root_present = self.resources.root_reads.has_root(task);
        if !self.tasks.contains_key(&task) && !root_present && !self.has_retired_task_read(task) {
            return Err(RootReadError::UnknownTask);
        }
        // An installed Root remains readable until its own channel is sealed.
        // The Core's close decision does not substitute for that physical cut.
        if !root_present
            && (snapshot.sealed
                || self.root_issuance_sealed
                || self.resources.control_admission_closed)
        {
            tracing::info!(
                pid = std::process::id(),
                context = ?self.identity,
                task = ?task,
                operation = ?operation,
                actor_sealed = snapshot.sealed,
                actor_external_open = snapshot.external_open,
                root_issuance_sealed = self.root_issuance_sealed,
                control_admission_closed = self.resources.control_admission_closed,
                root_present = self.resources.root_reads.has_root(task),
                root_task_retired = self.resources.root_reads.has_retained(task),
                task_owner,
                task_request_admission_open,
                reason = "AdmissionClosed",
                "Worker original Root R admission gate refused"
            );
            return Err(reject(ContextRequestError::AdmissionClosed));
        }
        // The original read seal follows physical Task/prefix retirement and
        // transfer of any installed Root into this Context. Under the still-open
        // Root/Context gates above, this sealed Task can no longer install one.
        if self.has_retired_task_read(task) && !self.resources.root_reads.has_root(task) {
            return Err(RootReadError::Task(
                crate::control_runtime::task::TaskRootReadError::NoRoot,
            ));
        }
        let retained = self.resources.root_reads.has_retained(task);
        // The installed Context Root owns reads independently of the closing
        // Task RPC issuer. Only actual Task retirement below permits the EOF
        // continuation to bypass that Task's original causal prefix.
        if !self.resources.root_reads.has_root(task)
            && !matches!(self.tasks.get(&task), Some(super::TaskHostOwner::Ready(host)) if host.request_admission_open())
        {
            tracing::info!(
                pid = std::process::id(),
                context = ?self.identity,
                task = ?task,
                operation = ?operation,
                actor_sealed = snapshot.sealed,
                actor_external_open = snapshot.external_open,
                root_issuance_sealed = self.root_issuance_sealed,
                control_admission_closed = self.resources.control_admission_closed,
                root_present = self.resources.root_reads.has_root(task),
                root_task_retired = self.resources.root_reads.has_retained(task),
                task_owner,
                task_request_admission_open,
                reason = "TaskNotReady",
                "Worker original Root R admission gate refused"
            );
            return Err(reject(ContextRequestError::TaskNotReady));
        }
        let mut funding = self
            .reserve_task_request_funding()
            .map_err(RootReadError::Request)?;
        let author_scope = funding.author.work.scope().clone();
        let work = funding.author.work.reference();
        let work_retirement = funding.author.work.retirement_observer();
        let report = funding.author.report.clone();
        if let Err(error) = self.resources.root_reads.reserve(task, work) {
            let partial = ContextRequestPartial::from_task_funding(funding, self.ledger.clone());
            self.retain_original_request_partial(partial);
            // Preserve the owning Root's exact capacity facts. The ordinary
            // full reply does not relinquish this original partial's custody.
            return Err(error);
        }
        let slot = match ready.register_context(&funding.author.work, self.registration()) {
            Ok(slot) => slot,
            Err(error) => {
                self.resources.root_reads.release(task, work);
                let partial =
                    ContextRequestPartial::from_task_funding(funding, self.ledger.clone());
                return Err(RootReadError::Request(self.retain_request_partial(
                    ContextRequestError::Ready(error),
                    Some(partial),
                )));
            }
        };
        let (notification, observer) =
            novarocks_control_runtime::reply_notification::ReplyNotification::for_admitted_work(
                &self.control_runtime,
                self.registration(),
                &funding.author.work,
            )
            .unwrap_or_else(|_| std::process::abort());
        let shared = RootReadShared::new(crate::control_runtime::request_endpoint::RequestNotice(
            slot.clone(),
        ));
        let reply = PublishedReply::new_for_admitted_work(
            &self.control_runtime,
            self.registration(),
            &funding.author.work,
        )
        .unwrap_or_else(|_| std::process::abort());
        let endpoint = Arc::new(RequestEndpoint {
            request: RequestMetadata {
                operation,
                deadline,
            },
            ready: ready.clone(),
            reply,
            external: RequestExternalLeg::new(slot),
        });
        let handle = RequestHandle {
            endpoint: endpoint.clone(),
            active: true,
            observer: Some(observer),
        };
        let custody = ContextRequestCustody {
            runtime: self.control_runtime.clone(),
            ledger: self.ledger.clone(),
            scope: author_scope,
            core_consumer: report
                .bind_actor_consumer(&self.control_runtime, self.registration(), core_consumer())
                .unwrap_or_else(|_| std::process::abort()),
            actor: self.actor,
            work,
            endpoint: ContextRequestEndpoint::Task(endpoint),
            request: None,
            task_route: None,
            domain: None,
            permit: None,
            role: None,
            authority: None,
            report,
            work_retirement,
            header_ready: funding
                .header_ready
                .take()
                .expect("same original deferred R header registration"),
            expected_task: Some(task),
            submitted: false,
            started: false,
            failed_message: None,
            #[cfg(novarocks_dev_controls)]
            owner_panic_on_renew: false,
            published: None,
            reply_completed: false,
            input_retired: false,
            core_receipt: CoreReceipt::Pending,
            exit_observed: false,
            core_acknowledged: false,
            external_acknowledged: false,
            unresolved_revocation: false,
            create: None,
            rejected_domain: None,
            notification,
            pending_notification: None,
            protocol_observation: Default::default(),
            pending_protocol_observation: None,
            protocol_observation_kind: Default::default(),
            release_observation: None,
            observation: None,
            retired_task_read: None,
            read_payload: None,
            deferred_root: Some(DeferredRootRequest {
                task,
                funding: Some(funding),
                unused: None,
                unused_sources: None,
                data: None,
                data_partial: None,
                shared: shared.clone(),
                retained,
                cut_waiting: false,
                binding_refusal: None,
                direct_outer: None,
            }),
        };
        assert!(self.resources.requests.insert(work, custody).is_none());
        Ok((work, handle, shared))
    }
    pub(super) fn admit_root_read_data(
        &mut self,
        request_work: ScopedWorkRef,
    ) -> Result<RootReadDataGuard, ContextRequestAdmissionRefusal> {
        let original = self
            .resources
            .requests
            .get(&request_work)
            .and_then(|request| request.deferred_root.as_ref())
            .expect("same original deferred Root R before data capture");
        assert!(
            original.data.is_none() && original.data_partial.is_none(),
            "one original Root data position per R"
        );
        let need = crate::control_runtime_policy::ROOT_READ_DATA_STORAGE;
        let scope = self.scope().clone();
        let reject = |error| ContextRequestAdmissionRefusal {
            error,
            partial: None,
        };
        let generation = scope
            .gates()
            .map_err(|error| reject(ContextRequestError::Runtime(error)))?
            .generation;
        let resources = need
            .resources()
            .map_err(|error| reject(ContextRequestError::Policy(error)))?;
        let guard = scope
            .reserve_child_next(
                generation,
                self.resources.roles[super::LIFETIME].capacity.id(),
                resources,
                WorkKind::Settlement,
            )
            .map_err(|error| reject(ContextRequestError::Runtime(error)))?;
        let (work, capacity) = guard
            .bind(WorkKind::Settlement)
            .unwrap_or_else(|_| std::process::abort());
        let observer = work.retirement_observer();
        let work_id = work.reference();
        let mut partial = ContextRequestPartial {
            ledger: self.ledger.clone(),
            work,
            capacity,
            report: None,
            permit: None,
            work_retirement: observer.clone(),
            report_revoked: false,
            exited: false,
            unresolved: false,
            outer: None,
            sources: None,
        };
        let report = match ReportSlot::reserve_next(
            &mut partial.capacity,
            generation,
            1,
            [(core_consumer(), ConsumerKind::Observer)],
        ) {
            Ok(report) => report,
            Err(error) => {
                // This zero-request data header belongs to this same admitted
                // R. Keeping it in a separate partial map would let R/reader
                // admission recycle before the actual data partial retired.
                let root = self
                    .resources
                    .requests
                    .get_mut(&request_work)
                    .and_then(|request| request.deferred_root.as_mut())
                    .expect("same original Root R data partial owner");
                assert!(root.data_partial.replace(partial).is_none());
                root.shared.notice.notify();
                return Err(ContextRequestAdmissionRefusal {
                    error: ContextRequestError::Runtime(error),
                    // The same R owns this nested partial and its retirement.
                    partial: Some(request_work),
                });
            }
        };
        partial.report = Some(report.clone());
        let reported = crate::control_runtime::admitted_role::adopt_reported_work(
            partial.work,
            partial.capacity,
            report.clone(),
        );
        let (role, mut authority) = ExitRole::from_admitted(reported);
        authority.start().unwrap_or_else(|_| std::process::abort());
        let original = self
            .resources
            .requests
            .get_mut(&request_work)
            .expect("same admitted Root R");
        let root = original
            .deferred_root
            .as_mut()
            .expect("same deferred Root read");
        let holder = RootReadDataGuard::new(authority, root.shared.clone());
        root.data = Some(RootReadData {
            role,
            report,
            observer,
            work: work_id,
            consumed: false,
        });
        Ok(holder)
    }
    /// The only physical notification path for this original R. It never
    /// invokes or drops a foreign Waker while the Context cell is borrowed.
    pub(crate) fn advance_request_notification(
        &mut self,
        work: ScopedWorkRef,
        ports: &super::task_driver::TaskDriverPorts,
    ) -> Result<(), ContextRequestError> {
        let registration = self.registration().clone();
        let Some(original) = self.resources.requests.get_mut(&work) else {
            return Ok(());
        };
        Self::advance_protocol_observation_job(original, ports, &registration, self.identity)?;
        Self::advance_unused_create(original, ports, &registration, self.identity)?;
        if let Some(capture) = &mut original.rejected_domain {
            capture.advance(ports, &registration, self.identity, &original.endpoint)?;
        }
        if original.pending_notification.is_none() {
            original.pending_notification = original.notification.take_job().map(|job| {
                crate::control_runtime::request_endpoint::RequestNotificationJob {
                    original: job,
                    notice: crate::control_runtime::request_endpoint::RequestNotice(
                        original.endpoint.external().slot().clone(),
                    ),
                }
            });
        }
        let Some(job) = original.pending_notification.take() else {
            return Ok(());
        };
        let Some(executor) = ports.lifecycle.as_ref() else {
            original.pending_notification = Some(job);
            return Err(ContextRequestError::MissingNotificationPort);
        };
        let before = ports.capacity.as_ref().map(|signal| signal.revision());
        match executor.try_submit_for_group(
            super::task_driver::TaskDriverHostJob::ReplyNotification(job),
            crate::control_runtime::preparation::HostFairnessGroup::Context(self.identity),
        ) {
            Ok(()) => original
                .endpoint
                .ready()
                .clear_capacity_wait(original.endpoint.external().slot())
                .map_err(ContextRequestError::Ready)?,
            Err((error, job)) => {
                let super::task_driver::TaskDriverHostJob::ReplyNotification(job) = job else {
                    unreachable!("same original reply notification");
                };
                original.pending_notification = Some(job);
                if let Some(before) = before {
                    original
                        .endpoint
                        .ready()
                        .park_capacity(original.endpoint.external().slot(), before)
                        .map_err(ContextRequestError::Ready)?;
                }
                if let (Some(signal), Some(before)) = (&ports.capacity, before) {
                    signal
                        .register_waiter(self.registration(), before)
                        .map_err(|_| ContextRequestError::MissingNotificationPort)?;
                }
                if !crate::control_runtime::request_endpoint::notification_submit_busy(&error) {
                    return Err(ContextRequestError::NotificationSubmit(error));
                }
            }
        }
        Ok(())
    }
    fn advance_unused_create(
        original: &mut ContextRequestCustody,
        ports: &super::task_driver::TaskDriverPorts,
        registration: &novarocks_control_runtime::driver::ActorRegistration,
        context: novarocks_execution_contract::QueryContextRef,
    ) -> Result<(), ContextRequestError> {
        if !original.create.as_ref().is_some_and(|create| {
            !create.transferred && (create.payload.is_some() || create.disposal.is_some())
        }) || !original.endpoint.external_ended()
            || !original.notification.is_idle()
            || !original.protocol_observation.is_idle()
            || original.failed_message.is_some()
            || original.unresolved_revocation
        {
            return Ok(());
        }
        // Retirement of the original input proves that every emitted effect,
        // including any LoadInput handoff, has actually completed. A replay or
        // refusal can retain its never-loaded payload without emitting one.
        if original.submitted && !original.input_retired {
            if !original.observe_core()? {
                return Ok(());
            }
            match original
                .runtime
                .retire_input(original.actor, original.request().token)
            {
                Ok(()) => original.input_retired = true,
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain,
                ) => return Ok(()),
                Err(error) => return Err(ContextRequestError::Driver(error)),
            }
        }
        let create = original.create.as_mut().expect("original Create capture");
        if create.disposal.is_none() {
            create.disposal = Some(create_disposal::TaskCreateDisposalJob::new(
                original.work.scope,
                original.work.work,
                create
                    .payload
                    .take()
                    .expect("original never-loaded payload"),
                create
                    .binding
                    .take()
                    .expect("original never-loaded binding"),
                create.sender.take().expect("original unused return sender"),
                crate::control_runtime::request_endpoint::RequestNotice(
                    original.endpoint.external().slot().clone(),
                ),
            ));
        }
        let job = create.disposal.take().expect("same original disposal");
        let Some(executor) = &ports.lifecycle else {
            create.disposal = Some(job);
            return Err(ContextRequestError::MissingNotificationPort);
        };
        let before = ports.capacity.as_ref().map(|signal| signal.revision());
        match executor.try_submit_for_group(
            super::task_driver::TaskDriverHostJob::DisposeCreate(job),
            crate::control_runtime::preparation::HostFairnessGroup::Context(context),
        ) {
            Ok(()) => original
                .endpoint
                .ready()
                .clear_capacity_wait(original.endpoint.external().slot())
                .map_err(ContextRequestError::Ready)?,
            Err((error, job)) => {
                let super::task_driver::TaskDriverHostJob::DisposeCreate(job) = job else {
                    unreachable!("same original Create disposal");
                };
                create.disposal = Some(job);
                if let Some(before) = before {
                    original
                        .endpoint
                        .ready()
                        .park_capacity(original.endpoint.external().slot(), before)
                        .map_err(ContextRequestError::Ready)?;
                }
                if let (Some(signal), Some(before)) = (&ports.capacity, before) {
                    signal
                        .register_waiter(registration, before)
                        .map_err(|_| ContextRequestError::MissingNotificationPort)?;
                }
                if !crate::control_runtime::request_endpoint::notification_submit_busy(&error) {
                    return Err(ContextRequestError::NotificationSubmit(error));
                }
            }
        }
        Ok(())
    }
    fn advance_protocol_observation_job(
        original: &mut ContextRequestCustody,
        ports: &super::task_driver::TaskDriverPorts,
        registration: &novarocks_control_runtime::driver::ActorRegistration,
        context: novarocks_execution_contract::QueryContextRef,
    ) -> Result<(), ContextRequestError> {
        if original.protocol_observation.failed() {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        if original.pending_protocol_observation.is_none()
            && original.protocol_observation.has_pending()
        {
            let events = ports
                .protocol_events
                .as_ref()
                .ok_or(ContextRequestError::MissingNotificationPort)?;
            original.pending_protocol_observation = original.protocol_observation.take_job(
                events,
                original.work.scope,
                original.work.work,
                crate::control_runtime::request_endpoint::RequestNotice(
                    original.endpoint.external().slot().clone(),
                ),
            );
        }
        let Some(job) = original.pending_protocol_observation.take() else {
            return Ok(());
        };
        let Some(executor) = &ports.lifecycle else {
            original.pending_protocol_observation = Some(job);
            return Err(ContextRequestError::MissingNotificationPort);
        };
        let before = ports.capacity.as_ref().map(|signal| signal.revision());
        match executor.try_submit_for_group(
            super::task_driver::TaskDriverHostJob::RequestObservation(job),
            crate::control_runtime::preparation::HostFairnessGroup::Context(context),
        ) {
            Ok(()) => original
                .endpoint
                .ready()
                .clear_capacity_wait(original.endpoint.external().slot())
                .map_err(ContextRequestError::Ready)?,
            Err((error, job)) => {
                let super::task_driver::TaskDriverHostJob::RequestObservation(job) = job else {
                    unreachable!("same original protocol observation");
                };
                original.pending_protocol_observation = Some(job);
                if let Some(before) = before {
                    original
                        .endpoint
                        .ready()
                        .park_capacity(original.endpoint.external().slot(), before)
                        .map_err(ContextRequestError::Ready)?;
                }
                if let (Some(signal), Some(before)) = (&ports.capacity, before) {
                    signal
                        .register_waiter(registration, before)
                        .map_err(|_| ContextRequestError::MissingNotificationPort)?;
                }
                if !crate::control_runtime::request_endpoint::notification_submit_busy(&error) {
                    return Err(ContextRequestError::NotificationSubmit(error));
                }
            }
        }
        Ok(())
    }
    pub(crate) fn notify_deferred_root_source_progress(&mut self, task: TaskIdentity) {
        self.resources.root_reads.source_progress(task);
    }
    pub(crate) fn advance_root_reads(
        &mut self,
        _ports: &super::task_driver::TaskDriverPorts,
        budget: usize,
    ) -> Result<bool, ContextRequestError> {
        for work in self.resources.root_reads.take_cut_retries(budget) {
            if let Some(original) = self.resources.requests.get(&work) {
                original.endpoint.external().notify();
            }
        }
        Ok(self.resources.root_reads.cut_retries_remaining())
    }
}

impl ContextRequestCustody {
    /// Some means this R has not handed its fact to Core and is still wholly
    /// governed by its original deferred/unused owner, not generic input logic.
    fn prepare_deferred_root(
        &mut self,
        task: Option<&mut TaskHostRecord>,
    ) -> Result<Option<bool>, ContextRequestError> {
        let Some(mut root) = self.deferred_root.take() else {
            return Ok(None);
        };
        let result = self.prepare_deferred_root_inner(&mut root, task);
        self.deferred_root = Some(root);
        result
    }
    /// Retrying this operation uses the same original input proof and once-only
    /// cell. Failure does not revoke its already admitted sender or input.
    fn try_bind_deferred_root_reply(
        &self,
        author: &novarocks_control_runtime::driver::InputAuthor,
    ) -> Result<bool, DriverError> {
        match self.endpoint.reply().bind_input(&self.runtime, author) {
            Ok(()) => Ok(true),
            Err(DriverError::ActorFailed) => {
                // A Core poller can fail independently of this host turn.
                // Only this actual incarnation's failure permits waiting for
                // its original failure publisher instead of sending the ACK.
                let _original_failure = self
                    .runtime
                    .failure_proof_for(self.core_consumer.registration())?;
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
    fn prepare_deferred_root_inner(
        &mut self,
        root: &mut DeferredRootRequest,
        task: Option<&mut TaskHostRecord>,
    ) -> Result<Option<bool>, ContextRequestError> {
        if self.notification.snapshot().failure.is_some() || root.binding_refusal.is_some() {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        let acknowledged = root.shared.end_acknowledged();
        let failed_before_binding = root.funding.is_some()
            && acknowledged
            && self
                .runtime
                .snapshot(self.actor)
                .map_err(ContextRequestError::Driver)?
                .failed;
        if failed_before_binding && !self.reply_completed {
            // Only the actual failure session may publish this original R's
            // OwnerFailed result. Its ACK cannot be delivered to a dead Core.
            return Ok(Some(false));
        }
        if root.funding.is_some() && (!acknowledged || failed_before_binding) {
            if !self.endpoint.external_ended() || !self.notification.is_idle() {
                return Ok(Some(false));
            }
            if !self.retire_root_data_inner(root)? {
                return Ok(Some(false));
            }
            // No causal input was ever submitted, so this original source
            // was never armed. Withdraw it explicitly before the same unused
            // original R can release its own Requests/metadata funding.
            self.header_ready.cancel();
            root.unused = Some(ContextRequestPartial::from_task_funding(
                root.funding.take().expect("same unused Root R"),
                self.ledger.clone(),
            ));
        }
        if let Some(unused) = &mut root.unused {
            if !unused.try_retire()? {
                root.shared.notice.notify();
                return Ok(Some(false));
            }
            self.endpoint
                .ready()
                .unregister(self.endpoint.external().slot())
                .map_err(ContextRequestError::Ready)?;
            return Ok(Some(true));
        }
        if let Some(funding) = root.funding.take() {
            let metadata = self.endpoint.metadata();
            let author_report = funding.author.report.clone();
            if let Some(task) = task {
                use crate::task_proposal_ingress::CutSourceAdmission;
                let admitted = match task.bind_request_author(
                    funding.author,
                    metadata,
                    InputClass::Control,
                    1,
                    CutSourceAdmission::Fresh(funding.sources),
                ) {
                    Ok(admitted) => admitted,
                    Err(refusal) => {
                        use crate::control_runtime::causal::RpcAuthorAdmissionRefusal;
                        match refusal {
                            RpcAuthorAdmissionRefusal::BeforeCut {
                                error,
                                admitted,
                                admission: CutSourceAdmission::Fresh(sources),
                            } => {
                                root.funding = Some(TaskRequestFunding {
                                    author: admitted,
                                    sources,
                                    outer: funding.outer,
                                    header_ready: funding.header_ready,
                                });
                                use crate::control_runtime::causal::RpcAuthorAdmissionError;
                                if matches!(
                                    error,
                                    RpcAuthorAdmissionError::Causal(CausalError::Driver(
                                        DriverError::ActorFailed
                                            | DriverError::Closed
                                            | DriverError::FailurePublicationRequired
                                    ))
                                ) && self
                                    .runtime
                                    .snapshot(self.actor)
                                    .map_err(ContextRequestError::Driver)?
                                    .failed
                                {
                                    // The exact BeforeCut refusal returned all unissued
                                    // originals. Await their real failure publication;
                                    // a bound cut/refused input never takes this path.
                                    return Ok(Some(false));
                                }
                                let error = match error {
                                    RpcAuthorAdmissionError::Causal(
                                        CausalError::Cut(novarocks_worker_protocol::proposal_summary::CutError::Full)
                                        | CausalError::Source(crate::control_runtime::source_positions::SourcePositionError::Full)
                                        | CausalError::RequestAdmissionClosed
                                    ) => {
                                        // No cut/input was created. A closed issuer is
                                        // not proof that its existing prefix retired.
                                        // Keep this same R and unused Source2 until
                                        // real Task retirement or source progress.
                                        root.cut_waiting = true;
                                        return Ok(Some(false));
                                    }
                                    RpcAuthorAdmissionError::Causal(error) => ContextRequestError::Causal(error),
                                    RpcAuthorAdmissionError::Binding(error) => ContextRequestError::Binding(error),
                                };
                                return Err(error);
                            }
                            original => {
                                // Shared binding failure retains the actual whole cut
                                // and payment, never a replacement role or refund.
                                root.binding_refusal = Some(TaskRequestBindPartial {
                                    _original_refusal: original,
                                    _original_outer: funding.outer,
                                });
                                return Err(ContextRequestError::SubmissionUnresolved);
                            }
                        }
                    }
                };
                // Store all admitted originals before a concurrent Core
                // failure can refuse the once-only reply binding. Its failure
                // publisher then still owns the exact never-sent sender.
                root.cut_waiting = false;
                self.role = Some(admitted.role);
                self.authority = Some(admitted.authority);
                self.report = author_report;
                self.task_route = Some(TaskRequestRoute::new(
                    root.task,
                    admitted.cut,
                    admitted.sender,
                    funding.outer,
                ));
                if !self
                    .try_bind_deferred_root_reply(&admitted.input_author)
                    .map_err(ContextRequestError::Driver)?
                {
                    return Ok(Some(false));
                }
                let mut route = self.task_route.take().expect("same original Root R route");
                let result = route.submit_root_end_acknowledged(self, task, &root.shared);
                self.task_route = Some(route);
                result?;
            } else if root.retained {
                // Only the installed token plus its actual Task-retired receipt
                // proves that this same R no longer needs a live Task prefix.
                let scope = funding.author.work.scope().clone();
                let permit = match self.runtime.reserve_admitted_input_for(
                    self.core_consumer.registration(),
                    self.actor,
                    InputClass::Control,
                    &funding.author.work,
                ) {
                    Ok(permit) => permit,
                    Err(error) => {
                        root.funding = Some(funding);
                        if matches!(
                            error,
                            DriverError::ActorFailed
                                | DriverError::Closed
                                | DriverError::FailurePublicationRequired
                        ) && self
                            .runtime
                            .snapshot(self.actor)
                            .map_err(ContextRequestError::Driver)?
                            .failed
                        {
                            return Ok(Some(false));
                        }
                        return Err(ContextRequestError::Driver(error));
                    }
                };
                root.cut_waiting = false;
                let input = permit.id();
                self.permit = Some(permit);
                self.request = Some(RequestContext {
                    token: input,
                    author: self.work,
                    operation: metadata.operation,
                    deadline: metadata.deadline,
                });
                root.unused_sources = Some(crate::control_runtime::source_slot_issuer::SourceSlotIssuePartial::from_admissions(&scope, funding.sources));
                // No Task source bank exists after actual retirement. Retain the
                // same outer payer through its real direct input/delivery exit.
                root.direct_outer = Some(funding.outer);
                let reported = crate::control_runtime::admitted_role::adopt_reported_work(
                    funding.author.work,
                    funding.author.capacity,
                    funding.author.report,
                );
                let (role, authority) = ExitRole::from_admitted(reported);
                self.role = Some(role);
                self.authority = Some(authority);
                self.report = author_report;
                // Keep every original in this R before any fallible operation.
                // Binding the real input retains this neutral ACK for the
                // existing stage-authenticated failed-input cleanup if the
                // reply binding or subsequent fill observes Core failure.
                self.authority
                    .as_mut()
                    .expect("original deferred R authority")
                    .start()
                    .map_err(ContextRequestError::Exit)?;
                self.started = true;
                let permit = self.permit.as_ref().expect("original deferred R input");
                permit.bind().map_err(ContextRequestError::Driver)?;
                let author = permit.author().map_err(ContextRequestError::Driver)?;
                let message = WorkerMessage::Context(ContextMessage::Command {
                    work: self.work,
                    request: Some(metadata),
                    command: ContextInput::RootEndAcknowledged { task: root.task },
                });
                match self.try_bind_deferred_root_reply(&author) {
                    Ok(true) => {}
                    Ok(false) => {
                        self.failed_message = Some(RefusedRequestInput {
                            error: DriverError::ActorFailed,
                            message,
                        });
                        return Ok(Some(false));
                    }
                    Err(error) => {
                        self.failed_message = Some(RefusedRequestInput { error, message });
                        return Err(ContextRequestError::Driver(error));
                    }
                }
                match self
                    .permit
                    .take()
                    .expect("one original deferred R fill")
                    .fill(message)
                {
                    Ok(actual) => {
                        assert_eq!(actual, input);
                        self.submitted = true;
                    }
                    Err((error, message)) => {
                        self.failed_message = Some(RefusedRequestInput { error, message });
                        return Err(ContextRequestError::Driver(error));
                    }
                }
            } else {
                root.funding = Some(funding);
                return Err(ContextRequestError::UnknownTask);
            }
        }
        Ok(None)
    }
    fn retire_root_data(&mut self) -> Result<bool, ContextRequestError> {
        let Some(mut root) = self.deferred_root.take() else {
            return Ok(true);
        };
        let result = self.retire_root_data_inner(&mut root);
        self.deferred_root = Some(root);
        result
    }
    fn retire_root_data_inner(
        &mut self,
        root: &mut DeferredRootRequest,
    ) -> Result<bool, ContextRequestError> {
        if let Some(partial) = &mut root.data_partial {
            if !partial.try_retire()? {
                root.shared.notice.notify();
                return Ok(false);
            }
            root.data_partial = None;
        }
        if let Some(data) = &mut root.data {
            match data.role.phase() {
                ExitRolePhase::Admitted | ExitRolePhase::Started => return Ok(false),
                ExitRolePhase::Unresolved => return Err(ContextRequestError::SubmissionUnresolved),
                _ => {}
            }
            if !data.consumed {
                data.report
                    .consume(core_consumer())
                    .map_err(ContextRequestError::Runtime)?;
                data.consumed = true;
            }
            if !data.observer.is_retired() {
                // This actual data body was admitted under the Context
                // lifetime; its request author can instead be process-owned.
                // Reap the recorded original owner, never the R's Scope.
                self.ledger
                    .reap_scope_ready(data.work.scope, 1)
                    .map_err(ContextRequestError::Runtime)?;
                if !data.observer.is_retired() {
                    root.shared.notice.notify();
                    return Ok(false);
                }
            }
            if data.role.phase() != ExitRolePhase::Retired
                && !data.role.try_retire().map_err(ContextRequestError::Exit)?
            {
                return Ok(false);
            }
        }
        if let Some(sources) = &mut root.unused_sources
            && !sources
                .try_retire(&self.ledger)
                .map_err(ContextRequestError::SourceSlots)?
        {
            root.shared.notice.notify();
            return Ok(false);
        }
        Ok(true)
    }
}

impl ContextHostRecord {
    /// Only the actual Task read-seal transfer can authorize a post-source R.
    /// An absent Task owner, terminal state or numeric identity is insufficient.
    pub(crate) fn submit_retired_task_command(
        &mut self,
        work: ScopedWorkRef,
        command: ContextCommand,
    ) -> Result<(), ContextRequestSubmitRefusal> {
        let task = match &command {
            ContextCommand::Cancel { task, .. }
            | ContextCommand::GetFinalInfo { task }
            | ContextCommand::GetDynamicFilters { task, .. } => *task,
            _ => {
                return Err(ContextRequestSubmitRefusal {
                    error: ContextRequestError::InvalidClass,
                    command: Some(command),
                });
            }
        };
        let Some(proof) = self.retired_task_reads.get(&task).cloned() else {
            return Err(ContextRequestSubmitRefusal {
                error: ContextRequestError::TaskPredecessorRequired,
                command: Some(command),
            });
        };
        let Some(original) = self.resources.requests.get_mut(&work) else {
            return Err(ContextRequestSubmitRefusal {
                error: ContextRequestError::UnknownRequest,
                command: Some(command),
            });
        };
        if original.submitted || original.retired_task_read.is_some() {
            return Err(ContextRequestSubmitRefusal {
                error: ContextRequestError::WrongPhase,
                command: Some(command),
            });
        }
        original.retired_task_read = Some(proof);
        original.submit(command)
    }
}

impl ContextHostRecord {
    /// Attach one return cell to this already-admitted Task read. The generic
    /// reply endpoint stays neutral; this is the same R's optional read body.
    pub(crate) fn attach_dynamic_filter_read(
        &mut self,
        work: ScopedWorkRef,
        task: TaskIdentity,
    ) -> Result<
        Arc<crate::control_runtime::task_read_payload::TaskReadPayloadShared>,
        ContextRequestError,
    > {
        let registration = self.registration().clone();
        let financial_scope = self.request_scope().clone();
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .ok_or(ContextRequestError::UnknownRequest)?;
        if original.expected_task != Some(task)
            || original.actor != self.actor
            || original.work != work
            || !original
                .core_consumer
                .registration()
                .same_registration(&registration)
            || !original.scope.same_scope(&financial_scope)
            || original.read_payload.is_some()
            || original.submitted
        {
            return Err(ContextRequestError::WrongRequest);
        }
        let shared = Arc::new(
            crate::control_runtime::task_read_payload::TaskReadPayloadShared::new(
                crate::control_runtime::request_endpoint::RequestNotice(
                    original.endpoint.external().slot().clone(),
                ),
            ),
        );
        original.read_payload = Some(shared.clone());
        Ok(shared)
    }

    /// The Core has selected the exact original payload. Pin it with the
    /// already-started R authority before completing the mechanical effect.
    /// No getter, codec or foreign destructor runs in this metadata turn.
    pub(super) fn prepare_dynamic_filter_read(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ContextRequestError> {
        self.control_runtime
            .validate_effect_ticket(ticket)
            .map_err(ContextRequestError::Driver)?;
        let WorkerAction::Context(
            novarocks_worker_protocol::context::ContextEffect::PrepareReadPayload {
                request,
                task,
                version,
                payload,
            },
        ) = &ticket.effect
        else {
            return Err(ContextRequestError::WrongRequest);
        };
        let work = *self
            .request_inputs
            .get(request)
            .ok_or(ContextRequestError::UnknownRequest)?;
        let registration = self.registration().clone();
        let financial_scope = self.request_scope().clone();
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .ok_or(ContextRequestError::UnknownRequest)?;
        if original.expected_task != Some(*task)
            || original.actor != self.actor
            || original.work != work
            || original.request().token != *request
            || original.request().author != original.work
            || !original.started
            || !original
                .core_consumer
                .registration()
                .same_registration(&registration)
            || !original.scope.same_scope(&financial_scope)
        {
            return Err(ContextRequestError::WrongRequest);
        }
        let shared = original
            .read_payload
            .as_ref()
            .ok_or(ContextRequestError::WrongRequest)?
            .clone();
        let owner = match self.tasks.get_mut(task) {
            Some(
                super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner),
            ) => owner,
            _ => return Err(ContextRequestError::UnknownTask),
        };
        let inbox = owner
            .dynamic_filters()
            .ok_or(ContextRequestError::WrongRequest)?;
        let authority = original
            .authority
            .take()
            .ok_or(ContextRequestError::WrongPhase)?;
        let loan = match inbox.loan_selected(*task, *version, *payload, authority) {
            Ok(loan) => loan,
            Err((error, authority)) => {
                original.authority = Some(authority);
                return Err(ContextRequestError::DynamicFilter(error));
            }
        };
        if let Err((error, loan)) = shared.install_ready(loan) {
            original.authority = Some(loan.reclaim());
            return Err(ContextRequestError::ReadPayload(error));
        }
        Ok(())
    }
}
