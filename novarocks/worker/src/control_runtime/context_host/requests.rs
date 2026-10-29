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
mod task_funding;
mod task_route;
use super::root_read::{DeferredRootRequest, RootReadData, RootReadDataGuard, RootReadShared};
pub(super) use task_funding::TaskRequestFunding;

use endpoint::ContextRequestEndpoint;
use task_route::TaskRequestRoute;

use super::ContextHostRecord;
use crate::control_runtime::source_slot_issuer::{SourceSlotIssueError, SourceSlotIssuePartial};
use crate::control_runtime::{
    actors::{ContextMessage, RequestMetadata, WorkerAction, WorkerActor, WorkerMessage},
    causal::CausalError,
    control_request::ControlRequestError,
    request_endpoint::{RequestEndpoint, RequestHandle},
    request_ready::{ControlRequestReady, RequestExternalLeg},
    task::TaskHostRecord,
};
use crate::control_runtime_policy::{PolicyError, StorageNeed};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{
        ActorRuntime, DriverError, EffectCompletionRefusal, EffectTicket, InputPermit, InputPhase,
    },
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::{CapacityBundle, WorkLease, WorkRetirementObserver},
    publication::{FailurePublicationSession, PublishedReply},
    reported_work::{ReportedWork, ReportedWorkBindingError},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    completion::ConsumerKind,
    identity::{ActionId, ActorId, ConsumerId, Generation, InputId, WorkId},
    mailbox::InputClass,
    scope::WorkKind,
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
    TaskPredecessorRequired,
    Causal(CausalError),
    SourceSlots(SourceSlotIssueError),
    TaskNotReady,
    TaskUnpublished,
    UnknownTask,
    BodyResultMismatch,
    RootReadCapacity,
    MissingNotificationPort,
    Observation(super::observation::ContextObservationError),
    NotificationSubmit(crate::control_runtime::host_executor::HostSubmitError),
    Domain(domain::DomainHostError),
}

#[derive(Debug)]
pub(crate) struct ContextRequestAdmissionRefusal {
    pub error: ContextRequestError,
    /// The actual Context retains this partial original; this address is not
    /// a caller-owned replacement Work or a successful RPC admission.
    pub partial: Option<WorkId>,
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
        original: ContextRequestSubmitRefusal,
        handle: ContextRequestHandle,
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
                handle: _,
            } => f
                .debug_struct("CreationInspectionSubmission")
                .field("original", original)
                .field("retains_external_handle", &true)
                .finish(),
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
    capacity: CapacityBundle,
    report: Option<ReportSlot>,
    permit: Option<InputPermit<WorkerActor>>,
    work_retirement: WorkRetirementObserver,
    report_revoked: bool,
    exited: bool,
    unresolved: bool,
    outer: Option<CapacityBundle>,
    sources: Option<SourceSlotIssuePartial>,
}

pub(crate) enum ContextRequestAdmissionPartial {
    Unused(Box<ContextRequestPartial>),
    BoundTask(Box<TaskRequestBindPartial>),
}
impl ContextRequestAdmissionPartial {
    fn try_retire(&mut self) -> Result<bool, ContextRequestError> {
        match self {
            Self::Unused(partial) => partial.try_retire(),
            Self::BoundTask(_) => Err(ContextRequestError::SubmissionUnresolved),
        }
    }
}
/// An unexpected whole-binding refusal can already own a real cut/sender.
/// Retain it for the actual failed-owner inventory; never revoke it as an
/// untouched source or invent a replacement main role to make cleanup pass.
pub(crate) struct TaskRequestBindPartial {
    refusal: crate::control_runtime::causal::RpcAuthorAdmissionRefusal,
    outer: CapacityBundle,
}

impl ContextRequestPartial {
    fn try_retire(&mut self) -> Result<bool, ContextRequestError> {
        if self.unresolved {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        if let Some(sources) = &mut self.sources
            && !sources
                .try_retire(&self.ledger)
                .map_err(ContextRequestError::SourceSlots)?
        {
            return Ok(false);
        }
        if let Some(permit) = self.permit.take()
            && let Err(error) = permit.revoke_unused()
        {
            self.unresolved = true;
            return Err(ContextRequestError::Driver(error));
        }
        if !self.report_revoked {
            if let Some(report) = &self.report {
                report
                    .revoke_unused()
                    .map_err(ContextRequestError::Runtime)?;
            }
            self.report_revoked = true;
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
        }
        if let Some(report) = &self.report
            && !report.try_retire().map_err(ContextRequestError::Runtime)?
        {
            return Ok(false);
        }
        // The report handle is never queried after its actual retirement.
        self.report = None;
        if !self.work_retirement.is_retired() {
            self.ledger
                .reap_scope_ready(self.work.scope().id(), 1)
                .map_err(ContextRequestError::Runtime)?;
            if !self.work_retirement.is_retired() {
                return Ok(false);
            }
        }
        if !self.capacity.is_released() {
            let remaining = self
                .capacity
                .resources()
                .map_err(ContextRequestError::Runtime)?;
            self.capacity
                .release(&remaining)
                .map_err(ContextRequestError::Runtime)?;
        }
        if let Some(outer) = &mut self.outer
            && !outer.is_released()
        {
            let remaining = outer.resources().map_err(ContextRequestError::Runtime)?;
            outer
                .release(&remaining)
                .map_err(ContextRequestError::Runtime)?;
        }
        Ok(true)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CoreReceipt {
    Pending,
    Consumed,
    Abandoned,
}

#[must_use = "The actual Context retains its request until every original leg retires"]
pub(crate) struct ContextRequestCustody {
    runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    actor: ActorId,
    generation: Generation,
    work: WorkId,
    endpoint: ContextRequestEndpoint,
    request: Option<RequestContext>,
    task_route: Option<TaskRequestRoute>,
    domain: Option<domain_driver::DomainRequest>,
    permit: Option<InputPermit<WorkerActor>>,
    role: Option<ExitRole>,
    authority: Option<ExitAuthority>,
    report: ReportSlot,
    work_retirement: WorkRetirementObserver,
    expected_task: Option<TaskIdentity>,
    submitted: bool,
    started: bool,
    failed_message: Option<WorkerMessage>,
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
    retired_task_read: Option<Arc<super::super::lifecycle_read::TaskReadSnapshot>>,
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
impl ContextRequestCustody {
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
        let scope = host.scope();
        let generation = scope
            .gates()
            .map_err(|e| (ContextRequestError::Runtime(e), None))?
            .generation;
        let guard = scope
            .reserve_child_next(
                generation,
                host.resources.roles[super::LIFETIME].capacity.id(),
                CONTEXT_REQUEST_STORAGE
                    .resources()
                    .map_err(|e| (ContextRequestError::Policy(e), None))?,
                WorkKind::Settlement,
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
            scope,
            generation,
            partial.work.id(),
            2,
            [
                (
                    core_consumer(),
                    ConsumerKind::Owner {
                        scope: scope.id(),
                        generation,
                    },
                ),
                (external_consumer(), ConsumerKind::Observer),
            ],
        ) {
            Ok(report) => report,
            Err(error) => return Err((ContextRequestError::Runtime(error), Some(partial))),
        };
        partial.report = Some(report.clone());
        let permit = match host.control_runtime.reserve_admitted_input(
            host.actor,
            source,
            class,
            &partial.work,
        ) {
            Ok(permit) => permit,
            Err(error) => return Err((ContextRequestError::Driver(error), Some(partial))),
        };
        let request = RequestContext {
            token: permit.id(),
            operation,
            deadline,
        };
        partial.permit = Some(permit);
        let slot = match ready.register_context(scope, partial.work.id(), host.registration()) {
            Ok(slot) => slot,
            Err(error) => return Err((ContextRequestError::Ready(error), Some(partial))),
        };
        let work = partial.work.id();
        let permit = partial.permit.take();
        let reported = match ReportedWork::try_new(partial.work, partial.capacity, report.clone()) {
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
            reply: PublishedReply::new(scope),
            external: RequestExternalLeg::new(slot),
        });
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
                actor: host.actor,
                generation,
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
                retired_task_read: None,
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
    pub(crate) fn work(&self) -> WorkId {
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
        let sealed_read = matches!(&command,
            ContextCommand::GetFinalInfo { task } | ContextCommand::GetDynamicFilters { task, .. }
                if self.retired_task_read.as_ref().is_some_and(|original| original.status.identity() == *task));
        if !sealed_read
            && matches!(
                command,
                ContextCommand::Cancel { .. }
                    | ContextCommand::UpdateTask { .. }
                    | ContextCommand::RootDrained { .. }
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
                self.failed_message = Some(original);
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
        if let Some(route) = route.as_mut() {
            route.prepare(
                self,
                task.as_deref_mut()
                    .ok_or(ContextRequestError::UnknownTask)?,
            )?;
            if self.submitted && self.request.is_none() && !route.abandoned_before_fill() {
                return Ok(false);
            }
        }
        let abandoned_before_fill = route
            .as_ref()
            .is_some_and(|route| route.abandoned_before_fill());
        if self.domain.as_ref().is_some_and(|domain| !domain.retired) {
            return Ok(false);
        }
        if let Some(create) = &mut self.create {
            if !create.transferred {
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
        }
        if self.protocol_observation.failed() || self.notification.snapshot().failure.is_some() {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        if self.failed_message.is_some() || self.unresolved_revocation {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        if !self.submitted {
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
                        .report
                        .consume(core_consumer())
                        .map_err(ContextRequestError::Runtime)?,
                    CoreReceipt::Abandoned => self
                        .report
                        .abandon_failed_owner(core_consumer(), &self.scope, self.generation)
                        .map_err(ContextRequestError::Runtime)?,
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
        if !self.work_retirement.is_retired() {
            self.ledger
                .reap_scope_ready(self.scope.id(), 1)
                .map_err(ContextRequestError::Runtime)?;
            if !self.work_retirement.is_retired() {
                self.endpoint.external().notify();
                return Ok(false);
            }
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
            let task = task.ok_or(ContextRequestError::UnknownTask)?;
            if !route.finish(self, task)? {
                return Ok(false);
            }
        }
        if let Some(root) = &mut self.deferred_root {
            if let Some(outer) = &mut root.direct_outer {
                outer
                    .release(&outer.resources().map_err(ContextRequestError::Runtime)?)
                    .map_err(ContextRequestError::Runtime)?;
            }
            root.direct_outer = None;
        }
        self.endpoint
            .ready()
            .unregister(self.endpoint.external().slot())
            .map_err(ContextRequestError::Ready)?;
        Ok(true)
    }
}

impl ContextHostRecord {
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
        let partial = partial.map(|partial| {
            let work = partial.work.id();
            assert!(
                self.resources
                    .request_partials
                    .insert(
                        work,
                        ContextRequestAdmissionPartial::Unused(Box::new(partial))
                    )
                    .is_none()
            );
            if let Some(notice) = &self.resources.notice {
                notice.notify();
            }
            work
        });
        ContextRequestAdmissionRefusal { error, partial }
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
    ) -> Result<(WorkId, ContextRequestHandle), ContextRequestAdmissionRefusal> {
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
            Err(original) => Err(ContextCreationInspectionRefusal::Submission { original, handle }),
        }
    }
    fn admit_request_kind(
        &mut self,
        ready: &ControlRequestReady,
        source: ActorId,
        kind: RequestAdmissionKind,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(WorkId, ContextRequestHandle), ContextRequestAdmissionRefusal> {
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
        if snapshot.failed && matches!(kind, RequestAdmissionKind::CreationInspection) {
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
        work: WorkId,
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
        work: WorkId,
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
        original.publish(ticket)
    }
    /// Finish the same original Reply action after the live failure owner has
    /// preserved the immutable first outbox commitment. This settles action
    /// metadata only, never the external delivery or any physical R holder.
    pub(crate) fn finish_failed_reply_effect(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (ContextRequestError, EffectTicket<WorkerAction>)> {
        if let Err(error) = stage.validate_scope(&self.control_runtime, self.actor, self.scope()) {
            return Err((ContextRequestError::Driver(error), ticket));
        }
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
        Ok(())
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
    pub(crate) fn advance_request(&mut self, work: WorkId) -> Result<bool, ContextRequestError> {
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
            let retired = original.try_retire(task)?;
            if let Some(root) = &original.deferred_root {
                self.resources
                    .root_reads
                    .set_cut_waiting(root.task, work, root.cut_waiting);
            }
            if let Some(request) = original.request {
                if let Some(old) = self.request_inputs.insert(request.token, work) {
                    assert_eq!(old, work);
                }
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
            return Ok(true);
        }
        if let Some(original) = self.resources.request_partials.get_mut(&work) {
            if !original.try_retire()? {
                // A bounded reaper may have retired a different ready sibling.
                // This original partial has no request-ready slot of its own.
                if let Some(notice) = &self.resources.notice {
                    notice.notify();
                }
                return Ok(false);
            }
            self.resources.request_partials.remove(&work);
            return Ok(true);
        }
        Err(ContextRequestError::UnknownRequest)
    }
}

/// The original admitted R retains the opaque Create capture until a real
/// loader takes it, or a physical cleanup body returns in its same paid slot.
struct TaskCreateCapture {
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
        work: WorkId,
        payload: crate::control_runtime::loading::TaskCreatePayload,
        binding: crate::control_runtime::task::TaskHostBinding,
    ) -> Result<(), TaskCreateSubmitRefusal> {
        let reject = |error, payload, binding| TaskCreateSubmitRefusal {
            error,
            originals: Some((payload, binding)),
        };
        if binding.context != self.identity {
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
            WorkId,
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
        work: WorkId,
        payload: crate::control_runtime::loading::TaskCreatePayload,
    ) {
        let capture = self
            .resources
            .requests
            .get_mut(&work)
            .expect("original R")
            .create
            .as_mut()
            .expect("original capture");
        assert!(capture.payload.is_none());
        capture.payload = Some(std::mem::ManuallyDrop::new(payload));
    }
    pub(super) fn create_transferred_to_loader(
        &mut self,
        work: WorkId,
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
        task: TaskIdentity,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<
        (WorkId, TaskContextRequestHandle, Arc<RootReadShared>),
        ContextRequestAdmissionRefusal,
    > {
        let reject = |error| ContextRequestAdmissionRefusal {
            error,
            partial: None,
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
        if snapshot.sealed || self.root_issuance_sealed || self.resources.control_admission_closed {
            return Err(reject(ContextRequestError::AdmissionClosed));
        }
        let retained = self.resources.root_reads.has_retained(task);
        if !retained
            && !matches!(self.tasks.get(&task), Some(super::TaskHostOwner::Ready(host)) if host.request_admission_open())
        {
            return Err(reject(ContextRequestError::TaskNotReady));
        }
        let funding = self.reserve_task_request_funding()?;
        let work = funding.author.work.id();
        let work_retirement = funding.author.work.retirement_observer();
        let report = funding.author.report.clone();
        if self.resources.root_reads.reserve(task, work).is_err() {
            let partial = ContextRequestPartial::from_task_funding(funding, self.ledger.clone());
            return Err(
                self.retain_request_partial(ContextRequestError::RootReadCapacity, Some(partial))
            );
        }
        let slot = match ready.register_context(self.scope(), work, self.registration()) {
            Ok(slot) => slot,
            Err(error) => {
                self.resources.root_reads.release(task, work);
                let partial =
                    ContextRequestPartial::from_task_funding(funding, self.ledger.clone());
                return Err(
                    self.retain_request_partial(ContextRequestError::Ready(error), Some(partial))
                );
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
        let endpoint = Arc::new(RequestEndpoint {
            request: RequestMetadata {
                operation,
                deadline,
            },
            ready: ready.clone(),
            reply: PublishedReply::new(self.scope()),
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
            scope: self.scope().clone(),
            actor: self.actor,
            generation: self.initial_generation,
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
            notification,
            pending_notification: None,
            protocol_observation: Default::default(),
            pending_protocol_observation: None,
            protocol_observation_kind: Default::default(),
            release_observation: None,
            observation: None,
            retired_task_read: None,
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
        request_work: WorkId,
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
        let work_id = work.id();
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
            &scope,
            generation,
            work_id,
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
        let holder = RootReadDataGuard::new(authority, root.shared.notice.clone());
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
        work: WorkId,
        ports: &super::task_driver::TaskDriverPorts,
    ) -> Result<(), ContextRequestError> {
        let registration = self.registration().clone();
        let Some(original) = self.resources.requests.get_mut(&work) else {
            return Ok(());
        };
        Self::advance_protocol_observation_job(original, ports, &registration)?;
        Self::advance_unused_create(original, ports, &registration)?;
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
        match executor.try_submit(super::task_driver::TaskDriverHostJob::ReplyNotification(
            job,
        )) {
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
                original.scope.id(),
                original.work,
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
        match executor.try_submit(super::task_driver::TaskDriverHostJob::DisposeCreate(job)) {
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
                original.scope.id(),
                original.work,
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
        match executor.try_submit(super::task_driver::TaskDriverHostJob::RequestObservation(
            job,
        )) {
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
    /// Some means this R has not handed a command to Core and is still wholly
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
    fn prepare_deferred_root_inner(
        &mut self,
        root: &mut DeferredRootRequest,
        task: Option<&mut TaskHostRecord>,
    ) -> Result<Option<bool>, ContextRequestError> {
        if self.notification.snapshot().failure.is_some() || root.binding_refusal.is_some() {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        if root.funding.is_some() && !root.shared.drain_requested() {
            if !self.endpoint.external_ended() || !self.notification.is_idle() {
                return Ok(Some(false));
            }
            if !self.retire_root_data_inner(root)? {
                return Ok(Some(false));
            }
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
                                });
                                use crate::control_runtime::causal::RpcAuthorAdmissionError;
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
                                    refusal: original,
                                    outer: funding.outer,
                                });
                                return Err(ContextRequestError::SubmissionUnresolved);
                            }
                        }
                    }
                };
                root.cut_waiting = false;
                self.role = Some(admitted.role);
                self.authority = Some(admitted.authority);
                self.report = author_report;
                let mut route =
                    TaskRequestRoute::new(root.task, admitted.cut, admitted.sender, funding.outer);
                let result =
                    route.submit(self, task, ContextCommand::RootDrained { task: root.task });
                self.task_route = Some(route);
                result.map_err(|error| error.error)?;
            } else if root.retained {
                // Only the installed token plus its actual Task-retired receipt
                // proves that this same R no longer needs a live Task prefix.
                let scope = funding.author.work.scope().clone();
                let permit = match self.runtime.reserve_admitted_input(
                    self.actor,
                    self.actor,
                    InputClass::Control,
                    &funding.author.work,
                ) {
                    Ok(permit) => permit,
                    Err(error) => {
                        root.funding = Some(funding);
                        return Err(ContextRequestError::Driver(error));
                    }
                };
                root.cut_waiting = false;
                let input = permit.id();
                self.request = Some(RequestContext {
                    token: input,
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
                let (role, mut authority) = ExitRole::from_admitted(reported);
                authority.start().map_err(ContextRequestError::Exit)?;
                self.role = Some(role);
                self.authority = Some(authority);
                self.started = true;
                permit.bind().map_err(ContextRequestError::Driver)?;
                let message = WorkerMessage::Context(ContextMessage::Command {
                    work: self.work,
                    request: Some(metadata),
                    command: ContextInput::Request(ContextCommand::RootDrained { task: root.task }),
                });
                match permit.fill(message) {
                    Ok(actual) => {
                        assert_eq!(actual, input);
                        self.submitted = true;
                    }
                    Err((error, message)) => {
                        self.failed_message = Some(message);
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
                self.ledger
                    .reap_scope_ready(self.scope.id(), 1)
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
        if let Some(sources) = &mut root.unused_sources {
            if !sources
                .try_retire(&self.ledger)
                .map_err(ContextRequestError::SourceSlots)?
            {
                root.shared.notice.notify();
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl ContextHostRecord {
    /// Only the actual Task read-seal transfer can authorize a post-source R.
    /// An absent Task owner, terminal state or numeric identity is insufficient.
    pub(crate) fn submit_retired_task_read(
        &mut self,
        work: WorkId,
        command: ContextCommand,
    ) -> Result<(), ContextRequestSubmitRefusal> {
        let task = match &command {
            ContextCommand::GetFinalInfo { task }
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
