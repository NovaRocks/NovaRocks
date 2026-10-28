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

mod task_funding;

use super::ContextHostRecord;
use crate::control_runtime::{
    actors::{ContextMessage, RequestMetadata, WorkerAction, WorkerActor, WorkerMessage},
    admitted_role::dispose_unused_completion,
    control_request::ControlRequestError,
    request_endpoint::{RequestEndpoint, RequestHandle},
    request_ready::{ControlRequestReady, RequestExternalLeg},
};
use crate::control_runtime_policy::{PolicyError, StorageNeed};
use crate::task_completion_roles::AdmittedCompletionRole;
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{
        ActorRuntime, DriverError, EffectCompletionRefusal, EffectTicket, InputPermit, InputPhase,
    },
    exit_role::{ExitAuthority, ExitRole, ExitRoleError},
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
    context::{ContextCommand, ContextCore, ContextInput, ContextReplyBody, RequestResult},
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
    effects: ContextCore::MAX_STEP_WORK,
    cuts: 0,
    replies: 1,
    completions: 1,
    captures: 1,
    work: 1,
};

fn core_consumer() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}
fn external_consumer() -> ConsumerId {
    ConsumerId::new(2).unwrap()
}

pub(crate) type ContextRequestHandle = RequestHandle<ContextReplyBody>;

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
    MissingCoreReceipt,
    SubmissionUnresolved,
    TaskPredecessorRequired,
    BodyResultMismatch,
}

#[derive(Debug)]
pub(crate) struct ContextRequestAdmissionRefusal {
    pub error: ContextRequestError,
    /// The actual Context retains this partial original; this address is not
    /// a caller-owned replacement Work or a successful RPC admission.
    pub partial: Option<WorkId>,
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
    sources: Vec<UnusedRequestSource>,
}

struct UnusedRequestSource {
    admitted: Option<AdmittedCompletionRole>,
    retirement: WorkRetirementObserver,
}

impl ContextRequestPartial {
    fn try_retire(&mut self) -> Result<bool, ContextRequestError> {
        if self.unresolved {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        for source in &mut self.sources {
            if let Some(admitted) = source.admitted.take() {
                // These exact originals were never exposed to an ingress or
                // source producer. This call observes their actual empty exit.
                dispose_unused_completion(admitted);
            }
        }
        if self
            .sources
            .iter()
            .any(|source| !source.retirement.is_retired())
        {
            self.ledger
                .reap_scope_ready(self.work.scope().id(), 1)
                .map_err(ContextRequestError::Runtime)?;
            if self
                .sources
                .iter()
                .any(|source| !source.retirement.is_retired())
            {
                return Ok(false);
            }
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

#[derive(Clone, Copy, PartialEq, Eq)]
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
    endpoint: Arc<RequestEndpoint<ContextReplyBody>>,
    permit: Option<InputPermit<WorkerActor>>,
    role: ExitRole,
    authority: Option<ExitAuthority>,
    report: ReportSlot,
    work_retirement: WorkRetirementObserver,
    expected_task: Option<TaskIdentity>,
    submitted: bool,
    started: bool,
    failed_message: Option<WorkerMessage>,
    published: Option<(InputId, ActionId)>,
    reply_completed: bool,
    input_retired: bool,
    core_receipt: CoreReceipt,
    exit_observed: bool,
    core_acknowledged: bool,
    external_acknowledged: bool,
    unresolved_revocation: bool,
}
impl ContextRequestCustody {
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
            sources: Vec::new(),
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
        let slot = match ready.register_context(scope.id(), partial.work.id()) {
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
                        sources: Vec::new(),
                    }),
                ));
            }
        };
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
        };
        Ok((
            Self {
                runtime: host.control_runtime.clone(),
                ledger: host.ledger.clone(),
                scope: scope.clone(),
                actor: host.actor,
                generation,
                work,
                endpoint,
                permit,
                role,
                authority: Some(authority),
                report,
                work_retirement: partial.work_retirement,
                expected_task: None,
                submitted: false,
                started: false,
                failed_message: None,
                published: None,
                reply_completed: false,
                input_retired: false,
                core_receipt: CoreReceipt::Pending,
                exit_observed: false,
                core_acknowledged: false,
                external_acknowledged: false,
                unresolved_revocation: false,
            },
            handle,
        ))
    }
    pub(crate) fn request(&self) -> RequestContext {
        self.endpoint.request
    }
    pub(crate) fn work(&self) -> WorkId {
        self.work
    }
    fn submit(&mut self, command: ContextCommand) -> Result<(), ContextRequestSubmitRefusal> {
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
        if matches!(
            command,
            ContextCommand::Cancel { .. }
                | ContextCommand::UpdateTask { .. }
                | ContextCommand::RootDrained { .. }
                | ContextCommand::GetFinalInfo { .. }
                | ContextCommand::GetDynamicFilters { .. }
        ) {
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
        let message = WorkerMessage::Context(ContextMessage::Command {
            work: self.work,
            request: Some(RequestMetadata {
                operation: self.request().operation,
                deadline: self.request().deadline,
            }),
            command: ContextInput::Request(command),
        });
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
        let body = match &ticket.effect {
            WorkerAction::Reply(reply)
                if reply.request == self.request() && reply.task == self.expected_task =>
            {
                if !reply.body.matches(reply.result) {
                    return Err(reject(ContextRequestError::BodyResultMismatch, ticket));
                }
                reply.body.clone()
            }
            _ => return Err(reject(ContextRequestError::WrongRequest, ticket)),
        };
        if ticket.actor != self.actor || !self.submitted || self.reply_completed {
            return Err(reject(ContextRequestError::WrongPhase, ticket));
        }
        let key = (ticket.input, ticket.descriptor.id);
        if let Some(original) = self.published {
            if original != key {
                return Err(reject(ContextRequestError::WrongRequest, ticket));
            }
        } else {
            if let Err((error, body)) =
                self.runtime
                    .commit_reply(&ticket, &self.endpoint.reply, body)
            {
                return Err(ContextRequestPublishRefusal::Publication {
                    error,
                    ticket,
                    body,
                });
            }
            self.published = Some(key);
        }
        self.runtime
            .effect_completed(ticket)
            .map_err(ContextRequestPublishRefusal::Completion)?;
        self.reply_completed = true;
        self.endpoint.external.notify();
        Ok(())
    }
    /// S8 supplies the actual same-scope failure publication session. An
    /// already published commitment is kept, not replaced with OwnerFailed.
    pub(crate) fn publish_owner_failure(
        &mut self,
        session: &mut FailurePublicationSession<WorkerActor>,
    ) -> Result<bool, ContextRequestError> {
        if self.endpoint.reply.is_committed() {
            return Ok(false);
        }
        let body = Arc::new(ContextReplyBody::Verdict(RequestResult::Outcome(
            OperationOutcome::OwnerFailed,
        )));
        session
            .commit_reply(&self.endpoint.reply, body)
            .map_err(|(error, _)| ContextRequestError::Driver(error))?;
        self.reply_completed = true;
        self.endpoint.external.notify();
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
    fn try_retire(&mut self) -> Result<bool, ContextRequestError> {
        if self.failed_message.is_some() || self.unresolved_revocation {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        if !self.submitted {
            if !self.endpoint.external_ended() {
                return Ok(false);
            }
            if let Some(permit) = self.permit.take()
                && let Err(error) = permit.revoke_unused()
            {
                self.unresolved_revocation = true;
                return Err(ContextRequestError::Driver(error));
            }
            if !self.exit_observed {
                self.authority
                    .take()
                    .expect("original unused authority")
                    .withdraw_unpublished_and_exit()
                    .map_err(ContextRequestError::Exit)?;
                self.exit_observed = true;
            }
        } else {
            if !self.input_retired {
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
            if !self.reply_completed || !self.endpoint.external_ended() {
                return Ok(false);
            }
            drop(self.endpoint.reply.take_once());
            if !self.exit_observed {
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
                self.endpoint.external.notify();
                return Ok(false);
            }
        }
        if !self.role.try_retire().map_err(ContextRequestError::Exit)? {
            return Ok(false);
        }
        self.endpoint
            .ready
            .unregister(self.endpoint.external.slot())
            .map_err(ContextRequestError::Ready)?;
        Ok(true)
    }
}

impl ContextHostRecord {
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
                    .insert(work, partial)
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
        match self.resources.requests.get_mut(&work) {
            Some(original) => original.submit(command),
            None => Err(ContextRequestSubmitRefusal {
                error: ContextRequestError::UnknownRequest,
                command: Some(command),
            }),
        }
    }
    pub(crate) fn publish_request_reply(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), ContextRequestPublishRefusal> {
        let work = match &ticket.effect {
            WorkerAction::Reply(reply) => self.request_inputs.get(&reply.request.token).copied(),
            _ => None,
        };
        let Some(original) = work.and_then(|work| self.resources.requests.get_mut(&work)) else {
            return Err(ContextRequestPublishRefusal::Rejected {
                error: ContextRequestError::UnknownRequest,
                ticket,
            });
        };
        original.publish(ticket)
    }
    /// One original ready address per call, without scanning another Context.
    pub(crate) fn advance_request(&mut self, work: WorkId) -> Result<bool, ContextRequestError> {
        if let Some(original) = self.resources.requests.get_mut(&work) {
            let input = original.request().token;
            if !original.try_retire()? {
                return Ok(false);
            }
            self.resources.requests.remove(&work);
            self.request_inputs.remove(&input);
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
