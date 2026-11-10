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

//! Canonical QA root control and its original runtime publication target.

use super::terminal::{owner_failure, project_failure};
use crate::{
    control_runtime::actor::{FrontendActor, FrontendEffect},
    task_execution::host::{
        failure_publication::{AttemptReply, FailurePublications, RootControlReplyKind},
        observations::{
            ObservationError, OrderedObservationInput, RootControlBinding, RootControlRef,
            WeakOrderedObservationInput,
        },
        publication::{AttemptPublicationOwner, PublicationError, PublicationRecord},
    },
};
use novarocks_attempt_protocol::failure::{AttemptFailure, HostFailureRef};
use novarocks_control_runtime::actor_turn::{ActorRuntime, ActorTurnError, EffectTicket};
use novarocks_coordination_contract::{
    AttemptRequestToken, AttemptToLogical, AttemptToLogicalBody, LogicalToAttempt,
    LogicalToAttemptBody,
};
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_query_application::{
    api::QueryExecutionError,
    coordination::{AcceptedRootControlPort, AcceptedRootControlRequest},
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};
use std::{
    fmt,
    num::NonZeroU64,
    sync::{Arc, Mutex, OnceLock, Weak},
};

struct Request {
    token: AttemptRequestToken,
    kind: RootControlReplyKind,
    original: AcceptedRootControlRequest,
    reply: AttemptReply,
    failure_projection: Option<(HostFailureRef, ExecutionFailureInput)>,
}

struct Requests {
    next: u64,
    original: Option<Request>,
    delivering: bool,
    writer_error: Option<ObservationError>,
}

pub(crate) struct RootControlRequests {
    binding: RootControlBinding,
    failure: Arc<FailurePublications>,
    ingress: OnceLock<WeakOrderedObservationInput<FrontendActor>>,
    publisher: OnceLock<Weak<AttemptPublicationOwner>>,
    state: Mutex<Requests>,
}

impl RootControlRequests {
    pub(crate) fn new(binding: RootControlBinding, failure: Arc<FailurePublications>) -> Arc<Self> {
        assert!(failure.matches(binding.registration(), binding.activation()));
        let original = Arc::new(Self {
            binding,
            failure,
            ingress: OnceLock::new(),
            publisher: OnceLock::new(),
            state: Mutex::new(Requests {
                next: 1,
                original: None,
                delivering: false,
                writer_error: None,
            }),
        });
        original.failure.bind_root_requests(&original);
        original
    }

    pub(crate) fn matches(&self, binding: &RootControlBinding) -> bool {
        self.binding.same_original(binding)
    }

    pub(crate) fn bind_publisher(&self, original: &Arc<AttemptPublicationOwner>) {
        assert!(self.publisher.set(Arc::downgrade(original)).is_ok());
    }

    pub(crate) fn owner_failed(&self) -> bool {
        self.failure.root_reply_fenced()
    }

    /// The original failure Work can finish an already-committed reply if
    /// Core failed after commit and before the normal carrier delivered it.
    pub(crate) async fn deliver_committed_after_failure(&self) -> Result<(), PublicationError> {
        let token = self
            .state
            .lock()
            .expect("original root-control request")
            .original
            .as_ref()
            .filter(|request| request.reply.is_committed())
            .map(|request| request.token);
        let Some(token) = token else { return Ok(()) };
        let publisher = self
            .publisher
            .get()
            .and_then(Weak::upgrade)
            .ok_or(PublicationError::MissingRootPublisher)?;
        match publisher.deliver_committed_root(token).await {
            Ok(()) | Err(PublicationError::MissingRootControlReply) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn bind_ingress(&self, ingress: &OrderedObservationInput<FrontendActor>) {
        assert!(self.binding.same_original(ingress.root_binding()));
        assert!(self.binding.reply_author().is_some());
        assert!(self.ingress.set(ingress.weak()).is_ok());
    }

    fn reserve_request(
        &self,
        request: AcceptedRootControlRequest,
    ) -> Result<RootControlRef, AcceptedRootControlRequest> {
        let Some(author) = self.binding.reply_author() else {
            return Err(request);
        };
        let kind = match request.terminal_control_root() {
            None => RootControlReplyKind::Seal,
            Some(root) if root == self.binding.root() => RootControlReplyKind::TerminalControl,
            _ => return Err(request),
        };
        let mut state = self.state.lock().expect("original root-control request");
        if state.original.is_some() || state.delivering || state.writer_error.is_some() {
            return Err(request);
        }
        let Some(next) = state.next.checked_add(1) else {
            return Err(request);
        };
        let token = AttemptRequestToken::new(NonZeroU64::new(state.next).unwrap());
        let Ok(reply) = self.failure.reserve_bound_reply(author, token, kind) else {
            return Err(request);
        };
        state.next = next;
        state.original = Some(Request {
            token,
            kind,
            original: request,
            reply,
            failure_projection: None,
        });
        Ok(self.binding.reference(token))
    }

    pub(crate) fn project(
        &self,
        reference: &RootControlRef,
    ) -> Result<LogicalToAttempt<AttemptFailure>, ObservationError> {
        if !reference.belongs_to(&self.binding) {
            return Err(ObservationError::InvalidRootControl);
        }
        let state = self.state.lock().expect("original root-control request");
        let original = state
            .original
            .as_ref()
            .filter(|request| request.token == reference.token())
            .ok_or(ObservationError::InvalidRootControl)?;
        Ok(LogicalToAttempt {
            activation: self.binding.activation(),
            body: match original.kind {
                RootControlReplyKind::Seal => LogicalToAttemptBody::SealRequest {
                    token: original.token,
                },
                RootControlReplyKind::TerminalControl => {
                    LogicalToAttemptBody::TerminalControlDemand {
                        token: original.token,
                        root: self.binding.root(),
                    }
                }
            },
        })
    }

    /// Commit under the real failure gate while this reply ticket is still
    /// Started. Actual QA delivery follows in the original off-poller Work.
    pub(crate) fn commit_reply(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        original: Arc<AttemptToLogical<AttemptFailure>>,
        projection: Option<(HostFailureRef, ExecutionFailureInput)>,
    ) -> Result<(), (ActorTurnError, Arc<AttemptToLogical<AttemptFailure>>)> {
        let Some((token, kind)) = reply_kind(&original.body) else {
            return Err((ActorTurnError::PublicationTargetMismatch, original));
        };
        if original.activation != self.binding.activation()
            || ticket.descriptor.id.get() != original.action.get()
        {
            return Err((ActorTurnError::PublicationTargetMismatch, original));
        }
        let target = self
            .state
            .lock()
            .expect("original root-control request")
            .original
            .as_mut()
            .filter(|request| request.token == token && request.kind == kind)
            .map(|request| {
                request.failure_projection = projection;
                request.reply.clone()
            });
        let Some(target) = target else {
            return Err((ActorTurnError::PublicationTargetMismatch, original));
        };
        runtime.commit_reply(ticket, &target, original)
    }

    /// Run outside the poller and publication gate. The unique original
    /// success publisher moves only for a committed successful Seal.
    pub(crate) fn deliver(
        &self,
        token: AttemptRequestToken,
        publication: &mut PublicationRecord,
    ) -> Result<(), PublicationError> {
        let Some((request, reply)) = self.take_committed(token) else {
            return Err(PublicationError::MissingRootControlReply);
        };
        let result = match &reply.body {
            AttemptToLogicalBody::SealResult {
                result: Ok(withdrawals),
                ..
            } => match publication.take_root_for_seal() {
                Ok(sender) => request
                    .original
                    .accept(sender, withdrawals.as_ref().to_vec())
                    .map_err(PublicationError::Root),
                Err(error) => {
                    request.original.reject(root_protocol_error(
                        "original root success publisher is unavailable",
                    ));
                    Err(error)
                }
            },
            AttemptToLogicalBody::TerminalControlResult { result: Ok(()), .. } => request
                .original
                .accept_terminal_control()
                .map_err(PublicationError::Root),
            AttemptToLogicalBody::SealResult {
                result: Err(failure),
                ..
            }
            | AttemptToLogicalBody::TerminalControlResult {
                result: Err(failure),
                ..
            } => {
                let mut resolve_host = |reference| {
                    request
                        .failure_projection
                        .as_ref()
                        .filter(|(original, _)| *original == reference)
                        .map(|(_, input)| input.clone())
                };
                let error = project_failure(failure, &mut resolve_host)
                    .map(|failure| QueryExecutionError::from_execution(failure.input().clone()))
                    .unwrap_or_else(|_| {
                        root_protocol_error(
                            "original root-control failure projection is unavailable",
                        )
                    });
                request.original.reject(error);
                Ok(())
            }
            _ => unreachable!("the root-control gate preserves the original reply kind"),
        };
        self.finish_delivery(token);
        result
    }

    pub(crate) fn deliver_owner_failed(&self, token: AttemptRequestToken) {
        if let Some((request, reply)) = self.take_committed(token) {
            assert!(matches!(
                &reply.body,
                AttemptToLogicalBody::SealResult {
                    result: Err(AttemptFailure::OwnerFailed),
                    ..
                } | AttemptToLogicalBody::TerminalControlResult {
                    result: Err(AttemptFailure::OwnerFailed),
                    ..
                }
            ));
            request
                .original
                .reject(QueryExecutionError::from_execution(owner_failure()));
            self.finish_delivery(token);
        }
    }

    fn take_committed(
        &self,
        token: AttemptRequestToken,
    ) -> Option<(Request, Arc<AttemptToLogical<AttemptFailure>>)> {
        let mut state = self.state.lock().expect("original root-control request");
        let request = state
            .original
            .as_ref()
            .filter(|request| request.token == token)?;
        let reply = request.reply.take_once()?;
        let request = state.original.take().unwrap();
        state.delivering = true;
        Some((request, reply))
    }

    fn finish_delivery(&self, token: AttemptRequestToken) {
        self.failure.release_reply(token);
        self.state
            .lock()
            .expect("original root-control request")
            .delivering = false;
    }
}

impl fmt::Debug for RootControlRequests {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootControlRequests")
            .field("activation", &self.binding.activation())
            .finish_non_exhaustive()
    }
}

impl AcceptedRootControlPort for RootControlRequests {
    fn enqueue_root_control(
        &self,
        request: AcceptedRootControlRequest,
    ) -> Result<(), AcceptedRootControlRequest> {
        let Some(ingress) = self
            .ingress
            .get()
            .and_then(WeakOrderedObservationInput::upgrade)
        else {
            return Err(request);
        };
        let reference = self.reserve_request(request)?;
        let token = reference.token();
        if ingress.queue_root_control(reference).is_err() {
            let mut state = self.state.lock().expect("original root-control request");
            if self.failure.cancel_unpublished_reply(token) {
                return Err(state
                    .original
                    .take()
                    .expect("refused original control")
                    .original);
            }
            // The failure gate already owns this exact request.
            return Ok(());
        }
        if let Err(error) = ingress.drive() {
            self.state
                .lock()
                .expect("original root-control request")
                .writer_error = Some(error);
        }
        Ok(())
    }
}

pub(crate) fn reply_kind(
    body: &AttemptToLogicalBody<AttemptFailure>,
) -> Option<(AttemptRequestToken, RootControlReplyKind)> {
    match body {
        AttemptToLogicalBody::SealResult { token, .. } => {
            Some((*token, RootControlReplyKind::Seal))
        }
        AttemptToLogicalBody::TerminalControlResult { token, .. } => {
            Some((*token, RootControlReplyKind::TerminalControl))
        }
        _ => None,
    }
}

fn root_protocol_error(detail: &str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    ))
}
