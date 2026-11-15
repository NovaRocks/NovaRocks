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

//! Original AP jobs waiting for an exact LP reply. This table never starts,
//! cancels or drops their rich captures. The original exit credential remains
//! in the same job until controlled retirement takes it back after reply fill.

use crate::control_runtime::{
    actor::{FrontendActor, FrontendEffect, FrontendInput},
    host::PreparedEffectJob,
};
use novarocks_attempt_protocol::{
    effect::EffectBody, input::AttemptInput, port_projection::project_logical_message,
};
use novarocks_control_runtime::actor_turn::{
    ActorFailureProof, ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket,
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptAuthorizationRef, AttemptRequestToken,
    AttemptToLogicalBody as ApBody,
    logical_port::{
        AttemptToLogical, AttemptToLogicalBody, LogicalToAttempt, LogicalToAttemptBody,
    },
};
use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
    task::Waker,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestKind {
    Authorization,
    PreReadyReplacement,
    EstablishSettlement(AttemptAuthorizationRef),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Descriptor {
    activation: AttemptActivationIdentity,
    action: AttemptActionId,
    token: AttemptRequestToken,
    kind: RequestKind,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestError {
    Closed,
    Full,
    ForeignActivation,
    WrongRequest,
    WrongReply,
    WrongFailureProof,
    DuplicateToken,
    UnknownToken,
    ReplyBorrowed,
    DuplicateReply,
    OriginalResult(ActorTurnError),
    Runtime(ActorTurnError),
}

#[must_use = "A refused insertion still owns the original job and exit credential"]
pub(crate) struct RequestInsertRefusal {
    pub(crate) error: RequestError,
    pub(crate) job: PreparedEffectJob,
}
impl fmt::Debug for RequestInsertRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestInsertRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
#[must_use = "Retain the rejected original reply and any projected passive input"]
pub(crate) struct RequestReplyRefusal {
    pub(crate) error: RequestError,
    pub(crate) reply: LogicalToAttempt,
    pub(crate) projected: Option<FrontendInput>,
}

// The underlying job's original ExitObligation checks actual exit. This wrapper
// has no Drop settlement: closing the table does not consume that credential.
struct RequestRowObligation {
    descriptor: Descriptor,
    job: Option<PreparedEffectJob>,
    reply_input: Option<InputId>,
    loaned: bool,
}
struct State {
    closed: bool,
    rows: BTreeMap<AttemptRequestToken, RequestRowObligation>,
    host_waiting: bool,
}
pub(crate) struct AttemptRequests {
    activation: AttemptActivationIdentity,
    attempt: ActorRegistration,
    logical: ActorRegistration,
    capacity: NonZeroUsize,
    state: Mutex<State>,
    retained: Mutex<Option<Arc<Self>>>,
    wake: Waker,
    changed: tokio::sync::Notify,
}
#[must_use = "The replied original job still requires controlled capture disposal and actual exit"]
pub(crate) struct RepliedRequestObligation {
    pub(crate) token: AttemptRequestToken,
    pub(crate) reply_input: InputId,
    pub(crate) job: PreparedEffectJob,
}
/// The same original request after its AP's actual kernel failure fence.
/// A missing recorded reply is not proof that its original result is empty.
/// The AP Source still owns actual capture/Join/report/input/header retirement.
#[must_use = "Retire the same original failed-AP request job through its real exit owner"]
pub(crate) struct FailedAttemptRequestObligation {
    pub(crate) token: AttemptRequestToken,
    pub(crate) action: AttemptActionId,
    pub(crate) reply_input: Option<InputId>,
    pub(crate) job: PreparedEffectJob,
}

impl AttemptRequests {
    pub(crate) fn new(
        activation: AttemptActivationIdentity,
        attempt: ActorRegistration,
        logical: ActorRegistration,
        capacity: NonZeroUsize,
        wake: Waker,
    ) -> Arc<Self> {
        Arc::new(Self {
            activation,
            attempt,
            logical,
            capacity,
            state: Mutex::new(State {
                closed: false,
                rows: BTreeMap::new(),
                host_waiting: false,
            }),
            retained: Mutex::new(None),
            wake,
            changed: tokio::sync::Notify::new(),
        })
    }
    pub(crate) fn matches_pair(
        &self,
        logical: &ActorRegistration,
        attempt: &ActorRegistration,
        activation: AttemptActivationIdentity,
    ) -> bool {
        self.activation == activation
            && self.logical.same_registration(logical)
            && self.attempt.same_registration(attempt)
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
    pub(crate) fn has_capacity(&self) -> bool {
        let state = self.lock();
        !state.closed && state.rows.len() < self.capacity.get()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.lock().rows.is_empty()
    }
    pub(crate) async fn changed(&self) {
        self.changed.notified().await;
    }
    pub(crate) fn close_admission(&self) {
        let changed = {
            let mut state = self.lock();
            !std::mem::replace(&mut state.closed, true)
        };
        if changed {
            self.changed.notify_one();
            self.wake.wake_by_ref();
        }
    }
    /// The exact request job is retained before its LP request is published.
    /// Caller rollback, if needed, must recover the same unpublished job.
    pub(crate) fn insert(
        self: &Arc<Self>,
        request: &AttemptToLogical,
        job: PreparedEffectJob,
    ) -> Result<(), RequestInsertRefusal> {
        let checked = (|| {
            let expected = descriptor(request)?;
            if expected.activation != self.activation {
                return Err(RequestError::ForeignActivation);
            }
            if original_descriptor(&job)? != expected {
                return Err(RequestError::WrongRequest);
            }
            let author = job
                .result
                .event_author()
                .map_err(RequestError::OriginalResult)?;
            if !self.attempt.matches_input_author(&author) {
                return Err(RequestError::WrongRequest);
            }
            Ok(expected)
        })();
        let expected = match checked {
            Ok(expected) => expected,
            Err(error) => return Err(RequestInsertRefusal { error, job }),
        };
        let mut state = self.lock();
        let error = if state.closed {
            Some(RequestError::Closed)
        } else if state.rows.contains_key(&expected.token) {
            Some(RequestError::DuplicateToken)
        } else if state.rows.len() == self.capacity.get() {
            Some(RequestError::Full)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(RequestInsertRefusal { error, job });
        }
        {
            let mut retained = self.retained.lock().unwrap_or_else(|p| p.into_inner());
            if retained.is_none() {
                *retained = Some(Arc::clone(self));
            }
        }
        state.rows.insert(
            expected.token,
            RequestRowObligation {
                descriptor: expected,
                job: Some(job),
                reply_input: None,
                loaned: false,
            },
        );
        drop(state);
        self.changed.notify_one();
        Ok(())
    }
    /// Only before handoff, while the caller still owns its unaccepted source
    /// ticket, may it undo insertion. A replied or loaned row never rolls back.
    pub(crate) fn take_unpublished(&self, token: AttemptRequestToken) -> Option<PreparedEffectJob> {
        let job = {
            let mut state = self.lock();
            let row = state.rows.get(&token)?;
            if row.loaned || row.reply_input.is_some() {
                return None;
            }
            state
                .rows
                .remove(&token)
                .expect("same original unpublished request row")
                .job
                .take()
        };
        self.release_if_empty();
        job
    }
    /// Recover at most one issued original, including an already-replied row,
    /// after the SAME AP has actually failed. This is not insertion rollback,
    /// a reply, capture disposal or exit evidence. Loaned rows keep custody and
    /// arrange a wake when their exact original job returns.
    pub(crate) fn take_one_after_attempt_failure(
        &self,
        proof: &ActorFailureProof,
    ) -> Result<Option<FailedAttemptRequestObligation>, RequestError> {
        if !proof.registration().same_registration(&self.attempt)
            || proof.initial_epoch() != self.attempt.initial_epoch()
            || proof.failed_epoch() <= proof.initial_epoch()
        {
            return Err(RequestError::WrongFailureProof);
        }
        let (closed, recovered) = {
            let mut state = self.lock();
            let closed = !std::mem::replace(&mut state.closed, true);
            let mut borrowed = false;
            let token = state.rows.iter().find_map(|(token, row)| {
                if row.loaned {
                    borrowed = true;
                    None
                } else {
                    Some(*token)
                }
            });
            state.host_waiting |= borrowed;
            let recovered = token.map(|token| {
                let mut original = state
                    .rows
                    .remove(&token)
                    .expect("same failed AP request row");
                FailedAttemptRequestObligation {
                    token,
                    action: original.descriptor.action,
                    reply_input: original.reply_input,
                    job: original
                        .job
                        .take()
                        .expect("same unborrowed original AP job"),
                }
            });
            (closed, recovered)
        };
        if recovered.is_some() {
            self.release_if_empty();
        } else if closed {
            self.changed.notify_one();
            wake_returned(|| self.wake.wake_by_ref());
        }
        Ok(recovered)
    }
    fn release_if_empty(&self) {
        let (retained, wake) = {
            let mut state = self.lock();
            let retained = if state.rows.is_empty() {
                self.retained
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take()
            } else {
                None
            };
            (retained, std::mem::take(&mut state.host_waiting))
        };
        // The original job has already moved to its controlled caller. The
        // last neutral table alias may drop only outside both custody locks.
        drop(retained);
        self.changed.notify_one();
        if wake {
            wake_returned(|| self.wake.wake_by_ref());
        }
    }
    pub(crate) fn deliver_reply_now(
        self: &Arc<Self>,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        reply: LogicalToAttempt,
    ) -> Result<InputId, RequestReplyRefusal> {
        let checked = (|| {
            if reply.activation != self.activation {
                return Err(RequestError::ForeignActivation);
            }
            let origin = runtime
                .registration_from_effect(ticket)
                .map_err(RequestError::Runtime)?;
            if !origin.same_registration(&self.logical) || !same_ticket_reply(ticket, &reply) {
                return Err(RequestError::WrongReply);
            }
            reply_descriptor(&reply)
        })();
        let (token, kind) = match checked {
            Ok(value) => value,
            Err(error) => {
                return Err(RequestReplyRefusal {
                    error,
                    reply,
                    projected: None,
                });
            }
        };
        let mut loan = match self.loan(token, kind) {
            Ok(loan) => loan,
            Err(error) => {
                return Err(RequestReplyRefusal {
                    error,
                    reply,
                    projected: None,
                });
            }
        };
        let projected = FrontendInput::Attempt(Box::new(AttemptInput::Port(
            project_logical_message(&reply),
        )));
        // The prior origin read grants no permission. This kernel operation
        // rechecks the CURRENT LP ticket and fills the original AP event in
        // one publication transaction, preserving the original result author.
        match loan
            .job
            .as_mut()
            .expect("original request loan")
            .result
            .fill_event_from_effect(runtime, ticket, None, projected)
        {
            Ok(input) => {
                loan.reply_input = Some(input);
                Ok(input)
            }
            Err((error, projected)) => Err(RequestReplyRefusal {
                error: RequestError::Runtime(error),
                reply,
                projected: Some(projected),
            }),
        }
    }
    /// The failed LP's original session replies to one exact AP job. Already
    /// committed positive replies remain intact, and late original producers
    /// may still insert requests until their actual exit closes admission.
    pub(crate) fn publish_one_owner_failed(
        self: &Arc<Self>,
        session: &mut novarocks_control_runtime::publication::FailurePublicationSession<
            FrontendActor,
        >,
        failure: &novarocks_coordination_contract::ExecutionFailure,
    ) -> Result<Option<InputId>, RequestError> {
        if !self.logical.same_registration(session.registration()) {
            return Err(RequestError::WrongReply);
        }
        let descriptor = {
            let mut state = self.lock();
            let mut borrowed = false;
            let descriptor = state.rows.values().find_map(|row| {
                if row.reply_input.is_some() {
                    return None;
                }
                if row.loaned {
                    borrowed = true;
                    return None;
                }
                Some(row.descriptor)
            });
            if borrowed {
                state.host_waiting = true;
            }
            descriptor
        };
        let Some(descriptor) = descriptor else {
            return Ok(None);
        };
        let mut loan = self.loan(descriptor.token, descriptor.kind)?;
        let body = match descriptor.kind {
            RequestKind::Authorization => LogicalToAttemptBody::AuthorizationReply {
                token: descriptor.token,
                decision: Err(novarocks_coordination_contract::logical_port::AttemptAuthorizationFailure::Rejected(failure.clone())),
            },
            RequestKind::PreReadyReplacement => LogicalToAttemptBody::PreReadyReplacementOwnerFailed {
                token: descriptor.token, failure: failure.clone(),
            },
            RequestKind::EstablishSettlement(authorization) => LogicalToAttemptBody::EstablishSettlementReply {
                token: descriptor.token, authorization, result: Err(failure.clone()),
            },
        };
        let projected = FrontendInput::Attempt(Box::new(AttemptInput::Port(
            project_logical_message(&LogicalToAttempt {
                activation: self.activation,
                body,
            }),
        )));
        let input = loan
            .job
            .as_mut()
            .expect("same original failed-owner reply job")
            .result
            .fill_event_from_failure(session, None, projected)
            .map_err(|(error, _)| RequestError::Runtime(error))?;
        loan.reply_input = Some(input);
        Ok(Some(input))
    }

    fn loan(
        self: &Arc<Self>,
        token: AttemptRequestToken,
        kind: RequestKind,
    ) -> Result<RequestLoanObligation, RequestError> {
        let mut state = self.lock();
        let row = state
            .rows
            .get_mut(&token)
            .ok_or(RequestError::UnknownToken)?;
        if row.descriptor.kind != kind {
            return Err(RequestError::WrongReply);
        }
        if row.reply_input.is_some() {
            return Err(RequestError::DuplicateReply);
        }
        if row.loaned {
            state.host_waiting = true;
            return Err(RequestError::ReplyBorrowed);
        }
        row.loaned = true;
        Ok(RequestLoanObligation {
            table: Arc::clone(self),
            token,
            job: row.job.take(),
            reply_input: None,
        })
    }
    pub(crate) fn take_replied(
        &self,
        token: AttemptRequestToken,
    ) -> Option<RepliedRequestObligation> {
        let replied = {
            let mut state = self.lock();
            let row = state.rows.get(&token)?;
            if row.loaned {
                state.host_waiting = true;
                return None;
            }
            let reply_input = row.reply_input?;
            let mut row = state
                .rows
                .remove(&token)
                .expect("same original replied job");
            RepliedRequestObligation {
                token,
                reply_input,
                job: row.job.take().expect("same retained original request job"),
            }
        };
        self.release_if_empty();
        Some(replied)
    }
}
#[must_use = "Return the same request job on success, refusal and unwind"]
struct RequestLoanObligation {
    table: Arc<AttemptRequests>,
    token: AttemptRequestToken,
    job: Option<PreparedEffectJob>,
    reply_input: Option<InputId>,
}
impl Drop for RequestLoanObligation {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.table.lock();
            let row = state
                .rows
                .get_mut(&self.token)
                .expect("request loan retains its row");
            assert!(row.loaned && row.job.is_none());
            row.job = self.job.take();
            row.loaned = false;
            if let Some(input) = self.reply_input {
                row.reply_input = Some(input);
            }
            let requested = std::mem::take(&mut state.host_waiting);
            self.reply_input.is_some() || requested
        };
        wake_returned(|| {
            self.table.changed.notify_one();
            if wake {
                self.table.wake.wake_by_ref();
            }
        });
    }
}
fn wake_returned(wake: impl FnOnce()) {
    if std::thread::panicking() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(wake));
    } else {
        wake();
    }
}
fn descriptor(request: &AttemptToLogical) -> Result<Descriptor, RequestError> {
    let (token, kind) = match request.body {
        AttemptToLogicalBody::AuthorizationRequest { token, .. } => {
            (token, RequestKind::Authorization)
        }
        AttemptToLogicalBody::PreReadyReplacementQuery { token, .. } => {
            (token, RequestKind::PreReadyReplacement)
        }
        AttemptToLogicalBody::EstablishSettlement {
            token,
            authorization,
            ..
        } => (token, RequestKind::EstablishSettlement(authorization)),
        _ => return Err(RequestError::WrongRequest),
    };
    Ok(Descriptor {
        activation: request.activation,
        action: request.action,
        token,
        kind,
    })
}
fn original_descriptor(job: &PreparedEffectJob) -> Result<Descriptor, RequestError> {
    let FrontendEffect::Attempt(effect) = &job.effect else {
        return Err(RequestError::WrongRequest);
    };
    let request = match &effect.protocol.body {
        EffectBody::Port(request) | EffectBody::EstablishAuthorization { request, .. } => request,
        _ => return Err(RequestError::WrongRequest),
    };
    if request.action != effect.protocol.id {
        return Err(RequestError::WrongRequest);
    }
    let (token, kind) = match request.body {
        ApBody::AuthorizationRequest { token, .. } => (token, RequestKind::Authorization),
        ApBody::PreReadyReplacementQuery { token, .. } => (token, RequestKind::PreReadyReplacement),
        ApBody::EstablishSettlement {
            token,
            authorization,
            ..
        } => (token, RequestKind::EstablishSettlement(authorization)),
        // DetachEstablish has no logical equivalent. It is explicitly rejected
        // until P12 removes the legacy guard workflow, never silently dropped.
        _ => return Err(RequestError::WrongRequest),
    };
    Ok(Descriptor {
        activation: request.activation,
        action: request.action,
        token,
        kind,
    })
}
fn reply_descriptor(
    reply: &LogicalToAttempt,
) -> Result<(AttemptRequestToken, RequestKind), RequestError> {
    match reply.body {
        LogicalToAttemptBody::AuthorizationReply { token, .. } => {
            Ok((token, RequestKind::Authorization))
        }
        LogicalToAttemptBody::PreReadyReplacementReply { token, .. } => {
            Ok((token, RequestKind::PreReadyReplacement))
        }
        LogicalToAttemptBody::EstablishSettlementReply {
            token,
            authorization,
            ..
        } => Ok((token, RequestKind::EstablishSettlement(authorization))),
        _ => Err(RequestError::WrongReply),
    }
}

fn same_ticket_reply(ticket: &EffectTicket<FrontendEffect>, reply: &LogicalToAttempt) -> bool {
    let FrontendEffect::Logical(effect) = &ticket.effect else {
        return false;
    };
    let crate::control_runtime::logical::adapter::LogicalHostEffect::Protocol(effect) =
        &effect.body
    else {
        return false;
    };
    let novarocks_logical_execution_protocol::effect::LogicalEffectBody::AttemptPort(original) =
        &effect.body
    else {
        return false;
    };
    if original.activation != reply.activation {
        return false;
    }
    match (&original.body, &reply.body) {
        (
            LogicalToAttemptBody::AuthorizationReply {
                token: a,
                decision: x,
            },
            LogicalToAttemptBody::AuthorizationReply {
                token: b,
                decision: y,
            },
        ) => a == b && x == y,
        (
            LogicalToAttemptBody::PreReadyReplacementReply {
                token: a,
                may_observe: x,
            },
            LogicalToAttemptBody::PreReadyReplacementReply {
                token: b,
                may_observe: y,
            },
        ) => a == b && x == y,
        (
            LogicalToAttemptBody::EstablishSettlementReply {
                token: a,
                authorization: x,
                result: r,
            },
            LogicalToAttemptBody::EstablishSettlementReply {
                token: b,
                authorization: y,
                result: s,
            },
        ) => a == b && x == y && r == s,
        _ => false,
    }
}

#[cfg(test)]
pub(super) mod tests;
