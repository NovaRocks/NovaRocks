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

//! Publication from the original supervisor Work after attempt Core failure.

use super::failure_lifecycle::FailureLifecycle;
use crate::control_runtime::actor::{FrontendActor, FrontendEffect};
use novarocks_attempt_protocol::failure::AttemptFailure;
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket},
    obligation::WorkObligation,
    publication::{FailurePublicationSession, PublishedReply},
    transaction::ScopeHandle,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptRequestToken, AttemptToLogical,
    AttemptToLogicalBody,
};
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

pub(crate) type AttemptReply = PublishedReply<AttemptToLogical<AttemptFailure>>;

#[derive(Clone, Copy)]
pub(crate) enum RootControlReplyKind {
    Seal,
    TerminalControl,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReplyAdmissionError {
    Full,
    OwnerFailed,
    Runtime(ActorTurnError),
}

struct RootReply {
    token: AttemptRequestToken,
    kind: RootControlReplyKind,
    target: AttemptReply,
}

enum RootReplies {
    Open(Option<RootReply>),
    Failed,
}

/// One bridge control position and one independent supervisor outbox. Normal
/// publication uses the same reply cell that failure will settle if still empty.
pub(crate) struct FailurePublications {
    registration: ActorRegistration,
    activation: AttemptActivationIdentity,
    replies: Mutex<RootReplies>,
    lost: AttemptReply,
    lost_ready: Notify,
}

impl FailurePublications {
    pub(crate) fn new(
        registration: ActorRegistration,
        activation: AttemptActivationIdentity,
        scope: &ScopeHandle,
    ) -> Self {
        assert!(registration.matches_scope(scope));
        Self {
            registration,
            activation,
            replies: Mutex::new(RootReplies::Open(None)),
            lost: PublishedReply::new(scope),
            lost_ready: Notify::new(),
        }
    }

    pub(crate) fn reserve_reply(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        author: &WorkObligation,
        token: AttemptRequestToken,
        kind: RootControlReplyKind,
    ) -> Result<AttemptReply, ReplyAdmissionError> {
        // The bridge Work must remain live across the attempt's failure fence.
        if self.registration.matches_scope(author.scope()) {
            return Err(ReplyAdmissionError::Runtime(
                ActorTurnError::PublicationTargetMismatch,
            ));
        }
        let target =
            match PublishedReply::new_for_admitted_work(runtime, &self.registration, author) {
                Ok(target) => target,
                Err(ActorTurnError::ActorFailed) => return Err(ReplyAdmissionError::OwnerFailed),
                Err(error) => return Err(ReplyAdmissionError::Runtime(error)),
            };
        let mut replies = self.replies.lock().expect("original bridge reply slot");
        let slot = match &mut *replies {
            RootReplies::Open(slot) if slot.is_none() => slot,
            RootReplies::Open(_) => return Err(ReplyAdmissionError::Full),
            RootReplies::Failed => return Err(ReplyAdmissionError::OwnerFailed),
        };
        *slot = Some(RootReply {
            token,
            kind,
            target: target.clone(),
        });
        Ok(target)
    }

    pub(crate) fn release_reply(&self, token: AttemptRequestToken) -> bool {
        let mut replies = self.replies.lock().expect("original bridge reply slot");
        if let RootReplies::Open(slot) = &mut *replies
            && slot
                .as_ref()
                .is_some_and(|reply| reply.token == token && reply.target.is_committed())
        {
            *slot = None;
            return true;
        }
        false
    }

    pub(crate) async fn take_lost(&self) -> Arc<AttemptToLogical<AttemptFailure>> {
        self.lost_ready.notified().await;
        self.lost
            .take_once()
            .expect("original bridge consumes its unique AttemptLost")
    }

    fn fail_reply(
        &self,
        session: &mut FailurePublicationSession<FrontendActor>,
        action: AttemptActionId,
    ) -> Result<(), ActorTurnError> {
        let replies = {
            let mut replies = self.replies.lock().expect("original bridge reply slot");
            std::mem::replace(&mut *replies, RootReplies::Failed)
        };
        let RootReplies::Open(reply) = replies else {
            return Err(ActorTurnError::FailurePublicationAlreadyClaimed);
        };
        if let Some(RootReply {
            token,
            kind,
            target,
        }) = reply
        {
            let body = match kind {
                RootControlReplyKind::Seal => AttemptToLogicalBody::SealResult {
                    token,
                    result: Err(AttemptFailure::OwnerFailed),
                },
                RootControlReplyKind::TerminalControl => {
                    AttemptToLogicalBody::TerminalControlResult {
                        token,
                        result: Err(AttemptFailure::OwnerFailed),
                    }
                }
            };
            match session.commit_reply(
                &target,
                Arc::new(AttemptToLogical {
                    activation: self.activation,
                    action,
                    body,
                }),
            ) {
                Ok(()) | Err((ActorTurnError::ReplyAlreadyCommitted, _)) => {}
                Err((error, _)) => return Err(error),
            }
        }
        Ok(())
    }
}

/// Original immutable Source custody transferred with the supervisor ticket.
#[must_use]
pub(crate) struct FailureContext {
    pub(crate) lifecycle: FailureLifecycle,
    pub(crate) publications: Arc<FailurePublications>,
}

impl FailureContext {
    /// Called by the original running Cleanup Work. The lifecycle snapshot is
    /// the evidence so far; unresolved actions may still start and contribute
    /// to the final remote residual after their start authors leave.
    pub(crate) fn publish(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        unresolved_actions: Box<[AttemptActionId]>,
    ) -> Result<(), ActorTurnError> {
        let mut session = runtime.claim_failure_publication(ticket)?;
        if !self
            .publications
            .registration
            .same_registration(session.registration())
        {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        // This action belongs to the separate supervisor outbox. It is not a
        // normal Core action or part of the Core publication sequence.
        let action = AttemptActionId::new(NonZeroU64::new(ticket.descriptor.id.get()).unwrap());
        let reply = self.publications.fail_reply(&mut session, action);
        let lost = AttemptToLogical {
            activation: self.publications.activation,
            action,
            body: AttemptToLogicalBody::AttemptLost {
                failure: AttemptFailure::OwnerFailed,
                unknown_participations: self.lifecycle.started_participations(),
                unresolved_actions,
            },
        };
        session
            .commit_reply(&self.publications.lost, Arc::new(lost))
            .map_err(|(error, _)| error)?;
        self.publications.lost_ready.notify_one();
        reply
    }
}
