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
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket},
    transaction::ScopeHandle,
};
use novarocks_coordination_contract::{AttemptActionId, AttemptActivationIdentity};
use std::{
    num::NonZeroU64,
    sync::{Arc, OnceLock},
};

/// The original registration binds its final endpoints once, before Core Run.
pub(crate) struct FailurePublications {
    registration: ActorRegistration,
    activation: AttemptActivationIdentity,
    ports: OnceLock<Arc<super::port::AttemptPortEndpoints>>,
}
impl FailurePublications {
    pub(crate) fn bind_ports(&self, original: Arc<super::port::AttemptPortEndpoints>) {
        assert!(original.attempt().same_registration(&self.registration));
        assert_eq!(original.activation(), self.activation);
        assert!(
            self.ports.set(original).is_ok(),
            "one original failed-owner port binding"
        );
    }
    #[cfg(test)]
    pub(crate) fn matches(
        &self,
        registration: &ActorRegistration,
        activation: AttemptActivationIdentity,
    ) -> bool {
        self.registration.same_registration(registration) && self.activation == activation
    }
    pub(crate) fn new(
        registration: ActorRegistration,
        activation: AttemptActivationIdentity,
        scope: &ScopeHandle,
    ) -> Self {
        assert!(registration.matches_scope(scope));
        Self {
            registration,
            activation,
            ports: OnceLock::new(),
        }
    }
}

/// Original immutable Source custody transferred with the supervisor ticket.
#[must_use]
pub(crate) struct FailureContext {
    pub(crate) lifecycle: FailureLifecycle,
    pub(crate) publications: Arc<FailurePublications>,
}

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "the prepaid refusal retains its original rejected fact inline without another allocation or disposal leg"
)]
pub(crate) enum FailurePublicationError {
    Runtime(ActorTurnError),
    Port(super::port::facts::FactPublishRefusal),
}
impl std::fmt::Display for FailurePublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(error) => write!(
                f,
                "original failed-attempt fact publication was refused: {error}"
            ),
            Self::Port(original) => write!(
                f,
                "original failed-attempt fact publication was refused: {:?}",
                original.error
            ),
        }
    }
}
impl std::error::Error for FailurePublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Port(_) => None,
        }
    }
}

impl FailureContext {
    /// The same running failure Work keeps its unique session across actual
    /// credit waits. Root ACKs remain in their original Source jobs; this
    /// failure path never manufactures another Seal or terminal decision.
    pub(crate) async fn publish_original(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        unresolved_actions: Box<[AttemptActionId]>,
    ) -> (
        Result<(), Arc<FailurePublicationError>>,
        Option<Result<(), Arc<super::publication::PublicationError>>>,
    ) {
        let Some(ports) = self.publications.ports.get() else {
            return (
                Err(Arc::new(FailurePublicationError::Runtime(
                    ActorTurnError::PublicationTargetMismatch,
                ))),
                None,
            );
        };
        let mut session = match runtime.claim_failure_publication(ticket) {
            Ok(session) => session,
            Err(error) => return (Err(Arc::new(FailurePublicationError::Runtime(error))), None),
        };
        let message = novarocks_coordination_contract::logical_port::AttemptToLogical {
            activation: ports.activation(),
            action: AttemptActionId::new(NonZeroU64::new(ticket.descriptor.id.get()).unwrap()),
            body:
                novarocks_coordination_contract::logical_port::AttemptToLogicalBody::AttemptLost {
                    failure: novarocks_coordination_contract::ExecutionFailure::from_input(
                        novarocks_attempt_protocol::port_projection::owner_failure(),
                    ),
                    unknown_participations: self.lifecycle.started_participations(),
                    unresolved_actions,
                },
        };
        let result = ports
            .facts
            .publish_owner_lost(runtime, &mut session, message)
            .await
            .map_err(|original| Arc::new(FailurePublicationError::Port(original)));
        (result, None)
    }
}
