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

//! Two-core host endpoints. Production composition switches atomically in P12.
#![expect(
    dead_code,
    reason = "P12 composes the approved two-core endpoints into the original host"
)]
pub(crate) mod abort;
pub(crate) mod control;
pub(crate) mod facts;
pub(crate) mod requests;
pub(crate) mod root_control;

use crate::control_runtime::actor::{FrontendActor, FrontendEffect};
use novarocks_control_runtime::actor_turn::{ActorRegistration, ActorRuntime, EffectTicket};
use novarocks_coordination_contract::{
    AttemptActivationIdentity,
    logical_port::{LogicalToAttempt, LogicalToAttemptBody},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Original endpoint objects for one actual registered attempt. This record
/// contains no attempt ScopeHandle and never calls request_stop on a Scope.
pub(crate) struct AttemptPortEndpoints {
    logical: ActorRegistration,
    attempt: ActorRegistration,
    activation: AttemptActivationIdentity,
    pub(crate) requests: Arc<requests::AttemptRequests>,
    pub(crate) facts: Arc<facts::AttemptFactBank>,
    control: control::SourcePublishedSlot,
    abort: Arc<abort::AttemptAbortIngress>,
    pub(crate) roots: Arc<root_control::RootControlRequests>,
    producer_exited: AtomicBool,
    startup: super::startup::AttemptStartup,
}
#[derive(Debug)]
pub(crate) enum PortDeliveryError {
    ForeignPair,
    StartupPending,
    Request(requests::RequestError),
    Control(control::PortControlError),
    Abort(abort::AbortPortError),
}
impl std::fmt::Display for PortDeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "original two-core endpoint rejected delivery: {self:?}")
    }
}
impl std::error::Error for PortDeliveryError {}
/// A pair refusal returns every original endpoint, including paid roles and
/// any pending job custody. It never drops them as an error side effect.
#[must_use = "Retain all original endpoints when pair binding is refused"]
pub(crate) struct PortBindingRefusal {
    pub(crate) error: PortDeliveryError,
    pub(crate) original: Box<AttemptPortEndpoints>,
}
impl std::fmt::Debug for PortBindingRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortBindingRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl AttemptPortEndpoints {
    pub(crate) fn bind(
        logical: ActorRegistration,
        attempt: ActorRegistration,
        activation: AttemptActivationIdentity,
        startup: super::startup::AttemptStartup,
        requests: Arc<requests::AttemptRequests>,
        facts: Arc<facts::AttemptFactBank>,
        control: control::SourcePublishedSlot,
        abort: Arc<abort::AttemptAbortIngress>,
        roots: Arc<root_control::RootControlRequests>,
    ) -> Result<Arc<Self>, PortBindingRefusal> {
        let valid = startup.matches_registration(&attempt)
            && requests.matches_pair(&logical, &attempt, activation)
            && facts.matches_pair(&logical, &attempt, activation)
            && control.matches_pair(&logical, &attempt, activation)
            && abort.matches_pair(&logical, &attempt, activation)
            && roots.binding().registration().same_registration(&attempt)
            && roots.binding().activation() == activation;
        let original = Self {
            logical,
            attempt,
            activation,
            requests,
            facts,
            control,
            abort,
            roots,
            startup,
            producer_exited: AtomicBool::new(false),
        };
        if !valid {
            return Err(PortBindingRefusal {
                error: PortDeliveryError::ForeignPair,
                original: Box::new(original),
            });
        }
        Ok(Arc::new(original))
    }
    pub(crate) fn startup(&self) -> super::startup::AttemptStartup {
        self.startup.clone()
    }
    pub(crate) fn logical(&self) -> &ActorRegistration {
        &self.logical
    }
    pub(crate) fn attempt(&self) -> &ActorRegistration {
        &self.attempt
    }
    pub(crate) fn activation(&self) -> AttemptActivationIdentity {
        self.activation
    }
    /// Called only after the original AP Source's future/captures can no longer
    /// publish a port occurrence. Neither terminal status nor sender Drop is
    /// this physical producer proof. Scope removal remains a separate fact.
    pub(crate) fn original_producer_exited(&self) {
        self.facts.close_admission();
        self.producer_exited.store(true, Ordering::Release);
    }
    pub(crate) fn producer_exited(&self) -> bool {
        self.producer_exited.load(Ordering::Acquire)
    }
    pub(crate) fn publish_owner_failed_stop(
        &self,
        session: &mut novarocks_control_runtime::publication::FailurePublicationSession<
            FrontendActor,
        >,
        failure: &novarocks_coordination_contract::ExecutionFailure,
    ) -> Result<control::OwnerFailedStopReceipt, PortDeliveryError> {
        if !self.startup.ready_for(&self.attempt) {
            return Err(PortDeliveryError::StartupPending);
        }
        self.control
            .publish_owner_failed_stop(session, failure)
            .map_err(PortDeliveryError::Control)
    }
    pub(crate) fn deliver(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        original: LogicalToAttempt,
    ) -> Result<(), PortDeliveryError> {
        if original.activation != self.activation {
            return Err(PortDeliveryError::ForeignPair);
        }
        if matches!(
            original.body,
            LogicalToAttemptBody::Stop { .. } | LogicalToAttemptBody::Abort { .. }
        ) && !self.startup.ready_for(&self.attempt)
        {
            return Err(PortDeliveryError::StartupPending);
        }
        match &original.body {
            LogicalToAttemptBody::AuthorizationReply { .. }
            | LogicalToAttemptBody::EstablishSettlementReply { .. }
            | LogicalToAttemptBody::PreReadyReplacementReply { .. } => self
                .requests
                .deliver_reply_now(runtime, ticket, original)
                .map(|_| ())
                .map_err(|refusal| PortDeliveryError::Request(refusal.error)),
            LogicalToAttemptBody::Abort { .. } => self
                .abort
                .deliver(runtime, ticket)
                .map(|_| ())
                .map_err(PortDeliveryError::Abort),
            LogicalToAttemptBody::Stop { .. } => self
                .control
                .deliver_stop(runtime, ticket, original)
                .map(|_| ())
                .map_err(PortDeliveryError::Control),
            _ => self
                .control
                .commit(runtime, ticket, &original)
                .map(|_| ())
                .map_err(PortDeliveryError::Control),
        }
    }
}
