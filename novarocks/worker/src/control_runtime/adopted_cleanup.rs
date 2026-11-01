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

//! The original cleanup ticket moves into its actual resource custodian.
//! This value creates no Work, callback, resource grant or replica obligation.

use super::{
    actors::{WorkerAction, WorkerActor},
    failure::ScopePublicationsDrained,
    request_endpoint::RequestNotice,
};
use novarocks_control_runtime::{
    driver::{ActorRuntime, DriverError, EffectTicket},
    transaction::ScopeHandle,
};

#[must_use = "The original resource owner must retain its cleanup ticket until actual completion"]
pub(crate) struct AdoptedCleanupEffect {
    runtime: ActorRuntime<WorkerActor>,
    ticket: Option<EffectTicket<WorkerAction>>,
    notice: Option<RequestNotice>,
}
impl AdoptedCleanupEffect {
    pub(in crate::control_runtime) fn from_normal(
        runtime: &ActorRuntime<WorkerActor>,
        ticket: EffectTicket<WorkerAction>,
        notice: Option<RequestNotice>,
    ) -> Result<Self, (DriverError, EffectTicket<WorkerAction>)> {
        if let Err(error) = runtime.validate_effect_ticket(&ticket) {
            return Err((error, ticket));
        }
        Ok(Self {
            runtime: runtime.clone(),
            ticket: Some(ticket),
            notice,
        })
    }
    pub(in crate::control_runtime) fn from_original(
        runtime: &ActorRuntime<WorkerActor>,
        stage: &ScopePublicationsDrained<'_>,
        ticket: EffectTicket<WorkerAction>,
        notice: Option<RequestNotice>,
    ) -> Result<Self, (DriverError, EffectTicket<WorkerAction>)> {
        if let Err(error) = stage.validate(runtime, ticket.actor) {
            return Err((error, ticket));
        }
        if !ticket.descriptor.kind.allowed_after_failure() {
            return Err((DriverError::PublicationTargetMismatch, ticket));
        }
        Ok(Self {
            runtime: runtime.clone(),
            ticket: Some(ticket),
            notice,
        })
    }

    pub(crate) fn effect(&self) -> &WorkerAction {
        &self
            .ticket
            .as_ref()
            .expect("original uncompleted cleanup")
            .effect
    }

    pub(crate) fn ticket(&self) -> &EffectTicket<WorkerAction> {
        self.ticket.as_ref().expect("original uncompleted cleanup")
    }

    pub(crate) fn validate_scope(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        scope: &ScopeHandle,
    ) -> Result<(), DriverError> {
        stage.validate_scope(
            &self.runtime,
            self.ticket.as_ref().ok_or(DriverError::UnknownInput)?.actor,
            scope,
        )
    }

    pub(crate) fn is_completed(&self) -> bool {
        self.ticket.is_none()
    }

    /// Only the concrete original owner calls this after its own positive
    /// completion condition. An epoch change never invalidates a real exit.
    pub(crate) fn complete(&mut self) -> Result<bool, DriverError> {
        let Some(ticket) = self.ticket.take() else {
            return Ok(false);
        };
        if let Err(refusal) = self.runtime.effect_completed(ticket) {
            self.ticket = Some(refusal.ticket);
            return Err(refusal.error);
        }
        if let Some(notice) = &self.notice {
            notice.notify();
        }
        Ok(true)
    }

    pub(crate) fn into_ticket(self) -> EffectTicket<WorkerAction> {
        self.ticket
            .expect("an unadopted cleanup retains its original ticket")
    }
}
