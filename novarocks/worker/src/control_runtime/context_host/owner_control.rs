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

//! One prepaid, neutral parent-owner fence input. It has no callback or
//! foreign capture. The Context's permanent metadata author remains live
//! until this role and its actual batch have closed.

use super::{ContextHostError, ContextHostRecord, TIMER};
use crate::control_runtime::actors::{ContextMessage, WorkerAction, WorkerActor, WorkerMessage};
use crate::control_runtime_policy::StorageNeed;
use novarocks_control_runtime::{
    driver::{ActorRegistration, ActorRuntime, DriverError, EffectTicket, InputPermit, InputRole},
    lease::WorkLease,
};
use novarocks_control_runtime_core::{
    identity::{ActorId, InputId},
    mailbox::InputClass,
};
use novarocks_execution_contract::QueryContextRef;
use novarocks_worker_protocol::{context::ContextInput, control::ControlEffect};

pub(super) const OWNER_CONTROL_STORAGE: StorageNeed = StorageNeed {
    inputs: 1,
    batches: 1,
    effects: crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
    requests: 0,
    cuts: 0,
    replies: 0,
    completions: 0,
    captures: 0,
    work: 0,
};

#[derive(Default)]
pub(crate) struct OwnerControl {
    binding: Option<OwnerBinding>,
    permit: Option<InputPermit<WorkerActor>>,
    role: Option<InputRole<WorkerActor>>,
    input: Option<InputId>,
    delivered: bool,
    removal_authorized: bool,
    failed: bool,
    closed: bool,
    unresolved: Option<DriverError>,
}
struct OwnerBinding {
    runtime: ActorRuntime<WorkerActor>,
    registration: ActorRegistration,
    root: ActorId,
    context: QueryContextRef,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnerFenceDelivery {
    Delivered,
    AlreadyDelivered,
    FailedOwner,
}
impl OwnerControl {
    pub(super) fn detachment_source(
        &mut self,
    ) -> Result<Option<&mut InputRole<WorkerActor>>, DriverError> {
        if let Some(error) = self.unresolved {
            return Err(error);
        }
        if self.permit.is_some()
            || self.input.is_some()
            || self
                .role
                .as_ref()
                .map(InputRole::in_flight)
                .transpose()?
                .flatten()
                .is_some()
        {
            return Err(DriverError::ActorNotRetired);
        }
        Ok(self.role.as_mut())
    }
    pub(super) fn execution_detached(&mut self) {
        self.role = None;
        self.closed = true;
    }
    pub(super) fn admit(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        registration: &ActorRegistration,
        source: ActorId,
        context: QueryContextRef,
        author: &WorkLease,
    ) -> Result<(), DriverError> {
        if !registration.matches_scope(author.scope()) {
            return Err(DriverError::InputAuthorMismatch);
        }
        runtime.validate_registration(registration)?;
        let actor = registration.actor();
        self.binding = Some(OwnerBinding {
            runtime: runtime.clone(),
            registration: registration.clone(),
            root: source,
            context,
        });
        self.permit =
            Some(runtime.reserve_admitted_input(actor, source, InputClass::Control, author)?);
        let permit = self.permit.take().expect("original parent-control input");
        match permit.into_role() {
            Ok(role) => self.role = Some(role),
            Err((error, permit)) => {
                self.permit = Some(permit);
                return Err(error);
            }
        }
        Ok(())
    }
    fn matches_owner(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        registration: &ActorRegistration,
    ) -> bool {
        self.binding.as_ref().is_some_and(|original| {
            original.runtime.same_runtime(runtime)
                && original.registration.same_registration(registration)
        })
    }
    /// The Root's actual RemoveHost decision ends future owner-fence issuance.
    /// This grants no H callback/capture, Task, or Scope exit acknowledgement.
    pub(in crate::control_runtime) fn authorize_removal(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), DriverError> {
        let original = self
            .binding
            .as_ref()
            .ok_or(DriverError::PublicationTargetMismatch)?;
        if ticket.actor != original.root
            || !matches!(&ticket.effect, WorkerAction::Control {
                effect: ControlEffect::RemoveHost { context }, ..
            } if *context == original.context)
        {
            return Err(DriverError::PublicationTargetMismatch);
        }
        original.runtime.validate_effect_ticket(ticket)?;
        self.removal_authorized = true;
        Ok(())
    }
    /// No arbitrary producer ever receives this role. Closing issuance and
    /// consuming its exact neutral input is sufficient; no host body, source
    /// or external delivery exit is inferred from an empty inbox.
    pub(in crate::control_runtime) fn close(&mut self) -> Result<bool, DriverError> {
        if let Some(error) = self.unresolved {
            return Err(error);
        }
        if self.closed {
            return Ok(true);
        }
        // Empty is not proof that a live parent has no future fence to send.
        if !self.delivered
            && !self.removal_authorized
            && !self.failed
            && (self.role.is_some() || self.permit.is_some())
        {
            return Ok(false);
        }
        if let Some(input) = self.input {
            let original = self
                .binding
                .as_ref()
                .expect("original parent-control binding");
            match original
                .runtime
                .retire_input(original.registration.actor(), input)
            {
                Ok(()) => self.input = None,
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain,
                ) => return Ok(false),
                Err(error) => {
                    self.unresolved = Some(error);
                    return Err(error);
                }
            }
        }
        if let Some(permit) = self.permit.take()
            && let Err(error) = permit.revoke_unused()
        {
            self.unresolved = Some(error);
            return Err(error);
        }
        if let Some(role) = &mut self.role
            && let Err(error) = role.close_after_exit()
        {
            self.unresolved = Some(error);
            return Err(error);
        }
        self.role = None;
        self.closed = true;
        Ok(true)
    }
    /// The actual completed failure-publication stage fences every future
    /// Core consumer; a real filled input still retires through close().
    pub(super) fn withdraw_failed(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
    ) -> Result<bool, DriverError> {
        let binding = self.binding.as_ref().ok_or(DriverError::UnknownRole)?;
        stage.validate(&binding.runtime, binding.registration.actor())?;
        self.failed = true;
        self.close()
    }
    /// A stopped metadata component fixture has no producer beyond its own
    /// serial driver. This is not production Root removal or a Scope exit.
    #[cfg(test)]
    pub(in crate::control_runtime) fn close_for_teardown(&mut self) -> Result<bool, DriverError> {
        self.removal_authorized = true;
        self.close()
    }
}
impl ContextHostRecord {
    /// The original Root effect fills this Context's prepaid role. Directory
    /// generations do not replace the Context's own protocol generation, and
    /// an owner fence has no transport request or externally issued token.
    pub(crate) fn handoff_owner_fence(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<OwnerFenceDelivery, ContextHostError> {
        let WorkerAction::Control {
            effect: ControlEffect::AbortContext { context, .. },
            ..
        } = &ticket.effect
        else {
            return Err(ContextHostError::WrongEffect);
        };
        if *context != self.identity || ticket.actor != self.control {
            return Err(ContextHostError::WrongContext);
        }
        self.control_runtime
            .validate_effect_ticket(ticket)
            .map_err(ContextHostError::Driver)?;
        if self
            .control_runtime
            .snapshot(self.actor)
            .map_err(ContextHostError::Driver)?
            .failed
        {
            // The existing failed-owner inventory still owns every physical
            // stop and capture obligation. This is not an exit acknowledgement.
            return Ok(OwnerFenceDelivery::FailedOwner);
        }
        if !self
            .resources
            .owner_control
            .matches_owner(&self.control_runtime, self.registration())
        {
            return Err(ContextHostError::WrongRuntime);
        }
        let source = &mut self.resources.owner_control;
        if let Some(error) = source.unresolved {
            return Err(ContextHostError::Driver(error));
        }
        if source.delivered {
            return Ok(OwnerFenceDelivery::AlreadyDelivered);
        }
        if source.closed {
            return Err(ContextHostError::Driver(DriverError::Closed));
        }
        let role = source
            .role
            .as_ref()
            .ok_or(ContextHostError::Driver(DriverError::UnknownRole))?;
        let input = self
            .control_runtime
            .fill_from_effect(
                ticket,
                role,
                WorkerMessage::Context(ContextMessage::Command {
                    work: self.resources.roles[TIMER].work.reference(),
                    request: None,
                    command: ContextInput::OwnerFenced,
                }),
            )
            .map_err(|(error, _neutral)| ContextHostError::Driver(error))?;
        source.input = Some(input);
        source.delivered = true;
        Ok(OwnerFenceDelivery::Delivered)
    }
    pub(crate) fn retire_owner_control(&mut self) -> Result<bool, ContextHostError> {
        self.resources
            .owner_control
            .close()
            .map_err(ContextHostError::Driver)
    }
    pub(crate) fn authorize_owner_control_removal(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), DriverError> {
        self.resources.owner_control.authorize_removal(ticket)?;
        self.resources.internal.authorize_removal(ticket)?;
        // Quiesce closes business intake, while Release and other admitted
        // control operations must still converge. Root's actual RemoveHost
        // handoff is the final gate for newly issued control requests.
        self.resources.control_admission_closed = true;
        Ok(())
    }
}
