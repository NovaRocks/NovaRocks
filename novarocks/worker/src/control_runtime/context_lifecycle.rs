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

//! Two original Root input owners carry Context retention and physical
//! removal. They spend the original Context admission's fixed storage and
//! Work, independently of every RPC and of the already closed child scope.

use super::{
    actors::{ControlMessage, WorkerAction, WorkerActor, WorkerMessage},
    context_host_jobs::{ContextHostJobError, ContextHostJobs},
    context_ready::ContextReadyNotice,
    failure::ScopePublicationsDrained,
};
use crate::control_runtime_policy::StorageNeed;
use novarocks_control_runtime::{
    driver::{ActorRuntime, DriverError, EffectTicket, InputPermit, InputRole},
    lease::WorkLease,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::{ActionId, ActorId, InputId, WorkId},
    mailbox::InputClass,
};
use novarocks_execution_contract::QueryContextRef;
use novarocks_worker_protocol::{
    context::ContextEffect,
    control::{ControlCore, ControlInput},
};

pub(crate) const CONTEXT_LIFECYCLE_INPUTS: usize = 2;
pub(crate) const CONTEXT_LIFECYCLE_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: CONTEXT_LIFECYCLE_INPUTS,
    batches: CONTEXT_LIFECYCLE_INPUTS,
    effects: CONTEXT_LIFECYCLE_INPUTS * ControlCore::MAX_STEP_WORK,
    work: 0,
    completions: 0,
    captures: 0,
    cuts: 0,
    replies: 0,
};

#[derive(Default)]
struct Port {
    permit: Option<InputPermit<WorkerActor>>,
    input: Option<InputRole<WorkerActor>>,
    sent: bool,
    closed: bool,
}
/// A partial initialization stays in the original Context owner. No source,
/// callable or additional Work is admitted here; no failed initialization can
/// issue a second bank over its original reserved input positions.
pub(crate) struct ContextLifecycle {
    runtime: ActorRuntime<WorkerActor>,
    root: ActorId,
    actor: ActorId,
    context: QueryContextRef,
    child: ScopeHandle,
    work: WorkId,
    ports: [Port; CONTEXT_LIFECYCLE_INPUTS],
    retained: Option<RetentionOrigin>,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum RetentionOrigin {
    Effect(InputId, ActionId),
    FailedOwner,
}
impl ContextLifecycle {
    pub(super) fn new(
        runtime: ActorRuntime<WorkerActor>,
        root: ActorId,
        actor: ActorId,
        context: QueryContextRef,
        child: ScopeHandle,
        author: &WorkLease,
    ) -> Self {
        Self {
            runtime,
            root,
            actor,
            context,
            child,
            work: author.id(),
            ports: Default::default(),
            retained: None,
        }
    }
    pub(super) fn initialize(&mut self, author: &WorkLease) -> Result<(), ContextHostJobError> {
        if author.id() != self.work
            || self
                .ports
                .iter()
                .any(|p| p.input.is_some() || p.permit.is_some())
        {
            return Err(ContextHostJobError::WrongPhase);
        }
        for port in &mut self.ports {
            port.permit = Some(self.runtime.reserve_admitted_input(
                self.root,
                self.actor,
                InputClass::Control,
                author,
            )?);
            match port
                .permit
                .take()
                .expect("original lifecycle reservation")
                .into_role()
            {
                Ok(input) => port.input = Some(input),
                Err((error, permit)) => {
                    port.permit = Some(permit);
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }
    pub(crate) fn forward_retained(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
        normal_close: bool,
    ) -> Result<(), ContextHostJobError> {
        self.runtime.validate_effect_ticket(ticket)?;
        if ticket.actor != self.actor
            || !matches!(&ticket.effect,
            WorkerAction::Context(ContextEffect::RetainContext { context }) if *context == self.context)
        {
            return Err(ContextHostJobError::WrongOwner);
        }
        let key = RetentionOrigin::Effect(ticket.input, ticket.descriptor.id);
        if self.retained == Some(key) {
            return Ok(());
        }
        if self.retained.is_some() {
            return Err(ContextHostJobError::WrongPhase);
        }
        let port = &mut self.ports[0];
        self.runtime
            .fill_from_effect(
                ticket,
                port.input.as_ref().ok_or(ContextHostJobError::WrongPhase)?,
                WorkerMessage::Control(ControlMessage::Command {
                    work: self.work,
                    request: None,
                    command: ControlInput::Retained {
                        context: self.context,
                        normal_close,
                    },
                }),
            )
            .map_err(|(error, _neutral)| ContextHostJobError::Driver(error))?;
        port.sent = true;
        self.retained = Some(key);
        Ok(())
    }
    /// The failed Core cannot issue RetainContext. Its actual drained failure
    /// owner forwards the same preadmitted lifecycle position after physical R
    /// supplies its real retention observation; Root keeps removal authority.
    pub(crate) fn forward_failed_retained(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        jobs: &ContextHostJobs,
    ) -> Result<bool, ContextHostJobError> {
        stage.validate(&self.runtime, self.actor)?;
        if !jobs.belongs_to_context(&self.runtime, self.actor, self.context, &self.child) {
            return Err(ContextHostJobError::WrongOwner);
        }
        if self.retained.is_some() {
            return Ok(false);
        }
        let Some(normal_close) = jobs.normal_close_observation() else {
            return Ok(false);
        };
        let port = &mut self.ports[0];
        if port.sent {
            return Err(ContextHostJobError::WrongPhase);
        }
        port.input
            .as_ref()
            .ok_or(ContextHostJobError::WrongPhase)?
            .fill(WorkerMessage::Control(ControlMessage::Command {
                work: self.work,
                request: None,
                command: ControlInput::Retained {
                    context: self.context,
                    normal_close,
                },
            }))
            .map_err(|(error, _neutral)| ContextHostJobError::Driver(error))?;
        port.sent = true;
        self.retained = Some(RetentionOrigin::FailedOwner);
        Ok(true)
    }
    /// The exact child Scope disappearing proves its actual actor attachment,
    /// Work, reports and captures were removed. H's original return supplies
    /// the independent positive opaque-resource exit proof. Neither a missing
    /// directory row nor a Core terminal state can replace these conditions.
    pub(crate) fn publish_removed(
        &mut self,
        jobs: &ContextHostJobs,
        notice: &ContextReadyNotice,
    ) -> Result<bool, ContextHostJobError> {
        if self.ports[1].sent {
            return Ok(true);
        }
        if self.retained.is_none()
            || !jobs.holder_disposed(&self.runtime, self.actor, self.context, &self.child)?
        {
            return Ok(false);
        }
        match self.child.gates() {
            Err(RuntimeError::UnknownScope) => {}
            Ok(_) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        // Validate this notice's actual Scope removal before Root consumes
        // HostRemoved. Service separately removes the same original directory
        // cell; a scheduling notice itself never proves that removal.
        notice
            .unregister_removed()
            .map_err(ContextHostJobError::Ready)?;
        let port = &mut self.ports[1];
        port.input
            .as_ref()
            .ok_or(ContextHostJobError::WrongPhase)?
            .fill(WorkerMessage::Control(ControlMessage::Command {
                work: self.work,
                request: None,
                command: ControlInput::HostRemoved {
                    context: self.context,
                },
            }))
            .map_err(|(error, _neutral)| ContextHostJobError::Driver(error))?;
        port.sent = true;
        Ok(true)
    }
    /// Close only after both actual Root batches retire. In particular the
    /// HostRemoved batch can still reference this same original payer through
    /// RetireReservation; completing that effect must precede this operation,
    /// and the payer's body/captures/fee retire strictly afterwards.
    pub(crate) fn retire_after_removed(&mut self) -> Result<bool, ContextHostJobError> {
        if !self.ports.iter().all(|p| p.sent) {
            return Ok(false);
        }
        let mut done = true;
        for port in &mut self.ports {
            if port.closed {
                continue;
            }
            let input = port.input.as_mut().ok_or(ContextHostJobError::WrongPhase)?;
            if let Some(id) = input.in_flight()? {
                match self.runtime.retire_input(self.root, id) {
                    Ok(()) => {}
                    Err(
                        DriverError::InputStillQueued
                        | DriverError::EffectsRemain
                        | DriverError::ReservationsRemain,
                    ) => {
                        done = false;
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            input.close_after_exit()?;
            port.closed = true;
        }
        Ok(done)
    }
    /// No Context fact was handed off and no host capture used this bank.
    /// Withdraw the exact partial or unused positions without fabricating a
    /// Retained/HostRemoved input or acknowledgement by the Root actor.
    pub(crate) fn withdraw_unused(&mut self) -> Result<(), ContextHostJobError> {
        if self.ports.iter().any(|port| port.sent) {
            return Err(ContextHostJobError::WrongPhase);
        }
        for port in &mut self.ports {
            if port.closed {
                continue;
            }
            if let Some(input) = port.input.as_mut() {
                input.close_after_exit()?;
            }
            if let Some(permit) = port.permit.take() {
                permit.revoke_unused()?;
            }
            port.closed = true;
        }
        Ok(())
    }
}
