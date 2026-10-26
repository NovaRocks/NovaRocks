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

//! A Task's actual admitted causal positions. Registration seals its prefix
//! under the proposal handoff lock; this host custodian moves only neutral
//! inputs to the common runtime. It never runs callbacks or source disposal.

use super::actors::{ContextMessage, WorkerActor, WorkerMessage};
use super::failure::ScopePublicationsDrained;
use crate::{
    settlement::RegistrationNotice,
    task_causal_input::CausalInputRole,
    task_port::TaskPendingNotice,
    task_proposal_ingress::{CausalEventInput, CausalEventKind, TaskProposalIngress},
};
use novarocks_control_runtime::driver::{ActorRuntime, DriverError, InputPhase};
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{ActorId, Generation, InputId, WorkId},
    mailbox::InputClass,
};
use novarocks_execution_contract::TaskIdentity;
use novarocks_worker_protocol::{
    context::ContextInput,
    proposal_summary::{CutError, CutId},
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CausalError {
    Cut(CutError),
    Driver(DriverError),
    UnknownRole,
    AlreadyAdmitted,
    BeforeAccepted,
    NotDelivered,
    InputObservationMissing,
}
impl From<CutError> for CausalError {
    fn from(error: CutError) -> Self {
        Self::Cut(error)
    }
}
impl From<DriverError> for CausalError {
    fn from(error: DriverError) -> Self {
        Self::Driver(error)
    }
}

#[must_use = "The original causal author must send or explicitly retire its unused role"]
pub(crate) struct TaskCausalSender {
    ingress: Arc<TaskProposalIngress>,
    cut: CutId,
    pending: Arc<dyn TaskPendingNotice>,
}
impl TaskCausalSender {
    pub(crate) fn send(self) {
        self.ingress
            .register_causal_event(self.cut)
            .expect("the original admitted role seals its causal prefix once");
        self.pending.pending();
    }
    pub(crate) fn unused(self) {
        self.ingress
            .revoke_event(self.cut)
            .expect("the original unused causal author is revoked once");
        self.pending.pending();
    }
}
impl RegistrationNotice for TaskCausalSender {
    fn bind_work(&mut self, work: WorkId) {
        self.ingress
            .bind_event_work(self.cut, work)
            .expect("one original Work binding for the prepaid settlement cut");
    }
    fn registered(self: Box<Self>) {
        (*self).send();
    }
    fn unused(self: Box<Self>) {
        (*self).unused();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CausalDelivery {
    pub cut: CutId,
    pub prefix: Option<InputId>,
    pub event: InputId,
}
struct Delivered {
    // None means no input transferred, not an input guessed absent later.
    proof: Option<InputId>,
    // Partial fill failure returns the precise original neutral payloads.
    // They remain in this paid role until explicit failed-owner takeover.
    rejected: Option<(Option<WorkerMessage>, WorkerMessage)>,
}
struct Role {
    inputs: CausalInputRole<WorkerActor>,
    delivered: Option<Delivered>,
}

pub(crate) struct TaskCausalInbox {
    runtime: ActorRuntime<WorkerActor>,
    actor: ActorId,
    source: ActorId,
    task: TaskIdentity,
    // Protocol close generation is independent of the runtime failure epoch.
    generation: Generation,
    ingress: Arc<TaskProposalIngress>,
    roles: BTreeMap<CutId, Role>,
}
impl TaskCausalInbox {
    pub(crate) fn new(
        runtime: ActorRuntime<WorkerActor>,
        actor: ActorId,
        source: ActorId,
        task: TaskIdentity,
        generation: Generation,
        ingress: Arc<TaskProposalIngress>,
    ) -> Self {
        Self {
            runtime,
            actor,
            source,
            task,
            generation,
            ingress,
            roles: BTreeMap::new(),
        }
    }
    /// The caller owns this role's original prepaid storage. Admission happens
    /// before attaching its callback. A failed pure reservation rolls back only
    /// these actually unused input positions, outside the proposal lock.
    pub(crate) fn admit_settlement(
        &mut self,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
    ) -> Result<(CutId, Box<dyn RegistrationNotice>), CausalError> {
        let (cut, notice) = self.admit(CausalEventKind::Settlement, consumers, pending)?;
        Ok((cut, Box::new(notice)))
    }
    pub(crate) fn admit_close(
        &mut self,
        generation: Generation,
        work: WorkId,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
    ) -> Result<(CutId, TaskCausalSender), CausalError> {
        let (cut, mut sender) =
            self.admit(CausalEventKind::ApplyClose(generation), consumers, pending)?;
        sender.bind_work(work);
        Ok((cut, sender))
    }
    fn admit(
        &mut self,
        kind: CausalEventKind,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
    ) -> Result<(CutId, TaskCausalSender), CausalError> {
        let mut inputs = CausalInputRole::reserve(
            &self.runtime,
            self.actor,
            self.source,
            InputClass::Lifecycle,
        )?;
        let cut = match self.ingress.reserve_event_cut(self.task, kind, consumers) {
            Ok(cut) => cut,
            Err(error) => {
                inputs
                    .close_after_exit()
                    .expect("unbound causal input holders have really exited");
                return Err(error.into());
            }
        };
        assert!(
            self.roles
                .insert(
                    cut,
                    Role {
                        inputs,
                        delivered: None
                    }
                )
                .is_none()
        );
        Ok((
            cut,
            TaskCausalSender {
                ingress: self.ingress.clone(),
                cut,
                pending,
            },
        ))
    }
    /// This custodian is exclusive; all roles for this Task use the same
    /// source FIFO. A different Task has its own source and can keep advancing.
    pub(crate) fn deliver_next(&mut self) -> Result<Option<CausalDelivery>, CausalError> {
        let input = match self.ingress.take_next_event(false) {
            Ok(input) => input,
            Err(CutError::NotReady | CutError::Consuming) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let cut = input.prefix.cut.expect("causal role owns one real cut");
        let role = self
            .roles
            .get_mut(&cut)
            .expect("original admitted causal input positions");
        assert!(role.delivered.is_none(), "one delivery per admitted role");
        let prefix = if input.prefix.summary.is_empty() {
            None
        } else {
            Some(WorkerMessage::Context(ContextMessage::Command {
                work: input.work,
                request: None,
                command: ContextInput::Summary {
                    task: self.task,
                    generation: self.generation,
                    cut: Some(cut),
                    summary: input.prefix.summary,
                },
            }))
        };
        let next_generation = match &input.event {
            ContextInput::ApplyTaskClose { generation, .. } => Some(*generation),
            _ => None,
        };
        let event = WorkerMessage::Context(ContextMessage::Command {
            work: input.work,
            request: None,
            command: input.event,
        });
        match role.inputs.fill_with_receipt(prefix, event) {
            Ok((prefix, event)) => {
                if let Some(generation) = next_generation {
                    self.generation = generation;
                }
                role.delivered = Some(Delivered {
                    proof: prefix.or(Some(event)),
                    rejected: None,
                });
                Ok(Some(CausalDelivery { cut, prefix, event }))
            }
            Err(failure) => {
                role.delivered = Some(Delivered {
                    proof: failure.predecessor_input,
                    rejected: Some((failure.predecessor, failure.event)),
                });
                Err(failure.error.into())
            }
        }
    }
    /// Observe the actual input before retiring its record. Neither numeric
    /// InputId order, a published hint, nor absence from a queue proves Core
    /// consumption. An empty prefix is covered by its actual event input.
    pub(crate) fn observe_core(&mut self, cut: CutId) -> Result<Consumption, CausalError> {
        let consumption = self.ingress.cut_consumption(cut).ok_or(CutError::Unknown)?;
        if consumption != Consumption::Pending {
            return Ok(consumption);
        }
        let delivered = self
            .roles
            .get(&cut)
            .ok_or(CausalError::UnknownRole)?
            .delivered
            .as_ref()
            .ok_or(CausalError::NotDelivered)?;
        let phase = match delivered.proof {
            Some(input) => Some(
                self.runtime
                    .input_custody(self.actor, input)?
                    .ok_or(CausalError::InputObservationMissing)?
                    .phase,
            ),
            None => None,
        };
        if phase == Some(InputPhase::Committed) {
            self.ingress.summary_consumed(Some(cut))?;
            return Ok(Consumption::Consumed);
        }
        Ok(Consumption::Pending)
    }
    pub(crate) fn abandon_core(
        &mut self,
        drained: &ScopePublicationsDrained<'_>,
        cut: CutId,
    ) -> Result<Consumption, CausalError> {
        drained.validate(&self.runtime, self.actor)?;
        let observed = self.observe_core(cut)?;
        if observed != Consumption::Pending {
            return Ok(observed);
        }
        let delivered = self
            .roles
            .get(&cut)
            .ok_or(CausalError::UnknownRole)?
            .delivered
            .as_ref()
            .ok_or(CausalError::NotDelivered)?;
        let abandoned = match delivered.proof {
            Some(input) => {
                self.runtime
                    .input_custody(self.actor, input)?
                    .ok_or(CausalError::InputObservationMissing)?
                    .phase
                    == InputPhase::AbandonedByOwnerFailure
            }
            None => true, // Exact partial-fill receipt proves no input transferred.
        };
        if !abandoned {
            return Ok(Consumption::Pending);
        }
        self.ingress.fail();
        self.ingress.abandon_consuming_event(cut)?;
        Ok(Consumption::AbandonedByOwnerFailure)
    }
    /// The failure owner uses the actual fixed runtime state, never a damaged
    /// Core table. Returned summaries still name sources in original custody;
    /// abandonment grants no source/capture exit or storage release.
    pub(crate) fn take_failed_queued(
        &mut self,
        drained: &ScopePublicationsDrained<'_>,
    ) -> Result<Option<CausalEventInput>, CausalError> {
        drained.validate(&self.runtime, self.actor)?;
        self.ingress.fail();
        match self.ingress.take_next_event(true) {
            Ok(input) => {
                let cut = input.prefix.cut.expect("original queued role cut");
                let role = self.roles.get_mut(&cut).expect("queued input role custody");
                assert!(role.delivered.is_none());
                role.delivered = Some(Delivered {
                    proof: None,
                    rejected: None,
                });
                Ok(Some(input))
            }
            Err(CutError::NotReady | CutError::Consuming) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    /// Actual failed-owner teardown takes any values that never transferred.
    /// The runtime still owns a partially handed-off prefix, if one exists.
    pub(crate) fn take_rejected(
        &mut self,
        cut: CutId,
    ) -> Option<(Option<WorkerMessage>, WorkerMessage)> {
        self.roles
            .get_mut(&cut)?
            .delivered
            .as_mut()?
            .rejected
            .take()
    }
    /// Source/capture/effect owners must first supply their true exit and all
    /// cut consumers. This wrapper only verifies/returns the original input
    /// positions; input retirement alone cannot manufacture those other facts.
    pub(crate) fn retire_after_exit(&mut self, cut: CutId) -> Result<(), CausalError> {
        let role = self.roles.get_mut(&cut).ok_or(CausalError::UnknownRole)?;
        if role.inputs.in_flight()?.iter().any(Option::is_some)
            || role
                .delivered
                .as_ref()
                .is_some_and(|d| d.rejected.is_some())
        {
            return Err(DriverError::RoleBusy.into());
        }
        if self.ingress.event_unused(cut)? {
            self.ingress.revoke_unused_cut(cut)?;
        } else {
            self.ingress.retire_cut(cut)?;
        }
        role.inputs
            .close_after_exit()
            .expect("exclusive idle causal positions retain their actor");
        self.roles.remove(&cut);
        Ok(())
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.roles.is_empty()
    }
}
