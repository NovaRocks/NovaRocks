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

//! Retire the same unbound guard or never-exposed bound Task admissions.

use super::{TaskHostError, UnpublishedTask, WorkerAction, WorkerActor};
use crate::control_runtime::{
    participation_host::failure::ScopePublicationsDrained,
    source_slot_issuer::SourceSlotIssuePartial, task_disposal::TaskDisposalCustody,
    task_host_call::TaskHostCall,
};
use novarocks_control_runtime::{
    actor_turn::{ActorRuntime, EffectTicket},
    obligation::{ReservationObligation, StepRegistration, WorkObligation},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::identity::{ActorId, ReservationId, ScopedWorkRef, WorkId};
use novarocks_execution_contract::TaskIdentity;
use novarocks_worker_protocol::{creation::PayloadId, participation::ParticipationEffect};

pub(super) struct BoundUnpublished {
    pub(super) lifetime: WorkObligation,
    pub(super) active: ReservationObligation,
    pub(super) source_partials: Vec<SourceSlotIssuePartial>,
    pub(super) host_calls: Vec<TaskHostCall>,
    pub(super) disposal: Option<TaskDisposalCustody>,
}
pub(super) enum UnpublishedCustody {
    Reserved {
        guard: Option<StepRegistration>,
        // These same-guard origins survive its actual unused withdrawal.
        // They observe its header; they never construct a bound WorkObligation.
        scope: ScopeHandle,
        work: WorkId,
        reservation: ReservationId,
    },
    Bound(BoundUnpublished),
}
#[derive(Clone, Copy)]
pub(super) enum UnpublishedRetirement {
    Guard,
    Children,
    Body,
    Captures,
    Inventory,
    Input,
    Header,
    Retired,
}
impl UnpublishedTask {
    pub(super) fn from_reserved(
        task: TaskIdentity,
        payload: PayloadId,
        ticket: &EffectTicket<WorkerAction>,
        guard: StepRegistration,
    ) -> Self {
        Self {
            task,
            loading_input: ticket.input,
            loading_action: ticket.descriptor.id,
            payload,
            retirement: UnpublishedRetirement::Guard,
            custody: UnpublishedCustody::Reserved {
                scope: guard.scope().clone(),
                work: guard.work_id(),
                reservation: guard.reservation_id(),
                guard: Some(guard),
            },
        }
    }
    fn scope(&self) -> &ScopeHandle {
        match &self.custody {
            UnpublishedCustody::Reserved { scope, .. } => scope,
            UnpublishedCustody::Bound(original) => original.lifetime.scope(),
        }
    }
    fn reference(&self) -> ScopedWorkRef {
        match &self.custody {
            UnpublishedCustody::Reserved { scope, work, .. } => ScopedWorkRef {
                scope: scope.id(),
                work: *work,
            },
            UnpublishedCustody::Bound(original) => original.lifetime.reference(),
        }
    }
    fn reservation(&self) -> ReservationId {
        match &self.custody {
            UnpublishedCustody::Reserved { reservation, .. } => *reservation,
            UnpublishedCustody::Bound(original) => original.active.id(),
        }
    }
    fn validate_failure(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        runtime: &ActorRuntime<WorkerActor>,
        actor: ActorId,
    ) -> Result<(), TaskHostError> {
        stage
            .validate_scope(runtime, actor, self.scope())
            .map_err(TaskHostError::ActorTurn)
    }
    /// Both actual custody variants are constructed before any Task port or
    /// loader exists. The raw incarnation is separately authenticated by the
    /// original Create R; an absent owner/optional field cannot mint this fact.
    pub(crate) fn validate_never_built_loader(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        runtime: &ActorRuntime<WorkerActor>,
        actor: ActorId,
    ) -> Result<(), TaskHostError> {
        self.validate_failure(stage, runtime, actor)
    }
    pub(crate) fn matches_loading(&self, ticket: &EffectTicket<WorkerAction>) -> bool {
        ticket.input == self.loading_input
            && ticket.descriptor.id == self.loading_action
            && ticket.descriptor.work == self.reference()
            && matches!(&ticket.effect,
                WorkerAction::Participation(ParticipationEffect::LoadInput { task, payload, reservation })
                    if *task == self.task && *payload == self.payload && *reservation == self.reservation())
    }
    /// Each unit advances one actual unused holder or one real local exit.
    pub(crate) fn advance_failed_unused(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        runtime: &ActorRuntime<WorkerActor>,
        actor: ActorId,
        ledger: &RuntimeLedger,
        budget: usize,
    ) -> Result<bool, TaskHostError> {
        self.validate_failure(stage, runtime, actor)?;
        if let UnpublishedCustody::Reserved { guard, .. } = &mut self.custody {
            if matches!(self.retirement, UnpublishedRetirement::Guard) && budget != 0 {
                let original = guard.take().ok_or(TaskHostError::SourceInventoryPending)?;
                // Actual StepGuard rollback: no payload/callback ever bound,
                // and no Runtime inbox lock is held while it is withdrawn.
                drop(original);
                self.retirement = UnpublishedRetirement::Input;
            }
            return Ok(matches!(
                self.retirement,
                UnpublishedRetirement::Input
                    | UnpublishedRetirement::Header
                    | UnpublishedRetirement::Retired
            ));
        }
        let UnpublishedCustody::Bound(original) = &mut self.custody else {
            unreachable!("same exclusive unpublished custody");
        };
        for _ in 0..budget {
            match self.retirement {
                UnpublishedRetirement::Children => {
                    if let Some(partial) = original.source_partials.last_mut() {
                        if partial
                            .try_retire(ledger)
                            .map_err(TaskHostError::SourceSlots)?
                        {
                            original.source_partials.pop();
                        }
                    } else if let Some(call) = original.host_calls.last() {
                        if call
                            .retire_unused(ledger)
                            .map_err(TaskHostError::HostCall)?
                        {
                            original.host_calls.pop();
                        }
                    } else if let Some(disposal) = &original.disposal {
                        if disposal
                            .retire_unused(ledger)
                            .map_err(TaskHostError::Disposal)?
                        {
                            original.disposal.take();
                        }
                    } else {
                        self.retirement = UnpublishedRetirement::Body;
                    }
                }
                UnpublishedRetirement::Body => {
                    original
                        .lifetime
                        .body_exited()
                        .map_err(TaskHostError::Runtime)?;
                    self.retirement = UnpublishedRetirement::Captures;
                }
                UnpublishedRetirement::Captures => {
                    original
                        .lifetime
                        .captures_exited()
                        .map_err(TaskHostError::Runtime)?;
                    self.retirement = UnpublishedRetirement::Inventory;
                }
                UnpublishedRetirement::Inventory => {
                    if !original
                        .lifetime
                        .actual_exit_ready()
                        .map_err(TaskHostError::Runtime)?
                    {
                        return Err(TaskHostError::SourceInventoryPending);
                    }
                    let quota = original
                        .active
                        .resources()
                        .map_err(TaskHostError::Runtime)?
                        .quota;
                    original
                        .active
                        .release_quota(&quota)
                        .map_err(TaskHostError::Runtime)?;
                    original.active.finish().map_err(TaskHostError::Runtime)?;
                    self.retirement = UnpublishedRetirement::Input;
                }
                UnpublishedRetirement::Input
                | UnpublishedRetirement::Header
                | UnpublishedRetirement::Retired => return Ok(true),
                UnpublishedRetirement::Guard => return Err(TaskHostError::WrongEffect),
            }
        }
        Ok(matches!(
            self.retirement,
            UnpublishedRetirement::Input
                | UnpublishedRetirement::Header
                | UnpublishedRetirement::Retired
        ))
    }
    fn header_retired(&self) -> Result<bool, TaskHostError> {
        match &self.custody {
            UnpublishedCustody::Bound(original) => {
                Ok(original.lifetime.retirement_observer().is_retired())
            }
            UnpublishedCustody::Reserved {
                scope, work, guard, ..
            } => {
                if guard.is_some() {
                    return Err(TaskHostError::SourceInventoryPending);
                }
                // The scope/work origin came from the uniquely held guard.
                // Its real rollback returned before this observation; IDs
                // never repeat. Absence is not inferred from a missing Task.
                Ok(scope
                    .work_lifecycle(*work)
                    .map_err(TaskHostError::Runtime)?
                    .is_none())
            }
        }
    }
    pub(crate) fn retire_after_input(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        runtime: &ActorRuntime<WorkerActor>,
        actor: ActorId,
        budget: usize,
    ) -> Result<(bool, bool), TaskHostError> {
        self.validate_failure(stage, runtime, actor)?;
        if budget == 0 {
            return Ok((false, false));
        }
        match self.retirement {
            UnpublishedRetirement::Input => {
                if runtime
                    .input_custody(actor, self.loading_input)
                    .map_err(TaskHostError::ActorTurn)?
                    .is_some()
                {
                    return Ok((false, false));
                }
                self.retirement = UnpublishedRetirement::Header;
            }
            UnpublishedRetirement::Header => {}
            UnpublishedRetirement::Retired => return Ok((true, false)),
            _ => return Err(TaskHostError::SourceInventoryPending),
        }
        let reaped = if self.header_retired()? {
            0
        } else {
            self.scope().reap_ready(1).map_err(TaskHostError::Runtime)?
        };
        if self.header_retired()? {
            self.retirement = UnpublishedRetirement::Retired;
            Ok((true, false))
        } else {
            Ok((false, reaped != 0))
        }
    }
}

impl super::TaskHostRecord {
    /// The only writer sets loader_issued before the first factory admission.
    /// The Initializing owner variant authenticates the enclosing construction
    /// phase; a missing optional loader alone never supplies this fact.
    pub(crate) fn loader_factory_not_entered(&self) -> bool {
        !self.loader_issued
    }
    pub(crate) fn validate_initializing_load(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), TaskHostError> {
        stage
            .validate_scope(self.resources.causal.runtime(), self.actor, &self.scope)
            .map_err(TaskHostError::ActorTurn)?;
        self.resources
            .causal
            .runtime()
            .effect_completion_observer(ticket)
            .map_err(TaskHostError::ActorTurn)?;
        if !self.loader_factory_not_entered()
            || ticket.actor != self.actor
            || ticket.generation != self.core_generation
            || ticket.descriptor.work != self.resources.lifetime.work().reference()
            || !matches!(&ticket.effect, WorkerAction::Participation(ParticipationEffect::LoadInput { task, payload, reservation })
                if *task == self.task && *payload == self.payload && *reservation == self.resources.active.id())
        {
            return Err(TaskHostError::WrongTaskOrPayload);
        }
        Ok(())
    }
}
