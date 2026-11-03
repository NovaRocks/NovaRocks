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

//! The original Participation changes representation only after exact execution
//! custody exits. Retention and its timer carry no executable Work authority.

use super::*;
use crate::retention::{DecidedTask, RetentionOrigin};

#[derive(Debug)]
enum CoreState {
    Live(Box<LiveParticipationCore>),
    Retained(Box<RetainedParticipation>),
}
#[derive(Debug)]
pub struct ParticipationCore {
    // Taking the owned state permits an O(1) representation change. A panic
    // discards this Core under the existing failed-owner policy, never resumes it.
    inner: Option<CoreState>,
}
impl ParticipationCore {
    pub const MAX_BODY_WORK: usize = LiveParticipationCore::MAX_BODY_WORK;
    pub const MAX_STEP_ACTIONS: usize = LiveParticipationCore::MAX_STEP_ACTIONS;
    pub const INTERNAL_CLASSES: usize = LiveParticipationCore::INTERNAL_CLASSES;
    pub const MAX_STEP_WORK: usize = LiveParticipationCore::MAX_STEP_WORK;
    pub fn max_step_work(&self) -> usize {
        Self::MAX_STEP_WORK
    }
    pub fn new(
        identity: ParticipationRef,
        generation: Generation,
        establishment: ParticipationEstablishment,
        config: ParticipationConfig,
    ) -> Result<Self, CoreError> {
        Ok(Self {
            inner: Some(CoreState::Live(Box::new(LiveParticipationCore::new(
                identity,
                generation,
                establishment,
                config,
            )?))),
        })
    }
    fn state_ref(&self) -> &CoreState {
        self.inner.as_ref().expect("original Participation state")
    }
    fn finish_representation(&mut self, step: &mut ParticipationStep) {
        let state = self.inner.take().expect("original Participation state");
        self.inner = Some(match state {
            CoreState::Live(core) if core.compaction_ready() => {
                let participation = core.identity;
                let close_epoch = core.generation;
                let retained = RetainedParticipation::from_live(*core);
                // Every live-only collection is already empty. The actual
                // O(1) swap and its notice are charged in this same step.
                step.records_visited += 1;
                step.effects.push(ParticipationEffect::RetainParticipation {
                    participation,
                    close_epoch,
                    inventory: retained.retention_inventory(),
                });
                CoreState::Retained(Box::new(retained))
            }
            other => other,
        });
    }
    pub fn is_compact_retained(&self) -> bool {
        matches!(self.state_ref(), CoreState::Retained(_))
    }
    pub fn identity(&self) -> ParticipationRef {
        match self.state_ref() {
            CoreState::Live(core) => core.identity,
            CoreState::Retained(core) => core.identity,
        }
    }
    pub fn generation(&self) -> Generation {
        match self.state_ref() {
            CoreState::Live(core) => core.generation,
            CoreState::Retained(core) => core.generation,
        }
    }
    /// The same admitted policy fixes Host and Core retention interpretation.
    pub fn request_horizon(&self) -> crate::lease::RequestHorizon {
        match self.state_ref() {
            CoreState::Live(core) => core.config.request_horizon,
            CoreState::Retained(core) => core.config.request_horizon,
        }
    }
    pub fn establishment(&self) -> ParticipationEstablishment {
        match self.state_ref() {
            CoreState::Live(core) => core.establishment,
            CoreState::Retained(core) => core.establishment,
        }
    }
    pub fn current_credential(&self) -> Option<CredentialState> {
        match self.state_ref() {
            CoreState::Live(core) => core.participation_domains.credential(),
            CoreState::Retained(core) => core.participation_domains.credential(),
        }
    }
    pub fn state(&self) -> ParticipationState {
        match self.state_ref() {
            CoreState::Live(core) => {
                if core.retention_expired {
                    ParticipationState::Gone
                } else {
                    core.state
                }
            }
            CoreState::Retained(core) => {
                if core.expired {
                    ParticipationState::Gone
                } else {
                    ParticipationState::TerminalRetained
                }
            }
        }
    }
    /// Executable Task records exist only while their actual custody is live.
    pub fn task(&self, task: TaskIdentity) -> Option<&TaskRecord> {
        match self.state_ref() {
            CoreState::Live(core) => core.tasks.get(&task),
            CoreState::Retained(_) => None,
        }
    }
    pub fn decided_task(&self, task: TaskIdentity) -> Option<&DecidedTask> {
        match self.state_ref() {
            CoreState::Live(core) => core.decided_tasks.get(&task),
            CoreState::Retained(core) => core.tasks.get(&task),
        }
    }
    pub fn status(&self, task: TaskIdentity) -> Option<&TaskStatus> {
        match self.state_ref() {
            CoreState::Live(core) => core.status(task),
            CoreState::Retained(core) => core.tasks.get(&task).map(|task| &task.status),
        }
    }
    pub fn tasks(&self) -> impl Iterator<Item = &TaskRecord> {
        match self.state_ref() {
            CoreState::Live(core) => Some(core.tasks.values()),
            CoreState::Retained(_) => None,
        }
        .into_iter()
        .flatten()
    }
    pub fn decided_tasks(&self) -> impl Iterator<Item = &DecidedTask> {
        match self.state_ref() {
            CoreState::Live(core) => core.decided_tasks.values(),
            CoreState::Retained(core) => core.tasks.values(),
        }
    }
    pub fn spent_identities(&self) -> usize {
        match self.state_ref() {
            CoreState::Live(core) => core.spent,
            CoreState::Retained(core) => core.spent,
        }
    }
    pub fn next_deadline(&self) -> Option<MonotonicInstant> {
        match self.state_ref() {
            CoreState::Live(core) => core.next_deadline(),
            CoreState::Retained(core) => core.next_deadline(),
        }
    }
    pub fn has_internal_work(&self) -> bool {
        match self.state_ref() {
            CoreState::Live(core) => core.has_internal_work(),
            CoreState::Retained(core) => {
                !core.replies.pending_replies.is_empty()
                    || core.next_deadline().is_some_and(|at| at <= core.now)
            }
        }
    }
    pub fn handle_request(
        &mut self,
        now: MonotonicInstant,
        request: RequestReplyTag,
        command: ParticipationCommand,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ParticipationStep, CoreError> {
        let mut result = match self.inner.as_mut().expect("original Participation state") {
            CoreState::Live(core) => core.handle_request(now, request, command, capacity),
            CoreState::Retained(core) => core.handle_request(now, request, command),
        };
        if let Ok(step) = &mut result {
            self.finish_representation(step);
        }
        result
    }
    pub fn handle(
        &mut self,
        now: MonotonicInstant,
        input: ParticipationInput,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ParticipationStep, CoreError> {
        let mut result = match self.inner.as_mut().expect("original Participation state") {
            CoreState::Live(core) => core.handle(now, input, capacity),
            CoreState::Retained(core) => core.handle(now, input),
        };
        if let Ok(step) = &mut result {
            self.finish_representation(step);
        }
        result
    }
    pub fn handle_root_end_acknowledged(
        &mut self,
        now: MonotonicInstant,
        original: RequestReplyTag,
        task: TaskIdentity,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ParticipationStep, CoreError> {
        let mut result = match self.inner.as_mut().expect("original Participation state") {
            CoreState::Live(core) => {
                core.handle_root_end_acknowledged(now, original, task, capacity)
            }
            CoreState::Retained(core) => core.root_ack(now, original, task),
        };
        if let Ok(step) = &mut result {
            self.finish_representation(step);
        }
        result
    }
}

#[derive(Debug)]
struct RetainedParticipation {
    identity: ParticipationRef,
    generation: Generation,
    establishment: ParticipationEstablishment,
    config: ParticipationConfig,
    now: MonotonicInstant,
    lease: InstalledLease,
    cause: Option<AbortCause>,
    participation_domains: ParticipationDomains,
    quiesce_decision: Option<QuiesceDecision>,
    tasks: BTreeMap<TaskIdentity, DecidedTask>,
    spent: usize,
    replies: requests::RequestState,
    origin: RetentionOrigin,
    expired: bool,
}
impl RetainedParticipation {
    fn retention_inventory(&self) -> crate::retention::RetainedParticipationInventory {
        crate::retention::RetainedParticipationInventory {
            decided_task_records: self.tasks.len(),
            metadata_bytes_estimate: self
                .tasks
                .len()
                .checked_mul(std::mem::size_of::<(TaskIdentity, DecidedTask)>())
                .and_then(|n| std::mem::size_of::<Self>().checked_add(n)),
        }
    }
    fn from_live(core: LiveParticipationCore) -> Self {
        // Every live-only collection is already empty. Moving these facts is
        // constant work and does not hide a Task/domain-map Drop in one turn.
        Self {
            identity: core.identity,
            generation: core.generation,
            establishment: core.establishment,
            config: core.config,
            now: core.now,
            lease: core.lease,
            cause: core.closing.map(|closing| closing.cause),
            participation_domains: core.participation_domains,
            quiesce_decision: core.quiesce_decision,
            tasks: core.decided_tasks,
            spent: core.spent,
            replies: core.replies,
            origin: core
                .retirement_origin
                .expect("original execution retirement"),
            expired: core.retention_expired,
        }
    }
    fn view(&self) -> answers::AnswerView<'_> {
        answers::AnswerView {
            identity: self.identity,
            state: ParticipationState::TerminalRetained,
            lease: self.lease,
            establishment: self.establishment,
            cause: self.cause,
            domains: &self.participation_domains,
            quiesce: self.quiesce_decision.as_ref(),
            max_tasks: self.config.max_tasks,
            live_tasks: None,
            decided_tasks: &self.tasks,
            final_info: None,
        }
    }
    fn next_deadline(&self) -> Option<MonotonicInstant> {
        (!self.expired)
            .then_some(self.origin.expires_at())
            .into_iter()
            .chain(self.replies.request_deadlines.first().map(|(at, _)| *at))
            .min()
    }
    fn advance_time(
        &mut self,
        now: MonotonicInstant,
        step: &mut ParticipationStep,
    ) -> Result<(), CoreError> {
        if now < self.now {
            return Err(CoreError::TimeRegressed);
        }
        self.now = now;
        if !self.expired && self.origin.has_expired(now) {
            self.expired = true;
            step.effects.push(ParticipationEffect::Expired {
                participation: self.identity,
                close_epoch: self.generation,
            });
        }
        Ok(())
    }
    fn handle_request(
        &mut self,
        now: MonotonicInstant,
        request: RequestReplyTag,
        mut command: ParticipationCommand,
    ) -> Result<ParticipationStep, CoreError> {
        if now < self.now {
            return Err(CoreError::TimeRegressed);
        }
        if self.replies.requests.contains_key(&request.token) {
            return Err(CoreError::WrongIdentity);
        }
        let mut step = ParticipationStep::default();
        self.advance_time(now, &mut step)?;
        let task = command.task();
        let domain_completion = match &command {
            ParticipationCommand::UpdateTask { task, payload, .. } => {
                Some(ParticipationEffect::DomainRequestCompleted {
                    request: request.token,
                    payload: *payload,
                    task: Some(*task),
                })
            }
            ParticipationCommand::UpdateParticipation { payload, .. } => {
                Some(ParticipationEffect::DomainRequestCompleted {
                    request: request.token,
                    payload: *payload,
                    task: None,
                })
            }
            _ => None,
        };
        if self.expired || now.has_reached(request.deadline) {
            let body = if self.expired {
                ParticipationReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::Gone))
            } else {
                self.view().expired_request(&command)
            };
            step.requests.push(ParticipationRequestSettlement::new(
                request,
                task,
                Arc::new(body),
            ));
            step.effects.extend(domain_completion);
            step.records_visited = 1;
            return Ok(step);
        }
        if self.replies.requests.len() >= self.config.max_pending_requests {
            return Err(CoreError::IllegalFact);
        }
        if let Some(task) = task {
            self.replies.request_tasks.insert(request.token, task);
        }
        self.replies.requests.insert(request.token, request);
        self.replies
            .request_deadlines
            .insert((request.deadline, request.token));
        if let ParticipationCommand::AwaitTaskReadSeal { task, command } = command {
            if let Some(outcome) = self.view().task_read_refusal(task)? {
                step.reply(
                    request.token,
                    ParticipationReplyBody::Verdict(RequestResult::Outcome(outcome)),
                );
            } else {
                self.replies.await_task_read(request.token, command)?;
            }
        } else {
            self.view()
                .terminal_command(request.token, &mut command)?
                .emit(request.token, &mut step);
        }
        let visited = self.replies.settle(now, &mut step);
        step.records_visited = 1 + visited + usize::from(!step.requests.is_empty());
        Ok(step)
    }
    fn handle(
        &mut self,
        now: MonotonicInstant,
        input: ParticipationInput,
    ) -> Result<ParticipationStep, CoreError> {
        if matches!(
            input,
            ParticipationInput::Request(_) | ParticipationInput::RootEndAcknowledged { .. }
        ) {
            return Err(CoreError::WrongIdentity);
        }
        let mut step = ParticipationStep::default();
        self.advance_time(now, &mut step)?;
        let mut decisions_visited = 0;
        match input {
            ParticipationInput::ResumeTaskRead { request, task } => {
                if let Some((original, command)) = self.replies.take_task_read(request, task) {
                    if self.expired || now.has_reached(original.deadline) {
                        let body = if self.expired {
                            ParticipationReplyBody::Verdict(RequestResult::Outcome(
                                OperationOutcome::Gone,
                            ))
                        } else {
                            ParticipationReplyBody::Verdict(RequestResult::TimeoutWhileAccepted)
                        };
                        self.replies
                            .complete(request, Arc::new(body), now, &mut step);
                    } else {
                        let mut command = command.into_command(task);
                        self.view()
                            .terminal_command(request, &mut command)?
                            .emit(request, &mut step);
                        decisions_visited = self.replies.settle(now, &mut step);
                    }
                }
            }
            ParticipationInput::Tick | ParticipationInput::NextInternal => {
                if let Some((&token, _)) = self.replies.pending_replies.first_key_value() {
                    self.replies.flush(token, &mut step);
                }
                if step.requests.is_empty() {
                    self.replies.expire(now, &mut step);
                }
            }
            ParticipationInput::ContinueRequestReplies { operation } => {
                self.replies.flush(operation, &mut step)
            }
            ParticipationInput::RequestCompleted { operation, outcome } => {
                self.replies.complete(
                    operation,
                    Arc::new(ParticipationReplyBody::Verdict(RequestResult::Outcome(
                        outcome,
                    ))),
                    now,
                    &mut step,
                );
            }
            ParticipationInput::ProbeEstablish {
                participation,
                request,
                ..
            } => {
                if participation != self.identity {
                    return Err(CoreError::WrongIdentity);
                }
                let outcome = if self.expired {
                    OperationOutcome::Gone
                } else {
                    OperationOutcome::ParticipationTerminalReceipt
                };
                step.effects.push(ParticipationEffect::EstablishProbed {
                    participation,
                    request: request.request,
                    result: EstablishReplayResult::Decided(
                        self.view().establishment_acknowledgement(outcome),
                    ),
                    decided_at: now,
                });
            }
            ParticipationInput::ExecutionCustodyRetired {
                participation,
                close_epoch,
                origin,
            } => {
                if participation != self.identity
                    || close_epoch != self.generation
                    || origin.retired_at() > now
                {
                    return Err(CoreError::WrongIdentity);
                }
                if origin != self.origin
                    || origin
                        != RetentionOrigin::new(origin.retired_at(), self.config.request_horizon)
                {
                    return Err(CoreError::IllegalFact);
                }
            }
            ParticipationInput::TaskExecutionCustodyRetired {
                task,
                payload,
                close_epoch,
            } => {
                if !self.tasks.get(&task).is_some_and(|record| {
                    record.custody_payload == payload && record.custody_epoch == close_epoch
                }) {
                    return Err(CoreError::WrongIdentity);
                }
            }
            ParticipationInput::OwnerFenced | ParticipationInput::ContinueCompaction => {}
            _ => return Err(CoreError::IllegalFact),
        }
        step.records_visited = 1 + decisions_visited + usize::from(!step.requests.is_empty());
        Ok(step)
    }
    fn root_ack(
        &mut self,
        now: MonotonicInstant,
        request: RequestReplyTag,
        task: TaskIdentity,
    ) -> Result<ParticipationStep, CoreError> {
        if self.replies.requests.contains_key(&request.token) {
            return Err(CoreError::WrongIdentity);
        }
        let mut step = ParticipationStep::default();
        self.advance_time(now, &mut step)?;
        let body = if task.verify_participation(self.identity).is_err() {
            ParticipationReplyBody::Verdict(RequestResult::Outcome(
                OperationOutcome::IdentityMismatch,
            ))
        } else if let Some(task) = self.tasks.get_mut(&task) {
            task.output = TaskOutputFacts::new(true);
            ParticipationReplyBody::RootDrain(RootDrainVerdict::AlreadyTerminal(
                task.status.state(),
            ))
        } else {
            ParticipationReplyBody::Verdict(RequestResult::Outcome(
                OperationOutcome::InvalidStateOrRequest,
            ))
        };
        step.requests.push(ParticipationRequestSettlement::new(
            request,
            Some(task),
            Arc::new(body),
        ));
        step.records_visited = 1;
        Ok(step)
    }
}

impl LiveParticipationCore {
    pub(super) fn next_task_identity(&self, after: Option<TaskIdentity>) -> Option<TaskIdentity> {
        use std::ops::Bound::{Excluded, Unbounded};
        let live = match after {
            Some(cursor) => self.tasks.range((Excluded(cursor), Unbounded)).next(),
            None => self.tasks.first_key_value(),
        }
        .map(|(&identity, _)| identity);
        let decided = match after {
            Some(cursor) => self
                .decided_tasks
                .range((Excluded(cursor), Unbounded))
                .next(),
            None => self.decided_tasks.first_key_value(),
        }
        .map(|(&identity, _)| identity);
        live.into_iter().chain(decided).min()
    }
    pub(super) fn note_task_custody_retired(
        &mut self,
        task: TaskIdentity,
        payload: PayloadId,
        close_epoch: Generation,
        step: &mut ParticipationStep,
    ) -> Result<(), CoreError> {
        if let Some(record) = self.decided_tasks.get(&task) {
            return if record.custody_payload == payload && record.custody_epoch == close_epoch {
                Ok(())
            } else {
                Err(CoreError::WrongIdentity)
            };
        }
        let record = self.tasks.get(&task).ok_or(CoreError::UnknownTask)?;
        if record.payload != payload || record.applied_close_generation != close_epoch {
            return Err(CoreError::WrongIdentity);
        }
        if !record.accepted() {
            return self.begin_rejected_creation_retirement(task, payload, step);
        }
        if record.phase != CreationPhase::Retained
            || !record.retirement_ready()
            || !record.proposals.is_empty()
            || record.initial_creation.is_some()
            || self.pending_task_updates.contains_key(&task)
            || self.task_domain_waiters.contains_key(&task)
            || self.creation_reply_tasks.contains(&task)
            || record.scheduled_close_generation.is_some()
            || record.scheduled_quiesce_generation.is_some()
        {
            return Err(CoreError::IllegalFact);
        }
        self.custody_retired.insert(task);
        Ok(())
    }
    pub(super) fn advance_compaction(
        &mut self,
        step: &mut ParticipationStep,
    ) -> Result<(), CoreError> {
        let Some(task) = self.custody_retired.first().copied() else {
            return Ok(());
        };
        if let Some(domains) = self.task_domains.get_mut(&task) {
            if domains.retire_one() {
                self.task_domains.remove(&task);
            }
            step.records_visited += 1;
            return Ok(());
        }
        if self.dynamic_filters.remove(&task).is_some() {
            step.records_visited += 1;
            return Ok(());
        }
        if let Some(due) = self.metric_due.remove(&task) {
            self.metric_deadlines.remove(&(due, task));
            step.records_visited += 2;
            return Ok(());
        }
        if self.metrics.remove(&task).is_some() {
            step.records_visited += 1;
            return Ok(());
        }
        let record = self.tasks.get(&task).ok_or(CoreError::IllegalFact)?;
        if !record.operator_statistics.is_empty() {
            return Err(CoreError::IllegalFact);
        }
        let status = Self::compose(record)?;
        let final_info = self.final_info.remove(&task);
        let record = self.tasks.remove(&task).expect("same exiting Task");
        let decided = record.into_decided(status, final_info)?;
        if self.decided_tasks.insert(task, decided).is_some() {
            return Err(CoreError::IllegalFact);
        }
        self.custody_retired.remove(&task);
        step.records_visited += 1;
        Ok(())
    }
    pub(super) fn expire_retention(&mut self, step: &mut ParticipationStep) {
        if !self.retention_expired
            && self
                .retirement_origin
                .is_some_and(|origin| origin.has_expired(self.now))
        {
            self.retention_expired = true;
            step.effects.push(ParticipationEffect::Expired {
                participation: self.identity,
                close_epoch: self.generation,
            });
        }
    }
    fn compaction_ready(&self) -> bool {
        self.state == ParticipationState::TerminalRetained
            && self.materialization_decided
            && self.retirement_origin.is_some()
            && self.tasks.is_empty()
            && self.custody_retired.is_empty()
            && self.preparing == 0
            && self.unretired_tasks == 0
            && !self.host_releasing
            && self.release_request.is_none()
            && self.scheduled_closes == 0
            && self.abort_waiters.is_empty()
            && self.quiesce_waiters.is_empty()
            && self.pending_quiesce.is_none()
            && self.pending_task_updates.is_empty()
            && self.pending_participation_update.is_none()
            && self.participation_domain_comparison.is_none()
            && self.task_domain_waiters.is_empty()
            && self.participation_domain_waiters.is_empty()
            && self.task_domains.is_empty()
            && self.dynamic_filters.is_empty()
            && self.final_info.is_empty()
            && self.metrics.is_empty()
            && self.metric_deadlines.is_empty()
            && self.metric_due.is_empty()
            && self.runnable_task_domains.is_empty()
            && self.creation_reply_tasks.is_empty()
    }
}
