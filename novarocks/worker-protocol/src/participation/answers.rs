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

//! One read-only answer author for live final facts and compact facts.

use super::*;
use crate::retention::DecidedTask;

pub(super) struct Answer {
    pub(super) body: ParticipationReplyBody,
    pub(super) effect: Option<ParticipationEffect>,
}
impl Answer {
    fn body(body: ParticipationReplyBody) -> Self {
        Self { body, effect: None }
    }
    pub(super) fn emit(self, token: InputId, step: &mut ParticipationStep) {
        if let Some(effect) = self.effect {
            step.effects.push(effect);
        }
        step.reply(token, self.body);
    }
}
#[derive(Clone, Copy)]
pub(super) enum TaskAnswerRef<'a> {
    Live(&'a TaskRecord),
    Decided(&'a DecidedTask),
}
impl TaskAnswerRef<'_> {
    fn accepted(self) -> bool {
        match self {
            Self::Live(task) => task.accepted(),
            Self::Decided(_) => true,
        }
    }
    fn status(self) -> Result<TaskStatus, CoreError> {
        match self {
            Self::Live(task) => LiveParticipationCore::compose(task),
            Self::Decided(task) => task.compose_status(),
        }
    }
    fn initial_domains(self) -> Arc<Vec<novarocks_execution_contract::TaskDomainReceipt>> {
        match self {
            Self::Live(task) => task.initial_domain_receipts.clone(),
            Self::Decided(task) => task.initial_domains.clone(),
        }
    }
    fn replay_creation(self) -> Result<ParticipationReplyBody, CoreError> {
        match self {
            Self::Live(task) => {
                if let Some(refusal) = &task.creation_refusal {
                    return Ok(match refusal {
                        AdmissionRefusal::Verdict(outcome) => {
                            ParticipationReplyBody::Verdict(RequestResult::Outcome(*outcome))
                        }
                        AdmissionRefusal::Domain(outcome, failure) => {
                            ParticipationReplyBody::Domain(DomainRefusal {
                                outcome: *outcome,
                                failure: failure.clone(),
                            })
                        }
                    });
                }
                if task
                    .pending_reply_outcome
                    .is_some_and(|outcome| outcome != OperationOutcome::Accepted)
                {
                    return Ok(LiveParticipationCore::creation_reply_body(task));
                }
            }
            Self::Decided(_) => {}
        }
        let status = self.status()?;
        Ok(ParticipationReplyBody::Create {
            outcome: OperationOutcome::Idempotent,
            receipt: Arc::new(
                novarocks_execution_contract::CreateTaskReceipt::from_shared(
                    status.identity(),
                    self.initial_domains(),
                    status,
                ),
            ),
        })
    }
    fn expired_creation(self) -> ParticipationReplyBody {
        match self {
            Self::Decided(_) => ParticipationReplyBody::Verdict(RequestResult::Outcome(
                OperationOutcome::Idempotent,
            )),
            Self::Live(record) => {
                if let Some(AdmissionRefusal::Domain(outcome, failure)) = &record.creation_refusal {
                    return ParticipationReplyBody::Domain(DomainRefusal {
                        outcome: *outcome,
                        failure: failure.clone(),
                    });
                }
                ParticipationReplyBody::Verdict(match record.pending_reply_outcome {
                    Some(OperationOutcome::Accepted) => {
                        RequestResult::Outcome(OperationOutcome::Idempotent)
                    }
                    Some(outcome) => RequestResult::Outcome(outcome),
                    None => RequestResult::TimeoutWhileAccepted,
                })
            }
        }
    }
}

pub(super) struct AnswerView<'a> {
    pub(super) identity: ParticipationRef,
    pub(super) state: ParticipationState,
    pub(super) lease: InstalledLease,
    pub(super) establishment: ParticipationEstablishment,
    pub(super) cause: Option<AbortCause>,
    pub(super) domains: &'a ParticipationDomains,
    pub(super) quiesce: Option<&'a QuiesceDecision>,
    pub(super) max_tasks: usize,
    pub(super) live_tasks: Option<&'a BTreeMap<TaskIdentity, TaskRecord>>,
    pub(super) decided_tasks: &'a BTreeMap<TaskIdentity, DecidedTask>,
    pub(super) final_info: Option<&'a BTreeMap<TaskIdentity, Arc<FinalTaskInfo>>>,
}
impl<'a> AnswerView<'a> {
    pub(super) fn from_live(core: &'a LiveParticipationCore) -> Self {
        Self {
            identity: core.identity,
            state: core.state,
            lease: core.lease,
            establishment: core.establishment,
            cause: core.closing.map(|closing| closing.cause),
            domains: &core.participation_domains,
            quiesce: core.quiesce_decision.as_ref(),
            max_tasks: core.config.max_tasks,
            live_tasks: Some(&core.tasks),
            decided_tasks: &core.decided_tasks,
            final_info: Some(&core.final_info),
        }
    }
    fn task(&self, identity: TaskIdentity) -> Option<TaskAnswerRef<'a>> {
        if let Some(task) = self.live_tasks.and_then(|tasks| tasks.get(&identity)) {
            return Some(TaskAnswerRef::Live(task));
        }
        self.decided_tasks
            .get(&identity)
            .map(TaskAnswerRef::Decided)
    }
    pub(super) fn expired_request(&self, command: &ParticipationCommand) -> ParticipationReplyBody {
        match command {
            ParticipationCommand::Create { task, .. }
            | ParticipationCommand::InspectCreation { task } => self.task(*task).map_or_else(
                || {
                    ParticipationReplyBody::Verdict(RequestResult::Outcome(
                        OperationOutcome::OperationTimedOut,
                    ))
                },
                TaskAnswerRef::expired_creation,
            ),
            _ => ParticipationReplyBody::Verdict(RequestResult::Outcome(
                OperationOutcome::OperationTimedOut,
            )),
        }
    }
    pub(super) fn closed_outcome(&self) -> OperationOutcome {
        if self.cause == Some(AbortCause::LeaseExpired) {
            OperationOutcome::LeaseExpired
        } else {
            OperationOutcome::ParticipationTerminalReceipt
        }
    }
    pub(super) fn acknowledgement(
        &self,
        outcome: OperationOutcome,
    ) -> ParticipationAcknowledgement {
        ParticipationAcknowledgement {
            outcome,
            receipt: Arc::new(
                novarocks_execution_contract::ParticipationReceipt::new(self.identity, self.state)
                    .with_lease(self.lease.receipt()),
            ),
            termination_cause: self.cause,
        }
    }
    pub(super) fn establishment_acknowledgement(
        &self,
        outcome: OperationOutcome,
    ) -> ParticipationAcknowledgement {
        let mut ack = self.acknowledgement(outcome);
        if let Some(current) = self.domains.credential() {
            let initial = self.establishment.semantics;
            let catalog = ParticipationDomainInput::Catalog {
                version: DomainVersion::FIRST,
                fingerprint: initial.catalog,
            };
            let filter = ParticipationDomainInput::Filter {
                version: DomainVersion::FIRST,
                fingerprint: initial.shared_filter,
            };
            let credential = ParticipationDomainInput::Credential(crate::domain::CredentialInput {
                credential_domain_id: current.credential_domain_id,
                epoch: current.epoch,
                current_resource: current.resource,
                replacement_resource: current.resource,
                same_material: true,
            });
            let progression = |input| {
                if outcome == OperationOutcome::Accepted {
                    DomainProgression::Apply
                } else {
                    self.domains.classify(input)
                }
            };
            let domains = vec![
                self.domains.receipt(catalog, progression(catalog)),
                self.domains.receipt(filter, progression(filter)),
                self.domains.receipt(credential, progression(credential)),
            ];
            ack.receipt = Arc::new(
                novarocks_execution_contract::ParticipationReceipt::new(self.identity, self.state)
                    .with_lease(self.lease.receipt())
                    .with_domains(domains),
            );
        }
        ack
    }
    pub(super) fn release(
        &self,
        release: novarocks_execution_contract::ReleaseOutcome,
    ) -> ParticipationReplyBody {
        ParticipationReplyBody::Release {
            participation: self.identity,
            release,
            state: self.state,
            termination_cause: self.cause,
        }
    }
    fn accepted_task(
        &self,
        identity: TaskIdentity,
    ) -> Result<Result<TaskAnswerRef<'a>, OperationOutcome>, CoreError> {
        if identity.verify_participation(self.identity).is_err() {
            return Ok(Err(OperationOutcome::IdentityMismatch));
        }
        match self.task(identity) {
            Some(task) if task.accepted() => Ok(Ok(task)),
            Some(_) => Ok(Err(OperationOutcome::InvalidStateOrRequest)),
            None => Ok(Err(TaskControlRefusal::NotOwned.outcome())),
        }
    }
    /// Admission checks only identity and the original Accepted decision;
    /// this does not inspect or publish the deferred read's business facts.
    pub(super) fn task_read_refusal(
        &self,
        task: TaskIdentity,
    ) -> Result<Option<OperationOutcome>, CoreError> {
        Ok(self.accepted_task(task)?.err())
    }
    /// A metadata inspection can refuse an update but never acquire the
    /// original Task's causal predecessor or authorize a domain application.
    pub(super) fn inspect_task_update_outcome(
        &self,
        task: TaskIdentity,
    ) -> Result<OperationOutcome, CoreError> {
        if task.verify_participation(self.identity).is_err() {
            return Ok(OperationOutcome::IdentityMismatch);
        }
        if !matches!(
            self.state,
            ParticipationState::Active | ParticipationState::Quiescing
        ) {
            return Ok(self.closed_outcome());
        }
        Ok(match self.accepted_task(task)? {
            Err(outcome) => outcome,
            Ok(TaskAnswerRef::Decided(_)) => OperationOutcome::TerminalRejected,
            Ok(TaskAnswerRef::Live(record)) if record.state.is_terminal() => {
                OperationOutcome::TerminalRejected
            }
            Ok(TaskAnswerRef::Live(_)) => OperationOutcome::InvalidStateOrRequest,
        })
    }
    pub(super) fn terminal_command(
        &self,
        token: InputId,
        command: &mut ParticipationCommand,
    ) -> Result<Answer, CoreError> {
        let cancel = matches!(command, ParticipationCommand::Cancel { .. });
        let dynamic = matches!(command, ParticipationCommand::GetDynamicFilters { .. });
        let verdict = |outcome| {
            Answer::body(ParticipationReplyBody::Verdict(RequestResult::Outcome(
                outcome,
            )))
        };
        Ok(match command {
            ParticipationCommand::AwaitTaskReadSeal { .. } => return Err(CoreError::IllegalFact),
            ParticipationCommand::InspectTaskUpdate { task } => {
                verdict(self.inspect_task_update_outcome(*task)?)
            }
            ParticipationCommand::Create { task, .. }
            | ParticipationCommand::InspectCreation { task } => {
                if task.verify_participation(self.identity).is_err() {
                    verdict(OperationOutcome::IdentityMismatch)
                } else if let Some(record) = self.task(*task) {
                    Answer::body(record.replay_creation()?)
                } else {
                    verdict(self.closed_outcome())
                }
            }
            ParticipationCommand::Cancel { task, .. }
            | ParticipationCommand::GetFinalInfo { task }
            | ParticipationCommand::GetDynamicFilters { task, .. } => {
                match self.accepted_task(*task)? {
                    Err(outcome) => verdict(outcome),
                    Ok(record) => {
                        if cancel {
                            Answer::body(ParticipationReplyBody::Task {
                                outcome: OperationOutcome::Accepted,
                                status: Arc::new(record.status()?),
                            })
                        } else if dynamic {
                            verdict(OperationOutcome::Idempotent)
                        } else {
                            let info = self
                                .decided_tasks
                                .get(task)
                                .and_then(|task| task.final_info.as_ref())
                                .or_else(|| self.final_info.and_then(|infos| infos.get(task)));
                            match info {
                                Some(info) => {
                                    Answer::body(ParticipationReplyBody::FinalInfo(info.clone()))
                                }
                                None => verdict(OperationOutcome::Idempotent),
                            }
                        }
                    }
                }
            }
            ParticipationCommand::UpdateTask { task, payload, .. } => Answer {
                body: ParticipationReplyBody::Verdict(RequestResult::Outcome(
                    self.closed_outcome(),
                )),
                effect: Some(ParticipationEffect::DomainRequestCompleted {
                    request: token,
                    payload: *payload,
                    task: Some(*task),
                }),
            },
            ParticipationCommand::UpdateParticipation { payload, .. } => Answer {
                body: ParticipationReplyBody::Verdict(RequestResult::Outcome(
                    self.closed_outcome(),
                )),
                effect: Some(ParticipationEffect::DomainRequestCompleted {
                    request: token,
                    payload: *payload,
                    task: None,
                }),
            },
            ParticipationCommand::Subscribe {
                generation,
                position,
            } => {
                if *generation == 0
                    || position.status.len() > self.max_tasks
                    || position.convergence.len() > self.max_tasks
                    || position.required.len() > self.max_tasks
                {
                    verdict(OperationOutcome::InvalidStateOrRequest)
                } else {
                    Answer {
                        body: ParticipationReplyBody::Verdict(RequestResult::Outcome(
                            OperationOutcome::Accepted,
                        )),
                        effect: Some(ParticipationEffect::Subscribe {
                            request: token,
                            generation: *generation,
                            position: Arc::new(std::mem::take(position)),
                        }),
                    }
                }
            }
            ParticipationCommand::Renew { .. } => Answer::body(
                ParticipationReplyBody::Participation(self.acknowledgement(self.closed_outcome())),
            ),
            ParticipationCommand::Quiesce => match self.quiesce {
                Some(decision) => Answer::body(ParticipationReplyBody::Quiesce {
                    outcome: OperationOutcome::ParticipationTerminalReceipt,
                    receipt: decision.receipt.clone(),
                }),
                None => verdict(self.closed_outcome()),
            },
            ParticipationCommand::Abort { .. } => {
                Answer::body(ParticipationReplyBody::Participation(
                    self.acknowledgement(OperationOutcome::ParticipationTerminalReceipt),
                ))
            }
            ParticipationCommand::Release => Answer::body(
                self.release(novarocks_execution_contract::ReleaseOutcome::AlreadyTerminal),
            ),
        })
    }
    pub(super) fn decided_task_command(
        &self,
        token: InputId,
        command: &ParticipationCommand,
    ) -> Result<Option<Answer>, CoreError> {
        let Some(identity) = command.task() else {
            return Ok(None);
        };
        let Some(task) = self.decided_tasks.get(&identity) else {
            return Ok(None);
        };
        let answer = match command {
            ParticipationCommand::UpdateTask { payload, .. }
                if matches!(
                    self.state,
                    ParticipationState::Active | ParticipationState::Quiescing
                ) =>
            {
                Answer {
                    body: ParticipationReplyBody::Verdict(RequestResult::Outcome(
                        OperationOutcome::TerminalRejected,
                    )),
                    effect: Some(ParticipationEffect::DomainRequestCompleted {
                        request: token,
                        payload: *payload,
                        task: Some(identity),
                    }),
                }
            }
            ParticipationCommand::Create { .. } | ParticipationCommand::InspectCreation { .. } => {
                Answer::body(TaskAnswerRef::Decided(task).replay_creation()?)
            }
            ParticipationCommand::Cancel { .. } => Answer::body(ParticipationReplyBody::Task {
                outcome: OperationOutcome::Accepted,
                status: Arc::new(task.compose_status()?),
            }),
            ParticipationCommand::GetFinalInfo { .. } => match &task.final_info {
                Some(info) => Answer::body(ParticipationReplyBody::FinalInfo(info.clone())),
                None => Answer::body(ParticipationReplyBody::Verdict(RequestResult::Outcome(
                    OperationOutcome::Idempotent,
                ))),
            },
            ParticipationCommand::GetDynamicFilters { .. } => {
                Answer::body(ParticipationReplyBody::Verdict(RequestResult::Outcome(
                    OperationOutcome::Idempotent,
                )))
            }
            _ => return Ok(None),
        };
        Ok(Some(answer))
    }
}
