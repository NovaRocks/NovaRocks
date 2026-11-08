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

//! Original attempt terminal priorities. Source ownership, QA recovery policy,
//! actual gate settlement and work/capture exit remain with their authors.

use crate::{
    error::{ParticipantObservationFailure, TaskExecutionError},
    execution::QueryTaskExecution,
    failure::{AttemptFailure, HostFailureKind},
    input::MembershipObservation,
};
use novarocks_coordination_contract::{
    AttemptRequestToken, AttemptStopReason, AttemptTerminal, MonotonicInstant,
    SelectedTaskTermination,
};
use novarocks_execution_contract::{
    OperationKind, OperationOutcome, ParticipationRef, TaskOperationId,
};
use novarocks_types::BackendProcessId;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreReadyObservation {
    Unknown(BackendProcessId),
    RejectedIdentity {
        participation: ParticipationRef,
        operation: TaskOperationId,
        kind: OperationKind,
    },
}
impl PreReadyObservation {
    pub(crate) fn backend(self) -> BackendProcessId {
        match self {
            Self::Unknown(backend) => backend,
            Self::RejectedIdentity { participation, .. } => participation.backend_process_id(),
        }
    }
    fn unknown(self) -> bool {
        matches!(self, Self::Unknown(_))
    }
}
/// Current SAME original owner/operation window. The Core constructs this
/// directly from its protocol owner, never from a later ACK/renewed duration.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OriginalHorizon {
    observation: PreReadyObservation,
    remaining: Option<Duration>,
}
impl OriginalHorizon {
    pub(crate) fn from_execution(
        execution: &QueryTaskExecution,
        observation: PreReadyObservation,
    ) -> Self {
        let remaining = match observation {
            PreReadyObservation::Unknown(backend) => {
                execution.pre_ready_observation_remaining(backend)
            }
            PreReadyObservation::RejectedIdentity {
                participation,
                operation,
                kind: OperationKind::UpdateParticipation,
            } => execution.rejected_establish_observation_remaining(participation, operation),
            PreReadyObservation::RejectedIdentity {
                participation,
                operation,
                kind: OperationKind::CreateTask,
            } => execution.rejected_create_observation_remaining(participation, operation),
            PreReadyObservation::RejectedIdentity { .. } => None,
        };
        Self {
            observation,
            remaining,
        }
    }
    fn open(self) -> bool {
        self.remaining.is_some_and(|remaining| !remaining.is_zero())
    }
}
/// Pure aggregate from the original authorization/withdrawal owner. Applied
/// means admission was closed AND its currently definitely-unsent settlements
/// actually published. Ready additionally means the original gate ledger has
/// no outstanding role-local issue. Neither state means RPC/capture exit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GatePreparation {
    Open,
    Applying,
    Applied { ready: bool },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalWake {
    /// A newly completed original round, including moved/notify/process-change
    /// wakeups. Input kind alone does not establish this boundary.
    Round,
    Continuation,
    Cancellation,
}
#[derive(Debug)]
pub(crate) struct TerminalFacts<'a> {
    pub(crate) success_ready: bool,
    pub(crate) selected_task: Option<&'a SelectedTaskTermination>,
    pub(crate) gate: GatePreparation,
    pub(crate) horizon: Option<OriginalHorizon>,
    pub(crate) wake: TerminalWake,
}
#[derive(Clone, Debug)]
pub(crate) enum TerminalDirective {
    /// An intra-Core action. P09's gate owner performs the pure close and
    /// drives original settlement/withdrawal effects; not a host policy call.
    PrepareConvergence,
    QueryPermission {
        request: AttemptRequestToken,
        refusal: AttemptFailure,
    },
    ObserveProcess {
        request: AttemptRequestToken,
        original: BackendProcessId,
    },
    NeedRequestCredit,
    Selected(AttemptTerminal<AttemptFailure>),
    Wait,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplyDisposition {
    Applied,
    Stale,
}
#[derive(Clone, Copy, Debug)]
enum ObservationPhase {
    Permission,
    Process,
    Waiting { expires_at: MonotonicInstant },
    Dormant,
}
#[derive(Clone, Copy, Debug)]
struct Flight {
    request: AttemptRequestToken,
    issued_at: MonotonicInstant,
    observation: PreReadyObservation,
}
#[derive(Clone, Debug)]
struct Pending {
    failure: AttemptFailure,
    observation: Option<PreReadyObservation>,
    allows_task: bool,
}
#[derive(Clone, Debug)]
struct Stop {
    reason: AttemptStopReason,
    failure: AttemptFailure,
}
#[derive(Debug)]
pub(crate) struct TerminalController {
    pending: Option<Pending>,
    residual: Option<AttemptFailure>,
    selected: Option<AttemptTerminal<AttemptFailure>>,
    // The old synchronous prepare already chose this forced terminal cut.
    // Only its actual close/settlement application may delay publication.
    preparing_terminal: Option<AttemptTerminal<AttemptFailure>>,
    stop: Option<Stop>,
    logical_failed: bool,
    phase: ObservationPhase,
    // An original MR turn cannot receive a later primary input while its QA
    // await or exact process read is unresolved. This is a short turn barrier,
    // never an ever-failed gate on later legal receive turns.
    decision_turn: Option<PreReadyObservation>,
    permission: Option<Flight>,
    permission_watch: Option<Flight>,
    qualification_changed: bool,
    sample: Option<Flight>,
}
impl TerminalController {
    pub(crate) fn new() -> Self {
        Self {
            pending: None,
            residual: None,
            selected: None,
            preparing_terminal: None,
            stop: None,
            logical_failed: false,
            phase: ObservationPhase::Dormant,
            decision_turn: None,
            permission: None,
            permission_watch: None,
            qualification_changed: false,
            sample: None,
        }
    }
    pub(crate) fn permits_protocol_turn(&self) -> bool {
        self.decision_turn.is_none() && self.preparing_terminal.is_none() && self.selected.is_none()
    }
    pub(crate) fn observation(&self) -> Option<PreReadyObservation> {
        self.pending
            .as_ref()
            .and_then(|pending| pending.observation)
    }
    pub(crate) fn terminal(&self) -> Option<&AttemptTerminal<AttemptFailure>> {
        self.selected.as_ref()
    }
    pub(crate) fn residual(&self) -> Option<&AttemptFailure> {
        self.residual.as_ref()
    }
    pub(crate) fn requires_logical_failure_stand_down(&self) -> bool {
        self.logical_failed
    }
    pub(crate) fn stop_reason(&self) -> Option<AttemptStopReason> {
        self.stop.as_ref().map(|stop| stop.reason)
    }
    pub(crate) fn observe_stop(&mut self, reason: AttemptStopReason, failure: AttemptFailure) {
        self.logical_failed |= reason == AttemptStopReason::LogicalExecutionFailed;
        if self.stop.is_none() {
            self.stop = Some(Stop { reason, failure });
        }
    }
    /// Only the first pending failure wins, except exact pre-ready Unknown
    /// may refine to a definite original failure. No string/class inference.
    pub(crate) fn record_execution_failure(
        &mut self,
        error: TaskExecutionError,
        execution: &QueryTaskExecution,
    ) -> Result<bool, TaskExecutionError> {
        let window = Self::refusal(&error)
            .map(|observation| OriginalHorizon::from_execution(execution, observation));
        self.record(AttemptFailure::TaskProtocol(Box::new(error)), window)
    }
    pub(crate) fn record_original_failure(
        &mut self,
        failure: AttemptFailure,
    ) -> Result<bool, TaskExecutionError> {
        self.record(failure, None)
    }
    fn refusal(error: &TaskExecutionError) -> Option<PreReadyObservation> {
        use novarocks_execution_contract::operation::OperationResult;
        match error {
            TaskExecutionError::PreReadyEstablishRejected {
                participation,
                operation_id,
                rejection:
                    OperationResult::Verdict {
                        outcome: OperationOutcome::IdentityMismatch,
                        ..
                    },
            } => Some(PreReadyObservation::RejectedIdentity {
                participation: *participation,
                operation: *operation_id,
                kind: OperationKind::UpdateParticipation,
            }),
            TaskExecutionError::OperationFailed {
                participation,
                operation_id,
                kind: OperationKind::CreateTask,
                rejection:
                    OperationResult::Verdict {
                        outcome: OperationOutcome::IdentityMismatch,
                        ..
                    },
            } => Some(PreReadyObservation::RejectedIdentity {
                participation: *participation,
                operation: *operation_id,
                kind: OperationKind::CreateTask,
            }),
            _ => None,
        }
    }
    fn record(
        &mut self,
        failure: AttemptFailure,
        window: Option<OriginalHorizon>,
    ) -> Result<bool, TaskExecutionError> {
        if self.selected.is_some() || self.preparing_terminal.is_some() {
            return Ok(false);
        }
        let (observation, allows_task) = match &failure {
            AttemptFailure::TaskProtocol(error) => {
                let observation = match error.as_ref() {
                    TaskExecutionError::PreReadyEstablishTransportUnknown { backend } => {
                        Some(PreReadyObservation::Unknown(*backend))
                    }
                    error => Self::refusal(error).filter(|observation| {
                        window.is_some_and(|window| {
                            window.observation == *observation && window.open()
                        })
                    }),
                };
                (
                    observation,
                    matches!(
                        error.as_ref(),
                        TaskExecutionError::ParticipantUnobservable {
                            state: ParticipantObservationFailure::ConnectionLost
                                | ParticipantObservationFailure::Transport(_),
                            ..
                        }
                    ),
                )
            }
            AttemptFailure::EstablishReplayExhausted {
                original,
                participation,
                ..
            } => {
                if original.kind != HostFailureKind::EstablishAuthorization {
                    return Err(TaskExecutionError::Schedule(
                        "exhausted Establish fact lacks its original authorization author".into(),
                    ));
                }
                // The separate operation binding comes from the canonical
                // request. Preserve original.site unchanged; it may describe
                // the logical actor instead of the Native operation.
                (
                    Some(PreReadyObservation::Unknown(
                        participation.backend_process_id(),
                    )),
                    false,
                )
            }
            _ => (None, false),
        };
        let replace = self.pending.is_none()
            || (self.observation().is_some_and(PreReadyObservation::unknown)
                && !observation.is_some_and(PreReadyObservation::unknown));
        if !replace {
            return Ok(false);
        }
        self.phase = match observation {
            Some(PreReadyObservation::Unknown(_)) => ObservationPhase::Process,
            Some(PreReadyObservation::RejectedIdentity { .. }) => ObservationPhase::Permission,
            None => ObservationPhase::Dormant,
        };
        self.decision_turn = None;
        self.permission_watch = None;
        self.qualification_changed = false;
        self.pending = Some(Pending {
            failure,
            observation,
            allows_task,
        });
        Ok(true)
    }
    fn horizon(
        &self,
        view: Option<OriginalHorizon>,
    ) -> Result<Option<OriginalHorizon>, TaskExecutionError> {
        match self.observation() {
            None => Ok(None),
            Some(observation) => match view {
                Some(view) if view.observation == observation => Ok(Some(view)),
                _ => Err(TaskExecutionError::Schedule(
                    "pre-ready decision lacks its same original operation horizon".into(),
                )),
            },
        }
    }
    fn close_observation(&mut self) {
        if let Some(pending) = self.pending.as_mut() {
            pending.observation = None;
        }
        self.phase = ObservationPhase::Dormant;
        self.permission_watch = None;
        self.qualification_changed = false;
    }
    fn publish(&mut self, terminal: AttemptTerminal<AttemptFailure>) -> TerminalDirective {
        self.selected = Some(terminal.clone());
        TerminalDirective::Selected(terminal)
    }
    fn finish(
        &mut self,
        terminal: AttemptTerminal<AttemptFailure>,
        gate: GatePreparation,
        force: bool,
    ) -> TerminalDirective {
        if force && !matches!(gate, GatePreparation::Applied { .. }) {
            self.preparing_terminal = Some(terminal.clone());
        }
        match gate {
            GatePreparation::Open => TerminalDirective::PrepareConvergence,
            GatePreparation::Applying => TerminalDirective::Wait,
            GatePreparation::Applied { ready } if ready || force => self.publish(terminal),
            _ => TerminalDirective::Wait,
        }
    }
    fn stop_terminal(&self) -> AttemptTerminal<AttemptFailure> {
        let stop = self.stop.as_ref().expect("original latched stop");
        // Original cancellation/logical failure remains typed; reason is used
        // only for control stand-down, never to reconstruct the failure.
        AttemptTerminal::Failed(stop.failure.clone())
    }
    pub(crate) fn poll(
        &mut self,
        now: MonotonicInstant,
        facts: TerminalFacts<'_>,
        request: Option<AttemptRequestToken>,
    ) -> Result<TerminalDirective, TaskExecutionError> {
        if self.selected.is_some() {
            return Ok(TerminalDirective::Wait);
        }
        if let Some(terminal) = self.preparing_terminal.as_ref() {
            let terminal = terminal.clone();
            return Ok(match facts.gate {
                GatePreparation::Open => TerminalDirective::PrepareConvergence,
                GatePreparation::Applying => TerminalDirective::Wait,
                GatePreparation::Applied { .. } => {
                    self.preparing_terminal = None;
                    self.publish(terminal)
                }
            });
        }
        let original_decision_turn = self.decision_turn.is_some();
        if let Some(observation) = self.decision_turn {
            if self
                .permission
                .is_some_and(|flight| flight.observation == observation)
                || self
                    .sample
                    .is_some_and(|flight| flight.observation == observation)
            {
                return Ok(TerminalDirective::Wait);
            }
            if matches!(self.phase, ObservationPhase::Dormant) {
                return Ok(self.finish(
                    AttemptTerminal::Failed(self.pending.as_ref().unwrap().failure.clone()),
                    facts.gate,
                    true,
                ));
            }
            if matches!(self.phase, ObservationPhase::Waiting { .. }) {
                // Preserve the old post-read cancellation check. Gate-ready
                // late cancellation uses its original cause; otherwise the
                // immediately-ready waiter retains a definitive rejection.
                if self.stop.is_some() {
                    match facts.gate {
                        GatePreparation::Open => return Ok(TerminalDirective::PrepareConvergence),
                        GatePreparation::Applying => return Ok(TerminalDirective::Wait),
                        GatePreparation::Applied { ready: true } => {
                            self.decision_turn = None;
                            return Ok(self.finish(self.stop_terminal(), facts.gate, false));
                        }
                        GatePreparation::Applied { ready: false } => {
                            self.decision_turn = None;
                            let terminal = if observation.unknown() {
                                self.stop_terminal()
                            } else {
                                AttemptTerminal::Failed(
                                    self.pending.as_ref().unwrap().failure.clone(),
                                )
                            };
                            return Ok(self.finish(terminal, facts.gate, true));
                        }
                    }
                }
                self.decision_turn = None;
                return Ok(TerminalDirective::Wait);
            }
        }
        if !original_decision_turn && facts.success_ready {
            if let Some(pending) = self.pending.take()
                && self.residual.is_none()
            {
                self.residual = Some(pending.failure);
            }
            self.phase = ObservationPhase::Dormant;
            return Ok(self.finish(AttemptTerminal::Completed, facts.gate, false));
        }
        let horizon = self.horizon(facts.horizon)?;
        // The original cancellation wake runs without a new round: do not
        // memoize an earlier Task candidate merely because it waited on gate.
        if !original_decision_turn
            && facts.wake == TerminalWake::Cancellation
            && self.stop.is_some()
            && (self.observation().is_some()
                || matches!(facts.gate, GatePreparation::Applied { ready: true }))
        {
            let terminal = if matches!(
                self.observation(),
                Some(PreReadyObservation::RejectedIdentity { .. })
            ) {
                AttemptTerminal::Failed(self.pending.as_ref().unwrap().failure.clone())
            } else {
                self.stop_terminal()
            };
            return Ok(self.finish(terminal, facts.gate, self.observation().is_some()));
        }
        let may_select_task = self
            .pending
            .as_ref()
            .is_none_or(|pending| pending.allows_task)
            || self.observation().is_some_and(PreReadyObservation::unknown);
        if !original_decision_turn
            && may_select_task
            && let Some(task) = facts
                .selected_task
                .filter(|selected| !selected.detail().is_derived())
        {
            let force = self.observation().is_some_and(PreReadyObservation::unknown)
                || self
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.allows_task);
            let result = self.finish(
                AttemptTerminal::Failed(AttemptFailure::SelectedTask(task.clone())),
                facts.gate,
                force,
            );
            if force || !matches!(result, TerminalDirective::Wait) {
                return Ok(result);
            }
        }
        if let (Some(observation), ObservationPhase::Waiting { expires_at }) =
            (self.observation(), self.phase)
        {
            let window = horizon.expect("checked original horizon");
            if facts.wake != TerminalWake::Round
                && !now.has_reached(expires_at)
                && !self.qualification_changed
                && window.open()
                && self.stop.is_none()
            {
                return Ok(TerminalDirective::Wait);
            }
            self.qualification_changed = false;
            self.phase = if observation.unknown() {
                ObservationPhase::Process
            } else {
                ObservationPhase::Permission
            };
        }
        // In a receive turn QA precedes early cancellation and horizon checks.
        if let Some(observation @ PreReadyObservation::RejectedIdentity { .. }) = self.observation()
            && matches!(self.phase, ObservationPhase::Permission)
            && self.permission.is_none()
        {
            self.decision_turn = Some(observation);
            let Some(request) = request else {
                return Ok(TerminalDirective::NeedRequestCredit);
            };
            self.permission = Some(Flight {
                request,
                issued_at: now,
                observation,
            });
            self.permission_watch = self.permission;
            return Ok(TerminalDirective::QueryPermission {
                request,
                refusal: self.pending.as_ref().unwrap().failure.clone(),
            });
        }
        if self.observation().is_some() && self.stop.is_some() {
            let terminal = if matches!(
                self.observation(),
                Some(PreReadyObservation::RejectedIdentity { .. })
            ) {
                AttemptTerminal::Failed(self.pending.as_ref().unwrap().failure.clone())
            } else {
                self.stop_terminal()
            };
            return Ok(self.finish(terminal, facts.gate, true));
        }
        if let Some(observation) = self.observation() {
            let window = horizon.expect("checked original horizon");
            if matches!(observation, PreReadyObservation::RejectedIdentity { .. }) && !window.open()
            {
                self.close_observation();
            } else {
                match self.phase {
                    ObservationPhase::Permission if self.permission.is_none() => {
                        self.decision_turn = Some(observation);
                        let Some(request) = request else {
                            return Ok(TerminalDirective::NeedRequestCredit);
                        };
                        self.permission = Some(Flight {
                            request,
                            issued_at: now,
                            observation,
                        });
                        self.permission_watch = self.permission;
                        return Ok(TerminalDirective::QueryPermission {
                            request,
                            refusal: self.pending.as_ref().unwrap().failure.clone(),
                        });
                    }
                    ObservationPhase::Process if self.sample.is_none() => {
                        self.decision_turn = Some(observation);
                        let Some(request) = request else {
                            return Ok(TerminalDirective::NeedRequestCredit);
                        };
                        self.sample = Some(Flight {
                            request,
                            issued_at: now,
                            observation,
                        });
                        return Ok(TerminalDirective::ObserveProcess {
                            request,
                            original: observation.backend(),
                        });
                    }
                    _ => return Ok(TerminalDirective::Wait),
                }
            }
        }
        if let Some(pending) = self.pending.as_ref() {
            return Ok(self.finish(
                AttemptTerminal::Failed(pending.failure.clone()),
                facts.gate,
                true,
            ));
        }
        if self.stop.is_some() {
            return Ok(self.finish(self.stop_terminal(), facts.gate, false));
        }
        Ok(TerminalDirective::Wait)
    }
    pub(crate) fn permission_changed(&mut self, request: AttemptRequestToken) -> ReplyDisposition {
        if self.selected.is_some()
            || self.preparing_terminal.is_some()
            || !self.permission_watch.is_some_and(|flight| {
                flight.request == request && self.observation() == Some(flight.observation)
            })
        {
            return ReplyDisposition::Stale;
        }
        self.qualification_changed = true;
        ReplyDisposition::Applied
    }
    pub(crate) fn permission_reply(
        &mut self,
        request: AttemptRequestToken,
        allowed: bool,
    ) -> ReplyDisposition {
        let Some(flight) = self.permission else {
            return ReplyDisposition::Stale;
        };
        if flight.request != request {
            return ReplyDisposition::Stale;
        }
        self.permission = None;
        if self.selected.is_some()
            || self.preparing_terminal.is_some()
            || self.observation() != Some(flight.observation)
        {
            return ReplyDisposition::Stale;
        }
        if allowed {
            self.phase = ObservationPhase::Process;
        } else {
            self.close_observation();
        }
        ReplyDisposition::Applied
    }
    pub(crate) fn process_reply(
        &mut self,
        now: MonotonicInstant,
        request: AttemptRequestToken,
        original: BackendProcessId,
        sampled_at: MonotonicInstant,
        observation: MembershipObservation,
        window: OriginalHorizon,
    ) -> Result<ReplyDisposition, TaskExecutionError> {
        let Some(flight) = self.sample else {
            return Ok(ReplyDisposition::Stale);
        };
        if flight.request != request {
            return Ok(ReplyDisposition::Stale);
        }
        if flight.observation.backend() != original
            || sampled_at < flight.issued_at
            || sampled_at > now
            || window.observation != flight.observation
        {
            return Err(TaskExecutionError::Schedule(
                "pre-ready process sample differs from its original request/timeline".into(),
            ));
        }
        self.sample = None;
        if self.selected.is_some()
            || self.preparing_terminal.is_some()
            || self.observation() != Some(flight.observation)
        {
            return Ok(ReplyDisposition::Stale);
        }
        let replacement = match observation {
            MembershipObservation::ExactReplacement(replacement) if replacement != original => {
                Some(replacement)
            }
            _ => None,
        };
        match flight.observation {
            PreReadyObservation::Unknown(_) => {
                if let Some(replacement) = replacement {
                    self.pending.as_mut().unwrap().failure = AttemptFailure::BackendReplaced {
                        original,
                        replacement,
                    };
                    self.close_observation();
                } else if !window.open() {
                    self.close_observation();
                } else {
                    self.phase = ObservationPhase::Waiting {
                        expires_at: now
                            .saturating_add(window.remaining.expect("open original horizon")),
                    };
                }
            }
            PreReadyObservation::RejectedIdentity {
                participation,
                operation,
                kind,
            } => {
                if !window.open() {
                    self.close_observation();
                } else if let Some(replacement) = replacement {
                    let pending = self.pending.as_mut().unwrap();
                    pending.failure = AttemptFailure::PreReadyReplacement {
                        refusal: Box::new(pending.failure.clone()),
                        participation,
                        operation_id: operation,
                        kind,
                        replacement,
                    };
                    self.close_observation();
                } else {
                    self.phase = ObservationPhase::Waiting {
                        expires_at: now
                            .saturating_add(window.remaining.expect("open original horizon")),
                    };
                }
            }
        }
        Ok(ReplyDisposition::Applied)
    }
    pub(crate) fn next_deadline(&self) -> Option<MonotonicInstant> {
        if self.selected.is_some() || self.preparing_terminal.is_some() {
            return None;
        }
        match self.phase {
            ObservationPhase::Waiting { expires_at } => Some(expires_at),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        failure::{AttemptFailureSite, HostFailureRef, HostProjectedFailure},
        ids::{IdSeed, OperationIds},
    };
    use novarocks_coordination_contract::AttemptFailureClass;
    use novarocks_execution_contract::{AbortCause, TerminationDetail, operation::OperationResult};
    use std::num::NonZeroU64;
    fn now(ms: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_millis(ms))
    }
    fn token(n: u64) -> AttemptRequestToken {
        AttemptRequestToken::new(NonZeroU64::new(n).unwrap())
    }
    fn backend(n: u64) -> BackendProcessId {
        crate::test_support::backend(n)
    }
    fn operation(n: u128) -> TaskOperationId {
        OperationIds::new(IdSeed::new(n, 1)).issue()
    }
    fn part() -> ParticipationRef {
        crate::test_support::remote(1).participation()
    }
    fn host(n: u64) -> AttemptFailure {
        AttemptFailure::Host(HostProjectedFailure {
            original: HostFailureRef(NonZeroU64::new(n).unwrap()),
            kind: HostFailureKind::Execution,
            class: AttemptFailureClass::ExecutionFailure,
            site: AttemptFailureSite::Attempt,
        })
    }
    fn task(peer: bool) -> SelectedTaskTermination {
        SelectedTaskTermination::new(
            crate::test_support::remote(2).identity(),
            TerminationDetail::Aborted(if peer {
                AbortCause::PeerTaskFailed
            } else {
                AbortCause::QueryFailed
            }),
        )
    }
    fn unknown(n: u64) -> AttemptFailure {
        AttemptFailure::TaskProtocol(Box::new(
            TaskExecutionError::PreReadyEstablishTransportUnknown {
                backend: backend(n),
            },
        ))
    }
    fn refusal(create: bool) -> AttemptFailure {
        let rejection = OperationResult::Verdict {
            outcome: OperationOutcome::IdentityMismatch,
            detail: None,
            acknowledgement: None,
        };
        AttemptFailure::TaskProtocol(Box::new(if create {
            TaskExecutionError::OperationFailed {
                participation: part(),
                operation_id: operation(10),
                kind: OperationKind::CreateTask,
                rejection,
            }
        } else {
            TaskExecutionError::PreReadyEstablishRejected {
                participation: part(),
                operation_id: operation(10),
                rejection,
            }
        }))
    }
    fn window(observation: PreReadyObservation, remaining: Option<u64>) -> OriginalHorizon {
        OriginalHorizon {
            observation,
            remaining: remaining.map(Duration::from_millis),
        }
    }
    fn current_window(
        controller: &TerminalController,
        remaining: Option<u64>,
    ) -> Option<OriginalHorizon> {
        controller
            .observation()
            .map(|observation| window(observation, remaining))
    }
    fn facts<'a>(
        controller: &TerminalController,
        success: bool,
        task: Option<&'a SelectedTaskTermination>,
        gate: GatePreparation,
        remaining: Option<u64>,
        wake: TerminalWake,
    ) -> TerminalFacts<'a> {
        TerminalFacts {
            success_ready: success,
            selected_task: task,
            gate,
            horizon: current_window(controller, remaining),
            wake,
        }
    }
    fn poll(
        controller: &mut TerminalController,
        at: u64,
        success: bool,
        selected: Option<&SelectedTaskTermination>,
        gate: GatePreparation,
        remaining: Option<u64>,
        wake: TerminalWake,
        request: Option<u64>,
    ) -> TerminalDirective {
        let facts = facts(controller, success, selected, gate, remaining, wake);
        controller.poll(now(at), facts, request.map(token)).unwrap()
    }
    fn record_refusal(
        controller: &mut TerminalController,
        create: bool,
        remaining: u64,
    ) -> AttemptFailure {
        let failure = refusal(create);
        let AttemptFailure::TaskProtocol(error) = &failure else {
            unreachable!()
        };
        let observation = TerminalController::refusal(error).unwrap();
        assert!(
            controller
                .record(failure.clone(), Some(window(observation, Some(remaining))))
                .unwrap()
        );
        failure
    }
    fn selected_failure(directive: TerminalDirective) -> AttemptFailure {
        match directive {
            TerminalDirective::Selected(AttemptTerminal::Failed(failure)) => failure,
            other => panic!("expected original failure, got {other:?}"),
        }
    }
    const READY: GatePreparation = GatePreparation::Applied { ready: true };
    const NOT_READY: GatePreparation = GatePreparation::Applied { ready: false };

    #[test]
    fn accepted_root_and_normal_drain_win_before_failure_task_cancel_but_wait_for_real_gate_ready()
    {
        let mut controller = TerminalController::new();
        let pending = host(1);
        let selected = task(false);
        controller.record_original_failure(pending.clone()).unwrap();
        controller.observe_stop(AttemptStopReason::Client, host(2));
        assert!(matches!(
            poll(
                &mut controller,
                0,
                true,
                Some(&selected),
                GatePreparation::Open,
                None,
                TerminalWake::Round,
                None
            ),
            TerminalDirective::PrepareConvergence
        ));
        assert_eq!(controller.residual(), Some(&pending));
        assert!(matches!(
            poll(
                &mut controller,
                1,
                true,
                Some(&selected),
                NOT_READY,
                None,
                TerminalWake::Cancellation,
                None
            ),
            TerminalDirective::Wait
        ));
        assert!(controller.terminal().is_none());
        assert!(matches!(
            poll(
                &mut controller,
                2,
                true,
                Some(&selected),
                READY,
                None,
                TerminalWake::Round,
                None
            ),
            TerminalDirective::Selected(AttemptTerminal::Completed)
        ));
        controller.observe_stop(AttemptStopReason::LogicalExecutionFailed, host(3));
        assert!(controller.requires_logical_failure_stand_down());
        assert!(matches!(
            controller.terminal(),
            Some(AttemptTerminal::Completed)
        ));
        assert!(!controller.record_original_failure(host(4)).unwrap());
    }
    #[test]
    fn sealed_root_without_completed_strict_tail_does_not_mask_original_failure() {
        let mut controller = TerminalController::new();
        controller.record_original_failure(host(1)).unwrap();
        assert_eq!(
            selected_failure(poll(
                &mut controller,
                0,
                false,
                None,
                NOT_READY,
                None,
                TerminalWake::Round,
                None
            )),
            host(1)
        );
    }
    #[test]
    fn forced_failure_still_requires_actual_gate_preparation_before_publication() {
        let mut controller = TerminalController::new();
        controller.record_original_failure(host(1)).unwrap();
        assert!(matches!(
            poll(
                &mut controller,
                0,
                false,
                None,
                GatePreparation::Open,
                None,
                TerminalWake::Round,
                None
            ),
            TerminalDirective::PrepareConvergence
        ));
        assert!(matches!(
            poll(
                &mut controller,
                1,
                false,
                None,
                GatePreparation::Applying,
                None,
                TerminalWake::Continuation,
                None
            ),
            TerminalDirective::Wait
        ));
        assert!(controller.terminal().is_none());
        assert_eq!(
            selected_failure(poll(
                &mut controller,
                2,
                false,
                None,
                NOT_READY,
                None,
                TerminalWake::Continuation,
                None
            )),
            host(1)
        );
    }
    #[test]
    fn only_typed_ordinary_observation_loss_yields_to_original_task_cause_even_when_gate_not_ready()
    {
        for state in [
            ParticipantObservationFailure::ConnectionLost,
            ParticipantObservationFailure::Transport("identity violation"),
        ] {
            let mut controller = TerminalController::new();
            let selected = task(false);
            controller
                .record_original_failure(AttemptFailure::TaskProtocol(Box::new(
                    TaskExecutionError::ParticipantUnobservable {
                        backend: backend(1),
                        state,
                    },
                )))
                .unwrap();
            assert_eq!(
                selected_failure(poll(
                    &mut controller,
                    0,
                    false,
                    Some(&selected),
                    NOT_READY,
                    None,
                    TerminalWake::Round,
                    None
                )),
                AttemptFailure::SelectedTask(selected)
            );
        }
        let mut controller = TerminalController::new();
        let selected = task(false);
        let original =
            AttemptFailure::TaskProtocol(Box::new(TaskExecutionError::ParticipantUnobservable {
                backend: backend(1),
                state: ParticipantObservationFailure::IdentityViolation("connection_lost"),
            }));
        controller
            .record_original_failure(original.clone())
            .unwrap();
        assert_eq!(
            selected_failure(poll(
                &mut controller,
                0,
                false,
                Some(&selected),
                NOT_READY,
                None,
                TerminalWake::Round,
                None
            )),
            original
        );
    }
    #[test]
    fn selected_task_is_memoized_but_unselected_task_waiting_for_gate_does_not_override_cancel_wake()
     {
        let selected = task(false);
        let mut controller = TerminalController::new();
        assert!(matches!(
            poll(
                &mut controller,
                0,
                false,
                Some(&selected),
                NOT_READY,
                None,
                TerminalWake::Round,
                None
            ),
            TerminalDirective::Wait
        ));
        assert!(controller.terminal().is_none());
        controller.observe_stop(AttemptStopReason::Deadline, host(9));
        assert_eq!(
            selected_failure(poll(
                &mut controller,
                1,
                false,
                Some(&selected),
                READY,
                None,
                TerminalWake::Cancellation,
                None
            )),
            host(9)
        );
        let mut first = TerminalController::new();
        assert_eq!(
            selected_failure(poll(
                &mut first,
                0,
                false,
                Some(&selected),
                READY,
                None,
                TerminalWake::Round,
                None
            )),
            AttemptFailure::SelectedTask(selected.clone())
        );
        first.record_original_failure(host(1)).unwrap();
        assert_eq!(
            first.terminal(),
            Some(&AttemptTerminal::Failed(AttemptFailure::SelectedTask(
                selected
            )))
        );
    }
    #[test]
    fn peer_placeholder_is_not_selected_as_an_originating_task_failure() {
        let mut controller = TerminalController::new();
        let peer = task(true);
        assert!(matches!(
            poll(
                &mut controller,
                0,
                false,
                Some(&peer),
                READY,
                None,
                TerminalWake::Round,
                None
            ),
            TerminalDirective::Wait
        ));
        assert!(controller.terminal().is_none());
    }
    #[test]
    fn unknown_refines_only_once_to_definite_original_failure_and_duplicate_unknown_cannot_replace_it()
     {
        let mut controller = TerminalController::new();
        controller.record_original_failure(unknown(1)).unwrap();
        assert!(!controller.record_original_failure(unknown(2)).unwrap());
        assert_eq!(
            controller.observation(),
            Some(PreReadyObservation::Unknown(backend(1)))
        );
        assert!(controller.record_original_failure(host(3)).unwrap());
        assert!(!controller.record_original_failure(host(4)).unwrap());
        assert!(!controller.record_original_failure(unknown(1)).unwrap());
        assert_eq!(
            selected_failure(poll(
                &mut controller,
                0,
                false,
                None,
                NOT_READY,
                None,
                TerminalWake::Round,
                None
            )),
            host(3)
        );
    }
    #[test]
    fn unknown_positive_replacement_precedes_expired_or_absent_horizon_while_unobserved_never_guesses_loss()
     {
        for remaining in [None, Some(0)] {
            let mut controller = TerminalController::new();
            controller.record_original_failure(unknown(1)).unwrap();
            assert!(
                matches!(poll(&mut controller,10,false,None,NOT_READY,remaining,TerminalWake::Round,Some(1)),TerminalDirective::ObserveProcess {original,..} if original==backend(1))
            );
            controller
                .process_reply(
                    now(11),
                    token(1),
                    backend(1),
                    now(10),
                    MembershipObservation::ExactReplacement(backend(2)),
                    current_window(&controller, remaining).unwrap(),
                )
                .unwrap();
            assert_eq!(
                selected_failure(poll(
                    &mut controller,
                    11,
                    false,
                    None,
                    NOT_READY,
                    None,
                    TerminalWake::Continuation,
                    None
                )),
                AttemptFailure::BackendReplaced {
                    original: backend(1),
                    replacement: backend(2)
                }
            );
            let mut unknown_controller = TerminalController::new();
            unknown_controller
                .record_original_failure(unknown(1))
                .unwrap();
            poll(
                &mut unknown_controller,
                10,
                false,
                None,
                NOT_READY,
                remaining,
                TerminalWake::Round,
                Some(1),
            );
            unknown_controller
                .process_reply(
                    now(11),
                    token(1),
                    backend(1),
                    now(10),
                    MembershipObservation::Unobserved,
                    current_window(&unknown_controller, remaining).unwrap(),
                )
                .unwrap();
            assert_eq!(
                selected_failure(poll(
                    &mut unknown_controller,
                    11,
                    false,
                    None,
                    NOT_READY,
                    None,
                    TerminalWake::Continuation,
                    None
                )),
                unknown(1)
            );
        }
    }
    #[test]
    fn exact_rejection_requires_qa_before_any_correlated_process_sample() {
        for create in [false, true] {
            let mut controller = TerminalController::new();
            let original = record_refusal(&mut controller, create, 100);
            assert!(
                matches!(poll(&mut controller,0,false,None,GatePreparation::Open,Some(100),TerminalWake::Round,Some(1)),TerminalDirective::QueryPermission {request,refusal} if request==token(1) && refusal==original)
            );
            assert!(controller.sample.is_none());
            assert_eq!(
                controller.permission_reply(token(99), true),
                ReplyDisposition::Stale
            );
            assert!(matches!(
                poll(
                    &mut controller,
                    1,
                    false,
                    None,
                    NOT_READY,
                    Some(99),
                    TerminalWake::Continuation,
                    Some(2)
                ),
                TerminalDirective::Wait
            ));
            assert_eq!(
                controller.permission_reply(token(1), true),
                ReplyDisposition::Applied
            );
            assert!(matches!(
                poll(
                    &mut controller,
                    2,
                    false,
                    None,
                    NOT_READY,
                    Some(98),
                    TerminalWake::Continuation,
                    None
                ),
                TerminalDirective::NeedRequestCredit
            ));
            assert!(
                matches!(poll(&mut controller,2,false,None,NOT_READY,Some(98),TerminalWake::Continuation,Some(2)),TerminalDirective::ObserveProcess {request,..} if request==token(2))
            );
            controller
                .process_reply(
                    now(3),
                    token(2),
                    backend(1),
                    now(2),
                    MembershipObservation::ExactReplacement(backend(2)),
                    current_window(&controller, Some(97)).unwrap(),
                )
                .unwrap();
            let failure = selected_failure(poll(
                &mut controller,
                3,
                false,
                None,
                NOT_READY,
                None,
                TerminalWake::Continuation,
                None,
            ));
            assert!(
                matches!(failure,AttemptFailure::PreReadyReplacement {refusal,participation,operation_id,kind,replacement} if *refusal==original && participation==part() && operation_id==operation(10) && kind==if create {OperationKind::CreateTask}else{OperationKind::UpdateParticipation} && replacement==backend(2))
            );
        }
    }
    #[test]
    fn denied_qa_preserves_definitive_failure_and_does_not_open_process_observation() {
        let mut controller = TerminalController::new();
        let original = record_refusal(&mut controller, true, 100);
        poll(
            &mut controller,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Round,
            Some(1),
        );
        assert_eq!(
            controller.permission_reply(token(1), false),
            ReplyDisposition::Applied
        );
        assert!(controller.sample.is_none());
        assert_eq!(
            selected_failure(poll(
                &mut controller,
                1,
                false,
                None,
                NOT_READY,
                None,
                TerminalWake::Continuation,
                None
            )),
            original
        );
    }
    #[test]
    fn original_rejection_horizon_is_checked_before_and_after_process_sample() {
        let mut before = TerminalController::new();
        let original = record_refusal(&mut before, true, 100);
        poll(
            &mut before,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Round,
            Some(1),
        );
        before.permission_reply(token(1), true);
        assert_eq!(
            selected_failure(poll(
                &mut before,
                100,
                false,
                None,
                NOT_READY,
                Some(0),
                TerminalWake::Continuation,
                None
            )),
            original
        );
        assert!(before.sample.is_none());
        let mut after = TerminalController::new();
        let original = record_refusal(&mut after, true, 100);
        poll(
            &mut after,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Round,
            Some(1),
        );
        after.permission_reply(token(1), true);
        poll(
            &mut after,
            99,
            false,
            None,
            NOT_READY,
            Some(1),
            TerminalWake::Continuation,
            Some(2),
        );
        after
            .process_reply(
                now(100),
                token(2),
                backend(1),
                now(99),
                MembershipObservation::ExactReplacement(backend(2)),
                current_window(&after, Some(0)).unwrap(),
            )
            .unwrap();
        assert_eq!(
            selected_failure(poll(
                &mut after,
                100,
                false,
                None,
                NOT_READY,
                None,
                TerminalWake::Continuation,
                None
            )),
            original
        );
    }
    #[test]
    fn rejection_waits_for_real_change_or_original_horizon_without_periodic_qa_or_process_polling()
    {
        let mut controller = TerminalController::new();
        record_refusal(&mut controller, true, 100);
        poll(
            &mut controller,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Round,
            Some(1),
        );
        assert_eq!(controller.next_deadline(), None);
        controller.permission_reply(token(1), true);
        poll(
            &mut controller,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Continuation,
            Some(2),
        );
        assert_eq!(controller.next_deadline(), None);
        controller
            .process_reply(
                now(1),
                token(2),
                backend(1),
                now(1),
                MembershipObservation::ExactOriginal,
                current_window(&controller, Some(99)).unwrap(),
            )
            .unwrap();
        assert_eq!(controller.next_deadline(), Some(now(100)));
        assert!(matches!(
            poll(
                &mut controller,
                2,
                false,
                None,
                NOT_READY,
                Some(98),
                TerminalWake::Continuation,
                Some(3)
            ),
            TerminalDirective::Wait
        ));
        for at in [6, 11, 31, 99] {
            assert!(matches!(
                poll(
                    &mut controller,
                    at,
                    false,
                    None,
                    NOT_READY,
                    Some(100 - at),
                    TerminalWake::Continuation,
                    Some(3)
                ),
                TerminalDirective::Wait
            ));
            assert_eq!(controller.next_deadline(), Some(now(100)));
        }
        assert!(
            matches!(poll(&mut controller,99,false,None,NOT_READY,Some(1),TerminalWake::Round,Some(3)),TerminalDirective::QueryPermission {request,..} if request==token(3))
        );
        assert_eq!(controller.next_deadline(), None);
    }
    #[test]
    fn real_qa_change_hint_reasks_original_policy_without_granting_permission_or_periodic_wakeup() {
        let mut controller = TerminalController::new();
        let original = record_refusal(&mut controller, true, 100);
        poll(
            &mut controller,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Round,
            Some(1),
        );
        controller.permission_reply(token(1), true);
        poll(
            &mut controller,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Continuation,
            Some(2),
        );
        controller
            .process_reply(
                now(1),
                token(2),
                backend(1),
                now(1),
                MembershipObservation::ExactOriginal,
                current_window(&controller, Some(99)).unwrap(),
            )
            .unwrap();
        poll(
            &mut controller,
            1,
            false,
            None,
            NOT_READY,
            Some(99),
            TerminalWake::Continuation,
            None,
        );
        assert_eq!(
            controller.permission_changed(token(99)),
            ReplyDisposition::Stale
        );
        assert!(matches!(
            poll(
                &mut controller,
                6,
                false,
                None,
                NOT_READY,
                Some(94),
                TerminalWake::Continuation,
                None
            ),
            TerminalDirective::Wait
        ));
        assert_eq!(
            controller.permission_changed(token(1)),
            ReplyDisposition::Applied
        );
        assert!(
            matches!(poll(&mut controller,6,false,None,NOT_READY,Some(94),TerminalWake::Continuation,Some(3)),TerminalDirective::QueryPermission { request,.. } if request==token(3))
        );
        // The hint is not a permission decision, process read or terminal.
        assert!(controller.sample.is_none());
        assert!(controller.terminal().is_none());
        assert_eq!(controller.next_deadline(), None);
        assert_eq!(
            controller.permission_changed(token(1)),
            ReplyDisposition::Stale
        );
        assert_eq!(
            controller.permission_reply(token(3), false),
            ReplyDisposition::Applied
        );
        assert_eq!(
            selected_failure(poll(
                &mut controller,
                7,
                false,
                None,
                NOT_READY,
                None,
                TerminalWake::Continuation,
                None
            )),
            original
        );
        assert_eq!(
            controller.permission_changed(token(3)),
            ReplyDisposition::Stale
        );
    }
    #[test]
    fn original_qa_change_before_boolean_reply_is_retained_after_current_process_turn_without_self_polling()
     {
        let mut controller = TerminalController::new();
        record_refusal(&mut controller, true, 100);
        poll(
            &mut controller,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Round,
            Some(1),
        );
        assert_eq!(
            controller.permission_changed(token(1)),
            ReplyDisposition::Applied
        );
        assert!(matches!(
            poll(
                &mut controller,
                1,
                false,
                None,
                NOT_READY,
                Some(99),
                TerminalWake::Continuation,
                Some(2)
            ),
            TerminalDirective::Wait
        ));
        controller.permission_reply(token(1), true);
        assert!(matches!(
            poll(
                &mut controller,
                1,
                false,
                None,
                NOT_READY,
                Some(99),
                TerminalWake::Continuation,
                Some(2)
            ),
            TerminalDirective::ObserveProcess { .. }
        ));
        controller
            .process_reply(
                now(2),
                token(2),
                backend(1),
                now(2),
                MembershipObservation::ExactOriginal,
                current_window(&controller, Some(98)).unwrap(),
            )
            .unwrap();
        assert!(matches!(
            poll(
                &mut controller,
                2,
                false,
                None,
                NOT_READY,
                Some(98),
                TerminalWake::Continuation,
                None
            ),
            TerminalDirective::Wait
        ));
        assert!(controller.permits_protocol_turn());
        assert!(
            matches!(poll(&mut controller,2,false,None,NOT_READY,Some(98),TerminalWake::Continuation,Some(3)),TerminalDirective::QueryPermission { request,.. } if request==token(3))
        );
        controller.permission_reply(token(3), true);
        poll(
            &mut controller,
            2,
            false,
            None,
            NOT_READY,
            Some(98),
            TerminalWake::Continuation,
            Some(4),
        );
        controller
            .process_reply(
                now(3),
                token(4),
                backend(1),
                now(3),
                MembershipObservation::ExactOriginal,
                current_window(&controller, Some(97)).unwrap(),
            )
            .unwrap();
        poll(
            &mut controller,
            3,
            false,
            None,
            NOT_READY,
            Some(97),
            TerminalWake::Continuation,
            None,
        );
        for at in [4, 8, 40, 99] {
            assert!(matches!(
                poll(
                    &mut controller,
                    at,
                    false,
                    None,
                    NOT_READY,
                    Some(100 - at),
                    TerminalWake::Continuation,
                    Some(5)
                ),
                TerminalDirective::Wait
            ));
            assert_eq!(controller.next_deadline(), Some(now(100)));
        }
    }
    #[test]
    fn forced_original_cut_survives_async_prepare_gap_but_never_claims_actual_exit() {
        for accepted_task in [false, true] {
            let mut controller = TerminalController::new();
            let original = if accepted_task { unknown(1) } else { host(8) };
            controller
                .record_original_failure(original.clone())
                .unwrap();
            let task = task(false);
            let selected = accepted_task.then_some(&task);
            assert!(matches!(
                poll(
                    &mut controller,
                    0,
                    false,
                    selected,
                    GatePreparation::Open,
                    Some(100),
                    TerminalWake::Round,
                    None
                ),
                TerminalDirective::PrepareConvergence
            ));
            assert!(controller.terminal().is_none());
            controller.observe_stop(AttemptStopReason::Client, host(9));
            assert!(matches!(
                poll(
                    &mut controller,
                    1,
                    false,
                    None,
                    GatePreparation::Applying,
                    Some(99),
                    TerminalWake::Cancellation,
                    None
                ),
                TerminalDirective::Wait
            ));
            assert_eq!(controller.stop_reason(), Some(AttemptStopReason::Client));
            assert!(!controller.record_original_failure(host(10)).unwrap());
            let failure = selected_failure(poll(
                &mut controller,
                2,
                true,
                None,
                NOT_READY,
                None,
                TerminalWake::Cancellation,
                None,
            ));
            assert_eq!(
                failure,
                if accepted_task {
                    AttemptFailure::SelectedTask(task)
                } else {
                    original
                }
            );
        }
    }
    #[test]
    fn original_turn_holds_across_request_credit_full_without_busy_deadline_or_later_seal() {
        for rejected in [false, true] {
            let mut controller = TerminalController::new();
            let original = if rejected {
                record_refusal(&mut controller, true, 100)
            } else {
                controller.record_original_failure(unknown(1)).unwrap();
                unknown(1)
            };
            assert!(matches!(
                poll(
                    &mut controller,
                    0,
                    false,
                    None,
                    NOT_READY,
                    Some(100),
                    TerminalWake::Round,
                    None
                ),
                TerminalDirective::NeedRequestCredit
            ));
            assert!(!controller.permits_protocol_turn());
            assert_eq!(controller.next_deadline(), None);
            assert!(matches!(
                poll(
                    &mut controller,
                    1,
                    true,
                    None,
                    READY,
                    Some(99),
                    TerminalWake::Continuation,
                    None
                ),
                TerminalDirective::NeedRequestCredit
            ));
            let directive = poll(
                &mut controller,
                1,
                true,
                None,
                READY,
                Some(99),
                TerminalWake::Continuation,
                Some(1),
            );
            if rejected {
                assert!(matches!(
                    directive,
                    TerminalDirective::QueryPermission { .. }
                ));
                controller.permission_reply(token(1), false);
            } else {
                assert!(matches!(
                    directive,
                    TerminalDirective::ObserveProcess { .. }
                ));
                controller
                    .process_reply(
                        now(100),
                        token(1),
                        backend(1),
                        now(100),
                        MembershipObservation::ExactOriginal,
                        current_window(&controller, Some(0)).unwrap(),
                    )
                    .unwrap();
            }
            assert_eq!(
                selected_failure(poll(
                    &mut controller,
                    100,
                    true,
                    None,
                    NOT_READY,
                    None,
                    TerminalWake::Continuation,
                    None
                )),
                original
            );
        }
    }
    #[test]
    fn original_qa_turn_blocks_later_seal_and_cancel_until_its_original_reply() {
        for allowed in [false, true] {
            let mut controller = TerminalController::new();
            let original = record_refusal(&mut controller, true, 100);
            controller.observe_stop(AttemptStopReason::Client, host(9));
            assert!(matches!(
                poll(
                    &mut controller,
                    0,
                    false,
                    None,
                    NOT_READY,
                    Some(100),
                    TerminalWake::Round,
                    Some(1)
                ),
                TerminalDirective::QueryPermission { .. }
            ));
            assert!(!controller.permits_protocol_turn());
            // A caller must not accept later Root/Seal facts across the await.
            assert!(matches!(
                poll(
                    &mut controller,
                    1,
                    true,
                    None,
                    READY,
                    Some(99),
                    TerminalWake::Cancellation,
                    None
                ),
                TerminalDirective::Wait
            ));
            assert_eq!(
                controller.permission_reply(token(1), allowed),
                ReplyDisposition::Applied
            );
            assert_eq!(
                selected_failure(poll(
                    &mut controller,
                    1,
                    true,
                    None,
                    NOT_READY,
                    Some(99),
                    TerminalWake::Continuation,
                    None
                )),
                original
            );
            assert!(controller.sample.is_none());
        }
    }
    #[test]
    fn process_turn_keeps_positive_cut_before_late_cancel_but_no_positive_runs_original_post_read_check()
     {
        for positive in [false, true] {
            for ready in [false, true] {
                let mut controller = TerminalController::new();
                let original = record_refusal(&mut controller, true, 100);
                poll(
                    &mut controller,
                    0,
                    false,
                    None,
                    NOT_READY,
                    Some(100),
                    TerminalWake::Round,
                    Some(1),
                );
                controller.permission_reply(token(1), true);
                assert!(matches!(
                    poll(
                        &mut controller,
                        0,
                        false,
                        None,
                        NOT_READY,
                        Some(100),
                        TerminalWake::Continuation,
                        Some(2)
                    ),
                    TerminalDirective::ObserveProcess { .. }
                ));
                controller.observe_stop(AttemptStopReason::Client, host(9));
                assert!(matches!(
                    poll(
                        &mut controller,
                        1,
                        true,
                        None,
                        READY,
                        Some(99),
                        TerminalWake::Cancellation,
                        None
                    ),
                    TerminalDirective::Wait
                ));
                controller
                    .process_reply(
                        now(1),
                        token(2),
                        backend(1),
                        now(1),
                        if positive {
                            MembershipObservation::ExactReplacement(backend(2))
                        } else {
                            MembershipObservation::ExactOriginal
                        },
                        current_window(&controller, Some(99)).unwrap(),
                    )
                    .unwrap();
                let selected = selected_failure(poll(
                    &mut controller,
                    1,
                    true,
                    None,
                    GatePreparation::Applied { ready },
                    Some(99),
                    TerminalWake::Continuation,
                    None,
                ));
                if positive {
                    assert!(
                        matches!(selected,AttemptFailure::PreReadyReplacement { refusal, replacement,.. } if *refusal==original && replacement==backend(2))
                    );
                } else {
                    assert_eq!(selected, if ready { host(9) } else { original });
                }
            }
        }
    }
    #[test]
    fn no_positive_process_turn_releases_short_barrier_for_later_legal_success() {
        let mut controller = TerminalController::new();
        controller.record_original_failure(unknown(1)).unwrap();
        poll(
            &mut controller,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Round,
            Some(1),
        );
        controller
            .process_reply(
                now(1),
                token(1),
                backend(1),
                now(1),
                MembershipObservation::ExactOriginal,
                current_window(&controller, Some(99)).unwrap(),
            )
            .unwrap();
        assert!(!controller.permits_protocol_turn());
        assert!(matches!(
            poll(
                &mut controller,
                1,
                true,
                None,
                READY,
                Some(99),
                TerminalWake::Continuation,
                None
            ),
            TerminalDirective::Wait
        ));
        assert!(controller.permits_protocol_turn());
        assert!(matches!(
            poll(
                &mut controller,
                2,
                true,
                None,
                READY,
                Some(98),
                TerminalWake::Round,
                None
            ),
            TerminalDirective::Selected(AttemptTerminal::Completed)
        ));
        assert_eq!(controller.residual(), Some(&unknown(1)));
    }
    #[test]
    fn forced_task_cut_during_existing_gate_application_cannot_fall_through_to_stop() {
        let mut controller = TerminalController::new();
        controller.record_original_failure(unknown(1)).unwrap();
        controller.observe_stop(AttemptStopReason::Client, host(9));
        let original = task(false);
        assert!(matches!(
            poll(
                &mut controller,
                0,
                false,
                Some(&original),
                GatePreparation::Applying,
                Some(100),
                TerminalWake::Round,
                None
            ),
            TerminalDirective::Wait
        ));
        assert_eq!(
            selected_failure(poll(
                &mut controller,
                1,
                true,
                None,
                NOT_READY,
                None,
                TerminalWake::Cancellation,
                None
            )),
            AttemptFailure::SelectedTask(original)
        );
    }
    #[test]
    fn original_horizon_wakes_waiting_without_expired_timer_spin() {
        for rejected in [false, true] {
            let mut controller = TerminalController::new();
            let original = if rejected {
                record_refusal(&mut controller, true, 2)
            } else {
                controller.record_original_failure(unknown(1)).unwrap();
                unknown(1)
            };
            let request = if rejected {
                assert!(matches!(
                    poll(
                        &mut controller,
                        0,
                        false,
                        None,
                        NOT_READY,
                        Some(2),
                        TerminalWake::Round,
                        Some(1)
                    ),
                    TerminalDirective::QueryPermission { .. }
                ));
                controller.permission_reply(token(1), true);
                2
            } else {
                1
            };
            assert!(matches!(
                poll(
                    &mut controller,
                    0,
                    false,
                    None,
                    NOT_READY,
                    Some(2),
                    TerminalWake::Continuation,
                    Some(request)
                ),
                TerminalDirective::ObserveProcess { .. }
            ));
            controller
                .process_reply(
                    now(1),
                    token(request),
                    backend(1),
                    now(1),
                    MembershipObservation::ExactOriginal,
                    current_window(&controller, Some(1)).unwrap(),
                )
                .unwrap();
            assert_eq!(controller.next_deadline(), Some(now(2)));
            assert!(!controller.permits_protocol_turn());
            assert!(matches!(
                poll(
                    &mut controller,
                    1,
                    false,
                    None,
                    NOT_READY,
                    Some(1),
                    TerminalWake::Continuation,
                    None
                ),
                TerminalDirective::Wait
            ));
            assert!(controller.permits_protocol_turn());
            let directive = poll(
                &mut controller,
                2,
                false,
                None,
                NOT_READY,
                Some(0),
                TerminalWake::Continuation,
                Some(3),
            );
            if rejected {
                assert!(
                    matches!(directive,TerminalDirective::QueryPermission { request,.. } if request==token(3))
                );
                controller.permission_reply(token(3), true);
                assert_eq!(
                    selected_failure(poll(
                        &mut controller,
                        2,
                        false,
                        None,
                        NOT_READY,
                        Some(0),
                        TerminalWake::Continuation,
                        None
                    )),
                    original
                );
            } else {
                // Unknown performs its last actual positive read before the
                // original expired horizon can select the retained failure.
                assert!(
                    matches!(directive,TerminalDirective::ObserveProcess { request,.. } if request==token(3))
                );
                assert_eq!(controller.next_deadline(), None);
                controller
                    .process_reply(
                        now(2),
                        token(3),
                        backend(1),
                        now(2),
                        MembershipObservation::ExactOriginal,
                        current_window(&controller, Some(0)).unwrap(),
                    )
                    .unwrap();
                assert_eq!(
                    selected_failure(poll(
                        &mut controller,
                        2,
                        false,
                        None,
                        NOT_READY,
                        None,
                        TerminalWake::Continuation,
                        None
                    )),
                    original
                );
            }
            assert_eq!(controller.next_deadline(), None);
        }
    }
    #[test]
    fn sample_predating_its_actual_request_cannot_reuse_cached_positive_replacement() {
        let mut controller = TerminalController::new();
        record_refusal(&mut controller, true, 100);
        poll(
            &mut controller,
            10,
            false,
            None,
            NOT_READY,
            Some(90),
            TerminalWake::Round,
            Some(1),
        );
        controller.permission_reply(token(1), true);
        poll(
            &mut controller,
            20,
            false,
            None,
            NOT_READY,
            Some(80),
            TerminalWake::Continuation,
            Some(2),
        );
        assert!(
            controller
                .process_reply(
                    now(21),
                    token(2),
                    backend(1),
                    now(19),
                    MembershipObservation::ExactReplacement(backend(2)),
                    current_window(&controller, Some(79)).unwrap()
                )
                .is_err()
        );
        assert!(controller.sample.is_some());
        assert!(matches!(
            controller.pending.as_ref().unwrap().failure,
            AttemptFailure::TaskProtocol(_)
        ));
        controller
            .process_reply(
                now(21),
                token(2),
                backend(1),
                now(20),
                MembershipObservation::ExactOriginal,
                current_window(&controller, Some(79)).unwrap(),
            )
            .unwrap();
        assert_eq!(controller.next_deadline(), Some(now(100)));
    }
    #[test]
    fn cancellation_preserves_rejection_but_unknown_uses_original_typed_stop_even_when_gate_not_ready()
     {
        for rejected in [false, true] {
            let mut controller = TerminalController::new();
            let original = if rejected {
                record_refusal(&mut controller, true, 100)
            } else {
                controller.record_original_failure(unknown(1)).unwrap();
                unknown(1)
            };
            let stop = host(25);
            controller.observe_stop(AttemptStopReason::DrainDeadline, stop.clone());
            let selected = selected_failure(poll(
                &mut controller,
                0,
                false,
                None,
                NOT_READY,
                Some(100),
                TerminalWake::Cancellation,
                None,
            ));
            assert_eq!(selected, if rejected { original } else { stop });
        }
    }
    #[test]
    fn unrelated_host_kind_class_and_text_never_create_a_pre_ready_unknown_policy() {
        let mut controller = TerminalController::new();
        let original = HostProjectedFailure {
            original: HostFailureRef(NonZeroU64::new(25).unwrap()),
            kind: HostFailureKind::EstablishAuthorization,
            class: AttemptFailureClass::RecoverableInfrastructure,
            site: AttemptFailureSite::Attempt,
        };
        controller
            .record_original_failure(AttemptFailure::Host(original))
            .unwrap();
        assert_eq!(controller.observation(), None);
        assert_eq!(
            selected_failure(poll(
                &mut controller,
                0,
                false,
                None,
                NOT_READY,
                None,
                TerminalWake::Round,
                None
            )),
            AttemptFailure::Host(original)
        );
        let mut tagged = TerminalController::new();
        tagged
            .record_original_failure(AttemptFailure::EstablishReplayExhausted {
                original,
                participation: part(),
                operation_id: operation(1),
            })
            .unwrap();
        assert_eq!(
            tagged.observation(),
            Some(PreReadyObservation::Unknown(backend(1)))
        );
        let AttemptFailure::EstablishReplayExhausted {
            original: retained, ..
        } = &tagged.pending.as_ref().unwrap().failure
        else {
            unreachable!()
        };
        assert_eq!(retained.site, AttemptFailureSite::Attempt); // No fake Operation site rewrites original cause.
    }
    #[test]
    fn unqualified_identity_refusal_does_not_invent_an_original_window() {
        let execution = crate::test_support::execution_with_strict_input();
        let mut controller = TerminalController::new();
        let AttemptFailure::TaskProtocol(error) = refusal(true) else {
            unreachable!()
        };
        controller
            .record_execution_failure(*error, &execution)
            .unwrap();
        assert_eq!(controller.observation(), None);
        assert_eq!(
            selected_failure(poll(
                &mut controller,
                0,
                false,
                None,
                NOT_READY,
                None,
                TerminalWake::Round,
                None
            )),
            refusal(true)
        );
    }
    #[test]
    fn stale_foreign_or_future_samples_do_not_supply_replacement_to_another_refusal() {
        let mut controller = TerminalController::new();
        record_refusal(&mut controller, true, 100);
        let window = current_window(&controller, Some(100)).unwrap();
        assert_eq!(
            controller
                .process_reply(
                    now(0),
                    token(9),
                    backend(1),
                    now(0),
                    MembershipObservation::ExactReplacement(backend(2)),
                    window
                )
                .unwrap(),
            ReplyDisposition::Stale
        );
        poll(
            &mut controller,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Round,
            Some(1),
        );
        controller.permission_reply(token(1), true);
        poll(
            &mut controller,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Continuation,
            Some(2),
        );
        assert!(
            controller
                .process_reply(
                    now(1),
                    token(2),
                    backend(3),
                    now(1),
                    MembershipObservation::ExactReplacement(backend(2)),
                    window
                )
                .is_err()
        );
        assert!(
            controller
                .process_reply(
                    now(1),
                    token(2),
                    backend(1),
                    now(2),
                    MembershipObservation::ExactReplacement(backend(2)),
                    window
                )
                .is_err()
        );
        assert!(controller.sample.is_some());
        controller
            .process_reply(
                now(1),
                token(2),
                backend(1),
                now(1),
                MembershipObservation::ExactReplacement(backend(1)),
                window,
            )
            .unwrap();
        assert!(controller.observation().is_some());
        assert!(matches!(controller.phase, ObservationPhase::Waiting { .. }));
    }
    #[test]
    fn old_unknown_sample_after_definite_refinement_cannot_attach_to_new_refusal_or_resurrect_terminal()
     {
        let mut controller = TerminalController::new();
        controller.record_original_failure(unknown(1)).unwrap();
        poll(
            &mut controller,
            0,
            false,
            None,
            NOT_READY,
            Some(100),
            TerminalWake::Round,
            Some(1),
        );
        let old = window(PreReadyObservation::Unknown(backend(1)), Some(100));
        record_refusal(&mut controller, true, 100);
        assert_eq!(
            controller
                .process_reply(
                    now(1),
                    token(1),
                    backend(1),
                    now(1),
                    MembershipObservation::ExactReplacement(backend(2)),
                    old
                )
                .unwrap(),
            ReplyDisposition::Stale
        );
        assert!(matches!(
            poll(
                &mut controller,
                1,
                false,
                None,
                NOT_READY,
                Some(99),
                TerminalWake::Round,
                Some(2)
            ),
            TerminalDirective::QueryPermission { .. }
        ));
        assert!(controller.sample.is_none());
        controller.observe_stop(AttemptStopReason::Client, host(30));
        assert!(matches!(
            poll(
                &mut controller,
                1,
                false,
                None,
                NOT_READY,
                Some(99),
                TerminalWake::Cancellation,
                None
            ),
            TerminalDirective::Wait
        ));
        assert_eq!(
            controller.permission_reply(token(2), true),
            ReplyDisposition::Applied
        );
        let selected = selected_failure(poll(
            &mut controller,
            1,
            false,
            None,
            NOT_READY,
            Some(99),
            TerminalWake::Cancellation,
            None,
        ));
        assert_eq!(selected, refusal(true));
        assert_eq!(
            controller.permission_reply(token(2), true),
            ReplyDisposition::Stale
        );
        assert_eq!(
            controller.terminal(),
            Some(&AttemptTerminal::Failed(refusal(true)))
        );
    }
}
