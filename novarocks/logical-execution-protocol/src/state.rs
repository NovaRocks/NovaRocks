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

//! Logical phases and exact activation generations. No state value owns work,
//! a result payload, a host resource or a cancellation-on-Drop authority.

use crate::{
    delivery::{DeliveryGate, DeliveryGateError},
    input::QualificationIdentity,
    recovery::{
        RecoveryDecision, RecoveryInput, RecoveryRefusal, RecoveryWindowInput, evaluate_recovery,
    },
    replacement::ReplacementPrerequisites,
    start::{LogicalConclusion, LogicalOutputMode},
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, ExecutionEffect, ExecutionFailure, ExecutionFailureInput,
    RecoveryMode,
};
use novarocks_types::{QueryExecutionId, QueryId};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AttemptDisposition {
    Current,
    Residual,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AttemptRecord {
    pub activation: AttemptActivationIdentity,
    pub disposition: AttemptDisposition,
    /// The original serialized author's accepted cause, distinct from a later
    /// cancellation or successor-preparation error ending the logical execution.
    pub accepted_failure: Option<ExecutionFailureInput>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionPhase {
    Instantiating {
        activation: AttemptActivationIdentity,
    },
    Running {
        activation: AttemptActivationIdentity,
    },
    Replacing {
        identity: QualificationIdentity,
    },
    Converging,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActorStateError {
    ForeignQuery,
    NotCurrent,
    ReusedAttempt,
    NonMonotonicAttempt,
    WrongPhase,
    AlreadyConcluded,
    SuccessRequiresEndOfStream,
    StaleActivation,
    StaleReplacement,
    ReplacementNotReady,
    RecoveryRefused(RecoveryRefusal),
    Delivery(DeliveryGateError),
}

impl From<DeliveryGateError> for ActorStateError {
    fn from(value: DeliveryGateError) -> Self {
        Self::Delivery(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateInvariantViolation {
    AttemptCount,
    CurrentActivation,
    ConclusionPhase,
    ReplacementPhase,
}

#[derive(Debug)]
pub(crate) struct LogicalExecutionState {
    initial: AttemptActivationIdentity,
    recovery_mode: RecoveryMode,
    effect: ExecutionEffect,
    output_mode: LogicalOutputMode,
    max_attempts: u32,
    attempts_started: u32,
    phase: ExecutionPhase,
    attempts: BTreeMap<QueryExecutionId, AttemptRecord>,
    delivery: DeliveryGate,
    conclusion: Option<LogicalConclusion>,
}

impl LogicalExecutionState {
    pub(crate) fn new(
        initial: AttemptActivationIdentity,
        recovery_mode: RecoveryMode,
        effect: ExecutionEffect,
        output_mode: LogicalOutputMode,
        max_attempts: u32,
    ) -> Result<Self, ActorStateError> {
        if max_attempts == 0 {
            return Err(ActorStateError::RecoveryRefused(
                RecoveryRefusal::AttemptBudget,
            ));
        }
        if effect == ExecutionEffect::External && recovery_mode != RecoveryMode::NoRecovery {
            return Err(ActorStateError::RecoveryRefused(
                RecoveryRefusal::ExternalEffect,
            ));
        }
        Ok(Self {
            initial,
            recovery_mode,
            effect,
            output_mode,
            max_attempts,
            attempts_started: 1,
            phase: ExecutionPhase::Instantiating {
                activation: initial,
            },
            attempts: BTreeMap::from([(
                initial.execution(),
                AttemptRecord {
                    activation: initial,
                    disposition: AttemptDisposition::Current,
                    accepted_failure: None,
                },
            )]),
            delivery: DeliveryGate::new(initial),
            conclusion: None,
        })
    }

    pub(crate) const fn query_id(&self) -> QueryId {
        self.initial.execution().query_id()
    }

    pub(crate) const fn phase(&self) -> ExecutionPhase {
        self.phase
    }

    pub(crate) const fn conclusion(&self) -> Option<LogicalConclusion> {
        self.conclusion
    }

    pub(crate) const fn output_visible(&self) -> bool {
        self.delivery.output_visible()
    }

    pub(crate) const fn attempts_started(&self) -> u32 {
        self.attempts_started
    }

    pub(crate) const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub(crate) const fn current_activation(&self) -> Option<AttemptActivationIdentity> {
        self.delivery.current()
    }

    pub(crate) fn attempt(&self, execution: QueryExecutionId) -> Option<&AttemptRecord> {
        self.attempts.get(&execution)
    }

    /// A historical fact can refine only its own exact record. Revoking issue
    /// authority does not erase the original failure author or grant authority
    /// over the successor. Do not call this with a derived logical stop cause.
    pub(crate) fn record_original_attempt_failure(
        &mut self,
        activation: AttemptActivationIdentity,
        input: ExecutionFailureInput,
    ) -> Result<(), ActorStateError> {
        self.validate_activation(activation)?;
        self.attempts
            .get_mut(&activation.execution())
            .expect("validated attempt remains registered")
            .accepted_failure = Some(input);
        Ok(())
    }

    pub(crate) fn validate_activation(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), ActorStateError> {
        if self
            .attempts
            .get(&activation.execution())
            .is_some_and(|record| record.activation == activation)
        {
            Ok(())
        } else {
            Err(ActorStateError::StaleActivation)
        }
    }

    pub(crate) fn require_running(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), ActorStateError> {
        self.validate_activation(activation)?;
        if self.conclusion.is_some() {
            return Err(ActorStateError::AlreadyConcluded);
        }
        match self.phase {
            ExecutionPhase::Running {
                activation: current,
            } if current == activation => Ok(()),
            ExecutionPhase::Running { .. } => Err(ActorStateError::NotCurrent),
            _ => Err(ActorStateError::WrongPhase),
        }
    }

    pub(crate) fn mark_running(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), ActorStateError> {
        self.validate_activation(activation)?;
        if self.conclusion.is_some() {
            return Err(ActorStateError::AlreadyConcluded);
        }
        match self.phase {
            ExecutionPhase::Instantiating {
                activation: current,
            } if current == activation => {
                self.phase = ExecutionPhase::Running { activation };
                Ok(())
            }
            _ => Err(ActorStateError::WrongPhase),
        }
    }

    /// P05 validates schema, carrier and original queue position first.
    pub(crate) fn mark_output_visible(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), ActorStateError> {
        self.require_running(activation)?;
        if self.output_mode != LogicalOutputMode::ResultStream {
            return Err(ActorStateError::WrongPhase);
        }
        self.delivery.mark_visible(activation)?;
        Ok(())
    }

    pub(crate) fn recovery_window(&self, deadline_reached: bool) -> RecoveryWindowInput {
        RecoveryWindowInput {
            mode: self.recovery_mode,
            effect: self.effect,
            output_visible: self.output_visible(),
            attempts_started: self.attempts_started,
            max_attempts: self.max_attempts,
            deadline_reached,
        }
    }

    #[cfg(test)]
    pub(crate) fn may_observe_pre_ready_replacement(&self, deadline_reached: bool) -> bool {
        self.conclusion.is_none()
            && matches!(self.phase, ExecutionPhase::Running { .. })
            && crate::recovery::recovery_window_refusal(self.recovery_window(deadline_reached))
                .is_none()
    }

    /// Called after the successor is prepared and the step owner rechecks the
    /// SAME ledger correlation, scope cause and original preparation window.
    pub(crate) fn begin_replacement(
        &mut self,
        failed: AttemptActivationIdentity,
        replacement: QueryExecutionId,
        failure: &ExecutionFailure,
        deadline_reached: bool,
    ) -> Result<QualificationIdentity, ActorStateError> {
        self.require_running(failed)?;
        self.validate_replacement_identity(failed.execution(), replacement)?;
        if failure.pre_ready_replacement().is_some_and(|witness| {
            witness.participation().query_execution_id() != failed.execution()
                || !witness.matches_input(failure.input())
        }) {
            return Err(ActorStateError::NotCurrent);
        }
        match evaluate_recovery(RecoveryInput {
            mode: self.recovery_mode,
            effect: self.effect,
            failure: failure.class(),
            pre_ready_replacement: failure.pre_ready_replacement(),
            output_visible: self.output_visible(),
            attempts_started: self.attempts_started,
            max_attempts: self.max_attempts,
            deadline_reached,
        }) {
            RecoveryDecision::BeginReplacement => {}
            RecoveryDecision::Refuse(reason) => {
                return Err(ActorStateError::RecoveryRefused(reason));
            }
        }
        let generation = self.delivery.revoke_for_replacement(failed)?;
        self.attempts
            .get_mut(&failed.execution())
            .expect("validated current attempt remains registered")
            .disposition = AttemptDisposition::Residual;
        let identity = QualificationIdentity {
            actor: failed.actor(),
            failed: failed.execution(),
            replacement,
            generation,
        };
        self.phase = ExecutionPhase::Replacing { identity };
        Ok(identity)
    }

    pub(crate) fn activate_replacement(
        &mut self,
        identity: QualificationIdentity,
        prerequisites: ReplacementPrerequisites,
    ) -> Result<AttemptActivationIdentity, ActorStateError> {
        self.require_replacement(identity)?;
        if !prerequisites.complete() {
            return Err(ActorStateError::ReplacementNotReady);
        }
        if self.attempts_started >= self.max_attempts {
            return Err(ActorStateError::RecoveryRefused(
                RecoveryRefusal::AttemptBudget,
            ));
        }
        let activation = AttemptActivationIdentity::from_parts(
            identity.actor.get(),
            identity.replacement,
            identity.generation,
        );
        self.delivery.activate(activation)?;
        let old = self.attempts.insert(
            identity.replacement,
            AttemptRecord {
                activation,
                disposition: AttemptDisposition::Current,
                accepted_failure: None,
            },
        );
        assert!(
            old.is_none(),
            "successor activation cannot replace an attempt record"
        );
        self.attempts_started = self
            .attempts_started
            .checked_add(1)
            .expect("admitted logical attempt count remains representable");
        self.phase = ExecutionPhase::Instantiating { activation };
        Ok(activation)
    }

    pub(crate) fn require_replacement(
        &self,
        identity: QualificationIdentity,
    ) -> Result<(), ActorStateError> {
        if self.conclusion.is_some() {
            return Err(ActorStateError::AlreadyConcluded);
        }
        match self.phase {
            ExecutionPhase::Replacing { identity: current } if current == identity => Ok(()),
            _ => Err(ActorStateError::StaleReplacement),
        }
    }

    fn validate_replacement_identity(
        &self,
        failed: QueryExecutionId,
        replacement: QueryExecutionId,
    ) -> Result<(), ActorStateError> {
        if replacement.query_id() != self.query_id() {
            return Err(ActorStateError::ForeignQuery);
        }
        if self.attempts.contains_key(&replacement) {
            return Err(ActorStateError::ReusedAttempt);
        }
        if replacement.attempt_id() <= failed.attempt_id() {
            return Err(ActorStateError::NonMonotonicAttempt);
        }
        Ok(())
    }

    pub(crate) fn conclude(
        &mut self,
        activation: AttemptActivationIdentity,
        conclusion: LogicalConclusion,
    ) -> Result<(), ActorStateError> {
        self.validate_activation(activation)?;
        if self.conclusion.is_some() {
            return Err(ActorStateError::AlreadyConcluded);
        }
        if self.current_activation() != Some(activation) {
            return Err(ActorStateError::NotCurrent);
        }
        match conclusion {
            LogicalConclusion::Succeeded => {
                self.require_running(activation)?;
                if self.output_mode != LogicalOutputMode::CompletionOnly {
                    return Err(ActorStateError::SuccessRequiresEndOfStream);
                }
            }
            LogicalConclusion::ComputationReady => return Err(ActorStateError::WrongPhase),
            LogicalConclusion::Failed | LogicalConclusion::Cancelled => {}
        }
        self.freeze_conclusion(conclusion);
        Ok(())
    }

    pub(crate) fn conclude_replacement(
        &mut self,
        identity: QualificationIdentity,
        conclusion: LogicalConclusion,
    ) -> Result<(), ActorStateError> {
        self.require_replacement(identity)?;
        if !matches!(
            conclusion,
            LogicalConclusion::Failed | LogicalConclusion::Cancelled
        ) {
            return Err(ActorStateError::WrongPhase);
        }
        self.freeze_conclusion(conclusion);
        Ok(())
    }

    /// Called only after P05 obtains the original End position and validates
    /// all success prerequisites. There is no provisional EOF verdict.
    pub(crate) fn commit_stream_success(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), ActorStateError> {
        self.require_running(activation)?;
        if self.output_mode != LogicalOutputMode::ResultStream {
            return Err(ActorStateError::WrongPhase);
        }
        self.freeze_conclusion(LogicalConclusion::Succeeded);
        Ok(())
    }

    /// P05 has validated the original source, plan, End receipt and root facts.
    pub(crate) fn accept_computation_ready(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), ActorStateError> {
        self.require_running(activation)?;
        if self.output_mode != LogicalOutputMode::InternalResultStream {
            return Err(ActorStateError::WrongPhase);
        }
        self.delivery.mark_visible(activation)?;
        self.freeze_conclusion(LogicalConclusion::ComputationReady);
        Ok(())
    }

    fn freeze_conclusion(&mut self, conclusion: LogicalConclusion) {
        assert!(
            self.conclusion.is_none(),
            "logical conclusion freezes exactly once"
        );
        if let Some(activation) = self.delivery.current() {
            self.attempts
                .get_mut(&activation.execution())
                .expect("current activation retains its original attempt record")
                .disposition = AttemptDisposition::Residual;
        }
        self.delivery.close();
        self.conclusion = Some(conclusion);
        self.phase = ExecutionPhase::Converging;
    }

    pub(crate) fn check_invariants(&self) -> Result<(), StateInvariantViolation> {
        if usize::try_from(self.attempts_started).ok() != Some(self.attempts.len())
            || self.attempts_started > self.max_attempts
        {
            return Err(StateInvariantViolation::AttemptCount);
        }
        let current_count = self
            .attempts
            .values()
            .filter(|record| record.disposition == AttemptDisposition::Current)
            .count();
        match self.phase {
            ExecutionPhase::Instantiating { activation }
            | ExecutionPhase::Running { activation } => {
                if self.conclusion.is_some()
                    || current_count != 1
                    || self.delivery.current() != Some(activation)
                    || !self
                        .attempts
                        .get(&activation.execution())
                        .is_some_and(|record| {
                            record.activation == activation
                                && record.disposition == AttemptDisposition::Current
                        })
                {
                    return Err(StateInvariantViolation::CurrentActivation);
                }
            }
            ExecutionPhase::Replacing { identity } => {
                if self.conclusion.is_some()
                    || current_count != 0
                    || self.delivery.current().is_some()
                    || identity.actor != self.initial.actor()
                    || !self.attempts.contains_key(&identity.failed)
                    || self.attempts.contains_key(&identity.replacement)
                {
                    return Err(StateInvariantViolation::ReplacementPhase);
                }
            }
            ExecutionPhase::Converging => {
                if self.conclusion.is_none()
                    || current_count != 0
                    || self.delivery.current().is_some()
                {
                    return Err(StateInvariantViolation::ConclusionPhase);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
