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

//! Closed whole-attempt recovery policy, shared with pre-ready observation.

use novarocks_coordination_contract::{
    AttemptFailureClass, ExecutionEffect, PreReadyProcessReplacementWitness, RecoveryMode,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryRefusal {
    Mode,
    ExternalEffect,
    FailureClass,
    OutputVisible,
    AttemptBudget,
    Deadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryDecision {
    BeginReplacement,
    Refuse(RecoveryRefusal),
}

/// The step owner derives failure class and witness from the SAME passive
/// failure, after checking its original Establish correlation. This module
/// neither authors a new failure classification nor checks a live ledger.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RecoveryInput<'a> {
    pub mode: RecoveryMode,
    pub effect: ExecutionEffect,
    pub failure: AttemptFailureClass,
    pub pre_ready_replacement: Option<&'a PreReadyProcessReplacementWitness>,
    pub output_visible: bool,
    pub attempts_started: u32,
    pub max_attempts: u32,
    pub deadline_reached: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RecoveryWindowInput {
    pub mode: RecoveryMode,
    pub effect: ExecutionEffect,
    pub output_visible: bool,
    pub attempts_started: u32,
    pub max_attempts: u32,
    pub deadline_reached: bool,
}

pub(crate) const fn recovery_window_refusal(input: RecoveryWindowInput) -> Option<RecoveryRefusal> {
    if !matches!(input.mode, RecoveryMode::RestartAttemptBeforeVisibility) {
        return Some(RecoveryRefusal::Mode);
    }
    if !matches!(input.effect, ExecutionEffect::None) {
        return Some(RecoveryRefusal::ExternalEffect);
    }
    if input.output_visible {
        return Some(RecoveryRefusal::OutputVisible);
    }
    if input.attempts_started >= input.max_attempts {
        return Some(RecoveryRefusal::AttemptBudget);
    }
    if input.deadline_reached {
        return Some(RecoveryRefusal::Deadline);
    }
    None
}

pub(crate) const fn evaluate_recovery(input: RecoveryInput<'_>) -> RecoveryDecision {
    let window = recovery_window_refusal(RecoveryWindowInput {
        mode: input.mode,
        effect: input.effect,
        output_visible: input.output_visible,
        attempts_started: input.attempts_started,
        max_attempts: input.max_attempts,
        deadline_reached: input.deadline_reached,
    });
    // Preserve the original refusal ordering: policy, failure basis, then the
    // runtime window. Observation does not invent a failure class.
    if let Some(reason @ (RecoveryRefusal::Mode | RecoveryRefusal::ExternalEffect)) = window {
        return RecoveryDecision::Refuse(reason);
    }
    if !matches!(
        input.failure,
        AttemptFailureClass::RecoverableInfrastructure
    ) && !(matches!(input.failure, AttemptFailureClass::ContractViolation)
        && input.pre_ready_replacement.is_some())
    {
        return RecoveryDecision::Refuse(RecoveryRefusal::FailureClass);
    }
    match window {
        Some(reason) => RecoveryDecision::Refuse(reason),
        None => RecoveryDecision::BeginReplacement,
    }
}

#[cfg(test)]
mod tests;
