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

//! Closed whole-attempt recovery policy.
//!
//! The business owner selects one mode. Coordination first decides whether an
//! attempt may enter replacement, then records the required runtime facts in
//! its own serialized state before it activates a successor.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryMode {
    NoRecovery,
    RestartAttemptBeforeVisibility,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionEffect {
    None,
    External,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptFailureClass {
    RecoverableInfrastructure,
    ContractViolation,
    Cancelled,
    DeadlineExceeded,
    CommitUnknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryRefusal {
    Mode,
    ExternalEffect,
    FailureClass,
    OutputVisible,
    AttemptBudget,
    Deadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryDecision {
    BeginReplacement,
    Refuse(RecoveryRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryInput {
    pub mode: RecoveryMode,
    pub effect: ExecutionEffect,
    pub failure: AttemptFailureClass,
    pub output_visible: bool,
    pub attempts_started: u32,
    pub max_attempts: u32,
    pub deadline_reached: bool,
}

pub const fn evaluate_recovery(input: RecoveryInput) -> RecoveryDecision {
    if !matches!(input.mode, RecoveryMode::RestartAttemptBeforeVisibility) {
        return RecoveryDecision::Refuse(RecoveryRefusal::Mode);
    }
    if !matches!(input.effect, ExecutionEffect::None) {
        return RecoveryDecision::Refuse(RecoveryRefusal::ExternalEffect);
    }
    if !matches!(
        input.failure,
        AttemptFailureClass::RecoverableInfrastructure
    ) {
        return RecoveryDecision::Refuse(RecoveryRefusal::FailureClass);
    }
    if input.output_visible {
        return RecoveryDecision::Refuse(RecoveryRefusal::OutputVisible);
    }
    if input.attempts_started >= input.max_attempts {
        return RecoveryDecision::Refuse(RecoveryRefusal::AttemptBudget);
    }
    if input.deadline_reached {
        return RecoveryDecision::Refuse(RecoveryRefusal::Deadline);
    }
    RecoveryDecision::BeginReplacement
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recoverable() -> RecoveryInput {
        RecoveryInput {
            mode: RecoveryMode::RestartAttemptBeforeVisibility,
            effect: ExecutionEffect::None,
            failure: AttemptFailureClass::RecoverableInfrastructure,
            output_visible: false,
            attempts_started: 1,
            max_attempts: 2,
            deadline_reached: false,
        }
    }

    #[test]
    fn effect_free_failure_can_enter_replacement_before_visibility() {
        assert_eq!(
            evaluate_recovery(recoverable()),
            RecoveryDecision::BeginReplacement
        );
    }

    #[test]
    fn visibility_and_external_effects_close_recovery_independently() {
        assert_eq!(
            evaluate_recovery(RecoveryInput {
                output_visible: true,
                ..recoverable()
            }),
            RecoveryDecision::Refuse(RecoveryRefusal::OutputVisible)
        );
        assert_eq!(
            evaluate_recovery(RecoveryInput {
                effect: ExecutionEffect::External,
                ..recoverable()
            }),
            RecoveryDecision::Refuse(RecoveryRefusal::ExternalEffect)
        );
    }
}
