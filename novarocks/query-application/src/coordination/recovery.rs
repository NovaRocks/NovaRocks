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

use novarocks_coordination_contract::{AttemptFailureClass, ExecutionEffect, RecoveryMode};

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
pub struct RecoveryInput<'a> {
    pub mode: RecoveryMode,
    pub effect: ExecutionEffect,
    pub failure: AttemptFailureClass,
    pub pre_ready_replacement: Option<&'a crate::api::PreReadyProcessReplacementWitness>,
    pub output_visible: bool,
    pub attempts_started: u32,
    pub max_attempts: u32,
    pub deadline_reached: bool,
}

/// The same owner-policy window is used before observing independent evidence
/// and when accepting replacement. It does not classify or rewrite a failure.
#[derive(Clone, Copy)]
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

pub const fn evaluate_recovery(input: RecoveryInput<'_>) -> RecoveryDecision {
    let window = recovery_window_refusal(RecoveryWindowInput {
        mode: input.mode,
        effect: input.effect,
        output_visible: input.output_visible,
        attempts_started: input.attempts_started,
        max_attempts: input.max_attempts,
        deadline_reached: input.deadline_reached,
    });
    // Preserve the original refusal ordering: policy, failure basis, then the
    // runtime window. Observation uses the same window without inventing a class.
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
mod tests {
    use super::*;

    fn recoverable<'a>() -> RecoveryInput<'a> {
        RecoveryInput {
            mode: RecoveryMode::RestartAttemptBeforeVisibility,
            effect: ExecutionEffect::None,
            failure: AttemptFailureClass::RecoverableInfrastructure,
            pre_ready_replacement: None,
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

    #[test]
    fn unsupported_capability_never_restarts_a_remote_attempt() {
        assert_eq!(
            evaluate_recovery(RecoveryInput {
                failure: AttemptFailureClass::Unsupported,
                ..recoverable()
            }),
            RecoveryDecision::Refuse(RecoveryRefusal::FailureClass)
        );
    }

    #[test]
    fn local_resource_governance_never_restarts_a_remote_attempt() {
        assert_eq!(
            evaluate_recovery(RecoveryInput {
                failure: AttemptFailureClass::ResourceGovernance,
                ..recoverable()
            }),
            RecoveryDecision::Refuse(RecoveryRefusal::FailureClass)
        );
    }
    #[test]
    fn witnessed_identity_refusal_adds_a_basis_without_reclassifying_or_bypassing_policy() {
        use novarocks_execution_contract::QueryContextRef;
        use novarocks_types::{
            AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
        };
        let execution =
            QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(1).unwrap()).unwrap();
        let context = QueryContextRef::new(
            execution,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let (_, witness, failure) = crate::api::pre_ready_identity_refusal_test_fixture(context);
        let basis = RecoveryInput {
            failure: failure.class(),
            pre_ready_replacement: Some(&witness),
            ..recoverable()
        };
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(evaluate_recovery(basis), RecoveryDecision::BeginReplacement);
        assert_eq!(
            evaluate_recovery(RecoveryInput {
                pre_ready_replacement: None,
                ..basis
            }),
            RecoveryDecision::Refuse(RecoveryRefusal::FailureClass)
        );
        assert_eq!(
            evaluate_recovery(RecoveryInput {
                mode: RecoveryMode::NoRecovery,
                ..basis
            }),
            RecoveryDecision::Refuse(RecoveryRefusal::Mode)
        );
        assert_eq!(
            evaluate_recovery(RecoveryInput {
                output_visible: true,
                ..basis
            }),
            RecoveryDecision::Refuse(RecoveryRefusal::OutputVisible)
        );
        assert_eq!(
            evaluate_recovery(RecoveryInput {
                deadline_reached: true,
                ..basis
            }),
            RecoveryDecision::Refuse(RecoveryRefusal::Deadline)
        );
        assert_eq!(
            evaluate_recovery(RecoveryInput {
                attempts_started: basis.max_attempts,
                ..basis
            }),
            RecoveryDecision::Refuse(RecoveryRefusal::AttemptBudget)
        );
    }
}
