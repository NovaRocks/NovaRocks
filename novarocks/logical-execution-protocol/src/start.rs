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

//! Frozen logical execution facts. No field grants access to a host resource.

use novarocks_coordination_contract::{
    AttemptActivationIdentity, ExecutionEffect, IdSeed, LogicalExecutionActorId, MonotonicInstant,
    RecoveryMode,
};
use novarocks_execution_contract::ParticipationRef;
use novarocks_result_contract::{ClientRowProfile, RootOutputKind};
use novarocks_types::QueryId;
use std::{
    collections::BTreeSet,
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalConclusion {
    Succeeded,
    ComputationReady,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalOutputMode {
    ResultStream,
    InternalResultStream,
    CompletionOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalFinishBoundary {
    ClientEof,
    TaskCompletion,
    ComputationReady,
}

/// Exact original plan-version bytes; never a replacement hash or a capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalPlanIdentity(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootCarrier {
    pub kind: RootOutputKind,
    pub client_rows: Option<ClientRowProfile>,
}

impl RootCarrier {
    pub fn validate(self) -> Result<(), StartRefusal> {
        if (self.kind == RootOutputKind::ClientRows) != self.client_rows.is_some() {
            return Err(StartRefusal::InvalidCarrier);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalPolicy {
    pub recovery: RecoveryMode,
    pub effect: ExecutionEffect,
    pub output: LogicalOutputMode,
    pub finish: LogicalFinishBoundary,
    pub retain_diagnostics: bool,
    pub root_carrier: Option<RootCarrier>,
    pub internal_plan: Option<LogicalPlanIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservationBudgets {
    pub diagnostics: Duration,
    pub convergence: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalBudgets {
    pub max_attempts: NonZeroU32,
    pub replacement_reservation_valid_for: Option<Duration>,
    pub remote_cleanup_timeout: Duration,
    /// Admission replay and Establish authorization have independent owners' limits.
    pub admission_issues_per_participation: NonZeroUsize,
    pub establish_per_participation: NonZeroUsize,
    pub abort_per_participation: NonZeroUsize,
    pub observation: ObservationBudgets,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitialAttempt {
    pub activation: AttemptActivationIdentity,
    pub participations: Box<[ParticipationRef]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalStart {
    pub identity: QueryId,
    pub actor_instance: LogicalExecutionActorId,
    pub policy: LogicalPolicy,
    pub budgets: LogicalBudgets,
    pub first_attempt: InitialAttempt,
    pub statement_deadline: Option<MonotonicInstant>,
    pub id_seed: IdSeed,
    pub started_at: MonotonicInstant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartRefusal {
    ForeignActivation,
    ForeignParticipation,
    DuplicateParticipation,
    ExternalRecovery,
    InvalidLifecyclePolicy,
    InvalidCarrier,
    MissingCarrier,
    MissingPlan,
    MissingRecoveryWindow,
    EmptyObservationBudget,
}

impl LogicalStart {
    pub fn validate(&self) -> Result<(), StartRefusal> {
        let activation = self.first_attempt.activation;
        if activation.actor() != self.actor_instance
            || activation.execution().query_id() != self.identity
        {
            return Err(StartRefusal::ForeignActivation);
        }
        let mut seen = BTreeSet::new();
        for participation in &self.first_attempt.participations {
            if participation.query_execution_id() != activation.execution() {
                return Err(StartRefusal::ForeignParticipation);
            }
            if !seen.insert(*participation) {
                return Err(StartRefusal::DuplicateParticipation);
            }
        }
        if self.policy.effect == ExecutionEffect::External
            && self.policy.recovery != RecoveryMode::NoRecovery
        {
            return Err(StartRefusal::ExternalRecovery);
        }
        if let Some(carrier) = self.policy.root_carrier {
            carrier.validate()?;
        }
        match (self.policy.output, self.policy.finish) {
            (LogicalOutputMode::ResultStream, LogicalFinishBoundary::ClientEof) => {
                if self.policy.effect == ExecutionEffect::External {
                    return Err(StartRefusal::InvalidLifecyclePolicy);
                }
                self.policy
                    .root_carrier
                    .ok_or(StartRefusal::MissingCarrier)?;
            }
            (LogicalOutputMode::InternalResultStream, LogicalFinishBoundary::ComputationReady) => {
                self.policy
                    .root_carrier
                    .ok_or(StartRefusal::MissingCarrier)?;
                self.policy.internal_plan.ok_or(StartRefusal::MissingPlan)?;
            }
            (LogicalOutputMode::CompletionOnly, LogicalFinishBoundary::TaskCompletion) => {}
            _ => return Err(StartRefusal::InvalidLifecyclePolicy),
        }
        if self.policy.recovery == RecoveryMode::RestartAttemptBeforeVisibility
            && self
                .budgets
                .replacement_reservation_valid_for
                .is_none_or(|v| v.is_zero())
        {
            return Err(StartRefusal::MissingRecoveryWindow);
        }
        if self.budgets.observation.diagnostics.is_zero()
            || self.budgets.observation.convergence.is_zero()
        {
            return Err(StartRefusal::EmptyObservationBudget);
        }
        Ok(())
    }
}
