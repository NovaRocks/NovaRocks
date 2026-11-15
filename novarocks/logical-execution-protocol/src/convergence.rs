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

//! Attempt cleanup consumes independent remote, relay and actual-exit facts.
//! Neither Worker closure nor the end of finite tracking proves physical exit.

#![expect(dead_code, reason = "wired in the cutover commit")]

use crate::{LogicalActionId, StepBudget, input::ObligationKind, stand_down::RemoteProgress};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptConvergenceMode, MonotonicInstant, QueryExecutionError,
};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupPath {
    ResolveThenFinish,
    EndTrackingThenAbandon,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupIntent {
    ResolveObligation(ObligationKind),
    EndRemoteTracking(ObligationKind),
    FinishResources,
    AbandonResources,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CleanupAction {
    pub action: LogicalActionId,
    pub intent: CleanupIntent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupPoll {
    Issued(CleanupAction),
    Idle,
    Yielded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConvergenceError {
    NotStarted,
    UnknownAction,
    ActionIdentityConflict,
    UnexpectedObligation,
    ConflictingResult,
    ResponsibilitySettlementFailed,
    ResourceSettlementFailed,
}

impl fmt::Display for ConvergenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotStarted => "attempt convergence has not started",
            Self::UnknownAction => "convergence fact names an unknown original action",
            Self::ActionIdentityConflict => "convergence action identity was already issued",
            Self::UnexpectedObligation => "convergence fact names an undeclared obligation",
            Self::ConflictingResult => "convergence action has conflicting original results",
            Self::ResponsibilitySettlementFailed => {
                "stand-down residual responsibility could not be settled"
            }
            Self::ResourceSettlementFailed => "attempt resource cleanup failed before actual exit",
        })
    }
}

impl std::error::Error for ConvergenceError {}

impl novarocks_execution_contract::failure::FailureProjection for ConvergenceError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let (class, code) = match self {
            Self::ResponsibilitySettlementFailed => (FailureClass::Unexpected, 93),
            Self::ResourceSettlementFailed => (FailureClass::Unexpected, 93),
            Self::NotStarted | Self::UnexpectedObligation => (FailureClass::Control, 85),
            Self::UnknownAction | Self::ActionIdentityConflict | Self::ConflictingResult => {
                (FailureClass::Invariant, 84)
            }
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ObligationPresence {
    pub running_work: bool,
    pub retired_attempt: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Completion {
    action: LogicalActionId,
    result: Option<bool>,
    exited: bool,
}

impl Completion {
    fn new(action: LogicalActionId) -> Self {
        Self {
            action,
            result: None,
            exited: false,
        }
    }
    const fn complete(self) -> bool {
        matches!(self.result, Some(true)) && self.exited
    }
    fn result(&mut self, success: bool) -> Result<(), ConvergenceError> {
        match self.result {
            Some(old) if old != success => Err(ConvergenceError::ConflictingResult),
            _ => {
                self.result = Some(success);
                Ok(())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ObligationProgress {
    expected: bool,
    registered: bool,
    completion: Option<Completion>,
}

impl ObligationProgress {
    const fn new(expected: bool) -> Self {
        Self {
            expected,
            registered: false,
            completion: None,
        }
    }
    fn complete(self) -> bool {
        !self.expected || self.completion.is_some_and(Completion::complete)
    }
}

/// Fixed-size state only: the remote rows live in the stand-down ledger.
/// The two obligation kinds are independent; no kind can satisfy its sibling.
#[derive(Debug)]
pub(crate) struct ConvergenceRecord {
    activation: AttemptActivationIdentity,
    mode: Option<AttemptConvergenceMode>,
    remote_deadline: Option<MonotonicInstant>,
    relay_expected: bool,
    relay_stopped: bool,
    deliveries_settled: bool,
    attempt_local_exited: bool,
    obligations: [ObligationProgress; 2],
    resources_present: bool,
    resources: Option<Completion>,
    selected: Option<CleanupPath>,
    failure: Option<(ConvergenceError, QueryExecutionError)>,
}

impl ConvergenceRecord {
    pub fn new(
        activation: AttemptActivationIdentity,
        relay_expected: bool,
        resources_present: bool,
        obligations: ObligationPresence,
    ) -> Self {
        Self {
            activation,
            mode: None,
            remote_deadline: None,
            relay_expected,
            relay_stopped: false,
            deliveries_settled: false,
            attempt_local_exited: false,
            obligations: [
                ObligationProgress::new(obligations.running_work),
                ObligationProgress::new(obligations.retired_attempt),
            ],
            resources_present,
            resources: None,
            selected: None,
            failure: None,
        }
    }

    /// Reentry may strengthen Normal to Cleanup and shorten the original
    /// deadline, never renew observation or reopen a selected cleanup path.
    pub fn begin(&mut self, mode: AttemptConvergenceMode, deadline: MonotonicInstant) {
        self.mode = Some(if self.mode == Some(AttemptConvergenceMode::Cleanup) {
            AttemptConvergenceMode::Cleanup
        } else {
            mode
        });
        self.remote_deadline = Some(
            self.remote_deadline
                .map_or(deadline, |old| old.min(deadline)),
        );
    }

    pub fn activation(&self) -> AttemptActivationIdentity {
        self.activation
    }
    pub fn remote_deadline(&self) -> Option<MonotonicInstant> {
        self.remote_deadline
    }
    pub fn selected_path(&self) -> Option<CleanupPath> {
        self.selected
    }
    pub fn failure(&self) -> Option<&QueryExecutionError> {
        self.failure.as_ref().map(|(_, error)| error)
    }

    pub fn observe_relay_stopped(&mut self, all_deliveries_settled: bool) {
        self.relay_stopped = true;
        self.deliveries_settled |= all_deliveries_settled;
    }
    pub fn observe_attempt_local_exit(&mut self) {
        self.attempt_local_exited = true;
    }

    pub fn observe_obligation_registered(
        &mut self,
        kind: ObligationKind,
    ) -> Result<(), ConvergenceError> {
        let obligation = &mut self.obligations[index(kind)];
        if !obligation.expected {
            return Err(ConvergenceError::UnexpectedObligation);
        }
        obligation.registered = true;
        Ok(())
    }

    /// At most the fixed two obligation slots and one resource slot are read.
    /// Result and actual action exit must both arrive before dependent cleanup.
    pub fn poll_cleanup(
        &mut self,
        now: MonotonicInstant,
        remote: RemoteProgress,
        action: LogicalActionId,
        budget: &mut dyn StepBudget,
    ) -> Result<CleanupPoll, ConvergenceError> {
        let deadline = self.remote_deadline.ok_or(ConvergenceError::NotStarted)?;
        if let Some((error, _)) = &self.failure {
            return Err(*error);
        }
        if !remote.initialized || !remote.tracking_complete {
            return Ok(CleanupPoll::Idle);
        }
        if self.selected.is_none() {
            let path = if remote.positive_resource_closure {
                CleanupPath::ResolveThenFinish
            } else if now >= deadline {
                CleanupPath::EndTrackingThenAbandon
            } else {
                return Ok(CleanupPoll::Idle);
            };
            self.selected = Some(path);
        }
        let path = self.selected.unwrap();
        for kind in [ObligationKind::RunningWork, ObligationKind::RetiredAttempt] {
            let progress = self.obligations[index(kind)];
            if !progress.expected {
                continue;
            }
            if !progress.registered || progress.completion.is_some_and(|c| !c.complete()) {
                continue;
            }
            if progress.completion.is_none() {
                self.check_fresh_action(action)?;
                if !budget.charge(1) {
                    return Ok(CleanupPoll::Yielded);
                }
                self.obligations[index(kind)].completion = Some(Completion::new(action));
                let intent = match path {
                    CleanupPath::ResolveThenFinish => CleanupIntent::ResolveObligation(kind),
                    CleanupPath::EndTrackingThenAbandon => CleanupIntent::EndRemoteTracking(kind),
                };
                return Ok(CleanupPoll::Issued(CleanupAction { action, intent }));
            }
        }
        if self
            .obligations
            .iter()
            .copied()
            .all(ObligationProgress::complete)
            && self.resources_present
            && self.resources.is_none()
        {
            self.check_fresh_action(action)?;
            if !budget.charge(1) {
                return Ok(CleanupPoll::Yielded);
            }
            self.resources = Some(Completion::new(action));
            let intent = match path {
                CleanupPath::ResolveThenFinish => CleanupIntent::FinishResources,
                CleanupPath::EndTrackingThenAbandon => CleanupIntent::AbandonResources,
            };
            return Ok(CleanupPoll::Issued(CleanupAction { action, intent }));
        }
        Ok(CleanupPoll::Idle)
    }

    pub fn observe_governance_result(
        &mut self,
        action: LogicalActionId,
        kind: ObligationKind,
        result: Result<(), QueryExecutionError>,
    ) -> Result<(), ConvergenceError> {
        let completion = self.obligations[index(kind)]
            .completion
            .as_mut()
            .ok_or(ConvergenceError::UnknownAction)?;
        if completion.action != action {
            return Err(ConvergenceError::UnknownAction);
        }
        completion.result(result.is_ok())?;
        if let Err(original) = result {
            self.failure
                .get_or_insert((ConvergenceError::ResponsibilitySettlementFailed, original));
            return Err(ConvergenceError::ResponsibilitySettlementFailed);
        }
        Ok(())
    }

    pub fn observe_resource_result(
        &mut self,
        action: LogicalActionId,
        result: Result<(), QueryExecutionError>,
    ) -> Result<(), ConvergenceError> {
        let completion = self
            .resources
            .as_mut()
            .ok_or(ConvergenceError::UnknownAction)?;
        if completion.action != action {
            return Err(ConvergenceError::UnknownAction);
        }
        completion.result(result.is_ok())?;
        if let Err(original) = result {
            self.failure
                .get_or_insert((ConvergenceError::ResourceSettlementFailed, original));
            return Err(ConvergenceError::ResourceSettlementFailed);
        }
        Ok(())
    }

    pub fn observe_action_exit(&mut self, action: LogicalActionId) -> Result<(), ConvergenceError> {
        for obligation in &mut self.obligations {
            if let Some(completion) = &mut obligation.completion
                && completion.action == action
            {
                completion.exited = true;
                return Ok(());
            }
        }
        if let Some(completion) = &mut self.resources
            && completion.action == action
        {
            completion.exited = true;
            return Ok(());
        }
        Err(ConvergenceError::UnknownAction)
    }

    pub fn cleanup_complete(&self, remote: RemoteProgress) -> bool {
        self.mode.is_some()
            && self.selected.is_some()
            && self.failure.is_none()
            && remote.initialized
            && remote.tracking_complete
            && (!self.relay_expected || (self.relay_stopped && self.deliveries_settled))
            && self.attempt_local_exited
            && self
                .obligations
                .iter()
                .copied()
                .all(ObligationProgress::complete)
            && (!self.resources_present || self.resources.is_some_and(Completion::complete))
    }

    fn check_fresh_action(&self, action: LogicalActionId) -> Result<(), ConvergenceError> {
        if self
            .obligations
            .iter()
            .any(|p| p.completion.is_some_and(|c| c.action == action))
            || self.resources.is_some_and(|c| c.action == action)
        {
            Err(ConvergenceError::ActionIdentityConflict)
        } else {
            Ok(())
        }
    }
}

const fn index(kind: ObligationKind) -> usize {
    match kind {
        ObligationKind::RunningWork => 0,
        ObligationKind::RetiredAttempt => 1,
    }
}

#[cfg(test)]
#[path = "convergence/tests.rs"]
mod tests;
