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

//! Prepaid logical-owner control for the original attempt actor.

use crate::control_runtime::actor::{FrontendActor, FrontendInput};
use novarocks_attempt_protocol::{
    failure::{AttemptFailure, AttemptFailureSite, ProtocolFailure},
    input::AttemptInput,
};
use novarocks_control_runtime::actor_turn::{ActorTurnError, StandingInputObligation};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptComputationCut, AttemptConvergenceMode, AttemptStopReason,
    LogicalToAttempt, LogicalToAttemptBody, MonotonicClock, MonotonicInstant,
};
use novarocks_query_application::{
    api::NativeActiveConvergenceRequest, session_outcome::governed_cancellation_failure_input,
};
use novarocks_workload_control::CancellationReason;
use std::time::Instant;

type Position = StandingInputObligation<FrontendActor>;

#[must_use]
pub(crate) struct AttemptOwnerControls {
    activation: AttemptActivationIdentity,
    stop: Position,
    convergence: Position,
    stopped: bool,
    closed: bool,
    submitted: Option<ConvergenceSubmission>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct ConvergenceSubmission {
    absolute_deadline: Instant,
    remote_deadline: MonotonicInstant,
    mode: AttemptConvergenceMode,
    cut: Option<AttemptComputationCut>,
    retain_diagnostics: bool,
}

impl AttemptOwnerControls {
    pub(crate) fn new(
        activation: AttemptActivationIdentity,
        stop: Position,
        convergence: Position,
    ) -> Self {
        Self {
            activation,
            stop,
            convergence,
            stopped: false,
            closed: false,
            submitted: None,
        }
    }

    pub(crate) fn stop_for_cancellation(
        &mut self,
        reason: CancellationReason,
    ) -> Result<Option<InputId>, ActorTurnError> {
        if self.stopped {
            return Ok(None);
        }
        let input = self
            .stop
            .fill(owner_input(self.activation, cancellation_stop(reason)))
            .map_err(|(error, _)| error)?;
        self.stopped = true;
        Ok(Some(input))
    }

    /// Preserve the original projection on reentry. The caller retries a
    /// tightened request after the preceding Control occurrence has retired.
    pub(crate) fn begin_convergence(
        &mut self,
        request: &NativeActiveConvergenceRequest,
        mode: AttemptConvergenceMode,
        retain_diagnostics: bool,
        clock: &dyn MonotonicClock,
    ) -> Result<Option<InputId>, ActorTurnError> {
        let cut = request
            .computation_ready()
            .map(|ready| AttemptComputationCut {
                activation: ready.activation(),
                root: ready.root(),
                cut: ready.cut(),
                diagnostic_deadline: ready.diagnostic_deadline(),
                convergence_deadline: ready.convergence_deadline(),
                end: ready.end(),
            });
        if cut.is_some_and(|cut| cut.activation != self.activation)
            || self.submitted.is_some_and(|original| original.cut != cut)
        {
            return Err(ActorTurnError::InvalidConfig);
        }
        let next = convergence_submission(
            self.submitted,
            request.remote_observation_deadline(),
            mode,
            cut,
            retain_diagnostics,
            clock,
        );
        if self.submitted == Some(next) {
            return Ok(None);
        }
        let body = LogicalToAttemptBody::BeginConvergence {
            mode: next.mode,
            remote_deadline: next.remote_deadline,
            computation_ready: next.cut,
            retain_diagnostics: next.retain_diagnostics,
        };
        let input = self
            .convergence
            .fill(owner_input(self.activation, body))
            .map_err(|(error, _)| error)?;
        self.submitted = Some(next);
        Ok(Some(input))
    }

    pub(crate) fn retire_ready(&self) -> Result<bool, ActorTurnError> {
        if self.closed {
            return Ok(true);
        }
        let stop = self.stop.retire_ready_after_exit()?;
        let convergence = self.convergence.retire_ready_after_exit()?;
        Ok(stop && convergence)
    }

    pub(crate) fn close_after_exit(&mut self) -> Result<(), ActorTurnError> {
        if self.closed {
            return Ok(());
        }
        if !self.retire_ready()? {
            return Err(ActorTurnError::StandingInputBusy);
        }
        self.stop.close_after_exit()?;
        self.convergence.close_after_exit()?;
        self.closed = true;
        Ok(())
    }
}

fn owner_input(
    activation: AttemptActivationIdentity,
    body: LogicalToAttemptBody<AttemptFailure>,
) -> FrontendInput {
    FrontendInput::Attempt(Box::new(AttemptInput::Port(LogicalToAttempt {
        activation,
        body,
    })))
}

fn cancellation_stop(reason: CancellationReason) -> LogicalToAttemptBody<AttemptFailure> {
    use CancellationReason as R;
    let stop = match reason {
        R::DeadlineExceeded => AttemptStopReason::Deadline,
        R::FrontendDrainDeadlineExceeded => AttemptStopReason::DrainDeadline,
        R::ServerShutdown | R::OwnerDropped => AttemptStopReason::Shutdown,
        R::Requested
        | R::ExplicitKill { .. }
        | R::ExplicitKillConnection { .. }
        | R::ClientCancelRequested
        | R::ClientDisconnected => AttemptStopReason::Client,
    };
    let original = governed_cancellation_failure_input(reason);
    LogicalToAttemptBody::Stop {
        reason: stop,
        failure: AttemptFailure::Protocol(ProtocolFailure {
            fact: original.fact(),
            detail: original.detail().clone(),
            site: AttemptFailureSite::Attempt,
        }),
    }
}

fn convergence_submission(
    previous: Option<ConvergenceSubmission>,
    absolute_deadline: Instant,
    mode: AttemptConvergenceMode,
    cut: Option<AttemptComputationCut>,
    retain_diagnostics: bool,
    clock: &dyn MonotonicClock,
) -> ConvergenceSubmission {
    let (absolute_deadline, remote_deadline) = match previous {
        Some(original) if original.absolute_deadline <= absolute_deadline => {
            (original.absolute_deadline, original.remote_deadline)
        }
        original => {
            let now = clock.now();
            let projected =
                now.saturating_add(absolute_deadline.saturating_duration_since(Instant::now()));
            let deadline = original.map_or(projected, |original| {
                original.remote_deadline.min(projected)
            });
            (absolute_deadline, deadline)
        }
    };
    let mode = if previous.is_some_and(|original| original.mode == AttemptConvergenceMode::Cleanup)
    {
        AttemptConvergenceMode::Cleanup
    } else {
        mode
    };
    ConvergenceSubmission {
        absolute_deadline,
        remote_deadline,
        mode,
        cut,
        retain_diagnostics: retain_diagnostics
            && previous.is_none_or(|original| original.retain_diagnostics),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Clock(MonotonicInstant);

    impl MonotonicClock for Clock {
        fn now(&self) -> MonotonicInstant {
            self.0
        }
    }

    #[test]
    fn convergence_reentry_keeps_the_original_deadline_and_cleanup_mode() {
        use std::time::Duration;
        let absolute = Instant::now();
        let now = MonotonicInstant::from_origin(Duration::from_secs(10));
        let later = now.saturating_add(Duration::from_secs(100));
        let original = convergence_submission(
            None,
            absolute,
            AttemptConvergenceMode::Normal,
            None,
            true,
            &Clock(now),
        );
        assert_eq!(original.remote_deadline, now);
        let cleanup = convergence_submission(
            Some(original),
            absolute + Duration::from_secs(10),
            AttemptConvergenceMode::Cleanup,
            None,
            false,
            &Clock(later),
        );
        assert_eq!(cleanup.absolute_deadline, absolute);
        assert_eq!(cleanup.remote_deadline, now);
        assert_eq!(cleanup.mode, AttemptConvergenceMode::Cleanup);
        let earlier = convergence_submission(
            Some(cleanup),
            absolute - Duration::from_secs(1),
            AttemptConvergenceMode::Normal,
            None,
            true,
            &Clock(later),
        );
        assert!(earlier.absolute_deadline < absolute);
        assert_eq!(earlier.remote_deadline, now);
        assert_eq!(earlier.mode, AttemptConvergenceMode::Cleanup);
        assert!(!earlier.retain_diagnostics);
    }

    #[test]
    fn frontend_drain_stop_preserves_qa_typed_cancellation() {
        let cancellation = CancellationReason::FrontendDrainDeadlineExceeded;
        let original = governed_cancellation_failure_input(cancellation.clone());
        let LogicalToAttemptBody::Stop {
            reason: AttemptStopReason::DrainDeadline,
            failure: AttemptFailure::Protocol(actual),
        } = cancellation_stop(cancellation)
        else {
            panic!("the original frontend drain deadline");
        };
        assert_eq!(actual.fact, original.fact());
        assert_eq!(&actual.detail, original.detail());
        assert_eq!(actual.site, AttemptFailureSite::Attempt);
    }
}
