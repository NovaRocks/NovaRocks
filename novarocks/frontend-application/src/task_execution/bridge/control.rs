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
    input::{AttemptInput, ExecutorFact},
};
use novarocks_control_runtime::actor_turn::{
    ActorRegistration, ActorTurnError, StandingInputObligation,
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptComputationCut, AttemptConvergenceMode, AttemptStopReason,
    LogicalToAttempt, LogicalToAttemptBody, MonotonicClock, MonotonicInstant,
};
use novarocks_query_application::{
    api::NativeActiveConvergenceRequest, session_outcome::governed_cancellation_failure_input,
};
use novarocks_workload_control::CancellationReason;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::sync::Notify;

type Position = StandingInputObligation<FrontendActor>;

const STOP: usize = 0;
const CONVERGENCE: usize = 1;

struct ControlInputs {
    positions: [Mutex<Position>; 2],
    fenced: AtomicBool,
    changed: Notify,
}
impl ControlInputs {
    fn position(&self, role: usize) -> std::sync::MutexGuard<'_, Position> {
        self.positions[role]
            .lock()
            .expect("original attempt owner control")
    }

    fn fill(&self, role: usize, value: FrontendInput) -> Result<InputId, ActorTurnError> {
        let position = self.position(role);
        if self.fenced.load(Ordering::Acquire) {
            return Err(ActorTurnError::Closed);
        }
        position.fill(value).map_err(|(error, _)| error)
    }

    fn fence(&self) {
        if !self.fenced.swap(true, Ordering::AcqRel) {
            self.changed.notify_waiters();
        }
    }

    fn close_positions(&self) -> Result<(), ActorTurnError> {
        self.position(STOP).close_after_exit()?;
        self.position(CONVERGENCE).close_after_exit()
    }
}

#[must_use]
pub(crate) struct AttemptOwnerControls {
    activation: AttemptActivationIdentity,
    inputs: Arc<ControlInputs>,
    stopped: bool,
    host_bound: bool,
    #[cfg(test)]
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
            inputs: Arc::new(ControlInputs {
                positions: [Mutex::new(stop), Mutex::new(convergence)],
                fenced: AtomicBool::new(false),
                changed: Notify::new(),
            }),
            stopped: false,
            host_bound: false,
            #[cfg(test)]
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
        let input = self.inputs.fill(
            STOP,
            owner_input(self.activation, cancellation_stop(reason)),
        )?;
        self.stopped = true;
        Ok(Some(input))
    }

    pub(crate) fn cancellation_submitted(&self) -> bool {
        self.stopped
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
            .inputs
            .fill(CONVERGENCE, owner_input(self.activation, body))?;
        self.submitted = Some(next);
        Ok(Some(input))
    }

    pub(crate) fn bind_host(&mut self, original: ActorRegistration) -> AttemptOwnerControlSource {
        assert!(!self.host_bound, "original controls already have a host");
        self.host_bound = true;
        let stop = self.inputs.position(STOP);
        let convergence = self.inputs.position(CONVERGENCE);
        assert!(
            stop.matches_registration(&original) && convergence.matches_registration(&original)
        );
        assert_ne!(stop.id(), convergence.id());
        AttemptOwnerControlSource {
            original,
            inputs: self.inputs.clone(),
            indexed: [None; 2],
            local_exit: None,
            closed: false,
        }
    }

    /// Wait for the original convergence occurrence and all its effects.
    pub(crate) async fn wait_convergence_idle(&self) -> Result<(), ActorTurnError> {
        loop {
            let changed = self.inputs.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let position = self.inputs.position(CONVERGENCE);
                if self.inputs.fenced.load(Ordering::Acquire) {
                    return Err(ActorTurnError::Closed);
                }
                if position.in_flight()?.is_none() {
                    return Ok(());
                }
            }
            changed.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn close_after_exit(&mut self) -> Result<(), ActorTurnError> {
        if self.closed {
            return Ok(());
        }
        if !self.inputs.position(STOP).retire_ready_after_exit()?
            || !self
                .inputs
                .position(CONVERGENCE)
                .retire_ready_after_exit()?
        {
            return Err(ActorTurnError::StandingInputBusy);
        }
        self.inputs.fence();
        self.inputs.close_positions()?;
        self.closed = true;
        Ok(())
    }
}

pub(crate) struct AttemptOwnerControlSource {
    original: ActorRegistration,
    inputs: Arc<ControlInputs>,
    indexed: [Option<InputId>; 2],
    local_exit: Option<LocalExitInput>,
    closed: bool,
}

/// This Lifecycle position belongs solely to the installed host. It remains
/// paid until the Core has handled the real local exit and its effects retire.
struct LocalExitInput {
    position: Position,
    requested: bool,
    submitted: bool,
    indexed: bool,
}

impl AttemptOwnerControlSource {
    pub(crate) fn with_local_exit(mut self, position: Position) -> Self {
        assert!(position.matches_registration(&self.original));
        self.local_exit = Some(LocalExitInput {
            position,
            requested: false,
            submitted: false,
            indexed: false,
        });
        self
    }

    pub(crate) fn request_local_exit(&mut self) {
        let local = self
            .local_exit
            .as_mut()
            .expect("original local exit position");
        assert!(!std::mem::replace(&mut local.requested, true));
    }

    pub(crate) fn waiting_for_local_exit(&self) -> bool {
        self.local_exit
            .as_ref()
            .is_some_and(|local| local.requested && !local.submitted)
    }

    pub(crate) fn matches(&self, original: &ActorRegistration) -> bool {
        self.original.same_registration(original)
    }

    pub(crate) fn index_committed(&mut self, inputs: &[InputId]) -> Result<bool, ActorTurnError> {
        if inputs.is_empty() {
            return Ok(false);
        }
        let mut found = false;
        for (role, indexed) in self.indexed.iter_mut().enumerate() {
            if let Some(input) = self.inputs.position(role).in_flight()?
                && inputs.contains(&input)
            {
                assert!(indexed.replace(input).is_none());
                found = true;
            }
        }
        if let Some(local) = &mut self.local_exit
            && local
                .position
                .in_flight()?
                .is_some_and(|input| inputs.contains(&input))
        {
            assert!(local.submitted && !local.indexed);
            local.indexed = true;
            found = true;
        }
        Ok(found)
    }

    pub(crate) const VISIT_UNITS: usize = 3;

    pub(crate) fn needs_progress(&self, failed: bool, closing: bool) -> bool {
        failed
            || closing
            || self.indexed.iter().any(Option::is_some)
            || self
                .local_exit
                .as_ref()
                .is_some_and(|local| local.requested && (!local.submitted || local.indexed))
    }

    pub(crate) fn progress(
        &mut self,
        failed: bool,
        closing: bool,
        local_exited: bool,
    ) -> Result<bool, ActorTurnError> {
        if failed || closing {
            self.inputs.fence();
        }
        let mut idle = true;
        for (role, indexed) in self.indexed.iter_mut().enumerate() {
            let position = self.inputs.position(role);
            if failed {
                match position.take_failed_role_payload() {
                    Ok(Some(original)) => {
                        // Dispose only this role's bounded protocol input.
                        // Startup shares the Source and owns its own payload.
                        drop(position);
                        original.dispose()?;
                        return Ok(true);
                    }
                    Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                    Err(error) => return Err(error),
                }
            }
            let retired = if failed || indexed.is_some() {
                position.retire_ready_after_exit()?
            } else {
                position.in_flight()?.is_none()
            };
            if retired && indexed.take().is_some() && role == CONVERGENCE {
                drop(position);
                self.inputs.changed.notify_waiters();
            }
            idle &= retired;
        }
        if let Some(local) = &mut self.local_exit {
            if failed {
                match local.position.take_failed_role_payload() {
                    Ok(Some(original)) => {
                        original.dispose()?;
                        return Ok(true);
                    }
                    Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                    Err(error) => return Err(error),
                }
            }
            let close = failed || closing || local.indexed;
            if close && local.position.retire_ready_after_exit()? {
                local.position.close_after_exit()?;
                self.local_exit = None;
            } else if local_exited && local.requested && !local.submitted && !close {
                let value = FrontendInput::Attempt(Box::new(AttemptInput::Executor(
                    ExecutorFact::LocalConvergenceExited,
                )));
                match local.position.fill(value) {
                    Ok(_) => local.submitted = true,
                    Err((ActorTurnError::ActorFailed | ActorTurnError::Closed, _)) => {}
                    Err((error, _)) => return Err(error),
                }
            }
        }
        if idle && self.local_exit.is_none() && self.inputs.fenced.load(Ordering::Acquire) {
            self.inputs.close_positions()?;
            self.closed = true;
        }
        Ok(false)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
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
