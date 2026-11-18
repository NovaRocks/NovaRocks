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

//! The installed host drives one paid Lifecycle position through startup.

use crate::control_runtime::actor::{FrontendActor, FrontendInput};
use novarocks_control_runtime::actor_turn::{
    ActorRegistration, ActorRuntime, ActorTurnError, StandingInputObligation,
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_query_application::api::NativeAttemptStartupOutcome;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupInput {
    Bootstrap,
    StartProtocol,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupStatus {
    Pending,
    Initialized,
    Ready,
    Failed,
    Closed,
}

/// A passive, cancellation-safe witness of the registered host's startup.
#[must_use]
#[derive(Clone)]
pub(crate) struct AttemptStartup {
    registration: ActorRegistration,
    status: watch::Receiver<StartupStatus>,
    released: Arc<AtomicU8>,
}

impl AttemptStartup {
    /// Release only initialization after the original HostExit and endpoints
    /// have been bound. Arbitrary host/header wakes cannot grant this release.
    pub(crate) fn release_initialization(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
    ) -> Result<(), ActorTurnError> {
        self.released.fetch_max(1, Ordering::AcqRel);
        runtime.notify_host_actor(&self.registration)
    }

    /// The original LP RunAttempt releases candidate admission on this same
    /// startup position. Initialization alone never starts protocol business.
    pub(crate) fn release_protocol(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
    ) -> Result<(), ActorTurnError> {
        if !matches!(
            *self.status.borrow(),
            StartupStatus::Initialized | StartupStatus::Ready
        ) {
            return Err(ActorTurnError::Closed);
        }
        self.released.store(2, Ordering::Release);
        runtime.notify_host_actor(&self.registration)
    }

    pub(crate) async fn wait_initialized(&mut self) -> Result<(), ActorTurnError> {
        let status = self
            .status
            .wait_for(|status| *status != StartupStatus::Pending)
            .await;
        match status.as_deref() {
            Ok(StartupStatus::Initialized | StartupStatus::Ready) => Ok(()),
            Ok(StartupStatus::Failed) => Err(ActorTurnError::ActorFailed),
            _ => Err(ActorTurnError::Closed),
        }
    }

    /// Observe only this original startup registration. Cloning the watch
    /// witness creates no new startup, input, admission or completion fact.
    pub(crate) fn matches_registration(&self, registration: &ActorRegistration) -> bool {
        self.registration.same_registration(registration)
    }

    #[cfg(test)]
    pub(crate) async fn wait_ready(&mut self) -> Result<(), ActorTurnError> {
        let status = self
            .status
            .wait_for(|status| {
                !matches!(status, StartupStatus::Pending | StartupStatus::Initialized)
            })
            .await;
        match status.as_deref() {
            Ok(StartupStatus::Ready) => Ok(()),
            Ok(StartupStatus::Failed) => Err(ActorTurnError::ActorFailed),
            _ => Err(ActorTurnError::Closed),
        }
    }

    /// Release the same initialized protocol, or retain its already-published
    /// normal closure. Closure is not readiness, terminal state or actual exit.
    pub(crate) async fn start_protocol(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
    ) -> Result<NativeAttemptStartupOutcome, ActorTurnError> {
        self.start_protocol_inner(
            runtime,
            #[cfg(test)]
            || {},
        )
        .await
    }

    async fn start_protocol_inner(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        #[cfg(test)] before_validate: impl FnOnce(),
    ) -> Result<NativeAttemptStartupOutcome, ActorTurnError> {
        let status = *self.status.borrow();
        if let Some(outcome) = Self::outcome(status)? {
            return Ok(outcome);
        }
        if status != StartupStatus::Initialized {
            return Err(ActorTurnError::Closed);
        }
        if self.status.has_changed().is_err() {
            // A final value may have been published after the initial sample
            // and before its Source exited. Channel loss alone is not closure.
            return self.published_outcome()?.ok_or(ActorTurnError::Closed);
        }
        #[cfg(test)]
        before_validate();
        if let Err(error) = runtime.validate_registration(&self.registration) {
            // The original host can publish its terminal observation and
            // actually retire after the watch preflight. Keep that same fact.
            return self.published_outcome()?.ok_or(error);
        }
        if let Err(error) = self.release_protocol(runtime) {
            // The same Source may close between the preflight and release.
            // Only its actual terminal observation can settle this race.
            return self.published_outcome()?.ok_or(error);
        }
        let status = self
            .status
            .wait_for(|status| {
                !matches!(status, StartupStatus::Pending | StartupStatus::Initialized)
            })
            .await
            .map(|status| *status)
            .map_err(|_| ActorTurnError::Closed)?;
        Self::outcome(status)?.ok_or(ActorTurnError::Closed)
    }

    fn published_outcome(&self) -> Result<Option<NativeAttemptStartupOutcome>, ActorTurnError> {
        Self::outcome(*self.status.borrow())
    }

    fn outcome(
        status: StartupStatus,
    ) -> Result<Option<NativeAttemptStartupOutcome>, ActorTurnError> {
        match status {
            StartupStatus::Ready => Ok(Some(NativeAttemptStartupOutcome::Ready)),
            StartupStatus::Closed => Ok(Some(NativeAttemptStartupOutcome::ClosedByProtocol)),
            StartupStatus::Failed => Err(ActorTurnError::ActorFailed),
            StartupStatus::Pending | StartupStatus::Initialized => Ok(None),
        }
    }

    #[cfg(test)]
    pub(crate) fn ready_for(&self, registration: &ActorRegistration) -> bool {
        self.registration.same_registration(registration)
            && *self.status.borrow() == StartupStatus::Ready
    }

    pub(crate) fn initialized_for(&self, registration: &ActorRegistration) -> bool {
        self.registration.same_registration(registration)
            && matches!(
                *self.status.borrow(),
                StartupStatus::Initialized | StartupStatus::Ready
            )
    }

    /// Test-only simulation of the original Run receipt after fixture setup.
    /// Production must pass the real initialization/Run gate in release_protocol.
    #[cfg(test)]
    pub(crate) fn release_legacy_run(&self, runtime: &ActorRuntime<FrontendActor>) {
        self.released.store(2, Ordering::Release);
        runtime
            .notify_host_actor(&self.registration)
            .expect("startup is scheduled on its installed original host");
    }
}

#[cfg(test)]
mod tests;

/// The position remains with the same host across dropped application waits.
/// Only that host indexes committed batches and retires the input.
pub(crate) struct AttemptStartupSource {
    registration: ActorRegistration,
    input: StandingInputObligation<FrontendActor>,
    next: Option<StartupInput>,
    in_flight: Option<InputId>,
    indexed: bool,
    closed: bool,
    status: watch::Sender<StartupStatus>,
    released: Arc<AtomicU8>,
}

impl AttemptStartupSource {
    pub(crate) fn new(
        registration: ActorRegistration,
        input: StandingInputObligation<FrontendActor>,
    ) -> (Self, AttemptStartup) {
        let (status, receiver) = watch::channel(StartupStatus::Pending);
        let released = Arc::new(AtomicU8::new(0));
        (
            Self {
                registration: registration.clone(),
                input,
                next: Some(StartupInput::Bootstrap),
                in_flight: None,
                indexed: false,
                closed: false,
                status,
                released: released.clone(),
            },
            AttemptStartup {
                registration,
                status: receiver,
                released,
            },
        )
    }

    pub(crate) fn matches(&self, registration: &ActorRegistration) -> bool {
        self.registration.same_registration(registration)
            && self.input.matches_registration(registration)
    }

    pub(crate) fn indexed(&mut self, inputs: &[InputId]) -> bool {
        if self.in_flight.is_some_and(|input| inputs.contains(&input)) {
            self.indexed = true;
            true
        } else {
            false
        }
    }

    /// Advance at most one original transition per paid visit. Returns true
    /// only when the same host has immediate work to do on another visit.
    pub(crate) fn progress(&mut self, failed: bool, closing: bool) -> Result<bool, ActorTurnError> {
        if failed || closing {
            self.status.send_if_modified(|status| {
                if !matches!(status, StartupStatus::Pending | StartupStatus::Initialized) {
                    return false;
                }
                *status = if failed {
                    StartupStatus::Failed
                } else {
                    StartupStatus::Closed
                };
                true
            });
        }
        if self.in_flight.is_some() {
            if !self.indexed && !failed {
                return Ok(false);
            }
            if self.input.retire_ready_after_exit()? {
                self.in_flight = None;
                self.indexed = false;
                if self.next == Some(StartupInput::StartProtocol) && !failed && !closing {
                    self.status.send_replace(StartupStatus::Initialized);
                }
                return Ok(true);
            }
            if failed {
                match self.input.take_failed_role_payload() {
                    // This exact role only accepts the two neutral startup
                    // inputs. There are no application captures to dispose.
                    Ok(Some(payload)) => {
                        assert_eq!(Some(payload.dispose()?), self.in_flight);
                        return Ok(true);
                    }
                    Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                    Err(error) => return Err(error),
                }
            }
            return Ok(false);
        }

        let outcome = if failed {
            StartupStatus::Failed
        } else if closing {
            StartupStatus::Closed
        } else if let Some(next) = self.next {
            let required = match next {
                StartupInput::Bootstrap => 1,
                StartupInput::StartProtocol => 2,
            };
            if self.released.load(Ordering::Acquire) < required {
                return Ok(false);
            }
            match self.input.fill(FrontendInput::AttemptStartup(next)) {
                Ok(input) => {
                    self.in_flight = Some(input);
                    self.next = matches!(next, StartupInput::Bootstrap)
                        .then_some(StartupInput::StartProtocol);
                    return Ok(false);
                }
                Err((ActorTurnError::ActorFailed, _)) => StartupStatus::Failed,
                Err((ActorTurnError::Closed, _)) => StartupStatus::Closed,
                Err((error, _)) => return Err(error),
            }
        } else {
            StartupStatus::Ready
        };

        self.input.close_after_exit()?;
        self.closed = true;
        self.status.send_replace(outcome);
        Ok(false)
    }

    pub(crate) fn is_ready(&self) -> bool {
        *self.status.borrow() == StartupStatus::Ready
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }
}
