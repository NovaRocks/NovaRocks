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
    ActorRegistration, ActorTurnError, StandingInputObligation,
};
use novarocks_control_runtime_core::identity::InputId;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupInput {
    Bootstrap,
    StartProtocol,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupStatus {
    Pending,
    Ready,
    Failed,
    Closed,
}

/// A passive, cancellation-safe witness of the registered host's startup.
#[must_use]
pub(crate) struct AttemptStartup {
    registration: ActorRegistration,
    status: watch::Receiver<StartupStatus>,
}

impl AttemptStartup {
    pub(crate) async fn wait_ready(&mut self) -> Result<(), ActorTurnError> {
        let status = self
            .status
            .wait_for(|status| *status != StartupStatus::Pending)
            .await;
        match status.as_deref() {
            Ok(StartupStatus::Ready) => Ok(()),
            Ok(StartupStatus::Failed) => Err(ActorTurnError::ActorFailed),
            _ => Err(ActorTurnError::Closed),
        }
    }

    pub(crate) fn ready_for(&self, registration: &ActorRegistration) -> bool {
        self.registration.same_registration(registration)
            && *self.status.borrow() == StartupStatus::Ready
    }
}

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
}

impl AttemptStartupSource {
    pub(crate) fn new(
        registration: ActorRegistration,
        input: StandingInputObligation<FrontendActor>,
    ) -> (Self, AttemptStartup) {
        let (status, receiver) = watch::channel(StartupStatus::Pending);
        (
            Self {
                registration: registration.clone(),
                input,
                next: Some(StartupInput::Bootstrap),
                in_flight: None,
                indexed: false,
                closed: false,
                status,
            },
            AttemptStartup {
                registration,
                status: receiver,
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
                if *status != StartupStatus::Pending {
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

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }
}
