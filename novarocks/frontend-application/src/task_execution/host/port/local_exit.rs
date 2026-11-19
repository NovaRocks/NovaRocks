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

//! The original host-owned Lifecycle position for real local convergence.
//! Stop policy and convergence commands are owned by the two-core ports.

use crate::control_runtime::actor::{FrontendActor, FrontendInput};
use novarocks_attempt_protocol::input::{AttemptInput, ExecutorFact};
use novarocks_control_runtime::actor_turn::{
    ActorRegistration, ActorTurnError, StandingInputObligation,
};
use novarocks_control_runtime_core::identity::InputId;

/// Checked by explicit progress through actual input/effect retirement and
/// original role closure. Drop is not a local-exit or completion certificate.
#[must_use = "Retire the same paid Lifecycle position after its actual input and effects"]
pub(crate) struct LocalExitInputObligation {
    original: ActorRegistration,
    position: StandingInputObligation<FrontendActor>,
    requested: bool,
    submitted: bool,
    indexed: bool,
    closed: bool,
}
impl LocalExitInputObligation {
    pub(crate) const VISIT_UNITS: usize = 1;
    pub(crate) fn new(
        original: ActorRegistration,
        position: StandingInputObligation<FrontendActor>,
    ) -> Self {
        assert!(position.matches_registration(&original));
        Self {
            original,
            position,
            requested: false,
            submitted: false,
            indexed: false,
            closed: false,
        }
    }
    pub(crate) fn matches(&self, original: &ActorRegistration) -> bool {
        self.original.same_registration(original)
    }
    pub(crate) fn request_local_exit(&mut self) {
        assert!(!std::mem::replace(&mut self.requested, true));
    }
    pub(crate) fn waiting_for_local_exit(&self) -> bool {
        self.requested && !self.submitted
    }
    pub(crate) fn index_committed(&mut self, inputs: &[InputId]) -> Result<bool, ActorTurnError> {
        if self
            .position
            .in_flight()?
            .is_some_and(|input| inputs.contains(&input))
        {
            assert!(self.submitted && !self.indexed);
            self.indexed = true;
            return Ok(true);
        }
        Ok(false)
    }
    pub(crate) fn needs_progress(&self, failed: bool, closing: bool) -> bool {
        !self.closed && (failed || closing || self.requested && (!self.submitted || self.indexed))
    }
    /// local_exited comes only from the CommonHost's original complete local
    /// quiescence predicate. It never comes from terminal, timeout or Stop.
    pub(crate) fn progress(
        &mut self,
        failed: bool,
        closing: bool,
        local_exited: bool,
    ) -> Result<bool, ActorTurnError> {
        if self.closed {
            return Ok(false);
        }
        if failed {
            match self.position.take_failed_role_payload() {
                Ok(Some(original)) => {
                    original.dispose()?;
                    return Ok(true);
                }
                Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                Err(error) => return Err(error),
            }
        }
        let close = failed || closing || self.indexed;
        if close && self.position.retire_ready_after_exit()? {
            self.position.close_after_exit()?;
            self.closed = true;
        } else if local_exited && self.requested && !self.submitted && !close {
            let value = FrontendInput::Attempt(Box::new(AttemptInput::Executor(
                ExecutorFact::LocalConvergenceExited,
            )));
            match self.position.fill(value) {
                Ok(_) => self.submitted = true,
                Err((ActorTurnError::ActorFailed | ActorTurnError::Closed, _)) => {}
                Err((error, _)) => return Err(error),
            }
        }
        Ok(false)
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }
}
