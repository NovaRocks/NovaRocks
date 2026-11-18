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

//! Ordered root observations and the single in-flight root control request.

use novarocks_coordination_contract::{
    AttemptRequestToken, AttemptRootFailure, AttemptRootProjection, ExecutionFailure,
    ExecutionFailureInput, ObservedTaskTransition, StatusObservation, classify_observation,
    classify_observed_task_transition,
};
use novarocks_execution_contract::{TaskIdentity, TaskState, TaskStatusCursor};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootControlKind {
    TerminalDemand,
    Seal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RootControlRequest {
    pub kind: RootControlKind,
    pub token: AttemptRequestToken,
    pub root: TaskIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootControlRefusal {
    IdentityMismatch,
    StatusConflict,
    IllegalTransition,
    FailureRefinement,
    UnexpectedReply,
    InvalidSuccessSeal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RootObservation {
    Live,
    /// Delivery is frozen while the original serialized author resolves cause.
    PendingFailure,
    Authoritative(ExecutionFailure),
    Ignored,
}

#[derive(Debug)]
pub(crate) struct RootControl {
    root: TaskIdentity,
    accepted: Option<AttemptRootProjection>,
    in_flight: Option<RootControlRequest>,
    want_terminal: bool,
    terminal_delivered: bool,
    want_seal: bool,
    sealed: bool,
}

impl RootControl {
    pub(crate) fn has_request(&self) -> bool {
        self.in_flight.is_none() && (self.want_terminal || self.want_seal)
    }
    pub(crate) fn readable(&self) -> bool {
        self.accepted.as_ref().is_some_and(|a| {
            a.status.installed()
                && !a.status.state().is_failure()
                && !a.status.state().is_cancellation()
                && !a.status.state().is_abort()
                && matches!(a.failure, AttemptRootFailure::None)
        })
    }
    pub(crate) fn new(root: TaskIdentity) -> Self {
        Self {
            root,
            accepted: None,
            in_flight: None,
            want_terminal: false,
            terminal_delivered: false,
            want_seal: false,
            sealed: false,
        }
    }
    pub(crate) const fn root(&self) -> TaskIdentity {
        self.root
    }
    pub(crate) fn accepted(&self) -> Option<&AttemptRootProjection> {
        self.accepted.as_ref()
    }
    pub(crate) fn finished(&self) -> bool {
        self.accepted.as_ref().is_some_and(|a| {
            a.status.state() == TaskState::Finished && matches!(a.failure, AttemptRootFailure::None)
        })
    }
    pub(crate) const fn sealed(&self) -> bool {
        self.sealed
    }
    #[cfg(test)]
    pub(crate) const fn in_flight(&self) -> Option<RootControlRequest> {
        self.in_flight
    }

    /// Withdrawal asks the same attempt owner; it never invents a failure cause.
    pub(crate) fn request_terminal(&mut self) {
        if !self.terminal_delivered {
            self.want_terminal = true;
        }
    }
    pub(crate) fn request_seal(&mut self) {
        if !self.sealed {
            self.want_seal = true;
        }
    }

    pub(crate) fn next_request(
        &mut self,
        token: AttemptRequestToken,
    ) -> Option<RootControlRequest> {
        if self.in_flight.is_some() {
            return None;
        }
        let kind = if self.want_terminal {
            RootControlKind::TerminalDemand
        } else if self.want_seal {
            RootControlKind::Seal
        } else {
            return None;
        };
        let request = RootControlRequest {
            kind,
            token,
            root: self.root,
        };
        match kind {
            RootControlKind::TerminalDemand => self.want_terminal = false,
            RootControlKind::Seal => self.want_seal = false,
        }
        self.in_flight = Some(request);
        Some(request)
    }

    /// A failed reply frees only this control request, preserving the typed error
    /// for the core to accept. It does not silently grant a seal or retry.
    pub(crate) fn settle(
        &mut self,
        kind: RootControlKind,
        token: AttemptRequestToken,
        succeeded: bool,
    ) -> Result<(), RootControlRefusal> {
        if !self
            .in_flight
            .is_some_and(|r| r.kind == kind && r.token == token)
        {
            return Err(RootControlRefusal::UnexpectedReply);
        }
        if succeeded && kind == RootControlKind::Seal && !self.finished() {
            return Err(RootControlRefusal::InvalidSuccessSeal);
        }
        self.in_flight = None;
        if succeeded {
            match kind {
                RootControlKind::TerminalDemand => self.terminal_delivered = true,
                RootControlKind::Seal => self.sealed = true,
            }
        }
        Ok(())
    }

    pub(crate) fn observe(
        &mut self,
        observed: AttemptRootProjection,
    ) -> Result<RootObservation, RootControlRefusal> {
        if observed.status.identity() != self.root {
            return Err(RootControlRefusal::IdentityMismatch);
        }
        if let AttemptRootFailure::Authoritative(selected) = &observed.failure
            && selected.identity().query_execution_id() != self.root.query_execution_id()
        {
            return Err(RootControlRefusal::IdentityMismatch);
        }
        if let Some(held) = self.accepted.as_ref()
            && held.status == observed.status
        {
            if held.failure == observed.failure {
                return Ok(RootObservation::Ignored);
            }
            if !matches!(
                (&held.failure, &observed.failure),
                (AttemptRootFailure::None, AttemptRootFailure::DerivedPending)
                    | (
                        AttemptRootFailure::None | AttemptRootFailure::DerivedPending,
                        AttemptRootFailure::Authoritative(_)
                    )
            ) {
                return Err(RootControlRefusal::FailureRefinement);
            }
        } else {
            let cursor = self.accepted.as_ref().map_or_else(
                || TaskStatusCursor::unobserved(self.root),
                |held| TaskStatusCursor::at(self.root, held.status.version()),
            );
            match classify_observation(
                cursor,
                self.accepted.as_ref().map(|a| &a.status),
                &observed.status,
            ) {
                StatusObservation::Ignore | StatusObservation::Idempotent => {
                    return Ok(RootObservation::Ignored);
                }
                StatusObservation::Accept => {}
                _ => return Err(RootControlRefusal::StatusConflict),
            }
            if let Some(held) = self.accepted.as_ref()
                && held.status.version().next() == Some(observed.status.version())
                && !matches!(
                    classify_observed_task_transition(held.status.state(), observed.status.state()),
                    ObservedTaskTransition::Advance | ObservedTaskTransition::SameState
                )
            {
                return Err(RootControlRefusal::IllegalTransition);
            }
        }
        let result = match &observed.failure {
            AttemptRootFailure::Authoritative(selected) => {
                match ExecutionFailureInput::from_selected_task(selected) {
                    Some(input) => {
                        RootObservation::Authoritative(ExecutionFailure::from_input(input))
                    }
                    None => return Err(RootControlRefusal::FailureRefinement),
                }
            }
            AttemptRootFailure::DerivedPending => RootObservation::PendingFailure,
            AttemptRootFailure::None => {
                let state = observed.status.state();
                if state.is_failure() || state.is_cancellation() || state.is_abort() {
                    RootObservation::PendingFailure
                } else {
                    RootObservation::Live
                }
            }
        };
        self.accepted = Some(observed);
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
