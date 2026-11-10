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

//! A prepaid Ordinary input role for one frozen participation.

use super::ObservationError;
use crate::control_runtime::actor::{FrontendActor, FrontendInput};
use novarocks_attempt_protocol::input::{AttemptInput, ExecutorFact, ReadProgressSnapshot};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorTurnError, StandingInputId, StandingInputObligation},
    coalescing_input::{CoalescedReceipt, CoalescingInput, Merge},
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_execution_contract::ParticipationRef;

pub(super) struct ReadProgressUpdate(Option<ReadProgressSnapshot>);

impl Merge for ReadProgressUpdate {
    fn merge(&mut self, newer: Self) {
        if let Some(newer) = newer.0
            && self.0.is_none_or(|old| {
                (newer.generation, newer.resume_revision) > (old.generation, old.resume_revision)
            })
        {
            self.0 = Some(newer);
        }
    }

    fn unit() -> Self {
        Self(None)
    }
}

pub(super) type ReadProgressReceipt = CoalescedReceipt<FrontendActor, ReadProgressUpdate>;

#[derive(Clone)]
pub(crate) struct ReadProgressInput {
    registration: ActorRegistration,
    participation: ParticipationRef,
    role: CoalescingInput<FrontendActor, ReadProgressUpdate>,
}

impl ReadProgressInput {
    pub(crate) fn new(
        registration: &ActorRegistration,
        participation: ParticipationRef,
        standing: StandingInputObligation<FrontendActor>,
    ) -> Self {
        assert!(
            registration.matches_input_author(
                &standing
                    .author()
                    .expect("original admitted read progress author")
            )
        );
        Self {
            registration: registration.clone(),
            participation,
            role: CoalescingInput::new(standing, |update: &ReadProgressUpdate| {
                FrontendInput::Attempt(Box::new(AttemptInput::Executor(
                    ExecutorFact::ReadProgress(update.0.expect("actual read resume fact")),
                )))
            }),
        }
    }

    pub(super) fn standing_id(&self) -> StandingInputId {
        self.role.id()
    }

    pub(super) fn matches(&self, registration: &ActorRegistration) -> bool {
        self.registration.same_registration(registration)
    }

    pub(super) fn publish(
        &self,
        progress: ReadProgressSnapshot,
    ) -> Result<ReadProgressReceipt, ObservationError> {
        if self.participation != progress.participation {
            return Err(ObservationError::UnknownStream);
        }
        self.role
            .notify_tracked(ReadProgressUpdate(Some(progress)))
            .map_err(ObservationError::Runtime)
    }

    /// The original host drives this after indexing the real runtime input.
    /// Each visit retires at most one occurrence, then admits a coalesced hint.
    #[cfg(test)]
    pub(crate) fn retire_ready(&self) -> Result<bool, ActorTurnError> {
        self.role.retire_ready_after_exit()
    }

    pub(super) fn retire_indexed(&self, input: InputId) -> Result<bool, ActorTurnError> {
        self.role.retire_indexed_after_exit(input)
    }

    pub(super) fn abandon_pending_after_failure(&self) {
        self.role.abandon_pending_after_failure();
    }

    pub(super) fn retire_failed(&self) -> Result<(), ActorTurnError> {
        self.role.retire_failed_after_exit()
    }

    /// This role is stable across generations. Only the original Source
    /// owner closes it, after all publishers and their receipts have exited.
    pub(crate) fn close_after_exit(&self) -> Result<(), ActorTurnError> {
        self.role.close_after_exit()
    }
}
