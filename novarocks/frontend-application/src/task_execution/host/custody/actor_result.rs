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

use super::{
    OriginalTransportJoinObligation, TransportJoinHandoff, TransportJoinRegistration,
    TransportRegistrationState,
};
use crate::{
    control_runtime::{actor::FrontendActor, transport_withdrawal::AttemptEffectEnvelope},
    task_execution::host::transport::BodyOutcome,
};
use novarocks_control_runtime::{actor_turn::InputAuthor, causal_input::CausalInputObligation};
use std::sync::Arc;

/// The original result position is held independently of the worker future.
/// A missing outcome after a positively observed exit is left to failure
/// custody. In particular, it never implies that a send was withdrawn.
#[must_use = "finish the original result input or give it to failure custody"]
pub(crate) struct ReturnedActorTransportResult {
    pub(crate) effect: Arc<AttemptEffectEnvelope>,
    pub(crate) original: CausalInputObligation<FrontendActor>,
    pub(crate) outcome: Option<BodyOutcome>,
}
pub(super) enum ActorResultState {
    Unbound,
    Held(ReturnedActorTransportResult),
    Taken,
}
impl TransportJoinHandoff {
    /// Transfer the paid actor position before the factory can run, fail or
    /// destroy its captures. Refusal preserves the original prepared job.
    pub(crate) fn bind_actor_result(
        &self,
        effect: Arc<AttemptEffectEnvelope>,
        expected: &InputAuthor,
        original: CausalInputObligation<FrontendActor>,
    ) -> Result<(), CausalInputObligation<FrontendActor>> {
        let Some(record) = self.registration.original.upgrade() else {
            return Err(original);
        };
        if !original
            .event_author()
            .is_ok_and(|author| author.same_binding(expected))
        {
            return Err(original);
        }
        let mut result = record
            .actor_result
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !matches!(*result, ActorResultState::Unbound) {
            return Err(original);
        }
        *result = ActorResultState::Held(ReturnedActorTransportResult {
            effect,
            original,
            outcome: None,
        });
        Ok(())
    }
}
impl TransportJoinRegistration {
    pub(crate) fn stage_actor_outcome(&self, outcome: BodyOutcome) -> Result<(), BodyOutcome> {
        let Some(record) = self.original.upgrade() else {
            return Err(outcome);
        };
        let mut state = record
            .actor_result
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let ActorResultState::Held(original) = &mut *state else {
            return Err(outcome);
        };
        if original.outcome.is_some() {
            return Err(outcome);
        }
        original.outcome = Some(outcome);
        Ok(())
    }
}
impl OriginalTransportJoinObligation {
    /// For a send, this is after the final original WC queue exit. An input
    /// admission without a Native row uses its own actual Join and report.
    pub(crate) fn take_actor_result(&self) -> Option<ReturnedActorTransportResult> {
        if !matches!(
            *self.native.state.lock().unwrap_or_else(|e| e.into_inner()),
            TransportRegistrationState::Issued
        ) {
            return None;
        }
        let mut state = self
            .native
            .actor_result
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !matches!(*state, ActorResultState::Held(_)) {
            return None;
        }
        let ActorResultState::Held(original) =
            std::mem::replace(&mut *state, ActorResultState::Taken)
        else {
            unreachable!("qualified original result");
        };
        drop(state);
        original.effect.retire_handoff();
        Some(original)
    }

    pub(super) fn actor_result_held(&self) -> bool {
        matches!(
            *self
                .native
                .actor_result
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
            ActorResultState::Held(_)
        )
    }
}
