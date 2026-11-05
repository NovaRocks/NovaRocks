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

use novarocks_execution_contract::ParticipationState;

/// An event in the coordinator's view of a remote query context.
///
/// This state machine governs what the frontend may send or conclude. It does
/// not authorize a Worker-side lifecycle mutation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ParticipationEvent {
    Establish,
    EstablishCompleted,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ParticipationTransition {
    Apply(ParticipationState),
    Idempotent,
    AlreadyTerminal,
    Illegal,
}

pub fn classify_participation_transition(
    state: ParticipationState,
    event: ParticipationEvent,
) -> ParticipationTransition {
    use ParticipationEvent as Event;
    use ParticipationState as State;

    match (state, event) {
        (State::Absent, Event::Establish) => ParticipationTransition::Apply(State::Establishing),
        (State::Establishing, Event::EstablishCompleted) => {
            ParticipationTransition::Apply(State::Active)
        }
        (State::Establishing, Event::Establish) => ParticipationTransition::Idempotent,
        (State::Active, Event::Establish | Event::EstablishCompleted) => {
            ParticipationTransition::Idempotent
        }
        (State::TerminalRetained | State::Gone, _) => ParticipationTransition::AlreadyTerminal,
        _ => ParticipationTransition::Illegal,
    }
}

pub const fn participation_state_is_closed(state: ParticipationState) -> bool {
    matches!(
        state,
        ParticipationState::Releasing
            | ParticipationState::Aborting
            | ParticipationState::TerminalRetained
            | ParticipationState::Gone
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn releasing_participation_is_closed() {
        assert!(participation_state_is_closed(ParticipationState::Releasing));
    }
}
