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

use novarocks_execution_contract::QueryContextState;

/// An event in the coordinator's view of a remote query context.
///
/// This state machine governs what the frontend may send or conclude. It does
/// not authorize a Worker-side lifecycle mutation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum QueryContextEvent {
    Establish,
    EstablishCompleted,
    Release,
    ReleaseCompleted,
    Abort,
    AbortCompleted,
    Reap,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ContextTransition {
    Apply(QueryContextState),
    Idempotent,
    LostToRelease,
    AlreadyTerminal,
    Illegal,
}

pub fn classify_context_transition(
    state: QueryContextState,
    event: QueryContextEvent,
) -> ContextTransition {
    use QueryContextEvent as Event;
    use QueryContextState as State;

    match (state, event) {
        (State::Absent, Event::Establish) => ContextTransition::Apply(State::Establishing),
        (State::Absent, Event::Abort) => ContextTransition::Apply(State::TerminalRetained),
        (State::Establishing, Event::EstablishCompleted) => ContextTransition::Apply(State::Active),
        (State::Establishing, Event::Abort) => ContextTransition::Apply(State::Aborting),
        (State::Establishing, Event::Establish) => ContextTransition::Idempotent,
        (State::Active, Event::Release) => ContextTransition::Apply(State::Releasing),
        (State::Active, Event::Abort) => ContextTransition::Apply(State::Aborting),
        (State::Active, Event::Establish | Event::EstablishCompleted) => {
            ContextTransition::Idempotent
        }
        (State::Releasing, Event::ReleaseCompleted) => {
            ContextTransition::Apply(State::TerminalRetained)
        }
        (State::Releasing, Event::Release) => ContextTransition::Idempotent,
        (State::Releasing, Event::Abort) => ContextTransition::LostToRelease,
        (State::Aborting, Event::AbortCompleted) => {
            ContextTransition::Apply(State::TerminalRetained)
        }
        (State::Aborting, Event::Abort) => ContextTransition::Idempotent,
        (State::TerminalRetained, Event::Reap) => ContextTransition::Apply(State::Gone),
        (State::TerminalRetained | State::Gone, _) => ContextTransition::AlreadyTerminal,
        _ => ContextTransition::Illegal,
    }
}

pub const fn context_state_is_closed(state: QueryContextState) -> bool {
    matches!(
        state,
        QueryContextState::Releasing
            | QueryContextState::Aborting
            | QueryContextState::TerminalRetained
            | QueryContextState::Gone
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_coordinator_context_path_is_closed_explicitly() {
        assert_eq!(
            classify_context_transition(QueryContextState::Active, QueryContextEvent::Release),
            ContextTransition::Apply(QueryContextState::Releasing)
        );
        assert!(context_state_is_closed(QueryContextState::Releasing));
    }
}
