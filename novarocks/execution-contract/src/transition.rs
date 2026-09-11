// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Cross-process query-context lifecycle facts.
//!
//! This module intentionally carries only the state observed across the Task
//! protocol. Worker transition policy and query coordination policy belong to
//! their respective application owners.

/// Lifecycle state of one query context.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum QueryContextState {
    /// No context has been installed for this exact identity.
    Absent,
    /// The creation gate owns the identity while shared facts are installed.
    Establishing,
    /// The context accepts normal task and domain operations.
    Active,
    /// Normal release is converging.
    Releasing,
    /// Forced termination is converging.
    Aborting,
    /// A terminal receipt and retirement fence remain observable.
    TerminalRetained,
    /// Retention ended; the exact identity cannot be recreated.
    Gone,
}

impl QueryContextState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "ABSENT",
            Self::Establishing => "ESTABLISHING",
            Self::Active => "ACTIVE",
            Self::Releasing => "RELEASING",
            Self::Aborting => "ABORTING",
            Self::TerminalRetained => "TERMINAL_RETAINED",
            Self::Gone => "GONE",
        }
    }
}

impl std::fmt::Display for QueryContextState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::QueryContextState;

    #[test]
    fn every_wire_state_has_a_stable_name() {
        assert_eq!(QueryContextState::Absent.as_str(), "ABSENT");
        assert_eq!(QueryContextState::Establishing.as_str(), "ESTABLISHING");
        assert_eq!(QueryContextState::Active.as_str(), "ACTIVE");
        assert_eq!(QueryContextState::Releasing.as_str(), "RELEASING");
        assert_eq!(QueryContextState::Aborting.as_str(), "ABORTING");
        assert_eq!(
            QueryContextState::TerminalRetained.as_str(),
            "TERMINAL_RETAINED"
        );
        assert_eq!(QueryContextState::Gone.as_str(), "GONE");
    }
}
