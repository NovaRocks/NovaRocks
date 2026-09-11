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

//! Application-owned retry and deadline policy for StateStore operations.
//!
//! These limits express how much work the application is willing to spend.
//! They are not provider capabilities and must not be inferred from, widened
//! by, or delegated to a StateStore implementation.

use std::time::Duration;

pub const DEFAULT_MAX_ATTEMPTS: usize = 5;
pub const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(4);
pub const MAX_ATTEMPTS_CEILING: usize = 5;
pub const OPERATION_TIMEOUT_CEILING: Duration = Duration::from_secs(4);

const RETRY_BACKOFFS: [Duration; 4] = [
    Duration::from_millis(10),
    Duration::from_millis(20),
    Duration::from_millis(40),
    Duration::from_millis(80),
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateStoreRunPolicyError {
    pub field: &'static str,
    pub message: &'static str,
}

impl std::fmt::Display for StateStoreRunPolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.field, self.message)
    }
}

impl std::error::Error for StateStoreRunPolicyError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateStoreRunPolicy {
    max_attempts: usize,
    operation_timeout: Duration,
}

impl Default for StateStoreRunPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        }
    }
}

impl StateStoreRunPolicy {
    pub fn new(
        max_attempts: usize,
        operation_timeout: Duration,
    ) -> Result<Self, StateStoreRunPolicyError> {
        if max_attempts == 0 || max_attempts > MAX_ATTEMPTS_CEILING {
            return Err(StateStoreRunPolicyError {
                field: "max_attempts",
                message: "must be between 1 and the built-in attempt ceiling",
            });
        }
        if operation_timeout.is_zero() || operation_timeout > OPERATION_TIMEOUT_CEILING {
            return Err(StateStoreRunPolicyError {
                field: "operation_timeout",
                message: "must be positive and no longer than the built-in budget ceiling",
            });
        }
        Ok(Self {
            max_attempts,
            operation_timeout,
        })
    }

    pub const fn max_attempts(&self) -> usize {
        self.max_attempts
    }

    pub const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    pub(crate) fn backoff_after(&self, attempts_spent: usize) -> Duration {
        let index = attempts_spent.saturating_sub(1);
        RETRY_BACKOFFS[index.min(RETRY_BACKOFFS.len() - 1)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_application_contract() {
        let policy = StateStoreRunPolicy::default();
        assert_eq!(policy.max_attempts(), 5);
        assert_eq!(policy.operation_timeout(), Duration::from_secs(4));
    }

    #[test]
    fn policy_may_tighten_but_never_relax() {
        let policy = StateStoreRunPolicy::new(2, Duration::from_millis(500))
            .expect("policy within the built-in ceilings");
        assert_eq!(policy.max_attempts(), 2);
        assert_eq!(policy.operation_timeout(), Duration::from_millis(500));

        assert!(StateStoreRunPolicy::new(0, DEFAULT_OPERATION_TIMEOUT).is_err());
        assert!(
            StateStoreRunPolicy::new(MAX_ATTEMPTS_CEILING + 1, DEFAULT_OPERATION_TIMEOUT).is_err()
        );
        assert!(StateStoreRunPolicy::new(DEFAULT_MAX_ATTEMPTS, Duration::ZERO).is_err());
        assert!(
            StateStoreRunPolicy::new(
                DEFAULT_MAX_ATTEMPTS,
                OPERATION_TIMEOUT_CEILING + Duration::from_millis(1),
            )
            .is_err()
        );
    }

    #[test]
    fn backoff_is_bounded_for_every_attempt_count() {
        let policy = StateStoreRunPolicy::default();
        for attempts_spent in 0..64 {
            assert!(policy.backoff_after(attempts_spent) <= Duration::from_millis(80));
        }
    }
}
