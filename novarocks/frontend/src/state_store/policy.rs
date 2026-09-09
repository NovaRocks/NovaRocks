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

//! How hard this application tries, and for how long.
//!
//! These are application decisions, not storage properties. The store's own
//! per-transaction deadline bounds one physical transaction; it says nothing
//! about how many attempts are worth making or how long the caller is willing
//! to wait overall. Keeping the two apart is why a provider no longer reports
//! an attempt ceiling through its limits.

use std::time::Duration;

/// Attempts a single logical operation may make.
pub const DEFAULT_MAX_ATTEMPTS: usize = 5;

/// Wall-clock budget for one logical operation, covering every attempt,
/// every wait for an in-doubt outcome, and every backoff between them.
pub const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(4);

/// Upper bounds. A configuration may tighten these, never relax them.
pub const MAX_ATTEMPTS_CEILING: usize = 5;
pub const OPERATION_TIMEOUT_CEILING: Duration = Duration::from_secs(4);

/// Backoff before each retry, indexed by attempts already spent.
///
/// Shorter than the operation budget by construction: a backoff is always
/// clamped to whatever remains, so the table can never extend a deadline.
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

/// A validated attempt-and-budget policy.
///
/// Constructed once from configuration and shared by every StateStore
/// consumer, so Catalog, MV and GC cannot drift into different retry rules the
/// way they did when each read the ceiling out of the provider's limits.
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

    /// Backoff to apply after `attempts_spent` attempts have already failed.
    ///
    /// Saturates at the last entry rather than panicking, so tightening
    /// `max_attempts` can never index past the table.
    pub(crate) fn backoff_after(&self, attempts_spent: usize) -> Duration {
        let index = attempts_spent.saturating_sub(1);
        RETRY_BACKOFFS[index.min(RETRY_BACKOFFS.len() - 1)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_accepted_operational_contract() {
        let policy = StateStoreRunPolicy::default();
        assert_eq!(policy.max_attempts(), 5);
        assert_eq!(policy.operation_timeout(), Duration::from_secs(4));
    }

    #[test]
    fn a_policy_may_tighten_but_never_relax() {
        let tightened =
            StateStoreRunPolicy::new(2, Duration::from_millis(500)).expect("tighter policy");
        assert_eq!(tightened.max_attempts(), 2);
        assert_eq!(tightened.operation_timeout(), Duration::from_millis(500));

        for (attempts, timeout) in [
            (0, DEFAULT_OPERATION_TIMEOUT),
            (MAX_ATTEMPTS_CEILING + 1, DEFAULT_OPERATION_TIMEOUT),
        ] {
            assert_eq!(
                StateStoreRunPolicy::new(attempts, timeout)
                    .expect_err("attempts outside the ceiling")
                    .field,
                "max_attempts"
            );
        }
        for timeout in [
            Duration::ZERO,
            OPERATION_TIMEOUT_CEILING + Duration::from_millis(1),
        ] {
            assert_eq!(
                StateStoreRunPolicy::new(DEFAULT_MAX_ATTEMPTS, timeout)
                    .expect_err("budget outside the ceiling")
                    .field,
                "operation_timeout"
            );
        }
    }

    #[test]
    fn the_two_budgets_are_independent() {
        // A storage transaction deadline and an application budget are separate
        // decisions; nothing here derives one from the other, and neither is
        // required to be larger.
        let policy = StateStoreRunPolicy::new(1, Duration::from_secs(4)).expect("policy");
        assert_eq!(policy.max_attempts(), 1);
        assert_eq!(policy.operation_timeout(), Duration::from_secs(4));
    }

    #[test]
    fn backoff_never_indexes_past_the_table() {
        let policy = StateStoreRunPolicy::default();
        // Every reachable attempt count, plus values a tightened policy could
        // never produce, all resolve to a real entry.
        for spent in 0..64 {
            assert!(policy.backoff_after(spent) <= Duration::from_millis(80));
        }
        assert_eq!(policy.backoff_after(1), Duration::from_millis(10));
        assert_eq!(policy.backoff_after(4), Duration::from_millis(80));
    }
}
