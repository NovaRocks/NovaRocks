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

//! Covered subscription retry policy. A real liveness marker repairs typed
//! connection loss immediately, while the failure streak resets only at an
//! ordinary break of that same stream. Explicit reconciliation skips that reset.

use crate::{error::TaskExecutionError, ids::IdSeed};
use novarocks_coordination_contract::MonotonicInstant;
use std::{num::NonZeroU64, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDisposition {
    Ignored,
    Backoff { until: MonotonicInstant },
    Exhausted { connection_loss: bool },
}

#[derive(Debug)]
pub struct CoveredRetryBudget {
    limit: u32,
    failures: u32,
    typed_connection_loss: bool,
    generation: Option<NonZeroU64>,
    had_liveness: bool,
    generation_active: bool,
    seed: IdSeed,
    participant: u128,
}
impl CoveredRetryBudget {
    pub fn new(limit: u32, seed: IdSeed, participant: u128) -> Result<Self, TaskExecutionError> {
        if limit == 0 {
            return Err(TaskExecutionError::Schedule(
                "covered subscription error budget must be nonzero".into(),
            ));
        }
        Ok(Self {
            limit,
            failures: 0,
            typed_connection_loss: false,
            generation: None,
            had_liveness: false,
            generation_active: false,
            seed,
            participant,
        })
    }
    /// Called both for automatic reconnect and explicit reconciliation. Neither
    /// successful opening nor reconciliation settles the previous live failure
    /// streak. Only the next ordinary break can reset it from this new stream.
    pub fn begin_generation(&mut self, generation: NonZeroU64) -> Result<(), TaskExecutionError> {
        if self.failures >= self.limit || self.generation.is_some_and(|old| generation <= old) {
            return Err(TaskExecutionError::Schedule(
                "covered retry generation is exhausted or regressed".into(),
            ));
        }
        self.generation = Some(generation);
        self.had_liveness = false;
        self.generation_active = true;
        Ok(())
    }
    /// The caller accepts only a first CatchUpComplete or a strictly newer
    /// current-generation Bookmark, including a complete read-state snapshot.
    pub fn liveness(&mut self, generation: NonZeroU64) {
        if self.generation_active && self.generation == Some(generation) {
            self.had_liveness = true;
            self.typed_connection_loss = false;
        }
    }
    /// EOF, a confirmed timeout or an original recoverable transport failure
    /// settles this generation once. Fatal protocol/identity errors bypass this
    /// retry budget and retain their original typed cause.
    pub fn failed(
        &mut self,
        generation: NonZeroU64,
        at: MonotonicInstant,
        observed_connection_loss: bool,
    ) -> RetryDisposition {
        if !self.generation_active || self.generation != Some(generation) {
            return RetryDisposition::Ignored;
        }
        self.generation_active = false;
        if self.had_liveness {
            self.failures = 0;
        }
        self.typed_connection_loss |= observed_connection_loss;
        self.failures = self
            .failures
            .checked_add(1)
            .expect("failure budget stops before overflow");
        if self.failures >= self.limit {
            return RetryDisposition::Exhausted {
                connection_loss: self.typed_connection_loss,
            };
        }
        let base = (100_u64 << (self.failures - 1).min(4)).min(1000);
        let percent = 80 + self.seed.jitter(self.participant, generation.get()) % 41;
        RetryDisposition::Backoff {
            until: at.saturating_add(Duration::from_millis(base * percent / 100)),
        }
    }
    pub const fn failures(&self) -> u32 {
        self.failures
    }
    pub const fn typed_connection_loss(&self) -> bool {
        self.typed_connection_loss
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn generation(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }
    fn at(ms: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_millis(ms))
    }
    fn budget(limit: u32) -> CoveredRetryBudget {
        CoveredRetryBudget::new(limit, IdSeed::new(100, 1), 77).unwrap()
    }
    fn failures(through: u64) -> CoveredRetryBudget {
        let mut budget = budget(8);
        for value in 1..=through {
            budget.begin_generation(generation(value)).unwrap();
            budget.failed(generation(value), at(0), true);
        }
        budget
    }
    #[test]
    fn valid_liveness_repairs_connection_immediately_but_streak_only_on_ordinary_break() {
        let mut budget = failures(3);
        budget.begin_generation(generation(4)).unwrap();
        assert_eq!(budget.failures(), 3);
        assert!(budget.typed_connection_loss());
        budget.liveness(generation(4));
        assert!(!budget.typed_connection_loss());
        assert_eq!(budget.failures(), 3);
        assert!(matches!(
            budget.failed(generation(4), at(0), false),
            RetryDisposition::Backoff { .. }
        ));
        assert_eq!(budget.failures(), 1);
    }
    #[test]
    fn reconciliation_bypasses_the_previous_live_streak_reset() {
        let mut budget = failures(3);
        budget.begin_generation(generation(4)).unwrap();
        budget.liveness(generation(4));
        budget.begin_generation(generation(5)).unwrap();
        budget.failed(generation(5), at(0), false);
        assert_eq!(budget.failures(), 4);
        assert!(!budget.typed_connection_loss());
        assert_eq!(
            budget.failed(generation(4), at(0), true),
            RetryDisposition::Ignored
        );
    }
    #[test]
    fn exact_nth_failure_exhausts_once_and_neither_open_nor_old_marker_repairs_it() {
        let mut budget = budget(2);
        budget.begin_generation(generation(1)).unwrap();
        budget.failed(generation(1), at(0), true);
        budget.begin_generation(generation(2)).unwrap();
        budget.liveness(generation(1));
        assert_eq!(
            budget.failed(generation(2), at(100), false),
            RetryDisposition::Exhausted {
                connection_loss: true
            }
        );
        assert_eq!(
            budget.failed(generation(2), at(200), true),
            RetryDisposition::Ignored
        );
        assert!(budget.begin_generation(generation(3)).is_err());
        let mut ordinary = self::budget(1);
        ordinary.begin_generation(generation(1)).unwrap();
        assert_eq!(
            ordinary.failed(generation(1), at(0), false),
            RetryDisposition::Exhausted {
                connection_loss: false
            }
        );
    }
    #[test]
    fn deterministic_exponential_backoff_includes_the_original_twenty_percent_jitter() {
        let mut left = budget(20);
        let mut right = budget(20);
        for value in 1..=12 {
            let generation = generation(value);
            left.begin_generation(generation).unwrap();
            right.begin_generation(generation).unwrap();
            let result = left.failed(generation, at(50), false);
            assert_eq!(result, right.failed(generation, at(50), false));
            let RetryDisposition::Backoff { until } = result else {
                panic!("unexhausted budget");
            };
            let delay = until.saturating_duration_since(at(50)).as_millis();
            let base = (100_u128 << (value - 1).min(4)).min(1000);
            assert!((base * 80 / 100..=base * 120 / 100).contains(&delay));
        }
        assert!(CoveredRetryBudget::new(0, IdSeed::new(1, 1), 0).is_err());
    }
}
