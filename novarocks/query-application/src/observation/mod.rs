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

//! Query-scoped observation accounting.
//!
//! SQL and Connector facts keep their typed representations. This module owns
//! only query-local correlation and one budget shared across all preparation
//! work, including failed optional candidates. Byte counters here are an
//! observed-retention contract: they reject values after an adapter reports a
//! footprint, but do not reserve memory and must not be presented as a hard
//! process-memory guarantee. Production hard capacity comes from WorkScope and
//! the process-local resource authority.

use std::{
    error::Error,
    fmt,
    num::{NonZeroU32, NonZeroUsize},
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObservationId(NonZeroU32);

impl ObservationId {
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Stable identity of one Connector negotiation subject. A compiler may
/// advance the same subject across rounds, but may not request it twice in one
/// round.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NegotiationId(NonZeroU32);

impl NegotiationId {
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationRequirement {
    Required,
    OptionalCandidate,
}

#[derive(Clone, Copy, Debug)]
pub struct PreparationCountLimits {
    max_rounds: NonZeroU32,
    max_observations: NonZeroUsize,
    max_negotiations: NonZeroUsize,
    max_diagnostics: NonZeroUsize,
}

impl PreparationCountLimits {
    pub const fn new(
        max_rounds: NonZeroU32,
        max_observations: NonZeroUsize,
        max_negotiations: NonZeroUsize,
        max_diagnostics: NonZeroUsize,
    ) -> Self {
        Self {
            max_rounds,
            max_observations,
            max_negotiations,
            max_diagnostics,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PreparationByteLimits {
    max_response_bytes: NonZeroUsize,
    max_diagnostic_bytes: NonZeroUsize,
    max_total_bytes: NonZeroUsize,
}

impl PreparationByteLimits {
    pub const fn new(
        max_response_bytes: NonZeroUsize,
        max_diagnostic_bytes: NonZeroUsize,
        max_total_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            max_response_bytes,
            max_diagnostic_bytes,
            max_total_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PreparationLimits {
    max_rounds: NonZeroU32,
    max_observations: NonZeroUsize,
    max_negotiations: NonZeroUsize,
    max_diagnostics: NonZeroUsize,
    max_response_bytes: NonZeroUsize,
    max_diagnostic_bytes: NonZeroUsize,
    max_total_bytes: NonZeroUsize,
    deadline: Instant,
}

impl PreparationLimits {
    pub const fn new(
        counts: PreparationCountLimits,
        bytes: PreparationByteLimits,
        deadline: Instant,
    ) -> Self {
        Self {
            max_rounds: counts.max_rounds,
            max_observations: counts.max_observations,
            max_negotiations: counts.max_negotiations,
            max_diagnostics: counts.max_diagnostics,
            max_response_bytes: bytes.max_response_bytes,
            max_diagnostic_bytes: bytes.max_diagnostic_bytes,
            max_total_bytes: bytes.max_total_bytes,
            deadline,
        }
    }

    pub const fn deadline(self) -> Instant {
        self.deadline
    }

    /// Maximum retained footprint one provider response may add.
    ///
    /// The preparation driver reserves this amount from the process-local
    /// resource authority before invoking the provider, then reconciles the
    /// reservation with the response's reported footprint.
    pub const fn max_response_bytes(self) -> usize {
        self.max_response_bytes.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparationLimit {
    Rounds,
    ObservationRequests,
    NegotiationRequests,
    Diagnostics,
    ResponseItemBytes,
    DiagnosticItemBytes,
    TotalBytes,
    Deadline,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparationBudgetError {
    limit: PreparationLimit,
    actual: usize,
    maximum: usize,
}

impl PreparationBudgetError {
    pub const fn limit(&self) -> PreparationLimit {
        self.limit
    }
}

impl fmt::Display for PreparationBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "query preparation exceeded {:?}: actual {}, maximum {}",
            self.limit, self.actual, self.maximum
        )
    }
}

impl Error for PreparationBudgetError {}

#[derive(Default)]
struct Ledger {
    rounds: usize,
    observations: usize,
    negotiations: usize,
    diagnostics: usize,
    bytes: usize,
}

#[derive(Clone)]
pub struct PreparationBudget {
    limits: PreparationLimits,
    ledger: Arc<Mutex<Ledger>>,
}

impl PreparationBudget {
    pub fn new(limits: PreparationLimits) -> Self {
        Self {
            limits,
            ledger: Arc::new(Mutex::new(Ledger::default())),
        }
    }

    pub fn begin_round(&self) -> Result<(), PreparationBudgetError> {
        self.check_deadline()?;
        self.update_count(PreparationLimit::Rounds, |ledger| &mut ledger.rounds)
    }

    pub fn begin_observation(&self, bytes: usize) -> Result<(), PreparationBudgetError> {
        self.check_deadline()?;
        self.update_count(PreparationLimit::ObservationRequests, |ledger| {
            &mut ledger.observations
        })?;
        self.add_bytes(bytes)
    }

    pub fn begin_negotiation(&self, bytes: usize) -> Result<(), PreparationBudgetError> {
        self.check_deadline()?;
        self.update_count(PreparationLimit::NegotiationRequests, |ledger| {
            &mut ledger.negotiations
        })?;
        self.add_bytes(bytes)
    }

    pub fn record_response(&self, bytes: usize) -> Result<(), PreparationBudgetError> {
        self.check_deadline()?;
        self.check_item_bytes(
            PreparationLimit::ResponseItemBytes,
            bytes,
            self.limits.max_response_bytes.get(),
        )?;
        self.add_bytes(bytes)
    }

    /// Charge the configured accounting weight for a legacy provider result
    /// whose retained heap footprint is unavailable. This is deliberately not
    /// called a reservation: the synchronous legacy call has already returned.
    pub fn charge_unmeasured_response_weight(&self) -> Result<(), PreparationBudgetError> {
        self.record_response(self.limits.max_response_bytes.get())
    }

    /// Account one retained optional-candidate diagnostic. Its message bytes
    /// share the same total-byte ceiling as successful provider facts.
    pub fn record_diagnostic(&self, bytes: usize) -> Result<(), PreparationBudgetError> {
        self.check_deadline()?;
        self.update_count(PreparationLimit::Diagnostics, |ledger| {
            &mut ledger.diagnostics
        })?;
        self.check_item_bytes(
            PreparationLimit::DiagnosticItemBytes,
            bytes,
            self.limits.max_diagnostic_bytes.get(),
        )?;
        self.add_bytes(bytes)
    }

    fn check_deadline(&self) -> Result<(), PreparationBudgetError> {
        if Instant::now() < self.limits.deadline {
            Ok(())
        } else {
            Err(PreparationBudgetError {
                limit: PreparationLimit::Deadline,
                actual: 1,
                maximum: 0,
            })
        }
    }

    fn update_count(
        &self,
        limit: PreparationLimit,
        select: impl FnOnce(&mut Ledger) -> &mut usize,
    ) -> Result<(), PreparationBudgetError> {
        let maximum = match limit {
            PreparationLimit::Rounds => self.limits.max_rounds.get() as usize,
            PreparationLimit::ObservationRequests => self.limits.max_observations.get(),
            PreparationLimit::NegotiationRequests => self.limits.max_negotiations.get(),
            PreparationLimit::Diagnostics => self.limits.max_diagnostics.get(),
            PreparationLimit::ResponseItemBytes
            | PreparationLimit::DiagnosticItemBytes
            | PreparationLimit::TotalBytes
            | PreparationLimit::Deadline => unreachable!(),
        };
        let mut ledger = self.ledger.lock().unwrap();
        let actual = select(&mut ledger);
        *actual = actual.checked_add(1).ok_or(PreparationBudgetError {
            limit,
            actual: usize::MAX,
            maximum,
        })?;
        if *actual <= maximum {
            Ok(())
        } else {
            Err(PreparationBudgetError {
                limit,
                actual: *actual,
                maximum,
            })
        }
    }

    fn add_bytes(&self, bytes: usize) -> Result<(), PreparationBudgetError> {
        let mut ledger = self.ledger.lock().unwrap();
        let maximum = self.limits.max_total_bytes.get();
        ledger.bytes = ledger
            .bytes
            .checked_add(bytes)
            .ok_or(PreparationBudgetError {
                limit: PreparationLimit::TotalBytes,
                actual: usize::MAX,
                maximum,
            })?;
        if ledger.bytes <= maximum {
            Ok(())
        } else {
            Err(PreparationBudgetError {
                limit: PreparationLimit::TotalBytes,
                actual: ledger.bytes,
                maximum,
            })
        }
    }

    fn check_item_bytes(
        &self,
        limit: PreparationLimit,
        actual: usize,
        maximum: usize,
    ) -> Result<(), PreparationBudgetError> {
        if actual <= maximum {
            Ok(())
        } else {
            Err(PreparationBudgetError {
                limit,
                actual,
                maximum,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn limits(bytes: usize, diagnostics: usize, deadline: Instant) -> PreparationLimits {
        PreparationLimits::new(
            PreparationCountLimits::new(NonZeroU32::new(2).unwrap(), nz(2), nz(2), nz(diagnostics)),
            PreparationByteLimits::new(nz(bytes), nz(bytes), nz(bytes)),
            deadline,
        )
    }

    #[test]
    fn failed_optional_diagnostics_share_count_and_byte_limits() {
        let budget = PreparationBudget::new(limits(7, 1, Instant::now() + Duration::from_secs(1)));
        budget.begin_observation(2).unwrap();
        budget.record_diagnostic(5).unwrap();
        let error = budget.record_diagnostic(1).unwrap_err();
        assert_eq!(error.limit(), PreparationLimit::Diagnostics);
    }

    #[test]
    fn request_is_charged_before_a_provider_can_fail() {
        let budget = PreparationBudget::new(limits(3, 1, Instant::now() + Duration::from_secs(1)));
        let error = budget.begin_negotiation(4).unwrap_err();
        assert_eq!(error.limit(), PreparationLimit::TotalBytes);
    }

    #[test]
    fn expired_deadline_rejects_before_accounting_work() {
        let budget =
            PreparationBudget::new(limits(1, 1, Instant::now() - Duration::from_millis(1)));
        assert_eq!(
            budget.begin_round().unwrap_err().limit(),
            PreparationLimit::Deadline
        );
    }

    #[test]
    fn byte_accounting_overflow_fails_closed_at_a_maximum_limit() {
        let budget = PreparationBudget::new(limits(
            usize::MAX,
            1,
            Instant::now() + Duration::from_secs(1),
        ));
        budget.ledger.lock().unwrap().bytes = usize::MAX;
        assert_eq!(
            budget.record_response(1).unwrap_err().limit(),
            PreparationLimit::TotalBytes
        );
    }
}
