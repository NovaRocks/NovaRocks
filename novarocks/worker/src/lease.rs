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

use std::time::Duration;

use novarocks_execution_contract::{LeaseReceipt, LeaseSequence, LeaseValidFor};

/// A reading on the opaque monotonic timeline owned by this worker process.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MonotonicInstant(Duration);

impl MonotonicInstant {
    pub const ORIGIN: Self = Self(Duration::ZERO);

    pub const fn from_origin(elapsed: Duration) -> Self {
        Self(elapsed)
    }

    pub const fn since_origin(self) -> Duration {
        self.0
    }

    pub fn saturating_add(self, delta: Duration) -> Self {
        Self(self.0.saturating_add(delta))
    }

    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }

    pub fn has_reached(self, deadline: Self) -> bool {
        self >= deadline
    }
}

/// The lease duration range enforced by one worker.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct LeaseBounds {
    min: Duration,
    max: Duration,
}

impl LeaseBounds {
    pub const DEFAULT: Self = Self {
        min: Duration::from_secs(1),
        max: Duration::from_secs(30),
    };

    /// Initial lease requested while shared context facts materialize.
    pub const INITIAL_REQUEST: Duration = Duration::from_secs(30);

    /// Steady-state lease requested by the frontend owner.
    pub const STEADY_REQUEST: Duration = Duration::from_secs(5);

    pub fn new(min: Duration, max: Duration) -> Option<Self> {
        if min.is_zero() || max < min {
            return None;
        }
        Some(Self { min, max })
    }

    pub const fn min(self) -> Duration {
        self.min
    }

    pub const fn max(self) -> Duration {
        self.max
    }

    pub fn clamp(self, requested: LeaseValidFor) -> Duration {
        requested.get().clamp(self.min, self.max)
    }
}

/// How the worker classifies one lease operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum LeaseProgression {
    Apply {
        receipt: LeaseReceipt,
    },
    Idempotent {
        receipt: LeaseReceipt,
    },
    Conflict {
        accepted: LeaseReceipt,
    },
    Stale {
        accepted: LeaseReceipt,
    },
    Gap {
        accepted: LeaseSequence,
        received: LeaseSequence,
    },
}

/// The lease currently installed in one worker-local query context.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct InstalledLease {
    receipt: LeaseReceipt,
    expires_at: MonotonicInstant,
}

impl InstalledLease {
    /// Installs sequence zero at the context creation linearization point.
    pub fn install_initial(
        requested: LeaseValidFor,
        bounds: LeaseBounds,
        now: MonotonicInstant,
    ) -> Self {
        let effective = bounds.clamp(requested);
        Self {
            receipt: LeaseReceipt::new(LeaseSequence::INITIAL, requested, effective),
            expires_at: now.saturating_add(effective),
        }
    }

    pub const fn receipt(self) -> LeaseReceipt {
        self.receipt
    }

    pub const fn expires_at(self) -> MonotonicInstant {
        self.expires_at
    }

    pub const fn sequence(self) -> LeaseSequence {
        self.receipt.sequence()
    }

    pub fn is_expired_at(self, now: MonotonicInstant) -> bool {
        now.has_reached(self.expires_at)
    }

    /// Classifies without extending or mutating the installed lease.
    pub fn classify_renewal(
        self,
        sequence: LeaseSequence,
        requested: LeaseValidFor,
    ) -> LeaseProgression {
        let accepted = self.receipt;
        if sequence == accepted.sequence() {
            return if requested == accepted.requested_valid_for() {
                LeaseProgression::Idempotent { receipt: accepted }
            } else {
                LeaseProgression::Conflict { accepted }
            };
        }
        if sequence < accepted.sequence() {
            return LeaseProgression::Stale { accepted };
        }
        match accepted.sequence().next() {
            Some(next) if next == sequence => LeaseProgression::Apply {
                receipt: LeaseReceipt::new(sequence, requested, Duration::ZERO),
            },
            _ => LeaseProgression::Gap {
                accepted: accepted.sequence(),
                received: sequence,
            },
        }
    }

    /// Applies a renewal already classified as [`LeaseProgression::Apply`].
    pub fn renew(
        self,
        sequence: LeaseSequence,
        requested: LeaseValidFor,
        bounds: LeaseBounds,
        now: MonotonicInstant,
    ) -> Self {
        let effective = bounds.clamp(requested);
        Self {
            receipt: LeaseReceipt::new(sequence, requested, effective),
            expires_at: now.saturating_add(effective),
        }
    }
}

/// The maximum legal arrival horizon retained by one worker.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RequestHorizon {
    frontend_queue_residence: Duration,
    server_wait: Duration,
    unknown_outcome_retry: Duration,
    observation_budget: Duration,
    safety_margin: Duration,
}

impl RequestHorizon {
    pub const DEFAULT: Self = Self {
        frontend_queue_residence: Duration::from_secs(15),
        server_wait: Duration::from_secs(15),
        unknown_outcome_retry: Duration::from_secs(30),
        observation_budget: Duration::from_secs(30),
        safety_margin: Duration::from_secs(30),
    };

    pub fn total(self) -> Duration {
        self.frontend_queue_residence
            .saturating_add(self.server_wait)
            .saturating_add(self.unknown_outcome_retry)
            .saturating_add(self.observation_budget)
            .saturating_add(self.safety_margin)
    }

    pub const fn frontend_queue_residence(self) -> Duration {
        self.frontend_queue_residence
    }

    pub const fn server_wait(self) -> Duration {
        self.server_wait
    }

    pub const fn unknown_outcome_retry(self) -> Duration {
        self.unknown_outcome_retry
    }

    pub const fn observation_budget(self) -> Duration {
        self.observation_budget
    }

    pub const fn safety_margin(self) -> Duration {
        self.safety_margin
    }

    pub fn retention_deadline(self, retired_at: MonotonicInstant) -> MonotonicInstant {
        retired_at.saturating_add(self.total())
    }

    pub fn must_retain_at(self, retired_at: MonotonicInstant, now: MonotonicInstant) -> bool {
        !now.has_reached(self.retention_deadline(retired_at))
    }

    /// Whether this worker can still accept a legal request for the retired
    /// record. Beyond this point it may answer `Gone`.
    pub fn request_is_legal_at(self, retired_at: MonotonicInstant, now: MonotonicInstant) -> bool {
        self.must_retain_at(retired_at, now)
    }
}

#[cfg(test)]
mod tests {
    use super::{InstalledLease, LeaseBounds, LeaseProgression, MonotonicInstant, RequestHorizon};
    use novarocks_execution_contract::{LeaseSequence, LeaseValidFor};
    use std::time::Duration;

    fn valid_for(seconds: u64) -> LeaseValidFor {
        LeaseValidFor::new(Duration::from_secs(seconds)).expect("representable duration")
    }

    fn at(seconds: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_secs(seconds))
    }

    #[test]
    fn initial_lease_uses_worker_bounds_and_worker_time() {
        let lease = InstalledLease::install_initial(valid_for(60), LeaseBounds::DEFAULT, at(10));
        assert_eq!(
            lease.receipt().effective_valid_for(),
            Duration::from_secs(30)
        );
        assert_eq!(lease.expires_at(), at(40));
        assert!(!lease.is_expired_at(at(39)));
        assert!(lease.is_expired_at(at(40)));
    }

    #[test]
    fn renewal_accepts_only_the_exact_next_sequence() {
        let lease = InstalledLease::install_initial(valid_for(5), LeaseBounds::DEFAULT, at(0));
        assert!(matches!(
            lease.classify_renewal(LeaseSequence::new(1), valid_for(5)),
            LeaseProgression::Apply { .. }
        ));
        assert!(matches!(
            lease.classify_renewal(LeaseSequence::INITIAL, valid_for(5)),
            LeaseProgression::Idempotent { .. }
        ));
        assert!(matches!(
            lease.classify_renewal(LeaseSequence::INITIAL, valid_for(6)),
            LeaseProgression::Conflict { .. }
        ));
        assert!(matches!(
            lease.classify_renewal(LeaseSequence::new(2), valid_for(5)),
            LeaseProgression::Gap { .. }
        ));
    }

    #[test]
    fn a_stale_renewal_cannot_extend_the_timer() {
        let initial = InstalledLease::install_initial(valid_for(5), LeaseBounds::DEFAULT, at(0));
        let renewed = initial.renew(
            LeaseSequence::new(1),
            valid_for(5),
            LeaseBounds::DEFAULT,
            at(2),
        );
        assert_eq!(renewed.expires_at(), at(7));
        assert!(matches!(
            renewed.classify_renewal(LeaseSequence::INITIAL, valid_for(5)),
            LeaseProgression::Stale { .. }
        ));
        assert_eq!(renewed.expires_at(), at(7));
    }

    #[test]
    fn request_horizon_keeps_the_boundary_exclusive() {
        let horizon = RequestHorizon::DEFAULT;
        assert_eq!(horizon.total(), Duration::from_secs(120));
        assert!(horizon.must_retain_at(at(10), at(129)));
        assert!(!horizon.must_retain_at(at(10), at(130)));
    }
}
