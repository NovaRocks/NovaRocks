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

/// Lease durations requested by query coordination.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RequestedLeaseDurations {
    initial: Duration,
    steady: Duration,
}

impl RequestedLeaseDurations {
    pub const DEFAULT: Self = Self {
        initial: Duration::from_secs(30),
        steady: Duration::from_secs(5),
    };

    pub const fn new(initial: Duration, steady: Duration) -> Option<Self> {
        if initial.is_zero() || steady.is_zero() {
            None
        } else {
            Some(Self { initial, steady })
        }
    }

    pub const fn initial(self) -> Duration {
        self.initial
    }

    pub const fn steady(self) -> Duration {
        self.steady
    }
}

/// A reading on the query application's process-local monotonic timeline.
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RenewSchedule {
    conservative_expiry: MonotonicInstant,
    next_renew_at: MonotonicInstant,
}

impl RenewSchedule {
    pub const SAFETY_MARGIN: Duration = Duration::from_secs(2);

    pub fn after(request_sent_at: MonotonicInstant, effective_valid_for: Duration) -> Self {
        let conservative_expiry = request_sent_at.saturating_add(effective_valid_for);
        let first_third = request_sent_at.saturating_add(effective_valid_for / 3);
        let latest_safe = MonotonicInstant::from_origin(
            conservative_expiry
                .since_origin()
                .saturating_sub(Self::SAFETY_MARGIN),
        );
        Self {
            conservative_expiry,
            next_renew_at: first_third.min(latest_safe),
        }
    }

    pub const fn conservative_expiry(self) -> MonotonicInstant {
        self.conservative_expiry
    }
    pub const fn next_renew_at(self) -> MonotonicInstant {
        self.next_renew_at
    }
    pub fn must_renew_at(self, now: MonotonicInstant) -> bool {
        now.has_reached(self.next_renew_at)
    }
    pub fn delay_from(self, now: MonotonicInstant) -> Duration {
        self.next_renew_at.saturating_duration_since(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renewal_uses_send_time_and_effective_duration() {
        let sent = MonotonicInstant::from_origin(Duration::from_secs(10));
        let schedule = RenewSchedule::after(sent, Duration::from_secs(9));
        assert_eq!(
            schedule.next_renew_at().since_origin(),
            Duration::from_secs(13)
        );
        assert_eq!(
            schedule.conservative_expiry().since_origin(),
            Duration::from_secs(19)
        );
    }
}
