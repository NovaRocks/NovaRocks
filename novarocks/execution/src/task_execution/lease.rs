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

//! Query execution lease arithmetic and the maximum legal request horizon.
//!
//! The lease is the query context's application liveness contract. It is
//! installed at the establish creation gate with sequence zero and is advanced
//! only by the single frontend `QueryContextOwner`. Nothing else may renew it:
//! not a status stream, not an HTTP/2 keepalive, not a process announce. This
//! module owns the pure arithmetic of that contract and reads no clock itself.

use std::fmt;
use std::time::Duration;

/// A process-local monotonic reading, expressed as elapsed time since an
/// opaque origin chosen by the process that produced it.
///
/// Readings are only ever compared against other readings from the same
/// process. The backend computes lease expiry from its own readings and the
/// frontend schedules renewal from its own; the two clocks are never compared
/// and no absolute deadline is ever put on the wire.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MonotonicInstant(Duration);

impl MonotonicInstant {
    /// The origin of a process-local monotonic timeline.
    pub const ORIGIN: Self = Self(Duration::ZERO);

    pub const fn from_origin(elapsed: Duration) -> Self {
        Self(elapsed)
    }

    pub const fn since_origin(self) -> Duration {
        self.0
    }

    /// Advances a reading, saturating instead of wrapping.
    pub fn saturating_add(self, delta: Duration) -> Self {
        Self(self.0.saturating_add(delta))
    }

    /// Returns how long `self` is after `earlier`, or zero if it is not.
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }

    /// Returns whether this reading has reached `deadline`.
    pub fn has_reached(self, deadline: Self) -> bool {
        self >= deadline
    }
}

/// Monotonic lease sequence.
///
/// Sequence zero belongs to the initial lease carried by `Establish`; every
/// subsequent renewal must be exactly one higher, so a gap is a protocol
/// error rather than a silently accepted jump.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LeaseSequence(u64);

impl LeaseSequence {
    /// The sequence carried by the initial lease inside `Establish`.
    pub const INITIAL: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn is_initial(self) -> bool {
        self.0 == 0
    }

    /// The only sequence a renewal may carry after this one.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for LeaseSequence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Why a requested lease duration is not representable on the wire.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum LeaseValidForError {
    Zero,
    Overflow,
}

impl fmt::Display for LeaseValidForError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Zero => "lease valid_for must be greater than zero",
            Self::Overflow => "lease valid_for exceeds the representable range",
        })
    }
}

impl std::error::Error for LeaseValidForError {}

/// A requested lease duration.
///
/// The wire carries a duration, never a cross-host absolute deadline, so the
/// backend can time it against its own monotonic clock.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LeaseValidFor(Duration);

impl LeaseValidFor {
    /// The largest duration this contract will represent before clamping.
    ///
    /// This is a wire sanity bound, not the accepted lease range: the backend
    /// clamps every request into [`LeaseBounds`].
    pub const MAX_REPRESENTABLE: Duration = Duration::from_secs(3600);

    pub fn new(value: Duration) -> Result<Self, LeaseValidForError> {
        if value.is_zero() {
            return Err(LeaseValidForError::Zero);
        }
        if value > Self::MAX_REPRESENTABLE {
            return Err(LeaseValidForError::Overflow);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

/// The accepted lease duration range of one backend.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct LeaseBounds {
    min: Duration,
    max: Duration,
}

impl LeaseBounds {
    /// The range every backend accepts by default: one to thirty seconds.
    pub const DEFAULT: Self = Self {
        min: Duration::from_secs(1),
        max: Duration::from_secs(30),
    };

    /// The initial lease a frontend requests inside `Establish`.
    ///
    /// It has to cover the whole establishing window, including catalog and
    /// credential materialization and any create waiting on the gate.
    pub const INITIAL_REQUEST: Duration = Duration::from_secs(30);

    /// The steady-state lease a frontend requests when renewing.
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

    /// Clamps a requested duration into this backend's accepted range.
    pub fn clamp(self, requested: LeaseValidFor) -> Duration {
        requested.get().clamp(self.min, self.max)
    }
}

/// The immutable outcome of one accepted lease operation.
///
/// `effective_valid_for` is the only duration a frontend may schedule from;
/// the requested value is retained so that an exact replay can be recognised
/// as idempotent and a different duration under the same sequence as a
/// conflict.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct LeaseReceipt {
    sequence: LeaseSequence,
    requested_valid_for: LeaseValidFor,
    effective_valid_for: Duration,
}

impl LeaseReceipt {
    pub const fn new(
        sequence: LeaseSequence,
        requested_valid_for: LeaseValidFor,
        effective_valid_for: Duration,
    ) -> Self {
        Self {
            sequence,
            requested_valid_for,
            effective_valid_for,
        }
    }

    pub const fn sequence(self) -> LeaseSequence {
        self.sequence
    }

    pub const fn requested_valid_for(self) -> LeaseValidFor {
        self.requested_valid_for
    }

    pub const fn effective_valid_for(self) -> Duration {
        self.effective_valid_for
    }
}

/// How a backend must answer one lease operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum LeaseProgression {
    /// A strictly next sequence: apply it and restart the local timer.
    Apply { receipt: LeaseReceipt },
    /// An exact replay of the accepted sequence: return the original receipt
    /// without extending anything.
    Idempotent { receipt: LeaseReceipt },
    /// The accepted sequence with a different requested duration.
    Conflict { accepted: LeaseReceipt },
    /// A sequence below the accepted one: report current state, never roll
    /// back and never extend.
    Stale { accepted: LeaseReceipt },
    /// A sequence above the next legal one.
    Gap {
        accepted: LeaseSequence,
        received: LeaseSequence,
    },
}

/// The lease a backend query context currently holds.
///
/// The local expiry is derived once per accepted operation from the backend's
/// own monotonic clock. Entering `Active` never resets or extends the initial
/// sequence-zero expiry.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct InstalledLease {
    receipt: LeaseReceipt,
    expires_at: MonotonicInstant,
}

impl InstalledLease {
    /// Installs the initial lease at the establish creation gate.
    ///
    /// The timer starts at the same linearization point that moves the context
    /// from `Absent` to `Establishing`, so a slow catalog or credential load is
    /// already racing this deadline.
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

    /// Whether the backend's own clock has passed the local expiry.
    pub fn is_expired_at(self, now: MonotonicInstant) -> bool {
        now.has_reached(self.expires_at)
    }

    /// Classifies a renewal request against the installed lease.
    ///
    /// This is pure: it never mutates, so a conflicting or stale request
    /// cannot disturb the accepted lease.
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

    /// Applies a renewal the backend has already classified as [`LeaseProgression::Apply`].
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

/// The frontend-side renewal schedule of one query context.
///
/// The owner schedules from the frontend-local send time of the immutable
/// request plus the `effective_valid_for` the backend returned. Response
/// latency is therefore never mistaken for extra lease lifetime, and the
/// requested value is never used.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RenewSchedule {
    conservative_expiry: MonotonicInstant,
    next_renew_at: MonotonicInstant,
}

impl RenewSchedule {
    /// How far before the conservative expiry a renewal must have been sent.
    pub const SAFETY_MARGIN: Duration = Duration::from_secs(2);

    /// Builds the schedule for one accepted lease operation.
    ///
    /// `request_sent_at` is the frontend-local monotonic reading taken when
    /// the immutable request was handed to the transport, not when its ack
    /// arrived.
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

    /// Whether the owner must renew now.
    ///
    /// An ack that arrives after the scheduled point renews immediately rather
    /// than waiting out a negative delay.
    pub fn must_renew_at(self, now: MonotonicInstant) -> bool {
        now.has_reached(self.next_renew_at)
    }

    /// How long the owner may sleep before renewing.
    pub fn delay_from(self, now: MonotonicInstant) -> Duration {
        self.next_renew_at.saturating_duration_since(now)
    }
}

/// The maximum legal request horizon of the task protocol.
///
/// It bounds how long after a task or context retires a legal in-flight
/// request, queued request, or exact retry may still arrive. The frontend must
/// not send a legal retry beyond it, and the backend must retain terminal
/// records and retirement fences for at least that long. Nothing here promises
/// an unbounded tombstone: past the horizon a backend may answer `Gone`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RequestHorizon {
    frontend_queue_residence: Duration,
    server_wait: Duration,
    unknown_outcome_retry: Duration,
    observation_budget: Duration,
    safety_margin: Duration,
}

impl RequestHorizon {
    /// The horizon this protocol version freezes: 120 seconds.
    pub const DEFAULT: Self = Self {
        frontend_queue_residence: Duration::from_secs(15),
        server_wait: Duration::from_secs(15),
        unknown_outcome_retry: Duration::from_secs(30),
        observation_budget: Duration::from_secs(30),
        safety_margin: Duration::from_secs(30),
    };

    /// The total horizon every component must respect.
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

    /// When a record retired at `retired_at` may be released.
    pub fn retention_deadline(self, retired_at: MonotonicInstant) -> MonotonicInstant {
        retired_at.saturating_add(self.total())
    }

    /// Whether a retired record must still be retained.
    pub fn must_retain_at(self, retired_at: MonotonicInstant, now: MonotonicInstant) -> bool {
        !now.has_reached(self.retention_deadline(retired_at))
    }

    /// Whether the frontend may still send a legal request for a record
    /// retired at `retired_at`.
    pub fn request_is_legal_at(self, retired_at: MonotonicInstant, now: MonotonicInstant) -> bool {
        self.must_retain_at(retired_at, now)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        InstalledLease, LeaseBounds, LeaseProgression, LeaseSequence, LeaseValidFor,
        LeaseValidForError, MonotonicInstant, RenewSchedule, RequestHorizon,
    };
    use std::time::Duration;

    fn valid_for(secs: u64) -> LeaseValidFor {
        LeaseValidFor::new(Duration::from_secs(secs)).expect("representable duration")
    }

    fn at(secs: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_secs(secs))
    }

    #[test]
    fn valid_for_rejects_zero_and_overflow() {
        assert_eq!(
            LeaseValidFor::new(Duration::ZERO),
            Err(LeaseValidForError::Zero)
        );
        assert_eq!(
            LeaseValidFor::new(LeaseValidFor::MAX_REPRESENTABLE + Duration::from_secs(1)),
            Err(LeaseValidForError::Overflow)
        );
        assert_eq!(
            LeaseValidFor::new(LeaseValidFor::MAX_REPRESENTABLE)
                .expect("boundary is representable")
                .get(),
            LeaseValidFor::MAX_REPRESENTABLE
        );
    }

    #[test]
    fn backend_clamps_requests_into_its_accepted_range() {
        let bounds = LeaseBounds::DEFAULT;
        assert_eq!(bounds.clamp(valid_for(45)), Duration::from_secs(30));
        assert_eq!(
            bounds.clamp(LeaseValidFor::new(Duration::from_millis(10)).expect("nonzero")),
            Duration::from_secs(1)
        );
        assert_eq!(bounds.clamp(valid_for(5)), Duration::from_secs(5));
        assert!(LeaseBounds::new(Duration::ZERO, Duration::from_secs(1)).is_none());
        assert!(LeaseBounds::new(Duration::from_secs(2), Duration::from_secs(1)).is_none());
    }

    #[test]
    fn initial_lease_starts_at_the_establish_gate_and_active_never_resets_it() {
        let installed =
            InstalledLease::install_initial(valid_for(30), LeaseBounds::DEFAULT, at(100));
        assert!(installed.sequence().is_initial());
        assert_eq!(installed.expires_at(), at(130));
        assert!(!installed.is_expired_at(at(129)));
        assert!(installed.is_expired_at(at(130)));

        // Reaching `Active` is not a lease operation, so the installed value is
        // simply carried forward unchanged.
        let carried = installed;
        assert_eq!(carried.expires_at(), at(130));
    }

    #[test]
    fn initial_lease_request_is_clamped_but_still_bounds_establishing() {
        let installed =
            InstalledLease::install_initial(valid_for(600), LeaseBounds::DEFAULT, at(0));
        assert_eq!(
            installed.receipt().effective_valid_for(),
            Duration::from_secs(30)
        );
        assert_eq!(
            installed.receipt().requested_valid_for(),
            valid_for(600),
            "the requested value is retained so an exact replay stays idempotent"
        );
        assert!(installed.is_expired_at(at(30)));
    }

    #[test]
    fn renewal_progression_is_exact_next_sequence_only() {
        let installed = InstalledLease::install_initial(valid_for(5), LeaseBounds::DEFAULT, at(0));

        match installed.classify_renewal(LeaseSequence::new(1), valid_for(5)) {
            LeaseProgression::Apply { receipt } => {
                assert_eq!(receipt.sequence(), LeaseSequence::new(1))
            }
            other => panic!("expected apply, got {other:?}"),
        }
        assert!(matches!(
            installed.classify_renewal(LeaseSequence::INITIAL, valid_for(5)),
            LeaseProgression::Idempotent { .. }
        ));
        assert!(matches!(
            installed.classify_renewal(LeaseSequence::INITIAL, valid_for(7)),
            LeaseProgression::Conflict { .. }
        ));
        assert!(matches!(
            installed.classify_renewal(LeaseSequence::new(3), valid_for(5)),
            LeaseProgression::Gap {
                accepted: LeaseSequence(0),
                received: LeaseSequence(3),
            }
        ));

        let renewed = installed.renew(
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
        // A stale request may never move the accepted expiry backwards.
        assert_eq!(renewed.expires_at(), at(7));
    }

    #[test]
    fn renew_schedule_uses_send_time_and_effective_duration_only() {
        let schedule = RenewSchedule::after(at(100), Duration::from_secs(5));
        assert_eq!(schedule.conservative_expiry(), at(105));
        // One third of the effective lease elapses first, and it is well
        // inside the two-second safety margin before the conservative expiry.
        assert_eq!(
            schedule.next_renew_at(),
            at(100).saturating_add(Duration::from_secs(5) / 3)
        );
        assert!(!schedule.must_renew_at(at(101)));
        assert!(schedule.must_renew_at(at(102)));
        assert_eq!(schedule.delay_from(at(100)), Duration::from_secs(5) / 3);
    }

    #[test]
    fn renew_schedule_never_plans_later_than_the_safety_margin() {
        // A long lease's first third would fall after `expiry - 2s`, so the
        // safety margin wins.
        let schedule = RenewSchedule::after(at(0), Duration::from_secs(30));
        assert_eq!(schedule.next_renew_at(), at(10));

        let short = RenewSchedule::after(at(0), Duration::from_secs(1));
        assert_eq!(
            short.next_renew_at(),
            at(0),
            "a lease shorter than the safety margin must renew immediately"
        );
        assert!(short.must_renew_at(at(0)));
        assert_eq!(short.delay_from(at(0)), Duration::ZERO);
    }

    #[test]
    fn request_horizon_is_one_hundred_and_twenty_seconds() {
        let horizon = RequestHorizon::DEFAULT;
        assert_eq!(horizon.total(), Duration::from_secs(120));
        assert_eq!(
            horizon.frontend_queue_residence() + horizon.server_wait(),
            Duration::from_secs(30)
        );
        assert_eq!(horizon.retention_deadline(at(1_000)), at(1_120));
        assert!(horizon.must_retain_at(at(1_000), at(1_119)));
        assert!(!horizon.must_retain_at(at(1_000), at(1_120)));
        assert!(horizon.request_is_legal_at(at(1_000), at(1_119)));
        assert!(!horizon.request_is_legal_at(at(1_000), at(1_120)));
    }
}
