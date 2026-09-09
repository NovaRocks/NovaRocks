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

//! Wire-safe query execution lease values and receipts.
//!
//! Local clocks, accepted duration policy, installation, renewal scheduling,
//! and retention horizons belong to the Worker or query application. This
//! module carries only values that cross that boundary.

use std::fmt;
use std::time::Duration;

/// Monotonic lease sequence.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LeaseSequence(u64);

impl LeaseSequence {
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

/// A requested lease duration. The wire never carries an absolute deadline.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LeaseValidFor(Duration);

impl LeaseValidFor {
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

/// The immutable outcome of one accepted lease operation.
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

#[cfg(test)]
mod tests {
    use super::{LeaseReceipt, LeaseSequence, LeaseValidFor, LeaseValidForError};
    use std::time::Duration;

    #[test]
    fn lease_duration_rejects_zero_and_overflow() {
        assert_eq!(
            LeaseValidFor::new(Duration::ZERO),
            Err(LeaseValidForError::Zero)
        );
        assert_eq!(
            LeaseValidFor::new(LeaseValidFor::MAX_REPRESENTABLE + Duration::from_millis(1)),
            Err(LeaseValidForError::Overflow)
        );
    }

    #[test]
    fn receipt_preserves_requested_and_effective_values() {
        let requested = LeaseValidFor::new(Duration::from_secs(30)).expect("valid duration");
        let receipt = LeaseReceipt::new(LeaseSequence::INITIAL, requested, Duration::from_secs(10));
        assert_eq!(receipt.sequence(), LeaseSequence::INITIAL);
        assert_eq!(receipt.requested_valid_for(), requested);
        assert_eq!(receipt.effective_valid_for(), Duration::from_secs(10));
        assert_eq!(LeaseSequence::INITIAL.next(), Some(LeaseSequence::new(1)));
    }
}
