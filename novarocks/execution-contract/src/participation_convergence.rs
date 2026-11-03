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

//! Exact, versioned worker participation convergence observations.

use std::fmt;
use std::num::NonZeroU64;

use crate::identity::ParticipationRef;

/// Monotonic version of one worker participation convergence observation.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ParticipationConvergenceVersion(NonZeroU64);

/// Why a convergence version is not representable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZeroParticipationConvergenceVersion;

impl fmt::Display for ZeroParticipationConvergenceVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("participation convergence version must be nonzero")
    }
}

impl std::error::Error for ZeroParticipationConvergenceVersion {}

impl ParticipationConvergenceVersion {
    /// The first convergence observation a worker publishes for a participation.
    pub const FIRST: Self = Self(NonZeroU64::new(1).expect("one is nonzero"));

    pub fn new(value: u64) -> Result<Self, ZeroParticipationConvergenceVersion> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ZeroParticipationConvergenceVersion)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// The next version, or `None` when this version exhausted the `u64` space.
    pub const fn next(self) -> Option<Self> {
        match self.0.get().checked_add(1) {
            Some(value) => match NonZeroU64::new(value) {
                Some(value) => Some(Self(value)),
                None => None,
            },
            None => None,
        }
    }
}

impl fmt::Display for ParticipationConvergenceVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(formatter)
    }
}

/// Closed worker-owned convergence fact for one exact participation.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ParticipationConvergenceState {
    /// Every task stopped and the participation rejects further admission.
    WorkerStoppedAndParticipationFenced,
}

/// One immutable convergence observation for an exact worker participation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ParticipationConvergenceReceipt {
    participation: ParticipationRef,
    version: ParticipationConvergenceVersion,
    state: ParticipationConvergenceState,
}

impl ParticipationConvergenceReceipt {
    pub const fn new(
        participation: ParticipationRef,
        version: ParticipationConvergenceVersion,
        state: ParticipationConvergenceState,
    ) -> Self {
        Self {
            participation,
            version,
            state,
        }
    }

    pub const fn participation(self) -> ParticipationRef {
        self.participation
    }

    pub const fn version(self) -> ParticipationConvergenceVersion {
        self.version
    }

    pub const fn state(self) -> ParticipationConvergenceState {
        self.state
    }
}

/// A frontend's observation position for one exact worker participation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ParticipationConvergenceCursor {
    participation: ParticipationRef,
    current_version: Option<ParticipationConvergenceVersion>,
}

impl ParticipationConvergenceCursor {
    pub const fn unobserved(participation: ParticipationRef) -> Self {
        Self {
            participation,
            current_version: None,
        }
    }

    pub const fn at(
        participation: ParticipationRef,
        version: ParticipationConvergenceVersion,
    ) -> Self {
        Self {
            participation,
            current_version: Some(version),
        }
    }

    pub const fn participation(self) -> ParticipationRef {
        self.participation
    }

    pub const fn current_version(self) -> Option<ParticipationConvergenceVersion> {
        self.current_version
    }

    pub const fn advanced_to(self, version: ParticipationConvergenceVersion) -> Self {
        Self {
            participation: self.participation,
            current_version: Some(version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_nonzero_and_advance_without_wrapping() {
        assert_eq!(ParticipationConvergenceVersion::FIRST.get(), 1);
        assert_eq!(
            ParticipationConvergenceVersion::FIRST
                .next()
                .expect("version two")
                .get(),
            2
        );
        assert!(ParticipationConvergenceVersion::new(0).is_err());
        assert!(
            ParticipationConvergenceVersion::new(u64::MAX)
                .expect("maximum is nonzero")
                .next()
                .is_none()
        );
    }
}
