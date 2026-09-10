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

//! Exact, versioned worker query-context convergence observations.

use std::fmt;
use std::num::NonZeroU64;

use crate::identity::QueryContextRef;

/// Monotonic version of one worker query-context convergence observation.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueryContextConvergenceVersion(NonZeroU64);

/// Why a convergence version is not representable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZeroQueryContextConvergenceVersion;

impl fmt::Display for ZeroQueryContextConvergenceVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("query context convergence version must be nonzero")
    }
}

impl std::error::Error for ZeroQueryContextConvergenceVersion {}

impl QueryContextConvergenceVersion {
    /// The first convergence observation a worker publishes for a context.
    pub const FIRST: Self = Self(NonZeroU64::new(1).expect("one is nonzero"));

    pub fn new(value: u64) -> Result<Self, ZeroQueryContextConvergenceVersion> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ZeroQueryContextConvergenceVersion)
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

impl fmt::Display for QueryContextConvergenceVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(formatter)
    }
}

/// Closed worker-owned convergence fact for one exact query context.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum QueryContextConvergenceState {
    /// Every task stopped and the context rejects further admission.
    WorkerStoppedAndContextFenced,
}

/// One immutable convergence observation for an exact worker context.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct QueryContextConvergenceReceipt {
    context: QueryContextRef,
    version: QueryContextConvergenceVersion,
    state: QueryContextConvergenceState,
}

impl QueryContextConvergenceReceipt {
    pub const fn new(
        context: QueryContextRef,
        version: QueryContextConvergenceVersion,
        state: QueryContextConvergenceState,
    ) -> Self {
        Self {
            context,
            version,
            state,
        }
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }

    pub const fn version(self) -> QueryContextConvergenceVersion {
        self.version
    }

    pub const fn state(self) -> QueryContextConvergenceState {
        self.state
    }
}

/// A frontend's observation position for one exact worker query context.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct QueryContextConvergenceCursor {
    context: QueryContextRef,
    current_version: Option<QueryContextConvergenceVersion>,
}

impl QueryContextConvergenceCursor {
    pub const fn unobserved(context: QueryContextRef) -> Self {
        Self {
            context,
            current_version: None,
        }
    }

    pub const fn at(context: QueryContextRef, version: QueryContextConvergenceVersion) -> Self {
        Self {
            context,
            current_version: Some(version),
        }
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }

    pub const fn current_version(self) -> Option<QueryContextConvergenceVersion> {
        self.current_version
    }

    pub const fn advanced_to(self, version: QueryContextConvergenceVersion) -> Self {
        Self {
            context: self.context,
            current_version: Some(version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_nonzero_and_advance_without_wrapping() {
        assert_eq!(QueryContextConvergenceVersion::FIRST.get(), 1);
        assert_eq!(
            QueryContextConvergenceVersion::FIRST
                .next()
                .expect("version two")
                .get(),
            2
        );
        assert!(QueryContextConvergenceVersion::new(0).is_err());
        assert!(
            QueryContextConvergenceVersion::new(u64::MAX)
                .expect("maximum is nonzero")
                .next()
                .is_none()
        );
    }
}
