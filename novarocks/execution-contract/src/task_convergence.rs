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

//! Exact, versioned observation that one worker task has actually exited.

use std::fmt;
use std::num::NonZeroU64;

use crate::identity::TaskIdentity;

/// This version belongs to the stop observation, not to the terminal status.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TaskConvergenceVersion(NonZeroU64);

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZeroTaskConvergenceVersion;

impl fmt::Display for ZeroTaskConvergenceVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("task convergence version must be nonzero")
    }
}

impl std::error::Error for ZeroTaskConvergenceVersion {}

impl TaskConvergenceVersion {
    pub const FIRST: Self = Self(NonZeroU64::new(1).expect("one is nonzero"));

    pub fn new(value: u64) -> Result<Self, ZeroTaskConvergenceVersion> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ZeroTaskConvergenceVersion)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

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

/// A worker-owned fact for one exact Task identity.
///
/// Publication requires all task-owned preparation, drivers, sends, writers,
/// and cleanup to have exited. Terminal status and a stop request alone do
/// not establish this fact. The observation is retained for reconnect replay.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TaskConvergenceReceipt {
    identity: TaskIdentity,
    version: TaskConvergenceVersion,
}

impl TaskConvergenceReceipt {
    pub const fn actual_stopped(identity: TaskIdentity, version: TaskConvergenceVersion) -> Self {
        Self { identity, version }
    }

    pub const fn identity(self) -> TaskIdentity {
        self.identity
    }

    pub const fn version(self) -> TaskConvergenceVersion {
        self.version
    }
}

/// A frontend's replay position for one exact Task identity.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TaskConvergenceCursor {
    identity: TaskIdentity,
    current_version: Option<TaskConvergenceVersion>,
}

impl TaskConvergenceCursor {
    pub const fn unobserved(identity: TaskIdentity) -> Self {
        Self {
            identity,
            current_version: None,
        }
    }

    pub const fn at(identity: TaskIdentity, version: TaskConvergenceVersion) -> Self {
        Self {
            identity,
            current_version: Some(version),
        }
    }

    pub const fn identity(self) -> TaskIdentity {
        self.identity
    }

    pub const fn current_version(self) -> Option<TaskConvergenceVersion> {
        self.current_version
    }

    pub const fn advanced_to(self, version: TaskConvergenceVersion) -> Self {
        Self::at(self.identity, version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_nonzero_and_do_not_wrap() {
        assert_eq!(TaskConvergenceVersion::FIRST.get(), 1);
        assert_eq!(TaskConvergenceVersion::FIRST.next().unwrap().get(), 2);
        assert!(TaskConvergenceVersion::new(0).is_err());
        assert!(
            TaskConvergenceVersion::new(u64::MAX)
                .unwrap()
                .next()
                .is_none()
        );
    }
}
