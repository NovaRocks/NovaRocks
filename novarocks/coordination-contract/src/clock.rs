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

/// The single process-local timeline shared by execution and Task owners.
/// Reading time grants no admission or transition authority.
pub trait MonotonicClock: std::fmt::Debug + Send + Sync {
    fn now(&self) -> MonotonicInstant;
}
