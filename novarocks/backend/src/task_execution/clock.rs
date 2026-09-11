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

//! The backend's own monotonic clock, taken as an injected dependency.
//!
//! Every deadline this module owns — the sequence-zero lease expiry, an
//! operation's effective wait, the metric publication throttle, and terminal
//! retention — is derived from readings of one injected clock. Nothing here
//! reads a wall clock or an absolute deadline that crossed the process
//! boundary, so a test drives the whole lifecycle by moving a value.

use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use novarocks_worker::MonotonicInstant;

/// The backend-local monotonic timeline.
pub trait BackendMonotonicClock: fmt::Debug + Send + Sync {
    fn now(&self) -> MonotonicInstant;
}

/// The production clock: readings elapsed from process start.
#[derive(Debug)]
pub struct ProcessMonotonicClock {
    origin: Instant,
}

impl ProcessMonotonicClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for ProcessMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl BackendMonotonicClock for ProcessMonotonicClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(self.origin.elapsed())
    }
}

/// A clock advanced only by its owner.
///
/// It exists so a lease expiry, a create's own deadline, a throttle interval,
/// and a retention horizon can all be crossed exactly, in a chosen order,
/// without sleeping.
#[derive(Debug, Default)]
pub struct ManualClock {
    elapsed: Mutex<Duration>,
}

impl ManualClock {
    pub fn new() -> Self {
        Self {
            elapsed: Mutex::new(Duration::ZERO),
        }
    }

    /// Moves the timeline forward.
    pub fn advance(&self, delta: Duration) {
        let mut elapsed = self.elapsed.lock().expect("manual clock lock");
        *elapsed = elapsed.saturating_add(delta);
    }
}

impl BackendMonotonicClock for ManualClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(*self.elapsed.lock().expect("manual clock lock"))
    }
}
