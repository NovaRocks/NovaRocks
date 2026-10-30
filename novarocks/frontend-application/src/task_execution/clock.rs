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

//! The injected monotonic clock every task-protocol owner reads.
//!
//! Lease renewal, queue residence, and the request horizon are all timed
//! against process-local monotonic readings. Nothing in this module reads a
//! wall clock or sleeps, and every reading arrives through this trait so a
//! test can advance time without waiting for it.

#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

#[cfg(test)]
use novarocks_coordination_contract::{MonotonicClock, MonotonicInstant};

/// Readings from this process's own monotonic timeline.
#[derive(Debug)]
#[cfg(test)]
pub struct ProcessMonotonicClock {
    origin: Instant,
}

#[cfg(test)]
impl ProcessMonotonicClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

#[cfg(test)]
impl Default for ProcessMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl MonotonicClock for ProcessMonotonicClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(self.origin.elapsed())
    }
}

/// A clock that only moves when a caller moves it.
///
/// It exists so the renewal schedule, the queue residence bound, and the
/// request horizon can be driven to an exact reading instead of being waited
/// out.
#[derive(Debug, Default)]
#[cfg(test)]
pub struct ManualClock {
    elapsed: Mutex<Duration>,
}

#[cfg(test)]
impl ManualClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn advance(&self, delta: Duration) {
        let mut elapsed = self.elapsed.lock().expect("manual clock");
        *elapsed = elapsed.saturating_add(delta);
    }
}

#[cfg(test)]
impl MonotonicClock for ManualClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(*self.elapsed.lock().expect("manual clock"))
    }
}
