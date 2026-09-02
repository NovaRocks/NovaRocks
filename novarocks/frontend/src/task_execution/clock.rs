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

use std::sync::Mutex;
use std::time::{Duration, Instant};

use novarocks_execution::task_execution::MonotonicInstant;

/// The clock a task-protocol owner reads.
pub trait TaskProtocolClock: std::fmt::Debug + Send + Sync {
    fn now(&self) -> MonotonicInstant;
}

/// Readings from this process's own monotonic timeline.
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

impl TaskProtocolClock for ProcessMonotonicClock {
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
pub struct ManualClock {
    elapsed: Mutex<Duration>,
}

impl ManualClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn advance(&self, delta: Duration) {
        let mut elapsed = self.elapsed.lock().expect("manual clock");
        *elapsed = elapsed.saturating_add(delta);
    }

    pub fn set(&self, elapsed: Duration) {
        *self.elapsed.lock().expect("manual clock") = elapsed;
    }
}

impl TaskProtocolClock for ManualClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(*self.elapsed.lock().expect("manual clock"))
    }
}
