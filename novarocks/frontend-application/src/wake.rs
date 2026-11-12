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

//! Notification adapters shared by original Frontend work owners.

use std::sync::Arc;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

/// Wakes the owner when an original input or result becomes ready.
pub trait StatusIntakeWake: std::fmt::Debug + Send + Sync {
    fn wake(&self);
}

/// Test wake retaining the same asynchronous notification as its caller.
/// Register before observing retained state so a racing wake is not lost.
#[derive(Debug, Default)]
#[cfg(test)]
pub struct TestIntakeWake {
    async_signal: tokio::sync::Notify,
}

#[cfg(test)]
impl TestIntakeWake {
    /// The same original wake also follows its owner into async residual
    /// supervision. Register before inspecting state to retain a racing wake.
    pub(crate) fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.async_signal.notified()
    }
}

#[cfg(test)]
impl StatusIntakeWake for TestIntakeWake {
    fn wake(&self) {
        self.async_signal.notify_one();
    }
}

#[cfg(test)]
impl std::task::Wake for TestIntakeWake {
    fn wake(self: Arc<Self>) {
        StatusIntakeWake::wake(self.as_ref());
    }
    fn wake_by_ref(self: &Arc<Self>) {
        StatusIntakeWake::wake(self.as_ref());
    }
}

/// Wakes a runner parked on a `Notify`.
#[derive(Debug)]
pub struct NotifyWake(Arc<tokio::sync::Notify>);

impl NotifyWake {
    pub fn new(notify: Arc<tokio::sync::Notify>) -> Self {
        Self(notify)
    }
}

impl StatusIntakeWake for NotifyWake {
    fn wake(&self) {
        self.0.notify_one();
    }
}

/// Counts wakes instead of parking anything.
///
/// It exists so a test can assert that a callback woke the runner exactly
/// once per publish without waiting on a task scheduler.
#[derive(Debug, Default)]
#[cfg(test)]
pub struct CountingWake(AtomicUsize);

#[cfg(test)]
impl CountingWake {
    pub fn count(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
impl StatusIntakeWake for CountingWake {
    fn wake(&self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
