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

//! Whether this backend process is draining.
//!
//! Draining is a process fact, not a query-lifecycle fact: SIGTERM makes this
//! BE ineligible for new work while everything already admitted runs to its
//! own terminal state. Two things read it — the heartbeat this BE answers and
//! the announce it sends — and both are answering the same question about the
//! same process.
//!
//! This is not a new abstraction over one flag. Before it there were two:
//! `BackendAnnounceTask` in `application.rs` held its own `Arc<AtomicBool>`
//! for the state it announced, the query-lifecycle registry held a separate
//! flag for the state the heartbeat reported, and `begin_drain` set both.
//! Nothing kept them equal, so a process that set one and not the other would
//! report `Draining` on one surface and `Running` on the other — and the two
//! surfaces are read by the same frontend, about the same process, to make the
//! same admission decision. Collapsing them into one owner removes that
//! possibility instead of layering over it.

use std::sync::atomic::{AtomicBool, Ordering};

/// The single drain flag of one backend process.
#[derive(Debug, Default)]
pub(crate) struct BackendDrainState {
    draining: AtomicBool,
}

impl BackendDrainState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Marks this process ineligible for new work. Draining is one-way: a
    /// process that has begun draining never becomes eligible again, so this
    /// takes no argument and cannot be un-set.
    pub(crate) fn begin_drain(&self) {
        self.draining.store(true, Ordering::Release);
    }

    pub(crate) fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::BackendDrainState;

    #[test]
    fn a_fresh_process_is_not_draining() {
        assert!(!BackendDrainState::new().is_draining());
    }

    #[test]
    fn draining_is_one_way_and_visible_to_every_reader() {
        // Both readers hold the same value, so there is no state in which one
        // reports Draining and the other Running.
        let state = BackendDrainState::new();
        state.begin_drain();
        assert!(state.is_draining());
        state.begin_drain();
        assert!(state.is_draining());
    }
}
