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
//! Scan morsel dispatch state.
//!
//! Responsibilities:
//! - Manages scan task queue ownership and work-distribution metadata across scan workers.
//! - Coordinates shared scan progress, in-flight task accounting, and completion signaling.
//!
//! Key exported interfaces:
//! - Types: `SharedScanState`, `ScanDispatchState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::node::scan::ScanMorsel;
use crate::exec::pipeline::scan::morsel::MorselQueueRef;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::novarocks_logging::debug;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

#[derive(Clone)]
/// Shared scan-state payload used by scan dispatch and async scan workers.
pub(super) struct SharedScanState {
    pub(super) dispatch: Arc<OnceLock<Result<Arc<ScanDispatchState>, String>>>,
}

impl SharedScanState {
    pub(super) fn new() -> Self {
        Self {
            dispatch: Arc::new(OnceLock::new()),
        }
    }
}

/// Scan dispatch metadata tracking pending, in-flight, and completed scan morsels.
pub(super) struct ScanDispatchState {
    queue: MorselQueueRef,
    queue_observable: Arc<Observable>,
    inflight_observable: Arc<Observable>,
    logged_queue_empty: AtomicBool,
    /// Early termination flag for scan-level limit optimization.
    /// When set to true, no new morsels should be picked up.
    reach_limit: AtomicBool,
    /// Global output rows counter shared across all async runners.
    /// Used for scan-level limit enforcement.
    output_rows: AtomicUsize,
}

// Relationship overview (per ScanSourceOperator / per driver):
//
//   ScanDispatchState (shared)           ScanSourceOperator (driver)
//   +--------------------------+         +----------------------------+
//   | MorselQueueRef            |         | ScanAsyncState (Chunk buf) |
//   | Observable(s)             |         | ScanAsyncRunner pool       |
//   +-------------+------------+         +--------------+-------------+
//                 |                                 ^
//                 | pop morsel                      |
//                 v                                 | push chunk
//        ScanAsyncRunner (async IO task)  -----------+
//
// Notes:
// - Multiple drivers share the same morsel queue.
// - Each driver has its own async runners and chunk buffer.
// - Async runners execute in scan_executor and feed chunks back to the driver.

impl ScanDispatchState {
    pub(super) fn new(queue: MorselQueueRef) -> Self {
        Self {
            queue,
            queue_observable: Arc::new(Observable::new()),
            inflight_observable: Arc::new(Observable::new()),
            logged_queue_empty: AtomicBool::new(false),
            reach_limit: AtomicBool::new(false),
            output_rows: AtomicUsize::new(0),
        }
    }

    pub(super) fn set_reach_limit(&self) {
        self.reach_limit.store(true, Ordering::Release);
    }

    pub(super) fn is_reach_limit(&self) -> bool {
        self.reach_limit.load(Ordering::Acquire)
    }

    pub(super) fn fetch_add_output_rows(&self, rows: usize) -> usize {
        self.output_rows.fetch_add(rows, Ordering::AcqRel)
    }

    pub(super) fn queue_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.queue_observable)
    }

    pub(super) fn inflight_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.inflight_observable)
    }

    pub(super) fn pop_morsel(&self) -> Option<ScanMorsel> {
        // Early termination: stop picking up new morsels if limit is reached.
        if self.is_reach_limit() {
            return None;
        }

        let notify = self.queue_observable.defer_notify();
        let morsel = self.queue.try_get();
        if self.queue.empty() {
            if self
                .logged_queue_empty
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                debug!(
                    "ScanDispatch morsel queue empty: observers={} original_morsels={}",
                    self.queue_observable.num_observers(),
                    self.num_original_morsels()
                );
            }
            notify.arm();
        }
        morsel
    }

    pub(super) fn queue_empty(&self) -> bool {
        self.is_reach_limit() || self.queue.empty()
    }

    pub(super) fn has_more(&self) -> bool {
        !self.is_reach_limit() && self.queue.has_more()
    }

    pub(super) fn num_original_morsels(&self) -> usize {
        self.queue.num_original_morsels()
    }
}
