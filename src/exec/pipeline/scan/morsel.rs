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
//! Scan morsel queue implementations.
//!
//! Responsibilities:
//! - Defines dynamic/fixed morsel queues for distributing scan tasks across drivers.
//! - Implements queue semantics for work stealing and completion signaling.
//!
//! Key exported interfaces:
//! - Types: `MorselQueue`, `MorselQueueRef`, `DynamicMorselQueue`, `FixedMorselQueue`.
//! - Functions: `create_empty_morsel_queue`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::exec::node::scan::ScanMorsel;

/// Queue contract for distributing scan morsels across scan-capable drivers.
pub trait MorselQueue: Send + Sync {
    fn try_get(&self) -> Option<ScanMorsel>;
    fn unget(&self, morsel: ScanMorsel);
    fn empty(&self) -> bool;
    fn has_more(&self) -> bool;
    fn set_has_more(&self, value: bool);
    fn num_original_morsels(&self) -> usize;
    fn append_morsels(&self, _morsels: Vec<ScanMorsel>) -> Result<(), String> {
        Err("morsel queue does not support append".to_string())
    }
}

/// Shared reference to a scan morsel queue implementation.
pub type MorselQueueRef = Arc<dyn MorselQueue>;

/// Dynamically extendable morsel queue used for adaptive scan task distribution.
pub struct DynamicMorselQueue {
    queue: Mutex<VecDeque<ScanMorsel>>,
    size: AtomicUsize,
    has_more: AtomicBool,
    original_morsels: usize,
}

impl DynamicMorselQueue {
    pub fn new(morsels: Vec<ScanMorsel>, has_more: bool) -> Arc<Self> {
        let size = morsels.len();
        Arc::new(Self {
            queue: Mutex::new(VecDeque::from(morsels)),
            size: AtomicUsize::new(size),
            has_more: AtomicBool::new(has_more),
            original_morsels: size,
        })
    }
}

impl MorselQueue for DynamicMorselQueue {
    fn try_get(&self) -> Option<ScanMorsel> {
        if self.size.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut guard = self.queue.lock().expect("morsel queue lock");
        let morsel = guard.pop_front();
        if morsel.is_some() {
            self.size.fetch_sub(1, Ordering::AcqRel);
        }
        morsel
    }

    fn unget(&self, morsel: ScanMorsel) {
        let mut guard = self.queue.lock().expect("morsel queue lock");
        guard.push_front(morsel);
        self.size.fetch_add(1, Ordering::AcqRel);
    }

    fn empty(&self) -> bool {
        self.size.load(Ordering::Acquire) == 0
    }

    fn has_more(&self) -> bool {
        self.has_more.load(Ordering::Acquire)
    }

    fn set_has_more(&self, value: bool) {
        self.has_more.store(value, Ordering::Release);
    }

    fn num_original_morsels(&self) -> usize {
        self.original_morsels
    }

    fn append_morsels(&self, morsels: Vec<ScanMorsel>) -> Result<(), String> {
        if morsels.is_empty() {
            return Ok(());
        }
        let mut guard = self.queue.lock().expect("morsel queue lock");
        self.size.fetch_add(morsels.len(), Ordering::AcqRel);
        for morsel in morsels {
            guard.push_back(morsel);
        }
        Ok(())
    }
}

/// Fixed-size morsel queue used when scan work units are precomputed.
pub struct FixedMorselQueue {
    queue: Mutex<VecDeque<ScanMorsel>>,
    size: AtomicUsize,
    original_morsels: usize,
}

impl FixedMorselQueue {
    pub fn new(morsels: Vec<ScanMorsel>) -> Arc<Self> {
        let size = morsels.len();
        Arc::new(Self {
            queue: Mutex::new(VecDeque::from(morsels)),
            size: AtomicUsize::new(size),
            original_morsels: size,
        })
    }
}

impl MorselQueue for FixedMorselQueue {
    fn try_get(&self) -> Option<ScanMorsel> {
        if self.size.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut guard = self.queue.lock().expect("morsel queue lock");
        let morsel = guard.pop_front();
        if morsel.is_some() {
            self.size.fetch_sub(1, Ordering::AcqRel);
        }
        morsel
    }

    fn unget(&self, morsel: ScanMorsel) {
        let mut guard = self.queue.lock().expect("morsel queue lock");
        guard.push_front(morsel);
        self.size.fetch_add(1, Ordering::AcqRel);
    }

    fn empty(&self) -> bool {
        self.size.load(Ordering::Acquire) == 0
    }

    fn has_more(&self) -> bool {
        false
    }

    fn set_has_more(&self, _value: bool) {}

    fn num_original_morsels(&self) -> usize {
        self.original_morsels
    }
}

/// Create an initially empty dynamic morsel queue with an explicit has-more flag.
pub fn create_empty_morsel_queue(has_more: bool) -> Arc<DynamicMorselQueue> {
    DynamicMorselQueue::new(Vec::new(), has_more)
}
