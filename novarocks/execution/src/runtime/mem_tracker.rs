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
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Tracks a logically accounted byte buffer that can be transferred across trackers.
///
/// This is used for non-Arrow allocations (e.g., serialized exchange payloads) where we
/// still want to apply the "current holder" ownership model.
#[derive(Debug)]
pub struct TrackedBytes {
    bytes: i64,
    tracker: Arc<MemTracker>,
}

impl TrackedBytes {
    pub fn new(bytes: usize, tracker: Arc<MemTracker>) -> Self {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        tracker.consume(bytes);
        Self { bytes, tracker }
    }

    pub fn bytes(&self) -> i64 {
        self.bytes
    }

    pub fn transfer_to(&mut self, tracker: Arc<MemTracker>) {
        if Arc::ptr_eq(&self.tracker, &tracker) {
            return;
        }
        self.tracker.release(self.bytes);
        tracker.consume(self.bytes);
        self.tracker = tracker;
    }
}

impl Drop for TrackedBytes {
    fn drop(&mut self) {
        self.tracker.release(self.bytes);
    }
}

/// Tracks logical memory usage for a component and its ancestors.
///
/// This is a lightweight accounting utility that only records bytes explicitly
/// reported by the caller. It does NOT reflect real process RSS or allocator
/// statistics.
#[derive(Debug)]
pub struct MemTracker {
    label: String,
    limit: i64,
    parent: Option<Arc<MemTracker>>,
    current: AtomicI64,
    peak: AtomicI64,
    allocated: AtomicI64,
    deallocated: AtomicI64,
    children: Mutex<Vec<Weak<MemTracker>>>,
}

impl MemTracker {
    /// Create a root tracker with no parent.
    pub fn new_root(label: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            label: label.into(),
            limit: -1,
            parent: None,
            current: AtomicI64::new(0),
            peak: AtomicI64::new(0),
            allocated: AtomicI64::new(0),
            deallocated: AtomicI64::new(0),
            children: Mutex::new(Vec::new()),
        })
    }

    /// Create a child tracker with the provided parent.
    pub fn new_child(label: impl Into<String>, parent: &Arc<MemTracker>) -> Arc<Self> {
        let child = Arc::new(Self {
            label: label.into(),
            limit: -1,
            parent: Some(Arc::clone(parent)),
            current: AtomicI64::new(0),
            peak: AtomicI64::new(0),
            allocated: AtomicI64::new(0),
            deallocated: AtomicI64::new(0),
            children: Mutex::new(Vec::new()),
        });
        parent
            .children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Arc::downgrade(&child));
        child
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn limit(&self) -> i64 {
        self.limit
    }

    pub fn current(&self) -> i64 {
        self.current.load(Ordering::Relaxed)
    }

    pub fn peak(&self) -> i64 {
        self.peak.load(Ordering::Relaxed)
    }

    pub fn allocated(&self) -> i64 {
        self.allocated.load(Ordering::Relaxed)
    }

    pub fn deallocated(&self) -> i64 {
        self.deallocated.load(Ordering::Relaxed)
    }

    pub fn children(&self) -> Vec<Arc<MemTracker>> {
        let mut out = Vec::new();
        let guard = self.children.lock().unwrap_or_else(|e| e.into_inner());
        for weak in guard.iter() {
            if let Some(child) = weak.upgrade() {
                out.push(child);
            }
        }
        out
    }

    /// Increase consumption for this tracker and all ancestors.
    pub fn consume(&self, bytes: i64) {
        if bytes <= 0 {
            return;
        }
        let mut tracker: Option<&MemTracker> = Some(self);
        while let Some(current) = tracker {
            let new_value = current.current.fetch_add(bytes, Ordering::AcqRel) + bytes;
            current.allocated.fetch_add(bytes, Ordering::AcqRel);
            current.update_peak(new_value);
            tracker = current.parent.as_deref();
        }
    }

    /// Decrease consumption for this tracker and all ancestors.
    pub fn release(&self, bytes: i64) {
        if bytes <= 0 {
            return;
        }
        let mut tracker: Option<&MemTracker> = Some(self);
        while let Some(current) = tracker {
            current.current.fetch_sub(bytes, Ordering::AcqRel);
            current.deallocated.fetch_add(bytes, Ordering::AcqRel);
            tracker = current.parent.as_deref();
        }
    }

    fn update_peak(&self, value: i64) {
        let mut prev = self.peak.load(Ordering::Relaxed);
        while value > prev {
            match self
                .peak
                .compare_exchange(prev, value, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
    }
}

static PROCESS_TRACKER: OnceLock<Arc<MemTracker>> = OnceLock::new();

/// Global process-level logical memory tracker.
pub fn process_mem_tracker() -> Arc<MemTracker> {
    Arc::clone(PROCESS_TRACKER.get_or_init(|| MemTracker::new_root("process")))
}
