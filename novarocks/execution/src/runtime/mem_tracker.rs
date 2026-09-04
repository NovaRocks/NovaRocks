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

    /// Acquires ownership accounting while enforcing every installed limit in
    /// the tracker ancestry.
    ///
    /// The charge is rolled back when admission fails because no
    /// `TrackedBytes` owner is returned to retain the corresponding buffer.
    pub fn try_new(bytes: usize, tracker: Arc<MemTracker>) -> Result<Self, String> {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        if let Err(error) = tracker.consume_and_check_limit(bytes) {
            tracker.release(bytes);
            return Err(error);
        }
        Ok(Self { bytes, tracker })
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
    limit: AtomicI64,
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
            limit: AtomicI64::new(-1),
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
            limit: AtomicI64::new(-1),
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
        self.limit.load(Ordering::Acquire)
    }

    /// Installs the immutable positive byte limit for this tracker.
    ///
    /// Replaying the same query contract is idempotent. A different limit is
    /// rejected so independently admitted fragments cannot silently change a
    /// query-scoped resource contract.
    pub fn install_limit_once(&self, limit: i64) -> Result<(), String> {
        if limit <= 0 {
            return Err(format!(
                "memory tracker {} requires a positive limit, got {limit}",
                self.label
            ));
        }
        match self
            .limit
            .compare_exchange(-1, limit, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                let current = self.current();
                if current > limit {
                    Err(self.limit_exceeded_detail(current, limit))
                } else {
                    Ok(())
                }
            }
            Err(existing) if existing == limit => Ok(()),
            Err(existing) => Err(format!(
                "memory tracker {} already has limit {existing}, cannot install {limit}",
                self.label
            )),
        }
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
        let _ = self.consume_and_check_limit(bytes);
    }

    /// Records newly owned bytes on this tracker and every ancestor, then
    /// reports the first crossed limit.
    ///
    /// Accounting is deliberately retained on error: callers use this after a
    /// state mutation has acquired memory, so rolling the counters back would
    /// hide live memory. The owner must fail the operation and release the
    /// complete retained amount when the state is dropped.
    pub fn consume_and_check_limit(&self, bytes: i64) -> Result<(), String> {
        if bytes <= 0 {
            return Ok(());
        }
        let mut exceeded = None;
        let mut tracker: Option<&MemTracker> = Some(self);
        while let Some(current) = tracker {
            let new_value = atomic_saturating_add(&current.current, bytes);
            atomic_saturating_add(&current.allocated, bytes);
            current.update_peak(new_value);
            let limit = current.limit();
            if exceeded.is_none() && limit > 0 && new_value > limit {
                exceeded = Some(current.limit_exceeded_detail(new_value, limit));
            }
            tracker = current.parent.as_deref();
        }
        exceeded.map_or(Ok(()), Err)
    }

    /// Decrease consumption for this tracker and all ancestors.
    pub fn release(&self, bytes: i64) {
        if bytes <= 0 {
            return;
        }
        let mut tracker: Option<&MemTracker> = Some(self);
        while let Some(current) = tracker {
            current
                .current
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                    value.checked_sub(bytes).filter(|remaining| *remaining >= 0)
                })
                .unwrap_or_else(|value| {
                    panic!(
                        "memory tracker {} release underflow: current {value} bytes, release {bytes} bytes",
                        current.label
                    )
                });
            atomic_saturating_add(&current.deallocated, bytes);
            tracker = current.parent.as_deref();
        }
    }

    /// Moves an existing charge between two tracker branches without charging
    /// their shared ancestors twice. Destination-only limits are checked
    /// before source ownership is released; failure leaves the source charge
    /// unchanged.
    pub fn try_transfer_charge(
        source: &Arc<Self>,
        destination: &Arc<Self>,
        bytes: i64,
    ) -> Result<(), String> {
        if bytes <= 0 || Arc::ptr_eq(source, destination) {
            return Ok(());
        }

        let source_path = tracker_path(source);
        let destination_path = tracker_path(destination);
        let mut source_shared = source_path.len();
        let mut destination_shared = destination_path.len();
        while source_shared > 0
            && destination_shared > 0
            && Arc::ptr_eq(
                &source_path[source_shared - 1],
                &destination_path[destination_shared - 1],
            )
        {
            source_shared -= 1;
            destination_shared -= 1;
        }

        let mut reserved: Vec<Arc<MemTracker>> = Vec::with_capacity(destination_shared);
        for tracker in &destination_path[..destination_shared] {
            if let Err(error) = tracker.try_consume_local(bytes) {
                for tracker in reserved.into_iter().rev() {
                    tracker.release_local(bytes);
                }
                return Err(error);
            }
            reserved.push(Arc::clone(tracker));
        }
        for tracker in &source_path[..source_shared] {
            tracker.release_local(bytes);
        }
        Ok(())
    }

    fn try_consume_local(&self, bytes: i64) -> Result<(), String> {
        let limit = self.limit();
        let mut current = self.current.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(bytes);
            if limit > 0 && next > limit {
                return Err(self.limit_exceeded_detail(next, limit));
            }
            match self.current.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    atomic_saturating_add(&self.allocated, bytes);
                    self.update_peak(next);
                    return Ok(());
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn release_local(&self, bytes: i64) {
        self.current
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(bytes).filter(|remaining| *remaining >= 0)
            })
            .unwrap_or_else(|value| {
                panic!(
                    "memory tracker {} local release underflow: current {value} bytes, release {bytes} bytes",
                    self.label
                )
            });
        atomic_saturating_add(&self.deallocated, bytes);
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

    fn limit_exceeded_detail(&self, current: i64, limit: i64) -> String {
        format!(
            "ResourceExhausted: memory limit exceeded for tracker {}: current {current} bytes, limit {limit} bytes",
            self.label
        )
    }
}

fn tracker_path(tracker: &Arc<MemTracker>) -> Vec<Arc<MemTracker>> {
    let mut path = Vec::new();
    let mut current = Some(Arc::clone(tracker));
    while let Some(tracker) = current {
        current = tracker.parent.as_ref().map(Arc::clone);
        path.push(tracker);
    }
    path
}

fn atomic_saturating_add(value: &AtomicI64, bytes: i64) -> i64 {
    value
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_add(bytes))
        })
        .map(|previous| previous.saturating_add(bytes))
        .unwrap_or(i64::MAX)
}

static PROCESS_TRACKER: OnceLock<Arc<MemTracker>> = OnceLock::new();

/// Global process-level logical memory tracker.
pub fn process_mem_tracker() -> Arc<MemTracker> {
    Arc::clone(PROCESS_TRACKER.get_or_init(|| MemTracker::new_root("process")))
}

#[cfg(test)]
mod tests {
    use super::MemTracker;

    #[test]
    fn immutable_limit_is_idempotent_and_rejects_drift() {
        let tracker = MemTracker::new_root("query");
        tracker.install_limit_once(64).expect("install limit");
        tracker.install_limit_once(64).expect("repeat exact limit");
        let error = tracker
            .install_limit_once(65)
            .expect_err("different limit must fail");
        assert!(error.contains("already has limit 64"), "{error}");
        assert_eq!(tracker.limit(), 64);
    }

    #[test]
    fn limit_failure_keeps_live_memory_accounted_until_release() {
        let query = MemTracker::new_root("query");
        query.install_limit_once(10).expect("install limit");
        let fragment = MemTracker::new_child("fragment", &query);

        fragment
            .consume_and_check_limit(8)
            .expect("below limit succeeds");
        let error = fragment
            .consume_and_check_limit(4)
            .expect_err("crossing limit fails");
        assert!(error.contains("tracker query"), "{error}");
        assert_eq!(fragment.current(), 12);
        assert_eq!(query.current(), 12);

        fragment.release(12);
        assert_eq!(fragment.current(), 0);
        assert_eq!(query.current(), 0);
    }

    #[test]
    fn child_charge_observes_ancestor_limit() {
        let process = MemTracker::new_root("process");
        let query = MemTracker::new_child("query", &process);
        query.install_limit_once(3).expect("install limit");
        let fragment = MemTracker::new_child("fragment", &query);

        fragment
            .consume_and_check_limit(4)
            .expect_err("ancestor limit is enforced");
        assert_eq!(fragment.current(), 4);
        assert_eq!(query.current(), 4);
        assert_eq!(process.current(), 4);
    }

    #[test]
    fn sibling_transfer_does_not_double_charge_shared_ancestor() {
        let query = MemTracker::new_root("query");
        query.install_limit_once(8).unwrap();
        let operator = MemTracker::new_child("operator", &query);
        let queue = MemTracker::new_child("queue", &operator);
        operator.consume_and_check_limit(8).unwrap();

        MemTracker::try_transfer_charge(&operator, &queue, 8).unwrap();

        assert_eq!(query.current(), 8);
        assert_eq!(query.peak(), 8);
        assert_eq!(operator.current(), 8);
        assert_eq!(queue.current(), 8);
        queue.release(8);
        assert_eq!(query.current(), 0);
    }

    #[test]
    fn failed_transfer_preserves_source_charge() {
        let query = MemTracker::new_root("query");
        let source = MemTracker::new_child("source", &query);
        let destination = MemTracker::new_child("destination", &query);
        destination.install_limit_once(4).unwrap();
        source.consume_and_check_limit(8).unwrap();

        assert!(MemTracker::try_transfer_charge(&source, &destination, 8).is_err());
        assert_eq!(source.current(), 8);
        assert_eq!(destination.current(), 0);
        assert_eq!(query.current(), 8);
        source.release(8);
    }

    #[test]
    #[should_panic(expected = "release underflow")]
    fn release_never_allows_negative_current_bytes() {
        let tracker = MemTracker::new_root("query");
        tracker.consume(1);
        tracker.release(1);
        tracker.release(1);
    }
}
