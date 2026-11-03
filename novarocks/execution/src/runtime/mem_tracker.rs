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

/// The original tracker's first crossed limit, without inferring a category
/// from its diagnostic text. It owns no tracker or query lifetime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemTrackerLimitExceeded {
    detail: String,
    requested: u64,
    limit: u64,
    committed: u64,
}
impl MemTrackerLimitExceeded {
    pub fn detail(&self) -> &str {
        &self.detail
    }
}
impl std::fmt::Display for MemTrackerLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}
impl std::error::Error for MemTrackerLimitExceeded {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemTrackerLimitInstallError {
    InvalidLimit { requested: i64 },
    ConflictingLimit { installed: i64, requested: i64 },
    Exceeded(MemTrackerLimitExceeded),
}
impl std::fmt::Display for MemTrackerLimitInstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimit { requested } => write!(
                f,
                "memory tracker requires a positive limit, got {requested}"
            ),
            Self::ConflictingLimit {
                installed,
                requested,
            } => write!(
                f,
                "memory tracker already has limit {installed}, cannot install {requested}"
            ),
            Self::Exceeded(source) => std::fmt::Display::fmt(source, f),
        }
    }
}
impl std::error::Error for MemTrackerLimitInstallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Exceeded(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for MemTrackerLimitInstallError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let code = match self {
            Self::Exceeded(source) => return source.failure_fact(),
            Self::InvalidLimit { .. } => 10,
            Self::ConflictingLimit { .. } => 11,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::MEMORY, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

/// The allocation callback keeps every known limit measurement without
/// allocating a diagnostic or retaining the tracker that owns the counter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MemTrackerLimitFact {
    pub(crate) requested: u64,
    pub(crate) limit: u64,
    pub(crate) committed: u64,
}
impl std::fmt::Display for MemTrackerLimitFact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "memory limit exceeded: requested={}, limit={}, committed={}",
            self.requested, self.limit, self.committed
        )
    }
}
impl std::error::Error for MemTrackerLimitFact {}

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
    pub fn try_new(
        bytes: usize,
        tracker: Arc<MemTracker>,
    ) -> Result<Self, MemoryAccountingFailure> {
        let bytes = i64::try_from(bytes)
            .map_err(|_| MemoryAccountingFailure::BytesOutOfRange { requested: bytes })?;
        if let Err(error) = tracker.consume_and_check_limit_typed(bytes) {
            tracker.release(bytes);
            return Err(MemoryAccountingFailure::Limit(error));
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

/// A retained buffer charge either exceeds the tracker's counter range or
/// is refused by an installed hierarchy limit. Neither is a SQL value error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemoryAccountingFailure {
    BytesOutOfRange { requested: usize },
    Limit(MemTrackerLimitExceeded),
}
impl MemoryAccountingFailure {
    pub fn detail(&self) -> &str {
        match self {
            Self::BytesOutOfRange { .. } => "retained memory charge exceeds the i64 counter range",
            Self::Limit(error) => error.detail(),
        }
    }
}
impl std::fmt::Display for MemoryAccountingFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BytesOutOfRange { requested } => write!(
                f,
                "retained memory charge of {requested} bytes exceeds the i64 counter range"
            ),
            Self::Limit(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for MemoryAccountingFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Limit(error) => Some(error),
            Self::BytesOutOfRange { .. } => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for MemoryAccountingFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, ResourceDimension, ResourceFacts,
            ResourceScope, domain,
        };
        match self {
            Self::Limit(error) => error.failure_fact(),
            Self::BytesOutOfRange { requested } => FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::MEMORY, std::num::NonZeroU16::new(9).unwrap()),
            )
            .with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Request),
                dimension: ResourceDimension::Bytes,
                requested: u64::try_from(*requested).ok(),
                required_growth: None,
                limit: Some(i64::MAX as u64),
                live: None,
                committed: None,
            }),
        }
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

/// Requests of one original limit-failure String, even when its host discards
/// it. These numerical facts neither reserve bytes nor select a failing limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TrackerLimitFailureRequestFacts {
    pub(crate) requested_bytes: usize,
    pub(crate) requests: usize,
    pub(crate) peak_bytes: usize,
    pub(crate) retained_bytes: usize,
    pub(crate) output_bytes: usize,
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

    /// Read-only evidence for an original runtime owner, never a new limit.
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn is_unique_limit_owner(&self, owner: &Self) -> bool {
        let limit = owner.limit();
        if limit <= 0 {
            return false;
        }
        let mut found = false;
        let mut current = Some(self);
        while let Some(tracker) = current {
            let same = std::ptr::eq(tracker, owner);
            found |= same;
            if tracker.limit() == limit && !same {
                return false;
            }
            current = tracker.parent.as_deref();
        }
        found
    }

    /// Installs the immutable positive byte limit for this tracker.
    ///
    /// Replaying the same query contract is idempotent. A different limit is
    /// rejected so independently admitted fragments cannot silently change a
    /// query-scoped resource contract.
    pub fn install_limit_once(&self, limit: i64) -> Result<(), MemTrackerLimitInstallError> {
        if limit <= 0 {
            return Err(MemTrackerLimitInstallError::InvalidLimit { requested: limit });
        }
        match self
            .limit
            .compare_exchange(-1, limit, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                let current = self.current();
                if current > limit {
                    Err(MemTrackerLimitInstallError::Exceeded(
                        MemTrackerLimitExceeded {
                            detail: self.limit_exceeded_detail(current, limit),
                            requested: 0,
                            limit: limit as u64,
                            committed: current as u64,
                        },
                    ))
                } else {
                    Ok(())
                }
            }
            Err(existing) if existing == limit => Ok(()),
            Err(existing) => Err(MemTrackerLimitInstallError::ConflictingLimit {
                installed: existing,
                requested: limit,
            }),
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
        self.consume_and_check_limit_typed(bytes)
            .map_err(|cause| cause.detail)
    }

    /// The same accounting operation with its original typed limit cause.
    /// Failed post-mutation accounting remains until the owner releases it;
    /// prospective construction callers balance their unconstructed request.
    pub fn consume_and_check_limit_typed(&self, bytes: i64) -> Result<(), MemTrackerLimitExceeded> {
        self.consume_with_limit_projection(bytes, |tracker, current, limit| {
            MemTrackerLimitExceeded {
                detail: tracker.limit_exceeded_detail(current, limit),
                requested: bytes as u64,
                limit: limit as u64,
                committed: current as u64,
            }
        })
    }

    /// The original accounting verdict without constructing an unadmitted diagnostic.
    pub(crate) fn consume_and_check_limit_nominal(
        &self,
        bytes: i64,
    ) -> Result<(), MemTrackerLimitFact> {
        self.consume_with_limit_projection(bytes, |_, committed, limit| MemTrackerLimitFact {
            requested: bytes as u64,
            limit: limit as u64,
            committed: committed as u64,
        })
    }

    fn consume_with_limit_projection<E>(
        &self,
        bytes: i64,
        project: impl Fn(&MemTracker, i64, i64) -> E,
    ) -> Result<(), E> {
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
                exceeded = Some(project(current, new_value, limit));
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
        Self::try_transfer_charge_typed(source, destination, bytes).map_err(|error| error.detail)
    }

    /// Preserve the nominal destination limit refusal through the same
    /// transfer body and its original rollback/source ownership law.
    pub fn try_transfer_charge_typed(
        source: &Arc<Self>,
        destination: &Arc<Self>,
        bytes: i64,
    ) -> Result<(), MemTrackerLimitExceeded> {
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
            if let Err(error) = tracker.try_consume_local_typed(bytes) {
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

    fn try_consume_local_typed(&self, bytes: i64) -> Result<(), MemTrackerLimitExceeded> {
        let limit = self.limit();
        let mut current = self.current.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(bytes);
            if limit > 0 && next > limit {
                return Err(MemTrackerLimitExceeded {
                    detail: self.limit_exceeded_detail(next, limit),
                    requested: bytes as u64,
                    limit: limit as u64,
                    committed: current as u64,
                });
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

    /// Borrow the same immutable ancestry as consume_and_check_limit. Only
    /// its first crossed limit formats, while accounting still visits all
    /// ancestors. Do not build tracker_path or clone labels here.
    pub(crate) fn limit_failure_request_facts(
        &self,
    ) -> Result<TrackerLimitFailureRequestFacts, novarocks_type_contract::ControlResourceError>
    {
        let mut maximum_label = 0;
        let mut tracker = Some(self);
        while let Some(current) = tracker {
            maximum_label = maximum_label.max(current.label.len());
            tracker = current.parent.as_deref();
        }
        let output_bytes = maximum_label
            .checked_add(121)
            .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
        // The original template has 83 literal bytes and a leading literal;
        // its pinned estimated_capacity is 166. Both actual i64 operands are
        // positive at this call site and have at most 19 decimal digits.
        let original = novarocks_type_contract::owned_resources::formatting::original_format_string_request_bound(
            166,
            output_bytes,
        )?;
        // The SAME formatter author returns initial C=166 for one request,
        // otherwise C=2*G for its maximum successful destination capacity G.
        let retained_bytes = if original.allocation_requests_upper_bound == 1 {
            original.request_bytes_upper_bound
        } else {
            original.request_bytes_upper_bound / 2
        };
        let peak_bytes = if original.allocation_requests_upper_bound == 1 {
            retained_bytes
        } else {
            retained_bytes
                .checked_add(retained_bytes / 2)
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?
        };
        Ok(TrackerLimitFailureRequestFacts {
            requested_bytes: original.request_bytes_upper_bound,
            requests: original.allocation_requests_upper_bound,
            peak_bytes,
            retained_bytes,
            output_bytes,
        })
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
///
/// This is the only root a production tracker may be built from: every other
/// owner attaches through `new_child` so one process owns exactly one tracker
/// hierarchy.
pub fn process_mem_tracker() -> Arc<MemTracker> {
    Arc::clone(PROCESS_TRACKER.get_or_init(|| MemTracker::new_root("process")))
}

/// Canonical label for the query-scoped tracker of one query identity.
///
/// Every owner that parents a subtree on a query mints its label here. Two
/// independently admitted construction paths therefore agree on one name
/// instead of drifting into differently-labelled siblings that no operator can
/// recognize as the same query.
pub fn query_tracker_label(high: i64, low: i64) -> String {
    format!("query_{high:x}_{low:x}")
}

#[cfg(test)]
mod tests {
    use super::{MemTracker, MemTrackerLimitInstallError};

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn unique_limit_witness_requires_the_original_ancestor() {
        let process = MemTracker::new_root("process");
        process.install_limit_once(1024).unwrap();
        let query = MemTracker::new_child("query", &process);
        query.install_limit_once(128).unwrap();
        let operator = MemTracker::new_child("operator", &query);
        assert!(operator.is_unique_limit_owner(&query));
        let foreign = MemTracker::new_child("query", &process);
        foreign.install_limit_once(128).unwrap();
        assert!(!operator.is_unique_limit_owner(&foreign));
        operator.install_limit_once(128).unwrap();
        assert!(!operator.is_unique_limit_owner(&query));
    }

    #[test]
    fn nominal_limit_projection_preserves_every_ancestor_counter() {
        let original_root = MemTracker::new_root("root".repeat(4096));
        let original_middle = MemTracker::new_child("middle", &original_root);
        let original_leaf = MemTracker::new_child("leaf", &original_middle);
        let nominal_root = MemTracker::new_root("root".repeat(4096));
        let nominal_middle = MemTracker::new_child("middle", &nominal_root);
        let nominal_leaf = MemTracker::new_child("leaf", &nominal_middle);
        for tracker in [&original_root, &nominal_root] {
            tracker.install_limit_once(1).unwrap();
        }
        for tracker in [&original_middle, &nominal_middle] {
            tracker.install_limit_once(2).unwrap();
        }
        for bytes in [-1, 0, 1, 2, 4] {
            assert_eq!(
                original_leaf.consume_and_check_limit(bytes).is_err(),
                nominal_leaf.consume_and_check_limit_nominal(bytes).is_err()
            );
            for (original, nominal) in [
                (&original_root, &nominal_root),
                (&original_middle, &nominal_middle),
                (&original_leaf, &nominal_leaf),
            ] {
                assert_eq!(nominal.current(), original.current());
                assert_eq!(nominal.allocated(), original.allocated());
                assert_eq!(nominal.peak(), original.peak());
            }
        }
        original_leaf.release(7);
        nominal_leaf.release(7);
        for (original, nominal) in [
            (&original_root, &nominal_root),
            (&original_middle, &nominal_middle),
            (&original_leaf, &nominal_leaf),
        ] {
            assert_eq!(nominal.current(), 0);
            assert_eq!(nominal.deallocated(), original.deallocated());
        }
    }

    #[test]
    fn first_limit_projection_runs_once_at_original_ancestor_position() {
        let root = MemTracker::new_root("root");
        let middle = MemTracker::new_child("middle", &root);
        let leaf = MemTracker::new_child("leaf", &middle);
        middle.install_limit_once(1).unwrap();
        root.install_limit_once(1).unwrap();
        let calls = std::cell::Cell::new(0);
        let result = leaf.consume_with_limit_projection(2, |tracker, current, limit| {
            calls.set(calls.get() + 1);
            assert!(std::ptr::eq(tracker, middle.as_ref()));
            assert_eq!((current, limit), (2, 1));
            assert_eq!(leaf.current(), 2);
            assert_eq!(root.current(), 0);
            tracker.limit_exceeded_detail(current, limit)
        });
        assert_eq!(calls.get(), 1);
        assert_eq!(
            result.unwrap_err(),
            "ResourceExhausted: memory limit exceeded for tracker middle: current 2 bytes, limit 1 bytes"
        );
        assert_eq!(root.current(), 2);
        assert_eq!(root.allocated(), 2);
        assert_eq!(root.peak(), 2);
        leaf.release(2);
        let result = leaf.consume_with_limit_projection(0, |_, _, _| {
            calls.set(calls.get() + 1);
        });
        assert!(result.is_ok());
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn tracker_limit_failure_requests_cover_actual_ancestry_and_preserve_original_text() {
        for count in [0, 16, 128, 4096] {
            let label = "root\0µ\n".repeat(count);
            let root = MemTracker::new_root(label.clone());
            let middle = MemTracker::new_child("middle", &root);
            let leaf = MemTracker::new_child("leaf", &middle);
            root.install_limit_once(1).unwrap();
            let facts = leaf.limit_failure_request_facts().unwrap();
            for tracker in [&root, &middle, &leaf] {
                assert_eq!(tracker.current(), 0);
                assert_eq!(tracker.allocated(), 0);
            }
            let original = leaf.consume_and_check_limit(2).unwrap_err();
            assert_eq!(
                original,
                format!(
                    "ResourceExhausted: memory limit exceeded for tracker {label}: current 2 bytes, limit 1 bytes"
                )
            );
            assert!(original.len() <= facts.output_bytes);
            assert!(original.capacity() <= facts.retained_bytes);
            assert!(facts.retained_bytes <= facts.peak_bytes);
            assert!(facts.peak_bytes <= facts.requested_bytes);
            assert!(facts.requests > 0);
            for tracker in [&root, &middle, &leaf] {
                assert_eq!(tracker.current(), 2);
                assert_eq!(tracker.allocated(), 2);
                assert_eq!(tracker.peak(), 2);
            }
            leaf.release(2);
            for tracker in [&root, &middle, &leaf] {
                assert_eq!(tracker.current(), 0);
                assert_eq!(tracker.allocated(), 2);
            }
        }
    }

    #[test]
    fn tracker_limit_failure_requests_do_not_change_first_failure_or_ancestor_accounting() {
        let root = MemTracker::new_root("long root label".repeat(1024));
        let leaf = MemTracker::new_child("leaf", &root);
        root.install_limit_once(1).unwrap();
        leaf.install_limit_once(1).unwrap();
        let facts = leaf.limit_failure_request_facts().unwrap();
        assert!(facts.output_bytes >= root.label().len() + 121);
        assert_eq!(
            leaf.consume_and_check_limit(2).unwrap_err(),
            "ResourceExhausted: memory limit exceeded for tracker leaf: current 2 bytes, limit 1 bytes"
        );
        assert_eq!(root.current(), 2);
        assert_eq!(root.allocated(), 2);
        assert_eq!(leaf.allocated(), 2);
        leaf.release(2);
        assert_eq!(root.current(), 0);
        assert_eq!(leaf.current(), 0);
    }

    #[test]
    fn installing_policy_over_actual_charge_keeps_source_and_counter_facts() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let tracker = MemTracker::new_root("policy source");
        tracker.consume(32);
        let error = tracker
            .install_limit_once(8)
            .expect_err("already charged bytes exceed installed policy");
        assert!(matches!(error, MemTrackerLimitInstallError::Exceeded(_)));
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().unwrap();
        assert_eq!(resource.requested, Some(0));
        assert_eq!(resource.committed, Some(32));
        assert_eq!(resource.limit, Some(8));
        assert_eq!(resource.live, None);
        assert_eq!(tracker.current(), 32);
        assert_eq!(tracker.limit(), 8);
        tracker.release(32);
        assert_eq!(tracker.current(), 0);
        assert!(matches!(
            MemTracker::new_root("invalid policy").install_limit_once(0),
            Err(MemTrackerLimitInstallError::InvalidLimit { requested: 0 })
        ));
    }

    #[test]
    fn immutable_limit_is_idempotent_and_rejects_drift() {
        let tracker = MemTracker::new_root("query");
        tracker.install_limit_once(64).expect("install limit");
        tracker.install_limit_once(64).expect("repeat exact limit");
        let error = tracker
            .install_limit_once(65)
            .expect_err("different limit must fail");
        assert_eq!(
            error,
            MemTrackerLimitInstallError::ConflictingLimit {
                installed: 64,
                requested: 65
            }
        );
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
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

        let error = MemTracker::try_transfer_charge_typed(&source, &destination, 8).unwrap_err();
        assert_eq!(
            error.detail(),
            "ResourceExhausted: memory limit exceeded for tracker destination: current 8 bytes, limit 4 bytes"
        );
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

impl novarocks_failure_contract::FailureProjection for MemTrackerLimitExceeded {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&MemTrackerLimitFact {
            requested: self.requested,
            limit: self.limit,
            committed: self.committed,
        })
    }
}

impl novarocks_failure_contract::FailureProjection for MemTrackerLimitFact {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, ResourceDimension, ResourceFacts, domain,
        };
        FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(domain::MEMORY, std::num::NonZeroU16::new(8).unwrap()),
        )
        .with_resource(ResourceFacts {
            request_scope: None,
            constraint_scope: None,
            dimension: ResourceDimension::Bytes,
            requested: Some(self.requested),
            required_growth: None,
            limit: Some(self.limit),
            live: None,
            committed: Some(self.committed),
        })
    }
}

#[cfg(test)]
mod failure_projection_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    #[test]
    fn nominal_refusal_keeps_post_charge_facts_without_retaining_its_tracker() {
        let nominal = MemTracker::new_root("nominal capacity source");
        let rich = MemTracker::new_root("rich capacity source");
        nominal.install_limit_once(8).unwrap();
        rich.install_limit_once(8).unwrap();
        let weak = Arc::downgrade(&nominal);
        let original = nominal.consume_and_check_limit_nominal(16).unwrap_err();
        let comparison = rich.consume_and_check_limit_typed(16).unwrap_err();
        assert_eq!(original.failure_fact(), comparison.failure_fact());
        assert_eq!(nominal.current(), 16);
        nominal.release(16);
        rich.release(16);
        drop(nominal);
        assert!(weak.upgrade().is_none());
        let resource = original.failure_fact().resource().unwrap();
        assert_eq!(
            (resource.requested, resource.limit, resource.committed),
            (Some(16), Some(8), Some(16))
        );
    }
    #[test]
    fn post_charge_limit_fact_preserves_actual_request_and_committed_bytes() {
        let tracker = MemTracker::new_root("receiver limit");
        tracker.install_limit_once(8).unwrap();
        let error = tracker.consume_and_check_limit_typed(16).unwrap_err();
        let fact = error.failure_fact().resource().unwrap();
        assert_eq!(
            (fact.requested, fact.limit, fact.committed),
            (Some(16), Some(8), Some(16))
        );
        assert_eq!(fact.live, None);
        assert_eq!(tracker.current(), 16);
        tracker.release(16);
        assert_eq!(tracker.current(), 0);
    }
}

#[cfg(test)]
mod retained_accounting_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn retained_charge_refusals_keep_facts_and_roll_back_every_charge() {
        let tracker = MemTracker::new_root("retained charge fixture");
        tracker.install_limit_once(8).unwrap();
        let error = TrackedBytes::try_new(16, Arc::clone(&tracker)).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.resource().unwrap().requested, Some(16));
        assert_eq!(fact.resource().unwrap().limit, Some(8));
        assert_eq!(tracker.current(), 0);
        if usize::BITS >= 64 {
            let error = TrackedBytes::try_new(usize::MAX, Arc::clone(&tracker)).unwrap_err();
            assert!(matches!(
                error,
                MemoryAccountingFailure::BytesOutOfRange { .. }
            ));
            let fact = error.failure_fact();
            assert_eq!(
                fact.resource().unwrap().requested,
                u64::try_from(usize::MAX).ok()
            );
            assert_eq!(fact.resource().unwrap().limit, Some(i64::MAX as u64));
            assert_eq!(tracker.current(), 0);
        }
    }
}
