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

//! Provider-local operation counters.
//!
//! These used to live in the neutral contract crate, which forced every
//! provider to share one vocabulary and exposed a `metrics_snapshot` method on
//! the store trait. The contract no longer carries observability, so the
//! counters live here: they are this provider's own accounting, readable by
//! this crate's tests and by nothing else.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub(super) const STATE_STORE_OPERATION_COUNT: usize = 6;
pub(super) const STATE_STORE_OUTCOME_COUNT: usize = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub(super) enum StateStoreOperation {
    Begin = 0,
    Get = 1,
    Range = 2,
    Put = 3,
    Delete = 4,
    Commit = 5,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub(super) enum StateStoreOutcome {
    Success = 0,
    Error = 1,
    Conflict = 2,
    TransientBeforeCommit = 3,
    DefiniteFailure = 4,
    CommitUnknown = 5,
}

/// A point-in-time copy of the counters.
///
/// Only this crate's tests read the counters back: nothing in the contract
/// exposes them, and no production caller asks for them. Keeping the snapshot
/// side test-only is what stops that from silently becoming dead production
/// surface again.
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StateStoreMetricsSnapshot {
    pub(super) operation_outcomes: [[u64; STATE_STORE_OUTCOME_COUNT]; STATE_STORE_OPERATION_COUNT],
    pub(super) operation_duration_observations: [u64; STATE_STORE_OPERATION_COUNT],
    pub(super) blocking_failure_count: u64,
    pub(super) bytes_read: u64,
    pub(super) bytes_written: u64,
    pub(super) page_records: u64,
}

#[cfg(test)]
impl StateStoreMetricsSnapshot {
    pub(super) fn operation_outcome_count(
        &self,
        operation: StateStoreOperation,
        outcome: StateStoreOutcome,
    ) -> u64 {
        self.operation_outcomes[operation as usize][outcome as usize]
    }

    pub(super) fn operation_duration_observations(&self, operation: StateStoreOperation) -> u64 {
        self.operation_duration_observations[operation as usize]
    }
}

#[derive(Debug)]
pub(super) struct StateStoreMetrics {
    operation_outcomes: [[AtomicU64; STATE_STORE_OUTCOME_COUNT]; STATE_STORE_OPERATION_COUNT],
    operation_duration_observations: [AtomicU64; STATE_STORE_OPERATION_COUNT],
    blocking_failure_count: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    page_records: AtomicU64,
}

impl StateStoreMetrics {
    pub(super) fn new() -> Self {
        Self {
            operation_outcomes: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            operation_duration_observations: std::array::from_fn(|_| AtomicU64::new(0)),
            blocking_failure_count: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            page_records: AtomicU64::new(0),
        }
    }

    pub(super) fn record_operation(
        &self,
        operation: StateStoreOperation,
        outcome: StateStoreOutcome,
        _duration: Duration,
    ) {
        self.operation_outcomes[operation as usize][outcome as usize]
            .fetch_add(1, Ordering::Relaxed);
        self.operation_duration_observations[operation as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_blocking_failure(&self) {
        self.blocking_failure_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_bytes_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(super) fn record_bytes_written(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(super) fn record_page_records(&self, records: u64) {
        self.page_records.fetch_add(records, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> StateStoreMetricsSnapshot {
        StateStoreMetricsSnapshot {
            operation_outcomes: std::array::from_fn(|operation| {
                std::array::from_fn(|outcome| {
                    self.operation_outcomes[operation][outcome].load(Ordering::Relaxed)
                })
            }),
            operation_duration_observations: std::array::from_fn(|operation| {
                self.operation_duration_observations[operation].load(Ordering::Relaxed)
            }),
            blocking_failure_count: self.blocking_failure_count.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            page_records: self.page_records.load(Ordering::Relaxed),
        }
    }
}
