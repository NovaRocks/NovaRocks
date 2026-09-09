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

//! Provider-private operation counters.
//!
//! These used to live in the neutral contract crate and be exposed through a
//! `StateStore::metrics_snapshot` trait method. Observability is not part of
//! the storage contract: what a provider counts, and whether it counts at all,
//! is its own business, so only the trait obligation is gone.
//!
//! Nothing outside this module reads the counters today. The readout surface
//! is therefore `#[cfg(test)]` rather than `pub(super)` with an `allow`: the
//! recording side is live on every transaction, and the gate says plainly that
//! no production caller consumes it yet.

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

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StateStoreMetricsSnapshot {
    pub operation_outcomes: [[u64; STATE_STORE_OUTCOME_COUNT]; STATE_STORE_OPERATION_COUNT],
    pub operation_duration_observations: [u64; STATE_STORE_OPERATION_COUNT],
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub page_records: u64,
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
}

#[derive(Debug)]
pub(super) struct StateStoreMetrics {
    operation_outcomes: [[AtomicU64; STATE_STORE_OUTCOME_COUNT]; STATE_STORE_OPERATION_COUNT],
    operation_duration_micros: [AtomicU64; STATE_STORE_OPERATION_COUNT],
    operation_duration_observations: [AtomicU64; STATE_STORE_OPERATION_COUNT],
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    page_records: AtomicU64,
}

impl StateStoreMetrics {
    pub(super) fn new() -> Self {
        Self {
            operation_outcomes: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            operation_duration_micros: std::array::from_fn(|_| AtomicU64::new(0)),
            operation_duration_observations: std::array::from_fn(|_| AtomicU64::new(0)),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            page_records: AtomicU64::new(0),
        }
    }

    pub(super) fn record_operation(
        &self,
        operation: StateStoreOperation,
        outcome: StateStoreOutcome,
        duration: Duration,
    ) {
        self.operation_outcomes[operation as usize][outcome as usize]
            .fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        self.operation_duration_micros[operation as usize].fetch_add(micros, Ordering::Relaxed);
        self.operation_duration_observations[operation as usize].fetch_add(1, Ordering::Relaxed);
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
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            page_records: self.page_records.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_metrics_record_outcomes_durations_and_byte_counters() {
        let metrics = StateStoreMetrics::new();
        metrics.record_operation(
            StateStoreOperation::Commit,
            StateStoreOutcome::Conflict,
            Duration::from_micros(7),
        );
        metrics.record_operation(
            StateStoreOperation::Commit,
            StateStoreOutcome::CommitUnknown,
            Duration::from_micros(3),
        );
        metrics.record_operation(
            StateStoreOperation::Get,
            StateStoreOutcome::Success,
            Duration::from_micros(1),
        );
        metrics.record_bytes_read(11);
        metrics.record_bytes_written(13);
        metrics.record_page_records(5);

        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot
                .operation_outcome_count(StateStoreOperation::Commit, StateStoreOutcome::Conflict),
            1
        );
        assert_eq!(
            snapshot.operation_outcome_count(
                StateStoreOperation::Commit,
                StateStoreOutcome::CommitUnknown
            ),
            1
        );
        assert_eq!(
            snapshot
                .operation_outcome_count(StateStoreOperation::Commit, StateStoreOutcome::Success),
            0
        );
        assert_eq!(
            snapshot.operation_duration_observations[StateStoreOperation::Commit as usize],
            2
        );
        assert_eq!(
            snapshot.operation_duration_observations[StateStoreOperation::Get as usize],
            1
        );
        assert_eq!(snapshot.bytes_read, 11);
        assert_eq!(snapshot.bytes_written, 13);
        assert_eq!(snapshot.page_records, 5);
    }
}
