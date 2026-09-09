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

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::StateStoreProviderId;

pub const STATE_STORE_OPERATION_COUNT: usize = 6;
pub const STATE_STORE_OUTCOME_COUNT: usize = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum StateStoreOperation {
    Begin = 0,
    Get = 1,
    Range = 2,
    Put = 3,
    Delete = 4,
    Commit = 5,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum StateStoreOutcome {
    Success = 0,
    Error = 1,
    Conflict = 2,
    TransientBeforeCommit = 3,
    DefiniteFailure = 4,
    CommitUnknown = 5,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreMetricsSnapshot {
    pub provider: StateStoreProviderId,
    pub begin_count: u64,
    pub get_count: u64,
    pub range_count: u64,
    pub put_count: u64,
    pub delete_count: u64,
    pub commit_count: u64,
    pub operation_outcomes: [[u64; STATE_STORE_OUTCOME_COUNT]; STATE_STORE_OPERATION_COUNT],
    pub operation_duration_micros: [u64; STATE_STORE_OPERATION_COUNT],
    pub operation_duration_observations: [u64; STATE_STORE_OPERATION_COUNT],
    pub retry_count: u64,
    pub deadline_count: u64,
    pub blocking_failure_count: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub page_records: u64,
    pub notification_lag_micros: u64,
    pub notification_lag_observations: u64,
}

impl StateStoreMetricsSnapshot {
    pub fn operation_outcome_count(
        &self,
        operation: StateStoreOperation,
        outcome: StateStoreOutcome,
    ) -> u64 {
        self.operation_outcomes[operation as usize][outcome as usize]
    }

    pub fn operation_duration_micros(&self, operation: StateStoreOperation) -> u64 {
        self.operation_duration_micros[operation as usize]
    }

    pub fn operation_duration_observations(&self, operation: StateStoreOperation) -> u64 {
        self.operation_duration_observations[operation as usize]
    }
}

#[derive(Debug)]
pub struct StateStoreMetrics {
    provider: StateStoreProviderId,
    operation_outcomes: [[AtomicU64; STATE_STORE_OUTCOME_COUNT]; STATE_STORE_OPERATION_COUNT],
    operation_duration_micros: [AtomicU64; STATE_STORE_OPERATION_COUNT],
    operation_duration_observations: [AtomicU64; STATE_STORE_OPERATION_COUNT],
    retry_count: AtomicU64,
    deadline_count: AtomicU64,
    blocking_failure_count: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    page_records: AtomicU64,
    notification_lag_micros: AtomicU64,
    notification_lag_observations: AtomicU64,
}

impl StateStoreMetrics {
    pub fn new(provider: StateStoreProviderId) -> Self {
        Self {
            provider,
            operation_outcomes: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            operation_duration_micros: std::array::from_fn(|_| AtomicU64::new(0)),
            operation_duration_observations: std::array::from_fn(|_| AtomicU64::new(0)),
            retry_count: AtomicU64::new(0),
            deadline_count: AtomicU64::new(0),
            blocking_failure_count: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            page_records: AtomicU64::new(0),
            notification_lag_micros: AtomicU64::new(0),
            notification_lag_observations: AtomicU64::new(0),
        }
    }

    pub const fn provider(&self) -> StateStoreProviderId {
        self.provider
    }

    pub fn record_operation(
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

    pub fn record_retry(&self) {
        self.retry_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_deadline(&self) {
        self.deadline_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_blocking_failure(&self) {
        self.blocking_failure_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_bytes_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_bytes_written(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_page_records(&self, records: u64) {
        self.page_records.fetch_add(records, Ordering::Relaxed);
    }

    pub fn record_notification_lag(&self, lag: Duration) {
        let micros = u64::try_from(lag.as_micros()).unwrap_or(u64::MAX);
        self.notification_lag_micros
            .fetch_max(micros, Ordering::Relaxed);
        self.notification_lag_observations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StateStoreMetricsSnapshot {
        let operation_outcomes = std::array::from_fn(|operation| {
            std::array::from_fn(|outcome| {
                self.operation_outcomes[operation][outcome].load(Ordering::Relaxed)
            })
        });
        StateStoreMetricsSnapshot {
            provider: self.provider,
            begin_count: operation_total(&operation_outcomes, StateStoreOperation::Begin),
            get_count: operation_total(&operation_outcomes, StateStoreOperation::Get),
            range_count: operation_total(&operation_outcomes, StateStoreOperation::Range),
            put_count: operation_total(&operation_outcomes, StateStoreOperation::Put),
            delete_count: operation_total(&operation_outcomes, StateStoreOperation::Delete),
            commit_count: operation_total(&operation_outcomes, StateStoreOperation::Commit),
            operation_outcomes,
            operation_duration_micros: std::array::from_fn(|operation| {
                self.operation_duration_micros[operation].load(Ordering::Relaxed)
            }),
            operation_duration_observations: std::array::from_fn(|operation| {
                self.operation_duration_observations[operation].load(Ordering::Relaxed)
            }),
            retry_count: self.retry_count.load(Ordering::Relaxed),
            deadline_count: self.deadline_count.load(Ordering::Relaxed),
            blocking_failure_count: self.blocking_failure_count.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            page_records: self.page_records.load(Ordering::Relaxed),
            notification_lag_micros: self.notification_lag_micros.load(Ordering::Relaxed),
            notification_lag_observations: self
                .notification_lag_observations
                .load(Ordering::Relaxed),
        }
    }
}

fn operation_total(
    outcomes: &[[u64; STATE_STORE_OUTCOME_COUNT]; STATE_STORE_OPERATION_COUNT],
    operation: StateStoreOperation,
) -> u64 {
    outcomes[operation as usize].iter().sum()
}
