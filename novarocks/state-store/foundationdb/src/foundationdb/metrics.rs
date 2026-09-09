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
//! Observability is no longer part of the StateStore contract: a store exposes
//! no metrics method, so nothing outside this crate can read these counters.
//! They are kept because they are how this provider's own behaviour is
//! described in logs and in its unit tests, and because a shared metrics type
//! in the domain crate would put an observability vocabulary back into a
//! contract that deliberately dropped it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use novarocks_state_store_api::StateStoreProviderId;

pub(super) const OPERATION_COUNT: usize = 6;
pub(super) const OUTCOME_COUNT: usize = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub(super) enum ProviderOperation {
    Begin = 0,
    Get = 1,
    Range = 2,
    Put = 3,
    Delete = 4,
    Commit = 5,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub(super) enum ProviderOutcome {
    Success = 0,
    Error = 1,
    Conflict = 2,
    TransientBeforeCommit = 3,
    DefiniteFailure = 4,
    CommitUnknown = 5,
}

#[derive(Debug)]
pub(super) struct ProviderMetrics {
    #[cfg_attr(not(test), allow(dead_code))]
    provider: StateStoreProviderId,
    operation_outcomes: [[AtomicU64; OUTCOME_COUNT]; OPERATION_COUNT],
    operation_duration_micros: [AtomicU64; OPERATION_COUNT],
    operation_duration_observations: [AtomicU64; OPERATION_COUNT],
    retry_count: AtomicU64,
    deadline_count: AtomicU64,
    blocking_failure_count: AtomicU64,
    bytes_written: AtomicU64,
    page_records: AtomicU64,
}

impl ProviderMetrics {
    pub(super) fn new(provider: StateStoreProviderId) -> Self {
        Self {
            provider,
            operation_outcomes: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            operation_duration_micros: std::array::from_fn(|_| AtomicU64::new(0)),
            operation_duration_observations: std::array::from_fn(|_| AtomicU64::new(0)),
            retry_count: AtomicU64::new(0),
            deadline_count: AtomicU64::new(0),
            blocking_failure_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            page_records: AtomicU64::new(0),
        }
    }

    pub(super) fn record_operation(
        &self,
        operation: ProviderOperation,
        outcome: ProviderOutcome,
        duration: Duration,
    ) {
        self.operation_outcomes[operation as usize][outcome as usize]
            .fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        self.operation_duration_micros[operation as usize].fetch_add(micros, Ordering::Relaxed);
        self.operation_duration_observations[operation as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_retry(&self) {
        self.retry_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_deadline(&self) {
        self.deadline_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_blocking_failure(&self) {
        self.blocking_failure_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_bytes_written(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(super) fn record_page_records(&self, records: u64) {
        self.page_records.fetch_add(records, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> ProviderMetricsSnapshot {
        let operation_outcomes = std::array::from_fn(|operation| {
            std::array::from_fn(|outcome| {
                self.operation_outcomes[operation][outcome].load(Ordering::Relaxed)
            })
        });
        ProviderMetricsSnapshot {
            provider: self.provider,
            commit_count: operation_total(&operation_outcomes, ProviderOperation::Commit),
            operation_outcomes,
            operation_duration_observations: std::array::from_fn(|operation| {
                self.operation_duration_observations[operation].load(Ordering::Relaxed)
            }),
            retry_count: self.retry_count.load(Ordering::Relaxed),
            deadline_count: self.deadline_count.load(Ordering::Relaxed),
            blocking_failure_count: self.blocking_failure_count.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            page_records: self.page_records.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProviderMetricsSnapshot {
    pub provider: StateStoreProviderId,
    pub commit_count: u64,
    pub operation_outcomes: [[u64; OUTCOME_COUNT]; OPERATION_COUNT],
    pub operation_duration_observations: [u64; OPERATION_COUNT],
    pub retry_count: u64,
    pub deadline_count: u64,
    pub blocking_failure_count: u64,
    pub bytes_written: u64,
    pub page_records: u64,
}

#[cfg(test)]
impl ProviderMetricsSnapshot {
    pub(super) fn operation_duration_observations(&self, operation: ProviderOperation) -> u64 {
        self.operation_duration_observations[operation as usize]
    }
}

#[cfg(test)]
fn operation_total(
    outcomes: &[[u64; OUTCOME_COUNT]; OPERATION_COUNT],
    operation: ProviderOperation,
) -> u64 {
    outcomes[operation as usize].iter().sum()
}
