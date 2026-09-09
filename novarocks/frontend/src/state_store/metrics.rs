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

//! What the application observes about its own StateStore operations.
//!
//! Deliberately separate from anything a provider records. A provider counts
//! workers, locks and physical I/O; the application counts how often its own
//! policy made it try again and how often it ran out of budget. Mixing the two
//! is what produced the previous arrangement, where three consumers each
//! invented a fake provider identity so they could label counters that were
//! never about a provider in the first place.

use std::sync::atomic::{AtomicU64, Ordering};

/// Which application owner a set of counters belongs to.
///
/// A business owner, not a storage provider. The distinction matters because
/// several owners share one store, and attributing their retries to the store
/// tells you nothing about which workload is struggling.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StateStoreConsumer(&'static str);

impl StateStoreConsumer {
    pub const CATALOG_ATTACHMENT: Self = Self("catalog-attachment");
    pub const MV_ACCELERATOR: Self = Self("mv-accelerator");
    pub const GC_OBSERVATION: Self = Self("gc-observation");

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for StateStoreConsumer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateStoreMetricsSnapshot {
    pub consumer: StateStoreConsumer,
    /// Retries caused by a conflict or a failure proven to precede commit.
    pub retries: u64,
    /// Retries caused by the instance being at its attempt ceiling. Counted
    /// apart from `retries` because it reports pressure on a resource bound,
    /// not contention over data.
    pub saturated_retries: u64,
    /// Operations that ran out of their budget.
    pub deadlines: u64,
    /// Operations that ended without proof either way.
    pub unresolved: u64,
}

/// Application-side counters for one StateStore consumer.
#[derive(Debug)]
pub struct StateStoreMetrics {
    consumer: StateStoreConsumer,
    retries: AtomicU64,
    saturated_retries: AtomicU64,
    deadlines: AtomicU64,
    unresolved: AtomicU64,
}

impl StateStoreMetrics {
    pub fn new(consumer: StateStoreConsumer) -> Self {
        Self {
            consumer,
            retries: AtomicU64::new(0),
            saturated_retries: AtomicU64::new(0),
            deadlines: AtomicU64::new(0),
            unresolved: AtomicU64::new(0),
        }
    }

    pub const fn consumer(&self) -> StateStoreConsumer {
        self.consumer
    }

    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_saturated_retry(&self) {
        self.saturated_retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_deadline(&self) {
        self.deadlines.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_unresolved(&self) {
        self.unresolved.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StateStoreMetricsSnapshot {
        StateStoreMetricsSnapshot {
            consumer: self.consumer,
            retries: self.retries.load(Ordering::Relaxed),
            saturated_retries: self.saturated_retries.load(Ordering::Relaxed),
            deadlines: self.deadlines.load(Ordering::Relaxed),
            unresolved: self.unresolved.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_are_attributed_to_a_business_owner() {
        let metrics = StateStoreMetrics::new(StateStoreConsumer::CATALOG_ATTACHMENT);
        metrics.record_retry();
        metrics.record_retry();
        metrics.record_saturated_retry();
        metrics.record_deadline();
        metrics.record_unresolved();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.consumer, StateStoreConsumer::CATALOG_ATTACHMENT);
        assert_eq!(snapshot.consumer.as_str(), "catalog-attachment");
        assert_eq!(snapshot.retries, 2);
        // Contention and resource pressure stay apart: one says the data is
        // busy, the other says this instance is.
        assert_eq!(snapshot.saturated_retries, 1);
        assert_eq!(snapshot.deadlines, 1);
        assert_eq!(snapshot.unresolved, 1);
    }

    #[test]
    fn each_owner_counts_separately() {
        let catalog = StateStoreMetrics::new(StateStoreConsumer::CATALOG_ATTACHMENT);
        let mv = StateStoreMetrics::new(StateStoreConsumer::MV_ACCELERATOR);
        catalog.record_retry();

        assert_eq!(catalog.snapshot().retries, 1);
        assert_eq!(mv.snapshot().retries, 0);
        assert_ne!(catalog.snapshot().consumer, mv.snapshot().consumer);
    }
}
