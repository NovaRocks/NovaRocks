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

//! "An attachment write of ours committed", as a signal and nothing more.
//!
//! This is the low-latency half of catalog reconciliation. It replaces the
//! StateStore change feed the controller used to poll, and it deliberately
//! keeps only the one bit that feed was ever read for: something under the
//! attachment prefix moved. It carries no key, no version and no payload,
//! because the consumer answers it with a complete authoritative reread — a
//! payload here could only be a second, weaker source of truth.
//!
//! # Why it is bounded and coalescing
//!
//! The channel holds one slot. A burst of DDL therefore collapses into a
//! single reconcile round rather than queueing one full enumeration per
//! statement, and a consumer that is mid-scan when writes land observes
//! exactly one further round afterwards. Nothing here can grow with write
//! rate, and nothing here can block a writer: publishing is a store into that
//! one slot.
//!
//! # What it is not
//!
//! It is not delivery: a signal only reaches subscribers of *this* repository
//! instance, so a write another frontend made — or one made through a second
//! repository over the same store — produces no wakeup at all. Missing a
//! wakeup is therefore normal, and correctness never rests on receiving one.
//! The consumer's periodic sweep is what bounds how long such a write can stay
//! unobserved.

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::watch;

/// Publishes committed-attachment-write wakeups to every subscriber.
#[derive(Debug)]
pub struct CatalogAttachmentWakeup {
    sequence: watch::Sender<u64>,
    published: AtomicU64,
}

impl Default for CatalogAttachmentWakeup {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogAttachmentWakeup {
    pub fn new() -> Self {
        Self {
            sequence: watch::channel(0).0,
            published: AtomicU64::new(0),
        }
    }

    /// Records one write whose durable effect is confirmed.
    ///
    /// The bar is the *effect*, not the authorship: a caller that recovered an
    /// undecidable commit by reading the authoritative record and finding the
    /// record gone (or its own exact identity present) has confirmed the
    /// effect, even though it cannot say whose write produced it. What must
    /// never publish is an attempt still in doubt — a wakeup that turned out to
    /// mean "maybe" would train the consumer to reconcile on non-events, and
    /// the periodic sweep already covers a write that did land unannounced.
    pub(crate) fn publish(&self) {
        self.published.fetch_add(1, Ordering::Relaxed);
        self.sequence
            .send_modify(|sequence| *sequence = sequence.wrapping_add(1));
    }

    /// A signal that reports every wakeup published after this call.
    pub fn subscribe(&self) -> CatalogAttachmentWakeupSignal {
        CatalogAttachmentWakeupSignal {
            sequence: self.sequence.subscribe(),
        }
    }

    /// Total wakeups published by this repository instance. Observability and
    /// tests only: a consumer must never derive freshness from a count.
    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }
}

/// One consumer's view of the wakeup channel.
///
/// Cloning shares the channel and keeps the clone's own "already seen"
/// position, so two controllers over one repository each get woken; a single
/// notification handed to whoever waited first would leave the other blind
/// until its next sweep.
#[derive(Clone, Debug)]
pub struct CatalogAttachmentWakeupSignal {
    sequence: watch::Receiver<u64>,
}

impl CatalogAttachmentWakeupSignal {
    /// Waits for at least one wakeup published since the last observation.
    ///
    /// Returns `false` once the publishing repository is gone, which is a
    /// permanent answer: the caller must fall back to its own timer rather
    /// than treat the immediate return as a wakeup and spin on it.
    pub async fn changed(&mut self) -> bool {
        self.sequence.changed().await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn a_burst_of_writes_collapses_into_one_wakeup() {
        let wakeup = CatalogAttachmentWakeup::new();
        let mut signal = wakeup.subscribe();
        for _ in 0..64 {
            wakeup.publish();
        }

        assert!(signal.changed().await, "the burst wakes the consumer once");
        assert_eq!(wakeup.published(), 64);
        // The slot held one pending wakeup, not 64: a second wait has nothing
        // queued behind the first.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), signal.changed())
                .await
                .is_err(),
            "a bounded channel must not queue one round per write"
        );
    }

    #[tokio::test]
    async fn every_subscriber_is_woken_not_just_the_first() {
        let wakeup = CatalogAttachmentWakeup::new();
        let mut first = wakeup.subscribe();
        let mut second = wakeup.subscribe();
        wakeup.publish();

        assert!(first.changed().await);
        assert!(second.changed().await);
    }

    #[tokio::test]
    async fn a_dropped_publisher_reports_a_permanent_end_rather_than_a_wakeup() {
        let wakeup = Arc::new(CatalogAttachmentWakeup::new());
        let mut signal = wakeup.subscribe();
        drop(wakeup);

        assert!(!signal.changed().await);
        // Still false, never a spurious wakeup: the consumer can park on its
        // own timer instead of spinning on a closed channel.
        assert!(!signal.changed().await);
    }

    #[tokio::test]
    async fn a_signal_ignores_wakeups_published_before_it_subscribed() {
        let wakeup = CatalogAttachmentWakeup::new();
        wakeup.publish();
        let mut signal = wakeup.subscribe();

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), signal.changed())
                .await
                .is_err(),
            "a fresh subscriber starts from the current position"
        );
    }
}
