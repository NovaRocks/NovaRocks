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

// Design: ADR-0143 (docs/adr/ADR-0143-state-store-answers-only-issued-attempts.md)

//! The cadence that returns abandoned write attempts to the store.
//!
//! The attempt contract is explicit that cleanup is *driven*, never spawned by
//! the supervisor: the supervisor owns no task, so nothing happens unless a
//! host asks. This module is that host obligation made real.
//!
//! Without it, an attempt whose commit was dispatched and whose handles were
//! then dropped keeps two things forever: a capacity slot on the instance, and
//! a row of provider-side evidence. Enough of them and the instance refuses new
//! writes while its evidence table grows — which is precisely the failure the
//! bounded-evidence rule exists to prevent. A mechanism with no caller is not a
//! mechanism.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use novarocks_state_store_api::StateStore;
use tokio::task::JoinHandle;

/// How often abandoned attempts are swept.
///
/// Abandonment is the exception — a provider releases what it witnessed itself
/// — so this is a safety net rather than a hot path, and it is deliberately
/// slow enough to cost nothing on an idle instance.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Returns abandoned attempts to the store on a fixed cadence.
pub struct AbandonedAttemptSweeper {
    store: Arc<dyn StateStore>,
    interval: Duration,
    stopping: Arc<AtomicBool>,
    worker: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl AbandonedAttemptSweeper {
    pub fn new(store: Arc<dyn StateStore>) -> Self {
        Self::with_interval(store, DEFAULT_SWEEP_INTERVAL)
    }

    pub fn with_interval(store: Arc<dyn StateStore>, interval: Duration) -> Self {
        Self {
            store,
            interval,
            stopping: Arc::new(AtomicBool::new(false)),
            worker: std::sync::Mutex::new(None),
        }
    }

    /// Runs one sweep and reports how many attempts were released.
    ///
    /// A provider that is not ready to release yet says so without it counting
    /// as a fault, so an ordinary sweep on a busy instance is quiet rather than
    /// noisy.
    pub async fn sweep_once(&self) -> Result<usize, String> {
        self.store
            .attempts()
            .drain_abandoned_attempts()
            .await
            .map_err(|error| error.to_string())
    }

    pub fn start(self: &Arc<Self>) -> Result<(), String> {
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| "abandoned attempt sweeper worker lock is poisoned".to_string())?;
        if worker.is_some() {
            return Err("abandoned attempt sweeper is already running".to_string());
        }
        self.stopping.store(false, Ordering::Release);
        let sweeper = Arc::clone(self);
        *worker = Some(tokio::spawn(async move {
            while !sweeper.stopping.load(Ordering::Acquire) {
                tokio::time::sleep(sweeper.interval).await;
                if sweeper.stopping.load(Ordering::Acquire) {
                    break;
                }
                match sweeper.sweep_once().await {
                    Ok(0) => {}
                    Ok(released) => {
                        tracing::debug!(released, "released abandoned state store attempts");
                    }
                    // Reported, not retried harder: the entries stay queued and
                    // stay charged, so the next sweep sees them again.
                    Err(error) => {
                        tracing::warn!(%error, "sweeping abandoned state store attempts failed");
                    }
                }
            }
        }));
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        let handle = self.worker.lock().ok().and_then(|mut worker| worker.take());
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use novarocks_state_store_testkit::testing::InMemoryStateStore;

    use super::*;

    #[tokio::test]
    async fn a_sweep_returns_an_abandoned_attempt_to_the_instance() {
        let store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::with_limits_and_capacity(
            "sweeper",
            Default::default(),
            NonZeroUsize::new(1).expect("capacity"),
        ));
        let sweeper = AbandonedAttemptSweeper::new(Arc::clone(&store));

        // Dispatch, then lose every handle without settling: the slot stays
        // charged because the write may still be landing.
        let (attempt, observation) = store.attempts().reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");
        drop(attempt);
        drop(observation);
        assert_eq!(store.attempts().abandoned(), 1);
        assert!(
            store.attempts().reserve().is_err(),
            "the instance is at its ceiling while the attempt is owed"
        );

        assert_eq!(sweeper.sweep_once().await.expect("sweep"), 1);
        assert_eq!(store.attempts().abandoned(), 0);
        store
            .attempts()
            .reserve()
            .expect("capacity comes back once the attempt is returned");
    }

    #[tokio::test]
    async fn an_idle_sweep_reports_nothing_and_changes_nothing() {
        let store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new("sweeper-idle"));
        let sweeper = AbandonedAttemptSweeper::new(Arc::clone(&store));
        assert_eq!(sweeper.sweep_once().await.expect("sweep"), 0);
        assert_eq!(store.attempts().outstanding(), 0);
    }
}
