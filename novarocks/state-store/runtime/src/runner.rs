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

//! Provider-neutral execution of one logical StateStore operation.
//!
//! Attempt identities and observations come from the open store instance. The
//! runtime keeps the observation it was issued and never reconstructs an
//! identity after losing sight of a commit.

use futures::future::BoxFuture;
use novarocks_state_store_api::{
    CommitObservation, CommitOutcome, CommitReceipt, StateStore, StateStoreError,
    StateStoreErrorKind, WriteTransaction,
};
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::StateStoreRunPolicy;

/// Application-owned observation hooks for the neutral runner.
///
/// Product domains keep their labels and counters. The StateStore runtime only
/// reports the four facts that its retry policy can produce.
pub trait StateStoreRunMetrics: Send + Sync {
    fn record_retry(&self);
    fn record_saturated_retry(&self);
    fn record_deadline(&self);
    fn record_unresolved(&self);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunSuccess<T> {
    pub value: T,
    pub receipt: CommitReceipt,
}

#[derive(Clone, Debug)]
pub enum RunFailure {
    Begin(StateStoreError),
    Operation(StateStoreError),
    RetryExhausted(StateStoreError),
    DefiniteFailure(StateStoreError),
    CommitUnknown {
        observation: CommitObservation,
        error: StateStoreError,
    },
    DeadlineExceeded,
}

/// Run a transaction body that has no externally visible side effects.
///
/// One absolute deadline covers reservation, body execution, commit, retry
/// backoff, and saturation waits for this call. Calling the runner again
/// creates a new budget, so any outer recovery loop must impose its own bound.
///
/// Each replay reserves a fresh attempt from the same open StateStore instance.
/// A dispatched commit that times out returns the exact observation capability
/// issued with that attempt; callers must resolve it or establish the effect by
/// an authoritative read before starting new work. Dropping or cancelling this
/// future must likewise be treated as possibly committed because cancellation
/// can race a dispatched provider commit. `DeadlineExceeded` is returned only
/// before commit dispatch; `CommitUnknown` means the runner lost certainty
/// after dispatch.
pub async fn run_side_effect_free<T, F>(
    store: &dyn StateStore,
    metrics: &dyn StateStoreRunMetrics,
    policy: StateStoreRunPolicy,
    purpose: &str,
    mut operation: F,
) -> Result<RunSuccess<T>, RunFailure>
where
    F: for<'a> FnMut(&'a mut dyn WriteTransaction) -> BoxFuture<'a, Result<T, StateStoreError>>,
{
    let deadline = Instant::now() + policy.operation_timeout();
    let mut attempts_spent = 0_usize;

    loop {
        let (write_attempt, observation) = match store.attempts().reserve() {
            Ok(reserved) => reserved,
            Err(error) if error.kind() == StateStoreErrorKind::Saturated => {
                metrics.record_saturated_retry();
                if !wait_for_retry(deadline, policy.backoff_after(attempts_spent + 1)).await {
                    return Err(deadline_exceeded(metrics));
                }
                continue;
            }
            Err(error) => return Err(RunFailure::Begin(error)),
        };

        attempts_spent += 1;

        let mut transaction =
            match timeout_at(deadline, store.begin_write(write_attempt, purpose)).await {
                Ok(Ok(transaction)) => transaction,
                Ok(Err(error)) => return Err(RunFailure::Begin(error)),
                Err(_) => return Err(deadline_exceeded(metrics)),
            };

        let value = match timeout_at(deadline, operation(transaction.as_mut())).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => return Err(RunFailure::Operation(error)),
            Err(_) => return Err(deadline_exceeded(metrics)),
        };

        let outcome = match timeout_at(deadline, transaction.commit()).await {
            Ok(outcome) => outcome,
            Err(_) => {
                metrics.record_deadline();
                metrics.record_unresolved();
                return Err(RunFailure::CommitUnknown {
                    observation,
                    error: StateStoreError::new(
                        StateStoreErrorKind::DeadlineExceeded,
                        "state store commit exceeded the operation budget",
                    ),
                });
            }
        };

        let retry_error = match outcome {
            CommitOutcome::Committed(receipt) => return Ok(RunSuccess { value, receipt }),
            CommitOutcome::Conflict(error) | CommitOutcome::TransientBeforeCommit(error) => error,
            CommitOutcome::DefiniteFailure(error) => {
                return Err(RunFailure::DefiniteFailure(error));
            }
            CommitOutcome::CommitUnknown(error) => {
                metrics.record_unresolved();
                return Err(RunFailure::CommitUnknown { observation, error });
            }
        };

        if attempts_spent >= policy.max_attempts() {
            return Err(RunFailure::RetryExhausted(retry_error));
        }

        metrics.record_retry();
        if !wait_for_retry(deadline, policy.backoff_after(attempts_spent)).await {
            return Err(deadline_exceeded(metrics));
        }
    }
}

async fn wait_for_retry(deadline: Instant, backoff: std::time::Duration) -> bool {
    let wake_at = (Instant::now() + backoff).min(deadline);
    sleep_until(wake_at).await;
    Instant::now() < deadline
}

fn deadline_exceeded(metrics: &dyn StateStoreRunMetrics) -> RunFailure {
    metrics.record_deadline();
    RunFailure::DeadlineExceeded
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use novarocks_state_store_testkit::testing::InMemoryStateStore;

    use super::*;

    #[derive(Default)]
    struct TestMetrics {
        retries: AtomicU64,
        saturated: AtomicU64,
        deadlines: AtomicU64,
        unresolved: AtomicU64,
    }

    impl StateStoreRunMetrics for TestMetrics {
        fn record_retry(&self) {
            self.retries.fetch_add(1, Ordering::Relaxed);
        }

        fn record_saturated_retry(&self) {
            self.saturated.fetch_add(1, Ordering::Relaxed);
        }

        fn record_deadline(&self) {
            self.deadlines.fetch_add(1, Ordering::Relaxed);
        }

        fn record_unresolved(&self) {
            self.unresolved.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn runner_uses_an_attempt_issued_by_the_open_store() {
        let store = InMemoryStateStore::new("runtime-test-cluster");
        let metrics = TestMetrics::default();

        let success = run_side_effect_free(
            &store,
            &metrics,
            StateStoreRunPolicy::default(),
            "runtime owner test",
            |_| Box::pin(async { Ok::<_, StateStoreError>(42_u8) }),
        )
        .await
        .expect("commit through StateStore SPI");

        assert_eq!(success.value, 42);
        assert_eq!(metrics.retries.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.unresolved.load(Ordering::Relaxed), 0);
    }
}
