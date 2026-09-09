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

//! Running one logical StateStore operation under an application budget.
//!
//! Every consumer of durable state goes through here, so attempt counting,
//! the overall deadline, and what counts as grounds for another try are
//! decided once. They used to be read out of the provider's limits, which let
//! storage dictate application policy and let two call sites disagree about
//! what a "known abort" permitted.
//!
//! # Identity is held, not re-derived
//!
//! An attempt's identity is issued by the open store instance. A caller that
//! loses sight of a commit does not reconstruct an id to ask about; it keeps
//! the [`CommitObservation`] it was handed and asks that. This is why the
//! failure carrying an unknown outcome carries the observation itself.

use futures::future::BoxFuture;
use novarocks_state_store_api::{
    CommitObservation, CommitOutcome, CommitReceipt, StateStore, StateStoreError,
    StateStoreErrorKind, WriteTransaction,
};
use tokio::time::{Instant, sleep_until, timeout_at};

use super::metrics::StateStoreMetrics;
use super::policy::StateStoreRunPolicy;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunSuccess<T> {
    pub value: T,
    pub receipt: CommitReceipt,
}

/// Why one logical operation did not produce a proven commit.
#[derive(Clone, Debug)]
pub enum RunFailure {
    /// The store refused to start a transaction.
    Begin(StateStoreError),
    /// The transaction body itself failed. No write landed.
    Operation(StateStoreError),
    /// Every permitted attempt was spent on retryable failures.
    RetryExhausted(StateStoreError),
    /// The store proved the write can never land.
    DefiniteFailure(StateStoreError),
    /// The commit may or may not have landed.
    ///
    /// The observation addresses the exact attempt in question. Resolving it
    /// is the only honest way to find out; assuming either answer is a bug,
    /// and starting fresh work without resolving it risks a double effect.
    CommitUnknown {
        observation: CommitObservation,
        error: StateStoreError,
    },
    /// The operation budget ran out.
    DeadlineExceeded,
}

/// Runs a transaction body that has no externally visible side effects.
///
/// The body may be replayed after a conflict, after a failure proven to have
/// happened before commit, or after admission saturation. Each replay runs
/// under a *new* attempt: the previous one is proven not to have committed, so
/// reusing its identity would buy nothing.
///
/// # Budget
///
/// One absolute deadline is established on entry and covers everything after
/// it: reservations, the body, the commit, waits, and backoff. It bounds *this*
/// call and nothing wider: a caller that calls back in starts a new operation
/// with a new budget.
///
/// That is deliberate rather than an oversight, and it is why the bound on a
/// recovery loop has to live in the caller. Re-entering after an attempt was
/// *proven* not to have committed is a genuinely new operation and should get a
/// full budget; re-entering to keep pushing at the same one would spend budgets
/// without limit, so a caller that replays counts its own replays and stops.
/// See `MAX_PROVEN_UNCOMMITTED_REPLAYS` in the catalog attachment repository
/// for the shape that is expected of such a loop.
///
/// # Cancellation safety
///
/// If the returned future is dropped, the caller must treat the operation as
/// possibly committed. Recovery is to resolve the observation for the attempt
/// in flight, or to perform an authoritative read that establishes the effect.
/// A dropped future is not evidence that nothing happened.
pub async fn run_side_effect_free<T, F>(
    store: &dyn StateStore,
    metrics: &StateStoreMetrics,
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
        // Reserving touches no storage, so saturation costs no attempt. It is
        // still bounded, by the same deadline as everything else.
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
                // The commit was dispatched and we stopped watching. That is
                // exactly the case the observation exists for. Both counters
                // fire: one says the budget ran out, the other says this
                // operation ended with nothing proven either way.
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

/// Sleeps for the backoff, clamped to the deadline. Returns false when the
/// budget is spent, so a backoff can never extend an operation.
async fn wait_for_retry(deadline: Instant, backoff: std::time::Duration) -> bool {
    let wake_at = (Instant::now() + backoff).min(deadline);
    sleep_until(wake_at).await;
    Instant::now() < deadline
}

fn deadline_exceeded(metrics: &StateStoreMetrics) -> RunFailure {
    metrics.record_deadline();
    RunFailure::DeadlineExceeded
}
