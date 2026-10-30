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
    CommitObservation, CommitOutcome, CommitReceipt, OriginalWriteTransaction, StateStore,
    StateStoreError, StateStoreErrorKind, StateStoreExecutionContext, StateStoreExecutionFailure,
    WriteAttempt, WriteTransaction,
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

/// Stable receiving of the SAME observation issued by the open instance.
/// This is observation custody only, not transaction or resource admission.
/// Implementations must publish their receiving fields before returning or
/// notifying, and drop any retired observation outside their metadata lock.
pub trait StateStoreRunReceiving: Send + Sync {
    fn receive_attempt(
        &self,
        observation: &CommitObservation,
    ) -> Result<(), StateStoreExecutionFailure>;
    /// Retire only the SAME slot's actual terminal NotCommitted observation.
    /// This grants no right to publish or settle an attempt.
    fn retire_not_committed(
        &self,
        observation: &CommitObservation,
    ) -> Result<(), StateStoreExecutionFailure>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunSuccess<T> {
    pub value: T,
    pub receipt: CommitReceipt,
}

#[derive(Clone, Debug)]
pub enum RunFailure<E = StateStoreError> {
    Begin(E),
    Operation(E),
    RetryExhausted(E),
    DefiniteFailure(E),
    CommitUnknown {
        observation: CommitObservation,
        error: E,
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
    run::<LegacyMode, T, _>(
        &LegacyMode,
        store,
        metrics,
        policy,
        purpose,
        deadline,
        move |transaction| operation(transaction),
    )
    .await
}

/// Borrow the selected provider's original transaction on the same execution
/// owner. Unsupported original providers refuse; this never calls legacy APIs.
/// The caller must retain that owner's physical custody through actual exit.
pub async fn run_side_effect_free_in<T, F>(
    store: &dyn StateStore,
    metrics: &dyn StateStoreRunMetrics,
    policy: StateStoreRunPolicy,
    purpose: &str,
    execution: &StateStoreExecutionContext,
    mut operation: F,
) -> Result<RunSuccess<T>, RunFailure<StateStoreExecutionFailure>>
where
    F: for<'a> FnMut(
        &'a mut dyn OriginalWriteTransaction,
    ) -> BoxFuture<'a, Result<T, StateStoreExecutionFailure>>,
{
    run_original(
        store,
        metrics,
        policy,
        purpose,
        execution,
        None,
        move |transaction| operation(transaction),
    )
    .await
}

/// Install every issued observation in the caller's original stable receiver
/// before the first provider begin call, body invocation or await.
pub async fn run_side_effect_free_in_receiving<T, F>(
    store: &dyn StateStore,
    metrics: &dyn StateStoreRunMetrics,
    policy: StateStoreRunPolicy,
    purpose: &str,
    execution: &StateStoreExecutionContext,
    receiving: &dyn StateStoreRunReceiving,
    mut operation: F,
) -> Result<RunSuccess<T>, RunFailure<StateStoreExecutionFailure>>
where
    F: for<'a> FnMut(
        &'a mut dyn OriginalWriteTransaction,
    ) -> BoxFuture<'a, Result<T, StateStoreExecutionFailure>>,
{
    run_original(
        store,
        metrics,
        policy,
        purpose,
        execution,
        Some(receiving),
        move |transaction| operation(transaction),
    )
    .await
}

async fn run_original<T, F>(
    store: &dyn StateStore,
    metrics: &dyn StateStoreRunMetrics,
    policy: StateStoreRunPolicy,
    purpose: &str,
    execution: &StateStoreExecutionContext,
    receiving: Option<&dyn StateStoreRunReceiving>,
    mut operation: F,
) -> Result<RunSuccess<T>, RunFailure<StateStoreExecutionFailure>>
where
    F: for<'a> FnMut(
        &'a mut dyn OriginalWriteTransaction,
    ) -> BoxFuture<'a, Result<T, StateStoreExecutionFailure>>,
{
    let deadline =
        (Instant::now() + policy.operation_timeout()).min(Instant::from_std(execution.deadline()));
    let execution = execution.with_deadline(deadline.into_std());
    let mode = OriginalMode {
        execution: &execution,
        receiving,
    };
    run::<OriginalMode<'_>, T, _>(
        &mode,
        store,
        metrics,
        policy,
        purpose,
        deadline,
        move |transaction| operation(transaction),
    )
    .await
}

// This closed mode only chooses a transaction API and preserves its error.
// Reservation, attempts, deadline, replay and Unknown policy have one author.
trait RunMode: Sync {
    type Transaction: ?Sized + Send + 'static;
    type Error: Send;
    fn from_store(error: StateStoreError) -> Self::Error;
    fn kind(error: &Self::Error) -> Option<StateStoreErrorKind>;
    fn receive_attempt(&self, _: &CommitObservation) -> Result<(), Self::Error> {
        Ok(())
    }
    fn retire_not_committed(&self, _: &CommitObservation) -> Result<(), Self::Error> {
        Ok(())
    }
    fn begin<'a>(
        &'a self,
        store: &'a dyn StateStore,
        attempt: WriteAttempt,
        purpose: &'a str,
    ) -> BoxFuture<'a, Result<Box<Self::Transaction>, Self::Error>>;
    fn commit<'a>(
        &'a self,
        transaction: Box<Self::Transaction>,
    ) -> BoxFuture<'a, Result<CommitOutcome, Self::Error>>;
}
struct LegacyMode;
impl RunMode for LegacyMode {
    type Transaction = dyn WriteTransaction;
    type Error = StateStoreError;
    fn from_store(error: StateStoreError) -> Self::Error {
        error
    }
    fn kind(error: &Self::Error) -> Option<StateStoreErrorKind> {
        Some(error.kind())
    }
    fn begin<'a>(
        &'a self,
        store: &'a dyn StateStore,
        attempt: WriteAttempt,
        purpose: &'a str,
    ) -> BoxFuture<'a, Result<Box<Self::Transaction>, Self::Error>> {
        Box::pin(store.begin_write(attempt, purpose))
    }
    fn commit<'a>(
        &'a self,
        transaction: Box<Self::Transaction>,
    ) -> BoxFuture<'a, Result<CommitOutcome, Self::Error>> {
        Box::pin(async move { Ok(transaction.commit().await) })
    }
}
struct OriginalMode<'a> {
    execution: &'a StateStoreExecutionContext,
    receiving: Option<&'a dyn StateStoreRunReceiving>,
}
impl RunMode for OriginalMode<'_> {
    type Transaction = dyn OriginalWriteTransaction;
    type Error = StateStoreExecutionFailure;
    fn from_store(error: StateStoreError) -> Self::Error {
        error.into_original_failure()
    }
    fn kind(error: &Self::Error) -> Option<StateStoreErrorKind> {
        error
            .downcast_ref::<StateStoreError>()
            .map(StateStoreError::kind)
    }
    fn receive_attempt(&self, observation: &CommitObservation) -> Result<(), Self::Error> {
        match self.receiving {
            Some(receiving) => receiving.receive_attempt(observation),
            None => Ok(()),
        }
    }
    fn retire_not_committed(&self, observation: &CommitObservation) -> Result<(), Self::Error> {
        match self.receiving {
            Some(receiving) => receiving.retire_not_committed(observation),
            None => Ok(()),
        }
    }
    fn begin<'a>(
        &'a self,
        store: &'a dyn StateStore,
        attempt: WriteAttempt,
        purpose: &'a str,
    ) -> BoxFuture<'a, Result<Box<Self::Transaction>, Self::Error>> {
        Box::pin(store.begin_original_write_in(attempt, purpose, self.execution))
    }
    fn commit<'a>(
        &'a self,
        mut transaction: Box<Self::Transaction>,
    ) -> BoxFuture<'a, Result<CommitOutcome, Self::Error>> {
        Box::pin(async move { transaction.commit_in().await })
    }
}
async fn run<M, T, F>(
    mode: &M,
    store: &dyn StateStore,
    metrics: &dyn StateStoreRunMetrics,
    policy: StateStoreRunPolicy,
    purpose: &str,
    deadline: Instant,
    mut operation: F,
) -> Result<RunSuccess<T>, RunFailure<M::Error>>
where
    M: RunMode,
    F: for<'a> FnMut(&'a mut M::Transaction) -> BoxFuture<'a, Result<T, M::Error>>,
{
    let mut attempts_spent = 0_usize;

    loop {
        let (write_attempt, observation) = match store.attempts().reserve().map_err(M::from_store) {
            Ok(reserved) => reserved,
            Err(error) if M::kind(&error) == Some(StateStoreErrorKind::Saturated) => {
                metrics.record_saturated_retry();
                if !wait_for_retry(deadline, policy.backoff_after(attempts_spent + 1)).await {
                    return Err(deadline_exceeded(metrics));
                }
                continue;
            }
            Err(error) => return Err(RunFailure::Begin(error)),
        };

        mode.receive_attempt(&observation)
            .map_err(RunFailure::Begin)?;
        attempts_spent += 1;

        let mut transaction =
            match timeout_at(deadline, mode.begin(store, write_attempt, purpose)).await {
                Ok(Ok(transaction)) => transaction,
                Ok(Err(error)) => return Err(RunFailure::Begin(error)),
                Err(_) => return Err(deadline_exceeded(metrics)),
            };

        let value = match timeout_at(deadline, operation(transaction.as_mut())).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => return Err(RunFailure::Operation(error)),
            Err(_) => return Err(deadline_exceeded(metrics)),
        };

        let outcome = match timeout_at(deadline, mode.commit(transaction)).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => {
                // No typed outcome proves a bare physical failure was before
                // commit. Keep this SAME observation and never replay it.
                metrics.record_unresolved();
                return Err(RunFailure::CommitUnknown { observation, error });
            }
            Err(_) => {
                metrics.record_deadline();
                metrics.record_unresolved();
                return Err(RunFailure::CommitUnknown {
                    observation,
                    error: M::from_store(StateStoreError::new(
                        StateStoreErrorKind::DeadlineExceeded,
                        "state store commit exceeded the operation budget",
                    )),
                });
            }
        };

        let retry_error = match outcome {
            CommitOutcome::Committed(receipt) => return Ok(RunSuccess { value, receipt }),
            CommitOutcome::Conflict(error) | CommitOutcome::TransientBeforeCommit(error) => error,
            CommitOutcome::DefiniteFailure(error) => {
                mode.retire_not_committed(&observation)
                    .map_err(RunFailure::Operation)?;
                return Err(RunFailure::DefiniteFailure(M::from_store(error)));
            }
            CommitOutcome::CommitUnknown(error) => {
                metrics.record_unresolved();
                return Err(RunFailure::CommitUnknown {
                    observation,
                    error: M::from_store(error),
                });
            }
        };
        // Release the receiver's strong alias before metrics, backoff and the
        // next reservation. Its source must already have published the SAME
        // slot's terminal NotCommitted proof; an outcome label alone is not it.
        mode.retire_not_committed(&observation)
            .map_err(RunFailure::Operation)?;
        let retry_error = M::from_store(retry_error);

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

fn deadline_exceeded<E>(metrics: &dyn StateStoreRunMetrics) -> RunFailure<E> {
    metrics.record_deadline();
    RunFailure::DeadlineExceeded
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use novarocks_state_store_testkit::testing::InMemoryStateStore;

    use super::*;

    #[derive(Default)]
    pub(super) struct TestMetrics {
        pub(super) retries: AtomicU64,
        pub(super) saturated: AtomicU64,
        pub(super) deadlines: AtomicU64,
        pub(super) unresolved: AtomicU64,
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

#[cfg(test)]
mod original_tests {
    use super::*;
    use novarocks_state_store_api::{
        StateStoreBlockingOriginal, StateStoreBlockingSlot, StateStoreExecutionPort,
        StateStoreOriginalUnsupported,
    };
    use novarocks_state_store_testkit::testing::InMemoryStateStore;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct NoPhysicalPort;
    impl StateStoreExecutionPort for NoPhysicalPort {
        fn bind_original_in(
            &self,
            _: &Arc<dyn StateStoreBlockingOriginal>,
            _: std::time::Instant,
            _: &mut Option<Box<dyn StateStoreBlockingSlot>>,
        ) -> Result<(), StateStoreExecutionFailure> {
            panic!("unsupported provider cannot bind physical work");
        }
        fn request_progress(&self) {
            panic!("unsupported provider has no original progress");
        }
    }
    #[tokio::test]
    async fn original_runner_refuses_unsupported_without_entering_legacy_operation() {
        let store = InMemoryStateStore::new("original-runner-no-fallback");
        let metrics = super::tests::TestMetrics::default();
        let port: Arc<dyn StateStoreExecutionPort> = Arc::new(NoPhysicalPort);
        let execution = StateStoreExecutionContext::new(
            &port,
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        );
        let result = run_side_effect_free_in(
            &store,
            &metrics,
            StateStoreRunPolicy::default(),
            "original unsupported fixture",
            &execution,
            |_| {
                Box::pin(async {
                    panic!("legacy operation must never run");
                    #[allow(unreachable_code)]
                    Ok::<(), StateStoreExecutionFailure>(())
                })
            },
        )
        .await;
        let Err(RunFailure::Begin(error)) = result else {
            panic!("selected provider must explicitly refuse original API");
        };
        assert!(
            error
                .downcast_ref::<StateStoreOriginalUnsupported>()
                .is_some()
        );
        assert_eq!(metrics.retries.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.unresolved.load(Ordering::Relaxed), 0);
    }
    #[derive(Default)]
    struct TestReceiving(Mutex<Option<CommitObservation>>);
    impl StateStoreRunReceiving for TestReceiving {
        fn receive_attempt(
            &self,
            observation: &CommitObservation,
        ) -> Result<(), StateStoreExecutionFailure> {
            *self.0.lock().unwrap() = Some(observation.clone());
            Ok(())
        }
        fn retire_not_committed(
            &self,
            _: &CommitObservation,
        ) -> Result<(), StateStoreExecutionFailure> {
            panic!("unsupported provider never produces a commit outcome");
        }
    }
    #[tokio::test]
    async fn original_runner_receives_issued_observation_before_provider_capability_refusal() {
        let store = InMemoryStateStore::new("original-receiving-before-begin");
        let metrics = super::tests::TestMetrics::default();
        let port: Arc<dyn StateStoreExecutionPort> = Arc::new(NoPhysicalPort);
        let execution = StateStoreExecutionContext::new(
            &port,
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        );
        let receiving = TestReceiving::default();
        let result = run_side_effect_free_in_receiving(
            &store,
            &metrics,
            StateStoreRunPolicy::default(),
            "receiving before unsupported begin",
            &execution,
            &receiving,
            |_| {
                Box::pin(async {
                    panic!("unsupported begin must not enter body");
                    #[allow(unreachable_code)]
                    Ok::<(), StateStoreExecutionFailure>(())
                })
            },
        )
        .await;
        assert!(matches!(result, Err(RunFailure::Begin(_))));
        let actual = receiving.0.lock().unwrap().take().unwrap();
        assert_eq!(actual.id().scope(), store.attempts().scope());
        assert_eq!(
            actual.publication_state().unwrap(),
            novarocks_state_store_api::CommitObservationState::NotDispatched
        );
        assert_eq!(store.attempts().outstanding(), 1);
        drop(actual);
        assert_eq!(store.attempts().outstanding(), 0);
    }

    #[derive(Debug)]
    struct RawPhysicalFailure;
    impl std::fmt::Display for RawPhysicalFailure {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Conflict Saturated: physical failure is not an outcome proof")
        }
    }
    impl std::error::Error for RawPhysicalFailure {}

    impl novarocks_failure_contract::FailureProjection for RawPhysicalFailure {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_failure_contract::FailureProjection::failure_fact(&StateStoreError::new(
                StateStoreErrorKind::Transient,
                "original test source failed",
            ))
        }
    }

    // Pure runner policy seam only: this does not implement a provider or
    // fabricate a dispatched/actual-exit receipt. Keep the real reservation.
    struct BareCommitFailure {
        raw: StateStoreExecutionFailure,
        attempt: Mutex<Option<WriteAttempt>>,
        begins: AtomicUsize,
        commits: AtomicUsize,
    }
    impl RunMode for BareCommitFailure {
        type Transaction = ();
        type Error = StateStoreExecutionFailure;
        fn from_store(error: StateStoreError) -> Self::Error {
            OriginalMode::from_store(error)
        }
        fn kind(error: &Self::Error) -> Option<StateStoreErrorKind> {
            OriginalMode::kind(error)
        }
        fn begin<'a>(
            &'a self,
            _: &'a dyn StateStore,
            attempt: WriteAttempt,
            _: &'a str,
        ) -> BoxFuture<'a, Result<Box<()>, Self::Error>> {
            self.begins.fetch_add(1, Ordering::Relaxed);
            *self.attempt.lock().unwrap() = Some(attempt);
            Box::pin(async { Ok(Box::new(())) })
        }
        fn commit<'a>(&'a self, _: Box<()>) -> BoxFuture<'a, Result<CommitOutcome, Self::Error>> {
            self.commits.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Err(self.raw.clone()) })
        }
    }
    #[tokio::test]
    async fn a_bare_commit_failure_keeps_the_issued_observation_and_raw_arc_without_replay() {
        let store = InMemoryStateStore::new("original-runner-raw-commit");
        let metrics = super::tests::TestMetrics::default();
        let raw = StateStoreExecutionFailure::from_static_detail(
            RawPhysicalFailure,
            "actual original commit failed",
        );
        let mode = BareCommitFailure {
            raw: raw.clone(),
            attempt: Mutex::new(None),
            begins: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
        };
        assert_eq!(
            OriginalMode::kind(&raw),
            None,
            "error text cannot manufacture an ErrorKind"
        );
        let failure = run(
            &mode,
            &store,
            &metrics,
            StateStoreRunPolicy::default(),
            "raw commit",
            Instant::now() + std::time::Duration::from_secs(1),
            |_| Box::pin(async { Ok(7_u8) }),
        )
        .await
        .unwrap_err();
        let RunFailure::CommitUnknown { observation, error } = failure else {
            panic!("bare commit failure must stay unknown");
        };
        assert_eq!(error, raw);
        assert_eq!(
            observation.id(),
            mode.attempt.lock().unwrap().as_ref().unwrap().id()
        );
        assert_eq!(mode.begins.load(Ordering::Relaxed), 1);
        assert_eq!(mode.commits.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.retries.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.unresolved.load(Ordering::Relaxed), 1);
    }
}
