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

use std::time::Duration;

use futures::future::BoxFuture;
use novarocks_state_store_api::{
    CommitOutcome, CommitReceipt, MAX_RUNNER_ATTEMPTS, StateStore, StateStoreError,
    StateStoreErrorKind, StateStoreMetrics, TransactionId, WriteTransaction,
};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep_until, timeout_at};
use uuid::Uuid;

const RETRY_BACKOFFS: [Duration; MAX_RUNNER_ATTEMPTS - 1] = [
    Duration::from_millis(10),
    Duration::from_millis(20),
    Duration::from_millis(40),
    Duration::from_millis(80),
];

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OperationId(Uuid);

impl OperationId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl From<Uuid> for OperationId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunSuccess<T> {
    pub value: T,
    pub receipt: CommitReceipt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunFailure {
    Begin(StateStoreError),
    Operation(StateStoreError),
    RetryExhausted(StateStoreError),
    DefiniteFailure(StateStoreError),
    CommitUnknown {
        transaction_id: TransactionId,
        error: StateStoreError,
    },
    DeadlineExceeded,
}

pub fn derive_transaction_id(operation_id: OperationId, attempt: usize) -> TransactionId {
    assert!(
        (1..=MAX_RUNNER_ATTEMPTS).contains(&attempt),
        "state store runner attempt must be between 1 and 5"
    );

    let mut digest = Sha256::new();
    digest.update(operation_id.as_uuid().as_bytes());
    digest.update((attempt as u32).to_be_bytes());
    let digest = digest.finalize();

    let mut bytes = [0_u8; 16];
    bytes[..6].copy_from_slice(&operation_id.as_uuid().as_bytes()[..6]);
    bytes[6..].copy_from_slice(&digest[..10]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    TransactionId::from(Uuid::from_bytes(bytes))
}

/// Run a transaction body with no externally visible side effects.
///
/// The body may be replayed only after a conflict or a failure known to have
/// happened before commit. Stable request identifiers must be allocated before
/// this call. A cancelled runner or commit timeout has an unknown outcome; the
/// caller must resolve the attempt transaction identities derived from the
/// same [`OperationId`] or establish the effect with an authoritative read.
pub async fn run_side_effect_free<T, F>(
    store: &dyn StateStore,
    metrics: &StateStoreMetrics,
    operation_id: OperationId,
    purpose: &str,
    mut operation: F,
) -> Result<RunSuccess<T>, RunFailure>
where
    F: for<'a> FnMut(&'a mut dyn WriteTransaction) -> BoxFuture<'a, Result<T, StateStoreError>>,
{
    let deadline = Instant::now() + store.limits().transaction_deadline;
    let max_attempts = store.limits().runner_max_attempts.min(MAX_RUNNER_ATTEMPTS);

    for attempt in 1..=max_attempts {
        let transaction_id = derive_transaction_id(operation_id, attempt);
        let mut transaction =
            match timeout_at(deadline, store.begin_write(transaction_id, purpose)).await {
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
                return Err(RunFailure::CommitUnknown {
                    transaction_id,
                    error: StateStoreError::new(
                        StateStoreErrorKind::DeadlineExceeded,
                        "state store commit exceeded the runner deadline",
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
                return Err(RunFailure::CommitUnknown {
                    transaction_id,
                    error,
                });
            }
        };

        if attempt == max_attempts {
            return Err(RunFailure::RetryExhausted(retry_error));
        }

        metrics.record_retry();
        let wake_at = (Instant::now() + RETRY_BACKOFFS[attempt - 1]).min(deadline);
        sleep_until(wake_at).await;
        if Instant::now() >= deadline {
            return Err(deadline_exceeded(metrics));
        }
    }

    unreachable!("state store limits require at least one runner attempt")
}

fn deadline_exceeded(metrics: &StateStoreMetrics) -> RunFailure {
    metrics.record_deadline();
    RunFailure::DeadlineExceeded
}

#[cfg(test)]
mod tests {
    use novarocks_state_store_api::{StateStoreMetrics, StateStoreProviderId};
    use novarocks_state_store_testkit::testing::InMemoryStateStore;
    use uuid::Uuid;

    use super::*;

    const RUNTIME_PROVIDER: StateStoreProviderId = StateStoreProviderId::new("runtime-test");

    #[test]
    fn transaction_identity_is_stable_per_operation_attempt() {
        let operation = OperationId::from(
            Uuid::parse_str("018f1d6f-1234-7890-8123-456789abcdef").expect("operation UUID"),
        );

        assert_eq!(
            derive_transaction_id(operation, 1),
            derive_transaction_id(operation, 1)
        );
        assert_ne!(
            derive_transaction_id(operation, 1),
            derive_transaction_id(operation, 2)
        );
    }

    #[tokio::test]
    async fn runner_commits_through_the_provider_neutral_spi() {
        let store = InMemoryStateStore::new("runtime-test-cluster");
        let metrics = StateStoreMetrics::new(RUNTIME_PROVIDER);

        let success = run_side_effect_free(
            &store,
            &metrics,
            OperationId::new_v7(),
            "runtime owner test",
            |_| Box::pin(async { Ok::<_, StateStoreError>(42_u8) }),
        )
        .await
        .expect("commit through StateStore SPI");

        assert_eq!(success.value, 42);
        assert_eq!(metrics.snapshot().retry_count, 0);
    }
}
