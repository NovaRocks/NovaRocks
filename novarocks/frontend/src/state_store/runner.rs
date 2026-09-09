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
    StateStoreErrorKind, TransactionId, WriteTransaction,
};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep_until, timeout_at};
use uuid::Uuid;

use super::metrics::StateStoreMetrics;

const RETRY_BACKOFFS: [Duration; MAX_RUNNER_ATTEMPTS - 1] = [
    Duration::from_millis(10),
    Duration::from_millis(20),
    Duration::from_millis(40),
    Duration::from_millis(80),
];

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OperationId(uuid::Uuid);

impl OperationId {
    pub fn new_v7() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    pub const fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl From<uuid::Uuid> for OperationId {
    fn from(value: uuid::Uuid) -> Self {
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

/// Runs a transaction body that has no externally visible side effects.
///
/// The operation may be replayed from the beginning after a conflict or a
/// failure known to have happened before commit. Stable request identifiers
/// must therefore be allocated before calling this function.
///
/// # Cancellation safety
///
/// If the future returned by this function is cancelled or dropped, the caller
/// must treat the operation as possibly committed. The caller must not restart
/// it with a new [`OperationId`]. Recovery must instead derive or resolve the
/// known attempt [`TransactionId`] values from the same `OperationId`, for every
/// attempt up to the store's configured `runner_max_attempts` limit, or perform
/// an authoritative re-read that establishes the operation's effect.
///
/// While the future remains polled, an ordinary commit timeout returns
/// [`RunFailure::CommitUnknown`] with the active `TransactionId`; the same
/// recovery rule applies.
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
