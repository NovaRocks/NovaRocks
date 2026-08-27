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

use crate::mv::domain::repository::{MvRepositoryError, MvRepositoryErrorKind};
use crate::state_store::metrics::StateStoreMetrics;
use crate::state_store::{OperationId, RunFailure, derive_transaction_id, run_side_effect_free};
use novarocks_spi::state_store::{
    CommitOutcome, CommitResolution, StateStore, StateStoreError, StateStoreErrorKind,
    TransactionId,
};

pub(crate) fn state_store_error(error: StateStoreError) -> MvRepositoryError {
    let kind = match error.kind() {
        StateStoreErrorKind::InvalidRequest | StateStoreErrorKind::LimitExceeded => {
            MvRepositoryErrorKind::InvalidRequest
        }
        StateStoreErrorKind::PreconditionFailed | StateStoreErrorKind::Conflict => {
            MvRepositoryErrorKind::Conflict
        }
        StateStoreErrorKind::Corruption => MvRepositoryErrorKind::Corruption,
        StateStoreErrorKind::DeadlineExceeded => MvRepositoryErrorKind::CommitUnknown,
        StateStoreErrorKind::InvalidConfiguration
        | StateStoreErrorKind::UnsupportedFormat
        | StateStoreErrorKind::Transient
        | StateStoreErrorKind::ProviderUnavailable
        | StateStoreErrorKind::Cancelled
        | StateStoreErrorKind::Internal => MvRepositoryErrorKind::Unavailable,
    };
    MvRepositoryError::new(kind, format!("MV StateStore operation failed: {error}"))
}

pub(crate) fn run_failure(error: RunFailure) -> MvRepositoryError {
    match error {
        RunFailure::Operation(error) => state_store_error(error),
        RunFailure::RetryExhausted(error) => MvRepositoryError::new(
            MvRepositoryErrorKind::Conflict,
            format!("MV StateStore transaction conflict: {error}"),
        ),
        RunFailure::CommitUnknown {
            transaction_id,
            error,
        } => MvRepositoryError::new(
            MvRepositoryErrorKind::CommitUnknown,
            format!("MV StateStore commit outcome is unknown for {transaction_id:?}: {error}"),
        ),
        RunFailure::Begin(error) | RunFailure::DefiniteFailure(error) => state_store_error(error),
        RunFailure::DeadlineExceeded => MvRepositoryError::new(
            MvRepositoryErrorKind::CommitUnknown,
            "MV StateStore transaction deadline exceeded",
        ),
    }
}

pub(crate) async fn resolve_commit(
    store: &dyn StateStore,
    transaction_id: &TransactionId,
) -> Result<CommitResolution, MvRepositoryError> {
    store
        .resolve_commit(transaction_id)
        .await
        .map_err(state_store_error)
}

pub(crate) async fn run<T, F>(
    store: &dyn StateStore,
    metrics: &StateStoreMetrics,
    operation_id: uuid::Uuid,
    purpose: &str,
    operation: F,
) -> Result<T, MvRepositoryError>
where
    F: for<'a> FnMut(
        &'a mut dyn novarocks_spi::state_store::WriteTransaction,
    ) -> futures::future::BoxFuture<'a, Result<T, StateStoreError>>,
{
    run_raw(store, metrics, operation_id, purpose, operation)
        .await
        .map_err(run_failure)
}

pub(crate) async fn run_raw<T, F>(
    store: &dyn StateStore,
    metrics: &StateStoreMetrics,
    operation_id: uuid::Uuid,
    purpose: &str,
    mut operation: F,
) -> Result<T, RunFailure>
where
    F: for<'a> FnMut(
        &'a mut dyn novarocks_spi::state_store::WriteTransaction,
    ) -> futures::future::BoxFuture<'a, Result<T, StateStoreError>>,
{
    let result = run_side_effect_free(
        store,
        metrics,
        OperationId::from(operation_id),
        purpose,
        &mut operation,
    )
    .await
    .map(|success| success.value);
    match result {
        Err(RunFailure::Begin(error) | RunFailure::DefiniteFailure(error))
            if error.kind() == StateStoreErrorKind::InvalidRequest =>
        {
            // A resolved-aborted commit leaves its derived transaction ID terminal.
            // Continue the same stable operation on its next deterministic attempt.
            run_after_known_abort(store, operation_id, purpose, operation).await
        }
        other => other,
    }
}

async fn run_after_known_abort<T, F>(
    store: &dyn StateStore,
    operation_id: uuid::Uuid,
    purpose: &str,
    mut operation: F,
) -> Result<T, RunFailure>
where
    F: for<'a> FnMut(
        &'a mut dyn novarocks_spi::state_store::WriteTransaction,
    ) -> futures::future::BoxFuture<'a, Result<T, StateStoreError>>,
{
    for attempt in 2..=store.limits().runner_max_attempts {
        let transaction_id = derive_transaction_id(OperationId::from(operation_id), attempt);
        let mut transaction = store
            .begin_write(transaction_id, purpose)
            .await
            .map_err(RunFailure::Begin)?;
        let value = operation(transaction.as_mut())
            .await
            .map_err(RunFailure::Operation)?;
        match transaction.commit().await {
            CommitOutcome::Committed(_) => return Ok(value),
            CommitOutcome::Conflict(_) | CommitOutcome::TransientBeforeCommit(_) => continue,
            CommitOutcome::DefiniteFailure(error) => {
                return Err(RunFailure::DefiniteFailure(error));
            }
            CommitOutcome::CommitUnknown(error) => {
                return Err(RunFailure::CommitUnknown {
                    transaction_id,
                    error,
                });
            }
        }
    }
    Err(RunFailure::RetryExhausted(StateStoreError::new(
        StateStoreErrorKind::Conflict,
        "MV StateStore known-abort retry budget exhausted",
    )))
}
