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

//! Translating storage outcomes into the MV Accelerator's own vocabulary.
//!
//! This is deliberately the whole of what the MV repository adds on top of the
//! shared runner. Attempt identity, attempt counting, backoff and the overall
//! budget belong to the runner and its policy; this module only decides what
//! each storage outcome means to an MV caller.

use crate::repository::{MvRepositoryError, MvRepositoryErrorKind};
use crate::repository_metrics::MvRepositoryMetrics;
use novarocks_state_store_api::{StateStore, StateStoreError, StateStoreErrorKind};
use novarocks_state_store_runtime::{
    RunFailure, StateStoreRunPolicy, run_side_effect_free, run_side_effect_free_in,
};

fn state_store_kind(error: &StateStoreError) -> MvRepositoryErrorKind {
    match error.kind() {
        StateStoreErrorKind::InvalidRequest | StateStoreErrorKind::LimitExceeded => {
            MvRepositoryErrorKind::InvalidRequest
        }
        StateStoreErrorKind::PreconditionFailed | StateStoreErrorKind::Conflict => {
            MvRepositoryErrorKind::Conflict
        }
        StateStoreErrorKind::Corruption => MvRepositoryErrorKind::Corruption,
        // Reachable only from `Begin`, `Operation` and `DefiniteFailure`, none
        // of which has dispatched a commit that could still land. Calling that
        // unknown would be conservative in the wrong direction: it is provably
        // clean, and saying otherwise blocks the caller from simply retrying.
        StateStoreErrorKind::DeadlineExceeded => MvRepositoryErrorKind::Unavailable,
        StateStoreErrorKind::InvalidConfiguration
        | StateStoreErrorKind::UnsupportedFormat
        | StateStoreErrorKind::Saturated
        | StateStoreErrorKind::Transient
        | StateStoreErrorKind::ProviderUnavailable
        | StateStoreErrorKind::Cancelled
        | StateStoreErrorKind::Internal => MvRepositoryErrorKind::Unavailable,
    }
}
pub(crate) fn state_store_error(error: StateStoreError) -> MvRepositoryError {
    MvRepositoryError::new(
        state_store_kind(&error),
        format!("MV StateStore operation failed: {error}"),
    )
    .with_original_failure(error.into_original_failure())
}

pub(crate) fn original_failure(
    error: novarocks_state_store_api::StateStoreExecutionFailure,
) -> MvRepositoryError {
    let kind = error
        .downcast_ref::<StateStoreError>()
        .map(state_store_kind)
        .unwrap_or(MvRepositoryErrorKind::Unavailable);
    MvRepositoryError::new(kind, "MV original StateStore operation failed")
        .with_original_failure(error)
}

pub(crate) trait RunError: std::fmt::Display {
    fn project(self) -> MvRepositoryError;
    fn into_cause(self) -> novarocks_state_store_api::StateStoreExecutionFailure;
}
impl RunError for StateStoreError {
    fn project(self) -> MvRepositoryError {
        state_store_error(self)
    }
    fn into_cause(self) -> novarocks_state_store_api::StateStoreExecutionFailure {
        self.into_original_failure()
    }
}
impl RunError for novarocks_state_store_api::StateStoreExecutionFailure {
    fn project(self) -> MvRepositoryError {
        original_failure(self)
    }
    fn into_cause(self) -> novarocks_state_store_api::StateStoreExecutionFailure {
        self
    }
}
pub(crate) fn run_failure<E: RunError>(error: RunFailure<E>) -> MvRepositoryError {
    match error {
        RunFailure::Operation(error) => error.project(),
        RunFailure::RetryExhausted(error) => MvRepositoryError::new(
            MvRepositoryErrorKind::Conflict,
            format!("MV StateStore transaction conflict: {error}"),
        )
        .with_original_failure(error.into_cause()),
        // The observation, not a reconstructed id, is what addresses this
        // attempt. It is reported so an operator can tell which attempt is
        // still in doubt; resolving it is the caller's decision, never a
        // silent retry hidden in here.
        RunFailure::CommitUnknown { observation, error } => MvRepositoryError::new(
            MvRepositoryErrorKind::CommitUnknown,
            format!(
                "MV StateStore commit outcome is unknown for attempt {}: {error}",
                observation.id()
            ),
        )
        .with_original_failure(error.into_cause())
        .with_commit_observation(observation),
        RunFailure::Begin(error) | RunFailure::DefiniteFailure(error) => error.project(),
        // The budget ran out before any commit was dispatched, so nothing
        // landed and there is nothing in doubt. Catalog and GC already report
        // this as an ordinary unavailability; MV used to be alone in calling it
        // unknown, which is the one answer that forbids a caller from retrying.
        RunFailure::DeadlineExceeded => MvRepositoryError::new(
            MvRepositoryErrorKind::Unavailable,
            "MV StateStore operation budget expired before any commit was dispatched",
        ),
    }
}

pub(crate) async fn run<T, F>(
    store: &dyn StateStore,
    metrics: &MvRepositoryMetrics,
    policy: StateStoreRunPolicy,
    purpose: &str,
    mut operation: F,
) -> Result<T, MvRepositoryError>
where
    F: for<'a> FnMut(
        &'a mut dyn novarocks_state_store_api::WriteTransaction,
    ) -> futures::future::BoxFuture<'a, Result<T, StateStoreError>>,
{
    run_side_effect_free(store, metrics, policy, purpose, &mut operation)
        .await
        .map(|success| success.value)
        .map_err(run_failure)
}

pub(crate) async fn run_in<T, F>(
    store: &dyn StateStore,
    metrics: &MvRepositoryMetrics,
    policy: StateStoreRunPolicy,
    purpose: &str,
    execution: &novarocks_state_store_api::StateStoreExecutionContext,
    mut operation: F,
) -> Result<T, MvRepositoryError>
where
    F: for<'a> FnMut(
        &'a mut dyn novarocks_state_store_api::OriginalWriteTransaction,
    ) -> futures::future::BoxFuture<
        'a,
        Result<T, novarocks_state_store_api::StateStoreExecutionFailure>,
    >,
{
    run_side_effect_free_in(store, metrics, policy, purpose, execution, &mut operation)
        .await
        .map(|success| success.value)
        .map_err(run_failure)
}
