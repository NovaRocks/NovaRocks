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

use crate::mv::domain::repository::{MvRepositoryError, MvRepositoryErrorKind};
use crate::state_store::metrics::StateStoreMetrics;
use crate::state_store::{RunFailure, StateStoreRunPolicy, run_side_effect_free};
use novarocks_state_store_api::{StateStore, StateStoreError, StateStoreErrorKind};

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
        | StateStoreErrorKind::Saturated
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
        ),
        RunFailure::Begin(error) | RunFailure::DefiniteFailure(error) => state_store_error(error),
        RunFailure::DeadlineExceeded => MvRepositoryError::new(
            MvRepositoryErrorKind::CommitUnknown,
            "MV StateStore transaction deadline exceeded",
        ),
    }
}

pub(crate) async fn run<T, F>(
    store: &dyn StateStore,
    metrics: &StateStoreMetrics,
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
