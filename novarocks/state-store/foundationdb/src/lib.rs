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

//! FoundationDB StateStore provider implementation.

pub mod config;

pub use config::{FoundationDbClientConfig, FoundationDbProviderConfig};

use novarocks_state_store_api::StateStoreProviderId;

pub const FOUNDATIONDB_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("foundationdb");

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FoundationDbProviderBuildError {
    NotCompiled,
    InvalidConfiguration,
}

impl FoundationDbProviderBuildError {
    pub fn into_state_store_error(self) -> novarocks_state_store_api::StateStoreError {
        match self {
            Self::NotCompiled => novarocks_state_store_api::StateStoreError::new(
                novarocks_state_store_api::StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB state store provider is not compiled in",
            ),
            Self::InvalidConfiguration => novarocks_state_store_api::StateStoreError::new(
                novarocks_state_store_api::StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store provider configuration is invalid",
            ),
        }
    }
}

#[cfg(feature = "foundationdb-provider")]
pub fn foundationdb_provider_factory(
    config: FoundationDbProviderConfig,
    client: FoundationDbClientConfig,
) -> Result<
    Box<dyn novarocks_state_store_api::StateStoreProviderFactory>,
    FoundationDbProviderBuildError,
> {
    FoundationDbStateStoreProviderFactory::new(config, client)
        .map(|factory| {
            Box::new(factory) as Box<dyn novarocks_state_store_api::StateStoreProviderFactory>
        })
        .map_err(|_| FoundationDbProviderBuildError::InvalidConfiguration)
}

#[cfg(not(feature = "foundationdb-provider"))]
pub fn foundationdb_provider_factory(
    _config: FoundationDbProviderConfig,
    _client: FoundationDbClientConfig,
) -> Result<
    Box<dyn novarocks_state_store_api::StateStoreProviderFactory>,
    FoundationDbProviderBuildError,
> {
    Err(FoundationDbProviderBuildError::NotCompiled)
}

#[cfg(feature = "foundationdb-provider")]
#[path = "foundationdb/budget.rs"]
mod budget;
#[cfg(feature = "foundationdb-provider")]
#[path = "foundationdb/changes.rs"]
mod changes;
#[cfg(feature = "foundationdb-provider")]
#[path = "foundationdb/codec.rs"]
mod codec;
#[cfg(feature = "foundationdb-provider")]
#[path = "foundationdb/commit.rs"]
mod commit;
#[cfg(feature = "foundationdb-provider")]
#[path = "foundationdb/identity.rs"]
mod identity;
#[cfg(feature = "foundationdb-provider")]
#[path = "foundationdb/provider.rs"]
pub mod provider;
#[cfg(feature = "foundationdb-provider")]
#[path = "foundationdb/range.rs"]
mod range;
#[cfg(feature = "foundationdb-provider")]
#[path = "foundationdb/runtime.rs"]
mod runtime;
#[cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]
mod test_config;
#[cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]
#[path = "foundationdb/test_support.rs"]
#[doc(hidden)]
pub mod test_support;
#[cfg(feature = "foundationdb-provider")]
#[path = "foundationdb/txn.rs"]
mod txn;

#[cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]
#[doc(hidden)]
pub use provider::FoundationDbProviderTestHarness;
#[cfg(feature = "foundationdb-provider")]
pub use provider::FoundationDbStateStoreProviderFactory;
#[cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]
#[doc(hidden)]
pub use test_config::{
    FoundationDbTestLimitOverrides, FoundationDbTestProviderConfig, FoundationDbTestStoreConfig,
};
#[cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]
#[doc(hidden)]
pub use test_support::{FoundationDbCommitGateControl, arm_next_foundationdb_commit};

#[cfg(all(test, not(feature = "foundationdb-provider")))]
mod default_tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::{
        FoundationDbClientConfig, FoundationDbProviderBuildError, FoundationDbProviderConfig,
        foundationdb_provider_factory,
    };

    #[test]
    fn default_build_reports_a_typed_not_compiled_result() {
        let error = match foundationdb_provider_factory(
            FoundationDbProviderConfig {
                cluster_file: PathBuf::from("unused.cluster"),
                keyspace_id: Uuid::nil(),
            },
            FoundationDbClientConfig {
                disable_multi_version_client: true,
                tls_cert_path: None,
                tls_key_path: None,
                tls_ca_path: None,
                tls_verify_peers: None,
                tls_password: None,
            },
        ) {
            Ok(_) => panic!("default FoundationDB package must not create a native factory"),
            Err(error) => error,
        };
        assert_eq!(error, FoundationDbProviderBuildError::NotCompiled);
    }
}

#[cfg(feature = "foundationdb-provider")]
use async_trait::async_trait;
#[cfg(feature = "foundationdb-provider")]
use foundationdb::FdbError;
#[cfg(feature = "foundationdb-provider")]
use std::sync::Arc;
#[cfg(feature = "foundationdb-provider")]
use uuid::Uuid;

#[cfg(feature = "foundationdb-provider")]
use self::codec::KeyspaceCodec;
#[cfg(feature = "foundationdb-provider")]
use self::identity::open_identity;
#[cfg(feature = "foundationdb-provider")]
use novarocks_state_store_api::{
    ChangePage, ChangePollRequest, CommitResolution, ReadTransaction, StateStore, StateStoreError,
    StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot, StoreIdentity, TransactionId,
    WriteTransaction,
};

#[cfg(feature = "foundationdb-provider")]
use self::runtime::ProviderHandle;
#[cfg(feature = "foundationdb-provider")]
use novarocks_state_store_api::StateStoreMetrics;

#[cfg(feature = "foundationdb-provider")]
pub(crate) struct FoundationDbStateStore {
    lease: ProviderHandle,
    codec: KeyspaceCodec,
    identity: StoreIdentity,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
}

#[cfg(feature = "foundationdb-provider")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderErrorMetricEvent {
    Deadline,
    BlockingFailure,
}

#[cfg(feature = "foundationdb-provider")]
fn provider_error_metric_event(error: &StateStoreError) -> Option<ProviderErrorMetricEvent> {
    match error.kind() {
        StateStoreErrorKind::DeadlineExceeded => Some(ProviderErrorMetricEvent::Deadline),
        StateStoreErrorKind::Transient | StateStoreErrorKind::ProviderUnavailable => {
            Some(ProviderErrorMetricEvent::BlockingFailure)
        }
        _ => None,
    }
}

#[cfg(feature = "foundationdb-provider")]
fn record_provider_error_metric(metrics: &StateStoreMetrics, error: &StateStoreError) {
    match provider_error_metric_event(error) {
        Some(ProviderErrorMetricEvent::Deadline) => metrics.record_deadline(),
        Some(ProviderErrorMetricEvent::BlockingFailure) => metrics.record_blocking_failure(),
        None => {}
    }
}

#[cfg(feature = "foundationdb-provider")]
fn classify_native_read_error(error: FdbError) -> StateStoreError {
    let kind = match error.code() {
        1031 => StateStoreErrorKind::DeadlineExceeded,
        2006 | 2007 => StateStoreErrorKind::InvalidConfiguration,
        2101 | 2102 | 2103 | 2109 | 2110 => StateStoreErrorKind::LimitExceeded,
        2000 | 2002 | 2004 | 2018 | 2020 | 2023 | 2108 => StateStoreErrorKind::InvalidRequest,
        _ if error.is_retryable() || error.is_maybe_committed() => {
            StateStoreErrorKind::ProviderUnavailable
        }
        _ => StateStoreErrorKind::Internal,
    };
    StateStoreError::new(kind, "FoundationDB native read failed")
}

#[cfg(feature = "foundationdb-provider")]
impl FoundationDbStateStore {
    async fn open(
        lease: ProviderHandle,
        limits: StateStoreLimits,
        cluster_id: String,
        keyspace_id: Uuid,
    ) -> Result<Self, StateStoreError> {
        let database = lease.database()?;
        let codec = KeyspaceCodec::new(keyspace_id);
        let identity = open_identity(database.as_ref(), &codec, &cluster_id)
            .await?
            .identity;
        tracing::info!(
            provider = "foundationdb",
            client_status = "ready",
            keyspace_hash = %codec.keyspace_hash(),
            "FoundationDB state store client is ready"
        );
        Ok(Self {
            lease,
            codec,
            identity,
            limits,
            metrics: Arc::new(StateStoreMetrics::new(FOUNDATIONDB_STATE_STORE_PROVIDER_ID)),
        })
    }
}

#[cfg(feature = "foundationdb-provider")]
#[async_trait]
impl StateStore for FoundationDbStateStore {
    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.metrics.snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        Ok(Box::new(self.begin_read_transaction()?))
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        Ok(Box::new(self.begin_write_transaction(transaction_id)?))
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        let database = self.lease.database()?;
        let result = changes::poll_changes(
            database.as_ref(),
            &self.codec,
            &self.identity,
            &self.limits,
            self.metrics.as_ref(),
            request,
        )
        .await;
        if let Ok(page) = &result {
            self.metrics.record_page_records(page.hints.len() as u64);
            let bytes = page.hints.iter().fold(0_u64, |total, hint| {
                total.saturating_add(
                    u64::try_from(hint.key.as_bytes().len() + hint.revision.as_bytes().len())
                        .unwrap_or(u64::MAX),
                )
            });
            self.metrics.record_bytes_read(bytes);
        }
        result
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Ok(self.identity.clone())
    }

    async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        let database = self.lease.database()?;
        commit::resolve_commit(
            database.as_ref(),
            &self.codec,
            &self.limits,
            *transaction_id,
            self.metrics.as_ref(),
        )
        .await
    }
}

#[cfg(all(test, feature = "foundationdb-provider"))]
mod tests {
    use super::*;
    use novarocks_state_store_api::{StateStoreErrorKind, StateStoreOperation};

    #[test]
    fn provider_error_metrics_count_each_blocker_without_public_operation_duplication() {
        assert_eq!(
            provider_error_metric_event(&StateStoreError::new(
                StateStoreErrorKind::DeadlineExceeded,
                "deadline",
            )),
            Some(ProviderErrorMetricEvent::Deadline)
        );
        assert_eq!(
            provider_error_metric_event(&StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "provider",
            )),
            Some(ProviderErrorMetricEvent::BlockingFailure)
        );
        assert_eq!(
            provider_error_metric_event(&StateStoreError::new(
                StateStoreErrorKind::Corruption,
                "corruption",
            )),
            None
        );

        let metrics = StateStoreMetrics::new(FOUNDATIONDB_STATE_STORE_PROVIDER_ID);
        record_provider_error_metric(
            &metrics,
            &StateStoreError::new(StateStoreErrorKind::DeadlineExceeded, "deadline"),
        );
        record_provider_error_metric(
            &metrics,
            &StateStoreError::new(StateStoreErrorKind::ProviderUnavailable, "provider"),
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.deadline_count, 1);
        assert_eq!(snapshot.blocking_failure_count, 1);
        assert_eq!(snapshot.commit_count, 0);
        assert_eq!(
            snapshot.operation_duration_observations(StateStoreOperation::Commit),
            0
        );
    }

    #[test]
    fn native_read_errors_preserve_deadline_provider_and_fail_closed_boundaries() {
        assert_eq!(
            classify_native_read_error(foundationdb::FdbError::from_code(1031)).kind(),
            StateStoreErrorKind::DeadlineExceeded
        );
        assert_eq!(
            classify_native_read_error(foundationdb::FdbError::from_code(1007)).kind(),
            StateStoreErrorKind::ProviderUnavailable
        );
        assert_eq!(
            classify_native_read_error(foundationdb::FdbError::from_code(2006)).kind(),
            StateStoreErrorKind::InvalidConfiguration
        );
        assert_eq!(
            classify_native_read_error(foundationdb::FdbError::from_code(2102)).kind(),
            StateStoreErrorKind::LimitExceeded
        );
        assert_eq!(
            classify_native_read_error(foundationdb::FdbError::from_code(9999)).kind(),
            StateStoreErrorKind::Internal
        );
    }
}
