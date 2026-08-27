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

#![cfg(feature = "state-store-conformance")]

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use novarocks_spi::state_store::{
    ChangePage, ChangePollRequest, CommitResolution, MAX_KEY_BYTES, StateStore, StateStoreError,
    StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot, StateStoreOpenRequest,
    StateStoreProviderDescriptor, StateStoreProviderFactory, StateStoreProviderId,
    StateStoreProviderInstance, StateStoreProviderLifecycle, StoreIdentity, TransactionId,
};

const TEST_PROVIDER_ID: StateStoreProviderId = StateStoreProviderId::new("test-provider");
const TEST_DESCRIPTOR: StateStoreProviderDescriptor =
    StateStoreProviderDescriptor::new(TEST_PROVIDER_ID, MAX_KEY_BYTES);

fn assert_factory_object_safe(_: Box<dyn StateStoreProviderFactory>) {}
fn assert_instance_object_safe(_: Box<dyn StateStoreProviderInstance>) {}

struct StubStateStore {
    limits: StateStoreLimits,
}

impl StubStateStore {
    fn new(limits: StateStoreLimits) -> Self {
        Self { limits }
    }
}

fn unused_transaction_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Internal,
        "stub state store transaction methods are unused",
    )
}

#[async_trait]
impl StateStore for StubStateStore {
    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        StateStoreMetricsSnapshot {
            provider: TEST_PROVIDER_ID,
            begin_count: 0,
            get_count: 0,
            range_count: 0,
            put_count: 0,
            delete_count: 0,
            commit_count: 0,
            operation_outcomes: [[0; 6]; 6],
            operation_duration_micros: [0; 6],
            operation_duration_observations: [0; 6],
            retry_count: 0,
            deadline_count: 0,
            blocking_failure_count: 0,
            bytes_read: 0,
            bytes_written: 0,
            page_records: 0,
            notification_lag_micros: 0,
            notification_lag_observations: 0,
        }
    }

    async fn begin_read(
        &self,
    ) -> Result<Box<dyn novarocks_spi::state_store::ReadTransaction>, StateStoreError> {
        Err(unused_transaction_error())
    }

    async fn begin_write(
        &self,
        _: TransactionId,
        _: &str,
    ) -> Result<Box<dyn novarocks_spi::state_store::WriteTransaction>, StateStoreError> {
        Err(unused_transaction_error())
    }

    async fn poll_changes(&self, _: &ChangePollRequest) -> Result<ChangePage, StateStoreError> {
        Err(unused_transaction_error())
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        Err(unused_transaction_error())
    }

    async fn resolve_commit(&self, _: &TransactionId) -> Result<CommitResolution, StateStoreError> {
        Err(unused_transaction_error())
    }
}

struct StubFactory;

#[async_trait]
impl StateStoreProviderFactory for StubFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &TEST_DESCRIPTOR
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        let _ = self;
        Ok(Box::new(StubInstance::ready_with_limits(request.limits)))
    }
}

struct StubInstance {
    lifecycle: StateStoreProviderLifecycle,
    store: Option<Arc<dyn StateStore>>,
}

impl StubInstance {
    fn ready() -> Self {
        Self::ready_with_limits(StateStoreLimits::default())
    }

    fn ready_with_limits(limits: StateStoreLimits) -> Self {
        Self {
            lifecycle: StateStoreProviderLifecycle::Ready,
            store: Some(Arc::new(StubStateStore::new(limits))),
        }
    }
}

#[async_trait]
impl StateStoreProviderInstance for StubInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &TEST_DESCRIPTOR
    }

    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        self.lifecycle
    }

    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        self.store.clone()
    }

    async fn shutdown(&mut self, _: Instant) -> Result<(), StateStoreError> {
        self.lifecycle = StateStoreProviderLifecycle::Stopped;
        self.store = None;
        Ok(())
    }
}

#[test]
fn provider_id_rejects_invalid_static_values() {
    assert_eq!(TEST_PROVIDER_ID.as_str(), "test-provider");
    assert_eq!(
        StateStoreProviderId::try_new("provider-2").expect("valid provider id"),
        StateStoreProviderId::new("provider-2")
    );
    assert!(StateStoreProviderId::try_new("").is_err());
    assert!(StateStoreProviderId::try_new("-provider").is_err());
    assert!(StateStoreProviderId::try_new("provider-").is_err());
    assert!(StateStoreProviderId::try_new("provider--two").is_err());
    assert!(StateStoreProviderId::try_new("Test Provider").is_err());
}

#[test]
fn provider_id_has_value_order_hash_and_descriptor_identity() {
    let sqlite = StateStoreProviderId::new("sqlite");
    let mysql = StateStoreProviderId::new("mysql");
    assert_eq!(sqlite, StateStoreProviderId::new("sqlite"));
    assert_ne!(sqlite, mysql);
    assert!(mysql < sqlite);

    let mut ordered = BTreeSet::new();
    ordered.insert(sqlite);
    ordered.insert(mysql);
    assert_eq!(
        ordered
            .into_iter()
            .map(StateStoreProviderId::as_str)
            .collect::<Vec<_>>(),
        vec!["mysql", "sqlite"]
    );

    let mut hashed = HashSet::new();
    hashed.insert(sqlite);
    hashed.insert(StateStoreProviderId::new("sqlite"));
    assert_eq!(hashed.len(), 1);
    assert_eq!(
        hash_of(sqlite),
        hash_of(StateStoreProviderId::new("sqlite"))
    );

    let descriptor = StateStoreProviderDescriptor::new(sqlite, MAX_KEY_BYTES);
    assert_eq!(descriptor.id, sqlite);
    assert_ne!(descriptor.id, mysql);
    assert_eq!(descriptor.max_key_bytes, MAX_KEY_BYTES);
}

fn hash_of(id: StateStoreProviderId) -> u64 {
    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn provider_traits_are_object_safe_and_factory_is_one_shot() {
    assert_factory_object_safe(Box::new(StubFactory));
    assert_instance_object_safe(Box::new(StubInstance::ready()));
}

#[tokio::test]
async fn instance_stops_exposure_before_shutdown_completes() {
    let mut instance = StubInstance::ready();
    assert!(instance.state_store().is_some());
    instance
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .expect("shutdown");
    assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Stopped);
    assert!(instance.state_store().is_none());
    instance
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .expect("idempotent shutdown");
}

#[test]
fn cleanup_context_keeps_primary_kind() {
    let error = StateStoreError::new(StateStoreErrorKind::ProviderUnavailable, "open failed")
        .with_cleanup_context(StateStoreError::new(
            StateStoreErrorKind::DeadlineExceeded,
            "cleanup timed out",
        ));
    assert_eq!(error.kind(), StateStoreErrorKind::ProviderUnavailable);
    assert_eq!(
        error.cleanup_context().expect("cleanup context").kind(),
        StateStoreErrorKind::DeadlineExceeded
    );
    assert_eq!(
        error.to_string(),
        "ProviderUnavailable: open failed; cleanup failed: DeadlineExceeded: cleanup timed out"
    );
}

#[tokio::test]
async fn factory_open_preserves_requested_limits_in_exposed_store() {
    let request = StateStoreOpenRequest {
        cluster_id: "test-cluster".to_owned(),
        limits: StateStoreLimits {
            max_page_size: 17,
            ..StateStoreLimits::default()
        },
        deadline: Instant::now() + Duration::from_secs(1),
    };
    let instance = Box::new(StubFactory)
        .open(request)
        .await
        .expect("open instance");
    assert_eq!(
        instance
            .state_store()
            .expect("ready store")
            .limits()
            .max_page_size,
        17
    );
}
