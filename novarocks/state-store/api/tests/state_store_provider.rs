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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use novarocks_state_store_api::{
    AttemptId, AttemptOutcome, AttemptSupervisor, InDoubtAdjudicator, MAX_KEY_BYTES,
    ReadTransaction, StateStore, StateStoreError, StateStoreErrorKind, StateStoreLimits,
    StateStoreOpenRequest, StateStoreProviderDescriptor, StateStoreProviderFactory,
    StateStoreProviderId, StateStoreProviderInstance, StateStoreProviderLifecycle, StoreIdentity,
    WriteAttempt, WriteTransaction,
};

const TEST_PROVIDER_ID: StateStoreProviderId = StateStoreProviderId::new("test-provider");
const TEST_DESCRIPTOR: StateStoreProviderDescriptor =
    StateStoreProviderDescriptor::new(TEST_PROVIDER_ID, MAX_KEY_BYTES);

fn assert_factory_object_safe(_: Box<dyn StateStoreProviderFactory>) {}
fn assert_instance_object_safe(_: Box<dyn StateStoreProviderInstance>) {}

fn unused_transaction_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Internal,
        "stub store performs no transactions",
    )
}

/// Never asked anything by these tests: they exercise the provider surface,
/// not adjudication, which `attempt.rs` covers directly.
struct UnusedAdjudicator;

#[async_trait]
impl InDoubtAdjudicator for UnusedAdjudicator {
    async fn adjudicate(&self, _: AttemptId) -> Result<AttemptOutcome, StateStoreError> {
        Err(unused_transaction_error())
    }

    async fn release_evidence(&self, _: AttemptId) -> Result<(), StateStoreError> {
        Err(unused_transaction_error())
    }
}

struct StubStateStore {
    limits: StateStoreLimits,
    attempts: AttemptSupervisor,
}

impl StubStateStore {
    fn new(limits: StateStoreLimits) -> Self {
        Self {
            limits,
            attempts: AttemptSupervisor::new(
                NonZeroUsize::new(4).expect("capacity"),
                Arc::new(UnusedAdjudicator),
            ),
        }
    }
}

#[async_trait]
impl StateStore for StubStateStore {
    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }

    fn attempts(&self) -> &AttemptSupervisor {
        &self.attempts
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        Err(unused_transaction_error())
    }

    async fn begin_write(
        &self,
        _: WriteAttempt,
        _: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        Err(unused_transaction_error())
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
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
        Ok(Box::new(StubInstance {
            lifecycle: StateStoreProviderLifecycle::Ready,
            store: Some(Arc::new(StubStateStore::new(request.limits))),
        }))
    }
}

struct StubInstance {
    lifecycle: StateStoreProviderLifecycle,
    store: Option<Arc<dyn StateStore>>,
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
        // Exposure stops first: a caller must not be able to take a fresh
        // handle out of an instance that is already draining.
        self.lifecycle = StateStoreProviderLifecycle::Draining;
        self.store = None;
        self.lifecycle = StateStoreProviderLifecycle::Stopped;
        Ok(())
    }
}

#[test]
fn provider_id_rejects_invalid_static_values() {
    for invalid in [
        "",
        "-leading",
        "trailing-",
        "double--dash",
        "Upper",
        "under_score",
    ] {
        assert!(
            StateStoreProviderId::try_new(invalid).is_err(),
            "{invalid} must be rejected"
        );
    }
    for valid in ["sqlite", "state-store-1", "a"] {
        assert!(
            StateStoreProviderId::try_new(valid).is_ok(),
            "{valid} must be accepted"
        );
    }
}

#[test]
fn provider_id_has_value_order_hash_and_descriptor_identity() {
    let a = StateStoreProviderId::new("alpha");
    let b = StateStoreProviderId::new("beta");
    assert!(a < b);
    assert_eq!(a, StateStoreProviderId::new("alpha"));
    assert_eq!(hash_of(a), hash_of(StateStoreProviderId::new("alpha")));
    assert_ne!(hash_of(a), hash_of(b));
    assert_eq!(TEST_DESCRIPTOR.id, TEST_PROVIDER_ID);
    assert_eq!(TEST_DESCRIPTOR.max_key_bytes, MAX_KEY_BYTES);
}

fn hash_of(id: StateStoreProviderId) -> u64 {
    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn provider_traits_are_object_safe_and_factory_is_one_shot() {
    assert_factory_object_safe(Box::new(StubFactory));
    assert_instance_object_safe(Box::new(StubInstance {
        lifecycle: StateStoreProviderLifecycle::Ready,
        store: None,
    }));
}

#[tokio::test]
async fn instance_stops_exposure_before_shutdown_completes() {
    let mut instance = StubInstance {
        lifecycle: StateStoreProviderLifecycle::Ready,
        store: Some(Arc::new(StubStateStore::new(StateStoreLimits::default()))),
    };
    assert!(instance.state_store().is_some());
    instance
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .expect("shutdown");
    assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Stopped);
    assert!(instance.state_store().is_none());
}

#[test]
fn cleanup_context_keeps_primary_kind() {
    let primary =
        StateStoreError::new(StateStoreErrorKind::Conflict, "primary").with_cleanup_context(
            StateStoreError::new(StateStoreErrorKind::Transient, "cleanup"),
        );
    assert_eq!(primary.kind(), StateStoreErrorKind::Conflict);
    assert_eq!(
        primary.cleanup_context().expect("cleanup").kind(),
        StateStoreErrorKind::Transient
    );
}

#[tokio::test]
async fn factory_open_preserves_requested_limits_and_issues_scoped_attempts() {
    let tightened = StateStoreLimits {
        max_page_size: 17,
        ..StateStoreLimits::default()
    };
    let instance = Box::new(StubFactory)
        .open(StateStoreOpenRequest {
            cluster_id: "cluster".to_string(),
            limits: tightened.clone(),
            deadline: Instant::now() + Duration::from_secs(1),
        })
        .await
        .expect("open");
    let store = instance.state_store().expect("exposed store");
    assert_eq!(store.limits(), &tightened);

    // Every opened instance issues into its own scope, so a capability cannot
    // address a store it did not come from.
    let (attempt, observation) = store.attempts().reserve().expect("reserve");
    assert_eq!(attempt.id().scope(), store.attempts().scope());
    assert_eq!(observation.id(), attempt.id());
    attempt
        .require_scope(store.attempts().scope())
        .expect("own scope");
}
