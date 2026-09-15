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

//! SPI-backed StateStore fixtures for Frontend unit tests.

#![allow(dead_code)]

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use novarocks_state_store_api::{
    StateStore, StateStoreError, StateStoreLimits, StateStoreOpenRequest,
    StateStoreProviderDescriptor, StateStoreProviderId, StateStoreProviderInstance,
    StateStoreProviderLifecycle,
};
use novarocks_state_store_testkit::testing::{
    InMemoryStateStore, InMemoryStateStoreProviderFactory,
};

use novarocks_state_store_runtime::{
    StateStoreHost as FrontendStateStoreHost, StateStoreHostError, StateStoreHostInput,
    StateStoreProviderRegistration, StateStoreProviderRegistry, StateStoreRunPolicy,
};

pub const TEST_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("frontend-unit-test");

pub const TEST_STATE_STORE_DESCRIPTOR: StateStoreProviderDescriptor =
    StateStoreProviderDescriptor::new(
        TEST_STATE_STORE_PROVIDER_ID,
        novarocks_state_store_api::MAX_KEY_BYTES,
    );

pub fn registry() -> StateStoreProviderRegistry {
    let mut registry = StateStoreProviderRegistry::new();
    registry
        .register(StateStoreProviderRegistration::new(
            TEST_STATE_STORE_DESCRIPTOR,
            |_| {
                Ok(Box::new(InMemoryStateStoreProviderFactory::new(
                    TEST_STATE_STORE_DESCRIPTOR,
                )))
            },
        ))
        .expect("register Frontend unit-test StateStore provider");
    registry
}

pub fn input(cluster_id: impl Into<String>) -> StateStoreHostInput {
    StateStoreHostInput {
        cluster_id: cluster_id.into(),
        provider_id: TEST_STATE_STORE_PROVIDER_ID,
        limits: StateStoreLimits::default(),
        run_policy: StateStoreRunPolicy::default(),
    }
}

/// Test-only provider that preserves a store by cluster ID across host reopen.
/// It lets lifecycle tests prove restart behavior without a disk-backed
/// provider and without treating a newly allocated empty store as a reopen.
pub const PERSISTENT_TEST_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("frontend-persistent-unit-test");

const PERSISTENT_TEST_STATE_STORE_DESCRIPTOR: StateStoreProviderDescriptor =
    StateStoreProviderDescriptor::new(
        PERSISTENT_TEST_STATE_STORE_PROVIDER_ID,
        novarocks_state_store_api::MAX_KEY_BYTES,
    );

type PersistentStores = Arc<Mutex<HashMap<String, Arc<InMemoryStateStore>>>>;

fn persistent_stores() -> PersistentStores {
    static STORES: OnceLock<PersistentStores> = OnceLock::new();
    Arc::clone(STORES.get_or_init(|| Arc::new(Mutex::new(HashMap::new()))))
}

struct PersistentInMemoryFactory;

#[async_trait::async_trait]
impl novarocks_state_store_api::StateStoreProviderFactory for PersistentInMemoryFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &PERSISTENT_TEST_STATE_STORE_DESCRIPTOR
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        let stores = persistent_stores();
        let mut stores = stores
            .lock()
            .expect("persistent Frontend test StateStore map");
        let state_store = stores
            .entry(request.cluster_id.clone())
            .or_insert_with(|| {
                Arc::new(InMemoryStateStore::with_limits(
                    request.cluster_id,
                    request.limits,
                ))
            })
            .clone();
        Ok(Box::new(PersistentInMemoryInstance { state_store }))
    }
}

struct PersistentInMemoryInstance {
    state_store: Arc<InMemoryStateStore>,
}

#[async_trait::async_trait]
impl StateStoreProviderInstance for PersistentInMemoryInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &PERSISTENT_TEST_STATE_STORE_DESCRIPTOR
    }

    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        StateStoreProviderLifecycle::Ready
    }

    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        Some(Arc::clone(&self.state_store) as Arc<dyn StateStore>)
    }

    async fn shutdown(&mut self, _deadline: Instant) -> Result<(), StateStoreError> {
        Ok(())
    }
}

pub(crate) fn persistent_registry() -> StateStoreProviderRegistry {
    let mut registry = StateStoreProviderRegistry::new();
    registry
        .register(StateStoreProviderRegistration::new(
            PERSISTENT_TEST_STATE_STORE_DESCRIPTOR,
            |_| Ok(Box::new(PersistentInMemoryFactory)),
        ))
        .expect("register persistent Frontend unit-test StateStore provider");
    registry
}

pub(crate) fn persistent_input(cluster_id: impl Into<String>) -> StateStoreHostInput {
    StateStoreHostInput {
        cluster_id: cluster_id.into(),
        provider_id: PERSISTENT_TEST_STATE_STORE_PROVIDER_ID,
        limits: StateStoreLimits::default(),
        run_policy: StateStoreRunPolicy::default(),
    }
}

pub(crate) async fn open_persistent(cluster_id: impl Into<String>) -> FrontendStateStoreHost {
    let registry = persistent_registry();
    FrontendStateStoreHost::open(
        &registry,
        persistent_input(cluster_id),
        Instant::now() + std::time::Duration::from_secs(5),
    )
    .await
    .expect("open persistent Frontend test StateStore")
}

/// Compatibility-free test fixture input. It represents only test data; the
/// actual host opening facts remain `StateStoreHostInput` above.
#[derive(Clone, Debug, Default)]
pub struct StateStoreLimitOverrides {
    pub max_key_bytes: Option<usize>,
    pub max_value_bytes: Option<usize>,
    pub max_page_size: Option<usize>,
    pub max_transaction_operations: Option<usize>,
    pub max_transaction_bytes: Option<usize>,
    pub transaction_deadline_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub enum StateStoreProviderConfig {
    Sqlite {
        path: PathBuf,
    },
    Mysql {
        database: String,
    },
    Foundationdb {
        cluster_file: PathBuf,
        keyspace_id: uuid::Uuid,
    },
}

#[derive(Clone, Debug)]
pub struct StateStoreConfig {
    pub cluster_id: String,
    pub limits: StateStoreLimitOverrides,
    pub provider: StateStoreProviderConfig,
}

#[derive(Clone, Debug)]
pub struct StateStoreAppConfig {
    pub store: StateStoreConfig,
    pub mysql_client: Option<()>,
}

#[derive(Clone, Debug)]
pub struct StateStoreHostConfig {
    pub state_store: StateStoreAppConfig,
    pub foundationdb_client: Option<()>,
}

pub fn builtin_state_store_provider_registry()
-> Result<StateStoreProviderRegistry, StateStoreHostError> {
    Ok(registry())
}

pub struct StateStoreHost(FrontendStateStoreHost);

impl StateStoreHost {
    pub async fn open(
        _registry: &StateStoreProviderRegistry,
        config: StateStoreHostConfig,
        deadline: Instant,
    ) -> Result<Self, StateStoreHostError> {
        let mut opening = input(config.state_store.store.cluster_id);
        apply_limits(&mut opening.limits, config.state_store.store.limits);
        let registry = registry();
        FrontendStateStoreHost::open(&registry, opening, deadline)
            .await
            .map(Self)
    }

    pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreHostError> {
        self.0.shutdown(deadline).await
    }
}

impl Deref for StateStoreHost {
    type Target = FrontendStateStoreHost;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StateStoreHost {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

fn apply_limits(limits: &mut StateStoreLimits, overrides: StateStoreLimitOverrides) {
    if let Some(value) = overrides.max_key_bytes {
        limits.max_key_bytes = value;
    }
    if let Some(value) = overrides.max_value_bytes {
        limits.max_value_bytes = value;
    }
    if let Some(value) = overrides.max_page_size {
        limits.max_page_size = value;
    }
    if let Some(value) = overrides.max_transaction_operations {
        limits.max_transaction_operations = value;
    }
    if let Some(value) = overrides.max_transaction_bytes {
        limits.max_transaction_bytes = value;
    }
    if let Some(value) = overrides.transaction_deadline_ms {
        limits.transaction_deadline = std::time::Duration::from_millis(value);
    }
}
