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

//! Provider-neutral StateStore fixture for Frontend integration tests.

#![allow(dead_code, unused_imports)]

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use novarocks_frontend::state_store::StateStoreProviderRegistration;
use novarocks_frontend::{
    StateStoreHost as FrontendStateStoreHost, StateStoreHostInput, StateStoreProviderRegistry,
};
use novarocks_spi::state_store::testing::InMemoryStateStore;
use novarocks_spi::state_store::{
    StateStore, StateStoreError, StateStoreLimits, StateStoreOpenRequest,
    StateStoreProviderDescriptor, StateStoreProviderFactory, StateStoreProviderId,
    StateStoreProviderInstance, StateStoreProviderLifecycle,
};

pub const TEST_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("frontend-test");

pub const TEST_STATE_STORE_DESCRIPTOR: StateStoreProviderDescriptor =
    StateStoreProviderDescriptor::new(
        TEST_STATE_STORE_PROVIDER_ID,
        novarocks_spi::state_store::MAX_KEY_BYTES,
    );

pub fn registry() -> StateStoreProviderRegistry {
    let mut registry = StateStoreProviderRegistry::new();
    registry
        .register(StateStoreProviderRegistration::new(
            TEST_STATE_STORE_DESCRIPTOR,
            |_| Ok(Box::new(PersistentInMemoryFactory)),
        ))
        .expect("register Frontend test StateStore provider");
    registry
}

type Stores = Arc<Mutex<HashMap<String, Arc<InMemoryStateStore>>>>;

fn stores() -> Stores {
    static STORES: OnceLock<Stores> = OnceLock::new();
    Arc::clone(STORES.get_or_init(|| Arc::new(Mutex::new(HashMap::new()))))
}

struct PersistentInMemoryFactory;

#[async_trait]
impl StateStoreProviderFactory for PersistentInMemoryFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &TEST_STATE_STORE_DESCRIPTOR
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        let stores = stores();
        let mut stores = stores.lock().expect("Frontend test StateStore map");
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

#[async_trait]
impl StateStoreProviderInstance for PersistentInMemoryInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &TEST_STATE_STORE_DESCRIPTOR
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

pub fn input(cluster_id: impl Into<String>) -> StateStoreHostInput {
    StateStoreHostInput {
        cluster_id: cluster_id.into(),
        provider_id: TEST_STATE_STORE_PROVIDER_ID,
        limits: StateStoreLimits::default(),
    }
}

pub fn input_with_limits(
    cluster_id: impl Into<String>,
    limits: StateStoreLimits,
) -> StateStoreHostInput {
    let mut input = input(cluster_id);
    input.limits = limits;
    input
}

pub async fn open(cluster_id: impl Into<String>) -> FrontendStateStoreHost {
    open_with_input(input(cluster_id)).await
}

pub async fn open_with_input(input: StateStoreHostInput) -> FrontendStateStoreHost {
    let registry = registry();
    FrontendStateStoreHost::open(&registry, input, Instant::now() + Duration::from_secs(5))
        .await
        .expect("open Frontend test StateStore")
}

// Transitional test adapter. It converts legacy fixture literals directly to
// provider-neutral `StateStoreHostInput`; it never opens a concrete provider.
pub use novarocks_frontend::state_store::{StateStoreHostErrorKind, StateStoreHostLifecycle};
pub use novarocks_frontend::{
    OperationId, RunFailure, RunSuccess, derive_transaction_id, run_side_effect_free,
};

#[derive(Clone, Debug, Default)]
pub struct StateStoreLimitOverrides {
    pub max_key_bytes: Option<usize>,
    pub max_value_bytes: Option<usize>,
    pub max_page_size: Option<usize>,
    pub max_transaction_operations: Option<usize>,
    pub max_transaction_bytes: Option<usize>,
    pub transaction_deadline_ms: Option<u64>,
    pub runner_max_attempts: Option<usize>,
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
-> Result<StateStoreProviderRegistry, novarocks_frontend::state_store::StateStoreHostError> {
    Ok(registry())
}

pub struct TestStateStoreHost(FrontendStateStoreHost);
impl TestStateStoreHost {
    pub async fn open(
        _registry: &StateStoreProviderRegistry,
        config: StateStoreHostConfig,
        deadline: Instant,
    ) -> Result<Self, novarocks_frontend::state_store::StateStoreHostError> {
        let mut opening = input(config.state_store.store.cluster_id);
        let limits = config.state_store.store.limits;
        if let Some(value) = limits.max_key_bytes {
            opening.limits.max_key_bytes = value;
        }
        if let Some(value) = limits.max_value_bytes {
            opening.limits.max_value_bytes = value;
        }
        if let Some(value) = limits.max_page_size {
            opening.limits.max_page_size = value;
        }
        if let Some(value) = limits.max_transaction_operations {
            opening.limits.max_transaction_operations = value;
        }
        if let Some(value) = limits.max_transaction_bytes {
            opening.limits.max_transaction_bytes = value;
        }
        if let Some(value) = limits.transaction_deadline_ms {
            opening.limits.transaction_deadline = std::time::Duration::from_millis(value);
        }
        if let Some(value) = limits.runner_max_attempts {
            opening.limits.runner_max_attempts = value;
        }
        let registry = registry();
        FrontendStateStoreHost::open(&registry, opening, deadline)
            .await
            .map(Self)
    }
    pub async fn shutdown(
        &mut self,
        deadline: Instant,
    ) -> Result<(), novarocks_frontend::state_store::StateStoreHostError> {
        self.0.shutdown(deadline).await
    }
}
impl Deref for TestStateStoreHost {
    type Target = FrontendStateStoreHost;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for TestStateStoreHost {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
pub type StateStoreHost = TestStateStoreHost;
