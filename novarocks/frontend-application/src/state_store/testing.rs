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
use novarocks_state_store_testkit::testing::InMemoryStateStore;
mod pure_original;
use pure_original::PureOriginalMemoryStore;

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
                Ok(Box::new(FrontendMemoryFactory {
                    descriptor: TEST_STATE_STORE_DESCRIPTOR,
                    persistent: false,
                }))
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

struct FrontendMemoryFactory {
    descriptor: StateStoreProviderDescriptor,
    persistent: bool,
}

#[async_trait::async_trait]
impl novarocks_state_store_api::StateStoreProviderFactory for FrontendMemoryFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    fn blocking_dependency_facts(
        &self,
    ) -> Option<Vec<novarocks_state_store_api::SharedDependency>> {
        // This fixture performs no external physical work.
        Some(Vec::new())
    }
    fn blocking_source_vocabulary(
        &self,
    ) -> Option<novarocks_state_store_api::StateStoreBlockingSourceVocabulary> {
        Some(
            novarocks_state_store_testkit::testing::pure_memory_original_vocabulary(
                self.descriptor.id,
            ),
        )
    }
    fn prepare_open_in(
        self: Box<Self>,
        request: StateStoreOpenRequest,
        _: &novarocks_state_store_api::StateStoreExecutionContext,
        target: &mut Option<Box<dyn novarocks_state_store_api::StateStoreOriginalOpen>>,
    ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
        if target.is_some() {
            return Err(StateStoreError::new(
                novarocks_state_store_api::StateStoreErrorKind::InvalidRequest,
                "frontend pure memory opening receiver is occupied",
            )
            .into_original_failure());
        }
        novarocks_state_store_testkit::testing::prepare_pure_memory_open_in(
            self.open_instance(request)
                .map_err(StateStoreError::into_original_failure)?,
            target,
        )
    }
    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        self.open_instance(request)
    }
}
impl FrontendMemoryFactory {
    fn open_instance(
        &self,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        if Instant::now() >= request.deadline {
            return Err(StateStoreError::new(
                novarocks_state_store_api::StateStoreErrorKind::DeadlineExceeded,
                "frontend pure memory opening deadline exceeded",
            ));
        }
        let state_store = if self.persistent {
            let stores = persistent_stores();
            let mut stores = stores
                .lock()
                .expect("persistent Frontend test StateStore map");
            stores
                .entry(request.cluster_id.clone())
                .or_insert_with(|| {
                    Arc::new(InMemoryStateStore::with_limits(
                        request.cluster_id,
                        request.limits,
                    ))
                })
                .clone()
        } else {
            Arc::new(InMemoryStateStore::with_limits(
                request.cluster_id,
                request.limits,
            ))
        };
        let state_store = Arc::new(PureOriginalMemoryStore::new(state_store));
        Ok(Box::new(FrontendMemoryInstance {
            descriptor: self.descriptor,
            scope: state_store.attempts().scope(),
            state_store,
            closed: false,
        }))
    }
}

struct FrontendMemoryInstance {
    descriptor: StateStoreProviderDescriptor,
    scope: novarocks_state_store_api::InstanceScope,
    closed: bool,
    state_store: Arc<PureOriginalMemoryStore>,
}

#[async_trait::async_trait]
impl StateStoreProviderInstance for FrontendMemoryInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    fn blocking_dependency_facts(
        &self,
    ) -> Option<Vec<novarocks_state_store_api::SharedDependency>> {
        // This fixture performs no external physical work.
        Some(Vec::new())
    }
    fn blocking_source_vocabulary(
        &self,
    ) -> Option<novarocks_state_store_api::StateStoreBlockingSourceVocabulary> {
        Some(
            novarocks_state_store_testkit::testing::pure_memory_original_vocabulary(
                self.descriptor.id,
            ),
        )
    }
    fn blocking_instance_scope(&self) -> Option<novarocks_state_store_api::InstanceScope> {
        Some(self.scope)
    }
    fn prepare_close_in(
        &mut self,
        _: &novarocks_state_store_api::StateStoreExecutionContext,
        target: &mut Option<Box<dyn novarocks_state_store_api::StateStoreOriginalClose>>,
    ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
        novarocks_state_store_testkit::testing::prepare_pure_memory_close_in(target)?;
        self.closed = true;
        Ok(())
    }
    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        if self.closed {
            StateStoreProviderLifecycle::Stopped
        } else {
            StateStoreProviderLifecycle::Ready
        }
    }

    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        (!self.closed).then(|| Arc::clone(&self.state_store) as Arc<dyn StateStore>)
    }

    async fn shutdown(&mut self, _deadline: Instant) -> Result<(), StateStoreError> {
        self.closed = true;
        Ok(())
    }
}

pub(crate) fn persistent_registry() -> StateStoreProviderRegistry {
    let mut registry = StateStoreProviderRegistry::new();
    registry
        .register(StateStoreProviderRegistration::new(
            PERSISTENT_TEST_STATE_STORE_DESCRIPTOR,
            |_| {
                Ok(Box::new(FrontendMemoryFactory {
                    descriptor: PERSISTENT_TEST_STATE_STORE_DESCRIPTOR,
                    persistent: true,
                }))
            },
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
