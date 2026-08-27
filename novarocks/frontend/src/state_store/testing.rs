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

use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::time::Instant;

use novarocks_spi::state_store::testing::InMemoryStateStoreProviderFactory;
use novarocks_spi::state_store::{
    StateStoreLimits, StateStoreProviderDescriptor, StateStoreProviderId,
};

use super::{StateStoreHost as FrontendStateStoreHost, StateStoreHostError};
use super::{StateStoreHostInput, StateStoreProviderRegistration, StateStoreProviderRegistry};

pub const TEST_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("frontend-unit-test");

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
    }
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
    if let Some(value) = overrides.runner_max_attempts {
        limits.runner_max_attempts = value;
    }
}
