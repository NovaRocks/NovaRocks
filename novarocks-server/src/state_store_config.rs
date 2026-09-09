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

// Design: ADR-0122 (docs/adr/ADR-0122-sqlite-is-the-only-production-state-store.md)

use std::path::PathBuf;

use anyhow::{Result, bail};
use novarocks_state_store_api::{MAX_KEY_BYTES, StateStoreProviderId};

use crate::state_store_limits::{StateStoreLimitOverrides, resolve_state_store_limits};

pub const SQLITE_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("sqlite");

/// Server-owned configuration for the only production StateStore provider.
///
/// Remote provider syntax is intentionally not represented here. MySQL and
/// FoundationDB remain experimental leaf crates, not server configuration or
/// composition choices.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreConfig {
    pub cluster_id: String,
    pub path: PathBuf,
    pub limits: StateStoreLimitOverrides,
}

impl StateStoreConfig {
    pub fn validate(&self) -> Result<()> {
        if self.cluster_id.trim().is_empty() {
            bail!("InvalidStateStoreConfig: cluster_id must not be empty");
        }
        if self.path.as_os_str().is_empty() {
            bail!("InvalidStateStoreConfig: path must not be empty");
        }
        // Resolved for its validation only: an override that relaxes a hard
        // bound has to fail here, before anything opens a database. The
        // resolved value itself belongs to composition, which builds the store.
        resolve_state_store_limits(&self.limits, MAX_KEY_BYTES)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreAppConfig {
    pub store: StateStoreConfig,
}

impl StateStoreAppConfig {
    pub fn validate(&self) -> Result<()> {
        self.store.validate()
    }
}
