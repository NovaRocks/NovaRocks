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

// Design: ADR-0119 (docs/adr/ADR-0119-sqlite-is-the-only-production-state-store.md)

use std::path::PathBuf;

use anyhow::{Result, bail};
use novarocks_spi::state_store::{MAX_KEY_BYTES, StateStoreProviderId};
use novarocks_state_store_sqlite::SqliteHistoryRetentionConfig;

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
    pub history_retention: SqliteHistoryRetentionConfig,
}

impl StateStoreConfig {
    pub fn validate(&self) -> Result<()> {
        if self.cluster_id.trim().is_empty() {
            bail!("InvalidStateStoreConfig: cluster_id must not be empty");
        }
        if self.path.as_os_str().is_empty() {
            bail!("InvalidStateStoreConfig: path must not be empty");
        }
        let limits = resolve_state_store_limits(&self.limits, MAX_KEY_BYTES)?;
        let retention = &self.history_retention;
        if retention.max_age_secs == 0
            || retention.max_change_rows == 0
            || retention.max_commit_receipts == 0
            || retention.maintenance_interval_commits == 0
            || retention.incremental_vacuum_pages == 0
        {
            bail!("InvalidStateStoreConfig: history_retention values must be non-zero");
        }
        if retention.max_change_rows < limits.max_transaction_operations {
            bail!(
                "InvalidStateStoreConfig: history_retention.max_change_rows must be at least max_transaction_operations"
            );
        }
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
// Design: ADR-0119 (docs/adr/ADR-0119-sqlite-is-the-only-production-state-store.md)
