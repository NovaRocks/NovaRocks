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

//! SQLite StateStore provider implementation.

mod history;
mod provider;
mod range;
mod schema;
mod txn;

use std::env;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use fs2::FileExt;
use rusqlite::ffi::ErrorCode as SqliteErrorCode;
use rusqlite::{Connection, OpenFlags};

use novarocks_spi::state_store::{
    ChangePage, ChangePollRequest, CommitResolution, ReadTransaction, StateStore, StateStoreError,
    StateStoreErrorKind, StateStoreLimits, StateStoreMetrics, StateStoreMetricsSnapshot,
    StateStoreOpenRequest, StateStoreProviderId, StoreIdentity, TransactionId, WriteTransaction,
};

pub use provider::SqliteStateStoreProviderFactory;

pub const SQLITE_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("sqlite");

#[derive(Clone)]
pub struct SqliteStateStoreContribution {
    path: PathBuf,
    history_retention: SqliteHistoryRetentionConfig,
}

impl SqliteStateStoreContribution {
    pub fn new(path: PathBuf, history_retention: SqliteHistoryRetentionConfig) -> Self {
        Self {
            path,
            history_retention,
        }
    }

    pub fn into_factory(self) -> SqliteStateStoreProviderFactory {
        SqliteStateStoreProviderFactory::new(self.path, self.history_retention)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteHistoryRetentionConfig {
    pub max_age_secs: u64,
    pub max_change_rows: usize,
    pub max_commit_receipts: usize,
    pub maintenance_interval_commits: usize,
    pub incremental_vacuum_pages: usize,
}

impl Default for SqliteHistoryRetentionConfig {
    fn default() -> Self {
        Self {
            max_age_secs: 7 * 24 * 60 * 60,
            max_change_rows: 1_000_000,
            max_commit_receipts: 1_000_000,
            maintenance_interval_commits: 256,
            incremental_vacuum_pages: 1024,
        }
    }
}

impl SqliteHistoryRetentionConfig {
    fn validate(&self, limits: &StateStoreLimits) -> Result<(), StateStoreError> {
        if self.max_age_secs == 0
            || self.max_change_rows == 0
            || self.max_commit_receipts == 0
            || self.maintenance_interval_commits == 0
            || self.incremental_vacuum_pages == 0
        {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "SQLite history retention values must be non-zero",
            ));
        }
        if self.max_change_rows < limits.max_transaction_operations {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "SQLite max change rows must cover one maximum transaction",
            ));
        }
        if self.max_change_rows > i64::MAX as usize || self.max_commit_receipts > i64::MAX as usize
        {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "SQLite history row limits exceed the supported integer range",
            ));
        }
        Ok(())
    }
}

struct SqliteStateStore {
    path: PathBuf,
    limits: StateStoreLimits,
    history_retention: SqliteHistoryRetentionConfig,
    metrics: Arc<StateStoreMetrics>,
    commit_registry: txn::CommitRegistry,
    #[cfg(test)]
    test_hooks: txn::TestHooks,
    identity: StoreIdentity,
    _owner_lock: File,
}

#[async_trait]
impl StateStore for SqliteStateStore {
    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.metrics.snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        Ok(Box::new(SqliteStateStore::begin_read(self).await?))
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        let _ = purpose;
        Ok(Box::new(
            SqliteStateStore::begin_write(self, transaction_id).await?,
        ))
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        request.validate(&self.limits)?;
        let result = range::poll_changes(
            self.path.clone(),
            self.identity.clone(),
            request.clone(),
            Arc::clone(&self.metrics),
        )
        .await;
        if let Ok(page) = &result {
            self.metrics.record_page_records(page.hints.len() as u64);
            let bytes = page.hints.iter().fold(0_u64, |total, hint| {
                total.saturating_add(
                    (hint.key.as_bytes().len() + hint.revision.as_bytes().len()) as u64,
                )
            });
            self.metrics.record_bytes_read(bytes);
        }
        result
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        Ok(self.identity.clone())
    }

    async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        SqliteStateStore::resolve_commit(self, transaction_id).await
    }
}

impl SqliteStateStore {
    async fn open(
        path: PathBuf,
        history_retention: SqliteHistoryRetentionConfig,
        request: StateStoreOpenRequest,
    ) -> Result<Self, StateStoreError> {
        history_retention.validate(&request.limits)?;

        if is_memory_path(&path) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "SQLite state store requires a persistent file path",
            ));
        }

        tokio::task::spawn_blocking(move || {
            open_blocking(path, history_retention, request.cluster_id, request.limits)
        })
        .await
        .map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "SQLite state store open worker failed",
            )
        })?
    }
}

fn open_blocking(
    path: PathBuf,
    history_retention: SqliteHistoryRetentionConfig,
    cluster_id: String,
    limits: StateStoreLimits,
) -> Result<SqliteStateStore, StateStoreError> {
    let path = canonicalize_database_path(&path)?;
    let owner_lock = acquire_owner_lock(&path)?;
    let mut connection = open_connection(&path)?;
    let identity = schema::initialize(&mut connection, cluster_id.as_bytes())?;
    history::reclaim_pending_on_open(&connection, &history_retention)?;

    Ok(SqliteStateStore {
        path,
        limits,
        history_retention,
        metrics: Arc::new(StateStoreMetrics::new(SQLITE_STATE_STORE_PROVIDER_ID)),
        commit_registry: txn::new_commit_registry(),
        #[cfg(test)]
        test_hooks: txn::new_test_hooks(),
        identity,
        _owner_lock: owner_lock,
    })
}

fn open_connection(path: &Path) -> Result<Connection, StateStoreError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let connection = Connection::open_with_flags(path, flags).map_err(|error| {
        sqlite_error(
            &error,
            StateStoreErrorKind::ProviderUnavailable,
            "failed to open SQLite state store database",
        )
    })?;
    let journal_mode = connection
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to configure SQLite state store connection",
            )
        })?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(connection_configuration_error());
    }
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to configure SQLite state store connection",
            )
        })?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to configure SQLite state store connection",
            )
        })?;

    let synchronous = connection
        .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to inspect SQLite state store connection configuration",
            )
        })?;
    let foreign_keys = connection
        .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to inspect SQLite state store connection configuration",
            )
        })?;
    if synchronous != 2 || foreign_keys != 1 {
        return Err(connection_configuration_error());
    }
    Ok(connection)
}

fn canonicalize_database_path(path: &Path) -> Result<PathBuf, StateStoreError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .map_err(|_| path_error("failed to resolve SQLite state store working directory"))?
            .join(path)
    };
    let file_name = absolute.file_name().ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "SQLite state store path must name a database file",
        )
    })?;
    let parent = absolute.parent().ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "SQLite state store path must have a parent directory",
        )
    })?;
    fs::create_dir_all(parent)
        .map_err(|_| path_error("failed to create SQLite state store directory"))?;
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|_| path_error("failed to canonicalize SQLite state store directory"))?;
    Ok(canonical_parent.join(file_name))
}

fn acquire_owner_lock(path: &Path) -> Result<File, StateStoreError> {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".owner.lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(PathBuf::from(lock_path))
        .map_err(|_| path_error("failed to open SQLite state store owner lock"))?;
    lock.try_lock_exclusive().map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "SQLite state store path is already owned by another provider",
        )
    })?;
    Ok(lock)
}

fn is_memory_path(path: &Path) -> bool {
    let Some(path) = path.to_str() else {
        return false;
    };
    let path = path.to_ascii_lowercase();
    path == ":memory:"
        || (path.starts_with("file:")
            && (path.contains(":memory:")
                || path
                    .split_once('?')
                    .is_some_and(|(_, query)| query.split('&').any(|part| part == "mode=memory"))))
}

const fn connection_configuration_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "failed to configure SQLite state store connection",
    )
}

const fn path_error(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::ProviderUnavailable, message)
}

fn sqlite_error(
    error: &rusqlite::Error,
    fallback: StateStoreErrorKind,
    message: &'static str,
) -> StateStoreError {
    StateStoreError::new(sqlite_error_kind(error, fallback), message)
}

fn sqlite_error_kind(
    error: &rusqlite::Error,
    fallback: StateStoreErrorKind,
) -> StateStoreErrorKind {
    match error.sqlite_error_code() {
        Some(SqliteErrorCode::DatabaseBusy | SqliteErrorCode::DatabaseLocked) => {
            StateStoreErrorKind::Transient
        }
        Some(
            SqliteErrorCode::CannotOpen
            | SqliteErrorCode::SystemIoFailure
            | SqliteErrorCode::ReadOnly
            | SqliteErrorCode::DiskFull
            | SqliteErrorCode::PermissionDenied
            | SqliteErrorCode::AuthorizationForStatementDenied,
        ) => StateStoreErrorKind::ProviderUnavailable,
        Some(
            SqliteErrorCode::DatabaseCorrupt
            | SqliteErrorCode::NotADatabase
            | SqliteErrorCode::SchemaChanged,
        ) => StateStoreErrorKind::Corruption,
        _ => fallback,
    }
}
