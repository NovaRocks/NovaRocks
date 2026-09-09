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

// Design: ADR-0122 (docs/adr/ADR-0122-sqlite-is-the-only-production-state-store.md)

mod evidence;
mod metrics;
mod provider;
mod range;
mod schema;
mod txn;

// This provider's run of the shared behaviour suite lives in `conformance.rs`,
// and is not compiled, because the suite is in `novarocks-state-store-testkit`
// and this crate does not depend on it. Enabling it is two lines: the
// dev-dependency the sibling MySQL and FoundationDB providers already carry,
//
//     [dev-dependencies]
//     novarocks-state-store-testkit = { path = "../testkit" }
//
// and the declaration below. All three groups -- basic, attempt, and fault --
// pass with those in place.
//
// #[cfg(test)]
#[cfg(test)]
mod conformance;

use std::env;
use std::fs::{self, File, OpenOptions};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use fs2::FileExt;
use rusqlite::ffi::ErrorCode as SqliteErrorCode;
use rusqlite::{Connection, OpenFlags};

use novarocks_state_store_api::{
    AttemptSupervisor, DEFAULT_MAX_OUTSTANDING_ATTEMPTS, InDoubtAdjudicator, ReadTransaction,
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreOpenRequest,
    StateStoreProviderId, StoreIdentity, WriteAttempt, WriteTransaction,
};

use evidence::SqliteCommitEvidence;
use metrics::StateStoreMetrics;

pub use provider::SqliteStateStoreProviderFactory;

pub const SQLITE_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("sqlite");

/// Everything Server has to decide to bring this provider up.
///
/// It is the database file and nothing else. History retention used to be
/// configured here; the change feed and the retired-attempt bounds it swept
/// are gone, and the only durable rows a commit still leaves are released by
/// the attempt that wrote them, so there is no retention policy left to state.
#[derive(Clone)]
pub struct SqliteStateStoreContribution {
    path: PathBuf,
}

impl SqliteStateStoreContribution {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn into_factory(self) -> SqliteStateStoreProviderFactory {
        SqliteStateStoreProviderFactory::new(self.path)
    }
}

struct SqliteStateStore {
    path: PathBuf,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
    /// Issues this instance's write attempts and accounts for their capacity.
    attempts: AttemptSupervisor,
    /// This instance's private commit evidence and its in-doubt callback.
    evidence: Arc<SqliteCommitEvidence>,
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

    fn attempts(&self) -> &AttemptSupervisor {
        &self.attempts
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        Ok(Box::new(SqliteStateStore::begin_read(self).await?))
    }

    async fn begin_write(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        let _ = purpose;
        // A capability another instance issued is refused before anything is
        // opened, so a handle retained across a reopen addresses nothing here.
        // The attempt is then simply dropped: it never reached this storage,
        // and this instance is in no position to make statements about it.
        attempt.require_scope(self.attempts.scope())?;
        Ok(Box::new(
            SqliteStateStore::begin_write(self, attempt).await?,
        ))
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        Ok(self.identity.clone())
    }
}

fn default_attempt_capacity() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_MAX_OUTSTANDING_ATTEMPTS).expect("default attempt capacity")
}

impl SqliteStateStore {
    async fn open(path: PathBuf, request: StateStoreOpenRequest) -> Result<Self, StateStoreError> {
        Self::open_with_capacity(path, request, default_attempt_capacity()).await
    }

    /// Opens an instance whose ceiling on charged attempts is
    /// `outstanding_attempts`.
    ///
    /// Only tests choose a value: saturation is unobservable at the contract
    /// default, and a server-side knob for it would be configuration nobody
    /// sets.
    async fn open_with_capacity(
        path: PathBuf,
        request: StateStoreOpenRequest,
        outstanding_attempts: NonZeroUsize,
    ) -> Result<Self, StateStoreError> {
        if is_memory_path(&path) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "SQLite state store requires a persistent file path",
            ));
        }

        tokio::task::spawn_blocking(move || {
            open_blocking(
                path,
                request.cluster_id,
                request.limits,
                outstanding_attempts,
            )
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
    cluster_id: String,
    limits: StateStoreLimits,
    outstanding_attempts: NonZeroUsize,
) -> Result<SqliteStateStore, StateStoreError> {
    let path = canonicalize_path(&path)?;
    let owner_lock = acquire_owner_lock(&path)?;
    let mut connection = open_connection_raw(&path)?;
    // Version acceptance comes first, and deliberately before the connection is
    // configured. A file this build does not understand is left exactly as it
    // was found: no WAL conversion, no vacuum, no schema mutation, no cleanup.
    let identity = schema::initialize(&mut connection, cluster_id.as_bytes())?;
    configure_connection(&connection)?;
    // Only now, on a file this build owns, is provider-private cleanup allowed.
    // Rows left by a previous instance are keyed by attempt identities nobody
    // can name any more, so nothing can ever ask about them again.
    evidence::purge_stale_evidence(&connection)?;
    connection
        .busy_timeout(limits.transaction_deadline)
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to configure SQLite commit evidence connection",
            )
        })?;

    let evidence = Arc::new(SqliteCommitEvidence::new(connection));
    let attempts = AttemptSupervisor::new(
        outstanding_attempts,
        Arc::clone(&evidence) as Arc<dyn InDoubtAdjudicator>,
    );

    Ok(SqliteStateStore {
        path,
        limits,
        metrics: Arc::new(StateStoreMetrics::new()),
        attempts,
        evidence,
        #[cfg(test)]
        test_hooks: txn::new_test_hooks(),
        identity,
        _owner_lock: owner_lock,
    })
}

fn open_connection(path: &Path) -> Result<Connection, StateStoreError> {
    let connection = open_connection_raw(path)?;
    configure_connection(&connection)?;
    Ok(connection)
}

fn open_connection_raw(path: &Path) -> Result<Connection, StateStoreError> {
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
    Ok(connection)
}

fn configure_connection(connection: &Connection) -> Result<(), StateStoreError> {
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
    Ok(())
}

fn canonicalize_path(path: &Path) -> Result<PathBuf, StateStoreError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_state_store_api::{Key, Precondition, Value};
    use tempfile::{TempDir, tempdir};

    /// A v2 database exactly as the previous build left one: the change feed,
    /// the transaction-id keyed commit ledger, and the eleven metadata keys
    /// that supported history retention.
    const LEGACY_V2_SCHEMA_SQL: &str = "\
        CREATE TABLE state_store_meta (key BLOB PRIMARY KEY, value BLOB NOT NULL);\
        CREATE TABLE state_store_kv (key BLOB PRIMARY KEY, value BLOB NOT NULL, version INTEGER NOT NULL);\
        CREATE TABLE state_store_changes (revision INTEGER NOT NULL, sequence INTEGER NOT NULL, key BLOB NOT NULL, committed_at_ms INTEGER NOT NULL, PRIMARY KEY(revision, sequence));\
        CREATE TABLE state_store_commits (transaction_id BLOB PRIMARY KEY, revision INTEGER NOT NULL, committed_at_ms INTEGER NOT NULL);\
        INSERT INTO state_store_meta(key, value) VALUES (x'736368656d615f76657273696f6e', x'00000002');\
        INSERT INTO state_store_kv(key, value, version) VALUES (x'6b31', x'7631', 1);";

    fn open_request(cluster_id: &str) -> StateStoreOpenRequest {
        StateStoreOpenRequest {
            cluster_id: cluster_id.to_owned(),
            limits: StateStoreLimits::default(),
            deadline: Instant::now() + Duration::from_secs(5),
        }
    }

    async fn open_store(path: &Path, cluster_id: &str) -> SqliteStateStore {
        SqliteStateStore::open(path.to_path_buf(), open_request(cluster_id))
            .await
            .expect("open SQLite state store")
    }

    fn key(bytes: &'static [u8]) -> Key {
        Key::try_from(Bytes::from_static(bytes)).expect("valid key")
    }

    fn value(bytes: &'static [u8]) -> Value {
        Value::try_from(Bytes::from_static(bytes)).expect("valid value")
    }

    fn sidecar(path: &Path, suffix: &str) -> PathBuf {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        PathBuf::from(sidecar)
    }

    /// The exact bytes of a file, or `None` when it does not exist.
    ///
    /// Comparing the contents outright is stronger than comparing a digest of
    /// them, and it distinguishes "unchanged" from "absent both times".
    fn file_bytes(path: &Path) -> Option<Vec<u8>> {
        fs::read(path).ok()
    }

    fn journal_mode(path: &Path) -> String {
        let connection = Connection::open(path).expect("inspect journal mode");
        connection
            .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
            .expect("read journal mode")
            .to_ascii_lowercase()
    }

    async fn commit_one(store: &SqliteStateStore, item: Key, payload: Value) {
        let (attempt, observation) = store.attempts().reserve().expect("reserve attempt");
        let mut transaction = StateStore::begin_write(store, attempt, "lib test write")
            .await
            .expect("begin write");
        transaction
            .put(item, payload, Precondition::Any)
            .await
            .expect("stage put");
        assert!(matches!(
            transaction.commit().await,
            novarocks_state_store_api::CommitOutcome::Committed(_)
        ));
        drop(observation);
    }

    async fn read_one(store: &SqliteStateStore, item: &Key) -> Option<Value> {
        let mut reader = StateStore::begin_read(store).await.expect("begin read");
        let record = reader.get(item).await.expect("read record");
        reader.abort().await.expect("abort read");
        record.map(|record| record.value)
    }

    #[tokio::test]
    async fn a_v3_file_reopens_with_its_business_data_and_identity_intact() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("state-store.sqlite");

        let store = open_store(&path, "cluster-a").await;
        let identity = StateStore::identity(&store).await.expect("identity");
        commit_one(&store, key(b"durable"), value(b"payload")).await;
        drop(store);

        let reopened = open_store(&path, "cluster-a").await;
        assert_eq!(
            read_one(&reopened, &key(b"durable")).await,
            Some(value(b"payload")),
            "a v3 file must come back with the rows it was left holding"
        );
        assert_eq!(
            StateStore::identity(&reopened).await.expect("identity"),
            identity,
            "reopening must not mint a new store identity"
        );
    }

    #[tokio::test]
    async fn a_reopened_instance_mints_a_new_attempt_scope() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("state-store.sqlite");

        let store = open_store(&path, "cluster-a").await;
        let first_scope = store.attempts().scope();
        // A capability minted before the close cannot address what comes after
        // it, so the reopened instance must not reuse the scope.
        drop(store);

        let reopened = open_store(&path, "cluster-a").await;
        assert_ne!(first_scope, reopened.attempts().scope());
    }

    #[test]
    fn legacy_v1_fails_before_connection_configuration() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("state-store.sqlite");
        let connection = Connection::open(&path).expect("open legacy database");
        connection
            .execute_batch(
                "CREATE TABLE state_store_meta (key BLOB PRIMARY KEY, value BLOB NOT NULL);\
                 INSERT INTO state_store_meta(key, value) VALUES (x'736368656d615f76657273696f6e', x'00000001');",
            )
            .expect("create legacy metadata");
        assert_eq!(journal_mode(&path), "delete");
        drop(connection);

        let error = match open_blocking(
            path.clone(),
            "test-cluster".to_owned(),
            StateStoreLimits::default(),
            default_attempt_capacity(),
        ) {
            Ok(_) => panic!("legacy schema unexpectedly opened"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StateStoreErrorKind::UnsupportedFormat);
        assert_eq!(journal_mode(&path), "delete");
    }

    #[test]
    fn a_v2_file_is_refused_without_a_single_byte_changing() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("state-store.sqlite");
        let connection = Connection::open(&path).expect("open v2 database");
        connection
            .execute_batch(LEGACY_V2_SCHEMA_SQL)
            .expect("create v2 schema");
        drop(connection);

        let before = file_bytes(&path).expect("v2 database exists");
        let wal_before = file_bytes(&sidecar(&path, "-wal"));

        let error = open_blocking(
            path.clone(),
            "test-cluster".to_owned(),
            StateStoreLimits::default(),
            default_attempt_capacity(),
        )
        .map(|_| ())
        .expect_err("a v2 file must be refused, never migrated");
        assert_eq!(error.kind(), StateStoreErrorKind::UnsupportedFormat);

        assert_eq!(
            file_bytes(&path).as_deref(),
            Some(before.as_slice()),
            "a refused v2 database must be left byte-for-byte as it was found"
        );
        assert_eq!(
            file_bytes(&sidecar(&path, "-wal")),
            wal_before,
            "a refused v2 database must not gain or lose a write-ahead log"
        );
        assert_eq!(
            journal_mode(&path),
            "delete",
            "refusing must happen before the journal mode is touched"
        );

        // The tables and rows are still the v2 ones: nothing was dropped,
        // rebuilt, or rewritten on the way out.
        let connection = Connection::open(&path).expect("reopen v2 database");
        let tables = connection
            .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .expect("inspect v2 tables");
        assert_eq!(
            tables,
            vec![
                "state_store_changes".to_owned(),
                "state_store_commits".to_owned(),
                "state_store_kv".to_owned(),
                "state_store_meta".to_owned(),
            ]
        );
    }

    #[test]
    fn a_v2_file_in_wal_mode_keeps_both_its_database_and_its_wal() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("state-store.sqlite");
        let holder = Connection::open(&path).expect("open v2 database");
        holder
            .pragma_update(None, "journal_mode", "WAL")
            .expect("enable WAL");
        holder
            .execute_batch(LEGACY_V2_SCHEMA_SQL)
            .expect("create v2 schema");
        // Held open on purpose: the write-ahead log only survives while a
        // connection exists, and the point of this test is that a refused open
        // leaves that log alone.
        let wal_path = sidecar(&path, "-wal");
        let before = file_bytes(&path).expect("v2 database exists");
        let wal_before = file_bytes(&wal_path).expect("v2 write-ahead log exists");
        assert!(!wal_before.is_empty(), "the WAL must hold the v2 schema");

        let error = open_blocking(
            path.clone(),
            "test-cluster".to_owned(),
            StateStoreLimits::default(),
            default_attempt_capacity(),
        )
        .map(|_| ())
        .expect_err("a v2 file must be refused, never migrated");
        assert_eq!(error.kind(), StateStoreErrorKind::UnsupportedFormat);

        assert_eq!(
            file_bytes(&path).as_deref(),
            Some(before.as_slice()),
            "a refused v2 database must not be checkpointed or rewritten"
        );
        assert_eq!(
            file_bytes(&wal_path).as_deref(),
            Some(wal_before.as_slice()),
            "a refused v2 write-ahead log must be left exactly as it was found"
        );
        drop(holder);
    }

    #[test]
    fn a_malformed_schema_version_is_corruption_not_an_unsupported_format() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("state-store.sqlite");
        let connection = Connection::open(&path).expect("open malformed database");
        connection
            .execute_batch(
                "CREATE TABLE state_store_meta (key BLOB PRIMARY KEY, value BLOB NOT NULL);\
                 INSERT INTO state_store_meta(key, value) VALUES (x'736368656d615f76657273696f6e', x'0003');",
            )
            .expect("create malformed metadata");
        drop(connection);
        let before = file_bytes(&path).expect("malformed database exists");

        let error = open_blocking(
            path.clone(),
            "test-cluster".to_owned(),
            StateStoreLimits::default(),
            default_attempt_capacity(),
        )
        .map(|_| ())
        .expect_err("a malformed version must not open");
        assert_eq!(error.kind(), StateStoreErrorKind::Corruption);
        assert_eq!(
            file_bytes(&path).as_deref(),
            Some(before.as_slice()),
            "a file this build cannot read must be left alone"
        );
    }

    #[test]
    fn a_v3_file_missing_its_version_row_is_corruption() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("state-store.sqlite");
        let connection = Connection::open(&path).expect("open partial database");
        connection
            .execute_batch("CREATE TABLE state_store_kv (key BLOB PRIMARY KEY, value BLOB NOT NULL, version INTEGER NOT NULL);")
            .expect("create partial schema");
        drop(connection);

        let error = open_blocking(
            path,
            "test-cluster".to_owned(),
            StateStoreLimits::default(),
            default_attempt_capacity(),
        )
        .map(|_| ())
        .expect_err("a store with no detectable version must not open");
        assert_eq!(error.kind(), StateStoreErrorKind::Corruption);
    }

    #[test]
    fn someone_elses_database_is_refused_rather_than_adopted() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("not-a-state-store.sqlite");
        let connection = Connection::open(&path).expect("open unrelated database");
        connection
            .execute_batch(
                "CREATE TABLE invoices (id INTEGER PRIMARY KEY, total INTEGER NOT NULL);\
                 INSERT INTO invoices(id, total) VALUES (1, 42);",
            )
            .expect("create unrelated schema");
        drop(connection);
        let before = file_bytes(&path).expect("unrelated database exists");

        let error = open_blocking(
            path.clone(),
            "test-cluster".to_owned(),
            StateStoreLimits::default(),
            default_attempt_capacity(),
        )
        .map(|_| ())
        .expect_err("a mistyped path must not annex somebody else's database");
        assert_eq!(error.kind(), StateStoreErrorKind::UnsupportedFormat);
        assert_eq!(
            file_bytes(&path).as_deref(),
            Some(before.as_slice()),
            "the refused database must not be vacuumed or added to"
        );
    }

    #[tokio::test]
    async fn the_owner_lock_refuses_a_second_instance_on_one_path() {
        let directory = TempDir::new().expect("temporary directory");
        let path = directory.path().join("state-store.sqlite");
        let owner = open_store(&path, "cluster-a").await;

        let error = SqliteStateStore::open(path.clone(), open_request("cluster-a"))
            .await
            .map(|_| ())
            .expect_err("one file has one owner");
        assert_eq!(error.kind(), StateStoreErrorKind::ProviderUnavailable);

        // Releasing the owner hands the path back rather than poisoning it.
        drop(owner);
        let successor = open_store(&path, "cluster-a").await;
        assert_eq!(
            StateStore::identity(&successor)
                .await
                .expect("identity")
                .cluster_id,
            "cluster-a"
        );
    }

    #[tokio::test]
    async fn a_bound_cluster_id_is_not_reassigned_by_configuration() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("state-store.sqlite");
        drop(open_store(&path, "cluster-a").await);

        let error = SqliteStateStore::open(path.clone(), open_request("cluster-b"))
            .await
            .map(|_| ())
            .expect_err("a bound store must not answer for another cluster");
        assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);

        // And the original binding still opens.
        let reopened = open_store(&path, "cluster-a").await;
        assert_eq!(
            StateStore::identity(&reopened)
                .await
                .expect("identity")
                .cluster_id,
            "cluster-a"
        );
    }

    #[tokio::test]
    async fn an_in_memory_path_is_rejected_as_configuration() {
        let error = SqliteStateStore::open(PathBuf::from(":memory:"), open_request("cluster-a"))
            .await
            .map(|_| ())
            .expect_err("an in-memory store keeps no durable evidence");
        assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    }
}
