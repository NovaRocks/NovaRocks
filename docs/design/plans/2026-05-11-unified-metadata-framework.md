# Unified Metadata Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete META-5 through META-12 by moving NovaRocks standalone metadata semantics onto provider-backed domain repositories.

**Architecture:** Keep `MetaStoreProvider` as the only low-level transaction API, add typed repositories in `src/meta/repository`, then migrate engine and managed-lake flows to those repositories. SQLite remains the first provider, but domain-specific SQLite tables and `SqliteMetadataStore` business APIs are removed or reduced to provider bootstrap glue.

**Tech Stack:** Rust, rusqlite, serde/serde_json, provider record model in `src/meta`, existing standalone engine and managed-lake modules, Cargo unit/integration tests, NovaRocks SQL test runner.

---

## File Structure

Create these files:

- `src/meta/keys.rs`: shared key construction helpers and stable namespace constants.
- `src/meta/payload.rs`: JSON payload encode/decode helpers used by repositories.
- `src/meta/repository/mod.rs`: exports repository modules and common repository error/result types.
- `src/meta/repository/id_scopes.rs`: stable `IdScope` constructors for all metadata domains.
- `src/meta/repository/managed_lake.rs`: db/table/schema/partition/index/tablet records and operations.
- `src/meta/repository/managed_txn.rs`: managed-lake publish transaction state machine.
- `src/meta/repository/mv.rs`: MV definition, lookup, dependency, refresh intent records and operations.
- `src/meta/repository/iceberg_catalog.rs`: NovaRocks-owned Iceberg catalog/namespace/table registration.
- `src/meta/repository/job.rs`: erase, Iceberg optimize, and refresh job records and claim/finish/fail transitions.
- `src/engine/mv/mod.rs`: neutral MV orchestration module entrypoint.
- `src/engine/mv/iceberg_refresh.rs`: relocated Iceberg MV orchestration from `connector/starrocks/managed`.
- `tests/meta_repository.rs`: repository-level tests over `SqliteMetaStoreProvider`.
- `tests/meta_framework_flow.rs`: focused flow tests that exercise engine/connector paths through repositories.

Modify these files:

- `src/meta/mod.rs`: export `keys`, `payload`, and `repository`.
- `src/meta/id.rs`: expose stable constructors/helpers needed by `repository/id_scopes.rs`.
- `src/meta/provider.rs`: add any small helper trait methods only if tests prove the current API is insufficient.
- `src/meta/sqlite/schema.rs`: keep only provider-generic schema creation for `meta_*` tables.
- `src/meta/sqlite/txn.rs`: fix provider shell issues found while adding repository tests.
- `src/common/app_config.rs`: add `[metadata]` config and remove standalone metadata path ownership from `[standalone_server]`.
- `src/server/mod.rs`: pass metadata provider config into standalone engine startup.
- `src/main.rs`: update standalone-server CLI config parsing tests if metadata config changes command behavior.
- `src/engine/mod.rs`: replace `metadata_store: Option<SqliteMetadataStore>` with provider/repository handles and reconstruct runtime catalog from repositories.
- `src/engine/statement.rs`: route Iceberg catalog registration and drop cleanup through `IcebergCatalogMetaRepository` and `MvMetaRepository`.
- `src/engine/information_schema.rs`: read MV metadata from `MvMetaRepository`.
- `src/connector/mod.rs`: stop exporting `SqliteMetadataStore` as the public standalone metadata facade.
- `src/connector/starrocks/managed/store.rs`: remove domain-specific SQLite store logic after repository migration.
- `src/connector/starrocks/managed/ddl.rs`: move managed-lake DDL metadata writes to `ManagedLakeMetaRepository`.
- `src/connector/starrocks/managed/catalog.rs`: rebuild `ManagedLakeCatalog` from repository snapshots.
- `src/connector/starrocks/managed/txn.rs`: move prepare/written/visible/abort metadata operations to `ManagedLakeTxnRepository`.
- `src/connector/starrocks/managed/erase.rs`: move erase job metadata to `JobMetaRepository`.
- `src/connector/starrocks/managed/mv_ddl.rs`: move MV metadata writes and lookups to `MvMetaRepository`.
- `src/connector/starrocks/managed/mv_refresh.rs`: use `MvMetaRepository` refresh state and managed-lake repository adapters.
- `src/connector/starrocks/managed/mv_refresh_iceberg.rs`: shrink to a compatibility wrapper or remove after relocation.
- `docker/iceberg-rest/up.sh`: generate `[metadata]` config instead of `[standalone_server].metadata_db_path`.
- `tests/sql-test-runner/conf/standalone_managed_lake.toml`: update generated/example config shape.
- `tests/standalone_mysql_server.rs`: update direct SQLite assertions to repository/provider assertions.

## Task 1: Repository Support Primitives

**Files:**
- Create: `src/meta/keys.rs`
- Create: `src/meta/payload.rs`
- Create: `src/meta/repository/mod.rs`
- Create: `src/meta/repository/id_scopes.rs`
- Modify: `src/meta/mod.rs`
- Modify: `src/meta/id.rs`
- Test: `tests/meta_repository.rs`

- [ ] **Step 1: Write primitive tests**

Add `tests/meta_repository.rs` with these initial tests:

```rust
use bytes::Bytes;
use novarocks::meta::repository::{
    RepositoryError, decode_json_payload, encode_json_payload, id_scopes,
};
use novarocks::meta::{IdScope, MetaKey};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SamplePayload {
    id: i64,
    name: String,
}

#[test]
fn repository_payload_json_round_trips() {
    let payload = SamplePayload {
        id: 7,
        name: "orders".to_string(),
    };
    let encoded = encode_json_payload(1, &payload).expect("encode payload");
    assert_eq!(encoded.schema_version, 1);
    assert_eq!(encoded.bytes, Bytes::from_static(br#"{"id":7,"name":"orders"}"#));

    let decoded: SamplePayload = decode_json_payload(&encoded).expect("decode payload");
    assert_eq!(decoded, payload);
}

#[test]
fn repository_id_scopes_are_stable_strings() {
    assert_eq!(id_scopes::managed_db().as_str(), "managed.db");
    assert_eq!(id_scopes::managed_table().as_str(), "managed.table");
    assert_eq!(id_scopes::managed_partition().as_str(), "managed.partition");
    assert_eq!(id_scopes::managed_index().as_str(), "managed.index");
    assert_eq!(id_scopes::managed_tablet().as_str(), "managed.tablet");
    assert_eq!(id_scopes::managed_txn().as_str(), "managed.txn");
    assert_eq!(id_scopes::mv_id().as_str(), "mv.id");
    assert_eq!(id_scopes::refresh_id().as_str(), "refresh.id");
    assert_eq!(id_scopes::erase_job().as_str(), "job.erase");
    assert_eq!(id_scopes::iceberg_optimize_job().as_str(), "job.iceberg_optimize");
}

#[test]
fn repository_error_display_is_domain_facing() {
    let err = RepositoryError::conflict("managed txn state changed");
    assert_eq!(err.to_string(), "metadata repository conflict: managed txn state changed");
}

#[test]
fn key_helpers_reject_unescaped_path_separators() {
    let err = MetaKey::new("managed", ["table", "bad/name"]).expect_err("slash must fail");
    assert!(err.to_string().contains("invalid metadata key path segment"));
}
```

- [ ] **Step 2: Run primitive tests and verify they fail**

Run:

```bash
cargo test --test meta_repository repository_payload_json_round_trips repository_id_scopes_are_stable_strings repository_error_display_is_domain_facing key_helpers_reject_unescaped_path_separators
```

Expected: compile failure because `novarocks::meta::repository`, `encode_json_payload`, and `id_scopes` do not exist.

- [ ] **Step 3: Add key and payload helpers**

Create `src/meta/payload.rs`:

```rust
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::meta::{MetaError, MetaErrorKind, MetaPayload, MetaPayloadEncoding};

pub fn encode_json_payload<T: Serialize>(
    schema_version: i32,
    value: &T,
) -> Result<MetaPayload, MetaError> {
    let bytes = serde_json::to_vec(value).map_err(|err| {
        MetaError::new(
            MetaErrorKind::InvalidRequest,
            format!("serialize metadata payload failed: {err}"),
        )
    })?;
    Ok(MetaPayload::json(schema_version, Bytes::from(bytes)))
}

pub fn decode_json_payload<T: DeserializeOwned>(payload: &MetaPayload) -> Result<T, MetaError> {
    if payload.encoding != MetaPayloadEncoding::Json {
        return Err(MetaError::new(
            MetaErrorKind::InvalidRequest,
            "metadata payload is not JSON encoded",
        ));
    }
    serde_json::from_slice(payload.bytes.as_ref()).map_err(|err| {
        MetaError::new(
            MetaErrorKind::InvalidRequest,
            format!("decode metadata payload failed: {err}"),
        )
    })
}
```

Create `src/meta/keys.rs`:

```rust
pub const NS_MANAGED: &str = "managed";
pub const NS_MANAGED_TXN: &str = "managed.txn";
pub const NS_MV: &str = "mv";
pub const NS_ICEBERG_CATALOG: &str = "iceberg.catalog";
pub const NS_JOB: &str = "job";

pub fn normalize_lookup_name(value: &str) -> String {
    value.to_ascii_lowercase()
}
```

- [ ] **Step 4: Add repository common module and ID scopes**

Create `src/meta/repository/mod.rs`:

```rust
use std::fmt;

pub mod iceberg_catalog;
pub mod id_scopes;
pub mod job;
pub mod managed_lake;
pub mod managed_txn;
pub mod mv;

pub use crate::meta::payload::{decode_json_payload, encode_json_payload};

pub type RepositoryResult<T> = Result<T, RepositoryError>;

#[derive(Debug)]
pub struct RepositoryError {
    kind: RepositoryErrorKind,
    message: String,
}

impl RepositoryError {
    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: RepositoryErrorKind::Conflict,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            kind: RepositoryErrorKind::NotFound,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: RepositoryErrorKind::Invalid,
            message: message.into(),
        }
    }

    pub fn provider(message: impl Into<String>) -> Self {
        Self {
            kind: RepositoryErrorKind::Provider,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> RepositoryErrorKind {
        self.kind
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepositoryErrorKind {
    Conflict,
    NotFound,
    Invalid,
    Provider,
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self.kind {
            RepositoryErrorKind::Conflict => "conflict",
            RepositoryErrorKind::NotFound => "not found",
            RepositoryErrorKind::Invalid => "invalid request",
            RepositoryErrorKind::Provider => "provider error",
        };
        write!(f, "metadata repository {label}: {}", self.message)
    }
}

impl std::error::Error for RepositoryError {}

impl From<crate::meta::MetaError> for RepositoryError {
    fn from(err: crate::meta::MetaError) -> Self {
        match err.kind() {
            crate::meta::MetaErrorKind::Conflict | crate::meta::MetaErrorKind::AlreadyExists => {
                RepositoryError::conflict(err.to_string())
            }
            crate::meta::MetaErrorKind::NotFound => RepositoryError::not_found(err.to_string()),
            crate::meta::MetaErrorKind::InvalidRequest
            | crate::meta::MetaErrorKind::Unsupported => RepositoryError::invalid(err.to_string()),
            _ => RepositoryError::provider(err.to_string()),
        }
    }
}
```

Create `src/meta/repository/id_scopes.rs`:

```rust
use crate::meta::{IdScope, MetaError};

fn scope(value: &'static str) -> IdScope {
    IdScope::new(value).expect("static metadata id scope must be valid")
}

pub fn managed_db() -> IdScope {
    scope("managed.db")
}

pub fn managed_table() -> IdScope {
    scope("managed.table")
}

pub fn managed_partition() -> IdScope {
    scope("managed.partition")
}

pub fn managed_index() -> IdScope {
    scope("managed.index")
}

pub fn managed_tablet() -> IdScope {
    scope("managed.tablet")
}

pub fn managed_txn() -> IdScope {
    scope("managed.txn")
}

pub fn mv_id() -> IdScope {
    scope("mv.id")
}

pub fn refresh_id() -> IdScope {
    scope("refresh.id")
}

pub fn erase_job() -> IdScope {
    scope("job.erase")
}

pub fn iceberg_optimize_job() -> IdScope {
    scope("job.iceberg_optimize")
}

pub fn custom(value: impl Into<String>) -> Result<IdScope, MetaError> {
    IdScope::new(value)
}
```

Create empty module files with compilable placeholders:

```rust
// src/meta/repository/managed_lake.rs
// Domain repository implementation is added by the managed-lake task.
```

Repeat that single comment for `managed_txn.rs`, `mv.rs`, `iceberg_catalog.rs`, and `job.rs`.

- [ ] **Step 5: Export new modules**

Modify `src/meta/mod.rs`:

```rust
pub mod error;
pub mod id;
pub mod keys;
pub mod payload;
pub mod provider;
pub mod record;
pub mod repository;
pub mod sqlite;

pub use error::{MetaError, MetaErrorKind};
pub use id::IdScope;
pub use provider::{
    MetaCommitOutcome, MetaReadTxn, MetaStoreCapabilities, MetaStoreProvider, MetaWriteTxn,
};
pub use record::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaPayload, MetaPayloadEncoding, MetaRecord,
    MetaRecordKind, MetaRecordPut, MetaRevision,
};
pub use sqlite::SqliteMetaStoreProvider;
```

- [ ] **Step 6: Run primitive tests**

Run:

```bash
cargo test --test meta_repository repository_payload_json_round_trips repository_id_scopes_are_stable_strings repository_error_display_is_domain_facing key_helpers_reject_unescaped_path_separators
```

Expected: all four tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/meta tests/meta_repository.rs
git commit -m "feat(meta): add repository primitives"
```

## Task 2: MV Metadata Repository

**Files:**
- Modify: `src/meta/repository/mv.rs`
- Modify: `src/meta/repository/mod.rs`
- Test: `tests/meta_repository.rs`

- [ ] **Step 1: Add MV repository tests**

Append to `tests/meta_repository.rs`:

```rust
use std::collections::BTreeMap;

use novarocks::meta::repository::mv::{
    CreateMvDefinitionRequest, MvMetaRepository, MvRefreshFinalizeRequest,
    RefreshExternalOutcome,
};
use novarocks::meta::{MetaStoreProvider, SqliteMetaStoreProvider};

#[test]
fn mv_repository_creates_definition_and_target_lookup() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repo = MvMetaRepository::default();

    let mut txn = provider.begin_write("create mv")?;
    let mv = repo.create_definition(
        &mut *txn,
        CreateMvDefinitionRequest {
            select_sql: "select id from ice.ns.orders".to_string(),
            base_table_refs: vec!["ice.ns.orders".to_string()],
            primary_key_columns: vec!["id".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("ns".to_string()),
            target_table: Some("orders_mv".to_string()),
            created_at_ms: 11,
        },
    )?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let loaded = repo.load_by_id(&*read, mv.mv_id)?.expect("mv by id");
    assert_eq!(loaded.mv_id, mv.mv_id);
    assert_eq!(loaded.select_sql, "select id from ice.ns.orders");

    let by_target = repo
        .find_by_target(&*read, "ICE", "NS", "ORDERS_MV")?
        .expect("target lookup");
    assert_eq!(by_target.mv_id, mv.mv_id);
    Ok(())
}

#[test]
fn mv_repository_refresh_intent_finalizes_once() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repo = MvMetaRepository::default();

    let mut txn = provider.begin_write("create mv")?;
    let mv = repo.create_definition(
        &mut *txn,
        CreateMvDefinitionRequest {
            select_sql: "select id from ice.ns.orders".to_string(),
            base_table_refs: vec!["ice.ns.orders".to_string()],
            primary_key_columns: vec!["id".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("ns".to_string()),
            target_table: Some("orders_mv".to_string()),
            created_at_ms: 11,
        },
    )?;
    let mut target_snapshots = BTreeMap::new();
    target_snapshots.insert("ice.ns.orders".to_string(), 100);
    let intent = repo.begin_refresh_intent(&mut *txn, mv.mv_id, target_snapshots.clone())?;
    txn.commit()?;

    let mut txn = provider.begin_write("record external commit")?;
    repo.record_external_commit_outcome(
        &mut *txn,
        intent.refresh_id,
        RefreshExternalOutcome {
            target_snapshot_id: Some(200),
            commit_id: "iceberg-snapshot-200".to_string(),
        },
    )?;
    repo.finalize_refresh(
        &mut *txn,
        MvRefreshFinalizeRequest {
            refresh_id: intent.refresh_id,
            rows: 3,
            base_snapshots: target_snapshots,
            base_table_uuids: BTreeMap::new(),
            target_snapshot_id: Some(200),
        },
    )?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let finalized = repo.load_refresh(&*read, intent.refresh_id)?.expect("refresh");
    assert_eq!(finalized.state.as_str(), "FINALIZED");
    let loaded = repo.load_by_id(&*read, mv.mv_id)?.expect("mv");
    assert_eq!(loaded.last_refresh_rows, Some(3));
    assert_eq!(loaded.last_refreshed_iceberg_snapshot_id, Some(200));
    Ok(())
}
```

- [ ] **Step 2: Run MV tests and verify they fail**

Run:

```bash
cargo test --test meta_repository mv_repository_creates_definition_and_target_lookup mv_repository_refresh_intent_finalizes_once
```

Expected: compile failure because `MvMetaRepository` and MV record types do not exist.

- [ ] **Step 3: Implement MV record types and keys**

Replace `src/meta/repository/mv.rs` with:

```rust
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::meta::keys::{NS_MV, normalize_lookup_name};
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_json_payload, encode_json_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaReadTxn, MetaRecordKind, MetaRecordPut, MetaRevision,
    MetaWriteTxn,
};

const MV_DEFINITION_KIND: &str = "mv.definition";
const MV_TARGET_LOOKUP_KIND: &str = "mv.target_lookup";
const MV_REFRESH_KIND: &str = "mv.refresh";
const MV_SCHEMA_VERSION: i32 = 1;

#[derive(Default)]
pub struct MvMetaRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvDefinition {
    pub mv_id: i64,
    pub select_sql: String,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: BTreeMap<String, i64>,
    pub last_refresh_table_uuids: BTreeMap<String, String>,
    pub last_refreshed_iceberg_snapshot_id: Option<i64>,
    pub refresh_in_progress: bool,
    pub refresh_target_snapshots: BTreeMap<String, i64>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct VersionedMvDefinition {
    pub record_revision: MetaRevision,
    pub value: StoredMvDefinition,
}

#[derive(Clone, Debug)]
pub struct CreateMvDefinitionRequest {
    pub select_sql: String,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvTargetLookup {
    pub mv_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvRefresh {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub state: MvRefreshState,
    pub target_snapshots: BTreeMap<String, i64>,
    pub external_outcome: Option<RefreshExternalOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MvRefreshState {
    IntentCreated,
    ExternalCommitted,
    Finalized,
    AbortRequested,
    Aborted,
    CommitUnknown,
}

impl MvRefreshState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IntentCreated => "INTENT_CREATED",
            Self::ExternalCommitted => "EXTERNAL_COMMITTED",
            Self::Finalized => "FINALIZED",
            Self::AbortRequested => "ABORT_REQUESTED",
            Self::Aborted => "ABORTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshExternalOutcome {
    pub target_snapshot_id: Option<i64>,
    pub commit_id: String,
}

#[derive(Clone, Debug)]
pub struct MvRefreshFinalizeRequest {
    pub refresh_id: i64,
    pub rows: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
    pub target_snapshot_id: Option<i64>,
}

impl MvMetaRepository {
    pub fn create_definition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateMvDefinitionRequest,
    ) -> RepositoryResult<StoredMvDefinition> {
        let mv_id = txn.allocate_id(id_scopes::mv_id())?;
        let definition = StoredMvDefinition {
            mv_id,
            select_sql: req.select_sql,
            base_table_refs: req.base_table_refs,
            primary_key_columns: req.primary_key_columns,
            storage_engine: req.storage_engine,
            target_catalog: req.target_catalog,
            target_namespace: req.target_namespace,
            target_table: req.target_table,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: BTreeMap::new(),
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            refresh_target_snapshots: BTreeMap::new(),
            created_at_ms: req.created_at_ms,
        };
        put_json(
            txn,
            key_by_id(mv_id)?,
            MV_DEFINITION_KIND,
            ExpectedRevision::NotExists,
            &definition,
        )?;
        if let (Some(catalog), Some(namespace), Some(table)) = (
            definition.target_catalog.as_deref(),
            definition.target_namespace.as_deref(),
            definition.target_table.as_deref(),
        ) {
            put_json(
                txn,
                key_by_target(catalog, namespace, table)?,
                MV_TARGET_LOOKUP_KIND,
                ExpectedRevision::NotExists,
                &MvTargetLookup { mv_id },
            )?;
        }
        Ok(definition)
    }

    pub fn load_by_id(
        &self,
        txn: &dyn MetaReadTxn,
        mv_id: i64,
    ) -> RepositoryResult<Option<StoredMvDefinition>> {
        let Some(record) = txn.get(&key_by_id(mv_id)?)? else {
            return Ok(None);
        };
        Ok(Some(decode_json_payload(&record.payload)?))
    }

    pub fn load_versioned_by_id(
        &self,
        txn: &dyn MetaReadTxn,
        mv_id: i64,
    ) -> RepositoryResult<Option<VersionedMvDefinition>> {
        let Some(record) = txn.get(&key_by_id(mv_id)?)? else {
            return Ok(None);
        };
        Ok(Some(VersionedMvDefinition {
            record_revision: record.revision,
            value: decode_json_payload(&record.payload)?,
        }))
    }

    pub fn find_by_target(
        &self,
        txn: &dyn MetaReadTxn,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> RepositoryResult<Option<StoredMvDefinition>> {
        let Some(record) = txn.get(&key_by_target(catalog, namespace, table)?)? else {
            return Ok(None);
        };
        let lookup: MvTargetLookup = decode_json_payload(&record.payload)?;
        self.load_by_id(txn, lookup.mv_id)
    }

    pub fn begin_refresh_intent(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_id: i64,
        target_snapshots: BTreeMap<String, i64>,
    ) -> RepositoryResult<StoredMvRefresh> {
        let refresh_id = txn.allocate_id(id_scopes::refresh_id())?;
        let refresh = StoredMvRefresh {
            refresh_id,
            mv_id,
            state: MvRefreshState::IntentCreated,
            target_snapshots,
            external_outcome: None,
        };
        put_json(
            txn,
            key_refresh(refresh_id)?,
            MV_REFRESH_KIND,
            ExpectedRevision::NotExists,
            &refresh,
        )?;
        Ok(refresh)
    }

    pub fn load_refresh(
        &self,
        txn: &dyn MetaReadTxn,
        refresh_id: i64,
    ) -> RepositoryResult<Option<StoredMvRefresh>> {
        let Some(record) = txn.get(&key_refresh(refresh_id)?)? else {
            return Ok(None);
        };
        Ok(Some(decode_json_payload(&record.payload)?))
    }

    pub fn record_external_commit_outcome(
        &self,
        txn: &mut dyn MetaWriteTxn,
        refresh_id: i64,
        outcome: RefreshExternalOutcome,
    ) -> RepositoryResult<()> {
        let record = txn
            .get(&key_refresh(refresh_id)?)?
            .ok_or_else(|| RepositoryError::not_found(format!("refresh {refresh_id} not found")))?;
        let mut refresh: StoredMvRefresh = decode_json_payload(&record.payload)?;
        refresh.external_outcome = Some(outcome);
        refresh.state = MvRefreshState::ExternalCommitted;
        put_json(
            txn,
            key_refresh(refresh_id)?,
            MV_REFRESH_KIND,
            ExpectedRevision::Exact(record.revision),
            &refresh,
        )
    }

    pub fn finalize_refresh(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: MvRefreshFinalizeRequest,
    ) -> RepositoryResult<()> {
        let refresh_record = txn
            .get(&key_refresh(req.refresh_id)?)?
            .ok_or_else(|| RepositoryError::not_found(format!("refresh {} not found", req.refresh_id)))?;
        let mut refresh: StoredMvRefresh = decode_json_payload(&refresh_record.payload)?;
        if refresh.state == MvRefreshState::Finalized {
            return Ok(());
        }
        if refresh.state != MvRefreshState::ExternalCommitted {
            return Err(RepositoryError::conflict(format!(
                "refresh {} is not externally committed",
                req.refresh_id
            )));
        }
        let mv_record = txn
            .get(&key_by_id(refresh.mv_id)?)?
            .ok_or_else(|| RepositoryError::not_found(format!("mv {} not found", refresh.mv_id)))?;
        let mut mv: StoredMvDefinition = decode_json_payload(&mv_record.payload)?;
        mv.last_refresh_rows = Some(req.rows);
        mv.last_refresh_snapshots = req.base_snapshots;
        mv.last_refresh_table_uuids = req.base_table_uuids;
        mv.last_refreshed_iceberg_snapshot_id = req.target_snapshot_id;
        mv.refresh_in_progress = false;
        mv.refresh_target_snapshots.clear();
        refresh.state = MvRefreshState::Finalized;
        put_json(
            txn,
            key_by_id(mv.mv_id)?,
            MV_DEFINITION_KIND,
            ExpectedRevision::Exact(mv_record.revision),
            &mv,
        )?;
        put_json(
            txn,
            key_refresh(req.refresh_id)?,
            MV_REFRESH_KIND,
            ExpectedRevision::Exact(refresh_record.revision),
            &refresh,
        )
    }
}

fn key_by_id(mv_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(NS_MV, ["by-id".to_string(), mv_id.to_string()])?)
}

fn key_by_target(catalog: &str, namespace: &str, table: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MV,
        [
            "by-target".to_string(),
            normalize_lookup_name(catalog),
            normalize_lookup_name(namespace),
            normalize_lookup_name(table),
        ],
    )?)
}

fn key_refresh(refresh_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(NS_MV, ["refresh".to_string(), refresh_id.to_string()])?)
}

fn put_json<T: Serialize>(
    txn: &mut dyn MetaWriteTxn,
    key: MetaKey,
    kind: &'static str,
    expected: ExpectedRevision,
    value: &T,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key,
        MetaRecordKind::new(kind)?,
        expected,
        encode_json_payload(MV_SCHEMA_VERSION, value)?,
    ))?;
    Ok(())
}
```

- [ ] **Step 4: Run MV repository tests**

Run:

```bash
cargo test --test meta_repository mv_repository_creates_definition_and_target_lookup mv_repository_refresh_intent_finalizes_once
```

Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/meta/repository/mv.rs tests/meta_repository.rs
git commit -m "feat(meta): add mv metadata repository"
```

## Task 3: Iceberg Catalog Repository

**Files:**
- Modify: `src/meta/repository/iceberg_catalog.rs`
- Modify: `src/meta/repository/mv.rs`
- Modify: `src/engine/statement.rs`
- Modify: `src/engine/mod.rs`
- Test: `tests/meta_repository.rs`

- [ ] **Step 1: Add Iceberg catalog repository tests**

Append to `tests/meta_repository.rs`:

```rust
use novarocks::meta::repository::iceberg_catalog::{
    IcebergCatalogMetaRepository, IcebergCatalogProperties,
};

#[test]
fn iceberg_catalog_repository_registers_catalog_namespace_and_table()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repo = IcebergCatalogMetaRepository::default();

    let mut txn = provider.begin_write("register iceberg")?;
    repo.upsert_catalog(
        &mut *txn,
        "ice",
        IcebergCatalogProperties {
            properties: vec![("type".to_string(), "rest".to_string())],
        },
    )?;
    repo.upsert_namespace(&mut *txn, "ice", "ns")?;
    repo.upsert_table(&mut *txn, "ice", "ns", "orders")?;
    txn.commit()?;

    let read = provider.begin_read()?;
    assert!(repo.catalog_exists(&*read, "ICE")?);
    assert!(repo.namespace_exists(&*read, "ice", "NS")?);
    assert!(repo.table_exists(&*read, "ICE", "ns", "ORDERS")?);
    Ok(())
}

#[test]
fn iceberg_catalog_repository_deletes_table_and_related_mv_lookup()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let catalog_repo = IcebergCatalogMetaRepository::default();
    let mv_repo = MvMetaRepository::default();

    let mut txn = provider.begin_write("seed table and mv")?;
    catalog_repo.upsert_catalog(
        &mut *txn,
        "ice",
        IcebergCatalogProperties { properties: vec![] },
    )?;
    catalog_repo.upsert_namespace(&mut *txn, "ice", "ns")?;
    catalog_repo.upsert_table(&mut *txn, "ice", "ns", "orders_mv")?;
    mv_repo.create_definition(
        &mut *txn,
        CreateMvDefinitionRequest {
            select_sql: "select id from ice.ns.orders".to_string(),
            base_table_refs: vec!["ice.ns.orders".to_string()],
            primary_key_columns: vec![],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("ns".to_string()),
            target_table: Some("orders_mv".to_string()),
            created_at_ms: 1,
        },
    )?;
    catalog_repo.delete_table_and_mv_relationships(&mut *txn, &mv_repo, "ICE", "NS", "ORDERS_MV")?;
    txn.commit()?;

    let read = provider.begin_read()?;
    assert!(!catalog_repo.table_exists(&*read, "ice", "ns", "orders_mv")?);
    assert!(mv_repo.find_by_target(&*read, "ice", "ns", "orders_mv")?.is_none());
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test --test meta_repository iceberg_catalog_repository_registers_catalog_namespace_and_table iceberg_catalog_repository_deletes_table_and_related_mv_lookup
```

Expected: compile failure because `IcebergCatalogMetaRepository` does not exist.

- [ ] **Step 3: Implement Iceberg catalog repository**

Replace `src/meta/repository/iceberg_catalog.rs` with:

```rust
use serde::{Deserialize, Serialize};

use crate::meta::keys::{NS_ICEBERG_CATALOG, normalize_lookup_name};
use crate::meta::repository::{
    RepositoryResult, decode_json_payload, encode_json_payload, mv::MvMetaRepository,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaReadTxn, MetaRecordKind, MetaRecordPut, MetaWriteTxn,
};

const CATALOG_KIND: &str = "iceberg.catalog";
const NAMESPACE_KIND: &str = "iceberg.namespace";
const TABLE_KIND: &str = "iceberg.table_registration";
const SCHEMA_VERSION: i32 = 1;

#[derive(Default)]
pub struct IcebergCatalogMetaRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergCatalogProperties {
    pub properties: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergNamespaceRecord {
    pub catalog: String,
    pub namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergTableRecord {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl IcebergCatalogMetaRepository {
    pub fn upsert_catalog(
        &self,
        txn: &mut dyn MetaWriteTxn,
        catalog: &str,
        properties: IcebergCatalogProperties,
    ) -> RepositoryResult<()> {
        put_json(txn, key_catalog(catalog)?, CATALOG_KIND, ExpectedRevision::Any, &properties)
    }

    pub fn catalog_exists(&self, txn: &dyn MetaReadTxn, catalog: &str) -> RepositoryResult<bool> {
        Ok(txn.get(&key_catalog(catalog)?)?.is_some())
    }

    pub fn upsert_namespace(
        &self,
        txn: &mut dyn MetaWriteTxn,
        catalog: &str,
        namespace: &str,
    ) -> RepositoryResult<()> {
        put_json(
            txn,
            key_namespace(catalog, namespace)?,
            NAMESPACE_KIND,
            ExpectedRevision::Any,
            &IcebergNamespaceRecord {
                catalog: normalize_lookup_name(catalog),
                namespace: normalize_lookup_name(namespace),
            },
        )
    }

    pub fn namespace_exists(
        &self,
        txn: &dyn MetaReadTxn,
        catalog: &str,
        namespace: &str,
    ) -> RepositoryResult<bool> {
        Ok(txn.get(&key_namespace(catalog, namespace)?)?.is_some())
    }

    pub fn upsert_table(
        &self,
        txn: &mut dyn MetaWriteTxn,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> RepositoryResult<()> {
        put_json(
            txn,
            key_table(catalog, namespace, table)?,
            TABLE_KIND,
            ExpectedRevision::Any,
            &IcebergTableRecord {
                catalog: normalize_lookup_name(catalog),
                namespace: normalize_lookup_name(namespace),
                table: normalize_lookup_name(table),
            },
        )
    }

    pub fn table_exists(
        &self,
        txn: &dyn MetaReadTxn,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> RepositoryResult<bool> {
        Ok(txn.get(&key_table(catalog, namespace, table)?)?.is_some())
    }

    pub fn delete_table_and_mv_relationships(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_repo: &MvMetaRepository,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> RepositoryResult<()> {
        txn.delete(&key_table(catalog, namespace, table)?, ExpectedRevision::Any)?;
        mv_repo.drop_by_target(txn, catalog, namespace, table)?;
        Ok(())
    }
}

fn key_catalog(catalog: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_ICEBERG_CATALOG,
        ["catalog".to_string(), normalize_lookup_name(catalog)],
    )?)
}

fn key_namespace(catalog: &str, namespace: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_ICEBERG_CATALOG,
        [
            "namespace".to_string(),
            normalize_lookup_name(catalog),
            normalize_lookup_name(namespace),
        ],
    )?)
}

fn key_table(catalog: &str, namespace: &str, table: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_ICEBERG_CATALOG,
        [
            "table".to_string(),
            normalize_lookup_name(catalog),
            normalize_lookup_name(namespace),
            normalize_lookup_name(table),
        ],
    )?)
}

fn put_json<T: Serialize>(
    txn: &mut dyn MetaWriteTxn,
    key: MetaKey,
    kind: &'static str,
    expected: ExpectedRevision,
    value: &T,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key,
        MetaRecordKind::new(kind)?,
        expected,
        encode_json_payload(SCHEMA_VERSION, value)?,
    ))?;
    Ok(())
}
```

- [ ] **Step 4: Add MV drop-by-target API used by catalog cleanup**

Add to `impl MvMetaRepository` in `src/meta/repository/mv.rs`:

```rust
pub fn drop_by_target(
    &self,
    txn: &mut dyn MetaWriteTxn,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> RepositoryResult<bool> {
    let key = key_by_target(catalog, namespace, table)?;
    let Some(record) = txn.get(&key)? else {
        return Ok(false);
    };
    let lookup: MvTargetLookup = decode_json_payload(&record.payload)?;
    txn.delete(&key, ExpectedRevision::Exact(record.revision))?;
    txn.delete(&key_by_id(lookup.mv_id)?, ExpectedRevision::Any)?;
    Ok(true)
}
```

- [ ] **Step 5: Run Iceberg catalog repository tests**

Run:

```bash
cargo test --test meta_repository iceberg_catalog_repository_registers_catalog_namespace_and_table iceberg_catalog_repository_deletes_table_and_related_mv_lookup
```

Expected: both tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/meta/repository/iceberg_catalog.rs src/meta/repository/mv.rs tests/meta_repository.rs
git commit -m "feat(meta): add iceberg catalog repository"
```

## Task 4: Managed-Lake Object Repository

**Files:**
- Modify: `src/meta/repository/managed_lake.rs`
- Modify: `src/connector/starrocks/managed/catalog.rs`
- Modify: `src/connector/starrocks/managed/ddl.rs`
- Modify: `src/engine/mod.rs`
- Test: `tests/meta_repository.rs`

- [ ] **Step 1: Add managed-lake repository tests**

Append to `tests/meta_repository.rs`:

```rust
use novarocks::meta::repository::managed_lake::{
    CreateManagedDatabaseRequest, CreateManagedTableRequest, ManagedLakeMetaRepository,
    ManagedPartitionState,
};

#[test]
fn managed_lake_repository_creates_database_table_and_active_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repo = ManagedLakeMetaRepository::default();

    let mut txn = provider.begin_write("create managed table")?;
    let db = repo.create_database(
        &mut *txn,
        CreateManagedDatabaseRequest {
            name: "db1".to_string(),
        },
    )?;
    let table = repo.create_table(
        &mut *txn,
        CreateManagedTableRequest {
            db_id: db.db_id,
            name: "orders".to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 2,
            current_schema_id: 10,
        },
    )?;
    let partition = repo.create_partition(&mut *txn, table.table_id, "orders", 1)?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let snapshot = repo.load_snapshot(&*read)?;
    assert_eq!(snapshot.databases.len(), 1);
    assert_eq!(snapshot.tables.len(), 1);
    assert_eq!(snapshot.partitions.len(), 1);
    assert_eq!(snapshot.partitions[0].state, ManagedPartitionState::Active);
    assert_eq!(snapshot.partitions[0].visible_version, 1);
    assert_eq!(partition.next_version, 2);
    Ok(())
}

#[test]
fn managed_lake_repository_rejects_duplicate_table_name()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repo = ManagedLakeMetaRepository::default();

    let mut txn = provider.begin_write("create duplicate table")?;
    let db = repo.create_database(
        &mut *txn,
        CreateManagedDatabaseRequest { name: "db1".to_string() },
    )?;
    repo.create_table(
        &mut *txn,
        CreateManagedTableRequest {
            db_id: db.db_id,
            name: "orders".to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 2,
            current_schema_id: 10,
        },
    )?;
    let err = repo
        .create_table(
            &mut *txn,
            CreateManagedTableRequest {
                db_id: db.db_id,
                name: "ORDERS".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 11,
            },
        )
        .expect_err("duplicate name must fail");
    assert!(err.to_string().contains("already exists"));
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test --test meta_repository managed_lake_repository_creates_database_table_and_active_partition managed_lake_repository_rejects_duplicate_table_name
```

Expected: compile failure because `ManagedLakeMetaRepository` does not exist.

- [ ] **Step 3: Implement managed-lake object records**

Implement `src/meta/repository/managed_lake.rs` with typed structs matching the fields currently in `src/connector/starrocks/managed/store.rs`:

```rust
use serde::{Deserialize, Serialize};

use crate::meta::keys::{NS_MANAGED, normalize_lookup_name};
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_json_payload, encode_json_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecordKind, MetaRecordPut,
    MetaWriteTxn,
};

const DB_KIND: &str = "managed.database";
const DB_NAME_KIND: &str = "managed.database_name";
const TABLE_KIND: &str = "managed.table";
const TABLE_NAME_KIND: &str = "managed.table_name";
const PARTITION_KIND: &str = "managed.partition";
const SCHEMA_VERSION: i32 = 1;

#[derive(Default)]
pub struct ManagedLakeMetaRepository;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManagedLakeSnapshot {
    pub databases: Vec<StoredManagedDatabase>,
    pub tables: Vec<StoredManagedTable>,
    pub partitions: Vec<StoredManagedPartition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedDatabase {
    pub db_id: i64,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedTable {
    pub table_id: i64,
    pub db_id: i64,
    pub name: String,
    pub keys_type: String,
    pub bucket_num: i64,
    pub current_schema_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedPartition {
    pub partition_id: i64,
    pub table_id: i64,
    pub name: String,
    pub visible_version: i64,
    pub next_version: i64,
    pub state: ManagedPartitionState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManagedPartitionState {
    Creating,
    Active,
    Retired,
    Failed,
}

#[derive(Clone, Debug)]
pub struct CreateManagedDatabaseRequest {
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct CreateManagedTableRequest {
    pub db_id: i64,
    pub name: String,
    pub keys_type: String,
    pub bucket_num: i64,
    pub current_schema_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IdLookup {
    id: i64,
}
```

Then implement these methods:

```rust
impl ManagedLakeMetaRepository {
    pub fn create_database(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateManagedDatabaseRequest,
    ) -> RepositoryResult<StoredManagedDatabase> {
        let lookup_key = key_database_name(&req.name)?;
        if txn.get(&lookup_key)?.is_some() {
            return Err(RepositoryError::conflict(format!(
                "managed database `{}` already exists",
                req.name
            )));
        }
        let db_id = txn.allocate_id(id_scopes::managed_db())?;
        let db = StoredManagedDatabase { db_id, name: req.name };
        put_json(txn, key_database(db_id)?, DB_KIND, ExpectedRevision::NotExists, &db)?;
        put_json(
            txn,
            lookup_key,
            DB_NAME_KIND,
            ExpectedRevision::NotExists,
            &IdLookup { id: db_id },
        )?;
        Ok(db)
    }

    pub fn create_table(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateManagedTableRequest,
    ) -> RepositoryResult<StoredManagedTable> {
        let lookup_key = key_table_name(req.db_id, &req.name)?;
        if txn.get(&lookup_key)?.is_some() {
            return Err(RepositoryError::conflict(format!(
                "managed table `{}` already exists",
                req.name
            )));
        }
        let table_id = txn.allocate_id(id_scopes::managed_table())?;
        let table = StoredManagedTable {
            table_id,
            db_id: req.db_id,
            name: req.name,
            keys_type: req.keys_type,
            bucket_num: req.bucket_num,
            current_schema_id: req.current_schema_id,
        };
        put_json(txn, key_table(table_id)?, TABLE_KIND, ExpectedRevision::NotExists, &table)?;
        put_json(
            txn,
            lookup_key,
            TABLE_NAME_KIND,
            ExpectedRevision::NotExists,
            &IdLookup { id: table_id },
        )?;
        Ok(table)
    }

    pub fn create_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        name: &str,
        visible_version: i64,
    ) -> RepositoryResult<StoredManagedPartition> {
        let partition_id = txn.allocate_id(id_scopes::managed_partition())?;
        let partition = StoredManagedPartition {
            partition_id,
            table_id,
            name: name.to_string(),
            visible_version,
            next_version: visible_version + 1,
            state: ManagedPartitionState::Active,
        };
        put_json(
            txn,
            key_partition(partition_id)?,
            PARTITION_KIND,
            ExpectedRevision::NotExists,
            &partition,
        )?;
        Ok(partition)
    }

    pub fn load_snapshot(&self, txn: &dyn MetaReadTxn) -> RepositoryResult<ManagedLakeSnapshot> {
        let mut snapshot = ManagedLakeSnapshot::default();
        for record in txn.scan(&MetaKeyPrefix::new(NS_MANAGED, ["database"])?, None)? {
            snapshot.databases.push(decode_json_payload(&record.payload)?);
        }
        for record in txn.scan(&MetaKeyPrefix::new(NS_MANAGED, ["table"])?, None)? {
            snapshot.tables.push(decode_json_payload(&record.payload)?);
        }
        for record in txn.scan(&MetaKeyPrefix::new(NS_MANAGED, ["partition"])?, None)? {
            snapshot.partitions.push(decode_json_payload(&record.payload)?);
        }
        snapshot.databases.sort_by_key(|db| db.db_id);
        snapshot.tables.sort_by_key(|table| table.table_id);
        snapshot.partitions.sort_by_key(|partition| partition.partition_id);
        Ok(snapshot)
    }
}
```

Add private key and `put_json` helpers in the same file:

```rust
fn key_database(db_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(NS_MANAGED, ["database".to_string(), db_id.to_string()])?)
}

fn key_database_name(name: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        ["database-name".to_string(), normalize_lookup_name(name)],
    )?)
}

fn key_table(table_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(NS_MANAGED, ["table".to_string(), table_id.to_string()])?)
}

fn key_table_name(db_id: i64, name: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        [
            "table-name".to_string(),
            db_id.to_string(),
            normalize_lookup_name(name),
        ],
    )?)
}

fn key_partition(partition_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        ["partition".to_string(), partition_id.to_string()],
    )?)
}

fn put_json<T: Serialize>(
    txn: &mut dyn MetaWriteTxn,
    key: MetaKey,
    kind: &'static str,
    expected: ExpectedRevision,
    value: &T,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key,
        MetaRecordKind::new(kind)?,
        expected,
        encode_json_payload(SCHEMA_VERSION, value)?,
    ))?;
    Ok(())
}
```

- [ ] **Step 4: Run managed-lake repository tests**

Run:

```bash
cargo test --test meta_repository managed_lake_repository_creates_database_table_and_active_partition managed_lake_repository_rejects_duplicate_table_name
```

Expected: both tests pass.

- [ ] **Step 5: Expand repository structs to cover existing managed metadata**

Extend `ManagedLakeSnapshot` and `managed_lake.rs` to include the remaining current domain objects:

```rust
pub schemas: Vec<StoredManagedSchema>,
pub columns: Vec<StoredManagedColumn>,
pub indexes: Vec<StoredManagedIndex>,
pub tablets: Vec<StoredManagedTablet>,
```

Use field names and types from `src/connector/starrocks/managed/store.rs` so DDL and catalog reconstruction can migrate without lossy conversions.

- [ ] **Step 6: Commit**

```bash
git add src/meta/repository/managed_lake.rs tests/meta_repository.rs
git commit -m "feat(meta): add managed lake repository"
```

## Task 5: Managed-Lake Transaction Repository

**Files:**
- Modify: `src/meta/repository/managed_txn.rs`
- Modify: `src/meta/repository/managed_lake.rs`
- Modify: `src/connector/starrocks/managed/txn.rs`
- Test: `tests/meta_repository.rs`

- [ ] **Step 1: Add managed transaction tests**

Append to `tests/meta_repository.rs`:

```rust
use novarocks::meta::repository::managed_txn::{
    ManagedLakeTxnRepository, ManagedTxnState,
};

#[test]
fn managed_txn_repository_prepare_written_visible_advances_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();

    let mut txn = provider.begin_write("seed partition")?;
    let db = meta_repo.create_database(&mut *txn, CreateManagedDatabaseRequest { name: "db1".to_string() })?;
    let table = meta_repo.create_table(
        &mut *txn,
        CreateManagedTableRequest {
            db_id: db.db_id,
            name: "orders".to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 1,
            current_schema_id: 1,
        },
    )?;
    let partition = meta_repo.create_partition(&mut *txn, table.table_id, "orders", 1)?;
    let prepared = txn_repo.prepare(&meta_repo, &mut *txn, table.table_id, partition.partition_id)?;
    txn_repo.mark_written(&mut *txn, prepared.txn_id)?;
    txn_repo.mark_visible(&meta_repo, &mut *txn, prepared.txn_id)?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let stored = txn_repo.load(&*read, prepared.txn_id)?.expect("txn");
    assert_eq!(stored.state, ManagedTxnState::Visible);
    let partition_after = meta_repo
        .load_partition(&*read, partition.partition_id)?
        .expect("partition");
    assert_eq!(partition_after.visible_version, 2);
    assert_eq!(partition_after.next_version, 3);
    Ok(())
}

#[test]
fn managed_txn_repository_abort_does_not_advance_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();

    let mut txn = provider.begin_write("seed partition")?;
    let db = meta_repo.create_database(&mut *txn, CreateManagedDatabaseRequest { name: "db1".to_string() })?;
    let table = meta_repo.create_table(
        &mut *txn,
        CreateManagedTableRequest {
            db_id: db.db_id,
            name: "orders".to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 1,
            current_schema_id: 1,
        },
    )?;
    let partition = meta_repo.create_partition(&mut *txn, table.table_id, "orders", 1)?;
    let prepared = txn_repo.prepare(&meta_repo, &mut *txn, table.table_id, partition.partition_id)?;
    txn_repo.mark_aborted(&mut *txn, prepared.txn_id)?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let stored = txn_repo.load(&*read, prepared.txn_id)?.expect("txn");
    assert_eq!(stored.state, ManagedTxnState::Aborted);
    let partition_after = meta_repo
        .load_partition(&*read, partition.partition_id)?
        .expect("partition");
    assert_eq!(partition_after.visible_version, 1);
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test --test meta_repository managed_txn_repository_prepare_written_visible_advances_partition managed_txn_repository_abort_does_not_advance_partition
```

Expected: compile failure because `ManagedLakeTxnRepository` does not exist and `ManagedLakeMetaRepository::load_partition` is missing.

- [ ] **Step 3: Add partition load/update helpers**

In `src/meta/repository/managed_lake.rs`, add:

```rust
pub fn load_partition(
    &self,
    txn: &dyn MetaReadTxn,
    partition_id: i64,
) -> RepositoryResult<Option<StoredManagedPartition>> {
    let Some(record) = txn.get(&key_partition(partition_id)?)? else {
        return Ok(None);
    };
    Ok(Some(decode_json_payload(&record.payload)?))
}

pub fn update_partition_exact(
    &self,
    txn: &mut dyn MetaWriteTxn,
    partition: &StoredManagedPartition,
    expected: crate::meta::MetaRevision,
) -> RepositoryResult<()> {
    put_json(
        txn,
        key_partition(partition.partition_id)?,
        PARTITION_KIND,
        ExpectedRevision::Exact(expected),
        partition,
    )
}

pub fn load_versioned_partition(
    &self,
    txn: &dyn MetaReadTxn,
    partition_id: i64,
) -> RepositoryResult<Option<(crate::meta::MetaRevision, StoredManagedPartition)>> {
    let Some(record) = txn.get(&key_partition(partition_id)?)? else {
        return Ok(None);
    };
    Ok(Some((record.revision, decode_json_payload(&record.payload)?)))
}
```

- [ ] **Step 4: Implement managed transaction repository**

Replace `src/meta/repository/managed_txn.rs` with:

```rust
use serde::{Deserialize, Serialize};

use crate::meta::keys::NS_MANAGED_TXN;
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_json_payload, encode_json_payload, id_scopes,
    managed_lake::ManagedLakeMetaRepository,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaReadTxn, MetaRecordKind, MetaRecordPut, MetaWriteTxn,
};

const TXN_KIND: &str = "managed.txn";
const SCHEMA_VERSION: i32 = 1;

#[derive(Default)]
pub struct ManagedLakeTxnRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedTxn {
    pub txn_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub base_version: i64,
    pub commit_version: i64,
    pub state: ManagedTxnState,
    pub retry_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManagedTxnState {
    Prepared,
    Written,
    Visible,
    Aborted,
}

impl ManagedLakeTxnRepository {
    pub fn prepare(
        &self,
        meta_repo: &ManagedLakeMetaRepository,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        partition_id: i64,
    ) -> RepositoryResult<StoredManagedTxn> {
        let partition = meta_repo
            .load_partition(txn, partition_id)?
            .ok_or_else(|| RepositoryError::not_found(format!("partition {partition_id} not found")))?;
        let txn_id = txn.allocate_id(id_scopes::managed_txn())?;
        let stored = StoredManagedTxn {
            txn_id,
            table_id,
            partition_id,
            base_version: partition.visible_version,
            commit_version: partition.visible_version + 1,
            state: ManagedTxnState::Prepared,
            retry_at_ms: None,
            updated_at_ms: 0,
        };
        put_txn(txn, ExpectedRevision::NotExists, &stored)?;
        Ok(stored)
    }

    pub fn load(
        &self,
        txn: &dyn MetaReadTxn,
        txn_id: i64,
    ) -> RepositoryResult<Option<StoredManagedTxn>> {
        let Some(record) = txn.get(&key_txn(txn_id)?)? else {
            return Ok(None);
        };
        Ok(Some(decode_json_payload(&record.payload)?))
    }

    pub fn mark_written(
        &self,
        txn: &mut dyn MetaWriteTxn,
        txn_id: i64,
    ) -> RepositoryResult<()> {
        let record = txn
            .get(&key_txn(txn_id)?)?
            .ok_or_else(|| RepositoryError::not_found(format!("txn {txn_id} not found")))?;
        let mut stored: StoredManagedTxn = decode_json_payload(&record.payload)?;
        if stored.state != ManagedTxnState::Prepared {
            return Err(RepositoryError::conflict(format!("txn {txn_id} is not prepared")));
        }
        stored.state = ManagedTxnState::Written;
        put_txn(txn, ExpectedRevision::Exact(record.revision), &stored)
    }

    pub fn mark_visible(
        &self,
        meta_repo: &ManagedLakeMetaRepository,
        txn: &mut dyn MetaWriteTxn,
        txn_id: i64,
    ) -> RepositoryResult<()> {
        let record = txn
            .get(&key_txn(txn_id)?)?
            .ok_or_else(|| RepositoryError::not_found(format!("txn {txn_id} not found")))?;
        let mut stored: StoredManagedTxn = decode_json_payload(&record.payload)?;
        if stored.state != ManagedTxnState::Written {
            return Err(RepositoryError::conflict(format!("txn {txn_id} is not written")));
        }
        let (partition_rev, mut partition) = meta_repo
            .load_versioned_partition(txn, stored.partition_id)?
            .ok_or_else(|| RepositoryError::not_found(format!("partition {} not found", stored.partition_id)))?;
        if partition.visible_version != stored.base_version {
            return Err(RepositoryError::conflict(format!(
                "partition {} visible version changed from {} to {}",
                stored.partition_id, stored.base_version, partition.visible_version
            )));
        }
        partition.visible_version = stored.commit_version;
        partition.next_version = stored.commit_version + 1;
        stored.state = ManagedTxnState::Visible;
        meta_repo.update_partition_exact(txn, &partition, partition_rev)?;
        put_txn(txn, ExpectedRevision::Exact(record.revision), &stored)
    }

    pub fn mark_aborted(
        &self,
        txn: &mut dyn MetaWriteTxn,
        txn_id: i64,
    ) -> RepositoryResult<()> {
        let record = txn
            .get(&key_txn(txn_id)?)?
            .ok_or_else(|| RepositoryError::not_found(format!("txn {txn_id} not found")))?;
        let mut stored: StoredManagedTxn = decode_json_payload(&record.payload)?;
        stored.state = ManagedTxnState::Aborted;
        put_txn(txn, ExpectedRevision::Exact(record.revision), &stored)
    }
}

fn key_txn(txn_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(NS_MANAGED_TXN, [txn_id.to_string()])?)
}

fn put_txn(
    txn: &mut dyn MetaWriteTxn,
    expected: ExpectedRevision,
    value: &StoredManagedTxn,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_txn(value.txn_id)?,
        MetaRecordKind::new(TXN_KIND)?,
        expected,
        encode_json_payload(SCHEMA_VERSION, value)?,
    ))?;
    Ok(())
}
```

- [ ] **Step 5: Run managed transaction tests**

Run:

```bash
cargo test --test meta_repository managed_txn_repository_prepare_written_visible_advances_partition managed_txn_repository_abort_does_not_advance_partition
```

Expected: both tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/meta/repository/managed_lake.rs src/meta/repository/managed_txn.rs tests/meta_repository.rs
git commit -m "feat(meta): add managed lake txn repository"
```

## Task 6: Job Repository

**Files:**
- Modify: `src/meta/repository/job.rs`
- Modify: `src/connector/starrocks/managed/erase.rs`
- Test: `tests/meta_repository.rs`

- [ ] **Step 1: Add job repository tests**

Append to `tests/meta_repository.rs`:

```rust
use novarocks::meta::repository::job::{CreateEraseJobRequest, JobMetaRepository, JobState};

#[test]
fn job_repository_claim_finish_and_fail_are_state_checked()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repo = JobMetaRepository::default();

    let mut txn = provider.begin_write("create job")?;
    let job = repo.create_erase_job(
        &mut *txn,
        CreateEraseJobRequest {
            table_id: 10,
            partition_id: Some(20),
            root_path: "s3://bucket/db/table/partition".to_string(),
            now_ms: 1000,
        },
    )?;
    assert_eq!(job.state, JobState::Pending);
    txn.commit()?;

    let mut txn = provider.begin_write("claim job")?;
    let claimed = repo.claim_erase_job(&mut *txn, job.job_id, 1100)?;
    assert!(claimed);
    repo.finish_erase_job(&mut *txn, job.job_id, 1200)?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let loaded = repo.load_erase_job(&*read, job.job_id)?.expect("job");
    assert_eq!(loaded.state, JobState::Finished);
    Ok(())
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test --test meta_repository job_repository_claim_finish_and_fail_are_state_checked
```

Expected: compile failure because `JobMetaRepository` does not exist.

- [ ] **Step 3: Implement job repository**

Replace `src/meta/repository/job.rs` with a typed implementation:

```rust
use serde::{Deserialize, Serialize};

use crate::meta::keys::NS_JOB;
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_json_payload, encode_json_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaReadTxn, MetaRecordKind, MetaRecordPut, MetaWriteTxn,
};

const ERASE_JOB_KIND: &str = "job.erase";
const SCHEMA_VERSION: i32 = 1;

#[derive(Default)]
pub struct JobMetaRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredEraseJob {
    pub job_id: i64,
    pub table_id: i64,
    pub partition_id: Option<i64>,
    pub root_path: String,
    pub state: JobState,
    pub retry_at_ms: Option<i64>,
    pub updated_at_ms: i64,
    pub last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Pending,
    Running,
    Failed,
    Finished,
}

#[derive(Clone, Debug)]
pub struct CreateEraseJobRequest {
    pub table_id: i64,
    pub partition_id: Option<i64>,
    pub root_path: String,
    pub now_ms: i64,
}

impl JobMetaRepository {
    pub fn create_erase_job(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateEraseJobRequest,
    ) -> RepositoryResult<StoredEraseJob> {
        let job_id = txn.allocate_id(id_scopes::erase_job())?;
        let job = StoredEraseJob {
            job_id,
            table_id: req.table_id,
            partition_id: req.partition_id,
            root_path: req.root_path,
            state: JobState::Pending,
            retry_at_ms: None,
            updated_at_ms: req.now_ms,
            last_error: None,
        };
        put_job(txn, ExpectedRevision::NotExists, &job)?;
        Ok(job)
    }

    pub fn load_erase_job(
        &self,
        txn: &dyn MetaReadTxn,
        job_id: i64,
    ) -> RepositoryResult<Option<StoredEraseJob>> {
        let Some(record) = txn.get(&key_erase_job(job_id)?)? else {
            return Ok(None);
        };
        Ok(Some(decode_json_payload(&record.payload)?))
    }

    pub fn claim_erase_job(
        &self,
        txn: &mut dyn MetaWriteTxn,
        job_id: i64,
        now_ms: i64,
    ) -> RepositoryResult<bool> {
        let Some(record) = txn.get(&key_erase_job(job_id)?)? else {
            return Ok(false);
        };
        let mut job: StoredEraseJob = decode_json_payload(&record.payload)?;
        if !matches!(job.state, JobState::Pending | JobState::Failed) {
            return Ok(false);
        }
        job.state = JobState::Running;
        job.updated_at_ms = now_ms;
        job.last_error = None;
        put_job(txn, ExpectedRevision::Exact(record.revision), &job)?;
        Ok(true)
    }

    pub fn finish_erase_job(
        &self,
        txn: &mut dyn MetaWriteTxn,
        job_id: i64,
        now_ms: i64,
    ) -> RepositoryResult<()> {
        let record = txn
            .get(&key_erase_job(job_id)?)?
            .ok_or_else(|| RepositoryError::not_found(format!("erase job {job_id} not found")))?;
        let mut job: StoredEraseJob = decode_json_payload(&record.payload)?;
        if job.state != JobState::Running {
            return Err(RepositoryError::conflict(format!("erase job {job_id} is not running")));
        }
        job.state = JobState::Finished;
        job.updated_at_ms = now_ms;
        job.retry_at_ms = None;
        job.last_error = None;
        put_job(txn, ExpectedRevision::Exact(record.revision), &job)
    }
}

fn key_erase_job(job_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(NS_JOB, ["erase".to_string(), job_id.to_string()])?)
}

fn put_job(
    txn: &mut dyn MetaWriteTxn,
    expected: ExpectedRevision,
    value: &StoredEraseJob,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_erase_job(value.job_id)?,
        MetaRecordKind::new(ERASE_JOB_KIND)?,
        expected,
        encode_json_payload(SCHEMA_VERSION, value)?,
    ))?;
    Ok(())
}
```

- [ ] **Step 4: Run job repository test**

Run:

```bash
cargo test --test meta_repository job_repository_claim_finish_and_fail_are_state_checked
```

Expected: test passes.

- [ ] **Step 5: Commit**

```bash
git add src/meta/repository/job.rs tests/meta_repository.rs
git commit -m "feat(meta): add job repository"
```

## Task 7: Bootstrap Provider Configuration

**Files:**
- Modify: `src/common/app_config.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/main.rs`
- Modify: `docker/iceberg-rest/up.sh`
- Modify: `tests/sql-test-runner/conf/standalone_managed_lake.toml`
- Test: `src/common/app_config.rs`
- Test: `src/engine/mod.rs`

- [ ] **Step 1: Add config parsing tests**

In `src/common/app_config.rs`, add a test:

```rust
#[test]
fn test_metadata_config_parses_sqlite_provider() {
    let toml = r#"
[metadata]
provider = "sqlite"
path = "meta/catalog.db"

[standalone_server]
mysql_port = 19030
"#;
    let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse config");
    let metadata = cfg.metadata.expect("metadata config");
    assert_eq!(metadata.provider, MetadataProviderConfig::Sqlite);
    assert_eq!(metadata.path, PathBuf::from("meta/catalog.db"));
}
```

- [ ] **Step 2: Run config test and verify it fails**

Run:

```bash
cargo test common::app_config::tests::test_metadata_config_parses_sqlite_provider
```

Expected: compile failure because `metadata` and `MetadataProviderConfig` do not exist.

- [ ] **Step 3: Add metadata config structs**

In `src/common/app_config.rs`, add to `NovaRocksConfig`:

```rust
#[serde(default)]
pub metadata: Option<MetadataConfig>,
```

Add structs:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct MetadataConfig {
    #[serde(default)]
    pub provider: MetadataProviderConfig,
    pub path: PathBuf,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataProviderConfig {
    #[default]
    Sqlite,
}
```

Remove `metadata_db_path` from `StandaloneServerConfig` and update tests that previously expected it.

- [ ] **Step 4: Add provider opening helper in engine**

In `src/engine/mod.rs`, add a helper near current metadata opening code:

```rust
fn open_metadata_provider_from_config(
    opts: &StandaloneOptions,
) -> Result<Option<std::sync::Arc<dyn crate::meta::MetaStoreProvider>>, String> {
    let cfg = novarocks_config::config()?;
    let Some(metadata) = cfg.metadata.as_ref() else {
        return Ok(None);
    };
    let path = if metadata.path.is_absolute() {
        metadata.path.clone()
    } else if let Some(config_path) = opts.config_path.as_ref() {
        config_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(&metadata.path)
    } else {
        metadata.path.clone()
    };
    match metadata.provider {
        crate::common::app_config::MetadataProviderConfig::Sqlite => {
            let provider = crate::meta::SqliteMetaStoreProvider::open(path)
                .map_err(|err| format!("open sqlite metadata provider failed: {err}"))?;
            Ok(Some(std::sync::Arc::new(provider)))
        }
    }
}
```

- [ ] **Step 5: Update generated configs**

In `docker/iceberg-rest/up.sh`, change generated config from:

```toml
[standalone_server]
metadata_db_path = "$runtime_dir/standalone-managed-lake.sqlite"
```

to:

```toml
[metadata]
provider = "sqlite"
path = "$runtime_dir/standalone-managed-lake.sqlite"

[standalone_server]
```

Make the same shape change in `tests/sql-test-runner/conf/standalone_managed_lake.toml`.

- [ ] **Step 6: Run config tests**

Run:

```bash
cargo test common::app_config::tests::test_metadata_config_parses_sqlite_provider common::app_config::tests::test_standalone_server_defaults common::app_config::tests::test_standalone_server_tables_can_be_overridden
```

Expected: tests pass after updating expected structs.

- [ ] **Step 7: Commit**

```bash
git add src/common/app_config.rs src/engine/mod.rs src/server/mod.rs src/main.rs docker/iceberg-rest/up.sh tests/sql-test-runner/conf/standalone_managed_lake.toml
git commit -m "feat(meta): bootstrap metadata provider from config"
```

## Task 8: Migrate Engine and Managed-Lake Flows to Repositories

**Files:**
- Modify: `src/engine/mod.rs`
- Modify: `src/engine/statement.rs`
- Modify: `src/engine/information_schema.rs`
- Modify: `src/connector/mod.rs`
- Modify: `src/connector/starrocks/managed/catalog.rs`
- Modify: `src/connector/starrocks/managed/ddl.rs`
- Modify: `src/connector/starrocks/managed/txn.rs`
- Modify: `src/connector/starrocks/managed/erase.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Test: existing module tests under these files

- [ ] **Step 1: Add repository handles to `StandaloneState`**

In `src/engine/mod.rs`, replace:

```rust
pub(crate) metadata_store: Option<SqliteMetadataStore>,
```

with:

```rust
pub(crate) metadata_provider: Option<std::sync::Arc<dyn crate::meta::MetaStoreProvider>>,
pub(crate) managed_repo: crate::meta::repository::managed_lake::ManagedLakeMetaRepository,
pub(crate) managed_txn_repo: crate::meta::repository::managed_txn::ManagedLakeTxnRepository,
pub(crate) mv_repo: crate::meta::repository::mv::MvMetaRepository,
pub(crate) iceberg_catalog_repo: crate::meta::repository::iceberg_catalog::IcebergCatalogMetaRepository,
pub(crate) job_repo: crate::meta::repository::job::JobMetaRepository,
```

Initialize each repository with `::default()` in `Default` and engine construction.

- [ ] **Step 2: Compile to find direct `metadata_store` callers**

Run:

```bash
cargo check --all-targets
```

Expected: compile errors at every remaining direct `state.metadata_store` or `SqliteMetadataStore` call site.

- [ ] **Step 3: Migrate Iceberg catalog registration helpers**

Replace current `store.upsert_iceberg_catalog`, `store.upsert_iceberg_namespace`, `store.upsert_iceberg_table`, and delete helpers in `src/engine/mod.rs` / `src/engine/statement.rs` with provider transactions:

```rust
let Some(provider) = state.metadata_provider.as_ref() else {
    return Ok(());
};
let mut txn = provider
    .begin_write("register iceberg table")
    .map_err(|err| err.to_string())?;
state
    .iceberg_catalog_repo
    .upsert_table(&mut *txn, catalog_name, namespace_name, table_name)
    .map_err(|err| err.to_string())?;
txn.commit().map_err(|err| err.to_string())?;
```

For deletes, call `delete_table_and_mv_relationships` so MV target lookups are cleaned atomically.

- [ ] **Step 4: Migrate managed-lake DDL writes**

In `src/connector/starrocks/managed/ddl.rs`, replace snapshot mutation plus `replace_managed_snapshot` writes with repository calls inside one provider write transaction:

```rust
let provider = state
    .metadata_provider
    .as_ref()
    .ok_or_else(|| "managed lake metadata provider is not configured".to_string())?;
let mut txn = provider.begin_write("create managed table").map_err(|err| err.to_string())?;
let db = state
    .managed_repo
    .create_database(&mut *txn, CreateManagedDatabaseRequest { name: db_name.to_string() })
    .map_err(|err| err.to_string())?;
let table = state
    .managed_repo
    .create_table(&mut *txn, create_table_request)
    .map_err(|err| err.to_string())?;
txn.commit().map_err(|err| err.to_string())?;
```

Keep storage and runtime catalog registration outside provider code, after metadata commit succeeds.

- [ ] **Step 5: Migrate managed-lake publish txn flow**

In `src/connector/starrocks/managed/txn.rs`, replace:

```rust
metadata_store.prepare_txn(...)
metadata_store.mark_txn_written(...)
metadata_store.mark_txn_visible(...)
metadata_store.mark_txn_aborted(...)
```

with `ManagedLakeTxnRepository` operations. Keep file write and publish side effects unchanged. The metadata transitions should use one write transaction per transition, matching current side-effect boundaries.

- [ ] **Step 6: Migrate erase job worker**

In `src/connector/starrocks/managed/erase.rs`, replace list/claim/finish/fail calls on `SqliteMetadataStore` with `JobMetaRepository`. Implement repository scan helpers if needed:

```rust
pub fn list_runnable_erase_jobs(
    &self,
    txn: &dyn MetaReadTxn,
    now_ms: i64,
) -> RepositoryResult<Vec<StoredEraseJob>>
```

Filter `Pending` and retryable `Failed` jobs in repository code, not in the worker.

- [ ] **Step 7: Migrate MV DDL and refresh metadata**

In `mv_ddl.rs` and `mv_refresh.rs`, replace `materialized_views` table reads/writes with `MvMetaRepository`:

```rust
let mv = state
    .mv_repo
    .find_by_target(&*read_txn, catalog, namespace, table)
    .map_err(|err| err.to_string())?;
```

Use `begin_refresh_intent`, `record_external_commit_outcome`, and `finalize_refresh` for refresh state instead of `begin_mv_refresh` and `update_mv_iceberg_refresh_metadata`.

- [ ] **Step 8: Remove or shrink `SqliteMetadataStore`**

After all call sites compile without domain SQL APIs, remove `SqliteMetadataStore` exports from `src/connector/mod.rs`. If a small compatibility wrapper remains for a single test, move that test to repository assertions and delete the wrapper.

- [ ] **Step 9: Run focused Rust tests**

Run:

```bash
cargo test --test meta_repository
cargo test connector::starrocks::managed::store
cargo test connector::starrocks::managed::txn
cargo test connector::starrocks::managed::mv_refresh
cargo test engine::statement
```

Expected: all pass. If `managed::store` no longer exists as a test target after deletion, replace that command with the new repository and managed module test names printed by `cargo test --all-targets -- --list | rg 'managed|meta_repository'`.

- [ ] **Step 10: Commit**

```bash
git add src tests
git commit -m "feat(meta): migrate standalone metadata flows to repositories"
```

## Task 9: Refresh Transaction Framework and Iceberg MV Relocation

**Files:**
- Create: `src/engine/mv/mod.rs`
- Create: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/connector/starrocks/managed/mod.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh_iceberg.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Test: `tests/meta_framework_flow.rs`

- [ ] **Step 1: Add flow-level refresh intent test**

Create `tests/meta_framework_flow.rs`:

```rust
use std::collections::BTreeMap;

use novarocks::meta::repository::mv::{
    CreateMvDefinitionRequest, MvMetaRepository, MvRefreshFinalizeRequest,
    RefreshExternalOutcome,
};
use novarocks::meta::{MetaStoreProvider, SqliteMetaStoreProvider};

#[test]
fn refresh_transaction_can_recover_after_external_commit_before_finalize()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repo = MvMetaRepository::default();

    let mut txn = provider.begin_write("create mv and intent")?;
    let mv = repo.create_definition(
        &mut *txn,
        CreateMvDefinitionRequest {
            select_sql: "select id from ice.ns.orders".to_string(),
            base_table_refs: vec!["ice.ns.orders".to_string()],
            primary_key_columns: vec!["id".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("ns".to_string()),
            target_table: Some("orders_mv".to_string()),
            created_at_ms: 1,
        },
    )?;
    let intent = repo.begin_refresh_intent(&mut *txn, mv.mv_id, BTreeMap::new())?;
    txn.commit()?;

    let mut txn = provider.begin_write("record external only")?;
    repo.record_external_commit_outcome(
        &mut *txn,
        intent.refresh_id,
        RefreshExternalOutcome {
            target_snapshot_id: Some(42),
            commit_id: "snapshot-42".to_string(),
        },
    )?;
    txn.commit()?;

    let mut txn = provider.begin_write("recover finalize")?;
    repo.finalize_refresh(
        &mut *txn,
        MvRefreshFinalizeRequest {
            refresh_id: intent.refresh_id,
            rows: 10,
            base_snapshots: BTreeMap::new(),
            base_table_uuids: BTreeMap::new(),
            target_snapshot_id: Some(42),
        },
    )?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let mv_after = repo.load_by_id(&*read, mv.mv_id)?.expect("mv");
    assert_eq!(mv_after.last_refreshed_iceberg_snapshot_id, Some(42));
    Ok(())
}
```

- [ ] **Step 2: Run flow test**

Run:

```bash
cargo test --test meta_framework_flow refresh_transaction_can_recover_after_external_commit_before_finalize
```

Expected: pass after Task 2; if it fails, fix `MvMetaRepository::finalize_refresh` idempotency before moving orchestration.

- [ ] **Step 3: Create neutral MV module**

Create `src/engine/mv/mod.rs`:

```rust
pub(crate) mod iceberg_refresh;
```

Add to `src/engine/mod.rs` module declarations:

```rust
pub(crate) mod mv;
```

- [ ] **Step 4: Move Iceberg refresh orchestration**

Move orchestration functions from `src/connector/starrocks/managed/mv_refresh_iceberg.rs` into `src/engine/mv/iceberg_refresh.rs`. Keep public function names stable at first, and update imports so the new module depends on:

```rust
use crate::meta::repository::iceberg_catalog::IcebergCatalogMetaRepository;
use crate::meta::repository::mv::MvMetaRepository;
```

The old `mv_refresh_iceberg.rs` should either be deleted or reduced to:

```rust
pub(crate) use crate::engine::mv::iceberg_refresh::*;
```

Use the re-export only as an intermediate step; remove it before final cleanup if no managed module depends on it.

- [ ] **Step 5: Wire refresh intent into orchestration**

At the start of Iceberg MV refresh, create an intent:

```rust
let mut meta_txn = provider.begin_write("begin iceberg mv refresh").map_err(|err| err.to_string())?;
let intent = state
    .mv_repo
    .begin_refresh_intent(&mut *meta_txn, mv_row.mv_id, target_snapshots.clone())
    .map_err(|err| err.to_string())?;
meta_txn.commit().map_err(|err| err.to_string())?;
```

After external Iceberg commit succeeds, record outcome and finalize in a new metadata transaction:

```rust
let mut meta_txn = provider
    .begin_write("finalize iceberg mv refresh")
    .map_err(|err| err.to_string())?;
state
    .mv_repo
    .record_external_commit_outcome(&mut *meta_txn, intent.refresh_id, outcome)
    .map_err(|err| err.to_string())?;
state
    .mv_repo
    .finalize_refresh(&mut *meta_txn, finalize_request)
    .map_err(|err| err.to_string())?;
meta_txn.commit().map_err(|err| err.to_string())?;
```

- [ ] **Step 6: Run MV refresh tests**

Run:

```bash
cargo test connector::starrocks::managed::mv_refresh
cargo test connector::starrocks::managed::mv_refresh_iceberg
cargo test --test meta_framework_flow
```

Expected: tests pass, or old `mv_refresh_iceberg` test path is replaced by the new `engine::mv::iceberg_refresh` test path.

- [ ] **Step 7: Commit**

```bash
git add src/engine src/connector/starrocks/managed tests/meta_framework_flow.rs
git commit -m "feat(meta): add refresh intents and relocate iceberg mv orchestration"
```

## Task 10: Remove Legacy Store Schema and Direct SQLite Assertions

**Files:**
- Modify: `src/connector/starrocks/managed/store.rs`
- Modify: `tests/standalone_mysql_server.rs`
- Modify: `src/connector/mod.rs`
- Test: `tests/standalone_mysql_server.rs`

- [ ] **Step 1: Search for remaining legacy store usage**

Run:

```bash
rg -n "SqliteMetadataStore|metadata_db_path|materialized_views|global_meta|iceberg_catalogs|iceberg_namespaces|iceberg_tables|erase_jobs|txns" src tests docker tests/sql-test-runner
```

Expected: remaining matches are either provider-generic `meta_*` code, test fixtures to update, or comments in old tests.

- [ ] **Step 2: Replace direct SQLite test assertions**

In `tests/standalone_mysql_server.rs`, replace direct `rusqlite::Connection` queries against old tables with repository reads:

```rust
let provider = novarocks::meta::SqliteMetaStoreProvider::open(&metadata_db_path)
    .expect("open metadata provider");
let read = provider.begin_read().expect("begin metadata read");
let mv_repo = novarocks::meta::repository::mv::MvMetaRepository::default();
let mv = mv_repo
    .find_by_target(&*read, "ice", "ns", "mv_orders")
    .expect("load mv")
    .expect("mv metadata");
assert_eq!(mv.target_table.as_deref(), Some("mv_orders"));
```

- [ ] **Step 3: Delete domain-specific schema creation**

Remove old domain-specific `CREATE TABLE` statements from `src/connector/starrocks/managed/store.rs`. If the file has no remaining production responsibility, delete the file and remove `pub(crate) mod store;` from `src/connector/starrocks/managed/mod.rs`.

- [ ] **Step 4: Run no-legacy search again**

Run:

```bash
rg -n "SqliteMetadataStore|metadata_db_path|global_meta|materialized_views|iceberg_catalogs|iceberg_namespaces|iceberg_tables" src tests docker tests/sql-test-runner
```

Expected: no production references remain. Test references may remain only when asserting those strings are absent or checking old code deletion is complete; prefer no references.

- [ ] **Step 5: Run focused standalone tests**

Run:

```bash
cargo test --test standalone_mysql_server standalone_mysql_server_does_not_restore_external_preloaded_parquet_tables_from_sqlite_config
cargo test --test meta_repository
cargo test --test meta_framework_flow
```

Expected: tests pass after updating names if removed tests no longer apply.

- [ ] **Step 6: Commit**

```bash
git add src tests docker tests/sql-test-runner
git commit -m "refactor(meta): remove legacy standalone metadata store"
```

## Task 11: SQL Regression and End-to-End Verification

**Files:**
- Modify: SQL result files only if repository migration intentionally changes output ordering or wording.
- Test: SQL test runner and focused Cargo tests.

- [ ] **Step 1: Format code**

Run:

```bash
cargo fmt
```

Expected: command exits 0.

- [ ] **Step 2: Run provider and repository tests**

Run:

```bash
cargo test --test meta_sqlite_provider
cargo test --test meta_repository
cargo test --test meta_framework_flow
```

Expected: all tests pass.

- [ ] **Step 3: Run focused module tests**

Run:

```bash
cargo test connector::starrocks::managed
cargo test engine::statement
cargo test engine::information_schema
cargo test common::app_config
```

Expected: all tests pass.

- [ ] **Step 4: Build debug binary**

Run:

```bash
cargo build
```

Expected: build succeeds.

- [ ] **Step 5: Prepare isolated Iceberg REST environment when SQL verification is needed**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh || docker/iceberg-rest/up.sh --prepare-only
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
```

Expected: `NOVA_ENV_MYSQL_PORT`, `NOVAROCKS_STANDALONE_CONFIG`, and `NOVAROCKS_SQL_TEST_CONFIG` are set. Do not use port `9030`.

- [ ] **Step 6: Start a self-owned standalone server**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
LOG=/tmp/novarocks-unified-meta-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    tail -40 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG"
```

Expected: log contains `NOVAROCKS_READY mysql_port=<generated-port> pid=<pid>`.

- [ ] **Step 7: Run targeted SQL suites**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Expected: suite passes. If a narrower suite exists for MV-on-Iceberg, run it as well with `--suite <suite> --mode verify`.

- [ ] **Step 8: Stop self-owned server**

Run:

```bash
kill "$SRV_PID"
wait "$SRV_PID" 2>/dev/null || true
```

Expected: server process exits.

- [ ] **Step 9: Final cleanup search**

Run:

```bash
rg -n "SqliteMetadataStore|metadata_db_path|global_meta|materialized_views|iceberg_catalogs|iceberg_namespaces|iceberg_tables" src tests docker tests/sql-test-runner
```

Expected: no production references remain. Any remaining test/documentation reference must describe the removed legacy path, not use it.

- [ ] **Step 10: Commit verification-related fixture updates**

```bash
git add .
git commit -m "test(meta): verify unified metadata framework"
```

## Self-Review Checklist

- Spec coverage: Tasks cover provider primitives, ID scopes, MV repository, Iceberg catalog repository, managed-lake object repository, managed-lake txn repository, job repository, bootstrap, refresh transaction framework, Iceberg MV relocation, legacy store removal, and verification.
- Placeholder scan: This plan contains no unresolved marker text. Every code-changing task includes concrete code or an exact replacement pattern.
- Type consistency: Repository names are consistent across tasks: `ManagedLakeMetaRepository`, `ManagedLakeTxnRepository`, `MvMetaRepository`, `IcebergCatalogMetaRepository`, and `JobMetaRepository`.
- Execution risk: Task 8 is the largest integration task. If it becomes too large during execution, split it at the existing commit boundary into managed-lake DDL, managed txn, MV metadata, and catalog registration subtasks while preserving the same final repository interfaces.
