# SQLite Avro Metadata Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace JSON metadata payloads with Avro binary payloads backed by a static schema catalog, fail fast on legacy JSON metadata stores, and enforce `FULL_TRANSITIVE` schema compatibility for future metadata evolution.

**Architecture:** Keep SQLite as the stable `meta_records` KV store and move business schema evolution into `src/meta/avro`. Repository modules encode/decode typed domain structs through a shared Avro payload helper instead of local JSON/version checks. `meta_provider_schema` records `store_format=avro` and `metadata_epoch=1`; unsupported legacy/non-Avro stores fail during provider open or record read.

**Tech Stack:** Rust 2024, `apache-avro = 0.21.0`, `rusqlite`, existing `src/meta` provider/repository framework, integration tests under `tests/`.

---

## File Structure

- Modify `src/meta/record.rs`: rename payload version semantics to schema id/fingerprint and make `avro` the supported encoding.
- Modify `src/meta/sqlite/schema.rs`: initialize provider markers and create the Avro payload columns.
- Modify `src/meta/sqlite/mod.rs`: keep provider open flow but rely on stricter schema initialization.
- Modify `src/meta/sqlite/txn.rs`: read/write `payload_schema_id` and `payload_schema_fingerprint`.
- Create `src/meta/avro/mod.rs`: module exports.
- Create `src/meta/avro/catalog.rs`: static schema entries, fingerprint validation, latest/writer lookup, and compatibility checks.
- Create `src/meta/avro/codec.rs`: Avro datum encode/decode helpers.
- Create `src/meta/avro/schemas/**/0001.avsc`: initial Avro schemas for all current repository record kinds.
- Modify `src/meta/mod.rs`: export the Avro module only through repository helpers.
- Modify `src/meta/payload.rs`: replace JSON helpers with Avro helpers or leave JSON only for tests removed by this plan.
- Modify `src/meta/repository/mod.rs`: expose shared `encode_record_payload` / `decode_record_payload`.
- Modify `src/meta/repository/{iceberg_catalog,job,managed_lake,managed_txn,mv}.rs`: remove local schema-version checks and use shared Avro helpers.
- Modify `tests/common/meta_provider_conformance.rs`: use opaque Avro payloads for provider-level tests.
- Modify `tests/meta_sqlite_provider.rs`: add provider marker and legacy-store rejection coverage.
- Modify `tests/meta_repository.rs`: switch payload helper tests to Avro and adjust mismatch tests to schema-id/fingerprint failures.
- Create `tests/meta_avro_catalog.rs`: catalog/fingerprint/compatibility/codec tests.

---

### Task 1: Provider Payload Shape and SQLite Store Markers

**Files:**
- Modify: `src/meta/record.rs`
- Modify: `src/meta/sqlite/schema.rs`
- Modify: `src/meta/sqlite/txn.rs`
- Modify: `tests/common/meta_provider_conformance.rs`
- Modify: `tests/meta_sqlite_provider.rs`

- [ ] **Step 1: Write failing provider tests for Avro payload metadata and legacy rejection**

Add these imports to `tests/meta_sqlite_provider.rs`:

```rust
use novarocks::meta::MetaStoreProvider;
use rusqlite::Connection;
```

Add these tests to `tests/meta_sqlite_provider.rs`:

```rust
#[test]
fn sqlite_provider_initializes_avro_store_markers() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    let provider = SqliteMetaStoreProvider::open(&path)?;
    drop(provider);

    let conn = Connection::open(&path)?;
    let store_format: String = conn.query_row(
        "SELECT CAST(value AS TEXT) FROM meta_provider_schema WHERE key = 'store_format'",
        [],
        |row| row.get(0),
    )?;
    let metadata_epoch: String = conn.query_row(
        "SELECT CAST(value AS TEXT) FROM meta_provider_schema WHERE key = 'metadata_epoch'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(store_format, "avro");
    assert_eq!(metadata_epoch, "1");
    Ok(())
}

#[test]
fn sqlite_provider_rejects_nonempty_legacy_json_store() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE meta_records (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                kind TEXT NOT NULL,
                revision INTEGER NOT NULL,
                payload_encoding TEXT NOT NULL,
                payload_schema_version INTEGER NOT NULL,
                payload BLOB NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, key)
            );
            CREATE TABLE meta_id_scopes (
                scope TEXT PRIMARY KEY,
                next_id INTEGER NOT NULL
            );
            INSERT INTO meta_records(
                namespace, key, kind, revision, payload_encoding,
                payload_schema_version, payload, created_at_ms, updated_at_ms
            )
            VALUES('mv', 'by-id/1', 'mv.definition', 1, 'json', 1, X'7B7D', 1, 1);
            "#,
        )?;
    }

    let err = SqliteMetaStoreProvider::open(&path).expect_err("legacy JSON store must fail");
    assert_eq!(err.kind(), novarocks::meta::MetaErrorKind::ProviderCorruption);
    assert!(
        err.to_string().contains("legacy or unsupported metadata store"),
        "{err}"
    );
    Ok(())
}

#[test]
fn sqlite_provider_rejects_unknown_metadata_epoch() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE meta_provider_schema (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            INSERT INTO meta_provider_schema(key, value)
            VALUES('store_format', 'avro'), ('metadata_epoch', '999');
            CREATE TABLE meta_records (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                kind TEXT NOT NULL,
                revision INTEGER NOT NULL,
                payload_encoding TEXT NOT NULL,
                payload_schema_id INTEGER NOT NULL,
                payload_schema_fingerprint TEXT NOT NULL,
                payload BLOB NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, key)
            );
            CREATE TABLE meta_id_scopes (
                scope TEXT PRIMARY KEY,
                next_id INTEGER NOT NULL
            );
            "#,
        )?;
    }

    let err = SqliteMetaStoreProvider::open(&path).expect_err("future epoch must fail");
    assert_eq!(err.kind(), novarocks::meta::MetaErrorKind::ProviderCorruption);
    assert!(err.to_string().contains("metadata epoch 999 is newer than supported epoch 1"));
    Ok(())
}
```

- [ ] **Step 2: Run the failing provider tests**

Run:

```bash
cargo test --test meta_sqlite_provider sqlite_provider_initializes_avro_store_markers -- --exact
cargo test --test meta_sqlite_provider sqlite_provider_rejects_nonempty_legacy_json_store -- --exact
cargo test --test meta_sqlite_provider sqlite_provider_rejects_unknown_metadata_epoch -- --exact
```

Expected: the first test fails because markers are absent; the second and third fail because `open()` currently accepts the stores.

- [ ] **Step 3: Update `MetaPayload` to carry Avro schema identity**

Replace the payload structs and encoding enum in `src/meta/record.rs` with:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaPayload {
    pub encoding: MetaPayloadEncoding,
    pub schema_id: i32,
    pub schema_fingerprint: String,
    pub bytes: Bytes,
}

impl MetaPayload {
    pub fn avro(
        schema_id: i32,
        schema_fingerprint: impl Into<String>,
        bytes: Bytes,
    ) -> Self {
        Self {
            encoding: MetaPayloadEncoding::Avro,
            schema_id,
            schema_fingerprint: schema_fingerprint.into(),
            bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetaPayloadEncoding {
    Avro,
}

impl MetaPayloadEncoding {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Avro => "avro",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, MetaError> {
        match value {
            "avro" => Ok(Self::Avro),
            _ => Err(MetaError::new(
                MetaErrorKind::ProviderCorruption,
                format!("unsupported metadata payload encoding `{value}`"),
            )),
        }
    }
}
```

- [ ] **Step 4: Update SQLite schema initialization and marker validation**

Replace `src/meta/sqlite/schema.rs` with this structure:

```rust
use rusqlite::{Connection, OptionalExtension, params};

use crate::meta::{MetaError, MetaErrorKind};

const STORE_FORMAT_KEY: &str = "store_format";
const STORE_FORMAT_AVRO: &str = "avro";
const METADATA_EPOCH_KEY: &str = "metadata_epoch";
const SUPPORTED_METADATA_EPOCH: i64 = 1;

pub(super) fn init_schema(conn: &Connection) -> Result<(), MetaError> {
    let has_provider_schema = table_exists(conn, "meta_provider_schema")?;
    let has_records = table_exists(conn, "meta_records")?;

    if !has_provider_schema && has_records && legacy_record_count(conn)? > 0 {
        return Err(MetaError::new(
            MetaErrorKind::ProviderCorruption,
            "legacy or unsupported metadata store is non-empty; JSON metadata is not migrated",
        ));
    }

    if !has_provider_schema && has_records && legacy_record_count(conn)? == 0 {
        conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS meta_records;
            DROP TABLE IF EXISTS meta_id_scopes;
            "#,
        )
        .map_err(super::txn::sqlite_error)?;
    }

    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS meta_provider_schema (
            key TEXT PRIMARY KEY,
            value BLOB NOT NULL
        );

        CREATE TABLE IF NOT EXISTS meta_records (
            namespace TEXT NOT NULL,
            key TEXT NOT NULL,
            kind TEXT NOT NULL,
            revision INTEGER NOT NULL,
            payload_encoding TEXT NOT NULL,
            payload_schema_id INTEGER NOT NULL,
            payload_schema_fingerprint TEXT NOT NULL,
            payload BLOB NOT NULL,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            PRIMARY KEY(namespace, key)
        );

        CREATE TABLE IF NOT EXISTS meta_id_scopes (
            scope TEXT PRIMARY KEY,
            next_id INTEGER NOT NULL
        );
        "#,
    )
    .map_err(super::txn::sqlite_error)?;

    ensure_store_marker(conn)?;
    ensure_supported_epoch(conn)?;
    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool, MetaError> {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
        params![table],
        |_| Ok(()),
    )
    .optional()
    .map(|row| row.is_some())
    .map_err(super::txn::sqlite_error)
}

fn legacy_record_count(conn: &Connection) -> Result<i64, MetaError> {
    conn.query_row("SELECT COUNT(*) FROM meta_records", [], |row| row.get(0))
        .optional()
        .map(|count| count.unwrap_or(0))
        .map_err(super::txn::sqlite_error)
}

fn ensure_store_marker(conn: &Connection) -> Result<(), MetaError> {
    let value: Option<String> = conn
        .query_row(
            "SELECT CAST(value AS TEXT) FROM meta_provider_schema WHERE key = ?1",
            params![STORE_FORMAT_KEY],
            |row| row.get(0),
        )
        .optional()
        .map_err(super::txn::sqlite_error)?;
    match value.as_deref() {
        Some(STORE_FORMAT_AVRO) => Ok(()),
        Some(other) => Err(MetaError::new(
            MetaErrorKind::ProviderCorruption,
            format!("unsupported metadata store format `{other}`"),
        )),
        None => {
            let records: i64 = conn
                .query_row("SELECT COUNT(*) FROM meta_records", [], |row| row.get(0))
                .map_err(super::txn::sqlite_error)?;
            if records != 0 {
                return Err(MetaError::new(
                    MetaErrorKind::ProviderCorruption,
                    "legacy or unsupported metadata store is non-empty; missing store_format marker",
                ));
            }
            conn.execute(
                "INSERT INTO meta_provider_schema(key, value) VALUES(?1, ?2)",
                params![STORE_FORMAT_KEY, STORE_FORMAT_AVRO],
            )
            .map_err(super::txn::sqlite_error)?;
            Ok(())
        }
    }
}

fn ensure_supported_epoch(conn: &Connection) -> Result<(), MetaError> {
    let value: Option<String> = conn
        .query_row(
            "SELECT CAST(value AS TEXT) FROM meta_provider_schema WHERE key = ?1",
            params![METADATA_EPOCH_KEY],
            |row| row.get(0),
        )
        .optional()
        .map_err(super::txn::sqlite_error)?;
    match value {
        Some(value) => {
            let epoch = value.parse::<i64>().map_err(|err| {
                MetaError::new(
                    MetaErrorKind::ProviderCorruption,
                    format!("metadata epoch `{value}` is not an integer: {err}"),
                )
            })?;
            if epoch > SUPPORTED_METADATA_EPOCH {
                return Err(MetaError::new(
                    MetaErrorKind::ProviderCorruption,
                    format!(
                        "metadata epoch {epoch} is newer than supported epoch {SUPPORTED_METADATA_EPOCH}"
                    ),
                ));
            }
            Ok(())
        }
        None => {
            conn.execute(
                "INSERT INTO meta_provider_schema(key, value) VALUES(?1, ?2)",
                params![METADATA_EPOCH_KEY, SUPPORTED_METADATA_EPOCH.to_string()],
            )
            .map_err(super::txn::sqlite_error)?;
            Ok(())
        }
    }
}
```

- [ ] **Step 5: Update SQLite transaction read/write SQL**

In `src/meta/sqlite/txn.rs`, replace every selected/written `payload_schema_version` column with `payload_schema_id, payload_schema_fingerprint`.

Use this record construction pattern in `get_record` and `row_to_record`:

```rust
let schema_id = row.get::<_, i32>(3)?;
let schema_fingerprint = row.get::<_, String>(4)?;
let payload = Bytes::from(row.get::<_, Vec<u8>>(5)?);
Ok(MetaRecord {
    key: key.clone(),
    kind,
    revision,
    payload: MetaPayload {
        encoding,
        schema_id,
        schema_fingerprint,
        bytes: payload,
    },
    created_at_ms: row.get(6)?,
    updated_at_ms: row.get(7)?,
})
```

Use this insert column list:

```sql
INSERT INTO meta_records(
    namespace, key, kind, revision, payload_encoding, payload_schema_id,
    payload_schema_fingerprint, payload, created_at_ms, updated_at_ms
)
VALUES (?1, ?2, ?3, 1, ?4, ?5, ?6, ?7, ?8, ?8)
```

Use this update assignment block in both update functions:

```sql
SET kind = ?1,
    revision = revision + 1,
    payload_encoding = ?2,
    payload_schema_id = ?3,
    payload_schema_fingerprint = ?4,
    payload = ?5,
    updated_at_ms = ?6
```

- [ ] **Step 6: Update provider conformance payloads**

In `tests/common/meta_provider_conformance.rs`, replace `MetaPayload::json(1, Bytes::from_static(...))` with this helper:

```rust
fn test_payload(bytes: &'static [u8]) -> MetaPayload {
    MetaPayload::avro(1, "0000000000000000", Bytes::from_static(bytes))
}
```

Then use:

```rust
let payload = test_payload(b"payload-mv1");
let updated_payload = test_payload(b"payload-mv2");
```

Provider conformance tests only validate storage semantics, so the bytes do not need to be valid Avro datum payloads.

- [ ] **Step 7: Run provider tests**

Run:

```bash
cargo test --test meta_sqlite_provider
```

Expected: PASS.

- [ ] **Step 8: Commit provider payload/storage shell**

```bash
git add src/meta/record.rs src/meta/sqlite/schema.rs src/meta/sqlite/txn.rs tests/common/meta_provider_conformance.rs tests/meta_sqlite_provider.rs
git commit -m "refactor(meta): add Avro payload identity to SQLite provider"
```

---

### Task 2: Avro Static Catalog and Codec

**Files:**
- Create: `src/meta/avro/mod.rs`
- Create: `src/meta/avro/catalog.rs`
- Create: `src/meta/avro/codec.rs`
- Create: `src/meta/avro/schemas/test.evolution/0001.avsc`
- Create: `src/meta/avro/schemas/test.evolution/0002.avsc`
- Modify: `src/meta/mod.rs`
- Create: `tests/meta_avro_catalog.rs`

- [ ] **Step 1: Write failing Avro catalog and codec tests**

Create `tests/meta_avro_catalog.rs`:

```rust
use std::collections::BTreeMap;

use novarocks::meta::avro::{decode_payload, encode_payload, schema_catalog};
use serde::{Deserialize, Serialize};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TestEvolutionV1 {
    id: i64,
    name: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TestEvolutionV2 {
    id: i64,
    name: String,
    tags: Vec<String>,
}

#[test]
fn avro_catalog_has_unique_subject_ids_and_fingerprints() -> TestResult {
    let catalog = schema_catalog()?;
    catalog.validate_unique_entries()?;
    let latest = catalog.latest("test.evolution")?;
    assert_eq!(latest.subject(), "test.evolution");
    assert_eq!(latest.id(), 2);
    assert_eq!(latest.fingerprint().len(), 16);
    Ok(())
}

#[test]
fn avro_catalog_enforces_full_transitive_compatibility() -> TestResult {
    schema_catalog()?.validate_full_transitive()?;
    Ok(())
}

#[test]
fn avro_codec_round_trips_latest_schema() -> TestResult {
    let payload = encode_payload(
        "test.evolution",
        &TestEvolutionV2 {
            id: 7,
            name: "mv".to_string(),
            tags: vec!["fast".to_string(), "safe".to_string()],
        },
    )?;
    let decoded: TestEvolutionV2 = decode_payload("test.evolution", &payload)?;
    assert_eq!(
        decoded,
        TestEvolutionV2 {
            id: 7,
            name: "mv".to_string(),
            tags: vec!["fast".to_string(), "safe".to_string()],
        }
    );
    Ok(())
}

#[test]
fn avro_codec_reads_older_writer_schema_with_latest_reader_defaults() -> TestResult {
    let catalog = schema_catalog()?;
    let writer = catalog.entry("test.evolution", 1)?;
    let payload = novarocks::meta::avro::encode_payload_with_schema(
        writer,
        &TestEvolutionV1 {
            id: 9,
            name: "old".to_string(),
        },
    )?;

    let decoded: TestEvolutionV2 = decode_payload("test.evolution", &payload)?;
    assert_eq!(
        decoded,
        TestEvolutionV2 {
            id: 9,
            name: "old".to_string(),
            tags: Vec::new(),
        }
    );
    Ok(())
}

#[test]
fn avro_codec_rejects_fingerprint_mismatch() -> TestResult {
    let mut payload = encode_payload(
        "test.evolution",
        &TestEvolutionV2 {
            id: 1,
            name: "bad".to_string(),
            tags: Vec::new(),
        },
    )?;
    payload.schema_fingerprint = "ffffffffffffffff".to_string();

    let err = decode_payload::<TestEvolutionV2>("test.evolution", &payload)
        .expect_err("fingerprint mismatch must fail");
    assert!(err.to_string().contains("fingerprint mismatch"), "{err}");
    Ok(())
}
```

- [ ] **Step 2: Create test evolution schemas**

Create `src/meta/avro/schemas/test.evolution/0001.avsc`:

```json
{
  "type": "record",
  "name": "TestEvolution",
  "namespace": "novarocks.meta.test",
  "fields": [
    { "name": "id", "type": "long" },
    { "name": "name", "type": "string" }
  ]
}
```

Create `src/meta/avro/schemas/test.evolution/0002.avsc`:

```json
{
  "type": "record",
  "name": "TestEvolution",
  "namespace": "novarocks.meta.test",
  "fields": [
    { "name": "id", "type": "long" },
    { "name": "name", "type": "string" },
    { "name": "tags", "type": { "type": "array", "items": "string" }, "default": [] }
  ]
}
```

- [ ] **Step 3: Run the failing Avro tests**

Run:

```bash
cargo test --test meta_avro_catalog
```

Expected: FAIL because `novarocks::meta::avro` does not exist.

- [ ] **Step 4: Add Avro module exports**

Create `src/meta/avro/mod.rs`:

```rust
mod catalog;
mod codec;

pub use catalog::{AvroSchemaCatalog, AvroSchemaEntry, schema_catalog};
pub use codec::{decode_payload, encode_payload, encode_payload_with_schema};
```

Add this line to `src/meta/mod.rs`:

```rust
pub mod avro;
```

- [ ] **Step 5: Implement static schema catalog**

Create `src/meta/avro/catalog.rs`:

```rust
use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use apache_avro::rabin::Rabin;
use apache_avro::{Schema, SchemaCompatibility};

use crate::meta::repository::{RepositoryError, RepositoryResult};

#[derive(Debug)]
pub struct AvroSchemaEntry {
    subject: &'static str,
    id: i32,
    raw_schema: &'static str,
    schema: Schema,
    fingerprint: String,
}

impl AvroSchemaEntry {
    pub fn subject(&self) -> &'static str {
        self.subject
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

#[derive(Debug)]
pub struct AvroSchemaCatalog {
    entries: BTreeMap<(&'static str, i32), AvroSchemaEntry>,
}

impl AvroSchemaCatalog {
    fn new() -> RepositoryResult<Self> {
        let mut entries = BTreeMap::new();
        for (subject, id, raw_schema) in schema_sources() {
            let schema = Schema::parse_str(raw_schema).map_err(|err| {
                RepositoryError::invalid(format!(
                    "failed to parse Avro schema {subject}/{id:04}: {err}"
                ))
            })?;
            let fingerprint = schema.fingerprint::<Rabin>().to_string();
            let previous = entries.insert(
                (subject, id),
                AvroSchemaEntry {
                    subject,
                    id,
                    raw_schema,
                    schema,
                    fingerprint,
                },
            );
            if previous.is_some() {
                return Err(RepositoryError::invalid(format!(
                    "duplicate Avro schema entry {subject}/{id:04}"
                )));
            }
        }
        let catalog = Self { entries };
        catalog.validate_unique_entries()?;
        catalog.validate_full_transitive()?;
        Ok(catalog)
    }

    pub fn latest(&self, subject: &str) -> RepositoryResult<&AvroSchemaEntry> {
        self.entries
            .iter()
            .filter(|((entry_subject, _), _)| *entry_subject == subject)
            .map(|(_, entry)| entry)
            .max_by_key(|entry| entry.id)
            .ok_or_else(|| RepositoryError::invalid(format!("unknown Avro subject `{subject}`")))
    }

    pub fn entry(&self, subject: &str, id: i32) -> RepositoryResult<&AvroSchemaEntry> {
        self.entries
            .get(&(subject, id))
            .ok_or_else(|| RepositoryError::invalid(format!("unknown Avro schema `{subject}` id {id}")))
    }

    pub fn validate_unique_entries(&self) -> RepositoryResult<()> {
        let mut fingerprints = BTreeSet::new();
        for entry in self.entries.values() {
            let key = (entry.subject, entry.id);
            if !fingerprints.insert((entry.subject, entry.fingerprint.clone())) {
                return Err(RepositoryError::invalid(format!(
                    "duplicate Avro fingerprint for subject `{}`: {}",
                    entry.subject, entry.fingerprint
                )));
            }
            if entry.fingerprint.len() != 16 {
                return Err(RepositoryError::invalid(format!(
                    "Avro schema {}/{} has non-Rabin fingerprint `{}`",
                    key.0, key.1, entry.fingerprint
                )));
            }
        }
        Ok(())
    }

    pub fn validate_full_transitive(&self) -> RepositoryResult<()> {
        let mut by_subject: BTreeMap<&str, Vec<&AvroSchemaEntry>> = BTreeMap::new();
        for entry in self.entries.values() {
            by_subject.entry(entry.subject).or_default().push(entry);
        }
        for (subject, entries) in by_subject {
            for writer in &entries {
                for reader in &entries {
                    SchemaCompatibility::can_read(writer.schema(), reader.schema()).map_err(|err| {
                        RepositoryError::invalid(format!(
                            "Avro subject `{subject}` is not FULL_TRANSITIVE: reader {} cannot read writer {}: {err}",
                            reader.id(),
                            writer.id()
                        ))
                    })?;
                }
            }
        }
        Ok(())
    }
}

pub fn schema_catalog() -> RepositoryResult<&'static AvroSchemaCatalog> {
    static CATALOG: LazyLock<RepositoryResult<AvroSchemaCatalog>> =
        LazyLock::new(AvroSchemaCatalog::new);
    match &*CATALOG {
        Ok(catalog) => Ok(catalog),
        Err(err) => Err(RepositoryError::invalid(err.to_string())),
    }
}

fn schema_sources() -> Vec<(&'static str, i32, &'static str)> {
    vec![
        (
            "test.evolution",
            1,
            include_str!("schemas/test.evolution/0001.avsc"),
        ),
        (
            "test.evolution",
            2,
            include_str!("schemas/test.evolution/0002.avsc"),
        ),
    ]
}
```

- [ ] **Step 6: Implement Avro datum codec**

Create `src/meta/avro/codec.rs`:

```rust
use std::io::Cursor;

use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::meta::avro::{AvroSchemaEntry, schema_catalog};
use crate::meta::repository::{RepositoryError, RepositoryResult};
use crate::meta::{MetaPayload, MetaPayloadEncoding};

pub fn encode_payload<T>(subject: &str, value: &T) -> RepositoryResult<MetaPayload>
where
    T: Serialize,
{
    let entry = schema_catalog()?.latest(subject)?;
    encode_payload_with_schema(entry, value)
}

pub fn encode_payload_with_schema<T>(
    entry: &AvroSchemaEntry,
    value: &T,
) -> RepositoryResult<MetaPayload>
where
    T: Serialize,
{
    let value = to_value(value)
        .map_err(|err| RepositoryError::invalid(format!("failed to convert `{}` to Avro value: {err}", entry.subject())))?;
    let bytes = to_avro_datum(entry.schema(), value)
        .map_err(|err| RepositoryError::invalid(format!("failed to encode `{}` Avro payload: {err}", entry.subject())))?;
    Ok(MetaPayload::avro(
        entry.id(),
        entry.fingerprint().to_string(),
        Bytes::from(bytes),
    ))
}

pub fn decode_payload<T>(subject: &str, payload: &MetaPayload) -> RepositoryResult<T>
where
    T: DeserializeOwned,
{
    if payload.encoding != MetaPayloadEncoding::Avro {
        return Err(RepositoryError::provider(format!(
            "metadata subject `{subject}` expected Avro payload, got {:?}",
            payload.encoding
        )));
    }
    let catalog = schema_catalog()?;
    let writer = catalog.entry(subject, payload.schema_id)?;
    if writer.fingerprint() != payload.schema_fingerprint {
        return Err(RepositoryError::provider(format!(
            "metadata subject `{subject}` schema id {} fingerprint mismatch: record has {}, catalog has {}",
            payload.schema_id,
            payload.schema_fingerprint,
            writer.fingerprint()
        )));
    }
    let reader = catalog.latest(subject)?;
    let mut cursor = Cursor::new(payload.bytes.as_ref());
    let value = from_avro_datum(writer.schema(), &mut cursor, Some(reader.schema())).map_err(|err| {
        RepositoryError::provider(format!(
            "failed to decode `{subject}` Avro payload with writer schema {} and reader schema {}: {err}",
            writer.id(),
            reader.id()
        ))
    })?;
    from_value::<T>(&value)
        .map_err(|err| RepositoryError::invalid(format!("failed to materialize `{subject}` Avro value: {err}")))
}
```

- [ ] **Step 7: Run Avro tests**

Run:

```bash
cargo test --test meta_avro_catalog
```

Expected: PASS.

- [ ] **Step 8: Commit Avro catalog and codec**

```bash
git add src/meta/avro src/meta/mod.rs tests/meta_avro_catalog.rs
git commit -m "feat(meta): add static Avro schema catalog"
```

---

### Task 3: Initial Repository Avro Schemas

**Files:**
- Create: `src/meta/avro/schemas/iceberg.catalog/0001.avsc`
- Create: `src/meta/avro/schemas/iceberg.namespace/0001.avsc`
- Create: `src/meta/avro/schemas/iceberg.table_registration/0001.avsc`
- Create: `src/meta/avro/schemas/job.erase/0001.avsc`
- Create: `src/meta/avro/schemas/job.iceberg_optimize/0001.avsc`
- Create: `src/meta/avro/schemas/managed.database/0001.avsc`
- Create: `src/meta/avro/schemas/managed.database_name/0001.avsc`
- Create: `src/meta/avro/schemas/managed.table/0001.avsc`
- Create: `src/meta/avro/schemas/managed.table_name/0001.avsc`
- Create: `src/meta/avro/schemas/managed.schema/0001.avsc`
- Create: `src/meta/avro/schemas/managed.column/0001.avsc`
- Create: `src/meta/avro/schemas/managed.partition/0001.avsc`
- Create: `src/meta/avro/schemas/managed.index/0001.avsc`
- Create: `src/meta/avro/schemas/managed.tablet/0001.avsc`
- Create: `src/meta/avro/schemas/managed.txn/0001.avsc`
- Create: `src/meta/avro/schemas/mv.definition/0001.avsc`
- Create: `src/meta/avro/schemas/mv.target_lookup/0001.avsc`
- Create: `src/meta/avro/schemas/mv.refresh/0001.avsc`
- Create: `src/meta/avro/schemas/mv.dependency/0001.avsc`
- Modify: `src/meta/avro/catalog.rs`

- [ ] **Step 1: Add all current repository schemas to the catalog source list**

Extend `schema_sources()` in `src/meta/avro/catalog.rs` with these entries:

```rust
("iceberg.catalog", 1, include_str!("schemas/iceberg.catalog/0001.avsc")),
("iceberg.namespace", 1, include_str!("schemas/iceberg.namespace/0001.avsc")),
("iceberg.table_registration", 1, include_str!("schemas/iceberg.table_registration/0001.avsc")),
("job.erase", 1, include_str!("schemas/job.erase/0001.avsc")),
("job.iceberg_optimize", 1, include_str!("schemas/job.iceberg_optimize/0001.avsc")),
("managed.database", 1, include_str!("schemas/managed.database/0001.avsc")),
("managed.database_name", 1, include_str!("schemas/managed.database_name/0001.avsc")),
("managed.table", 1, include_str!("schemas/managed.table/0001.avsc")),
("managed.table_name", 1, include_str!("schemas/managed.table_name/0001.avsc")),
("managed.schema", 1, include_str!("schemas/managed.schema/0001.avsc")),
("managed.column", 1, include_str!("schemas/managed.column/0001.avsc")),
("managed.partition", 1, include_str!("schemas/managed.partition/0001.avsc")),
("managed.index", 1, include_str!("schemas/managed.index/0001.avsc")),
("managed.tablet", 1, include_str!("schemas/managed.tablet/0001.avsc")),
("managed.txn", 1, include_str!("schemas/managed.txn/0001.avsc")),
("mv.definition", 1, include_str!("schemas/mv.definition/0001.avsc")),
("mv.target_lookup", 1, include_str!("schemas/mv.target_lookup/0001.avsc")),
("mv.refresh", 1, include_str!("schemas/mv.refresh/0001.avsc")),
("mv.dependency", 1, include_str!("schemas/mv.dependency/0001.avsc")),
```

- [ ] **Step 2: Create exact schema files using this mapping**

Use these Avro type conventions:

```text
Rust i64 -> "long"
Rust i32 -> "int"
Rust bool -> "boolean"
Rust String -> "string"
Rust Vec<T> -> {"type":"array","items":T}
Rust Vec<u8> -> "bytes"
Rust Vec<(String,String)> -> array of {"name":"StringPair","fields":[{"key":"string"},{"value":"string"}]}
Rust BTreeMap<String,i64> -> {"type":"map","values":"long"}
Rust BTreeMap<String,String> -> {"type":"map","values":"string"}
Rust Option<T> -> ["null", T] with "default": null
Serde SCREAMING_SNAKE_CASE enum -> Avro enum with the same uppercase symbols
```

The schema fields must match these Rust structs exactly:

```text
iceberg.catalog -> IcebergCatalogProperties:
  properties: array<StringPair>

iceberg.namespace -> IcebergNamespaceRecord:
  catalog: string
  namespace: string

iceberg.table_registration -> IcebergTableRecord:
  catalog: string
  namespace: string
  table: string

job.erase -> StoredEraseJob:
  job_id: long
  table_id: long
  partition_id: nullable long
  root_path: string
  state: enum JobState [PENDING, RUNNING, FAILED, FINISHED]
  retry_at_ms: nullable long
  updated_at_ms: long
  last_error: nullable string

job.iceberg_optimize -> StoredIcebergOptimizeJob:
  id: long
  catalog: string
  namespace: string
  table: string
  base_snapshot_id: long
  state: enum IcebergOptimizeJobState [PENDING, RUNNING, FINISHED, FAILED]
  created_at_ms: long
  started_at_ms: nullable long
  finished_at_ms: nullable long
  error_message: nullable string
  outcome: nullable IcebergOptimizeJobOutcome

IcebergOptimizeJobOutcome:
  target_snapshot_id: nullable long
  rewritten_data_files: long
  deleted_data_files: long
  added_data_files: long
  output_record_count: long

managed.database -> StoredManagedDatabase:
  db_id: long
  name: string

managed.database_name and managed.table_name and mv.target_lookup -> IdLookup/MvTargetLookup:
  id or mv_id: long

managed.table -> StoredManagedTable:
  table_id: long
  db_id: long
  name: string
  keys_type: string
  bucket_num: long
  current_schema_id: long
  state: enum ManagedTableState [CREATING, ACTIVE, DROPPING, FAILED]
  kind: enum ManagedTableKind [TABLE, MATERIALIZED_VIEW]

managed.schema -> StoredManagedSchema:
  schema_id: long
  table_id: long
  schema_version: long
  tablet_schema_pb: bytes

managed.column -> StoredManagedColumn:
  schema_id: long
  ordinal: long
  column_name: string
  logical_type: string
  nullable: boolean
  visible: boolean
  is_key: boolean

managed.partition -> StoredManagedPartition:
  partition_id: long
  table_id: long
  name: string
  visible_version: long
  next_version: long
  state: enum ManagedPartitionState [CREATING, ACTIVE, RETIRED, FAILED]

managed.index -> StoredManagedIndex:
  index_id: long
  table_id: long
  partition_id: long
  index_type: string
  state: enum ManagedIndexState [CREATING, ACTIVE, RETIRED, FAILED]

managed.tablet -> StoredManagedTablet:
  tablet_id: long
  partition_id: long
  index_id: long
  bucket_seq: long
  tablet_root_path: string

managed.txn -> StoredManagedTxn:
  txn_id: long
  table_id: long
  partition_id: long
  base_version: long
  commit_version: long
  state: enum ManagedTxnState [PREPARED, WRITTEN, VISIBLE, ABORTED]
  retry_at_ms: nullable long
  updated_at_ms: long

mv.definition -> StoredMvDefinition:
  mv_id: long
  select_sql: string
  base_table_refs: array<string>
  primary_key_columns: array<string>
  storage_engine: string
  target_catalog: nullable string
  target_namespace: nullable string
  target_table: nullable string
  schema_contract: nullable string JSON payload for MvSchemaContract
  partition_spec: nullable string JSON payload for MvPartitionContract
  last_refresh_ms: nullable long
  last_refresh_rows: nullable long
  last_refresh_snapshots: map<long>
  last_refresh_table_uuids: map<string>
  last_refreshed_iceberg_snapshot_id: nullable long
  refresh_in_progress: boolean
  active_refresh_id: nullable long
  refresh_target_snapshots: map<long>
  refresh_policy: enum StoredMvRefreshPolicy [MANUAL, ASYNC_ON_CHANGE, ASYNC_INTERVAL]
  refresh_paused: boolean
  refresh_interval_ms: nullable long
  max_staleness_ms: nullable long
  last_scheduler_error: nullable string
  next_refresh_after_ms: nullable long
  created_at_ms: long

mv.refresh -> StoredMvRefresh:
  refresh_id: long
  mv_id: long
  state: enum MvRefreshState [INTENT_CREATED, STAGING_COMMITTED, PUBLISH_COMMITTED, FINALIZED, ABORT_REQUESTED, ABORTED, COMMIT_UNKNOWN]
  target_catalog: nullable string
  target_namespace: nullable string
  target_table: nullable string
  staging_branch: nullable string
  expected_main_snapshot_id: nullable long
  staging_snapshot_id: nullable long
  published_snapshot_id: nullable long
  target_snapshots: map<long>
  base_table_uuids: map<string>
  rows: nullable long
  marker: nullable RefreshCommitMarker
  external_outcome: nullable RefreshExternalOutcome

RefreshCommitMarker:
  refresh_id: long
  mv_id: long
  token: string

RefreshExternalOutcome:
  target_snapshot_id: nullable long
  commit_id: string

mv.dependency -> StoredMvDependency:
  downstream_mv_id: long
  upstream: MvDependencyObjectRef
  created_at_ms: long

MvDependencyObjectRef:
  catalog: nullable string
  database_or_namespace: string
  name: string
  object_type: enum MvDependencyObjectType [TABLE, MATERIALIZED_VIEW]
  storage_engine: enum MvDependencyStorageEngine [MANAGED_LAKE, ICEBERG, EXTERNAL_TABLE]
```

For `schema_contract` and `partition_spec`, use nullable string for this first Avro cut and encode the existing JSON representation inside the domain adapter in Task 4. This keeps the first Avro migration focused and avoids modeling every nested MV contract structure in the first PR.

- [ ] **Step 3: Run catalog validation**

Run:

```bash
cargo test --test meta_avro_catalog avro_catalog_has_unique_subject_ids_and_fingerprints -- --exact
cargo test --test meta_avro_catalog avro_catalog_enforces_full_transitive_compatibility -- --exact
```

Expected: PASS.

- [ ] **Step 4: Commit schema catalog entries**

```bash
git add src/meta/avro/catalog.rs src/meta/avro/schemas
git commit -m "feat(meta): add initial Avro metadata schemas"
```

---

### Task 4: Shared Repository Avro Payload Helpers

**Files:**
- Modify: `src/meta/payload.rs`
- Modify: `src/meta/repository/mod.rs`
- Modify: `tests/meta_repository.rs`

- [ ] **Step 1: Write failing Avro payload helper tests**

In `tests/meta_repository.rs`, replace the existing JSON payload helper test with:

```rust
#[test]
fn repository_avro_payload_round_trips_sample_payload() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEvolution {
        id: i64,
        name: String,
        tags: Vec<String>,
    }

    let payload = novarocks::meta::repository::encode_record_payload(
        "test.evolution",
        &TestEvolution {
            id: 42,
            name: "sample".to_string(),
            tags: vec!["metadata".to_string()],
        },
    )?;
    assert_eq!(payload.encoding, novarocks::meta::MetaPayloadEncoding::Avro);
    assert_eq!(payload.schema_id, 2);
    assert_eq!(payload.schema_fingerprint.len(), 16);

    let decoded: TestEvolution =
        novarocks::meta::repository::decode_payload_for_kind("test.evolution", &payload)?;
    assert_eq!(
        decoded,
        TestEvolution {
            id: 42,
            name: "sample".to_string(),
            tags: vec!["metadata".to_string()],
        }
    );
    Ok(())
}
```

- [ ] **Step 2: Run the failing helper test**

Run:

```bash
cargo test --test meta_repository repository_avro_payload_round_trips_sample_payload -- --exact
```

Expected: FAIL because repository helper functions still expose JSON helpers.

- [ ] **Step 3: Replace JSON payload helper exports**

Replace `src/meta/payload.rs` with:

```rust
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::meta::MetaPayload;
use crate::meta::avro;
use crate::meta::repository::RepositoryResult;

pub fn encode_record_payload<T>(kind: &str, value: &T) -> RepositoryResult<MetaPayload>
where
    T: Serialize,
{
    avro::encode_payload(kind, value)
}

pub fn decode_payload_for_kind<T>(kind: &str, payload: &MetaPayload) -> RepositoryResult<T>
where
    T: DeserializeOwned,
{
    avro::decode_payload(kind, payload)
}
```

Update `src/meta/repository/mod.rs` exports:

```rust
pub use crate::meta::payload::{decode_payload_for_kind, encode_record_payload};
```

- [ ] **Step 4: Run the helper test**

Run:

```bash
cargo test --test meta_repository repository_avro_payload_round_trips_sample_payload -- --exact
```

Expected: PASS.

- [ ] **Step 5: Commit repository payload helper**

```bash
git add src/meta/payload.rs src/meta/repository/mod.rs tests/meta_repository.rs
git commit -m "refactor(meta): route repository payloads through Avro helpers"
```

---

### Task 5: Migrate Domain Repositories from JSON to Avro

**Files:**
- Modify: `src/meta/repository/iceberg_catalog.rs`
- Modify: `src/meta/repository/job.rs`
- Modify: `src/meta/repository/managed_lake.rs`
- Modify: `src/meta/repository/managed_txn.rs`
- Modify: `src/meta/repository/mv.rs`
- Modify: `tests/meta_repository.rs`

- [ ] **Step 1: Update repository imports**

In each repository file, replace:

```rust
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_json_payload, encode_json_payload, id_scopes,
};
```

with:

```rust
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_payload_for_kind, encode_record_payload, id_scopes,
};
```

In `iceberg_catalog.rs`, keep the existing `MvMetaRepository` import and use this repository import:

```rust
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_payload_for_kind, encode_record_payload,
};
```

- [ ] **Step 2: Replace encode calls**

Replace each call:

```rust
encode_json_payload(SOME_SCHEMA_VERSION, &value)?
```

with:

```rust
encode_record_payload(SOME_KIND, &value)?
```

For existing local variables named `stored`, use:

```rust
encode_record_payload(MANAGED_TXN_KIND, stored)?
```

For lookup structs, use the lookup kind constant already passed to `record_kind(...)`.

- [ ] **Step 3: Replace decode helper bodies**

In each repository file, replace local `decode_record_payload` with:

```rust
fn decode_record_payload<T>(record: &MetaRecord, expected_kind: &str) -> RepositoryResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    if record.kind.as_str() != expected_kind {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has kind {}, expected {expected_kind}",
            record.key.canonical_path(),
            record.kind.as_str()
        )));
    }
    decode_payload_for_kind(expected_kind, &record.payload)
}
```

Then update all call sites from:

```rust
decode_record_payload(&record, SOME_KIND, SOME_SCHEMA_VERSION)
```

to:

```rust
decode_record_payload(&record, SOME_KIND)
```

- [ ] **Step 4: Remove schema version constants**

Delete constants named like these from repository files:

```rust
const MV_DEFINITION_SCHEMA_VERSION: i32 = 2;
const MANAGED_TABLE_SCHEMA_VERSION: i32 = 1;
const ERASE_JOB_SCHEMA_VERSION: i32 = 1;
```

Keep record kind constants. Record kind is now the Avro subject.

- [ ] **Step 5: Add DTO adapters for Avro boundary shapes**

Do not encode production repository domain structs directly when their shape differs from the Avro v1 boundary.

For `iceberg.catalog`, Task 3 intentionally stores `properties` as an array of named key/value records in Avro while the domain type is still `Vec<(String, String)>`. Add private Avro DTO structs inside `src/meta/repository/iceberg_catalog.rs` and convert at the repository boundary:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IcebergCatalogPropertiesAvro {
    properties: Vec<StringPairAvro>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StringPairAvro {
    key: String,
    value: String,
}
```

Encode by converting each `(String, String)` pair into `StringPairAvro { key, value }`. Decode by converting each `StringPairAvro` back into the domain tuple. Keep the `iceberg.catalog/0001.avsc` `StringPair` record shape; do not replace it with tuple arrays.

Because Task 3 stores `schema_contract` and `partition_spec` as nullable JSON strings in Avro v1, add private Avro DTO structs inside `src/meta/repository/mv.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredMvDefinitionAvro {
    mv_id: i64,
    select_sql: String,
    base_table_refs: Vec<String>,
    primary_key_columns: Vec<String>,
    storage_engine: String,
    target_catalog: Option<String>,
    target_namespace: Option<String>,
    target_table: Option<String>,
    schema_contract: Option<String>,
    partition_spec: Option<String>,
    last_refresh_ms: Option<i64>,
    last_refresh_rows: Option<i64>,
    last_refresh_snapshots: BTreeMap<String, i64>,
    last_refresh_table_uuids: BTreeMap<String, String>,
    last_refreshed_iceberg_snapshot_id: Option<i64>,
    refresh_in_progress: bool,
    active_refresh_id: Option<i64>,
    refresh_target_snapshots: BTreeMap<String, i64>,
    refresh_policy: StoredMvRefreshPolicy,
    refresh_paused: bool,
    refresh_interval_ms: Option<i64>,
    max_staleness_ms: Option<i64>,
    last_scheduler_error: Option<String>,
    next_refresh_after_ms: Option<i64>,
    created_at_ms: i64,
}
```

Add conversion helpers:

```rust
impl TryFrom<&StoredMvDefinition> for StoredMvDefinitionAvro {
    type Error = RepositoryError;

    fn try_from(value: &StoredMvDefinition) -> Result<Self, Self::Error> {
        Ok(Self {
            mv_id: value.mv_id,
            select_sql: value.select_sql.clone(),
            base_table_refs: value.base_table_refs.clone(),
            primary_key_columns: value.primary_key_columns.clone(),
            storage_engine: value.storage_engine.clone(),
            target_catalog: value.target_catalog.clone(),
            target_namespace: value.target_namespace.clone(),
            target_table: value.target_table.clone(),
            schema_contract: value
                .schema_contract
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|err| RepositoryError::invalid(format!("encode MV schema contract failed: {err}")))?,
            partition_spec: value
                .partition_spec
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|err| RepositoryError::invalid(format!("encode MV partition contract failed: {err}")))?,
            last_refresh_ms: value.last_refresh_ms,
            last_refresh_rows: value.last_refresh_rows,
            last_refresh_snapshots: value.last_refresh_snapshots.clone(),
            last_refresh_table_uuids: value.last_refresh_table_uuids.clone(),
            last_refreshed_iceberg_snapshot_id: value.last_refreshed_iceberg_snapshot_id,
            refresh_in_progress: value.refresh_in_progress,
            active_refresh_id: value.active_refresh_id,
            refresh_target_snapshots: value.refresh_target_snapshots.clone(),
            refresh_policy: value.refresh_policy.clone(),
            refresh_paused: value.refresh_paused,
            refresh_interval_ms: value.refresh_interval_ms,
            max_staleness_ms: value.max_staleness_ms,
            last_scheduler_error: value.last_scheduler_error.clone(),
            next_refresh_after_ms: value.next_refresh_after_ms,
            created_at_ms: value.created_at_ms,
        })
    }
}
```

Use the inverse conversion when decoding `mv.definition`, parsing the JSON strings back into `MvSchemaContract` and `MvPartitionContract`.

- [ ] **Step 6: Remove legacy schema-version guards before Avro writes**

Before any domain repository writes Avro payloads, remove all old `*_SCHEMA_VERSION` decode guards and call sites. Avro `payload.schema_id` is the Avro catalog schema id, not the legacy JSON repository schema version.

Do not add `src/meta/avro/schemas/mv.definition/0002.avsc` only to match `MV_DEFINITION_SCHEMA_VERSION = 2`. The initial Avro `mv.definition` schema remains id 1, and the repository migration must treat that id as independent from the old JSON schema-version constants.

- [ ] **Step 7: Run repository tests**

Run:

```bash
cargo test --test meta_repository
```

Expected: PASS after Steps 1-5 are complete. If this command reports a stale `encode_json_payload`, `decode_json_payload`, or `*_SCHEMA_VERSION` symbol, apply the exact replacements from Steps 1-4 and rerun the same command.

- [ ] **Step 8: Replace schema-version mismatch tests**

Remove tests that assert old JSON schema version mismatch strings. Add a fingerprint mismatch test:

```rust
#[test]
fn mv_repository_rejects_definition_fingerprint_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            sample_mv_definition_request("select id from orders"),
        )?;
        txn.commit()?;
        definition.mv_id
    };

    {
        let mut txn = provider.begin_write("corrupt mv payload fingerprint")?;
        let versioned = repository
            .load_versioned_by_id(txn.as_ref(), mv_id)?
            .expect("definition exists");
        let record = txn
            .get(&MetaKey::new("mv", ["by-id", mv_id.to_string()])?)?
            .expect("raw definition record exists");
        let mut payload = record.payload.clone();
        payload.schema_fingerprint = "ffffffffffffffff".to_string();
        txn.put(MetaRecordPut::new(
            MetaKey::new("mv", ["by-id", mv_id.to_string()])?,
            MetaRecordKind::new("mv.definition")?,
            ExpectedRevision::Exact(versioned.record_revision),
            payload,
        ))?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let err = repository
        .load_by_id(read.as_ref(), mv_id)
        .expect_err("fingerprint mismatch should fail");
    assert!(err.to_string().contains("fingerprint mismatch"), "{err}");
    Ok(())
}
```

- [ ] **Step 9: Run repository tests**

Run:

```bash
cargo test --test meta_repository
```

Expected: PASS.

- [ ] **Step 10: Commit repository migration**

```bash
git add src/meta/repository src/meta/payload.rs tests/meta_repository.rs
git commit -m "refactor(meta): encode repository records as Avro"
```

---

### Task 6: End-to-End Provider Rejection and Store Format Coverage

**Files:**
- Modify: `tests/meta_sqlite_provider.rs`
- Modify: `tests/meta_repository.rs`

- [ ] **Step 1: Add bad payload read coverage**

Add to `tests/meta_repository.rs`:

```rust
#[test]
fn repository_rejects_unknown_avro_schema_id() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let key = MetaKey::new("mv", ["by-id", "777"])?;
    {
        let mut txn = provider.begin_write("write unknown schema id")?;
        txn.put(MetaRecordPut::new(
            key,
            MetaRecordKind::new("mv.definition")?,
            ExpectedRevision::NotExists,
            novarocks::meta::MetaPayload::avro(
                999,
                "0000000000000000",
                Bytes::from_static(b"not-valid-avro"),
            ),
        ))?;
        txn.commit()?;
    }

    let repository = MvMetaRepository::default();
    let read = provider.begin_read()?;
    let err = repository
        .load_by_id(read.as_ref(), 777)
        .expect_err("unknown schema id should fail");
    assert!(err.to_string().contains("unknown Avro schema `mv.definition` id 999"), "{err}");
    Ok(())
}
```

- [ ] **Step 2: Add legacy marker missing with Avro-shaped table coverage**

Add to `tests/meta_sqlite_provider.rs`:

```rust
#[test]
fn sqlite_provider_rejects_nonempty_store_missing_format_marker() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE meta_provider_schema (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            CREATE TABLE meta_records (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                kind TEXT NOT NULL,
                revision INTEGER NOT NULL,
                payload_encoding TEXT NOT NULL,
                payload_schema_id INTEGER NOT NULL,
                payload_schema_fingerprint TEXT NOT NULL,
                payload BLOB NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, key)
            );
            CREATE TABLE meta_id_scopes (
                scope TEXT PRIMARY KEY,
                next_id INTEGER NOT NULL
            );
            INSERT INTO meta_records(
                namespace, key, kind, revision, payload_encoding, payload_schema_id,
                payload_schema_fingerprint, payload, created_at_ms, updated_at_ms
            )
            VALUES('mv', 'by-id/1', 'mv.definition', 1, 'avro', 1, '0000000000000000', X'00', 1, 1);
            "#,
        )?;
    }

    let err = SqliteMetaStoreProvider::open(&path).expect_err("missing marker must fail");
    assert!(err.to_string().contains("missing store_format marker"), "{err}");
    Ok(())
}
```

- [ ] **Step 3: Run focused rejection tests**

Run:

```bash
cargo test --test meta_sqlite_provider sqlite_provider_rejects_nonempty_store_missing_format_marker -- --exact
cargo test --test meta_repository repository_rejects_unknown_avro_schema_id -- --exact
```

Expected: PASS.

- [ ] **Step 4: Commit rejection coverage**

```bash
git add tests/meta_sqlite_provider.rs tests/meta_repository.rs
git commit -m "test(meta): cover Avro metadata rejection paths"
```

---

### Task 7: Full Verification and Cleanup

**Files:**
- Modify: files touched by Tasks 1-6 when the verification commands below identify a stale symbol or formatting diff.

- [ ] **Step 1: Search for stale JSON metadata helpers and schema-version fields**

Run:

```bash
rg -n "encode_json_payload|decode_json_payload|payload_schema_version|schema_version mismatch|MetaPayload::json|MetaPayloadEncoding::Json|MetaPayloadEncoding::Protobuf" src tests
```

Expected: no stale metadata payload matches. Acceptable matches are `StoredManagedSchema.schema_version`, `tablet_schema_pb`, and test names that do not refer to meta provider payload schema.

- [ ] **Step 2: Run formatting check**

Run:

```bash
cargo fmt -- --check
```

Expected: PASS. If the command reports formatting differences, run:

```bash
cargo fmt
git diff --check
```

Expected after formatting: `git diff --check` exits 0.

- [ ] **Step 3: Run core build check**

Run:

```bash
cargo check --all-targets
```

Expected: PASS.

- [ ] **Step 4: Run targeted metadata tests**

Run:

```bash
cargo test --test meta_avro_catalog
cargo test --test meta_sqlite_provider
cargo test --test meta_repository
cargo test --test meta_framework_flow
```

Expected: PASS.

- [ ] **Step 5: Run managed-lake smoke tests after repository migration**

Run:

```bash
cargo test --test standalone_mysql_server managed -- --nocapture
```

Expected: PASS or no matching tests. If there are no matching tests, record that in the final implementation summary.

- [ ] **Step 6: Commit final cleanup**

If Step 1-5 produced changes:

```bash
git add src tests
git commit -m "chore(meta): clean up Avro metadata migration"
```

If there are no changes:

```bash
git status --short
```

Expected: clean worktree.
