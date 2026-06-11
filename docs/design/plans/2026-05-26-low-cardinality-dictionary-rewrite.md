# Low-cardinality Dictionary Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement standalone SQL low-cardinality dictionary rewrite for NovaRocks with real table-owned dictionary maintenance, StarRocks/Iceberg owner support, and cleanup of stale scan-source and managed-lake naming.

**Architecture:** Add a persistent `DictionaryManager` on `StandaloneState`, populated by `ANALYZE FULL TABLE` and invalidated by write/drop/alter flows. Add logical/physical dictionary metadata and a `LowCardinalityDictionaryRewrite` stage after column pruning; codegen consumes the optimizer output and emits `query_global_dicts`, `query_global_dict_exprs`, scan dict slots, and `TDecodeNode` without doing dictionary discovery.

**Tech Stack:** Rust, Arrow, Apache Avro metadata payloads, existing NovaRocks logical rewrite framework, Cascades physical implementation, Thrift plan structs, sql-tests runner.

---

## Scope Check

This is a single dependent implementation chain, not separate independent projects:

- Scan-source cleanup is required before dictionary owner identity can be type-safe.
- StarRocks naming cleanup is required before persistent dictionary metadata uses the new `starrocks.*` namespace.
- Dictionary metadata and maintenance are required before the optimizer can select Active snapshots.
- Logical rewrite, physical operator, and codegen must land together to make the two aggregate SQL regressions pass.

Each task below is independently testable and should be committed before moving to the next task.

## File Structure

### Scan Source And Table Identity

- Modify `src/sql/catalog.rs`
  - Rename `S3FileInfo` to `IcebergDataFileInfo`.
  - Change `ScanSource::S3ParquetFiles` to `ScanSource::IcebergDataFiles { table, files, cloud_properties }`.
  - Remove `TableDef::iceberg_table`.
  - Expand `IcebergTableInfo` with stable identity fields used by dictionaries.
- Modify `src/connector/iceberg/catalog/backend.rs`, `src/connector/starrocks/managed/ivm_delta_source.rs`, `src/engine/query_prep.rs`, `src/sql/codegen/nodes.rs`, `src/sql/codegen/descriptors.rs`, `src/sql/codegen/fragment_builder.rs`, `src/sql/explain.rs`, `src/sql/optimizer/statistics.rs`, and all direct `ScanSource::S3ParquetFiles` call sites.
  - Move Iceberg table identity into the scan source variant.
  - Delete or rewrite no-identity file scan tests.

### StarRocks Naming Cleanup

- Rename files:
  - `src/meta/repository/managed_lake.rs` -> `src/meta/repository/starrocks_table.rs`
  - `src/meta/repository/managed_txn.rs` -> `src/meta/repository/starrocks_txn.rs`
- Modify `src/meta/keys.rs`, `src/meta/repository/mod.rs`, `src/meta/avro/catalog.rs`, and schema folders under `src/meta/avro/schemas/`.
  - Rename metadata subjects from `managed.*` to `starrocks.*`.
  - Rename namespaces from `managed` / `managed.txn` to `starrocks` / `starrocks.txn`.
- Modify `src/connector/starrocks/managed/**`, `src/engine/**`, `src/common/app_config.rs`, `sql-tests/**`, and docs touched by compilation or regression tests.
  - Keep the Rust module path `connector::starrocks::managed` for this plan unless a task explicitly renames the whole directory; rename user-facing backend names, config type names, repository type names, metadata subjects, and error strings.

### Dictionary Metadata And Manager

- Create `src/meta/repository/dictionary.rs`
  - Store `DictionarySnapshot` records keyed by owner + column.
  - Provide load/upsert/mark-stale/drop APIs.
- Create Avro schemas:
  - `src/meta/avro/schemas/dictionary.snapshot/0001.avsc`
  - `src/meta/avro/schemas/dictionary.lookup/0001.avsc`
- Create `src/engine/dictionary/mod.rs`
  - Public `DictionaryManager` used by standalone engine flows.
- Create `src/engine/dictionary/model.rs`
  - Owner, watermark, snapshot, value, and skip reason types.
- Create `src/engine/dictionary/rebuild.rs`
  - `ANALYZE FULL` rebuild path.
- Create `src/engine/dictionary/maintenance.rs`
  - Insert/overwrite/delete/update/drop invalidation hooks.

### Optimizer Dictionary Rewrite

- Create `src/sql/optimizer/rewrite/rules/low_cardinality_dict/mod.rs`
- Create `src/sql/optimizer/rewrite/rules/low_cardinality_dict/collector.rs`
- Create `src/sql/optimizer/rewrite/rules/low_cardinality_dict/context.rs`
- Create `src/sql/optimizer/rewrite/rules/low_cardinality_dict/expr.rs`
- Create `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rewriter.rs`
- Create `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rule.rs`
- Modify `src/sql/optimizer/rewrite/rules/mod.rs` and `src/sql/optimizer/rewrite/registry.rs`
  - Register `LowCardinalityDictionaryRewrite` after `ColumnPruning`.
- Modify `src/sql/optimizer/operator.rs`, `src/sql/optimizer/cascades_rules/implement.rs`, `src/sql/optimizer/derive/mod.rs`, `src/sql/optimizer/derive/passthrough.rs`, and `src/sql/optimizer/physical_plan.rs`.
  - Add `LogicalDecodeOp` and `PhysicalDecodeOp`.

### Codegen And Runtime Plan Interface

- Modify `src/sql/codegen/fragment_builder.rs`
  - Carry query-global dictionaries during fragment building.
  - Emit `PhysicalDecode` as `TDecodeNode`.
  - Add hidden dict slots to StarRocks/Iceberg scan tuple descriptors.
- Modify `src/sql/codegen/nodes.rs`
  - Fill `TLakeScanNode.dict_string_id_to_int_ids`.
  - Add `build_decode_node`.
- Modify `src/runtime/coordinator.rs`
  - Preserve `query_global_dicts` and `query_global_dict_exprs` in coordinated fragments.
- Leave `src/lower/node/decode.rs`, `src/exec/expr/dict_decode.rs`, and `src/connector/starrocks/scan/reader.rs` behavior intact except for compile fixes and stricter miss errors already required by the design.

### Tests

- Add Rust unit tests under the new modules and existing test modules touched by each task.
- Add SQL tests:
  - `sql-tests/optimizer/sql/low_cardinality_dict_rewrite.sql`
  - `sql-tests/optimizer/sql/low_cardinality_dict_disabled.sql`
  - `sql-tests/optimizer/sql/low_cardinality_dict_stale.sql`
- Update expected output under `sql-tests/aggregate/result/` for the two existing failing aggregate cases only after behavior is verified.

---

### Task 1: Type-Safe Scan Source Cleanup

**Files:**
- Modify: `src/sql/catalog.rs`
- Modify: every `ScanSource::S3ParquetFiles`, `S3FileInfo`, and `TableDef` field named `iceberg_table` call site returned by:

```bash
rg -n "S3ParquetFiles|S3FileInfo|iceberg_table" src sql-tests tests
```

- Test: existing Rust tests in touched modules.

- [ ] **Step 1: Write compile-failing type migration**

In `src/sql/catalog.rs`, replace the `IcebergTableInfo`, `S3FileInfo`, `ScanSource`, and `TableDef` definitions with these public shapes while preserving unchanged existing field comments where useful:

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct IcebergTableInfo {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub table_uuid: Option<String>,
    pub current_snapshot_id: Option<i64>,
    pub schema_id: i32,
    pub location: String,
    pub schema: IcebergSchemaDef,
    pub serialized_metadata: Option<String>,
}

#[derive(Clone, Debug)]
pub struct IcebergDataFileInfo {
    pub path: String,
    pub size: i64,
    pub row_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub ivm_change_op: Option<i8>,
    pub delete_files: Vec<IcebergDeleteFileInfo>,
    pub manifest_path: Option<String>,
    pub partition_values: Vec<IcebergPartitionFieldValue>,
}

#[derive(Clone, Debug)]
pub enum ScanSource {
    StarRocks,
    IcebergDataFiles {
        table: IcebergTableInfo,
        files: Vec<IcebergDataFileInfo>,
        cloud_properties: BTreeMap<String, String>,
    },
    IcebergMetadataTable {
        table: IcebergTableInfo,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
        serialized_table: String,
        cloud_properties: BTreeMap<String, String>,
        metadata_payload: Option<String>,
    },
    IcebergDeltaTable {
        table: IcebergTableInfo,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
    },
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub iceberg_row_lineage_metadata_columns: Vec<ColumnDef>,
    pub source: ScanSource,
}
```

- [ ] **Step 2: Run compile to collect all migration errors**

Run:

```bash
cargo check 2>&1 | tee /tmp/novarocks-dict-task1-check.log
```

Expected: FAIL with missing `S3ParquetFiles`, `S3FileInfo`, and `iceberg_table` field errors. No runtime behavior should be changed yet.

- [ ] **Step 3: Migrate production Iceberg table builders**

In `src/connector/iceberg/catalog/backend.rs`, build `IcebergTableInfo` once from the resolved catalog entry and put it inside `ScanSource::IcebergDataFiles`. The constructed value must include these fields:

```rust
let iceberg_table_info = IcebergTableInfo {
    catalog: catalog_name.clone(),
    namespace: namespace_name.clone(),
    table: table_name.clone(),
    table_uuid: table.metadata().uuid().map(|uuid| uuid.to_string()),
    current_snapshot_id: table.metadata().current_snapshot_id(),
    schema_id: table.metadata().current_schema_id(),
    location: table.metadata().location().to_string(),
    schema,
    serialized_metadata: Some(serde_json::to_string(table.metadata()).map_err(|err| {
        format!("serialize iceberg table metadata failed: {err}")
    })?),
};
```

Every data-file scan must now use:

```rust
source: ScanSource::IcebergDataFiles {
    table: iceberg_table_info,
    files,
    cloud_properties,
},
```

- [ ] **Step 4: Migrate metadata and delta scan builders**

For metadata tables, put the same Iceberg identity into `ScanSource::IcebergMetadataTable { table, metadata_table_type, serialized_table, cloud_properties, metadata_payload }`. For delta scans, replace loose `catalog`, `namespace`, and `table` fields with:

```rust
source: ScanSource::IcebergDeltaTable {
    table: iceberg_table_info,
    from_snapshot_id,
    to_snapshot_id,
},
```

In `src/sql/codegen/nodes.rs`, update `build_iceberg_delta_scan_node` to read catalog/namespace/table from `table.catalog`, `table.namespace`, and `table.table`.

- [ ] **Step 5: Migrate codegen/statistics/explain consumers**

Apply these exact semantic replacements:

```rust
ScanSource::S3ParquetFiles { files, cloud_properties }
```

becomes:

```rust
ScanSource::IcebergDataFiles {
    table,
    files,
    cloud_properties,
}
```

`op.table.iceberg_table.as_ref()` becomes `iceberg_table_info(&op.table.source)`, with this helper added near the consumer:

```rust
fn iceberg_table_info(source: &ScanSource) -> Option<&IcebergTableInfo> {
    match source {
        ScanSource::IcebergDataFiles { table, .. }
        | ScanSource::IcebergMetadataTable { table, .. }
        | ScanSource::IcebergDeltaTable { table, .. } => Some(table),
        ScanSource::StarRocks => None,
    }
}
```

- [ ] **Step 6: Delete no-identity scan fixtures**

For tests that directly construct `ScanSource::S3ParquetFiles` without a real Iceberg identity, replace with `ScanSource::StarRocks` and a `PhysicalTableLayout` only if the test is about generic scan formatting. For tests about Iceberg stats/codegen, construct this helper in the local test module:

```rust
fn test_iceberg_table_info() -> IcebergTableInfo {
    IcebergTableInfo {
        catalog: "test_catalog".to_string(),
        namespace: "test_db".to_string(),
        table: "test_table".to_string(),
        table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
        current_snapshot_id: Some(7),
        schema_id: 1,
        location: "file:///tmp/test_table".to_string(),
        schema: IcebergSchemaDef { fields: vec![] },
        serialized_metadata: None,
    }
}
```

Every remaining `IcebergDataFiles` test fixture must pass `table: test_iceberg_table_info()`.

- [ ] **Step 7: Verify scan cleanup**

Run:

```bash
rg -n "S3ParquetFiles|S3FileInfo|iceberg_table" src sql-tests tests
cargo test -p novarocks --lib sql::catalog sql::codegen::nodes sql::explain -- --nocapture
cargo check
```

Expected:

- The `rg` command prints no matches.
- Targeted tests pass.
- `cargo check` passes.

- [ ] **Step 8: Commit**

Run:

```bash
git add src sql-tests tests
git commit -m "refactor: make iceberg scan identity explicit"
```

Expected: commit succeeds.

---

### Task 2: StarRocks Naming And Metadata Namespace Cleanup

**Files:**
- Rename: `src/meta/repository/managed_lake.rs` -> `src/meta/repository/starrocks_table.rs`
- Rename: `src/meta/repository/managed_txn.rs` -> `src/meta/repository/starrocks_txn.rs`
- Rename schema folders under `src/meta/avro/schemas/managed.*` to `src/meta/avro/schemas/starrocks.*`
- Modify: `src/meta/keys.rs`
- Modify: `src/meta/repository/mod.rs`
- Modify: `src/meta/avro/catalog.rs`
- Modify: `src/connector/starrocks/managed/**`
- Modify: `src/engine/**`
- Modify: `src/common/app_config.rs`
- Modify: `sql-tests/**` and docs required by tests.

- [ ] **Step 1: Rename repository modules and exported types**

Perform mechanical renames:

```bash
mv src/meta/repository/managed_lake.rs src/meta/repository/starrocks_table.rs
mv src/meta/repository/managed_txn.rs src/meta/repository/starrocks_txn.rs
perl -0pi -e 's/managed_lake/starrocks_table/g; s/managed_txn/starrocks_txn/g' $(rg -l "managed_lake|managed_txn" src)
perl -0pi -e 's/ManagedLakeMetaRepository/StarRocksTableMetaRepository/g; s/ManagedLakeTxnRepository/StarRocksTxnRepository/g' $(rg -l "ManagedLakeMetaRepository|ManagedLakeTxnRepository" src)
perl -0pi -e 's/ManagedLakeSnapshot/StarRocksTableSnapshot/g; s/StoredManaged/StoredStarRocks/g; s/CreateManaged/CreateStarRocks/g; s/ManagedTable/StarRocksTable/g; s/ManagedPartition/StarRocksPartition/g; s/ManagedIndex/StarRocksIndex/g; s/ManagedTablet/StarRocksTablet/g' $(rg -l "ManagedLakeSnapshot|StoredManaged|CreateManaged|ManagedTable|ManagedPartition|ManagedIndex|ManagedTablet" src)
```

Then run `cargo fmt` after the compile fixes in later steps.

- [ ] **Step 2: Rename metadata namespaces and schema subjects**

In `src/meta/keys.rs`, replace the managed constants with:

```rust
pub const NS_STARROCKS: &str = "starrocks";
pub const NS_STARROCKS_TXN: &str = "starrocks.txn";
pub const NS_MV: &str = "mv";
pub const NS_ICEBERG_CATALOG: &str = "iceberg_catalog";
pub const NS_JOB: &str = "job";
```

In the renamed repository modules, replace kind constants with:

```rust
const STARROCKS_DATABASE_KIND: &str = "starrocks.database";
const STARROCKS_DATABASE_NAME_KIND: &str = "starrocks.database_name";
const STARROCKS_TABLE_KIND: &str = "starrocks.table";
const STARROCKS_TABLE_NAME_KIND: &str = "starrocks.table_name";
const STARROCKS_SCHEMA_KIND: &str = "starrocks.schema";
const STARROCKS_COLUMN_KIND: &str = "starrocks.column";
const STARROCKS_PARTITION_KIND: &str = "starrocks.partition";
const STARROCKS_INDEX_KIND: &str = "starrocks.index";
const STARROCKS_TABLET_KIND: &str = "starrocks.tablet";
```

and transaction kind:

```rust
const STARROCKS_TXN_KIND: &str = "starrocks.txn";
```

- [ ] **Step 3: Rename Avro schema folders and catalog entries**

Run:

```bash
for dir in src/meta/avro/schemas/managed.*; do
  new="${dir/managed./starrocks.}"
  mv "$dir" "$new"
done
```

In `src/meta/avro/catalog.rs`, replace every `managed.` schema source with the matching `starrocks.` subject and include path. The entry for table must look like:

```rust
SchemaSource {
    subject: "starrocks.table",
    id: 1,
    raw_schema: include_str!("schemas/starrocks.table/0001.avsc"),
},
```

- [ ] **Step 4: Rename backend name from managed to starrocks**

In `src/connector/starrocks/managed/backend.rs`, `name()` must return:

```rust
"starrocks"
```

In `src/engine/mv/lifecycle.rs`, `MvStorageEngine::StarRocks.backend_name()` must return:

```rust
"starrocks"
```

In every backend lookup, replace literal `"managed"` with `"starrocks"` unless it is inside old docs not used by tests.

- [ ] **Step 5: Rename config type and user-visible strings**

Rename `ManagedLakeConfig` to `StarRocksTableConfig` in code. Error strings should use these forms:

```text
standalone StarRocks table config is missing
StarRocks table metadata exists but standalone StarRocks table config is missing
StarRocks table warehouse mismatch: snapshot={snapshot} config={config}
```

`mv_default_storage_engine` default remains `"starrocks"`. Unknown storage engine errors must list `starrocks` and `iceberg`.

- [ ] **Step 6: Verify naming cleanup**

Run:

```bash
rg -n "managed-lake|managed lake|ManagedLake|managed\\.database|managed\\.table|\\bmanaged\\b" src sql-tests docs -g '!docs/design/plans/2026-05-25-sqlite-avro-metadata-schema.md'
cargo test -p novarocks --lib meta::avro meta::repository connector::starrocks::managed -- --nocapture
cargo check
```

Expected:

- Remaining matches are either inside intentionally historical docs/specs or function names that are out of this plan's compile path and are scheduled in this task before commit.
- Rust tests pass.
- `cargo check` passes.

- [ ] **Step 7: Commit**

Run:

```bash
git add src sql-tests docs
git commit -m "refactor: rename managed storage metadata to starrocks"
```

Expected: commit succeeds.

---

### Task 3: Dictionary Repository And Manager Skeleton

**Files:**
- Create: `src/meta/repository/dictionary.rs`
- Create: `src/meta/avro/schemas/dictionary.snapshot/0001.avsc`
- Create: `src/meta/avro/schemas/dictionary.lookup/0001.avsc`
- Modify: `src/meta/repository/mod.rs`
- Modify: `src/meta/avro/catalog.rs`
- Modify: `src/meta/keys.rs`
- Create: `src/engine/dictionary/mod.rs`
- Create: `src/engine/dictionary/model.rs`
- Modify: `src/engine/mod.rs`

- [ ] **Step 1: Add dictionary Avro schemas**

Create `src/meta/avro/schemas/dictionary.snapshot/0001.avsc`:

```json
{
  "type": "record",
  "name": "DictionarySnapshot",
  "namespace": "novarocks.meta.dictionary",
  "fields": [
    { "name": "dictionary_id", "type": "long" },
    { "name": "owner_kind", "type": "string" },
    { "name": "owner_key", "type": "string" },
    { "name": "column_id", "type": [ "null", "long" ], "default": null },
    { "name": "column_name", "type": "string" },
    { "name": "data_type", "type": "string" },
    { "name": "version", "type": "long" },
    { "name": "watermark", "type": "string" },
    { "name": "values", "type": { "type": "array", "items": {
      "type": "record",
      "name": "DictionaryValue",
      "fields": [
        { "name": "id", "type": "int" },
        { "name": "bytes", "type": "bytes" }
      ]
    }}},
    { "name": "null_id", "type": "int", "default": 0 },
    { "name": "state", "type": "string" },
    { "name": "order_preserving", "type": "boolean", "default": false },
    { "name": "created_at_ms", "type": "long" },
    { "name": "updated_at_ms", "type": "long" }
  ]
}
```

Create `src/meta/avro/schemas/dictionary.lookup/0001.avsc`:

```json
{
  "type": "record",
  "name": "DictionaryLookup",
  "namespace": "novarocks.meta.dictionary",
  "fields": [
    { "name": "owner_kind", "type": "string" },
    { "name": "owner_key", "type": "string" },
    { "name": "column_name", "type": "string" },
    { "name": "dictionary_id", "type": "long" }
  ]
}
```

- [ ] **Step 2: Register dictionary schemas**

In `src/meta/avro/catalog.rs`, add:

```rust
SchemaSource {
    subject: "dictionary.snapshot",
    id: 1,
    raw_schema: include_str!("schemas/dictionary.snapshot/0001.avsc"),
},
SchemaSource {
    subject: "dictionary.lookup",
    id: 1,
    raw_schema: include_str!("schemas/dictionary.lookup/0001.avsc"),
},
```

In `src/meta/keys.rs`, add:

```rust
pub const NS_DICTIONARY: &str = "dictionary";
```

- [ ] **Step 3: Add repository model and APIs**

Create `src/meta/repository/dictionary.rs`:

```rust
use serde::{Deserialize, Serialize};

use crate::meta::keys::{NS_DICTIONARY, normalize_lookup_name};
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_payload_for_kind, encode_record_payload,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaWriteTxn,
};

const DICTIONARY_SNAPSHOT_KIND: &str = "dictionary.snapshot";
const DICTIONARY_LOOKUP_KIND: &str = "dictionary.lookup";

#[derive(Default)]
pub struct DictionaryMetaRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredDictionaryValue {
    pub id: i32,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredDictionarySnapshot {
    pub dictionary_id: i64,
    pub owner_kind: String,
    pub owner_key: String,
    pub column_id: Option<i64>,
    pub column_name: String,
    pub data_type: String,
    pub version: i64,
    pub watermark: String,
    pub values: Vec<StoredDictionaryValue>,
    pub null_id: i32,
    pub state: String,
    pub order_preserving: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredDictionaryLookup {
    pub owner_kind: String,
    pub owner_key: String,
    pub column_name: String,
    pub dictionary_id: i64,
}

impl DictionaryMetaRepository {
    pub fn load_active(
        &self,
        txn: &dyn MetaReadTxn,
        owner_kind: &str,
        owner_key: &str,
        column_name: &str,
    ) -> RepositoryResult<Option<StoredDictionarySnapshot>> {
        let lookup_key = dictionary_lookup_key(owner_kind, owner_key, column_name);
        let Some(lookup_record) = txn.get(&lookup_key)? else {
            return Ok(None);
        };
        let lookup: StoredDictionaryLookup =
            decode_payload_for_kind(DICTIONARY_LOOKUP_KIND, &lookup_record.payload)?;
        let snapshot_key = dictionary_snapshot_key(lookup.dictionary_id);
        let Some(snapshot_record) = txn.get(&snapshot_key)? else {
            return Err(RepositoryError::provider(format!(
                "dictionary lookup points to missing snapshot id {}",
                lookup.dictionary_id
            )));
        };
        let snapshot: StoredDictionarySnapshot =
            decode_payload_for_kind(DICTIONARY_SNAPSHOT_KIND, &snapshot_record.payload)?;
        if snapshot.state == "ACTIVE" {
            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }

    pub fn upsert_snapshot(
        &self,
        txn: &mut dyn MetaWriteTxn,
        snapshot: &StoredDictionarySnapshot,
    ) -> RepositoryResult<()> {
        let snapshot_key = dictionary_snapshot_key(snapshot.dictionary_id);
        let lookup = StoredDictionaryLookup {
            owner_kind: snapshot.owner_kind.clone(),
            owner_key: snapshot.owner_key.clone(),
            column_name: snapshot.column_name.clone(),
            dictionary_id: snapshot.dictionary_id,
        };
        txn.put(MetaRecordPut {
            key: snapshot_key,
            kind: MetaRecordKind::new(DICTIONARY_SNAPSHOT_KIND),
            payload: encode_record_payload(DICTIONARY_SNAPSHOT_KIND, snapshot)?,
            expected: ExpectedRevision::Any,
        })?;
        txn.put(MetaRecordPut {
            key: dictionary_lookup_key(
                &snapshot.owner_kind,
                &snapshot.owner_key,
                &snapshot.column_name,
            ),
            kind: MetaRecordKind::new(DICTIONARY_LOOKUP_KIND),
            payload: encode_record_payload(DICTIONARY_LOOKUP_KIND, &lookup)?,
            expected: ExpectedRevision::Any,
        })?;
        Ok(())
    }

    pub fn mark_owner_stale(
        &self,
        txn: &mut dyn MetaWriteTxn,
        owner_kind: &str,
        owner_key: &str,
    ) -> RepositoryResult<()> {
        let prefix = dictionary_lookup_prefix(owner_kind, owner_key);
        for record in txn.scan_prefix(&prefix)? {
            let lookup: StoredDictionaryLookup =
                decode_payload_for_kind(DICTIONARY_LOOKUP_KIND, &record.payload)?;
            self.mark_snapshot_state(txn, lookup.dictionary_id, "STALE")?;
        }
        Ok(())
    }

    pub fn drop_owner(
        &self,
        txn: &mut dyn MetaWriteTxn,
        owner_kind: &str,
        owner_key: &str,
    ) -> RepositoryResult<()> {
        let prefix = dictionary_lookup_prefix(owner_kind, owner_key);
        for record in txn.scan_prefix(&prefix)? {
            let lookup: StoredDictionaryLookup =
                decode_payload_for_kind(DICTIONARY_LOOKUP_KIND, &record.payload)?;
            self.mark_snapshot_state(txn, lookup.dictionary_id, "DROPPED")?;
            txn.delete(record.key, ExpectedRevision::Any)?;
        }
        Ok(())
    }

    fn mark_snapshot_state(
        &self,
        txn: &mut dyn MetaWriteTxn,
        dictionary_id: i64,
        state: &str,
    ) -> RepositoryResult<()> {
        let snapshot_key = dictionary_snapshot_key(dictionary_id);
        let Some(record) = txn.get(&snapshot_key)? else {
            return Ok(());
        };
        let mut snapshot: StoredDictionarySnapshot =
            decode_payload_for_kind(DICTIONARY_SNAPSHOT_KIND, &record.payload)?;
        snapshot.state = state.to_string();
        txn.put(MetaRecordPut {
            key: snapshot_key,
            kind: MetaRecordKind::new(DICTIONARY_SNAPSHOT_KIND),
            payload: encode_record_payload(DICTIONARY_SNAPSHOT_KIND, &snapshot)?,
            expected: ExpectedRevision::Exact(record.revision),
        })?;
        Ok(())
    }
}

fn dictionary_snapshot_key(dictionary_id: i64) -> MetaKey {
    MetaKey::from_parts([NS_DICTIONARY, "snapshot", &dictionary_id.to_string()])
}

fn dictionary_lookup_key(owner_kind: &str, owner_key: &str, column_name: &str) -> MetaKey {
    MetaKey::from_parts([
        NS_DICTIONARY,
        "lookup",
        &normalize_lookup_name(owner_kind),
        &normalize_lookup_name(owner_key),
        &normalize_lookup_name(column_name),
    ])
}

fn dictionary_lookup_prefix(owner_kind: &str, owner_key: &str) -> MetaKeyPrefix {
    MetaKeyPrefix::from_parts([
        NS_DICTIONARY,
        "lookup",
        &normalize_lookup_name(owner_kind),
        &normalize_lookup_name(owner_key),
    ])
}
```

- [ ] **Step 4: Wire repository module**

In `src/meta/repository/mod.rs`, add:

```rust
pub mod dictionary;
```

In test seed support, add `dictionary.snapshot` and `dictionary.lookup` branches using `StoredDictionarySnapshot` and `StoredDictionaryLookup`.

- [ ] **Step 5: Add engine dictionary model**

Create `src/engine/dictionary/model.rs`:

```rust
use std::collections::BTreeMap;

use arrow::datatypes::DataType;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DictionaryOwner {
    StarRocksTable {
        database: String,
        table: String,
        db_id: i64,
        table_id: i64,
    },
    IcebergTable {
        catalog: String,
        namespace: String,
        table: String,
        table_uuid: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DictionaryWatermark {
    StarRocks {
        schema_id: i64,
        tablets: Vec<StarRocksTabletWatermark>,
    },
    Iceberg {
        snapshot_id: Option<i64>,
        schema_id: i32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksTabletWatermark {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) visible_version: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DictionaryState {
    Active,
    Stale,
    Dropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DictionaryValue {
    pub(crate) id: i32,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct DictionarySnapshot {
    pub(crate) dictionary_id: i64,
    pub(crate) owner: DictionaryOwner,
    pub(crate) column_id: Option<i64>,
    pub(crate) column_name: String,
    pub(crate) data_type: DataType,
    pub(crate) version: i64,
    pub(crate) watermark: DictionaryWatermark,
    pub(crate) values: Vec<DictionaryValue>,
    pub(crate) null_id: i32,
    pub(crate) state: DictionaryState,
    pub(crate) order_preserving: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct QueryDictionarySelection {
    pub(crate) base_dictionaries: BTreeMap<String, DictionarySnapshot>,
}
```

- [ ] **Step 6: Add manager skeleton and state field**

Create `src/engine/dictionary/mod.rs`:

```rust
pub(crate) mod maintenance;
pub(crate) mod model;
pub(crate) mod rebuild;

use std::sync::Arc;

use crate::engine::dictionary::model::{DictionaryOwner, DictionarySnapshot};
use crate::meta::repository::dictionary::DictionaryMetaRepository;

#[derive(Clone)]
pub(crate) struct DictionaryManager {
    repo: Arc<DictionaryMetaRepository>,
}

impl Default for DictionaryManager {
    fn default() -> Self {
        Self {
            repo: Arc::new(DictionaryMetaRepository::default()),
        }
    }
}

impl DictionaryManager {
    pub(crate) fn load_active_snapshot(
        &self,
        _state: &crate::engine::StandaloneState,
        _owner: &DictionaryOwner,
        _column_name: &str,
    ) -> Result<Option<DictionarySnapshot>, String> {
        Ok(None)
    }
}
```

Create empty compile units:

```rust
// src/engine/dictionary/rebuild.rs
pub(crate) fn module_loaded() {}
```

```rust
// src/engine/dictionary/maintenance.rs
pub(crate) fn module_loaded() {}
```

In `src/engine/mod.rs`, add:

```rust
pub(crate) mod dictionary;
```

and add to `StandaloneState`:

```rust
pub(crate) dictionary_manager: dictionary::DictionaryManager,
```

with default:

```rust
dictionary_manager: dictionary::DictionaryManager::default(),
```

- [ ] **Step 7: Verify repository skeleton**

Run:

```bash
cargo test -p novarocks --lib meta::avro meta::repository::dictionary engine::dictionary -- --nocapture
cargo check
```

Expected: tests and `cargo check` pass.

- [ ] **Step 8: Commit**

Run:

```bash
git add src/meta src/engine
git commit -m "feat: add dictionary metadata repository"
```

Expected: commit succeeds.

---

### Task 4: ANALYZE FULL Rebuild And Write Invalidation

**Files:**
- Modify: `src/engine/dictionary/model.rs`
- Modify: `src/engine/dictionary/mod.rs`
- Modify: `src/engine/dictionary/rebuild.rs`
- Modify: `src/engine/dictionary/maintenance.rs`
- Modify: `src/engine/statistics.rs`
- Modify: `src/engine/insert_flow.rs`
- Modify: `src/engine/iceberg_writer.rs`
- Modify: `src/engine/iceberg_truncate.rs`
- Modify: `src/engine/mutation_flow.rs`
- Modify: `src/connector/starrocks/managed/txn.rs`
- Modify: `src/engine/statement.rs`

- [ ] **Step 1: Add owner and watermark conversion helpers**

In `src/engine/dictionary/model.rs`, add these methods:

```rust
impl DictionaryOwner {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            DictionaryOwner::StarRocksTable { .. } => "starrocks_table",
            DictionaryOwner::IcebergTable { .. } => "iceberg_table",
        }
    }

    pub(crate) fn stable_key(&self) -> String {
        match self {
            DictionaryOwner::StarRocksTable {
                database,
                table,
                db_id,
                table_id,
            } => format!("db={database};table={table};db_id={db_id};table_id={table_id}"),
            DictionaryOwner::IcebergTable {
                catalog,
                namespace,
                table,
                table_uuid,
            } => format!(
                "catalog={catalog};namespace={namespace};table={table};uuid={}",
                table_uuid.as_deref().unwrap_or("")
            ),
        }
    }
}

impl DictionaryWatermark {
    pub(crate) fn stable_json(&self) -> String {
        serde_json::to_string(self).expect("dictionary watermark serializes")
    }
}
```

Add `Serialize` / `Deserialize` derives to `DictionaryWatermark` and `StarRocksTabletWatermark`.

- [ ] **Step 2: Implement `DictionaryManager::load_active_snapshot`**

Replace the skeleton with repository-backed loading. The method must:

1. Return `Ok(None)` when `state.metadata_provider` is `None`.
2. Start a read transaction.
3. Load Active snapshot by owner kind/key/column.
4. Convert stored values to `DictionarySnapshot`.
5. Reject non-string-compatible data types with this error:

```text
dictionary snapshot <id> has unsupported data type <type>
```

- [ ] **Step 3: Implement StarRocks ANALYZE FULL rebuild**

In `src/engine/dictionary/rebuild.rs`, add:

```rust
pub(crate) fn rebuild_for_analyze_full(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    database: &str,
    table: &str,
    columns: Option<&[String]>,
) -> Result<usize, String> {
    let catalog = state.catalog.read().expect("standalone catalog read lock");
    let table_def = catalog.get(database, table)?;
    let crate::sql::catalog::ScanSource::StarRocks = table_def.source else {
        return Ok(0);
    };
    drop(catalog);

    let layout = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .physical_layout(database, table)?;
    let selected = select_string_columns(&table_def, columns);
    let mut built = 0usize;
    for column in selected {
        let snapshot = collect_starrocks_column_dictionary(state, database, table, &table_def, &layout, column)?;
        state.dictionary_manager.upsert_snapshot(state, snapshot)?;
        built += 1;
    }
    Ok(built)
}
```

`select_string_columns` must only return `Utf8`, `LargeUtf8`, `Binary`, and `LargeBinary` columns. `collect_starrocks_column_dictionary` must execute a standalone query equivalent to:

```sql
SELECT DISTINCT `<column>` FROM `<database>`.`<table>` WHERE `<column>` IS NOT NULL ORDER BY `<column>`
```

and assign non-null ids from `1` in sorted output order. It must set `null_id = 0` and `order_preserving = true`.

- [ ] **Step 4: Implement Iceberg ANALYZE FULL rebuild**

In the same module, add `rebuild_iceberg_for_analyze_full` for `ScanSource::IcebergDataFiles { table, .. }`. Owner uses:

```rust
DictionaryOwner::IcebergTable {
    catalog: table.catalog.clone(),
    namespace: table.namespace.clone(),
    table: table.table.clone(),
    table_uuid: table.table_uuid.clone(),
}
```

Watermark uses:

```rust
DictionaryWatermark::Iceberg {
    snapshot_id: table.current_snapshot_id,
    schema_id: table.schema_id,
}
```

The distinct-value query is the same SQL shape as StarRocks after the Iceberg table has been registered into the local catalog.

- [ ] **Step 5: Call dictionary rebuild from ANALYZE FULL only**

In `src/engine/statistics.rs`, after existing statistics handling succeeds for an `ANALYZE FULL TABLE` statement, call:

```rust
let rebuilt = crate::engine::dictionary::rebuild::rebuild_for_analyze_full(
    state,
    &key.db,
    &key.table,
    analyze_column_list(sql)?.as_deref(),
)?;
tracing::debug!("rebuilt {rebuilt} dictionary snapshots for ANALYZE FULL");
```

For `ANALYZE SAMPLE TABLE` and plain `ANALYZE TABLE`, do not build dictionaries.

- [ ] **Step 6: Add write invalidation hooks**

In `src/engine/dictionary/maintenance.rs`, add:

```rust
pub(crate) fn mark_table_stale(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    owner: crate::engine::dictionary::model::DictionaryOwner,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("mark dictionary stale")
        .map_err(|e| format!("open dictionary stale transaction failed: {e}"))?;
    state
        .dictionary_manager
        .repo()
        .mark_owner_stale(txn.as_mut(), owner.kind(), &owner.stable_key())
        .map_err(|e| format!("mark dictionary stale failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit dictionary stale transaction failed: {e}"))?;
    Ok(())
}

pub(crate) fn drop_table_dictionaries(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    owner: crate::engine::dictionary::model::DictionaryOwner,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("drop dictionary metadata")
        .map_err(|e| format!("open dictionary drop transaction failed: {e}"))?;
    state
        .dictionary_manager
        .repo()
        .drop_owner(txn.as_mut(), owner.kind(), &owner.stable_key())
        .map_err(|e| format!("drop dictionary metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit dictionary drop transaction failed: {e}"))?;
    Ok(())
}
```

Expose `DictionaryManager::repo(&self) -> &DictionaryMetaRepository`.

- [ ] **Step 7: Attach invalidation to writes**

Use conservative invalidation:

- `INSERT` / `INSERT SELECT` / `UPDATE` / `MERGE`: mark target table stale after commit.
- `INSERT OVERWRITE` / `TRUNCATE`: mark stale after commit.
- `DELETE`: keep Active for delete-only commits only when the code path has a delete-only operation marker; otherwise mark stale.
- `DROP TABLE` / `DROP DATABASE`: drop dictionary metadata.

Call the hooks immediately after the existing data commit and before returning `StatementResult::Ok`.

- [ ] **Step 8: Verify dictionary maintenance**

Add these tests with complete bodies in `src/engine/dictionary/rebuild.rs` or `src/engine/statistics.rs`:

- `analyze_sample_does_not_build_dictionary`: create a temp metadata provider, create a StarRocks table with a string column, run `ANALYZE SAMPLE TABLE`, scan metadata prefix `dictionary`, and assert zero `dictionary.snapshot` records.
- `analyze_full_builds_active_string_dictionary`: create the same table, insert `('b'), ('a'), ('a')`, run `ANALYZE FULL TABLE`, load the active snapshot, and assert ids `1 -> a`, `2 -> b`, `null_id = 0`, `order_preserving = true`.
- `insert_marks_active_dictionary_stale`: build the active snapshot through `ANALYZE FULL TABLE`, insert one extra string value, load active snapshot again, and assert it returns `None`.

Run:

```bash
cargo test -p novarocks --lib engine::dictionary engine::statistics -- --nocapture
cargo check
```

Expected: tests and `cargo check` pass.

- [ ] **Step 9: Commit**

Run:

```bash
git add src/engine src/connector src/meta
git commit -m "feat: maintain standalone dictionary snapshots"
```

Expected: commit succeeds.

---

### Task 5: Logical And Physical Decode Operators

**Files:**
- Modify: `src/sql/planner/plan.rs`
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/optimizer/convert.rs`
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Modify: `src/sql/optimizer/derive/mod.rs`
- Modify: `src/sql/optimizer/derive/passthrough.rs`
- Modify: `src/sql/optimizer/extract.rs`
- Modify: `src/sql/explain.rs`

- [ ] **Step 1: Add logical decode plan node**

In `src/sql/planner/plan.rs`, add:

```rust
LogicalPlan::Decode(DecodeNode),
```

and:

```rust
#[derive(Clone, Debug)]
pub(crate) struct DecodeNode {
    pub input: Box<LogicalPlan>,
    pub mappings: Vec<DecodeMapping>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodeMapping {
    pub dict_column: String,
    pub string_column: String,
}
```

Add traversal cases anywhere `LogicalPlan` is exhaustively matched.

- [ ] **Step 2: Add optimizer operators**

In `src/sql/optimizer/operator.rs`, add:

```rust
#[derive(Clone, Debug)]
pub(crate) struct LogicalDecodeOp {
    pub mappings: Vec<crate::sql::planner::plan::DecodeMapping>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalDecodeOp {
    pub mappings: Vec<crate::sql::planner::plan::DecodeMapping>,
}
```

Add `LogicalDecode` and `PhysicalDecode` variants to `Operator`, and include them in `is_logical`.

- [ ] **Step 3: Convert logical decode to memo**

In `src/sql/optimizer/convert.rs`, add the `LogicalPlan::Decode` case:

```rust
LogicalPlan::Decode(node) => {
    let child = logical_plan_to_memo(&node.input, memo);
    let op = Operator::LogicalDecode(LogicalDecodeOp {
        mappings: node.mappings.clone(),
    });
    let expr = MExpr {
        id: memo.next_expr_id(),
        op,
        children: vec![child],
    };
    memo.new_group(expr)
}
```

- [ ] **Step 4: Add implementation rule**

In `src/sql/optimizer/cascades_rules/implement.rs`, add:

```rust
pub(crate) struct DecodeToPhysical;

impl Rule for DecodeToPhysical {
    fn name(&self) -> &'static str {
        "DecodeToPhysical"
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalDecode(_))
    }

    fn apply(&self, expr: &MExpr, _memo: &Memo) -> Vec<NewExpr> {
        let Operator::LogicalDecode(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalDecode(PhysicalDecodeOp {
                mappings: op.mappings.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}
```

Add `Box::new(DecodeToPhysical)` to `all_implementation_rules()`.

- [ ] **Step 5: Add property derivation**

In `src/sql/optimizer/derive/passthrough.rs`, include `PhysicalDecodeOp` in distribution-blind passthroughs:

```rust
passthrough_distribution_blind_impls!(
    PhysicalFilterOp,
    PhysicalProjectOp,
    PhysicalDecodeOp,
    PhysicalSubqueryAliasOp,
    PhysicalCTEProduceOp,
    PhysicalRepeatOp,
);
```

In `src/sql/optimizer/derive/mod.rs`, add dispatch arms for `Operator::PhysicalDecode`.

- [ ] **Step 6: Add explain output**

In `src/sql/explain.rs`, print decode nodes as:

```text
DECODE [dict_col->string_col]
```

and include the stable `Decode` marker line for compatibility with existing SQL golden checks.

- [ ] **Step 7: Verify operators**

Run:

```bash
cargo test -p novarocks --lib sql::optimizer::cascades_rules::implement sql::explain -- --nocapture
cargo check
```

Expected: tests and `cargo check` pass.

- [ ] **Step 8: Commit**

Run:

```bash
git add src/sql
git commit -m "feat: add decode plan operators"
```

Expected: commit succeeds.

---

### Task 6: Codegen Dictionary Plan Interface

**Files:**
- Modify: `src/sql/codegen/mod.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/sql/codegen/expr_compiler.rs`
- Modify: `src/runtime/coordinator.rs`

- [ ] **Step 1: Add fragment dictionary output fields**

In `src/sql/codegen/mod.rs`, extend `FragmentBuildResult`:

```rust
pub query_global_dicts: Option<Vec<crate::data::TGlobalDict>>,
pub query_global_dict_exprs: Option<std::collections::BTreeMap<i32, crate::exprs::TExpr>>,
```

Initialize these to `None` in all fragment builders until Task 7 populates them.

- [ ] **Step 2: Preserve dictionaries in coordinator**

In `src/runtime/coordinator.rs`, replace all hard-coded empty dictionary arguments such as `None::<Vec<crate::data::TGlobalDict>>` and `None // query_global_dicts` with the values from `FragmentBuildResult`.

The `TPlanFragment` construction must pass:

```rust
fragment.query_global_dicts.clone(),
fragment.load_global_dicts.clone(),
fragment.query_global_dict_exprs.clone(),
```

using the exact field names available in the generated `TPlanFragment` constructor.

- [ ] **Step 3: Add `build_decode_node`**

In `src/sql/codegen/nodes.rs`, add:

```rust
pub(crate) fn build_decode_node(
    node_id: i32,
    row_tuples: Vec<i32>,
    dict_id_to_string_ids: BTreeMap<i32, i32>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::DECODE_NODE;
    node.num_children = 1;
    node.limit = -1;
    node.row_tuples = row_tuples;
    node.nullable_tuples = vec![];
    node.compact_data = true;
    node.decode_node = Some(plan_nodes::TDecodeNode {
        dict_id_to_string_ids: Some(dict_id_to_string_ids),
        string_functions: None,
    });
    node
}
```

- [ ] **Step 4: Emit PhysicalDecode**

In `PlanFragmentBuilder::visit`, add `Operator::PhysicalDecode(op) => self.visit_decode(op, node)`.

Add:

```rust
fn visit_decode(
    &mut self,
    op: &PhysicalDecodeOp,
    node: &PhysicalPlanNode,
) -> Result<VisitResult, String> {
    let child = self.visit(&node.children[0])?;
    let mut mapping = BTreeMap::new();
    for item in &op.mappings {
        let dict = child
            .scope
            .resolve_unqualified(&item.dict_column)
            .ok_or_else(|| format!("decode dict column `{}` is not in child scope", item.dict_column))?;
        let string = child
            .scope
            .resolve_unqualified(&item.string_column)
            .ok_or_else(|| format!("decode string column `{}` is not in child scope", item.string_column))?;
        mapping.insert(dict.slot_id, string.slot_id);
    }
    let decode_node = nodes::build_decode_node(self.alloc_node(), child.tuple_ids.clone(), mapping);
    let mut plan_nodes = vec![decode_node];
    plan_nodes.extend(child.plan_nodes);
    Ok(VisitResult {
        plan_nodes,
        scope: child.scope,
        tuple_ids: child.tuple_ids,
        cte_exchange_nodes: child.cte_exchange_nodes,
    })
}
```

- [ ] **Step 5: Add scan dict slot emission**

Add a per-scan dictionary plan hint to `PhysicalScanOp`:

```rust
pub dict_columns: Vec<ScanDictionaryColumn>,
```

with:

```rust
#[derive(Clone, Debug)]
pub(crate) struct ScanDictionaryColumn {
    pub source_column: String,
    pub dict_column: String,
    pub dictionary: crate::engine::dictionary::model::DictionarySnapshot,
}
```

Update all constructors to pass `dict_columns: vec![]`.

In `visit_scan`, when a dict column is present:

1. Add a hidden slot named `dict_column`.
2. Use `DataType::Int32`.
3. Register it in `ExprScope`.
4. Push a `TGlobalDict` with `column_id = dict slot id`, `strings = values.bytes`, and `ids = values.id`.
5. Fill `TLakeScanNode.dict_string_id_to_int_ids` with `string_slot_id -> dict_slot_id` for StarRocks scans.

- [ ] **Step 6: Verify codegen**

Add unit tests in `src/sql/codegen/fragment_builder.rs`:

- `physical_decode_emits_decode_node`: build `PhysicalDecode` over a scan, call `PlanFragmentBuilder::build`, and assert the first plan node has `TPlanNodeType::DECODE_NODE` plus a non-empty `decode_node.dict_id_to_string_ids`.
- `scan_dict_column_emits_query_global_dict`: build a StarRocks scan with one `ScanDictionaryColumn`, call `PlanFragmentBuilder::build`, and assert the root fragment has one `query_global_dicts` entry whose `ids` are `[1, 2]` and whose `strings` are `[b"a", b"b"]`.

Run:

```bash
cargo test -p novarocks --lib sql::codegen::fragment_builder runtime::coordinator -- --nocapture
cargo check
```

Expected: tests and `cargo check` pass.

- [ ] **Step 7: Commit**

Run:

```bash
git add src/sql/codegen src/sql/optimizer src/runtime
git commit -m "feat: emit dictionary decode plans"
```

Expected: commit succeeds.

---

### Task 7: LowCardinalityDictionaryRewrite Rule

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/mod.rs`
- Create: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/collector.rs`
- Create: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/context.rs`
- Create: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/expr.rs`
- Create: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rewriter.rs`
- Create: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rule.rs`
- Modify: `src/sql/optimizer/rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`
- Modify: `src/sql/optimizer/rewrite/context.rs`
- Modify: `src/sql/optimizer/options.rs`

- [ ] **Step 1: Add rule module and registration**

In `src/sql/optimizer/rewrite/rules/mod.rs`, add:

```rust
pub(crate) mod low_cardinality_dict;

pub(crate) fn low_cardinality_dictionary_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(low_cardinality_dict::LowCardinalityDictionaryRewriteRule)]
}
```

In `src/sql/optimizer/rewrite/registry.rs`, add a new stage after `ColumnPruning`:

```rust
RewriteStage::new(
    "LowCardinalityDictionaryRewrite",
    RewritePhase::StructuralRewrite,
    rules::low_cardinality_dictionary_rules(),
),
```

Update registry tests to include `"LowCardinalityDictionaryRewrite"`.

- [ ] **Step 2: Put dictionary access into RewriteContext**

In `src/sql/optimizer/rewrite/context.rs`, add:

```rust
dictionary_provider: Option<Arc<dyn QueryDictionaryProvider>>,
```

Define the trait:

```rust
pub(crate) trait QueryDictionaryProvider: Send + Sync {
    fn load_active_snapshot(
        &self,
        table: &crate::sql::catalog::TableDef,
        database: &str,
        column_name: &str,
    ) -> Result<Option<crate::engine::dictionary::model::DictionarySnapshot>, String>;
}
```

Add setters/getters:

```rust
pub(crate) fn set_dictionary_provider(&mut self, provider: Arc<dyn QueryDictionaryProvider>) {
    self.dictionary_provider = Some(provider);
}

pub(crate) fn dictionary_provider(&self) -> Option<&Arc<dyn QueryDictionaryProvider>> {
    self.dictionary_provider.as_ref()
}
```

Create a provider implementation in `src/engine/dictionary/mod.rs` that wraps `Arc<StandaloneState>`.

- [ ] **Step 3: Pass dictionary provider from engine optimizer entry**

Change `crate::sql::optimizer::optimize` signature to accept:

```rust
dictionary_provider: Option<std::sync::Arc<dyn rewrite::context::QueryDictionaryProvider>>,
```

and set it on `rewrite_ctx` before `query_rewrite_pipeline`.

Update call sites:

- Normal standalone query and explain paths pass `Some(Arc::new(DictionaryQueryProvider::new(state.clone())))`.
- Rust optimizer unit tests pass `None`.
- IVM internal query paths pass the same provider when they use `StandaloneState`.

- [ ] **Step 4: Implement expression helpers**

In `expr.rs`, add helpers:

```rust
pub(crate) fn is_string_like(data_type: &arrow::datatypes::DataType) -> bool {
    matches!(
        data_type,
        arrow::datatypes::DataType::Utf8
            | arrow::datatypes::DataType::LargeUtf8
            | arrow::datatypes::DataType::Binary
            | arrow::datatypes::DataType::LargeBinary
    )
}

pub(crate) fn column_ref_name(expr: &TypedExpr) -> Option<&str> {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => Some(column.as_str()),
        _ => None,
    }
}

pub(crate) fn rewrite_column_ref(expr: &TypedExpr, mapping: &DictionaryRewriteContext) -> TypedExpr {
    if let Some(name) = column_ref_name(expr)
        && let Some(dict_col) = mapping.dict_column_for(name)
    {
        return TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: dict_col.to_string(),
                column_id: expr.column_id_or_unset(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: expr.nullable,
        };
    }
    expr.clone()
}
```

Add `TypedExpr::column_id_or_unset()` only if no equivalent helper exists.

- [ ] **Step 5: Implement collector**

In `collector.rs`, implement a bottom-up scan over `LogicalPlan` that:

1. For every `Scan`, asks the provider for Active snapshots for string-like columns.
2. Records eligible `database.table.column -> DictionarySnapshot`.
3. Marks `Aggregate` group-by string columns eligible for dict rewrite.
4. Marks `Sort` / `TopN` string columns as requiring decode unless the selected snapshot has `order_preserving = true`.
5. Marks `Join` string equality keys eligible only when both sides use the same snapshot stable owner key and version.
6. Marks `Union DISTINCT`, `Intersect`, `Except`, `Window`, and `TableFunction` as decode boundaries.

The public entry must be:

```rust
pub(crate) fn collect(
    plan: &LogicalPlan,
    ctx: &RewriteContext,
) -> Result<DictionaryRewriteContext, String>
```

- [ ] **Step 6: Implement rewrite context**

In `context.rs`, store mappings:

```rust
#[derive(Clone, Debug, Default)]
pub(crate) struct DictionaryRewriteContext {
    scan_columns: BTreeMap<ScanColumnKey, DictionarySnapshot>,
    string_to_dict_column: BTreeMap<String, String>,
    decode_boundaries: BTreeSet<String>,
    changed: bool,
}
```

Provide methods:

```rust
pub(crate) fn dict_column_name(table: &str, column: &str) -> String {
    format!("__nr_dict_{}_{}", table.to_ascii_lowercase(), column.to_ascii_lowercase())
}

pub(crate) fn dict_column_for(&self, column: &str) -> Option<&str>
pub(crate) fn mark_changed(&mut self)
pub(crate) fn changed(&self) -> bool
```

- [ ] **Step 7: Implement scan and aggregate rewrite**

In `rewriter.rs`, rewrite:

- `LogicalPlan::Scan`: append hidden dict `OutputColumn`s and populate `ScanNode` dictionary metadata for codegen.
- `LogicalPlan::Aggregate`: replace eligible string group-by column refs with dict column refs and add a `Decode` above the aggregate when output columns require the string column.
- `LogicalPlan::Project`: pass through dict columns needed by parents and decode before final user-facing projection.
- `LogicalPlan::Sort`: decode before sort when `order_preserving = false`; otherwise sort on dict id.
- `LogicalPlan::Limit`: pass through without changing dictionary semantics.

Use this public entry:

```rust
pub(crate) fn rewrite(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String>
```

- [ ] **Step 8: Implement the rule**

In `rule.rs`:

```rust
pub(crate) struct LowCardinalityDictionaryRewriteRule;

impl LogicalRewriteRule for LowCardinalityDictionaryRewriteRule {
    fn name(&self) -> &'static str {
        "LowCardinalityDictionaryRewrite"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, ctx: &RewriteContext) -> bool {
        ctx.dictionary_provider().is_some() && contains_scan(plan)
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let mut dict_ctx = collector::collect(&plan, ctx)?;
        let rewritten = rewriter::rewrite(plan, &mut dict_ctx)?;
        if dict_ctx.changed() {
            Ok(RewriteResult::Changed(rewritten))
        } else {
            Ok(RewriteResult::Unchanged)
        }
    }
}
```

- [ ] **Step 9: Verify rewrite unit tests**

Add tests in `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rule.rs`:

- `group_by_string_rewrites_to_dict_column_and_decode`: build `Scan -> Aggregate` over a `Utf8` column with a fake provider returning an Active snapshot; assert the rewritten plan contains a dict group key and a `Decode` boundary before user output.
- `topn_non_order_preserving_decodes_before_sort`: use a fake snapshot with `order_preserving = false`; assert `Decode` is below `Sort` / `TopN`.
- `disable_rule_skips_dictionary_rewrite`: put `LowCardinalityDictionaryRewrite` in disabled rules; assert the plan debug string is identical before and after the pipeline.

Run:

```bash
cargo test -p novarocks --lib sql::optimizer::rewrite::rules::low_cardinality_dict sql::optimizer::rewrite::registry -- --nocapture
cargo check
```

Expected: tests and `cargo check` pass.

- [ ] **Step 10: Commit**

Run:

```bash
git add src/sql/optimizer src/sql/planner src/engine
git commit -m "feat: rewrite low-cardinality dictionary plans"
```

Expected: commit succeeds.

---

### Task 8: Join, Union, CTE, And Derived Dictionary Completion

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/collector.rs`
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/context.rs`
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/expr.rs`
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rewriter.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/expr_compiler.rs`

- [ ] **Step 1: Support same-dictionary joins**

For hash-join equality predicates, rewrite both sides to dict columns only when:

```rust
left_snapshot.owner.stable_key() == right_snapshot.owner.stable_key()
    && left_snapshot.version == right_snapshot.version
    && left_snapshot.column_name.eq_ignore_ascii_case(&right_snapshot.column_name)
```

Otherwise insert decode before the join key expression.

- [ ] **Step 2: Support UNION ALL dictionary propagation**

For `UNION ALL`, preserve dictionary columns only when every input has the same snapshot owner/version/column. When any input differs, insert decode on all inputs before the union.

For `UNION DISTINCT`, `INTERSECT`, and `EXCEPT`, always decode before the set operation.

- [ ] **Step 3: Support CTE producer and consumer**

For a CTE with one consumer, inline cleanup may remove it before dictionary rewrite. For remaining multi-consumer CTEs:

- Producer output must include dict columns only if all consumers use the same mapping.
- If consumers diverge, insert decode at producer output.
- `CTEConsume` must not invent a new dictionary mapping.

- [ ] **Step 4: Add derived dictionary expressions**

Allow a single string column function when it is deterministic and in this allowlist:

```rust
const DERIVED_DICT_FUNCTIONS: &[&str] = &[
    "upper",
    "lower",
    "trim",
    "ltrim",
    "rtrim",
];
```

For a derived expression, allocate a query-local dict column and store its source expression in `query_global_dict_exprs`. The generated thrift expression root must be `TExprNodeType::DICT_EXPR` with child 1 as the base dict slot and child 2 as the mapped expression, matching `lower/node/decode.rs`.

- [ ] **Step 5: Preserve aggregate function semantics**

Allow aggregate functions:

```rust
const DICT_AGG_FUNCTIONS: &[&str] = &[
    "count",
    "min",
    "max",
    "any_value",
    "array_agg",
    "approx_count_distinct",
];
```

Rules:

- `COUNT(*)` stays unchanged.
- `COUNT(col)` may consume dict id.
- `MIN` / `MAX` may consume dict id only when `order_preserving = true`; otherwise decode before aggregate.
- Ordered aggregate calls with `ORDER BY` decode unless the order expression is order-preserving.

- [ ] **Step 6: Verify completion tests**

Add tests:

- `same_dictionary_join_uses_dict_keys`: two scans share the same fake snapshot; assert the join equality predicate references the dict column on both sides.
- `different_dictionary_join_decodes_keys`: two fake snapshots use different versions; assert the join input contains decode boundaries before equality comparison.
- `union_all_same_dictionary_preserves_dict`: two `UNION ALL` inputs share the same snapshot; assert the union output carries the dict column mapping.
- `derived_upper_emits_query_global_dict_expr`: project `upper(string_col)`, build fragments, and assert `query_global_dict_exprs` contains one `DICT_EXPR`.

Run:

```bash
cargo test -p novarocks --lib sql::optimizer::rewrite::rules::low_cardinality_dict sql::codegen::fragment_builder -- --nocapture
cargo check
```

Expected: tests and `cargo check` pass.

- [ ] **Step 7: Commit**

Run:

```bash
git add src/sql src/engine
git commit -m "feat: complete dictionary rewrite coverage"
```

Expected: commit succeeds.

---

### Task 9: SQL Regression And Golden Updates

**Files:**
- Add: `sql-tests/optimizer/sql/low_cardinality_dict_rewrite.sql`
- Add: `sql-tests/optimizer/sql/low_cardinality_dict_disabled.sql`
- Add: `sql-tests/optimizer/sql/low_cardinality_dict_stale.sql`
- Modify: `sql-tests/aggregate/result/agg_test_agg_compressed_key.result`
- Modify: `sql-tests/aggregate/result/agg_test_agg_with_limit.result`
- Modify: any SQL test setup files changed by StarRocks naming cleanup.

- [ ] **Step 1: Add optimizer rewrite SQL coverage**

Create `sql-tests/optimizer/sql/low_cardinality_dict_rewrite.sql`:

```sql
-- @tags=optimizer,dictionary
DROP TABLE IF EXISTS ${case_db}.dict_rewrite_t;
CREATE TABLE ${case_db}.dict_rewrite_t (
  k INT,
  s STRING,
  v INT
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num' = '1');
INSERT INTO ${case_db}.dict_rewrite_t VALUES
  (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'c', 40);
ANALYZE FULL TABLE ${case_db}.dict_rewrite_t;
-- @result_contains=Decode
-- @skip_result_check=true
EXPLAIN COSTS SELECT DISTINCT s FROM ${case_db}.dict_rewrite_t;
SELECT s, SUM(v) FROM ${case_db}.dict_rewrite_t GROUP BY s ORDER BY s;
```

- [ ] **Step 2: Add rule-disable coverage**

Create `sql-tests/optimizer/sql/low_cardinality_dict_disabled.sql`:

```sql
-- @tags=optimizer,dictionary
DROP TABLE IF EXISTS ${case_db}.dict_disabled_t;
CREATE TABLE ${case_db}.dict_disabled_t (
  k INT,
  s STRING
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num' = '1');
INSERT INTO ${case_db}.dict_disabled_t VALUES (1, 'a'), (2, 'b'), (3, 'a');
ANALYZE FULL TABLE ${case_db}.dict_disabled_t;
SET disable_optimizer_rules = 'LowCardinalityDictionaryRewrite';
-- @result_not_contains=Decode
-- @skip_result_check=true
EXPLAIN COSTS SELECT DISTINCT s FROM ${case_db}.dict_disabled_t;
SET disable_optimizer_rules = '';
```

- [ ] **Step 3: Add stale dictionary coverage**

Create `sql-tests/optimizer/sql/low_cardinality_dict_stale.sql`:

```sql
-- @tags=optimizer,dictionary
DROP TABLE IF EXISTS ${case_db}.dict_stale_t;
CREATE TABLE ${case_db}.dict_stale_t (
  k INT,
  s STRING
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num' = '1');
INSERT INTO ${case_db}.dict_stale_t VALUES (1, 'a'), (2, 'b');
ANALYZE FULL TABLE ${case_db}.dict_stale_t;
INSERT INTO ${case_db}.dict_stale_t VALUES (3, 'c');
-- @result_not_contains=Decode
-- @skip_result_check=true
EXPLAIN COSTS SELECT DISTINCT s FROM ${case_db}.dict_stale_t;
SELECT DISTINCT s FROM ${case_db}.dict_stale_t ORDER BY s;
```

- [ ] **Step 4: Run targeted SQL verification**

Start the standalone server through the worktree environment:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

In another shell:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only low_cardinality_dict_rewrite,low_cardinality_dict_disabled,low_cardinality_dict_stale --mode verify
```

Expected: all three optimizer cases pass.

- [ ] **Step 5: Run failing aggregate cases**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite aggregate --only agg_test_agg_compressed_key,agg_test_agg_with_limit --mode verify
```

Expected: both cases pass. If output values are correct but golden files differ because dictionary rewrite changes stable plan text or row formatting, record only these two cases:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite aggregate --only agg_test_agg_compressed_key,agg_test_agg_with_limit --mode record
```

- [ ] **Step 6: Run broader validation**

Run:

```bash
cargo fmt --check
cargo test -p novarocks --lib sql::optimizer::rewrite::rules::low_cardinality_dict engine::dictionary sql::codegen::fragment_builder -- --nocapture
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite aggregate --only agg_test_agg_compressed_key,agg_test_agg_with_limit --mode verify
```

Expected: all commands pass.

- [ ] **Step 7: Commit**

Run:

```bash
git add sql-tests src
git commit -m "test: cover low-cardinality dictionary rewrite"
```

Expected: commit succeeds.

---

## Final Verification

Run:

```bash
cargo fmt --check
cargo check
cargo test -p novarocks --lib engine::dictionary sql::optimizer::rewrite::rules::low_cardinality_dict sql::codegen::fragment_builder -- --nocapture
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite aggregate --only agg_test_agg_compressed_key,agg_test_agg_with_limit --mode verify
```

Expected: every command exits with status 0.

## Self-Review

Spec coverage:

- Dictionary owner is real StarRocks/Iceberg table identity: Tasks 1, 3, 4, 7.
- No no-identity local/preloaded file scan path remains: Task 1.
- Managed-lake naming is removed from active backend and metadata names: Task 2.
- `ScanSource::S3ParquetFiles` is removed and replaced with Iceberg-specific variants: Task 1.
- Dictionary maintenance is defined through `DictionaryManager`, `ANALYZE FULL`, and write invalidation: Tasks 3 and 4.
- Query-local derived dictionaries do not persist to metadata: Task 8.
- Rewrite runs after column pruning and can be disabled by rule name: Task 7.
- Logical/physical decode and thrift emission are handled by optimizer/codegen, not execution fallback: Tasks 5 and 6.
- Join, union, CTE, sort/topN, aggregate, and project behavior are covered: Tasks 7 and 8.
- SQL regressions for `agg_test_agg_compressed_key.sql` and `agg_test_agg_with_limit.sql` are explicit gates: Task 9.

Placeholder scan:

```bash
rg -n "TBD|TODO|FIXME|implement later|fill in|appropriate error handling|Write tests for the above|Similar to Task" \
  docs/design/plans/2026-05-26-low-cardinality-dictionary-rewrite.md | rg -v "rg -n"
```

Expected: no matches.

Type consistency:

- `DictionaryOwner`, `DictionaryWatermark`, and `DictionarySnapshot` are introduced before rewrite/codegen tasks consume them.
- `ScanSource::IcebergDataFiles` always carries `IcebergTableInfo`.
- `LogicalDecodeOp` and `PhysicalDecodeOp` use `DecodeMapping`.
- `PhysicalScanOp::dict_columns` uses `ScanDictionaryColumn` and codegen emits `TGlobalDict` from the same `DictionarySnapshot`.
