# Iceberg v3 Row Lineage + Puffin DV Phase 2a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support `INSERT INTO`, `INSERT OVERWRITE`, and `DELETE` for `write.row-lineage=true` Iceberg v3 tables, with DELETE writing Puffin deletion vectors.

**Architecture:** Preserve the Phase 1 `IcebergCommitAction` boundary. INSERT keeps using `FastAppendCommit`; OVERWRITE adds v3 row-lineage row-range handling to the custom `OverwriteCommit`; DELETE routes row-lineage tables to a new `RowDeltaDvCommit` that consumes grouped `(data_file, pos)` values, merges existing DV blobs, writes new Puffin DV files, and rewrites touched delete manifests.

**Tech Stack:** Rust 2024, iceberg-rust 0.9 vendored patch, Arrow, OpenDAL `FileIO`, `roaring`, `crc32fast`, NovaRocks standalone integration tests.

---

## File Structure

| Path | Responsibility |
|---|---|
| `Cargo.toml` | Add `crc32fast` for Iceberg `deletion-vector-v1` CRC-32 payload checksums. |
| `src/connector/iceberg/commit/validation.rs` | Replace row-lineage rejection with write-mode classification and shared variant validation. |
| `src/connector/iceberg/commit/types.rs` | Add `CommitOpKind::RowDeltaDv` and `IcebergWriteMode`. |
| `src/connector/iceberg/commit/collector.rs` | Add a channel for grouped DELETE positions used by DV commit actions. |
| `src/connector/iceberg/commit/helpers.rs` | Let v3 manifest-list writing receive `first_row_id` and return final writer `next_row_id`. |
| `src/connector/iceberg/commit/overwrite.rs` | Add row-lineage row-range and manifest-list first-row-id handling. |
| `src/connector/iceberg/commit/puffin_dv.rs` | Encode/decode Iceberg `deletion-vector-v1` payloads and write/read single-blob Puffin files. |
| `src/connector/iceberg/commit/row_delta_dv.rs` | New RowDelta-with-DV commit action: validate old deletes, merge DV, write Puffin files, rewrite touched delete manifests. |
| `src/connector/iceberg/commit/run.rs` | Dispatch `CommitOpKind::RowDeltaDv`. |
| `src/connector/iceberg/commit/mod.rs` | Export new modules and validation helpers. |
| `src/engine/iceberg_writer.rs` | Use write-mode validation for INSERT/OVERWRITE. |
| `src/engine/delete_flow.rs` | Route row-lineage DELETE to `RowDeltaDvCommit`; keep legacy position-delete path unchanged. |
| `src/engine/mod.rs` | Add standalone integration coverage for row-lineage INSERT/OVERWRITE/DELETE and legacy-path preservation. |

---

### Task 1: Add Write-Mode Classification

**Files:**
- Modify: `src/connector/iceberg/commit/types.rs`
- Modify: `src/connector/iceberg/commit/validation.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Write failing tests for property parsing**

Add these tests to the existing `#[cfg(test)] mod tests` in `src/connector/iceberg/commit/validation.rs`:

```rust
#[test]
fn row_lineage_property_parser_accepts_true_case_insensitive() {
    let mut props = std::collections::HashMap::new();
    props.insert("write.row-lineage".to_string(), "TrUe".to_string());
    assert!(row_lineage_property_enabled(&props));
}

#[test]
fn row_lineage_property_parser_treats_missing_or_false_as_legacy() {
    let props = std::collections::HashMap::<String, String>::new();
    assert!(!row_lineage_property_enabled(&props));

    let mut props = std::collections::HashMap::new();
    props.insert("write.row-lineage".to_string(), "false".to_string());
    assert!(!row_lineage_property_enabled(&props));
}
```

- [ ] **Step 2: Run the validation tests and verify failure**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::commit::validation -- --nocapture
```

Expected: FAIL because `row_lineage_property_enabled` does not exist.

- [ ] **Step 3: Add the mode enum**

In `src/connector/iceberg/commit/types.rs`, extend the existing types:

```rust
/// Selects which write semantics apply to a target Iceberg table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergWriteMode {
    /// Phase 1 behavior: data files plus v2-compatible position-delete files.
    LegacyPositionDeletes,
    /// Iceberg v3 row-lineage behavior: data files carry row-range metadata;
    /// DELETE writes Puffin deletion vectors.
    RowLineageV3,
}
```

- [ ] **Step 4: Replace the row-lineage rejection helper**

In `src/connector/iceberg/commit/validation.rs`, keep `type_contains_variant` and replace `ensure_v3_writable` with:

```rust
use std::collections::HashMap;

use super::types::IcebergWriteMode;

pub fn row_lineage_property_enabled(props: &HashMap<String, String>) -> bool {
    props
        .get("write.row-lineage")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub fn classify_iceberg_write_mode(table: &Table) -> IcebergWriteMode {
    if row_lineage_property_enabled(table.metadata().properties()) {
        IcebergWriteMode::RowLineageV3
    } else {
        IcebergWriteMode::LegacyPositionDeletes
    }
}

pub fn ensure_iceberg_write_supported(table: &Table) -> Result<IcebergWriteMode, String> {
    let schema = table.metadata().current_schema();
    for f in schema.as_struct().fields() {
        if type_contains_variant(&f.field_type) {
            return Err(format!(
                "iceberg table column `{}` contains variant type; NovaRocks does not support writing variant columns.",
                f.name
            ));
        }
    }
    Ok(classify_iceberg_write_mode(table))
}
```

- [ ] **Step 5: Export the new helpers**

In `src/connector/iceberg/commit/mod.rs`, change the validation export to:

```rust
pub use types::{CommitOpKind, CommitOutcome, IcebergWriteMode, WrittenFile};
pub use validation::{
    classify_iceberg_write_mode, ensure_iceberg_write_supported, ensure_no_equality_deletes,
    ensure_single_partition_spec, match_select_schema_to_table, row_lineage_property_enabled,
};
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::commit::validation -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/validation.rs src/connector/iceberg/commit/mod.rs
git commit -m "feat: classify iceberg row lineage writes"
```

---

### Task 2: Route Engine Flows by Write Mode

**Files:**
- Modify: `src/engine/iceberg_writer.rs`
- Modify: `src/engine/delete_flow.rs`

- [ ] **Step 1: Write a failing legacy preservation assertion**

In `src/engine/mod.rs`, add this test next to the existing Iceberg DELETE tests:

```rust
#[test]
fn iceberg_legacy_delete_still_uses_position_delete_path() {
    let warehouse = TempDir::new().expect("warehouse");
    let (engine, session) = open_iceberg_session_with_table(&warehouse, "3");
    session
        .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
        .expect("seed");
    session
        .execute_in_database("delete from ice.db1.t where id = 1", "default")
        .expect("legacy delete");
    let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
    assert!(snap_after.is_some(), "legacy DELETE must still commit");
}
```

- [ ] **Step 2: Run the existing Iceberg tests**

Run:

```bash
cargo test -p novarocks --lib engine::tests::iceberg_ -- --nocapture
```

Expected: FAIL until imports are adjusted away from `ensure_v3_writable`.

- [ ] **Step 3: Update INSERT/OVERWRITE validation**

In `src/engine/iceberg_writer.rs`, change the imports:

```rust
use crate::connector::iceberg::commit::{
    CommitOpKind, IcebergCommitCollector, RunInput, WrittenFile, ensure_iceberg_write_supported,
    ensure_no_equality_deletes, ensure_single_partition_spec, run_iceberg_commit,
};
```

Replace:

```rust
ensure_v3_writable(&table)?;
ensure_single_partition_spec(&table)?;
if overwrite {
    ensure_no_equality_deletes(&table)?;
}
```

with:

```rust
let _write_mode = ensure_iceberg_write_supported(&table)?;
ensure_single_partition_spec(&table)?;
if overwrite {
    ensure_no_equality_deletes(&table)?;
}
```

- [ ] **Step 4: Update DELETE validation but keep legacy behavior**

In `src/engine/delete_flow.rs`, change the imports:

```rust
use crate::connector::iceberg::commit::{
    CommitOpKind, IcebergCommitCollector, IcebergWriteMode, PositionDeleteGroup, RunInput,
    ensure_iceberg_write_supported, ensure_no_equality_deletes, ensure_single_partition_spec,
    run_iceberg_commit, write_position_delete_files,
};
```

Replace:

```rust
ensure_v3_writable(&table)?;
ensure_single_partition_spec(&table)?;
ensure_no_equality_deletes(&table)?;
```

with:

```rust
let write_mode = ensure_iceberg_write_supported(&table)?;
ensure_single_partition_spec(&table)?;
ensure_no_equality_deletes(&table)?;
```

Keep the current position-delete implementation under:

```rust
match write_mode {
    IcebergWriteMode::LegacyPositionDeletes => {
        // Existing Phase 1 position-delete code remains here.
    }
    IcebergWriteMode::RowLineageV3 => {
        return Err("row-lineage DELETE selected the Puffin deletion-vector path before RowDeltaDvCommit was added".to_string());
    }
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p novarocks --lib engine::tests::iceberg_ -- --nocapture
```

Expected: PASS for legacy tests; row-lineage DELETE is not tested until Task 7 wires `RowDeltaDvCommit`.

- [ ] **Step 6: Commit**

```bash
git add src/engine/iceberg_writer.rs src/engine/delete_flow.rs src/engine/mod.rs
git commit -m "feat: route iceberg writes by row lineage mode"
```

---

### Task 3: Add Row-Lineage Metadata to OverwriteCommit

**Files:**
- Modify: `src/connector/iceberg/commit/helpers.rs`
- Modify: `src/connector/iceberg/commit/overwrite.rs`
- Test: `src/engine/mod.rs`

- [ ] **Step 1: Add failing integration tests for row-lineage INSERT and OVERWRITE**

In `src/engine/mod.rs`, add a helper:

```rust
fn open_row_lineage_iceberg_session_with_table(
    warehouse: &TempDir,
) -> (StandaloneNovaRocks, StandaloneSession) {
    let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
    let session = engine.session();
    let create_catalog_sql = format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    );
    session
        .execute_in_database(&create_catalog_sql, "default")
        .expect("create catalog");
    session
        .execute_in_database("create database ice.db1", "default")
        .expect("create database");
    session
        .execute_in_database(
            r#"create table ice.db1.t (id int, v string) tblproperties("format-version"="3","write.row-lineage"="true")"#,
            "default",
        )
        .expect("create row lineage table");
    (engine, session)
}

fn current_iceberg_row_lineage(
    engine: &StandaloneNovaRocks,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> (u64, Option<u64>, Option<u64>) {
    let registry = engine.inner.iceberg_catalogs.read().expect("registry");
    let entry = registry.get(catalog).expect("entry");
    entry.invalidate_table_cache(namespace, table);
    let loaded =
        crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
    let metadata = loaded.table.metadata();
    let snapshot = metadata.current_snapshot().expect("current snapshot");
    (
        metadata.next_row_id(),
        snapshot.first_row_id(),
        snapshot.added_rows_count(),
    )
}
```

Add tests:

```rust
#[test]
fn iceberg_row_lineage_insert_select_advances_next_row_id() {
    let warehouse = TempDir::new().expect("warehouse");
    let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
    session
        .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
        .expect("seed");
    session
        .execute_in_database(
            "insert into ice.db1.t select id, upper(v) from ice.db1.t where id <= 2",
            "default",
        )
        .expect("insert select");
    let (next_row_id, first_row_id, added_rows) =
        current_iceberg_row_lineage(&engine, "ice", "db1", "t");
    assert_eq!(first_row_id, Some(2));
    assert_eq!(added_rows, Some(2));
    assert_eq!(next_row_id, 4);
}

#[test]
fn iceberg_row_lineage_overwrite_writes_row_range() {
    let warehouse = TempDir::new().expect("warehouse");
    let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
    session
        .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'c')", "default")
        .expect("seed");
    session
        .execute_in_database(
            "insert overwrite ice.db1.t select id, upper(v) from ice.db1.t where id <= 2",
            "default",
        )
        .expect("overwrite");
    let (next_row_id, first_row_id, added_rows) =
        current_iceberg_row_lineage(&engine, "ice", "db1", "t");
    assert_eq!(first_row_id, Some(3));
    assert_eq!(added_rows, Some(2));
    assert_eq!(next_row_id, 5);
}
```

- [ ] **Step 2: Run tests and verify OVERWRITE failure**

Run:

```bash
cargo test -p novarocks --lib engine::tests::iceberg_row_lineage_ -- --nocapture
```

Expected: INSERT may pass through FastAppend; OVERWRITE fails because `OverwriteCommit` does not set v3 row-range.

- [ ] **Step 3: Update manifest-list helper**

In `src/connector/iceberg/commit/helpers.rs`, change `write_manifest_list` signature to:

```rust
pub async fn write_manifest_list(
    file_io: &FileIO,
    out_path: &str,
    entries: Vec<ManifestFile>,
    snap_id: i64,
    parent_snap_id: Option<i64>,
    sequence_number: i64,
    format_version: FormatVersion,
    first_row_id: Option<u64>,
) -> Result<Option<u64>, String>
```

For `FormatVersion::V3`, construct:

```rust
ManifestListWriter::v3(output, snap_id, parent_snap_id, sequence_number, first_row_id)
```

After `add_manifests`, capture:

```rust
let next_row_id = writer.next_row_id();
writer.close().await.map_err(|e| format!("ManifestListWriter::close failed: {e}"))?;
Ok(next_row_id)
```

Update existing non-row-lineage callers to pass `None` and ignore the returned value:

```rust
let _ = write_manifest_list(..., None).await?;
```

- [ ] **Step 4: Pass row-lineage state through OverwriteTxnAction**

In `src/connector/iceberg/commit/overwrite.rs`, add fields:

```rust
row_lineage_first_row_id: Option<u64>,
row_lineage_added_rows: u64,
```

In `OverwriteCommit::commit`, compute:

```rust
let row_lineage_first_row_id = match crate::connector::iceberg::commit::classify_iceberg_write_mode(ctx.table) {
    crate::connector::iceberg::commit::IcebergWriteMode::RowLineageV3 => Some(ctx.table.metadata().next_row_id()),
    crate::connector::iceberg::commit::IcebergWriteMode::LegacyPositionDeletes => None,
};
let row_lineage_added_rows = written.iter().map(|f| f.record_count).sum();
```

Populate the new `OverwriteTxnAction` fields.

- [ ] **Step 5: Write row-range in Overwrite snapshot**

In `OverwriteTxnAction::commit`, pass `self.row_lineage_first_row_id` into `write_manifest_list`. Build the snapshot in two phases:

```rust
let snapshot_builder = Snapshot::builder()
    .with_snapshot_id(new_snapshot_id)
    .with_parent_snapshot_id(parent_snapshot_id)
    .with_sequence_number(new_seq)
    .with_timestamp_ms(now_ms())
    .with_manifest_list(manifest_list_path)
    .with_summary(Summary {
        operation: Operation::Overwrite,
        additional_properties: overwrite_summary(&self.written, &existing),
    })
    .with_schema_id(self.schema_id);

let snapshot = if let Some(first_row_id) = self.row_lineage_first_row_id {
    snapshot_builder
        .with_row_range(first_row_id, self.row_lineage_added_rows)
        .build()
} else {
    snapshot_builder.build()
};
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p novarocks --lib engine::tests::iceberg_row_lineage_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/commit/helpers.rs src/connector/iceberg/commit/overwrite.rs src/engine/mod.rs
git commit -m "feat: write row lineage metadata for iceberg overwrite"
```

---

### Task 4: Implement Iceberg Deletion Vector Payload Codec

**Files:**
- Modify: `Cargo.toml`
- Create: `src/connector/iceberg/commit/puffin_dv.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Add failing codec tests**

Create `src/connector/iceberg/commit/puffin_dv.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deletion_vector_round_trips_32_and_64_bit_positions() {
        let mut dv = DeletionVector::default();
        dv.insert(0).unwrap();
        dv.insert(7).unwrap();
        dv.insert(u32::MAX as u64 + 3).unwrap();
        let bytes = dv.to_iceberg_payload().unwrap();
        let decoded = DeletionVector::from_iceberg_payload(&bytes).unwrap();
        assert!(decoded.contains(0));
        assert!(decoded.contains(7));
        assert!(decoded.contains(u32::MAX as u64 + 3));
        assert_eq!(decoded.cardinality(), 3);
    }

    #[test]
    fn deletion_vector_rejects_high_bit_positions() {
        let mut dv = DeletionVector::default();
        let err = dv.insert(1u64 << 63).unwrap_err();
        assert!(err.contains("positive 64-bit"));
    }
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::commit::puffin_dv -- --nocapture
```

Expected: FAIL because the module is not wired and types do not exist.

- [ ] **Step 3: Add dependency**

In `Cargo.toml`, under the existing bitmap/checksum dependencies:

```toml
crc32fast = "1"
```

- [ ] **Step 4: Implement `DeletionVector`**

Add:

```rust
use std::collections::BTreeMap;
use std::io::Cursor;

use roaring::RoaringBitmap;

const DV_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeletionVector {
    bitmaps: BTreeMap<u32, RoaringBitmap>,
    cardinality: u64,
}

impl DeletionVector {
    pub fn insert(&mut self, position: u64) -> Result<(), String> {
        if position >= (1u64 << 63) {
            return Err(format!("deletion vector position {position} is not a positive 64-bit position"));
        }
        let key = (position >> 32) as u32;
        let sub_position = position as u32;
        let bitmap = self.bitmaps.entry(key).or_default();
        if bitmap.insert(sub_position) {
            self.cardinality += 1;
        }
        Ok(())
    }

    pub fn merge(&mut self, other: &DeletionVector) {
        for (key, rhs) in &other.bitmaps {
            let bitmap = self.bitmaps.entry(*key).or_default();
            let before = bitmap.len();
            *bitmap |= rhs;
            self.cardinality += bitmap.len().saturating_sub(before);
        }
    }

    pub fn contains(&self, position: u64) -> bool {
        if position >= (1u64 << 63) {
            return false;
        }
        let key = (position >> 32) as u32;
        let sub_position = position as u32;
        self.bitmaps
            .get(&key)
            .map(|bitmap| bitmap.contains(sub_position))
            .unwrap_or(false)
    }

    pub fn cardinality(&self) -> u64 {
        self.cardinality
    }

    pub fn is_empty(&self) -> bool {
        self.cardinality == 0
    }

    pub fn to_iceberg_payload(&self) -> Result<Vec<u8>, String> {
        let mut vector = Vec::new();
        vector.extend_from_slice(&(self.bitmaps.len() as u64).to_le_bytes());
        for (key, bitmap) in &self.bitmaps {
            vector.extend_from_slice(&key.to_le_bytes());
            bitmap
                .serialize_into(&mut vector)
                .map_err(|e| format!("serialize roaring bitmap failed: {e}"))?;
        }

        let combined_len = DV_MAGIC
            .len()
            .checked_add(vector.len())
            .ok_or_else(|| "deletion vector length overflow".to_string())?;
        let combined_len_u32 = u32::try_from(combined_len)
            .map_err(|_| format!("deletion vector payload too large: {combined_len}"))?;

        let mut checksum = crc32fast::Hasher::new();
        checksum.update(&DV_MAGIC);
        checksum.update(&vector);
        let crc = checksum.finalize();

        let mut out = Vec::with_capacity(4 + combined_len + 4);
        out.extend_from_slice(&combined_len_u32.to_be_bytes());
        out.extend_from_slice(&DV_MAGIC);
        out.extend_from_slice(&vector);
        out.extend_from_slice(&crc.to_be_bytes());
        Ok(out)
    }

    pub fn from_iceberg_payload(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 16 {
            return Err("deletion vector payload too short".to_string());
        }
        let declared_len = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        if declared_len + 8 != bytes.len() {
            return Err(format!(
                "deletion vector length mismatch: declared payload+magic {declared_len}, total {}",
                bytes.len()
            ));
        }
        if bytes[4..8] != DV_MAGIC {
            return Err("deletion vector magic mismatch".to_string());
        }
        let expected_crc = u32::from_be_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
        let mut checksum = crc32fast::Hasher::new();
        checksum.update(&bytes[4..bytes.len() - 4]);
        let actual_crc = checksum.finalize();
        if expected_crc != actual_crc {
            return Err(format!(
                "deletion vector crc mismatch: expected {expected_crc}, actual {actual_crc}"
            ));
        }

        let vector = &bytes[8..bytes.len() - 4];
        let mut cursor = Cursor::new(vector);
        let mut count_buf = [0u8; 8];
        std::io::Read::read_exact(&mut cursor, &mut count_buf)
            .map_err(|e| format!("read deletion vector bitmap count failed: {e}"))?;
        let bitmap_count = u64::from_le_bytes(count_buf);
        let mut out = DeletionVector::default();
        for _ in 0..bitmap_count {
            let mut key_buf = [0u8; 4];
            std::io::Read::read_exact(&mut cursor, &mut key_buf)
                .map_err(|e| format!("read deletion vector key failed: {e}"))?;
            let key = u32::from_le_bytes(key_buf);
            let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
                .map_err(|e| format!("deserialize roaring bitmap failed: {e}"))?;
            out.cardinality += bitmap.len();
            out.bitmaps.insert(key, bitmap);
        }
        Ok(out)
    }
}
```

- [ ] **Step 5: Wire the module**

In `src/connector/iceberg/commit/mod.rs`:

```rust
mod puffin_dv;
pub use puffin_dv::DeletionVector;
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::commit::puffin_dv -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml src/connector/iceberg/commit/mod.rs src/connector/iceberg/commit/puffin_dv.rs
git commit -m "feat: add iceberg deletion vector codec"
```

---

### Task 5: Write and Read Single-Blob Puffin DV Files

**Files:**
- Modify: `src/connector/iceberg/commit/puffin_dv.rs`

- [ ] **Step 1: Add failing Puffin file round-trip test**

Add to `puffin_dv.rs` tests:

```rust
#[tokio::test]
async fn single_blob_puffin_round_trips_metadata_and_payload() {
    use iceberg::io::FileIOBuilder;

    let dir = tempfile::TempDir::new().unwrap();
    let file_io = FileIOBuilder::new_fs_io()
        .with_root(dir.path().to_str().unwrap())
        .build()
        .unwrap();
    let path = format!("{}/dv-test.puffin", dir.path().display());

    let mut dv = DeletionVector::default();
    dv.insert(1).unwrap();
    dv.insert(5).unwrap();

    let written = write_single_deletion_vector_puffin(
        &file_io,
        &path,
        "file:///tmp/data-00001.parquet",
        &dv,
    )
    .await
    .unwrap();

    assert_eq!(written.referenced_data_file, "file:///tmp/data-00001.parquet");
    assert_eq!(written.cardinality, 2);
    assert!(written.content_offset >= 4);
    assert!(written.content_size_in_bytes > 0);

    let loaded = read_deletion_vector_puffin(&file_io, &path, written.content_offset, written.content_size_in_bytes)
        .await
        .unwrap();
    assert_eq!(loaded, dv);
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::commit::puffin_dv::tests::single_blob_puffin_round_trips_metadata_and_payload -- --nocapture
```

Expected: FAIL because file helpers do not exist.

- [ ] **Step 3: Add Puffin metadata struct**

Add:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrittenPuffinDv {
    pub path: String,
    pub referenced_data_file: String,
    pub cardinality: u64,
    pub content_offset: i64,
    pub content_size_in_bytes: i64,
    pub file_size_in_bytes: u64,
}
```

- [ ] **Step 4: Implement single-blob writer**

Add:

```rust
use bytes::Bytes;
use iceberg::io::{FileIO, FileWrite};

pub async fn write_single_deletion_vector_puffin(
    file_io: &FileIO,
    path: &str,
    referenced_data_file: &str,
    dv: &DeletionVector,
) -> Result<WrittenPuffinDv, String> {
    let payload = dv.to_iceberg_payload()?;
    let output = file_io
        .new_output(path)
        .map_err(|e| format!("FileIO::new_output({path}) failed: {e}"))?;
    let mut writer = output
        .writer()
        .await
        .map_err(|e| format!("open puffin writer {path} failed: {e}"))?;

    const PUFFIN_MAGIC: &[u8; 4] = b"PFA1";
    writer
        .write(Bytes::copy_from_slice(PUFFIN_MAGIC))
        .await
        .map_err(|e| format!("write puffin header {path} failed: {e}"))?;

    let content_offset = PUFFIN_MAGIC.len() as i64;
    writer
        .write(Bytes::copy_from_slice(&payload))
        .await
        .map_err(|e| format!("write puffin blob {path} failed: {e}"))?;
    let content_size_in_bytes = i64::try_from(payload.len())
        .map_err(|_| format!("puffin blob too large: {}", payload.len()))?;

    let footer_json = serde_json::json!({
        "blobs": [{
            "type": "deletion-vector-v1",
            "fields": [],
            "snapshot-id": -1,
            "sequence-number": -1,
            "offset": content_offset,
            "length": content_size_in_bytes,
            "properties": {
                "referenced-data-file": referenced_data_file,
                "cardinality": dv.cardinality().to_string()
            }
        }],
        "properties": {
            "created-by": "NovaRocks"
        }
    })
    .to_string()
    .into_bytes();

    let mut footer = Vec::with_capacity(4 + footer_json.len() + 12);
    footer.extend_from_slice(PUFFIN_MAGIC);
    footer.extend_from_slice(&footer_json);
    footer.extend_from_slice(&(footer_json.len() as u32).to_le_bytes());
    footer.extend_from_slice(&[0u8; 4]);
    footer.extend_from_slice(PUFFIN_MAGIC);

    writer
        .write(Bytes::from(footer.clone()))
        .await
        .map_err(|e| format!("write puffin footer {path} failed: {e}"))?;
    writer
        .close()
        .await
        .map_err(|e| format!("close puffin file {path} failed: {e}"))?;

    let file_size_in_bytes = (PUFFIN_MAGIC.len() + payload.len() + footer.len()) as u64;
    Ok(WrittenPuffinDv {
        path: path.to_string(),
        referenced_data_file: referenced_data_file.to_string(),
        cardinality: dv.cardinality(),
        content_offset,
        content_size_in_bytes,
        file_size_in_bytes,
    })
}
```

- [ ] **Step 5: Implement reader for existing DV merge**

Add:

```rust
pub async fn read_deletion_vector_puffin(
    file_io: &FileIO,
    path: &str,
    content_offset: i64,
    content_size_in_bytes: i64,
) -> Result<DeletionVector, String> {
    if content_offset < 0 || content_size_in_bytes < 0 {
        return Err(format!(
            "invalid puffin DV range offset={content_offset} length={content_size_in_bytes} for {path}"
        ));
    }
    let input = file_io
        .new_input(path)
        .map_err(|e| format!("FileIO::new_input({path}) failed: {e}"))?;
    let bytes = input
        .read(
            content_offset as u64
                ..(content_offset as u64 + content_size_in_bytes as u64),
        )
        .await
        .map_err(|e| format!("read puffin DV blob {path} failed: {e}"))?;
    DeletionVector::from_iceberg_payload(&bytes)
}
```

- [ ] **Step 6: Run test**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::commit::puffin_dv -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/commit/puffin_dv.rs
git commit -m "feat: write puffin deletion vector files"
```

---

### Task 6: Add Collector Support for DELETE Position Groups and Commit Dispatch

**Files:**
- Modify: `src/connector/iceberg/commit/types.rs`
- Modify: `src/connector/iceberg/commit/collector.rs`
- Modify: `src/connector/iceberg/commit/run.rs`

- [ ] **Step 1: Write failing collector test**

Add to `src/connector/iceberg/commit/collector.rs` tests:

```rust
#[test]
fn collector_round_trips_injected_delete_groups() {
    let collector = IcebergCommitCollector::new(
        CommitOpKind::RowDeltaDv,
        iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new("db".to_string()),
            "t".to_string(),
        ),
        Some(1),
        1,
        std::sync::Arc::new(iceberg::spec::Schema::builder().build().unwrap()),
        std::sync::Arc::new(iceberg::spec::PartitionSpec::builder(iceberg::spec::Schema::builder().build().unwrap()).build().unwrap()),
        "file:///tmp/staging".to_string(),
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    );
    collector.inject_delete_group(super::position_delete_writer::PositionDeleteGroup {
        referenced_data_file: "file:///tmp/data.parquet".to_string(),
        positions: vec![1, 3, 5],
    });
    let groups = collector.take_delete_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].positions, vec![1, 3, 5]);
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::commit::collector -- --nocapture
```

Expected: FAIL because `CommitOpKind::RowDeltaDv` and delete-group methods do not exist.

- [ ] **Step 3: Add commit kind**

In `src/connector/iceberg/commit/types.rs`:

```rust
pub enum CommitOpKind {
    FastAppend,
    Overwrite,
    RowDelta,
    RowDeltaDv,
}
```

Update the distinct-variant test to include:

```rust
assert_ne!(CommitOpKind::RowDelta, CommitOpKind::RowDeltaDv);
```

- [ ] **Step 4: Add collector storage**

In `IcebergCommitCollector`, add:

```rust
delete_groups: Mutex<Vec<super::position_delete_writer::PositionDeleteGroup>>,
```

Initialize it in `new`:

```rust
delete_groups: Mutex::new(Vec::new()),
```

Add methods:

```rust
pub fn inject_delete_group(&self, group: super::position_delete_writer::PositionDeleteGroup) {
    self.delete_groups
        .lock()
        .expect("collector delete_groups lock poisoned")
        .push(group);
}

pub fn take_delete_groups(&self) -> Vec<super::position_delete_writer::PositionDeleteGroup> {
    let mut guard = self
        .delete_groups
        .lock()
        .expect("collector delete_groups lock poisoned");
    std::mem::take(&mut *guard)
}
```

- [ ] **Step 5: Add dispatch branch**

In `src/connector/iceberg/commit/run.rs`, add the match arm after `RowDelta`:

```rust
CommitOpKind::RowDeltaDv => Box::new(RowDeltaDvCommit),
```

Add the import:

```rust
use super::row_delta_dv::RowDeltaDvCommit;
```

This will not compile until Task 7 creates `row_delta_dv.rs`; keep this task and Task 7 in the same working batch before running full compile.

- [ ] **Step 6: Commit after Task 7 compile passes**

Commit this task together with Task 7:

```bash
git add src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/collector.rs src/connector/iceberg/commit/run.rs
git commit -m "feat: route row delta deletion vector commits"
```

---

### Task 7: Implement RowDeltaDvCommit

**Files:**
- Create: `src/connector/iceberg/commit/row_delta_dv.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`
- Modify: `src/engine/delete_flow.rs`
- Test: `src/engine/mod.rs`

- [ ] **Step 1: Add failing integration tests for row-lineage DELETE**

In `src/engine/mod.rs`, add:

```rust
#[test]
fn iceberg_row_lineage_delete_writes_puffin_dv_and_merges_second_delete() {
    let warehouse = TempDir::new().expect("warehouse");
    let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
    session
        .execute_in_database(
            "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
            "default",
        )
        .expect("seed");
    session
        .execute_in_database("delete from ice.db1.t where id = 2", "default")
        .expect("first delete");
    session
        .execute_in_database("delete from ice.db1.t where id = 3", "default")
        .expect("second delete");

    let registry = engine.inner.iceberg_catalogs.read().expect("registry");
    let entry = registry.get("ice").expect("entry");
    entry.invalidate_table_cache("db1", "t");
    let loaded =
        crate::connector::iceberg::catalog::load_table(&entry, "db1", "t").expect("load");
    let table = loaded.table;
    let dv_entries = crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        let manifests = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .expect("manifest list");
        let mut dv_entries = 0;
        for mf in manifests.entries() {
            if mf.content != iceberg::spec::ManifestContentType::Deletes {
                continue;
            }
            let manifest = mf.load_manifest(table.file_io()).await.expect("delete manifest");
            for entry in manifest.entries() {
                if entry.is_alive()
                    && entry.data_file().file_format() == iceberg::spec::DataFileFormat::Puffin
                {
                    dv_entries += 1;
                    assert!(entry.data_file().referenced_data_file().is_some());
                    assert!(entry.data_file().content_offset().is_some());
                    assert!(entry.data_file().content_size_in_bytes().is_some());
                }
            }
        }
        dv_entries
    })
    .expect("inspect manifests");
    assert_eq!(dv_entries, 1, "same data file should have one live merged DV");
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cargo test -p novarocks --lib engine::tests::iceberg_row_lineage_delete_writes_puffin_dv_and_merges_second_delete -- --nocapture
```

Expected: FAIL with `row-lineage DELETE selected the Puffin deletion-vector path before RowDeltaDvCommit was added`.

- [ ] **Step 3: Create RowDeltaDvCommit skeleton**

Create `src/connector/iceberg/commit/row_delta_dv.rs`:

```rust
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, MAIN_BRANCH,
    ManifestContentType, ManifestFile, ManifestWriterBuilder, Operation, PartitionSpecRef, SchemaRef,
    Snapshot, SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction};
use super::helpers::{generate_snapshot_id, metadata_dir, now_ms, write_manifest_list};
use super::position_delete_writer::PositionDeleteGroup;
use super::puffin_dv::{DeletionVector, WrittenPuffinDv, read_deletion_vector_puffin, write_single_deletion_vector_puffin};
use super::types::CommitOutcome;
```

Add structs:

```rust
pub struct RowDeltaDvCommit;

struct RowDeltaDvTxnAction {
    groups: Vec<PositionDeleteGroup>,
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    schema_id: i32,
    row_lineage_first_row_id: u64,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
}
```

- [ ] **Step 4: Implement action entrypoint**

Add:

```rust
#[async_trait]
impl IcebergCommitAction for RowDeltaDvCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let groups = ctx.collector.take_delete_groups();
        if groups.iter().all(|g| g.positions.is_empty()) {
            let id = ctx
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(0);
            return Ok(CommitOutcome {
                new_snapshot_id: id,
                written_manifest_paths: vec![],
            });
        }

        let manifest_paths_out = Arc::new(Mutex::new(Vec::new()));
        let action = RowDeltaDvTxnAction {
            groups,
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            partition_spec: ctx.collector.partition_spec.clone(),
            schema: ctx.table.metadata().current_schema().clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            row_lineage_first_row_id: ctx.table.metadata().next_row_id(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
        };

        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("RowDeltaDv apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("RowDeltaDv commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| "RowDeltaDv committed but new snapshot is not visible".to_string())?;
        Ok(CommitOutcome {
            new_snapshot_id,
            written_manifest_paths: manifest_paths_out.lock().unwrap().clone(),
        })
    }
}
```

- [ ] **Step 5: Implement file indexing and validation**

Inside `row_delta_dv.rs`, add:

```rust
struct LiveFile {
    data_file: DataFile,
    snapshot_id: i64,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
    manifest_path: String,
}

struct SnapshotIndex {
    data_files: HashMap<String, LiveFile>,
    untouched_manifests: Vec<ManifestFile>,
    touched_delete_existing: Vec<LiveFile>,
    touched_delete_manifest_paths: HashSet<String>,
}
```

Implement `build_snapshot_index(table, file_io, touched_files, vectors)`:

```rust
async fn build_snapshot_index(
    table: &Table,
    file_io: &FileIO,
    touched_files: &HashSet<String>,
    vectors: &mut HashMap<String, DeletionVector>,
) -> Result<SnapshotIndex, String> {
    let mut data_files = HashMap::new();
    let mut untouched_manifests = Vec::new();
    let mut touched_delete_existing = Vec::new();
    let mut touched_delete_manifest_paths = HashSet::new();
    let snapshot = table
        .metadata()
        .current_snapshot()
        .ok_or_else(|| "row-lineage DELETE requires a current snapshot".to_string())?;
    let list = snapshot
        .load_manifest_list(file_io, table.metadata())
        .await
        .map_err(|e| format!("load manifest list failed: {e}"))?;

    for mf in list.entries() {
        let manifest = mf
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load manifest {} failed: {e}", mf.manifest_path))?;
        let mut manifest_touched = false;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let seq = entry.sequence_number().unwrap_or(mf.sequence_number);
            let file_seq = entry.file_sequence_number;
            let snapshot_id = entry.snapshot_id().unwrap_or(mf.added_snapshot_id);
            let file = entry.data_file().clone();
            match mf.content {
                ManifestContentType::Data => {
                    data_files.insert(
                        file.file_path().to_string(),
                        LiveFile {
                            data_file: file,
                            snapshot_id,
                            sequence_number: seq,
                            file_sequence_number: file_seq,
                            manifest_path: mf.manifest_path.clone(),
                        },
                    );
                }
                ManifestContentType::Deletes => {
                    if file.content_type() == DataContentType::EqualityDeletes {
                        return Err("row-lineage DELETE does not support equality-delete files; compact them away first".to_string());
                    }
                    if file.file_format() != DataFileFormat::Puffin {
                        return Err("row-lineage DELETE found v2 position-delete files; compact them away before writing Puffin deletion vectors".to_string());
                    }
                    let referenced = file
                        .referenced_data_file()
                        .ok_or_else(|| format!("Puffin DV {} missing referenced_data_file", file.file_path()))?;
                    if touched_files.contains(&referenced) {
                        let offset = file.content_offset().ok_or_else(|| format!("Puffin DV {} missing content_offset", file.file_path()))?;
                        let len = file.content_size_in_bytes().ok_or_else(|| format!("Puffin DV {} missing content_size_in_bytes", file.file_path()))?;
                        let old = read_deletion_vector_puffin(file_io, file.file_path(), offset, len).await?;
                        vectors.entry(referenced).or_default().merge(&old);
                        manifest_touched = true;
                        touched_delete_manifest_paths.insert(mf.manifest_path.clone());
                    } else {
                        touched_delete_existing.push(LiveFile {
                            data_file: file,
                            snapshot_id,
                            sequence_number: seq,
                            file_sequence_number: file_seq,
                            manifest_path: mf.manifest_path.clone(),
                        });
                    }
                }
            }
        }
        if mf.content == ManifestContentType::Data || !manifest_touched {
            untouched_manifests.push(mf.clone());
        }
    }

    Ok(SnapshotIndex {
        data_files,
        untouched_manifests,
        touched_delete_existing,
        touched_delete_manifest_paths,
    })
}
```

- [ ] **Step 6: Implement TransactionAction commit**

Implement the commit method with this structure:

```rust
#[async_trait]
impl TransactionAction for RowDeltaDvTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        if format_version != FormatVersion::V3 {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "RowDeltaDvCommit requires an Iceberg v3 table",
            ));
        }
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let parent_snapshot_id = m.current_snapshot().map(|s| s.snapshot_id());
        let metadata_dir = metadata_dir(table);

        let mut vectors = groups_to_vectors(&self.groups).map_err(to_iceberg_unexpected)?;
        let touched_files: HashSet<String> = vectors.keys().cloned().collect();
        let index = build_snapshot_index(table, &self.file_io, &touched_files, &mut vectors)
            .await
            .map_err(to_iceberg_unexpected)?;

        let mut written_dvs = Vec::new();
        for (idx, (referenced, dv)) in vectors.iter().enumerate() {
            let path = format!(
                "{}/data/_staging/{}/dv-{:08x}.puffin",
                m.location(),
                self.commit_uuid,
                idx
            );
            self.abort_handle.record_data_file(path.clone());
            let written = write_single_deletion_vector_puffin(&self.file_io, &path, referenced, dv)
                .await
                .map_err(to_iceberg_unexpected)?;
            written_dvs.push(written);
        }

        let mut new_manifests = index.untouched_manifests;
        if !index.touched_delete_existing.is_empty() {
            let path = format!("{metadata_dir}/{}-row-delta-dv-existing-0.avro", self.commit_uuid);
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out.lock().unwrap().push(path.clone());
            let mf = write_existing_delete_manifest(
                &self.file_io,
                &path,
                &index.touched_delete_existing,
                self.partition_spec.clone(),
                self.schema.clone(),
                new_snapshot_id,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }
        let path = format!("{metadata_dir}/{}-row-delta-dv-added-0.avro", self.commit_uuid);
        self.abort_handle.record_manifest(path.clone());
        self.manifest_paths_out.lock().unwrap().push(path.clone());
        let added = write_added_dv_manifest(
            &self.file_io,
            &path,
            &written_dvs,
            &index.data_files,
            self.partition_spec.clone(),
            self.schema.clone(),
            new_seq,
            new_snapshot_id,
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        new_manifests.push(added);

        let manifest_list_path = format!("{metadata_dir}/snap-{}-{}.avro", new_snapshot_id, self.commit_uuid);
        self.abort_handle.record_manifest(manifest_list_path.clone());
        self.manifest_paths_out.lock().unwrap().push(manifest_list_path.clone());
        let _ = write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            new_manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            format_version,
            Some(self.row_lineage_first_row_id),
        )
        .await
        .map_err(to_iceberg_unexpected)?;

        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Delete,
                additional_properties: dv_summary(&written_dvs),
            })
            .with_schema_id(self.schema_id)
            .with_row_range(self.row_lineage_first_row_id, 0)
            .build();

        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference {
                        snapshot_id: new_snapshot_id,
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                },
            ],
            vec![
                TableRequirement::CurrentSchemaIdMatch { current_schema_id: m.current_schema_id() },
                TableRequirement::DefaultSpecIdMatch { default_spec_id: m.default_partition_spec_id() },
                TableRequirement::RefSnapshotIdMatch { r#ref: MAIN_BRANCH.to_string(), snapshot_id: parent_snapshot_id },
            ],
        ))
    }
}
```

- [ ] **Step 7: Add manifest writers**

Add `groups_to_vectors`, `write_existing_delete_manifest`, `write_added_dv_manifest`, `dv_data_file`, and `dv_summary` in `row_delta_dv.rs`. Use these signatures:

```rust
fn groups_to_vectors(groups: &[PositionDeleteGroup]) -> Result<HashMap<String, DeletionVector>, String>

async fn write_existing_delete_manifest(
    file_io: &FileIO,
    out_path: &str,
    files: &[LiveFile],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_snapshot_id: i64,
) -> Result<ManifestFile, String>

async fn write_added_dv_manifest(
    file_io: &FileIO,
    out_path: &str,
    dvs: &[WrittenPuffinDv],
    data_files: &HashMap<String, LiveFile>,
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_seq: i64,
    new_snapshot_id: i64,
) -> Result<ManifestFile, String>
```

For existing delete files, use `ManifestWriter::add_existing_file(file.data_file.clone(), file.snapshot_id, file.sequence_number, file.file_sequence_number)`.

For added DV files, build a `DataFile`:

```rust
fn dv_data_file(
    written: &WrittenPuffinDv,
    referenced: &LiveFile,
    partition_spec_id: i32,
) -> Result<DataFile, String> {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(written.path.clone())
        .file_format(DataFileFormat::Puffin)
        .partition(referenced.data_file.partition().clone())
        .partition_spec_id(partition_spec_id)
        .record_count(written.cardinality)
        .file_size_in_bytes(written.file_size_in_bytes)
        .referenced_data_file(Some(written.referenced_data_file.clone()))
        .content_offset(Some(written.content_offset))
        .content_size_in_bytes(Some(written.content_size_in_bytes))
        .build()
        .map_err(|e| format!("build DV DataFile failed: {e}"))
}
```

Call `dv_data_file(&written, referenced, partition_spec.spec_id())` from `write_added_dv_manifest`.

- [ ] **Step 8: Wire module export and delete_flow**

In `mod.rs`:

```rust
mod row_delta_dv;
pub use row_delta_dv::RowDeltaDvCommit;
```

In `delete_flow.rs`, replace the RowLineageV3 error branch with:

```rust
IcebergWriteMode::RowLineageV3 => {
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::RowDeltaDv,
        table_ident,
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir.clone(),
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    for group in groups {
        collector.inject_delete_group(group);
    }
    let fs = build_local_fs_operator()?;
    let _outcome = block_on_iceberg(async {
        run_iceberg_commit(RunInput {
            collector: collector.clone(),
            catalog: catalog.clone(),
            table,
            fs,
            file_io,
        })
        .await
    })??;
}
```

Keep the legacy branch exactly as the existing position-delete path.

- [ ] **Step 9: Run focused tests**

Run:

```bash
cargo test -p novarocks --lib engine::tests::iceberg_row_lineage_delete_writes_puffin_dv_and_merges_second_delete -- --nocapture
```

Expected: PASS.

- [ ] **Step 10: Commit Task 6 and Task 7 together**

```bash
git add src/connector/iceberg/commit/collector.rs src/connector/iceberg/commit/mod.rs src/connector/iceberg/commit/row_delta_dv.rs src/connector/iceberg/commit/run.rs src/connector/iceberg/commit/types.rs src/engine/delete_flow.rs src/engine/mod.rs
git commit -m "feat: write puffin deletion vectors for row lineage deletes"
```

---

### Task 8: Negative Tests and Final Verification

**Files:**
- Modify: `src/engine/mod.rs`
- Modify: `docs/superpowers/specs/2026-04-29-iceberg-v3-row-lineage-dv-phase2-design.md`

- [ ] **Step 1: Add position-delete rejection unit test**

In `src/connector/iceberg/commit/row_delta_dv.rs`, add:

```rust
#[test]
fn validate_delete_file_for_row_lineage_rejects_position_delete_parquet() {
    let file = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path("file:///tmp/delete.parquet".to_string())
        .file_format(DataFileFormat::Parquet)
        .partition(iceberg::spec::Struct::empty())
        .partition_spec_id(0)
        .record_count(1)
        .file_size_in_bytes(10)
        .referenced_data_file(Some("file:///tmp/data.parquet".to_string()))
        .build()
        .unwrap();
    let err = validate_delete_file_for_row_lineage(&file).unwrap_err();
    assert!(err.contains("position-delete"));
    assert!(err.contains("compact"));
}
```

Add the helper used by `build_snapshot_index`:

```rust
fn validate_delete_file_for_row_lineage(file: &DataFile) -> Result<(), String> {
    if file.content_type() == DataContentType::EqualityDeletes {
        return Err(
            "row-lineage DELETE does not support equality-delete files; compact them away first"
                .to_string(),
        );
    }
    if file.file_format() != DataFileFormat::Puffin {
        return Err(
            "row-lineage DELETE found v2 position-delete files; compact them away before writing Puffin deletion vectors"
                .to_string(),
        );
    }
    Ok(())
}
```

Then replace the inline equality / Puffin validation in `build_snapshot_index` with `validate_delete_file_for_row_lineage(&file)?`.

- [ ] **Step 2: Run the negative test**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::commit::row_delta_dv::validate_delete_file_for_row_lineage_rejects_position_delete_parquet -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Update design spec status**

In `docs/superpowers/specs/2026-04-29-iceberg-v3-row-lineage-dv-phase2-design.md`, change:

```markdown
**状态**：Draft（待 review）
```

to:

```markdown
**状态**：Accepted for Phase 2a planning
```

- [ ] **Step 4: Run full focused validation**

Run:

```bash
cargo fmt --check
cargo test -p novarocks --lib connector::iceberg::commit -- --nocapture
cargo test -p novarocks --lib engine::tests::iceberg_ -- --nocapture
cargo build -p novarocks
```

Expected:

- `cargo fmt --check`: PASS
- commit module tests: PASS
- Iceberg engine tests: PASS
- build: PASS

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-04-29-iceberg-v3-row-lineage-dv-phase2-design.md src/connector/iceberg/commit/row_delta_dv.rs
git commit -m "test: cover row lineage deletion vector edge cases"
```

---

## Final Review Checklist

- [ ] `write.row-lineage=false` / missing property still uses Phase 1 path.
- [ ] `write.row-lineage=true` INSERT writes snapshot row-range through FastAppend.
- [ ] `write.row-lineage=true` OVERWRITE writes snapshot row-range and advances `next-row-id`.
- [ ] `write.row-lineage=true` DELETE writes `file_format=Puffin` delete entries.
- [ ] A second DELETE against the same data file merges with the old DV and leaves one live DV.
- [ ] Existing v2 position-delete files on a row-lineage table fail-fast with a compaction message.
- [ ] Puffin DV payload uses Iceberg `deletion-vector-v1`: big-endian length, magic `D1 D3 39 64`, segmented Roaring portable vector, big-endian CRC-32.
- [ ] Puffin blob metadata uses `snapshot-id=-1`, `sequence-number=-1`, `referenced-data-file`, and `cardinality`.
- [ ] Definite commit failures clean staged Puffin files through `AbortLog`; commit-unknown leaves them in place.
