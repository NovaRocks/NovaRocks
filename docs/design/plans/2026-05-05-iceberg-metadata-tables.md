# Iceberg Metadata Tables (snapshots / history / refs / partitions) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire up the four highest-frequency Iceberg inspection tables — `snapshots`, `history`, `refs`, `partitions` — end-to-end (Java JVM bridge → Rust enum/builders → SQL regression coverage), so operators can run `SELECT * FROM catalog.db.tbl$snapshots` etc. against NovaRocks.

**Architecture:** Mirror the existing `FILES` / `MANIFESTS` pattern. Each new table type adds (a) a `case` arm in `IcebergMetadataBridge.scan` with a small `scanXxx(table, ...)` Java method that emits Jackson-serialized rows, (b) an `IcebergMetadataTableType` enum variant + scanner-type string in Rust, (c) a `RawXxxMetadataRow` deserialize struct + a `XxxMetadataRow` struct + `load_xxx_rows` + `build_xxx_chunks` + `build_xxx_array` in `src/connector/iceberg/metadata.rs`, and (d) a match arm in `IcebergMetadataScanOp::execute_iter`. None of these tables read manifest splits — `serialized_split` is ignored on the Java side (same as the existing `MANIFESTS` case).

**Tech Stack:** Rust (arrow-rs builders, serde_json), Java 11 (Apache Iceberg API, Jackson), embedded JNI bridge (already bootstrapped in `src/connector/iceberg/jvm.rs`).

---

## Background — Where Things Live

Read once before starting; keep these open while implementing.

- Rust enum + scan op + builders: [src/connector/iceberg/metadata.rs](src/connector/iceberg/metadata.rs) — enum at L37-61, `execute_iter` match at L199-234, `build_file_array` reference at L588-681, `build_manifest_array` reference at L683-742.
- JNI bridge: [src/connector/iceberg/jvm.rs:69-130](src/connector/iceberg/jvm.rs:69) — calls `com.novarocks.connector.iceberg.IcebergMetadataBridge.scan(scannerType, serializedTable, serializedSplit, serializedPredicate, loadColumnStats)`.
- Java bridge: [java/iceberg-metadata-bridge/src/main/java/com/novarocks/connector/iceberg/IcebergMetadataBridge.java](java/iceberg-metadata-bridge/src/main/java/com/novarocks/connector/iceberg/IcebergMetadataBridge.java) — switch at L80-90, `scanManifests` at L186-210 is the closest reference for table-level (no-split) scans.
- Plan-side wiring (already done — no changes needed): [src/lower/node/hdfs_scan.rs:585-589, 887-907](src/lower/node/hdfs_scan.rs:585) — receives `metadata_table_type` from FE Thrift, builds `IcebergMetadataScanConfig`.
- Iceberg StarRocks reference scanners (read-only): `~/project/starrocks/java-extensions/iceberg-metadata-reader/src/main/java/com/starrocks/connector/iceberg/Iceberg{Snapshots,History,Refs,Partitions}TableScanner.java`. Use these to confirm column lists and value semantics.
- SQL regression test layout: [sql-tests/iceberg/sql/](sql-tests/iceberg/sql/) — files; runner config [tests/sql-test-runner/conf/standalone_iceberg_local.conf](tests/sql-test-runner/conf/standalone_iceberg_local.conf).

---

## Column Schemas (Iceberg Spec, Confirmed Against StarRocks Reference)

Used to populate `IcebergMetadataOutputColumn.name` matchers. FE will send these names as the requested output columns; you only need to handle the names the test queries actually project, but to be safe implement all spec columns per table.

**snapshots:**
| name | Iceberg type | Arrow handling |
|---|---|---|
| `committed_at` | timestamptz (micros) | `Int64Array` of micros (FE applies tz cast) |
| `snapshot_id` | long | `Int64Array` |
| `parent_id` | long, nullable | `Int64Array` with nulls |
| `operation` | string, nullable | `StringArray` with nulls |
| `manifest_list` | string | `StringArray` |
| `summary` | map<string,string> | Map array — reuse pattern from `build_i32_utf8_map_array` but with string keys |

**history:**
| name | Iceberg type | Arrow handling |
|---|---|---|
| `made_current_at` | timestamptz (millis in source; convert to micros) | `Int64Array` of micros |
| `snapshot_id` | long | `Int64Array` |
| `parent_id` | long, nullable | `Int64Array` with nulls |
| `is_current_ancestor` | boolean | `BooleanArray` |

**refs:**
| name | Iceberg type | Arrow handling |
|---|---|---|
| `name` | string | `StringArray` |
| `type` | string ("BRANCH"/"TAG") | `StringArray` |
| `snapshot_id` | long | `Int64Array` |
| `max_reference_age_in_ms` | long, nullable | `Int64Array` with nulls |
| `min_snapshots_to_keep` | int, nullable | `Int32Array` with nulls |
| `max_snapshot_age_in_ms` | long, nullable | `Int64Array` with nulls |

**partitions:** (most involved — has dynamic struct column)
| name | Iceberg type | Arrow handling |
|---|---|---|
| `partition` | struct<dynamic, per partition spec> | `StructArray` matching FE-supplied schema (see Task 5 for handling) |
| `spec_id` | int | `Int32Array` |
| `record_count` | long | `Int64Array` |
| `file_count` | int | `Int32Array` |
| `total_data_file_size_in_bytes` | long | `Int64Array` |
| `position_delete_record_count` | long, nullable | `Int64Array` |
| `position_delete_file_count` | int, nullable | `Int32Array` |
| `equality_delete_record_count` | long, nullable | `Int64Array` |
| `equality_delete_file_count` | int, nullable | `Int32Array` |
| `last_updated_at` | timestamptz, nullable | `Int64Array` micros |
| `last_updated_snapshot_id` | long, nullable | `Int64Array` |

---

## File Structure

**Modify:**
- `src/connector/iceberg/metadata.rs` — extend enum, add row types, add load/build/array functions for 4 tables, add execute_iter arms.
- `java/iceberg-metadata-bridge/src/main/java/com/novarocks/connector/iceberg/IcebergMetadataBridge.java` — add 4 switch cases + 4 `scanXxx` methods + 4 row classes.

**Create:**
- `sql-tests/iceberg/sql/iceberg_metadata_tables.sql` — end-to-end coverage for all four tables (also adds the first regression coverage for `files`/`manifests`).
- `sql-tests/iceberg/result/iceberg_metadata_tables.result` — generated via `--mode record`.

No changes needed in `src/lower/node/hdfs_scan.rs` — it already forwards any `metadata_table_type` string verbatim into `IcebergMetadataTableType::parse`.

---

## Task 1: Extend `IcebergMetadataTableType` Enum

**Files:**
- Modify: `src/connector/iceberg/metadata.rs:37-61`

- [ ] **Step 1: Write failing parse tests for the 4 new variants**

Append to the `tests` mod in `src/connector/iceberg/metadata.rs`:

```rust
#[test]
fn test_parse_snapshots_history_refs_partitions() {
    assert_eq!(
        IcebergMetadataTableType::parse("SNAPSHOTS").unwrap(),
        IcebergMetadataTableType::Snapshots
    );
    assert_eq!(
        IcebergMetadataTableType::parse("history").unwrap(),
        IcebergMetadataTableType::History
    );
    assert_eq!(
        IcebergMetadataTableType::parse("Refs").unwrap(),
        IcebergMetadataTableType::Refs
    );
    assert_eq!(
        IcebergMetadataTableType::parse("partitions").unwrap(),
        IcebergMetadataTableType::Partitions
    );
}

#[test]
fn test_jvm_scanner_type_for_new_variants() {
    assert_eq!(
        IcebergMetadataTableType::Snapshots.as_jvm_scanner_type(),
        "SNAPSHOTS"
    );
    assert_eq!(
        IcebergMetadataTableType::History.as_jvm_scanner_type(),
        "HISTORY"
    );
    assert_eq!(
        IcebergMetadataTableType::Refs.as_jvm_scanner_type(),
        "REFS"
    );
    assert_eq!(
        IcebergMetadataTableType::Partitions.as_jvm_scanner_type(),
        "PARTITIONS"
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p novarocks --lib connector::iceberg::metadata::tests::test_parse_snapshots_history_refs_partitions connector::iceberg::metadata::tests::test_jvm_scanner_type_for_new_variants`

Expected: FAIL with "no variant `Snapshots`" / similar.

- [ ] **Step 3: Add the 4 variants and mappings**

Edit [src/connector/iceberg/metadata.rs:37-61](src/connector/iceberg/metadata.rs:37) to:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergMetadataTableType {
    Files,
    Manifests,
    LogicalIcebergMetadata,
    Snapshots,
    History,
    Refs,
    Partitions,
}

impl IcebergMetadataTableType {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_uppercase().as_str() {
            "FILES" => Ok(Self::Files),
            "MANIFESTS" => Ok(Self::Manifests),
            "LOGICAL_ICEBERG_METADATA" => Ok(Self::LogicalIcebergMetadata),
            "SNAPSHOTS" => Ok(Self::Snapshots),
            "HISTORY" => Ok(Self::History),
            "REFS" => Ok(Self::Refs),
            "PARTITIONS" => Ok(Self::Partitions),
            other => Err(format!("unsupported iceberg metadata table type: {other}")),
        }
    }

    fn as_jvm_scanner_type(&self) -> &'static str {
        match self {
            Self::Files => "FILES",
            Self::Manifests => "MANIFESTS",
            Self::LogicalIcebergMetadata => "LOGICAL_ICEBERG_METADATA",
            Self::Snapshots => "SNAPSHOTS",
            Self::History => "HISTORY",
            Self::Refs => "REFS",
            Self::Partitions => "PARTITIONS",
        }
    }
}
```

The existing `execute_iter` match at L199-234 is non-exhaustive on the new variants — add four temporary arms returning `Err("not yet implemented: <variant>".to_string())` so the build stays green. They will be replaced in Tasks 2-5.

```rust
            IcebergMetadataTableType::Snapshots
            | IcebergMetadataTableType::History
            | IcebergMetadataTableType::Refs
            | IcebergMetadataTableType::Partitions => {
                return Err(format!(
                    "iceberg metadata table {:?} is not implemented yet",
                    self.cfg.metadata_table_type
                ));
            }
```

- [ ] **Step 4: Run tests + clippy**

Run: `cargo test -p novarocks --lib connector::iceberg::metadata && cargo clippy -p novarocks -- -D warnings`
Expected: PASS, no warnings.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/metadata.rs
git commit -m "feat(iceberg): add Snapshots/History/Refs/Partitions enum variants

Extends IcebergMetadataTableType so parse() accepts the four new
metadata-table strings. Execute paths return an explicit
'not implemented yet' error and will be filled in by follow-up
commits."
```

---

## Task 2: `snapshots` — Java Bridge + Rust Builders

**Files:**
- Modify: `java/iceberg-metadata-bridge/src/main/java/com/novarocks/connector/iceberg/IcebergMetadataBridge.java`
- Modify: `src/connector/iceberg/metadata.rs`

- [ ] **Step 1: Java — add `SNAPSHOTS` switch case and `scanSnapshots`**

In `IcebergMetadataBridge.java` at L80 (the `switch (normalized)`), add before `default`:

```java
            case "SNAPSHOTS":
                return OBJECT_MAPPER.writeValueAsBytes(scanSnapshots(table));
```

Add new method (place near `scanManifests`):

```java
    private static List<SnapshotMetadataRow> scanSnapshots(Table table) {
        List<SnapshotMetadataRow> rows = new ArrayList<>();
        for (org.apache.iceberg.Snapshot snapshot : table.snapshots()) {
            SnapshotMetadataRow row = new SnapshotMetadataRow();
            // Iceberg snapshot timestamps are millis; convert to micros for Arrow timestamp(us).
            row.committed_at_micros = snapshot.timestampMillis() * 1000L;
            row.snapshot_id = snapshot.snapshotId();
            row.parent_id = snapshot.parentId();
            row.operation = snapshot.operation();
            row.manifest_list = snapshot.manifestListLocation();
            Map<String, String> summary = snapshot.summary();
            if (summary != null && !summary.isEmpty()) {
                row.summary = summary.entrySet().stream()
                    .map(e -> new StringStringEntry(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
            }
            rows.add(row);
        }
        return rows;
    }
```

Add row class (place near other public row classes):

```java
    public static final class SnapshotMetadataRow {
        public long committed_at_micros;
        public long snapshot_id;
        public Long parent_id;
        public String operation;
        public String manifest_list;
        public List<StringStringEntry> summary;
    }

    public static final class StringStringEntry {
        public String key;
        public String value;

        public StringStringEntry() {}
        public StringStringEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
```

- [ ] **Step 2: Java — `mvn package` to confirm bridge jar still builds**

Run: `cd java/iceberg-metadata-bridge && mvn -q package -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Rust — add `RawSnapshotMetadataRow` / `SnapshotMetadataRow` + `load_snapshot_rows` + `build_snapshot_chunks` + `build_snapshot_array`**

Append to `src/connector/iceberg/metadata.rs` (after the manifest equivalents):

```rust
#[derive(Clone, Debug)]
struct SnapshotMetadataRow {
    committed_at_micros: i64,
    snapshot_id: i64,
    parent_id: Option<i64>,
    operation: Option<String>,
    manifest_list: String,
    summary: Option<Vec<(String, String)>>,
}

#[derive(Deserialize)]
struct RawStringStringEntry {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct RawSnapshotMetadataRow {
    committed_at_micros: i64,
    snapshot_id: i64,
    parent_id: Option<i64>,
    operation: Option<String>,
    manifest_list: String,
    summary: Option<Vec<RawStringStringEntry>>,
}

impl From<RawSnapshotMetadataRow> for SnapshotMetadataRow {
    fn from(raw: RawSnapshotMetadataRow) -> Self {
        Self {
            committed_at_micros: raw.committed_at_micros,
            snapshot_id: raw.snapshot_id,
            parent_id: raw.parent_id,
            operation: raw.operation,
            manifest_list: raw.manifest_list,
            summary: raw.summary.map(|entries| {
                entries.into_iter().map(|e| (e.key, e.value)).collect()
            }),
        }
    }
}

fn load_snapshot_rows(
    cfg: &IcebergMetadataScanConfig,
) -> Result<Vec<SnapshotMetadataRow>, String> {
    let payload = scan_metadata(
        IcebergMetadataTableType::Snapshots.as_jvm_scanner_type(),
        &cfg.serialized_table,
        "",
        "",
        cfg.load_column_stats,
    )?;
    let rows: Vec<RawSnapshotMetadataRow> = serde_json::from_slice(&payload)
        .map_err(|e| format!("parse JVM iceberg snapshots metadata rows failed: {e}"))?;
    Ok(rows.into_iter().map(SnapshotMetadataRow::from).collect())
}

fn build_snapshot_chunks(
    rows: &[SnapshotMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_snapshot_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, output_chunk_schema, arrays, rows.len(), batch_size)
}

fn build_snapshot_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[SnapshotMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "committed_at" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.committed_at_micros).collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "parent_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.parent_id).collect::<Vec<_>>(),
        ))),
        "operation" => Ok(Arc::new(StringArray::from(
            rows.iter().map(|r| r.operation.as_deref()).collect::<Vec<_>>(),
        ))),
        "manifest_list" => Ok(Arc::new(StringArray::from(
            rows.iter().map(|r| Some(r.manifest_list.as_str())).collect::<Vec<_>>(),
        ))),
        "summary" => build_string_string_map_array(rows.iter().map(|r| r.summary.as_ref())),
        other => Err(format!("unsupported iceberg snapshots metadata column: {}", other)),
    }
}

fn build_string_string_map_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<(String, String)>>>,
{
    let mut builder = MapBuilder::new(
        Some(iceberg_map_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    for row in rows {
        match row {
            Some(entries) => {
                for (key, value) in entries {
                    builder.keys().append_value(key);
                    builder.values().append_value(value);
                }
                builder
                    .append(true)
                    .map_err(|e| format!("append map row failed: {}", e))?;
            }
            None => {
                builder
                    .append(false)
                    .map_err(|e| format!("append null map row failed: {}", e))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}
```

- [ ] **Step 4: Rust — wire into `execute_iter`**

Replace the temporary `Snapshots | History | Refs | Partitions =>` arm from Task 1. Carve out `Snapshots`:

```rust
            IcebergMetadataTableType::Snapshots => {
                let rows = load_snapshot_rows(&self.cfg)?;
                build_snapshot_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::History
            | IcebergMetadataTableType::Refs
            | IcebergMetadataTableType::Partitions => {
                return Err(format!(
                    "iceberg metadata table {:?} is not implemented yet",
                    self.cfg.metadata_table_type
                ));
            }
```

- [ ] **Step 5: Rust unit test for the array builders (no JVM)**

Append to the `tests` mod:

```rust
#[test]
fn test_build_snapshot_arrays_basic_shapes() {
    use super::SnapshotMetadataRow;
    let rows = vec![SnapshotMetadataRow {
        committed_at_micros: 1_700_000_000_000_000,
        snapshot_id: 42,
        parent_id: Some(41),
        operation: Some("append".into()),
        manifest_list: "s3://bucket/manifest-list.avro".into(),
        summary: Some(vec![("added-records".into(), "10".into())]),
    }];
    let columns = [
        ("snapshot_id", DataType::Int64),
        ("operation", DataType::Utf8),
    ];
    for (name, ty) in &columns {
        let col = super::IcebergMetadataOutputColumn {
            name: (*name).into(),
            slot_id: SlotId::new(1),
            data_type: ty.clone(),
            nullable: true,
        };
        let arr = super::build_snapshot_array(&col, &rows).unwrap();
        assert_eq!(arr.len(), 1);
    }
}
```

- [ ] **Step 6: Run `cargo build` + tests**

Run:
```
cargo build -p novarocks
cargo test -p novarocks --lib connector::iceberg::metadata
```
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/metadata.rs java/iceberg-metadata-bridge/
git commit -m "feat(iceberg): support \$snapshots metadata table

Adds the SNAPSHOTS scan-type to the JVM bridge and Rust builders.
Returns committed_at (micros), snapshot_id, parent_id, operation,
manifest_list, and summary map<string,string>."
```

---

## Task 3: `history` — Java Bridge + Rust Builders

**Files:**
- Modify: `java/iceberg-metadata-bridge/src/main/java/com/novarocks/connector/iceberg/IcebergMetadataBridge.java`
- Modify: `src/connector/iceberg/metadata.rs`

- [ ] **Step 1: Java — add `HISTORY` case and `scanHistory`**

Add to switch:
```java
            case "HISTORY":
                return OBJECT_MAPPER.writeValueAsBytes(scanHistory(table));
```

Add method:

```java
    private static List<HistoryMetadataRow> scanHistory(Table table) {
        List<HistoryMetadataRow> rows = new ArrayList<>();
        if (table.history() == null) {
            return rows;
        }
        // Build the set of ancestor snapshot ids of the current snapshot for is_current_ancestor.
        java.util.Set<Long> ancestors = new java.util.HashSet<>();
        if (table.currentSnapshot() != null) {
            org.apache.iceberg.Snapshot s = table.currentSnapshot();
            while (s != null) {
                ancestors.add(s.snapshotId());
                Long parent = s.parentId();
                s = parent == null ? null : table.snapshot(parent);
            }
        }
        for (org.apache.iceberg.HistoryEntry entry : table.history()) {
            HistoryMetadataRow row = new HistoryMetadataRow();
            row.made_current_at_micros = entry.timestampMillis() * 1000L;
            row.snapshot_id = entry.snapshotId();
            org.apache.iceberg.Snapshot snap = table.snapshot(entry.snapshotId());
            row.parent_id = snap == null ? null : snap.parentId();
            row.is_current_ancestor = ancestors.contains(entry.snapshotId());
            rows.add(row);
        }
        return rows;
    }
```

Add row class:
```java
    public static final class HistoryMetadataRow {
        public long made_current_at_micros;
        public long snapshot_id;
        public Long parent_id;
        public boolean is_current_ancestor;
    }
```

- [ ] **Step 2: Build the jar** — `cd java/iceberg-metadata-bridge && mvn -q package -DskipTests`. Expected: BUILD SUCCESS.

- [ ] **Step 3: Rust — add row types, loader, builder, array dispatch**

Append to `metadata.rs`:

```rust
#[derive(Clone, Debug)]
struct HistoryMetadataRow {
    made_current_at_micros: i64,
    snapshot_id: i64,
    parent_id: Option<i64>,
    is_current_ancestor: bool,
}

#[derive(Deserialize)]
struct RawHistoryMetadataRow {
    made_current_at_micros: i64,
    snapshot_id: i64,
    parent_id: Option<i64>,
    is_current_ancestor: bool,
}

impl From<RawHistoryMetadataRow> for HistoryMetadataRow {
    fn from(raw: RawHistoryMetadataRow) -> Self {
        Self {
            made_current_at_micros: raw.made_current_at_micros,
            snapshot_id: raw.snapshot_id,
            parent_id: raw.parent_id,
            is_current_ancestor: raw.is_current_ancestor,
        }
    }
}

fn load_history_rows(
    cfg: &IcebergMetadataScanConfig,
) -> Result<Vec<HistoryMetadataRow>, String> {
    let payload = scan_metadata(
        IcebergMetadataTableType::History.as_jvm_scanner_type(),
        &cfg.serialized_table,
        "",
        "",
        cfg.load_column_stats,
    )?;
    let rows: Vec<RawHistoryMetadataRow> = serde_json::from_slice(&payload)
        .map_err(|e| format!("parse JVM iceberg history metadata rows failed: {e}"))?;
    Ok(rows.into_iter().map(HistoryMetadataRow::from).collect())
}

fn build_history_chunks(
    rows: &[HistoryMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_history_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, output_chunk_schema, arrays, rows.len(), batch_size)
}

fn build_history_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[HistoryMetadataRow],
) -> Result<ArrayRef, String> {
    use arrow::array::BooleanArray;
    match column.name.as_str() {
        "made_current_at" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.made_current_at_micros).collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "parent_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.parent_id).collect::<Vec<_>>(),
        ))),
        "is_current_ancestor" => Ok(Arc::new(BooleanArray::from(
            rows.iter().map(|r| r.is_current_ancestor).collect::<Vec<_>>(),
        ))),
        other => Err(format!("unsupported iceberg history metadata column: {}", other)),
    }
}
```

- [ ] **Step 4: Wire `History` arm in `execute_iter`**

Carve out from the placeholder:
```rust
            IcebergMetadataTableType::History => {
                let rows = load_history_rows(&self.cfg)?;
                build_history_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::Refs | IcebergMetadataTableType::Partitions => {
                return Err(format!(
                    "iceberg metadata table {:?} is not implemented yet",
                    self.cfg.metadata_table_type
                ));
            }
```

- [ ] **Step 5: Build + test**

Run: `cargo build -p novarocks && cargo test -p novarocks --lib connector::iceberg::metadata`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/metadata.rs java/iceberg-metadata-bridge/
git commit -m "feat(iceberg): support \$history metadata table

Adds the HISTORY scan-type with made_current_at, snapshot_id,
parent_id, and is_current_ancestor (computed against the current
snapshot's ancestor chain)."
```

---

## Task 4: `refs` — Java Bridge + Rust Builders

**Files:**
- Modify: `java/iceberg-metadata-bridge/src/main/java/com/novarocks/connector/iceberg/IcebergMetadataBridge.java`
- Modify: `src/connector/iceberg/metadata.rs`

- [ ] **Step 1: Java — add `REFS` case and `scanRefs`**

Add to switch:
```java
            case "REFS":
                return OBJECT_MAPPER.writeValueAsBytes(scanRefs(table));
```

Add method:

```java
    private static List<RefMetadataRow> scanRefs(Table table) {
        List<RefMetadataRow> rows = new ArrayList<>();
        Map<String, org.apache.iceberg.SnapshotRef> refs = table.refs();
        if (refs == null) {
            return rows;
        }
        for (Map.Entry<String, org.apache.iceberg.SnapshotRef> entry : refs.entrySet()) {
            org.apache.iceberg.SnapshotRef ref = entry.getValue();
            RefMetadataRow row = new RefMetadataRow();
            row.name = entry.getKey();
            row.type_ = ref.isBranch() ? "BRANCH" : "TAG";
            row.snapshot_id = ref.snapshotId();
            row.max_reference_age_in_ms = ref.maxRefAgeMs();
            row.min_snapshots_to_keep = ref.minSnapshotsToKeep();
            row.max_snapshot_age_in_ms = ref.maxSnapshotAgeMs();
            rows.add(row);
        }
        return rows;
    }
```

Add row class — note: `type` is a Java keyword, hence trailing underscore + Jackson annotation:

```java
    public static final class RefMetadataRow {
        public String name;
        @com.fasterxml.jackson.annotation.JsonProperty("type")
        public String type_;
        public long snapshot_id;
        public Long max_reference_age_in_ms;
        public Integer min_snapshots_to_keep;
        public Long max_snapshot_age_in_ms;
    }
```

- [ ] **Step 2: Build jar** — `cd java/iceberg-metadata-bridge && mvn -q package -DskipTests`. Expected: BUILD SUCCESS.

- [ ] **Step 3: Rust — types, loader, builder, dispatch**

Append:

```rust
#[derive(Clone, Debug)]
struct RefMetadataRow {
    name: String,
    type_: String,
    snapshot_id: i64,
    max_reference_age_in_ms: Option<i64>,
    min_snapshots_to_keep: Option<i32>,
    max_snapshot_age_in_ms: Option<i64>,
}

#[derive(Deserialize)]
struct RawRefMetadataRow {
    name: String,
    #[serde(rename = "type")]
    type_: String,
    snapshot_id: i64,
    max_reference_age_in_ms: Option<i64>,
    min_snapshots_to_keep: Option<i32>,
    max_snapshot_age_in_ms: Option<i64>,
}

impl From<RawRefMetadataRow> for RefMetadataRow {
    fn from(raw: RawRefMetadataRow) -> Self {
        Self {
            name: raw.name,
            type_: raw.type_,
            snapshot_id: raw.snapshot_id,
            max_reference_age_in_ms: raw.max_reference_age_in_ms,
            min_snapshots_to_keep: raw.min_snapshots_to_keep,
            max_snapshot_age_in_ms: raw.max_snapshot_age_in_ms,
        }
    }
}

fn load_ref_rows(cfg: &IcebergMetadataScanConfig) -> Result<Vec<RefMetadataRow>, String> {
    let payload = scan_metadata(
        IcebergMetadataTableType::Refs.as_jvm_scanner_type(),
        &cfg.serialized_table,
        "",
        "",
        cfg.load_column_stats,
    )?;
    let rows: Vec<RawRefMetadataRow> = serde_json::from_slice(&payload)
        .map_err(|e| format!("parse JVM iceberg refs metadata rows failed: {e}"))?;
    Ok(rows.into_iter().map(RefMetadataRow::from).collect())
}

fn build_ref_chunks(
    rows: &[RefMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_ref_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, output_chunk_schema, arrays, rows.len(), batch_size)
}

fn build_ref_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[RefMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "name" => Ok(Arc::new(StringArray::from(
            rows.iter().map(|r| Some(r.name.as_str())).collect::<Vec<_>>(),
        ))),
        "type" => Ok(Arc::new(StringArray::from(
            rows.iter().map(|r| Some(r.type_.as_str())).collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "max_reference_age_in_ms" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.max_reference_age_in_ms).collect::<Vec<_>>(),
        ))),
        "min_snapshots_to_keep" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|r| r.min_snapshots_to_keep).collect::<Vec<_>>(),
        ))),
        "max_snapshot_age_in_ms" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.max_snapshot_age_in_ms).collect::<Vec<_>>(),
        ))),
        other => Err(format!("unsupported iceberg refs metadata column: {}", other)),
    }
}
```

- [ ] **Step 4: Wire `Refs` arm in `execute_iter`**

```rust
            IcebergMetadataTableType::Refs => {
                let rows = load_ref_rows(&self.cfg)?;
                build_ref_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::Partitions => {
                return Err(format!(
                    "iceberg metadata table {:?} is not implemented yet",
                    self.cfg.metadata_table_type
                ));
            }
```

- [ ] **Step 5: Build + test**

Run: `cargo build -p novarocks && cargo test -p novarocks --lib connector::iceberg::metadata`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/metadata.rs java/iceberg-metadata-bridge/
git commit -m "feat(iceberg): support \$refs metadata table

Adds the REFS scan-type returning name, type (BRANCH/TAG),
snapshot_id, and the three retention knobs."
```

---

## Task 5: `partitions` — Java Bridge + Rust Builders (with dynamic struct column)

This is the heaviest task because the `partition` (a.k.a. `partition_value`) column's struct schema depends on the table's partition spec, and FE will request whichever projection it needs.

**Strategy:** Iceberg's `MetadataTableType.PARTITIONS` produces aggregated rows already; reuse it on the Java side via `MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.PARTITIONS)`. The struct values are returned as a flat ordered `List<String>` of human-string-ified partition values (one entry per partition field, in spec field order). FE-supplied schema for the `partition` column carries the field names + types — Rust assembles a `StructArray` whose children are `StringArray`s populated from those values, in the same order.

**Files:**
- Modify: `java/iceberg-metadata-bridge/src/main/java/com/novarocks/connector/iceberg/IcebergMetadataBridge.java`
- Modify: `src/connector/iceberg/metadata.rs`

- [ ] **Step 1: Java — add `PARTITIONS` case and `scanPartitions`**

Add to switch:
```java
            case "PARTITIONS":
                return OBJECT_MAPPER.writeValueAsBytes(scanPartitions(table));
```

Add method (uses Iceberg's built-in PARTITIONS metadata table to do the heavy lifting):

```java
    private static List<PartitionMetadataRow> scanPartitions(Table table) throws Exception {
        if (table.currentSnapshot() == null) {
            return List.of();
        }
        org.apache.iceberg.Table partitionsTable =
                org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance(
                        table, org.apache.iceberg.MetadataTableType.PARTITIONS);
        org.apache.iceberg.TableScan scan = partitionsTable.newScan();
        Map<String, Integer> col2pos = new HashMap<>();
        int pos = 0;
        for (org.apache.iceberg.types.Types.NestedField f : scan.schema().columns()) {
            col2pos.put(f.name(), pos++);
        }
        List<PartitionMetadataRow> rows = new ArrayList<>();
        try (org.apache.iceberg.io.CloseableIterator<org.apache.iceberg.FileScanTask> tasks =
                scan.planFiles().iterator()) {
            if (!tasks.hasNext()) {
                return rows;
            }
            try (org.apache.iceberg.io.CloseableIterator<StructLike> reader =
                    tasks.next().asDataTask().rows().iterator()) {
                while (reader.hasNext()) {
                    StructLike sl = reader.next();
                    PartitionMetadataRow row = new PartitionMetadataRow();
                    // partition column is a StructLike at position 0 (Iceberg fixed name "partition")
                    Integer partPos = col2pos.get("partition");
                    if (partPos != null) {
                        StructLike part = sl.get(partPos, StructLike.class);
                        if (part != null) {
                            List<String> values = new ArrayList<>();
                            int spec = sl.get(col2pos.get("spec_id"), Integer.class);
                            PartitionSpec partSpec = table.specs().get(spec);
                            org.apache.iceberg.types.Types.StructType partType = partSpec.partitionType();
                            for (int i = 0; i < part.size(); i++) {
                                Class<?> javaType = partSpec.javaClasses()[i];
                                Object v = part.get(i, javaType);
                                values.add(v == null ? null
                                        : org.apache.iceberg.transforms.Transforms.identity()
                                              .toHumanString(partType.fields().get(i).type(), v));
                            }
                            row.partition_values = values;
                        }
                    }
                    row.spec_id = (Integer) sl.get(col2pos.get("spec_id"), Integer.class);
                    row.record_count = nullableLong(sl, col2pos, "record_count");
                    row.file_count = nullableInt(sl, col2pos, "file_count");
                    row.total_data_file_size_in_bytes =
                            nullableLong(sl, col2pos, "total_data_file_size_in_bytes");
                    row.position_delete_record_count =
                            nullableLong(sl, col2pos, "position_delete_record_count");
                    row.position_delete_file_count =
                            nullableInt(sl, col2pos, "position_delete_file_count");
                    row.equality_delete_record_count =
                            nullableLong(sl, col2pos, "equality_delete_record_count");
                    row.equality_delete_file_count =
                            nullableInt(sl, col2pos, "equality_delete_file_count");
                    Long lastUpdatedMs = nullableLong(sl, col2pos, "last_updated_at");
                    row.last_updated_at_micros = lastUpdatedMs == null ? null : lastUpdatedMs * 1000L;
                    row.last_updated_snapshot_id =
                            nullableLong(sl, col2pos, "last_updated_snapshot_id");
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private static Long nullableLong(StructLike sl, Map<String, Integer> col2pos, String name) {
        Integer p = col2pos.get(name);
        if (p == null) return null;
        return sl.get(p, Long.class);
    }

    private static Integer nullableInt(StructLike sl, Map<String, Integer> col2pos, String name) {
        Integer p = col2pos.get(name);
        if (p == null) return null;
        return sl.get(p, Integer.class);
    }
```

Add row class:

```java
    public static final class PartitionMetadataRow {
        public List<String> partition_values;
        public Integer spec_id;
        public Long record_count;
        public Integer file_count;
        public Long total_data_file_size_in_bytes;
        public Long position_delete_record_count;
        public Integer position_delete_file_count;
        public Long equality_delete_record_count;
        public Integer equality_delete_file_count;
        public Long last_updated_at_micros;
        public Long last_updated_snapshot_id;
    }
```

- [ ] **Step 2: Build jar** — `cd java/iceberg-metadata-bridge && mvn -q package -DskipTests`. Expected: BUILD SUCCESS.

- [ ] **Step 3: Rust — types, loader**

Append:

```rust
#[derive(Clone, Debug)]
struct PartitionMetadataRow {
    partition_values: Option<Vec<Option<String>>>,
    spec_id: Option<i32>,
    record_count: Option<i64>,
    file_count: Option<i32>,
    total_data_file_size_in_bytes: Option<i64>,
    position_delete_record_count: Option<i64>,
    position_delete_file_count: Option<i32>,
    equality_delete_record_count: Option<i64>,
    equality_delete_file_count: Option<i32>,
    last_updated_at_micros: Option<i64>,
    last_updated_snapshot_id: Option<i64>,
}

#[derive(Deserialize)]
struct RawPartitionMetadataRow {
    partition_values: Option<Vec<Option<String>>>,
    spec_id: Option<i32>,
    record_count: Option<i64>,
    file_count: Option<i32>,
    total_data_file_size_in_bytes: Option<i64>,
    position_delete_record_count: Option<i64>,
    position_delete_file_count: Option<i32>,
    equality_delete_record_count: Option<i64>,
    equality_delete_file_count: Option<i32>,
    last_updated_at_micros: Option<i64>,
    last_updated_snapshot_id: Option<i64>,
}

impl From<RawPartitionMetadataRow> for PartitionMetadataRow {
    fn from(r: RawPartitionMetadataRow) -> Self {
        Self {
            partition_values: r.partition_values,
            spec_id: r.spec_id,
            record_count: r.record_count,
            file_count: r.file_count,
            total_data_file_size_in_bytes: r.total_data_file_size_in_bytes,
            position_delete_record_count: r.position_delete_record_count,
            position_delete_file_count: r.position_delete_file_count,
            equality_delete_record_count: r.equality_delete_record_count,
            equality_delete_file_count: r.equality_delete_file_count,
            last_updated_at_micros: r.last_updated_at_micros,
            last_updated_snapshot_id: r.last_updated_snapshot_id,
        }
    }
}

fn load_partition_rows(
    cfg: &IcebergMetadataScanConfig,
) -> Result<Vec<PartitionMetadataRow>, String> {
    let payload = scan_metadata(
        IcebergMetadataTableType::Partitions.as_jvm_scanner_type(),
        &cfg.serialized_table,
        "",
        "",
        cfg.load_column_stats,
    )?;
    let rows: Vec<RawPartitionMetadataRow> = serde_json::from_slice(&payload)
        .map_err(|e| format!("parse JVM iceberg partitions metadata rows failed: {e}"))?;
    Ok(rows.into_iter().map(PartitionMetadataRow::from).collect())
}
```

- [ ] **Step 4: Rust — partition struct array builder**

The `partition` (or `partition_value` per StarRocks naming — implement both name aliases to be safe) column is a `Struct<...>` whose fields come from FE's column metadata. Each child column receives the Nth value of `partition_values` cast/parsed from human-string back to the child type. **Minimal version:** require child types to be `Utf8` (StarRocks delivers them as VARCHAR humanized strings) — if a non-Utf8 child appears, return an explicit error. Add support for richer types in a follow-up.

```rust
fn build_partition_struct_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[PartitionMetadataRow],
) -> Result<ArrayRef, String> {
    let DataType::Struct(fields) = &column.data_type else {
        return Err(format!(
            "iceberg partitions `{}` column expects Struct, got {:?}",
            column.name, column.data_type
        ));
    };
    let n_fields = fields.len();
    let mut child_builders: Vec<StringBuilder> =
        (0..n_fields).map(|_| StringBuilder::new()).collect();
    let mut struct_nulls = NullBufferBuilder::new(rows.len());
    for row in rows {
        match row.partition_values.as_ref() {
            Some(values) if values.len() == n_fields => {
                struct_nulls.append(true);
                for (i, v) in values.iter().enumerate() {
                    match v.as_deref() {
                        Some(s) => child_builders[i].append_value(s),
                        None => child_builders[i].append_null(),
                    }
                }
            }
            Some(values) => {
                return Err(format!(
                    "iceberg partitions row has {} partition values but FE schema has {} fields",
                    values.len(),
                    n_fields
                ));
            }
            None => {
                struct_nulls.append(false);
                for b in &mut child_builders {
                    b.append_null();
                }
            }
        }
    }
    for f in fields.iter() {
        if !matches!(f.data_type(), DataType::Utf8) {
            return Err(format!(
                "iceberg partitions struct child `{}` must be Utf8 (got {:?}); richer types not yet supported",
                f.name(), f.data_type()
            ));
        }
    }
    let children: Vec<ArrayRef> = child_builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect();
    let arr = StructArray::new(fields.clone(), children, struct_nulls.finish());
    Ok(Arc::new(arr))
}
```

- [ ] **Step 5: Rust — top-level dispatch + chunks**

```rust
fn build_partition_chunks(
    rows: &[PartitionMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_partition_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, output_chunk_schema, arrays, rows.len(), batch_size)
}

fn build_partition_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[PartitionMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "partition" | "partition_value" => build_partition_struct_array(column, rows),
        "spec_id" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|r| r.spec_id).collect::<Vec<_>>(),
        ))),
        "record_count" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.record_count).collect::<Vec<_>>(),
        ))),
        "file_count" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|r| r.file_count).collect::<Vec<_>>(),
        ))),
        "total_data_file_size_in_bytes" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.total_data_file_size_in_bytes).collect::<Vec<_>>(),
        ))),
        "position_delete_record_count" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.position_delete_record_count).collect::<Vec<_>>(),
        ))),
        "position_delete_file_count" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|r| r.position_delete_file_count).collect::<Vec<_>>(),
        ))),
        "equality_delete_record_count" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.equality_delete_record_count).collect::<Vec<_>>(),
        ))),
        "equality_delete_file_count" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|r| r.equality_delete_file_count).collect::<Vec<_>>(),
        ))),
        "last_updated_at" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.last_updated_at_micros).collect::<Vec<_>>(),
        ))),
        "last_updated_snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.last_updated_snapshot_id).collect::<Vec<_>>(),
        ))),
        other => Err(format!("unsupported iceberg partitions metadata column: {}", other)),
    }
}
```

- [ ] **Step 6: Wire `Partitions` arm in `execute_iter` (replace placeholder)**

```rust
            IcebergMetadataTableType::Partitions => {
                let rows = load_partition_rows(&self.cfg)?;
                build_partition_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
```

- [ ] **Step 7: Rust unit test for the partition struct builder (no JVM)**

```rust
#[test]
fn test_build_partition_struct_array_aligns_children() {
    let rows = vec![
        super::PartitionMetadataRow {
            partition_values: Some(vec![Some("2026-01-01".into()), Some("US".into())]),
            spec_id: Some(0),
            record_count: Some(10),
            file_count: Some(1),
            total_data_file_size_in_bytes: Some(123),
            position_delete_record_count: None,
            position_delete_file_count: None,
            equality_delete_record_count: None,
            equality_delete_file_count: None,
            last_updated_at_micros: None,
            last_updated_snapshot_id: None,
        },
        super::PartitionMetadataRow {
            partition_values: None,
            spec_id: Some(0),
            record_count: Some(0),
            file_count: Some(0),
            total_data_file_size_in_bytes: Some(0),
            position_delete_record_count: None,
            position_delete_file_count: None,
            equality_delete_record_count: None,
            equality_delete_file_count: None,
            last_updated_at_micros: None,
            last_updated_snapshot_id: None,
        },
    ];
    let column = super::IcebergMetadataOutputColumn {
        name: "partition".into(),
        slot_id: SlotId::new(1),
        data_type: DataType::Struct(
            vec![
                Arc::new(Field::new("dt", DataType::Utf8, true)),
                Arc::new(Field::new("country", DataType::Utf8, true)),
            ]
            .into(),
        ),
        nullable: true,
    };
    let arr = super::build_partition_array(&column, &rows).unwrap();
    assert_eq!(arr.len(), 2);
}
```

- [ ] **Step 8: Build + test**

Run: `cargo build -p novarocks && cargo test -p novarocks --lib connector::iceberg::metadata`
Expected: PASS, including the new partition test.

- [ ] **Step 9: Commit**

```bash
git add src/connector/iceberg/metadata.rs java/iceberg-metadata-bridge/
git commit -m "feat(iceberg): support \$partitions metadata table

Adds the PARTITIONS scan-type. The Java side reuses Iceberg's
built-in MetadataTableType.PARTITIONS and humanizes partition
values to strings; the Rust side assembles the FE-supplied
partition struct schema with Utf8 children. Richer non-Utf8
struct children return an explicit error."
```

---

## Task 6: End-to-End SQL Regression Test

> **Status (2026-05-06):** Deferred. Probe against the bundled standalone-server FE returned `ERROR 1235: unsupported identifier 'tbl$snapshots'`. The standalone parser at [src/engine/catalog.rs](src/engine/catalog.rs) `normalize_identifier()` only accepts `[a-zA-Z_][a-zA-Z0-9_]*`; `$tabletype` routing is a known gap with an existing TODO at [src/engine/mod.rs:4538](src/engine/mod.rs:4538) (`// $snapshots (not yet supported in NovaRocks).`). Tasks 1–5 (BE side) are complete and unit-tested; the missing piece is a separate FE-side parser feature that should be planned independently and will unblock this task. The SQL below is left in place as the target test once parser support lands.


**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_metadata_tables.sql`
- Create (via `--mode record`): `sql-tests/iceberg/result/iceberg_metadata_tables.result`

- [ ] **Step 1: Write the SQL test**

Use the project convention from neighboring tests (e.g. `sql-tests/iceberg/sql/iceberg_partition_evolution_1.sql`) for catalog setup. The test should:
1. Create a partitioned Iceberg table with at least 2 commits (so `snapshots` and `history` have ≥2 rows).
2. Add a tag/branch (so `refs` has more than just `main`).
3. Query the four metadata tables (`$snapshots`, `$history`, `$refs`, `$partitions`) plus the already-supported `$files` and `$manifests` for regression coverage.

```sql
-- iceberg_metadata_tables.sql
SET catalog iceberg_local;
CREATE DATABASE IF NOT EXISTS metadata_tables_db;
USE metadata_tables_db;

DROP TABLE IF EXISTS t;
CREATE TABLE t (id BIGINT, dt DATE) PARTITION BY (dt);

INSERT INTO t VALUES (1, DATE '2026-01-01'), (2, DATE '2026-01-02');
INSERT INTO t VALUES (3, DATE '2026-01-03');

-- $snapshots: expect 2 rows for the 2 INSERTs.
SELECT operation, summary['added-records'] AS added
FROM t$snapshots
ORDER BY committed_at;

-- $history: same 2 rows, all is_current_ancestor = true.
SELECT is_current_ancestor
FROM t$history
ORDER BY made_current_at;

-- $refs: at least the 'main' BRANCH ref must exist on every Iceberg table.
SELECT name, type FROM t$refs ORDER BY name;

-- $partitions: 3 partitions with their record counts.
SELECT partition.dt AS dt, record_count, file_count
FROM t$partitions
ORDER BY partition.dt;

-- Regression coverage for previously-supported tables.
SELECT count(*) FROM t$files;
SELECT count(*) FROM t$manifests;

DROP TABLE t;
DROP DATABASE metadata_tables_db;
```

> The FE syntax for metadata tables in NovaRocks's bundled standalone FE today is `t$snapshots` (validated by the existing `IcebergMetadataTableType::parse` call site in [src/lower/node/hdfs_scan.rs:585-589](src/lower/node/hdfs_scan.rs:585), which only fires when FE has already populated `metadata_table_type`). Before recording results, run a single ad-hoc `SELECT * FROM metadata_tables_db.t$snapshots LIMIT 1;` from a `mysql` client against `--port 9030` to confirm the FE actually emits the metadata scan node — if it returns "no such table", the FE-side parser may need a flag or this is a separate FE-side gap; capture the error and stop, do not proceed to record. Tag/branch coverage beyond `main` is intentionally out of scope here — adding tag DDL to the bundled FE is its own task.

- [ ] **Step 2: Start the standalone server**

In a second terminal:
```
NO_PROXY=127.0.0.1,localhost cargo run --release --features embedded-jvm -- standalone-server --port 9030
```
Wait until "ready on 127.0.0.1:9030" appears.

- [ ] **Step 3: Record results**

Run:
```
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_metadata_tables --mode record
```
Expected: writes `sql-tests/iceberg/result/iceberg_metadata_tables.result`.

- [ ] **Step 4: Verify replay passes**

Run:
```
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_metadata_tables --mode verify
```
Expected: PASS.

- [ ] **Step 5: Hand-inspect the result file**

Open `sql-tests/iceberg/result/iceberg_metadata_tables.result` and confirm:
- `$snapshots` has 2 rows with operation = `append`.
- `$history.is_current_ancestor` = `true` on both rows.
- `$refs` includes `main` (BRANCH) and `release-2026-q1` (TAG).
- `$partitions` has 3 rows for dates 2026-01-01..03 with `record_count = 1`.
- `$files` and `$manifests` counts match expectations.

If any row is wrong, do **not** edit the `.result` to make it match — fix the underlying code (likely a column-name mismatch between FE projection and Rust dispatch) and re-record.

- [ ] **Step 6: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_metadata_tables.sql \
        sql-tests/iceberg/result/iceberg_metadata_tables.result
git commit -m "test(iceberg): end-to-end coverage for metadata tables

Covers \$snapshots, \$history, \$refs, \$partitions plus regression
coverage for the previously-implemented \$files and \$manifests."
```

---

## Out of Scope (Defer to Follow-ups)

These were considered and intentionally left out to keep this plan minimal:

- Non-`Utf8` partition struct children. Iceberg's `PARTITIONS` table delivers values as the original partition types (date / int / string / timestamp). For now Rust requires Utf8 children, which works because StarRocks FE projects them as VARCHAR. If a future FE projects native types, extend `build_partition_struct_array` to handle `Date32`, `Timestamp`, `Int32`, `Int64` children.
- `metadata_log_entries`, `properties`, `all_data_files`, `all_manifests`, `entries`, `position_deletes`. Not in the requested set; mechanical follow-on if needed.
- Predicate pushdown into the Java bridge for these new tables. Currently `serializedPredicate` is forwarded only for `LOGICAL_ICEBERG_METADATA`. The new tables are typically small (one or a handful of rows), so projection-only is acceptable.

---

## Checklist Before Calling Done

- [ ] `cargo fmt && cargo clippy -- -D warnings && cargo build && cargo test -p novarocks --lib connector::iceberg::metadata` all pass.
- [ ] `mvn -q package -DskipTests` in `java/iceberg-metadata-bridge` succeeds.
- [ ] `--mode verify` of the new SQL test passes against a fresh `standalone-server`.
- [ ] All 4 metadata-table types appear in `IcebergMetadataTableType::parse` with both upper- and lower-case acceptance covered by tests.
- [ ] No new `// TODO` or `unimplemented!()` strings in the changed Rust files.
