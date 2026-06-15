# Phase 1: Distributed v3 DV-delete (DeletionVectors sink + per-`_file` shuffle + from-files commit) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `DELETE` on a v3 (deletion-vector) Iceberg table write its Puffin DVs on the **BE** via a new distributed `DeletionVectors` sink — shuffled by `_file` so each BE owns a data file and reads+merges its existing DV — with the **FE** only committing metadata; remove the v3-DV-specific local scan/inject path.

**Architecture:** Mirror the already-distributed v2 position-delete path (`DistributedDeleteWriteExecutor` → `execute_query_as_iceberg_write` → `ICEBERG_DELETE_SINK`). New pieces: (1) a `DeletionVectors` sink mode → new `ICEBERG_DV_SINK`; (2) a per-`_file` hash shuffle in front of the sink so one BE owns all positions for a data file; (3) a BE sink writer that reads the file's existing DV, merges new positions, writes one merged Puffin blob, and reports it; (4) thrift/`WrittenFile` fields so the Puffin descriptor (offset/size/cardinality) round-trips BE→FE; (5) a metadata-only `RowDeltaDvFromFiles` commit that registers BE-written DV files without rebuilding/merging/writing.

**Tech Stack:** Rust; Apache Thrift IDL (`idl/thrift/`); iceberg-rust 0.9 (vendored); roaring bitmaps (`puffin_dv::DeletionVector`); NovaRocks Cascades optimizer (`DistributionSpec`); sql-test-runner.

**Profile note (CLAUDE.md §8.2):** use `cargo test --lib` (profile `dev`, ~18s incremental) for the source-introspection / unit guards; use `--profile dev-opt` only for SQL-suite runs.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `idl/thrift/Types.thrift` | wire types | add `content_offset`/`content_size_in_bytes`/`cardinality` to `TIcebergDataFile` (fields 16-18) |
| `idl/thrift/DataSinks.thrift` | sink types | add `ICEBERG_DV_SINK` to `TDataSinkType` |
| `src/connector/iceberg/commit/types.rs` | commit model | add `content_offset`/`content_size_in_bytes`/`cardinality` to `WrittenFile`; add `CommitOpKind::RowDeltaDvFromFiles` |
| `src/connector/iceberg/commit/collector.rs` | writer→commit round-trip | `convert` maps new fields + derives `DataFileFormat` from the `format` string (not hardcoded Parquet) |
| `src/sql/codegen/iceberg_write_sink.rs` | FE sink spec | add `IcebergWriteSinkMode::DeletionVectors` → `ICEBERG_DV_SINK` |
| `src/lower/fragment.rs` | sink lowering | map `ICEBERG_DV_SINK` → `IcebergSinkMode::DeletionVectors` |
| `src/connector/iceberg/sink.rs` | BE writer | add `IcebergSinkMode::DeletionVectors` + `push_chunk_deletion_vector` (read existing DV, merge, write Puffin, report) |
| `src/engine/mod.rs` | write entrypoint | thread an optional required-root-distribution into `execute_query_as_iceberg_write` → `optimize` |
| `src/sql/optimizer/mod.rs` | optimizer entry | accept a `root_required` override (default `gather()`) |
| `src/connector/iceberg/commit/row_delta_dv_from_files.rs` | metadata-only commit | new `RowDeltaDvFromFilesCommit` (registers BE DV files; no build/merge/write) |
| `src/connector/iceberg/commit/run.rs` | dispatch | route `RowDeltaDvFromFiles` |
| `src/engine/delete_flow.rs` | DELETE routing | replace the v3-DV local branch with a distributed DV executor; delete `scan_for_position_deletes_at`/`InjectedDeleteGroupExecutor`/old `run_delete_dv_write_transaction` |
| `sql-tests/iceberg-dml/sql/dv_delete_distributed.sql` (+ `.result`) | e2e | DV-delete correctness (also run under 1FE+2BE) |

---

## Task 1: Thrift — carry the Puffin DV descriptor on the wire

**Files:**
- Modify: `idl/thrift/Types.thrift:600-618` (`TIcebergDataFile`)
- Modify: `idl/thrift/DataSinks.thrift:45-64` (`TDataSinkType`)

- [ ] **Step 1: Add fields to `TIcebergDataFile`** (after field 15)

```thrift
    15: optional TIcebergPartitionDescriptor partition_values_descriptor;
    // Puffin deletion-vector carriage (BE writer -> FE coordinator). Optional =>
    // backward-compatible for the FE-compatible path and for Parquet files.
    16: optional i64 content_offset;
    17: optional i64 content_size_in_bytes;
    18: optional i64 cardinality;
```

- [ ] **Step 2: Add the DV sink type** (end of `TDataSinkType`)

```thrift
    SPLIT_DATA_STREAM_SINK,
    NOOP_SINK,
    ICEBERG_DELETE_SINK,
    ICEBERG_DV_SINK
```

- [ ] **Step 3: Regenerate thrift + build**

Run: `cargo build --lib 2>&1 | tail -20`
Expected: PASS (new optional fields + enum variant compile; generated `types::TIcebergDataFile` now has `content_offset`/`content_size_in_bytes`/`cardinality: Option<i64>`).

- [ ] **Step 4: Commit**

```bash
git add idl/thrift/Types.thrift idl/thrift/DataSinks.thrift
git commit -m "feat(thrift): add Puffin DV descriptor fields + ICEBERG_DV_SINK"
```

---

## Task 2: `WrittenFile` carries DV offset/size/cardinality; `convert` honors format

**Files:**
- Modify: `src/connector/iceberg/commit/types.rs:98-130` (`WrittenFile`)
- Modify: `src/connector/iceberg/commit/collector.rs:328-399` (`convert`)
- Test: inline `#[cfg(test)] mod tests` in `collector.rs`

- [ ] **Step 1: Write the failing test** (append to collector tests)

```rust
#[test]
fn convert_preserves_puffin_dv_descriptor() {
    let collector = test_collector_with_metadata(); // existing helper in this test mod
    let df = crate::types::TIcebergDataFile {
        path: Some("s3://b/data/dv-00000000.puffin".to_string()),
        format: Some("puffin".to_string()),
        record_count: Some(3),
        file_size_in_bytes: Some(40),
        file_content: Some(crate::types::TIcebergFileContent::POSITION_DELETES),
        referenced_data_file: Some("s3://b/data/f.parquet".to_string()),
        partition_spec_id: Some(0),
        content_offset: Some(4),
        content_size_in_bytes: Some(12),
        cardinality: Some(3),
        ..Default::default()
    };
    let wf = collector.convert(df).expect("convert");
    assert_eq!(wf.format, DataFileFormat::Puffin);
    assert_eq!(wf.content_offset, Some(4));
    assert_eq!(wf.content_size_in_bytes, Some(12));
    assert_eq!(wf.cardinality, Some(3));
    assert_eq!(wf.referenced_data_file.as_deref(), Some("s3://b/data/f.parquet"));
}
```

(If no `test_collector_with_metadata` helper exists, build the collector inline from `IcebergCommitCollector` fields used elsewhere in the test module — reuse whatever the existing `convert`-adjacent tests use for `self.metadata`.)

- [ ] **Step 2: Run it — expect FAIL** (fields don't exist / format hardcoded)

Run: `cargo test --lib commit::collector::tests::convert_preserves_puffin_dv_descriptor`
Expected: FAIL (`no field content_offset on WrittenFile`).

- [ ] **Step 3: Add fields to `WrittenFile`** (`types.rs`, after `first_row_id`)

```rust
    pub first_row_id: Option<i64>,
    /// Puffin DV blob offset/size + cardinality (set only for Puffin DV files).
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub cardinality: Option<u64>,
```

Fix every `WrittenFile { .. }` constructor the compiler flags by adding `content_offset: None, content_size_in_bytes: None, cardinality: None` (grep: `rg "WrittenFile \{" src/`).

- [ ] **Step 4: Map the fields + derive format in `convert`** (`collector.rs`)

Replace the hardcoded `format: DataFileFormat::Parquet,` and add the new fields:

```rust
        let format = match df.format.as_deref() {
            Some("puffin") => DataFileFormat::Puffin,
            _ => DataFileFormat::Parquet,
        };
        Ok(WrittenFile {
            path,
            format,
            content,
            // ... existing fields unchanged ...
            first_row_id: df.first_row_id,
            content_offset: df.content_offset,
            content_size_in_bytes: df.content_size_in_bytes,
            cardinality: df.cardinality.map(|c| c.max(0) as u64),
        })
```

- [ ] **Step 5: Run test — expect PASS**

Run: `cargo test --lib commit::collector::tests::convert_preserves_puffin_dv_descriptor`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/collector.rs
git commit -m "feat(commit): WrittenFile carries Puffin DV descriptor; convert honors format"
```

---

## Task 3: FE sink mode `DeletionVectors` → `ICEBERG_DV_SINK`

**Files:**
- Modify: `src/sql/codegen/iceberg_write_sink.rs:27-41`
- Test: `src/sql/codegen/iceberg_write_sink.rs` tests (pattern at `:460-471`)

- [ ] **Step 1: Failing test**

```rust
#[test]
fn dv_mode_maps_to_iceberg_dv_sink() {
    let mut spec = test_support::simple_sink_spec();
    spec.mode = IcebergWriteSinkMode::DeletionVectors;
    let sink = spec.build_sink(0);
    assert_eq!(sink.type_, data_sinks::TDataSinkType::ICEBERG_DV_SINK);
}
```

- [ ] **Step 2: Run — expect FAIL** (`no variant DeletionVectors`)

Run: `cargo test --lib codegen::iceberg_write_sink::tests::dv_mode_maps_to_iceberg_dv_sink`
Expected: FAIL.

- [ ] **Step 3: Add the variant + mapping**

```rust
pub(crate) enum IcebergWriteSinkMode {
    Data,
    RowLineageData,
    PositionDeletes,
    DeletionVectors,
}

impl IcebergWriteSinkMode {
    fn data_sink_type(self) -> data_sinks::TDataSinkType {
        match self {
            Self::Data | Self::RowLineageData => data_sinks::TDataSinkType::ICEBERG_TABLE_SINK,
            Self::PositionDeletes => data_sinks::TDataSinkType::ICEBERG_DELETE_SINK,
            Self::DeletionVectors => data_sinks::TDataSinkType::ICEBERG_DV_SINK,
        }
    }
}
```

- [ ] **Step 4: Run — expect PASS**; **Step 5: Commit**

```bash
git add src/sql/codegen/iceberg_write_sink.rs
git commit -m "feat(codegen): add DeletionVectors sink mode -> ICEBERG_DV_SINK"
```

---

## Task 4: BE lowering maps `ICEBERG_DV_SINK` → `IcebergSinkMode::DeletionVectors`

**Files:**
- Modify: `src/connector/iceberg/sink.rs:86-90` (add enum variant)
- Modify: `src/lower/fragment.rs:512-561` (mode dispatch)
- Test: inline test in `fragment.rs` or `sink.rs`

- [ ] **Step 1: Add the BE enum variant**

```rust
pub enum IcebergSinkMode {
    Data,
    PositionDeletes,
    DeletionVectors,
}
```

- [ ] **Step 2: Failing test** (lowering picks the mode from sink type) — add to `fragment.rs` tests:

```rust
#[test]
fn iceberg_dv_sink_lowers_to_deletion_vectors_mode() {
    let mode = super::iceberg_sink_mode_for_type(data_sinks::TDataSinkType::ICEBERG_DV_SINK);
    assert_eq!(mode, crate::connector::iceberg::sink::IcebergSinkMode::DeletionVectors);
}
```

(If the mapping is inline in `fragment.rs:530-534` rather than a named fn, first extract it into `fn iceberg_sink_mode_for_type(t: TDataSinkType) -> IcebergSinkMode` — a pure refactor — then write the test.)

- [ ] **Step 3: Run — expect FAIL; Step 4: Implement the mapping**

```rust
fn iceberg_sink_mode_for_type(t: data_sinks::TDataSinkType) -> IcebergSinkMode {
    match t {
        data_sinks::TDataSinkType::ICEBERG_DELETE_SINK => IcebergSinkMode::PositionDeletes,
        data_sinks::TDataSinkType::ICEBERG_DV_SINK => IcebergSinkMode::DeletionVectors,
        _ => IcebergSinkMode::Data,
    }
}
```

Also add a `write_chunk` dispatch arm in `sink.rs:570-582`: `IcebergSinkMode::DeletionVectors => self.push_chunk_deletion_vector(state, chunk)` (the fn is added in Task 5; until then stub it to `unimplemented!()` so this compiles — Task 5 replaces the stub and its test makes it real).

- [ ] **Step 5: Run — expect PASS; Step 6: Commit**

```bash
git add src/connector/iceberg/sink.rs src/lower/fragment.rs
git commit -m "feat(lower): map ICEBERG_DV_SINK to DeletionVectors sink mode"
```

---

## Task 5: BE writer `push_chunk_deletion_vector` (read existing DV → merge → write Puffin → report)

This is the core. Structural template: `push_chunk_position_delete` (`sink.rs:765-887`) — same partition/`_file` grouping, FileIO selection (`build_staged_file_io`, `sink.rs:1437-1457`), and `state.add_sink_commit_info(...)` reporting. Difference: instead of writing a Parquet `[file_path,pos]` file, build a merged `DeletionVector` per `_file` and write a Puffin blob.

**APIs to use (all verified to exist):**
- `crate::connector::iceberg::commit::DeletionVector` — `insert(u64)`, `merge(&DeletionVector)`, `cardinality() -> u64` (`puffin_dv.rs:30-69`).
- `write_single_deletion_vector_puffin(file_io: &iceberg::io::FileIO, path: &str, referenced_data_file: &str, dv: &DeletionVector) -> Result<WrittenPuffinDv>` (`puffin_dv.rs:215`). `WrittenPuffinDv { path, referenced_data_file, cardinality, content_offset, content_size_in_bytes, file_size_in_bytes }`.
- Existing-DV read: `crate::connector::iceberg::scan_deletes::previously_deleted_positions_at_snapshot(table, snapshot_id, factory, normalizer, data_file_path_filter) -> Result<HashMap<String, RoaringTreemap>>` (`scan_deletes.rs:733`). Bridge `RoaringTreemap` → `DeletionVector` via `DeletionVector::insert`/`to_roaring_treemap` (`puffin_dv.rs:92`).
- `build_staged_file_io(data_location, s3_config)` (`sink.rs:1437`).

**Files:**
- Modify: `src/connector/iceberg/sink.rs` (add `push_chunk_deletion_vector`; the `IcebergSinkPlan` already carries `target_table_metadata`, `data_location`, `object_store_s3`, `target_partition_spec_id`, `position_delete_data_file_partitions`).
- Test: inline `sink.rs` test.

- [ ] **Step 1: Failing unit test** — feed a 2-row chunk for one `_file`, assert a Puffin DV `TSinkCommitInfo` is reported with the right descriptor.

```rust
#[test]
fn push_chunk_deletion_vector_reports_puffin_descriptor() {
    // Build a backend in DeletionVectors mode over a local-fs staging dir,
    // with target_table_metadata = a v3 table whose data file "f.parquet" has NO existing DV.
    let (backend, state) = test_support::dv_sink_backend_local(/* tmpdir */);
    // chunk columns: [_file: Utf8, _pos: Int64]  (+ partition source cols if partitioned)
    let chunk = test_support::chunk_file_pos(&[("f.parquet", 0), ("f.parquet", 2)]);
    backend.write_chunk(&state, chunk).expect("write");
    let infos = crate::runtime::sink_commit::list(state.finst_id());
    assert_eq!(infos.len(), 1);
    let df = infos[0].iceberg_data_file.as_ref().unwrap();
    assert_eq!(df.format.as_deref(), Some("puffin"));
    assert_eq!(df.file_content, Some(crate::types::TIcebergFileContent::POSITION_DELETES));
    assert_eq!(df.referenced_data_file.as_deref(), Some("f.parquet"));
    assert_eq!(df.cardinality, Some(2));
    assert!(df.content_offset.is_some() && df.content_size_in_bytes.is_some());
}
```

(Add `test_support::dv_sink_backend_local` + `chunk_file_pos` helpers mirroring the existing position-delete sink test fixtures — reuse `simple_sink_spec`/`single_bucket_partition_metadata_json` at `iceberg_write_sink.rs:280-345` and the `IcebergSinkPlan` construction used by existing `sink.rs` tests.)

- [ ] **Step 2: Run — expect FAIL** (`unimplemented!`).

Run: `cargo test --lib connector::iceberg::sink::tests::push_chunk_deletion_vector_reports_puffin_descriptor`

- [ ] **Step 3: Implement `push_chunk_deletion_vector`**

```rust
fn push_chunk_deletion_vector(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
    // 1. Materialize [_file, _pos] exactly like push_chunk_position_delete (reuse its
    //    eval_exprs + align_arrays_to_schema + build_position_delete_output_schema path).
    let batch = self.build_file_pos_batch(&chunk)?; // factor the shared head of push_chunk_position_delete

    // 2. Group row indices by referenced data file (_file column 0).
    let groups = self.group_positions_by_file(&batch)?; // HashMap<String /*file*/, Vec<i64> /*pos*/>
    if groups.is_empty() { return Ok(()); }

    let metadata = self.plan.target_table_metadata.as_ref()
        .ok_or("iceberg DV sink missing target table metadata")?;
    let file_io = build_staged_file_io(&self.plan.data_location, self.plan.object_store_s3.as_ref())?;

    // 3. Read existing DV positions for exactly the files this BE owns (per-_file shuffle
    //    guarantees this BE owns all positions for each file, so the merge is complete).
    let owned: std::collections::HashSet<String> = groups.keys().cloned().collect();
    let existing = read_existing_dv_positions(metadata, &file_io, &owned)?; // HashMap<String, DeletionVector>

    for (referenced, positions) in groups {
        let mut dv = existing.get(&referenced).cloned().unwrap_or_default();
        for pos in positions {
            if pos < 0 { return Err(format!("DV sink: negative position {pos}")); }
            dv.insert(pos as u64).map_err(|e| e.to_string())?;
        }
        let (path, partition_path) = self.build_file_path_with_prefix(state, &referenced, "dv")?;
        let path = format!("{path}.puffin");
        let written = block_on_iceberg(write_single_deletion_vector_puffin(
            &file_io, &path, &referenced, &dv,
        )).map_err(|e| e.to_string())?;

        let partition_values_descriptor = self.partition_descriptor_for_file(&referenced, metadata)?;
        let data_file = types::TIcebergDataFile {
            path: Some(written.path),
            format: Some("puffin".to_string()),
            record_count: Some(written.cardinality as i64),
            file_size_in_bytes: Some(written.file_size_in_bytes as i64),
            partition_path: Some(partition_path),
            file_content: Some(types::TIcebergFileContent::POSITION_DELETES),
            referenced_data_file: Some(written.referenced_data_file),
            content_offset: Some(written.content_offset),
            content_size_in_bytes: Some(written.content_size_in_bytes),
            cardinality: Some(written.cardinality as i64),
            partition_values_descriptor: Some(partition_values_descriptor),
            partition_spec_id: Some(self.referenced_file_spec_id(&referenced, metadata)?),
            ..Default::default()
        };
        state.add_sink_commit_info(types::TSinkCommitInfo {
            iceberg_data_file: Some(data_file),
            ..Default::default()
        });
    }
    Ok(())
}
```

Helper `read_existing_dv_positions(metadata, file_io, owned)`: build an `iceberg::table::Table` from `metadata` (the sink already holds `TableMetadata`), resolve current snapshot id, call `previously_deleted_positions_at_snapshot(&table, snapshot_id, &factory, &normalizer, |f| owned.contains(f))`, and convert each `RoaringTreemap` to a `DeletionVector`. Factor `partition_descriptor_for_file` / `referenced_file_spec_id` out of the existing `position_delete_data_file_partitions` index logic (`build_position_delete_data_file_partition_index`, `sink.rs:349`) — the DV partition comes from the referenced data file's spec, identical to position deletes.

- [ ] **Step 4: Run — expect PASS** (Puffin file written under the tmpdir; descriptor reported).

- [ ] **Step 5: Second test — existing-DV merge.** Seed the table metadata with an existing Puffin DV for `f.parquet` (positions {5}); delete pos {0,2}; assert reported `cardinality == 3` (merged) — pins that the BE reads+merges, not overwrites.

- [ ] **Step 6: Run both — expect PASS; Step 7: Commit**

```bash
git add src/connector/iceberg/sink.rs
git commit -m "feat(sink): BE-side DeletionVectors writer (read existing DV, merge, write Puffin)"
```

---

## Task 6: Per-`_file` hash shuffle in front of the DV sink

**Files:**
- Modify: `src/sql/optimizer/mod.rs:156-164` (accept a `root_required` override)
- Modify: `src/engine/mod.rs:3403-3458` (`execute_query_as_iceberg_write` threads it)
- Test: fragment-builder golden (pattern: `build_maps_hash_distribution_to_hash_partitioned_edge`, `fragment_builder.rs:8315`)

- [ ] **Step 1: Add an optimizer entry that takes a root distribution.** Keep `optimize(...)` as-is (defaults to `gather()`); add:

```rust
pub(crate) fn optimize_with_root_distribution(
    logical: LogicalPlanNode,
    table_stats: &TableStats,
    factory: ExprFactory,
    root_distribution: DistributionSpec,
) -> Result<PhysicalPlanNode, String> {
    // identical to optimize() but:
    let root_required = PhysicalPropertySet { distribution: root_distribution, ordering: OrderingSpec::Any };
    // ... same memo/search/extract using root_required ...
}
```

- [ ] **Step 2: Thread it through `execute_query_as_iceberg_write`.** Add a param `root_distribution: Option<DistributionSpec>`; when `Some`, call `optimize_with_root_distribution`, else `optimize`. Update the v2 call sites to pass `None`.

- [ ] **Step 3: Failing test** — assert a DV-mode write plan gets a `HASH_PARTITIONED` exchange keyed on `_file`. Model on `build_maps_hash_distribution_to_hash_partitioned_edge`: build a small physical plan with `PhysicalDistributionOp { spec: DistributionSpec::shuffle_agg([file_col_id]) }` at the root, run `build_with_iceberg_sink`, assert the inter-fragment edge's `TDataPartition.type_ == HASH_PARTITIONED` and the partition expr resolves to the `_file` slot.

- [ ] **Step 4: Run — expect FAIL; Step 5: At the DV-delete call site** (Task 8) pass `Some(DistributionSpec::shuffle_agg([file_col_id]))` where `file_col_id` is the `ColumnId` of the `_file` output column of the delete SELECT (resolve from the planned output columns; `_file` is the first projected column of `build_delete_position_sink_query`).

- [ ] **Step 6: Run — expect PASS; Step 7: Commit**

```bash
git add src/sql/optimizer/mod.rs src/engine/mod.rs
git commit -m "feat(optimizer): allow root distribution override for distributed DV-delete shuffle"
```

---

## Task 7: Metadata-only `RowDeltaDvFromFiles` commit

The existing `RowDeltaDvCommit` (`row_delta_dv.rs`) builds bitmaps + reads/merges existing DVs + writes Puffin at commit. The from-files variant **skips** those (BE already did them) and registers the BE-written Puffin DV `WrittenFile`s. Keep: referenced-file-is-live check, `validate_delete_file_for_row_lineage`, carry-forward of untouched delete entries, added-DV manifest (`dv_data_file`/`write_added_dv_manifest`), manifest list + snapshot + summary + OCC requirements.

**Files:**
- Create: `src/connector/iceberg/commit/row_delta_dv_from_files.rs`
- Modify: `src/connector/iceberg/commit/types.rs:23-54` (`CommitOpKind::RowDeltaDvFromFiles`)
- Modify: `src/connector/iceberg/commit/run.rs:96-120` (dispatch)
- Modify: `src/connector/iceberg/commit/mod.rs` (module decl)
- Test: SQL-level (Task 10) + a focused commit unit test if a fixture table is available.

- [ ] **Step 1: Add the op kind**

```rust
pub enum CommitOpKind {
    FastAppend, Overwrite, RowDelta, RowDeltaDv,
    RowDeltaDvFromFiles,
    RewriteDataFiles, CowUpdate, Truncate, OverwritePartitions, RewriteManifests,
}
```

- [ ] **Step 2: Implement `RowDeltaDvFromFilesCommit`.** Take `ctx.collector.take_written_files()` (now Puffin DV files: `format == Puffin`, `content == PositionDeletes`, with `content_offset/content_size_in_bytes/cardinality` populated — Task 2). For each, construct iceberg `DataFile` via a `dv_data_file_from_written` mirroring `dv_data_file` (`row_delta_dv.rs:789-803`) but reading `content_offset`/`content_size_in_bytes`/`record_count(cardinality)`/`file_size_in_bytes` from the `WrittenFile` instead of a `WrittenPuffinDv`. Reuse `build_snapshot_index`'s **metadata half** (live data-file index + untouched manifests + touched-delete carry-forward) but with the read+merge removed: mark a touched file's old DV entry replaced using its manifest-entry `record_count()`/`file_size_in_bytes()` (no `read_deletion_vector_puffin`). Emit the same manifest list / snapshot / `Operation::Delete` summary / OCC `TableRequirement`s as `RowDeltaDvTxnAction` (`row_delta_dv.rs:321-437`).

  Factor the shared metadata helpers (`build_snapshot_index` split into `build_live_index` + the replaced-marking, `write_added_dv_manifest`, `write_existing_delete_manifest`, `write_manifest_list`) into a `pub(super)` module shared by `row_delta_dv.rs` and `row_delta_dv_from_files.rs` to avoid duplication (DRY).

- [ ] **Step 3: Dispatch** (`run.rs`)

```rust
CommitOpKind::RowDeltaDvFromFiles => Box::new(RowDeltaDvFromFilesCommit),
```

- [ ] **Step 4: Build the lib + run existing DV commit tests to ensure no regression**

Run: `cargo build --lib && cargo test --lib commit::row_delta_dv`
Expected: PASS (shared-helper extraction didn't change `RowDeltaDvCommit` behavior).

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/row_delta_dv_from_files.rs src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/run.rs src/connector/iceberg/commit/mod.rs src/connector/iceberg/commit/row_delta_dv.rs
git commit -m "feat(commit): metadata-only RowDeltaDvFromFiles registering BE-written Puffin DVs"
```

---

## Task 8: Distributed DV-delete executor + wire into `execute_delete_statement`

**Files:**
- Modify: `src/engine/delete_flow.rs` (replace v3-DV branch `155-207`; add `DistributedDvDeleteWriteExecutor`; rewrite `run_delete_dv_write_transaction`)
- Test: source-introspection guard (below).

- [ ] **Step 1: Failing guard test** (append to `delete_flow.rs` `#[cfg(test)] mod tests`, header `:1787`)

```rust
#[test]
fn dv_delete_uses_distributed_dv_sink_not_local_collect() {
    let source = include_str!("delete_flow.rs");
    let branch = source
        .split("IcebergSqlDeleteStrategy::DeletionVectors").nth(1).expect("DV branch")
        .split("let resolved = {").next().expect("DV branch body");
    assert!(branch.contains("execute_query_as_iceberg_write")
            || branch.contains("DistributedDvDeleteWriteExecutor"),
        "v3 DV-delete must use the distributed iceberg write path");
    assert!(!branch.contains("scan_for_position_deletes_at"),
        "v3 DV-delete must not collect position deletes in the coordinator");
    assert!(!branch.contains("InjectedDeleteGroupExecutor"),
        "v3 DV-delete must not use the local injected-delete-group executor");
}
```

- [ ] **Step 2: Run — expect FAIL** (branch still local).

- [ ] **Step 3: Add `DistributedDvDeleteWriteExecutor`** (clone of `DistributedDeleteWriteExecutor`, `delete_flow.rs:258-300`, with `root_distribution` for the `_file` shuffle):

```rust
struct DistributedDvDeleteWriteExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    delete_query: sqlparser::ast::Query,
    sink_spec: IcebergWriteSinkSpec,        // mode = DeletionVectors
    file_col_id: ColumnId,                   // _file output column
    commit_executor: IcebergWriteCommitExecutor,
}
impl IcebergWriteTransactionExecutor for DistributedDvDeleteWriteExecutor {
    fn run_coordinated_write(&self, _spec: &IcebergWriteTransactionSpec) -> Result<CoordinatedQueryResult, String> {
        let mut result = crate::engine::execute_query_as_iceberg_write(
            &self.state, Some(&self.target.catalog), &self.target.namespace,
            &self.delete_query, self.sink_spec.clone(),
            None,
            Some(DistributionSpec::shuffle_agg(vec![self.file_col_id])), // per-_file shuffle
        )?;
        if result.write_commit.as_ref().is_some_and(|c| !write_commit_has_files(c)) {
            result.write_commit = None;
        }
        Ok(result)
    }
    fn commit(&self, _s: &IcebergWriteTransactionSpec, wc: &WriteCommitInput) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_executor.commit_write_input(wc)
    }
    fn finalize(&self, _s: &IcebergWriteTransactionSpec) -> Result<(), String> { self.commit_executor.finalize() }
}
```

(Note: `execute_query_as_iceberg_write` gains the trailing `root_distribution` param in Task 6; update the v2 executor call to pass `None`.)

- [ ] **Step 4: Rewrite `run_delete_dv_write_transaction`** to build the DV sink spec (`mode = DeletionVectors`, columns `[_file,_pos,<partition src>]` via `position_delete_sink_input_columns`, `iceberg_writer.rs:476`), resolve `file_col_id` from the planned `_file` column, build collector with `CommitOpKind::RowDeltaDvFromFiles`, and run `IcebergWriteTransactionRunner` with `DistributedDvDeleteWriteExecutor`. Model the spec/collector setup on `run_delete_write_transaction` (`delete_flow.rs:400-464`).

- [ ] **Step 5: Replace the v3-DV branch** (`delete_flow.rs:155-207`) to call the rewritten `run_delete_dv_write_transaction` (now distributed) instead of `scan_for_position_deletes_at` + the local path.

- [ ] **Step 6: Run guard — expect PASS; run the whole delete_flow test module**

Run: `cargo test --lib engine::delete_flow::tests`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/engine/delete_flow.rs
git commit -m "feat(delete): cut v3 DV-delete to distributed DeletionVectors sink"
```

---

## Task 9: Remove the now-dead v3-DV local code

**Files:** `src/engine/delete_flow.rs`

- [ ] **Step 1: Delete** `InjectedDeleteGroupExecutor` (`302-336`), the old injected `run_delete_dv_write_transaction` body now replaced, `scan_for_position_deletes_at`/`scan_for_position_deletes` (`1076-1166`) and their DV-only preload helpers (`load_existing_delete_visibility_by_data_file_at` if unused elsewhere — grep first). Keep `local_writer_commit_input`/`has_preloaded_commit_output` (still used by COW/equality/MERGE until Phase 5).

- [ ] **Step 2: Build + run delete tests + the guard**

Run: `cargo build --lib && cargo test --lib engine::delete_flow::tests`
Expected: PASS; guard still green; no `unused` warnings for the deleted helpers (grep to confirm no dangling references).

- [ ] **Step 3: Commit**

```bash
git add src/engine/delete_flow.rs
git commit -m "refactor(delete): remove dead v3-DV local scan/inject path"
```

---

## Task 10: End-to-end SQL test (all-in-one + 1FE+2BE)

**Files:**
- Create: `sql-tests/iceberg-dml/sql/dv_delete_distributed.sql`
- Create: `sql-tests/iceberg-dml/result/dv_delete_distributed.result`

- [ ] **Step 1: Write the case** (models `partition_evolution_v3_delete.sql`; exercises multi-file + existing-DV merge)

```sql
-- @order_sensitive=true
-- @tags=iceberg_dml,delete,dv
-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_dv_delete_distributed FORCE;
CREATE TABLE ${case_db}.t_dv_delete_distributed (id BIGINT, g BIGINT, v INT)
PARTITION BY bucket(g, 4)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO ${case_db}.t_dv_delete_distributed VALUES
  (1,10,100),(2,20,200),(3,30,300),(4,40,400),(5,50,500),(6,60,600);
DELETE FROM ${case_db}.t_dv_delete_distributed WHERE id IN (2,5);
DELETE FROM ${case_db}.t_dv_delete_distributed WHERE v > 550;   -- second delete merges into existing DV
-- query 2
SELECT COUNT(*) AS cnt FROM ${case_db}.t_dv_delete_distributed;
-- query 3
SELECT id, g, v FROM ${case_db}.t_dv_delete_distributed ORDER BY id;
-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_dv_delete_distributed FORCE;
```

- [ ] **Step 2: Record the expected result** (server up per CLAUDE.md §7.3)

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --only dv_delete_distributed --mode record`
Expected: writes `dv_delete_distributed.result` (`cnt=4`; rows id 1,3,4 remain — 6 deleted by `v>550`).

- [ ] **Step 3: Verify all-in-one**

Run: `... --suite iceberg-dml --only dv_delete_distributed --mode verify`
Expected: PASS.

- [ ] **Step 4: Verify 1FE+2BE** (the real distribution check)

```bash
cargo build --profile dev-opt
NOVAROCKS_BIN=target/dev-opt/novarocks NO_PROXY=127.0.0.1,localhost \
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --only dv_delete_distributed \
  --mode verify --cluster-mode cross-process --cluster-size 2 -j 1
```
Expected: PASS — identical result to all-in-one (proves per-`_file` shuffle + BE DV write + merged-DV correctness across 2 BEs).

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg-dml/sql/dv_delete_distributed.sql sql-tests/iceberg-dml/result/dv_delete_distributed.result
git commit -m "test(iceberg-dml): distributed v3 DV-delete e2e (all-in-one + 1FE+2BE)"
```

---

## Acceptance (Phase 1 done when all true)

- [ ] `dv_delete_uses_distributed_dv_sink_not_local_collect` guard passes; no `scan_for_position_deletes_at`/`InjectedDeleteGroupExecutor` in the DV branch.
- [ ] `dv_delete_distributed.sql` passes in **both** all-in-one and `--cluster-mode cross-process --cluster-size 2`, byte-identical.
- [ ] Existing `iceberg-dml` + `commit::row_delta_dv` tests still green (no regression in the central DV path; it stays for IMV refresh).
- [ ] FE writes no DV files: the Puffin DVs appear under `data/` written by the BE sink; the FE commit only writes manifests/metadata (inspect via the e2e run's object store, or assert in the commit unit test that `RowDeltaDvFromFilesCommit` performs no `write_single_deletion_vector_puffin`).
- [ ] `cargo fmt` + `cargo clippy --lib` clean.

## Out of scope (later phases)

MERGE matched-delete (Phase 2, reuses this DV sink + op-code routing), equality-delete (Phase 3), COW-update (Phase 4), and removing the shared `local_writer_commit_input`/`has_preloaded_commit_output` (Phase 5, after COW/equality/MERGE are cut over).

## Risk note (carried from spec §12)

The heaviest new logic is `read_existing_dv_positions` on the BE (building a `Table` read-view from `TableMetadata` + reading the snapshot's DV manifests). If constructing the read-view in the sink proves too coupled, fall back to passing the touched files' existing-DV manifest descriptors (path+offset+len) down into the sink via the sink thrift (an extra field on `TIcebergTableSink`) so the BE only does `read_deletion_vector_puffin` + merge — no snapshot walk. Decide during Task 5.
