# Iceberg/HDFS Scan Dictionary-Encode Execution (Option A) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the iceberg/HDFS scan execution layer the same global-dictionary encode pipeline the StarRocks lake scan already has, so the low-cardinality dictionary rewrite can run end-to-end on Iceberg-backed tables; then migrate the `low-cardinality` SQL suite to Iceberg v3.

**Architecture:** The StarRocks lake reader already turns a dict-encoded INT slot into a Utf8 storage read + an Arrow post-process that maps strings → dict ids (`build_scan_schema_for_global_dict_encoding` + `encode_batch_with_query_global_dicts` in `src/connector/starrocks/scan/reader.rs`). These helpers are storage-agnostic. We (1) lift them into a shared module, (2) carry a `QueryGlobalDictEncodeMap` into the parquet reader and apply the same Utf8-read-then-encode dance *inside* the parquet iterator (it must happen before `normalize_batch_to_chunk_schema`, which would otherwise try to `cast(Utf8 → Int32)` and fail), (3) build that encode map in `lower_hdfs_scan_node` from the fragment's `query_global_dict_map` (mirroring `lower_lake_scan_node`), (4) stop the standalone codegen guard from rejecting `dict_columns` on iceberg scans, (5) re-enable iceberg dictionary owners, (6) validate the Int32-slot interactions (predicate pushdown, v3 row-lineage, equality delete), and (7) migrate the suite.

**Tech Stack:** Rust, Apache Arrow (`RecordBatch`/`Chunk`/`ChunkSchema`), Iceberg v3 + parquet readers, StarRocks-compat thrift plan nodes, NovaRocks standalone SQL engine + sql-test-runner.

---

## Context the engineer must know before starting

**This worktree was fast-forwarded onto `origin/main` (`4f06699e`, the merged PR #217 "Migrate stable CI SQL suites to Iceberg v3 storage").** That is the state every file reference below assumes. If you are on a worktree at `e959a83e` you are one commit behind and the gate code below will not match — run `git merge --ff-only origin/main` first. Confirm with: `grep -n "IcebergDataFiles { .. } => Ok(None)" src/engine/dictionary/mod.rs` (expect a hit at ~line 330).

**Two scan-codegen paths, one lowering.** The FE-compatible path lowers StarRocks thrift `TPlanNode`s through `src/lower/node/*.rs`. The standalone path (`src/sql/codegen/fragment_builder.rs`) *also* emits thrift `TPlanNode`s (via `nodes::build_scan_node`) and lowers them through the **same** `src/lower/node/*.rs`. So the dict execution plumbing in `lower_hdfs_scan_node` + the parquet reader serves both. The `low-cardinality` suite runs through the standalone path.

**How the StarRocks lake dict path already works (the thing we mirror):**
- `fragment_builder.rs` (standalone codegen): the dict rewriter put `op.dict_columns` on the scan. fragment_builder allocates ONE `Int32` slot per dict column at the source column's storage position (lines ~590-611), emits a `TGlobalDict` payload onto every fragment in the stack (lines ~834-853, **unconditional — already runs for iceberg today**), and for a StarRocks `lake_scan_node` patches `dict_string_id_to_int_ids` (a self-map). For a non-StarRocks scan it currently **errors** at line ~811.
- `lower/fragment.rs:~255` builds a `QueryGlobalDictMap` (`HashMap<TSlotId, Arc<HashMap<i32, Vec<u8>>>>`, id→bytes) from `fragment.query_global_dicts` and threads it into `lower_plan_node`, which passes it to `lower_lake_scan_node`.
- `lower_lake_scan_node` calls `build_scan_query_global_dicts(scan_output_chunk_schema.slot_ids(), query_global_dict_map)` → a `QueryGlobalDictEncodeMap` (`HashMap<SlotId, Arc<HashMap<Vec<u8>, i32>>>`, bytes→id) and stores it on `StarRocksScanConfig.query_global_dicts`.
- The StarRocks reader (`reader.rs::open`) calls `build_scan_schema_for_global_dict_encoding` (rewrites the dict slot's read type Int32→Utf8) then `encode_batch_with_query_global_dicts` (Utf8→Int32 dict id).

**Key insight that simplifies this:** the encode map is built **purely** from `slot_ids` + `query_global_dict_map`; the thrift `dict_string_id_to_int_ids` field only drives a slot-id remap that is a **no-op self-map** in the standalone path. `THdfsScanNode` has **no** such field and **we do not add one** — the iceberg scan gets its dicts entirely from the (already-emitted) `query_global_dict_map`. So enabling iceberg is: thread the map into the HDFS lowering, build the encode map, apply it in the parquet reader, and stop the guard from erroring.

**Type names (verbatim):**
- `QueryGlobalDictEncodeMap = HashMap<SlotId, Arc<HashMap<Vec<u8>, i32>>>` — currently `src/connector/starrocks/scan/op.rs:38`. (`SlotId` = `crate::common::ids::SlotId`.)
- `QueryGlobalDictMap = HashMap<types::TSlotId, Arc<HashMap<i32, Vec<u8>>>>` — `src/lower/node/decode.rs:39`.
- `ChunkSchemaRef` = `crate::exec::chunk::ChunkSchemaRef`. Relevant methods: `.slots()`, `.slot_ids() -> &[SlotId]`, `.arrow_schema_ref() -> SchemaRef`, `ChunkSchema::try_ref_from_schema_and_slot_ids(&Schema, &[SlotId]) -> Result<ChunkSchemaRef, String>`, `.with_fields_in_order(Vec<Field>) -> Result<ChunkSchema, String>`.

**Build profile:** use `cargo build --profile dev-opt` (artifacts in `target/dev-opt/`) for the dev/test loop — near-release query speed, ~32s incremental rebuild. Use plain `cargo build` (debug) only for pure correctness checks. Never `--release` for iteration.

**SQL-test environment (verbatim from CLAUDE.md §7.3 / §8.4):**
```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
# start standalone-server in the background, gate first query on NOVAROCKS_READY:
LOG=/tmp/novarocks-server.log
NO_PROXY=127.0.0.1,localhost target/dev-opt/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { echo "server died"; tail -20 "$LOG"; exit 1; }
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timeout"; kill -9 "$SRV_PID"; exit 1; }
```
Run a suite:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite <suite> --mode <verify|record> -j 1
```

---

## File Structure

**New file:**
- `src/exec/dict_encode.rs` — shared, storage-agnostic dict encode helpers + `QueryGlobalDictEncodeMap`. Lives at `exec` level because both `formats::parquet` and `connector::starrocks::scan` depend on `exec` (putting it in `connector` would create a `formats → connector` cycle). One responsibility: Arrow string-column ↔ dict-id encoding driven by a slot-keyed map + a `ChunkSchema`.

**Modified — engine/lowering/codegen:**
- `src/exec/mod.rs` — register `pub mod dict_encode;`.
- `src/connector/starrocks/scan/op.rs` — re-export `QueryGlobalDictEncodeMap` from `dict_encode` (keep the public path stable).
- `src/connector/starrocks/scan/reader.rs` — import the 3 helpers from `dict_encode` instead of defining them; drop the moved code + moved test.
- `src/lower/node/decode.rs` — host `build_scan_query_global_dicts` as `pub(crate)` (shared by lake + hdfs lowering).
- `src/lower/node/lake_scan.rs` — drop its private `build_scan_query_global_dicts`/`ScanQueryGlobalDicts`, import from `decode`.
- `src/lower/node/hdfs_scan.rs` — new `query_global_dict_map` param; build the encode map; set `HdfsScanConfig.query_global_dicts`.
- `src/lower/node/mod.rs` — pass `query_global_dict_map` into `lower_hdfs_scan_node` at the `HDFS_SCAN_NODE` dispatch (~line 518).
- `src/connector/hdfs.rs` — `HdfsScanConfig.query_global_dicts` field; in `HdfsScanOp::execute_iter`, inject it into the parquet/orc format config before `build_format_iter`.
- `src/formats/parquet/mod.rs` — `ParquetScanConfig.query_global_dicts` field; `ParquetScanIter` precomputes a Utf8 "scan-read" `ChunkSchemaRef` and applies normalize-to-scan-read + encode.
- `src/sql/codegen/fragment_builder.rs` — relax the `dict_columns`-on-non-StarRocks guard (~line 807) to allow iceberg `hdfs_scan_node`; update the guard test (~line 6097).
- `src/engine/dictionary/mod.rs` — revert the `owner_for` iceberg gate (line ~330) + flip its gate test (~line 528).
- `src/engine/dictionary/rebuild.rs` — revert the `build_owner` iceberg gate (line ~116) + the doc comments.

**Modified — SQL suite (Task 7):**
- `sql-tests/low-cardinality/init.sql`, `cleanup.sql` (new), `sql/*.sql` (rewritten CREATE TABLEs), `result/*.result` (re-recorded).

---

## Task 1: Shared dict-encode module

**Files:**
- Create: `src/exec/dict_encode.rs`
- Modify: `src/exec/mod.rs`, `src/connector/starrocks/scan/op.rs:38`, `src/connector/starrocks/scan/reader.rs`

- [ ] **Step 1: Create the shared module** with the type alias + the 4 helpers moved verbatim from `reader.rs` (`is_integer_dict_code_type` lines ~628-641, `build_scan_schema_for_global_dict_encoding` 643-695, `encode_batch_with_query_global_dicts` 697-744, `encode_utf8_column_to_dict_ids` 746-826, `dict_scan_data_type_for_output` 828-841, `encode_column_to_dict_ids` 843-880) plus the moved unit test.

Create `src/exec/dict_encode.rs`:

```rust
//! Storage-agnostic global-dictionary encode helpers.
//!
//! A dict-encoded output column is declared `Int32` in the tuple/chunk
//! schema, but the underlying storage holds the original `Utf8` strings.
//! `build_scan_schema_for_global_dict_encoding` rewrites the scan-read type
//! back to `Utf8` so the reader materializes strings; once read,
//! `encode_batch_with_query_global_dicts` maps each string to its dict id
//! (per a query-global snapshot) and casts to the declared output type.
//!
//! These were lifted out of `connector::starrocks::scan::reader` so the
//! iceberg/HDFS (parquet) scan path can reuse the exact same Arrow
//! post-processing — the encode is independent of the storage format.

use crate::common::ids::SlotId;
use crate::exec::chunk::ChunkSchemaRef;
use crate::novarocks_logging::info;
use arrow::array::{Array, ArrayRef, Int32Builder, LargeStringArray, ListArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// `slot_id -> (string bytes -> dict id)` for every dict-encoded scan slot.
pub type QueryGlobalDictEncodeMap = HashMap<SlotId, Arc<HashMap<Vec<u8>, i32>>>;

fn is_integer_dict_code_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

// ... (move build_scan_schema_for_global_dict_encoding, encode_batch_with_query_global_dicts,
//      encode_utf8_column_to_dict_ids, dict_scan_data_type_for_output, encode_column_to_dict_ids
//      VERBATIM from reader.rs lines 643-880, changing only their visibility to `pub`
//      for build_scan_schema_for_global_dict_encoding + encode_batch_with_query_global_dicts.)
```

The two functions the readers call must be `pub`:
```rust
pub fn build_scan_schema_for_global_dict_encoding(
    output_schema: &SchemaRef,
    output_chunk_schema: &ChunkSchemaRef,
    query_global_dicts: &QueryGlobalDictEncodeMap,
) -> Result<(SchemaRef, bool), String> { /* body verbatim from reader.rs:643-695 */ }

pub fn encode_batch_with_query_global_dicts(
    scan_batch: RecordBatch,
    output_schema: &SchemaRef,
    output_chunk_schema: &ChunkSchemaRef,
    query_global_dicts: &QueryGlobalDictEncodeMap,
) -> Result<RecordBatch, String> { /* body verbatim from reader.rs:697-744 */ }
```
The other three (`encode_utf8_column_to_dict_ids`, `dict_scan_data_type_for_output`, `encode_column_to_dict_ids`) stay private (`fn`) in this module.

Move the test `encode_batch_with_query_global_dicts_maps_utf8_to_ids` (reader.rs:933-971) into a `#[cfg(test)] mod tests` here verbatim, fixing the `use super::...` line to pull `QueryGlobalDictEncodeMap, encode_batch_with_query_global_dicts` from this module.

- [ ] **Step 2: Register the module.** In `src/exec/mod.rs` add `pub mod dict_encode;` next to the other `pub mod` lines (e.g. after `pub mod chunk;`).

- [ ] **Step 3: Re-point `QueryGlobalDictEncodeMap`.** In `src/connector/starrocks/scan/op.rs:38`, replace the `pub type QueryGlobalDictEncodeMap = ...;` definition with a re-export:
```rust
pub use crate::exec::dict_encode::QueryGlobalDictEncodeMap;
```
(Keep the name reachable so existing `use super::op::QueryGlobalDictEncodeMap;` sites compile unchanged.)

- [ ] **Step 4: Trim `reader.rs`.** Delete lines 628-880 (the 6 functions: `is_integer_dict_code_type` through `encode_column_to_dict_ids`) and the moved test (933-971). Replace the import line 24 so the file still gets the type and now the helpers:
```rust
use super::op::LakeScanSchemaMeta;
use crate::exec::dict_encode::{
    QueryGlobalDictEncodeMap, build_scan_schema_for_global_dict_encoding,
    encode_batch_with_query_global_dicts,
};
```
Remove any now-unused imports in `reader.rs` that only the deleted helpers used (`Int32Builder`, `LargeStringArray`, `ListArray`, `arrow::compute::cast`, possibly `StringArray`) — let the compiler's `unused_imports` warnings drive this; keep imports still used by the rest of `reader.rs`.

- [ ] **Step 5: Build + run the moved test.**

Run: `cargo build --profile dev-opt 2>&1 | tail -30`
Expected: clean build (fix any unused-import warnings in `reader.rs` flagged as errors under `-D warnings` if the crate denies them — check `cargo build` output).

Run: `cargo test --lib dict_encode 2>&1 | tail -20`
Expected: `encode_batch_with_query_global_dicts_maps_utf8_to_ids ... ok` (and `schema_signature...` stays in reader.rs — it was NOT moved).

- [ ] **Step 6: Commit.**
```bash
git add src/exec/dict_encode.rs src/exec/mod.rs src/connector/starrocks/scan/op.rs src/connector/starrocks/scan/reader.rs
git commit -m "refactor(scan): lift global-dict encode helpers into shared exec::dict_encode"
```

---

## Task 2: Apply the encode in the parquet reader

**Files:**
- Modify: `src/connector/hdfs.rs:34` (struct), `src/connector/hdfs.rs:164-181` (execute_iter)
- Modify: `src/formats/parquet/mod.rs:199-211` (struct), `:457-490` (`ParquetScanIter::new`), `:985-991` (`next`)
- Test: `src/formats/parquet/mod.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing test** in the parquet `tests` module (near the existing parquet tests). It builds a `ParquetScanConfig` whose `chunk_schema` declares one slot `Int32`, hands the iterator-equivalent a Utf8 batch, and asserts the encode produces Int32 ids. Because `ParquetScanIter` reads real files, test the extracted method directly — add a small helper `fn encode_scan_batch(&self, batch) -> Result<RecordBatch,String>` (Step 4) and test *that*:

```rust
#[test]
fn parquet_scan_iter_encodes_dict_columns_utf8_to_int32() {
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::dict_encode::QueryGlobalDictEncodeMap;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    // Output chunk schema: dict slot declared Int32.
    let chunk_schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(7),
            Field::new("s", DataType::Int32, true),
            None,
            None,
        )])
        .expect("chunk schema"),
    );
    let mut dict_values = HashMap::new();
    dict_values.insert(b"a".to_vec(), 11);
    let mut dicts = QueryGlobalDictEncodeMap::new();
    dicts.insert(SlotId::new(7), Arc::new(dict_values));

    // The scan-read batch is Utf8 (what the parquet reader materializes once
    // the scan-read schema is rewritten to Utf8).
    let scan_read_schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let scan_batch = RecordBatch::try_new(
        scan_read_schema,
        vec![Arc::new(StringArray::from(vec![Some("a"), Some("z"), None]))],
    )
    .expect("scan batch");

    // build_scan_schema + encode (the exact operations ParquetScanIter::next does):
    let out_arrow = chunk_schema.arrow_schema_ref();
    let encoded = crate::exec::dict_encode::encode_batch_with_query_global_dicts(
        scan_batch, &out_arrow, &chunk_schema, &dicts,
    )
    .expect("encode");
    let ids = encoded.column(0).as_any().downcast_ref::<Int32Array>().expect("int32");
    assert_eq!(ids.value(0), 11);
    assert_eq!(ids.value(1), 0); // dict miss -> 0
    assert!(ids.is_null(2));
}
```

- [ ] **Step 2: Run it to confirm it fails to compile** (the field/method don't exist yet).

Run: `cargo test --lib -p novarocks parquet_scan_iter_encodes_dict 2>&1 | tail -20`
Expected: compile error (`QueryGlobalDictEncodeMap` import path fine after Task 1, but the test passes already because it only calls the shared helper). **If it already passes, that is fine** — it documents the contract; the real wiring is Steps 3-5 and is covered by the SQL suite in Task 7. Keep the test.

- [ ] **Step 3: Add the field to `HdfsScanConfig`** (`src/connector/hdfs.rs:34`). After `iceberg_table_locations`:
```rust
    /// Per-slot global dictionary encode maps (string bytes -> dict id) for
    /// dict-encoded output columns. Empty for all non-dict scans. Injected
    /// into the format config in `execute_iter`; the format reader rewrites
    /// the dict slot's read type to Utf8 and encodes the strings to ids.
    pub query_global_dicts: crate::exec::dict_encode::QueryGlobalDictEncodeMap,
```
Then update every `HdfsScanConfig { .. }` literal so it compiles. Find them:
```bash
grep -rn "HdfsScanConfig {" src/
```
Add `query_global_dicts: Default::default(),` to each (the lowering site in `hdfs_scan.rs:1197` will be set for real in Task 3; the in-file test literals at `hdfs.rs:708,744,803,834,860` get `Default::default()`).

- [ ] **Step 4: Add the field to `ParquetScanConfig`** (`src/formats/parquet/mod.rs:199-211`), after `iceberg_output_schema`:
```rust
    /// Per-slot global dictionary encode maps. Non-empty only for dict-encoded
    /// scans. When set, the iterator reads the dict columns as Utf8 and maps
    /// them to Int32 dict ids (see `ParquetScanIter`).
    pub query_global_dicts: crate::exec::dict_encode::QueryGlobalDictEncodeMap,
```
Update every `ParquetScanConfig { .. }` literal (`grep -rn "ParquetScanConfig {" src/`) with `query_global_dicts: Default::default(),` — including `src/lower/node/hdfs_scan.rs:1153` (set for real in Task 3) and any parquet-module test literals.

- [ ] **Step 5: Inject the map in `execute_iter`** (`src/connector/hdfs.rs`, in the `format = match format { ... }` block at lines 167-180). For the Parquet arm, after the datacache line, set the map; for ORC, leave it (ORC dict support is out of scope — see Validation):
```rust
        format = match format {
            FileFormatConfig::Parquet(mut parquet_cfg) => {
                parquet_cfg.datacache = parquet_cfg
                    .datacache
                    .with_external_range_options(external_datacache.as_ref())?;
                parquet_cfg.query_global_dicts = self.cfg.query_global_dicts.clone();
                FileFormatConfig::Parquet(parquet_cfg)
            }
            FileFormatConfig::Orc(mut orc_cfg) => {
                orc_cfg.datacache = orc_cfg
                    .datacache
                    .with_external_range_options(external_datacache.as_ref())?;
                FileFormatConfig::Orc(orc_cfg)
            }
        };
```

- [ ] **Step 6: Wire the encode into `ParquetScanIter`.** In `ParquetScanIter::new` (~line 457-490) precompute, once, a Utf8 "scan-read" chunk schema (equal to `cfg.chunk_schema` when there are no dicts), and store it + a flag on the struct. Add struct fields:
```rust
    scan_read_chunk_schema: ChunkSchemaRef,
    has_dict_encoded_output: bool,
```
In `new`, after `cfg` is in scope:
```rust
        let (scan_read_chunk_schema, has_dict_encoded_output) = if cfg.query_global_dicts.is_empty() {
            (cfg.chunk_schema.clone(), false)
        } else {
            let out_arrow = cfg.chunk_schema.arrow_schema_ref();
            let (scan_arrow, has_dict) =
                crate::exec::dict_encode::build_scan_schema_for_global_dict_encoding(
                    &out_arrow,
                    &cfg.chunk_schema,
                    &cfg.query_global_dicts,
                )?;
            if has_dict {
                let scan_chunk = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                    scan_arrow.as_ref(),
                    cfg.chunk_schema.slot_ids(),
                )?;
                (scan_chunk, true)
            } else {
                (cfg.chunk_schema.clone(), false)
            }
        };
```
> NOTE: confirm `ParquetScanIter::new` returns `Result`. If it returns `Self` (infallible) today, either (a) make it return `Result<Self, String>` and update `build_parquet_iter` (~line 257) to `?` it, or (b) build the scan-read schema in `build_parquet_iter` and pass it in. Prefer (a). Store both new fields in the returned `Self`.

In `next` (lines 985-991), change the normalize target to the scan-read schema and add the encode step:
```rust
                    let batch = match reorder_batch(&self.cfg, batch)
                        .and_then(|b| convert_variant_columns(&self.cfg, b))
                        .and_then(|b| {
                            normalize_batch_to_chunk_schema(b, &self.scan_read_chunk_schema)
                        })
                        .and_then(|b| {
                            if self.has_dict_encoded_output {
                                crate::exec::dict_encode::encode_batch_with_query_global_dicts(
                                    b,
                                    &self.cfg.chunk_schema.arrow_schema_ref(),
                                    &self.cfg.chunk_schema,
                                    &self.cfg.query_global_dicts,
                                )
                            } else {
                                Ok(b)
                            }
                        })
                    {
                        Ok(batch) => batch,
                        Err(e) => return Some(Err(e)),
                    };
```
The chunk-wrap at lines 1013-1024 is unchanged: after encode the batch fields are Int32 again, matching `self.cfg.chunk_schema`. When `query_global_dicts` is empty, `scan_read_chunk_schema == cfg.chunk_schema` and the encode branch is skipped → byte-for-byte identical to today.

- [ ] **Step 7: Build + test.**

Run: `cargo build --profile dev-opt 2>&1 | tail -30` → clean.
Run: `cargo test --lib parquet 2>&1 | tail -25` → all parquet tests pass, including the new one.

- [ ] **Step 8: Commit.**
```bash
git add src/connector/hdfs.rs src/formats/parquet/mod.rs
git commit -m "feat(scan): parquet reader encodes dict columns (Utf8 read -> Int32 ids)"
```

---

## Task 3: Build the encode map in the HDFS lowering

**Files:**
- Modify: `src/lower/node/decode.rs` (host the builder), `src/lower/node/lake_scan.rs:671-708` (drop dup), `src/lower/node/hdfs_scan.rs:346-355` + `:1197-1206`, `src/lower/node/mod.rs:518-527`

- [ ] **Step 1: Move `build_scan_query_global_dicts` to `decode.rs`.** Cut the function + its `ScanQueryGlobalDicts` alias from `lake_scan.rs:671-708` and paste into `src/lower/node/decode.rs` (which already defines `QueryGlobalDictMap`), making it `pub(crate)` and returning the shared type:
```rust
use crate::exec::dict_encode::QueryGlobalDictEncodeMap;

/// Convert a fragment's `QueryGlobalDictMap` (id -> bytes) into the
/// scan-side encode map (bytes -> id) restricted to `output_slots`.
pub(crate) fn build_scan_query_global_dicts(
    output_slots: &[SlotId],
    query_global_dict_map: &QueryGlobalDictMap,
) -> Result<QueryGlobalDictEncodeMap, String> {
    /* body verbatim from lake_scan.rs:676-707, return type now QueryGlobalDictEncodeMap */
}
```
Ensure `decode.rs` imports `SlotId` (`crate::common::ids::SlotId`) and `Arc`, `HashMap`, `info` as needed.

- [ ] **Step 2: Point `lake_scan.rs` at the moved builder.** In `lake_scan.rs`, add `use super::decode::build_scan_query_global_dicts;` (or `use crate::lower::node::decode::build_scan_query_global_dicts;`) and delete the now-removed local `type ScanQueryGlobalDicts` + `fn build_scan_query_global_dicts`. The call at `lake_scan.rs:533` is unchanged.

- [ ] **Step 3: Add the param to `lower_hdfs_scan_node`** (`hdfs_scan.rs:346`). Insert `query_global_dict_map: &QueryGlobalDictMap,` (import the type: `use crate::lower::node::decode::QueryGlobalDictMap;` if not already in scope) after `connectors: &ConnectorRegistry,`:
```rust
pub(crate) fn lower_hdfs_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    _tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    query_opts: Option<&internal_service::TQueryOptions>,
    connectors: &ConnectorRegistry,
    query_global_dict_map: &QueryGlobalDictMap,
    mut out_layout: Layout,
) -> Result<Lowered, String> {
```

- [ ] **Step 4: Build + set the encode map.** In `hdfs_scan.rs`, just before the `let cfg = HdfsScanConfig { ... };` at line 1197, build the map from the physical-data slot ids (`data_slot_ids`, the same slots the parquet `chunk_schema` uses — built at line 1149-1152):
```rust
    let query_global_dicts = crate::lower::node::decode::build_scan_query_global_dicts(
        &data_slot_ids,
        query_global_dict_map,
    )?;
```
Then add to the `HdfsScanConfig` literal (line 1197-1206):
```rust
    let cfg = HdfsScanConfig {
        ranges,
        original_range_count,
        has_more,
        limit,
        profile_label: Some(format!("hdfs_scan_node_id={}", node.node_id)),
        format,
        object_store_config: object_store_config.clone(),
        iceberg_table_locations,
        query_global_dicts,
    };
```
(`data_slot_ids` is a `Vec<SlotId>` already in scope at this point — confirm; if it is `&[SlotId]`/another shape, pass a slice. It is the argument to `try_ref_from_schema_and_slot_ids` at line 1151, so it is the right slot list.)

- [ ] **Step 5: Pass the map at the dispatch** (`src/lower/node/mod.rs:518-527`). Add `query_global_dict_map,` to the `lower_hdfs_scan_node(...)` call, in the new param's position (after `connectors,`):
```rust
        t if t == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE => lower_hdfs_scan_node(
            node,
            desc_tbl,
            tuple_slots,
            layout_hints,
            exec_params,
            query_opts,
            connectors,
            query_global_dict_map,
            out_layout,
        )?,
```
`query_global_dict_map` is already a parameter of the enclosing dispatch fn (it is passed to `lower_lake_scan_node` at line 546).

- [ ] **Step 6: Build.**

Run: `cargo build --profile dev-opt 2>&1 | tail -30`
Expected: clean. (No new unit test here — the behavior is covered end-to-end by Task 6's manual check and Task 7's suite. A lowering unit test would require hand-building a thrift `TPlanNode` + `TDescriptorTable` + a populated `QueryGlobalDictMap`, which is high-cost and brittle; the suite is the right level.)

- [ ] **Step 7: Commit.**
```bash
git add src/lower/node/decode.rs src/lower/node/lake_scan.rs src/lower/node/hdfs_scan.rs src/lower/node/mod.rs
git commit -m "feat(lower): build scan dict encode map for HDFS/iceberg scans"
```

---

## Task 4: Relax the standalone codegen guard for iceberg

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs:803-816` (guard), `:6075-6105` (the test that asserts the old error)

- [ ] **Step 1: Update the guard.** Replace lines 807-816:
```rust
        if !string_to_dict_slot.is_empty() {
            if let Some(lake) = scan_plan_node.lake_scan_node.as_mut() {
                lake.dict_string_id_to_int_ids = Some(string_to_dict_slot);
            } else {
                return Err(format!(
                    "scan `{}.{}` has dict_columns but is not a StarRocks lake scan",
                    op.database, op.table.name,
                ));
            }
        }
```
with:
```rust
        // StarRocks lake scans carry the dict slot self-map on the wire via
        // `TLakeScanNode.dict_string_id_to_int_ids`. Iceberg/HDFS scans have no
        // such thrift field and don't need one: the dict slot is already an
        // Int32 storage slot, and the per-fragment `TGlobalDict` payloads
        // emitted below feed `lower_hdfs_scan_node`'s encode map directly
        // (the parquet reader reads Utf8 and encodes to dict ids). So for an
        // iceberg `hdfs_scan_node` we leave the thrift node untouched. Any
        // other scan kind receiving dict_columns is a planning bug.
        if !string_to_dict_slot.is_empty() {
            if let Some(lake) = scan_plan_node.lake_scan_node.as_mut() {
                lake.dict_string_id_to_int_ids = Some(string_to_dict_slot);
            } else if scan_plan_node.hdfs_scan_node.is_some() {
                // iceberg/HDFS: dicts flow via query_global_dicts in lowering.
            } else {
                return Err(format!(
                    "scan `{}.{}` has dict_columns but is neither a StarRocks lake scan nor an iceberg/HDFS scan",
                    op.database, op.table.name,
                ));
            }
        }
```

- [ ] **Step 2: Update the guard test.** Read `fragment_builder.rs:6010-6105` to see the test (`grep -n "non-StarRocks scan with dict_columns must error" src/sql/codegen/fragment_builder.rs`). It builds an iceberg-source scan with a `dict_columns` entry and asserts `visit_scan` returns `Err`. Flip it to assert success now that iceberg is supported. Concretely, change the assertion block (~6095-6104) from the `Ok(_) => panic!(...)` / error-substring check to:
```rust
        // Iceberg scans now support dict execution (Option A): visit_scan must
        // succeed and the per-fragment TGlobalDict payload must be emitted.
        result.expect("iceberg scan with dict_columns must now succeed");
```
Adjust the surrounding test name/comment to match (rename `..._must_error` → `..._iceberg_dict_columns_is_supported` if the test fn name encodes the old intent). Keep the StarRocks-lake-scan positive tests (`fragment_builder.rs:5805-5807`, `5995-5998`) unchanged.

- [ ] **Step 3: Build + test.**

Run: `cargo build --profile dev-opt 2>&1 | tail -20` → clean.
Run: `cargo test --lib fragment_builder 2>&1 | tail -30` → all pass, including the flipped test.

- [ ] **Step 4: Commit.**
```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "feat(codegen): allow dict_columns on iceberg/HDFS scans (Option A)"
```

---

## Task 5: Re-enable iceberg dictionary owners

**Files:**
- Modify: `src/engine/dictionary/mod.rs:322-335` + the gate test `:528-...`
- Modify: `src/engine/dictionary/rebuild.rs:1-12` (doc), `:116-123` (build_owner)

- [ ] **Step 1: Revert `owner_for`** (`mod.rs`). Replace the gated arm:
```rust
            // Iceberg scans have no execution-layer dictionary-encode
            // support: ... Revisit if/when iceberg scan dict execution
            // support (Option A) lands.
            ScanSource::IcebergDataFiles { .. } => Ok(None),
```
with the original owner mapping:
```rust
            ScanSource::IcebergDataFiles { table: info, .. } => {
                Ok(Some(DictionaryOwner::IcebergTable {
                    catalog: info.catalog.clone(),
                    namespace: info.namespace.clone(),
                    table: info.table.clone(),
                    table_uuid: info.table_uuid.clone(),
                }))
            }
```

- [ ] **Step 2: Flip the gate test** (`mod.rs:~530`, `fn dictionary_provider_skips_iceberg_data_files_scan`). The fixture (Active iceberg snapshot + `IcebergDataFiles` table) stays; change the final assertion + name to require the snapshot is now found:
```rust
    /// Option A landed: iceberg scans DO participate in the low-cardinality
    /// dictionary rewrite (the iceberg/HDFS parquet scan path now encodes
    /// dict columns). owner_for must map IcebergDataFiles to an iceberg owner
    /// so an Active iceberg snapshot is loaded.
    #[test]
    fn dictionary_provider_loads_iceberg_data_files_snapshot() {
        // ... fixture unchanged ...
        let loaded = provider
            .load_active_snapshot(&table, "test_db", "s")
            .expect("load_active_snapshot returns Ok");
        let snapshot = loaded.expect("iceberg scans support dict execution; snapshot must load");
        assert_eq!(snapshot.column_name, "s");
        assert_eq!(snapshot.dictionary_id, 99);
    }
```

- [ ] **Step 3: Revert `build_owner`** (`rebuild.rs:116`). Replace:
```rust
        ScanSource::IcebergDataFiles { .. } => Ok(None),
```
with:
```rust
        ScanSource::IcebergDataFiles { table: info, .. } => {
            Ok(Some(DictionaryOwner::IcebergTable {
                catalog: info.catalog.clone(),
                namespace: info.namespace.clone(),
                table: info.table.clone(),
                table_uuid: info.table_uuid.clone(),
            }))
        }
```
(The iceberg watermark arm at `rebuild.rs:61` is already present and becomes reachable again.)

- [ ] **Step 4: Revert the doc comments** in `rebuild.rs:1-12` and the `rebuild_dictionaries` doc that the gate commit reworded — restore "Both StarRocks and Iceberg backends are handled". (Use `git show 545826b5 -- src/engine/dictionary/rebuild.rs` to see the exact pre-gate wording and restore it.)

- [ ] **Step 5: Build + test.**

Run: `cargo build --profile dev-opt 2>&1 | tail -20` → clean.
Run: `cargo test --lib dictionary 2>&1 | tail -30` → all pass, including the flipped owner test.

- [ ] **Step 6: Commit.**
```bash
git add src/engine/dictionary/mod.rs src/engine/dictionary/rebuild.rs
git commit -m "feat(dictionary): re-enable iceberg dict owners (Option A exec support landed)"
```

---

## Task 6: Engine-level verification (lib tests + manual iceberg dict query)

**Files:** none (verification only)

- [ ] **Step 1: Full lib build + tests.**

Run: `cargo build --profile dev-opt 2>&1 | tail -20`
Run: `cargo test --lib 2>&1 | tail -40`
Expected: only the **5 known pre-existing failures** in `mv_shape` / `pipeline::builder` (from main commit `45f6e676`, unrelated to this work — see memory `project_stable_suites_iceberg_v3.md`). **Zero new failures.** If a NEW failure appears, stop and debug (use superpowers:systematic-debugging) before continuing.

- [ ] **Step 2: Manual end-to-end iceberg dict check.** Bring up the env + server (see "SQL-test environment" above). Then drive a minimal iceberg dict scenario through the MySQL port. Build a script `/tmp/dict_smoke.sql`:
```sql
CREATE EXTERNAL CATALOG IF NOT EXISTS dict_smoke_cat PROPERTIES (
  "type"="iceberg",
  "iceberg.catalog.type"="hadoop",
  "iceberg.catalog.warehouse"="<value of $NOVAROCKS_ICEBERG_REST_WAREHOUSE or hadoop warehouse from env>",
  "aws.s3.access_key"="<oss_ak>", "aws.s3.secret_key"="<oss_sk>",
  "aws.s3.endpoint"="<oss_endpoint>", "aws.s3.enable_path_style_access"="true"
);
-- (use the same PROPERTIES the migrated suites use; read sql-tests/sort/init.sql and
--  $NOVAROCKS_SQL_TEST_CONFIG [env] for the concrete values)
SET catalog dict_smoke_cat;
CREATE DATABASE IF NOT EXISTS dsm;
CREATE TABLE dsm.t (k INT, s STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO dsm.t VALUES (1,'aa'),(2,'bb'),(3,'aa'),(4,'cc');
ANALYZE FULL TABLE dsm.t;
EXPLAIN VERBOSE SELECT DISTINCT s FROM dsm.t;     -- expect a DECODE node
SELECT DISTINCT s FROM dsm.t ORDER BY s;          -- expect aa,bb,cc (correct strings)
SELECT s, count(*) FROM dsm.t GROUP BY s ORDER BY s;  -- expect aa,2 / bb,1 / cc,1
DROP DATABASE dsm; DROP CATALOG dict_smoke_cat;
```
Run via mysql client on `$NOVA_ENV_MYSQL_PORT` (or the sql-test-runner with a one-off case). 

Expected: `EXPLAIN VERBOSE` output **contains `DECODE`**; the `SELECT DISTINCT` returns the original strings (proving encode→exec→decode round-trips); group-by counts are correct. If the strings come back wrong/empty or a dict-miss error fires, debug before Task 7.

- [ ] **Step 3: Validation audit (Int32 dict slot interactions).** While the server is up, probe the three risk areas (these inform Task 7 and any extra fixes):
  - **Predicate pushdown:** `EXPLAIN VERBOSE SELECT s FROM dsm.t WHERE s = 'aa';` and run it. Confirm the result is correct and the plan does not push a malformed Int32-vs-Utf8 min/max predicate into the iceberg scan. (If `WHERE s = 'aa'` returns wrong rows or errors, the dict-domain predicate rewrite vs. scan pushdown needs a fix — capture the EXPLAIN and debug; likely the predicate must stay above the scan or be rewritten to the dict id. Note findings in the task's commit message.)
  - **v3 row-lineage:** `SELECT _row_id, s FROM dsm.t ORDER BY _row_id;` — dict column coexisting with row-lineage metadata columns must return correct strings + ids.
  - **Equality delete:** `DELETE FROM dsm.t WHERE k = 1;` then re-`ANALYZE FULL` and re-run the DISTINCT/group-by — confirm no dict-miss and correct results after a delete. (Per the suite README, writes flip the dict to STALE; confirm the rewrite then falls back cleanly rather than using a stale dict.)

  Record any anomaly as a follow-up fix task (insert it before Task 7) using superpowers:systematic-debugging. If all three behave, proceed.

- [ ] **Step 4: No code → no commit.** (If Step 3 surfaced a fix, commit that fix with a descriptive message and note it here.)

---

## Task 7: Migrate the `low-cardinality` suite to Iceberg v3

**Files:**
- Create: `sql-tests/low-cardinality/init.sql`, `sql-tests/low-cardinality/cleanup.sql`
- Modify: `sql-tests/low-cardinality/sql/{rewrite,stale,disabled,compressed_key,compressed_key2}.sql`
- Re-record: `sql-tests/low-cardinality/result/{rewrite,stale,compressed_key,compressed_key2}.result`

> **DECISION (locked by user, 2026-05-31): keep `compressed_key.sql` + `compressed_key2.sql` on StarRocks native; migrate only the dict-behavior files (`rewrite`/`stale`/`disabled`) to Iceberg v3.** Rationale: `compressed_key*`'s 16+ `LARGEINT` columns test 128-bit scan min-max stats; Iceberg has no LARGEINT and maps it to `DECIMAL(38,0)` whose max (~9.99×10³⁷) **cannot hold ±2¹²⁷ (~1.7×10³⁸)**, so the boundary inserts (e.g. `compressed_key.sql:535/725`) would fail/truncate. Keeping them native preserves BOTH the dict-on-iceberg coverage (the migrated files) AND the true 128-bit min-max coverage. The suite becomes mixed-storage — this is intentional and must be documented in the suite README. **Catalog-context wrinkle:** `init.sql` activates an iceberg catalog suite-wide, so the native files must be pinned back to `default_catalog` (verify the runner's per-case `@catalog` override / qualified-name mechanism — see Step 4).

- [ ] **Step 1: Create `init.sql`** (`sql-tests/low-cardinality/init.sql`), mirroring `sql-tests/sort/init.sql`:
```sql
-- @catalog=lowcard_cat_${suite_uuid0}
CREATE EXTERNAL CATALOG IF NOT EXISTS `lowcard_cat_${suite_uuid0}`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="${iceberg_catalog_type}",
    "iceberg.catalog.warehouse"="${iceberg_catalog_warehouse}",
    "aws.s3.access_key"="${oss_ak}",
    "aws.s3.secret_key"="${oss_sk}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.enable_path_style_access"="true"
);
```

- [ ] **Step 2: Create `cleanup.sql`** (`sql-tests/low-cardinality/cleanup.sql`):
```sql
DROP CATALOG IF EXISTS `lowcard_cat_${suite_uuid0}`;
```

- [ ] **Step 3: Migrate the dict-behavior CREATE TABLEs** (`rewrite.sql`, `stale.sql`, `disabled.sql`). These are the true Option-A validation files (no LargeInt). Run the helper then verify:
```bash
python3 tools/dev/migrate_suite_iceberg_v3.py sql-tests/low-cardinality/sql
```
Then hand-verify the three small files now read (rewrite.sql example):
```sql
CREATE TABLE ${case_db}.dict_rewrite_t (
  k INT,
  s STRING,
  v INT
) TBLPROPERTIES ("format-version" = "3");
```
Keep the `ANALYZE FULL TABLE ${case_db}.dict_rewrite_t;`, the `-- @result_contains=DECODE` (rewrite), `-- @result_not_contains=DECODE` (stale, disabled), and all other `-- @` directives **unchanged**. (The helper is idempotent and CTAS-safe; it only strips native storage clauses + adds `format-version=3`.)

- [ ] **Step 4: Keep `compressed_key`/`compressed_key2` native (locked decision).** Step 3's helper edited every file in `sql/`; revert the two largeint files so they retain their native DDL:
```bash
git checkout -- sql-tests/low-cardinality/sql/compressed_key.sql sql-tests/low-cardinality/sql/compressed_key2.sql
```
Then ensure these two files run in `default_catalog`, not the suite's iceberg catalog. First **verify the runner's catalog mechanism**: read `tests/sql-test-runner/` for how a suite-level `init.sql` `-- @catalog=` directive sets the active catalog and whether a per-case `-- @catalog=default_catalog` directive (or `SET catalog default_catalog;` / fully-qualified `default_catalog.<db>.<table>` names) overrides it for an individual case. Apply whichever the runner supports to `compressed_key.sql` + `compressed_key2.sql` so their native `CREATE TABLE`/`ANALYZE`/`SELECT` execute against `default_catalog`. (If the runner has no per-case override and qualified names don't work, STOP and report — do not let native DDL hit the iceberg catalog, which would silently mangle the storage clauses.) Document the split in `sql-tests/low-cardinality/README.md` — add a short paragraph: "Dict-behavior cases (rewrite/stale/disabled) run on Iceberg v3 to exercise Option A dict execution; compressed_key/compressed_key2 stay on StarRocks native because they exercise true 128-bit (LARGEINT) min-max stats that don't survive the Iceberg LARGEINT→DECIMAL(38,0) mapping."

- [ ] **Step 5: Record goldens.** With env + server up:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite low-cardinality --mode record -j 1
```

- [ ] **Step 6: Diff-review the goldens.**
```bash
git diff sql-tests/low-cardinality/result/
```
Every change must be explainable by storage/type mapping (e.g. row order, decimal display), **not** a regression. The `rewrite.result`/`stale.result` are `@skip_result_check=true` plan assertions — confirm `rewrite` still shows `DECODE` and `stale`/`disabled` still show no `DECODE`. Any unexplained change → debug (do not accept blindly).

- [ ] **Step 7: Verify the suite.**
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite low-cardinality --mode verify -j 1
```
Expected: **all cases pass.** Specifically `rewrite.sql`'s `-- @result_contains=DECODE` holds on iceberg (the headline proof that Option A works end-to-end).

- [ ] **Step 8: Commit.**
```bash
git add sql-tests/low-cardinality/
git commit -m "test(low-cardinality): migrate dict-rewrite suite to Iceberg v3"
```

---

## Task 8: Regression — runtime-filter (grf_broadcast) + iceberg-* suites

**Files:** none (verification; re-record only if a golden legitimately shifts)

- [ ] **Step 1: runtime-filter suite.**
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite runtime-filter --mode verify -j 1
```
Expected: all pass (esp. `runtime_filter_push_down_grf_broadcast`). This suite was re-recorded for v3 in PR #217; our changes only add dict behavior to scans that have `dict_columns` (none here), so it must be untouched. If `grf_broadcast` regresses, our scan change leaked into a non-dict path — debug.

- [ ] **Step 2: iceberg-* suites** (no regression from the owner/guard/lowering changes):
```bash
for s in iceberg iceberg-ddl iceberg-dml iceberg-ivm iceberg-rest iceberg-compatibility; do
  echo "=== $s ==="
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$s" --mode verify -j 1 || echo "FAILED: $s"
done
```
Expected: all green (counts per design doc: iceberg 24, ddl 47, dml 37, ivm 62, rest 9, compatibility 12). These run Spark-write/NovaRocks-read and NovaRocks-only paths; none use `dict_columns`, so they validate we didn't disturb the normal (no-dict) iceberg scan path.

- [ ] **Step 3: Final whole-suite sanity** (optional but recommended): re-run the other migrated generic suites that touch scans heavily — `filter`, `join`, `sort` — `--mode verify -j 1`. Expected green.

- [ ] **Step 4: No-regression confirmation.** Summarize pass/fail counts. If everything is green, the feature is complete.

---

## Self-Review (completed during planning)

- **Spec coverage:** Task 1 ↔ scope item 1 (lift helpers); Task 2 ↔ item 2 (HdfsScanConfig field + apply encode); Task 3 ↔ item 3 (encode map + scan-read type rewrite in HDFS lowering — note: the type rewrite is realized inside the parquet reader via the lifted helper, driven by the lowering-built map, because the parquet `normalize_batch_to_chunk_schema` is the cast chokepoint); Task 4 ↔ item 4 (guard); Task 5 ↔ item 5 (owner gate); Task 6 ↔ item 6 (validation: predicate pushdown / row-lineage / equality-delete); Task 7 ↔ item 7 (suite migration + DECODE assertion); Task 8 ↔ the explicit runtime-filter + iceberg-* regression ask.
- **Deviation from scope wording (item 2 vs item 3):** the encode must run *inside* the parquet reader (before `normalize_batch_to_chunk_schema`'s `cast(Utf8→Int32)`), so the operative field is on `ParquetScanConfig`; `HdfsScanConfig` carries it as the connector-level source and `execute_iter` injects it into the parquet config. This is the only way to honor "apply encode to scan output batches" without the cast failing. Documented in Task 2.
- **No thrift change:** unlike `TLakeScanNode`, `THdfsScanNode` gets no `dict_string_id_to_int_ids` field; iceberg dicts flow purely via the already-emitted `query_global_dict_map`. This keeps us inside CLAUDE.md rule 1 (no speculative FE-contract changes).
- **Type consistency:** `QueryGlobalDictEncodeMap` (bytes→id) is the single shared type (moved to `exec::dict_encode`, re-exported from `op.rs`); `build_scan_query_global_dicts` returns it; `ParquetScanConfig`/`HdfsScanConfig` store it; the lake path's identical local alias `ScanQueryGlobalDicts` is removed.
- **Risk register (validate in Task 6 Step 3, decide in Task 7):** (1) dict-domain predicate pushdown onto an Int32 slot over Utf8 storage; (2) LargeInt→Decimal(38,0) overflow in `compressed_key`/`compressed_key2` — user decision required.
