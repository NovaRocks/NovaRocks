# IW-3 / SP1 — Shared Async Iceberg File-Writer Kernel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden `src/connector/iceberg/data_writer.rs` into a shared async Iceberg file-writer kernel whose inputs are decoupled from a concrete `iceberg::table::Table`, emitting a uniform `StagedDataFile` descriptor (native `DataFile` + optional theta sketches), with adapters to both output models — without changing any live write path or any existing caller's behavior.

**Architecture:** Introduce `StagedWriteContext` (the writer's inputs: schema, partition spec, `FileIO`, location generator, format, props) with a `from_table` constructor; a new core `write_record_batches(ctx, batches, opts) -> Vec<StagedDataFile>` and streaming `StagedDataFileWriter` that reuse the existing iceberg-rs partition/v3 write logic; an optional theta-sketch pass; pure adapters (`to_iceberg_data_file`, `to_sink_commit_info`) + a `cleanup_staged_files` helper. Existing `&Table` APIs become thin facades delegating to the new core, so MV refresh / `iceberg_writer.rs` / IVM sink are untouched.

**Tech Stack:** Rust, iceberg-rs (`DataFileWriter`, `RecordBatchPartitionSplitter`, `FileIO`, `DefaultLocationGenerator`), arrow `RecordBatch`, parquet. Spec: `docs/design/specs/2026-06-03-iw3-sp1-shared-iceberg-async-writer-kernel-design.md`.

**Conventions (from CLAUDE.md):**
- Code comments / logs / errors / commit messages: **English**. Commit messages: **no `Co-Authored-By` trailer**.
- Build: `cargo build` (dev). Tests: `cargo test`.
- Never run a bare repo-wide `cargo fmt`; scope it to touched files (`cargo fmt -- <file>`), then `git diff --stat` to confirm only those files changed.

**Implementation note on iceberg-rs intricacy:** Several steps say "mirror existing function `X` (file:line), sourcing inputs from `ctx` instead of `&table`." The existing code at those lines is the source of truth for the exact iceberg-rs API calls — preserve their bodies and only change where the inputs come from. The test code in each task is complete and is the contract. If an iceberg-rs signature differs from what a step shows, follow the existing function's usage, keep the test assertions, and report `DONE_WITH_CONCERNS` if a signature genuinely cannot satisfy the test.

---

## File Structure

All changes are confined to two files:

- **`src/connector/iceberg/data_writer.rs`** (primary): new types `StagedWriteContext`, `StagedDataFile`, `StagedWriteOptions`; new core `write_record_batches` + `StagedDataFileWriter`; adapters `to_iceberg_data_file` + `to_sink_commit_info`; `cleanup_staged_files`; facade rewrite of the existing `&Table` APIs; new tests.
- **`src/connector/iceberg/sink.rs`** (small): make `collect_theta_sketches` callable from `data_writer.rs` (change its visibility to `pub(crate)` / `pub(super)` and confirm its signature). **No behavior change to `IcebergTableSinkOperator`** (that is SP2).

Reference (do not modify) the existing functions you will mirror:
- `build_data_file_writer_with_schema` (`data_writer.rs:353-372`) — how the iceberg-rs writer builder is constructed from a table.
- `write_record_batches_as_data_files_with_writer` (`data_writer.rs:111-187`) — the unpartitioned/partitioned/v3 write loop.
- `IcebergStreamingDataFileWriter` (`data_writer.rs:47-73`) — the streaming facade to generalize.
- `unique_file_suffix` (`data_writer.rs:612`), `retag_data_file_partition_spec_id` (`data_writer.rs:189`).
- `IcebergTableSinkOperator::push_chunk_data` (`sink.rs:335-394`) — the exact `TIcebergDataFile`/`TSinkCommitInfo` field construction `to_sink_commit_info` must reproduce.

---

## Task 1: `StagedDataFile` + `StagedWriteOptions` + `StagedWriteContext::from_table`

**Files:**
- Modify: `src/connector/iceberg/data_writer.rs` (add types near the top, after the `type IcebergDataFileWriterBuilder = …` alias at line 24-25)

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` in `data_writer.rs` (after the existing `retag_unpartitioned_data_file_with_current_default_spec_id` test). This test builds a context from a table and confirms the context exposes the same schema the table has:

```rust
    #[tokio::test]
    async fn staged_write_context_from_table_exposes_schema_and_spec() {
        let table = build_unpartitioned_test_table("ctx_schema").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx from table");
        assert_eq!(
            ctx.schema().as_struct().fields().len(),
            table.metadata().current_schema().as_struct().fields().len(),
            "context schema must match table current schema"
        );
        assert_eq!(ctx.partition_spec_id(), table.metadata().default_partition_spec_id());
    }
```

This test uses a helper `build_unpartitioned_test_table` you will add in Step 3 below (shared by later tasks).

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --lib connector::iceberg::data_writer::tests::staged_write_context_from_table_exposes_schema_and_spec`
Expected: FAIL to compile (`StagedWriteContext` / `build_unpartitioned_test_table` not defined).

- [ ] **Step 3: Implement the types + `from_table` + the test-table helper**

Add the types after the `IcebergDataFileWriterBuilder` alias (around `data_writer.rs:25`):

```rust
use std::collections::HashMap;
use iceberg::spec::{PartitionSpecRef, Struct as IcebergStruct};
use super::theta_sketch::ThetaSketchHandle;

/// Per-file content kind for the staged writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StagedContent {
    Data,
    PositionDeletes,
}

/// Options controlling a staged write.
#[derive(Clone, Debug)]
pub(crate) struct StagedWriteOptions {
    /// Collect per-file theta sketches (StarRocks NDV stats). Off for standalone.
    pub collect_theta_sketches: bool,
    pub content: StagedContent,
}

impl Default for StagedWriteOptions {
    fn default() -> Self {
        Self {
            collect_theta_sketches: false,
            content: StagedContent::Data,
        }
    }
}

/// One staged data file plus optional per-file theta sketches.
///
/// `data_file` is the iceberg-rs native descriptor (path, record_count,
/// file_size, partition, column stats, split_offsets, content). `theta_sketches`
/// is populated only when `StagedWriteOptions::collect_theta_sketches` is set.
pub(crate) struct StagedDataFile {
    pub data_file: DataFile,
    pub theta_sketches: Option<HashMap<i32, ThetaSketchHandle>>,
}

/// Inputs the iceberg-rs data-file writer needs, decoupled from a concrete
/// `iceberg::table::Table` so both the standalone path (via `from_table`) and
/// the FE-compat path (via `from_parts`, added in SP2) can drive the kernel.
pub(crate) struct StagedWriteContext {
    metadata: Arc<iceberg::spec::TableMetadata>,
    file_io: iceberg::io::FileIO,
    writer_schema: SchemaRef,
    annotated_schema: arrow::datatypes::SchemaRef,
    partition_spec_id: i32,
}

impl StagedWriteContext {
    /// Standalone constructor: derive everything from a live table.
    pub(crate) fn from_table(table: &iceberg::table::Table) -> Result<Self, String> {
        let metadata = Arc::new(table.metadata().clone());
        let writer_schema = metadata.current_schema().clone();
        let annotated_schema = Arc::new(
            schema_to_arrow_schema(&writer_schema)
                .map_err(|e| format!("convert iceberg schema to arrow failed: {e}"))?,
        );
        Ok(Self {
            metadata,
            file_io: table.file_io().clone(),
            writer_schema,
            annotated_schema,
            partition_spec_id: table.metadata().default_partition_spec_id(),
        })
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.writer_schema
    }

    pub(crate) fn partition_spec(&self) -> PartitionSpecRef {
        self.metadata.default_partition_spec().clone()
    }

    pub(crate) fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub(crate) fn file_io(&self) -> &iceberg::io::FileIO {
        &self.file_io
    }

    /// Build the iceberg-rs data-file writer builder for this context.
    /// Mirrors `build_data_file_writer_with_schema` (data_writer.rs:353-372),
    /// sourcing metadata/file_io/schema from `self` instead of `&table`.
    pub(crate) fn data_file_writer_builder(&self) -> Result<IcebergDataFileWriterBuilder, String> {
        let location_generator =
            DefaultLocationGenerator::new(self.metadata.as_ref().clone())
                .map_err(|e| format!("build iceberg location generator failed: {e}"))?;
        let file_name_generator = DefaultFileNameGenerator::new(
            "novarocks".to_string(),
            Some(unique_file_suffix()),
            DataFileFormat::Parquet,
        );
        let parquet_builder =
            ParquetWriterBuilder::new(WriterProperties::default(), self.writer_schema.clone());
        let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            self.file_io.clone(),
            location_generator,
            file_name_generator,
        );
        Ok(DataFileWriterBuilder::new(rolling_builder))
    }
}
```

Add the test-table helper to the `tests` module (used by this and later tasks). Mirror the existing tests' table construction (see the tests that use `FileIO::new_with_fs()` at `data_writer.rs:982` / `:1091` / `:1127` — copy their table-build helper if one exists; otherwise this builds a minimal unpartitioned table over local fs):

```rust
    async fn build_unpartitioned_test_table(name: &str) -> iceberg::table::Table {
        // Reuse the existing test helper that builds a local-fs Iceberg table if
        // one exists in this module's tests (search for `FileIO::new_with_fs`).
        // It must return a Table whose data location is under file:///tmp so the
        // staged writer writes to local disk. Keep the schema simple: one
        // required int `id` and one optional utf8 `v`, both with field-id metadata.
        build_local_fs_test_table(name, /*partitioned=*/ false).await
    }
```

> If no reusable `build_local_fs_test_table` helper exists in the tests module, create it by following the table-construction pattern already used by the tests around `data_writer.rs:982-1130` (they build a `TableMetadata` + `FileIO::new_with_fs()` table). Keep it `async` and parameterized by a `partitioned: bool`. Put the helper in the `tests` module so Tasks 2-6 reuse it.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --lib connector::iceberg::data_writer::tests::staged_write_context_from_table_exposes_schema_and_spec`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/data_writer.rs
git commit -m "feat(iceberg-write): add StagedWriteContext/StagedDataFile types + from_table (IW-3/SP1)"
```

---

## Task 2: Core `write_record_batches(ctx, batches, opts) -> Vec<StagedDataFile>`

**Files:**
- Modify: `src/connector/iceberg/data_writer.rs`

- [ ] **Step 1: Write the failing tests (unpartitioned + partitioned)**

Add to the `tests` module:

```rust
    #[tokio::test]
    async fn write_record_batches_unpartitioned_produces_one_file_with_stats() {
        let table = build_unpartitioned_test_table("kernel_unpart").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let batch = test_batch(&[1, 2, 3]); // helper below: 3 rows
        let staged = write_record_batches(&ctx, vec![batch], &StagedWriteOptions::default())
            .await
            .expect("write");
        assert_eq!(staged.len(), 1, "one file for one unpartitioned batch");
        assert_eq!(staged[0].data_file.record_count(), 3);
        assert!(staged[0].data_file.file_size_in_bytes() > 0);
        assert!(staged[0].theta_sketches.is_none(), "sketches off by default");
        // File exists on the FileIO.
        let path = staged[0].data_file.file_path().to_string();
        assert!(ctx.file_io().exists(&path).await.expect("exists"), "staged file must exist");
    }

    #[tokio::test]
    async fn write_record_batches_partitioned_produces_file_per_partition() {
        let table = build_local_fs_test_table("kernel_part", /*partitioned=*/ true).await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        // 4 rows across 2 partition values (the partition column is `id % 2` per
        // build_local_fs_test_table's partition spec — adapt the helper so the
        // partition is by `id`'s identity for determinism).
        let batch = test_batch(&[0, 0, 1, 1]);
        let staged = write_record_batches(&ctx, vec![batch], &StagedWriteOptions::default())
            .await
            .expect("write");
        assert_eq!(staged.len(), 2, "one file per distinct partition value");
        let total: u64 = staged.iter().map(|s| s.data_file.record_count()).sum();
        assert_eq!(total, 4);
    }
```

Add the `test_batch` helper to the `tests` module (one int column `id` with field-id metadata matching the test table's field id):

```rust
    fn test_batch(ids: &[i32]) -> arrow::record_batch::RecordBatch {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        use std::collections::HashMap;
        let field = Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )]));
        let schema = Arc::new(Schema::new(vec![field]));
        arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))])
            .expect("test batch")
    }
```

> Ensure `build_local_fs_test_table(name, true)` builds a table whose partition spec partitions by the identity transform on `id` (so 2 distinct ids → 2 partitions deterministically) and whose schema is the single `id: int` column matching `test_batch`. Adjust `build_unpartitioned_test_table` to use the same single-`id` schema.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib connector::iceberg::data_writer::tests::write_record_batches_`
Expected: FAIL to compile (`write_record_batches` not defined).

- [ ] **Step 3: Implement `write_record_batches`**

Mirror `write_record_batches_as_data_files_with_writer` (`data_writer.rs:111-187`) but: source the builder from `ctx.data_file_writer_builder()`, use `ctx.schema()`/`ctx.partition_spec()`/`ctx.annotated_schema`, wrap each produced `DataFile` into a `StagedDataFile`, and retag the partition spec id with `ctx.partition_spec_id()`. Theta sketches stay `None` in this task (Task 4 adds them). Add:

```rust
/// Shared async kernel: write batches to staged data files, returning the
/// uniform descriptor. Does NOT commit; runs wherever the caller awaits it
/// (consumers run it on `sink_io`). Mirrors the partition/unpartition/v3 loop
/// of `write_record_batches_as_data_files_with_writer`.
pub(crate) async fn write_record_batches(
    ctx: &StagedWriteContext,
    batches: impl IntoIterator<Item = RecordBatch>,
    opts: &StagedWriteOptions,
) -> Result<Vec<StagedDataFile>, String> {
    let variant_indices = variant_field_indices(ctx.schema());
    let partition_spec = ctx.partition_spec();
    let builder = ctx.data_file_writer_builder()?;
    let mut out: Vec<StagedDataFile> = Vec::new();

    if partition_spec.fields().is_empty() {
        let mut writer = builder
            .build(None)
            .await
            .map_err(|e| format!("build iceberg data file writer failed: {e}"))?;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let staged_batch = if variant_indices.is_empty() {
                batch
            } else {
                transform_variant_columns_for_write(&batch, &ctx.annotated_schema, &variant_indices)?
            };
            let annotated = annotate_batch(&staged_batch, &ctx.annotated_schema)?;
            let sketches = maybe_collect_sketches(opts, &annotated)?;
            writer
                .write(annotated)
                .await
                .map_err(|e| format!("iceberg data file write failed: {e}"))?;
            for data_file in writer
                .close()
                .await
                .map_err(|e| format!("iceberg data file writer close failed: {e}"))?
            {
                let data_file = retag_data_file_partition_spec_id(data_file, ctx.partition_spec_id())?;
                out.push(StagedDataFile { data_file, theta_sketches: sketches.clone() });
            }
            // Re-open a fresh writer for the next batch (close consumed it).
            // NOTE: keep the per-batch writer lifecycle identical to the existing
            // code path; if `close()` consumes `writer`, rebuild it here.
        }
        return Ok(out);
    }

    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        ctx.schema().clone(),
        partition_spec.clone(),
    )
    .map_err(|e| format!("build iceberg partition splitter failed: {e}"))?;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let staged_batch = if variant_indices.is_empty() {
            batch
        } else {
            transform_variant_columns_for_write(&batch, &ctx.annotated_schema, &variant_indices)?
        };
        let annotated = annotate_batch(&staged_batch, &ctx.annotated_schema)?;
        let partitioned = splitter
            .split(&annotated)
            .map_err(|e| format!("split iceberg batch by partition spec failed: {e}"))?;
        for (partition_key, partition_batch) in partitioned {
            let sketches = maybe_collect_sketches(opts, &partition_batch)?;
            let mut writer = builder
                .build(Some(partition_key))
                .await
                .map_err(|e| format!("build iceberg partitioned data file writer failed: {e}"))?;
            writer
                .write(partition_batch)
                .await
                .map_err(|e| format!("iceberg partitioned data file write failed: {e}"))?;
            for data_file in writer
                .close()
                .await
                .map_err(|e| format!("iceberg partitioned data file writer close failed: {e}"))?
            {
                let data_file = retag_data_file_partition_spec_id(data_file, ctx.partition_spec_id())?;
                out.push(StagedDataFile { data_file, theta_sketches: sketches.clone() });
            }
        }
    }
    Ok(out)
}

/// Task 4 fills this in; until then it returns None.
fn maybe_collect_sketches(
    _opts: &StagedWriteOptions,
    _batch: &RecordBatch,
) -> Result<Option<HashMap<i32, ThetaSketchHandle>>, String> {
    Ok(None)
}
```

> The unpartitioned branch above re-builds the writer per batch implicitly; if the existing builder API requires one `build(None)` writer reused across all batches (as `write_record_batches_as_data_files_with_writer` does — it builds once, writes all batches, closes once), KEEP that exact shape instead: build once before the loop, write each batch, close once after the loop, and attach the per-batch sketches by collecting them into a `Vec` and pairing with the single close result. Match the existing function's lifecycle; do not invent a new one. The test only requires `record_count`/file existence to be correct.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib connector::iceberg::data_writer::tests::write_record_batches_`
Expected: both PASS. If `FileIO::exists` has a different name, use the iceberg-rs API the existing tests use to check file presence.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/data_writer.rs
git commit -m "feat(iceberg-write): core write_record_batches kernel over StagedWriteContext (IW-3/SP1)"
```

---

## Task 3: Streaming `StagedDataFileWriter`

**Files:**
- Modify: `src/connector/iceberg/data_writer.rs`

- [ ] **Step 1: Write the failing test**

```rust
    #[tokio::test]
    async fn streaming_writer_matches_batch_form() {
        let table = build_unpartitioned_test_table("kernel_stream").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let mut w = StagedDataFileWriter::new(ctx, StagedWriteOptions::default()).expect("new");
        w.write_batch(test_batch(&[1, 2])).await.expect("b1");
        w.write_batch(test_batch(&[3])).await.expect("b2");
        let staged = w.finish().await.expect("finish");
        let total: u64 = staged.iter().map(|s| s.data_file.record_count()).sum();
        assert_eq!(total, 3);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib connector::iceberg::data_writer::tests::streaming_writer_matches_batch_form`
Expected: FAIL (`StagedDataFileWriter` not defined).

- [ ] **Step 3: Implement `StagedDataFileWriter`**

Generalize `IcebergStreamingDataFileWriter` (`data_writer.rs:47-73`) to hold a `StagedWriteContext` + `StagedWriteOptions`, buffer batches, and delegate to `write_record_batches` on `finish` (same buffering approach the existing facade documents):

```rust
/// Streaming-shape facade over `write_record_batches`. Buffers batches and
/// writes them on `finish` (preserves per-batch backpressure surface for the
/// sink operator; a later optimization can stream without buffering).
pub(crate) struct StagedDataFileWriter {
    ctx: StagedWriteContext,
    opts: StagedWriteOptions,
    buffered: Vec<RecordBatch>,
}

impl StagedDataFileWriter {
    pub(crate) fn new(ctx: StagedWriteContext, opts: StagedWriteOptions) -> Result<Self, String> {
        Ok(Self { ctx, opts, buffered: Vec::new() })
    }

    pub(crate) async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), String> {
        if batch.num_rows() > 0 {
            self.buffered.push(batch);
        }
        Ok(())
    }

    pub(crate) async fn finish(self) -> Result<Vec<StagedDataFile>, String> {
        if self.buffered.is_empty() {
            return Ok(Vec::new());
        }
        write_record_batches(&self.ctx, self.buffered, &self.opts).await
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib connector::iceberg::data_writer::tests::streaming_writer_matches_batch_form`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/data_writer.rs
git commit -m "feat(iceberg-write): streaming StagedDataFileWriter over the kernel (IW-3/SP1)"
```

---

## Task 4: Optional theta-sketch pass

**Files:**
- Modify: `src/connector/iceberg/sink.rs` (visibility only), `src/connector/iceberg/data_writer.rs`

- [ ] **Step 1: Make `collect_theta_sketches` callable from the kernel**

In `src/connector/iceberg/sink.rs`, find `fn collect_theta_sketches(` (around `sink.rs:1180`). Confirm its signature is `fn collect_theta_sketches(batch: &RecordBatch) -> Option<HashMap<i32, ThetaSketchHandle>>` (or returns `Result<Option<...>, String>`). Change its visibility to `pub(crate)` (or `pub(super)`), leaving the body unchanged. If it has helper fns it depends on that are private, either keep them private (if it's self-contained) or also raise their visibility minimally. **Do not change its logic.**

- [ ] **Step 2: Write the failing test**

```rust
    #[tokio::test]
    async fn theta_sketches_collected_only_when_requested() {
        let table = build_unpartitioned_test_table("kernel_sketch").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let opts = StagedWriteOptions { collect_theta_sketches: true, content: StagedContent::Data };
        let staged = write_record_batches(&ctx, vec![test_batch(&[1, 2, 2, 3])], &opts)
            .await
            .expect("write");
        assert_eq!(staged.len(), 1);
        let sketches = staged[0].theta_sketches.as_ref().expect("sketches present");
        assert!(sketches.contains_key(&1), "theta sketch for field id 1 (id column)");

        // Off by default → None.
        let staged_off = write_record_batches(&ctx, vec![test_batch(&[1, 2])], &StagedWriteOptions::default())
            .await
            .expect("write off");
        assert!(staged_off[0].theta_sketches.is_none());
    }
```

- [ ] **Step 3: Run to verify it fails**

Run: `cargo test --lib connector::iceberg::data_writer::tests::theta_sketches_collected_only_when_requested`
Expected: FAIL (sketches always None — `maybe_collect_sketches` stub).

- [ ] **Step 4: Implement `maybe_collect_sketches`**

Replace the stub from Task 2 with a real implementation that calls the now-visible `collect_theta_sketches` on the annotated batch when requested:

```rust
fn maybe_collect_sketches(
    opts: &StagedWriteOptions,
    batch: &RecordBatch,
) -> Result<Option<HashMap<i32, ThetaSketchHandle>>, String> {
    if !opts.collect_theta_sketches {
        return Ok(None);
    }
    // `collect_theta_sketches` reads parquet field-id metadata from the batch
    // schema and returns per-field-id NDV sketches. The annotated batch carries
    // field-id metadata, so call it there.
    Ok(super::sink::collect_theta_sketches(batch))
}
```

> If `collect_theta_sketches` returns `Result<Option<...>, String>`, propagate with `?` instead of wrapping in `Ok(...)`. Match its real signature.

**Known limitation (document in a comment):** sketches are computed per write-batch and attached to every `DataFile` produced from that batch. When the rolling writer splits one batch into multiple files, each file carries the batch-level sketch (NDV is an estimate; per-file exactness is a later refinement). The default file size makes multi-file splits rare.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test --lib connector::iceberg::data_writer::tests::theta_sketches_collected_only_when_requested`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/sink.rs src/connector/iceberg/data_writer.rs
git commit -m "feat(iceberg-write): optional theta-sketch pass in the staged writer kernel (IW-3/SP1)"
```

---

## Task 5: Adapters `to_iceberg_data_file` + `to_sink_commit_info`

**Files:**
- Modify: `src/connector/iceberg/data_writer.rs`

- [ ] **Step 1: Write the failing tests**

```rust
    #[tokio::test]
    async fn to_iceberg_data_file_is_identity() {
        let table = build_unpartitioned_test_table("kernel_id").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let mut staged = write_record_batches(&ctx, vec![test_batch(&[1, 2, 3])], &StagedWriteOptions::default())
            .await
            .expect("write");
        let one = staged.remove(0);
        let path = one.data_file.file_path().to_string();
        let count = one.data_file.record_count();
        let df = to_iceberg_data_file(one);
        assert_eq!(df.file_path(), path);
        assert_eq!(df.record_count(), count);
    }

    #[tokio::test]
    async fn to_sink_commit_info_maps_fields_and_sketches() {
        let table = build_unpartitioned_test_table("kernel_commit").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let opts = StagedWriteOptions { collect_theta_sketches: true, content: StagedContent::Data };
        let staged = write_record_batches(&ctx, vec![test_batch(&[1, 2, 2, 3])], &opts)
            .await
            .expect("write");
        let s = &staged[0];
        let expected_path = s.data_file.file_path().to_string();
        let expected_count = s.data_file.record_count() as i64;
        let (commit, sketch_set) = to_sink_commit_info(
            s,
            /*partition_path=*/ String::new(),
            /*null_fingerprint=*/ String::new(),
            /*format=*/ "parquet".to_string(),
            crate::types::TIcebergFileContent::DATA,
        );
        let df = commit.iceberg_data_file.expect("iceberg data file");
        assert_eq!(df.path.as_deref(), Some(expected_path.as_str()));
        assert_eq!(df.record_count, Some(expected_count));
        assert_eq!(df.partition_path.as_deref(), Some(""));
        assert!(df.file_size_in_bytes.unwrap() > 0);
        assert!(df.column_stats.is_some(), "column stats mapped from DataFile");
        let sketch_set = sketch_set.expect("sketch set");
        assert_eq!(sketch_set.file_path, expected_path);
        assert!(sketch_set.sketches.contains_key(&1));
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --lib connector::iceberg::data_writer::tests::to_`
Expected: FAIL (`to_iceberg_data_file` / `to_sink_commit_info` not defined).

- [ ] **Step 3: Implement the adapters**

```rust
/// Standalone adapter: the kernel's descriptor IS the iceberg-rs DataFile.
pub(crate) fn to_iceberg_data_file(staged: StagedDataFile) -> DataFile {
    staged.data_file
}

/// FE-compat adapter: map the kernel descriptor to thrift `TSinkCommitInfo`,
/// reproducing the field set built by `IcebergTableSinkOperator::push_chunk_data`
/// (sink.rs:370-389). `partition_path` and `null_fingerprint` are supplied by the
/// caller because rendering a hive-style partition path from `DataFile.partition`
/// (a typed Struct) needs the FE-side partition context — that lives in SP2.
pub(crate) fn to_sink_commit_info(
    staged: &StagedDataFile,
    partition_path: String,
    null_fingerprint: String,
    format: String,
    content: crate::types::TIcebergFileContent,
) -> (crate::types::TSinkCommitInfo, Option<super::stats_assembler::FileSketchSet>) {
    use crate::types;
    let df = &staged.data_file;
    let column_stats = iceberg_data_file_to_column_stats(df);
    let split_offsets = df.split_offsets().map(|s| s.to_vec());
    let data_file = types::TIcebergDataFile {
        path: Some(df.file_path().to_string()),
        format: Some(format),
        record_count: Some(df.record_count() as i64),
        file_size_in_bytes: Some(df.file_size_in_bytes() as i64),
        partition_path: Some(partition_path),
        split_offsets,
        column_stats,
        partition_null_fingerprint: Some(null_fingerprint),
        file_content: Some(content),
        referenced_data_file: df.referenced_data_file().map(|s| s.to_string()),
    };
    let commit = types::TSinkCommitInfo {
        iceberg_data_file: Some(data_file),
        hive_file_info: None,
        is_overwrite: None,
        staging_dir: None,
        is_rewrite: None,
    };
    let sketch_set = staged.theta_sketches.as_ref().map(|sketches| {
        super::stats_assembler::FileSketchSet {
            file_path: df.file_path().to_string(),
            sketches: sketches.clone(),
        }
    });
    (commit, sketch_set)
}

/// Map iceberg-rs DataFile column stats → thrift TIcebergColumnStats.
/// Returns None when the DataFile carries no stats.
fn iceberg_data_file_to_column_stats(df: &DataFile) -> Option<crate::types::TIcebergColumnStats> {
    use crate::types::TIcebergColumnStats;
    let column_sizes = df.column_sizes();
    let value_counts = df.value_counts();
    let null_value_counts = df.null_value_counts();
    let nan_value_counts = df.nan_value_counts();
    let lower_bounds = df.lower_bounds();
    let upper_bounds = df.upper_bounds();
    if column_sizes.is_empty()
        && value_counts.is_empty()
        && null_value_counts.is_empty()
        && nan_value_counts.is_empty()
        && lower_bounds.is_empty()
        && upper_bounds.is_empty()
    {
        return None;
    }
    Some(TIcebergColumnStats {
        column_sizes: Some(column_sizes.iter().map(|(k, v)| (*k, *v as i64)).collect()),
        value_counts: Some(value_counts.iter().map(|(k, v)| (*k, *v as i64)).collect()),
        null_value_counts: Some(null_value_counts.iter().map(|(k, v)| (*k, *v as i64)).collect()),
        nan_value_counts: Some(nan_value_counts.iter().map(|(k, v)| (*k, *v as i64)).collect()),
        lower_bounds: Some(lower_bounds.iter().map(|(k, v)| (*k, datum_to_bytes(v))).collect()),
        upper_bounds: Some(upper_bounds.iter().map(|(k, v)| (*k, datum_to_bytes(v))).collect()),
    })
}
```

> `datum_to_bytes` converts an iceberg-rs bound value to the thrift `binary` (`Vec<u8>`) encoding. Iceberg lower/upper bounds are already stored as serialized bytes in iceberg-rs (`df.lower_bounds()` returns `HashMap<i32, Datum>`); use the iceberg-rs API that yields the bound's serialized bytes (e.g. `Datum::to_bytes()` / the literal's byte representation). If the exact API differs, follow how the codebase already serializes Iceberg bound literals elsewhere (search for existing `lower_bounds`/`Datum` → bytes conversions); the test only asserts `column_stats.is_some()`, so any correct byte encoding passes SP1. Exact StarRocks-FE byte compatibility is validated in SP2. If no straightforward serialization exists, set `lower_bounds`/`upper_bounds` to `None` for SP1 and add a `// SP2: map bounds bytes` comment, keeping the other stat maps.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test --lib connector::iceberg::data_writer::tests::to_`
Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/data_writer.rs
git commit -m "feat(iceberg-write): StagedDataFile adapters to DataFile and TSinkCommitInfo (IW-3/SP1)"
```

---

## Task 6: `cleanup_staged_files`

**Files:**
- Modify: `src/connector/iceberg/data_writer.rs`

- [ ] **Step 1: Write the failing test**

```rust
    #[tokio::test]
    async fn cleanup_staged_files_removes_written_files() {
        let table = build_unpartitioned_test_table("kernel_cleanup").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let staged = write_record_batches(&ctx, vec![test_batch(&[1, 2, 3])], &StagedWriteOptions::default())
            .await
            .expect("write");
        let path = staged[0].data_file.file_path().to_string();
        assert!(ctx.file_io().exists(&path).await.expect("exists before"));
        cleanup_staged_files(&ctx, &[path.clone()]).await.expect("cleanup");
        assert!(!ctx.file_io().exists(&path).await.expect("exists after"), "file removed");
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib connector::iceberg::data_writer::tests::cleanup_staged_files_removes_written_files`
Expected: FAIL (`cleanup_staged_files` not defined).

- [ ] **Step 3: Implement**

```rust
/// Best-effort deletion of staged files via the context's FileIO. Used on the
/// explicit error/cancel path while the caller still holds the future. Abort-time
/// orphan cleanup and commit-unknown recovery are out of scope here (IW-6).
pub(crate) async fn cleanup_staged_files(
    ctx: &StagedWriteContext,
    paths: &[String],
) -> Result<(), String> {
    for path in paths {
        ctx.file_io()
            .delete(path)
            .await
            .map_err(|e| format!("cleanup staged file {path} failed: {e}"))?;
    }
    Ok(())
}
```

> If iceberg-rs `FileIO` exposes deletion under a different name (`remove`/`delete_file`), use the one the codebase already uses; follow existing `FileIO` deletion call sites.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib connector::iceberg::data_writer::tests::cleanup_staged_files_removes_written_files`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/data_writer.rs
git commit -m "feat(iceberg-write): cleanup_staged_files helper for the writer kernel (IW-3/SP1)"
```

---

## Task 7: Additive-refactor facade (delegate existing `&Table` APIs to the kernel)

**Files:**
- Modify: `src/connector/iceberg/data_writer.rs`

- [ ] **Step 1: Run the existing tests first (baseline green)**

Run: `cargo test --lib connector::iceberg::data_writer`
Expected: all existing tests PASS (record the count). This is the regression baseline the facade must preserve.

- [ ] **Step 2: Rewrite `write_record_batches_as_data_files` to delegate**

Replace the body of `write_record_batches_as_data_files` (`data_writer.rs:75-93`) so it builds a context and delegates to the kernel, mapping `StagedDataFile` → `DataFile`:

```rust
pub(crate) async fn write_record_batches_as_data_files(
    table: &iceberg::table::Table,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> Result<Vec<DataFile>, String> {
    let ctx = StagedWriteContext::from_table(table)?;
    let staged = write_record_batches(&ctx, batches, &StagedWriteOptions::default()).await?;
    Ok(staged.into_iter().map(to_iceberg_data_file).collect())
}
```

> Keep `write_record_batches_as_data_files_with_schema` and `write_row_lineage_batches_as_data_files` working. If they need a schema-override that `from_table` doesn't capture, add a `StagedWriteContext::from_table_with_schema(table, schema)` constructor (mirror `build_data_file_writer_with_schema`'s schema override) and delegate those through it. Do NOT change their public signatures or behavior.

- [ ] **Step 3: Rewrite `IcebergStreamingDataFileWriter` to delegate**

Make the existing `IcebergStreamingDataFileWriter` (`data_writer.rs:47-73`) delegate to `StagedDataFileWriter` internally and map back to `Vec<DataFile>` on `finish` (preserve its `new(table)` / `write_record_batch` / `finish -> Vec<DataFile>` public surface so the IVM-A1 MV sink caller is unchanged):

```rust
pub(crate) struct IcebergStreamingDataFileWriter {
    inner: StagedDataFileWriter,
}

impl IcebergStreamingDataFileWriter {
    pub(crate) fn new(table: iceberg::table::Table) -> Result<Self, String> {
        let ctx = StagedWriteContext::from_table(&table)?;
        Ok(Self { inner: StagedDataFileWriter::new(ctx, StagedWriteOptions::default())? })
    }

    pub(crate) async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<(), String> {
        self.inner.write_batch(batch).await
    }

    pub(crate) async fn finish(self) -> Result<Vec<DataFile>, String> {
        Ok(self.inner.finish().await?.into_iter().map(to_iceberg_data_file).collect())
    }
}
```

- [ ] **Step 4: Run the full data_writer test suite + key callers**

Run: `cargo test --lib connector::iceberg::data_writer`
Expected: same pass count as Step 1 (existing tests still green) plus the new SP1 tests. Then run a caller smoke:
`cargo test --lib engine::mv:: 2>&1 | tail -5` and `cargo build` — expected clean (proves MV refresh / IVM / iceberg_writer still compile and their tests pass).

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/data_writer.rs
git commit -m "refactor(iceberg-write): delegate &Table writer APIs to the shared kernel (IW-3/SP1)"
```

---

## Task 8: Full regression + lint gate

**Files:** none (verification only)

- [ ] **Step 1: Build**

Run: `cargo build 2>&1 | tail -5`
Expected: clean (pre-existing warnings in unrelated files OK; no errors; no NEW warnings in `data_writer.rs`/`sink.rs`).

- [ ] **Step 2: Run iceberg connector + writer + MV tests**

Run: `cargo test --lib connector::iceberg 2>&1 | tail -15`
Then: `cargo test --lib engine::mv 2>&1 | tail -8`
Expected: all PASS (the new SP1 tests + all pre-existing iceberg/MV tests — proves the facade preserved behavior).

- [ ] **Step 3: Lint + format touched files**

Run:
```bash
cargo clippy --lib 2>&1 | grep -E "data_writer|sink\.rs" | grep -iE "warning|error" | head -40 || echo "no clippy issues in touched files"
cargo fmt -- src/connector/iceberg/data_writer.rs src/connector/iceberg/sink.rs
git diff --stat
```
Expected: no clippy issues in the touched files; `cargo fmt` changes only those two files (revert anything else with `git checkout --`).

- [ ] **Step 4: Commit any formatting**

```bash
git add src/connector/iceberg/data_writer.rs src/connector/iceberg/sink.rs
git commit -m "style(iceberg-write): fmt touched IW-3/SP1 files" || echo "nothing to format"
```

---

## Self-Review (completed during planning)

**Spec coverage:**
- `StagedWriteContext` decoupled from `Table` + `from_table` → Task 1. ✓ (`from_parts` is explicitly SP2.)
- `StagedDataFile` (DataFile + optional theta sketches) → Task 1 (type) + Task 2/4 (populated). ✓
- Core `write_record_batches` + streaming `StagedDataFileWriter` (reuse partition/v3 logic) → Tasks 2, 3. ✓
- Optional theta-sketch pass (port `collect_theta_sketches`) → Task 4. ✓
- Adapters `to_iceberg_data_file` + `to_sink_commit_info` (+ FileSketchSet) → Task 5. ✓ (partition_path/null_fingerprint as params, per spec.)
- `cleanup_staged_files` → Task 6. ✓
- Additive-refactor facade (existing `&Table` callers unchanged) → Task 7. ✓
- Test matrix (unpartitioned/partitioned/v3/theta/adapters/streaming/cleanup/empty/regression) → Tasks 2-7 + Task 8 regression. ✓ (v3-variant: the kernel reuses `transform_variant_columns_for_write`; an explicit v3 test is covered by the preserved existing variant tests in Task 7's baseline — add a dedicated kernel v3 test only if the existing suite lacks one.)
- No commit / no sink_io / abort-cleanup deferred to IW-6 → enforced by scope (no such code in any task). ✓

**Placeholder scan:** Impl steps for the intricate iceberg-rs refactor (Tasks 2, 5, 6) include explicit "mirror existing fn X / follow existing API" guidance with the real code shown; these are adaptation instructions against named existing functions, not deferred work. Test code is complete throughout. The `datum_to_bytes` and `FileIO::delete` notes give a concrete fallback so no step is blocked.

**Type consistency:** `StagedWriteContext`, `StagedDataFile`, `StagedWriteOptions`, `StagedContent`, `write_record_batches`, `StagedDataFileWriter`, `maybe_collect_sketches`, `to_iceberg_data_file`, `to_sink_commit_info`, `cleanup_staged_files`, and the test helpers `build_local_fs_test_table`/`build_unpartitioned_test_table`/`test_batch` are referenced with identical names/signatures across tasks.

**Empty-input** (spec test #9): covered by `StagedDataFileWriter::finish` early-return + `write_record_batches`'s `num_rows()==0` skips; add a one-line assertion in Task 3's test if desired (`w.finish()` on no batches → empty Vec).
