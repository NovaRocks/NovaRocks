# NovaRocks patches over upstream iceberg-rust 0.9.0

Upstream source: https://crates.io/crates/iceberg/0.9.0

These patches are the minimum required to let NovaRocks implement custom
Transaction actions for INSERT OVERWRITE and DELETE flows that iceberg-rust 0.9
does not yet ship as built-in actions (`overwrite_files`, `row_delta`).

When upstream lands native equivalents — likely in 0.10/0.11 — this whole
vendor directory and the corresponding `[patch.crates-io]` block in the root
`Cargo.toml` should be deleted, and the NovaRocks `OverwriteCommit` and
`RowDeltaCommit` impls (`src/connector/iceberg/commit/{overwrite,row_delta}.rs`)
should be re-pointed at the upstream actions.

Tracked under spec §0.4 / Plan Task 9.

## Patch 1 — `src/transaction/action.rs`

Raise `TransactionAction` trait visibility from `pub(crate)` to `pub` so that
downstream crates can implement the trait.

```diff
- #[async_trait]
- pub(crate) trait TransactionAction: AsAny + Sync + Send {
+ #[async_trait]
+ pub trait TransactionAction: AsAny + Sync + Send {
```

## Patch 2 — `src/catalog/mod.rs`

Raise `TableCommit::builder().build()` visibility from `pub(crate)` to `pub`
so that downstream crates can construct `TableCommit` directly when invoking
`Catalog::update_table` from a custom action.

```diff
- #[builder(build_method(vis = "pub(crate)"))]
+ #[builder(build_method(vis = "pub"))]
  pub struct TableCommit {
```

## Patch 3 — `src/arrow/record_batch_transformer.rs` (`_pos` / `_row_id` virtual columns)

iceberg-rust 0.9 declares the `_file`, `_pos`, and `_row_id` reserved metadata columns in
[`src/metadata_columns.rs`](src/metadata_columns.rs), and `TableScanBuilder`
accepts them in `select(...)`. Only `_file` is wired up in
[`src/arrow/reader.rs:422-427`](src/arrow/reader.rs:422); projecting `_pos`
reaches the `RecordBatchTransformer` with `RESERVED_FIELD_ID_POS` in
`projected_iceberg_field_ids` but no entry in `constant_fields` (it can't be
a constant — `_pos` is per-row), and the transformer falls through to the
"regular field" branch which can't find the field id in the data file's
schema and errors with `Unexpected => field not found`. `_row_id` has the same
per-row shape, but also needs the Iceberg v3 `first_row_id` assigned to the
data file.

This patch teaches the Arrow reader to inject `_pos` as a Parquet `RowNumber`
virtual column and lets `RecordBatchTransformer` either pass that column
through for `_pos` or derive `_row_id = first_row_id + _pos`. Because the row
number is produced by parquet's reader, both metadata columns continue to use
the original physical row number after `RowSelection`, predicate filters, or
row-group selection skip rows.

Concretely:

* When `_pos` or `_row_id` is projected, `arrow/reader.rs` adds a virtual
  Arrow field named `_pos` with Parquet `RowNumber` extension type and the
  Iceberg `_pos` reserved field id metadata.
* `FileScanTask` carries optional `first_row_id` from the data file manifest
  entry so v3 scans can derive `_row_id` without guessing.
* Schema-side branch in `generate_batch_transform`: when `field_id ==
  RESERVED_FIELD_ID_POS` or `field_id == RESERVED_FIELD_ID_ROW_ID`, emit the
  corresponding metadata field with `DataType::Int64` and field-id metadata.
  (Without this branch the transformer would still fall through to "field not
  found".)
* Operations-side branch in `generate_transform_operations`: when
  `field_id == RESERVED_FIELD_ID_POS`, use the reader-provided source field
  instead of looking for `_pos` in the table schema; when
  `field_id == RESERVED_FIELD_ID_ROW_ID`, require `first_row_id` and derive
  the row id from the reader-provided RowNumber column.
* `delete_file_loader.rs` calls the shared parquet-open helper with no virtual
  columns so position-delete file loading keeps the old behavior.

* `_row_id` stored-column override: when the parquet file physically contains a
  column tagged with `RESERVED_FIELD_ID_ROW_ID`, `generate_transform_operations`
  records its source index in `ColumnSource::RowId::stored_source_index`. At
  per-row materialization, non-NULL stored values take precedence over the
  `first_row_id + _pos` fallback. NULL stored values, missing stored columns,
  and the previous-patch-3 path all fall back unchanged.

No public API renames; downstream callers just see `_pos` and `_row_id`
working across both plain scans and row-selection scans.

Spec ref: <https://iceberg.apache.org/spec/#reserved-field-ids> — `_pos` =
2147483645 = `i32::MAX - 2`; `_row_id` = 2147483540 = `i32::MAX - 107`.

## Patch 4 — Puffin deletion-vector read support

iceberg-rust 0.9 ships full Puffin write/read primitives in
[`src/puffin`](src/puffin) but the scan-side delete-file loader
([`src/arrow/caching_delete_file_loader.rs`](src/arrow/caching_delete_file_loader.rs))
hard-codes a Parquet code path for every `DataContentType::PositionDeletes`
entry — Puffin `deletion-vector-v1` blobs (which are required to read any v3
table that has been row-lineage-deleted) crash with `Failed to load Parquet
metadata, Corrupt footer`. Upstream marks this with a `// TODO: Delete Vector
loader from Puffin files` comment.

This patch teaches the loader to recognise Puffin DV entries and decode them
into the existing [`DeleteVector`](src/delete_vector.rs) type so that
`build_deletes_row_selection` works without modification.

Concretely:

* `FileScanTaskDeleteFile` (`src/scan/task.rs`) gains four new
  `#[serde(default)]` fields: `file_format: DataFileFormat`,
  `referenced_data_file: Option<String>`, `content_offset: Option<i64>`, and
  `content_size_in_bytes: Option<i64>`. They are populated from the manifest
  entry by the existing `From<&DeleteFileContext>` impl. Defaulting
  `file_format` to `Parquet` keeps existing serialized tasks compatible.
* `BasicDeleteFileLoader::puffin_dv_to_delete_vector`
  (`src/arrow/delete_file_loader.rs`) reads the byte range
  `[content_offset, content_offset + content_size_in_bytes)` from the Puffin
  file via `FileIO` and decodes it as Iceberg `deletion-vector-v1` (BE length
  / magic `D1 D3 39 64` / LE bitmap-count / per-segment Roaring portable
  bitmap / BE CRC), producing a `DeleteVector` keyed off the referenced data
  file path.
* `CachingDeleteFileLoader::load_file_for_task`
  (`src/arrow/caching_delete_file_loader.rs`) routes
  `DataContentType::PositionDeletes` entries with
  `DataFileFormat::Puffin` to the new helper and returns a new
  `DeleteFileContext::PuffinDv` variant. `parse_file_content_for_task`
  converts that variant into a single-entry
  `ParsedDeleteFileContext::DelVecs { file_path: <puffin path>, results: {
  referenced_data_file => dv } }`, so the rest of the loader pipeline is
  unchanged.
* The existing test sites that build `FileScanTaskDeleteFile { … }`
  literally (`src/arrow/delete_filter.rs`, `src/arrow/reader.rs`,
  `src/arrow/caching_delete_file_loader.rs`) gain explicit
  `file_format: DataFileFormat::Parquet` and `None` defaults for the new
  Puffin fields.

Net change: ~150 lines across four files. Public API surface change is
limited to additive fields on `FileScanTaskDeleteFile`; deserialised
upstream tasks remain readable.

Spec ref:
<https://iceberg.apache.org/spec/#deletion-vector-files> and
<https://iceberg.apache.org/puffin-spec/>.

When this lands upstream (tracked under
[apache/iceberg-rust#1312](https://github.com/apache/iceberg-rust/issues/1312)
or successor) the helper can be deleted in favour of the upstream Puffin
loader.

## Patch 5 — `_last_updated_sequence_number` virtual column

iceberg-rust 0.9 declares `_last_updated_sequence_number` in
[`src/metadata_columns.rs`](src/metadata_columns.rs:65) but neither
`FileScanTask` nor `RecordBatchTransformer` carry the data-file
`data_sequence_number` needed to implement the column's spec-defined
fallback. This patch wires the field through.

Concretely:

* `FileScanTask` gains `data_sequence_number: Option<i64>` populated from
  the manifest entry's `data_sequence_number()` in
  `scan/context.rs::into_file_scan_task`.
* `RecordBatchTransformerBuilder::with_data_sequence_number(Option<i64>)`
  threads the value to the transformer.
* New `ColumnSource::LastUpdatedSeqNum { fallback_value, stored_source_index }`
  variant: when the parquet file physically stores a column tagged with
  `RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER`, non-NULL stored values
  take precedence; NULL/missing rows use the file's
  `data_sequence_number` as the spec-defined fallback.
* `arrow/reader.rs` calls `with_data_sequence_number(task.data_sequence_number)`
  on every transformer-builder chain so the value reaches the dispatch.

Spec ref: <https://iceberg.apache.org/spec/#row-lineage> —
`_last_updated_sequence_number` = 2147483539 = `i32::MAX - 108`.

## Verification after rebase

When bumping the vendored copy to a newer iceberg-rust patch release:

1. `diff -ru` against the new upstream source to confirm only those two lines
   diverge (plus this `PATCH.md` and the inline `// NovaRocks patch:` comments).
2. `cargo build -p novarocks` from the worktree root.
3. `cargo test -p novarocks --lib commit:: -- --nocapture` should still pass.

If upstream changes the surrounding code substantially, re-apply by hand and
update this file.
