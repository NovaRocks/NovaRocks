# NovaRocks patches over upstream iceberg-rust 0.9.0

Upstream source: https://crates.io/crates/iceberg/0.9.0

The remaining patches provide Iceberg format/read primitives, view support,
authoritative staged-create initialization and the public terminal TableCommit
builder. NovaRocks owns immutable commit intents, staging, retry and publication;
production mutation preparation does not use the SDK transaction/action layer.

## Retired transaction changes

P1 (public TransactionAction) and P9 (eager SDK staging/export) are removed.
The transaction sources are restored byte-for-byte to the published 0.9.0 crate,
including its backon dependency. U1's TableCommit mutation/query helpers are
removed. U6's MemoryCatalog test adaptations are restored to upstream synchronous
apply. Ordinary SDK test seeds use that upstream API; they grant no production
publication or cleanup authority.

## Patch 8 — authoritative REST staged-create initialization updates

`TableMetadata::staged_create_initialization_updates` converts the metadata
returned by a REST `stage-create=true` response into the initialization
updates for the first `assert-create` commit. Values come exclusively from
the authoritative response metadata, including UUID, location, schema/spec/
sort-order IDs, high-watermarks, format version, and properties. The helper
rejects non-initial metadata that the initialization update set cannot
represent exactly instead of guessing or silently dropping state.

## Patch 2 — `src/catalog/mod.rs`

Raise `TableCommit::builder().build()` visibility from `pub(crate)` to `pub`
so that the NovaRocks FrozenRequest owner can convert one complete terminal
request for `Catalog::update_table`. This boundary is retained through IRU-6;
custom SDK actions are no longer its caller.

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

## Patch 6 — `PrimitiveType::Variant` + Arrow Struct mapping

## Patch 7 — Lenient V3 metadata read for Spark REST tables without `next-row-id`

Some Spark + Iceberg 1.8.1 REST Catalog tables can be written with
`format-version=3` while omitting the top-level `next-row-id` field when
row-lineage is not explicitly enabled. Upstream iceberg-rust 0.9.0 treats the
field as required for all V3 metadata and fails deserialization before
NovaRocks can read the table.

This patch defaults a missing `next-row-id` to `INITIAL_ROW_ID` during V3
metadata deserialization. Serialization of NovaRocks-written V3 metadata still
emits the field, so writer behavior is unchanged. The compatibility path only
widens read tolerance for external V3 metadata.

Files: `src/spec/datatypes.rs`, `src/arrow/schema.rs`.

iceberg-rust 0.9.0 has no `PrimitiveType::Variant` arm, so any
`metadata.json` field with `"type": "variant"` fails to deserialize.
NovaRocks needs to read AND write Iceberg v3 tables that carry variant
columns. This patch adds:

* `PrimitiveType::Variant` on the `PrimitiveType` enum, going through
  the default lowercase rename so serde reads/writes `"variant"` as
  expected. The compatibility table never matches a literal — variant
  default values / partition / stats are all out-of-scope for now.
* `ToArrowSchemaConverter::primitive` returns
  `DataType::Struct{ metadata: Binary req, value: Binary req }` for
  `Variant`. Subfields deliberately carry no `PARQUET:field_id` —
  spec assigns one iceberg field id to the variant column itself.
* `ToArrowSchemaConverter::field` attaches
  `ARROW:extension:name = "arrow.parquet.variant"` (with empty
  `ARROW:extension:metadata`) when the underlying iceberg type is
  `Variant`. parquet-rs 58.2 reads these keys and emits
  `LogicalType::Variant` automatically when the consumer enables the
  `variant_experimental` feature.

When upstream iceberg-rust 0.10/0.11 ships native variant support,
this whole block becomes redundant; remove the enum arm, the primitive
arm, and the metadata-key attachments together.

Spec ref: <https://iceberg.apache.org/spec/#variant> and
parquet's `LogicalType::Variant` (parquet-rs source
`src/arrow/schema/extension.rs::logical_type_for_struct`).

## Patch 7 — bump arrow / parquet to 58.2

Files: `Cargo.toml` (vendor copy only; root is bumped in lock-step),
`src/transform/temporal.rs`.

iceberg-rust 0.9.0 originally pinned `arrow-* = "57.1"` and
`parquet = "57.1"`. NovaRocks needs parquet 58.x to reach the
`variant_experimental` feature (used by PATCH 6 to emit
`LogicalType::Variant`). The diff is mechanical — every `"57.1"` literal
in `[dependencies.arrow-*]` and `[dependencies.parquet]` becomes `"58.2"`.

arrow 58.0 deprecates `Date32Type::to_naive_date` in favour of
`to_naive_date_opt`. `src/transform/temporal.rs` calls the deprecated
form at three sites (the `Year` and `Month` transforms over `Date`
literals); each is rewritten to
`to_naive_date_opt(*v).expect("Date32Type::to_naive_date_opt overflow")`,
preserving the previous panic-on-overflow semantics while clearing the
`-D warnings` build.

A future upstream upgrade must compare its Arrow/Parquet vocabulary and the
remaining reader/format changes before retiring this dependency patch.

## Verification after rebase

Compare the restored transaction directory and MemoryCatalog source with the
published crate byte-for-byte. Compare all other vendor changes against this
remaining inventory; the public TableCommit builder and view/format patches must
not disappear with transaction restoration. Build the workspace and run the
connector commit tests, the upstream SDK tests and the REST catalog tests using
the root workspace dependency graph.

## View API surface (NovaRocks)

- `Catalog` trait: added default-erroring view methods (`create_view`,
  `load_view`, `update_view`, `drop_view`, `view_exists`, `list_views`).
- Added `ViewRequirement` (`assert-view-uuid`) and `ViewCommit` (public
  builder, mirrors `TableCommit`).
- `ViewCreation.location` is now `Option<String>` so REST servers can
  assign the location; `ViewMetadataBuilder::from_view_creation` errors
  on `None`.
- `ViewRepresentations::new` is public so downstream crates can build
  representation lists.
