# NovaRocks patches over upstream iceberg-rust 0.9.0

Upstream source: https://crates.io/crates/iceberg/0.9.0

These patches are the minimum required to let NovaRocks use an eagerly staged,
single-dispatch transaction and implement custom Transaction actions for INSERT
OVERWRITE and DELETE flows that iceberg-rust 0.9 does not yet ship as built-in
actions (`overwrite_files`, `row_delta`).

When upstream lands native equivalents — likely in 0.10/0.11 — this whole
vendor directory and the corresponding `[patch.crates-io]` block in the root
`Cargo.toml` should be deleted, and the NovaRocks `OverwriteCommit` and
`RowDeltaCommit` impls (`src/connector/iceberg/commit/{overwrite,row_delta}.rs`)
should be re-pointed at the upstream actions.

Tracked under spec §0.4 / Plan Task 9.

## Patch 9 — eager transaction staging and side-effect-free commit export

Upstream 0.9 stores actions in `Transaction` and evaluates them only from
`commit()`. That method reloads the table and transparently replays the whole
action list on retryable errors. A later action therefore cannot read metadata
created by an earlier action before catalog publication, and a caller cannot
give one already-frozen commit to an external publication owner.

This patch changes the generic transaction model rather than adding a
NovaRocks-specific shadow transaction:

* `ApplyTransactionAction::apply` is asynchronous and evaluates the action
  immediately against the transaction-local staged table.
* Each action's updates are applied to that staged table before the next action
  runs. Action requirements are checked against that stage-local table, then
  normalized to the equivalent OCC requirement against the transaction's
  original base before publication. Exact duplicates are collapsed, so two
  snapshot actions do not export mutually exclusive `assert-ref-snapshot-id`
  values.
* `Transaction::staged_table` and `Transaction::staged_snapshot` expose the
  read-only local result needed to construct a later action.
* `Transaction::into_table_commit` consumes the transaction and exports the
  complete `TableCommit` without catalog I/O, refresh, dispatch, or retry.
* The convenience `Transaction::commit` performs at most one
  `Catalog::update_table` call. Retryable errors are returned to the caller and
  never replay actions inside the library.
* The now-unused `backon` dependency is removed.

Library tests lock down append-then-statistics visibility, zero-effect export,
and one dispatch for retryable and non-retryable catalog errors.

## Patch 8 — authoritative REST staged-create initialization updates

`TableMetadata::staged_create_initialization_updates` converts the metadata
returned by a REST `stage-create=true` response into the initialization
updates for the first `assert-create` commit. Values come exclusively from
the authoritative response metadata, including UUID, location, schema/spec/
sort-order IDs, high-watermarks, format version, and properties. The helper
rejects non-initial metadata that the initialization update set cannot
represent exactly instead of guessing or silently dropping state.

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

When upstream iceberg-rust 0.10 lands with its own arrow/parquet bump,
this entry is removed by the same path that already retires PATCH 1–5.

## Verification after rebase

When bumping the vendored copy to a newer iceberg-rust patch release:

1. `diff -ru` against the new upstream source to confirm only those two lines
   diverge (plus this `PATCH.md` and the inline `// NovaRocks patch:` comments).
2. `cargo build -p novarocks` from the worktree root.
3. `cargo test -p novarocks --lib commit:: -- --nocapture` should still pass.

If upstream changes the surrounding code substantially, re-apply by hand and
update this file.

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
