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

## Patch 3 — `src/arrow/record_batch_transformer.rs` (`_pos` virtual column)

iceberg-rust 0.9 declares the `_file` and `_pos` reserved metadata columns in
[`src/metadata_columns.rs`](src/metadata_columns.rs), and `TableScanBuilder`
accepts both in `select(...)`. Only `_file` is wired up in
[`src/arrow/reader.rs:422-427`](src/arrow/reader.rs:422); projecting `_pos`
reaches the `RecordBatchTransformer` with `RESERVED_FIELD_ID_POS` in
`projected_iceberg_field_ids` but no entry in `constant_fields` (it can't be
a constant — `_pos` is per-row), and the transformer falls through to the
"regular field" branch which can't find the field id in the data file's
schema and errors with `Unexpected => field not found`.

This patch teaches `RecordBatchTransformer` to inject `_pos` as a per-row
`Int64Array` whose values are the running offset within the data file.

Concretely:

* New `ColumnSource::RowIndex` variant — sibling to `PassThrough`, `Promote`,
  and `Add`. Produces `[row_offset, row_offset+1, …, row_offset+N-1]` per
  batch.
* New `RecordBatchTransformer.row_offset: u64` field. Initialized to 0 in
  `RecordBatchTransformerBuilder::build`. Advances by `record_batch.num_rows()`
  every successful `Modify` pass through `process_record_batch`. Per-file
  scope is correct because the reader builds one transformer per
  `FileScanTask`.
* Schema-side branch in `generate_batch_transform`: when `field_id ==
  RESERVED_FIELD_ID_POS`, emit `Field::new("_pos", DataType::Int64, …)` with
  the field-id metadata. (Without this branch the transformer would still
  fall through to "field not found".)
* Operations-side branch in `generate_transform_operations`: when
  `field_id == RESERVED_FIELD_ID_POS`, emit `ColumnSource::RowIndex`.
* `transform_columns` is renamed to `transform_columns_with_offset` and
  takes the offset explicitly so the new RowIndex case can compose values
  off of it.

Net change: ~60 lines in one file. No public API renames; `ColumnSource` and
`RecordBatchTransformer` are both `pub(crate)` so the new variant / field is
not visible to downstream callers (they just see "_pos works now").

Spec ref: <https://iceberg.apache.org/spec/#reserved-field-ids> — `_pos` =
2147483645 = `i32::MAX - 2`.

## Verification after rebase

When bumping the vendored copy to a newer iceberg-rust patch release:

1. `diff -ru` against the new upstream source to confirm only those two lines
   diverge (plus this `PATCH.md` and the inline `// NovaRocks patch:` comments).
2. `cargo build -p novarocks` from the worktree root.
3. `cargo test -p novarocks --lib commit:: -- --nocapture` should still pass.

If upstream changes the surrounding code substantially, re-apply by hand and
update this file.
