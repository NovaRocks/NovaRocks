# Iceberg v3 Variant Write Path — Design

Status: draft
Date: 2026-05-06
Scope: NovaRocks BE — Iceberg connector
Companion plan: to be authored via the `writing-plans` skill

## 0. Goal & Non-Goals

### 0.1 Goal

Enable `INSERT INTO ... SELECT` / `INSERT INTO ... VALUES` to successfully
write Iceberg v3 tables that contain variant value columns, producing
parquet files whose physical layout matches the
[Iceberg v3 spec for variant](https://iceberg.apache.org/spec/#variant) —
i.e. a parquet `group` annotated with `LogicalType::Variant`, containing two
required binary leaves named `metadata` and `value`.

### 0.2 Non-Goals (explicit fail-fast in this PR)

- INSERT OVERWRITE writing variant tables.
- DELETE / UPDATE / MERGE / sql-delete writing variant tables (row-lineage v3
  or legacy position-delete).
- Variant *shredding* (the optional `typed_value` subtree).
- Variant default values (`initial-default` / `write-default`).
- Variant in partition spec, sort order, or equality delete columns — these
  are *spec-prohibited*, not deferred; they always fail-fast.

Each non-goal must surface a distinct, actionable error message — not a
catch-all "writer cannot encode variant" rejection.

## 1. Architecture

```
INSERT INTO t SELECT ...   (variant column is LargeBinary in NovaRocks chunk)
        |
        v
sink operator → connector::iceberg::sink::run_insert
        |
        v
[validation]        ensure_iceberg_write_supported(table)
        |             - reject variant in partition spec / sort order / equality ids
        |             - allow variant *value* columns
        |           match_select_schema_to_table:
        |             - variant target column accepts LargeBinary source
        v
[data_writer.rs]    write_record_batches_as_data_files
        |
        |  ── new step ── transform_variant_columns_for_write(batch, schema, indices)
        |     LargeBinary[v] (size+meta+val format)
        |        → StructArray { metadata: BinaryArray (req), value: BinaryArray (req) }
        |     Arrow field has VariantType extension metadata
        |     (set by PATCH 6 schema_to_arrow_schema)
        v
iceberg::ParquetWriter   parquet group with LogicalType::Variant + 2 binary leaves
                         (parquet-rs feature `variant_experimental`)
        |
        v
DataFile → manifest commit (existing path)
```

Stats handling: no special code is needed. The iceberg `IndexByParquetPathName`
visitor walks the iceberg schema; with PATCH 6, `Variant` is a leaf
`PrimitiveType` and the visitor stops there, so no `v.metadata` / `v.value`
parquet path is registered. `parquet_to_data_file_builder` calls
`index_by_parquet_path.get(...)` for those subfield paths, gets `None`, and
`continue`s. The top-level `v` is a parquet group (no column chunk), so it
contributes nothing either. Result: variant columns naturally have no
`lower_bounds` / `upper_bounds` / `value_counts` / `null_value_counts` /
`column_sizes` entries — exactly what the spec requires.

## 2. Vendor Patches

Two new entries in `vendor/iceberg-0.9.0/PATCH.md`.

### 2.1 PATCH 6 — `PrimitiveType::Variant`

Files: `vendor/iceberg-0.9.0/src/spec/datatypes.rs`,
`vendor/iceberg-0.9.0/src/arrow/schema.rs`.

`datatypes.rs`:

```rust
pub enum PrimitiveType {
    Boolean, Int, Long, Float, Double,
    Decimal { precision: u32, scale: u32 },
    Date, Time, Timestamp, Timestamptz, TimestampNs, TimestamptzNs,
    String, Uuid, Fixed(u64), Binary,
    Variant,
}
```

- Serde rename `"variant"` ↔ `Variant`. Goes through the same simple-string
  rename path as `Boolean` / `String`; it must *not* enter the custom
  `decimal` / `fixed` deserializer branches.
- `compatible(&self, &PrimitiveLiteral) -> bool`: returns `false` for the
  `Variant` arm (no v3 variant literal exists in iceberg-rust 0.9; default
  values are out of scope here).
- Do not extend `PrimitiveLiteral`, `Datum`, or `Literal`. Variant
  intentionally does not enter default-value, partition, or stats paths.

`arrow/schema.rs::type_to_arrow_type`:

```rust
PrimitiveType::Variant => DataType::Struct(Fields::from(vec![
    Field::new("metadata", DataType::Binary, /*nullable=*/ false),
    Field::new("value",    DataType::Binary, /*nullable=*/ false),
])),
```

`arrow/schema.rs::schema_to_arrow_schema` — the loop that builds top-level
`arrow::Field`s and attaches `PARQUET:field_id` metadata gains an extra step:
when the underlying iceberg type is `PrimitiveType::Variant`, also attach

```
ARROW:extension:name      = "arrow.parquet.variant"
ARROW:extension:metadata  = ""
```

This is the exact convention parquet-rs 58.x uses (see
`parquet/src/arrow/schema/extension.rs::logical_type_for_struct`); with
`variant_experimental` enabled, the parquet writer emits
`LogicalType::Variant` automatically, with no `WriterProperties` knob to
set.

The variant subfields (`metadata`, `value`) deliberately do **not** carry
`PARQUET:field_id` metadata. Spec only assigns one iceberg field id to the
variant column; the subfield names are the spec's positional contract.

### 2.2 PATCH 7 — bump arrow / parquet to 58.2

Files: `vendor/iceberg-0.9.0/Cargo.toml`.

Mechanical replace of `arrow-* = "57.1"` and `parquet = "57.1"` to `"58.2"`.
No semantic change is expected; verify by `cargo build -p iceberg` after
the bump.

### 2.3 Top-level Cargo.toml

```diff
- arrow = { version = "57.1.0", features = ["prettyprint", "ipc", "ipc_compression"] }
- parquet = { version = "57.1.0", features = ["arrow"] }
+ arrow = { version = "58.2.0", features = ["prettyprint", "ipc", "ipc_compression"] }
+ parquet = { version = "58.2.0", features = ["arrow", "variant_experimental"] }
```

`arrow-buffer` / `arrow-data` follow to `58.2.0`. No new top-level
dependency on `parquet-variant-compute` — the extension type is enabled via
metadata key strings, not via that crate's helper struct.

## 3. Write-side transform

New module: `src/connector/iceberg/variant_write.rs`.

```rust
/// Top-level arrow indices that correspond to PrimitiveType::Variant
/// fields in the iceberg current schema.
pub(crate) fn variant_field_indices(
    iceberg_schema: &iceberg::spec::SchemaRef,
) -> Vec<usize>;

/// Replace LargeBinary columns at the given indices with
/// StructArray { metadata: BinaryArray (req), value: BinaryArray (req) }.
/// Each input row is the size-prefixed VariantValue::serialize byte string
/// `[size:u32 LE | metadata bytes | value bytes]`. The function reuses
/// underlying buffers; per row it parses the metadata header to find the
/// metadata-bytes length, then derives `value` as the remainder.
///
/// Null rows: the parent StructArray null bit is set. Required-leaf
/// semantics are satisfied because parquet's definition-level encoding
/// emits the null at the parent group; the child BYTE_ARRAY values are
/// never read on null parent rows. The child BinaryArrays therefore use
/// zero-length placeholders at those positions (NOT marked null at the
/// child level, which would conflict with `nullable: false`).
pub(crate) fn transform_variant_columns_for_write(
    batch: &RecordBatch,
    annotated_schema: &arrow::datatypes::SchemaRef,
    variant_indices: &[usize],
) -> Result<RecordBatch, String>;
```

Metadata-length parsing reuses `src/exec/variant.rs::load_metadata` /
`validate_metadata`; we add a private helper `metadata_byte_len(payload)`
in `variant_write.rs` that returns the metadata segment length without
re-validating the value.

`data_writer.rs::write_record_batches_as_data_files_with_writer` change:

```rust
let variant_indices = variant_field_indices(metadata.current_schema());
// inside the per-batch loop:
let staged = if variant_indices.is_empty() {
    batch
} else {
    transform_variant_columns_for_write(&batch, &annotated_schema, &variant_indices)?
};
let annotated = annotate_batch(&staged, &annotated_schema)?;
```

`annotated_schema` is already produced by `schema_to_arrow_schema` and, after
PATCH 6, carries the right Struct + extension metadata at variant positions.
`annotate_batch` (a `RecordBatch::try_new` rebuild against
`annotated_schema`) succeeds because the column type already matches.

`build_data_file_writer_with_schema` is unchanged.

## 4. Validation changes (`commit/validation.rs`)

- Delete `ensure_no_variant_columns` and the name-based proxy
  `type_contains_variant`. PATCH 6 makes them obsolete.
- New helpers, both called by `ensure_iceberg_write_supported(table)`:
  - `ensure_no_variant_in_partition_spec(table)` — walks
    `metadata.default_partition_spec().fields()` and rejects any field
    whose `source_id` resolves to a variant column.
  - `ensure_no_variant_in_sort_order(table)` — walks
    `metadata.default_sort_order().fields()` and rejects likewise.

Equality-delete columns are NOT a table-level property — `equality_ids`
lives on individual DataFiles, not on `TableMetadata`. The spec's
"variant cannot be an equality-delete identifier" rule is therefore
enforced at the ADD EQUALITY DELETE write path (see § 6), not in INSERT
validation.

Each helper traverses the iceberg schema, finds the offending column name(s),
and returns a single string error of the shape:

> `"iceberg table column '<name>' is variant; variant columns cannot appear in <where>. <how to fix>."`

`ensure_iceberg_write_supported(table)` calls the three helpers in order and
short-circuits on the first error.

## 5. Schema match (`commit/validation.rs::match_select_schema_to_table`)

Add a special arm in `arrow_iceberg_types_compatible`:

```rust
if matches!(iceberg_ty, iceberg::spec::Type::Primitive(PrimitiveType::Variant)) {
    return matches!(arrow_ty, DataType::LargeBinary);
}
```

NovaRocks' execution layer collapses both `BINARY` and `VARIANT` into Arrow
`LargeBinary` (`src/lower/type_lowering.rs:89,170`). At the
`match_select_schema_to_table` boundary we cannot distinguish the two from
Arrow alone. The interaction risk is `INSERT INTO t (v_variant) SELECT
binary_col`.

**Default behavior (this PR)**: accept LargeBinary compatibility unconditionally.
A user passing a non-variant `LargeBinary` (e.g. `INSERT INTO t (v_variant)
SELECT binary_col`) produces an iceberg row whose variant decoder treats
unparseable metadata as a null variant on read
(`src/formats/parquet/mod.rs:1907-1911` already has this fallback). No file
corruption; semantic-error only.

**Optional follow-up (plan-stage spike, may be deferred)**: inspect whether
the SELECT subtree's FE-provided `slot_types` are reachable at the
`run_insert` entry. If reachable, add a strict check that variant target
columns require `slot_types[i] == TPrimitiveType::VARIANT`. If not
reachable cheaply, drop the spike — the read-side null fallback bounds
the worst-case behavior.

## 6. Other write paths — fail-fast guards

`ensure_iceberg_write_supported` now allows variant value columns. To keep
non-INSERT paths off, add a single helper:

```rust
pub fn ensure_no_variant_columns_for_row_level_mutation(table: &Table) -> Result<(), String>;
```

Called at (verified file names):

- INSERT OVERWRITE — `src/engine/iceberg_writer.rs` (the OVERWRITE branch
  that imports `commit/overwrite.rs`).
- DELETE — `src/engine/delete_flow.rs`.
- UPDATE / MERGE INTO — `src/engine/mutation_flow.rs` (handles both UPDATE
  and `MERGE INTO`; comment at line 1182 anchors the MERGE block).
- ADD EQUALITY DELETE — `src/engine/equality_delete_flow.rs` (this is also
  where the spec's "variant cannot be an equality-delete identifier" rule
  is enforced — reject when the user-supplied equality column list names a
  variant column).

The error message names the path (e.g. "DELETE on iceberg tables with
variant columns is not supported in this release; INSERT is supported.")
so users can tell why an otherwise valid command was rejected.

## 7. Testing

| Layer | Tests |
|---|---|
| `variant_write::metadata_byte_len` and `transform_variant_columns_for_write` | one row / many rows / null row / two adjacent variant columns / non-variant columns passing through |
| PATCH 6 `PrimitiveType::Variant` | serde round-trip on a metadata.json snippet; `type_to_arrow_type(Variant)` returns the expected Struct shape; `schema_to_arrow_schema` attaches the extension-name metadata |
| `validation` — new helpers | partition spec / sort order / equality ids each containing a variant column → actionable error |
| `data_writer` integration | build an in-memory iceberg table with one `(id Int, v Variant)` schema, drive `write_record_batches_as_data_files` end-to-end, assert the parquet file's physical schema (`group v (variant) { metadata: BINARY req; value: BINARY req }`) and that NovaRocks' `convert_variant_columns` round-trips the same payload back |
| sql-tests positive | new suite case: `CREATE TABLE ... (id INT, v VARIANT) USING iceberg`; `INSERT INTO ... VALUES (parse_json('{...}'))`; `SELECT v FROM t` round-trips; multi-row, null, nested object/array |
| sql-tests negative | partition by variant / sort by variant / equality delete by variant / DELETE / UPDATE / MERGE / INSERT OVERWRITE on a variant table — each yields the matching error message |

CI bar: `cargo fmt`, `cargo clippy`, `cargo build`, `cargo test`, and the
sql-tests suite that includes the cases above run in `--mode verify`.

## 8. Compatibility notes

- The `parquet/iceberg metadata.json` produced by NovaRocks for a
  variant-bearing table is unchanged — variant typing already lives in the
  table's schema, NovaRocks does not author DDL here.
- Files written by this path are spec-compliant: `LogicalType::Variant`
  parent group, two required binary leaves, no stats keyed by variant
  field id. Spark / Trino / iceberg-java readers should read them
  unmodified.
- NovaRocks' own read path (`convert_variant_columns`) requires no change;
  it already handles the parquet `Struct{metadata, value}` form and
  collapses it back to internal `LargeBinary`.
- vendor PATCH 6 / 7 add to a 5-patch list. When upstream iceberg-rust
  ships native `Variant` (likely 0.10 / 0.11), the same removal path that
  PATCH.md already prescribes applies — delete vendor, repoint deps,
  remove the two `metadata` keys we attach.
