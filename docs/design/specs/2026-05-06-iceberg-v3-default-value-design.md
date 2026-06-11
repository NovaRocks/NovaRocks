# Iceberg v3 Default Value Support — Design

Status: Draft
Date: 2026-05-06
Scope: Iceberg v3 `initial-default` (read) + `write-default` (write) semantics across DDL,
schema transport, parquet read path, and INSERT write path.

## 0. Goals and Non-Goals

### 0.1 Goals

1. Honor `initial-default` when reading v3 Iceberg tables — fill columns missing from
   data files (because the column was added after the file was written) with the
   schema's default literal instead of NULL.
2. Honor `write-default` when executing `INSERT` against v3 Iceberg tables — when an
   explicit column list omits a column whose schema specifies a write-default,
   physically materialize the default literal into the new file.
3. Accept `DEFAULT <literal>` clauses in `CREATE TABLE` and `ALTER TABLE ADD COLUMN`,
   write them into Iceberg `NestedField.initial_default` and `NestedField.write_default`
   in metadata.json.
4. Reject non-NULL DEFAULT on v1/v2 tables with an explicit, actionable error.

### 0.2 Non-Goals

- `ALTER COLUMN ... SET DEFAULT` / `DROP DEFAULT` (post-creation write-default mutation).
  At creation time `initial_default == write_default`; later divergence is out of scope.
- DEFAULT for complex types (struct / list / map). Rejected at parse time.
- DEFAULT for `LargeInt`, `Time`, `UUID`, `Fixed`. Rejected at parse time.
- INSERT positional count auto-fill. `INSERT INTO t VALUES (...)` with fewer values
  than columns continues to error; only the explicit-column-list form triggers
  write-default.
- StarRocks FE coordination. The thrift extension is forward-compatible: FE may
  populate the new optional fields whenever it adds support; until then FE-mode
  reads degrade gracefully to NULL fill.

## 1. Background

The Iceberg v3 spec adds two optional fields to every schema field:

- `initial-default` — used to populate the column for rows in files written **before**
  the column existed. Frozen at column-add time; never re-interpreted.
- `write-default` — used to populate the column when a writer issues an `INSERT`
  that does not supply the column's value.

NovaRocks today:

- Vendored `iceberg-0.9.0` already round-trips both fields through `NestedField`
  (`vendor/iceberg-0.9.0/src/spec/datatypes.rs`).
- DDL parser accepts only `DEFAULT NULL`
  (`src/engine/statement.rs:1135 parse_add_column_change`).
  Non-NULL defaults are explicitly rejected.
- DDL writer constructs `NestedField::optional(...)` with no defaults
  (`src/connector/iceberg/catalog/schema_update.rs:435`).
- Parquet reader fills missing columns with `new_null_array`
  (`src/formats/parquet/mod.rs:1654` in `align_batch_to_iceberg_schema`).
- Thrift descriptor `TIcebergSchemaField` carries only `field_id`, `name`, `children`
  (`idl/thrift/Descriptors.thrift:557`); defaults are stripped on the way to the BE.
- INSERT VALUES path fills omitted columns with `Literal::Null`
  (`src/engine/insert.rs:79 reorder_insert_row`).

## 2. Decisions (Locked)

| # | Decision |
|---|---|
| D1 | **Sub-features in scope**: A (read initial-default backfill) + B (write write-default fill on INSERT) + C (DDL CREATE/ADD COLUMN with DEFAULT) + D (v3 format-version gate). |
| D2 | **Type coverage**: Boolean, TinyInt, SmallInt, Int, BigInt, Float, Double, Decimal(p,s), String, Date, DateTime, Binary. Other primitives and all complex types rejected at parse time. |
| D3 | **DDL surface**: only `CREATE TABLE` and `ALTER TABLE ADD COLUMN` with `DEFAULT`. `ALTER COLUMN SET/DROP DEFAULT` is out of scope. At creation, `initial_default == write_default`. |
| D4 | **write-default trigger**: only the explicit-column-list form (`INSERT INTO t (a, b) VALUES (...)` / `INSERT INTO t (a, b) SELECT ...`). Positional INSERT continues to require exact column count. |
| D5 | **v2 policy**: hard-reject non-NULL DEFAULT with `non-NULL DEFAULT requires Iceberg format-version 3; set TBLPROPERTIES('format-version'='3')`. `DEFAULT NULL` continues to work on v2 (does not write `initial_default` into metadata). |
| D6 | **FE-mode policy**: extend `TIcebergSchemaField` thrift with one optional field (`initial_default_json`). Standalone-mode populates it via codegen and benefits immediately. FE-mode falls back to NULL fill until StarRocks FE populates it — no regression. `write_default` does not flow through thrift; it stays on `ColumnDef` for engine-side INSERT. |
| D7 | **Read-side fail-fast**: when a default literal cannot be decoded (corrupted JSON, unsupported type), the scan errors instead of silently degrading to NULL. |

## 3. Architecture and Data Flow

```
┌─ SQL Parser ─────────────────────────────────────────────────────────────────┐
│ CREATE TABLE / ALTER ADD COLUMN parses DEFAULT literal into DefaultLiteral   │
│ - TableColumnDef            { ..., default: Option<DefaultLiteral> }         │
│ - IcebergSchemaChange::AddColumn { default: Option<DefaultLiteral> }         │
│ - DEFAULT NULL  →  default = Some(DefaultLiteral::Null) (does not persist)    │
└──────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─ DDL Lower / Catalog Write ──────────────────────────────────────────────────┐
│ DefaultLiteral → iceberg::spec::Literal                                      │
│ NestedField::optional(...).with_initial_default(lit).with_write_default(lit) │
│ v2 table + non-NULL DEFAULT → fail fast                                      │
└──────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─ Schema Transport ───────────────────────────────────────────────────────────┐
│ iceberg::spec::Schema → IcebergSchemaFieldDef → TIcebergSchemaField          │
│ catalog model carries initial_default + write_default;                       │
│ thrift carries only initial_default_json (write_default stays engine-side)   │
└──────────────────────────────────────────────────────────────────────────────┘
                                    ↓                                ↓
┌─ Read Path (A) ─────────────────────┐  ┌─ Write Path (B) ────────────────────┐
│ Arrow Field metadata carries default │  │ INSERT explicit-list omitted column │
│ JSON. ParquetScanConfig stays as is. │  │ uses write_default literal instead  │
│ align_batch_to_iceberg_schema fills │  │ of NULL.                            │
│ missing columns with default literal │  │ - VALUES: reorder_insert_row        │
│ via build_iceberg_default_array.     │  │ - FROM QUERY: align_chunk_to_target │
└──────────────────────────────────────┘  └─────────────────────────────────────┘
```

### 3.1 Invariants

1. At column creation `initial_default == write_default` (we never split them).
2. v2 tables never carry non-NULL `initial_default` / `write_default` in metadata.
   `DEFAULT NULL` on v2 controls only column nullability and does not write a
   default literal.
3. Read-side never silently degrades unknown defaults to NULL — corrupted or
   unsupported defaults raise an explicit scan error.

## 4. DDL: Grammar and AST

### 4.1 Grammar

```
column_def        := identifier  type  [NOT NULL | NULL]  [DEFAULT default_value]
                                                          [aggregation]
add_column_change := ADD COLUMN identifier type [DEFAULT (default_value | NULL)]
default_value     := boolean_literal
                   | numeric_literal
                   | string_literal              // for STRING / DATE / DATETIME
                   | hex_byte_literal            // x'DEADBEEF' for BINARY
```

DEFAULT accepts only single-value literals. Functions (`now()`, `current_timestamp`),
arithmetic, and other expressions are rejected.

### 4.2 AST Changes

```rust
// src/sql/parser/ast/mod.rs
pub(crate) struct TableColumnDef {
    pub name: String,
    pub data_type: SqlType,
    pub nullable: bool,
    pub aggregation: Option<ColumnAggregation>,
    pub default: Option<DefaultLiteral>,           // new
}

pub(crate) enum DefaultLiteral {
    Null,                                          // DEFAULT NULL — not persisted
    Bool(bool),
    Int(i64),                                      // tinyint/smallint/int/bigint
    Float(f64),                                    // float/double
    Decimal { unscaled: i128, scale: i8 },
    String(String),
    Date(i32),                                     // days since epoch
    DateTime(i64),                                 // micros since epoch
    Binary(Vec<u8>),
}

// src/engine/statement.rs
pub(crate) enum IcebergSchemaChange {
    AddColumn {
        name: String,
        data_type: SqlType,
        default: Option<DefaultLiteral>,           // replaces default_null: bool
    },
    // RenameColumn / DropColumn / ModifyColumn unchanged
}
```

`DEFAULT NULL` collapses into `Some(DefaultLiteral::Null)`; lack of `DEFAULT` clause
becomes `None`. Both result in NestedField with no `initial_default` set, but the
parser still uses the difference to detect duplicate `DEFAULT` clauses.

### 4.3 SqlType → DefaultLiteral → iceberg::spec::Literal

| SqlType | Accepted literal forms | iceberg PrimitiveLiteral |
|---|---|---|
| Boolean | `TRUE` / `FALSE` | `Boolean` |
| TinyInt / SmallInt / Int | integer (range-checked) | `Int(i32)` |
| BigInt | integer | `Long(i64)` |
| Float | numeric | `Float(f32)` |
| Double | numeric | `Double(f64)` |
| Decimal(p,s) | numeric (scale must equal column scale) | `Decimal { unscaled, scale }` |
| String | string | `String` |
| Date | `'YYYY-MM-DD'` | `Date(days)` |
| DateTime | `'YYYY-MM-DD HH:MM:SS[.ffffff]'` | `Timestamp(micros)` |
| Binary | `x'DEADBEEF'` | `Binary(bytes)` |
| LargeInt / Time / Array / Map / Struct | — | parse error |

### 4.4 Validation Order

1. **Parse**: capture `DEFAULT literal`, default to `None` on absence.
2. **Type check**: literal must lower cleanly to the column's `SqlType` (e.g.
   `INT DEFAULT 'abc'` fails; `DECIMAL(10,2) DEFAULT 1.234` fails on scale).
3. **format-version gate**: if `default` is present and non-NULL:
   - `CREATE TABLE`: read `format-version` from `WITH (...)` properties; missing
     defaults to v2; v1/v2 + non-NULL DEFAULT errors.
   - `ALTER TABLE ADD COLUMN`: read `format-version` from current table metadata;
     v1/v2 errors.
   - Error template: `non-NULL DEFAULT requires Iceberg format-version 3; set TBLPROPERTIES('format-version'='3')`.
4. **Persist**: pass through to `build_updated_schema` /
   `connector::iceberg::catalog::registry::create_table`, which call
   `NestedField::optional(...).with_initial_default(lit).with_write_default(lit)`.

## 5. Schema Transport

### 5.1 IcebergSchemaFieldDef

```rust
// src/sql/catalog.rs
pub struct IcebergSchemaFieldDef {
    pub field_id: i32,
    pub name: String,
    pub initial_default: Option<iceberg::spec::Literal>,   // new
    pub write_default: Option<iceberg::spec::Literal>,     // new
    pub children: Vec<IcebergSchemaFieldDef>,
}
```

Populated in `connector::iceberg::catalog::backend.rs::iceberg_field_def` by
copying `NestedField.initial_default` and `NestedField.write_default`.

### 5.2 ColumnDef

```rust
// src/sql/catalog.rs
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub write_default: Option<iceberg::spec::Literal>,     // new
}
```

Used by the INSERT write path. Non-Iceberg backends leave `write_default = None`.

### 5.3 Thrift Extension

```thrift
// idl/thrift/Descriptors.thrift
struct TIcebergSchemaField {
    1: optional i32 field_id
    2: optional string name
    3: optional string initial_default_json    // new — Literal serialized as spec JSON
    100: optional list<TIcebergSchemaField> children
}
```

Only `initial_default_json` is added. The read path is the only thrift consumer
of defaults; INSERT happens entirely on the engine side using
`ColumnDef.write_default`, which is populated locally from the iceberg-rust
schema and does not flow through thrift. Adding `write_default_json` would be
unused in this scope.

The new field is forward-compatible with existing FE serializers. Standalone
codegen (`src/sql/codegen/descriptors.rs::to_thrift_iceberg_schema_field`)
populates it by serializing the iceberg `Literal` via the spec's JSON form.
FE-mode emitters that do not yet populate the field produce thrift that
deserializes to `None` → graceful fallback to NULL fill.

### 5.4 Arrow Field Metadata

`apply_field_id_recursive` (`src/connector/iceberg/schema.rs:121`) currently writes
`PARQUET_FIELD_ID_META_KEY`. Extend it to also write:

- `novarocks.iceberg.initial_default` → JSON string of the Literal, when present.

The key is scoped to NovaRocks and defined as a constant in
`src/connector/iceberg/schema.rs`. Only initial-default is carried here because
the parquet reader is the only Field-metadata consumer of defaults.

## 6. Read Path (A): initial-default Backfill

### 6.1 Entry Point

`align_batch_to_iceberg_schema` (`src/formats/parquet/mod.rs:1620`). Current code
fills missing columns with `new_null_array(target.data_type(), row_count)`
(line 1654). Replace with a helper that consults the field's metadata:

```rust
fn build_iceberg_default_array(
    target_field: &Field,
    row_count: usize,
) -> Result<ArrayRef, String>;
```

Behavior:

1. Read `novarocks.iceberg.initial_default` from `target_field.metadata()`.
2. Absent → `new_null_array(target.data_type(), row_count)` (preserves today's
   behavior).
3. Present → parse JSON via `iceberg::spec::Literal::try_from_json`; convert via
   `literal_to_constant_array(literal, target.data_type(), row_count)`.
4. JSON parse failure → `corrupted initial-default JSON for column <name>: <err>`.
5. Type unsupported (uuid / fixed / nested) → `unsupported initial-default literal for column <name>: <type>`.

### 6.2 Constant Array Builder

`literal_to_constant_array(lit, target_type, row_count) -> Result<ArrayRef, String>`
implements the inverse of §4.3. Each supported primitive maps to the corresponding
Arrow `*Array::from_iter_values(vec![v; row_count])`. The function lives next to
`align_batch_to_iceberg_schema` in `src/formats/parquet/mod.rs`.

### 6.3 Nested Struct Children

`align_iceberg_array_to_field` (`src/formats/parquet/mod.rs:1548`) hits the same
`new_null_array` line at 1583 when a struct child is missing. Replace identically
with `build_iceberg_default_array`. In-scope types do not include nested defaults,
so this is defensive only — but it routes through the fail-fast unsupported-type
branch correctly when an external writer wrote a nested default.

### 6.4 No New Switch

`has_iceberg_schema_evolution` (`src/formats/parquet/mod.rs:354`) is the existing
gate (`iceberg_output_schema.is_some()`). Default backfill operates only inside this
path. No new config flag is introduced; absence of `iceberg_output_schema` means
there is no field-id-based schema evolution and "missing column" is undefined.

## 7. Write Path (B): write-default Materialization

### 7.1 VALUES Path

Modify `reorder_insert_row` (`src/engine/insert.rs:59`) so it knows the target
column index when filling a missing slot:

```rust
None => {
    let column = &target_columns[target_idx];
    match &column.write_default {
        Some(lit) => reordered.push(literal_from_iceberg(lit, &column.data_type)?),
        None => reordered.push(Literal::Null),
    }
}
```

This requires changing the iteration to expose `target_idx`. The simplest shape
is to enumerate `mapping`:

```rust
for (target_idx, source_idx) in mapping.iter().enumerate() { ... }
```

`literal_from_iceberg(lit, dt) -> Result<ast::Literal, String>` is the inverse of
the DDL-side `DefaultLiteral → iceberg::spec::Literal` mapping; it lives next to
`reorder_insert_row` in `src/engine/insert.rs`.

### 7.2 FROM QUERY Path

`execute_iceberg_insert_or_overwrite` (`src/engine/iceberg_writer.rs:51`) currently
ignores `_insert_columns`. Use it:

1. If `insert_columns` is non-empty:
   - SELECT must produce exactly `insert_columns.len()` columns
   - After `run_select_to_chunks`, run a new helper
     `align_chunk_to_target_schema(chunks, insert_columns, &target_columns)` that
     remaps each chunk into the target schema's column order, filling
     unmentioned columns with `write_default` (or NULL) constant arrays of the
     batch's row count.
2. If `insert_columns` is empty: behavior unchanged (positional, count must match).

`align_chunk_to_target_schema` reuses `literal_to_constant_array` from §6.2.

### 7.3 Validation Time

INSERT-time validation (NOT NULL violations, type mismatches) is unchanged. The
only new code path is "omitted column with write_default → fill literal" instead
of "omitted column → fill NULL".

## 8. Examples

`CREATE TABLE t (a INT, b INT DEFAULT 5) WITH ('format-version'='3')`:

| Statement | Stored | SELECT result |
|---|---|---|
| `INSERT INTO t (a) VALUES (1)` | parquet has `(1, 5)` | `(1, 5)` |
| `INSERT INTO t VALUES (1, 2)` | parquet has `(1, 2)` | `(1, 2)` |
| `INSERT INTO t VALUES (1)` | error: column count mismatch | — |

`ALTER TABLE t ADD COLUMN c INT DEFAULT 7` after table has rows from prior file F1
(which lacks column c):

| Source | b | c |
|---|---|---|
| F1 (pre-add file) | file value | 7 (initial-default backfill) |
| `INSERT INTO t (a,b) VALUES (1,2)` post-ADD | 2 | 7 (write-default materialized) |
| `INSERT INTO t (a,b,c) VALUES (1,2,9)` post-ADD | 2 | 9 (user-supplied) |

## 9. Error Conditions

| Scenario | Stage | Error |
|---|---|---|
| `INT DEFAULT 'abc'` | Parse / type check | `DEFAULT value type does not match column type` |
| `DECIMAL(10,2) DEFAULT 1.234` | Parse / type check | `DEFAULT value scale 3 does not match column scale 2` |
| Non-NULL DEFAULT on v1/v2 table | DDL pre-commit | `non-NULL DEFAULT requires Iceberg format-version 3; ...` |
| Duplicate DEFAULT clause | Parse | `duplicate DEFAULT clause` |
| DEFAULT on UUID / Time / Array / Map / Struct | Parse | `DEFAULT value not supported for type <T>` |
| Scan reads metadata with corrupted default JSON | Scan setup | `corrupted initial-default JSON for column <name>: <err>` |
| Scan reads metadata with unsupported default type | Scan setup | `unsupported initial-default literal for column <name>: <type>` |

## 10. Test Plan

### 10.1 Unit Tests

| Module | Cases |
|---|---|
| Parser (`src/engine/statement.rs`, `src/sql/parser/dialect/create_table.rs`) | per-type literal parses; duplicate DEFAULT rejected; DEFAULT NULL preserved; complex / unsupported types rejected; type mismatch rejected |
| Type mapping | DefaultLiteral ↔ iceberg::spec::Literal round-trip; decimal scale guard; date/datetime string parsing |
| `schema_update.rs` | ADD COLUMN with non-NULL DEFAULT on v3 → both `initial_default` and `write_default` set; on v2 → error; ADD COLUMN DEFAULT NULL on both → no metadata default written |
| CREATE TABLE (`registry.rs`) | v3 + DEFAULT writes metadata correctly; v2 + DEFAULT errors with the prescribed message |
| `descriptors.rs` codegen | TIcebergSchemaField round-trip preserves default JSON; default JSON parses back to the original Literal |
| `build_iceberg_default_array` | each supported primitive type produces correct constant array; row_count=0 boundary; unsupported type fails fast; corrupted JSON fails fast |
| `reorder_insert_row` | omitted column + write_default → uses literal; omitted column without write_default → NULL (preserves current behavior) |
| `align_chunk_to_target_schema` | SELECT projection in arbitrary order is mapped by name; omitted target column filled with write_default constant |

### 10.2 SQL Integration Tests

New cases under a new `sql-tests` suite `iceberg-v3-default`:

- `create_v3_table_with_default` — full lifecycle.
- `create_v2_table_with_default_rejected` — error message match.
- `add_column_default_to_v3_with_existing_data` — pre-add file reads default,
  post-add INSERT materializes default.
- `add_column_default_each_primitive_type` — each of D2's types.
- `default_null_on_v2_still_works` — does not regress existing v2 behavior.
- `insert_explicit_column_list_omits_default_column` — write-default fills.
- `insert_positional_count_mismatch_still_errors` — semantic guarantee.
- `select_old_file_after_add_column_returns_initial_default`.
- `corrupted_default_metadata_fails_fast` — synthetic broken metadata.

### 10.3 Interop Verification (Manual / Documented)

Documented under `docs/guides/iceberg-v3-default-interop.md` (followup), not in CI:

- Spark / pyiceberg writes a v3 table with `initial-default = 42` and data files
  that omit the column → NovaRocks `SELECT *` returns 42.
- NovaRocks creates v3 table `(a INT, b INT DEFAULT 5)`, executes
  `INSERT (a) VALUES (1)` → pyiceberg / Spark reads `(1, 5)`.

### 10.4 Failure Modes Under Test

- **Silent NULL-degradation** — explicitly tested in
  `corrupted_default_metadata_fails_fast` and an unsupported-default unit test.
- **v2 metadata pollution** — schema_update v2 unit tests verify metadata.json
  never contains `initial_default` / `write_default` for v2 tables.
- **VALUES positional drift** — `insert_positional_count_mismatch_still_errors`.
- **Double DEFAULT** — parser unit test.

### 10.5 Performance

- Read path: per-scan, only a single constant array per missing column (same
  cost class as `new_null_array`). No regression.
- Write path: VALUES path adds at most one literal clone per omitted column per
  row; FROM QUERY path adds one constant column construction per omitted column
  per chunk. Negligible relative to write IO.
- DDL path: one-time, no runtime impact.

## 11. Migration and Rollout

- Existing v2 tables continue to work unchanged. `DEFAULT NULL` paths remain
  byte-identical.
- Existing v3 tables created without defaults continue to work unchanged
  (Field metadata has no default key → fall through to NULL fill).
- FE-mode read paths continue to work unchanged until StarRocks FE populates
  the new optional thrift fields. No coordinated release required.
- No data migration needed.

## 12. Open Items / Future Work

- `ALTER COLUMN SET DEFAULT` / `DROP DEFAULT` (post-creation write-default
  mutation). Spec-allowed; tracked separately.
- DEFAULT for nested types (struct/list/map). Likely never user-facing, but the
  read-side fail-fast path is already correct.
- Positional INSERT auto-fill with write-default. Deliberately out of scope to
  preserve current strict-count semantics.
- StarRocks FE coordination to populate `initial_default_json` so FE-mode
  reads honor defaults end-to-end.
