# Iceberg CTAS Implementation Plan (PR-3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `CREATE TABLE [IF NOT EXISTS] <ident> [PARTITIONED BY (...)] [PROPERTIES (...)] AS <select>` for Iceberg targets. Schema is inferred from SELECT output; the created table is **always** `format-version=3` + `row-lineage=true`. The execution is two-phase: catalog `create_table` first, then a `FastAppendCommit` carrying the SELECT output. Any failure after `create_table` triggers `drop_table` rollback so the user does not see half-built tables.

**Architecture:** Extend the `CreateTable` AST node with `as_select: Option<Box<Query>>`; recognize `AS <select>` in the existing CREATE TABLE parser; reject the four hard-coded incompatibilities at parse time (`.branch_<x>` target, `'format-version' = '2'`, `'row-lineage' = 'false'`, explicit column list). Add a new engine module `src/engine/iceberg_ctas.rs` that runs schema inference, force-injects v3 properties, calls `catalog.create_table`, drives `IcebergSinkPlan` over the SELECT plan to produce `DataFile`s, dispatches `FastAppendCommit` via `run_iceberg_commit`, and on any failure attempts `catalog.drop_table` with documented error messages. Variant / geometry / geography column types from the SELECT output are rejected because their write path is not done.

**Tech Stack:** Rust 2021, sqlparser-rs (forked via `StarRocksDialect`), iceberg-rust 0.9.0 (vendored), arrow-rs.

**Spec:** `docs/design/specs/2026-05-06-iceberg-v3-write-path-completion-design.md` §5 / §7.3 / §8.

**Depends on:** None at the code level. Document-wise, prefer landing this **after** PR-1 (TRUNCATE) and PR-2 (OVERWRITE PARTITIONS) so the commit / abort / drop infrastructure is well exercised.

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Modify | `src/sql/parser/ast/mod.rs` | Add `as_select: Option<Box<Query>>` to the `CreateTable` AST node (or whatever form it takes — confirm during pre-flight) |
| Modify | `src/sql/parser/dialect/create_table.rs:24-26` | After parsing the CREATE TABLE prefix, look ahead for `AS <select>` and parse the query; enforce four parse-time rejections (branch target / format-version=2 / row-lineage=false / explicit column list) |
| Modify | `src/engine/statement.rs:840` | In `execute_create_table_statement`, when `kind == Iceberg && as_select.is_some()`, dispatch to new CTAS flow |
| Create | `src/engine/iceberg_ctas.rs` | Five-step CTAS engine flow + drop_table rollback + variant/geometry rejection + unit tests for failure paths |
| Modify | `src/engine/mod.rs` | `pub(crate) mod iceberg_ctas;` |
| Create | `sql-tests/iceberg/sql/iceberg_v3_ctas.sql` | SQL regression suite per spec §7.3 |
| Create | `sql-tests/iceberg/result/iceberg_v3_ctas.result` | Recorded fixture |
| Modify | `/Users/harbor/Documents/Obsidian/NovaRocks Iceberg v3 完成度清单.md` | §10 mark **two** rows `[x]` (CTAS + CTAS-default-v3); §20 fixture row; trailing changelog |

---

## Pre-Flight Investigation

Before Task 1, the implementer should read:

- `src/sql/parser/ast/mod.rs` — full `CreateTable` definition (the AST may use `CreateTableStmt { kind: CreateTableKind::Iceberg { ... } }` form). Confirm whether `as_select` should sit on the outer struct or the `Iceberg` variant.
- `src/sql/parser/dialect/create_table.rs` (full file) — current shape of `parse_create_table_statement`; understand the order of clause parsing.
- `src/engine/statement.rs:840` — full `execute_create_table_statement`; learn how the `CreateTableKind::Iceberg { ... }` arm currently builds `NestedField`s and dispatches to the catalog.
- `src/connector/iceberg/catalog/registry.rs:382 (create_table) and :473 (drop_table)` — the catalog facade `create_table` / `drop_table` signatures currently used. Verify error semantics (does `drop_table` error if the table doesn't exist? Spec §5.3 needs that to be a soft-success).
- `src/engine/iceberg_writer.rs::execute_iceberg_insert_or_overwrite` — the FastAppend path. CTAS Step C/D will call `drive_iceberg_sink` + `run_iceberg_commit` exactly the same way.
- `src/sql/optimizer/` (top level) — find how a `Query` AST gets analyzed/planned to a runnable plan (`grep -rn "fn plan_query\|fn analyze_query" src/sql/`). CTAS Step A needs this entry.

---

## Task 1: Extend `CreateTable` AST with `as_select`

**Files:**
- Modify: `src/sql/parser/ast/mod.rs`

- [ ] **Step 1.1: Locate the existing definition**

```bash
grep -n "pub struct CreateTableStmt\|pub enum CreateTableKind" src/sql/parser/ast/mod.rs
```

Read the surrounding lines to choose where `as_select` sits. Recommended placement: as a field on `CreateTableStmt`, since the AS-SELECT clause applies to all backends in principle (even though we currently only implement it for Iceberg).

- [ ] **Step 1.2: Add the field**

```rust
pub struct CreateTableStmt {
    // ... existing fields ...
    /// Present when the SQL was `CREATE TABLE ... AS <select>`.
    /// Schema and (optionally) partition spec are inferred from the query.
    pub as_select: Option<Box<Query>>,
}
```

(The exact `Query` type — `crate::sql::parser::ast::Query` or sqlparser's — must match the existing `Statement::Insert::source` representation. Pre-flight will confirm.)

- [ ] **Step 1.3: Update every call site**

Run: `grep -rn "CreateTableStmt {" src/`

For each constructor, add `as_select: None` to preserve existing behavior. Build:

```bash
cargo build 2>&1 | tail -20
```

Expected: clean.

- [ ] **Step 1.4: Commit**

```bash
git add src/sql/parser/ast/mod.rs src/
git commit -m "refactor(ast): add as_select field to CreateTableStmt"
```

---

## Task 2: Parse `AS <select>` in CREATE TABLE

**Files:**
- Modify: `src/sql/parser/dialect/create_table.rs`

- [ ] **Step 2.1: Failing parser tests**

Add to `create_table.rs`'s `#[cfg(test)] mod tests` (or the sibling test file):

```rust
#[test]
fn parse_create_table_as_select_basic() {
    let stmt = parse_create_table_one("CREATE TABLE t AS SELECT 1 AS x, 'a' AS y").unwrap();
    assert!(stmt.as_select.is_some());
    assert_eq!(stmt.column_defs.len(), 0,
        "explicit column list must be empty for CTAS");
}

#[test]
fn parse_create_table_as_select_if_not_exists() {
    let stmt = parse_create_table_one("CREATE TABLE IF NOT EXISTS t AS SELECT 1 AS x").unwrap();
    assert!(stmt.if_not_exists);
    assert!(stmt.as_select.is_some());
}

#[test]
fn parse_create_table_as_select_with_partitioned_by_and_properties() {
    let stmt = parse_create_table_one(
        "CREATE TABLE t PARTITIONED BY (days(ts)) PROPERTIES('k'='v') AS SELECT 1 AS x, NOW() AS ts"
    ).unwrap();
    assert!(stmt.as_select.is_some());
    assert!(stmt.partitioned_by.is_some());
    assert!(stmt.properties.iter().any(|(k, v)| k == "k" && v == "v"));
}

#[test]
fn parse_create_table_as_select_branch_rejected() {
    let err = parse_create_table_one("CREATE TABLE t.branch_dev AS SELECT 1 AS x").unwrap_err();
    assert!(err.to_lowercase().contains("branch"),
        "expected branch rejection, got: {err}");
}

#[test]
fn parse_create_table_as_select_format_version_2_rejected() {
    let err = parse_create_table_one(
        "CREATE TABLE t PROPERTIES('format-version'='2') AS SELECT 1 AS x"
    ).unwrap_err();
    assert!(err.to_lowercase().contains("format-version"),
        "expected format-version rejection, got: {err}");
}

#[test]
fn parse_create_table_as_select_row_lineage_false_rejected() {
    let err = parse_create_table_one(
        "CREATE TABLE t PROPERTIES('row-lineage'='false') AS SELECT 1 AS x"
    ).unwrap_err();
    assert!(err.to_lowercase().contains("row-lineage"),
        "expected row-lineage rejection, got: {err}");
}

#[test]
fn parse_create_table_as_select_with_explicit_columns_rejected() {
    let err = parse_create_table_one(
        "CREATE TABLE t (id INT, name VARCHAR(32)) AS SELECT 1, 'a'"
    ).unwrap_err();
    assert!(err.to_lowercase().contains("column"),
        "expected explicit-column rejection, got: {err}");
}

#[test]
fn parse_create_table_without_as_select_unchanged() {
    let stmt = parse_create_table_one(
        "CREATE TABLE t (id INT) ENGINE=ICEBERG"
    ).unwrap();
    assert!(stmt.as_select.is_none());
    assert_eq!(stmt.column_defs.len(), 1);
}
```

(`parse_create_table_one` is a helper that wraps the parser and returns `CreateTableStmt`. Reuse the existing helper if present; otherwise compose from `parse_one(...)` plus an `as Statement::CreateTable`.)

- [ ] **Step 2.2: Run, expect FAILS**

```bash
cargo test --lib parse_create_table_as_select
cargo test --lib parse_create_table_without_as_select_unchanged
```

Expected: 7 FAIL (`as_select` not parsed, rejections not present), 1 PASS (the no-AS regression).

- [ ] **Step 2.3: Implement parsing**

In `parse_create_table_statement`, **after** the existing PROPERTIES clause is consumed, add:

```rust
let as_select = if parser.parse_keyword(Keyword::AS) {
    // Hard rejections that depend on already-parsed state.
    if !column_defs.is_empty() {
        return Err(
            "CTAS with explicit column definitions is not supported; \
             use CREATE TABLE then INSERT instead".to_string(),
        );
    }
    let query = parser.parse_query()
        .map_err(|e| format!("CTAS: failed to parse SELECT: {e}"))?;
    Some(Box::new(query))
} else {
    None
};

if as_select.is_some() {
    // Branch / tag target rejection.
    if has_iceberg_ref_suffix(&name.0) {
        return Err("CTAS does not support branch target".to_string());
    }
    // PROPERTIES rejections.
    for (k, v) in &properties {
        match k.to_ascii_lowercase().as_str() {
            "format-version" if v != "3" => {
                return Err(format!(
                    "CTAS only supports format-version=3, got '{v}'"));
            }
            "row-lineage" if v.to_ascii_lowercase() != "true" => {
                return Err(format!(
                    "CTAS requires row-lineage=true, got '{v}'"));
            }
            _ => {}
        }
    }
}
```

Then construct `CreateTableStmt { ..., as_select }`. Where `parse_query` lives — sqlparser-rs's `Parser::parse_query()` returns `sqlparser::ast::Query`; the project may also have an internal wrapper. Confirm during pre-flight; if internal, use the internal one.

`has_iceberg_ref_suffix` is a small helper:

```rust
fn has_iceberg_ref_suffix(parts: &[Ident]) -> bool {
    if let Some(last) = parts.last() {
        let s = &last.value;
        s.starts_with("branch_") || s.starts_with("tag_")
    } else {
        false
    }
}
```

- [ ] **Step 2.4: Run, expect PASS**

```bash
cargo test --lib parse_create_table_as_select
```

Expected: 7 PASS.

- [ ] **Step 2.5: Run all parser tests, no regression**

```bash
cargo test --lib parser:: 2>&1 | tail -20
```

Expected: clean.

- [ ] **Step 2.6: Commit**

```bash
git add src/sql/parser/dialect/create_table.rs src/sql/parser/ast/mod.rs
git commit -m "feat(parser): CREATE TABLE AS SELECT + parse-time CTAS rejections"
```

---

## Task 3: Add `iceberg_ctas` engine module skeleton

**Files:**
- Create: `src/engine/iceberg_ctas.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/engine/statement.rs:840`

- [ ] **Step 3.1: Create skeleton**

```rust
//! CTAS for Iceberg backend.
//!
//! Five steps (spec §5):
//!   A. Plan the SELECT and infer the Iceberg schema.
//!   B. Catalog `create_table` (atomic point #1).
//!   C. Drive IcebergSinkPlan over the planned SELECT (atomic point #2).
//!   D. `run_iceberg_commit(FastAppendCommit)` (atomic point #3).
//!   E. On C / D failure, drop_table to roll back; on drop_table failure,
//!      return the documented combined error.

use std::sync::Arc;

use crate::connector::backend::ResolvedTable;
use crate::engine::{StandaloneState, StatementResult};
use crate::sql::parser::ast::CreateTableStmt;

pub(crate) fn execute_iceberg_ctas(
    _state: &Arc<StandaloneState>,
    _stmt: &CreateTableStmt,
    _current_database: &str,
) -> Result<StatementResult, String> {
    Err("CTAS not yet implemented".to_string())
}

#[cfg(test)]
mod tests {
    // Failure-path unit tests are populated in Task 7.
}
```

- [ ] **Step 3.2: Register module**

In `src/engine/mod.rs`:

```rust
pub(crate) mod iceberg_ctas;
```

- [ ] **Step 3.3: Wire dispatch in `statement.rs`**

In `execute_create_table_statement`, before the existing `CreateTableKind::Iceberg { ... }` arm runs, check `as_select`:

```rust
if let Some(_) = &stmt.as_select {
    if matches!(stmt.kind, CreateTableKind::Iceberg { .. }) {
        return crate::engine::iceberg_ctas::execute_iceberg_ctas(
            state, stmt, current_database);
    }
    return Err("CTAS is currently only supported for ENGINE=ICEBERG targets"
        .to_string());
}
```

(Or, if the project's existing CREATE TABLE flow does not treat ENGINE explicitly, reject CTAS for any non-iceberg target with the same message.)

- [ ] **Step 3.4: Build**

```bash
cargo build 2>&1 | tail -10
```

Expected: clean. CTAS attempts will surface the placeholder error.

- [ ] **Step 3.5: Commit**

```bash
git add src/engine/iceberg_ctas.rs src/engine/mod.rs src/engine/statement.rs
git commit -m "feat(engine): iceberg_ctas placeholder module + statement.rs dispatch"
```

---

## Task 4: Step A — plan SELECT and infer schema

**Files:**
- Modify: `src/engine/iceberg_ctas.rs`

- [ ] **Step 4.1: Failing test**

```rust
#[test]
fn step_a_infers_schema_from_simple_select() {
    let stmt = parse_ctas("CREATE TABLE t AS SELECT 1 AS id, 'a' AS name");
    let state = test_helpers::standalone_state_with_iceberg_warehouse();
    let (_plan, schema) = plan_and_infer_schema(&state, &stmt, "default")
        .expect("schema inference");
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.fields()[0].name(), "id");
    assert_eq!(schema.fields()[0].field_type().to_string(), "int");
    assert_eq!(schema.fields()[1].name(), "name");
    assert!(matches!(schema.fields()[1].field_type(),
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String)));
}

#[test]
fn step_a_rejects_duplicate_column_names() {
    let stmt = parse_ctas("CREATE TABLE t AS SELECT 1 AS x, 2 AS x");
    let state = test_helpers::standalone_state_with_iceberg_warehouse();
    let err = plan_and_infer_schema(&state, &stmt, "default").unwrap_err();
    assert!(err.contains("duplicate column name"));
}

#[test]
fn step_a_rejects_variant_column() {
    let stmt = parse_ctas("CREATE TABLE t AS SELECT parse_json('{}') AS v");
    let state = test_helpers::standalone_state_with_iceberg_warehouse();
    let err = plan_and_infer_schema(&state, &stmt, "default").unwrap_err();
    assert!(err.to_lowercase().contains("variant"),
        "expected variant rejection, got: {err}");
}
```

`parse_ctas` parses raw SQL into a `CreateTableStmt`. `plan_and_infer_schema(state, stmt, db) -> Result<(LogicalPlan, iceberg::spec::Schema), String>` is the helper introduced by this task.

- [ ] **Step 4.2: Run, expect FAILS**

```bash
cargo test --lib step_a_
```

Expected: 3 FAILS — function not defined.

- [ ] **Step 4.3: Implement**

```rust
use iceberg::spec::{NestedField, Schema as IcebergSchema, Type as IcebergType};
use crate::sql::analyzer::analyze_query;

pub(super) fn plan_and_infer_schema(
    state: &Arc<StandaloneState>,
    stmt: &CreateTableStmt,
    current_database: &str,
) -> Result<(LogicalPlan, IcebergSchema), String> {
    let query = stmt.as_select.as_ref()
        .ok_or("CTAS: missing AS SELECT clause")?;
    let plan = analyze_query(state, query, current_database)
        .map_err(|e| format!("CTAS: failed to analyze SELECT: {e}"))?;
    let output_schema = plan.output_schema();

    // Duplicate-column-name check.
    let mut seen = std::collections::HashSet::new();
    for col in output_schema.columns() {
        if !seen.insert(col.name()) {
            return Err(format!(
                "duplicate column name '{}' in CTAS; alias one with AS",
                col.name(),
            ));
        }
    }

    // Map arrow / starrocks types → iceberg types, allocating field-ids 1..N.
    let mut fields = Vec::with_capacity(output_schema.columns().len());
    for (i, col) in output_schema.columns().iter().enumerate() {
        let field_id = (i + 1) as i32;
        let iceberg_type = arrow_type_to_iceberg(col.data_type())
            .map_err(|e| format!(
                "CTAS column '{}': {e}; CTAS does not support \
                 variant/geometry/geography columns yet; use CREATE TABLE \
                 then INSERT", col.name()))?;
        fields.push(Arc::new(NestedField::optional(
            field_id, col.name(), iceberg_type)));
    }
    let schema = IcebergSchema::builder()
        .with_fields(fields)
        .with_schema_id(0)
        .build()
        .map_err(|e| format!("CTAS schema build failed: {e}"))?;
    Ok((plan, schema))
}

fn arrow_type_to_iceberg(t: &arrow::datatypes::DataType) -> Result<IcebergType, String> {
    use arrow::datatypes::DataType as Dt;
    use iceberg::spec::PrimitiveType as Pt;
    match t {
        Dt::Boolean => Ok(IcebergType::Primitive(Pt::Boolean)),
        Dt::Int32 => Ok(IcebergType::Primitive(Pt::Int)),
        Dt::Int64 => Ok(IcebergType::Primitive(Pt::Long)),
        Dt::Float32 => Ok(IcebergType::Primitive(Pt::Float)),
        Dt::Float64 => Ok(IcebergType::Primitive(Pt::Double)),
        Dt::Utf8 | Dt::LargeUtf8 => Ok(IcebergType::Primitive(Pt::String)),
        Dt::Binary | Dt::LargeBinary => Ok(IcebergType::Primitive(Pt::Binary)),
        Dt::Date32 => Ok(IcebergType::Primitive(Pt::Date)),
        Dt::Time64(_) => Ok(IcebergType::Primitive(Pt::Time)),
        Dt::Timestamp(_, None) => Ok(IcebergType::Primitive(Pt::Timestamp)),
        Dt::Timestamp(_, Some(_)) => Ok(IcebergType::Primitive(Pt::Timestamptz)),
        Dt::Decimal128(p, s) => Ok(IcebergType::Primitive(Pt::Decimal {
            precision: *p as u32, scale: *s as u32,
        })),
        Dt::Struct(fields) => {
            // Recurse for nested struct.
            let mut iceberg_fields = Vec::with_capacity(fields.len());
            for (i, f) in fields.iter().enumerate() {
                let id = (i + 1) as i32;  // local field-id; allocator caller re-numbers
                iceberg_fields.push(Arc::new(NestedField::optional(
                    id, f.name(), arrow_type_to_iceberg(f.data_type())?)));
            }
            Ok(IcebergType::Struct(iceberg::spec::StructType::new(iceberg_fields)))
        }
        Dt::List(field) | Dt::LargeList(field) => {
            Ok(IcebergType::List(iceberg::spec::ListType {
                element_field: Arc::new(NestedField::optional(
                    -1, "element", arrow_type_to_iceberg(field.data_type())?)),
            }))
        }
        Dt::Map(field, _) => {
            // Arrow Map = Struct{key, value}; expand.
            // ... (omitted for brevity; mirror existing CREATE TABLE mapping
            //  in src/engine/statement.rs::execute_create_table_statement
            //  Iceberg branch — pre-flight: grep for the existing mapper)
            todo!("delegate to existing mapper used by plain CREATE TABLE")
        }
        // Reject types whose write path is incomplete:
        Dt::Utf8View | Dt::BinaryView => Err("variant".to_string()),
        // Geometry / geography types come from a custom extension type. If
        // present, the matcher above will not catch them; instead, the
        // existing arrow extension mechanism returns a marker — pre-flight
        // to find how the codebase tags variant / geometry types and add
        // explicit branches here that return Err("variant" | "geometry" | "geography").
        other => Err(format!("unsupported arrow type {other:?}")),
    }
}
```

The nested field-id allocation in `arrow_type_to_iceberg` uses local ids (`-1` placeholder for nested types); the caller (`plan_and_infer_schema`) needs to do a second pass to re-number nested field-ids depth-first. Pre-flight: check whether `iceberg::spec::Schema::builder` already does this — if yes, drop the placeholder; if no, add a second pass:

```rust
fn assign_nested_field_ids(schema: &mut IcebergSchema, next_id: &mut i32) {
    // depth-first traversal that assigns each NestedField a unique id
    // ...
}
```

- [ ] **Step 4.4: Run, PASS**

```bash
cargo test --lib step_a_
```

Expected: 3 PASS.

- [ ] **Step 4.5: Commit**

```bash
git add src/engine/iceberg_ctas.rs
git commit -m "feat(ctas): step A — plan SELECT and infer iceberg schema"
```

---

## Task 5: Steps B–D — create table, write data, commit

**Files:**
- Modify: `src/engine/iceberg_ctas.rs`

- [ ] **Step 5.1: Wire the full happy path**

Replace `execute_iceberg_ctas` with:

```rust
pub(crate) fn execute_iceberg_ctas(
    state: &Arc<StandaloneState>,
    stmt: &CreateTableStmt,
    current_database: &str,
) -> Result<StatementResult, String> {
    // -- Step A: plan + schema.
    let (plan, schema) = plan_and_infer_schema(state, stmt, current_database)?;

    // Resolve target identifier (catalog/namespace/table).
    let target_ident = resolve_ctas_table_ident(stmt, current_database)?;

    // -- IF NOT EXISTS short-circuit.
    let catalog = state.connectors.read().expect("registry")
        .catalog_backend("iceberg")?
        .iceberg_catalog().clone();
    if stmt.if_not_exists && catalog.table_exists(&target_ident).map_err(|e| e.to_string())? {
        return Ok(StatementResult::Ok);
    }

    // -- Build PartitionSpec from PARTITIONED BY (validate columns exist in schema).
    let partition_spec = build_partition_spec(&schema, &stmt.partitioned_by)?;

    // -- Force-inject v3 properties.
    let mut properties = stmt.properties.iter().cloned()
        .collect::<std::collections::HashMap<String, String>>();
    properties.insert("format-version".to_string(), "3".to_string());
    properties.insert("row-lineage".to_string(), "true".to_string());

    // -- Step B: create_table.
    let create_req = iceberg::TableCreation::builder()
        .name(target_ident.name().to_string())
        .schema(schema.clone())
        .partition_spec(partition_spec.clone())
        .properties(properties)
        .build();
    let table = block_on_iceberg(async {
        catalog.create_table(target_ident.namespace(), create_req).await
    })??;

    // -- Step C + D: drive the sink and commit.
    // On any failure, drop_table to roll back.
    let result = (|| -> Result<StatementResult, String> {
        let written = drive_iceberg_sink_from_plan(state, &table, &plan)?;
        let staging_dir = format!(
            "{}/metadata/_staging/{}",
            table.metadata().location(), uuid::Uuid::new_v4());
        let collector = Arc::new(IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            target_ident.clone(),
            None,  // first snapshot — no parent
            0,     // first sequence number
            schema.clone(),
            partition_spec.clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        ));
        let default_spec_id = partition_spec.spec_id();
        for df in &written {
            let wf = data_file_to_written_file(df, default_spec_id)?;
            collector.inject_written_file(wf);
        }
        let abort_cleanup = build_abort_cleanup_for_table(&table)?;
        let file_io = table.file_io().clone();
        block_on_iceberg(async {
            run_iceberg_commit(RunInput {
                collector,
                catalog: catalog.clone(),
                table,
                fs: abort_cleanup.fs,
                file_io,
                cleanup_path_mapper: abort_cleanup.path_mapper,
                cow_update_sidecar: None,
                target_ref: "main".to_string(),
            }).await
        })??;
        invalidate_iceberg_caches(state, &target_ident)?;
        Ok(StatementResult::Ok)
    })();

    // -- Step E: on failure, attempt drop_table.
    match result {
        Ok(r) => Ok(r),
        Err(original_err) => {
            let drop_result = block_on_iceberg(async {
                catalog.drop_table(&target_ident).await
            });
            match drop_result {
                Ok(_) => {
                    // Distinguish whether failure was during data write (Step C)
                    // or during commit (Step D). Step D failures leave orphan
                    // data files behind — flag them in the message.
                    if original_err.contains("commit") {
                        Err(format!(
                            "CTAS failed during commit: {original_err}; \
                             cleaned up; orphan data files left in \
                             {table_loc}/data/", table_loc = "<warehouse>/<table>"))
                    } else {
                        Err(format!(
                            "CTAS failed during data write: {original_err}; \
                             cleaned up"))
                    }
                }
                Err(drop_err) => {
                    Err(format!(
                        "CTAS failed at <step>: {original_err}; cleanup also \
                         failed: {drop_err}; table {target_ident} may exist \
                         as orphan, drop manually"))
                }
            }
        }
    }
}

fn resolve_ctas_table_ident(stmt: &CreateTableStmt, current_database: &str)
    -> Result<TableIdent, String>
{
    // ... use the existing CREATE TABLE name resolution path ...
    todo!("delegate to existing helper")
}

fn build_partition_spec(
    schema: &IcebergSchema,
    partitioned_by: &Option<Vec<PartitionTransform>>,
) -> Result<PartitionSpec, String> {
    let Some(transforms) = partitioned_by else {
        return Ok(PartitionSpec::unpartitioned());
    };
    let mut builder = PartitionSpec::builder().with_spec_id(0);
    for (i, t) in transforms.iter().enumerate() {
        let source_field = schema.field_by_name(&t.column)
            .ok_or_else(|| format!(
                "partition column '{}' not found in SELECT output", t.column))?;
        builder = builder.add_partition_field(/* ... */)?;
    }
    builder.build().map_err(|e| e.to_string())
}
```

Helpers `drive_iceberg_sink_from_plan`, `build_abort_cleanup_for_table`, `data_file_to_written_file`, `invalidate_iceberg_caches` — `drive_iceberg_sink_from_plan` is a small adapter; the others exist in `iceberg_writer.rs` (mark `pub(crate)` if needed). Pre-flight verifies symbol availability.

`PartitionTransform`, `TableIdent`, `PartitionSpec` — types from the iceberg-rust vendored crate / project AST. Confirm during pre-flight.

- [ ] **Step 5.2: Build to compile-check**

```bash
cargo build 2>&1 | tail -20
```

Expected: a few `todo!()` paths but the file compiles. Replace `todo!()` after pre-flight.

- [ ] **Step 5.3: Smoke test happy path**

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
sleep 5
mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS test_ctas;
USE test_ctas;
-- Source data.
CREATE TABLE src (id INT, name VARCHAR(32))
  ENGINE=ICEBERG PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c');
-- CTAS.
CREATE TABLE dst AS SELECT id, UPPER(name) AS uname FROM src;
SELECT id, uname FROM dst ORDER BY id;
-- Confirm dst is v3 + row-lineage.
SHOW CREATE TABLE dst;
SQL
kill %1
```

Expected: `dst` contains 3 rows; `SHOW CREATE TABLE` reveals `format-version=3, row-lineage=true`.

- [ ] **Step 5.4: Commit**

```bash
git add src/engine/iceberg_ctas.rs
git commit -m "feat(ctas): full happy path B+C+D + IF NOT EXISTS shortcut"
```

---

## Task 6: Step E — failure rollback unit tests

**Files:**
- Modify: `src/engine/iceberg_ctas.rs`

These unit tests use a mock catalog to inject failures at Step C / Step D / drop_table. The sql-tests framework lacks fault-injection hooks, so this lives in `#[cfg(test)] mod tests`.

- [ ] **Step 6.1: Failing test for Step C failure**

```rust
#[tokio::test]
async fn step_c_failure_drops_table_and_returns_clean_error() {
    let (state, mock_catalog) = test_helpers::ctas_state_with_failing_sink();
    let stmt = parse_ctas("CREATE TABLE t AS SELECT 1 AS x");
    let err = execute_iceberg_ctas(&state, &stmt, "default").unwrap_err();
    assert!(err.contains("data write"), "got: {err}");
    assert!(err.contains("cleaned up"), "got: {err}");
    // Table should be gone.
    assert_eq!(mock_catalog.dropped_count(), 1);
}
```

`test_helpers::ctas_state_with_failing_sink` builds an in-memory state where `IcebergSinkPlan` execution returns an error for any input. `mock_catalog.dropped_count()` records `drop_table` calls.

- [ ] **Step 6.2: Failing test for Step D failure**

```rust
#[tokio::test]
async fn step_d_failure_drops_table_and_warns_about_orphan_files() {
    let (state, mock_catalog) = test_helpers::ctas_state_with_failing_commit();
    let stmt = parse_ctas("CREATE TABLE t AS SELECT 1 AS x");
    let err = execute_iceberg_ctas(&state, &stmt, "default").unwrap_err();
    assert!(err.contains("commit"));
    assert!(err.contains("orphan data files"));
    assert_eq!(mock_catalog.dropped_count(), 1);
}
```

- [ ] **Step 6.3: Failing test for drop_table failure**

```rust
#[tokio::test]
async fn drop_failure_returns_combined_error_message() {
    let (state, _mock) = test_helpers::ctas_state_with_failing_commit_and_drop();
    let stmt = parse_ctas("CREATE TABLE t AS SELECT 1 AS x");
    let err = execute_iceberg_ctas(&state, &stmt, "default").unwrap_err();
    assert!(err.contains("cleanup also failed"));
    assert!(err.contains("drop manually"));
}
```

- [ ] **Step 6.4: Build the mock catalog test helpers**

`test_helpers::ctas_state_with_*` — these likely don't exist yet. Implement minimal versions in `src/engine/iceberg_ctas.rs::tests` (or a sibling `test_helpers` mod). Mock pattern:

```rust
#[cfg(test)]
mod test_helpers {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    pub struct MockCatalog {
        pub create_calls: AtomicUsize,
        pub drop_calls: AtomicUsize,
        pub create_should_fail: bool,
        pub drop_should_fail: bool,
    }

    impl MockCatalog {
        pub fn dropped_count(&self) -> usize {
            self.drop_calls.load(Ordering::SeqCst)
        }
    }

    // impl iceberg::Catalog for MockCatalog { ... }
    // (minimal stub returning canned successes/failures)
    // ...
}
```

The mock catalog lives behind `#[cfg(test)]` only. The real `execute_iceberg_ctas` calls a `Catalog` trait method, so as long as the mock implements that trait, it can be substituted via `state.connectors`.

If wiring up a mock through `StandaloneState` is too heavy, factor out the catalog handle: have `execute_iceberg_ctas` accept `Arc<dyn Catalog>` as a parameter and have a thin wrapper `execute_iceberg_ctas_default` that pulls the iceberg catalog from `state` and forwards. The test then constructs its own `Arc<dyn Catalog>` mock and calls `execute_iceberg_ctas` directly.

- [ ] **Step 6.5: Run the three tests, expect PASS**

```bash
cargo test --lib step_c_failure step_d_failure drop_failure
```

Expected: 3 PASS.

- [ ] **Step 6.6: Commit**

```bash
git add src/engine/iceberg_ctas.rs
git commit -m "test(ctas): step E rollback paths (C-fail, D-fail, drop-fail)"
```

---

## Task 7: SQL regression suite

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_v3_ctas.sql`
- Create: `sql-tests/iceberg/result/iceberg_v3_ctas.result`

- [ ] **Step 7.1: SQL fixture per spec §7.3**

```sql
-- iceberg_v3_ctas.sql
-- Suite: iceberg
-- Coverage: spec §7.3 CTAS happy + error paths.

DROP DATABASE IF EXISTS iceberg_ctas;
CREATE DATABASE iceberg_ctas;
USE iceberg_ctas;

-- Source table for CTAS to read from.
CREATE TABLE src (id INT, name VARCHAR(32), region VARCHAR(8), ts TIMESTAMP)
  ENGINE=ICEBERG PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO src VALUES
  (1, 'alice',   'us', '2026-05-01 10:00:00'),
  (2, 'bob',     'eu', '2026-05-01 11:00:00'),
  (3, 'charlie', 'us', '2026-05-02 12:00:00');

-- Case 1: basic CTAS (no PARTITIONED BY, no PROPERTIES).
CREATE TABLE dst1 AS SELECT id, name FROM src;
SELECT * FROM dst1 ORDER BY id;
-- Verify dst1 is v3 + row-lineage.
SHOW CREATE TABLE dst1;

-- Case 2: PARTITIONED BY (days(ts)).
CREATE TABLE dst2 PARTITIONED BY (days(ts)) AS SELECT id, ts FROM src;
SELECT id FROM dst2 ORDER BY id;

-- Case 3: PROPERTIES with non-version-related keys.
CREATE TABLE dst3 PROPERTIES('write.parquet.compression-codec'='zstd')
  AS SELECT id FROM src;
SHOW CREATE TABLE dst3;

-- Case 4: nested types (struct / list).
CREATE TABLE dst4 AS
  SELECT named_struct('x', 1, 'y', 'a') AS s, ARRAY[1, 2, 3] AS l FROM src LIMIT 1;
SELECT * FROM dst4;

-- Case 5: IF NOT EXISTS, table already exists.
CREATE TABLE IF NOT EXISTS dst1 AS SELECT id FROM src WHERE 1=0;
-- Expect: dst1 unchanged (still has rows from Case 1); SELECT below proves it.
SELECT COUNT(*) FROM dst1;

-- Case 6: IF NOT EXISTS, table does not exist.
CREATE TABLE IF NOT EXISTS dst6 AS SELECT id FROM src;
SELECT COUNT(*) FROM dst6;

-- Case 7: CTAS then INSERT INTO continues.
INSERT INTO dst1 VALUES (99, 'late');
SELECT id FROM dst1 ORDER BY id;

-- Case 8 (error): branch-qualified CTAS target.
CREATE TABLE dst8.branch_dev AS SELECT id FROM src;

-- Case 9 (error): PROPERTIES('format-version'='2').
CREATE TABLE dst9 PROPERTIES('format-version'='2') AS SELECT id FROM src;

-- Case 10 (error): PROPERTIES('row-lineage'='false').
CREATE TABLE dst10 PROPERTIES('row-lineage'='false') AS SELECT id FROM src;

-- Case 11 (error): explicit column definitions.
CREATE TABLE dst11 (id INT) AS SELECT 1;

-- Case 12 (error): SELECT with duplicate column names without alias.
CREATE TABLE dst12 AS SELECT 1 AS x, 2 AS x;

-- Case 13 (error): PARTITIONED BY column not in SELECT output.
CREATE TABLE dst13 PARTITIONED BY (identity(ghost)) AS SELECT id FROM src;

-- Case 14 (error): table already exists, no IF NOT EXISTS.
CREATE TABLE dst1 AS SELECT id FROM src;

-- Cleanup.
DROP DATABASE iceberg_ctas;
```

- [ ] **Step 7.2: Record + verify**

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_v3_ctas --mode record --record-from target

# inspect for Case 8-14 — error messages should be clean strings, not panics.

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_v3_ctas --mode verify
kill %1
```

Expected: verify PASS.

- [ ] **Step 7.3: Commit**

```bash
git add sql-tests/iceberg/
git commit -m "test(iceberg): iceberg_v3_ctas regression suite"
```

---

## Task 8: Documentation update

(Pattern matches PR-1 Task 9 / PR-2 Task 10 — not duplicated.)

- [ ] **Step 8.1: §10 — change two rows from `[ ]` to `[x]`**:
  - `- [x] CTAS（CREATE TABLE AS SELECT）写 Iceberg ← 落地于 2026-MM-DD · #<PR>`
  - `- [x] CTAS 写 Iceberg + 默认 V3 row-lineage ← 落地于 2026-MM-DD · #<PR>`
- [ ] **Step 8.2: §20** — append fixture row for `iceberg_v3_ctas.sql`.
- [ ] **Step 8.3: trailing changelog** — append row.
- [ ] **Step 8.4: §23 P3** — strike `~~CTAS~~` if listed (verify; CTAS may not be in P3 currently).

---

## Task 9: Final verification

- [ ] **Step 9.1**: `cargo test --lib`
- [ ] **Step 9.2**: `cargo fmt --check && cargo clippy --lib --tests -- -D warnings`
- [ ] **Step 9.3**: full iceberg suite verify (release build, parallel)
- [ ] **Step 9.4**: open PR with title `feat(iceberg): CREATE TABLE AS SELECT (CTAS) for v3 row-lineage tables`

---

## Self-Review Notes

**Spec coverage:**

| Spec section | Implemented in |
|---|---|
| §0 CTAS strict v3 + row-lineage | Task 5 (Step B injects properties) + Task 2 (parser rejects opt-out) |
| §0 schema from SELECT | Task 4 |
| §0 CTAS rejects branch target | Task 2 |
| §0 atomic rollback on failure | Task 5 (Step E) + Task 6 (unit tests) |
| §2.2 CTAS parser + four parse-time rejections | Task 2 |
| §5.1 five-step engine flow A/B/C/D/E | Task 4 (A) + Task 5 (B/C/D/E happy + rollback) |
| §5.2 schema rules (duplicate column / variant rejection / IF NOT EXISTS / branch / target_ref="main") | Task 4 + Task 5 |
| §5.3 four-quadrant failure error messages | Task 5 (Step E) + Task 6 (unit tests) |
| §5.4 fail-fast (parser-level) | Task 2 |
| §5.5 reuse INSERT FastAppend / data_writer / AbortLog | Task 5 (collector + run_iceberg_commit + drive_iceberg_sink_from_plan) |
| §7.3 SQL regression cases 1–14 | Task 7 |
| §8.2 checklist (two rows + §20 + changelog) | Task 8 |

**Type-consistency:**

- `as_select: Option<Box<Query>>` — same `Query` type as existing `Statement::Insert::source` (subject to pre-flight confirmation).
- `target_ref: String` always `"main"` for CTAS (parser rejects branch suffixes).
- `properties: HashMap<String, String>` — `format-version` and `row-lineage` keys force-injected by Step 5.1; parser pre-validates user-supplied conflicting values.
- `IcebergSchema` field-id: linear allocation in Step 4.3, with a documented second pass for nested type re-numbering.

**Placeholder scan:** several `todo!()` / `delegate to existing helper` markers in Task 5.1's body. These are deliberate: the implementer must verify the existing helper (e.g. `data_file_to_written_file`, `arrow Map → iceberg Map` mapping) before copying. Each `todo!()` is annotated with the name of the helper to delegate to. None block compilation when the corresponding helper is wired up; tests in Task 5.3 / Task 6 fail loudly if any `todo!()` is reached at runtime.

**Risks (spec §10.1):**

- **R1 (CTAS Step D commit failure → orphan data files)**: explicitly surfaced in the error message produced by Task 5's Step E; relies on future REMOVE ORPHAN FILES (clue: spec §0.2 non-goal, plan §10).
- **R3 (variant / geometry / geography write path incomplete)**: rejected at Task 4.3's `arrow_type_to_iceberg`. Pre-flight must locate where these types are tagged in arrow extension metadata (or wherever NovaRocks tracks them) so the rejection is reliable.
