# Iceberg V3 UPDATE / MERGE INTO Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Iceberg v3 row-lineage `UPDATE ... FROM` with stable `_row_id`, default copy-on-write behavior, optional merge-on-read behavior, and MV incremental refresh support for both modes.

**Architecture:** Add a unified `engine::mutation_flow` that parses UPDATE intent into row-level mutation batches, validates stable row identity, and dispatches to COW or MOR writers based on Iceberg table properties. COW writes marked `operation=overwrite` snapshots plus a NovaRocks sidecar; MOR extends the existing Puffin-DV RowDelta path to add updated data files in the same snapshot. The Iceberg change planner consumes update markers so MV refresh can materialize old/new row pairs instead of treating row-level updates as ordinary overwrite or unrelated delete/insert.

**Tech Stack:** Rust, sqlparser, Apache Arrow `RecordBatch`, vendored `iceberg-rust` 0.9 APIs, Parquet writer, Puffin deletion vectors, NovaRocks `sql-tests`.

---

## Scope Check

This plan covers the whole accepted design, but the execution is intentionally staged so each stage is testable on its own:

1. COW UPDATE correctness with stable row lineage.
2. MOR UPDATE correctness with DV plus updated rows.
3. MV incremental refresh integration for COW and MOR.
4. MERGE SQL plumbing after UPDATE proves the shared mutation executor.

Do not start Stage 4 until Stages 1-3 are committed and validated. Stage 4 depends on the mutation executor and MV change model from the earlier stages.

---

## File Structure

- Create `src/engine/mutation_flow.rs`: UPDATE execution coordinator, write-mode selection, validation, join query construction, matched-row extraction, and dispatch to COW/MOR writers.
- Modify `src/engine/mod.rs`: import `mutation_flow`, route raw sqlparser `Statement::Update`, and expose tests.
- Modify `src/engine/statement.rs`: add conversion from sqlparser UPDATE to a compact `UpdateStmt`.
- Modify `src/sql/parser/ast/mod.rs`: add `UpdateStmt`, `UpdateAssignment`, and `MutationSource`.
- Modify `src/connector/iceberg/commit/types.rs`: add `IcebergUpdateMode`, update marker constants, sidecar model, and helper parsers.
- Modify `src/connector/iceberg/commit/validation.rs`: add v3 row-lineage-only validation and update mode selection.
- Modify `src/connector/iceberg/data_writer.rs`: add a row-lineage-aware data writer entrypoint that can store reserved metadata columns.
- Create `src/connector/iceberg/commit/update_cow.rs`: commit action for row-level COW update snapshots with `operation=overwrite`.
- Modify `src/connector/iceberg/commit/row_delta_dv.rs`: support adding data files alongside DVs for MOR updates.
- Modify `src/connector/iceberg/commit/mod.rs`: export update commit helpers.
- Modify `src/connector/iceberg/changes.rs`: recognize COW/MOR update markers and materialize update old/new row sets.
- Modify `src/connector/starrocks/managed/ivm_change_stream.rs`: pass richer update batches through `materialize_changes`.
- Modify `src/connector/starrocks/managed/mv_refresh_strategy.rs`: keep ordinary overwrite as full refresh while allowing marked COW updates to stay incremental.
- Add SQL tests under `sql-tests/iceberg/` and `sql-tests/mv-on-iceberg/`.

---

### Task 1: Add Mutation Types And Update Mode Selection

**Files:**
- Modify: `src/sql/parser/ast/mod.rs`
- Modify: `src/connector/iceberg/commit/types.rs`
- Modify: `src/connector/iceberg/commit/validation.rs`
- Test: unit tests in `src/connector/iceberg/commit/validation.rs`

- [ ] **Step 1: Add custom UPDATE statement types**

Add the following near the existing `DeleteStmt` in `src/sql/parser/ast/mod.rs`:

```rust
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct UpdateStmt {
    pub table: ObjectName,
    pub alias: Option<String>,
    pub assignments: Vec<UpdateAssignment>,
    pub source: Option<MutationSource>,
    pub where_clause: Option<sqlparser::ast::Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct UpdateAssignment {
    pub column: String,
    pub value: sqlparser::ast::Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MutationSource {
    Table {
        name: ObjectName,
        alias: Option<String>,
    },
    Query {
        query: Box<sqlparser::ast::Query>,
        alias: Option<String>,
    },
}
```

- [ ] **Step 2: Add update mode and marker constants**

Add `use serde::{Deserialize, Serialize};` with the existing imports in
`src/connector/iceberg/commit/types.rs`, then add this after `IcebergSqlDeleteStrategy`:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergUpdateMode {
    CopyOnWrite,
    MergeOnRead,
}

pub const NOVAROCKS_ROW_LEVEL_OP: &str = "novarocks.row-level-op";
pub const NOVAROCKS_ROW_LEVEL_OP_UPDATE: &str = "update";
pub const NOVAROCKS_UPDATE_MODE: &str = "novarocks.update.mode";
pub const NOVAROCKS_UPDATE_MODE_COW: &str = "copy-on-write";
pub const NOVAROCKS_UPDATE_MODE_MOR: &str = "merge-on-read";
pub const NOVAROCKS_UPDATE_SIDECAR: &str = "novarocks.update.sidecar";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationSidecar {
    pub version: u32,
    pub operation: String,
    pub mode: String,
    pub base_snapshot_id: i64,
    pub target_table_uuid: String,
    pub updated_row_ids: Vec<i64>,
    pub touched_data_files: Vec<MutationSidecarFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationSidecarFile {
    pub old_file: String,
    pub new_files: Vec<String>,
    pub row_ids: Vec<i64>,
}
```

- [ ] **Step 3: Add row-lineage update validators**

Add to `src/connector/iceberg/commit/validation.rs`:

```rust
use super::types::IcebergUpdateMode;

pub fn ensure_update_requires_v3_row_lineage(table: &Table) -> Result<(), String> {
    ensure_no_variant_columns(table)?;
    let metadata = table.metadata();
    if metadata.format_version() != FormatVersion::V3 || !row_lineage_property_enabled(metadata.properties()) {
        return Err(
            "UPDATE requires an Iceberg v3 table with write.row-lineage=true".to_string(),
        );
    }
    Ok(())
}

pub fn select_iceberg_update_mode(table: &Table) -> Result<IcebergUpdateMode, String> {
    ensure_update_requires_v3_row_lineage(table)?;
    let value = table
        .metadata()
        .properties()
        .get("write.update.mode")
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "copy-on-write".to_string());
    match value.as_str() {
        "copy-on-write" => Ok(IcebergUpdateMode::CopyOnWrite),
        "merge-on-read" => Ok(IcebergUpdateMode::MergeOnRead),
        other => Err(format!(
            "unsupported write.update.mode `{other}`; expected copy-on-write or merge-on-read"
        )),
    }
}
```

- [ ] **Step 4: Add unit tests for update mode selection**

In `src/connector/iceberg/commit/validation.rs`, add the pure helper shown below and the
metadata-level helper tests that exercise it:

```rust
#[test]
fn update_mode_defaults_to_copy_on_write() {
    let props = HashMap::from([("write.row-lineage".to_string(), "true".to_string())]);
    assert_eq!(
        select_update_mode_from_properties(FormatVersion::V3, &props).expect("mode"),
        IcebergUpdateMode::CopyOnWrite
    );
}

#[test]
fn update_mode_accepts_merge_on_read() {
    let props = HashMap::from([
        ("write.row-lineage".to_string(), "true".to_string()),
        ("write.update.mode".to_string(), "merge-on-read".to_string()),
    ]);
    assert_eq!(
        select_update_mode_from_properties(FormatVersion::V3, &props).expect("mode"),
        IcebergUpdateMode::MergeOnRead
    );
}

#[test]
fn update_mode_rejects_v3_without_row_lineage() {
    let props = HashMap::new();
    let err = select_update_mode_from_properties(FormatVersion::V3, &props).expect_err("must fail");
    assert!(err.contains("write.row-lineage=true"), "{err}");
}

#[test]
fn update_mode_rejects_invalid_property() {
    let props = HashMap::from([
        ("write.row-lineage".to_string(), "true".to_string()),
        ("write.update.mode".to_string(), "delta".to_string()),
    ]);
    let err = select_update_mode_from_properties(FormatVersion::V3, &props).expect_err("must fail");
    assert!(err.contains("unsupported write.update.mode"), "{err}");
}
```

The helper under test should be:

```rust
fn select_update_mode_from_properties(
    format_version: FormatVersion,
    props: &HashMap<String, String>,
) -> Result<IcebergUpdateMode, String> {
    if format_version != FormatVersion::V3 || !row_lineage_property_enabled(props) {
        return Err(
            "UPDATE requires an Iceberg v3 table with write.row-lineage=true".to_string(),
        );
    }
    let value = props
        .get("write.update.mode")
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "copy-on-write".to_string());
    match value.as_str() {
        "copy-on-write" => Ok(IcebergUpdateMode::CopyOnWrite),
        "merge-on-read" => Ok(IcebergUpdateMode::MergeOnRead),
        other => Err(format!(
            "unsupported write.update.mode `{other}`; expected copy-on-write or merge-on-read"
        )),
    }
}
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cargo test --lib update_mode -- --nocapture
```

Expected before implementation: compile failure or missing helper.
Expected after implementation: all update-mode tests pass.

- [ ] **Step 6: Commit Task 1**

```bash
git add src/sql/parser/ast/mod.rs src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/validation.rs
git commit -m "feat(iceberg): add update mutation modes"
```

---

### Task 2: Parse And Route UPDATE ... FROM

**Files:**
- Modify: `src/engine/statement.rs`
- Modify: `src/engine/mod.rs`
- Create: `src/engine/mutation_flow.rs`
- Modify: `src/engine/mod.rs` module declarations
- Test: unit tests in `src/engine/statement.rs`

- [ ] **Step 1: Add sqlparser UPDATE conversion tests**

Add tests to `src/engine/statement.rs`:

```rust
#[test]
fn convert_update_from_table_source() {
    let stmt = crate::sql::parser::parse_sql_raw(
        "update ice.db1.t as t set v = s.v from staging.src as s where t.id = s.id",
    )
    .expect("parse");
    let sqlparser::ast::Statement::Update(_) = &stmt else {
        panic!("expected update statement: {stmt:?}");
    };
    let update = convert_sqlparser_update_to_custom(&stmt).expect("convert");
    assert_eq!(update.table.0, vec!["ice", "db1", "t"]);
    assert_eq!(update.alias.as_deref(), Some("t"));
    assert_eq!(update.assignments.len(), 1);
    assert_eq!(update.assignments[0].column, "v");
    assert!(update.source.is_some());
    assert!(update.where_clause.is_some());
}

#[test]
fn convert_update_rejects_multi_column_assignment() {
    let stmt = crate::sql::parser::parse_sql_raw(
        "update ice.db1.t set (v1, v2) = (1, 2) where id = 1",
    )
    .expect("parse");
    let err = convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
    assert!(err.contains("single-column UPDATE assignments"), "{err}");
}
```

- [ ] **Step 2: Implement `convert_sqlparser_update_to_custom`**

Add to `src/engine/statement.rs` near the DELETE converter:

```rust
pub(crate) fn convert_sqlparser_update_to_custom(
    statement: &sqlparser::ast::Statement,
) -> Result<crate::sql::parser::ast::UpdateStmt, String> {
    use crate::sql::parser::ast::{MutationSource, UpdateAssignment, UpdateStmt};
    use sqlparser::ast as sqlast;

    let sqlast::Statement::Update(update) = statement else {
        return Err("expected UPDATE statement".to_string());
    };
    let sqlast::Update {
        table,
        assignments,
        from,
        selection,
        returning,
        limit,
        ..
    } = update;
    if returning.is_some() {
        return Err("UPDATE RETURNING is not supported".to_string());
    }
    if limit.is_some() {
        return Err("UPDATE LIMIT is not supported".to_string());
    }

    let (target_name, target_alias) = match &table.relation {
        sqlast::TableFactor::Table { name, alias, .. } => (
            crate::sql::parser::dialect::convert_object_name(name.clone())?,
            alias.as_ref().map(|a| a.name.value.clone()),
        ),
        other => return Err(format!("UPDATE target must be a table, got {other:?}")),
    };

    let mut out_assignments = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let sqlast::AssignmentTarget::ColumnName(column_name) = &assignment.target else {
            return Err("only single-column UPDATE assignments are supported".to_string());
        };
        if column_name.0.len() != 1 {
            return Err(format!(
                "UPDATE assignment must reference an unqualified target column, got `{column_name}`"
            ));
        }
        out_assignments.push(UpdateAssignment {
            column: column_name.0[0].value.clone(),
            value: assignment.value.clone(),
        });
    }
    if out_assignments.is_empty() {
        return Err("UPDATE requires at least one assignment".to_string());
    }

    let source = convert_update_from_source(from)?;
    Ok(UpdateStmt {
        table: target_name,
        alias: target_alias,
        assignments: out_assignments,
        source,
        where_clause: selection.clone(),
    })
}

fn convert_update_from_source(
    from: &Option<sqlparser::ast::UpdateTableFromKind>,
) -> Result<Option<crate::sql::parser::ast::MutationSource>, String> {
    use crate::sql::parser::ast::MutationSource;
    use sqlparser::ast as sqlast;

    let Some(from) = from else {
        return Ok(None);
    };
    let tables = match from {
        sqlast::UpdateTableFromKind::BeforeSet(tables)
        | sqlast::UpdateTableFromKind::AfterSet(tables) => tables,
    };
    if tables.len() != 1 {
        return Err(format!(
            "UPDATE ... FROM supports exactly one source relation, got {}",
            tables.len()
        ));
    }
    if !tables[0].joins.is_empty() {
        return Err("UPDATE ... FROM joins must be wrapped in a subquery".to_string());
    }
    match &tables[0].relation {
        sqlast::TableFactor::Table { name, alias, .. } => Ok(Some(MutationSource::Table {
            name: crate::sql::parser::dialect::convert_object_name(name.clone())?,
            alias: alias.as_ref().map(|a| a.name.value.clone()),
        })),
        sqlast::TableFactor::Derived { subquery, alias, .. } => Ok(Some(MutationSource::Query {
            query: subquery.clone(),
            alias: alias.as_ref().map(|a| a.name.value.clone()),
        })),
        other => Err(format!("unsupported UPDATE ... FROM source: {other:?}")),
    }
}
```

- [ ] **Step 3: Add mutation flow skeleton**

Create `src/engine/mutation_flow.rs`:

```rust
use std::sync::Arc;

use crate::engine::{StandaloneState, StatementResult};
use crate::sql::parser::ast::UpdateStmt;

pub(crate) fn execute_update_statement(
    state: &Arc<StandaloneState>,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        &stmt.table,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "UPDATE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    let _ = (target, stmt);
    Err("UPDATE execution reaches mutation_flow; Task 3 adds validation and match planning".to_string())
}
```

- [ ] **Step 4: Route UPDATE statements**

Modify `src/engine/mod.rs`:

```rust
mod mutation_flow;
```

Add a raw statement match arm near INSERT/DELETE:

```rust
sqlast::Statement::Update { .. } => {
    let stmt = crate::engine::statement::convert_sqlparser_update_to_custom(statement)?;
    crate::engine::mutation_flow::execute_update_statement(
        &self.inner,
        &stmt,
        current_catalog,
        current_database,
    )
}
```

Use the existing match variable name in `execute_sql` if it is not `statement`; do not clone the AST unless the borrow checker requires it.

- [ ] **Step 5: Run parser tests**

Run:

```bash
cargo test --lib convert_update -- --nocapture
```

Expected: both conversion tests pass.

- [ ] **Step 6: Commit Task 2**

```bash
git add src/sql/parser/ast/mod.rs src/engine/statement.rs src/engine/mod.rs src/engine/mutation_flow.rs
git commit -m "feat(sql): route update from statements"
```

---

### Task 3: Build Mutation Query And Validations

**Files:**
- Modify: `src/engine/mutation_flow.rs`
- Test: unit tests in `src/engine/mutation_flow.rs`

- [ ] **Step 1: Add validation unit tests**

Add tests in `src/engine/mutation_flow.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog::ColumnDef;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_key: false,
            aggregation: None,
            logical_type: None,
        }
    }

    #[test]
    fn reject_reserved_update_columns() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "_row_id".to_string(),
                value: sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
                    "1".to_string(),
                    false,
                )),
            }],
            &[col("id"), col("v")],
            &[],
        )
        .expect_err("must reject");
        assert!(err.contains("reserved Iceberg metadata column"), "{err}");
    }

    #[test]
    fn reject_partition_column_update() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "id".to_string(),
                value: sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
                    "1".to_string(),
                    false,
                )),
            }],
            &[col("id"), col("v")],
            &["id".to_string()],
        )
        .expect_err("must reject");
        assert!(err.contains("partition column"), "{err}");
    }
}
```

- [ ] **Step 2: Implement assignment validation**

Add to `src/engine/mutation_flow.rs`:

```rust
fn validate_update_assignments(
    assignments: &[crate::sql::parser::ast::UpdateAssignment],
    target_columns: &[crate::engine::catalog::ColumnDef],
    partition_columns: &[String],
) -> Result<(), String> {
    let target_names = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let partition_names = partition_columns
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let mut seen = std::collections::HashSet::new();
    for assignment in assignments {
        let name = assignment.column.to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "_row_id" | "_last_updated_sequence_number" | "_file" | "_pos"
        ) {
            return Err(format!(
                "UPDATE cannot assign reserved Iceberg metadata column `{}`",
                assignment.column
            ));
        }
        if !target_names.contains(&name) {
            return Err(format!(
                "UPDATE assignment references unknown target column `{}`",
                assignment.column
            ));
        }
        if partition_names.contains(&name) {
            return Err(format!(
                "UPDATE cannot modify Iceberg partition column `{}` in the first implementation",
                assignment.column
            ));
        }
        if !seen.insert(name) {
            return Err(format!(
                "UPDATE assignment lists target column `{}` more than once",
                assignment.column
            ));
        }
    }
    Ok(())
}
```

- [ ] **Step 3: Add duplicate row-id checker**

Add test:

```rust
#[test]
fn duplicate_row_ids_are_rejected() {
    let err = validate_unique_target_row_ids(&[7, 8, 7]).expect_err("duplicate");
    assert!(err.contains("_row_id=7"), "{err}");
}
```

Add implementation:

```rust
fn validate_unique_target_row_ids(row_ids: &[i64]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for row_id in row_ids {
        if !seen.insert(*row_id) {
            return Err(format!(
                "UPDATE source matched target row _row_id={} more than once; deduplicate the source before retrying",
                row_id
            ));
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Add join query builder contract**

Add a pure helper to construct the SELECT wrapper. It must project identity columns before user columns:

```rust
fn build_update_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: Option<&str>,
    assignments_sql: &[(&str, &str)],
    where_sql: Option<&str>,
) -> String {
    let mut select_items = vec![
        format!("{target_alias}._file AS __nr_file"),
        format!("{target_alias}._pos AS __nr_pos"),
        format!("{target_alias}._row_id AS __nr_row_id"),
        format!(
            "{target_alias}._last_updated_sequence_number AS __nr_last_updated_sequence_number"
        ),
        format!("{target_alias}.*"),
    ];
    for (column, expr) in assignments_sql {
        select_items.push(format!("{expr} AS __nr_new_{column}"));
    }
    let mut sql = format!("SELECT {} FROM {target_sql}", select_items.join(", "));
    if let Some(source) = source_sql {
        sql.push_str(" CROSS JOIN ");
        sql.push_str(source);
    }
    if let Some(pred) = where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(pred);
    }
    sql
}
```

Add test:

```rust
#[test]
fn update_match_query_projects_identity_columns() {
    let sql = build_update_match_query_sql(
        "ice.db1.t AS t",
        "t",
        Some("staging.s AS s"),
        &[("v", "s.v")],
        Some("t.id = s.id"),
    );
    assert!(sql.contains("t._row_id AS __nr_row_id"), "{sql}");
    assert!(sql.contains("s.v AS __nr_new_v"), "{sql}");
    assert!(sql.contains("WHERE t.id = s.id"), "{sql}");
}
```

The first implementation can use this SQL string route because the existing engine already accepts SELECT SQL and registers Iceberg tables for queries.

- [ ] **Step 5: Run mutation helper tests**

Run:

```bash
cargo test --lib mutation_flow -- --nocapture
```

Expected: validation and query-builder tests pass.

- [ ] **Step 6: Commit Task 3**

```bash
git add src/engine/mutation_flow.rs
git commit -m "feat(engine): add update mutation validation"
```

---

### Task 4: Add Row-Lineage Data Writer

**Files:**
- Modify: `src/connector/iceberg/data_writer.rs`
- Test: unit tests in `src/connector/iceberg/data_writer.rs`

- [ ] **Step 1: Add row-lineage write input types**

In `src/connector/iceberg/data_writer.rs`, add:

```rust
#[derive(Clone, Debug)]
pub(crate) struct RowLineageColumns {
    pub row_ids: arrow::array::Int64Array,
    pub last_updated_sequence_numbers: arrow::array::Int64Array,
}

#[derive(Clone, Debug)]
pub(crate) struct RowLineageWriteBatch {
    pub user_batch: arrow::record_batch::RecordBatch,
    pub lineage: RowLineageColumns,
}
```

- [ ] **Step 2: Add schema merger helper**

Add helper:

```rust
pub(crate) fn append_row_lineage_columns(
    batch: &arrow::record_batch::RecordBatch,
    lineage: RowLineageColumns,
) -> Result<arrow::record_batch::RecordBatch, String> {
    use std::sync::Arc;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    if batch.num_rows() != lineage.row_ids.len()
        || batch.num_rows() != lineage.last_updated_sequence_numbers.len()
    {
        return Err(format!(
            "row-lineage column length mismatch: rows={}, row_ids={}, last_updated={}",
            batch.num_rows(),
            lineage.row_ids.len(),
            lineage.last_updated_sequence_numbers.len()
        ));
    }

    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(
        Field::new("_row_id", DataType::Int64, false)
            .with_metadata(std::collections::HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2147483540".to_string(),
            )])),
    ));
    fields.push(Arc::new(
        Field::new("_last_updated_sequence_number", DataType::Int64, true)
            .with_metadata(std::collections::HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2147483539".to_string(),
            )])),
    ));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(lineage.row_ids) as ArrayRef);
    columns.push(Arc::new(lineage.last_updated_sequence_numbers) as ArrayRef);
    arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("build row-lineage record batch failed: {e}"))
}
```

- [ ] **Step 3: Add tests for metadata columns**

Add:

```rust
#[test]
fn append_row_lineage_columns_sets_reserved_field_ids() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use std::sync::Arc;

    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["a", "b"]))],
    )
    .expect("batch");
    let out = append_row_lineage_columns(
        &batch,
        RowLineageColumns {
            row_ids: Int64Array::from(vec![10, 11]),
            last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
        },
    )
    .expect("append");
    assert_eq!(out.num_columns(), 3);
    assert_eq!(out.schema().field(1).name(), "_row_id");
    assert_eq!(
        out.schema().field(1).metadata().get(PARQUET_FIELD_ID_META_KEY).map(String::as_str),
        Some("2147483540")
    );
    assert_eq!(out.schema().field(2).name(), "_last_updated_sequence_number");
}
```

- [ ] **Step 4: Add writer entrypoint**

Add an entrypoint that delegates to existing data file writing after appending metadata columns:

```rust
pub(crate) async fn write_row_lineage_batches_as_data_files(
    table: &iceberg::table::Table,
    batches: &[RowLineageWriteBatch],
) -> Result<Vec<iceberg::spec::DataFile>, String> {
    let mut enriched = Vec::with_capacity(batches.len());
    for batch in batches {
        enriched.push(append_row_lineage_columns(
            &batch.user_batch,
            batch.lineage.clone(),
        )?);
    }
    write_record_batches_as_data_files(table, enriched).await
}
```

- [ ] **Step 5: Run data writer tests**

Run:

```bash
cargo test --lib row_lineage_columns -- --nocapture
```

Expected: row-lineage schema tests pass.

- [ ] **Step 6: Commit Task 4**

```bash
git add src/connector/iceberg/data_writer.rs
git commit -m "feat(iceberg): write row lineage metadata columns"
```

---

### Task 5: Implement Copy-On-Write UPDATE Commit

**Files:**
- Create: `src/connector/iceberg/commit/update_cow.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`
- Modify: `src/connector/iceberg/commit/types.rs`
- Modify: `src/engine/mutation_flow.rs`
- Test: unit tests in `src/connector/iceberg/commit/update_cow.rs`

- [ ] **Step 1: Add COW commit module export**

In `src/connector/iceberg/commit/mod.rs`:

```rust
mod update_cow;
pub use update_cow::{CowUpdateCommit, write_mutation_sidecar};
```

- [ ] **Step 2: Create COW commit skeleton**

Create `src/connector/iceberg/commit/update_cow.rs`:

```rust
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, MAIN_BRANCH, ManifestFile, Operation, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction};
use super::helpers::{generate_snapshot_id, metadata_dir, now_ms, write_manifest_list};
use super::overwrite::{enumerate_live_data_files, write_added_data_manifest, write_overwrite_deletes_manifest};
use super::types::{
    CommitOutcome, MutationSidecar, NOVAROCKS_ROW_LEVEL_OP, NOVAROCKS_ROW_LEVEL_OP_UPDATE,
    NOVAROCKS_UPDATE_MODE, NOVAROCKS_UPDATE_MODE_COW, NOVAROCKS_UPDATE_SIDECAR, WrittenFile,
};

pub struct CowUpdateCommit {
    pub sidecar: MutationSidecar,
}

#[async_trait]
impl IcebergCommitAction for CowUpdateCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "CowUpdateCommit received {:?} content; expected Data only",
                    f.content
                ));
            }
        }
        let manifest_paths_out = Arc::new(Mutex::new(Vec::new()));
        let action = CowUpdateTxnAction {
            written,
            sidecar: self.sidecar.clone(),
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
        };
        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("CowUpdate apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("CowUpdate commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| "CowUpdate committed but new snapshot is not visible".to_string())?;
        Ok(CommitOutcome {
            new_snapshot_id,
            written_manifest_paths: manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .clone(),
        })
    }
}

struct CowUpdateTxnAction {
    written: Vec<WrittenFile>,
    sidecar: MutationSidecar,
    commit_uuid: Uuid,
    file_io: FileIO,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
}
```

Before importing those helpers, change `enumerate_live_data_files` and
`write_overwrite_deletes_manifest` in `overwrite.rs` from private `async fn` to
`pub(super) async fn`; `write_added_data_manifest` is already `pub(super)`.

- [ ] **Step 3: Add sidecar writer**

In `update_cow.rs`:

```rust
pub async fn write_mutation_sidecar(
    file_io: &FileIO,
    path: &str,
    sidecar: &MutationSidecar,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(sidecar)
        .map_err(|e| format!("serialize mutation sidecar failed: {e}"))?;
    file_io
        .new_output(path)
        .map_err(|e| format!("create mutation sidecar output failed: {e}"))?
        .write(bytes.into())
        .await
        .map_err(|e| format!("write mutation sidecar failed: {e}"))?;
    Ok(())
}
```

- [ ] **Step 4: Implement transaction action**

Add to `update_cow.rs`:

```rust
#[async_trait]
impl TransactionAction for CowUpdateTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        if m.format_version() != FormatVersion::V3 {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "CowUpdateCommit requires an Iceberg v3 table",
            ));
        }
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let parent_snapshot_id = m.current_snapshot().map(|s| s.snapshot_id());
        let metadata_dir = metadata_dir(table);

        let existing = enumerate_live_data_files(table, &self.file_io)
            .await
            .map_err(to_iceberg_unexpected)?;
        let touched = self
            .sidecar
            .touched_data_files
            .iter()
            .map(|f| f.old_file.as_str())
            .collect::<std::collections::HashSet<_>>();
        let existing_touched = existing
            .into_iter()
            .filter(|(df, _, _)| touched.contains(df.file_path()))
            .collect::<Vec<_>>();
        if existing_touched.len() != touched.len() {
            return Err(to_iceberg_unexpected(format!(
                "COW UPDATE touched {} data file(s), but only {} are live in the current snapshot",
                touched.len(),
                existing_touched.len()
            )));
        }

        let sidecar_path = format!("{metadata_dir}/{}-update-sidecar.json", self.commit_uuid);
        self.abort_handle.record_manifest(sidecar_path.clone());
        write_mutation_sidecar(&self.file_io, &sidecar_path, &self.sidecar)
            .await
            .map_err(to_iceberg_unexpected)?;

        let mut manifests: Vec<ManifestFile> = Vec::new();
        let delete_manifest_path = format!("{metadata_dir}/{}-cow-update-deletes-0.avro", self.commit_uuid);
        self.abort_handle.record_manifest(delete_manifest_path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(delete_manifest_path.clone());
        manifests.push(
            write_overwrite_deletes_manifest(
                &self.file_io,
                &delete_manifest_path,
                &existing_touched,
                m.default_partition_spec().clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                m.format_version(),
            )
            .await
            .map_err(to_iceberg_unexpected)?,
        );

        let data_manifest_path = format!("{metadata_dir}/{}-cow-update-data-0.avro", self.commit_uuid);
        self.abort_handle.record_manifest(data_manifest_path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(data_manifest_path.clone());
        manifests.push(
            write_added_data_manifest(
                &self.file_io,
                &data_manifest_path,
                &self.written,
                m.default_partition_spec().clone(),
                m.current_schema().clone(),
                new_seq,
                new_snapshot_id,
                m.format_version(),
            )
            .await
            .map_err(to_iceberg_unexpected)?,
        );

        let manifest_list_path = format!("{metadata_dir}/snap-{}-{}.avro", new_snapshot_id, self.commit_uuid);
        self.abort_handle.record_manifest(manifest_list_path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(manifest_list_path.clone());
        write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            m.format_version(),
            Some(m.next_row_id()),
        )
        .await
        .map_err(to_iceberg_unexpected)?;

        let mut props = std::collections::HashMap::new();
        props.insert(NOVAROCKS_ROW_LEVEL_OP.to_string(), NOVAROCKS_ROW_LEVEL_OP_UPDATE.to_string());
        props.insert(NOVAROCKS_UPDATE_MODE.to_string(), NOVAROCKS_UPDATE_MODE_COW.to_string());
        props.insert(NOVAROCKS_UPDATE_SIDECAR.to_string(), sidecar_path);
        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: props,
            })
            .with_schema_id(m.current_schema_id())
            .with_row_range(m.next_row_id(), 0)
            .build();

        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference {
                        snapshot_id: new_snapshot_id,
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                },
            ],
            vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: m.current_schema_id(),
                },
                TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: m.default_partition_spec_id(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: parent_snapshot_id,
                },
            ],
        ))
    }
}

fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}
```

- [ ] **Step 5: Add sidecar serialization test**

Add:

```rust
#[test]
fn mutation_sidecar_round_trips_json() {
    let sidecar = MutationSidecar {
        version: 1,
        operation: "update".to_string(),
        mode: "copy-on-write".to_string(),
        base_snapshot_id: 7,
        target_table_uuid: "uuid-1".to_string(),
        updated_row_ids: vec![10, 11],
        touched_data_files: vec![super::types::MutationSidecarFile {
            old_file: "file-a.parquet".to_string(),
            new_files: vec!["file-b.parquet".to_string()],
            row_ids: vec![10, 11],
        }],
    };
    let json = serde_json::to_string(&sidecar).expect("json");
    let decoded: MutationSidecar = serde_json::from_str(&json).expect("decode");
    assert_eq!(decoded, sidecar);
}
```

Place this test in `types.rs` if importing `MutationSidecarFile` from `update_cow.rs` is awkward.

- [ ] **Step 6: Run COW commit compile tests**

Run:

```bash
cargo test --lib mutation_sidecar -- --nocapture
cargo check --lib
```

Expected: sidecar test passes and library compiles.

- [ ] **Step 7: Commit Task 5**

```bash
git add src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/mod.rs src/connector/iceberg/commit/update_cow.rs src/engine/mutation_flow.rs
git commit -m "feat(iceberg): add cow update commit"
```

---

### Task 6: Wire COW UPDATE Execution

**Files:**
- Modify: `src/engine/mutation_flow.rs`
- Test: integration tests in `src/engine/mod.rs`

- [ ] **Step 1: Add COW update integration test**

In `src/engine/mod.rs` tests, add:

```rust
#[test]
fn iceberg_v3_cow_update_preserves_row_id() {
    let warehouse = TempDir::new().expect("warehouse");
    let (_engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
    session
        .execute_in_database(
            "insert into ice.db1.t values (1, 'a'), (2, 'b')",
            "default",
        )
        .expect("insert");
    let before = collect_id_rowid_seq(
        &session,
        "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
    );
    session
        .execute_in_database(
            "update ice.db1.t as t set v = 'bb' where t.id = 2",
            "default",
        )
        .expect("update");
    let after = collect_id_rowid_seq(
        &session,
        "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
    );
    assert_eq!(before[0].1, after[0].1);
    assert_eq!(before[1].1, after[1].1);
    assert_ne!(before[1].2, after[1].2, "updated row sequence should advance");
}
```

Use the existing `collect_id_rowid_seq` helper near the current row-lineage tests.

- [ ] **Step 2: Implement execution path for no-source UPDATE**

In `execute_update_statement`, after loading the Iceberg table and selecting COW mode, build and run a match query. Task 6 supports `WHERE`-only target updates; Task 8 adds `UPDATE ... FROM` source joins.

```rust
let mode = crate::connector::iceberg::commit::select_iceberg_update_mode(&table)?;
match mode {
    IcebergUpdateMode::CopyOnWrite => execute_cow_update(
        state,
        &target,
        &table,
        stmt,
        current_database,
    ),
    IcebergUpdateMode::MergeOnRead => execute_mor_update(
        state,
        &target,
        &table,
        stmt,
        current_database,
    ),
}
```

Add this concrete stage gate for `execute_mor_update` during Task 6:

```rust
fn execute_mor_update(
    _state: &Arc<StandaloneState>,
    _target: &crate::engine::backend_resolver::TargetBackend,
    _table: &iceberg::table::Table,
    _stmt: &UpdateStmt,
    _current_database: &str,
) -> Result<StatementResult, String> {
    Err("merge-on-read UPDATE is implemented in the next stage".to_string())
}
```

The message is a concrete stage gate, not a user-facing fallback.

- [ ] **Step 3: Materialize matched rows**

Implement a helper that runs the generated SELECT and extracts identity columns:

```rust
struct MatchedUpdateBatch {
    row_ids: Vec<i64>,
    file_paths: Vec<String>,
    row_positions: Vec<i64>,
    old_rows: arrow::record_batch::RecordBatch,
    new_rows: arrow::record_batch::RecordBatch,
}

fn execute_update_match_query(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_database: &str,
) -> Result<MatchedUpdateBatch, String> {
    let statement = crate::sql::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("internal UPDATE match query was not a SELECT".to_string());
    };
    let result = {
        let catalog = state.catalog.read().expect("standalone catalog read lock");
        crate::engine::execute_query(&query, &catalog, current_database, state.exchange_port, None)?
    };
    matched_update_batch_from_query_result(result)
}
```

`matched_update_batch_from_query_result` must:

- read `__nr_file` as `StringArray`
- read `__nr_pos` as `Int64Array`
- read `__nr_row_id` as `Int64Array`
- collect `__nr_new_<column>` columns into the new row batch
- return an empty batch when the query returns no rows

- [ ] **Step 4: Write COW rewritten files**

For the first working COW implementation, rewrite all touched files by reading live rows and replacing matched row ids. Add helper signatures:

```rust
async fn write_cow_update_files(
    table: &iceberg::table::Table,
    matched: &MatchedUpdateBatch,
) -> Result<(Vec<iceberg::spec::DataFile>, MutationSidecar), String> {
    if matched.row_ids.is_empty() {
        return Ok((Vec::new(), empty_sidecar(table)?));
    }
    validate_unique_target_row_ids(&matched.row_ids)?;
    let rewritten_batches = build_cow_rewrite_batches(table, matched).await?;
    let data_files =
        crate::connector::iceberg::data_writer::write_row_lineage_batches_as_data_files(
            table,
            &rewritten_batches,
        )
        .await?;
    let sidecar = build_cow_sidecar(table, matched, &data_files)?;
    Ok((data_files, sidecar))
}
```

`build_cow_rewrite_batches` must apply existing deletes/DVs. Reuse the current delete visibility helpers from `delete_flow.rs` or move shared read-visible-row logic into a connector helper so both DELETE and UPDATE use the same semantics.

- [ ] **Step 5: Commit through `CowUpdateCommit`**

Add `CommitOpKind::CowUpdate` in `types.rs`, route it to `CowUpdateCommit` in
`commit/run.rs`, inject written files into `IcebergCommitCollector`, and call
`run_iceberg_commit`.

```rust
let collector = Arc::new(IcebergCommitCollector::new(
    CommitOpKind::CowUpdate,
    table_ident,
    table.metadata().current_snapshot().map(|s| s.snapshot_id()),
    table.metadata().last_sequence_number(),
    table.metadata().current_schema().clone(),
    table.metadata().default_partition_spec().clone(),
    staging_dir.clone(),
    crate::common::types::UniqueId { hi: 0, lo: 0 },
));
for df in data_files {
    collector.inject_written_file(crate::engine::iceberg_writer::data_file_to_written_file(
        &df,
        table.metadata().default_partition_spec_id(),
    )?);
}
```

- [ ] **Step 6: Run COW integration test**

Run:

```bash
cargo test --lib iceberg_v3_cow_update_preserves_row_id -- --nocapture
```

Expected before full implementation: fails at UPDATE execution.
Expected after implementation: passes and shows stable row ids.

- [ ] **Step 7: Commit Task 6**

```bash
git add src/engine/mutation_flow.rs src/engine/mod.rs src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/run.rs
git commit -m "feat(iceberg): execute cow update"
```

---

### Task 7: Implement Merge-On-Read UPDATE

**Files:**
- Modify: `src/connector/iceberg/commit/row_delta_dv.rs`
- Modify: `src/connector/iceberg/commit/types.rs`
- Modify: `src/engine/mutation_flow.rs`
- Test: integration tests in `src/engine/mod.rs`

- [ ] **Step 1: Add MOR update integration test**

Add:

```rust
#[test]
fn iceberg_v3_mor_update_preserves_row_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = dir.path().join("wh");
    let engine = StandaloneEngine::new_for_test().expect("engine");
    engine
        .execute_in_database(
            &format!(
                r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
                warehouse.display()
            ),
            "default",
        )
        .expect("catalog");
    engine.execute_in_database("create database ice.ns", "default").expect("db");
    engine
        .execute_in_database(
            r#"create table ice.ns.t (id bigint, v string)
               tblproperties("format-version"="3","write.row-lineage"="true","write.update.mode"="merge-on-read")"#,
            "default",
        )
        .expect("table");
    engine
        .execute_in_database(
            r#"insert into ice.ns.t select 1, 'a' union all select 2, 'b'"#,
            "default",
        )
        .expect("insert");
    let before = collect_row_id_tuples(
        &engine,
        "select id, _row_id, _last_updated_sequence_number from ice.ns.t order by id",
    );
    engine
        .execute_in_database(
            r#"update ice.ns.t as t set v = 'aa' where t.id = 1"#,
            "default",
        )
        .expect("update");
    let after = collect_row_id_tuples(
        &engine,
        "select id, _row_id, _last_updated_sequence_number from ice.ns.t order by id",
    );
    assert_eq!(before[0].1, after[0].1);
    assert_ne!(before[0].2, after[0].2);
}
```

- [ ] **Step 2: Extend RowDeltaDvCommit to read update data files**

`IcebergCommitCollector` already carries written files and delete groups independently. In
`RowDeltaDvCommit::commit`, keep the existing delete group drain and also drain written data files:

```rust
let groups = ctx.collector.take_delete_groups();
let written = ctx.collector.take_written_files()?;
```

Update the no-op check:

```rust
if groups.iter().all(|g| g.positions.is_empty()) && written.is_empty() {
    return Ok(CommitOutcome {
        new_snapshot_id: ctx
            .table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or_default(),
        written_manifest_paths: Vec::new(),
    });
}
```

- [ ] **Step 3: Write added data manifest in `RowDeltaDvCommit`**

In `RowDeltaDvTxnAction`, add:

```rust
written: Vec<WrittenFile>,
```

After DV manifests are added, write data manifest when `written` is non-empty:

```rust
if !self.written.is_empty() {
    let data_path = format!(
        "{metadata_dir}/{}-row-delta-update-data-0.avro",
        self.commit_uuid
    );
    self.abort_handle.record_manifest(data_path.clone());
    self.manifest_paths_out
        .lock()
        .expect("manifest_paths_out poisoned")
        .push(data_path.clone());
    let data_manifest = super::overwrite::write_added_data_manifest(
        &self.file_io,
        &data_path,
        &self.written,
        table.metadata().default_partition_spec().clone(),
        self.schema.clone(),
        new_seq,
        new_snapshot_id,
        format_version,
    )
    .await
    .map_err(to_iceberg_unexpected)?;
    new_manifests.push(data_manifest);
}
```

- [ ] **Step 4: Add MOR update markers**

In the `Summary` for update mode, include:

```rust
let mut summary_props = dv_summary(&written_dvs);
if !self.written.is_empty() {
    summary_props.insert(
        super::types::NOVAROCKS_ROW_LEVEL_OP.to_string(),
        super::types::NOVAROCKS_ROW_LEVEL_OP_UPDATE.to_string(),
    );
    summary_props.insert(
        super::types::NOVAROCKS_UPDATE_MODE.to_string(),
        super::types::NOVAROCKS_UPDATE_MODE_MOR.to_string(),
    );
}
```

Use this in `Summary { operation: Operation::Delete, additional_properties: summary_props }`.

- [ ] **Step 5: Wire MOR execution in `mutation_flow`**

Implement `execute_mor_update` using the matched batch:

```rust
fn execute_mor_update(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    table: &iceberg::table::Table,
    stmt: &UpdateStmt,
    current_database: &str,
) -> Result<StatementResult, String> {
    let matched = materialize_update_matches(state, target, stmt, current_database)?;
    if matched.row_ids.is_empty() {
        return Ok(StatementResult::Ok);
    }
    validate_unique_target_row_ids(&matched.row_ids)?;
    let delete_groups = build_position_delete_groups(&matched)?;
    let update_batches = build_mor_update_batches(&matched)?;
    let data_files = crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        crate::connector::iceberg::data_writer::write_row_lineage_batches_as_data_files(
            table,
            &update_batches,
        )
        .await
    })
    .and_then(|inner| inner)?;
    commit_mor_update(state, target, table, delete_groups, data_files)
}
```

`build_position_delete_groups` groups `(file_path, row_pos)` by file and produces the same `PositionDeleteGroup` type used by existing DELETE.

- [ ] **Step 6: Run MOR test**

Run:

```bash
cargo test --lib iceberg_v3_mor_update_preserves_row_id -- --nocapture
```

Expected: MOR update test passes and SELECT sees the updated row once.

- [ ] **Step 7: Commit Task 7**

```bash
git add src/connector/iceberg/commit/row_delta_dv.rs src/connector/iceberg/commit/types.rs src/engine/mutation_flow.rs src/engine/mod.rs
git commit -m "feat(iceberg): execute mor update"
```

---

### Task 8: Support UPDATE ... FROM Source Joins

**Files:**
- Modify: `src/engine/mutation_flow.rs`
- Test: integration tests in `src/engine/mod.rs`

- [ ] **Step 1: Add UPDATE FROM source test**

Add:

```rust
#[test]
fn iceberg_v3_update_from_source_table() {
    let warehouse = TempDir::new().expect("warehouse");
    let (_engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
    session
        .execute_in_database(
            r#"create table src (id int, new_v string) duplicate key(id) distributed by hash(id) buckets 1"#,
            "default",
        )
        .expect("source");
    session
        .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
        .expect("insert target");
    session
        .execute_in_database("insert into src values (2, 'bb')", "default")
        .expect("insert source");
    session
        .execute_in_database(
            "update ice.db1.t as t set v = s.new_v from src as s where t.id = s.id",
            "default",
        )
        .expect("update");
    let rows = collect_id_v(&session, "select id, v from ice.db1.t order by id");
    assert_eq!(rows, vec![(1, "a".to_string()), (2, "bb".to_string())]);
}
```

- [ ] **Step 2: Render source relation SQL**

In `mutation_flow.rs`, add:

```rust
fn mutation_source_to_sql(source: &Option<crate::sql::parser::ast::MutationSource>) -> Result<Option<String>, String> {
    use crate::sql::parser::ast::MutationSource;
    match source {
        None => Ok(None),
        Some(MutationSource::Table { name, alias }) => {
            let mut sql = name.0.join(".");
            if let Some(alias) = alias {
                sql.push_str(" AS ");
                sql.push_str(alias);
            }
            Ok(Some(sql))
        }
        Some(MutationSource::Query { query, alias }) => {
            let alias = alias
                .as_deref()
                .ok_or_else(|| "UPDATE subquery source requires an alias".to_string())?;
            Ok(Some(format!("({query}) AS {alias}")))
        }
    }
}
```

- [ ] **Step 3: Use source SQL in match query**

Update `materialize_update_matches` so it passes the source SQL into `build_update_match_query_sql`. For target alias:

```rust
let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
let target_sql = target_table_sql(target, target_alias);
```

Add this local helper:

```rust
fn target_table_sql(target: &crate::engine::backend_resolver::TargetBackend, alias: &str) -> String {
    format!("{}.{}.{} AS {}", target.catalog, target.namespace, target.table, alias)
}
```

- [ ] **Step 4: Add duplicate source match test**

Add:

```rust
#[test]
fn iceberg_v3_update_from_rejects_duplicate_source_match() {
    let warehouse = TempDir::new().expect("warehouse");
    let (_engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
    session
        .execute_in_database(
            r#"create table src (id int, new_v string) duplicate key(id) distributed by hash(id) buckets 1"#,
            "default",
        )
        .expect("source");
    session
        .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
        .expect("insert target");
    session
        .execute_in_database("insert into src values (1, 'x'), (1, 'y')", "default")
        .expect("insert source");
    let err = session
        .execute_in_database(
            "update ice.db1.t as t set v = s.new_v from src as s where t.id = s.id",
            "default",
        )
        .expect_err("duplicate source should fail");
    assert!(err.contains("more than once"), "{err}");
}
```
        "default",
    )
    .expect_err("duplicate source should fail");
assert!(err.contains("more than once"), "{err}");
```

- [ ] **Step 5: Run UPDATE FROM tests**

Run:

```bash
cargo test --lib iceberg_v3_update_from -- --nocapture
```

Expected: source-driven update passes; duplicate source match fails before commit.

- [ ] **Step 6: Commit Task 8**

```bash
git add src/engine/mutation_flow.rs src/engine/mod.rs
git commit -m "feat(iceberg): support update from source"
```

---

### Task 9: Add MV Change Planner Support For Update Markers

**Files:**
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh_strategy.rs`
- Modify: `src/connector/starrocks/managed/ivm_change_stream.rs`
- Test: unit tests in `src/connector/iceberg/changes.rs` and `src/connector/starrocks/managed/mv_refresh_strategy.rs`

- [ ] **Step 1: Add planner classification tests**

In `src/connector/iceberg/changes.rs`, add tests:

```rust
#[test]
fn classify_marked_cow_update_overwrite_as_update() {
    let s = snap(
        7,
        Some(6),
        Operation::Overwrite,
        &[
            ("novarocks.row-level-op", "update"),
            ("novarocks.update.mode", "copy-on-write"),
            ("novarocks.update.sidecar", "file:///tmp/sidecar.json"),
        ],
        0,
    );
    assert_eq!(
        classify_snapshot(&s, None).expect("classify"),
        Some(LineageAction::CollectCowUpdate { snapshot_id: 7 })
    );
}

#[test]
fn ordinary_overwrite_still_maps_to_full_refresh_signal() {
    let s = snap(7, Some(6), Operation::Overwrite, &[], 0);
    let err = classify_snapshot(&s, None).expect_err("ordinary overwrite");
    assert!(matches!(err, ChangeError::UnsupportedOperation { op, .. } if op == "overwrite"));
}
```

- [ ] **Step 2: Extend `LineageAction`**

Add:

```rust
CollectCowUpdate { snapshot_id: i64 },
CollectMorUpdate { snapshot_id: i64 },
```

Update action matching to include these variants wherever snapshot id is extracted.

- [ ] **Step 3: Add marker classifier**

Add:

```rust
fn update_marker_mode(snapshot: &iceberg::spec::Snapshot) -> Option<&str> {
    let props = &snapshot.summary().additional_properties;
    if props.get(super::commit::types::NOVAROCKS_ROW_LEVEL_OP).map(String::as_str)
        != Some(super::commit::types::NOVAROCKS_ROW_LEVEL_OP_UPDATE)
    {
        return None;
    }
    props
        .get(super::commit::types::NOVAROCKS_UPDATE_MODE)
        .map(String::as_str)
}
```

Adjust module paths based on existing visibility; if `changes.rs` cannot import through `super::commit`, import from `crate::connector::iceberg::commit`.

Update `classify_snapshot`:

```rust
Operation::Overwrite => match update_marker_mode(snapshot) {
    Some(super::commit::types::NOVAROCKS_UPDATE_MODE_COW) => {
        Ok(Some(LineageAction::CollectCowUpdate { snapshot_id }))
    }
    _ => Err(ChangeError::UnsupportedOperation {
        snapshot_id,
        op: "overwrite".to_string(),
    }),
},
Operation::Delete => match update_marker_mode(snapshot) {
    Some(super::commit::types::NOVAROCKS_UPDATE_MODE_MOR) => {
        Ok(Some(LineageAction::CollectMorUpdate { snapshot_id }))
    }
    _ => Ok(Some(LineageAction::CollectDeletes { snapshot_id })),
},
```

- [ ] **Step 4: Extend change batch model**

Add to `IcebergChangeBatch`:

```rust
pub cow_updates: Vec<CowUpdateRef>,
pub mor_updates: Vec<MorUpdateRef>,
```

Define:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CowUpdateRef {
    pub snapshot_id: i64,
    pub sidecar_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MorUpdateRef {
    pub snapshot_id: i64,
}
```

All existing constructors in tests must initialize these with `Vec::new()`.

- [ ] **Step 5: Collect update refs**

In `collect_files`, for COW action:

```rust
LineageAction::CollectCowUpdate { .. } => {
    let props = &snapshot.summary().additional_properties;
    let sidecar_path = props
        .get(crate::connector::iceberg::commit::NOVAROCKS_UPDATE_SIDECAR)
        .ok_or_else(|| ChangeError::InternalInconsistency(format!(
            "COW update snapshot {snapshot_id} missing novarocks.update.sidecar"
        )))?
        .clone();
    cow_updates.push(CowUpdateRef { snapshot_id, sidecar_path });
}
```

For MOR action, collect files like both `CollectInserts` and `CollectDeletes` for the same snapshot. Extract the current `CollectInserts` manifest walk into `collect_added_data_files_for_manifest_list` and the current `CollectDeletes` manifest walk into `collect_added_delete_files_for_manifest_list`, both taking `snapshot_id`, `file_io`, `manifest_list`, and mutable output vectors.

```rust
LineageAction::CollectMorUpdate { .. } => {
    collect_added_data_files_for_manifest_list(
        snapshot_id,
        file_io,
        &manifest_list,
        &mut inserts,
    )
    .await?;
    collect_added_delete_files_for_manifest_list(
        snapshot_id,
        file_io,
        &manifest_list,
        &mut deletes,
        &mut equality_deletes,
    )
    .await?;
    mor_updates.push(MorUpdateRef { snapshot_id });
}
```

- [ ] **Step 6: Update refresh strategy tests**

In `src/connector/starrocks/managed/mv_refresh_strategy.rs`, keep the ordinary
overwrite fallback behavior covered with this test:

```rust
#[test]
fn ordinary_overwrite_change_error_still_maps_to_full_refresh() {
    assert_eq!(
        policy_from_change_error(ChangeError::UnsupportedOperation {
            snapshot_id: 22,
            op: "overwrite".to_string(),
        }),
        MvRefreshPolicy::FullRefresh {
            target_snapshot_id: Some(22),
            reason: FullRefreshReason::InsertOverwrite { snapshot_id: 22 },
        }
    );
}
```

The marked COW path is covered in `changes.rs` by
`classify_marked_cow_update_overwrite_as_update`; it does not create a
`ChangeError`, so `policy_from_change_error` is not invoked for that path.

- [ ] **Step 7: Run planner tests**

Run:

```bash
cargo test --lib 'cow_update|mor_update|overwrite_error_maps_to_full_refresh' -- --nocapture
```

Expected: marked update snapshots are incremental; ordinary overwrite remains full refresh.

- [ ] **Step 8: Commit Task 9**

```bash
git add src/connector/iceberg/changes.rs src/connector/starrocks/managed/mv_refresh_strategy.rs src/connector/starrocks/managed/ivm_change_stream.rs
git commit -m "feat(mv): classify iceberg update snapshots"
```

---

### Task 10: Materialize COW And MOR UPDATE Changes For MV

**Files:**
- Modify: `src/connector/iceberg/changes.rs`
- Test: integration tests in `src/connector/starrocks/managed/mv_refresh.rs`

- [ ] **Step 1: Add MV COW update test**

In `src/connector/starrocks/managed/mv_refresh.rs`, add a test based on existing v3 row-lineage MV tests:

```rust
#[test]
fn projection_mv_incremental_refresh_handles_cow_update() {
    // Setup v3 row-lineage Iceberg base table with default write.update.mode.
    // Create projection/filter MV with primary key row identity.
    // Full refresh once.
    // UPDATE base table row id=2.
    // Refresh MV incrementally.
    // Assert MV rows reflect updated values and no duplicate row exists.
}
```

Use concrete SQL:

```sql
CREATE TABLE ice.ns.orders (id BIGINT, status STRING, amount BIGINT)
TBLPROPERTIES ("format-version"="3","write.row-lineage"="true");
INSERT INTO ice.ns.orders SELECT 1, 'open', 10 UNION ALL SELECT 2, 'open', 20;
CREATE MATERIALIZED VIEW mv_orders PRIMARY KEY(id)
AS SELECT id, status, amount FROM ice.ns.orders WHERE status = 'open';
REFRESH MATERIALIZED VIEW mv_orders;
UPDATE ice.ns.orders AS o SET amount = 25 WHERE o.id = 2;
REFRESH MATERIALIZED VIEW mv_orders;
SELECT id, amount FROM mv_orders ORDER BY id;
```

Expected result: `(1, 10), (2, 25)`.

- [ ] **Step 2: Add MV MOR update test**

Use the same test shape, but create base table with:

```sql
TBLPROPERTIES (
  "format-version"="3",
  "write.row-lineage"="true",
  "write.update.mode"="merge-on-read"
)
```

Expected result is the same.

- [ ] **Step 3: Implement sidecar read helper**

In `changes.rs`:

```rust
async fn read_mutation_sidecar(
    file_io: &iceberg::io::FileIO,
    path: &str,
) -> Result<crate::connector::iceberg::commit::MutationSidecar, String> {
    let bytes = file_io
        .new_input(path)
        .map_err(|e| format!("open mutation sidecar input failed: {e}"))?
        .read()
        .await
        .map_err(|e| format!("read mutation sidecar failed: {e}"))?;
    serde_json::from_slice(bytes.as_ref())
        .map_err(|e| format!("parse mutation sidecar failed: {e}"))
}
```

- [ ] **Step 4: Materialize COW update old/new rows**

Add logic in `materialize_changes`:

```rust
if !batch.cow_updates.is_empty() {
    let cow_changes = materialize_cow_updates(
        state,
        current_database,
        sql,
        base_ref,
        base_table,
        &batch.cow_updates,
        object_store_config,
    )?;
    deletes = concat_query_results(deletes, cow_changes.deletes)?;
    inserts = concat_query_results(inserts, cow_changes.inserts)?;
}
```

`materialize_cow_updates` must:

1. Read sidecar.
2. Read old files listed by `old_file` and filter rows by `row_ids`.
3. Read new files listed by `new_files` and filter rows by same `row_ids`.
4. Execute MV SELECT against old rows via `execute_query_for_mv_incremental_deletes`.
5. Execute MV SELECT against new rows via the existing added-file path or a new in-memory row override path.

- [ ] **Step 5: Materialize MOR update rows**

MOR update snapshots already produce added data files and DVs. Ensure `CollectMorUpdate` adds those to the normal `inserts` and `deletes` vectors. No separate materializer is needed beyond the marker classification.

- [ ] **Step 6: Add query-result concat helper**

`QueryResult` currently has `row_count`, `into_chunks`, and `empty`; add this concat helper near
the MV change materialization code:

```rust
fn concat_query_results(
    mut left: crate::engine::QueryResult,
    right: crate::engine::QueryResult,
) -> Result<crate::engine::QueryResult, String> {
    if left.columns.is_empty() {
        return Ok(right);
    }
    if right.columns.is_empty() {
        return Ok(left);
    }
    if left.columns != right.columns {
        return Err("cannot concatenate query results with different schemas".to_string());
    }
    left.chunks.extend(right.chunks);
    Ok(left)
}
```

- [ ] **Step 7: Run MV tests**

Run:

```bash
cargo test --lib 'projection_mv_incremental_refresh_handles_cow_update|projection_mv_incremental_refresh_handles_mor_update' -- --nocapture
```

Expected: both MV tests pass.

- [ ] **Step 8: Commit Task 10**

```bash
git add src/connector/iceberg/changes.rs src/connector/starrocks/managed/mv_refresh.rs
git commit -m "feat(mv): refresh incrementally after iceberg updates"
```

---

### Task 11: Add SQL Regression Coverage

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_v3_update_cow.sql`
- Create: `sql-tests/iceberg/result/iceberg_v3_update_cow.result`
- Create: `sql-tests/iceberg/sql/iceberg_v3_update_mor.sql`
- Create: `sql-tests/iceberg/result/iceberg_v3_update_mor.result`
- Create: `sql-tests/mv-on-iceberg/sql/managed_lake_mv_update_cow.sql`
- Create: `sql-tests/mv-on-iceberg/result/managed_lake_mv_update_cow.result`
- Create: `sql-tests/mv-on-iceberg/sql/managed_lake_mv_update_mor.sql`
- Create: `sql-tests/mv-on-iceberg/result/managed_lake_mv_update_mor.result`

- [ ] **Step 1: Add COW SQL case**

Create `sql-tests/iceberg/sql/iceberg_v3_update_cow.sql`:

```sql
-- name: iceberg_v3_update_cow
DROP TABLE IF EXISTS ice.ns.update_cow;
CREATE TABLE ice.ns.update_cow (
  id BIGINT,
  v STRING
) TBLPROPERTIES ("format-version"="3","write.row-lineage"="true");
INSERT INTO ice.ns.update_cow SELECT 1, 'a' UNION ALL SELECT 2, 'b';
SELECT id, v FROM ice.ns.update_cow ORDER BY id;
UPDATE ice.ns.update_cow AS t SET v = 'bb' WHERE t.id = 2;
SELECT id, v FROM ice.ns.update_cow ORDER BY id;
SELECT COUNT(DISTINCT _row_id), COUNT(*) FROM ice.ns.update_cow;
```

Create expected result:

```text
1	a
2	b
1	a
2	bb
2	2
```

Adjust result formatting to match existing `sql-tests/iceberg/result/*.result` conventions.

- [ ] **Step 2: Add MOR SQL case**

Create `sql-tests/iceberg/sql/iceberg_v3_update_mor.sql` with the same SQL but table properties:

```sql
TBLPROPERTIES (
  "format-version"="3",
  "write.row-lineage"="true",
  "write.update.mode"="merge-on-read"
)
```

Expected result is the same as COW.

- [ ] **Step 3: Add MV COW SQL case**

Create `sql-tests/mv-on-iceberg/sql/managed_lake_mv_update_cow.sql`:

```sql
-- name: managed_lake_mv_update_cow
DROP MATERIALIZED VIEW IF EXISTS mv_update_cow;
DROP TABLE IF EXISTS ice.ns.mv_update_cow_base;
CREATE TABLE ice.ns.mv_update_cow_base (
  id BIGINT,
  status STRING,
  amount BIGINT
) TBLPROPERTIES ("format-version"="3","write.row-lineage"="true");
INSERT INTO ice.ns.mv_update_cow_base SELECT 1, 'open', 10 UNION ALL SELECT 2, 'open', 20;
CREATE MATERIALIZED VIEW mv_update_cow
PRIMARY KEY(id)
AS SELECT id, amount FROM ice.ns.mv_update_cow_base WHERE status = 'open';
REFRESH MATERIALIZED VIEW mv_update_cow;
SELECT id, amount FROM mv_update_cow ORDER BY id;
UPDATE ice.ns.mv_update_cow_base AS t SET amount = 25 WHERE t.id = 2;
REFRESH MATERIALIZED VIEW mv_update_cow;
SELECT id, amount FROM mv_update_cow ORDER BY id;
```

Expected result:

```text
1	10
2	20
1	10
2	25
```

- [ ] **Step 4: Add MV MOR SQL case**

Create `sql-tests/mv-on-iceberg/sql/managed_lake_mv_update_mor.sql` with the same SQL and `write.update.mode=merge-on-read`. Expected result is the same.

- [ ] **Step 5: Run focused SQL tests**

Start standalone with a private config and port. Do not use port `9030`.

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config tests/sql-test-runner/conf/standalone_managed_lake.conf --port 19036
```

In a second terminal:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --port 19036 \
  --only iceberg_v3_update_cow,iceberg_v3_update_mor \
  --mode verify \
  --query-timeout 120

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite mv-on-iceberg \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --port 19036 \
  --only managed_lake_mv_update_cow,managed_lake_mv_update_mor \
  --mode verify \
  --query-timeout 120
```

Expected: all four SQL cases pass.

- [ ] **Step 6: Commit Task 11**

```bash
git add sql-tests/iceberg/sql/iceberg_v3_update_cow.sql \
        sql-tests/iceberg/result/iceberg_v3_update_cow.result \
        sql-tests/iceberg/sql/iceberg_v3_update_mor.sql \
        sql-tests/iceberg/result/iceberg_v3_update_mor.result \
        sql-tests/mv-on-iceberg/sql/managed_lake_mv_update_cow.sql \
        sql-tests/mv-on-iceberg/result/managed_lake_mv_update_cow.result \
        sql-tests/mv-on-iceberg/sql/managed_lake_mv_update_mor.sql \
        sql-tests/mv-on-iceberg/result/managed_lake_mv_update_mor.result
git commit -m "test(sql): cover iceberg update on mv refresh"
```

---

### Task 12: Final Verification

**Files:**
- No source changes unless verification exposes a defect.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt --check
```

Expected: command exits 0.

- [ ] **Step 2: Library tests**

Run:

```bash
cargo test --lib
```

Expected: all library tests pass.

- [ ] **Step 3: Focused SQL suites**

Run the SQL commands from Task 11 Step 5 against private port `19036`.

Expected: all new UPDATE and MV UPDATE SQL cases pass.

- [ ] **Step 4: Inspect git diff**

Run:

```bash
git diff --check
git status --short
```

Expected:

- `git diff --check` exits 0.
- `git status --short` shows only intended source/test files before final commit, or clean after all task commits.

- [ ] **Step 5: Summarize implementation**

Prepare a concise summary with:

```text
- UPDATE ... FROM supported for Iceberg v3 row-lineage tables.
- Default COW update preserves _row_id and writes marked overwrite snapshots with sidecar.
- MOR update writes Puffin DV plus updated data files in one snapshot.
- MV incremental refresh handles both COW and MOR update markers.
- Validation: cargo fmt --check, cargo test --lib, focused sql-tests on private port 19036.
```

---

### Task 13: MERGE INTO Follow-Up Plan Gate

**Files:**
- No code changes in this task.

- [ ] **Step 1: Confirm UPDATE stages are complete**

Run:

```bash
git log --oneline -n 8
cargo test --lib 'iceberg_v3_.*update|projection_mv_incremental_refresh_handles_.*update' -- --nocapture
```

Expected: UPDATE COW, UPDATE MOR, and MV update commits are present; focused tests pass.

- [ ] **Step 2: Open a new plan for MERGE SQL**

Create a separate plan file:

```text
docs/design/plans/2026-05-04-iceberg-v3-merge-into.md
```

The plan must map:

- `WHEN MATCHED UPDATE` to `MutationAction::Update`
- `WHEN MATCHED DELETE` to existing DELETE/DV row action
- `WHEN NOT MATCHED INSERT` to append
- duplicate target `_row_id` to fail fast

Do not implement MERGE in the UPDATE branch before this separate plan is reviewed.
