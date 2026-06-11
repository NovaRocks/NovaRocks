# Iceberg Schema Evolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add first-slice Iceberg schema evolution support in NovaRocks standalone: StarRocks-style top-level column DDL plus field-id-correct queries over evolved local-FS and S3 Iceberg tables.

**Architecture:** Add a narrow parser/statement boundary for supported `ALTER TABLE` schema DDL, commit schema changes through an Iceberg metadata-only transaction action, and refresh Iceberg metadata before every query plan. Carry Iceberg schema field IDs in `TableDef` so standalone codegen emits `TIcebergTable.iceberg_schema`, letting the existing Parquet field-id alignment path read evolved files correctly.

**Tech Stack:** Rust, sqlparser, vendored iceberg-rust 0.9 transaction actions, Arrow `RecordBatch`, Parquet field IDs, NovaRocks `sql-tests`, MinIO/S3-compatible object store.

---

## Scope Check

The approved design is one cohesive slice:

- DDL parser and execution for top-level Iceberg schema changes.
- Iceberg metadata commit and cache invalidation.
- Query metadata refresh and field-id descriptor propagation.
- Local-FS and MinIO/S3 SQL coverage.

It does not need decomposition into separate specs. The tasks below are ordered so each produces independently testable progress.

## File Structure

- Create: `src/connector/iceberg/catalog/schema_update.rs`
  - Owns schema-change validation, field-id-preserving schema construction, and the metadata-only Iceberg transaction action.
- Modify: `src/connector/iceberg/catalog/mod.rs`
  - Exports the new schema-update entry point.
- Modify: `src/connector/iceberg/catalog/registry.rs`
  - Reuses type conversion helpers for schema updates and exposes a cache-bypassing load helper.
- Modify: `src/engine/statement.rs`
  - Parses the supported `ALTER TABLE ... ADD/DROP/RENAME/MODIFY COLUMN` forms.
- Modify: `src/engine/mod.rs`
  - Dispatches parsed schema DDL before generic SQL parsing and invalidates local caches after success.
- Modify: `src/engine/query_prep.rs`
  - Force-refreshes Iceberg table metadata on every query registration.
- Modify: `src/sql/catalog.rs`
  - Adds optional Iceberg table metadata to standalone `TableDef`.
- Modify: `src/connector/iceberg/catalog/backend.rs`
  - Populates Iceberg schema metadata when building `TableDef`.
- Modify: `src/sql/codegen/descriptors.rs`
  - Adds helpers to emit Iceberg `TTableDescriptor` with `TIcebergTable.iceberg_schema`.
- Modify: `src/sql/codegen/fragment_builder.rs`
  - Uses the new descriptor helper for Iceberg scan tables.
- Add: `tests/sql-test-runner/conf/standalone_iceberg_local.conf`
  - Points the Iceberg SQL runner at a local Hadoop-style warehouse without requiring MinIO.
- Add: `sql-tests/iceberg/sql/iceberg_schema_evolution_local.sql`
  - Local-FS/Hadoop catalog SQL coverage.
- Add: `sql-tests/iceberg/result/iceberg_schema_evolution_local.result`
  - Expected output for local-FS coverage.
- Add: `sql-tests/iceberg/sql/iceberg_schema_evolution_s3.sql`
  - S3-backed catalog SQL coverage.
- Add: `sql-tests/iceberg/result/iceberg_schema_evolution_s3.result`
  - Expected output for S3 coverage.

## Task 1: Parser and Engine Dispatch

**Files:**
- Modify: `src/engine/statement.rs`
- Modify: `src/engine/mod.rs`

- [ ] **Step 1: Write parser tests first**

Add these tests inside `src/engine/statement.rs`'s existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn parse_alter_iceberg_schema_add_column_default_null() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT DEFAULT NULL",
        )
        .expect("parse");

        assert_eq!(stmt.table.parts, vec!["ice", "db", "orders"]);
        assert_eq!(
            stmt.change,
            super::IcebergSchemaChange::AddColumn {
                name: "discount".to_string(),
                data_type: crate::sql::parser::ast::SqlType::Int,
                default_null: true,
            }
        );
    }

    #[test]
    fn parse_alter_iceberg_schema_drop_rename_modify() {
        let drop_stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE ice.db.orders DROP COLUMN old_col")
                .expect("drop");
        assert_eq!(
            drop_stmt.change,
            super::IcebergSchemaChange::DropColumn {
                name: "old_col".to_string(),
            }
        );

        let rename_stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders RENAME COLUMN old_col TO new_col",
        )
        .expect("rename");
        assert_eq!(
            rename_stmt.change,
            super::IcebergSchemaChange::RenameColumn {
                old_name: "old_col".to_string(),
                new_name: "new_col".to_string(),
            }
        );

        let modify_stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE ice.db.orders MODIFY COLUMN id BIGINT")
                .expect("modify");
        assert_eq!(
            modify_stmt.change,
            super::IcebergSchemaChange::ModifyColumn {
                name: "id".to_string(),
                new_type: crate::sql::parser::ast::SqlType::BigInt,
            }
        );
    }

    #[test]
    fn parse_alter_iceberg_schema_rejects_unsupported_add_forms() {
        let not_null = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT NOT NULL",
        )
        .expect_err("not null should fail");
        assert!(not_null.contains("ADD COLUMN NOT NULL is not supported"));

        let non_null_default = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT DEFAULT 1",
        )
        .expect_err("non-null default should fail");
        assert!(non_null_default.contains("default values other than NULL"));
    }
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test --lib parse_alter_iceberg_schema -- --nocapture
```

Expected: compile failure because `parse_alter_iceberg_schema_sql` and `IcebergSchemaChange` do not exist yet.

- [ ] **Step 3: Add the statement model and parser**

In `src/engine/statement.rs`, add these imports near the top if they are not already present:

```rust
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;
```

Add the statement model near `AddEqualityDeleteStmt`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AlterIcebergSchemaStmt {
    pub(crate) table: ObjectName,
    pub(crate) change: IcebergSchemaChange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergSchemaChange {
    AddColumn {
        name: String,
        data_type: SqlType,
        default_null: bool,
    },
    DropColumn {
        name: String,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    ModifyColumn {
        name: String,
        new_type: SqlType,
    },
}
```

Add the probe and parser after the existing `ADD FILES` parser helpers:

```rust
pub(crate) fn looks_like_alter_iceberg_schema(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE")
        && (upper.contains("ADD COLUMN")
            || upper.contains("DROP COLUMN")
            || upper.contains("RENAME COLUMN")
            || upper.contains("MODIFY COLUMN"))
        && !upper.contains(" ADD FILES FROM")
        && !upper.contains(" ADD EQUALITY DELETE")
}

pub(crate) fn parse_alter_iceberg_schema_sql(
    sql: &str,
) -> Result<AlterIcebergSchemaStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let dialect = crate::sql::parser::dialect::StarRocksDialect;
    let mut parser = Parser::new(&dialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE schema DDL: {e}"))?;

    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;
    let table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;

    let change = if parser.parse_keywords(&[Keyword::ADD, Keyword::COLUMN]) {
        parse_add_column_change(&mut parser)?
    } else if parser.parse_keywords(&[Keyword::DROP, Keyword::COLUMN]) {
        let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        IcebergSchemaChange::DropColumn { name }
    } else if parser.parse_keywords(&[Keyword::RENAME, Keyword::COLUMN]) {
        let old_name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        parser.expect_keyword(Keyword::TO).map_err(|e| e.to_string())?;
        let new_name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        IcebergSchemaChange::RenameColumn { old_name, new_name }
    } else if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "MODIFY") {
        parser.next_token();
        parser
            .expect_keyword(Keyword::COLUMN)
            .map_err(|e| e.to_string())?;
        let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        let new_type = crate::sql::parser::dialect::convert_sql_type(
            parser.parse_data_type().map_err(|e| e.to_string())?,
        )?;
        IcebergSchemaChange::ModifyColumn { name, new_type }
    } else {
        return Err("unsupported ALTER TABLE schema evolution clause".to_string());
    };

    if parser.peek_token_ref().token == Token::SemiColon {
        parser.next_token();
    }
    if parser.peek_token_ref().token != Token::EOF {
        return Err(format!(
            "unsupported trailing ALTER TABLE schema tokens starting at {}",
            parser.peek_token_ref().token
        ));
    }

    Ok(AlterIcebergSchemaStmt { table, change })
}

fn parse_add_column_change(parser: &mut Parser<'_>) -> Result<IcebergSchemaChange, String> {
    let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
    let data_type = crate::sql::parser::dialect::convert_sql_type(
        parser.parse_data_type().map_err(|e| e.to_string())?,
    )?;
    let mut default_null = false;
    loop {
        if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            return Err("ADD COLUMN NOT NULL is not supported for Iceberg schema evolution".to_string());
        }
        if parser.parse_keyword(Keyword::NULL) {
            continue;
        }
        if parser.parse_keyword(Keyword::DEFAULT) {
            if parser.parse_keyword(Keyword::NULL) {
                default_null = true;
                continue;
            }
            return Err("ADD COLUMN default values other than NULL are not supported".to_string());
        }
        break;
    }
    Ok(IcebergSchemaChange::AddColumn {
        name,
        data_type,
        default_null,
    })
}
```

- [ ] **Step 4: Dispatch before the ad hoc ALTER handlers**

In `src/engine/mod.rs`, add `looks_like_alter_iceberg_schema` to the `use crate::engine::statement::{...}` list. In `StandaloneEngine::execute_sql_with_options`, place this before the equality-delete and add-files probes:

```rust
        // ALTER TABLE ... ADD/DROP/RENAME/MODIFY COLUMN
        if looks_like_alter_iceberg_schema(&normalized) {
            return self.handle_alter_iceberg_schema(&normalized, current_catalog, current_database);
        }
```

Add the method beside `handle_add_files`:

```rust
    fn handle_alter_iceberg_schema(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let stmt = crate::engine::statement::parse_alter_iceberg_schema_sql(sql)?;
        crate::connector::iceberg::catalog::alter_table_schema(
            &self.inner,
            &stmt,
            current_catalog,
            current_database,
        )?;
        Ok(StatementResult::Ok)
    }
```

- [ ] **Step 5: Run parser tests**

Run:

```bash
cargo test --lib parse_alter_iceberg_schema -- --nocapture
```

Expected: parser tests pass. If dispatch fails to compile because `alter_table_schema` does not exist, keep the dispatch call behind Task 2 by temporarily adding a stub with `Err("Iceberg schema evolution is not implemented".to_string())`; Task 2 replaces it.

- [ ] **Step 6: Commit**

```bash
git add src/engine/statement.rs src/engine/mod.rs
git commit -m "feat: parse iceberg schema evolution ddl"
```

## Task 2: Schema Update Module

**Files:**
- Create: `src/connector/iceberg/catalog/schema_update.rs`
- Modify: `src/connector/iceberg/catalog/mod.rs`
- Modify: `src/connector/iceberg/catalog/registry.rs`

- [ ] **Step 1: Write schema-construction tests**

Create `src/connector/iceberg/catalog/schema_update.rs` with the test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::ast::SqlType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Float)).into(),
            ])
            .build()
            .expect("schema")
    }

    #[test]
    fn add_column_assigns_fresh_field_id() {
        let updated = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::AddColumn {
                name: "new_col".to_string(),
                data_type: SqlType::Int,
                default_null: true,
            },
        )
        .expect("updated");
        let field = updated.field_by_name("new_col").expect("new field");
        assert_eq!(field.id, 3);
    }

    #[test]
    fn rename_and_modify_preserve_field_id() {
        let renamed = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::RenameColumn {
                old_name: "id".to_string(),
                new_name: "order_id".to_string(),
            },
        )
        .expect("renamed");
        assert_eq!(renamed.field_by_name("order_id").expect("renamed").id, 1);

        let modified = apply_change_to_schema_for_test(
            &renamed,
            2,
            &IcebergSchemaChange::ModifyColumn {
                name: "order_id".to_string(),
                new_type: SqlType::BigInt,
            },
        )
        .expect("modified");
        let field = modified.field_by_name("order_id").expect("modified");
        assert_eq!(field.id, 1);
        assert_eq!(field.field_type, Type::Primitive(PrimitiveType::Long));
    }

    #[test]
    fn drop_removes_field_without_reusing_id() {
        let dropped = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::DropColumn {
                name: "v".to_string(),
            },
        )
        .expect("dropped");
        assert!(dropped.field_by_name("v").is_none());

        let added = apply_change_to_schema_for_test(
            &dropped,
            2,
            &IcebergSchemaChange::AddColumn {
                name: "later".to_string(),
                data_type: SqlType::Int,
                default_null: false,
            },
        )
        .expect("added");
        assert_eq!(added.field_by_name("later").expect("later").id, 3);
    }

    #[test]
    fn modify_rejects_unsafe_type_changes() {
        let err = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::ModifyColumn {
                name: "id".to_string(),
                new_type: SqlType::Double,
            },
        )
        .expect_err("unsafe change");
        assert!(err.contains("unsupported Iceberg type evolution"));
    }
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test --lib schema_update -- --nocapture
```

Expected: compile failure because the module and helper functions are incomplete.

- [ ] **Step 3: Add module exports and shared type conversion**

In `src/connector/iceberg/catalog/mod.rs`:

```rust
pub(crate) mod schema_update;

pub(crate) use schema_update::alter_table_schema;
```

In `src/connector/iceberg/catalog/registry.rs`, change:

```rust
fn iceberg_type_for_sql_type(data_type: &SqlType, next_field_id: &mut i32) -> Result<Type, String>
```

to:

```rust
pub(crate) fn iceberg_type_for_sql_type(
    data_type: &SqlType,
    next_field_id: &mut i32,
) -> Result<Type, String>
```

- [ ] **Step 4: Implement pure schema-change helpers**

In `schema_update.rs`, add:

```rust
use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

use crate::engine::catalog::normalize_identifier;
use crate::engine::statement::IcebergSchemaChange;
use crate::sql::parser::ast::SqlType;

pub(crate) fn apply_change_to_schema_for_test(
    current: &Schema,
    last_column_id: i32,
    change: &IcebergSchemaChange,
) -> Result<Schema, String> {
    build_updated_schema(current, last_column_id, change)
}

fn build_updated_schema(
    current: &Schema,
    last_column_id: i32,
    change: &IcebergSchemaChange,
) -> Result<Schema, String> {
    reject_reserved_change(change)?;
    let mut fields = current
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();

    match change {
        IcebergSchemaChange::AddColumn { name, data_type, .. } => {
            reject_name_conflict(&fields, name)?;
            let mut next_nested_id = last_column_id
                .checked_add(2)
                .ok_or_else(|| "too many iceberg columns".to_string())?;
            let ty = crate::connector::iceberg::catalog::registry::iceberg_type_for_sql_type(
                data_type,
                &mut next_nested_id,
            )?;
            let id = last_column_id
                .checked_add(1)
                .ok_or_else(|| "too many iceberg columns".to_string())?;
            fields.push(NestedField::optional(id, name, ty));
        }
        IcebergSchemaChange::DropColumn { name } => {
            let normalized = normalize_identifier(name)?;
            let before = fields.len();
            fields.retain(|f| normalize_identifier(&f.name).ok().as_deref() != Some(normalized.as_str()));
            if fields.len() == before {
                return Err(format!("unknown Iceberg column `{name}`"));
            }
        }
        IcebergSchemaChange::RenameColumn { old_name, new_name } => {
            reject_name_conflict(&fields, new_name)?;
            let normalized = normalize_identifier(old_name)?;
            let field = fields
                .iter_mut()
                .find(|f| normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str()))
                .ok_or_else(|| format!("unknown Iceberg column `{old_name}`"))?;
            field.name = new_name.clone();
        }
        IcebergSchemaChange::ModifyColumn { name, new_type } => {
            let normalized = normalize_identifier(name)?;
            let field = fields
                .iter_mut()
                .find(|f| normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str()))
                .ok_or_else(|| format!("unknown Iceberg column `{name}`"))?;
            field.field_type = widen_type(&field.field_type, new_type)?;
        }
    }

    Schema::builder()
        .with_fields(fields.into_iter().map(Arc::new).collect())
        .build()
        .map_err(|e| format!("build evolved iceberg schema failed: {e}"))
}

fn reject_name_conflict(fields: &[NestedField], name: &str) -> Result<(), String> {
    let normalized = normalize_identifier(name)?;
    if fields
        .iter()
        .any(|f| normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str()))
    {
        return Err(format!("Iceberg column `{name}` already exists"));
    }
    Ok(())
}

fn reject_reserved_change(change: &IcebergSchemaChange) -> Result<(), String> {
    let names: Vec<&str> = match change {
        IcebergSchemaChange::AddColumn { name, .. } => vec![name.as_str()],
        IcebergSchemaChange::DropColumn { name } => vec![name.as_str()],
        IcebergSchemaChange::RenameColumn { old_name, new_name } => {
            vec![old_name.as_str(), new_name.as_str()]
        }
        IcebergSchemaChange::ModifyColumn { name, .. } => vec![name.as_str()],
    };
    for name in names {
        if crate::exec::row_position::is_iceberg_row_id(name)
            || crate::exec::row_position::is_iceberg_last_updated_sequence_number(name)
        {
            return Err(format!("Iceberg schema evolution cannot modify reserved column `{name}`"));
        }
    }
    Ok(())
}

fn widen_type(current: &Type, new_type: &SqlType) -> Result<Type, String> {
    match (current, new_type) {
        (Type::Primitive(PrimitiveType::Int), SqlType::BigInt) => {
            Ok(Type::Primitive(PrimitiveType::Long))
        }
        (Type::Primitive(PrimitiveType::Float), SqlType::Double) => {
            Ok(Type::Primitive(PrimitiveType::Double))
        }
        _ => Err(format!(
            "unsupported Iceberg type evolution: {current:?} -> {new_type:?}"
        )),
    }
}
```

- [ ] **Step 5: Implement the transaction action and public entry point**

Extend `schema_update.rs`:

```rust
use async_trait::async_trait;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{Catalog, TableRequirement, TableUpdate};

use crate::engine::backend_resolver::resolve_existing_table_target;
use crate::engine::StandaloneState;
use crate::engine::statement::AlterIcebergSchemaStmt;
use std::sync::Arc as StdArc;

struct SchemaUpdateTxnAction {
    change: IcebergSchemaChange,
}

#[async_trait]
impl TransactionAction for SchemaUpdateTxnAction {
    async fn commit(self: StdArc<Self>, table: &iceberg::table::Table) -> iceberg::Result<ActionCommit> {
        let metadata = table.metadata();
        let current_schema = metadata.current_schema();
        let new_schema = build_updated_schema(
            current_schema,
            metadata.last_column_id(),
            &self.change,
        )
        .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e))?;

        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSchema { schema: new_schema },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ],
            vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: metadata.current_schema_id(),
                },
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id: metadata.last_column_id(),
                },
            ],
        ))
    }
}

pub(crate) fn alter_table_schema(
    state: &Arc<StandaloneState>,
    stmt: &AlterIcebergSchemaStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), String> {
    let target = resolve_existing_table_target(
        state,
        &stmt.table,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err("Iceberg schema evolution only supports standalone iceberg catalogs".to_string());
    }

    protect_schema_change(state, &target, &stmt.change)?;

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let loaded = crate::connector::iceberg::catalog::registry::load_table(
        &entry,
        &target.namespace,
        &target.table,
    )?;
    let catalog = crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)?;

    crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        let tx = Transaction::new(&loaded.table);
        let tx = SchemaUpdateTxnAction {
            change: stmt.change.clone(),
        }
        .apply(tx)?;
        tx.commit(&catalog).await
    })
    .map_err(|e| format!("alter iceberg schema runtime failed: {e}"))?
    .map_err(|e| format!("alter iceberg schema failed: {e}"))?;

    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, &target)?;
    Ok(())
}
```

`TableMetadata::last_column_id()` already exists in the vendored API; do not edit vendor files for this task.

- [ ] **Step 6: Add initial protection helper**

In `schema_update.rs`, add a minimal version that rejects reserved names and delegates deeper drop checks to Task 3:

```rust
fn protect_schema_change(
    _state: &Arc<StandaloneState>,
    _target: &crate::engine::backend_resolver::TargetBackend,
    change: &IcebergSchemaChange,
) -> Result<(), String> {
    reject_reserved_change(change)
}
```

Task 3 expands this helper for equality-delete and MV dependencies.

- [ ] **Step 7: Run schema-update tests**

Run:

```bash
cargo test --lib schema_update -- --nocapture
```

Expected: all schema-update unit tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs src/connector/iceberg/catalog/mod.rs src/connector/iceberg/catalog/registry.rs
git commit -m "feat: add iceberg schema update transaction"
```

## Task 3: Drop Protection and DDL Execution Coverage

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`
- Modify: `src/engine/mod.rs`

- [ ] **Step 1: Add protection tests**

In `schema_update.rs` tests, add:

```rust
    #[test]
    fn reserved_column_changes_are_rejected() {
        let err = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::DropColumn {
                name: "_row_id".to_string(),
            },
        )
        .expect_err("reserved");
        assert!(err.contains("reserved column"));
    }
```

Add an integration-shaped unit test for equality-delete protection using a small helper:

```rust
    #[test]
    fn drop_rejects_equality_delete_dependency_by_name() {
        let deps = vec!["id".to_string()];
        let err = reject_drop_dependencies_for_test("id", &deps, &[])
            .expect_err("drop dependency");
        assert!(err.contains("equality-delete"));
    }
```

- [ ] **Step 2: Implement equality-delete and MV protection helpers**

Add these helpers in `schema_update.rs`:

```rust
fn reject_drop_dependencies_for_test(
    column: &str,
    equality_delete_columns: &[String],
    mv_sqls: &[String],
) -> Result<(), String> {
    reject_drop_dependencies(column, equality_delete_columns, mv_sqls)
}

fn reject_drop_dependencies(
    column: &str,
    equality_delete_columns: &[String],
    mv_sqls: &[String],
) -> Result<(), String> {
    let normalized = normalize_identifier(column)?;
    if equality_delete_columns
        .iter()
        .any(|c| normalize_identifier(c).ok().as_deref() == Some(normalized.as_str()))
    {
        return Err(format!(
            "DROP COLUMN `{column}` is blocked because an Iceberg equality-delete file references it"
        ));
    }
    for sql in mv_sqls {
        if sql_mentions_identifier(sql, &normalized) {
            return Err(format!(
                "DROP COLUMN `{column}` is blocked because a managed materialized view references it"
            ));
        }
    }
    Ok(())
}

fn sql_mentions_identifier(sql: &str, normalized_identifier: &str) -> bool {
    sql.split(|ch: char| !(ch == '_' || ch.is_ascii_alphanumeric()))
        .filter(|token| !token.is_empty())
        .any(|token| token.eq_ignore_ascii_case(normalized_identifier))
}
```

Expand `protect_schema_change`:

```rust
fn protect_schema_change(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    change: &IcebergSchemaChange,
) -> Result<(), String> {
    reject_reserved_change(change)?;
    let IcebergSchemaChange::DropColumn { name } = change else {
        return Ok(());
    };

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let loaded = crate::connector::iceberg::catalog::registry::load_table(
        &entry,
        &target.namespace,
        &target.table,
    )?;
    let files =
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(&loaded.table)?;
    let equality_delete_columns = files
        .iter()
        .flat_map(|file| file.delete_files.iter())
        .filter(|delete_file| {
            delete_file.file_content
                == crate::sql::catalog::IcebergDeleteFileContent::Equality
        })
        .flat_map(|delete_file| delete_file.equality_column_names.clone())
        .collect::<Vec<_>>();

    let mv_sqls = managed_mv_sqls_for_target(state, target)?;
    reject_drop_dependencies(name, &equality_delete_columns, &mv_sqls)
}

fn managed_mv_sqls_for_target(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<Vec<String>, String> {
    let Some(store) = state.metadata_store.as_ref() else {
        return Ok(Vec::new());
    };
    let snapshot = store.load_snapshot()?.managed;
    let target_key = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
    Ok(snapshot
        .materialized_views
        .into_iter()
        .filter(|mv| {
            mv.base_table_refs.iter().any(|base| {
                base.catalog.eq_ignore_ascii_case(&target.catalog)
                    && base.namespace.eq_ignore_ascii_case(&target.namespace)
                    && base.table.eq_ignore_ascii_case(&target.table)
            }) || mv.select_sql.to_ascii_lowercase().contains(&target_key.to_ascii_lowercase())
        })
        .map(|mv| mv.select_sql)
        .collect())
}
```

- [ ] **Step 3: Run protection tests**

Run:

```bash
cargo test --lib schema_update -- --nocapture
```

Expected: schema-update tests pass.

- [ ] **Step 4: Add a dispatch smoke test**

In `src/engine/mod.rs` tests, add a smoke test that executes schema DDL against an unknown Iceberg catalog and verifies the custom schema DDL path is reached before generic SQL parsing:

```rust
#[test]
fn alter_iceberg_schema_dispatches_before_generic_sqlparser() {
    let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("engine");
    let err = engine
        .session()
        .execute("ALTER TABLE missing.db.t ADD COLUMN c INT")
        .expect_err("unknown catalog");
    assert!(err.contains("unknown catalog"));
}
```

- [ ] **Step 5: Run focused engine tests**

Run:

```bash
cargo test --lib alter_iceberg_schema_dispatches -- --nocapture
```

Expected: dispatch smoke test passes or is skipped if the existing engine test API does not support a simple default engine. Do not add a brittle test if setup requires external MinIO.

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs src/engine/mod.rs
git commit -m "feat: protect iceberg schema ddl"
```

## Task 4: Query Refresh and Iceberg Field-ID Metadata

**Files:**
- Modify: `src/sql/catalog.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`
- Modify: `src/engine/query_prep.rs`
- Modify: `src/sql/codegen/descriptors.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`

- [ ] **Step 1: Add catalog metadata tests**

In `src/sql/catalog.rs` tests, add:

```rust
    #[test]
    fn table_def_can_carry_iceberg_schema_metadata() {
        let table = TableDef {
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            iceberg_table: Some(IcebergTableInfo {
                location: "file:///tmp/warehouse/db/orders".to_string(),
                schema: IcebergSchemaDef {
                    fields: vec![IcebergSchemaFieldDef {
                        field_id: 1,
                        name: "id".to_string(),
                        children: vec![],
                    }],
                },
            }),
            storage: TableStorage::S3ParquetFiles {
                files: vec![],
                cloud_properties: Default::default(),
            },
        };
        assert_eq!(
            table.iceberg_table.as_ref().unwrap().schema.fields[0].field_id,
            1
        );
    }
```

- [ ] **Step 2: Add metadata structs to `TableDef`**

In `src/sql/catalog.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergSchemaFieldDef {
    pub field_id: i32,
    pub name: String,
    pub children: Vec<IcebergSchemaFieldDef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergSchemaDef {
    pub fields: Vec<IcebergSchemaFieldDef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergTableInfo {
    pub location: String,
    pub schema: IcebergSchemaDef,
}
```

Add to `TableDef`:

```rust
    pub iceberg_table: Option<IcebergTableInfo>,
```

Update every `TableDef { ... }` literal touched by the compiler to include `iceberg_table: None`, except real Iceberg table creation in `backend.rs`.

- [ ] **Step 3: Populate Iceberg metadata in `backend.rs`**

In `src/connector/iceberg/catalog/backend.rs`, add conversion helpers:

```rust
fn iceberg_schema_def(schema: &iceberg::spec::Schema) -> crate::sql::catalog::IcebergSchemaDef {
    crate::sql::catalog::IcebergSchemaDef {
        fields: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| iceberg_schema_field_def(field.as_ref()))
            .collect(),
    }
}

fn iceberg_schema_field_def(
    field: &iceberg::spec::NestedField,
) -> crate::sql::catalog::IcebergSchemaFieldDef {
    let children = match &field.field_type {
        iceberg::spec::Type::Struct(struct_ty) => struct_ty
            .fields()
            .iter()
            .map(|child| iceberg_schema_field_def(child.as_ref()))
            .collect(),
        iceberg::spec::Type::List(list_ty) => {
            vec![iceberg_schema_field_def(list_ty.element_field.as_ref())]
        }
        iceberg::spec::Type::Map(map_ty) => vec![
            iceberg_schema_field_def(map_ty.key_field.as_ref()),
            iceberg_schema_field_def(map_ty.value_field.as_ref()),
        ],
        _ => vec![],
    };
    crate::sql::catalog::IcebergSchemaFieldDef {
        field_id: field.id,
        name: field.name.clone(),
        children,
    }
}
```

In `build_iceberg_table_def_with_data_files`, set:

```rust
        iceberg_table: Some(crate::sql::catalog::IcebergTableInfo {
            location: loaded.table.metadata().location().to_string(),
            schema: iceberg_schema_def(loaded.table.metadata().current_schema()),
        }),
```

- [ ] **Step 4: Force refresh query registration**

In `src/engine/query_prep.rs`, replace the existing skip block:

```rust
        if !force_refresh {
            let local = state.catalog.read().expect("catalog read lock");
            if local.get(&target.namespace, &target.table).is_ok() {
                continue;
            }
        }
```

with:

```rust
        if !force_refresh && target.backend_name != "iceberg" {
            let local = state.catalog.read().expect("catalog read lock");
            if local.get(&target.namespace, &target.table).is_ok() {
                continue;
            }
        }
        if target.backend_name == "iceberg" {
            let guard = state
                .iceberg_catalogs
                .read()
                .expect("iceberg catalog read lock");
            let entry = guard.get(&target.catalog)?;
            entry.invalidate_table_cache(&target.namespace, &target.table);
        }
```

- [ ] **Step 5: Emit Iceberg table descriptors**

In `src/sql/codegen/descriptors.rs`, import the new metadata types and add helpers:

```rust
use crate::sql::catalog::{IcebergSchemaDef, IcebergSchemaFieldDef, TableDef};
```

Add:

```rust
    pub fn add_table_for_scan(
        &mut self,
        table_id: types::TTableId,
        db_name: &str,
        table: &TableDef,
    ) {
        if let Some(iceberg) = table.iceberg_table.as_ref() {
            self.add_iceberg_table(table_id, db_name, table, iceberg);
        } else {
            self.add_table(table_id, db_name, &table.name, table.columns.len() as i32);
        }
    }

    fn add_iceberg_table(
        &mut self,
        table_id: types::TTableId,
        db_name: &str,
        table: &TableDef,
        iceberg: &crate::sql::catalog::IcebergTableInfo,
    ) {
        if !self.table_ids.insert(table_id) {
            return;
        }
        let columns = table
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| descriptors::TColumn {
                column_name: column.name.clone(),
                column_type: None,
                aggregation_type: None,
                is_key: Some(false),
                is_allow_null: Some(column.nullable),
                default_value: None,
                default_expr: None,
                is_bloom_filter_column: Some(false),
                define_expr: None,
                is_auto_increment: Some(false),
                col_unique_id: Some(idx as i32),
                has_bitmap_index: Some(false),
                agg_state_desc: None,
                index_len: None,
                type_desc: super::type_infer::arrow_type_to_type_desc(&column.data_type).ok(),
            })
            .collect::<Vec<_>>();
        let iceberg_table = descriptors::TIcebergTable {
            location: Some(iceberg.location.clone()),
            columns: Some(columns),
            iceberg_schema: Some(to_thrift_iceberg_schema(&iceberg.schema)),
            partition_column_names: None,
            compressed_partitions: None,
            partitions: None,
            iceberg_equal_delete_schema: None,
            partition_info: None,
            sort_order: None,
        };
        self.tables.push(descriptors::TTableDescriptor::new(
            table_id,
            types::TTableType::ICEBERG_TABLE,
            table.columns.len() as i32,
            0,
            table.name.clone(),
            db_name.to_string(),
            None::<descriptors::TMySQLTable>,
            None::<descriptors::TOlapTable>,
            None::<descriptors::TSchemaTable>,
            None::<descriptors::TBrokerTable>,
            None::<descriptors::TEsTable>,
            None::<descriptors::TJDBCTable>,
            None::<descriptors::THdfsTable>,
            Some(iceberg_table),
            None::<descriptors::THudiTable>,
            None::<descriptors::TDeltaLakeTable>,
            None::<descriptors::TFileTable>,
            None::<descriptors::TTableFunctionTable>,
            None::<descriptors::TPaimonTable>,
        ));
    }
```

Add conversion functions outside the impl:

```rust
fn to_thrift_iceberg_schema(schema: &IcebergSchemaDef) -> descriptors::TIcebergSchema {
    descriptors::TIcebergSchema {
        fields: Some(schema.fields.iter().map(to_thrift_iceberg_field).collect()),
    }
}

fn to_thrift_iceberg_field(field: &IcebergSchemaFieldDef) -> descriptors::TIcebergSchemaField {
    descriptors::TIcebergSchemaField {
        field_id: Some(field.field_id),
        name: Some(field.name.clone()),
        children: if field.children.is_empty() {
            None
        } else {
            Some(field.children.iter().map(to_thrift_iceberg_field).collect())
        },
    }
}
```

- [ ] **Step 6: Use the new descriptor helper**

In `src/sql/codegen/fragment_builder.rs`, replace:

```rust
            self.desc_builder.add_table(
                layout.table_id,
                &op.database,
                &op.table.name,
                op.table.columns.len() as i32,
            );
```

with:

```rust
            self.desc_builder
                .add_table_for_scan(layout.table_id, &op.database, &op.table);
```

For non-managed Iceberg scans where `physical_layout` is `None`, allocate a stable synthetic table id before `add_tuple` so `find_iceberg_table` can locate the descriptor:

```rust
        let descriptor_table_id = physical_layout
            .as_ref()
            .map(|layout| layout.table_id)
            .or_else(|| op.table.iceberg_table.as_ref().map(|_| scan_node_id as i64));
        if let Some(table_id) = descriptor_table_id {
            self.desc_builder
                .add_table_for_scan(table_id, &op.database, &op.table);
        }
```

Then pass `descriptor_table_id` to `add_tuple`.

- [ ] **Step 7: Run field-id-related tests**

Run:

```bash
cargo test --lib table_def_can_carry_iceberg_schema_metadata -- --nocapture
cargo test --lib iceberg_schema_evolution_reads_renamed_columns_by_field_id -- --nocapture
```

Expected: tests pass.

- [ ] **Step 8: Compile focused codegen path**

Run:

```bash
cargo test --lib fragment_builder -- --nocapture
```

Expected: fragment builder tests pass. Update every compiler-reported `TableDef` literal with `iceberg_table: None`; keep the new catalog metadata test and real Iceberg `backend.rs` table definitions as `Some(...)`.

- [ ] **Step 9: Commit**

```bash
git add src/sql/catalog.rs src/connector/iceberg/catalog/backend.rs src/engine/query_prep.rs src/sql/codegen/descriptors.rs src/sql/codegen/fragment_builder.rs
git commit -m "feat: propagate iceberg schema field ids"
```

## Task 5: Local-FS SQL Coverage

**Files:**
- Add: `tests/sql-test-runner/conf/standalone_iceberg_local.conf`
- Add: `sql-tests/iceberg/sql/iceberg_schema_evolution_local.sql`
- Add: `sql-tests/iceberg/result/iceberg_schema_evolution_local.result`

- [ ] **Step 1: Add the local-FS runner config**

Create `tests/sql-test-runner/conf/standalone_iceberg_local.conf`:

```toml
[cluster]
host = 127.0.0.1
port = 19031
user = root
password =

[env]
iceberg_catalog_type = hadoop
iceberg_catalog_warehouse = /tmp/novarocks-sql-tests/iceberg-local-catalog/
oss_ak = admin
oss_sk = admin123
oss_endpoint = http://127.0.0.1:9000
```

Before running this case, start standalone on the same private port:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 19031
```

This config intentionally uses a local warehouse path. The existing `standalone_iceberg.conf` is MinIO-backed and belongs to Task 6.

- [ ] **Step 2: Add the local-FS SQL case**

Create `sql-tests/iceberg/sql/iceberg_schema_evolution_local.sql`:

```sql
-- @order_sensitive=true
-- Validate top-level Iceberg schema evolution over the local Hadoop-style catalog.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
DROP TABLE IF EXISTS orders_local;
CREATE TABLE orders_local (
  id INT,
  amount FLOAT
);
INSERT INTO orders_local VALUES (1, 10.5), (2, 20.25);
ALTER TABLE orders_local ADD COLUMN note_text STRING DEFAULT NULL;
SELECT id, amount, note_text FROM orders_local ORDER BY id;

-- query 2
USE iceberg_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
INSERT INTO orders_local (id, amount, note_text) VALUES (3, 30.75, 'new');
SELECT id, amount, note_text FROM orders_local ORDER BY id;

-- query 3
USE iceberg_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
ALTER TABLE orders_local RENAME COLUMN amount TO total_amount;
SELECT id, total_amount, note_text FROM orders_local ORDER BY id;

-- query 4
USE iceberg_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
ALTER TABLE orders_local MODIFY COLUMN id BIGINT;
SELECT id + 10000000000 AS widened_id, total_amount FROM orders_local ORDER BY id;

-- query 5
USE iceberg_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
ALTER TABLE orders_local DROP COLUMN note_text;
SELECT * FROM orders_local ORDER BY id;

-- query 6
-- @expect_error=unknown column
USE iceberg_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
SELECT note_text FROM orders_local;

-- query 7
SET catalog default_catalog;
DROP TABLE iceberg_cat_${suite_uuid0}.schema_evolution_local_${uuid0}.orders_local FORCE;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
```

- [ ] **Step 3: Record the local-FS expected result**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_iceberg_local.conf \
  --only iceberg_schema_evolution_local \
  --mode record \
  --query-timeout 120
```

Expected: `sql-tests/iceberg/result/iceberg_schema_evolution_local.result` is created with query outputs showing NULLs after ADD, preserved values after RENAME, widened BIGINT arithmetic, and no `note_text` after DROP.

- [ ] **Step 4: Verify local-FS SQL case**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_iceberg_local.conf \
  --only iceberg_schema_evolution_local \
  --mode verify \
  --query-timeout 120
```

Expected: `total=1 pass=1 fail=0`.

- [ ] **Step 5: Commit**

```bash
git add tests/sql-test-runner/conf/standalone_iceberg_local.conf sql-tests/iceberg/sql/iceberg_schema_evolution_local.sql sql-tests/iceberg/result/iceberg_schema_evolution_local.result
git commit -m "test: cover local iceberg schema evolution"
```

## Task 6: MinIO/S3 SQL Coverage

**Files:**
- Add: `sql-tests/iceberg/sql/iceberg_schema_evolution_s3.sql`
- Add: `sql-tests/iceberg/result/iceberg_schema_evolution_s3.result`

- [ ] **Step 1: Add the S3 SQL case**

Create `sql-tests/iceberg/sql/iceberg_schema_evolution_s3.sql`:

```sql
-- @order_sensitive=true
-- Validate top-level Iceberg schema evolution against an S3-backed catalog.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
DROP TABLE IF EXISTS orders_s3;
CREATE TABLE orders_s3 (
  id INT,
  amount FLOAT
);
INSERT INTO orders_s3 VALUES (1, 10.5), (2, 20.25);
ALTER TABLE orders_s3 ADD COLUMN note STRING DEFAULT NULL;
SELECT id, amount, note FROM orders_s3 ORDER BY id;

-- query 2
USE iceberg_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
INSERT INTO orders_s3 (id, amount, note) VALUES (3, 30.75, 's3-new');
ALTER TABLE orders_s3 RENAME COLUMN amount TO total_amount;
SELECT id, total_amount, note FROM orders_s3 ORDER BY id;

-- query 3
USE iceberg_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
ALTER TABLE orders_s3 MODIFY COLUMN id BIGINT;
ALTER TABLE orders_s3 DROP COLUMN note;
SELECT id + 10000000000 AS widened_id, total_amount FROM orders_s3 ORDER BY id;

-- query 4
SET catalog default_catalog;
DROP TABLE iceberg_cat_${suite_uuid0}.schema_evolution_s3_${uuid0}.orders_s3 FORCE;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
```

- [ ] **Step 2: Record S3 expected result**

Start standalone on the private port declared by `standalone_iceberg.conf`. Do not use the user's reserved `9030`:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 19030
```

In another shell:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_iceberg.conf \
  --only iceberg_schema_evolution_s3 \
  --mode record \
  --query-timeout 120
```

Expected: `sql-tests/iceberg/result/iceberg_schema_evolution_s3.result` is created. If MinIO is unreachable, stop and start MinIO rather than marking the test optional.

- [ ] **Step 3: Verify S3 expected result**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_iceberg.conf \
  --only iceberg_schema_evolution_s3 \
  --mode verify \
  --query-timeout 120
```

Expected: `total=1 pass=1 fail=0`.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_schema_evolution_s3.sql sql-tests/iceberg/result/iceberg_schema_evolution_s3.result
git commit -m "test: cover s3 iceberg schema evolution"
```

## Task 7: Full Verification and Cleanup

**Files:**
- Verify all touched files.

- [ ] **Step 1: Run formatter**

Run:

```bash
cargo fmt --check
```

Expected: no output and exit 0. If it fails, run `cargo fmt`, inspect the diff, and commit formatting with the implementation changes that caused it.

- [ ] **Step 2: Run focused Rust tests**

Run:

```bash
cargo test --lib parse_alter_iceberg_schema -- --nocapture
cargo test --lib schema_update -- --nocapture
cargo test --lib iceberg_schema_evolution_reads_renamed_columns_by_field_id -- --nocapture
cargo test --lib fragment_builder -- --nocapture
```

Expected: all pass.

- [ ] **Step 3: Run full library tests if focused tests are clean**

Run:

```bash
cargo test --lib
```

Expected: `test result: ok`.

- [ ] **Step 4: Run local-FS SQL case**

Run against a standalone server on `19031`:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_iceberg_local.conf \
  --only iceberg_schema_evolution_local \
  --mode verify \
  --query-timeout 120
```

Expected: `total=1 pass=1 fail=0`.

- [ ] **Step 5: Run MinIO/S3 SQL case**

Run against a standalone server on `19030` with MinIO reachable at `127.0.0.1:9000`:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_iceberg.conf \
  --only iceberg_schema_evolution_s3 \
  --mode verify \
  --query-timeout 120
```

Expected: `total=1 pass=1 fail=0`.

- [ ] **Step 6: Check whitespace and final diff**

Run:

```bash
git diff --check
git status --short
git log --oneline -8
```

Expected: `git diff --check` exits 0. `git status --short` should only show intentional files if the final commit has not been made.

- [ ] **Step 7: Final commit if any verification-only fixes were needed**

If Step 1 through Step 6 required follow-up edits, commit them:

```bash
git add src/connector/iceberg/catalog/schema_update.rs src/connector/iceberg/catalog/mod.rs src/connector/iceberg/catalog/registry.rs src/engine/statement.rs src/engine/mod.rs src/engine/query_prep.rs src/sql/catalog.rs src/connector/iceberg/catalog/backend.rs src/sql/codegen/descriptors.rs src/sql/codegen/fragment_builder.rs tests/sql-test-runner/conf/standalone_iceberg_local.conf sql-tests/iceberg/sql/iceberg_schema_evolution_local.sql sql-tests/iceberg/result/iceberg_schema_evolution_local.result sql-tests/iceberg/sql/iceberg_schema_evolution_s3.sql sql-tests/iceberg/result/iceberg_schema_evolution_s3.result
git commit -m "fix: finalize iceberg schema evolution coverage"
```

Expected: commit succeeds only when there are actual staged changes.

## Self-Review

- Spec coverage:
  - SQL surface is covered by Task 1.
  - Schema metadata commit is covered by Task 2.
  - Drop protection is covered by Task 3.
  - Query refresh and field-ID propagation are covered by Task 4.
  - Local-FS and MinIO/S3 SQL regression coverage are covered by Tasks 5 and 6.
  - Verification is covered by Task 7.
- Placeholder scan:
  - No `TBD`, `TODO`, or unspecified "add tests" steps remain.
- Type consistency:
  - The plan uses `IcebergSchemaChange` from `src/engine/statement.rs` in both parser and schema-update modules.
  - `TableDef.iceberg_table` is consistently represented as `Option<IcebergTableInfo>`.
  - The descriptor conversion consistently maps `IcebergSchemaDef` to `TIcebergSchema`.
