# Iceberg Partition Evolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 支持 NovaRocks standalone Iceberg table 的 partition evolution，包括 `CREATE TABLE ... PARTITION BY`、`ALTER TABLE ... ADD/DROP PARTITION COLUMN`、多 partition spec 的 `SELECT`、`INSERT INTO` 和 v2 position-delete `DELETE FROM` 正确性。

**Architecture:** Parser/AST 先把 StarRocks 风格 partition transform 表达成稳定的 `IcebergPartitionFieldExpr`；Iceberg catalog 层集中负责 transform/type validation、`PartitionSpec` 构建和 ALTER metadata update；engine write/delete 层只消费 catalog metadata，不拼写 Iceberg JSON。读路径继续扫描 current snapshot 的所有 live data manifests，本任务不做 partition pruning；多 spec `INSERT OVERWRITE` 和 `ADD EQUALITY DELETE` 保持 fail-fast。

**Tech Stack:** Rust 2021, sqlparser 0.61.0, iceberg 0.9.0 vendor patch, Arrow `RecordBatch`, OpenDAL, MinIO-backed `sql-tests`.

**Spec:** [docs/design/specs/2026-05-03-iceberg-partition-evolution-design.md](../specs/2026-05-03-iceberg-partition-evolution-design.md)

---

## File Structure

Planned code boundaries:

- Modify: `src/sql/parser/ast/mod.rs`
  - Add `IcebergPartitionFieldExpr` and `AlterIcebergPartitionSpecStmt`.
  - Add `partition_fields` to `CreateTableKind::Iceberg`.
- Modify: `src/sql/parser/dialect/create_table.rs`
  - Parse `PARTITION BY` into AST instead of skipping it.
  - Unit-test create-table transform parsing and invalid transform arguments.
- Modify: `src/engine/statement.rs`
  - Add `looks_like_alter_partition_column` and `parse_alter_partition_column_sql`.
  - Add unit tests for ALTER parsing.
- Modify: `src/engine/mod.rs`
  - Dispatch ALTER partition DDL before standard sqlparser fallback.
- Modify: `src/connector/backend.rs`
  - Thread `partition_fields` through `CreateTableRequest`.
- Modify: `src/connector/iceberg/catalog/backend.rs`
  - Pass create-table partition fields into Iceberg registry.
- Modify: `src/connector/starrocks/managed/backend.rs`
  - Ignore `partition_fields` explicitly for managed-lake create table.
- Create: `src/connector/iceberg/partition_spec.rs`
  - Own transform mapping, stable field names, type validation, duplicate/missing checks, and `UnboundPartitionSpec` construction.
- Modify: `src/connector/iceberg/mod.rs`
  - Export `partition_spec`.
- Modify: `src/connector/iceberg/catalog/registry.rs`
  - Use initial partition spec during `create_table`.
  - Add `alter_partition_spec`.
  - Preserve data file `partition_spec_id` and partition values in extracted scan metadata.
- Modify: `src/engine/iceberg_writer.rs`
  - Allow `INSERT INTO` on multi-spec tables.
  - Keep `INSERT OVERWRITE` guarded on single-spec tables.
- Modify: `src/engine/delete_flow.rs`
  - Allow v2 position-delete `DELETE FROM` on multi-spec tables.
  - Group delete positions by referenced data file with inherited spec id and partition values.
  - Keep row-lineage DV multi-spec fail-fast unless that path is made per-file-spec-aware in the same task.
- Modify: `src/connector/iceberg/commit/position_delete_writer.rs`
  - Write `WrittenFile.partition_spec_id` and `partition_values` from each referenced data file group.
- Modify: `src/connector/iceberg/commit/row_delta.rs`
  - Write one delete manifest per partition spec id for position-delete commits.
- Modify: `src/connector/iceberg/commit/validation.rs`
  - Split single-spec guard into scoped helpers for overwrite/equality delete.
- Modify: `src/engine/equality_delete_flow.rs`
  - Keep multi-spec/evolved partitioned equality delete rejected with a clear message.
- Modify: `sql-tests/iceberg/sql/iceberg_partition_evolution_1.sql`
  - Remove `EXPLAIN VERBOSE partitions=x/y` assertions and assert result correctness.
- Modify: `sql-tests/iceberg/sql/iceberg_partition_evolution_replace.sql`
  - Keep month -> day and identity -> bucket correctness checks.
- Create: `sql-tests/iceberg/sql/iceberg_partition_evolution_delete.sql`
  - SQL case for DELETE across old and new specs.
- Create: `sql-tests/iceberg/sql/iceberg_partition_evolution_unsupported.sql`
  - SQL case for multi-spec overwrite/equality-delete rejection.

All commands below run from `/Users/harbor/.codex/worktrees/9255/NovaRocks`.

---

## Task 1: Parser and AST for Partition Transforms

**Files:**
- Modify: `src/sql/parser/ast/mod.rs`
- Modify: `src/sql/parser/dialect/create_table.rs`
- Modify: `src/engine/statement.rs`

- [ ] **Step 1: Add AST nodes**

In `src/sql/parser/ast/mod.rs`, add these definitions near `CreateTableKind`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergPartitionFieldExpr {
    Identity { column: String },
    Year { column: String },
    Month { column: String },
    Day { column: String },
    Hour { column: String },
    Bucket { column: String, num_buckets: u32 },
    Truncate { column: String, width: u32 },
    Void { column: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AlterIcebergPartitionSpecStmt {
    AddPartitionColumn {
        table: ObjectName,
        field: IcebergPartitionFieldExpr,
    },
    DropPartitionColumn {
        table: ObjectName,
        field: IcebergPartitionFieldExpr,
    },
}
```

Then change `CreateTableKind::Iceberg` to carry partition fields:

```rust
Iceberg {
    columns: Vec<TableColumnDef>,
    key_desc: Option<TableKeyDesc>,
    bucket_count: Option<u32>,
    partition_fields: Vec<IcebergPartitionFieldExpr>,
    properties: Vec<(String, String)>,
},
```

- [ ] **Step 2: Run the parser compile test and record the expected failure**

Run:

```bash
cargo test --lib sql::parser::dialect::create_table::tests::create_table_parser_preserves_bucket_count -- --exact
```

Expected: compile fails at `CreateTableKind::Iceberg` construction and pattern matches that do not mention `partition_fields`.

- [ ] **Step 3: Thread the new field through existing construction and matches**

Update existing `CreateTableKind::Iceberg` construction in `src/sql/parser/dialect/create_table.rs` to:

```rust
let kind = CreateTableKind::Iceberg {
    columns,
    key_desc,
    bucket_count,
    partition_fields,
    properties,
};
```

For existing matches that do not need the value, use:

```rust
CreateTableKind::Iceberg {
    columns,
    key_desc,
    bucket_count,
    partition_fields,
    properties,
} => {
    let _ = bucket_count;
    let _ = partition_fields;
    /* existing body */
}
```

Use this exact rule: managed-lake paths may ignore `partition_fields`; Iceberg catalog paths must pass it onward.

- [ ] **Step 4: Parse `PARTITION BY` into a vector**

In `src/sql/parser/dialect/create_table.rs`, import `IcebergPartitionFieldExpr` and initialize:

```rust
let mut partition_fields = Vec::new();
```

Replace the current `PARTITION` branch with:

```rust
} else if peek_word_eq(parser, 0, "PARTITION") {
    partition_fields = parse_partition_by_clause(parser)?;
}
```

Add these helpers in the same file:

```rust
fn parse_partition_by_clause(
    parser: &mut Parser<'_>,
) -> Result<Vec<IcebergPartitionFieldExpr>, String> {
    parser.next_token(); // PARTITION
    parser
        .expect_keyword(Keyword::BY)
        .map_err(|e| format!("expected BY after PARTITION: {e}"))?;

    if parser.consume_token(&Token::LParen) {
        let mut fields = Vec::new();
        loop {
            if parser.consume_token(&Token::RParen) {
                break;
            }
            if !fields.is_empty() {
                parser
                    .expect_token(&Token::Comma)
                    .map_err(|e| format!("expected , in PARTITION BY: {e}"))?;
            }
            fields.push(parse_partition_field_expr(parser)?);
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        if fields.is_empty() {
            return Err("PARTITION BY requires at least one field".to_string());
        }
        return Ok(fields);
    }

    let mut fields = vec![parse_partition_field_expr(parser)?];
    while parser.consume_token(&Token::Comma) {
        fields.push(parse_partition_field_expr(parser)?);
    }
    Ok(fields)
}

pub(crate) fn parse_partition_field_expr(
    parser: &mut Parser<'_>,
) -> Result<IcebergPartitionFieldExpr, String> {
    let ident = parser
        .parse_identifier()
        .map_err(|e| format!("parse partition field failed: {e}"))?
        .value;

    if !parser.consume_token(&Token::LParen) {
        return Ok(IcebergPartitionFieldExpr::Identity {
            column: crate::engine::catalog::normalize_identifier(&ident)?,
        });
    }

    let transform = ident.to_ascii_lowercase();
    let column = parser
        .parse_identifier()
        .map_err(|e| format!("partition transform `{transform}` requires a source column: {e}"))?
        .value;
    let column = crate::engine::catalog::normalize_identifier(&column)?;

    let out = match transform.as_str() {
        "year" => IcebergPartitionFieldExpr::Year { column },
        "month" => IcebergPartitionFieldExpr::Month { column },
        "day" => IcebergPartitionFieldExpr::Day { column },
        "hour" => IcebergPartitionFieldExpr::Hour { column },
        "void" => IcebergPartitionFieldExpr::Void { column },
        "bucket" => {
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| format!("bucket transform requires num_buckets: {e}"))?;
            let num_buckets = parse_positive_u32(parser, "bucket num_buckets")?;
            IcebergPartitionFieldExpr::Bucket {
                column,
                num_buckets,
            }
        }
        "truncate" => {
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| format!("truncate transform requires width: {e}"))?;
            let width = parse_positive_u32(parser, "truncate width")?;
            IcebergPartitionFieldExpr::Truncate { column, width }
        }
        other => return Err(format!("unsupported iceberg partition transform `{other}`")),
    };

    parser
        .expect_token(&Token::RParen)
        .map_err(|e| format!("partition transform `{transform}` missing ): {e}"))?;
    Ok(out)
}

fn parse_positive_u32(parser: &mut Parser<'_>, label: &str) -> Result<u32, String> {
    let token = parser.next_token();
    let Token::Number(value, _) = token.token else {
        return Err(format!("{label} must be a positive integer, got {}", token.token));
    };
    let parsed = value
        .parse::<u32>()
        .map_err(|e| format!("{label} `{value}` is invalid: {e}"))?;
    if parsed == 0 {
        return Err(format!("{label} must be positive"));
    }
    Ok(parsed)
}
```

- [ ] **Step 5: Add create-table parser tests**

In `src/sql/parser/dialect/create_table.rs` test module, add:

```rust
#[test]
fn create_table_parser_preserves_partition_transforms() {
    let dialect = StarRocksDialect;
    let mut parser = Parser::new(&dialect)
        .try_with_sql(
            "create table tbl (id bigint, ts datetime, name string) \
             partition by (month(ts), bucket(id, 16), truncate(name, 8))",
        )
        .expect("parser");
    let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
    let CreateTableKind::Iceberg {
        partition_fields, ..
    } = stmt.kind;
    assert_eq!(
        partition_fields,
        vec![
            crate::sql::parser::ast::IcebergPartitionFieldExpr::Month {
                column: "ts".to_string()
            },
            crate::sql::parser::ast::IcebergPartitionFieldExpr::Bucket {
                column: "id".to_string(),
                num_buckets: 16
            },
            crate::sql::parser::ast::IcebergPartitionFieldExpr::Truncate {
                column: "name".to_string(),
                width: 8
            },
        ]
    );
}

#[test]
fn create_table_parser_rejects_invalid_partition_transform_args() {
    for sql in [
        "create table tbl (id bigint) partition by bucket(id, 0)",
        "create table tbl (name string) partition by truncate(name, 0)",
        "create table tbl (id bigint) partition by unknown(id)",
    ] {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
        assert!(
            parse_create_table_statement(&mut parser).is_err(),
            "expected partition transform parse failure for {sql}"
        );
    }
}
```

- [ ] **Step 6: Add ALTER parser shape**

In `src/engine/statement.rs`, add:

```rust
pub(crate) fn looks_like_alter_partition_column(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE")
        && (upper.contains(" ADD PARTITION COLUMN ")
            || upper.contains(" DROP PARTITION COLUMN "))
}

pub(crate) fn parse_alter_partition_column_sql(
    sql: &str,
) -> Result<crate::sql::parser::ast::AlterIcebergPartitionSpecStmt, String> {
    use crate::sql::parser::ast::AlterIcebergPartitionSpecStmt;
    const ALTER_TABLE: &str = "ALTER TABLE";
    const ADD: &str = " ADD PARTITION COLUMN ";
    const DROP: &str = " DROP PARTITION COLUMN ";

    let upper = sql.to_ascii_uppercase();
    let alter_idx = upper.find(ALTER_TABLE).ok_or("missing ALTER TABLE")?;
    let (op_idx, is_add, marker) = if let Some(idx) = upper.find(ADD) {
        (idx, true, ADD)
    } else if let Some(idx) = upper.find(DROP) {
        (idx, false, DROP)
    } else {
        return Err("ALTER TABLE requires ADD/DROP PARTITION COLUMN".to_string());
    };

    let table_str = sql[alter_idx + ALTER_TABLE.len()..op_idx].trim();
    let table_parts = table_str
        .split('.')
        .map(crate::engine::catalog::normalize_identifier)
        .collect::<Result<Vec<_>, _>>()?;
    if table_parts.is_empty() {
        return Err("ALTER TABLE PARTITION COLUMN requires a table name".to_string());
    }

    let expr_sql = sql[op_idx + marker.len()..].trim().trim_end_matches(';');
    let dialect = crate::sql::parser::dialect::StarRocksDialect;
    let mut parser = sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(expr_sql)
        .map_err(|e| format!("parse PARTITION COLUMN expression: {e}"))?;
    let field =
        crate::sql::parser::dialect::create_table::parse_partition_field_expr(&mut parser)?;

    let table = crate::sql::parser::ast::ObjectName { parts: table_parts };
    if is_add {
        Ok(AlterIcebergPartitionSpecStmt::AddPartitionColumn { table, field })
    } else {
        Ok(AlterIcebergPartitionSpecStmt::DropPartitionColumn { table, field })
    }
}
```

- [ ] **Step 7: Add ALTER parser tests**

In `src/engine/statement.rs` tests, add:

```rust
#[test]
fn parse_alter_partition_column_statement() {
    use crate::sql::parser::ast::{AlterIcebergPartitionSpecStmt, IcebergPartitionFieldExpr};

    let add = super::parse_alter_partition_column_sql(
        "ALTER TABLE ice.db.orders ADD PARTITION COLUMN bucket(user_id, 32)",
    )
    .expect("parse add");
    assert_eq!(
        add,
        AlterIcebergPartitionSpecStmt::AddPartitionColumn {
            table: crate::sql::parser::ast::ObjectName {
                parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()]
            },
            field: IcebergPartitionFieldExpr::Bucket {
                column: "user_id".to_string(),
                num_buckets: 32
            }
        }
    );

    let drop = super::parse_alter_partition_column_sql(
        "ALTER TABLE ice.db.orders DROP PARTITION COLUMN month(ts)",
    )
    .expect("parse drop");
    assert_eq!(
        drop,
        AlterIcebergPartitionSpecStmt::DropPartitionColumn {
            table: crate::sql::parser::ast::ObjectName {
                parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()]
            },
            field: IcebergPartitionFieldExpr::Month {
                column: "ts".to_string()
            }
        }
    );
}
```

- [ ] **Step 8: Run parser tests**

Run:

```bash
cargo test --lib sql::parser::dialect::create_table::tests::create_table_parser_preserves_partition_transforms -- --exact
cargo test --lib sql::parser::dialect::create_table::tests::create_table_parser_rejects_invalid_partition_transform_args -- --exact
cargo test --lib engine::statement::tests::parse_alter_partition_column_statement -- --exact
```

Expected: all three tests pass.

- [ ] **Step 9: Commit parser work**

Run:

```bash
git add src/sql/parser/ast/mod.rs src/sql/parser/dialect/create_table.rs src/engine/statement.rs
git commit -m "feat: parse iceberg partition evolution DDL"
```

---

## Task 2: Iceberg PartitionSpec Builder and CREATE TABLE Wiring

**Files:**
- Create: `src/connector/iceberg/partition_spec.rs`
- Modify: `src/connector/iceberg/mod.rs`
- Modify: `src/connector/backend.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`
- Modify: `src/connector/starrocks/managed/backend.rs`
- Modify: `src/connector/iceberg/catalog/registry.rs`
- Modify: `src/engine/statement.rs`

- [ ] **Step 1: Add create-table request field**

In `src/connector/backend.rs`, extend `CreateTableRequest`:

```rust
pub partition_fields: Vec<crate::sql::parser::ast::IcebergPartitionFieldExpr>,
```

In `src/engine/statement.rs`, pass the field in `execute_create_table_statement`:

```rust
backend.create_table(crate::connector::backend::CreateTableRequest {
    catalog: target.catalog.clone(),
    namespace: target.namespace.clone(),
    table: target.table.clone(),
    columns,
    key_desc,
    bucket_count,
    partition_fields,
    properties,
})?;
```

In managed-lake backend, destructure `CreateTableRequest { partition_fields, .. }` and reject non-empty values:

```rust
if !req.partition_fields.is_empty() {
    return Err("managed-lake CREATE TABLE does not support Iceberg PARTITION BY".to_string());
}
```

- [ ] **Step 2: Create the partition spec module**

Create `src/connector/iceberg/partition_spec.rs`:

```rust
use iceberg::spec::{
    PartitionSpec, PartitionSpecRef, Schema, Transform, Type, UnboundPartitionField,
    UnboundPartitionSpec, UnboundPartitionSpecBuilder,
};

use crate::engine::catalog::normalize_identifier;
use crate::sql::parser::ast::IcebergPartitionFieldExpr;

pub(crate) fn build_initial_partition_spec(
    schema: &Schema,
    fields: &[IcebergPartitionFieldExpr],
) -> Result<Option<UnboundPartitionSpec>, String> {
    if fields.is_empty() {
        return Ok(None);
    }
    let mut builder = UnboundPartitionSpec::builder();
    for field in fields {
        let source_id = source_field_id(schema, field)?;
        validate_transform(schema, source_id, field)?;
        builder = builder
            .add_partition_field(source_id, stable_field_name(field), to_transform(field))
            .map_err(|e| format!("build iceberg partition spec failed: {e}"))?;
    }
    Ok(Some(builder.build()))
}

pub(crate) fn build_evolved_partition_spec(
    schema: &Schema,
    current: &PartitionSpecRef,
    change: PartitionSpecChange<'_>,
) -> Result<UnboundPartitionSpec, String> {
    let mut fields: Vec<UnboundPartitionField> =
        current.fields().iter().cloned().map(Into::into).collect();
    match change {
        PartitionSpecChange::Add(expr) => {
            let source_id = source_field_id(schema, expr)?;
            validate_transform(schema, source_id, expr)?;
            let transform = to_transform(expr);
            if fields
                .iter()
                .any(|field| field.source_id == source_id && field.transform == transform)
            {
                return Err(format!(
                    "partition field `{}` already exists in current default spec {}",
                    stable_field_name(expr),
                    current.spec_id()
                ));
            }
            fields.push(UnboundPartitionField {
                source_id,
                field_id: None,
                name: stable_field_name(expr),
                transform,
            });
        }
        PartitionSpecChange::Drop(expr) => {
            let source_id = source_field_id(schema, expr)?;
            let transform = to_transform(expr);
            let before = fields.len();
            fields.retain(|field| !(field.source_id == source_id && field.transform == transform));
            if fields.len() == before {
                return Err(format!(
                    "partition field `{}` is not present in current default spec {}",
                    stable_field_name(expr),
                    current.spec_id()
                ));
            }
        }
    }

    let mut builder = UnboundPartitionSpecBuilder::new();
    for field in fields {
        builder = builder
            .add_partition_fields([field])
            .map_err(|e| format!("build evolved iceberg partition spec failed: {e}"))?;
    }
    Ok(builder.build())
}

pub(crate) enum PartitionSpecChange<'a> {
    Add(&'a IcebergPartitionFieldExpr),
    Drop(&'a IcebergPartitionFieldExpr),
}

pub(crate) fn spec_count(table: &iceberg::table::Table) -> usize {
    table.metadata().partition_specs_iter().count()
}

pub(crate) fn partition_spec_by_id(
    table: &iceberg::table::Table,
    spec_id: i32,
) -> Result<PartitionSpecRef, String> {
    table
        .metadata()
        .partition_spec_by_id(spec_id)
        .cloned()
        .ok_or_else(|| format!("iceberg table metadata missing partition spec id {spec_id}"))
}

fn source_field_id(schema: &Schema, expr: &IcebergPartitionFieldExpr) -> Result<i32, String> {
    let column = normalize_identifier(source_column(expr))?;
    schema
        .as_struct()
        .fields()
        .iter()
        .find(|field| normalize_identifier(&field.name).ok().as_deref() == Some(column.as_str()))
        .map(|field| field.id)
        .ok_or_else(|| format!("partition source column `{column}` does not exist"))
}

fn source_column(expr: &IcebergPartitionFieldExpr) -> &str {
    match expr {
        IcebergPartitionFieldExpr::Identity { column }
        | IcebergPartitionFieldExpr::Year { column }
        | IcebergPartitionFieldExpr::Month { column }
        | IcebergPartitionFieldExpr::Day { column }
        | IcebergPartitionFieldExpr::Hour { column }
        | IcebergPartitionFieldExpr::Bucket { column, .. }
        | IcebergPartitionFieldExpr::Truncate { column, .. }
        | IcebergPartitionFieldExpr::Void { column } => column,
    }
}

fn to_transform(expr: &IcebergPartitionFieldExpr) -> Transform {
    match expr {
        IcebergPartitionFieldExpr::Identity { .. } => Transform::Identity,
        IcebergPartitionFieldExpr::Year { .. } => Transform::Year,
        IcebergPartitionFieldExpr::Month { .. } => Transform::Month,
        IcebergPartitionFieldExpr::Day { .. } => Transform::Day,
        IcebergPartitionFieldExpr::Hour { .. } => Transform::Hour,
        IcebergPartitionFieldExpr::Bucket { num_buckets, .. } => Transform::Bucket(*num_buckets),
        IcebergPartitionFieldExpr::Truncate { width, .. } => Transform::Truncate(*width),
        IcebergPartitionFieldExpr::Void { .. } => Transform::Void,
    }
}

fn stable_field_name(expr: &IcebergPartitionFieldExpr) -> String {
    match expr {
        IcebergPartitionFieldExpr::Identity { column } => normalize_identifier(column).unwrap_or_else(|_| column.clone()),
        IcebergPartitionFieldExpr::Year { column } => format!("{}_year", normalize_identifier(column).unwrap_or_else(|_| column.clone())),
        IcebergPartitionFieldExpr::Month { column } => format!("{}_month", normalize_identifier(column).unwrap_or_else(|_| column.clone())),
        IcebergPartitionFieldExpr::Day { column } => format!("{}_day", normalize_identifier(column).unwrap_or_else(|_| column.clone())),
        IcebergPartitionFieldExpr::Hour { column } => format!("{}_hour", normalize_identifier(column).unwrap_or_else(|_| column.clone())),
        IcebergPartitionFieldExpr::Bucket { column, num_buckets } => format!("{}_bucket_{}", normalize_identifier(column).unwrap_or_else(|_| column.clone()), num_buckets),
        IcebergPartitionFieldExpr::Truncate { column, width } => format!("{}_truncate_{}", normalize_identifier(column).unwrap_or_else(|_| column.clone()), width),
        IcebergPartitionFieldExpr::Void { column } => format!("{}_void", normalize_identifier(column).unwrap_or_else(|_| column.clone())),
    }
}

fn validate_transform(
    schema: &Schema,
    source_id: i32,
    expr: &IcebergPartitionFieldExpr,
) -> Result<(), String> {
    let field = schema
        .field_by_id(source_id)
        .ok_or_else(|| format!("partition source field id {source_id} is missing"))?;
    let ty = &field.field_type;
    match expr {
        IcebergPartitionFieldExpr::Year { .. }
        | IcebergPartitionFieldExpr::Month { .. }
        | IcebergPartitionFieldExpr::Day { .. }
        | IcebergPartitionFieldExpr::Hour { .. } => {
            if !matches!(
                ty,
                Type::Primitive(iceberg::spec::PrimitiveType::Date)
                    | Type::Primitive(iceberg::spec::PrimitiveType::Timestamp)
                    | Type::Primitive(iceberg::spec::PrimitiveType::Timestamptz)
            ) {
                return Err(format!(
                    "temporal partition transform requires date/timestamp source, got {ty:?}"
                ));
            }
        }
        IcebergPartitionFieldExpr::Bucket { .. }
        | IcebergPartitionFieldExpr::Truncate { .. }
        | IcebergPartitionFieldExpr::Identity { .. }
        | IcebergPartitionFieldExpr::Void { .. } => {
            if !matches!(ty, Type::Primitive(_)) {
                return Err(format!(
                    "iceberg partition transform requires primitive source, got {ty:?}"
                ));
            }
        }
    }
    Ok(())
}
```

Add to `src/connector/iceberg/mod.rs`:

```rust
pub(crate) mod partition_spec;
```

- [ ] **Step 3: Wire CREATE TABLE into `TableCreation`**

Change `src/connector/iceberg/catalog/registry.rs::create_table` signature to accept partition fields:

```rust
pub(crate) fn create_table(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
    columns: &[TableColumnDef],
    key_desc: Option<&TableKeyDesc>,
    partition_fields: &[crate::sql::parser::ast::IcebergPartitionFieldExpr],
    properties: &[(String, String)],
) -> Result<(), String> {
```

Build optional spec before `TableCreation`:

```rust
let schema = build_iceberg_schema(columns)?;
let partition_spec =
    crate::connector::iceberg::partition_spec::build_initial_partition_spec(
        &schema,
        partition_fields,
    )?;
let mut table_creation = TableCreation::builder()
    .name(table_name)
    .schema(schema)
    .properties(all_properties)
    .format_version(format_version);
if let Some(spec) = partition_spec {
    table_creation = table_creation.partition_spec(spec);
}
let table_creation = table_creation.build();
```

Update `src/connector/iceberg/catalog/backend.rs` call:

```rust
reg_create_table(
    &entry,
    &req.namespace,
    &req.table,
    &req.columns,
    req.key_desc.as_ref(),
    &req.partition_fields,
    &req.properties,
)
```

- [ ] **Step 4: Add partition spec unit tests**

In `src/connector/iceberg/partition_spec.rs`, add tests that build a schema using `iceberg::spec::Schema::builder()` and validate transforms:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema};
    use std::sync::Arc;

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).unwrap()),
                Arc::new(NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::Timestamp)).unwrap()),
                Arc::new(NestedField::optional(3, "name", Type::Primitive(PrimitiveType::String)).unwrap()),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn initial_spec_builds_expected_transforms() {
        let spec = build_initial_partition_spec(
            &schema(),
            &[
                IcebergPartitionFieldExpr::Month {
                    column: "ts".to_string(),
                },
                IcebergPartitionFieldExpr::Bucket {
                    column: "id".to_string(),
                    num_buckets: 16,
                },
                IcebergPartitionFieldExpr::Truncate {
                    column: "name".to_string(),
                    width: 8,
                },
            ],
        )
        .unwrap()
        .unwrap()
        .bind(Arc::new(schema()))
        .unwrap();

        assert_eq!(spec.fields().len(), 3);
        assert_eq!(spec.fields()[0].name, "ts_month");
        assert_eq!(spec.fields()[0].transform, Transform::Month);
        assert_eq!(spec.fields()[1].name, "id_bucket_16");
        assert_eq!(spec.fields()[1].transform, Transform::Bucket(16));
        assert_eq!(spec.fields()[2].name, "name_truncate_8");
        assert_eq!(spec.fields()[2].transform, Transform::Truncate(8));
    }

    #[test]
    fn temporal_transform_rejects_non_temporal_source() {
        let err = build_initial_partition_spec(
            &schema(),
            &[IcebergPartitionFieldExpr::Month {
                column: "name".to_string(),
            }],
        )
        .unwrap_err();
        assert!(err.contains("date/timestamp"), "{err}");
    }
}
```

- [ ] **Step 5: Run create-table wiring tests**

Run:

```bash
cargo test --lib connector::iceberg::partition_spec::tests::initial_spec_builds_expected_transforms -- --exact
cargo test --lib connector::iceberg::partition_spec::tests::temporal_transform_rejects_non_temporal_source -- --exact
cargo test --lib sql::parser::dialect::create_table::tests::create_table_parser_preserves_tblproperties -- --exact
```

Expected: all pass.

- [ ] **Step 6: Commit CREATE TABLE partition spec support**

Run:

```bash
git add src/connector/backend.rs src/connector/iceberg/mod.rs src/connector/iceberg/partition_spec.rs src/connector/iceberg/catalog/backend.rs src/connector/iceberg/catalog/registry.rs src/connector/starrocks/managed/backend.rs src/engine/statement.rs
git commit -m "feat: create iceberg tables with partition specs"
```

---

## Task 3: ALTER TABLE ADD/DROP PARTITION COLUMN

**Files:**
- Modify: `src/connector/backend.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`
- Modify: `src/connector/iceberg/catalog/registry.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/engine/statement.rs`

- [ ] **Step 1: Add catalog trait method**

In `src/connector/backend.rs`, extend `CatalogBackend`:

```rust
fn alter_iceberg_partition_spec(
    &self,
    catalog: &str,
    namespace: &str,
    table: &str,
    stmt: crate::sql::parser::ast::AlterIcebergPartitionSpecStmt,
) -> Result<(), String>;
```

For non-Iceberg implementations, return:

```rust
Err(format!(
    "{} backend does not support Iceberg partition evolution DDL",
    self.name()
))
```

- [ ] **Step 2: Implement Iceberg backend method**

In `src/connector/iceberg/catalog/backend.rs`, add:

```rust
fn alter_iceberg_partition_spec(
    &self,
    catalog: &str,
    namespace: &str,
    table: &str,
    stmt: crate::sql::parser::ast::AlterIcebergPartitionSpecStmt,
) -> Result<(), String> {
    let entry = self.entry(catalog)?;
    super::registry::alter_partition_spec(&entry, namespace, table, stmt)
}
```

- [ ] **Step 3: Add registry commit function**

In `src/connector/iceberg/catalog/registry.rs`, add:

```rust
pub(crate) fn alter_partition_spec(
    entry: &IcebergCatalogEntry,
    namespace_name: &str,
    table_name: &str,
    stmt: crate::sql::parser::ast::AlterIcebergPartitionSpecStmt,
) -> Result<(), String> {
    use iceberg::catalog::{TableRequirement, TableUpdate};
    use iceberg::TableCommit;

    let namespace = NamespaceIdent::new(normalize_identifier(namespace_name)?);
    let table_name = normalize_identifier(table_name)?;
    let catalog = build_hadoop_catalog(entry)?;
    let ident = TableIdent::new(namespace, table_name.clone());
    let table = block_on_iceberg(async { catalog.load_table(&ident).await })
        .map_err(|e| format!("load iceberg table runtime failed: {e}"))?
        .map_err(|e| format!("load iceberg table {ident}: {e}"))?;
    let metadata = table.metadata();
    let base_default_spec_id = metadata.default_partition_spec_id();
    let schema = metadata.current_schema();
    let current = metadata.default_partition_spec();
    let change = match &stmt {
        crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn { field, .. } => {
            crate::connector::iceberg::partition_spec::PartitionSpecChange::Add(field)
        }
        crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::DropPartitionColumn { field, .. } => {
            crate::connector::iceberg::partition_spec::PartitionSpecChange::Drop(field)
        }
    };
    let evolved =
        crate::connector::iceberg::partition_spec::build_evolved_partition_spec(
            schema.as_ref(),
            current,
            change,
        )?;

    let commit = TableCommit::builder()
        .ident(ident.clone())
        .requirements(vec![TableRequirement::DefaultSpecIdMatch {
            default_spec_id: base_default_spec_id,
        }])
        .updates(vec![
            TableUpdate::AddSpec { spec: evolved },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ])
        .build();
    block_on_iceberg(async { catalog.update_table(commit).await })
        .map_err(|e| format!("alter iceberg partition spec runtime failed: {e}"))?
        .map_err(|e| format!("alter iceberg partition spec failed: {e}"))?;
    entry.invalidate_table_cache(namespace_name, &table_name);
    Ok(())
}
```

- [ ] **Step 4: Dispatch ALTER from engine**

In `src/engine/mod.rs`, import the new parser helpers from `statement.rs`, then add before `ADD EQUALITY DELETE`:

```rust
if looks_like_alter_partition_column(&normalized) {
    let stmt = crate::engine::statement::parse_alter_partition_column_sql(&normalized)?;
    return self.handle_alter_partition_spec(stmt, current_catalog, current_database);
}
```

Add method:

```rust
fn handle_alter_partition_spec(
    &self,
    stmt: crate::sql::parser::ast::AlterIcebergPartitionSpecStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let table_name = match &stmt {
        crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn { table, .. }
        | crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::DropPartitionColumn { table, .. } => table,
    };
    let target = crate::engine::backend_resolver::resolve_table_target(
        &self.inner,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "ALTER TABLE ADD/DROP PARTITION COLUMN only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    let backend = self
        .inner
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;
    backend.alter_iceberg_partition_spec(
        &target.catalog,
        &target.namespace,
        &target.table,
        stmt,
    )?;
    crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.inner, &target)?;
    Ok(StatementResult::Ok)
}
```

- [ ] **Step 5: Add embedded engine ALTER test**

In `src/engine/mod.rs` tests, add a small standalone test following the local Iceberg create/insert style already used around existing Iceberg tests:

```rust
#[test]
fn iceberg_alter_partition_spec_accepts_add_and_drop() {
    let warehouse = TempDir::new().expect("warehouse tempdir");
    let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
    session
        .execute_in_database(
            r#"create table ice.db1.t_evolved
               (id bigint, ts datetime)
               partition by month(ts)
               tblproperties("format-version"="2")"#,
            "default",
        )
        .expect("create partitioned table");
    session
        .execute_in_database(
            "alter table ice.db1.t_evolved drop partition column month(ts)",
            "default",
        )
        .expect("drop partition column");
    session
        .execute_in_database(
            "alter table ice.db1.t_evolved add partition column bucket(id, 8)",
            "default",
        )
        .expect("add partition column");
}
```

- [ ] **Step 6: Run ALTER validation**

Run:

```bash
cargo test --lib engine::tests::iceberg_alter_partition_spec_accepts_add_and_drop -- --exact
```

Expected: pass.

- [ ] **Step 7: Commit ALTER support**

Run:

```bash
git add src/connector/backend.rs src/connector/iceberg/catalog/backend.rs src/connector/iceberg/catalog/registry.rs src/engine/mod.rs src/engine/statement.rs
git commit -m "feat: alter iceberg partition specs"
```

---

## Task 4: INSERT INTO Multi-Spec Support and Overwrite Guard

**Files:**
- Modify: `src/engine/iceberg_writer.rs`
- Modify: `src/connector/iceberg/commit/validation.rs`

- [ ] **Step 1: Split single-spec validation helpers**

In `src/connector/iceberg/commit/validation.rs`, keep `ensure_single_partition_spec` and add clearer wrappers:

```rust
pub fn ensure_overwrite_single_partition_spec(table: &Table) -> Result<(), String> {
    ensure_single_partition_spec(table).map_err(|err| {
        format!(
            "INSERT OVERWRITE on an evolved Iceberg table is not supported yet: {err}"
        )
    })
}

pub fn ensure_equality_delete_single_partition_spec(table: &Table) -> Result<(), String> {
    ensure_single_partition_spec(table).map_err(|err| {
        format!(
            "ADD EQUALITY DELETE on an evolved Iceberg table is not supported yet: {err}"
        )
    })
}
```

- [ ] **Step 2: Gate only overwrite in `iceberg_writer`**

In `src/engine/iceberg_writer.rs`, change imports:

```rust
use crate::connector::iceberg::commit::{
    ensure_iceberg_write_supported, ensure_no_equality_deletes,
    ensure_overwrite_single_partition_spec, run_iceberg_commit,
};
```

Replace:

```rust
ensure_single_partition_spec(&table)?;
if overwrite {
    ensure_no_equality_deletes(&table)?;
}
```

with:

```rust
if overwrite {
    ensure_overwrite_single_partition_spec(&table)?;
    ensure_no_equality_deletes(&table)?;
}
```

Keep the existing `metadata.default_partition_spec()` collector setup and `default_spec_id` conversion. This makes `INSERT INTO` write using the current default spec, while `INSERT OVERWRITE` remains single-spec only.

- [ ] **Step 3: Keep unsupported path coverage in SQL**

Do not add a fabricated Iceberg table fixture in `validation.rs`. The unsupported `INSERT OVERWRITE` and `ADD EQUALITY DELETE` paths are covered by `sql-tests/iceberg/sql/iceberg_partition_evolution_unsupported.sql` in Task 7, using a real evolved Iceberg table.

- [ ] **Step 4: Run writer tests**

Run:

```bash
cargo test --lib engine::tests::iceberg_insert_overwrite_replaces_rows -- --exact
cargo test --lib engine::tests::iceberg_delete_where_removes_matching_rows -- --exact
```

Expected: existing overwrite/delete tests still pass.

- [ ] **Step 5: Commit writer guard change**

Run:

```bash
git add src/engine/iceberg_writer.rs src/connector/iceberg/commit/validation.rs
git commit -m "feat: allow inserts into evolved iceberg tables"
```

---

## Task 5: Data File Spec Metadata for DELETE Grouping

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs`
- Modify: `src/engine/delete_flow.rs`
- Modify: `src/connector/iceberg/commit/position_delete_writer.rs`

- [ ] **Step 1: Preserve data file partition metadata**

In `src/connector/iceberg/catalog/registry.rs`, extend `DataFileWithStats`:

```rust
pub partition_spec_id: Option<i32>,
pub partition_values: Option<iceberg::spec::Struct>,
```

When pushing data files in `extract_data_files_with_stats`, set:

```rust
partition_spec_id: Some(manifest_file.partition_spec_id),
partition_values: Some(df.partition().clone()),
```

Update all test fixtures constructing `DataFileWithStats` with:

```rust
partition_spec_id: None,
partition_values: None,
```

- [ ] **Step 2: Add delete target metadata map**

In `src/engine/delete_flow.rs`, add:

```rust
#[derive(Clone, Debug)]
struct ReferencedDataFilePartition {
    partition_spec_id: i32,
    partition_values: iceberg::spec::Struct,
}

type ReferencedDataFilePartitions = HashMap<String, ReferencedDataFilePartition>;

fn load_referenced_data_file_partitions(
    table: &iceberg::table::Table,
) -> Result<ReferencedDataFilePartitions, String> {
    let mut out = HashMap::new();
    for data_file in crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(table)? {
        let spec_id = data_file.partition_spec_id.ok_or_else(|| {
            format!("iceberg data file {} missing partition spec id", data_file.path)
        })?;
        let partition_values = data_file.partition_values.ok_or_else(|| {
            format!("iceberg data file {} missing partition values", data_file.path)
        })?;
        out.insert(
            data_file.path,
            ReferencedDataFilePartition {
                partition_spec_id: spec_id,
                partition_values,
            },
        );
    }
    Ok(out)
}
```

- [ ] **Step 3: Extend position delete group**

In `src/connector/iceberg/commit/position_delete_writer.rs`, change `PositionDeleteGroup`:

```rust
pub struct PositionDeleteGroup {
    pub referenced_data_file: String,
    pub partition_spec_id: i32,
    pub partition_values: iceberg::spec::Struct,
    pub positions: Vec<i64>,
}
```

Update existing group construction in tests and callers. In `write_position_delete_files`, remove the function-level `partition_spec_id` argument and use group metadata:

```rust
partition_values: group.partition_values.clone(),
partition_spec_id: group.partition_spec_id,
```

- [ ] **Step 4: Build groups with inherited spec metadata**

In `src/engine/delete_flow.rs::scan_for_position_deletes`, pass `ReferencedDataFilePartitions` and construct groups:

```rust
Ok(by_file
    .into_iter()
    .map(|(referenced_data_file, positions)| {
        let partition = referenced_partitions.get(&referenced_data_file).ok_or_else(|| {
            format!(
                "DELETE matched data file `{referenced_data_file}` but its partition spec metadata is missing"
            )
        })?;
        Ok(PositionDeleteGroup {
            referenced_data_file,
            partition_spec_id: partition.partition_spec_id,
            partition_values: partition.partition_values.clone(),
            positions,
        })
    })
    .collect::<Result<Vec<_>, String>>()?)
```

Call `load_referenced_data_file_partitions(&table)?` before the scan.

- [ ] **Step 5: Run focused compile tests**

Run:

```bash
cargo test --lib connector::iceberg::commit::position_delete_writer::tests::schema_has_reserved_field_ids -- --exact
cargo test --lib engine::delete_flow::tests::position_delete_collection_skips_rows_hidden_by_position_deletes -- --exact
```

Expected: pass after all group constructors are updated.

- [ ] **Step 6: Commit data file metadata threading**

Run:

```bash
git add src/connector/iceberg/catalog/registry.rs src/engine/delete_flow.rs src/connector/iceberg/commit/position_delete_writer.rs
git commit -m "feat: track iceberg partition spec per delete target"
```

---

## Task 6: RowDelta Delete Manifests per Partition Spec

**Files:**
- Modify: `src/connector/iceberg/commit/row_delta.rs`
- Modify: `src/engine/delete_flow.rs`

- [ ] **Step 1: Group written delete files by spec id**

In `src/connector/iceberg/commit/row_delta.rs`, add helper:

```rust
fn group_written_by_spec(written: Vec<WrittenFile>) -> BTreeMap<i32, Vec<WrittenFile>> {
    let mut grouped = BTreeMap::new();
    for file in written {
        grouped
            .entry(file.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(file);
    }
    grouped
}
```

Import `std::collections::BTreeMap`.

- [ ] **Step 2: Write one delete manifest per spec**

In `RowDeltaTxnAction::commit`, replace the single `write_delete_manifest` call with:

```rust
let mut new_delete_manifests = Vec::new();
for (idx, (spec_id, files)) in group_written_by_spec(self.written.clone())
    .into_iter()
    .enumerate()
{
    let spec = m
        .partition_spec_by_id(spec_id)
        .cloned()
        .ok_or_else(|| {
            to_iceberg_unexpected(format!(
                "RowDelta delete file references unknown partition spec id {spec_id}"
            ))
        })?;
    let delete_manifest_path = format!(
        "{metadata_dir}/{}-row-delta-deletes-{idx}.avro",
        self.commit_uuid
    );
    self.abort_handle.record_manifest(delete_manifest_path.clone());
    self.manifest_paths_out
        .lock()
        .expect("manifest_paths_out poisoned")
        .push(delete_manifest_path.clone());
    let manifest = write_delete_manifest(
        &self.file_io,
        &delete_manifest_path,
        &files,
        spec,
        m.current_schema().clone(),
        new_seq,
        new_snapshot_id,
        format_version,
    )
    .await
    .map_err(to_iceberg_unexpected)?;
    new_delete_manifests.push(manifest);
}
```

Then push all manifests into base entries:

```rust
let mut entries = read_base_manifest_list(table, &self.file_io)
    .await
    .map_err(to_iceberg_unexpected)?;
entries.extend(new_delete_manifests);
```

- [ ] **Step 3: Keep DV multi-spec explicitly guarded**

In `src/engine/delete_flow.rs`, after `let delete_strategy = classify_sql_delete_strategy(&table)?;`, replace unconditional `ensure_single_partition_spec(&table)?` with:

```rust
if matches!(delete_strategy, IcebergSqlDeleteStrategy::DeletionVectors)
    && crate::connector::iceberg::partition_spec::spec_count(&table) > 1
{
    return Err(
        "DELETE with Iceberg v3 deletion vectors on evolved partition specs is not supported yet; use v2 position-delete tables for partition-evolution DELETE coverage"
            .to_string(),
    );
}
```

Do not call `ensure_single_partition_spec` for v2 position-delete DELETE.

- [ ] **Step 4: Add a row-delta grouping unit test**

In `src/connector/iceberg/commit/row_delta.rs` tests:

```rust
fn test_written_position_delete(path: &str) -> WrittenFile {
    WrittenFile {
        path: path.to_string(),
        format: iceberg::spec::DataFileFormat::Parquet,
        content: iceberg::spec::DataContentType::PositionDeletes,
        partition_values: iceberg::spec::Struct::empty(),
        partition_spec_id: 0,
        record_count: 1,
        file_size_in_bytes: 256,
        split_offsets: vec![],
        column_sizes: Default::default(),
        value_counts: Default::default(),
        null_value_counts: Default::default(),
        key_metadata: None,
        referenced_data_file: Some("s3://bucket/data.parquet".to_string()),
        equality_ids: None,
    }
}

#[test]
fn row_delta_groups_written_delete_files_by_partition_spec() {
    let mut a = test_written_position_delete("s3://bucket/delete-a.parquet");
    a.partition_spec_id = 0;
    let mut b = test_written_position_delete("s3://bucket/delete-b.parquet");
    b.partition_spec_id = 7;

    let grouped = group_written_by_spec(vec![b.clone(), a.clone()]);
    assert_eq!(grouped.keys().copied().collect::<Vec<_>>(), vec![0, 7]);
    assert_eq!(grouped.get(&0).unwrap()[0].path, a.path);
    assert_eq!(grouped.get(&7).unwrap()[0].path, b.path);
}
```

- [ ] **Step 5: Run row-delta tests**

Run:

```bash
cargo test --lib connector::iceberg::commit::row_delta::tests::row_delta_groups_written_delete_files_by_partition_spec -- --exact
cargo test --lib engine::tests::iceberg_delete_where_removes_matching_rows -- --exact
```

Expected: pass.

- [ ] **Step 6: Commit RowDelta multi-spec delete manifest support**

Run:

```bash
git add src/connector/iceberg/commit/row_delta.rs src/engine/delete_flow.rs
git commit -m "feat: commit position deletes under referenced partition specs"
```

---

## Task 7: SQL Tests for DDL, SELECT, INSERT, DELETE, and Unsupported Writes

**Files:**
- Modify: `sql-tests/iceberg/sql/iceberg_partition_evolution_1.sql`
- Modify: `sql-tests/iceberg/sql/iceberg_partition_evolution_replace.sql`
- Create: `sql-tests/iceberg/sql/iceberg_partition_evolution_delete.sql`
- Create: `sql-tests/iceberg/sql/iceberg_partition_evolution_unsupported.sql`
- Create or update result files under `sql-tests/iceberg/result/`

- [ ] **Step 1: Remove explain partition-ratio assertions**

In `iceberg_partition_evolution_1.sql`, remove `EXPLAIN VERBOSE` statements that assert `partitions=x/y`. Keep only result-driven assertions:

```sql
SELECT COUNT(*) FROM ${case_db}.test_users_bucketed;
SELECT SUM(score) FROM ${case_db}.test_users_bucketed WHERE user_id IN (1, 17, 33, 49);
SELECT user_id, SUM(score)
FROM ${case_db}.test_users_bucketed
GROUP BY user_id
ORDER BY user_id;
```

- [ ] **Step 2: Add DELETE SQL case**

Create `sql-tests/iceberg/sql/iceberg_partition_evolution_delete.sql`:

```sql
-- Test Point: Iceberg DELETE works across historical partition specs
-- Method: create a bucket-partitioned table, insert old-spec rows, evolve to a new bucket spec, insert new-spec rows, delete rows from both specs, and verify remaining rows
-- Scope: standalone Iceberg table DDL, INSERT INTO, SELECT, DELETE FROM

DROP TABLE IF EXISTS ${case_db}.t_partition_evolution_delete FORCE;

CREATE TABLE ${case_db}.t_partition_evolution_delete (
    id BIGINT,
    user_id BIGINT,
    score INT
) PARTITION BY bucket(user_id, 4)
TBLPROPERTIES ("format-version" = "2");

INSERT INTO ${case_db}.t_partition_evolution_delete VALUES
    (1, 10, 100),
    (2, 20, 200),
    (3, 30, 300);

ALTER TABLE ${case_db}.t_partition_evolution_delete DROP PARTITION COLUMN bucket(user_id, 4);
ALTER TABLE ${case_db}.t_partition_evolution_delete ADD PARTITION COLUMN bucket(user_id, 8);

INSERT INTO ${case_db}.t_partition_evolution_delete VALUES
    (4, 40, 400),
    (5, 50, 500),
    (6, 60, 600);

DELETE FROM ${case_db}.t_partition_evolution_delete WHERE id IN (2, 5);

SELECT id, user_id, score
FROM ${case_db}.t_partition_evolution_delete
ORDER BY id;

DROP TABLE IF EXISTS ${case_db}.t_partition_evolution_delete FORCE;
```

Expected result rows:

```text
1	10	100
3	30	300
4	40	400
6	60	600
```

- [ ] **Step 3: Add unsupported SQL case**

Create `sql-tests/iceberg/sql/iceberg_partition_evolution_unsupported.sql`:

```sql
-- Test Point: Unsupported writes on evolved Iceberg partition specs fail fast
-- Method: evolve a partition spec, then verify INSERT OVERWRITE and ADD EQUALITY DELETE return clear unsupported errors
-- Scope: standalone Iceberg table DDL, unsupported write guards

DROP TABLE IF EXISTS ${case_db}.t_partition_evolution_unsupported FORCE;

CREATE TABLE ${case_db}.t_partition_evolution_unsupported (
    id BIGINT,
    user_id BIGINT,
    score INT
) PARTITION BY bucket(user_id, 4)
TBLPROPERTIES ("format-version" = "2");

INSERT INTO ${case_db}.t_partition_evolution_unsupported VALUES (1, 10, 100);

ALTER TABLE ${case_db}.t_partition_evolution_unsupported DROP PARTITION COLUMN bucket(user_id, 4);
ALTER TABLE ${case_db}.t_partition_evolution_unsupported ADD PARTITION COLUMN bucket(user_id, 8);

-- @expect_error=INSERT OVERWRITE on an evolved Iceberg table is not supported yet
INSERT OVERWRITE ${case_db}.t_partition_evolution_unsupported SELECT * FROM ${case_db}.t_partition_evolution_unsupported;

-- @expect_error=ADD EQUALITY DELETE on an evolved Iceberg table is not supported yet
ALTER TABLE ${case_db}.t_partition_evolution_unsupported ADD EQUALITY DELETE (id) VALUES (1);

DROP TABLE IF EXISTS ${case_db}.t_partition_evolution_unsupported FORCE;
```

- [ ] **Step 4: Run SQL cases in record mode**

Start the local standalone debug server with the MinIO test config in a persistent terminal:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config tests/sql-test-runner/conf/standalone_managed_lake.conf --port 9030
```

Then run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --only iceberg_partition_evolution_1,iceberg_partition_evolution_replace,iceberg_partition_evolution_delete,iceberg_partition_evolution_unsupported \
  --mode record \
  --query-timeout 120
```

Expected: result files are created or updated for the four cases.

- [ ] **Step 5: Re-run SQL cases in verify mode**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --only iceberg_partition_evolution_1,iceberg_partition_evolution_replace,iceberg_partition_evolution_delete,iceberg_partition_evolution_unsupported \
  --mode verify \
  --query-timeout 120
```

Expected: all four cases pass.

- [ ] **Step 6: Commit SQL coverage**

Run:

```bash
git add sql-tests/iceberg/sql/iceberg_partition_evolution_1.sql sql-tests/iceberg/sql/iceberg_partition_evolution_replace.sql sql-tests/iceberg/sql/iceberg_partition_evolution_delete.sql sql-tests/iceberg/sql/iceberg_partition_evolution_unsupported.sql sql-tests/iceberg/result
git commit -m "test: cover iceberg partition evolution"
```

---

## Task 8: Full Verification and Cleanup

**Files:**
- All touched files from Tasks 1-7

- [ ] **Step 1: Run formatter**

Run:

```bash
cargo fmt
```

Expected: command exits 0.

- [ ] **Step 2: Run focused Rust tests**

Run:

```bash
cargo test --lib sql::parser::dialect::create_table::tests::create_table_parser_preserves_partition_transforms -- --exact
cargo test --lib sql::parser::dialect::create_table::tests::create_table_parser_rejects_invalid_partition_transform_args -- --exact
cargo test --lib engine::statement::tests::parse_alter_partition_column_statement -- --exact
cargo test --lib connector::iceberg::partition_spec::tests::initial_spec_builds_expected_transforms -- --exact
cargo test --lib connector::iceberg::partition_spec::tests::temporal_transform_rejects_non_temporal_source -- --exact
cargo test --lib connector::iceberg::commit::row_delta::tests::row_delta_groups_written_delete_files_by_partition_spec -- --exact
cargo test --lib engine::tests::iceberg_alter_partition_spec_accepts_add_and_drop -- --exact
cargo test --lib engine::tests::iceberg_delete_where_removes_matching_rows -- --exact
```

Expected: every command reports `test result: ok`.

- [ ] **Step 3: Run SQL verification**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --only iceberg_partition_evolution_1,iceberg_partition_evolution_replace,iceberg_partition_evolution_delete,iceberg_partition_evolution_unsupported \
  --mode verify \
  --query-timeout 120
```

Expected: all selected cases pass.

- [ ] **Step 4: Run final static checks**

Run:

```bash
cargo fmt --check
git diff --check
git status --short
```

Expected:

```text
cargo fmt --check exits 0
git diff --check exits 0
git status --short shows only intentional source, SQL, result, and docs changes before the final commit
```

- [ ] **Step 5: Final commit**

If formatter or SQL recording changed files after Task 7, commit those changes:

```bash
git add src sql-tests docs/design/plans/2026-05-04-iceberg-partition-evolution.md
git commit -m "chore: verify iceberg partition evolution support"
```

If there are no changes, do not create an empty commit.

---

## Execution Notes

- Debug build is sufficient for this task. Use `cargo build` / `cargo run` without `--release` for local server validation.
- Keep `INSERT OVERWRITE`, multi-spec equality delete, partition pruning, MV refresh, and schema evolution outside this implementation.
- Error messages in code must be English and specific enough to identify the source column, transform, backend, or unsupported write mode.
- User-facing summaries and docs stay in Chinese.
