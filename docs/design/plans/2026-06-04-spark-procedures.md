# Spark Procedures Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Spark-style Iceberg system procedures to NovaRocks, route legacy maintenance statements through a unified action layer, and implement V3 Puffin DV repack for `rewrite_position_delete_files`.

**Architecture:** Add a focused procedure parser and a unified `MaintenanceActionRequest` / `MaintenanceActionOutcome` layer in `src/engine/`. Spark `CALL` and legacy `ALTER TABLE ...` both normalize into that action layer, while connector-level Iceberg commit modules own manifest, Puffin, and DV rewrite semantics.

**Tech Stack:** Rust, sqlparser-rs token parser, Arrow `RecordBatch`/`QueryResult`, iceberg-rs catalog/manifest APIs, vendor Puffin reader/writer, NovaRocks SQL test runner.

---

## Scope Check

The approved spec is one subsystem: standalone Iceberg maintenance procedures. It does not need to be split into separate specs because every task works toward the same SQL surface and action execution path.

First implementation scope:

- Implement `CALL <catalog>.system.<procedure>(...)`.
- Implement unified maintenance action request/outcome and Spark result conversion.
- Route legacy `ALTER TABLE ...` maintenance commands through the unified action layer.
- Expose Spark-compatible procedures for `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`, `rewrite_data_files`, and `rewrite_position_delete_files`.
- Implement only V3 Puffin DV repack for `rewrite_position_delete_files`.
- Reject V2 Parquet position delete rewrite and non-empty `where`.

## File Structure

- Create `src/engine/procedure.rs`
  - Parse and validate Spark-style `CALL`.
  - Normalize named and positional procedure arguments.
  - Convert procedure calls into typed procedure requests.

- Create `src/engine/iceberg_maintenance.rs`
  - Define `MaintenanceActionKind`, `MaintenanceActionSource`, `MaintenanceActionRequest`, and `MaintenanceActionOutcome`.
  - Resolve Iceberg targets.
  - Dispatch action execution.
  - Convert action outcomes into Spark-compatible `QueryResult`s.

- Create `src/connector/iceberg/commit/rewrite_position_delete_files.rs`
  - Load current delete manifests.
  - Reject V2 Parquet position deletes.
  - Plan V3 Puffin DV repack groups.
  - Commit replacement delete manifests.

- Modify `src/connector/iceberg/commit/puffin_dv.rs`
  - Add multi-blob Puffin DV writer.
  - Keep existing single-blob writer for row-delta DELETE.
  - Keep reader offset/length based.

- Modify `src/connector/iceberg/commit/mod.rs`
  - Export `rewrite_position_delete_files` module and multi-blob Puffin helpers.

- Modify `src/engine/mod.rs`
  - Register new modules.
  - Route `CALL` before generic parser fallback.
  - Update existing legacy maintenance handlers to build `MaintenanceActionRequest`.

- Modify `src/engine/iceberg_rewrite_manifests.rs`, `src/engine/iceberg_expire_snapshots.rs`, `src/engine/iceberg_remove_orphan_files.rs`
  - Convert these wrappers to call `iceberg_maintenance` or remove direct use once all callers are migrated.

- Add SQL cases:
  - `sql-tests/iceberg/sql/iceberg_spark_procedures_basic.sql`
  - `sql-tests/iceberg/sql/iceberg_v3_rewrite_position_delete_files.sql`
  - `sql-tests/iceberg/sql/iceberg_spark_procedures_errors.sql`

---

### Task 1: Spark `CALL` Parser

**Files:**
- Create: `src/engine/procedure.rs`
- Modify: `src/engine/mod.rs`
- Test: `src/engine/procedure.rs`

- [ ] **Step 1: Write failing parser tests**

Add `src/engine/procedure.rs` with the test module first. The file will not compile until Step 3 adds the types and parser.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_call_detects_system_procedure() {
        assert!(looks_like_call_procedure(
            "CALL ice.system.rewrite_manifests(table => 'db.t')"
        ));
        assert!(!looks_like_call_procedure("SELECT 1"));
    }

    #[test]
    fn parse_named_arguments() {
        let stmt = parse_call_procedure_sql(
            "CALL ice.system.rewrite_position_delete_files(table => 'db.t', options => map('rewrite-all', 'true'))",
        )
        .unwrap();
        assert_eq!(stmt.catalog, "ice");
        assert_eq!(stmt.namespace, "system");
        assert_eq!(stmt.procedure, "rewrite_position_delete_files");
        assert_eq!(stmt.args.len(), 2);
        assert!(matches!(stmt.mode, ProcedureArgMode::Named));
        assert_eq!(stmt.arg("table").unwrap().as_string().unwrap(), "db.t");
        assert_eq!(
            stmt.arg("options")
                .unwrap()
                .as_string_map()
                .unwrap()
                .get("rewrite-all")
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn parse_positional_arguments() {
        let stmt =
            parse_call_procedure_sql("CALL ice.system.rewrite_manifests('db.t', false)").unwrap();
        assert_eq!(stmt.catalog, "ice");
        assert_eq!(stmt.procedure, "rewrite_manifests");
        assert!(matches!(stmt.mode, ProcedureArgMode::Positional));
        assert_eq!(stmt.args[0].name, None);
        assert_eq!(stmt.args[0].value.as_string().unwrap(), "db.t");
        assert_eq!(stmt.args[1].value.as_bool().unwrap(), false);
    }

    #[test]
    fn rejects_mixed_named_and_positional_arguments() {
        let err = parse_call_procedure_sql(
            "CALL ice.system.rewrite_manifests('db.t', use_caching => false)",
        )
        .unwrap_err();
        assert!(err.contains("cannot mix positional and named arguments"));
    }

    #[test]
    fn rejects_non_system_namespace() {
        let err = parse_call_procedure_sql("CALL ice.admin.rewrite_manifests(table => 'db.t')")
            .unwrap_err();
        assert!(err.contains("Iceberg procedures must use system namespace"));
    }
}
```

- [ ] **Step 2: Run parser tests and verify failure**

Run:

```bash
cargo test --lib procedure::tests::looks_like_call_detects_system_procedure
```

Expected: compile failure mentioning unresolved `looks_like_call_procedure` or missing `procedure` module.

- [ ] **Step 3: Implement parser module**

Add these definitions in `src/engine/procedure.rs`. Keep parsing strict and normalize identifiers with `normalize_identifier`.

```rust
use std::collections::BTreeMap;

use crate::engine::catalog::normalize_identifier;
use crate::sql::parser::dialect::StarRocksDialect;
use sqlparser::ast::ObjectName;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProcedureArgMode {
    Named,
    Positional,
    Empty,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ProcedureArgValue {
    String(String),
    Boolean(bool),
    Integer(i64),
    TimestampMillis(i64),
    StringMap(BTreeMap<String, String>),
    Null,
}

impl ProcedureArgValue {
    pub(crate) fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(value) => Some(*value),
            _ => None,
        }
    }

    pub(crate) fn as_string_map(&self) -> Option<&BTreeMap<String, String>> {
        match self {
            Self::StringMap(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProcedureArg {
    pub(crate) name: Option<String>,
    pub(crate) value: ProcedureArgValue,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CallProcedureStmt {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) procedure: String,
    pub(crate) args: Vec<ProcedureArg>,
    pub(crate) mode: ProcedureArgMode,
}

impl CallProcedureStmt {
    pub(crate) fn arg(&self, name: &str) -> Option<&ProcedureArgValue> {
        let name = name.to_ascii_lowercase();
        self.args
            .iter()
            .find(|arg| arg.name.as_deref() == Some(name.as_str()))
            .map(|arg| &arg.value)
    }
}

pub(crate) fn looks_like_call_procedure(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    parser.parse_keyword(Keyword::CALL)
}

pub(crate) fn parse_call_procedure_sql(sql: &str) -> Result<CallProcedureStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse CALL procedure: {e}"))?;
    parser
        .expect_keyword(Keyword::CALL)
        .map_err(|e| format!("parse CALL procedure: {e}"))?;
    let object = parser
        .parse_object_name(false)
        .map_err(|e| format!("parse CALL procedure name: {e}"))?;
    let parts = normalize_object_name(object)?;
    if parts.len() != 3 {
        return Err(
            "CALL procedure name must be <catalog>.system.<procedure>".to_string(),
        );
    }
    if parts[1] != "system" {
        return Err("Iceberg procedures must use system namespace".to_string());
    }
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("parse CALL arguments: {e}"))?;
    let args = parse_arg_list(&mut parser)?;
    parser
        .expect_token(&Token::RParen)
        .map_err(|e| format!("parse CALL arguments: {e}"))?;
    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser)?;
    let mode = classify_arg_mode(&args)?;
    ensure_no_duplicate_named_args(&args)?;
    Ok(CallProcedureStmt {
        catalog: parts[0].clone(),
        namespace: parts[1].clone(),
        procedure: parts[2].clone(),
        args,
        mode,
    })
}
```

Implement helpers in the same file:

```rust
fn normalize_object_name(name: ObjectName) -> Result<Vec<String>, String> {
    name.0
        .into_iter()
        .map(|ident| normalize_identifier(&ident.value))
        .collect()
}

fn classify_arg_mode(args: &[ProcedureArg]) -> Result<ProcedureArgMode, String> {
    let has_named = args.iter().any(|arg| arg.name.is_some());
    let has_positional = args.iter().any(|arg| arg.name.is_none());
    match (has_named, has_positional) {
        (false, false) => Ok(ProcedureArgMode::Empty),
        (true, false) => Ok(ProcedureArgMode::Named),
        (false, true) => Ok(ProcedureArgMode::Positional),
        (true, true) => Err("CALL procedure cannot mix positional and named arguments".to_string()),
    }
}

fn ensure_no_duplicate_named_args(args: &[ProcedureArg]) -> Result<(), String> {
    let mut seen = std::collections::BTreeSet::new();
    for arg in args {
        if let Some(name) = &arg.name {
            if !seen.insert(name.clone()) {
                return Err(format!("CALL procedure duplicate argument `{name}`"));
            }
        }
    }
    Ok(())
}
```

For argument parsing, implement these exact accepted forms:

```rust
fn parse_arg_list(parser: &mut Parser<'_>) -> Result<Vec<ProcedureArg>, String> {
    let mut args = Vec::new();
    if matches!(parser.peek_token().token, Token::RParen) {
        return Ok(args);
    }
    loop {
        args.push(parse_arg(parser)?);
        if matches!(parser.peek_token().token, Token::Comma) {
            parser.next_token();
            continue;
        }
        break;
    }
    Ok(args)
}

fn parse_arg(parser: &mut Parser<'_>) -> Result<ProcedureArg, String> {
    if let Token::Word(word) = parser.peek_token().token.clone() {
        if token_at_is_fat_arrow(parser, 1) {
            parser.next_token();
            consume_fat_arrow(parser)?;
            let name = normalize_identifier(&word.value)?;
            let value = parse_arg_value(parser)?;
            return Ok(ProcedureArg {
                name: Some(name),
                value,
            });
        }
    }
    Ok(ProcedureArg {
        name: None,
        value: parse_arg_value(parser)?,
    })
}

fn parse_arg_value(parser: &mut Parser<'_>) -> Result<ProcedureArgValue, String> {
    match parser.next_token().token {
        Token::SingleQuotedString(value) => Ok(ProcedureArgValue::String(value)),
        Token::Number(value, _) => value
            .parse::<i64>()
            .map(ProcedureArgValue::Integer)
            .map_err(|e| format!("CALL procedure integer parse failed: {e}")),
        Token::Word(word) if word.value.eq_ignore_ascii_case("true") => {
            Ok(ProcedureArgValue::Boolean(true))
        }
        Token::Word(word) if word.value.eq_ignore_ascii_case("false") => {
            Ok(ProcedureArgValue::Boolean(false))
        }
        Token::Word(word) if word.value.eq_ignore_ascii_case("null") => Ok(ProcedureArgValue::Null),
        Token::Word(word) if word.value.eq_ignore_ascii_case("map") => parse_string_map(parser),
        Token::Word(word) if word.value.eq_ignore_ascii_case("timestamp") => {
            parse_timestamp_literal(parser)
        }
        other => Err(format!("unsupported CALL procedure argument value: {other}")),
    }
}
```

Use the local sqlparser API that already appears in `src/engine/statement.rs`: `parser.peek_nth_token_ref(offset)`. Implement `token_at_is_fat_arrow` and `consume_fat_arrow` as explicit token helpers:

```rust
fn token_at_is_fat_arrow(parser: &Parser<'_>, offset: usize) -> bool {
    matches!(parser.peek_nth_token_ref(offset).token, Token::RArrow)
}

fn consume_fat_arrow(parser: &mut Parser<'_>) -> Result<(), String> {
    match parser.next_token().token {
        Token::RArrow => Ok(()),
        other => Err(format!("CALL procedure expected =>, got {other}")),
    }
}
```

- [ ] **Step 4: Register module and run tests**

Modify `src/engine/mod.rs` module declarations:

```rust
pub(crate) mod procedure;
```

Run:

```bash
cargo test --lib procedure::tests
```

Expected: all procedure parser tests pass.

- [ ] **Step 5: Commit parser**

```bash
git add src/engine/procedure.rs src/engine/mod.rs
git commit -m "feat: parse Spark-style Iceberg procedures"
```

---

### Task 2: Maintenance Action Types and Spark Result Builder

**Files:**
- Create: `src/engine/iceberg_maintenance.rs`
- Modify: `src/engine/mod.rs`
- Test: `src/engine/iceberg_maintenance.rs`

- [ ] **Step 1: Write failing action/result tests**

Create `src/engine/iceberg_maintenance.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_position_delete_files_schema_matches_spark() {
        let outcome = MaintenanceActionOutcome::RewritePositionDeleteFiles {
            rewritten_delete_files_count: 2,
            added_delete_files_count: 1,
            rewritten_bytes_count: 128,
            added_bytes_count: 96,
        };
        let result = outcome.to_spark_query_result().unwrap();
        let names = result
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "rewritten_delete_files_count",
                "added_delete_files_count",
                "rewritten_bytes_count",
                "added_bytes_count"
            ]
        );
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn remove_orphan_files_returns_one_row_per_location() {
        let outcome = MaintenanceActionOutcome::RemoveOrphanFiles {
            orphan_file_locations: vec![
                "s3://bucket/table/data/a.parquet".to_string(),
                "s3://bucket/table/metadata/old.avro".to_string(),
            ],
        };
        let result = outcome.to_spark_query_result().unwrap();
        assert_eq!(result.columns[0].name, "orphan_file_location");
        assert_eq!(result.row_count(), 2);
    }
}
```

- [ ] **Step 2: Run result tests and verify failure**

Run:

```bash
cargo test --lib iceberg_maintenance::tests::rewrite_position_delete_files_schema_matches_spark
```

Expected: compile failure because `iceberg_maintenance` is not registered.

- [ ] **Step 3: Implement action types**

Add the core types:

```rust
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::engine::{QueryResult, QueryResultColumn, StandaloneState, StatementResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceActionSource {
    SparkProcedure,
    LegacyAlter,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceActionKind {
    RewriteDataFiles,
    RewriteManifests,
    ExpireSnapshots,
    RemoveOrphanFiles,
    RewritePositionDeleteFiles,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MaintenanceActionOptions {
    pub(crate) values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MaintenanceActionRequest {
    pub(crate) source: MaintenanceActionSource,
    pub(crate) kind: MaintenanceActionKind,
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) options: MaintenanceActionOptions,
    pub(crate) older_than_ms: Option<i64>,
    pub(crate) retain_last: Option<u32>,
    pub(crate) use_caching: Option<bool>,
    pub(crate) spec_id: Option<i32>,
    pub(crate) branch: Option<String>,
    pub(crate) where_clause: Option<String>,
}
```

Add outcome variants:

```rust
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MaintenanceActionOutcome {
    RewriteManifests {
        rewritten_manifests_count: i32,
        added_manifests_count: i32,
    },
    ExpireSnapshots {
        deleted_data_files_count: Option<i64>,
        deleted_position_delete_files_count: Option<i64>,
        deleted_equality_delete_files_count: Option<i64>,
        deleted_manifest_files_count: Option<i64>,
        deleted_manifest_lists_count: Option<i64>,
        deleted_statistics_files_count: Option<i64>,
    },
    RemoveOrphanFiles {
        orphan_file_locations: Vec<String>,
    },
    RewriteDataFiles {
        rewritten_data_files_count: i32,
        added_data_files_count: i32,
        rewritten_bytes_count: i64,
        failed_data_files_count: i32,
        removed_delete_files_count: i32,
    },
    RewritePositionDeleteFiles {
        rewritten_delete_files_count: i32,
        added_delete_files_count: i32,
        rewritten_bytes_count: i64,
        added_bytes_count: i64,
    },
}
```

- [ ] **Step 4: Implement Spark result conversion**

Add helper builders in the same file:

```rust
impl MaintenanceActionOutcome {
    pub(crate) fn to_spark_query_result(&self) -> Result<QueryResult, String> {
        match self {
            Self::RewriteManifests {
                rewritten_manifests_count,
                added_manifests_count,
            } => build_i32_i32_result(
                vec!["rewritten_manifests_count", "added_manifests_count"],
                vec![*rewritten_manifests_count, *added_manifests_count],
            ),
            Self::ExpireSnapshots {
                deleted_data_files_count,
                deleted_position_delete_files_count,
                deleted_equality_delete_files_count,
                deleted_manifest_files_count,
                deleted_manifest_lists_count,
                deleted_statistics_files_count,
            } => build_nullable_i64_result(
                vec![
                    "deleted_data_files_count",
                    "deleted_position_delete_files_count",
                    "deleted_equality_delete_files_count",
                    "deleted_manifest_files_count",
                    "deleted_manifest_lists_count",
                    "deleted_statistics_files_count",
                ],
                vec![
                    *deleted_data_files_count,
                    *deleted_position_delete_files_count,
                    *deleted_equality_delete_files_count,
                    *deleted_manifest_files_count,
                    *deleted_manifest_lists_count,
                    *deleted_statistics_files_count,
                ],
            ),
            Self::RemoveOrphanFiles {
                orphan_file_locations,
            } => build_string_rows_result("orphan_file_location", orphan_file_locations),
            Self::RewriteDataFiles {
                rewritten_data_files_count,
                added_data_files_count,
                rewritten_bytes_count,
                failed_data_files_count,
                removed_delete_files_count,
            } => build_rewrite_data_files_result(
                *rewritten_data_files_count,
                *added_data_files_count,
                *rewritten_bytes_count,
                *failed_data_files_count,
                *removed_delete_files_count,
            ),
            Self::RewritePositionDeleteFiles {
                rewritten_delete_files_count,
                added_delete_files_count,
                rewritten_bytes_count,
                added_bytes_count,
            } => build_rewrite_position_delete_files_result(
                *rewritten_delete_files_count,
                *added_delete_files_count,
                *rewritten_bytes_count,
                *added_bytes_count,
            ),
        }
    }
}
```

Implement the array builders with `crate::engine::record_batch_to_chunk(batch)?`. For `Int32Array` and `Int64Array`, use one-row arrays. For nullable expire columns, use `Int64Array::from(Vec<Option<i64>>)`.

- [ ] **Step 5: Register module and run tests**

Modify `src/engine/mod.rs`:

```rust
pub(crate) mod iceberg_maintenance;
```

Run:

```bash
cargo test --lib iceberg_maintenance::tests
```

Expected: all action/result tests pass.

- [ ] **Step 6: Commit action model**

```bash
git add src/engine/iceberg_maintenance.rs src/engine/mod.rs
git commit -m "feat: add Iceberg maintenance action model"
```

---

### Task 3: Route `CALL` and Existing Maintenance Procedures Through Unified Actions

**Files:**
- Modify: `src/engine/procedure.rs`
- Modify: `src/engine/iceberg_maintenance.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/engine/iceberg_rewrite_manifests.rs`
- Modify: `src/engine/iceberg_expire_snapshots.rs`
- Modify: `src/engine/iceberg_remove_orphan_files.rs`
- Test: `src/engine/procedure.rs`
- Test: `src/engine/iceberg_maintenance.rs`

- [ ] **Step 1: Write failing normalization tests**

Add tests that convert `CallProcedureStmt` to `MaintenanceActionRequest`:

```rust
#[test]
fn named_rewrite_manifests_to_action_request() {
    let stmt =
        parse_call_procedure_sql("CALL ice.system.rewrite_manifests(table => 'db.t')").unwrap();
    let req = crate::engine::iceberg_maintenance::MaintenanceActionRequest::from_call(&stmt, "db")
        .unwrap();
    assert_eq!(req.catalog, "ice");
    assert_eq!(req.namespace, "db");
    assert_eq!(req.table, "t");
    assert_eq!(
        req.kind,
        crate::engine::iceberg_maintenance::MaintenanceActionKind::RewriteManifests
    );
}

#[test]
fn positional_rewrite_manifests_to_action_request() {
    let stmt = parse_call_procedure_sql("CALL ice.system.rewrite_manifests('db.t', false)").unwrap();
    let req = crate::engine::iceberg_maintenance::MaintenanceActionRequest::from_call(&stmt, "db")
        .unwrap();
    assert_eq!(req.use_caching, Some(false));
}

#[test]
fn unknown_procedure_rejected() {
    let stmt = parse_call_procedure_sql("CALL ice.system.unknown_proc(table => 'db.t')").unwrap();
    let err = crate::engine::iceberg_maintenance::MaintenanceActionRequest::from_call(&stmt, "db")
        .unwrap_err();
    assert!(err.contains("unsupported Iceberg system procedure"));
}
```

- [ ] **Step 2: Run normalization tests and verify failure**

Run:

```bash
cargo test --lib named_rewrite_manifests_to_action_request positional_rewrite_manifests_to_action_request unknown_procedure_rejected
```

Expected: compile failure because `from_call` is missing.

- [ ] **Step 3: Implement request normalization**

Add `MaintenanceActionRequest::from_call`:

```rust
impl MaintenanceActionRequest {
    pub(crate) fn from_call(
        stmt: &crate::engine::procedure::CallProcedureStmt,
        current_database: &str,
    ) -> Result<Self, String> {
        let kind = match stmt.procedure.as_str() {
            "rewrite_data_files" => MaintenanceActionKind::RewriteDataFiles,
            "rewrite_manifests" => MaintenanceActionKind::RewriteManifests,
            "expire_snapshots" => MaintenanceActionKind::ExpireSnapshots,
            "remove_orphan_files" => MaintenanceActionKind::RemoveOrphanFiles,
            "rewrite_position_delete_files" => MaintenanceActionKind::RewritePositionDeleteFiles,
            other => return Err(format!("unsupported Iceberg system procedure `{other}`")),
        };
        let named = normalize_procedure_args(stmt)?;
        let table = required_string_arg(&named, "table")?;
        let (catalog, namespace, table) =
            resolve_procedure_table_name(&stmt.catalog, current_database, &table)?;
        let mut req = Self {
            source: MaintenanceActionSource::SparkProcedure,
            kind,
            catalog,
            namespace,
            table,
            options: MaintenanceActionOptions::default(),
            older_than_ms: optional_timestamp_arg(&named, "older_than")?,
            retain_last: optional_u32_arg(&named, "retain_last")?,
            use_caching: optional_bool_arg(&named, "use_caching")?,
            spec_id: optional_i32_arg(&named, "spec_id")?,
            branch: optional_string_arg(&named, "branch")?,
            where_clause: optional_string_arg(&named, "where")?,
        };
        if let Some(options) = optional_string_map_arg(&named, "options")? {
            req.options = MaintenanceActionOptions { values: options };
        }
        validate_supported_args(stmt.procedure.as_str(), named.keys())?;
        Ok(req)
    }
}
```

Implement `normalize_procedure_args` to map positional arguments exactly:

```rust
fn positional_names(procedure: &str) -> Result<&'static [&'static str], String> {
    match procedure {
        "rewrite_data_files" => Ok(&["table", "strategy", "sort_order", "options", "where", "branch"]),
        "rewrite_manifests" => Ok(&["table", "use_caching", "spec_id"]),
        "expire_snapshots" => Ok(&[
            "table",
            "older_than",
            "retain_last",
            "max_concurrent_deletes",
            "stream_results",
            "snapshot_ids",
            "clean_expired_metadata",
        ]),
        "remove_orphan_files" => Ok(&[
            "table",
            "older_than",
            "location",
            "dry_run",
            "max_concurrent_deletes",
            "file_list_view",
            "equal_schemes",
            "equal_authorities",
            "prefix_mismatch_mode",
            "prefix_listing",
            "stream_results",
        ]),
        "rewrite_position_delete_files" => Ok(&["table", "options", "where"]),
        other => Err(format!("unsupported Iceberg system procedure `{other}`")),
    }
}
```

- [ ] **Step 4: Implement existing action execution for three synchronous procedures**

In `src/engine/iceberg_maintenance.rs`, add:

```rust
pub(crate) fn execute_maintenance_action(
    state: &Arc<StandaloneState>,
    request: MaintenanceActionRequest,
) -> Result<StatementResult, String> {
    match request.source {
        MaintenanceActionSource::SparkProcedure => {
            let outcome = execute_maintenance_action_outcome(state, &request)?;
            Ok(StatementResult::Query(outcome.to_spark_query_result()?))
        }
        MaintenanceActionSource::LegacyAlter => {
            execute_legacy_maintenance_action(state, request)
        }
    }
}
```

For `RewriteManifests`, `ExpireSnapshots`, and `RemoveOrphanFiles`, call the existing connector functions directly and return typed outcomes:

```rust
fn execute_maintenance_action_outcome(
    state: &Arc<StandaloneState>,
    request: &MaintenanceActionRequest,
) -> Result<MaintenanceActionOutcome, String> {
    match request.kind {
        MaintenanceActionKind::RewriteManifests => run_rewrite_manifests_action(state, request),
        MaintenanceActionKind::ExpireSnapshots => run_expire_snapshots_action(state, request),
        MaintenanceActionKind::RemoveOrphanFiles => run_remove_orphan_files_action(state, request),
        MaintenanceActionKind::RewriteDataFiles => run_rewrite_data_files_action(state, request),
        MaintenanceActionKind::RewritePositionDeleteFiles => {
            run_rewrite_position_delete_files_action(state, request)
        }
    }
}
```

For legacy source, return `StatementResult::Ok` after executing the same outcome path for synchronous actions:

```rust
fn execute_legacy_maintenance_action(
    state: &Arc<StandaloneState>,
    request: MaintenanceActionRequest,
) -> Result<StatementResult, String> {
    match request.kind {
        MaintenanceActionKind::RewriteDataFiles => create_legacy_optimize_job(state, &request),
        _ => {
            let _ = execute_maintenance_action_outcome(state, &request)?;
            Ok(StatementResult::Ok)
        }
    }
}
```

Keep existing `execute_iceberg_rewrite_manifests`, `execute_iceberg_expire_snapshots`, and `execute_iceberg_remove_orphan_files` wrappers compiling by rewriting them to create `MaintenanceActionRequest { source: LegacyAlter, ... }`.

- [ ] **Step 5: Route `CALL` in `execute_in_context_inner`**

In `src/engine/mod.rs`, import:

```rust
use crate::engine::procedure::{looks_like_call_procedure, parse_call_procedure_sql};
```

Add this before legacy maintenance routing:

```rust
if looks_like_call_procedure(&normalized) {
    let stmt = parse_call_procedure_sql(&normalized)?;
    let request = crate::engine::iceberg_maintenance::MaintenanceActionRequest::from_call(
        &stmt,
        current_database,
    )?;
    return crate::engine::iceberg_maintenance::execute_maintenance_action(
        &self.inner,
        request,
    );
}
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test --lib procedure::tests iceberg_maintenance::tests
cargo test --lib parse_alter_table_rewrite_manifests_basic parse_remove_orphan_files_basic
```

Expected: all parser and maintenance model tests pass, and existing legacy parser tests still pass.

- [ ] **Step 7: Commit dispatcher**

```bash
git add src/engine/procedure.rs src/engine/iceberg_maintenance.rs src/engine/mod.rs src/engine/iceberg_rewrite_manifests.rs src/engine/iceberg_expire_snapshots.rs src/engine/iceberg_remove_orphan_files.rs
git commit -m "feat: route Iceberg maintenance procedures"
```

---

### Task 4: Spark `rewrite_data_files` Procedure with Legacy Async Compatibility

**Files:**
- Modify: `src/engine/iceberg_maintenance.rs`
- Modify: `src/connector/iceberg/compact.rs`
- Modify: `src/connector/iceberg/commit/rewrite_data_files.rs`
- Test: `src/engine/iceberg_maintenance.rs`

- [ ] **Step 1: Write failing result/outcome tests**

Add tests for Spark schema and unsupported options:

```rust
#[test]
fn rewrite_data_files_schema_matches_spark_40() {
    let outcome = MaintenanceActionOutcome::RewriteDataFiles {
        rewritten_data_files_count: 2,
        added_data_files_count: 1,
        rewritten_bytes_count: 4096,
        failed_data_files_count: 0,
        removed_delete_files_count: 3,
    };
    let result = outcome.to_spark_query_result().unwrap();
    let names = result
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "rewritten_data_files_count",
            "added_data_files_count",
            "rewritten_bytes_count",
            "failed_data_files_count",
            "removed_delete_files_count"
        ]
    );
}

#[test]
fn rewrite_data_files_rejects_sort_strategy_for_first_version() {
    let mut request = test_request(MaintenanceActionKind::RewriteDataFiles);
    request
        .options
        .values
        .insert("unsupported-key".to_string(), "true".to_string());
    let err = validate_rewrite_data_files_request(&request).unwrap_err();
    assert!(err.contains("unsupported rewrite_data_files option"));
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test --lib rewrite_data_files_schema_matches_spark_40 rewrite_data_files_rejects_sort_strategy_for_first_version
```

Expected: compile failure for missing validation helper or schema builder.

- [ ] **Step 3: Add synchronous rewrite-data-files action**

Add validation:

```rust
fn validate_rewrite_data_files_request(request: &MaintenanceActionRequest) -> Result<(), String> {
    if request.where_clause.is_some() {
        return Err("rewrite_data_files where is not supported in NovaRocks yet".to_string());
    }
    if request.branch.is_some() {
        return Err("rewrite_data_files branch is not supported in NovaRocks yet".to_string());
    }
    for key in request.options.values.keys() {
        match key.as_str() {
            "rewrite-all" | "min-input-files" | "target-file-size-bytes" => {}
            other => return Err(format!("unsupported rewrite_data_files option `{other}`")),
        }
    }
    Ok(())
}
```

Implement `run_rewrite_data_files_action` by reusing the current whole-table optimize execution core. The synchronous path should:

1. Resolve Iceberg catalog entry.
2. Load the table.
3. Count live data/delete files before rewrite.
4. Execute the same rewrite-data-files commit action used by the optimize worker.
5. Count live files after rewrite.
6. Return `MaintenanceActionOutcome::RewriteDataFiles`.

Replace `count_current_live_files` with a richer helper in `src/connector/iceberg/commit/rewrite_data_files.rs`:

```rust
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LiveFileMetrics {
    pub data_files: i64,
    pub delete_files: i64,
    pub data_bytes: i64,
    pub delete_bytes: i64,
}

pub(crate) async fn current_live_file_metrics(
    table: &Table,
    file_io: &FileIO,
) -> Result<LiveFileMetrics, String> {
    let live = enumerate_live_files(table, file_io).await?;
    Ok(LiveFileMetrics {
        data_files: i64::try_from(live.data_files.len())
            .map_err(|_| "live data file count overflow".to_string())?,
        delete_files: i64::try_from(live.delete_files.len())
            .map_err(|_| "live delete file count overflow".to_string())?,
        data_bytes: live
            .data_files
            .iter()
            .map(|entry| entry.data_file.file_size_in_bytes())
            .sum(),
        delete_bytes: live
            .delete_files
            .iter()
            .map(|entry| entry.data_file.file_size_in_bytes())
            .sum(),
    })
}
```

Keep `count_current_live_files` as a thin wrapper around `current_live_file_metrics` so existing callers remain source-compatible. Expose the whole-table optimize executor from `src/connector/iceberg/compact.rs` as `pub(crate)` and call that helper from `iceberg_maintenance.rs`. Preserve the async job path for `LegacyAlter`.

Outcome mapping:

```rust
let rewritten_data_files_count = before_data_files as i32;
let added_data_files_count = after_data_files as i32;
let removed_delete_files_count = before_delete_files.saturating_sub(after_delete_files) as i32;
let outcome = MaintenanceActionOutcome::RewriteDataFiles {
    rewritten_data_files_count,
    added_data_files_count,
    rewritten_bytes_count: before_metrics.data_bytes,
    failed_data_files_count: 0,
    removed_delete_files_count,
};
```

- [ ] **Step 4: Keep legacy `ALTER TABLE ... OPTIMIZE` async**

Move the current job creation logic from `handle_alter_table_optimize` into:

```rust
fn create_legacy_optimize_job(
    state: &Arc<StandaloneState>,
    request: &MaintenanceActionRequest,
) -> Result<StatementResult, String>
```

Build the legacy request from `AlterTableOptimizeStmt` in `handle_alter_table_optimize`, then call `execute_maintenance_action`.

- [ ] **Step 5: Run targeted tests**

Run:

```bash
cargo test --lib rewrite_data_files_schema_matches_spark_40
cargo test --lib optimize_show_job_ids
```

Expected: schema test passes and existing optimize job tests still pass.

- [ ] **Step 6: Commit rewrite_data_files route**

```bash
git add src/engine/iceberg_maintenance.rs src/engine/mod.rs src/connector/iceberg/compact.rs src/connector/iceberg/commit/rewrite_data_files.rs
git commit -m "feat: expose rewrite_data_files procedure"
```

---

### Task 5: Multi-Blob Puffin DV Writer

**Files:**
- Modify: `src/connector/iceberg/commit/puffin_dv.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`
- Test: `src/connector/iceberg/commit/puffin_dv.rs`

- [ ] **Step 1: Write failing multi-blob tests**

Add tests after the existing single-blob tests:

```rust
#[tokio::test]
async fn multi_blob_puffin_round_trips_two_dvs() {
    let dir = tempfile::tempdir().unwrap();
    let file_io = iceberg::io::FileIOBuilder::new_fs_io()
        .with_root(dir.path().to_str().unwrap())
        .build();
    let path = format!("{}/multi-dv.puffin", dir.path().to_str().unwrap());
    let mut first = DeletionVector::new();
    first.insert(1).unwrap();
    first.insert(9).unwrap();
    let mut second = DeletionVector::new();
    second.insert(3).unwrap();
    second.insert(11).unwrap();

    let written = write_multi_deletion_vector_puffin(
        &file_io,
        &path,
        &[
            DeletionVectorBlobInput {
                referenced_data_file: "file:///warehouse/t/data/a.parquet".to_string(),
                deletion_vector: first.clone(),
                snapshot_id: 10,
                sequence_number: 20,
            },
            DeletionVectorBlobInput {
                referenced_data_file: "file:///warehouse/t/data/b.parquet".to_string(),
                deletion_vector: second.clone(),
                snapshot_id: 10,
                sequence_number: 20,
            },
        ],
    )
    .await
    .unwrap();

    assert_eq!(written.len(), 2);
    assert_eq!(written[0].path, path);
    assert_eq!(written[1].path, path);
    assert_ne!(written[0].content_offset, written[1].content_offset);
    assert_eq!(
        read_deletion_vector_puffin(
            &file_io,
            &path,
            written[0].content_offset,
            written[0].content_size_in_bytes
        )
        .await
        .unwrap(),
        first
    );
    assert_eq!(
        read_deletion_vector_puffin(
            &file_io,
            &path,
            written[1].content_offset,
            written[1].content_size_in_bytes
        )
        .await
        .unwrap(),
        second
    );
}

#[tokio::test]
async fn multi_blob_puffin_rejects_empty_input() {
    let dir = tempfile::tempdir().unwrap();
    let file_io = iceberg::io::FileIOBuilder::new_fs_io()
        .with_root(dir.path().to_str().unwrap())
        .build();
    let path = format!("{}/empty.puffin", dir.path().to_str().unwrap());
    let err = write_multi_deletion_vector_puffin(&file_io, &path, &[])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("requires at least one deletion vector"));
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test --lib multi_blob_puffin_round_trips_two_dvs multi_blob_puffin_rejects_empty_input
```

Expected: compile failure for missing `write_multi_deletion_vector_puffin`.

- [ ] **Step 3: Implement multi-blob writer**

In `puffin_dv.rs`, add:

```rust
#[derive(Clone, Debug)]
pub struct DeletionVectorBlobInput {
    pub referenced_data_file: String,
    pub deletion_vector: DeletionVector,
    pub snapshot_id: i64,
    pub sequence_number: i64,
}

pub async fn write_multi_deletion_vector_puffin(
    file_io: &iceberg::io::FileIO,
    path: &str,
    inputs: &[DeletionVectorBlobInput],
) -> Result<Vec<WrittenPuffinDv>> {
    ensure!(
        !inputs.is_empty(),
        "write_multi_deletion_vector_puffin requires at least one deletion vector"
    );

    let output_file = file_io
        .new_output(path)
        .with_context(|| format!("failed to create Puffin output file: {path}"))?;
    let mut writer = iceberg::puffin::PuffinWriter::new(
        &output_file,
        std::collections::HashMap::from([("created-by".to_string(), "NovaRocks".to_string())]),
        false,
    )
    .await
    .context("failed to create Puffin deletion vector writer")?;

    for input in inputs {
        let payload = input.deletion_vector.to_iceberg_payload()?;
        let blob = iceberg::puffin::Blob::builder()
            .r#type("deletion-vector-v1".to_string())
            .fields(Vec::new())
            .snapshot_id(input.snapshot_id)
            .sequence_number(input.sequence_number)
            .data(payload)
            .properties(std::collections::HashMap::from([
                (
                    "referenced-data-file".to_string(),
                    input.referenced_data_file.clone(),
                ),
                (
                    "cardinality".to_string(),
                    input.deletion_vector.cardinality().to_string(),
                ),
            ]))
            .build();
        writer
            .add(blob, iceberg::puffin::CompressionCodec::None)
            .await
            .context("failed to write Puffin deletion vector blob")?;
    }
    writer
        .close()
        .await
        .context("failed to close Puffin deletion vector writer")?;

    read_written_dv_blob_metadata(file_io, path, inputs).await
}
```

Implement `read_written_dv_blob_metadata` by opening `PuffinReader::new(file_io.new_input(path)?)`, reading `file_metadata().await`, matching each blob by `properties["referenced-data-file"]`, and returning `WrittenPuffinDv` with metadata `offset`, `length`, `cardinality`, and file size.

- [ ] **Step 4: Export helper**

Modify `src/connector/iceberg/commit/mod.rs`:

```rust
pub use puffin_dv::{
    DeletionVector, DeletionVectorBlobInput, WrittenPuffinDv, read_deletion_vector_puffin,
    write_multi_deletion_vector_puffin, write_single_deletion_vector_puffin,
};
```

- [ ] **Step 5: Run Puffin tests**

Run:

```bash
cargo test --lib puffin_dv::tests
```

Expected: existing single-blob tests and new multi-blob tests pass.

- [ ] **Step 6: Commit Puffin helper**

```bash
git add src/connector/iceberg/commit/puffin_dv.rs src/connector/iceberg/commit/mod.rs
git commit -m "feat: write multi-blob Puffin deletion vectors"
```

---

### Task 6: `rewrite_position_delete_files` V3 DV Repack Commit Action

**Files:**
- Create: `src/connector/iceberg/commit/rewrite_position_delete_files.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`
- Modify: `src/engine/iceberg_maintenance.rs`
- Test: `src/connector/iceberg/commit/rewrite_position_delete_files.rs`

- [ ] **Step 1: Write failing planning tests**

Create module tests that cover option validation and grouping:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_default_min_input_files_is_two() {
        let options = RewritePositionDeleteOptions::from_map(&std::collections::BTreeMap::new())
            .unwrap();
        assert!(!options.rewrite_all);
        assert_eq!(options.min_input_files, 2);
    }

    #[test]
    fn options_reject_unknown_key() {
        let options = std::collections::BTreeMap::from([(
            "partial-progress.enabled".to_string(),
            "true".to_string(),
        )]);
        let err = RewritePositionDeleteOptions::from_map(&options).unwrap_err();
        assert!(err.contains("unsupported rewrite_position_delete_files option"));
    }

    #[test]
    fn v2_position_delete_detection_rejects_parquet_delete_file() {
        let file = iceberg::spec::DataFileBuilder::default()
            .content(iceberg::spec::DataContentType::PositionDeletes)
            .file_path("file:///tmp/delete.parquet".to_string())
            .file_format(iceberg::spec::DataFileFormat::Parquet)
            .partition(iceberg::spec::Struct::empty())
            .record_count(1)
            .file_size_in_bytes(64)
            .build()
            .unwrap();
        let err = classify_delete_file_for_rewrite(&file).unwrap_err();
        assert!(err.contains("V2 Parquet position delete rewrite is not supported"));
    }
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test --lib rewrite_position_delete_files::tests
```

Expected: compile failure because module is not created or registered.

- [ ] **Step 3: Implement options and outcome**

Add:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RewritePositionDeleteOptions {
    pub rewrite_all: bool,
    pub min_input_files: usize,
    pub target_file_size_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RewritePositionDeleteOutcome {
    pub rewritten_delete_files_count: i32,
    pub added_delete_files_count: i32,
    pub rewritten_bytes_count: i64,
    pub added_bytes_count: i64,
}

impl RewritePositionDeleteOptions {
    pub fn from_map(values: &std::collections::BTreeMap<String, String>) -> Result<Self, String> {
        let mut out = Self {
            rewrite_all: false,
            min_input_files: 2,
            target_file_size_bytes: None,
        };
        for (key, value) in values {
            match key.as_str() {
                "rewrite-all" => out.rewrite_all = parse_bool_option(key, value)?,
                "min-input-files" => out.min_input_files = parse_usize_option(key, value)?,
                "target-file-size-bytes" => {
                    out.target_file_size_bytes = Some(parse_u64_option(key, value)?)
                }
                other => {
                    return Err(format!(
                        "unsupported rewrite_position_delete_files option `{other}`"
                    ));
                }
            }
        }
        Ok(out)
    }
}
```

- [ ] **Step 4: Implement manifest scan and V2 rejection**

Add `run_rewrite_position_delete_files`:

```rust
pub async fn run_rewrite_position_delete_files(
    catalog: std::sync::Arc<dyn iceberg::Catalog>,
    table_ident: iceberg::TableIdent,
    options: RewritePositionDeleteOptions,
) -> Result<RewritePositionDeleteOutcome, String> {
    crate::connector::iceberg::commit::retry::commit_with_retry(|_attempt| {
        let catalog = catalog.clone();
        let table_ident = table_ident.clone();
        let options = options.clone();
        async move { run_one_attempt(catalog, table_ident, options).await }
    })
    .await
}
```

In `run_one_attempt`:

1. Load table and current snapshot.
2. Return zero outcome when current snapshot is absent.
3. Load manifest list.
4. Iterate delete manifests only.
5. For each live delete entry:
   - `DataContentType::PositionDeletes` + `DataFileFormat::Parquet` returns unsupported.
   - Puffin DV requires `referenced_data_file`, `content_offset`, and `content_size_in_bytes`.
   - Group by `referenced_data_file`.

Use existing row-delta helpers as reference for reading `referenced_data_file()` and content offsets. Keep the first implementation serial.

- [ ] **Step 5: Implement V3 repack**

For candidate groups:

```rust
let should_rewrite = options.rewrite_all || group.entries.len() >= options.min_input_files;
```

For each rewrite group:

1. Read each old DV with `read_deletion_vector_puffin`.
2. Merge into one `DeletionVector`.
3. Preserve `spec_id`, partition tuple, and data sequence number; reject mismatches.
4. Write candidate groups into one or more Puffin files with `write_multi_deletion_vector_puffin`.
5. Create new delete `DataFile` entries with `DataFileBuilder` and copied partition metadata.
6. Write added delete manifests using `ManifestWriterBuilder::build_v3_deletes`.
7. Write deleted delete manifests for old DV entries with status deleted.
8. Write manifest list and commit `Operation::Replace`.

Commit requirements:

```rust
vec![
    TableRequirement::CurrentSchemaIdMatch {
        current_schema_id: metadata.current_schema_id(),
    },
    TableRequirement::DefaultSpecIdMatch {
        default_spec_id: metadata.default_partition_spec_id(),
    },
    TableRequirement::RefSnapshotIdMatch {
        r#ref: "main".to_string(),
        snapshot_id: parent_snapshot_id,
    },
]
```

Snapshot summary keys:

```rust
"rewritten-delete-files" -> rewritten_count.to_string()
"added-delete-files" -> added_count.to_string()
"rewritten-bytes" -> rewritten_bytes.to_string()
"added-bytes" -> added_bytes.to_string()
"operation" is Operation::Replace through the snapshot summary object
```

- [ ] **Step 6: Wire engine action**

In `src/engine/iceberg_maintenance.rs`, implement `run_rewrite_position_delete_files_action`:

```rust
fn run_rewrite_position_delete_files_action(
    state: &Arc<StandaloneState>,
    request: &MaintenanceActionRequest,
) -> Result<MaintenanceActionOutcome, String> {
    if request.where_clause.is_some() {
        return Err("rewrite_position_delete_files where is not supported in NovaRocks".to_string());
    }
    let options = crate::connector::iceberg::commit::rewrite_position_delete_files::RewritePositionDeleteOptions::from_map(
        &request.options.values,
    )?;
    let (catalog, table_ident) = load_iceberg_catalog_and_ident(state, request)?;
    let outcome = crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
        crate::connector::iceberg::commit::rewrite_position_delete_files::run_rewrite_position_delete_files(
            catalog,
            table_ident,
            options,
        )
        .await
    })?
    .map_err(|e| {
        format!(
            "rewrite_position_delete_files failed for {}.{}.{}: {e}",
            request.catalog, request.namespace, request.table
        )
    })?;
    Ok(MaintenanceActionOutcome::RewritePositionDeleteFiles {
        rewritten_delete_files_count: outcome.rewritten_delete_files_count,
        added_delete_files_count: outcome.added_delete_files_count,
        rewritten_bytes_count: outcome.rewritten_bytes_count,
        added_bytes_count: outcome.added_bytes_count,
    })
}
```

- [ ] **Step 7: Run targeted Rust tests**

Run:

```bash
cargo test --lib rewrite_position_delete_files::tests
cargo test --lib puffin_dv::tests
cargo test --lib iceberg_maintenance::tests
```

Expected: all targeted tests pass.

- [ ] **Step 8: Commit V3 repack**

```bash
git add src/connector/iceberg/commit/rewrite_position_delete_files.rs src/connector/iceberg/commit/mod.rs src/engine/iceberg_maintenance.rs
git commit -m "feat: rewrite V3 Puffin deletion vectors"
```

---

### Task 7: SQL Tests for Spark Procedures

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_spark_procedures_basic.sql`
- Create: `sql-tests/iceberg/sql/iceberg_spark_procedures_errors.sql`
- Create: `sql-tests/iceberg/sql/iceberg_v3_rewrite_position_delete_files.sql`
- Test: SQL runner output

- [ ] **Step 1: Add basic procedure SQL case**

Create `sql-tests/iceberg/sql/iceberg_spark_procedures_basic.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=iceberg,procedures,spark

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG proc_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${starrocks_table_warehouse}/proc_ice_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE proc_ice_${uuid0}.ns_${uuid0};
CREATE TABLE proc_ice_${uuid0}.ns_${uuid0}.orders (
  id INT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO proc_ice_${uuid0}.ns_${uuid0}.orders VALUES (1, 10), (2, 20);

-- query 2
-- @db=proc_ice_${uuid0}.ns_${uuid0}
CALL proc_ice_${uuid0}.system.rewrite_manifests(table => 'ns_${uuid0}.orders');

-- query 3
-- @db=proc_ice_${uuid0}.ns_${uuid0}
CALL proc_ice_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders');

-- query 4
-- @skip_result_check=true
DROP TABLE proc_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE proc_ice_${uuid0}.ns_${uuid0};
DROP CATALOG proc_ice_${uuid0};
```

- [ ] **Step 2: Add error SQL case**

Create `sql-tests/iceberg/sql/iceberg_spark_procedures_errors.sql`:

```sql
-- @sequential=true
-- @tags=iceberg,procedures,errors

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG proc_err_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${starrocks_table_warehouse}/proc_err_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE proc_err_${uuid0}.ns_${uuid0};
CREATE TABLE proc_err_${uuid0}.ns_${uuid0}.orders (id INT) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 2
-- @expect_error=Iceberg procedures must use system namespace
CALL proc_err_${uuid0}.admin.rewrite_manifests(table => 'ns_${uuid0}.orders');

-- query 3
-- @expect_error=unsupported Iceberg system procedure
CALL proc_err_${uuid0}.system.unknown_proc(table => 'ns_${uuid0}.orders');

-- query 4
-- @expect_error=where is not supported
CALL proc_err_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders', where => 'id = 1');

-- query 5
-- @expect_error=unsupported rewrite_position_delete_files option
CALL proc_err_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders', options => map('partial-progress.enabled', 'true'));

-- query 6
-- @skip_result_check=true
DROP TABLE proc_err_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE proc_err_${uuid0}.ns_${uuid0};
DROP CATALOG proc_err_${uuid0};
```

- [ ] **Step 3: Add V3 DV repack SQL case**

Create `sql-tests/iceberg/sql/iceberg_v3_rewrite_position_delete_files.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=iceberg,procedures,rewrite_position_delete_files,v3,dv

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG dv_proc_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${starrocks_table_warehouse}/dv_proc_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE dv_proc_${uuid0}.ns_${uuid0};
CREATE TABLE dv_proc_${uuid0}.ns_${uuid0}.orders (
  id INT,
  user_id INT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO dv_proc_${uuid0}.ns_${uuid0}.orders VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300), (4, 40, 400);
DELETE FROM dv_proc_${uuid0}.ns_${uuid0}.orders WHERE id = 2;
DELETE FROM dv_proc_${uuid0}.ns_${uuid0}.orders WHERE id = 4;

-- query 2
-- @db=dv_proc_${uuid0}.ns_${uuid0}
SELECT id, user_id, amount FROM orders ORDER BY id;

-- query 3
-- @db=dv_proc_${uuid0}.ns_${uuid0}
CALL dv_proc_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders', options => map('rewrite-all', 'true'));

-- query 4
-- @db=dv_proc_${uuid0}.ns_${uuid0}
SELECT id, user_id, amount FROM orders ORDER BY id;

-- query 5
-- @skip_result_check=true
DROP TABLE dv_proc_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE dv_proc_${uuid0}.ns_${uuid0};
DROP CATALOG dv_proc_${uuid0};
```

- [ ] **Step 4: Record SQL results**

Start the local Iceberg REST environment and server:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost cargo run --profile dev-opt -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

In another shell, record the new cases:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg \
  --only iceberg_spark_procedures_basic,iceberg_spark_procedures_errors,iceberg_v3_rewrite_position_delete_files \
  --mode record
```

Expected: runner records results for all three cases.

- [ ] **Step 5: Verify SQL cases**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg \
  --only iceberg_spark_procedures_basic,iceberg_spark_procedures_errors,iceberg_v3_rewrite_position_delete_files \
  --mode verify
```

Expected: all three cases pass.

- [ ] **Step 6: Commit SQL tests**

```bash
git add sql-tests/iceberg/sql/iceberg_spark_procedures_basic.sql sql-tests/iceberg/sql/iceberg_spark_procedures_errors.sql sql-tests/iceberg/sql/iceberg_v3_rewrite_position_delete_files.sql sql-tests/iceberg/result/iceberg_spark_procedures_basic.result sql-tests/iceberg/result/iceberg_spark_procedures_errors.result sql-tests/iceberg/result/iceberg_v3_rewrite_position_delete_files.result
git commit -m "test: add Spark procedure SQL coverage"
```

---

### Task 8: Final Verification and Cleanup

**Files:**
- Modify only files with formatting or narrow fixes discovered by verification.

- [ ] **Step 1: Run Rust targeted tests**

Run:

```bash
cargo test --lib procedure::tests
cargo test --lib iceberg_maintenance::tests
cargo test --lib puffin_dv::tests
cargo test --lib rewrite_position_delete_files::tests
```

Expected: all targeted tests pass.

- [ ] **Step 2: Run existing maintenance regression tests**

Run:

```bash
cargo test --lib parse_alter_table_rewrite_manifests_basic parse_alter_table_expire_snapshots_sql parse_remove_orphan_files_basic
cargo test --lib optimize_show_job_ids
```

Expected: all existing maintenance parser/job tests pass.

- [ ] **Step 3: Run SQL verification**

With the standalone server already running:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg \
  --only iceberg_spark_procedures_basic,iceberg_spark_procedures_errors,iceberg_v3_rewrite_position_delete_files,iceberg_v3_optimize_compact_data_files,iceberg_v3_rewrite_manifests,iceberg_v3_expire_snapshots,iceberg_v3_remove_orphan_files \
  --mode verify
```

Expected: selected Iceberg maintenance cases pass.

- [ ] **Step 4: Run formatting and diff checks**

Run:

```bash
cargo fmt --check
git diff --check HEAD
```

Expected: both commands pass. If `cargo fmt --check` reports unrelated pre-existing drift, run `cargo fmt` only on touched Rust files and re-run `git diff --check HEAD`.

- [ ] **Step 5: Inspect final diff**

Run:

```bash
git status --short
git diff --stat HEAD
git diff --name-only HEAD
```

Expected: only files from this plan are modified.

- [ ] **Step 6: Commit final narrow fixes**

If Step 4 or Step 5 required any narrow fixes:

```bash
git add <fixed-files>
git commit -m "fix: finalize Spark procedure alignment"
```

If no fixes were needed, do not create an empty commit.
