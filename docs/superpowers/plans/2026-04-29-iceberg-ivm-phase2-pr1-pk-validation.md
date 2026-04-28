# Iceberg IVM Phase 2 — PR-1: PRIMARY KEY Parsing & CREATE-Time Validation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `PRIMARY KEY (col, ...)` clause to `CREATE MATERIALIZED VIEW` and reject — at DDL time, before any tablet is written — any MV definition whose PK columns don't satisfy the IVM Phase-2 contract (column exists in iceberg base, NOT NULL, hashable scalar type, base format-version = 2).

**Architecture:** Surface-level: extend the existing `materialized_view.rs` parser to capture an optional PK column list onto `CreateMaterializedViewStmt`. Validation: a new pure-function `validate_ivm_primary_key` in `mv_ddl.rs` that consumes the resolved iceberg base-table descriptor (already produced by `analyze_mv_select` + `load_current_iceberg_base_table`) and the parsed PK list, and returns the new `ChangeError` enum. The validation is invoked at the top of both `create_mv` (managed-lake-stored MV) and `create_iceberg_mv` (iceberg-stored MV) paths, gated by `stmt.primary_key.is_some()` so MVs without a PK clause keep their current behavior bit-for-bit. The parsed PK is otherwise unused this PR — PR-3/4 wire it into ROW_ID computation and `last_refresh_snapshots`.

**Tech Stack:** Rust, `sqlparser` AST extensions, NovaRocks managed-lake DDL flow, `iceberg::table::Table.metadata()` for format-version detection, `sql-tests/write-path` regression suite.

---

## File Structure

- Modify `src/sql/parser/ast/mod.rs`: add `primary_key: Option<Vec<String>>` field to `CreateMaterializedViewStmt`.
- Modify `src/sql/parser/dialect/materialized_view.rs`: parse optional `PRIMARY KEY (col, ...)` clause between DISTRIBUTED BY and REFRESH; add unit tests for happy path, missing parens, empty list, duplicate columns.
- Create `src/connector/iceberg/changes.rs`: new module hosting the `ChangeError` enum (full set per spec §6, PR-1 only constructs the PK + iceberg-format variants; rest reserved for PR-2/3/4).
- Modify `src/connector/iceberg/mod.rs`: export the new `changes` module.
- Modify `src/connector/starrocks/managed/mv_ddl.rs`: add `validate_ivm_primary_key(stmt_pk, loaded_base) -> Result<(), ChangeError>` and call it from `create_mv` after `analyze_mv_select`; add unit tests for each error path + happy path.
- Modify `src/connector/starrocks/managed/mv_refresh_iceberg.rs`: call the same `validate_ivm_primary_key` from `create_iceberg_mv` so iceberg-stored MVs get the same validation.
- Create `sql-tests/write-path/sql/managed_lake_mv_ivm_pk_invalid.sql` + `sql-tests/write-path/result/managed_lake_mv_ivm_pk_invalid.result`: regression case covering each rejection.

## Implementation Constraints

- The PK clause is **optional**. `CREATE MATERIALIZED VIEW` without `PRIMARY KEY` must continue producing the exact same AST and the exact same DDL behavior as before this PR — no field reorderings, no extra validation, no change to existing tests.
- Duplicate column names inside a PK clause must be rejected at parse time (clearer error site than later semantic validation).
- An empty PK clause `PRIMARY KEY ()` must be rejected at parse time.
- The validation function must take only the AST PK list and the already-loaded iceberg base-table descriptor — no I/O of its own. The caller is responsible for loading the base table once and passing it in.
- Unsupported variants of `ChangeError` (e.g. `LineageBroken`, `EqualityDeleteUnsupported`) must be defined in this PR but never constructed; a `#[allow(dead_code)]` attribute on those variants is acceptable to silence the lint until PR-2 lands.
- The validation must run **before** any catalog mutation, tablet creation, or metadata-store write. The first existing line in `create_mv` that mutates state is `managed.write()...contains_table` (read-only) followed by `snapshot.materialized_views.push(...)` — insert validation before the first `analyze_mv_select` call so we fail fast on obviously-bad PK without even resolving base tables, and after `analyze_mv_select` for the column-existence checks.
- Hashable scalar types accepted by PR-1: `BIGINT`, `INT`, `SMALLINT`, `TINYINT`, `STRING` / `VARCHAR`, `DATE`, `DATETIME`, `DECIMAL`. Reject `ARRAY`, `MAP`, `STRUCT`, `JSON`, `BOOLEAN` (unstable hash semantics), `FLOAT`, `DOUBLE` (NaN equality issues).
- This PR does **not** persist the PK into MV catalog metadata. The PK is validated and dropped. PR-3 will add a `primary_key_columns` column to `StoredMaterializedView` and wire it through.

---

## Task 1: Extend `CreateMaterializedViewStmt` AST With Optional PRIMARY KEY

**Files:**
- Modify: `src/sql/parser/ast/mod.rs:56-71`

- [ ] **Step 1: Add the field**

Modify the struct to add the new field at the end (before the closing brace):

```rust
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateMaterializedViewStmt {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub distribution: Option<MaterializedViewDistribution>,
    pub refresh_manual_explicit: bool,
    /// Raw SQL text of the SELECT body after `AS`. Produced by re-serializing
    /// the parsed `sqlparser::ast::Query`; used for storage and for
    /// re-parsing on every REFRESH in Phase 1.
    pub select_sql: String,
    pub select_query: sqlparser::ast::Query,
    /// Key-value pairs from `PROPERTIES(...)`, retained for later semantic
    /// interpretation (e.g. `storage_engine`). Empty when the clause is
    /// absent.
    pub properties: Vec<(String, String)>,
    /// Columns named in `PRIMARY KEY (col, ...)`. `None` when the clause is
    /// absent. The clause is the IVM Phase-2 opt-in marker; columns must
    /// reference the iceberg base table and satisfy the constraints checked
    /// by `mv_ddl::validate_ivm_primary_key`.
    pub primary_key: Option<Vec<String>>,
}
```

- [ ] **Step 2: Build to surface every construction site**

Run: `cargo build`
Expected: FAIL with E0063 / "missing field `primary_key`" at every place that constructs `CreateMaterializedViewStmt`. The known site is `src/sql/parser/dialect/materialized_view.rs:101-111`. Note any additional sites listed in the build error output.

- [ ] **Step 3: Default-construct `primary_key: None` at every existing construction site**

For the parser site at `src/sql/parser/dialect/materialized_view.rs:101-111`, add `primary_key: None,` before the closing brace. For any additional sites surfaced by Step 2 (test fixtures, etc.), add `primary_key: None,` as well.

- [ ] **Step 4: Build clean**

Run: `cargo build`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql/parser/ast/mod.rs src/sql/parser/dialect/materialized_view.rs
git commit -m "feat(mv): add optional primary_key field to CreateMaterializedViewStmt

Surfaces an Option<Vec<String>> on the AST node for the upcoming IVM
Phase-2 PRIMARY KEY clause. Currently always None; the parser will set
it in the next commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Parse `PRIMARY KEY (col, ...)` Clause

**Files:**
- Modify: `src/sql/parser/dialect/materialized_view.rs` (`parse_create_materialized_view` body and tests module)

- [ ] **Step 1: Write the parser-level happy-path test**

Append inside `mod tests` (before the closing `}` of the module, around line 575):

```rust
    #[test]
    fn parse_create_mv_with_primary_key_captures_columns() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             PRIMARY KEY (order_id, line_id) \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert_eq!(
            mv.primary_key.as_deref(),
            Some(["order_id".to_string(), "line_id".to_string()].as_slice()),
        );
    }

    #[test]
    fn parse_create_mv_without_primary_key_keeps_field_none() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert!(mv.primary_key.is_none());
    }

    #[test]
    fn parse_create_mv_rejects_empty_primary_key_list() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             PRIMARY KEY () \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("primary key"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn parse_create_mv_rejects_duplicate_primary_key_columns() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             PRIMARY KEY (order_id, order_id) \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("duplicate"),
            "unexpected err: {err}"
        );
    }
```

- [ ] **Step 2: Run tests and confirm they fail**

Run: `cargo test -p novarocks --lib sql::parser::dialect::materialized_view::tests::parse_create_mv_with_primary_key_captures_columns sql::parser::dialect::materialized_view::tests::parse_create_mv_rejects_empty_primary_key_list sql::parser::dialect::materialized_view::tests::parse_create_mv_rejects_duplicate_primary_key_columns sql::parser::dialect::materialized_view::tests::parse_create_mv_without_primary_key_keeps_field_none`
Expected: FAIL — happy-path test fails because parser hits unexpected token `PRIMARY` and surfaces a generic parse error; rejection tests fail similarly. (`without_primary_key_keeps_field_none` may pass once Task 1 lands, that's fine.)

- [ ] **Step 3: Add the parser logic**

In `src/sql/parser/dialect/materialized_view.rs`, locate `parse_create_materialized_view`. After the `parse_refresh_clause` block (the `let refresh_manual_explicit = ...` block ending around line 74) and before the `ORDER BY` rejection (line 77), add the PK parser:

```rust
    // Optional PRIMARY KEY (col, ...) clause — IVM Phase-2 opt-in marker.
    let primary_key = if parser.parse_keyword(Keyword::PRIMARY) {
        parser
            .expect_keyword(Keyword::KEY)
            .map_err(|e| format!("expected KEY after PRIMARY: {e}"))?;
        parser
            .expect_token(&Token::LParen)
            .map_err(|e| format!("expected ( after PRIMARY KEY: {e}"))?;
        let mut cols: Vec<String> = Vec::new();
        loop {
            if parser.consume_token(&Token::RParen) {
                break;
            }
            let ident = parser
                .parse_identifier()
                .map_err(|e| format!("parse PRIMARY KEY column failed: {e}"))?;
            let name = ident.value;
            if cols.iter().any(|c| c.eq_ignore_ascii_case(&name)) {
                return Err(format!(
                    "duplicate column `{name}` in PRIMARY KEY clause"
                ));
            }
            cols.push(name);
            if parser.consume_token(&Token::RParen) {
                break;
            }
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| format!("expected , or ) in PRIMARY KEY column list: {e}"))?;
        }
        if cols.is_empty() {
            return Err("PRIMARY KEY clause requires at least one column".to_string());
        }
        Some(cols)
    } else {
        None
    };
```

Then update the `Ok(Statement::CreateMaterializedView(...))` block at the bottom of the function to include the new field:

```rust
    Ok(Statement::CreateMaterializedView(
        CreateMaterializedViewStmt {
            name,
            if_not_exists,
            distribution,
            refresh_manual_explicit,
            select_sql,
            select_query: *query,
            properties,
            primary_key,
        },
    ))
```

(Remove the placeholder `primary_key: None,` that Task 1 added at this site.)

- [ ] **Step 4: Run the four PK tests and confirm they pass**

Run: `cargo test -p novarocks --lib sql::parser::dialect::materialized_view::tests::parse_create_mv_with_primary_key_captures_columns sql::parser::dialect::materialized_view::tests::parse_create_mv_without_primary_key_keeps_field_none sql::parser::dialect::materialized_view::tests::parse_create_mv_rejects_empty_primary_key_list sql::parser::dialect::materialized_view::tests::parse_create_mv_rejects_duplicate_primary_key_columns`
Expected: 4 passed; 0 failed.

- [ ] **Step 5: Run the full materialized_view parser test module to confirm no regression**

Run: `cargo test -p novarocks --lib sql::parser::dialect::materialized_view::tests`
Expected: all tests pass — existing ones (PARTITION BY rejection, ORDER BY rejection, REFRESH ASYNC rejection, etc.) plus the four new ones.

- [ ] **Step 6: Commit**

```bash
git add src/sql/parser/dialect/materialized_view.rs
git commit -m "feat(mv): parse PRIMARY KEY (col, ...) clause on CREATE MATERIALIZED VIEW

Optional clause placed between DISTRIBUTED BY/REFRESH and ORDER BY.
Empty list and duplicate columns are rejected at parse time. Semantic
validation against the iceberg base table follows in a later commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Define `ChangeError` Enum In A New `iceberg::changes` Module

**Files:**
- Create: `src/connector/iceberg/changes.rs`
- Modify: `src/connector/iceberg/mod.rs:18-25` (module list)

- [ ] **Step 1: Create the module file**

Write `src/connector/iceberg/changes.rs`:

```rust
//! Errors and (in later PRs) data structures for iceberg snapshot-lineage
//! change planning under IVM Phase 2. This file is the home of the new
//! `plan_changes` entrypoint that PR-2 will introduce; PR-1 only lands the
//! error enum so that CREATE-time PRIMARY KEY validation has a stable type
//! to return.

/// All failure modes the iceberg change-planning and IVM CREATE/REFRESH
/// paths can surface. STRICT fail-fast: every variant is a hard rejection,
/// not a fallback signal. Variants not constructed in this PR are reserved
/// for PR-2 (`plan_changes` lineage walk) and PR-3/4 (runtime checks).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ChangeError {
    /// `previous_snapshot` referenced by stored MV state is no longer
    /// reachable from the current snapshot's parent chain (e.g. expired).
    LineageBroken { previous_snapshot: i64 },

    /// Snapshot operation is not understood or not in scope for this phase
    /// (e.g. `overwrite`, vendor-specific ops).
    UnsupportedOperation { snapshot_id: i64, op: String },

    /// Equality-delete file encountered; only position-deletes are in scope.
    EqualityDeleteUnsupported { snapshot_id: i64 },

    /// Iceberg v3 deletion-vector file encountered; out of scope.
    DeletionVectorUnsupported { snapshot_id: i64 },

    /// Schema evolution between `previous_snapshot` and `current_snapshot`
    /// (or any unsupported schema-related rejection at CREATE time).
    SchemaEvolutionUnsupported { detail: String },

    /// REPLACE snapshot failed the compaction-only sanity checks (records
    /// changed / schema-id changed / no added or no removed files).
    ReplaceValidationFailed { snapshot_id: i64, reason: String },

    /// CREATE-time: PRIMARY KEY column does not exist on the iceberg base
    /// table.
    PrimaryKeyMissingFromBase { pk_col: String },

    /// CREATE-time: PRIMARY KEY column is nullable on the base table.
    PrimaryKeyNullable { pk_col: String },

    /// CREATE-time: PRIMARY KEY column has a non-hashable scalar type.
    PrimaryKeyTypeUnsupported { pk_col: String, ty: String },

    /// Runtime: PRIMARY KEY column observed NULL in a base row at refresh
    /// time. Not constructed in PR-1.
    PrimaryKeyValueNull { row_info: String },

    /// CREATE-time: iceberg base table is not format-version 2.
    IcebergFormatUnsupported { format_version: i32 },

    /// Catch-all for invariant violations the codebase should never hit;
    /// constructing one is a bug, not a user error.
    InternalInconsistency(String),
}

impl std::fmt::Display for ChangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeError::LineageBroken { previous_snapshot } => write!(
                f,
                "iceberg lineage broken: previous snapshot {previous_snapshot} is unreachable from current snapshot"
            ),
            ChangeError::UnsupportedOperation { snapshot_id, op } => write!(
                f,
                "iceberg snapshot {snapshot_id} has unsupported operation `{op}`"
            ),
            ChangeError::EqualityDeleteUnsupported { snapshot_id } => write!(
                f,
                "iceberg snapshot {snapshot_id} contains equality-delete files; not supported in this phase"
            ),
            ChangeError::DeletionVectorUnsupported { snapshot_id } => write!(
                f,
                "iceberg snapshot {snapshot_id} contains v3 deletion-vector files; not supported in this phase"
            ),
            ChangeError::SchemaEvolutionUnsupported { detail } => write!(
                f,
                "iceberg schema evolution not supported: {detail}"
            ),
            ChangeError::ReplaceValidationFailed { snapshot_id, reason } => write!(
                f,
                "iceberg REPLACE snapshot {snapshot_id} failed compaction validation: {reason}"
            ),
            ChangeError::PrimaryKeyMissingFromBase { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` does not exist on the iceberg base table"
            ),
            ChangeError::PrimaryKeyNullable { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` must be NOT NULL on the iceberg base table"
            ),
            ChangeError::PrimaryKeyTypeUnsupported { pk_col, ty } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` has unsupported type `{ty}`; only hashable scalar types are allowed"
            ),
            ChangeError::PrimaryKeyValueNull { row_info } => write!(
                f,
                "PRIMARY KEY value is NULL in base row: {row_info}"
            ),
            ChangeError::IcebergFormatUnsupported { format_version } => write!(
                f,
                "iceberg base table format-version {format_version} is not supported; IVM Phase 2 requires v2"
            ),
            ChangeError::InternalInconsistency(detail) => {
                write!(f, "internal inconsistency: {detail}")
            }
        }
    }
}

impl std::error::Error for ChangeError {}
```

- [ ] **Step 2: Wire the module into the iceberg connector**

Edit `src/connector/iceberg/mod.rs`. Add the new `pub mod changes;` line right after `pub mod catalog;`:

```rust
pub mod catalog;
pub mod changes;
pub mod commit;
mod jvm;
pub mod metadata;
pub mod position_delete;
pub mod schema;
pub mod sink;
mod state;
```

- [ ] **Step 3: Add a smoke test for the Display impl**

Append to `src/connector/iceberg/changes.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::ChangeError;

    #[test]
    fn display_primary_key_missing() {
        let e = ChangeError::PrimaryKeyMissingFromBase {
            pk_col: "order_id".to_string(),
        };
        let s = format!("{e}");
        assert!(s.contains("order_id"), "{s}");
        assert!(s.to_lowercase().contains("primary key"), "{s}");
    }

    #[test]
    fn display_iceberg_format_unsupported() {
        let e = ChangeError::IcebergFormatUnsupported { format_version: 1 };
        let s = format!("{e}");
        assert!(s.contains("format-version 1"), "{s}");
        assert!(s.to_lowercase().contains("v2"), "{s}");
    }
}
```

- [ ] **Step 4: Build and run the new tests**

Run: `cargo test -p novarocks --lib connector::iceberg::changes::tests`
Expected: 2 passed; 0 failed.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/changes.rs src/connector/iceberg/mod.rs
git commit -m "feat(iceberg): add ChangeError enum for IVM Phase-2 fail-fast

New module src/connector/iceberg/changes.rs hosts the unified error type
for iceberg change-planning and IVM CREATE/REFRESH paths. PR-1 only
constructs the PRIMARY KEY and IcebergFormatUnsupported variants; the
rest are reserved for plan_changes (PR-2) and the runtime path (PR-3/4).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Implement `validate_ivm_primary_key`

**Files:**
- Modify: `src/connector/starrocks/managed/mv_ddl.rs` (add function + unit tests)

- [ ] **Step 1: Write unit tests for every error path and the happy path**

Append inside the existing `#[cfg(test)] mod tests` block in `mv_ddl.rs` (find the block; if there isn't one, scan from the bottom of the file — the file already has `extract_base_table_refs_rejects_non_iceberg_tables` at line 967, so add right after that test):

```rust
    use crate::connector::iceberg::changes::ChangeError;
    use crate::connector::starrocks::managed::mv_ddl::validate_ivm_primary_key;

    /// Build a `BaseTableDescriptor` directly without touching iceberg-rust.
    /// Mirrors the production projection done by the caller in `create_mv`.
    fn descriptor(
        format_version: i32,
        cols: &[(&str, &str, bool)], // name, type, nullable
    ) -> super::BaseTableDescriptor {
        super::BaseTableDescriptor {
            format_version,
            columns: cols
                .iter()
                .map(|(n, t, nullable)| super::BaseColumnDescriptor {
                    name: (*n).to_string(),
                    sql_type: (*t).to_string(),
                    nullable: *nullable,
                })
                .collect(),
        }
    }

    #[test]
    fn validate_ivm_pk_happy_path() {
        let base = descriptor(
            2,
            &[
                ("order_id", "BIGINT", false),
                ("customer", "STRING", true),
            ],
        );
        validate_ivm_primary_key(&["order_id".to_string()], &base).expect("ok");
    }

    #[test]
    fn validate_ivm_pk_rejects_v1_base_table() {
        let base = descriptor(1, &[("order_id", "BIGINT", false)]);
        let err = validate_ivm_primary_key(&["order_id".to_string()], &base).expect_err("err");
        assert!(matches!(err, ChangeError::IcebergFormatUnsupported { format_version: 1 }));
    }

    #[test]
    fn validate_ivm_pk_rejects_missing_column() {
        let base = descriptor(2, &[("customer", "STRING", true)]);
        let err = validate_ivm_primary_key(&["order_id".to_string()], &base).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::PrimaryKeyMissingFromBase { pk_col } if pk_col == "order_id"
        ));
    }

    #[test]
    fn validate_ivm_pk_rejects_nullable_column() {
        let base = descriptor(2, &[("order_id", "BIGINT", true)]);
        let err = validate_ivm_primary_key(&["order_id".to_string()], &base).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::PrimaryKeyNullable { pk_col } if pk_col == "order_id"
        ));
    }

    #[test]
    fn validate_ivm_pk_rejects_unhashable_type_double() {
        let base = descriptor(2, &[("order_id", "DOUBLE", false)]);
        let err = validate_ivm_primary_key(&["order_id".to_string()], &base).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::PrimaryKeyTypeUnsupported { pk_col, .. } if pk_col == "order_id"
        ));
    }

    #[test]
    fn validate_ivm_pk_rejects_unhashable_type_array() {
        let base = descriptor(2, &[("tags", "ARRAY<STRING>", false)]);
        let err = validate_ivm_primary_key(&["tags".to_string()], &base).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::PrimaryKeyTypeUnsupported { pk_col, .. } if pk_col == "tags"
        ));
    }

    #[test]
    fn validate_ivm_pk_accepts_decimal_and_string() {
        let base = descriptor(
            2,
            &[
                ("k1", "DECIMAL(18,2)", false),
                ("k2", "STRING", false),
            ],
        );
        validate_ivm_primary_key(
            &["k1".to_string(), "k2".to_string()],
            &base,
        )
        .expect("ok");
    }

    #[test]
    fn validate_ivm_pk_first_failure_wins_per_column_order() {
        // missing comes before nullable in column order; expect missing.
        let base = descriptor(
            2,
            &[
                ("present_but_nullable", "BIGINT", true),
            ],
        );
        let err = validate_ivm_primary_key(
            &[
                "absent".to_string(),
                "present_but_nullable".to_string(),
            ],
            &base,
        )
        .expect_err("err");
        assert!(matches!(
            err,
            ChangeError::PrimaryKeyMissingFromBase { pk_col } if pk_col == "absent"
        ));
    }
```

- [ ] **Step 2: Run tests and confirm they fail**

Run: `cargo test -p novarocks --lib connector::starrocks::managed::mv_ddl::tests::validate_ivm_pk`
Expected: FAIL — `BaseTableDescriptor`, `BaseColumnDescriptor`, and `validate_ivm_primary_key` don't exist yet.

- [ ] **Step 3: Implement the descriptor types and the validation function**

Add to `src/connector/starrocks/managed/mv_ddl.rs` near the other private helpers (e.g. just after `validate_unique_aggregate_physical_column_names` at line 379):

```rust
/// Lightweight projection of the iceberg base table that
/// `validate_ivm_primary_key` needs. Built once at the top of `create_mv`
/// from the loaded iceberg table; passing this struct keeps validation
/// pure and easy to unit-test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseColumnDescriptor {
    pub name: String,
    /// Uppercased SQL type as the analyzer/iceberg-schema mapper produced
    /// it (e.g. `BIGINT`, `STRING`, `DECIMAL(18,2)`, `ARRAY<STRING>`).
    pub sql_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseTableDescriptor {
    pub format_version: i32,
    pub columns: Vec<BaseColumnDescriptor>,
}

/// Validate that a parsed `PRIMARY KEY (col, ...)` clause on a CREATE
/// MATERIALIZED VIEW statement satisfies the IVM Phase-2 contract:
///
/// 1. The base table is iceberg format-version 2.
/// 2. Every PK column exists on the base table.
/// 3. Every PK column is NOT NULL on the base table.
/// 4. Every PK column has a hashable scalar type.
///
/// Errors fail fast in declared column order — the first mismatch wins.
/// Returns `Ok(())` on success and discards the PK list (PR-1 does not
/// persist it; PR-3 will).
pub(crate) fn validate_ivm_primary_key(
    pk_columns: &[String],
    base: &BaseTableDescriptor,
) -> Result<(), crate::connector::iceberg::changes::ChangeError> {
    use crate::connector::iceberg::changes::ChangeError;

    if base.format_version != 2 {
        return Err(ChangeError::IcebergFormatUnsupported {
            format_version: base.format_version,
        });
    }
    for pk in pk_columns {
        let col = base
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(pk))
            .ok_or_else(|| ChangeError::PrimaryKeyMissingFromBase {
                pk_col: pk.clone(),
            })?;
        if col.nullable {
            return Err(ChangeError::PrimaryKeyNullable {
                pk_col: col.name.clone(),
            });
        }
        if !is_hashable_pk_type(&col.sql_type) {
            return Err(ChangeError::PrimaryKeyTypeUnsupported {
                pk_col: col.name.clone(),
                ty: col.sql_type.clone(),
            });
        }
    }
    Ok(())
}

/// Hashable scalar-type predicate for IVM Phase-2 PRIMARY KEY columns.
/// Accepts: BIGINT, INT, SMALLINT, TINYINT, STRING, VARCHAR, DATE,
/// DATETIME, DECIMAL (with or without precision/scale).
/// Rejects: BOOLEAN, FLOAT, DOUBLE, ARRAY, MAP, STRUCT, JSON.
fn is_hashable_pk_type(sql_type: &str) -> bool {
    let upper = sql_type.to_ascii_uppercase();
    let head = upper.split(['(', '<']).next().unwrap_or("").trim();
    matches!(
        head,
        "BIGINT"
            | "INT"
            | "INTEGER"
            | "SMALLINT"
            | "TINYINT"
            | "STRING"
            | "VARCHAR"
            | "CHAR"
            | "DATE"
            | "DATETIME"
            | "TIMESTAMP"
            | "DECIMAL"
    )
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cargo test -p novarocks --lib connector::starrocks::managed::mv_ddl::tests::validate_ivm_pk`
Expected: 8 passed; 0 failed.

- [ ] **Step 5: Run the full mv_ddl test module**

Run: `cargo test -p novarocks --lib connector::starrocks::managed::mv_ddl`
Expected: all tests pass — the existing ones plus the eight new ones.

- [ ] **Step 6: Commit**

```bash
git add src/connector/starrocks/managed/mv_ddl.rs
git commit -m "feat(mv): validate_ivm_primary_key with full unit coverage

Adds BaseColumnDescriptor / BaseTableDescriptor lightweight projections
and a pure-function validator that checks: iceberg v2, PK column exists,
NOT NULL, hashable scalar type. Validator is unwired this commit; the
next two commits hook it into create_mv and create_iceberg_mv.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Wire Validation Into `create_mv` (Managed-Lake-Stored MV Path)

**Files:**
- Modify: `src/connector/starrocks/managed/mv_ddl.rs` (`create_mv` body, near the top after `analyze_mv_select`)
- Modify: `src/connector/starrocks/managed/mv_refresh.rs` (`load_current_iceberg_base_table` is already public — no change there; this task only consumes it)

- [ ] **Step 1: Add a helper that builds `BaseTableDescriptor` from a loaded iceberg table**

Append to `src/connector/starrocks/managed/mv_ddl.rs` next to `validate_ivm_primary_key`:

```rust
/// Build the `BaseTableDescriptor` projection from an already-loaded
/// iceberg table. Used by `create_mv` and `create_iceberg_mv` before
/// invoking `validate_ivm_primary_key`.
pub(crate) fn descriptor_from_loaded(
    loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
) -> BaseTableDescriptor {
    let format_version = loaded.table.metadata().format_version() as i32;
    let columns = loaded
        .columns
        .iter()
        .map(|col| BaseColumnDescriptor {
            name: col.name.clone(),
            sql_type: format!("{}", col.data_type),
            nullable: col.nullable,
        })
        .collect();
    BaseTableDescriptor {
        format_version,
        columns,
    }
}
```

If `iceberg::table::TableMetadata::format_version()` is not available on the version of `iceberg-rust` in `Cargo.lock`, fall back to `loaded.table.metadata().format_version` (field access) or whatever the existing codebase uses elsewhere — grep `format_version` under `src/connector/iceberg/` to confirm the spelling, and adjust the helper accordingly. The contract is: return the v1/v2/v3 integer.

If `ColumnDef.data_type` (`SqlType`) doesn't have a `Display` impl, use `format!("{:?}", col.data_type)` instead — the test predicate only inspects the head token (`BIGINT`, `ARRAY`, etc.), so debug-form is acceptable for the validator's purposes. Note any deviation in the commit message.

- [ ] **Step 2: Hook the validation into `create_mv`**

Locate `create_mv` (line 103). After `let analysis = analyze_mv_select(state, current_database, &stmt.select_query)?;` (around line 147) and before `let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;`, insert:

```rust
    // IVM Phase-2 PRIMARY KEY validation. Only runs when the user opted in
    // by writing `PRIMARY KEY (...)` in the DDL; otherwise behavior is
    // unchanged.
    if let Some(pk_cols) = stmt.primary_key.as_deref() {
        let refs = extract_base_table_refs(&analysis.resolved_refs)?;
        let base_ref = refs.first().ok_or_else(|| {
            "PRIMARY KEY on materialized view requires exactly one iceberg base table".to_string()
        })?;
        if refs.len() > 1 {
            return Err(
                "PRIMARY KEY on materialized view requires exactly one iceberg base table"
                    .to_string(),
            );
        }
        let loaded = crate::connector::starrocks::managed::mv_refresh::load_current_iceberg_base_table(
            state, base_ref,
        )?;
        let descriptor = descriptor_from_loaded(&loaded);
        validate_ivm_primary_key(pk_cols, &descriptor).map_err(|e| e.to_string())?;
    }
```

- [ ] **Step 3: Add an integration test exercising the full `create_mv` rejection path**

Append inside `mod tests` of `mv_ddl.rs`. (This requires a harness that constructs a minimal `StandaloneState` with an iceberg base table — if the existing test module doesn't already have such a harness, skip this step; the SQL regression case in Task 7 covers the full integration. Document the skip in the commit message.)

- [ ] **Step 4: Build and run all mv_ddl tests**

Run: `cargo test -p novarocks --lib connector::starrocks::managed::mv_ddl`
Expected: all tests pass.

Run: `cargo build`
Expected: PASS, no warnings about unused `descriptor_from_loaded`.

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/managed/mv_ddl.rs
git commit -m "feat(mv): wire IVM PRIMARY KEY validation into create_mv

When the parsed CreateMaterializedViewStmt carries a PRIMARY KEY clause,
create_mv now loads the (single) iceberg base table, projects it into a
BaseTableDescriptor, and runs validate_ivm_primary_key before any catalog
or tablet mutation. MVs without PRIMARY KEY take the same path as before.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Wire Validation Into `create_iceberg_mv` (Iceberg-Stored MV Path)

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh_iceberg.rs:48-`

- [ ] **Step 1: Read the function signature and entry point**

Run: `sed -n '48,120p' src/connector/starrocks/managed/mv_refresh_iceberg.rs`
Note where `analyze_mv_select` (or its equivalent) and `load_current_iceberg_base_table` are called. The hook point is the same idea as in Task 5: after the base-table identity is known, before any iceberg metadata write or catalog mutation.

- [ ] **Step 2: Insert the validation block**

In `create_iceberg_mv`, after the base-table reference has been resolved and the iceberg table has been loaded (look for the existing `load_current_iceberg_base_table` call) and before any side effect (catalog insert, metadata write), add the same validation block as Task 5 Step 2:

```rust
    if let Some(pk_cols) = stmt.primary_key.as_deref() {
        // Reuse the same descriptor + validator as the managed-lake path.
        let descriptor =
            crate::connector::starrocks::managed::mv_ddl::descriptor_from_loaded(&loaded);
        crate::connector::starrocks::managed::mv_ddl::validate_ivm_primary_key(
            pk_cols, &descriptor,
        )
        .map_err(|e| e.to_string())?;
    }
```

`loaded` here is whatever local binding `create_iceberg_mv` already has for the `IcebergLoadedTable`. If the function does not load the base table on the create path (some create paths only resolve the ref without loading), add an explicit `load_current_iceberg_base_table` call inside the `if let Some(pk_cols) = ...` block so we only pay the cost when PK is present.

- [ ] **Step 3: Build**

Run: `cargo build`
Expected: PASS.

- [ ] **Step 4: Run mv_refresh_iceberg-related tests**

Run: `cargo test -p novarocks --lib connector::starrocks::managed::mv_refresh_iceberg`
Expected: all existing tests pass; no new ones added (covered by SQL regression in Task 7).

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/managed/mv_refresh_iceberg.rs
git commit -m "feat(mv): wire IVM PRIMARY KEY validation into create_iceberg_mv

Iceberg-stored MV path now runs the same validate_ivm_primary_key as the
managed-lake-stored path, gated on stmt.primary_key.is_some(). The
validation happens before any iceberg-side catalog or metadata write so
rejection is fully transactional from the user's perspective.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: SQL Regression Case — `managed_lake_mv_ivm_pk_invalid`

**Files:**
- Create: `sql-tests/write-path/sql/managed_lake_mv_ivm_pk_invalid.sql`
- Create: `sql-tests/write-path/result/managed_lake_mv_ivm_pk_invalid.result`

- [ ] **Step 1: Write the SQL case**

Write `sql-tests/write-path/sql/managed_lake_mv_ivm_pk_invalid.sql`:

```sql
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,validation
-- Test Objective:
-- 1. Validate that CREATE MATERIALIZED VIEW with PRIMARY KEY rejects DDL
--    that violates the IVM Phase-2 contract before any catalog mutation.
-- 2. Cover: missing column, nullable column, unhashable type, empty PK
--    list (parser-level), duplicate PK columns (parser-level).
-- 3. Confirm that omitting PRIMARY KEY is unchanged behavior.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_ivm_pk_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_ivm_pk_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_ivm_pk_${uuid0}.ns_${uuid0};
-- order_id is NOT NULL; customer is nullable; tags is an array.
CREATE TABLE mv_ivm_pk_${uuid0}.ns_${uuid0}.orders (
  order_id BIGINT NOT NULL,
  customer STRING,
  amount DOUBLE,
  tags ARRAY<STRING>
);
INSERT INTO mv_ivm_pk_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 100.0, ['x']),
  (2, 'B', 200.0, ['y']);

-- query 2: PK references a column that does not exist on the base table.
-- @expect_error=PRIMARY KEY column `bogus` does not exist
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_missing
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (bogus)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 3: PK references a nullable column.
-- @expect_error=PRIMARY KEY column `customer` must be NOT NULL
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_nullable
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (customer)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 4: PK references an unhashable scalar type (DOUBLE).
-- @expect_error=PRIMARY KEY column `amount` has unsupported type
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_double
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (amount)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 5: PK references an unhashable composite type (ARRAY).
-- @expect_error=PRIMARY KEY column `tags` has unsupported type
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_array
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (tags)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 6: empty PK list — parser-level rejection.
-- @expect_error=PRIMARY KEY clause requires at least one column
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_empty
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY ()
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 7: duplicate PK columns — parser-level rejection.
-- @expect_error=duplicate column `order_id` in PRIMARY KEY clause
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_dupe
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (order_id, order_id)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 8: happy path — valid PK on BIGINT NOT NULL column. Just confirm
-- creation succeeds. PR-1 does not persist the PK; later refresh tests
-- belong to PR-3+.
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_ok
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (order_id)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 9: confirm that omitting PRIMARY KEY still works as before.
CREATE MATERIALIZED VIEW ${case_db}.mv_no_pk
DISTRIBUTED BY HASH(customer) BUCKETS 2
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;
```

- [ ] **Step 2: Generate the expected result file**

Run the test in `record` mode to materialize the result file:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
SERVER_PID=$!
sleep 5  # one-shot wait for server startup

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path --only managed_lake_mv_ivm_pk_invalid --mode record

kill $SERVER_PID
```

Verify `sql-tests/write-path/result/managed_lake_mv_ivm_pk_invalid.result` was created and contains entries for each query — error messages for queries 2-7 and a clean creation for queries 8-9.

- [ ] **Step 3: Verify the case in `verify` mode**

Restart the standalone server and re-run in `verify`:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
SERVER_PID=$!
sleep 5

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path --only managed_lake_mv_ivm_pk_invalid --mode verify

kill $SERVER_PID
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/write-path/sql/managed_lake_mv_ivm_pk_invalid.sql sql-tests/write-path/result/managed_lake_mv_ivm_pk_invalid.result
git commit -m "test(mv): SQL regression for IVM PRIMARY KEY CREATE-time rejection

Covers the seven rejection paths (missing column, nullable column,
DOUBLE PK, ARRAY PK, empty PK list, duplicate PK columns) plus two
happy paths (valid PK accepted, no PK clause unchanged).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Format, Clippy, And Final Build

**Files:** none (verification only)

- [ ] **Step 1: Format**

Run: `cargo fmt --all`
Expected: no diff, or clean reformat. If any files change, include them in the final commit.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: no warnings. If clippy complains about the `#[allow(dead_code)]` on unused `ChangeError` variants, that's expected and intentional for PR-1; the attribute is the silencer.

- [ ] **Step 3: Full library test**

Run: `cargo test -p novarocks --lib`
Expected: all tests pass.

- [ ] **Step 4: Full debug build**

Run: `cargo build`
Expected: PASS, no warnings.

- [ ] **Step 5: If formatting produced changes, amend or commit them**

```bash
git status
# If anything is modified by `cargo fmt`:
git add -u
git commit -m "chore: cargo fmt after PR-1 IVM PRIMARY KEY validation

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-04-29-iceberg-ivm-phase2-design.md` §8 and §11 PR-1 line):**

| Spec requirement | Task |
|---|---|
| §8.1 (1) PK column exists in base | Task 4 (`validate_ivm_pk_rejects_missing_column`) |
| §8.1 (2) PK column NOT NULL | Task 4 (`validate_ivm_pk_rejects_nullable_column`) |
| §8.1 (3) PK column hashable scalar | Task 4 (`validate_ivm_pk_rejects_unhashable_type_*`) |
| §8.1 (4) base table is iceberg v2 | Task 4 (`validate_ivm_pk_rejects_v1_base_table`) |
| §8.1 (5) no non-deterministic functions | **deferred to PR-3+** (out of PR-1 scope; only relevant when REFRESH actually goes through the new IVM path) |
| §8.1 (6) reversible aggregates only | **deferred to PR-3+** (same reason) |
| §6 ChangeError enum defined | Task 3 |
| §11 PR-1 "PRIMARY KEY parsing + validation, new error type, CREATE-time fail-fast" | Tasks 1, 2, 4, 5, 6 |
| §11 PR-1 "covers test 02_create_mv_pk_invalid" | Task 7 (renamed to `managed_lake_mv_ivm_pk_invalid` to match the actual repo convention `sql-tests/write-path/sql/<name>.sql`) |

**Deviations from the spec, called out for reviewer:**

1. **Test path:** spec §10.2 says `tests/sql-test-runner/cases/mv-ivm/`, but that directory doesn't exist. The repo's actual layout is `sql-tests/<suite>/sql/` + `sql-tests/<suite>/result/`. PR-1 lands the case under `sql-tests/write-path/` next to the existing `managed_lake_mv_aggregate_ivm.sql` (same lifecycle layer). If a `mv-ivm` suite directory is later created, this case can be moved with `git mv` in PR-6.
2. **Validation file:** spec §8 says `src/lower/mv_ddl.rs (新增) 或扩展现有 MV DDL 校验入口`. PR-1 takes the second option — extends the existing `src/connector/starrocks/managed/mv_ddl.rs` rather than creating a new file under `src/lower/`. This keeps validation co-located with the rest of MV DDL.
3. **PK persistence:** PR-1 validates the PK and discards it. Persisting `primary_key_columns` into `StoredMaterializedView` is deferred to PR-3 where the actual ROW_ID-computation path lands and needs to read it back.
4. **§8.1 checks 5 and 6** (non-deterministic functions, reversible aggregates) are deferred to a later PR for the reason listed in the table above.

**Type consistency check:** every signature defined in PR-1 is consumed only in PR-1:
- `BaseColumnDescriptor` and `BaseTableDescriptor` — defined Task 4, consumed Tasks 4–6.
- `validate_ivm_primary_key(pk_columns: &[String], base: &BaseTableDescriptor) -> Result<(), ChangeError>` — defined Task 4, consumed Tasks 5 and 6.
- `descriptor_from_loaded(&IcebergLoadedTable) -> BaseTableDescriptor` — defined Task 5, consumed Tasks 5 and 6.
- `ChangeError` variants — defined Task 3, consumed Tasks 4 and unit tests.

**Placeholder scan:** none of the `TBD` / `add appropriate error handling` / "similar to Task N" patterns appear. Each task includes the full code, test code, command lines, and commit message.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-29-iceberg-ivm-phase2-pr1-pk-validation.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
