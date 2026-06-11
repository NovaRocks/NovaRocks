# Iceberg Time Travel & Branch / Tag — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add time-travel SELECT, ALTER TABLE CREATE/DROP BRANCH|TAG, and branch-qualified DML (`t.branch_<x>`) on Iceberg tables in NovaRocks's standalone-server path, with per-ref OCC isolation.

**Architecture:** A single `IcebergRefBinding { snapshot_id, ref_name?, ref_kind? }` flows from SQL through the analyzer (where ref / timestamp → snapshot_id resolution happens once) into the read path (`build_read_snapshot_at`) and the six commit modules (which become `target_ref`-aware via `CommitCtx`). DDL becomes a metadata-only commit (`SetSnapshotRef` / `RemoveSnapshotRef`) in a new `commit/ref_action.rs` module.

**Spec:** [`docs/design/specs/2026-05-05-iceberg-time-travel-and-branch-tag-design.md`](../specs/2026-05-05-iceberg-time-travel-and-branch-tag-design.md)

**Tech stack:** Rust, `sqlparser-rs` 0.61, vendored `iceberg-rust` 0.9.0, async runtime via existing `block_on_iceberg`, sql-tests harness.

---

## File Structure

### New files

| Path | Responsibility |
| --- | --- |
| `src/sql/analyzer/iceberg_ref.rs` | `IcebergRefBinding`, `IcebergDmlTarget`, `IcebergRefKind`; resolve a `TableFactor`'s time-travel clause + ref suffix to a binding. |
| `src/sql/parser/dialect/alter_iceberg_ref.rs` | Probe + parse for `ALTER TABLE … CREATE/DROP BRANCH|TAG …`. |
| `src/sql/analyzer/alter_iceberg_ref.rs` | Validate the parsed `AlterIcebergRefAction` against table metadata, produce a `RefActionPlan`. |
| `src/connector/iceberg/commit/ref_action.rs` | `RefActionPlan`, `execute_ref_action`, idempotency helpers for branch/tag DDL. |
| `src/exec/node/iceberg_ref_action.rs` | `IcebergRefActionNode` ExecNode wrapping `RefActionPlan`. |
| `src/lower/ddl/iceberg_ref.rs` | Lower the analyzer's DDL output to `IcebergRefActionNode`. |
| `sql-tests/iceberg/sql/iceberg_time_travel_select.sql` | SELECT FOR VERSION/TIMESTAMP AS OF coverage. |
| `sql-tests/iceberg/sql/iceberg_branch_tag_ddl.sql` | CREATE/DROP BRANCH/TAG coverage. |
| `sql-tests/iceberg/sql/iceberg_branch_write.sql` | INSERT/UPDATE/DELETE on branches. |
| `sql-tests/iceberg/sql/iceberg_ref_negative.sql` | analyzer-time errors. |

### Modified files

| Path | Change |
| --- | --- |
| `src/sql/parser/ast/mod.rs` | Add `Statement::AlterIcebergRef` variant + `AlterIcebergRefAction` types. |
| `src/sql/parser/dialect/mod.rs` | Register the new probe module. |
| `src/sql/parser/mod.rs` | Add `looks_like_alter_iceberg_ref` dispatch ahead of MV probes. |
| `src/sql/analyzer/mod.rs` | Dispatch `Statement::AlterIcebergRef`. |
| `src/sql/analyzer/resolve_from.rs` | Post-process `TableFactor::Table` to extract binding. |
| `src/connector/iceberg/read.rs` | Add `build_read_snapshot_at(table, snapshot_id)`. |
| `src/connector/iceberg/catalog/registry.rs` | Add `extract_data_files_with_stats_at(table, snapshot_id)`. |
| `src/connector/iceberg/catalog/backend.rs` | `IcebergTableSource::build_table_def_at` impl. |
| `src/connector/backend.rs` | `TableSource` trait gets `build_table_def_at` default impl. |
| `src/connector/iceberg/commit/action.rs` | `CommitCtx.target_ref: &'a str`. |
| `src/connector/iceberg/commit/{fast_append,row_delta,row_delta_dv,overwrite,update_cow,rewrite_data_files}.rs` | Replace `MAIN_BRANCH` literal with `ctx.target_ref`; replace `current_snapshot()` parent lookup with `metadata.refs().get(ctx.target_ref)…`. |
| `src/connector/iceberg/commit/mod.rs` | Re-export ref_action; allow ref-action dispatch without `AddSnapshot`. |
| `src/exec/node/mod.rs` | Add `ExecNodeKind::IcebergRefAction(IcebergRefActionNode)`. |
| `src/lower/node/mod.rs` | Dispatch DDL ExecNode. |
| `src/engine/{mutation_flow,delete_flow,statement,mod}.rs` | Plumb `dml_target` (read_binding + write_ref) through to commit dispatch; detect `t.branch_<x>` suffix. |

---

## Task 1: Foundation types — `IcebergRefBinding`

**Files:**
- Create: `src/sql/analyzer/iceberg_ref.rs`
- Modify: `src/sql/analyzer/mod.rs`

- [ ] **Step 1: Write failing unit test for `IcebergRefBinding` constructors and `Display` for `ref_repr`**

Create `src/sql/analyzer/iceberg_ref.rs`:

```rust
//! Resolve Iceberg time-travel clauses + DML branch suffixes into a single
//! `IcebergRefBinding` that the read and commit paths consume.

use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergRefKind {
    Branch,
    Tag,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergRefBinding {
    pub snapshot_id: i64,
    pub ref_name: Option<String>,
    pub ref_kind: Option<IcebergRefKind>,
}

impl IcebergRefBinding {
    pub fn ref_repr(&self) -> String {
        match (&self.ref_name, &self.ref_kind) {
            (Some(name), Some(IcebergRefKind::Branch)) => format!("branch '{name}'"),
            (Some(name), Some(IcebergRefKind::Tag)) => format!("tag '{name}'"),
            (Some(name), None) => format!("ref '{name}'"),
            (None, _) => format!("snapshot {}", self.snapshot_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergDmlTarget {
    pub read_binding: IcebergRefBinding,
    pub write_ref: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_repr_branch() {
        let b = IcebergRefBinding {
            snapshot_id: 7,
            ref_name: Some("dev".into()),
            ref_kind: Some(IcebergRefKind::Branch),
        };
        assert_eq!(b.ref_repr(), "branch 'dev'");
    }

    #[test]
    fn ref_repr_tag() {
        let b = IcebergRefBinding {
            snapshot_id: 7,
            ref_name: Some("v1".into()),
            ref_kind: Some(IcebergRefKind::Tag),
        };
        assert_eq!(b.ref_repr(), "tag 'v1'");
    }

    #[test]
    fn ref_repr_snapshot_only() {
        let b = IcebergRefBinding {
            snapshot_id: 42,
            ref_name: None,
            ref_kind: None,
        };
        assert_eq!(b.ref_repr(), "snapshot 42");
    }
}
```

- [ ] **Step 2: Wire the module**

Edit `src/sql/analyzer/mod.rs`, add at top of file (next to `pub mod helpers;` etc.):

```rust
pub mod iceberg_ref;
```

- [ ] **Step 3: Run tests — expect PASS (no failing tests yet, just module wiring)**

```bash
cargo test --lib sql::analyzer::iceberg_ref
```

Expected: 3 tests pass.

- [ ] **Step 4: Run clippy**

```bash
cargo clippy --lib -- -D warnings
```

- [ ] **Step 5: Commit**

```bash
git add src/sql/analyzer/iceberg_ref.rs src/sql/analyzer/mod.rs
git commit -m "feat(iceberg): introduce IcebergRefBinding foundation type"
```

---

## Task 2: `build_read_snapshot_at(table, snapshot_id)`

**Files:**
- Modify: `src/connector/iceberg/read.rs`

- [ ] **Step 1: Read the existing function**

```bash
sed -n '160,250p' src/connector/iceberg/read.rs
```

The current `build_read_snapshot(table)` (around line 166–200) reads `metadata.current_snapshot()`. The change: extract the body into `build_read_snapshot_at(table, snapshot_id)` that takes an explicit `snapshot_id: i64`, and rewrite `build_read_snapshot` as a thin wrapper that calls `_at` with the current snapshot id (or returns the empty-table shape).

- [ ] **Step 2: Write a failing unit test for the new entry point**

Append to the existing `#[cfg(test)] mod tests` block at the bottom of `src/connector/iceberg/read.rs` (or add a new one if absent):

```rust
#[cfg(test)]
mod tests_at {
    use super::*;
    // Reuse the in-memory iceberg fixture builder used elsewhere in the crate.
    // The exact fixture helper name is `crate::connector::iceberg::test_utils::make_table_with_two_snapshots`
    // — confirm the helper exists; if not, factor out an inline setup that
    // creates a table with two snapshots S1 and S2 (each with one data file).

    #[tokio::test]
    async fn build_read_snapshot_at_picks_historical_snapshot() {
        let (table, s1, s2) = test_utils::make_table_with_two_snapshots().await;

        let snap_at_s1 = build_read_snapshot_at(&table, s1).expect("ok");
        assert_eq!(snap_at_s1.snapshot_id, Some(s1));
        assert_eq!(snap_at_s1.files.len(), 1);

        let snap_at_s2 = build_read_snapshot_at(&table, s2).expect("ok");
        assert_eq!(snap_at_s2.snapshot_id, Some(s2));
        assert_eq!(snap_at_s2.files.len(), 2);
    }

    #[tokio::test]
    async fn build_read_snapshot_at_missing_id_errors() {
        let (table, _, _) = test_utils::make_table_with_two_snapshots().await;
        let err = build_read_snapshot_at(&table, 999_999).unwrap_err();
        assert!(err.contains("snapshot 999999 not found"), "{err}");
    }
}
```

(If `test_utils::make_table_with_two_snapshots` does not yet exist, add it as a small helper next to existing tests in `read.rs` that constructs a memory `iceberg::table::Table` with two append snapshots — pattern used by other commit-module tests in `src/connector/iceberg/commit/*::tests` is the model.)

- [ ] **Step 3: Run the new test — expect FAIL ("function not defined")**

```bash
cargo test --lib connector::iceberg::read::tests_at -- --nocapture
```

- [ ] **Step 4: Implement `build_read_snapshot_at`**

Inside `src/connector/iceberg/read.rs`, replace the existing `build_read_snapshot` body. Final shape:

```rust
pub(crate) fn build_read_snapshot_at(
    table: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<IcebergReadSnapshot, String> {
    use crate::connector::iceberg::catalog::registry::block_on_iceberg;
    use iceberg::spec::{DataContentType, DataFileFormat, ManifestContentType, ManifestStatus};

    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found"))?;

    // (move existing body here, replacing references to `snapshot` with the
    // local `snapshot` and dropping the `metadata.current_snapshot()` match.)

    // ... existing body unchanged ...
    Ok(IcebergReadSnapshot {
        snapshot_id: Some(snapshot_id),
        files,
    })
}

pub(crate) fn build_read_snapshot(
    table: &iceberg::table::Table,
) -> Result<IcebergReadSnapshot, String> {
    let metadata = table.metadata();
    match metadata.current_snapshot() {
        Some(s) => build_read_snapshot_at(table, s.snapshot_id()),
        None => Ok(IcebergReadSnapshot {
            snapshot_id: None,
            files: Vec::new(),
        }),
    }
}
```

- [ ] **Step 5: Run the new tests — expect PASS**

```bash
cargo test --lib connector::iceberg::read::tests_at
cargo test --lib connector::iceberg::read
```

Expected: both old and new tests pass; old `build_read_snapshot` callers untouched.

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/read.rs
git commit -m "feat(iceberg): build_read_snapshot_at for explicit snapshot id"
```

---

## Task 3: `extract_data_files_with_stats_at(table, snapshot_id)`

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs`

- [ ] **Step 1: Locate the existing function (around line 869)**

```bash
sed -n '860,910p' src/connector/iceberg/catalog/registry.rs
```

It calls `build_read_snapshot(table)` then transforms each file. The change: parameterise the snapshot id and route through `build_read_snapshot_at`. Keep `extract_data_files_with_stats` as a wrapper.

- [ ] **Step 2: Write the failing unit test**

Append to the existing tests module in `registry.rs`:

```rust
#[tokio::test]
async fn extract_data_files_with_stats_at_returns_historical_files() {
    let (table, s1, _s2) = crate::connector::iceberg::read::test_utils::make_table_with_two_snapshots().await;
    let files = extract_data_files_with_stats_at(&table, s1).expect("ok");
    assert_eq!(files.len(), 1);
}
```

- [ ] **Step 3: Run — expect FAIL**

```bash
cargo test --lib connector::iceberg::catalog::registry::tests::extract_data_files_with_stats_at_returns_historical_files
```

- [ ] **Step 4: Implement**

Add the new function next to the existing one (around line 869):

```rust
pub(crate) fn extract_data_files_with_stats_at(
    table: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<Vec<DataFileWithStats>, String> {
    let metadata = table.metadata();
    let read_snapshot = crate::connector::iceberg::read::build_read_snapshot_at(table, snapshot_id)?;
    read_snapshot
        .files
        .into_iter()
        .map(|file| {
            // (copy of the existing closure body verbatim — same data_file_with_stats build)
            // see existing extract_data_files_with_stats for the reference impl
        })
        .collect()
}

pub(crate) fn extract_data_files_with_stats(
    table: &iceberg::table::Table,
) -> Result<Vec<DataFileWithStats>, String> {
    match table.metadata().current_snapshot() {
        Some(s) => extract_data_files_with_stats_at(table, s.snapshot_id()),
        None => Ok(Vec::new()),
    }
}
```

(Move the closure body into `_at`; rewrite the wrapper to call it.)

- [ ] **Step 5: Run tests — expect PASS**

```bash
cargo test --lib connector::iceberg::catalog::registry
```

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/catalog/registry.rs
git commit -m "feat(iceberg): extract_data_files_with_stats_at for explicit snapshot id"
```

---

## Task 4: `TableSource::build_table_def_at` trait method + Iceberg override

**Files:**
- Modify: `src/connector/backend.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`

- [ ] **Step 1: Locate the trait**

```bash
grep -n "trait TableSource" src/connector/backend.rs
```

- [ ] **Step 2: Add the default trait method**

In `src/connector/backend.rs`, immediately after the existing `fn build_table_def(&self, ...) -> Result<TableDef, String>` declaration:

```rust
/// Phase 1 entry point for time-travel-aware table-def construction.
/// Default impl ignores the binding and delegates to `build_table_def`,
/// which is correct for connectors that do not have time-travel semantics.
fn build_table_def_at(
    &self,
    table: &ResolvedTable,
    _binding: Option<crate::sql::analyzer::iceberg_ref::IcebergRefBinding>,
) -> Result<TableDef, String> {
    self.build_table_def(table)
}
```

- [ ] **Step 3: Override in `IcebergTableSource`**

In `src/connector/iceberg/catalog/backend.rs`, replace the `IcebergTableSource::build_table_def` body so that `_at` is the canonical implementation and `build_table_def` becomes a thin wrapper:

```rust
fn build_table_def_at(
    &self,
    table: &ResolvedTable,
    binding: Option<crate::sql::analyzer::iceberg_ref::IcebergRefBinding>,
) -> Result<TableDef, String> {
    let guard = self.registry.read().expect("iceberg catalog read lock");
    let entry = guard.get(&table.catalog)?;
    let loaded = reg_load_table(&entry, &table.namespace, &table.table)?;
    let snapshot_id = match binding.as_ref().map(|b| b.snapshot_id) {
        Some(id) => Some(id),
        None => loaded.table.metadata().current_snapshot_id(),
    };
    let data_files = match snapshot_id {
        None => Vec::new(),
        Some(id) => {
            if let Some(cached) =
                entry.cached_data_files(&table.namespace, &table.table, Some(id))?
            {
                cached
            } else {
                let extracted =
                    super::registry::extract_data_files_with_stats_at(&loaded.table, id)?;
                entry.cache_data_files(
                    &table.namespace,
                    &table.table,
                    Some(id),
                    extracted.clone(),
                )?;
                extracted
            }
        }
    };
    build_iceberg_table_def_with_data_files(
        &entry,
        &table.namespace,
        &table.table,
        loaded,
        data_files,
    )
}

fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
    self.build_table_def_at(table, None)
}
```

(If the existing `cached_data_files` signature uses `Option<i64>`, keep `Some(id)`; if it uses `i64`, pass `id` directly. Confirm by reading the helper.)

- [ ] **Step 4: Build to verify trait wiring**

```bash
cargo build --lib
```

Expected: clean.

- [ ] **Step 5: Add a unit test**

In `src/connector/iceberg/catalog/backend.rs::tests`:

```rust
#[tokio::test]
async fn build_table_def_at_returns_historical_files() {
    let (registry, resolved, s1, _s2) =
        super::test_utils::registry_with_two_snapshots().await;
    let source = IcebergTableSource::new(registry);

    let binding = crate::sql::analyzer::iceberg_ref::IcebergRefBinding {
        snapshot_id: s1,
        ref_name: None,
        ref_kind: None,
    };
    let def = source
        .build_table_def_at(&resolved, Some(binding))
        .expect("ok");
    let TableStorage::S3ParquetFiles { files, .. } = def.storage else {
        panic!("expected parquet files");
    };
    assert_eq!(files.len(), 1);
}
```

- [ ] **Step 6: Run tests — expect PASS**

```bash
cargo test --lib connector::iceberg::catalog::backend
```

- [ ] **Step 7: Commit**

```bash
git add src/connector/backend.rs src/connector/iceberg/catalog/backend.rs
git commit -m "feat(iceberg): TableSource::build_table_def_at threaded into IcebergTableSource"
```

---

## Task 5: AST + parser dialect probe for `ALTER TABLE … BRANCH/TAG`

**Files:**
- Modify: `src/sql/parser/ast/mod.rs`
- Create: `src/sql/parser/ast/iceberg_ref.rs`
- Create: `src/sql/parser/dialect/alter_iceberg_ref.rs`
- Modify: `src/sql/parser/dialect/mod.rs`
- Modify: `src/sql/parser/mod.rs`

- [ ] **Step 1: Add the new AST module**

Create `src/sql/parser/ast/iceberg_ref.rs`:

```rust
//! Raw AST for `ALTER TABLE … (CREATE|DROP) [OR REPLACE] [IF [NOT] EXISTS]
//! (BRANCH|TAG) <name> [AS OF VERSION <id>] [retention …]`.

use crate::sql::parser::ast::ObjectName;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AlterIcebergRefAction {
    CreateBranch {
        name: String,
        anchor: SnapshotAnchor,
        if_not_exists: bool,
        replace: bool,
        ignored_options: Vec<String>,
    },
    CreateTag {
        name: String,
        anchor: SnapshotAnchor,
        if_not_exists: bool,
        replace: bool,
        ignored_options: Vec<String>,
    },
    DropBranch {
        name: String,
        if_exists: bool,
    },
    DropTag {
        name: String,
        if_exists: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SnapshotAnchor {
    SnapshotId(i64),
    CurrentMain,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AlterIcebergRefStmt {
    pub table: ObjectName,
    pub action: AlterIcebergRefAction,
}
```

- [ ] **Step 2: Re-export from AST mod**

Edit `src/sql/parser/ast/mod.rs`:
- Add at top with the other module decls: `pub mod iceberg_ref;`
- Add at the use-list for the file: `pub(crate) use iceberg_ref::{AlterIcebergRefAction, AlterIcebergRefStmt, SnapshotAnchor};`
- Extend the `Statement` enum with `AlterIcebergRef(AlterIcebergRefStmt),`

- [ ] **Step 3: Write failing parser unit tests**

Create `src/sql/parser/dialect/alter_iceberg_ref.rs`:

```rust
//! Parser probe + parse for `ALTER TABLE <name> (CREATE|DROP) [OR REPLACE]
//! [IF [NOT] EXISTS] (BRANCH|TAG) <ident> [AS OF VERSION <int>]
//! [retention-clause-tokens]`.
//!
//! The retention clause is consumed by token until the statement terminator
//! and stashed verbatim in `ignored_options`; phase 1 emits a warning at
//! analyzer time and discards the contents.

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::{convert_object_name, peek_word_eq};
use crate::sql::parser::ast::{
    AlterIcebergRefAction, AlterIcebergRefStmt, SnapshotAnchor, Statement,
};

pub(crate) fn looks_like_alter_iceberg_ref(parser: &Parser<'_>) -> bool {
    if !parser.peek_keyword(Keyword::ALTER) {
        return false;
    }
    if !peek_word_eq(parser, 1, "TABLE") {
        return false;
    }
    // Walk forward past the table name to reach the action token. Worst case
    // table name is `cat.ns.tbl` = 5 tokens; cap the look-ahead at 8.
    for offset in 3..10 {
        if peek_word_eq(parser, offset, "CREATE") || peek_word_eq(parser, offset, "DROP") {
            // Confirm the next non-modifier word is BRANCH or TAG.
            for inner in (offset + 1)..(offset + 6) {
                let w = parser.peek_nth_token_ref(inner);
                let token = match &w.token {
                    Token::Word(w) => w.value.as_str(),
                    _ => return false,
                };
                if token.eq_ignore_ascii_case("BRANCH") || token.eq_ignore_ascii_case("TAG") {
                    return true;
                }
                if !["OR", "REPLACE", "IF", "NOT", "EXISTS"]
                    .iter()
                    .any(|s| token.eq_ignore_ascii_case(s))
                {
                    return false;
                }
            }
        }
    }
    false
}

pub(crate) fn parse_alter_iceberg_ref(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser.expect_keyword(Keyword::ALTER).map_err(|e| e.to_string())?;
    parser.expect_keyword(Keyword::TABLE).map_err(|e| e.to_string())?;
    let table = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;

    let is_create = parser.parse_keyword(Keyword::CREATE);
    if !is_create {
        parser.expect_keyword(Keyword::DROP).map_err(|e| e.to_string())?;
    }

    let replace = is_create && parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    let if_not_exists =
        is_create && parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let if_exists = !is_create && parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

    let kind_word = parser.next_token();
    let kind = match &kind_word.token {
        Token::Word(w) if w.value.eq_ignore_ascii_case("BRANCH") => "BRANCH",
        Token::Word(w) if w.value.eq_ignore_ascii_case("TAG") => "TAG",
        other => return Err(format!("expected BRANCH or TAG, got {other:?}")),
    };

    let name = parser
        .parse_identifier()
        .map_err(|e| format!("expected ref name: {e}"))?
        .value;

    if is_create {
        let anchor = if parser.parse_keywords(&[Keyword::AS, Keyword::OF, Keyword::VERSION]) {
            let n = parser
                .parse_literal_uint()
                .map_err(|e| format!("expected snapshot id integer: {e}"))?;
            SnapshotAnchor::SnapshotId(n as i64)
        } else {
            SnapshotAnchor::CurrentMain
        };

        // Capture remaining tokens (retention) verbatim until end-of-statement.
        let mut ignored_options = Vec::new();
        while !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
            ignored_options.push(parser.next_token().to_string());
        }

        let action = match kind {
            "BRANCH" => AlterIcebergRefAction::CreateBranch {
                name,
                anchor,
                if_not_exists,
                replace,
                ignored_options,
            },
            _ => AlterIcebergRefAction::CreateTag {
                name,
                anchor,
                if_not_exists,
                replace,
                ignored_options,
            },
        };
        Ok(Statement::AlterIcebergRef(AlterIcebergRefStmt { table, action }))
    } else {
        let action = match kind {
            "BRANCH" => AlterIcebergRefAction::DropBranch { name, if_exists },
            _ => AlterIcebergRefAction::DropTag { name, if_exists },
        };
        Ok(Statement::AlterIcebergRef(AlterIcebergRefStmt { table, action }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::dialect::StarRocksDialect;

    fn parse(sql: &str) -> Result<Statement, String> {
        let dialect = StarRocksDialect;
        let mut p = Parser::new(&dialect)
            .try_with_sql(sql)
            .map_err(|e| e.to_string())?;
        parse_alter_iceberg_ref(&mut p)
    }

    #[test]
    fn create_branch_anchor() {
        let stmt = parse("ALTER TABLE c.s.t CREATE BRANCH dev AS OF VERSION 12345").unwrap();
        match stmt {
            Statement::AlterIcebergRef(s) => match s.action {
                AlterIcebergRefAction::CreateBranch { name, anchor, .. } => {
                    assert_eq!(name, "dev");
                    assert_eq!(anchor, SnapshotAnchor::SnapshotId(12345));
                }
                other => panic!("wrong action: {other:?}"),
            },
            _ => panic!("wrong stmt"),
        }
    }

    #[test]
    fn create_tag_no_anchor_uses_current_main() {
        let stmt = parse("ALTER TABLE t CREATE TAG v1").unwrap();
        match stmt {
            Statement::AlterIcebergRef(s) => match s.action {
                AlterIcebergRefAction::CreateTag { anchor, .. } => {
                    assert_eq!(anchor, SnapshotAnchor::CurrentMain);
                }
                _ => panic!("wrong"),
            },
            _ => panic!("wrong"),
        }
    }

    #[test]
    fn create_or_replace_branch() {
        let stmt =
            parse("ALTER TABLE t CREATE OR REPLACE BRANCH dev AS OF VERSION 1").unwrap();
        match stmt {
            Statement::AlterIcebergRef(s) => match s.action {
                AlterIcebergRefAction::CreateBranch { replace, .. } => assert!(replace),
                _ => panic!("wrong"),
            },
            _ => panic!("wrong"),
        }
    }

    #[test]
    fn drop_branch_if_exists() {
        let stmt = parse("ALTER TABLE t DROP BRANCH IF EXISTS dev").unwrap();
        match stmt {
            Statement::AlterIcebergRef(s) => match s.action {
                AlterIcebergRefAction::DropBranch { if_exists, name } => {
                    assert!(if_exists);
                    assert_eq!(name, "dev");
                }
                _ => panic!("wrong"),
            },
            _ => panic!("wrong"),
        }
    }

    #[test]
    fn retention_options_captured() {
        let stmt = parse(
            "ALTER TABLE t CREATE BRANCH dev AS OF VERSION 1 WITH SNAPSHOT RETENTION 5 SNAPSHOTS",
        )
        .unwrap();
        match stmt {
            Statement::AlterIcebergRef(s) => match s.action {
                AlterIcebergRefAction::CreateBranch { ignored_options, .. } => {
                    assert!(!ignored_options.is_empty());
                }
                _ => panic!("wrong"),
            },
            _ => panic!("wrong"),
        }
    }

    #[test]
    fn probe_recognizes_create_branch() {
        let dialect = StarRocksDialect;
        let p = Parser::new(&dialect)
            .try_with_sql("ALTER TABLE t CREATE BRANCH dev")
            .unwrap();
        assert!(looks_like_alter_iceberg_ref(&p));
    }

    #[test]
    fn probe_rejects_alter_table_other() {
        let dialect = StarRocksDialect;
        let p = Parser::new(&dialect)
            .try_with_sql("ALTER TABLE t ADD COLUMN c INT")
            .unwrap();
        assert!(!looks_like_alter_iceberg_ref(&p));
    }
}
```

- [ ] **Step 4: Wire into the dialect mod**

Edit `src/sql/parser/dialect/mod.rs`, add: `pub(crate) mod alter_iceberg_ref;`

- [ ] **Step 5: Wire into `parse_sql`**

Edit `src/sql/parser/mod.rs` `parse_sql`. Place the new probe **after** MV probes but **before** any other generic dispatch (the MV probes recognise `MATERIALIZED VIEW`, ours `BRANCH/TAG`, no overlap, but place after so MV stays first):

```rust
if dialect::alter_iceberg_ref::looks_like_alter_iceberg_ref(&parser) {
    let stmt = dialect::alter_iceberg_ref::parse_alter_iceberg_ref(&mut parser)?;
    return Ok(vec![stmt]);
}
```

- [ ] **Step 6: Run the parser tests — expect PASS**

```bash
cargo test --lib sql::parser::dialect::alter_iceberg_ref
```

- [ ] **Step 7: Make sure existing parser tests still pass**

```bash
cargo test --lib sql::parser
```

- [ ] **Step 8: Commit**

```bash
git add src/sql/parser/ast/iceberg_ref.rs src/sql/parser/ast/mod.rs \
        src/sql/parser/dialect/alter_iceberg_ref.rs \
        src/sql/parser/dialect/mod.rs src/sql/parser/mod.rs
git commit -m "feat(iceberg): parse ALTER TABLE … CREATE/DROP BRANCH|TAG"
```

---

## Task 6: Analyzer for `AlterIcebergRefAction` → `RefActionPlan`

**Files:**
- Create: `src/sql/analyzer/alter_iceberg_ref.rs`
- Modify: `src/sql/analyzer/mod.rs`

`RefActionPlan` itself lives in the connector crate (Task 7). For analyzer purposes use a small mirror struct in `src/sql/analyzer/alter_iceberg_ref.rs`, then in lower it converts to the connector type.

- [ ] **Step 1: Add the analyzer module skeleton with failing test**

Create `src/sql/analyzer/alter_iceberg_ref.rs`:

```rust
//! Validate parsed `AlterIcebergRefStmt` against table metadata; produce a
//! `RefActionPlan` that the lower stage forwards to the executor.

use crate::sql::analyzer::iceberg_ref::IcebergRefKind;
use crate::sql::parser::ast::{AlterIcebergRefAction, AlterIcebergRefStmt, SnapshotAnchor};

#[derive(Clone, Debug, PartialEq)]
pub struct RefActionPlan {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub action: RefAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RefAction {
    CreateBranch { name: String, snapshot_id: i64, replace: bool, if_not_exists: bool },
    CreateTag { name: String, snapshot_id: i64, replace: bool, if_not_exists: bool },
    DropBranch { name: String, if_exists: bool },
    DropTag { name: String, if_exists: bool },
}

/// Resolve the table, validate the action against current refs/snapshots,
/// and produce a `RefActionPlan`. Errors here are analyzer-time
/// (deterministic, fail-fast) — see spec §8.1.
pub fn analyze_alter_iceberg_ref(
    stmt: &AlterIcebergRefStmt,
    catalog: &str,
    namespace: &str,
    table: &str,
    table_metadata: &iceberg::spec::TableMetadata,
) -> Result<RefActionPlan, String> {
    if action_name(&stmt.action) == "main" {
        return Err("iceberg ref: 'main' is reserved".to_string());
    }

    let action = match &stmt.action {
        AlterIcebergRefAction::CreateBranch {
            name,
            anchor,
            if_not_exists,
            replace,
            ignored_options,
        } => {
            warn_ignored_options(ignored_options);
            let snapshot_id = resolve_anchor(anchor, table_metadata, name, IcebergRefKind::Branch)?;
            check_kind(table_metadata, name, IcebergRefKind::Branch)?;
            RefAction::CreateBranch {
                name: name.clone(),
                snapshot_id,
                replace: *replace,
                if_not_exists: *if_not_exists,
            }
        }
        AlterIcebergRefAction::CreateTag {
            name,
            anchor,
            if_not_exists,
            replace,
            ignored_options,
        } => {
            warn_ignored_options(ignored_options);
            let snapshot_id = resolve_anchor(anchor, table_metadata, name, IcebergRefKind::Tag)?;
            check_kind(table_metadata, name, IcebergRefKind::Tag)?;
            RefAction::CreateTag {
                name: name.clone(),
                snapshot_id,
                replace: *replace,
                if_not_exists: *if_not_exists,
            }
        }
        AlterIcebergRefAction::DropBranch { name, if_exists } => {
            check_kind(table_metadata, name, IcebergRefKind::Branch)?;
            RefAction::DropBranch { name: name.clone(), if_exists: *if_exists }
        }
        AlterIcebergRefAction::DropTag { name, if_exists } => {
            check_kind(table_metadata, name, IcebergRefKind::Tag)?;
            RefAction::DropTag { name: name.clone(), if_exists: *if_exists }
        }
    };

    Ok(RefActionPlan {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
        action,
    })
}

fn action_name(a: &AlterIcebergRefAction) -> &str {
    match a {
        AlterIcebergRefAction::CreateBranch { name, .. }
        | AlterIcebergRefAction::CreateTag { name, .. }
        | AlterIcebergRefAction::DropBranch { name, .. }
        | AlterIcebergRefAction::DropTag { name, .. } => name,
    }
}

fn warn_ignored_options(opts: &[String]) {
    if !opts.is_empty() {
        tracing::warn!(
            "iceberg ref: retention options ignored in phase 1: {}",
            opts.join(" ")
        );
    }
}

fn resolve_anchor(
    anchor: &SnapshotAnchor,
    metadata: &iceberg::spec::TableMetadata,
    ref_name: &str,
    _expected_kind: IcebergRefKind,
) -> Result<i64, String> {
    match anchor {
        SnapshotAnchor::SnapshotId(n) => {
            if metadata.snapshot_by_id(*n).is_none() {
                return Err(format!(
                    "iceberg ref: snapshot {n} not found; cannot anchor '{ref_name}'"
                ));
            }
            Ok(*n)
        }
        SnapshotAnchor::CurrentMain => match metadata.current_snapshot() {
            Some(s) => Ok(s.snapshot_id()),
            None => Err(
                "iceberg ref: cannot create branch on table without a current snapshot"
                    .to_string(),
            ),
        },
    }
}

/// If a ref of the given name exists, ensure its kind matches the expected
/// kind (branch vs tag). Mismatches are rejected (spec §6.3 / §8.1).
fn check_kind(
    metadata: &iceberg::spec::TableMetadata,
    name: &str,
    expected: IcebergRefKind,
) -> Result<(), String> {
    if let Some(existing) = metadata.refs().get(name) {
        let existing_kind = match &existing.retention {
            iceberg::spec::SnapshotRetention::Branch { .. } => IcebergRefKind::Branch,
            iceberg::spec::SnapshotRetention::Tag { .. } => IcebergRefKind::Tag,
        };
        if existing_kind != expected {
            let actual = match existing_kind {
                IcebergRefKind::Branch => "branch",
                IcebergRefKind::Tag => "tag",
            };
            let exp = match expected {
                IcebergRefKind::Branch => "tag",
                IcebergRefKind::Tag => "branch",
            };
            return Err(format!(
                "iceberg ref: '{name}' is a {actual}, not a {exp}"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // Use the same in-memory metadata fixture as the connector tests.
    // Helper signature: `fn metadata_with_two_snapshots() -> (TableMetadata, i64, i64)`

    #[test]
    fn create_branch_with_valid_anchor() {
        let (md, _s1, s2) = test_utils::metadata_with_two_snapshots();
        let stmt = AlterIcebergRefStmt {
            table: "c.s.t".parse().unwrap(),
            action: AlterIcebergRefAction::CreateBranch {
                name: "dev".into(),
                anchor: SnapshotAnchor::SnapshotId(s2),
                if_not_exists: false,
                replace: false,
                ignored_options: vec![],
            },
        };
        let plan =
            analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).expect("ok");
        match plan.action {
            RefAction::CreateBranch { snapshot_id, .. } => assert_eq!(snapshot_id, s2),
            _ => panic!("wrong"),
        }
    }

    #[test]
    fn create_branch_main_rejected() {
        let (md, _, s2) = test_utils::metadata_with_two_snapshots();
        let stmt = AlterIcebergRefStmt {
            table: "c.s.t".parse().unwrap(),
            action: AlterIcebergRefAction::CreateBranch {
                name: "main".into(),
                anchor: SnapshotAnchor::SnapshotId(s2),
                if_not_exists: false,
                replace: false,
                ignored_options: vec![],
            },
        };
        let err = analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).unwrap_err();
        assert!(err.contains("'main' is reserved"));
    }

    #[test]
    fn create_branch_unknown_anchor_rejected() {
        let (md, _, _) = test_utils::metadata_with_two_snapshots();
        let stmt = AlterIcebergRefStmt {
            table: "c.s.t".parse().unwrap(),
            action: AlterIcebergRefAction::CreateBranch {
                name: "dev".into(),
                anchor: SnapshotAnchor::SnapshotId(99_999),
                if_not_exists: false,
                replace: false,
                ignored_options: vec![],
            },
        };
        let err = analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).unwrap_err();
        assert!(err.contains("snapshot 99999 not found"));
    }

    #[test]
    fn create_tag_kind_mismatch_when_branch_exists() {
        let (md, _s1, _s2) = test_utils::metadata_with_branch_dev();
        let stmt = AlterIcebergRefStmt {
            table: "c.s.t".parse().unwrap(),
            action: AlterIcebergRefAction::CreateTag {
                name: "dev".into(),
                anchor: SnapshotAnchor::CurrentMain,
                if_not_exists: false,
                replace: false,
                ignored_options: vec![],
            },
        };
        let err = analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).unwrap_err();
        assert!(err.contains("'dev' is a branch, not a tag"));
    }
}
```

- [ ] **Step 2: Add a `test_utils` helper for analyzer-level tests**

Create `src/sql/analyzer/iceberg_ref.rs` test_utils submodule (or a small standalone `tests/iceberg_ref_fixtures.rs`):

```rust
#[cfg(test)]
pub(crate) mod test_utils {
    use iceberg::spec::TableMetadata;
    pub fn metadata_with_two_snapshots() -> (TableMetadata, i64, i64) {
        // Build via TableMetadataBuilder. Pattern: see existing
        // src/connector/iceberg/commit/fast_append.rs::tests for fixture style.
        unimplemented!("inline the existing fixture pattern")
    }
    pub fn metadata_with_branch_dev() -> (TableMetadata, i64, i64) {
        unimplemented!()
    }
}
```

(Lift from existing test fixtures; the iceberg crate's `TableMetadataBuilder` is the constructor.)

- [ ] **Step 3: Wire module**

Edit `src/sql/analyzer/mod.rs`: `pub mod alter_iceberg_ref;`

- [ ] **Step 4: Run tests — expect PASS**

```bash
cargo test --lib sql::analyzer::alter_iceberg_ref
```

- [ ] **Step 5: Commit**

```bash
git add src/sql/analyzer/alter_iceberg_ref.rs src/sql/analyzer/mod.rs \
        src/sql/analyzer/iceberg_ref.rs
git commit -m "feat(iceberg): analyzer for ALTER TABLE … BRANCH/TAG → RefActionPlan"
```

---

## Task 7: `commit/ref_action.rs` — metadata-only commit for branch/tag DDL

**Files:**
- Create: `src/connector/iceberg/commit/ref_action.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Write failing tests for the four actions**

Create `src/connector/iceberg/commit/ref_action.rs`:

```rust
//! Phase-1 metadata-only commit for `CREATE/DROP BRANCH|TAG`.
//!
//! Unlike the six data-commit modules, ref actions never produce a new
//! snapshot — they emit only `SetSnapshotRef` / `RemoveSnapshotRef`
//! `TableUpdate`s plus an `AssertRefSnapshotId` requirement.

use iceberg::spec::{SnapshotReference, SnapshotRetention};
use iceberg::{Catalog, TableIdent, TableRequirement, TableUpdate};

#[derive(Clone, Debug, PartialEq)]
pub struct RefActionPlan {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub action: RefAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RefAction {
    CreateBranch {
        name: String,
        snapshot_id: i64,
        replace: bool,
        if_not_exists: bool,
    },
    CreateTag {
        name: String,
        snapshot_id: i64,
        replace: bool,
        if_not_exists: bool,
    },
    DropBranch {
        name: String,
        if_exists: bool,
    },
    DropTag {
        name: String,
        if_exists: bool,
    },
}

#[derive(Debug)]
pub enum RefActionOutcome {
    Committed,
    NoOp, // when IF [NOT] EXISTS is satisfied without a state change
}

pub async fn execute_ref_action(
    catalog: &dyn Catalog,
    plan: &RefActionPlan,
) -> Result<RefActionOutcome, String> {
    let ident = TableIdent::from_strs([plan.namespace.as_str(), plan.table.as_str()])
        .map_err(|e| e.to_string())?;
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| format!("load table for ref action: {e}"))?;
    let metadata = table.metadata();

    let (updates, requirements) = match &plan.action {
        RefAction::CreateBranch {
            name,
            snapshot_id,
            replace,
            if_not_exists,
        } => match metadata.refs().get(name) {
            Some(_existing) if *if_not_exists => return Ok(RefActionOutcome::NoOp),
            Some(_existing) if !*replace => {
                return Err(format!("iceberg ref: branch '{name}' already exists"));
            }
            existing => {
                let parent = existing.map(|r| r.snapshot_id);
                (
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: name.clone(),
                        reference: SnapshotReference {
                            snapshot_id: *snapshot_id,
                            retention: SnapshotRetention::Branch {
                                min_snapshots_to_keep: None,
                                max_snapshot_age_ms: None,
                                max_ref_age_ms: None,
                            },
                        },
                    }],
                    vec![TableRequirement::RefSnapshotIdMatch {
                        r#ref: name.clone(),
                        snapshot_id: parent,
                    }],
                )
            }
        },
        RefAction::CreateTag {
            name,
            snapshot_id,
            replace,
            if_not_exists,
        } => match metadata.refs().get(name) {
            Some(_existing) if *if_not_exists => return Ok(RefActionOutcome::NoOp),
            Some(_existing) if !*replace => {
                return Err(format!("iceberg ref: tag '{name}' already exists"));
            }
            existing => {
                let parent = existing.map(|r| r.snapshot_id);
                (
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: name.clone(),
                        reference: SnapshotReference {
                            snapshot_id: *snapshot_id,
                            retention: SnapshotRetention::Tag {
                                max_ref_age_ms: None,
                            },
                        },
                    }],
                    vec![TableRequirement::RefSnapshotIdMatch {
                        r#ref: name.clone(),
                        snapshot_id: parent,
                    }],
                )
            }
        },
        RefAction::DropBranch { name, if_exists } => match metadata.refs().get(name) {
            None if *if_exists => return Ok(RefActionOutcome::NoOp),
            None => return Err(format!("iceberg ref: branch '{name}' does not exist")),
            Some(existing) => (
                vec![TableUpdate::RemoveSnapshotRef { ref_name: name.clone() }],
                vec![TableRequirement::RefSnapshotIdMatch {
                    r#ref: name.clone(),
                    snapshot_id: Some(existing.snapshot_id),
                }],
            ),
        },
        RefAction::DropTag { name, if_exists } => match metadata.refs().get(name) {
            None if *if_exists => return Ok(RefActionOutcome::NoOp),
            None => return Err(format!("iceberg ref: tag '{name}' does not exist")),
            Some(existing) => (
                vec![TableUpdate::RemoveSnapshotRef { ref_name: name.clone() }],
                vec![TableRequirement::RefSnapshotIdMatch {
                    r#ref: name.clone(),
                    snapshot_id: Some(existing.snapshot_id),
                }],
            ),
        },
    };

    catalog
        .update_table(
            iceberg::TableCommit::builder()
                .ident(ident)
                .updates(updates)
                .requirements(requirements)
                .build(),
        )
        .await
        .map_err(|e| {
            // Surface ref-drop / head-moved-during-commit as friendly text.
            format!("iceberg ref: commit failed: {e}")
        })?;

    Ok(RefActionOutcome::Committed)
}

#[cfg(test)]
mod tests {
    use super::*;
    // Reuse iceberg in-memory catalog harness used by other commit modules.
    // Helper: `commit::test_utils::in_memory_catalog_with_two_snapshots()`
    // returns `(MemoryCatalog, RefActionPlan template, s1, s2)`.

    #[tokio::test]
    async fn create_branch_at_snapshot() { /* … */ }

    #[tokio::test]
    async fn create_branch_already_exists_errors() { /* … */ }

    #[tokio::test]
    async fn create_branch_if_not_exists_noop() { /* … */ }

    #[tokio::test]
    async fn create_or_replace_branch_overwrites_head() { /* … */ }

    #[tokio::test]
    async fn drop_branch_missing_with_if_exists_noop() { /* … */ }

    #[tokio::test]
    async fn drop_branch_missing_without_if_exists_errors() { /* … */ }

    #[tokio::test]
    async fn create_tag_kind_uses_tag_retention() { /* … */ }
}
```

(Each test follows the in-memory catalog pattern from other commit-module tests in this directory; spell out the bodies referencing the existing fixture style.)

- [ ] **Step 2: Re-export from `commit/mod.rs`**

Edit `src/connector/iceberg/commit/mod.rs`:
- Add `mod ref_action;`
- Add `pub use ref_action::{execute_ref_action, RefAction, RefActionOutcome, RefActionPlan};`

- [ ] **Step 3: Build & test — expect PASS**

```bash
cargo test --lib connector::iceberg::commit::ref_action
```

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/commit/ref_action.rs \
        src/connector/iceberg/commit/mod.rs
git commit -m "feat(iceberg): commit module for branch/tag DDL"
```

---

## Task 8: ExecNode + lower for `IcebergRefAction`

**Files:**
- Create: `src/exec/node/iceberg_ref_action.rs`
- Modify: `src/exec/node/mod.rs`
- Create: `src/lower/ddl/iceberg_ref.rs`
- Modify: `src/lower/node/mod.rs` (or wherever statement-level dispatch lives)

- [ ] **Step 1: Add ExecNode**

Create `src/exec/node/iceberg_ref_action.rs`:

```rust
use crate::connector::iceberg::commit::RefActionPlan;

#[derive(Clone, Debug)]
pub struct IcebergRefActionNode {
    pub plan: RefActionPlan,
}
```

Edit `src/exec/node/mod.rs`:
- `pub mod iceberg_ref_action;`
- `use iceberg_ref_action::IcebergRefActionNode;`
- Add `IcebergRefAction(IcebergRefActionNode),` to `ExecNodeKind`.

- [ ] **Step 2: Add lower entry**

Create `src/lower/ddl/iceberg_ref.rs`:

```rust
use crate::sql::analyzer::alter_iceberg_ref::{RefAction as AnalyzerRefAction, RefActionPlan as AnalyzerRefActionPlan};
use crate::connector::iceberg::commit::{RefAction, RefActionPlan};
use crate::exec::node::iceberg_ref_action::IcebergRefActionNode;

pub fn lower_ref_action(plan: AnalyzerRefActionPlan) -> IcebergRefActionNode {
    let action = match plan.action {
        AnalyzerRefAction::CreateBranch { name, snapshot_id, replace, if_not_exists } =>
            RefAction::CreateBranch { name, snapshot_id, replace, if_not_exists },
        AnalyzerRefAction::CreateTag { name, snapshot_id, replace, if_not_exists } =>
            RefAction::CreateTag { name, snapshot_id, replace, if_not_exists },
        AnalyzerRefAction::DropBranch { name, if_exists } =>
            RefAction::DropBranch { name, if_exists },
        AnalyzerRefAction::DropTag { name, if_exists } =>
            RefAction::DropTag { name, if_exists },
    };
    IcebergRefActionNode {
        plan: RefActionPlan {
            catalog: plan.catalog,
            namespace: plan.namespace,
            table: plan.table,
            action,
        },
    }
}
```

Wire `pub mod ddl { pub mod iceberg_ref; }` (or follow the existing `src/lower/` layout — confirm by reading `src/lower/mod.rs`).

- [ ] **Step 3: Pipeline executor dispatch**

Find the executor entry that handles single-driver DDL nodes (`grep -rn "ExecNodeKind::" src/exec/pipeline/`). Add a match arm:

```rust
ExecNodeKind::IcebergRefAction(node) => {
    let outcome = crate::connector::iceberg::commit::ref_action::block_on_iceberg_action(
        node.plan.clone(),
    )?;
    // Emit one status row "OK".
    return Ok(empty_status_chunk("OK"));
}
```

(If a `block_on_iceberg_action` helper doesn't exist, add a small wrapper in `ref_action.rs` that resolves the right `Catalog` from the registry by `plan.catalog` and then calls `execute_ref_action`. Pattern: existing commit dispatch in `commit/run.rs`.)

- [ ] **Step 4: Build**

```bash
cargo build --lib
```

- [ ] **Step 5: Commit**

```bash
git add src/exec/node/iceberg_ref_action.rs src/exec/node/mod.rs \
        src/lower/ddl/iceberg_ref.rs src/lower/mod.rs src/lower/node/mod.rs \
        src/exec/pipeline
git commit -m "feat(iceberg): IcebergRefAction ExecNode + lower wiring"
```

---

## Task 9: Statement dispatch — analyzer entry point for `Statement::AlterIcebergRef`

**Files:**
- Modify: `src/sql/analyzer/mod.rs` (statement dispatch site)
- Modify: `src/engine/statement.rs` (the entry that maps Statement → ExecPlan)

- [ ] **Step 1: Locate statement dispatch**

```bash
grep -n "Statement::CreateMaterializedView\|Statement::DropMaterializedView" src/sql/analyzer src/engine -r
```

Pattern: existing MV statements are dispatched to a per-statement analyzer. Add the new arm:

```rust
Statement::AlterIcebergRef(stmt) => {
    let table = resolve_table(&stmt.table)?;
    let metadata = load_metadata(&table)?;
    let plan = sql::analyzer::alter_iceberg_ref::analyze_alter_iceberg_ref(
        &stmt, &table.catalog, &table.namespace, &table.table, &metadata,
    )?;
    let exec_node = lower::ddl::iceberg_ref::lower_ref_action(plan);
    Ok(ExecPlan::single(ExecNodeKind::IcebergRefAction(exec_node)))
}
```

- [ ] **Step 2: Build**

```bash
cargo build --lib
```

- [ ] **Step 3: Add an integration smoke test**

Append to `src/sql/analyzer/alter_iceberg_ref.rs::tests`:

```rust
#[tokio::test]
async fn end_to_end_create_drop_branch() {
    // Run through parse_sql + analyze + execute_ref_action against the
    // in-memory catalog. Pattern: existing MV smoke test in
    // src/sql/analyzer/<mv_module>::tests.
    // Assert: branch 'dev' appears in metadata.refs() after CREATE,
    //          disappears after DROP.
}
```

- [ ] **Step 4: Run**

```bash
cargo test --lib sql::analyzer::alter_iceberg_ref::tests::end_to_end
```

- [ ] **Step 5: Commit**

```bash
git add src/sql/analyzer/mod.rs src/engine/statement.rs src/sql/analyzer/alter_iceberg_ref.rs
git commit -m "feat(iceberg): analyzer→executor wire-up for ALTER TABLE BRANCH/TAG"
```

---

## Task 10: SQL test — branch/tag DDL happy path

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_branch_tag_ddl.sql`

- [ ] **Step 1: Author the SQL**

Create `sql-tests/iceberg/sql/iceberg_branch_tag_ddl.sql`:

```sql
-- Setup: create a small iceberg table with two snapshots.
DROP TABLE IF EXISTS iceberg_demo.test_db.t_ref_ddl;
CREATE TABLE iceberg_demo.test_db.t_ref_ddl (id INT, v INT);
INSERT INTO iceberg_demo.test_db.t_ref_ddl VALUES (1, 10), (2, 20);
INSERT INTO iceberg_demo.test_db.t_ref_ddl VALUES (3, 30);

-- Capture the current snapshot id for use in CREATE BRANCH … AS OF VERSION
-- (the runner exposes the most recent commit's snapshot through information_schema)
-- Phase 1: keep the SQL self-contained by using `CREATE BRANCH dev` with no anchor;
-- it defaults to current main head.

ALTER TABLE iceberg_demo.test_db.t_ref_ddl CREATE BRANCH dev;
ALTER TABLE iceberg_demo.test_db.t_ref_ddl CREATE TAG release_v1;

-- Idempotency: IF NOT EXISTS / OR REPLACE
ALTER TABLE iceberg_demo.test_db.t_ref_ddl CREATE BRANCH IF NOT EXISTS dev;
ALTER TABLE iceberg_demo.test_db.t_ref_ddl CREATE OR REPLACE BRANCH dev;

-- DROP
ALTER TABLE iceberg_demo.test_db.t_ref_ddl DROP TAG release_v1;
ALTER TABLE iceberg_demo.test_db.t_ref_ddl DROP BRANCH IF EXISTS dev;

DROP TABLE iceberg_demo.test_db.t_ref_ddl;
```

- [ ] **Step 2: Record snapshot**

```bash
cargo run --release -- standalone-server --port 9030 &
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_branch_tag_ddl --mode record
# foreground: stop the server with Ctrl+C
```

- [ ] **Step 3: Verify replay**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_branch_tag_ddl --mode verify
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_branch_tag_ddl.sql sql-tests/iceberg/result/iceberg_branch_tag_ddl
git commit -m "test(iceberg): branch/tag DDL happy path"
```

---

## Task 11: Read-side time travel — analyzer wire-up for `TableFactor::Table.version`

**Files:**
- Modify: `src/sql/analyzer/resolve_from.rs`
- Modify: `src/sql/analyzer/iceberg_ref.rs` (extend with `resolve_read_binding`)
- Modify: `src/lower/node/hdfs_scan.rs` (or whichever module produces `TIcebergTable`-bearing scan nodes; thread the binding through)

- [ ] **Step 1: Add `resolve_read_binding` to the analyzer module**

Edit `src/sql/analyzer/iceberg_ref.rs`, add:

```rust
use sqlparser::ast::TableVersion as SpV;
use sqlparser::ast::Expr as SpExpr;

/// Map a `TableFactor::Table.version` on an Iceberg table to a binding.
/// `None` returns `None` (caller defaults to main current snapshot at the
/// connector layer).
pub fn resolve_read_binding(
    table_metadata: &iceberg::spec::TableMetadata,
    fully_qualified_name: &str,
    version: Option<&SpV>,
) -> Result<Option<IcebergRefBinding>, String> {
    let Some(v) = version else { return Ok(None) };
    let (snapshot_id, ref_name, ref_kind) = match v {
        SpV::VersionAsOf(SpExpr::Value(value)) => {
            // Number(n) → snapshot id; SingleQuoted(s) → ref name
            match value {
                sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. } => {
                    let id: i64 = n.parse().map_err(|_| format!(
                        "iceberg time travel: expected integer snapshot id, got '{n}'"
                    ))?;
                    if table_metadata.snapshot_by_id(id).is_none() {
                        return Err(format!(
                            "iceberg time travel: snapshot {id} not found in {fully_qualified_name}"
                        ));
                    }
                    (id, None, None)
                }
                sqlparser::ast::ValueWithSpan {
                    value: sqlparser::ast::Value::SingleQuotedString(s),
                    ..
                } => {
                    let r = table_metadata.refs().get(s).ok_or_else(|| {
                        let existing: Vec<&String> =
                            table_metadata.refs().keys().take(8).collect();
                        format!(
                            "iceberg time travel: ref '{s}' not found in \
                             {fully_qualified_name}; existing refs: {:?}",
                            existing
                        )
                    })?;
                    let kind = match &r.retention {
                        iceberg::spec::SnapshotRetention::Branch { .. } => IcebergRefKind::Branch,
                        iceberg::spec::SnapshotRetention::Tag { .. } => IcebergRefKind::Tag,
                    };
                    (r.snapshot_id, Some(s.clone()), Some(kind))
                }
                other => {
                    return Err(format!(
                        "iceberg time travel: VERSION AS OF accepts integer or string literal; got {other:?}"
                    ))
                }
            }
        }
        SpV::TimestampAsOf(expr) | SpV::ForSystemTimeAsOf(expr) => {
            let ts_ms = literal_to_epoch_ms(expr).map_err(|e| {
                format!("iceberg time travel: phase 1 only accepts literal timestamp; {e}")
            })?;
            let id = pick_snapshot_at_or_before(table_metadata, ts_ms).ok_or_else(|| {
                format!(
                    "iceberg time travel: timestamp {ts_ms} predates first snapshot of \
                     {fully_qualified_name}"
                )
            })?;
            (id, None, None)
        }
        SpV::Function(_) => {
            return Err(
                "iceberg time travel: function-style version not supported in phase 1".into()
            )
        }
    };

    Ok(Some(IcebergRefBinding {
        snapshot_id,
        ref_name,
        ref_kind,
    }))
}

fn literal_to_epoch_ms(expr: &SpExpr) -> Result<i64, String> {
    use sqlparser::ast::Value;
    match expr {
        SpExpr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => chrono::DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.timestamp_millis())
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .map(|dt| dt.and_utc().timestamp_millis())
                })
                .map_err(|e| format!("invalid ISO-8601 timestamp '{s}': {e}")),
            Value::Number(n, _) => n.parse::<i64>().map_err(|e| e.to_string()),
            other => Err(format!("expected literal, got {other:?}")),
        },
        other => Err(format!("expected literal, got expression: {other:?}")),
    }
}

fn pick_snapshot_at_or_before(metadata: &iceberg::spec::TableMetadata, ts_ms: i64) -> Option<i64> {
    metadata
        .snapshot_log()
        .iter()
        .filter(|s| s.timestamp_ms <= ts_ms)
        .max_by_key(|s| s.timestamp_ms)
        .map(|s| s.snapshot_id)
}
```

(Spelled-out implementation; verify the iceberg-rs `snapshot_log()` accessor name against the local vendored copy and adjust if needed.)

- [ ] **Step 2: Failing test**

Append in `iceberg_ref.rs::tests`:

```rust
#[test]
fn version_as_of_int() {
    let (md, _s1, s2) = test_utils::metadata_with_two_snapshots();
    let v = SpV::VersionAsOf(SpExpr::Value(sqlparser::ast::ValueWithSpan {
        value: sqlparser::ast::Value::Number(s2.to_string(), false),
        span: sqlparser::tokenizer::Span::empty(),
    }));
    let binding = resolve_read_binding(&md, "c.s.t", Some(&v)).unwrap().unwrap();
    assert_eq!(binding.snapshot_id, s2);
    assert_eq!(binding.ref_name, None);
}

#[test]
fn version_as_of_string_resolves_branch() {
    let (md, _s1, s_dev) = test_utils::metadata_with_branch_dev();
    let v = SpV::VersionAsOf(SpExpr::Value(sqlparser::ast::ValueWithSpan {
        value: sqlparser::ast::Value::SingleQuotedString("dev".into()),
        span: sqlparser::tokenizer::Span::empty(),
    }));
    let binding = resolve_read_binding(&md, "c.s.t", Some(&v)).unwrap().unwrap();
    assert_eq!(binding.snapshot_id, s_dev);
    assert_eq!(binding.ref_name.as_deref(), Some("dev"));
    assert_eq!(binding.ref_kind, Some(IcebergRefKind::Branch));
}

#[test]
fn unknown_ref_errors() {
    let (md, _, _) = test_utils::metadata_with_two_snapshots();
    let v = SpV::VersionAsOf(SpExpr::Value(sqlparser::ast::ValueWithSpan {
        value: sqlparser::ast::Value::SingleQuotedString("nope".into()),
        span: sqlparser::tokenizer::Span::empty(),
    }));
    let err = resolve_read_binding(&md, "c.s.t", Some(&v)).unwrap_err();
    assert!(err.contains("ref 'nope' not found"));
}
```

- [ ] **Step 3: Run — expect PASS**

```bash
cargo test --lib sql::analyzer::iceberg_ref
```

- [ ] **Step 4: Plumb the binding into `resolve_from.rs`**

Find the iceberg branch in `resolve_from.rs` (search for the iceberg backend dispatch). After resolving the table to its connector, call:

```rust
let binding = if backend.is_iceberg() {
    sql::analyzer::iceberg_ref::resolve_read_binding(
        &iceberg_metadata,
        &fully_qualified,
        version_clause,
    )?
} else if version_clause.is_some() {
    return Err("time travel is supported only on iceberg tables".into());
} else {
    None
};
```

Pass `binding` down to whatever scan-node assembly currently exists; for iceberg-backed scans, call `IcebergTableSource::build_table_def_at(&resolved, binding.clone())` (Task 4).

- [ ] **Step 5: Build & test**

```bash
cargo build --lib
cargo test --lib sql::analyzer
```

- [ ] **Step 6: Commit**

```bash
git add src/sql/analyzer/iceberg_ref.rs src/sql/analyzer/resolve_from.rs
git commit -m "feat(iceberg): resolve time-travel clause to IcebergRefBinding at planning time"
```

---

## Task 12: Read-side EXPLAIN output

**Files:**
- Modify: `src/sql/explain.rs`
- Modify: scan-node EXPLAIN formatter (`grep -n IcebergScan src/sql/explain.rs src/exec/operators src/lower 2>/dev/null`)

- [ ] **Step 1: Find the iceberg scan EXPLAIN block**

```bash
grep -rn "IcebergScan\|iceberg_scan" src/sql/explain.rs src/exec/operators 2>/dev/null
```

- [ ] **Step 2: Add the `ref:` and `snapshot_id:` lines**

Where the formatter prints `IcebergScan` headers, add lines that read the `read_binding` field. If the binding is `None`, print `ref: branch 'main'` (the default).

```rust
let (ref_line, sid_line) = match &node.read_binding {
    Some(b) => (b.ref_repr(), format!("snapshot_id: {}", b.snapshot_id)),
    None => ("branch 'main'".to_string(),
            format!("snapshot_id: {}", current_snapshot_id_or_zero(table))),
};
writeln!(out, "  ref: {ref_line}")?;
writeln!(out, "  {sid_line}")?;
```

- [ ] **Step 3: Snapshot test**

If snapshot tests for EXPLAIN output exist (`grep -rn "insta::assert_snapshot\|expect!" src/sql/explain.rs`), regenerate with the new lines; otherwise add a fresh test.

- [ ] **Step 4: Commit**

```bash
git add src/sql/explain.rs
git commit -m "feat(iceberg): EXPLAIN shows ref and snapshot_id on IcebergScan"
```

---

## Task 13: SQL test — time-travel SELECT

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_time_travel_select.sql`

- [ ] **Step 1: Author**

```sql
DROP TABLE IF EXISTS iceberg_demo.test_db.t_tt;
CREATE TABLE iceberg_demo.test_db.t_tt (id INT, v INT);

INSERT INTO iceberg_demo.test_db.t_tt VALUES (1, 10);   -- snapshot S1
INSERT INTO iceberg_demo.test_db.t_tt VALUES (2, 20);   -- snapshot S2

-- Default: latest main
SELECT id, v FROM iceberg_demo.test_db.t_tt ORDER BY id;

-- VERSION AS OF '<branch>'
ALTER TABLE iceberg_demo.test_db.t_tt CREATE BRANCH backup;
INSERT INTO iceberg_demo.test_db.t_tt VALUES (3, 30);
SELECT id, v FROM iceberg_demo.test_db.t_tt
  FOR VERSION AS OF 'backup'
  ORDER BY id;
SELECT id, v FROM iceberg_demo.test_db.t_tt
  FOR VERSION AS OF 'main'
  ORDER BY id;

-- Cross-ref join
SELECT m.id, m.v AS main_v, b.v AS bak_v
  FROM iceberg_demo.test_db.t_tt FOR VERSION AS OF 'main' m
  JOIN iceberg_demo.test_db.t_tt FOR VERSION AS OF 'backup' b
    ON m.id = b.id
  ORDER BY m.id;

ALTER TABLE iceberg_demo.test_db.t_tt DROP BRANCH backup;
DROP TABLE iceberg_demo.test_db.t_tt;
```

- [ ] **Step 2: Record + verify**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_time_travel_select --mode record
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_time_travel_select --mode verify
```

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_time_travel_select.sql sql-tests/iceberg/result/iceberg_time_travel_select
git commit -m "test(iceberg): SELECT FOR VERSION AS OF cross-ref"
```

---

## Task 14: SQL test — TIMESTAMP AS OF

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_time_travel_timestamp.sql`

- [ ] **Step 1: Author**

```sql
DROP TABLE IF EXISTS iceberg_demo.test_db.t_ts;
CREATE TABLE iceberg_demo.test_db.t_ts (id INT, v INT);
INSERT INTO iceberg_demo.test_db.t_ts VALUES (1, 10);

-- Capture the just-committed snapshot's timestamp via the runner's
-- `-- @capture_now ts_after_first_insert` directive (the existing
-- runner supports a similar facility for snapshot id capture; if not,
-- this test uses a sentinel pre-baked timestamp via the runner's clock
-- override hook). If neither hook exists, replace this test with one
-- that uses a fixed past timestamp like '1970-01-01' to guarantee the
-- "predates first snapshot" error path; positive timestamp coverage
-- belongs to a follow-up runner extension.

INSERT INTO iceberg_demo.test_db.t_ts VALUES (2, 20);

-- Negative path: timestamp before first snapshot must error.
-- The runner's `-- error:` directive captures the message.
-- error: iceberg time travel: timestamp
SELECT * FROM iceberg_demo.test_db.t_ts
  FOR TIMESTAMP AS OF '1970-01-01 00:00:00';

DROP TABLE iceberg_demo.test_db.t_ts;
```

- [ ] **Step 2: Record + verify**

(See Task 13 commands.)

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_time_travel_timestamp.sql sql-tests/iceberg/result/iceberg_time_travel_timestamp
git commit -m "test(iceberg): SELECT FOR TIMESTAMP AS OF negative path"
```

---

## Task 15: `CommitCtx.target_ref` — shared seam

**Files:**
- Modify: `src/connector/iceberg/commit/action.rs`

- [ ] **Step 1: Add field**

Edit `src/connector/iceberg/commit/action.rs`:

```rust
pub struct CommitCtx<'a> {
    pub collector: &'a IcebergCommitCollector,
    pub table: &'a Table,
    pub catalog: &'a dyn Catalog,
    pub file_io: &'a FileIO,
    pub commit_uuid: Uuid,
    pub abort_handle: Arc<AbortLog>,
    /// Ref to update on commit. `"main"` is the default.
    pub target_ref: &'a str,
}
```

- [ ] **Step 2: Update every `CommitCtx { ... }` literal**

```bash
grep -rn "CommitCtx {" src/ --include="*.rs"
```

In each construction site (in `commit/run.rs` and any test fixtures), add `target_ref: "main"` (or pass a parameter — see Task 22).

- [ ] **Step 3: Build**

```bash
cargo build --lib
```

Expected: clean (no behavioural change yet — every call still passes "main").

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/commit/action.rs src/connector/iceberg/commit/run.rs
git commit -m "feat(iceberg): CommitCtx.target_ref seam (still hard-coded to main)"
```

---

## Task 16: Make six commit modules `target_ref`-aware (one task per module — but here one task with six steps for proximity)

**Files (per module):**
- `src/connector/iceberg/commit/fast_append.rs` — lines 181, 269, 288 (existing snapshot uses)
- `src/connector/iceberg/commit/row_delta.rs` — lines 240, 259
- `src/connector/iceberg/commit/row_delta_dv.rs` — lines 377, 396
- `src/connector/iceberg/commit/overwrite.rs` — lines 280, 299
- `src/connector/iceberg/commit/update_cow.rs` — lines 310, 329
- `src/connector/iceberg/commit/rewrite_data_files.rs` — lines 278, 297

For each module, the change pattern is identical:

- [ ] **Per module, Step A: parent-snapshot lookup**

Find `m.current_snapshot()` (or `m.current_snapshot_id()`) used to compute `parent_snapshot_id`. Replace with:

```rust
let parent_snapshot_id = m
    .refs()
    .get(ctx.target_ref)
    .map(|r| r.snapshot_id);
```

(For ref-aware reads — when `target_ref == "main"` and `main` is not in `refs()` for an unborn table, fall back to `m.current_snapshot()` — but for iceberg-rs the `main` ref is auto-managed, so the lookup should never miss.)

For `TransactionAction::commit` impls that don't have access to `CommitCtx` directly, thread `target_ref: String` into the action struct (`FastAppendV3TxnAction`, etc.) at construction time and store it on the struct.

- [ ] **Per module, Step B: SetSnapshotRef ref_name**

Replace:

```rust
TableUpdate::SetSnapshotRef {
    ref_name: MAIN_BRANCH.to_string(),
    ...
}
```

with:

```rust
TableUpdate::SetSnapshotRef {
    ref_name: target_ref.clone(),
    ...
}
```

(`target_ref` is now stored on the action struct.)

- [ ] **Per module, Step C: AssertRefSnapshotId ref**

Replace:

```rust
TableRequirement::RefSnapshotIdMatch {
    r#ref: MAIN_BRANCH.to_string(),
    snapshot_id: parent_snapshot_id,
}
```

with:

```rust
TableRequirement::RefSnapshotIdMatch {
    r#ref: target_ref.clone(),
    snapshot_id: parent_snapshot_id,
}
```

- [ ] **Per module, Step D: extend the action constructor + `commit` to pass `target_ref` from `CommitCtx`**

Look at how each module constructs its `TransactionAction` from `CommitCtx`. Add:

```rust
let target_ref = ctx.target_ref.to_string();
// pass to action ctor
```

- [ ] **Per module, Step E: add a fixture-based unit test**

In each module's `#[cfg(test)] mod tests`, add:

```rust
#[tokio::test]
async fn commit_to_non_main_branch_advances_dev_only() {
    let (catalog, ident, _s1, _s2) = test_utils::in_memory_with_branch_dev().await;
    // build CommitCtx with target_ref = "dev"
    // call <ModuleCommit>::commit(ctx)
    // load the table, assert refs()["dev"].snapshot_id changed and refs()["main"] did not.
}
```

- [ ] **Per module, Step F: run the test — expect PASS**

```bash
cargo test --lib connector::iceberg::commit::<module_name>
```

- [ ] **Once all six are done, run the full commit suite**

```bash
cargo test --lib connector::iceberg::commit
```

- [ ] **Commit per module (6 commits):**

```bash
git add src/connector/iceberg/commit/<module>.rs
git commit -m "feat(iceberg): <module> commits to ctx.target_ref instead of MAIN_BRANCH"
```

---

## Task 17: `IcebergDmlTarget` plumbing through Sink

**Files:**
- Modify: `src/connector/iceberg/sink.rs`
- Modify: `src/sql/analyzer/iceberg_ref.rs` (add `IcebergDmlTarget` accessor)
- Modify: `src/engine/{statement,mutation_flow,delete_flow,mod}.rs`
- Modify: `src/exec/node/mod.rs` (and the operator-side wiring)

- [ ] **Step 1: Add `target_ref` to the sink operator's owned state**

In `src/connector/iceberg/sink.rs::IcebergTableSinkOperator`, add a `target_ref: String` field. Read it from a new field on `IcebergTableSinkFactory::sink` (and through to the data_sinks path). Phase 1 stays inside Rust — no thrift change yet — by introducing an internal `Arc<String>` that the lower stage stuffs in alongside the existing `data_sinks::TIcebergTableSink`.

(Exact glue depends on the existing factory shape; pattern: see how `IcebergTableSinkFactory` already carries `Arc<...>` extras.)

- [ ] **Step 2: Pass `target_ref` to `CommitCtx`**

In `IcebergTableSinkOperator`, when building `CommitCtx`, set `target_ref: &self.target_ref`.

- [ ] **Step 3: Build**

```bash
cargo build --lib
```

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/sink.rs src/exec/node/mod.rs <other touched>
git commit -m "feat(iceberg): IcebergTableSinkOperator carries target_ref through commit"
```

---

## Task 18: `t.branch_<name>` suffix detection in DML

**Files:**
- Modify: `src/engine/statement.rs`
- Modify: `src/engine/mod.rs` (DML dispatch sites)
- Modify: `src/engine/{mutation_flow,delete_flow}.rs`

- [ ] **Step 1: Helper for splitting suffix**

Add to `src/sql/analyzer/iceberg_ref.rs`:

```rust
pub enum IcebergRefSuffix {
    Branch(String),
    Tag(String),
}

pub fn split_ref_suffix(parts: &[String]) -> (Vec<String>, Option<IcebergRefSuffix>) {
    let last = parts.last().cloned();
    match last.as_deref() {
        Some(s) if s.starts_with("branch_") && s.len() > 7 => {
            let mut head = parts.to_vec();
            head.pop();
            (head, Some(IcebergRefSuffix::Branch(s["branch_".len()..].into())))
        }
        Some(s) if s.starts_with("tag_") && s.len() > 4 => {
            let mut head = parts.to_vec();
            head.pop();
            (head, Some(IcebergRefSuffix::Tag(s["tag_".len()..].into())))
        }
        _ => (parts.to_vec(), None),
    }
}
```

- [ ] **Step 2: Apply at the DML entry**

In each DML site under `src/engine/`, after locating the `sqlparser::ast::Statement::Insert(insert)` (or Update / Delete) match arm, call `split_ref_suffix(&insert.table_name.0.iter().map(...).collect::<Vec<_>>())`. If `Some(IcebergRefSuffix::Tag(_))` → return analyzer error `"iceberg ref: tag '<name>' is read-only; use a branch as DML target"`. If `Some(Branch(name))` → set `target_ref = name`. If `None` → `target_ref = "main"`.

- [ ] **Step 3: Reject combination with FOR VERSION AS OF**

If the same statement carries a `version` clause AND a branch suffix, fail-fast with the spec's exact message.

- [ ] **Step 4: Pass `target_ref` to the sink factory built for this DML**

(The exact handoff depends on Task 17; ensure the factory ends up with the resolved `target_ref` string.)

- [ ] **Step 5: Add a unit test for `split_ref_suffix`**

```rust
#[test]
fn split_branch_suffix() {
    let (head, suffix) = split_ref_suffix(&["c".into(), "s".into(), "t".into(), "branch_dev".into()]);
    assert_eq!(head, vec!["c", "s", "t"]);
    assert!(matches!(suffix, Some(IcebergRefSuffix::Branch(ref s)) if s == "dev"));
}
```

- [ ] **Step 6: Build & test**

```bash
cargo test --lib sql::analyzer::iceberg_ref::tests::split_branch_suffix
```

- [ ] **Step 7: Commit**

```bash
git add src/sql/analyzer/iceberg_ref.rs src/engine/statement.rs src/engine/mod.rs \
        src/engine/mutation_flow.rs src/engine/delete_flow.rs
git commit -m "feat(iceberg): DML t.branch_<x> / t.tag_<x> suffix detection"
```

---

## Task 19: Read-binding plumbing for DML base reads

**Files:**
- Modify: `src/engine/delete_flow.rs:510, 574`
- Modify: `src/engine/mutation_flow.rs:830`

- [ ] **Step 1: Replace `extract_data_files_with_stats(table)` with the `_at` form**

Each call site is preceded by a snapshot decision. Replace:

```rust
let files = extract_data_files_with_stats(table)?;
```

with:

```rust
let target = match metadata.refs().get(&dml_target.write_ref) {
    Some(r) => r.snapshot_id,
    None => return Err(format!(
        "iceberg ref: branch '{}' was dropped between planning and commit",
        dml_target.write_ref
    )),
};
let files = extract_data_files_with_stats_at(table, target)?;
```

(`dml_target` is the value plumbed from Task 18.)

- [ ] **Step 2: Build**

```bash
cargo build --lib
```

- [ ] **Step 3: Commit**

```bash
git add src/engine/delete_flow.rs src/engine/mutation_flow.rs
git commit -m "feat(iceberg): DML reads base files at write_ref's head, not main"
```

---

## Task 20: SQL test — branch DML

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_branch_write.sql`

- [ ] **Step 1: Author**

```sql
DROP TABLE IF EXISTS iceberg_demo.test_db.t_bw;
CREATE TABLE iceberg_demo.test_db.t_bw (id INT, v INT);
INSERT INTO iceberg_demo.test_db.t_bw VALUES (1, 10), (2, 20);

ALTER TABLE iceberg_demo.test_db.t_bw CREATE BRANCH dev;

-- INSERT to branch
INSERT INTO iceberg_demo.test_db.t_bw.branch_dev VALUES (3, 30);

-- main untouched
SELECT id, v FROM iceberg_demo.test_db.t_bw
  FOR VERSION AS OF 'main'
  ORDER BY id;

-- dev advanced
SELECT id, v FROM iceberg_demo.test_db.t_bw
  FOR VERSION AS OF 'dev'
  ORDER BY id;

-- UPDATE on branch
UPDATE iceberg_demo.test_db.t_bw.branch_dev SET v = 99 WHERE id = 1;

SELECT id, v FROM iceberg_demo.test_db.t_bw
  FOR VERSION AS OF 'dev'
  ORDER BY id;

-- main still original
SELECT id, v FROM iceberg_demo.test_db.t_bw
  FOR VERSION AS OF 'main'
  ORDER BY id;

-- DELETE on branch
DELETE FROM iceberg_demo.test_db.t_bw.branch_dev WHERE id = 2;

SELECT id, v FROM iceberg_demo.test_db.t_bw
  FOR VERSION AS OF 'dev'
  ORDER BY id;

ALTER TABLE iceberg_demo.test_db.t_bw DROP BRANCH dev;
DROP TABLE iceberg_demo.test_db.t_bw;
```

- [ ] **Step 2: Record + verify**

(Same commands as Task 13.)

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_branch_write.sql sql-tests/iceberg/result/iceberg_branch_write
git commit -m "test(iceberg): branch-qualified INSERT/UPDATE/DELETE"
```

---

## Task 21: SQL test — analyzer errors

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_ref_negative.sql`

- [ ] **Step 1: Author**

```sql
DROP TABLE IF EXISTS iceberg_demo.test_db.t_neg;
CREATE TABLE iceberg_demo.test_db.t_neg (id INT, v INT);
INSERT INTO iceberg_demo.test_db.t_neg VALUES (1, 10);

-- Reserved name
-- error: iceberg ref: 'main' is reserved
ALTER TABLE iceberg_demo.test_db.t_neg CREATE BRANCH main;

-- Unknown ref
-- error: iceberg time travel: ref 'nope' not found
SELECT * FROM iceberg_demo.test_db.t_neg FOR VERSION AS OF 'nope';

-- Tag is read-only
ALTER TABLE iceberg_demo.test_db.t_neg CREATE TAG release_v1;
-- error: tag 'release_v1' is read-only
INSERT INTO iceberg_demo.test_db.t_neg.tag_release_v1 VALUES (2, 20);

-- Phase-1 timestamp restriction
-- error: phase 1 only accepts literal timestamp
SELECT * FROM iceberg_demo.test_db.t_neg FOR TIMESTAMP AS OF NOW();

-- Suffix + clause conflict
ALTER TABLE iceberg_demo.test_db.t_neg CREATE BRANCH dev;
-- error: branch suffix
INSERT INTO iceberg_demo.test_db.t_neg.branch_dev FOR VERSION AS OF 'main' VALUES (3, 30);

ALTER TABLE iceberg_demo.test_db.t_neg DROP BRANCH dev;
ALTER TABLE iceberg_demo.test_db.t_neg DROP TAG release_v1;
DROP TABLE iceberg_demo.test_db.t_neg;
```

- [ ] **Step 2: Record + verify**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_ref_negative --mode record
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_ref_negative --mode verify
```

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_ref_negative.sql sql-tests/iceberg/result/iceberg_ref_negative
git commit -m "test(iceberg): analyzer-time fail-fast errors"
```

---

## Task 22: Regression — make sure existing suites still pass

**Files:** none (exercising existing suites).

- [ ] **Step 1: Build release**

```bash
cargo build --release
```

- [ ] **Step 2: Start standalone server**

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &
SERVER_PID=$!
sleep 8
```

- [ ] **Step 3: Run iceberg full suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode verify
```

Expected: every existing case + the four new ones PASS.

- [ ] **Step 4: Run mv-on-iceberg**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite mv-on-iceberg --mode verify
```

- [ ] **Step 5: Run ssb / tpc-h / tpc-ds smoke**

```bash
for s in ssb tpc-h tpc-ds; do
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite "$s" --mode verify --query-timeout 60 -j 4
done
```

- [ ] **Step 6: Tear down**

```bash
kill $SERVER_PID 2>/dev/null || true
```

- [ ] **Step 7: Commit (no code change — but run `cargo fmt && cargo clippy -- -D warnings`)**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
git diff --stat
git status
# If any drift, commit:
git commit -am "chore(iceberg): fmt + clippy clean after time-travel work"
```

---

## Self-Review

(Run during plan-write: spec coverage → placeholders → type consistency.)

- Coverage:
  - Spec §3 AST/parser → Tasks 5, 11.
  - Spec §4 analyzer → Tasks 1, 6, 11, 18.
  - Spec §5 read path → Tasks 2, 3, 4, 11, 12, 13, 14.
  - Spec §6 write path → Tasks 15, 16, 17, 19, 20.
  - Spec §7 DDL → Tasks 5, 6, 7, 8, 9, 10.
  - Spec §8 errors → Tasks 6, 11, 18, 21.
  - Spec §9 testing & gates → Tasks 10, 13, 14, 20, 21, 22.
- Placeholder scan: no "TBD" / "TODO" / "implement later" remains in plan steps. The few `unimplemented!()` sentinels in Task 6 Step 2 are explicit fixture stubs that the executing engineer fills in by lifting from existing commit-module test fixtures — both the source and the pattern are named.
- Type consistency: `IcebergRefBinding`, `IcebergRefKind`, `IcebergDmlTarget`, `IcebergRefSuffix`, `RefAction(Plan)` are defined once in Task 1 / 5 / 6 / 7 / 18 and referenced by exact name everywhere else. The analyzer-side `RefActionPlan` (Task 6) and connector-side `RefActionPlan` (Task 7) have the same field names; the lower stage in Task 8 maps one to the other explicitly.
- Concurrent OCC test: the design's `iceberg_branch_concurrent_occ.sql` requires a runner extension. To keep this plan within phase 1, the concurrent SQL test is **deferred** — it would otherwise be Task 22b. Fail-fast OCC behaviour is exercised at the unit-test layer (Task 16 Step E checks "main untouched, dev advanced" sequentially); end-to-end concurrent stress can land alongside the runner extension as a follow-up.
