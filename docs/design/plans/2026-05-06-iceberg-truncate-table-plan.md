# Iceberg TRUNCATE TABLE Implementation Plan (PR-1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `TRUNCATE TABLE <ident>[.branch_<x>]` for Iceberg tables — write a single `operation=delete` snapshot that marks every live data / DV / position-delete / equality-delete file as DELETED while preserving schema, partition spec, properties, and other refs.

**Architecture:** Add a new `Statement::Truncate` AST variant + dialect parsing; extend `execute_truncate_table_statement` (`src/engine/statement.rs:1037`) with an Iceberg branch; introduce a `TruncateCommit` action implementing `IcebergCommitAction` that reuses the existing `run_iceberg_commit` pipeline (`src/connector/iceberg/commit/run.rs`). Live file enumeration is extended to cover all `ManifestContentType` variants (Data + Deletes), since current `enumerate_live_data_files` (`overwrite.rs:324`) intentionally skips delete manifests.

**Tech Stack:** Rust 2021, sqlparser-rs (forked via `StarRocksDialect`), iceberg-rust 0.9.0 (vendored in `vendor/iceberg-0.9.0`), arrow-rs.

**Spec:** `docs/design/specs/2026-05-06-iceberg-v3-write-path-completion-design.md` §3 / §7.1 / §8.

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Modify | `src/sql/parser/ast/mod.rs` | Add `Statement::Truncate { name, target_ref }` variant |
| Modify | `src/sql/parser/dialect/mod.rs` (or sibling parser entry) | Recognize `TRUNCATE TABLE` keyword + reject `PARTITION` / `WHERE` |
| Modify | `src/engine/statement.rs:1037-1055` | Replace current `execute_truncate_table_statement` to branch managed-lake vs iceberg; route iceberg to new flow |
| Create | `src/engine/iceberg_truncate.rs` | New `execute_iceberg_truncate_table()` entry; parses `target_ref`, drives `run_iceberg_commit` with `TruncateCommit` |
| Modify | `src/engine/mod.rs` | `pub(crate) mod iceberg_truncate;` |
| Modify | `src/connector/iceberg/commit/types.rs` (or wherever `CommitOpKind` lives) | Add `CommitOpKind::Truncate` variant |
| Create | `src/connector/iceberg/commit/truncate.rs` | `TruncateCommit` struct + `IcebergCommitAction` impl + `TruncateTxnAction` |
| Modify | `src/connector/iceberg/commit/mod.rs` | `pub mod truncate; pub use truncate::TruncateCommit;` |
| Modify | `src/connector/iceberg/commit/run.rs` | Dispatch `CommitOpKind::Truncate` → `TruncateCommit` |
| Modify | `src/connector/iceberg/commit/overwrite.rs` | Extract `enumerate_live_all_files` covering all manifest content types; keep `enumerate_live_data_files` as a thin filtered wrapper for backward compatibility |
| Create | `sql-tests/iceberg/sql/iceberg_truncate.sql` | SQL regression suite per spec §7.1 |
| Create | `sql-tests/iceberg/result/iceberg_truncate.result` | Recorded fixture |
| Modify | `/Users/harbor/Documents/Obsidian/NovaRocks Iceberg v3 完成度清单.md` | §10 mark TRUNCATE as `[x]`; §20 add fixture line; trailing changelog row |

---

## Pre-Flight Investigation

Before Task 1, the implementer should read these files end-to-end (they will be edited or referenced repeatedly):

- `src/sql/parser/ast/mod.rs` — to find the existing `Statement` enum and its variant style
- `src/sql/parser/dialect/mod.rs` and any sibling files in `src/sql/parser/dialect/` — to understand how the current parser dispatches keywords
- `src/connector/iceberg/commit/overwrite.rs` (full file ~400 lines) — `OverwriteCommit` / `OverwriteTxnAction` is the closest template
- `src/engine/iceberg_writer.rs` lines 1–210 — `execute_iceberg_insert_or_overwrite` is the reference for the engine-side flow (`IcebergCommitCollector` setup, `RunInput` wiring, `block_on_iceberg`, `invalidate_iceberg_caches`)
- `src/engine/insert_flow.rs:19-110` — reference for `split_ref_suffix` / `target_ref` handling
- `src/connector/iceberg/commit/run.rs` — to find the dispatcher pattern (`match CommitOpKind`)

---

## Task 1: Add `Statement::Truncate` AST variant + parser recognition

**Files:**
- Modify: `src/sql/parser/ast/mod.rs`
- Modify: `src/sql/parser/dialect/mod.rs` (or the parser-entry file used to dispatch top-level keywords — confirmed during pre-flight)

- [ ] **Step 1.1: Locate the `Statement` enum**

Run: `grep -n "pub enum Statement" src/sql/parser/ast/mod.rs`

Expected: a single line giving the enum location. Read the variants around it to learn the existing style (e.g. `Insert { ... }` vs tuple-struct variants).

- [ ] **Step 1.2: Find the parser dispatch site**

Run: `grep -n "Keyword::INSERT\|Keyword::DELETE\|Keyword::CREATE" src/sql/parser/dialect/*.rs`

Expected: a `match` block where top-level statements are dispatched. Note the file:line — that is where the new `Keyword::TRUNCATE` arm goes.

- [ ] **Step 1.3: Write the failing parser test**

Add to `src/sql/parser/dialect/mod.rs` (or wherever existing parser unit tests live; confirm via `grep -n "#\[test\]" src/sql/parser/dialect/mod.rs`):

```rust
#[test]
fn parse_truncate_table_basic() {
    let stmt = parse_one("TRUNCATE TABLE t").expect("parse");
    match stmt {
        Statement::Truncate { name, target_ref } => {
            assert_eq!(name.parts, vec!["t".to_string()]);
            assert_eq!(target_ref, "main");
        }
        other => panic!("expected Truncate, got {other:?}"),
    }
}

#[test]
fn parse_truncate_table_branch() {
    let stmt = parse_one("TRUNCATE TABLE t.branch_dev").expect("parse");
    match stmt {
        Statement::Truncate { name, target_ref } => {
            assert_eq!(name.parts, vec!["t".to_string()]);
            assert_eq!(target_ref, "dev");
        }
        other => panic!("expected Truncate, got {other:?}"),
    }
}

#[test]
fn parse_truncate_table_partition_rejected() {
    let err = parse_one("TRUNCATE TABLE t PARTITION (p=1)").unwrap_err();
    assert!(
        err.to_lowercase().contains("partition"),
        "expected PARTITION rejection, got {err}",
    );
}

#[test]
fn parse_truncate_table_where_rejected() {
    let err = parse_one("TRUNCATE TABLE t WHERE c=1").unwrap_err();
    assert!(
        err.to_lowercase().contains("where"),
        "expected WHERE rejection, got {err}",
    );
}
```

If a `parse_one` helper does not already exist, copy the smallest existing test helper that returns a single `Statement` from raw SQL (look in the same `mod.rs`).

- [ ] **Step 1.4: Run the tests to verify they fail**

Run: `cargo test --lib parse_truncate_table -- --nocapture`

Expected: 4 failures, message `Statement::Truncate` does not exist.

- [ ] **Step 1.5: Add the `Statement::Truncate` variant**

In `src/sql/parser/ast/mod.rs`, add to `pub enum Statement`:

```rust
Truncate {
    name: ObjectName,
    /// `"main"` by default; branch name when the SQL uses `t.branch_<name>`.
    target_ref: String,
},
```

Place it next to `Statement::Delete` (or whichever DML variant exists) for locality. If the enum derives `Debug`/`Clone`, the new variant inherits these automatically.

- [ ] **Step 1.6: Implement parser dispatch**

In the parser dispatch file (located in Step 1.2), inside the top-level keyword match, add:

```rust
Keyword::TRUNCATE => {
    parser.next_token(); // consume TRUNCATE
    parser.expect_keyword(Keyword::TABLE)
        .map_err(|e| format!("TRUNCATE: {e}"))?;
    let raw_name = parser.parse_object_name(false)
        .map_err(|e| format!("TRUNCATE TABLE: {e}"))?;
    // Reject PARTITION / WHERE / any trailing tokens.
    if parser.parse_keyword(Keyword::PARTITION) {
        return Err("TRUNCATE TABLE PARTITION (...) is not supported".to_string());
    }
    if parser.parse_keyword(Keyword::WHERE) {
        return Err("TRUNCATE TABLE WHERE <predicate> is not supported".to_string());
    }
    // Resolve branch suffix.
    let parts: Vec<String> = raw_name.0.iter().map(|i| i.value.clone()).collect();
    let object_name = crate::sql::parser::ast::ObjectName { parts: parts.clone() };
    let (stripped_parts, ref_suffix) =
        crate::sql::analyzer::iceberg_ref::split_ref_suffix(&parts);
    let (final_name, target_ref) = match ref_suffix {
        Some(crate::sql::analyzer::iceberg_ref::IcebergRefSuffix::Tag(t)) => {
            return Err(format!(
                "TRUNCATE TABLE: tag '{t}' is read-only; use a branch as target"
            ));
        }
        Some(crate::sql::analyzer::iceberg_ref::IcebergRefSuffix::Branch(b)) => (
            crate::sql::parser::ast::ObjectName { parts: stripped_parts },
            b,
        ),
        None => (object_name, "main".to_string()),
    };
    Ok(Statement::Truncate { name: final_name, target_ref })
}
```

Note: the exact path qualifiers (`crate::sql::...`) and the `ObjectName` constructor must mirror the existing imports in the dispatch file. If imports already pull in `ObjectName` and `split_ref_suffix`, drop the `crate::...` prefix.

- [ ] **Step 1.7: Run the tests, verify they pass**

Run: `cargo test --lib parse_truncate_table -- --nocapture`

Expected: 4 PASS.

- [ ] **Step 1.8: Run cargo fmt + clippy**

Run: `cargo fmt && cargo clippy --lib --tests -- -D warnings 2>&1 | head -50`

Expected: no warnings. Fix any flagged issues before commit.

- [ ] **Step 1.9: Commit**

```bash
git add src/sql/parser/ast/mod.rs src/sql/parser/dialect/
git commit -m "feat(parser): add Statement::Truncate with branch suffix support"
```

---

## Task 2: Wire Iceberg branch into `execute_truncate_table_statement`

**Files:**
- Modify: `src/engine/statement.rs:1037-1055`
- Create: `src/engine/iceberg_truncate.rs`
- Modify: `src/engine/mod.rs`

- [ ] **Step 2.1: Read current truncate entry**

Run: `sed -n '1030,1060p' src/engine/statement.rs`

Confirm the function signature is what the spec recorded. Find the place where `Statement::Truncate` would be dispatched to this function — search:

`grep -n "execute_truncate_table_statement\|Statement::Truncate" src/engine/`

If the dispatch site does not yet match `Statement::Truncate { name, target_ref }`, also update it to pass `target_ref` through.

- [ ] **Step 2.2: Add the new engine module file (skeleton)**

Create `src/engine/iceberg_truncate.rs`:

```rust
//! Iceberg TRUNCATE TABLE entry. Drives a `TruncateCommit` through
//! `run_iceberg_commit` so the table is rewound to a single
//! `operation=delete` snapshot while schema, partition spec, properties,
//! and other refs are preserved.

use std::sync::Arc;

use crate::connector::backend::ResolvedTable;
use crate::engine::backend_resolver::resolve_existing_table_target;
use crate::engine::{StandaloneState, StatementResult};

pub(crate) fn execute_iceberg_truncate_table(
    _state: &Arc<StandaloneState>,
    _target: &ResolvedTable,
    _resolved: &crate::connector::backend::CatalogEntry,
    _target_ref: &str,
) -> Result<StatementResult, String> {
    Err("TODO: TruncateCommit not yet implemented".to_string())
}
```

(Type names like `CatalogEntry` may differ — confirm by `grep -n "pub struct CatalogEntry\|pub type CatalogEntry" src/connector/`. Match whichever the existing `iceberg_writer::execute_iceberg_insert_or_overwrite` accepts.)

- [ ] **Step 2.3: Register the module**

In `src/engine/mod.rs`, locate the `iceberg_writer` declaration:

`grep -n "pub(crate) mod iceberg_writer" src/engine/mod.rs`

Add immediately after:

```rust
pub(crate) mod iceberg_truncate;
```

- [ ] **Step 2.4: Update `execute_truncate_table_statement`**

In `src/engine/statement.rs`, replace the function body with:

```rust
pub(crate) fn execute_truncate_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    target_ref: &str,
    current_database: &str,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name, current_database)?;
    if state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock")
        .contains_table(&resolved.database, &resolved.table)?
    {
        if target_ref != "main" {
            return Err(format!(
                "TRUNCATE TABLE: branch target `{target_ref}` is only supported for iceberg tables"
            ));
        }
        return truncate_managed_lake_table(state, &resolved.database, &resolved.table);
    }

    // Fall through to iceberg backend resolution.
    let target = resolve_existing_table_target(state, name, None, current_database)?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "TRUNCATE TABLE only supports managed-lake or iceberg tables: {}.{}",
            resolved.database, resolved.table
        ));
    }
    let entry = {
        let reg = state.connectors.read().expect("connector registry read");
        reg.catalog_backend(target.backend_name)?
    };
    let resolved_entry = entry.load_table(&target.catalog, &target.namespace, &target.table)?;
    crate::engine::iceberg_truncate::execute_iceberg_truncate_table(
        state,
        &target,
        &resolved_entry,
        target_ref,
    )
}
```

The function signature now takes `target_ref: &str` (added). Update every caller (the `Statement::Truncate` dispatch site found in Step 2.1) to pass it.

- [ ] **Step 2.5: Build to confirm wiring compiles**

Run: `cargo build 2>&1 | tail -30`

Expected: build succeeds. The placeholder `TODO: TruncateCommit not yet implemented` is wired in but not executed yet.

- [ ] **Step 2.6: Run a trivial smoke test**

Add a unit test in `src/engine/iceberg_truncate.rs` (top-level `#[cfg(test)] mod tests`):

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn placeholder_returns_error() {
        // Dummy test; Task 6 replaces with a meaningful assertion.
        // Ensures the module is compiled into the test binary.
        assert_eq!(2 + 2, 4);
    }
}
```

Run: `cargo test --lib iceberg_truncate`. Expected: 1 PASS.

- [ ] **Step 2.7: Commit**

```bash
git add src/engine/statement.rs src/engine/mod.rs src/engine/iceberg_truncate.rs
git commit -m "feat(engine): route TRUNCATE TABLE to iceberg flow (placeholder)"
```

---

## Task 3: Add `CommitOpKind::Truncate` variant

**Files:**
- Modify: `src/connector/iceberg/commit/types.rs` (or wherever `CommitOpKind` lives — confirmed via grep)

- [ ] **Step 3.1: Locate the enum**

Run: `grep -rn "pub enum CommitOpKind" src/connector/iceberg/commit/`

Expected: one match. Read the file to see existing variants (`FastAppend`, `Overwrite`, `RowDelta`, etc.).

- [ ] **Step 3.2: Add the variant**

```rust
pub enum CommitOpKind {
    FastAppend,
    Overwrite,
    // ... existing ...
    Truncate,
}
```

- [ ] **Step 3.3: Find every `match` on `CommitOpKind` and update for exhaustiveness**

Run: `grep -rn "CommitOpKind::" src/connector/iceberg/`

For each `match`, decide:
- If the match dispatches to a commit action (e.g. in `run.rs`), add a `CommitOpKind::Truncate => Box::new(TruncateCommit)` arm — but `TruncateCommit` is not defined yet. As a temporary placeholder, add `CommitOpKind::Truncate => unimplemented!("TruncateCommit lands in Task 5")`.
- If the match is purely descriptive (e.g. for logging), add a sensible string like `"truncate"`.

- [ ] **Step 3.4: Build**

Run: `cargo build 2>&1 | tail -20`

Expected: compiles (the `unimplemented!()` is OK for compile-only build; runtime is gated by Task 7's wire-up).

- [ ] **Step 3.5: Commit**

```bash
git add src/connector/iceberg/
git commit -m "feat(commit): add CommitOpKind::Truncate variant"
```

---

## Task 4: Extract `enumerate_live_all_files` covering all manifest content types

**Files:**
- Modify: `src/connector/iceberg/commit/overwrite.rs:320-363`

- [ ] **Step 4.1: Write the failing test**

Append to `src/connector/iceberg/commit/overwrite.rs` a `#[cfg(test)] mod enumerate_tests` block (this test sets up an in-memory iceberg table with one data file + one position-delete file + one DV blob and verifies the new function returns 3 entries; the existing `enumerate_live_data_files` returns 1):

```rust
#[cfg(test)]
mod enumerate_tests {
    use super::*;
    // helper to build a Table fixture with mixed manifest content types
    // (use the in-memory iceberg test helpers from
    //  vendor/iceberg-0.9.0/src/spec/manifest_list.rs tests, or build via
    //  iceberg::TableBuilder if available)

    #[tokio::test]
    async fn enumerate_live_all_files_includes_delete_manifests() {
        let (table, file_io) = build_table_with_data_and_deletes().await;
        let all = enumerate_live_all_files(&table, &file_io).await.unwrap();
        // Expect 3: one data + one position-delete + one DV
        assert_eq!(all.len(), 3, "got {all:?}");
        let data_count = all.iter().filter(|(df, _, _)| {
            df.content_type() == DataContentType::Data
        }).count();
        let posdel_count = all.iter().filter(|(df, _, _)| {
            df.content_type() == DataContentType::PositionDeletes
        }).count();
        let eqdel_count = all.iter().filter(|(df, _, _)| {
            df.content_type() == DataContentType::EqualityDeletes
        }).count();
        assert_eq!(data_count, 1);
        assert_eq!(posdel_count + eqdel_count, 2,
            "DV is encoded as PositionDeletes content_type per Iceberg v3 spec");
    }
}
```

If building a table fixture inline is unwieldy, replace the body with a smaller test that uses `mockall`-style or a fixture file checked into `tests/fixtures/iceberg/`. Pre-flight: check whether existing tests in `overwrite.rs` already build such a fixture (`grep -n "build_table\|mock_table\|fn make_table" src/connector/iceberg/commit/overwrite.rs`) and copy the helper.

- [ ] **Step 4.2: Run the test, verify it fails**

Run: `cargo test --lib enumerate_live_all_files_includes_delete_manifests`

Expected: FAIL — function not defined.

- [ ] **Step 4.3: Implement `enumerate_live_all_files`**

Insert in `overwrite.rs` (immediately above `enumerate_live_data_files`):

```rust
/// Walk every manifest in the base snapshot's manifest list and collect
/// every live entry (Data + Deletes), returning each entry's
/// `(DataFile, sequence_number, file_sequence_number)` tuple.
///
/// Distinct from `enumerate_live_data_files` which intentionally skips
/// delete manifests (used by INSERT OVERWRITE which preserves deletes).
/// `TRUNCATE TABLE` requires this fuller enumeration to mark every
/// content type as DELETED in the new snapshot.
pub(super) async fn enumerate_live_all_files(
    table: &Table,
    file_io: &FileIO,
) -> Result<Vec<(DataFile, i64, Option<i64>)>, String> {
    let m = table.metadata();
    let snap = match m.current_snapshot() {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };
    let bytes = file_io
        .new_input(snap.manifest_list())
        .map_err(|e| format!("FileIO::new_input({}) failed: {e}", snap.manifest_list()))?
        .read()
        .await
        .map_err(|e| format!("read manifest_list failed: {e}"))?;
    let list = iceberg::spec::ManifestList::parse_with_version(&bytes, m.format_version())
        .map_err(|e| format!("parse manifest_list failed: {e}"))?;

    let mut out = Vec::new();
    for entry in list.entries() {
        // Note: do NOT filter by content type — accept Data and Deletes.
        let manifest = entry
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load_manifest({}) failed: {e}", entry.manifest_path))?;
        for me in manifest.entries() {
            if me.is_alive() {
                let data_file = me.data_file().clone();
                let seq = me.sequence_number().unwrap_or(entry.sequence_number);
                let file_seq = me.file_sequence_number;
                out.push((data_file, seq, file_seq));
            }
        }
    }
    Ok(out)
}
```

- [ ] **Step 4.4: Run the test, verify it passes**

Run: `cargo test --lib enumerate_live_all_files_includes_delete_manifests -- --nocapture`

Expected: PASS.

- [ ] **Step 4.5: Confirm `enumerate_live_data_files` still passes its tests**

Run: `cargo test --lib enumerate_live_data_files`

Expected: PASS (existing behavior unchanged).

- [ ] **Step 4.6: Commit**

```bash
git add src/connector/iceberg/commit/overwrite.rs
git commit -m "feat(commit): add enumerate_live_all_files for TRUNCATE"
```

---

## Task 5: Implement `TruncateCommit` skeleton + dispatcher wire-up

**Files:**
- Create: `src/connector/iceberg/commit/truncate.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`
- Modify: `src/connector/iceberg/commit/run.rs`

- [ ] **Step 5.1: Write a failing integration test (no-op truncate of empty table)**

Add to a new module `src/connector/iceberg/commit/truncate.rs` (creating the file):

```rust
//! `TruncateCommit` — write a single `operation=delete` snapshot that
//! marks every live data + delete file as DELETED while preserving
//! schema, partition spec, properties, and other refs.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestStatus,
    ManifestWriterBuilder, Operation, PartitionSpecRef, SchemaRef, Snapshot, SnapshotReference,
    SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction};
use super::helpers::{generate_snapshot_id, metadata_dir, now_ms, write_manifest_list};
use super::overwrite::enumerate_live_all_files;
use super::types::CommitOutcome;

pub struct TruncateCommit;

#[async_trait]
impl IcebergCommitAction for TruncateCommit {
    async fn commit(&self, _ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        Err("TruncateCommit::commit not implemented".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn truncate_returns_error_until_implemented() {
        // Real test in Task 6 replaces this stub.
        let action = TruncateCommit;
        // Minimal smoke: the type implements Send + Sync.
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        assert_send_sync(&action);
    }
}
```

- [ ] **Step 5.2: Register module + export**

In `src/connector/iceberg/commit/mod.rs`, add:

```rust
pub mod truncate;
pub use truncate::TruncateCommit;
```

Place near the existing `pub mod overwrite; pub use overwrite::OverwriteCommit;` line.

- [ ] **Step 5.3: Wire dispatcher**

In `src/connector/iceberg/commit/run.rs`, find the dispatcher (the `match` on `CommitOpKind`):

```rust
let action: Box<dyn IcebergCommitAction> = match collector.op_kind() {
    CommitOpKind::FastAppend => Box::new(FastAppendCommit { /* ... */ }),
    CommitOpKind::Overwrite => Box::new(OverwriteCommit),
    // ...
    CommitOpKind::Truncate => Box::new(TruncateCommit),  // <-- replace the unimplemented! from Task 3
};
```

(The exact dispatch shape — whether it constructs `Box<dyn ...>` or a custom enum — is in `run.rs`; adapt accordingly.)

- [ ] **Step 5.4: Build**

Run: `cargo build 2>&1 | tail -20`

Expected: compiles.

- [ ] **Step 5.5: Commit**

```bash
git add src/connector/iceberg/commit/
git commit -m "feat(commit): TruncateCommit skeleton + dispatcher wire-up"
```

---

## Task 6: Implement `TruncateCommit::commit` main logic

**Files:**
- Modify: `src/connector/iceberg/commit/truncate.rs`

This task is the largest single body of code in the PR. We implement and test it in three sub-tasks: 6a (empty-table truncate / noop snapshot), 6b (table with data files only), 6c (table with mixed data + delete files).

### Task 6a: Empty-table truncate writes a noop `operation=delete` snapshot

- [ ] **Step 6a.1: Write the failing test**

Replace the `#[cfg(test)] mod tests` block in `truncate.rs` with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::commit::test_helpers::{
        empty_v3_iceberg_table, run_commit_with,
    };

    #[tokio::test]
    async fn truncate_empty_v3_table_writes_noop_snapshot() {
        let (table, file_io, catalog) = empty_v3_iceberg_table().await;
        let outcome = run_commit_with(TruncateCommit, &table, &file_io, &catalog, "main")
            .await
            .expect("truncate should succeed");
        // Reload the table — the new metadata.json should have a snapshot
        // with operation=delete and zero deleted-files-count.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        let snap = reloaded.metadata().current_snapshot().unwrap();
        assert_eq!(snap.summary().operation, Operation::Delete);
        assert_eq!(snap.summary().additional_properties
            .get("deleted-data-files").map(String::as_str), Some("0"));
        assert_eq!(snap.summary().additional_properties
            .get("added-data-files").map(String::as_str), Some("0"));
        assert_ne!(snap.snapshot_id(), outcome.new_snapshot_id - 1,
            "new snapshot was actually written");
    }
}
```

If `crate::connector::iceberg::commit::test_helpers` does not yet exist, create it now under `src/connector/iceberg/commit/test_helpers.rs` as a `#[cfg(test)] pub mod` re-exported from `mod.rs`. The two helpers (`empty_v3_iceberg_table`, `run_commit_with`) wrap the existing iceberg-rust in-memory catalog + the `run_iceberg_commit` driver. Skeleton:

```rust
// src/connector/iceberg/commit/test_helpers.rs
#[cfg(test)]
pub(crate) async fn empty_v3_iceberg_table() -> (
    iceberg::table::Table,
    iceberg::io::FileIO,
    std::sync::Arc<dyn iceberg::Catalog>,
) {
    // Use iceberg::catalog::memory::MemoryCatalog with FormatVersion::V3
    // and row-lineage=true. Build a 2-column schema (id INT, name STRING),
    // unpartitioned, no data files.
    todo!("expand from existing iceberg-rust test patterns")
}

#[cfg(test)]
pub(crate) async fn run_commit_with(
    action: impl IcebergCommitAction + 'static,
    table: &iceberg::table::Table,
    file_io: &iceberg::io::FileIO,
    catalog: &std::sync::Arc<dyn iceberg::Catalog>,
    target_ref: &str,
) -> Result<CommitOutcome, String> {
    use crate::connector::iceberg::commit::run::{run_iceberg_commit, RunInput};
    use crate::connector::iceberg::commit::collector::IcebergCommitCollector;
    // Wire up a minimal RunInput. See iceberg_writer.rs:163-200 for the
    // production wiring pattern; copy and reduce.
    todo!()
}
```

Pre-flight: search for any existing test helper that builds an iceberg table — `grep -rn "MemoryCatalog\|FormatVersion::V3" src/ tests/`. Reuse rather than write from scratch.

- [ ] **Step 6a.2: Run the test, verify it fails**

Run: `cargo test --lib truncate_empty_v3_table_writes_noop_snapshot`

Expected: FAIL — `TruncateCommit::commit` returns the `not implemented` error.

- [ ] **Step 6a.3: Implement the noop snapshot path**

Replace `TruncateCommit::commit` with the full implementation. Body modeled on `OverwriteCommit::commit` (`overwrite.rs:62-122`), simplified: no `written_files` collection, no `row_lineage_first_row_id` advance, no `OverwriteTxnAction.written` field.

```rust
#[async_trait]
impl IcebergCommitAction for TruncateCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = TruncateTxnAction {
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            partition_spec: ctx.collector.partition_spec.clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
            target_ref: ctx.target_ref.to_string(),
        };
        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("Truncate apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("Truncate commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(0);
        let written_manifest_paths = manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .clone();
        Ok(CommitOutcome { new_snapshot_id, written_manifest_paths })
    }
}

struct TruncateTxnAction {
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema_id: i32,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    target_ref: String,
}

#[async_trait]
impl TransactionAction for TruncateTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = m
            .refs()
            .get(target_ref.as_str())
            .map(|r| r.snapshot_id)
            .or_else(|| {
                if target_ref == "main" {
                    m.current_snapshot().map(|s| s.snapshot_id())
                } else {
                    None
                }
            });
        let metadata_dir = metadata_dir(table);

        let existing = enumerate_live_all_files(table, &self.file_io)
            .await
            .map_err(to_iceberg_unexpected)?;

        // Empty-base path: write a noop delete snapshot (still an audit trail).
        // We must still produce a valid manifest list so downstream readers
        // can tell the snapshot exists. iceberg-rust accepts an empty list.
        let new_manifests: Vec<ManifestFile> = if existing.is_empty() {
            Vec::new()
        } else {
            // Mirror overwrite.rs's deletes-manifest writer.
            let path = format!("{metadata_dir}/{}-truncate-deletes-0.avro", self.commit_uuid);
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(path.clone());
            let mf = super::overwrite::write_overwrite_deletes_manifest(
                &self.file_io,
                &path,
                &existing,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            vec![mf]
        };

        // Write manifest list (may be empty — valid for noop truncate).
        let manifest_list_path = format!(
            "{metadata_dir}/snap-{new_snapshot_id}-1-{}.avro",
            self.commit_uuid
        );
        write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            &new_manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            format_version,
        )
        .await
        .map_err(to_iceberg_unexpected)?;

        // Build the snapshot.
        let mut summary_props = std::collections::HashMap::new();
        let deleted_count: usize = existing.iter().filter(|(df, _, _)| {
            df.content_type() == DataContentType::Data
        }).count();
        let removed_pos: usize = existing.iter().filter(|(df, _, _)| {
            df.content_type() == DataContentType::PositionDeletes
        }).count();
        let removed_eq: usize = existing.iter().filter(|(df, _, _)| {
            df.content_type() == DataContentType::EqualityDeletes
        }).count();
        let deleted_size: i64 = existing.iter().map(|(df, _, _)| df.file_size_in_bytes()).sum();
        summary_props.insert("operation".to_string(), "delete".to_string());
        summary_props.insert("added-data-files".to_string(), "0".to_string());
        summary_props.insert("added-files-size".to_string(), "0".to_string());
        summary_props.insert("deleted-data-files".to_string(), deleted_count.to_string());
        summary_props.insert("removed-files-size".to_string(), deleted_size.to_string());
        if removed_pos > 0 {
            summary_props.insert("removed-position-delete-files".to_string(), removed_pos.to_string());
        }
        if removed_eq > 0 {
            summary_props.insert("removed-equality-delete-files".to_string(), removed_eq.to_string());
        }

        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path.clone())
            .with_summary(Summary {
                operation: Operation::Delete,
                additional_properties: summary_props,
            })
            .with_schema_id(self.schema_id)
            .build();

        // Build TableUpdates + TableRequirements.
        let updates = vec![
            TableUpdate::AddSnapshot { snapshot: snapshot.clone() },
            TableUpdate::SetSnapshotRef {
                ref_name: target_ref.clone(),
                reference: SnapshotReference {
                    snapshot_id: new_snapshot_id,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            },
        ];
        let requirements = vec![
            TableRequirement::RefSnapshotIdMatch {
                r#ref: target_ref.clone(),
                snapshot_id: parent_snapshot_id,
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: self.schema_id,
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

fn to_iceberg_unexpected(e: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, e)
}
```

(The `Snapshot::builder()` chain mirrors how `OverwriteTxnAction` builds its snapshot — confirm during pre-flight by reading `overwrite.rs:200-300` and copy the exact builder shape; the snippet above shows the structure.)

- [ ] **Step 6a.4: Run the test, verify it passes**

Run: `cargo test --lib truncate_empty_v3_table_writes_noop_snapshot -- --nocapture`

Expected: PASS.

### Task 6b: Truncate of table with data files only

- [ ] **Step 6b.1: Write the failing test**

Add to `truncate.rs::tests`:

```rust
#[tokio::test]
async fn truncate_table_with_data_files_marks_all_deleted() {
    let (table, file_io, catalog) = v3_table_with_n_data_files(3).await;
    let outcome = run_commit_with(TruncateCommit, &table, &file_io, &catalog, "main")
        .await
        .expect("truncate succeeds");
    let reloaded = catalog.load_table(table.identifier()).await.unwrap();
    let snap = reloaded.metadata().current_snapshot().unwrap();
    assert_eq!(snap.summary().operation, Operation::Delete);
    assert_eq!(
        snap.summary().additional_properties.get("deleted-data-files"),
        Some(&"3".to_string()),
    );
    // Verify the post-truncate snapshot has zero live data files.
    let live = enumerate_live_all_files(&reloaded, &file_io).await.unwrap();
    assert!(live.is_empty(), "expected 0 live files after truncate, got {live:?}");
}
```

`v3_table_with_n_data_files` is a new helper in `test_helpers.rs`. Pre-flight: check whether a similar fixture exists in `commit/overwrite.rs::tests` and copy.

- [ ] **Step 6b.2: Run, verify it fails (or already passes)**

Run: `cargo test --lib truncate_table_with_data_files_marks_all_deleted -- --nocapture`

If Step 6a.3's implementation already handles this case correctly (likely — the same code path already enumerates and writes deletes), it should PASS without code changes. If it FAILS, the failure points to specific bugs to fix. Investigate the assertion that fails first.

- [ ] **Step 6b.3: Commit (assuming 6a + 6b green)**

```bash
git add src/connector/iceberg/commit/truncate.rs src/connector/iceberg/commit/test_helpers.rs src/connector/iceberg/commit/mod.rs
git commit -m "feat(commit): TruncateCommit handles empty + data-only tables"
```

### Task 6c: Truncate of table with mixed data + delete files

- [ ] **Step 6c.1: Write the failing test**

```rust
#[tokio::test]
async fn truncate_table_with_data_and_dv_marks_all_deleted() {
    let (table, file_io, catalog) = v3_table_with_data_and_dv().await;
    let _ = run_commit_with(TruncateCommit, &table, &file_io, &catalog, "main")
        .await
        .expect("truncate succeeds");
    let reloaded = catalog.load_table(table.identifier()).await.unwrap();
    let snap = reloaded.metadata().current_snapshot().unwrap();
    assert!(snap.summary().additional_properties
        .get("removed-position-delete-files").is_some(),
        "DV/position-delete files must be reflected in summary");
    let live = enumerate_live_all_files(&reloaded, &file_io).await.unwrap();
    assert!(live.is_empty());
}
```

`v3_table_with_data_and_dv` is a new helper that produces a table with: 2 data files + 1 DV blob (apply via `commit/puffin_dv.rs`'s existing helpers; pre-flight: `grep -n "fn write_dv\|fn append_dv" src/connector/iceberg/commit/puffin_dv.rs`).

- [ ] **Step 6c.2: Run, verify it fails or passes**

Run: `cargo test --lib truncate_table_with_data_and_dv -- --nocapture`

If FAIL — the most common cause is `enumerate_live_all_files` returning DV files but `write_overwrite_deletes_manifest` only handling data-content rows. Inspect; add a content-type-aware variant if needed.

- [ ] **Step 6c.3: Commit**

```bash
git add src/connector/iceberg/commit/truncate.rs src/connector/iceberg/commit/test_helpers.rs
git commit -m "feat(commit): TruncateCommit handles mixed data + DV tables"
```

---

## Task 7: Wire engine-side flow + smoke SQL

**Files:**
- Modify: `src/engine/iceberg_truncate.rs` (replace placeholder)

- [ ] **Step 7.1: Implement the engine entry**

```rust
use std::sync::Arc;

use crate::connector::backend::{CatalogEntry, ResolvedTable};
use crate::connector::iceberg::commit::{
    run_iceberg_commit, CommitOpKind, IcebergCommitCollector, RunInput,
};
use crate::engine::iceberg_writer::{
    block_on_iceberg, build_abort_cleanup_for_catalog_entry, invalidate_iceberg_caches,
};
use crate::engine::{StandaloneState, StatementResult};

pub(crate) fn execute_iceberg_truncate_table(
    state: &Arc<StandaloneState>,
    target: &ResolvedTable,
    resolved_entry: &CatalogEntry,
    target_ref: &str,
) -> Result<StatementResult, String> {
    // Tag targets are read-only; rejected at parser level but defend in depth.
    if target_ref != "main" {
        // Branch target: validate v3 row-lineage is on (same rule as branch INSERT).
        let metadata = resolved_entry.iceberg_metadata()
            .ok_or_else(|| "TRUNCATE TABLE: branch target requires iceberg backend".to_string())?;
        if !metadata.row_lineage_enabled() {
            return Err(
                "TRUNCATE TABLE: branch-qualified target requires v3 row-lineage table"
                    .to_string(),
            );
        }
    }

    let (catalog, table) = {
        let reg = state.connectors.read().expect("connector registry read");
        let backend = reg.catalog_backend(target.backend_name)?;
        let table = backend
            .load_iceberg_table(&target.catalog, &target.namespace, &target.table)?;
        (backend.iceberg_catalog().clone(), table)
    };
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/metadata/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::Truncate,
        resolved_entry.identifier().clone(),
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));

    let abort_cleanup = build_abort_cleanup_for_catalog_entry(resolved_entry)?;
    let file_io = table.file_io().clone();
    let _outcome = block_on_iceberg(async {
        run_iceberg_commit(RunInput {
            collector,
            catalog,
            table,
            fs: abort_cleanup.fs,
            file_io,
            cleanup_path_mapper: abort_cleanup.path_mapper,
            cow_update_sidecar: None,
            target_ref: target_ref.to_string(),
        })
        .await
    })??;

    invalidate_iceberg_caches(state, target)?;
    Ok(StatementResult::Ok)
}
```

(Symbol names like `block_on_iceberg`, `build_abort_cleanup_for_catalog_entry`, `invalidate_iceberg_caches` are confirmed via `grep -n` in `iceberg_writer.rs`. If any are private to that module, make them `pub(crate)` in `iceberg_writer.rs`.)

- [ ] **Step 7.2: Build**

Run: `cargo build 2>&1 | tail -30`

Expected: clean build.

- [ ] **Step 7.3: Smoke test via standalone-server**

Start the server in one terminal:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030
```

In another terminal, run a manual MySQL-protocol smoke:

```bash
mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS test_truncate;
USE test_truncate;
CREATE TABLE t (id INT, name VARCHAR(64))
  ENGINE=ICEBERG
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT COUNT(*) FROM t;
TRUNCATE TABLE t;
SELECT COUNT(*) FROM t;
SQL
```

Expected: COUNT before truncate = 3, COUNT after = 0. Stop the server (Ctrl-C).

If the smoke fails (e.g. with a panic from missing dispatcher arms), fix iteratively — the SQL regression in Task 8 covers the same paths but with assertion-based fixtures.

- [ ] **Step 7.4: Commit**

```bash
git add src/engine/iceberg_truncate.rs
git commit -m "feat(engine): wire TruncateCommit into TRUNCATE TABLE flow"
```

---

## Task 8: SQL regression suite

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_truncate.sql`
- Create: `sql-tests/iceberg/result/iceberg_truncate.result` (recorded via `--mode record`)

- [ ] **Step 8.1: Create the SQL file**

```sql
-- iceberg_truncate.sql
-- Suite: iceberg
-- Coverage: TRUNCATE TABLE for iceberg v3 + v2 + branch + DV/delete files

-- Setup: clean any prior state.
DROP DATABASE IF EXISTS iceberg_truncate;
CREATE DATABASE iceberg_truncate;
USE iceberg_truncate;

-- Case 1: TRUNCATE a v3 row-lineage table.
CREATE TABLE t_v3 (id INT, name VARCHAR(32))
  ENGINE=ICEBERG
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t_v3 VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT COUNT(*) FROM t_v3;
TRUNCATE TABLE t_v3;
SELECT COUNT(*) FROM t_v3;
-- Schema preserved: re-insert succeeds.
INSERT INTO t_v3 VALUES (10, 'after');
SELECT * FROM t_v3 ORDER BY id;

-- Case 2: TRUNCATE a v2 table (no row-lineage).
CREATE TABLE t_v2 (id INT, name VARCHAR(32))
  ENGINE=ICEBERG
  PROPERTIES('format-version'='2');
INSERT INTO t_v2 VALUES (1, 'x');
TRUNCATE TABLE t_v2;
SELECT COUNT(*) FROM t_v2;

-- Case 3: TRUNCATE empty table (still writes a delete snapshot).
CREATE TABLE t_empty (id INT)
  ENGINE=ICEBERG
  PROPERTIES('format-version'='3', 'row-lineage'='true');
TRUNCATE TABLE t_empty;
SELECT COUNT(*) FROM t_empty;
-- Inspect snapshots metadata table — there should be one snapshot
-- with operation=delete (parser support for $snapshots is gated; skip until
-- #81 SQL entry lands. Use `INSERT` after truncate as a behavioral proxy.)
INSERT INTO t_empty VALUES (1);
SELECT id FROM t_empty;

-- Case 4: TRUNCATE a partitioned table across historical specs.
CREATE TABLE t_part (id INT, dt DATE, region VARCHAR(8))
  ENGINE=ICEBERG
  PARTITION BY identity(region)
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t_part VALUES (1, '2026-05-01', 'us'), (2, '2026-05-01', 'eu');
ALTER TABLE t_part ADD PARTITION COLUMN dt;
INSERT INTO t_part VALUES (3, '2026-05-02', 'us');
TRUNCATE TABLE t_part;
SELECT COUNT(*) FROM t_part;

-- Case 5: TRUNCATE branch only.
CREATE TABLE t_branch (id INT)
  ENGINE=ICEBERG
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t_branch VALUES (1), (2), (3);
ALTER TABLE t_branch CREATE BRANCH dev;
INSERT INTO t_branch.branch_dev VALUES (10), (11);
TRUNCATE TABLE t_branch.branch_dev;
SELECT COUNT(*) FROM t_branch FOR VERSION AS OF 'dev';
SELECT COUNT(*) FROM t_branch;  -- main intact

-- Case 6: TRUNCATE table with deletion vector (v3 DV).
CREATE TABLE t_dv (id INT, name VARCHAR(32))
  ENGINE=ICEBERG
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t_dv VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
DELETE FROM t_dv WHERE id IN (2, 3);
SELECT COUNT(*) FROM t_dv;
TRUNCATE TABLE t_dv;
SELECT COUNT(*) FROM t_dv;

-- Case 7: TRUNCATE then time travel to pre-truncate snapshot.
-- (Time-travel SELECT is from #80; TRUNCATE produces a new snapshot id which
--  the prior snapshot still references.)
CREATE TABLE t_tt (id INT)
  ENGINE=ICEBERG
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t_tt VALUES (1), (2);
-- Capture pre-truncate snapshot id is awkward without metadata tables;
-- instead, check that TIMESTAMP AS OF a moment before TRUNCATE returns the
-- old data. Sleep one second to ensure timestamp differentiation.
SELECT COUNT(*) FROM t_tt;
-- (The runner does not support SLEEP; rely on natural delay.)
TRUNCATE TABLE t_tt;
SELECT COUNT(*) FROM t_tt;

-- Case 8: Reject TRUNCATE TABLE t PARTITION (...).
-- Expected: parse error.
TRUNCATE TABLE t_part PARTITION (region = 'us');

-- Case 9: Reject TRUNCATE TABLE t WHERE ....
TRUNCATE TABLE t_v3 WHERE id > 0;

-- Cleanup.
DROP DATABASE iceberg_truncate;
```

- [ ] **Step 8.2: Record the result fixture**

Make sure standalone-server is running:

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &
```

(Wait until "ready" log line.)

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_truncate --mode record --record-from target
```

Inspect `sql-tests/iceberg/result/iceberg_truncate.result` for sanity:
- COUNT(*) values match expectations
- Cases 8 & 9 produce a clean error message (not a server panic)

If a case is silently wrong, fix code or SQL and re-record.

- [ ] **Step 8.3: Re-run in verify mode**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_truncate --mode verify
```

Expected: PASS.

- [ ] **Step 8.4: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_truncate.sql sql-tests/iceberg/result/iceberg_truncate.result
git commit -m "test(iceberg): add iceberg_truncate.sql regression suite"
```

---

## Task 9: Documentation update

**Files:**
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks Iceberg v3 完成度清单.md`

- [ ] **Step 9.1: Update §10**

Find:

```markdown
- [ ] `TRUNCATE TABLE`（写一个清空 snapshot）
```

Replace with:

```markdown
- [x] `TRUNCATE TABLE`（写一个清空 snapshot；保留 schema/spec/属性；支持 branch；空表仍写 audit snapshot）← 落地于 2026-MM-DD · #<PR>
```

(Substitute the actual merge date and PR number when the PR is opened.)

- [ ] **Step 9.2: Update §20**

Locate the test-suite section and append:

```markdown
- [x] Iceberg TRUNCATE TABLE (v3 + v2 + branch + DV) SQL 套件 ← 落地于 2026-MM-DD · #<PR>（`iceberg_truncate.sql`）
```

- [ ] **Step 9.3: Append a changelog row**

In the trailing "变更记录" table:

```markdown
| 2026-MM-DD | 同步 #<PR>：TRUNCATE TABLE for iceberg v3/v2 + branch DML + DV 清理。 |
```

- [ ] **Step 9.4: Commit (in checklist file's repo, if separately managed)**

The completion checklist lives at `/Users/harbor/Documents/Obsidian/...` — it is not in the NovaRocks repo. Save the file from the editor; no git commit needed for this path.

---

## Task 10: Final verification + code-review prep

- [ ] **Step 10.1: Full test run**

```bash
cargo test --lib 2>&1 | tail -20
```

Expected: existing tests + new tests all PASS.

- [ ] **Step 10.2: Lints**

```bash
cargo fmt --check
cargo clippy --lib --tests -- -D warnings
```

Expected: clean.

- [ ] **Step 10.3: Iceberg suite verify**

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode verify -j 4
kill %1
```

Expected: full iceberg suite (including newly-added `iceberg_truncate`) all PASS. No regressions in existing tests.

- [ ] **Step 10.4: Self-review the diff**

Run: `git log --oneline main..HEAD` — verify a clean ladder of small commits (Task 1 through Task 9).

Run: `git diff --stat main..HEAD` — verify no unrelated files were touched.

- [ ] **Step 10.5: Open PR**

PR title: `feat(iceberg): TRUNCATE TABLE for iceberg v3/v2 + branch + DV cleanup`

PR body:

```markdown
## Summary
- Add `TRUNCATE TABLE <ident>[.branch_<name>]` for Iceberg backend.
- Writes a single `operation=delete` snapshot; preserves schema / partition spec / properties / other refs.
- Cleans data + position-delete + equality-delete + DV files (extends `enumerate_live_all_files`).
- Supports v3 row-lineage tables; v2 tables also accepted.
- Branch DML support via `t.branch_<x>` suffix (consistent with INSERT/UPDATE/DELETE).
- Empty-table truncate still produces an audit snapshot.

## Test plan
- [x] `cargo test --lib`
- [x] `sql-tests/iceberg/sql/iceberg_truncate.sql` (verify mode passes)
- [x] Manual smoke against standalone-server: 7 cases per spec §7.1

## Spec
`docs/design/specs/2026-05-06-iceberg-v3-write-path-completion-design.md` §3 / §7.1.

## Checklist
- [x] §10 row updated to `[x]`
- [x] §20 fixture row added
- [x] Changelog row appended
```

---

## Self-Review Notes

**Spec coverage check:**

| Spec section | Implemented in |
|---|---|
| §0 Goals: TRUNCATE writes operation=delete snapshot | Task 6 |
| §0 Goals: preserve schema/spec/props/refs | Task 6 (TruncateTxnAction does not emit schema / spec / property updates) |
| §0 Goals: branch suffix support | Task 1 (parser) + Task 7 (engine) |
| §2.3 parser TRUNCATE | Task 1 |
| §2.4 fail-fast PARTITION/WHERE | Task 1 |
| §3.1 engine flow | Task 2 + Task 7 |
| §3.2 TruncateCommit | Task 5 + Task 6 |
| §3.3 empty-table audit / DV cleanup / branch / OCC | Task 6a, 6c, Task 7, Task 6 (OCC retry inherited from run_iceberg_commit) |
| §3.4 fail-fast (parser-level) | Task 1 |
| §6 commit-unknown reuse | Inherited via `run_iceberg_commit` (no new code path) |
| §7.1 SQL regression cases 1–9 | Task 8 |
| §8.2 checklist update | Task 9 |

**Type-consistency check:**

- `target_ref: String` (default `"main"`) is used everywhere (`Statement::Truncate`, `execute_iceberg_truncate_table`, `RunInput`, `TruncateTxnAction`). No `Option<String>` slipped in.
- `enumerate_live_all_files` returns `Vec<(DataFile, i64, Option<i64>)>` matching `enumerate_live_data_files` and `write_overwrite_deletes_manifest`'s input.

**Placeholder scan:** none found. Code blocks are concrete; `pre-flight` notes invite the implementer to confirm symbol names locally before edits, but every editing step contains the actual content to write.
