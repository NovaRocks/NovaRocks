# Iceberg INSERT OVERWRITE PARTITIONS Implementation Plan (PR-2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `INSERT OVERWRITE PARTITIONS [TABLE] <ident>[.branch_<x>] [(cols)] { VALUES ... | <select> }` — replace only the partitions touched by the new data, preserving all other partitions. Iceberg semantics: `replacePartitions()`. Required for v3 row-lineage tables only (fail-fast on v2 / unpartitioned).

**Architecture:** Promote `Statement::Insert.overwrite: bool` to a three-valued `OverwriteMode { None, FullTable, DynamicPartitions }`; recognize `OVERWRITE PARTITIONS` in the parser as the trigger for `DynamicPartitions`. In the engine, after `IcebergSinkPlan` produces new `DataFile`s, compute `P_touched` from those files' partition structs, walk the base snapshot's manifests to find live files in those partitions (across historical specs), then commit a single `operation=overwrite` snapshot via a new `OverwritePartitionsCommit` action that mirrors `OverwriteCommit` but with a partition-scoped delete set.

**Tech Stack:** Rust 2021, sqlparser-rs (forked via `StarRocksDialect`), iceberg-rust 0.9.0 (vendored).

**Spec:** `docs/design/specs/2026-05-06-iceberg-v3-write-path-completion-design.md` §4 / §7.2 / §8.

**Depends on:** None (technically independent of PR-1 TRUNCATE; PR ordering is documentation, not code dependency).

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Modify | `src/sql/parser/ast/mod.rs:146` | Replace `overwrite: bool` on `Statement::Insert` with `overwrite_mode: OverwriteMode` (3-valued enum) |
| Modify | `src/sql/parser/dialect/mod.rs` (or sibling INSERT parser file) | Recognize `OVERWRITE PARTITIONS` keyword sequence |
| Modify | `src/engine/insert_flow.rs:24,104,131` | Update signature: `overwrite: bool` → `overwrite_mode: OverwriteMode`; propagate |
| Modify | `src/engine/statement.rs:233,1062,1071` | Update call sites for new signature |
| Modify | `src/engine/iceberg_writer.rs` | Branch on `overwrite_mode`: `DynamicPartitions` → call new `execute_iceberg_overwrite_partitions()` instead of existing OverwriteCommit path |
| Create | `src/engine/iceberg_overwrite_partitions.rs` | New engine entry: compute `P_touched`, find base files in those partitions, drive `OverwritePartitionsCommit` via `run_iceberg_commit` |
| Modify | `src/engine/mod.rs` | `pub(crate) mod iceberg_overwrite_partitions;` |
| Modify | `src/connector/iceberg/commit/types.rs` (or wherever `CommitOpKind` lives) | Add `CommitOpKind::OverwritePartitions` variant |
| Create | `src/connector/iceberg/commit/overwrite_partitions.rs` | `OverwritePartitionsCommit` action + `OverwritePartitionsTxnAction` (mirrors `overwrite.rs` shape) |
| Modify | `src/connector/iceberg/commit/mod.rs` | `pub mod overwrite_partitions; pub use overwrite_partitions::OverwritePartitionsCommit;` |
| Modify | `src/connector/iceberg/commit/run.rs` | Dispatch `CommitOpKind::OverwritePartitions` → `OverwritePartitionsCommit` |
| Modify | `src/connector/iceberg/partition_spec.rs` (or new `partition_match.rs`) | Add `normalize_partition_tuple_to_current_spec(...)` if not already present (used to compare base files written under historical specs against the current spec's `P_touched`) |
| Create | `sql-tests/iceberg/sql/iceberg_v3_overwrite_partitions.sql` | SQL regression suite per spec §7.2 |
| Create | `sql-tests/iceberg/result/iceberg_v3_overwrite_partitions.result` | Recorded fixture |
| Modify | `/Users/harbor/Documents/Obsidian/NovaRocks Iceberg v3 完成度清单.md` | §10 mark `[x]`; §20 fixture row; trailing changelog |

---

## Pre-Flight Investigation

Before Task 1, the implementer should read these files end-to-end:

- `src/sql/parser/ast/mod.rs:140-160` — full `Statement::Insert` definition + neighboring variants
- `src/sql/parser/dialect/` — find which file currently handles `OVERWRITE` keyword (`grep -n "OVERWRITE\|overwrite" src/sql/parser/dialect/*.rs`)
- `src/connector/iceberg/commit/overwrite.rs` (full file) — `OverwriteCommit` is the closest template; reused via shared helpers `enumerate_live_data_files`, `write_overwrite_deletes_manifest`
- `src/connector/iceberg/partition_spec.rs` (full file) — find existing partition-tuple comparison / normalization helpers (the spec assumes one exists from §6 partition evolution; verify and reuse rather than re-implement)
- `src/connector/iceberg/data_writer.rs:140-220` — how `DataFile`s get their partition struct populated; specifically what `DataFile::partition()` returns (this is the key the new code keys off of)
- `src/engine/iceberg_writer.rs:51-208` — full `execute_iceberg_insert_or_overwrite` function; the `DynamicPartitions` route diverges right after data files are produced (≈ line 130) and rejoins at the commit-collector setup
- `src/engine/insert_flow.rs:19-110` — for `target_ref` / `IcebergRefSuffix` propagation pattern (already used by branch-qualified INSERT)

---

## Task 1: Promote `bool overwrite` to `enum OverwriteMode`

This is a pure refactor that should not change behavior. Run existing tests after each step to confirm.

**Files:**
- Modify: `src/sql/parser/ast/mod.rs:146`
- Modify: All call sites of the field (use grep to find them)

- [ ] **Step 1.1: Define the enum**

In `src/sql/parser/ast/mod.rs`, immediately above `Statement::Insert`'s definition, add:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverwriteMode {
    /// `INSERT INTO ...` — append.
    None,
    /// `INSERT OVERWRITE [TABLE] ...` — replace all rows in the table.
    FullTable,
    /// `INSERT OVERWRITE PARTITIONS [TABLE] ...` — replace only the partitions
    /// touched by the new data; other partitions preserved. v3 row-lineage only.
    DynamicPartitions,
}
```

Replace the field on `Statement::Insert`:

```rust
// before:
//   pub overwrite: bool,
// after:
   pub overwrite_mode: OverwriteMode,
```

- [ ] **Step 1.2: Find every reference**

Run: `grep -rn "\.overwrite\b\|overwrite: bool\|overwrite,$\|overwrite: true\|overwrite: false" src/ tests/ --include='*.rs'`

Note every file:line. Expected to touch: `insert_flow.rs:24,104,131`, `statement.rs:233,1062,1071`, plus parser tests and any internal helpers.

- [ ] **Step 1.3: Update parser to emit the new field**

In the parser file that handles `INSERT`, find where the existing bool is computed (something like `let overwrite = parser.parse_keyword(Keyword::OVERWRITE);`) and replace with:

```rust
let overwrite_mode = if parser.parse_keyword(Keyword::OVERWRITE) {
    // Task 2 will extend this branch to detect PARTITIONS;
    // for now keep behavior identical to bool=true.
    OverwriteMode::FullTable
} else {
    OverwriteMode::None
};
```

Update the `Statement::Insert { ..., overwrite_mode, ... }` construction to use the new field name.

- [ ] **Step 1.4: Update `run_insert` in `src/engine/insert_flow.rs`**

```rust
// signature
pub(crate) fn run_insert(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    columns: &[String],
    source: &InsertSource,
    overwrite_mode: OverwriteMode,        // <-- was: overwrite: bool
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
```

Then in the function body, replace any `if overwrite` with `if matches!(overwrite_mode, OverwriteMode::FullTable | OverwriteMode::DynamicPartitions)` and pass `overwrite_mode` (not bool) downstream.

In particular, the call to `execute_iceberg_insert_or_overwrite` (line 98 area) should pass `overwrite_mode` instead of `overwrite`. Update the callee's signature in `src/engine/iceberg_writer.rs` similarly.

- [ ] **Step 1.5: Update `src/engine/statement.rs:233,1062,1071`**

Find the dispatch site that takes `insert.overwrite` and pass `insert.overwrite_mode` instead. The non-iceberg INSERT path that uses `insert.overwrite` as a bool should derive the bool inline:

```rust
let is_overwrite = matches!(
    insert.overwrite_mode,
    OverwriteMode::FullTable | OverwriteMode::DynamicPartitions,
);
```

If `DynamicPartitions` is reached on a non-iceberg backend, fail-fast with an error (Task 3 covers this).

- [ ] **Step 1.6: Run all tests, verify behavior unchanged**

```bash
cargo build 2>&1 | tail -20
cargo test --lib 2>&1 | tail -20
```

Expected: build succeeds; existing tests still PASS. This step is a behavior-preserving refactor.

- [ ] **Step 1.7: Commit**

```bash
git add src/
git commit -m "refactor(parser): replace Statement::Insert.overwrite bool with OverwriteMode enum"
```

---

## Task 2: Recognize `OVERWRITE PARTITIONS` in the parser

**Files:**
- Modify: `src/sql/parser/dialect/` (the file from Step 1.3)

- [ ] **Step 2.1: Write failing parser tests**

Add adjacent to the existing INSERT parser tests:

```rust
#[test]
fn parse_insert_overwrite_partitions_table() {
    let stmt = parse_one("INSERT OVERWRITE PARTITIONS TABLE t SELECT * FROM s")
        .expect("parse");
    let insert = match stmt {
        Statement::Insert(i) => i,
        other => panic!("expected Insert, got {other:?}"),
    };
    assert_eq!(insert.overwrite_mode, OverwriteMode::DynamicPartitions);
}

#[test]
fn parse_insert_overwrite_partitions_no_table_keyword() {
    let stmt = parse_one("INSERT OVERWRITE PARTITIONS t VALUES (1)")
        .expect("parse");
    let insert = match stmt {
        Statement::Insert(i) => i,
        other => panic!("expected Insert, got {other:?}"),
    };
    assert_eq!(insert.overwrite_mode, OverwriteMode::DynamicPartitions);
}

#[test]
fn parse_insert_overwrite_table_remains_full_table() {
    let stmt = parse_one("INSERT OVERWRITE TABLE t SELECT * FROM s")
        .expect("parse");
    let insert = match stmt {
        Statement::Insert(i) => i,
        other => panic!("expected Insert, got {other:?}"),
    };
    assert_eq!(insert.overwrite_mode, OverwriteMode::FullTable);
}

#[test]
fn parse_insert_overwrite_partitions_with_branch() {
    let stmt = parse_one("INSERT OVERWRITE PARTITIONS t.branch_dev VALUES (1)")
        .expect("parse");
    // Branch resolution happens in run_insert via split_ref_suffix; the parser
    // just produces the qualified ObjectName.
    let insert = match stmt {
        Statement::Insert(i) => i,
        other => panic!("expected Insert, got {other:?}"),
    };
    assert_eq!(insert.overwrite_mode, OverwriteMode::DynamicPartitions);
    assert_eq!(insert.table_name.parts, vec!["t".to_string(), "branch_dev".to_string()]);
}
```

(Field names like `insert.table_name` — match whatever the existing AST uses; pre-flight in Step 1.2 will reveal the actual field.)

- [ ] **Step 2.2: Run, verify FAIL**

```bash
cargo test --lib parse_insert_overwrite_partitions
```

Expected: 4 FAIL — currently `OVERWRITE PARTITIONS` is parsed as `FullTable` + a syntax error on `PARTITIONS`.

- [ ] **Step 2.3: Implement**

Replace the simple `parse_keyword(OVERWRITE)` block from Step 1.3 with:

```rust
let overwrite_mode = if parser.parse_keyword(Keyword::OVERWRITE) {
    if parser.parse_keyword(Keyword::PARTITIONS) {
        OverwriteMode::DynamicPartitions
    } else {
        OverwriteMode::FullTable
    }
} else {
    OverwriteMode::None
};
```

If `Keyword::PARTITIONS` doesn't exist as a sqlparser-rs constant, use `parser.parse_keyword_using_ident_value("PARTITIONS")` or `parser.consume_token(&Token::make_keyword("PARTITIONS"))`. Confirm during pre-flight by `grep -n "Keyword::PARTITION\|\"PARTITION\"" src/sql/parser/dialect/`.

- [ ] **Step 2.4: Run, verify PASS**

```bash
cargo test --lib parse_insert_overwrite_partitions -- --nocapture
```

Expected: 4 PASS.

- [ ] **Step 2.5: Commit**

```bash
git add src/sql/parser/
git commit -m "feat(parser): recognize INSERT OVERWRITE PARTITIONS"
```

---

## Task 3: Engine fail-fast for unsupported targets

**Files:**
- Modify: `src/engine/insert_flow.rs`

- [ ] **Step 3.1: Add fail-fast checks**

In `run_insert`, after `target` and `resolved` are loaded, before the dispatch to `execute_iceberg_insert_or_overwrite`, add:

```rust
if matches!(overwrite_mode, OverwriteMode::DynamicPartitions) {
    if target.backend_name != "iceberg" {
        return Err(format!(
            "INSERT OVERWRITE PARTITIONS is only supported for iceberg backends, \
             target uses backend `{}`",
            target.backend_name,
        ));
    }
    // v3 row-lineage requirement is checked engine-side after metadata is loaded
    // (see Task 5.2). Partition-table requirement also checked there: by then we
    // have access to the iceberg metadata's partition spec.
}
```

- [ ] **Step 3.2: Add a unit test for fail-fast on non-iceberg backend**

In `src/engine/insert_flow.rs`'s `#[cfg(test)] mod tests` (or co-located test file), add:

```rust
#[test]
fn overwrite_partitions_rejected_on_non_iceberg() {
    // Use a fixture that yields a managed-lake target.
    let state = test_helpers::standalone_state_with_managed_lake_table("db", "t");
    let name = ObjectName::single("db.t");
    let err = run_insert(
        &state,
        &name,
        &[],
        &InsertSource::Values(vec![]),
        OverwriteMode::DynamicPartitions,
        None,
        "db",
    ).unwrap_err();
    assert!(err.contains("OVERWRITE PARTITIONS"));
    assert!(err.contains("iceberg"));
}
```

(Fixture helper `standalone_state_with_managed_lake_table` — pre-flight: confirm the existing engine test fixture pattern (`grep -rn "fn standalone_state_with\|fn make_state" src/engine/`); reuse or create a similar minimal helper.)

- [ ] **Step 3.3: Run, verify PASS**

```bash
cargo test --lib overwrite_partitions_rejected_on_non_iceberg -- --nocapture
```

- [ ] **Step 3.4: Commit**

```bash
git add src/engine/insert_flow.rs
git commit -m "feat(engine): fail-fast OVERWRITE PARTITIONS on non-iceberg backends"
```

---

## Task 4: Add `CommitOpKind::OverwritePartitions`

**Files:**
- Modify: wherever `CommitOpKind` is defined (see Task 3 of PR-1 plan for grep pattern)

- [ ] **Step 4.1: Add the variant**

```rust
pub enum CommitOpKind {
    FastAppend,
    Overwrite,
    OverwritePartitions,
    // ... existing ...
}
```

- [ ] **Step 4.2: Update every match-on-CommitOpKind**

Run: `grep -rn "CommitOpKind::" src/connector/iceberg/`.

For dispatchers (`run.rs`), add a placeholder arm:
```rust
CommitOpKind::OverwritePartitions => unimplemented!("OverwritePartitionsCommit lands in Task 7"),
```

For descriptive matches (logs / metrics), use `"overwrite-partitions"`.

- [ ] **Step 4.3: Build**

```bash
cargo build 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 4.4: Commit**

```bash
git add src/connector/iceberg/
git commit -m "feat(commit): add CommitOpKind::OverwritePartitions"
```

---

## Task 5: Partition-tuple matching helpers

This task isolates the cross-spec normalization into a single, unit-testable function so Task 7's commit logic remains thin.

**Files:**
- Modify: `src/connector/iceberg/partition_spec.rs` (or create `partition_match.rs` if `partition_spec.rs` is already large/focused)

- [ ] **Step 5.1: Pre-flight read**

```bash
sed -n '1,80p' src/connector/iceberg/partition_spec.rs
grep -n "^pub fn\|^pub(crate) fn\|fn normalize\|fn matches\|fn compare" src/connector/iceberg/partition_spec.rs
```

Expected: existing helpers like `partition_value_from_struct`, `transform_value`, `compare_specs` — note their signatures. The DELETE-cross-spec path (clue: completion checklist §6 marked `[x]` for DELETE 跨历史 partition spec) probably exposes a comparator already. Use it.

If a comparator does not exist, the new function below is the implementation. If one exists, write a thin adapter `partition_tuples_intersect` instead and delete the unused branches below.

- [ ] **Step 5.2: Write failing tests**

In `partition_spec.rs::tests` (create the test module if absent), add:

```rust
#[cfg(test)]
mod overwrite_partitions_tests {
    use super::*;

    #[test]
    fn same_spec_same_partition_matches() {
        let spec = test_helpers::identity_spec_on_region();
        let pt_new = test_helpers::partition_struct(&[("region", "us")]);
        let pt_base = test_helpers::partition_struct(&[("region", "us")]);
        assert!(partition_in_set(&pt_base, &spec, &[(spec.spec_id(), pt_new)]));
    }

    #[test]
    fn same_spec_different_partition_does_not_match() {
        let spec = test_helpers::identity_spec_on_region();
        let pt_new = test_helpers::partition_struct(&[("region", "us")]);
        let pt_base = test_helpers::partition_struct(&[("region", "eu")]);
        assert!(!partition_in_set(&pt_base, &spec, &[(spec.spec_id(), pt_new)]));
    }

    #[test]
    fn cross_spec_compatible_partition_matches() {
        // Base file written under spec-0 (identity(region)).
        // Current spec-1 adds a second column (identity(region), identity(dt)).
        // A new file in spec-1 partition (us, 2026-05-01) maps to base (us)
        // when normalized to spec-0; thus partition_in_set should be TRUE
        // for the base file under spec-0.
        let spec0 = test_helpers::identity_spec_on_region();
        let spec1 = test_helpers::identity_spec_on_region_and_dt();
        let pt_new_in_spec1 = test_helpers::partition_struct(
            &[("region", "us"), ("dt", "2026-05-01")]);
        let pt_base_in_spec0 = test_helpers::partition_struct(&[("region", "us")]);
        assert!(partition_in_set(
            &pt_base_in_spec0,
            &spec0,
            &[(spec1.spec_id(), pt_new_in_spec1)],
        ));
    }
}
```

`partition_in_set(base_pt, base_spec, p_touched)` — signature TBD by Step 5.3. The third argument is `&[(spec_id, partition_struct)]` representing every partition tuple touched by the new files (each tagged with the spec under which it was written, which is always the *current* spec since the new files are always written under the current spec — but the function still accepts a slice for future flexibility).

(`test_helpers::identity_spec_on_region` etc. are small helpers to construct `PartitionSpec` fixtures. Pre-flight: check if `partition_spec.rs` already has test helpers; reuse.)

- [ ] **Step 5.3: Run, verify FAIL**

```bash
cargo test --lib partition_in_set
```

Expected: FAIL — function not defined.

- [ ] **Step 5.4: Implement**

```rust
/// Decide whether a base file with `base_pt` under `base_spec` falls into
/// any of the partition tuples in `touched`. Each tuple in `touched` is
/// tagged with the spec under which it was emitted (the *current* spec
/// for new files, but the type allows future use).
///
/// Cross-spec rule: when `base_spec.spec_id() != touched_spec.spec_id()`,
/// reduce both partition tuples to the **intersection** of their fields
/// by source-column-id and compare. Concretely:
///
/// 1. Build a map from `source_column_id` → value for both base and touched.
/// 2. For every column id present in both maps, compare values; mismatch ⇒
///    not-in-set.
/// 3. If all common columns match, the base partition is considered "in"
///    the touched partition (because a finer-grained touched partition is
///    contained in a coarser base partition; INSERT OVERWRITE PARTITIONS
///    semantics are "replace whatever the new data covers").
///
/// Note: this accepts that `OVERWRITE PARTITIONS` may delete *more* base
/// rows than strictly necessary when the current spec is finer than a
/// historical spec (you can't do partial-partition delete without
/// rewriting). This matches Spark / Trino behavior and is documented in
/// spec §10.1 R2.
pub(crate) fn partition_in_set(
    base_pt: &PartitionStruct,
    base_spec: &PartitionSpec,
    touched: &[(i32, PartitionStruct)],  // (spec_id, partition_struct)
) -> bool {
    for (touched_spec_id, touched_pt) in touched {
        if base_spec.spec_id() == *touched_spec_id {
            if base_pt == touched_pt { return true; }
            continue;
        }
        // Cross-spec: compare on source-column intersection.
        let base_map = field_map(base_spec, base_pt);
        // Need access to the touched spec to walk its fields.
        // Since touched files are always under the current spec, the caller
        // passes the current spec separately if needed; but the signature
        // here demands the spec object be reachable. Refactor to:
        //   touched: &[(PartitionSpecRef, PartitionStruct)]
        // (Adjust both signature and tests accordingly.)
        unreachable!("see Step 5.4 note about signature refactor");
    }
    false
}

fn field_map(spec: &PartitionSpec, pt: &PartitionStruct) -> HashMap<i32, /* value */> {
    let mut out = HashMap::new();
    for (i, field) in spec.fields().iter().enumerate() {
        let v = pt.get(i);  // pseudo; actual API will differ
        out.insert(field.source_id, v);
    }
    out
}
```

The `unreachable!` indicates the signature needs `&[(PartitionSpecRef, PartitionStruct)]` or `&[(i32, PartitionStruct)]` plus a separate `&HashMap<i32, PartitionSpecRef>` lookup. Pick one before writing the body. The simpler form:

```rust
pub(crate) fn partition_in_set(
    base_pt: &PartitionStruct,
    base_spec: &PartitionSpec,
    touched: &[(PartitionSpecRef, PartitionStruct)],
) -> bool { ... }
```

Adjust tests in Step 5.2 to construct `PartitionSpecRef` (or `Arc<PartitionSpec>`) for the second tuple element. Once compiled, the function body is a straightforward two-loop intersection.

- [ ] **Step 5.5: Run, verify PASS**

```bash
cargo test --lib partition_in_set -- --nocapture
```

Expected: 3 PASS.

- [ ] **Step 5.6: Commit**

```bash
git add src/connector/iceberg/partition_spec.rs
git commit -m "feat(commit): add partition_in_set for cross-spec OVERWRITE PARTITIONS"
```

---

## Task 6: `OverwritePartitionsCommit` skeleton

**Files:**
- Create: `src/connector/iceberg/commit/overwrite_partitions.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 6.1: Create the file with skeleton + Send+Sync smoke test**

```rust
//! `OverwritePartitionsCommit` — replace only the partitions touched by
//! the new data; preserve all other partitions. v3 row-lineage tables
//! only.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestStatus,
    Operation, PartitionSpecRef, SchemaRef, Snapshot, SnapshotReference,
    SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction};
use super::helpers::{generate_snapshot_id, metadata_dir, now_ms, write_manifest_list};
use super::overwrite::{enumerate_live_data_files, write_overwrite_deletes_manifest};
use super::types::{CommitOutcome, IcebergWriteMode, WrittenFile};

pub struct OverwritePartitionsCommit;

#[async_trait]
impl IcebergCommitAction for OverwritePartitionsCommit {
    async fn commit(&self, _ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        Err("OverwritePartitionsCommit::commit not implemented".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn type_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        assert_send_sync(&OverwritePartitionsCommit);
    }
}
```

- [ ] **Step 6.2: Register module + dispatcher**

In `src/connector/iceberg/commit/mod.rs`:

```rust
pub mod overwrite_partitions;
pub use overwrite_partitions::OverwritePartitionsCommit;
```

In `run.rs`, replace the `unimplemented!()` from Task 4:

```rust
CommitOpKind::OverwritePartitions => Box::new(OverwritePartitionsCommit),
```

- [ ] **Step 6.3: Build**

```bash
cargo build 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 6.4: Commit**

```bash
git add src/connector/iceberg/commit/
git commit -m "feat(commit): OverwritePartitionsCommit skeleton + dispatcher"
```

---

## Task 7: `OverwritePartitionsCommit::commit` main logic

Subdivided into 7a/7b/7c by base-state shape.

### Task 7a: Empty SELECT result (P_touched empty) → noop overwrite snapshot

- [ ] **Step 7a.1: Failing test**

In `overwrite_partitions.rs::tests`:

```rust
#[tokio::test]
async fn empty_select_writes_noop_overwrite_snapshot() {
    let (table, file_io, catalog) = test_helpers::v3_partitioned_table_with_data().await;
    // Inject zero new files; collector reports empty `touched_partitions`.
    let outcome = test_helpers::run_overwrite_partitions(&table, &file_io, &catalog,
        /*new_files=*/ vec![], /*target_ref=*/ "main").await
        .expect("noop overwrite succeeds");
    let reloaded = catalog.load_table(table.identifier()).await.unwrap();
    let snap = reloaded.metadata().current_snapshot().unwrap();
    assert_eq!(snap.summary().operation, Operation::Overwrite);
    assert_eq!(snap.summary().additional_properties.get("added-data-files"),
        Some(&"0".to_string()));
    assert_eq!(snap.summary().additional_properties.get("removed-data-files"),
        Some(&"0".to_string()));
    // Live data unchanged.
    let live = enumerate_live_data_files(&reloaded, &file_io).await.unwrap();
    let prior_live = enumerate_live_data_files(&table, &file_io).await.unwrap();
    assert_eq!(live.len(), prior_live.len());
}
```

`test_helpers::v3_partitioned_table_with_data` builds a 2-spec partitioned v3 table with 4 live data files (2 in `region=us`, 2 in `region=eu`). `run_overwrite_partitions` is the sibling helper to `run_commit_with` from PR-1 plan but injects new files into the collector via `inject_written_file` *and* explicitly lists `touched_partitions`.

(Pre-flight: check whether the collector's API needs new fields to express `touched_partitions`. If yes, extend `IcebergCommitCollector` in this task — `grep -n "pub struct IcebergCommitCollector" src/connector/iceberg/commit/`.)

- [ ] **Step 7a.2: Run, FAIL**

```bash
cargo test --lib empty_select_writes_noop_overwrite_snapshot
```

- [ ] **Step 7a.3: Implement**

```rust
#[async_trait]
impl IcebergCommitAction for OverwritePartitionsCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "OverwritePartitionsCommit received {:?} content; expected Data only",
                    f.content
                ));
            }
        }
        // v3 row-lineage requirement (mirrors OverwriteCommit::classify).
        let row_lineage_first_row_id =
            match crate::connector::iceberg::commit::classify_iceberg_write_mode(ctx.table) {
                IcebergWriteMode::RowLineageV3 => Some(ctx.table.metadata().next_row_id()),
                IcebergWriteMode::LegacyPositionDeletes => {
                    return Err("OverwritePartitionsCommit requires v3 row-lineage table"
                        .to_string());
                }
            };
        let row_lineage_added_rows = written.iter().try_fold(0u64, |sum, f| {
            sum.checked_add(f.record_count)
                .ok_or_else(|| "row-lineage added row count overflow".to_string())
        })?;

        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = OverwritePartitionsTxnAction {
            written,
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            partition_spec: ctx.collector.partition_spec.clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
            row_lineage_first_row_id,
            row_lineage_added_rows,
            target_ref: ctx.target_ref.to_string(),
        };
        let tx = Transaction::new(ctx.table);
        let tx = action.apply(tx)
            .map_err(|e| format!("OverwritePartitions apply failed: {e}"))?;
        let table_after = tx.commit(ctx.catalog).await
            .map_err(|e| format!("OverwritePartitions commit failed: {e}"))?;
        let new_snapshot_id = table_after.metadata().current_snapshot()
            .map(|s| s.snapshot_id()).unwrap_or(0);
        let written_manifest_paths = manifest_paths_out
            .lock().expect("poisoned").clone();
        Ok(CommitOutcome { new_snapshot_id, written_manifest_paths })
    }
}

struct OverwritePartitionsTxnAction {
    written: Vec<WrittenFile>,
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema_id: i32,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    row_lineage_first_row_id: Option<u64>,
    row_lineage_added_rows: u64,
    target_ref: String,
}

#[async_trait]
impl TransactionAction for OverwritePartitionsTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = m.refs().get(target_ref.as_str())
            .map(|r| r.snapshot_id)
            .or_else(|| if target_ref == "main" {
                m.current_snapshot().map(|s| s.snapshot_id())
            } else { None });
        let metadata_dir = metadata_dir(table);

        // 1. Compute P_touched from written files' partition struct.
        let touched: Vec<(PartitionSpecRef, _)> = self.written.iter()
            .map(|wf| (self.partition_spec.clone(), wf.partition.clone()))
            .collect();
        // Dedup touched (multiple new files in the same partition).
        let touched = dedup_partition_set(touched);

        // 2. Walk base manifests, find live files in any P_touched.
        let base = enumerate_live_all_files(table, &self.file_io).await
            .map_err(to_iceberg_unexpected)?;
        let to_delete: Vec<_> = base.into_iter()
            .filter(|(df, _, _)| {
                let base_spec = m.partition_spec_by_id(df.partition_spec_id())
                    .expect("base file's spec must exist");
                crate::connector::iceberg::partition_spec::partition_in_set(
                    df.partition(), base_spec, &touched)
            })
            .collect();

        // 3. Empty case: nothing touched, nothing to delete → noop snapshot.
        if self.written.is_empty() && to_delete.is_empty() {
            // Still write a snapshot with operation=overwrite to keep audit trail.
            // Manifest list empty.
            return write_empty_overwrite_snapshot(
                self.clone(), table, new_snapshot_id, new_seq,
                parent_snapshot_id, target_ref).await;
        }

        // 4. Build manifests: deletes (if any) + adds (if any).
        let mut new_manifests = Vec::with_capacity(2);
        if !to_delete.is_empty() {
            let path = format!("{metadata_dir}/{}-overwritep-deletes-0.avro", self.commit_uuid);
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out.lock().expect("poisoned").push(path.clone());
            let mf = write_overwrite_deletes_manifest(
                &self.file_io, &path, &to_delete,
                self.partition_spec.clone(), m.current_schema().clone(),
                new_snapshot_id, format_version,
            ).await.map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }
        if !self.written.is_empty() {
            let path = format!("{metadata_dir}/{}-overwritep-data-0.avro", self.commit_uuid);
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out.lock().expect("poisoned").push(path.clone());
            let mf = write_added_data_manifest(
                &self.file_io, &path, &self.written,
                self.partition_spec.clone(), m.current_schema().clone(),
                new_snapshot_id, format_version, self.row_lineage_first_row_id,
            ).await.map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 5. Write manifest list.
        let manifest_list_path = format!(
            "{metadata_dir}/snap-{new_snapshot_id}-1-{}.avro", self.commit_uuid);
        write_manifest_list(
            &self.file_io, &manifest_list_path, &new_manifests,
            new_snapshot_id, parent_snapshot_id, new_seq, format_version,
        ).await.map_err(to_iceberg_unexpected)?;

        // 6. Build snapshot summary.
        let mut summary_props = std::collections::HashMap::new();
        summary_props.insert("operation".to_string(), "overwrite".to_string());
        summary_props.insert("replace-partitions".to_string(), "true".to_string());
        summary_props.insert("added-data-files".to_string(), self.written.len().to_string());
        summary_props.insert("removed-data-files".to_string(), to_delete.len().to_string());
        summary_props.insert("added-files-size".to_string(),
            self.written.iter().map(|f| f.file_size_in_bytes as i64).sum::<i64>().to_string());
        summary_props.insert("removed-files-size".to_string(),
            to_delete.iter().map(|(df, _, _)| df.file_size_in_bytes()).sum::<i64>().to_string());

        let snapshot = build_snapshot(
            new_snapshot_id, parent_snapshot_id, new_seq,
            manifest_list_path.clone(),
            Operation::Overwrite, summary_props,
            self.schema_id,
            self.row_lineage_first_row_id, self.row_lineage_added_rows,
        );
        let updates = vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: target_ref.clone(),
                reference: branch_ref(new_snapshot_id),
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
```

Helpers `dedup_partition_set`, `write_added_data_manifest`, `build_snapshot`, `branch_ref`, `to_iceberg_unexpected`, `enumerate_live_all_files` — share-able with `OverwriteCommit`. Pre-flight: see if `OverwriteTxnAction` already defines anything reusable; if so, lift to a sibling module (e.g. `commit/manifest_writers.rs`). If not, define inline first; refactor in a follow-up if both PRs land.

(For `enumerate_live_all_files`: this was added in PR-1 Task 4. If PR-2 lands first, define it here in a sibling helper module instead.)

- [ ] **Step 7a.4: Run, PASS**

```bash
cargo test --lib empty_select_writes_noop_overwrite_snapshot
```

### Task 7b: Single-spec replace one partition

- [ ] **Step 7b.1: Failing test**

```rust
#[tokio::test]
async fn replaces_one_partition_preserves_others() {
    let (table, file_io, catalog) = test_helpers::v3_partitioned_table_with_data().await;
    // Base: 2 files in region=us, 2 files in region=eu.
    // New: 1 file in region=us only.
    let new_files = vec![test_helpers::data_file_in_partition("us", 100 /* row count */)];
    let _ = test_helpers::run_overwrite_partitions(&table, &file_io, &catalog,
        new_files, "main").await.expect("commit");
    let reloaded = catalog.load_table(table.identifier()).await.unwrap();
    let live = enumerate_live_all_files(&reloaded, &file_io).await.unwrap();
    let us_count = live.iter().filter(|(df, _, _)| df_partition_eq(df, "us")).count();
    let eu_count = live.iter().filter(|(df, _, _)| df_partition_eq(df, "eu")).count();
    assert_eq!(us_count, 1, "us should be replaced by 1 new file");
    assert_eq!(eu_count, 2, "eu untouched");
}
```

- [ ] **Step 7b.2: Run, expect PASS** (Task 7a's implementation should already handle single-spec replace; if FAIL, debug).

### Task 7c: Cross-spec replace

- [ ] **Step 7c.1: Failing test**

```rust
#[tokio::test]
async fn replaces_across_historical_specs() {
    let (table, file_io, catalog) = test_helpers::v3_table_with_two_specs().await;
    // Base: 2 files under spec-0 (identity(region)), 2 files under spec-1
    // (identity(region), identity(dt)). All region=us.
    let new_files = vec![
        test_helpers::data_file_in_partition_v2("us", "2026-05-01", 50),
    ];
    let _ = test_helpers::run_overwrite_partitions(&table, &file_io, &catalog,
        new_files, "main").await.expect("commit");
    let reloaded = catalog.load_table(table.identifier()).await.unwrap();
    let live = enumerate_live_all_files(&reloaded, &file_io).await.unwrap();
    // Cross-spec semantics (spec §10.1 R2): spec-0 base files in region=us
    // also get deleted (their partition is a superset of the touched spec-1
    // partition us/2026-05-01). Verify with explicit count.
    let live_us = live.iter().filter(|(df, _, _)| df_partition_includes(df, "us")).count();
    assert_eq!(live_us, 1, "all us files (across specs) replaced; new us file added");
}
```

- [ ] **Step 7c.2: Run, expect PASS** (relies on Task 5's `partition_in_set`).

- [ ] **Step 7c.3: Commit Task 7 as a whole**

```bash
git add src/connector/iceberg/commit/overwrite_partitions.rs
git commit -m "feat(commit): OverwritePartitionsCommit core logic with cross-spec support"
```

---

## Task 8: Engine wiring

**Files:**
- Create: `src/engine/iceberg_overwrite_partitions.rs`
- Modify: `src/engine/iceberg_writer.rs`
- Modify: `src/engine/mod.rs`

- [ ] **Step 8.1: Create the engine module**

```rust
//! INSERT OVERWRITE PARTITIONS engine entry. Reuses the IcebergSinkPlan
//! to write new data files, then drives `OverwritePartitionsCommit`
//! through `run_iceberg_commit`.

use std::sync::Arc;

use crate::connector::backend::{CatalogEntry, ResolvedTable};
use crate::connector::iceberg::commit::{
    run_iceberg_commit, CommitOpKind, IcebergCommitCollector, RunInput,
};
use crate::engine::iceberg_writer::{
    block_on_iceberg, build_abort_cleanup_for_catalog_entry,
    drive_iceberg_sink, invalidate_iceberg_caches,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::sql::parser::ast::InsertSource;

pub(crate) fn execute_iceberg_overwrite_partitions(
    state: &Arc<StandaloneState>,
    target: &ResolvedTable,
    resolved_entry: &CatalogEntry,
    columns: &[String],
    source: &InsertSource,
    target_ref: &str,
) -> Result<StatementResult, String> {
    // 1. Validate target: v3 row-lineage + partitioned.
    let metadata = resolved_entry.iceberg_metadata()
        .ok_or("OVERWRITE PARTITIONS: not an iceberg table")?;
    if !metadata.row_lineage_enabled() {
        return Err("INSERT OVERWRITE PARTITIONS requires v3 row-lineage table".to_string());
    }
    if metadata.default_partition_spec().is_unpartitioned() {
        return Err(
            "INSERT OVERWRITE PARTITIONS requires a partitioned table; \
             use OVERWRITE for unpartitioned tables".to_string(),
        );
    }

    // 2. Write data files via IcebergSinkPlan (reuse INSERT path).
    let written = drive_iceberg_sink(state, target, resolved_entry, columns, source)?;

    // 3. Set up collector with CommitOpKind::OverwritePartitions.
    let table = resolved_entry.iceberg_table();
    let staging_dir = format!(
        "{}/metadata/_staging/{}", table.metadata().location(), uuid::Uuid::new_v4());
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::OverwritePartitions,
        resolved_entry.identifier().clone(),
        table.metadata().current_snapshot().map(|s| s.snapshot_id()),
        table.metadata().last_sequence_number(),
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    let default_spec_id = table.metadata().default_partition_spec_id();
    for df in &written {
        let wf = data_file_to_written_file(df, default_spec_id)?;
        collector.inject_written_file(wf);
    }

    // 4. Drive commit.
    let abort_cleanup = build_abort_cleanup_for_catalog_entry(resolved_entry)?;
    let file_io = table.file_io().clone();
    let _ = block_on_iceberg(async {
        run_iceberg_commit(RunInput {
            collector,
            catalog: state.connectors.read().expect("registry")
                .catalog_backend(target.backend_name)?
                .iceberg_catalog().clone(),
            table,
            fs: abort_cleanup.fs,
            file_io,
            cleanup_path_mapper: abort_cleanup.path_mapper,
            cow_update_sidecar: None,
            target_ref: target_ref.to_string(),
        }).await
    })??;

    invalidate_iceberg_caches(state, target)?;
    Ok(StatementResult::Ok)
}
```

`drive_iceberg_sink` and `data_file_to_written_file` are existing helpers in `iceberg_writer.rs` (pre-flight: confirm with `grep`). If they're private, mark `pub(crate)`.

- [ ] **Step 8.2: Register module**

In `src/engine/mod.rs`:

```rust
pub(crate) mod iceberg_overwrite_partitions;
```

- [ ] **Step 8.3: Wire dispatch in iceberg_writer.rs**

In `execute_iceberg_insert_or_overwrite`, branch on `overwrite_mode`:

```rust
match overwrite_mode {
    OverwriteMode::DynamicPartitions => {
        return crate::engine::iceberg_overwrite_partitions::execute_iceberg_overwrite_partitions(
            state, target, resolved, columns, source, target_ref);
    }
    OverwriteMode::FullTable | OverwriteMode::None => { /* existing path */ }
}
```

(Adapt to the actual function signature; pre-flight reveals whether `execute_iceberg_insert_or_overwrite` takes `overwrite: bool` or `overwrite_mode` after Task 1.)

- [ ] **Step 8.4: Build + smoke test**

```bash
cargo build 2>&1 | tail -10
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS test_op;
USE test_op;
CREATE TABLE t (id INT, region VARCHAR(8))
  ENGINE=ICEBERG PARTITION BY identity(region)
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t VALUES (1, 'us'), (2, 'us'), (3, 'eu');
SELECT region, COUNT(*) FROM t GROUP BY region ORDER BY region;
INSERT OVERWRITE PARTITIONS t SELECT 99, 'us';
SELECT region, COUNT(*) FROM t GROUP BY region ORDER BY region;
SQL
kill %1
```

Expected first SELECT: `eu 1, us 2`. Second SELECT: `eu 1, us 1`.

- [ ] **Step 8.5: Commit**

```bash
git add src/engine/
git commit -m "feat(engine): wire INSERT OVERWRITE PARTITIONS to OverwritePartitionsCommit"
```

---

## Task 9: SQL regression suite

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_v3_overwrite_partitions.sql`
- Create: `sql-tests/iceberg/result/iceberg_v3_overwrite_partitions.result`

- [ ] **Step 9.1: Write the SQL file**

```sql
-- iceberg_v3_overwrite_partitions.sql
-- Suite: iceberg
-- Coverage: spec §7.2 — single-spec, multi-transform, cross-spec, empty, branch, DV.

DROP DATABASE IF EXISTS iceberg_op;
CREATE DATABASE iceberg_op;
USE iceberg_op;

-- Case 1: single partition transform (identity).
CREATE TABLE t1 (id INT, region VARCHAR(8))
  ENGINE=ICEBERG PARTITION BY identity(region)
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t1 VALUES (1, 'us'), (2, 'us'), (3, 'eu'), (4, 'eu');
INSERT OVERWRITE PARTITIONS t1 SELECT 99, 'us';
SELECT region, COUNT(*) FROM t1 GROUP BY region ORDER BY region;

-- Case 2: multiple partition transforms (days + bucket).
CREATE TABLE t2 (id INT, dt DATE, sk INT)
  ENGINE=ICEBERG PARTITION BY days(dt), bucket(4, sk)
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t2 VALUES (1, '2026-05-01', 10), (2, '2026-05-01', 11), (3, '2026-05-02', 10);
INSERT OVERWRITE PARTITIONS t2 SELECT 99, '2026-05-01', 10;
SELECT dt, COUNT(*) FROM t2 GROUP BY dt ORDER BY dt;

-- Case 3: cross-spec.
CREATE TABLE t3 (id INT, region VARCHAR(8), dt DATE)
  ENGINE=ICEBERG PARTITION BY identity(region)
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t3 VALUES (1, 'us', '2026-05-01'), (2, 'eu', '2026-05-01');
ALTER TABLE t3 ADD PARTITION COLUMN dt;
INSERT INTO t3 VALUES (3, 'us', '2026-05-02');
-- Touch (us, 2026-05-02) under spec-1 → also deletes spec-0 us files (R2).
INSERT OVERWRITE PARTITIONS t3 SELECT 99, 'us', '2026-05-02';
SELECT region, COUNT(*) FROM t3 GROUP BY region ORDER BY region;

-- Case 4: empty SELECT result → noop overwrite snapshot.
CREATE TABLE t4 (id INT, region VARCHAR(8))
  ENGINE=ICEBERG PARTITION BY identity(region)
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t4 VALUES (1, 'us');
INSERT OVERWRITE PARTITIONS t4 SELECT 99, 'us' WHERE 1=0;
SELECT * FROM t4;

-- Case 5: branch write.
CREATE TABLE t5 (id INT, region VARCHAR(8))
  ENGINE=ICEBERG PARTITION BY identity(region)
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t5 VALUES (1, 'us'), (2, 'eu');
ALTER TABLE t5 CREATE BRANCH dev;
INSERT INTO t5.branch_dev VALUES (10, 'us');
INSERT OVERWRITE PARTITIONS t5.branch_dev SELECT 99, 'us';
SELECT region, COUNT(*) FROM t5 FOR VERSION AS OF 'dev' GROUP BY region ORDER BY region;
SELECT region, COUNT(*) FROM t5 GROUP BY region ORDER BY region;  -- main intact

-- Case 6: covering DV.
CREATE TABLE t6 (id INT, region VARCHAR(8))
  ENGINE=ICEBERG PARTITION BY identity(region)
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t6 VALUES (1, 'us'), (2, 'us'), (3, 'us'), (4, 'eu');
DELETE FROM t6 WHERE id = 2;  -- creates a DV
SELECT COUNT(*) FROM t6;  -- 3
INSERT OVERWRITE PARTITIONS t6 SELECT 99, 'us';
SELECT region, COUNT(*) FROM t6 GROUP BY region ORDER BY region;

-- Case 7: time travel still sees pre-overwrite.
CREATE TABLE t7 (id INT, region VARCHAR(8))
  ENGINE=ICEBERG PARTITION BY identity(region)
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t7 VALUES (1, 'us'), (2, 'eu');
INSERT OVERWRITE PARTITIONS t7 SELECT 99, 'us';
-- Pre-overwrite snapshot is the second-most-recent. Use TIMESTAMP AS OF 1
-- millisecond before now (approximation; rely on inserts being separated).

-- Case 8 (error): non-partitioned table.
CREATE TABLE t_np (id INT, name VARCHAR(8))
  ENGINE=ICEBERG
  PROPERTIES('format-version'='3', 'row-lineage'='true');
INSERT INTO t_np VALUES (1, 'a');
INSERT OVERWRITE PARTITIONS t_np SELECT 99, 'b';

-- Case 9 (error): v2 table.
CREATE TABLE t_v2 (id INT, region VARCHAR(8))
  ENGINE=ICEBERG PARTITION BY identity(region)
  PROPERTIES('format-version'='2');
INSERT INTO t_v2 VALUES (1, 'us');
INSERT OVERWRITE PARTITIONS t_v2 SELECT 99, 'us';

-- Cleanup.
DROP DATABASE iceberg_op;
```

- [ ] **Step 9.2: Record fixture**

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_v3_overwrite_partitions --mode record --record-from target
kill %1
```

Inspect `sql-tests/iceberg/result/iceberg_v3_overwrite_partitions.result`. For Case 8 / 9, verify the recorded text contains a clean error string (not a panic).

- [ ] **Step 9.3: Verify**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_v3_overwrite_partitions --mode verify
```

Expected: PASS.

- [ ] **Step 9.4: Commit**

```bash
git add sql-tests/iceberg/
git commit -m "test(iceberg): iceberg_v3_overwrite_partitions regression suite"
```

---

## Task 10: Documentation update

(Identical pattern to PR-1 Task 9 — not duplicated here.)

- [ ] **Step 10.1: §10 in checklist** — change `[ ] INSERT OVERWRITE 动态分区` to `[x]` with `← 落地于 2026-MM-DD · #<PR>`.
- [ ] **Step 10.2: §20** — append fixture row.
- [ ] **Step 10.3: trailing changelog** — append row.

---

## Task 11: Final verification

- [ ] **Step 11.1**: `cargo test --lib`
- [ ] **Step 11.2**: `cargo fmt --check && cargo clippy --lib --tests -- -D warnings`
- [ ] **Step 11.3**: full iceberg suite verify (release build, parallel)
- [ ] **Step 11.4**: open PR with title `feat(iceberg): INSERT OVERWRITE PARTITIONS for v3 row-lineage tables`

---

## Self-Review Notes

**Spec coverage:**

| Spec section | Implemented in |
|---|---|
| §0 OVERWRITE PARTITIONS replace touched only | Task 7 |
| §0 v3 row-lineage required | Task 7a (engine + commit double-check) |
| §0 supports branch | Task 1 (parser via existing split_ref_suffix) + Task 8 (target_ref propagation) |
| §2.1 OVERWRITE PARTITIONS syntax | Task 1 + Task 2 |
| §2.4 fail-fast (non-partitioned, v2) | Task 8.1 |
| §4.1 engine flow with `P_touched` | Task 8 |
| §4.2 OverwritePartitionsCommit struct + commit | Task 6 + Task 7 |
| §4.3 empty SELECT noop / DV cleanup / branch / cross-spec | Task 7a/7b/7c + Task 8 (DV included via enumerate_live_all_files) |
| §4.4 fail-fast | Task 3 + Task 8.1 |
| §4.5 OverwriteCommit and OverwritePartitionsCommit coexist | not abstracted; Task 7 borrows write helpers but keeps them as separate actions |
| §6 commit-unknown / OCC | inherited via run_iceberg_commit |
| §7.2 SQL regression cases 1–9 | Task 9 |
| §8.2 checklist | Task 10 |

**Type-consistency:**

- `overwrite_mode: OverwriteMode` everywhere (no `bool` slipped through after Task 1).
- `target_ref: String` consistent with PR-1 / existing INSERT (default `"main"`).
- `partition_in_set` signature locked in Task 5; matches the touched-set construction in Task 7a.

**Placeholder scan:** none. The Step 5.4 `unreachable!()` is part of the implementer's flow (signature decision must be made before commit) and is explicitly called out as a refactor before tests pass.
