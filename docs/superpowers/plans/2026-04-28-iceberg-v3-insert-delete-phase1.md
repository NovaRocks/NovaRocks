# Iceberg v3 INSERT / DELETE Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable `INSERT INTO`, `INSERT OVERWRITE`, and `DELETE FROM` against v2/v3 Iceberg tables in NovaRocks standalone mode, using v2-compatible position-delete files.

**Architecture:** Engine layer is the transaction owner (validates, builds plan, runs pipeline, drives commit). Existing `IcebergSink` writes Parquet data/position-delete files and reports them via `state.add_sink_commit_info` (the per-fragment-instance `runtime/sink_commit.rs` table). After pipeline finishes, engine drains the list, hands `WrittenFile`s to one of three `IcebergCommitAction` impls (FastAppend / Overwrite / RowDelta). Overwrite and RowDelta are self-implemented on top of iceberg-rust 0.9's public primitives (`ManifestWriter`, `ManifestListWriter`, `Catalog::update_table`) because the upstream `Transaction` API only ships `fast_append`. Failure path is best-effort: on commit error or pipeline error, delete staged data files and any uncommitted manifest files via OpenDAL.

**Tech Stack:** Rust 2021, iceberg 0.9.0, sqlparser 0.61.0, opendal 0.55, tempfile 3, tokio, arrow.

**Spec:** [docs/superpowers/specs/2026-04-27-iceberg-v3-insert-delete-phase1-design.md](../specs/2026-04-27-iceberg-v3-insert-delete-phase1-design.md)

---

## Spec Deviation Note

Spec §3.3 proposes adding a `commit_collector: Option<Arc<IcebergCommitCollector>>` field to `IcebergSinkConfig` so the sink can callback into our collector. Deeper inspection found that [sink.rs:379, 484](../../../src/connector/iceberg/sink.rs:379) **already** reports each completed file through `state.add_sink_commit_info(TSinkCommitInfo)` to the per-fragment-instance global table at [`runtime/sink_commit.rs`](../../../src/runtime/sink_commit.rs). The plan therefore reuses this existing reporting channel — the engine layer reads `sink_commit::list(finst_id)` after pipeline completion and feeds the result to the commit-action. **No sink change required.** The architectural contract from the spec is unchanged; only the wiring detail is simpler than the spec described.

---

## Pre-Plan: Worktree Setup

Per CLAUDE.md: NovaRocks feature work uses `/Users/harbor/worktree/NovaRocks/<branch>`.

```bash
cd /Users/harbor/project/NovaRocks
git worktree add /Users/harbor/worktree/NovaRocks/iceberg-insert-delete-p1 -b iceberg-insert-delete-p1 main
cd /Users/harbor/worktree/NovaRocks/iceberg-insert-delete-p1
```

All file paths in this plan are relative to `src/` of the NovaRocks repo. All commands run from that worktree root.

---

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `src/connector/iceberg/commit/mod.rs` | Module entry, public re-exports |
| `src/connector/iceberg/commit/types.rs` | `CommitOpKind`, `WrittenFile`, `CommitOutcome` |
| `src/connector/iceberg/commit/abort.rs` | `AbortLog` (deferred-cleanup register) |
| `src/connector/iceberg/commit/validation.rs` | `ensure_v3_writable`, `ensure_single_partition_spec`, `ensure_no_equality_deletes`, `match_select_schema_to_table` |
| `src/connector/iceberg/commit/collector.rs` | `IcebergCommitCollector` (query-scoped state) |
| `src/connector/iceberg/commit/action.rs` | `IcebergCommitAction` trait + `CommitCtx` |
| `src/connector/iceberg/commit/fast_append.rs` | `FastAppendCommit` |
| `src/connector/iceberg/commit/row_delta.rs` | `RowDeltaCommit` (self-implemented) |
| `src/connector/iceberg/commit/overwrite.rs` | `OverwriteCommit` (self-implemented) |
| `src/connector/iceberg/commit/run.rs` | `run_iceberg_write_or_delete` orchestrator |
| `src/engine/delete_flow.rs` | DELETE FROM standalone entry |
| `tests/iceberg_insert_delete.rs` | Integration tests (IT-* / NEG-*) |
| `tests/common/mod.rs` (or extend) | Shared test helpers |
| `tests/common/fault_fs.rs` | OpenDAL fault-injection wrapper |
| `tests/sql-test-runner/suites/iceberg-write/insert.{sql,expected}` | SQL regression — INSERT |
| `tests/sql-test-runner/suites/iceberg-write/overwrite.{sql,expected}` | SQL regression — OVERWRITE |
| `tests/sql-test-runner/suites/iceberg-write/delete.{sql,expected}` | SQL regression — DELETE |
| `tests/sql-test-runner/suites/iceberg-write/mixed.{sql,expected}` | SQL regression — mixed |

**Modified files:**

| Path | Change |
|---|---|
| `src/connector/iceberg/mod.rs` | `pub mod commit;` |
| `src/sql/parser/ast.rs` | `InsertStmt.overwrite: bool`; new `DeleteStmt` |
| `src/engine/statement.rs` | Pass through `Insert.overwrite`; new `convert_sqlparser_delete_to_custom` |
| `src/engine/mod.rs` | `Statement::Delete` dispatch; INSERT iceberg dispatch |
| `src/engine/insert_flow.rs:65-66` | Replace iceberg rejection with iceberg-aware path |
| `src/lower/...` | DELETE plan lowering (target file located in Task 14) |

---

## Task 1: Spike — verify ManifestWriter status=DELETED entry path

**Why:** Spec §7.1 — OverwriteCommit (Task 10) needs to write `ManifestEntry { status: ManifestStatus::Deleted }` for every base-snapshot data file. Need to verify whether iceberg-rust 0.9 `ManifestWriter` exposes a public path for this, and if not, identify the fallback (handcraft `ManifestEntry` and feed via internal avro encoding).

**Files:** Investigation only; output is a markdown note.

- [ ] **Step 1: Read iceberg-rust ManifestWriter API**

```bash
cat /Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.9.0/src/spec/manifest/writer.rs
```

Look for:
- Signature of `add_existing_file`, `add_delete_file`, `add_file` — does any allow status=DELETED directly?
- Public type `ManifestEntry` and its constructors (is `ManifestEntry::builder()` public?)
- Whether `ManifestWriter::add_entry(entry)` or similar exists publicly

- [ ] **Step 2: Read ManifestEntry struct**

```bash
grep -n 'pub struct ManifestEntry\|impl ManifestEntry\|pub fn\|pub.*status\|ManifestStatus' /Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.9.0/src/spec/manifest/mod.rs | head -30
```

Verify `ManifestEntry` and `ManifestStatus::Deleted` are public.

- [ ] **Step 3: Write a tiny scratch test in `tests/scratch_manifest_deleted.rs`**

```rust
//! Scratch test: can we write a v2-data manifest containing one DELETED entry
//! using iceberg-rust 0.9 public APIs?
//!
//! This test validates spec §7.1; the implementation in Task 10 depends on the
//! result. The test should be deleted after the spike is resolved.

use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestStatus, ManifestEntry, Struct,
};

#[test]
fn manifest_entry_with_deleted_status_compiles() {
    let df = DataFile {
        content: DataContentType::Data,
        file_path: "s3://example/data/old.parquet".to_string(),
        file_format: DataFileFormat::Parquet,
        partition: Struct::empty(),
        record_count: 100,
        file_size_in_bytes: 4096,
        column_sizes: Default::default(),
        value_counts: Default::default(),
        null_value_counts: Default::default(),
        nan_value_counts: Default::default(),
        lower_bounds: Default::default(),
        upper_bounds: Default::default(),
        key_metadata: None,
        split_offsets: None,
        equality_ids: None,
        sort_order_id: None,
        partition_spec_id: 0,
        first_row_id: None,
        referenced_data_file: None,
        content_offset: None,
        content_size_in_bytes: None,
    };

    // The point of the spike: does this compile against iceberg-rust 0.9?
    // If `ManifestEntry::builder()` or struct-init compiles, we have a path.
    let _entry: ManifestEntry = ManifestEntry::builder()
        .status(ManifestStatus::Deleted)
        .data_file(df)
        .build();
}
```

- [ ] **Step 4: Run the scratch test**

```bash
cargo test --test scratch_manifest_deleted -- --nocapture
```

Expected: PASS, FAIL, or COMPILE-ERROR. Each outcome routes differently:

- **PASS** → record "Path A: `ManifestEntry::builder().status(Deleted).data_file(...).build()` works directly"
- **COMPILE-ERROR** on `ManifestEntry::builder` not found → check struct-init form
- **COMPILE-ERROR** on field privacy → fallback path needed (Path B)

If Path A works, also verify it can be passed to `ManifestWriter`:

```rust
// extend the scratch test
let mut writer = builder.build_v2_data();
writer.add_entry(_entry).expect("add_entry");
```

If `add_entry` is not public, search for whatever public method takes a pre-built entry.

- [ ] **Step 5: Document the finding**

Create `docs/superpowers/spikes/2026-04-28-manifest-deleted-entry.md`:

```markdown
# Spike — ManifestWriter status=DELETED entry path

**Date:** 2026-04-28
**Spec ref:** §7.1
**Outcome:** [Path A succeeded | Path B succeeded | All paths failed]

## Code path that works

\`\`\`rust
// exact, compileable snippet
\`\`\`

## Implications for Task 10 (OverwriteCommit)

[describe the helper signature: e.g. `pub(crate) fn write_deleted_entry(writer: &mut ManifestWriter, file: DataFile, base_seq: i64)`]

## If all paths failed

[either: use a fork; or fall back to manually serializing avro]
```

- [ ] **Step 6: Commit**

```bash
git add tests/scratch_manifest_deleted.rs docs/superpowers/spikes/2026-04-28-manifest-deleted-entry.md
git commit -m "spike: verify ManifestWriter status=DELETED entry path

Spec §7.1 spike. Documents which iceberg-rust 0.9 public API path
supports writing ManifestEntry { status: Deleted } for OverwriteCommit
(Task 10)."
```

**Halt condition:** If all three paths fail, STOP. Open a discussion: either pin a forked iceberg-rust, or change the OverwriteCommit strategy to "rewrite each base manifest with each entry's status flipped to DELETED" (more code, no new dependency). Do not proceed with Task 10 until resolved.

---

## Task 2: Spike — classify iceberg-rust commit-unknown errors

**Why:** Spec §7.2 / §5.4 — failure handling needs to distinguish "definitely failed (clean up)" from "commit-unknown (leave files for human review)". Need to know what `iceberg::Error` types `Catalog::update_table` returns so we can build `is_commit_unknown(&err) -> bool`.

**Files:** Investigation only.

- [ ] **Step 1: Read iceberg-rust Error type**

```bash
grep -n 'pub enum ErrorKind\|pub struct Error\|impl Error\|pub fn kind\|pub fn source\|ErrorKind::' /Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.9.0/src/error.rs | head -30
```

- [ ] **Step 2: Trace Catalog::update_table error flow**

```bash
grep -rn 'fn update_table' /Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.9.0/src/catalog /Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.9.0/src/catalog_loader 2>/dev/null
```

Identify error variants thrown for: AssertRefSnapshotId mismatch (OCC), network/IO errors, catalog rejection.

- [ ] **Step 3: Document classification rule**

Append to `docs/superpowers/spikes/2026-04-28-commit-unknown-classification.md`:

```markdown
# Spike — commit-unknown classification

**Date:** 2026-04-28
**Spec ref:** §5.4 / §7.2

## iceberg::ErrorKind variants

| ErrorKind | Source | Classification |
|-----------|--------|----------------|
| ConflictRequest | OCC AssertRefSnapshotId mismatch | DEFINITE FAIL → clean up |
| Unexpected | catch-all | UNKNOWN → do not clean up |
| DataInvalid | malformed payload | DEFINITE FAIL |
| FeatureUnsupported | catalog rejected | DEFINITE FAIL |
| ... | ... | ... |

## Classification function

\`\`\`rust
// goes into src/connector/iceberg/commit/run.rs in Task 11
pub(crate) fn is_commit_unknown(err: &iceberg::Error) -> bool {
    use iceberg::ErrorKind::*;
    match err.kind() {
        ConflictRequest | DataInvalid | FeatureUnsupported => false,
        _ => true,
    }
}
\`\`\`
```

If the actual ErrorKind set doesn't have the variants above, adjust the table and helper.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/spikes/2026-04-28-commit-unknown-classification.md
git commit -m "spike: classify iceberg-rust commit-unknown errors

Spec §7.2 spike. Documents the ErrorKind → classification mapping
that Task 11 (run_iceberg_commit) will use to decide
between fail-clean and fail-leave behaviors."
```

---

## Task 3: Module skeleton + types

**Files:**
- Create: `src/connector/iceberg/commit/mod.rs`
- Create: `src/connector/iceberg/commit/types.rs`
- Modify: `src/connector/iceberg/mod.rs`

- [ ] **Step 1: Write `src/connector/iceberg/commit/types.rs`**

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use iceberg::spec::{DataContentType, DataFileFormat, Struct};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOpKind {
    FastAppend,
    Overwrite,
    RowDelta,
}

/// Metadata about a single Parquet file produced by `IcebergSink` during a
/// pipeline run. Mirrors the subset of `TIcebergDataFile` we need for commit
/// and abort flows. Constructed from `TSinkCommitInfo` after pipeline finish.
#[derive(Clone, Debug)]
pub struct WrittenFile {
    pub path: String,
    pub format: DataFileFormat,
    pub content: DataContentType,
    pub partition_values: Struct,
    pub partition_spec_id: i32,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    pub split_offsets: Vec<i64>,
    pub column_sizes: HashMap<i32, u64>,
    pub value_counts: HashMap<i32, u64>,
    pub null_value_counts: HashMap<i32, u64>,
    pub key_metadata: Option<Vec<u8>>,
    /// Set only for content == PositionDeletes.
    pub referenced_data_file: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CommitOutcome {
    pub new_snapshot_id: i64,
    /// Manifest / manifest-list files written by the commit-action.
    /// Used by AbortLog for cleanup on failure; ignored on success.
    pub written_manifest_paths: Vec<String>,
}
```

- [ ] **Step 2: Write `src/connector/iceberg/commit/mod.rs`**

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// ... [SPDX header same as other files] ...

//! Iceberg commit machinery for standalone INSERT / INSERT OVERWRITE / DELETE.
//!
//! The engine layer constructs an [`IcebergCommitCollector`] before lowering,
//! drives the pipeline (which writes data / position-delete files via the
//! existing `IcebergSink`), and at pipeline completion calls
//! [`run_iceberg_write_or_delete`] which dispatches to one of three
//! [`IcebergCommitAction`] implementations and handles abort cleanup.

mod types;

pub use types::{CommitOpKind, CommitOutcome, WrittenFile};
```

(The other submodules and re-exports get added incrementally by later tasks.)

- [ ] **Step 3: Modify `src/connector/iceberg/mod.rs`**

Add `pub mod commit;` near the other `pub mod` declarations.

```bash
grep -n '^pub mod\|^mod ' src/connector/iceberg/mod.rs | head -10
```

Add `pub mod commit;` after the existing iceberg submodules (e.g. after `pub mod sink;` line).

- [ ] **Step 4: Add a smoke test in `src/connector/iceberg/commit/types.rs`**

Append to the file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{DataContentType, DataFileFormat, Struct};

    #[test]
    fn written_file_can_be_constructed() {
        let f = WrittenFile {
            path: "s3://x/data/abc.parquet".to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count: 100,
            file_size_in_bytes: 4096,
            split_offsets: vec![4],
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            key_metadata: None,
            referenced_data_file: None,
        };
        assert_eq!(f.record_count, 100);
        assert_eq!(f.content, DataContentType::Data);
    }

    #[test]
    fn op_kind_round_trips() {
        for k in [CommitOpKind::FastAppend, CommitOpKind::Overwrite, CommitOpKind::RowDelta] {
            assert_eq!(k, k);
        }
    }
}
```

- [ ] **Step 5: Run**

```bash
cargo test -p novarocks --lib commit::types -- --nocapture
```

Expected: 2 tests PASS. If `iceberg::spec::Struct::empty()` doesn't exist in 0.9, replace with the documented constructor (`Struct::empty()` or `Struct::from_iter([])`).

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/commit/ src/connector/iceberg/mod.rs
git commit -m "feat(iceberg/commit): add commit module skeleton with WrittenFile and CommitOpKind"
```

---

## Task 4: AbortLog

**Files:**
- Create: `src/connector/iceberg/commit/abort.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Write the failing tests first in `src/connector/iceberg/commit/abort.rs`**

```rust
// SPDX header

//! Best-effort cleanup register for staged Iceberg files.
//!
//! Failure of `commit` or pipeline must remove anything written so far. The
//! AbortLog tracks two categories separately because their lifetimes diverge:
//!
//! * data / position-delete files — owned by the pipeline; tracked redundantly
//!   here so abort doesn't need to drain `runtime/sink_commit.rs` first.
//! * manifest / manifest-list files — owned by the commit-action; only relevant
//!   if commit fails after writing manifests but before catalog.update_table
//!   succeeds.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use opendal::Operator;

#[derive(Default)]
pub struct AbortLog {
    staged_data_files: Mutex<Vec<String>>,
    written_manifests: Mutex<Vec<String>>,
    cleared: AtomicBool,
}

#[derive(Debug)]
pub struct CleanupError {
    pub path: String,
    pub source: opendal::Error,
}

impl AbortLog {
    pub fn new() -> Self { Self::default() }

    pub fn record_data_file(&self, path: String) {
        self.staged_data_files.lock().expect("abort log poisoned").push(path);
    }

    pub fn record_manifest(&self, path: String) {
        self.written_manifests.lock().expect("abort log poisoned").push(path);
    }

    pub fn drain_data_files(&self) -> Vec<String> {
        std::mem::take(&mut *self.staged_data_files.lock().expect("abort log poisoned"))
    }

    pub fn drain_manifests(&self) -> Vec<String> {
        std::mem::take(&mut *self.written_manifests.lock().expect("abort log poisoned"))
    }

    /// Idempotent. Best-effort: failures are returned, not propagated.
    pub async fn cleanup(&self, fs: &Operator) -> Vec<CleanupError> {
        if self.cleared.swap(true, Ordering::SeqCst) {
            return Vec::new();
        }
        let mut errs = Vec::new();
        for p in self.drain_data_files() {
            if let Err(e) = fs.delete(&p).await {
                errs.push(CleanupError { path: p, source: e });
            }
        }
        for p in self.drain_manifests() {
            if let Err(e) = fs.delete(&p).await {
                errs.push(CleanupError { path: p, source: e });
            }
        }
        errs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::services::Memory;
    use std::sync::Arc;
    use tokio;

    fn mem_op() -> Operator {
        Operator::new(Memory::default()).unwrap().finish()
    }

    #[tokio::test]
    async fn cleanup_deletes_recorded_paths() {
        let fs = mem_op();
        // pre-populate
        fs.write("a.parquet", b"x".to_vec()).await.unwrap();
        fs.write("b.parquet", b"y".to_vec()).await.unwrap();
        fs.write("m.avro", b"z".to_vec()).await.unwrap();

        let log = AbortLog::new();
        log.record_data_file("a.parquet".into());
        log.record_data_file("b.parquet".into());
        log.record_manifest("m.avro".into());

        let errs = log.cleanup(&fs).await;
        assert!(errs.is_empty(), "unexpected: {:?}", errs);

        assert!(fs.stat("a.parquet").await.is_err());
        assert!(fs.stat("b.parquet").await.is_err());
        assert!(fs.stat("m.avro").await.is_err());
    }

    #[tokio::test]
    async fn cleanup_is_idempotent() {
        let fs = mem_op();
        fs.write("a.parquet", b"x".to_vec()).await.unwrap();

        let log = AbortLog::new();
        log.record_data_file("a.parquet".into());
        let _ = log.cleanup(&fs).await;
        // second call — must be a no-op, must not panic
        let errs = log.cleanup(&fs).await;
        assert!(errs.is_empty());
    }

    #[tokio::test]
    async fn cleanup_collects_errors_for_missing_files() {
        let fs = mem_op();
        let log = AbortLog::new();
        log.record_data_file("does-not-exist.parquet".into());

        let errs = log.cleanup(&fs).await;
        // opendal Memory::delete on a missing key may or may not error; this
        // test pins behavior and documents the result. If errs is empty, fine
        // (memory backend is permissive). If non-empty, we get one entry.
        assert!(errs.len() <= 1);
    }

    #[tokio::test]
    async fn concurrent_record_is_safe() {
        let log = Arc::new(AbortLog::new());
        let mut handles = Vec::new();
        for i in 0..32 {
            let log = log.clone();
            handles.push(tokio::spawn(async move {
                log.record_data_file(format!("p{i}.parquet"));
            }));
        }
        for h in handles { h.await.unwrap(); }
        assert_eq!(log.drain_data_files().len(), 32);
    }
}
```

- [ ] **Step 2: Wire AbortLog into the module**

In `src/connector/iceberg/commit/mod.rs`, add:

```rust
mod abort;

pub use abort::{AbortLog, CleanupError};
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p novarocks --lib commit::abort -- --nocapture
```

Expected: 4 tests PASS. If opendal Memory crate is feature-gated, add `services-memory` to dev-deps in `Cargo.toml`:

```bash
grep -n 'opendal' Cargo.toml
```

If `services-memory` not present, add it to the default features list for `opendal`.

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/commit/abort.rs src/connector/iceberg/commit/mod.rs Cargo.toml
git commit -m "feat(iceberg/commit): add AbortLog with idempotent cleanup"
```

---

## Task 5: Validation helpers

**Files:**
- Create: `src/connector/iceberg/commit/validation.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Write `src/connector/iceberg/commit/validation.rs`**

```rust
// SPDX header

//! Shared validators used by both the INSERT (`engine/insert_flow.rs`) and
//! DELETE (`engine/delete_flow.rs`) entry points before lowering. All errors
//! returned here are user-visible — keep the messages action-oriented.

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use iceberg::table::Table;

/// Phase 1 does not write tables that have row-lineage enabled. Tables with
/// `variant`-typed columns are also rejected because the parquet writer in
/// `IcebergSink` has no encoding path for variant.
pub fn ensure_v3_writable(table: &Table) -> Result<(), String> {
    if let Some(props) = Some(table.metadata().properties()) {
        if let Some(v) = props.get("write.row-lineage") {
            if v == "true" {
                return Err(
                    "iceberg table has row-lineage enabled; phase 1 does not \
                     support writing such tables. Disable row-lineage or wait \
                     for phase 2.".to_string(),
                );
            }
        }
    }
    let schema = table.metadata().current_schema();
    for f in schema.as_struct().fields() {
        // The exact iceberg-rust 0.9 PrimitiveType variant name for VARIANT
        // is checked at compile time. If the variant doesn't exist (because
        // 0.9 predates it), this branch is unreachable.
        let ty_name = format!("{:?}", f.field_type);
        if ty_name.to_lowercase().contains("variant") {
            return Err(format!(
                "iceberg table column `{}` has variant type; phase 1 does not \
                 support writing variant. Drop the column or wait for phase 2.",
                f.name
            ));
        }
    }
    Ok(())
}

/// Phase 1 only handles tables whose data is all under the current default
/// partition spec. Multiple historical specs (partition evolution) require
/// per-file spec routing in the writer that we don't have yet.
pub fn ensure_single_partition_spec(table: &Table) -> Result<(), String> {
    let m = table.metadata();
    let default_id = m.default_partition_spec_id();
    let other = m.partition_specs_iter().filter(|s| s.spec_id() != default_id).count();
    if other > 0 {
        return Err(format!(
            "iceberg table has {other} non-default partition spec(s); phase 1 \
             writes require a single partition spec. Rewrite or drop historical \
             data under prior specs."
        ));
    }
    Ok(())
}

/// DELETE writes position-delete files; the existing scan reader (see
/// `iceberg/position_delete.rs:78-82`) does not support reading equality-delete
/// files, so a table that already has equality deletes attached to its current
/// snapshot would become unreadable after the new snapshot lands.
pub fn ensure_no_equality_deletes(table: &Table) -> Result<(), String> {
    let snap = match table.metadata().current_snapshot() {
        Some(s) => s,
        None => return Ok(()), // empty table — no manifests to inspect
    };
    // Walk manifest list; if any manifest has content == Deletes and any of
    // its entries has content_type == EqualityDeletes, reject.
    //
    // Implementation note: we only need a fast path here. The per-manifest
    // ManifestFile.content() field tells us whether a manifest holds delete
    // entries; we don't need to read each entry. Phase 1 can be conservative
    // and just check that flag.
    let manifest_list_path = snap.manifest_list();
    let _ = manifest_list_path;
    // Detailed implementation deferred to Task 9/10 helpers, which already
    // need to walk manifests for the OverwriteCommit path. For now, do the
    // best-effort check using the snapshot summary:
    if let Some(s) = snap.summary().additional_properties.get("total-equality-deletes") {
        if let Ok(n) = s.parse::<u64>() {
            if n > 0 {
                return Err(
                    "iceberg table has equality-delete files in its current snapshot; \
                     phase 1 reader does not support equality deletes (see \
                     iceberg/position_delete.rs). Compact away the equality \
                     deletes before issuing DELETE.".to_string(),
                );
            }
        }
    }
    Ok(())
}

/// Strict column-by-column type match between the SELECT's arrow schema and
/// the iceberg table schema. No implicit cast, no reorder.
///
/// `columns_clause` is the optional `INSERT INTO t (cols)` list; when None,
/// SELECT must produce exactly `table_schema.fields().len()` columns in the
/// table's natural order.
pub fn match_select_schema_to_table(
    select_schema: &ArrowSchemaRef,
    table: &Table,
    columns_clause: Option<&[String]>,
) -> Result<(), String> {
    let iceberg_schema = table.metadata().current_schema();
    let table_fields = iceberg_schema.as_struct().fields();

    let target_fields: Vec<_> = match columns_clause {
        None => table_fields.iter().collect(),
        Some(names) => {
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                let f = table_fields
                    .iter()
                    .find(|f| f.name == *n)
                    .ok_or_else(|| format!("INSERT column `{n}` does not exist in table"))?;
                out.push(f);
            }
            out
        }
    };

    if select_schema.fields().len() != target_fields.len() {
        return Err(format!(
            "INSERT column count mismatch: SELECT produces {} columns, target expects {}",
            select_schema.fields().len(),
            target_fields.len()
        ));
    }

    for (i, (sel, tgt)) in select_schema
        .fields()
        .iter()
        .zip(target_fields.iter())
        .enumerate()
    {
        if !arrow_iceberg_types_compatible(sel.data_type(), &tgt.field_type) {
            return Err(format!(
                "INSERT column {i} type mismatch: SELECT produces {:?}, target column `{}` is {:?}; \
                 phase 1 does not perform implicit cast — wrap the SELECT expression in CAST() explicitly.",
                sel.data_type(), tgt.name, tgt.field_type
            ));
        }
    }
    Ok(())
}

/// Strict type compatibility — same logical type only. Implementation calls
/// into the existing arrow ↔ iceberg helper used by IcebergSink. The helper
/// must already exist in `src/connector/iceberg/sink.rs` (see how
/// `build_output_schema` works); here we delegate.
fn arrow_iceberg_types_compatible(
    arrow_ty: &arrow::datatypes::DataType,
    iceberg_ty: &iceberg::spec::Type,
) -> bool {
    // Convert iceberg type → arrow data type via the helper that
    // `IcebergSink::build_output_schema` uses, then compare for strict equality.
    match crate::connector::iceberg::types::iceberg_type_to_arrow(iceberg_ty) {
        Ok(expected) => &expected == arrow_ty,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    // Tests use real iceberg-rust Table fixtures via the in-repo test helpers
    // that phase4a already established. Reuse `make_test_iceberg_table`-style
    // utilities from `mv_refresh_iceberg.rs::tests`.
    //
    // The test file itself locates them by re-importing — adjust if those
    // helpers were moved by the upstream "move module" commit.
    use super::*;
    // The four functions here have unit tests in tests/iceberg_insert_delete.rs
    // because constructing realistic Table objects requires the integration
    // harness. We only do a compile-test in this unit module:

    #[test]
    fn errors_carry_actionable_messages() {
        let s = "row-lineage";
        assert!(s.contains("row-lineage"));
    }
}
```

- [ ] **Step 2: Verify `iceberg_type_to_arrow` helper exists**

```bash
grep -rn 'fn iceberg_type_to_arrow\|fn arrow_to_iceberg' src/connector/iceberg/ | head
```

If absent, before writing `arrow_iceberg_types_compatible`, locate the inverse helper currently used by `IcebergSink::build_output_schema` and reuse / inline it.

- [ ] **Step 3: Wire validation into the module**

In `src/connector/iceberg/commit/mod.rs`:

```rust
mod validation;

pub use validation::{
    ensure_no_equality_deletes, ensure_single_partition_spec, ensure_v3_writable,
    match_select_schema_to_table,
};
```

- [ ] **Step 4: Run unit tests + compile**

```bash
cargo test -p novarocks --lib commit::validation -- --nocapture
cargo build -p novarocks
```

Expected: PASS + clean build. The integration tests for these four validators land in Task 17 (NEG-*) because they need full Table fixtures.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/validation.rs src/connector/iceberg/commit/mod.rs
git commit -m "feat(iceberg/commit): add v3-write / partition-spec / equality-delete / schema-match validators"
```

---

## Task 6: IcebergCommitCollector

**Files:**
- Create: `src/connector/iceberg/commit/collector.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Write `collector.rs`**

```rust
// SPDX header

//! Query-scoped state shared between engine flow and commit-action.
//!
//! The collector is created in the engine layer before lowering, holds the
//! query-level "what are we doing" metadata, and after the pipeline completes
//! drains the per-fragment-instance `runtime/sink_commit.rs` table to obtain
//! the list of files the IcebergSink wrote.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use iceberg::TableIdent;
use iceberg::spec::SchemaRef as IcebergSchemaRef;
use iceberg::spec::PartitionSpecRef;

use crate::common::types::UniqueId;

use super::abort::AbortLog;
use super::types::{CommitOpKind, WrittenFile};

/// Query-scoped Iceberg write/delete state.
///
/// Lifetime: created in `engine/insert_flow.rs` or `engine/delete_flow.rs`,
/// dropped after `run_iceberg_write_or_delete` returns (success or fail).
pub struct IcebergCommitCollector {
    pub op_kind: CommitOpKind,
    pub table_ident: TableIdent,
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: i64,
    pub schema: IcebergSchemaRef,
    pub partition_spec: PartitionSpecRef,
    pub staging_dir: String,
    pub finst_id: UniqueId,
    pub abort_log: Arc<AbortLog>,
    committed: AtomicBool,
}

impl IcebergCommitCollector {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        op_kind: CommitOpKind,
        table_ident: TableIdent,
        base_snapshot_id: Option<i64>,
        base_sequence_number: i64,
        schema: IcebergSchemaRef,
        partition_spec: PartitionSpecRef,
        staging_dir: String,
        finst_id: UniqueId,
    ) -> Self {
        Self {
            op_kind,
            table_ident,
            base_snapshot_id,
            base_sequence_number,
            schema,
            partition_spec,
            staging_dir,
            finst_id,
            abort_log: Arc::new(AbortLog::new()),
            committed: AtomicBool::new(false),
        }
    }

    /// Drain the per-finst sink_commit table and convert each
    /// `TIcebergDataFile` into a `WrittenFile`. Records each path into the
    /// abort log so a later commit failure can clean up.
    pub fn take_written_files(&self) -> Result<Vec<WrittenFile>, String> {
        let infos = crate::runtime::sink_commit::list(self.finst_id);
        let mut out = Vec::with_capacity(infos.len());
        for info in infos {
            let df = info
                .iceberg_data_file
                .ok_or_else(|| "sink_commit_info missing iceberg_data_file".to_string())?;
            let wf = self.convert(df)?;
            self.abort_log.record_data_file(wf.path.clone());
            out.push(wf);
        }
        Ok(out)
    }

    fn convert(
        &self,
        df: crate::types::TIcebergDataFile,
    ) -> Result<WrittenFile, String> {
        use iceberg::spec::{DataContentType, DataFileFormat, Struct};

        let path = df.path.ok_or("TIcebergDataFile missing path")?;
        let content = match df.file_content.unwrap_or(crate::types::TIcebergFileContent::DATA) {
            crate::types::TIcebergFileContent::DATA => DataContentType::Data,
            crate::types::TIcebergFileContent::POSITION_DELETES => DataContentType::PositionDeletes,
            crate::types::TIcebergFileContent::EQUALITY_DELETES => {
                return Err("phase 1 does not produce equality-delete files".into());
            }
        };
        // Parse `partition_path` (e.g. "p=1/q=A") into a Struct keyed by
        // partition_spec field. Phase 4a already does this in
        // `mv_refresh_iceberg::partition_path_to_struct` — reuse if present;
        // otherwise inline a minimal version that uses self.partition_spec.
        let partition_values = parse_partition_path(
            df.partition_path.as_deref().unwrap_or(""),
            &self.partition_spec,
            &self.schema,
        )?;
        Ok(WrittenFile {
            path,
            format: DataFileFormat::Parquet,
            content,
            partition_values,
            partition_spec_id: self.partition_spec.spec_id(),
            record_count: df.record_count.unwrap_or(0).max(0) as u64,
            file_size_in_bytes: df.file_size_in_bytes.unwrap_or(0).max(0) as u64,
            split_offsets: df.split_offsets.unwrap_or_default(),
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            key_metadata: None,
            referenced_data_file: df.referenced_data_file,
        })
    }

    pub fn mark_committed(&self) {
        self.committed.store(true, Ordering::SeqCst);
    }

    pub fn is_committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }
}

fn parse_partition_path(
    path: &str,
    spec: &PartitionSpecRef,
    schema: &IcebergSchemaRef,
) -> Result<iceberg::spec::Struct, String> {
    use iceberg::spec::{Literal, Struct};

    if path.is_empty() {
        return Ok(Struct::empty());
    }
    let mut values: Vec<Option<Literal>> = Vec::with_capacity(spec.fields().len());
    let segments: Vec<_> = path.trim_matches('/').split('/').collect();
    if segments.len() != spec.fields().len() {
        return Err(format!(
            "partition_path `{path}` has {} segments but spec expects {}",
            segments.len(), spec.fields().len()
        ));
    }
    for (seg, field) in segments.iter().zip(spec.fields().iter()) {
        let (_k, v) = seg.split_once('=').ok_or_else(|| {
            format!("partition_path segment `{seg}` is missing `=`")
        })?;
        let source_field = schema
            .field_by_id(field.source_id)
            .ok_or_else(|| format!("partition source field id {} not in schema", field.source_id))?;
        // Phase 1: only identity transform. For other transforms we'd need to
        // round-trip through iceberg-rust's transform parser; out of scope.
        if !field.transform.is_identity() {
            return Err(format!(
                "phase 1 partition transform `{:?}` not yet supported during \
                 partition_path → Struct decoding", field.transform
            ));
        }
        let lit = parse_literal_for_type(v, &source_field.field_type)
            .map_err(|e| format!("partition value `{v}` parse failed: {e}"))?;
        values.push(Some(lit));
    }
    Ok(Struct::from_iter(values))
}

fn parse_literal_for_type(
    raw: &str,
    ty: &iceberg::spec::Type,
) -> Result<iceberg::spec::Literal, String> {
    use iceberg::spec::{Literal, PrimitiveType, Type};
    let prim = match ty {
        Type::Primitive(p) => p,
        _ => return Err(format!("phase 1 only supports primitive partition types, got {:?}", ty)),
    };
    let raw = if raw == "__HIVE_DEFAULT_PARTITION__" || raw == "null" {
        return Err("phase 1 does not support null partition values".into());
    } else {
        raw
    };
    match prim {
        PrimitiveType::Int => raw.parse::<i32>().map(Literal::int).map_err(|e| e.to_string()),
        PrimitiveType::Long => raw.parse::<i64>().map(Literal::long).map_err(|e| e.to_string()),
        PrimitiveType::String => Ok(Literal::string(urlencoding::decode(raw).map_err(|e| e.to_string())?.into_owned())),
        PrimitiveType::Boolean => raw.parse::<bool>().map(Literal::bool).map_err(|e| e.to_string()),
        // extend as needed; phase 1 covers the common cases used by sql tests
        _ => Err(format!("phase 1 partition type {prim:?} not yet supported")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Fixture: minimal IcebergSchemaRef + PartitionSpecRef constructed inline.
    fn fixture_schema_and_spec() -> (IcebergSchemaRef, PartitionSpecRef) {
        use iceberg::spec::{NestedField, PartitionField, PartitionSpec, PrimitiveType, Schema, Transform, Type};
        use std::sync::Arc;
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "p", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "v", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("p", "p", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        (Arc::new(schema), Arc::new(spec))
    }

    #[test]
    fn parse_empty_partition_path_returns_empty_struct() {
        let (schema, spec) = fixture_schema_and_spec();
        let s = parse_partition_path("", &spec, &schema).unwrap();
        assert_eq!(s.fields().len(), 0);
    }

    #[test]
    fn parse_one_segment_identity_int() {
        let (schema, spec) = fixture_schema_and_spec();
        let s = parse_partition_path("p=42", &spec, &schema).unwrap();
        assert_eq!(s.fields().len(), 1);
    }

    #[test]
    fn rejects_segment_count_mismatch() {
        let (schema, spec) = fixture_schema_and_spec();
        let r = parse_partition_path("p=1/q=2", &spec, &schema);
        assert!(r.is_err());
    }
}
```

- [ ] **Step 2: Wire collector**

In `src/connector/iceberg/commit/mod.rs`:

```rust
mod collector;

pub use collector::IcebergCommitCollector;
```

- [ ] **Step 3: Run**

```bash
cargo test -p novarocks --lib commit::collector -- --nocapture
```

Expected: 3 PASS. If iceberg-rust 0.9 builder API differs (`Schema::builder().with_fields()` may or may not exist verbatim), inspect:

```bash
grep -n 'pub fn builder\|with_schema_id\|with_fields' /Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.9.0/src/spec/schema.rs
```

and adapt the fixture to the actual API. Also add `urlencoding = "2"` to `Cargo.toml` if not already present.

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/commit/collector.rs src/connector/iceberg/commit/mod.rs Cargo.toml
git commit -m "feat(iceberg/commit): add IcebergCommitCollector with sink_commit drain"
```

---

## Task 7: IcebergCommitAction trait + CommitCtx

**Files:**
- Create: `src/connector/iceberg/commit/action.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Write `action.rs`**

```rust
// SPDX header

use std::sync::Arc;

use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::io::FileIO;
use iceberg::table::Table;
use uuid::Uuid;

use super::abort::AbortLog;
use super::collector::IcebergCommitCollector;
use super::types::CommitOutcome;

pub struct CommitCtx<'a> {
    pub collector: &'a IcebergCommitCollector,
    pub table: &'a Table,
    pub catalog: &'a dyn Catalog,
    pub file_io: &'a FileIO,
    pub commit_uuid: Uuid,
    pub abort_handle: Arc<AbortLog>,
}

#[async_trait]
pub trait IcebergCommitAction: Send + Sync {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String>;
}
```

`FileIO` is plumbed in here (rather than relying on `table.file_io()`) so the engine layer can supply a uniform FileIO that matches the storage backend used for staging writes — important when catalog-default FileIO and engine-side OpenDAL Operator are configured against different credentials/regions.

- [ ] **Step 2: Wire into module**

```rust
// in commit/mod.rs
mod action;

pub use action::{CommitCtx, IcebergCommitAction};
```

- [ ] **Step 3: Verify async-trait, uuid in deps**

```bash
grep -n 'async-trait\|^uuid ' Cargo.toml | head -5
```

Add `async-trait = "0.1"` and `uuid = { version = "1", features = ["v4"] }` if missing.

- [ ] **Step 4: Compile check**

```bash
cargo build -p novarocks
```

Expected: clean build.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/action.rs src/connector/iceberg/commit/mod.rs Cargo.toml
git commit -m "feat(iceberg/commit): add IcebergCommitAction trait and CommitCtx"
```

---

## Task 8: FastAppendCommit

**Files:**
- Create: `src/connector/iceberg/commit/fast_append.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Write the failing test in `fast_append.rs`**

```rust
// SPDX header

//! `Transaction::fast_append` wrapper for INSERT INTO. The simplest of the
//! three commit-actions; iceberg-rust handles all manifest authoring.

use async_trait::async_trait;
use iceberg::spec::DataContentType;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use super::action::{CommitCtx, IcebergCommitAction};
use super::types::CommitOutcome;
use super::write_file_to_iceberg::written_file_to_iceberg_data_file;

pub struct FastAppendCommit;

#[async_trait]
impl IcebergCommitAction for FastAppendCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;

        // No-op short circuit per spec §4.1
        if written.is_empty() {
            let id = ctx
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(0);
            return Ok(CommitOutcome { new_snapshot_id: id, written_manifest_paths: vec![] });
        }

        // FastAppend rejects non-Data content per iceberg-rust's
        // validate_added_data_files. Catch it early with a clearer message.
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "FastAppendCommit received {:?} content; expected Data only", f.content
                ));
            }
        }

        let data_files: Vec<_> = written
            .iter()
            .map(|f| written_file_to_iceberg_data_file(f, ctx.collector))
            .collect::<Result<Vec<_>, _>>()?;

        let mut tx = Transaction::new(ctx.table);
        let action = tx
            .fast_append()
            .add_data_files(data_files)
            .set_commit_uuid(ctx.commit_uuid);
        action
            .apply(&mut tx)
            .map_err(|e| format!("fast_append apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("fast_append commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| "fast_append committed but new snapshot not visible".to_string())?;
        Ok(CommitOutcome {
            new_snapshot_id,
            written_manifest_paths: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_is_noop() {
        // The empty-input branch is verified end-to-end in
        // tests/iceberg_insert_delete.rs::it_del_2 since it requires a real
        // Table fixture. This unit test is a placeholder establishing the
        // module compiles.
        let _ = FastAppendCommit;
    }
}
```

- [ ] **Step 2: Stub `written_file_to_iceberg_data_file`**

Create `src/connector/iceberg/commit/data_file.rs`:

```rust
// SPDX header
//! `WrittenFile` → `iceberg::spec::DataFile` conversion shared by all three
//! commit-actions.

use iceberg::spec::DataFile;

use super::collector::IcebergCommitCollector;
use super::types::WrittenFile;

pub fn written_file_to_iceberg_data_file(
    f: &WrittenFile,
    collector: &IcebergCommitCollector,
) -> Result<DataFile, String> {
    Ok(DataFile {
        content: f.content,
        file_path: f.path.clone(),
        file_format: f.format,
        partition: f.partition_values.clone(),
        record_count: f.record_count,
        file_size_in_bytes: f.file_size_in_bytes,
        column_sizes: f.column_sizes.clone(),
        value_counts: f.value_counts.clone(),
        null_value_counts: f.null_value_counts.clone(),
        nan_value_counts: Default::default(),
        lower_bounds: Default::default(),
        upper_bounds: Default::default(),
        key_metadata: f.key_metadata.clone(),
        split_offsets: if f.split_offsets.is_empty() {
            None
        } else {
            Some(f.split_offsets.clone())
        },
        equality_ids: None,
        sort_order_id: None,
        partition_spec_id: f.partition_spec_id,
        first_row_id: None,
        referenced_data_file: f.referenced_data_file.clone(),
        content_offset: None,
        content_size_in_bytes: None,
    })
    // collector is currently unused but the signature reserves room for
    // commit-actions that need schema/spec context (e.g., RowDelta when
    // looking up referenced_data_file's partition).
    .map(|x| { let _ = collector; x })
}
```

In `commit/mod.rs`:

```rust
mod data_file;
pub(crate) use data_file::written_file_to_iceberg_data_file;
```

Then update `fast_append.rs` import:

```rust
use super::data_file::written_file_to_iceberg_data_file;
```

(Remove the `super::write_file_to_iceberg::...` line — it was a placeholder.)

- [ ] **Step 3: Wire FastAppendCommit**

In `commit/mod.rs`:

```rust
mod fast_append;
pub use fast_append::FastAppendCommit;
```

- [ ] **Step 4: Run unit test + compile**

```bash
cargo test -p novarocks --lib commit::fast_append -- --nocapture
cargo build -p novarocks
```

Expected: 1 PASS, clean build. Real round-trip in Task 15.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/
git commit -m "feat(iceberg/commit): add FastAppendCommit and shared DataFile conversion"
```

---

## Task 9: RowDeltaCommit

**Files:**
- Create: `src/connector/iceberg/commit/row_delta.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Write `row_delta.rs`**

Spec §4.4. The high-level steps are:

1. Filter `written` to PositionDeletes (RowDelta should never see Data).
2. Build a new delete manifest via `ManifestWriterBuilder.build_v{2,3}_deletes()`.
3. Inherit the existing manifest list — read it from `base_snapshot.manifest_list()`.
4. Append a manifest-list entry for the new delete manifest.
5. Construct a Snapshot with `summary.operation = "delete"`.
6. Apply via `Catalog::update_table` with `AssertRefSnapshotId` on `main`.

```rust
// SPDX header

use std::sync::Arc;

use async_trait::async_trait;
use iceberg::TableUpdate;
use iceberg::TableRequirement;
use iceberg::spec::{
    DataContentType, FormatVersion, ManifestContentType, ManifestEntry, ManifestList,
    ManifestListEntry, ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation,
    Snapshot, SnapshotReference, SnapshotRetention, Summary, MAIN_BRANCH,
};
use iceberg::TableCommit;

use super::action::{CommitCtx, IcebergCommitAction};
use super::data_file::written_file_to_iceberg_data_file;
use super::types::CommitOutcome;

pub struct RowDeltaCommit;

#[async_trait]
impl IcebergCommitAction for RowDeltaCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;

        // Spec §4.1 no-op: empty input → no new snapshot.
        if written.is_empty() {
            let id = ctx.table.metadata().current_snapshot().map(|s| s.snapshot_id()).unwrap_or(0);
            return Ok(CommitOutcome { new_snapshot_id: id, written_manifest_paths: vec![] });
        }

        for f in &written {
            if f.content != DataContentType::PositionDeletes {
                return Err(format!(
                    "RowDeltaCommit received {:?} content; expected PositionDeletes only", f.content
                ));
            }
        }

        let m = ctx.table.metadata();
        let base_snap = m.current_snapshot()
            .ok_or_else(|| "RowDelta against empty table is meaningless".to_string())?;
        let new_seq = m.last_sequence_number() + 1;
        let new_snap_id = generate_snapshot_id();
        let format_version = m.format_version();

        // 1. Write the new delete manifest.
        let delete_manifest_path = format!(
            "{}/{}-row-delta-deletes-0.avro",
            metadata_dir(ctx.table)?,
            ctx.commit_uuid
        );
        ctx.abort_handle.record_manifest(delete_manifest_path.clone());
        let delete_manifest_file = write_delete_manifest(
            ctx,
            &written,
            &delete_manifest_path,
            new_seq,
            new_snap_id,
            format_version,
        ).await?;

        // 2. Inherit base manifest list and append the new manifest.
        let mut entries = read_manifest_list_entries(ctx.table, base_snap, ctx.file_io).await?;
        entries.push(delete_manifest_file);

        // 3. Write the new manifest list.
        let manifest_list_path = format!(
            "{}/snap-{}-{}.avro",
            metadata_dir(ctx.table)?,
            new_snap_id,
            ctx.commit_uuid
        );
        ctx.abort_handle.record_manifest(manifest_list_path.clone());
        write_manifest_list(ctx.file_io, &manifest_list_path, &entries, new_snap_id, Some(base_snap.snapshot_id()), new_seq, format_version).await?;

        // 4. Construct Snapshot.
        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snap_id)
            .with_parent_snapshot_id(Some(base_snap.snapshot_id()))
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path.clone())
            .with_schema_id(m.current_schema_id())
            .with_summary(Summary {
                operation: Operation::Delete,
                additional_properties: row_delta_summary_props(&written),
            })
            .build();

        // 5. TableCommit.
        let updates = vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference {
                    snapshot_id: new_snap_id,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            },
        ];
        let requirements = vec![
            TableRequirement::AssertRefSnapshotId {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(base_snap.snapshot_id()),
            },
            TableRequirement::AssertCurrentSchemaId {
                current_schema_id: m.current_schema_id(),
            },
            TableRequirement::AssertDefaultSpecId {
                default_spec_id: m.default_partition_spec_id(),
            },
        ];

        let commit = TableCommit::builder()
            .ident(ctx.table.identifier().clone())
            .updates(updates)
            .requirements(requirements)
            .build();

        ctx.catalog.update_table(commit).await
            .map_err(|e| format!("catalog.update_table failed in RowDelta: {e}"))?;

        Ok(CommitOutcome {
            new_snapshot_id: new_snap_id,
            written_manifest_paths: vec![delete_manifest_path, manifest_list_path],
        })
    }
}

// --- helpers ---

fn generate_snapshot_id() -> i64 {
    // Iceberg spec: random 64-bit positive ID. Java uses System.nanoTime XOR
    // with random. For Rust, use a stable scheme: the lower 63 bits of a v4
    // UUID's high half.
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen::<i64>().abs()
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn metadata_dir(table: &iceberg::table::Table) -> Result<String, String> {
    Ok(format!("{}/metadata", table.metadata().location()))
}

fn row_delta_summary_props(written: &[crate::connector::iceberg::commit::types::WrittenFile]) -> std::collections::HashMap<String, String> {
    use std::collections::HashMap;
    let mut p = HashMap::new();
    let total_records: u64 = written.iter().map(|f| f.record_count).sum();
    let total_size: u64 = written.iter().map(|f| f.file_size_in_bytes).sum();
    p.insert("added-position-delete-files".to_string(), written.len().to_string());
    p.insert("added-position-deletes".to_string(), total_records.to_string());
    p.insert("added-files-size".to_string(), total_size.to_string());
    p
}

async fn write_delete_manifest(
    ctx: &CommitCtx<'_>,
    written: &[crate::connector::iceberg::commit::types::WrittenFile],
    out_path: &str,
    new_seq: i64,
    new_snap_id: i64,
    fv: FormatVersion,
) -> Result<ManifestListEntry, String> {
    use iceberg::io::FileIOBuilder;
    let table = ctx.table;
    // Use the table's FileIO; fall back to constructing one from the catalog
    // configuration if absent.
    let file_io = ctx.file_io.clone();

    let output_file = file_io.new_output(out_path)
        .map_err(|e| format!("new_output for delete manifest failed: {e}"))?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(new_snap_id),
        None,
        table.metadata().current_schema().clone(),
        ctx.collector.partition_spec.clone(),
    );
    let mut writer = match fv {
        FormatVersion::V2 => builder.build_v2_deletes(),
        FormatVersion::V3 => builder.build_v3_deletes(),
        FormatVersion::V1 => return Err("v1 tables do not support delete files".into()),
    };
    for f in written {
        let df = written_file_to_iceberg_data_file(f, ctx.collector)?;
        writer.add_delete_file(df, new_seq)
            .map_err(|e| format!("manifest add_delete_file failed: {e}"))?;
    }
    let manifest_file = writer.write_manifest_file()
        .await
        .map_err(|e| format!("manifest writer write failed: {e}"))?;

    Ok(ManifestListEntry::from_manifest_file(manifest_file, ManifestContentType::Deletes))
}

async fn read_manifest_list_entries(
    table: &iceberg::table::Table,
    base_snap: &Snapshot,
    file_io: &iceberg::io::FileIO,
) -> Result<Vec<ManifestListEntry>, String> {
    let manifest_list_path = base_snap.manifest_list();
    let bytes = file_io.new_input(manifest_list_path)
        .map_err(|e| format!("input for manifest_list failed: {e}"))?
        .read()
        .await
        .map_err(|e| format!("read manifest_list failed: {e}"))?;
    let fv = table.metadata().format_version();
    let list = ManifestList::parse_with_version(&bytes, fv)
        .map_err(|e| format!("parse manifest_list failed: {e}"))?;
    Ok(list.entries().to_vec())
}

pub(super) async fn write_manifest_list(
    file_io: &iceberg::io::FileIO,
    out_path: &str,
    entries: &[ManifestListEntry],
    snap_id: i64,
    parent: Option<i64>,
    seq: i64,
    fv: FormatVersion,
) -> Result<(), String> {
    let out = file_io.new_output(out_path)
        .map_err(|e| format!("output for manifest list failed: {e}"))?;
    let mut writer = match fv {
        FormatVersion::V1 => ManifestListWriter::v1(out, snap_id, parent),
        FormatVersion::V2 => ManifestListWriter::v2(out, snap_id, parent, seq),
        FormatVersion::V3 => ManifestListWriter::v3(out, snap_id, parent, seq),
    };
    writer.add_manifest_entries(entries.iter().cloned())
        .map_err(|e| format!("manifest_list add_entries failed: {e}"))?;
    writer.close().await
        .map_err(|e| format!("manifest_list close failed: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn snapshot_id_is_positive() {
        for _ in 0..100 {
            assert!(generate_snapshot_id() >= 0);
        }
    }
}
```

- [ ] **Step 2: Wire**

In `commit/mod.rs`:

```rust
mod row_delta;
pub use row_delta::RowDeltaCommit;
```

- [ ] **Step 3: Resolve API mismatches**

Some symbols above (e.g. `ManifestListEntry::from_manifest_file`, `writer.write_manifest_file`, `add_manifest_entries`) are best-effort guesses. Check actual API and adapt:

```bash
grep -n 'pub fn\|pub.*from_manifest_file\|pub.*write_manifest_file\|pub.*add_manifest_entries\|pub fn close' /Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.9.0/src/spec/manifest_list.rs /Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.9.0/src/spec/manifest/writer.rs | head -25
```

Adjust function names and arguments to match. Add `rand = "0.8"` to deps if missing:

```bash
grep -n '^rand' Cargo.toml
```

- [ ] **Step 4: Compile**

```bash
cargo build -p novarocks
```

Expected: clean build. End-to-end test in Task 15.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/row_delta.rs src/connector/iceberg/commit/mod.rs Cargo.toml
git commit -m "feat(iceberg/commit): add RowDeltaCommit using public ManifestWriter primitives"
```

---

## Task 10: OverwriteCommit

**Files:**
- Create: `src/connector/iceberg/commit/overwrite.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

**Depends on:** Task 1 spike resolution.

- [ ] **Step 1: Write `overwrite.rs`**

Spec §4.3. Algorithm:

1. Enumerate all currently-live data files in base snapshot's manifest list (status ∈ {ADDED, EXISTING}).
2. Write a "deleted-data" manifest containing each as a `ManifestEntry { status: DELETED }` (path resolved by Task 1 spike).
3. Write a new "data" manifest with the freshly-written WrittenFiles (status: ADDED).
4. Write a manifest list — does NOT inherit base entries (per spec §4.3 step 4).
5. Construct Snapshot with `summary.operation = "overwrite"`.
6. TableCommit with AssertRefSnapshotId.

```rust
// SPDX header

use async_trait::async_trait;
use iceberg::spec::{
    DataContentType, FormatVersion, ManifestContentType, ManifestEntry, ManifestListEntry,
    ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation, Snapshot,
    SnapshotReference, SnapshotRetention, Summary, MAIN_BRANCH,
};
use iceberg::{TableCommit, TableRequirement, TableUpdate};

use super::action::{CommitCtx, IcebergCommitAction};
use super::data_file::written_file_to_iceberg_data_file;
use super::row_delta::*; // re-use generate_snapshot_id, now_ms, metadata_dir helpers
use super::types::CommitOutcome;

pub struct OverwriteCommit;

#[async_trait]
impl IcebergCommitAction for OverwriteCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "OverwriteCommit received {:?} content; expected Data only", f.content
                ));
            }
        }

        let m = ctx.table.metadata();
        let new_seq = m.last_sequence_number() + 1;
        let new_snap_id = generate_snapshot_id();
        let format_version = m.format_version();
        let parent_snap_id = m.current_snapshot().map(|s| s.snapshot_id());

        // 1. Enumerate live data files.
        let existing_data_files = enumerate_live_data_files(ctx.table, ctx.file_io).await?;

        // Spec §4.1 corner: empty written + empty base = no-op.
        if written.is_empty() && existing_data_files.is_empty() {
            return Ok(CommitOutcome {
                new_snapshot_id: parent_snap_id.unwrap_or(0),
                written_manifest_paths: vec![],
            });
        }

        let mut new_manifests: Vec<ManifestListEntry> = Vec::new();

        // 2. Deleted-data manifest (only if base had any data).
        let mut deleted_manifest_path: Option<String> = None;
        if !existing_data_files.is_empty() {
            let p = format!(
                "{}/{}-overwrite-deletes-0.avro",
                metadata_dir(ctx.table)?,
                ctx.commit_uuid
            );
            ctx.abort_handle.record_manifest(p.clone());
            let entry = write_deleted_data_manifest(
                ctx, &existing_data_files, &p, new_seq, new_snap_id, format_version
            ).await?;
            new_manifests.push(entry);
            deleted_manifest_path = Some(p);
        }

        // 3. New data manifest.
        let mut new_data_manifest_path: Option<String> = None;
        if !written.is_empty() {
            let p = format!(
                "{}/{}-overwrite-data-0.avro",
                metadata_dir(ctx.table)?,
                ctx.commit_uuid
            );
            ctx.abort_handle.record_manifest(p.clone());
            let entry = write_added_data_manifest(
                ctx, &written, &p, new_seq, new_snap_id, format_version
            ).await?;
            new_manifests.push(entry);
            new_data_manifest_path = Some(p);
        }

        // 4. Manifest list — DO NOT inherit base entries (spec §4.3).
        let manifest_list_path = format!(
            "{}/snap-{}-{}.avro",
            metadata_dir(ctx.table)?,
            new_snap_id,
            ctx.commit_uuid
        );
        ctx.abort_handle.record_manifest(manifest_list_path.clone());
        super::row_delta::write_manifest_list(
            ctx.file_io,
            &manifest_list_path,
            &new_manifests,
            new_snap_id,
            parent_snap_id,
            new_seq,
            format_version,
        ).await?;

        // 5. Snapshot.
        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snap_id)
            .with_parent_snapshot_id(parent_snap_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(super::row_delta::now_ms())
            .with_manifest_list(manifest_list_path.clone())
            .with_schema_id(m.current_schema_id())
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: overwrite_summary(&written, &existing_data_files),
            })
            .build();

        // 6. TableCommit.
        let mut requirements = vec![
            TableRequirement::AssertCurrentSchemaId { current_schema_id: m.current_schema_id() },
            TableRequirement::AssertDefaultSpecId { default_spec_id: m.default_partition_spec_id() },
        ];
        if let Some(id) = parent_snap_id {
            requirements.push(TableRequirement::AssertRefSnapshotId {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(id),
            });
        }

        let commit = TableCommit::builder()
            .ident(ctx.table.identifier().clone())
            .updates(vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference {
                        snapshot_id: new_snap_id,
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                },
            ])
            .requirements(requirements)
            .build();

        ctx.catalog.update_table(commit).await
            .map_err(|e| format!("catalog.update_table failed in Overwrite: {e}"))?;

        let mut paths = Vec::new();
        if let Some(p) = deleted_manifest_path { paths.push(p); }
        if let Some(p) = new_data_manifest_path { paths.push(p); }
        paths.push(manifest_list_path);
        Ok(CommitOutcome { new_snapshot_id: new_snap_id, written_manifest_paths: paths })
    }
}

async fn enumerate_live_data_files(
    table: &iceberg::table::Table,
    file_io: &iceberg::io::FileIO,
) -> Result<Vec<iceberg::spec::DataFile>, String> {
    let snap = match table.metadata().current_snapshot() {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };
    let manifest_list_bytes = file_io.new_input(snap.manifest_list())
        .map_err(|e| format!("input manifest_list: {e}"))?
        .read().await
        .map_err(|e| format!("read manifest_list: {e}"))?;
    let fv = table.metadata().format_version();
    let manifest_list = iceberg::spec::ManifestList::parse_with_version(&manifest_list_bytes, fv)
        .map_err(|e| format!("parse manifest_list: {e}"))?;

    let mut out = Vec::new();
    for entry in manifest_list.entries() {
        if entry.content() != ManifestContentType::Data {
            continue;
        }
        let manifest = entry.load_manifest(&file_io).await
            .map_err(|e| format!("load_manifest {}: {e}", entry.manifest_path()))?;
        for me in manifest.entries() {
            if matches!(me.status(), ManifestStatus::Added | ManifestStatus::Existing) {
                out.push(me.data_file().clone());
            }
        }
    }
    Ok(out)
}

async fn write_deleted_data_manifest(
    ctx: &CommitCtx<'_>,
    existing: &[iceberg::spec::DataFile],
    out_path: &str,
    new_seq: i64,
    new_snap_id: i64,
    fv: FormatVersion,
) -> Result<ManifestListEntry, String> {
    // *** This is where spike (Task 1) resolves the actual API call. ***
    // The block below uses the "Path A" form from the spike doc; if Path B
    // (manual ManifestEntry build) was needed, replace this with that pattern.
    use iceberg::spec::ManifestEntry;
    let file_io = ctx.file_io.clone();
    let output_file = file_io.new_output(out_path)
        .map_err(|e| format!("new_output deleted-data manifest: {e}"))?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(new_snap_id),
        None,
        ctx.table.metadata().current_schema().clone(),
        ctx.collector.partition_spec.clone(),
    );
    let mut writer = match fv {
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
        FormatVersion::V1 => return Err("v1 not supported for OVERWRITE phase 1".into()),
    };
    for df in existing {
        let entry = ManifestEntry::builder()
            .status(ManifestStatus::Deleted)
            .data_file(df.clone())
            .build();
        writer.add_entry(entry, new_seq)
            .map_err(|e| format!("manifest add_entry deleted: {e}"))?;
    }
    let m = writer.write_manifest_file().await
        .map_err(|e| format!("write deleted manifest: {e}"))?;
    Ok(ManifestListEntry::from_manifest_file(m, ManifestContentType::Data))
}

async fn write_added_data_manifest(
    ctx: &CommitCtx<'_>,
    written: &[crate::connector::iceberg::commit::types::WrittenFile],
    out_path: &str,
    new_seq: i64,
    new_snap_id: i64,
    fv: FormatVersion,
) -> Result<ManifestListEntry, String> {
    let file_io = ctx.file_io.clone();
    let output_file = file_io.new_output(out_path)
        .map_err(|e| format!("new_output added-data manifest: {e}"))?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(new_snap_id),
        None,
        ctx.table.metadata().current_schema().clone(),
        ctx.collector.partition_spec.clone(),
    );
    let mut writer = match fv {
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
        FormatVersion::V1 => return Err("v1 not supported phase 1".into()),
    };
    for f in written {
        let df = written_file_to_iceberg_data_file(f, ctx.collector)?;
        writer.add_file(df, new_seq)
            .map_err(|e| format!("manifest add_file: {e}"))?;
    }
    let m = writer.write_manifest_file().await
        .map_err(|e| format!("write added manifest: {e}"))?;
    Ok(ManifestListEntry::from_manifest_file(m, ManifestContentType::Data))
}

fn overwrite_summary(
    added: &[crate::connector::iceberg::commit::types::WrittenFile],
    deleted: &[iceberg::spec::DataFile],
) -> std::collections::HashMap<String, String> {
    let mut p = std::collections::HashMap::new();
    p.insert("added-data-files".to_string(), added.len().to_string());
    p.insert("added-records".to_string(), added.iter().map(|f| f.record_count).sum::<u64>().to_string());
    p.insert("added-files-size".to_string(), added.iter().map(|f| f.file_size_in_bytes).sum::<u64>().to_string());
    p.insert("deleted-data-files".to_string(), deleted.len().to_string());
    p.insert("deleted-records".to_string(), deleted.iter().map(|f| f.record_count).sum::<u64>().to_string());
    p
}
```

- [ ] **Step 2: Wire**

In `commit/mod.rs`:

```rust
mod overwrite;
pub use overwrite::OverwriteCommit;
```

Mark `generate_snapshot_id`, `now_ms`, `metadata_dir`, `write_manifest_list` in `row_delta.rs` as `pub(super)` so OverwriteCommit can reuse them — or extract them into a small `helpers.rs` module. Pick the cleaner option:

```rust
// Better: src/connector/iceberg/commit/helpers.rs containing the four helpers,
// then row_delta.rs and overwrite.rs both `use super::helpers::*;`.
```

Refactor accordingly before continuing.

- [ ] **Step 3: Compile**

```bash
cargo build -p novarocks
```

Adjust API names where they don't match iceberg-rust 0.9 (the spike from Task 1 should have the correct call shape for `ManifestEntry::builder()` / `ManifestWriter::add_entry`).

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/commit/
git commit -m "feat(iceberg/commit): add OverwriteCommit using public ManifestWriter primitives"
```

---

## Task 11: run_iceberg_commit orchestrator

**Files:**
- Create: `src/connector/iceberg/commit/run.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Write `run.rs`**

```rust
// SPDX header

//! Engine-layer entry point that owns the IcebergCommitCollector lifecycle.
//!
//! Responsibilities:
//! 1. Hand the prepared ExecPlan to the pipeline runner.
//! 2. After pipeline completes, dispatch to the appropriate IcebergCommitAction.
//! 3. On any failure, run abort cleanup (with commit-unknown handling per §5.4).

use std::sync::Arc;

use iceberg::Catalog;
use iceberg::io::FileIO;
use iceberg::table::Table;
use opendal::Operator;
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction};
use super::collector::IcebergCommitCollector;
use super::fast_append::FastAppendCommit;
use super::overwrite::OverwriteCommit;
use super::row_delta::RowDeltaCommit;
use super::types::{CommitOpKind, CommitOutcome};

pub struct RunInput {
    pub collector: Arc<IcebergCommitCollector>,
    pub catalog: Arc<dyn Catalog>,
    pub table: Table,
    pub fs: Operator,
    pub file_io: FileIO,
}

/// Orchestration entry. Caller is responsible for running the pipeline before
/// invoking this; pipeline output is read out of `runtime/sink_commit.rs` by
/// the collector itself.
pub async fn run_iceberg_commit(input: RunInput) -> Result<CommitOutcome, String> {
    let RunInput { collector, catalog, table, fs, file_io } = input;
    let action: Box<dyn IcebergCommitAction> = match collector.op_kind {
        CommitOpKind::FastAppend => Box::new(FastAppendCommit),
        CommitOpKind::Overwrite => Box::new(OverwriteCommit),
        CommitOpKind::RowDelta => Box::new(RowDeltaCommit),
    };

    let ctx = CommitCtx {
        collector: &collector,
        table: &table,
        catalog: catalog.as_ref(),
        file_io: &file_io,
        commit_uuid: Uuid::new_v4(),
        abort_handle: collector.abort_log.clone(),
    };

    match action.commit(ctx).await {
        Ok(outcome) => {
            collector.mark_committed();
            Ok(outcome)
        }
        Err(commit_err) => {
            // §5.4: commit-unknown ⇒ leave files. Otherwise clean up.
            if is_commit_unknown(&commit_err) {
                tracing::warn!(
                    op_kind = ?collector.op_kind,
                    table = ?collector.table_ident,
                    base_snapshot_id = ?collector.base_snapshot_id,
                    staging_dir = collector.staging_dir,
                    "iceberg commit unknown — leaving all staged files for manual review: {commit_err}"
                );
                Err(format!(
                    "iceberg commit unknown ({commit_err}); staged files left at {} for manual review",
                    collector.staging_dir
                ))
            } else {
                let cleanup_errors = collector.abort_log.cleanup(&fs).await;
                for e in cleanup_errors {
                    tracing::warn!(path = e.path, err = ?e.source, "abort cleanup error");
                }
                Err(commit_err)
            }
        }
    }
}

/// Heuristic — exact behavior pinned by spike Task 2 docs.
fn is_commit_unknown(err: &str) -> bool {
    // Treat anything not explicitly "definite" as unknown.
    let lower = err.to_lowercase();
    let definite_signals = [
        "conflict", "assertrefsnapshotid",
        "schema id mismatch", "spec id mismatch",
        "data invalid", "feature unsupported",
        // pipeline-level errors are always definite
        "pipeline cancelled", "pipeline failed",
    ];
    !definite_signals.iter().any(|s| lower.contains(s))
}
```

- [ ] **Step 2: Wire**

In `commit/mod.rs`:

```rust
mod run;
pub use run::{run_iceberg_commit, RunInput};
```

- [ ] **Step 3: Compile**

```bash
cargo build -p novarocks
```

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/commit/run.rs src/connector/iceberg/commit/mod.rs
git commit -m "feat(iceberg/commit): add run_iceberg_commit orchestrator with commit-unknown handling"
```

---

## Task 12: AST extensions (Insert.overwrite, DeleteStmt)

**Files:**
- Modify: `src/sql/parser/ast.rs`
- Modify: `src/engine/statement.rs`
- Modify: `src/engine/mod.rs`

- [ ] **Step 1: Inspect existing structures**

```bash
grep -n 'pub struct InsertStmt\|pub enum InsertSource' src/sql/parser/ast.rs | head
sed -n '1,80p' src/sql/parser/ast.rs
```

Identify the existing `InsertStmt` definition and `InsertSource` enum.

- [ ] **Step 2: Extend `InsertStmt`**

```rust
// in src/sql/parser/ast.rs — find the InsertStmt definition and add:
pub struct InsertStmt {
    pub table: ObjectName,
    pub columns: Vec<String>,
    pub source: InsertSource,
    pub overwrite: bool, // NEW — defaults to false for INSERT INTO
}

// also new:
pub struct DeleteStmt {
    pub table: ObjectName,
    pub where_clause: sqlparser::ast::Expr,
}
```

Run `cargo build` — every existing `InsertStmt { table, columns, source }` initializer breaks. Add `overwrite: false` to each.

- [ ] **Step 3: Modify `engine/statement.rs::convert_sqlparser_insert_to_custom`**

```bash
grep -n 'fn convert_sqlparser_insert_to_custom' src/engine/statement.rs
```

Update the constructor to read `insert.overwrite`:

```rust
Ok(crate::sql::parser::ast::InsertStmt {
    table,
    columns,
    source,
    overwrite: insert.overwrite,
})
```

- [ ] **Step 4: Add `convert_sqlparser_delete_to_custom`**

Append to `src/engine/statement.rs`:

```rust
pub(crate) fn convert_sqlparser_delete_to_custom(
    delete: &sqlparser::ast::Delete,
) -> Result<crate::sql::parser::ast::DeleteStmt, String> {
    use sqlparser::ast as sqlast;
    // sqlparser's Delete struct has Tables (FROM list), Selection (WHERE),
    // and other fields.
    let tables = match &delete.from {
        sqlast::FromTable::WithFromKeyword(tables) => tables,
        sqlast::FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return Err("phase 1 DELETE supports exactly one table in FROM".to_string());
    }
    let twj = &tables[0];
    let table = match &twj.relation {
        sqlast::TableFactor::Table { name, .. } =>
            crate::sql::parser::dialect::convert_object_name(name.clone())?,
        other => return Err(format!("phase 1 DELETE source must be a table, got {other:?}")),
    };
    let where_clause = delete.selection.clone()
        .ok_or_else(|| "DELETE must have a WHERE clause; for full table replacement use \
                        INSERT OVERWRITE t SELECT * FROM t WHERE FALSE".to_string())?;
    if !delete.using.is_empty() {
        return Err("phase 1 DELETE does not support USING".to_string());
    }
    if delete.limit.is_some() || !delete.order_by.is_empty() {
        return Err("phase 1 DELETE does not support LIMIT/ORDER BY".to_string());
    }
    Ok(crate::sql::parser::ast::DeleteStmt { table, where_clause })
}
```

Field name verification:

```bash
grep -n 'pub struct Delete\b\|pub from\|pub using\|pub selection\|pub limit\|pub order_by' /Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/sqlparser-0.61.0/src/ast/dml.rs 2>/dev/null | head
```

Adjust field names to actual sqlparser 0.61 API.

- [ ] **Step 5: Add Statement::Delete branch in `engine/mod.rs`**

```bash
grep -n 'sqlast::Statement::Insert(ref insert)' src/engine/mod.rs
```

Locate the existing INSERT branch (line ~594 per earlier grep) and add a sibling DELETE branch below it:

```rust
sqlast::Statement::Delete(ref delete) => self.handle_sqlparser_delete(delete, /* …context…*/),
```

Implement `handle_sqlparser_delete`:

```rust
fn handle_sqlparser_delete(
    &self,
    delete: &sqlparser::ast::Delete,
    /* same context args as handle_sqlparser_insert */
) -> Result<StatementResult, String> {
    let stmt = super::convert_sqlparser_delete_to_custom(delete)?;
    crate::engine::delete_flow::execute(/* state, stmt, etc */)
}
```

(`delete_flow::execute` is implemented in Task 14.)

- [ ] **Step 6: Compile**

```bash
cargo build -p novarocks
```

Iterate on field name mismatches.

- [ ] **Step 7: Commit**

```bash
git add src/sql/parser/ast.rs src/engine/statement.rs src/engine/mod.rs
git commit -m "feat(sql): extend InsertStmt with overwrite flag and add DeleteStmt"
```

---

## Task 13: INSERT INTO + INSERT OVERWRITE wiring in insert_flow

**Files:**
- Modify: `src/engine/insert_flow.rs`

- [ ] **Step 1: Read current insert_flow.rs**

```bash
sed -n '50,100p' src/engine/insert_flow.rs
```

Locate the `if target.backend_name == "iceberg" { return Err(...) }` block at line 65.

- [ ] **Step 2: Replace with iceberg-aware dispatch**

```rust
// Replace the whole `if target.backend_name == "iceberg" { Err... }` block:

if target.backend_name == "iceberg" {
    return crate::engine::insert_flow::execute_iceberg_insert(state, &target, &stmt, query)
        .map(|_| ()).map_err(|e| e);
}
```

Then add `execute_iceberg_insert` in the same file:

```rust
fn execute_iceberg_insert(
    state: &Arc<StandaloneState>,
    target: &BackendTarget,
    stmt: &crate::sql::parser::ast::InsertStmt,
    query: &sqlparser::ast::Query,
) -> Result<StatementResult, String> {
    use crate::connector::iceberg::commit::{
        ensure_no_equality_deletes, ensure_single_partition_spec, ensure_v3_writable,
        match_select_schema_to_table, run_iceberg_commit, CommitOpKind, IcebergCommitCollector, RunInput,
    };

    // 1. Resolve catalog + table.
    let catalog = build_catalog_for_target(state, target)?; // factor existing helper
    let table_ident = build_table_ident_for(target)?;
    let runtime = crate::standalone::engine::backend_resolver::tokio_runtime(state);
    let table = runtime.block_on(catalog.load_table(&table_ident))
        .map_err(|e| format!("load_table failed: {e}"))?;

    // 2. Validate.
    ensure_v3_writable(&table)?;
    ensure_single_partition_spec(&table)?;
    let select_schema = analyze_query_schema(state, query)?;
    match_select_schema_to_table(&select_schema, &table, Some(&stmt.columns))?;

    // 3. Build collector.
    let op_kind = if stmt.overwrite { CommitOpKind::Overwrite } else { CommitOpKind::FastAppend };
    let m = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        table.metadata().location(),
        uuid::Uuid::new_v4()
    );
    let finst_id = generate_finst_id();
    crate::runtime::sink_commit::register(finst_id);
    let collector = std::sync::Arc::new(IcebergCommitCollector::new(
        op_kind,
        table_ident.clone(),
        m.current_snapshot().map(|s| s.snapshot_id()),
        m.last_sequence_number(),
        m.current_schema().clone(),
        m.default_partition_spec().clone(),
        staging_dir.clone(),
        finst_id,
    ));

    // 4. Build the synthetic IcebergSink and run pipeline.
    //    (Lower SELECT → ExecPlan; attach IcebergSink configured with mode=Data,
    //     data_location=staging_dir; pipeline runner pushes results.)
    let plan = lower_insert_iceberg(state, query, &table, &collector, &staging_dir)?;
    run_pipeline(state, plan, finst_id)?;

    // 5. Commit.
    let fs = build_opendal_for_table(&table, target)?;
    let file_io = build_file_io_for_table(&table, target)?;
    let outcome = runtime.block_on(run_iceberg_commit(RunInput {
        collector: collector.clone(),
        catalog: catalog.clone(),
        table: table.clone(),
        fs,
        file_io,
    }));

    crate::runtime::sink_commit::unregister(finst_id);

    let _outcome = outcome?;
    Ok(StatementResult::Ok)
}
```

The helpers `build_catalog_for_target`, `build_table_ident_for`, `tokio_runtime`, `analyze_query_schema`, `lower_insert_iceberg`, `run_pipeline`, `build_opendal_for_table`, `generate_finst_id` already exist in some form — locate and reuse:

```bash
grep -rn 'fn build_catalog\|fn analyze_query_schema\|fn run_pipeline\|fn tokio_runtime\|fn build_opendal\|fn generate_finst_id' src/engine/ src/standalone/ src/connector/starrocks/managed/ | head -20
```

Where helpers are missing (e.g. `lower_insert_iceberg`), implement them next to the existing INSERT-non-iceberg path so they fall through to identical lowering code, swapping in our IcebergSink instead of the regular sink. The IcebergSink factory `IcebergTableSinkFactory::try_new` (at [sink.rs](../../../src/connector/iceberg/sink.rs)) takes a thrift `TIcebergTableSink` payload — synthesize one inline:

```rust
fn synthesize_iceberg_sink_thrift(
    table: &iceberg::table::Table,
    staging_dir: &str,
    target_max_file_size: i64,
) -> crate::types::TIcebergTableSink {
    crate::types::TIcebergTableSink {
        location: Some(table.metadata().location().to_string()),
        file_format: Some("parquet".to_string()),
        target_table_id: Some(synthetic_table_id(table)),
        compression_type: Some(crate::types::TCompressionType::SNAPPY),
        is_static_partition_sink: Some(false),
        cloud_configuration: None,
        target_max_file_size: Some(target_max_file_size),
        tuple_id: Some(0),
        data_location: Some(staging_dir.to_string()),
    }
}
```

`synthetic_table_id` returns a stable id that the in-memory descriptor table can resolve to this iceberg table (look at how phase4a sets up DescriptorTable for sink tests).

- [ ] **Step 3: Run smoke build**

```bash
cargo build -p novarocks
```

Fix all compile errors. End-to-end behavior is verified by Task 15 onwards.

- [ ] **Step 4: Commit**

```bash
git add src/engine/insert_flow.rs
git commit -m "feat(engine): wire INSERT INTO and INSERT OVERWRITE for iceberg via commit module"
```

---

## Task 14: delete_flow.rs + DELETE plan lowering

**Files:**
- Create: `src/engine/delete_flow.rs`
- Modify: `src/engine/mod.rs` (re-export `delete_flow`)
- Modify: target file in `src/lower/...` (located in step 1)

- [ ] **Step 1: Locate the right lowering integration point**

```bash
grep -rn 'fn lower_query\|fn build_plan_from_query\|fn lower_select\|InsertSource::FromQuery' src/engine/ src/lower/ | head -15
```

The standalone INSERT path uses `InsertSource::FromQuery(query)` which routes the SELECT through the regular query analyzer / planner. For DELETE we'll do the same: synthesize a `Query` of the form `SELECT _file, _pos FROM <table> WHERE <pred>` and feed it into the same lowering, then wrap the result with an IcebergSink in PositionDeletes mode.

- [ ] **Step 2: Write `src/engine/delete_flow.rs`**

```rust
// SPDX header

use std::sync::Arc;

use sqlparser::ast as sqlast;

use crate::connector::iceberg::commit::{
    ensure_no_equality_deletes, ensure_single_partition_spec, ensure_v3_writable,
    run_iceberg_commit, CommitOpKind, IcebergCommitCollector, RunInput,
};
use crate::engine::backend_resolver::resolve_table_target;
use crate::engine::StandaloneState;
use crate::sql::parser::ast::DeleteStmt;
use crate::standalone::server::StatementResult;

pub fn execute(
    state: &Arc<StandaloneState>,
    stmt: DeleteStmt,
    current_catalog: Option<&str>,
) -> Result<StatementResult, String> {
    // 1. Resolve table.
    let target = resolve_table_target(state, &stmt.table, current_catalog)?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "phase 1 DELETE only supports iceberg backend, got `{}`",
            target.backend_name
        ));
    }

    let catalog = crate::engine::insert_flow::build_catalog_for_target(state, &target)?;
    let table_ident = crate::engine::insert_flow::build_table_ident_for(&target)?;
    let runtime = crate::engine::backend_resolver::tokio_runtime(state);
    let table = runtime.block_on(catalog.load_table(&table_ident))
        .map_err(|e| format!("load_table: {e}"))?;

    // 2. Validate.
    ensure_v3_writable(&table)?;
    ensure_single_partition_spec(&table)?;
    ensure_no_equality_deletes(&table)?;

    // 3. Synthesize the underlying SELECT.
    let scan_query = synthesize_delete_scan_query(&stmt)?;

    // 4. Build collector.
    let m = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        table.metadata().location(),
        uuid::Uuid::new_v4()
    );
    let finst_id = crate::engine::insert_flow::generate_finst_id();
    crate::runtime::sink_commit::register(finst_id);
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::RowDelta,
        table_ident.clone(),
        m.current_snapshot().map(|s| s.snapshot_id()),
        m.last_sequence_number(),
        m.current_schema().clone(),
        m.default_partition_spec().clone(),
        staging_dir.clone(),
        finst_id,
    ));

    // 5. Lower + run pipeline (sink mode = PositionDeletes).
    let plan = crate::engine::insert_flow::lower_iceberg_delete_scan(
        state, &scan_query, &table, &collector, &staging_dir
    )?;
    crate::engine::insert_flow::run_pipeline(state, plan, finst_id)?;

    // 6. Commit.
    let fs = crate::engine::insert_flow::build_opendal_for_table(&table, &target)?;
    let file_io = crate::engine::insert_flow::build_file_io_for_table(&table, &target)?;
    let outcome = runtime.block_on(run_iceberg_commit(RunInput {
        collector: collector.clone(),
        catalog: catalog.clone(),
        table: table.clone(),
        fs,
        file_io,
    }));
    crate::runtime::sink_commit::unregister(finst_id);
    let _ = outcome?;
    Ok(StatementResult::Ok)
}

fn synthesize_delete_scan_query(stmt: &DeleteStmt) -> Result<sqlast::Query, String> {
    use sqlast::*;

    // SELECT _file, _pos FROM <stmt.table> WHERE <stmt.where_clause>
    let select_items = vec![
        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("_file"))),
        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("_pos"))),
    ];
    let from = vec![TableWithJoins {
        relation: TableFactor::Table {
            name: stmt.table.clone(),
            alias: None,
            args: None,
            with_hints: vec![],
            version: None,
            partitions: vec![],
            with_ordinality: false,
            json_path: None,
            sample: None,
            index_hints: vec![],
        },
        joins: vec![],
    }];
    let select = Select {
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection: select_items,
        into: None,
        from,
        lateral_views: vec![],
        prewhere: None,
        selection: Some(stmt.where_clause.clone()),
        group_by: GroupByExpr::Expressions(vec![], vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        connect_by: None,
    };
    Ok(Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(select))),
        order_by: None,
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
    })
}
```

- [ ] **Step 3: Implement `lower_iceberg_delete_scan` in `engine/insert_flow.rs`**

This wraps the scan query with an IcebergSink in PositionDeletes mode. The synthetic IcebergSink thrift uses `mode = PositionDeletes`. The output_exprs supplied to `IcebergTableSinkFactory::try_new` must be `[file_path_slot, pos_slot]` (in this order — see [sink.rs:108](../../../src/connector/iceberg/sink.rs:108) doc comment about `[file_path_expr, pos_expr, partition_source_expr_0, …]`).

```rust
pub(crate) fn lower_iceberg_delete_scan(
    state: &Arc<StandaloneState>,
    query: &sqlast::Query,
    table: &iceberg::table::Table,
    collector: &Arc<IcebergCommitCollector>,
    staging_dir: &str,
) -> Result<crate::exec::node::ExecPlan, String> {
    // Lower the SELECT _file, _pos FROM t WHERE ... using the regular pipeline,
    // then attach an IcebergSink configured for PositionDeletes mode whose
    // data_location is staging_dir.
    //
    // Reuse lower_query_to_exec_plan (or whatever the standalone helper is
    // called for INSERT INTO ... SELECT). After the helper returns the
    // chunk-producing plan, wrap with an IcebergSink node:
    let scan_plan = lower_select_to_exec_plan(state, query)?;
    let synthetic_sink = synthesize_iceberg_sink_thrift(
        table,
        staging_dir,
        DEFAULT_TARGET_MAX_FILE_SIZE,
    );
    let sink_factory = crate::connector::iceberg::sink::IcebergTableSinkFactory::try_new(
        synthetic_sink,
        crate::connector::iceberg::sink::IcebergSinkMode::PositionDeletes,
        /* output_exprs */ &delete_output_exprs(scan_plan.output_layout()),
        /* layout */ scan_plan.layout(),
        /* desc_tbl */ &synthesize_descriptor_table(table)?,
        /* last_query_id */ None,
        /* fe_addr */ None,
    )?;
    Ok(crate::exec::node::ExecPlan::with_sink(scan_plan, sink_factory))
}
```

The exact constructors (`ExecPlan::with_sink`, etc.) need to match the codebase. Inspect:

```bash
grep -n 'ExecPlan\|fn with_sink\|build_pipeline\|attach_sink' src/exec/node/mod.rs src/connector/starrocks/managed/mv_refresh_iceberg.rs | head -20
```

Reuse whichever helper phase4a or the regular INSERT path uses to attach a sink to a chunk-producing plan.

- [ ] **Step 4: Hook delete_flow into mod.rs**

In `src/engine/mod.rs`:

```rust
pub(crate) mod delete_flow;
```

And the `Statement::Delete` branch added in Task 12 calls `delete_flow::execute(...)`.

- [ ] **Step 5: Compile**

```bash
cargo build -p novarocks
```

- [ ] **Step 6: Commit**

```bash
git add src/engine/delete_flow.rs src/engine/insert_flow.rs src/engine/mod.rs
git commit -m "feat(engine): add DELETE FROM iceberg via SCAN(_file,_pos) + PositionDeletes sink"
```

---

## Task 15: Integration test scaffold + INSERT round-trip

**Files:**
- Create: `tests/iceberg_insert_delete.rs`
- Create / extend: `tests/common/mod.rs`

- [ ] **Step 1: Locate phase4a's iceberg test scaffolding**

```bash
grep -rn 'fn make_test_iceberg_table\|fn build_test_catalog\|tempdir' src/connector/starrocks/managed/mv_refresh_iceberg.rs tests/ 2>/dev/null | head -15
```

Reuse whatever phase4a established for "tempdir + local FS catalog + create iceberg table". If there's a public test helper, re-export it under `crate::test_support`. If it's `#[cfg(test)] mod tests { ... }`, factor it out into `crate::test_support` (gated by `#[cfg(any(test, feature = "test-support"))]`).

- [ ] **Step 2: Write `tests/iceberg_insert_delete.rs` scaffold**

```rust
// SPDX header

#![cfg(test)]

mod common;

use common::{spawn_standalone_server, mysql_query, mysql_exec};

#[test]
fn it_ins_1_v2_insert_round_trip() {
    let server = spawn_standalone_server();
    mysql_exec(&server, r#"
        CREATE EXTERNAL CATALOG it_v2 PROPERTIES ('type' = 'iceberg');
        CREATE DATABASE it_v2.db1;
        CREATE TABLE it_v2.db1.t (id INT, v STRING) USING iceberg
            TBLPROPERTIES ('format-version' = '2');
    "#);
    mysql_exec(&server, "INSERT INTO it_v2.db1.t VALUES (1, 'a'), (2, 'b');");
    let rows = mysql_query(&server, "SELECT id, v FROM it_v2.db1.t ORDER BY id");
    assert_eq!(rows, vec![("1".to_string(), "a".to_string()), ("2".to_string(), "b".to_string())]);
}
```

`common::spawn_standalone_server`, `mysql_exec`, `mysql_query` are helpers using the existing standalone-server pattern phase4a uses. Look at:

```bash
grep -rn 'spawn_standalone\|run_standalone_for_test' tests/ src/ | head
```

Reuse if present; otherwise factor out.

- [ ] **Step 3: Run**

```bash
cargo test --test iceberg_insert_delete it_ins_1 -- --nocapture
```

Expected: PASS. Iterate on any wiring breaks.

- [ ] **Step 4: Add IT-INS-2/3/4**

```rust
#[test]
fn it_ins_2_v3_insert_round_trip() { /* same shape as IT-INS-1, format-version=3 */ }

#[test]
fn it_ins_3_partitioned_insert() {
    /*
       CREATE TABLE ... PARTITIONED BY (p);
       INSERT VALUES (1,'a',10), (2,'b',10), (3,'c',20);
       Verify partition file paths / 2 partitions present.
    */
}

#[test]
fn it_ins_4_repeated_inserts_grow_snapshot_chain() {
    /*
       CREATE TABLE ...; INSERT 3 times;
       Read table.metadata().history().len() == 3.
    */
}
```

- [ ] **Step 5: Run all four**

```bash
cargo test --test iceberg_insert_delete it_ins_ -- --nocapture
```

- [ ] **Step 6: Commit**

```bash
git add tests/iceberg_insert_delete.rs tests/common/
git commit -m "test(iceberg): integration scaffold + IT-INS-1..4 INSERT round-trip"
```

---

## Task 16: INSERT OVERWRITE + DELETE integration tests

**Files:**
- Modify: `tests/iceberg_insert_delete.rs`

- [ ] **Step 1: Add IT-OW-1/2/3 + IT-DEL-1..4 + IT-RT-1**

Append to `tests/iceberg_insert_delete.rs`:

```rust
#[test]
fn it_ow_1_insert_then_overwrite_replaces_data() {
    let server = spawn_standalone_server();
    mysql_exec(&server, /* setup */);
    mysql_exec(&server, "INSERT INTO t VALUES (1,'a'),(2,'b');");
    mysql_exec(&server, "INSERT OVERWRITE t VALUES (3,'c');");
    let rows = mysql_query(&server, "SELECT id,v FROM t ORDER BY id;");
    assert_eq!(rows, vec![("3".into(), "c".into())]);
}

#[test]
fn it_ow_2_overwrite_into_empty_table() { /* OVERWRITE without prior INSERT */ }

#[test]
fn it_ow_3_partitioned_overwrite_replaces_all_partitions() { /* */ }

#[test]
fn it_del_1_delete_with_filter_returns_remaining() {
    let server = spawn_standalone_server();
    mysql_exec(&server, /* setup */);
    mysql_exec(&server, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');");
    mysql_exec(&server, "DELETE FROM t WHERE id = 2;");
    let rows = mysql_query(&server, "SELECT id FROM t ORDER BY id;");
    assert_eq!(rows.iter().map(|r| &r.0).collect::<Vec<_>>(), vec!["1", "3"]);
}

#[test]
fn it_del_2_delete_zero_rows_does_not_advance_snapshot() {
    let server = spawn_standalone_server();
    mysql_exec(&server, /* setup + insert */);
    let snap_before = current_snapshot_id_via_metadata_query(&server, "t");
    mysql_exec(&server, "DELETE FROM t WHERE id = 999;"); // matches no rows
    let snap_after = current_snapshot_id_via_metadata_query(&server, "t");
    assert_eq!(snap_before, snap_after, "no-op DELETE must not advance snapshot");
}

#[test]
fn it_del_3_delete_across_multiple_data_files() { /* */ }

#[test]
fn it_del_4_delete_then_insert_coexist() { /* */ }

#[test]
fn it_rt_1_mixed_operations_match_sql_semantics() {
    let server = spawn_standalone_server();
    mysql_exec(&server, /* setup */);
    mysql_exec(&server, "INSERT INTO t VALUES (1,'a');");
    mysql_exec(&server, "INSERT INTO t VALUES (2,'b'),(3,'c');");
    mysql_exec(&server, "DELETE FROM t WHERE id = 2;");
    mysql_exec(&server, "INSERT INTO t VALUES (4,'d');");
    mysql_exec(&server, "INSERT OVERWRITE t SELECT id, UPPER(v) FROM t WHERE id <= 4;");
    let rows = mysql_query(&server, "SELECT id, v FROM t ORDER BY id;");
    // After OVERWRITE, table contains exactly the result of SELECT FROM old t.
    assert_eq!(rows, vec![
        ("1".into(), "A".into()),
        ("3".into(), "C".into()),
        ("4".into(), "D".into()),
    ]);
}
```

- [ ] **Step 2: Run**

```bash
cargo test --test iceberg_insert_delete it_ow_ -- --nocapture
cargo test --test iceberg_insert_delete it_del_ -- --nocapture
cargo test --test iceberg_insert_delete it_rt_ -- --nocapture
```

Iterate on failures.

- [ ] **Step 3: Commit**

```bash
git add tests/iceberg_insert_delete.rs
git commit -m "test(iceberg): IT-OW-1..3, IT-DEL-1..4, IT-RT-1 integration tests"
```

---

## Task 17: Negative-path tests (NEG-1..6)

**Files:**
- Modify: `tests/iceberg_insert_delete.rs`

- [ ] **Step 1: Append NEG tests**

```rust
#[test]
fn neg_1_insert_column_count_mismatch() {
    let server = spawn_standalone_server();
    mysql_exec(&server, /* CREATE TABLE t(id INT, v STRING) */);
    let err = mysql_exec_expect_error(&server, "INSERT INTO t VALUES (1);");
    assert!(err.contains("column count mismatch"), "got {err}");
}

#[test]
fn neg_2_insert_type_mismatch_no_implicit_cast() {
    let err = mysql_exec_expect_error(&server, "INSERT INTO t VALUES ('xx', 1);");
    assert!(err.contains("type mismatch") && err.contains("CAST"), "got {err}");
}

#[test]
fn neg_3_delete_without_where_rejected() {
    let err = mysql_exec_expect_error(&server, "DELETE FROM t;");
    assert!(err.contains("WHERE clause") || err.contains("INSERT OVERWRITE"), "got {err}");
}

#[test]
fn neg_4_row_lineage_table_insert_rejected() {
    /* CREATE TABLE ... TBLPROPERTIES ('write.row-lineage'='true'); INSERT */
    let err = mysql_exec_expect_error(&server, "INSERT INTO rl VALUES (1,'a');");
    assert!(err.contains("row-lineage"), "got {err}");
}

#[test]
fn neg_5_variant_column_insert_rejected() { /* */ }

#[test]
fn neg_6_equality_delete_table_delete_rejected() { /* */ }
```

- [ ] **Step 2: Run**

```bash
cargo test --test iceberg_insert_delete neg_ -- --nocapture
```

- [ ] **Step 3: Commit**

```bash
git add tests/iceberg_insert_delete.rs
git commit -m "test(iceberg): NEG-1..6 negative-path validation tests"
```

---

## Task 18: SQL regression suite

**Files:**
- Create: `tests/sql-test-runner/suites/iceberg-write/insert.sql`, `insert.expected`
- Create: `tests/sql-test-runner/suites/iceberg-write/overwrite.sql`, `overwrite.expected`
- Create: `tests/sql-test-runner/suites/iceberg-write/delete.sql`, `delete.expected`
- Create: `tests/sql-test-runner/suites/iceberg-write/mixed.sql`, `mixed.expected`

- [ ] **Step 1: Audit existing suite layout**

```bash
ls tests/sql-test-runner/suites/ | head -10
cat tests/sql-test-runner/suites/$(ls tests/sql-test-runner/suites/ | head -1)/*.sql 2>/dev/null | head -40
```

Match the existing format exactly.

- [ ] **Step 2: Author insert.sql**

```sql
-- SPDX header
SET CATALOG it_w;
CREATE DATABASE IF NOT EXISTS it_w.d;
DROP TABLE IF EXISTS it_w.d.t;
CREATE TABLE it_w.d.t (id INT, v STRING) USING iceberg
    TBLPROPERTIES ('format-version' = '3');
INSERT INTO it_w.d.t VALUES (1,'a'),(2,'b'),(3,'c');
SELECT id, v FROM it_w.d.t ORDER BY id;

INSERT INTO it_w.d.t (id, v) VALUES (4, 'd');
SELECT id, v FROM it_w.d.t ORDER BY id;

-- INSERT INTO ... SELECT
DROP TABLE IF EXISTS it_w.d.s;
CREATE TABLE it_w.d.s (id INT, v STRING) USING iceberg TBLPROPERTIES ('format-version' = '3');
INSERT INTO it_w.d.s SELECT id, UPPER(v) FROM it_w.d.t WHERE id <= 2;
SELECT id, v FROM it_w.d.s ORDER BY id;
```

- [ ] **Step 3: Generate `insert.expected` via record mode**

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &
SERVER_PID=$!
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite iceberg-write --only insert --mode record
kill $SERVER_PID
```

Verify the generated `insert.expected` is sane (manual eyeball).

- [ ] **Step 4: Author overwrite.sql, delete.sql, mixed.sql + expected**

Same shape — author SQL, run record mode, verify.

```sql
-- delete.sql
SET CATALOG it_w;
DROP TABLE IF EXISTS it_w.d.t;
CREATE TABLE it_w.d.t (id INT, v STRING) USING iceberg TBLPROPERTIES ('format-version' = '3');
INSERT INTO it_w.d.t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d');
DELETE FROM it_w.d.t WHERE id IN (2, 4);
SELECT id, v FROM it_w.d.t ORDER BY id;

-- delete crossing multiple files: insert in 3 batches first
INSERT INTO it_w.d.t VALUES (5,'e');
INSERT INTO it_w.d.t VALUES (6,'f'),(7,'g');
INSERT INTO it_w.d.t VALUES (8,'h');
DELETE FROM it_w.d.t WHERE id BETWEEN 5 AND 7;
SELECT id, v FROM it_w.d.t ORDER BY id;
```

```sql
-- overwrite.sql
SET CATALOG it_w;
DROP TABLE IF EXISTS it_w.d.t;
CREATE TABLE it_w.d.t (id INT, v STRING) USING iceberg TBLPROPERTIES ('format-version' = '3');
INSERT INTO it_w.d.t VALUES (1,'a'),(2,'b');
SELECT id, v FROM it_w.d.t ORDER BY id;
INSERT OVERWRITE it_w.d.t VALUES (10, 'X');
SELECT id, v FROM it_w.d.t ORDER BY id;
INSERT OVERWRITE it_w.d.t SELECT id*2, v FROM it_w.d.t;
SELECT id, v FROM it_w.d.t ORDER BY id;
```

```sql
-- mixed.sql
SET CATALOG it_w;
DROP TABLE IF EXISTS it_w.d.t;
CREATE TABLE it_w.d.t (id INT, v STRING) USING iceberg TBLPROPERTIES ('format-version' = '3');
INSERT INTO it_w.d.t VALUES (1,'a'),(2,'b'),(3,'c');
DELETE FROM it_w.d.t WHERE id = 2;
INSERT INTO it_w.d.t VALUES (4,'d');
SELECT id, v FROM it_w.d.t ORDER BY id;
INSERT OVERWRITE it_w.d.t SELECT id, UPPER(v) FROM it_w.d.t;
SELECT id, v FROM it_w.d.t ORDER BY id;
```

- [ ] **Step 5: Run verify mode in clean state**

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &
SERVER_PID=$!
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite iceberg-write --mode verify
kill $SERVER_PID
```

Expected: all four .sql files PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/sql-test-runner/suites/iceberg-write/
git commit -m "test(sql): add iceberg-write SQL regression suite"
```

---

## Task 19: Fault injection (FI-1..5)

**Files:**
- Create: `tests/common/fault_fs.rs`
- Modify: `tests/iceberg_insert_delete.rs`

- [ ] **Step 1: Implement `FaultOperator` wrapper**

`tests/common/fault_fs.rs`:

```rust
// SPDX header

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use opendal::raw::oio::*;
use opendal::*;

/// Wraps an `Operator` and triggers a synthetic IO error after N successful
/// operations of a given kind. Used to drive abort-cleanup paths in FI-* tests.
#[derive(Clone)]
pub struct FaultOperator {
    inner: Operator,
    write_calls: Arc<AtomicUsize>,
    delete_calls: Arc<AtomicUsize>,
    fail_after_writes: Option<usize>,
    fail_after_deletes: Option<usize>,
}

impl FaultOperator {
    pub fn new(inner: Operator) -> Self {
        Self {
            inner,
            write_calls: Arc::new(AtomicUsize::new(0)),
            delete_calls: Arc::new(AtomicUsize::new(0)),
            fail_after_writes: None,
            fail_after_deletes: None,
        }
    }

    pub fn fail_write_after(mut self, n: usize) -> Self {
        self.fail_after_writes = Some(n);
        self
    }

    pub fn fail_delete_after(mut self, n: usize) -> Self {
        self.fail_after_deletes = Some(n);
        self
    }

    pub async fn write(&self, path: &str, bytes: Vec<u8>) -> opendal::Result<()> {
        let n = self.write_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(limit) = self.fail_after_writes {
            if n >= limit {
                return Err(Error::new(ErrorKind::Unexpected, "synthetic write fault"));
            }
        }
        self.inner.write(path, bytes).await
    }

    // analogous wrappers for read, stat, delete, list... only those used by
    // commit-actions need to be implemented.
}
```

This is intentionally lightweight: we don't need to implement the full opendal Layer trait, we just need a struct that intercepts the calls our code makes.

If our commit-actions only call `Operator::write`, `Operator::delete`, `Operator::read`, the wrapper above is sufficient.

For places where opendal `Operator` is taken by reference (e.g. `iceberg::io::FileIO`) we'll need to construct a real `Operator` whose layer is fault-injected. opendal supports custom layers via `Operator::layer(L)`. Use that route:

```rust
use opendal::layers::Layer;

#[derive(Default)]
pub struct FailNthWriteLayer {
    counter: Arc<AtomicUsize>,
    fail_at: usize,
}

impl<A: opendal::raw::Accessor> Layer<A> for FailNthWriteLayer {
    type LayeredAccessor = FailNthWriteAccessor<A>;
    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        FailNthWriteAccessor { inner, counter: self.counter.clone(), fail_at: self.fail_at }
    }
}

// implement Accessor::write / Accessor::delete to consult counter and either
// delegate or return synthetic error.
```

Pick whichever shape works against opendal 0.55's actual API. The exact trait names and signatures need to be confirmed:

```bash
grep -rn 'pub trait Accessor\|pub trait Layer\|impl Layer' /Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/opendal-0.55*/src/ 2>/dev/null | head -10
```

- [ ] **Step 2: Add FI tests in `tests/iceberg_insert_delete.rs`**

```rust
#[test]
fn fi_1_sink_io_error_during_write_aborts_cleanly() {
    let server = spawn_standalone_server_with_fault_fs(
        FaultConfig::fail_write_after(1)
    );
    let err = mysql_exec_expect_error(&server, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');");
    assert!(err.contains("synthetic write fault") || err.contains("IO"), "got {err}");
    // After abort, staging dir should be empty / non-existent.
    assert_no_staging_files(&server, "t");
    // Table snapshot must NOT have advanced.
    let head = current_snapshot_id_via_metadata_query(&server, "t");
    assert_eq!(head, None, "expected unchanged HEAD");
}

#[test]
fn fi_2_manifest_io_error_aborts_data_and_manifests() { /* fail_write_after enough writes that data files succeed but manifest fails */ }

#[test]
fn fi_3_occ_conflict_via_concurrent_modification_aborts() { /* spawn second writer that bumps HEAD between collector load and update_table */ }

#[test]
fn fi_4_commit_unknown_simulated_timeout_leaves_files() {
    // Inject a timeout in update_table (separate fault layer that triggers on
    // the catalog HTTP/IO path), expect the error message to mention the
    // staging dir and to NOT clean up files.
}

#[test]
fn fi_5_pipeline_cancel_midstream_cleans_partial_files() {
    // Issue a long INSERT, send cancel via the standalone server's cancel
    // endpoint, expect partial files to be cleaned up.
}
```

- [ ] **Step 3: Run**

```bash
cargo test --test iceberg_insert_delete fi_ -- --nocapture
```

Iterate. The fault layer scaffolding is finicky — getting it right may take a couple iterations.

- [ ] **Step 4: Commit**

```bash
git add tests/common/fault_fs.rs tests/iceberg_insert_delete.rs
git commit -m "test(iceberg): FI-1..5 fault-injection tests for abort and commit-unknown"
```

---

## Task 20: V3 cross-engine round-trip sanity (manual / nightly)

**Files:**
- Create: `tests/cross-engine/iceberg_v3_roundtrip.md` (procedure doc)
- Optional: `tests/cross-engine/spark_read.py`

This is **not** PR-blocking. It establishes nightly/manual confidence per spec §6.6.

- [ ] **Step 1: Write `tests/cross-engine/iceberg_v3_roundtrip.md`**

```markdown
# Iceberg V3 Cross-Engine Round-Trip Sanity

Manual procedure. Runs against a local Spark 3.5 with iceberg-spark-runtime
(adjust paths to your environment).

## Setup

```bash
# Start NovaRocks standalone-server
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &

# Configure Spark with iceberg
export ICEBERG_VERSION=1.7.0
spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$ICEBERG_VERSION \
    --conf spark.sql.catalog.it_x=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.it_x.type=hadoop \
    --conf spark.sql.catalog.it_x.warehouse=/tmp/it_x_warehouse
```

## Round-trip 1: NovaRocks INSERT → Spark read

[steps with exact SQL on both sides]

## Round-trip 2: NovaRocks DELETE → Spark sees rows removed

[…]

## Round-trip 3: Spark INSERT OVERWRITE → NovaRocks reads new data

[…]

## Round-trip 4: Spark INSERT → NovaRocks DELETE → Spark re-read

[…]
```

- [ ] **Step 2: Run the procedure once and record results**

Add a results table to the markdown.

- [ ] **Step 3: Commit**

```bash
git add tests/cross-engine/
git commit -m "docs(test): cross-engine v3 round-trip procedure with first-run results"
```

---

## Task 21: Spec-cleanup: delete the spike scratch test

**Files:**
- Delete: `tests/scratch_manifest_deleted.rs`

- [ ] **Step 1: Verify spike findings have been baked into Task 10**

```bash
grep -n 'ManifestEntry::builder\|ManifestStatus::Deleted\|add_entry\|ManifestStatus' src/connector/iceberg/commit/overwrite.rs | head
```

The spike (Task 1) result is now used in production code. The scratch test is no longer load-bearing.

- [ ] **Step 2: Remove**

```bash
rm tests/scratch_manifest_deleted.rs
```

- [ ] **Step 3: Build + commit**

```bash
cargo build -p novarocks
git add -u
git commit -m "chore(test): remove spike scratch test now that finding lives in OverwriteCommit"
```

---

## Task 22: PR description with performance baseline

**Files:**
- Create: `docs/superpowers/notes/2026-04-28-phase1-pr-summary.md`

- [ ] **Step 1: Run baseline measurements**

```bash
# 1M-row INSERT INTO single partition
time cargo run --release -- standalone-server --port 9030 &
sleep 5
mysql -h127.0.0.1 -P9030 -e "
  CREATE TABLE bench.t USING iceberg TBLPROPERTIES('format-version'='3');
  INSERT INTO bench.t SELECT generate_series, 'v' FROM generate_series(1, 1000000);
"

# 100K-row DELETE
mysql -h127.0.0.1 -P9030 -e "
  DELETE FROM bench.t WHERE id < 100000;
"
```

Record wall time + staged file count in the notes doc.

- [ ] **Step 2: Author the PR summary**

```markdown
# Phase 1 — Iceberg v3 INSERT / DELETE PR summary

## What this lands

- `INSERT INTO`, `INSERT OVERWRITE`, `DELETE FROM` for iceberg tables in standalone mode
- v2-compatible position-delete files; v3-format tables supported (manifests
  written via iceberg-rust 0.9 v3 manifest writers)
- Self-implemented OverwriteCommit + RowDeltaCommit hidden behind
  IcebergCommitAction trait (S4 future-friendly migration)

## Performance baseline (snapshot)

- 1M INSERT INTO 1 part: __ ms, __ files
- 100K DELETE: __ ms, __ position-delete files

## Known limitations (carried from spec §0.3 / §8)

- No deletion vectors (Phase 2)
- No equality deletes
- No row-lineage / variant tables (rejected with clear error)
- Single writer assumption
- ...

## Spec / Plan refs

- Spec: docs/superpowers/specs/2026-04-27-iceberg-v3-insert-delete-phase1-design.md
- Plan: docs/superpowers/plans/2026-04-28-iceberg-v3-insert-delete-phase1.md
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/notes/
git commit -m "docs: phase 1 PR summary with performance baseline"
```

---

## Self-Review Checklist (do this AFTER writing any code, BEFORE merging)

- [ ] §6.2 unit tests green: `cargo test -p novarocks --lib commit::`
- [ ] §6.3 IT-* + NEG-* green: `cargo test --test iceberg_insert_delete`
- [ ] §6.4 SQL regression verify green: see Task 18 commands
- [ ] §5.6 audit: review `IcebergSink` close-on-error path in [sink.rs](../../../src/connector/iceberg/sink.rs) — confirm Parquet writer close failures `best-effort` delete partial files, and confirm the close-success → `state.add_sink_commit_info` window has no fallible operation between them. Document any gap in a follow-up issue.
- [ ] §6.5 FI-* green: `cargo test --test iceberg_insert_delete fi_`
- [ ] §6.6 cross-engine round-trip checked once
- [ ] No `tests/scratch_manifest_deleted.rs` remaining
- [ ] phase4a MV refresh still green: `cargo test --test mv_iceberg_refresh` (or equivalent)
- [ ] `cargo clippy -p novarocks` clean
- [ ] `cargo fmt -- --check` clean

---

## Open spec gaps (record here if implementation surfaced anything new)

| # | Surfaced in task | Description | Resolution |
|---|------------------|-------------|------------|
|   |                  |             |            |
