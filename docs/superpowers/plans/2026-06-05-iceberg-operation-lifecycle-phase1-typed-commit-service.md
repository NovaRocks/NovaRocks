# Iceberg Operation Lifecycle Phase 1 Typed Commit Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `run_iceberg_commit`'s internal string-based commit failure classification with a typed commit service facade while preserving the existing `Result<CommitOutcome, String>` API for current callers.

**Architecture:** Add a focused `commit::service` module that owns typed commit outcome/error concepts, cleanup outcome summaries, recovery evidence, and the commit-error classifier. Refactor `commit::run` so `run_iceberg_commit_typed` returns `Result<CommitOutcome, CommitServiceError>`, and keep `run_iceberg_commit` as a compatibility wrapper that formats the typed error into the old user-facing string. This first PR does not introduce operation persistence or migrate MV refresh repository state; those remain Phase 2/3 work from the design spec.

**Tech Stack:** Rust; NovaRocks Iceberg commit modules under `src/connector/iceberg/commit/`; unit tests via `cargo test --lib connector::iceberg::commit`.

**Spec:** [docs/superpowers/specs/2026-06-05-iceberg-operation-lifecycle-design.md](../specs/2026-06-05-iceberg-operation-lifecycle-design.md)

---

## Scope

This plan implements only Phase 1 from the spec:

- typed commit service result/error types
- typed commit unknown classifier
- typed `run_iceberg_commit_typed`
- legacy `run_iceberg_commit` wrapper for existing call sites
- focused unit tests

It intentionally does not implement operation records, MV refresh `operation_id`, startup recovery migration, distributed writer lifecycle, or diagnostic SQL.

## File Structure

- **Create** `src/connector/iceberg/commit/service.rs` — typed commit service API surface: `CommitServiceOutcome`, `CommitServiceError`, `CleanupAttempt`, `RecoveryEvidence`, and classifier helpers.
- **Modify** `src/connector/iceberg/commit/run.rs` — route commit execution through `run_iceberg_commit_typed`; preserve old `run_iceberg_commit` by formatting typed errors.
- **Modify** `src/connector/iceberg/commit/mod.rs` — export the new service module types and typed runner.
- **Test only through existing unit-test target** — no SQL tests are required for this typed facade PR because behavior of current callers is preserved by the legacy wrapper.

---

## Task 1: Add typed service primitives

**Files:**
- Create: `src/connector/iceberg/commit/service.rs`
- Test: `src/connector/iceberg/commit/service.rs`

- [ ] **Step 1: Write failing tests for typed failure classification and legacy formatting**

Create `src/connector/iceberg/commit/service.rs` with only this test module first:

```rust
#[cfg(test)]
mod tests {
    use iceberg::{NamespaceIdent, TableIdent};
    use iceberg::spec::{NestedField, PartitionSpec, Schema, Type};
    use std::sync::Arc;

    use crate::common::types::UniqueId;
    use crate::connector::iceberg::commit::{
        CommitOpKind, IcebergCommitCollector,
    };

    use super::*;

    fn test_collector() -> IcebergCommitCollector {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(iceberg::spec::PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let partition_spec = PartitionSpec::builder(schema.clone())
            .build()
            .expect("partition spec");
        IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            TableIdent::new(NamespaceIdent::new("db".to_string()), "tbl".to_string()),
            Some(42),
            7,
            Arc::new(schema),
            Arc::new(partition_spec),
            "s3://bucket/db/tbl/data/_staging/abc".to_string(),
            UniqueId { hi: 11, lo: 22 },
        )
    }

    #[test]
    fn classify_commit_error_returns_known_uncommitted_for_definite_failures() {
        assert_eq!(
            classify_commit_error(
                "FastAppend commit failed: catalog commit conflict on assert-ref-snapshot-id"
            ),
            CommitFailureKind::KnownUncommitted
        );
        assert_eq!(
            classify_commit_error("RowDelta commit failed: data invalid"),
            CommitFailureKind::KnownUncommitted
        );
        assert_eq!(
            classify_commit_error("pipeline cancelled mid-write"),
            CommitFailureKind::KnownUncommitted
        );
    }

    #[test]
    fn classify_commit_error_returns_unknown_for_transport_like_failures() {
        assert_eq!(
            classify_commit_error("FastAppend commit failed: connection reset by peer"),
            CommitFailureKind::Unknown
        );
        assert_eq!(
            classify_commit_error("RowDelta commit failed: unexpected error"),
            CommitFailureKind::Unknown
        );
    }

    #[test]
    fn unknown_error_carries_recovery_evidence_and_legacy_message() {
        let collector = test_collector();
        let evidence = RecoveryEvidence::from_collector(&collector);
        let err = CommitServiceError::unknown(
            "connection reset by peer".to_string(),
            evidence.clone(),
        );
        assert!(err.is_unknown());
        assert_eq!(err.message(), "connection reset by peer");
        assert_eq!(evidence.table_ident, "db.tbl");
        assert_eq!(evidence.op_kind, CommitOpKind::FastAppend);
        assert_eq!(evidence.base_snapshot_id, Some(42));
        assert_eq!(evidence.base_sequence_number, 7);
        assert_eq!(
            err.clone().into_legacy_string(),
            "iceberg commit unknown (connection reset by peer); staged files left at s3://bucket/db/tbl/data/_staging/abc for manual review"
        );
    }

    #[test]
    fn known_uncommitted_error_carries_cleanup_summary_and_legacy_message() {
        let cleanup = CleanupAttempt::completed(vec!["a.parquet".to_string(), "m.avro".to_string()]);
        let err = CommitServiceError::known_uncommitted(
            "catalog commit conflict".to_string(),
            cleanup.clone(),
        );
        assert!(!err.is_unknown());
        assert_eq!(err.message(), "catalog commit conflict");
        assert_eq!(cleanup.error_count, 2);
        assert_eq!(
            err.into_legacy_string(),
            "iceberg commit failed: catalog commit conflict; abort cleanup ran (2 error(s))"
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
cargo test --lib connector::iceberg::commit::service::tests -- --nocapture
```

Expected: compile fails because `commit::service` is not declared/exported yet and all tested types/functions are missing.

- [ ] **Step 3: Implement `service.rs`**

Replace `src/connector/iceberg/commit/service.rs` with:

```rust
use super::abort::CleanupError;
use super::collector::IcebergCommitCollector;
use super::types::{CommitOpKind, CommitOutcome};

pub type CommitServiceOutcome = CommitOutcome;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitFailureKind {
    KnownUncommitted,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupAttempt {
    pub attempted: bool,
    pub error_count: usize,
    pub error_paths: Vec<String>,
}

impl CleanupAttempt {
    pub fn not_attempted() -> Self {
        Self {
            attempted: false,
            error_count: 0,
            error_paths: Vec::new(),
        }
    }

    pub fn completed(error_paths: Vec<String>) -> Self {
        Self {
            attempted: true,
            error_count: error_paths.len(),
            error_paths,
        }
    }

    pub fn from_cleanup_errors(errors: &[CleanupError]) -> Self {
        Self::completed(errors.iter().map(|err| err.path.clone()).collect())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryEvidence {
    pub table_ident: String,
    pub op_kind: CommitOpKind,
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: i64,
    pub staging_dir: String,
}

impl RecoveryEvidence {
    pub fn from_collector(collector: &IcebergCommitCollector) -> Self {
        Self {
            table_ident: collector.table_ident.to_string(),
            op_kind: collector.op_kind,
            base_snapshot_id: collector.base_snapshot_id,
            base_sequence_number: collector.base_sequence_number,
            staging_dir: collector.staging_dir.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitServiceError {
    KnownUncommitted {
        message: String,
        cleanup: CleanupAttempt,
    },
    Unknown {
        message: String,
        evidence: RecoveryEvidence,
    },
}

impl CommitServiceError {
    pub fn known_uncommitted(message: String, cleanup: CleanupAttempt) -> Self {
        Self::KnownUncommitted { message, cleanup }
    }

    pub fn unknown(message: String, evidence: RecoveryEvidence) -> Self {
        Self::Unknown { message, evidence }
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown { .. })
    }

    pub fn message(&self) -> &str {
        match self {
            Self::KnownUncommitted { message, .. } | Self::Unknown { message, .. } => message,
        }
    }

    pub fn into_legacy_string(self) -> String {
        match self {
            Self::KnownUncommitted { message, cleanup } => {
                format!(
                    "iceberg commit failed: {message}; abort cleanup ran ({} error(s))",
                    cleanup.error_count
                )
            }
            Self::Unknown { message, evidence } => {
                format!(
                    "iceberg commit unknown ({message}); staged files left at {} for manual review",
                    evidence.staging_dir
                )
            }
        }
    }
}

pub fn classify_commit_error(err: &str) -> CommitFailureKind {
    let lower = err.to_lowercase();
    let definite_signals = [
        "conflict",
        "assertrefsnapshotid",
        "ref_snapshot_id_match",
        "schema id mismatch",
        "schemaidmatch",
        "spec id mismatch",
        "specidmatch",
        "data invalid",
        "datainvalid",
        "feature unsupported",
        "featureunsupported",
        "table not found",
        "tablenotfound",
        "table already exists",
        "tablealreadyexists",
        "namespace not found",
        "namespacenotfound",
        "namespace already exists",
        "namespacealreadyexists",
        "precondition failed",
        "preconditionfailed",
        "catalog commit conflict",
        "catalogcommitconflict",
        "expected data only",
        "pipeline cancelled",
        "pipeline failed",
    ];
    if definite_signals.iter().any(|s| lower.contains(s)) {
        CommitFailureKind::KnownUncommitted
    } else {
        CommitFailureKind::Unknown
    }
}

#[cfg(test)]
mod tests {
    use iceberg::{NamespaceIdent, TableIdent};
    use iceberg::spec::{NestedField, PartitionSpec, Schema, Type};
    use std::sync::Arc;

    use crate::common::types::UniqueId;
    use crate::connector::iceberg::commit::{CommitOpKind, IcebergCommitCollector};

    use super::*;

    fn test_collector() -> IcebergCommitCollector {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(iceberg::spec::PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let partition_spec = PartitionSpec::builder(schema.clone())
            .build()
            .expect("partition spec");
        IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            TableIdent::new(NamespaceIdent::new("db".to_string()), "tbl".to_string()),
            Some(42),
            7,
            Arc::new(schema),
            Arc::new(partition_spec),
            "s3://bucket/db/tbl/data/_staging/abc".to_string(),
            UniqueId { hi: 11, lo: 22 },
        )
    }

    #[test]
    fn classify_commit_error_returns_known_uncommitted_for_definite_failures() {
        assert_eq!(
            classify_commit_error(
                "FastAppend commit failed: catalog commit conflict on assert-ref-snapshot-id"
            ),
            CommitFailureKind::KnownUncommitted
        );
        assert_eq!(
            classify_commit_error("RowDelta commit failed: data invalid"),
            CommitFailureKind::KnownUncommitted
        );
        assert_eq!(
            classify_commit_error("pipeline cancelled mid-write"),
            CommitFailureKind::KnownUncommitted
        );
    }

    #[test]
    fn classify_commit_error_returns_unknown_for_transport_like_failures() {
        assert_eq!(
            classify_commit_error("FastAppend commit failed: connection reset by peer"),
            CommitFailureKind::Unknown
        );
        assert_eq!(
            classify_commit_error("RowDelta commit failed: unexpected error"),
            CommitFailureKind::Unknown
        );
    }

    #[test]
    fn unknown_error_carries_recovery_evidence_and_legacy_message() {
        let collector = test_collector();
        let evidence = RecoveryEvidence::from_collector(&collector);
        let err =
            CommitServiceError::unknown("connection reset by peer".to_string(), evidence.clone());
        assert!(err.is_unknown());
        assert_eq!(err.message(), "connection reset by peer");
        assert_eq!(evidence.table_ident, "db.tbl");
        assert_eq!(evidence.op_kind, CommitOpKind::FastAppend);
        assert_eq!(evidence.base_snapshot_id, Some(42));
        assert_eq!(evidence.base_sequence_number, 7);
        assert_eq!(
            err.clone().into_legacy_string(),
            "iceberg commit unknown (connection reset by peer); staged files left at s3://bucket/db/tbl/data/_staging/abc for manual review"
        );
    }

    #[test]
    fn known_uncommitted_error_carries_cleanup_summary_and_legacy_message() {
        let cleanup =
            CleanupAttempt::completed(vec!["a.parquet".to_string(), "m.avro".to_string()]);
        let err = CommitServiceError::known_uncommitted(
            "catalog commit conflict".to_string(),
            cleanup.clone(),
        );
        assert!(!err.is_unknown());
        assert_eq!(err.message(), "catalog commit conflict");
        assert_eq!(cleanup.error_count, 2);
        assert_eq!(
            err.into_legacy_string(),
            "iceberg commit failed: catalog commit conflict; abort cleanup ran (2 error(s))"
        );
    }
}
```

- [ ] **Step 4: Declare the module and run tests**

In `src/connector/iceberg/commit/mod.rs`, add the module declaration near `mod run;`:

```rust
mod service;
```

Do not export the types yet. Run:

```bash
cargo test --lib connector::iceberg::commit::service::tests -- --nocapture
```

Expected: PASS for the four service tests.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/service.rs src/connector/iceberg/commit/mod.rs
git commit -m "feat: add typed Iceberg commit service errors"
```

---

## Task 2: Refactor commit runner to typed API plus legacy wrapper

**Files:**
- Modify: `src/connector/iceberg/commit/run.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`
- Test: `src/connector/iceberg/commit/run.rs`

- [ ] **Step 1: Write failing export and legacy-format tests**

In `src/connector/iceberg/commit/run.rs`, replace the old classifier tests with these tests inside the existing `#[cfg(test)] mod tests`:

```rust
#[test]
fn commit_service_error_legacy_string_preserves_known_failure_format() {
    let err = CommitServiceError::known_uncommitted(
        "FastAppend commit failed: data invalid".to_string(),
        CleanupAttempt::completed(vec!["staged.parquet".to_string()]),
    );
    assert_eq!(
        err.into_legacy_string(),
        "iceberg commit failed: FastAppend commit failed: data invalid; abort cleanup ran (1 error(s))"
    );
}

#[test]
fn commit_service_error_legacy_string_preserves_unknown_format() {
    let evidence = RecoveryEvidence {
        table_ident: "db.tbl".to_string(),
        op_kind: CommitOpKind::FastAppend,
        base_snapshot_id: Some(10),
        base_sequence_number: 3,
        staging_dir: "s3://bucket/db/tbl/data/_staging/abc".to_string(),
    };
    let err = CommitServiceError::unknown("connection reset by peer".to_string(), evidence);
    assert_eq!(
        err.into_legacy_string(),
        "iceberg commit unknown (connection reset by peer); staged files left at s3://bucket/db/tbl/data/_staging/abc for manual review"
    );
}

#[test]
fn run_dispatch_accepts_rewrite_data_files_variant() {
    let _ = CommitOpKind::RewriteDataFiles;
    let _ = CommitOpKind::RewriteManifests;
    let _ = std::any::type_name::<crate::connector::iceberg::commit::RewriteDataFilesCommit>();
}
```

Add this import at the top of the test module:

```rust
use super::super::service::{CleanupAttempt, CommitServiceError, RecoveryEvidence};
```

- [ ] **Step 2: Run the focused tests to verify they fail**

Run:

```bash
cargo test --lib connector::iceberg::commit::run::tests -- --nocapture
```

Expected: compile fails until `run.rs` imports the new service types and deletes the old private classifier tests.

- [ ] **Step 3: Refactor imports in `run.rs`**

At the top of `src/connector/iceberg/commit/run.rs`, replace:

```rust
use super::types::{CommitOpKind, CommitOutcome};
```

with:

```rust
use super::service::{
    CleanupAttempt, CommitFailureKind, CommitServiceError, RecoveryEvidence, classify_commit_error,
};
use super::types::{CommitOpKind, CommitOutcome};
```

- [ ] **Step 4: Add typed runner and keep legacy wrapper**

Replace the current `pub async fn run_iceberg_commit(input: RunInput) -> Result<CommitOutcome, String>` function with:

```rust
/// Legacy compatibility wrapper for existing engine paths.
///
/// New lifecycle-aware callers should use [`run_iceberg_commit_typed`] so they
/// can branch on `CommitServiceError` without parsing user-facing strings.
pub async fn run_iceberg_commit(input: RunInput) -> Result<CommitOutcome, String> {
    run_iceberg_commit_typed(input)
        .await
        .map_err(CommitServiceError::into_legacy_string)
}

/// Dispatch a commit-action and return typed commit outcome/error.
///
/// On definite commit failure this function runs best-effort abort cleanup and
/// returns `KnownUncommitted`. On commit-unknown failure it leaves staged files
/// untouched and returns `Unknown` with recovery evidence.
pub async fn run_iceberg_commit_typed(
    input: RunInput,
) -> Result<CommitOutcome, CommitServiceError> {
    let RunInput {
        collector,
        catalog,
        table,
        fs,
        file_io,
        cleanup_path_mapper,
        cow_update_rewrite,
        target_ref,
        snapshot_properties,
    } = input;

    let action: Box<dyn IcebergCommitAction> = match collector.op_kind {
        CommitOpKind::FastAppend => Box::new(FastAppendCommit),
        CommitOpKind::Overwrite => Box::new(OverwriteCommit),
        CommitOpKind::RowDelta => Box::new(RowDeltaCommit),
        CommitOpKind::RowDeltaDv => Box::new(RowDeltaDvCommit),
        CommitOpKind::RewriteDataFiles => Box::new(RewriteDataFilesCommit),
        CommitOpKind::CowUpdate => Box::new(CowUpdateCommit {
            rewrite: cow_update_rewrite
                .ok_or_else(|| {
                    CommitServiceError::known_uncommitted(
                        "CowUpdate commit requires a rewrite set".to_string(),
                        CleanupAttempt::not_attempted(),
                    )
                })?,
        }),
        CommitOpKind::Truncate => Box::new(TruncateCommit),
        CommitOpKind::OverwritePartitions => {
            Box::new(super::overwrite_partitions::OverwritePartitionsCommit)
        }
        CommitOpKind::RewriteManifests => {
            return Err(CommitServiceError::known_uncommitted(
                "CommitOpKind::RewriteManifests must be invoked via run_rewrite_manifests directly, not the collector dispatcher".to_string(),
                CleanupAttempt::not_attempted(),
            ));
        }
    };

    let ctx = CommitCtx {
        collector: &collector,
        table: &table,
        catalog: catalog.as_ref(),
        file_io: &file_io,
        commit_uuid: Uuid::new_v4(),
        abort_handle: collector.abort_log.clone(),
        target_ref: &target_ref,
        snapshot_properties: &snapshot_properties,
    };

    match action.commit(ctx).await {
        Ok(outcome) => {
            collector.mark_committed();
            Ok(outcome)
        }
        Err(commit_err) => match classify_commit_error(&commit_err) {
            CommitFailureKind::Unknown => {
                let evidence = RecoveryEvidence::from_collector(&collector);
                tracing::warn!(
                    op_kind = ?collector.op_kind,
                    table = %collector.table_ident,
                    base_snapshot_id = ?collector.base_snapshot_id,
                    staging_dir = collector.staging_dir,
                    "iceberg commit unknown — leaving all staged files for manual review: {commit_err}"
                );
                Err(CommitServiceError::unknown(commit_err, evidence))
            }
            CommitFailureKind::KnownUncommitted => {
                let cleanup_errors = if let Some(mapper) = cleanup_path_mapper {
                    collector
                        .abort_log
                        .cleanup_with_path_mapper(&fs, |path| mapper(path))
                        .await
                } else {
                    collector.abort_log.cleanup(&fs).await
                };
                for e in &cleanup_errors {
                    tracing::warn!(path = %e.path, source = ?e.source, "abort cleanup error");
                }
                Err(CommitServiceError::known_uncommitted(
                    commit_err,
                    CleanupAttempt::from_cleanup_errors(&cleanup_errors),
                ))
            }
        },
    }
}
```

- [ ] **Step 5: Delete the old private classifier**

Remove the whole `fn is_commit_unknown_message(err: &str) -> bool` function and remove its two old tests:

```rust
fn is_commit_unknown_classifies_definite_failures()
fn is_commit_unknown_classifies_unknown_failures()
```

The classifier now lives in `service.rs`.

- [ ] **Step 6: Export typed runner and service types**

In `src/connector/iceberg/commit/mod.rs`, replace:

```rust
pub use run::{CleanupPathMapper, RunInput, run_iceberg_commit};
```

with:

```rust
pub use run::{CleanupPathMapper, RunInput, run_iceberg_commit, run_iceberg_commit_typed};
pub use service::{
    CleanupAttempt, CommitFailureKind, CommitServiceError, CommitServiceOutcome, RecoveryEvidence,
    classify_commit_error,
};
```

- [ ] **Step 7: Run focused tests**

Run:

```bash
cargo test --lib connector::iceberg::commit::service::tests -- --nocapture
cargo test --lib connector::iceberg::commit::run::tests -- --nocapture
```

Expected: both commands PASS.

- [ ] **Step 8: Commit**

```bash
git add src/connector/iceberg/commit/run.rs src/connector/iceberg/commit/mod.rs
git commit -m "feat: return typed Iceberg commit errors"
```

---

## Task 3: Verify legacy callers still compile through the compatibility wrapper

**Files:**
- Modify only if compile requires import cleanup:
  - `src/engine/iceberg_writer.rs`
  - `src/engine/delete_flow.rs`
  - `src/engine/mutation_flow.rs`
  - `src/engine/equality_delete_flow.rs`
  - `src/engine/iceberg_truncate.rs`
  - `src/engine/mv/iceberg_refresh.rs`
  - `src/connector/iceberg/compact.rs`

- [ ] **Step 1: Compile the library**

Run:

```bash
cargo test --lib connector::iceberg::commit --no-run
```

Expected: PASS compilation. Existing callers of `run_iceberg_commit` should not require signature changes because the compatibility wrapper still returns `Result<CommitOutcome, String>`.

- [ ] **Step 2: If imports fail, make only mechanical import fixes**

If the compiler reports an unused or missing import caused by the new exports, apply only the exact compiler-requested import cleanup. Do not migrate engine call sites to `run_iceberg_commit_typed` in this task.

Example allowed cleanup:

```rust
use crate::connector::iceberg::commit::{
    CommitOpKind, IcebergCommitCollector, RunInput, run_iceberg_commit,
};
```

Example disallowed scope expansion:

```rust
// Do not do this in Phase 1:
use crate::connector::iceberg::commit::{CommitServiceError, run_iceberg_commit_typed};
```

- [ ] **Step 3: Confirm old string parser remains localized to MV only**

Run:

```bash
rg -n "iceberg commit unknown \\(|is_iceberg_commit_unknown_error|is_commit_unknown_message" src/connector/iceberg src/engine
```

Expected:

- No `is_commit_unknown_message` in `src/connector/iceberg/commit/run.rs`.
- `iceberg commit unknown (` only appears in `CommitServiceError::into_legacy_string` and existing MV compatibility code.
- `is_iceberg_commit_unknown_error` may still exist in `src/engine/mv/iceberg_refresh.rs`; that migration belongs to Phase 3.

- [ ] **Step 4: Commit import fixes if any were needed**

If Step 2 required changes, commit them:

```bash
git add <changed-files>
git commit -m "chore: keep Iceberg commit callers on legacy wrapper"
```

If no files changed, do not create a commit.

---

## Task 4: Format and run final validation

**Files:**
- Modify only if formatting changes touched files:
  - `src/connector/iceberg/commit/service.rs`
  - `src/connector/iceberg/commit/run.rs`
  - `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 1: Format touched Rust files**

Run:

```bash
cargo fmt -- src/connector/iceberg/commit/service.rs src/connector/iceberg/commit/run.rs src/connector/iceberg/commit/mod.rs
```

Expected: command exits 0.

- [ ] **Step 2: Check formatting did not touch unrelated files**

Run:

```bash
git diff --stat
```

Expected: only these files appear unless Task 3 needed mechanical import cleanup:

```text
src/connector/iceberg/commit/mod.rs
src/connector/iceberg/commit/run.rs
src/connector/iceberg/commit/service.rs
```

- [ ] **Step 3: Run focused commit-module tests**

Run:

```bash
cargo test --lib connector::iceberg::commit -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Run a broader compile check for engine call sites**

Run:

```bash
cargo test --lib engine::iceberg_writer --no-run
```

Expected: PASS compilation. This catches the most common standalone INSERT caller of `run_iceberg_commit`.

- [ ] **Step 5: Commit formatting or validation-driven fixes**

If formatting or small compile fixes changed files:

```bash
git add src/connector/iceberg/commit/service.rs src/connector/iceberg/commit/run.rs src/connector/iceberg/commit/mod.rs
git commit -m "chore: format typed Iceberg commit service"
```

If no files changed, do not create a commit.

---

## Task 5: Acceptance checklist for Phase 1

**Files:**
- No source changes expected.

- [ ] **Step 1: Confirm typed API exists**

Run:

```bash
rg -n "run_iceberg_commit_typed|CommitServiceError|CommitFailureKind|RecoveryEvidence" src/connector/iceberg/commit
```

Expected:

- `run_iceberg_commit_typed` is defined in `src/connector/iceberg/commit/run.rs`.
- `CommitServiceError`, `CommitFailureKind`, and `RecoveryEvidence` are defined in `src/connector/iceberg/commit/service.rs`.
- These types are exported from `src/connector/iceberg/commit/mod.rs`.

- [ ] **Step 2: Confirm string classifier was removed from `run.rs`**

Run:

```bash
rg -n "is_commit_unknown_message" src/connector/iceberg/commit/run.rs
```

Expected: no matches.

- [ ] **Step 3: Confirm legacy API is still available for current callers**

Run:

```bash
rg -n "pub async fn run_iceberg_commit\\(" src/connector/iceberg/commit/run.rs
```

Expected: one match; the function returns `Result<CommitOutcome, String>` and calls `run_iceberg_commit_typed`.

- [ ] **Step 4: Record final status**

Run:

```bash
git status --short --branch
```

Expected: clean working tree on the implementation branch.

If all checks pass, Phase 1 is complete and ready for the next plan: shared operation lifecycle skeleton.
