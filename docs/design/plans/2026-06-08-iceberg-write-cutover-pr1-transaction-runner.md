# Iceberg Write Cutover PR-1: Transaction Runner + Coordinator Return-Shape — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the engine-owned `IcebergWriteTransactionRunner` that drives the Iceberg operation state machine (Preparing → Writing → Committing → Committed → Finalizing → Finalized, plus failure terminals), proven with fake writer/commit outcomes against a real metadata-backed test state, and extend the execution coordinator to expose writer outcome via `execute_with_write_outcome` / `CoordinatedQueryResult`. **No user-facing write routing change** — SQL flows do not call the runner yet (that is PR-2).

**Architecture:** The runner orchestrates already-existing pieces: the IW-6 adapter (`runtime`/`engine` `write_operation_lifecycle`), the operation repository (`create_operation` / `transition_operation` / `record_operation_fact`), and the typed-commit fact mappers (`operation_fact_from_commit_result` / `operation_fact_from_finalize_failure`). Its side-effecting dependencies (running the coordinated write, calling the commit service, post-commit finalization) sit behind a single trait `IcebergWriteTransactionExecutor`, so unit tests inject a fake while PR-2 supplies the real executor. Persistence uses the real repo against a SQLite-backed test `StandaloneState`, so state transitions are asserted for real.

**Tech Stack:** Rust, NovaRocks standalone engine, managed-lake metadata repository (`SqliteMetaStoreProvider`), Iceberg operation repository + typed commit service, Cargo unit tests. Default cargo features only (no `compat`, no thirdparty, no C++).

---

## Background facts (verified against the worktree @ origin/main b676697e)

Coordinator (`src/runtime/coordinator.rs`):
- `ExecutionCoordinator` struct (lines 47-67); `execute(self) -> Result<QueryResult, String>` (lines 69-436). At lines 410-418 it logs `fetch_result.write_commit` then discards it; lines 420-435 assemble `QueryResult`.
- `submit_and_fetch_loop(...) -> Result<SubmitAndFetchResult, String>`; `SubmitAndFetchResult { chunks, write_commit: Option<WriteCommitInput> }` (lines 672-676).
- The coordinator is **metadata-agnostic** (no `crate::engine` / catalog imports) — keep it so.
- Single production caller: `src/engine/mod.rs:~2915-2921` chains `ExecutionCoordinator::new(...).execute()`.
- `WriteCommitInput` / `WriteAbortInput` are `pub(crate)` in `src/runtime/write_coordinator.rs` (lines 43-46, 62-67). `WriteCoordinator::abort_input() -> Option<WriteAbortInput>` exists (only used in tests today).
- `QueryResult` / `QueryResultColumn` in `src/runtime/query_result.rs:12-23`.

Operation repository (`src/meta/repository/iceberg_operation.rs`):
- `create_operation(&self, txn: &mut dyn MetaWriteTxn, req: CreateIcebergOperationRequest) -> RepositoryResult<StoredIcebergOperation>`
- `load_operation(&self, txn: &dyn MetaReadTxn, operation_id: i64) -> RepositoryResult<Option<StoredIcebergOperation>>`
- `transition_operation(&self, txn: &mut dyn MetaWriteTxn, operation_id: i64, to_state: IcebergOperationState, now_ms: i64) -> RepositoryResult<()>` (validates the transition; idempotent on same-state)
- `record_operation_fact(&self, txn: &mut dyn MetaWriteTxn, req: IcebergOperationFactUpdate) -> RepositoryResult<()>` (combines transition + fact merge)
- `CreateIcebergOperationRequest { operation_kind, target, attempt_id, base_snapshot_id, base_snapshot_map, staged_artifacts, created_at_ms }`
- `IcebergOperationTarget { catalog, namespace, table, ref_name }`
- `IcebergOperationKind { InsertAppend, InsertOverwrite, RowDelta, MvRefresh, Maintenance }`
- `IcebergOperationState { Preparing, Writing, Collecting, Committing, Committed, CommitUnknown, Finalizing, Finalized, Aborting, Aborted, FailedKnownUncommitted, FinalizeFailedKnownCommitted }`
- `IcebergOperationFactUpdate { operation_id, state, commit_outcome, cleanup_outcome, recovery_evidence, failure, now_ms }`

Fact mappers (`src/connector/iceberg/operation_lifecycle.rs`):
- `operation_fact_from_commit_result(result: Result<&CommitOutcome, &CommitServiceError>) -> IcebergOperationFact`
- `operation_fact_from_finalize_failure(message: String) -> IcebergOperationFact`
- `IcebergOperationFact { state, commit_outcome, cleanup_outcome, recovery_evidence, failure }` (no `operation_id` / `now_ms` — the runner adds those to build an `IcebergOperationFactUpdate`).

Commit service (`src/connector/iceberg/commit/`):
- `run_iceberg_commit_typed(input: RunInput) -> Result<CommitOutcome, CommitServiceError>` (async; called via `block_on_iceberg` in the engine).
- `CommitOutcome { new_snapshot_id, written_manifest_paths }`; `CommitServiceError { KnownUncommitted { message, cleanup }, Unknown { message, evidence } }`; `CommitOpKind { FastAppend, Overwrite, OverwritePartitions, RowDelta, RowDeltaDv, CowUpdate, ... }`.

Adapter + test pattern:
- `src/engine/write_operation_lifecycle.rs`: `create_writer_operation_from_commit(state, ctx, commit) -> Result<i64, String>`, `record_writer_abort_fact(state, op_id, abort, now_ms) -> Result<(), String>`; both use `state.metadata_provider` + `state.iceberg_operation_repo` with `provider.begin_write(label)? … txn.commit()`.
- `StandaloneState` (`src/engine/mod.rs`): `metadata_provider: Option<Arc<dyn MetaStoreProvider>>`, `iceberg_operation_repo: IcebergOperationRepository`, plus `iceberg_catalogs` / `catalog_mgr` / `dictionary_manager` (used by finalize in PR-2). `StandaloneState::default()` works for tests.
- Test state pattern (mirror exactly):
  ```rust
  let dir = tempfile::tempdir().expect("metadata tempdir");
  let provider: Arc<dyn MetaStoreProvider> =
      Arc::new(SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite")).expect("provider"));
  let state = Arc::new(StandaloneState { metadata_provider: Some(Arc::clone(&provider)), ..StandaloneState::default() });
  // read back:
  let read = provider.begin_read().expect("read txn");
  let stored = state.iceberg_operation_repo.load_operation(read.as_ref(), op_id).unwrap().unwrap();
  ```

> **Implementer note on exact APIs:** signatures above are verified, but small things may need adapting against the real source: the `RepositoryResult` error type (map to `String` with `.map_err(|e| e.to_string())`), the exact `MetaWriteTxn` borrow (`txn.as_mut()`), and whether a current-millis helper already exists (search `now_ms` / `unix_millis` in `src/engine` and reuse it; otherwise use the `current_unix_millis()` helper defined in Task 2). Verify before assuming.

## Design decisions locked for PR-1

1. **Create the `Preparing` record before running the write** (covers a mid-write failure path that produces a `WriteAbortInput`). `staged_artifacts` is empty at create; the abort fact carries staged info.
2. **Empty query-sourced write → transition the operation to `Aborted`** (reason "empty input, no-op"). The repo has no delete; `Aborted` is the clean terminal and preserves an audit trail. (Statically-empty inputs are short-circuited at the SQL-flow layer in PR-2, before any record is created.)
3. **`write_abort` on success is `None`;** the coordinator's failure→abort surfacing is wired in PR-2 when routing lands. PR-1 exercises the abort branch via the fake executor.
4. **Dependency seam = one trait** (`IcebergWriteTransactionExecutor`) so the runner is fake-tested. PR-2 supplies the real executor (coordinator + `run_iceberg_commit_typed` + cache/dict finalize).

## File Structure

- `src/runtime/coordinator.rs` — add `CoordinatedQueryResult`, `execute_with_write_outcome()`; make `execute()` a wrapper. Re-export `WriteAbortInput`.
- `src/engine/write_transaction.rs` (**new**) — spec types, the `IcebergWriteTransactionExecutor` trait, `IcebergWriteTransactionRunner`, `IcebergWriteTransactionOutcome`, the orchestration logic, and the fake-backed unit tests.
- `src/engine/mod.rs` — register `mod write_transaction;`.

---

## Task 1: Coordinator return-shape (`execute_with_write_outcome` + `CoordinatedQueryResult`)

**Files:**
- Modify: `src/runtime/coordinator.rs`

This is a behavior-preserving refactor (the single `execute()` caller is unchanged); it is verified by the existing suite plus a compile, and the new fields are exercised by the runner's fake tests in later tasks.

- [ ] **Step 1: Add `CoordinatedQueryResult` and re-export `WriteAbortInput`**

In `src/runtime/coordinator.rs`, extend the `write_coordinator` import to include `WriteAbortInput`:

```rust
use crate::runtime::write_coordinator::{
    WriteAbortInput, WriteCommitInput, WriteCoordinator, WriterKey, register_query, unregister_query,
};
```

Add the new return type near the top of the file (after the imports):

```rust
/// Result of a coordinated execution, exposing the writer-side outcome to the
/// engine layer. `write_commit` is set when writers reported a commit input on
/// the success path. `write_abort` is reserved for the failure path that PR-2
/// wires when SQL write routing moves onto the coordinator; on the current
/// success path it is `None`.
#[derive(Debug)]
pub(crate) struct CoordinatedQueryResult {
    pub(crate) query_result: QueryResult,
    pub(crate) write_commit: Option<WriteCommitInput>,
    pub(crate) write_abort: Option<WriteAbortInput>,
}
```

- [ ] **Step 2: Rename the current `execute()` body to `execute_with_write_outcome()` and return the new struct**

Change the method signature from `pub(crate) fn execute(self) -> Result<QueryResult, String>` to:

```rust
pub(crate) fn execute_with_write_outcome(self) -> Result<CoordinatedQueryResult, String> {
```

Keep the entire existing body. At the tail (the current lines ~410-435), replace the "log then discard + return QueryResult" with: keep the `tracing::info!` log of `fetch_result.write_commit` (it is still useful), build the `QueryResult` exactly as before, and return it wrapped:

```rust
        let query_result = QueryResult {
            columns: root_fragment
                .output_columns
                .iter()
                .map(|c| QueryResultColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    logical_type: None,
                })
                .collect(),
            chunks: fetch_result.chunks,
        };
        Ok(CoordinatedQueryResult {
            query_result,
            write_commit: fetch_result.write_commit,
            write_abort: None,
        })
```

- [ ] **Step 3: Add the compatible `execute()` wrapper**

Add immediately after `execute_with_write_outcome`:

```rust
    /// Backward-compatible entry point: runs the coordinated execution and
    /// returns only the query result, discarding the writer outcome. Existing
    /// callers that do not participate in the Iceberg write lifecycle use this.
    pub(crate) fn execute(self) -> Result<QueryResult, String> {
        self.execute_with_write_outcome()
            .map(|outcome| outcome.query_result)
    }
```

- [ ] **Step 4: Build and run the coordinator + write-coordinator suites**

Run (timeout 600000; first build may be cold):
```
cargo build
cargo test -q write_coordinator
cargo test -q report_exec_status
cargo test -q -- runtime::coordinator
```
Expected: build succeeds; the existing coordinator/write-coordinator/report tests stay green (the `execute()` call site in `engine/mod.rs` compiles unchanged). If any `coordinator` unit tests call `execute()`, they must still pass.

- [ ] **Step 5: Commit**

```bash
git add src/runtime/coordinator.rs
git commit -m "coordinator: expose write outcome via execute_with_write_outcome/CoordinatedQueryResult"
```

---

## Task 2: Spec types + executor trait + module registration

**Files:**
- Create: `src/engine/write_transaction.rs`
- Modify: `src/engine/mod.rs` (register module)

- [ ] **Step 1: Create the module with spec types, the executor trait, and a current-millis helper**

Create `src/engine/write_transaction.rs`:

```rust
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Engine-owned Iceberg write transaction runner.
//!
//! The runner is the default boundary for user-level Iceberg SQL writes that
//! need coordinated file output, metadata commit, lifecycle persistence, and
//! post-commit finalization. It drives the Iceberg operation state machine and
//! persists facts via the operation repository, delegating the side-effecting
//! steps (running the coordinated write, calling the typed commit service,
//! finalization) to an [`IcebergWriteTransactionExecutor`]. PR-1 ships the
//! runner + fake-backed tests; the real executor and SQL routing land in PR-2.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::connector::iceberg::commit::run::CommitOutcome;
use crate::connector::iceberg::commit::service::CommitServiceError;
use crate::connector::iceberg::commit::types::CommitOpKind;
use crate::engine::StandaloneState;
use crate::meta::repository::iceberg_operation::{IcebergOperationKind, IcebergOperationTarget};
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::runtime::query_result::QueryResult;
use crate::runtime::write_coordinator::WriteCommitInput;

/// How the runner should commit the collected writer output.
pub(crate) struct IcebergWriteCommitPolicy {
    pub(crate) commit_op_kind: CommitOpKind,
    pub(crate) base_snapshot_id: Option<i64>,
    pub(crate) base_snapshot_map: BTreeMap<String, i64>,
    pub(crate) target_ref: String,
    pub(crate) snapshot_properties: BTreeMap<String, String>,
}

/// SQL-specific validation captured at spec-build time. Consumed by the
/// executor's write step (the runner itself does not validate). Grown in PR-2.
pub(crate) struct IcebergWriteValidationPolicy {
    /// Branch writes require Iceberg format v3.
    pub(crate) require_v3_for_branch: bool,
}

/// What the write produces. The runner does not execute the source; the
/// executor does. Variants are filled out as flows are cut over in PR-2+.
pub(crate) enum IcebergWriteSource {
    /// Rows produced by a coordinated query/mutation plan.
    CoordinatedPlan,
}

/// A complete description of one Iceberg write transaction. SQL flows build
/// this; the runner owns the lifecycle.
pub(crate) struct IcebergWriteTransactionSpec {
    pub(crate) target: IcebergOperationTarget,
    pub(crate) operation_kind: IcebergOperationKind,
    pub(crate) attempt_id: String,
    pub(crate) commit: IcebergWriteCommitPolicy,
    pub(crate) validation: IcebergWriteValidationPolicy,
    pub(crate) source: IcebergWriteSource,
}

/// Outcome of a successful (or empty/no-op) transaction.
#[derive(Debug)]
pub(crate) struct IcebergWriteTransactionOutcome {
    pub(crate) query_result: QueryResult,
    /// `Some` for committed writes; `None` for empty/no-op writes.
    pub(crate) operation_id: Option<i64>,
    /// `Some` for committed writes.
    pub(crate) committed_snapshot_id: Option<i64>,
}

/// The side-effecting dependencies of a write transaction. Real implementation
/// (PR-2) wraps the execution coordinator + typed commit service + cache/dict
/// finalization; tests inject a fake.
pub(crate) trait IcebergWriteTransactionExecutor {
    /// Run the coordinated writer plan, returning the writer outcome.
    fn run_coordinated_write(
        &self,
        spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String>;

    /// Commit the collected writer output through the typed commit service.
    fn commit(
        &self,
        spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError>;

    /// Post-commit finalization (cache invalidation, dictionary stale marking).
    fn finalize(&self, spec: &IcebergWriteTransactionSpec) -> Result<(), String>;
}

/// Current time in unix milliseconds for operation-record timestamps.
fn current_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
```

> Implementer: if a current-millis helper already exists in `src/engine` (search `now_ms`/`unix_millis`), use it instead of `current_unix_millis` and drop the local one.

- [ ] **Step 2: Register the module**

In `src/engine/mod.rs`, add alongside the other `mod` declarations (near `mod write_operation_lifecycle;`):

```rust
mod write_transaction;
```

- [ ] **Step 3: Build to verify types compile**

Run: `cargo build` (timeout 600000)
Expected: compiles. Warnings about unused items are expected at this stage (the runner is added in Task 3); do not suppress them with `#[allow(dead_code)]` unless the build fails — they clear once Task 3 lands.

- [ ] **Step 4: Commit**

```bash
git add src/engine/write_transaction.rs src/engine/mod.rs
git commit -m "engine: add Iceberg write transaction spec types + executor trait"
```

---

## Task 3: Runner orchestration + fake executor + success path

**Files:**
- Modify: `src/engine/write_transaction.rs`

- [ ] **Step 1: Write the failing success-path test**

Append a `#[cfg(test)] mod tests` block to `src/engine/write_transaction.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::commit::run::CommitOutcome;
    use crate::connector::iceberg::commit::service::CommitServiceError;
    use crate::meta::MetaStoreProvider;
    use crate::meta::sqlite::SqliteMetaStoreProvider;
    use crate::meta::repository::iceberg_operation::IcebergOperationState;
    use crate::runtime::query_result::QueryResult;
    use crate::runtime::write_coordinator::{WriteAbortInput, WriteCommitInput};
    use std::cell::RefCell;

    struct TestEnv {
        state: Arc<StandaloneState>,
        provider: Arc<dyn MetaStoreProvider>,
        _dir: tempfile::TempDir,
    }

    fn test_env() -> TestEnv {
        let dir = tempfile::tempdir().expect("metadata tempdir");
        let provider: Arc<dyn MetaStoreProvider> = Arc::new(
            SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite")).expect("provider"),
        );
        let state = Arc::new(StandaloneState {
            metadata_provider: Some(Arc::clone(&provider)),
            ..StandaloneState::default()
        });
        TestEnv { state, provider, _dir: dir }
    }

    fn sample_spec() -> IcebergWriteTransactionSpec {
        IcebergWriteTransactionSpec {
            target: IcebergOperationTarget {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                ref_name: None,
            },
            operation_kind: IcebergOperationKind::InsertAppend,
            attempt_id: "attempt-1".to_string(),
            commit: IcebergWriteCommitPolicy {
                commit_op_kind: CommitOpKind::FastAppend,
                base_snapshot_id: Some(7),
                base_snapshot_map: BTreeMap::new(),
                target_ref: "main".to_string(),
                snapshot_properties: BTreeMap::new(),
            },
            validation: IcebergWriteValidationPolicy { require_v3_for_branch: false },
            source: IcebergWriteSource::CoordinatedPlan,
        }
    }

    fn empty_query_result() -> QueryResult {
        QueryResult { columns: Vec::new(), chunks: Vec::new() }
    }

    fn write_commit_with_one_writer() -> WriteCommitInput {
        // Build a WriteCommitInput carrying one writer with one data file.
        // Reuse the helper shape from write_operation_lifecycle tests; the
        // exact constructor is in crate::runtime::write_coordinator.
        crate::runtime::write_operation_lifecycle::test_support::write_commit_with_data_file()
    }

    /// Fake executor returning canned outcomes for each step.
    struct FakeExecutor {
        write: RefCell<Option<Result<CoordinatedQueryResult, String>>>,
        commit: RefCell<Option<Result<CommitOutcome, CommitServiceError>>>,
        finalize: Result<(), String>,
    }

    impl IcebergWriteTransactionExecutor for FakeExecutor {
        fn run_coordinated_write(
            &self,
            _spec: &IcebergWriteTransactionSpec,
        ) -> Result<CoordinatedQueryResult, String> {
            self.write.borrow_mut().take().expect("write outcome set once")
        }
        fn commit(
            &self,
            _spec: &IcebergWriteTransactionSpec,
            _write_commit: &WriteCommitInput,
        ) -> Result<CommitOutcome, CommitServiceError> {
            self.commit.borrow_mut().take().expect("commit outcome set once")
        }
        fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
            self.finalize.clone()
        }
    }

    #[test]
    fn successful_append_drives_operation_to_finalized() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
            }))),
            commit: RefCell::new(Some(Ok(CommitOutcome {
                new_snapshot_id: 1234,
                written_manifest_paths: vec!["s3://bucket/m.avro".to_string()],
            }))),
            finalize: Ok(()),
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);

        let outcome = runner.run(sample_spec()).expect("run");

        assert_eq!(outcome.committed_snapshot_id, Some(1234));
        let op_id = outcome.operation_id.expect("operation id");

        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), op_id)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::Finalized);
        assert_eq!(stored.commit_outcome.as_ref().map(|c| c.snapshot_id), Some(1234));
    }
}
```

> Implementer notes: (a) Provide a small `pub(crate) mod test_support` in `src/runtime/write_operation_lifecycle.rs` exposing `write_commit_with_data_file()` if one is not already shared — the existing tests in that file build such a value; promote that constructor to a `#[cfg(test)]`-visible helper, OR inline an equivalent `WriteCommitInput` here using `crate::runtime::write_coordinator` constructors. Pick whichever matches the existing visibility; do not duplicate logic. (b) Confirm the `SqliteMetaStoreProvider` import path (`crate::meta::sqlite::SqliteMetaStoreProvider` vs `crate::meta::SqliteMetaStoreProvider`) against `write_operation_lifecycle.rs`'s test imports and match it.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -q successful_append_drives_operation_to_finalized`
Expected: FAIL — `IcebergWriteTransactionRunner` does not exist yet.

- [ ] **Step 3: Implement the runner**

Add to `src/engine/write_transaction.rs` (before the test module), the runner and its private persistence helpers:

```rust
use crate::connector::iceberg::operation_lifecycle::{
    operation_fact_from_commit_result, operation_fact_from_finalize_failure, IcebergOperationFact,
};
use crate::meta::repository::iceberg_operation::{
    CreateIcebergOperationRequest, IcebergOperationFactUpdate, IcebergOperationState,
};

/// Drives one Iceberg write transaction through the operation state machine.
pub(crate) struct IcebergWriteTransactionRunner<'a, E: IcebergWriteTransactionExecutor> {
    state: Arc<StandaloneState>,
    executor: &'a E,
}

impl<'a, E: IcebergWriteTransactionExecutor> IcebergWriteTransactionRunner<'a, E> {
    pub(crate) fn new(state: Arc<StandaloneState>, executor: &'a E) -> Self {
        Self { state, executor }
    }

    pub(crate) fn run(
        &self,
        spec: IcebergWriteTransactionSpec,
    ) -> Result<IcebergWriteTransactionOutcome, String> {
        // 1. Create the Preparing record before running the write, so a write
        //    failure that produced staged files is recoverable.
        let operation_id = self.create_preparing(&spec)?;

        // 2. Run the coordinated write.
        let written = self.executor.run_coordinated_write(&spec)?;

        // 3. Writer abort path: record FailedKnownUncommitted and surface error.
        if let Some(abort) = &written.write_abort {
            crate::engine::write_operation_lifecycle::record_writer_abort_fact(
                &self.state,
                operation_id,
                abort,
                current_unix_millis(),
            )?;
            return Err(format!(
                "iceberg write operation {operation_id} aborted before commit: {}",
                abort.reason
            ));
        }

        // 4. Empty input: no writer commit -> transition to Aborted (no-op).
        let Some(write_commit) = written.write_commit.as_ref().filter(|c| !c.writers.is_empty())
        else {
            self.transition(operation_id, IcebergOperationState::Aborting)?;
            self.transition(operation_id, IcebergOperationState::Aborted)?;
            return Ok(IcebergWriteTransactionOutcome {
                query_result: written.query_result,
                operation_id: None,
                committed_snapshot_id: None,
            });
        };

        // 5. Advance to Committing and call the commit service.
        self.transition(operation_id, IcebergOperationState::Committing)?;
        match self.executor.commit(&spec, write_commit) {
            Ok(commit_outcome) => {
                let snapshot_id = commit_outcome.new_snapshot_id;
                self.record_fact(
                    operation_id,
                    operation_fact_from_commit_result(Ok(&commit_outcome)),
                )?;
                // 6. Finalize.
                self.transition(operation_id, IcebergOperationState::Finalizing)?;
                match self.executor.finalize(&spec) {
                    Ok(()) => {
                        self.transition(operation_id, IcebergOperationState::Finalized)?;
                        Ok(IcebergWriteTransactionOutcome {
                            query_result: written.query_result,
                            operation_id: Some(operation_id),
                            committed_snapshot_id: Some(snapshot_id),
                        })
                    }
                    Err(message) => {
                        self.record_fact(
                            operation_id,
                            operation_fact_from_finalize_failure(message),
                        )?;
                        Err(format!(
                            "iceberg write operation {operation_id}: metadata commit succeeded \
                             (snapshot {snapshot_id}, known committed) but finalization failed; \
                             do not retry the write"
                        ))
                    }
                }
            }
            Err(commit_err) => {
                let message = commit_err.to_string();
                self.record_fact(
                    operation_id,
                    operation_fact_from_commit_result(Err(&commit_err)),
                )?;
                Err(format!(
                    "iceberg write operation {operation_id} commit failed: {message}"
                ))
            }
        }
    }

    fn create_preparing(&self, spec: &IcebergWriteTransactionSpec) -> Result<i64, String> {
        let provider = self.metadata_provider()?;
        let request = CreateIcebergOperationRequest {
            operation_kind: spec.operation_kind,
            target: spec.target.clone(),
            attempt_id: spec.attempt_id.clone(),
            base_snapshot_id: spec.commit.base_snapshot_id,
            base_snapshot_map: spec.commit.base_snapshot_map.clone(),
            staged_artifacts: Vec::new(),
            created_at_ms: current_unix_millis(),
        };
        let mut txn = provider.begin_write("create iceberg write operation")?;
        let stored = self
            .state
            .iceberg_operation_repo
            .create_operation(txn.as_mut(), request)
            .map_err(|e| e.to_string())?;
        txn.commit()?;
        Ok(stored.operation_id)
    }

    fn transition(&self, operation_id: i64, to: IcebergOperationState) -> Result<(), String> {
        let provider = self.metadata_provider()?;
        let mut txn = provider.begin_write("advance iceberg write operation")?;
        self.state
            .iceberg_operation_repo
            .transition_operation(txn.as_mut(), operation_id, to, current_unix_millis())
            .map_err(|e| e.to_string())?;
        txn.commit()
    }

    fn record_fact(&self, operation_id: i64, fact: IcebergOperationFact) -> Result<(), String> {
        let provider = self.metadata_provider()?;
        let update = IcebergOperationFactUpdate {
            operation_id,
            state: fact.state,
            commit_outcome: fact.commit_outcome,
            cleanup_outcome: fact.cleanup_outcome,
            recovery_evidence: fact.recovery_evidence,
            failure: fact.failure,
            now_ms: current_unix_millis(),
        };
        let mut txn = provider.begin_write("record iceberg write operation fact")?;
        self.state
            .iceberg_operation_repo
            .record_operation_fact(txn.as_mut(), update)
            .map_err(|e| e.to_string())?;
        txn.commit()
    }

    fn metadata_provider(&self) -> Result<&Arc<dyn crate::meta::MetaStoreProvider>, String> {
        self.state
            .metadata_provider
            .as_ref()
            .ok_or_else(|| "metadata provider is required for iceberg write operations".to_string())
    }
}
```

> Implementer notes: (a) Confirm `IcebergOperationFact` is exported from `connector::iceberg::operation_lifecycle` (the field set is `{ state, commit_outcome, cleanup_outcome, recovery_evidence, failure }`); if its fields differ, map them through. (b) `CommitServiceError` must implement `Display`/`ToString` (the legacy path uses `into_legacy_string()`); if `.to_string()` is not available, use the existing conversion (search `into_legacy_string`). (c) `IcebergOperationKind` / `IcebergOperationTarget` / `CommitOpKind` need to `derive(Clone, Copy/Clone)` as used — if `operation_kind` is not `Copy`, clone it. (d) `provider.begin_write(...)` returns a write-txn handle whose `.as_mut()` yields `&mut dyn MetaWriteTxn` and `.commit()` returns `Result<(), String>` (mirror `engine/write_operation_lifecycle.rs`).

- [ ] **Step 4: Run the success test to verify it passes**

Run: `cargo build && cargo test -q successful_append_drives_operation_to_finalized`
Expected: PASS — stored operation ends in `Finalized` with `commit_outcome.snapshot_id == 1234`.

- [ ] **Step 5: Commit**

```bash
git add src/engine/write_transaction.rs
git commit -m "engine: implement IcebergWriteTransactionRunner state-machine orchestration"
```

---

## Task 4: Failure-path and empty-input tests

**Files:**
- Modify: `src/engine/write_transaction.rs` (tests module)

- [ ] **Step 1: Write the failing failure/edge tests**

Add to the `tests` module. Each builds a `FakeExecutor` with the relevant canned outcome and asserts the stored operation's terminal state/fact.

```rust
    fn one_writer_abort() -> WriteAbortInput {
        crate::runtime::write_operation_lifecycle::test_support::write_abort_with_data_file()
    }

    #[test]
    fn writer_abort_records_failed_known_uncommitted() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: None,
                write_abort: Some(one_writer_abort()),
            }))),
            commit: RefCell::new(None),
            finalize: Ok(()),
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let err = runner.run(sample_spec()).expect_err("abort surfaces error");
        assert!(err.contains("aborted before commit"), "got: {err}");
        // The operation should exist and be FailedKnownUncommitted. Find it by
        // listing; since run() returns Err it does not hand back the id, so the
        // create step used attempt_id "attempt-1" — load via the repo's lookup.
        // (Implementer: if the repo lacks a by-attempt lookup, capture the id by
        // having create_preparing log it, or assert via a list_operations call.)
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::FailedKnownUncommitted);
    }

    #[test]
    fn commit_known_uncommitted_records_failed_known_uncommitted() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
            }))),
            commit: RefCell::new(Some(Err(CommitServiceError::KnownUncommitted {
                message: "conflict".to_string(),
                cleanup: crate::connector::iceberg::commit::service::CleanupAttempt {
                    attempted: true,
                    error_count: 0,
                    error_paths: Vec::new(),
                },
            }))),
            finalize: Ok(()),
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let err = runner.run(sample_spec()).expect_err("commit failure surfaces");
        assert!(err.contains("commit failed"), "got: {err}");
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::FailedKnownUncommitted);
    }

    #[test]
    fn commit_unknown_records_commit_unknown_and_skips_finalize() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
            }))),
            commit: RefCell::new(Some(Err(CommitServiceError::Unknown {
                message: "rpc timeout".to_string(),
                evidence: crate::connector::iceberg::commit::service::RecoveryEvidence {
                    table_ident: "db.orders".to_string(),
                    op_kind: CommitOpKind::FastAppend,
                    base_snapshot_id: Some(7),
                    base_sequence_number: 3,
                    staging_dir: "s3://bucket/_staging/x".to_string(),
                },
            }))),
            finalize: Err("finalize must not be called".to_string()),
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let _ = runner.run(sample_spec()).expect_err("commit unknown surfaces");
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::CommitUnknown);
        assert!(stored.recovery_evidence.is_some());
    }

    #[test]
    fn finalize_failure_records_finalize_failed_known_committed() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
            }))),
            commit: RefCell::new(Some(Ok(CommitOutcome {
                new_snapshot_id: 9,
                written_manifest_paths: Vec::new(),
            }))),
            finalize: Err("cache invalidation failed".to_string()),
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let err = runner.run(sample_spec()).expect_err("finalize failure surfaces");
        assert!(err.contains("known committed"), "got: {err}");
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::FinalizeFailedKnownCommitted);
    }

    #[test]
    fn empty_write_transitions_to_aborted_with_no_committed_outcome() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: None,
                write_abort: None,
            }))),
            commit: RefCell::new(None),
            finalize: Ok(()),
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let outcome = runner.run(sample_spec()).expect("empty is OK");
        assert_eq!(outcome.operation_id, None);
        assert_eq!(outcome.committed_snapshot_id, None);
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::Aborted);
    }
```

> Implementer notes: (a) The tests assume the first created operation has `operation_id == 1`. Verify the repo's id allocation starts at 1; if not, capture the id another way (e.g., add a `list_operations`/by-attempt lookup if one exists, or have the abort/failure paths still return the id via a richer error — but do NOT change the runner's public success contract just for tests; prefer reading the single row). (b) Confirm `CleanupAttempt` / `RecoveryEvidence` field names + the `op_kind`/`base_sequence_number` types against `service.rs`; adapt literally. (c) If `write_abort_with_data_file()` / `write_commit_with_data_file()` test helpers are not already shared from `write_operation_lifecycle.rs`, add them there under a `#[cfg(test)] pub(crate) mod test_support` and reuse — do not duplicate.

- [ ] **Step 2: Run the new tests**

Run: `cargo test -q -- engine::write_transaction::tests` (timeout 600000)
Expected: all five new tests + the success test pass (6 total). If a state assertion fails, inspect the actual stored state and reconcile with the repo's transition validation (e.g., `record_operation_fact` may require a specific predecessor state — the runner advances to `Committing` before recording the commit fact, which the success/known-uncommitted/unknown tests already do).

- [ ] **Step 3: Commit**

```bash
git add src/engine/write_transaction.rs
git commit -m "engine: fake-test runner failure and empty-input state transitions"
```

---

## Task 5: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Format and lint**

Run (timeout 600000):
```
cargo fmt
cargo clippy --all-targets 2>&1 | tail -25
```
Expected: no formatting churn beyond the new code; no new clippy warnings in `src/runtime/coordinator.rs` or `src/engine/write_transaction.rs`. The `IcebergWriteValidationPolicy` / `IcebergWriteSource` fields are consumed by the spec (passed into the runner) — if clippy flags a field as never-read because PR-1's runner does not yet act on it, leave a short `// consumed by the executor in PR-2` note rather than deleting it (it is a spec-mandated type). If clippy hard-errors, prefer wiring the field into a trivial read (e.g. include it in a `Debug` derive) over an `#[allow]`.

- [ ] **Step 2: Run the focused + regression suites**

Run (timeout 600000 each):
```
cargo test -q -- engine::write_transaction::tests
cargo test -q write_commit
cargo test -q writer_abort
cargo test -q staged_artifacts
cargo test -q writer_operation
cargo test -q write_coordinator
cargo test -q report_exec_status
cargo test -q -- runtime::coordinator
git diff --check
```
Expected: the 6 runner tests pass; the IW-4/IW-6 regression suites stay green (the coordinator refactor is behavior-preserving; the runner adds no routing); `git diff --check` clean.

If a previously-green test now fails, STOP and report it — do not paper over it.

- [ ] **Step 3: Commit any fmt-only changes**

```bash
git add -A
git commit -m "engine: fmt for write transaction runner" || echo "nothing to commit"
```

---

## Self-Review

**Spec coverage (PR-1 scope of `docs/design/specs/2026-06-08-iceberg-write-lifecycle-cutover-design.md`):**
- "define runner/spec types" → Task 2 (`IcebergWriteTransactionSpec`, `IcebergWriteSource`, `IcebergWriteCommitPolicy`, `IcebergWriteValidationPolicy`, `IcebergWriteTransactionOutcome`) + Task 3 (`IcebergWriteTransactionRunner`).
- "drive the operation state machine with fake writer + fake commit-service outcomes" → Task 3 success + Task 4 failure/empty cases, all via `FakeExecutor` against a real metadata-backed state.
- "extend the coordinator with execute_with_write_outcome returning CoordinatedQueryResult { query_result, write_commit, write_abort } while keeping execute() as a compatible wrapper; expose WriteAbortInput" → Task 1.
- "NO user-facing write routing change" → no SQL flow / `engine/mod.rs` routing edits; only the module registration.
- State machine: Preparing (Task 3 create) → Committing → Committed → Finalizing → Finalized (success), plus FailedKnownUncommitted (abort + commit-known-uncommitted), CommitUnknown, FinalizeFailedKnownCommitted, Aborted (empty) — all asserted.

**Placeholder scan:** No TBD/TODO. Each code step shows the code; each test shows assertions + command. Implementer notes flag exact-signature adaptations (RepositoryResult error mapping, test-helper sharing, id allocation) — these are verification-against-real-source instructions, not missing content.

**Type consistency:** `IcebergWriteTransactionExecutor` methods (`run_coordinated_write` / `commit` / `finalize`) are defined in Task 2 and implemented by `FakeExecutor` + called by the runner in Task 3 identically. `CoordinatedQueryResult { query_result, write_commit, write_abort }` is defined in Task 1 and consumed by the executor return + runner in Task 3. `IcebergOperationFact` field set used in `record_fact` matches the mapper output. `IcebergWriteTransactionOutcome { query_result, operation_id, committed_snapshot_id }` is consistent across runner + tests.

**Known boundaries (intentional, documented):** (1) `write_abort` is `None` on the coordinator success path; the real failure→abort wiring lands in PR-2 with routing. (2) Empty query-sourced → `Aborted` (the repo has no delete; chosen with the user). (3) The runner does not act on `IcebergWriteValidationPolicy` / `IcebergWriteSource` yet — they are spec-mandated types consumed by the real executor in PR-2. (4) Mid-write process crash (no commit/abort produced) leaves a `Preparing` record for a future recovery sweep (out of PR-1 scope).
