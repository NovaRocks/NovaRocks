# Iceberg Operation Lifecycle Phase 2 Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the shared Iceberg operation lifecycle state machine and metadata repository skeleton without wiring MV refresh or distributed writer execution paths yet.

**Architecture:** Keep operation facts in `src/meta/repository/iceberg_operation.rs`, following the existing metadata repository pattern used by MV and job records. Keep commit-service-specific mapping in `src/connector/iceberg/operation_lifecycle.rs` so the metadata layer does not depend on connector commit types. This PR creates the state model, persistence API, and mapping tests only; MV refresh and writer adapters remain Phase 3/4 work.

**Tech Stack:** Rust; NovaRocks metadata repository (`MetaStoreProvider`, Avro payloads, SQLite tests); Iceberg commit service typed errors from `src/connector/iceberg/commit/service.rs`; unit and integration-style repository tests through `cargo test`.

**Spec:** [docs/superpowers/specs/2026-06-05-iceberg-operation-lifecycle-design.md](../specs/2026-06-05-iceberg-operation-lifecycle-design.md)

---

## Scope

This plan implements only Phase 2 from the spec:

- operation state and state transition helper
- operation record storage model
- operation metadata repository create/load/list/transition skeleton
- recovery evidence, cleanup outcome, failure, and next-action record types
- commit-service outcome/error to operation fact mapping tests

It intentionally does not:

- modify `src/engine/mv/iceberg_refresh.rs`
- modify `src/runtime/write_coordinator.rs`
- add startup recovery migration
- expose diagnostic SQL or SHOW fields
- start partition MV work

## File Structure

- **Modify** `src/meta/keys.rs` — add the `iceberg_operation` metadata namespace.
- **Modify** `src/meta/repository/id_scopes.rs` — add a stable `iceberg.operation` id scope.
- **Modify** `src/meta/repository/mod.rs` — export the new repository module and include it in test seed decoding when needed.
- **Create** `src/meta/repository/iceberg_operation.rs` — operation state enums, record structs, transition helper, repository API, and module unit tests.
- **Modify** `src/connector/iceberg/mod.rs` — export the operation lifecycle mapping module.
- **Create** `src/connector/iceberg/operation_lifecycle.rs` — connector-layer mapping from `CommitOutcome` / `CommitServiceError` into operation state/fact records.
- **Modify** `tests/meta_repository.rs` — repository round-trip and optimistic-transition tests.

---

## Task 1: Add Operation Domain Model and Transition Helper

**Files:**
- Create: `src/meta/repository/iceberg_operation.rs`
- Modify: `src/meta/repository/mod.rs`
- Modify: `src/meta/keys.rs`
- Modify: `src/meta/repository/id_scopes.rs`
- Test: `src/meta/repository/iceberg_operation.rs`

- [ ] **Step 1: Add failing unit tests for state transitions**

Create `src/meta/repository/iceberg_operation.rs` with the standard ASF header and this initial test module:

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_state_as_str_is_stable_for_diagnostics() {
        assert_eq!(IcebergOperationState::Preparing.as_str(), "PREPARING");
        assert_eq!(IcebergOperationState::CommitUnknown.as_str(), "COMMIT_UNKNOWN");
        assert_eq!(
            IcebergOperationState::FinalizeFailedKnownCommitted.as_str(),
            "FINALIZE_FAILED_KNOWN_COMMITTED"
        );
    }

    #[test]
    fn transition_helper_allows_main_commit_path_and_idempotent_replay() {
        assert!(validate_operation_transition(
            IcebergOperationState::Preparing,
            IcebergOperationState::Writing
        )
        .is_ok());
        assert!(validate_operation_transition(
            IcebergOperationState::Writing,
            IcebergOperationState::Collecting
        )
        .is_ok());
        assert!(validate_operation_transition(
            IcebergOperationState::Collecting,
            IcebergOperationState::Committing
        )
        .is_ok());
        assert!(validate_operation_transition(
            IcebergOperationState::Committing,
            IcebergOperationState::Committed
        )
        .is_ok());
        assert!(validate_operation_transition(
            IcebergOperationState::Committed,
            IcebergOperationState::Finalizing
        )
        .is_ok());
        assert!(validate_operation_transition(
            IcebergOperationState::Finalizing,
            IcebergOperationState::Finalized
        )
        .is_ok());
        assert!(validate_operation_transition(
            IcebergOperationState::CommitUnknown,
            IcebergOperationState::CommitUnknown
        )
        .is_ok());
    }

    #[test]
    fn transition_helper_rejects_commit_unknown_to_aborted() {
        let err = validate_operation_transition(
            IcebergOperationState::CommitUnknown,
            IcebergOperationState::Aborted,
        )
        .expect_err("commit unknown must not be treated as aborted");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
        assert!(err.to_string().contains("COMMIT_UNKNOWN"));
        assert!(err.to_string().contains("ABORTED"));
    }

    #[test]
    fn transition_helper_routes_finalize_failure_to_known_committed_failure() {
        assert!(validate_operation_transition(
            IcebergOperationState::Finalizing,
            IcebergOperationState::FinalizeFailedKnownCommitted
        )
        .is_ok());
        assert!(!IcebergOperationState::FinalizeFailedKnownCommitted.is_finished());
        assert!(IcebergOperationState::Finalized.is_finished());
        assert!(IcebergOperationState::Aborted.is_finished());
        assert!(IcebergOperationState::FailedKnownUncommitted.is_finished());
    }
}
```

- [ ] **Step 2: Declare the module and namespace so the test compiles far enough to fail on missing types**

In `src/meta/repository/mod.rs`, add:

```rust
pub mod iceberg_operation;
```

Place it near `pub mod iceberg_catalog;`.

In `src/meta/keys.rs`, add:

```rust
pub const NS_ICEBERG_OPERATION: &str = "iceberg_operation";
```

In `src/meta/repository/id_scopes.rs`, add:

```rust
pub fn iceberg_operation() -> IdScope {
    stable("iceberg.operation")
}
```

- [ ] **Step 3: Run the state tests to verify they fail**

Run:

```bash
cargo test --lib meta::repository::iceberg_operation::tests -- --nocapture
```

Expected: compile fails because `IcebergOperationState`, `validate_operation_transition`, and `RepositoryErrorKind` are not defined/imported yet.

- [ ] **Step 4: Implement operation enums and transition helper**

Replace the top of `src/meta/repository/iceberg_operation.rs` above the test module with:

```rust
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::meta::repository::{RepositoryError, RepositoryResult};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationKind {
    InsertAppend,
    InsertOverwrite,
    RowDelta,
    MvRefresh,
    Maintenance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationState {
    Preparing,
    Writing,
    Collecting,
    Committing,
    Committed,
    CommitUnknown,
    Finalizing,
    Finalized,
    Aborting,
    Aborted,
    FailedKnownUncommitted,
    FinalizeFailedKnownCommitted,
}

impl IcebergOperationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "PREPARING",
            Self::Writing => "WRITING",
            Self::Collecting => "COLLECTING",
            Self::Committing => "COMMITTING",
            Self::Committed => "COMMITTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
            Self::Finalizing => "FINALIZING",
            Self::Finalized => "FINALIZED",
            Self::Aborting => "ABORTING",
            Self::Aborted => "ABORTED",
            Self::FailedKnownUncommitted => "FAILED_KNOWN_UNCOMMITTED",
            Self::FinalizeFailedKnownCommitted => "FINALIZE_FAILED_KNOWN_COMMITTED",
        }
    }

    pub fn is_finished(self) -> bool {
        matches!(
            self,
            Self::Finalized | Self::Aborted | Self::FailedKnownUncommitted
        )
    }
}

pub fn validate_operation_transition(
    from: IcebergOperationState,
    to: IcebergOperationState,
) -> RepositoryResult<()> {
    if from == to {
        return Ok(());
    }
    let allowed = matches!(
        (from, to),
        (IcebergOperationState::Preparing, IcebergOperationState::Writing)
            | (IcebergOperationState::Preparing, IcebergOperationState::Committing)
            | (IcebergOperationState::Preparing, IcebergOperationState::Aborting)
            | (
                IcebergOperationState::Preparing,
                IcebergOperationState::FailedKnownUncommitted
            )
            | (IcebergOperationState::Writing, IcebergOperationState::Collecting)
            | (IcebergOperationState::Writing, IcebergOperationState::Committing)
            | (IcebergOperationState::Writing, IcebergOperationState::Aborting)
            | (
                IcebergOperationState::Writing,
                IcebergOperationState::FailedKnownUncommitted
            )
            | (IcebergOperationState::Collecting, IcebergOperationState::Committing)
            | (IcebergOperationState::Collecting, IcebergOperationState::Aborting)
            | (
                IcebergOperationState::Collecting,
                IcebergOperationState::FailedKnownUncommitted
            )
            | (IcebergOperationState::Committing, IcebergOperationState::Committed)
            | (IcebergOperationState::Committing, IcebergOperationState::CommitUnknown)
            | (
                IcebergOperationState::Committing,
                IcebergOperationState::FailedKnownUncommitted
            )
            | (IcebergOperationState::Committed, IcebergOperationState::Finalizing)
            | (IcebergOperationState::Committed, IcebergOperationState::Finalized)
            | (IcebergOperationState::Finalizing, IcebergOperationState::Finalized)
            | (
                IcebergOperationState::Finalizing,
                IcebergOperationState::FinalizeFailedKnownCommitted
            )
            | (IcebergOperationState::Finalizing, IcebergOperationState::CommitUnknown)
            | (IcebergOperationState::Aborting, IcebergOperationState::Aborted)
            | (
                IcebergOperationState::Aborting,
                IcebergOperationState::FailedKnownUncommitted
            )
    );
    if allowed {
        Ok(())
    } else {
        Err(RepositoryError::conflict(format!(
            "invalid Iceberg operation state transition from {} to {}",
            from.as_str(),
            to.as_str()
        )))
    }
}
```

Also add this import inside the test module:

```rust
use crate::meta::repository::RepositoryErrorKind;
```

- [ ] **Step 5: Run the state tests to verify they pass**

Run:

```bash
cargo test --lib meta::repository::iceberg_operation::tests -- --nocapture
```

Expected: PASS for the four state-machine tests.

- [ ] **Step 6: Commit Task 1**

Run:

```bash
git add src/meta/keys.rs src/meta/repository/id_scopes.rs src/meta/repository/mod.rs src/meta/repository/iceberg_operation.rs
git commit -m "feat: add Iceberg operation state model"
```

---

## Task 2: Add Operation Record Repository Skeleton

**Files:**
- Modify: `src/meta/repository/iceberg_operation.rs`
- Modify: `src/meta/repository/mod.rs`
- Test: `tests/meta_repository.rs`

- [ ] **Step 1: Add failing repository round-trip tests**

In `tests/meta_repository.rs`, extend the imports:

```rust
use novarocks::meta::repository::iceberg_operation::{
    CreateIcebergOperationRequest, IcebergOperationKind, IcebergOperationRepository,
    IcebergOperationState, IcebergOperationTarget,
};
```

Add these tests near the other metadata repository tests:

```rust
#[test]
fn iceberg_operation_repository_create_load_and_list_unfinished()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let operation_id = {
        let mut txn = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::MvRefresh,
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "analytics".to_string(),
                    table: "mv_sales".to_string(),
                    ref_name: Some("main".to_string()),
                },
                attempt_id: "attempt-1".to_string(),
                base_snapshot_id: Some(42),
                base_snapshot_map: BTreeMap::from([("ice.sales.orders".to_string(), 7)]),
                staged_artifacts: vec!["s3://warehouse/mv/_staging/a.parquet".to_string()],
                created_at_ms: 1000,
            },
        )?;
        assert_eq!(stored.state, IcebergOperationState::Preparing);
        assert_eq!(stored.created_at_ms, 1000);
        assert_eq!(stored.updated_at_ms, 1000);
        assert_eq!(stored.finished_at_ms, None);
        txn.commit()?;
        stored.operation_id
    };

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.operation_id, operation_id);
    assert_eq!(loaded.operation_kind, IcebergOperationKind::MvRefresh);
    assert_eq!(loaded.target.catalog, "ice");
    assert_eq!(loaded.target.namespace, "analytics");
    assert_eq!(loaded.target.table, "mv_sales");
    assert_eq!(loaded.target.ref_name.as_deref(), Some("main"));
    assert_eq!(loaded.base_snapshot_id, Some(42));
    assert_eq!(loaded.base_snapshot_map["ice.sales.orders"], 7);
    assert_eq!(loaded.staged_artifacts.len(), 1);

    let unfinished = repository.list_unfinished_operations(read.as_ref())?;
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].operation_id, operation_id);

    Ok(())
}

#[test]
fn iceberg_operation_repository_finished_operations_are_not_unfinished()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let operation_id = {
        let mut txn = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::InsertAppend,
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "orders".to_string(),
                    ref_name: None,
                },
                attempt_id: "attempt-1".to_string(),
                base_snapshot_id: None,
                base_snapshot_map: BTreeMap::new(),
                staged_artifacts: Vec::new(),
                created_at_ms: 1000,
            },
        )?;
        txn.commit()?;
        stored.operation_id
    };

    {
        let mut txn = provider.begin_write("transition iceberg operation")?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Committing,
            1100,
        )?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Committed,
            1200,
        )?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Finalized,
            1300,
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::Finalized);
    assert_eq!(loaded.updated_at_ms, 1300);
    assert_eq!(loaded.finished_at_ms, Some(1300));
    assert!(repository.list_unfinished_operations(read.as_ref())?.is_empty());

    Ok(())
}
```

- [ ] **Step 2: Run the repository tests to verify they fail**

Run:

```bash
cargo test --test meta_repository iceberg_operation_repository -- --nocapture
```

Expected: compile fails because repository structs and methods do not exist yet.

- [ ] **Step 3: Add record structs and repository API**

In `src/meta/repository/iceberg_operation.rs`, add these imports:

```rust
use crate::meta::keys::NS_ICEBERG_OPERATION;
use crate::meta::repository::{
    decode_payload_for_kind, encode_record_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaRevision, MetaWriteTxn,
};
```

Add this constant and repository marker:

```rust
const ICEBERG_OPERATION_KIND: &str = "iceberg.operation";

#[derive(Default)]
pub struct IcebergOperationRepository;
```

Add these record structs below the state helper:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergOperationTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    #[serde(default)]
    pub ref_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationFailureKind {
    KnownUncommitted,
    Unknown,
    FinalizeKnownCommitted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationNextAction {
    None,
    RetryAbort,
    RetryFinalize,
    ManualInspect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergOperationFailureRecord {
    pub kind: IcebergOperationFailureKind,
    pub message: String,
    pub next_action: IcebergOperationNextAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergCommitOutcomeRecord {
    pub snapshot_id: i64,
    pub written_manifest_paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergCleanupOutcomeRecord {
    pub attempted: bool,
    pub error_count: i64,
    pub error_paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergRecoveryEvidenceRecord {
    pub table_ident: String,
    pub commit_op_kind: String,
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: Option<i64>,
    pub staging_dir: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredIcebergOperation {
    pub operation_id: i64,
    pub operation_kind: IcebergOperationKind,
    pub target: IcebergOperationTarget,
    pub state: IcebergOperationState,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
    pub staged_artifacts: Vec<String>,
    #[serde(default)]
    pub commit_request: Option<String>,
    #[serde(default)]
    pub commit_outcome: Option<IcebergCommitOutcomeRecord>,
    #[serde(default)]
    pub cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    #[serde(default)]
    pub recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    #[serde(default)]
    pub failure: Option<IcebergOperationFailureRecord>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub finished_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedIcebergOperation {
    pub record_revision: MetaRevision,
    pub value: StoredIcebergOperation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIcebergOperationRequest {
    pub operation_kind: IcebergOperationKind,
    pub target: IcebergOperationTarget,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
    pub staged_artifacts: Vec<String>,
    pub created_at_ms: i64,
}
```

Add this repository implementation:

```rust
impl IcebergOperationRepository {
    pub fn create_operation(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateIcebergOperationRequest,
    ) -> RepositoryResult<StoredIcebergOperation> {
        let operation_id = txn.allocate_id(id_scopes::iceberg_operation())?;
        let stored = StoredIcebergOperation {
            operation_id,
            operation_kind: req.operation_kind,
            target: req.target,
            state: IcebergOperationState::Preparing,
            attempt_id: req.attempt_id,
            base_snapshot_id: req.base_snapshot_id,
            base_snapshot_map: req.base_snapshot_map,
            staged_artifacts: req.staged_artifacts,
            commit_request: None,
            commit_outcome: None,
            cleanup_outcome: None,
            recovery_evidence: None,
            failure: None,
            created_at_ms: req.created_at_ms,
            updated_at_ms: req.created_at_ms,
            finished_at_ms: None,
        };
        put_operation(txn, &stored, ExpectedRevision::NotExists)?;
        Ok(stored)
    }

    pub fn load_operation(
        &self,
        txn: &dyn MetaReadTxn,
        operation_id: i64,
    ) -> RepositoryResult<Option<StoredIcebergOperation>> {
        Ok(load_versioned_operation(txn, operation_id)?.map(|versioned| versioned.value))
    }

    pub fn list_unfinished_operations(
        &self,
        txn: &dyn MetaReadTxn,
    ) -> RepositoryResult<Vec<StoredIcebergOperation>> {
        Ok(txn
            .scan(&key_prefix_operation()?, None)?
            .into_iter()
            .map(decode_operation_record)
            .collect::<RepositoryResult<Vec<_>>>()?
            .into_iter()
            .map(|versioned| versioned.value)
            .filter(|operation| !operation.state.is_finished())
            .collect())
    }

    pub fn transition_operation(
        &self,
        txn: &mut dyn MetaWriteTxn,
        operation_id: i64,
        to_state: IcebergOperationState,
        now_ms: i64,
    ) -> RepositoryResult<()> {
        let mut versioned = load_versioned_operation(txn, operation_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("iceberg operation {operation_id} not found"))
        })?;
        validate_operation_transition(versioned.value.state, to_state)?;
        versioned.value.state = to_state;
        versioned.value.updated_at_ms = now_ms;
        if to_state.is_finished() {
            versioned.value.finished_at_ms = Some(now_ms);
        }
        put_operation(
            txn,
            &versioned.value,
            ExpectedRevision::Exact(versioned.record_revision),
        )
    }
}
```

Add these helper functions:

```rust
fn load_versioned_operation(
    txn: &dyn MetaReadTxn,
    operation_id: i64,
) -> RepositoryResult<Option<VersionedIcebergOperation>> {
    txn.get(&key_operation(operation_id)?)?
        .map(decode_operation_record)
        .transpose()
}

fn decode_operation_record(record: MetaRecord) -> RepositoryResult<VersionedIcebergOperation> {
    if record.kind.as_str() != ICEBERG_OPERATION_KIND {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has kind {}, expected {ICEBERG_OPERATION_KIND}",
            record.key.canonical_path(),
            record.kind.as_str()
        )));
    }
    let value = decode_payload_for_kind(ICEBERG_OPERATION_KIND, &record.payload).map_err(|err| {
        RepositoryError::provider(format!(
            "failed to decode metadata record {} as {ICEBERG_OPERATION_KIND}: {err}",
            record.key.canonical_path()
        ))
    })?;
    Ok(VersionedIcebergOperation {
        record_revision: record.revision,
        value,
    })
}

fn put_operation(
    txn: &mut dyn MetaWriteTxn,
    operation: &StoredIcebergOperation,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_operation(operation.operation_id)?,
        record_kind(ICEBERG_OPERATION_KIND)?,
        expected,
        encode_record_payload(ICEBERG_OPERATION_KIND, operation)?,
    ))?;
    Ok(())
}

fn key_operation(operation_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_ICEBERG_OPERATION,
        ["by-id".to_string(), operation_id.to_string()],
    )?)
}

fn key_prefix_operation() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(NS_ICEBERG_OPERATION, ["by-id"])?)
}

fn record_kind(value: &str) -> RepositoryResult<MetaRecordKind> {
    Ok(MetaRecordKind::new(value)?)
}
```

- [ ] **Step 4: Register the Avro test seed decoder**

In `src/meta/repository/mod.rs`, import the stored operation type in the `test_avro_seed` module:

```rust
use crate::meta::repository::iceberg_operation::StoredIcebergOperation;
```

Add this match arm in `encode_seed_payload`:

```rust
"iceberg.operation" => encode_from_json::<StoredIcebergOperation>(kind, payload),
```

- [ ] **Step 5: Run repository tests**

Run:

```bash
cargo test --test meta_repository iceberg_operation_repository -- --nocapture
```

Expected: PASS for both `iceberg_operation_repository_*` tests.

- [ ] **Step 6: Run module tests again**

Run:

```bash
cargo test --lib meta::repository::iceberg_operation::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit Task 2**

Run:

```bash
git add src/meta/repository/iceberg_operation.rs src/meta/repository/mod.rs tests/meta_repository.rs
git commit -m "feat: persist Iceberg operation records"
```

---

## Task 3: Add Commit Service to Operation Fact Mapping

**Files:**
- Create: `src/connector/iceberg/operation_lifecycle.rs`
- Modify: `src/connector/iceberg/mod.rs`
- Test: `src/connector/iceberg/operation_lifecycle.rs`

- [ ] **Step 1: Add failing connector-layer mapping tests**

Create `src/connector/iceberg/operation_lifecycle.rs` with the standard ASF header and this test module:

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

#[cfg(test)]
mod tests {
    use crate::connector::iceberg::commit::{
        CleanupAttempt, CommitOpKind, CommitOutcome, CommitServiceError, RecoveryEvidence,
    };
    use crate::meta::repository::iceberg_operation::{
        IcebergOperationFailureKind, IcebergOperationNextAction, IcebergOperationState,
    };

    use super::*;

    #[test]
    fn committed_outcome_maps_to_committed_state_and_snapshot_record() {
        let outcome = CommitOutcome {
            new_snapshot_id: 99,
            written_manifest_paths: vec!["s3://warehouse/metadata/m0.avro".to_string()],
        };
        let fact = operation_fact_from_commit_result(Ok(&outcome));
        assert_eq!(fact.state, IcebergOperationState::Committed);
        assert_eq!(fact.commit_outcome.expect("outcome").snapshot_id, 99);
        assert_eq!(fact.failure, None);
        assert_eq!(fact.cleanup_outcome, None);
        assert_eq!(fact.recovery_evidence, None);
    }

    #[test]
    fn known_uncommitted_error_maps_cleanup_and_failure() {
        let error = CommitServiceError::known_uncommitted(
            "catalog commit conflict".to_string(),
            CleanupAttempt::completed(vec!["s3://warehouse/data/a.parquet".to_string()]),
        );
        let fact = operation_fact_from_commit_result(Err(&error));
        assert_eq!(fact.state, IcebergOperationState::FailedKnownUncommitted);
        assert_eq!(
            fact.failure.as_ref().expect("failure").kind,
            IcebergOperationFailureKind::KnownUncommitted
        );
        assert_eq!(
            fact.failure.as_ref().expect("failure").next_action,
            IcebergOperationNextAction::None
        );
        assert_eq!(fact.cleanup_outcome.as_ref().expect("cleanup").attempted, true);
        assert_eq!(fact.cleanup_outcome.as_ref().expect("cleanup").error_count, 1);
        assert_eq!(fact.recovery_evidence, None);
    }

    #[test]
    fn unknown_error_maps_to_commit_unknown_with_manual_inspect() {
        let error = CommitServiceError::unknown(
            "connection reset by peer".to_string(),
            RecoveryEvidence {
                table_ident: "ice.sales.orders".to_string(),
                op_kind: CommitOpKind::FastAppend,
                base_snapshot_id: Some(42),
                base_sequence_number: 7,
                staging_dir: "s3://warehouse/orders/_staging/attempt-1".to_string(),
            },
        );
        let fact = operation_fact_from_commit_result(Err(&error));
        assert_eq!(fact.state, IcebergOperationState::CommitUnknown);
        assert_eq!(
            fact.failure.as_ref().expect("failure").kind,
            IcebergOperationFailureKind::Unknown
        );
        assert_eq!(
            fact.failure.as_ref().expect("failure").next_action,
            IcebergOperationNextAction::ManualInspect
        );
        assert_eq!(
            fact.recovery_evidence.as_ref().expect("evidence").staging_dir,
            "s3://warehouse/orders/_staging/attempt-1"
        );
        assert_eq!(fact.cleanup_outcome, None);
    }

    #[test]
    fn finalize_failure_maps_to_known_committed_failure() {
        let fact = operation_fact_from_finalize_failure("mv metadata update failed".to_string());
        assert_eq!(fact.state, IcebergOperationState::FinalizeFailedKnownCommitted);
        assert_eq!(
            fact.failure.as_ref().expect("failure").kind,
            IcebergOperationFailureKind::FinalizeKnownCommitted
        );
        assert_eq!(
            fact.failure.as_ref().expect("failure").next_action,
            IcebergOperationNextAction::RetryFinalize
        );
    }
}
```

In `src/connector/iceberg/mod.rs`, add:

```rust
pub mod operation_lifecycle;
```

- [ ] **Step 2: Run the mapping tests to verify they fail**

Run:

```bash
cargo test --lib connector::iceberg::operation_lifecycle::tests -- --nocapture
```

Expected: compile fails because the mapping functions and `IcebergOperationFact` do not exist yet.

- [ ] **Step 3: Implement mapping helpers**

Replace the top of `src/connector/iceberg/operation_lifecycle.rs` above the test module with:

```rust
use crate::connector::iceberg::commit::{CleanupAttempt, CommitOutcome, CommitServiceError};
use crate::meta::repository::iceberg_operation::{
    IcebergCleanupOutcomeRecord, IcebergCommitOutcomeRecord, IcebergOperationFailureKind,
    IcebergOperationFailureRecord, IcebergOperationNextAction, IcebergOperationState,
    IcebergRecoveryEvidenceRecord,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergOperationFact {
    pub state: IcebergOperationState,
    pub commit_outcome: Option<IcebergCommitOutcomeRecord>,
    pub cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    pub recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    pub failure: Option<IcebergOperationFailureRecord>,
}

pub fn operation_fact_from_commit_result(
    result: Result<&CommitOutcome, &CommitServiceError>,
) -> IcebergOperationFact {
    match result {
        Ok(outcome) => IcebergOperationFact {
            state: IcebergOperationState::Committed,
            commit_outcome: Some(IcebergCommitOutcomeRecord {
                snapshot_id: outcome.new_snapshot_id,
                written_manifest_paths: outcome.written_manifest_paths.clone(),
            }),
            cleanup_outcome: None,
            recovery_evidence: None,
            failure: None,
        },
        Err(CommitServiceError::KnownUncommitted { message, cleanup }) => IcebergOperationFact {
            state: IcebergOperationState::FailedKnownUncommitted,
            commit_outcome: None,
            cleanup_outcome: Some(cleanup_outcome_from_attempt(cleanup)),
            recovery_evidence: None,
            failure: Some(IcebergOperationFailureRecord {
                kind: IcebergOperationFailureKind::KnownUncommitted,
                message: message.clone(),
                next_action: if cleanup.attempted {
                    IcebergOperationNextAction::None
                } else {
                    IcebergOperationNextAction::RetryAbort
                },
            }),
        },
        Err(CommitServiceError::Unknown { message, evidence }) => IcebergOperationFact {
            state: IcebergOperationState::CommitUnknown,
            commit_outcome: None,
            cleanup_outcome: None,
            recovery_evidence: Some(IcebergRecoveryEvidenceRecord {
                table_ident: evidence.table_ident.clone(),
                commit_op_kind: format!("{:?}", evidence.op_kind),
                base_snapshot_id: evidence.base_snapshot_id,
                base_sequence_number: Some(evidence.base_sequence_number),
                staging_dir: evidence.staging_dir.clone(),
            }),
            failure: Some(IcebergOperationFailureRecord {
                kind: IcebergOperationFailureKind::Unknown,
                message: message.clone(),
                next_action: IcebergOperationNextAction::ManualInspect,
            }),
        },
    }
}

pub fn operation_fact_from_finalize_failure(message: String) -> IcebergOperationFact {
    IcebergOperationFact {
        state: IcebergOperationState::FinalizeFailedKnownCommitted,
        commit_outcome: None,
        cleanup_outcome: None,
        recovery_evidence: None,
        failure: Some(IcebergOperationFailureRecord {
            kind: IcebergOperationFailureKind::FinalizeKnownCommitted,
            message,
            next_action: IcebergOperationNextAction::RetryFinalize,
        }),
    }
}

fn cleanup_outcome_from_attempt(cleanup: &CleanupAttempt) -> IcebergCleanupOutcomeRecord {
    IcebergCleanupOutcomeRecord {
        attempted: cleanup.attempted,
        error_count: cleanup.error_count as i64,
        error_paths: cleanup.error_paths.clone(),
    }
}
```

- [ ] **Step 4: Run mapping tests**

Run:

```bash
cargo test --lib connector::iceberg::operation_lifecycle::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run existing commit service tests**

Run:

```bash
cargo test --lib connector::iceberg::commit::service::tests -- --nocapture
```

Expected: PASS. This confirms the mapping uses the Phase 1 typed errors without regressing their local behavior.

- [ ] **Step 6: Commit Task 3**

Run:

```bash
git add src/connector/iceberg/mod.rs src/connector/iceberg/operation_lifecycle.rs
git commit -m "feat: map Iceberg commit facts to operation states"
```

---

## Task 4: Add Repository Fact Recording Helpers

**Files:**
- Modify: `src/meta/repository/iceberg_operation.rs`
- Test: `tests/meta_repository.rs`

- [ ] **Step 1: Add failing tests for recording operation facts**

In `tests/meta_repository.rs`, extend the `iceberg_operation` import with:

```rust
IcebergCleanupOutcomeRecord, IcebergCommitOutcomeRecord, IcebergOperationFactUpdate,
IcebergOperationFailureKind, IcebergOperationFailureRecord, IcebergOperationNextAction,
```

Add this test:

```rust
#[test]
fn iceberg_operation_repository_records_commit_unknown_fact_without_finishing()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let operation_id = {
        let mut txn = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::InsertAppend,
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "orders".to_string(),
                    ref_name: None,
                },
                attempt_id: "attempt-unknown".to_string(),
                base_snapshot_id: Some(42),
                base_snapshot_map: BTreeMap::new(),
                staged_artifacts: vec!["s3://warehouse/orders/_staging/a.parquet".to_string()],
                created_at_ms: 1000,
            },
        )?;
        repository.transition_operation(
            txn.as_mut(),
            stored.operation_id,
            IcebergOperationState::Committing,
            1100,
        )?;
        txn.commit()?;
        stored.operation_id
    };

    {
        let mut txn = provider.begin_write("record commit unknown")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::CommitUnknown,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: Some(IcebergOperationFailureRecord {
                    kind: IcebergOperationFailureKind::Unknown,
                    message: "connection reset by peer".to_string(),
                    next_action: IcebergOperationNextAction::ManualInspect,
                }),
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let stored = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(stored.state, IcebergOperationState::CommitUnknown);
    assert_eq!(stored.finished_at_ms, None);
    assert_eq!(
        stored.failure.as_ref().expect("failure").next_action,
        IcebergOperationNextAction::ManualInspect
    );
    assert_eq!(repository.list_unfinished_operations(read.as_ref())?.len(), 1);
    Ok(())
}

#[test]
fn iceberg_operation_repository_records_known_uncommitted_cleanup_and_finishes()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let operation_id = {
        let mut txn = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::InsertAppend,
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "orders".to_string(),
                    ref_name: None,
                },
                attempt_id: "attempt-known".to_string(),
                base_snapshot_id: Some(42),
                base_snapshot_map: BTreeMap::new(),
                staged_artifacts: vec!["s3://warehouse/orders/_staging/a.parquet".to_string()],
                created_at_ms: 1000,
            },
        )?;
        repository.transition_operation(
            txn.as_mut(),
            stored.operation_id,
            IcebergOperationState::Committing,
            1100,
        )?;
        txn.commit()?;
        stored.operation_id
    };

    {
        let mut txn = provider.begin_write("record known uncommitted")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FailedKnownUncommitted,
                commit_outcome: None,
                cleanup_outcome: Some(IcebergCleanupOutcomeRecord {
                    attempted: true,
                    error_count: 1,
                    error_paths: vec!["s3://warehouse/orders/_staging/a.parquet".to_string()],
                }),
                recovery_evidence: None,
                failure: Some(IcebergOperationFailureRecord {
                    kind: IcebergOperationFailureKind::KnownUncommitted,
                    message: "data invalid".to_string(),
                    next_action: IcebergOperationNextAction::None,
                }),
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let stored = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(stored.state, IcebergOperationState::FailedKnownUncommitted);
    assert_eq!(stored.finished_at_ms, Some(1200));
    assert_eq!(stored.cleanup_outcome.as_ref().expect("cleanup").error_count, 1);
    assert!(repository.list_unfinished_operations(read.as_ref())?.is_empty());
    Ok(())
}
```

- [ ] **Step 2: Run the fact-recording tests to verify they fail**

Run:

```bash
cargo test --test meta_repository iceberg_operation_repository_records -- --nocapture
```

Expected: compile fails because `IcebergOperationFactUpdate` and `record_operation_fact` do not exist yet.

- [ ] **Step 3: Implement fact update request and method**

In `src/meta/repository/iceberg_operation.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergOperationFactUpdate {
    pub operation_id: i64,
    pub state: IcebergOperationState,
    pub commit_outcome: Option<IcebergCommitOutcomeRecord>,
    pub cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    pub recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    pub failure: Option<IcebergOperationFailureRecord>,
    pub now_ms: i64,
}
```

Inside `impl IcebergOperationRepository`, add:

```rust
pub fn record_operation_fact(
    &self,
    txn: &mut dyn MetaWriteTxn,
    req: IcebergOperationFactUpdate,
) -> RepositoryResult<()> {
    let mut versioned = load_versioned_operation(txn, req.operation_id)?.ok_or_else(|| {
        RepositoryError::not_found(format!("iceberg operation {} not found", req.operation_id))
    })?;
    validate_operation_transition(versioned.value.state, req.state)?;
    versioned.value.state = req.state;
    versioned.value.commit_outcome = req.commit_outcome;
    versioned.value.cleanup_outcome = req.cleanup_outcome;
    versioned.value.recovery_evidence = req.recovery_evidence;
    versioned.value.failure = req.failure;
    versioned.value.updated_at_ms = req.now_ms;
    if req.state.is_finished() {
        versioned.value.finished_at_ms = Some(req.now_ms);
    }
    put_operation(
        txn,
        &versioned.value,
        ExpectedRevision::Exact(versioned.record_revision),
    )
}
```

- [ ] **Step 4: Run fact-recording tests**

Run:

```bash
cargo test --test meta_repository iceberg_operation_repository_records -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run all operation repository tests**

Run:

```bash
cargo test --test meta_repository iceberg_operation -- --nocapture
```

Expected: PASS for all tests whose names contain `iceberg_operation`.

- [ ] **Step 6: Commit Task 4**

Run:

```bash
git add src/meta/repository/iceberg_operation.rs tests/meta_repository.rs
git commit -m "feat: record Iceberg operation lifecycle facts"
```

---

## Task 5: Final Formatting and Validation

**Files:**
- Modify only if formatting touches files:
  - `src/meta/keys.rs`
  - `src/meta/repository/id_scopes.rs`
  - `src/meta/repository/mod.rs`
  - `src/meta/repository/iceberg_operation.rs`
  - `src/connector/iceberg/mod.rs`
  - `src/connector/iceberg/operation_lifecycle.rs`
  - `tests/meta_repository.rs`

- [ ] **Step 1: Format touched Rust files**

Run:

```bash
cargo fmt -- src/meta/keys.rs src/meta/repository/id_scopes.rs src/meta/repository/mod.rs src/meta/repository/iceberg_operation.rs src/connector/iceberg/mod.rs src/connector/iceberg/operation_lifecycle.rs tests/meta_repository.rs
```

Expected: command exits 0.

- [ ] **Step 2: Verify no unrelated files were formatted**

Run:

```bash
git diff --stat
```

Expected: no diff if previous task commits already contained formatted code, or only the files listed in this task.

- [ ] **Step 3: Run focused state/repository/mapping tests**

Run:

```bash
cargo test --lib meta::repository::iceberg_operation::tests -- --nocapture
cargo test --test meta_repository iceberg_operation -- --nocapture
cargo test --lib connector::iceberg::operation_lifecycle::tests -- --nocapture
```

Expected: all commands PASS.

- [ ] **Step 4: Run broader compile checks**

Run:

```bash
cargo test --lib connector::iceberg::commit --no-run
cargo test --test meta_repository --no-run
```

Expected: both commands PASS. Existing warnings are acceptable only if they were already present before this Phase 2 branch.

- [ ] **Step 5: Verify acceptance criteria from the spec**

Run:

```bash
rg -n "IcebergOperationState|validate_operation_transition|CommitUnknown|FinalizeFailedKnownCommitted|IcebergOperationRepository|operation_fact_from_commit_result" src/meta src/connector tests
```

Expected:

- `IcebergOperationState` is defined in `src/meta/repository/iceberg_operation.rs`.
- `validate_operation_transition` is defined and tested.
- `CommitUnknown` transition tests reject direct transition to `Aborted`.
- `FinalizeFailedKnownCommitted` is represented and mapped from finalize failure.
- `IcebergOperationRepository` exists and can persist/list operation records.
- `operation_fact_from_commit_result` exists in connector Iceberg lifecycle mapping.

- [ ] **Step 6: Commit formatting changes if any**

If Step 1 produced formatting-only changes, run:

```bash
git add src/meta/keys.rs src/meta/repository/id_scopes.rs src/meta/repository/mod.rs src/meta/repository/iceberg_operation.rs src/connector/iceberg/mod.rs src/connector/iceberg/operation_lifecycle.rs tests/meta_repository.rs
git commit -m "chore: format Iceberg operation lifecycle skeleton"
```

If there is no diff, do not create a commit.

- [ ] **Step 7: Final clean working tree check**

Run:

```bash
git status --short --branch
```

Expected: clean working tree on `codex/iceberg-operation-lifecycle-skeleton`.

When all checks pass, Phase 2 is ready for review. The next plan after this PR is Phase 3: MV refresh adapter integration with shared operation lifecycle.
