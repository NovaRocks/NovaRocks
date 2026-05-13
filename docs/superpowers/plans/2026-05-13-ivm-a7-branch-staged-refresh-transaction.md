# IVM-A7 Branch-Staged Refresh Transaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Iceberg-backed MV refresh as a branch-staged publish transaction so Iceberg `main` and NovaRocks MV metadata remain recoverably consistent.

**Architecture:** NovaRocks metadata records refresh intent, staging outcome, publish outcome, and terminal state. Iceberg refresh writes to a NovaRocks-owned staging branch, then atomically advances `main` with an expected-ref guard. Startup and pre-refresh recovery reconcile unfinished refresh records against Iceberg refs and snapshots.

**Tech Stack:** Rust, `iceberg-rust`, NovaRocks `MetaStoreProvider` / `MvMetaRepository`, existing Iceberg commit actions, `cargo test`, SQL test runner for final smoke coverage.

---

## File Structure

- `src/meta/repository/mv.rs`
  - Owns durable MV refresh transaction records and state transitions.
  - Add branch-staged refresh fields, request structs, and transition methods.

- `tests/meta_repository.rs`
  - Repository-level unit tests for lifecycle, abort, commit-unknown, and active refresh rejection.

- `src/connector/iceberg/commit/mv_refresh_ref.rs`
  - New focused helper module for MV refresh staging branch, publish-to-main, branch cleanup, and snapshot marker validation.

- `src/connector/iceberg/commit/mod.rs`
  - Export the new helper types.

- `src/connector/iceberg/commit/action.rs`
  - Carry snapshot summary properties through `CommitCtx`.

- `src/connector/iceberg/commit/run.rs`
  - Carry snapshot summary properties through `RunInput`.

- `src/connector/iceberg/commit/fast_append.rs`
  - Add refresh marker properties to custom snapshot summaries.
  - Reject non-`main` target refs on the built-in v2 append path.

- `src/connector/iceberg/commit/overwrite.rs`
  - Add refresh marker properties to custom overwrite snapshot summaries.

- `src/engine/mv/iceberg_refresh.rs`
  - Orchestrate branch-staged refresh, publish, finalize, cleanup, and recovery.
  - Create new Iceberg MV targets as branch-capable v3 row-lineage tables.

- `src/engine/mod.rs`
  - Invoke Iceberg MV refresh recovery after Iceberg catalog restore.

- `tests/meta_framework_flow.rs`
  - Update the old external-commit recovery test to the branch-staged lifecycle.

## Task 1: Repository Refresh Transaction Model

**Files:**
- Modify: `src/meta/repository/mv.rs`
- Modify: `tests/meta_repository.rs`
- Modify: `tests/meta_framework_flow.rs`

- [ ] **Step 1: Add failing repository lifecycle test**

Append this test to `tests/meta_repository.rs` near the existing MV repository tests:

```rust
#[test]
fn mv_repository_branch_staged_refresh_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("analytics".to_string()),
                target_table: Some("orders_mv".to_string()),
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
        definition.mv_id
    };

    let marker_token = "marker-token-1001".to_string();

    let refresh_id = {
        let mut txn = provider.begin_write("begin branch staged refresh")?;
        let mut base_snapshots = BTreeMap::new();
        base_snapshots.insert("iceberg.sales.orders".to_string(), 50);
        let refresh = repository.begin_iceberg_refresh_intent(
            txn.as_mut(),
            BeginIcebergMvRefreshRequest {
                mv_id,
                target_catalog: "ice".to_string(),
                target_namespace: "analytics".to_string(),
                target_table: "orders_mv".to_string(),
                staging_branch: "__nova_mv_refresh_1_1001".to_string(),
                expected_main_snapshot_id: Some(200),
                base_snapshots,
                marker_token: marker_token.clone(),
            },
        )?;
        assert_eq!(refresh.state, MvRefreshState::IntentCreated);
        assert_eq!(refresh.staging_branch.as_deref(), Some("__nova_mv_refresh_1_1001"));
        assert_eq!(refresh.expected_main_snapshot_id, Some(200));
        txn.commit()?;
        refresh.refresh_id
    };

    {
        let mut txn = provider.begin_write("record staging commit")?;
        repository.record_staging_commit(
            txn.as_mut(),
            RecordStagingCommitRequest {
                refresh_id,
                staging_snapshot_id: 300,
                rows: 3,
                base_table_uuids: BTreeMap::from([(
                    "iceberg.sales.orders".to_string(),
                    "uuid-orders".to_string(),
                )]),
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("record publish commit")?;
        repository.record_publish_commit(
            txn.as_mut(),
            RecordPublishCommitRequest {
                refresh_id,
                published_snapshot_id: 300,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("finalize branch staged refresh")?;
        repository.finalize_refresh(txn.as_mut(), MvRefreshFinalizeRequest {
            refresh_id,
            rows: 3,
            base_snapshots: BTreeMap::from([("iceberg.sales.orders".to_string(), 50)]),
            base_table_uuids: BTreeMap::from([(
                "iceberg.sales.orders".to_string(),
                "uuid-orders".to_string(),
            )]),
            target_snapshot_id: Some(300),
        })?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let refresh = repository
        .load_refresh(read.as_ref(), refresh_id)?
        .expect("refresh should persist");
    assert_eq!(refresh.state, MvRefreshState::Finalized);
    assert_eq!(refresh.staging_snapshot_id, Some(300));
    assert_eq!(refresh.published_snapshot_id, Some(300));
    assert_eq!(refresh.rows, Some(3));
    assert_eq!(
        refresh.marker,
        Some(RefreshCommitMarker {
            refresh_id,
            mv_id,
            token: marker_token,
        })
    );

    let definition = repository
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should persist");
    assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(300));
    assert_eq!(definition.last_refresh_rows, Some(3));
    assert_eq!(definition.active_refresh_id, None);
    assert!(!definition.refresh_in_progress);
    Ok(())
}
```

- [ ] **Step 2: Run lifecycle test and verify it fails**

Run:

```bash
cargo test --test meta_repository mv_repository_branch_staged_refresh_lifecycle -- --nocapture
```

Expected: compile failure naming missing `RefreshCommitMarker`, `BeginIcebergMvRefreshRequest`, `RecordStagingCommitRequest`, `RecordPublishCommitRequest`, and `begin_iceberg_refresh_intent`.

- [ ] **Step 3: Extend repository data types**

In `src/meta/repository/mv.rs`, replace `StoredMvRefresh`, `MvRefreshState`, and request definitions around the current refresh structs with this block. Preserve the existing `CreateMvDefinitionRequest` and `UpdateManagedMvRefreshSummaryRequest` definitions.

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshCommitMarker {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvRefresh {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub state: MvRefreshState,
    #[serde(default)]
    pub target_catalog: Option<String>,
    #[serde(default)]
    pub target_namespace: Option<String>,
    #[serde(default)]
    pub target_table: Option<String>,
    #[serde(default)]
    pub staging_branch: Option<String>,
    #[serde(default)]
    pub expected_main_snapshot_id: Option<i64>,
    #[serde(default)]
    pub staging_snapshot_id: Option<i64>,
    #[serde(default)]
    pub published_snapshot_id: Option<i64>,
    #[serde(default)]
    pub target_snapshots: BTreeMap<String, i64>,
    #[serde(default)]
    pub base_table_uuids: BTreeMap<String, String>,
    #[serde(default)]
    pub rows: Option<i64>,
    #[serde(default)]
    pub marker: Option<RefreshCommitMarker>,
    #[serde(default)]
    pub external_outcome: Option<RefreshExternalOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvRefreshState {
    IntentCreated,
    StagingCommitted,
    #[serde(alias = "EXTERNAL_COMMITTED")]
    PublishCommitted,
    Finalized,
    AbortRequested,
    Aborted,
    CommitUnknown,
}

impl MvRefreshState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IntentCreated => "INTENT_CREATED",
            Self::StagingCommitted => "STAGING_COMMITTED",
            Self::PublishCommitted => "PUBLISH_COMMITTED",
            Self::Finalized => "FINALIZED",
            Self::AbortRequested => "ABORT_REQUESTED",
            Self::Aborted => "ABORTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginIcebergMvRefreshRequest {
    pub mv_id: i64,
    pub target_catalog: String,
    pub target_namespace: String,
    pub target_table: String,
    pub staging_branch: String,
    pub expected_main_snapshot_id: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub marker_token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordStagingCommitRequest {
    pub refresh_id: i64,
    pub staging_snapshot_id: i64,
    pub rows: i64,
    pub base_table_uuids: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPublishCommitRequest {
    pub refresh_id: i64,
    pub published_snapshot_id: i64,
}

```

- [ ] **Step 4: Add repository transition methods**

In `impl MvMetaRepository`, add these methods after `begin_refresh_intent`. Keep `begin_refresh_intent` for managed-compatible call sites, but implement Iceberg refresh through the new request method.

```rust
pub fn begin_iceberg_refresh_intent(
    &self,
    txn: &mut dyn MetaWriteTxn,
    req: BeginIcebergMvRefreshRequest,
) -> RepositoryResult<StoredMvRefresh> {
    let mut definition = self.load_versioned_by_id(txn, req.mv_id)?.ok_or_else(|| {
        RepositoryError::not_found(format!("mv definition {} not found", req.mv_id))
    })?;
    if definition.value.refresh_in_progress || definition.value.active_refresh_id.is_some() {
        return Err(RepositoryError::conflict(format!(
            "mv definition {} already has refresh in progress",
            req.mv_id
        )));
    }

    let refresh_id = txn.allocate_id(id_scopes::refresh_id())?;
    let marker = RefreshCommitMarker {
        refresh_id,
        mv_id: req.mv_id,
        token: req.marker_token,
    };
    definition.value.refresh_in_progress = true;
    definition.value.active_refresh_id = Some(refresh_id);
    definition.value.refresh_target_snapshots = req.base_snapshots.clone();
    put_definition(
        txn,
        &definition,
        ExpectedRevision::Exact(definition.record_revision.clone()),
    )?;

    let refresh = StoredMvRefresh {
        refresh_id,
        mv_id: req.mv_id,
        state: MvRefreshState::IntentCreated,
        target_catalog: Some(req.target_catalog),
        target_namespace: Some(req.target_namespace),
        target_table: Some(req.target_table),
        staging_branch: Some(req.staging_branch),
        expected_main_snapshot_id: req.expected_main_snapshot_id,
        staging_snapshot_id: None,
        published_snapshot_id: None,
        target_snapshots: req.base_snapshots,
        base_table_uuids: BTreeMap::new(),
        rows: None,
        marker: Some(marker),
        external_outcome: None,
    };
    put_refresh(txn, &refresh, ExpectedRevision::NotExists)?;
    Ok(refresh)
}

pub fn record_staging_commit(
    &self,
    txn: &mut dyn MetaWriteTxn,
    req: RecordStagingCommitRequest,
) -> RepositoryResult<()> {
    let mut refresh = load_versioned_refresh(txn, req.refresh_id)?.ok_or_else(|| {
        RepositoryError::not_found(format!("mv refresh {} not found", req.refresh_id))
    })?;
    if refresh.value.state != MvRefreshState::IntentCreated {
        return Err(RepositoryError::conflict(format!(
            "mv refresh {} is {}, expected {}",
            req.refresh_id,
            refresh.value.state.as_str(),
            MvRefreshState::IntentCreated.as_str()
        )));
    }
    refresh.value.state = MvRefreshState::StagingCommitted;
    refresh.value.staging_snapshot_id = Some(req.staging_snapshot_id);
    refresh.value.rows = Some(req.rows);
    refresh.value.base_table_uuids = req.base_table_uuids;
    put_refresh(txn, &refresh.value, ExpectedRevision::Exact(refresh.record_revision))
}

pub fn record_publish_commit(
    &self,
    txn: &mut dyn MetaWriteTxn,
    req: RecordPublishCommitRequest,
) -> RepositoryResult<()> {
    let mut refresh = load_versioned_refresh(txn, req.refresh_id)?.ok_or_else(|| {
        RepositoryError::not_found(format!("mv refresh {} not found", req.refresh_id))
    })?;
    if refresh.value.state == MvRefreshState::PublishCommitted {
        return Ok(());
    }
    if refresh.value.state != MvRefreshState::StagingCommitted {
        return Err(RepositoryError::conflict(format!(
            "mv refresh {} is {}, expected {}",
            req.refresh_id,
            refresh.value.state.as_str(),
            MvRefreshState::StagingCommitted.as_str()
        )));
    }
    refresh.value.state = MvRefreshState::PublishCommitted;
    refresh.value.published_snapshot_id = Some(req.published_snapshot_id);
    refresh.value.external_outcome = Some(RefreshExternalOutcome {
        target_snapshot_id: Some(req.published_snapshot_id),
        commit_id: format!("iceberg-snapshot-{}", req.published_snapshot_id),
    });
    put_refresh(txn, &refresh.value, ExpectedRevision::Exact(refresh.record_revision))
}

pub fn mark_refresh_commit_unknown(
    &self,
    txn: &mut dyn MetaWriteTxn,
    refresh_id: i64,
) -> RepositoryResult<()> {
    let mut refresh = load_versioned_refresh(txn, refresh_id)?.ok_or_else(|| {
        RepositoryError::not_found(format!("mv refresh {refresh_id} not found"))
    })?;
    if matches!(
        refresh.value.state,
        MvRefreshState::Finalized | MvRefreshState::Aborted
    ) {
        return Ok(());
    }
    refresh.value.state = MvRefreshState::CommitUnknown;
    put_refresh(txn, &refresh.value, ExpectedRevision::Exact(refresh.record_revision))
}
```

- [ ] **Step 5: Update finalize to require publish for branch-staged refresh**

In `finalize_refresh`, replace the state check with:

```rust
if refresh.value.state == MvRefreshState::Finalized {
    return Ok(());
}
if refresh.value.state != MvRefreshState::PublishCommitted {
    return Err(RepositoryError::conflict(format!(
        "mv refresh {} is {}, expected {}",
        req.refresh_id,
        refresh.value.state.as_str(),
        MvRefreshState::PublishCommitted.as_str()
    )));
}
```

Keep the existing definition update logic, and set `refresh.value.state = MvRefreshState::Finalized` before `put_refresh`.

- [ ] **Step 6: Update clear-refresh behavior**

In `clear_refresh_progress`, keep existing active refresh clearing, but only mark non-terminal refreshes as `Aborted`. Treat `CommitUnknown` as terminal for clearing attempts by returning a conflict:

```rust
if let Some(refresh_id) = definition.value.active_refresh_id
    && let Some(refresh) = load_versioned_refresh(txn, refresh_id)?
    && refresh.value.state == MvRefreshState::CommitUnknown
{
    return Err(RepositoryError::conflict(format!(
        "mv definition {} active refresh {} is commit-unknown",
        definition.value.mv_id, refresh_id
    )));
}
```

- [ ] **Step 7: Update old tests to new lifecycle**

Modify `tests/meta_framework_flow.rs` so the existing recovery test uses `begin_iceberg_refresh_intent`, `record_staging_commit`, `record_publish_commit`, and `finalize_refresh` instead of `record_external_commit_outcome`.

- [ ] **Step 8: Run repository tests**

Run:

```bash
cargo test --test meta_repository mv_repository_branch_staged_refresh_lifecycle -- --nocapture
cargo test --test meta_repository mv_repository_rejects_second_refresh_intent -- --nocapture
cargo test --test meta_framework_flow -- --nocapture
```

Expected: all listed tests pass.

- [ ] **Step 9: Commit repository model**

```bash
git add src/meta/repository/mv.rs tests/meta_repository.rs tests/meta_framework_flow.rs
git commit -m "feat(meta): add branch-staged MV refresh transaction state"
```

## Task 2: Iceberg Ref Helpers and Snapshot Markers

**Files:**
- Create: `src/connector/iceberg/commit/mv_refresh_ref.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`
- Modify: `src/connector/iceberg/commit/action.rs`
- Modify: `src/connector/iceberg/commit/run.rs`
- Modify: `src/connector/iceberg/commit/fast_append.rs`
- Modify: `src/connector/iceberg/commit/overwrite.rs`
- Modify call sites of `RunInput` under `src/engine/**` and `src/connector/iceberg/compact.rs`

- [ ] **Step 1: Add failing Iceberg ref helper test**

Create `src/connector/iceberg/commit/mv_refresh_ref.rs` with the module skeleton and tests:

```rust
use std::collections::BTreeMap;

use iceberg::spec::Snapshot;
use iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};

pub const MV_REFRESH_ID_PROP: &str = "novarocks.mv.refresh_id";
pub const MV_ID_PROP: &str = "novarocks.mv.id";
pub const MV_REFRESH_TOKEN_PROP: &str = "novarocks.mv.refresh_token";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshSnapshotMarker {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
}

impl MvRefreshSnapshotMarker {
    pub fn to_summary_properties(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (MV_REFRESH_ID_PROP.to_string(), self.refresh_id.to_string()),
            (MV_ID_PROP.to_string(), self.mv_id.to_string()),
            (MV_REFRESH_TOKEN_PROP.to_string(), self.token.clone()),
        ])
    }
}

pub fn snapshot_matches_refresh_marker(
    snapshot: &Snapshot,
    marker: &MvRefreshSnapshotMarker,
) -> bool {
    let props = &snapshot.summary().additional_properties;
    props
        .get(MV_REFRESH_ID_PROP)
        .and_then(|value| value.parse::<i64>().ok())
        == Some(marker.refresh_id)
        && props
            .get(MV_ID_PROP)
            .and_then(|value| value.parse::<i64>().ok())
            == Some(marker.mv_id)
        && props.get(MV_REFRESH_TOKEN_PROP).map(String::as_str) == Some(marker.token.as_str())
}
```

Add this unit test at the bottom of the same file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{Operation, Summary};

    #[test]
    fn marker_round_trips_through_snapshot_summary() {
        let marker = MvRefreshSnapshotMarker {
            refresh_id: 77,
            mv_id: 12,
            token: "token-77".to_string(),
        };
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: marker.to_summary_properties().into_iter().collect(),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(300)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:/tmp/manifest-list.avro".to_string())
            .with_summary(summary)
            .with_schema_id(0)
            .build();
        assert!(snapshot_matches_refresh_marker(&snapshot, &marker));
    }
}
```

- [ ] **Step 2: Export module and run marker test**

Add this to `src/connector/iceberg/commit/mod.rs`:

```rust
mod mv_refresh_ref;
pub use mv_refresh_ref::{
    MV_ID_PROP, MV_REFRESH_ID_PROP, MV_REFRESH_TOKEN_PROP, MvRefreshSnapshotMarker,
    snapshot_matches_refresh_marker,
};
```

Run:

```bash
cargo test connector::iceberg::commit::mv_refresh_ref::tests::marker_round_trips_through_snapshot_summary --lib -- --nocapture
```

Expected: pass.

- [ ] **Step 3: Carry snapshot properties through commit context**

In `src/connector/iceberg/commit/action.rs`, add a field to `CommitCtx`:

```rust
pub snapshot_properties: &'a BTreeMap<String, String>,
```

Also add this import:

```rust
use std::collections::BTreeMap;
```

In `src/connector/iceberg/commit/run.rs`, add this field to `RunInput`:

```rust
pub snapshot_properties: BTreeMap<String, String>,
```

Destructure it in `run_iceberg_commit` and pass it into `CommitCtx`:

```rust
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
```

Add `use std::collections::BTreeMap;` to `run.rs`.

- [ ] **Step 4: Update existing `RunInput` call sites with empty properties**

For every non-MV call site constructing `RunInput`, add:

```rust
snapshot_properties: BTreeMap::new(),
```

Add `use std::collections::BTreeMap;` in files that do not already import it. Expected files include:

- `src/engine/iceberg_writer.rs`
- `src/engine/delete_flow.rs`
- `src/engine/equality_delete_flow.rs`
- `src/engine/iceberg_truncate.rs`
- `src/engine/mutation_flow.rs`
- `src/connector/iceberg/compact.rs`

- [ ] **Step 5: Merge marker properties into custom snapshot summaries**

In `src/connector/iceberg/commit/fast_append.rs`, replace:

```rust
let summary = Summary {
    operation: Operation::Append,
    additional_properties: append_summary(&self.written, total_records),
};
```

with:

```rust
let mut additional_properties = append_summary(&self.written, total_records);
additional_properties.extend(self.snapshot_properties.clone());
let summary = Summary {
    operation: Operation::Append,
    additional_properties,
};
```

Add `snapshot_properties: BTreeMap<String, String>` to `FastAppendV3TxnAction` and set it from `ctx.snapshot_properties.clone()`. Add `use std::collections::BTreeMap;`.

In `src/connector/iceberg/commit/overwrite.rs`, replace both `Summary` constructions so they merge `self.snapshot_properties.clone()` into `overwrite_summary(&self.written, &existing)`. Add `snapshot_properties: BTreeMap<String, String>` to `OverwriteTxnAction`.

- [ ] **Step 6: Reject branch writes on built-in v2 append path**

In `FastAppendCommit::commit`, before the built-in `Transaction::fast_append` path, add:

```rust
if ctx.target_ref != "main" {
    return Err(format!(
        "FastAppendCommit branch target_ref={} requires the custom v3 row-lineage append path",
        ctx.target_ref
    ));
}
```

This keeps A7 from silently ignoring `target_ref` on the built-in append path.

- [ ] **Step 7: Add publish helper skeleton**

Extend `src/connector/iceberg/commit/mv_refresh_ref.rs` with these types and function signatures:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshPublishPlan {
    pub namespace: String,
    pub table: String,
    pub staging_branch: String,
    pub expected_main_snapshot_id: Option<i64>,
    pub staging_snapshot_id: i64,
    pub marker: MvRefreshSnapshotMarker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshPublishOutcome {
    pub published_snapshot_id: i64,
}

pub async fn publish_staging_branch_to_main(
    catalog: &dyn Catalog,
    plan: &MvRefreshPublishPlan,
) -> Result<MvRefreshPublishOutcome, String> {
    let ident = TableIdent::from_strs([plan.namespace.as_str(), plan.table.as_str()])
        .map_err(|e| format!("iceberg mv publish: invalid table identifier: {e}"))?;
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| format!("iceberg mv publish: load table failed: {e}"))?;
    let metadata = table.metadata();
    let main_snapshot = metadata.current_snapshot().map(|s| s.snapshot_id());
    if main_snapshot != plan.expected_main_snapshot_id {
        return Err(format!(
            "iceberg mv publish: main snapshot mismatch for {}.{}: expected {:?}, current {:?}",
            plan.namespace, plan.table, plan.expected_main_snapshot_id, main_snapshot
        ));
    }
    let staging_ref = metadata.refs().get(&plan.staging_branch).ok_or_else(|| {
        format!("iceberg mv publish: staging branch {} does not exist", plan.staging_branch)
    })?;
    if staging_ref.snapshot_id != plan.staging_snapshot_id {
        return Err(format!(
            "iceberg mv publish: staging branch {} points to {}, expected {}",
            plan.staging_branch, staging_ref.snapshot_id, plan.staging_snapshot_id
        ));
    }
    let staging_snapshot = metadata.snapshot_by_id(plan.staging_snapshot_id).ok_or_else(|| {
        format!("iceberg mv publish: staging snapshot {} not found", plan.staging_snapshot_id)
    })?;
    if !snapshot_matches_refresh_marker(staging_snapshot, &plan.marker) {
        return Err(format!(
            "iceberg mv publish: staging snapshot {} marker mismatch",
            plan.staging_snapshot_id
        ));
    }

    let commit = TableCommit::builder()
        .ident(ident)
        .updates(vec![TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            reference: iceberg::spec::SnapshotReference {
                snapshot_id: plan.staging_snapshot_id,
                retention: iceberg::spec::SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        }])
        .requirements(vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_string(),
            snapshot_id: plan.expected_main_snapshot_id,
        }])
        .build();
    catalog
        .update_table(commit)
        .await
        .map_err(|e| format!("iceberg mv publish: commit failed: {e}"))?;
    Ok(MvRefreshPublishOutcome {
        published_snapshot_id: plan.staging_snapshot_id,
    })
}
```

- [ ] **Step 8: Export publish helper and run compile check**

Add exports in `src/connector/iceberg/commit/mod.rs`:

```rust
pub use mv_refresh_ref::{
    MvRefreshPublishOutcome, MvRefreshPublishPlan, publish_staging_branch_to_main,
};
```

Run:

```bash
cargo test connector::iceberg::commit::mv_refresh_ref --lib -- --nocapture
cargo check --all-targets
```

Expected: marker unit test passes and `cargo check --all-targets` completes.

- [ ] **Step 9: Commit Iceberg helper work**

```bash
git add src/connector/iceberg/commit src/engine src/connector/iceberg/compact.rs
git commit -m "feat(iceberg): add MV refresh staging ref helpers"
```

## Task 3: Create Branch-Capable Iceberg MV Targets

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add failing target format test**

In the `#[cfg(test)]` module of `src/engine/mv/iceberg_refresh.rs`, add:

```rust
#[test]
fn create_iceberg_mv_creates_branch_capable_v3_target() {
    let env = open_test_state_with_iceberg_catalog("ice", "analytics");
    create_base_table(&env.state, "ice", "sales", "orders");
    let stmt = parse_create_mv(
        "CREATE MATERIALIZED VIEW mv_orders
         DISTRIBUTED BY HASH(id) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT id, name FROM ice.sales.orders",
    );

    create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
        .expect("create iceberg mv");

    let entry = {
        let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
        catalogs.get("ice").expect("catalog")
    };
    let loaded = crate::connector::iceberg::catalog::load_table(
        &entry,
        "analytics",
        "mv_orders",
    )
    .expect("load target table");
    assert_eq!(
        loaded.table.metadata().format_version(),
        iceberg::spec::FormatVersion::V3
    );
    assert_eq!(
        loaded.table.metadata().properties().get("write.row-lineage").map(String::as_str),
        Some("true")
    );
}
```

- [ ] **Step 2: Run target format test and verify it fails**

Run:

```bash
cargo test engine::mv::iceberg_refresh::tests::create_iceberg_mv_creates_branch_capable_v3_target --lib -- --nocapture
```

Expected: fail because `create_iceberg_mv` currently creates format-version `2`.

- [ ] **Step 3: Change target table creation properties**

In `create_iceberg_mv`, replace the table properties passed to `registry::create_table`:

```rust
&[("format-version".to_string(), "2".to_string())],
```

with:

```rust
&[
    ("format-version".to_string(), "3".to_string()),
    ("write.row-lineage".to_string(), "true".to_string()),
],
```

- [ ] **Step 4: Run target creation tests**

Run:

```bash
cargo test engine::mv::iceberg_refresh::tests::create_iceberg_mv_creates_branch_capable_v3_target --lib -- --nocapture
cargo test engine::mv::iceberg_refresh::tests::create_iceberg_mv_uses_current_catalog_target_without_managed_table_row --lib -- --nocapture
```

Expected: both tests pass.

- [ ] **Step 5: Commit target creation change**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(mv): create Iceberg MV targets as branch-capable v3 tables"
```

## Task 4: Branch-Staged Refresh Orchestration

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add failing staging-commit test**

Add a unit test near `write_chunks_round_trip_through_iceberg_table`:

```rust
#[test]
fn iceberg_mv_commit_to_staging_branch_does_not_move_main() {
    use crate::connector::iceberg::catalog::registry::{
        build_catalog_entry, build_iceberg_catalog,
    };
    use crate::connector::iceberg::commit::MvRefreshSnapshotMarker;

    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = format!("file://{}/wh", dir.path().display());
    let entry = build_catalog_entry(
        "ice",
        &[
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
            ("iceberg.catalog.warehouse".to_string(), warehouse),
        ],
    )
    .expect("catalog entry");
    let catalog = build_iceberg_catalog(&entry).expect("catalog");

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let ns = iceberg::NamespaceIdent::from_strs(["test_ns"]).unwrap();
        catalog.create_namespace(&ns, std::collections::HashMap::new()).await.unwrap();
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![StdArc::new(iceberg::spec::NestedField::required(
                1,
                "k",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let table = catalog
            .create_table(
                &ns,
                iceberg::TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(iceberg::spec::FormatVersion::V3)
                    .properties([("write.row-lineage".to_string(), "true".to_string())])
                    .build(),
            )
            .await
            .unwrap();

        let ident = TableIdent::from_strs(["test_ns", "t"]).unwrap();
        let initial = single_int_chunk(&[0]);
        let initial_written = write_chunks_as_iceberg_data_files(&table, &initial).await.unwrap();
        let initial_snapshot = commit_iceberg_mv_target_files(
            &table,
            &catalog,
            &entry,
            &ident,
            CommitOpKind::FastAppend,
            initial_written,
        )
        .await
        .unwrap()
        .new_snapshot_id;
        let table = catalog.load_table(&ident).await.unwrap();
        let current = table.metadata().current_snapshot().map(|s| s.snapshot_id());
        assert_eq!(current, Some(initial_snapshot));

        let marker = MvRefreshSnapshotMarker {
            refresh_id: 7,
            mv_id: 3,
            token: "token-7".to_string(),
        };
        let staging_branch = "__nova_mv_refresh_3_7";
        crate::connector::iceberg::commit::execute_ref_action(
            catalog.as_ref(),
            &crate::connector::iceberg::commit::RefActionPlan {
                catalog: "ice".to_string(),
                namespace: "test_ns".to_string(),
                table: "t".to_string(),
                action: crate::connector::iceberg::commit::RefAction::CreateBranch {
                    name: staging_branch.to_string(),
                    snapshot_id: current.expect("main snapshot"),
                    replace: false,
                    if_not_exists: false,
                },
            },
        )
        .await
        .unwrap();
        let table = catalog.load_table(&ident).await.unwrap();

        let chunks = single_int_chunk(&[1, 2, 3]);
        let written = write_chunks_as_iceberg_data_files(&table, &chunks).await.unwrap();
        let staging_snapshot = commit_iceberg_mv_target_files_with_ref(
            &table,
            &catalog,
            &entry,
            &ident,
            CommitOpKind::FastAppend,
            written,
            staging_branch,
            marker.to_summary_properties(),
        )
        .await
        .unwrap()
        .new_snapshot_id;

        let reloaded = catalog.load_table(&ident).await.unwrap();
        assert_eq!(reloaded.metadata().current_snapshot().map(|s| s.snapshot_id()), current);
        assert_eq!(
            reloaded.metadata().refs().get(staging_branch).map(|r| r.snapshot_id),
            Some(staging_snapshot)
        );
    });
}
```

If `single_int_chunk` does not exist, add this helper in the same test module:

```rust
fn single_int_chunk(values: &[i32]) -> Vec<crate::exec::chunk::Chunk> {
    let arrow_schema = StdArc::new(ArrowSchema::new(vec![Field::new("k", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![StdArc::new(Int32Array::from(values.to_vec()))],
    )
    .expect("record batch");
    let chunk_schema_ref = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        &arrow_schema,
        &[crate::common::ids::SlotId(0)],
    )
    .expect("chunk schema");
    vec![crate::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema_ref)]
}
```

- [ ] **Step 2: Run staging-commit test and verify it fails**

Run:

```bash
cargo test engine::mv::iceberg_refresh::tests::iceberg_mv_commit_to_staging_branch_does_not_move_main --lib -- --nocapture
```

Expected: compile failure because `commit_iceberg_mv_target_files_with_ref` does not exist.

- [ ] **Step 3: Add branch-aware commit helper**

In `src/engine/mv/iceberg_refresh.rs`, split the existing `commit_iceberg_mv_target_files` into a wrapper and branch-aware helper:

```rust
async fn commit_iceberg_mv_target_files(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn Catalog>,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    ident: &TableIdent,
    op_kind: CommitOpKind,
    data_files: Vec<DataFile>,
) -> Result<CommitOutcome, String> {
    commit_iceberg_mv_target_files_with_ref(
        table,
        catalog,
        entry,
        ident,
        op_kind,
        data_files,
        "main",
        std::collections::BTreeMap::new(),
    )
    .await
}

async fn commit_iceberg_mv_target_files_with_ref(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn Catalog>,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    ident: &TableIdent,
    op_kind: CommitOpKind,
    data_files: Vec<DataFile>,
    target_ref: &str,
    snapshot_properties: std::collections::BTreeMap<String, String>,
) -> Result<CommitOutcome, String> {
    let metadata = table.metadata();
    let staging_dir = format!("{}/data/_staging/{}", metadata.location(), uuid::Uuid::new_v4());
    let collector = Arc::new(IcebergCommitCollector::new(
        op_kind,
        ident.clone(),
        metadata.refs().get(target_ref).map(|r| r.snapshot_id).or_else(|| {
            if target_ref == "main" {
                metadata.current_snapshot().map(|s| s.snapshot_id())
            } else {
                None
            }
        }),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    let default_spec_id = metadata.default_partition_spec_id();
    for df in data_files {
        collector.inject_written_file(crate::engine::iceberg_writer::data_file_to_written_file(
            &df,
            default_spec_id,
        )?);
    }
    let abort_cleanup = crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(entry)?;
    run_iceberg_commit(RunInput {
        collector,
        catalog: catalog.clone(),
        table: table.clone(),
        fs: abort_cleanup.fs,
        file_io: table.file_io().clone(),
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties,
    })
    .await
}
```

- [ ] **Step 4: Run staging-commit test**

Run:

```bash
cargo test engine::mv::iceberg_refresh::tests::iceberg_mv_commit_to_staging_branch_does_not_move_main --lib -- --nocapture
```

Expected: pass.

- [ ] **Step 5: Update refresh path to stage, publish, and finalize**

In `refresh_iceberg_mv`, before starting a write-bearing refresh, compute:

```rust
let expected_main_snapshot_id = target_loaded
    .table
    .metadata()
    .current_snapshot()
    .map(|s| s.snapshot_id());
let staging_branch = format!(
    "__nova_mv_refresh_{}_{}",
    mv_definition.mv_id,
    uuid::Uuid::new_v4().simple()
);
```

Replace calls to `begin_iceberg_mv_refresh_intent` with a new helper that accepts target identity, expected main snapshot, base snapshots, and staging branch. The helper must call `begin_iceberg_refresh_intent`.

After writing chunks and before finalizing metadata:

```rust
record_iceberg_mv_staging_commit(state, refresh_id, new_snapshot_id, total_rows, table_uuids.clone())?;
let published_snapshot_id = publish_iceberg_mv_refresh(
    state,
    &target,
    &target_entry,
    &staging_branch,
    expected_main_snapshot_id,
    new_snapshot_id,
    refresh_id,
    mv_definition.mv_id,
)?;
record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
finalize_iceberg_mv_refresh(
    state,
    refresh_id,
    total_rows,
    snapshots.clone(),
    table_uuids.clone(),
    published_snapshot_id,
)?;
drop_iceberg_mv_staging_branch(state, &target, &target_entry, &staging_branch)?;
```

Use the same sequence in `first_refresh_iceberg_mv`, `rebuild_iceberg_mv`, and non-empty `incremental_refresh_iceberg_mv`. Keep metadata-only no-op and empty-delta refreshes on a metadata-only finalize path because they do not create a new Iceberg snapshot.

- [ ] **Step 6: Run focused MV tests**

Run:

```bash
cargo test engine::mv::iceberg_refresh::tests::write_chunks_round_trip_through_iceberg_table --lib -- --nocapture
cargo test engine::mv::iceberg_refresh::tests::iceberg_mv_commit_to_staging_branch_does_not_move_main --lib -- --nocapture
cargo test engine::mv::iceberg_refresh::tests::refresh_iceberg_mv_fails_when_target_snapshot_was_modified_externally --lib -- --nocapture
```

Expected: all listed tests pass. The external modification test may need assertion text updates if it now fails through publish guard instead of pre-refresh validation.

- [ ] **Step 7: Commit staged orchestration**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(mv): publish Iceberg MV refresh through staging branch"
```

## Task 5: Refresh Recovery

**Files:**
- Modify: `src/meta/repository/mv.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mod.rs`
- Modify: `tests/meta_repository.rs`

- [ ] **Step 1: Add repository query for unfinished refreshes**

Add this test to `tests/meta_repository.rs`:

```rust
#[test]
fn mv_repository_lists_unfinished_refreshes() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_mv_definition(&provider, &repository)?;

    let mut txn = provider.begin_write("begin refresh")?;
    let refresh = repository.begin_iceberg_refresh_intent(
        txn.as_mut(),
        BeginIcebergMvRefreshRequest {
            mv_id,
            target_catalog: "ice".to_string(),
            target_namespace: "analytics".to_string(),
            target_table: "orders_mv".to_string(),
            staging_branch: "__nova_mv_refresh_1_1".to_string(),
            expected_main_snapshot_id: Some(10),
            base_snapshots: BTreeMap::new(),
            marker_token: "token".to_string(),
        },
    )?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let unfinished = repository.list_unfinished_refreshes(read.as_ref())?;
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].refresh_id, refresh.refresh_id);
    Ok(())
}
```

If `create_test_mv_definition` is not present, add a helper near other MV tests:

```rust
fn create_test_mv_definition(
    provider: &SqliteMetaStoreProvider,
    repository: &MvMetaRepository,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut txn = provider.begin_write("create mv definition")?;
    let definition = repository.create_definition(
        txn.as_mut(),
        CreateMvDefinitionRequest {
            select_sql: "SELECT id FROM iceberg.sales.orders".to_string(),
            base_table_refs: vec!["iceberg.sales.orders".to_string()],
            primary_key_columns: vec!["id".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("analytics".to_string()),
            target_table: Some("orders_mv".to_string()),
            created_at_ms: 11,
        },
    )?;
    txn.commit()?;
    Ok(definition.mv_id)
}
```

- [ ] **Step 2: Implement unfinished-refresh query**

In `src/meta/repository/mv.rs`, add:

```rust
pub fn list_unfinished_refreshes(
    &self,
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<StoredMvRefresh>> {
    Ok(txn
        .scan(&key_prefix_refresh()?, None)?
        .into_iter()
        .map(decode_refresh_record)
        .collect::<RepositoryResult<Vec<_>>>()?
        .into_iter()
        .map(|versioned| versioned.value)
        .filter(|refresh| {
            !matches!(
                refresh.state,
                MvRefreshState::Finalized | MvRefreshState::Aborted
            )
        })
        .collect())
}
```

Add these helpers near `load_versioned_refresh` and `key_refresh`:

```rust
fn decode_refresh_record(record: MetaRecord) -> RepositoryResult<VersionedMvRefresh> {
    let value = decode_record_payload(&record, MV_REFRESH_KIND, MV_REFRESH_SCHEMA_VERSION)?;
    Ok(VersionedMvRefresh {
        record_revision: record.revision,
        value,
    })
}

fn key_prefix_refresh() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(NS_MV, ["refresh"])?)
}
```

- [ ] **Step 3: Run unfinished-refresh test**

Run:

```bash
cargo test --test meta_repository mv_repository_lists_unfinished_refreshes -- --nocapture
```

Expected: pass.

- [ ] **Step 4: Add recovery function skeleton**

In `src/engine/mv/iceberg_refresh.rs`, add a public crate-visible recovery entrypoint:

```rust
pub(crate) fn recover_iceberg_mv_refreshes(state: &Arc<StandaloneState>) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open iceberg MV refresh recovery read transaction failed: {e}"))?;
    let unfinished = state
        .mv_repo
        .list_unfinished_refreshes(read.as_ref())
        .map_err(|e| format!("load unfinished iceberg MV refreshes failed: {e}"))?;
    drop(read);
    for refresh in unfinished {
        recover_one_iceberg_mv_refresh(state, refresh)?;
    }
    Ok(())
}
```

Add `recover_one_iceberg_mv_refresh` with an initial safe implementation that returns an error for missing target identity:

```rust
fn recover_one_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    refresh: crate::meta::repository::mv::StoredMvRefresh,
) -> Result<(), String> {
    let target = IcebergMvTarget {
        catalog: refresh
            .target_catalog
            .clone()
            .ok_or_else(|| format!("mv refresh {} missing target catalog", refresh.refresh_id))?,
        namespace: refresh
            .target_namespace
            .clone()
            .ok_or_else(|| format!("mv refresh {} missing target namespace", refresh.refresh_id))?,
        table: refresh
            .target_table
            .clone()
            .ok_or_else(|| format!("mv refresh {} missing target table", refresh.refresh_id))?,
    };
    let (entry, catalog, loaded) = load_iceberg_mv_target(state, &target)?;
    reconcile_iceberg_mv_refresh(state, refresh, &target, &entry, &catalog, &loaded.table)
}
```

- [ ] **Step 5: Implement conservative reconcile rules**

Add `reconcile_iceberg_mv_refresh` that:

- Reads current `main` snapshot from `table.metadata().current_snapshot()`.
- Reads staging branch snapshot from `table.metadata().refs().get(staging_branch)`.
- Checks marker through `snapshot_matches_refresh_marker`.
- Calls repository methods to abort, record publish, finalize, or mark commit-unknown.

Use this core branch for `StagingCommitted`:

```rust
match refresh.state {
    MvRefreshState::StagingCommitted => {
        let expected = refresh.expected_main_snapshot_id;
        let main = table.metadata().current_snapshot().map(|s| s.snapshot_id());
        let staging_branch = refresh.staging_branch.as_deref().ok_or_else(|| {
            format!("mv refresh {} missing staging branch", refresh.refresh_id)
        })?;
        let staging = table.metadata().refs().get(staging_branch).map(|r| r.snapshot_id);
        if main == expected && staging == refresh.staging_snapshot_id {
            drop_iceberg_mv_staging_branch(state, target, entry, staging_branch)?;
            mark_iceberg_mv_refresh_aborted(state, refresh.refresh_id)?;
            return Ok(());
        }
        if main == refresh.staging_snapshot_id {
            record_iceberg_mv_publish_commit(
                state,
                refresh.refresh_id,
                refresh.staging_snapshot_id.ok_or_else(|| {
                    format!("mv refresh {} missing staging snapshot", refresh.refresh_id)
                })?,
            )?;
            finalize_recovered_iceberg_mv_refresh(state, &refresh)?;
            drop_iceberg_mv_staging_branch(state, target, entry, staging_branch)?;
            return Ok(());
        }
        mark_iceberg_mv_refresh_commit_unknown(state, refresh.refresh_id)?;
        Ok(())
    }
    _ => mark_iceberg_mv_refresh_commit_unknown(state, refresh.refresh_id),
}
```

Then add the equivalent `IntentCreated` and `PublishCommitted` branches from the design spec.

- [ ] **Step 6: Wire recovery into engine startup**

In `src/engine/mod.rs`, update `restore_metadata_if_needed`:

```rust
fn restore_metadata_if_needed(state: &Arc<StandaloneState>) -> Result<(), String> {
    restore_managed_lake(state)?;
    restore_iceberg_catalogs(state)?;
    crate::engine::mv::iceberg_refresh::recover_iceberg_mv_refreshes(state)?;
    crate::engine::mv::iceberg_refresh::restore_iceberg_mv_targets(state)?;
    Ok(())
}
```

- [ ] **Step 7: Add pre-refresh recovery**

At the start of `refresh_iceberg_mv`, after resolving `target`, call:

```rust
recover_iceberg_mv_refreshes(state)?;
```

Then reload `mv_definition` after recovery so the refresh path sees finalized or aborted state.

- [ ] **Step 8: Run recovery-related tests**

Run:

```bash
cargo test --test meta_repository mv_repository_lists_unfinished_refreshes -- --nocapture
cargo test engine::mv::iceberg_refresh::tests::drop_iceberg_mv_rejects_active_refresh_before_external_drop --lib -- --nocapture
cargo check --all-targets
```

Expected: tests pass and `cargo check --all-targets` completes.

- [ ] **Step 9: Commit recovery skeleton**

```bash
git add src/meta/repository/mv.rs src/engine/mv/iceberg_refresh.rs src/engine/mod.rs tests/meta_repository.rs
git commit -m "feat(mv): recover branch-staged Iceberg MV refreshes"
```

## Task 6: Fault Injection and End-to-End Validation

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add engine-level recovery tests for staged and published states**

In `src/engine/mv/iceberg_refresh.rs` test module, add two tests:

```rust
#[test]
fn recover_iceberg_mv_refresh_aborts_unpublished_staging_snapshot() {
    let env = open_test_state_with_iceberg_catalog("ice", "analytics");
    create_base_table(&env.state, "ice", "sales", "orders");
    create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
    seed_active_staging_refresh(&env.state, "ice", "analytics", "mv_orders", false);

    recover_iceberg_mv_refreshes(&env.state).expect("recover");

    let provider = env.state.metadata_provider.as_ref().expect("provider");
    let read = provider.begin_read().expect("read");
    let refreshes = env
        .state
        .mv_repo
        .list_unfinished_refreshes(read.as_ref())
        .expect("unfinished");
    assert!(refreshes.is_empty(), "unpublished staging refresh should be terminal");
}

#[test]
fn recover_iceberg_mv_refresh_finalizes_published_snapshot() {
    let env = open_test_state_with_iceberg_catalog("ice", "analytics");
    create_base_table(&env.state, "ice", "sales", "orders");
    create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
    let published_snapshot = seed_active_staging_refresh(&env.state, "ice", "analytics", "mv_orders", true);

    recover_iceberg_mv_refreshes(&env.state).expect("recover");

    let definition = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
        .expect("mv definition");
    assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(published_snapshot));
    assert_eq!(definition.active_refresh_id, None);
    assert!(!definition.refresh_in_progress);
}
```

Implement `seed_active_staging_refresh` in the test module by reusing helper functions already used by `write_chunks_round_trip_through_iceberg_table`. It must:

- load the target table;
- create a staging branch;
- write one chunk to the staging branch with a marker;
- record repository intent and staging commit;
- optionally publish `main` and record publish commit only when `publish_main` is true.

- [ ] **Step 2: Run recovery tests and verify failures**

Run:

```bash
cargo test engine::mv::iceberg_refresh::tests::recover_iceberg_mv_refresh_aborts_unpublished_staging_snapshot --lib -- --nocapture
cargo test engine::mv::iceberg_refresh::tests::recover_iceberg_mv_refresh_finalizes_published_snapshot --lib -- --nocapture
```

Expected: both tests fail until `seed_active_staging_refresh` and recovery branches are complete.

- [ ] **Step 3: Complete recovery branches**

Finish `IntentCreated`, `StagingCommitted`, and `PublishCommitted` branches in `reconcile_iceberg_mv_refresh` so the two tests pass. Do not auto-finalize if marker validation fails; call `mark_iceberg_mv_refresh_commit_unknown`.

- [ ] **Step 4: Add external-main-modification test**

Add this test:

```rust
#[test]
fn recover_iceberg_mv_refresh_marks_unknown_when_main_changed_externally() {
    let env = open_test_state_with_iceberg_catalog("ice", "analytics");
    create_base_table(&env.state, "ice", "sales", "orders");
    create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
    seed_active_staging_refresh(&env.state, "ice", "analytics", "mv_orders", false);
    advance_target_main_without_refresh_marker(&env.state, "ice", "analytics", "mv_orders");

    recover_iceberg_mv_refreshes(&env.state).expect("recover");

    let provider = env.state.metadata_provider.as_ref().expect("provider");
    let read = provider.begin_read().expect("read");
    let unfinished = env.state.mv_repo.list_unfinished_refreshes(read.as_ref()).expect("unfinished");
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].state, MvRefreshState::CommitUnknown);
}
```

Implement `advance_target_main_without_refresh_marker` by doing a normal `FastAppend` to `main` with `BTreeMap::new()` snapshot properties.

- [ ] **Step 5: Run fault injection tests**

Run:

```bash
cargo test engine::mv::iceberg_refresh::tests::recover_iceberg_mv_refresh_aborts_unpublished_staging_snapshot --lib -- --nocapture
cargo test engine::mv::iceberg_refresh::tests::recover_iceberg_mv_refresh_finalizes_published_snapshot --lib -- --nocapture
cargo test engine::mv::iceberg_refresh::tests::recover_iceberg_mv_refresh_marks_unknown_when_main_changed_externally --lib -- --nocapture
```

Expected: all three tests pass.

- [ ] **Step 6: Run broader verification**

Run:

```bash
cargo fmt --check
cargo check --all-targets
cargo test --test meta_repository -- --nocapture
cargo test --test meta_framework_flow -- --nocapture
cargo test engine::mv::iceberg_refresh --lib -- --nocapture
cargo build
```

Expected: all commands pass.

- [ ] **Step 7: Commit fault injection coverage**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "test(mv): cover branch-staged refresh recovery"
```

## Task 7: Final Review and Documentation Sync

**Files:**
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks TODO/TODO List.md`
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks TODO/IVM-A7-refresh-transaction.md`

- [ ] **Step 1: Update Obsidian A7 note**

In `/Users/harbor/Documents/Obsidian/NovaRocks TODO/IVM-A7-refresh-transaction.md`, update the completion target to branch-staged refresh transaction:

```markdown
## 当前设计结论

A7 的目标形态是 branch-staged refresh transaction：

1. NovaRocks metadata 写 refresh intent。
2. Iceberg target 创建 `__nova_mv_refresh_<mv_id>_<refresh_id>` staging branch。
3. refresh 输出写入 staging branch。
4. publish 阶段用 expected-main-snapshot guard 原子推进 `main`。
5. metadata finalize 记录 base snapshots、target snapshot、rows。
6. recovery 根据 intent、staging branch、main snapshot 自动 abort/finalize，未知状态 fail fast。

不保留 direct-main fallback；不支持 Iceberg ref 能力的 target fail fast。
```

- [ ] **Step 2: Update TODO List A7 status**

In `/Users/harbor/Documents/Obsidian/NovaRocks TODO/TODO List.md`, keep A7 in unfinished state until code is merged, but update the one-line description:

```markdown
| ⭐⭐⭐ | **A7** · Refresh transaction 与失败恢复 | [IVM-A7-refresh-transaction.md](IVM-A7-refresh-transaction.md) | 目标协议为 branch-staged publish：staging branch 写入、main 原子发布、metadata recovery 自动收敛 |
```

- [ ] **Step 3: Run final status checks**

Run:

```bash
git status --short
git log --oneline -5
```

Expected: only the Obsidian docs are unstaged, plus the committed code changes from earlier tasks.

- [ ] **Step 4: Commit Obsidian docs only if requested**

The Obsidian vault is outside the NovaRocks repo. Do not commit it in the NovaRocks repository. If the user wants the Obsidian vault committed separately, run:

```bash
git -C /Users/harbor/Documents/Obsidian status --short
git -C /Users/harbor/Documents/Obsidian add "NovaRocks TODO/TODO List.md" "NovaRocks TODO/IVM-A7-refresh-transaction.md"
git -C /Users/harbor/Documents/Obsidian commit -m "docs: update NovaRocks A7 refresh transaction plan"
```

- [ ] **Step 5: Final verification summary**

Collect and report:

```bash
git status --short --branch
cargo fmt --check
cargo check --all-targets
cargo test --test meta_repository -- --nocapture
cargo test engine::mv::iceberg_refresh --lib -- --nocapture
cargo build
```

Expected: Rust commands pass; if Obsidian docs remain unstaged outside the repo, report that separately.
