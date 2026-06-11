# B4 Refresh Policy Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add persistent materialized-view refresh policy metadata and expose it in `SHOW MATERIALIZED VIEWS`, without starting any background scheduler or changing refresh execution.

**Architecture:** Store refresh policy fields on `StoredMvDefinition` in `MvMetaRepository`, with serde defaults so existing version-2 MV definition records remain readable. Keep `mv_flow` and `MvBackend` refresh execution unchanged. Surface the metadata through `MvListRow` and append new `SHOW MATERIALIZED VIEWS` columns after the existing columns so `LastRefreshRows` keeps its current index.

**Tech Stack:** Rust, serde JSON payloads, NovaRocks `MvMetaRepository`, Arrow `RecordBatch`, existing `SHOW MATERIALIZED VIEWS` path, `cargo test`.

---

## Scope

This plan implements only `B4-1 Refresh policy metadata` from the Obsidian NovaRocks B4 refresh scheduler roadmap note.

Included:

- Repository-level refresh policy enum and persisted metadata fields.
- Repository update API for policy metadata and scheduler-visible state fields.
- Default `MANUAL` policy for all newly created MVs.
- `SHOW MATERIALIZED VIEWS` output of policy and paused/scheduler state.
- Focused Rust tests and one MySQL-server tuple update.

Not included:

- Parser support for `REFRESH ASYNC`, `EVERY INTERVAL`, `ON CHANGE`, or `ALTER MATERIALIZED VIEW`.
- Any background coordinator, queue, polling loop, or scheduler thread.
- Query rewrite staleness behavior.

## File Structure

- Modify `src/meta/repository/mv.rs`
  - Owns persistent MV refresh policy metadata and repository update API.
  - Adds `StoredMvRefreshPolicy` and `UpdateMvRefreshMetadataRequest`.

- Modify `tests/meta_repository.rs`
  - Verifies default metadata and non-default round-trip through SQLite-backed repository transactions.
  - Verifies invalid policy metadata is rejected before persistence.

- Modify `src/engine/mv/lifecycle.rs`
  - Extends `MvListRow` with display fields used by every MV backend.

- Modify `src/connector/starrocks/managed/mv_ddl.rs`
  - Sources display values from `StoredMvDefinition`.
  - Appends `RefreshPaused`, `NextRefreshTime`, `LastSchedulerError`, and `MaxStalenessMs` to `SHOW MATERIALIZED VIEWS`.
  - Keeps existing `RefreshMode` column, now backed by stored policy.

- Modify `tests/standalone_mysql_server.rs`
  - Updates the exact `SHOW MATERIALIZED VIEWS` row tuple for the appended columns.

---

### Task 1: Repository Metadata Model

**Files:**

- Modify: `src/meta/repository/mv.rs`
- Modify: `tests/meta_repository.rs`

- [ ] **Step 1: Write the failing repository round-trip test**

In `tests/meta_repository.rs`, update the MV import list:

```rust
use novarocks::meta::repository::mv::{
    BeginIcebergMvRefreshRequest, CreateMvDefinitionRequest, CreateMvDependencyRequest,
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine, MvMetaRepository,
    MvRefreshFinalizeRequest, MvRefreshState, MvTargetLookup, RecordPublishCommitRequest,
    RecordStagingCommitRequest, RefreshCommitMarker, RefreshExternalOutcome,
    StoredMvRefreshPolicy, UpdateManagedMvRefreshSummaryRequest,
    UpdateMvRefreshMetadataRequest,
};
```

Add this test near `sample_mv_definition_request`:

```rust
#[test]
fn mv_repository_refresh_policy_metadata_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition =
            repository.create_definition(txn.as_mut(), sample_mv_definition_request("select id from orders"))?;
        assert_eq!(definition.refresh_policy, StoredMvRefreshPolicy::Manual);
        assert!(!definition.refresh_paused);
        assert_eq!(definition.refresh_interval_ms, None);
        assert_eq!(definition.max_staleness_ms, None);
        assert_eq!(definition.last_scheduler_error, None);
        assert_eq!(definition.next_refresh_after_ms, None);
        txn.commit()?;
        definition.mv_id
    };

    {
        let mut txn = provider.begin_write("update mv refresh metadata")?;
        let updated = repository.update_refresh_metadata(
            txn.as_mut(),
            UpdateMvRefreshMetadataRequest {
                mv_id,
                refresh_policy: StoredMvRefreshPolicy::AsyncInterval,
                refresh_paused: true,
                refresh_interval_ms: Some(300_000),
                max_staleness_ms: Some(900_000),
                last_scheduler_error: Some("catalog timeout".to_string()),
                next_refresh_after_ms: Some(1_700_000_000_000),
            },
        )?;
        assert_eq!(updated.refresh_policy, StoredMvRefreshPolicy::AsyncInterval);
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should exist");
    assert_eq!(loaded.refresh_policy, StoredMvRefreshPolicy::AsyncInterval);
    assert!(loaded.refresh_paused);
    assert_eq!(loaded.refresh_interval_ms, Some(300_000));
    assert_eq!(loaded.max_staleness_ms, Some(900_000));
    assert_eq!(loaded.last_scheduler_error.as_deref(), Some("catalog timeout"));
    assert_eq!(loaded.next_refresh_after_ms, Some(1_700_000_000_000));

    Ok(())
}
```

- [ ] **Step 2: Run the repository test and verify it fails**

Run:

```bash
cargo test --test meta_repository mv_repository_refresh_policy_metadata_round_trips -- --nocapture
```

Expected: compile failure mentioning unresolved `StoredMvRefreshPolicy`, unresolved `UpdateMvRefreshMetadataRequest`, and missing fields on `StoredMvDefinition`.

- [ ] **Step 3: Add the persistent enum and request type**

In `src/meta/repository/mv.rs`, add this enum after `CreateMvDefinitionRequest`:

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StoredMvRefreshPolicy {
    #[default]
    Manual,
    AsyncOnChange,
    AsyncInterval,
}

impl StoredMvRefreshPolicy {
    pub fn as_sql_str(&self) -> &'static str {
        match self {
            Self::Manual => "DEFERRED_MANUAL",
            Self::AsyncOnChange => "ASYNC_ON_CHANGE",
            Self::AsyncInterval => "ASYNC_INTERVAL",
        }
    }

    fn accepts_interval(&self) -> bool {
        matches!(self, Self::AsyncInterval)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateMvRefreshMetadataRequest {
    pub mv_id: i64,
    pub refresh_policy: StoredMvRefreshPolicy,
    pub refresh_paused: bool,
    pub refresh_interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
    pub last_scheduler_error: Option<String>,
    pub next_refresh_after_ms: Option<i64>,
}
```

- [ ] **Step 4: Add refresh metadata fields to `StoredMvDefinition`**

In `src/meta/repository/mv.rs`, add these fields to `StoredMvDefinition` after `refresh_target_snapshots`:

```rust
    #[serde(default)]
    pub refresh_policy: StoredMvRefreshPolicy,
    #[serde(default)]
    pub refresh_paused: bool,
    #[serde(default)]
    pub refresh_interval_ms: Option<i64>,
    #[serde(default)]
    pub max_staleness_ms: Option<i64>,
    #[serde(default)]
    pub last_scheduler_error: Option<String>,
    #[serde(default)]
    pub next_refresh_after_ms: Option<i64>,
```

In `create_definition_with_id`, set default values in the `StoredMvDefinition` literal:

```rust
            refresh_policy: StoredMvRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
```

Do not change `MV_DEFINITION_SCHEMA_VERSION`. The existing payload version is still `2`; serde defaults keep older version-2 records readable, and older binaries ignore unknown JSON fields because the struct does not deny unknown fields.

- [ ] **Step 5: Add validation and update API**

In `impl MvMetaRepository`, add this public method after `create_definition_with_id`:

```rust
    pub fn update_refresh_metadata(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: UpdateMvRefreshMetadataRequest,
    ) -> RepositoryResult<StoredMvDefinition> {
        validate_refresh_metadata(&req)?;
        let mut definition = self.load_versioned_by_id(txn, req.mv_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv definition {} not found", req.mv_id))
        })?;
        definition.value.refresh_policy = req.refresh_policy;
        definition.value.refresh_paused = req.refresh_paused;
        definition.value.refresh_interval_ms = req.refresh_interval_ms;
        definition.value.max_staleness_ms = req.max_staleness_ms;
        definition.value.last_scheduler_error = req.last_scheduler_error;
        definition.value.next_refresh_after_ms = req.next_refresh_after_ms;
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;
        Ok(definition.value)
    }
```

Add this helper near `definition_target_matches`:

```rust
fn validate_refresh_metadata(req: &UpdateMvRefreshMetadataRequest) -> RepositoryResult<()> {
    if req.refresh_policy.accepts_interval() {
        match req.refresh_interval_ms {
            Some(value) if value > 0 => {}
            _ => {
                return Err(RepositoryError::invalid(
                    "ASYNC_INTERVAL refresh policy requires positive refresh_interval_ms",
                ));
            }
        }
    } else if req.refresh_interval_ms.is_some() {
        return Err(RepositoryError::invalid(format!(
            "{} refresh policy cannot set refresh_interval_ms",
            req.refresh_policy.as_sql_str()
        )));
    }

    if let Some(value) = req.max_staleness_ms
        && value <= 0
    {
        return Err(RepositoryError::invalid(
            "max_staleness_ms must be positive when set",
        ));
    }

    if let Some(value) = req.next_refresh_after_ms
        && value < 0
    {
        return Err(RepositoryError::invalid(
            "next_refresh_after_ms must be non-negative when set",
        ));
    }

    Ok(())
}
```

- [ ] **Step 6: Run the round-trip test and verify it passes**

Run:

```bash
cargo test --test meta_repository mv_repository_refresh_policy_metadata_round_trips -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Add validation regression tests**

In `tests/meta_repository.rs`, add this test after the round-trip test:

```rust
#[test]
fn mv_repository_rejects_invalid_refresh_policy_metadata() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition =
            repository.create_definition(txn.as_mut(), sample_mv_definition_request("select id from orders"))?;
        txn.commit()?;
        definition.mv_id
    };

    let mut txn = provider.begin_write("reject invalid refresh metadata")?;
    let manual_with_interval = repository.update_refresh_metadata(
        txn.as_mut(),
        UpdateMvRefreshMetadataRequest {
            mv_id,
            refresh_policy: StoredMvRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: Some(60_000),
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
        },
    );
    assert!(
        manual_with_interval
            .expect_err("manual policy must reject interval")
            .to_string()
            .contains("cannot set refresh_interval_ms")
    );

    let interval_without_interval = repository.update_refresh_metadata(
        txn.as_mut(),
        UpdateMvRefreshMetadataRequest {
            mv_id,
            refresh_policy: StoredMvRefreshPolicy::AsyncInterval,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
        },
    );
    assert!(
        interval_without_interval
            .expect_err("interval policy must require interval")
            .to_string()
            .contains("requires positive refresh_interval_ms")
    );

    let negative_staleness = repository.update_refresh_metadata(
        txn.as_mut(),
        UpdateMvRefreshMetadataRequest {
            mv_id,
            refresh_policy: StoredMvRefreshPolicy::AsyncOnChange,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: Some(0),
            last_scheduler_error: None,
            next_refresh_after_ms: None,
        },
    );
    assert!(
        negative_staleness
            .expect_err("zero max staleness must be rejected")
            .to_string()
            .contains("max_staleness_ms must be positive")
    );

    Ok(())
}
```

- [ ] **Step 8: Run repository validation tests**

Run:

```bash
cargo test --test meta_repository mv_repository_refresh_policy -- --nocapture
```

Expected: PASS for both `mv_repository_refresh_policy_metadata_round_trips` and `mv_repository_rejects_invalid_refresh_policy_metadata`.

- [ ] **Step 9: Commit repository metadata changes**

Run:

```bash
git add src/meta/repository/mv.rs tests/meta_repository.rs
git commit -m "feat: add MV refresh policy metadata"
```

Expected: commit succeeds.

---

### Task 2: `SHOW MATERIALIZED VIEWS` Display

**Files:**

- Modify: `src/engine/mv/lifecycle.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`

- [ ] **Step 1: Write the failing row-level display test**

In `src/connector/starrocks/managed/mv_ddl.rs`, change the helper signature:

```rust
    fn insert_iceberg_mv_relationship(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        select_sql: &str,
    ) -> i64 {
```

In the helper body, keep the created definition and return `mv_id`:

```rust
        let definition = state
            .mv_repo
            .create_definition(
                txn.as_mut(),
                CreateMvDefinitionRequest {
                    select_sql: select_sql.to_string(),
                    base_table_refs: vec![format!("{catalog}.sales.orders")],
                    primary_key_columns: Vec::new(),
                    storage_engine: ManagedMvStorageEngine::Iceberg.as_sql_str().to_string(),
                    target_catalog: Some(catalog.to_string()),
                    target_namespace: Some(namespace.to_string()),
                    target_table: Some(table.to_string()),
                    schema_contract: None,
                    partition_spec: None,
                    created_at_ms: now_ms(),
                },
            )
            .expect("insert iceberg mv relationship");
        let mv_id = definition.mv_id;
        txn.commit().expect("commit iceberg mv relationship");
        mv_id
```

Add imports at the top of `src/connector/starrocks/managed/mv_ddl.rs`:

```rust
use crate::meta::repository::mv::{CreateMvDefinitionRequest, StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest};
```

Replace the existing single import of `CreateMvDefinitionRequest` with the line above, keeping rustfmt to split it if needed.

Add this test after `show_materialized_views_lists_iceberg_relationship_without_managed_table_row`:

```rust
    #[test]
    fn show_materialized_views_exposes_refresh_policy_metadata() {
        let (state, _dir) = open_state_with_sqlite_store();
        let mv_id = insert_iceberg_mv_relationship(
            &state,
            "ice",
            "analytics",
            "mv_orders",
            "SELECT id FROM ice.sales.orders",
        );

        let provider = state.metadata_provider.as_ref().expect("metadata provider");
        let mut txn = provider
            .begin_write("set mv refresh metadata")
            .expect("open write txn");
        state
            .mv_repo
            .update_refresh_metadata(
                txn.as_mut(),
                UpdateMvRefreshMetadataRequest {
                    mv_id,
                    refresh_policy: StoredMvRefreshPolicy::AsyncInterval,
                    refresh_paused: true,
                    refresh_interval_ms: Some(300_000),
                    max_staleness_ms: Some(900_000),
                    last_scheduler_error: Some("catalog timeout".to_string()),
                    next_refresh_after_ms: Some(1_700_000_000_000),
                },
            )
            .expect("update refresh metadata");
        txn.commit().expect("commit refresh metadata");

        let stmt = ShowMaterializedViewsStmt { database: None };
        let rows = list_mv_rows(&state, Some("ice"), &stmt, None).expect("show mvs");
        let row = rows
            .iter()
            .find(|row| row.name == "mv_orders")
            .expect("mv row should be present");

        assert_eq!(row.refresh_mode, "ASYNC_INTERVAL");
        assert_eq!(row.refresh_paused, "true");
        assert_eq!(row.next_refresh_time.as_deref(), Some("1700000000000"));
        assert_eq!(row.last_scheduler_error.as_deref(), Some("catalog timeout"));
        assert_eq!(row.max_staleness_ms.as_deref(), Some("900000"));
    }
```

- [ ] **Step 2: Run the display test and verify it fails**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_exposes_refresh_policy_metadata -- --nocapture
```

Expected: compile failure mentioning missing `MvListRow` fields such as `refresh_paused`, `next_refresh_time`, `last_scheduler_error`, and `max_staleness_ms`.

- [ ] **Step 3: Extend `MvListRow`**

In `src/engine/mv/lifecycle.rs`, update `MvListRow`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvListRow {
    pub name: String,
    pub database: String,
    pub storage_engine: String,
    pub refresh_mode: String,
    pub last_refresh_time: Option<String>,
    pub last_refresh_rows: Option<String>,
    pub base_tables: String,
    pub select_text: String,
    pub dependencies: String,
    pub refresh_paused: String,
    pub next_refresh_time: Option<String>,
    pub last_scheduler_error: Option<String>,
    pub max_staleness_ms: Option<String>,
}
```

- [ ] **Step 4: Populate display fields in `list_mv_rows`**

In `src/connector/starrocks/managed/mv_ddl.rs`, replace both `rows.push(MvListRow { ... })` literals so they use stored policy metadata.

For the Iceberg branch:

```rust
            rows.push(MvListRow {
                name: target_table,
                database: target_namespace,
                storage_engine: mv.storage_engine.clone(),
                refresh_mode: mv.refresh_policy.as_sql_str().to_string(),
                last_refresh_time: mv.last_refresh_ms.map(|value| value.to_string()),
                last_refresh_rows: mv.last_refresh_rows.map(|value| value.to_string()),
                base_tables: mv.base_table_refs.join(", "),
                select_text: mv.select_sql.clone(),
                dependencies: dependency_display_for_mv(state, mv.mv_id)?,
                refresh_paused: mv.refresh_paused.to_string(),
                next_refresh_time: mv.next_refresh_after_ms.map(|value| value.to_string()),
                last_scheduler_error: mv.last_scheduler_error.clone(),
                max_staleness_ms: mv.max_staleness_ms.map(|value| value.to_string()),
            });
```

For the managed-lake branch:

```rust
        rows.push(MvListRow {
            name: table.name.clone(),
            database,
            storage_engine: mv.storage_engine.clone(),
            refresh_mode: mv.refresh_policy.as_sql_str().to_string(),
            last_refresh_time: mv.last_refresh_ms.map(|value| value.to_string()),
            last_refresh_rows: mv.last_refresh_rows.map(|value| value.to_string()),
            base_tables: mv.base_table_refs.join(", "),
            select_text: mv.select_sql.clone(),
            dependencies: dependency_display_for_mv(state, mv.mv_id)?,
            refresh_paused: mv.refresh_paused.to_string(),
            next_refresh_time: mv.next_refresh_after_ms.map(|value| value.to_string()),
            last_scheduler_error: mv.last_scheduler_error.clone(),
            max_staleness_ms: mv.max_staleness_ms.map(|value| value.to_string()),
        });
```

- [ ] **Step 5: Append result columns in `build_mv_rows_result`**

In `src/connector/starrocks/managed/mv_ddl.rs`, append these `QueryResultColumn` entries after `Dependencies`:

```rust
        QueryResultColumn {
            name: "RefreshPaused".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "NextRefreshTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastSchedulerError".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "MaxStalenessMs".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
```

Append matching `Field` entries after the existing `Dependencies` field:

```rust
        Field::new("RefreshPaused", DataType::Utf8, false),
        Field::new("NextRefreshTime", DataType::Utf8, true),
        Field::new("LastSchedulerError", DataType::Utf8, true),
        Field::new("MaxStalenessMs", DataType::Utf8, true),
```

Append matching arrays after the existing `dependencies` array:

```rust
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_paused.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.next_refresh_time.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_scheduler_error.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.max_staleness_ms.clone())
                .collect::<Vec<_>>(),
        )),
```

- [ ] **Step 6: Run the display test and verify it passes**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_exposes_refresh_policy_metadata -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Run existing MV DDL display tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_lists_iceberg_relationship_without_managed_table_row -- --nocapture
cargo test --lib connector::starrocks::managed::mv_ddl::tests::list_mv_rows_filters_managed_and_iceberg_storage_engines -- --nocapture
```

Expected: both PASS.

- [ ] **Step 8: Commit display changes**

Run:

```bash
git add src/engine/mv/lifecycle.rs src/connector/starrocks/managed/mv_ddl.rs
git commit -m "feat: show MV refresh policy metadata"
```

Expected: commit succeeds.

---

### Task 3: Exact MySQL Row Consumer Update

**Files:**

- Modify: `tests/standalone_mysql_server.rs`

- [ ] **Step 1: Run the exact row test and verify the current failure**

Run:

```bash
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
```

Expected before updating the test: failure or compile mismatch because `SHOW MATERIALIZED VIEWS` returns the appended metadata columns.

- [ ] **Step 2: Update the tuple type and assertions**

In `tests/standalone_mysql_server.rs`, replace the `MvShowRow` type inside `standalone_mysql_server_mv_show_output_matches_expected_columns`:

```rust
    type MvShowRow = (
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    );
```

Update the assertions after `assert!(row.7.to_ascii_lowercase().contains("select"));`:

```rust
    assert_eq!(row.8, "");
    assert_eq!(row.9, "false");
    assert_eq!(row.10, None);
    assert_eq!(row.11, None);
    assert_eq!(row.12, None);
```

Column order after this task:

```text
0 Name
1 Database
2 StorageEngine
3 RefreshMode
4 LastRefreshTime
5 LastRefreshRows
6 BaseTables
7 SelectText
8 Dependencies
9 RefreshPaused
10 NextRefreshTime
11 LastSchedulerError
12 MaxStalenessMs
```

This preserves `LastRefreshRows` at index `5`, so helper code in `src/connector/starrocks/managed/mv_refresh.rs::show_mv_last_refresh_rows` does not need a change.

- [ ] **Step 3: Run the exact row test**

Run:

```bash
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
```

Expected: PASS. If the test skips due unavailable managed-lake config, record that it skipped and continue with the unit tests from Tasks 1 and 2.

- [ ] **Step 4: Commit test consumer update**

Run:

```bash
git add tests/standalone_mysql_server.rs
git commit -m "test: update MV show metadata columns"
```

Expected: commit succeeds.

---

### Task 4: Final Verification

**Files:**

- Verify only, unless formatting changes are required.

- [ ] **Step 1: Run formatter**

Run:

```bash
cargo fmt
```

Expected: command exits 0. If it changes files, inspect `git diff --stat` and include only files from this plan.

- [ ] **Step 2: Run focused repository tests**

Run:

```bash
cargo test --test meta_repository mv_repository_refresh_policy -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Run focused MV DDL tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_exposes_refresh_policy_metadata -- --nocapture
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_lists_iceberg_relationship_without_managed_table_row -- --nocapture
cargo test --lib connector::starrocks::managed::mv_ddl::tests::list_mv_rows_filters_managed_and_iceberg_storage_engines -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Run the exact MySQL output test**

Run:

```bash
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
```

Expected: PASS or environment skip from `maybe_write_managed_lake_config`. A skip is acceptable for this plan only if all repository and MV DDL unit tests pass.

- [ ] **Step 5: Run format check**

Run:

```bash
cargo fmt --check
```

Expected: PASS.

- [ ] **Step 6: Inspect final diff**

Run:

```bash
git status --short
git diff --stat
```

Expected: only planned files changed if there are uncommitted changes after formatting:

```text
src/meta/repository/mv.rs
tests/meta_repository.rs
src/engine/mv/lifecycle.rs
src/connector/starrocks/managed/mv_ddl.rs
tests/standalone_mysql_server.rs
```

- [ ] **Step 7: Commit formatting changes if needed**

If `cargo fmt` changed files after earlier commits, run:

```bash
git add src/meta/repository/mv.rs tests/meta_repository.rs src/engine/mv/lifecycle.rs src/connector/starrocks/managed/mv_ddl.rs tests/standalone_mysql_server.rs
git commit -m "style: format MV refresh policy metadata"
```

Expected: commit succeeds only if there are formatting changes. If there are no changes, do not create an empty commit.

## Self-Review Notes

Spec coverage:

- `refresh_policy`, `refresh_paused`, `refresh_interval_ms`, `max_staleness_ms`, `last_scheduler_error`, and `next_refresh_after_ms` are added to MV metadata in Task 1.
- Default `MANUAL` behavior is covered by Task 1 tests and displayed through the existing `RefreshMode` column in Task 2.
- Metadata round-trip is covered by `mv_repository_refresh_policy_metadata_round_trips`.
- `SHOW MATERIALIZED VIEWS` visibility is covered by Task 2 and Task 3.
- Existing manual refresh execution remains unchanged because no code in `mv_flow`, `MvBackend`, `mv_refresh`, or `iceberg_refresh` is modified.

Placeholder scan:

- This plan contains no placeholder implementation steps. Every code-changing step has concrete snippets and verification commands.

Type consistency:

- Persistent enum name: `StoredMvRefreshPolicy`.
- Repository update request: `UpdateMvRefreshMetadataRequest`.
- Display fields on `MvListRow`: `refresh_paused`, `next_refresh_time`, `last_scheduler_error`, `max_staleness_ms`.
- `SHOW MATERIALIZED VIEWS` appended column names: `RefreshPaused`, `NextRefreshTime`, `LastSchedulerError`, `MaxStalenessMs`.
