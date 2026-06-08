# MV Refresh Operation Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Connect branch-staged Iceberg MV refreshes to the shared Iceberg operation lifecycle introduced by Phase 2.

**Architecture:** Keep MV refresh state as the MV-domain summary, and use `iceberg.operation` records as the owner of commit/finalize facts. Add a nullable `operation_id` to `StoredMvRefresh`, create the operation in the same transaction as the staged refresh intent, and advance the operation from MV refresh helper functions. Metadata-only refreshes stay outside the operation lifecycle because they do not enter an Iceberg metadata commit critical section.

**Tech Stack:** Rust, NovaRocks metadata repository, Avro metadata payloads, Iceberg MV refresh driver tests.

---

## File Structure

- Modify `src/meta/repository/mv.rs`: add `StoredMvRefresh::operation_id`, persist it, and add an optional `operation_id` field to `BeginIcebergMvRefreshRequest`.
- Add `src/meta/avro/schemas/mv.refresh/0002.avsc`: add nullable `operation_id` with default `null`, and register it as the latest `mv.refresh` schema while preserving `0001.avsc` for old records.
- Modify `src/engine/mv/iceberg_refresh.rs`: create `IcebergOperationKind::MvRefresh` records for staged refresh intents and record lifecycle facts on commit, commit-unknown, abort, and finalize.
- Modify `tests/meta_repository.rs`: cover repository persistence for the new refresh `operation_id`.
- Modify `src/engine/mv/iceberg_refresh.rs` tests: cover end-to-end staged refresh operation creation and finalization.

## Task 1: Persist MV Refresh Operation Id

- [x] **Step 1: Add a failing repository test**

Add a test near `mv_repository_branch_staged_refresh_lifecycle` in `tests/meta_repository.rs`:

```rust
#[test]
fn mv_repository_branch_staged_refresh_persists_operation_id()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_mv_definition(&provider, &repository)?;

    let refresh_id = {
        let mut txn = provider.begin_write("begin branch-staged refresh")?;
        let refresh = repository.begin_iceberg_refresh_intent(
            txn.as_mut(),
            BeginIcebergMvRefreshRequest {
                mv_id,
                target_catalog: "ice".to_string(),
                target_namespace: "analytics".to_string(),
                target_table: "orders_mv".to_string(),
                staging_branch: "__nova_mv_refresh_1_1001".to_string(),
                expected_main_snapshot_id: Some(10),
                base_snapshots: BTreeMap::new(),
                marker_token: "marker".to_string(),
                operation_id: Some(99),
            },
        )?;
        assert_eq!(refresh.operation_id, Some(99));
        txn.commit()?;
        refresh.refresh_id
    };

    let read = provider.begin_read()?;
    let loaded = repository
        .load_refresh(read.as_ref(), refresh_id)?
        .expect("refresh");
    assert_eq!(loaded.operation_id, Some(99));
    Ok(())
}
```

- [x] **Step 2: Run the test and confirm RED**

Run:

```bash
cargo test --test meta_repository mv_repository_branch_staged_refresh_persists_operation_id -- --nocapture
```

Expected: compile failure because `BeginIcebergMvRefreshRequest::operation_id` and `StoredMvRefresh::operation_id` do not exist.

- [x] **Step 3: Add the field and Avro schema entry**

Add `operation_id: Option<i64>` to `StoredMvRefresh`, `BeginIcebergMvRefreshRequest`, and every `StoredMvRefresh` construction. Add a new `mv.refresh/0002.avsc` schema version with this Avro field after `mv_id`, leaving `0001.avsc` unchanged:

```json
{ "name": "operation_id", "type": ["null", "long"], "default": null }
```

- [x] **Step 4: Run the repository test and Avro catalog tests**

Run:

```bash
cargo test --test meta_repository mv_repository_branch_staged_refresh_persists_operation_id -- --nocapture
cargo test --test meta_avro_catalog -- --nocapture
```

Expected: both pass.

## Task 2: Create Operation Records for Staged MV Refreshes

- [x] **Step 1: Add a failing MV refresh driver test**

Add a test in `src/engine/mv/iceberg_refresh.rs` test module:

```rust
#[test]
fn staged_iceberg_mv_refresh_creates_operation_record() {
    let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
    create_base_table(&env.state, "ice", "sales", "orders");
    create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
    let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
        .expect("mv definition");
    let target = IcebergMvTarget {
        catalog: "ice".to_string(),
        namespace: "analytics".to_string(),
        table: "mv_orders".to_string(),
    };

    let refresh_id = begin_staged_iceberg_mv_refresh_intent(
        &env.state,
        &target,
        mv.mv_id,
        Some(10),
        BTreeMap::from([("ice.sales.orders".to_string(), 20)]),
        "__nova_mv_refresh_operation",
    )
    .expect("begin staged refresh");

    let provider = env.state.metadata_provider.as_ref().expect("provider");
    let read = provider.begin_read().expect("read");
    let refresh = env
        .state
        .mv_repo
        .load_refresh(read.as_ref(), refresh_id)
        .expect("load refresh")
        .expect("refresh");
    let operation_id = refresh.operation_id.expect("operation id");
    let operation = env
        .state
        .iceberg_operation_repo
        .load_operation(read.as_ref(), operation_id)
        .expect("load operation")
        .expect("operation");
    assert_eq!(operation.operation_kind, IcebergOperationKind::MvRefresh);
    assert_eq!(operation.target.catalog, "ice");
    assert_eq!(operation.target.namespace, "analytics");
    assert_eq!(operation.target.table, "mv_orders");
    assert_eq!(operation.base_snapshot_id, Some(10));
    assert_eq!(operation.base_snapshot_map["ice.sales.orders"], 20);
    assert_eq!(operation.staged_artifacts, vec!["branch:__nova_mv_refresh_operation"]);
}
```

- [x] **Step 2: Run the test and confirm RED**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::staged_iceberg_mv_refresh_creates_operation_record -- --nocapture
```

Expected: compile failure or assertion failure because staged refresh does not create an operation record yet.

- [x] **Step 3: Create operation and refresh atomically**

In `begin_staged_iceberg_mv_refresh_intent`, allocate the MV refresh first, create an `IcebergOperationKind::MvRefresh` operation in the same transaction with target/ref/staged branch metadata, then store the operation id on the refresh.

- [x] **Step 4: Run the test and confirm GREEN**

Run the same `cargo test --lib ...staged_iceberg_mv_refresh_creates_operation_record` command. Expected: pass.

## Task 3: Advance Operation Facts Along MV Refresh

- [x] **Step 1: Add tests for commit/finalize state transitions**

Add tests that call the existing private helpers:

- `record_iceberg_mv_staging_commit` moves the operation to `Committed` with the staging snapshot.
- `record_iceberg_mv_publish_commit` keeps the operation committed with the published snapshot.
- `finalize_iceberg_mv_refresh` moves the operation to `Finalized`.
- `mark_iceberg_mv_refresh_commit_unknown` moves the operation to `CommitUnknown` and does not finish it.

- [x] **Step 2: Run the tests and confirm RED**

Run the targeted `cargo test --lib engine::mv::iceberg_refresh::tests::<test-name> -- --nocapture` commands. Expected: operation remains `Preparing`.

- [x] **Step 3: Add helper functions**

Add focused helpers in `iceberg_refresh.rs`:

- `load_mv_refresh_operation_id`
- `record_mv_operation_fact`
- `transition_mv_operation`
- `commit_outcome_for_mv_snapshot`
- `mv_commit_unknown_fact`
- `mv_finalize_failure_fact`

The helpers should use the stored `operation_id`; they must return a clear error if a staged refresh lacks one.

- [x] **Step 4: Wire existing lifecycle helpers**

Wire these functions:

- `record_iceberg_mv_staging_commit`: record operation `Committed`.
- `record_iceberg_mv_publish_commit`: refine operation `Committed` to published snapshot.
- `mark_iceberg_mv_refresh_commit_unknown`: record operation `CommitUnknown`.
- `abort_iceberg_mv_refresh`: transition operation to `Aborting` then `Aborted` when the MV abort succeeds.
- `finalize_iceberg_mv_refresh`: transition `Committed -> Finalizing`, finalize MV metadata, then transition `Finalizing -> Finalized`; if finalize fails after `Finalizing`, record `FinalizeFailedKnownCommitted`.

- [x] **Step 5: Run targeted tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::staged_iceberg_mv_refresh_creates_operation_record -- --nocapture
cargo test --lib engine::mv::iceberg_refresh::tests::recover_ -- --nocapture
cargo test --test meta_repository mv_repository_branch_staged_refresh -- --nocapture
```

Expected: all targeted tests pass.

## Task 4: Final Verification

- [x] **Step 1: Format and build**

Run:

```bash
cargo fmt
cargo build --lib
```

- [x] **Step 2: Run Iceberg MV tests**

Run the focused library tests first:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests -- --nocapture
```

If Docker-backed SQL verification is required for changed behavior, start the generated environment and run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify
```

- [x] **Step 3: Inspect diff and prepare PR**

Run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors and only intentional files changed.
