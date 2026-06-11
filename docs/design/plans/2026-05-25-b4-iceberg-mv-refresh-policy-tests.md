# B4 Iceberg MV Refresh Policy Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move B4 refresh policy SQL coverage onto the Iceberg target MV path and strengthen deterministic Rust tests for scheduler/status semantics.

**Architecture:** Use `sql-tests/iceberg-ivm` as the user-facing regression gate because Iceberg target MVs are created with `PROPERTIES ('storage_engine' = 'iceberg')`. Keep timing-sensitive scheduler behavior in Rust unit tests and repository/show tests so retry, recovery, and status derivation stay deterministic. Remove the earlier `mv-on-iceberg` refresh-policy SQL case so managed-lake target coverage is not mistaken for the primary B4 path.

**Tech Stack:** Rust, NovaRocks SQL test runner, `sql-tests/iceberg-ivm`, existing MV metadata repository tests, existing generated Iceberg REST/MinIO environment.

---

### Task 1: Migrate SQL Coverage To Iceberg Target MV

**Files:**
- Delete: `sql-tests/mv-on-iceberg/sql/managed_lake_mv_refresh_policy_metadata.sql`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_refresh_policy_metadata.sql`

- [ ] **Step 1: Confirm the old case is currently in the wrong suite**

Run:

```bash
test -f sql-tests/mv-on-iceberg/sql/managed_lake_mv_refresh_policy_metadata.sql
rg -n "orders_policy_mv|REFRESH ASYNC EVERY INTERVAL|SHOW MATERIALIZED VIEWS" \
  sql-tests/mv-on-iceberg/sql/managed_lake_mv_refresh_policy_metadata.sql
```

Expected: the file exists and creates a managed-lake target MV because it has no
`PROPERTIES ('storage_engine' = 'iceberg')` clause.

- [ ] **Step 2: Delete the wrong-suite SQL case**

Run:

```bash
git rm sql-tests/mv-on-iceberg/sql/managed_lake_mv_refresh_policy_metadata.sql
```

Expected: the file is staged for deletion.

- [ ] **Step 3: Add the Iceberg target SQL case**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_refresh_policy_metadata.sql` with exactly this content:

```sql
-- @sequential=true
-- @tags=mv,iceberg,ivm,storage_engine_iceberg,refresh_policy,scheduler
-- Test Objective:
-- 1. Validate Iceberg target MV refresh policy metadata is visible through SHOW MATERIALIZED VIEWS.
-- 2. Validate PAUSE/RESUME and ALTER SET REFRESH update user-facing scheduler state.

-- query 1
CREATE EXTERNAL CATALOG ice_ivm_policy_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_policy_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_policy_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_policy_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_policy_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_policy_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 1
REFRESH ASYNC EVERY INTERVAL 5 MINUTE
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders;

-- query 2
-- @result_contains=orders_policy_mv_
-- @result_contains=iceberg
-- @result_contains=ASYNC_INTERVAL
-- @result_contains=RefreshState
-- @result_contains=RetryAfterTime
-- @result_contains=PENDING
SHOW MATERIALIZED VIEWS;

-- query 3
ALTER MATERIALIZED VIEW orders_policy_mv_${uuid0} PAUSE REFRESH;

-- query 4
-- @result_contains=orders_policy_mv_
-- @result_contains=ASYNC_INTERVAL
-- @result_contains=true
-- @result_contains=PAUSED
SHOW MATERIALIZED VIEWS;

-- query 5
ALTER MATERIALIZED VIEW orders_policy_mv_${uuid0} SET REFRESH ASYNC ON CHANGE;
ALTER MATERIALIZED VIEW orders_policy_mv_${uuid0} RESUME REFRESH;

-- query 6
-- @result_contains=orders_policy_mv_
-- @result_contains=ASYNC_ON_CHANGE
-- @result_contains=false
-- @result_contains=PENDING
SHOW MATERIALIZED VIEWS;

-- query 7
DROP MATERIALIZED VIEW orders_policy_mv_${uuid0};
DROP TABLE ice_ivm_policy_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_policy_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_policy_${uuid0};
```

- [ ] **Step 4: Start the generated test environment and server**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build
LOG=/tmp/novarocks-b4-iceberg-refresh-policy-server.log
PIDFILE=/tmp/novarocks-b4-iceberg-refresh-policy-server.pid
NO_PROXY=127.0.0.1,localhost \
  target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
echo "$SRV_PID" >"$PIDFILE"
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then
    break
  fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone-server died during startup; tail of $LOG:" >&2
    tail -40 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || {
  echo "timed out waiting for NOVAROCKS_READY" >&2
  tail -40 "$LOG" >&2
  kill "$SRV_PID"
  exit 1
}
```

Expected: the log contains `NOVAROCKS_READY mysql_port=<port>`.

- [ ] **Step 5: Verify the migrated SQL case**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_refresh_policy_metadata \
  --mode verify
```

Expected: `total=1`, `pass=1`, `fail=0`.

- [ ] **Step 6: Stop the standalone server**

Run:

```bash
PIDFILE=/tmp/novarocks-b4-iceberg-refresh-policy-server.pid
SRV_PID=$(cat "$PIDFILE")
kill "$SRV_PID"
sleep 1
source docker/iceberg-rest/runtime/current/env.sh
lsof -nP -iTCP:"$NOVA_ENV_MYSQL_PORT" -sTCP:LISTEN || true
```

Expected: no listener remains on the generated NovaRocks MySQL port.

- [ ] **Step 7: Confirm no wrong-suite refresh-policy SQL case remains**

Run:

```bash
rg -n "managed_lake_mv_refresh_policy_metadata|orders_policy_mv" sql-tests/mv-on-iceberg || true
rg -n "orders_policy_mv_|REFRESH ASYNC EVERY INTERVAL|storage_engine' = 'iceberg'" \
  sql-tests/iceberg-ivm/sql/iceberg_ivm_refresh_policy_metadata.sql
```

Expected: the first command prints nothing; the second command finds the new
Iceberg target case.

- [ ] **Step 8: Commit SQL migration**

Run:

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_refresh_policy_metadata.sql
git add -u sql-tests/mv-on-iceberg/sql/managed_lake_mv_refresh_policy_metadata.sql
git commit -m "test: move MV refresh policy SQL coverage to Iceberg target"
```

Expected: one commit containing the old file deletion and new `iceberg-ivm`
case.

### Task 2: Strengthen Deterministic Scheduler And SHOW Tests

**Files:**
- Modify: `src/engine/mv_scheduler.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`

- [ ] **Step 1: Add scheduler regression tests**

In `src/engine/mv_scheduler.rs`, inside the existing `#[cfg(test)] mod tests`,
add these tests after `non_retryable_user_error_does_not_plan_periodic_retry`:

```rust
    #[test]
    fn periodic_policy_skips_paused_and_enqueues_after_resume() {
        let mut paused = test_definition(7, StoredMvRefreshPolicy::AsyncInterval);
        paused.refresh_interval_ms = Some(1_000);
        paused.next_refresh_after_ms = Some(500);
        paused.refresh_paused = true;

        assert!(plan_periodic_refreshes(&[paused.clone()], 1_000).is_empty());

        let mut resumed = paused;
        resumed.refresh_paused = false;

        let decisions = plan_periodic_refreshes(&[resumed], 1_000);

        assert_eq!(
            decisions
                .into_iter()
                .map(|decision| decision.mv_id)
                .collect::<Vec<_>>(),
            vec![7]
        );
    }

    #[test]
    fn scheduler_guard_reports_non_retryable_user_error() {
        let mut definition = test_definition(7, StoredMvRefreshPolicy::AsyncInterval);
        definition.refresh_interval_ms = Some(1_000);
        definition.last_scheduler_error = Some("USER_ERROR: unsupported MV shape".to_string());

        let decision = scheduler_guard_for_definition(&definition, None, 1_000);

        assert_eq!(decision.state, RefreshTaskState::FailedUserError);
        assert!(!decision.can_enqueue);
    }

    #[test]
    fn successful_drain_resets_failure_attempts() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.failure_attempts.insert(7, 3);
        coordinator.enqueue_refresh(7, RefreshTaskReason::Manual);
        let mut executor = RecordingRefreshExecutor::default();

        coordinator
            .drain_ready_for_test(&mut executor, 1_000)
            .expect("drain succeeds");

        assert_eq!(executor.executed_mv_ids(), vec![7]);
        assert!(!coordinator.failure_attempts.contains_key(&7));
        assert_eq!(
            coordinator.state_for_mv(7),
            Some(RefreshTaskState::Succeeded)
        );
    }
```

- [ ] **Step 2: Run the scheduler tests**

Run:

```bash
cargo test --lib engine::mv_scheduler -- --nocapture
```

Expected: all scheduler tests pass.

- [ ] **Step 3: Add Iceberg target SHOW recovery-state test imports**

In `src/connector/starrocks/managed/mv_ddl.rs`, change the test-only MV import
from:

```rust
#[cfg(test)]
use crate::meta::repository::mv::{StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest};
```

to:

```rust
#[cfg(test)]
use crate::meta::repository::mv::{
    BeginIcebergMvRefreshRequest, StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest,
};
```

- [ ] **Step 4: Add Iceberg target blocked-recovery SHOW test**

In `src/connector/starrocks/managed/mv_ddl.rs`, inside the existing
`#[cfg(test)] mod tests`, add this test after
`show_materialized_views_exposes_non_retryable_scheduler_error`:

```rust
    #[test]
    fn show_materialized_views_exposes_iceberg_target_blocked_recovery() {
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
            .begin_write("seed commit-unknown refresh")
            .expect("open write txn");
        let refresh = state
            .mv_repo
            .begin_iceberg_refresh_intent(
                txn.as_mut(),
                BeginIcebergMvRefreshRequest {
                    mv_id,
                    target_catalog: "ice".to_string(),
                    target_namespace: "analytics".to_string(),
                    target_table: "mv_orders".to_string(),
                    staging_branch: "__nova_mv_refresh_test".to_string(),
                    expected_main_snapshot_id: None,
                    base_snapshots: std::collections::BTreeMap::new(),
                    marker_token: "marker".to_string(),
                },
            )
            .expect("begin iceberg refresh intent");
        state
            .mv_repo
            .mark_refresh_commit_unknown(txn.as_mut(), refresh.refresh_id)
            .expect("mark commit unknown");
        txn.commit().expect("commit refresh metadata");

        let stmt = ShowMaterializedViewsStmt { database: None };
        let rows = list_mv_rows(&state, Some("ice"), &stmt, None).expect("show mvs");
        let row = rows
            .iter()
            .find(|row| row.name == "mv_orders")
            .expect("mv row should be present");

        assert_eq!(row.storage_engine, "iceberg");
        assert_eq!(row.refresh_state, "BLOCKED_RECOVERY");
    }
```

- [ ] **Step 5: Tighten the existing refresh metadata test for Iceberg target context**

In `show_materialized_views_exposes_refresh_policy_metadata`, add this assertion
before `assert_eq!(row.refresh_mode, "ASYNC_INTERVAL");`:

```rust
        assert_eq!(row.storage_engine, "iceberg");
```

- [ ] **Step 6: Run focused SHOW tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_exposes -- --nocapture
```

Expected: the refresh metadata, non-retryable error, and blocked recovery SHOW
tests pass.

- [ ] **Step 7: Format and commit Rust test strengthening**

Run:

```bash
cargo fmt --check
git add src/engine/mv_scheduler.rs src/connector/starrocks/managed/mv_ddl.rs
git commit -m "test: strengthen MV refresh scheduler status coverage"
```

Expected: one commit with only deterministic Rust test updates.

### Task 3: Final Verification

**Files:**
- Verify only; no file edits expected.

- [ ] **Step 1: Run focused Rust verification**

Run:

```bash
cargo fmt --check
cargo check --lib
cargo test --lib engine::mv_scheduler -- --nocapture
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_exposes -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
```

Expected: all commands exit 0. Existing repository warnings are acceptable if
they do not fail the commands.

- [ ] **Step 2: Run migrated SQL verification**

Run with the same server startup pattern from Task 1:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build
LOG=/tmp/novarocks-b4-iceberg-refresh-policy-server.log
PIDFILE=/tmp/novarocks-b4-iceberg-refresh-policy-server.pid
NO_PROXY=127.0.0.1,localhost \
  target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
echo "$SRV_PID" >"$PIDFILE"
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then
    break
  fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone-server died during startup; tail of $LOG:" >&2
    tail -40 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || {
  echo "timed out waiting for NOVAROCKS_READY" >&2
  tail -40 "$LOG" >&2
  kill "$SRV_PID"
  exit 1
}
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_refresh_policy_metadata \
  --mode verify
kill "$SRV_PID"
```

Expected: SQL runner summary shows `total=1`, `pass=1`, `fail=0`, and the
server is stopped after the run.

- [ ] **Step 3: Check whitespace and worktree scope**

Run:

```bash
git diff --check HEAD~4..HEAD
git status --short
```

Expected: `git diff --check HEAD~4..HEAD` exits 0. `git status --short` may
still show the pre-existing unrelated `src/sql/codegen/expr_compiler.rs` dirty
diff; no B4 test files should be unstaged.
