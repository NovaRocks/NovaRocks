# B4 Refresh Observability And Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成 B4 中除 B4-7 MV rewrite/freshness rewrite 之外的剩余工作：B4-8 用户可观测性与运维控制、B4-9 scheduler 生产加固。

**Architecture:** 继续复用 `SHOW MATERIALIZED VIEWS` 作为第一版运维视图，不新增 rewrite 入口。`mv_scheduler` 增加自动调度前置 guard、retry 分类和指数 backoff；`mv_ddl` 从 metadata 和 active refresh 记录推导可展示状态。

**Tech Stack:** Rust, existing MV metadata repository, existing `SHOW MATERIALIZED VIEWS`, existing Iceberg refresh recovery, `cargo test`.

---

### Task 1: B4-8 Refresh Status Observability

**Files:**
- Modify: `src/engine/mv/lifecycle.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`
- Modify: `tests/standalone_mysql_server.rs`

- [ ] **Step 1: Write failing tests**

Add or update tests so `SHOW MATERIALIZED VIEWS` exposes `RefreshState` and `RetryAfterTime`:

```rust
assert_eq!(row.refresh_state, "FAILED_BACKOFF");
assert_eq!(row.retry_after_time.as_deref(), Some("1700000000000"));
```

Update `standalone_mysql_server_mv_show_output_matches_expected_columns` to include the new columns after `MaxStalenessMs`:

```rust
vec![
    "Name", "Database", "StorageEngine", "RefreshMode", "LastRefreshTime",
    "LastRefreshRows", "BaseTables", "SelectText", "Dependencies",
    "RefreshPaused", "NextRefreshTime", "LastSchedulerError",
    "MaxStalenessMs", "RefreshState", "RetryAfterTime",
]
```

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_exposes_refresh_policy_metadata -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
```

Expected: fail because the new fields/columns do not exist.

- [ ] **Step 2: Implement status derivation**

Add `refresh_state` and `retry_after_time` to `MvListRow`.

Derive state conservatively:
- paused -> `PAUSED`
- active refresh with `COMMIT_UNKNOWN` -> `BLOCKED_RECOVERY`
- active refresh in any other unfinished state -> `RUNNING`
- scheduler error with future `next_refresh_after_ms` -> `FAILED_BACKOFF`
- automatic policy with due or missing `next_refresh_after_ms` -> `PENDING`
- automatic policy with future `next_refresh_after_ms` -> `SUCCEEDED`
- manual policy with no active scheduler state -> `MANUAL`

`RetryAfterTime` is populated only when `LastSchedulerError` is set and `next_refresh_after_ms` is in the future.

- [ ] **Step 3: Verify and commit**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_exposes_refresh_policy_metadata -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
cargo fmt --check
git add src/engine/mv/lifecycle.rs src/connector/starrocks/managed/mv_ddl.rs tests/standalone_mysql_server.rs
git commit -m "feat: expose MV refresh scheduler status"
```

Expected: tests pass; existing PAUSE/RESUME DDL remains unchanged.

### Task 2: B4-9 Automatic Refresh Recovery Guards

**Files:**
- Modify: `src/engine/mv_scheduler.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`

- [ ] **Step 1: Write failing tests**

Add scheduler tests:

```rust
#[test]
fn scheduler_blocks_commit_unknown_refresh() {
    let mut definition = test_definition(7, StoredMvRefreshPolicy::AsyncInterval);
    definition.refresh_interval_ms = Some(10_000);
    definition.active_refresh_id = Some(99);
    definition.refresh_in_progress = true;
    let active = ActiveRefreshState {
        refresh_id: 99,
        state: MvRefreshState::CommitUnknown,
    };

    let decision = scheduler_guard_for_definition(&definition, Some(&active), 1_000);

    assert_eq!(decision.state, RefreshTaskState::BlockedRecovery);
    assert!(!decision.can_enqueue);
}

#[test]
fn scheduler_skips_running_refresh_without_reenqueue() {
    let mut definition = test_definition(7, StoredMvRefreshPolicy::AsyncInterval);
    definition.refresh_interval_ms = Some(10_000);
    definition.active_refresh_id = Some(99);
    definition.refresh_in_progress = true;
    let active = ActiveRefreshState {
        refresh_id: 99,
        state: MvRefreshState::IntentCreated,
    };

    let decision = scheduler_guard_for_definition(&definition, Some(&active), 1_000);

    assert_eq!(decision.state, RefreshTaskState::Running);
    assert!(!decision.can_enqueue);
}
```

Expected: fail because the guard does not exist and scheduling still only checks policy.

- [ ] **Step 2: Implement guard**

Implement an `ActiveRefreshState` read model and `scheduler_guard_for_definition`.

Use it in periodic and snapshot-watch planning:
- `CommitUnknown` blocks enqueue and records/keeps `BLOCKED_RECOVERY`.
- other active refresh states are treated as running and skipped.
- paused MVs remain skipped.

At runtime, use `active_refresh_id` and `mv_repo.load_refresh(...)` inside the scheduler metadata read transaction.

- [ ] **Step 3: Verify and commit**

Run:

```bash
cargo test --lib engine::mv_scheduler::tests::scheduler_blocks_commit_unknown_refresh -- --nocapture
cargo test --lib engine::mv_scheduler::tests::scheduler_skips_running_refresh_without_reenqueue -- --nocapture
cargo test --lib engine::mv_scheduler -- --nocapture
cargo fmt --check
git add src/engine/mv_scheduler.rs src/connector/starrocks/managed/mv_ddl.rs
git commit -m "feat: block automatic MV refresh during recovery"
```

Expected: scheduler no longer retries active or commit-unknown MVs.

### Task 3: B4-9 Retry Policy Hardening

**Files:**
- Modify: `src/common/app_config.rs`
- Modify: `src/engine/mv_scheduler.rs`

- [ ] **Step 1: Write failing tests**

Add tests:

```rust
#[test]
fn transient_failures_use_bounded_exponential_backoff() {
    let config = RefreshCoordinatorConfig {
        enabled: true,
        failure_backoff_ms: 1_000,
        max_failure_backoff_ms: 8_000,
        ..RefreshCoordinatorConfig::default()
    };
    assert_eq!(scheduler_backoff_ms(&config, 1), 1_000);
    assert_eq!(scheduler_backoff_ms(&config, 2), 2_000);
    assert_eq!(scheduler_backoff_ms(&config, 5), 8_000);
}

#[test]
fn non_retryable_user_error_does_not_plan_periodic_retry() {
    let mut definition = test_definition(7, StoredMvRefreshPolicy::AsyncInterval);
    definition.refresh_interval_ms = Some(1_000);
    definition.last_scheduler_error = Some("USER_ERROR: unsupported MV shape".to_string());
    definition.next_refresh_after_ms = None;

    let decisions = plan_periodic_refreshes(&[definition], 10_000);

    assert!(decisions.is_empty());
}
```

Expected: fail because max backoff and retryable classification are missing.

- [ ] **Step 2: Implement retry policy**

Add hidden config:
- `mv_refresh_scheduler_max_failure_backoff_ms`, default `1_800_000` (30 minutes)

Add helpers:
- `scheduler_backoff_ms(config, attempt)`
- `classify_scheduler_failure(err)`
- `is_retryable_scheduler_error_text(err)`

Runtime behavior:
- transient failure stores raw error and schedules exponential backoff.
- user error stores `USER_ERROR: <err>`, leaves `next_refresh_after_ms` empty, and planner does not automatically retry until metadata changes clears the error.
- success clears the error and resets in-memory attempts for that MV.

- [ ] **Step 3: Verify and commit**

Run:

```bash
cargo test --lib engine::mv_scheduler::tests::transient_failures_use_bounded_exponential_backoff -- --nocapture
cargo test --lib engine::mv_scheduler::tests::non_retryable_user_error_does_not_plan_periodic_retry -- --nocapture
cargo test --lib engine::mv_scheduler -- --nocapture
cargo fmt --check
git add src/common/app_config.rs src/engine/mv_scheduler.rs
git commit -m "feat: harden MV refresh scheduler retry policy"
```

Expected: retry policy tests pass and all scheduler tests remain green.

### Final Verification

Run:

```bash
cargo fmt --check
cargo check --lib
cargo test --lib engine::mv_scheduler -- --nocapture
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_exposes_refresh_policy_metadata -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_refresh_policy_ddl_updates_show_metadata -- --nocapture
git diff --check HEAD~4..HEAD
```

Expected: all commands pass. Do not stage or revert the unrelated existing `src/sql/codegen/expr_compiler.rs` change.
