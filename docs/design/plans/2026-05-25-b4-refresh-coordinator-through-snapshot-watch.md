# B4 Refresh Coordinator Through Snapshot Watch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 B4-3 到 B4-6：在 standalone 模式中增加默认关闭的 MV refresh coordinator、手动入队执行、周期刷新策略，以及 Iceberg base snapshot watch 触发。

**Architecture:** 新增 `src/engine/mv_scheduler.rs`，把可测试的调度状态机、队列、候选扫描、snapshot watch 逻辑集中在一个模块里。生产路径通过 `StandaloneState`、`MvMetaRepository`、`mv_flow::refresh_mv` 和 Iceberg catalog current snapshot 读取进行接线；测试主要用 fake clock/executor/snapshot source 驱动纯 Rust 逻辑。

**Tech Stack:** Rust, Tokio runtime for optional background worker, existing metadata repository, existing MV refresh flow, `cargo test`.

---

### Task 1: B4-3 Refresh Coordinator Skeleton

**Files:**
- Create: `src/engine/mv_scheduler.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/common/app_config.rs`

- [ ] **Step 1: Write failing tests**

Add unit tests in `src/engine/mv_scheduler.rs`:

```rust
#[test]
fn disabled_coordinator_handle_does_not_start_worker() {
    let handle = RefreshCoordinatorHandle::disabled();
    assert!(!handle.is_enabled());
}

#[test]
fn scan_candidates_skips_manual_and_paused_mvs() {
    let now_ms = 1_000;
    let manual = test_definition(1, StoredMvRefreshPolicy::Manual);
    let mut paused = test_definition(2, StoredMvRefreshPolicy::AsyncOnChange);
    paused.refresh_paused = true;
    let async_mv = test_definition(3, StoredMvRefreshPolicy::AsyncOnChange);

    let candidates = scan_refresh_candidates(&[manual, paused, async_mv], now_ms);

    assert_eq!(candidates, vec![RefreshCandidate {
        mv_id: 3,
        policy: StoredMvRefreshPolicy::AsyncOnChange,
        state: RefreshTaskState::Pending,
    }]);
}
```

Run: `cargo test --lib engine::mv_scheduler::tests::disabled_coordinator_handle_does_not_start_worker engine::mv_scheduler::tests::scan_candidates_skips_manual_and_paused_mvs -- --nocapture`

Expected: fail because `mv_scheduler` does not exist.

- [ ] **Step 2: Minimal implementation**

Implement:
- `RefreshCoordinatorConfig` with defaults: `enabled=false`, conservative tick, `max_concurrent_refreshes=1`, failure backoff.
- `RefreshTaskState` enum: `Pending`, `Running`, `Succeeded`, `FailedBackoff`, `BlockedRecovery`, `Paused`.
- `RefreshCandidate`.
- `RefreshCoordinatorHandle::disabled()` and `is_enabled()`.
- `scan_refresh_candidates(definitions, now_ms)`.
- `start_refresh_coordinator_for_server(engine, config)` as a no-op handle when disabled.

Add hidden/test-oriented config fields under `[standalone_server]`:
- `mv_refresh_scheduler_enabled`
- `mv_refresh_scheduler_interval_ms`
- `mv_refresh_scheduler_max_concurrent`
- `mv_refresh_scheduler_failure_backoff_ms`

Default all fields to current behavior: disabled and no user-visible scheduling.

- [ ] **Step 3: Verify and commit**

Run:

```bash
cargo test --lib engine::mv_scheduler::tests::disabled_coordinator_handle_does_not_start_worker -- --nocapture
cargo test --lib engine::mv_scheduler::tests::scan_candidates_skips_manual_and_paused_mvs -- --nocapture
cargo fmt --check
git add docs/design/plans/2026-05-25-b4-refresh-coordinator-through-snapshot-watch.md src/engine/mv_scheduler.rs src/engine/mod.rs src/server/mod.rs src/common/app_config.rs
git commit -m "feat: add MV refresh coordinator skeleton"
```

Expected: tests pass; scheduler remains disabled by default.

### Task 2: B4-4 Manual Queue Execution Path

**Files:**
- Modify: `src/engine/mv_scheduler.rs`

- [ ] **Step 1: Write failing tests**

Add tests:

```rust
#[test]
fn enqueue_refresh_deduplicates_same_mv_until_drained() {
    let mut coordinator = RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
    assert!(coordinator.enqueue_refresh(7, RefreshTaskReason::Manual));
    assert!(!coordinator.enqueue_refresh(7, RefreshTaskReason::Manual));
    assert_eq!(coordinator.pending_len(), 1);
}

#[test]
fn drain_once_executes_manual_refresh_and_records_success() {
    let mut coordinator = RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
    coordinator.enqueue_refresh(7, RefreshTaskReason::Manual);
    let mut executor = RecordingRefreshExecutor::default();

    coordinator.drain_ready_for_test(&mut executor, 1_000).unwrap();

    assert_eq!(executor.executed_mv_ids(), vec![7]);
    assert_eq!(coordinator.state_for_mv(7), Some(RefreshTaskState::Succeeded));
}

#[test]
fn drain_once_records_failure_backoff() {
    let mut coordinator = RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
    coordinator.enqueue_refresh(7, RefreshTaskReason::Manual);
    let mut executor = RecordingRefreshExecutor::failing("refresh failed");

    coordinator.drain_ready_for_test(&mut executor, 1_000).unwrap();

    assert_eq!(coordinator.state_for_mv(7), Some(RefreshTaskState::FailedBackoff));
}
```

Expected: fail because queue and executor are missing.

- [ ] **Step 2: Minimal implementation**

Implement:
- `RefreshTaskReason` enum: `Manual`, `Periodic`, `SnapshotChange`.
- `RefreshCoordinator` queue and `in_queue`/`running` dedupe sets.
- `RefreshExecutor` trait with `execute_refresh(mv_id) -> Result<(), String>`.
- `drain_ready` honoring `max_concurrent_refreshes` and per-MV serial execution.
- Production executor that maps `StoredMvDefinition` to `RefreshMaterializedViewStmt` and calls `mv_flow::refresh_mv`.

The production executor must fail fast when it cannot resolve an MV target name from metadata.

- [ ] **Step 3: Verify and commit**

Run:

```bash
cargo test --lib engine::mv_scheduler::tests::enqueue_refresh_deduplicates_same_mv_until_drained -- --nocapture
cargo test --lib engine::mv_scheduler::tests::drain_once_executes_manual_refresh_and_records_success -- --nocapture
cargo test --lib engine::mv_scheduler::tests::drain_once_records_failure_backoff -- --nocapture
cargo fmt --check
git add src/engine/mv_scheduler.rs src/engine/mod.rs
git commit -m "feat: add MV refresh queue execution"
```

Expected: tests pass; no background refresh happens unless config enables the coordinator.

### Task 3: B4-5 Periodic Refresh Policy

**Files:**
- Modify: `src/engine/mv_scheduler.rs`

- [ ] **Step 1: Write failing tests**

Add tests:

```rust
#[test]
fn periodic_policy_enqueues_only_when_due() {
    let mut due = test_definition(1, StoredMvRefreshPolicy::AsyncInterval);
    due.refresh_interval_ms = Some(10_000);
    due.next_refresh_after_ms = Some(1_000);
    let mut future = test_definition(2, StoredMvRefreshPolicy::AsyncInterval);
    future.refresh_interval_ms = Some(10_000);
    future.next_refresh_after_ms = Some(2_000);

    let decisions = plan_periodic_refreshes(&[due, future], 1_500);

    assert_eq!(decisions.into_iter().map(|d| d.mv_id).collect::<Vec<_>>(), vec![1]);
}

#[test]
fn periodic_success_sets_next_refresh_after() {
    let mut definition = test_definition(1, StoredMvRefreshPolicy::AsyncInterval);
    definition.refresh_interval_ms = Some(10_000);

    let req = metadata_update_after_success(&definition, 1_500).unwrap();

    assert_eq!(req.last_scheduler_error, None);
    assert_eq!(req.next_refresh_after_ms, Some(11_500));
}

#[test]
fn periodic_failure_sets_backoff_and_preserves_policy() {
    let definition = test_definition(1, StoredMvRefreshPolicy::AsyncInterval);

    let req = metadata_update_after_failure(&definition, "boom", 1_500, 30_000);

    assert_eq!(req.last_scheduler_error, Some("boom".to_string()));
    assert_eq!(req.next_refresh_after_ms, Some(31_500));
    assert_eq!(req.refresh_policy, StoredMvRefreshPolicy::AsyncInterval);
}
```

Expected: fail because periodic planning and metadata helpers are missing.

- [ ] **Step 2: Minimal implementation**

Implement:
- `plan_periodic_refreshes(definitions, now_ms)`.
- Success/failure metadata update helpers.
- Coordinator tick that scans metadata, enqueues due interval MVs, executes, and writes `next_refresh_after_ms` / `last_scheduler_error`.
- Backoff behavior that avoids tight loops after failures.

- [ ] **Step 3: Verify and commit**

Run:

```bash
cargo test --lib engine::mv_scheduler::tests::periodic_policy_enqueues_only_when_due -- --nocapture
cargo test --lib engine::mv_scheduler::tests::periodic_success_sets_next_refresh_after -- --nocapture
cargo test --lib engine::mv_scheduler::tests::periodic_failure_sets_backoff_and_preserves_policy -- --nocapture
cargo fmt --check
git add src/engine/mv_scheduler.rs
git commit -m "feat: schedule periodic MV refresh policies"
```

Expected: due periodic MVs enqueue once, success advances next time, failure enters backoff.

### Task 4: B4-6 Iceberg Snapshot Watch Trigger

**Files:**
- Modify: `src/engine/mv_scheduler.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh.rs` if a small current-snapshot helper needs to be exposed.

- [ ] **Step 1: Write failing tests**

Add tests:

```rust
#[test]
fn snapshot_watch_does_not_enqueue_when_snapshot_is_unchanged() {
    let mut coordinator = RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
    coordinator.observe_snapshot_for_test(7, "ice.ns.tbl", 100);
    let definition = async_on_change_definition(7, vec!["ice.ns.tbl"]);
    let mut source = FakeSnapshotSource::new([("ice.ns.tbl", Ok(Some(100)))]);

    coordinator.poll_snapshot_watch_for_test(&[definition], &mut source, 1_000).unwrap();

    assert_eq!(coordinator.pending_len(), 0);
}

#[test]
fn snapshot_watch_enqueues_once_when_snapshot_advances() {
    let mut coordinator = RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
    coordinator.observe_snapshot_for_test(7, "ice.ns.tbl", 100);
    let definition = async_on_change_definition(7, vec!["ice.ns.tbl"]);
    let mut source = FakeSnapshotSource::new([("ice.ns.tbl", Ok(Some(101)))]);

    coordinator.poll_snapshot_watch_for_test(&[definition], &mut source, 1_000).unwrap();

    assert_eq!(coordinator.pending_mv_ids_for_test(), vec![7]);
    assert!(!coordinator.enqueue_refresh(7, RefreshTaskReason::SnapshotChange));
}

#[test]
fn snapshot_watch_records_error_without_overwriting_known_snapshot() {
    let mut coordinator = RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
    coordinator.observe_snapshot_for_test(7, "ice.ns.tbl", 100);
    let definition = async_on_change_definition(7, vec!["ice.ns.tbl"]);
    let mut source = FakeSnapshotSource::new([("ice.ns.tbl", Err("catalog unavailable"))]);

    coordinator.poll_snapshot_watch_for_test(&[definition], &mut source, 1_000).unwrap();

    assert_eq!(coordinator.observed_snapshot_for_test(7, "ice.ns.tbl"), Some(100));
    assert_eq!(coordinator.state_for_mv(7), Some(RefreshTaskState::FailedBackoff));
}
```

Expected: fail because snapshot watch does not exist.

- [ ] **Step 2: Minimal implementation**

Implement:
- `SnapshotSource` trait with production implementation using Iceberg catalog cache invalidation and current snapshot load.
- Observed map keyed by `(mv_id, base_table_fqn)`.
- Snapshot watch polling for `StoredMvRefreshPolicy::AsyncOnChange` only.
- Behavior:
  - no current snapshot: update observed state only if there was no known snapshot, do not enqueue.
  - unchanged snapshot: no enqueue.
  - advanced snapshot: enqueue once and update observed snapshot.
  - catalog error: keep last observed snapshot, record scheduler error/backoff.

- [ ] **Step 3: Verify and commit**

Run:

```bash
cargo test --lib engine::mv_scheduler::tests::snapshot_watch_does_not_enqueue_when_snapshot_is_unchanged -- --nocapture
cargo test --lib engine::mv_scheduler::tests::snapshot_watch_enqueues_once_when_snapshot_advances -- --nocapture
cargo test --lib engine::mv_scheduler::tests::snapshot_watch_records_error_without_overwriting_known_snapshot -- --nocapture
cargo fmt --check
git add src/engine/mv_scheduler.rs src/connector/starrocks/managed/mv_refresh.rs
git commit -m "feat: add Iceberg MV snapshot watch trigger"
```

Expected: watch trigger passes unchanged, advanced, multi-base, and error/backoff behavior.

### Final Verification

Run:

```bash
cargo fmt --check
cargo check --lib
cargo test --lib engine::mv_scheduler -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_refresh_policy_ddl_updates_show_metadata -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
git diff --check origin/main..HEAD
```

Expected: all listed commands pass. Existing unrelated dirty changes in `src/sql/codegen/expr_compiler.rs` must not be staged or reverted.
