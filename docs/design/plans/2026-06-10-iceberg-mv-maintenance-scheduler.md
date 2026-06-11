# IV3-11 Iceberg MV 自动 Maintenance 调度器 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 NovaRocks 自产的 Iceberg MV 存储表实现自动 maintenance(EXPIRE SNAPSHOTS / OPTIMIZE / DV compaction):独立 MaintenanceCoordinator 线程 + refresh 完成事件 + 兜底 tick + 零 IO 纯函数策略引擎。

**Architecture:** 新模块 `src/engine/mv_maintenance/`(mod.rs 协调器与执行器、policy.rs 纯策略、stats.rs 统计采集、tests.rs 集成测试),骨架照抄 `src/engine/mv_scheduler.rs` 的 RefreshCoordinator 模式(独立线程、mpsc 信号、now_ms 注入、trait 注入执行器)。EXPIRE / DV compaction 直接 await 现有 `run_*` commit-action;OPTIMIZE 复用现有 SQLite job 队列与 optimize worker。

**Tech Stack:** Rust;vendored iceberg-rust 0.9.0(`vendor/iceberg-0.9.0`);std::sync::mpsc;SQLite metadata provider;`block_on_iceberg` 桥接 async。

**Spec:** `docs/design/specs/2026-06-10-iceberg-mv-maintenance-scheduler-design.md`

**全局约定(每个 Task 都适用):**
- 代码注释/日志/错误信息/commit message 一律英文。
- 单测命令统一形如 `cargo test --lib <模块路径过滤>`,期望输出含 `test result: ok`。
- 不准给 commit message 加 `Co-Authored-By` trailer。
- 所有新代码 `cargo fmt` 后再 commit。

---

### Task 1: 前置验证测试 — replace snapshot 被下游增量刷新吸收

现状调研结论:`src/connector/iceberg/changes.rs:462` 的 `classify_snapshot` 已对 `Operation::Replace` 验证后吸收。本任务写端到端测试确认 `ALTER TABLE ... OPTIMIZE` 的真实产物能被增量 MV 刷新跨越。**预期直接 GREEN(验证现状)。若 RED:停止本计划,向用户报告阻塞依赖,先修 changes.rs 链路。**

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`(在文件末尾既有 `#[cfg(test)] mod tests` 内追加,helper 均已存在于该模块)

- [ ] **Step 1: 写测试**

在 `src/engine/mv/iceberg_refresh.rs` 的测试模块中(`open_test_state_with_hadoop_iceberg_catalog` 定义之后的任意位置)追加:

```rust
    #[test]
    fn incremental_refresh_absorbs_optimize_replace_snapshot() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(1, "east", 10)]);

        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, count(*) AS c, sum(amount) AS s
                FROM ice.sales.fact GROUP BY region",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create incremental MV");
        execute_iceberg_sql(
            &env.state,
            Some("ice"),
            &env.current_db,
            "REFRESH MATERIALIZED VIEW mv_fact",
        );

        // Second append snapshot on the base, then compact the base table.
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(2, "west", 5)]);
        execute_iceberg_sql(
            &env.state,
            Some("ice"),
            &env.current_db,
            "ALTER TABLE ice.sales.fact OPTIMIZE",
        );
        // The optimize worker thread is not spawned under cfg(test); drive the
        // pending job synchronously so the base table gains a REPLACE snapshot.
        crate::connector::iceberg::compact::run_optimize_jobs_once(&env.state)
            .expect("run optimize job");

        // Third append after the replace snapshot, then refresh incrementally.
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(3, "east", 7)]);
        execute_iceberg_sql(
            &env.state,
            Some("ice"),
            &env.current_db,
            "REFRESH MATERIALIZED VIEW mv_fact",
        );

        // Lineage walk previous -> current crossed the REPLACE snapshot; rows
        // must reflect all three appends.
        assert_aggregate_region_rows(
            &env.state,
            "ice",
            &env.current_db,
            "mv_fact",
            &[("east", 2, 17), ("west", 1, 5)],
        );
    }
```

实现提示:`assert_aggregate_region_rows` 已存在(签名 `(state, current_catalog, current_database, mv_name, expected: &[(&str, i64, i64)])`,查询 `SELECT region, c, s FROM {mv} ORDER BY region`)。若 `run_optimize_jobs_once` 对模块不可见,确认其声明为 `pub(crate) fn run_optimize_jobs_once(state: &Arc<StandaloneState>) -> Result<(), String>`(`src/connector/iceberg/compact.rs:132`,已是 pub(crate),无需改动)。

- [ ] **Step 2: 跑测试,确认 GREEN**

Run: `cargo test --lib engine::mv::iceberg_refresh::tests::incremental_refresh_absorbs_optimize_replace_snapshot`
Expected: PASS。若 FAIL:**停止计划**,把失败输出原样报告给用户(这是 spec §9.1 的阻塞依赖)。

- [ ] **Step 3: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "test(mv): verify incremental refresh absorbs OPTIMIZE replace snapshots"
```

---

### Task 2: 配置面 — StandaloneServerConfig 新增 7 个 iceberg_maintenance 字段

**Files:**
- Modify: `src/common/app_config.rs:914-980` 附近(`StandaloneServerConfig` 定义、default 工厂函数、`Default` impl)

- [ ] **Step 1: 写失败测试**

在 `src/common/app_config.rs` 文件末尾既有 `#[cfg(test)]` 测试模块(`:1333` 起)内追加:

```rust
    #[test]
    fn standalone_server_config_iceberg_maintenance_defaults() {
        let cfg: StandaloneServerConfig =
            toml::from_str("").expect("empty standalone_server section parses");
        assert!(cfg.iceberg_maintenance_enabled);
        assert_eq!(cfg.iceberg_maintenance_tick_interval_ms, 600_000);
        assert_eq!(cfg.iceberg_maintenance_max_concurrent, 1);
        assert_eq!(cfg.iceberg_maintenance_compaction_min_data_files, 100);
        assert_eq!(cfg.iceberg_maintenance_dv_min_delete_files, 10);
        assert_eq!(cfg.iceberg_maintenance_action_cooldown_ms, 3_600_000);
        assert_eq!(cfg.iceberg_maintenance_max_consecutive_failures, 4);
        assert_eq!(cfg, StandaloneServerConfig::default());
    }
```

- [ ] **Step 2: 跑测试确认 FAIL**

Run: `cargo test --lib common::app_config -- iceberg_maintenance`
Expected: 编译错误(字段不存在)。

- [ ] **Step 3: 实现**

在 `StandaloneServerConfig` 结构体的 `mv_refresh_scheduler_max_failure_backoff_ms` 字段之后追加:

```rust
    #[serde(default = "default_standalone_iceberg_maintenance_enabled")]
    pub iceberg_maintenance_enabled: bool,
    #[serde(default = "default_standalone_iceberg_maintenance_tick_interval_ms")]
    pub iceberg_maintenance_tick_interval_ms: u64,
    #[serde(default = "default_standalone_iceberg_maintenance_max_concurrent")]
    pub iceberg_maintenance_max_concurrent: usize,
    #[serde(default = "default_standalone_iceberg_maintenance_compaction_min_data_files")]
    pub iceberg_maintenance_compaction_min_data_files: u64,
    #[serde(default = "default_standalone_iceberg_maintenance_dv_min_delete_files")]
    pub iceberg_maintenance_dv_min_delete_files: u64,
    #[serde(default = "default_standalone_iceberg_maintenance_action_cooldown_ms")]
    pub iceberg_maintenance_action_cooldown_ms: i64,
    #[serde(default = "default_standalone_iceberg_maintenance_max_consecutive_failures")]
    pub iceberg_maintenance_max_consecutive_failures: u32,
```

default 工厂函数(紧跟既有 `default_standalone_mv_refresh_scheduler_*` 函数之后):

```rust
fn default_standalone_iceberg_maintenance_enabled() -> bool {
    true
}

fn default_standalone_iceberg_maintenance_tick_interval_ms() -> u64 {
    600_000
}

fn default_standalone_iceberg_maintenance_max_concurrent() -> usize {
    1
}

fn default_standalone_iceberg_maintenance_compaction_min_data_files() -> u64 {
    100
}

fn default_standalone_iceberg_maintenance_dv_min_delete_files() -> u64 {
    10
}

fn default_standalone_iceberg_maintenance_action_cooldown_ms() -> i64 {
    3_600_000
}

fn default_standalone_iceberg_maintenance_max_consecutive_failures() -> u32 {
    4
}
```

`impl Default for StandaloneServerConfig` 中对应追加(在 `mv_refresh_scheduler_max_failure_backoff_ms` 行之后):

```rust
            iceberg_maintenance_enabled: default_standalone_iceberg_maintenance_enabled(),
            iceberg_maintenance_tick_interval_ms:
                default_standalone_iceberg_maintenance_tick_interval_ms(),
            iceberg_maintenance_max_concurrent:
                default_standalone_iceberg_maintenance_max_concurrent(),
            iceberg_maintenance_compaction_min_data_files:
                default_standalone_iceberg_maintenance_compaction_min_data_files(),
            iceberg_maintenance_dv_min_delete_files:
                default_standalone_iceberg_maintenance_dv_min_delete_files(),
            iceberg_maintenance_action_cooldown_ms:
                default_standalone_iceberg_maintenance_action_cooldown_ms(),
            iceberg_maintenance_max_consecutive_failures:
                default_standalone_iceberg_maintenance_max_consecutive_failures(),
```

- [ ] **Step 4: 跑测试确认 PASS**

Run: `cargo test --lib common::app_config`
Expected: 全部 PASS(含既有测试)。

- [ ] **Step 5: Commit**

```bash
git add src/common/app_config.rs
git commit -m "feat(config): add [standalone_server] iceberg_maintenance_* settings"
```

---

### Task 3: 属性白名单 — 放行 `novarocks.maintenance.enabled`

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs:3885-3910`(`is_reserved_property_key`)及该文件测试模块(`:2016` 附近的 reserved_key 测试群)

- [ ] **Step 1: 写失败测试**

在 `reserved_key_allows_iceberg_write_props` 测试(schema_update.rs:2071)旁追加:

```rust
    #[test]
    fn reserved_key_allows_maintenance_enabled_escape_hatch() {
        // The per-table auto-maintenance escape hatch must be user-settable
        // even though the rest of the novarocks.* namespace stays reserved.
        assert!(is_reserved_property_key("novarocks.maintenance.enabled").is_none());
        assert!(is_reserved_property_key("novarocks.maintenance.future-knob").is_some());
    }
```

- [ ] **Step 2: 跑测试确认 FAIL**

Run: `cargo test --lib connector::iceberg::catalog::schema_update -- reserved_key_allows_maintenance`
Expected: FAIL(第一个 assert 失败,目前整个 `novarocks.*` 被拒绝)。

- [ ] **Step 3: 实现**

在 `is_reserved_property_key` 中 `if key.starts_with("novarocks.")` 之前插入:

```rust
    // Escape hatch for automatic table maintenance (IV3-11). This single key
    // is intentionally user-settable; everything else under novarocks.* stays
    // engine-owned.
    if key == "novarocks.maintenance.enabled" {
        return None;
    }
```

- [ ] **Step 4: 跑测试确认 PASS**

Run: `cargo test --lib connector::iceberg::catalog::schema_update -- reserved_key`
Expected: 全部 PASS(含既有 reserved_key 测试)。

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): allow novarocks.maintenance.enabled table property"
```

---

### Task 4: policy.rs — 零 IO 策略引擎(本计划的单测主力)

**Files:**
- Create: `src/engine/mv_maintenance/policy.rs`
- Create: `src/engine/mv_maintenance/mod.rs`(本任务仅放 mod 声明,内容后续任务补)
- Modify: `src/engine/mod.rs:49` 附近(追加 `pub(crate) mod mv_maintenance;`)

- [ ] **Step 1: 建模块骨架**

`src/engine/mv_maintenance/mod.rs`:

```rust
//! Automatic maintenance for NovaRocks-owned Iceberg MV storage tables
//! (IV3-11): EXPIRE SNAPSHOTS / OPTIMIZE / DV compaction, driven by a
//! background coordinator. See
//! docs/design/specs/2026-06-10-iceberg-mv-maintenance-scheduler-design.md.

pub(crate) mod policy;
```

`src/engine/mod.rs` 在 `pub(crate) mod mv_scheduler;` 之后追加:

```rust
pub(crate) mod mv_maintenance;
```

- [ ] **Step 2: 写 policy.rs 的类型与失败测试**

`src/engine/mv_maintenance/policy.rs` 全文(先只写类型 + 测试,`evaluate_table` 等函数体留 `todo!()` 让测试编译失败转运行失败也可,推荐直接一次写完到 Step 4 的实现再跑;严格 TDD 者可先 `todo!()`):

类型部分:

```rust
//! Pure decision logic for automatic Iceberg MV maintenance. No IO: every
//! input is collected by stats.rs / the coordinator and passed in by value,
//! which keeps the whole policy table-test friendly.

use std::collections::{BTreeMap, BTreeSet, HashMap};

pub(crate) const DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS: i64 = 432_000_000; // 5 days (Iceberg default)
pub(crate) const DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: u32 = 1;
pub(crate) const DEFAULT_TARGET_FILE_SIZE_BYTES: u64 = 536_870_912; // 512 MiB (Iceberg default)
/// avg file size < 3/4 of target counts as "small files dominate".
const SMALL_FILE_RATIO_NUM: u64 = 3;
const SMALL_FILE_RATIO_DEN: u64 = 4;
/// Failure backoff is fixed (not config-exposed in v1), matching the spec.
pub(crate) const FAILURE_BACKOFF_BASE_MS: i64 = 60_000;
pub(crate) const FAILURE_BACKOFF_MAX_MS: i64 = 1_800_000;

pub(crate) const MAINTENANCE_ENABLED_PROPERTY: &str = "novarocks.maintenance.enabled";
pub(crate) const EXPIRE_MAX_AGE_PROPERTY: &str = "history.expire.max-snapshot-age-ms";
pub(crate) const EXPIRE_MIN_KEEP_PROPERTY: &str = "history.expire.min-snapshots-to-keep";
pub(crate) const TARGET_FILE_SIZE_PROPERTY: &str = "write.target-file-size-bytes";

/// Global thresholds resolved from `[standalone_server]` config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenancePolicyConfig {
    pub(crate) compaction_min_data_files: u64,
    pub(crate) dv_min_delete_files: u64,
    pub(crate) action_cooldown_ms: i64,
    pub(crate) max_consecutive_failures: u32,
}

impl Default for MaintenancePolicyConfig {
    fn default() -> Self {
        Self {
            compaction_min_data_files: 100,
            dv_min_delete_files: 10,
            action_cooldown_ms: 3_600_000,
            max_consecutive_failures: 4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotInfo {
    pub(crate) snapshot_id: i64,
    pub(crate) timestamp_ms: i64,
}

/// Raw facts about one MV storage table, collected from a single metadata load.
#[derive(Clone, Debug, Default)]
pub(crate) struct TableMaintenanceStats {
    pub(crate) current_snapshot_id: Option<i64>,
    pub(crate) snapshots: Vec<SnapshotInfo>,
    pub(crate) total_data_files: Option<u64>,
    pub(crate) total_files_size_bytes: Option<u64>,
    pub(crate) total_delete_files: Option<u64>,
    pub(crate) properties: HashMap<String, String>,
    pub(crate) non_main_ref_count: usize,
    /// min over downstream incremental consumers of the timestamp of their
    /// last-consumed snapshot of this table. None = no downstream consumers.
    pub(crate) downstream_floor_ts_ms: Option<i64>,
    /// true when a downstream consumer references a snapshot we could not
    /// resolve in this table's metadata; expire must then be skipped.
    pub(crate) downstream_floor_unknown: bool,
}

/// Per-table policy: global defaults overridden by table properties.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TablePolicy {
    pub(crate) enabled: bool,
    pub(crate) expire_max_age_ms: i64,
    pub(crate) expire_min_keep: u32,
    pub(crate) target_file_size_bytes: u64,
    pub(crate) compaction_min_data_files: u64,
    pub(crate) dv_min_delete_files: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ActionKind {
    Expire,
    RewriteDv,
    Optimize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceAction {
    ExpireSnapshots { older_than_ms: i64, retain_last: u32 },
    RewritePositionDeletes { min_input_files: usize },
    SubmitOptimize,
}

impl MaintenanceAction {
    pub(crate) fn kind(&self) -> ActionKind {
        match self {
            Self::ExpireSnapshots { .. } => ActionKind::Expire,
            Self::RewritePositionDeletes { .. } => ActionKind::RewriteDv,
            Self::SubmitOptimize => ActionKind::Optimize,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SkipReason {
    Disabled,
    NonMainRefs,
    DownstreamFloorUnknown,
    NothingToExpire,
    SnapshotUnchanged,
    MissingSummaryStats,
    BelowThreshold,
    SuppressedByOptimize,
    Cooldown,
    FailureBackoff,
    CircuitBroken,
}

/// Coordinator-memory state for one table; lost on restart by design (all
/// actions are idempotent re-evaluations of current metadata).
#[derive(Clone, Debug, Default)]
pub(crate) struct TableRuntimeState {
    pub(crate) last_seen_snapshot_id: Option<i64>,
    pub(crate) last_action_ms: BTreeMap<ActionKind, i64>,
    pub(crate) consecutive_failures: BTreeMap<ActionKind, u32>,
    pub(crate) next_attempt_after_ms: BTreeMap<ActionKind, i64>,
    pub(crate) circuit_broken: BTreeSet<ActionKind>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct EvaluationOutcome {
    pub(crate) actions: Vec<MaintenanceAction>,
    pub(crate) skips: Vec<(ActionKind, SkipReason)>,
}
```

函数部分(实现见 Step 4,TDD 时先写测试):

```rust
impl TablePolicy {
    pub(crate) fn resolve(
        global: &MaintenancePolicyConfig,
        properties: &HashMap<String, String>,
    ) -> Self { ... }
}

pub(crate) fn evaluate_table(
    stats: &TableMaintenanceStats,
    policy: &TablePolicy,
    runtime: &TableRuntimeState,
    global: &MaintenancePolicyConfig,
    now_ms: i64,
) -> EvaluationOutcome { ... }

pub(crate) fn failure_backoff_ms(attempt: u32) -> i64 { ... }
```

- [ ] **Step 3: 写单测(同文件 `#[cfg(test)] mod tests`)**

测试清单(每条一个 `#[test]`,工厂函数 `fn stats(...)`/`fn policy(...)` 自己写小 builder):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn base_stats() -> TableMaintenanceStats {
        TableMaintenanceStats {
            current_snapshot_id: Some(30),
            snapshots: vec![
                SnapshotInfo { snapshot_id: 10, timestamp_ms: 1_000 },
                SnapshotInfo { snapshot_id: 20, timestamp_ms: 2_000 },
                SnapshotInfo { snapshot_id: 30, timestamp_ms: 3_000 },
            ],
            total_data_files: Some(200),
            total_files_size_bytes: Some(200 * 1024 * 1024), // avg 1 MiB << 384 MiB
            total_delete_files: Some(0),
            properties: HashMap::new(),
            non_main_ref_count: 0,
            downstream_floor_ts_ms: None,
            downstream_floor_unknown: false,
        }
    }

    fn enabled_policy() -> TablePolicy {
        TablePolicy::resolve(&MaintenancePolicyConfig::default(), &HashMap::new())
    }

    const NOW: i64 = 1_000_000_000;

    // --- TablePolicy::resolve ---
    #[test]
    fn resolve_uses_iceberg_defaults_without_properties() {
        let p = enabled_policy();
        assert!(p.enabled);
        assert_eq!(p.expire_max_age_ms, DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS);
        assert_eq!(p.expire_min_keep, DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP);
        assert_eq!(p.target_file_size_bytes, DEFAULT_TARGET_FILE_SIZE_BYTES);
        assert_eq!(p.compaction_min_data_files, 100);
        assert_eq!(p.dv_min_delete_files, 10);
    }

    #[test]
    fn resolve_honors_table_properties() {
        let mut props = HashMap::new();
        props.insert(EXPIRE_MAX_AGE_PROPERTY.to_string(), "1000".to_string());
        props.insert(EXPIRE_MIN_KEEP_PROPERTY.to_string(), "3".to_string());
        props.insert(TARGET_FILE_SIZE_PROPERTY.to_string(), "1048576".to_string());
        props.insert(MAINTENANCE_ENABLED_PROPERTY.to_string(), "false".to_string());
        let p = TablePolicy::resolve(&MaintenancePolicyConfig::default(), &props);
        assert!(!p.enabled);
        assert_eq!(p.expire_max_age_ms, 1000);
        assert_eq!(p.expire_min_keep, 3);
        assert_eq!(p.target_file_size_bytes, 1_048_576);
    }

    #[test]
    fn resolve_ignores_malformed_property_values() {
        let mut props = HashMap::new();
        props.insert(EXPIRE_MAX_AGE_PROPERTY.to_string(), "not-a-number".to_string());
        let p = TablePolicy::resolve(&MaintenancePolicyConfig::default(), &props);
        assert_eq!(p.expire_max_age_ms, DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS);
    }

    // --- disabled / guards ---
    #[test]
    fn disabled_table_skips_everything() {
        let mut props = HashMap::new();
        props.insert(MAINTENANCE_ENABLED_PROPERTY.to_string(), "false".to_string());
        let policy = TablePolicy::resolve(&MaintenancePolicyConfig::default(), &props);
        let out = evaluate_table(
            &base_stats(),
            &policy,
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.actions.is_empty());
        assert!(out.skips.iter().all(|(_, r)| *r == SkipReason::Disabled));
    }

    // --- expire ---
    #[test]
    fn expire_triggers_when_old_snapshots_exist() {
        // All snapshots are far older than NOW - 5d.
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        let expire = out
            .actions
            .iter()
            .find(|a| a.kind() == ActionKind::Expire)
            .expect("expire planned");
        match expire {
            MaintenanceAction::ExpireSnapshots { older_than_ms, retain_last } => {
                assert_eq!(*older_than_ms, NOW - DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS);
                assert_eq!(*retain_last, 1);
            }
            other => panic!("unexpected action {other:?}"),
        }
    }

    #[test]
    fn expire_skips_when_no_snapshot_is_old_enough() {
        let mut stats = base_stats();
        // Snapshots are "now"-ish, way inside the 5-day window.
        for (i, s) in stats.snapshots.iter_mut().enumerate() {
            s.timestamp_ms = NOW - i as i64;
        }
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.skips.contains(&(ActionKind::Expire, SkipReason::NothingToExpire)));
    }

    #[test]
    fn expire_cutoff_is_tightened_by_downstream_floor() {
        let mut stats = base_stats();
        stats.downstream_floor_ts_ms = Some(1_500); // protect snapshot 20 and newer
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        match out.actions.iter().find(|a| a.kind() == ActionKind::Expire) {
            Some(MaintenanceAction::ExpireSnapshots { older_than_ms, .. }) => {
                assert_eq!(*older_than_ms, 1_500);
            }
            other => panic!("expected tightened expire, got {other:?}"),
        }
    }

    #[test]
    fn expire_skips_when_downstream_floor_unknown() {
        let mut stats = base_stats();
        stats.downstream_floor_unknown = true;
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out
            .skips
            .contains(&(ActionKind::Expire, SkipReason::DownstreamFloorUnknown)));
    }

    #[test]
    fn expire_skips_when_non_main_refs_exist() {
        let mut stats = base_stats();
        stats.non_main_ref_count = 1;
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.skips.contains(&(ActionKind::Expire, SkipReason::NonMainRefs)));
    }

    #[test]
    fn expire_respects_min_snapshots_to_keep() {
        let mut stats = base_stats();
        stats.snapshots.truncate(1); // single snapshot, min_keep = 1
        stats.current_snapshot_id = Some(10);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.skips.contains(&(ActionKind::Expire, SkipReason::NothingToExpire)));
    }

    // --- optimize / dv ---
    #[test]
    fn optimize_triggers_on_many_small_files_and_suppresses_dv() {
        let mut stats = base_stats();
        stats.total_delete_files = Some(50); // DV would trigger alone
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.actions.contains(&MaintenanceAction::SubmitOptimize));
        assert!(out
            .skips
            .contains(&(ActionKind::RewriteDv, SkipReason::SuppressedByOptimize)));
    }

    #[test]
    fn optimize_skips_below_file_count_threshold() {
        let mut stats = base_stats();
        stats.total_data_files = Some(99);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.skips.contains(&(ActionKind::Optimize, SkipReason::BelowThreshold)));
    }

    #[test]
    fn optimize_skips_when_avg_file_size_is_large() {
        let mut stats = base_stats();
        // avg = 512 MiB == target: not small files.
        stats.total_files_size_bytes =
            Some(stats.total_data_files.unwrap() * DEFAULT_TARGET_FILE_SIZE_BYTES);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.skips.contains(&(ActionKind::Optimize, SkipReason::BelowThreshold)));
    }

    #[test]
    fn optimize_skips_when_summary_stats_missing() {
        let mut stats = base_stats();
        stats.total_files_size_bytes = None;
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out
            .skips
            .contains(&(ActionKind::Optimize, SkipReason::MissingSummaryStats)));
    }

    #[test]
    fn dv_triggers_on_delete_file_threshold_without_optimize() {
        let mut stats = base_stats();
        stats.total_data_files = Some(10); // below optimize threshold
        stats.total_delete_files = Some(10);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out
            .actions
            .contains(&MaintenanceAction::RewritePositionDeletes { min_input_files: 2 }));
    }

    #[test]
    fn compaction_signals_skip_when_snapshot_unchanged() {
        let mut runtime = TableRuntimeState::default();
        runtime.last_seen_snapshot_id = Some(30); // == current
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out
            .skips
            .contains(&(ActionKind::Optimize, SkipReason::SnapshotUnchanged)));
        // Expire is still evaluated (pure, from already-loaded metadata).
        assert!(out.actions.iter().any(|a| a.kind() == ActionKind::Expire));
    }

    // --- cooldown / backoff / circuit ---
    #[test]
    fn optimize_respects_cooldown() {
        let mut runtime = TableRuntimeState::default();
        runtime.last_action_ms.insert(ActionKind::Optimize, NOW - 1);
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.skips.contains(&(ActionKind::Optimize, SkipReason::Cooldown)));
    }

    #[test]
    fn expire_has_no_cooldown() {
        let mut runtime = TableRuntimeState::default();
        runtime.last_action_ms.insert(ActionKind::Expire, NOW - 1);
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.actions.iter().any(|a| a.kind() == ActionKind::Expire));
    }

    #[test]
    fn failure_backoff_defers_action() {
        let mut runtime = TableRuntimeState::default();
        runtime.next_attempt_after_ms.insert(ActionKind::Expire, NOW + 1);
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.skips.contains(&(ActionKind::Expire, SkipReason::FailureBackoff)));
    }

    #[test]
    fn circuit_breaker_blocks_action() {
        let mut runtime = TableRuntimeState::default();
        runtime.circuit_broken.insert(ActionKind::Expire);
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.skips.contains(&(ActionKind::Expire, SkipReason::CircuitBroken)));
    }

    #[test]
    fn failure_backoff_is_bounded_exponential() {
        assert_eq!(failure_backoff_ms(1), 60_000);
        assert_eq!(failure_backoff_ms(2), 120_000);
        assert_eq!(failure_backoff_ms(10), FAILURE_BACKOFF_MAX_MS);
    }
}
```

- [ ] **Step 4: 实现函数体**

```rust
impl TablePolicy {
    pub(crate) fn resolve(
        global: &MaintenancePolicyConfig,
        properties: &HashMap<String, String>,
    ) -> Self {
        fn parse_or<T: std::str::FromStr>(
            properties: &HashMap<String, String>,
            key: &str,
            default: T,
        ) -> T {
            properties
                .get(key)
                .and_then(|v| v.trim().parse::<T>().ok())
                .unwrap_or(default)
        }
        let enabled = properties
            .get(MAINTENANCE_ENABLED_PROPERTY)
            .map(|v| !v.trim().eq_ignore_ascii_case("false"))
            .unwrap_or(true);
        Self {
            enabled,
            expire_max_age_ms: parse_or(
                properties,
                EXPIRE_MAX_AGE_PROPERTY,
                DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS,
            ),
            expire_min_keep: parse_or(
                properties,
                EXPIRE_MIN_KEEP_PROPERTY,
                DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP,
            )
            .max(1),
            target_file_size_bytes: parse_or(
                properties,
                TARGET_FILE_SIZE_PROPERTY,
                DEFAULT_TARGET_FILE_SIZE_BYTES,
            )
            .max(1),
            compaction_min_data_files: global.compaction_min_data_files.max(1),
            dv_min_delete_files: global.dv_min_delete_files.max(1),
        }
    }
}

pub(crate) fn failure_backoff_ms(attempt: u32) -> i64 {
    let shift = attempt.max(1).saturating_sub(1).min(62);
    let multiplier = 1_i64.checked_shl(shift).unwrap_or(i64::MAX);
    FAILURE_BACKOFF_BASE_MS
        .saturating_mul(multiplier)
        .min(FAILURE_BACKOFF_MAX_MS)
}

/// Per-action admission guards shared by all three actions.
fn admission(
    kind: ActionKind,
    runtime: &TableRuntimeState,
    global: &MaintenancePolicyConfig,
    now_ms: i64,
) -> Result<(), SkipReason> {
    if runtime.circuit_broken.contains(&kind) {
        return Err(SkipReason::CircuitBroken);
    }
    if runtime
        .next_attempt_after_ms
        .get(&kind)
        .map(|next| *next > now_ms)
        .unwrap_or(false)
    {
        return Err(SkipReason::FailureBackoff);
    }
    // Cooldown applies to write-amplifying actions only; expire is naturally
    // rate-limited by candidate availability.
    if matches!(kind, ActionKind::Optimize | ActionKind::RewriteDv)
        && runtime
            .last_action_ms
            .get(&kind)
            .map(|last| last.saturating_add(global.action_cooldown_ms) > now_ms)
            .unwrap_or(false)
    {
        return Err(SkipReason::Cooldown);
    }
    Ok(())
}

fn plan_expire(
    stats: &TableMaintenanceStats,
    policy: &TablePolicy,
    now_ms: i64,
) -> Result<MaintenanceAction, SkipReason> {
    if stats.non_main_ref_count > 0 {
        return Err(SkipReason::NonMainRefs);
    }
    if stats.downstream_floor_unknown {
        return Err(SkipReason::DownstreamFloorUnknown);
    }
    let mut cutoff = now_ms.saturating_sub(policy.expire_max_age_ms);
    if let Some(floor) = stats.downstream_floor_ts_ms {
        cutoff = cutoff.min(floor);
    }
    if stats.snapshots.len() <= policy.expire_min_keep as usize {
        return Err(SkipReason::NothingToExpire);
    }
    let expirable = stats
        .snapshots
        .iter()
        .filter(|s| s.timestamp_ms < cutoff && Some(s.snapshot_id) != stats.current_snapshot_id)
        .count();
    if expirable == 0 {
        return Err(SkipReason::NothingToExpire);
    }
    Ok(MaintenanceAction::ExpireSnapshots {
        older_than_ms: cutoff,
        retain_last: policy.expire_min_keep,
    })
}

fn plan_optimize(
    stats: &TableMaintenanceStats,
    policy: &TablePolicy,
) -> Result<MaintenanceAction, SkipReason> {
    let (Some(files), Some(size)) = (stats.total_data_files, stats.total_files_size_bytes) else {
        return Err(SkipReason::MissingSummaryStats);
    };
    if files == 0 || files < policy.compaction_min_data_files {
        return Err(SkipReason::BelowThreshold);
    }
    let avg = size / files;
    // Trigger only when avg < (NUM/DEN) * target, i.e. small files dominate.
    if avg.saturating_mul(SMALL_FILE_RATIO_DEN)
        >= policy.target_file_size_bytes.saturating_mul(SMALL_FILE_RATIO_NUM)
    {
        return Err(SkipReason::BelowThreshold);
    }
    Ok(MaintenanceAction::SubmitOptimize)
}

fn plan_rewrite_dv(
    stats: &TableMaintenanceStats,
    policy: &TablePolicy,
) -> Result<MaintenanceAction, SkipReason> {
    let Some(delete_files) = stats.total_delete_files else {
        return Err(SkipReason::MissingSummaryStats);
    };
    if delete_files < policy.dv_min_delete_files {
        return Err(SkipReason::BelowThreshold);
    }
    Ok(MaintenanceAction::RewritePositionDeletes { min_input_files: 2 })
}

pub(crate) fn evaluate_table(
    stats: &TableMaintenanceStats,
    policy: &TablePolicy,
    runtime: &TableRuntimeState,
    global: &MaintenancePolicyConfig,
    now_ms: i64,
) -> EvaluationOutcome {
    let mut out = EvaluationOutcome::default();
    if !policy.enabled {
        for kind in [ActionKind::Expire, ActionKind::RewriteDv, ActionKind::Optimize] {
            out.skips.push((kind, SkipReason::Disabled));
        }
        return out;
    }

    // Expire: evaluated every pass (pure computation over loaded metadata).
    match admission(ActionKind::Expire, runtime, global, now_ms)
        .and_then(|()| plan_expire(stats, policy, now_ms))
    {
        Ok(action) => out.actions.push(action),
        Err(reason) => out.skips.push((ActionKind::Expire, reason)),
    }

    // Compaction signals only make sense when the table changed since the
    // last pass (Dremio-style short circuit).
    let snapshot_changed = stats.current_snapshot_id != runtime.last_seen_snapshot_id;
    if !snapshot_changed {
        out.skips.push((ActionKind::Optimize, SkipReason::SnapshotUnchanged));
        out.skips.push((ActionKind::RewriteDv, SkipReason::SnapshotUnchanged));
        return out;
    }

    let optimize = admission(ActionKind::Optimize, runtime, global, now_ms)
        .and_then(|()| plan_optimize(stats, policy));
    let optimize_planned = optimize.is_ok();
    match optimize {
        Ok(action) => out.actions.push(action),
        Err(reason) => out.skips.push((ActionKind::Optimize, reason)),
    }

    if optimize_planned {
        // Whole-table rewrite absorbs delete files; a DV pass would be wasted.
        out.skips.push((ActionKind::RewriteDv, SkipReason::SuppressedByOptimize));
    } else {
        match admission(ActionKind::RewriteDv, runtime, global, now_ms)
            .and_then(|()| plan_rewrite_dv(stats, policy))
        {
            Ok(action) => out.actions.push(action),
            Err(reason) => out.skips.push((ActionKind::RewriteDv, reason)),
        }
    }
    out
}
```

- [ ] **Step 5: 跑测试确认 PASS**

Run: `cargo test --lib engine::mv_maintenance::policy`
Expected: 全部 PASS(约 20 个测试)。

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv_maintenance/ src/engine/mod.rs
git commit -m "feat(mv-maintenance): pure policy engine for auto expire/optimize/dv"
```

---

### Task 5: stats.rs — 统计采集与下游安全下界

**Files:**
- Create: `src/engine/mv_maintenance/stats.rs`
- Modify: `src/engine/mv_maintenance/mod.rs`(追加 `pub(crate) mod stats;`)

- [ ] **Step 1: 写纯函数部分的失败测试**

`src/engine/mv_maintenance/stats.rs` 底部 `#[cfg(test)] mod tests`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::repository::mv::{StoredMvDefinition, StoredMvRefreshPolicy};
    use std::collections::BTreeMap;

    fn definition_with_consumed(fqn: &str, snapshot_id: i64) -> StoredMvDefinition {
        let mut last_refresh_snapshots = BTreeMap::new();
        last_refresh_snapshots.insert(fqn.to_string(), snapshot_id);
        StoredMvDefinition {
            mv_id: 1,
            select_sql: "SELECT 1".to_string(),
            base_table_refs: vec![fqn.to_string()],
            primary_key_columns: vec![],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("analytics".to_string()),
            target_table: Some("mv_x".to_string()),
            schema_contract: None,
            partition_spec: None,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots,
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            refresh_policy: StoredMvRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        }
    }

    #[test]
    fn floor_is_min_consumed_snapshot_timestamp() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        ts_by_id.insert(20, 2_000);
        let defs = vec![
            definition_with_consumed("ice.sales.t", 20),
            definition_with_consumed("ice.sales.t", 10),
        ];
        let floor = downstream_floor(&defs, "ice.sales.t", &ts_by_id);
        assert_eq!(floor, DownstreamFloor { floor_ts_ms: Some(1_000), unknown: false });
    }

    #[test]
    fn floor_is_none_without_consumers() {
        let defs = vec![definition_with_consumed("ice.sales.other", 10)];
        let floor = downstream_floor(&defs, "ice.sales.t", &BTreeMap::new());
        assert_eq!(floor, DownstreamFloor { floor_ts_ms: None, unknown: false });
    }

    #[test]
    fn floor_unknown_when_consumed_snapshot_missing_from_metadata() {
        let defs = vec![definition_with_consumed("ice.sales.t", 99)];
        let floor = downstream_floor(&defs, "ice.sales.t", &BTreeMap::new());
        assert!(floor.unknown);
    }

    #[test]
    fn floor_considers_in_progress_refresh_pins() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        let mut def = definition_with_consumed("ice.sales.other", 1);
        def.refresh_target_snapshots
            .insert("ice.sales.t".to_string(), 10);
        let floor = downstream_floor(&[def], "ice.sales.t", &ts_by_id);
        assert_eq!(floor.floor_ts_ms, Some(1_000));
    }

    #[test]
    fn summary_u64_parses_and_rejects() {
        let mut props = std::collections::HashMap::new();
        props.insert("total-data-files".to_string(), "42".to_string());
        props.insert("bad".to_string(), "x".to_string());
        assert_eq!(summary_u64(&props, "total-data-files"), Some(42));
        assert_eq!(summary_u64(&props, "bad"), None);
        assert_eq!(summary_u64(&props, "absent"), None);
    }
}
```

注意:`StoredMvDefinition` 字段以 `src/meta/repository/mv.rs:23-59` 为准逐字段构造;若编译器报缺字段/多字段,按实际定义增删(该结构体有 `#[serde(default)]` 字段,但 Rust 构造仍需全量)。

- [ ] **Step 2: 跑测试确认 FAIL(符号不存在)**

Run: `cargo test --lib engine::mv_maintenance::stats`
Expected: 编译失败。

- [ ] **Step 3: 实现 stats.rs**

```rust
//! Collects per-table maintenance facts from a single Iceberg metadata load:
//! snapshot list, current-snapshot summary counters, table properties, refs,
//! and the downstream-consumer floor that protects incremental MV lineage.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::meta::repository::mv::StoredMvDefinition;

use super::policy::{SnapshotInfo, TableMaintenanceStats};

/// Iceberg snapshot-summary keys (string literals: the constants in
/// vendor/iceberg-0.9.0/src/spec/snapshot_summary.rs are private).
const TOTAL_DATA_FILES_KEY: &str = "total-data-files";
const TOTAL_DELETE_FILES_KEY: &str = "total-delete-files";
const TOTAL_FILES_SIZE_KEY: &str = "total-files-size";
const MAIN_BRANCH: &str = "main";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DownstreamFloor {
    pub(crate) floor_ts_ms: Option<i64>,
    pub(crate) unknown: bool,
}

pub(crate) fn summary_u64(props: &HashMap<String, String>, key: &str) -> Option<u64> {
    props.get(key).and_then(|v| v.trim().parse::<u64>().ok())
}

/// Minimum consumed-snapshot timestamp across all MV definitions that read
/// `table_fqn` incrementally (committed positions and in-flight pins). A
/// consumer pointing at a snapshot we cannot resolve marks the floor unknown,
/// which blocks expire for safety.
pub(crate) fn downstream_floor(
    definitions: &[StoredMvDefinition],
    table_fqn: &str,
    snapshot_ts_by_id: &BTreeMap<i64, i64>,
) -> DownstreamFloor {
    let mut floor_ts: Option<i64> = None;
    let mut unknown = false;
    let mut consider = |snapshot_id: i64| match snapshot_ts_by_id.get(&snapshot_id) {
        Some(ts) => floor_ts = Some(floor_ts.map_or(*ts, |f| f.min(*ts))),
        None => unknown = true,
    };
    for definition in definitions {
        if let Some(id) = definition.last_refresh_snapshots.get(table_fqn) {
            consider(*id);
        }
        if let Some(id) = definition.refresh_target_snapshots.get(table_fqn) {
            consider(*id);
        }
    }
    DownstreamFloor { floor_ts_ms: floor_ts, unknown }
}

/// Load fresh metadata for one MV storage table and assemble stats.
/// `definitions` is the full MV list from the same pass, used for the floor.
pub(crate) fn collect_table_stats(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    table: &str,
    definitions: &[StoredMvDefinition],
) -> Result<TableMaintenanceStats, String> {
    let (iceberg_catalog, table_ident, _object_store) =
        crate::engine::iceberg_maintenance::resolve_maintenance_catalog(
            state, catalog, namespace, table,
        )?;
    let loaded = crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
        iceberg_catalog.load_table(&table_ident).await
    })?
    .map_err(|e| format!("load iceberg table {catalog}.{namespace}.{table} for maintenance failed: {e}"))?;
    let metadata = loaded.metadata();

    let snapshots: Vec<SnapshotInfo> = metadata
        .snapshots()
        .map(|s| SnapshotInfo { snapshot_id: s.snapshot_id(), timestamp_ms: s.timestamp_ms() })
        .collect();
    let snapshot_ts_by_id: BTreeMap<i64, i64> = snapshots
        .iter()
        .map(|s| (s.snapshot_id, s.timestamp_ms))
        .collect();

    let summary = metadata
        .current_snapshot()
        .map(|s| s.summary().additional_properties.clone())
        .unwrap_or_default();

    let fqn = format!("{catalog}.{namespace}.{table}");
    let floor = downstream_floor(definitions, &fqn, &snapshot_ts_by_id);

    let non_main_ref_count = metadata
        .refs()
        .keys()
        .filter(|name| name.as_str() != MAIN_BRANCH)
        .count();

    Ok(TableMaintenanceStats {
        current_snapshot_id: metadata.current_snapshot_id(),
        snapshots,
        total_data_files: summary_u64(&summary, TOTAL_DATA_FILES_KEY),
        total_files_size_bytes: summary_u64(&summary, TOTAL_FILES_SIZE_KEY),
        total_delete_files: summary_u64(&summary, TOTAL_DELETE_FILES_KEY),
        properties: metadata.properties().clone(),
        non_main_ref_count,
        downstream_floor_ts_ms: floor.floor_ts_ms,
        downstream_floor_unknown: floor.unknown,
    })
}
```

注意:`collect_table_stats` 依赖 Task 6 的 `resolve_maintenance_catalog`,本任务编译会失败 —— **Task 5 与 Task 6 须在同一次编译验证内完成**(先写两者再统一跑测试),或将 Step 3 的 `collect_table_stats` 留到 Task 6 完成后再加。推荐顺序:Step 1-2(纯函数测试)→ 先实现 `summary_u64` / `downstream_floor` / `DownstreamFloor` → Step 4 跑纯函数测试 PASS → Task 6 → 回来补 `collect_table_stats` → 全量编译。

- [ ] **Step 4: 跑纯函数测试确认 PASS**

Run: `cargo test --lib engine::mv_maintenance::stats`
Expected: 5 个测试 PASS。

- [ ] **Step 5: Commit(可与 Task 6 合并提交,见 Task 6 Step 5)**

---

### Task 6: iceberg_maintenance.rs 重构 — 提取可复用的 catalog 解析与 job 入队

**Files:**
- Modify: `src/engine/iceberg_maintenance.rs:394-441`(`create_legacy_optimize_job`)、`:478-497`(`build_action_catalog`)

- [ ] **Step 1: 提取 `resolve_maintenance_catalog`**

把 `build_action_catalog` 的函数体抽成按字符串参数的 `pub(crate)` 函数,原函数委托:

```rust
/// Resolve a registered iceberg catalog into an executable handle for
/// maintenance actions. Shared by SQL-driven maintenance and the automatic
/// maintenance coordinator (mv_maintenance).
pub(crate) fn resolve_maintenance_catalog(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace: &str,
    table: &str,
) -> Result<(Arc<dyn Catalog>, TableIdent, Option<ObjectStoreConfig>), String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(catalog_name)?
    };
    entry.invalidate_table_cache(namespace, table);
    let object_store_config = entry.object_store_config().cloned();
    let catalog: Arc<dyn Catalog> = build_iceberg_catalog(&entry)?;
    let table_ident = TableIdent::new(
        NamespaceIdent::new(namespace.to_string()),
        table.to_string(),
    );
    Ok((catalog, table_ident, object_store_config))
}

fn build_action_catalog(
    state: &Arc<StandaloneState>,
    request: &MaintenanceActionRequest,
) -> Result<(Arc<dyn Catalog>, TableIdent, Option<ObjectStoreConfig>), String> {
    resolve_maintenance_catalog(state, &request.catalog, &request.namespace, &request.table)
}
```

- [ ] **Step 2: 提取 `enqueue_optimize_job`**

把 `create_legacy_optimize_job` 中「load 表取 base_snapshot_id + 写 job」的主体抽出,返回 job id:

```rust
/// Create a pending whole-table optimize job for `catalog.namespace.table`.
/// Returns the job id. Fails with the repository conflict error when an
/// active (pending/running) job already exists for the table.
pub(crate) fn enqueue_optimize_job(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<i64, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Err("iceberg optimize requires metadata provider".to_string());
    };
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(catalog)?
    };
    entry.invalidate_table_cache(namespace, table);
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table)?;
    let base_snapshot_id = loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id())
        .ok_or_else(|| {
            format!("iceberg table {catalog}.{namespace}.{table} has no current snapshot")
        })?;
    let mut txn = provider
        .begin_write("create iceberg optimize job")
        .map_err(|e| format!("open iceberg optimize job transaction failed: {e}"))?;
    let job = state
        .job_repo
        .create_iceberg_optimize_job(
            txn.as_mut(),
            CreateIcebergOptimizeJobRequest {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                base_snapshot_id,
                now_ms: maintenance_now_ms(),
            },
        )
        .map_err(|e| format!("create iceberg optimize job failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg optimize job failed: {e}"))?;
    Ok(job.id)
}
```

`create_legacy_optimize_job` 改为委托(保留原 OPTIMIZE 的报错文案语义):

```rust
fn create_legacy_optimize_job(
    state: &Arc<StandaloneState>,
    request: &MaintenanceActionRequest,
) -> Result<StatementResult, String> {
    enqueue_optimize_job(state, &request.catalog, &request.namespace, &request.table)
        .map_err(|e| format!("ALTER TABLE OPTIMIZE: {e}"))?;
    Ok(StatementResult::Ok)
}
```

注意:原实现的错误文案(`"ALTER TABLE OPTIMIZE requires metadata provider"` / `"requires iceberg table ... to have a current snapshot"`)有变化;先 `grep -rn "OPTIMIZE requires" sql-tests/ tests/ src/` 确认无 golden/断言依赖原文案,有则保持原文案不动(把文案参数化或在委托处还原)。

- [ ] **Step 3: 编译 + 既有测试回归**

Run: `cargo build && cargo test --lib engine::iceberg_maintenance && cargo test --lib engine::mv_maintenance::stats`
Expected: 编译通过,既有 maintenance 测试 PASS。此时补上 Task 5 Step 3 中的 `collect_table_stats`(依赖已就位),再 `cargo build` 确认。

- [ ] **Step 4: (不需要新测试)** `resolve_maintenance_catalog` / `enqueue_optimize_job` 是行为保持的提取,由既有测试 + Task 10 集成测试覆盖。

- [ ] **Step 5: Commit**

```bash
git add src/engine/iceberg_maintenance.rs src/engine/mv_maintenance/
git commit -m "feat(mv-maintenance): table stats collection and reusable maintenance helpers"
```

---

### Task 7: mod.rs — 执行器 trait、Coordinator 决策与簿记(纯逻辑部分)

**Files:**
- Modify: `src/engine/mv_maintenance/mod.rs`

- [ ] **Step 1: 写类型与失败测试**

mod.rs 追加类型:

```rust
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::engine::StandaloneState;

use self::policy::{
    evaluate_table, failure_backoff_ms, ActionKind, EvaluationOutcome, MaintenanceAction,
    MaintenancePolicyConfig, TableMaintenanceStats, TablePolicy, TableRuntimeState,
};

/// Signals consumed by the coordinator thread. `Wake` is sent after every
/// successful MV refresh; `Stop` is sent by the handle on drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceSignal {
    Wake,
    Stop,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenanceTarget {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

impl MaintenanceTarget {
    pub(crate) fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OptimizeSubmission {
    Submitted { job_id: i64 },
    AlreadyActive,
}

/// Side-effect boundary: real impl talks to iceberg catalogs and the job
/// queue; tests inject a recording fake (same pattern as RefreshExecutor in
/// mv_scheduler.rs).
pub(crate) trait MaintenanceExecutor {
    fn expire_snapshots(
        &mut self,
        target: &MaintenanceTarget,
        older_than_ms: i64,
        retain_last: u32,
    ) -> Result<crate::connector::iceberg::commit::expire_snapshots::ExpireOutcome, String>;

    fn rewrite_position_deletes(
        &mut self,
        target: &MaintenanceTarget,
        min_input_files: usize,
    ) -> Result<
        crate::connector::iceberg::commit::rewrite_position_delete_files::RewritePositionDeleteOutcome,
        String,
    >;

    fn submit_optimize(
        &mut self,
        target: &MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenanceCoordinatorConfig {
    pub(crate) enabled: bool,
    pub(crate) tick_interval_ms: u64,
    pub(crate) max_concurrent: usize,
    pub(crate) policy: MaintenancePolicyConfig,
}

impl MaintenanceCoordinatorConfig {
    pub(crate) fn from_standalone_config(
        config: &crate::common::app_config::StandaloneServerConfig,
    ) -> Self {
        Self {
            enabled: config.iceberg_maintenance_enabled,
            tick_interval_ms: config.iceberg_maintenance_tick_interval_ms.max(1),
            max_concurrent: config.iceberg_maintenance_max_concurrent.max(1),
            policy: MaintenancePolicyConfig {
                compaction_min_data_files: config.iceberg_maintenance_compaction_min_data_files,
                dv_min_delete_files: config.iceberg_maintenance_dv_min_delete_files,
                action_cooldown_ms: config.iceberg_maintenance_action_cooldown_ms,
                max_consecutive_failures: config.iceberg_maintenance_max_consecutive_failures,
            },
        }
    }
}

pub(crate) struct MaintenanceCoordinator {
    config: MaintenanceCoordinatorConfig,
    runtime: BTreeMap<i64, TableRuntimeState>,
}
```

簿记与单表执行(决策→执行→状态更新)的可测核心:

```rust
impl MaintenanceCoordinator {
    pub(crate) fn new(config: MaintenanceCoordinatorConfig) -> Self {
        Self { config, runtime: BTreeMap::new() }
    }

    fn runtime_entry(&mut self, mv_id: i64) -> &mut TableRuntimeState {
        self.runtime.entry(mv_id).or_default()
    }

    fn record_success(&mut self, mv_id: i64, kind: ActionKind) {
        let entry = self.runtime_entry(mv_id);
        entry.consecutive_failures.remove(&kind);
        entry.next_attempt_after_ms.remove(&kind);
    }

    fn record_failure(&mut self, mv_id: i64, kind: ActionKind, now_ms: i64, max_failures: u32) {
        let entry = self.runtime_entry(mv_id);
        let attempts = entry.consecutive_failures.entry(kind).or_insert(0);
        *attempts = attempts.saturating_add(1);
        if *attempts >= max_failures {
            entry.circuit_broken.insert(kind);
        } else {
            entry
                .next_attempt_after_ms
                .insert(kind, now_ms.saturating_add(failure_backoff_ms(*attempts)));
        }
    }

    /// Evaluate one table and run the planned actions through `executor`.
    /// Returns the evaluation outcome for logging/testing.
    pub(crate) fn process_table(
        &mut self,
        mv_id: i64,
        target: &MaintenanceTarget,
        stats: &TableMaintenanceStats,
        executor: &mut dyn MaintenanceExecutor,
        now_ms: i64,
    ) -> EvaluationOutcome {
        let policy = TablePolicy::resolve(&self.config.policy, &stats.properties);
        let outcome = {
            let runtime = self.runtime_entry(mv_id);
            evaluate_table(stats, &policy, runtime, &self.config.policy, now_ms)
        };
        let max_failures = self.config.policy.max_consecutive_failures;
        for action in &outcome.actions {
            let kind = action.kind();
            self.runtime_entry(mv_id).last_action_ms.insert(kind, now_ms);
            let result: Result<String, String> = match action {
                MaintenanceAction::ExpireSnapshots { older_than_ms, retain_last } => executor
                    .expire_snapshots(target, *older_than_ms, *retain_last)
                    .map(|o| {
                        format!(
                            "expired_snapshots={} deleted_files={}",
                            o.expired_snapshot_count, o.deleted_file_count
                        )
                    }),
                MaintenanceAction::RewritePositionDeletes { min_input_files } => executor
                    .rewrite_position_deletes(target, *min_input_files)
                    .map(|o| {
                        format!(
                            "rewritten_delete_files={} added_delete_files={}",
                            o.rewritten_delete_files_count, o.added_delete_files_count
                        )
                    }),
                MaintenanceAction::SubmitOptimize => {
                    executor.submit_optimize(target).map(|s| match s {
                        OptimizeSubmission::Submitted { job_id } => {
                            format!("optimize_job_id={job_id}")
                        }
                        OptimizeSubmission::AlreadyActive => "optimize_job=already-active".to_string(),
                    })
                }
            };
            match result {
                Ok(detail) => {
                    self.record_success(mv_id, kind);
                    tracing::info!(
                        table = %target.fqn(),
                        action = ?kind,
                        data_files = ?stats.total_data_files,
                        files_size = ?stats.total_files_size_bytes,
                        delete_files = ?stats.total_delete_files,
                        %detail,
                        "auto maintenance action completed"
                    );
                }
                Err(err) => {
                    self.record_failure(mv_id, kind, now_ms, max_failures);
                    tracing::warn!(
                        table = %target.fqn(),
                        action = ?kind,
                        error = %err,
                        "auto maintenance action failed"
                    );
                }
            }
        }
        for (kind, reason) in &outcome.skips {
            tracing::debug!(
                table = %target.fqn(),
                action = ?kind,
                reason = ?reason,
                "auto maintenance action skipped"
            );
        }
        self.runtime_entry(mv_id).last_seen_snapshot_id = stats.current_snapshot_id;
        outcome
    }
}
```

测试(mod.rs `#[cfg(test)] mod tests`):RecordingExecutor fake(照 mv_scheduler.rs:923 的 `RecordingRefreshExecutor` 模式):

```rust
#[cfg(test)]
mod tests {
    use super::policy::*;
    use super::*;
    use crate::connector::iceberg::commit::expire_snapshots::ExpireOutcome;
    use crate::connector::iceberg::commit::rewrite_position_delete_files::RewritePositionDeleteOutcome;

    #[derive(Default)]
    struct RecordingExecutor {
        expires: Vec<(String, i64, u32)>,
        dv_rewrites: Vec<(String, usize)>,
        optimize_submissions: Vec<String>,
        fail_expire: Option<String>,
    }

    impl MaintenanceExecutor for RecordingExecutor {
        fn expire_snapshots(
            &mut self,
            target: &MaintenanceTarget,
            older_than_ms: i64,
            retain_last: u32,
        ) -> Result<ExpireOutcome, String> {
            self.expires.push((target.fqn(), older_than_ms, retain_last));
            match self.fail_expire.as_ref() {
                Some(message) => Err(message.clone()),
                None => Ok(ExpireOutcome { expired_snapshot_count: 1, deleted_file_count: 2 }),
            }
        }

        fn rewrite_position_deletes(
            &mut self,
            target: &MaintenanceTarget,
            min_input_files: usize,
        ) -> Result<RewritePositionDeleteOutcome, String> {
            self.dv_rewrites.push((target.fqn(), min_input_files));
            Ok(RewritePositionDeleteOutcome::default())
        }

        fn submit_optimize(
            &mut self,
            target: &MaintenanceTarget,
        ) -> Result<OptimizeSubmission, String> {
            self.optimize_submissions.push(target.fqn());
            Ok(OptimizeSubmission::Submitted { job_id: 7 })
        }
    }

    fn target() -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_x".to_string(),
        }
    }

    fn coordinator() -> MaintenanceCoordinator {
        MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            enabled: true,
            tick_interval_ms: 600_000,
            max_concurrent: 1,
            policy: MaintenancePolicyConfig::default(),
        })
    }

    fn old_small_file_stats() -> TableMaintenanceStats {
        TableMaintenanceStats {
            current_snapshot_id: Some(30),
            snapshots: vec![
                SnapshotInfo { snapshot_id: 10, timestamp_ms: 1_000 },
                SnapshotInfo { snapshot_id: 30, timestamp_ms: 3_000 },
            ],
            total_data_files: Some(200),
            total_files_size_bytes: Some(200 * 1024 * 1024),
            total_delete_files: Some(0),
            properties: std::collections::HashMap::new(),
            non_main_ref_count: 0,
            downstream_floor_ts_ms: None,
            downstream_floor_unknown: false,
        }
    }

    const NOW: i64 = 1_000_000_000;

    #[test]
    fn process_table_runs_planned_actions_and_records_snapshot() {
        let mut coordinator = coordinator();
        let mut executor = RecordingExecutor::default();
        let outcome =
            coordinator.process_table(1, &target(), &old_small_file_stats(), &mut executor, NOW);
        assert_eq!(executor.expires.len(), 1);
        assert_eq!(executor.optimize_submissions.len(), 1);
        assert!(executor.dv_rewrites.is_empty()); // suppressed by optimize
        assert_eq!(outcome.actions.len(), 2);
        // Second pass with identical stats: snapshot unchanged -> no compaction,
        // expire still planned (old snapshot remains in fake-world stats).
        let outcome2 =
            coordinator.process_table(1, &target(), &old_small_file_stats(), &mut executor, NOW + 1);
        assert!(outcome2
            .skips
            .contains(&(ActionKind::Optimize, SkipReason::SnapshotUnchanged)));
    }

    #[test]
    fn repeated_failures_trip_the_circuit_breaker() {
        let mut coordinator = coordinator();
        let mut executor = RecordingExecutor {
            fail_expire: Some("simulated failure".to_string()),
            ..RecordingExecutor::default()
        };
        let mut now = NOW;
        // 4 failures (default max) -> circuit broken; backoff between attempts.
        for _ in 0..4 {
            // Re-evaluation happens only after backoff expires; jump past it.
            now += FAILURE_BACKOFF_MAX_MS + 1;
            coordinator.process_table(1, &target(), &old_small_file_stats(), &mut executor, now);
        }
        assert_eq!(executor.expires.len(), 4);
        now += FAILURE_BACKOFF_MAX_MS + 1;
        let outcome =
            coordinator.process_table(1, &target(), &old_small_file_stats(), &mut executor, now);
        assert!(outcome.skips.contains(&(ActionKind::Expire, SkipReason::CircuitBroken)));
        assert_eq!(executor.expires.len(), 4, "no further attempts after circuit break");
    }

    #[test]
    fn optimize_cooldown_prevents_immediate_retrigger() {
        let mut coordinator = coordinator();
        let mut executor = RecordingExecutor::default();
        coordinator.process_table(1, &target(), &old_small_file_stats(), &mut executor, NOW);
        // New snapshot but within cooldown.
        let mut stats = old_small_file_stats();
        stats.current_snapshot_id = Some(31);
        let outcome = coordinator.process_table(1, &target(), &stats, &mut executor, NOW + 1);
        assert!(outcome.skips.contains(&(ActionKind::Optimize, SkipReason::Cooldown)));
        assert_eq!(executor.optimize_submissions.len(), 1);
    }
}
```

- [ ] **Step 2: 跑测试确认 FAIL → 实现(类型与 impl 同 Step 1 代码)→ 确认 PASS**

Run: `cargo test --lib engine::mv_maintenance`
Expected: policy + stats + mod 全部 PASS。

注意:`ExpireOutcome` 当前没有 derive(Debug 以外的东西也没有 Default);RecordingExecutor 手工构造字面量即可(`ExpireOutcome { expired_snapshot_count: 1, deleted_file_count: 2 }` 两字段都是 pub)。若 `expire_snapshots` 模块/类型不是 pub 路径可达,在 `src/connector/iceberg/commit/mod.rs` 确认 `pub mod expire_snapshots;`(现状即是 pub,`run_expire_snapshots` 被 engine 调用)。

- [ ] **Step 3: Commit**

```bash
git add src/engine/mv_maintenance/mod.rs
git commit -m "feat(mv-maintenance): coordinator bookkeeping and executor boundary"
```

---

### Task 8: run_pass IO 装配 + 真实执行器 + 线程与 Handle

**Files:**
- Modify: `src/engine/mv_maintenance/mod.rs`

- [ ] **Step 1: 实现真实执行器**

```rust
/// Production executor: expire / DV-rewrite run inline via block_on_iceberg;
/// optimize is submitted to the existing SQLite job queue and executed by the
/// iceberg-optimize-worker.
pub(crate) struct StateMaintenanceExecutor {
    state: Arc<StandaloneState>,
}

impl StateMaintenanceExecutor {
    pub(crate) fn new(state: Arc<StandaloneState>) -> Self {
        Self { state }
    }
}

impl MaintenanceExecutor for StateMaintenanceExecutor {
    fn expire_snapshots(
        &mut self,
        target: &MaintenanceTarget,
        older_than_ms: i64,
        retain_last: u32,
    ) -> Result<crate::connector::iceberg::commit::expire_snapshots::ExpireOutcome, String> {
        let (catalog, table_ident, _) = crate::engine::iceberg_maintenance::resolve_maintenance_catalog(
            &self.state,
            &target.catalog,
            &target.namespace,
            &target.table,
        )?;
        let params = crate::connector::iceberg::commit::expire_snapshots::ExpireParams {
            older_than_ms: Some(older_than_ms),
            retain_last: Some(retain_last),
        };
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
            crate::connector::iceberg::commit::expire_snapshots::run_expire_snapshots(
                catalog,
                table_ident,
                params,
            )
            .await
        })?
    }

    fn rewrite_position_deletes(
        &mut self,
        target: &MaintenanceTarget,
        min_input_files: usize,
    ) -> Result<
        crate::connector::iceberg::commit::rewrite_position_delete_files::RewritePositionDeleteOutcome,
        String,
    > {
        let (catalog, table_ident, _) = crate::engine::iceberg_maintenance::resolve_maintenance_catalog(
            &self.state,
            &target.catalog,
            &target.namespace,
            &target.table,
        )?;
        let options =
            crate::connector::iceberg::commit::rewrite_position_delete_files::RewritePositionDeleteOptions {
                rewrite_all: false,
                min_input_files,
            };
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
            crate::connector::iceberg::commit::rewrite_position_delete_files::run_rewrite_position_delete_files(
                catalog,
                table_ident,
                options,
            )
            .await
        })?
    }

    fn submit_optimize(
        &mut self,
        target: &MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        match crate::engine::iceberg_maintenance::enqueue_optimize_job(
            &self.state,
            &target.catalog,
            &target.namespace,
            &target.table,
        ) {
            Ok(job_id) => Ok(OptimizeSubmission::Submitted { job_id }),
            // The job repo rejects duplicates with a conflict error; treat it
            // as "already active" instead of a failure.
            Err(err) if err.contains("already exists") => Ok(OptimizeSubmission::AlreadyActive),
            Err(err) => Err(err),
        }
    }
}
```

- [ ] **Step 2: 实现候选发现与 run_pass**

```rust
struct MaintenanceCandidate {
    mv_id: i64,
    target: MaintenanceTarget,
    refresh_in_flight: bool,
}

fn load_candidates(
    state: &Arc<StandaloneState>,
) -> Result<(Vec<crate::meta::repository::mv::StoredMvDefinition>, Vec<MaintenanceCandidate>), String>
{
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok((Vec::new(), Vec::new()));
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open mv maintenance read transaction failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("list mv definitions for maintenance failed: {e}"))?;
    let candidates = definitions
        .iter()
        .filter(|d| d.storage_engine.eq_ignore_ascii_case("iceberg"))
        .filter_map(|d| {
            let (Some(catalog), Some(namespace), Some(table)) = (
                d.target_catalog.as_ref(),
                d.target_namespace.as_ref(),
                d.target_table.as_ref(),
            ) else {
                return None;
            };
            Some(MaintenanceCandidate {
                mv_id: d.mv_id,
                target: MaintenanceTarget {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                },
                refresh_in_flight: d.refresh_in_progress || d.active_refresh_id.is_some(),
            })
        })
        .collect();
    Ok((definitions, candidates))
}

impl MaintenanceCoordinator {
    /// One full evaluation pass over all Iceberg-backed MV storage tables.
    /// Deterministic given (state contents, now_ms); the integration tests
    /// call this directly instead of going through the thread.
    pub(crate) fn run_pass(
        &mut self,
        state: &Arc<StandaloneState>,
        executor: &mut dyn MaintenanceExecutor,
        now_ms: i64,
    ) -> Result<(), String> {
        let (definitions, candidates) = load_candidates(state)?;
        let mut executed_tables = 0usize;
        for candidate in &candidates {
            if candidate.refresh_in_flight {
                tracing::debug!(
                    table = %candidate.target.fqn(),
                    "auto maintenance skipped: refresh in flight"
                );
                continue;
            }
            let stats = match stats::collect_table_stats(
                state,
                &candidate.target.catalog,
                &candidate.target.namespace,
                &candidate.target.table,
                &definitions,
            ) {
                Ok(stats) => stats,
                Err(err) => {
                    tracing::warn!(
                        table = %candidate.target.fqn(),
                        error = %err,
                        "auto maintenance stats collection failed"
                    );
                    continue;
                }
            };
            if executed_tables >= self.config.max_concurrent {
                // Defer without observing the snapshot so the next pass
                // re-evaluates this table from scratch.
                continue;
            }
            let outcome =
                self.process_table(candidate.mv_id, &candidate.target, &stats, executor, now_ms);
            if !outcome.actions.is_empty() {
                executed_tables += 1;
            }
        }
        Ok(())
    }
}
```


- [ ] **Step 3: 实现 Handle / 线程 / 启动函数(照 mv_scheduler.rs:813-879 骨架)**

```rust
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) struct MaintenanceCoordinatorHandle {
    enabled: bool,
    signal_tx: Option<Sender<MaintenanceSignal>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl MaintenanceCoordinatorHandle {
    pub(crate) fn disabled() -> Self {
        Self { enabled: false, signal_tx: None, worker: None }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl Drop for MaintenanceCoordinatorHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.signal_tx.take() {
            let _ = tx.send(MaintenanceSignal::Stop);
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

/// Notify the coordinator that an MV refresh committed. Cheap no-op when the
/// coordinator is not running (tests, disabled config).
pub(crate) fn notify_refresh_completed(state: &Arc<StandaloneState>) {
    if let Ok(guard) = state.maintenance_signal_tx.lock() {
        if let Some(tx) = guard.as_ref() {
            let _ = tx.send(MaintenanceSignal::Wake);
        }
    }
}

pub(crate) fn start_maintenance_coordinator_for_server(
    engine: &crate::engine::StandaloneNovaRocks,
    config: MaintenanceCoordinatorConfig,
) -> MaintenanceCoordinatorHandle {
    if !config.enabled {
        return MaintenanceCoordinatorHandle::disabled();
    }
    let state = Arc::clone(&engine.inner);
    let (signal_tx, signal_rx) = mpsc::channel();
    if let Ok(mut guard) = state.maintenance_signal_tx.lock() {
        *guard = Some(signal_tx.clone());
    }
    let worker_config = config.clone();
    let worker_state = Arc::clone(&state);
    let worker = thread::Builder::new()
        .name("novarocks-iceberg-maintenance".to_string())
        .spawn(move || {
            let mut coordinator = MaintenanceCoordinator::new(worker_config.clone());
            let mut executor = StateMaintenanceExecutor::new(Arc::clone(&worker_state));
            loop {
                if let Err(err) =
                    coordinator.run_pass(&worker_state, &mut executor, current_time_ms())
                {
                    tracing::warn!(error = %err, "iceberg maintenance pass failed");
                }
                match signal_rx.recv_timeout(Duration::from_millis(worker_config.tick_interval_ms))
                {
                    Ok(MaintenanceSignal::Stop) | Err(RecvTimeoutError::Disconnected) => break,
                    Ok(MaintenanceSignal::Wake) => {
                        // Coalesce bursts of refresh completions into one pass.
                        let mut stop = false;
                        while let Ok(signal) = signal_rx.try_recv() {
                            if signal == MaintenanceSignal::Stop {
                                stop = true;
                                break;
                            }
                        }
                        if stop {
                            break;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                }
            }
        });
    match worker {
        Ok(worker) => MaintenanceCoordinatorHandle {
            enabled: true,
            signal_tx: Some(signal_tx),
            worker: Some(worker),
        },
        Err(err) => {
            tracing::warn!(error = %err, "failed to start iceberg maintenance worker");
            MaintenanceCoordinatorHandle::disabled()
        }
    }
}
```

- [ ] **Step 4: 编译 + 既有测试**

Run: `cargo build && cargo test --lib engine::mv_maintenance`
Expected: 编译通过(`state.maintenance_signal_tx` 字段尚不存在会失败 —— 该字段在 Task 9 加;**Task 8 与 Task 9 同一次编译验证**,可顺序写完再统一跑)。

- [ ] **Step 5: Commit(与 Task 9 合并,见 Task 9)**

---

### Task 9: 接线 — StandaloneState 字段、refresh 事件投递、服务启动

**Files:**
- Modify: `src/engine/mod.rs:217-247`(StandaloneState 结构体)、`:249-280`(Default impl)
- Modify: `src/engine/mv_flow.rs:617` 附近(refresh_mv 成功返回前)
- Modify: `src/server/mod.rs:60-76`(ResolvedStandaloneServerOptions)、`:166-194`(run_with_resolved_options)、`:196-253`(resolve/extract)

- [ ] **Step 1: StandaloneState 字段**

结构体内(`views` 字段之前)追加:

```rust
    /// Wake-up channel for the iceberg maintenance coordinator; injected by
    /// the server after the coordinator thread starts, None otherwise.
    pub(crate) maintenance_signal_tx:
        std::sync::Mutex<Option<std::sync::mpsc::Sender<crate::engine::mv_maintenance::MaintenanceSignal>>>,
```

`impl Default for StandaloneState` 对应追加:

```rust
            maintenance_signal_tx: std::sync::Mutex::new(None),
```

(`open_body` 的结构体字面量以 `..Default::default()` 结尾,无需改动。)

- [ ] **Step 2: refresh 成功事件投递**

`src/engine/mv_flow.rs` 的 `refresh_mv` 末尾,把:

```rust
    Ok(StatementResult::Ok)
}
```

改为(仅 `refresh_mv` 这一处,:617;不要动同文件其他 `Ok(StatementResult::Ok)`):

```rust
    crate::engine::mv_maintenance::notify_refresh_completed(state);
    Ok(StatementResult::Ok)
}
```

- [ ] **Step 3: 服务端配置贯通**

`src/server/mod.rs`:

1. import 区追加:`use crate::engine::mv_maintenance::MaintenanceCoordinatorConfig;`
2. `ResolvedStandaloneServerOptions` 增加字段:

```rust
    maintenance: MaintenanceCoordinatorConfig,
```

3. `extract_server_settings` 返回值改为 4 元组:

```rust
fn extract_server_settings(
    standalone: Option<&crate::common::app_config::StandaloneServerConfig>,
    port_override: Option<u16>,
) -> Result<(u16, String, RefreshCoordinatorConfig, MaintenanceCoordinatorConfig), String> {
    let mut mysql_port = DEFAULT_MYSQL_PORT;
    let mut user = ROOT_USER.to_string();
    let mut refresh_coordinator = RefreshCoordinatorConfig::default();
    let mut maintenance = MaintenanceCoordinatorConfig::from_standalone_config(
        &crate::common::app_config::StandaloneServerConfig::default(),
    );

    if let Some(sc) = standalone {
        mysql_port = sc.mysql_port;
        if sc.user != ROOT_USER {
            return Err(format!(
                "standalone server only supports user `{ROOT_USER}`, got `{}`",
                sc.user
            ));
        }
        user = sc.user.clone();
        refresh_coordinator = RefreshCoordinatorConfig::from_standalone_config(sc);
        maintenance = MaintenanceCoordinatorConfig::from_standalone_config(sc);
    }

    if let Some(port) = port_override {
        mysql_port = port;
    }

    Ok((mysql_port, user, refresh_coordinator, maintenance))
}
```

4. 所有 `extract_server_settings` 调用点同步解构 4 元组并填充 `maintenance` 字段(`resolve_server_options` 与 preloaded-config 路径各一处;`grep -n "extract_server_settings" src/server/mod.rs` 找全)。
5. `run_with_resolved_options` 在 refresh coordinator 启动行之后追加:

```rust
    let _maintenance_coordinator =
        crate::engine::mv_maintenance::start_maintenance_coordinator_for_server(
            &engine,
            resolved.maintenance.clone(),
        );
```

- [ ] **Step 4: 编译 + 全量单测**

Run: `cargo build && cargo test --lib engine::mv_maintenance && cargo test --lib server`
Expected: 编译通过、测试 PASS。`src/server/mod.rs` 若有 `extract_server_settings` 的既有测试,按 4 元组更新断言。

- [ ] **Step 5: Commit(含 Task 8 改动)**

```bash
git add src/engine/mv_maintenance/mod.rs src/engine/mod.rs src/engine/mv_flow.rs src/server/mod.rs
git commit -m "feat(mv-maintenance): coordinator thread, refresh wake-up wiring, server startup"
```

---

### Task 10: 集成测试 — 验收场景 ①②③④

**Files:**
- Create: `src/engine/mv_maintenance/tests.rs`
- Modify: `src/engine/mv_maintenance/mod.rs`(追加 `#[cfg(test)] mod tests;` —— 注意 mod.rs 内已有单测模块时,集成测试模块命名为 `#[cfg(test)] mod integration_tests;`,文件名 `integration_tests.rs`)

测试驱动方式:**不起线程**,构造真实 `StandaloneState`(hadoop 本地 catalog,照抄 `iceberg_refresh.rs:12162` 的 `open_test_state_with_hadoop_iceberg_catalog` 模式,helper 复制进本文件),直接调 `coordinator.run_pass(&state, &mut StateMaintenanceExecutor::new(...), now_ms)`,`now_ms` 手工注入。

- [ ] **Step 1: 写测试环境 helper(复制改名)**

```rust
//! End-to-end tests for automatic MV maintenance, driven deterministically by
//! calling run_pass directly (no coordinator thread, injected now_ms).

use std::sync::Arc;

use tempfile::TempDir;

use super::*;
use crate::engine::{StandaloneSession, StandaloneState};
use crate::runtime::query_result::StatementResult;

struct MaintenanceTestEnv {
    state: Arc<StandaloneState>,
    current_db: String,
    _metadata_dir: TempDir,
    _warehouse_dir: TempDir,
}

fn open_env() -> MaintenanceTestEnv {
    let metadata_dir = TempDir::new().expect("metadata tempdir");
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let metadata_path = metadata_dir.path().join("standalone.sqlite");
    let metadata_provider =
        crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open meta provider");
    let state = Arc::new(StandaloneState {
        metadata_provider: Some(Arc::new(metadata_provider)),
        ..StandaloneState::default()
    });
    crate::connector::register_standalone_backends(&state);
    {
        let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
        catalogs
            .create_catalog(
                "ice",
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    (
                        "iceberg.catalog.warehouse".to_string(),
                        format!("file://{}", warehouse_dir.path().display()),
                    ),
                ],
            )
            .expect("create iceberg catalog");
    }
    crate::connector::register_iceberg_catalog_mgr_entry(&state, "ice")
        .expect("register iceberg catalog mgr entry");
    MaintenanceTestEnv {
        state,
        current_db: "analytics".to_string(),
        _metadata_dir: metadata_dir,
        _warehouse_dir: warehouse_dir,
    }
}

fn exec_sql(env: &MaintenanceTestEnv, sql: &str) {
    let session = StandaloneSession { inner: Arc::clone(&env.state) };
    match session
        .execute_in_context(sql, Some("ice"), &env.current_db, None)
        .unwrap_or_else(|e| panic!("execute `{sql}` failed: {e}"))
    {
        StatementResult::Ok | StatementResult::Query(_) => {}
    }
}

fn coordinator_with(policy_overrides: impl FnOnce(&mut MaintenanceCoordinatorConfig)) -> MaintenanceCoordinator {
    let mut config = MaintenanceCoordinatorConfig {
        enabled: true,
        tick_interval_ms: 600_000,
        max_concurrent: 10,
        policy: policy::MaintenancePolicyConfig::default(),
    };
    policy_overrides(&mut config);
    MaintenanceCoordinator::new(config)
}

fn mv_table_snapshot_count(env: &MaintenanceTestEnv, namespace: &str, table: &str) -> usize {
    let (catalog, ident, _) = crate::engine::iceberg_maintenance::resolve_maintenance_catalog(
        &env.state, "ice", namespace, table,
    )
    .expect("resolve catalog");
    let loaded = crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
        catalog.load_table(&ident).await
    })
    .expect("runtime")
    .expect("load table");
    loaded.metadata().snapshots().len()
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
```

环境搭建语句:建库表用 SQL(`CREATE DATABASE`/`CREATE TABLE`/`INSERT`/`CREATE MATERIALIZED VIEW`/`REFRESH MATERIALIZED VIEW`)经 `exec_sql` 全链路执行;若 `CREATE TABLE` 在 hadoop catalog 上的 SQL 形态与 `iceberg_refresh.rs` 测试不同,改用其 `create_aggregate_fact_table` 同款 registry API(`crate::connector::iceberg::catalog::registry` 的 create/insert helper,见 iceberg_refresh.rs:12353/12574 的写法)。

- [ ] **Step 2: 场景②③ — 自动 expire 按表属性触发且尊重下游下界**

```rust
#[test]
fn auto_expire_honors_history_expire_property_and_keeps_min_snapshots() {
    let env = open_env();
    exec_sql(&env, "CREATE DATABASE IF NOT EXISTS sales");
    exec_sql(&env, "CREATE DATABASE IF NOT EXISTS analytics");
    exec_sql(&env, "CREATE TABLE ice.sales.fact (id INT, region STRING, amount BIGINT)");
    exec_sql(&env, "INSERT INTO ice.sales.fact VALUES (1, 'east', 10)");
    exec_sql(
        &env,
        "CREATE MATERIALIZED VIEW mv_fact \
         DISTRIBUTED BY HASH(region) BUCKETS 1 \
         PROPERTIES('storage_engine'='iceberg') \
         AS SELECT region, count(*) AS c FROM ice.sales.fact GROUP BY region",
    );
    // Three refreshes -> >= 3 snapshots on the MV storage table.
    exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_fact");
    exec_sql(&env, "INSERT INTO ice.sales.fact VALUES (2, 'west', 5)");
    exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_fact");
    exec_sql(&env, "INSERT INTO ice.sales.fact VALUES (3, 'east', 7)");
    exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_fact");
    let before = mv_table_snapshot_count(&env, "analytics", "mv_fact");
    assert!(before >= 3, "expected >= 3 MV snapshots, got {before}");

    // Tiny retention window so everything but the protected set is expirable.
    exec_sql(
        &env,
        "ALTER TABLE ice.analytics.mv_fact \
         SET TBLPROPERTIES('history.expire.max-snapshot-age-ms'='1')",
    );

    let mut coordinator = coordinator_with(|_| {});
    let mut executor = StateMaintenanceExecutor::new(Arc::clone(&env.state));
    coordinator
        .run_pass(&env.state, &mut executor, now_ms())
        .expect("maintenance pass");

    let after = mv_table_snapshot_count(&env, "analytics", "mv_fact");
    assert!(after < before, "expire should remove old snapshots ({before} -> {after})");
    assert!(after >= 1, "min-snapshots-to-keep must hold");
    // MV must still answer queries after expire.
    exec_sql(&env, "SELECT * FROM mv_fact");
}
```

下游下界(场景③核心):

```rust
#[test]
fn auto_expire_respects_downstream_incremental_consumer() {
    let env = open_env();
    exec_sql(&env, "CREATE DATABASE IF NOT EXISTS sales");
    exec_sql(&env, "CREATE DATABASE IF NOT EXISTS analytics");
    exec_sql(&env, "CREATE TABLE ice.sales.fact (id INT, region STRING, amount BIGINT)");
    exec_sql(&env, "INSERT INTO ice.sales.fact VALUES (1, 'east', 10)");
    exec_sql(
        &env,
        "CREATE MATERIALIZED VIEW mv_a \
         DISTRIBUTED BY HASH(region) BUCKETS 1 \
         PROPERTIES('storage_engine'='iceberg') \
         AS SELECT id, region, amount FROM ice.sales.fact",
    );
    exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_a");
    exec_sql(
        &env,
        "CREATE MATERIALIZED VIEW mv_b \
         DISTRIBUTED BY HASH(region) BUCKETS 1 \
         PROPERTIES('storage_engine'='iceberg') \
         AS SELECT region, count(*) AS c FROM mv_a GROUP BY region",
    );
    exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_b"); // consumes mv_a snapshot S1

    // Advance mv_a twice without refreshing mv_b: mv_b now lags at S1.
    exec_sql(&env, "INSERT INTO ice.sales.fact VALUES (2, 'west', 5)");
    exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_a");
    exec_sql(&env, "INSERT INTO ice.sales.fact VALUES (3, 'east', 7)");
    exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_a");

    exec_sql(
        &env,
        "ALTER TABLE ice.analytics.mv_a \
         SET TBLPROPERTIES('history.expire.max-snapshot-age-ms'='1')",
    );
    let mut coordinator = coordinator_with(|_| {});
    let mut executor = StateMaintenanceExecutor::new(Arc::clone(&env.state));
    coordinator
        .run_pass(&env.state, &mut executor, now_ms())
        .expect("maintenance pass");

    // The lineage from mv_b's consumed snapshot forward must survive: an
    // incremental refresh of mv_b still works (LineageBroken would fail it).
    exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_b");
    exec_sql(&env, "SELECT * FROM mv_b");
}
```

(若 `mv_b` 的 SELECT 引用 `mv_a` 的名字解析需要库前缀,按 create 成功的形态调整为 `analytics.mv_a` —— 以 `iceberg_refresh.rs` 既有 MV-on-MV 测试的写法为准,`grep -n "FROM mv_" src/engine/mv/iceberg_refresh.rs` 找样例。)

- [ ] **Step 3: 场景① — 自动 OPTIMIZE 收敛文件数**

```rust
#[test]
fn auto_optimize_compacts_small_files() {
    let env = open_env();
    exec_sql(&env, "CREATE DATABASE IF NOT EXISTS sales");
    exec_sql(&env, "CREATE DATABASE IF NOT EXISTS analytics");
    exec_sql(&env, "CREATE TABLE ice.sales.fact (id INT, region STRING, amount BIGINT)");
    exec_sql(
        &env,
        "CREATE MATERIALIZED VIEW mv_fact \
         DISTRIBUTED BY HASH(region) BUCKETS 1 \
         PROPERTIES('storage_engine'='iceberg') \
         AS SELECT id, region, amount FROM ice.sales.fact",
    );
    // Each refresh appends at least one small data file to the MV table.
    for i in 0..3 {
        exec_sql(
            &env,
            &format!("INSERT INTO ice.sales.fact VALUES ({i}, 'east', {i})"),
        );
        exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_fact");
    }

    // Lower the trigger so 3 small files qualify.
    let mut coordinator = coordinator_with(|config| {
        config.policy.compaction_min_data_files = 2;
    });
    let mut executor = StateMaintenanceExecutor::new(Arc::clone(&env.state));
    coordinator
        .run_pass(&env.state, &mut executor, now_ms())
        .expect("maintenance pass");

    // run_pass only submits the job; the worker thread is not spawned under
    // cfg(test), so drive it synchronously.
    crate::connector::iceberg::compact::run_optimize_jobs_once(&env.state)
        .expect("run optimize job");

    // All jobs must be finished (none failed).
    let provider = env.state.metadata_provider.as_ref().expect("provider");
    let read = provider.begin_read().expect("read txn");
    let jobs = env
        .state
        .job_repo
        .show_iceberg_optimize_jobs(read.as_ref())
        .expect("list jobs");
    assert!(!jobs.is_empty(), "expected an auto-submitted optimize job");
    assert!(
        jobs.iter().all(|j| matches!(
            j.state,
            crate::meta::repository::job::IcebergOptimizeJobState::Finished
        )),
        "jobs: {jobs:?}"
    );
    // MV must still answer queries after compaction.
    exec_sql(&env, "SELECT * FROM mv_fact");
}
```

- [ ] **Step 4: 场景④ — 逃生门**

```rust
#[test]
fn maintenance_escape_hatch_disables_table() {
    let env = open_env();
    exec_sql(&env, "CREATE DATABASE IF NOT EXISTS sales");
    exec_sql(&env, "CREATE DATABASE IF NOT EXISTS analytics");
    exec_sql(&env, "CREATE TABLE ice.sales.fact (id INT, region STRING, amount BIGINT)");
    exec_sql(&env, "INSERT INTO ice.sales.fact VALUES (1, 'east', 10)");
    exec_sql(
        &env,
        "CREATE MATERIALIZED VIEW mv_fact \
         DISTRIBUTED BY HASH(region) BUCKETS 1 \
         PROPERTIES('storage_engine'='iceberg') \
         AS SELECT region, count(*) AS c FROM ice.sales.fact GROUP BY region",
    );
    exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_fact");
    exec_sql(&env, "INSERT INTO ice.sales.fact VALUES (2, 'west', 5)");
    exec_sql(&env, "REFRESH MATERIALIZED VIEW mv_fact");
    let before = mv_table_snapshot_count(&env, "analytics", "mv_fact");

    exec_sql(
        &env,
        "ALTER TABLE ice.analytics.mv_fact SET TBLPROPERTIES(\
         'history.expire.max-snapshot-age-ms'='1',\
         'novarocks.maintenance.enabled'='false')",
    );
    let mut coordinator = coordinator_with(|_| {});
    let mut executor = StateMaintenanceExecutor::new(Arc::clone(&env.state));
    coordinator
        .run_pass(&env.state, &mut executor, now_ms())
        .expect("maintenance pass");

    let after = mv_table_snapshot_count(&env, "analytics", "mv_fact");
    assert_eq!(before, after, "disabled table must not be touched");
}
```

- [ ] **Step 5: 跑全部集成测试**

Run: `cargo test --lib engine::mv_maintenance`
Expected: 全部 PASS。常见修正点:SQL 形态(CREATE TABLE/三段名)、MV-on-MV 名称解析、ALTER TABLE SET TBLPROPERTIES 语法 —— 全部以 `src/engine/mv/iceberg_refresh.rs` 与 `src/engine/statement.rs` 现状为准,**不得为测试通过而改动生产语义**。

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv_maintenance/
git commit -m "test(mv-maintenance): end-to-end auto expire/optimize/escape-hatch scenarios"
```

---

### Task 11: 收尾 — fmt/clippy/全量回归 + 文档状态

- [ ] **Step 1: 质量门**

```bash
cargo fmt
cargo clippy --all-targets 2>&1 | tail -20   # 新增告警须清零(允许既有告警)
cargo test --lib
```

Expected: fmt 无 diff、clippy 对新模块零告警、全量 lib 测试 PASS。

- [ ] **Step 2: 编译三种 profile 烟囱验证**

```bash
cargo build && cargo build --profile dev-opt
```

Expected: 均成功。

- [ ] **Step 3: 手动冒烟(可选但推荐)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
# 另一终端:建 MV、refresh 数次,观察日志中的
# "auto maintenance action completed" / "auto maintenance action skipped"
```

- [ ] **Step 4: 最终 commit(如有零散修正)**

```bash
git add -A
git commit -m "chore(mv-maintenance): fmt/clippy cleanups"
```

---

## Self-Review 结论(计划作者自查)

- **Spec 覆盖**:§4 架构(Task 7/8/9)、§5 策略与修正规则(Task 4/5)、§6 配置(Task 2)+ 白名单(Task 3)、§7 执行/失败/熔断(Task 7/8)、§8 日志(Task 7 process_table)、§9.1 前置验证(Task 1)、§9.2-9.5 边界(Task 8 候选过滤/Task 4 no-op 冷却/job 互斥)、§10 测试三层(Task 4 单测 / Task 7 生命周期 / Task 10 集成)。无遗漏。
- **已知偏差(均已回写 spec)**:e2e 用 Rust 确定性集成测试取代计时敏感的 sql-tests;snapshot 短路只作用于 compaction 信号;事件载荷简化为无参 `Wake`(coordinator 全量评估 + 每表短路,语义等价于 spec 的 `RefreshCompleted { mv_id }`,实现更简单)。
- **类型一致性**:`MaintenanceSignal`/`MaintenanceTarget`/`OptimizeSubmission`/`ActionKind`/`MaintenanceAction`/`SkipReason`/`TableRuntimeState`/`TableMaintenanceStats` 各任务间名称已核对一致;`ExpireOutcome`/`RewritePositionDeleteOutcome` 用现有 commit 模块的真实类型。
- **风险提示**:Task 10 的 SQL 形态(hadoop catalog 下 CREATE TABLE / MV-on-MV 名称解析)以现有测试为准可能需小调;`StoredMvDefinition` 全字段构造在 Task 5 测试里若编译报字段不符,按 `src/meta/repository/mv.rs` 现状增删。
