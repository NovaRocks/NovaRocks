# Iceberg MV Partition P3a Thresholds Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first P3 degradation threshold for Iceberg MV affected-partition pruning.

**Architecture:** Store MV refresh pruning thresholds in standalone config, copy them into `StandaloneState`, and pass them into each refresh context and merge sink plan. Consumers keep correctness unchanged: when an allow-list exceeds `max_affected_partitions`, they drop only partition pruning and continue scanning without the allow-list.

**Tech Stack:** Rust, serde TOML config, Iceberg MV refresh context, Iceberg merge sink, cargo unit tests.

---

### Task 1: Config And Limit Object

**Files:**
- Modify: `src/common/app_config.rs`
- Modify: `src/engine/mv/refresh_context.rs`
- Modify: `src/engine/mod.rs`

- [x] **Step 1: Write failing config tests**

Add tests that expect `[standalone_server]` defaults of `mv_refresh_max_touched_groups = 100000` and `mv_refresh_max_affected_partitions = 4096`, plus TOML overrides.

- [x] **Step 2: Add config fields and defaults**

Add `mv_refresh_max_touched_groups` and `mv_refresh_max_affected_partitions` to `StandaloneServerConfig`, with serde default functions and `Default` initialization.

- [x] **Step 3: Add runtime limit object**

Add `MvRefreshPruningLimits` with defaults matching config, plus `from_standalone_config`.

- [x] **Step 4: Store limits in standalone state**

Resolve limits during `StandaloneNovaRocks::open_body` and store them in `StandaloneState`.

### Task 2: Apply Affected Partition Thresholds

**Files:**
- Modify: `src/engine/mv/refresh_context.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mv/iceberg_merge_sink.rs`

- [x] **Step 1: Write failing target-state test**

Extend the refresh-context unit test so a known allow-list larger than `max_affected_partitions` returns `None`.

- [x] **Step 2: Write failing merge-sink test**

Add a merge-sink test so batch-local partition derivation larger than `max_affected_partitions` returns `TargetPartitionFilter::None`.

- [x] **Step 3: Apply threshold in refresh context**

In `target_state_partition_allow_list`, return `None` and log `fallback_reason = "affected_partition_threshold"` when the known partition count exceeds the configured maximum.

- [x] **Step 4: Apply threshold in merge sink**

Pass `MvRefreshPruningLimits` into `IcebergMergeSinkPlan` and `delete_batch_partition_filter`; when either plan-time or batch-derived allow-list exceeds the configured maximum, return `TargetPartitionFilter::None`.

### Task 3: Verification

**Files:**
- Test only.

- [x] **Step 1: Format**

Run `cargo fmt`.

- [x] **Step 2: Run targeted tests**

Run:

```bash
cargo test --lib common::app_config::tests::standalone_server_config_mv_refresh_pruning_defaults
cargo test --lib common::app_config::tests::standalone_server_config_mv_refresh_pruning_overrides
cargo test --lib engine::mv::refresh_context::tests::target_state_partition_allow_list_respects_threshold
cargo test --lib engine::mv::iceberg_merge_sink::tests::delete_batch_partition_filter_drops_batch_allow_list_over_threshold
cargo test --lib engine::mv::iceberg_merge_sink
cargo test --lib engine::mv::refresh_context
```

- [x] **Step 3: Diff checks**

Run `git diff --check` and inspect `git diff --stat`.
