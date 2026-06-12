# Iceberg MV Partition P3b Touched Groups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply `mv_refresh_max_touched_groups` to aggregate-state merge old-state pruning.

**Architecture:** Keep target-state file/partition pruning separate from in-memory old-state row pruning. `AggregateStateMergePlan` carries the same `MvRefreshPruningLimits` from the refresh context; when touched row ids exceed the threshold, the merge builds old aggregate state from all old chunks instead of first filtering by touched row ids, while output remains restricted to touched rows.

**Tech Stack:** Rust, Iceberg MV aggregate-state merge, direct exec plan, cargo unit tests.

---

### Task 1: Touched Group Threshold In Merge Core

**Files:**
- Modify: `src/engine/mv/iceberg_aggregate_state.rs`

- [x] **Step 1: Write failing merge-core test**

Add a test that calls a new `merge_aggregate_state_chunks_for_change_stream_with_pruning_limits` helper with `max_touched_groups = 1` and two touched row ids. Include an untouched old row with invalid aggregate state bytes; the test should expect an error, proving the over-threshold path decoded full old state instead of filtering old chunks first.

- [x] **Step 2: Add pruning-limit helper**

Add `MvRefreshPruningLimits::touched_group_count_exceeds_limit`.

- [x] **Step 3: Implement threshold branch**

When `touched_row_ids.len() > max_touched_groups`, pass `old_chunks.to_vec()` into `build_old_state_map`; otherwise keep the existing `filter_physical_chunks_by_row_ids` path.

### Task 2: Carry Limits Through The Operator

**Files:**
- Modify: `src/sql/codegen/mod.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/exec/operators/aggregate_state_merge.rs`

- [x] **Step 1: Add limit fields to direct exec and exec plan**

Carry `MvRefreshPruningLimits` through `DirectExecPlan::AggregateStateMerge` and `AggregateStateMergePlan`.

- [x] **Step 2: Wire codegen from refresh context**

Use `mv_refresh_ctx.map(|ctx| ctx.pruning_limits).unwrap_or_default()` when creating the direct exec plan.

- [x] **Step 3: Wire execution**

Have `AggregateStateMergeSourceOperator` pass the plan limits into the merge helper.

### Task 3: Verification

**Files:**
- Test only.

- [x] **Step 1: Format**

Run `cargo fmt`.

- [x] **Step 2: Run targeted tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_aggregate_state::tests::merge_over_touched_group_threshold_uses_full_old_state
cargo test --lib exec::operators::aggregate_state_merge
cargo test --lib sql::codegen::fragment_builder::tests::aggregate_state_merge_direct_codegen_wraps_delta_as_physical_input
```

- [x] **Step 3: Run MV module**

Run `cargo test --lib engine::mv`.
