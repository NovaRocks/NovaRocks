# Iceberg MV Partition P2-c: Union Branch Affected-Partition Merge

> **For harbor:** REQUIRED SUB-SKILL: Use test-driven-development to implement the plan task-by-task.

**Goal:** Make UNION ALL projection/filter and AllBasesRequired aggregate refresh plans merge per-base affected partition derivation results into a single target allow-list when every changed branch/base is derivable.

**Architecture:** PR #291 left UNION ALL plan-side affected partitions as `NotDerived`. The existing single-base planner can derive partitions from `plan_changes` file metadata. P2-c adds a multi-base planner wrapper that runs the same derivation per base for incremental refreshes, treats unchanged bases as an empty known set, and unions all known target partition keys. Any per-base `NotDerived` result preserves BestEffort semantics by returning one `NotDerived` reason instead of partially pruning. No execution rewrite or locator behavior changes are needed.

## Task 1: result merge helper

Add tests in `src/engine/mv/iceberg_refresh.rs`:

```rust
#[test]
fn merge_affected_partition_results_unions_known_sets() { ... }

#[test]
fn merge_affected_partition_results_preserves_first_not_derived_reason() { ... }
```

Run: `cargo test --lib engine::mv::iceberg_refresh::partition_planning_tests -- --nocapture`

Expected: compile FAIL because `merge_affected_partition_results` does not exist.

Implement `merge_affected_partition_results(context, results)`:

- `Known` results union their `BTreeSet<MvPartitionKey>`.
- first `NotDerived` returns `NotDerived("{context}: {base}: {reason}")`.
- all `Unpartitioned` returns `Unpartitioned`.
- mixed `Known` and `Unpartitioned` returns `NotDerived`.

## Task 2: multi-base planner wrapper

Add `plan_multi_base_affected_partitions(...)` near the single-base planner:

- `Noop` delegates to `noop_affected_partitions`.
- `Incremental` plans only bases where previous/current snapshots differ; unchanged bases contribute an empty known set.
- missing previous/current snapshot or missing loaded table returns `NotDerived` with the base FQN.
- `Full`/`Rebuild` stays BestEffort `NotDerived` for partitioned targets, `Unpartitioned` for unpartitioned targets.

Use it in:

- `plan_iceberg_union_projection_mv_refresh`
- `plan_iceberg_all_bases_aggregate_mv_refresh`

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::partition_planning_tests
cargo test --lib engine::mv::iceberg_refresh::tests::plan_iceberg_mv_refresh_reports_append_insert_affected_partitions
cargo test --lib engine::mv
```

## Task 3: final checks

Run:

```bash
cargo fmt
git diff --check
```

Then commit:

```bash
git add src/engine/mv/iceberg_refresh.rs docs/superpowers/plans/2026-06-12-iceberg-mv-partition-p2c-union-branch-merge.md
git commit -m "feat(mv): merge union branch affected partitions"
```
