# Iceberg MV Partition P2-b Join PF Locator Pruning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let join projection/filter MV refreshes prune delete-side target-row locator scans by deriving a batch-local partition allow-list from the signed DELETE rows reaching the merge sink.

**Architecture:** PR #291 already threads plan-time `affected_partitions` into `IcebergMergeSinkPlan.partition_filter`; join PF remains `NotDerived` at planning time because the correct partition values come from the rewritten join delta stream. This plan adds a generic target-visible-column binder/evaluator in `partition::derivation`, then lets the merge sink refine `TargetPartitionFilter::None` into a per-delete-batch `AllowList` when the schema contract has a derivable target partition spec. If the batch cannot be evaluated, refresh fails with the existing partition derivation error instead of silently applying an unsafe partial filter.

**Tech Stack:** Rust, Arrow `RecordBatch`, existing `AffectedTargetPartitions` / `TargetPartitionFilter`, Iceberg MV merge sink and locator.

---

### Task 1: target-visible-column binder + RecordBatch evaluator

**Files:**
- Modify: `src/engine/mv/partition/derivation.rs`
- Modify: `src/engine/mv/partition/mod.rs`

- [ ] **Step 1: Write the failing tests**

Add tests in `src/engine/mv/partition/derivation.rs`:

```rust
#[test]
fn bind_spec_to_target_visible_columns_uses_target_output_names() {
    let contract =
        count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
    let spec = resolve_partition_derivation_spec(&contract)
        .expect("resolve")
        .expect("partitioned");
    let bound = bind_spec_to_target_visible_columns(&spec, &contract).expect("bind");
    assert_eq!(bound.len(), 1);
    assert_eq!(bound[0].partition_field_name, "region");
    assert_eq!(bound[0].column_name, "region");
}

#[test]
fn evaluate_partition_spec_record_batch_dedupes_delete_rows() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let contract =
        count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
    let spec = resolve_partition_derivation_spec(&contract)
        .expect("resolve")
        .expect("partitioned");
    let bound = bind_spec_to_target_visible_columns(&spec, &contract).expect("bind");
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("region", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["west", "east", "west"]))],
    )
    .expect("batch");

    let partitions =
        evaluate_partition_spec_record_batch(spec.target_spec_id, &bound, &batch)
            .expect("evaluate");

    assert_eq!(partitions.into_iter().collect::<Vec<_>>(), vec![key("east"), key("west")]);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib engine::mv::partition::derivation -- --nocapture`
Expected: compile FAIL because `bind_spec_to_target_visible_columns` and `evaluate_partition_spec_record_batch` do not exist.

- [ ] **Step 3: Implement**

Add `bind_spec_to_target_visible_columns(spec, contract)` that maps every `PartitionDerivationField::output_index` to `contract.target.visible_columns[output_index].output_name`, preserving `partition_field_name` and `transform`. Add `evaluate_partition_spec_record_batch(target_spec_id, bound_fields, batch)` with the same transform/row-to-key loop as `evaluate_partition_spec`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib engine::mv::partition::derivation`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/partition/derivation.rs src/engine/mv/partition/mod.rs docs/superpowers/plans/2026-06-12-iceberg-mv-partition-p2b-join-pf-locator-pruning.md
git commit -m "feat(mv): add target-visible partition derivation evaluator"
```

---

### Task 2: merge sink batch-local partition filter

**Files:**
- Modify: `src/engine/mv/iceberg_merge_sink.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Write failing merge-sink unit tests**

Add tests in `src/engine/mv/iceberg_merge_sink.rs` that construct a `RecordBatch` with a visible `region` column and assert:

```rust
#[test]
fn delete_batch_partition_filter_prefers_plan_time_allow_list() {
    let plan_filter = crate::engine::mv::partition::TargetPartitionFilter::AllowList(
        [partition_key("planned")].into_iter().collect(),
    );
    let batch = partition_batch(["batch"]);
    let filter = delete_batch_partition_filter(
        &plan_filter,
        Some(&bound_partition_derivation()),
        &batch,
    )
    .expect("filter");
    assert_eq!(filter, plan_filter);
}

#[test]
fn delete_batch_partition_filter_derives_batch_allow_list_when_plan_filter_is_none() {
    let batch = partition_batch(["west", "east", "west"]);
    let filter = delete_batch_partition_filter(
        &crate::engine::mv::partition::TargetPartitionFilter::None,
        Some(&bound_partition_derivation()),
        &batch,
    )
    .expect("filter");
    assert_eq!(
        filter,
        crate::engine::mv::partition::TargetPartitionFilter::AllowList(
            [partition_key("east"), partition_key("west")].into_iter().collect(),
        )
    );
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --lib engine::mv::iceberg_merge_sink -- --nocapture`
Expected: compile FAIL because `delete_batch_partition_filter` and the new plan field do not exist.

- [ ] **Step 3: Implement**

Add `partition_derivation: Option<BoundTargetPartitionDerivation>` to `IcebergMergeSinkPlan`, where the helper stores `target_spec_id` and `Vec<BoundPartitionField>`. Build it at the single plan construction site in `iceberg_refresh.rs` with `resolve_partition_derivation_spec` + `bind_spec_to_target_visible_columns`. In `handle_delete_batch`, call `delete_batch_partition_filter(&self.plan.partition_filter, self.plan.partition_derivation.as_ref(), &batch)` and pass the returned filter to all locator calls.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib engine::mv::iceberg_merge_sink`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_merge_sink.rs src/engine/mv/iceberg_refresh.rs
git commit -m "feat(mv): derive delete-side locator partition filter from join delta batches"
```

---

### Task 3: verification

- [ ] **Step 1: format + targeted tests**

```bash
cargo fmt
cargo test --lib engine::mv::partition::derivation
cargo test --lib engine::mv::iceberg_merge_sink
```

- [ ] **Step 2: broader verification**

```bash
cargo test --lib engine::mv
cargo clippy --lib -- -D warnings
```

If repository-wide clippy still reports the pre-existing warning pile noted in PR #291, run `cargo clippy --lib 2>&1 | grep -E "partition|iceberg_merge_sink|iceberg_refresh"` and require zero hits in touched code.

- [ ] **Step 3: SQL behavior lock**

Run `iceberg-ivm --mode verify` with the generated `docker/iceberg-rest/runtime/current/env.sh` config. Expected: same failed-case set as #291 after the rebase comment: 10 pre-existing failures, no new join/partition failure.
