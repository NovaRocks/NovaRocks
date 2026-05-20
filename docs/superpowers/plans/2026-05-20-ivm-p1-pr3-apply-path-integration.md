# IVM-P1 PR3 — Apply path integration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch the production aggregate / join aggregate apply path over to the partition-aware lookup landed by PR1 and the partition-derivation landed by PR2. After PR3, `apply_iceberg_aggregate_delta_chunks` (1) computes touched group row ids from the delta chunks, (2) derives the affected target partitions via `partition::aggregate_delta::derive_from_aggregate_delta`, (3) converts the result to a `TargetPartitionFilter`, (4) calls `iceberg_aggregate_state::load_touched_aggregate_target_state` (replacing `load_current_aggregate_target_state`), (5) reuses the existing merge, and (6) hands the same `TargetPartitionFilter` to the target locator. SQL regression cases and tracing observability fields land in PR4.

**Architecture:** Thread `schema_contract: &MvSchemaContract` through the two incremental refresh functions (`incremental_refresh_iceberg_aggregate_mv`, `incremental_refresh_iceberg_join_aggregate_mv`) — both already have `mv_definition` in scope and the schema contract is reachable from the outer `refresh_*_aggregate_iceberg_mv` callers. Add a private helper `build_aggregate_target_partition_filter(layout, schema_contract, delta_chunks)` in `iceberg_refresh.rs` that calls PR2's derivation and projects `AffectedAggregateTargetPartitions` → `TargetPartitionFilter`. Inside `apply_iceberg_aggregate_delta_chunks`, hoist `touched_row_ids = delta_row_ids(layout, delta_chunks)` before merge, build the filter, drive `load_touched_aggregate_target_state` with the filter, and pass the filter to `locate_target_rows_by_string_apply_key`. Error messages gain `target.{catalog,namespace,table}` and `mv_definition.mv_id` for production diagnosability. Existing staging-branch / commit / publish lifecycle is unchanged.

**Tech Stack:** Rust, NovaRocks MV refresh stack, Iceberg-rust 0.9, `cargo test --lib` for fast unit tests. Integration SQL coverage is intentionally deferred to PR4.

---

## File Structure

- Modify: `src/engine/mv/iceberg_refresh.rs`
  - Thread `schema_contract: &MvSchemaContract` through `incremental_refresh_iceberg_aggregate_mv`, `incremental_refresh_iceberg_join_aggregate_mv`, and `apply_iceberg_aggregate_delta_chunks`.
  - Add private helper `build_aggregate_target_partition_filter(layout, schema_contract, delta_chunks) -> Result<(TargetPartitionFilter, BTreeSet<String>), String>` — note the helper returns BOTH the filter and the precomputed touched row id set so the apply path does not walk the delta twice.
  - Inside `apply_iceberg_aggregate_delta_chunks`:
    - call the helper above
    - swap `load_current_aggregate_target_state(...)` for `load_touched_aggregate_target_state(target_table, layout, schema_contract, &touched_row_ids, &partition_filter)`
    - add `&partition_filter` to the existing `locate_target_rows_by_string_apply_key(...)` call
    - prefix mv-id / target-fqn into newly introduced error messages

PR3 does NOT touch:
- `src/engine/mv/iceberg_aggregate_state.rs` (already exposes both `load_current_*` and `load_touched_*`; PR3 just swaps the caller).
- `src/engine/mv/partition/` (already final from PR1+PR2).
- `src/engine/mv/iceberg_target_apply.rs` (PR1 already accepts `partition_filter`).
- The non-aggregate locator callers in `iceberg_join_coalesce.rs` / `iceberg_merge_sink.rs` (out of scope; they keep `TargetPartitionFilter::None`).

`load_current_aggregate_target_state` and its `_async` form are NOT removed in this PR. Some tests still reference them as a baseline. A follow-up cleanup PR can drop them once integration tests in PR4 confirm the new path is correct.

---

## Task 1: Thread `schema_contract` through the two incremental refresh functions

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

This task is signature-only. No behavior changes. Existing tests must still pass at the end. The goal is to get `&MvSchemaContract` from `refresh_*_aggregate_iceberg_mv` (where it is already in scope) all the way down to `apply_iceberg_aggregate_delta_chunks`.

- [ ] **Step 1: Add `schema_contract` to `incremental_refresh_iceberg_aggregate_mv` and `apply_iceberg_aggregate_delta_chunks`**

In `src/engine/mv/iceberg_refresh.rs`:

Find `fn incremental_refresh_iceberg_aggregate_mv(` (around line 1693). Add a new parameter `schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,` immediately after `mv_definition: &StoredMvDefinition,`.

Find `fn apply_iceberg_aggregate_delta_chunks(` (around line 1855). Add the same `schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,` parameter immediately after `mv_definition: &StoredMvDefinition,`.

- [ ] **Step 2: Update the call site in `refresh_single_aggregate_iceberg_mv` to pass `schema_contract`**

Find `incremental_refresh_iceberg_aggregate_mv(` invocation (around line 1672). The function already has `schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,` in its own signature. Pass it through:

```rust
        Some(prev) => incremental_refresh_iceberg_aggregate_mv(
            state,
            target,
            target_entry,
            iceberg_catalog,
            target_table,
            expected_main_snapshot_id,
            current_catalog,
            current_database,
            mv_definition,
            schema_contract,        // ← new
            base_ref,
            prev,
            current,
            &loaded,
            aggregate_shape,
            &pin,
        ),
```

- [ ] **Step 3: Update the in-function call to `apply_iceberg_aggregate_delta_chunks` in `incremental_refresh_iceberg_aggregate_mv`**

Find the call (around line 1839) and add `schema_contract` after `mv_definition`:

```rust
    apply_iceberg_aggregate_delta_chunks(
        state,
        target,
        target_entry,
        iceberg_catalog,
        target_table,
        expected_main_snapshot_id,
        mv_definition,
        schema_contract,             // ← new
        &layout,
        &delta_chunks,
        pin.to_snapshot_map(),
        pin.to_table_uuid_map(),
    )
```

- [ ] **Step 4: Same change in `incremental_refresh_iceberg_join_aggregate_mv` and its caller**

Find `fn incremental_refresh_iceberg_join_aggregate_mv(` (around line 2246) and add `schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,` immediately after `mv_definition`.

Find its call site inside `refresh_join_aggregate_iceberg_mv` (search for `incremental_refresh_iceberg_join_aggregate_mv(`). That outer function already takes `schema_contract`; pass it through to the inner one.

Find the `apply_iceberg_aggregate_delta_chunks(` call inside `incremental_refresh_iceberg_join_aggregate_mv` (around line 2347) and add `schema_contract`:

```rust
    apply_iceberg_aggregate_delta_chunks(
        state,
        target,
        target_entry,
        iceberg_catalog,
        target_table,
        expected_main_snapshot_id,
        mv_definition,
        schema_contract,             // ← new
        &layout,
        &delta_chunks,
        pin.to_snapshot_map(),
        pin.to_table_uuid_map(),
    )
```

- [ ] **Step 5: Run focused tests**

```bash
cd /Users/harbor/.codex/worktrees/2185/NovaRocks
cargo test --lib --no-run
```

Expected: compile succeeds. Existing aggregate refresh tests are unchanged in behavior — the parameter is unused inside `apply_iceberg_aggregate_delta_chunks` for now (allowed because Task 2 immediately uses it).

If clippy / compiler emits `unused variable: schema_contract` inside `apply_iceberg_aggregate_delta_chunks`, leave it for Task 2 to address (Task 2 will consume it). To keep the build clean during Task 1, prefix the parameter with `_` temporarily IF clippy errors out:

```rust
fn apply_iceberg_aggregate_delta_chunks(
    ...
    _schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    ...
```

Task 2 will rename it back to `schema_contract` when it starts being used.

- [ ] **Step 6: Run the broader aggregate refresh tests**

```bash
cargo test --lib engine::mv::iceberg_refresh
```

Expected: all pre-existing aggregate refresh tests still pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "refactor: thread schema_contract into iceberg aggregate apply path"
```

---

## Task 2: Add `build_aggregate_target_partition_filter` and use it in apply path

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

This task introduces the new helper and consumes it. After Task 2 the apply path computes touched_row_ids once, derives affected partitions, builds a `TargetPartitionFilter`, replaces `load_current_aggregate_target_state` with `load_touched_aggregate_target_state`, and forwards the filter to the locator.

- [ ] **Step 1: Write failing tests for `build_aggregate_target_partition_filter`**

Append to the existing `#[cfg(test)] mod tests` block in `src/engine/mv/iceberg_refresh.rs` (or a dedicated `mod aggregate_apply_tests` if the existing tests live elsewhere; in either case, place the new tests close to the other aggregate refresh tests so they share fixtures).

```rust
    #[test]
    fn build_aggregate_target_partition_filter_returns_allow_list_for_partitioned_contract() {
        use crate::engine::mv::partition::{
            MvPartitionKey, MvPartitionKeyField, MvPartitionValue, TargetPartitionFilter,
        };
        // Reuse PR2 derivation test fixtures (count_layout_with_group_key /
        // count_contract_with_partition / batch_with_group_key). If those are
        // private to partition::aggregate_delta::tests, copy the minimal fixture
        // here — DO NOT make them pub(crate) just for this test.
        let layout = aggregate_apply_test_helpers::count_layout("region");
        let contract = aggregate_apply_test_helpers::count_contract_with_identity_partition(
            "region", 11,
        );
        let chunk = aggregate_apply_test_helpers::batch_with_group_key(
            "region",
            arrow::datatypes::DataType::Utf8,
            std::sync::Arc::new(arrow::array::StringArray::from(vec![Some("a"), Some("b")]))
                as arrow::array::ArrayRef,
        );
        let (filter, touched) =
            build_aggregate_target_partition_filter(&layout, &contract, &[chunk]).expect("filter");
        match filter {
            TargetPartitionFilter::AllowList(set) => {
                let keys: Vec<_> = set.iter().cloned().collect();
                let want: Vec<_> = ["a", "b"]
                    .iter()
                    .map(|v| {
                        MvPartitionKey::new(
                            7,
                            vec![MvPartitionKeyField::new(
                                "region".to_string(),
                                MvPartitionValue::String((*v).to_string()),
                            )],
                        )
                    })
                    .collect();
                assert_eq!(keys, want);
            }
            other => panic!("expected AllowList, got {other:?}"),
        }
        assert_eq!(touched.len(), 2);
    }

    #[test]
    fn build_aggregate_target_partition_filter_returns_none_for_unpartitioned_contract() {
        use crate::engine::mv::partition::TargetPartitionFilter;
        let layout = aggregate_apply_test_helpers::count_layout("region");
        let mut contract = aggregate_apply_test_helpers::count_contract_with_identity_partition(
            "region", 11,
        );
        contract.target.partition = None;
        let chunk = aggregate_apply_test_helpers::batch_with_group_key(
            "region",
            arrow::datatypes::DataType::Utf8,
            std::sync::Arc::new(arrow::array::StringArray::from(vec![Some("a")]))
                as arrow::array::ArrayRef,
        );
        let (filter, touched) =
            build_aggregate_target_partition_filter(&layout, &contract, &[chunk]).expect("filter");
        assert!(matches!(filter, TargetPartitionFilter::None));
        assert_eq!(touched.len(), 1);
    }

    #[test]
    fn build_aggregate_target_partition_filter_propagates_derivation_error_with_field_name() {
        let layout = aggregate_apply_test_helpers::count_layout("region");
        let contract = aggregate_apply_test_helpers::count_contract_with_void_partition(
            "region", 11,
        );
        let chunk = aggregate_apply_test_helpers::batch_with_group_key(
            "region",
            arrow::datatypes::DataType::Utf8,
            std::sync::Arc::new(arrow::array::StringArray::from(vec![Some("a")]))
                as arrow::array::ArrayRef,
        );
        let err = build_aggregate_target_partition_filter(&layout, &contract, &[chunk])
            .unwrap_err();
        assert!(err.contains("region"), "{err}");
        assert!(err.contains("void"), "{err}");
    }
```

Then add a small local `mod aggregate_apply_test_helpers` inside the `#[cfg(test)] mod tests` block holding the three fixture functions referenced above. The fixtures are small adaptations of PR2's `partition::aggregate_delta::tests` helpers — they live in this module so the iceberg_refresh tests are self-contained without pubbing aggregate_delta's helpers. Place this helper module ABOVE the three new tests:

```rust
    mod aggregate_apply_test_helpers {
        use crate::connector::starrocks::managed::ddl::managed_physical_column;
        use crate::connector::starrocks::managed::mv_agg_state::{
            AggregateMvLayout, AggregateStateColumn, AggregateStateRole, AggregateVisibleColumn,
        };
        use crate::connector::starrocks::managed::mv_shape::AggregateFunctionKind;
        use crate::exec::chunk::Chunk;
        use crate::meta::repository::mv_contract::{
            ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
            ExpressionLineage, HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
            MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
            TargetContract, TargetVisibleColumn,
        };
        use crate::sql::parser::ast::SqlType;
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        pub(super) fn count_layout(group_key: &str) -> AggregateMvLayout {
            let row_id =
                managed_physical_column("__row_id__".to_string(), SqlType::String, false, false, true);
            let group =
                managed_physical_column(group_key.to_string(), SqlType::String, true, true, false);
            let counter =
                managed_physical_column("c".to_string(), SqlType::BigInt, false, true, false);
            let state = managed_physical_column(
                "__agg_state_c".to_string(),
                SqlType::BigInt,
                false,
                false,
                false,
            );
            AggregateMvLayout {
                row_id_column: row_id.clone(),
                visible_columns: vec![
                    AggregateVisibleColumn {
                        name: group_key.to_string(),
                        data_type: DataType::Utf8,
                        sql_type: SqlType::String,
                        nullable: true,
                        source_index: 0,
                    },
                    AggregateVisibleColumn {
                        name: "c".to_string(),
                        data_type: DataType::Int64,
                        sql_type: SqlType::BigInt,
                        nullable: false,
                        source_index: 1,
                    },
                ],
                state_columns: vec![AggregateStateColumn {
                    name: "__agg_state_c".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: false,
                    visible_source_index: 1,
                    aggregate_index: 0,
                    function: AggregateFunctionKind::Count,
                    state_role: AggregateStateRole::Single,
                    count_star: true,
                }],
                group_key_source_indexes: vec![0],
                physical_columns: vec![row_id, group, counter, state],
            }
        }

        pub(super) fn count_contract_with_identity_partition(
            partition_field_name: &str,
            source_target_field_id: i32,
        ) -> MvSchemaContract {
            count_contract_with_transform(
                partition_field_name,
                source_target_field_id,
                MvPartitionTransformContract::Identity,
            )
        }

        pub(super) fn count_contract_with_void_partition(
            partition_field_name: &str,
            source_target_field_id: i32,
        ) -> MvSchemaContract {
            count_contract_with_transform(
                partition_field_name,
                source_target_field_id,
                MvPartitionTransformContract::Void,
            )
        }

        fn count_contract_with_transform(
            partition_field_name: &str,
            source_target_field_id: i32,
            transform: MvPartitionTransformContract,
        ) -> MvSchemaContract {
            MvSchemaContract {
                contract_version: 1,
                base: BaseContract {
                    table_fqn: "ice.sales.orders".to_string(),
                    table_uuid: "base-uuid".to_string(),
                    alias_at_create: None,
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot {
                        fields: vec![BaseFieldRecord {
                            field_id: 1,
                            name_at_create: "region".to_string(),
                            type_signature: "string".to_string(),
                            required: true,
                        }],
                    },
                },
                bases: Vec::new(),
                output: OutputContract {
                    columns: vec![
                        OutputColumnLineage {
                            expression: ExpressionLineage {
                                kind: ExpressionKind::Column,
                                referenced_base_field_ids: vec![1],
                                referenced_base_fields: Vec::new(),
                            },
                        },
                        OutputColumnLineage {
                            expression: ExpressionLineage {
                                kind: ExpressionKind::Column,
                                referenced_base_field_ids: Vec::new(),
                                referenced_base_fields: Vec::new(),
                            },
                        },
                    ],
                    filter: None,
                },
                join: None,
                aggregate: None,
                target: TargetContract {
                    table_fqn: "ice.analytics.mv_orders".to_string(),
                    table_uuid: "target-uuid".to_string(),
                    schema_id_at_create: 0,
                    visible_columns: vec![
                        TargetVisibleColumn {
                            output_name: partition_field_name.to_string(),
                            target_field_id: source_target_field_id,
                            type_signature: "string".to_string(),
                            nullable: true,
                        },
                        TargetVisibleColumn {
                            output_name: "c".to_string(),
                            target_field_id: 12,
                            type_signature: "bigint".to_string(),
                            nullable: false,
                        },
                    ],
                    hidden_apply_key: HiddenApplyKeyContract {
                        column_name: "__row_id__".to_string(),
                        target_field_id: 10,
                        source: ApplyKeySource::GroupRowId,
                    },
                    partition: Some(MvPartitionContract {
                        target_spec_id: 7,
                        fields: vec![MvPartitionFieldContract {
                            partition_field_id: 100,
                            partition_field_name: partition_field_name.to_string(),
                            source_target_field_id,
                            source_column_name: partition_field_name.to_string(),
                            transform,
                        }],
                    }),
                },
            }
        }

        pub(super) fn batch_with_group_key(name: &str, dt: DataType, values: ArrayRef) -> Chunk {
            let n = values.len();
            let row_ids: Vec<String> = (0..n).map(|i| format!("rid-{i}")).collect();
            let row_id_arr: ArrayRef = Arc::new(StringArray::from(row_ids));
            let counts: ArrayRef = Arc::new(Int64Array::from(vec![1i64; n]));
            let states: ArrayRef = Arc::new(Int64Array::from(vec![1i64; n]));
            let schema = Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new(name, dt, true),
                Field::new("c", DataType::Int64, false),
                Field::new("__agg_state_c", DataType::Int64, false),
            ]));
            let batch =
                RecordBatch::try_new(schema, vec![row_id_arr, values, counts, states]).unwrap();
            crate::engine::record_batch_to_chunk(batch).unwrap()
        }
    }
```

- [ ] **Step 2: Run failing tests**

```bash
cd /Users/harbor/.codex/worktrees/2185/NovaRocks
cargo test --lib engine::mv::iceberg_refresh::tests::build_aggregate_target_partition_filter
```

Expected: compile fails because `build_aggregate_target_partition_filter` does not exist.

- [ ] **Step 3: Implement the helper and integrate it into `apply_iceberg_aggregate_delta_chunks`**

In `src/engine/mv/iceberg_refresh.rs`, add the helper function. Place it directly above `apply_iceberg_aggregate_delta_chunks`:

```rust
fn build_aggregate_target_partition_filter(
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    delta_chunks: &[crate::exec::chunk::Chunk],
) -> Result<
    (
        crate::engine::mv::partition::TargetPartitionFilter,
        std::collections::BTreeSet<String>,
    ),
    String,
> {
    // Touched group row ids are common to derivation (which technically does
    // not need them) and to the downstream state lookup + locator. Compute
    // them here so the apply path does not walk the delta twice.
    let touched_row_ids = aggregate_delta_touched_row_ids(layout, delta_chunks)?;

    let derived = crate::engine::mv::partition::derive_from_aggregate_delta(
        &crate::engine::mv::partition::AggregateDeltaPartitionInput {
            layout,
            schema_contract,
            delta_chunks,
        },
    )
    .map_err(|err| err.to_string())?;

    let filter = match derived {
        crate::engine::mv::partition::AffectedAggregateTargetPartitions::Unpartitioned => {
            crate::engine::mv::partition::TargetPartitionFilter::None
        }
        crate::engine::mv::partition::AffectedAggregateTargetPartitions::Known { partitions } => {
            crate::engine::mv::partition::TargetPartitionFilter::AllowList(partitions)
        }
    };
    Ok((filter, touched_row_ids))
}

fn aggregate_delta_touched_row_ids(
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
    delta_chunks: &[crate::exec::chunk::Chunk],
) -> Result<std::collections::BTreeSet<String>, String> {
    use arrow::array::{Array, StringArray};

    let row_id_column = &layout.row_id_column.column.name;
    let mut row_ids = std::collections::BTreeSet::new();
    for chunk in delta_chunks {
        let schema = chunk.batch.schema();
        let row_id_index = schema.index_of(row_id_column).map_err(|e| {
            format!(
                "iceberg aggregate delta missing row id column `{row_id_column}`: {e}"
            )
        })?;
        let row_id_array = chunk
            .batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                format!(
                    "iceberg aggregate delta row id column `{row_id_column}` must be Utf8"
                )
            })?;
        for row in 0..row_id_array.len() {
            if row_id_array.is_null(row) {
                return Err(format!(
                    "iceberg aggregate delta row id column `{row_id_column}` cannot be NULL"
                ));
            }
            row_ids.insert(row_id_array.value(row).to_string());
        }
    }
    Ok(row_ids)
}
```

Now edit `apply_iceberg_aggregate_delta_chunks` (the function modified in Task 1). Find the existing block:

```rust
    let old_chunks =
        crate::engine::mv::iceberg_aggregate_state::load_current_aggregate_target_state(
            target_table,
            layout,
        )?;
    let merge = crate::engine::mv::iceberg_aggregate_state::merge_aggregate_target_state(
        layout,
        &old_chunks,
        delta_chunks,
    )?;
```

Replace it with:

```rust
    let (partition_filter, touched_row_ids) =
        build_aggregate_target_partition_filter(layout, schema_contract, delta_chunks)?;
    let (old_chunks, _lookup_stats) =
        crate::engine::mv::iceberg_aggregate_state::load_touched_aggregate_target_state(
            target_table,
            layout,
            schema_contract,
            &touched_row_ids,
            &partition_filter,
        )?;
    let merge = crate::engine::mv::iceberg_aggregate_state::merge_aggregate_target_state(
        layout,
        &old_chunks,
        delta_chunks,
    )?;
```

(`_lookup_stats` keeps the structured stats reachable for PR4's tracing instrumentation without forcing PR3 to consume them.)

Find the existing locator call further down in the same function (around line 1962 pre-Task-1, shifted by ~6 lines after Task 1):

```rust
        let groups = match data_block_on(
            crate::engine::mv::iceberg_target_apply::locate_target_rows_by_string_apply_key(
                &target_table,
                ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
                &delete_row_ids,
                &existing_deletes_by_file,
                &referenced_data_file_partitions,
                &crate::engine::mv::partition::TargetPartitionFilter::None,
            ),
        ) {
```

Replace `&crate::engine::mv::partition::TargetPartitionFilter::None` with `&partition_filter`:

```rust
        let groups = match data_block_on(
            crate::engine::mv::iceberg_target_apply::locate_target_rows_by_string_apply_key(
                &target_table,
                ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
                &delete_row_ids,
                &existing_deletes_by_file,
                &referenced_data_file_partitions,
                &partition_filter,
            ),
        ) {
```

If you had to rename the Task 1 parameter to `_schema_contract` to keep the build clean during Task 1, rename it back to `schema_contract` here.

- [ ] **Step 4: Run the helper tests + broader aggregate refresh tests**

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::build_aggregate_target_partition_filter
cargo test --lib engine::mv::iceberg_refresh
```

Expected: the three new helper tests pass; all pre-existing iceberg_refresh tests still pass (the algorithmic change is invisible to existing test fixtures because PR2's derivation returns `Unpartitioned` for unpartitioned contracts and the row-id filter is a strict subset of the old full-scan behavior).

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat: wire aggregate apply through partition-pruned touched-group lookup"
```

---

## Task 3: Add error context (mv id + target fqn) to apply-path failures

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

Spec §9 requires error messages to include `mv id` and `target fqn` so refresh failures are diagnosable in production logs. PR3 introduces two new error sites — the partition filter build and the touched-state lookup — and inherits one from the locator. This task wraps those three error returns with mv id / target fqn context.

- [ ] **Step 1: Write a failing test**

Append to `#[cfg(test)] mod tests` block in `src/engine/mv/iceberg_refresh.rs` (near the Task 2 tests):

```rust
    #[test]
    fn aggregate_apply_error_message_includes_mv_id_and_target_fqn() {
        let layout = aggregate_apply_test_helpers::count_layout("region");
        let contract = aggregate_apply_test_helpers::count_contract_with_void_partition(
            "region", 11,
        );
        let chunk = aggregate_apply_test_helpers::batch_with_group_key(
            "region",
            arrow::datatypes::DataType::Utf8,
            std::sync::Arc::new(arrow::array::StringArray::from(vec![Some("a")]))
                as arrow::array::ArrayRef,
        );
        let target_fqn = "ice.analytics.mv_orders";
        let mv_id = 4242i64;
        let err = wrap_aggregate_apply_error(
            target_fqn,
            mv_id,
            build_aggregate_target_partition_filter(&layout, &contract, &[chunk]).err().unwrap(),
        );
        assert!(err.contains("mv_id=4242"), "{err}");
        assert!(err.contains(target_fqn), "{err}");
        assert!(err.contains("void"), "{err}"); // original cause preserved
    }
```

- [ ] **Step 2: Run the failing test**

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::aggregate_apply_error_message_includes_mv_id_and_target_fqn
```

Expected: compile fails because `wrap_aggregate_apply_error` does not exist.

- [ ] **Step 3: Add the wrapper and use it in `apply_iceberg_aggregate_delta_chunks`**

Add a small helper near `build_aggregate_target_partition_filter`:

```rust
fn wrap_aggregate_apply_error(target_fqn: &str, mv_id: i64, cause: String) -> String {
    format!(
        "iceberg aggregate MV apply failed (target={target_fqn}, mv_id={mv_id}): {cause}"
    )
}

fn target_fqn_string(target: &IcebergMvTarget) -> String {
    format!("{}.{}.{}", target.catalog, target.namespace, target.table)
}
```

In `apply_iceberg_aggregate_delta_chunks`, after the existing `if delta_chunks.iter().all(...) { return finalize_iceberg_mv_metadata_only_refresh(...) }` short-circuit, bind:

```rust
    let target_fqn = target_fqn_string(target);
    let mv_id = mv_definition.mv_id;
```

Then wrap the three new / changed error returns:

1. `build_aggregate_target_partition_filter` call:

```rust
    let (partition_filter, touched_row_ids) =
        build_aggregate_target_partition_filter(layout, schema_contract, delta_chunks)
            .map_err(|e| wrap_aggregate_apply_error(&target_fqn, mv_id, e))?;
```

2. `load_touched_aggregate_target_state` call:

```rust
    let (old_chunks, _lookup_stats) =
        crate::engine::mv::iceberg_aggregate_state::load_touched_aggregate_target_state(
            target_table,
            layout,
            schema_contract,
            &touched_row_ids,
            &partition_filter,
        )
        .map_err(|e| wrap_aggregate_apply_error(&target_fqn, mv_id, e))?;
```

3. The locator call: it already wraps errors through `handle_iceberg_mv_commit_error`, which already includes target context. Do NOT double-wrap there.

Other pre-existing error returns (commit, publish, staging branch) are out of scope for this task — they already pass through `handle_iceberg_mv_commit_error` / `handle_iceberg_mv_definite_pre_publish_error`.

- [ ] **Step 4: Run the tests**

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::aggregate_apply_error_message_includes_mv_id_and_target_fqn
cargo test --lib engine::mv::iceberg_refresh
```

Expected: the new test passes and all pre-existing iceberg_refresh tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat: include mv id and target fqn in aggregate apply errors"
```

---

## Task 4: Final verification

**Files:**
- No new source files. This task verifies the full PR3 surface.

- [ ] **Step 1: Format**

```bash
cd /Users/harbor/.codex/worktrees/2185/NovaRocks
cargo fmt
```

Expected: exit 0.

- [ ] **Step 2: Lint (PR3-touched file only)**

```bash
cargo clippy --all-targets --no-deps 2>&1 | grep -E "(error|warning):.*iceberg_refresh\.rs" || echo "OK"
```

Expected: `OK` (any pre-existing warnings on unrelated files are not Task 4's responsibility).

- [ ] **Step 3: Aggregate refresh + partition tests**

```bash
cargo test --lib engine::mv::iceberg_refresh
cargo test --lib engine::mv::partition
cargo test --lib engine::mv::iceberg_aggregate_state
```

Expected: all suites pass.

- [ ] **Step 4: Full library compile**

```bash
cargo test --lib --no-run
```

Expected: compile succeeds.

- [ ] **Step 5: Diff hygiene**

```bash
git diff --check
```

Expected: empty.

- [ ] **Step 6: Commit fmt-only changes if any**

```bash
git status -sb
```

If changes exist:

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "style: format aggregate apply path integration"
```

Otherwise skip.

---

## Self-Review

**Spec coverage** (spec §13 PR3 deliverables → plan Task):

| Spec deliverable | Plan Task |
|---|---|
| `build_aggregate_target_partition_filter` helper | Task 2 |
| Replace `load_current_aggregate_target_state` with `load_touched_aggregate_target_state` | Task 2 |
| Locator call accepts the same `TargetPartitionFilter` as state lookup | Task 2 |
| Error info adds `mv id / target fqn` | Task 3 |
| Metadata-only short-circuit preserved | Task 2 (unchanged code path) |
| Existing staging branch / commit / publish lifecycle untouched | Task 2 (only the load + locator filter calls change) |
| §12.2 integration tests (Rust) | DEFERRED to PR4 — PR3's TDD coverage is unit-level (helper + error wrap); SQL integration is the PR4 deliverable. This deviation from the spec table is documented at the top of this plan. |

**Placeholder scan:** No `TBD`, `TODO`, `implement later`. The `_lookup_stats` binding is intentional — it preserves the typed return shape for PR4 to consume without forcing PR3 to add tracing scaffolding.

**Type consistency:**
- `build_aggregate_target_partition_filter` signature (`layout`, `schema_contract`, `delta_chunks`) and return shape (`(TargetPartitionFilter, BTreeSet<String>)`) match between Task 2 definition and Task 2 caller inside `apply_iceberg_aggregate_delta_chunks`.
- `schema_contract: &MvSchemaContract` threading is consistent across `incremental_refresh_iceberg_aggregate_mv`, `incremental_refresh_iceberg_join_aggregate_mv`, and `apply_iceberg_aggregate_delta_chunks` (all three add the same-named parameter immediately after `mv_definition`).
- `wrap_aggregate_apply_error(target_fqn: &str, mv_id: i64, cause: String) -> String` and `target_fqn_string(&IcebergMvTarget) -> String` signatures match between Task 3 definition and use sites.
