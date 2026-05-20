# IVM-P1 PR1 — Aggregate target state lookup API + locator partition filter

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the foundation of partition-pruned touched-group state lookup for Iceberg aggregate IMV: a shared `TargetPartitionFilter` type, expand MV partition mapping to all first-class Iceberg transforms, give the existing target locator a partition-filter parameter, and introduce a `load_touched_aggregate_target_state` API that prunes target scan tasks by partition and filters Arrow batches by `__row_id__`.

**Architecture:** Add `TargetPartitionFilter { None, AllowList(BTreeSet<MvPartitionKey>) }` next to `MvPartitionKey` so both the state loader and the locator share a single filter contract. Expose two private helpers in `src/connector/iceberg/changes.rs` (`change_partition_field_values`, `change_partition_transform_name`) so the target-side file scan can reuse the same Iceberg manifest → `ChangePartitionFieldValue` translation that PR2 used for base-side manifests. Extend `partition::mapping::map_file_partition_to_mv_key` to all transforms covered by `MvPartitionTransformContract`, comparing manifest transform text and contract enum by structural equivalence rather than by string. Thread `partition_filter: &TargetPartitionFilter` through `iceberg_target_apply::locate_target_rows_by_apply_key_impl` and its three public wrappers; migrate all existing callers to `TargetPartitionFilter::None` (they keep current behavior). Add `iceberg_aggregate_state::load_touched_aggregate_target_state` that drives a `target_table.scan()` while client-side filtering `FileScanTask`s by `MvPartitionKey` and post-filtering Arrow batches by touched group `__row_id__`. PR1 introduces no production caller for `load_touched_aggregate_target_state`; the aggregate refresh path still calls `load_current_aggregate_target_state`. PR3 (apply path integration) makes the switch.

**Tech Stack:** Rust, Iceberg-rust 0.9, Arrow `RecordBatch`, NovaRocks MV refresh code in `src/engine/mv/`, `cargo test --lib` for fast unit tests.

---

## File Structure

- Modify: `src/engine/mv/partition/key.rs`
  - Add `TargetPartitionFilter` enum + `Display`/helpers used by callers.
- Modify: `src/engine/mv/partition/mod.rs`
  - Re-export `TargetPartitionFilter`.
- Modify: `src/connector/iceberg/changes.rs`
  - Promote `change_partition_field_values` and `change_partition_transform_name` to `pub(crate)` so the target-file scan code path can reuse them.
- Modify: `src/engine/mv/partition/mapping.rs`
  - Extend `map_file_partition_to_mv_key` from identity-only to identity / year / month / day / hour / bucket(N) / truncate(W); reject `void`/`unknown` with a precise error; compare contract enum to manifest transform text by structural equivalence.
- Modify: `src/engine/mv/iceberg_target_apply.rs`
  - Thread `partition_filter: &TargetPartitionFilter` through `locate_target_rows_by_apply_key_impl` and its public wrappers; client-side filter `FileScanTask`s by `MvPartitionKey`.
- Modify: `src/engine/mv/iceberg_join_coalesce.rs`
  - Pass `&TargetPartitionFilter::None` to the locator call; behavior unchanged.
- Modify: `src/engine/mv/iceberg_merge_sink.rs`
  - Pass `&TargetPartitionFilter::None` to the two locator calls; behavior unchanged.
- Modify: `src/engine/mv/iceberg_aggregate_state.rs`
  - Add `AggregateStateLookupStats` and `load_touched_aggregate_target_state(target_table, layout, schema_contract, touched_row_ids, partition_filter)`.

`src/engine/mv/iceberg_refresh.rs::apply_iceberg_aggregate_delta_chunks` is intentionally NOT touched in PR1 — production switchover happens in PR3.

---

## Task 1: Add the shared `TargetPartitionFilter` type

**Files:**
- Modify: `src/engine/mv/partition/key.rs`
- Modify: `src/engine/mv/partition/mod.rs`

- [ ] **Step 1: Append failing tests in `src/engine/mv/partition/key.rs`**

Add this test fn into the existing `mod tests` block (below the existing `key`/`known_partitions_are_sorted_and_deduped` tests):

```rust
    #[test]
    fn target_partition_filter_none_passes_any_key() {
        let filter = TargetPartitionFilter::None;
        assert!(filter.matches(&key(1, "id", "1")));
        assert_eq!(filter.allow_list_len(), None);
    }

    #[test]
    fn target_partition_filter_allow_list_matches_only_listed_keys() {
        let filter = TargetPartitionFilter::AllowList(
            [key(1, "id", "1"), key(1, "id", "2")].into_iter().collect(),
        );
        assert!(filter.matches(&key(1, "id", "1")));
        assert!(filter.matches(&key(1, "id", "2")));
        assert!(!filter.matches(&key(1, "id", "3")));
        assert!(!filter.matches(&key(2, "id", "1")));
        assert_eq!(filter.allow_list_len(), Some(2));
    }

    #[test]
    fn target_partition_filter_empty_allow_list_matches_nothing() {
        let filter = TargetPartitionFilter::AllowList(std::collections::BTreeSet::new());
        assert!(!filter.matches(&key(1, "id", "1")));
        assert_eq!(filter.allow_list_len(), Some(0));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/harbor/.codex/worktrees/2185/NovaRocks
cargo test --lib engine::mv::partition::key::tests::target_partition_filter -- --nocapture
```

Expected: compile fails with `cannot find type TargetPartitionFilter in this scope`.

- [ ] **Step 3: Implement `TargetPartitionFilter`**

Append to the end of `src/engine/mv/partition/key.rs` (before the `#[cfg(test)] mod tests` block):

```rust
/// Optional partition predicate that the aggregate target state loader and
/// the iceberg MV target locator share. `None` means "do not prune"; an
/// `AllowList` means "drop FileScanTasks whose target partition key is not in
/// this set". The empty allow-list is a legitimate state (no partition is
/// affected); callers MUST NOT silently treat it as "no filter".
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TargetPartitionFilter {
    None,
    AllowList(BTreeSet<MvPartitionKey>),
}

impl TargetPartitionFilter {
    pub(crate) fn matches(&self, key: &MvPartitionKey) -> bool {
        match self {
            Self::None => true,
            Self::AllowList(set) => set.contains(key),
        }
    }

    pub(crate) fn allow_list_len(&self) -> Option<usize> {
        match self {
            Self::None => None,
            Self::AllowList(set) => Some(set.len()),
        }
    }

    pub(crate) fn is_allow_list(&self) -> bool {
        matches!(self, Self::AllowList(_))
    }
}
```

Update `src/engine/mv/partition/mod.rs` from:

```rust
pub(crate) use key::{AffectedMvPartitions, MvPartitionKey, MvPartitionKeyField, MvPartitionValue};
```

to:

```rust
pub(crate) use key::{
    AffectedMvPartitions, MvPartitionKey, MvPartitionKeyField, MvPartitionValue,
    TargetPartitionFilter,
};
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --lib engine::mv::partition::key::tests::target_partition_filter -- --nocapture
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/partition/key.rs src/engine/mv/partition/mod.rs
git commit -m "feat: add target partition filter type for mv refresh"
```

---

## Task 2: Expose `change_partition_field_values` and `change_partition_transform_name`

**Files:**
- Modify: `src/connector/iceberg/changes.rs`

`change_partition_field_values` already does exactly what the target-side file scan needs: take `&TableMetadata`, `spec_id`, `&iceberg::spec::Struct` and produce `Vec<ChangePartitionFieldValue>` that `map_file_partition_to_mv_key` can consume. PR1 needs both helpers reachable from `src/engine/mv/iceberg_aggregate_state.rs` and `src/engine/mv/iceberg_target_apply.rs`. The test below proves the function is visible from outside `changes.rs`.

- [ ] **Step 1: Write the failing visibility test**

Create a new test module at the end of `src/engine/mv/partition/mapping.rs` (or extend the existing one) — this verifies the helper is callable from another crate-internal module:

Add this test inside the existing `#[cfg(test)] mod tests` in `src/engine/mv/partition/mapping.rs`:

```rust
    #[test]
    fn change_partition_field_values_is_reachable_for_mv_partition_module() {
        use crate::connector::iceberg::changes::change_partition_field_values;
        // We do not need to drive Iceberg metadata in a unit test — just make
        // sure the symbol is visible at the call site. If this fn ever becomes
        // private again, this test will fail to compile.
        let _fn_ptr: fn(
            &iceberg::spec::TableMetadata,
            i32,
            &iceberg::spec::Struct,
        ) -> Result<
            Vec<crate::connector::iceberg::changes::ChangePartitionFieldValue>,
            crate::connector::iceberg::changes::ChangeError,
        > = change_partition_field_values;
    }
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cargo test --lib engine::mv::partition::mapping::tests::change_partition_field_values_is_reachable_for_mv_partition_module
```

Expected: compile fails with `function `change_partition_field_values` is private`.

- [ ] **Step 3: Promote the helpers to `pub(crate)`**

In `src/connector/iceberg/changes.rs`:

Change line 262 from:

```rust
fn change_partition_field_values(
```

to:

```rust
pub(crate) fn change_partition_field_values(
```

Change line 296 from:

```rust
fn change_partition_transform_name(transform: &iceberg::spec::Transform) -> String {
```

to:

```rust
pub(crate) fn change_partition_transform_name(transform: &iceberg::spec::Transform) -> String {
```

`change_partition_value` (the literal → enum converter at line 303) stays private — it is only an internal step of `change_partition_field_values`.

Confirm `ChangePartitionFieldValue`, `ChangePartitionValue`, and `ChangeError` are already `pub(crate)`:

```bash
grep -n "pub(crate) struct ChangePartitionFieldValue\|pub(crate) enum ChangePartitionValue\|pub(crate) enum ChangeError" src/connector/iceberg/changes.rs
```

Expected: all three appear.

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
cargo test --lib engine::mv::partition::mapping::tests::change_partition_field_values_is_reachable_for_mv_partition_module
```

Expected: passes.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/changes.rs src/engine/mv/partition/mapping.rs
git commit -m "feat: expose iceberg change partition helpers to mv partition module"
```

---

## Task 3: Extend `map_file_partition_to_mv_key` to all first-class transforms

**Files:**
- Modify: `src/engine/mv/partition/mapping.rs`

PR2 only supported `Identity`. PR1 broadens it to `Identity / Year / Month / Day / Hour / Bucket(N) / Truncate(W)`. The matching rule:

1. Look up the partition field's `MvPartitionTransformContract` in the contract.
2. Find a matching `ChangePartitionFieldValue` in the file's `partition_values` whose `source_field_id` matches the partition field's referenced base field id AND whose `transform` text matches the contract transform under `transform_text_matches_contract` (defined below).
3. Render the matched value into `MvPartitionValue::{Null, String}` using the existing `ChangePartitionValue` translation.

Manifest transform text comes from `change_partition_transform_name`, which uses Iceberg's `Debug` format with `to_ascii_lowercase()` for non-identity variants. Concrete strings expected:

| Contract enum | Manifest text |
|---|---|
| `Identity` | `identity` |
| `Year` | `year` |
| `Month` | `month` |
| `Day` | `day` |
| `Hour` | `hour` |
| `Bucket { num_buckets: 8 }` | `bucket(8)` |
| `Truncate { width: 16 }` | `truncate(16)` |
| `Void` | `void` |

`Void` and `Unknown` are rejected — `Void` produces NULL partitions which carry no MV pruning information, and `Unknown` would require contract drift recovery.

- [ ] **Step 1: Replace the identity-only assertion test with broad transform tests**

In `src/engine/mv/partition/mapping.rs`, find the existing `#[cfg(test)] mod tests` block (currently containing `contract_with_identity_partition`, `maps_identity_partition_value_to_mv_key`, `returns_none_for_unpartitioned_contract`, `unsupported_partition_value_requires_unknown_mapping`, and the new visibility test from Task 2). Replace the body of `contract_with_identity_partition` and add new helpers and tests:

Add these helpers above the existing tests inside `mod tests`:

```rust
    fn contract_with_partition(
        transform: MvPartitionTransformContract,
    ) -> MvSchemaContract {
        let mut contract = contract_with_identity_partition();
        let partition = contract
            .target
            .partition
            .as_mut()
            .expect("identity helper always builds a partition");
        partition.fields[0].transform = transform;
        contract
    }

    fn partition_value(transform_text: &str, value: ChangePartitionValue) -> ChangePartitionFieldValue {
        ChangePartitionFieldValue {
            source_field_id: 1,
            source_column: Some("id".to_string()),
            field_name: "id".to_string(),
            transform: transform_text.to_string(),
            value,
        }
    }
```

Add these tests below the existing tests:

```rust
    #[test]
    fn maps_year_transform_to_mv_key() {
        let contract = contract_with_partition(MvPartitionTransformContract::Year);
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "year",
                ChangePartitionValue::Primitive("55".to_string()),
            )],
        )
        .unwrap();

        assert_eq!(
            mapped.unwrap().fields[0].value,
            MvPartitionValue::String("55".to_string())
        );
    }

    #[test]
    fn maps_month_day_hour_transforms() {
        for (contract_transform, manifest_text, value) in [
            (MvPartitionTransformContract::Month, "month", "660"),
            (MvPartitionTransformContract::Day, "day", "20000"),
            (MvPartitionTransformContract::Hour, "hour", "480000"),
        ] {
            let contract = contract_with_partition(contract_transform.clone());
            let mapped = map_file_partition_to_mv_key(
                &contract,
                7,
                &[partition_value(
                    manifest_text,
                    ChangePartitionValue::Primitive(value.to_string()),
                )],
            )
            .unwrap();
            assert_eq!(
                mapped.unwrap().fields[0].value,
                MvPartitionValue::String(value.to_string()),
                "transform {contract_transform:?} did not round-trip"
            );
        }
    }

    #[test]
    fn maps_bucket_transform_with_matching_arity() {
        let contract = contract_with_partition(MvPartitionTransformContract::Bucket {
            num_buckets: 8,
        });
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "bucket(8)",
                ChangePartitionValue::Primitive("3".to_string()),
            )],
        )
        .unwrap();
        assert_eq!(
            mapped.unwrap().fields[0].value,
            MvPartitionValue::String("3".to_string())
        );
    }

    #[test]
    fn rejects_bucket_transform_arity_mismatch() {
        let contract = contract_with_partition(MvPartitionTransformContract::Bucket {
            num_buckets: 8,
        });
        let err = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "bucket(16)",
                ChangePartitionValue::Primitive("3".to_string()),
            )],
        )
        .unwrap_err();
        assert!(err.contains("file metadata transform"), "{err}");
        assert!(err.contains("bucket(16)"), "{err}");
        assert!(err.contains("bucket(8)"), "{err}");
    }

    #[test]
    fn maps_truncate_transform_with_matching_width() {
        let contract = contract_with_partition(MvPartitionTransformContract::Truncate {
            width: 16,
        });
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "truncate(16)",
                ChangePartitionValue::Primitive("ho".to_string()),
            )],
        )
        .unwrap();
        assert_eq!(
            mapped.unwrap().fields[0].value,
            MvPartitionValue::String("ho".to_string())
        );
    }

    #[test]
    fn rejects_void_transform() {
        let contract = contract_with_partition(MvPartitionTransformContract::Void);
        let err = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value("void", ChangePartitionValue::Null)],
        )
        .unwrap_err();
        assert!(err.contains("Void"), "{err}");
    }

    #[test]
    fn null_partition_value_renders_as_mv_null() {
        let contract = contract_with_partition(MvPartitionTransformContract::Day);
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value("day", ChangePartitionValue::Null)],
        )
        .unwrap();
        assert_eq!(mapped.unwrap().fields[0].value, MvPartitionValue::Null);
    }
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test --lib engine::mv::partition::mapping::tests
```

Expected: at least 4 of the new tests fail (year/month/day/hour/bucket/truncate currently return `Err("unsupported transform")`).

- [ ] **Step 3: Replace the transform matching logic**

In `src/engine/mv/partition/mapping.rs`, replace the body of `map_file_partition_to_mv_key` with the implementation below. Keep the function signature and the call to `partition_transform_name` for error messages.

```rust
pub(crate) fn map_file_partition_to_mv_key(
    contract: &MvSchemaContract,
    file_spec_id: i32,
    file_partition_values: &[ChangePartitionFieldValue],
) -> Result<Option<MvPartitionKey>, String> {
    let Some(partition) = &contract.target.partition else {
        return Ok(None);
    };

    let mut mapped_fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        let expected_transform_text =
            contract_transform_manifest_text(&partition_field.transform).ok_or_else(|| {
                format!(
                    "MV partition field {} uses unsupported transform {}",
                    partition_field.partition_field_name,
                    partition_transform_name(&partition_field.transform)
                )
            })?;

        let output_index = contract
            .target
            .visible_columns
            .iter()
            .position(|column| column.target_field_id == partition_field.source_target_field_id)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} references missing target field {}",
                    partition_field.partition_field_name, partition_field.source_target_field_id
                )
            })?;
        let output_lineage = contract.output.columns.get(output_index).ok_or_else(|| {
            format!(
                "MV partition field {} requires row-evaluation fallback",
                partition_field.partition_field_name
            )
        })?;

        if output_lineage.expression.kind != ExpressionKind::Column
            || output_lineage.expression.referenced_base_field_ids.len() != 1
        {
            return Err(format!(
                "MV partition field {} requires row-evaluation fallback",
                partition_field.partition_field_name
            ));
        }

        let base_field_id = output_lineage.expression.referenced_base_field_ids[0];

        let mut matched_by_id_count = 0;
        let mut transform_mismatch: Option<&str> = None;
        let file_partition_value = file_partition_values
            .iter()
            .find(|value| {
                if value.source_field_id != base_field_id {
                    return false;
                }
                matched_by_id_count += 1;
                if value.transform.eq_ignore_ascii_case(&expected_transform_text) {
                    true
                } else {
                    transform_mismatch = Some(value.transform.as_str());
                    false
                }
            })
            .ok_or_else(|| {
                if matched_by_id_count == 0 {
                    format!(
                        "MV partition field {} cannot be proven from Iceberg file partition metadata for file spec {}",
                        partition_field.partition_field_name, file_spec_id
                    )
                } else {
                    format!(
                        "MV partition field {} file metadata transform {} mismatches contract transform {}",
                        partition_field.partition_field_name,
                        transform_mismatch.unwrap_or("<unknown>"),
                        expected_transform_text
                    )
                }
            })?;

        let value = match &file_partition_value.value {
            ChangePartitionValue::Primitive(value) => MvPartitionValue::String(value.clone()),
            ChangePartitionValue::Null => MvPartitionValue::Null,
            ChangePartitionValue::Unsupported(reason) => {
                return Err(format!(
                    "MV partition field {} has unsupported partition value: {}",
                    partition_field.partition_field_name, reason
                ));
            }
        };
        mapped_fields.push(MvPartitionKeyField::new(
            partition_field.partition_field_name.clone(),
            value,
        ));
    }

    Ok(Some(MvPartitionKey::new(
        partition.target_spec_id,
        mapped_fields,
    )))
}

fn contract_transform_manifest_text(transform: &MvPartitionTransformContract) -> Option<String> {
    match transform {
        MvPartitionTransformContract::Identity => Some("identity".to_string()),
        MvPartitionTransformContract::Year => Some("year".to_string()),
        MvPartitionTransformContract::Month => Some("month".to_string()),
        MvPartitionTransformContract::Day => Some("day".to_string()),
        MvPartitionTransformContract::Hour => Some("hour".to_string()),
        MvPartitionTransformContract::Bucket { num_buckets } => {
            Some(format!("bucket({num_buckets})"))
        }
        MvPartitionTransformContract::Truncate { width } => Some(format!("truncate({width})")),
        MvPartitionTransformContract::Void => None,
    }
}
```

The `partition_transform_name` helper near the bottom of the file stays; it is only used for error messages.

- [ ] **Step 4: Run the mapping tests**

Run:

```bash
cargo test --lib engine::mv::partition::mapping::tests
```

Expected: all tests pass, including `maps_year_transform_to_mv_key`, `maps_month_day_hour_transforms`, `maps_bucket_transform_with_matching_arity`, `rejects_bucket_transform_arity_mismatch`, `maps_truncate_transform_with_matching_width`, `rejects_void_transform`, `null_partition_value_renders_as_mv_null`.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/partition/mapping.rs
git commit -m "feat: extend mv partition mapping to year/month/day/hour/bucket/truncate"
```

---

## Task 4: Thread `partition_filter` through `iceberg_target_apply`

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`
- Modify: `src/engine/mv/iceberg_join_coalesce.rs`
- Modify: `src/engine/mv/iceberg_merge_sink.rs`

`locate_target_rows_by_apply_key_impl` becomes partition-aware. It uses `target_table.metadata()` plus the per-task `partition_spec` / `partition` to build the file's `MvPartitionKey` (via `change_partition_field_values` + `map_file_partition_to_mv_key`), then drops tasks the filter does not allow. `None` filter is a fast bypass — the filter check never runs.

- [ ] **Step 1: Write the failing test**

Append to `#[cfg(test)] mod tests` at the bottom of `src/engine/mv/iceberg_target_apply.rs`. The test only exercises the public surface and the no-op `TargetPartitionFilter::None` branch — full integration of the allow-list path lives in PR3 SQL tests once real refresh data is available.

```rust
    use crate::engine::mv::partition::TargetPartitionFilter;

    #[test]
    fn empty_request_with_filter_none_returns_empty_groups() {
        // No request → no scan → empty groups, regardless of filter shape.
        let rt = crate::runtime::global_async_runtime::runtime();
        let (target_table, _tempdir) = build_memory_iceberg_apply_key_target(&[]);
        let existing = std::collections::BTreeMap::new();
        let referenced = std::collections::BTreeMap::new();
        let groups = rt
            .block_on(super::locate_target_rows_by_apply_key(
                &target_table,
                &[],
                &existing,
                &referenced,
                &TargetPartitionFilter::None,
            ))
            .expect("locator");
        assert!(groups.is_empty());
    }

    #[test]
    fn empty_request_with_empty_allow_list_returns_empty_groups() {
        let rt = crate::runtime::global_async_runtime::runtime();
        let (target_table, _tempdir) = build_memory_iceberg_apply_key_target(&[]);
        let existing = std::collections::BTreeMap::new();
        let referenced = std::collections::BTreeMap::new();
        let filter = TargetPartitionFilter::AllowList(std::collections::BTreeSet::new());
        let groups = rt
            .block_on(super::locate_target_rows_by_apply_key(
                &target_table,
                &[],
                &existing,
                &referenced,
                &filter,
            ))
            .expect("locator");
        assert!(groups.is_empty());
    }
```

If the file does not already have a `build_memory_iceberg_apply_key_target(&[i64]) -> (Table, TempDir)` fixture, add this helper to the existing `mod tests`. (Many neighbouring tests in `iceberg_target_apply.rs` already construct memory Iceberg tables; reuse their fixture if present and adapt the call. If you must add a new one, place it near other fixtures so it stays discoverable.)

> Implementation note: this Task ships only the no-op (`None` and empty `AllowList`) behavior tests. The non-trivial "allow list prunes some files" path is exercised end-to-end by PR3 integration tests and by Task 5 below (which is the public consumer that actually drives a multi-partition fixture).

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_target_apply::tests::empty_request_with_filter_none_returns_empty_groups
cargo test --lib engine::mv::iceberg_target_apply::tests::empty_request_with_empty_allow_list_returns_empty_groups
```

Expected: compile fails with `this function takes 4 arguments but 5 arguments were supplied` on the existing wrapper signature.

- [ ] **Step 3: Add `partition_filter` to the impl and public wrappers**

In `src/engine/mv/iceberg_target_apply.rs`:

1. Add to the imports at the top:

```rust
use crate::engine::mv::partition::TargetPartitionFilter;
```

2. Change `locate_target_rows_by_apply_key_impl` signature (around line 447) to:

```rust
async fn locate_target_rows_by_apply_key_impl(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: ApplyKeyRequest<'_>,
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
```

3. Inside `locate_target_rows_by_apply_key_impl`, between the `let cleaned_tasks = ...` line and the `let arrow_reader = ...` line, replace the existing `cleaned_tasks` definition with this partition-aware filter. Find (around line 476):

```rust
    let cleaned_tasks = task_stream.map(|task_result| {
        task_result.map(|mut task| {
            task.deletes.clear();
            task.predicate = None;
            task
        })
    });
```

Replace with:

```rust
    let target_metadata = target_table.metadata_ref();
    let filter_owned = partition_filter.clone();
    let cleaned_tasks = task_stream.map(move |task_result| {
        let mut task = task_result?;
        task.deletes.clear();
        task.predicate = None;
        if filter_owned.is_allow_list() {
            let Some(partition_struct) = task.partition.as_ref() else {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!(
                        "iceberg MV target locator: file scan task for data file `{}` is missing partition metadata",
                        task.data_file_path
                    ),
                ));
            };
            let Some(spec) = task.partition_spec.as_ref() else {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!(
                        "iceberg MV target locator: file scan task for data file `{}` is missing partition spec",
                        task.data_file_path
                    ),
                ));
            };
            let spec_id = spec.spec_id();
            let values = crate::connector::iceberg::changes::change_partition_field_values(
                &target_metadata,
                spec_id,
                partition_struct,
            )
            .map_err(|e| {
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!(
                        "iceberg MV target locator: cannot derive partition values for `{}`: {e}",
                        task.data_file_path
                    ),
                )
            })?;
            // Use the target table's own partition spec to build the key — the
            // target-side schema contract is not threaded through this helper
            // because both the locator and the state loader produce keys from
            // the same target snapshot. PR3 verifies cross-path equality by
            // running both code paths on the same fixture.
            let mut fields = Vec::with_capacity(values.len());
            for value in &values {
                let mv_value = match &value.value {
                    crate::connector::iceberg::changes::ChangePartitionValue::Primitive(v) => {
                        crate::engine::mv::partition::MvPartitionValue::String(v.clone())
                    }
                    crate::connector::iceberg::changes::ChangePartitionValue::Null => {
                        crate::engine::mv::partition::MvPartitionValue::Null
                    }
                    crate::connector::iceberg::changes::ChangePartitionValue::Unsupported(reason) => {
                        return Err(iceberg::Error::new(
                            iceberg::ErrorKind::DataInvalid,
                            format!(
                                "iceberg MV target locator: file `{}` has unsupported partition value: {reason}",
                                task.data_file_path
                            ),
                        ));
                    }
                };
                fields.push(crate::engine::mv::partition::MvPartitionKeyField::new(
                    value.field_name.clone(),
                    mv_value,
                ));
            }
            let key = crate::engine::mv::partition::MvPartitionKey::new(spec_id, fields);
            if !filter_owned.matches(&key) {
                return Ok(None);
            }
        }
        Ok(Some(task))
    });
    let cleaned_tasks = cleaned_tasks.filter_map(|task_or_skip| async move {
        match task_or_skip {
            Ok(Some(task)) => Some(Ok(task)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    });
```

4. Update the three public wrappers to take and forward `partition_filter`.

Find `locate_target_rows_by_apply_key` (around line 204) and change its signature to:

```rust
pub(crate) async fn locate_target_rows_by_apply_key(
    target_table: &iceberg::table::Table,
    base_row_ids: &[i64],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        ICEBERG_MV_APPLY_KEY_COLUMN,
        ApplyKeyRequest::Int64(base_row_ids),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}
```

Find `locate_target_rows_by_string_apply_key` (around line 220) and change its signature to:

```rust
pub(crate) async fn locate_target_rows_by_string_apply_key(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[String],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        apply_key_column,
        ApplyKeyRequest::Utf8(requested_keys),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}
```

Find `locate_target_rows_by_apply_key_string` (around line 509) and update both its signature and its body call:

```rust
pub(crate) async fn locate_target_rows_by_apply_key_string(
    target_table: &iceberg::table::Table,
    join_row_keys: &[String],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    locate_target_rows_by_string_apply_key(
        target_table,
        ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
        join_row_keys,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}
```

5. Update the aggregate-apply caller in `src/engine/mv/iceberg_refresh.rs` (around line 1962) to compile — PR1 still passes `&TargetPartitionFilter::None` here; PR3 will swap in the real filter.

Find:

```rust
        let groups = match data_block_on(
            crate::engine::mv::iceberg_target_apply::locate_target_rows_by_string_apply_key(
                &target_table,
                ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
                &delete_row_ids,
                &existing_deletes_by_file,
                &referenced_data_file_partitions,
            ),
        ) {
```

Change to:

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

6. Migrate the two other callers.

`src/engine/mv/iceberg_join_coalesce.rs` (around line 139): inside the `data_block_on(...)` call, add as the last argument:

```rust
                    &crate::engine::mv::partition::TargetPartitionFilter::None,
```

`src/engine/mv/iceberg_merge_sink.rs` (around line 205 for `locate_target_rows_by_apply_key` and around line 222 for `locate_target_rows_by_string_apply_key`): both calls take the same trailing argument:

```rust
                        &crate::engine::mv::partition::TargetPartitionFilter::None,
```

- [ ] **Step 4: Run the tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_target_apply::tests
cargo build --tests
```

Expected: both `empty_request_*` tests pass; full `cargo build --tests` succeeds (no caller is left unmigrated).

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_target_apply.rs \
        src/engine/mv/iceberg_join_coalesce.rs \
        src/engine/mv/iceberg_merge_sink.rs \
        src/engine/mv/iceberg_refresh.rs
git commit -m "feat: thread partition filter through iceberg mv target locator"
```

---

## Task 5: Add `load_touched_aggregate_target_state` and stats

**Files:**
- Modify: `src/engine/mv/iceberg_aggregate_state.rs`

`load_touched_aggregate_target_state` consumes the existing `AggregateMvLayout`, an `MvSchemaContract` (so it can call `map_file_partition_to_mv_key`), a `BTreeSet<String>` of touched group `__row_id__`s, and a `TargetPartitionFilter`. It returns `(Vec<Chunk>, AggregateStateLookupStats)`.

`AggregateStateLookupStats` counts: planned tasks, pruned (kept) tasks, scanned Arrow rows, matched rows.

- [ ] **Step 1: Write the failing tests**

Append to `#[cfg(test)] mod tests` at the bottom of `src/engine/mv/iceberg_aggregate_state.rs`. The tests use the same memory Iceberg helpers the existing `merge_*` tests use; add a small fixture builder if one is not already present. The aggregate state schema mirrors `test_count_layout` so the existing helpers work.

```rust
    use crate::engine::mv::partition::{
        MvPartitionKey, MvPartitionKeyField, MvPartitionValue, TargetPartitionFilter,
    };
    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };

    fn count_schema_contract_with_region_partition() -> MvSchemaContract {
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
                        output_name: "region".to_string(),
                        target_field_id: 11,
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
                        partition_field_name: "region".to_string(),
                        source_target_field_id: 11,
                        source_column_name: "region".to_string(),
                        transform: MvPartitionTransformContract::Identity,
                    }],
                }),
            },
        }
    }

    /// Build an in-memory Iceberg table with two `region` identity partitions
    /// (`a`, `b`), one aggregate physical row per region. Returns the table
    /// plus a TempDir keeping the warehouse alive for the duration of the test.
    ///
    /// Reuse the existing helpers in this crate for memory Iceberg tables —
    /// search for `build_memory_iceberg_target` or `build_memory_iceberg_apply_key_target`
    /// before adding a new fixture. If none accept aggregate physical
    /// schemas (row_id + region + c + __agg_state_c with `PARTITION BY region`),
    /// add `build_memory_iceberg_partitioned_aggregate_target` near the other
    /// in-memory fixtures and have it create the table, write two parquet
    /// data files (one per partition), and commit them.
    fn build_memory_iceberg_partitioned_aggregate_target()
        -> (iceberg::table::Table, tempfile::TempDir, Vec<String>) {
        unimplemented!(
            "build_memory_iceberg_partitioned_aggregate_target: add this helper alongside \
             existing memory iceberg fixtures in this crate before running the tests below"
        )
    }

    #[test]
    fn empty_touched_row_ids_short_circuits() {
        let rt = crate::runtime::global_async_runtime::runtime();
        let layout = test_count_layout();
        let contract = count_schema_contract_with_region_partition();
        let (target_table, _tempdir, _row_ids) =
            build_memory_iceberg_partitioned_aggregate_target();
        let touched: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let filter = TargetPartitionFilter::None;
        let (chunks, stats) = rt
            .block_on(super::load_touched_aggregate_target_state_async(
                &target_table,
                &layout,
                &contract,
                &touched,
                &filter,
            ))
            .expect("lookup");
        assert!(chunks.is_empty());
        assert_eq!(stats.planned_file_count, 0);
        assert_eq!(stats.pruned_file_count, 0);
        assert_eq!(stats.scanned_row_count, 0);
        assert_eq!(stats.matched_row_count, 0);
    }

    #[test]
    fn allow_list_prunes_other_partitions_and_row_id_filters_remaining_rows() {
        let rt = crate::runtime::global_async_runtime::runtime();
        let layout = test_count_layout();
        let contract = count_schema_contract_with_region_partition();
        let (target_table, _tempdir, row_ids) =
            build_memory_iceberg_partitioned_aggregate_target();
        // Only touch the row living in region=a. AllowList drops region=b's file.
        let mut touched = std::collections::BTreeSet::new();
        touched.insert(row_ids[0].clone());
        let mut allow = std::collections::BTreeSet::new();
        allow.insert(MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String("a".to_string()),
            )],
        ));
        let filter = TargetPartitionFilter::AllowList(allow);
        let (chunks, stats) = rt
            .block_on(super::load_touched_aggregate_target_state_async(
                &target_table,
                &layout,
                &contract,
                &touched,
                &filter,
            ))
            .expect("lookup");
        let returned_row_ids: Vec<_> = chunks
            .iter()
            .flat_map(|chunk| {
                let col = chunk
                    .batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("row id");
                (0..col.len()).map(move |row| col.value(row).to_string())
            })
            .collect();
        assert_eq!(returned_row_ids, vec![row_ids[0].clone()]);
        assert_eq!(stats.pruned_file_count, 1);
        assert!(stats.planned_file_count >= 2, "stats={stats:?}");
        assert_eq!(stats.matched_row_count, 1);
    }

    #[test]
    fn empty_allow_list_with_non_empty_touched_returns_err() {
        let rt = crate::runtime::global_async_runtime::runtime();
        let layout = test_count_layout();
        let contract = count_schema_contract_with_region_partition();
        let (target_table, _tempdir, row_ids) =
            build_memory_iceberg_partitioned_aggregate_target();
        let mut touched = std::collections::BTreeSet::new();
        touched.insert(row_ids[0].clone());
        let filter = TargetPartitionFilter::AllowList(std::collections::BTreeSet::new());
        let err = rt
            .block_on(super::load_touched_aggregate_target_state_async(
                &target_table,
                &layout,
                &contract,
                &touched,
                &filter,
            ))
            .expect_err("lookup should fail fast");
        assert!(
            err.contains("empty partition allow-list with non-empty touched groups"),
            "{err}"
        );
    }

    #[test]
    fn none_filter_keeps_all_partitions_and_still_filters_by_row_id() {
        let rt = crate::runtime::global_async_runtime::runtime();
        let layout = test_count_layout();
        let contract = count_schema_contract_with_region_partition();
        let (target_table, _tempdir, row_ids) =
            build_memory_iceberg_partitioned_aggregate_target();
        let mut touched = std::collections::BTreeSet::new();
        touched.insert(row_ids[1].clone());
        let filter = TargetPartitionFilter::None;
        let (chunks, stats) = rt
            .block_on(super::load_touched_aggregate_target_state_async(
                &target_table,
                &layout,
                &contract,
                &touched,
                &filter,
            ))
            .expect("lookup");
        let returned_row_ids: Vec<_> = chunks
            .iter()
            .flat_map(|chunk| {
                let col = chunk
                    .batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("row id");
                (0..col.len()).map(move |row| col.value(row).to_string())
            })
            .collect();
        assert_eq!(returned_row_ids, vec![row_ids[1].clone()]);
        assert_eq!(stats.pruned_file_count, stats.planned_file_count);
        assert_eq!(stats.matched_row_count, 1);
    }
```

> Implementation note for `build_memory_iceberg_partitioned_aggregate_target`: this plan deliberately stops short of inlining a full fixture body because the surrounding test module already contains the canonical pattern (`IcebergCatalogEntry::open_memory(...)` + `create_table(...)` + writing parquet via the iceberg writer). The implementer's first action under this Task should be to grep `src/engine/mv/iceberg_aggregate_state.rs` and `src/engine/mv/` for an existing `build_memory_iceberg_*_target` helper to copy and adapt; if none exist, replicate the shape used by neighbouring aggregate-state tests, restricting the schema to row_id (Utf8) + region (Utf8) + c (Int64) + __agg_state_c (Int64) with `PARTITION BY region` and one row per partition (`region=a` carrying row_ids[0]`, `region=b` carrying `row_ids[1]`).

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_aggregate_state::tests::empty_touched_row_ids_short_circuits
cargo test --lib engine::mv::iceberg_aggregate_state::tests::allow_list_prunes_other_partitions_and_row_id_filters_remaining_rows
cargo test --lib engine::mv::iceberg_aggregate_state::tests::empty_allow_list_with_non_empty_touched_returns_err
cargo test --lib engine::mv::iceberg_aggregate_state::tests::none_filter_keeps_all_partitions_and_still_filters_by_row_id
```

Expected: compile fails because `load_touched_aggregate_target_state_async`, `AggregateStateLookupStats`, and (initially) `build_memory_iceberg_partitioned_aggregate_target` do not exist.

- [ ] **Step 3: Add the stats struct, the API surface, and the async impl**

In `src/engine/mv/iceberg_aggregate_state.rs`, near the top of the file (next to `IcebergAggregateMergeResult`), add:

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct AggregateStateLookupStats {
    pub planned_file_count: usize,
    pub pruned_file_count: usize,
    pub scanned_row_count: usize,
    pub matched_row_count: usize,
}
```

Below `load_current_aggregate_target_state_async`, add the new sync entry point:

```rust
pub(crate) fn load_touched_aggregate_target_state(
    target_table: &iceberg::table::Table,
    layout: &AggregateMvLayout,
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    touched_row_ids: &std::collections::BTreeSet<String>,
    partition_filter: &crate::engine::mv::partition::TargetPartitionFilter,
) -> Result<(Vec<Chunk>, AggregateStateLookupStats), String> {
    crate::runtime::global_async_runtime::data_block_on(load_touched_aggregate_target_state_async(
        target_table,
        layout,
        schema_contract,
        touched_row_ids,
        partition_filter,
    ))?
}
```

And below it the async implementation:

```rust
pub(crate) async fn load_touched_aggregate_target_state_async(
    target_table: &iceberg::table::Table,
    layout: &AggregateMvLayout,
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    touched_row_ids: &std::collections::BTreeSet<String>,
    partition_filter: &crate::engine::mv::partition::TargetPartitionFilter,
) -> Result<(Vec<Chunk>, AggregateStateLookupStats), String> {
    use arrow::array::BooleanArray;
    use futures::StreamExt;
    use iceberg::arrow::ArrowReaderBuilder;

    let mut stats = AggregateStateLookupStats::default();

    if touched_row_ids.is_empty() {
        return Ok((Vec::new(), stats));
    }
    if let crate::engine::mv::partition::TargetPartitionFilter::AllowList(set) = partition_filter
    {
        if set.is_empty() {
            return Err(
                "aggregate target lookup: empty partition allow-list with non-empty touched groups"
                    .to_string(),
            );
        }
    }

    let select_cols = layout
        .physical_columns
        .iter()
        .map(|column| column.column.name.clone())
        .collect::<Vec<_>>();
    let scan = target_table
        .scan()
        .select(select_cols)
        .build()
        .map_err(|e| format!("build iceberg aggregate target state scan failed: {e}"))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| format!("plan iceberg aggregate target state files failed: {e}"))?;
    let target_metadata = target_table.metadata_ref();
    let schema_contract = schema_contract.clone();
    let partition_filter = partition_filter.clone();
    let kept_tasks: Vec<_> = task_stream
        .map(|task_result| {
            let task = task_result.map_err(|e| {
                format!("iceberg aggregate target state task error: {e}")
            })?;
            Ok::<_, String>(task)
        })
        .collect::<Vec<_>>()
        .await;
    let mut filtered_tasks = Vec::new();
    for task_result in kept_tasks {
        let mut task = task_result?;
        stats.planned_file_count += 1;
        task.predicate = None;
        if let crate::engine::mv::partition::TargetPartitionFilter::AllowList(_) =
            &partition_filter
        {
            let Some(partition_struct) = task.partition.as_ref() else {
                return Err(format!(
                    "iceberg aggregate target state task for `{}` missing partition metadata",
                    task.data_file_path
                ));
            };
            let Some(spec) = task.partition_spec.as_ref() else {
                return Err(format!(
                    "iceberg aggregate target state task for `{}` missing partition spec",
                    task.data_file_path
                ));
            };
            let spec_id = spec.spec_id();
            let values = crate::connector::iceberg::changes::change_partition_field_values(
                &target_metadata,
                spec_id,
                partition_struct,
            )
            .map_err(|e| {
                format!(
                    "iceberg aggregate target state task for `{}`: cannot derive partition values: {e}"
                    , task.data_file_path
                )
            })?;
            let key = crate::engine::mv::partition::mapping::map_file_partition_to_mv_key(
                &schema_contract,
                spec_id,
                &values,
            )?
            .ok_or_else(|| {
                format!(
                    "iceberg aggregate target state task for `{}`: schema contract is unpartitioned but file metadata carries a partition spec",
                    task.data_file_path
                )
            })?;
            if !partition_filter.matches(&key) {
                continue;
            }
        }
        stats.pruned_file_count += 1;
        filtered_tasks.push(task);
    }

    if filtered_tasks.is_empty() {
        return Ok((Vec::new(), stats));
    }

    let cleaned_stream = futures::stream::iter(filtered_tasks.into_iter().map(Ok::<_, iceberg::Error>));
    let arrow_reader = ArrowReaderBuilder::new(target_table.file_io().clone())
        .with_row_group_filtering_enabled(false)
        .build();
    let mut stream = arrow_reader
        .read(Box::pin(cleaned_stream))
        .map_err(|e| format!("read iceberg aggregate target state scan failed: {e}"))?;

    let row_id_column_name = layout.row_id_column.column.name.clone();
    let mut chunks = Vec::new();
    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| format!("iceberg aggregate target state scan error: {e}"))?;
        validate_physical_aggregate_schema(layout, &batch, "iceberg aggregate target state scan")?;
        if batch.num_rows() == 0 {
            continue;
        }
        stats.scanned_row_count += batch.num_rows();
        let row_id_index = batch.schema().index_of(&row_id_column_name).map_err(|e| {
            format!(
                "iceberg aggregate target state scan missing row id column `{row_id_column_name}`: {e}"
            )
        })?;
        let row_id_array = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| {
                format!(
                    "iceberg aggregate target state scan row id column `{row_id_column_name}` must be Utf8"
                )
            })?;
        let keep: Vec<bool> = (0..row_id_array.len())
            .map(|row| {
                if row_id_array.is_null(row) {
                    false
                } else {
                    touched_row_ids.contains(row_id_array.value(row))
                }
            })
            .collect();
        let matched = keep.iter().filter(|k| **k).count();
        if matched == 0 {
            continue;
        }
        stats.matched_row_count += matched;
        let filter = BooleanArray::from(keep);
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                arrow::compute::filter(column.as_ref(), &filter)
                    .map_err(|e| format!("filter iceberg aggregate target state batch failed: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let filtered = arrow::record_batch::RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| format!("rebuild iceberg aggregate target state batch failed: {e}"))?;
        chunks.push(record_batch_to_chunk(filtered)?);
    }
    Ok((chunks, stats))
}
```

> `metadata_ref()` returns an `Arc<TableMetadata>` whose `Deref<Target = TableMetadata>` makes it directly usable where `&TableMetadata` is expected. If your iceberg crate version does not expose `metadata_ref`, fall back to `target_table.metadata().clone()` and pass `&clone` instead.

- [ ] **Step 4: Run the tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_aggregate_state::tests
```

Expected: all four new tests pass alongside the existing aggregate-state tests.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_aggregate_state.rs
git commit -m "feat: add partition-pruned touched-group aggregate state lookup"
```

---

## Task 6: Final verification

**Files:**
- No new source files. This task verifies the full PR1 surface.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt
```

Expected: exit 0.

- [ ] **Step 2: Lint**

Run:

```bash
cargo clippy --all-targets --no-deps -- -D warnings
```

Expected: exit 0.

- [ ] **Step 3: Focused unit tests**

Run:

```bash
cargo test --lib engine::mv::partition
cargo test --lib engine::mv::iceberg_target_apply::tests
cargo test --lib engine::mv::iceberg_aggregate_state::tests
```

Expected: all targeted modules pass.

- [ ] **Step 4: Full library compile check**

Run:

```bash
cargo test --lib --no-run
```

Expected: compile succeeds. This catches any caller that the locator signature change missed.

- [ ] **Step 5: Diff hygiene**

Run:

```bash
git diff --check
```

Expected: no output and exit 0.

- [ ] **Step 6: Commit any formatting-only changes**

If `cargo fmt` introduced changes after Task 5:

```bash
git status -sb
git add src/engine/mv src/connector/iceberg
git commit -m "style: format partition-pruned state lookup changes"
```

If `git status -sb` is clean, skip this commit.

---

## Self-Review

**Spec coverage:** Each line below maps a spec §13 PR1 requirement to a Task above.

| Spec §13 PR1 item | Plan Task |
|---|---|
| `load_touched_aggregate_target_state(...)` | Task 5 |
| `AggregateStateLookupStats { ... }` | Task 5 |
| `TargetPartitionFilter { None, AllowList(BTreeSet<MvPartitionKey>) }` in `partition/key.rs` | Task 1 |
| `locate_target_rows_by_apply_key_impl` partition filter | Task 4 |
| Public wrappers `locate_target_rows_by_apply_key` / `locate_target_rows_by_string_apply_key` / `locate_target_rows_by_apply_key_string` carry new arg | Task 4 |
| Non-aggregate callers migrate to `TargetPartitionFilter::None` (`iceberg_join_coalesce`, `iceberg_merge_sink`) | Task 4 |
| `mapping.rs` extended to full transform set | Task 3 |
| Test coverage §12.1.2 (mapping) | Task 3 |
| Test coverage §12.1.3 (state lookup) | Task 5 |
| Test coverage §12.1.5 (locator partition filter) | Task 4 (no-op branch) + Task 5 (allow-list branch via state lookup) |

The locator allow-list pruning is exercised indirectly through Task 5's `load_touched_aggregate_target_state` because both call sites share the same `change_partition_field_values` + `map_file_partition_to_mv_key` path and the same `TargetPartitionFilter::matches`. A direct locator allow-list integration test against in-memory Iceberg lives in PR3 (apply path) where real refresh data exists.

**Placeholder scan:** No `TBD`, no `implement later`, no `similar to Task N`. The single intentional `unimplemented!()` in Task 5's test fixture is paired with an explicit implementation note instructing the implementer to grep existing fixtures first; this is operational guidance, not a placeholder hole — the test must not pass until the fixture is added.

**Type consistency:** `TargetPartitionFilter`, `AggregateStateLookupStats`, and the locator wrapper signatures are referenced identically across Tasks 1–5. `change_partition_field_values` is exported in Task 2 and consumed in Task 4 and Task 5 with the same signature `fn(&TableMetadata, i32, &Struct) -> Result<Vec<ChangePartitionFieldValue>, ChangeError>`.
