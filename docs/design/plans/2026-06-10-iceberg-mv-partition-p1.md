# Iceberg MV Partition P1 (Derivation Foundation) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Phase P1 of `docs/design/specs/2026-06-10-iceberg-mv-partition-umbrella-design.md`: a unified `AffectedTargetPartitions` type, an extracted partition-derivation library (resolve / bind / evaluate), deletion of the dead pre-cutover apply path, a `DerivePartitionSpecRule` rewrite stage writing into `ImvPlanAnnotation`, and a `PartitionPruningPolicy` field on `RefreshCapabilities` — all with **zero behavior change** (policy is `BestEffort` everywhere, spec decision D5).

**Architecture:** Production aggregate-MV pruning today is: plan-time manifest derivation (`plan_aggregate_mv_affected_partitions` → `RefreshPlan.affected_partitions`) consumed at codegen file binding (`refresh_context::target_state_partition_allow_list`), plus the `DeltaInputRowIds` runtime row filter. The delta-chunk derivation chain (`apply_iceberg_aggregate_delta_chunks`, `build_aggregate_target_partition_filter`, `derive_from_aggregate_delta`'s evaluation loops, `load_touched/current_aggregate_target_state`) is **dead code** since the PR #231 merge-sink cutover — but its evaluation logic is the asset P2+ needs, so PR-1 extracts it into a library before PR-2 deletes the dead shell. PR-3 adds the rewrite-stage annotation (observability + P2 foundation; nothing live consumes the spec yet).

**Tech Stack:** Rust, Arrow, vendored iceberg-rust 0.9 (`vendor/iceberg-0.9.0`), the IMV rewrite framework under `src/sql/optimizer/rewrite/`.

**Build/test conventions:** unit iteration with plain `cargo build` / `cargo test --lib` (profile `dev`); SQL suites with `--profile dev-opt` artifacts. The `iceberg-ivm` suite needs the standalone server + docker fixture (commands given at PR boundaries).

---

## PR map

| PR | Tasks | Content |
|---|---|---|
| PR-1 | 1–4 | `partition/derivation.rs` library (additive only) |
| PR-2 | 5–8 | Unify `AffectedMvPartitions` → `AffectedTargetPartitions` on the live path; delete dead pre-cutover apply path |
| PR-3 | 9–13 | `ImvPlanAnnotation.partition` + `DerivePartitionSpecRule` + pipeline stage + `PartitionPruningPolicy` |

Every task ends in a commit; every PR boundary ends in a full verification step.

---

### Task 1: `AffectedTargetPartitions` unified type

**Files:**
- Create: `src/engine/mv/partition/derivation.rs`
- Modify: `src/engine/mv/partition/mod.rs`

- [ ] **Step 1: Write the failing test**

Append to the new file `src/engine/mv/partition/derivation.rs` (create the file with just the test module first):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mv::partition::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};

    fn key(value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn affected_target_partitions_known_dedupes_and_sorts() {
        let result = AffectedTargetPartitions::known([key("b"), key("a"), key("a")]);
        let AffectedTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        assert_eq!(
            partitions.into_iter().collect::<Vec<_>>(),
            vec![key("a"), key("b")]
        );
    }

    #[test]
    fn affected_target_partitions_not_derived_preserves_reason() {
        let result = AffectedTargetPartitions::not_derived("join MV planning not implemented");
        assert_eq!(
            result.not_derived_reason(),
            Some("join MV planning not implemented")
        );
        assert!(result.is_not_derived());
        assert_eq!(result.partition_count(), 0);
    }

    #[test]
    fn affected_target_partitions_unpartitioned_is_not_not_derived() {
        assert!(!AffectedTargetPartitions::Unpartitioned.is_not_derived());
        assert_eq!(AffectedTargetPartitions::Unpartitioned.partition_count(), 0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib engine::mv::partition::derivation -- --nocapture`
Expected: compile FAIL — `AffectedTargetPartitions` not defined (and `derivation` module not declared yet; add `pub(crate) mod derivation;` to `src/engine/mv/partition/mod.rs` as part of this step so the failure is about the type, not the module).

- [ ] **Step 3: Write minimal implementation**

At the top of `src/engine/mv/partition/derivation.rs`:

```rust
//! Unified partition-derivation library for Iceberg MV refresh.
//!
//! `AffectedTargetPartitions` is the single result type for every affected-
//! partition source (plan-time manifest planning and delta-chunk evaluation).
//! `NotDerived` carries an explicit reason; consumers decide via
//! `PartitionPruningPolicy` (BestEffort in v1, spec D5) whether that means
//! "no pruning" or "fail the refresh".

use std::collections::BTreeSet;

use crate::engine::mv::partition::MvPartitionKey;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedTargetPartitions {
    Unpartitioned,
    Known { partitions: BTreeSet<MvPartitionKey> },
    NotDerived { reason: String },
}

impl AffectedTargetPartitions {
    pub(crate) fn known<I: IntoIterator<Item = MvPartitionKey>>(partitions: I) -> Self {
        Self::Known {
            partitions: partitions.into_iter().collect(),
        }
    }

    pub(crate) fn not_derived(reason: impl Into<String>) -> Self {
        Self::NotDerived {
            reason: reason.into(),
        }
    }

    pub(crate) fn not_derived_reason(&self) -> Option<&str> {
        match self {
            Self::NotDerived { reason } => Some(reason.as_str()),
            Self::Unpartitioned | Self::Known { .. } => None,
        }
    }

    pub(crate) fn is_not_derived(&self) -> bool {
        matches!(self, Self::NotDerived { .. })
    }

    pub(crate) fn partition_count(&self) -> usize {
        match self {
            Self::Unpartitioned | Self::NotDerived { .. } => 0,
            Self::Known { partitions } => partitions.len(),
        }
    }
}
```

In `src/engine/mv/partition/mod.rs` add the module and re-export:

```rust
pub(crate) mod derivation;
pub(crate) use derivation::AffectedTargetPartitions;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib engine::mv::partition::derivation`
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/partition/derivation.rs src/engine/mv/partition/mod.rs
git commit -m "feat(mv): add unified AffectedTargetPartitions type"
```

---

### Task 2: `PartitionDerivationSpec` + contract-level `resolve_partition_derivation_spec`

The resolution phase of `derive_from_aggregate_delta` (`src/engine/mv/partition/aggregate_delta.rs:126-208`) splits in two: steps 1–2 + transform conversion are contract-only and move here; steps 3–4 (layout) move to Task 3's binder. `AffectedPartitionError` and `contract_transform_to_iceberg` move from `aggregate_delta.rs` into `derivation.rs` (with their tests); `aggregate_delta.rs` re-imports them.

**Files:**
- Modify: `src/engine/mv/partition/derivation.rs`
- Modify: `src/engine/mv/partition/aggregate_delta.rs` (remove moved items, import from `derivation`)
- Modify: `src/engine/mv/partition/mod.rs` (re-export `AffectedPartitionError` from `derivation`)

- [ ] **Step 1: Write the failing tests**

Append to the test module in `derivation.rs`. The contract fixture is a trimmed copy of `count_contract_with_partition` from `aggregate_delta.rs:720-799` — copy that function **verbatim** into this test module (it builds an `MvSchemaContract` with one visible partitionable column; bring its `use` items along: `BaseContract`, `BaseFieldRecord`, `BaseSchemaSnapshot`, `ExpressionKind`, `ExpressionLineage`, `HiddenApplyKeyContract`, `MvPartitionContract`, `MvPartitionFieldContract`, `MvSchemaContract`, `OutputColumnLineage`, `OutputContract`, `TargetContract`, `TargetVisibleColumn`, `ApplyKeySource`, `MvPartitionTransformContract` from `crate::meta::repository::mv_contract`). Then:

```rust
    #[test]
    fn resolve_returns_none_for_unpartitioned_contract() {
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition = None;
        assert!(resolve_partition_derivation_spec(&contract).unwrap().is_none());
    }

    #[test]
    fn resolve_returns_none_for_empty_partition_fields() {
        // Mirrors is_unpartitioned_mv_contract: empty fields == unpartitioned.
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition.as_mut().unwrap().fields.clear();
        assert!(resolve_partition_derivation_spec(&contract).unwrap().is_none());
    }

    #[test]
    fn resolve_produces_spec_for_pure_column_identity_partition() {
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        let spec = resolve_partition_derivation_spec(&contract)
            .expect("resolve")
            .expect("partitioned");
        assert_eq!(spec.target_spec_id, 7);
        assert_eq!(spec.fields.len(), 1);
        assert_eq!(spec.fields[0].partition_field_name, "region");
        assert_eq!(spec.fields[0].source_target_field_id, 11);
        assert_eq!(spec.fields[0].output_index, 0);
        assert_eq!(spec.fields[0].transform, iceberg::spec::Transform::Identity);
    }

    #[test]
    fn resolve_rejects_void_transform() {
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Void, 11);
        let err = resolve_partition_derivation_spec(&contract).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::TransformUnsupported { ref field, ref transform }
                if field == "region" && transform == "void"
        ));
    }

    #[test]
    fn resolve_rejects_missing_target_field() {
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition.as_mut().unwrap().fields[0].source_target_field_id = 999;
        let err = resolve_partition_derivation_spec(&contract).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::GroupKeyColumnMissing { ref field, .. } if field == "region"
        ));
    }

    #[test]
    fn resolve_rejects_non_pure_output_lineage() {
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.output.columns[0].expression.kind = ExpressionKind::Func;
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids = vec![1, 2];
        let err = resolve_partition_derivation_spec(&contract).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::OutputLineageNotPureColumn { ref field } if field == "region"
        ));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib engine::mv::partition::derivation`
Expected: compile FAIL — `resolve_partition_derivation_spec`, `PartitionDerivationSpec`, `AffectedPartitionError` (in this module) not defined.

- [ ] **Step 3: Write the implementation**

Move `AffectedPartitionError` (the enum, its `Display`, and `impl std::error::Error`, `aggregate_delta.rs:40-99`) and `contract_transform_to_iceberg` (`aggregate_delta.rs:386-404`) into `derivation.rs` unchanged. In `aggregate_delta.rs`, delete the moved items and add:

```rust
use crate::engine::mv::partition::derivation::{
    AffectedPartitionError, contract_transform_to_iceberg,
};
```

(make `contract_transform_to_iceberg` `pub(crate)` when moving). In `mod.rs`, change the `AffectedPartitionError` re-export to come from `derivation`. Move the two tests `affected_partition_error_display_includes_field_and_reason`, `contract_transform_to_iceberg_handles_all_first_class_transforms`, `contract_transform_to_iceberg_rejects_void` from `aggregate_delta.rs` tests into `derivation.rs` tests.

Then add to `derivation.rs`:

```rust
use crate::meta::repository::mv_contract::{ExpressionKind, MvSchemaContract};

/// Plan-time resolution result: which delta output column feeds each target
/// partition field, and through which Iceberg transform. Resolved purely from
/// the persisted contract — no layout / chunk dependency (spec D5: binding to
/// physical chunk columns happens in the apply-side binder).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PartitionDerivationSpec {
    pub target_spec_id: i32,
    pub fields: Vec<PartitionDerivationField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PartitionDerivationField {
    pub partition_field_name: String,
    pub source_target_field_id: i32,
    /// Position in `contract.target.visible_columns` (== output column index).
    pub output_index: usize,
    pub transform: iceberg::spec::Transform,
}

/// Resolve the contract-level partition derivation spec.
///
/// Returns `Ok(None)` for unpartitioned contracts (no `target.partition`, or
/// an empty field list — mirroring `is_unpartitioned_mv_contract`). Errors are
/// the plan-time subset of [`AffectedPartitionError`]: `TransformUnsupported`,
/// `OutputLineageNotPureColumn`, `GroupKeyColumnMissing` (contract drift).
pub(crate) fn resolve_partition_derivation_spec(
    contract: &MvSchemaContract,
) -> Result<Option<PartitionDerivationSpec>, AffectedPartitionError> {
    let Some(partition) = contract.target.partition.as_ref() else {
        return Ok(None);
    };
    if partition.fields.is_empty() {
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        let output_index = contract
            .target
            .visible_columns
            .iter()
            .position(|col| col.target_field_id == partition_field.source_target_field_id)
            .ok_or_else(|| AffectedPartitionError::GroupKeyColumnMissing {
                field: partition_field.partition_field_name.clone(),
                reason: format!(
                    "contract has no visible column for target field id {}",
                    partition_field.source_target_field_id
                ),
            })?;

        let lineage = contract.output.columns.get(output_index).ok_or_else(|| {
            AffectedPartitionError::OutputLineageNotPureColumn {
                field: partition_field.partition_field_name.clone(),
            }
        })?;
        let is_single_base_column = lineage.expression.kind == ExpressionKind::Column
            && lineage.expression.referenced_base_field_ids.len() == 1;
        let is_join_column = lineage.expression.kind == ExpressionKind::Column
            && lineage.expression.referenced_base_field_ids.is_empty()
            && lineage.expression.referenced_base_fields.len() == 1;
        if !is_single_base_column && !is_join_column {
            return Err(AffectedPartitionError::OutputLineageNotPureColumn {
                field: partition_field.partition_field_name.clone(),
            });
        }

        let transform = contract_transform_to_iceberg(
            &partition_field.transform,
            &partition_field.partition_field_name,
        )?;

        fields.push(PartitionDerivationField {
            partition_field_name: partition_field.partition_field_name.clone(),
            source_target_field_id: partition_field.source_target_field_id,
            output_index,
            transform,
        });
    }

    Ok(Some(PartitionDerivationSpec {
        target_spec_id: partition.target_spec_id,
        fields,
    }))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib engine::mv::partition`
Expected: all derivation tests PASS, all pre-existing `aggregate_delta` tests still PASS (the moved error/transform helpers resolve through the new imports).

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/partition/
git commit -m "feat(mv): add contract-level partition derivation spec resolution"
```

---

### Task 3: layout binder + chunk evaluator; `derive_from_aggregate_delta` becomes a composition

**Files:**
- Modify: `src/engine/mv/partition/derivation.rs`
- Modify: `src/engine/mv/partition/aggregate_delta.rs`

- [ ] **Step 1: Write the failing test**

Append to `derivation.rs` tests (reuse the `count_layout_with_group_key` and `batch_with_group_key` fixtures — copy them verbatim from `aggregate_delta.rs:662-718` and `:801-815`, with their `use` items):

```rust
    #[test]
    fn bind_and_evaluate_identity_partition_over_chunks() {
        use arrow::array::StringArray;
        use arrow::datatypes::DataType;
        use crate::sql::parser::ast::SqlType;

        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        let spec = resolve_partition_derivation_spec(&contract)
            .expect("resolve")
            .expect("partitioned");
        let bound = bind_spec_to_aggregate_layout(&spec, &layout).expect("bind");
        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].column_name, "region");

        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")]))
                as arrow::array::ArrayRef,
        );
        let partitions =
            evaluate_partition_spec(spec.target_spec_id, &bound, &[chunk]).expect("evaluate");
        assert_eq!(partitions.len(), 2);
        for key in &partitions {
            assert_eq!(key.spec_id, 7);
            assert_eq!(key.fields[0].field_name, "region");
        }
    }

    #[test]
    fn bind_rejects_non_group_key_output_index() {
        use arrow::datatypes::DataType;
        use crate::sql::parser::ast::SqlType;

        let mut layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        layout.group_key_source_indexes = vec![1]; // "region" (index 0) no longer a group key
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        let spec = resolve_partition_derivation_spec(&contract)
            .expect("resolve")
            .expect("partitioned");
        let err = bind_spec_to_aggregate_layout(&spec, &layout).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::OutputLineageNotPureColumn { ref field } if field == "region"
        ));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib engine::mv::partition::derivation`
Expected: compile FAIL — `bind_spec_to_aggregate_layout`, `evaluate_partition_spec`, `BoundPartitionField` not defined.

- [ ] **Step 3: Write the implementation**

Move `arrow_array_row_to_partition_value` (`aggregate_delta.rs:267-384`) into `derivation.rs` unchanged (keep it private to the module) along with its three tests (`arrow_row_to_partition_value_*` and `client_side_serialization_matches_file_metadata_path_for_primitive_literals` + `manifest_primitive_to_string` helper). Add:

```rust
use crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout;
use crate::engine::mv::partition::MvPartitionKeyField;
use crate::exec::chunk::Chunk;

/// A derivation field bound to a physical delta-chunk column name.
#[derive(Clone, Debug)]
pub(crate) struct BoundPartitionField {
    pub partition_field_name: String,
    pub column_name: String,
    pub transform: iceberg::spec::Transform,
}

/// Bind a resolved spec to an aggregate layout (steps 3-4 of the original
/// derivation): the output index must be a group key, and the physical Arrow
/// column name comes from the layout's visible columns.
pub(crate) fn bind_spec_to_aggregate_layout(
    spec: &PartitionDerivationSpec,
    layout: &AggregateMvLayout,
) -> Result<Vec<BoundPartitionField>, AffectedPartitionError> {
    let mut bound = Vec::with_capacity(spec.fields.len());
    for field in &spec.fields {
        if !layout.group_key_source_indexes.contains(&field.output_index) {
            return Err(AffectedPartitionError::OutputLineageNotPureColumn {
                field: field.partition_field_name.clone(),
            });
        }
        let column = layout.visible_columns.get(field.output_index).ok_or_else(|| {
            AffectedPartitionError::GroupKeyColumnMissing {
                field: field.partition_field_name.clone(),
                reason: format!(
                    "layout has no visible column for output index {}",
                    field.output_index
                ),
            }
        })?;
        bound.push(BoundPartitionField {
            partition_field_name: field.partition_field_name.clone(),
            column_name: column.name.clone(),
            transform: field.transform,
        });
    }
    Ok(bound)
}

/// Mechanically evaluate bound partition fields over delta chunks: apply each
/// field's Iceberg transform to its source column, then build one
/// `MvPartitionKey` per row, deduplicated into a sorted set. This is the
/// transformation + partitioning phase extracted verbatim from the original
/// `derive_from_aggregate_delta`.
pub(crate) fn evaluate_partition_spec(
    target_spec_id: i32,
    bound_fields: &[BoundPartitionField],
    delta_chunks: &[Chunk],
) -> Result<BTreeSet<MvPartitionKey>, AffectedPartitionError> {
    let mut partitions: BTreeSet<MvPartitionKey> = BTreeSet::new();

    for chunk in delta_chunks {
        if chunk.batch.num_rows() == 0 {
            continue;
        }

        let mut transformed: Vec<arrow::array::ArrayRef> = Vec::with_capacity(bound_fields.len());
        for field in bound_fields {
            let col_index = chunk
                .batch
                .schema()
                .index_of(&field.column_name)
                .map_err(|e| AffectedPartitionError::GroupKeyColumnMissing {
                    field: field.partition_field_name.clone(),
                    reason: format!("delta chunk is missing column `{}`: {e}", field.column_name),
                })?;
            let array = chunk.batch.column(col_index).clone();
            let xform =
                iceberg::transform::create_transform_function(&field.transform).map_err(|e| {
                    AffectedPartitionError::TransformFailed {
                        field: field.partition_field_name.clone(),
                        source: e.to_string(),
                    }
                })?;
            let out =
                xform
                    .transform(array)
                    .map_err(|e| AffectedPartitionError::TransformFailed {
                        field: field.partition_field_name.clone(),
                        source: e.to_string(),
                    })?;
            transformed.push(out);
        }

        let row_count = chunk.batch.num_rows();
        for row in 0..row_count {
            let mut fields = Vec::with_capacity(bound_fields.len());
            for (bound_field, array) in bound_fields.iter().zip(transformed.iter()) {
                let value = arrow_array_row_to_partition_value(
                    array.as_ref(),
                    row,
                    &bound_field.partition_field_name,
                )?;
                fields.push(MvPartitionKeyField::new(
                    bound_field.partition_field_name.clone(),
                    value,
                ));
            }
            partitions.insert(MvPartitionKey::new(target_spec_id, fields));
        }
    }

    Ok(partitions)
}
```

Then rewrite `derive_from_aggregate_delta` in `aggregate_delta.rs` as the composition (delete its in-function resolution/evaluation bodies):

```rust
pub(crate) fn derive_from_aggregate_delta(
    input: &AggregateDeltaPartitionInput<'_>,
) -> Result<AffectedAggregateTargetPartitions, AffectedPartitionError> {
    use crate::engine::mv::partition::derivation::{
        bind_spec_to_aggregate_layout, evaluate_partition_spec, resolve_partition_derivation_spec,
    };

    let Some(spec) = resolve_partition_derivation_spec(input.schema_contract)? else {
        return Ok(AffectedAggregateTargetPartitions::Unpartitioned);
    };
    let bound = bind_spec_to_aggregate_layout(&spec, input.layout)?;
    let partitions = evaluate_partition_spec(spec.target_spec_id, &bound, input.delta_chunks)?;
    Ok(AffectedAggregateTargetPartitions::Known { partitions })
}
```

Update `mod.rs` re-exports:

```rust
pub(crate) use derivation::{
    AffectedPartitionError, AffectedTargetPartitions, BoundPartitionField,
    PartitionDerivationField, PartitionDerivationSpec, bind_spec_to_aggregate_layout,
    evaluate_partition_spec, resolve_partition_derivation_spec,
};
```

- [ ] **Step 4: Run the full partition test battery**

Run: `cargo test --lib engine::mv::partition`
Expected: PASS, including ALL pre-existing `derive_*` tests in `aggregate_delta.rs` (they are the behavior lock for the extraction — `derive_identity_returns_known_partition_per_unique_value`, `derive_day_transform_normalizes_dates_to_day_buckets`, `derive_bucket_transform_uses_iceberg_hash`, `derive_unpartitioned_contract_returns_unpartitioned`, `derive_void_transform_returns_unsupported_error`, `derive_missing_target_field_returns_group_key_missing`, `derive_non_pure_output_lineage_returns_error`, `derive_missing_chunk_column_returns_group_key_missing`, `derive_empty_chunks_returns_known_empty_set`, `derive_accepts_join_aggregate_pure_column_lineage`, `derive_rejects_join_aggregate_multi_base_field_lineage`).

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/partition/
git commit -m "feat(mv): extract partition derivation binder and chunk evaluator"
```

---

### Task 4: PR-1 verification

- [ ] **Step 1: Full lib tests + lint**

```bash
cargo fmt
cargo clippy --lib -- -D warnings
cargo test --lib
```
Expected: all PASS, no new warnings. (`clippy` scope: if pre-existing warnings elsewhere make `-D warnings` impractical, run `cargo clippy --lib 2>&1 | grep partition` and require zero hits instead.)

- [ ] **Step 2: Commit any fmt fallout and push PR-1**

```bash
git add -A && git commit -m "chore: fmt" || true
```
Open PR-1 titled `feat(mv): partition derivation library (P1 PR-1)`. PR body must state: additive-only, no behavior change, extraction locked by existing aggregate_delta tests.

---

### Task 5: unify the live path onto `AffectedTargetPartitions`

`AffectedMvPartitions` (Unpartitioned / Known{new,old} / Unknown) is replaced by `AffectedTargetPartitions` everywhere live. The new/old split is dropped — its only live consumer (`target_state_partition_allow_list`, `src/engine/mv/refresh_context.rs:814-844`) unions the two sets anyway. `Unknown` reason strings are preserved verbatim as `NotDerived` reasons.

**Files:**
- Modify: `src/engine/mv/partition/key.rs` (delete `AffectedMvPartitions` + its tests)
- Modify: `src/engine/mv/partition/planner.rs` (return type + construction)
- Modify: `src/engine/mv/partition/mod.rs` (drop old re-export)
- Modify: `src/engine/mv/lifecycle.rs:110-119, 135-140` (`RefreshPlan.affected_partitions`, `IcebergRefreshPlan.affected_partitions`)
- Modify: `src/engine/mv/refresh_context.rs:66-73, 814-844` (field type + allow-list match)
- Modify: `src/engine/mv/iceberg_refresh.rs` (`noop_affected_partitions`, `plan_aggregate_mv_affected_partitions` at `:4728`, `plan_projection_filter_affected_partitions`, `unknown_join_affected_partitions`, `unknown_union_all_affected_partitions`, `log_planned_iceberg_mv_affected_partitions` at `:4789`, plus every `.is_unknown()` / `.unknown_reason()` call site)

- [ ] **Step 1: Mechanical migration**

Run `rg -n "AffectedMvPartitions" src/` to enumerate every site, then apply:

| Old | New |
|---|---|
| `AffectedMvPartitions::Unpartitioned` | `AffectedTargetPartitions::Unpartitioned` |
| `AffectedMvPartitions::known(new, old)` | `AffectedTargetPartitions::known(new.into_iter().chain(old))` |
| `AffectedMvPartitions::Known { new_partitions, old_partitions }` (match) | `AffectedTargetPartitions::Known { partitions }` |
| `AffectedMvPartitions::unknown(reason)` | `AffectedTargetPartitions::not_derived(reason)` — reason strings unchanged |
| `AffectedMvPartitions::Unknown { reason }` (match) | `AffectedTargetPartitions::NotDerived { reason }` |
| `.is_unknown()` | `.is_not_derived()` |
| `.unknown_reason()` | `.not_derived_reason()` |

In `target_state_partition_allow_list`, the Known arm becomes:

```rust
crate::engine::mv::partition::AffectedTargetPartitions::Known { partitions } => {
    Ok(Some(partitions.clone()))
}
```

(the warn-fallback `NotDerived` arm keeps the existing `tracing::warn!` wording). In `planner.rs`, `plan_affected_partitions` collects `new_partitions` / `old_partitions` exactly as today and returns `AffectedTargetPartitions::known(new_partitions.into_iter().chain(old_partitions))`. Update `planner.rs` and `key.rs` unit tests to the merged-set expectations (port `known_partitions_are_sorted_and_deduped` etc. onto the new type or delete where now redundant with Task 1 tests).

- [ ] **Step 2: Compile-fix sweep**

Run: `cargo build 2>&1 | head -50`, fix remaining type errors until clean. Then `cargo test --lib`.
Expected: PASS. Pay attention to `lifecycle.rs` test fixtures and any `iceberg_refresh.rs` tests constructing `AffectedMvPartitions` (e.g. allow-list label tests around `:16800`).

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor(mv): unify affected-partition types onto AffectedTargetPartitions"
```

---

### Task 6: delete the dead pre-cutover apply path

Each deletion below MUST be preceded by a caller check; if a symbol turns out to have a live (non-test) caller, **keep it** and record the finding in the PR description instead of deleting.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mv/iceberg_aggregate_state.rs`
- Modify: `src/engine/mv/partition/aggregate_delta.rs` (delete file content; remove module)
- Modify: `src/engine/mv/partition/mod.rs`

- [ ] **Step 1: Delete the apply shell in `iceberg_refresh.rs`**

For each symbol run `rg -n "<name>" src/` first; expected: definition + intra-dead references only.

1. `apply_iceberg_aggregate_delta_chunks` (`:4176`) — no callers (verified 2026-06-10); delete.
2. `build_aggregate_target_partition_filter` (`:4109`) — callers: (1) only; delete with its test block (`build_aggregate_target_partition_filter_*` tests around `:16678-16770`).
3. `aggregate_delta_touched_row_ids` (`:4142`) — verify; if only (1) calls it, delete.
4. `emit_aggregate_apply_event` / its `AggregateApplyEvent`-style input struct / `partition_filter_label` / `partition_filter_count` / `wrap_aggregate_apply_error` — verify each; delete those whose only callers are (1)-(3), together with the `TracingTestBuffer` event-shape tests (`:16829+`). The `iceberg_aggregate_mv.apply` and `iceberg_aggregate_mv.partition_derivation_failed` events die with the dead path; live-path observability is re-introduced in P3 (umbrella spec §6.3) — say so in a code-removal note in the PR description, and update the stale doc comment at `:4073`/`:16836` (it claims the iceberg-ivm suite exercises this path end-to-end; it does not since PR #231).

- [ ] **Step 2: Delete the dead loaders in `iceberg_aggregate_state.rs`**

Verify then delete: `load_touched_aggregate_target_state` / `_async`, `load_current_aggregate_target_state` / `_async`, `AggregateStateLookupStats`, and their tests (`:1323-1450+`). **Verify first** that `merge_aggregate_target_state` and any helper shared with the live merge-sink/codegen path stays — anything reachable from `src/engine/mv/iceberg_merge_sink.rs`, `src/sql/codegen/**`, or `refresh_context.rs` is live and must remain.

- [ ] **Step 3: Delete `derive_from_aggregate_delta` and its module**

`rg -n "derive_from_aggregate_delta|AffectedAggregateTargetPartitions|AggregateDeltaPartitionInput" src/` — after Steps 1-2 the remaining references should be `aggregate_delta.rs` itself and `mod.rs`. Delete `src/engine/mv/partition/aggregate_delta.rs` entirely (its evaluation tests were ported in Tasks 2-3; port any remaining `derive_*` test the new module does not already cover by rewriting it against `resolve + bind + evaluate` in `derivation.rs` — the eleven `derive_*` tests listed in Task 3 Step 4 must all have equivalents before the file is removed). Update `mod.rs`: remove the `aggregate_delta` module and its re-exports.

- [ ] **Step 4: Build with dead-code lint check**

```bash
cargo build 2>&1 | grep -i "warning.*never used" ; cargo test --lib
```
Expected: no new dead-code warnings introduced by the deletion (leftover orphan helpers must be caught here), all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(mv): remove dead pre-cutover aggregate apply path"
```

---

### Task 7: PR-2 verification (lib + lint)

- [ ] **Step 1:**

```bash
cargo fmt
cargo clippy --lib -- -D warnings   # or the grep-scoped variant from Task 4
cargo test --lib
```
Expected: PASS.

---

### Task 8: PR-2 verification (iceberg-ivm SQL suite)

The suite is the behavior lock proving the deletions and type unification changed nothing user-visible.

- [ ] **Step 1: Start the environment and server**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build --profile dev-opt
LOG=/tmp/novarocks-p1-server.log
NO_PROXY=127.0.0.1,localhost target/dev-opt/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { tail -20 "$LOG"; exit 1; }
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo timeout; kill -9 "$SRV_PID"; exit 1; }
```

- [ ] **Step 2: Run the suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify
```
Expected: same pass count as on `main` (run on `main` first if a baseline is needed). Kill the server afterwards (`kill $SRV_PID`).

- [ ] **Step 3: Push PR-2**

Title: `refactor(mv): unify affected-partition types and remove dead apply path (P1 PR-2)`. Body must list every deleted symbol with its caller-check evidence, and link spec §2 D5.

---

### Task 9: enable `ImvPlanAnnotation.partition`

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/annotation.rs`

- [ ] **Step 1: Write the failing test**

Append to a new test module in `annotation.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_annotation_has_no_partition_outcome() {
        let annotation = ImvPlanAnnotation::default();
        assert!(annotation.partition.is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib optimizer::rewrite::imv::annotation`
Expected: compile FAIL — no field `partition`.

- [ ] **Step 3: Implement**

Replace the placeholder struct (`annotation.rs:13-19`) with:

```rust
use crate::engine::mv::partition::PartitionDerivationSpec;

/// IMV-pipeline-level plan annotations, populated by rewrite rules and
/// returned to the refresh driver via `ImvRewriteOutcome.annotation`.
#[derive(Clone, Debug, Default)]
pub(crate) struct ImvPlanAnnotation {
    /// Partition derivation outcome. `None` means the derivation stage did
    /// not run or did not match (non-aggregate shapes in P1, or the rule was
    /// disabled via `disable_optimizer_rules`).
    pub partition: Option<ImvPartitionAnnotation>,
}

/// Plan-time partition derivation outcome (umbrella spec §4.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ImvPartitionAnnotation {
    Unpartitioned,
    /// One spec for non-union shapes; one per branch for union families (P2).
    Derivable { specs: Vec<PartitionDerivationSpec> },
    NotDerivable { reason: String },
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib optimizer::rewrite::imv`
Expected: PASS, including the pre-existing `annotation_is_default_initialized_in_extension_slot` test in `entrypoint.rs` (it compares `Debug` of two defaults — still equal).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/annotation.rs
git commit -m "feat(imv): add partition derivation slot to ImvPlanAnnotation"
```

---

### Task 10: `DerivePartitionSpecRule`

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/partition_derivation.rs`
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs` (declare module)
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs` (test fixtures + e2e tests)

- [ ] **Step 1: Write the failing e2e tests**

In `entrypoint.rs` tests, generalize the aggregate context fixture. Rename the body of `aggregate_mv_ctx()` (`entrypoint.rs:235-314`) into:

```rust
    fn aggregate_mv_ctx_customized(
        mutate: impl FnOnce(&mut crate::meta::repository::mv_contract::MvSchemaContract),
    ) -> Arc<IcebergMvRewriteContext> {
        // ...identical body, except immediately after the existing
        // `contract.aggregate = Some(...)` block insert:
        mutate(&mut contract);
        // ...rest identical (mv_def.schema_contract = Some(contract.clone()); etc.)
    }

    fn aggregate_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        aggregate_mv_ctx_customized(|_| {})
    }

    fn partitioned_aggregate_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        use crate::meta::repository::mv_contract::{
            MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract,
        };
        aggregate_mv_ctx_customized(|contract| {
            contract.target.partition = Some(MvPartitionContract {
                target_spec_id: 7,
                fields: vec![MvPartitionFieldContract {
                    partition_field_id: 1000,
                    partition_field_name: "k".to_string(),
                    source_target_field_id: 100,
                    source_column_name: "k".to_string(),
                    transform: MvPartitionTransformContract::Identity,
                }],
            });
        })
    }
```

(`make_schema_contract` exposes visible column `"k"` with `target_field_id: 100` and pure-column output lineage `referenced_base_field_ids: vec![1]` — see `refresh_context.rs:1228-1267` — so the partition field resolves.)

Then add the tests:

```rust
    use crate::sql::optimizer::rewrite::imv::annotation::ImvPartitionAnnotation;

    #[test]
    fn imv_pipeline_annotates_partition_spec_for_partitioned_aggregate() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: aggregate_plan(),
            mv_ctx: partitioned_aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect("aggregate IMV pipeline must rewrite and validate");

        let Some(ImvPartitionAnnotation::Derivable { specs }) = &outcome.annotation.partition
        else {
            panic!(
                "expected Derivable partition annotation, got {:?}",
                outcome.annotation.partition
            );
        };
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].target_spec_id, 7);
        assert_eq!(specs[0].fields.len(), 1);
        assert_eq!(specs[0].fields[0].partition_field_name, "k");
        assert_eq!(specs[0].fields[0].source_target_field_id, 100);
        assert_eq!(specs[0].fields[0].output_index, 0);
        assert_eq!(specs[0].fields[0].transform, iceberg::spec::Transform::Identity);
    }

    #[test]
    fn imv_pipeline_annotates_unpartitioned_for_plain_aggregate() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: aggregate_plan(),
            mv_ctx: aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect("aggregate IMV pipeline must rewrite and validate");
        assert_eq!(
            outcome.annotation.partition,
            Some(ImvPartitionAnnotation::Unpartitioned)
        );
    }

    #[test]
    fn imv_pipeline_annotates_not_derivable_for_non_pure_partition_lineage() {
        use crate::meta::repository::mv_contract::{
            ExpressionKind, MvPartitionContract, MvPartitionFieldContract,
            MvPartitionTransformContract,
        };
        let ctx = aggregate_mv_ctx_customized(|contract| {
            contract.target.partition = Some(MvPartitionContract {
                target_spec_id: 7,
                fields: vec![MvPartitionFieldContract {
                    partition_field_id: 1000,
                    partition_field_name: "k".to_string(),
                    source_target_field_id: 100,
                    source_column_name: "k".to_string(),
                    transform: MvPartitionTransformContract::Identity,
                }],
            });
            contract.output.columns[0].expression.kind = ExpressionKind::Func;
            contract.output.columns[0]
                .expression
                .referenced_base_field_ids = vec![1, 2];
        });
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: aggregate_plan(),
            mv_ctx: ctx,
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect("NotDerivable must not fail the rewrite");
        let Some(ImvPartitionAnnotation::NotDerivable { reason }) = &outcome.annotation.partition
        else {
            panic!("expected NotDerivable, got {:?}", outcome.annotation.partition);
        };
        assert!(reason.contains("k"), "reason must name the field: {reason}");
    }
```

Robustness note for this test: it assumes `RewriteAggregateStateRule` does not itself
reject the mutated output lineage before our stage runs. If the pipeline errors out
during aggregate rewrite, switch the trigger to a transform failure instead — keep the
lineage pure and set `transform: MvPartitionTransformContract::Void` on the partition
field; `resolve_partition_derivation_spec` then yields `TransformUnsupported` and the
annotation is still `NotDerivable` with `"k"` in the reason.

```rust

    #[test]
    fn imv_pipeline_leaves_partition_annotation_unset_for_projection_filter() {
        // Reuses the existing project-over-scan shape: no AggregateStateMerge,
        // so the rule never matches and the slot stays None (P1 scope).
        let scan = iceberg_scan_plan();
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: column_ref(1, "k", DataType::Int64, false),
                output_name: "k".to_string(),
                output_column_id: ColumnId(1),
            }],
            output_qualifier: None,
            required_output_columns: None,
        });
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: project,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect("projection/filter rewrite must succeed");
        assert!(outcome.annotation.partition.is_none());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib optimizer::rewrite::imv::entrypoint`
Expected: the four new tests FAIL (annotation stays `None` — rule does not exist yet; the first three fail on the `Some(...)` assertions).

- [ ] **Step 3: Implement the rule**

Create `src/sql/optimizer/rewrite/imv/partition_derivation.rs`:

```rust
//! Plan-time partition derivation: resolve the contract-level
//! `PartitionDerivationSpec` and record the outcome on `ImvPlanAnnotation`.
//!
//! P1 scope (umbrella spec §5.1 / D5): matches aggregate-state-merge shapes
//! only; the annotation is observability + P2 input — live pruning still
//! flows from plan-time manifest derivation, so this rule never changes the
//! plan and never fails the rewrite.

use crate::engine::mv::partition::resolve_partition_derivation_spec;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::annotation::{
    ImvExtension, ImvPartitionAnnotation, ImvPlanAnnotation,
};
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct DerivePartitionSpecRule;

impl LogicalRewriteRule for DerivePartitionSpecRule {
    fn name(&self) -> &'static str {
        "DerivePartitionSpec"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, ctx: &RewriteContext) -> bool {
        if !matches!(plan, LogicalPlan::AggregateStateMerge(_)) {
            return false;
        }
        // Resolve once per pipeline run: the annotation slot is the guard.
        ctx.extension::<ImvExtension>()
            .is_some_and(|ext| ext.annotation.partition.is_none())
    }

    fn apply(&self, _plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or("DerivePartitionSpec requires ImvExtension")?
            .clone();

        let outcome = match resolve_partition_derivation_spec(&ext.mv_ctx.schema_contract) {
            Ok(None) => ImvPartitionAnnotation::Unpartitioned,
            Ok(Some(spec)) => ImvPartitionAnnotation::Derivable { specs: vec![spec] },
            // Plan-time resolution failure is recorded, not raised: pruning is
            // an optimization and v1 policy is BestEffort everywhere (D5).
            Err(err) => ImvPartitionAnnotation::NotDerivable {
                reason: err.to_string(),
            },
        };

        tracing::info!(
            event = "iceberg_mv.partition_derivation",
            mv_id = ext.mv_ctx.mv_id,
            target = %format!(
                "{}.{}.{}",
                ext.mv_ctx.target.catalog, ext.mv_ctx.target.namespace, ext.mv_ctx.target.table
            ),
            outcome = match &outcome {
                ImvPartitionAnnotation::Unpartitioned => "unpartitioned",
                ImvPartitionAnnotation::Derivable { .. } => "derivable",
                ImvPartitionAnnotation::NotDerivable { .. } => "not_derivable",
            },
            reason = outcome_reason(&outcome),
            "IMV partition derivation spec resolved"
        );

        ctx.set_extension::<ImvExtension>(ImvExtension {
            annotation: ImvPlanAnnotation {
                partition: Some(outcome),
            },
            ..ext
        });
        Ok(RewriteResult::Unchanged)
    }
}

fn outcome_reason(outcome: &ImvPartitionAnnotation) -> &str {
    match outcome {
        ImvPartitionAnnotation::NotDerivable { reason } => reason.as_str(),
        _ => "",
    }
}
```

Declare in `src/sql/optimizer/rewrite/imv/mod.rs`: `pub(crate) mod partition_derivation;`.

Note: if `IcebergMvRewriteContext.schema_contract` is `Arc<MvSchemaContract>` (it is, per `refresh_context.rs:36-62`), pass `&ext.mv_ctx.schema_contract` — auto-deref covers it.

- [ ] **Step 4: Wire the stage (required for the e2e tests to pass)** — see Task 11 Step 1; do both before running.

- [ ] **Step 5: Run tests**

Run: `cargo test --lib optimizer::rewrite::imv`
Expected: the four Task-10 tests PASS once Task 11 Step 1 is also in.

- [ ] **Step 6: Commit** (joint with Task 11)

---

### Task 11: pipeline stage `imv-partition-derivation`

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs`
- Modify: `src/sql/optimizer/rewrite/imv/entrypoint.rs` (two stage-list assertions)

- [ ] **Step 1: Insert the stage**

In `build_imv_pipeline()` (`pipeline.rs:29-100`), after the `"imv-apply-key"` stage and before `"imv-marker-cleanup"`:

```rust
        RewriteStage::new(
            "imv-partition-derivation",
            RewritePhase::SemanticRewrite,
            vec![Box::new(DerivePartitionSpecRule) as Box<dyn LogicalRewriteRule>],
        ),
```

with `use crate::sql::optimizer::rewrite::imv::partition_derivation::DerivePartitionSpecRule;`.

- [ ] **Step 2: Update the stage-list assertions**

1. `entrypoint.rs` `unknown_disabled_rule_name_is_ignored` (`:830`): `assert_eq!(outcome.trace.stage_names().len(), 12);`
2. `entrypoint.rs` `imv_pipeline_traces_stage_names` (`:955-981`): insert `"imv-partition-derivation"` between `"imv-apply-key"` and `"imv-marker-cleanup"`.
3. Add an ordering test in `pipeline.rs` tests:

```rust
    #[test]
    fn pipeline_runs_partition_derivation_after_apply_key_before_validation() {
        let p = build_imv_pipeline();
        let names = p.stage_names();
        let ak = names.iter().position(|n| *n == "imv-apply-key").unwrap();
        let pd = names
            .iter()
            .position(|n| *n == "imv-partition-derivation")
            .expect("imv-partition-derivation stage must exist");
        let val = names.iter().position(|n| *n == "imv-validation").unwrap();
        assert!(ak < pd && pd < val, "stage order: {names:?}");
        assert!(
            p.rule_names().iter().any(|n| *n == "DerivePartitionSpec"),
            "DerivePartitionSpec must be registered"
        );
    }
```

- [ ] **Step 3: Run the full imv test battery**

Run: `cargo test --lib optimizer::rewrite::imv`
Expected: PASS, including all Task-10 tests and all pre-existing pipeline/entrypoint tests.

- [ ] **Step 4: Commit (Tasks 10+11 together)**

```bash
git add src/sql/optimizer/rewrite/imv/
git commit -m "feat(imv): add DerivePartitionSpec rewrite stage writing partition annotation"
```

---

### Task 12: `PartitionPruningPolicy` on `RefreshCapabilities`

**Files:**
- Modify: `src/engine/mv/refresh_property.rs`

- [ ] **Step 1: Write the failing test**

In `refresh_property.rs` tests:

```rust
    #[test]
    fn partition_pruning_policy_is_best_effort_for_every_shape_in_v1() {
        // D5: Required exists but is not assigned to any shape yet; tightening
        // partitioned aggregates is deferred to P2/P3.
        let contract = crate::engine::mv::refresh_context::tests_support::make_schema_contract();
        let caps = RefreshCapabilities::from_schema_contract(&contract).unwrap();
        assert_eq!(caps.partition_pruning, PartitionPruningPolicy::BestEffort);
    }
```

(`tests_support::make_schema_contract()` — `refresh_context.rs:1202` — builds a valid ProjectionFilter-shaped contract, which `from_schema_contract` accepts. If `refresh_property.rs` tests already have a local contract fixture, prefer that one for consistency.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib engine::mv::refresh_property`
Expected: compile FAIL — no `partition_pruning` field / `PartitionPruningPolicy` type.

- [ ] **Step 3: Implement**

Add to `refresh_property.rs` (near `RefreshIdentity`):

```rust
/// What a NotDerivable partition derivation outcome means for the refresh
/// (umbrella spec §4.3). v1 (D5): every shape is BestEffort — matching the
/// pre-existing warn + unpruned-scan behavior. `Required` is defined for
/// P2/P3, where partitioned aggregates may be tightened to fail fast.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartitionPruningPolicy {
    Required,
    BestEffort,
}
```

Add the field to `RefreshCapabilities` (`:131-143`):

```rust
    /// Policy applied when partition derivation reports NotDerivable.
    pub(crate) partition_pruning: PartitionPruningPolicy,
```

and in `from_schema_contract` (`:242-249`), set it in the final struct literal:

```rust
        partition_pruning: PartitionPruningPolicy::BestEffort,
```

- [ ] **Step 4: Fix construction sites and run tests**

Run `cargo build 2>&1 | head -30`; any test or production code constructing `RefreshCapabilities` literally must gain the new field (set `BestEffort`). Then `cargo test --lib engine::mv`.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/refresh_property.rs
git commit -m "feat(mv): add PartitionPruningPolicy to RefreshCapabilities (BestEffort in v1)"
```

---

### Task 13: PR-3 verification + docs

- [ ] **Step 1: Full verification**

```bash
cargo fmt
cargo clippy --lib -- -D warnings   # or grep-scoped variant
cargo test --lib
```
Then re-run the `iceberg-ivm` suite exactly as in Task 8 (env + server + `--suite iceberg-ivm --mode verify`).
Expected: identical results to the PR-2 run — the annotation/policy work is observability-only.

- [ ] **Step 2: Docs**

1. Update the roadmap entry for 任务 12 in the Obsidian vault (`~/Documents/Obsidian/NovaRocks TODO/NovaRocks Roadmap.md` 推荐执行顺序 row 12, and `partition-contract-under-rewrite.md` 状态 section): P1 landed (3 PRs — library / unification+deletion / annotation+policy), P2 (locator filter + join PF + union merge) next.
2. Verify the umbrella spec needs no further amendment; if implementation diverged anywhere, amend `docs/design/specs/2026-06-10-iceberg-mv-partition-umbrella-design.md` in the same PR.

- [ ] **Step 3: Push PR-3**

Title: `feat(imv): partition derivation annotation stage + pruning policy (P1 PR-3)`. Body: links to the umbrella spec, states "behavior unchanged; annotation consumed by tests/tracing only until P2".

---

## Self-review checklist (run after writing code, before each PR)

1. **Behavior lock:** `iceberg-ivm` pass count unchanged vs `main`; all eleven `derive_*` test equivalents exist in `derivation.rs` before `aggregate_delta.rs` is deleted.
2. **No silent fallback added:** the only `NotDerived`/`NotDerivable` consumers in P1 are the pre-existing warn path and the new tracing event.
3. **Caller-check evidence** for every deleted symbol is in the PR-2 description.
4. **Spec deviations** (if any) are written back into the umbrella spec, not left in code comments.
