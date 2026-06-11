# Iceberg MV Affected Partition Planner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a structured affected-partition result to Iceberg MV refresh planning without changing refresh execution scope.

**Architecture:** Introduce a focused `src/engine/mv/partition/` module that owns affected partition keys, mapping, and planner decisions. `plan_iceberg_mv_refresh` calls that module after snapshot-range planning and stores the result on top-level `RefreshPlan`; execution continues to use the existing refresh paths.

**Tech Stack:** Rust, Iceberg metadata/change planning, NovaRocks MV lifecycle, targeted `cargo test --lib` unit tests.

---

## File Structure

- Create `src/engine/mv/partition/mod.rs`
  - Exposes the affected partition API to the MV planner.
- Create `src/engine/mv/partition/key.rs`
  - Defines `AffectedMvPartitions`, `MvPartitionKey`, `MvPartitionKeyField`, and `MvPartitionValue`.
- Create `src/engine/mv/partition/mapping.rs`
  - Maps `MvSchemaContract.target.partition` plus structured file partition values into `MvPartitionKey`.
- Create `src/engine/mv/partition/planner.rs`
  - Plans `AffectedMvPartitions` from `IcebergChangeBatch`.
- Modify `src/engine/mv/mod.rs`
  - Registers the new `partition` module.
- Modify `src/engine/mv/lifecycle.rs`
  - Adds `RefreshPlan.affected_partitions`.
- Modify `src/engine/mv/iceberg_refresh.rs`
  - Calls the affected partition planner for Iceberg MV refresh planning.
- Modify `src/engine/mv_flow.rs`
  - Updates the mock backend test plan construction.
- Modify `src/connector/starrocks/managed/backend.rs`
  - Gives managed-lake refresh plans a conservative affected partition result.
- Modify `src/connector/iceberg/changes.rs`
  - Exposes structured partition field values on data-file change refs.
- Modify call-site tests that construct `DataFileRef`, `DeletedDataFileRef`, or `EqualityDeleteRef`
  - Adds `partition_values: Vec::new()` unless the test specifically validates partition mapping.

## Task 1: Add the affected partition result model

**Files:**
- Create: `src/engine/mv/partition/mod.rs`
- Create: `src/engine/mv/partition/key.rs`
- Modify: `src/engine/mv/mod.rs`

- [ ] **Step 1: Write model tests first**

Add this test module to the bottom of `src/engine/mv/partition/key.rs` while creating the file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn key(spec_id: i32, field: &str, value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            spec_id,
            vec![MvPartitionKeyField::new(
                field.to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn known_partitions_are_sorted_and_deduped() {
        let result = AffectedMvPartitions::known(
            [key(2, "id", "2"), key(1, "id", "1"), key(1, "id", "1")],
            [key(3, "id", "3")],
        );

        let AffectedMvPartitions::Known {
            new_partitions,
            old_partitions,
        } = result
        else {
            panic!("expected known affected partitions");
        };
        assert_eq!(new_partitions.into_iter().collect::<Vec<_>>(), vec![key(1, "id", "1"), key(2, "id", "2")]);
        assert_eq!(old_partitions.into_iter().collect::<Vec<_>>(), vec![key(3, "id", "3")]);
    }

    #[test]
    fn unknown_preserves_reason() {
        assert_eq!(
            AffectedMvPartitions::unknown("row evaluation fallback is not implemented")
                .unknown_reason(),
            Some("row evaluation fallback is not implemented")
        );
    }

    #[test]
    fn unpartitioned_is_not_unknown() {
        assert!(!AffectedMvPartitions::Unpartitioned.is_unknown());
        assert_eq!(AffectedMvPartitions::Unpartitioned.partition_count(), 0);
    }
}
```

- [ ] **Step 2: Run the failing model tests**

Run:

```bash
cargo test --lib engine::mv::partition::key
```

Expected: compile fails because `src/engine/mv/partition/key.rs` and the model types are not implemented yet.

- [ ] **Step 3: Implement the model**

Create `src/engine/mv/partition/key.rs` with:

```rust
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedMvPartitions {
    Unpartitioned,
    Known {
        new_partitions: BTreeSet<MvPartitionKey>,
        old_partitions: BTreeSet<MvPartitionKey>,
    },
    Unknown {
        reason: String,
    },
}

impl AffectedMvPartitions {
    pub(crate) fn known<N, O>(new_partitions: N, old_partitions: O) -> Self
    where
        N: IntoIterator<Item = MvPartitionKey>,
        O: IntoIterator<Item = MvPartitionKey>,
    {
        Self::Known {
            new_partitions: new_partitions.into_iter().collect(),
            old_partitions: old_partitions.into_iter().collect(),
        }
    }

    pub(crate) fn unknown(reason: impl Into<String>) -> Self {
        Self::Unknown {
            reason: reason.into(),
        }
    }

    pub(crate) fn unknown_reason(&self) -> Option<&str> {
        match self {
            Self::Unknown { reason } => Some(reason.as_str()),
            Self::Unpartitioned | Self::Known { .. } => None,
        }
    }

    pub(crate) fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown { .. })
    }

    pub(crate) fn partition_count(&self) -> usize {
        match self {
            Self::Unpartitioned | Self::Unknown { .. } => 0,
            Self::Known {
                new_partitions,
                old_partitions,
            } => new_partitions.len() + old_partitions.len(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MvPartitionKey {
    pub spec_id: i32,
    pub fields: Vec<MvPartitionKeyField>,
}

impl MvPartitionKey {
    pub(crate) fn new(spec_id: i32, fields: Vec<MvPartitionKeyField>) -> Self {
        Self { spec_id, fields }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MvPartitionKeyField {
    pub field_name: String,
    pub value: MvPartitionValue,
}

impl MvPartitionKeyField {
    pub(crate) fn new(field_name: String, value: MvPartitionValue) -> Self {
        Self { field_name, value }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MvPartitionValue {
    Null,
    String(String),
}
```

Create `src/engine/mv/partition/mod.rs` with:

```rust
pub(crate) mod key;

pub(crate) use key::{
    AffectedMvPartitions, MvPartitionKey, MvPartitionKeyField, MvPartitionValue,
};
```

Modify `src/engine/mv/mod.rs`:

```rust
pub(crate) mod partition;
```

- [ ] **Step 4: Run the model tests**

Run:

```bash
cargo test --lib engine::mv::partition::key
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/mod.rs src/engine/mv/partition/mod.rs src/engine/mv/partition/key.rs
git commit -m "feat: add mv affected partition model"
```

## Task 2: Add affected partitions to RefreshPlan

**Files:**
- Modify: `src/engine/mv/lifecycle.rs`
- Modify: `src/engine/mv_flow.rs`
- Modify: `src/connector/starrocks/managed/backend.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add the field to `RefreshPlan`**

Modify `src/engine/mv/lifecycle.rs`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct RefreshPlan {
    pub mv_id: Option<i64>,
    pub target: MvTarget,
    pub storage_engine: MvStorageEngine,
    pub mode: RefreshMode,
    pub base_refs: Vec<MvBaseRef>,
    pub snapshot_pins: BTreeMap<String, Option<i64>>,
    pub affected_partitions: crate::engine::mv::partition::AffectedMvPartitions,
    pub backend_plan: BackendRefreshPlan,
}
```

- [ ] **Step 2: Update non-Iceberg refresh plan constructors**

In `src/engine/mv_flow.rs`, add this field in the mock backend `RefreshPlan`:

```rust
affected_partitions: crate::engine::mv::partition::AffectedMvPartitions::unknown(
    "mock MV backend does not plan affected partitions",
),
```

In `src/connector/starrocks/managed/backend.rs`, add:

```rust
affected_partitions: crate::engine::mv::partition::AffectedMvPartitions::unknown(
    "managed-lake MV partition planning is not implemented",
),
```

- [ ] **Step 3: Update Iceberg refresh plan constructors with temporary conservative values**

In each `RefreshPlan` returned by `src/engine/mv/iceberg_refresh.rs::plan_iceberg_mv_refresh`, add:

```rust
affected_partitions: crate::engine::mv::partition::AffectedMvPartitions::unknown(
    "iceberg MV affected partition planning is not wired",
),
```

This compiles the lifecycle API before the real planner is introduced in later tasks.

- [ ] **Step 4: Run a focused compile check**

Run:

```bash
cargo test --lib engine::mv::partition::key --no-run
```

Expected: compile succeeds.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/lifecycle.rs src/engine/mv_flow.rs src/connector/starrocks/managed/backend.rs src/engine/mv/iceberg_refresh.rs
git commit -m "feat: carry affected partitions in refresh plans"
```

## Task 3: Expose structured file partition metadata in IcebergChangeBatch

**Files:**
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/connector/starrocks/managed/ivm_delta_source.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Modify: `src/exec/operators/iceberg_delta_scan.rs`

- [ ] **Step 1: Extend the existing metadata-preservation test**

Modify `src/connector/iceberg/changes.rs::tests::data_file_ref_preserves_partition_and_lineage_metadata` so the synthetic file carries structured partition metadata:

```rust
let file = super::DataFileRef {
    path: "s3://bucket/t/data.parquet".to_string(),
    size: 10,
    record_count: Some(2),
    partition_spec_id: Some(4),
    partition_key: Some("city=A".to_string()),
    partition_values: vec![super::ChangePartitionFieldValue {
        source_column: "city".to_string(),
        field_name: "city".to_string(),
        transform: "identity".to_string(),
        value: Some("A".to_string()),
    }],
    first_row_id: Some(100),
    data_sequence_number: Some(12),
};

assert_eq!(file.partition_spec_id, Some(4));
assert_eq!(file.partition_key.as_deref(), Some("city=A"));
assert_eq!(file.partition_values[0].source_column, "city");
assert_eq!(file.partition_values[0].field_name, "city");
assert_eq!(file.partition_values[0].transform, "identity");
assert_eq!(file.partition_values[0].value.as_deref(), Some("A"));
assert_eq!(file.first_row_id, Some(100));
assert_eq!(file.data_sequence_number, Some(12));
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cargo test --lib connector::iceberg::changes::tests::data_file_ref_preserves_partition_and_lineage_metadata
```

Expected: compile fails because `ChangePartitionFieldValue` and `partition_values` do not exist.

- [ ] **Step 3: Add the structured value type and fields**

In `src/connector/iceberg/changes.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChangePartitionFieldValue {
    pub source_column: String,
    pub field_name: String,
    pub transform: String,
    pub value: Option<String>,
}
```

Add `partition_values: Vec<ChangePartitionFieldValue>` to these structs:

```rust
pub(crate) struct DataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}

pub(crate) struct DeletedDataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}

pub(crate) struct EqualityDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub equality_ids: Vec<i32>,
    pub sequence_number: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
}
```

- [ ] **Step 4: Populate structured values while collecting change files**

Change `collect_files` helper calls so metadata is passed through:

```rust
collect_added_data_files_for_manifest_list(
    metadata,
    snapshot_id,
    file_io,
    &manifest_list,
    &mut inserts,
)
.await?;
```

Use the same `metadata` argument for `collect_deleted_data_files_for_manifest_list` and `collect_added_delete_files_for_manifest_list`.

Add this helper in `src/connector/iceberg/changes.rs`:

```rust
fn change_partition_field_values(
    metadata: &iceberg::spec::TableMetadata,
    spec_id: i32,
    partition: &iceberg::spec::Struct,
) -> Result<Vec<ChangePartitionFieldValue>, ChangeError> {
    let Some(spec) = metadata.partition_spec_by_id(spec_id) else {
        return Err(ChangeError::InternalInconsistency(format!(
            "iceberg table metadata missing partition spec id {spec_id}"
        )));
    };
    let schema = metadata.current_schema();
    let mut values = Vec::with_capacity(spec.fields().len());
    for (idx, field) in spec.fields().iter().enumerate() {
        let source_column = schema
            .field_by_id(field.source_id)
            .map(|source| source.name.clone())
            .unwrap_or_else(|| format!("#{}", field.source_id));
        let value = partition
            .fields()
            .get(idx)
            .and_then(|literal| literal.as_ref())
            .and_then(change_partition_value_string);
        values.push(ChangePartitionFieldValue {
            source_column,
            field_name: field.name.clone(),
            transform: change_partition_transform_name(&field.transform),
            value,
        });
    }
    Ok(values)
}
```

Add transform/value helpers:

```rust
fn change_partition_transform_name(transform: &iceberg::spec::Transform) -> String {
    match transform {
        iceberg::spec::Transform::Identity => "identity".to_string(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

fn change_partition_value_string(literal: &iceberg::spec::Literal) -> Option<String> {
    let iceberg::spec::Literal::Primitive(value) = literal else {
        return None;
    };
    match value {
        iceberg::spec::PrimitiveLiteral::Boolean(v) => Some(v.to_string()),
        iceberg::spec::PrimitiveLiteral::Int(v) => Some(v.to_string()),
        iceberg::spec::PrimitiveLiteral::Long(v) => Some(v.to_string()),
        iceberg::spec::PrimitiveLiteral::Float(v) => Some(v.0.to_string()),
        iceberg::spec::PrimitiveLiteral::Double(v) => Some(v.0.to_string()),
        iceberg::spec::PrimitiveLiteral::String(v) => Some(v.clone()),
        iceberg::spec::PrimitiveLiteral::Binary(v) => Some(format!("{v:?}")),
        iceberg::spec::PrimitiveLiteral::Int128(_)
        | iceberg::spec::PrimitiveLiteral::UInt128(_)
        | iceberg::spec::PrimitiveLiteral::AboveMax
        | iceberg::spec::PrimitiveLiteral::BelowMin => None,
    }
}
```

When constructing `DataFileRef`, `DeletedDataFileRef`, and `EqualityDeleteRef`, add:

```rust
partition_values: change_partition_field_values(
    metadata,
    manifest_file.partition_spec_id,
    df.partition(),
)?,
```

- [ ] **Step 5: Update synthetic constructors**

For synthetic constructors in these files, add `partition_values: Vec::new()`:

```rust
src/connector/starrocks/managed/ivm_delta_source.rs
src/connector/starrocks/managed/mv_refresh.rs
src/exec/operators/iceberg_delta_scan.rs
src/connector/iceberg/changes.rs
```

- [ ] **Step 6: Run the metadata test**

Run:

```bash
cargo test --lib connector::iceberg::changes::tests::data_file_ref_preserves_partition_and_lineage_metadata
```

Expected: test passes.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/changes.rs src/connector/starrocks/managed/ivm_delta_source.rs src/connector/starrocks/managed/mv_refresh.rs src/exec/operators/iceberg_delta_scan.rs
git commit -m "feat: expose iceberg change partition values"
```

## Task 4: Implement partition mapping from contract to file metadata

**Files:**
- Create: `src/engine/mv/partition/mapping.rs`
- Modify: `src/engine/mv/partition/mod.rs`

- [ ] **Step 1: Write mapping tests**

Add tests in `src/engine/mv/partition/mapping.rs` that build a minimal `MvSchemaContract` with one identity partition field and one `ChangePartitionFieldValue`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::changes::ChangePartitionFieldValue;
    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };

    fn contract_with_identity_partition() -> MvSchemaContract {
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
                        name_at_create: "id".to_string(),
                        type_signature: "int".to_string(),
                        required: true,
                    }],
                },
            },
            bases: Vec::new(),
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: Vec::new(),
                    },
                }],
                filter: None,
            },
            join: None,
            target: TargetContract {
                table_fqn: "ice.analytics.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 10,
                    type_signature: "int".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 11,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: Some(MvPartitionContract {
                    target_spec_id: 7,
                    fields: vec![MvPartitionFieldContract {
                        partition_field_id: 100,
                        partition_field_name: "id".to_string(),
                        source_target_field_id: 10,
                        source_column_name: "id".to_string(),
                        transform: MvPartitionTransformContract::Identity,
                    }],
                }),
            },
        }
    }

    #[test]
    fn maps_identity_partition_value_to_mv_key() {
        let contract = contract_with_identity_partition();
        let values = vec![ChangePartitionFieldValue {
            source_column: "id".to_string(),
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: Some("42".to_string()),
        }];

        let key = map_file_partition_to_mv_key(&contract, 7, &values)
            .expect("mapping result")
            .expect("partition key");

        assert_eq!(key.spec_id, 7);
        assert_eq!(key.fields[0].field_name, "id");
        assert_eq!(key.fields[0].value, MvPartitionValue::String("42".to_string()));
    }

    #[test]
    fn returns_none_for_unpartitioned_contract() {
        let mut contract = contract_with_identity_partition();
        contract.target.partition = None;
        assert_eq!(
            map_file_partition_to_mv_key(&contract, 7, &[]).expect("mapping result"),
            None
        );
    }
}
```

- [ ] **Step 2: Run the failing mapping tests**

Run:

```bash
cargo test --lib engine::mv::partition::mapping
```

Expected: compile fails because `mapping.rs` and `map_file_partition_to_mv_key` do not exist.

- [ ] **Step 3: Implement mapping**

Create `src/engine/mv/partition/mapping.rs` with:

```rust
use crate::connector::iceberg::changes::ChangePartitionFieldValue;
use crate::engine::mv::partition::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};
use crate::meta::repository::mv_contract::{
    ExpressionKind, MvPartitionTransformContract, MvSchemaContract,
};

pub(crate) fn map_file_partition_to_mv_key(
    contract: &MvSchemaContract,
    file_spec_id: i32,
    file_partition_values: &[ChangePartitionFieldValue],
) -> Result<Option<MvPartitionKey>, String> {
    let Some(partition) = contract.target.partition.as_ref() else {
        return Ok(None);
    };
    let mut fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        if partition_field.transform != MvPartitionTransformContract::Identity {
            return Err(format!(
                "MV partition field {} uses unsupported transform {:?}",
                partition_field.partition_field_name, partition_field.transform
            ));
        }
        let target_index = contract
            .target
            .visible_columns
            .iter()
            .position(|column| column.target_field_id == partition_field.source_target_field_id)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} references missing target field id {}",
                    partition_field.partition_field_name,
                    partition_field.source_target_field_id
                )
            })?;
        let output = contract.output.columns.get(target_index).ok_or_else(|| {
            format!(
                "MV partition field {} references output index {} beyond contract output length {}",
                partition_field.partition_field_name,
                target_index,
                contract.output.columns.len()
            )
        })?;
        if output.expression.kind != ExpressionKind::Column
            || output.expression.referenced_base_field_ids.len() != 1
        {
            return Err(format!(
                "MV partition field {} requires row-evaluation fallback",
                partition_field.partition_field_name
            ));
        }
        let base_field_id = output.expression.referenced_base_field_ids[0];
        let base_field = contract
            .base
            .schema_at_create
            .fields
            .iter()
            .find(|field| field.field_id == base_field_id)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} references missing base field id {}",
                    partition_field.partition_field_name, base_field_id
                )
            })?;
        let file_value = file_partition_values
            .iter()
            .find(|value| value.source_column == base_field.name_at_create && value.transform == "identity")
            .ok_or_else(|| {
                format!(
                    "MV partition field {} cannot be proven from Iceberg file partition metadata",
                    partition_field.partition_field_name
                )
            })?;
        fields.push(MvPartitionKeyField::new(
            partition_field.partition_field_name.clone(),
            match file_value.value.as_ref() {
                Some(value) => MvPartitionValue::String(value.clone()),
                None => MvPartitionValue::Null,
            },
        ));
    }
    Ok(Some(MvPartitionKey::new(file_spec_id, fields)))
}
```

Modify `src/engine/mv/partition/mod.rs`:

```rust
pub(crate) mod key;
pub(crate) mod mapping;

pub(crate) use key::{
    AffectedMvPartitions, MvPartitionKey, MvPartitionKeyField, MvPartitionValue,
};
```

- [ ] **Step 4: Run mapping tests**

Run:

```bash
cargo test --lib engine::mv::partition::mapping
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/partition/mod.rs src/engine/mv/partition/mapping.rs
git commit -m "feat: map mv partition contract to file partitions"
```

## Task 5: Implement the affected partition planner

**Files:**
- Create: `src/engine/mv/partition/planner.rs`
- Modify: `src/engine/mv/partition/mod.rs`

- [ ] **Step 1: Write planner tests**

Add tests in `src/engine/mv/partition/planner.rs` for append-only, overwrite old/new, and row-level delete unknown. Duplicate the `contract_with_identity_partition()` helper from Task 4 inside this test module so the file is self-contained.

Use this append-only test:

```rust
#[test]
fn append_only_insert_returns_new_partitions() {
    let contract = contract_with_identity_partition();
    let batch = IcebergChangeBatch {
        previous_snapshot_id: 10,
        current_snapshot_id: 11,
        inserts: vec![DataFileRef {
            path: "file:///data-1.parquet".to_string(),
            size: 128,
            record_count: Some(1),
            partition_spec_id: Some(7),
            partition_key: Some("id=42".to_string()),
            partition_values: vec![ChangePartitionFieldValue {
                source_column: "id".to_string(),
                field_name: "id".to_string(),
                transform: "identity".to_string(),
                value: Some("42".to_string()),
            }],
            first_row_id: Some(100),
            data_sequence_number: Some(1),
        }],
        deletes: Vec::new(),
        equality_deletes: Vec::new(),
        deleted_data_files: Vec::new(),
    };

    let result = plan_affected_partitions(&AffectedPartitionPlanInput {
        schema_contract: &contract,
        change_batch: Some(&batch),
    });

    let AffectedMvPartitions::Known {
        new_partitions,
        old_partitions,
    } = result
    else {
        panic!("expected known affected partitions");
    };
    assert_eq!(new_partitions.len(), 1);
    assert!(old_partitions.is_empty());
}
```

Use this overwrite test:

```rust
#[test]
fn overwrite_diff_returns_new_and_old_partitions() {
    let contract = contract_with_identity_partition();
    let batch = IcebergChangeBatch {
        previous_snapshot_id: 10,
        current_snapshot_id: 11,
        inserts: vec![DataFileRef {
            path: "file:///new.parquet".to_string(),
            size: 128,
            record_count: Some(1),
            partition_spec_id: Some(7),
            partition_key: Some("id=2".to_string()),
            partition_values: vec![ChangePartitionFieldValue {
                source_column: "id".to_string(),
                field_name: "id".to_string(),
                transform: "identity".to_string(),
                value: Some("2".to_string()),
            }],
            first_row_id: Some(200),
            data_sequence_number: Some(2),
        }],
        deletes: Vec::new(),
        equality_deletes: Vec::new(),
        deleted_data_files: vec![DeletedDataFileRef {
            path: "file:///old.parquet".to_string(),
            size: 128,
            record_count: Some(1),
            partition_spec_id: Some(7),
            partition_key: Some("id=1".to_string()),
            partition_values: vec![ChangePartitionFieldValue {
                source_column: "id".to_string(),
                field_name: "id".to_string(),
                transform: "identity".to_string(),
                value: Some("1".to_string()),
            }],
            first_row_id: Some(100),
            data_sequence_number: Some(1),
        }],
    };

    let result = plan_affected_partitions(&AffectedPartitionPlanInput {
        schema_contract: &contract,
        change_batch: Some(&batch),
    });

    let AffectedMvPartitions::Known {
        new_partitions,
        old_partitions,
    } = result
    else {
        panic!("expected known affected partitions");
    };
    assert_eq!(new_partitions.len(), 1);
    assert_eq!(old_partitions.len(), 1);
}
```

Use this row-level delete test:

```rust
#[test]
fn position_delete_returns_unknown() {
    let contract = contract_with_identity_partition();
    let batch = IcebergChangeBatch {
        previous_snapshot_id: 10,
        current_snapshot_id: 11,
        inserts: Vec::new(),
        deletes: vec![PositionDeleteRef {
            delete_file_path: "file:///delete.parquet".to_string(),
            delete_file_size: 64,
            record_count: Some(1),
            referenced_data_file: Some("file:///data.parquet".to_string()),
            file_format: iceberg::spec::DataFileFormat::Parquet,
            content_offset: None,
            content_size_in_bytes: None,
        }],
        equality_deletes: Vec::new(),
        deleted_data_files: Vec::new(),
    };
    let input = AffectedPartitionPlanInput {
        schema_contract: &contract,
        change_batch: Some(&batch),
    };

    assert_eq!(
        plan_affected_partitions(&input).unknown_reason(),
        Some("row-level delete affected partitions require row-evaluation fallback")
    );
}
```

- [ ] **Step 2: Run the failing planner tests**

Run:

```bash
cargo test --lib engine::mv::partition::planner
```

Expected: compile fails because `planner.rs` and `plan_affected_partitions` do not exist.

- [ ] **Step 3: Implement planner logic**

Create `src/engine/mv/partition/planner.rs` with:

```rust
use crate::connector::iceberg::changes::IcebergChangeBatch;
use crate::engine::mv::partition::mapping::map_file_partition_to_mv_key;
use crate::engine::mv::partition::{AffectedMvPartitions, MvPartitionKey};
use crate::meta::repository::mv_contract::MvSchemaContract;

pub(crate) struct AffectedPartitionPlanInput<'a> {
    pub schema_contract: &'a MvSchemaContract,
    pub change_batch: Option<&'a IcebergChangeBatch>,
}

pub(crate) fn plan_affected_partitions(
    input: &AffectedPartitionPlanInput<'_>,
) -> AffectedMvPartitions {
    if input.schema_contract.target.partition.is_none() {
        return AffectedMvPartitions::Unpartitioned;
    }
    let Some(batch) = input.change_batch else {
        return AffectedMvPartitions::unknown(
            "full refresh affected partition planning is not implemented",
        );
    };
    if !batch.deletes.is_empty() || !batch.equality_deletes.is_empty() {
        return AffectedMvPartitions::unknown(
            "row-level delete affected partitions require row-evaluation fallback",
        );
    }

    let mut new_partitions = Vec::<MvPartitionKey>::new();
    for file in &batch.inserts {
        let Some(spec_id) = file.partition_spec_id else {
            return AffectedMvPartitions::unknown(format!(
                "inserted data file {} is missing partition spec id",
                file.path
            ));
        };
        match map_file_partition_to_mv_key(input.schema_contract, spec_id, &file.partition_values) {
            Ok(Some(key)) => new_partitions.push(key),
            Ok(None) => return AffectedMvPartitions::Unpartitioned,
            Err(reason) => return AffectedMvPartitions::unknown(reason),
        }
    }

    let mut old_partitions = Vec::<MvPartitionKey>::new();
    for file in &batch.deleted_data_files {
        let Some(spec_id) = file.partition_spec_id else {
            return AffectedMvPartitions::unknown(format!(
                "deleted data file {} is missing partition spec id",
                file.path
            ));
        };
        match map_file_partition_to_mv_key(input.schema_contract, spec_id, &file.partition_values) {
            Ok(Some(key)) => old_partitions.push(key),
            Ok(None) => return AffectedMvPartitions::Unpartitioned,
            Err(reason) => return AffectedMvPartitions::unknown(reason),
        }
    }

    AffectedMvPartitions::known(new_partitions, old_partitions)
}
```

Modify `src/engine/mv/partition/mod.rs`:

```rust
pub(crate) mod key;
pub(crate) mod mapping;
pub(crate) mod planner;
```

- [ ] **Step 4: Run planner tests**

Run:

```bash
cargo test --lib engine::mv::partition::planner
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/partition/mod.rs src/engine/mv/partition/planner.rs src/engine/mv/partition/mapping.rs
git commit -m "feat: plan affected mv partitions from iceberg changes"
```

## Task 6: Wire the planner into Iceberg MV refresh planning

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add helper functions near `plan_iceberg_mv_refresh`**

Add:

```rust
fn unknown_join_affected_partitions() -> crate::engine::mv::partition::AffectedMvPartitions {
    crate::engine::mv::partition::AffectedMvPartitions::unknown(
        "join MV affected partition planning is not implemented",
    )
}

fn noop_affected_partitions(
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
) -> crate::engine::mv::partition::AffectedMvPartitions {
    if schema_contract.target.partition.is_none() {
        crate::engine::mv::partition::AffectedMvPartitions::Unpartitioned
    } else {
        crate::engine::mv::partition::AffectedMvPartitions::known(
            std::iter::empty::<crate::engine::mv::partition::MvPartitionKey>(),
            std::iter::empty::<crate::engine::mv::partition::MvPartitionKey>(),
        )
    }
}
```

- [ ] **Step 2: Plan affected partitions for incremental single-base refresh**

In `plan_iceberg_mv_refresh`, after `mode` is computed and before returning `RefreshPlan`, add:

```rust
let affected_partitions = match mode {
    RefreshMode::Noop => noop_affected_partitions(schema_contract),
    RefreshMode::Incremental => {
        let previous = previous_snapshot_id.expect("incremental refresh has previous snapshot");
        let current = current_snapshot_id.expect("incremental refresh has current snapshot");
        match plan_changes(&loaded.table, previous, Some(current), &[]) {
            Ok(batch) => crate::engine::mv::partition::planner::plan_affected_partitions(
                &crate::engine::mv::partition::planner::AffectedPartitionPlanInput {
                    schema_contract,
                    change_batch: Some(&batch),
                },
            ),
            Err(err) => crate::engine::mv::partition::AffectedMvPartitions::unknown(format!(
                "failed to plan Iceberg changes for affected partitions: {err}"
            )),
        }
    }
    RefreshMode::Full | RefreshMode::Rebuild => {
        crate::engine::mv::partition::planner::plan_affected_partitions(
            &crate::engine::mv::partition::planner::AffectedPartitionPlanInput {
                schema_contract,
                change_batch: None,
            },
        )
    }
};
```

Use this variable in the single-base `RefreshPlan`:

```rust
affected_partitions,
```

- [ ] **Step 3: Wire no-op empty-base and join branches**

For the empty-base no-op branch, use:

```rust
affected_partitions: noop_affected_partitions(schema_contract),
```

For the join branch, use:

```rust
affected_partitions: unknown_join_affected_partitions(),
```

- [ ] **Step 4: Add planning log**

Before each Iceberg `RefreshPlan` return, log the result:

```rust
tracing::info!(
    target = %format!("{}.{}.{}", iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table),
    affected_partitions = ?affected_partitions,
    "planned iceberg MV affected partitions"
);
```

For branches where the expression is inline, bind it to a local `affected_partitions` first so the log and `RefreshPlan` use the same value.

- [ ] **Step 5: Run a focused compile check**

Run:

```bash
cargo test --lib engine::mv::partition --no-run
```

Expected: compile succeeds.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat: wire affected partitions into iceberg mv planning"
```

## Task 7: Add refresh planner tests

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add a partitioned base-table helper**

In the `#[cfg(test)]` module in `src/engine/mv/iceberg_refresh.rs`, add:

```rust
fn create_identity_partitioned_base_table(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    table: &str,
) {
    let entry = {
        let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
        catalogs.get(catalog).expect("catalog")
    };
    let columns = vec![
        crate::sql::TableColumnDef {
            name: "id".to_string(),
            data_type: crate::sql::SqlType::Int,
            nullable: false,
            aggregation: None,
            default: None,
        },
        crate::sql::TableColumnDef {
            name: "name".to_string(),
            data_type: crate::sql::SqlType::String,
            nullable: true,
            aggregation: None,
            default: None,
        },
    ];
    crate::connector::iceberg::catalog::registry::create_table(
        &entry,
        namespace,
        table,
        &columns,
        None,
        &[crate::sql::parser::ast::IcebergPartitionFieldExpr::Identity {
            column: "id".to_string(),
        }],
        &[
            ("format-version".to_string(), "3".to_string()),
            ("write.row-lineage".to_string(), "true".to_string()),
        ],
    )
    .expect("create identity-partitioned iceberg table");
}
```

- [ ] **Step 2: Add append-only planner test**

Add:

```rust
#[test]
fn plan_iceberg_mv_refresh_reports_append_insert_affected_partitions() {
    let env = open_test_state_with_iceberg_catalog("ice", "analytics");
    create_identity_partitioned_base_table(&env.state, "ice", "sales", "orders");
    insert_into_iceberg_table(&env.state, "ice", "sales", "orders", &[(1, "a")]);
    let stmt = parse_create_mv(
        "CREATE MATERIALIZED VIEW mv_orders
         PARTITION BY (id)
         DISTRIBUTED BY HASH(id) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT id, name FROM ice.sales.orders",
    );
    create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
        .expect("create partitioned iceberg mv");

    let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
    refresh_iceberg_mv(&env.state, Some("ice"), &env.current_db, &refresh)
        .expect("first refresh");
    insert_into_iceberg_table(&env.state, "ice", "sales", "orders", &[(2, "b")]);

    let target = crate::engine::mv::lifecycle::MvTarget {
        catalog: Some("ice".to_string()),
        database: "analytics".to_string(),
        name: "mv_orders".to_string(),
    };
    let plan =
        plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
            .expect("second refresh plan");

    let crate::engine::mv::partition::AffectedMvPartitions::Known {
        new_partitions,
        old_partitions,
    } = plan.affected_partitions
    else {
        panic!("expected known affected partitions: {:?}", plan.affected_partitions);
    };
    assert_eq!(new_partitions.len(), 1);
    assert!(old_partitions.is_empty());
}
```

- [ ] **Step 3: Add unpartitioned and join unknown tests**

Add:

```rust
#[test]
fn plan_iceberg_mv_refresh_reports_unpartitioned_for_unpartitioned_mv() {
    let env = open_test_state_with_iceberg_catalog("ice", "analytics");
    create_base_table_with_rows(&env.state, "ice", "sales", "orders", &[(1, "a")]);
    create_mv_and_refresh_once(&env.state, Some("ice"), &env.current_db, "mv_orders");
    insert_into_iceberg_table(&env.state, "ice", "sales", "orders", &[(2, "b")]);

    let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
    let target = crate::engine::mv::lifecycle::MvTarget {
        catalog: Some("ice".to_string()),
        database: "analytics".to_string(),
        name: "mv_orders".to_string(),
    };
    let plan =
        plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
            .expect("refresh plan");

    assert_eq!(
        plan.affected_partitions,
        crate::engine::mv::partition::AffectedMvPartitions::Unpartitioned
    );
}
```

Add this join test:

```rust
#[test]
fn plan_iceberg_mv_refresh_reports_unknown_for_join_mv() {
    let env = open_test_state_with_iceberg_catalog("ice", "analytics");
    create_base_table(&env.state, "ice", "sales", "left_orders");
    create_base_table(&env.state, "ice", "sales", "right_orders");
    let stmt = parse_create_mv(
        "CREATE MATERIALIZED VIEW mv_join_orders
         DISTRIBUTED BY HASH(id) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT l.id, l.name
            FROM ice.sales.left_orders l
            JOIN ice.sales.right_orders r ON l.id = r.id",
    );
    create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
        .expect("create join iceberg mv");

    let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_join_orders");
    let target = crate::engine::mv::lifecycle::MvTarget {
        catalog: Some("ice".to_string()),
        database: "analytics".to_string(),
        name: "mv_join_orders".to_string(),
    };
    let plan =
        plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
            .expect("join refresh plan");

    assert_eq!(
        plan.affected_partitions.unknown_reason(),
        Some("join MV affected partition planning is not implemented")
    );
}
```

- [ ] **Step 4: Run the refresh planner tests**

Run:

```bash
cargo test --lib plan_iceberg_mv_refresh_reports_append_insert_affected_partitions
cargo test --lib plan_iceberg_mv_refresh_reports_unpartitioned_for_unpartitioned_mv
cargo test --lib plan_iceberg_mv_refresh_reports_unknown_for_join_mv
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "test: cover iceberg mv affected partition planning"
```

## Task 8: Final verification

**Files:**
- No new source files. This task verifies the full PR2 surface.

- [ ] **Step 1: Run formatting**

Run:

```bash
cargo fmt
```

Expected: command exits 0.

- [ ] **Step 2: Run focused Rust tests**

Run:

```bash
cargo test --lib engine::mv::partition
cargo test --lib connector::iceberg::changes::tests::data_file_ref_preserves_partition_and_lineage_metadata
cargo test --lib plan_iceberg_mv_refresh_reports_append_insert_affected_partitions
cargo test --lib plan_iceberg_mv_refresh_reports_unpartitioned_for_unpartitioned_mv
```

Expected: all tests pass.

- [ ] **Step 3: Run compile-level regression check**

Run:

```bash
cargo test --lib --no-run
```

Expected: compile succeeds.

- [ ] **Step 4: Run diff hygiene**

Run:

```bash
git diff --check
```

Expected: no output and exit 0.

- [ ] **Step 5: Commit any formatting-only changes**

If `cargo fmt` changed files after the last task commit, commit only those formatting changes:

```bash
git status -sb
git add src/engine/mv src/connector/iceberg src/connector/starrocks/managed src/exec/operators/iceberg_delta_scan.rs
git commit -m "style: format affected partition planner changes"
```

If `git status -sb` is clean, skip this commit.
