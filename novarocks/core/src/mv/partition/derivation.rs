// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Unified partition-derivation domain for Iceberg MV refresh.
//!
//! `AffectedTargetPartitions` is the single result type for every affected-
//! partition source (plan-time manifest planning and delta-chunk evaluation).
//! `NotDerived` carries an explicit reason; consumers decide via
//! `PartitionPruningPolicy` (BestEffort in v1, spec D5) whether that means
//! "no pruning" or "fail the refresh".

use std::collections::BTreeSet;

use crate::exec::chunk::Chunk;
use crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout;
use crate::mv::model::{MvPartitionKey, MvPartitionKeyField};
use crate::mv::persistence::schema as mv_schema;
#[cfg(test)]
use crate::runtime::query_result::record_batch_to_chunk;
use mv_schema::{ExpressionKind, MvSchemaContract};

/// Reasons aggregate-delta partition derivation can refuse a delta batch.
/// Every variant carries enough context for the refresh error message to
/// name the failing field and / or value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedPartitionError {
    /// The schema contract has no `target.partition` but the caller expected
    /// a partitioned MV. Only raised when the layout reports group-key columns
    /// but the contract is unpartitioned (callers should treat `partition =
    /// None` as Unpartitioned, not as an error — this variant is reserved
    /// for drift between layout and contract).
    ContractMissing(String),
    /// Transform listed in the contract has no first-class derivation rule.
    TransformUnsupported { field: String, transform: String },
    /// Output column referenced by the partition field is not a pure column
    /// expression, OR resolves to a non-group-key column in the layout.
    OutputLineageNotPureColumn { field: String },
    /// Partition field references a target visible column that does not
    /// exist in the contract, or whose backing visible column is missing
    /// from the layout / from the delta chunk schema.
    GroupKeyColumnMissing { field: String, reason: String },
    /// Group-key column in the delta chunk has an Arrow type that the
    /// Iceberg transform function refuses.
    GroupKeyTypeMismatch {
        field: String,
        want: String,
        got: String,
    },
    /// `iceberg::transform::create_transform_function(...).transform(array)`
    /// itself returned an error.
    TransformFailed { field: String, source: String },
}

impl std::fmt::Display for AffectedPartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ContractMissing(reason) => write!(
                f,
                "aggregate target partition contract missing or inconsistent: {reason}"
            ),
            Self::TransformUnsupported { field, transform } => write!(
                f,
                "aggregate target partition field {field} uses unsupported transform {transform}"
            ),
            Self::OutputLineageNotPureColumn { field } => write!(
                f,
                "aggregate target partition field {field} requires row-evaluation fallback"
            ),
            Self::GroupKeyColumnMissing { field, reason } => {
                write!(f, "aggregate target partition field {field}: {reason}")
            }
            Self::GroupKeyTypeMismatch { field, want, got } => write!(
                f,
                "aggregate target partition field {field} delta column type mismatch: want {want}, got {got}"
            ),
            Self::TransformFailed { field, source } => write!(
                f,
                "aggregate target partition field {field} transform failed: {source}"
            ),
        }
    }
}

impl std::error::Error for AffectedPartitionError {}

pub(crate) fn contract_transform_to_iceberg(
    transform: &mv_schema::MvPartitionTransformContract,
    field: &str,
) -> Result<iceberg::spec::Transform, AffectedPartitionError> {
    use mv_schema::MvPartitionTransformContract as C;
    match transform {
        C::Identity => Ok(iceberg::spec::Transform::Identity),
        C::Year => Ok(iceberg::spec::Transform::Year),
        C::Month => Ok(iceberg::spec::Transform::Month),
        C::Day => Ok(iceberg::spec::Transform::Day),
        C::Hour => Ok(iceberg::spec::Transform::Hour),
        C::Bucket { num_buckets } => Ok(iceberg::spec::Transform::Bucket(*num_buckets)),
        C::Truncate { width } => Ok(iceberg::spec::Transform::Truncate(*width)),
        C::Void => Err(AffectedPartitionError::TransformUnsupported {
            field: field.to_string(),
            transform: "void".to_string(),
        }),
    }
}

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
///
/// Invariant: `PartitionDerivationField::output_index` is a position in
/// `contract.target.visible_columns` (see `resolve_partition_derivation_spec`),
/// and this function reuses it as a position in `layout.visible_columns`. That
/// is sound only because both lists are built from the same output-column
/// projection in the same order. If those orderings ever diverge, the
/// group-key membership check would pass but the binding would silently pick
/// the wrong physical column — keep the two projections aligned.
pub(crate) fn bind_spec_to_aggregate_layout(
    spec: &PartitionDerivationSpec,
    layout: &AggregateMvLayout,
) -> Result<Vec<BoundPartitionField>, AffectedPartitionError> {
    let mut bound = Vec::with_capacity(spec.fields.len());
    for field in &spec.fields {
        if !layout
            .group_key_source_indexes
            .contains(&field.output_index)
        {
            return Err(AffectedPartitionError::OutputLineageNotPureColumn {
                field: field.partition_field_name.clone(),
            });
        }
        let column = layout
            .visible_columns
            .get(field.output_index)
            .ok_or_else(|| AffectedPartitionError::GroupKeyColumnMissing {
                field: field.partition_field_name.clone(),
                reason: format!(
                    "layout has no visible column for output index {}",
                    field.output_index
                ),
            })?;
        bound.push(BoundPartitionField {
            partition_field_name: field.partition_field_name.clone(),
            column_name: column.name.clone(),
            transform: field.transform,
        });
    }
    Ok(bound)
}

/// Bind a resolved spec to target-visible output column names. This is used by
/// merge-sink consumers, where the signed change batch already has target
/// output columns rather than an aggregate layout.
pub(crate) fn bind_spec_to_target_visible_columns(
    spec: &PartitionDerivationSpec,
    contract: &MvSchemaContract,
) -> Result<Vec<BoundPartitionField>, AffectedPartitionError> {
    let mut bound = Vec::with_capacity(spec.fields.len());
    for field in &spec.fields {
        let column = contract
            .target
            .visible_columns
            .get(field.output_index)
            .ok_or_else(|| AffectedPartitionError::GroupKeyColumnMissing {
                field: field.partition_field_name.clone(),
                reason: format!(
                    "target contract has no visible column for output index {}",
                    field.output_index
                ),
            })?;
        bound.push(BoundPartitionField {
            partition_field_name: field.partition_field_name.clone(),
            column_name: column.output_name.clone(),
            transform: field.transform,
        });
    }
    Ok(bound)
}

/// Mechanically evaluate bound partition fields over delta chunks: apply each
/// field's Iceberg transform to its source column, then build one
/// `MvPartitionKey` per row, deduplicated into a sorted set. This is the
/// transformation + partitioning phase of the contract-driven derivation
/// pipeline (resolve -> bind -> evaluate).
pub(crate) fn evaluate_partition_spec(
    target_spec_id: i32,
    bound_fields: &[BoundPartitionField],
    delta_chunks: &[Chunk],
) -> Result<BTreeSet<MvPartitionKey>, AffectedPartitionError> {
    let mut partitions: BTreeSet<MvPartitionKey> = BTreeSet::new();

    for chunk in delta_chunks {
        evaluate_partition_record_batch_into(
            target_spec_id,
            bound_fields,
            &chunk.batch,
            &mut partitions,
        )?;
    }

    Ok(partitions)
}

pub(crate) fn evaluate_partition_spec_record_batch(
    target_spec_id: i32,
    bound_fields: &[BoundPartitionField],
    batch: &arrow::record_batch::RecordBatch,
) -> Result<BTreeSet<MvPartitionKey>, AffectedPartitionError> {
    let mut partitions: BTreeSet<MvPartitionKey> = BTreeSet::new();
    evaluate_partition_record_batch_into(target_spec_id, bound_fields, batch, &mut partitions)?;
    Ok(partitions)
}

fn evaluate_partition_record_batch_into(
    target_spec_id: i32,
    bound_fields: &[BoundPartitionField],
    batch: &arrow::record_batch::RecordBatch,
    partitions: &mut BTreeSet<MvPartitionKey>,
) -> Result<(), AffectedPartitionError> {
    if batch.num_rows() == 0 {
        return Ok(());
    }

    let mut transformed: Vec<arrow::array::ArrayRef> = Vec::with_capacity(bound_fields.len());
    for field in bound_fields {
        let col_index = batch.schema().index_of(&field.column_name).map_err(|e| {
            AffectedPartitionError::GroupKeyColumnMissing {
                field: field.partition_field_name.clone(),
                reason: format!("delta chunk is missing column `{}`: {e}", field.column_name),
            }
        })?;
        let array = batch.column(col_index).clone();
        let xform =
            iceberg::transform::create_transform_function(&field.transform).map_err(|e| {
                AffectedPartitionError::TransformFailed {
                    field: field.partition_field_name.clone(),
                    source: e.to_string(),
                }
            })?;
        let out = xform
            .transform(array)
            .map_err(|e| AffectedPartitionError::TransformFailed {
                field: field.partition_field_name.clone(),
                source: e.to_string(),
            })?;
        transformed.push(out);
    }

    for row in 0..batch.num_rows() {
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
    Ok(())
}

fn arrow_array_row_to_partition_value(
    array: &dyn arrow::array::Array,
    row: usize,
    field: &str,
) -> Result<crate::mv::model::MvPartitionValue, AffectedPartitionError> {
    use crate::mv::model::MvPartitionValue;
    use arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};

    if array.is_null(row) {
        return Ok(MvPartitionValue::Null);
    }

    let primitive = match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Boolean downcast");
            arr.value(row).to_string()
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 downcast");
            arr.value(row).to_string()
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 downcast");
            arr.value(row).to_string()
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("Float32 downcast");
            arr.value(row).to_string()
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64 downcast");
            arr.value(row).to_string()
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 downcast");
            arr.value(row).to_string()
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Date32 downcast");
            arr.value(row).to_string()
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("TimestampSecond downcast");
            arr.value(row).to_string()
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("TimestampMillisecond downcast");
            arr.value(row).to_string()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("TimestampMicrosecond downcast");
            arr.value(row).to_string()
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("TimestampNanosecond downcast");
            arr.value(row).to_string()
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("Decimal128 downcast");
            // Stringify as the raw integer representation so manifest's
            // PrimitiveLiteral::Decimal-equivalent rendering aligns. (Iceberg
            // partition-side stringification uses raw integer too; if/when
            // change_partition_value gains a Decimal arm, that helper and
            // this branch must stay in sync.)
            arr.value(row).to_string()
        }
        other => {
            return Err(AffectedPartitionError::GroupKeyTypeMismatch {
                field: field.to_string(),
                want: "Iceberg-compatible primitive Arrow type".to_string(),
                got: format!("{other:?}"),
            });
        }
    };

    Ok(MvPartitionValue::String(primitive))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::model::{
        AffectedTargetPartitions, MvPartitionKey, MvPartitionKeyField, MvPartitionValue,
    };

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
    fn to_target_partition_filter_maps_known_to_allow_list() {
        let result = AffectedTargetPartitions::known([key("a"), key("b")]);
        let filter = result.to_target_partition_filter();
        let crate::mv::model::TargetPartitionFilter::AllowList(set) = filter else {
            panic!("expected AllowList");
        };
        assert_eq!(set.len(), 2);
        assert!(set.contains(&key("a")));
        assert!(set.contains(&key("b")));
    }

    #[test]
    fn to_target_partition_filter_maps_empty_known_to_empty_allow_list() {
        let filter = AffectedTargetPartitions::known(std::iter::empty::<MvPartitionKey>())
            .to_target_partition_filter();
        let crate::mv::model::TargetPartitionFilter::AllowList(set) = filter else {
            panic!("expected AllowList for empty Known set");
        };
        assert!(set.is_empty());
    }

    #[test]
    fn to_target_partition_filter_maps_unpartitioned_and_not_derived_to_none() {
        assert_eq!(
            AffectedTargetPartitions::Unpartitioned.to_target_partition_filter(),
            crate::mv::model::TargetPartitionFilter::None
        );
        assert_eq!(
            AffectedTargetPartitions::not_derived("x").to_target_partition_filter(),
            crate::mv::model::TargetPartitionFilter::None
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

    // --- Moved from aggregate_delta.rs: AffectedPartitionError display test ---

    use mv_schema::MvPartitionTransformContract;

    #[test]
    fn affected_partition_error_display_includes_field_and_reason() {
        let err = AffectedPartitionError::TransformUnsupported {
            field: "region".to_string(),
            transform: "void".to_string(),
        };
        let message = format!("{err}");
        assert!(message.contains("region"), "{message}");
        assert!(message.contains("void"), "{message}");
    }

    // --- Moved from aggregate_delta.rs: contract_transform_to_iceberg tests ---

    #[test]
    fn contract_transform_to_iceberg_handles_all_first_class_transforms() {
        for (input, expect) in [
            (
                MvPartitionTransformContract::Identity,
                iceberg::spec::Transform::Identity,
            ),
            (
                MvPartitionTransformContract::Year,
                iceberg::spec::Transform::Year,
            ),
            (
                MvPartitionTransformContract::Month,
                iceberg::spec::Transform::Month,
            ),
            (
                MvPartitionTransformContract::Day,
                iceberg::spec::Transform::Day,
            ),
            (
                MvPartitionTransformContract::Hour,
                iceberg::spec::Transform::Hour,
            ),
            (
                MvPartitionTransformContract::Bucket { num_buckets: 8 },
                iceberg::spec::Transform::Bucket(8),
            ),
            (
                MvPartitionTransformContract::Truncate { width: 16 },
                iceberg::spec::Transform::Truncate(16),
            ),
        ] {
            let result =
                contract_transform_to_iceberg(&input, "test_field").expect("transform conversion");
            assert_eq!(result, expect, "input={input:?}");
        }
    }

    #[test]
    fn contract_transform_to_iceberg_rejects_void() {
        let err = contract_transform_to_iceberg(&MvPartitionTransformContract::Void, "test_field")
            .unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::TransformUnsupported { ref field, ref transform }
                if field == "test_field" && transform == "void"
        ));
    }

    // --- Test fixture: copied verbatim from aggregate_delta.rs:720-799 ---

    use mv_schema::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionLineage,
        HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract, MvSchemaContract,
        OutputColumnLineage, OutputContract, TargetContract, TargetVisibleColumn,
    };

    fn count_contract_with_partition(
        partition_field_name: &str,
        transform: MvPartitionTransformContract,
        source_target_field_id: i32,
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
            branch: None,
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

    // --- New tests for resolve_partition_derivation_spec ---

    #[test]
    fn resolve_returns_none_for_unpartitioned_contract() {
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition = None;
        assert!(
            resolve_partition_derivation_spec(&contract)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn resolve_returns_none_for_empty_partition_fields() {
        // Mirrors is_unpartitioned_mv_contract: empty fields == unpartitioned.
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition.as_mut().unwrap().fields.clear();
        assert!(
            resolve_partition_derivation_spec(&contract)
                .unwrap()
                .is_none()
        );
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

    // --- Test fixtures for bind/evaluate tests (copied verbatim from aggregate_delta.rs) ---

    use crate::exec::chunk::Chunk;
    use crate::mv::aggregate_state::mv_agg_state::{
        AggregateMvLayout, AggregateStateColumn, AggregateVisibleColumn,
    };
    use crate::mv::aggregate_state::physical_column::starrocks_physical_column;
    use crate::mv::model::{AggregateFunctionKind, AggregateStateRole};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_catalog::schema::SqlType;
    use std::sync::Arc as StdArcFixture;

    fn count_layout_with_group_key(
        name: &str,
        data_type: DataType,
        sql_type: SqlType,
    ) -> AggregateMvLayout {
        let row_id = starrocks_physical_column(
            "__row_id__".to_string(),
            SqlType::String,
            false,
            false,
            true,
        );
        let group =
            starrocks_physical_column(name.to_string(), sql_type.clone(), true, true, false);
        let counter =
            starrocks_physical_column("c".to_string(), SqlType::BigInt, false, true, false);
        let state = starrocks_physical_column(
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
                    name: name.to_string(),
                    data_type,
                    sql_type,
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
            aggregate_input_types: vec![None],
            group_key_source_indexes: vec![0],
            physical_columns: vec![row_id, group, counter, state],
        }
    }

    fn batch_with_group_key(name: &str, dt: DataType, values: arrow::array::ArrayRef) -> Chunk {
        use arrow::array::{Int64Array, StringArray};
        let n = values.len();
        let row_ids: Vec<String> = (0..n).map(|i| format!("rid-{i}")).collect();
        let row_id_arr: arrow::array::ArrayRef = StdArcFixture::new(StringArray::from(row_ids));
        let counts: arrow::array::ArrayRef = StdArcFixture::new(Int64Array::from(vec![1i64; n]));
        let states: arrow::array::ArrayRef = StdArcFixture::new(Int64Array::from(vec![1i64; n]));
        let schema = StdArcFixture::new(Schema::new(vec![
            Field::new("__row_id__", DataType::Utf8, false),
            Field::new(name, dt, true),
            Field::new("c", DataType::Int64, false),
            Field::new("__agg_state_c", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![row_id_arr, values, counts, states]).unwrap();
        record_batch_to_chunk(batch).unwrap()
    }

    // --- Moved from aggregate_delta.rs: arrow_array_row_to_partition_value tests ---

    use arrow::array::{
        BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };

    #[test]
    fn arrow_row_to_partition_value_supports_iceberg_primitive_arrow_types() {
        use arrow::array::{ArrayRef, StringArray};
        let bool_arr = StdArcFixture::new(BooleanArray::from(vec![Some(true)])) as ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(bool_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("true".to_string())
        );
        let int_arr = StdArcFixture::new(Int32Array::from(vec![Some(7)])) as ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(int_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("7".to_string())
        );
        let long_arr = StdArcFixture::new(Int64Array::from(vec![Some(20000)])) as ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(long_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("20000".to_string())
        );
        let float_arr = StdArcFixture::new(Float32Array::from(vec![Some(1.5f32)])) as ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(float_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("1.5".to_string())
        );
        let double_arr = StdArcFixture::new(Float64Array::from(vec![Some(2.5f64)])) as ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(double_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("2.5".to_string())
        );
        let str_arr = StdArcFixture::new(StringArray::from(vec![Some("east")])) as ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(str_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("east".to_string())
        );
        // Date32: number of days since 1970-01-01.
        let date_arr = StdArcFixture::new(Date32Array::from(vec![Some(19500)])) as ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(date_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("19500".to_string())
        );
        // TimestampMicrosecond: integer micros since epoch.
        let ts_arr = StdArcFixture::new(TimestampMicrosecondArray::from(vec![Some(
            1_700_000_000_000_000,
        )])) as ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(ts_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("1700000000000000".to_string())
        );
    }

    #[test]
    fn arrow_row_to_partition_value_handles_null() {
        let arr = StdArcFixture::new(Int32Array::from(vec![None::<i32>])) as arrow::array::ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::Null
        );
    }

    #[test]
    fn arrow_row_to_partition_value_rejects_unsupported_arrow_type() {
        // Use a UInt32Array — not an Iceberg-native partition output type.
        let arr = StdArcFixture::new(arrow::array::UInt32Array::from(vec![Some(1u32)]))
            as arrow::array::ArrayRef;
        let err = arrow_array_row_to_partition_value(arr.as_ref(), 0, "f").unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::GroupKeyTypeMismatch { ref field, .. } if field == "f"
        ));
    }

    #[test]
    fn client_side_serialization_matches_file_metadata_path_for_primitive_literals() {
        // Property-style equality: for every primitive value Iceberg can carry
        // in a partition struct, the file-metadata path's stringification and
        // the client-side path's stringification must agree, so MvPartitionKey
        // values from base manifests and from delta chunks compare equal.
        use arrow::array::ArrayRef;
        use iceberg::spec::PrimitiveLiteral;

        // (manifest literal, builder of a 1-row Arrow array carrying the same value)
        let cases: Vec<(PrimitiveLiteral, ArrayRef)> = vec![
            (
                PrimitiveLiteral::Boolean(true),
                StdArcFixture::new(BooleanArray::from(vec![Some(true)])) as ArrayRef,
            ),
            (
                PrimitiveLiteral::Int(42),
                StdArcFixture::new(Int32Array::from(vec![Some(42)])) as ArrayRef,
            ),
            (
                PrimitiveLiteral::Long(100),
                StdArcFixture::new(Int64Array::from(vec![Some(100)])) as ArrayRef,
            ),
            (
                PrimitiveLiteral::Float(ordered_float::OrderedFloat(0.5)),
                StdArcFixture::new(Float32Array::from(vec![Some(0.5f32)])) as ArrayRef,
            ),
            (
                PrimitiveLiteral::Double(ordered_float::OrderedFloat(0.25)),
                StdArcFixture::new(Float64Array::from(vec![Some(0.25f64)])) as ArrayRef,
            ),
            (
                PrimitiveLiteral::String("east".to_string()),
                StdArcFixture::new(arrow::array::StringArray::from(vec![Some("east")])) as ArrayRef,
            ),
        ];
        for (lit, arr) in cases {
            let manifest_value = manifest_primitive_to_string(&lit);
            let client_value = arrow_array_row_to_partition_value(arr.as_ref(), 0, "f").unwrap();
            assert_eq!(
                MvPartitionValue::String(manifest_value),
                client_value,
                "literal={lit:?}"
            );
        }
    }

    fn manifest_primitive_to_string(lit: &iceberg::spec::PrimitiveLiteral) -> String {
        // Helper that mirrors `change_partition_value` from changes.rs for the
        // primitive subset this property test exercises. If `change_partition_value`
        // ever changes its stringification rule, this helper must be updated and
        // the property test will catch the divergence in `arrow_array_row_to_partition_value`.
        match lit {
            iceberg::spec::PrimitiveLiteral::Boolean(v) => v.to_string(),
            iceberg::spec::PrimitiveLiteral::Int(v) => v.to_string(),
            iceberg::spec::PrimitiveLiteral::Long(v) => v.to_string(),
            iceberg::spec::PrimitiveLiteral::Float(v) => v.0.to_string(),
            iceberg::spec::PrimitiveLiteral::Double(v) => v.0.to_string(),
            iceberg::spec::PrimitiveLiteral::String(v) => v.clone(),
            _ => unreachable!("only the primitives this test exercises are listed above"),
        }
    }

    // --- Tests for bind_spec_to_aggregate_layout and evaluate_partition_spec ---

    #[test]
    fn bind_and_evaluate_identity_partition_over_chunks() {
        use arrow::array::StringArray;
        use arrow::datatypes::DataType;
        use novarocks_catalog::schema::SqlType;

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
            StdArcFixture::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")]))
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
        use novarocks_catalog::schema::SqlType;

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
            Arc::new(Schema::new(vec![Field::new(
                "region",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["west", "east", "west"]))],
        )
        .expect("batch");

        let partitions = evaluate_partition_spec_record_batch(spec.target_spec_id, &bound, &batch)
            .expect("evaluate");

        assert_eq!(
            partitions.into_iter().collect::<Vec<_>>(),
            vec![key("east"), key("west")]
        );
    }

    // --- End-to-end derivation behavior-lock tests -------------------------
    //
    // Ported from the removed pre-cutover aggregate-delta derivation tests.
    // `derive_for_test` composes the three public stages
    // (resolve -> bind -> evaluate) exactly as the old single-shot deriver
    // did, so these lock the same observable behavior over the retained
    // derivation library. `None` mirrors the old `Unpartitioned` result;
    // `Some(partitions)` mirrors the old `Known { partitions }` result.
    fn derive_for_test(
        contract: &MvSchemaContract,
        layout: &AggregateMvLayout,
        chunks: &[Chunk],
    ) -> Result<Option<BTreeSet<MvPartitionKey>>, AffectedPartitionError> {
        let Some(spec) = resolve_partition_derivation_spec(contract)? else {
            return Ok(None);
        };
        let bound = bind_spec_to_aggregate_layout(&spec, layout)?;
        let partitions = evaluate_partition_spec(spec.target_spec_id, &bound, chunks)?;
        Ok(Some(partitions))
    }

    #[test]
    fn derive_identity_returns_known_partition_per_unique_value() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArcFixture::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")]))
                as arrow::array::ArrayRef,
        );

        let partitions = derive_for_test(&contract, &layout, &[chunk])
            .expect("derive")
            .expect("partitioned");
        let names: Vec<_> = partitions
            .iter()
            .map(|key| match &key.fields[0].value {
                MvPartitionValue::String(s) => s.clone(),
                MvPartitionValue::Null => "<NULL>".to_string(),
            })
            .collect();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
        for key in &partitions {
            assert_eq!(key.spec_id, 7);
            assert_eq!(key.fields[0].field_name, "region");
        }
    }

    #[test]
    fn derive_day_transform_normalizes_dates_to_day_buckets() {
        let layout = count_layout_with_group_key("ts", DataType::Date32, SqlType::Date);
        let contract = count_contract_with_partition("ts", MvPartitionTransformContract::Day, 11);
        // Two distinct days: 19500 and 19501.
        let chunk = batch_with_group_key(
            "ts",
            DataType::Date32,
            StdArcFixture::new(Date32Array::from(vec![
                Some(19500),
                Some(19501),
                Some(19500),
            ])) as arrow::array::ArrayRef,
        );

        let partitions = derive_for_test(&contract, &layout, &[chunk])
            .expect("derive")
            .expect("partitioned");
        let values: Vec<_> = partitions
            .iter()
            .map(|key| match &key.fields[0].value {
                MvPartitionValue::String(s) => s.clone(),
                MvPartitionValue::Null => "<NULL>".to_string(),
            })
            .collect();
        // Day transform on a Date32 input should yield the integer day-since-epoch
        // for each distinct row. After dedup and sort: "19500", "19501".
        assert_eq!(values, vec!["19500".to_string(), "19501".to_string()]);
    }

    #[test]
    fn derive_bucket_transform_uses_iceberg_hash() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract = count_contract_with_partition(
            "region",
            MvPartitionTransformContract::Bucket { num_buckets: 8 },
            11,
        );
        // Build the chunk and run derivation.
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArcFixture::new(StringArray::from(vec![Some("east"), Some("west")]))
                as arrow::array::ArrayRef,
        );

        // Independently compute the expected bucket values via iceberg-rust
        // and assert the derivation produced exactly those.
        let arr: arrow::array::ArrayRef =
            StdArcFixture::new(StringArray::from(vec![Some("east"), Some("west")]));
        let xform =
            iceberg::transform::create_transform_function(&iceberg::spec::Transform::Bucket(8))
                .expect("transform");
        let out = xform.transform(arr).expect("apply");
        let expected: Vec<String> = (0..out.len())
            .map(|i| {
                let arr = out.as_any().downcast_ref::<Int32Array>().expect("int32");
                arr.value(i).to_string()
            })
            .collect();

        let partitions = derive_for_test(&contract, &layout, &[chunk])
            .expect("derive")
            .expect("partitioned");
        let got: std::collections::BTreeSet<String> = partitions
            .iter()
            .map(|key| match &key.fields[0].value {
                MvPartitionValue::String(s) => s.clone(),
                MvPartitionValue::Null => "<NULL>".to_string(),
            })
            .collect();
        let want: std::collections::BTreeSet<String> = expected.into_iter().collect();
        assert_eq!(got, want);
    }

    #[test]
    fn derive_unpartitioned_contract_returns_unpartitioned() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition = None;
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArcFixture::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );

        // `None` is the ported equivalent of the old `Unpartitioned` result.
        assert!(
            derive_for_test(&contract, &layout, &[chunk])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn derive_void_transform_returns_unsupported_error() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Void, 11);
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArcFixture::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );

        let err = derive_for_test(&contract, &layout, &[chunk]).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::TransformUnsupported { ref field, ref transform }
                if field == "region" && transform == "void"
        ));
    }

    #[test]
    fn derive_missing_target_field_returns_group_key_missing() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition.as_mut().unwrap().fields[0].source_target_field_id = 999;
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArcFixture::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );

        let err = derive_for_test(&contract, &layout, &[chunk]).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::GroupKeyColumnMissing { ref field, .. } if field == "region"
        ));
    }

    #[test]
    fn derive_non_pure_output_lineage_returns_error() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        // Force the output column to look like a non-pure expression.
        contract.output.columns[0].expression.kind = ExpressionKind::Func;
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids = vec![1, 2];

        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArcFixture::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );
        let err = derive_for_test(&contract, &layout, &[chunk]).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::OutputLineageNotPureColumn { ref field } if field == "region"
        ));
    }

    #[test]
    fn derive_missing_chunk_column_returns_group_key_missing() {
        use arrow::array::ArrayRef;
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        // Build a chunk whose group-key column name does NOT match the layout's.
        let row_ids: ArrayRef = StdArcFixture::new(StringArray::from(vec![Some("rid-0")]));
        let other: ArrayRef = StdArcFixture::new(StringArray::from(vec![Some("a")]));
        let counts: ArrayRef = StdArcFixture::new(Int64Array::from(vec![1i64]));
        let states: ArrayRef = StdArcFixture::new(Int64Array::from(vec![1i64]));
        let schema = StdArcFixture::new(Schema::new(vec![
            Field::new("__row_id__", DataType::Utf8, false),
            Field::new("not_region", DataType::Utf8, true),
            Field::new("c", DataType::Int64, false),
            Field::new("__agg_state_c", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![row_ids, other, counts, states]).unwrap();
        let chunk = record_batch_to_chunk(batch).unwrap();

        let err = derive_for_test(&contract, &layout, &[chunk]).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::GroupKeyColumnMissing { ref field, .. } if field == "region"
        ));
    }

    #[test]
    fn derive_empty_chunks_returns_known_empty_set() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArcFixture::new(StringArray::from(Vec::<Option<&str>>::new()))
                as arrow::array::ArrayRef,
        );

        // Empty chunks must still resolve to `Known` (Some) with an empty set,
        // not Unpartitioned (None).
        let partitions = derive_for_test(&contract, &layout, &[chunk])
            .expect("derive")
            .expect("partitioned");
        assert!(partitions.is_empty());
    }

    #[test]
    fn derive_accepts_join_aggregate_pure_column_lineage() {
        use mv_schema::QualifiedFieldLineage;
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        // Swap the lineage from single-base form to join form: clear
        // referenced_base_field_ids and populate referenced_base_fields
        // with a single qualified ref. This simulates a join-aggregate MV
        // where the output column is backed by a qualified field reference
        // instead of a direct base field id.
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids = Vec::new();
        contract.output.columns[0].expression.referenced_base_fields =
            vec![QualifiedFieldLineage {
                table_fqn: "ice.sales.orders".to_string(),
                qualifier_at_create: "base".to_string(),
                field_id: 1,
            }];

        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArcFixture::new(StringArray::from(vec![Some("a"), Some("b")]))
                as arrow::array::ArrayRef,
        );
        let partitions = derive_for_test(&contract, &layout, &[chunk])
            .expect("derive")
            .expect("partitioned");
        assert_eq!(partitions.len(), 2);
    }

    #[test]
    fn derive_rejects_join_aggregate_multi_base_field_lineage() {
        use mv_schema::QualifiedFieldLineage;
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        // Two base-field refs simulates a computed/joined expression, which
        // is NOT a pure passthrough and should be rejected. This represents
        // a scenario where the output column depends on multiple base fields
        // (e.g., a computed column in a join context).
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids = Vec::new();
        contract.output.columns[0].expression.referenced_base_fields = vec![
            QualifiedFieldLineage {
                table_fqn: "ice.sales.orders".to_string(),
                qualifier_at_create: "f".to_string(),
                field_id: 1,
            },
            QualifiedFieldLineage {
                table_fqn: "ice.sales.orders".to_string(),
                qualifier_at_create: "d".to_string(),
                field_id: 2,
            },
        ];

        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArcFixture::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );
        let err = derive_for_test(&contract, &layout, &[chunk]).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::OutputLineageNotPureColumn { ref field } if field == "region"
        ));
    }
}
