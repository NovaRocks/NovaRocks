use std::collections::BTreeSet;

use crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout;
use crate::engine::mv::partition::MvPartitionKey;
use crate::exec::chunk::Chunk;
use crate::meta::repository::mv_contract::{ExpressionKind, MvSchemaContract};

/// Set of MV target partitions affected by a signed aggregate delta batch.
///
/// `Unpartitioned` is the legitimate state for non-partitioned MV targets;
/// callers MUST NOT treat it as "no information available". A failed
/// derivation surfaces an [`AffectedPartitionError`] instead — the design
/// is strict fail-fast, no silent fallback (see spec §5 principle 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedAggregateTargetPartitions {
    Unpartitioned,
    Known { partitions: BTreeSet<MvPartitionKey> },
}

impl AffectedAggregateTargetPartitions {
    pub(crate) fn known<I: IntoIterator<Item = MvPartitionKey>>(partitions: I) -> Self {
        Self::Known {
            partitions: partitions.into_iter().collect(),
        }
    }

    pub(crate) fn partitions(&self) -> Option<&BTreeSet<MvPartitionKey>> {
        match self {
            Self::Unpartitioned => None,
            Self::Known { partitions } => Some(partitions),
        }
    }
}

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

/// Inputs required to derive the affected target partitions from a signed
/// aggregate delta batch.
pub(crate) struct AggregateDeltaPartitionInput<'a> {
    pub(crate) layout: &'a AggregateMvLayout,
    pub(crate) schema_contract: &'a MvSchemaContract,
    pub(crate) delta_chunks: &'a [Chunk],
}

/// Derive the set of target partitions touched by a signed aggregate delta
/// batch given the MV schema contract and the aggregate layout.
///
/// Returns `Unpartitioned` when the contract has no `target.partition`.
/// Returns `Known { partitions }` with the deduplicated, sorted set of all
/// partition keys touched by the delta rows.
pub(crate) fn derive_from_aggregate_delta(
    input: &AggregateDeltaPartitionInput<'_>,
) -> Result<AffectedAggregateTargetPartitions, AffectedPartitionError> {
    use crate::engine::mv::partition::{MvPartitionKey, MvPartitionKeyField};

    let Some(partition) = input.schema_contract.target.partition.as_ref() else {
        return Ok(AffectedAggregateTargetPartitions::Unpartitioned);
    };

    // Resolve each partition field to (column_name, iceberg_transform) once,
    // before touching any chunk data.
    struct ResolvedField {
        partition_field_name: String,
        column_name: String,
        transform: iceberg::spec::Transform,
    }

    let mut resolved: Vec<ResolvedField> = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        // Step 1: find output_index for this partition field's target field id.
        let output_index = input
            .schema_contract
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

        // Step 2: verify output column lineage is a pure column expression.
        let lineage = input
            .schema_contract
            .output
            .columns
            .get(output_index)
            .ok_or_else(|| AffectedPartitionError::OutputLineageNotPureColumn {
                field: partition_field.partition_field_name.clone(),
            })?;
        if lineage.expression.kind != ExpressionKind::Column
            || lineage.expression.referenced_base_field_ids.len() != 1
        {
            return Err(AffectedPartitionError::OutputLineageNotPureColumn {
                field: partition_field.partition_field_name.clone(),
            });
        }

        // Step 3: verify output_index is in the layout's group key (defense in depth).
        if !input.layout.group_key_source_indexes.contains(&output_index) {
            return Err(AffectedPartitionError::OutputLineageNotPureColumn {
                field: partition_field.partition_field_name.clone(),
            });
        }

        // Step 4: resolve the Arrow column name from the layout.
        let column = input
            .layout
            .visible_columns
            .get(output_index)
            .ok_or_else(|| AffectedPartitionError::GroupKeyColumnMissing {
                field: partition_field.partition_field_name.clone(),
                reason: format!(
                    "layout has no visible column for output index {output_index}"
                ),
            })?;

        let transform = contract_transform_to_iceberg(
            &partition_field.transform,
            &partition_field.partition_field_name,
        )?;

        resolved.push(ResolvedField {
            partition_field_name: partition_field.partition_field_name.clone(),
            column_name: column.name.clone(),
            transform,
        });
    }

    let mut partitions: BTreeSet<MvPartitionKey> = BTreeSet::new();

    for chunk in input.delta_chunks {
        if chunk.batch.num_rows() == 0 {
            continue;
        }

        // Apply each partition field's transform to its source column once per chunk.
        let mut transformed: Vec<arrow::array::ArrayRef> = Vec::with_capacity(resolved.len());
        for field in &resolved {
            let col_index = chunk
                .batch
                .schema()
                .index_of(&field.column_name)
                .map_err(|e| AffectedPartitionError::GroupKeyColumnMissing {
                    field: field.partition_field_name.clone(),
                    reason: format!(
                        "delta chunk is missing column `{}`: {e}",
                        field.column_name
                    ),
                })?;
            let array = chunk.batch.column(col_index).clone();
            let xform = iceberg::transform::create_transform_function(&field.transform)
                .map_err(|e| AffectedPartitionError::TransformFailed {
                    field: field.partition_field_name.clone(),
                    source: e.to_string(),
                })?;
            let out = xform.transform(array).map_err(|e| AffectedPartitionError::TransformFailed {
                field: field.partition_field_name.clone(),
                source: e.to_string(),
            })?;
            transformed.push(out);
        }

        let row_count = chunk.batch.num_rows();
        for row in 0..row_count {
            let mut fields = Vec::with_capacity(resolved.len());
            for (resolved_field, array) in resolved.iter().zip(transformed.iter()) {
                let value = arrow_array_row_to_partition_value(
                    array.as_ref(),
                    row,
                    &resolved_field.partition_field_name,
                )?;
                fields.push(MvPartitionKeyField::new(
                    resolved_field.partition_field_name.clone(),
                    value,
                ));
            }
            partitions.insert(MvPartitionKey::new(partition.target_spec_id, fields));
        }
    }

    Ok(AffectedAggregateTargetPartitions::Known { partitions })
}

fn arrow_array_row_to_partition_value(
    array: &dyn arrow::array::Array,
    row: usize,
    field: &str,
) -> Result<crate::engine::mv::partition::MvPartitionValue, AffectedPartitionError> {
    use arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use crate::engine::mv::partition::MvPartitionValue;

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

fn contract_transform_to_iceberg(
    transform: &crate::meta::repository::mv_contract::MvPartitionTransformContract,
    field: &str,
) -> Result<iceberg::spec::Transform, AffectedPartitionError> {
    use crate::meta::repository::mv_contract::MvPartitionTransformContract as C;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mv::partition::{
        MvPartitionKey, MvPartitionKeyField, MvPartitionValue,
    };

    fn sample_key(value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn affected_aggregate_target_partitions_known_dedupes_and_sorts() {
        let result = AffectedAggregateTargetPartitions::known(
            [sample_key("b"), sample_key("a"), sample_key("a")],
        );
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        assert_eq!(
            partitions.into_iter().collect::<Vec<_>>(),
            vec![sample_key("a"), sample_key("b")]
        );
    }

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

    #[test]
    fn affected_aggregate_target_partitions_unpartitioned_has_no_partitions() {
        let result = AffectedAggregateTargetPartitions::Unpartitioned;
        assert!(result.partitions().is_none());
    }

    use crate::meta::repository::mv_contract::MvPartitionTransformContract;

    #[test]
    fn contract_transform_to_iceberg_handles_all_first_class_transforms() {
        for (input, expect) in [
            (MvPartitionTransformContract::Identity, iceberg::spec::Transform::Identity),
            (MvPartitionTransformContract::Year, iceberg::spec::Transform::Year),
            (MvPartitionTransformContract::Month, iceberg::spec::Transform::Month),
            (MvPartitionTransformContract::Day, iceberg::spec::Transform::Day),
            (MvPartitionTransformContract::Hour, iceberg::spec::Transform::Hour),
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
        let err = contract_transform_to_iceberg(
            &MvPartitionTransformContract::Void,
            "test_field",
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::TransformUnsupported { ref field, ref transform }
                if field == "test_field" && transform == "void"
        ));
    }

    use arrow::array::{
        BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
        StringArray, TimestampMicrosecondArray,
    };
    use std::sync::Arc as StdArc;

    #[test]
    fn arrow_row_to_partition_value_supports_iceberg_primitive_arrow_types() {
        let bool_arr = StdArc::new(BooleanArray::from(vec![Some(true)])) as arrow::array::ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(bool_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("true".to_string())
        );
        let int_arr = StdArc::new(Int32Array::from(vec![Some(7)])) as arrow::array::ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(int_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("7".to_string())
        );
        let long_arr = StdArc::new(Int64Array::from(vec![Some(20000)])) as arrow::array::ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(long_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("20000".to_string())
        );
        let float_arr = StdArc::new(Float32Array::from(vec![Some(1.5f32)])) as arrow::array::ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(float_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("1.5".to_string())
        );
        let double_arr = StdArc::new(Float64Array::from(vec![Some(2.5f64)])) as arrow::array::ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(double_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("2.5".to_string())
        );
        let str_arr = StdArc::new(StringArray::from(vec![Some("east")])) as arrow::array::ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(str_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("east".to_string())
        );
        // Date32: number of days since 1970-01-01.
        let date_arr = StdArc::new(Date32Array::from(vec![Some(19500)])) as arrow::array::ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(date_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("19500".to_string())
        );
        // TimestampMicrosecond: integer micros since epoch.
        let ts_arr = StdArc::new(TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_000)]))
            as arrow::array::ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(ts_arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::String("1700000000000000".to_string())
        );
    }

    #[test]
    fn arrow_row_to_partition_value_handles_null() {
        let arr = StdArc::new(Int32Array::from(vec![None::<i32>])) as arrow::array::ArrayRef;
        assert_eq!(
            arrow_array_row_to_partition_value(arr.as_ref(), 0, "f").unwrap(),
            MvPartitionValue::Null
        );
    }

    #[test]
    fn arrow_row_to_partition_value_rejects_unsupported_arrow_type() {
        // Use a UInt32Array — not an Iceberg-native partition output type.
        let arr =
            StdArc::new(arrow::array::UInt32Array::from(vec![Some(1u32)])) as arrow::array::ArrayRef;
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
                StdArc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef,
            ),
            (
                PrimitiveLiteral::Int(42),
                StdArc::new(Int32Array::from(vec![Some(42)])) as ArrayRef,
            ),
            (
                PrimitiveLiteral::Long(100),
                StdArc::new(Int64Array::from(vec![Some(100)])) as ArrayRef,
            ),
            (
                PrimitiveLiteral::Float(ordered_float::OrderedFloat(0.5)),
                StdArc::new(Float32Array::from(vec![Some(0.5f32)])) as ArrayRef,
            ),
            (
                PrimitiveLiteral::Double(ordered_float::OrderedFloat(0.25)),
                StdArc::new(Float64Array::from(vec![Some(0.25f64)])) as ArrayRef,
            ),
            (
                PrimitiveLiteral::String("east".to_string()),
                StdArc::new(StringArray::from(vec![Some("east")])) as ArrayRef,
            ),
        ];
        for (lit, arr) in cases {
            let manifest_value = manifest_primitive_to_string(&lit);
            let client_value =
                arrow_array_row_to_partition_value(arr.as_ref(), 0, "f").unwrap();
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

    // --- Task 4 tests: derive_from_aggregate_delta ---

    use crate::connector::starrocks::managed::ddl::managed_physical_column;
    use crate::connector::starrocks::managed::mv_agg_state::{
        AggregateMvLayout, AggregateStateColumn, AggregateStateRole, AggregateVisibleColumn,
    };
    use crate::connector::starrocks::managed::mv_shape::AggregateFunctionKind;
    use crate::exec::chunk::Chunk;
    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvSchemaContract, OutputColumnLineage, OutputContract, TargetContract, TargetVisibleColumn,
    };
    use crate::sql::parser::ast::SqlType;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn count_layout_with_group_key(
        name: &str,
        data_type: DataType,
        sql_type: SqlType,
    ) -> AggregateMvLayout {
        let row_id =
            managed_physical_column("__row_id__".to_string(), SqlType::String, false, false, true);
        let group =
            managed_physical_column(name.to_string(), sql_type.clone(), true, true, false);
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
            group_key_source_indexes: vec![0],
            physical_columns: vec![row_id, group, counter, state],
        }
    }

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

    fn batch_with_group_key(name: &str, dt: DataType, values: arrow::array::ArrayRef) -> Chunk {
        let n = values.len();
        let row_ids: Vec<String> = (0..n).map(|i| format!("rid-{i}")).collect();
        let row_id_arr: arrow::array::ArrayRef =
            StdArc::new(StringArray::from(row_ids));
        let counts: arrow::array::ArrayRef = StdArc::new(Int64Array::from(vec![1i64; n]));
        let states: arrow::array::ArrayRef = StdArc::new(Int64Array::from(vec![1i64; n]));
        let schema = StdArc::new(Schema::new(vec![
            Field::new("__row_id__", DataType::Utf8, false),
            Field::new(name, dt, true),
            Field::new("c", DataType::Int64, false),
            Field::new("__agg_state_c", DataType::Int64, false),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![row_id_arr, values, counts, states]).unwrap();
        crate::engine::record_batch_to_chunk(batch).unwrap()
    }

    #[test]
    fn derive_identity_returns_known_partition_per_unique_value() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract = count_contract_with_partition(
            "region",
            MvPartitionTransformContract::Identity,
            11,
        );
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")]))
                as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let result = derive_from_aggregate_delta(&input).expect("derive");
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
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
        let contract =
            count_contract_with_partition("ts", MvPartitionTransformContract::Day, 11);
        // Two distinct days: 19500 and 19501.
        let chunk = batch_with_group_key(
            "ts",
            DataType::Date32,
            StdArc::new(Date32Array::from(vec![Some(19500), Some(19501), Some(19500)]))
                as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let result = derive_from_aggregate_delta(&input).expect("derive");
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
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
            StdArc::new(StringArray::from(vec![Some("east"), Some("west")]))
                as arrow::array::ArrayRef,
        );

        // Independently compute the expected bucket values via iceberg-rust
        // and assert the derivation produced exactly those.
        let arr: arrow::array::ArrayRef =
            StdArc::new(StringArray::from(vec![Some("east"), Some("west")]));
        let xform =
            iceberg::transform::create_transform_function(&iceberg::spec::Transform::Bucket(8))
                .expect("transform");
        let out = xform.transform(arr).expect("apply");
        let expected: Vec<String> = (0..out.len())
            .map(|i| {
                let arr = out
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int32");
                arr.value(i).to_string()
            })
            .collect();

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let result = derive_from_aggregate_delta(&input).expect("derive");
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
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
        let mut contract = count_contract_with_partition(
            "region",
            MvPartitionTransformContract::Identity,
            11,
        );
        contract.target.partition = None;
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        assert!(matches!(
            derive_from_aggregate_delta(&input).unwrap(),
            AffectedAggregateTargetPartitions::Unpartitioned
        ));
    }

    #[test]
    fn derive_void_transform_returns_unsupported_error() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract = count_contract_with_partition(
            "region",
            MvPartitionTransformContract::Void,
            11,
        );
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let err = derive_from_aggregate_delta(&input).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::TransformUnsupported { ref field, ref transform }
                if field == "region" && transform == "void"
        ));
    }

    #[test]
    fn derive_missing_target_field_returns_group_key_missing() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract = count_contract_with_partition(
            "region",
            MvPartitionTransformContract::Identity,
            11,
        );
        contract
            .target
            .partition
            .as_mut()
            .unwrap()
            .fields[0]
            .source_target_field_id = 999;
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let err = derive_from_aggregate_delta(&input).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::GroupKeyColumnMissing { ref field, .. } if field == "region"
        ));
    }

    #[test]
    fn derive_non_pure_output_lineage_returns_error() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract = count_contract_with_partition(
            "region",
            MvPartitionTransformContract::Identity,
            11,
        );
        // Force the output column to look like a non-pure expression.
        contract.output.columns[0].expression.kind = ExpressionKind::Func;
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids = vec![1, 2];

        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );
        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let err = derive_from_aggregate_delta(&input).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::OutputLineageNotPureColumn { ref field } if field == "region"
        ));
    }

    #[test]
    fn derive_missing_chunk_column_returns_group_key_missing() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract = count_contract_with_partition(
            "region",
            MvPartitionTransformContract::Identity,
            11,
        );
        // Build a chunk whose group-key column name does NOT match the layout's.
        let row_ids: arrow::array::ArrayRef =
            StdArc::new(StringArray::from(vec![Some("rid-0")]));
        let other: arrow::array::ArrayRef = StdArc::new(StringArray::from(vec![Some("a")]));
        let counts: arrow::array::ArrayRef = StdArc::new(Int64Array::from(vec![1i64]));
        let states: arrow::array::ArrayRef = StdArc::new(Int64Array::from(vec![1i64]));
        let schema = StdArc::new(Schema::new(vec![
            Field::new("__row_id__", DataType::Utf8, false),
            Field::new("not_region", DataType::Utf8, true),
            Field::new("c", DataType::Int64, false),
            Field::new("__agg_state_c", DataType::Int64, false),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![row_ids, other, counts, states]).unwrap();
        let chunk = crate::engine::record_batch_to_chunk(batch).unwrap();

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let err = derive_from_aggregate_delta(&input).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::GroupKeyColumnMissing { ref field, .. } if field == "region"
        ));
    }

    #[test]
    fn derive_empty_chunks_returns_known_empty_set() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract = count_contract_with_partition(
            "region",
            MvPartitionTransformContract::Identity,
            11,
        );
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(Vec::<Option<&str>>::new()))
                as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let result = derive_from_aggregate_delta(&input).expect("derive");
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        assert!(partitions.is_empty());
    }
}
