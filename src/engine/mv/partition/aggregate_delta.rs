use std::collections::BTreeSet;

use crate::engine::mv::partition::MvPartitionKey;

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

/// Inputs required to derive the affected target partitions from an aggregate
/// delta batch. Populated by Tasks 2–3; this stub preserves the public
/// surface referenced by `mod.rs` re-exports until the full implementation
/// lands.
pub(crate) struct AggregateDeltaPartitionInput {
    // Fields added in Task 2.
    _priv: (),
}

/// Derive the set of target partitions touched by a signed aggregate delta
/// batch given the MV schema contract and the aggregate layout.
/// Implementation added in Task 2–4; the stub exists so that `mod.rs` can
/// re-export the symbol without breaking compilation.
pub(crate) fn derive_from_aggregate_delta(
    _input: &AggregateDeltaPartitionInput,
) -> Result<AffectedAggregateTargetPartitions, AffectedPartitionError> {
    unimplemented!("derive_from_aggregate_delta: implementation added in Task 2")
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
}
