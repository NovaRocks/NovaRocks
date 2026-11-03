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
//! Shared input geometry, exact output validation and key operations used by
//! compiled window execution and sort. Window function evaluation belongs to
//! the compiled owners; this module contains no expression/aggregate evaluator.

#[path = "analytic_failure.rs"]
mod analytic_failure;
#[path = "analytic_input_stages.rs"]
mod analytic_input_stages;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::runtime::task::ExecutionResult;
use analytic_failure::contract as analytic_contract;
use arrow::array::{Array, ArrayRef, UInt32Builder};
use arrow::compute::take;
use arrow::datatypes::DataType;
use std::cmp::Ordering;
#[cfg(test)]
use std::collections::VecDeque;

/// Borrowed result of the ONE original analytic output-layout validator.
/// The complete carrier handoff uses the exact output column ordinal; it never
/// parses diagnostics or substitutes a function's source for an input column.
#[derive(Debug)]
pub(crate) enum AnalyticOutputValidationFailure<'a> {
    ColumnCount {
        expected: usize,
        actual: usize,
    },
    Length {
        column: usize,
        expected_rows: usize,
        actual: usize,
    },
    Type {
        column: usize,
        expected: &'a DataType,
        actual: &'a DataType,
    },
}
impl AnalyticOutputValidationFailure<'_> {
    pub(crate) fn output_ordinal(&self) -> Option<usize> {
        match self {
            Self::ColumnCount { .. } => None,
            Self::Length { column, .. } | Self::Type { column, .. } => Some(*column),
        }
    }
}
impl std::fmt::Display for AnalyticOutputValidationFailure<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ColumnCount { expected, actual } => write!(
                f,
                "analytic output column count mismatch: descriptor={expected} actual={actual}"
            ),
            Self::Length {
                column,
                expected_rows,
                actual,
            } => write!(
                f,
                "analytic output length mismatch at column {column}: expected_rows={expected_rows} actual={actual}"
            ),
            Self::Type {
                column,
                expected,
                actual,
            } => write!(
                f,
                "analytic output type mismatch at column {column}: descriptor={expected:?} actual={actual:?}"
            ),
        }
    }
}
/// ONE original analytic input gathering. The nonempty source proof belongs
/// to each caller; this helper changes neither Arrow order nor original Data.
pub(crate) fn concat_original_analytic_input(input: &[Chunk]) -> ExecutionResult<Chunk> {
    analytic_input_stages::unscoped(analytic_input_stages::gather(
        input,
        &mut analytic_input_stages::UnscopedWork,
    ))
}

#[cfg(test)]
fn validate_analytic_output_columns(
    columns: &[ArrayRef],
    output_chunk_schema: &crate::exec::chunk::ChunkSchemaRef,
    total_rows: usize,
) -> ExecutionResult<()> {
    validate_analytic_output_columns_typed(columns, output_chunk_schema, total_rows)
        .map_err(analytic_contract)
}
pub(crate) fn validate_analytic_output_columns_typed<'a>(
    columns: &'a [ArrayRef],
    output_chunk_schema: &'a crate::exec::chunk::ChunkSchemaRef,
    total_rows: usize,
) -> Result<(), AnalyticOutputValidationFailure<'a>> {
    let expected_columns = output_chunk_schema.slots().len();
    if columns.len() != expected_columns {
        return Err(AnalyticOutputValidationFailure::ColumnCount {
            expected: expected_columns,
            actual: columns.len(),
        });
    }
    for (idx, (col, slot)) in columns.iter().zip(output_chunk_schema.slots()).enumerate() {
        if col.len() != total_rows {
            return Err(AnalyticOutputValidationFailure::Length {
                column: idx,
                expected_rows: total_rows,
                actual: col.len(),
            });
        }
        let expected = slot.field();
        if col.data_type() != expected.data_type() {
            return Err(AnalyticOutputValidationFailure::Type {
                column: idx,
                expected: expected.data_type(),
                actual: col.data_type(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn compute_partitions(
    keys: &[ArrayRef],
    rows: usize,
) -> ExecutionResult<Vec<(usize, usize)>> {
    if rows == 0 {
        return Ok(Vec::new());
    }
    if keys.is_empty() {
        return Ok(vec![(0, rows)]);
    }
    let mut parts = Vec::new();
    let mut start = 0usize;
    for i in 1..rows {
        if !row_equal_on_keys(keys, i - 1, i)? {
            parts.push((start, i));
            start = i;
        }
    }
    parts.push((start, rows));
    Ok(parts)
}

#[cfg(test)]
pub(crate) fn split_analytic_output_chunks(
    output_chunk_schema: ChunkSchemaRef,
    columns: &[ArrayRef],
    input: &[Chunk],
) -> ExecutionResult<VecDeque<Chunk>> {
    analytic_input_stages::unscoped(analytic_input_stages::split(
        output_chunk_schema,
        columns,
        input,
        &mut analytic_input_stages::UnscopedWork,
    ))
}

pub(crate) fn reorder_chunk_by_partition_keys(
    chunk: &Chunk,
    keys: &[ArrayRef],
) -> ExecutionResult<Chunk> {
    analytic_input_stages::unscoped(analytic_input_stages::regroup(
        chunk,
        keys,
        &mut analytic_input_stages::UnscopedWork,
    ))
}

#[cfg(test)]
fn compare_rows_on_partition_keys(
    keys: &[ArrayRef],
    left: usize,
    right: usize,
) -> ExecutionResult<Ordering> {
    analytic_input_stages::unscoped_compare(keys, left, right)
}

pub(crate) fn row_equal_on_keys(
    keys: &[ArrayRef],
    left: usize,
    right: usize,
) -> ExecutionResult<bool> {
    for a in keys {
        if !value_equal_or_both_null(a.as_ref(), left, right)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn value_equal_or_both_null(array: &dyn Array, left: usize, right: usize) -> ExecutionResult<bool> {
    if array.is_null(left) && array.is_null(right) {
        return Ok(true);
    }
    if array.is_null(left) || array.is_null(right) {
        return Ok(false);
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| analytic_contract("failed to downcast BooleanArray"))?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Int8Array"))?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Int16Array"))?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Int32Array"))?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Int64Array"))?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::UInt32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .ok_or_else(|| analytic_contract("failed to downcast UInt32Array"))?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Float32Array"))?;
            Ok(a.value(left).to_bits() == a.value(right).to_bits())
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Float64Array"))?;
            Ok(a.value(left).to_bits() == a.value(right).to_bits())
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| analytic_contract("failed to downcast StringArray"))?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Date32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Date32Array"))?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                    .ok_or_else(|| analytic_contract("failed to downcast TimestampSecondArray"))?;
                Ok(a.value(left) == a.value(right))
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        analytic_contract("failed to downcast TimestampMillisecondArray")
                    })?;
                Ok(a.value(left) == a.value(right))
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        analytic_contract("failed to downcast TimestampMicrosecondArray")
                    })?;
                Ok(a.value(left) == a.value(right))
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        analytic_contract("failed to downcast TimestampNanosecondArray")
                    })?;
                Ok(a.value(left) == a.value(right))
            }
        },
        DataType::Decimal128(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Decimal128Array"))?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Decimal256(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal256Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Decimal256Array"))?;
            Ok(a.value(left) == a.value(right))
        }
        other => Err(analytic_contract(format_args!(
            "unsupported key type for equality: {:?}",
            other
        ))),
    }
}

fn compare_at(array: &dyn Array, left: usize, right: usize) -> ExecutionResult<std::cmp::Ordering> {
    match array.data_type() {
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| analytic_contract("failed to downcast BooleanArray"))?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Int8Array"))?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Int16Array"))?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Int32Array"))?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Int64Array"))?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Float32Array"))?;
            Ok(cmp_f64(a.value(left) as f64, a.value(right) as f64))
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Float64Array"))?;
            Ok(cmp_f64(a.value(left), a.value(right)))
        }
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                    .ok_or_else(|| analytic_contract("failed to downcast TimestampSecondArray"))?;
                Ok(a.value(left).cmp(&a.value(right)))
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        analytic_contract("failed to downcast TimestampMillisecondArray")
                    })?;
                Ok(a.value(left).cmp(&a.value(right)))
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        analytic_contract("failed to downcast TimestampMicrosecondArray")
                    })?;
                Ok(a.value(left).cmp(&a.value(right)))
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        analytic_contract("failed to downcast TimestampNanosecondArray")
                    })?;
                Ok(a.value(left).cmp(&a.value(right)))
            }
        },
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| analytic_contract("failed to downcast StringArray"))?;
            Ok(a.value(left).cmp(a.value(right)))
        }
        DataType::Date32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Date32Array"))?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Decimal128(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Decimal128Array"))?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Decimal256(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal256Array>()
                .ok_or_else(|| analytic_contract("failed to downcast Decimal256Array"))?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        other => Err(analytic_contract(format_args!(
            "unsupported type for min/max: {:?}",
            other
        ))),
    }
}

fn cmp_f64(a: f64, b: f64) -> std::cmp::Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::Field;
    use novarocks_types::SlotId;
    use std::sync::Arc;

    fn int32_chunk_schema() -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("v", DataType::Int32, true),
                None,
                None,
            )])
            .unwrap(),
        )
    }

    fn int32_chunk(values: Vec<Option<i32>>) -> Chunk {
        Chunk::try_new_with_columns(
            int32_chunk_schema(),
            vec![Arc::new(Int32Array::from(values)) as ArrayRef],
        )
        .unwrap()
    }

    fn int64_chunk_schema() -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("v", DataType::Int64, true),
                None,
                None,
            )])
            .unwrap(),
        )
    }

    fn binary_output_chunk_schema() -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("v", DataType::Binary, true),
                None,
                None,
            )])
            .unwrap(),
        )
    }

    fn two_int32_chunk_schema() -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    Field::new("v", DataType::Int32, true),
                    None,
                    None,
                ),
                ChunkSlotSchema::new_with_field(
                    SlotId::new(2),
                    Field::new("w", DataType::Int32, true),
                    None,
                    None,
                ),
            ])
            .unwrap(),
        )
    }

    fn int64_chunk(values: Vec<Option<i64>>) -> Chunk {
        Chunk::try_new_with_columns(
            int64_chunk_schema(),
            vec![Arc::new(Int64Array::from(values)) as ArrayRef],
        )
        .unwrap()
    }

    #[test]
    fn analytic_output_rejects_descriptor_type_drift() {
        // Validate the same Int64 source against the Binary descriptor through
        // the retained typed layout owner, without a retired window evaluator.
        let input = int64_chunk(vec![Some(1), Some(2)]);
        let columns = input.columns().to_vec();
        let output = binary_output_chunk_schema();
        let error = validate_analytic_output_columns_typed(&columns, &output, 2)
            .expect_err("descriptor drift should be rejected");
        let AnalyticOutputValidationFailure::Type {
            column,
            expected,
            actual,
        } = &error
        else {
            panic!("exact original type failure");
        };
        assert_eq!(*column, 0);
        assert_eq!(*expected, &DataType::Binary);
        assert_eq!(*actual, &DataType::Int64);
        assert_eq!(error.output_ordinal(), Some(0));
        assert_eq!(
            error.to_string(),
            "analytic output type mismatch at column 0: descriptor=Binary actual=Int64"
        );
    }

    #[test]
    fn analytic_output_rejects_column_count_mismatch() {
        let columns = vec![Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef];

        let err = validate_analytic_output_columns(&columns, &two_int32_chunk_schema(), 2)
            .expect_err("column count mismatch should be rejected");
        let schema = two_int32_chunk_schema();
        let typed = validate_analytic_output_columns_typed(&columns, &schema, 2).unwrap_err();
        assert!(matches!(
            typed,
            AnalyticOutputValidationFailure::ColumnCount {
                expected: 2,
                actual: 1
            }
        ));
        assert_eq!(typed.output_ordinal(), None);

        assert!(
            err.to_string()
                .contains("analytic output column count mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn analytic_output_rejects_length_mismatch() {
        let columns = vec![Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef];

        let err = validate_analytic_output_columns(&columns, &int32_chunk_schema(), 2)
            .expect_err("column length mismatch should be rejected");
        let schema = int32_chunk_schema();
        let typed = validate_analytic_output_columns_typed(&columns, &schema, 2).unwrap_err();
        assert!(matches!(
            typed,
            AnalyticOutputValidationFailure::Length {
                column: 0,
                expected_rows: 2,
                actual: 1
            }
        ));
        assert_eq!(typed.output_ordinal(), Some(0));

        assert!(
            err.to_string().contains("analytic output length mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn analytic_output_preserves_input_chunk_boundaries() {
        let input = vec![
            int32_chunk(vec![Some(1), Some(2)]),
            int32_chunk(vec![Some(3), Some(4), Some(5)]),
        ];
        let gathered = concat_original_analytic_input(&input).unwrap();
        let columns = gathered.columns().to_vec();
        let schema = int32_chunk_schema();
        validate_analytic_output_columns_typed(&columns, &schema, 5).unwrap();
        let output = split_analytic_output_chunks(schema, &columns, &input).unwrap();
        assert_eq!(
            output.iter().map(Chunk::len).collect::<Vec<_>>(),
            vec![2, 3]
        );
        for (actual, expected) in output.iter().zip(&input) {
            assert_eq!(
                actual.columns()[0].to_data(),
                expected.columns()[0].to_data()
            );
        }
    }
}

#[cfg(test)]
#[path = "analytic_input_stage_tests.rs"]
mod analytic_input_stage_tests;

#[cfg(test)]
mod source_operation_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::sync::Arc;
    #[test]
    fn real_key_equality_guard_does_not_keep_its_input_array() {
        let input: ArrayRef = Arc::new(arrow::array::BinaryArray::from(vec![
            b"a".as_slice(),
            b"b".as_slice(),
        ]));
        let weak = Arc::downgrade(&input);
        let failure = compute_partitions(&[input], 2).unwrap_err();
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 505);
    }
}
