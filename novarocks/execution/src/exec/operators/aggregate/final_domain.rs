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

use std::error::Error;
use std::fmt;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::exec::hash_table::key_column::KeyColumn;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FinalAggregateDomainError {
    MembershipKeyCount {
        actual: usize,
    },
    FinalKeyMaterialization(String),
    FinalKeyStructure(String),
    FinalKeyTypeMismatch {
        expected: DataType,
        actual: DataType,
    },
    FinalKeyRowCountMismatch {
        expected: usize,
        actual: usize,
    },
}

impl fmt::Display for FinalAggregateDomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MembershipKeyCount { actual } => write!(
                formatter,
                "final aggregate domain requires exactly one membership key column, got {actual}"
            ),
            Self::FinalKeyMaterialization(detail) => {
                write!(
                    formatter,
                    "failed to materialize final aggregate key column: {detail}"
                )
            }
            Self::FinalKeyStructure(detail) => {
                write!(formatter, "invalid final aggregate key column: {detail}")
            }
            Self::FinalKeyTypeMismatch { expected, actual } => write!(
                formatter,
                "final aggregate key materialization type mismatch: expected {expected:?}, got {actual:?}"
            ),
            Self::FinalKeyRowCountMismatch { expected, actual } => write!(
                formatter,
                "final aggregate key materialization row count mismatch: expected {expected}, got {actual}"
            ),
        }
    }
}

impl Error for FinalAggregateDomainError {}

/// Materializes the one install-frozen membership key from finalized aggregate
/// state. Execution owns typed final-domain encoding and resource accounting;
/// Core only verifies the aggregate kernel shape and transfers its Arrow column.
pub(crate) fn extract_final_aggregate_key(
    final_key_columns: &[KeyColumn],
) -> Result<(DataType, ArrayRef), FinalAggregateDomainError> {
    let [final_key_column] = final_key_columns else {
        return Err(FinalAggregateDomainError::MembershipKeyCount {
            actual: final_key_columns.len(),
        });
    };
    let expected_rows = final_key_row_count(final_key_column)?;
    let expected_type = final_key_column.data_type();
    let array = final_key_column
        .to_array()
        .map_err(FinalAggregateDomainError::FinalKeyMaterialization)?;
    if array.len() != expected_rows {
        return Err(FinalAggregateDomainError::FinalKeyRowCountMismatch {
            expected: expected_rows,
            actual: array.len(),
        });
    }
    if array.data_type() != &expected_type {
        return Err(FinalAggregateDomainError::FinalKeyTypeMismatch {
            expected: expected_type,
            actual: array.data_type().clone(),
        });
    }
    Ok((expected_type, array))
}

fn final_key_row_count(final_key_column: &KeyColumn) -> Result<usize, FinalAggregateDomainError> {
    fn parallel_row_count(
        values: usize,
        nulls: usize,
        key_type: &str,
    ) -> Result<usize, FinalAggregateDomainError> {
        if values != nulls {
            return Err(FinalAggregateDomainError::FinalKeyStructure(format!(
                "{key_type} values/null bitmap length mismatch: values={values} nulls={nulls}"
            )));
        }
        Ok(values)
    }

    match final_key_column {
        KeyColumn::Int8 { values, nulls } => parallel_row_count(values.len(), nulls.len(), "Int8"),
        KeyColumn::Int16 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Int16")
        }
        KeyColumn::Int32 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Int32")
        }
        KeyColumn::Int64 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Int64")
        }
        KeyColumn::Float32 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Float32")
        }
        KeyColumn::Float64 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Float64")
        }
        KeyColumn::Boolean { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Boolean")
        }
        KeyColumn::Utf8 {
            offsets,
            data,
            nulls,
        } => {
            let expected_offsets = nulls.len().checked_add(1).ok_or_else(|| {
                FinalAggregateDomainError::FinalKeyStructure(
                    "Utf8 null bitmap length overflows offset count".to_string(),
                )
            })?;
            if offsets.len() != expected_offsets
                || offsets.first() != Some(&0)
                || offsets.last() != Some(&data.len())
            {
                return Err(FinalAggregateDomainError::FinalKeyStructure(
                    "Utf8 offsets do not match the key data and null bitmap".to_string(),
                ));
            }
            for window in offsets.windows(2) {
                let start = window[0];
                let end = window[1];
                if start > end || end > data.len() {
                    return Err(FinalAggregateDomainError::FinalKeyStructure(
                        "Utf8 offsets are not monotonic and in-bounds".to_string(),
                    ));
                }
                std::str::from_utf8(&data[start..end]).map_err(|error| {
                    FinalAggregateDomainError::FinalKeyStructure(format!(
                        "Utf8 key bytes are invalid: {error}"
                    ))
                })?;
            }
            Ok(nulls.len())
        }
        KeyColumn::Date32 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Date32")
        }
        KeyColumn::Timestamp { values, nulls, .. } => {
            parallel_row_count(values.len(), nulls.len(), "Timestamp")
        }
        KeyColumn::Decimal128 { values, nulls, .. } => {
            parallel_row_count(values.len(), nulls.len(), "Decimal128")
        }
        KeyColumn::Decimal256 { values, nulls, .. } => {
            parallel_row_count(values.len(), nulls.len(), "Decimal256")
        }
        KeyColumn::LargeIntBinary { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "LargeInt")
        }
        KeyColumn::ListUtf8 { values } => Ok(values.len()),
        KeyColumn::ListInt32 { values } => Ok(values.len()),
        KeyColumn::Complex {
            keys,
            nulls,
            values,
            ..
        } => {
            let rows = parallel_row_count(keys.len(), nulls.len(), "Complex")?;
            parallel_row_count(rows, values.len(), "Complex")
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::DataType;

    use super::{FinalAggregateDomainError, extract_final_aggregate_key};
    use crate::exec::hash_table::key_column::KeyColumn;

    #[test]
    fn final_aggregate_key_transfers_the_validated_arrow_column() {
        let columns = vec![KeyColumn::Int64 {
            values: vec![7, -2, 7],
            nulls: vec![1, 1, 1],
        }];

        let (data_type, array) = extract_final_aggregate_key(&columns).expect("valid key column");

        assert_eq!(data_type, DataType::Int64);
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array")
                .values(),
            &[7, -2, 7]
        );
    }

    #[test]
    fn final_aggregate_key_rejects_invalid_kernel_shape_before_execution_encoding() {
        assert_eq!(
            extract_final_aggregate_key(&[]),
            Err(FinalAggregateDomainError::MembershipKeyCount { actual: 0 })
        );
        let multiple = vec![
            KeyColumn::Int64 {
                values: vec![1],
                nulls: vec![1],
            },
            KeyColumn::Int64 {
                values: vec![2],
                nulls: vec![1],
            },
        ];
        assert_eq!(
            extract_final_aggregate_key(&multiple),
            Err(FinalAggregateDomainError::MembershipKeyCount { actual: 2 })
        );
        let malformed = vec![KeyColumn::Int64 {
            values: vec![1, 2],
            nulls: vec![1],
        }];
        assert!(matches!(
            extract_final_aggregate_key(&malformed),
            Err(FinalAggregateDomainError::FinalKeyStructure(_))
        ));
    }
}
