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

use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::DataType;
use thrift::OrderedFloat;

use crate::connector::iceberg::file_pruning::IcebergFilePruningMetadata;
use crate::sql::catalog::{ColumnDef, IcebergColumnStats, IcebergDataFileInfo};
use crate::thrift::{exprs, plan_nodes};

pub(crate) fn iceberg_file_pruning_metadata_to_thrift(
    file: &IcebergDataFileInfo,
    columns: &[ColumnDef],
) -> Option<BTreeMap<i32, exprs::TExprMinMaxValue>> {
    let stats = file.column_stats.as_ref()?;
    if stats.is_empty() || columns.is_empty() {
        return None;
    }

    let mut out = BTreeMap::new();
    for (ordinal, column) in columns.iter().enumerate() {
        let Some(stat) = find_column_stats(stats, &column.name) else {
            continue;
        };
        let Some(value) = thrift_min_max_value_from_stats(stat, &column.data_type) else {
            continue;
        };
        out.insert(i32::try_from(ordinal).ok()?, value);
    }

    if out.is_empty() { None } else { Some(out) }
}

pub(crate) fn iceberg_file_pruning_metadata_from_thrift(
    hdfs_range: &plan_nodes::THdfsScanRange,
    column_names: &[String],
) -> Option<IcebergFilePruningMetadata> {
    let values = hdfs_range.min_max_values.as_ref()?;
    if values.is_empty() || column_names.is_empty() {
        return None;
    }

    let mut columns = HashMap::new();
    for (ordinal, value) in values {
        let Ok(ordinal) = usize::try_from(*ordinal) else {
            continue;
        };
        let Some(column) = column_names.get(ordinal) else {
            continue;
        };
        let Some(stats) = column_stats_from_thrift_min_max_value(value) else {
            continue;
        };
        columns.insert(column.clone(), stats);
    }

    if columns.is_empty() {
        None
    } else {
        Some(IcebergFilePruningMetadata { columns })
    }
}

fn thrift_min_max_value_from_stats(
    stats: &IcebergColumnStats,
    data_type: &DataType,
) -> Option<exprs::TExprMinMaxValue> {
    let has_null = stats.null_count.unwrap_or(0) > 0;
    let all_null = stats
        .value_count
        .zip(stats.null_count)
        .is_some_and(|(value_count, null_count)| value_count > 0 && value_count == null_count);

    match data_type {
        DataType::Boolean => {
            let lower = stats.lower_bound.as_deref().and_then(decode_bool_bound)?;
            let upper = stats.upper_bound.as_deref().and_then(decode_bool_bound)?;
            Some(exprs::TExprMinMaxValue::new(
                exprs::TExprNodeType::BOOL_LITERAL,
                has_null,
                all_null,
                Some(i64::from(lower)),
                Some(i64::from(upper)),
                None::<OrderedFloat<f64>>,
                None::<OrderedFloat<f64>>,
            ))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let lower = stats
                .lower_bound
                .as_deref()
                .and_then(|bytes| decode_int_bound_for_type(bytes, data_type))?;
            let upper = stats
                .upper_bound
                .as_deref()
                .and_then(|bytes| decode_int_bound_for_type(bytes, data_type))?;
            Some(exprs::TExprMinMaxValue::new(
                exprs::TExprNodeType::INT_LITERAL,
                has_null,
                all_null,
                Some(lower),
                Some(upper),
                None::<OrderedFloat<f64>>,
                None::<OrderedFloat<f64>>,
            ))
        }
        DataType::Float32 | DataType::Float64 => {
            let lower = stats
                .lower_bound
                .as_deref()
                .and_then(|bytes| decode_float_bound_for_type(bytes, data_type))?;
            let upper = stats
                .upper_bound
                .as_deref()
                .and_then(|bytes| decode_float_bound_for_type(bytes, data_type))?;
            if lower.is_nan() || upper.is_nan() {
                return None;
            }
            Some(exprs::TExprMinMaxValue::new(
                exprs::TExprNodeType::FLOAT_LITERAL,
                has_null,
                all_null,
                None::<i64>,
                None::<i64>,
                Some(OrderedFloat(lower)),
                Some(OrderedFloat(upper)),
            ))
        }
        _ => None,
    }
}

fn column_stats_from_thrift_min_max_value(
    value: &exprs::TExprMinMaxValue,
) -> Option<IcebergColumnStats> {
    let (lower_bound, upper_bound) = match value.type_ {
        exprs::TExprNodeType::BOOL_LITERAL => {
            let lower = bool_bound_to_byte(value.min_int_value?)?;
            let upper = bool_bound_to_byte(value.max_int_value?)?;
            (vec![lower], vec![upper])
        }
        exprs::TExprNodeType::INT_LITERAL => (
            value.min_int_value?.to_le_bytes().to_vec(),
            value.max_int_value?.to_le_bytes().to_vec(),
        ),
        exprs::TExprNodeType::FLOAT_LITERAL => {
            let lower = value.min_float_value?.0;
            let upper = value.max_float_value?.0;
            if lower.is_nan() || upper.is_nan() {
                return None;
            }
            (lower.to_le_bytes().to_vec(), upper.to_le_bytes().to_vec())
        }
        _ => return None,
    };

    Some(IcebergColumnStats {
        null_count: None,
        value_count: None,
        column_size: None,
        lower_bound: Some(lower_bound),
        upper_bound: Some(upper_bound),
    })
}

fn find_column_stats<'a>(
    column_stats: &'a HashMap<String, IcebergColumnStats>,
    column: &str,
) -> Option<&'a IcebergColumnStats> {
    column_stats.get(column).or_else(|| {
        column_stats
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, stats)| stats)
    })
}

fn bool_bound_to_byte(value: i64) -> Option<u8> {
    match value {
        0 => Some(0),
        1 => Some(1),
        _ => None,
    }
}

fn decode_bool_bound(bytes: &[u8]) -> Option<bool> {
    match bytes {
        [0] => Some(false),
        [1] => Some(true),
        _ => None,
    }
}

fn decode_int_bound_for_type(bytes: &[u8], data_type: &DataType) -> Option<i64> {
    match data_type {
        DataType::Int8 => {
            let arr: [u8; 1] = bytes.try_into().ok()?;
            Some(i64::from(i8::from_le_bytes(arr)))
        }
        DataType::Int16 => {
            let arr: [u8; 2] = bytes.try_into().ok()?;
            Some(i64::from(i16::from_le_bytes(arr)))
        }
        DataType::Int32 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(i64::from(i32::from_le_bytes(arr)))
        }
        DataType::Int64 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(i64::from_le_bytes(arr))
        }
        _ => None,
    }
}

fn decode_float_bound_for_type(bytes: &[u8], data_type: &DataType) -> Option<f64> {
    match data_type {
        DataType::Float32 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(f64::from(f32::from_le_bytes(arr)))
        }
        DataType::Float64 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(f64::from_le_bytes(arr))
        }
        _ => None,
    }
}
