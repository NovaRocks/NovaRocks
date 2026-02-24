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
use arrow::datatypes::DataType;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GroupKeyStrategy {
    /// No grouping keys (scalar aggregation).
    Scalar,
    /// Single fixed-width numeric or temporal key.
    OneNumber,
    /// Single UTF-8 key stored directly in the hash map.
    OneString,
    /// Multiple fixed-width keys packed into a small fixed-size buffer (<= 16 bytes).
    FixedSize,
    /// Multiple fixed-width numeric keys that can be bit-compressed into a fixed buffer.
    CompressedFixed,
    /// Variable-length serialized row key (fallback for mixed or wide keys).
    Serialized,
}

pub(crate) fn pick_group_key_strategy(types: &[DataType]) -> GroupKeyStrategy {
    if types.is_empty() {
        return GroupKeyStrategy::Scalar;
    }
    if types.len() == 1 {
        if matches!(types[0], DataType::Utf8) {
            return GroupKeyStrategy::OneString;
        }
        if is_fixed_width_key_type(&types[0]) {
            return GroupKeyStrategy::OneNumber;
        }
        return GroupKeyStrategy::Serialized;
    }
    if can_apply_compressed_key(types) {
        return GroupKeyStrategy::CompressedFixed;
    }
    if can_apply_fixed_size_key(types) {
        return GroupKeyStrategy::FixedSize;
    }
    GroupKeyStrategy::Serialized
}

pub(crate) fn can_apply_fixed_size_key(types: &[DataType]) -> bool {
    let null_bytes = (types.len() + 7) / 8;
    let mut total = null_bytes;
    for data_type in types {
        let Some(width) = fixed_width_size(data_type) else {
            return false;
        };
        total = total.saturating_add(width);
        if total > 16 {
            return false;
        }
    }
    true
}

pub(crate) fn can_apply_compressed_key(types: &[DataType]) -> bool {
    if types.len() < 2 {
        return false;
    }
    let mut total = 0usize;
    for data_type in types {
        if !is_compressible_key_type(data_type) {
            return false;
        }
        let Some(width) = fixed_width_size(data_type) else {
            return false;
        };
        total = total.saturating_add(width);
    }
    total > 16
}

pub(crate) fn is_fixed_width_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Boolean
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Null
    )
}

pub(crate) fn is_compressible_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Timestamp(_, _)
    )
}

pub(crate) fn fixed_width_size(data_type: &DataType) -> Option<usize> {
    let width = match data_type {
        DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 4,
        DataType::Int64 => 8,
        DataType::Float32 => 4,
        DataType::Float64 => 8,
        DataType::Boolean => 1,
        DataType::Date32 => 4,
        DataType::Timestamp(_, _) => 8,
        DataType::Decimal128(_, _) => 16,
        DataType::Null => 1,
        _ => return None,
    };
    Some(width)
}
