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
//! Runtime-filter module exports and shared conversions.
//!
//! Responsibilities:
//! - Re-exports runtime-filter implementations and codec helpers used across exec runtime.
//! - Provides shared type conversion helpers required by filter serialization.
//!
//! Key exported interfaces:
//! - Functions: `data_type_to_tprimitive`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use arrow::datatypes::DataType;

use crate::common::largeint;

mod apply;
mod bitset;
mod bloom;
mod codec;
mod in_filter;
mod local;
mod membership;
mod merger;
mod min_max;

pub(crate) use apply::{
    filter_chunk_by_in_filters_with_exprs, filter_chunk_by_membership_filters_with_exprs,
};
pub(crate) use bitset::RuntimeBitsetFilter;
pub(crate) use bloom::RuntimeBloomFilter;
pub(crate) use codec::{
    StarrocksRuntimeFilterType, decode_starrocks_in_filter, decode_starrocks_membership_filter,
    encode_starrocks_bloom_filter, encode_starrocks_empty_filter, encode_starrocks_in_filter,
    peek_starrocks_filter_type,
};
pub(crate) use in_filter::{LocalRuntimeInFilterSet, RuntimeInFilter};
pub(crate) use local::{LocalRuntimeFilterSet, RuntimeFilterMembership};
pub(crate) use membership::{RuntimeEmptyFilter, RuntimeMembershipFilter};
pub(crate) use merger::{
    MAX_RUNTIME_IN_FILTER_CONDITIONS, PartialRuntimeInFilterMerger,
    RuntimeMembershipFilterBuildParam,
};
pub(crate) use min_max::RuntimeMinMaxFilter;

pub(in crate::exec::runtime_filter) use bloom::SimdBlockFilter;
pub(in crate::exec::runtime_filter) use in_filter::RuntimeInFilterValues;
pub(in crate::exec::runtime_filter) use local::row_has_null;

/// Map Arrow data type to StarRocks thrift primitive type for runtime filter serialization.
pub(crate) fn data_type_to_tprimitive(
    data_type: &DataType,
) -> Result<crate::types::TPrimitiveType, String> {
    use crate::types;
    let t = match data_type {
        DataType::Boolean => types::TPrimitiveType::BOOLEAN,
        DataType::Int8 => types::TPrimitiveType::TINYINT,
        DataType::Int16 => types::TPrimitiveType::SMALLINT,
        DataType::Int32 => types::TPrimitiveType::INT,
        DataType::Int64 => types::TPrimitiveType::BIGINT,
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            types::TPrimitiveType::LARGEINT
        }
        DataType::Float32 => types::TPrimitiveType::FLOAT,
        DataType::Float64 => types::TPrimitiveType::DOUBLE,
        DataType::Date32 => types::TPrimitiveType::DATE,
        DataType::Timestamp(_, _) => types::TPrimitiveType::DATETIME,
        DataType::Utf8 => types::TPrimitiveType::VARCHAR,
        DataType::Decimal128(_, _) => types::TPrimitiveType::DECIMAL128,
        _ => {
            return Err(format!(
                "unsupported runtime filter data type: {:?}",
                data_type
            ));
        }
    };
    Ok(t)
}
