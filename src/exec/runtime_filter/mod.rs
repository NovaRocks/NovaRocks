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
//! Runtime-filter module exports.
//!
//! Responsibilities:
//! - Re-exports runtime-filter implementations and codec helpers used across exec runtime.
//!
//! Key exported interfaces:
//! - Internal type model: `RuntimeFilterType`, `RuntimeDecimalWidth`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

mod apply;
mod bitset;
mod bloom;
mod codec;
mod in_filter;
mod local;
mod membership;
mod merger;
pub(crate) mod min_max;
mod proto_type;
mod types;

#[allow(unused_imports)]
pub(crate) use apply::{
    RuntimeFilterDictionaryFoldCache, filter_chunk_by_in_filters_with_exprs,
    filter_chunk_by_in_filters_with_exprs_and_dict_cache,
    filter_chunk_by_membership_filters_with_exprs,
    filter_chunk_by_membership_filters_with_exprs_and_dict_cache,
    filter_chunk_by_min_max_filters_with_exprs,
    filter_chunk_by_min_max_filters_with_exprs_and_dict_cache,
};
pub(crate) use bitset::{RuntimeBitsetFilter, maybe_build_runtime_bitset_filter};
pub(crate) use bloom::RuntimeBloomFilter;
pub(crate) use codec::{
    StarrocksRuntimeFilterType, decode_starrocks_in_filter, decode_starrocks_membership_filter,
    encode_starrocks_bitset_filter, encode_starrocks_bloom_filter, encode_starrocks_empty_filter,
    encode_starrocks_in_filter, peek_starrocks_filter_type,
};
pub(crate) use in_filter::{LocalRuntimeInFilterSet, RuntimeInFilter};
pub(crate) use local::LocalRuntimeFilterSet;
pub(crate) use membership::{RuntimeEmptyFilter, RuntimeMembershipFilter};
pub(crate) use merger::{
    MAX_RUNTIME_IN_FILTER_CONDITIONS, PartialRuntimeInFilterMerger,
    RUNTIME_FILTER_JOIN_MODE_BROADCAST, RUNTIME_FILTER_JOIN_MODE_PARTITIONED,
    RuntimeFilterMergeDropCounters, RuntimeMembershipBuildOptions,
    RuntimeMembershipFilterBuildParam,
};
pub(crate) use min_max::RuntimeMinMaxFilter;
pub(crate) use proto_type::{arrow_type_from_proto_type_desc, arrow_type_to_proto_type_desc};
// Staged export for the B3 internal type model; follow-up tasks add production consumers.
#[allow(unused_imports)]
pub(crate) use types::{RuntimeDecimalWidth, RuntimeFilterType};

pub(in crate::exec::runtime_filter) use bloom::SimdBlockFilter;
pub(in crate::exec::runtime_filter) use in_filter::RuntimeInFilterValues;
