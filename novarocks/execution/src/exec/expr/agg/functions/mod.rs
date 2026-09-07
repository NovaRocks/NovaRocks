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
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_functions::{
    AggregateImplementationIdentity, AggregateStateFormatIdentity, FunctionKind,
};

use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;

use std::sync::Arc;

use super::registry::{
    ExecutionFunctionSetBuilder, ExecutionFunctionSetError, RetainedMemoryPolicy,
};
use super::{AggInputView, AggSpec, AggStatePtr};

#[derive(Clone, Debug)]
pub(super) enum AggKind {
    Count,
    CountState,
    CountStateSigned,
    BoolState,
    BoolStateSigned,
    MinState,
    MaxState,
    MinStateSigned,
    MaxStateSigned,
    CountDistinct,
    CountIf,
    SumInt,
    SumLargeInt,
    SumFloat,
    SumDecimal128,
    SumDecimal256,
    SumStateInt64,
    SumStateDecimal128,
    CountStateMerge,
    AvgStateMerge,
    MinStateMerge,
    MaxStateMerge,
    BoolAndStateMerge,
    BoolOrStateMerge,
    CountDistinctStateMerge,
    ApproxCountDistinctStateMerge,
    SumStateMerge,
    SumStateSignedInt64,
    SumStateSignedDecimal128,
    MinInt,
    MaxInt,
    MinFloat,
    MaxFloat,
    MinBool,
    MaxBool,
    MinUtf8,
    MaxUtf8,
    MinDate32,
    MaxDate32,
    MinTimestamp,
    MaxTimestamp,
    MinLargeInt,
    MaxLargeInt,
    MinDecimal128,
    MaxDecimal128,
    MinDecimal256,
    MaxDecimal256,
    AvgInt,
    AvgFloat,
    AvgDecimal128,
    AvgDecimal256,
    VariancePop,
    VarianceSamp,
    StddevPop,
    StddevSamp,
    AnyValue,
    BoolOr,
    BoolAnd,
    CovarPop,
    CovarSamp,
    Corr,
    MaxBy,
    MinBy,
    MaxByV2,
    MinByV2,
    GroupConcat {
        is_distinct: bool,
        is_asc_order: Vec<bool>,
        nulls_first: Vec<bool>,
        max_len: i64,
    },
    MultiDistinctSum,
    MapAgg,
    SumMap,
    ArrayAgg {
        is_distinct: bool,
        is_asc_order: Vec<bool>,
        nulls_first: Vec<bool>,
    },
    ArrayUniqueAgg,
    MannWhitneyUTest,
    DictMerge,
    BitmapAgg,
    BitmapUnionInt,
    PercentileUnion,
    PercentileApprox,
    PercentileApproxWeighted,
    PercentileCont,
    PercentileDisc,
    PercentileDiscLc,
    ApproxTopK,
    HllRawHash,
    HllRawMerge,
    HllUnionCount,
    DsHllHash,
    DsHllMerge,
    DsHllCount,
    MinN,
    MaxN,
}

mod any_value;
mod approx_top_k;
mod array_agg;
mod avg;
mod bitmap_union_int;
mod bool_and;
mod bool_or;
pub mod common;
mod corr_covar;
mod count;
mod count_distinct;
mod count_if;
mod dict_merge;
mod ds_hll;
mod group_concat;
pub mod hll_raw;
mod mann_whitney_u_test;
mod map_agg;
mod max;
mod max_by;
mod min;
mod minmax_n;
mod multi_distinct_sum;
mod percentile;
mod percentile_placeholder;
mod state_combinators;
mod sum;
mod sum_map;
mod variance;

use any_value::AnyValueAgg;
use approx_top_k::ApproxTopKAgg;
use array_agg::ArrayAggAgg;
use avg::AvgAgg;
use bitmap_union_int::BitmapUnionIntAgg;
use bool_and::BoolAndAgg;
use bool_or::BoolOrAgg;
use corr_covar::CovarCorrAgg;
use count::CountAgg;
use count_distinct::CountDistinctAgg;
use count_if::CountIfAgg;
use dict_merge::DictMergeAgg;
use ds_hll::DsHllAgg;
use group_concat::GroupConcatAgg;
use hll_raw::HllRawAgg;
use mann_whitney_u_test::MannWhitneyUTestAgg;
use map_agg::MapAggAgg;
use max::MaxAgg;
use max_by::MaxMinByAgg;
use min::MinAgg;
use minmax_n::MinMaxNAgg;
use multi_distinct_sum::MultiDistinctSumAgg;
use percentile::PercentileAgg;
use percentile_placeholder::PercentilePlaceholderAgg;
use state_combinators::approx_count_distinct::{
    ApproxCountDistinctStateAgg, ApproxCountDistinctStateSignedAgg,
};
use state_combinators::avg::{AvgStateAgg, AvgStateSignedAgg};
use state_combinators::bool_or_and::{BoolStateAgg, BoolStateSignedAgg};
use state_combinators::count::{CountStateAgg, CountStateSignedAgg};
use state_combinators::count_distinct::{CountDistinctStateAgg, CountDistinctStateSignedAgg};
use state_combinators::min_max::{MinMaxStateAgg, MinMaxStateSignedAgg};
use state_combinators::opaque_merge::OpaqueStateMergeAgg;
use state_combinators::sum::{SumStateAgg, SumStateMergeAgg, SumStateSignedAgg};
use sum::SumAgg;
use sum_map::SumMapAgg;
use variance::VarStdAgg;

pub(super) trait AggregateFunction: Send + Sync {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String>;

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize);

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String>;

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String>;

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8);
    fn init_state_with_tracker(
        &self,
        spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        if matches!(
            self.retained_memory_policy(spec),
            RetainedMemoryPolicy::AllocationTracked
        ) {
            return Err(
                "allocation-tracked aggregate must implement tracker-aware state initialization"
                    .to_string(),
            );
        }
        let _ = tracker;
        self.init_state(spec, ptr);
        Ok(())
    }
    fn drop_state(&self, spec: &AggSpec, ptr: *mut u8);
    /// Returns memory retained outside the aggregate arena allocation.
    ///
    /// Inline state bytes are already charged by `AggStateArena` and must not
    /// be included. Pointer-backed states must include the pointed-to state
    /// body because the arena only contains the pointer slot.
    fn retained_bytes(&self, spec: &AggSpec, ptr: *const u8) -> usize;

    /// Total memory contract. Every implementation must choose a policy, so a
    /// new aggregate cannot silently fall back to post-allocation accounting.
    fn retained_memory_policy(&self, spec: &AggSpec) -> RetainedMemoryPolicy;

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String>;

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String>;

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String>;
}

static COUNT: CountAgg = CountAgg;
static COUNT_STATE: CountStateAgg = CountStateAgg;
static COUNT_STATE_SIGNED: CountStateSignedAgg = CountStateSignedAgg;
static COUNT_DISTINCT_STATE: CountDistinctStateAgg = CountDistinctStateAgg;
static COUNT_DISTINCT_STATE_SIGNED: CountDistinctStateSignedAgg = CountDistinctStateSignedAgg;
static APPROX_COUNT_DISTINCT_STATE: ApproxCountDistinctStateAgg = ApproxCountDistinctStateAgg;
static APPROX_COUNT_DISTINCT_STATE_SIGNED: ApproxCountDistinctStateSignedAgg =
    ApproxCountDistinctStateSignedAgg;
static BOOL_STATE: BoolStateAgg = BoolStateAgg;
static BOOL_STATE_SIGNED: BoolStateSignedAgg = BoolStateSignedAgg;
static MIN_MAX_STATE: MinMaxStateAgg = MinMaxStateAgg;
static MIN_MAX_STATE_SIGNED: MinMaxStateSignedAgg = MinMaxStateSignedAgg;
static COUNT_DISTINCT: CountDistinctAgg = CountDistinctAgg;
static COUNT_IF: CountIfAgg = CountIfAgg;
static GROUP_CONCAT: GroupConcatAgg = GroupConcatAgg;
static SUM: SumAgg = SumAgg;
static COUNT_STATE_MERGE: OpaqueStateMergeAgg = OpaqueStateMergeAgg::new(
    "count_state_merge",
    AggKind::CountStateMerge,
    crate::exec::expr::function::mv_state::count_state_union,
);
static AVG_STATE_MERGE: OpaqueStateMergeAgg = OpaqueStateMergeAgg::new(
    "avg_state_merge",
    AggKind::AvgStateMerge,
    crate::exec::expr::function::mv_state::avg_state_union,
);
static MIN_STATE_MERGE: OpaqueStateMergeAgg = OpaqueStateMergeAgg::new(
    "min_state_merge",
    AggKind::MinStateMerge,
    crate::exec::expr::function::mv_state::min_state_union,
);
static MAX_STATE_MERGE: OpaqueStateMergeAgg = OpaqueStateMergeAgg::new(
    "max_state_merge",
    AggKind::MaxStateMerge,
    crate::exec::expr::function::mv_state::max_state_union,
);
static BOOL_AND_STATE_MERGE: OpaqueStateMergeAgg = OpaqueStateMergeAgg::new(
    "bool_and_state_merge",
    AggKind::BoolAndStateMerge,
    crate::exec::expr::function::mv_state::bool_and_state_union,
);
static BOOL_OR_STATE_MERGE: OpaqueStateMergeAgg = OpaqueStateMergeAgg::new(
    "bool_or_state_merge",
    AggKind::BoolOrStateMerge,
    crate::exec::expr::function::mv_state::bool_or_state_union,
);
static COUNT_DISTINCT_STATE_MERGE: OpaqueStateMergeAgg = OpaqueStateMergeAgg::new(
    "count_distinct_state_merge",
    AggKind::CountDistinctStateMerge,
    crate::exec::expr::function::mv_state::count_distinct_state_union,
);
static APPROX_COUNT_DISTINCT_STATE_MERGE: OpaqueStateMergeAgg = OpaqueStateMergeAgg::new(
    "approx_count_distinct_state_merge",
    AggKind::ApproxCountDistinctStateMerge,
    crate::exec::expr::function::mv_state::approx_count_distinct_state_union,
);
static SUM_STATE: SumStateAgg = SumStateAgg;
static SUM_STATE_MERGE: SumStateMergeAgg = SumStateMergeAgg;
static SUM_STATE_SIGNED: SumStateSignedAgg = SumStateSignedAgg;
static MIN: MinAgg = MinAgg;
static MAX: MaxAgg = MaxAgg;
static AVG: AvgAgg = AvgAgg;
static AVG_STATE: AvgStateAgg = AvgStateAgg;
static AVG_STATE_SIGNED: AvgStateSignedAgg = AvgStateSignedAgg;
static ARRAY_AGG: ArrayAggAgg = ArrayAggAgg;
static VAR_STD: VarStdAgg = VarStdAgg;
static ANY_VALUE: AnyValueAgg = AnyValueAgg;
static BOOL_OR: BoolOrAgg = BoolOrAgg;
static BOOL_AND: BoolAndAgg = BoolAndAgg;
static COVAR_CORR: CovarCorrAgg = CovarCorrAgg;
static MAX_MIN_BY: MaxMinByAgg = MaxMinByAgg;
static MULTI_DISTINCT_SUM: MultiDistinctSumAgg = MultiDistinctSumAgg;
static MAP_AGG: MapAggAgg = MapAggAgg;
static SUM_MAP: SumMapAgg = SumMapAgg;
static MANN_WHITNEY: MannWhitneyUTestAgg = MannWhitneyUTestAgg;
static DICT_MERGE: DictMergeAgg = DictMergeAgg;
static DS_HLL: DsHllAgg = DsHllAgg;
static BITMAP_UNION_INT: BitmapUnionIntAgg = BitmapUnionIntAgg;
static PERCENTILE: PercentileAgg = PercentileAgg;
static PERCENTILE_PLACEHOLDER: PercentilePlaceholderAgg = PercentilePlaceholderAgg;
static APPROX_TOP_K: ApproxTopKAgg = ApproxTopKAgg;
static HLL_RAW: HllRawAgg = HllRawAgg;
static MIN_MAX_N: MinMaxNAgg = MinMaxNAgg;

#[cfg(test)]
fn resolve_by_func(func: &AggFunction) -> Result<&'static dyn AggregateFunction, String> {
    resolve_by_name(func.name.as_str())
}

#[derive(Clone, Copy)]
struct BuiltinAggregateImplementation {
    canonical_name: &'static str,
    implementation_contract: &'static str,
    expected_state_format: &'static str,
    function: &'static dyn AggregateFunction,
}

macro_rules! builtin_aggregate {
    ($name:literal, $function:expr) => {
        BuiltinAggregateImplementation {
            canonical_name: $name,
            implementation_contract: concat!("novarocks/", $name, "/legacy-exec-v1"),
            expected_state_format: concat!("novarocks/", $name, "/state-v1"),
            function: $function,
        }
    };
}

// This is the implementation-side closed set. Startup composition enumerates
// it directly, so adding executable code without matching catalog metadata
// fails closed instead of remaining invisible to the reverse coverage check.
static BUILTIN_AGGREGATE_IMPLEMENTATIONS: &[BuiltinAggregateImplementation] = &[
    builtin_aggregate!("count", &COUNT),
    builtin_aggregate!("count_state", &COUNT_STATE),
    builtin_aggregate!("count_state_signed", &COUNT_STATE_SIGNED),
    builtin_aggregate!("count_distinct_state", &COUNT_DISTINCT_STATE),
    builtin_aggregate!("count_distinct_state_signed", &COUNT_DISTINCT_STATE_SIGNED),
    builtin_aggregate!("approx_count_distinct_state", &APPROX_COUNT_DISTINCT_STATE),
    builtin_aggregate!(
        "approx_count_distinct_state_signed",
        &APPROX_COUNT_DISTINCT_STATE_SIGNED
    ),
    builtin_aggregate!("bool_or_state", &BOOL_STATE),
    builtin_aggregate!("bool_and_state", &BOOL_STATE),
    builtin_aggregate!("bool_or_state_signed", &BOOL_STATE_SIGNED),
    builtin_aggregate!("bool_and_state_signed", &BOOL_STATE_SIGNED),
    builtin_aggregate!("min_state", &MIN_MAX_STATE),
    builtin_aggregate!("max_state", &MIN_MAX_STATE),
    builtin_aggregate!("min_state_signed", &MIN_MAX_STATE_SIGNED),
    builtin_aggregate!("max_state_signed", &MIN_MAX_STATE_SIGNED),
    builtin_aggregate!("multi_distinct_count", &COUNT_DISTINCT),
    builtin_aggregate!("count_if", &COUNT_IF),
    builtin_aggregate!("group_concat", &GROUP_CONCAT),
    builtin_aggregate!("string_agg", &GROUP_CONCAT),
    builtin_aggregate!("sum", &SUM),
    builtin_aggregate!("count_state_merge", &COUNT_STATE_MERGE),
    builtin_aggregate!("avg_state_merge", &AVG_STATE_MERGE),
    builtin_aggregate!("min_state_merge", &MIN_STATE_MERGE),
    builtin_aggregate!("max_state_merge", &MAX_STATE_MERGE),
    builtin_aggregate!("bool_and_state_merge", &BOOL_AND_STATE_MERGE),
    builtin_aggregate!("bool_or_state_merge", &BOOL_OR_STATE_MERGE),
    builtin_aggregate!("count_distinct_state_merge", &COUNT_DISTINCT_STATE_MERGE),
    builtin_aggregate!(
        "approx_count_distinct_state_merge",
        &APPROX_COUNT_DISTINCT_STATE_MERGE
    ),
    builtin_aggregate!("sum_state", &SUM_STATE),
    builtin_aggregate!("sum_state_merge", &SUM_STATE_MERGE),
    builtin_aggregate!("sum_state_signed", &SUM_STATE_SIGNED),
    builtin_aggregate!("min", &MIN),
    builtin_aggregate!("max", &MAX),
    builtin_aggregate!("avg", &AVG),
    builtin_aggregate!("avg_state", &AVG_STATE),
    builtin_aggregate!("avg_state_signed", &AVG_STATE_SIGNED),
    builtin_aggregate!("array_agg", &ARRAY_AGG),
    builtin_aggregate!("array_agg_distinct", &ARRAY_AGG),
    builtin_aggregate!("array_unique_agg", &ARRAY_AGG),
    builtin_aggregate!("variance", &VAR_STD),
    builtin_aggregate!("variance_pop", &VAR_STD),
    builtin_aggregate!("var_pop", &VAR_STD),
    builtin_aggregate!("variance_samp", &VAR_STD),
    builtin_aggregate!("var_samp", &VAR_STD),
    builtin_aggregate!("stddev", &VAR_STD),
    builtin_aggregate!("stddev_pop", &VAR_STD),
    builtin_aggregate!("stddev_samp", &VAR_STD),
    builtin_aggregate!("std", &VAR_STD),
    builtin_aggregate!("any_value", &ANY_VALUE),
    builtin_aggregate!("percentile_union", &PERCENTILE),
    builtin_aggregate!("percentile_approx", &PERCENTILE),
    builtin_aggregate!("percentile_approx_weighted", &PERCENTILE),
    builtin_aggregate!("percentile_disc", &PERCENTILE_PLACEHOLDER),
    builtin_aggregate!("percentile_cont", &PERCENTILE_PLACEHOLDER),
    builtin_aggregate!("percentile_disc_lc", &PERCENTILE_PLACEHOLDER),
    builtin_aggregate!("bool_or", &BOOL_OR),
    builtin_aggregate!("boolor_agg", &BOOL_OR),
    builtin_aggregate!("bool_and", &BOOL_AND),
    builtin_aggregate!("booland_agg", &BOOL_AND),
    builtin_aggregate!("covar_pop", &COVAR_CORR),
    builtin_aggregate!("covar_samp", &COVAR_CORR),
    builtin_aggregate!("corr", &COVAR_CORR),
    builtin_aggregate!("max_by", &MAX_MIN_BY),
    builtin_aggregate!("min_by", &MAX_MIN_BY),
    builtin_aggregate!("multi_distinct_sum", &MULTI_DISTINCT_SUM),
    builtin_aggregate!("map_agg", &MAP_AGG),
    builtin_aggregate!("sum_map", &SUM_MAP),
    builtin_aggregate!("mann_whitney_u_test", &MANN_WHITNEY),
    builtin_aggregate!("dict_merge", &DICT_MERGE),
    builtin_aggregate!("bitmap_agg", &BITMAP_UNION_INT),
    builtin_aggregate!("bitmap_union", &BITMAP_UNION_INT),
    builtin_aggregate!("bitmap_union_count", &BITMAP_UNION_INT),
    builtin_aggregate!("bitmap_union_int", &BITMAP_UNION_INT),
    builtin_aggregate!("approx_top_k", &APPROX_TOP_K),
    builtin_aggregate!("min_n", &MIN_MAX_N),
    builtin_aggregate!("max_n", &MIN_MAX_N),
    builtin_aggregate!("ds_hll_count_distinct", &DS_HLL),
    builtin_aggregate!("ds_hll_count_distinct_union", &DS_HLL),
    builtin_aggregate!("ds_hll_count_distinct_merge", &DS_HLL),
    builtin_aggregate!("approx_count_distinct_hll_sketch", &DS_HLL),
    builtin_aggregate!("hll_union", &HLL_RAW),
    builtin_aggregate!("hll_raw_agg", &HLL_RAW),
    builtin_aggregate!("hll_union_agg", &HLL_RAW),
    builtin_aggregate!("ndv", &HLL_RAW),
    builtin_aggregate!("approx_count_distinct", &HLL_RAW),
];

#[cfg(test)]
fn resolve_by_name(name: &str) -> Result<&'static dyn AggregateFunction, String> {
    let canonical_name = canonical_agg_name(name);
    BUILTIN_AGGREGATE_IMPLEMENTATIONS
        .iter()
        .find(|implementation| implementation.canonical_name == canonical_name)
        .map(|implementation| implementation.function)
        .ok_or_else(|| format!("unsupported agg function: {canonical_name}"))
}

/// Installs the implementation-side aggregate manifest. Both overload and
/// state-format identities are declared independently from catalog metadata;
/// sealing compares the two closed sets exactly. Function names are used only
/// during process composition, and prepared kernels never perform a registry
/// lookup in the hot path.
pub fn contribute_builtin_aggregate_implementations(
    builder: &mut ExecutionFunctionSetBuilder,
) -> Result<(), ExecutionFunctionSetError> {
    for implementation in BUILTIN_AGGREGATE_IMPLEMENTATIONS {
        let _definition = builder
            .catalog_builder()
            .definition(implementation.canonical_name, FunctionKind::Aggregate)
            .ok_or_else(|| {
                ExecutionFunctionSetError::BuiltinAggregateImplementationWithoutMetadata {
                    canonical_name: implementation.canonical_name.into(),
                }
            })?;
        // This identity is the implementation-side executable contract. It is
        // intentionally not copied from catalog metadata: seal must detect a
        // metadata-only overload instead of fabricating implementation
        // coverage for it.
        let overloads = [novarocks_functions::AggregateOverloadIdentity::try_new(
            format!("builtin/{}/v1", implementation.canonical_name),
        )?];
        builder.register_legacy_aggregate(
            implementation.canonical_name,
            overloads,
            AggregateImplementationIdentity::try_new(implementation.implementation_contract)?,
            AggregateStateFormatIdentity::try_new(implementation.expected_state_format)?,
            implementation.function,
        )?;
    }
    Ok(())
}

#[cfg(test)]
fn resolve_by_kind(kind: &AggKind) -> &'static dyn AggregateFunction {
    match kind {
        AggKind::Count => &COUNT,
        AggKind::CountState => &COUNT_STATE,
        AggKind::CountStateSigned => &COUNT_STATE_SIGNED,
        AggKind::BoolState => &BOOL_STATE,
        AggKind::BoolStateSigned => &BOOL_STATE_SIGNED,
        AggKind::MinState | AggKind::MaxState => &MIN_MAX_STATE,
        AggKind::MinStateSigned | AggKind::MaxStateSigned => &MIN_MAX_STATE_SIGNED,
        AggKind::CountDistinct => &COUNT_DISTINCT,
        AggKind::CountIf => &COUNT_IF,
        AggKind::GroupConcat { .. } => &GROUP_CONCAT,
        AggKind::SumInt
        | AggKind::SumLargeInt
        | AggKind::SumFloat
        | AggKind::SumDecimal128
        | AggKind::SumDecimal256 => &SUM,
        AggKind::CountStateMerge => &COUNT_STATE_MERGE,
        AggKind::AvgStateMerge => &AVG_STATE_MERGE,
        AggKind::MinStateMerge => &MIN_STATE_MERGE,
        AggKind::MaxStateMerge => &MAX_STATE_MERGE,
        AggKind::BoolAndStateMerge => &BOOL_AND_STATE_MERGE,
        AggKind::BoolOrStateMerge => &BOOL_OR_STATE_MERGE,
        AggKind::CountDistinctStateMerge => &COUNT_DISTINCT_STATE_MERGE,
        AggKind::ApproxCountDistinctStateMerge => &APPROX_COUNT_DISTINCT_STATE_MERGE,
        AggKind::SumStateInt64 | AggKind::SumStateDecimal128 => &SUM_STATE,
        AggKind::SumStateMerge => &SUM_STATE_MERGE,
        AggKind::SumStateSignedInt64 | AggKind::SumStateSignedDecimal128 => &SUM_STATE_SIGNED,
        AggKind::MinInt
        | AggKind::MinFloat
        | AggKind::MinBool
        | AggKind::MinUtf8
        | AggKind::MinDate32
        | AggKind::MinTimestamp
        | AggKind::MinLargeInt
        | AggKind::MinDecimal128
        | AggKind::MinDecimal256 => &MIN,
        AggKind::MaxInt
        | AggKind::MaxFloat
        | AggKind::MaxBool
        | AggKind::MaxUtf8
        | AggKind::MaxDate32
        | AggKind::MaxTimestamp
        | AggKind::MaxLargeInt
        | AggKind::MaxDecimal128
        | AggKind::MaxDecimal256 => &MAX,
        AggKind::AvgInt | AggKind::AvgFloat | AggKind::AvgDecimal128 | AggKind::AvgDecimal256 => {
            &AVG
        }
        AggKind::ArrayAgg { .. } | AggKind::ArrayUniqueAgg => &ARRAY_AGG,
        AggKind::VariancePop | AggKind::VarianceSamp | AggKind::StddevPop | AggKind::StddevSamp => {
            &VAR_STD
        }
        AggKind::AnyValue => &ANY_VALUE,
        AggKind::BoolOr => &BOOL_OR,
        AggKind::BoolAnd => &BOOL_AND,
        AggKind::CovarPop | AggKind::CovarSamp | AggKind::Corr => &COVAR_CORR,
        AggKind::MaxBy | AggKind::MinBy | AggKind::MaxByV2 | AggKind::MinByV2 => &MAX_MIN_BY,
        AggKind::MultiDistinctSum => &MULTI_DISTINCT_SUM,
        AggKind::MapAgg => &MAP_AGG,
        AggKind::SumMap => &SUM_MAP,
        AggKind::MannWhitneyUTest => &MANN_WHITNEY,
        AggKind::DictMerge => &DICT_MERGE,
        AggKind::DsHllHash | AggKind::DsHllMerge | AggKind::DsHllCount => &DS_HLL,
        AggKind::BitmapAgg => &BITMAP_UNION_INT,
        AggKind::BitmapUnionInt => &BITMAP_UNION_INT,
        AggKind::PercentileUnion
        | AggKind::PercentileApprox
        | AggKind::PercentileApproxWeighted => &PERCENTILE,
        AggKind::PercentileCont | AggKind::PercentileDisc | AggKind::PercentileDiscLc => {
            &PERCENTILE_PLACEHOLDER
        }
        AggKind::ApproxTopK => &APPROX_TOP_K,
        AggKind::HllRawHash | AggKind::HllRawMerge | AggKind::HllUnionCount => &HLL_RAW,
        AggKind::MinN | AggKind::MaxN => &MIN_MAX_N,
    }
}

#[cfg(test)]
fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

#[cfg(test)]
pub(super) fn build_spec_from_type(
    func: &AggFunction,
    input_type: Option<&DataType>,
    input_is_intermediate: bool,
) -> Result<AggSpec, String> {
    resolve_by_func(func)?.build_spec_from_type(func, input_type, input_is_intermediate)
}

#[cfg(test)]
pub(in crate::exec::expr::agg) fn state_layout_for_kind(kind: &AggKind) -> (usize, usize) {
    resolve_by_kind(kind).state_layout_for(kind)
}

// Legacy aggregate unit tests exercise individual state implementations
// without composing a process function set. Keep this bypass test-only so no
// production path can evade startup sealing or prepared-kernel binding.
#[cfg(test)]
pub(in crate::exec::expr::agg) fn build_input_view<'a>(
    spec: &AggSpec,
    array: &'a Option<ArrayRef>,
) -> Result<AggInputView<'a>, String> {
    resolve_by_kind(&spec.kind).build_input_view(spec, array)
}

#[cfg(test)]
pub(in crate::exec::expr::agg) fn build_merge_view<'a>(
    spec: &AggSpec,
    array: &'a Option<ArrayRef>,
) -> Result<AggInputView<'a>, String> {
    resolve_by_kind(&spec.kind).build_merge_view(spec, array)
}

#[cfg(test)]
pub(in crate::exec::expr::agg) fn init_state(spec: &AggSpec, ptr: *mut u8) {
    resolve_by_kind(&spec.kind).init_state(spec, ptr)
}

#[cfg(test)]
pub(in crate::exec::expr::agg) fn drop_state(spec: &AggSpec, ptr: *mut u8) {
    resolve_by_kind(&spec.kind).drop_state(spec, ptr)
}

#[cfg(test)]
pub(in crate::exec::expr::agg) fn update_batch(
    spec: &AggSpec,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    resolve_by_kind(&spec.kind).update_batch(spec, offset, state_ptrs, input)
}

#[cfg(test)]
pub(in crate::exec::expr::agg) fn merge_batch(
    spec: &AggSpec,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    resolve_by_kind(&spec.kind).merge_batch(spec, offset, state_ptrs, input)
}

#[cfg(test)]
pub(in crate::exec::expr::agg) fn build_array(
    spec: &AggSpec,
    offset: usize,
    group_states: &[AggStatePtr],
    output_intermediate: bool,
) -> Result<ArrayRef, String> {
    resolve_by_kind(&spec.kind).build_array(spec, offset, group_states, output_intermediate)
}

#[cfg(test)]
mod registry_tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn builtin_implementation_set_is_unique_and_seals_against_sql_metadata() {
        let names = BUILTIN_AGGREGATE_IMPLEMENTATIONS
            .iter()
            .map(|implementation| implementation.canonical_name)
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), BUILTIN_AGGREGATE_IMPLEMENTATIONS.len());
        assert_eq!(
            names.len(),
            85,
            "update the executable-policy matrix deliberately"
        );

        let mut builder = ExecutionFunctionSetBuilder::new();
        novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
            .unwrap();
        contribute_builtin_aggregate_implementations(&mut builder).unwrap();
        builder.seal().unwrap();
    }

    #[test]
    fn builtin_implementation_without_metadata_is_not_silently_skipped() {
        let mut builder = ExecutionFunctionSetBuilder::new();
        assert!(matches!(
            contribute_builtin_aggregate_implementations(&mut builder),
            Err(
                ExecutionFunctionSetError::BuiltinAggregateImplementationWithoutMetadata {
                    canonical_name
                }
            ) if canonical_name.as_ref() == "count"
        ));
    }
}
