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

use crate::exec::node::aggregate::AggFunction;

use super::{AggInputView, AggSpec, AggStatePtr};

#[derive(Clone, Debug)]
pub(super) enum AggKind {
    Count,
    CountDistinct,
    CountDistinctNonNegative,
    CountIf,
    SumInt,
    SumLargeInt,
    SumFloat,
    SumDecimal128,
    SumDecimal256,
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
    ArrayAgg {
        is_distinct: bool,
        is_asc_order: Vec<bool>,
        nulls_first: Vec<bool>,
    },
    ArrayUniqueAgg,
    Retention,
    WindowFunnel,
    Histogram,
    MannWhitneyUTest,
    DictMerge,
    BitmapAgg,
    BitmapUnionInt,
    PercentilePlaceholder,
    ApproxTopK,
    HllRawHash,
    HllRawMerge,
    HllUnionCount,
}

mod any_value;
mod approx_top_k;
mod array_agg;
mod avg;
mod bitmap_union_int;
mod bool_or;
pub(crate) mod common;
mod corr_covar;
mod count;
mod count_distinct;
mod count_if;
mod dict_merge;
mod group_concat;
mod histogram;
mod hll_raw;
mod mann_whitney_u_test;
mod map_agg;
mod max;
mod max_by;
mod min;
mod multi_distinct_sum;
mod percentile_placeholder;
mod retention;
mod sum;
mod variance;
mod window_funnel;

use any_value::AnyValueAgg;
use approx_top_k::ApproxTopKAgg;
use array_agg::ArrayAggAgg;
use avg::AvgAgg;
use bitmap_union_int::BitmapUnionIntAgg;
use bool_or::BoolOrAgg;
use corr_covar::CovarCorrAgg;
use count::CountAgg;
use count_distinct::CountDistinctAgg;
use count_if::CountIfAgg;
use dict_merge::DictMergeAgg;
use group_concat::GroupConcatAgg;
use histogram::HistogramAgg;
use hll_raw::HllRawAgg;
use mann_whitney_u_test::MannWhitneyUTestAgg;
use map_agg::MapAggAgg;
use max::MaxAgg;
use max_by::MaxMinByAgg;
use min::MinAgg;
use multi_distinct_sum::MultiDistinctSumAgg;
use percentile_placeholder::PercentilePlaceholderAgg;
use retention::RetentionAgg;
use sum::SumAgg;
use variance::VarStdAgg;
use window_funnel::WindowFunnelAgg;

pub(super) trait AggregateFunction {
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
    fn drop_state(&self, spec: &AggSpec, ptr: *mut u8);

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
static COUNT_DISTINCT: CountDistinctAgg = CountDistinctAgg;
static COUNT_IF: CountIfAgg = CountIfAgg;
static GROUP_CONCAT: GroupConcatAgg = GroupConcatAgg;
static SUM: SumAgg = SumAgg;
static MIN: MinAgg = MinAgg;
static MAX: MaxAgg = MaxAgg;
static AVG: AvgAgg = AvgAgg;
static ARRAY_AGG: ArrayAggAgg = ArrayAggAgg;
static VAR_STD: VarStdAgg = VarStdAgg;
static ANY_VALUE: AnyValueAgg = AnyValueAgg;
static BOOL_OR: BoolOrAgg = BoolOrAgg;
static COVAR_CORR: CovarCorrAgg = CovarCorrAgg;
static MAX_MIN_BY: MaxMinByAgg = MaxMinByAgg;
static MULTI_DISTINCT_SUM: MultiDistinctSumAgg = MultiDistinctSumAgg;
static MAP_AGG: MapAggAgg = MapAggAgg;
static RETENTION: RetentionAgg = RetentionAgg;
static WINDOW_FUNNEL: WindowFunnelAgg = WindowFunnelAgg;
static HISTOGRAM: HistogramAgg = HistogramAgg;
static MANN_WHITNEY: MannWhitneyUTestAgg = MannWhitneyUTestAgg;
static DICT_MERGE: DictMergeAgg = DictMergeAgg;
static BITMAP_UNION_INT: BitmapUnionIntAgg = BitmapUnionIntAgg;
static PERCENTILE_PLACEHOLDER: PercentilePlaceholderAgg = PercentilePlaceholderAgg;
static APPROX_TOP_K: ApproxTopKAgg = ApproxTopKAgg;
static HLL_RAW: HllRawAgg = HllRawAgg;

fn resolve_by_func(func: &AggFunction) -> Result<&'static dyn AggregateFunction, String> {
    match canonical_agg_name(func.name.as_str()) {
        "count" => Ok(&COUNT),
        "count_distinct"
        | "multi_distinct_count"
        | "ds_theta_count_distinct"
        | "approx_count_distinct_hll_sketch" => Ok(&COUNT_DISTINCT),
        "count_if" => Ok(&COUNT_IF),
        "group_concat" | "string_agg" => Ok(&GROUP_CONCAT),
        "sum" => Ok(&SUM),
        "min" => Ok(&MIN),
        "max" => Ok(&MAX),
        "avg" => Ok(&AVG),
        "array_agg" | "array_agg_distinct" | "array_unique_agg" => Ok(&ARRAY_AGG),
        "variance" | "variance_pop" | "var_pop" | "variance_samp" | "var_samp" | "stddev"
        | "stddev_pop" | "stddev_samp" | "std" => Ok(&VAR_STD),
        "any_value" => Ok(&ANY_VALUE),
        "percentile_disc" | "percentile_cont" | "percentile_disc_lc" => Ok(&PERCENTILE_PLACEHOLDER),
        "bool_or" | "boolor_agg" => Ok(&BOOL_OR),
        "covar_pop" | "covar_samp" | "corr" => Ok(&COVAR_CORR),
        "max_by" | "min_by" | "max_by_v2" | "min_by_v2" => Ok(&MAX_MIN_BY),
        "multi_distinct_sum" => Ok(&MULTI_DISTINCT_SUM),
        "map_agg" => Ok(&MAP_AGG),
        "retention" => Ok(&RETENTION),
        "window_funnel" => Ok(&WINDOW_FUNNEL),
        "histogram" => Ok(&HISTOGRAM),
        "mann_whitney_u_test" => Ok(&MANN_WHITNEY),
        "dict_merge" => Ok(&DICT_MERGE),
        "bitmap_agg" | "bitmap_union" | "bitmap_union_count" => Ok(&BITMAP_UNION_INT),
        "bitmap_union_int" => Ok(&BITMAP_UNION_INT),
        "approx_top_k" => Ok(&APPROX_TOP_K),
        "hll_union"
        | "hll_raw_agg"
        | "hll_raw"
        | "hll_union_agg"
        | "ndv"
        | "approx_count_distinct"
        | "ds_hll_count_distinct"
        | "ds_hll_count_distinct_union"
        | "ds_hll_count_distinct_merge" => Ok(&HLL_RAW),
        other => Err(format!("unsupported agg function: {}", other)),
    }
}

fn resolve_by_kind(kind: &AggKind) -> &'static dyn AggregateFunction {
    match kind {
        AggKind::Count => &COUNT,
        AggKind::CountDistinct | AggKind::CountDistinctNonNegative => &COUNT_DISTINCT,
        AggKind::CountIf => &COUNT_IF,
        AggKind::GroupConcat { .. } => &GROUP_CONCAT,
        AggKind::SumInt
        | AggKind::SumLargeInt
        | AggKind::SumFloat
        | AggKind::SumDecimal128
        | AggKind::SumDecimal256 => &SUM,
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
        AggKind::CovarPop | AggKind::CovarSamp | AggKind::Corr => &COVAR_CORR,
        AggKind::MaxBy | AggKind::MinBy | AggKind::MaxByV2 | AggKind::MinByV2 => &MAX_MIN_BY,
        AggKind::MultiDistinctSum => &MULTI_DISTINCT_SUM,
        AggKind::MapAgg => &MAP_AGG,
        AggKind::Retention => &RETENTION,
        AggKind::WindowFunnel => &WINDOW_FUNNEL,
        AggKind::Histogram => &HISTOGRAM,
        AggKind::MannWhitneyUTest => &MANN_WHITNEY,
        AggKind::DictMerge => &DICT_MERGE,
        AggKind::BitmapAgg => &BITMAP_UNION_INT,
        AggKind::BitmapUnionInt => &BITMAP_UNION_INT,
        AggKind::PercentilePlaceholder => &PERCENTILE_PLACEHOLDER,
        AggKind::ApproxTopK => &APPROX_TOP_K,
        AggKind::HllRawHash | AggKind::HllRawMerge | AggKind::HllUnionCount => &HLL_RAW,
    }
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

pub(super) fn build_spec_from_type(
    func: &AggFunction,
    input_type: Option<&DataType>,
    input_is_intermediate: bool,
) -> Result<AggSpec, String> {
    resolve_by_func(func)?.build_spec_from_type(func, input_type, input_is_intermediate)
}

pub(in crate::exec::expr::agg) fn state_layout_for_kind(kind: &AggKind) -> (usize, usize) {
    resolve_by_kind(kind).state_layout_for(kind)
}

pub(in crate::exec::expr::agg) fn build_input_view<'a>(
    spec: &AggSpec,
    array: &'a Option<ArrayRef>,
) -> Result<AggInputView<'a>, String> {
    resolve_by_kind(&spec.kind).build_input_view(spec, array)
}

pub(in crate::exec::expr::agg) fn build_merge_view<'a>(
    spec: &AggSpec,
    array: &'a Option<ArrayRef>,
) -> Result<AggInputView<'a>, String> {
    resolve_by_kind(&spec.kind).build_merge_view(spec, array)
}

pub(in crate::exec::expr::agg) fn init_state(spec: &AggSpec, ptr: *mut u8) {
    resolve_by_kind(&spec.kind).init_state(spec, ptr)
}

pub(in crate::exec::expr::agg) fn drop_state(spec: &AggSpec, ptr: *mut u8) {
    resolve_by_kind(&spec.kind).drop_state(spec, ptr)
}

pub(in crate::exec::expr::agg) fn update_batch(
    spec: &AggSpec,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    resolve_by_kind(&spec.kind).update_batch(spec, offset, state_ptrs, input)
}

pub(in crate::exec::expr::agg) fn merge_batch(
    spec: &AggSpec,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    resolve_by_kind(&spec.kind).merge_batch(spec, offset, state_ptrs, input)
}

pub(in crate::exec::expr::agg) fn build_array(
    spec: &AggSpec,
    offset: usize,
    group_states: &[AggStatePtr],
    output_intermediate: bool,
) -> Result<ArrayRef, String> {
    resolve_by_kind(&spec.kind).build_array(spec, offset, group_states, output_intermediate)
}
