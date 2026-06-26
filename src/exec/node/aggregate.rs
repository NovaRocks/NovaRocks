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
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprId;
use crate::exec::node::ExecNode;
use crate::thrift::types::TPrimitiveType;
use arrow::datatypes::DataType;

#[derive(Clone, Debug)]
pub struct AggTypeSignature {
    pub intermediate_type: Option<DataType>,
    pub output_type: Option<DataType>,
    /// The FE-declared type of the first input argument (TFunction.arg_types[0]).
    /// StarRocks BE uses this scale for avg(decimal) (see ctx->get_arg_type(0)->scale).
    pub input_arg_type: Option<DataType>,
}

/// Structured ORDER BY / DISTINCT metadata for ordered aggregates
/// (array_agg / group_concat). Replaces the former function-name string encoding
/// (e.g. `array_agg|a=1,0|n=0,1`, `group_concat|d=1|a=..|n=..|m=1024`). Default
/// (empty / false / None) means no ORDER BY and not DISTINCT — the common case.
/// array_agg's DISTINCT stays folded into the base name `array_agg_distinct`;
/// `is_distinct` here carries group_concat's DISTINCT.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AggOrderSpec {
    /// ORDER BY direction per sort key (true = ASC).
    pub is_asc_order: Vec<bool>,
    /// NULLS FIRST per sort key (true = NULLS FIRST).
    pub nulls_first: Vec<bool>,
    /// group_concat DISTINCT (array_agg uses the `array_agg_distinct` base name).
    pub is_distinct: bool,
    /// group_concat max output length (group_concat_max_len); None otherwise.
    pub group_concat_max_len: Option<i64>,
}

#[derive(Clone, Debug, Default)]
pub struct AggFunction {
    /// Lowercased function name from FE (e.g. "sum", "count").
    pub name: String,
    /// Input expressions for aggregate arguments; empty means COUNT(*)-style aggregate.
    pub inputs: Vec<ExprId>,
    /// Whether this aggregate consumes intermediate states from a previous aggregation stage.
    /// This corresponds to StarRocks FE's `is_merge_agg`.
    pub input_is_intermediate: bool,
    pub types: Option<AggTypeSignature>,
    /// ORDER BY / DISTINCT metadata for ordered aggregates (array_agg / group_concat).
    /// Replaces the former function-name string encoding.
    pub order: AggOrderSpec,
}

/// Spec for a TopN runtime filter built by the AGG operator.
/// `expr_order` indexes into the group-by columns to select which column
/// to compute min/max from. The FE bug hardcodes this to 0.
#[derive(Clone, Debug)]
pub struct TopNRuntimeFilterSpec {
    pub filter_id: i32,
    pub expr_order: usize,
    /// The primitive type of the build expression (from FE).
    pub build_type: TPrimitiveType,
    /// The column name on the probe (scan) side that this filter targets.
    pub probe_column_name: String,
    /// TopN limit — filter is only published when group count >= limit.
    pub limit: usize,
    /// Sort direction of the TopN (from FE). true = ASC ⇒ upper-bound-only filter.
    pub is_asc: bool,
    /// Null ordering of the TopN (from FE); reserved for future null handling.
    #[allow(dead_code)]
    pub is_nulls_first: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamingPreaggregationMode {
    Auto,
    ForceStreaming,
    ForcePreaggregation,
    LimitedMem,
}

#[derive(Clone, Debug)]
pub struct AggregateNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub group_by: Vec<ExprId>,
    pub functions: Vec<AggFunction>,
    pub need_finalize: bool,
    /// True only when *all* functions in this node are merge-aggregates.
    /// Mixed merge/update aggregates are supported via per-function flags.
    pub input_is_intermediate: bool,
    pub output_chunk_schema: ChunkSchemaRef,
    pub topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
    pub streaming_preaggregation_mode: Option<StreamingPreaggregationMode>,
}
