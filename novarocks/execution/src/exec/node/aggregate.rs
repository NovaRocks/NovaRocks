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
use std::num::NonZeroU32;

use crate::runtime_filter::RuntimeFilterProducerContract;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprId;
use crate::exec::node::ExecNode;
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
/// (array_agg / group_concat). Default (empty / false / None) means no ORDER BY
/// and not DISTINCT — the common case.
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
    pub order: AggOrderSpec,
}

#[derive(Clone, Debug)]
pub struct AggregateTopNRuntimeFilterProducerBinding {
    pub group_key_expr_id: ExprId,
    pub group_key_ordinal: usize,
    pub limit: NonZeroU32,
    pub contract: RuntimeFilterProducerContract,
}

#[derive(Clone, Debug)]
pub struct AggregateRuntimeFilterSpec {
    pub topn_producers: Vec<AggregateTopNRuntimeFilterProducerBinding>,
}

impl AggregateRuntimeFilterSpec {
    /// Compat fragments do not carry native runtime-filter producers.
    pub const fn empty() -> Self {
        Self {
            topn_producers: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.topn_producers.is_empty()
    }

    pub fn try_new(
        topn_producers: Vec<AggregateTopNRuntimeFilterProducerBinding>,
    ) -> Result<Self, String> {
        Ok(Self { topn_producers })
    }
}

impl AggregateTopNRuntimeFilterProducerBinding {
    pub const fn new(
        group_key_expr_id: ExprId,
        group_key_ordinal: usize,
        limit: NonZeroU32,
        contract: RuntimeFilterProducerContract,
    ) -> Self {
        Self {
            group_key_expr_id,
            group_key_ordinal,
            limit,
            contract,
        }
    }

    pub const fn contract(&self) -> &RuntimeFilterProducerContract {
        &self.contract
    }

    pub const fn binding_id(&self) -> u32 {
        self.contract.binding_id().get()
    }

    pub const fn channel_id(&self) -> u32 {
        self.contract.channel_id().get()
    }
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
    pub runtime_filter_spec: AggregateRuntimeFilterSpec,
    pub streaming_preaggregation_mode: Option<StreamingPreaggregationMode>,
}
