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
use crate::common::ids::SlotId;
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

#[derive(Clone, Debug)]
pub struct AggFunction {
    /// Lowercased function name from FE (e.g. "sum", "count").
    pub name: String,
    /// Input expressions for aggregate arguments; empty means COUNT(*)-style aggregate.
    pub inputs: Vec<ExprId>,
    /// Whether this aggregate consumes intermediate states from a previous aggregation stage.
    /// This corresponds to StarRocks FE's `is_merge_agg`.
    pub input_is_intermediate: bool,
    pub types: Option<AggTypeSignature>,
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
    pub output_slots: Vec<SlotId>,
}
