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
use novarocks_functions::ResolvedAggregateSignature;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprId;
use crate::exec::node::ExecNode;
use novarocks_types::SlotId;

#[derive(Clone, Debug)]
pub enum WindowType {
    Rows,
    Range,
}

#[derive(Clone, Debug)]
pub enum WindowBoundary {
    CurrentRow,
    Preceding(i64),
    Following(i64),
}

#[derive(Clone, Debug)]
pub struct WindowFrame {
    /// Absence means UNBOUNDED PRECEDING.
    pub start: Option<WindowBoundary>,
    /// Absence means UNBOUNDED FOLLOWING.
    pub end: Option<WindowBoundary>,
    pub window_type: WindowType,
}

#[derive(Clone, Debug)]
pub enum WindowFunctionKind {
    RowNumber,
    Rank,
    DenseRank,
    CumeDist,
    PercentRank,
    Ntile,
    FirstValue {
        ignore_nulls: bool,
    },
    FirstValueRewrite {
        ignore_nulls: bool,
    },
    LastValue {
        ignore_nulls: bool,
    },
    Lead {
        ignore_nulls: bool,
    },
    Lag {
        ignore_nulls: bool,
    },
    SessionNumber,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    BitmapUnion,
    BitmapUnionCount,
    MaxBy,
    MinBy,
    VarianceSamp,
    StddevSamp,
    BoolOr,
    CovarPop,
    CovarSamp,
    Corr,
    ArrayAgg {
        is_distinct: bool,
        is_asc_order: Vec<bool>,
        nulls_first: Vec<bool>,
    },
    ApproxTopK,
}

#[derive(Clone, Debug)]
pub struct WindowFunctionSpec {
    pub kind: WindowFunctionKind,
    /// Physical expressions evaluated by the analytic operator. These may be
    /// packed (for example max_by(value, key) becomes one Struct expression)
    /// while the exact binding freezes the unpacked executable update channel
    /// types, including function ORDER BY keys.
    pub args: Vec<ExprId>,
    pub return_type: DataType,
    /// Exact ordinary aggregate identity selected by FE analysis. Window-only
    /// functions carry `None`; every aggregate-as-window function carries the
    /// executable canonical name plus its exact update signature.
    pub aggregate_binding: Option<WindowAggregateBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowAggregateBinding {
    pub function_name: String,
    pub resolved: ResolvedAggregateSignature,
}

#[derive(Clone, Debug)]
pub enum AnalyticOutputColumn {
    InputSlotId(SlotId),
    Window(usize),
}

#[derive(Clone, Debug)]
pub struct AnalyticNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub partition_exprs: Vec<ExprId>,
    pub order_by_exprs: Vec<ExprId>,
    pub functions: Vec<WindowFunctionSpec>,
    pub window: Option<WindowFrame>,
    /// Output column plan in final physical column order.
    pub output_columns: Vec<AnalyticOutputColumn>,
    pub output_chunk_schema: ChunkSchemaRef,
}
