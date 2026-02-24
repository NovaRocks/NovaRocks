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
use crate::exec::expr::ExprId;
use crate::exec::node::ExecNode;

#[derive(Clone, Debug)]
pub struct SortExpression {
    pub expr: ExprId,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SortTopNType {
    // Keep exactly `offset + limit` rows in sorted order.
    RowNumber,
    // Keep rows whose SQL `RANK()` is within `limit`.
    // Rows tied at the boundary are included together.
    Rank,
    // Keep rows whose SQL `DENSE_RANK()` is within `limit`.
    // Rows tied at the boundary are included together.
    // Note: current StarRocks FE ranking-window pushdown does not emit
    // `TTopNType::DENSE_RANK` yet; we keep this variant for compatibility
    // and for direct-plan/engine-side coverage.
    DenseRank,
}

#[derive(Clone, Debug)]
pub struct SortNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub use_top_n: bool,
    pub order_by: Vec<SortExpression>,
    pub limit: Option<usize>,
    pub offset: usize,
    // Ranking semantics for topn boundary handling.
    pub topn_type: SortTopNType,
    pub max_buffered_rows: Option<usize>,
    pub max_buffered_bytes: Option<usize>,
}
