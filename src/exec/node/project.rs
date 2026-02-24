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

use super::ExecNode;

#[derive(Clone, Debug)]
pub struct ProjectNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    /// True for pipeline-internal projections inserted during lowering (e.g. layout fixes).
    /// These should be treated as subordinate operators in StarRocks FE profile parsing:
    /// FE groups operators by `plan_node_id` and derives plan-node `OutputRows` from a selected
    /// "main" operator's `CommonMetrics.PullRowNum`, so internal helper operators must be excluded
    /// via `CommonMetrics.IsSubordinate`.
    pub is_subordinate: bool,
    pub exprs: Vec<ExprId>,
    /// Slot ids for each expr in `exprs` (including CSE and outputs), in evaluation order.
    pub expr_slot_ids: Vec<SlotId>,
    /// If Some, only output these expr indices.
    pub output_indices: Option<Vec<usize>>,
    /// Slot ids for the physical output columns (matches post-output_indices order).
    pub output_slots: Vec<SlotId>,
}
