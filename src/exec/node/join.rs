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
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprId;
use crate::exec::node::ExecNode;
use crate::types;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    NullAwareLeftAnti,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum JoinDistributionMode {
    Broadcast,
    Partitioned,
}

#[derive(Clone, Debug)]
pub struct JoinRuntimeFilterSpec {
    pub filter_id: i32,
    pub expr_order: usize,
    pub probe_slot_id: SlotId,
    pub merge_nodes: Vec<types::TNetworkAddress>,
    pub has_remote_targets: bool,
}

#[derive(Clone, Debug)]
pub struct JoinNode {
    pub left: Box<ExecNode>,
    pub right: Box<ExecNode>,
    pub node_id: i32,
    pub join_type: JoinType,
    pub distribution_mode: JoinDistributionMode,
    pub left_chunk_schema: ChunkSchemaRef,
    pub right_chunk_schema: ChunkSchemaRef,
    pub join_scope_chunk_schema: ChunkSchemaRef,
    pub probe_keys: Vec<ExprId>,
    pub build_keys: Vec<ExprId>,
    /// Null-safe flags aligned with join key pairs from FE eq_join_conjuncts.
    /// `true` means this key uses null-safe equality (`<=>` / EQ_FOR_NULL).
    pub eq_null_safe: Vec<bool>,
    pub residual_predicate: Option<ExprId>,
    pub runtime_filters: Vec<JoinRuntimeFilterSpec>,
}

impl JoinNode {
    pub fn left_schema(&self) -> arrow::datatypes::SchemaRef {
        self.left_chunk_schema.arrow_schema_ref()
    }

    pub fn right_schema(&self) -> arrow::datatypes::SchemaRef {
        self.right_chunk_schema.arrow_schema_ref()
    }

    pub fn join_scope_schema(&self) -> arrow::datatypes::SchemaRef {
        self.join_scope_chunk_schema.arrow_schema_ref()
    }
}
