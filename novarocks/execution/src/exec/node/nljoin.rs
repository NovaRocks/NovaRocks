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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum NestedLoopJoinType {
    Inner,
    Cross,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    LeftAnti,
    NullAwareLeftAnti,
}

#[derive(Clone, Debug)]
pub struct NestedLoopJoinNode {
    pub left: Box<ExecNode>,
    pub right: Box<ExecNode>,
    pub node_id: i32,
    pub join_type: NestedLoopJoinType,
    pub join_conjunct: Option<ExprId>,
    pub left_chunk_schema: ChunkSchemaRef,
    pub right_chunk_schema: ChunkSchemaRef,
    pub join_scope_chunk_schema: ChunkSchemaRef,
}

impl NestedLoopJoinNode {
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
