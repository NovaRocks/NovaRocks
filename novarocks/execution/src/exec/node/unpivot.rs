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
use novarocks_types::SlotId;

use super::ExecNode;

#[derive(Clone, Debug)]
pub struct UnpivotNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub passthrough_columns: Vec<UnpivotPassthroughColumn>,
    pub value_output_slot_id: SlotId,
    pub literal_output_slot_ids: Vec<SlotId>,
    pub value_mappings: Vec<UnpivotValueMapping>,
    pub output_chunk_schema: ChunkSchemaRef,
    pub max_output_rows: usize,
    pub max_output_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct UnpivotPassthroughColumn {
    pub input_slot_id: SlotId,
    pub output_slot_id: SlotId,
}

#[derive(Clone, Debug)]
pub struct UnpivotValueMapping {
    pub input_value_slot_id: SlotId,
    pub constants: Vec<UnpivotConstant>,
}

#[derive(Clone, Debug)]
pub enum UnpivotConstant {
    Scalar { expr_id: ExprId, nullable: bool },
    Int32List(Vec<i32>),
    Utf8Map(Vec<(String, String)>),
}
