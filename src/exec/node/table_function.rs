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
use arrow::datatypes::{DataType, SchemaRef};

use crate::common::ids::SlotId;
use crate::exec::node::ExecNode;

#[derive(Clone, Debug)]
pub enum TableFunctionOutputSlot {
    Outer { slot: SlotId },
    Result { index: usize },
}

#[derive(Clone, Debug)]
pub struct TableFunctionNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub function_name: String,
    pub param_slots: Vec<SlotId>,
    pub outer_slots: Vec<SlotId>,
    pub fn_result_slots: Vec<SlotId>,
    pub fn_result_required: bool,
    pub is_left_join: bool,
    pub param_types: Vec<DataType>,
    pub ret_types: Vec<DataType>,
    pub output_schema: SchemaRef,
    pub output_slots: Vec<SlotId>,
    pub output_slot_sources: Vec<TableFunctionOutputSlot>,
}
