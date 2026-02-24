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
use std::collections::HashMap;

use crate::common::ids::SlotId;
use crate::exec::row_position::RowPositionDescriptor;

#[derive(Clone, Debug)]
pub struct LookUpNode {
    pub node_id: i32,
    pub row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    pub output_slots: Vec<SlotId>,
    pub output_slots_by_tuple: HashMap<i32, Vec<SlotId>>,
}

impl LookUpNode {
    pub fn output_slots(&self) -> &[SlotId] {
        &self.output_slots
    }
}
