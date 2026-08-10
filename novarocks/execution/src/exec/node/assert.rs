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
use crate::exec::node::ExecNode;

use novarocks_types::SlotId;

#[derive(Clone, Debug)]
pub enum Assertion {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug)]
pub enum AssertNumRowsMode {
    Global {
        desired_num_rows: Option<usize>,
        assertion: Assertion,
        subquery_string: Option<String>,
    },
    PerKeyAtMostOne {
        // The planner must hash-partition by these slots before this assert for global per-key semantics.
        key_slots: Vec<SlotId>,
        key_labels: Vec<String>,
        message_prefix: String,
    },
}

#[derive(Clone, Debug)]
pub struct AssertNumRowsNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub mode: AssertNumRowsMode,
}
