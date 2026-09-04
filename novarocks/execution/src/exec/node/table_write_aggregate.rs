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

//! Immutable ordinary aggregate facts embedded in the write dataflow.
//!
//! These values describe only engine aggregate bindings, slots, a grouping
//! key, and generic Unpivot constants. They deliberately carry no Connector
//! descriptor or artifact-format meaning.

use std::sync::Arc;

use novarocks_functions::ResolvedAggregateSignature;
use novarocks_types::SlotId;

use super::unpivot::UnpivotConstant;

#[derive(Clone, Debug)]
pub struct WriterPartialAggregateCall {
    pub input_slot_id: SlotId,
    pub function_name: Arc<str>,
    pub resolved: ResolvedAggregateSignature,
    pub intermediate_slot_id: SlotId,
}

#[derive(Clone, Debug, Default)]
pub struct WriterPartialAggregatePlan {
    pub calls: Vec<WriterPartialAggregateCall>,
}

#[derive(Clone, Debug)]
pub struct WriterFinalAggregateCall {
    pub function_name: Arc<str>,
    pub resolved: ResolvedAggregateSignature,
    pub intermediate_input_slot_id: SlotId,
    pub final_output_slot_id: SlotId,
}

#[derive(Clone, Debug)]
pub struct WriterGroupedUnpivotMapping {
    pub grouping_key: u32,
    pub input_value_slot_id: SlotId,
    pub constants: Vec<UnpivotConstant>,
}

#[derive(Clone, Debug)]
pub struct WriterGroupedUnpivotPlan {
    pub grouping_input_slot_id: SlotId,
    pub grouping_output_slot_id: SlotId,
    pub passthrough_output_slot_id: SlotId,
    pub value_output_slot_id: SlotId,
    pub literal_output_slot_ids: Vec<SlotId>,
    pub mappings: Vec<WriterGroupedUnpivotMapping>,
    pub max_output_rows: usize,
    pub max_output_bytes: usize,
}

#[derive(Clone, Debug, Default)]
pub struct WriterFinalAggregatePlan {
    pub calls: Vec<WriterFinalAggregateCall>,
    pub unpivot: Option<WriterGroupedUnpivotPlan>,
}
