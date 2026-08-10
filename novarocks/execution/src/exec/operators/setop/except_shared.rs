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

//! Shared state specialization for EXCEPT set-operation execution.
//!
//! Responsibilities:
//! - Defines EXCEPT-specific probe/update/output marker semantics.
//! - Reuses generic distinct set-operation shared-state mechanics.
//!
//! Key exported interfaces:
//! - Types: `ExceptSharedState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use super::distinct_set_shared::{DistinctSetSemantics, DistinctSetSharedState};

#[derive(Clone)]
pub struct ExceptSemantics;

impl DistinctSetSemantics for ExceptSemantics {
    type Marker = bool;

    const NODE_NAME: &'static str = "EXCEPT_NODE";
    const GROUP_ID_SEQUENCE_MISMATCH: &'static str = "except group id sequence mismatch";
    const GROUP_ID_OUT_OF_BOUNDS: &'static str = "except deleted flag group id out of bounds";
    const SINK_OPERATOR_NAME: &'static str = "ExceptSink";
    const SOURCE_OPERATOR_NAME: &'static str = "ExceptSource";
    const SOURCE_REJECT_INPUT_ERROR: &'static str = "except source operator does not accept input";
    const SOURCE_SLOT_MISMATCH_PREFIX: &'static str = "except output slot mismatch";

    fn new_marker() -> Self::Marker {
        false
    }

    fn apply_probe(marker: &mut Self::Marker, _stage: usize) -> Result<(), String> {
        *marker = true;
        Ok(())
    }

    fn marker_selected(marker: &Self::Marker, _stage_total: usize) -> Result<bool, String> {
        Ok(!*marker)
    }
}

pub(crate) type ExceptSharedState = DistinctSetSharedState<ExceptSemantics>;
