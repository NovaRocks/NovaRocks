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

//! Shared state specialization for INTERSECT set-operation execution.
//!
//! Responsibilities:
//! - Defines INTERSECT-specific probe/update/output marker semantics.
//! - Reuses generic distinct set-operation shared-state mechanics.
//!
//! Key exported interfaces:
//! - Types: `IntersectSharedState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use super::distinct_set_shared::{DistinctSetSemantics, DistinctSetSharedState};

#[derive(Clone)]
pub struct IntersectSemantics;

impl DistinctSetSemantics for IntersectSemantics {
    type Marker = u16;

    const NODE_NAME: &'static str = "INTERSECT_NODE";
    const GROUP_ID_SEQUENCE_MISMATCH: &'static str = "intersect group id sequence mismatch";
    const GROUP_ID_OUT_OF_BOUNDS: &'static str = "intersect hit_times group id out of bounds";
    const SINK_OPERATOR_NAME: &'static str = "IntersectSink";
    const SOURCE_OPERATOR_NAME: &'static str = "IntersectSource";
    const SOURCE_REJECT_INPUT_ERROR: &'static str =
        "intersect source operator does not accept input";
    const SOURCE_SLOT_MISMATCH_PREFIX: &'static str = "intersect output slot mismatch";

    fn new_marker() -> Self::Marker {
        0
    }

    fn apply_probe(marker: &mut Self::Marker, stage: usize) -> Result<(), String> {
        let stage_u16 = u16::try_from(stage).map_err(|_| "intersect stage overflow".to_string())?;
        let prev_u16 = u16::try_from(stage.saturating_sub(1))
            .map_err(|_| "intersect stage overflow".to_string())?;
        if *marker == prev_u16 {
            *marker = stage_u16;
        }
        Ok(())
    }

    fn marker_selected(marker: &Self::Marker, stage_total: usize) -> Result<bool, String> {
        let final_hit = u16::try_from(stage_total.saturating_sub(1))
            .map_err(|_| "intersect stage overflow".to_string())?;
        Ok(*marker == final_hit)
    }
}

pub(crate) type IntersectSharedState = DistinctSetSharedState<IntersectSemantics>;
