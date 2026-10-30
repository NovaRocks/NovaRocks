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

//! Attempt-local input pumps owned by the single Task protocol driver.

use std::sync::Arc;

use super::feedback_pump::{DynamicFilterFeedbackPump, TaskDynamicFilterReads};
use super::round::TaskRound;
use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;

/// The original attempt-local sources for runtime-filter feedback.
///
/// It is one struct rather than seven parameters so the one call site cannot
/// silently drop a fact by reordering: every field here is a loop's only
/// source, and a loop with a missing source is a loop that never runs.
pub(crate) struct AttemptPumps {
    pub(crate) feedback_state: Arc<RuntimeFilterFeedbackState>,
    /// How many feedback channels the sealed filter deployment declared.
    pub(crate) declared_feedback_channels: usize,
    pub(crate) reads: Arc<dyn TaskDynamicFilterReads>,
}

/// Installs one attempt's per-turn owners and declares the set complete.
///
/// One loop now, not two. The credential rotation owner used to be installed
/// here and returned so the caller could stop it during the drain; a consumer
/// that renews for itself has nothing for a coordinator turn to drive, so
/// there is nothing to stop (CAD-1 D3).
pub(crate) fn install_attempt_pumps(round: &mut TaskRound, pumps: AttemptPumps) {
    // The dynamic filter reader closes the loop the connector split sources
    // are already waiting on. Without it every channel stays pending, each
    // source waits out its own initial cap and then enumerates unpruned --
    // which changes pruning and latency but no rows, so no result assertion
    // anywhere can see its absence.
    if let Some(pump) = DynamicFilterFeedbackPump::new(
        pumps.feedback_state,
        pumps.reads,
        pumps.declared_feedback_channels,
    ) {
        round.add_pump(Box::new(pump));
    }
    round.seal_pumps();
}
