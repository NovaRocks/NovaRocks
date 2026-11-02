// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Worker-specific projection of the shared conclusion author.
use crate::proposal_summary::{ConclusionState, Diagnostics, ProposalSummary, SourceId};
use novarocks_execution_contract::{TaskConclusion, TaskState, TerminationDetail};

pub struct ConclusionPublication {
    pub changed: bool,
    pub generation: u64,
    pub conclusion: TaskConclusion,
    pub source: Option<SourceId>,
    pub released: Vec<SourceId>,
}
pub fn adjudicate(
    summary: ProposalSummary,
    current: &mut ConclusionState,
    diagnostics: &mut Diagnostics,
) -> Option<ConclusionPublication> {
    let previous_generation = current.generation;
    let result = summary.apply(current.clone(), diagnostics.clone());
    *current = result.state;
    *diagnostics = result.diagnostics;
    if current.generation != previous_generation {
        Some(ConclusionPublication {
            changed: true,
            generation: current.generation,
            conclusion: current
                .accepted
                .clone()
                .expect("accepted generation has a conclusion"),
            source: current.source,
            released: result.released,
        })
    } else if !result.released.is_empty() {
        // Source disposal is also an ordered runtime effect even when the
        // accepted conclusion did not change.
        current
            .accepted
            .clone()
            .map(|conclusion| ConclusionPublication {
                changed: false,
                generation: current.generation,
                conclusion,
                source: current.source,
                released: result.released,
            })
    } else {
        None
    }
}
pub const fn terminating_state(conclusion: &TaskConclusion) -> Option<TaskState> {
    match conclusion {
        TaskConclusion::Succeeded => None,
        TaskConclusion::Terminated(TerminationDetail::Failed(_)) => Some(TaskState::Failing),
        TaskConclusion::Terminated(TerminationDetail::Canceled(_)) => Some(TaskState::Canceling),
        TaskConclusion::Terminated(TerminationDetail::Aborted(_)) => Some(TaskState::Aborting),
    }
}
pub const fn terminal_state(conclusion: &TaskConclusion) -> TaskState {
    match conclusion {
        TaskConclusion::Succeeded => TaskState::Finished,
        TaskConclusion::Terminated(TerminationDetail::Failed(_)) => TaskState::Failed,
        TaskConclusion::Terminated(TerminationDetail::Canceled(_)) => TaskState::Canceled,
        TaskConclusion::Terminated(TerminationDetail::Aborted(_)) => TaskState::Aborted,
    }
}
