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

use std::fmt;

use novarocks_execution_contract::TaskState;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum StageState {
    Running,
    Flushing,
    Finished,
    Canceled,
    Aborting,
    Aborted,
    Failed,
}

impl StageState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Flushing => "FLUSHING",
            Self::Finished => "FINISHED",
            Self::Canceled => "CANCELED",
            Self::Aborting => "ABORTING",
            Self::Aborted => "ABORTED",
            Self::Failed => "FAILED",
        }
    }
}

impl fmt::Display for StageState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn derive_stage_state(scheduling_complete: bool, tasks: &[TaskState]) -> Option<StageState> {
    if tasks.is_empty() {
        return None;
    }
    if tasks.contains(&TaskState::Failed) {
        return Some(StageState::Failed);
    }
    if tasks.contains(&TaskState::Aborting) {
        return Some(StageState::Aborting);
    }
    if tasks.contains(&TaskState::Aborted) {
        return Some(if tasks.iter().all(|state| state.is_terminal()) {
            StageState::Aborted
        } else {
            StageState::Aborting
        });
    }
    if !scheduling_complete {
        return Some(StageState::Running);
    }
    if tasks.iter().any(|state| {
        matches!(
            state,
            TaskState::Planned | TaskState::Running | TaskState::Failing | TaskState::Canceling
        )
    }) {
        return Some(StageState::Running);
    }
    if tasks.iter().all(|state| *state == TaskState::Finished) {
        return Some(StageState::Finished);
    }
    if tasks.iter().all(|state| state.is_terminal()) && tasks.contains(&TaskState::Canceled) {
        return Some(StageState::Canceled);
    }
    if tasks.iter().all(|state| {
        matches!(
            state,
            TaskState::Flushing | TaskState::Finished | TaskState::Canceled
        )
    }) && tasks.contains(&TaskState::Flushing)
    {
        return Some(StageState::Flushing);
    }
    Some(StageState::Running)
}

pub const fn parent_released_children(parent: StageState) -> bool {
    matches!(
        parent,
        StageState::Flushing
            | StageState::Finished
            | StageState::Canceled
            | StageState::Aborted
            | StageState::Failed
    )
}
