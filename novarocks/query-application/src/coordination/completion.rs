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

use novarocks_execution_contract::{TaskState, TaskStatus};

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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RootReadFacts {
    root_state: TaskState,
    root_eof_observed: bool,
    query_failure_latched: bool,
}

impl RootReadFacts {
    pub const fn new(
        root_state: TaskState,
        root_eof_observed: bool,
        query_failure_latched: bool,
    ) -> Self {
        Self {
            root_state,
            root_eof_observed,
            query_failure_latched,
        }
    }

    pub const fn client_visible_completion(self) -> bool {
        matches!(self.root_state, TaskState::Finished)
            && self.root_eof_observed
            && !self.query_failure_latched
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AttemptDrainFacts {
    all_tasks_terminal: bool,
    all_output_released: bool,
    all_contexts_released: bool,
}

impl AttemptDrainFacts {
    pub const fn new(tasks: bool, output: bool, contexts: bool) -> Self {
        Self {
            all_tasks_terminal: tasks,
            all_output_released: output,
            all_contexts_released: contexts,
        }
    }

    pub const fn drained(self) -> bool {
        self.all_tasks_terminal && self.all_output_released && self.all_contexts_released
    }
    pub const fn all_tasks_terminal(self) -> bool {
        self.all_tasks_terminal
    }
    pub const fn all_output_released(self) -> bool {
        self.all_output_released
    }
    pub const fn all_contexts_released(self) -> bool {
        self.all_contexts_released
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WriteCompletionFacts {
    all_writers_finished: bool,
    root_finish_task_finished: bool,
    prepared_write_set_complete: bool,
    external_commit_succeeded: bool,
    any_writer_canceled: bool,
}

impl WriteCompletionFacts {
    pub const fn new(
        writers: bool,
        root: bool,
        prepared: bool,
        committed: bool,
        canceled: bool,
    ) -> Self {
        Self {
            all_writers_finished: writers,
            root_finish_task_finished: root,
            prepared_write_set_complete: prepared,
            external_commit_succeeded: committed,
            any_writer_canceled: canceled,
        }
    }

    pub const fn client_visible_completion(self) -> bool {
        !self.any_writer_canceled
            && self.all_writers_finished
            && self.root_finish_task_finished
            && self.prepared_write_set_complete
            && self.external_commit_succeeded
    }
}

pub fn terminals_are_success_compatible<'a>(
    statuses: impl IntoIterator<Item = &'a TaskStatus>,
) -> bool {
    statuses
        .into_iter()
        .all(TaskStatus::is_success_compatible_terminal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_completion_does_not_wait_for_attempt_drain() {
        assert!(RootReadFacts::new(TaskState::Finished, true, false).client_visible_completion());
        assert!(!AttemptDrainFacts::new(true, false, false).drained());
    }

    #[test]
    fn a_cancelled_writer_cannot_be_write_success() {
        assert!(
            !WriteCompletionFacts::new(true, true, true, true, true).client_visible_completion()
        );
    }
}
