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

//! Assemble one subscription request from the original applied Task facts.

use super::QueryTaskExecution;
use crate::{
    capacity::StepContext,
    error::TaskExecutionError,
    observation::{QuiesceCursor, SubscriptionRequest},
};
use novarocks_execution_contract::{
    ParticipationConvergenceCursor, ParticipationRef, TaskConvergenceCursor, TaskIdentity,
    TaskStatusCursor,
};
use std::num::NonZeroU64;

#[derive(Debug)]
pub(crate) struct SubscriptionRequestBuild {
    participation: ParticipationRef,
    generation: NonZeroU64,
    status: Vec<TaskStatusCursor>,
    convergence: Vec<TaskConvergenceCursor>,
    identities: Vec<TaskIdentity>,
}

impl SubscriptionRequestBuild {
    pub(crate) fn new(
        execution: &QueryTaskExecution,
        participation: ParticipationRef,
        generation: NonZeroU64,
    ) -> Result<Self, TaskExecutionError> {
        let count = execution
            .tasks_by_participation
            .get(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?
            .len();
        Ok(Self {
            participation,
            generation,
            status: Vec::with_capacity(count),
            convergence: Vec::with_capacity(count),
            identities: Vec::with_capacity(count),
        })
    }

    /// The parent retains the original protocol phase while assembly yields.
    /// Rows use the frozen participation-local Task order.
    pub(crate) fn advance(
        &mut self,
        execution: &QueryTaskExecution,
        context: &mut dyn StepContext,
    ) -> Result<Option<SubscriptionRequest>, TaskExecutionError> {
        let tasks = &execution.tasks_by_participation[&self.participation];
        let state = &execution.covered_observation[&self.participation];
        while let Some(&identity) = tasks.get(self.identities.len()) {
            if !context.charge(1) {
                return Ok(None);
            }
            let task = execution
                .task_by_identity_ref(identity)
                .ok_or(TaskExecutionError::UnknownOperation)?;
            self.status.push(task.cursor());
            self.convergence
                .push(state.actual_stopped.get(&identity).map_or_else(
                    || TaskConvergenceCursor::unobserved(identity),
                    |receipt| TaskConvergenceCursor::at(identity, receipt.version()),
                ));
            self.identities.push(identity);
        }
        Ok(Some(SubscriptionRequest {
            participation: self.participation,
            generation: self.generation,
            status_cursors: std::mem::take(&mut self.status).into_boxed_slice(),
            task_convergence_cursors: std::mem::take(&mut self.convergence).into_boxed_slice(),
            required_identities: std::mem::take(&mut self.identities).into_boxed_slice(),
            participation_cursor: state.participation_convergence.map(|receipt| {
                ParticipationConvergenceCursor::at(self.participation, receipt.version())
            }),
            quiesce_cursor: state.quiesce.as_ref().map(|receipt| QuiesceCursor {
                participation: self.participation,
                fence_version: NonZeroU64::new(receipt.fence_version()),
            }),
        }))
    }
}
