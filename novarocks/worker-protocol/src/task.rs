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

//! Pure Task facts. Logical conclusion never stands in for physical exit.
use crate::convergence::TaskConvergence;
use crate::{
    creation::PayloadId,
    proposal_summary::{ConclusionState, Diagnostics, ProposalSummary},
};
use novarocks_control_runtime_core::identity::{Generation, ReservationId};
use novarocks_execution_contract::{
    DynamicFilterAdvertisement, OperatorStatistics, TaskResourceFacts, TaskStatus, TaskWriterFacts,
};
use novarocks_execution_contract::{
    OperationOutcome, TaskIdentity, TaskOperationId, TaskOutputFacts, TaskState,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreationPhase {
    Loading,
    Accepted,
    Rejected,
    Retained,
}
#[derive(Debug)]
pub struct TaskRecord {
    pub identity: TaskIdentity,
    pub phase: CreationPhase,
    pub payload: PayloadId,
    pub active_reservation: ReservationId,
    pub preparation_reservation: Option<ReservationId>,
    pub waiting_replies: std::collections::BTreeSet<TaskOperationId>,
    pub pending_reply_outcome: Option<OperationOutcome>,
    pub state: TaskState,
    pub version: u64,
    pub installed: bool,
    pub published: TaskStatus,
    pub resources: TaskResourceFacts,
    pub writer: Option<TaskWriterFacts>,
    pub filter_advertisement: Option<DynamicFilterAdvertisement>,
    pub operator_statistics: Vec<OperatorStatistics>,
    pub operator_statistics_truncated: bool,
    pub loader_exited: bool,
    pub preparation_body_exited: bool,
    pub preparation_exited: bool,
    pub actual_stopped: bool,
    pub runtime_actual_stopped_received: bool,
    pub runtime_resources_converged_received: bool,
    pub preparation_exit_holds: [bool; 4],
    pub output_released: bool,
    pub output: TaskOutputFacts,
    pub convergence: TaskConvergence,
    pub resources_converged: bool,
    pub root_idle: bool,
    pub applied_close_generation: Generation,
    /// A scheduled protocol cut, not an additional Work/exit authority.
    pub scheduled_close_generation: Option<Generation>,
    pub conclusion: ConclusionState,
    pub diagnostics: Diagnostics,
    pub proposals: ProposalSummary,
}
impl TaskRecord {
    pub fn accepted(&self) -> bool {
        self.pending_reply_outcome == Some(OperationOutcome::Accepted)
    }

    pub fn loading(
        identity: TaskIdentity,
        payload: PayloadId,
        reservation: ReservationId,
        request: TaskOperationId,
        generation: Generation,
    ) -> Self {
        Self {
            identity,
            phase: CreationPhase::Loading,
            payload,
            active_reservation: reservation,
            preparation_reservation: None,
            waiting_replies: [request].into_iter().collect(),
            pending_reply_outcome: None,
            state: TaskState::Planned,
            version: 1,
            installed: false,
            published: TaskStatus::created(identity),
            resources: TaskResourceFacts::empty(),
            writer: None,
            filter_advertisement: None,
            operator_statistics: Vec::new(),
            operator_statistics_truncated: false,
            loader_exited: false,
            preparation_body_exited: false,
            preparation_exited: false,
            actual_stopped: false,
            runtime_actual_stopped_received: false,
            runtime_resources_converged_received: false,
            preparation_exit_holds: [false; 4],
            output_released: false,
            output: TaskOutputFacts::new(false),
            convergence: TaskConvergence::new(),
            resources_converged: false,
            root_idle: false,
            applied_close_generation: generation,
            scheduled_close_generation: None,
            conclusion: ConclusionState::default(),
            diagnostics: Diagnostics::default(),
            proposals: ProposalSummary::default(),
        }
    }
    pub fn physical_exit_ready(&self) -> bool {
        self.loader_exited
            && self.preparation_exited
            && self.actual_stopped
            && self.output_released
            && self.resources_converged
            && self.root_idle
    }
    pub fn retirement_ready(&self) -> bool {
        (self.state.is_terminal() || self.phase == CreationPhase::Rejected)
            && self.physical_exit_ready()
            && self.waiting_replies.is_empty()
    }
    pub fn bump_version(&mut self) {
        self.version = self.version.checked_add(1).expect("task version exhausted");
    }
}
