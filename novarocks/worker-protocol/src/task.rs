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
    creation::{AdmissionRefusal, PayloadId},
    proposal_summary::{ConclusionState, Diagnostics, ProposalSummary},
};
use novarocks_control_runtime_core::identity::{Generation, InputId, ReservationId};
use novarocks_execution_contract::{
    DynamicFilterAdvertisement, OperatorStatistics, TaskResourceFacts, TaskStatus, TaskWriterFacts,
};
use novarocks_execution_contract::{OperationOutcome, TaskIdentity, TaskOutputFacts, TaskState};

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
    pub waiting_replies: std::collections::BTreeSet<InputId>,
    pub pending_reply_outcome: Option<OperationOutcome>,
    /// Original entity decision time, shared only by its actual Create waiters.
    pub creation_decided_at: Option<crate::lease::MonotonicInstant>,
    pub creation_reply: Option<std::sync::Arc<crate::context::ContextReplyBody>>,
    pub initial_domain_receipts:
        std::sync::Arc<Vec<novarocks_execution_contract::TaskDomainReceipt>>,
    pub creation_refusal: Option<AdmissionRefusal>,
    /// The original Context self-source observed physical rejection disposal;
    /// identity remains owned until its bounded neutral inventory drain ends.
    pub(crate) rejected_retirement_observed: bool,
    pub(crate) initial_creation: Option<crate::context::InitialCreation>,
    pub state: TaskState,
    pub version: u64,
    pub installed: bool,
    pub published: TaskStatus,
    pub resources: TaskResourceFacts,
    pub writer: Option<TaskWriterFacts>,
    pub filter_advertisement: Option<DynamicFilterAdvertisement>,
    pub operator_statistics: Vec<OperatorStatistics>,
    pub operator_statistics_truncated: bool,
    /// One original terminal observation seals final statistics against late samples.
    pub statistics_sealed: bool,
    pub terminal_observation_received: bool,
    pub normal_close_validated: bool,
    pub loader_exited: bool,
    pub preparation_body_exited: bool,
    pub preparation_exited: bool,
    pub actual_stopped: bool,
    pub runtime_actual_stopped_received: bool,
    pub runtime_resources_converged_received: bool,
    /// Successful production is an intent, not an accepted result. The
    /// original completion work must actually exit before this can become a
    /// success proposal, and output responsibility remains independent.
    pub completion_success_pending: bool,
    pub preparation_exit_holds: [bool; 4],
    pub output_released: bool,
    pub output: TaskOutputFacts,
    pub convergence: TaskConvergence,
    pub resources_converged: bool,
    pub root_idle: bool,
    pub root_transferred: bool,
    pub applied_close_generation: Generation,
    /// A scheduled protocol cut, not an additional Work/exit authority.
    pub scheduled_close_generation: Option<Generation>,
    /// Normal close is a separate once-only causal author, including after Abort starts.
    pub scheduled_quiesce_generation: Option<Generation>,
    pub applied_quiesce_generation: Option<Generation>,
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
        request: InputId,
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
            creation_decided_at: None,
            creation_reply: None,
            initial_domain_receipts: std::sync::Arc::new(Vec::new()),
            creation_refusal: None,
            rejected_retirement_observed: false,
            initial_creation: None,
            state: TaskState::Planned,
            version: 1,
            installed: false,
            published: TaskStatus::created(identity),
            resources: TaskResourceFacts::empty(),
            writer: None,
            filter_advertisement: None,
            operator_statistics: Vec::new(),
            operator_statistics_truncated: false,
            statistics_sealed: false,
            terminal_observation_received: false,
            normal_close_validated: false,
            loader_exited: false,
            preparation_body_exited: false,
            preparation_exited: false,
            actual_stopped: false,
            runtime_actual_stopped_received: false,
            runtime_resources_converged_received: false,
            completion_success_pending: false,
            preparation_exit_holds: [false; 4],
            output_released: false,
            output: TaskOutputFacts::new(false),
            convergence: TaskConvergence::new(),
            resources_converged: false,
            root_idle: false,
            root_transferred: false,
            applied_close_generation: generation,
            scheduled_close_generation: None,
            scheduled_quiesce_generation: None,
            applied_quiesce_generation: None,
            conclusion: ConclusionState::default(),
            diagnostics: Diagnostics::default(),
            proposals: ProposalSummary::default(),
        }
    }
    pub fn physical_exit_ready(&self) -> bool {
        self.loader_exited
            && self.preparation_exited
            && self.actual_stopped
            && self.resources_converged
            && ((self.output_released && self.root_idle) || self.root_transferred)
    }
    pub fn retirement_ready(&self) -> bool {
        (self.state.is_terminal() || self.phase == CreationPhase::Rejected)
            && self.physical_exit_ready()
            && self.waiting_replies.is_empty()
    }
    pub(crate) fn compose_status(&self) -> Result<TaskStatus, crate::context::CoreError> {
        let termination = match self.conclusion.accepted.as_ref() {
            Some(novarocks_execution_contract::TaskConclusion::Terminated(detail)) => {
                Some(detail.clone())
            }
            _ => None,
        };
        compose_status(
            self.identity,
            self.version,
            self.state,
            termination,
            self.output,
            self.installed,
            self.resources,
            self.writer,
            self.filter_advertisement,
        )
    }
    pub(crate) fn into_decided(
        self,
        status: TaskStatus,
        final_info: Option<std::sync::Arc<novarocks_execution_contract::FinalTaskInfo>>,
    ) -> Result<crate::retention::DecidedTask, crate::context::CoreError> {
        if !self.accepted()
            || self.phase != CreationPhase::Retained
            || !self.state.is_terminal()
            || !self.retirement_ready()
            || !self.proposals.is_empty()
            || self.initial_creation.is_some()
            || self.scheduled_close_generation.is_some()
            || self.scheduled_quiesce_generation.is_some()
            || !self.operator_statistics.is_empty()
            || self.creation_reply.is_none()
            || self.creation_decided_at.is_none()
        {
            return Err(crate::context::CoreError::IllegalFact);
        }
        Ok(crate::retention::DecidedTask {
            custody_payload: self.payload,
            custody_epoch: self.applied_close_generation,
            status,
            output: self.output,
            initial_domains: self.initial_domain_receipts,
            final_info,
            diagnostics: self.diagnostics,
            convergence: self.convergence.snapshot(),
            source: self.conclusion.source,
        })
    }
    pub fn bump_version(&mut self) {
        self.version = self.version.checked_add(1).expect("task version exhausted");
    }
}

/// One status composer for both executing and already-decided neutral facts.
pub(crate) fn compose_status(
    identity: TaskIdentity,
    version: u64,
    state: TaskState,
    termination: Option<novarocks_execution_contract::TerminationDetail>,
    output: TaskOutputFacts,
    installed: bool,
    resources: TaskResourceFacts,
    writer: Option<TaskWriterFacts>,
    filter: Option<DynamicFilterAdvertisement>,
) -> Result<TaskStatus, crate::context::CoreError> {
    use crate::context::CoreError;
    let mut status = TaskStatus::try_new_with_installed(
        identity,
        novarocks_execution_contract::TaskStatusVersion::new(version)
            .map_err(|_| CoreError::VersionExhausted)?,
        state,
        termination,
        output,
        installed,
    )
    .map_err(|_| CoreError::IllegalFact)?
    .with_resources(resources);
    if let Some(writer) = writer {
        status = status.with_writer(writer);
    }
    if let Some(filter) = filter {
        status = status.with_dynamic_filters(filter);
    }
    Ok(status)
}
