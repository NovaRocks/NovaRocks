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

use super::*;
use crate::facts::{
    AttemptFacts, LogicalFacts, LogicalPhase, LogicalSchemaState, ParticipationFacts,
};
impl<'a> LogicalFacts<'a> {
    pub(crate) fn new(core: &'a LogicalExecutionCore) -> Self {
        Self { core }
    }
    pub fn identity(&self) -> novarocks_types::QueryId {
        self.core.start.identity
    }
    pub fn actor(&self) -> novarocks_coordination_contract::LogicalExecutionActorId {
        self.core.start.actor_instance
    }
    pub fn phase(&self) -> LogicalPhase {
        match self.core.state.phase() {
            crate::state::ExecutionPhase::Instantiating { activation } => {
                LogicalPhase::Instantiating(activation)
            }
            crate::state::ExecutionPhase::Running { activation } => {
                LogicalPhase::Running(activation)
            }
            crate::state::ExecutionPhase::Replacing { identity } => {
                LogicalPhase::Replacing(identity)
            }
            crate::state::ExecutionPhase::Converging => LogicalPhase::Converging,
        }
    }
    pub fn conclusion(&self) -> Option<LogicalConclusion> {
        self.core.state.conclusion()
    }
    pub fn failure(&self) -> Option<&'a QueryExecutionError> {
        self.core.logical_failure.as_ref()
    }
    pub fn supervision_failure(&self) -> Option<&'a QueryExecutionError> {
        self.core.supervision_failure.as_ref()
    }
    pub fn current_activation(&self) -> Option<AttemptActivationIdentity> {
        self.core.state.current_activation()
    }
    pub fn attempts_started(&self) -> u32 {
        self.core.state.attempts_started()
    }
    pub fn max_attempts(&self) -> u32 {
        self.core.state.max_attempts()
    }
    pub fn original_internal_consumer_exited(&self) -> bool {
        self.core
            .ready
            .as_ref()
            .is_some_and(InternalReady::original_consumer_exited)
    }
    pub fn output_visible(&self) -> bool {
        self.core.state.output_visible()
    }
    pub fn schema(&self) -> LogicalSchemaState {
        match self.core.visibility.schema() {
            crate::visibility::SchemaState::NotStarted => LogicalSchemaState::NotStarted,
            crate::visibility::SchemaState::InFlight => LogicalSchemaState::InFlight,
            crate::visibility::SchemaState::Done => LogicalSchemaState::Done,
        }
    }
    pub fn ready_cut(&self) -> Option<novarocks_coordination_contract::AttemptComputationCut> {
        self.core
            .ready
            .as_ref()
            .and_then(InternalReady::accepted)
            .map(|a| a.cut)
    }
    pub fn outstanding_actions(&self) -> usize {
        self.core.actions.len()
    }
    pub fn retirable(&self) -> bool {
        self.core.retirable
    }
    pub fn protocol_join_ready(&self) -> bool {
        self.core.protocol_join_ready()
    }
    pub fn ingress_closed(&self) -> bool {
        self.core.ingress_closed
    }
    pub fn attempts(&self) -> impl Iterator<Item = AttemptFacts<'a>> + 'a {
        let core = self.core;
        core.attempts.values().map(move |attempt| AttemptFacts {
            activation: attempt.activation,
            participations: &attempt.participations,
            accepted_failure: core
                .state
                .attempt(attempt.activation.execution())
                .and_then(|a| a.accepted_failure.as_ref()),
            native_run_failure: attempt.native_run_failure.as_ref(),
            authority_open: !core.business_closed() && attempt.establish.authority_open(),
            activated: attempt.activated,
            relay_started: attempt.relay_started,
            all_participations_closed: attempt.stand_down.residual_resource_settled(),
            remote_tracking_ended: attempt.stand_down.remote_tracking_ended(),
            cleanup_complete: attempt
                .convergence
                .cleanup_complete(attempt.stand_down.progress()),
            cleanup_path: attempt.convergence.selected_path(),
            cleanup_failure: attempt.convergence.failure(),
            remote_deadline: attempt.convergence.remote_deadline(),
            root_projection: attempt.root.as_ref().and_then(RootControl::accepted),
            delivery_root: core.visibility.root(attempt.activation),
        })
    }
    pub fn participation(
        &self,
        activation: AttemptActivationIdentity,
        p: ParticipationRef,
    ) -> Option<ParticipationFacts> {
        let attempt = self
            .core
            .attempts
            .get(&activation.execution())
            .filter(|a| a.activation == activation)?;
        let snapshot = attempt.stand_down.snapshot(p)?;
        Some(ParticipationFacts {
            closure: snapshot.closure,
            abort_operation: snapshot.identity.map(|i| i.operation_id),
            abort_issue_state: snapshot.issue_state,
            abort_authorizations: snapshot.authorizations,
            current_abort_action: snapshot.current_action,
        })
    }
}
