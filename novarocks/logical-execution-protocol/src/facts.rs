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

//! Read-only protocol observations. These facts grant no resources or authority.
pub use crate::convergence::CleanupPath;
pub use crate::stand_down::{AbortParticipationIssueState, ParticipationClosureState};
use crate::{
    LogicalExecutionCore,
    input::{FailureReport, QualificationIdentity},
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptRootProjection, ExecutionFailureInput, MonotonicInstant,
    QueryExecutionError,
};
use novarocks_execution_contract::{ParticipationRef, TaskIdentity, TaskOperationId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalPhase {
    Instantiating(AttemptActivationIdentity),
    Running(AttemptActivationIdentity),
    Replacing(QualificationIdentity),
    Converging,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalSchemaState {
    NotStarted,
    InFlight,
    Done,
}

pub struct LogicalFacts<'a> {
    pub(crate) core: &'a LogicalExecutionCore,
}
#[derive(Clone, Copy, Debug)]
pub struct AttemptFacts<'a> {
    pub activation: AttemptActivationIdentity,
    pub participations: &'a [ParticipationRef],
    pub accepted_failure: Option<&'a ExecutionFailureInput>,
    pub native_run_failure: Option<&'a FailureReport>,
    pub authority_open: bool,
    pub activated: bool,
    pub relay_started: bool,
    pub all_participations_closed: bool,
    pub remote_tracking_ended: bool,
    pub cleanup_complete: bool,
    pub cleanup_path: Option<CleanupPath>,
    pub cleanup_failure: Option<&'a QueryExecutionError>,
    pub remote_deadline: Option<MonotonicInstant>,
    pub root_projection: Option<&'a AttemptRootProjection>,
    pub delivery_root: Option<TaskIdentity>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParticipationFacts {
    pub closure: ParticipationClosureState,
    pub abort_operation: Option<TaskOperationId>,
    pub abort_issue_state: Option<AbortParticipationIssueState>,
    pub abort_authorizations: usize,
    pub current_abort_action: Option<novarocks_coordination_contract::AttemptActionId>,
}
