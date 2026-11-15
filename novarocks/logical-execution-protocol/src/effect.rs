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

//! Ordered decisions. Lane and work kind derive from the body, never caller defaults.

use crate::{
    LogicalActionId, LogicalRequestToken, LogicalSourceId,
    input::{ObligationKind, QualificationIdentity, SuccessorTopology},
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptComputationCut, AttemptInheritedAdmission,
    ExecutionFailureInput, MonotonicInstant, QueryExecutionError, RuntimeFilterDiagnostic,
    logical_port::LogicalToAttempt,
};
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use novarocks_types::QueryExecutionId;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalWorkKind {
    Business,
    Settlement,
    Cleanup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalLane {
    Attempt(AttemptActivationIdentity),
    Native,
    Relay(AttemptActivationIdentity),
    Consumer,
    Governance,
    Diagnostics,
}

#[derive(Clone, Debug)]
pub enum NativeEffect {
    ActivateAttempt {
        activation: AttemptActivationIdentity,
        inherited_admissions: Vec<AttemptInheritedAdmission>,
    },
    /// One original installed attempt run, supervised separately from activation.
    RunAttempt {
        activation: AttemptActivationIdentity,
    },
    PrepareSuccessor {
        failed: QueryExecutionId,
        replacement: QueryExecutionId,
        topology: SuccessorTopology,
        deadline: MonotonicInstant,
    },
    DispatchQualification {
        identity: QualificationIdentity,
        operation_id: TaskOperationId,
        failed_participations: Arc<[ParticipationRef]>,
        replacement_participations: Arc<[ParticipationRef]>,
        issued_at: MonotonicInstant,
        expires_at: MonotonicInstant,
    },
    ActivateReservation {
        identity: QualificationIdentity,
        operation_id: TaskOperationId,
    },
    CancelQualification {
        identity: QualificationIdentity,
        operation_id: TaskOperationId,
    },
    AbandonReservation {
        identity: QualificationIdentity,
    },
    FinishResources {
        activation: AttemptActivationIdentity,
    },
    AbandonResources {
        activation: AttemptActivationIdentity,
    },
    ConvergeDormant {
        execution: QueryExecutionId,
        remote_deadline: MonotonicInstant,
    },
    RecordTerminalCut {
        activation: AttemptActivationIdentity,
        cut: MonotonicInstant,
        remote_deadline: MonotonicInstant,
    },
    RecordStreamingSuccess {
        activation: AttemptActivationIdentity,
    },
    RecordComputationReady {
        cut: AttemptComputationCut,
    },
    RetirePreparation,
}

#[derive(Clone, Debug)]
pub enum RelayResponse {
    Opened,
    DeliveryGranted,
    VisibilityGranted,
    Applied,
    Rejected(QueryExecutionError),
}

#[derive(Clone, Debug)]
pub enum RelayEffect {
    Start {
        activation: AttemptActivationIdentity,
    },
    Stop {
        activation: AttemptActivationIdentity,
    },
    Reply {
        source: LogicalSourceId,
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        response: RelayResponse,
    },
}

#[derive(Clone, Debug)]
pub enum InternalResponse {
    SourceRegistered,
    Applied,
    Rejected(QueryExecutionError),
}

#[derive(Clone, Debug)]
pub enum ConsumerEffect {
    WriteSchema {
        activation: AttemptActivationIdentity,
    },
    ReserveEndSlot {
        activation: AttemptActivationIdentity,
    },
    EnqueueEnd {
        activation: AttemptActivationIdentity,
    },
    ExposeOutput {
        activation: AttemptActivationIdentity,
    },
    PublishFailure(QueryExecutionError),
    /// Completion-only success, after the original Task completion boundary.
    PublishCompletion,
    /// The original attempt-local stream keeps its accepted failure across a
    /// successor. This is separate from the logical execution's final result.
    PublishAttemptFailure {
        activation: AttemptActivationIdentity,
        error: QueryExecutionError,
    },
    InternalReply {
        source: LogicalSourceId,
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        response: InternalResponse,
    },
    PublishReady {
        source: LogicalSourceId,
        token: LogicalRequestToken,
        cut: AttemptComputationCut,
    },
    PublishDiagnostics {
        activation: AttemptActivationIdentity,
        deadline: MonotonicInstant,
    },
    PublishConvergence {
        activation: AttemptActivationIdentity,
        all_participations_closed: bool,
        remote_tracking_ended: bool,
        local_resources_exited: bool,
    },
}

#[derive(Clone, Debug)]
pub enum GovernanceEffect {
    RegisterObligation {
        activation: AttemptActivationIdentity,
        kind: ObligationKind,
    },
    ResolveObligation {
        activation: AttemptActivationIdentity,
        kind: ObligationKind,
    },
    EndRemoteTracking {
        activation: AttemptActivationIdentity,
        kind: ObligationKind,
    },
    ReleaseExecutionStage,
    RequestShutdownCancellation,
    RequestClientCancellation,
    CompleteWorkOwner,
}

#[derive(Clone, Debug)]
pub struct AttemptLifecycleDiagnostic {
    pub activation: AttemptActivationIdentity,
    pub accepted_failure: Option<ExecutionFailureInput>,
    pub all_participations_closed: bool,
    pub runtime_filters: Option<Arc<RuntimeFilterDiagnostic>>,
}

#[derive(Clone, Debug)]
pub enum LogicalEffectBody {
    AttemptPort(Box<LogicalToAttempt>),
    Native(NativeEffect),
    Relay(RelayEffect),
    Consumer(ConsumerEffect),
    Governance(GovernanceEffect),
    AttemptLifecycleDiagnostic(Box<AttemptLifecycleDiagnostic>),
    /// The adapter reads the core's immutable facts in this same exclusive turn.
    SnapshotReply {
        source: LogicalSourceId,
        token: LogicalRequestToken,
    },
    RegistryJoinReply {
        source: LogicalSourceId,
        token: LogicalRequestToken,
        ready: bool,
    },
    /// A required supervision operation failed. The carrier retains this exact
    /// typed cause and fills its original infrastructure-failure marker.
    SupervisionFailed(QueryExecutionError),
    Retirable,
}

#[derive(Clone, Debug)]
pub struct LogicalEffect {
    pub action: LogicalActionId,
    pub body: LogicalEffectBody,
}

impl LogicalEffect {
    pub fn lane(&self) -> LogicalLane {
        match &self.body {
            LogicalEffectBody::AttemptPort(message) => LogicalLane::Attempt(message.activation),
            LogicalEffectBody::Native(_) | LogicalEffectBody::Retirable => LogicalLane::Native,
            LogicalEffectBody::Relay(effect) => LogicalLane::Relay(match effect {
                RelayEffect::Start { activation }
                | RelayEffect::Stop { activation }
                | RelayEffect::Reply { activation, .. } => *activation,
            }),
            LogicalEffectBody::Consumer(_) => LogicalLane::Consumer,
            LogicalEffectBody::Governance(_) | LogicalEffectBody::RegistryJoinReply { .. } => {
                LogicalLane::Governance
            }
            LogicalEffectBody::AttemptLifecycleDiagnostic(_)
            | LogicalEffectBody::SupervisionFailed(_)
            | LogicalEffectBody::SnapshotReply { .. } => LogicalLane::Diagnostics,
        }
    }

    pub fn work_kind(&self) -> LogicalWorkKind {
        match &self.body {
            LogicalEffectBody::Native(
                NativeEffect::ActivateAttempt { .. }
                | NativeEffect::RunAttempt { .. }
                | NativeEffect::PrepareSuccessor { .. }
                | NativeEffect::DispatchQualification { .. }
                | NativeEffect::ActivateReservation { .. },
            )
            | LogicalEffectBody::Relay(RelayEffect::Start { .. })
            | LogicalEffectBody::Consumer(
                ConsumerEffect::WriteSchema { .. }
                | ConsumerEffect::ReserveEndSlot { .. }
                | ConsumerEffect::EnqueueEnd { .. }
                | ConsumerEffect::ExposeOutput { .. },
            )
            | LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation { .. }) => {
                LogicalWorkKind::Business
            }
            LogicalEffectBody::AttemptPort(_)
            | LogicalEffectBody::Native(
                NativeEffect::CancelQualification { .. }
                | NativeEffect::AbandonReservation { .. }
                | NativeEffect::FinishResources { .. }
                | NativeEffect::AbandonResources { .. }
                | NativeEffect::ConvergeDormant { .. }
                | NativeEffect::RecordTerminalCut { .. }
                | NativeEffect::RecordStreamingSuccess { .. }
                | NativeEffect::RecordComputationReady { .. }
                | NativeEffect::RetirePreparation,
            )
            | LogicalEffectBody::Relay(RelayEffect::Stop { .. } | RelayEffect::Reply { .. })
            | LogicalEffectBody::Consumer(
                ConsumerEffect::PublishFailure(_)
                | ConsumerEffect::PublishCompletion
                | ConsumerEffect::PublishAttemptFailure { .. }
                | ConsumerEffect::InternalReply { .. }
                | ConsumerEffect::PublishReady { .. }
                | ConsumerEffect::PublishDiagnostics { .. }
                | ConsumerEffect::PublishConvergence { .. },
            )
            | LogicalEffectBody::Governance(
                GovernanceEffect::ResolveObligation { .. }
                | GovernanceEffect::EndRemoteTracking { .. }
                | GovernanceEffect::ReleaseExecutionStage
                | GovernanceEffect::RequestShutdownCancellation
                | GovernanceEffect::RequestClientCancellation
                | GovernanceEffect::CompleteWorkOwner,
            )
            | LogicalEffectBody::AttemptLifecycleDiagnostic(_)
            | LogicalEffectBody::SupervisionFailed(_)
            | LogicalEffectBody::SnapshotReply { .. }
            | LogicalEffectBody::RegistryJoinReply { .. }
            | LogicalEffectBody::Retirable => LogicalWorkKind::Settlement,
        }
    }
}
