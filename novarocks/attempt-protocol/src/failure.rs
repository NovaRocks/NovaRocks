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

//! Passive failure facts. Original application errors stay in host custody.

use novarocks_coordination_contract::AttemptFailureClass;
use novarocks_execution_contract::failure::FailureFact;
use novarocks_execution_contract::{
    OperationKind, ParticipationRef, SafeDetail, TaskIdentity, TaskOperationId,
};
use std::num::NonZeroU64;

/// Reference to an original typed host error, scoped to this activation.
/// It is neither an error classifier nor permission to access another owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HostFailureRef(pub NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostFailureKind {
    Workload,
    Execution,
    SplitAssignment,
    LogicalActor,
    NativeTransport,
    EstablishAuthorization,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptFailureSite {
    Attempt,
    Participation(ParticipationRef),
    Task {
        identity: TaskIdentity,
        phase: novarocks_execution_contract::TaskFailurePhase,
    },
    TaskControl {
        identity: TaskIdentity,
    },
    RootRead {
        root: TaskIdentity,
    },
    Backend {
        process: novarocks_types::BackendProcessId,
    },
    Operation {
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        kind: OperationKind,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtocolFailure {
    pub fact: FailureFact,
    pub detail: SafeDetail,
    pub site: AttemptFailureSite,
}

/// The original typed host projection, including its unmodified site and phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostProjectedFailure {
    pub original: HostFailureRef,
    pub kind: HostFailureKind,
    pub class: AttemptFailureClass,
    pub site: AttemptFailureSite,
}

impl std::fmt::Display for HostProjectedFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "host failure {:?} at {:?} (reference {})",
            self.kind, self.site, self.original.0
        )
    }
}
impl std::error::Error for HostProjectedFailure {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptFailure {
    Protocol(ProtocolFailure),
    TaskProtocol(Box<crate::error::TaskExecutionError>),
    Host(HostProjectedFailure),
    SelectedTask(novarocks_coordination_contract::SelectedTaskTermination),
    /// The original pure gate combines its same-operation transport-unknown
    /// history with QA's independent typed exhausted-budget reply. The original
    /// host error remains retained; kind/class/text cannot infer this fact.
    EstablishReplayExhausted {
        original: HostProjectedFailure,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
    },
    /// Positive replacement at the original frozen endpoint. The bridge uses
    /// the existing BackendLost author; this is not heartbeat/admission loss.
    BackendReplaced {
        original: novarocks_types::BackendProcessId,
        replacement: novarocks_types::BackendProcessId,
    },
    /// Independent accepted replacement evidence beside the SAME definitive
    /// refusal. The bridge still invokes QA's existing witness validation and
    /// attach operation; this fact grants no retry, admission or exit proof.
    PreReadyReplacement {
        refusal: Box<AttemptFailure>,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        kind: OperationKind,
        replacement: novarocks_types::BackendProcessId,
    },
}
