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

//! Attempt-local outgoing facts before passive cross-core projection.
//! Physical send generations select original host receipts and never cross the port.

use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptAuthorizationRef, AttemptConvergenceClosure,
    AttemptIssueSettlement, AttemptRequestToken, AttemptRootProjection, AttemptStopReason,
    ResultParticipationWithdrawal, RuntimeFilterDiagnostic,
};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, OperationOutcome, ParticipationAdmissionTicketReceipt,
    ParticipationRef, TaskOperationId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptTerminal<F> {
    Completed,
    Failed(F),
    Stopped {
        reason: AttemptStopReason,
        /// Original typed cancellation/logical failure; never reconstructed
        /// from the coarse control reason or its display text.
        failure: F,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum AttemptAuthorization {
    BeginAdmission(AcquireParticipationAdmissionTicket),
    SettleAdmission {
        /// Resolve this same original QA receipt; identity grants no access.
        issue: AttemptAuthorizationRef,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        outcome: Option<OperationOutcome>,
        receipt: Option<ParticipationAdmissionTicketReceipt>,
        prior_transport_unknown: bool,
    },
    /// The original request allocation is held by the host under this exact identity.
    Establish {
        participation: ParticipationRef,
        operation_id: TaskOperationId,
    },
    ReauthorizeEstablish {
        participation: ParticipationRef,
        /// Expected original request, checked against the returned submission.
        operation_id: TaskOperationId,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortIssueSettlement {
    DefinitelyUnsent,
    TransportUnknown,
    Worker(OperationOutcome),
    FailedClosed,
}

#[derive(Clone, Debug)]
pub struct AttemptToLogical<F> {
    pub activation: AttemptActivationIdentity,
    pub action: AttemptActionId,
    pub body: AttemptToLogicalBody<F>,
}

#[derive(Clone, Debug)]
pub enum AttemptToLogicalBody<F> {
    AuthorizationRequest {
        token: AttemptRequestToken,
        request: AttemptAuthorization,
    },
    PreReadyReplacementQuery {
        token: AttemptRequestToken,
        refusal: F,
    },
    EstablishSettlement {
        token: AttemptRequestToken,
        /// AP selects current/oldest-late/all-late order, not the host lookup.
        authorization: AttemptAuthorizationRef,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        settlement: AttemptIssueSettlement,
    },
    AbortSettlement {
        /// Original actor submission and its corresponding Native admission.
        abort_action: AttemptActionId,
        generation: AttemptActionId,
        /// The canonical positive receipt can come from a different generation.
        receipt_generation: Option<AttemptActionId>,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        settlement: AbortIssueSettlement,
    },
    RootStatus(Box<AttemptRootProjection>),
    SealResult {
        token: AttemptRequestToken,
        result: Result<std::sync::Arc<[ResultParticipationWithdrawal]>, F>,
    },
    TerminalControlResult {
        token: AttemptRequestToken,
        result: Result<(), F>,
    },
    AttemptTerminal(AttemptTerminal<F>),
    /// The failed core is never read to reconstruct this host-authored fact.
    AttemptLost {
        failure: F,
        unknown_participations: Box<[ParticipationRef]>,
        unresolved_actions: Box<[AttemptActionId]>,
    },
    ConvergenceProgress {
        local_exited: bool,
        closure: Option<std::sync::Arc<AttemptConvergenceClosure>>,
        lifecycle_runtime_filters: Option<std::sync::Arc<RuntimeFilterDiagnostic>>,
    },
}
