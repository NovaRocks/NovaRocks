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

//! Transport-neutral task execution contracts.
//!
//! This module is the single definition point for the native task protocol's
//! domain model: identities, the immutable task descriptor, closed typed
//! operations and domains, task and query-context states, immutable versioned
//! status, the query execution lease, and the pure transitions that classify
//! every one of them.
//!
//! It deliberately depends on neither generated protobuf, gRPC, the frontend,
//! the backend, nor any connector provider implementation. The wire grammar is
//! a separate representation of these types, and the frontend and backend own
//! their own runtime state machines built on top of them; only immutable
//! values, this module's pure validation, and the central codec are shared
//! across the process boundary.
// Design: ADR-0135 (docs/adr/ADR-0135-native-distributed-work-as-tasks.md)

pub mod descriptor;
pub mod domain;
pub mod identity;
pub mod lease;
pub mod operation;
pub mod status;
pub mod transition;

pub use descriptor::{
    DescriptorError, ExchangeDestination, ExchangeEdge, ExchangeInbound, ExchangeSource,
    ExchangeTopology, IngressRejection, PhysicalFragmentPlan, TaskDescriptor,
};
pub use domain::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialDomain, CredentialEpoch,
    CredentialLeaseId, DomainConflict, DomainProgression, DomainVersion, EdgeOpenVersion,
    EdgeSendPermission, ExchangeEdgeDomain, ExchangeEdgeId, PlanNodeId, QueryContextDomainKind,
    ScalarDomain, SplitDomain, SplitOffer, SplitSequence, SplitWatermark, TaskDomainKind,
};
pub use identity::{
    IdentityField, IdentityMismatch, QueryContextRef, StageRef, TaskIdentity, TaskOperationId,
    TaskOperationIdError,
};
pub use lease::{
    InstalledLease, LeaseBounds, LeaseProgression, LeaseReceipt, LeaseSequence, LeaseValidFor,
    LeaseValidForError, MonotonicInstant, RenewSchedule, RequestHorizon,
};
pub use operation::{
    AbortQueryContext, AdvanceQueryContextDomain, CancelTask, CreateTask, CreateTaskReceipt,
    CredentialUpdate, DEFAULT_STATUS_SUBSCRIPTION_ERROR_BUDGET, DispatchBudget, DispatchLane,
    EstablishQueryContext, FetchTaskDynamicFilters, FrontendAction, GetFinalTaskInfo, MaxWait,
    MaxWaitError, OperationEnvelope, OperationKind, OperationOutcome, OperationWaitCaps,
    PlanNodeSplitReceipt, QueryContextDomainReceipt, QueryContextDomainUpdate, QueryContextReceipt,
    ReleaseOutcome, ReleaseQueryContext, RenewQueryExecutionLease, RequestError,
    SplitAssignmentIntent, TaskDomainReceipt, TaskDomainUpdate, TaskExecutionBudgets,
    TransportBudget, UpdateQueryContext, UpdateTask, UpdateTaskReceipt,
};
pub use status::{
    AbortCause, CancelReason, DynamicFilterAdvertisement, FinalInfoDisagreement, FinalTaskInfo,
    FinalTaskInfoError, GoneObservation, OperatorStatistics, ResultPacketVerdict, RootResultStream,
    SafeDetail, SafeFieldPath, SafeTextTooLong, StatusObservation, TaskFailure,
    TaskFailureCategory, TaskOutputFacts, TaskResourceFacts, TaskState, TaskStatus,
    TaskStatusCursor, TaskStatusError, TaskStatusVersion, TaskWriterFacts, TerminationDetail,
    classify_gone, classify_observation, verify_final_info,
};
pub use transition::{
    AttemptDrainFacts, ContextOperationKind, ContextTransition, LatchOutcome, OperationAdmission,
    QueryContextEvent, QueryContextState, RootDrainAction, RootReadFacts, StageState,
    TaskTransition, TerminationLatch, WriteCompletionFacts, classify_context_transition,
    classify_operation_admission, classify_root_drain, classify_task_transition,
    derive_stage_state, parent_released_children, terminals_are_success_compatible,
};
