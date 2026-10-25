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

//! Worker-owned policy for native task execution.
//!
//! The execution contract supplies immutable cross-process facts. This crate
//! decides how one worker admits those facts, advances local lifecycle state,
//! fences exchange input, and accounts for its process-local time. It owns no
//! transport model and has no dependency on a query application or frontend.

/// Capabilities compiled by this owner; activation still requires a role session.
pub const fn compiled_test_capabilities() -> (&'static str, &'static [&'static str]) {
    (
        env!("CARGO_PKG_NAME"),
        &[
            #[cfg(novarocks_dev_controls)]
            "test-controls",
        ],
    )
}

use std::num::NonZeroUsize;

mod admission;
mod admission_epoch;
mod catalog_manager;
mod catalog_manager_config;
mod clock;
pub mod control_runtime_policy;
pub mod connector_batch_transform;
pub mod connector_variant_transform;
pub mod connector_write_runtime;
mod convergence;
mod credential_slot;
mod deadline;
mod domain;
mod drain;
mod failure_reason;
pub mod guarded_bytes;
mod host;
mod inbound_capability;
mod ingress;
mod lease;
mod lease_expiry_index;
mod lifecycle;
mod observation;
mod operation;
pub mod query_context;
pub mod read_attempt;
mod receipt;
mod reliable_transport;
pub mod result_batch;
pub mod result_buffer;
pub mod root_result_channel;
mod runtime_filter_error;
mod settlement;
pub mod sink_commit;
mod status;
mod task_completion;
mod task_conclusion;
mod task_creation_gate;
mod task_diagnostics;
mod task_domain_execution;
mod task_execution_ports;
mod task_preparation;
mod task_proposal_ingress;
mod task_protocol_event;
pub use task_preparation::{
    TaskPreparationChild, TaskPreparationChildExit, TaskPreparationChildRegistrationFailure,
};
mod task_registry;
mod task_registry_config;
mod task_registry_entry;
pub use host::TaskPreparationContext;
pub use task_registry_entry::TaskPreparationView;
#[cfg(test)]
mod task_registry_tests;
pub mod typed_connector_runtime;
pub mod typed_page_source;
mod typed_preparation_flow;
pub mod typed_scan_filter;
mod typed_scan_runtime;

/// Worker-owned runtime-filter artifacts and local resource contracts.
pub mod runtime_filter {
    pub mod artifact;
    pub mod artifact_query;
    pub mod codec;
    pub mod domain;
    pub mod execution_session;
    pub mod final_domain;
    pub mod fixture;
    pub mod materializer;
    pub mod observation;
    pub mod participant;
    pub mod participant_ingress;
    pub mod typed_scan;
}

pub use admission::{
    AdmissionReservationObservation, AdmissionTicketAcquisitionRejection, AdmissionTicketAuthority,
    AdmissionTicketConfig, AdmissionTicketGrant, AdmissionTicketObservation,
    AdmissionTicketProgression, AdmissionTicketRedemption, AdmissionTicketRedemptionRejection,
    AdmissionTicketState, MAX_ADMISSION_RESERVATIONS, MAX_ADMISSION_TICKET_VALID_FOR,
};
pub use admission_epoch::WorkerAdmissionEpochAuthority;
pub use catalog_manager::{
    CatalogLeaseSnapshot, CatalogManager, CatalogManagerError, CatalogPruneResult,
    ConnectorExecutionRoleBindingFactorySet,
};
pub use catalog_manager_config::{CatalogManagerConfig, DEFAULT_MAX_RETAINED_CATALOGS};
pub use clock::{ManualClock, ProcessMonotonicClock, WorkerMonotonicClock};
pub use convergence::{
    TaskConvergence, TaskConvergenceAdvance, TaskConvergenceRejection, TaskConvergenceSnapshot,
};
pub use credential_slot::QueryContextCredentialSlot;
pub use deadline::{WorkerDeadlineAuthority, WorkerDeadlineSupervisor};
pub use domain::{
    DomainPolicyRejection, QueryContextDomains, TaskDomains, commit_task_domain_updates,
    plan_task_domain_updates, task_domain_reaches_execution, validate_task_domain_membership,
};
pub use drain::WorkerDrainState;
#[cfg(novarocks_dev_controls)]
pub use host::TestTaskUpdateConsumptionReservation;
pub use host::{
    HostRejection, PreparationControlLoan, PreparedTaskInstallation, QueryContextHost,
    ReleasedContextEvidence, RunnableTask, SharedFactsRequest, TaskExecutionHost,
};
pub use inbound_capability::{
    InboundFrameAdmission, InboundFrameClaim, InboundFrameDelivery, NormalClosedInbound,
    TaskInboundCapabilities, TaskInboundCapabilityLimits,
};
pub use ingress::{IngressRejection, authorize_inbound_frame};
pub use lease::{InstalledLease, LeaseBounds, LeaseProgression, MonotonicInstant, RequestHorizon};
pub use lifecycle::{
    ContextOperationKind, ContextTransition, LatchOutcome, OperationAdmission, QueryContextEvent,
    RootDrainAction, TaskTransition, TerminationLatch, classify_context_transition,
    classify_operation_admission, classify_root_drain, classify_task_transition,
};
pub use novarocks_execution_contract::AdmissionEpochCapability;
pub use observation::{
    ContextConvergenceCursorError, CoveredObservationBookmark, CoveredObservationFact,
    CoveredObservationFrame, CoveredSubscription, CoveredSubscriptionError, CursorObservation,
    QuiesceObservationCursor, SelectedCoveredObservation, TaskStatusEvent, TaskStatusSource,
    TaskStatusSubscriptionPosition,
};
pub use operation::OperationWaitCaps;
pub use receipt::{
    AdmissionTicketOutcome, CancelTaskOutcome, CreateTaskOutcome, DynamicFilterReadOutcome,
    FinalTaskInfoOutcome, OperationReceipt, QueryContextOutcome, QuiesceQueryContextOutcome,
    ReleaseAcknowledgement, ReleaseQueryContextOutcome, TaskDynamicFilterRead, UpdateTaskOutcome,
};
pub use reliable_transport::{
    ReliableTransportAckOutcome, ReliableTransportFailOpenReason, ReliableTransportFailureOutcome,
    ReliableTransportPolicy, ReliableTransportResourceLimit, ReliableTransportSendOutcome,
    ReliableTransportState, ReliableTransportStateError, ReliableTransportTick,
};
pub use runtime_filter_error::{RuntimeFilterContractError, RuntimeFilterContractErrorCode};
pub use status::{
    METRIC_PUBLISH_MIN_INTERVAL, RootResultBinding, RootResultRoute, StatusAdvance,
    TaskMetricsSink, TaskStatusOwner, TaskStatusReporter,
};
pub use task_completion::{
    CompletionReservationError, TaskCompletionAction, TaskCompletionReservation,
    TaskCompletionSignal, TaskCompletionSupervisor,
};
pub use task_creation_gate::{NoopTaskCreationGate, TaskCreationGate};
pub use task_diagnostics::{
    DiagnosticRecordOutcome, PreparedCleanupDiagnostic, TaskDiagnosticRole,
    TaskSecondaryDiagnostic, TaskSecondaryDiagnosticsSnapshot,
};
pub use task_domain_execution::{
    DomainExecutionRejection, apply_planned_task_domain_updates, apply_task_domain_updates,
    commit_task_domain_execution_updates, plan_task_domain_execution_updates,
    validate_task_domain_execution_membership,
};
pub use task_execution_ports::{
    TaskExecutionMetrics, TaskExecutionPorts, TaskProtocolObserver, TaskResultLifecycle,
};
pub use task_protocol_event::{RuntimeFilterReleaseObservation, TaskProtocolEvent};
pub use task_registry::{
    DeadlineSweep, RegistryCounters, RegistryLockObservation, RegistryLockSnapshot,
    TaskExecutionRegistry, TaskPreparationSnapshot, TaskPreparationSnapshotError,
};
pub use task_registry_config::{TaskExecutionRegistryConfig, TaskPreparationLimits};
pub use typed_preparation_flow::ScanPreparationTimer;
pub use typed_scan_runtime::{
    CatalogExecutionAccessError, CatalogReadExecutionResolver, CatalogWriteExecutionResolver,
    ConnectorResourceBindingError, RuntimeFilterSessionResolver, ScanPreparationConfig,
    ScanStreamHost, TypedConnectorReadDescriptor, TypedScanRuntime,
};

/// Positive, ordered joint retained-result limits owned by one worker process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerResultRetainedLimits {
    per_root: NonZeroUsize,
    per_process: NonZeroUsize,
}

/// The immutable result policy is installed before a channel can reserve.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultRetainedPolicyError {
    ZeroRoot,
    ZeroProcess,
    RootExceedsProcess { per_root: usize, per_process: usize },
}
impl std::fmt::Display for ResultRetainedPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroRoot => {
                f.write_str("per-root joint result retained-byte cap must be greater than 0")
            }
            Self::ZeroProcess => {
                f.write_str("per-process joint result retained-byte cap must be greater than 0")
            }
            Self::RootExceedsProcess {
                per_root,
                per_process,
            } => write!(
                f,
                "per-root joint result retained-byte cap {per_root} must not exceed per-process cap {per_process}"
            ),
        }
    }
}
impl std::error::Error for ResultRetainedPolicyError {}
impl novarocks_execution_contract::failure::FailureProjection for ResultRetainedPolicyError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Invariant,
            match self {
                Self::ZeroRoot => R::ResultPolicyZeroRoot,
                Self::ZeroProcess => R::ResultPolicyZeroProcess,
                Self::RootExceedsProcess { .. } => R::ResultPolicyRootExceedsProcess,
            }
            .reason(),
        )
    }
}

impl WorkerResultRetainedLimits {
    pub fn try_new(per_root: usize, per_process: usize) -> Result<Self, ResultRetainedPolicyError> {
        let per_root = NonZeroUsize::new(per_root).ok_or(ResultRetainedPolicyError::ZeroRoot)?;
        let per_process =
            NonZeroUsize::new(per_process).ok_or(ResultRetainedPolicyError::ZeroProcess)?;
        if per_root > per_process {
            return Err(ResultRetainedPolicyError::RootExceedsProcess {
                per_root: per_root.get(),
                per_process: per_process.get(),
            });
        }
        Ok(Self {
            per_root,
            per_process,
        })
    }
    pub fn per_root(self) -> NonZeroUsize {
        self.per_root
    }
    pub fn per_process(self) -> NonZeroUsize {
        self.per_process
    }
}

#[cfg(any(test, novarocks_dev_controls))]
pub use host::TestPreparationControl;
#[cfg(any(test, novarocks_dev_controls))]
pub use task_registry_entry::TestPreparationIntent;

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");

#[cfg(test)]
mod result_policy_failure_tests {
    use super::*;
    use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
    #[test]
    fn immutable_policy_refusal_is_not_a_live_capacity_denial() {
        let cases = [
            (0, 1, ResultRetainedPolicyError::ZeroRoot),
            (1, 0, ResultRetainedPolicyError::ZeroProcess),
            (
                2,
                1,
                ResultRetainedPolicyError::RootExceedsProcess {
                    per_root: 2,
                    per_process: 1,
                },
            ),
        ];
        for (root, process, expected) in cases {
            let actual = WorkerResultRetainedLimits::try_new(root, process).unwrap_err();
            assert_eq!(actual, expected);
            assert_eq!(actual.failure_fact().class(), FailureClass::Invariant);
            assert_eq!(actual.failure_fact().resource(), None);
        }
        let valid = WorkerResultRetainedLimits::try_new(8, 16).unwrap();
        assert_eq!((valid.per_root().get(), valid.per_process().get()), (8, 16));
    }
}
