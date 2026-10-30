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
//! Query-owned decisions over immutable producer-authored execution facts.
//!
//! A failure site is evidence, not a second Task conclusion or a recovery
//! instruction. Transport loss must be authored where it was actually observed;
//! capacity, a local timeout and an answered remote refusal cannot manufacture it.

use crate::api::{NativeAttemptTopologyRequirement, QueryExecutionErrorKind};
use novarocks_coordination_contract::AttemptFailureClass;
use novarocks_execution_contract::{
    QueryContextRef, SafeDetail, TaskFailurePhase, TaskIdentity, TaskOperationId,
    failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
    operation::OperationKind,
};
use novarocks_types::BackendProcessId;
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};
use std::num::NonZeroU16;

/// Shared identity-refusal reason authored by the FE Task protocol projector.
/// It describes identity-refusal semantics, not remote provenance, a specific
/// identity field, pre-ready state, or independent process replacement.
pub const TASK_PROTOCOL_IDENTITY_MISMATCH_REASON: FailureReason =
    FailureReason::new(domain::QUERY, NonZeroU16::new(15).unwrap());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionFailureSite {
    Task {
        identity: TaskIdentity,
        phase: TaskFailurePhase,
    },
    /// A control termination reports no fabricated preparation/execution phase.
    TaskControl {
        identity: TaskIdentity,
    },
    Operation {
        context: QueryContextRef,
        operation_id: TaskOperationId,
        kind: OperationKind,
    },
    RootRead {
        root: TaskIdentity,
    },
    Backend {
        process: BackendProcessId,
    },
    /// A local coordinator failure with no Task/operation acceptance claim.
    Coordinator,
}

/// Facts and the bounded safe rendering cross the boundary together exactly
/// once. No caller can replace only the class or reconstruct facts from text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionFailureInput {
    fact: FailureFact,
    detail: SafeDetail,
    site: ExecutionFailureSite,
}
impl ExecutionFailureInput {
    pub const fn new(fact: FailureFact, detail: SafeDetail, site: ExecutionFailureSite) -> Self {
        Self { fact, detail, site }
    }
    pub fn from_selected_task(
        selected: &novarocks_coordination_contract::SelectedTaskTermination,
    ) -> Option<Self> {
        use novarocks_execution_contract::{AbortCause, TerminationDetail};
        let identity = selected.identity();
        let (fact, detail, site) = match selected.detail() {
            TerminationDetail::Failed(failure) => (
                failure.fact(),
                failure.detail().clone(),
                ExecutionFailureSite::Task {
                    identity,
                    phase: failure.phase(),
                },
            ),
            TerminationDetail::Aborted(cause) => {
                let reason = match cause {
                    AbortCause::PeerTaskFailed => return None,
                    AbortCause::LeaseExpired => QueryFailureReason::TaskLeaseExpired,
                    AbortCause::QueryFailed => QueryFailureReason::TaskAborted,
                };
                (
                    reason.failure_fact(),
                    SafeDetail::truncating(cause.as_str()),
                    ExecutionFailureSite::TaskControl { identity },
                )
            }
            TerminationDetail::Canceled(reason) => (
                QueryFailureReason::TaskCancelled.failure_fact(),
                SafeDetail::truncating(reason.as_str()),
                ExecutionFailureSite::TaskControl { identity },
            ),
        };
        Some(Self::new(fact, detail, site))
    }

    pub const fn fact(&self) -> FailureFact {
        self.fact
    }
    pub const fn detail(&self) -> &SafeDetail {
        &self.detail
    }
    pub const fn site(&self) -> ExecutionFailureSite {
        self.site
    }
    pub fn attempt_class(&self) -> AttemptFailureClass {
        if self.fact.class() == FailureClass::Dependency
            && self.fact.evidence()
                == novarocks_execution_contract::failure::FailureEvidence::Originating
            && self.fact.reason() == QueryFailureReason::BackendLost.failure_fact().reason()
            && matches!(self.site, ExecutionFailureSite::Backend { .. })
        {
            return AttemptFailureClass::RecoverableInfrastructure;
        }
        if self.fact.evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site, ExecutionFailureSite::Coordinator)
            && self.fact.class() == QueryFailureReason::Cancelled.failure_fact().class()
            && self.fact.reason() == QueryFailureReason::Cancelled.failure_fact().reason()
        {
            return AttemptFailureClass::Cancelled;
        }
        if self.fact.evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site, ExecutionFailureSite::Coordinator)
            && self.fact.class() == QueryFailureReason::DeadlineExceeded.failure_fact().class()
            && self.fact.reason() == QueryFailureReason::DeadlineExceeded.failure_fact().reason()
        {
            return AttemptFailureClass::DeadlineExceeded;
        }
        if self.fact.evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site, ExecutionFailureSite::Coordinator)
            && self.fact.class() == QueryFailureReason::CommitUnknown.failure_fact().class()
            && self.fact.reason() == QueryFailureReason::CommitUnknown.failure_fact().reason()
        {
            return AttemptFailureClass::CommitUnknown;
        }
        match self.fact.class() {
            FailureClass::Unsupported => AttemptFailureClass::Unsupported,
            FailureClass::Resource => AttemptFailureClass::ResourceGovernance,
            FailureClass::Invariant => AttemptFailureClass::ContractViolation,
            FailureClass::Unexpected
            | FailureClass::Data
            | FailureClass::Dependency
            | FailureClass::Representation
            | FailureClass::Control => AttemptFailureClass::ExecutionFailure,
        }
    }
    pub fn topology_requirement(&self) -> NativeAttemptTopologyRequirement {
        match (self.attempt_class(), self.site) {
            (
                AttemptFailureClass::RecoverableInfrastructure,
                ExecutionFailureSite::Backend { process },
            ) => NativeAttemptTopologyRequirement::ExcludeProcess(process),
            _ => NativeAttemptTopologyRequirement::LiveSnapshot,
        }
    }
    pub fn error_kind(&self) -> QueryExecutionErrorKind {
        match self.attempt_class() {
            AttemptFailureClass::Cancelled => QueryExecutionErrorKind::Cancelled,
            AttemptFailureClass::DeadlineExceeded => QueryExecutionErrorKind::DeadlineExceeded,
            AttemptFailureClass::ResourceGovernance => QueryExecutionErrorKind::Rejected,
            _ => QueryExecutionErrorKind::Failed,
        }
    }
    pub fn descriptor(&self) -> SqlExecutionErrorDescriptor {
        use SqlExecutionErrorKind as K;
        let kind = if self.fact.evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site, ExecutionFailureSite::Coordinator)
            && self.fact.class() == QueryFailureReason::Cancelled.failure_fact().class()
            && self.fact.reason() == QueryFailureReason::Cancelled.failure_fact().reason()
        {
            K::Cancelled
        } else if self.fact.evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site, ExecutionFailureSite::Coordinator)
            && self.fact.class() == QueryFailureReason::DeadlineExceeded.failure_fact().class()
            && self.fact.reason() == QueryFailureReason::DeadlineExceeded.failure_fact().reason()
        {
            K::DeadlineExceeded
        } else if self.fact.evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site, ExecutionFailureSite::Coordinator)
            && self.fact.class() == QueryFailureReason::CommitUnknown.failure_fact().class()
            && self.fact.reason() == QueryFailureReason::CommitUnknown.failure_fact().reason()
        {
            K::CommitUnknown
        } else if self.fact.class() == FailureClass::Data
            && self.fact.reason()
                == novarocks_function_contract::kernel_failure::NUMERIC_OUT_OF_RANGE
        {
            K::NumericOutOfRange
        } else {
            match self.fact.class() {
                FailureClass::Unsupported if self.is_compiled_capability() => {
                    K::UnsupportedCompiledShape
                }
                FailureClass::Unsupported => K::Unsupported,
                FailureClass::Data => K::Data,
                FailureClass::Dependency => K::Dependency,
                FailureClass::Resource => K::ResourceExhausted,
                FailureClass::Representation => K::Representation,
                FailureClass::Invariant => K::Invariant,
                FailureClass::Unexpected => K::Unexpected,
                FailureClass::Control => K::Control,
            }
        };
        SqlExecutionErrorDescriptor::for_kind(kind)
    }
    fn is_compiled_capability(&self) -> bool {
        use novarocks_failure_contract::local_compiler_reason as compiler_failure;
        use novarocks_function_contract::kernel_failure;
        if self.fact.evidence()
            != novarocks_execution_contract::failure::FailureEvidence::Originating
        {
            return false;
        }
        let reason = self.fact.reason();
        // These exact reasons are emitted only by the compiled-plan authors.
        if [
            compiler_failure::UNSUPPORTED_FRAGMENT,
            compiler_failure::UNSUPPORTED_EXPRESSION,
            compiler_failure::UNSUPPORTED_CALL,
            novarocks_failure_contract::plan_codec_reason::ENCODE_UNSUPPORTED,
        ]
        .contains(&reason)
        {
            return true;
        }
        // Function preparation is shared with non-compiler callers. Only the
        // typed Task preparation path establishes compiled use here; every BE
        // Task preparation consumes the original frozen compiled package.
        matches!(
            self.site,
            ExecutionFailureSite::Task {
                phase: TaskFailurePhase::Preparation,
                ..
            }
        ) && [
            kernel_failure::MISSING_PURE_IMPLEMENTATION,
            kernel_failure::UNSUPPORTED_COMPARISON,
            kernel_failure::UNSUPPORTED_ARITHMETIC,
            kernel_failure::UNSUPPORTED_CAST,
            novarocks_function_contract::UNAVAILABLE_IMPLEMENTATION,
        ]
        .contains(&reason)
    }
    pub fn user_error(&self) -> UserError {
        UserError::from_descriptor(
            self.descriptor().descriptor,
            self.detail.as_str(),
            None,
            RetryClass::Never,
        )
    }
}

/// Reasons produced by query coordination itself. Domain errors arriving from
/// Task, operation or root owners retain their original fact without wrapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryFailureReason {
    Cancelled,
    DeadlineExceeded,
    /// An actual failed backend connection/process observation. Never a local
    /// read deadline, observation budget, Preparing or plain remote Unavailable.
    BackendLost,
    TransportDeadline,
    TransportRefused,
    TransportCapacity,
    ResultProtocol,
    ActorProtocol,
    ObservationLost,
    PreparationProtocol,
    CommitUnknown,
    TaskAborted,
    TaskLeaseExpired,
    TaskCancelled,
    NativeFuturePanicked,
    InternalConsumerPanicked,
    SplitAssignmentFailed,
    PreparationFailed,
    RecoveryPreparationBudgetExceeded,
    LogicalFailureUnspecified,
    LogicalOwnerStopped,
    BlockingWorkerPanicked,
    BlockingExecutorClosed,
    BlockingSourceContract,
    BlockingQueueFull,
}
impl FailureProjection for QueryFailureReason {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::Cancelled => (FailureClass::Control, 1),
            Self::DeadlineExceeded => (FailureClass::Control, 2),
            Self::BackendLost => (FailureClass::Dependency, 3),
            Self::TransportDeadline => (FailureClass::Dependency, 4),
            Self::TransportRefused => (FailureClass::Dependency, 5),
            Self::TransportCapacity => (FailureClass::Resource, 13),
            Self::ResultProtocol => (FailureClass::Invariant, 6),
            Self::ActorProtocol => (FailureClass::Invariant, 7),
            Self::ObservationLost => (FailureClass::Dependency, 8),
            Self::PreparationProtocol => (FailureClass::Invariant, 9),
            Self::CommitUnknown => (FailureClass::Unexpected, 10),
            Self::TaskAborted => (FailureClass::Control, 11),
            Self::TaskLeaseExpired => (FailureClass::Control, 12),
            Self::TaskCancelled => (FailureClass::Control, 27),
            Self::NativeFuturePanicked => (FailureClass::Unexpected, 33),
            Self::InternalConsumerPanicked => (FailureClass::Unexpected, 175),
            Self::SplitAssignmentFailed => (FailureClass::Unexpected, 176),
            Self::PreparationFailed => (FailureClass::Unexpected, 34),
            Self::RecoveryPreparationBudgetExceeded => (FailureClass::Dependency, 151),
            Self::LogicalFailureUnspecified => (FailureClass::Unexpected, 172),
            Self::LogicalOwnerStopped => (FailureClass::Control, 174),
            Self::BlockingWorkerPanicked => (FailureClass::Unexpected, 177),
            Self::BlockingExecutorClosed => (FailureClass::Dependency, 178),
            Self::BlockingSourceContract => (FailureClass::Invariant, 179),
            Self::BlockingQueueFull => (FailureClass::Resource, 180),
        };
        let reason = FailureReason::new(domain::QUERY, NonZeroU16::new(code).unwrap());
        if matches!(self, Self::LogicalOwnerStopped) {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlExecutionErrorKind {
    UnsupportedCompiledShape,
    Unsupported,
    ResourceExhausted,
    NumericOutOfRange,
    Data,
    Representation,
    Dependency,
    Invariant,
    Unexpected,
    Control,
    Cancelled,
    DeadlineExceeded,
    CommitUnknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlExecutionErrorDescriptor {
    pub descriptor: ErrorCodeDescriptor,
    pub errno: u16,
    pub sqlstate: &'static str,
}
impl SqlExecutionErrorDescriptor {
    pub const fn for_kind(kind: SqlExecutionErrorKind) -> Self {
        use SqlExecutionErrorKind::*;
        let (code, errno, sqlstate) = match kind {
            UnsupportedCompiledShape => ("UnsupportedCompiledShape", 1105, "HY000"),
            Unsupported => ("query.execution.unsupported", 1105, "HY000"),
            ResourceExhausted => ("query.execution.resource_exhausted", 1041, "HY000"),
            NumericOutOfRange => ("query.execution.numeric_out_of_range", 1690, "22003"),
            Data => ("query.execution.invalid_data", 1105, "HY000"),
            Representation => ("query.execution.representation", 1105, "HY000"),
            Dependency => ("query.execution.dependency", 1105, "HY000"),
            Invariant => ("query.execution.invariant", 1105, "HY000"),
            Unexpected => ("query.execution.unexpected", 1105, "HY000"),
            Control => ("query.execution.control", 1105, "HY000"),
            Cancelled => ("query.execution.cancelled", 1317, "70100"),
            DeadlineExceeded => ("query.execution.deadline_exceeded", 1317, "70100"),
            CommitUnknown => ("query.execution.commit_unknown", 1105, "HY000"),
        };
        Self {
            descriptor: ErrorCodeDescriptor {
                code: ErrorCodeId::new(code),
                phase: ErrorPhase::Execution,
                status: ErrorCodeStatus::Active,
            },
            errno,
            sqlstate,
        }
    }
}
/// The existing compiled capability identity is registered unchanged. Its
/// historical spelling does not authorize any other EngineErrorCode as a SQL
/// descriptor; consumers still require this exact registered Execution entry.
pub const UNSUPPORTED_COMPILED_SHAPE_DESCRIPTOR: ErrorCodeDescriptor =
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::UnsupportedCompiledShape)
        .descriptor;

pub const EXECUTION_ERROR_CODE_DESCRIPTORS: &[ErrorCodeDescriptor] = &[
    UNSUPPORTED_COMPILED_SHAPE_DESCRIPTOR,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::Unsupported).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::ResourceExhausted).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::NumericOutOfRange).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::Data).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::Representation).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::Dependency).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::Invariant).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::Unexpected).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::Control).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::Cancelled).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::DeadlineExceeded).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::CommitUnknown).descriptor,
];
pub fn execution_descriptor_for_code(code: ErrorCodeId) -> Option<SqlExecutionErrorDescriptor> {
    use SqlExecutionErrorKind::*;
    [
        UnsupportedCompiledShape,
        Unsupported,
        ResourceExhausted,
        NumericOutOfRange,
        Data,
        Representation,
        Dependency,
        Invariant,
        Unexpected,
        Control,
        Cancelled,
        DeadlineExceeded,
        CommitUnknown,
    ]
    .into_iter()
    .map(SqlExecutionErrorDescriptor::for_kind)
    .find(|entry| entry.descriptor.code == code)
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    use crate::api::QueryExecutionError;
    use novarocks_execution_contract::failure::{
        FailureEvidence, ResourceDimension, ResourceFacts, ResourceScope,
    };
    use novarocks_execution_contract::{SafeDetail, TaskFailure, TerminationDetail};
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn task(id: u32) -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(61, 21), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(3).unwrap(),
            TaskId::new(id).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    #[test]
    fn unsupported_fact_keeps_the_registered_engine_identity_at_both_client_exits() {
        use std::num::NonZeroU16;
        let fact = FailureFact::new(
            FailureClass::Unsupported,
            FailureReason::new(domain::LOCAL_COMPILER, NonZeroU16::new(1).unwrap()),
        );
        let input = ExecutionFailureInput::new(
            fact,
            SafeDetail::new("FULL OUTER hash join").unwrap(),
            ExecutionFailureSite::Task {
                identity: task(9),
                phase: TaskFailurePhase::Preparation,
            },
        );
        assert_eq!(input.fact(), fact);
        assert_eq!(input.attempt_class(), AttemptFailureClass::Unsupported);
        let descriptor = input.descriptor();
        assert_eq!(
            descriptor.descriptor.code.as_str(),
            novarocks_types::EngineErrorCode::UnsupportedCompiledShape.as_str(),
        );
        assert_eq!(descriptor.descriptor, UNSUPPORTED_COMPILED_SHAPE_DESCRIPTOR);
        assert_eq!((descriptor.errno, descriptor.sqlstate), (1105, "HY000"));
        assert_eq!(
            execution_descriptor_for_code(descriptor.descriptor.code),
            Some(descriptor),
        );
        let user = input.user_error();
        assert_eq!(user.phase(), ErrorPhase::Execution);
        assert_eq!(user.location(), None);
        assert_eq!(user.retry(), RetryClass::Never);
        let service =
            crate::session_error::QueryServiceError::from_execution_failure(input.clone());
        let stream = QueryExecutionError::from_execution(input.clone());
        assert_eq!(service.execution_failure(), Some(&input));
        assert_eq!(stream.execution_failure(), Some(&input));
        assert_eq!(
            service.message(),
            "[UnsupportedCompiledShape] FULL OUTER hash join"
        );
        assert_eq!(
            crate::session_error::execution_public_message(&stream),
            service.message(),
        );
        let invalid = ExecutionFailureInput::new(
            QueryFailureReason::ResultProtocol.failure_fact(),
            SafeDetail::new("unsupported diagnostic is not a capability").unwrap(),
            ExecutionFailureSite::Coordinator,
        );
        assert_eq!(
            invalid.attempt_class(),
            AttemptFailureClass::ContractViolation
        );
        assert_ne!(invalid.descriptor().descriptor, descriptor.descriptor);
    }

    #[test]
    fn ordinary_provider_unsupported_keeps_its_source_and_generic_sql_identity() {
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let source = ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "UnsupportedCompiledShape is only diagnostic text here",
        );
        let cause = novarocks_failure_contract::SharedFailureCause::from_static_detail(
            source,
            "provider capability is unavailable",
        );
        let fact = cause.failure_fact();
        let service = crate::session_error::QueryServiceError::from_failure_cause(cause.clone());
        assert_eq!(service.failure_cause(), Some(&cause));
        let input = service.execution_failure().unwrap();
        assert_eq!(input.fact(), fact);
        assert_eq!(input.attempt_class(), AttemptFailureClass::Unsupported);
        let descriptor = input.descriptor();
        assert_eq!(
            descriptor.descriptor.code.as_str(),
            "query.execution.unsupported"
        );
        assert_eq!((descriptor.errno, descriptor.sqlstate), (1105, "HY000"));
        assert_eq!(
            execution_descriptor_for_code(descriptor.descriptor.code),
            Some(descriptor)
        );
        assert!(EXECUTION_ERROR_CODE_DESCRIPTORS.contains(&descriptor.descriptor));
        let stream = QueryExecutionError::from_execution(input.clone());
        assert_eq!(stream.execution_failure(), Some(input));
        assert_eq!(
            crate::session_error::execution_public_message(&stream),
            service.message()
        );
        assert_eq!(input.user_error().retry(), RetryClass::Never);
    }

    #[test]
    fn selected_preparation_keeps_exact_shared_function_capability_reasons() {
        use novarocks_function_contract::{FunctionBindingError, kernel_failure};
        let binding = FunctionBindingError::UnavailableImplementation(
            novarocks_type_contract::FunctionOverloadId::try_new("fixture/missing/pure-v1")
                .unwrap(),
        );
        assert_eq!(
            binding.failure_fact().reason(),
            novarocks_function_contract::UNAVAILABLE_IMPLEMENTATION
        );
        let facts = [
            FailureFact::new(
                FailureClass::Unsupported,
                kernel_failure::MISSING_PURE_IMPLEMENTATION,
            ),
            FailureFact::new(
                FailureClass::Unsupported,
                kernel_failure::UNSUPPORTED_COMPARISON,
            ),
            FailureFact::new(
                FailureClass::Unsupported,
                kernel_failure::UNSUPPORTED_ARITHMETIC,
            ),
            FailureFact::new(FailureClass::Unsupported, kernel_failure::UNSUPPORTED_CAST),
            binding.failure_fact(),
        ];
        for fact in facts {
            let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
                task(9),
                TerminationDetail::Failed(TaskFailure::new_in_phase(
                    fact,
                    SafeDetail::truncating("original function preparation capability"),
                    TaskFailurePhase::Preparation,
                )),
            );
            let input = ExecutionFailureInput::from_selected_task(&selected).unwrap();
            assert_eq!(input.fact(), fact);
            assert_eq!(
                input.descriptor().descriptor,
                UNSUPPORTED_COMPILED_SHAPE_DESCRIPTOR
            );
            // The same shared producer fact alone does not prove compiled use.
            for site in [
                ExecutionFailureSite::Coordinator,
                ExecutionFailureSite::Task {
                    identity: task(9),
                    phase: TaskFailurePhase::Execution,
                },
            ] {
                let other = ExecutionFailureInput::new(fact, input.detail().clone(), site);
                assert_eq!(other.fact(), fact);
                assert_eq!(
                    other.descriptor().descriptor.code.as_str(),
                    "query.execution.unsupported"
                );
            }
        }
    }

    #[test]
    fn preparation_site_and_diagnostic_do_not_upgrade_foreign_unsupported_facts() {
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let provider = ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "compiled shape unavailable",
        );
        for fact in [
            provider.failure_fact(),
            FailureFact::new(
                FailureClass::Unsupported,
                FailureReason::new(domain::LOCAL_COMPILER, NonZeroU16::new(65000).unwrap()),
            ),
            FailureFact::new(
                FailureClass::Unsupported,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(65000).unwrap()),
            ),
        ] {
            let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
                task(9),
                TerminationDetail::Failed(TaskFailure::new_in_phase(
                    fact,
                    SafeDetail::truncating("UnsupportedCompiledShape"),
                    TaskFailurePhase::Preparation,
                )),
            );
            let input = ExecutionFailureInput::from_selected_task(&selected).unwrap();
            assert_eq!(input.fact(), fact);
            assert_eq!(
                input.descriptor().descriptor.code.as_str(),
                "query.execution.unsupported"
            );
        }
        let stop = FailureFact::stop_observation(
            novarocks_function_contract::kernel_failure::UNSUPPORTED_CAST,
        );
        let input = ExecutionFailureInput::new(
            stop,
            SafeDetail::truncating("original stop"),
            ExecutionFailureSite::Task {
                identity: task(9),
                phase: TaskFailurePhase::Preparation,
            },
        );
        assert_eq!(input.fact(), stop);
        assert_eq!(
            input.descriptor().descriptor.code.as_str(),
            "query.execution.control"
        );
    }

    #[test]
    fn exact_producer_facts_survive_a_non_root_preparation_failure() {
        let origin = task(9);
        let resources = ResourceFacts {
            request_scope: Some(ResourceScope::Request),
            constraint_scope: None,
            dimension: ResourceDimension::Bytes,
            requested: Some(0),
            required_growth: None,
            limit: None,
            live: Some(u64::MAX),
            committed: None,
        };
        let fact = QueryFailureReason::TransportCapacity
            .failure_fact()
            .with_resource(resources);
        let failure = TaskFailure::new_in_phase(
            fact,
            SafeDetail::truncating("finite preparation capacity"),
            TaskFailurePhase::Preparation,
        );
        let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
            origin,
            TerminationDetail::Failed(failure),
        );
        let input = ExecutionFailureInput::from_selected_task(&selected).unwrap();
        assert_eq!(input.fact(), fact);
        assert_eq!(
            input.site(),
            ExecutionFailureSite::Task {
                identity: origin,
                phase: TaskFailurePhase::Preparation
            }
        );
        assert_eq!(
            input.attempt_class(),
            AttemptFailureClass::ResourceGovernance
        );
        assert_eq!(
            input.topology_requirement(),
            NativeAttemptTopologyRequirement::LiveSnapshot
        );
        let sql = input.descriptor();
        assert_eq!((sql.errno, sql.sqlstate), (1041, "HY000"));
        let user = input.user_error();
        assert_eq!(user.phase(), ErrorPhase::Execution);
        assert_eq!(user.location(), None);
    }
    #[test]
    fn only_actual_backend_loss_can_request_a_replacement_process() {
        let backend = BackendProcessId::new_v7();
        for reason in [
            QueryFailureReason::TransportDeadline,
            QueryFailureReason::TransportRefused,
            QueryFailureReason::ObservationLost,
            QueryFailureReason::TaskLeaseExpired,
        ] {
            let input = ExecutionFailureInput::new(
                reason.failure_fact(),
                SafeDetail::truncating("bounded read failed"),
                ExecutionFailureSite::Backend { process: backend },
            );
            assert_eq!(input.attempt_class(), AttemptFailureClass::ExecutionFailure);
            assert_eq!(
                input.topology_requirement(),
                NativeAttemptTopologyRequirement::LiveSnapshot
            );
        }
        let input = ExecutionFailureInput::new(
            QueryFailureReason::BackendLost.failure_fact(),
            SafeDetail::truncating("original connection lost"),
            ExecutionFailureSite::Backend { process: backend },
        );
        assert_eq!(
            input.attempt_class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(
            input.topology_requirement(),
            NativeAttemptTopologyRequirement::ExcludeProcess(backend)
        );
    }
    #[test]
    fn a_stop_observation_is_neither_an_accepted_cancel_nor_an_invariant() {
        let reason = QueryFailureReason::Cancelled.failure_fact().reason();
        let input = ExecutionFailureInput::new(
            FailureFact::stop_observation(reason),
            SafeDetail::truncating("work observed stop"),
            ExecutionFailureSite::Coordinator,
        );
        assert_eq!(input.fact().evidence(), FailureEvidence::StopObservation);
        assert_eq!(input.attempt_class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(
            (input.descriptor().errno, input.descriptor().sqlstate),
            (1105, "HY000")
        );
        let unknown = ExecutionFailureInput::new(
            QueryFailureReason::NativeFuturePanicked.failure_fact(),
            SafeDetail::truncating("attempt future panicked"),
            ExecutionFailureSite::Coordinator,
        );
        assert_eq!(
            unknown.attempt_class(),
            AttemptFailureClass::ExecutionFailure
        );
    }
    #[test]
    fn numeric_sql_code_uses_the_kernel_owned_reason_and_class() {
        let reason = novarocks_function_contract::kernel_failure::NUMERIC_OUT_OF_RANGE;
        let numeric = ExecutionFailureInput::new(
            FailureFact::new(FailureClass::Data, reason),
            SafeDetail::truncating("numeric value out of range"),
            ExecutionFailureSite::Coordinator,
        );
        assert_eq!(
            (numeric.descriptor().errno, numeric.descriptor().sqlstate),
            (1690, "22003")
        );
        let foreign_class = ExecutionFailureInput::new(
            FailureFact::new(FailureClass::Representation, reason),
            SafeDetail::truncating("unsupported representation"),
            ExecutionFailureSite::Coordinator,
        );
        assert_eq!(
            (
                foreign_class.descriptor().errno,
                foreign_class.descriptor().sqlstate
            ),
            (1105, "HY000")
        );
    }
}
