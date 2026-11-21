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

use crate::api::NativeAttemptTopologyRequirement;
use novarocks_coordination_contract::AttemptFailureClass;
use novarocks_execution_contract::{
    TaskFailurePhase,
    failure::{FailureClass, FailureProjection},
};
#[cfg(test)]
use novarocks_execution_contract::{
    TaskIdentity,
    failure::{FailureFact, FailureReason, domain},
};
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};
#[cfg(test)]
use std::num::NonZeroU16;

pub use novarocks_coordination_contract::{
    ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
    TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
};

/// The connector explicitly approved its bounded diagnostic for protocol use.
pub fn has_explicit_connector_detail(error: &crate::session_error::QueryServiceError) -> bool {
    use novarocks_spi::connector::{ConnectorError, ConnectorMutationFailure};
    let Some(cause) = error.failure_cause() else {
        return false;
    };
    cause
        .downcast_ref::<ConnectorError>()
        .or_else(|| {
            cause
                .downcast_ref::<ConnectorMutationFailure>()?
                .connector_source()
        })
        .and_then(ConnectorError::public_failure_detail)
        .is_some()
}

/// Project the publication verdict from the original terminal for a specific
/// data mutation. The statement tag is selected at admission, not from SQL text.
pub fn data_mutation_publication_effect(
    error: &crate::session_error::QueryServiceError,
    tag: &str,
) -> Option<crate::api::StatementExternalEffectDisposition> {
    use crate::api::StatementExternalEffectDisposition as Effect;
    use novarocks_spi::connector::{LakePublicationDisposition as Verdict, LakePublicationFamily};
    let terminal = error.publication_terminal()?;
    if terminal.header().family() != LakePublicationFamily::DataMutation
        || !terminal
            .client_statement_tag()
            .is_some_and(|actual| actual.as_str() == tag)
    {
        return None;
    }
    Some(match terminal.disposition() {
        Verdict::KnownUncommitted => Effect::KnownUncommitted,
        Verdict::CommitUnknown => Effect::CommitUnknown,
        Verdict::KnownCommitted => Effect::KnownCommitted,
    })
}

pub trait ExecutionFailurePresentation {
    fn topology_requirement(&self) -> NativeAttemptTopologyRequirement;
    fn descriptor(&self) -> SqlExecutionErrorDescriptor;
    fn user_error(&self) -> UserError;
    fn row_match_violation_message(&self) -> Option<&'static str>;
}

impl ExecutionFailurePresentation for ExecutionFailureInput {
    fn topology_requirement(&self) -> NativeAttemptTopologyRequirement {
        match (self.attempt_class(), self.site()) {
            (
                AttemptFailureClass::RecoverableInfrastructure,
                ExecutionFailureSite::Backend { process },
            ) => NativeAttemptTopologyRequirement::ExcludeProcess(process),
            _ => NativeAttemptTopologyRequirement::LiveSnapshot,
        }
    }
    fn descriptor(&self) -> SqlExecutionErrorDescriptor {
        use SqlExecutionErrorKind as K;
        let kind = if self.fact().evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site(), ExecutionFailureSite::Coordinator)
            && self.fact().class() == QueryFailureReason::Cancelled.failure_fact().class()
            && self.fact().reason() == QueryFailureReason::Cancelled.failure_fact().reason()
        {
            K::Cancelled
        } else if self.fact().evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site(), ExecutionFailureSite::Coordinator)
            && self.fact().class() == QueryFailureReason::DeadlineExceeded.failure_fact().class()
            && self.fact().reason() == QueryFailureReason::DeadlineExceeded.failure_fact().reason()
        {
            K::DeadlineExceeded
        } else if self.fact().evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site(), ExecutionFailureSite::Coordinator)
            && self.fact().class() == QueryFailureReason::CommitUnknown.failure_fact().class()
            && self.fact().reason() == QueryFailureReason::CommitUnknown.failure_fact().reason()
        {
            K::CommitUnknown
        } else if self.fact().class() == FailureClass::Data
            && self.fact().reason()
                == novarocks_failure_contract::sql_reason::MERGE_TARGET_ROW_MULTIPLE_MATCHES
        {
            K::MergeTargetRowMultipleMatches
        } else if self.fact().class() == FailureClass::Data
            && self.fact().reason()
                == novarocks_failure_contract::sql_reason::UPDATE_TARGET_ROW_MULTIPLE_MATCHES
        {
            K::UpdateTargetRowMultipleMatches
        } else if self.fact().class() == FailureClass::Data
            && self.fact().reason()
                == novarocks_function_contract::kernel_failure::NUMERIC_OUT_OF_RANGE
        {
            K::NumericOutOfRange
        } else if self.fact().class() == FailureClass::Data
            && self.fact().reason() == novarocks_function_contract::kernel_failure::DIVISION_BY_ZERO
        {
            K::DivisionByZero
        } else if self.fact().class() == FailureClass::Data
            && self.fact().reason()
                == novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT
        {
            K::InvalidCastArgument
        } else if self.fact().class() == FailureClass::Data
            && self.fact().reason()
                == novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT
        {
            K::InvalidFunctionArgument
        } else {
            match self.fact().class() {
                FailureClass::Unsupported if is_compiled_capability(self) => {
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
    fn user_error(&self) -> UserError {
        UserError::from_descriptor(
            self.descriptor().descriptor,
            self.row_match_violation_message()
                .unwrap_or(self.detail().as_str()),
            None,
            RetryClass::Never,
        )
    }

    /// A planner-declared SQL identity controls the public message; the original
    /// execution fact, safe diagnostic and site remain available independently.
    fn row_match_violation_message(&self) -> Option<&'static str> {
        if self.fact().class() != FailureClass::Data {
            return None;
        }
        match self.fact().reason() {
            reason if reason == novarocks_failure_contract::sql_reason::MERGE_TARGET_ROW_MULTIPLE_MATCHES => Some(MERGE_TARGET_ROW_MULTIPLE_MATCHES_MESSAGE),
            reason if reason == novarocks_failure_contract::sql_reason::UPDATE_TARGET_ROW_MULTIPLE_MATCHES => Some(UPDATE_TARGET_ROW_MULTIPLE_MATCHES_MESSAGE),
            _ => None,
        }
    }
}

fn is_compiled_capability(input: &ExecutionFailureInput) -> bool {
    use novarocks_failure_contract::local_compiler_reason as compiler_failure;
    use novarocks_function_contract::kernel_failure;
    if input.fact().evidence()
        != novarocks_execution_contract::failure::FailureEvidence::Originating
    {
        return false;
    }
    let reason = input.fact().reason();
    // These exact reasons are emitted only by the compiled-plan authors.
    if [
        compiler_failure::UNSUPPORTED_FRAGMENT,
        compiler_failure::UNSUPPORTED_EXPRESSION,
        compiler_failure::UNSUPPORTED_CALL,
        novarocks_failure_contract::plan_codec_reason::ENCODE_UNSUPPORTED,
        novarocks_failure_contract::sql_reason::STRUCTURAL_WAIT_CYCLE,
    ]
    .contains(&reason)
    {
        return true;
    }
    // Function preparation is shared with non-compiler callers. Only the
    // typed Task preparation path establishes compiled use here; every BE
    // Task preparation consumes the original frozen compiled package.
    matches!(
        input.site(),
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

pub const MERGE_TARGET_ROW_MULTIPLE_MATCHES_MESSAGE: &str =
    "One MERGE target table row matched more than one source row";
pub const UPDATE_TARGET_ROW_MULTIPLE_MATCHES_MESSAGE: &str =
    "One UPDATE target table row matched more than one source row";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlExecutionErrorKind {
    UnsupportedCompiledShape,
    Unsupported,
    ResourceExhausted,
    NumericOutOfRange,
    DivisionByZero,
    InvalidCastArgument,
    InvalidFunctionArgument,
    Data,
    MergeTargetRowMultipleMatches,
    UpdateTargetRowMultipleMatches,
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
            DivisionByZero => ("query.execution.division_by_zero", 1365, "22012"),
            InvalidCastArgument => ("query.execution.invalid_cast_argument", 1105, "HY000"),
            InvalidFunctionArgument => ("query.execution.invalid_function_argument", 1105, "HY000"),
            Data => ("query.execution.invalid_data", 1105, "HY000"),
            MergeTargetRowMultipleMatches => (
                "query.execution.merge_target_row_multiple_matches",
                1105,
                "HY000",
            ),
            UpdateTargetRowMultipleMatches => (
                "query.execution.update_target_row_multiple_matches",
                1105,
                "HY000",
            ),
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
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::DivisionByZero).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::InvalidCastArgument).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::InvalidFunctionArgument)
        .descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::Data).descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::MergeTargetRowMultipleMatches)
        .descriptor,
    SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::UpdateTargetRowMultipleMatches)
        .descriptor,
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
        DivisionByZero,
        InvalidCastArgument,
        InvalidFunctionArgument,
        Data,
        MergeTargetRowMultipleMatches,
        UpdateTargetRowMultipleMatches,
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
    fn sql_wait_cycle_has_compiled_capability_identity_without_text_classification() {
        let reason = novarocks_failure_contract::sql_reason::STRUCTURAL_WAIT_CYCLE;
        let fact = FailureFact::new(FailureClass::Unsupported, reason);
        let input = ExecutionFailureInput::new(
            fact,
            SafeDetail::new("structural wait cycle requires decoupled retained output (B4)")
                .unwrap(),
            ExecutionFailureSite::Coordinator,
        );
        assert_eq!(input.fact(), fact);
        assert_eq!(input.attempt_class(), AttemptFailureClass::Unsupported);
        assert_eq!(
            input.descriptor().descriptor,
            UNSUPPORTED_COMPILED_SHAPE_DESCRIPTOR
        );
        assert_eq!(input.user_error().retry(), RetryClass::Never);
        let service =
            crate::session_error::QueryServiceError::from_execution_failure(input.clone());
        let stream = QueryExecutionError::from_execution(input);
        assert!(service.message().starts_with("[UnsupportedCompiledShape]"));
        assert_eq!(
            crate::session_error::execution_public_message(&stream),
            service.message()
        );
        // A generic SQL capability is not a compiled-shape refusal just
        // because it is in the same domain or repeats the diagnostic.
        let ordinary = ExecutionFailureInput::new(
            FailureFact::new(
                FailureClass::Unsupported,
                FailureReason::new(domain::SQL, std::num::NonZeroU16::new(10).unwrap()),
            ),
            SafeDetail::new("structural wait cycle").unwrap(),
            ExecutionFailureSite::Coordinator,
        );
        assert_ne!(
            ordinary.descriptor().descriptor,
            UNSUPPORTED_COMPILED_SHAPE_DESCRIPTOR
        );
        let stop = ExecutionFailureInput::new(
            FailureFact::stop_observation(reason),
            SafeDetail::new("structural wait cycle").unwrap(),
            ExecutionFailureSite::Coordinator,
        );
        assert_ne!(
            stop.descriptor().descriptor,
            UNSUPPORTED_COMPILED_SHAPE_DESCRIPTOR
        );
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
    fn representation_fact_preserves_nonretryable_execution_identity() {
        // This typed-input test covers application policy; the real List
        // guard condition is tested by Functions without a cross-package API.
        let fact = FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(320).unwrap()),
        );
        let input = ExecutionFailureInput::new(
            fact,
            SafeDetail::truncating("List output exceeds its format extent"),
            ExecutionFailureSite::Coordinator,
        );
        assert_eq!(input.fact(), fact);
        assert_eq!(input.attempt_class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(
            input.descriptor().descriptor.code.as_str(),
            "query.execution.representation"
        );
        assert_eq!(input.user_error().retry(), RetryClass::Never);
    }

    #[test]
    fn invalid_cast_argument_requires_the_original_kernel_data_reason() {
        let reason = novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT;
        assert_eq!(reason.domain(), domain::KERNEL.get());
        assert_eq!(reason.code(), 44);
        let expected =
            SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::InvalidCastArgument);
        assert_eq!(
            expected.descriptor.code.as_str(),
            "query.execution.invalid_cast_argument"
        );
        assert_eq!((expected.errno, expected.sqlstate), (1105, "HY000"));
        assert_eq!(expected.descriptor.phase, ErrorPhase::Execution);
        assert_eq!(expected.descriptor.status, ErrorCodeStatus::Active);
        assert!(EXECUTION_ERROR_CODE_DESCRIPTORS.contains(&expected.descriptor));
        assert_eq!(
            execution_descriptor_for_code(expected.descriptor.code),
            Some(expected)
        );
        for detail in [
            "unrelated numeric overflow",
            "invalid encoding",
            "invalid cast argument",
        ] {
            let fact = FailureFact::new(FailureClass::Data, reason);
            let input = ExecutionFailureInput::new(
                fact,
                SafeDetail::truncating(detail),
                ExecutionFailureSite::Coordinator,
            );
            let copied = input.clone();
            assert_eq!(copied.fact(), fact);
            assert_eq!(copied.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(copied.detail(), input.detail());
            assert_eq!(copied.descriptor(), expected);
            assert_eq!(
                copied.attempt_class(),
                AttemptFailureClass::ExecutionFailure
            );
            assert_eq!(copied.user_error().retry(), RetryClass::Never);
        }
        for (class, expected_code) in [
            (
                FailureClass::Representation,
                "query.execution.representation",
            ),
            (FailureClass::Invariant, "query.execution.invariant"),
            (FailureClass::Control, "query.execution.control"),
            (FailureClass::Resource, "query.execution.resource_exhausted"),
            (FailureClass::Unsupported, "query.execution.unsupported"),
        ] {
            let fact = FailureFact::new(class, reason);
            let input = ExecutionFailureInput::new(
                fact,
                SafeDetail::truncating("invalid cast argument"),
                ExecutionFailureSite::Coordinator,
            );
            assert_eq!(input.fact(), fact);
            assert_eq!(input.descriptor().descriptor.code.as_str(), expected_code);
        }
        for (reason, expected_code) in [
            (
                FailureReason::new(domain::EXECUTION, NonZeroU16::new(44).unwrap()),
                "query.execution.invalid_data",
            ),
            (
                FailureReason::new(domain::KERNEL, NonZeroU16::new(40).unwrap()),
                "query.execution.invalid_data",
            ),
            (
                novarocks_function_contract::kernel_failure::NUMERIC_OUT_OF_RANGE,
                "query.execution.numeric_out_of_range",
            ),
            (
                novarocks_function_contract::kernel_failure::DIVISION_BY_ZERO,
                "query.execution.division_by_zero",
            ),
        ] {
            let fact = FailureFact::new(FailureClass::Data, reason);
            let input = ExecutionFailureInput::new(
                fact,
                SafeDetail::truncating("invalid cast argument"),
                ExecutionFailureSite::Coordinator,
            );
            assert_eq!(input.fact(), fact);
            assert_eq!(input.descriptor().descriptor.code.as_str(), expected_code);
        }
    }

    #[test]
    fn invalid_function_argument_requires_the_original_kernel_data_reason() {
        let reason = novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT;
        assert_eq!(reason.domain(), domain::KERNEL.get());
        assert_eq!(reason.code(), 45);
        let expected =
            SqlExecutionErrorDescriptor::for_kind(SqlExecutionErrorKind::InvalidFunctionArgument);
        assert_eq!(
            expected.descriptor.code.as_str(),
            "query.execution.invalid_function_argument"
        );
        assert_eq!((expected.errno, expected.sqlstate), (1105, "HY000"));
        assert_eq!(expected.descriptor.phase, ErrorPhase::Execution);
        assert_eq!(expected.descriptor.status, ErrorCodeStatus::Active);
        assert!(EXECUTION_ERROR_CODE_DESCRIPTORS.contains(&expected.descriptor));
        assert_eq!(
            execution_descriptor_for_code(expected.descriptor.code),
            Some(expected)
        );
        for detail in [
            "unrelated numeric overflow",
            "invalid encoding",
            "invalid function argument",
        ] {
            let fact = FailureFact::new(FailureClass::Data, reason);
            let input = ExecutionFailureInput::new(
                fact,
                SafeDetail::truncating(detail),
                ExecutionFailureSite::Coordinator,
            );
            let copied = input.clone();
            assert_eq!(copied.fact(), fact);
            assert_eq!(copied.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(copied.detail(), input.detail());
            assert_eq!(copied.descriptor(), expected);
            assert_eq!(
                copied.attempt_class(),
                AttemptFailureClass::ExecutionFailure
            );
            assert_eq!(copied.user_error().retry(), RetryClass::Never);
        }
        for (class, expected_code) in [
            (
                FailureClass::Representation,
                "query.execution.representation",
            ),
            (FailureClass::Invariant, "query.execution.invariant"),
            (FailureClass::Control, "query.execution.control"),
            (FailureClass::Dependency, "query.execution.dependency"),
            (FailureClass::Unexpected, "query.execution.unexpected"),
            (FailureClass::Resource, "query.execution.resource_exhausted"),
            (FailureClass::Unsupported, "query.execution.unsupported"),
        ] {
            let fact = FailureFact::new(class, reason);
            let input = ExecutionFailureInput::new(
                fact,
                SafeDetail::truncating("invalid function argument"),
                ExecutionFailureSite::Coordinator,
            );
            assert_eq!(input.fact(), fact);
            assert_eq!(input.descriptor().descriptor.code.as_str(), expected_code);
        }
        for (reason, expected_code) in [
            (
                FailureReason::new(domain::EXECUTION, NonZeroU16::new(45).unwrap()),
                "query.execution.invalid_data",
            ),
            (
                FailureReason::new(domain::KERNEL, NonZeroU16::new(40).unwrap()),
                "query.execution.invalid_data",
            ),
            (
                novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT,
                "query.execution.invalid_cast_argument",
            ),
            (
                novarocks_function_contract::kernel_failure::NUMERIC_OUT_OF_RANGE,
                "query.execution.numeric_out_of_range",
            ),
            (
                novarocks_function_contract::kernel_failure::DIVISION_BY_ZERO,
                "query.execution.division_by_zero",
            ),
        ] {
            let fact = FailureFact::new(FailureClass::Data, reason);
            let input = ExecutionFailureInput::new(
                fact,
                SafeDetail::truncating("invalid function argument"),
                ExecutionFailureSite::Coordinator,
            );
            assert_eq!(input.fact(), fact);
            assert_eq!(input.descriptor().descriptor.code.as_str(), expected_code);
        }
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

    #[test]
    fn division_by_zero_requires_the_kernel_data_reason() {
        let reason = novarocks_function_contract::kernel_failure::DIVISION_BY_ZERO;
        for (class, detail, expected_code) in [
            (
                FailureClass::Data,
                "numeric overflow",
                "query.execution.division_by_zero",
            ),
            (
                FailureClass::Representation,
                "division by zero",
                "query.execution.representation",
            ),
            (
                FailureClass::Invariant,
                "division by zero",
                "query.execution.invariant",
            ),
        ] {
            let input = ExecutionFailureInput::new(
                FailureFact::new(class, reason),
                SafeDetail::truncating(detail),
                ExecutionFailureSite::Coordinator,
            );
            assert_eq!(input.descriptor().descriptor.code.as_str(), expected_code);
            if class == FailureClass::Data {
                assert_eq!(
                    (input.descriptor().errno, input.descriptor().sqlstate),
                    (1365, "22012")
                );
            }
        }
        let ordinary = ExecutionFailureInput::new(
            FailureFact::new(
                FailureClass::Data,
                novarocks_function_contract::kernel_failure::NUMERIC_OUT_OF_RANGE,
            ),
            SafeDetail::truncating("division by zero"),
            ExecutionFailureSite::Coordinator,
        );
        assert_eq!(ordinary.descriptor().errno, 1690);
    }
}
