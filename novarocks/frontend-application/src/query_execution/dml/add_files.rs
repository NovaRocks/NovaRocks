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

//! Transitional reverse port for frontend-owned `ALTER TABLE … ADD FILES`.
//!
//! The opaque prepared handle owns the one exact-generation
//! [`DataMutationSession`]. In particular, it keeps the original connector
//! request context and lease alive for one same-statement, read-only
//! adjudication. This module intentionally exposes only provider-neutral
//! stack facts, never a catalog client, table handle, or private manifest.

use std::any::Any;
use std::sync::{Arc, Mutex};

use novarocks_failure_contract::{
    BoundedDetail, FailureProjection, SharedFailureCause, connector_reason,
};
use novarocks_spi::connector::{
    ConnectorAddFilesFormat, ConnectorAddFilesRecursion, ConnectorError, ConnectorErrorKind,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorMutationOperationId,
    ConnectorPayloadRetentionGuard, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization,
};

use crate::connector::data_mutation::{
    CompletedDataMutation, DataMutationDispatchState, DataMutationIntent, DataMutationSession,
    KnownUncommittedDataMutation, ResolvedDataMutation,
};
use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::QueryExecutionContext;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AddFilesArguments {
    pub location: Option<Arc<str>>,
    pub format: Option<ConnectorAddFilesFormat>,
    pub recursion: ConnectorAddFilesRecursion,
}

/// Lowered table and source location from TABLE EXECUTE argument binding.
///
/// Table resolution remains a frontend concern; this command deliberately
/// carries no parser-private AST or raw SQL source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddFilesCommand {
    pub table_parts: Vec<String>,
    pub arguments: AddFilesArguments,
}

pub(crate) struct ResolvedAddFiles {
    pub(crate) target: crate::catalog_application::resolver::TargetBackend,
    pub(crate) planning: novarocks_spi::connector::ConnectorControlPlanningPin,
    pub(crate) arguments: AddFilesArguments,
}

pub struct PlanAddFilesRequest {
    pub command: AddFilesCommand,
    pub(crate) retained: Option<(
        crate::catalog_application::resolver::TargetBackend,
        novarocks_spi::connector::ConnectorControlPlanningPin,
    )>,
    pub mutation_operation_id: [u8; 16],
    pub query_options: Option<QueryOptions>,
    /// This is the immutable execution context admitted by the frontend. The
    /// engine derives the connector context from it once and never recaptures
    /// deadline, cancellation, topology, or optimizer settings.
    pub execution: QueryExecutionContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddFilesFailure {
    cause: SharedFailureCause,
    interrupted: bool,
}

impl AddFilesFailure {
    pub(crate) fn from_connector(error: ConnectorError) -> Self {
        let detail = authored_procedure_detail(&error).map(BoundedDetail::truncating);
        let interrupted = error.kind() == ConnectorErrorKind::Interrupted;
        let cause = match detail {
            Some(detail) => SharedFailureCause::from_bounded_detail(error, detail),
            None => SharedFailureCause::new(error),
        };
        Self { cause, interrupted }
    }

    pub(crate) fn into_service(
        self,
    ) -> novarocks_query_application::session_error::QueryServiceError {
        let control = self
            .interrupted
            .then(|| novarocks_type_contract::CompileControlError::Source(self.cause.clone()));
        novarocks_query_application::session_error::QueryServiceError::from_failure_cause(
            self.cause,
        )
        .with_compile_control(control)
    }
}

fn authored_procedure_detail(error: &ConnectorError) -> Option<&str> {
    use connector_reason::*;
    // These reasons carry provider-authored procedure diagnostics. For other
    // connector errors require the producer's explicit public projection.
    match error.failure_fact().reason() {
        ADD_FILES_DISABLED
        | PROCEDURE_ARGUMENT_INVALID
        | LOCATION_NOT_FOUND
        | SOURCE_TYPE_MISMATCH
        | NOT_NULL_VIOLATION
        | ICEBERG_COMMIT_ERROR
        | ICEBERG_COMMIT_STATE_UNKNOWN
        | COMMIT_TRANSACTION_CONFLICT => Some(
            error
                .public_failure_detail()
                .unwrap_or_else(|| error.message()),
        ),
        _ => error.public_failure_detail(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddFilesDispatchState {
    ConfirmedNotDispatched,
    PossiblyDispatched,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddFilesPlanError {
    KnownUncommitted(AddFilesFailure),
    ContractFailure {
        failure: AddFilesFailure,
        dispatch: AddFilesDispatchState,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AddFilesPlanSummary {
    pub file_count: u32,
    pub row_count: u64,
    pub total_bytes: u64,
}

/// Provider-neutral facts retained only by this statement. `public_plan_wire`
/// is the canonical SPI wire, whose provider payload remains opaque to the
/// frontend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddFilesPlanFacts {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub provider_id: String,
    pub instance_id: String,
    pub incarnation: [u8; 16],
    pub mutation_operation_id: [u8; 16],
    pub request_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub public_plan_wire: Vec<u8>,
}

pub trait AddFilesPrepared: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct PreparedAddFiles {
    pub facts: AddFilesPlanFacts,
    pub handle: Arc<dyn AddFilesPrepared>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddFilesEffect {
    Applied,
    NoOp,
}

/// The canonical SPI receipt wire stays on the statement stack.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddFilesReceipt {
    pub provider_id: String,
    pub instance_id: String,
    pub incarnation: [u8; 16],
    pub mutation_operation_id: [u8; 16],
    pub operation_kind: String,
    pub request_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub summary: AddFilesPlanSummary,
    pub public_receipt_wire: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddFilesFinalization {
    Complete,
    Failed(AddFilesFailure),
}

/// Lossless SPI-owned evidence encoding retained for exact-session
/// adjudication only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddFilesEvidence {
    pub schema_version: u16,
    pub digest: [u8; 32],
    pub wire_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddFilesOutcome {
    KnownCommitted {
        effect: AddFilesEffect,
        receipt: AddFilesReceipt,
        finalization: AddFilesFinalization,
    },
    KnownUncommitted {
        failure: AddFilesFailure,
    },
    CommitUnknown {
        failure: AddFilesFailure,
        evidence: AddFilesEvidence,
    },
    ContractFailure {
        failure: AddFilesFailure,
        dispatch: AddFilesDispatchState,
    },
}

/// One-to-one core capability used only by the frontend ADD FILES owner.
/// It is deliberately statement-specific rather than a generic DML SPI.
pub trait AddFilesEngine: Send + Sync {
    fn plan_add_files(
        &self,
        request: PlanAddFilesRequest,
    ) -> Result<PreparedAddFiles, AddFilesPlanError>;

    fn execute_add_files(
        &self,
        prepared: &dyn AddFilesPrepared,
        byte_author: ConnectorPayloadRetentionGuard,
    ) -> AddFilesOutcome;

    fn adjudicate_add_files(
        &self,
        prepared: &dyn AddFilesPrepared,
        evidence: &AddFilesEvidence,
    ) -> AddFilesOutcome;
}

struct CorePreparedAddFiles {
    session: Mutex<DataMutationSession>,
}

impl AddFilesPrepared for CorePreparedAddFiles {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl AddFilesEngine for DmlExecutionKernel {
    fn plan_add_files(
        &self,
        request: PlanAddFilesRequest,
    ) -> Result<PreparedAddFiles, AddFilesPlanError> {
        let (target, planning) = request.retained.ok_or_else(|| {
            plan_connector_failure(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "ADD FILES has no retained table procedure binding",
            ))
        })?;
        if target.provider_id.as_str() != "iceberg" {
            return Err(plan_string_failure(format!(
                "ADD FILES only supports iceberg tables: {}.{}",
                target.namespace, target.table
            )));
        }
        let descriptor = planning.binding().descriptor();
        if descriptor.instance_id.as_str() != target.catalog
            || descriptor.provider_id != target.provider_id
        {
            return Err(plan_connector_failure(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "ADD FILES target differs from its retained binding",
            )));
        }
        let connector_context = self
            .connector_request_context_for_execution(
                request.query_options.as_ref(),
                &request.execution,
            )
            .map_err(plan_string_failure)?;
        crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_pin_and_context(
            self.mv_storage_observation().as_ref(),
            &planning,
            self.mv_readiness().as_ref(),
            &target,
            crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Insert,
            connector_context.clone(),
        )
        .map_err(plan_string_failure)?;
        let instance_id = descriptor.instance_id.clone();
        let table = novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: target.namespace.clone().into(),
            table: target.table.clone().into(),
        };
        let operation = ConnectorMutationOperationId::from_bytes(request.mutation_operation_id);
        let arguments = request.command.arguments;
        let intent = DataMutationIntent::register_existing_files(
            arguments.location,
            arguments.format,
            arguments.recursion,
        );
        let lease = self
            .connector_control()
            .acquire_exact_data_mutation(planning.control_runtime_id())
            .map_err(plan_connector_failure)?;
        let session = DataMutationSession::plan_on_session(
            lease,
            operation,
            table,
            intent,
            connector_context,
        )
        .map_err(project_plan_error)?;
        let descriptor = session.descriptor_ref();
        let plan = session.plan_ref();
        let public_plan_wire =
            plan.try_to_wire_v1()
                .map_err(|error| AddFilesPlanError::ContractFailure {
                    failure: AddFilesFailure::from_connector(error),
                    dispatch: AddFilesDispatchState::ConfirmedNotDispatched,
                })?;
        let facts = AddFilesPlanFacts {
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
            provider_id: descriptor.provider_id.as_str().to_string(),
            instance_id: descriptor.instance_id.as_str().to_string(),
            incarnation: plan.owner().incarnation.to_bytes(),
            mutation_operation_id: plan.operation_id().to_bytes(),
            request_digest: plan.request_digest(),
            plan_digest: plan.plan_digest(),
            state_digest: plan.state_digest(),
            public_plan_wire: public_plan_wire.to_vec(),
        };
        Ok(PreparedAddFiles {
            facts,
            handle: Arc::new(CorePreparedAddFiles {
                session: Mutex::new(session),
            }),
        })
    }

    fn execute_add_files(
        &self,
        prepared: &dyn AddFilesPrepared,
        byte_author: ConnectorPayloadRetentionGuard,
    ) -> AddFilesOutcome {
        let prepared = match downcast_prepared(prepared) {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        let mut session = match prepared.session.lock() {
            Ok(session) => session,
            Err(_) => return poisoned_session(),
        };
        project_outcome(session.execute_registration_once(byte_author))
    }

    fn adjudicate_add_files(
        &self,
        prepared: &dyn AddFilesPrepared,
        evidence: &AddFilesEvidence,
    ) -> AddFilesOutcome {
        let prepared = match downcast_prepared(prepared) {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        let evidence = match ExternalMutationEvidence::try_from_wire_v1(&evidence.wire_bytes) {
            Ok(decoded)
                if decoded.schema_version() == evidence.schema_version
                    && decoded.digest() == evidence.digest =>
            {
                decoded
            }
            Ok(_) => {
                return contract_outcome(
                    ConnectorErrorKind::CorruptData,
                    "ADD FILES evidence digest or schema version does not match its wire bytes",
                    AddFilesDispatchState::ConfirmedNotDispatched,
                );
            }
            Err(error) => return project_contract_error(error, false),
        };
        let mut session = match prepared.session.lock() {
            Ok(session) => session,
            Err(_) => return poisoned_session(),
        };
        project_outcome(session.adjudicate_once(evidence))
    }
}

#[expect(
    clippy::result_large_err,
    reason = "The externally visible ADD FILES outcome preserves its typed failure payload."
)]
fn downcast_prepared(
    prepared: &dyn AddFilesPrepared,
) -> Result<&CorePreparedAddFiles, AddFilesOutcome> {
    prepared
        .as_any()
        .downcast_ref::<CorePreparedAddFiles>()
        .ok_or_else(|| {
            contract_outcome(
                ConnectorErrorKind::InvalidRequest,
                "foreign ADD FILES prepared handle",
                AddFilesDispatchState::ConfirmedNotDispatched,
            )
        })
}

fn poisoned_session() -> AddFilesOutcome {
    contract_outcome(
        ConnectorErrorKind::Internal,
        "ADD FILES prepared session lock is poisoned",
        AddFilesDispatchState::ConfirmedNotDispatched,
    )
}

fn plan_string_failure(message: String) -> AddFilesPlanError {
    plan_connector_failure(ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        message,
    ))
}

fn plan_connector_failure(error: ConnectorError) -> AddFilesPlanError {
    AddFilesPlanError::KnownUncommitted(AddFilesFailure::from_connector(error))
}

fn project_plan_error(outcome: ResolvedDataMutation) -> AddFilesPlanError {
    match outcome {
        ResolvedDataMutation::KnownUncommitted { failure, cleanup } => {
            AddFilesPlanError::KnownUncommitted(project_uncommitted(failure, cleanup))
        }
        ResolvedDataMutation::ContractFailure { error, dispatch } => {
            AddFilesPlanError::ContractFailure {
                failure: AddFilesFailure::from_connector(error),
                dispatch: project_dispatch(dispatch),
            }
        }
        ResolvedDataMutation::KnownCommitted(_) | ResolvedDataMutation::CommitUnknown { .. } => {
            AddFilesPlanError::ContractFailure {
                failure: AddFilesFailure::from_connector(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "ADD FILES planning returned a post-dispatch outcome",
                )),
                dispatch: AddFilesDispatchState::ConfirmedNotDispatched,
            }
        }
    }
}

fn project_outcome(outcome: ResolvedDataMutation) -> AddFilesOutcome {
    match outcome {
        ResolvedDataMutation::KnownCommitted(completed) => project_committed(*completed),
        ResolvedDataMutation::KnownUncommitted { failure, cleanup } => {
            AddFilesOutcome::KnownUncommitted {
                failure: project_uncommitted(failure, cleanup),
            }
        }
        ResolvedDataMutation::CommitUnknown { failure, evidence } => {
            match evidence.try_to_wire_v1() {
                Ok(wire) => AddFilesOutcome::CommitUnknown {
                    failure: project_mutation_failure(failure),
                    evidence: AddFilesEvidence {
                        schema_version: evidence.schema_version(),
                        digest: evidence.digest(),
                        wire_bytes: wire.to_vec(),
                    },
                },
                Err(error) => project_contract_error(error, true),
            }
        }
        ResolvedDataMutation::ContractFailure { error, dispatch } => {
            AddFilesOutcome::ContractFailure {
                failure: AddFilesFailure::from_connector(error),
                dispatch: project_dispatch(dispatch),
            }
        }
    }
}

fn project_committed(completed: CompletedDataMutation) -> AddFilesOutcome {
    let summary = completed.receipt.summary();
    let receipt = match completed.receipt.try_to_wire_v1() {
        Ok(wire) => AddFilesReceipt {
            provider_id: completed
                .receipt
                .descriptor()
                .provider_id
                .as_str()
                .to_string(),
            instance_id: completed
                .receipt
                .descriptor()
                .instance_id
                .as_str()
                .to_string(),
            incarnation: completed.receipt.incarnation().to_bytes(),
            mutation_operation_id: completed.receipt.operation_id().to_bytes(),
            operation_kind: completed.receipt.operation_kind().to_string(),
            request_digest: completed.receipt.request_digest(),
            plan_digest: completed.receipt.plan_digest(),
            state_digest: completed.receipt.state_digest(),
            summary: AddFilesPlanSummary {
                file_count: summary.file_count(),
                row_count: summary.row_count(),
                total_bytes: summary.total_bytes(),
            },
            public_receipt_wire: wire.to_vec(),
        },
        Err(error) => return project_contract_error(error, true),
    };
    AddFilesOutcome::KnownCommitted {
        effect: match completed.effect {
            ExternalMutationEffect::Applied => AddFilesEffect::Applied,
            ExternalMutationEffect::NoOp => AddFilesEffect::NoOp,
        },
        receipt,
        finalization: match completed.finalization {
            ExternalMutationFinalization::Complete => AddFilesFinalization::Complete,
            ExternalMutationFinalization::Failed(failure) => {
                AddFilesFinalization::Failed(project_mutation_failure(failure))
            }
        },
    }
}

fn project_uncommitted(
    failure: KnownUncommittedDataMutation,
    cleanup: novarocks_spi::connector::ExternalMutationFinalization,
) -> AddFilesFailure {
    if let ExternalMutationFinalization::Failed(error) = cleanup {
        tracing::warn!(%error, "uncommitted ADD FILES retains incomplete cleanup");
    }
    match failure {
        KnownUncommittedDataMutation::Planning(error) => AddFilesFailure::from_connector(error),
        KnownUncommittedDataMutation::Provider(failure) => project_mutation_failure(failure),
    }
}

fn project_mutation_failure(failure: ConnectorMutationFailure) -> AddFilesFailure {
    let interrupted = failure.kind() == ConnectorMutationFailureKind::Interrupted;
    let detail = failure
        .connector_source()
        .and_then(authored_procedure_detail)
        .map(BoundedDetail::truncating);
    let cause = match detail {
        Some(detail) => SharedFailureCause::from_bounded_detail(failure, detail),
        None => SharedFailureCause::new(failure),
    };
    AddFilesFailure { cause, interrupted }
}

fn project_dispatch(dispatch: DataMutationDispatchState) -> AddFilesDispatchState {
    match dispatch {
        DataMutationDispatchState::ConfirmedNotDispatched => {
            AddFilesDispatchState::ConfirmedNotDispatched
        }
        DataMutationDispatchState::PossiblyDispatched => AddFilesDispatchState::PossiblyDispatched,
    }
}

fn project_contract_error(error: ConnectorError, possibly_dispatched: bool) -> AddFilesOutcome {
    AddFilesOutcome::ContractFailure {
        failure: AddFilesFailure::from_connector(error),
        dispatch: if possibly_dispatched {
            AddFilesDispatchState::PossiblyDispatched
        } else {
            AddFilesDispatchState::ConfirmedNotDispatched
        },
    }
}

fn contract_outcome(
    kind: ConnectorErrorKind,
    message: impl Into<String>,
    dispatch: AddFilesDispatchState,
) -> AddFilesOutcome {
    AddFilesOutcome::ContractFailure {
        failure: AddFilesFailure::from_connector(ConnectorError::new(kind, message)),
        dispatch,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorDataMutationPlanSummary, ConnectorDataMutationReceipt,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        ProviderBindingEpoch,
    };

    use super::*;

    #[test]
    fn reverse_port_is_object_safe() {
        fn accepts_object_safe_port(_: &dyn AddFilesEngine) {}
        let _ = accepts_object_safe_port;
    }

    #[test]
    fn failed_plan_wire_is_pre_dispatch_contract_failure() {
        let error = project_plan_error(ResolvedDataMutation::ContractFailure {
            error: ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "ADD FILES plan could not be encoded",
            ),
            dispatch: DataMutationDispatchState::ConfirmedNotDispatched,
        });
        assert!(matches!(
            error,
            AddFilesPlanError::ContractFailure {
                dispatch: AddFilesDispatchState::ConfirmedNotDispatched,
                ..
            }
        ));
    }

    #[test]
    fn finalization_failure_preserves_known_committed_external_truth() {
        let receipt = ConnectorDataMutationReceipt::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: ConnectorInstanceId::parse("catalog").expect("instance"),
            },
            ProviderBindingEpoch::from_bytes([1; 16]),
            ConnectorMutationOperationId::from_bytes([2; 16]),
            novarocks_spi::connector::REGISTER_EXISTING_FILES_KIND,
            [3; 32],
            [4; 32],
            [5; 32],
            ConnectorDataMutationPlanSummary::new(7, 11, 13),
            Bytes::from_static(b"provider-receipt"),
        )
        .expect("receipt");
        let outcome = project_committed(CompletedDataMutation {
            cleanup: ExternalMutationFinalization::Complete,
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization: ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Internal,
                "generic cache finalization failed",
            )),
        });
        assert!(matches!(
            outcome,
            AddFilesOutcome::KnownCommitted {
                finalization: AddFilesFinalization::Failed(_),
                ..
            }
        ));
    }
}
