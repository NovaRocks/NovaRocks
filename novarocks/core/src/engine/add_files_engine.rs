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
//! [`DataMutationSession`].  In particular, it keeps the original connector
//! request context and lease alive through the frontend's durable evidence
//! barrier.  This module intentionally exposes only provider-neutral durable
//! facts, never a catalog client, table handle, or private manifest.

use std::any::Any;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{
    ConnectorDataMutationSourceScope, ConnectorError, ConnectorErrorKind, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorMutationOperationId, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization,
};

use crate::connector::data_mutation::{
    CompletedDataMutation, DataMutationDispatchState, DataMutationIntent, DataMutationSession,
    KnownUncommittedDataMutation, ResolvedDataMutation,
};
use crate::engine::domain::DmlExecutionKernel;
use crate::query_execution::request_context::QueryExecutionContext;
use crate::sql::parser::dialect::add_files::classify_add_files;
use novarocks_execution::runtime::query_options::QueryOptions;

pub use crate::sql::parser::dialect::add_files::AddFilesCommand;

pub struct PlanAddFilesRequest {
    pub command: AddFilesCommand,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub mutation_operation_id: [u8; 16],
    pub query_options: Option<QueryOptions>,
    /// This is the immutable execution context admitted by the frontend. The
    /// engine derives the connector context from it once and never recaptures
    /// deadline, cancellation, topology, or optimizer settings.
    pub execution: QueryExecutionContext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddFilesFailureKind {
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Unauthenticated,
    PermissionDenied,
    Unsupported,
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    Unavailable,
    CorruptData,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddFilesFailure {
    pub kind: AddFilesFailureKind,
    pub message: String,
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

/// Provider-neutral facts that the frontend must durably persist before it
/// fences dispatch. `public_plan_wire` is the canonical SPI wire, whose
/// provider payload remains opaque to the frontend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddFilesPlanFacts {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub source_location: String,
    pub provider_id: String,
    pub instance_id: String,
    pub incarnation: [u8; 16],
    pub mutation_operation_id: [u8; 16],
    pub request_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub summary: AddFilesPlanSummary,
    pub source_scope: ConnectorDataMutationSourceScope,
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

/// The canonical SPI receipt wire is preserved as an opaque durable artifact.
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

/// Lossless SPI-owned evidence encoding. Frontend stores and returns these
/// bytes unchanged; decoding is only an exact-session safety check here.
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
    fn classify_add_files(&self, sql: &str) -> Result<Option<AddFilesCommand>, String>;

    fn plan_add_files(
        &self,
        request: PlanAddFilesRequest,
    ) -> Result<PreparedAddFiles, AddFilesPlanError>;

    /// Establish this attempt's external fence before dispatch and return the
    /// provider receipt that acknowledges the published marker.
    ///
    /// The default fails closed: an engine that cannot expose its mutation
    /// authority must not register files into a table.
    fn establish_add_files_external_fence(
        &self,
        _prepared: &dyn AddFilesPrepared,
        _fence: novarocks_spi::connector::ConnectorExternalOperationFence,
    ) -> Result<
        novarocks_spi::connector::ConnectorExternalFenceReceipt,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(
            crate::engine::external_write_fence::external_fence_authority_unavailable(
                "ADD FILES engine does not expose an external operation fence authority",
            ),
        )
    }

    fn execute_add_files(&self, prepared: &dyn AddFilesPrepared) -> AddFilesOutcome;

    fn reconcile_add_files(
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
    fn establish_add_files_external_fence(
        &self,
        prepared: &dyn AddFilesPrepared,
        fence: novarocks_spi::connector::ConnectorExternalOperationFence,
    ) -> Result<
        novarocks_spi::connector::ConnectorExternalFenceReceipt,
        novarocks_spi::connector::ConnectorError,
    > {
        let prepared = downcast_prepared(prepared).map_err(|_| {
            crate::engine::external_write_fence::invalid_fence_request(
                "foreign ADD FILES prepared handle".to_string(),
            )
        })?;
        let session = prepared.session.lock().map_err(|error| {
            crate::engine::external_write_fence::invalid_fence_request(format!(
                "ADD FILES prepared session lock: {error}"
            ))
        })?;
        session.establish_external_fence(fence)
    }

    fn classify_add_files(&self, sql: &str) -> Result<Option<AddFilesCommand>, String> {
        classify_add_files(sql)
    }

    fn plan_add_files(
        &self,
        request: PlanAddFilesRequest,
    ) -> Result<PreparedAddFiles, AddFilesPlanError> {
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            self,
            &crate::sql::parser::ast::ObjectName {
                parts: request.command.table_parts,
            },
            request.current_catalog.as_deref(),
            &request.current_database,
        )
        .map_err(plan_string_failure)?;
        if target.backend_name != "iceberg" {
            return Err(plan_string_failure(format!(
                "ADD FILES only supports iceberg tables: {}.{}",
                target.namespace, target.table
            )));
        }
        crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
            self.connector_control().as_ref(),
            self.mv_storage_observation().as_ref(),
            &target,
            crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Insert,
        )
        .map_err(plan_string_failure)?;
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )
        .map_err(plan_string_failure)?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(plan_connector_failure)?;
        let session = DataMutationSession::plan(
            self.connector_control().as_ref(),
            &instance_id,
            ConnectorMutationOperationId::from_bytes(request.mutation_operation_id),
            novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: target.namespace.clone().into(),
                table: target.table.clone().into(),
            },
            DataMutationIntent::register_existing_files(request.command.location.clone()),
            connector_context,
        )
        .map_err(project_plan_error)?;
        let descriptor = session.descriptor_ref();
        let plan = session.plan_ref();
        let source_scope =
            plan.source_scope()
                .ok_or_else(|| AddFilesPlanError::ContractFailure {
                    failure: AddFilesFailure {
                        kind: AddFilesFailureKind::CorruptData,
                        message: "ADD FILES plan did not contain a source ownership scope"
                            .to_string(),
                    },
                    dispatch: AddFilesDispatchState::ConfirmedNotDispatched,
                })?;
        let public_plan_wire =
            plan.try_to_wire_v1()
                .map_err(|error| AddFilesPlanError::ContractFailure {
                    failure: project_connector_error(error),
                    dispatch: AddFilesDispatchState::ConfirmedNotDispatched,
                })?;
        let summary = plan.summary();
        let facts = AddFilesPlanFacts {
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
            source_location: request.command.location,
            provider_id: descriptor.provider_id.as_str().to_string(),
            instance_id: descriptor.instance_id.as_str().to_string(),
            incarnation: plan.owner().incarnation.to_bytes(),
            mutation_operation_id: plan.operation_id().to_bytes(),
            request_digest: plan.request_digest(),
            plan_digest: plan.plan_digest(),
            state_digest: plan.state_digest(),
            summary: AddFilesPlanSummary {
                file_count: summary.file_count(),
                row_count: summary.row_count(),
                total_bytes: summary.total_bytes(),
            },
            source_scope,
            public_plan_wire: public_plan_wire.to_vec(),
        };
        Ok(PreparedAddFiles {
            facts,
            handle: Arc::new(CorePreparedAddFiles {
                session: Mutex::new(session),
            }),
        })
    }

    fn execute_add_files(&self, prepared: &dyn AddFilesPrepared) -> AddFilesOutcome {
        let prepared = match downcast_prepared(prepared) {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        let mut session = match prepared.session.lock() {
            Ok(session) => session,
            Err(_) => return poisoned_session(),
        };
        project_outcome(session.execute_once(self))
    }

    fn reconcile_add_files(
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
                    AddFilesFailureKind::CorruptData,
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
        project_outcome(session.reconcile_once(evidence, self))
    }
}

fn downcast_prepared(
    prepared: &dyn AddFilesPrepared,
) -> Result<&CorePreparedAddFiles, AddFilesOutcome> {
    prepared
        .as_any()
        .downcast_ref::<CorePreparedAddFiles>()
        .ok_or_else(|| {
            contract_outcome(
                AddFilesFailureKind::InvalidRequest,
                "foreign ADD FILES prepared handle",
                AddFilesDispatchState::ConfirmedNotDispatched,
            )
        })
}

fn poisoned_session() -> AddFilesOutcome {
    contract_outcome(
        AddFilesFailureKind::Internal,
        "ADD FILES prepared session lock is poisoned",
        AddFilesDispatchState::ConfirmedNotDispatched,
    )
}

fn plan_string_failure(message: String) -> AddFilesPlanError {
    AddFilesPlanError::KnownUncommitted(AddFilesFailure {
        kind: AddFilesFailureKind::InvalidRequest,
        message,
    })
}

fn plan_connector_failure(error: ConnectorError) -> AddFilesPlanError {
    AddFilesPlanError::KnownUncommitted(project_connector_error(error))
}

fn project_plan_error(outcome: ResolvedDataMutation) -> AddFilesPlanError {
    match outcome {
        ResolvedDataMutation::KnownUncommitted { failure } => {
            AddFilesPlanError::KnownUncommitted(project_uncommitted(failure))
        }
        ResolvedDataMutation::ContractFailure { error, dispatch } => {
            AddFilesPlanError::ContractFailure {
                failure: project_connector_error(error),
                dispatch: project_dispatch(dispatch),
            }
        }
        ResolvedDataMutation::KnownCommitted(_) | ResolvedDataMutation::CommitUnknown { .. } => {
            AddFilesPlanError::ContractFailure {
                failure: AddFilesFailure {
                    kind: AddFilesFailureKind::Internal,
                    message: "ADD FILES planning returned a post-dispatch outcome".to_string(),
                },
                dispatch: AddFilesDispatchState::ConfirmedNotDispatched,
            }
        }
    }
}

fn project_outcome(outcome: ResolvedDataMutation) -> AddFilesOutcome {
    match outcome {
        ResolvedDataMutation::KnownCommitted(completed) => project_committed(completed),
        ResolvedDataMutation::KnownUncommitted { failure } => AddFilesOutcome::KnownUncommitted {
            failure: project_uncommitted(failure),
        },
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
                failure: project_connector_error(error),
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

fn project_uncommitted(failure: KnownUncommittedDataMutation) -> AddFilesFailure {
    match failure {
        KnownUncommittedDataMutation::Planning(error) => project_connector_error(error),
        KnownUncommittedDataMutation::Provider(failure) => project_mutation_failure(failure),
    }
}

fn project_connector_error(error: ConnectorError) -> AddFilesFailure {
    AddFilesFailure {
        kind: match error.kind() {
            ConnectorErrorKind::InvalidRequest => AddFilesFailureKind::InvalidRequest,
            ConnectorErrorKind::NotFound => AddFilesFailureKind::NotFound,
            ConnectorErrorKind::PermissionDenied => AddFilesFailureKind::PermissionDenied,
            ConnectorErrorKind::Unsupported => AddFilesFailureKind::Unsupported,
            ConnectorErrorKind::Cancelled => AddFilesFailureKind::Cancelled,
            ConnectorErrorKind::DeadlineExceeded => AddFilesFailureKind::DeadlineExceeded,
            ConnectorErrorKind::ResourceExhausted => AddFilesFailureKind::ResourceExhausted,
            ConnectorErrorKind::Unavailable => AddFilesFailureKind::Unavailable,
            ConnectorErrorKind::CorruptData => AddFilesFailureKind::CorruptData,
            ConnectorErrorKind::Internal => AddFilesFailureKind::Internal,
        },
        message: error.to_string(),
    }
}

fn project_mutation_failure(failure: ConnectorMutationFailure) -> AddFilesFailure {
    AddFilesFailure {
        kind: match failure.kind() {
            ConnectorMutationFailureKind::InvalidRequest => AddFilesFailureKind::InvalidRequest,
            ConnectorMutationFailureKind::NotFound => AddFilesFailureKind::NotFound,
            ConnectorMutationFailureKind::AlreadyExists => AddFilesFailureKind::AlreadyExists,
            ConnectorMutationFailureKind::Conflict => AddFilesFailureKind::Conflict,
            ConnectorMutationFailureKind::Unauthenticated => AddFilesFailureKind::Unauthenticated,
            ConnectorMutationFailureKind::PermissionDenied => AddFilesFailureKind::PermissionDenied,
            ConnectorMutationFailureKind::Unsupported => AddFilesFailureKind::Unsupported,
            ConnectorMutationFailureKind::Cancelled => AddFilesFailureKind::Cancelled,
            ConnectorMutationFailureKind::DeadlineExceeded => AddFilesFailureKind::DeadlineExceeded,
            ConnectorMutationFailureKind::ResourceExhausted => {
                AddFilesFailureKind::ResourceExhausted
            }
            ConnectorMutationFailureKind::Unavailable => AddFilesFailureKind::Unavailable,
            ConnectorMutationFailureKind::CorruptData => AddFilesFailureKind::CorruptData,
            ConnectorMutationFailureKind::Internal => AddFilesFailureKind::Internal,
        },
        message: failure.message().to_string(),
    }
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
        failure: project_connector_error(error),
        dispatch: if possibly_dispatched {
            AddFilesDispatchState::PossiblyDispatched
        } else {
            AddFilesDispatchState::ConfirmedNotDispatched
        },
    }
}

fn contract_outcome(
    kind: AddFilesFailureKind,
    message: impl Into<String>,
    dispatch: AddFilesDispatchState,
) -> AddFilesOutcome {
    AddFilesOutcome::ContractFailure {
        failure: AddFilesFailure {
            kind,
            message: message.into(),
        },
        dispatch,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorDataMutationPlanSummary, ConnectorDataMutationReceipt,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorProviderId,
    };

    use super::*;

    #[test]
    fn reverse_port_is_object_safe() {
        fn accepts_object_safe_port(_: &dyn AddFilesEngine) {}
        let _ = accepts_object_safe_port;
    }

    #[test]
    fn classifier_has_no_engine_side_effects() {
        assert_eq!(classify_add_files("SELECT 1").unwrap(), None);
    }

    #[test]
    fn missing_source_scope_is_pre_dispatch_contract_failure() {
        let error = project_plan_error(ResolvedDataMutation::ContractFailure {
            error: ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "ADD FILES plan did not contain a source ownership scope",
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
            ConnectorInstanceIncarnation::from_bytes([1; 16]),
            ConnectorMutationOperationId::from_bytes([2; 16]),
            novarocks_spi::connector::REGISTER_EXISTING_FILES_KIND,
            [3; 32],
            [4; 32],
            [5; 32],
            ConnectorDataMutationPlanSummary::try_new(7, 11, 13).expect("summary"),
            Bytes::from_static(b"provider-receipt"),
        )
        .expect("receipt");
        let outcome = project_committed(CompletedDataMutation {
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
                finalization: AddFilesFinalization::Failed(AddFilesFailure {
                    kind: AddFilesFailureKind::Internal,
                    ..
                }),
                ..
            }
        ));
    }
}
