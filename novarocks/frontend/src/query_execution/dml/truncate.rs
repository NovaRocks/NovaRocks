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

//! Transitional reverse port for frontend-owned `TRUNCATE TABLE` routing.
//!
//! The opaque prepared handle retains the exact SPI-4C2 lease, plan and
//! request context. Execute and reconcile are separate calls so the frontend
//! can durably record unknown evidence before reconciliation.

use std::any::Any;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization,
};

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks::connector::data_mutation::{
    CompletedDataMutation, DataMutationDispatchState, DataMutationIntent, DataMutationSession,
    KnownUncommittedDataMutation, ResolvedDataMutation,
};
use novarocks_protocol::lifecycle::QueryOptions;
use novarocks_sql::syntax::ObjectName;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TruncateCommand {
    pub target_parts: Vec<String>,
    pub target_ref: String,
}

/// Recognize and validate the syntax of one `TRUNCATE TABLE` command without
/// resolving its catalog or performing any connector operation.
pub fn parse_truncate_command(sql: &str) -> Result<Option<TruncateCommand>, String> {
    novarocks_sql::planning::dml::parse_truncate_command(sql).map(|command| {
        command.map(|command| TruncateCommand {
            target_parts: command.target_parts,
            target_ref: command.target_ref,
        })
    })
}

pub struct PlanTruncateRequest {
    pub command: TruncateCommand,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub mutation_operation_id: [u8; 16],
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TruncateFailureKind {
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
pub struct TruncateFailure {
    pub kind: TruncateFailureKind,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TruncateDispatchState {
    ConfirmedNotDispatched,
    PossiblyDispatched,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TruncatePlanError {
    KnownUncommitted(TruncateFailure),
    ContractFailure {
        failure: TruncateFailure,
        dispatch: TruncateDispatchState,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TruncatePlanSummary {
    pub file_count: u32,
    pub row_count: u64,
    pub total_bytes: u64,
}

/// Provider-neutral facts that the frontend persists before execute.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TruncatePlanFacts {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub target_ref: String,
    pub provider_id: String,
    pub instance_id: String,
    pub incarnation: [u8; 16],
    pub mutation_operation_id: [u8; 16],
    pub request_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub summary: TruncatePlanSummary,
}

pub trait TruncatePrepared: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct PreparedTruncate {
    pub facts: TruncatePlanFacts,
    pub handle: Arc<dyn TruncatePrepared>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TruncateEffect {
    Applied,
    NoOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TruncateReceipt {
    pub provider_id: String,
    pub instance_id: String,
    pub incarnation: [u8; 16],
    pub mutation_operation_id: [u8; 16],
    pub operation_kind: String,
    pub request_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub summary: TruncatePlanSummary,
    pub opaque_payload: Vec<u8>,
    pub opaque_payload_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TruncateFinalization {
    Complete,
    Failed(TruncateFailure),
}

/// Lossless SPI-owned evidence encoding. Frontend persists these opaque bytes
/// and returns them unchanged; it never parses provider-private payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TruncateEvidence {
    pub schema_version: u16,
    pub digest: [u8; 32],
    pub wire_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TruncateOutcome {
    KnownCommitted {
        effect: TruncateEffect,
        receipt: TruncateReceipt,
        finalization: TruncateFinalization,
    },
    KnownUncommitted {
        failure: TruncateFailure,
    },
    CommitUnknown {
        failure: TruncateFailure,
        evidence: TruncateEvidence,
    },
    ContractFailure {
        failure: TruncateFailure,
        dispatch: TruncateDispatchState,
    },
}

/// One-to-one core capability used only by the frontend TRUNCATE owner.
pub trait TruncateEngine: Send + Sync {
    fn classify_truncate(&self, sql: &str) -> Result<Option<TruncateCommand>, String>;

    fn plan_truncate(
        &self,
        request: PlanTruncateRequest,
    ) -> Result<PreparedTruncate, TruncatePlanError>;

    /// Establish this attempt's external fence before dispatch and return the
    /// provider receipt that acknowledges the published marker.
    ///
    /// The default fails closed: an engine that cannot expose its mutation
    /// authority must not run a destructive execute.
    fn establish_truncate_external_fence(
        &self,
        _prepared: &dyn TruncatePrepared,
        _fence: novarocks_spi::connector::ConnectorExternalOperationFence,
    ) -> Result<
        novarocks_spi::connector::ConnectorExternalFenceReceipt,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(
            crate::query_execution::dml::external_write_fence::external_fence_authority_unavailable(
                "TRUNCATE engine does not expose an external operation fence authority",
            ),
        )
    }

    fn execute_truncate(&self, prepared: &dyn TruncatePrepared) -> TruncateOutcome;

    fn reconcile_truncate(
        &self,
        prepared: &dyn TruncatePrepared,
        evidence: &TruncateEvidence,
    ) -> TruncateOutcome;
}

struct CorePreparedTruncate {
    session: Mutex<DataMutationSession>,
}

impl TruncatePrepared for CorePreparedTruncate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl TruncateEngine for DmlExecutionKernel {
    fn establish_truncate_external_fence(
        &self,
        prepared: &dyn TruncatePrepared,
        fence: novarocks_spi::connector::ConnectorExternalOperationFence,
    ) -> Result<
        novarocks_spi::connector::ConnectorExternalFenceReceipt,
        novarocks_spi::connector::ConnectorError,
    > {
        // The frontend seals the fence because only it holds the resource
        // identity: the plan carries the operation id, but the table and target
        // ref live inside the provider's opaque payload.
        let prepared = downcast_prepared(prepared).map_err(|_| {
            crate::query_execution::dml::external_write_fence::invalid_fence_request(
                "foreign TRUNCATE prepared handle".to_string(),
            )
        })?;
        let session = prepared.session.lock().map_err(|error| {
            crate::query_execution::dml::external_write_fence::invalid_fence_request(format!(
                "TRUNCATE prepared session lock: {error}"
            ))
        })?;
        session.establish_external_fence(fence)
    }

    fn classify_truncate(&self, sql: &str) -> Result<Option<TruncateCommand>, String> {
        parse_truncate_command(sql)
    }

    fn plan_truncate(
        &self,
        request: PlanTruncateRequest,
    ) -> Result<PreparedTruncate, TruncatePlanError> {
        if request.command.target_ref.is_empty() {
            return Err(TruncatePlanError::KnownUncommitted(TruncateFailure {
                kind: TruncateFailureKind::InvalidRequest,
                message: "TRUNCATE target ref must not be empty".to_string(),
            }));
        }
        let target = novarocks::catalog_application::resolver::resolve_existing_table_target(
            self,
            &ObjectName {
                parts: request.command.target_parts,
            },
            request.current_catalog.as_deref(),
            &request.current_database,
        )
        .map_err(plan_string_failure)?;
        if target.backend_name != "iceberg" {
            return Err(plan_string_failure(format!(
                "TRUNCATE TABLE only supports iceberg tables: {}.{}",
                target.namespace, target.table
            )));
        }
        novarocks::mv::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
            self.connector_control().as_ref(),
            self.mv_storage_observation().as_ref(),
            &target,
            novarocks::mv::iceberg_guard::IcebergMvUserMutation::Truncate,
        )
        .map_err(plan_string_failure)?;
        let connector_context = novarocks::connector::connector_request_context_for_execution(
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
            DataMutationIntent::truncate(request.command.target_ref.clone()),
            connector_context,
        )
        .map_err(project_plan_error)?;
        let descriptor = session.descriptor_ref();
        let plan = session.plan_ref();
        let summary = plan.summary();
        let facts = TruncatePlanFacts {
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
            target_ref: request.command.target_ref,
            provider_id: descriptor.provider_id.as_str().to_string(),
            instance_id: descriptor.instance_id.as_str().to_string(),
            incarnation: plan.owner().incarnation.to_bytes(),
            mutation_operation_id: plan.operation_id().to_bytes(),
            request_digest: plan.request_digest(),
            plan_digest: plan.plan_digest(),
            state_digest: plan.state_digest(),
            summary: TruncatePlanSummary {
                file_count: summary.file_count(),
                row_count: summary.row_count(),
                total_bytes: summary.total_bytes(),
            },
        };
        Ok(PreparedTruncate {
            facts,
            handle: Arc::new(CorePreparedTruncate {
                session: Mutex::new(session),
            }),
        })
    }

    fn execute_truncate(&self, prepared: &dyn TruncatePrepared) -> TruncateOutcome {
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

    fn reconcile_truncate(
        &self,
        prepared: &dyn TruncatePrepared,
        evidence: &TruncateEvidence,
    ) -> TruncateOutcome {
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
                    TruncateFailureKind::CorruptData,
                    "TRUNCATE evidence digest or schema version does not match its wire bytes",
                    TruncateDispatchState::ConfirmedNotDispatched,
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
    prepared: &dyn TruncatePrepared,
) -> Result<&CorePreparedTruncate, TruncateOutcome> {
    prepared
        .as_any()
        .downcast_ref::<CorePreparedTruncate>()
        .ok_or_else(|| {
            contract_outcome(
                TruncateFailureKind::InvalidRequest,
                "foreign TRUNCATE prepared handle",
                TruncateDispatchState::ConfirmedNotDispatched,
            )
        })
}

fn poisoned_session() -> TruncateOutcome {
    contract_outcome(
        TruncateFailureKind::Internal,
        "TRUNCATE prepared session lock is poisoned",
        TruncateDispatchState::ConfirmedNotDispatched,
    )
}

fn plan_string_failure(message: String) -> TruncatePlanError {
    TruncatePlanError::KnownUncommitted(TruncateFailure {
        kind: TruncateFailureKind::InvalidRequest,
        message,
    })
}

fn plan_connector_failure(error: ConnectorError) -> TruncatePlanError {
    TruncatePlanError::KnownUncommitted(project_connector_error(error))
}

fn project_plan_error(outcome: ResolvedDataMutation) -> TruncatePlanError {
    match outcome {
        ResolvedDataMutation::KnownUncommitted { failure } => {
            TruncatePlanError::KnownUncommitted(project_uncommitted(failure))
        }
        ResolvedDataMutation::ContractFailure { error, dispatch } => {
            TruncatePlanError::ContractFailure {
                failure: project_connector_error(error),
                dispatch: project_dispatch(dispatch),
            }
        }
        ResolvedDataMutation::KnownCommitted(_) | ResolvedDataMutation::CommitUnknown { .. } => {
            TruncatePlanError::ContractFailure {
                failure: TruncateFailure {
                    kind: TruncateFailureKind::Internal,
                    message: "TRUNCATE planning returned a post-dispatch outcome".to_string(),
                },
                dispatch: TruncateDispatchState::ConfirmedNotDispatched,
            }
        }
    }
}

fn project_outcome(outcome: ResolvedDataMutation) -> TruncateOutcome {
    match outcome {
        ResolvedDataMutation::KnownCommitted(completed) => project_committed(completed),
        ResolvedDataMutation::KnownUncommitted { failure } => TruncateOutcome::KnownUncommitted {
            failure: project_uncommitted(failure),
        },
        ResolvedDataMutation::CommitUnknown { failure, evidence } => {
            match evidence.try_to_wire_v1() {
                Ok(wire) => TruncateOutcome::CommitUnknown {
                    failure: project_mutation_failure(failure),
                    evidence: TruncateEvidence {
                        schema_version: evidence.schema_version(),
                        digest: evidence.digest(),
                        wire_bytes: wire.to_vec(),
                    },
                },
                Err(error) => project_contract_error(error, true),
            }
        }
        ResolvedDataMutation::ContractFailure { error, dispatch } => {
            TruncateOutcome::ContractFailure {
                failure: project_connector_error(error),
                dispatch: project_dispatch(dispatch),
            }
        }
    }
}

fn project_committed(completed: CompletedDataMutation) -> TruncateOutcome {
    let summary = completed.receipt.summary();
    let receipt = TruncateReceipt {
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
        summary: TruncatePlanSummary {
            file_count: summary.file_count(),
            row_count: summary.row_count(),
            total_bytes: summary.total_bytes(),
        },
        opaque_payload: completed.receipt.provider_payload().to_vec(),
        opaque_payload_digest: completed.receipt.provider_payload_digest(),
    };
    TruncateOutcome::KnownCommitted {
        effect: match completed.effect {
            ExternalMutationEffect::Applied => TruncateEffect::Applied,
            ExternalMutationEffect::NoOp => TruncateEffect::NoOp,
        },
        receipt,
        finalization: match completed.finalization {
            ExternalMutationFinalization::Complete => TruncateFinalization::Complete,
            ExternalMutationFinalization::Failed(failure) => {
                TruncateFinalization::Failed(project_mutation_failure(failure))
            }
        },
    }
}

fn project_uncommitted(failure: KnownUncommittedDataMutation) -> TruncateFailure {
    match failure {
        KnownUncommittedDataMutation::Planning(error) => project_connector_error(error),
        KnownUncommittedDataMutation::Provider(failure) => project_mutation_failure(failure),
    }
}

fn project_connector_error(error: ConnectorError) -> TruncateFailure {
    TruncateFailure {
        kind: match error.kind() {
            ConnectorErrorKind::InvalidRequest => TruncateFailureKind::InvalidRequest,
            ConnectorErrorKind::NotFound => TruncateFailureKind::NotFound,
            ConnectorErrorKind::PermissionDenied => TruncateFailureKind::PermissionDenied,
            ConnectorErrorKind::Unsupported => TruncateFailureKind::Unsupported,
            ConnectorErrorKind::Cancelled => TruncateFailureKind::Cancelled,
            ConnectorErrorKind::DeadlineExceeded => TruncateFailureKind::DeadlineExceeded,
            ConnectorErrorKind::ResourceExhausted => TruncateFailureKind::ResourceExhausted,
            ConnectorErrorKind::Unavailable => TruncateFailureKind::Unavailable,
            ConnectorErrorKind::CorruptData => TruncateFailureKind::CorruptData,
            ConnectorErrorKind::Internal => TruncateFailureKind::Internal,
        },
        message: error.to_string(),
    }
}

fn project_mutation_failure(failure: ConnectorMutationFailure) -> TruncateFailure {
    TruncateFailure {
        kind: match failure.kind() {
            ConnectorMutationFailureKind::InvalidRequest => TruncateFailureKind::InvalidRequest,
            ConnectorMutationFailureKind::NotFound => TruncateFailureKind::NotFound,
            ConnectorMutationFailureKind::AlreadyExists => TruncateFailureKind::AlreadyExists,
            ConnectorMutationFailureKind::Conflict => TruncateFailureKind::Conflict,
            ConnectorMutationFailureKind::Unauthenticated => TruncateFailureKind::Unauthenticated,
            ConnectorMutationFailureKind::PermissionDenied => TruncateFailureKind::PermissionDenied,
            ConnectorMutationFailureKind::Unsupported => TruncateFailureKind::Unsupported,
            ConnectorMutationFailureKind::Cancelled => TruncateFailureKind::Cancelled,
            ConnectorMutationFailureKind::DeadlineExceeded => TruncateFailureKind::DeadlineExceeded,
            ConnectorMutationFailureKind::ResourceExhausted => {
                TruncateFailureKind::ResourceExhausted
            }
            ConnectorMutationFailureKind::Unavailable => TruncateFailureKind::Unavailable,
            ConnectorMutationFailureKind::CorruptData => TruncateFailureKind::CorruptData,
            ConnectorMutationFailureKind::Internal => TruncateFailureKind::Internal,
        },
        message: failure.message().to_string(),
    }
}

fn project_dispatch(dispatch: DataMutationDispatchState) -> TruncateDispatchState {
    match dispatch {
        DataMutationDispatchState::ConfirmedNotDispatched => {
            TruncateDispatchState::ConfirmedNotDispatched
        }
        DataMutationDispatchState::PossiblyDispatched => TruncateDispatchState::PossiblyDispatched,
    }
}

fn project_contract_error(error: ConnectorError, possibly_dispatched: bool) -> TruncateOutcome {
    TruncateOutcome::ContractFailure {
        failure: project_connector_error(error),
        dispatch: if possibly_dispatched {
            TruncateDispatchState::PossiblyDispatched
        } else {
            TruncateDispatchState::ConfirmedNotDispatched
        },
    }
}

fn contract_outcome(
    kind: TruncateFailureKind,
    message: impl Into<String>,
    dispatch: TruncateDispatchState,
) -> TruncateOutcome {
    TruncateOutcome::ContractFailure {
        failure: TruncateFailure {
            kind,
            message: message.into(),
        },
        dispatch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifier_is_side_effect_free_and_preserves_branch() {
        assert_eq!(parse_truncate_command("SELECT 1").unwrap(), None);
        assert_eq!(
            parse_truncate_command("TRUNCATE TABLE ice.db.orders.branch_dev").unwrap(),
            Some(TruncateCommand {
                target_parts: vec!["ice".into(), "db".into(), "orders".into()],
                target_ref: "dev".into(),
            })
        );
    }

    #[test]
    fn classifier_rejects_read_only_and_partial_forms() {
        assert!(parse_truncate_command("TRUNCATE TABLE ice.db.orders.tag_v1").is_err());
        assert!(parse_truncate_command("TRUNCATE TABLE ice.db.orders PARTITION (p1)").is_err());
    }

    #[test]
    fn reverse_port_is_object_safe() {
        fn accepts_object_safe_port(_: &dyn TruncateEngine) {}
        let _ = accepts_object_safe_port;
    }

    #[test]
    fn resource_exhausted_plan_preflight_is_known_uncommitted() {
        let outcome = ResolvedDataMutation::KnownUncommitted {
            failure: KnownUncommittedDataMutation::Planning(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg TRUNCATE evidence exceeds durable wire cap",
            )),
        };
        assert!(matches!(
            project_plan_error(outcome),
            TruncatePlanError::KnownUncommitted(TruncateFailure {
                kind: TruncateFailureKind::ResourceExhausted,
                ..
            })
        ));
    }
}
