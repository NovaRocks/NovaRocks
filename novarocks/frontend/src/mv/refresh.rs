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

//! Frontend-owned execution of one SQL-prepared MV refresh attempt.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use novarocks::connector::mutation::{
    CompletedCatalogMutation, ResolvedCatalogMutation, resolve_catalog_mutation_with_lease,
};
use novarocks::mv::application::{MvApplicationError, MvApplicationErrorKind, MvStatementResult};
use novarocks::mv::application::{MvFirstRefreshWriteActivator, MvFirstRefreshWriteActivatorSink};
use novarocks::mv::persistence::refresh::{
    FrontendMvRefreshAction, FrontendMvRefreshActionPhase, FrontendMvRefreshActionState,
    FrontendMvRefreshCommittedVersion, FrontendMvRefreshEvidence, FrontendMvRefreshLedger,
    MvRefreshFinalizeRequest,
};
use novarocks::mv::repository::{
    BeginFrontendMvRefreshIntentRequest, MvRepository, MvRepositoryError,
};
use novarocks::query_execution::ConnectorWriteCompletion;
use novarocks::query_execution::contract::ConnectorWriteExecutionRegistration;
use novarocks::query_execution::service::QueryExecutionService;
use novarocks::sql::mv_refresh::{
    MvRefreshFinalizeFacts, PreparedDistributedWriteRequest, PreparedMvRefresh,
    PreparedMvRefreshWork,
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationReceipt, ConnectorControlRegistry,
    ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorMutationOperationId,
    ConnectorRefAction, ConnectorRefKind, ConnectorRefreshPublicationGuard,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorWriteReceipt, CreateOrReplacePolicy,
    DropPolicy, ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
};
use sha2::{Digest, Sha256};

/// Dependencies retained by the frontend composition root.  They have no
/// provider catalog client or core lifecycle helper: all external work passes
/// through the exact control lease and the distributed query service.
#[derive(Clone)]
pub(super) struct FrontendMvRefreshDependencies {
    pub(super) query_execution: QueryExecutionService,
    pub(super) connector_control: Arc<dyn ConnectorControlRegistry>,
    pub(super) first_refresh_activator: Arc<FrontendMvFirstRefreshWriteActivatorPort>,
}

/// Composition-owned indirection between the frontend lifecycle and the Core
/// provider adapter. It is intentionally bound only after Core has opened its
/// connector registry; no all-in-one direct call is available before then.
pub(crate) struct FrontendMvFirstRefreshWriteActivatorPort {
    activator: RwLock<Option<Arc<dyn MvFirstRefreshWriteActivator>>>,
}

impl FrontendMvFirstRefreshWriteActivatorPort {
    pub(crate) fn new() -> Self {
        Self {
            activator: RwLock::new(None),
        }
    }

    fn bind(&self, activator: Arc<dyn MvFirstRefreshWriteActivator>) -> Result<(), String> {
        let mut current = self
            .activator
            .write()
            .map_err(|_| "MV first-refresh activator lock is poisoned".to_string())?;
        if current.is_some() {
            return Err("MV first-refresh activator is already bound".to_string());
        }
        *current = Some(activator);
        Ok(())
    }

    fn bind_write(
        &self,
        prepared: novarocks::sql::mv_refresh::first_refresh::PreparedMvFirstRefreshWrite,
        lease: &novarocks_spi::connector::ConnectorWriteLease,
        execution: &novarocks::query_execution::request_context::QueryExecutionContext,
    ) -> Result<PreparedDistributedWriteRequest, MvApplicationError> {
        let activator = self
            .activator
            .read()
            .map_err(|_| unavailable("MV first-refresh activator lock is poisoned"))?
            .clone()
            .ok_or_else(|| unavailable("MV first-refresh provider activation is unavailable"))?;
        activator
            .bind_first_refresh_write(prepared, lease, execution)
            .map_err(invalid)
    }

    fn bind_incremental_write(
        &self,
        prepared: novarocks::sql::mv_refresh::incremental::PreparedMvIncrementalWrite,
        lease: &novarocks_spi::connector::ConnectorWriteLease,
        execution: &novarocks::query_execution::request_context::QueryExecutionContext,
    ) -> Result<PreparedDistributedWriteRequest, MvApplicationError> {
        let activator = self
            .activator
            .read()
            .map_err(|_| unavailable("MV first-refresh activator lock is poisoned"))?
            .clone()
            .ok_or_else(|| unavailable("MV incremental provider activation is unavailable"))?;
        activator
            .bind_incremental_refresh_write(prepared, lease, execution)
            .map_err(invalid)
    }
}

impl MvFirstRefreshWriteActivatorSink for FrontendMvFirstRefreshWriteActivatorPort {
    fn bind_mv_first_refresh_write_activator(
        &self,
        activator: Arc<dyn MvFirstRefreshWriteActivator>,
    ) -> Result<(), String> {
        self.bind(activator)
    }
}

pub(super) fn execute(
    repository: &dyn MvRepository,
    dependencies: &FrontendMvRefreshDependencies,
    refresh: PreparedMvRefresh,
    connector_context: ConnectorRequestContext,
    execution: &novarocks::query_execution::request_context::QueryExecutionContext,
) -> Result<MvStatementResult, MvApplicationError> {
    // A no-snapshot first observation is deliberately not a durable refresh.
    // It has no external action and no base watermark that can be finalized;
    // Treating it as a durable refresh would require a nonexistent watermark
    // and incorrectly fence the MV before a later base-table commit is seen.
    if matches!(&refresh.work, PreparedMvRefreshWork::NoOp) {
        return Ok(MvStatementResult::Ok);
    }
    let target_catalog = refresh
        .finalize
        .target
        .catalog
        .as_deref()
        .ok_or_else(|| invalid("MV refresh requires an explicit connector catalog"))?;
    let instance_id =
        ConnectorInstanceId::parse(target_catalog).map_err(|error| invalid(error.to_string()))?;
    let planning_lease = dependencies
        .connector_control
        .acquire_current(&instance_id)
        .map_err(|error| unavailable(error.to_string()))?;
    let actual_binding = ConnectorExecutionBindingKey {
        instance_id: planning_lease.binding().descriptor().instance_id.clone(),
        incarnation: planning_lease.binding().incarnation(),
    };
    if actual_binding != refresh.observed_binding {
        return Err(MvApplicationError::new(
            MvApplicationErrorKind::CommitUnknown,
            "MV refresh connector generation changed after SQL preparation",
        ));
    }

    let base_snapshots = required_base_snapshots(&refresh.finalize)?;
    let has_external_actions = matches!(&refresh.work, PreparedMvRefreshWork::DataProducing { .. });
    let ledger = new_ledger(&refresh, &planning_lease, has_external_actions)?;
    repository
        .begin_frontend_refresh_intent(BeginFrontendMvRefreshIntentRequest {
            refresh_id: refresh.attempt.refresh_id,
            mv_id: refresh.finalize.mv_id,
            target_catalog: target_catalog.to_string(),
            target_namespace: refresh.finalize.target.database.clone(),
            target_table: refresh.finalize.target.name.clone(),
            staging_branch: refresh.attempt.staging_branch.clone(),
            expected_main_snapshot_id: refresh.finalize.expected_target_snapshot_id,
            base_snapshots: base_snapshots.clone(),
            marker_token: refresh.attempt.marker_token.clone(),
            prepare_external_actions: has_external_actions,
            ledger,
        })
        .map_err(repository_error)?;

    let PreparedMvRefresh {
        attempt,
        finalize,
        work,
        ..
    } = refresh;
    match work {
        PreparedMvRefreshWork::NoOp | PreparedMvRefreshWork::MetadataOnly => {
            repository
                .finalize_frontend_refresh_without_external_actions(MvRefreshFinalizeRequest {
                    refresh_id: attempt.refresh_id,
                    rows: 0,
                    base_snapshots,
                    base_table_uuids: finalize.base_table_uuids,
                    target_snapshot_id: finalize.expected_target_snapshot_id,
                })
                .map_err(repository_error)?;
            Ok(MvStatementResult::Ok)
        }
        PreparedMvRefreshWork::DataProducing {
            distributed_writes,
            first_refresh_writes,
            incremental_writes,
        } => execute_data_refresh(
            repository,
            dependencies,
            &planning_lease,
            attempt,
            finalize,
            distributed_writes,
            first_refresh_writes,
            incremental_writes,
            base_snapshots,
            connector_context,
            execution,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_data_refresh(
    repository: &dyn MvRepository,
    dependencies: &FrontendMvRefreshDependencies,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: novarocks::sql::mv_refresh::MvRefreshAttemptIdentity,
    finalize: MvRefreshFinalizeFacts,
    distributed_writes: Vec<PreparedDistributedWriteRequest>,
    first_refresh_writes: Vec<
        novarocks::sql::mv_refresh::first_refresh::PreparedMvFirstRefreshWrite,
    >,
    incremental_writes: Vec<novarocks::sql::mv_refresh::incremental::PreparedMvIncrementalWrite>,
    base_snapshots: BTreeMap<String, i64>,
    connector_context: ConnectorRequestContext,
    execution: &novarocks::query_execution::request_context::QueryExecutionContext,
) -> Result<MvStatementResult, MvApplicationError> {
    if distributed_writes.len() + first_refresh_writes.len() + incremental_writes.len() != 1 {
        return Err(invalid(
            "MV refresh data preparation must produce exactly one staged distributed write",
        ));
    }
    let mutation_lease = planning_lease
        .derive_mutation_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let table = table_identity(&finalize, mutation_lease.descriptor().instance_id.clone());

    let staged = resolve_catalog_mutation_with_lease(
        &mutation_lease,
        ConnectorMutationOperationId::from_bytes(attempt.staging_create_operation_id),
        ConnectorCatalogMutationOperation::AlterRef {
            table: table.clone(),
            action: ConnectorRefAction::Create {
                kind: ConnectorRefKind::Branch,
                name: attempt.staging_branch.clone().into(),
                snapshot_id: finalize.expected_target_snapshot_id,
                policy: CreateOrReplacePolicy::NoOpIfExists,
            },
        },
        connector_context.clone(),
    );
    record_catalog_action(
        repository,
        attempt.refresh_id,
        FrontendMvRefreshActionPhase::StagingCreate,
        attempt.staging_create_operation_id,
        staged,
        None,
    )?;

    let write_lease = planning_lease
        .derive_write_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let write = match (
        distributed_writes.into_iter().next(),
        first_refresh_writes.into_iter().next(),
        incremental_writes.into_iter().next(),
    ) {
        (Some(write), None, None) => write,
        (None, Some(first_refresh), None) => {
            if first_refresh.operation_id() != attempt.write_operation_id {
                return Err(invalid(
                    "SQL-prepared MV first-refresh write does not use the frontend-preallocated operation ID",
                ));
            }
            dependencies.first_refresh_activator.bind_write(
                first_refresh,
                &write_lease,
                execution,
            )?
        }
        (None, None, Some(incremental)) => {
            if incremental.operation_id() != attempt.write_operation_id {
                return Err(invalid(
                    "SQL-prepared MV incremental write does not use the frontend-preallocated operation ID",
                ));
            }
            dependencies
                .first_refresh_activator
                .bind_incremental_write(incremental, &write_lease, execution)?
        }
        _ => unreachable!("checked exactly one prepared MV write"),
    };
    if write.write_operation_id() != attempt.write_operation_id {
        return Err(invalid(
            "SQL-prepared MV write does not use the frontend-preallocated operation ID",
        ));
    }
    let cohort_id = write.write_cohort_id();
    let session = dependencies
        .query_execution
        .begin_write_operation_with_lease(write.registration(), write_lease)
        .map_err(|error| invalid(error.to_string()))?;
    let registration = ConnectorWriteExecutionRegistration::try_new(session, cohort_id)
        .map_err(|error| invalid(error.to_string()))?;
    let request = write
        .into_request(execution, registration)
        .map_err(|error| invalid(error.to_string()))?;
    let outcome = dependencies
        .query_execution
        .execute(request)
        .map_err(|error| {
            MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
        })?
        .into_write()
        .map_err(|error| {
            MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
        })?;
    let (result, direct_commit, abort, completion) = outcome.into_parts_with_connector();
    if !result.columns.is_empty() || !result.chunks.is_empty() {
        return Err(invalid(
            "MV refresh connector staging terminal returned a client result payload",
        ));
    }
    if direct_commit.is_some() {
        return Err(invalid(
            "MV refresh connector staging terminal returned a legacy direct commit payload",
        ));
    }
    if let Some(abort) = abort {
        record_action(
            repository,
            attempt.refresh_id,
            FrontendMvRefreshActionPhase::Write,
            attempt.write_operation_id.to_bytes(),
            FrontendMvRefreshActionState::KnownUncommitted,
            None,
            None,
            None,
            false,
        )?;
        return Err(MvApplicationError::new(
            MvApplicationErrorKind::Engine,
            format!("MV refresh distributed write aborted: {}", abort.reason()),
        ));
    }
    let completion = completion.ok_or_else(|| {
        invalid("MV refresh distributed write completed without connector terminal reports")
    })?;
    let receipt = resolve_write_commit(
        repository,
        attempt.refresh_id,
        attempt.write_operation_id.to_bytes(),
        &completion,
        connector_context.clone(),
    )?;
    let committed_version = receipt.committed_version().ok_or_else(|| {
        invalid("MV refresh connector write committed without a provider version")
    })?;
    let rows = i64::try_from(receipt.resulting_row_count().ok_or_else(|| {
        invalid("MV refresh connector write committed without resulting row-count fact")
    })?)
    .map_err(|_| invalid("MV refresh committed row count exceeds i64 range"))?;
    let frontend_version = frontend_version(committed_version)?;

    recovery_phase_barrier("write-committed")?;

    let guard = ConnectorRefreshPublicationGuard::try_new(
        attempt.refresh_id,
        finalize.mv_id,
        attempt.marker_token.clone(),
    )
    .map_err(|error| invalid(error.to_string()))?;
    let publication = resolve_catalog_mutation_with_lease(
        &mutation_lease,
        ConnectorMutationOperationId::from_bytes(attempt.publication_operation_id),
        ConnectorCatalogMutationOperation::AlterRef {
            table: table.clone(),
            action: ConnectorRefAction::FastForwardBranch {
                source_branch: attempt.staging_branch.clone().into(),
                target_branch: "main".into(),
                committed_version: committed_version.clone(),
                expected_target_snapshot_id: finalize.expected_target_snapshot_id,
                guard,
            },
        },
        connector_context.clone(),
    );
    record_catalog_action(
        repository,
        attempt.refresh_id,
        FrontendMvRefreshActionPhase::Publication,
        attempt.publication_operation_id,
        publication,
        Some(frontend_version.clone()),
    )?;

    recovery_phase_barrier("publication-committed")?;

    let cleanup = resolve_catalog_mutation_with_lease(
        &mutation_lease,
        ConnectorMutationOperationId::from_bytes(attempt.staging_drop_operation_id),
        ConnectorCatalogMutationOperation::AlterRef {
            table,
            action: ConnectorRefAction::Drop {
                kind: ConnectorRefKind::Branch,
                name: attempt.staging_branch.clone().into(),
                policy: DropPolicy::NoOpIfMissing,
            },
        },
        connector_context,
    );
    record_catalog_action(
        repository,
        attempt.refresh_id,
        FrontendMvRefreshActionPhase::StagingDrop,
        attempt.staging_drop_operation_id,
        cleanup,
        None,
    )?;

    repository
        .finalize_refresh(MvRefreshFinalizeRequest {
            refresh_id: attempt.refresh_id,
            rows,
            base_snapshots,
            base_table_uuids: finalize.base_table_uuids,
            target_snapshot_id: frontend_version.snapshot_id,
        })
        .map_err(|error| {
            MvApplicationError::new(
                MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                error.to_string(),
            )
        })?;
    Ok(MvStatementResult::Ok)
}

fn new_ledger(
    refresh: &PreparedMvRefresh,
    lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    has_external_actions: bool,
) -> Result<FrontendMvRefreshLedger, MvApplicationError> {
    let cohort_ids = match &refresh.work {
        PreparedMvRefreshWork::DataProducing {
            distributed_writes,
            first_refresh_writes,
            incremental_writes,
        } => distributed_writes
            .iter()
            .map(|write| hex::encode(write.write_cohort_id().to_bytes()))
            .chain(
                first_refresh_writes
                    .iter()
                    .map(|write| hex::encode(write.primary_cohort().to_bytes())),
            )
            .chain(
                incremental_writes
                    .iter()
                    .map(|write| hex::encode(write.primary_cohort().to_bytes())),
            )
            .collect(),
        PreparedMvRefreshWork::NoOp | PreparedMvRefreshWork::MetadataOnly => Vec::new(),
    };
    if has_external_actions && cohort_ids.is_empty() {
        return Err(invalid(
            "MV refresh data preparation contains no writer cohorts",
        ));
    }
    Ok(FrontendMvRefreshLedger {
        request_id: refresh.attempt.request_id.to_vec(),
        provider_id: lease
            .binding()
            .descriptor()
            .provider_id
            .as_str()
            .to_string(),
        instance_id: lease
            .binding()
            .descriptor()
            .instance_id
            .as_str()
            .to_string(),
        incarnation: lease.binding().incarnation().to_bytes().to_vec(),
        expected_target_version: None,
        staging_create_operation_id: refresh.attempt.staging_create_operation_id.to_vec(),
        write_operation_id: refresh.attempt.write_operation_id.to_bytes().to_vec(),
        publication_operation_id: refresh.attempt.publication_operation_id.to_vec(),
        staging_drop_operation_id: refresh.attempt.staging_drop_operation_id.to_vec(),
        cohort_ids,
        actions: Vec::new(),
        cleanup_pending: false,
    })
}

fn required_base_snapshots(
    facts: &MvRefreshFinalizeFacts,
) -> Result<BTreeMap<String, i64>, MvApplicationError> {
    facts
        .base_snapshots
        .iter()
        .map(|(table, snapshot)| {
            snapshot
                .map(|snapshot| (table.clone(), snapshot))
                .ok_or_else(|| invalid(format!("MV refresh has no snapshot fact for {table}")))
        })
        .collect()
}

fn table_identity(
    facts: &MvRefreshFinalizeFacts,
    instance_id: ConnectorInstanceId,
) -> ConnectorTableIdentity {
    ConnectorTableIdentity {
        instance_id,
        namespace: facts.target.database.clone().into(),
        table: facts.target.name.clone().into(),
    }
}

fn resolve_write_commit(
    repository: &dyn MvRepository,
    refresh_id: i64,
    operation_id: [u8; 16],
    completion: &ConnectorWriteCompletion,
    context: ConnectorRequestContext,
) -> Result<ConnectorWriteReceipt, MvApplicationError> {
    let outcome = completion
        .session()
        .commit(context.clone())
        .map_err(|error| {
            MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
        })?;
    resolve_write_outcome(
        repository,
        refresh_id,
        operation_id,
        completion,
        outcome,
        context,
        true,
    )
}

fn resolve_write_outcome(
    repository: &dyn MvRepository,
    refresh_id: i64,
    operation_id: [u8; 16],
    completion: &ConnectorWriteCompletion,
    outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    context: ConnectorRequestContext,
    allow_reconcile: bool,
) -> Result<ConnectorWriteReceipt, MvApplicationError> {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization,
            ..
        } => {
            let committed_version = receipt.committed_version().ok_or_else(|| {
                invalid("MV refresh connector write committed without a provider version")
            })?;
            record_action(
                repository,
                refresh_id,
                FrontendMvRefreshActionPhase::Write,
                operation_id,
                FrontendMvRefreshActionState::KnownCommitted,
                Some(receipt_evidence(
                    receipt.payload().as_ref(),
                    receipt.digest(),
                )),
                Some(frontend_version(committed_version)?),
                None,
                matches!(finalization, ExternalMutationFinalization::Complete),
            )?;
            match finalization {
                ExternalMutationFinalization::Complete => Ok(receipt),
                ExternalMutationFinalization::Failed(failure) => Err(MvApplicationError::new(
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                    failure.to_string(),
                )),
            }
        }
        ExternalMutationOutcome::KnownUncommitted { failure } => {
            record_action(
                repository,
                refresh_id,
                FrontendMvRefreshActionPhase::Write,
                operation_id,
                FrontendMvRefreshActionState::KnownUncommitted,
                None,
                None,
                None,
                false,
            )?;
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                failure.to_string(),
            ))
        }
        ExternalMutationOutcome::CommitUnknown { failure, evidence } if allow_reconcile => {
            let reconciled = completion
                .session()
                .reconcile(evidence, context.clone())
                .map_err(|error| {
                    MvApplicationError::new(
                        MvApplicationErrorKind::CommitUnknown,
                        error.to_string(),
                    )
                })?;
            resolve_write_outcome(
                repository,
                refresh_id,
                operation_id,
                completion,
                reconciled,
                context,
                false,
            )
        }
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
            record_action(
                repository,
                refresh_id,
                FrontendMvRefreshActionPhase::Write,
                operation_id,
                FrontendMvRefreshActionState::CommitUnknown,
                None,
                None,
                Some(evidence_from_external(&evidence)),
                false,
            )?;
            Err(MvApplicationError::new(
                MvApplicationErrorKind::CommitUnknown,
                failure.to_string(),
            ))
        }
    }
}

fn record_catalog_action(
    repository: &dyn MvRepository,
    refresh_id: i64,
    phase: FrontendMvRefreshActionPhase,
    operation_id: [u8; 16],
    outcome: ResolvedCatalogMutation,
    fallback_version: Option<FrontendMvRefreshCommittedVersion>,
) -> Result<CompletedCatalogMutation, MvApplicationError> {
    match outcome {
        ResolvedCatalogMutation::KnownCommitted(completed) => {
            let action = catalog_action(
                phase,
                operation_id,
                &completed.receipt,
                completed.finalization.clone(),
                fallback_version,
            )?;
            record_action_value(repository, refresh_id, action)?;
            match completed.finalization {
                ExternalMutationFinalization::Complete => Ok(completed),
                ExternalMutationFinalization::Failed(failure) => Err(MvApplicationError::new(
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                    failure.to_string(),
                )),
            }
        }
        ResolvedCatalogMutation::KnownUncommitted { failure } => {
            record_action(
                repository,
                refresh_id,
                phase,
                operation_id,
                FrontendMvRefreshActionState::KnownUncommitted,
                None,
                None,
                None,
                false,
            )?;
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                failure.to_string(),
            ))
        }
        ResolvedCatalogMutation::CommitUnknown { failure, evidence } => {
            record_action(
                repository,
                refresh_id,
                phase,
                operation_id,
                FrontendMvRefreshActionState::CommitUnknown,
                None,
                None,
                Some(evidence_from_external(&evidence)),
                false,
            )?;
            Err(MvApplicationError::new(
                MvApplicationErrorKind::CommitUnknown,
                failure.to_string(),
            ))
        }
        ResolvedCatalogMutation::ContractFailure { error, .. } => Err(MvApplicationError::new(
            MvApplicationErrorKind::Engine,
            error.to_string(),
        )),
    }
}

fn catalog_action(
    phase: FrontendMvRefreshActionPhase,
    operation_id: [u8; 16],
    receipt: &ConnectorCatalogMutationReceipt,
    finalization: ExternalMutationFinalization,
    fallback_version: Option<FrontendMvRefreshCommittedVersion>,
) -> Result<FrontendMvRefreshAction, MvApplicationError> {
    Ok(FrontendMvRefreshAction {
        phase,
        state: FrontendMvRefreshActionState::KnownCommitted,
        operation_id: operation_id.to_vec(),
        receipt: receipt
            .provider_version()
            .map(|payload| receipt_evidence(payload.as_ref(), sha256(payload.as_ref()))),
        committed_version: receipt
            .committed_version()
            .map(frontend_version)
            .transpose()?
            .or(fallback_version),
        external_evidence: None,
        provider_finalized: matches!(finalization, ExternalMutationFinalization::Complete),
    })
}

#[allow(clippy::too_many_arguments)]
fn record_action(
    repository: &dyn MvRepository,
    refresh_id: i64,
    phase: FrontendMvRefreshActionPhase,
    operation_id: [u8; 16],
    state: FrontendMvRefreshActionState,
    receipt: Option<FrontendMvRefreshEvidence>,
    committed_version: Option<FrontendMvRefreshCommittedVersion>,
    external_evidence: Option<FrontendMvRefreshEvidence>,
    provider_finalized: bool,
) -> Result<(), MvApplicationError> {
    record_action_value(
        repository,
        refresh_id,
        FrontendMvRefreshAction {
            phase,
            state,
            operation_id: operation_id.to_vec(),
            receipt,
            committed_version,
            external_evidence,
            provider_finalized,
        },
    )
}

fn record_action_value(
    repository: &dyn MvRepository,
    refresh_id: i64,
    action: FrontendMvRefreshAction,
) -> Result<(), MvApplicationError> {
    repository
        .record_frontend_refresh_action(refresh_id, action)
        .map_err(repository_error)
}

fn frontend_version(
    version: &novarocks_spi::connector::ConnectorCommittedVersion,
) -> Result<FrontendMvRefreshCommittedVersion, MvApplicationError> {
    FrontendMvRefreshCommittedVersion::try_new(version.payload().to_vec(), version.snapshot_id())
        .map_err(invalid)
}

fn receipt_evidence(payload: &[u8], digest: [u8; 32]) -> FrontendMvRefreshEvidence {
    FrontendMvRefreshEvidence {
        payload: payload.to_vec(),
        digest: digest.to_vec(),
    }
}

fn evidence_from_external(evidence: &ExternalMutationEvidence) -> FrontendMvRefreshEvidence {
    let payload = evidence.provider_payload();
    receipt_evidence(payload.as_ref(), sha256(payload.as_ref()))
}

fn sha256(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

/// Debug-only, runner-owned crash barrier for recovery integration tests.
///
/// A trigger is deliberately a filesystem capability that is accepted only
/// when the process already has the configured query-lifecycle fault root.
/// Production builds compile this to a no-op, and normal debug deployments do
/// not enter it unless a test creates the exact one-shot trigger file.
#[cfg(debug_assertions)]
fn recovery_phase_barrier(phase: &str) -> Result<(), MvApplicationError> {
    use std::time::{Duration, Instant};

    let Some(root) = novarocks::common::app_config::config()
        .ok()
        .and_then(|config| config.debug.query_lifecycle_fault_dir())
    else {
        return Ok(());
    };
    let path = root.join(format!("mv-refresh-at-{phase}.trigger"));
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(invalid(format!(
                "read MV recovery phase trigger {}: {error}",
                path.display()
            )));
        }
    };
    let mut fields = contents.lines().filter_map(|line| line.split_once('='));
    let Some(("token", token)) = fields.next() else {
        return Err(invalid("MV recovery phase trigger has no token"));
    };
    if token.is_empty() || fields.next().is_some() {
        return Err(invalid("MV recovery phase trigger has invalid contents"));
    }
    eprintln!("NOVAROCKS_MV_RECOVERY_PHASE phase={phase} token={token}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    if path.exists() {
        return Err(MvApplicationError::new(
            MvApplicationErrorKind::Engine,
            format!("timed out waiting for MV recovery test action at phase {phase}"),
        ));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn recovery_phase_barrier(_phase: &str) -> Result<(), MvApplicationError> {
    Ok(())
}

fn invalid(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}

fn unavailable(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::Unavailable, message)
}

fn repository_error(error: MvRepositoryError) -> MvApplicationError {
    let kind = match error.kind() {
        novarocks::mv::repository::MvRepositoryErrorKind::Conflict => {
            MvApplicationErrorKind::AlreadyActive
        }
        novarocks::mv::repository::MvRepositoryErrorKind::NotFound => {
            MvApplicationErrorKind::TargetGone
        }
        novarocks::mv::repository::MvRepositoryErrorKind::Corruption => {
            MvApplicationErrorKind::Corruption
        }
        novarocks::mv::repository::MvRepositoryErrorKind::CommitUnknown => {
            MvApplicationErrorKind::RecoveryRequired
        }
        novarocks::mv::repository::MvRepositoryErrorKind::KnownCommittedFinalizeFailed => {
            MvApplicationErrorKind::RecoveryRequired
        }
        novarocks::mv::repository::MvRepositoryErrorKind::Unavailable => {
            MvApplicationErrorKind::Unavailable
        }
        _ => MvApplicationErrorKind::Repository,
    };
    MvApplicationError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks::mv::application::{
        MvFirstRefreshWriteActivator, MvFirstRefreshWriteActivatorSink,
    };
    use novarocks::query_execution::request_context::QueryExecutionContext;
    use novarocks::sql::mv_refresh::PreparedDistributedWriteRequest;
    use novarocks::sql::mv_refresh::first_refresh::PreparedMvFirstRefreshWrite;
    use novarocks::sql::mv_refresh::incremental::PreparedMvIncrementalWrite;
    use novarocks_spi::connector::ConnectorWriteLease;

    use super::FrontendMvFirstRefreshWriteActivatorPort;

    struct FakeActivator;

    impl MvFirstRefreshWriteActivator for FakeActivator {
        fn bind_first_refresh_write(
            &self,
            _prepared: PreparedMvFirstRefreshWrite,
            _exact_lease: &ConnectorWriteLease,
            _execution: &QueryExecutionContext,
        ) -> Result<PreparedDistributedWriteRequest, String> {
            unreachable!("the composition test never binds a write")
        }

        fn bind_incremental_refresh_write(
            &self,
            _prepared: PreparedMvIncrementalWrite,
            _exact_lease: &ConnectorWriteLease,
            _execution: &QueryExecutionContext,
        ) -> Result<PreparedDistributedWriteRequest, String> {
            unreachable!("the composition test never binds a write")
        }
    }

    #[test]
    fn first_refresh_activator_is_bound_once_after_frontend_composition() {
        let port = FrontendMvFirstRefreshWriteActivatorPort::new();
        port.bind_mv_first_refresh_write_activator(Arc::new(FakeActivator))
            .expect("first Core activation adapter binds");

        let error = port
            .bind_mv_first_refresh_write_activator(Arc::new(FakeActivator))
            .expect_err("a second Core activation adapter must fail closed");
        assert_eq!(error, "MV first-refresh activator is already bound");
    }
}
