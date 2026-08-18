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

use crate::mv::domain::application::{
    MvApplicationError, MvApplicationErrorKind, MvStatementResult,
};
use crate::mv::domain::persistence::refresh::{
    FrontendMvRefreshAction, FrontendMvRefreshActionPhase, FrontendMvRefreshActionState,
    FrontendMvRefreshCommittedVersion, FrontendMvRefreshEvidence, FrontendMvRefreshLedger,
    MvRefreshFinalizeRequest,
};
use crate::mv::domain::repository::{
    BeginFrontendMvRefreshIntentRequest, MvRepository, MvRepositoryError,
};
use crate::native::fragment_encoder::encode_native_fragment_bundle;
use crate::query_execution::ConnectorWriteCompletion;
use crate::query_execution::contract::ConnectorWriteExecutionRegistration;
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvRefreshCommittedFacts, MvRefreshPublishedFacts,
};
use crate::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshAttemptIdentity, PreparedMvRefresh, PreparedMvRefreshWork, PreparedMvRefreshWrite,
};
use crate::query_execution::mv_native_write::{
    MvRefreshProviderActivation, MvRefreshProviderActivationSink, PreparedMvNativeWriteAssembly,
};
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::query_execution::service::QueryExecutionService;
use novarocks::connector::mutation::{
    CompletedCatalogMutation, ResolvedCatalogMutation, resolve_catalog_mutation_with_lease,
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationReceipt, ConnectorControlRegistry,
    ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorMutationOperationId,
    ConnectorRefAction, ConnectorRefKind, ConnectorRefreshPublicationGuard,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorWriteReceipt, CreateOrReplacePolicy,
    DropPolicy, ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome,
};
use novarocks_sql::planning::mv::MvRefreshFinalizeFacts;
use sha2::{Digest, Sha256};

/// Dependencies retained by the frontend composition root.  They have no
/// provider catalog client or core lifecycle helper: all external work passes
/// through the exact control lease and the distributed query service.
#[derive(Clone)]
pub(super) struct FrontendMvRefreshDependencies {
    pub(super) query_execution: QueryExecutionService,
    pub(super) connector_control: Arc<dyn ConnectorControlRegistry>,
    pub(super) provider_activation: Arc<FrontendMvRefreshProviderActivationPort>,
    /// Cluster-wide refresh ownership. `None` only where a single owner is
    /// structurally guaranteed, such as a composition without a StateStore.
    pub(super) ownership: Option<super::coordination::MvRefreshOwnershipContext>,
}

/// Composition-owned indirection between the frontend lifecycle and the Core
/// provider adapter. It is intentionally bound only after Core has opened its
/// connector registry; no all-in-one direct call is available before then.
pub(crate) struct FrontendMvRefreshProviderActivationPort {
    activation: RwLock<Option<Arc<dyn MvRefreshProviderActivation>>>,
}

impl FrontendMvRefreshProviderActivationPort {
    pub(crate) fn new() -> Self {
        Self {
            activation: RwLock::new(None),
        }
    }

    fn bind(&self, activation: Arc<dyn MvRefreshProviderActivation>) -> Result<(), String> {
        let mut current = self
            .activation
            .write()
            .map_err(|_| "MV refresh provider activation lock is poisoned".to_string())?;
        if current.is_some() {
            return Err("MV refresh provider activation is already bound".to_string());
        }
        *current = Some(activation);
        Ok(())
    }

    fn activate_write(
        &self,
        prepared: PreparedMvRefreshWrite,
        planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
        lease: &novarocks_spi::connector::ConnectorWriteLease,
        execution: &crate::common::admitted_query_context::QueryExecutionContext,
    ) -> Result<PreparedMvNativeWriteAssembly, MvApplicationError> {
        let activation = self
            .activation
            .read()
            .map_err(|_| unavailable("MV refresh provider activation lock is poisoned"))?
            .clone()
            .ok_or_else(|| unavailable("MV refresh provider activation is unavailable"))?;
        activation
            .activate_write(prepared, planning_lease, lease, execution)
            .map_err(invalid)
    }

    fn interpret_write_commit(
        &self,
        intent: crate::query_execution::mv_assembly::refresh_artifact::MvRefreshPublicationIntent,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, MvApplicationError> {
        let activation = self
            .activation
            .read()
            .map_err(|_| unavailable("MV refresh provider activation lock is poisoned"))?
            .clone()
            .ok_or_else(|| unavailable("MV refresh provider activation is unavailable"))?;
        activation
            .interpret_write_commit(intent, receipt)
            .map_err(invalid)
    }

    pub(super) fn sync_repartition_descriptor(
        &self,
        mv_id: i64,
        partition_spec: crate::mv::domain::persistence::schema::MvPartitionContract,
        committed_partitioning: novarocks_spi::connector::ConnectorCommittedPartitioning,
        connector_context: &ConnectorRequestContext,
    ) -> Result<(), MvApplicationError> {
        let activation = self
            .activation
            .read()
            .map_err(|_| unavailable("MV refresh provider activation lock is poisoned"))?
            .clone()
            .ok_or_else(|| unavailable("MV refresh provider activation is unavailable"))?;
        activation
            .sync_repartition_descriptor(
                mv_id,
                partition_spec,
                committed_partitioning,
                connector_context,
            )
            .map_err(|error| {
                MvApplicationError::new(MvApplicationErrorKind::KnownCommittedFinalizeFailed, error)
            })
    }
}

impl MvRefreshProviderActivationSink for FrontendMvRefreshProviderActivationPort {
    fn bind_mv_refresh_provider_activation(
        &self,
        activation: Arc<dyn MvRefreshProviderActivation>,
    ) -> Result<(), String> {
        self.bind(activation)
    }
}

pub(super) fn execute(
    repository: &dyn MvRepository,
    dependencies: &FrontendMvRefreshDependencies,
    refresh: PreparedMvRefresh,
    connector_context: ConnectorRequestContext,
    execution: &crate::common::admitted_query_context::QueryExecutionContext,
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

    // Take cluster-wide ownership of this target before any durable state.
    //
    // The order is load-bearing: the repository consults the ownership registry
    // as its fence source, so registering must precede the first durable
    // transition. Resolving the resource through `planning_lease`'s binding --
    // the same generation the refresh was admitted under -- keeps the lease keyed
    // by the identity this attempt actually observed.
    //
    // Ownership is sticky per target, so this is usually a no-op returning the
    // lease a previous refresh of the same MV already won.
    let _owned = match &dependencies.ownership {
        Some(context) => {
            let resource = super::coordination::resolve_target_resource_for(
                planning_lease.binding(),
                ConnectorTableIdentity {
                    instance_id: instance_id.clone(),
                    namespace: refresh.finalize.target.database.as_str().into(),
                    table: refresh.finalize.target.name.as_str().into(),
                },
                &connector_context,
            )
            .map_err(|error| unavailable(error.to_string()))?;
            let owned =
                context.block_on_acquisition(super::coordination::acquire_refresh_ownership(
                    context,
                    refresh.finalize.mv_id,
                    resource,
                ));
            match owned {
                Ok(owned) => Some(owned),
                // Contention is not a fault: another frontend is refreshing this
                // target right now, and doing it twice is the thing being
                // prevented. Surfaced as a retryable conflict.
                Err(refusal) => {
                    return Err(MvApplicationError::new(
                        // The cluster-wide analogue of the process-local
                        // activity gate: this target is already being refreshed,
                        // just by a different frontend.
                        MvApplicationErrorKind::AlreadyActive,
                        format!(
                            "another frontend currently owns this materialized view's                              refresh ({refusal:?})"
                        ),
                    ));
                }
            }
        }
        None => None,
    };

    let base_snapshots = required_base_snapshots(&refresh.finalize)?;
    let base_table_uuids = refresh.finalize.base_table_uuids.clone();
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
            base_table_uuids: base_table_uuids.clone(),
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
                    base_table_uuids,
                    target_snapshot_id: finalize.expected_target_snapshot_id,
                    partition_spec: None,
                })
                .map_err(repository_error)?;
            Ok(MvStatementResult::Ok)
        }
        PreparedMvRefreshWork::DataProducing { write } => execute_data_refresh(
            repository,
            dependencies,
            &planning_lease,
            attempt,
            finalize,
            write,
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
    attempt: MvRefreshAttemptIdentity,
    finalize: MvRefreshFinalizeFacts,
    prepared: PreparedMvRefreshWrite,
    base_snapshots: BTreeMap<String, i64>,
    connector_context: ConnectorRequestContext,
    execution: &crate::common::admitted_query_context::QueryExecutionContext,
) -> Result<MvStatementResult, MvApplicationError> {
    let atomic_repartition = prepared
        .publication_intent()
        .partition_spec_replacement()
        .is_some();
    let mutation_lease = if atomic_repartition {
        None
    } else {
        Some(
            planning_lease
                .derive_mutation_lease()
                .map_err(|error| unavailable(error.to_string()))?,
        )
    };
    let table = mutation_lease
        .as_ref()
        .map(|lease| table_identity(&finalize, lease.descriptor().instance_id.clone()));

    if atomic_repartition {
        record_proof_only_phase(
            repository,
            attempt.refresh_id,
            FrontendMvRefreshActionPhase::StagingCreate,
            attempt.staging_create_operation_id,
            None,
            None,
        )?;
    } else {
        let mutation_lease = mutation_lease
            .as_ref()
            .expect("ordinary MV refresh has a mutation lease");
        let staged = resolve_catalog_mutation_with_lease(
            mutation_lease,
            ConnectorMutationOperationId::from_bytes(attempt.staging_create_operation_id),
            ConnectorCatalogMutationOperation::AlterRef {
                table: table.clone().expect("ordinary MV refresh has a target"),
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
    }

    let write_lease = planning_lease
        .derive_write_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    if prepared.operation_id() != attempt.write_operation_id {
        return Err(invalid(
            "SQL-prepared MV write does not use the frontend-preallocated operation ID",
        ));
    }
    let publication_intent = prepared.publication_intent().clone();
    let assembly = dependencies.provider_activation.activate_write(
        prepared,
        planning_lease,
        &write_lease,
        execution,
    )?;
    let encoding = assembly.native_encoding();
    let native_bundle = encode_native_fragment_bundle(encoding.encoding_view()).map_err(invalid)?;
    let write = assembly.finish(native_bundle).map_err(invalid)?;
    if write.write_operation_id() != attempt.write_operation_id {
        return Err(invalid(
            "SQL-prepared MV write does not use the frontend-preallocated operation ID",
        ));
    }
    let cohort_id = write.write_cohort_id();
    let session = dependencies
        .query_execution
        .begin_write_operation(write.registration(), write_lease)
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
    let CommittedWrite { effect, receipt } = resolve_write_commit(
        repository,
        attempt.refresh_id,
        attempt.write_operation_id.to_bytes(),
        &completion,
        connector_context.clone(),
    )?;
    let committed = dependencies
        .provider_activation
        .interpret_write_commit(publication_intent, &receipt)?;
    let committed_version = committed.committed_version().clone();
    let write_frontend_version = frontend_version(&committed_version)?;

    recovery_phase_barrier("write-committed")?;

    // A no-op write staged nothing, so the staging branch still points at the
    // target head and there is no snapshot to fast-forward onto main. Issuing
    // the publication anyway would ask the provider to promote a snapshot that
    // carries no marker for this refresh, which fails closed. Record the phase
    // as proof-only and let the unchanged version carry through to finalize,
    // exactly as the atomic-repartition path does when the write already
    // published itself.
    let published_by_write = atomic_repartition || matches!(effect, ExternalMutationEffect::NoOp);
    let publication_version = if published_by_write {
        record_proof_only_phase(
            repository,
            attempt.refresh_id,
            FrontendMvRefreshActionPhase::Publication,
            attempt.publication_operation_id,
            Some(receipt_evidence(
                receipt.payload().as_ref(),
                receipt.digest(),
            )),
            Some(write_frontend_version.clone()),
        )?;
        committed_version
    } else {
        let guard = ConnectorRefreshPublicationGuard::try_new(
            attempt.refresh_id,
            finalize.mv_id,
            attempt.marker_token.clone(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        let publication = resolve_catalog_mutation_with_lease(
            mutation_lease
                .as_ref()
                .expect("ordinary MV refresh has a mutation lease"),
            ConnectorMutationOperationId::from_bytes(attempt.publication_operation_id),
            ConnectorCatalogMutationOperation::AlterRef {
                table: table.clone().expect("ordinary MV refresh has a target"),
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
        let publication = record_catalog_action(
            repository,
            attempt.refresh_id,
            FrontendMvRefreshActionPhase::Publication,
            attempt.publication_operation_id,
            publication,
            Some(write_frontend_version.clone()),
        )?;
        publication
            .receipt
            .committed_version()
            .cloned()
            .unwrap_or(committed_version)
    };
    let published =
        MvRefreshPublishedFacts::try_new(committed, publication_version).map_err(invalid)?;
    let published_frontend_version = frontend_version(published.publication_version())?;

    recovery_phase_barrier("publication-committed")?;

    if atomic_repartition {
        record_proof_only_phase(
            repository,
            attempt.refresh_id,
            FrontendMvRefreshActionPhase::StagingDrop,
            attempt.staging_drop_operation_id,
            None,
            None,
        )?;
    } else {
        let cleanup = resolve_catalog_mutation_with_lease(
            mutation_lease
                .as_ref()
                .expect("ordinary MV refresh has a mutation lease"),
            ConnectorMutationOperationId::from_bytes(attempt.staging_drop_operation_id),
            ConnectorCatalogMutationOperation::AlterRef {
                table: table.expect("ordinary MV refresh has a target"),
                action: ConnectorRefAction::Drop {
                    kind: ConnectorRefKind::Branch,
                    name: attempt.staging_branch.clone().into(),
                    policy: DropPolicy::NoOpIfMissing,
                },
            },
            connector_context.clone(),
        );
        record_catalog_action(
            repository,
            attempt.refresh_id,
            FrontendMvRefreshActionPhase::StagingDrop,
            attempt.staging_drop_operation_id,
            cleanup,
            None,
        )?;
    }

    let committed_partitioning = published.committed().committed_partitioning();
    let partition_spec = committed_partitioning
        .map(mv_partition_contract)
        .transpose()?;
    if let Some(committed_partitioning) = committed_partitioning {
        let partition_spec = partition_spec.as_ref().ok_or_else(|| {
            invalid("MV repartition commit is missing its application partition contract")
        })?;
        dependencies
            .provider_activation
            .sync_repartition_descriptor(
                finalize.mv_id,
                partition_spec.clone(),
                committed_partitioning.clone(),
                &connector_context,
            )?;
    }
    repository
        .finalize_refresh(MvRefreshFinalizeRequest {
            refresh_id: attempt.refresh_id,
            rows: published.committed().resulting_row_count(),
            base_snapshots,
            base_table_uuids: finalize.base_table_uuids,
            target_snapshot_id: published_frontend_version.snapshot_id,
            partition_spec,
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
        PreparedMvRefreshWork::DataProducing { write } => {
            vec![hex::encode(write.primary_cohort().to_bytes())]
        }
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

/// A committed write terminal, kept together with the effect the provider
/// reported for it.
///
/// The effect decides whether a publication is still owed: a provider that
/// resolved an empty delta through its declared no-external-commit disposition
/// leaves the staging branch exactly where the target already was, so there is
/// nothing to fast-forward.
struct CommittedWrite {
    effect: ExternalMutationEffect,
    receipt: ConnectorWriteReceipt,
}

fn resolve_write_commit(
    repository: &dyn MvRepository,
    refresh_id: i64,
    operation_id: [u8; 16],
    completion: &ConnectorWriteCompletion,
    context: ConnectorRequestContext,
) -> Result<CommittedWrite, MvApplicationError> {
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
) -> Result<CommittedWrite, MvApplicationError> {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
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
                ExternalMutationFinalization::Complete => Ok(CommittedWrite { effect, receipt }),
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

fn record_proof_only_phase(
    repository: &dyn MvRepository,
    refresh_id: i64,
    phase: FrontendMvRefreshActionPhase,
    operation_id: [u8; 16],
    receipt: Option<FrontendMvRefreshEvidence>,
    committed_version: Option<FrontendMvRefreshCommittedVersion>,
) -> Result<(), MvApplicationError> {
    record_action_value(
        repository,
        refresh_id,
        proof_only_action(phase, operation_id, receipt, committed_version),
    )
}

fn proof_only_action(
    phase: FrontendMvRefreshActionPhase,
    operation_id: [u8; 16],
    receipt: Option<FrontendMvRefreshEvidence>,
    committed_version: Option<FrontendMvRefreshCommittedVersion>,
) -> FrontendMvRefreshAction {
    FrontendMvRefreshAction {
        phase,
        state: FrontendMvRefreshActionState::KnownCommitted,
        operation_id: operation_id.to_vec(),
        receipt,
        committed_version,
        external_evidence: None,
        provider_finalized: true,
    }
}

pub(super) fn mv_partition_contract(
    committed: &novarocks_spi::connector::ConnectorCommittedPartitioning,
) -> Result<crate::mv::domain::persistence::schema::MvPartitionContract, MvApplicationError> {
    use crate::mv::domain::persistence::schema::{
        MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract,
    };
    use novarocks_spi::connector::ConnectorManagedPartitionTransform as Transform;

    let fields = committed
        .fields()
        .iter()
        .map(|field| MvPartitionFieldContract {
            partition_field_id: field.partition_field_id(),
            partition_field_name: field.partition_field_name().to_string(),
            source_target_field_id: field.source_field_id(),
            source_column_name: field.source_column_name().to_string(),
            transform: match field.transform() {
                Transform::Identity => MvPartitionTransformContract::Identity,
                Transform::Year => MvPartitionTransformContract::Year,
                Transform::Month => MvPartitionTransformContract::Month,
                Transform::Day => MvPartitionTransformContract::Day,
                Transform::Hour => MvPartitionTransformContract::Hour,
                Transform::Bucket { buckets } => MvPartitionTransformContract::Bucket {
                    num_buckets: buckets,
                },
                Transform::Truncate { width } => MvPartitionTransformContract::Truncate { width },
                Transform::Void => MvPartitionTransformContract::Void,
            },
        })
        .collect();
    Ok(MvPartitionContract {
        target_spec_id: committed.spec_id(),
        fields,
    })
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

    let Some(root) = crate::common::query_lifecycle_fault::configured_root() else {
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
        crate::mv::domain::repository::MvRepositoryErrorKind::Conflict => {
            MvApplicationErrorKind::AlreadyActive
        }
        crate::mv::domain::repository::MvRepositoryErrorKind::NotFound => {
            MvApplicationErrorKind::TargetGone
        }
        crate::mv::domain::repository::MvRepositoryErrorKind::Corruption => {
            MvApplicationErrorKind::Corruption
        }
        crate::mv::domain::repository::MvRepositoryErrorKind::CommitUnknown => {
            MvApplicationErrorKind::RecoveryRequired
        }
        crate::mv::domain::repository::MvRepositoryErrorKind::KnownCommittedFinalizeFailed => {
            MvApplicationErrorKind::RecoveryRequired
        }
        crate::mv::domain::repository::MvRepositoryErrorKind::Unavailable => {
            MvApplicationErrorKind::Unavailable
        }
        _ => MvApplicationErrorKind::Repository,
    };
    MvApplicationError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use crate::common::admitted_query_context::QueryExecutionContext;
    use crate::mv::domain::persistence::refresh::{
        FrontendMvRefreshActionPhase, FrontendMvRefreshActionState,
        FrontendMvRefreshCommittedVersion,
    };
    use crate::query_execution::mv_assembly::refresh_artifact::{
        MvRefreshCommittedFacts, MvRefreshPublicationIntent,
    };
    use crate::query_execution::mv_assembly::refresh_handoff::PreparedMvRefreshWrite;
    use crate::query_execution::mv_native_write::{
        MvRefreshProviderActivation, MvRefreshProviderActivationSink, PreparedMvNativeWriteAssembly,
    };
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorCommittedPartitionField, ConnectorCommittedPartitioning,
        ConnectorControlPlanningLease, ConnectorManagedPartitionTransform, ConnectorRequestContext,
        ConnectorWriteLease, ConnectorWriteReceipt, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::{FrontendMvRefreshProviderActivationPort, proof_only_action, receipt_evidence};

    #[derive(Default)]
    struct FakeActivator {
        descriptor_projection: Mutex<
            Option<(
                i64,
                crate::mv::domain::persistence::schema::MvPartitionContract,
                ConnectorCommittedPartitioning,
            )>,
        >,
        failure: Option<&'static str>,
    }

    impl MvRefreshProviderActivation for FakeActivator {
        fn activate_write(
            &self,
            _prepared: PreparedMvRefreshWrite,
            _planning_lease: &ConnectorControlPlanningLease,
            _exact_lease: &ConnectorWriteLease,
            _execution: &QueryExecutionContext,
        ) -> Result<PreparedMvNativeWriteAssembly, String> {
            unreachable!("the composition test never binds a write")
        }

        fn interpret_write_commit(
            &self,
            _intent: MvRefreshPublicationIntent,
            _receipt: &ConnectorWriteReceipt,
        ) -> Result<MvRefreshCommittedFacts, String> {
            unreachable!("the composition test never interprets a receipt")
        }

        fn sync_repartition_descriptor(
            &self,
            mv_id: i64,
            partition_spec: crate::mv::domain::persistence::schema::MvPartitionContract,
            committed_partitioning: ConnectorCommittedPartitioning,
            _connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<(), String> {
            *self
                .descriptor_projection
                .lock()
                .expect("descriptor projection observation lock") =
                Some((mv_id, partition_spec, committed_partitioning));
            if let Some(failure) = self.failure {
                return Err(failure.to_string());
            }
            Ok(())
        }
    }

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn connector_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("connector request context")
    }

    fn committed_partitioning() -> ConnectorCommittedPartitioning {
        ConnectorCommittedPartitioning::try_new(
            12,
            vec![
                ConnectorCommittedPartitionField::try_new(
                    1_050,
                    "id_bucket_16",
                    10,
                    "id",
                    0,
                    ConnectorManagedPartitionTransform::Bucket { buckets: 16 },
                )
                .expect("committed partition field"),
            ],
        )
        .expect("committed partitioning")
    }

    #[test]
    fn provider_activation_is_bound_once_after_frontend_composition() {
        let port = FrontendMvRefreshProviderActivationPort::new();
        port.bind_mv_refresh_provider_activation(Arc::new(FakeActivator::default()))
            .expect("first Core activation adapter binds");

        let error = port
            .bind_mv_refresh_provider_activation(Arc::new(FakeActivator::default()))
            .expect_err("a second Core activation adapter must fail closed");
        assert_eq!(error, "MV refresh provider activation is already bound");
    }

    #[test]
    fn repartition_descriptor_projection_forwards_raw_committed_partitioning() {
        let port = FrontendMvRefreshProviderActivationPort::new();
        let activation = Arc::new(FakeActivator::default());
        port.bind_mv_refresh_provider_activation(activation.clone())
            .expect("bind Core activation adapter");
        let committed_partitioning = committed_partitioning();
        let partition_spec = super::mv_partition_contract(&committed_partitioning)
            .expect("application partition contract");
        let expected_partition_spec = partition_spec.clone();
        let expected_committed_partitioning = committed_partitioning.clone();

        port.sync_repartition_descriptor(
            42,
            partition_spec,
            committed_partitioning,
            &connector_context(),
        )
        .expect("descriptor projection");

        assert_eq!(
            *activation
                .descriptor_projection
                .lock()
                .expect("descriptor projection observation lock"),
            Some((42, expected_partition_spec, expected_committed_partitioning,))
        );
    }

    #[test]
    fn repartition_descriptor_projection_failure_remains_a_finalize_failure() {
        let port = FrontendMvRefreshProviderActivationPort::new();
        let activation = Arc::new(FakeActivator {
            descriptor_projection: Mutex::new(None),
            failure: Some("guarded descriptor projection conflict"),
        });
        port.bind_mv_refresh_provider_activation(activation)
            .expect("bind Core activation adapter");
        let committed_partitioning = committed_partitioning();
        let partition_spec = super::mv_partition_contract(&committed_partitioning)
            .expect("application partition contract");

        let error = port
            .sync_repartition_descriptor(
                42,
                partition_spec,
                committed_partitioning,
                &connector_context(),
            )
            .expect_err("descriptor projection conflict must retain the refresh fence");

        assert_eq!(
            error.kind(),
            crate::mv::domain::application::MvApplicationErrorKind::KnownCommittedFinalizeFailed
        );
    }

    #[test]
    fn proof_only_publication_keeps_its_phase_operation_identity() {
        let write_operation_id = [7; 16];
        let publication_operation_id = [9; 16];
        let write_receipt_payload = b"atomic-write-receipt";
        let write_receipt_digest = super::sha256(write_receipt_payload);
        let committed_version =
            FrontendMvRefreshCommittedVersion::try_new(b"committed-version".to_vec(), Some(42))
                .expect("committed version");

        let action = proof_only_action(
            FrontendMvRefreshActionPhase::Publication,
            publication_operation_id,
            Some(receipt_evidence(
                write_receipt_payload,
                write_receipt_digest,
            )),
            Some(committed_version.clone()),
        );

        assert_ne!(publication_operation_id, write_operation_id);
        assert_eq!(action.phase, FrontendMvRefreshActionPhase::Publication);
        assert_eq!(action.operation_id, publication_operation_id);
        assert_eq!(action.state, FrontendMvRefreshActionState::KnownCommitted);
        assert_eq!(action.committed_version, Some(committed_version));
        assert_eq!(
            action.receipt.expect("write proof").digest,
            write_receipt_digest
        );
        assert!(action.external_evidence.is_none());
        assert!(action.provider_finalized);
    }
}
