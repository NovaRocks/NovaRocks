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

//! Frontend-owned table-maintenance parser, repository, and dispatch service.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use novarocks::common::cleanup_fault::{CleanupFaultKind, claim_configured as claim_cleanup_fault};
use novarocks::connector::cleanup_maintenance::CleanupBatchExecution;
use novarocks::connector::distributed_rewrite_application::DistributedRewriteIntent;
use novarocks::connector::metadata_maintenance::MetadataMaintenanceIntent;
use novarocks::engine::table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceRequestContext,
    MaintenanceStatementResult, MaintenanceTarget, OptimizeSubmission, TableMaintenanceEngine,
    TableMaintenanceService,
};
use novarocks_spi::connector::{
    BatchReceipt, ConnectorCleanupOperationId, ConnectorCleanupPlan, ConnectorCleanupPlanSummary,
    ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorMetadataMaintenancePlan, ConnectorMetadataMaintenancePlanSummary,
    ConnectorMutationOperationId, ConnectorWriteOperationId, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, PreparedBatch,
};
use novarocks_spi::state_store::StateStore;
use tokio::runtime::Handle;

use self::model::{
    CleanupBatchCheckpoint, CleanupOperationCreate, CleanupOperationState, CleanupPlanPayload,
    DistributedRewriteAttemptCheckpoint, DistributedRewriteAttemptDisposition,
    DistributedRewriteOpaquePayload, DistributedRewriteOperationCreate,
    DistributedRewriteOperationKind, DistributedRewritePlanPayload, MetadataMaintenanceExactOwner,
    MetadataMaintenanceOpaquePayload, MetadataMaintenanceOperationCreate,
    MetadataMaintenanceOperationKind, MetadataMaintenancePlanPayload, OptimizeJobCreate,
};
use self::parser::{
    ParsedMaintenanceAction, ParsedMaintenanceStatement, is_spark_maintenance_call,
    parse_maintenance_statement, parse_show_optimize,
};
use self::repository::{
    CleanupOperationRepository, DistributedRewriteOperationRepository,
    MetadataMaintenanceOperationRepository, OptimizeJobRepository, RepositoryErrorKind,
    cleanup_payload_digest, distributed_rewrite_payload_digest,
    metadata_maintenance_payload_digest,
};
use self::result::{action_result, optimize_jobs_result};
use self::worker::OptimizeWorker;

pub mod model;
pub mod parser;
pub mod repository;
pub mod result;
pub mod worker;

const OPTIMIZE_STATE_STORE_REQUIRED: &str = "ALTER TABLE OPTIMIZE requires frontend StateStore";
const SHOW_STATE_STORE_REQUIRED: &str = "SHOW ALTER TABLE OPTIMIZE requires frontend StateStore";
const AUTOMATIC_OPTIMIZE_STATE_STORE_REQUIRED: &str =
    "automatic optimize requires frontend StateStore";
const METADATA_MAINTENANCE_STATE_STORE_REQUIRED: &str =
    "connector metadata maintenance requires frontend StateStore";
const DISTRIBUTED_REWRITE_STATE_STORE_REQUIRED: &str =
    "connector distributed rewrite requires frontend StateStore";
const CLEANUP_STATE_STORE_REQUIRED: &str = "connector orphan cleanup requires frontend StateStore";

enum WorkerLifecycle {
    NotStarted,
    Started(Option<OptimizeWorker>),
    Stopped(Result<(), String>),
}

// Design: ADR-0009 (docs/adr/ADR-0009-frontend-table-maintenance-owner.md)
pub struct FrontendTableMaintenanceService {
    repository: Option<Arc<OptimizeJobRepository>>,
    metadata_repository: Option<Arc<MetadataMaintenanceOperationRepository>>,
    distributed_rewrite_repository: Option<Arc<DistributedRewriteOperationRepository>>,
    cleanup_repository: Option<Arc<CleanupOperationRepository>>,
    worker: Mutex<WorkerLifecycle>,
    runtime: Handle,
}

impl FrontendTableMaintenanceService {
    pub async fn open(store: Option<Arc<dyn StateStore>>, runtime: Handle) -> Result<Self, String> {
        let (repository, metadata_repository, distributed_rewrite_repository, cleanup_repository) =
            match store {
                Some(store) => (
                    Some(Arc::new(
                        OptimizeJobRepository::open(Arc::clone(&store))
                            .await
                            .map_err(|error| {
                                format!("open frontend optimize job repository failed: {error}")
                            })?,
                    )),
                    Some(Arc::new(
                        MetadataMaintenanceOperationRepository::open(Arc::clone(&store))
                            .await
                            .map_err(|error| {
                                format!(
                                    "open frontend metadata maintenance repository failed: {error}"
                                )
                            })?,
                    )),
                    Some(Arc::new(
                        DistributedRewriteOperationRepository::open(Arc::clone(&store))
                            .await
                            .map_err(|error| {
                                format!(
                                    "open frontend distributed rewrite repository failed: {error}"
                                )
                            })?,
                    )),
                    Some(Arc::new(
                        CleanupOperationRepository::open(Arc::clone(&store))
                            .await
                            .map_err(|error| {
                                format!("open frontend cleanup repository failed: {error}")
                            })?,
                    )),
                ),
                None => (None, None, None, None),
            };
        Ok(Self {
            repository,
            metadata_repository,
            distributed_rewrite_repository,
            cleanup_repository,
            worker: Mutex::new(WorkerLifecycle::NotStarted),
            runtime,
        })
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    fn execute_user_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
        action: ParsedMaintenanceAction,
        spark_result: bool,
    ) -> Result<MaintenanceStatementResult, String> {
        engine.reject_user_action_on_mv(&target)?;
        if let ParsedMaintenanceAction::RewriteManifests {
            use_caching,
            spec_id,
        } = action.clone()
        {
            let outcome = self.execute_durable_metadata_action(
                engine,
                target,
                MetadataMaintenanceIntent::rewrite_metadata_layout(),
                MetadataMaintenanceOperationKind::RewriteMetadataLayout,
                use_caching,
                spec_id,
            )?;
            return if spark_result {
                action_result(outcome)
            } else {
                Ok(MaintenanceStatementResult::Ok)
            };
        }
        if let ParsedMaintenanceAction::ExpireSnapshots {
            older_than_ms,
            retain_last,
        } = action.clone()
        {
            let outcome = self.execute_durable_metadata_action(
                engine,
                target,
                MetadataMaintenanceIntent::expire_table_versions(older_than_ms, retain_last),
                MetadataMaintenanceOperationKind::ExpireTableVersions,
                None,
                None,
            )?;
            return if spark_result {
                action_result(outcome)
            } else {
                Ok(MaintenanceStatementResult::Ok)
            };
        }
        if let ParsedMaintenanceAction::RewriteDataFiles {
            options,
            branch,
            where_clause,
        } = action.clone()
        {
            let intent = distributed_data_rewrite_intent(
                &options,
                branch.as_deref(),
                where_clause.as_deref(),
            )?;
            let outcome = self.execute_durable_distributed_rewrite(
                engine,
                target,
                intent,
                DistributedRewriteOperationKind::RewriteDataFiles,
                None,
            )?;
            return if spark_result {
                action_result(outcome)
            } else {
                Ok(MaintenanceStatementResult::Ok)
            };
        }
        if let ParsedMaintenanceAction::RewritePositionDeleteFiles {
            options,
            where_clause,
        } = action.clone()
        {
            let intent = distributed_position_rewrite_intent(&options, where_clause.as_deref())?;
            let outcome = self.execute_durable_distributed_rewrite(
                engine,
                target,
                intent,
                DistributedRewriteOperationKind::RewritePositionDeleteFiles,
                None,
            )?;
            return if spark_result {
                action_result(outcome)
            } else {
                Ok(MaintenanceStatementResult::Ok)
            };
        }
        if let ParsedMaintenanceAction::RemoveOrphanFiles { older_than_ms } = action.clone() {
            let outcome = self.execute_durable_cleanup(engine, target, older_than_ms)?;
            return if spark_result {
                action_result(outcome)
            } else {
                Ok(MaintenanceStatementResult::Ok)
            };
        }
        let request = action.into_request(engine, target)?;
        let outcome = engine.execute_action(request)?;
        if spark_result {
            action_result(outcome)
        } else {
            Ok(MaintenanceStatementResult::Ok)
        }
    }

    fn execute_durable_cleanup(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
        older_than_ms: i64,
    ) -> Result<MaintenanceActionOutcome, String> {
        let repository = self
            .cleanup_repository
            .as_ref()
            .ok_or_else(|| CLEANUP_STATE_STORE_REQUIRED.to_string())?;
        let operation_id = ConnectorCleanupOperationId::new();
        let durable_id = uuid::Uuid::from_bytes(operation_id.to_bytes());
        let session = engine.plan_cleanup_maintenance(&target, operation_id, older_than_ms)?;
        let plan = session.plan_ref();
        let owner = MetadataMaintenanceExactOwner {
            instance_id: plan.owner().instance_id.as_str().to_string(),
            incarnation_id: uuid::Uuid::from_bytes(plan.owner().incarnation.to_bytes()),
        };
        self.block_on(repository.create(CleanupOperationCreate {
            operation_id: durable_id,
            target: target.clone(),
            owner,
            request_digest: plan.request_digest(),
            older_than_ms,
            created_at_ms: now_unix_millis(),
        }))
        .map_err(|error| format!("persist orphan cleanup pending operation failed: {error}"))?;
        let candidate_count = u32::try_from(plan.summary().candidate_count())
            .map_err(|_| "orphan cleanup candidate count exceeds durable limit".to_string())?;
        let batch_count = u16::try_from(plan.summary().batch_count())
            .map_err(|_| "orphan cleanup batch count exceeds durable limit".to_string())?;
        let manifest_parts = u16::try_from(plan.summary().manifest_parts())
            .map_err(|_| "orphan cleanup manifest part count exceeds durable limit".to_string())?;
        let artifact_handle = plan.provider_payload().to_vec();
        self.block_on(repository.plan(
            durable_id,
            CleanupPlanPayload {
                plan_digest: plan.plan_digest(),
                base_state_digest: plan.base_state_digest(),
                manifest_digest: plan.manifest_digest(),
                artifact_handle_digest: cleanup_payload_digest(&artifact_handle),
                artifact_handle,
                candidate_count,
                total_bytes: plan.summary().total_bytes(),
                manifest_parts,
                batch_count,
            },
            now_unix_millis(),
        ))
        .map_err(|error| format!("persist orphan cleanup plan failed: {error}"))?;

        if batch_count == 0 {
            let locations = cleanup_candidate_locations(engine, &session)?;
            self.block_on(repository.finish(durable_id, now_unix_millis()))
                .map_err(|error| {
                    format!("persist zero-candidate cleanup finish failed: {error}")
                })?;
            if let Err(error) = engine.finalize_cleanup_terminal(&session) {
                tracing::warn!(%error, operation_id = %durable_id, "orphan cleanup terminal artifact finalization failed");
            }
            return Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
                orphan_file_locations: locations,
            });
        }

        for ordinal in 0..batch_count {
            let prepared = engine.prepare_cleanup_batch(&session, u32::from(ordinal))?;
            let prepared_handle = prepared
                .try_to_wire_v1()
                .map_err(|error| format!("encode orphan cleanup prepared evidence: {error}"))?
                .to_vec();
            let prepared_checkpoint = CleanupBatchCheckpoint {
                ordinal,
                prepared_handle_digest: cleanup_payload_digest(&prepared_handle),
                prepared_handle,
                receipt_handle_digest: None,
                receipt_handle: None,
                deleted_count: 0,
                already_absent_count: 0,
                failed_count: 0,
                unknown_count: 0,
            };
            self.block_on(repository.prepare_batch(
                durable_id,
                prepared_checkpoint.clone(),
                now_unix_millis(),
            ))
            .map_err(|error| format!("persist orphan cleanup prepared batch failed: {error}"))?;
            match engine.execute_cleanup_batch(&session, prepared)? {
                CleanupBatchExecution::Receipt(receipt) => {
                    let checkpoint = cleanup_receipt_checkpoint(prepared_checkpoint, &receipt);
                    if claim_cleanup_fault(CleanupFaultKind::CheckpointFailed)
                        .map_err(|error| format!("claim cleanup checkpoint fault: {error}"))?
                    {
                        return Err("debug cleanup checkpoint write failed; exact-generation reconciliation is required".to_string());
                    }
                    let operation = self
                        .block_on(repository.checkpoint_batch(durable_id, checkpoint))
                        .map_err(|error| {
                            format!("persist orphan cleanup batch receipt failed: {error}")
                        })?;
                    if operation.state == CleanupOperationState::ReconcilePending {
                        return Err("orphan cleanup batch outcome is unknown and requires exact-generation reconciliation".to_string());
                    }
                }
                CleanupBatchExecution::Uncertain(error) => {
                    self.block_on(repository.mark_reconcile_pending(durable_id, now_unix_millis()))
                        .map_err(|store| {
                            format!("persist orphan cleanup uncertain dispatch failed: {store}")
                        })?;
                    return Err(format!(
                        "orphan cleanup dispatch outcome is unknown: {error}"
                    ));
                }
            }
        }
        let locations = cleanup_candidate_locations(engine, &session)?;
        self.block_on(repository.finish(durable_id, now_unix_millis()))
            .map_err(|error| format!("persist orphan cleanup terminal state failed: {error}"))?;
        if let Err(error) = engine.finalize_cleanup_terminal(&session) {
            tracing::warn!(%error, operation_id = %durable_id, "orphan cleanup terminal artifact finalization failed");
        }
        Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
            orphan_file_locations: locations,
        })
    }

    fn execute_durable_distributed_rewrite(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
        intent: DistributedRewriteIntent,
        kind: DistributedRewriteOperationKind,
        claimed_optimize_job_id: Option<i64>,
    ) -> Result<MaintenanceActionOutcome, String> {
        let repository = self
            .distributed_rewrite_repository
            .as_ref()
            .ok_or_else(|| DISTRIBUTED_REWRITE_STATE_STORE_REQUIRED.to_string())?;
        let operation_id = ConnectorWriteOperationId::new();
        let durable_id = uuid::Uuid::from_bytes(operation_id.to_bytes());
        let request_payload = distributed_rewrite_request_payload(intent);
        let session = engine.plan_distributed_rewrite(&target, operation_id, intent)?;
        let plan = session.plan();
        let create = DistributedRewriteOperationCreate {
            operation_id: durable_id,
            target: target.clone(),
            owner: MetadataMaintenanceExactOwner {
                instance_id: plan.owner().instance_id.as_str().to_string(),
                incarnation_id: uuid::Uuid::from_bytes(plan.owner().incarnation.to_bytes()),
            },
            kind,
            request_digest: plan.request_digest(),
            base_state_digest: plan.state_digest(),
            request_payload_digest: distributed_rewrite_payload_digest(&request_payload),
            request_payload,
            created_at_ms: now_unix_millis(),
        };
        let created = match claimed_optimize_job_id {
            Some(job_id) => {
                self.block_on(repository.create_for_claimed_optimize_job(create, job_id))
            }
            None => self.block_on(repository.create(create)),
        };
        created.map_err(|error| {
            format!("persist distributed rewrite pending operation failed: {error}")
        })?;
        let plan_payload = plan.provider_payload().to_vec();
        self.block_on(
            repository.plan(
                durable_id,
                DistributedRewritePlanPayload {
                    plan_digest: plan.plan_digest(),
                    manifest_digest: plan.manifest_digest(),
                    cohort_set_digest: session.cohort_set_digest(),
                    payload_digest: distributed_rewrite_payload_digest(&plan_payload),
                    payload: plan_payload,
                    cohort_count: u32::try_from(plan.cohorts().len())
                        .map_err(|_| "distributed rewrite cohort count exceeds u32".to_string())?,
                },
                now_unix_millis(),
            ),
        )
        .map_err(|error| format!("persist distributed rewrite plan failed: {error}"))?;

        if session.is_noop() {
            let receipt = b"distributed-rewrite-noop-v1".to_vec();
            self.block_on(repository.finish(
                durable_id,
                DistributedRewriteOpaquePayload {
                    digest: distributed_rewrite_payload_digest(&receipt),
                    payload: receipt,
                },
                now_unix_millis(),
            ))
            .map_err(|error| {
                format!("persist distributed rewrite no-op receipt failed: {error}")
            })?;
            return rewrite_noop_outcome(kind);
        }

        self.block_on(repository.start_staging(durable_id, now_unix_millis()))
            .map_err(|error| {
                format!("persist distributed rewrite staging state failed: {error}")
            })?;
        for cohort in plan.cohorts() {
            let completion =
                match engine.stage_distributed_rewrite_cohort(&session, cohort.cohort_id()) {
                    Ok(completion) => completion,
                    Err(error) => {
                        return self.abort_failed_distributed_rewrite(
                            engine, repository, durable_id, &session, error,
                        );
                    }
                };
            let checkpoint =
                match engine.checkpoint_distributed_rewrite_attempt(&session, &completion) {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        return self.abort_failed_distributed_rewrite(
                            engine, repository, durable_id, &session, error,
                        );
                    }
                };
            self.block_on(
                repository
                    .checkpoint_attempt(durable_id, distributed_rewrite_checkpoint(checkpoint)),
            )
            .map_err(|error| {
                format!("persist distributed rewrite attempt checkpoint failed: {error}")
            })?;
        }
        self.block_on(repository.mark_commit_pending(durable_id, now_unix_millis()))
            .map_err(|error| format!("persist distributed rewrite commit state failed: {error}"))?;
        match engine.commit_distributed_rewrite(&session)? {
            ExternalMutationOutcome::KnownCommitted {
                receipt,
                finalization,
                ..
            } => {
                let rewrite_receipt = engine.finalize_distributed_rewrite(&session, &receipt)?;
                let payload = rewrite_receipt.provider_payload().to_vec();
                self.block_on(repository.finish(
                    durable_id,
                    DistributedRewriteOpaquePayload {
                        digest: distributed_rewrite_payload_digest(&payload),
                        payload,
                    },
                    now_unix_millis(),
                ))
                .map_err(|error| format!("persist distributed rewrite receipt failed: {error}"))?;
                if let ExternalMutationFinalization::Failed(failure) = finalization {
                    return Err(format!(
                        "distributed rewrite committed but finalization failed: {failure}"
                    ));
                }
                rewrite_outcome_from_receipt(kind, rewrite_receipt.summary(), plan.summary())
            }
            ExternalMutationOutcome::KnownUncommitted { failure } => self
                .abort_failed_distributed_rewrite(
                    engine,
                    repository,
                    durable_id,
                    &session,
                    format!("distributed rewrite commit was not applied: {failure}"),
                ),
            ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
                let payload = evidence
                    .try_to_wire_v1()
                    .map_err(|error| error.to_string())?
                    .to_vec();
                let evidence = ExternalMutationEvidence::try_from_wire_v1(&payload)
                    .map_err(|error| format!("restore distributed rewrite evidence: {error}"))?;
                self.block_on(repository.mark_reconcile_pending(
                    durable_id,
                    DistributedRewriteOpaquePayload {
                        digest: distributed_rewrite_payload_digest(&payload),
                        payload,
                    },
                    now_unix_millis(),
                ))
                .map_err(|error| {
                    format!("persist distributed rewrite reconcile state failed: {error}")
                })?;
                match engine.reconcile_distributed_rewrite(&session, evidence)? {
                    ExternalMutationOutcome::KnownCommitted {
                        receipt,
                        finalization,
                        ..
                    } => {
                        let rewrite_receipt =
                            engine.finalize_distributed_rewrite(&session, &receipt)?;
                        let payload = rewrite_receipt.provider_payload().to_vec();
                        self.block_on(repository.finish(
                            durable_id,
                            DistributedRewriteOpaquePayload {
                                digest: distributed_rewrite_payload_digest(&payload),
                                payload,
                            },
                            now_unix_millis(),
                        ))
                        .map_err(|error| {
                            format!("persist reconciled distributed rewrite receipt failed: {error}")
                        })?;
                        if let ExternalMutationFinalization::Failed(finalization) = finalization {
                            return Err(format!(
                                "distributed rewrite reconciled as committed but finalization failed: {finalization}"
                            ));
                        }
                        rewrite_outcome_from_receipt(kind, rewrite_receipt.summary(), plan.summary())
                    }
                    ExternalMutationOutcome::KnownUncommitted { failure: reconcile_failure } => {
                        self.abort_failed_distributed_rewrite(
                            engine,
                            repository,
                            durable_id,
                            &session,
                            format!(
                                "distributed rewrite commit was not applied after reconcile: {reconcile_failure}"
                            ),
                        )
                    }
                    ExternalMutationOutcome::CommitUnknown { failure: reconcile_failure, .. } => {
                        Err(format!(
                            "distributed rewrite commit outcome remains unknown after reconcile: {failure}; {reconcile_failure}"
                        ))
                    }
                }
            }
        }
    }

    fn abort_failed_distributed_rewrite(
        &self,
        engine: &dyn TableMaintenanceEngine,
        repository: &DistributedRewriteOperationRepository,
        operation_id: uuid::Uuid,
        session: &novarocks::connector::distributed_rewrite_application::DistributedRewriteMaintenanceSession,
        error: String,
    ) -> Result<MaintenanceActionOutcome, String> {
        self.block_on(repository.mark_abort_pending(operation_id, now_unix_millis()))
            .map_err(|store| format!("persist distributed rewrite abort state failed: {store}"))?;
        match engine.abort_distributed_rewrite(session) {
            Ok(_) => {
                self.block_on(repository.fail(operation_id, error.clone(), now_unix_millis()))
                    .map_err(|store| {
                        format!("persist distributed rewrite failure failed: {store}")
                    })?;
                Err(error)
            }
            Err(abort) => {
                self.block_on(repository.mark_unresolved(
                    operation_id,
                    format!("{error}; abort unresolved: {abort}"),
                    now_unix_millis(),
                ))
                .map_err(|store| {
                    format!("persist distributed rewrite unresolved state failed: {store}")
                })?;
                Err(format!(
                    "{error}; distributed rewrite abort unresolved: {abort}"
                ))
            }
        }
    }

    /// The automatic optimize worker has its own durable-job owner, but its
    /// external rewrite must still use this exact same distributed-rewrite
    /// operation path.  The temporary service has no worker lifecycle and is
    /// used only to share the fenced C1 transaction implementation.
    pub(crate) fn execute_optimize_distributed_rewrite(
        runtime: &Handle,
        distributed_rewrite_repository: Arc<DistributedRewriteOperationRepository>,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
        claimed_optimize_job_id: i64,
    ) -> Result<MaintenanceActionOutcome, String> {
        let service = Self {
            repository: None,
            metadata_repository: None,
            distributed_rewrite_repository: Some(distributed_rewrite_repository),
            cleanup_repository: None,
            worker: Mutex::new(WorkerLifecycle::NotStarted),
            runtime: runtime.clone(),
        };
        service.execute_durable_distributed_rewrite(
            engine,
            target,
            DistributedRewriteIntent::DataFiles { rewrite_all: true },
            DistributedRewriteOperationKind::RewriteDataFiles,
            Some(claimed_optimize_job_id),
        )
    }

    fn execute_durable_metadata_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
        intent: MetadataMaintenanceIntent,
        kind: MetadataMaintenanceOperationKind,
        use_caching: Option<bool>,
        spec_id: Option<i32>,
    ) -> Result<MaintenanceActionOutcome, String> {
        if use_caching.is_some() {
            return Err(
                "rewrite_manifests `use_caching` is not implemented in NovaRocks yet".to_string(),
            );
        }
        if spec_id.is_some() {
            return Err(
                "rewrite_manifests `spec_id` is not implemented in NovaRocks yet".to_string(),
            );
        }
        let repository = self
            .metadata_repository
            .as_ref()
            .ok_or_else(|| METADATA_MAINTENANCE_STATE_STORE_REQUIRED.to_string())?;
        let operation_id = ConnectorMutationOperationId::new();
        let durable_id = uuid::Uuid::from_bytes(operation_id.to_bytes());
        let session = engine.plan_metadata_maintenance(&target, operation_id, intent)?;
        let plan = session.plan_ref();
        let request_payload = plan.provider_payload().to_vec();
        let request_payload_digest = metadata_maintenance_payload_digest(&request_payload);
        self.block_on(repository.create(MetadataMaintenanceOperationCreate {
            operation_id: durable_id,
            target: target.clone(),
            owner: MetadataMaintenanceExactOwner {
                instance_id: plan.owner().instance_id.as_str().to_string(),
                incarnation_id: uuid::Uuid::from_bytes(plan.owner().incarnation.to_bytes()),
            },
            kind,
            request_digest: plan.request_digest(),
            request_payload_digest,
            base_state_digest: plan.state_digest(),
            request_payload,
            created_at_ms: now_unix_millis(),
        }))
        .map_err(|error| {
            format!("persist metadata maintenance pending operation failed: {error}")
        })?;
        let plan_payload = plan.provider_payload().to_vec();
        self.block_on(repository.start(
            durable_id,
            MetadataMaintenancePlanPayload {
                plan_digest: plan.plan_digest(),
                payload_digest: metadata_maintenance_payload_digest(&plan_payload),
                payload: plan_payload,
                summary: [
                    plan.summary().source_items(),
                    plan.summary().replacement_items(),
                    plan.summary().candidate_versions(),
                    plan.summary().cleanup_candidates(),
                    plan.summary().total_bytes(),
                ],
            },
            now_unix_millis(),
        ))
        .map_err(|error| format!("persist metadata maintenance plan failed: {error}"))?;
        match engine.execute_planned_metadata_maintenance(session) {
            Ok(completed) => {
                let receipt = completed.receipt;
                self.block_on(repository.finish(
                    durable_id,
                    MetadataMaintenanceOpaquePayload {
                        digest: metadata_maintenance_payload_digest(receipt.provider_payload()),
                        payload: receipt.provider_payload().to_vec(),
                    },
                    now_unix_millis(),
                ))
                .map_err(|error| format!("persist metadata maintenance receipt failed: {error}"))?;
                let summary = receipt.summary();
                Ok(match kind {
                    MetadataMaintenanceOperationKind::RewriteMetadataLayout => {
                        MaintenanceActionOutcome::RewriteManifests {
                            rewritten_manifests_count: i32::try_from(summary.rewritten_items)
                                .map_err(|_| {
                                    "rewrite manifest count exceeds Spark result range".to_string()
                                })?,
                            added_manifests_count: i32::try_from(summary.added_items).map_err(
                                |_| "added manifest count exceeds Spark result range".to_string(),
                            )?,
                        }
                    }
                    MetadataMaintenanceOperationKind::ExpireTableVersions => {
                        MaintenanceActionOutcome::ExpireSnapshots {
                            deleted_data_files_count: None,
                            deleted_position_delete_files_count: None,
                            deleted_equality_delete_files_count: None,
                            deleted_manifest_files_count: None,
                            deleted_manifest_lists_count: None,
                            deleted_statistics_files_count: None,
                        }
                    }
                })
            }
            Err(error) => {
                // The engine-facing compatibility port deliberately does not
                // claim that a provider error happened before dispatch.
                // Preserve the operation fence and opaque evidence for an
                // exact-generation reconcile owner.
                let evidence = error.as_bytes().to_vec();
                self.block_on(repository.mark_reconcile_pending(
                    durable_id,
                    MetadataMaintenanceOpaquePayload {
                        digest: metadata_maintenance_payload_digest(&evidence),
                        payload: evidence,
                    },
                ))
                .map_err(|store| {
                    format!("record metadata maintenance reconcile state failed: {store}")
                })?;
                Err(error)
            }
        }
    }

    fn submit_user_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<MaintenanceStatementResult, String> {
        engine.reject_user_action_on_mv(&target)?;
        self.submit_user_optimize_inner(engine, target)?;
        Ok(MaintenanceStatementResult::Ok)
    }

    fn submit_user_optimize_inner(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<(), String> {
        let repository = self
            .repository
            .as_ref()
            .ok_or_else(|| OPTIMIZE_STATE_STORE_REQUIRED.to_string())?;
        let base_snapshot_id = engine.current_snapshot_id(&target)?;
        let request = OptimizeJobCreate {
            target: target.clone(),
            base_snapshot_id,
            created_at_ms: now_unix_millis(),
        };
        match self.block_on(repository.create(request)) {
            Ok(_) => {
                self.wakeup_worker()?;
                Ok(())
            }
            Err(error) if error.kind() == RepositoryErrorKind::AlreadyActive => Err(format!(
                "ALTER TABLE OPTIMIZE: create iceberg optimize job failed: {error}"
            )),
            Err(error) => Err(format!(
                "create frontend optimize job for {}.{}.{} failed: {error}",
                target.catalog, target.namespace, target.table
            )),
        }
    }

    fn submit_automatic_optimize_inner(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        let repository = self
            .repository
            .as_ref()
            .ok_or_else(|| AUTOMATIC_OPTIMIZE_STATE_STORE_REQUIRED.to_string())?;
        let base_snapshot_id = engine.current_snapshot_id(&target)?;
        match self.block_on(repository.create(OptimizeJobCreate {
            target,
            base_snapshot_id,
            created_at_ms: now_unix_millis(),
        })) {
            Ok(job) => {
                self.wakeup_worker()?;
                Ok(OptimizeSubmission::Submitted { job_id: job.job_id })
            }
            Err(error) if error.kind() == RepositoryErrorKind::AlreadyActive => {
                Ok(OptimizeSubmission::AlreadyActive)
            }
            Err(error) => Err(format!("submit automatic optimize failed: {error}")),
        }
    }

    fn show_optimize(
        &self,
        sql: &str,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        let repository = self
            .repository
            .as_ref()
            .ok_or_else(|| SHOW_STATE_STORE_REQUIRED.to_string())?;
        let statement = parse_show_optimize(sql)?;
        let mut jobs = self
            .block_on(repository.list())
            .map_err(|error| format!("show frontend optimize jobs failed: {error}"))?;
        let catalog_filter = statement.catalog.as_deref().or(context.current_catalog);
        let database_filter = statement
            .database
            .as_deref()
            .unwrap_or(context.current_database);
        if let Some(catalog) = catalog_filter {
            jobs.retain(|job| job.target.catalog == catalog);
        }
        jobs.retain(|job| job.target.namespace == database_filter);
        if let Some(table_name) = statement.table_name.as_deref() {
            jobs.retain(|job| job.target.table == table_name);
        }
        jobs.sort_by_key(|job| (job.created_at_ms, job.job_id));
        if statement.order_by_create_time_desc {
            jobs.reverse();
        }
        if let Some(limit) = statement.limit {
            jobs.truncate(limit);
        }
        optimize_jobs_result(jobs)
    }

    fn wakeup_worker(&self) -> Result<(), String> {
        let worker = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        if let WorkerLifecycle::Started(Some(worker)) = &*worker {
            worker.wakeup();
        }
        Ok(())
    }

    fn recover_metadata_operations(
        &self,
        engine: &dyn TableMaintenanceEngine,
    ) -> Result<(), String> {
        let Some(repository) = &self.metadata_repository else {
            return Ok(());
        };
        for operation in self
            .block_on(repository.list_reconcile_candidates())
            .map_err(|error| {
                format!("list metadata maintenance recovery candidates failed: {error}")
            })?
        {
            let result = (|| -> Result<(), String> {
                let stored = self
                    .block_on(repository.load_plan(operation.operation_id))
                    .map_err(|error| {
                        format!("load metadata maintenance recovery plan failed: {error}")
                    })?
                    .ok_or_else(|| "metadata maintenance recovery plan is missing".to_string())?;
                let kind = match operation.kind {
                    MetadataMaintenanceOperationKind::RewriteMetadataLayout => {
                        novarocks_spi::connector::REWRITE_METADATA_LAYOUT_KIND
                    }
                    MetadataMaintenanceOperationKind::ExpireTableVersions => {
                        novarocks_spi::connector::EXPIRE_TABLE_VERSIONS_KIND
                    }
                };
                let plan = ConnectorMetadataMaintenancePlan::try_restore(
                    ConnectorExecutionBindingKey {
                        instance_id: ConnectorInstanceId::parse(&operation.owner.instance_id)
                            .map_err(|error| error.to_string())?,
                        incarnation: ConnectorInstanceIncarnation::from_bytes(
                            *operation.owner.incarnation_id.as_bytes(),
                        ),
                    },
                    ConnectorMutationOperationId::from_bytes(*operation.operation_id.as_bytes()),
                    kind,
                    operation.request_digest,
                    operation.base_state_digest,
                    ConnectorMetadataMaintenancePlanSummary::new(
                        stored.summary[0],
                        stored.summary[1],
                        stored.summary[2],
                        stored.summary[3],
                        stored.summary[4],
                    ),
                    bytes::Bytes::from(stored.payload),
                    stored.plan_digest,
                )
                .map_err(|error| error.to_string())?;
                let completed = engine.reconcile_metadata_maintenance(&operation.target, plan)?;
                let receipt = completed.receipt;
                self.block_on(repository.finish(
                    operation.operation_id,
                    MetadataMaintenanceOpaquePayload {
                        digest: metadata_maintenance_payload_digest(receipt.provider_payload()),
                        payload: receipt.provider_payload().to_vec(),
                    },
                    now_unix_millis(),
                ))
                .map_err(|error| {
                    format!("persist recovered metadata maintenance receipt failed: {error}")
                })?;
                Ok(())
            })();
            if let Err(error) = result {
                self.block_on(repository.mark_unresolved(
                    operation.operation_id,
                    error,
                    now_unix_millis(),
                ))
                .map_err(|store| {
                    format!("mark metadata maintenance operation unresolved failed: {store}")
                })?;
            }
        }
        Ok(())
    }

    fn recover_distributed_rewrite_operations(&self) -> Result<(), String> {
        let Some(repository) = &self.distributed_rewrite_repository else {
            return Ok(());
        };
        // A rewrite lease is generation-fenced. After a frontend restart the
        // old incarnation is intentionally unavailable, so this owner must
        // not substitute the current binding or resubmit staged work. Keep
        // the durable fence visible for an exact-generation recovery design.
        for operation in self
            .block_on(repository.list_recovery_candidates())
            .map_err(|error| {
                format!("list distributed rewrite recovery candidates failed: {error}")
            })?
        {
            self.block_on(repository.mark_unresolved(
                operation.operation_id,
                "distributed rewrite requires its original exact connector generation after frontend restart".to_string(),
                now_unix_millis(),
            ))
            .map_err(|error| format!("mark distributed rewrite operation unresolved failed: {error}"))?;
        }
        Ok(())
    }

    fn recover_cleanup_operations(
        &self,
        engine: &dyn TableMaintenanceEngine,
    ) -> Result<(), String> {
        let Some(repository) = &self.cleanup_repository else {
            return Ok(());
        };
        for operation in self
            .block_on(repository.list_recovery_candidates())
            .map_err(|error| format!("list orphan cleanup recovery candidates failed: {error}"))?
        {
            let result = (|| -> Result<(), String> {
                let stored_plan = self
                    .block_on(repository.load_plan(operation.operation_id))
                    .map_err(|error| format!("load orphan cleanup recovery plan failed: {error}"))?
                    .ok_or_else(|| "orphan cleanup recovery plan is missing".to_string())?;
                let ordinal = if operation.state == CleanupOperationState::ReconcilePending {
                    operation.next_batch_ordinal.saturating_sub(1)
                } else {
                    operation.next_batch_ordinal
                };
                let checkpoint = self
                    .block_on(repository.load_batch(operation.operation_id, ordinal))
                    .map_err(|error| {
                        format!("load orphan cleanup recovery batch failed: {error}")
                    })?;
                let Some(checkpoint) = checkpoint else {
                    // A Running operation can have completed a checkpoint and
                    // not yet prepared its next batch when FE stops. There is
                    // no destructive dispatch to reconcile in that state, and
                    // startup must not manufacture an Unresolved fence for it.
                    // ReconcilePending, by contrast, always denotes a durable
                    // prepared batch and is corrupt without one.
                    if operation.state == CleanupOperationState::Running {
                        return Ok(());
                    }
                    return Err("orphan cleanup recovery has no prepared batch".to_string());
                };
                let plan = cleanup_plan_from_durable(&operation, stored_plan)?;
                let prepared = PreparedBatch::try_from_wire_v1(bytes::Bytes::from(
                    checkpoint.prepared_handle.clone(),
                ))
                .map_err(|error| format!("restore orphan cleanup prepared evidence: {error}"))?;
                let session = engine.recover_cleanup_for_reconcile(
                    &operation.target,
                    plan,
                    prepared.clone(),
                )?;
                let receipt = engine.reconcile_cleanup_batch(&session, prepared)?;
                let resolved = cleanup_receipt_checkpoint(checkpoint, &receipt);
                let operation = self
                    .block_on(
                        repository.checkpoint_reconciled_batch(operation.operation_id, resolved),
                    )
                    .map_err(|error| {
                        format!("persist reconciled orphan cleanup receipt failed: {error}")
                    })?;
                if operation.state == CleanupOperationState::ReconcilePending {
                    return Err("orphan cleanup reconcile outcome remains unknown".to_string());
                }
                if operation.next_batch_ordinal == operation.batch_count.unwrap_or(0) {
                    self.block_on(repository.finish(operation.operation_id, now_unix_millis()))
                        .map_err(|error| {
                            format!(
                                "persist recovered orphan cleanup terminal state failed: {error}"
                            )
                        })?;
                    if let Err(error) = engine.finalize_cleanup_terminal(&session) {
                        tracing::warn!(%error, operation_id = %operation.operation_id, "orphan cleanup recovered terminal artifact finalization failed");
                    }
                }
                Ok(())
            })();
            if let Err(error) = result {
                self.block_on(repository.mark_unresolved(
                    operation.operation_id,
                    error,
                    now_unix_millis(),
                ))
                .map_err(|store| {
                    format!("mark orphan cleanup operation unresolved failed: {store}")
                })?;
            }
        }
        Ok(())
    }
}

impl TableMaintenanceService for FrontendTableMaintenanceService {
    fn start(&self, engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
        self.recover_metadata_operations(engine.as_ref())?;
        self.recover_distributed_rewrite_operations()?;
        self.recover_cleanup_operations(engine.as_ref())?;
        let mut worker = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        match &*worker {
            WorkerLifecycle::NotStarted => {
                let optimize_worker = self
                    .repository
                    .as_ref()
                    .map(|repository| {
                        let distributed_rewrite_repository = self
                            .distributed_rewrite_repository
                            .as_ref()
                            .ok_or_else(|| DISTRIBUTED_REWRITE_STATE_STORE_REQUIRED.to_string())?;
                        OptimizeWorker::start(
                            &self.runtime,
                            Arc::clone(repository),
                            Arc::clone(distributed_rewrite_repository),
                            Arc::downgrade(&engine),
                        )
                    })
                    .transpose()?;
                *worker = WorkerLifecycle::Started(optimize_worker);
                Ok(())
            }
            WorkerLifecycle::Started(_) => {
                Err("table maintenance service is already started".to_string())
            }
            WorkerLifecycle::Stopped(_) => {
                Err("table maintenance service cannot be restarted after shutdown".to_string())
            }
        }
    }

    fn try_handle_statement(
        &self,
        engine: &dyn TableMaintenanceEngine,
        sql: &str,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<Option<MaintenanceStatementResult>, String> {
        let Some(statement) = parse_maintenance_statement(sql, context)? else {
            return Ok(None);
        };
        let result = match statement {
            ParsedMaintenanceStatement::Execute { name_parts, action } => {
                let target = engine.resolve_target(&name_parts, context)?;
                self.execute_user_action(engine, target, action, is_spark_maintenance_call(sql))?
            }
            ParsedMaintenanceStatement::SubmitOptimize { name_parts } => {
                if self.repository.is_none() {
                    return Err(OPTIMIZE_STATE_STORE_REQUIRED.to_string());
                }
                let target = engine.resolve_target(&name_parts, context)?;
                self.submit_user_optimize(engine, target)?
            }
            ParsedMaintenanceStatement::ShowOptimize => self.show_optimize(sql, context)?,
        };
        Ok(Some(result))
    }

    fn execute_automatic_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        match request {
            MaintenanceActionRequest::RewriteDataFiles {
                target,
                options,
                branch,
                where_clause,
                ..
            } => self.execute_durable_distributed_rewrite(
                engine,
                target,
                distributed_data_rewrite_intent(
                    &options,
                    branch.as_deref(),
                    where_clause.as_deref(),
                )?,
                DistributedRewriteOperationKind::RewriteDataFiles,
                None,
            ),
            MaintenanceActionRequest::RewritePositionDeleteFiles {
                target,
                options,
                where_clause,
            } => self.execute_durable_distributed_rewrite(
                engine,
                target,
                distributed_position_rewrite_intent(&options, where_clause.as_deref())?,
                DistributedRewriteOperationKind::RewritePositionDeleteFiles,
                None,
            ),
            MaintenanceActionRequest::RemoveOrphanFiles {
                target,
                older_than_ms,
            } => self.execute_durable_cleanup(engine, target, older_than_ms),
            MaintenanceActionRequest::ExpireSnapshots {
                target,
                older_than_ms,
                retain_last,
            } => self.execute_durable_metadata_action(
                engine,
                target,
                MetadataMaintenanceIntent::expire_table_versions(older_than_ms, retain_last),
                MetadataMaintenanceOperationKind::ExpireTableVersions,
                None,
                None,
            ),
            other => Err(format!(
                "automatic maintenance action has no durable lifecycle route: {other:?}"
            )),
        }
    }

    fn submit_automatic_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        self.submit_automatic_optimize_inner(engine, target)
    }

    fn execute_automatic_optimize_durably(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        let repository = self
            .repository
            .as_ref()
            .ok_or_else(|| AUTOMATIC_OPTIMIZE_STATE_STORE_REQUIRED.to_string())?;
        let distributed_rewrite_repository = self
            .distributed_rewrite_repository
            .as_ref()
            .ok_or_else(|| DISTRIBUTED_REWRITE_STATE_STORE_REQUIRED.to_string())?;
        let base_snapshot_id = engine.current_snapshot_id(&target)?;
        let job = match self.block_on(repository.create(OptimizeJobCreate {
            target: target.clone(),
            base_snapshot_id,
            created_at_ms: now_unix_millis(),
        })) {
            Ok(job) => job,
            Err(error) if error.kind() == RepositoryErrorKind::AlreadyActive => {
                return Ok(OptimizeSubmission::AlreadyActive);
            }
            Err(error) => return Err(format!("create automatic optimize job failed: {error}")),
        };
        let claimed = self
            .block_on(repository.claim(job.job_id, now_unix_millis()))
            .map_err(|error| {
                format!(
                    "claim automatic optimize job {} failed: {error}",
                    job.job_id
                )
            })?
            .ok_or_else(|| {
                format!(
                    "automatic optimize job {} disappeared before claim",
                    job.job_id
                )
            })?;
        let outcome = Self::execute_optimize_distributed_rewrite(
            &self.runtime,
            Arc::clone(distributed_rewrite_repository),
            engine,
            target,
            claimed.job_id,
        );
        match outcome {
            Ok(outcome) => {
                let outcome = worker::optimize_outcome(outcome)?;
                self.block_on(repository.record_outcome(claimed.job_id, outcome))
                    .map_err(|error| {
                        format!("record automatic optimize outcome failed: {error}")
                    })?;
                self.block_on(repository.finish(claimed.job_id, now_unix_millis()))
                    .map_err(|error| format!("finish automatic optimize job failed: {error}"))?;
                Ok(OptimizeSubmission::Submitted {
                    job_id: claimed.job_id,
                })
            }
            Err(error) => {
                // The rewrite application port deliberately does not reduce a
                // failed external mutation to a string class here.  Leaving
                // the claimed durable job non-terminal preserves recovery
                // evidence for both commit-unknown and finalize failures;
                // marking it failed would fabricate a safe terminal state.
                Err(format!(
                    "automatic optimize job {} requires recovery: {error}",
                    claimed.job_id
                ))
            }
        }
    }

    fn shutdown(&self) -> Result<(), String> {
        let mut worker = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        let lifecycle = std::mem::replace(&mut *worker, WorkerLifecycle::Stopped(Ok(())));
        drop(worker);

        let result = match lifecycle {
            WorkerLifecycle::NotStarted => Ok(()),
            WorkerLifecycle::Started(Some(mut worker)) => worker.shutdown(),
            WorkerLifecycle::Started(None) => Ok(()),
            WorkerLifecycle::Stopped(result) => result,
        };
        let mut worker = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        *worker = WorkerLifecycle::Stopped(result.clone());
        result
    }
}

impl ParsedMaintenanceAction {
    fn into_request(
        self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<MaintenanceActionRequest, String> {
        match self {
            Self::RewriteDataFiles {
                options,
                branch,
                where_clause,
            } => Ok(MaintenanceActionRequest::RewriteDataFiles {
                base_snapshot_id: engine.current_snapshot_id(&target)?,
                target,
                job_id: None,
                options,
                branch,
                where_clause,
            }),
            Self::RewriteManifests {
                use_caching,
                spec_id,
            } => Ok(MaintenanceActionRequest::RewriteManifests {
                target,
                use_caching,
                spec_id,
            }),
            Self::ExpireSnapshots {
                older_than_ms,
                retain_last,
            } => Ok(MaintenanceActionRequest::ExpireSnapshots {
                target,
                older_than_ms,
                retain_last,
            }),
            Self::RemoveOrphanFiles { older_than_ms } => {
                Ok(MaintenanceActionRequest::RemoveOrphanFiles {
                    target,
                    older_than_ms,
                })
            }
            Self::RewritePositionDeleteFiles {
                options,
                where_clause,
            } => Ok(MaintenanceActionRequest::RewritePositionDeleteFiles {
                target,
                options,
                where_clause,
            }),
        }
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn cleanup_receipt_checkpoint(
    mut checkpoint: CleanupBatchCheckpoint,
    receipt: &BatchReceipt,
) -> CleanupBatchCheckpoint {
    let handle = receipt.provider_payload().to_vec();
    let summary = receipt.summary();
    checkpoint.receipt_handle_digest = Some(cleanup_payload_digest(&handle));
    checkpoint.receipt_handle = Some(handle);
    checkpoint.deleted_count = summary.deleted();
    checkpoint.already_absent_count = summary.already_absent();
    checkpoint.failed_count = summary.failed();
    checkpoint.unknown_count = summary.unknown();
    checkpoint
}

fn cleanup_candidate_locations(
    engine: &dyn TableMaintenanceEngine,
    session: &novarocks::connector::cleanup_maintenance::CleanupMaintenanceSession,
) -> Result<Vec<String>, String> {
    let mut offset = 0u64;
    let mut locations = Vec::new();
    loop {
        let page = engine.read_cleanup_candidate_page(session, offset, 1024)?;
        locations.extend(page.locations().iter().map(|location| location.to_string()));
        if page.complete() {
            return Ok(locations);
        }
        offset = offset
            .checked_add(page.locations().len() as u64)
            .ok_or_else(|| "orphan cleanup candidate page offset overflow".to_string())?;
    }
}

fn cleanup_plan_from_durable(
    operation: &self::model::CleanupOperation,
    stored: CleanupPlanPayload,
) -> Result<ConnectorCleanupPlan, String> {
    let owner = ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse(&operation.owner.instance_id)
            .map_err(|error| error.to_string())?,
        incarnation: ConnectorInstanceIncarnation::from_bytes(
            *operation.owner.incarnation_id.as_bytes(),
        ),
    };
    ConnectorCleanupPlan::try_restore(
        owner,
        ConnectorCleanupOperationId::from_bytes(*operation.operation_id.as_bytes()),
        operation.request_digest,
        stored.base_state_digest,
        stored.manifest_digest,
        ConnectorCleanupPlanSummary::try_new(
            u64::from(stored.candidate_count),
            stored.total_bytes,
            u32::from(stored.manifest_parts),
            u32::from(stored.batch_count),
        )
        .map_err(|error| error.to_string())?,
        bytes::Bytes::from(stored.artifact_handle),
        stored.plan_digest,
    )
    .map_err(|error| error.to_string())
}

fn distributed_data_rewrite_intent(
    options: &std::collections::BTreeMap<String, String>,
    branch: Option<&str>,
    where_clause: Option<&str>,
) -> Result<DistributedRewriteIntent, String> {
    if where_clause.is_some() {
        return Err("rewrite_data_files where is not supported in NovaRocks yet".to_string());
    }
    if branch.is_some() {
        return Err("rewrite_data_files branch is not supported in NovaRocks yet".to_string());
    }
    let mut rewrite_all = false;
    for (key, value) in options {
        match key.as_str() {
            "rewrite-all" if value.eq_ignore_ascii_case("true") => rewrite_all = true,
            "rewrite-all" => {
                return Err("rewrite_data_files option `rewrite-all` must be `true`".to_string());
            }
            "min-input-files" | "target-file-size-bytes" => {
                return Err(format!("unsupported rewrite_data_files option `{key}`"));
            }
            other => return Err(format!("unsupported rewrite_data_files option `{other}`")),
        }
    }
    Ok(DistributedRewriteIntent::DataFiles { rewrite_all })
}

fn distributed_position_rewrite_intent(
    options: &std::collections::BTreeMap<String, String>,
    where_clause: Option<&str>,
) -> Result<DistributedRewriteIntent, String> {
    if where_clause.is_some() {
        return Err(
            "rewrite_position_delete_files where is not supported in NovaRocks".to_string(),
        );
    }
    let mut rewrite_all = false;
    let mut min_input_files = Some(2_u32);
    for (key, value) in options {
        match key.as_str() {
            "rewrite-all" => {
                rewrite_all = if value.eq_ignore_ascii_case("true") {
                    true
                } else if value.eq_ignore_ascii_case("false") {
                    false
                } else {
                    return Err(
                        "rewrite_position_delete_files option `rewrite-all` must be `true` or `false`"
                            .to_string(),
                    );
                };
            }
            "min-input-files" => {
                let parsed = value.parse::<u32>().map_err(|_| {
                    "rewrite_position_delete_files option `min-input-files` must be positive".to_string()
                })?;
                if parsed == 0 {
                    return Err("rewrite_position_delete_files option `min-input-files` must be positive".to_string());
                }
                min_input_files = Some(parsed);
            }
            "target-file-size-bytes" => return Err(
                "rewrite_position_delete_files option `target-file-size-bytes` is not implemented in NovaRocks yet".to_string(),
            ),
            other => return Err(format!("unsupported rewrite_position_delete_files option `{other}`")),
        }
    }
    Ok(DistributedRewriteIntent::PositionDeletes {
        rewrite_all,
        min_input_files,
    })
}

fn distributed_rewrite_request_payload(intent: DistributedRewriteIntent) -> Vec<u8> {
    match intent {
        DistributedRewriteIntent::DataFiles { rewrite_all } => {
            format!("distributed-rewrite-request-v1:data:{rewrite_all}").into_bytes()
        }
        DistributedRewriteIntent::PositionDeletes {
            rewrite_all,
            min_input_files,
        } => format!(
            "distributed-rewrite-request-v1:position:{rewrite_all}:{}",
            min_input_files.unwrap_or_default()
        )
        .into_bytes(),
    }
}

fn distributed_rewrite_checkpoint(
    checkpoint: novarocks_spi::connector::ConnectorDistributedRewriteAttemptCheckpoint,
) -> DistributedRewriteAttemptCheckpoint {
    DistributedRewriteAttemptCheckpoint {
        cohort_id: checkpoint.cohort_id.to_bytes(),
        execution_id: checkpoint.execution_id,
        disposition: match checkpoint.disposition {
            novarocks_spi::connector::ConnectorDistributedRewriteAttemptDisposition::Accepted => {
                DistributedRewriteAttemptDisposition::Accepted
            }
            novarocks_spi::connector::ConnectorDistributedRewriteAttemptDisposition::Superseded => {
                DistributedRewriteAttemptDisposition::Superseded
            }
        },
        attempt_digest: checkpoint.attempt_digest,
        artifact_digest: checkpoint.artifact_digest,
        artifact_handle: checkpoint.artifact_handle.to_vec(),
        checkpoint_digest: checkpoint.checkpoint_digest,
    }
}

fn rewrite_noop_outcome(
    kind: DistributedRewriteOperationKind,
) -> Result<MaintenanceActionOutcome, String> {
    rewrite_outcome_from_receipt(
        kind,
        novarocks_spi::connector::ConnectorDistributedRewriteReceiptSummary::default(),
        novarocks_spi::connector::ConnectorDistributedRewritePlanSummary::default(),
    )
}

fn rewrite_outcome_from_receipt(
    kind: DistributedRewriteOperationKind,
    receipt: novarocks_spi::connector::ConnectorDistributedRewriteReceiptSummary,
    plan: novarocks_spi::connector::ConnectorDistributedRewritePlanSummary,
) -> Result<MaintenanceActionOutcome, String> {
    let count = |value: u64, name: &str| {
        i32::try_from(value).map_err(|_| format!("distributed rewrite metric `{name}` overflow"))
    };
    match kind {
        DistributedRewriteOperationKind::RewriteDataFiles => {
            Ok(MaintenanceActionOutcome::RewriteDataFiles {
                target_snapshot_id: receipt.target_version,
                rewritten_data_files_count: count(receipt.input_data_files, "input_data_files")?,
                added_data_files_count: count(receipt.output_data_files, "output_data_files")?,
                rewritten_bytes_count: i64::try_from(plan.input_bytes)
                    .map_err(|_| "distributed rewrite input bytes overflow".to_string())?,
                failed_data_files_count: 0,
                removed_delete_files_count: count(
                    receipt.input_delete_files,
                    "input_delete_files",
                )?,
                output_record_count: i64::try_from(receipt.output_rows)
                    .map_err(|_| "distributed rewrite output rows overflow".to_string())?,
            })
        }
        DistributedRewriteOperationKind::RewritePositionDeleteFiles => {
            Ok(MaintenanceActionOutcome::RewritePositionDeleteFiles {
                rewritten_delete_files_count: count(
                    receipt.input_delete_files,
                    "input_delete_files",
                )?,
                added_delete_files_count: count(
                    receipt.output_delete_files,
                    "output_delete_files",
                )?,
                rewritten_bytes_count: i64::try_from(plan.input_bytes)
                    .map_err(|_| "distributed rewrite input bytes overflow".to_string())?,
                added_bytes_count: 0,
            })
        }
    }
}
