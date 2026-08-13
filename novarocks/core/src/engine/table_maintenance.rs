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

//! Table-maintenance application ports shared with `novarocks-frontend`.
//!
//! This dependency-inversion boundary exposes only the typed engine
//! capabilities and application results needed by the frontend owner. It does
//! not expose standalone engine state or connector handles.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

use crate::connector::cleanup_maintenance::{CleanupBatchExecution, CleanupMaintenanceSession};
use crate::connector::distributed_rewrite_application::{
    DistributedRewriteIntent, DistributedRewriteMaintenanceSession,
};
use crate::connector::metadata_maintenance::{
    CompletedMetadataMaintenance, MetadataMaintenanceIntent, MetadataMaintenanceSession,
};
use crate::query_execution::ConnectorWriteCompletion;
use crate::query_execution::cancellation::QueryCancellationView;
use crate::runtime::query_result::QueryResult;
use crate::sql::parser::dialect::StarRocksDialect;
use novarocks_spi::connector::{
    BatchReceipt, CandidatePage, ConnectorCleanupOperationId, ConnectorCleanupPlan,
    ConnectorDistributedRewriteAttemptCheckpoint, ConnectorDistributedRewriteReceipt,
    ConnectorMetadataMaintenancePlan, ConnectorMutationOperationId, ConnectorWriteAbortOutcome,
    ConnectorWriteCohortId, ConnectorWriteInputShape, ConnectorWriteReceipt,
    ExternalMutationEvidence, ExternalMutationOutcome, PreparedBatch,
};

pub const TABLE_MAINTENANCE_SERVICE_UNAVAILABLE: &str = "table maintenance service is not injected";

#[derive(Clone, Copy, Debug)]
pub struct MaintenanceRequestContext<'a> {
    pub current_catalog: Option<&'a str>,
    pub current_database: &'a str,
}

/// Immutable worker-owned cancellation context for automatic maintenance.
/// It crosses the Core-to-Frontend port without exposing a session, catalog,
/// or provider object. Implementations must check it before every durable
/// dispatch and preserve durable recovery state if cancellation races a
/// dispatched external mutation.
#[derive(Clone)]
pub struct AutomaticMaintenanceContext {
    cancellation: QueryCancellationView,
    deadline: Option<Instant>,
}

impl AutomaticMaintenanceContext {
    pub fn new(cancellation: QueryCancellationView) -> Self {
        Self {
            cancellation,
            deadline: None,
        }
    }

    pub fn with_deadline(cancellation: QueryCancellationView, deadline: Instant) -> Self {
        Self {
            cancellation,
            deadline: Some(deadline),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub fn ensure_active(&self) -> Result<(), String> {
        if self.is_cancelled() {
            return Err("automatic maintenance cancelled before durable dispatch".to_string());
        }
        self.deadline
            .is_none_or(|deadline| Instant::now() < deadline)
            .then_some(())
            .ok_or_else(|| {
                "automatic maintenance deadline elapsed before durable dispatch".to_string()
            })
    }
}

/// Write capability for one frontend-owned maintenance execution attempt.
///
/// This cancellation is deliberately provider-neutral. The frontend keeps the
/// source and passes only [`MaintenanceAttemptContext`] through the engine
/// port, so a lost coordination lease can stop subsequent connector work
/// without exposing a lease, repository, or provider object to Core.
#[derive(Clone, Debug, Default)]
pub struct MaintenanceAttemptCancellationSource {
    cancelled: Arc<AtomicBool>,
}

impl MaintenanceAttemptCancellationSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn context(&self) -> MaintenanceAttemptContext {
        MaintenanceAttemptContext {
            cancelled: Arc::clone(&self.cancelled),
        }
    }

    /// Returns true only for the first cancellation request.
    pub fn cancel(&self) -> bool {
        !self.cancelled.swap(true, Ordering::AcqRel)
    }
}

/// What the engine could get back when asking about a dead generation's work.
///
/// `Unsupported` is a first-class answer, not an error to be papered over: a
/// provider without a historical inspector leaves the operation unresolved,
/// and the caller must not fall back to the ordinary exact-generation
/// reconcile — the exact generation is the thing that no longer exists.
#[derive(Clone, Debug)]
pub enum HistoricalMaintenanceInspection {
    Unsupported(String),
    Observed(Box<novarocks_spi::connector::ConnectorHistoricalMaintenanceObservation>),
}

/// Read-only cancellation view shared by all provider calls in one durable
/// maintenance attempt.
#[derive(Clone, Debug, Default)]
pub struct MaintenanceAttemptContext {
    cancelled: Arc<AtomicBool>,
}

impl MaintenanceAttemptContext {
    pub fn uncancelled() -> Self {
        Self::default()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn connector_request_context(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorRequestContext, String> {
        crate::connector::connector_request_context(None, Arc::clone(&self.cancelled))
    }

    /// Preserve the statement's admitted connector deadline and cancellation
    /// while also stopping subsequent provider work when this maintenance
    /// attempt loses its frontend fence.
    ///
    /// A durable maintenance service owns the fence cancellation; it must not
    /// replace the request cancellation captured by the SQL admission path.
    pub(crate) fn connector_request_context_with_attempt(
        &self,
        request: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::ConnectorRequestContext, String> {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            request.deadline(),
            Arc::new(MaintenanceAttemptConnectorCancellation {
                request: Arc::clone(request.cancellation()),
                attempt: Arc::clone(&self.cancelled),
            }),
            request.max_handle_payload_bytes(),
            request.max_total_payload_bytes(),
        )
        .map_err(|error| error.to_string())
    }
}

struct MaintenanceAttemptConnectorCancellation {
    request: Arc<dyn novarocks_spi::connector::ConnectorCancellation>,
    attempt: Arc<AtomicBool>,
}

impl novarocks_spi::connector::ConnectorCancellation for MaintenanceAttemptConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.request.is_cancelled() || self.attempt.load(Ordering::Acquire)
    }
}

#[derive(Clone, Debug)]
pub enum MaintenanceStatementResult {
    Ok,
    Query(QueryResult),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MaintenanceTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceActionRequest {
    RewriteDataFiles {
        target: MaintenanceTarget,
        base_snapshot_id: i64,
        job_id: Option<i64>,
        options: BTreeMap<String, String>,
        branch: Option<String>,
        where_clause: Option<String>,
    },
    RewriteManifests {
        target: MaintenanceTarget,
        use_caching: Option<bool>,
        spec_id: Option<i32>,
    },
    ExpireSnapshots {
        target: MaintenanceTarget,
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    },
    RemoveOrphanFiles {
        target: MaintenanceTarget,
        older_than_ms: i64,
    },
    RewritePositionDeleteFiles {
        target: MaintenanceTarget,
        options: BTreeMap<String, String>,
        where_clause: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceActionOutcome {
    RewriteDataFiles {
        target_snapshot_id: Option<i64>,
        rewritten_data_files_count: i32,
        added_data_files_count: i32,
        rewritten_bytes_count: i64,
        failed_data_files_count: i32,
        removed_delete_files_count: i32,
        output_record_count: i64,
    },
    RewriteManifests {
        rewritten_manifests_count: i32,
        added_manifests_count: i32,
    },
    ExpireSnapshots {
        deleted_data_files_count: Option<i64>,
        deleted_position_delete_files_count: Option<i64>,
        deleted_equality_delete_files_count: Option<i64>,
        deleted_manifest_files_count: Option<i64>,
        deleted_manifest_lists_count: Option<i64>,
        deleted_statistics_files_count: Option<i64>,
    },
    RemoveOrphanFiles {
        orphan_file_locations: Vec<String>,
    },
    RewritePositionDeleteFiles {
        rewritten_delete_files_count: i32,
        added_delete_files_count: i32,
        rewritten_bytes_count: i64,
        added_bytes_count: i64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum OptimizeJobState {
    Pending,
    Running,
    Finished,
    Failed,
}

impl OptimizeJobState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Finished => "FINISHED",
            Self::Failed => "FAILED",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimizeSubmission {
    Submitted { job_id: i64 },
    AlreadyActive,
}

// Design: ADR-0009 (docs/adr/ADR-0009-frontend-table-maintenance-owner.md)
pub trait TableMaintenanceEngine: Send + Sync {
    fn resolve_target(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, String>;

    fn reject_user_action_on_mv(&self, target: &MaintenanceTarget) -> Result<(), String>;

    fn current_snapshot_id(&self, target: &MaintenanceTarget) -> Result<i64, String>;

    fn execute_action(
        &self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String>;

    fn plan_metadata_maintenance(
        &self,
        _target: &MaintenanceTarget,
        _operation_id: ConnectorMutationOperationId,
        _intent: MetadataMaintenanceIntent,
    ) -> Result<MetadataMaintenanceSession, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    /// Context-aware planning entrypoint used by a fenced frontend attempt.
    /// Existing engine implementations remain source-compatible: until they
    /// override this method, the legacy planning method is used.
    fn plan_metadata_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorMutationOperationId,
        intent: MetadataMaintenanceIntent,
        _attempt: &MaintenanceAttemptContext,
    ) -> Result<MetadataMaintenanceSession, String> {
        self.plan_metadata_maintenance(target, operation_id, intent)
    }

    fn execute_planned_metadata_maintenance(
        &self,
        _session: MetadataMaintenanceSession,
    ) -> Result<CompletedMetadataMaintenance, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn reconcile_metadata_maintenance(
        &self,
        _target: &MaintenanceTarget,
        _plan: ConnectorMetadataMaintenancePlan,
    ) -> Result<CompletedMetadataMaintenance, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn reconcile_metadata_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorMetadataMaintenancePlan,
        _attempt: &MaintenanceAttemptContext,
    ) -> Result<CompletedMetadataMaintenance, String> {
        self.reconcile_metadata_maintenance(target, plan)
    }

    /// Plan a provider-neutral FE-only orphan cleanup operation. The returned
    /// session owns its exact connector generation for the durable frontend
    /// operation; no BE or generic action route participates.
    fn plan_cleanup_maintenance(
        &self,
        _target: &MaintenanceTarget,
        _operation_id: ConnectorCleanupOperationId,
        _older_than_ms: i64,
    ) -> Result<CleanupMaintenanceSession, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn plan_cleanup_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        _attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.plan_cleanup_maintenance(target, operation_id, older_than_ms)
    }

    fn recover_cleanup_for_reconcile(
        &self,
        _target: &MaintenanceTarget,
        _plan: ConnectorCleanupPlan,
        _prepared: PreparedBatch,
    ) -> Result<CleanupMaintenanceSession, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn recover_cleanup_for_reconcile_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorCleanupPlan,
        prepared: PreparedBatch,
        _attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.recover_cleanup_for_reconcile(target, plan, prepared)
    }

    fn prepare_cleanup_batch(
        &self,
        _session: &CleanupMaintenanceSession,
        _batch_ordinal: u32,
    ) -> Result<PreparedBatch, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn execute_cleanup_batch(
        &self,
        _session: &CleanupMaintenanceSession,
        _prepared: PreparedBatch,
    ) -> Result<CleanupBatchExecution, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn reconcile_cleanup_batch(
        &self,
        _session: &CleanupMaintenanceSession,
        _prepared: PreparedBatch,
    ) -> Result<BatchReceipt, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn read_cleanup_candidate_page(
        &self,
        _session: &CleanupMaintenanceSession,
        _offset: u64,
        _limit: u32,
    ) -> Result<CandidatePage, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn finalize_cleanup_terminal(
        &self,
        _session: &CleanupMaintenanceSession,
    ) -> Result<(), String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn plan_distributed_rewrite(
        &self,
        _target: &MaintenanceTarget,
        _operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        _intent: DistributedRewriteIntent,
    ) -> Result<DistributedRewriteMaintenanceSession, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn plan_distributed_rewrite_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        intent: DistributedRewriteIntent,
        _attempt: &MaintenanceAttemptContext,
    ) -> Result<DistributedRewriteMaintenanceSession, String> {
        self.plan_distributed_rewrite(target, operation_id, intent)
    }

    // Design: ADR-0067 (docs/adr/ADR-0067-historical-maintenance-recovery-is-a-separate-capability.md)
    /// Ask the *live* connector generation what it can prove about work a dead
    /// generation left behind.
    ///
    /// The descriptor names the dead binding as evidence only. Engines that do
    /// not implement this report it as unsupported so the caller keeps the
    /// operation unresolved.
    fn inspect_historical_maintenance(
        &self,
        _target: &MaintenanceTarget,
        _descriptor: novarocks_spi::connector::ConnectorHistoricalMaintenanceDescriptor,
        _attempt: &MaintenanceAttemptContext,
    ) -> Result<HistoricalMaintenanceInspection, String> {
        Ok(HistoricalMaintenanceInspection::Unsupported(
            TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string(),
        ))
    }

    fn stage_distributed_rewrite_cohort(
        &self,
        _session: &DistributedRewriteMaintenanceSession,
        _cohort_id: ConnectorWriteCohortId,
    ) -> Result<ConnectorWriteCompletion, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn checkpoint_distributed_rewrite_attempt(
        &self,
        _session: &DistributedRewriteMaintenanceSession,
        _completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn commit_distributed_rewrite(
        &self,
        _session: &DistributedRewriteMaintenanceSession,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn abort_distributed_rewrite(
        &self,
        _session: &DistributedRewriteMaintenanceSession,
    ) -> Result<ConnectorWriteAbortOutcome, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn reconcile_distributed_rewrite(
        &self,
        _session: &DistributedRewriteMaintenanceSession,
        _evidence: ExternalMutationEvidence,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn finalize_distributed_rewrite(
        &self,
        _session: &DistributedRewriteMaintenanceSession,
        _receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }
}

pub trait TableMaintenanceService: Send + Sync {
    fn start(&self, engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String>;

    fn try_handle_statement(
        &self,
        engine: &dyn TableMaintenanceEngine,
        sql: &str,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<Option<MaintenanceStatementResult>, String>;

    /// Execute the read-only maintenance subset without manufacturing a
    /// `TableMaintenanceEngine`.  Durable command writes keep their explicit
    /// engine and request execution context.
    fn try_handle_readonly_statement(
        &self,
        _sql: &str,
        _context: MaintenanceRequestContext<'_>,
    ) -> Result<Option<MaintenanceStatementResult>, String> {
        Ok(None)
    }

    fn execute_automatic_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String>;

    fn execute_automatic_action_with_context(
        &self,
        engine: &dyn TableMaintenanceEngine,
        request: MaintenanceActionRequest,
        context: &AutomaticMaintenanceContext,
    ) -> Result<MaintenanceActionOutcome, String> {
        context.ensure_active()?;
        self.execute_automatic_action(engine, request)
    }

    fn submit_automatic_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String>;

    /// Execute an automatic OPTIMIZE as one complete durable job lifecycle.
    /// Unlike submission, success means the job was claimed, executed and
    /// terminally persisted before the caller releases its MV activity gate.
    fn execute_automatic_optimize_durably(
        &self,
        _engine: &dyn TableMaintenanceEngine,
        _target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_owned())
    }

    fn execute_automatic_optimize_durably_with_context(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
        context: &AutomaticMaintenanceContext,
    ) -> Result<OptimizeSubmission, String> {
        context.ensure_active()?;
        self.execute_automatic_optimize_durably(engine, target)
    }

    fn shutdown(&self) -> Result<(), String>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyTableMaintenanceService;

impl TableMaintenanceService for EmptyTableMaintenanceService {
    fn start(&self, _engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
        Ok(())
    }

    fn try_handle_statement(
        &self,
        _engine: &dyn TableMaintenanceEngine,
        sql: &str,
        _context: MaintenanceRequestContext<'_>,
    ) -> Result<Option<MaintenanceStatementResult>, String> {
        if looks_like_maintenance_statement(sql) {
            return Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_owned());
        }
        Ok(None)
    }

    fn execute_automatic_action(
        &self,
        _engine: &dyn TableMaintenanceEngine,
        _request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_owned())
    }

    fn submit_automatic_optimize(
        &self,
        _engine: &dyn TableMaintenanceEngine,
        _target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_owned())
    }

    fn execute_automatic_optimize_durably(
        &self,
        _engine: &dyn TableMaintenanceEngine,
        _target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_owned())
    }

    fn shutdown(&self) -> Result<(), String> {
        Ok(())
    }
}

impl crate::engine::StandaloneState {
    fn shared_for_table_maintenance(&self) -> Result<Arc<Self>, String> {
        self.self_weak.upgrade().ok_or_else(|| {
            "standalone state is not attached to a shared engine instance".to_string()
        })
    }
}

impl TableMaintenanceEngine for crate::engine::StandaloneState {
    fn resolve_target(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, String> {
        let state = self.shared_for_table_maintenance()?;
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &state,
            &crate::sql::parser::ast::ObjectName {
                parts: name_parts.to_vec(),
            },
            context.current_catalog,
            context.current_database,
        )?;
        if target.backend_name != "iceberg" {
            return Err(format!(
                "table maintenance only supports iceberg backends, got `{}`",
                target.backend_name
            ));
        }
        Ok(MaintenanceTarget {
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
        })
    }

    fn reject_user_action_on_mv(&self, target: &MaintenanceTarget) -> Result<(), String> {
        let state = self.shared_for_table_maintenance()?;
        crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
            &state,
            &crate::engine::backend_resolver::TargetBackend {
                backend_name: "iceberg",
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            },
            crate::engine::mv::iceberg_guard::IcebergMvUserMutation::AlterTable,
        )
    }

    fn current_snapshot_id(&self, target: &MaintenanceTarget) -> Result<i64, String> {
        let state = self.shared_for_table_maintenance()?;
        crate::engine::iceberg_maintenance::current_snapshot_id_with_ports(
            state.connector_control.as_ref(),
            target,
            crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))?,
        )
    }

    fn execute_action(
        &self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        if matches!(request, MaintenanceActionRequest::RemoveOrphanFiles { .. }) {
            return Err(
                "remove orphan files must be dispatched by the frontend durable cleanup owner"
                    .to_string(),
            );
        }
        let state = self.shared_for_table_maintenance()?;
        crate::engine::iceberg_maintenance::execute_action_with_ports(
            state.connector_control.as_ref(),
            state.as_ref(),
            request,
            crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))?,
        )
    }

    fn plan_metadata_maintenance(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorMutationOperationId,
        intent: MetadataMaintenanceIntent,
    ) -> Result<MetadataMaintenanceSession, String> {
        self.plan_metadata_maintenance_with_attempt_context(
            target,
            operation_id,
            intent,
            &MaintenanceAttemptContext::uncancelled(),
        )
    }

    fn plan_metadata_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorMutationOperationId,
        intent: MetadataMaintenanceIntent,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<MetadataMaintenanceSession, String> {
        let state = self.shared_for_table_maintenance()?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        crate::connector::metadata_maintenance::plan_metadata_maintenance_session(
            state.connector_control.as_ref(),
            &instance_id,
            operation_id,
            novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: target.namespace.clone().into(),
                table: target.table.clone().into(),
            },
            intent,
            attempt.connector_request_context()?,
        )
    }

    fn execute_planned_metadata_maintenance(
        &self,
        session: MetadataMaintenanceSession,
    ) -> Result<CompletedMetadataMaintenance, String> {
        crate::connector::metadata_maintenance::execute_planned_metadata_maintenance(session, self)
    }

    fn reconcile_metadata_maintenance(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorMetadataMaintenancePlan,
    ) -> Result<CompletedMetadataMaintenance, String> {
        self.reconcile_metadata_maintenance_with_attempt_context(
            target,
            plan,
            &MaintenanceAttemptContext::uncancelled(),
        )
    }

    fn reconcile_metadata_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorMetadataMaintenancePlan,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CompletedMetadataMaintenance, String> {
        let state = self.shared_for_table_maintenance()?;
        crate::connector::metadata_maintenance::reconcile_metadata_maintenance_session(
            state.connector_control.as_ref(),
            self,
            novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
                    .map_err(|error| error.to_string())?,
                namespace: target.namespace.clone().into(),
                table: target.table.clone().into(),
            },
            plan,
            attempt.connector_request_context()?,
        )
    }

    fn plan_cleanup_maintenance(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.plan_cleanup_maintenance_with_attempt_context(
            target,
            operation_id,
            older_than_ms,
            &MaintenanceAttemptContext::uncancelled(),
        )
    }

    fn plan_cleanup_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        let state = self.shared_for_table_maintenance()?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        CleanupMaintenanceSession::plan(
            state.connector_control.as_ref(),
            &instance_id,
            operation_id,
            novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: target.namespace.clone().into(),
                table: target.table.clone().into(),
            },
            older_than_ms,
            attempt.connector_request_context()?,
        )
        .map_err(|error| format!("plan orphan cleanup operation: {error}"))
    }

    fn recover_cleanup_for_reconcile(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorCleanupPlan,
        prepared: PreparedBatch,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.recover_cleanup_for_reconcile_with_attempt_context(
            target,
            plan,
            prepared,
            &MaintenanceAttemptContext::uncancelled(),
        )
    }

    fn inspect_historical_maintenance(
        &self,
        target: &MaintenanceTarget,
        descriptor: novarocks_spi::connector::ConnectorHistoricalMaintenanceDescriptor,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<HistoricalMaintenanceInspection, String> {
        use novarocks_spi::connector::ConnectorHistoricalMaintenanceResolver;

        let state = self.shared_for_table_maintenance()?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        // Acquiring the live inspector is where "this provider cannot do
        // historical recovery" is decided. Anything else -- a lost lease, a
        // corrupt descriptor -- is a real failure the caller must see.
        let lease = match state
            .connector_control
            .acquire_current_historical_maintenance(&instance_id)
        {
            Ok(lease) => lease,
            Err(error)
                if error.kind() == novarocks_spi::connector::ConnectorErrorKind::Unsupported =>
            {
                return Ok(HistoricalMaintenanceInspection::Unsupported(
                    error.to_string(),
                ));
            }
            Err(error) => {
                return Err(format!(
                    "acquire historical maintenance recovery capability: {error}"
                ));
            }
        };
        let observation = lease
            .inspect(descriptor, attempt.connector_request_context()?)
            .map_err(|error| format!("inspect historical maintenance operation: {error}"))?;
        Ok(HistoricalMaintenanceInspection::Observed(Box::new(
            observation,
        )))
    }

    fn recover_cleanup_for_reconcile_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorCleanupPlan,
        prepared: PreparedBatch,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        let state = self.shared_for_table_maintenance()?;
        CleanupMaintenanceSession::recover_for_reconcile(
            state.connector_control.as_ref(),
            novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
                    .map_err(|error| error.to_string())?,
                namespace: target.namespace.clone().into(),
                table: target.table.clone().into(),
            },
            plan,
            prepared,
            attempt.connector_request_context()?,
        )
        .map_err(|error| format!("recover orphan cleanup operation: {error}"))
    }

    fn prepare_cleanup_batch(
        &self,
        session: &CleanupMaintenanceSession,
        batch_ordinal: u32,
    ) -> Result<PreparedBatch, String> {
        session
            .prepare_batch(batch_ordinal)
            .map_err(|error| format!("prepare orphan cleanup batch: {error}"))
    }

    fn execute_cleanup_batch(
        &self,
        session: &CleanupMaintenanceSession,
        prepared: PreparedBatch,
    ) -> Result<CleanupBatchExecution, String> {
        session
            .execute_batch(prepared)
            .map_err(|error| format!("execute orphan cleanup batch: {error}"))
    }

    fn reconcile_cleanup_batch(
        &self,
        session: &CleanupMaintenanceSession,
        prepared: PreparedBatch,
    ) -> Result<BatchReceipt, String> {
        session
            .reconcile_batch(prepared)
            .map_err(|error| format!("reconcile orphan cleanup batch: {error}"))
    }

    fn read_cleanup_candidate_page(
        &self,
        session: &CleanupMaintenanceSession,
        offset: u64,
        limit: u32,
    ) -> Result<CandidatePage, String> {
        session
            .read_candidate_page(offset, limit)
            .map_err(|error| format!("read orphan cleanup candidate page: {error}"))
    }

    fn finalize_cleanup_terminal(&self, session: &CleanupMaintenanceSession) -> Result<(), String> {
        session
            .finalize_terminal()
            .map_err(|error| format!("finalize orphan cleanup artifacts: {error}"))
    }

    fn plan_distributed_rewrite(
        &self,
        target: &MaintenanceTarget,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        intent: DistributedRewriteIntent,
    ) -> Result<DistributedRewriteMaintenanceSession, String> {
        let state = self.shared_for_table_maintenance()?;
        let execution = crate::engine::capture_maintenance_execution(&state)?;
        let context = crate::connector::connector_request_context_for_execution(None, &execution)?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        crate::connector::distributed_rewrite_application::plan_distributed_rewrite_session(
            &state.query_execution,
            state.connector_control.as_ref(),
            &instance_id,
            novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: target.namespace.clone().into(),
                table: target.table.clone().into(),
            },
            operation_id,
            intent,
            execution,
            context,
        )
    }

    fn plan_distributed_rewrite_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        intent: DistributedRewriteIntent,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<DistributedRewriteMaintenanceSession, String> {
        let state = self.shared_for_table_maintenance()?;
        let execution = crate::engine::capture_maintenance_execution(&state)?;
        let context = attempt.connector_request_context()?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        crate::connector::distributed_rewrite_application::plan_distributed_rewrite_session(
            &state.query_execution,
            state.connector_control.as_ref(),
            &instance_id,
            novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: target.namespace.clone().into(),
                table: target.table.clone().into(),
            },
            operation_id,
            intent,
            execution,
            context,
        )
    }

    fn stage_distributed_rewrite_cohort(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<ConnectorWriteCompletion, String> {
        let state = self.shared_for_table_maintenance()?;
        stage_frozen_rewrite_cohort(
            &state,
            session.session(),
            cohort_id,
            session.execution(),
            session.context(),
        )
        .map(|(completion, _summary)| completion)
    }

    fn checkpoint_distributed_rewrite_attempt(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, String> {
        session
            .session()
            .checkpoint_accepted(completion)
            .map_err(|error| format!("checkpoint distributed rewrite attempt: {error}"))
    }

    fn commit_distributed_rewrite(
        &self,
        session: &DistributedRewriteMaintenanceSession,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        session
            .session()
            .commit(session.context().clone())
            .map_err(|error| format!("commit distributed rewrite operation: {error}"))
    }

    fn abort_distributed_rewrite(
        &self,
        session: &DistributedRewriteMaintenanceSession,
    ) -> Result<ConnectorWriteAbortOutcome, String> {
        session
            .session()
            .abort(session.context().clone())
            .map_err(|error| format!("abort distributed rewrite operation: {error}"))
    }

    fn reconcile_distributed_rewrite(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        evidence: ExternalMutationEvidence,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        session
            .session()
            .reconcile(evidence, session.context().clone())
            .map_err(|error| format!("reconcile distributed rewrite operation: {error}"))
    }

    fn finalize_distributed_rewrite(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, String> {
        session
            .session()
            .finalize_committed(receipt)
            .map_err(|error| format!("finalize distributed rewrite operation: {error}"))
    }
}

/// Stage one provider-frozen rewrite cohort through the ordinary connector
/// read and write contracts retained by its exact composite lease.
fn stage_frozen_rewrite_cohort(
    state: &Arc<crate::engine::StandaloneState>,
    session: &crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    (
        ConnectorWriteCompletion,
        crate::query_execution::outcome::ConnectorWriteStagingSummary,
    ),
    String,
> {
    let cohort = session
        .plan()
        .cohorts()
        .iter()
        .find(|candidate| candidate.cohort_id() == cohort_id)
        .ok_or_else(|| "distributed rewrite execution names an unknown cohort".to_string())?;
    let read = crate::query_execution::distributed_rewrite::plan_frozen_rewrite_connector_read(
        session.lease(),
        execution.topology(),
        cohort.source(),
        cohort.scan_schema(),
        (0..cohort.scan_schema().fields().len()).collect(),
        context.clone(),
    )
    .map_err(|error| format!("plan frozen rewrite source: {error}"))?;
    let table_bindings =
        Arc::new(crate::engine::query_planning::bindings::QueryTableBindingStore::try_new()?);
    let source_binding =
        crate::query_execution::distributed_rewrite::admit_frozen_rewrite_scan_binding(
            table_bindings.as_ref(),
            cohort.scan_schema(),
        )?;
    let resolver = crate::query_execution::distributed_rewrite::frozen_rewrite_read_resolver(
        source_binding,
        read,
    );
    let physical_plan =
        crate::query_execution::distributed_rewrite::frozen_rewrite_scan_physical_plan(
            cohort.scan_schema(),
            source_binding,
        );
    let target_binding =
        crate::engine::query_planning::write_sink::admit_prepared_connector_write_target(
            table_bindings.as_ref(),
            rewrite_target_identity(session, cohort_id),
            cohort.preparation().clone(),
            session.lease().planning_lease(),
        )?;
    let sink = crate::engine::query_planning::write_sink::sql_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        rewrite_sink_mode(cohort.preparation().input())?,
        crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
        None,
    )?;
    let registration = session
        .execution_registration(cohort_id)
        .map_err(|error| format!("register frozen rewrite cohort: {error}"))?;
    crate::engine::execute_frozen_rewrite_physical_plan_as_iceberg_staging(
        state,
        physical_plan,
        sink,
        Some(execution),
        context,
        table_bindings.as_ref(),
        &resolver,
        registration,
    )
}

fn rewrite_target_identity(
    session: &crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
) -> crate::sql::planner::table::SqlTableIdentity {
    crate::sql::planner::table::SqlTableIdentity {
        catalog: session
            .lease()
            .binding_key()
            .instance_id
            .as_str()
            .to_string(),
        namespace: "__connector_rewrite".to_string(),
        table: format!("cohort_{}", hex::encode(cohort_id.to_bytes())),
    }
}

fn rewrite_sink_mode(
    input: &ConnectorWriteInputShape,
) -> Result<crate::sql::planner::distributed::write::contract::SqlWriteSinkMode, String> {
    use crate::sql::planner::distributed::write::contract::SqlWriteSinkMode;

    match input {
        ConnectorWriteInputShape::Data { .. } => Ok(SqlWriteSinkMode::Data),
        ConnectorWriteInputShape::RowLineage { .. } => Ok(SqlWriteSinkMode::RowLineageData),
        ConnectorWriteInputShape::PositionDelete { .. } => Ok(SqlWriteSinkMode::PositionDeletes),
        ConnectorWriteInputShape::DeletionVector { .. } => Ok(SqlWriteSinkMode::DeletionVectors),
        ConnectorWriteInputShape::EqualityDelete { .. } => Ok(SqlWriteSinkMode::EqualityDeletes),
    }
}

fn looks_like_maintenance_statement(sql: &str) -> bool {
    if crate::sql::parser::procedure::looks_like_call_procedure(sql) {
        return true;
    }
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    if parser.parse_keyword(Keyword::SHOW) {
        return parser.parse_keyword(Keyword::ALTER)
            && parser.parse_keyword(Keyword::TABLE)
            && consume_word(&mut parser, "OPTIMIZE");
    }
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }
    consume_word(&mut parser, "OPTIMIZE")
        || (consume_word(&mut parser, "REWRITE") && consume_word(&mut parser, "MANIFESTS"))
        || (consume_word(&mut parser, "EXPIRE") && consume_word(&mut parser, "SNAPSHOTS"))
        || (consume_word(&mut parser, "REMOVE")
            && consume_word(&mut parser, "ORPHAN")
            && consume_word(&mut parser, "FILES"))
}

fn consume_word(parser: &mut Parser<'_>, expected: &str) -> bool {
    if parser
        .peek_token()
        .token
        .to_string()
        .eq_ignore_ascii_case(expected)
    {
        parser.next_token();
        true
    } else {
        false
    }
}

#[cfg(test)]
mod maintenance_attempt_context_tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use super::{MaintenanceAttemptCancellationSource, MaintenanceAttemptContext};

    #[test]
    fn source_context_and_connector_request_share_one_cancellation_flag() {
        let source = MaintenanceAttemptCancellationSource::new();
        let attempt = source.context();
        let cloned = attempt.clone();
        let connector = attempt
            .connector_request_context()
            .expect("connector request context");

        assert!(!attempt.is_cancelled());
        assert!(!connector.cancellation().is_cancelled());
        assert!(source.cancel());
        assert!(!source.cancel());
        assert!(attempt.is_cancelled());
        assert!(cloned.is_cancelled());
        assert!(connector.cancellation().is_cancelled());
    }

    #[test]
    fn uncancelled_attempts_do_not_share_state() {
        let first = MaintenanceAttemptContext::uncancelled();
        let second_source = MaintenanceAttemptCancellationSource::new();
        let second = second_source.context();

        assert!(second_source.cancel());
        assert!(!first.is_cancelled());
        assert!(second.is_cancelled());
    }

    #[test]
    fn attempt_context_preserves_request_cancellation_and_deadline() {
        let request_cancelled = Arc::new(AtomicBool::new(false));
        let request =
            crate::connector::connector_request_context(None, Arc::clone(&request_cancelled))
                .expect("request connector context");
        let source = MaintenanceAttemptCancellationSource::new();
        let combined = source
            .context()
            .connector_request_context_with_attempt(&request)
            .expect("combined connector context");

        assert_eq!(combined.deadline(), request.deadline());
        assert_eq!(
            combined.max_handle_payload_bytes(),
            request.max_handle_payload_bytes()
        );
        assert_eq!(
            combined.max_total_payload_bytes(),
            request.max_total_payload_bytes()
        );
        assert!(!combined.cancellation().is_cancelled());

        request_cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(combined.cancellation().is_cancelled());

        let request =
            crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))
                .expect("fresh request connector context");
        let source = MaintenanceAttemptCancellationSource::new();
        let combined = source
            .context()
            .connector_request_context_with_attempt(&request)
            .expect("combined connector context");
        assert!(source.cancel());
        assert!(combined.cancellation().is_cancelled());
    }
}
