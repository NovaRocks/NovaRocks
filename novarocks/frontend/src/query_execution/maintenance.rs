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

pub mod command;
pub(crate) mod iceberg;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

use crate::common::query_cancellation::QueryCancellationView;
use crate::query_execution::ConnectorWriteCompletion;
use crate::query_execution::distributed_rewrite::DistributedRewriteMaintenanceSession;
use crate::runtime::query_result::QueryResult;
use novarocks::connector::cleanup_maintenance::{CleanupBatchExecution, CleanupMaintenanceSession};
use novarocks::connector::distributed_rewrite_application::DistributedRewriteIntent;
use novarocks::connector::metadata_maintenance::{
    CompletedMetadataMaintenance, MetadataMaintenanceIntent, MetadataMaintenanceSession,
};
use novarocks::maintenance::MaintenanceTarget;
use novarocks_spi::connector::{
    BatchReceipt, CandidatePage, ConnectorCleanupOperationId, ConnectorCleanupPlan,
    ConnectorDistributedRewriteAttemptCheckpoint, ConnectorDistributedRewriteReceipt,
    ConnectorMetadataMaintenancePlan, ConnectorMutationOperationId, ConnectorWriteAbortOutcome,
    ConnectorWriteCohortId, ConnectorWriteInputShape, ConnectorWriteReceipt,
    ExternalMutationEvidence, ExternalMutationOutcome, PreparedBatch,
};
use novarocks_sql::syntax::StarRocksDialect;

pub const TABLE_MAINTENANCE_SERVICE_UNAVAILABLE: &str = "table maintenance service is not injected";

/// Core-prepared, Frontend-encoded staging dispatch for one exact rewrite
/// cohort. The Frontend can inspect the immutable encoder input, but only this
/// carrier retains the prepared fragments, admitted execution context, and
/// sealed connector-write registration required to submit the write.
pub struct PreparedDistributedRewriteCohort {
    encoding: crate::query_execution::compiler::NativeFragmentEncodingInput,
    query_execution: crate::query_execution::service::QueryExecutionService,
    execution: crate::common::admitted_query_context::QueryExecutionContext,
    connector_write: crate::query_execution::contract::ConnectorWriteExecutionRegistration,
}

impl PreparedDistributedRewriteCohort {
    fn new(
        encoding: crate::query_execution::compiler::NativeFragmentEncodingInput,
        query_execution: crate::query_execution::service::QueryExecutionService,
        execution: crate::common::admitted_query_context::QueryExecutionContext,
        connector_write: crate::query_execution::contract::ConnectorWriteExecutionRegistration,
    ) -> Self {
        Self {
            encoding,
            query_execution,
            execution,
            connector_write,
        }
    }

    /// The only read-only Frontend input for native fragment encoding.
    pub fn encoding(&self) -> &crate::query_execution::compiler::NativeFragmentEncodingInput {
        &self.encoding
    }

    /// Consume the exact Core preparation and its Frontend-produced native
    /// bundle to submit the sealed connector write.
    pub fn finish(
        self,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<ConnectorWriteCompletion, String> {
        if !self.encoding.matches_native_attachment(&native_bundle) {
            return Err(
                "native fragment bundle does not match the sealed maintenance encoding input"
                    .into(),
            );
        }
        let (_, prepared) = self.encoding.into_parts();
        let request =
            crate::query_execution::contract::build_distributed_query_request_with_execution(
                prepared,
                native_bundle,
                None,
                crate::query_execution::contract::DistributedQueryIntent::Write,
                &self.execution,
            )
            .map_err(|error| error.to_string())?;
        let request = crate::query_execution::contract::with_connector_write_operation(
            request,
            self.connector_write,
        )
        .map_err(|error| error.to_string())?;
        let (query_result, _write_commit, write_abort, connector_completion) = self
            .query_execution
            .execute(request)
            .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_write)
            .map(crate::query_execution::outcome::WriteExecutionOutcome::into_parts_with_connector)
            .map_err(|error| error.to_string())?;
        if !query_result.columns.is_empty() || !query_result.chunks.is_empty() {
            return Err("connector staging terminal returned a result payload".to_string());
        }
        if let Some(abort) = write_abort {
            return Err(format!(
                "connector staging terminal aborted: {}",
                abort.reason
            ));
        }
        let completion = connector_completion.ok_or_else(|| {
            "connector staging terminal has no accepted connector completion".to_string()
        })?;
        completion
            .staging_summary()
            .map_err(|error| error.to_string())?;
        Ok(completion)
    }
}

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
        novarocks::connector::connector_request_context(None, Arc::clone(&self.cancelled))
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

/// CLS-R2 boundary: the implementation moves to the frontend with the rest of
/// the maintenance application; this stable Core domain port remains available
/// to the Frontend MV background runtime until CLS-R3.
// Design: ADR-0083 (docs/adr/ADR-0083-frontend-owns-table-maintenance-execution-port.md)
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

    fn prepare_distributed_rewrite_cohort(
        &self,
        _session: &DistributedRewriteMaintenanceSession,
        _cohort_id: ConnectorWriteCohortId,
    ) -> Result<PreparedDistributedRewriteCohort, String> {
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

/// One foreground SQL maintenance command bound to the exact request admitted
/// by Frontend.  It deliberately contains only the maintenance kernel and
/// immutable request facts; it cannot recover an application facade or capture
/// a second topology, deadline, or cancellation scope.
#[derive(Clone)]
pub(crate) struct RequestScopedMaintenanceEngine {
    kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
    execution: crate::common::admitted_query_context::QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl RequestScopedMaintenanceEngine {
    pub fn new(
        kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
        execution: crate::common::admitted_query_context::QueryExecutionContext,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            kernel,
            execution,
            connector_context,
        }
    }

    fn connector_context_for_attempt(
        &self,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<novarocks_spi::connector::ConnectorRequestContext, String> {
        attempt.connector_request_context_with_attempt(&self.connector_context)
    }

    fn target_identity(
        target: &MaintenanceTarget,
    ) -> Result<novarocks_spi::connector::ConnectorTableIdentity, String> {
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        Ok(novarocks_spi::connector::ConnectorTableIdentity {
            instance_id,
            namespace: target.namespace.clone().into(),
            table: target.table.clone().into(),
        })
    }
}

/// One freshly-admitted automatic-maintenance attempt.
///
/// The frontend captures the live backend topology and cancellation scope
/// before constructing this value. Core retains it only while it plans one
/// distributed rewrite; the resulting provider session keeps its own exact
/// generation and execution identity for recovery.
#[derive(Clone)]
pub struct BackgroundMaintenanceAttempt {
    execution: crate::common::admitted_query_context::QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl BackgroundMaintenanceAttempt {
    pub fn new(
        execution: crate::common::admitted_query_context::QueryExecutionContext,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            execution,
            connector_context,
        }
    }
}

/// Frontend-composed admission boundary for long-lived automatic maintenance.
///
/// Implementations must capture a fresh live topology and cancellation scope
/// for each call. There is deliberately no Core default, process-global lookup
/// or application-facade fallback.
pub trait BackgroundMaintenanceAttemptFactory: Send + Sync {
    fn begin_automatic_maintenance_attempt(&self) -> Result<BackgroundMaintenanceAttempt, String>;
}

/// Long-lived automatic-maintenance engine.
///
/// Unlike the request-scoped SQL engine, this value is safe to retain in
/// frontend workers: it holds only the explicit maintenance kernel and the
/// frontend-owned attempt factory. It never captures a state aggregate or a
/// weak self reference.
#[derive(Clone)]
pub struct BackgroundMaintenanceEngine {
    kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
    attempt_factory: Arc<dyn BackgroundMaintenanceAttemptFactory>,
}

impl BackgroundMaintenanceEngine {
    pub fn new(
        kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
        attempt_factory: Arc<dyn BackgroundMaintenanceAttemptFactory>,
    ) -> Self {
        Self {
            kernel,
            attempt_factory,
        }
    }

    fn request_engine(&self) -> Result<RequestScopedMaintenanceEngine, String> {
        let attempt = self.attempt_factory.begin_automatic_maintenance_attempt()?;
        Ok(RequestScopedMaintenanceEngine::new(
            self.kernel.clone(),
            attempt.execution,
            attempt.connector_context,
        ))
    }
}

impl novarocks::connector::metadata_maintenance::MetadataMaintenanceCacheFinalizer
    for RequestScopedMaintenanceEngine
{
    fn invalidate_generic_table(
        &self,
        table: &novarocks_spi::connector::ConnectorTableIdentity,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        novarocks::connector::metadata_maintenance::MetadataMaintenanceCacheFinalizer::invalidate_generic_table(
            &self.kernel,
            table,
        )
    }
}

impl TableMaintenanceEngine for RequestScopedMaintenanceEngine {
    fn resolve_target(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, String> {
        let target = novarocks::catalog_application::resolver::resolve_existing_table_target(
            &self.kernel,
            &novarocks_sql::syntax::ObjectName {
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
        use novarocks_spi::connector::{
            ConnectorControlResolver, ConnectorInstanceId, ConnectorTableResolution,
        };

        let instance_id = ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| format!("parse Iceberg catalog identity for MV guard: {error}"))?;
        let exact_lease = ConnectorControlResolver::acquire_current(
            self.kernel.connector_control().as_ref(),
            &instance_id,
        )
        .map_err(|error| format!("acquire exact Iceberg generation for MV guard: {error}"))?;
        let identity = novarocks_spi::connector::ConnectorTableIdentity {
            instance_id,
            namespace: Arc::from(target.namespace.as_str()),
            table: Arc::from(target.table.as_str()),
        };
        let metadata = novarocks::connector::metadata_load_connector_table_with_planning_lease(
            &exact_lease,
            self.connector_context.clone(),
            &target.namespace,
            &target.table,
            ConnectorTableResolution::StrictBaseTable,
        )?;
        if metadata.identity != identity {
            return Err(
                "connector loaded a different table while checking the MV mutation guard"
                    .to_string(),
            );
        }
        if self
            .kernel
            .mv_storage_observation()
            .observe_lake_package(&exact_lease, &metadata, self.connector_context.clone())
            .map_err(|error| format!("observe Iceberg MV package for mutation guard: {error}"))?
            .is_some()
        {
            return Err(format!(
                "table {}.{}.{} is a materialized view; use ALTER MATERIALIZED VIEW or DROP MATERIALIZED VIEW",
                target.catalog, target.namespace, target.table,
            ));
        }
        Ok(())
    }

    fn current_snapshot_id(&self, target: &MaintenanceTarget) -> Result<i64, String> {
        self::iceberg::current_snapshot_id_with_ports(
            self.kernel.connector_control().as_ref(),
            target,
            self.connector_context.clone(),
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
        self::iceberg::execute_action_with_ports(
            self.kernel.connector_control().as_ref(),
            &self.kernel,
            request,
            self.connector_context.clone(),
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
        let identity = Self::target_identity(target)?;
        novarocks::connector::metadata_maintenance::plan_metadata_maintenance_session(
            self.kernel.connector_control().as_ref(),
            &identity.instance_id.clone(),
            operation_id,
            identity,
            intent,
            self.connector_context_for_attempt(attempt)?,
        )
    }

    fn execute_planned_metadata_maintenance(
        &self,
        session: MetadataMaintenanceSession,
    ) -> Result<CompletedMetadataMaintenance, String> {
        novarocks::connector::metadata_maintenance::execute_planned_metadata_maintenance(
            session, self,
        )
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
        novarocks::connector::metadata_maintenance::reconcile_metadata_maintenance_session(
            self.kernel.connector_control().as_ref(),
            self,
            Self::target_identity(target)?,
            plan,
            self.connector_context_for_attempt(attempt)?,
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
        let identity = Self::target_identity(target)?;
        CleanupMaintenanceSession::plan(
            self.kernel.connector_control().as_ref(),
            &identity.instance_id.clone(),
            operation_id,
            identity,
            older_than_ms,
            self.connector_context_for_attempt(attempt)?,
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

    fn recover_cleanup_for_reconcile_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorCleanupPlan,
        prepared: PreparedBatch,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        CleanupMaintenanceSession::recover_for_reconcile(
            self.kernel.connector_control().as_ref(),
            Self::target_identity(target)?,
            plan,
            prepared,
            self.connector_context_for_attempt(attempt)?,
        )
        .map_err(|error| format!("recover orphan cleanup operation: {error}"))
    }

    fn inspect_historical_maintenance(
        &self,
        target: &MaintenanceTarget,
        descriptor: novarocks_spi::connector::ConnectorHistoricalMaintenanceDescriptor,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<HistoricalMaintenanceInspection, String> {
        use novarocks_spi::connector::ConnectorHistoricalMaintenanceResolver;

        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        let lease = match self
            .kernel
            .connector_control()
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
            .inspect(descriptor, self.connector_context_for_attempt(attempt)?)
            .map_err(|error| format!("inspect historical maintenance operation: {error}"))?;
        Ok(HistoricalMaintenanceInspection::Observed(Box::new(
            observation,
        )))
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
        self.plan_distributed_rewrite_with_context(
            target,
            operation_id,
            intent,
            self.connector_context.clone(),
        )
    }

    fn plan_distributed_rewrite_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        intent: DistributedRewriteIntent,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<DistributedRewriteMaintenanceSession, String> {
        self.plan_distributed_rewrite_with_context(
            target,
            operation_id,
            intent,
            self.connector_context_for_attempt(attempt)?,
        )
    }

    fn prepare_distributed_rewrite_cohort(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<PreparedDistributedRewriteCohort, String> {
        prepare_frozen_rewrite_cohort_with_ports(
            self.kernel.connector_control().as_ref(),
            self.kernel.query_execution(),
            session.session(),
            cohort_id,
            session.execution(),
            session.context(),
        )
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

impl RequestScopedMaintenanceEngine {
    fn plan_distributed_rewrite_with_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        intent: DistributedRewriteIntent,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<DistributedRewriteMaintenanceSession, String> {
        let identity = Self::target_identity(target)?;
        novarocks::connector::distributed_rewrite_application::plan_distributed_rewrite_session(
            self.kernel.query_execution(),
            self.kernel.connector_control().as_ref(),
            &identity.instance_id.clone(),
            identity,
            operation_id,
            intent,
            self.execution.clone(),
            connector_context,
        )
    }
}

impl TableMaintenanceEngine for BackgroundMaintenanceEngine {
    fn resolve_target(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, String> {
        self.request_engine()?.resolve_target(name_parts, context)
    }

    fn reject_user_action_on_mv(&self, target: &MaintenanceTarget) -> Result<(), String> {
        self.request_engine()?.reject_user_action_on_mv(target)
    }

    fn current_snapshot_id(&self, target: &MaintenanceTarget) -> Result<i64, String> {
        self.request_engine()?.current_snapshot_id(target)
    }

    fn execute_action(
        &self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        self.request_engine()?.execute_action(request)
    }

    fn plan_metadata_maintenance(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorMutationOperationId,
        intent: MetadataMaintenanceIntent,
    ) -> Result<MetadataMaintenanceSession, String> {
        self.request_engine()?
            .plan_metadata_maintenance(target, operation_id, intent)
    }

    fn plan_metadata_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorMutationOperationId,
        intent: MetadataMaintenanceIntent,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<MetadataMaintenanceSession, String> {
        self.request_engine()?
            .plan_metadata_maintenance_with_attempt_context(target, operation_id, intent, attempt)
    }

    fn execute_planned_metadata_maintenance(
        &self,
        session: MetadataMaintenanceSession,
    ) -> Result<CompletedMetadataMaintenance, String> {
        self.request_engine()?
            .execute_planned_metadata_maintenance(session)
    }

    fn reconcile_metadata_maintenance(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorMetadataMaintenancePlan,
    ) -> Result<CompletedMetadataMaintenance, String> {
        self.request_engine()?
            .reconcile_metadata_maintenance(target, plan)
    }

    fn reconcile_metadata_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorMetadataMaintenancePlan,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CompletedMetadataMaintenance, String> {
        self.request_engine()?
            .reconcile_metadata_maintenance_with_attempt_context(target, plan, attempt)
    }

    fn plan_cleanup_maintenance(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.request_engine()?
            .plan_cleanup_maintenance(target, operation_id, older_than_ms)
    }

    fn plan_cleanup_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.request_engine()?
            .plan_cleanup_maintenance_with_attempt_context(
                target,
                operation_id,
                older_than_ms,
                attempt,
            )
    }

    fn recover_cleanup_for_reconcile(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorCleanupPlan,
        prepared: PreparedBatch,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.request_engine()?
            .recover_cleanup_for_reconcile(target, plan, prepared)
    }

    fn recover_cleanup_for_reconcile_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        plan: ConnectorCleanupPlan,
        prepared: PreparedBatch,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.request_engine()?
            .recover_cleanup_for_reconcile_with_attempt_context(target, plan, prepared, attempt)
    }

    fn inspect_historical_maintenance(
        &self,
        target: &MaintenanceTarget,
        descriptor: novarocks_spi::connector::ConnectorHistoricalMaintenanceDescriptor,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<HistoricalMaintenanceInspection, String> {
        self.request_engine()?
            .inspect_historical_maintenance(target, descriptor, attempt)
    }

    fn prepare_cleanup_batch(
        &self,
        session: &CleanupMaintenanceSession,
        batch_ordinal: u32,
    ) -> Result<PreparedBatch, String> {
        self.request_engine()?
            .prepare_cleanup_batch(session, batch_ordinal)
    }

    fn execute_cleanup_batch(
        &self,
        session: &CleanupMaintenanceSession,
        prepared: PreparedBatch,
    ) -> Result<CleanupBatchExecution, String> {
        self.request_engine()?
            .execute_cleanup_batch(session, prepared)
    }

    fn reconcile_cleanup_batch(
        &self,
        session: &CleanupMaintenanceSession,
        prepared: PreparedBatch,
    ) -> Result<BatchReceipt, String> {
        self.request_engine()?
            .reconcile_cleanup_batch(session, prepared)
    }

    fn read_cleanup_candidate_page(
        &self,
        session: &CleanupMaintenanceSession,
        offset: u64,
        limit: u32,
    ) -> Result<CandidatePage, String> {
        self.request_engine()?
            .read_cleanup_candidate_page(session, offset, limit)
    }

    fn finalize_cleanup_terminal(&self, session: &CleanupMaintenanceSession) -> Result<(), String> {
        self.request_engine()?.finalize_cleanup_terminal(session)
    }

    fn plan_distributed_rewrite(
        &self,
        target: &MaintenanceTarget,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        intent: DistributedRewriteIntent,
    ) -> Result<DistributedRewriteMaintenanceSession, String> {
        self.request_engine()?
            .plan_distributed_rewrite(target, operation_id, intent)
    }

    fn plan_distributed_rewrite_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        intent: DistributedRewriteIntent,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<DistributedRewriteMaintenanceSession, String> {
        self.request_engine()?
            .plan_distributed_rewrite_with_attempt_context(target, operation_id, intent, attempt)
    }

    fn prepare_distributed_rewrite_cohort(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<PreparedDistributedRewriteCohort, String> {
        self.request_engine()?
            .prepare_distributed_rewrite_cohort(session, cohort_id)
    }

    fn checkpoint_distributed_rewrite_attempt(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, String> {
        self.request_engine()?
            .checkpoint_distributed_rewrite_attempt(session, completion)
    }

    fn commit_distributed_rewrite(
        &self,
        session: &DistributedRewriteMaintenanceSession,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        self.request_engine()?.commit_distributed_rewrite(session)
    }

    fn abort_distributed_rewrite(
        &self,
        session: &DistributedRewriteMaintenanceSession,
    ) -> Result<ConnectorWriteAbortOutcome, String> {
        self.request_engine()?.abort_distributed_rewrite(session)
    }

    fn reconcile_distributed_rewrite(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        evidence: ExternalMutationEvidence,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        self.request_engine()?
            .reconcile_distributed_rewrite(session, evidence)
    }

    fn finalize_distributed_rewrite(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, String> {
        self.request_engine()?
            .finalize_distributed_rewrite(session, receipt)
    }
}

/// Prepare one provider-frozen rewrite cohort through the ordinary connector
/// read and write contracts retained by its exact composite lease. Native
/// assembly remains a Frontend-only step after this sealed Core preparation.
fn prepare_frozen_rewrite_cohort_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    query_execution: &crate::query_execution::service::QueryExecutionService,
    session: &crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
    execution: &crate::common::admitted_query_context::QueryExecutionContext,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedDistributedRewriteCohort, String> {
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
    let table_bindings = Arc::new(
        novarocks::catalog_application::query_bindings::QueryTableBindingStore::try_new()?,
    );
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
        crate::query_execution::planning::write_sink::admit_prepared_frozen_connector_write_target(
            table_bindings.as_ref(),
            rewrite_target_identity(session, cohort_id),
            cohort.preparation().clone(),
            session.lease().planning_lease(),
        )?;
    let sink =
        crate::query_execution::planning::write_sink::dml_write_plan_input_for_admitted_target(
            table_bindings.as_ref(),
            target_binding,
            rewrite_sink_mode(cohort.preparation().input())?,
            novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
        )?;
    let registration = session
        .execution_registration(cohort_id)
        .map_err(|error| format!("register frozen rewrite cohort: {error}"))?;
    novarocks::connector::validate_request_context(context)?;
    let mut optimizer_settings = execution.optimizer_settings().clone();
    if optimizer_settings.cbo_broadcast_backend_count.is_none() {
        optimizer_settings.effective_backend_count =
            Some(execution.topology().targets().len() as f64);
    }
    let distributed_plan =
        novarocks_sql::planning::dml::build_frozen_connector_write_distributed_plan(
            physical_plan,
            sink,
            &optimizer_settings,
        )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        connector_control,
        context,
        Some(table_bindings.as_ref()),
        Some(&resolver),
        crate::query_execution::dml::write::scan_preparation_options(
            &optimizer_settings,
            execution,
        )?,
    )?;
    Ok(PreparedDistributedRewriteCohort::new(
        crate::query_execution::compiler::NativeFragmentEncodingInput::new(
            distributed_plan,
            prepared,
        ),
        query_execution.clone(),
        execution.clone(),
        registration,
    ))
}

fn rewrite_target_identity(
    session: &crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
) -> novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity {
    novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::new(
        session
            .lease()
            .binding_key()
            .instance_id
            .as_str()
            .to_string(),
        "__connector_rewrite",
        format!("cohort_{}", hex::encode(cohort_id.to_bytes())),
    )
}

fn rewrite_sink_mode(
    input: &ConnectorWriteInputShape,
) -> Result<novarocks_sql::planning::dml::DmlWriteSinkMode, String> {
    match input {
        ConnectorWriteInputShape::Data { .. } => {
            Ok(novarocks_sql::planning::dml::DmlWriteSinkMode::Data)
        }
        ConnectorWriteInputShape::RowLineage { .. } => {
            Ok(novarocks_sql::planning::dml::DmlWriteSinkMode::RowLineageData)
        }
        ConnectorWriteInputShape::PositionDelete { .. } => {
            Ok(novarocks_sql::planning::dml::DmlWriteSinkMode::PositionDeletes)
        }
        ConnectorWriteInputShape::DeletionVector { .. } => {
            Ok(novarocks_sql::planning::dml::DmlWriteSinkMode::DeletionVectors)
        }
        ConnectorWriteInputShape::EqualityDelete { .. } => {
            Ok(novarocks_sql::planning::dml::DmlWriteSinkMode::EqualityDeletes)
        }
    }
}

pub(crate) fn looks_like_maintenance_statement(sql: &str) -> bool {
    if novarocks_sql::syntax::looks_like_call_procedure(sql) {
        return true;
    }
    let Ok(normalized) = novarocks_sql::syntax::normalize_for_raw_parse(sql) else {
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
            novarocks::connector::connector_request_context(None, Arc::clone(&request_cancelled))
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
            novarocks::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))
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
