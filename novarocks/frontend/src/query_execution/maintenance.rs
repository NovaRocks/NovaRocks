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

use crate::common::query_cancellation::QueryCancellationView;
use crate::connector::cleanup_maintenance::{CleanupBatchExecution, CleanupMaintenanceSession};
use crate::connector::distributed_rewrite_application::DistributedRewriteIntent;
use crate::connector::metadata_maintenance::{
    CompletedMetadataMaintenance, MetadataMaintenanceIntent, MetadataMaintenanceSession,
};
use crate::maintenance::MaintenanceTarget;
use crate::query_execution::distributed_rewrite::DistributedRewriteMaintenanceSession;
use crate::query_execution::preparation::scan::ScanBindingResolver;
use crate::runtime::query_result::QueryResult;
use novarocks_spi::connector::{
    CandidatePage, ConnectorCleanupOperationId, ConnectorCleanupOwnedRefSelection,
    ConnectorControlResolver, ConnectorDistributedRewriteReceipt, ConnectorError,
    ConnectorMutationOperationId, ConnectorRewriteCohortRead, ConnectorTableObjectBindingFailure,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectId, ConnectorTableObjectRebindRequest,
    ConnectorTableObjectSelector, ConnectorTableResolution, ConnectorWriteAbortOutcome,
    ConnectorWriteCohortId, ConnectorWriteInputShape, ConnectorWriteReceipt,
    ExternalMutationOutcome, PreparedBatch,
};

pub const TABLE_MAINTENANCE_SERVICE_UNAVAILABLE: &str = "table maintenance service is not injected";

/// Core-prepared, Frontend-encoded staging dispatch for one exact rewrite
/// cohort. The Frontend can inspect the immutable encoder input, but only this
/// carrier retains the prepared fragments, admitted execution context, and
/// sealed connector-write registration required to submit the write.
pub struct PreparedDistributedRewriteCohort {
    encoding: crate::query_execution::compiler::NativeFragmentEncodingInput,
    query_execution: crate::query_execution::service::QueryExecutionService,
    execution: crate::common::admitted_query_context::QueryExecutionContext,
    write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl PreparedDistributedRewriteCohort {
    /// A rewrite group's plan always carries a writer node, so its encoding
    /// input always needs this session's sealed recipes. Sealing them here,
    /// from the very session the cohort commits through, is what keeps the plan
    /// and the recipes from being two independent caller choices that can
    /// disagree -- a plan submitted without them fails to encode at all.
    fn new(
        encoding: crate::query_execution::compiler::NativeFragmentEncodingInput,
        query_execution: crate::query_execution::service::QueryExecutionService,
        execution: crate::common::admitted_query_context::QueryExecutionContext,
        write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ) -> Result<Self, String> {
        let sealed_write_targets = write_session
            .seal_write_targets()
            .map_err(|error| format!("seal distributed rewrite write targets: {error}"))?;
        Ok(Self {
            encoding: encoding.with_sealed_write_targets(sealed_write_targets),
            query_execution,
            execution,
            write_session,
        })
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
    ) -> Result<crate::query_execution::outcome::ConnectorWriteSessionCompletion, String> {
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
        let request = crate::query_execution::contract::with_connector_write_session(
            request,
            std::sync::Arc::clone(&self.write_session),
        )
        .map_err(|error| error.to_string())?;
        let session_completion = self
            .query_execution
            .execute(request)
            .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_write)
            .map(crate::query_execution::outcome::WriteExecutionOutcome::into_write_session)
            .map_err(|error| error.to_string())?;
        // The dual barrier is what produces a session completion, so its
        // absence here means the write data plane never closed -- not that the
        // group happened to stage nothing.
        session_completion.ok_or_else(|| {
            "connector staging terminal closed without a prepared write set".to_string()
        })
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

    #[allow(
        dead_code,
        reason = "Retained for staged query-execution contract and lifecycle integration."
    )]
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
    TargetReplaced,
}

/// Result of rebinding a durable maintenance target to its current table.
///
/// `Bound` is the only result that permits an attempt to continue. A missing
/// target and a same-name replacement are distinct terminal outcomes; provider
/// capability and transport errors remain errors and must not be reclassified.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceTargetRebind {
    Bound,
    Replaced,
    Missing,
}

impl OptimizeJobState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Finished => "FINISHED",
            Self::Failed => "FAILED",
            Self::TargetReplaced => "TARGET_REPLACED",
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

    /// Capture the provider-owned physical identity together with submission
    /// target resolution. Durable callers persist this value with the logical
    /// target and must rebind it before every future maintenance attempt.
    fn capture_target_object_id(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<ConnectorTableObjectId, String>;

    /// Rebind the logical target only when it remains the captured physical
    /// object. This intentionally has no default: silently skipping the
    /// binding check would allow maintenance work on a replacement table.
    fn rebind_target_object(
        &self,
        target: &MaintenanceTarget,
        expected_object_id: &ConnectorTableObjectId,
    ) -> Result<MaintenanceTargetRebind, String>;

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

    /// Plan the second cleanup pass after the frontend has durably selected
    /// exact mature owned refs. An empty selection is a valid ref-only plan;
    /// it must never become an object sweep.
    fn plan_selected_owned_ref_cleanup_maintenance(
        &self,
        _target: &MaintenanceTarget,
        _operation_id: ConnectorCleanupOperationId,
        _older_than_ms: i64,
        _selection: ConnectorCleanupOwnedRefSelection,
    ) -> Result<CleanupMaintenanceSession, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn plan_selected_owned_ref_cleanup_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        selection: ConnectorCleanupOwnedRefSelection,
        _attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.plan_selected_owned_ref_cleanup_maintenance(
            target,
            operation_id,
            older_than_ms,
            selection,
        )
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

    fn prepare_distributed_rewrite_cohort(
        &self,
        _session: &DistributedRewriteMaintenanceSession,
        _cohort_id: ConnectorWriteCohortId,
    ) -> Result<PreparedDistributedRewriteCohort, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn accumulate_distributed_rewrite_group(
        &self,
        _session: &DistributedRewriteMaintenanceSession,
        _completion: crate::query_execution::outcome::ConnectorWriteSessionCompletion,
    ) -> Result<(), String> {
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

    /// Executes one already-lowered maintenance write statement. The parser
    /// owner has consumed SQL syntax before this boundary, so implementations
    /// must not reparse or probe raw source text.
    fn handle_typed_statement(
        &self,
        _engine: &dyn TableMaintenanceEngine,
        _statement: crate::table_maintenance::ParsedMaintenanceStatement,
        _spark_procedure: bool,
        _context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    /// Executes the read-only typed `SHOW ALTER TABLE OPTIMIZE` presentation
    /// without creating a maintenance engine or re-entering a raw parser.
    #[allow(
        private_interfaces,
        reason = "The typed SHOW OPTIMIZE parser carrier is intentionally confined to the table-maintenance boundary."
    )]
    fn handle_typed_show_optimize(
        &self,
        _statement: crate::table_maintenance::ParsedShowOptimize,
        _context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
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

    fn capture_target_object_id_with_context(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<ConnectorTableObjectId, String> {
        capture_target_object_id_with_ports(
            self.kernel.connector_control().as_ref(),
            target,
            self.connector_context.clone(),
        )
    }

    fn rebind_target_object_with_context(
        &self,
        target: &MaintenanceTarget,
        expected_object_id: &ConnectorTableObjectId,
    ) -> Result<MaintenanceTargetRebind, String> {
        rebind_target_object_with_ports(
            self.kernel.connector_control().as_ref(),
            target,
            expected_object_id,
            self.connector_context.clone(),
        )
    }
}

fn capture_target_object_id_with_ports(
    controls: &dyn ConnectorControlResolver,
    target: &MaintenanceTarget,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorTableObjectId, String> {
    let identity = RequestScopedMaintenanceEngine::target_identity(target)?;
    let lease = controls
        .acquire_current(&identity.instance_id)
        .map_err(|error| {
            format!("acquire current connector generation for target capture: {error}")
        })?;
    let binding = lease
        .binding()
        .metadata()
        .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
            table: identity,
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context,
        })
        .map(|binding| binding.object_id);
    captured_target_object_id_from_connector_result(binding)
}

fn rebind_target_object_with_ports(
    controls: &dyn ConnectorControlResolver,
    target: &MaintenanceTarget,
    expected_object_id: &ConnectorTableObjectId,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MaintenanceTargetRebind, String> {
    let identity = RequestScopedMaintenanceEngine::target_identity(target)?;
    let lease = controls
        .acquire_current(&identity.instance_id)
        .map_err(|error| {
            format!("acquire current connector generation for target rebind: {error}")
        })?;
    let binding =
        lease
            .binding()
            .metadata()
            .rebind_table_object_binding(ConnectorTableObjectRebindRequest {
                table: identity,
                expected_object_id: expected_object_id.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context,
            });
    maintenance_target_rebind_from_connector_result(binding.map(|_| ()))
}

fn captured_target_object_id_from_connector_result(
    object_id: Result<ConnectorTableObjectId, ConnectorError>,
) -> Result<ConnectorTableObjectId, String> {
    object_id.map_err(|error| format!("capture maintenance target object identity: {error}"))
}

fn maintenance_target_rebind_from_connector_result(
    binding: Result<(), ConnectorError>,
) -> Result<MaintenanceTargetRebind, String> {
    match binding {
        Ok(_) => Ok(MaintenanceTargetRebind::Bound),
        Err(error) => match error.table_object_binding_failure() {
            Some(ConnectorTableObjectBindingFailure::Replaced) => {
                Ok(MaintenanceTargetRebind::Replaced)
            }
            Some(ConnectorTableObjectBindingFailure::Missing) => {
                Ok(MaintenanceTargetRebind::Missing)
            }
            None => Err(format!(
                "rebind maintenance target object identity: {error}"
            )),
        },
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

impl crate::connector::metadata_maintenance::MetadataMaintenanceCacheFinalizer
    for RequestScopedMaintenanceEngine
{
    fn invalidate_generic_table(
        &self,
        table: &novarocks_spi::connector::ConnectorTableIdentity,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        crate::connector::metadata_maintenance::MetadataMaintenanceCacheFinalizer::invalidate_generic_table(
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
        let target = crate::catalog_application::resolver::resolve_existing_table_target(
            &self.kernel,
            &novarocks_sql::semantic::ObjectName {
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

    fn capture_target_object_id(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<ConnectorTableObjectId, String> {
        self.capture_target_object_id_with_context(target)
    }

    fn rebind_target_object(
        &self,
        target: &MaintenanceTarget,
        expected_object_id: &ConnectorTableObjectId,
    ) -> Result<MaintenanceTargetRebind, String> {
        self.rebind_target_object_with_context(target, expected_object_id)
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
        let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
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
        if crate::mv::domain::storage_observation::observe_lake_package(
            self.kernel.mv_storage_observation().as_ref(),
            &exact_lease,
            &metadata,
            self.connector_context.clone(),
        )
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
        crate::connector::metadata_maintenance::plan_metadata_maintenance_session(
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
        crate::connector::metadata_maintenance::execute_planned_metadata_maintenance(session, self)
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

    fn plan_selected_owned_ref_cleanup_maintenance(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        selection: ConnectorCleanupOwnedRefSelection,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.plan_selected_owned_ref_cleanup_maintenance_with_attempt_context(
            target,
            operation_id,
            older_than_ms,
            selection,
            &MaintenanceAttemptContext::uncancelled(),
        )
    }

    fn plan_selected_owned_ref_cleanup_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        selection: ConnectorCleanupOwnedRefSelection,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        let identity = Self::target_identity(target)?;
        CleanupMaintenanceSession::plan_selected_owned_refs(
            self.kernel.connector_control().as_ref(),
            &identity.instance_id.clone(),
            operation_id,
            identity,
            older_than_ms,
            selection,
            self.connector_context_for_attempt(attempt)?,
        )
        .map_err(|error| format!("plan selected owned-ref cleanup operation: {error}"))
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
            self.kernel.typed_connector_control(),
            self.kernel.function_catalog().as_ref(),
            self.kernel.query_execution(),
            session.session(),
            cohort_id,
            session.execution(),
            session.context(),
        )
    }

    fn accumulate_distributed_rewrite_group(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        completion: crate::query_execution::outcome::ConnectorWriteSessionCompletion,
    ) -> Result<(), String> {
        let (_, prepared) = completion.into_parts();
        session
            .session()
            .accumulate(prepared)
            .map_err(|error| format!("accumulate distributed rewrite group: {error}"))
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
        crate::connector::distributed_rewrite_application::plan_distributed_rewrite_session(
            self.kernel.query_execution(),
            self.kernel.connector_control().as_ref(),
            self.kernel.typed_connector_control(),
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

    fn capture_target_object_id(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<ConnectorTableObjectId, String> {
        self.request_engine()?.capture_target_object_id(target)
    }

    fn rebind_target_object(
        &self,
        target: &MaintenanceTarget,
        expected_object_id: &ConnectorTableObjectId,
    ) -> Result<MaintenanceTargetRebind, String> {
        self.request_engine()?
            .rebind_target_object(target, expected_object_id)
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

    fn plan_selected_owned_ref_cleanup_maintenance(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        selection: ConnectorCleanupOwnedRefSelection,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.request_engine()?
            .plan_selected_owned_ref_cleanup_maintenance(
                target,
                operation_id,
                older_than_ms,
                selection,
            )
    }

    fn plan_selected_owned_ref_cleanup_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        selection: ConnectorCleanupOwnedRefSelection,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.request_engine()?
            .plan_selected_owned_ref_cleanup_maintenance_with_attempt_context(
                target,
                operation_id,
                older_than_ms,
                selection,
                attempt,
            )
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

    fn accumulate_distributed_rewrite_group(
        &self,
        session: &DistributedRewriteMaintenanceSession,
        completion: crate::query_execution::outcome::ConnectorWriteSessionCompletion,
    ) -> Result<(), String> {
        self.request_engine()?
            .accumulate_distributed_rewrite_group(session, completion)
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
    typed_connector_control: &std::sync::Arc<crate::connector::ConnectorControlHost>,
    function_catalog: &dyn novarocks_sql::compiler::SqlFunctionCatalog,
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
    let table_bindings =
        Arc::new(crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()?);
    // A cohort that rewrites table rows names exactly the data files its
    // commit replaces, so its read is frozen from that set. A cohort that
    // rewrites delete artifacts reads no table rows at all, so it has no data
    // file set to be frozen from and names its frozen group instead -- the
    // same group its commit resolves the replaced artifacts from.
    let owner = session
        .lease()
        .planning_lease()
        .binding()
        .descriptor()
        .instance_id
        .clone();
    let (resolver, physical_plan): (Box<dyn ScanBindingResolver>, _) = match cohort.read() {
        ConnectorRewriteCohortRead::PinnedFileSet(pinned) => {
            let source_binding =
                crate::query_execution::distributed_rewrite::admit_pinned_rewrite_scan_binding(
                    table_bindings.as_ref(),
                    cohort.scan_schema(),
                )?;
            let read = crate::query_execution::preparation::scan::QueryPinnedFileSetRead {
                pinned: pinned.clone(),
                owner,
                planning_lease: session.lease().planning_lease(),
            };
            let resolver =
                crate::query_execution::distributed_rewrite::pinned_rewrite_read_resolver(
                    source_binding,
                    read,
                );
            let physical_plan =
                crate::query_execution::distributed_rewrite::pinned_rewrite_scan_physical_plan(
                    cohort.scan_schema(),
                    source_binding,
                );
            (Box::new(resolver), physical_plan)
        }
        ConnectorRewriteCohortRead::DeleteArtifactGroup(group) => {
            let source_binding =
                crate::query_execution::distributed_rewrite::admit_rewrite_group_scan_binding(
                    table_bindings.as_ref(),
                    cohort.scan_schema(),
                )?;
            let read = crate::query_execution::preparation::scan::QueryRewriteGroupRead {
                group: group.clone(),
                group_digest: cohort.group_digest(),
                owner,
                planning_lease: session.lease().planning_lease(),
            };
            let resolver = crate::query_execution::distributed_rewrite::rewrite_group_read_resolver(
                source_binding,
                read,
            );
            let physical_plan =
                crate::query_execution::distributed_rewrite::rewrite_group_scan_physical_plan(
                    cohort.scan_schema(),
                    source_binding,
                );
            (Box::new(resolver), physical_plan)
        }
    };
    // The session sealed one logical target per frozen group, so this group's
    // writer recipe and its ordinal both come from there. Deriving the ordinal
    // from anything else would let a group's artifacts be attributed to another
    // group's writer, which the prepared set cannot catch.
    let write_target = session
        .write_target(cohort_id)
        .map_err(|error| format!("resolve frozen rewrite group write target: {error}"))?;
    let target_binding =
        crate::query_execution::planning::write_sink::admit_session_connector_write_target(
            table_bindings.as_ref(),
            rewrite_target_identity(session, cohort_id),
            write_target,
            session.lease().planning_lease(),
        )?;
    let sink =
        crate::query_execution::planning::write_sink::dml_write_plan_input_for_admitted_target(
            table_bindings.as_ref(),
            target_binding,
            rewrite_sink_mode(write_target.input())?,
            novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
        )?;
    crate::connector::validate_request_context(context)?;
    let mut optimizer_settings = execution.optimizer_settings().clone();
    if optimizer_settings.cbo_broadcast_backend_count.is_none() {
        optimizer_settings.effective_backend_count =
            Some(execution.topology().targets().len() as f64);
    }
    let distributed_plan =
        novarocks_sql::planning::dml::build_frozen_connector_write_dataflow_plan(
            physical_plan,
            sink,
            write_target.ordinal(),
            write_target.statistics().requirements(),
            function_catalog,
            &optimizer_settings,
        )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        connector_control,
        context,
        Some(table_bindings.as_ref()),
        Some(resolver.as_ref()),
        crate::query_execution::dml::write::scan_preparation_options(
            typed_connector_control,
            &optimizer_settings,
            execution,
        )?,
    )?;
    let write_session = session
        .write_session()
        .ok_or_else(|| "distributed rewrite no-op has no write session".to_string())?;
    PreparedDistributedRewriteCohort::new(
        crate::query_execution::compiler::NativeFragmentEncodingInput::new(
            distributed_plan,
            prepared,
        ),
        query_execution.clone(),
        execution.clone(),
        write_session.clone(),
    )
}

fn rewrite_target_identity(
    session: &crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
) -> novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity {
    novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::new(
        session
            .lease()
            .descriptor()
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

#[cfg(test)]
mod maintenance_attempt_context_tests {
    use bytes::Bytes;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorTableObjectBindingFailure,
        ConnectorTableObjectId,
    };

    use super::{
        MaintenanceAttemptCancellationSource, MaintenanceAttemptContext, MaintenanceTargetRebind,
        OptimizeJobState, captured_target_object_id_from_connector_result,
        maintenance_target_rebind_from_connector_result,
    };

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

    #[test]
    fn capture_preserves_the_provider_owned_object_id() {
        let object_id = ConnectorTableObjectId::try_new(Bytes::from_static(b"captured-object"))
            .expect("valid object id");

        let captured = captured_target_object_id_from_connector_result(Ok(object_id.clone()))
            .expect("capture succeeds");

        assert_eq!(captured, object_id);
    }

    #[test]
    fn rebind_accepts_the_same_physical_object() {
        assert_eq!(
            maintenance_target_rebind_from_connector_result(Ok(())).expect("bound"),
            MaintenanceTargetRebind::Bound
        );
    }

    #[test]
    fn rebind_classifies_replacement_without_reading_the_error_message() {
        let error = ConnectorError::table_object_binding(
            ConnectorTableObjectBindingFailure::Replaced,
            "unrelated provider wording",
        );

        assert_eq!(
            maintenance_target_rebind_from_connector_result(Err(error)).expect("replacement"),
            MaintenanceTargetRebind::Replaced
        );
    }

    #[test]
    fn rebind_classifies_missing_without_reading_the_error_message() {
        let error = ConnectorError::table_object_binding(
            ConnectorTableObjectBindingFailure::Missing,
            "unrelated provider wording",
        );

        assert_eq!(
            maintenance_target_rebind_from_connector_result(Err(error)).expect("missing"),
            MaintenanceTargetRebind::Missing
        );
    }

    #[test]
    fn rebind_keeps_provider_unsupported_as_an_error() {
        let error = ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "provider does not implement target rebinding",
        );

        let error = maintenance_target_rebind_from_connector_result(Err(error))
            .expect_err("unsupported is not replacement");

        assert!(error.contains("Unsupported"));
    }

    #[test]
    fn optimize_target_replacement_has_a_stable_show_value() {
        assert_eq!(OptimizeJobState::TargetReplaced.as_str(), "TARGET_REPLACED");
    }
}
