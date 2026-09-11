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

//! Core-owned distributed-query request contract.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::common::query_cancellation::QueryCancellationView;
use crate::query_execution::artifact::{
    PreparedDistributedAttemptTemplate, PreparedDistributedQuery,
};
use crate::query_execution::native_fragment::NativeFragmentAttachment;
pub use crate::query_execution::outcome::DistributedQueryOutcome;
pub use crate::query_execution::outcome::FragmentProfileSet;
pub use crate::query_execution::outcome::QueryOutcomeFactory;
use crate::query_execution::post_compile::NativeFragmentEncodingInput;
pub use crate::query_execution::profile::ProfileTerminalBuilder;
pub use crate::query_execution::statistics::StatisticsCollectionProgram;
pub use crate::query_execution::statistics::StatisticsExecutionMode;
pub use crate::query_execution::statistics::StatisticsExecutionPolicy;
use novarocks_execution::exec::spill::{SpillConfig, SpillMode};
use novarocks_execution::runtime::query_options::{
    QueryCacheOptions, QueryOptions as RuntimeQueryOptions,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::preparation::FrozenExecutionDescription;
use novarocks_types::BackendProcessId;

#[cfg(test)]
pub(crate) use novarocks_types::QueryId;

/// Query options resolved by core before ownership crosses into frontend.
///
/// The runtime representation stays private; frontend only receives stable
/// scalar views needed to schedule, submit, and time out native work.
pub struct ResolvedQueryOptions {
    runtime: RuntimeQueryOptions,
}

impl ResolvedQueryOptions {
    pub(crate) fn from_upstream(options: Option<QueryOptions>) -> Self {
        let mut runtime = options
            .as_ref()
            .map(reconstruct_runtime_query_options)
            .unwrap_or_default();
        let pipeline_dop = novarocks_execution::runtime::exec_env::calc_pipeline_dop(
            runtime.pipeline_dop.unwrap_or_default(),
        );
        debug_assert!(pipeline_dop > 0, "resolved pipeline DOP must be positive");
        runtime.pipeline_dop = Some(pipeline_dop);
        Self { runtime }
    }

    pub fn timeout_ms(&self) -> i64 {
        self.runtime
            .query_timeout
            .map(|seconds| i64::from(seconds) * 1_000)
            .unwrap_or(300_000)
    }

    pub fn native_submission_options(&self) -> NativeSubmissionOptionsView {
        NativeSubmissionOptionsView {
            pipeline_dop: self
                .runtime
                .pipeline_dop
                .expect("core resolves pipeline DOP before request handoff"),
            enable_profile: self.runtime.enable_profile,
        }
    }

    pub fn runtime_filter_lifecycle(&self) -> RuntimeFilterLifecycleView {
        let (delivery_expire, query_expire) =
            novarocks_execution::runtime::query_options::query_expire_durations(Some(
                &self.runtime,
            ));
        RuntimeFilterLifecycleView {
            delivery_expire,
            query_expire,
        }
    }

    /// Frozen execution options exposed to the Frontend only for its
    /// role-owned native wire projection.  This does not provide lifecycle
    /// construction or a mutable execution handle.
    pub fn runtime_options(&self) -> &RuntimeQueryOptions {
        &self.runtime
    }
}

/// Reconstructs the Frontend-local execution view from an already validated
/// protocol value without creating a second wire representation or decoder.
fn reconstruct_runtime_query_options(options: &QueryOptions) -> RuntimeQueryOptions {
    let src = options.as_proto();
    RuntimeQueryOptions {
        batch_size: (src.batch_size > 0).then_some(src.batch_size),
        query_timeout: (src.query_timeout > 0).then_some(src.query_timeout),
        query_delivery_timeout: (src.query_delivery_timeout > 0)
            .then_some(src.query_delivery_timeout),
        enable_profile: src.enable_profile,
        runtime_profile_report_interval: (src.runtime_profile_report_interval > 0)
            .then_some(src.runtime_profile_report_interval),
        pipeline_dop: (src.pipeline_dop > 0).then_some(src.pipeline_dop),
        exec_mem_limit: (src.query_mem_limit > 0).then_some(src.query_mem_limit),
        connector_io_tasks_per_scan_operator: (src.connector_io_tasks_per_scan_operator > 0)
            .then_some(src.connector_io_tasks_per_scan_operator),
        orc_use_column_names: src.orc_use_column_names,
        enable_file_metacache: src.enable_file_metacache,
        enable_file_pagecache: src.enable_file_pagecache,
        enable_parquet_reader_page_index: src.enable_parquet_reader_page_index,
        runtime_filter_scan_wait_time_ms: src.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: src.runtime_filter_wait_timeout_ms,
        allow_throw_exception: src.allow_throw_exception,
        group_concat_max_len: src.group_concat_max_len,
        enable_join_runtime_bitset_filter: src.enable_join_runtime_bitset_filter,
        global_runtime_filter_build_max_size: (src.global_runtime_filter_build_max_size > 0)
            .then_some(src.global_runtime_filter_build_max_size),
        cache: QueryCacheOptions {
            enable_scan_datacache: src.enable_scan_datacache,
            enable_populate_datacache: src.enable_populate_datacache,
            enable_datacache_async_populate_mode: src.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: src.enable_datacache_io_adaptor,
            enable_cache_select: src.enable_cache_select,
            datacache_evict_probability: src.datacache_evict_probability,
            datacache_priority: (src.datacache_priority != 0).then_some(src.datacache_priority),
            datacache_ttl_seconds: (src.datacache_ttl_seconds > 0)
                .then_some(src.datacache_ttl_seconds),
            datacache_sharing_work_period: (src.datacache_sharing_work_period > 0)
                .then_some(src.datacache_sharing_work_period),
        },
        spill: src.enable_spill.then(|| {
            let spill = src
                .spill_options
                .as_ref()
                .expect("validated enabled spilling has spill options");
            SpillConfig {
                enable_spill: src.enable_spill,
                spill_mode: match spill.spill_mode {
                    0 => SpillMode::Auto,
                    1 => SpillMode::Force,
                    2 => SpillMode::None,
                    _ => {
                        unreachable!("validated Protocol query options have a supported spill mode")
                    }
                },
                spill_mem_limit_threshold: (spill.spill_mem_limit_threshold > 0.0)
                    .then_some(spill.spill_mem_limit_threshold),
                spill_operator_min_bytes: (spill.spill_operator_min_bytes > 0)
                    .then_some(spill.spill_operator_min_bytes),
                spill_operator_max_bytes: (spill.spill_operator_max_bytes > 0)
                    .then_some(spill.spill_operator_max_bytes),
                spill_encode_level: (spill.spill_encode_level > 0)
                    .then_some(spill.spill_encode_level),
                enable_spill_buffer_read: Some(spill.enable_spill_buffer_read),
                max_spill_read_buffer_bytes_per_driver: (spill
                    .max_spill_read_buffer_bytes_per_driver
                    > 0)
                .then_some(spill.max_spill_read_buffer_bytes_per_driver),
                spill_mem_table_size: (spill.spill_mem_table_size > 0)
                    .then_some(spill.spill_mem_table_size),
                spill_mem_table_num: (spill.spill_mem_table_num > 0)
                    .then_some(spill.spill_mem_table_num),
            }
        }),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeSubmissionOptionsView {
    pipeline_dop: i32,
    enable_profile: bool,
}

impl NativeSubmissionOptionsView {
    pub const fn pipeline_dop(self) -> i32 {
        self.pipeline_dop
    }

    pub const fn enable_profile(self) -> bool {
        self.enable_profile
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterLifecycleView {
    delivery_expire: std::time::Duration,
    query_expire: std::time::Duration,
}

impl RuntimeFilterLifecycleView {
    pub const fn delivery_expire(self) -> std::time::Duration {
        self.delivery_expire
    }

    pub const fn query_expire(self) -> std::time::Duration {
        self.query_expire
    }
}

/// The engine-visible purpose of a distributed execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedQueryIntent {
    Result,
    Write,
    Profile,
    /// Internal collection execution. Its completion carries typed evidence,
    /// never a `QueryResult` that could be returned as user MySQL rows.
    Statistics,
}

/// An owned request passed from core to the injected execution coordinator.
///
/// Every field is private so role crates cannot assemble a request from
/// unrelated prepared/native artifacts or replace its cancellation/completion
/// capabilities.
pub struct DistributedQueryRequest {
    payload: DistributedQueryPayload,
    topology: crate::common::backend_topology::BackendTopologySnapshot,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    completion: QueryOutcomeFactory,
    /// The NCP-6 write session, present exactly when this query's plan carries
    /// the dataflow write shape.
    write_stack_session: Option<Arc<crate::query_execution::write_session::ConnectorWriteSession>>,
    write_root_decode_contract:
        Option<crate::query_execution::write_result::RootWriteDecodeContract>,
    statistics_program: Option<StatisticsCollectionProgram>,
}

enum DistributedQueryPayload {
    RestartableRead(Arc<RestartableReadExecution>),
    SingleUse {
        description: Arc<FrozenExecutionDescription>,
        artifacts: PreparedDistributedQuery,
        options: Arc<ResolvedQueryOptions>,
    },
}

/// Closed, immutable source for every legacy coordinator round of one
/// restartable read.
///
/// The frozen description, static native template, Connector access recipes,
/// resolved options and result intent are created together by the sole
/// finalizer. This carrier deliberately does not pretend to be an executable
/// query-application request: that move-only request can be created only when
/// the production Native adapter also supplies its exact seed.
pub(crate) struct RestartableReadExecution {
    description: Arc<FrozenExecutionDescription>,
    attempt_template: PreparedDistributedAttemptTemplate,
    options: Arc<ResolvedQueryOptions>,
    intent: DistributedQueryIntent,
}

impl RestartableReadExecution {
    pub(crate) fn instantiate_attempt(
        self: &Arc<Self>,
        execution: &QueryExecutionContext,
    ) -> DistributedQueryRequest {
        DistributedQueryRequest {
            payload: DistributedQueryPayload::RestartableRead(Arc::clone(self)),
            topology: execution.topology().clone(),
            deadline: execution.deadline(),
            cancellation: execution.cancellation().clone(),
            completion: QueryOutcomeFactory::new(self.intent),
            write_stack_session: None,
            write_root_decode_contract: None,
            statistics_program: None,
        }
    }

    pub(crate) fn shared_plan(&self) -> Arc<novarocks_sql::plan_read::DistributedPlan> {
        self.description.shared_plan()
    }

    #[cfg(test)]
    pub(crate) fn instantiate_artifacts_for_test(&self) -> PreparedDistributedQuery {
        self.attempt_template.instantiate()
    }
}

impl DistributedQueryRequest {
    pub fn frozen_description(&self) -> &FrozenExecutionDescription {
        match &self.payload {
            DistributedQueryPayload::RestartableRead(read) => read.description.as_ref(),
            DistributedQueryPayload::SingleUse { description, .. } => description.as_ref(),
        }
    }

    pub fn intent(&self) -> DistributedQueryIntent {
        self.completion.intent()
    }

    pub fn options(&self) -> &ResolvedQueryOptions {
        match &self.payload {
            DistributedQueryPayload::RestartableRead(read) => read.options.as_ref(),
            DistributedQueryPayload::SingleUse { options, .. } => options.as_ref(),
        }
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }

    pub fn topology(&self) -> &crate::common::backend_topology::BackendTopologySnapshot {
        &self.topology
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn statistics_program(&self) -> Option<&StatisticsCollectionProgram> {
        self.statistics_program.as_ref()
    }

    fn write_root_targets(
        &self,
    ) -> Option<&[novarocks_spi::connector::write_stack::WriteTargetOrdinal]> {
        match &self.payload {
            DistributedQueryPayload::RestartableRead(_) => None,
            DistributedQueryPayload::SingleUse { artifacts, .. } => artifacts.write_root_targets(),
        }
    }

    /// Return the closed capability for replacement attempts when this logical
    /// execution's frozen recovery policy permits them.
    pub(crate) fn restartable_read(&self) -> Option<Arc<RestartableReadExecution>> {
        match &self.payload {
            DistributedQueryPayload::RestartableRead(read) => Some(Arc::clone(read)),
            DistributedQueryPayload::SingleUse { .. } => None,
        }
    }

    pub fn into_parts(self) -> DistributedQueryRequestParts {
        let (description, artifacts, options) = match self.payload {
            DistributedQueryPayload::RestartableRead(read) => (
                Arc::clone(&read.description),
                read.attempt_template.instantiate(),
                Arc::clone(&read.options),
            ),
            DistributedQueryPayload::SingleUse {
                description,
                artifacts,
                options,
            } => (description, artifacts, options),
        };
        DistributedQueryRequestParts {
            description,
            artifacts,
            options,
            topology: self.topology,
            deadline: self.deadline,
            cancellation: self.cancellation,
            completion: self.completion,
            write_stack_session: self.write_stack_session,
            write_root_decode_contract: self.write_root_decode_contract,
            statistics_program: self.statistics_program,
        }
    }
}

/// Consuming frontend handoff. There is deliberately no constructor,
/// `Clone`, or inverse recombination API.
pub struct DistributedQueryRequestParts {
    pub description: Arc<FrozenExecutionDescription>,
    pub artifacts: PreparedDistributedQuery,
    pub options: Arc<ResolvedQueryOptions>,
    pub topology: crate::common::backend_topology::BackendTopologySnapshot,
    pub deadline: Option<Instant>,
    pub cancellation: QueryCancellationView,
    pub completion: QueryOutcomeFactory,
    pub(crate) write_stack_session:
        Option<Arc<crate::query_execution::write_session::ConnectorWriteSession>>,
    pub(crate) write_root_decode_contract:
        Option<crate::query_execution::write_result::RootWriteDecodeContract>,
    pub statistics_program: Option<StatisticsCollectionProgram>,
}

pub(crate) fn build_request_from_finalized_execution(
    finalized: crate::query_execution::post_compile::FinalizedDistributedExecution,
    options: Option<QueryOptions>,
    intent: DistributedQueryIntent,
    execution: &QueryExecutionContext,
    statistics_program: Option<StatisticsCollectionProgram>,
) -> Result<DistributedQueryRequest, DistributedQueryError> {
    if (intent == DistributedQueryIntent::Statistics) != statistics_program.is_some() {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "statistics intent and typed StatisticsCollectionProgram must be present together",
        ));
    }
    let (description, attempt_template) = finalized.into_parts();
    let options = Arc::new(ResolvedQueryOptions::from_upstream(options));
    let restartable_read = matches!(
        intent,
        DistributedQueryIntent::Result | DistributedQueryIntent::Profile
    ) && description.kind()
        == novarocks_query_application::api::QueryExecutionKind::Read
        && description.recovery()
            == novarocks_query_application::coordination::RecoveryMode::RestartAttemptBeforeVisibility;
    let payload = if restartable_read {
        DistributedQueryPayload::RestartableRead(Arc::new(RestartableReadExecution {
            description,
            attempt_template,
            options,
            intent,
        }))
    } else {
        DistributedQueryPayload::SingleUse {
            description,
            artifacts: attempt_template.instantiate(),
            options,
        }
    };
    Ok(DistributedQueryRequest {
        payload,
        topology: execution.topology().clone(),
        deadline: execution.deadline(),
        cancellation: execution.cancellation().clone(),
        completion: QueryOutcomeFactory::new(intent),
        write_stack_session: None,
        write_root_decode_contract: None,
        statistics_program,
    })
}

/// Request construction accepts only the execution projection captured at
/// admission; callers cannot synthesize an empty topology or cancellation
/// fallback at the coordinator boundary.
pub(crate) fn build_distributed_query_request_with_execution(
    encoding: NativeFragmentEncodingInput,
    native_bundle: NativeFragmentAttachment,
    options: Option<QueryOptions>,
    intent: DistributedQueryIntent,
    execution: &QueryExecutionContext,
) -> Result<DistributedQueryRequest, DistributedQueryError> {
    crate::query_execution::post_compile::PreparedDistributedQueryAssembly::new(
        encoding,
        options,
        intent,
        execution.clone(),
    )
    .finish(native_bundle)
    .map_err(|error| {
        DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error)
    })
}

/// Build a distributed request for internal statistics collection.  The
/// program is intentionally required here rather than carried in generic
/// query options, preventing a client-result request from acquiring a
/// statistics completion capability.
pub(crate) fn build_statistics_query_request_with_execution(
    encoding: NativeFragmentEncodingInput,
    native_bundle: NativeFragmentAttachment,
    options: Option<QueryOptions>,
    program: StatisticsCollectionProgram,
    execution: &QueryExecutionContext,
) -> Result<DistributedQueryRequest, DistributedQueryError> {
    crate::query_execution::post_compile::PreparedDistributedQueryAssembly::new(
        encoding,
        options,
        DistributedQueryIntent::Statistics,
        execution.clone(),
    )
    .finish_statistics(native_bundle, program)
    .map_err(|error| {
        DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error)
    })
}

/// Attach the NCP-6 write session to a sealed distributed write request.
///
/// A query carries this or the placement-deferred writer template, never both:
/// they describe the same write through two different data planes, and a query
/// that claimed both would have two answers to "did this commit".
pub(crate) fn with_connector_write_session(
    mut request: DistributedQueryRequest,
    session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
) -> Result<DistributedQueryRequest, DistributedQueryError> {
    if request.intent() != DistributedQueryIntent::Write {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "a connector write session is only valid for a distributed write request",
        ));
    }
    if request.write_stack_session.is_some() {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "distributed query already has a connector write session",
        ));
    }
    let query_targets = request.write_root_targets().ok_or_else(|| {
        DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "distributed write plan has no query-local TableFinish target contract",
        )
    })?;
    let decode_contract = crate::query_execution::write_result::RootWriteDecodeContract::try_new(
        query_targets,
        session.targets(),
    )
    .map_err(|error| {
        DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error)
    })?;
    request.write_stack_session = Some(session);
    request.write_root_decode_contract = Some(decode_contract);
    Ok(request)
}

/// Stable error categories exposed by the coordinator boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedQueryErrorKind {
    ContractViolation,
    Rejected,
    /// The statement observed a pre-ControlReady topology disposition, but
    /// its owner cannot prove the stable semantic binding and zero-effect
    /// conditions required to construct a replacement round.
    TopologyRetryUnsupported,
    Failed,
}

/// Closed, pre-ControlReady topology outcomes that a statement-level round
/// controller may consider for one bounded replan.  This is carried as typed
/// coordinator evidence rather than reconstructed from an error string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreReadyTopologyOutcome {
    BackendDraining {
        backend_idx: usize,
        process_id: BackendProcessId,
    },
    BackendProcessMismatch {
        backend_idx: usize,
        process_id: BackendProcessId,
    },
    BackendNotEligible {
        backend_idx: usize,
        process_id: BackendProcessId,
    },
    /// The backend rejected the FE's exact native compatibility identity
    /// before ControlReady. This is retryable only through the existing
    /// effect-gated whole-round controller.
    CompatibilityMismatch {
        backend_idx: usize,
        process_id: BackendProcessId,
    },
}

/// A coordinator failure that core can surface without naming a coordinator
/// implementation or frontend state type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryError {
    kind: DistributedQueryErrorKind,
    message: String,
    pre_ready_topology_outcome: Option<PreReadyTopologyOutcome>,
    pre_ready_topology_observation: bool,
}

impl DistributedQueryError {
    pub fn new(kind: DistributedQueryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            pre_ready_topology_outcome: None,
            pre_ready_topology_observation: false,
        }
    }

    /// Constructed only by the pre-ControlReady coordinator/barrier path.
    /// Callers must never infer this disposition from transport text or a
    /// post-ready lifecycle failure.
    pub(crate) fn pre_ready_topology(
        outcome: PreReadyTopologyOutcome,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: DistributedQueryErrorKind::Rejected,
            message: message.into(),
            pre_ready_topology_outcome: Some(outcome),
            pre_ready_topology_observation: false,
        }
    }

    /// A pre-ControlReady lifecycle transport loss can wait briefly for the
    /// membership authority to prove an exact captured-process replacement.
    /// It is not retry evidence by itself and must never be constructed from
    /// display text or after ControlReady.
    pub(crate) fn pre_ready_topology_observation(message: impl Into<String>) -> Self {
        Self {
            kind: DistributedQueryErrorKind::Failed,
            message: message.into(),
            pre_ready_topology_outcome: None,
            pre_ready_topology_observation: true,
        }
    }

    /// Turns typed pre-ready topology evidence into a fail-closed statement
    /// result when the operation has no whole-round replanning owner. The
    /// original outcome remains available to observability and callers; it is
    /// not reduced to transport/display text.
    pub(crate) fn topology_retry_unsupported(
        outcome: PreReadyTopologyOutcome,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: DistributedQueryErrorKind::TopologyRetryUnsupported,
            message: message.into(),
            pre_ready_topology_outcome: Some(outcome),
            pre_ready_topology_observation: false,
        }
    }

    pub fn kind(&self) -> DistributedQueryErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) const fn pre_ready_topology_outcome(&self) -> Option<PreReadyTopologyOutcome> {
        self.pre_ready_topology_outcome
    }

    pub(crate) const fn requires_pre_ready_topology_observation(&self) -> bool {
        self.pre_ready_topology_observation
    }
}

impl fmt::Display for DistributedQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for DistributedQueryError {}

/// Frontend-owned distributed query execution port.
pub trait DistributedQueryCoordinator: Send + Sync + 'static {
    /// Reserve the identity of one logical query without creating an
    /// execution attempt. SELECT preparation uses it only for diagnostics;
    /// the coordinator mints attempt identity after the frozen operation is
    /// submitted.
    fn reserve_logical_query(
        &self,
    ) -> Result<crate::query_execution::completion::LogicalQueryReservation, DistributedQueryError>
    {
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::Rejected,
            "distributed query coordinator does not reserve logical query identities",
        ))
    }

    /// Reserve the first attempt identity before connector metadata
    /// materialization. Production owns the query-id source; injected test
    /// coordinators fail closed unless they explicitly implement this port.
    fn reserve_initial_attempt(
        &self,
    ) -> Result<crate::query_execution::completion::QueryAttemptReservation, DistributedQueryError>
    {
        let logical = self.reserve_logical_query()?;
        crate::query_execution::completion::QueryAttemptReservation::first(logical.into_query_id())
    }

    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError>;

    /// Execute one non-retriable reserved attempt. Callers use this when
    /// connector planning has already observed attempt-scoped capabilities
    /// but statement semantics do not permit automatic whole-round replanning.
    fn execute_reserved(
        &self,
        _request: DistributedQueryRequest,
        _reservation: crate::query_execution::completion::QueryAttemptReservation,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "injected coordinator does not implement reserved distributed attempts",
        ))
    }

    /// Execute a statement operation whose replacement rounds must carry a
    /// newly derived completion formatter as well as a newly derived request.
    /// The default retains legacy single-round behavior for narrow test
    /// coordinators; the production coordinator overrides it with the
    /// statement-level pre-ready controller.
    fn execute_prepared(
        &self,
        operation: crate::query_execution::completion::PreparedDistributedQuery,
    ) -> Result<crate::runtime::statement_result::StatementResult, DistributedQueryError> {
        let (request, completion, attempt_factory, _logical_reservation) = operation.into_parts();
        if attempt_factory.is_some() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "injected coordinator does not implement statement-level pre-ready replan",
            ));
        }
        let outcome = self.execute(request)?;
        completion
            .complete(outcome)
            .map_err(|error| DistributedQueryError::new(DistributedQueryErrorKind::Failed, error))
    }

    /// Execute a statement operation whose caller retains its raw outcome.
    /// This is deliberately separate from `execute_prepared`: a distributed
    /// write must preserve connector commit/abort handles for the frontend
    /// transaction runner and cannot be rendered as a `StatementResult`.
    fn execute_prepared_raw(
        &self,
        operation: crate::query_execution::completion::PreparedRetriableDistributedRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let (request, _round_factory, reservation) = operation.into_parts();
        if reservation.is_some() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "injected coordinator does not implement reserved raw distributed attempts",
            ));
        }
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            format!(
                "injected coordinator does not implement raw pre-ready replan for {:?}",
                request.intent()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::reconstruct_runtime_query_options;
    use novarocks_proto_codec::lifecycle::QueryOptions;
    use novarocks_proto_models::novarocks;

    #[test]
    fn reconstructed_runtime_options_preserve_protocol_scalars() {
        let protocol = QueryOptions::parse(novarocks::QueryOptions {
            batch_size: 4096,
            query_timeout: 60,
            query_delivery_timeout: 30,
            enable_profile: true,
            runtime_profile_report_interval: 7,
            pipeline_dop: 8,
            query_mem_limit: 1 << 20,
            connector_io_tasks_per_scan_operator: 12,
            runtime_filter_scan_wait_time_ms: Some(250),
            runtime_filter_wait_timeout_ms: Some(5000),
            allow_throw_exception: true,
            group_concat_max_len: Some(65_535),
            enable_join_runtime_bitset_filter: Some(false),
            global_runtime_filter_build_max_size: 1 << 19,
            orc_use_column_names: true,
            enable_file_metacache: true,
            enable_file_pagecache: true,
            enable_parquet_reader_page_index: true,
            enable_scan_datacache: true,
            enable_populate_datacache: true,
            enable_datacache_async_populate_mode: true,
            enable_datacache_io_adaptor: true,
            enable_cache_select: true,
            datacache_evict_probability: Some(75),
            datacache_priority: 2,
            datacache_ttl_seconds: 3600,
            datacache_sharing_work_period: 10,
            enable_spill: true,
            spill_options: Some(novarocks::SpillOptions {
                spill_mode: 1,
                spill_mem_limit_threshold: 0.75,
                spill_operator_min_bytes: 64,
                spill_operator_max_bytes: 128,
                spill_encode_level: 3,
                enable_spill_buffer_read: true,
                max_spill_read_buffer_bytes_per_driver: 256,
                spill_mem_table_size: 512,
                spill_mem_table_num: 4,
            }),
        })
        .expect("valid query options");

        let runtime = reconstruct_runtime_query_options(&protocol);

        assert_eq!(runtime.batch_size, Some(4096));
        assert_eq!(runtime.query_timeout, Some(60));
        assert_eq!(runtime.query_delivery_timeout, Some(30));
        assert!(runtime.enable_profile);
        assert_eq!(runtime.runtime_profile_report_interval, Some(7));
        assert_eq!(runtime.pipeline_dop, Some(8));
        assert_eq!(runtime.exec_mem_limit, Some(1 << 20));
        assert_eq!(runtime.connector_io_tasks_per_scan_operator, Some(12));
        assert_eq!(runtime.runtime_filter_scan_wait_time_ms, Some(250));
        assert_eq!(runtime.runtime_filter_wait_timeout_ms, Some(5000));
        assert!(runtime.allow_throw_exception);
        assert_eq!(runtime.group_concat_max_len, Some(65_535));
        assert_eq!(runtime.enable_join_runtime_bitset_filter, Some(false));
        assert_eq!(runtime.global_runtime_filter_build_max_size, Some(1 << 19));
        assert!(runtime.orc_use_column_names);
        assert!(runtime.enable_file_metacache);
        assert!(runtime.enable_file_pagecache);
        assert!(runtime.enable_parquet_reader_page_index);
        assert_eq!(runtime.cache.datacache_evict_probability, Some(75));
        assert_eq!(runtime.cache.datacache_priority, Some(2));
        assert_eq!(runtime.cache.datacache_ttl_seconds, Some(3600));
        assert_eq!(runtime.cache.datacache_sharing_work_period, Some(10));
        let spill = runtime.spill.expect("enabled spill is reconstructed");
        assert!(spill.enable_spill);
        assert_eq!(spill.spill_mem_limit_threshold, Some(0.75));
        assert_eq!(spill.spill_operator_min_bytes, Some(64));
        assert_eq!(spill.spill_operator_max_bytes, Some(128));
        assert_eq!(spill.spill_encode_level, Some(3));
        assert_eq!(spill.max_spill_read_buffer_bytes_per_driver, Some(256));
        assert_eq!(spill.spill_mem_table_size, Some(512));
        assert_eq!(spill.spill_mem_table_num, Some(4));
    }
}
