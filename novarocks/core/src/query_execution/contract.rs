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
use std::time::Instant;

use crate::protocol::native::encode::NativeFragmentBundle;
use crate::query_execution::artifact::PreparedDistributedQuery;
use crate::query_execution::cancellation::QueryCancellationView;
pub use crate::query_execution::outcome::DistributedQueryOutcome;
pub use crate::query_execution::outcome::FragmentProfileSet;
pub use crate::query_execution::outcome::QueryOutcomeFactory;
use crate::query_execution::preparation::PreparedFragmentSet;
pub use crate::query_execution::profile::ProfileTerminalBuilder;
use crate::query_execution::request_context::QueryExecutionContext;
pub use crate::query_execution::statistics::StatisticsCollectionProgram;
pub use crate::query_execution::statistics::StatisticsExecutionMode;
pub use crate::query_execution::statistics::StatisticsExecutionPolicy;
use crate::runtime::query_options::QueryOptions;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorExecutionBindingKey, ConnectorRequestContext, ConnectorTableHandle,
    ConnectorWriteCohortId, ConnectorWriteExecutionId, ConnectorWriteIntent, ConnectorWriteLease,
    ConnectorWriteOperationId, ConnectorWritePlanningRequest,
};

use crate::query_execution::write_operation::ConnectorWriteOperationSession;
pub(crate) use novarocks_types::QueryId;

/// Query options resolved by core before ownership crosses into frontend.
///
/// The runtime representation stays private; frontend only receives stable
/// scalar views needed to schedule, submit, and time out native work.
pub struct ResolvedQueryOptions {
    runtime: QueryOptions,
}

impl ResolvedQueryOptions {
    pub(crate) fn from_upstream(options: Option<QueryOptions>) -> Self {
        let mut runtime = options.unwrap_or_default();
        let pipeline_dop =
            crate::runtime::exec_env::calc_pipeline_dop(runtime.pipeline_dop.unwrap_or_default());
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
            crate::runtime::query_options::query_expire_durations(Some(&self.runtime));
        RuntimeFilterLifecycleView {
            delivery_expire,
            query_expire,
        }
    }

    pub(crate) fn runtime_options(&self) -> &QueryOptions {
        &self.runtime
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

/// DML-owned facts required to plan one provider-neutral writer manifest
/// after frontend placement.  The request deliberately excludes execution ID
/// and expected writers: both are derived from the immutable placement and
/// cannot be supplied by a pre-scheduling caller.
#[derive(Clone)]
pub struct ConnectorWritePlanningTemplate {
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    table: ConnectorTableHandle,
    intent: ConnectorWriteIntent,
    input_schema: SchemaRef,
    provider_payload: Bytes,
    context: ConnectorRequestContext,
}

impl ConnectorWritePlanningTemplate {
    pub fn new(
        operation_id: ConnectorWriteOperationId,
        table: ConnectorTableHandle,
        intent: ConnectorWriteIntent,
        input_schema: SchemaRef,
        provider_payload: Bytes,
        context: ConnectorRequestContext,
    ) -> Self {
        Self::new_in_cohort(
            operation_id,
            ConnectorWriteCohortId::primary(operation_id),
            table,
            intent,
            input_schema,
            provider_payload,
            context,
        )
    }

    pub fn new_in_cohort(
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        table: ConnectorTableHandle,
        intent: ConnectorWriteIntent,
        input_schema: SchemaRef,
        provider_payload: Bytes,
        context: ConnectorRequestContext,
    ) -> Self {
        Self {
            operation_id,
            cohort_id,
            table,
            intent,
            input_schema,
            provider_payload,
            context,
        }
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub fn connector_instance_id(&self) -> &novarocks_spi::connector::ConnectorInstanceId {
        self.table.owner()
    }

    pub const fn intent(&self) -> ConnectorWriteIntent {
        self.intent
    }

    pub fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }

    pub fn stable_digest(
        &self,
        owner: &ConnectorExecutionBindingKey,
    ) -> Result<[u8; 32], ConnectorError> {
        self.clone()
            .into_request(ConnectorWriteExecutionId::new([0; 16], 0))
            .stable_digest(owner)
    }

    pub fn into_request(
        self,
        execution_id: ConnectorWriteExecutionId,
    ) -> ConnectorWritePlanningRequest {
        ConnectorWritePlanningRequest {
            operation_id: self.operation_id,
            cohort_id: self.cohort_id,
            execution_id,
            table: self.table,
            intent: self.intent,
            input_schema: self.input_schema,
            expected_writers: Vec::new(),
            provider_payload: self.provider_payload,
            context: self.context,
        }
    }
}

/// The complete provider-neutral cohort registration supplied before any
/// writer attempt may be planned.  Frontend consumes this value exactly once
/// to acquire one exact-generation lease and seal the immutable cohort set.
#[derive(Clone)]
pub struct ConnectorWriteOperationRegistration {
    operation_id: ConnectorWriteOperationId,
    connector_instance_id: novarocks_spi::connector::ConnectorInstanceId,
    cohorts: Vec<ConnectorWritePlanningTemplate>,
}

impl ConnectorWriteOperationRegistration {
    pub fn try_new(
        cohorts: Vec<ConnectorWritePlanningTemplate>,
    ) -> Result<Self, DistributedQueryError> {
        let first = cohorts.first().ok_or_else(|| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "connector write operation registration has no cohorts",
            )
        })?;
        let operation_id = first.operation_id();
        let connector_instance_id = first.connector_instance_id().clone();
        let mut cohort_ids = std::collections::BTreeSet::new();
        for cohort in &cohorts {
            if cohort.operation_id() != operation_id
                || cohort.connector_instance_id() != &connector_instance_id
                || !cohort_ids.insert(cohort.cohort_id())
            {
                return Err(DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    "connector write operation registration contains a foreign or duplicate cohort",
                ));
            }
        }
        Ok(Self {
            operation_id,
            connector_instance_id,
            cohorts,
        })
    }

    pub fn single(cohort: ConnectorWritePlanningTemplate) -> Self {
        Self::try_new(vec![cohort]).expect("one connector write cohort is a valid registration")
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub fn connector_instance_id(&self) -> &novarocks_spi::connector::ConnectorInstanceId {
        &self.connector_instance_id
    }

    pub fn into_cohorts(self) -> Vec<ConnectorWritePlanningTemplate> {
        self.cohorts
    }
}

/// One sealed cohort selected for a concrete distributed execution attempt.
#[derive(Clone)]
pub struct ConnectorWriteExecutionRegistration {
    session: ConnectorWriteOperationSession,
    cohort_id: ConnectorWriteCohortId,
}

impl ConnectorWriteExecutionRegistration {
    pub fn try_new(
        session: ConnectorWriteOperationSession,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<Self, DistributedQueryError> {
        if !session.contains_cohort(cohort_id) {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "connector write execution references a cohort outside the sealed operation",
            ));
        }
        Ok(Self { session, cohort_id })
    }

    pub fn session(&self) -> &ConnectorWriteOperationSession {
        &self.session
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }
}

/// An owned request passed from core to the injected execution coordinator.
///
/// Every field is private so role crates cannot assemble a request from
/// unrelated prepared/native artifacts or replace its cancellation/completion
/// capabilities.
pub struct DistributedQueryRequest {
    artifacts: PreparedDistributedQuery,
    options: ResolvedQueryOptions,
    topology: crate::query_execution::backend::BackendTopologySnapshot,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    completion: QueryOutcomeFactory,
    connector_write: Option<ConnectorWriteExecutionRegistration>,
    statistics_program: Option<StatisticsCollectionProgram>,
}

impl DistributedQueryRequest {
    pub fn intent(&self) -> DistributedQueryIntent {
        self.completion.intent()
    }

    pub fn artifacts(&self) -> &PreparedDistributedQuery {
        &self.artifacts
    }

    pub fn options(&self) -> &ResolvedQueryOptions {
        &self.options
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }

    pub fn topology(&self) -> &crate::query_execution::backend::BackendTopologySnapshot {
        &self.topology
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn connector_write(&self) -> Option<&ConnectorWriteExecutionRegistration> {
        self.connector_write.as_ref()
    }

    pub fn statistics_program(&self) -> Option<&StatisticsCollectionProgram> {
        self.statistics_program.as_ref()
    }

    pub fn into_parts(self) -> DistributedQueryRequestParts {
        DistributedQueryRequestParts {
            artifacts: self.artifacts,
            options: self.options,
            topology: self.topology,
            deadline: self.deadline,
            cancellation: self.cancellation,
            completion: self.completion,
            connector_write: self.connector_write,
            statistics_program: self.statistics_program,
        }
    }
}

/// Consuming frontend handoff. There is deliberately no constructor,
/// `Clone`, or inverse recombination API.
pub struct DistributedQueryRequestParts {
    pub artifacts: PreparedDistributedQuery,
    pub options: ResolvedQueryOptions,
    pub topology: crate::query_execution::backend::BackendTopologySnapshot,
    pub deadline: Option<Instant>,
    pub cancellation: QueryCancellationView,
    pub completion: QueryOutcomeFactory,
    pub connector_write: Option<ConnectorWriteExecutionRegistration>,
    pub statistics_program: Option<StatisticsCollectionProgram>,
}

/// Request construction accepts only the execution projection captured at
/// admission; callers cannot synthesize an empty topology or cancellation
/// fallback at the coordinator boundary.
pub(crate) fn build_distributed_query_request_with_execution(
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    options: Option<QueryOptions>,
    intent: DistributedQueryIntent,
    execution: &QueryExecutionContext,
) -> Result<DistributedQueryRequest, DistributedQueryError> {
    if intent == DistributedQueryIntent::Statistics {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "statistics execution requires a typed StatisticsCollectionProgram",
        ));
    }
    Ok(DistributedQueryRequest {
        artifacts: PreparedDistributedQuery::new(prepared, native_bundle),
        options: ResolvedQueryOptions::from_upstream(options),
        topology: execution.topology().clone(),
        deadline: execution.deadline(),
        cancellation: execution.cancellation().clone(),
        completion: QueryOutcomeFactory::new(intent),
        connector_write: None,
        statistics_program: None,
    })
}

/// Build a distributed request for internal statistics collection.  The
/// program is intentionally required here rather than carried in generic
/// query options, preventing a client-result request from acquiring a
/// statistics completion capability.
pub(crate) fn build_statistics_query_request_with_execution(
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    options: Option<QueryOptions>,
    program: StatisticsCollectionProgram,
    execution: &QueryExecutionContext,
) -> DistributedQueryRequest {
    DistributedQueryRequest {
        artifacts: PreparedDistributedQuery::new(prepared, native_bundle),
        options: ResolvedQueryOptions::from_upstream(options),
        topology: execution.topology().clone(),
        deadline: execution.deadline(),
        cancellation: execution.cancellation().clone(),
        completion: QueryOutcomeFactory::new(DistributedQueryIntent::Statistics),
        connector_write: None,
        statistics_program: Some(program),
    }
}

/// Attach a placement-deferred connector writer request to an otherwise
/// sealed distributed execution request. Only the core request builder can
/// invoke this: callers cannot recombine prepared/native artifacts.
pub(crate) fn with_connector_write_operation(
    mut request: DistributedQueryRequest,
    registration: ConnectorWriteExecutionRegistration,
) -> Result<DistributedQueryRequest, DistributedQueryError> {
    if request.intent() != DistributedQueryIntent::Write {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "connector write planning is only valid for distributed write requests",
        ));
    }
    if request.connector_write.is_some() {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "distributed query already has a connector write planning template",
        ));
    }
    request.connector_write = Some(registration);
    Ok(request)
}

/// Stable error categories exposed by the coordinator boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedQueryErrorKind {
    ContractViolation,
    Rejected,
    Failed,
}

/// A coordinator failure that core can surface without naming a coordinator
/// implementation or frontend state type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryError {
    kind: DistributedQueryErrorKind,
    message: String,
}

impl DistributedQueryError {
    pub fn new(kind: DistributedQueryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> DistributedQueryErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
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
    fn begin_write_operation(
        &self,
        _registration: ConnectorWriteOperationRegistration,
    ) -> Result<ConnectorWriteOperationSession, DistributedQueryError> {
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::Rejected,
            "distributed query coordinator has no connector write operation service",
        ))
    }

    /// Seal a distributed writer against a lease acquired by the application
    /// from the exact control generation used during planning.
    fn begin_write_operation_with_lease(
        &self,
        _registration: ConnectorWriteOperationRegistration,
        _lease: ConnectorWriteLease,
    ) -> Result<ConnectorWriteOperationSession, DistributedQueryError> {
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::Rejected,
            "distributed query coordinator does not accept caller-retained connector write leases",
        ))
    }

    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError>;
}
