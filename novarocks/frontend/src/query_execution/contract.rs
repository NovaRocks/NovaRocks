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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::common::query_cancellation::QueryCancellationView;
use crate::query_execution::artifact::PreparedDistributedQuery;
use crate::query_execution::native_fragment::NativeFragmentAttachment;
pub use crate::query_execution::outcome::DistributedQueryOutcome;
pub use crate::query_execution::outcome::FragmentProfileSet;
pub use crate::query_execution::outcome::QueryOutcomeFactory;
use crate::query_execution::preparation::PreparedFragmentSet;
pub use crate::query_execution::profile::ProfileTerminalBuilder;
pub use crate::query_execution::statistics::StatisticsCollectionProgram;
pub use crate::query_execution::statistics::StatisticsExecutionMode;
pub use crate::query_execution::statistics::StatisticsExecutionPolicy;
use novarocks_execution::exec::spill::{SpillConfig, SpillMode};
use novarocks_execution::runtime::query_options::{
    QueryCacheOptions, QueryOptions as RuntimeQueryOptions,
};
use novarocks_proto::lifecycle::QueryOptions;
use novarocks_proto::provider::EnsureConnectorExecutionBindingRejection;
use novarocks_spi::connector::{
    ConnectorActivatedWriteCohort, ConnectorError, ConnectorExecutionBindingKey,
    ConnectorRequestContext, ConnectorWriteActivationIntent, ConnectorWriteActivationRequest,
    ConnectorWriteActivationSource, ConnectorWriteCohortId, ConnectorWriteExecutionId,
    ConnectorWriteLease, ConnectorWriteOperationId, ConnectorWritePlanningRequest,
    ConnectorWritePreparation,
};

use crate::query_execution::write_operation::ConnectorWriteOperationSession;
use novarocks_sql::plan_read::FragmentId;
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

/// DML-owned facts required to plan one provider-neutral writer manifest
/// after frontend placement.  The request deliberately excludes execution ID
/// and expected writers: both are derived from the immutable placement and
/// cannot be supplied by a pre-scheduling caller.
#[derive(Clone)]
pub struct ConnectorWritePlanningTemplate {
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    activation: ConnectorActivatedWriteCohort,
    context: ConnectorRequestContext,
    // This lease is derived from the planning generation that accepted the
    // preparation.  Keeping it with the inert planning template prevents an
    // execution handoff from replacing it with a current-generation lookup.
    lease: ConnectorWriteLease,
}

impl ConnectorWritePlanningTemplate {
    pub fn from_activated_cohort(
        activation: ConnectorActivatedWriteCohort,
        context: ConnectorRequestContext,
        lease: ConnectorWriteLease,
    ) -> Result<Self, ConnectorError> {
        activation.validate()?;
        if activation.owner() != lease.binding_key() {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "activated connector write cohort does not match its exact write lease",
            ));
        }
        Ok(Self::new(
            activation.operation_id(),
            activation,
            context,
            lease,
        ))
    }

    /// Activate an ordinary Provider-signed preparation on its retained exact
    /// lease and construct the only legal planning template for it.
    pub fn activate_prepared(
        operation_id: ConnectorWriteOperationId,
        preparation: ConnectorWritePreparation,
        context: ConnectorRequestContext,
        lease: ConnectorWriteLease,
    ) -> Result<Self, ConnectorError> {
        Self::activate_prepared_with_intent(
            operation_id,
            preparation,
            ConnectorWriteActivationIntent::Ordinary,
            context,
            lease,
        )
    }

    /// Activate a prepared write with an application-owned, provider-neutral
    /// intent.  Only the exact lease may turn the preparation into planning
    /// authority; the intent is bound into the activation digest at that
    /// transition.
    pub fn activate_prepared_with_intent(
        operation_id: ConnectorWriteOperationId,
        preparation: ConnectorWritePreparation,
        intent: ConnectorWriteActivationIntent,
        context: ConnectorRequestContext,
        lease: ConnectorWriteLease,
    ) -> Result<Self, ConnectorError> {
        let activation = lease.activate_write(ConnectorWriteActivationRequest {
            operation_id,
            source: ConnectorWriteActivationSource::Prepared(preparation),
            intent,
            context: context.clone(),
        })?;
        let cohort = activation
            .cohort(ConnectorWriteCohortId::primary(operation_id))
            .ok_or_else(|| {
                ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                    "ordinary connector write activation omitted its primary cohort",
                )
            })?;
        Ok(Self::new(operation_id, cohort, context, lease))
    }
    pub fn new(
        operation_id: ConnectorWriteOperationId,
        activation: ConnectorActivatedWriteCohort,
        context: ConnectorRequestContext,
        lease: ConnectorWriteLease,
    ) -> Self {
        Self::new_in_cohort(
            operation_id,
            activation.cohort_id(),
            activation,
            context,
            lease,
        )
    }

    pub fn new_in_cohort(
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        activation: ConnectorActivatedWriteCohort,
        context: ConnectorRequestContext,
        lease: ConnectorWriteLease,
    ) -> Self {
        Self {
            operation_id,
            cohort_id,
            activation,
            context,
            lease,
        }
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub fn connector_instance_id(&self) -> &novarocks_spi::connector::ConnectorInstanceId {
        self.activation.preparation().table().owner()
    }

    pub fn preparation(&self) -> &ConnectorWritePreparation {
        self.activation.preparation()
    }

    pub fn request_context(&self) -> &ConnectorRequestContext {
        &self.context
    }

    pub fn intent(&self) -> novarocks_spi::connector::ConnectorWriteIntent {
        self.activation.preparation().intent()
    }

    pub fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }

    /// The exact write lease derived by the planning generation that signed
    /// this preparation.  A caller must retain this capability through bind;
    /// it must never reacquire a current generation.
    pub fn lease(&self) -> ConnectorWriteLease {
        self.lease.clone()
    }

    pub fn retains_lease_generation(&self, lease: &ConnectorWriteLease) -> bool {
        self.lease.retains_same_generation(lease)
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
            activation: self.activation,
            expected_writers: Vec::new(),
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
    owner: ConnectorExecutionBindingKey,
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
        let owner = first.preparation().owner().clone();
        let lease = first.lease();
        let context = first.request_context();
        let mut cohort_ids = std::collections::BTreeSet::new();
        for cohort in &cohorts {
            let candidate_context = cohort.request_context();
            if cohort.operation_id() != operation_id
                || cohort.preparation().owner() != &owner
                || !cohort.retains_lease_generation(&lease)
                || !cohort_ids.insert(cohort.cohort_id())
            {
                return Err(DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    "connector write operation registration contains a foreign or duplicate cohort",
                ));
            }
            if candidate_context.deadline() != context.deadline()
                || candidate_context.max_handle_payload_bytes()
                    != context.max_handle_payload_bytes()
                || candidate_context.max_total_payload_bytes() != context.max_total_payload_bytes()
                || !Arc::ptr_eq(candidate_context.cancellation(), context.cancellation())
            {
                return Err(DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    "connector write operation registration contains inconsistent request contexts",
                ));
            }
        }
        Ok(Self {
            operation_id,
            owner,
            cohorts,
        })
    }

    pub fn single(cohort: ConnectorWritePlanningTemplate) -> Self {
        Self::try_new(vec![cohort]).expect("one connector write cohort is a valid registration")
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub fn sealed_cohorts(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorSealedWriteCohortSet, ConnectorError> {
        let descriptors = self
            .cohorts
            .iter()
            .map(|template| {
                Ok(
                    novarocks_spi::connector::ConnectorWriteCohortDescriptor::new(
                        template.cohort_id(),
                        template.intent(),
                        template.stable_digest(&self.owner)?,
                    ),
                )
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        novarocks_spi::connector::ConnectorSealedWriteCohortSet::try_new(
            self.operation_id,
            descriptors,
        )
    }

    pub fn into_cohorts(self) -> Vec<ConnectorWritePlanningTemplate> {
        self.cohorts
    }
}

/// Exact operation-scoped routing from terminal writer fragments to cohorts.
#[derive(Clone)]
pub struct ConnectorWriteExecutionRegistration {
    session: ConnectorWriteOperationSession,
    routing: ConnectorWriteExecutionRouting,
}

#[derive(Clone)]
enum ConnectorWriteExecutionRouting {
    Single(ConnectorWriteCohortId),
    ByWriter(BTreeMap<FragmentId, ConnectorWriteCohortId>),
}

impl ConnectorWriteExecutionRegistration {
    /// Register a genuinely single-cohort distributed execution.
    ///
    /// The scheduling artifact will bind every terminal writer fragment to
    /// this cohort. Multi-cohort executions must use
    /// [`Self::try_new_with_writer_fragment_cohorts`] instead.
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
        Ok(Self {
            session,
            routing: ConnectorWriteExecutionRouting::Single(cohort_id),
        })
    }

    pub fn try_new_with_writer_fragment_cohorts<I>(
        session: ConnectorWriteOperationSession,
        writer_fragment_cohorts: I,
    ) -> Result<Self, DistributedQueryError>
    where
        I: IntoIterator<Item = (FragmentId, ConnectorWriteCohortId)>,
    {
        let mut canonical = BTreeMap::new();
        for (fragment_id, cohort_id) in writer_fragment_cohorts {
            if canonical.insert(fragment_id, cohort_id).is_some() {
                return Err(DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    "connector write execution contains a duplicate writer fragment",
                ));
            }
        }
        if canonical.is_empty() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "connector write execution has no terminal writer fragments",
            ));
        }
        if canonical
            .values()
            .any(|cohort_id| !session.contains_cohort(*cohort_id))
        {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "connector write execution mapping references a cohort outside the sealed operation",
            ));
        }
        Ok(Self {
            session,
            routing: ConnectorWriteExecutionRouting::ByWriter(canonical),
        })
    }

    pub fn single<I>(
        session: ConnectorWriteOperationSession,
        writer_fragment_ids: I,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<Self, DistributedQueryError>
    where
        I: IntoIterator<Item = FragmentId>,
    {
        Self::try_new_with_writer_fragment_cohorts(
            session,
            writer_fragment_ids
                .into_iter()
                .map(|fragment_id| (fragment_id, cohort_id)),
        )
    }

    pub fn session(&self) -> &ConnectorWriteOperationSession {
        &self.session
    }

    pub fn writer_fragment_cohorts(&self) -> Option<&BTreeMap<FragmentId, ConnectorWriteCohortId>> {
        match &self.routing {
            ConnectorWriteExecutionRouting::Single(_) => None,
            ConnectorWriteExecutionRouting::ByWriter(routing) => Some(routing),
        }
    }

    pub fn cohort_id_for_writer_fragment(
        &self,
        fragment_id: FragmentId,
    ) -> Option<ConnectorWriteCohortId> {
        match &self.routing {
            ConnectorWriteExecutionRouting::Single(cohort_id) => Some(*cohort_id),
            ConnectorWriteExecutionRouting::ByWriter(routing) => routing.get(&fragment_id).copied(),
        }
    }

    pub fn single_cohort_id(&self) -> Option<ConnectorWriteCohortId> {
        match &self.routing {
            ConnectorWriteExecutionRouting::Single(cohort_id) => Some(*cohort_id),
            ConnectorWriteExecutionRouting::ByWriter(routing) => {
                let mut cohorts = routing.values().copied();
                let cohort_id = cohorts.next()?;
                cohorts
                    .all(|candidate| candidate == cohort_id)
                    .then_some(cohort_id)
            }
        }
    }

    pub fn resolve_writer_fragment_cohorts<I>(
        &self,
        writer_fragment_ids: I,
    ) -> Result<BTreeMap<FragmentId, ConnectorWriteCohortId>, DistributedQueryError>
    where
        I: IntoIterator<Item = FragmentId>,
    {
        let writer_fragment_ids = writer_fragment_ids.into_iter().collect::<BTreeSet<_>>();
        if writer_fragment_ids.is_empty() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "connector write execution has no terminal writer fragments",
            ));
        }
        match &self.routing {
            ConnectorWriteExecutionRouting::Single(cohort_id) => Ok(writer_fragment_ids
                .into_iter()
                .map(|fragment_id| (fragment_id, *cohort_id))
                .collect()),
            ConnectorWriteExecutionRouting::ByWriter(routing) => {
                if routing.keys().copied().collect::<BTreeSet<_>>() != writer_fragment_ids {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "connector write execution mapping does not exactly cover terminal writer fragments",
                    ));
                }
                Ok(routing.clone())
            }
        }
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
    topology: crate::common::backend_topology::BackendTopologySnapshot,
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

    pub fn topology(&self) -> &crate::common::backend_topology::BackendTopologySnapshot {
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
    pub topology: crate::common::backend_topology::BackendTopologySnapshot,
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
    native_bundle: NativeFragmentAttachment,
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
    native_bundle: NativeFragmentAttachment,
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
    connector_binding_rejection: Option<EnsureConnectorExecutionBindingRejection>,
}

impl DistributedQueryError {
    pub fn new(kind: DistributedQueryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            connector_binding_rejection: None,
        }
    }

    /// Preserves a BE-provided, Protocol-validated binding rejection without
    /// reducing its reason or retry semantics to display text.
    pub fn connector_binding_rejected(
        context: impl Into<String>,
        rejection: EnsureConnectorExecutionBindingRejection,
    ) -> Self {
        let context = context.into();
        let field_path = rejection
            .safe_field_path()
            .map(|value| format!(" field_path={value}"))
            .unwrap_or_default();
        Self {
            kind: DistributedQueryErrorKind::Rejected,
            message: format!(
                "{context}: connector execution binding rejected: reason={:?} retryable_before_progress={} detail={}{}",
                rejection.reason(),
                rejection.retryable_before_progress(),
                rejection.safe_detail(),
                field_path,
            ),
            connector_binding_rejection: Some(rejection),
        }
    }

    pub fn kind(&self) -> DistributedQueryErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn connector_binding_rejection(&self) -> Option<&EnsureConnectorExecutionBindingRejection> {
        self.connector_binding_rejection.as_ref()
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
        _lease: ConnectorWriteLease,
    ) -> Result<ConnectorWriteOperationSession, DistributedQueryError> {
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::Rejected,
            "distributed query coordinator has no connector write operation service",
        ))
    }

    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError>;
}

#[cfg(test)]
mod tests {
    use super::reconstruct_runtime_query_options;
    use novarocks_proto::lifecycle::QueryOptions;
    use novarocks_proto::novarocks;

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
