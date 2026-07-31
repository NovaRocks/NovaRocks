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

//! Typed Core-to-Frontend application port for unified statistics commands.
//!
//! This module deliberately contains no parser AST or SQL string. Core turns
//! its parser variants into these commands exactly once; the frontend owns
//! durable job state and implements this port without raw-SQL interception.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorControlRegistry, ConnectorMutationOperationId,
    ConnectorRequestContext, ConnectorStatisticsResolver, ConnectorTableHandle,
    ConnectorTableResolution, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_STATISTICS_METRICS,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, StatisticsCollectionRequest, StatisticsDataVersion,
    StatisticsMetric, StatisticsMetricRequest, StatisticsMetricState, StatisticsMetricValue,
    StatisticsPublishPreparationRequest, StatisticsPublishRequest, StatisticsReadRequest,
    StatisticsReconcileRequest,
};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

/// Portable immutable table/version pin that the frontend may persist in a
/// durable ANALYZE job. It contains opaque provider bytes only—never a reader,
/// scan artifact, sketch, or executable runtime object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTablePin {
    pub connector_instance_id: String,
    pub table_handle: Vec<u8>,
    pub data_version: Vec<u8>,
    /// Resolved base-table column names from the same metadata snapshot as
    /// the handle/data-version pair. Empty `ANALYZE TABLE` uses this list;
    /// the worker must never load latest schema merely to expand `*`.
    pub columns: Vec<String>,
}

/// Core resolves a logical ANALYZE target exactly once. The frontend invokes
/// this before creating a job and persists the returned pin; background work
/// has no latest-name resolution capability.
pub trait StatisticsTargetResolver: Send + Sync {
    fn resolve_table_pin(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<StatisticsTablePin, StatisticsApplicationError>;
}

/// Frontend composition sink installed before engine open. Core calls it once
/// after connector control is ready, so ANALYZE submission can resolve and
/// persist a pin without giving the durable worker a resolver.
pub trait StatisticsTargetResolverSink: Send + Sync {
    fn bind_statistics_target_resolver(
        &self,
        resolver: Arc<dyn StatisticsTargetResolver>,
    ) -> Result<(), String>;
}

/// Read-only Core table-statistics surface. Unlike ANALYZE submission, it is
/// intentionally available without a StateStore and resolves its latest table
/// metadata only for this one short-lived read.
pub trait StatisticsTableReader: Send + Sync {
    fn show_table_stats(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<Vec<StatisticsTableStatView>, StatisticsApplicationError>;
}

/// Frontend composition sink installed alongside the target resolver. The
/// frontend adapts this typed result for the SQL application port; it never
/// receives a raw SQL string or an optimizer/provider handle.
pub trait StatisticsTableReaderSink: Send + Sync {
    fn bind_statistics_table_reader(
        &self,
        reader: Arc<dyn StatisticsTableReader>,
    ) -> Result<(), String>;
}

/// Durable worker input expressed without any frontend repository type.  The
/// table/version pin was fixed at ANALYZE submission; Core must not turn this
/// back into a name lookup when an attempt eventually runs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsAttemptRequest {
    pub operation_id: Uuid,
    pub table_pin: StatisticsTablePin,
    pub metric_names: Vec<String>,
}

/// Attempt-local material retained only by the execution/publish boundary.
/// It is deliberately opaque to the durable frontend: StateStore persists
/// just the separately prepared reconciliation evidence.
pub trait StatisticsCollectedAttempt: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

/// Core implementation of provider-native collection and publication.  The
/// frontend owns the job state machine and lease fence; Core owns connector
/// leases, the distributed request, and `ExternalMutationOutcome` handling.
pub trait StatisticsAttemptExecutor: Send + Sync {
    fn collect(
        &self,
        request: &StatisticsAttemptRequest,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsApplicationError>;

    fn prepare_publish(
        &self,
        request: &StatisticsAttemptRequest,
        collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsApplicationError>;

    fn publish(
        &self,
        request: &StatisticsAttemptRequest,
        collected: &dyn StatisticsCollectedAttempt,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsApplicationError>;

    fn reconcile(
        &self,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsApplicationError>;
}

/// Composition sink used after Core has installed connector control and the
/// native coordinator.  A frontend with no StateStore may bind the read-only
/// surfaces above but must decline this executor rather than construct an
/// in-memory job table.
pub trait StatisticsAttemptExecutorSink: Send + Sync {
    fn bind_statistics_attempt_executor(
        &self,
        executor: Arc<dyn StatisticsAttemptExecutor>,
    ) -> Result<(), String>;
}

pub struct ConnectorStatisticsTargetResolver {
    controls: Arc<dyn ConnectorControlRegistry>,
}

impl ConnectorStatisticsTargetResolver {
    pub fn new(controls: Arc<dyn ConnectorControlRegistry>) -> Self {
        Self { controls }
    }
}

impl StatisticsTargetResolver for ConnectorStatisticsTargetResolver {
    fn resolve_table_pin(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<StatisticsTablePin, StatisticsApplicationError> {
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let (resolved, _) = crate::connector::metadata_load_table(
            self.controls.as_ref(),
            context,
            &target.catalog,
            &target.namespace,
            &target.table,
            ConnectorTableResolution::StrictBaseTable,
        )
        .map_err(StatisticsApplicationError::new)?;
        let pin = resolved.statistics_pin.ok_or_else(|| {
            StatisticsApplicationError::new(
                "connector metadata did not provide a statistics data-version pin",
            )
        })?;
        Ok(StatisticsTablePin {
            connector_instance_id: pin.table.owner().as_str().to_string(),
            table_handle: pin.table.payload().to_vec(),
            data_version: pin.data_version.as_bytes().to_vec(),
            columns: resolved
                .columns
                .into_iter()
                .map(|column| column.name)
                .collect(),
        })
    }
}

pub struct ConnectorStatisticsTableReader {
    controls: Arc<dyn ConnectorControlRegistry>,
}

impl ConnectorStatisticsTableReader {
    pub fn new(controls: Arc<dyn ConnectorControlRegistry>) -> Self {
        Self { controls }
    }
}

impl StatisticsTableReader for ConnectorStatisticsTableReader {
    fn show_table_stats(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<Vec<StatisticsTableStatView>, StatisticsApplicationError> {
        let context = statistics_request_context()?;
        let (resolved, _) = crate::connector::metadata_load_table(
            self.controls.as_ref(),
            context.clone(),
            &target.catalog,
            &target.namespace,
            &target.table,
            ConnectorTableResolution::StrictBaseTable,
        )
        .map_err(StatisticsApplicationError::new)?;
        let pin = resolved.statistics_pin.ok_or_else(|| {
            StatisticsApplicationError::new(
                "connector metadata did not provide a statistics data-version pin",
            )
        })?;
        let requested_metric_count =
            1usize.saturating_add(resolved.columns.len().saturating_mul(5));
        if requested_metric_count > MAX_CONNECTOR_STATISTICS_METRICS {
            return Err(StatisticsApplicationError::new(format!(
                "SHOW TABLE STATS requires {requested_metric_count} metrics, exceeding the connector statistics limit of {MAX_CONNECTOR_STATISTICS_METRICS}",
            )));
        }
        let mut metrics = Vec::with_capacity(requested_metric_count);
        metrics.push(StatisticsMetric::RowCount);
        for column in &resolved.columns {
            let name: Arc<str> = Arc::from(column.name.as_str());
            metrics.extend([
                StatisticsMetric::NullCount {
                    column: Arc::clone(&name),
                },
                StatisticsMetric::Minimum {
                    column: Arc::clone(&name),
                },
                StatisticsMetric::Maximum {
                    column: Arc::clone(&name),
                },
                StatisticsMetric::AverageSize {
                    column: Arc::clone(&name),
                },
                StatisticsMetric::ThetaNdv { column: name },
            ]);
        }
        let metrics = StatisticsMetricRequest::try_new(metrics)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let lease = self
            .controls
            .acquire_current_statistics(pin.table.owner())
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let evidence = lease
            .read(StatisticsReadRequest {
                table: pin.table,
                data_version: pin.data_version,
                metrics,
                context,
            })
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        Ok(evidence
            .metrics
            .into_iter()
            .map(|(metric, state)| statistics_table_stat_view(metric, state))
            .collect())
    }
}

fn statistics_request_context() -> Result<ConnectorRequestContext, StatisticsApplicationError> {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        Arc::new(NeverCancelled),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| StatisticsApplicationError::new(error.to_string()))
}

fn statistics_table_stat_view(
    metric: StatisticsMetric,
    state: StatisticsMetricState,
) -> StatisticsTableStatView {
    let metric = match metric {
        StatisticsMetric::RowCount => "row_count".to_string(),
        StatisticsMetric::NullCount { column } => format!("null_count:{column}"),
        StatisticsMetric::Minimum { column } => format!("minimum:{column}"),
        StatisticsMetric::Maximum { column } => format!("maximum:{column}"),
        StatisticsMetric::AverageSize { column } => format!("average_size:{column}"),
        StatisticsMetric::ThetaNdv { column } => format!("theta_ndv:{column}"),
    };
    let (value, status) = match state {
        StatisticsMetricState::Available(value) => {
            (Some(statistics_metric_value(value)), "AVAILABLE".into())
        }
        StatisticsMetricState::Missing(missing) => (None, format!("MISSING:{:?}", missing.kind)),
        StatisticsMetricState::Error(error) => (None, format!("ERROR:{:?}", error.kind)),
    };
    StatisticsTableStatView {
        metric,
        value,
        status,
    }
}

fn statistics_metric_value(value: StatisticsMetricValue) -> String {
    match value {
        StatisticsMetricValue::U64(value) => value.to_string(),
        StatisticsMetricValue::I64(value) => value.to_string(),
        StatisticsMetricValue::F64(value) => value.to_string(),
        // Do not surface opaque connector bytes through SQL. Providers that
        // choose a byte metric must publish a user-safe scalar representation.
        StatisticsMetricValue::Bytes(_) => "<opaque>".to_string(),
    }
}

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsApplicationCommand {
    AnalyzeTable {
        target: StatisticsTableTarget,
        columns: Vec<String>,
    },
    ShowAnalyzeJobs,
    CancelAnalyze {
        job_id: Uuid,
    },
    ShowTableStats {
        target: StatisticsTableTarget,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobView {
    pub job_id: Uuid,
    pub operation_id: Uuid,
    pub state: String,
    pub attempt: u32,
    pub target: StatisticsTableTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableStatView {
    pub metric: String,
    pub value: Option<String>,
    pub status: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsApplicationResult {
    JobSubmitted(StatisticsJobView),
    JobCancellationRequested(StatisticsJobView),
    AnalyzeJobs(Vec<StatisticsJobView>),
    TableStats(Vec<StatisticsTableStatView>),
}

pub trait StatisticsApplicationPort: Send + Sync {
    fn execute(
        &self,
        command: StatisticsApplicationCommand,
    ) -> Result<StatisticsApplicationResult, StatisticsApplicationError>;
}

/// Non-frontend composition must not gain an in-memory statistics authority.
/// It fails closed until a frontend explicitly installs the durable port.
pub struct UnavailableStatisticsApplicationPort;

impl StatisticsApplicationPort for UnavailableStatisticsApplicationPort {
    fn execute(
        &self,
        _command: StatisticsApplicationCommand,
    ) -> Result<StatisticsApplicationResult, StatisticsApplicationError> {
        Err(StatisticsApplicationError::new(
            "unified statistics application service is not installed",
        ))
    }
}

/// Engine-owned implementation of a durable attempt. The frontend can retain
/// only this trait object and its opaque collected value; it cannot obtain a
/// connector reader, catalog handle, or an unpinned metadata path.
pub struct ConnectorStatisticsAttemptExecutor {
    state: std::sync::Weak<super::StandaloneState>,
}

impl ConnectorStatisticsAttemptExecutor {
    pub(crate) fn new(state: std::sync::Weak<super::StandaloneState>) -> Self {
        Self { state }
    }

    fn state(&self) -> Result<Arc<super::StandaloneState>, StatisticsApplicationError> {
        self.state.upgrade().ok_or_else(|| {
            StatisticsApplicationError::transient(
                "statistics engine is shutting down before the durable attempt completed",
            )
        })
    }

    fn collection_context() -> Result<ConnectorRequestContext, StatisticsApplicationError> {
        ConnectorRequestContext::try_new(
            Instant::now() + crate::query_execution::statistics::MAX_STATISTICS_ATTEMPT_DURATION,
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn table_and_version(
        request: &StatisticsAttemptRequest,
    ) -> Result<(ConnectorTableHandle, StatisticsDataVersion), StatisticsApplicationError> {
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(
            &request.table_pin.connector_instance_id,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let table = ConnectorTableHandle::try_new(
            instance_id,
            Bytes::copy_from_slice(&request.table_pin.table_handle),
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let version =
            StatisticsDataVersion::try_new(Bytes::copy_from_slice(&request.table_pin.data_version))
                .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        Ok((table, version))
    }

    fn metrics(
        request: &StatisticsAttemptRequest,
    ) -> Result<StatisticsMetricRequest, StatisticsApplicationError> {
        let mut metrics = vec![StatisticsMetric::RowCount];
        let columns = if request.metric_names.is_empty() {
            &request.table_pin.columns
        } else {
            &request.metric_names
        };
        for column in columns {
            let column: Arc<str> = Arc::from(column.as_str());
            metrics.extend([
                StatisticsMetric::NullCount {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::Minimum {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::Maximum {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::AverageSize {
                    column: Arc::clone(&column),
                },
                StatisticsMetric::ThetaNdv { column },
            ]);
        }
        StatisticsMetricRequest::try_new(metrics)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn operation_id(request: &StatisticsAttemptRequest) -> ConnectorMutationOperationId {
        ConnectorMutationOperationId::from_bytes(*request.operation_id.as_bytes())
    }

    fn collected<'a>(
        collected: &'a dyn StatisticsCollectedAttempt,
    ) -> Result<&'a ConnectorStatisticsCollectedAttempt, StatisticsApplicationError> {
        collected
            .as_any()
            .downcast_ref::<ConnectorStatisticsCollectedAttempt>()
            .ok_or_else(|| {
                StatisticsApplicationError::new(
                    "statistics publication received a collection artifact from another executor",
                )
            })
    }

    fn outcome(
        outcome: ExternalMutationOutcome<novarocks_spi::connector::StatisticsReceipt>,
    ) -> Result<(), StatisticsApplicationError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Complete,
                ..
            } => Ok(()),
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Failed(failure),
                ..
            }
            | ExternalMutationOutcome::KnownUncommitted { failure } => {
                Err(StatisticsApplicationError::new(failure.to_string()))
            }
            ExternalMutationOutcome::CommitUnknown { failure, .. } => {
                Err(StatisticsApplicationError::reconcile(failure.to_string()))
            }
        }
    }
}

struct ConnectorStatisticsCollectedAttempt {
    lease: novarocks_spi::connector::ConnectorStatisticsLease,
    table: ConnectorTableHandle,
    result: novarocks_spi::connector::StatisticsCollectionResult,
    context: ConnectorRequestContext,
}

impl StatisticsCollectedAttempt for ConnectorStatisticsCollectedAttempt {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatisticsAttemptExecutor for ConnectorStatisticsAttemptExecutor {
    fn collect(
        &self,
        request: &StatisticsAttemptRequest,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsApplicationError> {
        let state = self.state()?;
        let (table, data_version) = Self::table_and_version(request)?;
        let metrics = Self::metrics(request)?;
        let context = Self::collection_context()?;
        let lease = state
            .connector_control
            .acquire_current_statistics(table.owner())
            .map_err(|error| StatisticsApplicationError::transient(error.to_string()))?;
        let plan = lease
            .prepare_collection(StatisticsCollectionRequest {
                operation_id: Self::operation_id(request),
                table: table.clone(),
                data_version,
                metrics,
                context: context.clone(),
            })
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let program = crate::query_execution::statistics::StatisticsCollectionProgram::try_new(
            plan,
            crate::query_execution::statistics::StatisticsExecutionPolicy::try_new(
                crate::query_execution::statistics::StatisticsExecutionMode::DurableJobAttempt,
                crate::query_execution::statistics::MAX_STATISTICS_ATTEMPT_DURATION,
            )
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let topology = state
            .backend_topology
            .snapshot()
            .map_err(|error| StatisticsApplicationError::transient(error.to_string()))?;
        let cancellation = crate::query_execution::cancellation::QueryCancellationSource::new();
        let execution = crate::query_execution::request_context::QueryExecutionContext::new(
            state.execution_role,
            topology,
            Some(Instant::now() + program.policy().attempt_timeout()),
            cancellation.view(),
            crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        );
        let connectors = state
            .connectors
            .read()
            .map_err(|_| StatisticsApplicationError::transient("connector registry lock poisoned"))?
            .clone();
        let distributed = crate::query_execution::statistics::build_statistics_collection_request(
            &connectors,
            state.connector_control.as_ref(),
            &execution,
            context.clone(),
            program,
            Some(&lease),
        )
        .map_err(|error| StatisticsApplicationError::transient(error.to_string()))?;
        let result = state
            .query_execution
            .execute(distributed)
            .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_statistics)
            .map(|outcome| outcome.into_collection_result())
            .map_err(|error| StatisticsApplicationError::transient(error.to_string()))?;
        Ok(Box::new(ConnectorStatisticsCollectedAttempt {
            lease,
            table,
            result,
            context,
        }))
    }

    fn prepare_publish(
        &self,
        request: &StatisticsAttemptRequest,
        collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsApplicationError> {
        let collected = Self::collected(collected)?;
        collected
            .lease
            .prepare_publish(StatisticsPublishPreparationRequest {
                operation_id: Self::operation_id(request),
                table: collected.table.clone(),
                result: collected.result.clone(),
                context: collected.context.clone(),
            })
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))
    }

    fn publish(
        &self,
        request: &StatisticsAttemptRequest,
        collected: &dyn StatisticsCollectedAttempt,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsApplicationError> {
        let collected = Self::collected(collected)?;
        Self::outcome(
            collected
                .lease
                .publish(StatisticsPublishRequest {
                    operation_id: Self::operation_id(request),
                    table: collected.table.clone(),
                    result: collected.result.clone(),
                    context: collected.context.clone(),
                    evidence: evidence.clone(),
                })
                .map_err(|error| StatisticsApplicationError::new(error.to_string()))?,
        )
    }

    fn reconcile(
        &self,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsApplicationError> {
        let state = self.state()?;
        let lease = state
            .connector_control
            .acquire_current_statistics(&evidence.descriptor().instance_id)
            .map_err(|error| StatisticsApplicationError::reconcile(error.to_string()))?;
        Self::outcome(
            lease
                .reconcile(StatisticsReconcileRequest {
                    evidence: evidence.clone(),
                    context: Self::collection_context()?,
                })
                .map_err(|error| StatisticsApplicationError::reconcile(error.to_string()))?,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsApplicationError {
    message: String,
    retryable: bool,
    requires_reconcile: bool,
}

impl StatisticsApplicationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            requires_reconcile: false,
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
            requires_reconcile: false,
        }
    }

    pub fn reconcile(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            requires_reconcile: true,
        }
    }

    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    pub const fn requires_reconcile(&self) -> bool {
        self.requires_reconcile
    }
}

impl fmt::Display for StatisticsApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatisticsApplicationError {}
