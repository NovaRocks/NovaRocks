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

//! Typed frontend application contract for unified statistics commands.
//!
//! This module deliberately contains no parser AST or raw-SQL interception.
//! The frontend owns target resolution, current-process job state, and worker composition.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, ConnectorControlPlanningLease, ConnectorControlRegistry,
    ConnectorError, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorTableMetadata, ConnectorTableObjectBinding, ConnectorTableObjectBindingFailure,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectId, ConnectorTableObjectRebindRequest,
    ConnectorTableObjectSelector, ConnectorTableRequest, ConnectorTableResolution,
    LakePublicationId, MAX_CONNECTOR_STATISTICS_METRICS, StatisticsBasisRelation,
    StatisticsDataVersion, StatisticsMetric, StatisticsMetricRequest, StatisticsMetricSource,
    StatisticsMetricState, StatisticsMetricValue, StatisticsNumericNature, StatisticsReadRequest,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

/// Column-selection intent retained by a current-process ANALYZE job.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StatisticsColumnIntent {
    /// The attempt expands all SQL-visible columns from its freshly rebound schema.
    AllColumns,
    /// Explicit SQL column names supplied by the statement.
    Explicit(Vec<String>),
}

/// Submission-time observation of an ANALYZE target.
///
/// `sql_columns` is only for immediate statement validation. It must never be
/// persisted: an attempt derives its own column set after identity-gated rebind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTargetCapture {
    pub connector_instance_id: String,
    pub namespace: String,
    pub table: String,
    pub object_id: Vec<u8>,
    pub sql_columns: Vec<String>,
}

/// One current-process worker request. It carries only logical identity, physical
/// object identity, and collection intent; provider handles and versions are
/// strictly attempt-local.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsAttemptRequest {
    pub operation_id: LakePublicationId,
    pub connector_instance_id: String,
    pub namespace: String,
    pub table: String,
    pub object_id: Vec<u8>,
    pub columns: StatisticsColumnIntent,
}

/// Identity-gated current binding retained only within one attempt and its
/// planning lease lifetime. It must never be retained by the process runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsAttemptBinding {
    pub table: novarocks_spi::connector::ConnectorTableHandle,
    pub data_version: StatisticsDataVersion,
    pub sql_columns: Vec<arrow::datatypes::FieldRef>,
}

/// The frontend captures a logical ANALYZE target before job creation.
pub trait StatisticsTargetResolver: Send + Sync {
    fn capture_table_object(
        &self,
        target: &StatisticsTableTarget,
        context: ConnectorRequestContext,
    ) -> Result<StatisticsTargetCapture, StatisticsApplicationError>;
}

/// Frontend composition sink installed before engine open. Frontend composition calls it once
/// after connector control is ready, so ANALYZE submission can resolve and
/// persist a pin without giving the durable worker a resolver.
pub trait StatisticsTargetResolverSink: Send + Sync {
    fn bind_statistics_target_resolver(
        &self,
        resolver: Arc<dyn StatisticsTargetResolver>,
    ) -> Result<(), String>;
}

/// Read-only frontend table-statistics surface. Unlike ANALYZE submission, it is
/// intentionally short-lived and resolves its latest table
/// metadata only for this one short-lived read.
pub trait StatisticsTableReader: Send + Sync {
    fn show_table_stats(
        &self,
        target: &StatisticsTableTarget,
        context: ConnectorRequestContext,
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

/// Frontend-owned implementation of provider-native collection and
/// publication. One consuming call owns the session from provider begin through
/// ordinary distributed execution and the single external finish attempt.
pub trait StatisticsAttemptExecutor: Send + Sync {
    fn execute(
        &self,
        request: &StatisticsAttemptRequest,
        cancellation: crate::common::query_cancellation::QueryCancellationView,
    ) -> Result<(), StatisticsApplicationError>;
}

/// Composition sink used after the frontend has installed connector control and the
/// native coordinator.
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
    fn capture_table_object(
        &self,
        target: &StatisticsTableTarget,
        context: ConnectorRequestContext,
    ) -> Result<StatisticsTargetCapture, StatisticsApplicationError> {
        let instance_id = ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let lease = self
            .controls
            .acquire_current(&instance_id)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        if lease.binding().statistics().is_none() {
            return Err(StatisticsApplicationError::from_connector_error(
                ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                    "connector control generation has no statistics capability",
                ),
            ));
        }
        let captured = lease
            .binding()
            .metadata()
            .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
                table: ConnectorTableIdentity {
                    instance_id,
                    namespace: Arc::from(target.namespace.as_str()),
                    table: Arc::from(target.table.as_str()),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context,
            })
            .map_err(StatisticsApplicationError::from_connector_error)?;
        Ok(StatisticsTargetCapture {
            connector_instance_id: captured.metadata.table.owner().as_str().to_string(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            object_id: captured.object_id.as_bytes().to_vec(),
            sql_columns: sql_visible_column_names(&captured.metadata.schema),
        })
    }
}

/// Rebind a durable request through the caller's existing planning lease.
///
/// This keeps metadata rebinding and statistics preparation in one connector
/// generation, and the returned provider facts remain attempt-local.
pub(crate) fn rebind_table_object(
    lease: &ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    request: &StatisticsAttemptRequest,
) -> Result<StatisticsAttemptBinding, StatisticsApplicationError> {
    let instance_id = ConnectorInstanceId::parse(&request.connector_instance_id)
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
    let expected_object_id =
        ConnectorTableObjectId::try_new(Bytes::copy_from_slice(&request.object_id))
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
    let ConnectorTableObjectBinding { metadata, .. } = lease
        .binding()
        .metadata()
        .rebind_table_object_binding(ConnectorTableObjectRebindRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(request.namespace.as_str()),
                table: Arc::from(request.table.as_str()),
            },
            expected_object_id,
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context,
        })
        .map_err(StatisticsApplicationError::from_connector_error)?;
    let data_version = metadata.statistics_data_version.clone().ok_or_else(|| {
        StatisticsApplicationError::new(
            "connector metadata did not provide a statistics data-version for rebound table",
        )
    })?;
    Ok(StatisticsAttemptBinding {
        table: metadata.table,
        data_version,
        sql_columns: sql_visible_columns(&metadata.schema),
    })
}

/// The SQL-visible fields, carrying their types.
///
/// A caller that only validates names takes them off the field. A caller that
/// builds a statistics metric request needs the type: which metrics a column
/// can answer depends on it.
fn sql_visible_columns(schema: &arrow::datatypes::SchemaRef) -> Vec<arrow::datatypes::FieldRef> {
    schema
        .fields()
        .iter()
        .filter(|field| {
            field
                .metadata()
                .get(CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_none_or(|value| !value.eq_ignore_ascii_case("true"))
        })
        .map(Arc::clone)
        .collect()
}

fn sql_visible_column_names(schema: &arrow::datatypes::SchemaRef) -> Vec<String> {
    sql_visible_columns(schema)
        .iter()
        .map(|field| field.name().clone())
        .collect()
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
        context: ConnectorRequestContext,
    ) -> Result<Vec<StatisticsTableStatView>, StatisticsApplicationError> {
        let metadata = load_statistics_table_metadata(
            self.controls.as_ref(),
            context.clone(),
            &target.catalog,
            &target.namespace,
            &target.table,
            ConnectorTableResolution::StrictBaseTable,
        )
        .map_err(StatisticsApplicationError::new)?;
        let data_version = metadata.statistics_data_version.clone().ok_or_else(|| {
            StatisticsApplicationError::new(
                "connector metadata did not provide a statistics data-version pin",
            )
        })?;
        let sql_columns = sql_visible_columns(&metadata.schema);
        let requested_column_metric_count = sql_columns.len().saturating_mul(5);
        if requested_column_metric_count > MAX_CONNECTOR_STATISTICS_METRICS {
            return Err(StatisticsApplicationError::new(format!(
                "SHOW TABLE STATS requires {requested_column_metric_count} column metrics, exceeding the connector statistics limit of {MAX_CONNECTOR_STATISTICS_METRICS}",
            )));
        }
        let requested_metric_count = 1usize.saturating_add(requested_column_metric_count);
        let mut metrics = Vec::with_capacity(requested_metric_count);
        metrics.push(StatisticsMetric::RowCount);
        for column in sql_columns {
            let name: Arc<str> = Arc::from(column.name().as_str());
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
            .acquire_current_statistics(metadata.table.owner())
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let evidence = lease
            .read(StatisticsReadRequest {
                table: metadata.table,
                data_version,
                metrics,
                context,
            })
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let queried_version = evidence.data_version().clone();
        Ok(evidence
            .into_metrics()
            .into_iter()
            .map(|(metric, state)| statistics_table_stat_view(metric, state, &queried_version))
            .collect())
    }
}

/// Load one short-lived statistics observation directly through the
/// frontend-owned connector-control registry.  This preserves the exact
/// generation lease for the load and does not expose a Core metadata bridge.
fn load_statistics_table_metadata(
    controls: &dyn ConnectorControlRegistry,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
) -> Result<ConnectorTableMetadata, String> {
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let binding = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    binding
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context,
        })
        .map_err(|error| error.to_string())
}

/// Placeholder for the per-metric columns of a row that has no measured value.
const NO_OBSERVATION: &str = "-";

fn statistics_table_stat_view(
    metric: StatisticsMetric,
    state: StatisticsMetricState,
    queried_version: &StatisticsDataVersion,
) -> StatisticsTableStatView {
    let metric = match metric {
        StatisticsMetric::RowCount => "row_count".to_string(),
        StatisticsMetric::NullCount { column } => format!("null_count:{column}"),
        StatisticsMetric::Minimum { column } => format!("minimum:{column}"),
        StatisticsMetric::Maximum { column } => format!("maximum:{column}"),
        StatisticsMetric::AverageSize { column } => format!("average_size:{column}"),
        StatisticsMetric::ThetaNdv { column } => format!("theta_ndv:{column}"),
    };
    let observation = match state {
        StatisticsMetricState::Available(observation) => observation,
        StatisticsMetricState::Missing(missing) => {
            return unobserved_stat_view(metric, format!("MISSING:{:?}", missing.kind));
        }
        StatisticsMetricState::Error(error) => {
            return unobserved_stat_view(metric, format!("ERROR:{:?}", error.kind));
        }
    };
    StatisticsTableStatView {
        metric,
        value: Some(statistics_metric_value(observation.value().clone())),
        status: "AVAILABLE".into(),
        basis_version: statistics_basis_version(observation.basis_version(), queried_version),
        source: match observation.source() {
            StatisticsMetricSource::CurrentManifest => "CURRENT_MANIFEST".into(),
            StatisticsMetricSource::ProviderArtifact => "PROVIDER_ARTIFACT".into(),
            StatisticsMetricSource::VisibleRowScan => "VISIBLE_ROW_SCAN".into(),
            StatisticsMetricSource::Provider(name) => format!("PROVIDER:{name}"),
        },
        numeric_nature: match observation.numeric_nature() {
            StatisticsNumericNature::Exact => "EXACT",
            StatisticsNumericNature::UpperBound => "UPPER_BOUND",
            StatisticsNumericNature::LowerBound => "LOWER_BOUND",
            StatisticsNumericNature::TwoSidedApproximate => "APPROXIMATE",
        }
        .into(),
        basis_relation: match observation.basis_relation() {
            StatisticsBasisRelation::Identical => "IDENTICAL",
            StatisticsBasisRelation::BasisIsSubset => "BASIS_IS_SUBSET",
            StatisticsBasisRelation::BasisIsSuperset => "BASIS_IS_SUPERSET",
            StatisticsBasisRelation::Incomparable => "INCOMPARABLE",
        }
        .into(),
    }
}

fn unobserved_stat_view(metric: String, status: String) -> StatisticsTableStatView {
    StatisticsTableStatView {
        metric,
        value: None,
        status,
        basis_version: NO_OBSERVATION.into(),
        source: NO_OBSERVATION.into(),
        numeric_nature: NO_OBSERVATION.into(),
        basis_relation: NO_OBSERVATION.into(),
    }
}

/// Renders which table state a value was measured on without leaking the
/// provider's private version encoding through SQL.
///
/// `SAME` is the common case and the one users need to distinguish; when the
/// basis differs, a stable digest is enough to see *that* it differs and to
/// tell two bases apart, while `basis_relation` says how it differs.
fn statistics_basis_version(
    basis: &StatisticsDataVersion,
    queried: &StatisticsDataVersion,
) -> String {
    if basis == queried {
        return "SAME".to_string();
    }
    let digest: [u8; 32] = Sha256::digest(basis.as_bytes()).into();
    let mut rendered = String::from("sha256:");
    for byte in &digest[..8] {
        rendered.push_str(&format!("{byte:02x}"));
    }
    rendered
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsApplicationCommand {
    AnalyzeTable {
        target: StatisticsTableTarget,
        columns: StatisticsColumnIntent,
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
    pub operation_id: LakePublicationId,
    pub state: String,
    pub attempt: u32,
    pub target: StatisticsTableTarget,
    pub error_kind: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableStatView {
    pub metric: String,
    pub value: Option<String>,
    pub status: String,
    /// Which table state the value was measured on, relative to the one being
    /// shown. `SAME` when they are the same state; otherwise a digest, because
    /// the version token is a provider-private encoding.
    pub basis_version: String,
    pub source: String,
    pub numeric_nature: String,
    pub basis_relation: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsApplicationResult {
    /// The job was accepted and left running: the statement did not wait for
    /// it, so its outcome is not known yet.
    JobSubmitted(StatisticsJobView),
    /// The statement waited and the job reached a terminal state.
    ///
    /// This is deliberately not the same answer as `JobSubmitted`. A waited
    /// statement knows whether the work succeeded, and collapsing the two
    /// would report a job that failed as a statement that worked.
    JobCompleted(StatisticsJobView),
    JobCancellationRequested(StatisticsJobView),
    AnalyzeJobs(Vec<StatisticsJobView>),
    TableStats(Vec<StatisticsTableStatView>),
}

pub trait StatisticsApplicationPort: Send + Sync {
    fn execute(
        &self,
        command: StatisticsApplicationCommand,
        execution: Option<&crate::common::admitted_query_context::QueryExecutionContext>,
    ) -> Result<StatisticsApplicationResult, StatisticsApplicationError>;
}

/// A frontend composition with no statistics application authority fails closed.
pub struct UnavailableStatisticsApplicationPort;

impl StatisticsApplicationPort for UnavailableStatisticsApplicationPort {
    fn execute(
        &self,
        _command: StatisticsApplicationCommand,
        _execution: Option<&crate::common::admitted_query_context::QueryExecutionContext>,
    ) -> Result<StatisticsApplicationResult, StatisticsApplicationError> {
        Err(StatisticsApplicationError::new(
            "unified statistics application service is not installed",
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsApplicationError {
    message: String,
    publication_terminal: Option<StatisticsPublicationTerminal>,
    target_binding_failure: Option<ConnectorTableObjectBindingFailure>,
}

/// Exact publication classification carried across the current attempt only.
/// It is terminal diagnostics, never a request to recover or reconcile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsPublicationTerminal {
    KnownUncommitted,
    KnownCommittedFinalization,
    CommitUnknown,
}

impl StatisticsApplicationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            publication_terminal: None,
            target_binding_failure: None,
        }
    }

    pub fn publication(
        terminal: StatisticsPublicationTerminal,
        message: impl Into<String>,
    ) -> Self {
        Self {
            message: message.into(),
            publication_terminal: Some(terminal),
            target_binding_failure: None,
        }
    }

    fn from_connector_error(error: ConnectorError) -> Self {
        let target_binding_failure = error.table_object_binding_failure();
        Self {
            message: error.to_string(),
            publication_terminal: None,
            target_binding_failure,
        }
    }

    pub const fn publication_terminal(&self) -> Option<StatisticsPublicationTerminal> {
        self.publication_terminal
    }

    pub const fn target_binding_failure(&self) -> Option<ConnectorTableObjectBindingFailure> {
        self.target_binding_failure
    }
}

impl fmt::Display for StatisticsApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatisticsApplicationError {}
