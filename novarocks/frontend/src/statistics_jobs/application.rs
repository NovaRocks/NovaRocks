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
//! The frontend owns target resolution, durable job state, and worker composition.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_spi::connector::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, ConnectorCancellation, ConnectorControlRegistry,
    ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableMetadata,
    ConnectorTableRequest, ConnectorTableResolution, ExternalMutationEvidence,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_STATISTICS_METRICS,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, StatisticsBasisRelation, StatisticsDataVersion,
    StatisticsMetric, StatisticsMetricRequest, StatisticsMetricSource, StatisticsMetricState,
    StatisticsMetricValue, StatisticsNumericNature, StatisticsReadRequest,
};
use sha2::{Digest, Sha256};
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

/// The frontend resolves a logical ANALYZE target exactly once. The frontend invokes
/// this before creating a job and persists the returned pin; background work
/// has no latest-name resolution capability.
pub trait StatisticsTargetResolver: Send + Sync {
    fn resolve_table_pin(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<StatisticsTablePin, StatisticsApplicationError>;
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
/// table/version pin was fixed at ANALYZE submission; the worker must not turn this
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

/// Frontend-owned implementation of provider-native collection and
/// publication. The frontend retains this durable-worker port and the immutable
/// request types; the frontend owns connector leases, native mapping, the
/// distributed request, and `ExternalMutationOutcome` handling.
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

/// Composition sink used after the frontend has installed connector control and the
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
        let metadata = load_statistics_table_metadata(
            self.controls.as_ref(),
            context,
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
        Ok(StatisticsTablePin {
            connector_instance_id: metadata.table.owner().as_str().to_string(),
            table_handle: metadata.table.payload().to_vec(),
            data_version: data_version.as_bytes().to_vec(),
            columns: metadata
                .schema
                .fields()
                .iter()
                .filter(|field| {
                    field
                        .metadata()
                        .get(CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                        .is_none_or(|value| !value.eq_ignore_ascii_case("true"))
                })
                .map(|field| field.name().to_string())
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
        let requested_metric_count = 1usize.saturating_add(
            metadata
                .schema
                .fields()
                .iter()
                .filter(|field| {
                    field
                        .metadata()
                        .get(CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                        .is_none_or(|value| !value.eq_ignore_ascii_case("true"))
                })
                .count()
                .saturating_mul(5),
        );
        if requested_metric_count > MAX_CONNECTOR_STATISTICS_METRICS {
            return Err(StatisticsApplicationError::new(format!(
                "SHOW TABLE STATS requires {requested_metric_count} metrics, exceeding the connector statistics limit of {MAX_CONNECTOR_STATISTICS_METRICS}",
            )));
        }
        let mut metrics = Vec::with_capacity(requested_metric_count);
        metrics.push(StatisticsMetric::RowCount);
        for field in metadata.schema.fields().iter().filter(|field| {
            field
                .metadata()
                .get(CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_none_or(|value| !value.eq_ignore_ascii_case("true"))
        }) {
            let name: Arc<str> = Arc::from(field.name().as_str());
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

fn statistics_request_context() -> Result<ConnectorRequestContext, StatisticsApplicationError> {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        Arc::new(NeverCancelled),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| StatisticsApplicationError::new(error.to_string()))
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

/// A frontend composition with no durable statistics authority fails closed.
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
