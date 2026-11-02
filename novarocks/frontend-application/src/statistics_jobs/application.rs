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
//! The frontend owns SQL/connector adaptation and target resolution. The
//! statistics application crate owns current-process job state and worker
//! composition.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, ConnectorControlPlanningLease, ConnectorControlRegistry,
    ConnectorError, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorTableObjectBinding, ConnectorTableObjectBindingFailure,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectId, ConnectorTableObjectRebindRequest,
    ConnectorTableObjectSelector, ConnectorTableResolution, LakePublicationId,
    StatisticsBasisRelation, StatisticsDataVersion, StatisticsMetric, StatisticsMetricSource,
    StatisticsMetricState, StatisticsMetricValue, StatisticsNumericNature,
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
    pub job_admission: novarocks_spi::connector::ConnectorTableJobAdmission,
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
    fn planning_lease(
        &self,
        _target: &StatisticsTableTarget,
    ) -> Result<ConnectorControlPlanningLease, StatisticsApplicationError> {
        Err(StatisticsApplicationError::new(
            "statistics capture has no governed planning lease",
        ))
    }
    fn capture_table_object_async<'a>(
        &'a self,
        _target: &'a StatisticsTableTarget,
        _context: ConnectorRequestContext,
        _lease: &'a ConnectorControlPlanningLease,
        _cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<StatisticsTargetCapture, StatisticsApplicationError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            Err(StatisticsApplicationError::new(
                "statistics capture has no asynchronous source",
            ))
        })
    }
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
    fn planning_lease(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<ConnectorControlPlanningLease, StatisticsApplicationError> {
        let id = ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        self.controls
            .acquire_current(&id)
            .map_err(StatisticsApplicationError::from_connector_error)
    }
    fn capture_table_object_async<'a>(
        &'a self,
        target: &'a StatisticsTableTarget,
        context: ConnectorRequestContext,
        lease: &'a ConnectorControlPlanningLease,
        cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<StatisticsTargetCapture, StatisticsApplicationError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let instance_id = ConnectorInstanceId::parse(&target.catalog)
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
                .capture_table_object_binding_for_statistics_async(
                    ConnectorTableObjectCaptureRequest {
                        table: ConnectorTableIdentity {
                            instance_id,
                            namespace: Arc::from(target.namespace.as_str()),
                            table: Arc::from(target.table.as_str()),
                        },
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        selector: ConnectorTableObjectSelector::Current,
                        context: context.clone(),
                    },
                    cpu,
                )
                .await
                .map_err(StatisticsApplicationError::from_connector_error)?;
            let job_admission = lease
                .binding()
                .metadata()
                .admit_table_job(
                    novarocks_spi::connector::ConnectorTableJobAdmissionRequest {
                        table: ConnectorTableIdentity {
                            instance_id: ConnectorInstanceId::parse(&target.catalog).map_err(
                                |error| StatisticsApplicationError::new(error.to_string()),
                            )?,
                            namespace: Arc::from(target.namespace.as_str()),
                            table: Arc::from(target.table.as_str()),
                        },
                        job: novarocks_spi::connector::ConnectorTableJobKind::Statistics,
                        context: context.clone(),
                    },
                )
                .map_err(StatisticsApplicationError::from_connector_error)?;
            Ok(StatisticsTargetCapture {
                job_admission,
                connector_instance_id: captured.metadata.table.owner().as_str().to_string(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
                object_id: captured.object_id.as_bytes().to_vec(),
                sql_columns: sql_visible_column_names(captured.metadata.schema()),
            })
        })
    }
}

pub(crate) async fn rebind_table_object_async(
    lease: &ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    request: &StatisticsAttemptRequest,
    cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
) -> Result<StatisticsAttemptBinding, StatisticsApplicationError> {
    let instance_id = ConnectorInstanceId::parse(&request.connector_instance_id)
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
    let expected_object_id =
        ConnectorTableObjectId::try_new(Bytes::copy_from_slice(&request.object_id))
            .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
    let ConnectorTableObjectBinding { metadata, .. } = lease
        .binding()
        .metadata()
        .rebind_table_object_binding_for_statistics_async(
            ConnectorTableObjectRebindRequest {
                table: ConnectorTableIdentity {
                    instance_id,
                    namespace: Arc::from(request.namespace.as_str()),
                    table: Arc::from(request.table.as_str()),
                },
                expected_object_id,
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context,
            },
            cpu,
        )
        .await
        .map_err(StatisticsApplicationError::from_connector_error)?;
    let data_version = metadata.statistics_data_version.clone().ok_or_else(|| {
        StatisticsApplicationError::new(
            "connector metadata did not provide a statistics data-version for rebound table",
        )
    })?;
    let sql_columns = sql_visible_columns(metadata.schema());
    Ok(StatisticsAttemptBinding {
        table: metadata.table,
        data_version,
        sql_columns,
    })
}

/// The SQL-visible fields, carrying their types.
///
/// A caller that only validates names takes them off the field. A caller that
/// builds a statistics metric request needs the type: which metrics a column
/// can answer depends on it.
fn sql_visible_columns(schema: &arrow::datatypes::Schema) -> Vec<arrow::datatypes::FieldRef> {
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

fn sql_visible_column_names(schema: &arrow::datatypes::Schema) -> Vec<String> {
    sql_visible_columns(schema)
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

/// Placeholder for the per-metric columns of a row that has no measured value.
const NO_OBSERVATION: &str = "-";

pub(crate) fn statistics_table_stat_view(
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
    JobCancellationRequested(StatisticsJobView),
    AnalyzeJobs(Vec<StatisticsJobView>),
    TableStats(Vec<StatisticsTableStatView>),
}

pub trait StatisticsApplicationPort: Send + Sync {
    fn prepare_statement_action(
        &self,
        _command: StatisticsApplicationCommand,
        _execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
        _family: novarocks_result_contract::ClientPresentationFamily,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        StatisticsApplicationError,
    > {
        Err(StatisticsApplicationError::new(
            "statistics application has no governed statement source",
        ))
    }
    fn execute<'a>(
        &'a self,
        command: StatisticsApplicationCommand,
        execution: Option<
            &'a novarocks_query_application::admitted_query_context::QueryExecutionContext,
        >,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<StatisticsApplicationResult, StatisticsApplicationError>,
                > + Send
                + 'a,
        >,
    >;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsApplicationError {
    message: String,
    target_binding_failure: Option<ConnectorTableObjectBindingFailure>,
    source: Option<ConnectorError>,
    statistics_failure: Option<novarocks_statistics_application::StatisticsFailure>,
    publication_failure: Option<novarocks_statistics_application::StatisticsPublicationFailure>,
    publication_terminal: Option<novarocks_spi::connector::LakePublicationTerminal>,
}

impl StatisticsApplicationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            target_binding_failure: None,
            source: None,
            statistics_failure: None,
            publication_failure: None,
            publication_terminal: None,
        }
    }

    pub(crate) fn from_connector_error(error: ConnectorError) -> Self {
        let target_binding_failure = error.table_object_binding_failure();
        Self {
            message: error.to_string(),
            target_binding_failure,
            source: Some(error),
            statistics_failure: None,
            publication_failure: None,
            publication_terminal: None,
        }
    }

    pub(crate) fn from_statistics_job(
        job: &novarocks_statistics_application::StatisticsJob,
    ) -> Self {
        use novarocks_spi::connector::{
            LakePublicationDisposition as D, LakePublicationFamily, LakePublicationMarkerHeader,
            LakePublicationNextAction, LakePublicationTarget, LakePublicationTerminal,
        };
        use novarocks_statistics_application::StatisticsPublicationFact as P;
        let failure = job.failure.clone();
        let publication_failure = job
            .publication_failure
            .or(job.publication_finalization_failure);
        let disposition = match job.publication {
            P::KnownUncommitted => Some(D::KnownUncommitted),
            P::KnownCommitted => Some(D::KnownCommitted),
            P::CommitUnknown => Some(D::CommitUnknown),
            P::NotStarted | P::NoOp => None,
        };
        let publication_terminal = disposition.map(|disposition| {
            LakePublicationTerminal::new(
                LakePublicationMarkerHeader::new(
                    LakePublicationId::try_from_uuid(job.publication_id.as_uuid())
                        .expect("original statistics UUIDv7"),
                    LakePublicationFamily::Statistics,
                ),
                LakePublicationTarget::try_new(
                    job.target.catalog.as_ref().to_owned(),
                    job.target.namespace.as_ref().to_owned(),
                    Some(job.target.table.as_ref().to_owned()),
                    None,
                )
                .expect("published statistics retained its provider-admitted target"),
                disposition,
                if disposition.do_not_retry() {
                    LakePublicationNextAction::InspectPublishedState
                } else {
                    LakePublicationNextAction::RetryStatement
                },
                None,
            )
        });
        Self {
            message: publication_failure
                .as_ref()
                .map(|failure| failure.public_message())
                .or_else(|| failure.as_ref().map(|f| f.message.to_string()))
                .unwrap_or_else(|| format!("statistics job completed with {:?}", job.state)),
            target_binding_failure: None,
            source: None,
            statistics_failure: failure,
            publication_failure,
            publication_terminal,
        }
    }

    pub(crate) fn publication_disposition(
        &self,
    ) -> Option<novarocks_spi::connector::LakePublicationDisposition> {
        self.publication_terminal
            .as_ref()
            .map(|terminal| terminal.disposition())
    }

    pub(crate) fn with_observer_message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    pub(crate) fn to_query_service_error(
        &self,
    ) -> novarocks_query_application::session_error::QueryServiceError {
        use novarocks_query_application::{
            execution_error::{ExecutionFailureInput, ExecutionFailureSite},
            session_error::{QueryServiceError, QueryServiceErrorKind},
        };
        let mut error = if let Some(failure) = &self.publication_failure {
            QueryServiceError::from_execution_failure(ExecutionFailureInput::new(
                failure.fact(),
                novarocks_execution_contract::SafeDetail::new(failure.public_message())
                    .expect("fixed statistics publication detail fits safe boundary"),
                ExecutionFailureSite::Coordinator,
            ))
            .with_compile_control_provenance(failure.compile_control())
        } else if let Some(control) = self
            .statistics_failure
            .as_ref()
            .and_then(|f| f.compile_control.as_ref())
        {
            QueryServiceError::from_compile_control(control.clone())
        } else {
            QueryServiceError::new(QueryServiceErrorKind::Internal, &self.message)
        };
        if let Some(terminal) = &self.publication_terminal {
            if error.execution_failure().is_none() && terminal.do_not_retry() {
                let header = terminal.header();
                let disposition = match terminal.disposition() {
                    novarocks_spi::connector::LakePublicationDisposition::KnownCommitted => {
                        "KnownCommitted"
                    }
                    novarocks_spi::connector::LakePublicationDisposition::CommitUnknown => {
                        "CommitUnknown"
                    }
                    novarocks_spi::connector::LakePublicationDisposition::KnownUncommitted => {
                        unreachable!()
                    }
                };
                let message = format!(
                    "{} (lake publication id={} family={} disposition={} do_not_retry=true; do not retry statement)",
                    error.message(),
                    header.publication_id(),
                    header.family().as_str(),
                    disposition
                );
                error = QueryServiceError::with_publication_terminal(message, terminal.clone())
                    .with_compile_control(error.compile_control_error())
                    .with_compile_control_provenance(error.compile_control_provenance());
            } else {
                error = error.attach_publication_terminal(terminal.clone());
            }
        }
        error
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

impl std::error::Error for StatisticsApplicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}
