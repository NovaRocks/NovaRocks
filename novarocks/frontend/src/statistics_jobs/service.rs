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
// software distributed under the Apache License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Frontend adapters for the statistics application product owner.
//!
//! This module translates typed frontend commands and connector target
//! captures. It deliberately owns neither a job ledger nor business attempt
//! orchestration. Role composition starts the product-owned job runtime with
//! the concrete attempt adapter; without the required bindings the entrypoint
//! fails closed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::application;
use super::model::StatisticsJobTarget;
use novarocks_statistics_application::{
    StatisticsAttemptExecutor, StatisticsColumns, StatisticsJob, StatisticsJobCreate,
    StatisticsJobId, StatisticsJobRuntime, StatisticsJobService, StatisticsTarget,
};
use novarocks_workload_control::{RootAdmissionHandle, WorkClass, WorkOwner, WorkRequest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsStatementResult {
    JobSubmitted(StatisticsJob),
    JobCancellationRequested(StatisticsJob),
    AnalyzeJobs(Vec<StatisticsJob>),
    TableStats(Vec<StatisticsTableStatRow>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableStatRow {
    pub metric_name: String,
    pub value: Option<String>,
    pub status: String,
    pub basis_version: String,
    pub source: String,
    pub numeric_nature: String,
    pub basis_relation: String,
}

pub trait TableStatisticsReader: Send + Sync {
    fn show_table_stats(
        &self,
        target: &StatisticsJobTarget,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<StatisticsTableStatRow>, String>;
}

/// T12's role composition supplies a root for each independently-owned
/// background statistics business request. There is intentionally no default
/// implementation: borrowing the statement observer would let observer
/// cancellation destroy the job root.
pub trait StatisticsJobRootScopeSource: Send + Sync {
    fn begin_statistics_job(&self) -> Result<WorkOwner, String>;
}

/// Role-local source for independently-owned ANALYZE jobs.
///
/// The Server injects only the narrow root-admission capability.  A submitted
/// job retains the returned owner in the statistics application repository;
/// the admission's transient business permit is deliberately released after
/// it has become that durable process-local responsibility.
#[derive(Clone)]
pub struct RootAdmissionStatisticsJobSource {
    admission: RootAdmissionHandle,
}

impl RootAdmissionStatisticsJobSource {
    pub fn new(admission: RootAdmissionHandle) -> Self {
        Self { admission }
    }
}

impl StatisticsJobRootScopeSource for RootAdmissionStatisticsJobSource {
    fn begin_statistics_job(&self) -> Result<WorkOwner, String> {
        let root = self
            .admission
            .try_begin_root(WorkRequest::new(WorkClass::Statistics))
            .map_err(|error| error.to_string())?;
        // The job repository now owns the root responsibility. It is not a
        // foreground business admission and must not retain that permit.
        drop(root.business);
        Ok(root.owner)
    }
}

struct StatisticsTableReaderAdapter {
    inner: Arc<dyn application::StatisticsTableReader>,
}

impl TableStatisticsReader for StatisticsTableReaderAdapter {
    fn show_table_stats(
        &self,
        target: &StatisticsJobTarget,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<StatisticsTableStatRow>, String> {
        self.inner
            .show_table_stats(
                &application::StatisticsTableTarget {
                    catalog: target.catalog.clone(),
                    namespace: target.namespace.clone(),
                    table: target.table.clone(),
                },
                context,
            )
            .map_err(|error| error.to_string())
            .map(|rows| {
                rows.into_iter()
                    .map(|row| StatisticsTableStatRow {
                        metric_name: row.metric,
                        value: row.value,
                        status: row.status,
                        basis_version: row.basis_version,
                        source: row.source,
                        numeric_nature: row.numeric_nature,
                        basis_relation: row.basis_relation,
                    })
                    .collect()
            })
    }
}

/// Construct the sole role-local adapter for SQL statistics reads. The
/// product port receives it at construction and never accepts a later sink.
pub(crate) fn table_statistics_reader_for_role(
    controls: Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
) -> Arc<dyn TableStatisticsReader> {
    Arc::new(StatisticsTableReaderAdapter {
        inner: Arc::new(application::ConnectorStatisticsTableReader::new(controls)),
    })
}

pub struct FrontendStatisticsApplicationPort {
    job_runtime: StatisticsJobRuntime,
    target_resolver: Arc<dyn application::StatisticsTargetResolver>,
    root_scope: Arc<dyn StatisticsJobRootScopeSource>,
    table_statistics: Arc<dyn TableStatisticsReader>,
    runtime: tokio::runtime::Handle,
}
impl FrontendStatisticsApplicationPort {
    pub fn new(
        job_service: StatisticsJobService,
        target_resolver: Arc<dyn application::StatisticsTargetResolver>,
        root_scope: Arc<dyn StatisticsJobRootScopeSource>,
        table_statistics: Arc<dyn TableStatisticsReader>,
        core_executor: Arc<dyn StatisticsAttemptExecutor>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            job_runtime: StatisticsJobRuntime::start(job_service, core_executor, runtime.clone()),
            target_resolver,
            root_scope,
            table_statistics,
            runtime,
        }
    }
    pub async fn shutdown_worker_until(&self, _deadline: Instant) -> Result<(), String> {
        self.job_runtime.shutdown_until(_deadline).await
    }
    pub fn request_worker_stop_for_process_exit(&self) {
        self.job_runtime.request_stop_for_process_exit();
    }

    async fn execute_command(
        &self,
        command: application::StatisticsApplicationCommand,
        submitted_at_ms: i64,
        connector_context: Option<novarocks_spi::connector::ConnectorRequestContext>,
    ) -> Result<StatisticsStatementResult, application::StatisticsApplicationError> {
        match command {
            application::StatisticsApplicationCommand::AnalyzeTable { target, columns } => {
                let context = connector_context.ok_or_else(|| {
                    application::StatisticsApplicationError::new(
                        "ANALYZE target capture requires an admitted execution context",
                    )
                })?;
                let capture = self
                    .target_resolver
                    .capture_table_object(&target, context)?;
                let owner = self
                    .root_scope
                    .begin_statistics_job()
                    .map_err(application::StatisticsApplicationError::new)?;
                let columns = match columns {
                    application::StatisticsColumnIntent::AllColumns => StatisticsColumns::All,
                    application::StatisticsColumnIntent::Explicit(columns) => {
                        StatisticsColumns::Explicit(
                            columns.into_iter().map(Arc::<str>::from).collect(),
                        )
                    }
                };
                self.job_runtime
                    .submit(
                        StatisticsJobCreate {
                            target: StatisticsTarget {
                                catalog: Arc::from(target.catalog),
                                namespace: Arc::from(target.namespace),
                                table: Arc::from(target.table),
                                object_id: Arc::from(capture.object_id),
                            },
                            columns,
                            submitted_at_ms,
                        },
                        owner,
                    )
                    .await
                    .map(StatisticsStatementResult::JobSubmitted)
                    .map_err(|error| {
                        application::StatisticsApplicationError::new(error.to_string())
                    })
            }
            application::StatisticsApplicationCommand::ShowAnalyzeJobs => self
                .job_runtime
                .list()
                .await
                .map(StatisticsStatementResult::AnalyzeJobs)
                .map_err(|error| application::StatisticsApplicationError::new(error.to_string())),
            application::StatisticsApplicationCommand::CancelAnalyze { job_id } => self
                .job_runtime
                .request_cancel(StatisticsJobId::from_uuid(job_id), submitted_at_ms)
                .await
                .map(StatisticsStatementResult::JobCancellationRequested)
                .map_err(|error| application::StatisticsApplicationError::new(error.to_string())),
            application::StatisticsApplicationCommand::ShowTableStats { target } => self
                .table_statistics
                .show_table_stats(
                    &target.into(),
                    connector_context.ok_or_else(|| {
                        application::StatisticsApplicationError::new(
                            "SHOW TABLE STATS requires an admitted execution context",
                        )
                    })?,
                )
                .map(StatisticsStatementResult::TableStats)
                .map_err(application::StatisticsApplicationError::new),
        }
    }
}

impl application::StatisticsApplicationPort for FrontendStatisticsApplicationPort {
    fn execute(
        &self,
        command: application::StatisticsApplicationCommand,
        execution: Option<
            &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        >,
    ) -> Result<application::StatisticsApplicationResult, application::StatisticsApplicationError>
    {
        let connector_context = match (&command, execution) {
            (application::StatisticsApplicationCommand::AnalyzeTable { .. }, Some(execution)) => {
                Some(statistics_connector_context(execution, true)?)
            }
            (application::StatisticsApplicationCommand::ShowTableStats { .. }, Some(execution)) => {
                Some(statistics_connector_context(execution, false)?)
            }
            (application::StatisticsApplicationCommand::AnalyzeTable { .. }, None) => {
                return Err(application::StatisticsApplicationError::new(
                    "ANALYZE requires an admitted execution context",
                ));
            }
            (application::StatisticsApplicationCommand::ShowTableStats { .. }, None) => {
                return Err(application::StatisticsApplicationError::new(
                    "SHOW TABLE STATS requires an admitted execution context",
                ));
            }
            _ => None,
        };
        let at_ms = now_ms().map_err(application::StatisticsApplicationError::new)?;
        let result = tokio::task::block_in_place(|| {
            self.runtime
                .block_on(self.execute_command(command, at_ms, connector_context))
        })?;
        Ok(map_application_result(result))
    }
}

fn statistics_connector_context(
    execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    require_deadline: bool,
) -> Result<
    novarocks_spi::connector::ConnectorRequestContext,
    application::StatisticsApplicationError,
> {
    let deadline = match execution.deadline() {
        Some(value) => value,
        None if require_deadline => {
            return Err(application::StatisticsApplicationError::new(
                "ANALYZE target capture requires an admitted deadline",
            ));
        }
        None => Instant::now()
            .checked_add(Duration::from_secs(30))
            .ok_or_else(|| {
                application::StatisticsApplicationError::new(
                    "statistics metadata-read deadline overflow",
                )
            })?,
    };
    novarocks_spi::connector::ConnectorRequestContext::try_new(
        deadline,
        Arc::new(StatisticsApplicationCancellation(
            execution.cancellation().clone(),
        )),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| application::StatisticsApplicationError::new(error.to_string()))
}
struct StatisticsApplicationCancellation(
    novarocks_query_application::cancellation::QueryCancellationView,
);
impl novarocks_spi::connector::ConnectorCancellation for StatisticsApplicationCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}
fn map_application_result(
    result: StatisticsStatementResult,
) -> application::StatisticsApplicationResult {
    match result {
        StatisticsStatementResult::JobSubmitted(job) => {
            application::StatisticsApplicationResult::JobSubmitted(job_view(job))
        }
        StatisticsStatementResult::JobCancellationRequested(job) => {
            application::StatisticsApplicationResult::JobCancellationRequested(job_view(job))
        }
        StatisticsStatementResult::AnalyzeJobs(jobs) => {
            application::StatisticsApplicationResult::AnalyzeJobs(
                jobs.into_iter().map(job_view).collect(),
            )
        }
        StatisticsStatementResult::TableStats(rows) => {
            application::StatisticsApplicationResult::TableStats(
                rows.into_iter()
                    .map(|row| application::StatisticsTableStatView {
                        metric: row.metric_name,
                        value: row.value,
                        status: row.status,
                        basis_version: row.basis_version,
                        source: row.source,
                        numeric_nature: row.numeric_nature,
                        basis_relation: row.basis_relation,
                    })
                    .collect(),
            )
        }
    }
}
fn job_view(job: StatisticsJob) -> application::StatisticsJobView {
    let finalization_failure = job.publication_finalization_failure.clone();
    let failure = job.failure.clone().or(finalization_failure.clone());
    application::StatisticsJobView {
        job_id: job.id.as_uuid(),
        operation_id: novarocks_spi::connector::LakePublicationId::try_from_uuid(
            job.publication_id.as_uuid(),
        )
        .expect("statistics publication IDs are UUIDv7"),
        state: match job.state {
            novarocks_statistics_application::StatisticsJobState::Active(phase) => {
                format!("{phase:?}")
            }
            novarocks_statistics_application::StatisticsJobState::Terminal(conclusion) => {
                format!("{conclusion:?}")
            }
        }
        .to_ascii_uppercase(),
        attempt: u32::from(job.query_attempt_id.is_some()),
        target: application::StatisticsTableTarget {
            catalog: job.target.catalog.to_string(),
            namespace: job.target.namespace.to_string(),
            table: job.target.table.to_string(),
        },
        error_kind: failure.as_ref().map(|_| {
            if finalization_failure.is_some() {
                "FINALIZATION".into()
            } else {
                "STATISTICS".into()
            }
        }),
        error_message: failure.map(|failure| failure.message.to_string()),
    }
}
fn now_ms() -> Result<i64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis()
        .try_into()
        .map_err(|_| "statistics time exceeds i64 milliseconds".into())
}
