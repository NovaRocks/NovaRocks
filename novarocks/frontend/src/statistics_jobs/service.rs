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

//! Typed frontend application surface for current-process statistics jobs.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use uuid::Uuid;

use super::application;
use super::model::{StatisticsJob, StatisticsJobCreate, StatisticsJobTarget};
use super::repository::{StatisticsJobRepository, StatisticsJobRepositoryError};
use super::worker::{StatisticsAnalyzeWorker, StatisticsAttemptError, StatisticsAttemptExecutor};
use crate::workload_lifecycle::FrontendServingLifecycle;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzeTableStatement {
    pub target: StatisticsJobTarget,
    pub columns: application::StatisticsColumnIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowAnalyzeJobsStatement {
    pub target: Option<StatisticsJobTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelAnalyzeStatement {
    pub job_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowTableStatsStatement {
    pub target: StatisticsJobTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsStatement {
    AnalyzeTable(AnalyzeTableStatement),
    ShowAnalyzeJobs(ShowAnalyzeJobsStatement),
    CancelAnalyze(CancelAnalyzeStatement),
    ShowTableStats(ShowTableStatsStatement),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsStatementResult {
    JobSubmitted(StatisticsJob),
    JobCompleted(StatisticsJob),
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

pub trait StatisticsJobTargetResolver: Send + Sync {
    fn capture_table_object(
        &self,
        target: &StatisticsJobTarget,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<application::StatisticsTargetCapture, String>;
}

struct UnavailableStatisticsJobTargetResolver;

impl StatisticsJobTargetResolver for UnavailableStatisticsJobTargetResolver {
    fn capture_table_object(
        &self,
        _target: &StatisticsJobTarget,
        _context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<application::StatisticsTargetCapture, String> {
        Err("ANALYZE is unavailable until the frontend statistics target resolver is bound".into())
    }
}

struct StatisticsTargetResolverAdapter {
    inner: Arc<dyn application::StatisticsTargetResolver>,
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
            .map_err(|error| error.to_string())
    }
}

impl StatisticsJobTargetResolver for StatisticsTargetResolverAdapter {
    fn capture_table_object(
        &self,
        target: &StatisticsJobTarget,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<application::StatisticsTargetCapture, String> {
        self.inner
            .capture_table_object(
                &application::StatisticsTableTarget {
                    catalog: target.catalog.clone(),
                    namespace: target.namespace.clone(),
                    table: target.table.clone(),
                },
                context,
            )
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone)]
pub struct StatisticsApplicationService {
    repository: StatisticsJobRepository,
    target_resolver: Arc<StatisticsTargetResolverSlot>,
}

struct StatisticsTargetResolverSlot {
    resolver: std::sync::RwLock<Arc<dyn StatisticsJobTargetResolver>>,
    bound: std::sync::atomic::AtomicBool,
}

impl StatisticsTargetResolverSlot {
    fn unbound() -> Self {
        Self {
            resolver: std::sync::RwLock::new(Arc::new(UnavailableStatisticsJobTargetResolver)),
            bound: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl StatisticsApplicationService {
    pub fn new() -> Self {
        Self {
            repository: StatisticsJobRepository::new(),
            target_resolver: Arc::new(StatisticsTargetResolverSlot::unbound()),
        }
    }

    pub fn repository(&self) -> StatisticsJobRepository {
        self.repository.clone()
    }

    pub fn bind_target_resolver(
        &self,
        resolver: Arc<dyn StatisticsJobTargetResolver>,
    ) -> Result<(), String> {
        let mut slot = self
            .target_resolver
            .resolver
            .write()
            .map_err(|_| "statistics target resolver lock poisoned".to_string())?;
        if self
            .target_resolver
            .bound
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return Err("statistics target resolver is already bound".to_string());
        }
        *slot = resolver;
        Ok(())
    }

    pub async fn execute(
        &self,
        statement: StatisticsStatement,
        submitted_at_ms: i64,
        table_statistics: &dyn TableStatisticsReader,
        connector_context: Option<novarocks_spi::connector::ConnectorRequestContext>,
    ) -> Result<StatisticsStatementResult, StatisticsApplicationError> {
        match statement {
            StatisticsStatement::AnalyzeTable(statement) => {
                let resolver = self
                    .target_resolver
                    .resolver
                    .read()
                    .map_err(|_| {
                        StatisticsApplicationError::target_resolution(
                            "statistics target resolver lock poisoned",
                        )
                    })?
                    .clone();
                let target_capture = resolver
                    .capture_table_object(
                        &statement.target,
                        connector_context.ok_or_else(|| {
                            StatisticsApplicationError::target_resolution(
                                "ANALYZE target capture requires an admitted execution context",
                            )
                        })?,
                    )
                    .map_err(StatisticsApplicationError::target_resolution)?;
                let job = self
                    .repository
                    .create(StatisticsJobCreate {
                        target: statement.target,
                        connector_instance_id: target_capture.connector_instance_id,
                        object_id: target_capture.object_id,
                        columns: statement.columns,
                        submitted_at_ms,
                    })
                    .await
                    .map_err(StatisticsApplicationError::repository)?;
                Ok(StatisticsStatementResult::JobSubmitted(job))
            }
            StatisticsStatement::ShowAnalyzeJobs(statement) => {
                let mut jobs = self
                    .repository
                    .list()
                    .await
                    .map_err(StatisticsApplicationError::repository)?;
                if let Some(target) = statement.target {
                    jobs.retain(|job| job.target == target);
                }
                Ok(StatisticsStatementResult::AnalyzeJobs(jobs))
            }
            StatisticsStatement::CancelAnalyze(statement) => self
                .repository
                .request_cancel(statement.job_id, submitted_at_ms)
                .await
                .map(StatisticsStatementResult::JobCancellationRequested)
                .map_err(StatisticsApplicationError::repository),
            StatisticsStatement::ShowTableStats(statement) => {
                let context = connector_context.ok_or_else(|| {
                    StatisticsApplicationError::table_statistics(
                        "SHOW TABLE STATS requires an admitted execution context".to_string(),
                    )
                })?;
                table_statistics
                    .show_table_stats(&statement.target, context)
                    .map(StatisticsStatementResult::TableStats)
                    .map_err(StatisticsApplicationError::table_statistics)
            }
        }
    }

    async fn wait_for_terminal(
        &self,
        job_id: Uuid,
        execution: &crate::common::admitted_query_context::QueryExecutionContext,
    ) -> Result<Option<StatisticsJob>, StatisticsApplicationError> {
        loop {
            let job = self
                .repository
                .get(job_id)
                .await
                .map_err(StatisticsApplicationError::repository)?
                .ok_or_else(|| StatisticsApplicationError {
                    kind: StatisticsApplicationErrorKind::Repository,
                    message: format!(
                        "statistics job {job_id} disappeared while waiting for completion"
                    ),
                })?;
            if job.state.is_terminal() {
                return Ok(Some(job));
            }
            // This observer detaches; it never changes the process-owned job.
            if execution.cancellation().is_cancelled() {
                return Ok(None);
            }
            let sleep_for = match execution.deadline() {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Ok(None);
                    }
                    remaining.min(Duration::from_millis(25))
                }
                None => Duration::from_millis(25),
            };
            tokio::time::sleep(sleep_for).await;
        }
    }
}

impl Default for StatisticsApplicationService {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsApplicationErrorKind {
    Repository,
    TableStatistics,
    TargetResolution,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsApplicationError {
    kind: StatisticsApplicationErrorKind,
    message: String,
}

impl StatisticsApplicationError {
    fn repository(error: StatisticsJobRepositoryError) -> Self {
        Self {
            kind: StatisticsApplicationErrorKind::Repository,
            message: error.to_string(),
        }
    }

    fn table_statistics(error: String) -> Self {
        Self {
            kind: StatisticsApplicationErrorKind::TableStatistics,
            message: error,
        }
    }

    fn target_resolution(error: impl Into<String>) -> Self {
        Self {
            kind: StatisticsApplicationErrorKind::TargetResolution,
            message: error.into(),
        }
    }

    pub const fn kind(&self) -> StatisticsApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for StatisticsApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatisticsApplicationError {}

/// Frontend owner for parsed statistics commands and the current process
/// worker. It accepts no SQL text across this boundary.
pub struct FrontendStatisticsApplicationPort {
    service: StatisticsApplicationService,
    table_statistics: std::sync::RwLock<Option<Arc<dyn TableStatisticsReader>>>,
    runtime: tokio::runtime::Handle,
    attempt_executor: Mutex<Option<Arc<dyn StatisticsAttemptExecutor>>>,
    worker: Mutex<Option<StatisticsAnalyzeWorker>>,
    workload_lifecycle: Option<FrontendServingLifecycle>,
}

impl FrontendStatisticsApplicationPort {
    pub fn new(service: StatisticsApplicationService, runtime: tokio::runtime::Handle) -> Self {
        Self {
            service,
            table_statistics: std::sync::RwLock::new(None),
            runtime,
            attempt_executor: Mutex::new(None),
            worker: Mutex::new(None),
            workload_lifecycle: None,
        }
    }

    /// Installs the shared FE lifecycle before the process-local worker starts.
    pub(crate) fn with_workload_lifecycle(mut self, lifecycle: FrontendServingLifecycle) -> Self {
        self.workload_lifecycle = Some(lifecycle);
        self
    }

    pub fn bind_table_statistics_reader(
        &self,
        reader: Arc<dyn TableStatisticsReader>,
    ) -> Result<(), String> {
        let mut slot = self
            .table_statistics
            .write()
            .map_err(|_| "statistics table reader lock poisoned".to_string())?;
        if slot.is_some() {
            return Err("statistics table reader is already bound".to_string());
        }
        *slot = Some(reader);
        Ok(())
    }

    pub fn bind_statistics_target_resolver(
        &self,
        resolver: Arc<dyn application::StatisticsTargetResolver>,
    ) -> Result<(), String> {
        self.service
            .bind_target_resolver(Arc::new(StatisticsTargetResolverAdapter {
                inner: resolver,
            }))
    }

    fn bind_statistics_attempt_executor(
        &self,
        executor: Arc<dyn application::StatisticsAttemptExecutor>,
    ) -> Result<(), String> {
        let adapter: Arc<dyn StatisticsAttemptExecutor> =
            Arc::new(StatisticsAttemptAdapter { inner: executor });
        let mut executor_slot = self
            .attempt_executor
            .lock()
            .map_err(|_| "statistics attempt executor lock poisoned".to_string())?;
        if executor_slot.is_some() {
            return Err("statistics attempt executor is already bound".to_string());
        }
        let workload_lifecycle = self.workload_lifecycle.clone().ok_or_else(|| {
            "statistics worker requires the shared frontend serving lifecycle".to_string()
        })?;
        let worker = tokio::task::block_in_place(|| {
            self.runtime.block_on(StatisticsAnalyzeWorker::start(
                &self.runtime,
                self.service.repository(),
                Arc::clone(&adapter),
                workload_lifecycle,
            ))
        })?;
        let mut worker_slot = self
            .worker
            .lock()
            .map_err(|_| "statistics worker lock poisoned".to_string())?;
        if worker_slot.is_some() {
            return Err("statistics worker is already started".to_string());
        }
        *executor_slot = Some(adapter);
        *worker_slot = Some(worker);
        Ok(())
    }

    pub fn shutdown_worker(&self) -> Result<(), String> {
        if let Some(mut worker) = self
            .worker
            .lock()
            .map_err(|_| "statistics worker lock poisoned".to_string())?
            .take()
        {
            worker.shutdown()?;
        }
        self.attempt_executor
            .lock()
            .map_err(|_| "statistics attempt executor lock poisoned".to_string())?
            .take();
        Ok(())
    }
}

impl application::StatisticsApplicationPort for FrontendStatisticsApplicationPort {
    fn execute(
        &self,
        command: application::StatisticsApplicationCommand,
        execution: Option<&crate::common::admitted_query_context::QueryExecutionContext>,
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
        let statement = match command {
            application::StatisticsApplicationCommand::AnalyzeTable { target, columns } => {
                StatisticsStatement::AnalyzeTable(AnalyzeTableStatement {
                    target: target.into(),
                    columns,
                })
            }
            application::StatisticsApplicationCommand::ShowAnalyzeJobs => {
                StatisticsStatement::ShowAnalyzeJobs(ShowAnalyzeJobsStatement { target: None })
            }
            application::StatisticsApplicationCommand::CancelAnalyze { job_id } => {
                StatisticsStatement::CancelAnalyze(CancelAnalyzeStatement { job_id })
            }
            application::StatisticsApplicationCommand::ShowTableStats { target } => {
                StatisticsStatement::ShowTableStats(ShowTableStatsStatement {
                    target: target.into(),
                })
            }
        };
        let submitted_at_ms = now_ms().map_err(application::StatisticsApplicationError::new)?;
        let reader = self
            .table_statistics
            .read()
            .map_err(|_| {
                application::StatisticsApplicationError::new(
                    "statistics table reader lock poisoned",
                )
            })?
            .clone()
            .unwrap_or_else(|| Arc::new(UnboundTableStatisticsReader));
        let result = tokio::task::block_in_place(|| {
            self.runtime.block_on(self.service.execute(
                statement,
                submitted_at_ms,
                reader.as_ref(),
                connector_context,
            ))
        })
        .map_err(|error| application::StatisticsApplicationError::new(error.to_string()))?;
        if matches!(result, StatisticsStatementResult::JobSubmitted(_))
            && let Ok(worker) = self.worker.lock()
            && let Some(worker) = worker.as_ref()
        {
            worker.wakeup();
        }
        let result = match (result, execution) {
            (StatisticsStatementResult::JobSubmitted(job), Some(execution)) => {
                match tokio::task::block_in_place(|| {
                    self.runtime
                        .block_on(self.service.wait_for_terminal(job.job_id, execution))
                }) {
                    Ok(Some(completed)) => StatisticsStatementResult::JobCompleted(completed),
                    Ok(None) => StatisticsStatementResult::JobSubmitted(job),
                    Err(error) => {
                        return Err(application::StatisticsApplicationError::new(
                            error.to_string(),
                        ));
                    }
                }
            }
            (other, _) => other,
        };
        Ok(map_application_result(result))
    }
}

fn statistics_connector_context(
    execution: &crate::common::admitted_query_context::QueryExecutionContext,
    require_admitted_deadline: bool,
) -> Result<
    novarocks_spi::connector::ConnectorRequestContext,
    application::StatisticsApplicationError,
> {
    let deadline = match execution.deadline() {
        Some(deadline) => deadline,
        None if require_admitted_deadline => {
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

struct StatisticsApplicationCancellation(crate::common::query_cancellation::QueryCancellationView);

impl novarocks_spi::connector::ConnectorCancellation for StatisticsApplicationCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}

impl application::StatisticsTargetResolverSink for FrontendStatisticsApplicationPort {
    fn bind_statistics_target_resolver(
        &self,
        resolver: Arc<dyn application::StatisticsTargetResolver>,
    ) -> Result<(), String> {
        self.bind_statistics_target_resolver(resolver)
    }
}

impl application::StatisticsTableReaderSink for FrontendStatisticsApplicationPort {
    fn bind_statistics_table_reader(
        &self,
        reader: Arc<dyn application::StatisticsTableReader>,
    ) -> Result<(), String> {
        self.bind_table_statistics_reader(Arc::new(StatisticsTableReaderAdapter { inner: reader }))
    }
}

impl application::StatisticsAttemptExecutorSink for FrontendStatisticsApplicationPort {
    fn bind_statistics_attempt_executor(
        &self,
        executor: Arc<dyn application::StatisticsAttemptExecutor>,
    ) -> Result<(), String> {
        self.bind_statistics_attempt_executor(executor)
    }
}

struct StatisticsAttemptAdapter {
    inner: Arc<dyn application::StatisticsAttemptExecutor>,
}

impl StatisticsAttemptAdapter {
    fn request(job: &StatisticsJob) -> application::StatisticsAttemptRequest {
        application::StatisticsAttemptRequest {
            operation_id: job.operation_id,
            connector_instance_id: job.connector_instance_id.clone(),
            namespace: job.target.namespace.clone(),
            table: job.target.table.clone(),
            object_id: job.object_id.clone(),
            columns: job.columns.clone(),
        }
    }

    fn map_error(error: application::StatisticsApplicationError) -> StatisticsAttemptError {
        if let Some(failure) = error.target_binding_failure() {
            let kind = match failure {
                novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced => {
                    super::model::StatisticsJobErrorKind::TargetReplaced
                }
                novarocks_spi::connector::ConnectorTableObjectBindingFailure::Missing => {
                    super::model::StatisticsJobErrorKind::TargetMissing
                }
            };
            StatisticsAttemptError::permanent(kind, error.to_string())
        } else if let Some(terminal) = error.publication_terminal() {
            StatisticsAttemptError::publication(terminal, error.to_string())
        } else {
            StatisticsAttemptError::permanent(
                super::model::StatisticsJobErrorKind::Connector,
                error.to_string(),
            )
        }
    }
}

impl StatisticsAttemptExecutor for StatisticsAttemptAdapter {
    fn execute(
        &self,
        job: &StatisticsJob,
        cancellation: crate::common::query_cancellation::QueryCancellationView,
    ) -> Result<(), StatisticsAttemptError> {
        self.inner
            .execute(&Self::request(job), cancellation)
            .map_err(Self::map_error)
    }
}

struct UnboundTableStatisticsReader;

impl TableStatisticsReader for UnboundTableStatisticsReader {
    fn show_table_stats(
        &self,
        _target: &StatisticsJobTarget,
        _context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<StatisticsTableStatRow>, String> {
        Err(
            "SHOW TABLE STATS is unavailable until the frontend statistics table reader is bound"
                .into(),
        )
    }
}

fn map_application_result(
    result: StatisticsStatementResult,
) -> application::StatisticsApplicationResult {
    match result {
        StatisticsStatementResult::JobSubmitted(job) => {
            application::StatisticsApplicationResult::JobSubmitted(job_view(job))
        }
        StatisticsStatementResult::JobCompleted(job) => {
            application::StatisticsApplicationResult::JobCompleted(job_view(job))
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
    application::StatisticsJobView {
        job_id: job.job_id,
        operation_id: job.operation_id,
        state: format!("{:?}", job.state).to_ascii_uppercase(),
        attempt: job.attempt,
        target: application::StatisticsTableTarget {
            catalog: job.target.catalog,
            namespace: job.target.namespace,
            table: job.target.table,
        },
        error_kind: job
            .error
            .as_ref()
            .map(|error| format!("{:?}", error.kind).to_ascii_uppercase()),
        error_message: job.error.map(|error| error.message),
    }
}

fn now_ms() -> Result<i64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis()
        .try_into()
        .map_err(|_| "statistics submission timestamp overflow".to_string())
}
