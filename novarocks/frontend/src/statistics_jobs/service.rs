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

//! Typed frontend application surface for statistics statements.
//!
//! Parser integration converts only the four supported statement AST variants
//! to `StatisticsStatement`; this module never receives SQL text and never
//! reparses it.  A missing StateStore remains a configuration error for every
//! job command, while read-only table-stat display is supplied independently.

use std::fmt;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use novarocks::engine::statistics_application as core_application;

use super::model::{
    StatisticsJob, StatisticsJobCreate, StatisticsJobTablePin, StatisticsJobTarget,
};
use super::repository::{StatisticsJobRepository, StatisticsJobRepositoryError};
use super::worker::{
    StatisticsAnalyzeWorker, StatisticsAttemptError, StatisticsAttemptExecutor,
    StatisticsCollectedAttempt,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzeTableStatement {
    pub target: StatisticsJobTarget,
    pub metric_names: Vec<String>,
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
    JobCancellationRequested(StatisticsJob),
    AnalyzeJobs(Vec<StatisticsJob>),
    TableStats(Vec<StatisticsTableStatRow>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableStatRow {
    pub metric_name: String,
    pub value: Option<String>,
    pub status: String,
}

/// Read-only statistics data may exist without a StateStore. This port is
/// intentionally separate from durable job ownership and has no write method.
pub trait TableStatisticsReader: Send + Sync {
    fn show_table_stats(
        &self,
        target: &StatisticsJobTarget,
    ) -> Result<Vec<StatisticsTableStatRow>, String>;
}

/// Resolves an ANALYZE logical target exactly once, before its durable job is
/// created. The returned pin is persisted with the job and never refreshed by
/// the worker; this keeps collection and publication on one data version.
pub trait StatisticsJobTargetResolver: Send + Sync {
    fn resolve_table_pin(
        &self,
        target: &StatisticsJobTarget,
    ) -> Result<StatisticsJobTablePin, String>;
}

struct UnavailableStatisticsJobTargetResolver;

impl StatisticsJobTargetResolver for UnavailableStatisticsJobTargetResolver {
    fn resolve_table_pin(
        &self,
        _target: &StatisticsJobTarget,
    ) -> Result<StatisticsJobTablePin, String> {
        Err("ANALYZE is unavailable until the Core statistics target resolver is bound".into())
    }
}

struct CoreStatisticsTargetResolverAdapter {
    inner: std::sync::Arc<dyn core_application::StatisticsTargetResolver>,
}

struct CoreStatisticsTableReaderAdapter {
    inner: std::sync::Arc<dyn core_application::StatisticsTableReader>,
}

impl TableStatisticsReader for CoreStatisticsTableReaderAdapter {
    fn show_table_stats(
        &self,
        target: &StatisticsJobTarget,
    ) -> Result<Vec<StatisticsTableStatRow>, String> {
        self.inner
            .show_table_stats(&core_application::StatisticsTableTarget {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            })
            .map(|rows| {
                rows.into_iter()
                    .map(|row| StatisticsTableStatRow {
                        metric_name: row.metric,
                        value: row.value,
                        status: row.status,
                    })
                    .collect()
            })
            .map_err(|error| error.to_string())
    }
}

impl StatisticsJobTargetResolver for CoreStatisticsTargetResolverAdapter {
    fn resolve_table_pin(
        &self,
        target: &StatisticsJobTarget,
    ) -> Result<StatisticsJobTablePin, String> {
        let pin = self
            .inner
            .resolve_table_pin(&core_application::StatisticsTableTarget {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            })
            .map_err(|error| error.to_string())?;
        Ok(StatisticsJobTablePin {
            connector_instance_id: pin.connector_instance_id,
            table_handle: pin.table_handle,
            data_version: pin.data_version,
            columns: pin.columns,
        })
    }
}

#[derive(Clone)]
pub struct StatisticsApplicationService {
    repository: Option<StatisticsJobRepository>,
    target_resolver: std::sync::Arc<StatisticsTargetResolverSlot>,
}

struct StatisticsTargetResolverSlot {
    resolver: std::sync::RwLock<std::sync::Arc<dyn StatisticsJobTargetResolver>>,
    bound: std::sync::atomic::AtomicBool,
}

impl StatisticsTargetResolverSlot {
    fn unbound() -> Self {
        Self {
            resolver: std::sync::RwLock::new(std::sync::Arc::new(
                UnavailableStatisticsJobTargetResolver,
            )),
            bound: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn bound(resolver: std::sync::Arc<dyn StatisticsJobTargetResolver>) -> Self {
        Self {
            resolver: std::sync::RwLock::new(resolver),
            bound: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

impl StatisticsApplicationService {
    pub fn unavailable() -> Self {
        Self {
            repository: None,
            target_resolver: std::sync::Arc::new(StatisticsTargetResolverSlot::unbound()),
        }
    }

    pub fn with_repository(repository: StatisticsJobRepository) -> Self {
        Self {
            repository: Some(repository),
            target_resolver: std::sync::Arc::new(StatisticsTargetResolverSlot::unbound()),
        }
    }

    pub fn worker_repository(&self) -> Option<StatisticsJobRepository> {
        self.repository.clone()
    }

    pub fn with_repository_and_target_resolver(
        repository: StatisticsJobRepository,
        target_resolver: std::sync::Arc<dyn StatisticsJobTargetResolver>,
    ) -> Self {
        Self {
            repository: Some(repository),
            target_resolver: std::sync::Arc::new(StatisticsTargetResolverSlot::bound(
                target_resolver,
            )),
        }
    }

    pub fn bind_target_resolver(
        &self,
        resolver: std::sync::Arc<dyn StatisticsJobTargetResolver>,
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
    ) -> Result<StatisticsStatementResult, StatisticsApplicationError> {
        match statement {
            StatisticsStatement::AnalyzeTable(statement) => {
                let repository = self.repository()?;
                let resolver = self
                    .target_resolver
                    .resolver
                    .read()
                    .map_err(|_| {
                        StatisticsApplicationError::target_resolution(
                            "statistics target resolver lock poisoned".to_string(),
                        )
                    })?
                    .clone();
                let table_pin = resolver
                    .resolve_table_pin(&statement.target)
                    .map_err(StatisticsApplicationError::target_resolution)?;
                // An omitted column list means every column from this one-time
                // resolution. Persist that concrete, bounded list so a later
                // worker attempt never has to resolve latest metadata again.
                let metric_names = if statement.metric_names.is_empty() {
                    table_pin.columns.clone()
                } else {
                    statement.metric_names
                };
                let job = repository
                    .create(StatisticsJobCreate {
                        target: statement.target,
                        table_pin,
                        metric_names,
                        submitted_at_ms,
                    })
                    .await
                    .map_err(StatisticsApplicationError::repository)?;
                Ok(StatisticsStatementResult::JobSubmitted(job))
            }
            StatisticsStatement::ShowAnalyzeJobs(statement) => {
                let repository = self.repository()?;
                let mut jobs = repository
                    .list()
                    .await
                    .map_err(StatisticsApplicationError::repository)?;
                if let Some(target) = statement.target {
                    jobs.retain(|job| job.target == target);
                }
                Ok(StatisticsStatementResult::AnalyzeJobs(jobs))
            }
            StatisticsStatement::CancelAnalyze(statement) => {
                let repository = self.repository()?;
                repository
                    .request_cancel(statement.job_id, submitted_at_ms)
                    .await
                    .map(StatisticsStatementResult::JobCancellationRequested)
                    .map_err(StatisticsApplicationError::repository)
            }
            StatisticsStatement::ShowTableStats(statement) => table_statistics
                .show_table_stats(&statement.target)
                .map(StatisticsStatementResult::TableStats)
                .map_err(StatisticsApplicationError::table_statistics),
        }
    }

    fn repository(&self) -> Result<&StatisticsJobRepository, StatisticsApplicationError> {
        self.repository
            .as_ref()
            .ok_or_else(StatisticsApplicationError::state_store_required)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsApplicationErrorKind {
    StateStoreRequired,
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
    fn state_store_required() -> Self {
        Self {
            kind: StatisticsApplicationErrorKind::StateStoreRequired,
            message: "statistics job commands require a configured frontend StateStore".into(),
        }
    }

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

    fn target_resolution(error: String) -> Self {
        Self {
            kind: StatisticsApplicationErrorKind::TargetResolution,
            message: error,
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

/// Adapter installed by the frontend composition root.  Core owns command
/// parsing while this service owns durable StateStore access; no SQL text is
/// accepted across this boundary.
pub struct FrontendStatisticsApplicationPort {
    service: StatisticsApplicationService,
    table_statistics: std::sync::RwLock<Option<std::sync::Arc<dyn TableStatisticsReader>>>,
    runtime: tokio::runtime::Handle,
    attempt_executor: Mutex<Option<Arc<dyn StatisticsAttemptExecutor>>>,
    worker: Mutex<Option<StatisticsAnalyzeWorker>>,
}

impl FrontendStatisticsApplicationPort {
    pub fn new(service: StatisticsApplicationService, runtime: tokio::runtime::Handle) -> Self {
        Self {
            service,
            table_statistics: std::sync::RwLock::new(None),
            runtime,
            attempt_executor: Mutex::new(None),
            worker: Mutex::new(None),
        }
    }

    /// Called only after Core has opened its pin-aware statistics reader.
    /// Rebinding would permit a stale engine to replace the active reader, so
    /// reject it rather than silently changing a live application boundary.
    pub fn bind_table_statistics_reader(
        &self,
        reader: std::sync::Arc<dyn TableStatisticsReader>,
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

    /// Called after Core has opened its connector-control registry. Rebinding
    /// would permit a different engine generation to change ANALYZE target
    /// resolution while jobs are live, so fail rather than silently replace it.
    pub fn bind_core_statistics_target_resolver(
        &self,
        resolver: std::sync::Arc<dyn core_application::StatisticsTargetResolver>,
    ) -> Result<(), String> {
        self.service.bind_target_resolver(std::sync::Arc::new(
            CoreStatisticsTargetResolverAdapter { inner: resolver },
        ))
    }

    fn bind_core_statistics_attempt_executor(
        &self,
        executor: Arc<dyn core_application::StatisticsAttemptExecutor>,
    ) -> Result<(), String> {
        let Some(repository) = self.service.worker_repository() else {
            // SHOW TABLE STATS remains available without StateStore, but a
            // durable job worker must never fall back to an in-memory table.
            return Ok(());
        };
        let adapter: Arc<dyn StatisticsAttemptExecutor> =
            Arc::new(CoreStatisticsAttemptAdapter { inner: executor });
        let mut executor_slot = self
            .attempt_executor
            .lock()
            .map_err(|_| "statistics attempt executor lock poisoned".to_string())?;
        if executor_slot.is_some() {
            return Err("statistics attempt executor is already bound".to_string());
        }
        let worker = tokio::task::block_in_place(|| {
            self.runtime.block_on(StatisticsAnalyzeWorker::start(
                &self.runtime,
                Arc::new(repository),
                Arc::clone(&adapter),
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
        let worker = self
            .worker
            .lock()
            .map_err(|_| "statistics worker lock poisoned".to_string())?
            .take();
        if let Some(mut worker) = worker {
            worker.shutdown()?;
        }
        self.attempt_executor
            .lock()
            .map_err(|_| "statistics attempt executor lock poisoned".to_string())?
            .take();
        Ok(())
    }
}

impl core_application::StatisticsApplicationPort for FrontendStatisticsApplicationPort {
    fn execute(
        &self,
        command: core_application::StatisticsApplicationCommand,
    ) -> Result<
        core_application::StatisticsApplicationResult,
        core_application::StatisticsApplicationError,
    > {
        let statement = match command {
            core_application::StatisticsApplicationCommand::AnalyzeTable { target, columns } => {
                StatisticsStatement::AnalyzeTable(AnalyzeTableStatement {
                    target: target.into(),
                    metric_names: columns,
                })
            }
            core_application::StatisticsApplicationCommand::ShowAnalyzeJobs => {
                StatisticsStatement::ShowAnalyzeJobs(ShowAnalyzeJobsStatement { target: None })
            }
            core_application::StatisticsApplicationCommand::CancelAnalyze { job_id } => {
                StatisticsStatement::CancelAnalyze(CancelAnalyzeStatement { job_id })
            }
            core_application::StatisticsApplicationCommand::ShowTableStats { target } => {
                StatisticsStatement::ShowTableStats(ShowTableStatsStatement {
                    target: target.into(),
                })
            }
        };
        let submitted_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| core_application::StatisticsApplicationError::new(error.to_string()))?
            .as_millis()
            .try_into()
            .map_err(|_| {
                core_application::StatisticsApplicationError::new(
                    "statistics submission timestamp overflow",
                )
            })?;
        let reader = self
            .table_statistics
            .read()
            .map_err(|_| {
                core_application::StatisticsApplicationError::new(
                    "statistics table reader lock poisoned",
                )
            })?
            .clone();
        let reader: std::sync::Arc<dyn TableStatisticsReader> =
            reader.unwrap_or_else(|| std::sync::Arc::new(UnboundTableStatisticsReader));
        let result = tokio::task::block_in_place(|| {
            self.runtime.block_on(
                self.service
                    .execute(statement, submitted_at_ms, reader.as_ref()),
            )
        })
        .map_err(|error| core_application::StatisticsApplicationError::new(error.to_string()))?;
        if matches!(result, StatisticsStatementResult::JobSubmitted(_)) {
            if let Ok(worker) = self.worker.lock() {
                if let Some(worker) = worker.as_ref() {
                    worker.wakeup();
                }
            }
        }
        Ok(map_core_result(result))
    }
}

impl core_application::StatisticsTargetResolverSink for FrontendStatisticsApplicationPort {
    fn bind_statistics_target_resolver(
        &self,
        resolver: std::sync::Arc<dyn core_application::StatisticsTargetResolver>,
    ) -> Result<(), String> {
        self.bind_core_statistics_target_resolver(resolver)
    }
}

impl core_application::StatisticsTableReaderSink for FrontendStatisticsApplicationPort {
    fn bind_statistics_table_reader(
        &self,
        reader: std::sync::Arc<dyn core_application::StatisticsTableReader>,
    ) -> Result<(), String> {
        self.bind_table_statistics_reader(std::sync::Arc::new(CoreStatisticsTableReaderAdapter {
            inner: reader,
        }))
    }
}

impl core_application::StatisticsAttemptExecutorSink for FrontendStatisticsApplicationPort {
    fn bind_statistics_attempt_executor(
        &self,
        executor: Arc<dyn core_application::StatisticsAttemptExecutor>,
    ) -> Result<(), String> {
        self.bind_core_statistics_attempt_executor(executor)
    }
}

struct CoreCollectedAttempt {
    inner: Box<dyn core_application::StatisticsCollectedAttempt>,
}

impl StatisticsCollectedAttempt for CoreCollectedAttempt {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct CoreStatisticsAttemptAdapter {
    inner: Arc<dyn core_application::StatisticsAttemptExecutor>,
}

impl CoreStatisticsAttemptAdapter {
    fn request(job: &StatisticsJob) -> core_application::StatisticsAttemptRequest {
        core_application::StatisticsAttemptRequest {
            operation_id: job.operation_id,
            table_pin: core_application::StatisticsTablePin {
                connector_instance_id: job.table_pin.connector_instance_id.clone(),
                table_handle: job.table_pin.table_handle.clone(),
                data_version: job.table_pin.data_version.clone(),
                columns: job.table_pin.columns.clone(),
            },
            metric_names: job.metric_names.clone(),
        }
    }

    fn collected<'a>(
        collected: &'a dyn StatisticsCollectedAttempt,
    ) -> Result<&'a dyn core_application::StatisticsCollectedAttempt, StatisticsAttemptError> {
        collected
            .as_any()
            .downcast_ref::<CoreCollectedAttempt>()
            .map(|collected| collected.inner.as_ref())
            .ok_or_else(|| {
                StatisticsAttemptError::permanent(
                    super::model::StatisticsJobErrorKind::Internal,
                    "statistics worker received a collection artifact from another executor",
                )
            })
    }

    fn map_error(error: core_application::StatisticsApplicationError) -> StatisticsAttemptError {
        if error.requires_reconcile() {
            StatisticsAttemptError::reconcile(
                super::model::StatisticsJobErrorKind::Publish,
                error.to_string(),
            )
        } else if error.retryable() {
            StatisticsAttemptError::transient(
                super::model::StatisticsJobErrorKind::Connector,
                error.to_string(),
            )
        } else {
            StatisticsAttemptError::permanent(
                super::model::StatisticsJobErrorKind::Connector,
                error.to_string(),
            )
        }
    }
}

impl StatisticsAttemptExecutor for CoreStatisticsAttemptAdapter {
    fn collect(
        &self,
        job: &StatisticsJob,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError> {
        self.inner
            .collect(&Self::request(job))
            .map(|inner| {
                Box::new(CoreCollectedAttempt { inner }) as Box<dyn StatisticsCollectedAttempt>
            })
            .map_err(Self::map_error)
    }

    fn prepare_publish(
        &self,
        job: &StatisticsJob,
        collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<novarocks_spi::connector::ExternalMutationEvidence, StatisticsAttemptError> {
        self.inner
            .prepare_publish(&Self::request(job), Self::collected(collected)?)
            .map_err(Self::map_error)
    }

    fn publish(
        &self,
        job: &StatisticsJob,
        collected: &dyn StatisticsCollectedAttempt,
        evidence: &novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        self.inner
            .publish(&Self::request(job), Self::collected(collected)?, evidence)
            .map_err(Self::map_error)
    }

    fn reconcile(
        &self,
        job: &StatisticsJob,
        evidence: &novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        let _ = job;
        self.inner.reconcile(evidence).map_err(Self::map_error)
    }
}

struct UnboundTableStatisticsReader;

impl TableStatisticsReader for UnboundTableStatisticsReader {
    fn show_table_stats(
        &self,
        _target: &StatisticsJobTarget,
    ) -> Result<Vec<StatisticsTableStatRow>, String> {
        Err("SHOW TABLE STATS is unavailable until the Core statistics reader is bound".into())
    }
}

impl From<core_application::StatisticsTableTarget> for StatisticsJobTarget {
    fn from(value: core_application::StatisticsTableTarget) -> Self {
        Self {
            catalog: value.catalog,
            namespace: value.namespace,
            table: value.table,
        }
    }
}

fn map_core_result(
    result: StatisticsStatementResult,
) -> core_application::StatisticsApplicationResult {
    match result {
        StatisticsStatementResult::JobSubmitted(job) => {
            core_application::StatisticsApplicationResult::JobSubmitted(job_view(job))
        }
        StatisticsStatementResult::JobCancellationRequested(job) => {
            core_application::StatisticsApplicationResult::JobCancellationRequested(job_view(job))
        }
        StatisticsStatementResult::AnalyzeJobs(jobs) => {
            core_application::StatisticsApplicationResult::AnalyzeJobs(
                jobs.into_iter().map(job_view).collect(),
            )
        }
        StatisticsStatementResult::TableStats(rows) => {
            core_application::StatisticsApplicationResult::TableStats(
                rows.into_iter()
                    .map(|row| core_application::StatisticsTableStatView {
                        metric: row.metric_name,
                        value: row.value,
                        status: row.status,
                    })
                    .collect(),
            )
        }
    }
}

fn job_view(job: StatisticsJob) -> core_application::StatisticsJobView {
    core_application::StatisticsJobView {
        job_id: job.job_id,
        operation_id: job.operation_id,
        state: format!("{:?}", job.state).to_ascii_uppercase(),
        attempt: job.attempt,
        target: core_application::StatisticsTableTarget {
            catalog: job.target.catalog,
            namespace: job.target.namespace,
            table: job.target.table,
        },
    }
}
