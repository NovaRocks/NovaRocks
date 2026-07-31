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

use uuid::Uuid;

use super::model::{StatisticsJob, StatisticsJobCreate, StatisticsJobTarget};
use super::repository::{StatisticsJobRepository, StatisticsJobRepositoryError};

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

#[derive(Clone)]
pub struct StatisticsApplicationService {
    repository: Option<StatisticsJobRepository>,
}

impl StatisticsApplicationService {
    pub const fn unavailable() -> Self {
        Self { repository: None }
    }

    pub fn with_repository(repository: StatisticsJobRepository) -> Self {
        Self {
            repository: Some(repository),
        }
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
                let job = repository
                    .create(StatisticsJobCreate {
                        target: statement.target,
                        metric_names: statement.metric_names,
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
            StatisticsStatement::CancelAnalyze(_) => {
                // Cancellation must be routed to the active fenced worker; a
                // session cannot mutate a durable job directly.
                self.repository()?;
                Err(StatisticsApplicationError::worker_required())
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
    WorkerRequired,
    Repository,
    TableStatistics,
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

    fn worker_required() -> Self {
        Self {
            kind: StatisticsApplicationErrorKind::WorkerRequired,
            message: "CANCEL ANALYZE must be executed by the active fenced statistics worker"
                .into(),
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
