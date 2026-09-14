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

//! Frontend-owned typed executor for durable statistics commands.

use std::sync::Arc;

use crate::statistics_jobs::application::{
    StatisticsApplicationCommand, StatisticsApplicationPort, StatisticsApplicationResult,
    StatisticsColumnIntent, StatisticsTableTarget,
};
use novarocks_query_application::api::build_nullable_utf8_query_result;
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_sql::semantic::StatisticsSqlCommand;
use novarocks_sql::semantic::command::AnalyzeModeSql;
use novarocks_types::naming::normalize_identifier;

#[derive(Clone)]
pub struct StatisticsCommandExecutor {
    application: Arc<dyn StatisticsApplicationPort>,
}

fn statistics_application_target(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatisticsTableTarget, String> {
    let default_catalog = current_catalog.unwrap_or("default_catalog");
    let (catalog, namespace, table) = match parts {
        [table] => (default_catalog, current_database, table.as_str()),
        [namespace, table] => (default_catalog, namespace.as_str(), table.as_str()),
        [catalog, namespace, table] => (catalog.as_str(), namespace.as_str(), table.as_str()),
        _ => {
            return Err(format!(
                "statistics table name must be table, db.table, or catalog.db.table: {}",
                parts.join(".")
            ));
        }
    };
    Ok(StatisticsTableTarget {
        catalog: normalize_identifier(catalog)?,
        namespace: normalize_identifier(namespace)?,
        table: normalize_identifier(table)?,
    })
}

fn statistics_application_result(
    result: StatisticsApplicationResult,
) -> Result<StatementResult, String> {
    match result {
        StatisticsApplicationResult::JobSubmitted(_)
        | StatisticsApplicationResult::JobCancellationRequested(_) => Ok(StatementResult::Ok),
        StatisticsApplicationResult::AnalyzeJobs(jobs) => statistics_string_result(
            &[
                "job_id",
                "operation_id",
                "state",
                "attempt",
                "catalog",
                "namespace",
                "table",
                "error_kind",
                "error_message",
            ],
            jobs.into_iter()
                .map(|job| {
                    vec![
                        Some(job.job_id.to_string()),
                        Some(job.operation_id.to_string()),
                        Some(job.state),
                        Some(job.attempt.to_string()),
                        Some(job.target.catalog),
                        Some(job.target.namespace),
                        Some(job.target.table),
                        job.error_kind,
                        job.error_message,
                    ]
                })
                .collect(),
        ),
        StatisticsApplicationResult::TableStats(rows) => statistics_string_result(
            &[
                "metric",
                "value",
                "status",
                "basis_version",
                "source",
                "numeric_nature",
                "basis_relation",
            ],
            rows.into_iter()
                .map(|row| {
                    vec![
                        Some(row.metric),
                        row.value,
                        Some(row.status),
                        Some(row.basis_version),
                        Some(row.source),
                        Some(row.numeric_nature),
                        Some(row.basis_relation),
                    ]
                })
                .collect(),
        ),
    }
}

fn statistics_string_result(
    names: &[&str],
    rows: Vec<Vec<Option<String>>>,
) -> Result<StatementResult, String> {
    if rows.iter().any(|row| row.len() != names.len()) {
        return Err("statistics application returned malformed tabular result".to_owned());
    }
    build_nullable_utf8_query_result(names, rows).map(StatementResult::Query)
}

impl StatisticsCommandExecutor {
    pub fn new(application: Arc<dyn StatisticsApplicationPort>) -> Self {
        Self { application }
    }

    /// Executes the complete semantic command admitted by Query Application.
    ///
    /// This adapter receives no parser AST and cannot reparse source SQL. It
    /// owns only the role-local projection to the statistics product port.
    pub fn execute_command(
        &self,
        command: &StatisticsSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: Option<
            &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        >,
    ) -> Result<StatementResult, String> {
        let command = match command {
            StatisticsSqlCommand::AnalyzeTable {
                mode,
                table,
                columns,
                with_sync_mode,
            } => {
                if *mode != AnalyzeModeSql::Default || *with_sync_mode {
                    return Err(
                        "ANALYZE mode and sync options are not supported by the statistics application"
                            .to_string(),
                    );
                }
                StatisticsApplicationCommand::AnalyzeTable {
                    target: statistics_application_target(
                        &table.parts,
                        current_catalog,
                        current_database,
                    )?,
                    columns: if columns.is_empty() {
                        StatisticsColumnIntent::AllColumns
                    } else {
                        StatisticsColumnIntent::Explicit(columns.clone())
                    },
                }
            }
            StatisticsSqlCommand::ShowAnalyzeJobs => StatisticsApplicationCommand::ShowAnalyzeJobs,
            StatisticsSqlCommand::CancelAnalyze { job_id } => {
                StatisticsApplicationCommand::CancelAnalyze {
                    job_id: uuid::Uuid::parse_str(job_id)
                        .map_err(|error| format!("invalid ANALYZE job ID '{job_id}': {error}"))?,
                }
            }
            StatisticsSqlCommand::ShowTableStats { table } => {
                StatisticsApplicationCommand::ShowTableStats {
                    target: statistics_application_target(
                        &table.parts,
                        current_catalog,
                        current_database,
                    )?,
                }
            }
            StatisticsSqlCommand::ShowBasicStatsMeta
            | StatisticsSqlCommand::ShowHistogramStatsMeta
            | StatisticsSqlCommand::DropStats { .. }
            | StatisticsSqlCommand::DropHistogram { .. }
            | StatisticsSqlCommand::DropMultipleColumnsStats { .. } => {
                return Err(
                    "statistics command is not supported by the statistics application".to_string(),
                );
            }
        };
        self.application
            .execute(command, execution)
            .map_err(|error| error.to_string())
            .and_then(statistics_application_result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{Array, StringArray};
    use uuid::Uuid;

    use super::StatisticsCommandExecutor;
    use crate::statistics_jobs::application::{
        StatisticsApplicationCommand, StatisticsApplicationError, StatisticsApplicationPort,
        StatisticsApplicationResult, StatisticsJobView, StatisticsTableStatView,
        StatisticsTableTarget,
    };

    fn statistics_command(sql: &str) -> novarocks_sql::semantic::StatisticsSqlCommand {
        let statement = novarocks_query_application::sql::parse_single_statement(sql)
            .expect("parse statistics statement");
        let Some(novarocks_query_application::sql::ProductSqlCommand::Statistics(command)) =
            novarocks_query_application::sql::lower_product_sql_command(&statement)
                .expect("lower statistics statement")
        else {
            panic!("expected semantic statistics command");
        };
        command
    }

    #[derive(Default)]
    struct RecordingStatisticsApplicationPort {
        commands: Mutex<Vec<StatisticsApplicationCommand>>,
    }

    impl RecordingStatisticsApplicationPort {
        fn commands(&self) -> Vec<StatisticsApplicationCommand> {
            self.commands.lock().expect("statistics commands").clone()
        }
    }

    impl StatisticsApplicationPort for RecordingStatisticsApplicationPort {
        fn execute(
            &self,
            command: StatisticsApplicationCommand,
            _execution: Option<
                &novarocks_query_application::admitted_query_context::QueryExecutionContext,
            >,
        ) -> Result<StatisticsApplicationResult, StatisticsApplicationError> {
            self.commands
                .lock()
                .expect("statistics commands")
                .push(command.clone());
            match command {
                StatisticsApplicationCommand::AnalyzeTable { target, .. } => Ok(
                    StatisticsApplicationResult::JobSubmitted(StatisticsJobView {
                        job_id: Uuid::nil(),
                        operation_id: novarocks_spi::connector::LakePublicationId::new_v7(),
                        state: "SUBMITTED".into(),
                        attempt: 0,
                        target,
                        error_kind: None,
                        error_message: None,
                    }),
                ),
                StatisticsApplicationCommand::ShowAnalyzeJobs
                | StatisticsApplicationCommand::CancelAnalyze { .. } => {
                    Ok(StatisticsApplicationResult::AnalyzeJobs(Vec::new()))
                }
                StatisticsApplicationCommand::ShowTableStats { .. } => {
                    Ok(StatisticsApplicationResult::TableStats(vec![
                        StatisticsTableStatView {
                            metric: "row_count".into(),
                            value: Some("42".into()),
                            status: "AVAILABLE".into(),
                            basis_version: "SAME".into(),
                            source: "PROVIDER_ARTIFACT".into(),
                            numeric_nature: "EXACT".into(),
                            basis_relation: "IDENTICAL".into(),
                        },
                    ]))
                }
            }
        }
    }

    #[test]
    fn typed_statistics_statements_use_the_frontend_application_owner() {
        let port = Arc::new(RecordingStatisticsApplicationPort::default());
        let executor =
            StatisticsCommandExecutor::new(Arc::clone(&port) as Arc<dyn StatisticsApplicationPort>);

        let analyze = statistics_command("ANALYZE TABLE ice.analytics.orders (order_id)");
        assert!(
            executor
                .execute_command(&analyze, None, "default", None)
                .is_ok()
        );
        let show = statistics_command("SHOW TABLE STATS ice.analytics.orders");
        let show_stats = executor
            .execute_command(&show, None, "default", None)
            .expect("show typed table stats");
        let novarocks_query_application::protocol_delivery::QuerySessionOutput::Query(show_stats) =
            show_stats
        else {
            panic!("SHOW TABLE STATS must return a query result");
        };
        assert_eq!(show_stats.columns[0].name(), "metric");
        assert_eq!(show_stats.columns[1].name(), "value");
        let value = show_stats.batches[0].column(1);
        let value = value
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("statistics value string column");
        assert_eq!(value.value(0), "42");

        assert_eq!(
            port.commands(),
            vec![
                StatisticsApplicationCommand::AnalyzeTable {
                    target: StatisticsTableTarget {
                        catalog: "ice".into(),
                        namespace: "analytics".into(),
                        table: "orders".into(),
                    },
                    columns: crate::statistics_jobs::application::StatisticsColumnIntent::Explicit(
                        vec!["order_id".into()],
                    ),
                },
                StatisticsApplicationCommand::ShowTableStats {
                    target: StatisticsTableTarget {
                        catalog: "ice".into(),
                        namespace: "analytics".into(),
                        table: "orders".into(),
                    },
                },
            ]
        );
    }

    #[test]
    fn malformed_statistics_rows_keep_the_product_diagnostic() {
        let error = super::statistics_string_result(
            &["job_id", "state"],
            vec![vec![Some("job-1".to_owned())]],
        )
        .expect_err("statistics rows must match their declared columns");
        assert_eq!(
            error,
            "statistics application returned malformed tabular result"
        );
    }
}
