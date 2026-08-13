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

//! Closed typed executor for durable statistics commands.

use crate::engine::domain::StatisticsExecutionKernel;
use crate::engine::{
    StatementResult, statistics_application_result, statistics_application_target,
};

#[derive(Clone)]
pub struct StatisticsCommandExecutor {
    kernel: StatisticsExecutionKernel,
}

impl StatisticsCommandExecutor {
    pub(crate) fn new(kernel: StatisticsExecutionKernel) -> Self {
        Self { kernel }
    }

    pub fn try_execute(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<Option<StatementResult>, String> {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        let mut statements = match crate::sql::parser::parse_sql(&normalized) {
            Ok(statements) => statements,
            Err(_) => return Ok(None),
        };
        if statements.len() != 1 {
            return Err("statistics command accepts exactly one statement".to_string());
        }
        use crate::sql::parser::ast::Statement;
        let statement = statements.pop().expect("one checked statement");
        let command = match statement {
            Statement::AnalyzeTable(statement) => {
                crate::engine::statistics_application::StatisticsApplicationCommand::AnalyzeTable {
                    target: statistics_application_target(
                        &statement.name,
                        current_catalog,
                        current_database,
                    )?,
                    columns: statement.columns,
                }
            }
            Statement::ShowAnalyzeJobs(_) => {
                crate::engine::statistics_application::StatisticsApplicationCommand::ShowAnalyzeJobs
            }
            Statement::CancelAnalyze(statement) => {
                crate::engine::statistics_application::StatisticsApplicationCommand::CancelAnalyze {
                    job_id: uuid::Uuid::parse_str(&statement.job_id).map_err(|error| {
                        format!("invalid ANALYZE job ID '{}': {error}", statement.job_id)
                    })?,
                }
            }
            Statement::ShowTableStats(statement) => {
                crate::engine::statistics_application::StatisticsApplicationCommand::ShowTableStats {
                    target: statistics_application_target(
                        &statement.name,
                        current_catalog,
                        current_database,
                    )?,
                }
            }
            _ => return Ok(None),
        };
        self.kernel
            .statistics_application()
            .execute(command)
            .map_err(|error| error.to_string())
            .and_then(statistics_application_result)
            .map(Some)
    }
}
