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

use crate::statistics::application::{
    StatisticsApplicationCommand, StatisticsApplicationPort, StatisticsColumnIntent,
    StatisticsTableTarget,
};
use novarocks_sql::semantic::StatisticsSqlCommand;
use novarocks_types::naming::normalize_identifier;
use std::sync::Arc;
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

impl StatisticsCommandExecutor {
    pub fn new(application: Arc<dyn StatisticsApplicationPort>) -> Self {
        Self { application }
    }
    pub(crate) fn prepare_statement_action(
        &self,
        command: &StatisticsSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        context: &novarocks_spi::connector::ConnectorRequestContext,
        family: novarocks_result_contract::ClientPresentationFamily,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        novarocks_query_application::api::CommandError,
    > {
        self.application
            .prepare_statement_action(
                lower_application_command(command, current_catalog, current_database)?,
                execution,
                context,
                family,
            )
            .map_err(crate::statistics::application::StatisticsApplicationError::into_command_error)
    }
}
fn lower_application_command(
    command: &StatisticsSqlCommand,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatisticsApplicationCommand, String> {
    Ok(match command {
        StatisticsSqlCommand::AnalyzeTable { table, columns } => {
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
        StatisticsSqlCommand::ShowStats { table } => StatisticsApplicationCommand::ShowStats {
            target: statistics_application_target(&table.parts, current_catalog, current_database)?,
        },
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn statistics_commands_keep_session_target_and_column_intent() {
        let statement = novarocks_query_application::sql::parse_single_statement(
            "ANALYZE TABLE orders (order_id)",
        )
        .unwrap();
        let Some(novarocks_query_application::sql::ProductSqlCommand::Statistics(command)) =
            novarocks_query_application::sql::lower_product_sql_command(&statement).unwrap()
        else {
            panic!("statistics command")
        };
        assert_eq!(
            lower_application_command(&command, Some("ice"), "analytics").unwrap(),
            StatisticsApplicationCommand::AnalyzeTable {
                target: StatisticsTableTarget {
                    catalog: "ice".into(),
                    namespace: "analytics".into(),
                    table: "orders".into()
                },
                columns: StatisticsColumnIntent::Explicit(vec!["order_id".into()])
            }
        );
    }
}
