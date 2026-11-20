// SPDX-License-Identifier: Apache-2.0

use super::property_cpu_context::PropertyCpuContext;
use crate::query_execution::completion_facts::FrontendPlanningCpuSource;
use crate::statistics::application::StatisticsColumnIntent;
use novarocks_parser::ast::TrinoAnalyze;
use novarocks_query_application::{
    session_error::QueryServiceError, sql::trino_table_command::prepare_trino_analyze,
};
use novarocks_spi::connector::ConnectorStatisticsSession;
use novarocks_sql::semantic::StatisticsSqlCommand;
use std::sync::Arc;

pub(crate) struct AnalyzeProperties {
    pub(crate) source: Arc<str>,
    pub(crate) syntax: TrinoAnalyze,
    pub(crate) context: PropertyCpuContext,
    pub(crate) cpu: Arc<FrontendPlanningCpuSource>,
}

pub(crate) struct AnalyzePropertyCpuInput {
    pub(crate) source: Arc<str>,
    pub(crate) syntax: TrinoAnalyze,
    pub(crate) context: PropertyCpuContext,
    pub(crate) statistics: ConnectorStatisticsSession,
}

impl AnalyzePropertyCpuInput {
    pub(crate) fn run(self) -> Result<StatisticsColumnIntent, QueryServiceError> {
        let declarations = self
            .statistics
            .analyze_property_declarations()
            .map_err(|error| {
                QueryServiceError::from_failure_cause(
                    novarocks_failure_contract::SharedFailureCause::new(error),
                )
            })?;
        let mut constants = self.context.constant_port();
        let StatisticsSqlCommand::AnalyzeTable { columns, .. } = prepare_trino_analyze(
            &self.source,
            &self.syntax,
            &declarations,
            &mut constants,
            &self.context.control,
        )?
        else {
            unreachable!("ANALYZE prepares a single statistics column intent");
        };
        Ok(match columns {
            None => StatisticsColumnIntent::AllColumns,
            Some(columns) => StatisticsColumnIntent::Explicit(columns),
        })
    }
}
