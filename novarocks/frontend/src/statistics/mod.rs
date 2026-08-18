pub mod command;
mod legacy;
mod model;
mod observation;
mod provider;
mod query;
mod statement;

use std::sync::RwLock;

use crate::runtime::query_result::QueryResult;
use novarocks::catalog_application::query_catalog::QueryCatalogService;

pub use legacy::{
    CatalogColumnStatistics, CatalogTableStatistics, StatisticsColumn, StatisticsInsertObservation,
    StatisticsInsertSource, StatisticsLiteral, StatisticsOverwriteMode, StatisticsRequestContext,
    StatisticsStatementResult,
};

use self::model::StatisticsState;

pub struct FrontendStatisticsService {
    state: RwLock<StatisticsState>,
}

impl FrontendStatisticsService {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(StatisticsState::default()),
        }
    }
}

impl Default for FrontendStatisticsService {
    fn default() -> Self {
        Self::new()
    }
}

impl FrontendStatisticsService {
    pub fn try_handle_statement(
        &self,
        catalog_service: &QueryCatalogService,
        sql: &str,
        context: StatisticsRequestContext<'_>,
    ) -> Result<Option<StatisticsStatementResult>, String> {
        statement::try_handle_statement(self, catalog_service, sql, context)
    }

    pub fn try_query(
        &self,
        sql: &str,
        query: &sqlparser::ast::Query,
        context: StatisticsRequestContext<'_>,
    ) -> Result<Option<QueryResult>, String> {
        query::try_query(self, sql, query, context.current_database)
    }

    pub fn observe_query(
        &self,
        query: &sqlparser::ast::Query,
        current_database: &str,
    ) -> Result<(), String> {
        observation::observe_query(self, query, current_database)
    }

    pub fn observe_insert(
        &self,
        observation: StatisticsInsertObservation<'_>,
        target_columns: Option<Vec<StatisticsColumn>>,
    ) -> Result<(), String> {
        let Some(target_columns) = target_columns else {
            return Ok(());
        };
        self::observation::observe_insert(self, observation, &target_columns)
    }

    pub fn observe_update(&self, sql: &str, current_database: &str) -> Result<(), String> {
        observation::observe_update(self, sql, current_database)
    }

    pub fn drop_table(&self, database: &str, table: &str) {
        observation::drop_table(self, database, table);
    }

    pub fn drop_database(&self, database: &str) {
        observation::drop_database(self, database);
    }

    pub fn catalog_table_statistics(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<CatalogTableStatistics>, String> {
        provider::catalog_table_statistics(self, database, table)
    }
}
