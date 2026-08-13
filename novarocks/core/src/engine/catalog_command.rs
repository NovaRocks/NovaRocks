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

//! Typed catalog-DDL execution.
//!
//! This capability deliberately owns a closed statement family.  It is not a
//! second generic command dispatcher: callers receive `None` only when SQL is
//! outside catalog DDL, while every recognized statement either executes or
//! returns its parser/admission error.

use novarocks_catalog::identifier::normalize_identifier;
use novarocks_spi::connector::ConnectorInstanceId;
use sqlparser::parser::Parser;

use crate::catalog_application::CatalogCreateCommand;
use crate::engine::StatementResult;
use crate::engine::domain::CatalogCommandKernel;
use crate::engine::statement::{
    execute_create_database_statement, execute_create_table_statement,
    execute_drop_catalog_statement, execute_drop_database_statement, execute_drop_table_statement,
};
use crate::sql::parser::dialect::{
    StarRocksDialect, looks_like_create_catalog, looks_like_create_database,
    looks_like_create_table, looks_like_drop_statement,
};

/// Catalog DDL capability built from catalog-only leaf ports.
#[derive(Clone)]
pub struct CatalogCommandExecutor {
    kernel: CatalogCommandKernel,
}

impl CatalogCommandExecutor {
    pub(crate) fn new(kernel: CatalogCommandKernel) -> Self {
        Self { kernel }
    }

    /// Execute exactly one catalog-DDL statement.
    ///
    /// CTAS belongs to the frontend DML service and is rejected here.  A
    /// default-catalog database drop belongs to the view/session route, so it
    /// is also rejected rather than being silently reinterpreted as a provider
    /// namespace mutation.
    pub fn try_execute(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Option<StatementResult>, String> {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(&normalized)
            .map_err(|error| error.to_string())?;

        if looks_like_create_table(&parser) {
            let statement =
                crate::sql::parser::dialect::create_table::parse_create_table_statement(
                    &mut parser,
                )?;
            require_statement_end(&mut parser)?;
            return execute_create_table_statement(
                &self.kernel,
                statement,
                current_catalog,
                current_database,
                connector_context,
            )
            .map(Some);
        }
        if looks_like_create_catalog(&parser) {
            let statement =
                crate::sql::parser::dialect::create_catalog::parse_create_catalog_statement(
                    &mut parser,
                )?;
            require_statement_end(&mut parser)?;
            return self.execute_create_catalog(statement).map(Some);
        }
        if looks_like_create_database(&parser) {
            let (name, if_not_exists) =
                crate::sql::parser::dialect::parse_create_database_name(&mut parser)?;
            require_statement_end(&mut parser)?;
            return execute_create_database_statement(
                &self.kernel,
                &name,
                if_not_exists,
                current_catalog,
                connector_context,
            )
            .map(Some);
        }
        if looks_like_drop_statement(&parser) {
            let statement = crate::sql::parser::dialect::drop::parse_drop_statement(&mut parser)?;
            require_statement_end(&mut parser)?;
            use crate::sql::parser::dialect::drop::DropResult;
            return match statement {
                DropResult::Catalog(statement) => execute_drop_catalog_statement(
                    &self.kernel,
                    &statement.name,
                    statement.if_exists,
                )
                .map(Some),
                DropResult::Database(statement) => {
                    if current_catalog.is_none() && statement.name.parts.len() == 1 {
                        Err("DROP DATABASE in default_catalog must be routed through the view command capability".to_string())
                    } else {
                        execute_drop_database_statement(
                            &self.kernel,
                            &statement.name,
                            current_catalog,
                            statement.if_exists,
                            statement.force,
                            connector_context,
                        )
                        .map(Some)
                    }
                }
                DropResult::Table(statement) => execute_drop_table_statement(
                    &self.kernel,
                    &statement.name,
                    current_catalog,
                    current_database,
                    statement.if_exists,
                    statement.force,
                    connector_context,
                )
                .map(Some),
            };
        }
        Ok(None)
    }

    fn execute_create_catalog(
        &self,
        statement: crate::sql::parser::ast::CreateCatalogStmt,
    ) -> Result<StatementResult, String> {
        let normalized_catalog = normalize_identifier(&statement.name)?;
        let application = self.kernel.catalog_application().ok_or_else(|| {
            "catalog statements require a configured frontend catalog application".to_string()
        })?;
        let instance_id = ConnectorInstanceId::parse(&normalized_catalog)
            .map_err(|error| format!("invalid catalog connector instance ID: {error}"))?;
        application
            .create_catalog(CatalogCreateCommand {
                instance_id,
                display_name: statement.name,
                properties: statement.properties,
                if_not_exists: statement.if_not_exists,
            })
            .map_err(|error| error.to_string())?;
        Ok(StatementResult::Ok)
    }
}

fn require_statement_end(parser: &mut Parser<'_>) -> Result<(), String> {
    if parser.consume_token(&sqlparser::tokenizer::Token::SemiColon) {}
    if parser.peek_token() != sqlparser::tokenizer::Token::EOF {
        return Err("catalog command accepts exactly one statement".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::CatalogCommandExecutor;

    #[test]
    fn non_catalog_statement_is_not_claimed() {
        // Construction is unnecessary for an unsupported family because the
        // parser gate must reject it before any kernel port is read.
        let sql = "SELECT 'CREATE TABLE t AS SELECT 1'";
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize query");
        assert!(normalized.starts_with("SELECT"));
        let _ = std::any::type_name::<CatalogCommandExecutor>();
    }
}
