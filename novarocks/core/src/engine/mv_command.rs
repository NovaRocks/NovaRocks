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

//! Closed typed executor for the non-refresh Iceberg MV statement family.

use crate::engine::StatementResult;
use crate::engine::domain::MvExecutionKernel;

#[derive(Clone)]
pub struct MvCommandExecutor {
    kernel: MvExecutionKernel,
}

impl MvCommandExecutor {
    pub(crate) fn new(kernel: MvExecutionKernel) -> Self {
        Self { kernel }
    }

    /// Execute CREATE, DROP and SHOW MATERIALIZED VIEWS through the injected
    /// provider backend. Refresh and repartition intentionally remain outside
    /// this closed family until their request-frozen frontend lifecycle has an
    /// equally explicit capability.
    pub fn try_execute(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Option<StatementResult>, String> {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        let mut statements = match crate::sql::parser::parse_sql(&normalized) {
            Ok(statements) => statements,
            Err(_) => return Ok(None),
        };
        if statements.len() != 1 {
            return Err("MV command accepts exactly one statement".to_string());
        }
        match statements.pop().expect("one checked statement") {
            crate::sql::parser::ast::Statement::CreateMaterializedView(statement) => {
                crate::engine::mv_flow::create_mv_with_kernel(
                    &self.kernel,
                    current_catalog,
                    current_database,
                    &statement,
                    connector_context,
                )
                .map(Some)
            }
            crate::sql::parser::ast::Statement::DropMaterializedView(statement) => {
                crate::engine::mv_flow::drop_mv_with_kernel(
                    &self.kernel,
                    current_catalog,
                    current_database,
                    &statement,
                    connector_context,
                )
                .map(Some)
            }
            crate::sql::parser::ast::Statement::ShowMaterializedViews(statement) => {
                crate::engine::mv_flow::list_mvs_with_kernel(
                    &self.kernel,
                    current_catalog,
                    &statement,
                )
                .map(Some)
            }
            _ => Ok(None),
        }
    }
}
