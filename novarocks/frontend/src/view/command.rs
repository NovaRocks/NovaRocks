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

//! Frontend-owned command executor for external-view statements.

use crate::query_execution::kernels::ViewExecutionKernel;
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_query_application::view::{ViewRequestContext, ViewStatementResult};
use novarocks_spi::connector::ConnectorRequestContext;

#[derive(Clone)]
pub(crate) struct ViewCommandExecutor {
    kernel: ViewExecutionKernel,
}

impl ViewCommandExecutor {
    pub(crate) fn new(kernel: ViewExecutionKernel) -> Self {
        Self { kernel }
    }

    /// Execute a View command already admitted by `novarocks-parser`.
    pub(crate) fn execute(
        &self,
        statement: &novarocks_parser::ast::ViewStatement,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        self.kernel
            .view_service()
            .execute_statement(
                &crate::view::engine::FrontendViewEngine::new(self.kernel.clone()),
                statement,
                ViewRequestContext {
                    current_catalog,
                    current_database,
                    connector_context: Some(connector_context),
                },
            )
            .map(|result| match result {
                ViewStatementResult::Ok => StatementResult::Ok,
                ViewStatementResult::Query(result) => StatementResult::Query(result),
            })
    }

    /// Removes view metadata when typed catalog admission drops a database.
    pub(crate) fn drop_database(&self, catalog: &str, database: &str) -> Result<(), String> {
        self.kernel.view_service().drop_database(catalog, database)
    }
}
