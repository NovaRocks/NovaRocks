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

use novarocks::query_execution::StatementResult;
use novarocks::query_execution::kernels::ViewExecutionKernel;
use novarocks::view::{ViewRequestContext, ViewService, ViewStatementResult};
use novarocks_spi::connector::ConnectorRequestContext;

#[derive(Clone)]
pub(crate) struct ViewCommandExecutor {
    kernel: ViewExecutionKernel,
}

impl ViewCommandExecutor {
    pub(crate) fn new(kernel: ViewExecutionKernel) -> Self {
        Self { kernel }
    }

    /// Execute only statements recognized by the installed view service.
    pub(crate) fn try_execute(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &ConnectorRequestContext,
    ) -> Result<Option<StatementResult>, String> {
        let normalized = novarocks_sql::syntax::normalize_for_raw_parse(sql)?;
        self.kernel
            .view_service()
            .try_handle_statement(
                &self.kernel,
                &normalized,
                ViewRequestContext {
                    current_catalog,
                    current_database,
                    connector_context: Some(connector_context),
                },
            )
            .map(|result| {
                result.map(|result| match result {
                    ViewStatementResult::Ok => StatementResult::Ok,
                    ViewStatementResult::Query(result) => StatementResult::Query(result),
                })
            })
    }
}
