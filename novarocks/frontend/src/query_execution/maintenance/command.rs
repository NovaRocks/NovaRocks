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

//! Closed table-maintenance command capabilities.

use std::sync::Arc;

use super::{
    MaintenanceRequestContext, MaintenanceStatementResult, RequestScopedMaintenanceEngine,
    TableMaintenanceService,
};
use crate::runtime::statement_result::StatementResult;

/// Foreground maintenance command capability.  Each invocation creates a
/// short-lived engine carrying only the Frontend-composed maintenance ports
/// and the already-admitted request execution.  It cannot use the legacy
/// Core application façade or manufacture a topology/cancellation fallback.
#[derive(Clone)]
pub struct MaintenanceCommandExecutor {
    kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
}

impl MaintenanceCommandExecutor {
    pub fn new(kernel: crate::query_execution::kernels::MaintenanceExecutionKernel) -> Self {
        Self { kernel }
    }

    /// Executes one parser-admitted maintenance write without reparsing SQL.
    pub fn execute(
        &self,
        statement: &novarocks_parser::ast::MaintenanceStatement,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &crate::common::admitted_query_context::QueryExecutionContext,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let lowered = crate::table_maintenance::lower_typed_maintenance_statement(
            statement,
            MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
        )?;
        let engine = RequestScopedMaintenanceEngine::new(
            self.kernel.clone(),
            execution.clone(),
            connector_context.clone(),
        );
        // The maintenance domain contract is async, because its durable work is
        // I/O. This call site is not: every SQL statement runs inside the
        // `spawn_blocking` closure in `query.rs`, which is where the foreground
        // statement lease, timeout and cancellation are anchored.
        //
        // Hoisting the command branch out of that closure means restructuring
        // statement admission itself, which belongs to the query-application and
        // query-preparation work lines, not here. So the adapter sits at this
        // edge — the SQL routing boundary another work line owns — and never in
        // a domain contract. It deletes when that closure is lifted.
        let spark_call = crate::table_maintenance::is_typed_spark_maintenance_call(statement);
        let service = self.kernel.service();
        let context = MaintenanceRequestContext {
            current_catalog,
            current_database,
        };
        let outcome = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(service.handle_typed_statement(&engine, lowered, spark_call, context))
        });
        outcome.map(statement_result)
    }
}

#[derive(Clone)]
pub struct MaintenanceReadCommandExecutor {
    service: Arc<dyn TableMaintenanceService>,
}

impl MaintenanceReadCommandExecutor {
    pub fn new(service: Arc<dyn TableMaintenanceService>) -> Self {
        Self { service }
    }

    /// Executes a parser-admitted `SHOW ALTER TABLE OPTIMIZE` presentation
    /// without recreating a raw parser or a maintenance engine.
    pub fn execute(
        &self,
        statement: &novarocks_parser::ast::ShowAlterTableOptimize,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        self.service
            .handle_typed_show_optimize(
                crate::table_maintenance::lower_typed_show_optimize(statement)?,
                MaintenanceRequestContext {
                    current_catalog,
                    current_database,
                },
            )
            .map(statement_result)
    }
}

fn statement_result(result: MaintenanceStatementResult) -> StatementResult {
    match result {
        MaintenanceStatementResult::Ok => StatementResult::Ok,
        MaintenanceStatementResult::Query(result) => StatementResult::Query(result),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        MaintenanceActionOutcome, MaintenanceActionRequest, OptimizeSubmission,
        TableMaintenanceEngine,
    };
    use super::*;
    use std::sync::Arc;

    struct ReadOnlyService {
        called: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl TableMaintenanceService for ReadOnlyService {
        fn start(&self, _engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
            Ok(())
        }

        fn handle_typed_show_optimize(
            &self,
            _statement: crate::table_maintenance::ParsedShowOptimize,
            _context: MaintenanceRequestContext<'_>,
        ) -> Result<MaintenanceStatementResult, String> {
            self.called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(MaintenanceStatementResult::Ok)
        }

        async fn execute_automatic_action(
            &self,
            _engine: &dyn TableMaintenanceEngine,
            _request: MaintenanceActionRequest,
        ) -> Result<MaintenanceActionOutcome, String> {
            Err("not used".to_string())
        }

        fn submit_automatic_optimize(
            &self,
            _engine: &dyn TableMaintenanceEngine,
            _target: crate::maintenance::MaintenanceTarget,
        ) -> Result<OptimizeSubmission, String> {
            Err("not used".to_string())
        }

        async fn shutdown_until(&self, _deadline: std::time::Instant) -> Result<(), String> {
            Ok(())
        }

        fn request_shutdown_for_process_exit(&self) {}
    }

    #[test]
    fn show_optimize_uses_read_only_service_without_engine() {
        let service = Arc::new(ReadOnlyService {
            called: std::sync::atomic::AtomicBool::new(false),
        });
        let executor = MaintenanceReadCommandExecutor::new(Arc::clone(&service) as Arc<_>);
        let statements =
            novarocks_parser::parse("SHOW ALTER TABLE OPTIMIZE").expect("parser statement");
        let [
            novarocks_parser::ast::Statement::Maintenance(
                novarocks_parser::ast::MaintenanceStatement::ShowOptimize(statement),
            ),
        ] = statements.as_slice()
        else {
            panic!("expected SHOW ALTER TABLE OPTIMIZE");
        };
        let result = executor
            .execute(statement, Some("ice"), "db")
            .expect("execute");
        assert!(matches!(result, StatementResult::Ok));
        assert!(service.called.load(std::sync::atomic::Ordering::SeqCst));
    }
}
