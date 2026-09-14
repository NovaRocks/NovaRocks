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
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;

/// Foreground maintenance command capability.  Each invocation creates a
/// short-lived engine carrying only the Frontend-composed maintenance ports
/// and the already-admitted request execution.  It cannot use the legacy
/// Core application façade or manufacture a topology/cancellation fallback.
#[derive(Clone)]
pub struct MaintenanceCommandExecutor {
    kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
    runtime: tokio::runtime::Handle,
}

impl MaintenanceCommandExecutor {
    pub fn new(
        kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self { kernel, runtime }
    }

    /// Executes one Query-Application semantic maintenance command.
    ///
    /// The command has already been parser-admitted and lowered, so this
    /// adapter never receives source syntax or performs an AST round trip.
    pub fn execute_command(
        &self,
        command: &novarocks_sql::semantic::MaintenanceSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let lowered = crate::table_maintenance::lower_semantic_maintenance_statement(
            command,
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
        let spark_call = matches!(
            command,
            novarocks_sql::semantic::MaintenanceSqlCommand::Call { .. }
        );
        let service = self.kernel.service();
        let context = MaintenanceRequestContext {
            current_catalog,
            current_database,
        };
        self.runtime
            .block_on(service.handle_typed_statement(&engine, lowered, spark_call, context))
            .map(statement_result)
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

    /// Executes a semantic `SHOW ALTER TABLE OPTIMIZE` presentation command.
    pub fn execute_command(
        &self,
        statement: &novarocks_sql::semantic::command::ShowOptimizeSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        self.service
            .handle_typed_show_optimize(
                crate::table_maintenance::lower_semantic_show_optimize(statement)?,
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
    use super::super::TableMaintenanceEngine;
    use super::*;
    use novarocks_table_maintenance::{
        MaintenanceActionOutcome, MaintenanceActionRequest, OptimizeSubmission,
    };
    use std::sync::Arc;
    use std::thread;

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
            _target: novarocks_table_maintenance::MaintenanceTarget,
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
        let statement =
            novarocks_query_application::sql::parse_single_statement("SHOW ALTER TABLE OPTIMIZE")
                .expect("parser statement");
        let Some(novarocks_query_application::sql::ProductSqlCommand::Maintenance(
            novarocks_sql::semantic::MaintenanceSqlCommand::ShowOptimize(command),
        )) = novarocks_query_application::sql::lower_product_sql_command(&statement)
            .expect("lower semantic command")
        else {
            panic!("expected semantic SHOW ALTER TABLE OPTIMIZE");
        };
        let result = executor
            .execute_command(&command, Some("ice"), "db")
            .expect("execute");
        assert!(matches!(result, StatementResult::Ok));
        assert!(service.called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn injected_process_runtime_drives_work_from_a_plain_worker_thread() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let handle = runtime.handle().clone();
        let result = thread::spawn(move || handle.block_on(async { 17_u8 }))
            .join()
            .expect("plain worker must not require a current Tokio reactor");
        assert_eq!(result, 17);
    }
}
