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

//! Closed read-only table-maintenance command capability.

use std::sync::Arc;

use crate::engine::StatementResult;
use crate::engine::table_maintenance::{
    MaintenanceRequestContext, MaintenanceStatementResult, TableMaintenanceService,
};

#[derive(Clone)]
pub struct MaintenanceReadCommandExecutor {
    service: Arc<dyn TableMaintenanceService>,
}

impl MaintenanceReadCommandExecutor {
    pub(crate) fn new(service: Arc<dyn TableMaintenanceService>) -> Self {
        Self { service }
    }

    pub fn try_execute(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<Option<StatementResult>, String> {
        if !crate::engine::statement::looks_like_show_alter_table_optimize(sql) {
            return Ok(None);
        }
        self.service
            .try_handle_readonly_statement(
                sql,
                MaintenanceRequestContext {
                    current_catalog,
                    current_database,
                },
            )
            .map(|result| {
                result.map(|result| match result {
                    MaintenanceStatementResult::Ok => StatementResult::Ok,
                    MaintenanceStatementResult::Query(result) => StatementResult::Query(result),
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::engine::table_maintenance::{
        MaintenanceActionOutcome, MaintenanceActionRequest, OptimizeSubmission,
        TableMaintenanceEngine,
    };

    struct ReadOnlyService {
        called: AtomicBool,
    }

    impl TableMaintenanceService for ReadOnlyService {
        fn start(&self, _engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
            Ok(())
        }

        fn try_handle_statement(
            &self,
            _engine: &dyn TableMaintenanceEngine,
            _sql: &str,
            _context: MaintenanceRequestContext<'_>,
        ) -> Result<Option<MaintenanceStatementResult>, String> {
            unreachable!("read-only executor must not request a maintenance engine")
        }

        fn try_handle_readonly_statement(
            &self,
            _sql: &str,
            _context: MaintenanceRequestContext<'_>,
        ) -> Result<Option<MaintenanceStatementResult>, String> {
            self.called.store(true, Ordering::SeqCst);
            Ok(Some(MaintenanceStatementResult::Ok))
        }

        fn execute_automatic_action(
            &self,
            _engine: &dyn TableMaintenanceEngine,
            _request: MaintenanceActionRequest,
        ) -> Result<MaintenanceActionOutcome, String> {
            Err("not used".to_string())
        }

        fn submit_automatic_optimize(
            &self,
            _engine: &dyn TableMaintenanceEngine,
            _target: crate::engine::table_maintenance::MaintenanceTarget,
        ) -> Result<OptimizeSubmission, String> {
            Err("not used".to_string())
        }

        fn shutdown(&self) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn show_optimize_uses_read_only_service_without_engine() {
        let service = Arc::new(ReadOnlyService {
            called: AtomicBool::new(false),
        });
        let executor = MaintenanceReadCommandExecutor::new(Arc::clone(&service) as Arc<_>);
        let result = executor
            .try_execute("SHOW ALTER TABLE OPTIMIZE", Some("ice"), "db")
            .expect("execute");
        assert!(matches!(result, Some(StatementResult::Ok)));
        assert!(service.called.load(Ordering::SeqCst));
    }
}
