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

//! Original statement statistics composition.
use super::{
    application, attempt_executor::StatisticsStatementExecutor, read::StatisticsReadService,
};
use std::sync::Arc;
pub(crate) struct FrontendStatisticsApplicationPort {
    statement_executor: Arc<dyn StatisticsStatementExecutor>,
    target_resolver: Arc<dyn application::StatisticsTargetResolver>,
    read: StatisticsReadService,
}
impl FrontendStatisticsApplicationPort {
    pub(crate) fn new(
        target_resolver: Arc<dyn application::StatisticsTargetResolver>,
        statement_executor: Arc<dyn StatisticsStatementExecutor>,
        read: StatisticsReadService,
    ) -> Self {
        Self {
            statement_executor,
            target_resolver,
            read,
        }
    }
}
impl application::StatisticsApplicationPort for FrontendStatisticsApplicationPort {
    fn prepare_statement_action(
        &self,
        command: application::StatisticsApplicationCommand,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        context: &novarocks_spi::connector::ConnectorRequestContext,
        family: novarocks_result_contract::ClientPresentationFamily,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        application::StatisticsApplicationError,
    > {
        match command {
            application::StatisticsApplicationCommand::AnalyzeTable { target, columns } => {
                if novarocks_sql::system_table::is_system_table_name(
                    &target.catalog,
                    &target.namespace,
                ) {
                    return Err(
                        application::StatisticsApplicationError::from_connector_error(
                            novarocks_spi::connector::ConnectorError::new(
                                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                                "statistics collection on system tables is unsupported",
                            ),
                        ),
                    );
                }
                self.statement_executor.prepare_statement(
                    self.target_resolver.clone(),
                    target,
                    columns,
                    execution,
                    context,
                )
            }
            application::StatisticsApplicationCommand::ShowStats { target } => {
                self.read.prepare_action(target, execution, context, family)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_query_application::{
        admitted_query_context::QueryExecutionContext, api::BackendTopologySnapshot,
        cancellation::QueryCancellationSource, session_control::GovernedStatementAction,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct UnusedPorts(AtomicUsize);
    impl application::StatisticsTargetResolver for UnusedPorts {}
    impl StatisticsStatementExecutor for UnusedPorts {
        fn prepare_statement(
            &self,
            _: Arc<dyn application::StatisticsTargetResolver>,
            _: application::StatisticsTableTarget,
            _: application::StatisticsColumnIntent,
            _: &QueryExecutionContext,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<Box<dyn GovernedStatementAction>, application::StatisticsApplicationError>
        {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(application::StatisticsApplicationError::new(
                "ordinary statement reached executor",
            ))
        }
    }

    #[tokio::test]
    async fn system_analyze_refuses_before_capacity_provider_io_and_capture() {
        let unused = Arc::new(UnusedPorts::default());
        let runtime = tokio::runtime::Handle::current();
        let read = StatisticsReadService::new(
            unused.clone(),
            crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(runtime.clone()),
            runtime,
            Arc::new(novarocks_query_application::view::QueryViewService::new()),
        );
        let executor = crate::statistics::command::StatisticsCommandExecutor::new(Arc::new(
            FrontendStatisticsApplicationPort::new(unused.clone(), unused.clone(), read),
        ));
        // No capacity, CPU or I/O source exists. The system predicate must
        // select Unsupported before attempting any ordinary preparation.
        let execution = QueryExecutionContext::new(
            novarocks_types::ClusterRole::Fe,
            BackendTopologySnapshot::empty(0),
            None,
            QueryCancellationSource::new().view(),
            Default::default(),
            Default::default(),
        );
        let context = crate::connector::test_request_context();
        let family =
            novarocks_result_contract::ClientPresentationFamily::TrinoJson(Default::default());
        for (sql, catalog, namespace) in [
            (
                "ANALYZE TABLE system.metadata.materialized_views",
                None,
                "db",
            ),
            ("ANALYZE TABLE SYSTEM.INFORMATION_SCHEMA.TABLES", None, "db"),
            (
                "ANALYZE TABLE catalog.information_schema.tables",
                None,
                "db",
            ),
            (
                "ANALYZE TABLE tables",
                Some("catalog"),
                "information_schema",
            ),
        ] {
            let statement = novarocks_query_application::sql::parse_single_statement(sql).unwrap();
            let Some(novarocks_query_application::sql::ProductSqlCommand::Statistics(command)) =
                novarocks_query_application::sql::lower_product_sql_command(&statement).unwrap()
            else {
                panic!("typed statistics command")
            };
            let error = executor
                .prepare_statement_action(
                    &command, catalog, namespace, &execution, &context, family,
                )
                .err()
                .expect("system refusal");
            let cause = error.failure_cause().unwrap();
            let original = cause
                .downcast_ref::<novarocks_spi::connector::ConnectorError>()
                .unwrap();
            assert_eq!(
                original.kind(),
                novarocks_spi::connector::ConnectorErrorKind::Unsupported
            );
            assert_eq!(cause.failure_fact(), original.failure_fact());
            let service = crate::query::product_command_service_error(error);
            assert_eq!(
                service
                    .execution_failure()
                    .unwrap()
                    .descriptor()
                    .descriptor
                    .code
                    .as_str(),
                "query.execution.unsupported"
            );
            assert!(service.failure_cause().is_some());
        }
        assert_eq!(unused.0.load(Ordering::SeqCst), 0);
        let ordinary = application::StatisticsApplicationCommand::AnalyzeTable {
            target: application::StatisticsTableTarget {
                catalog: "ice".into(),
                namespace: "db".into(),
                table: "orders".into(),
            },
            columns: application::StatisticsColumnIntent::AllColumns,
        };
        // The same service still admits ordinary ANALYZE to its real executor.
        let application = FrontendStatisticsApplicationPort::new(
            unused.clone(),
            unused.clone(),
            StatisticsReadService::new(
                unused.clone(),
                crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(
                    tokio::runtime::Handle::current(),
                ),
                tokio::runtime::Handle::current(),
                Arc::new(novarocks_query_application::view::QueryViewService::new()),
            ),
        );
        use super::application::StatisticsApplicationPort;
        assert!(
            application
                .prepare_statement_action(ordinary, &execution, &context, family)
                .is_err()
        );
        assert_eq!(unused.0.load(Ordering::SeqCst), 1);
    }
}
