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

//! CREATE CATALOG property computation on the original supervised planning CPU.

use super::property_cpu_context::PropertyCpuContext;
use novarocks_parser::ast::TrinoCreateCatalog;
use novarocks_query_application::{
    session_error::QueryServiceError, sql::property_evaluation::prepare_trino_create_catalog,
};
use novarocks_sql::semantic::command::CatalogCreateCommand;
use std::sync::Arc;

pub(crate) struct CatalogPropertyCpuInput {
    pub(crate) source: Arc<str>,
    pub(crate) statement: TrinoCreateCatalog,
    pub(crate) context: PropertyCpuContext,
}
impl CatalogPropertyCpuInput {
    pub(crate) fn run(self) -> Result<CatalogCreateCommand, QueryServiceError> {
        let mut port = self.context.constant_port();
        prepare_trino_create_catalog(
            &self.source,
            &self.statement,
            &mut port,
            &self.context.control,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::sql::session::SessionSqlState;
    use novarocks_sql::compiler::SqlCompileControl;
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, ResultWindowAlias, RootWork, WorkClass, WorkRequest,
        WorkloadConfig, WorkloadControl,
    };
    use std::sync::RwLock;

    struct Fixture {
        root: RootWork,
        window: ResultWindowAlias,
        _control: WorkloadControl,
    }
    impl Fixture {
        fn new() -> Self {
            let control = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1 << 20,
                    control_bytes: 1024,
                    per_scope_bytes: (1 << 20) - 1024,
                },
            )
            .unwrap();
            control
                .configure_result_capacity(ResultCapacityConfig::V1)
                .unwrap();
            control.mark_ready().unwrap();
            let (root, grant) = control
                .root_admission()
                .try_begin_root_with_result(
                    WorkRequest::new(WorkClass::Management),
                    novarocks_workload_control::ResultWindowClass::Local,
                )
                .unwrap();
            Self {
                root,
                window: grant.retain_alias(),
                _control: control,
            }
        }
        fn trino_input(&self, source: &str) -> CatalogPropertyCpuInput {
            let novarocks_parser::ast::TrinoStatement::CreateCatalog(statement) =
                novarocks_parser::parser::parse_statement(source).unwrap()
            else {
                panic!("real typed CREATE CATALOG");
            };
            self.input_with_syntax(source, statement)
        }

        fn input_with_syntax(
            &self,
            source: &str,
            statement: TrinoCreateCatalog,
        ) -> CatalogPropertyCpuInput {
            let frozen = SessionSqlState::default();
            CatalogPropertyCpuInput {
                source: Arc::from(source),
                statement,
                context: PropertyCpuContext {
                    current_database: frozen.current_database().to_owned(),
                    sql_semantics: frozen.sql_semantics().clone(),
                    interpreter: Arc::new(crate::query_execution::required_constant_eval::test_required_constant_interpreter()),
                    control: SqlCompileControl::unbounded(),
                    scope: self.root.owner.scope(),
                    window: self.window.clone(),
                    catalog: Arc::new(RwLock::new(novarocks_sql::planning::catalog::PlannerMemoryCatalog::default())),
                    functions: Arc::new(novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap()),
                    policy: crate::application::test_constant_policy(),
                },
            }
        }
        fn close(self) {
            assert_eq!(self.window.retained_payload_bytes(), 0);
            drop(self.window);
            self.root.owner.complete();
            self.root.business.release();
        }
    }

    pub(crate) fn prepare_catalog_for_test(source: &str) -> CatalogCreateCommand {
        let fixture = Fixture::new();
        let command = fixture
            .trino_input(source)
            .run()
            .expect("real catalog property preparation");
        fixture.close();
        command
    }

    #[test]
    fn typed_catalog_cpu_preserves_connector_properties_and_guard_exit() {
        let fixture = Fixture::new();
        let input = fixture.trino_input("CREATE CATALOG IF NOT EXISTS Warehouse USING iceberg COMMENT 'admitted' WITH (\"MixedKey\" = 'selected中')");
        let original_catalog = input.context.catalog.clone();
        let command = input.run().unwrap();
        assert_eq!(command.name, "Warehouse");
        assert!(command.if_not_exists);
        assert_eq!(command.connector_name.as_deref(), Some("iceberg"));
        assert_eq!(
            command.properties,
            vec![("MixedKey".into(), "selected中".into())]
        );
        drop(original_catalog.try_write().expect("catalog guard exited"));
        drop(command);
        let error = fixture
            .trino_input(
                "CREATE CATALOG Warehouse USING iceberg WITH (same = 'first', same = 'second')",
            )
            .run()
            .unwrap_err();
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.admit.invalid_catalog_property"
        );
        assert!(error.user_error().unwrap().location().is_some());
        drop(error);
        fixture.close();
    }
}

#[cfg(test)]
pub(crate) use tests::prepare_catalog_for_test;
