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

use novarocks_functions::ConstantValue;
use novarocks_parser::ast::{Expr, legacy::CreateCatalog};
use novarocks_query_application::{
    session_error::QueryServiceError,
    sql::property_evaluation::{
        DdlPropertyConstantError, DdlPropertyConstantPort, prepare_legacy_create_catalog,
    },
};
use novarocks_sql::{
    compiler::{
        RequiredConstantTarget, SqlCompileControl, SqlFunctionCatalog, SqlPlannerTableSnapshot,
    },
    semantic::command::CatalogCreateCommand,
    sql_mode::SqlSemanticSettings,
};
use novarocks_workload_control::{ResultWindowAlias, WorkScope};
use std::sync::{Arc, RwLock};

pub(crate) struct CatalogPropertyCpuInput {
    pub(crate) source: Arc<str>,
    pub(crate) statement: CreateCatalog,
    pub(crate) current_database: String,
    pub(crate) sql_semantics: SqlSemanticSettings,
    pub(crate) interpreter: Arc<crate::RequiredConstantInterpreter>,
    pub(crate) control: SqlCompileControl,
    pub(crate) scope: WorkScope,
    pub(crate) window: ResultWindowAlias,
    pub(crate) catalog: Arc<RwLock<novarocks_sql::planning::catalog::PlannerMemoryCatalog>>,
    pub(crate) functions: Arc<dyn SqlFunctionCatalog>,
    pub(crate) policy: novarocks_functions::ConstantPolicy,
}

impl CatalogPropertyCpuInput {
    pub(crate) fn run(self) -> Result<CatalogCreateCommand, QueryServiceError> {
        let mut port = CatalogConstantPort { input: &self };
        prepare_legacy_create_catalog(&self.source, &self.statement, &mut port, &self.control)
    }
}

struct CatalogConstantPort<'a> {
    input: &'a CatalogPropertyCpuInput,
}
impl DdlPropertyConstantPort for CatalogConstantPort<'_> {
    fn evaluate(
        &mut self,
        expression: &Expr,
        target: RequiredConstantTarget,
    ) -> Result<ConstantValue, DdlPropertyConstantError> {
        let input = self.input;
        let prepared = {
            let catalog = input.catalog.read().map_err(super::poisoned_state)?;
            let evaluator = crate::query_execution::ddl_properties::DdlPropertyConstantEvaluator {
                catalog: &SqlPlannerTableSnapshot::new(&*catalog),
                current_database: &input.current_database,
                functions: input.functions.clone(),
                sql_semantics: &input.sql_semantics,
                constant_policy: input.policy,
                control: &input.control,
                interpreter: &input.interpreter,
                scope: &input.scope,
                window: &input.window,
            };
            evaluator.prepare(expression, target)
        }?;
        // The catalog read guard and snapshot above have exited. This is the
        // same required interpreter under the caller's original scope/window.
        input
            .interpreter
            .evaluate(prepared, &input.scope, &input.window)
            .map_err(crate::query_execution::ddl_properties::evaluation_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::ast::{CatalogStatement, Statement};
    use novarocks_query_application::sql::{parse_single_statement, session::SessionSqlState};
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

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
        fn input(&self, source: &str) -> CatalogPropertyCpuInput {
            let Statement::Catalog(CatalogStatement::CreateCatalog(statement)) =
                parse_single_statement(source).unwrap()
            else {
                panic!("real CREATE CATALOG");
            };
            let frozen = SessionSqlState::default();
            CatalogPropertyCpuInput {
                source: Arc::from(source), statement,
                current_database: frozen.current_database().to_owned(),
                sql_semantics: frozen.sql_semantics().clone(),
                interpreter: Arc::new(crate::query_execution::required_constant_eval::test_required_constant_interpreter()),
                control: SqlCompileControl::unbounded(), scope: self.root.owner.scope(),
                window: self.window.clone(), catalog: Arc::new(RwLock::new(Default::default())),
                functions: Arc::new(novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap()),
                policy: crate::application::test_constant_policy(),
            }
        }
        fn close(self) {
            assert_eq!(self.window.retained_payload_bytes(), 0);
            drop(self.window);
            self.root.owner.complete();
            self.root.business.release();
        }
    }

    pub(crate) fn prepare_legacy_catalog_for_test(source: &str) -> CatalogCreateCommand {
        let fixture = Fixture::new();
        let command = fixture
            .input(source)
            .run()
            .expect("real catalog property preparation");
        fixture.close();
        command
    }

    #[test]
    fn catalog_cpu_prepares_complete_configuration_without_publishing_partial_properties() {
        let fixture = Fixture::new();
        let input = fixture.input("CREATE EXTERNAL CATALOG IF NOT EXISTS Warehouse COMMENT 'admitted' PROPERTIES ('type'='iceberg', 'MixedKey'='selected中')");
        let original_catalog = input.catalog.clone();
        let command = input.run().unwrap();
        assert_eq!(command.name, "Warehouse");
        assert!(command.if_not_exists);
        assert_eq!(
            command.properties,
            vec![
                ("type".into(), "iceberg".into()),
                ("MixedKey".into(), "selected中".into())
            ]
        );
        // No read guard survives the computation; the block around prepare
        // additionally keeps it outside every interpreter call in production.
        drop(original_catalog.try_write().expect("catalog guard exited"));
        drop(command);
        let error = fixture
            .input("CREATE CATALOG Warehouse PROPERTIES ('same'='first','same'='second')")
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
pub(crate) use tests::prepare_legacy_catalog_for_test;
