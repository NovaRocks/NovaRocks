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
pub(super) fn assert_original_same_owner(
    port: &novarocks_physical_plan::ResultPort,
    source: &novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
) {
    let fragment = &source.plan().fragments()[&port.fragment];
    let node = &fragment.nodes()[&port.output.node];
    assert_eq!(node.output, port.output);
    assert_eq!(port.fields.len(), port.output.columns.len());
    for (ordinal, field) in port.fields.iter().enumerate() {
        assert_eq!(field.value, port.output.columns[ordinal]);
        assert_eq!(field.ty, fragment.values()[&field.value].ty);
    }
}

use novarocks_expression_contract::ExprNode;
use novarocks_functions::ConstantValue;
use novarocks_physical_plan::{Fragment, PhysicalNode, PipelineDopDomain, PlanVersionId};
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlAuthoredPhysicalPlan,
    SqlCompileControl, SqlCompileIntent, SqlCompileProgressError, SqlCompiler,
    SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    builtin_sql_function_catalog,
};
use novarocks_sql::sql_mode::SqlSemanticSettings;
use novarocks_type_contract::CompilePhase;

pub(super) fn session(semantics: SqlSemanticSettings, fold: bool) -> SqlSessionContext {
    let mut optimizer_settings = SessionOptimizerSettings {
        enable_materialized_view_rewrite: Some(false),
        enable_common_subexpr_reuse: Some(false),
        ..Default::default()
    };
    if !fold {
        optimizer_settings
            .disabled_rules
            .push("FoldConstant".into());
    }
    SqlSessionContext {
        sql_semantics: semantics,
        current_catalog: None,
        current_database: "fixture".into(),
        optimizer_settings,
    }
}

pub(super) fn table_free_request(
    sql: &str,
    session: SqlSessionContext,
    control: &SqlCompileControl,
) -> SqlFinalPlanCompileRequest {
    table_free_request_with_constant_policy(
        sql,
        session,
        control,
        novarocks_sql_testkit::sql_fixture_constant_policy(),
    )
}

pub(super) fn table_free_request_with_constant_policy(
    sql: &str,
    session: SqlSessionContext,
    control: &SqlCompileControl,
    policy: novarocks_functions::ConstantValidation,
) -> SqlFinalPlanCompileRequest {
    plan_request_with_constant_policy(
        SqlStatementInput::parsed_query(novarocks_sql_testkit::parse_query(sql)),
        SqlCompileIntent::Query,
        session,
        control,
        policy,
    )
}

pub(super) fn plan_request(
    statement: SqlStatementInput,
    intent: SqlCompileIntent,
    session: SqlSessionContext,
    control: &SqlCompileControl,
) -> SqlFinalPlanCompileRequest {
    plan_request_with_constant_policy(
        statement,
        intent,
        session,
        control,
        novarocks_sql_testkit::sql_fixture_constant_policy(),
    )
}

fn plan_request_with_constant_policy(
    statement: SqlStatementInput,
    intent: SqlCompileIntent,
    session: SqlSessionContext,
    control: &SqlCompileControl,
    policy: novarocks_functions::ConstantValidation,
) -> SqlFinalPlanCompileRequest {
    SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([84; 16]).unwrap(),
        statement,
        intent,
        session,
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        policy,
        control.clone(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        DEFAULT_COMPLETION_LIMITS,
    )
}

pub(super) fn compile_table_free(
    sql: &str,
    session: SqlSessionContext,
    control: &SqlCompileControl,
) -> Result<SqlAuthoredPhysicalPlan, SqlCompileProgressError> {
    let request = table_free_request(sql, session, control).try_into_completion()?;
    Ok(SqlCompiler::start(request, control)?
        .into_complete()?
        .into_plan())
}

pub(super) fn expression_sources(
    owner: &SqlAuthoredPhysicalPlan,
) -> impl Iterator<Item = (&Fragment, &PhysicalNode, &ExprNode)> {
    owner.plan().fragments().values().flat_map(|fragment| {
        fragment.nodes().values().flat_map(move |node| {
            node.expressions
                .iter()
                .flat_map(|unit| unit.definitions())
                .map(move |source| (fragment, node, source))
        })
    })
}

pub(super) fn text(value: &ConstantValue) -> &str {
    value
        .utf8_observed(
            CompilePhase::FunctionSpecialization,
            &SqlCompileControl::unbounded(),
        )
        .unwrap()
        .expect("present UTF8 constant")
}

pub(super) fn analyze_table_free(
    sql: &str,
    intent: SqlCompileIntent,
    session: SqlSessionContext,
    functions: &dyn novarocks_sql::compiler::SqlFunctionCatalog,
    control: &SqlCompileControl,
) -> Result<novarocks_sql::compiler::SqlAnalyzeOutput, novarocks_sql::compiler::SqlCompileError> {
    use novarocks_sql::compiler::{SqlAnalyzeRequest, SqlPlannerTableSnapshot};

    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let tables = SqlPlannerTableSnapshot::new(&catalog);
    SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(novarocks_sql_testkit::parse_query(sql)),
        intent,
        session,
        SqlPlanningEnvironment::Distributed,
        &tables,
        functions,
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        None,
        novarocks_sql_testkit::sql_fixture_constant_policy(),
        control.clone(),
    ))
}

pub(super) fn optimize_table_free(
    sql: &str,
    session: SqlSessionContext,
    functions: &dyn novarocks_sql::compiler::SqlFunctionCatalog,
    control: &SqlCompileControl,
) -> Result<(), novarocks_sql::compiler::SqlCompileError> {
    let analyzed = analyze_table_free(sql, SqlCompileIntent::Query, session, functions, control)?
        .into_pending()?;
    let statistics = novarocks_sql::planning::dml::DmlStatisticsSnapshot::empty();
    SqlCompiler::optimize(novarocks_sql::compiler::SqlOptimizeRequest::new(
        analyzed,
        &statistics,
        control.clone(),
    ))?;
    Ok(())
}

pub(super) mod table_parent {

    use super::{optimize_table_free, session};
    use crate::fold_observation::FoldProbe;
    use novarocks_functions::{FunctionBindingError, FunctionBindingSelection, FunctionKind};
    use novarocks_sql::{
        binding::SqlFunctionBinding,
        compiler::{SqlCompileError, SqlFunctionCatalog, builtin_sql_function_catalog},
    };
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Clone, Debug)]
    pub(crate) enum Outcome {
        Allow,
        Unavailable,
        Control(CompileControlError),
    }
    #[derive(Clone, Debug)]
    pub(crate) struct TableParentCatalog {
        inner: Arc<dyn SqlFunctionCatalog>,
        outcome: Outcome,
        pub(crate) calls: Arc<AtomicUsize>,
        pub(crate) sources: Arc<Mutex<Vec<FunctionBindingSelection>>>,
        parent: Arc<Mutex<Option<novarocks_functions::ResolvedFunctionBinding>>>,
    }
    impl SqlFunctionCatalog for TableParentCatalog {
        fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
            Arc::new(self.clone())
        }
        fn admit_authored_environment_observed(
            &self,
            binding: &SqlFunctionBinding,
            control: &dyn PureCompileControl,
        ) -> Result<(), FunctionBindingError> {
            self.inner
                .admit_authored_environment_observed(binding, control)
        }

        fn select_exact_overload_observed(
            &self,
            function_id: &novarocks_functions::FunctionId,
            kind: novarocks_functions::FunctionKind,
            overload: &novarocks_functions::FunctionOverloadId,
            request: novarocks_functions::FunctionBindingRequest<'_>,
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            Arc<novarocks_functions::FunctionBindingSelection>,
            novarocks_functions::FunctionBindingError,
        > {
            self.inner
                .select_exact_overload_observed(function_id, kind, overload, request, control)
        }
        fn pure_overload_declaration_observed<'a>(
            &'a self,
            function_id: &novarocks_functions::FunctionId,
            kind: novarocks_functions::FunctionKind,
            overload: &novarocks_functions::FunctionOverloadId,
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::PureOverloadDeclaration<'a>,
            novarocks_functions::FunctionSpecializationFailure,
        > {
            if kind == FunctionKind::Table {
                let parent = self
                    .parent
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("the actual Table resolver supplied this admission's parent")
                    .clone();
                assert_eq!(&parent.function_id, function_id);
                assert_eq!(parent.kind, kind);
                assert_eq!(&parent.selected.overload, overload);
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.sources.lock().unwrap().push(parent.selected);
                control
                    .checkpoint(CompilePhase::FunctionSpecialization, 0)
                    .map_err(novarocks_functions::FunctionSpecializationFailure::Control)?;
                match self.outcome.clone() {
                    Outcome::Allow => {}
                    Outcome::Unavailable => {
                        return Err(
                        novarocks_functions::FunctionSpecializationFailure::MissingPureImplementation(
                            overload.clone(),
                        ),
                    );
                    }
                    Outcome::Control(cause) => {
                        return Err(novarocks_functions::FunctionSpecializationFailure::Control(
                            cause,
                        ));
                    }
                }
            }
            self.inner
                .pure_overload_declaration_observed(function_id, kind, overload, control)
        }

        fn prepare_fresh_selected(
            &self,
            input: novarocks_functions::CallEffectInput<'_>,
            selected: Arc<novarocks_functions::FunctionBindingSelection>,
            options: novarocks_functions::PureCallPreparation,
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::PureCallSpecialization,
            novarocks_functions::FunctionSpecializationFailure,
        > {
            self.inner
                .prepare_fresh_selected(input, selected, options, control)
        }
        fn resolve_scalar_signature(
            &self,
            name: &str,
            arg_types: &[arrow::datatypes::DataType],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionSignature,
            novarocks_functions::FunctionResolutionError,
        > {
            self.inner
                .resolve_scalar_signature(name, arg_types, control)
        }
        fn resolve_scalar_binding(
            &self,
            name: &str,
            arguments: &[novarocks_functions::FunctionArgument],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionBinding,
            novarocks_functions::FunctionBindingError,
        > {
            self.inner.resolve_scalar_binding(name, arguments, control)
        }
        fn finish_scalar_binding(
            &self,
            function: &novarocks_functions::FunctionId,
            overload: &novarocks_functions::FunctionOverloadId,
            request: novarocks_functions::FunctionBindingRequest<'_>,
            control: &dyn PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionBinding,
            novarocks_functions::FunctionBindingError,
        > {
            self.inner
                .finish_scalar_binding(function, overload, request, control)
        }
        fn resolve_scalar_binding_with_expected_result(
            &self,
            name: &str,
            arguments: &[novarocks_functions::FunctionArgument],
            expected: &novarocks_functions::FunctionValueType,
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionBinding,
            novarocks_functions::FunctionBindingError,
        > {
            self.inner
                .resolve_scalar_binding_with_expected_result(name, arguments, expected, control)
        }
        fn resolve_value_conversion_binding(
            &self,
            argument: &novarocks_functions::FunctionArgument,
            target: &novarocks_functions::FunctionValueType,
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionBinding,
            novarocks_functions::FunctionBindingError,
        > {
            self.inner
                .resolve_value_conversion_binding(argument, target, control)
        }
        fn resolve_window_binding(
            &self,
            name: &str,
            arguments: &[novarocks_functions::FunctionArgument],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionBinding,
            novarocks_functions::FunctionBindingError,
        > {
            self.inner.resolve_window_binding(name, arguments, control)
        }
        fn resolve_table_binding(
            &self,
            name: &str,
            arguments: &[novarocks_functions::FunctionArgument],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionBinding,
            novarocks_functions::FunctionBindingError,
        > {
            let binding = self.inner.resolve_table_binding(name, arguments, control)?;
            *self.parent.lock().unwrap() = Some(binding.clone());
            Ok(binding)
        }
        fn contains_aggregate(&self, name: &str) -> bool {
            self.inner.contains_aggregate(name)
        }
        fn resolve_aggregate_binding(
            &self,
            name: &str,
            logical_argument_count: usize,
            arguments: &[novarocks_functions::FunctionArgument],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionBinding,
            novarocks_functions::FunctionBindingError,
        > {
            self.inner
                .resolve_aggregate_binding(name, logical_argument_count, arguments, control)
        }
        fn resolve_aggregate_binding_trusted(
            &self,
            name: &str,
            logical_argument_count: usize,
            arguments: &[novarocks_functions::FunctionArgument],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionBinding,
            novarocks_functions::FunctionBindingError,
        > {
            self.inner.resolve_aggregate_binding_trusted(
                name,
                logical_argument_count,
                arguments,
                control,
            )
        }
        fn resolve_aggregate_signature(
            &self,
            name: &str,
            arg_types: &[arrow::datatypes::DataType],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedAggregateSignature,
            novarocks_functions::FunctionResolutionError,
        > {
            self.inner
                .resolve_aggregate_signature(name, arg_types, control)
        }
        fn resolve_aggregate_update_signature(
            &self,
            name: &str,
            logical_arg_types: &[arrow::datatypes::DataType],
            update_arg_types: &[arrow::datatypes::DataType],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedAggregateSignature,
            novarocks_functions::FunctionResolutionError,
        > {
            self.inner.resolve_aggregate_update_signature(
                name,
                logical_arg_types,
                update_arg_types,
                control,
            )
        }
        fn resolve_aggregate_trusted(
            &self,
            name: &str,
            arg_types: &[arrow::datatypes::DataType],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedAggregateSignature,
            novarocks_functions::FunctionResolutionError,
        > {
            self.inner
                .resolve_aggregate_trusted(name, arg_types, control)
        }
        fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility {
            self.inner.volatility(name)
        }
    }

    pub(crate) fn source(outcome: Outcome) -> TableParentCatalog {
        TableParentCatalog {
            inner: builtin_sql_function_catalog().snapshot(),
            outcome,
            calls: Arc::new(AtomicUsize::new(0)),
            sources: Arc::new(Mutex::new(Vec::new())),
            parent: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn optimize(
        catalog: &TableParentCatalog,
        probe: &Arc<FoldProbe>,
    ) -> Result<(), SqlCompileError> {
        optimize_table_free(
            "SELECT u.value FROM (SELECT 1 AS seed) q CROSS JOIN UNNEST(ARRAY[REVERSE('abc')]) AS u(value)",
            session(Default::default(), true),
            catalog,
            &probe.control(),
        )
    }
}
