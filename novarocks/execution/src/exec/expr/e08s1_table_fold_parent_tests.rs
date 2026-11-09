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
//! Original ordinary Table source and its real parent-before-argument admission loan.
use super::sql_fold_observation_test_source::{EVALUATOR, SERIAL, fold_count, reset, session};
use novarocks_functions::{FunctionBindingError, FunctionBindingSelection, FunctionKind};
use novarocks_sql::{
    analyze_error::AnalyzeErrorKind,
    binding::SqlFunctionBinding,
    compiler::{
        SqlAnalyzeRequest, SqlCompileControl, SqlCompileError, SqlCompileIntent, SqlCompiler,
        SqlFunctionCatalog, SqlOptimizeRequest, SqlPlannerTableSnapshot, SqlPlanningEnvironment,
        SqlStatementInput, builtin_sql_function_catalog,
    },
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
#[derive(Clone, Debug)]
enum Outcome {
    Allow,
    Unavailable,
    Control(CompileControlError),
}
#[derive(Clone, Debug)]
struct TableParentCatalog {
    inner: Arc<dyn SqlFunctionCatalog>,
    outcome: Outcome,
    calls: Arc<AtomicUsize>,
    sources: Arc<Mutex<Vec<FunctionBindingSelection>>>,
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

fn source(outcome: Outcome) -> TableParentCatalog {
    TableParentCatalog {
        inner: builtin_sql_function_catalog().snapshot(),
        outcome,
        calls: Arc::new(AtomicUsize::new(0)),
        sources: Arc::new(Mutex::new(Vec::new())),
        parent: Arc::new(Mutex::new(None)),
    }
}
fn optimize(catalog: &TableParentCatalog) -> Result<(), SqlCompileError> {
    let tables = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let tables = SqlPlannerTableSnapshot::new(&tables);
    let control = SqlCompileControl::unbounded();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(crate::exec::expr::test_support::parse_query("SELECT u.value FROM (SELECT 1 AS seed) q CROSS JOIN LATERAL UNNEST([REVERSE('abc')]) AS u(value)")),
        SqlCompileIntent::Query,
        session(),
        SqlPlanningEnvironment::Distributed,
        &tables,
        catalog,
        &EVALUATOR,
        None,
        super::test_support::constant_policy(),
        control.clone(),
    ))?
    .into_pending()?;
    SqlCompiler::optimize(SqlOptimizeRequest::new(
        analyzed,
        &novarocks_sql::planning::dml::DmlStatisticsSnapshot::empty(),
        control,
    ))?;
    Ok(())
}
#[test]
fn e08s1_table_fold_original_source_keeps_real_child_evaluation() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    let catalog = source(Outcome::Allow);
    optimize(&catalog).unwrap();
    assert!(
        fold_count() > 0,
        "original actual Table child reaches its calculator"
    );
    eprintln!(
        "ordinary Table actual parent loans: {:?}",
        catalog.sources.lock().unwrap()
    );
}
#[test]
fn e08s1_table_fold_parent_unavailable_precedes_real_argument_evaluation() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    let catalog = source(Outcome::Unavailable);
    let error = optimize(&catalog).unwrap_err();
    let SqlCompileError::Analyze(error) = error else {
        panic!("typed original admission: {error:?}")
    };
    assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
    let sources = catalog.sources.lock().unwrap();
    assert_eq!(sources.len(), 1, "one actual failing parent, no tail loan");
    assert!(error.message().contains(sources[0].overload.as_str()));
    assert_eq!(catalog.calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        fold_count(),
        0,
        "unavailable Table parent precedes its real child"
    );
}
#[test]
fn e08s1_table_fold_parent_three_control_causes_keep_origin_and_no_child_tail() {
    let _serial = SERIAL.lock().unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        reset();
        let catalog = source(Outcome::Control(cause.clone()));
        let expected: SqlCompileError = cause.into();
        assert_eq!(optimize(&catalog).unwrap_err(), expected);
        assert_eq!(catalog.calls.load(Ordering::Relaxed), 1);
        assert_eq!(catalog.sources.lock().unwrap().len(), 1);
        assert_eq!(
            fold_count(),
            0,
            "control refuses this actual Table parent before children"
        );
    }
}
