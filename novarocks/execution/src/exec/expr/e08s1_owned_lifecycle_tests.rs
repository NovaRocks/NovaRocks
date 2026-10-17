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
//! Actual installed lifecycle/static-profile admission. This is not the full E08s1 seal.
use super::sql_scalar_presence_original_tests::{EVALUATOR, SERIAL, fold_count, reset, session};
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    FunctionArgument, FunctionBindingError, FunctionValueType, PureCallLifecycle as Lifecycle,
    PureKernelAbi,
};
use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::compiler::{
    SqlAnalyzeRequest, SqlCompileControl, SqlCompileError, SqlCompileIntent, SqlCompiler,
    SqlPhysicalEmissionMode, SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlStatementInput,
    builtin_sql_function_catalog,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn arg(ty: DataType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: FunctionValueType::new(ty, true),
        constant: None,
    }
}
fn analyze(sql: &str, mode: SqlPhysicalEmissionMode) -> Result<(), SqlCompileError> {
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let tables = SqlPlannerTableSnapshot::new(&catalog);
    let request = SqlAnalyzeRequest::new(
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        session(),
        SqlPlanningEnvironment::Distributed,
        &tables,
        builtin_sql_function_catalog(),
        &EVALUATOR,
        None,
        super::pure_differential::constant_policy(),
        mode,
        SqlCompileControl::unbounded(),
    );
    SqlCompiler::analyze(request)?.into_pending()?;
    Ok(())
}
#[test]
fn e08s1_selected_loan_checks_actual_five_lifecycles_without_preparing_a_value() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let c = SqlCompileControl::unbounded();
    let scalar = original
        .resolve_scalar_binding("reverse", &[arg(DataType::Utf8)], &c)
        .unwrap();
    let aggregate = original
        .resolve_aggregate_binding("count", 1, &[arg(DataType::Int32)], &c)
        .unwrap();
    let window = original
        .resolve_window_binding("row_number", &[], &c)
        .unwrap();
    let table = original
        .resolve_table_binding(
            "unnest",
            &[arg(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int64,
                true,
            ))))],
            &c,
        )
        .unwrap();
    for (binding, lifecycle, abi) in [
        (&scalar, Lifecycle::Scalar, PureKernelAbi::ScalarV1),
        (
            &aggregate,
            Lifecycle::Aggregate,
            PureKernelAbi::AggregateWindowV1,
        ),
        (
            &aggregate,
            Lifecycle::AggregateWindow,
            PureKernelAbi::AggregateWindowV1,
        ),
        (&window, Lifecycle::Window, PureKernelAbi::WindowV1),
        (&table, Lifecycle::Table, PureKernelAbi::TableV1),
    ] {
        let loan = original
            .pure_overload_declaration_observed(
                &binding.function_id,
                binding.kind,
                &binding.selected.overload,
                &c,
            )
            .unwrap();
        assert_eq!(loan.implementation().abi, abi);
        scoped
            .admit_bound_lifecycle_observed(binding, lifecycle, &c)
            .unwrap();
        original
            .admit_bound_lifecycle_observed(binding, lifecycle, &c)
            .unwrap();
    }
    assert!(
        matches!(scoped.admit_bound_lifecycle_observed(&scalar, Lifecycle::Table, &c),
        Err(FunctionBindingError::UnavailableImplementation(id)) if id == scalar.selected.overload)
    );
}
#[test]
fn e08s1_missing_aggregate_window_and_over_keep_unavailable_span_before_any_fold() {
    let _serial = SERIAL.lock().unwrap();
    for (sql, name) in [
        ("SELECT dict_merge('x', 1)", "builtin.aggregate/dict_merge/"),
        (
            "SELECT session_number(1, 2) OVER (ORDER BY 1)",
            "builtin.window/session_number/",
        ),
    ] {
        reset();
        analyze(sql, SqlPhysicalEmissionMode::OriginalNativeV1).unwrap();
        let error = analyze(
            sql,
            SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        )
        .unwrap_err();
        let SqlCompileError::Analyze(error) = error else {
            panic!("typed analyze failure: {error}")
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
        assert!(error.span().is_some());
        assert!(error.message().contains(name), "{}", error.message());
        assert!(error.control_error().is_none());
        assert_eq!(fold_count(), 0);
    }
}
#[test]
fn e08s1_supported_over_and_invalid_regexp_data_are_not_evaluated_by_static_gate() {
    let _serial = SERIAL.lock().unwrap();
    for sql in [
        "SELECT count(1) OVER ()",
        "SELECT max_by(1, 2) OVER ()",
        "SELECT min_by(1, 2) OVER ()",
        "SELECT row_number() OVER ()",
        "SELECT regexp_count('x', '[')",
    ] {
        reset();
        analyze(
            sql,
            SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        )
        .unwrap();
        assert_eq!(fold_count(), 0);
    }
}
struct Refuse {
    cause: CompileControlError,
    calls: AtomicUsize,
}
impl PureCompileControl for Refuse {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::Relaxed),
            0,
            "no checkpoint after first refusal"
        );
        Err(self.cause)
    }
}
#[test]
fn e08s1_actual_selected_lifecycle_retains_three_compile_causes_and_original_has_no_lookup() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let c = SqlCompileControl::unbounded();
    let binding = original
        .resolve_aggregate_binding("count", 1, &[arg(DataType::Int32)], &c)
        .unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Refuse {
            cause,
            calls: AtomicUsize::new(0),
        };
        original
            .admit_bound_lifecycle_observed(&binding, Lifecycle::AggregateWindow, &control)
            .unwrap();
        assert_eq!(control.calls.load(Ordering::Relaxed), 0);
        assert!(
            matches!(scoped.admit_bound_lifecycle_observed(&binding, Lifecycle::AggregateWindow, &control),
            Err(FunctionBindingError::Control(actual)) if actual == cause)
        );
        assert_eq!(control.calls.load(Ordering::Relaxed), 1);
    }
}
