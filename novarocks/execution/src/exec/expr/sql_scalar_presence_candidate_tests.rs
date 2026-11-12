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
//! Actual analysis/request-snapshot admission probes; not a complete support seal.
use super::sql_fold_observation_test_source::{EVALUATOR, SERIAL, fold_count, reset, session};
use arrow::datatypes::DataType;
use novarocks_functions::{
    FunctionArgument, FunctionBindingError, FunctionKind, FunctionSpecializationFailure,
    FunctionValueType, ResolvedFunctionBinding,
};
use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId, ScanReadBudget};
use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, SqlAnalyzeRequest, SqlCompileControl, SqlCompileError,
    SqlCompileIntent, SqlCompiler, SqlFinalPlanCompileRequest, SqlFunctionCatalog,
    SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlStatementInput,
    builtin_sql_function_catalog,
};
use novarocks_type_contract::{CompileControlError, PureCompileControl};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn analyze(sql: &str) -> Result<(), SqlCompileError> {
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let catalog = SqlPlannerTableSnapshot::new(&catalog);
    let request = SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(crate::exec::expr::test_support::parse_query(sql)),
        SqlCompileIntent::Query,
        session(),
        SqlPlanningEnvironment::Distributed,
        &catalog,
        builtin_sql_function_catalog(),
        &EVALUATOR,
        None,
        super::test_support::constant_policy(),
        SqlCompileControl::unbounded(),
    );
    SqlCompiler::analyze(request)?.into_pending()?;
    Ok(())
}
fn unavailable(error: SqlCompileError, expected: &str) {
    let SqlCompileError::Analyze(error) = error else {
        panic!("expected typed FE analyze refusal: {error}");
    };
    assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
    assert_eq!(
        error.code(),
        AnalyzeErrorKind::UnavailableImplementation
            .descriptor()
            .code
    );
    assert!(
        error.span().is_some(),
        "retain the actual parser-owned function span"
    );
    assert!(error.message().contains(expected), "{}", error.message());
    assert!(error.control_error().is_none());
}

#[test]
fn sql_presence_candidate_hour_literal_and_projected_column_refuse_before_fold() {
    let _serial = SERIAL.lock().unwrap();
    for sql in [
        "SELECT hour_from_unixtime(CAST(1700000000 AS BIGINT)) AS h",
        "SELECT hour_from_unixtime(t.k) AS h FROM (SELECT CAST(1700000000 AS BIGINT) AS k) t",
    ] {
        reset();
        assert_eq!(fold_count(), 0);
        unavailable(
            analyze(sql).err().unwrap(),
            "builtin.scalar/hour_from_unixtime/(i64)->i32;strict;legacy",
        );
        assert_eq!(fold_count(), 0);
        assert_eq!(fold_count(), 0);
    }
}

#[test]
fn sql_presence_candidate_invalid_pattern_is_not_prepared_or_evaluated_by_admission() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    {
        analyze("SELECT regexp_count('x', '[') AS n").unwrap();
        analyze("SELECT regexp_count('x', t.pattern) AS n FROM (SELECT '[' AS pattern) t").unwrap();
        assert_eq!(fold_count(), 0);
    }
}

fn argument(dtype: DataType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: FunctionValueType::new(dtype, true),
        constant: None,
    }
}
#[test]
fn sql_presence_candidate_snapshot_keeps_exact_bindings_and_table_owner_separate() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let again = scoped.snapshot().snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    let args = [argument(DataType::Utf8)];
    let original_binding = original
        .resolve_scalar_binding("reverse", &args, &control)
        .unwrap();
    assert_eq!(
        original_binding.admitted_execution_abi, None,
        "original declaration is not admission"
    );
    let actual_abi = original
        .pure_overload_declaration_observed(
            &original_binding.function_id,
            original_binding.kind,
            &original_binding.selected.overload,
            &control,
        )
        .unwrap()
        .implementation()
        .abi;
    // Scoped admission adds the real installed ABI without replacing source facts.
    let mut expected = original_binding.clone();
    expected.admitted_execution_abi = Some(actual_abi);
    for scope in [scoped.as_ref(), again.as_ref()] {
        let mut admitted = scope
            .resolve_scalar_binding("reverse", &args, &control)
            .unwrap();
        assert_eq!(admitted.admitted_execution_abi, Some(actual_abi));
        assert_eq!(admitted, expected);
        // Only the independently checked admission refinement differs. Every
        // original function, selected signature, semantics and source fact stays.
        admitted.admitted_execution_abi = None;
        assert_eq!(admitted, original_binding);
    }
    let hour = [argument(DataType::Int64)];
    let bound = original
        .resolve_scalar_binding("hour_from_unixtime", &hour, &control)
        .unwrap();
    for scope in [scoped.as_ref(), again.as_ref()] {
        assert!(
            matches!(scope.resolve_scalar_binding("hour_from_unixtime", &hour, &control), Err(FunctionBindingError::UnavailableImplementation(ref id)) if id == &bound.selected.overload)
        );
    }
    // This uses the actual registered table owner, separately from scalar admission.
    let bounds = [argument(DataType::List(Arc::new(
        arrow::datatypes::Field::new("item", DataType::Int64, true),
    )))];
    let table = original
        .resolve_table_binding("unnest", &bounds, &control)
        .unwrap();
    assert_eq!(table.kind, FunctionKind::Table);
    assert_eq!(table.admitted_execution_abi, None);
    let actual_table_abi = original
        .pure_overload_declaration_observed(
            &table.function_id,
            table.kind,
            &table.selected.overload,
            &control,
        )
        .unwrap()
        .implementation()
        .abi;
    for scope in [scoped.as_ref(), again.as_ref()] {
        let mut admitted = scope
            .resolve_table_binding("unnest", &bounds, &control)
            .unwrap();
        assert_eq!(admitted.admitted_execution_abi, Some(actual_table_abi));
        // The actual table implementation author establishes the admission
        // refinement independently; all original complete binding facts agree.
        admitted.admitted_execution_abi = None;
        assert_eq!(admitted, table);
    }
}

#[test]
fn sql_presence_candidate_exact_selected_loan_cannot_bypass_missing_owner() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    let args = [argument(DataType::Int64)];
    let bound = original
        .resolve_scalar_binding("hour_from_unixtime", &args, &control)
        .unwrap();
    let request = novarocks_functions::FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: args.len(),
        expected_result_type: None,
    };
    original
        .select_exact_overload_observed(
            &bound.function_id,
            bound.kind,
            &bound.selected.overload,
            request,
            &control,
        )
        .unwrap();
    assert!(matches!(scoped.select_exact_overload_observed(
        &bound.function_id, bound.kind, &bound.selected.overload, request, &control,
    ), Err(FunctionBindingError::UnavailableImplementation(ref id)) if id == &bound.selected.overload));
}

#[test]
fn sql_presence_candidate_completion_uses_actual_request_mode_before_fold_or_publication() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([87; 16]).unwrap(),
        SqlStatementInput::parsed_query(crate::exec::expr::test_support::parse_query(
            "SELECT hour_from_unixtime(CAST(1700000000 AS BIGINT)) AS h",
        )),
        SqlCompileIntent::Query,
        session(),
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        &EVALUATOR,
        super::test_support::constant_policy(),
        SqlCompileControl::unbounded(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: 64,
            max_batch_bytes: 1 << 20,
        },
        DEFAULT_COMPLETION_LIMITS,
    );
    unavailable(
        request
            .try_into_completion()
            .err()
            .expect("unavailable before completed source"),
        "builtin.scalar/hour_from_unixtime/(i64)->i32;strict;legacy",
    );
    assert_eq!(fold_count(), 0);
}

#[derive(Clone, Debug)]
struct RefusePresence {
    original: Arc<dyn SqlFunctionCatalog>,
    cause: CompileControlError,
    calls: Arc<AtomicUsize>,
}
impl SqlFunctionCatalog for RefusePresence {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(self.clone())
    }
    fn pure_overload_declaration_observed<'a>(
        &'a self,
        _: &novarocks_functions::FunctionId,
        _: FunctionKind,
        _: &novarocks_functions::FunctionOverloadId,
        _: &dyn PureCompileControl,
    ) -> Result<novarocks_functions::PureOverloadDeclaration<'a>, FunctionSpecializationFailure>
    {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Err(FunctionSpecializationFailure::Control(self.cause.clone()))
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        types: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.original.resolve_scalar_signature(name, types, control)
    }
    fn resolve_scalar_binding(
        &self,
        name: &str,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.original.resolve_scalar_binding(name, args, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.original.contains_aggregate(name)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        types: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.original
            .resolve_aggregate_signature(name, types, control)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        types: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.original
            .resolve_aggregate_trusted(name, types, control)
    }
    fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility {
        self.original.volatility(name)
    }
}
#[test]
fn sql_presence_candidate_declared_owner_control_is_first_cause_without_later_admission() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let original = RefusePresence {
            original: builtin_sql_function_catalog().snapshot(),
            cause: cause.clone(),
            calls: Arc::clone(&calls),
        };
        let args = [argument(DataType::Utf8)];
        let scoped = original.snapshot_for_scalar_presence();
        assert!(
            matches!(scoped.resolve_scalar_binding("reverse", &args, &SqlCompileControl::unbounded()), Err(FunctionBindingError::Control(actual)) if actual == cause)
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        // Original mode continues to use the actual unprojected resolver, not the failing loan.
        original
            .resolve_scalar_binding("reverse", &args, &SqlCompileControl::unbounded())
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}
