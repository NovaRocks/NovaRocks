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

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use arrow::datatypes::DataType;
use novarocks_functions::{
    FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionResolutionError,
    FunctionResultType, FunctionValueType, FunctionVolatility, ResolvedAggregateSignature,
    ResolvedFunctionBinding, ResolvedFunctionSignature,
};
use novarocks_parser::ast;
use novarocks_type_contract::{CompileControlError, PureCompileControl};

use crate::analysis::{ExprKind, QueryBody, ResolvedQuery};
use crate::catalog::PlannerTableProvider;
use crate::compiler::SqlFunctionCatalog;

#[derive(Debug)]
struct EmptyCatalog;
impl PlannerTableProvider for EmptyCatalog {
    fn resolve_table_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        table: &str,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        Err(format!("unexpected table: {table}"))
    }
}

#[derive(Clone, Debug)]
struct OneShotArrayControl {
    cause: FunctionBindingError,
    array_bindings: Arc<AtomicUsize>,
    refused: Arc<AtomicBool>,
    after_refusal: Arc<AtomicUsize>,
}
impl OneShotArrayControl {
    fn new(cause: CompileControlError) -> Self {
        Self::binding_failure(FunctionBindingError::Control(cause))
    }
    fn binding_failure(cause: FunctionBindingError) -> Self {
        Self {
            cause,
            array_bindings: Arc::new(AtomicUsize::new(0)),
            refused: Arc::new(AtomicBool::new(false)),
            after_refusal: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn observe_delegate(&self) {
        if self.refused.load(Ordering::SeqCst) {
            self.after_refusal.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl SqlFunctionCatalog for OneShotArrayControl {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        self.observe_delegate();
        Arc::new(self.clone())
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog()
            .resolve_scalar_signature(name, args, control)
    }
    fn resolve_scalar_binding(
        &self,
        name: &str,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.observe_delegate();
        if name == "__array_literal" && self.array_bindings.fetch_add(1, Ordering::SeqCst) == 0 {
            self.refused.store(true, Ordering::SeqCst);
            return Err(self.cause.clone());
        }
        // A retry deliberately succeeds, so the fixture exposes a swallowed
        // first refusal instead of merely failing again for the same reason.
        crate::functions::builtin_sql_function_catalog().resolve_scalar_binding(name, args, control)
    }
    fn resolve_scalar_binding_with_expected_result(
        &self,
        name: &str,
        args: &[FunctionArgument],
        expected: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog()
            .resolve_scalar_binding_with_expected_result(name, args, expected, control)
    }
    fn resolve_value_conversion_binding(
        &self,
        arg: &FunctionArgument,
        target: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog()
            .resolve_value_conversion_binding(arg, target, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog().contains_aggregate(name)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog()
            .resolve_aggregate_signature(name, args, control)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog()
            .resolve_aggregate_trusted(name, args, control)
    }
    fn volatility(&self, name: &str) -> FunctionVolatility {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog().volatility(name)
    }
}

fn analyze(
    sql: &str,
    functions: &dyn SqlFunctionCatalog,
) -> Result<ResolvedQuery, crate::analyze_error::AnalyzeError> {
    analyze_with_policy_and_control(
        sql,
        functions,
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
}

fn analyze_with_policy_and_control(
    sql: &str,
    functions: &dyn SqlFunctionCatalog,
    policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<ResolvedQuery, crate::analyze_error::AnalyzeError> {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query");
    };
    super::analyze_with_function_catalog(
        query,
        &EmptyCatalog,
        "default",
        functions,
        policy,
        control,
    )
    .map(|result| result.0)
}

#[test]
fn element_at_type_probe_preserves_first_catalog_control_without_rebinding() {
    // This is the real parser/analyzer/catalog binding path; the test does
    // not install a CPU implementation or generalize analyzer control APIs.
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let functions = OneShotArrayControl::new(cause.clone());
        let error = analyze("SELECT element_at([1,2],1)", &functions).unwrap_err();
        assert_eq!(error.control_error(), Some(cause));
        assert_eq!(error.span(), None);
        assert_eq!(functions.array_bindings.load(Ordering::SeqCst), 1);
        assert_eq!(functions.after_refusal.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn element_at_type_probe_keeps_original_ordinary_binding_error_without_rebinding() {
    for sql in ["SELECT element_at([1,2],1)", "SELECT element_at( [1,2], 1)"] {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query");
        };
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select");
        };
        let [ast::SelectItem::UnnamedExpr(ast::Expr::FunctionCall(function))] =
            select.projection.as_slice()
        else {
            panic!("element_at call");
        };
        let Some(ast::Expr::Array(array)) = function.arguments.first() else {
            panic!("original array argument");
        };
        let array_span = array.span;
        assert_ne!(array_span, function.span);

        let original = FunctionBindingError::NoMatchingOverload;
        let expected = crate::analyze_error::AnalyzeError::function_binding(original.clone())
            .at_type_mismatch(array_span);
        let functions = OneShotArrayControl::binding_failure(original);
        // The same fixture succeeds on a later array binding. Every observed
        // delegate after the first ordinary refusal must therefore stay absent.
        let error = analyze(sql, &functions).unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TypeMismatch
        );
        assert_eq!(error.code().as_str(), "sql.analyze.type_mismatch");
        assert_eq!(error.span(), Some(array_span));
        assert_eq!(error.message(), "no matching declared overload");
        assert!(error.control_error().is_none());
        assert!(error.source_failure().is_none());
        assert!(functions.refused.load(Ordering::SeqCst));
        assert_eq!(functions.array_bindings.load(Ordering::SeqCst), 1);
        assert_eq!(functions.after_refusal.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn element_at_type_probe_keeps_real_zero_row_constant_policy_author() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};

    let mut policy = crate::constant::test_constant_policy();
    policy.max_rows = 0;
    for sql in ["SELECT [1,2]", "SELECT element_at([1,2],1)"] {
        let error = analyze_with_policy_and_control(
            sql,
            crate::functions::builtin_sql_function_catalog(),
            policy,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap_err();
        assert!(error.control_error().is_none());
        let source = error
            .source_failure()
            .expect("retain the actual constant author");
        assert!(matches!(
            source.downcast_ref::<FunctionBindingError>(),
            Some(FunctionBindingError::Constant(
                novarocks_constant_contract::ConstantError::Limit(
                    "constant factory row limit exceeded"
                )
            ))
        ));
        let fact = source.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(
            fact.reason().domain(),
            novarocks_failure_contract::domain::CONSTANT.get()
        );
        assert_eq!(fact.reason().code(), 2);
    }
}

#[test]
fn element_at_type_probe_keeps_original_constant_source_before_later_cancellation() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};

    struct CancelAfterCatalogRefusal {
        refused: Arc<AtomicBool>,
        observed_after_refusal: AtomicUsize,
    }
    impl crate::compiler::SqlCancellationObservation for CancelAfterCatalogRefusal {
        fn is_cancelled(&self) -> bool {
            if self.refused.load(Ordering::SeqCst) {
                self.observed_after_refusal.fetch_add(1, Ordering::SeqCst);
                true
            } else {
                false
            }
        }
    }
    // Obtain this cause from the real Constant author and its zero-row policy.
    // The catalog fixture returns this exact original cause once, then would
    // succeed on a retry. Cancellation becomes observable only after refusal.
    let mut policy = crate::constant::test_constant_policy();
    policy.max_rows = 0;
    let constant = crate::constant::admit_syntax_constant(
        &crate::analysis::LiteralValue::Int(1),
        &FunctionValueType::new(DataType::Int8, false),
        policy,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap_err();
    assert!(matches!(
        constant,
        novarocks_constant_contract::ConstantError::Limit("constant factory row limit exceeded")
    ));
    let original = FunctionBindingError::Constant(constant);
    let functions = OneShotArrayControl::binding_failure(original.clone());
    let cancellation = Arc::new(CancelAfterCatalogRefusal {
        refused: Arc::clone(&functions.refused),
        observed_after_refusal: AtomicUsize::new(0),
    });
    let control = crate::compiler::SqlCompileControl::new(None, cancellation.clone());
    let error = analyze_with_policy_and_control(
        "SELECT element_at([1,2],1)",
        &functions,
        crate::constant::test_constant_policy(),
        &control,
    )
    .unwrap_err();
    assert!(error.control_error().is_none(), "{error:?}");
    let source = error
        .source_failure()
        .expect("retain the original catalog source");
    assert_eq!(
        source.downcast_ref::<FunctionBindingError>(),
        Some(&original)
    );
    let fact = source.failure_fact();
    assert_eq!(fact.class(), FailureClass::Resource);
    assert_eq!(
        fact.reason().domain(),
        novarocks_failure_contract::domain::CONSTANT.get()
    );
    assert_eq!(fact.reason().code(), 2);
    assert!(functions.refused.load(Ordering::SeqCst));
    assert_eq!(functions.array_bindings.load(Ordering::SeqCst), 1);
    assert_eq!(functions.after_refusal.load(Ordering::SeqCst), 0);
    assert_eq!(
        cancellation.observed_after_refusal.load(Ordering::SeqCst),
        0
    );
}

#[test]
fn element_at_type_probe_preserves_selected_unavailable_without_rebinding() {
    let overload = novarocks_functions::FunctionOverloadId::try_new(
        "test.scalar/element-at-first-array-unavailable/v1",
    )
    .unwrap();
    let original = FunctionBindingError::UnavailableImplementation(overload);
    let functions = OneShotArrayControl::binding_failure(original.clone());
    let error = analyze("SELECT element_at([1,2],1)", &functions).unwrap_err();
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::UnavailableImplementation
    );
    assert_eq!(error.to_string(), original.to_string());
    assert!(error.control_error().is_none());
    assert_eq!(functions.array_bindings.load(Ordering::SeqCst), 1);
    assert_eq!(functions.after_refusal.load(Ordering::SeqCst), 0);
}

#[test]
fn element_at_type_probe_routes_actual_array_and_map_sources_without_overload_guessing() {
    let query = analyze(
        "SELECT element_at([1,2],1), [1,2][1], element_at(map{1:2},1), map{1:2}[1]",
        crate::functions::builtin_sql_function_catalog(),
    )
    .unwrap();
    let QueryBody::Select(select) = query.body else {
        panic!("select");
    };
    let expected = [
        // Both spellings select the existing INTEGER index overload.
        (
            "__array_element_at",
            DataType::Int32,
            DataType::Int32,
            false,
        ),
        (
            "__array_element_at",
            DataType::Int32,
            DataType::Int32,
            false,
        ),
        ("__map_element_at", DataType::Int32, DataType::Int32, false),
        ("__map_element_at", DataType::Int32, DataType::Int32, false),
    ];
    assert_eq!(select.projection.len(), expected.len());
    for (project, (name, result, index, index_nullable)) in select.projection.iter().zip(expected) {
        let ExprKind::FunctionCall {
            name: actual,
            binding,
            args,
            ..
        } = &project.expr.kind
        else {
            panic!("bound subscript");
        };
        assert_eq!(actual, name);
        assert_eq!(
            binding.function_id.as_str(),
            format!("builtin.scalar/{name}/v1")
        );
        assert_eq!(args.len(), 2);
        assert_eq!(binding.selected.argument_types.len(), 2);
        for (argument, selected) in args.iter().zip(binding.selected.argument_types.iter()) {
            assert_eq!(
                selected,
                &FunctionArgumentType::Value(argument.value_type.clone())
            );
        }
        let result = FunctionValueType::new(result, true);
        assert_eq!(project.expr.value_type, result);
        assert_eq!(
            binding.selected.result_type,
            FunctionResultType::Scalar(result)
        );
        assert_eq!(
            args[1].value_type,
            FunctionValueType::new(index, index_nullable)
        );
        assert!(
            matches!(
                args[1].kind,
                ExprKind::Literal(crate::analysis::LiteralValue::Int(1))
            ),
            "original INTEGER probe needs no narrowing"
        );
        assert!(matches!(args[0].kind, ExprKind::FunctionCall { .. }));
    }
}
