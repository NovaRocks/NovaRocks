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
//! Real SQL dependency observation through the production FE constant port.
use arrow::array::{Array, StringArray};
use novarocks_functions::ConstantValue;
use novarocks_functions::{
    ConstantError, FunctionArgumentType, FunctionResultType, ResolvedFunctionBinding,
};
use novarocks_physical_plan::{ExprKind, PipelineDopDomain, PlanVersionId, ScanReadBudget};
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, FoldArg, FoldRequest, FrozenFoldNode, SessionOptimizerSettings,
    SqlAuthoredPhysicalPlan, SqlCompileControl, SqlCompileIntent, SqlCompileProgress, SqlCompiler,
    SqlConstantEvaluationError, SqlConstantEvaluator, SqlFinalPlanCompileRequest,
    SqlFoldDependencyInput, SqlFoldDependencyObserver, SqlFoldDependencySource,
    SqlFoldEvaluationOutcome, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    builtin_sql_function_catalog,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::sync::{Arc, LazyLock, Mutex};

#[derive(Clone)]
enum Mode {
    CompiledCore,
    Decline,
    Fail(SqlConstantEvaluationError),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorTag {
    Evaluation,
    Control,
    Constant,
    Source,
    Outer,
}
#[derive(Debug)]
enum Outcome {
    Produced(ConstantValue),
    Declined,
    Error {
        tag: ErrorTag,
        raw_text_len: usize,
        control: Option<CompileControlError>,
    },
}
#[derive(Debug)]
struct Receipt {
    binding: Option<ResolvedFunctionBinding>,
    source_result_constraint: Option<FunctionValueType>,
    source_constraint_origin: Option<novarocks_sql::binding::SqlResultConstraintOrigin>,
    source_decimal_policy: Option<novarocks_type_contract::DecimalOverflowPolicy>,
    request_result: FunctionValueType,
    request_kind: FrozenFoldNode,
    arguments: Vec<FoldArg>,
    outcome: Option<Outcome>,
}
struct Probe {
    refuse_start: Option<CompileControlError>,
    receipts: Mutex<Vec<Receipt>>,
}
impl Probe {
    fn new() -> Self {
        Self {
            refuse_start: None,
            receipts: Mutex::new(Vec::new()),
        }
    }
}
// Observation is captured before the production FE port consumes the frozen node.
fn calculate(
    mode: &Mode,
    request: &FoldRequest,
    control: &dyn PureCompileControl,
) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
    match mode {
        Mode::Decline => return Ok(None),
        Mode::Fail(error) => return Err(error.clone()),
        Mode::CompiledCore => {}
    }
    super::constant_evaluator().eval_scalar(request, control)
}

impl SqlFoldDependencyObserver for Probe {
    fn before_fold_dependency_observed(
        &self,
        input: SqlFoldDependencyInput<'_>,
        _: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        if let Some(cause) = self.refuse_start.clone() {
            return Err(cause);
        }
        let (source_result_constraint, source_constraint_origin, source_decimal_policy) =
            match input.source {
                SqlFoldDependencySource::Function(binding) => (
                    binding.result_constraint().cloned(),
                    Some(binding.result_constraint_origin().clone()),
                    Some(binding.decimal_overflow_policy()),
                ),
                SqlFoldDependencySource::Intrinsic => (None, None, None),
            };
        let binding = match input.source {
            SqlFoldDependencySource::Function(binding) => {
                assert!(matches!(input.request.kind, FrozenFoldNode::Call(_)));
                assert_eq!(binding.logical_argument_count, input.request.args.len());
                assert_eq!(
                    binding.selected.result_type,
                    FunctionResultType::Scalar(input.request.result_type.clone())
                );
                assert_eq!(
                    binding.selected.argument_types.len(),
                    input.request.args.len()
                );
                for (actual, argument) in binding
                    .selected
                    .argument_types
                    .iter()
                    .zip(&input.request.args)
                {
                    assert_eq!(
                        actual,
                        &FunctionArgumentType::Value(argument.value_type.clone())
                    );
                    assert_eq!(argument.value.value_type(), &argument.value_type);
                }
                Some(binding.resolved().clone())
            }
            SqlFoldDependencySource::Intrinsic => {
                assert!(!matches!(input.request.kind, FrozenFoldNode::Call(_)));
                None
            }
        };
        // Tests own an explicit bounded ledger. No formal MEM grant is claimed.
        let mut receipts = self.receipts.lock().unwrap();
        receipts
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        receipts.push(Receipt {
            binding,
            source_result_constraint,
            source_constraint_origin,
            source_decimal_policy,
            request_result: input.request.result_type.clone(),
            request_kind: input.request.kind.clone(),
            arguments: input.request.args.clone(),
            outcome: None,
        });
        Ok(())
    }
    fn after_fold_dependency_observed(
        &self,
        _: SqlFoldDependencyInput<'_>,
        outcome: SqlFoldEvaluationOutcome<'_>,
    ) {
        let outcome = match outcome {
            SqlFoldEvaluationOutcome::ProducedConstant(value) => Outcome::Produced(value.clone()),
            SqlFoldEvaluationOutcome::Declined => Outcome::Declined,
            SqlFoldEvaluationOutcome::Error(error) => {
                let (tag, raw_text_len, control) = match error {
                    SqlConstantEvaluationError::Evaluation(message) => {
                        (ErrorTag::Evaluation, message.len(), None)
                    }
                    SqlConstantEvaluationError::Control(cause) => {
                        (ErrorTag::Control, 0, Some(cause.clone()))
                    }
                    SqlConstantEvaluationError::Constant(_) => (ErrorTag::Constant, 0, None),
                    SqlConstantEvaluationError::Source(_) => (ErrorTag::Source, 0, None),
                    SqlConstantEvaluationError::Outer(_) => (ErrorTag::Outer, 0, None),
                };
                Outcome::Error {
                    tag,
                    raw_text_len,
                    control,
                }
            }
        };
        // No capacity request, error-string cloning, evaluator, or name binding.
        let mut receipts = self.receipts.lock().unwrap();
        let receipt = receipts.last_mut().unwrap();
        assert!(receipt.outcome.is_none());
        receipt.outcome = Some(outcome);
    }
}
// Deliberately no observer methods and no query-specific mutable state.
pub(super) struct ExistingEvaluator {
    mode: Mode,
}
impl SqlConstantEvaluator for ExistingEvaluator {
    fn eval_scalar(
        &self,
        request: &FoldRequest,
        control: &dyn PureCompileControl,
    ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
        calculate(&self.mode, request, control)
    }
}
pub(super) static REAL: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::CompiledCore,
};
static DECLINE: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Decline,
};
static CANCEL: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Control(
        CompileControlError::Cancelled,
    )),
};
static DEADLINE: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Control(
        CompileControlError::DeadlineExceeded,
    )),
};
static RESOURCE: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Control(
        CompileControlError::ResourceExhausted,
    )),
};
static LONG_ERROR: LazyLock<ExistingEvaluator> = LazyLock::new(|| ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Evaluation(
        "original folding evaluation failure ".repeat(40),
    )),
});
static CONSTANT_ERROR: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Constant(
        ConstantError::Invalid("original constant refusal"),
    )),
};
static TYPE_ERROR: LazyLock<ExistingEvaluator> = LazyLock::new(|| ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::from_domain(
        novarocks_type_contract::ValueTypeError::TooDeep,
    )),
});
static PREPARATION_ERROR: LazyLock<ExistingEvaluator> = LazyLock::new(|| ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::outer_domain(
        novarocks_functions::KernelFailure::InvalidProgram(
            novarocks_functions::KernelDiagnostic::new("original preparation refusal"),
        ),
    )),
});
static PROJECTED_SOURCE_ERROR: LazyLock<ExistingEvaluator> = LazyLock::new(|| ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::from_domain(
        novarocks_execution::runtime::task::ExecutionFailure::from(
            novarocks_functions::KernelFailure::ResourceExhausted,
        ),
    )),
});
struct MustNotEvaluate;
impl SqlConstantEvaluator for MustNotEvaluate {
    fn eval_scalar(
        &self,
        _: &FoldRequest,
        _: &dyn PureCompileControl,
    ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
        panic!("a refused observation must not perform original evaluation")
    }
}
static MUST_NOT_EVALUATE: MustNotEvaluate = MustNotEvaluate;
fn policy() -> novarocks_functions::ConstantPolicy {
    novarocks_functions::ConstantPolicy {
        max_rows: 64,
        max_array_nodes: 1024,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
fn compile_typed(
    sql: &str,
    evaluator: &'static dyn SqlConstantEvaluator,
    observer: Option<Arc<Probe>>,
) -> Result<SqlAuthoredPhysicalPlan, novarocks_sql::compiler::SqlCompileProgressError> {
    compile_with_observer(
        sql,
        evaluator,
        observer.map(|observer| observer as Arc<dyn SqlFoldDependencyObserver>),
    )
}

pub(super) fn compile_with_observer(
    sql: &str,
    evaluator: &'static dyn SqlConstantEvaluator,
    observer: Option<Arc<dyn SqlFoldDependencyObserver>>,
) -> Result<SqlAuthoredPhysicalPlan, novarocks_sql::compiler::SqlCompileProgressError> {
    let control = match observer {
        Some(observer) => SqlCompileControl::unbounded().with_fold_dependency_observer(observer),
        None => SqlCompileControl::unbounded(),
    };
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([83; 16]).unwrap(),
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                ..SessionOptimizerSettings::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        evaluator,
        policy(),
        control.clone(),
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
    match SqlCompiler::start(request.try_into_completion()?, &control)? {
        SqlCompileProgress::Complete(completed) => Ok(completed.into_plan()),
        SqlCompileProgress::Incomplete(need) => Err(
            novarocks_sql::compiler::SqlCompileError::InvalidRequest(format!(
                "real table-free fixture unexpectedly asked for facts: {need:?}"
            ))
            .into(),
        ),
    }
}
fn compile(
    sql: &str,
    evaluator: &'static dyn SqlConstantEvaluator,
    observer: Option<Arc<Probe>>,
) -> Result<SqlAuthoredPhysicalPlan, String> {
    compile_typed(sql, evaluator, observer).map_err(|error| error.to_string())
}
fn probe() -> Arc<Probe> {
    Arc::new(Probe::new())
}
fn calls(plan: &SqlAuthoredPhysicalPlan) -> usize {
    plan.plan()
        .fragments()
        .values()
        .flat_map(|f| f.expressions().iter())
        .filter(|(_, e)| matches!(e.kind, ExprKind::FunctionCall { .. }))
        .count()
}
fn text(value: &ConstantValue) -> String {
    let array = value.pool().array();
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(value.ordinal() as usize)
        .into()
}
#[test]
fn sql_fold_dependency_real_upper_removed_from_plan_keeps_exact_binding_and_cv() {
    let evaluator = probe();
    let source = compile(
        "SELECT upper('Straße') AS folded",
        &REAL,
        Some(evaluator.clone()),
    )
    .unwrap();
    assert_eq!(
        calls(&source),
        0,
        "successful original fold must remove its call"
    );
    let receipts = evaluator.receipts.lock().unwrap();
    let receipt = receipts
        .iter()
        .find(|r| r.binding.is_some())
        .expect("folded function dependency was lost");
    assert_eq!(text(&receipt.arguments[0].value), "Straße");
    match receipt.outcome.as_ref().unwrap() {
        Outcome::Produced(value) => {
            assert_eq!(text(value), "STRASSE");
            assert_eq!(value.value_type(), &receipt.request_result);
        }
        other => panic!("unexpected actual legacy result: {other:?}"),
    }
    assert!(receipts.iter().all(|receipt| receipt.outcome.is_some()));
}
#[test]
fn sql_fold_dependency_two_actual_functions_both_recorded_before_replacement() {
    let evaluator = probe();
    let source = compile(
        "SELECT upper('first') AS a, upper('second') AS b",
        &REAL,
        Some(evaluator.clone()),
    )
    .unwrap();
    assert_eq!(calls(&source), 0);
    let receipts = evaluator.receipts.lock().unwrap();
    let values = receipts
        .iter()
        .filter(|r| r.binding.is_some())
        .map(|r| text(&r.arguments[0].value))
        .collect::<Vec<_>>();
    assert!(values.iter().any(|v| v == "first"));
    assert!(values.iter().any(|v| v == "second"));
}
#[test]
fn sql_fold_dependency_cast_intrinsic_retains_exact_source_result_policy() {
    let evaluator = probe();
    let source = compile(
        "SELECT CAST(12 AS VARCHAR) AS folded_cast",
        &REAL,
        Some(evaluator.clone()),
    )
    .unwrap();
    assert_eq!(calls(&source), 0);
    let receipts = evaluator.receipts.lock().unwrap();
    let receipt = receipts
        .iter()
        .find(|r| {
            matches!(
                r.request_kind,
                FrozenFoldNode::Intrinsic(
                    novarocks_sql::compiler::FrozenFoldIntrinsic::Cast { .. }
                )
            )
        })
        .expect("cast invocation was omitted");
    assert!(receipt.binding.is_none());
    assert_eq!(receipt.arguments[0].value.try_i64().unwrap(), Some(12));
    assert_eq!(
        receipt.request_result.data_type().clone(),
        arrow::datatypes::DataType::Utf8
    );
    assert!(matches!(receipt.outcome, Some(Outcome::Produced(_))));
}
#[test]
fn sql_fold_dependency_decline_and_long_evaluation_error_preserve_fail_open() {
    let long_len = "original folding evaluation failure ".repeat(40).len();
    for calculator in [&DECLINE as &'static dyn SqlConstantEvaluator, &*LONG_ERROR] {
        let observer = probe();
        let actual = compile(
            "SELECT upper('kept') AS kept",
            calculator,
            Some(observer.clone()),
        )
        .unwrap();
        let unchanged = compile("SELECT upper('kept') AS kept", calculator, None).unwrap();
        assert_eq!(calls(&actual), calls(&unchanged));
        assert!(calls(&actual) >= 1);
        let receipts = observer.receipts.lock().unwrap();
        for receipt in receipts.iter().filter(|r| r.binding.is_some()) {
            match receipt.outcome.as_ref().unwrap() {
                Outcome::Declined => {}
                Outcome::Error {
                    tag: ErrorTag::Evaluation,
                    raw_text_len,
                    ..
                } => {
                    assert_eq!(*raw_text_len, long_len);
                    assert!(*raw_text_len > 512);
                }
                other => panic!("unexpected result: {other:?}"),
            }
        }
    }
}
#[test]
fn sql_fold_dependency_start_control_refusal_has_no_evaluation_or_completion() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let observer = Arc::new(Probe {
            refuse_start: Some(cause.clone()),
            ..Probe::new()
        });
        let error = compile_typed(
            "SELECT upper('never')",
            &MUST_NOT_EVALUATE,
            Some(observer.clone()),
        )
        .unwrap_err();
        assert!(matches!(
            (cause, error),
            (
                CompileControlError::Cancelled,
                novarocks_sql::compiler::SqlCompileProgressError::Compile(
                    novarocks_sql::compiler::SqlCompileError::Cancelled
                )
            ) | (
                CompileControlError::DeadlineExceeded,
                novarocks_sql::compiler::SqlCompileProgressError::Compile(
                    novarocks_sql::compiler::SqlCompileError::DeadlineExceeded
                )
            ) | (
                CompileControlError::ResourceExhausted,
                novarocks_sql::compiler::SqlCompileProgressError::Compile(
                    novarocks_sql::compiler::SqlCompileError::ResourceExhausted
                )
            )
        ));
        assert!(observer.receipts.lock().unwrap().is_empty());
    }
}
#[test]
fn sql_fold_dependency_original_control_errors_stay_first_and_typed() {
    for (cause, calculator) in [
        (CompileControlError::Cancelled, &CANCEL),
        (CompileControlError::DeadlineExceeded, &DEADLINE),
        (CompileControlError::ResourceExhausted, &RESOURCE),
    ] {
        let observer = probe();
        let actual = compile(
            "SELECT upper('control')",
            calculator,
            Some(observer.clone()),
        )
        .unwrap_err();
        let original = compile("SELECT upper('control')", calculator, None).unwrap_err();
        assert_eq!(actual, original);
        let receipts = observer.receipts.lock().unwrap();
        assert_eq!(receipts.len(), 1);
        assert!(
            matches!(&receipts[0].outcome, Some(Outcome::Error { tag: ErrorTag::Control, control: Some(c), .. }) if *c == cause)
        );
    }
}
#[test]
fn sql_fold_dependency_original_static_failure_classes_and_text_unchanged() {
    for calculator in [
        &CONSTANT_ERROR as &'static dyn SqlConstantEvaluator,
        &*TYPE_ERROR,
        &*PREPARATION_ERROR,
    ] {
        let observer = probe();
        let actual =
            compile("SELECT upper('static')", calculator, Some(observer.clone())).unwrap_err();
        let original = compile("SELECT upper('static')", calculator, None).unwrap_err();
        assert_eq!(actual, original);
        let receipts = observer.receipts.lock().unwrap();
        assert_eq!(receipts.len(), 1);
        assert!(matches!(
            &receipts[0].outcome,
            Some(Outcome::Error {
                tag: ErrorTag::Constant | ErrorTag::Source | ErrorTag::Outer,
                ..
            })
        ));
    }
}
#[test]
fn sql_fold_dependency_projected_source_keeps_identity_and_does_not_mint_control() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let observer = probe();
    let actual = compile_typed(
        "SELECT upper('projected')",
        &*PROJECTED_SOURCE_ERROR,
        Some(observer.clone()),
    )
    .unwrap_err();
    let original =
        compile_typed("SELECT upper('projected')", &*PROJECTED_SOURCE_ERROR, None).unwrap_err();
    use novarocks_sql::compiler::{SqlCompileError, SqlCompileProgressError};
    use std::error::Error;
    assert!(
        actual
            .source()
            .unwrap()
            .downcast_ref::<SqlCompileError>()
            .is_some()
    );
    let unwrap_source = |error| match error {
        SqlCompileProgressError::Compile(SqlCompileError::CompilationSource(source)) => source,
        _ => panic!("projected resource source must propagate"),
    };
    let source = unwrap_source(actual);
    assert_eq!(source, unwrap_source(original));
    assert_eq!(source.failure_fact().class(), FailureClass::Resource);
    assert!(
        source
            .downcast_ref::<novarocks_execution::runtime::task::ExecutionFailure>()
            .is_some()
    );
    let receipts = observer.receipts.lock().unwrap();
    assert_eq!(receipts.len(), 1);
    assert!(matches!(
        &receipts[0].outcome,
        Some(Outcome::Error {
            tag: ErrorTag::Source,
            control: None,
            ..
        })
    ));
}

#[test]
fn sql_fold_dependency_query_owned_receipts_are_isolated_and_released() {
    let left = probe();
    let right = probe();
    let left_weak = Arc::downgrade(&left);
    let right_weak = Arc::downgrade(&right);
    std::thread::scope(|scope| {
        scope.spawn(|| compile("SELECT upper('left')", &REAL, Some(left.clone())).unwrap());
        scope.spawn(|| compile("SELECT upper('right')", &REAL, Some(right.clone())).unwrap());
    });
    for (observer, expected) in [(&left, "left"), (&right, "right")] {
        let receipts = observer.receipts.lock().unwrap();
        let values = receipts
            .iter()
            .filter(|r| r.binding.is_some())
            .map(|r| text(&r.arguments[0].value))
            .collect::<Vec<_>>();
        assert!(!values.is_empty());
        assert!(values.iter().all(|value| value == expected));
    }
    drop(left);
    drop(right);
    assert!(left_weak.upgrade().is_none());
    assert!(right_weak.upgrade().is_none());
}

#[test]
fn sql_fold_dependency_observer_absence_keeps_existing_calculator_path() {
    let source = compile("SELECT upper('plain')", &REAL, None).unwrap();
    assert_eq!(calls(&source), 0);
}

#[test]
fn sql_fold_dependency_original_unconstrained_binding_is_not_selected_result_constraint() {
    let observer = probe();
    let source = compile(
        "SELECT upper('unconstrained') AS folded",
        &REAL,
        Some(observer.clone()),
    )
    .unwrap();
    assert_eq!(calls(&source), 0);
    let receipts = observer.receipts.lock().unwrap();
    let receipt = receipts
        .iter()
        .find(|receipt| receipt.binding.is_some())
        .expect("actual original function fold must be observed");
    let binding = receipt.binding.as_ref().unwrap();
    assert_eq!(
        binding.selected.result_type,
        FunctionResultType::Scalar(receipt.request_result.clone())
    );
    assert!(receipt.source_result_constraint.is_none());
    assert_eq!(
        receipt.source_constraint_origin,
        Some(novarocks_sql::binding::SqlResultConstraintOrigin::Unconstrained)
    );
    assert_eq!(
        receipt.source_decimal_policy,
        Some(
            novarocks_sql::sql_mode::SqlSemanticSettings::default()
                .sql_mode()
                .decimal_overflow_policy()
        )
    );
    assert!(matches!(receipt.outcome, Some(Outcome::Produced(_))));
}

#[path = "sql_call_dependency_loan_tests.rs"]
mod sql_call_dependency_loan_tests;

#[path = "ds_hll_state_fold_source_tests.rs"]
mod ds_hll_state_fold_source_tests;

#[test]
fn native_decimal_array_and_temporal_literal_compile_sources_remain_valid() {
    use arrow::datatypes::{DataType, Field};
    let decimal_items = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Decimal128(3, 2),
        true,
    )));
    let double_items = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
    for (sql, expected) in [
        // One integer digit and two fractional digits require DECIMAL(3,2).
        ("SELECT [0.0,0.25,0.5,1.0]", decimal_items),
        (
            "SELECT percentile_approx_weighted(2.0,1,[0.0,0.25,0.5,1.0],2048)",
            double_items.clone(),
        ),
        (
            "SELECT d = '2020-01-01' FROM (SELECT CAST('2020-01-01' AS DATE) AS d) t",
            DataType::Boolean,
        ),
        (
            "SELECT ABS(CAST(99999999.9949 AS DECIMAL(13,4))) > 99999999.9999",
            DataType::Boolean,
        ),
        // An explicit DOUBLE item declaration remains distinct from inference.
        ("SELECT ARRAY<DOUBLE>[0.0,0.25,0.5,1.0]", double_items),
    ] {
        let owner = compile_typed(sql, super::constant_evaluator(), None)
            .unwrap_or_else(|error| panic!("real FE literal completion for {sql}: {error:#?}"));
        let result = owner.plan().result_port().expect("public query result");
        assert_eq!(result.fields.len(), 1, "{sql}");
        assert_eq!(result.fields[0].ty.data_type(), &expected, "{sql}");
        assert_eq!(
            result.fields[0].ty.logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical,
            "{sql}",
        );
        novarocks_sql::compiler::author_fragment_package_semantics(
            &owner,
            policy(),
            &SqlCompileControl::unbounded(),
        )
        .unwrap_or_else(|error| panic!("real literal package semantics for {sql}: {error:#?}"));
    }
}
