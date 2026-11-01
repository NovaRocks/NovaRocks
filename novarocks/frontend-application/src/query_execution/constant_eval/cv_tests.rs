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

//! Checked CV fixtures exercise the real staged FE evaluator, not a second
//! evaluator or a syntax readback adapter. Policies are finite test inputs.
use super::*;
use arrow::array::{Date32Array, Float32Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::Field;
use novarocks_functions::{ConstantError, ConstantPolicy};
use novarocks_sql::compiler::{
    BinOp, ConversionPlan, FoldArg, FrozenFoldIntrinsic, FrozenFoldNode, UnOp,
};
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, NR_LOGICAL_TYPE_KEY, ValueLayout,
    ValueTypeStorageLifetime,
};
use std::sync::Mutex;

struct TestControl;
impl PureCompileControl for TestControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
fn pool(array: ArrayRef, ty: FunctionValueType) -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(Field::new(
            "literal",
            array.data_type().clone(),
            ty.nullable(),
        )),
        ty,
        array.to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &TestControl,
    )
    .unwrap()
}
fn arg(array: ArrayRef, nullable: bool) -> FoldArg {
    let ty = FunctionValueType::new(array.data_type().clone(), nullable);
    typed_arg(array, ty)
}
fn typed_arg(array: ArrayRef, ty: FunctionValueType) -> FoldArg {
    let source_type = array.data_type().clone();
    let value = pool(array, ty.clone()).value(0).unwrap();
    assert_eq!(value.field().data_type(), &source_type);
    assert_eq!(value.value_type(), &ty);
    FoldArg {
        value,
        value_type: ty,
    }
}
fn json_list_type(source_type: &DataType) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        storage.clone(),
    )
    .unwrap();
    let value = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    FunctionValueType::try_from_value(value, ValueLayout::try_new(source_type, storage).unwrap())
        .unwrap()
}
fn i32(value: i32) -> FoldArg {
    arg(Arc::new(Int32Array::from(vec![value])), false)
}
fn i64(value: i64) -> FoldArg {
    arg(Arc::new(Int64Array::from(vec![value])), false)
}
fn text(value: &str) -> FoldArg {
    arg(Arc::new(StringArray::from(vec![value])), false)
}
fn decimal(value: i128, precision: u8, scale: i8) -> FoldArg {
    arg(
        Arc::new(
            Decimal128Array::from(vec![value])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ),
        false,
    )
}
// Test-only SQL syntax requests. This enum never dispatches an implementation.
// Exact binding, control and conversion facts are captured from SqlCompiler.
#[derive(Clone, Debug)]
enum FixtureNode {
    BinaryOp(BinOp, DecimalOverflowPolicy),
    UnaryOp(UnOp),
    Cast(DecimalOverflowPolicy),
    Function { name: String },
}
static CAPTURE_SERIAL: Mutex<()> = Mutex::new(());
static CAPTURE_STATE: Mutex<Option<CaptureState>> = Mutex::new(None);
static RECORDER: Recorder = Recorder;
struct CaptureState {
    node: FixtureNode,
    sources: Option<Vec<ConstantValue>>,
    result: DataType,
    captured: Option<FoldRequest>,
    observed: Vec<FoldRequest>,
}
struct Recorder;
impl SqlConstantEvaluator for Recorder {
    fn eval_scalar(
        &self,
        request: &FoldRequest,
        control: &dyn PureCompileControl,
    ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
        let mut capture_guard = CAPTURE_STATE.lock().unwrap();
        let state = capture_guard.as_mut().expect("active fixture capture");
        state.observed.push(request.clone());
        let shape = matches!(
            (&state.node, &request.kind),
            (FixtureNode::Function { .. }, FrozenFoldNode::Call(_))
                | (
                    FixtureNode::Cast(_),
                    FrozenFoldNode::Intrinsic(FrozenFoldIntrinsic::Cast { .. })
                )
                | (
                    FixtureNode::UnaryOp(UnOp::Not),
                    FrozenFoldNode::Intrinsic(FrozenFoldIntrinsic::Not)
                )
                | (
                    FixtureNode::UnaryOp(UnOp::Negate),
                    FrozenFoldNode::Intrinsic(FrozenFoldIntrinsic::Negate)
                )
                | (
                    FixtureNode::UnaryOp(UnOp::BitwiseNot),
                    FrozenFoldNode::Intrinsic(FrozenFoldIntrinsic::BitNot)
                )
                | (
                    FixtureNode::BinaryOp(_, _),
                    FrozenFoldNode::Intrinsic(
                        FrozenFoldIntrinsic::Arithmetic { .. }
                            | FrozenFoldIntrinsic::Comparison { .. }
                            | FrozenFoldIntrinsic::NullSafeEq { .. }
                            | FrozenFoldIntrinsic::Conjunction
                            | FrozenFoldIntrinsic::Disjunction
                    )
                )
        );
        let sources_match = state.sources.as_ref().is_none_or(|sources| {
            sources.len() == request.args.len()
                && sources.iter().zip(&request.args).all(|(expected, actual)| {
                    expected.ordinal() == actual.value.ordinal()
                        && Arc::ptr_eq(expected.pool().array(), actual.value.pool().array())
                        && expected.value_type() == actual.value.value_type()
                })
        });
        if shape && sources_match && request.result_type.data_type() == &state.result {
            assert!(
                state.captured.is_none(),
                "capture terminates at the first exact authored node"
            );
            state.captured = Some(request.clone());
            // Stop fixture construction before final plan sealing or execution.
            // This sentinel is asserted by the helper and is never used in the
            // evaluator assertions or as an optional-fold expected result.
            return Err(SqlConstantEvaluationError::Control(
                CompileControlError::Cancelled,
            ));
        }
        drop(capture_guard);
        constant_evaluator().eval_scalar(request, control)
    }
    fn admit_fold_parent_observed(
        &self,
        binding: &novarocks_sql::binding::SqlFunctionBinding,
        lifecycle: novarocks_functions::PureCallLifecycle,
        control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        constant_evaluator().admit_fold_parent_observed(binding, lifecycle, control)
    }
    fn before_fold_dependency_observed(
        &self,
        input: novarocks_sql::compiler::SqlFoldDependencyInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        constant_evaluator().before_fold_dependency_observed(input, control)
    }
    fn after_fold_dependency_observed(
        &self,
        input: novarocks_sql::compiler::SqlFoldDependencyInput<'_>,
        outcome: novarocks_sql::compiler::SqlFoldEvaluationOutcome<'_>,
    ) {
        constant_evaluator().after_fold_dependency_observed(input, outcome)
    }
}
fn capture(
    statement: novarocks_sql::compiler::SqlStatementInput,
    node: FixtureNode,
    sources: Option<Vec<ConstantValue>>,
    result: DataType,
) -> FoldRequest {
    use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId, ScanReadBudget};
    use novarocks_sql::compiler::*;
    let _serial = CAPTURE_SERIAL.lock().unwrap();
    *CAPTURE_STATE.lock().unwrap() = Some(CaptureState {
        node,
        sources,
        result,
        captured: None,
        observed: Vec::new(),
    });
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let control = SqlCompileControl::unbounded();
    let compile = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([87; 16]).unwrap(),
        statement,
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: Default::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                ..Default::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        catalog.snapshot(),
        &RECORDER,
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
    )
    .try_into_completion()
    .map_err(SqlCompileProgressError::Compile)
    .and_then(|request| SqlCompiler::start(request, &control))
    .and_then(|progress| progress.into_complete())
    .map(|_| ());
    let state = CAPTURE_STATE.lock().unwrap().take().unwrap();
    assert!(
        matches!(
            compile,
            Err(SqlCompileProgressError::Compile(SqlCompileError::Cancelled))
        ),
        "fixture must terminate only at its captured SQL node: {compile:?}; \
         expected node={:?}, source types={:?}, result={:?}; observed={:?}",
        state.node,
        state.sources.as_ref().map(|sources| (
            sources.len(),
            sources
                .iter()
                .take(4)
                .map(|source| (
                    source.ordinal(),
                    source.value_type(),
                    source.pool().backing_identity()
                ))
                .collect::<Vec<_>>()
        )),
        state.result,
        state
            .observed
            .iter()
            .take(8)
            .map(|request| (
                request.args.len(),
                request
                    .args
                    .iter()
                    .take(4)
                    .map(|arg| (
                        arg.value.ordinal(),
                        &arg.value_type,
                        arg.value.pool().backing_identity(),
                    ))
                    .collect::<Vec<_>>(),
                &request.result_type,
            ))
            .collect::<Vec<_>>(),
    );
    state
        .captured
        .expect("SQL must have authored the exact request")
}
// Bind each original checked CV to the exact nested source occurrence in its
// owned SELECT projection. Later SELECT items use the analyzer's existing
// alias substitution, so bindings see the actual source types before folding.
// Keeping Nested also exercises structural-site validation after normalization.
fn statement_with_sources(
    expression: &str,
    hint: &str,
    sources: &[ConstantValue],
) -> novarocks_sql::compiler::SqlStatementInput {
    use novarocks_sql::compiler::{
        SqlConstantExpressionSite, SqlConstantExpressionStep, SqlExpressionConstant,
        SqlStatementInput,
    };
    let projections = (0..sources.len())
        .map(|i| format!("(NULL) AS v{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("SELECT {hint}{projections}, {expression} AS result");
    let statements = novarocks_parser::parse(&sql).unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("one query")
    };
    let constants = sources
        .iter()
        .enumerate()
        .map(|(column, value)| {
            SqlExpressionConstant::new(
                vec![],
                SqlConstantExpressionSite::SelectProjection { column },
                value.clone(),
            )
            .with_expression_path(vec![SqlConstantExpressionStep::Nested])
        })
        .collect::<Vec<_>>()
        .into();
    SqlStatementInput::query_with_expression_constants(
        Box::new(query.clone()),
        constants,
        &novarocks_sql::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
}
fn identity(arg: FoldArg) -> FoldRequest {
    // A same-type root CAST is removed by analyzer::value_conversion. Obtain
    // the real shared conversion author's Identity from a same-type SQL
    // comparison instead; unlike a Carrier/Call it has no hidden receipt.
    let sources = vec![i64(1).value, i64(2).value];
    let mut request = capture(
        statement_with_sources("v0 = v1", "", &sources),
        FixtureNode::BinaryOp(BinOp::Eq, DecimalOverflowPolicy::OutputNull),
        Some(sources),
        DataType::Boolean,
    );
    let FrozenFoldNode::Intrinsic(FrozenFoldIntrinsic::Comparison { left, right, .. }) =
        &request.kind
    else {
        panic!("the real SQL comparison must publish both conversion plans")
    };
    assert!(matches!(left, ConversionPlan::Identity));
    assert!(matches!(right, ConversionPlan::Identity));
    let plan = left.clone();
    // Compose only the receipt-free authored intrinsic plan. No bound call or
    // prepared kernel is forged; both complete identity types are identical.
    request.kind = FrozenFoldNode::Intrinsic(FrozenFoldIntrinsic::Cast { plan });
    request.result_type = arg.value_type.clone();
    request.args = vec![arg];
    request
}
fn sql_target(target: &DataType) -> String {
    match target {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Utf8 => "VARCHAR".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Decimal128(p, s) if *s >= 0 => format!("DECIMAL({p},{s})"),
        other => panic!("this conversion has no SQL fixture spelling: {other:?}"),
    }
}
fn request(kind: FixtureNode, args: Vec<FoldArg>, result: DataType) -> FoldRequest {
    if matches!(kind, FixtureNode::Cast(_))
        && args.len() == 1
        && args[0].value.value_type().data_type()
            == FunctionValueType::new(result.clone(), true).data_type()
    {
        let mut request = identity(FoldArg {
            value: args[0].value.clone(),
            value_type: args[0].value.value_type().clone(),
        });
        // Deliberate declared-source mutations in forgery tests must remain
        // visible to the exact-source gate, never to SQL binding selection.
        request.args = args;
        return request;
    }
    let vars = (0..args.len()).map(|i| format!("v{i}")).collect::<Vec<_>>();
    let expression = match &kind {
        FixtureNode::Function { name } => format!("{name}({})", vars.join(",")),
        FixtureNode::Cast(_) => {
            assert_eq!(vars.len(), 1);
            format!("CAST({} AS {})", vars[0], sql_target(&result))
        }
        FixtureNode::UnaryOp(op) => {
            assert_eq!(vars.len(), 1);
            format!(
                "{}({})",
                match op {
                    UnOp::Not => "NOT ",
                    UnOp::Negate => "-",
                    UnOp::BitwiseNot => "~",
                },
                vars[0]
            )
        }
        FixtureNode::BinaryOp(op, _) => {
            assert_eq!(vars.len(), 2);
            format!(
                "{} {} {}",
                vars[0],
                match op {
                    BinOp::Add => "+",
                    BinOp::Sub => "-",
                    BinOp::Mul => "*",
                    BinOp::Div => "/",
                    BinOp::Mod => "%",
                    BinOp::Eq => "=",
                    BinOp::Ne => "!=",
                    BinOp::Lt => "<",
                    BinOp::Le => "<=",
                    BinOp::Gt => ">",
                    BinOp::Ge => ">=",
                    BinOp::EqForNull => "<=>",
                    BinOp::And => "AND",
                    BinOp::Or => "OR",
                },
                vars[1]
            )
        }
    };
    let hint = if matches!(
        kind,
        FixtureNode::BinaryOp(_, DecimalOverflowPolicy::ReportError)
            | FixtureNode::Cast(DecimalOverflowPolicy::ReportError)
    ) {
        "/*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ "
    } else {
        ""
    };
    let sources = args.iter().map(|arg| arg.value.clone()).collect::<Vec<_>>();
    let statement = statement_with_sources(&expression, hint, &sources);
    let mut request = capture(
        statement,
        kind,
        Some(sources),
        FunctionValueType::new(result, true).data_type().clone(),
    );
    for (actual, supplied) in request.args.iter().zip(&args) {
        assert_eq!(actual.value.ordinal(), supplied.value.ordinal());
        assert!(Arc::ptr_eq(
            actual.value.pool().array(),
            supplied.value.pool().array()
        ));
        assert_eq!(actual.value.value_type(), supplied.value.value_type());
        assert!(Arc::ptr_eq(
            actual.value.pool().field_ref(),
            supplied.value.pool().field_ref()
        ));
    }
    request.args = args;
    request
}
fn cast(args: Vec<FoldArg>, target: DataType) -> FoldRequest {
    request(
        FixtureNode::Cast(DecimalOverflowPolicy::OutputNull),
        args,
        target,
    )
}
fn binary(op: BinOp, args: Vec<FoldArg>, result: DataType) -> FoldRequest {
    request(
        FixtureNode::BinaryOp(op, DecimalOverflowPolicy::OutputNull),
        args,
        result,
    )
}

fn evaluate(request: &FoldRequest) -> ConstantValue {
    let value = constant_evaluator()
        .eval_scalar(request, &TestControl)
        .unwrap()
        .unwrap();
    assert!(
        value
            .value_type()
            .exactly_equals_observed::<ConstantError>(&request.result_type, || Ok(()))
            .unwrap()
    );
    assert_eq!(value.field().name(), "literal");
    assert_eq!(value.pool().resource_facts().rows, 1);
    value
}
fn decimal_value(value: &ConstantValue) -> i128 {
    value
        .pool()
        .array()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .value(value.ordinal() as usize)
}

#[test]
fn folds_int32_addition_without_an_integer_carrier_roundtrip() {
    // Reusing one immutable source mirrors SQL's equal-constant interning;
    // the two arguments still assert their exact original checked backing.
    let request = binary(BinOp::Add, vec![i32(1); 2], DataType::Int64);
    assert!(
        request
            .args
            .iter()
            .all(|arg| arg.value_type.data_type() == &DataType::Int32)
    );
    assert_eq!(request.result_type.data_type(), &DataType::Int64);
    let output = evaluate(&request);
    assert_eq!(
        output
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}
#[test]
fn folds_date_format_and_utf8_to_date32_through_real_kernels() {
    let date = arg(Arc::new(Date32Array::from(vec![18262])), false);
    let output = evaluate(&request(
        FixtureNode::Function {
            name: "date_format".into(),
        },
        vec![date, text("%Y-%m-%d")],
        DataType::Utf8,
    ));
    assert_eq!(
        output
            .utf8_observed(CompilePhase::FunctionSpecialization, &TestControl)
            .unwrap(),
        Some("2020-01-01")
    );
    assert_eq!(
        evaluate(&cast(vec![text("2020-01-01")], DataType::Date32))
            .try_date32()
            .unwrap(),
        Some(18262)
    );
}
#[test]
fn folds_decimal_multiplication_and_checked_rounding_cast_with_exact_coefficients() {
    assert_eq!(
        decimal_value(&evaluate(&binary(
            BinOp::Mul,
            vec![decimal(125, 10, 2), decimal(400, 10, 2)],
            DataType::Decimal128(20, 4)
        ))),
        50000
    );
    assert_eq!(
        decimal_value(&evaluate(&cast(
            vec![decimal(99_999_999, 8, 3)],
            DataType::Decimal128(8, 2)
        ))),
        10_000_000
    );
    let overflow = evaluate(&cast(
        vec![decimal(99_999_999, 8, 3)],
        DataType::Decimal128(7, 2),
    ));
    assert!(
        overflow
            .is_null_observed(CompilePhase::FunctionSpecialization, &TestControl)
            .unwrap()
    );
}
#[test]
fn declines_unmapped_unary_shapes_without_a_fake_kernel() {
    for kind in [
        FixtureNode::UnaryOp(UnOp::Negate),
        FixtureNode::UnaryOp(UnOp::BitwiseNot),
    ] {
        assert!(matches!(
            constant_evaluator()
                .eval_scalar(&request(kind, vec![i32(1)], DataType::Int32), &TestControl),
            Ok(None)
        ));
    }
    let boolean = arg(
        Arc::new(arrow::array::BooleanArray::from(vec![true])),
        false,
    );
    assert_eq!(
        evaluate(&request(
            FixtureNode::UnaryOp(UnOp::Not),
            vec![boolean],
            DataType::Boolean
        ))
        .try_boolean()
        .unwrap(),
        Some(false)
    );
    // Unknown names are rejected by SQL analysis; no valid FrozenFoldCall can
    // be manufactured for a name that has no binding or installed owner.
    use novarocks_sql::compiler::*;
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let control = SqlCompileControl::unbounded();
    let result = SqlFinalPlanCompileRequest::new(
        novarocks_physical_plan::PlanVersionId::try_new([88; 16]).unwrap(),
        SqlStatementInput::sql("SELECT no_such_novarocks_function(1)"),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: Default::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: Default::default(),
        },
        SqlPlanningEnvironment::Distributed,
        catalog.snapshot(),
        constant_evaluator(),
        policy(),
        control.clone(),
        novarocks_physical_plan::PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        novarocks_physical_plan::ScanReadBudget {
            max_batch_rows: 64,
            max_batch_bytes: 1 << 20,
        },
        DEFAULT_COMPLETION_LIMITS,
    )
    .try_into_completion()
    .map_err(SqlCompileProgressError::Compile)
    .and_then(|request| SqlCompiler::start(request, &control))
    .and_then(|progress| progress.into_complete());
    let error = result
        .err()
        .expect("unbound function must fail before a frozen-call request exists");
    assert!(
        matches!(
            error,
            SqlCompileProgressError::Compile(SqlCompileError::Analyze(_))
        ),
        "{error}"
    );
    assert!(
        error.to_string().contains("no_such_novarocks_function"),
        "{error}"
    );
}

fn assert_invalid_arithmetic(result: &Result<Option<ConstantValue>, SqlConstantEvaluationError>) {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let Err(SqlConstantEvaluationError::Outer(source)) = result else {
        panic!("invalid frozen arithmetic must retain its outer producer: {result:?}")
    };
    assert_eq!(
        source.failure_fact(),
        novarocks_functions::ArithmeticPrepareError::TypeMismatch.failure_fact()
    );
    assert_eq!(source.failure_fact().class(), FailureClass::Invariant);
    assert!(matches!(
        source.downcast_ref::<novarocks_local_program::ProgramPrimitiveError>(),
        Some(novarocks_local_program::ProgramPrimitiveError::Arithmetic(
            novarocks_functions::ArithmeticPrepareError::TypeMismatch
        ))
    ));
}
#[test]
fn division_by_zero_is_typed_null_and_forged_nonnullable_arithmetic_is_invalid() {
    let mut request = binary(BinOp::Div, vec![i32(1), i32(0)], DataType::Float64);
    assert!(
        evaluate(&request)
            .is_null_observed(CompilePhase::FunctionSpecialization, &TestControl)
            .unwrap()
    );
    request.result_type = request.result_type.with_nullable(false).unwrap();
    assert_invalid_arithmetic(&constant_evaluator().eval_scalar(&request, &TestControl));
}
#[test]
fn exact_source_gate_rejects_carrier_domain_and_nullable_forgery() {
    let original = i32(7);
    for ty in [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Int64, false),
    ] {
        let mut forged = original.clone();
        forged.value_type = ty;
        assert!(matches!(
            constant_evaluator().eval_scalar(&cast(vec![forged], DataType::Int64), &TestControl),
            Err(SqlConstantEvaluationError::Constant(
                ConstantError::Invalid(_)
            ))
        ));
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let mut forged = text("{}");
    forged.value_type = json;
    assert!(matches!(
        constant_evaluator().eval_scalar(&cast(vec![forged], DataType::Utf8), &TestControl),
        Err(SqlConstantEvaluationError::Constant(
            ConstantError::Invalid(_)
        ))
    ));
}
#[test]
fn authored_nominal_and_nested_domains_decline_without_carrier_retagging() {
    for (ty, array) in [
        (
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
            Arc::new(StringArray::from(vec!["{}"])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                true,
                ValueLogicalType::Variant,
            )
            .unwrap(),
            Arc::new(arrow::array::LargeBinaryArray::from(vec![b"x".as_slice()])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(DataType::Binary, true, ValueLogicalType::Hll)
                .unwrap(),
            Arc::new(arrow::array::BinaryArray::from(vec![b"x".as_slice()])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                true,
                ValueLogicalType::Bitmap,
            )
            .unwrap(),
            Arc::new(arrow::array::BinaryArray::from(vec![b"x".as_slice()])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
            arrow::array::new_null_array(&DataType::FixedSizeBinary(16), 1),
        ),
    ] {
        let value = pool(array, ty.clone()).value(0).unwrap();
        assert!(matches!(
            constant_evaluator().eval_scalar(
                &identity(FoldArg {
                    value,
                    value_type: ty
                }),
                &TestControl
            ),
            Ok(None)
        ));
    }
    let child =
        Field::new("item", DataType::Utf8, true).with_metadata(std::collections::HashMap::<
            String,
            String,
        >::from([(
            NR_LOGICAL_TYPE_KEY.into(),
            "json".into(),
        )]));
    let source_type = DataType::List(Arc::new(child));
    let ty = json_list_type(&source_type);
    let null = typed_arg(arrow::array::new_null_array(&source_type, 1), ty.clone());
    assert_eq!(null.value.field().data_type(), &source_type);
    assert_eq!(
        null.value_type.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    let request = identity(null);
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Ok(None)
    ));
}
#[test]
fn checked_type_refusal_and_later_source_forgery_are_not_hidden_by_domain_decline() {
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let value = pool(Arc::new(StringArray::from(vec!["{}"])), json.clone())
        .value(0)
        .unwrap();
    assert!(
        FunctionValueType::try_with_logical_type(DataType::Int64, false, ValueLogicalType::Json)
            .is_err()
    );
    // A malformed complete declaration cannot enter the evaluator. A later
    // legitimate JSON declaration with a forged source still fails before an
    // earlier argument's domain decline hides it.
    let invalid = FoldArg {
        value: value.clone(),
        value_type: FunctionValueType::new(DataType::Int64, false),
    };
    let request = request(
        FixtureNode::Function {
            name: "coalesce".into(),
        },
        vec![
            FoldArg {
                value,
                value_type: json,
            },
            invalid,
        ],
        DataType::Utf8,
    );
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Err(SqlConstantEvaluationError::Constant(_))
    ));
}
#[test]
fn exact_largeint_constant_and_nonzero_pool_ordinal_keep_nominal_identity() {
    let ty = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let value = i128::from(i64::MAX) + 17;
    let bytes = [
        1_i128.to_be_bytes(),
        value.to_be_bytes(),
        (-3_i128).to_be_bytes(),
    ];
    let array = Arc::new(
        arrow::array::FixedSizeBinaryArray::try_from_iter(bytes.iter().map(|v| v.as_slice()))
            .unwrap(),
    );
    let source = pool(array, ty.clone()).value(1).unwrap();
    let mut request = cast(
        vec![FoldArg {
            value: source,
            value_type: ty.clone(),
        }],
        ty.data_type().clone(),
    );
    request.result_type = ty;
    assert_eq!(evaluate(&request).try_largeint().unwrap(), Some(value));
    request.args[0].value_type = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Err(SqlConstantEvaluationError::Constant(_))
    ));
}
#[test]
fn float32_signaling_nan_and_signed_zero_never_widen_through_float64_syntax() {
    for bits in [
        0x7f80_0001,
        0xff80_0123,
        0x8000_0000,
        0x0000_0001,
        0x7f80_0000,
    ] {
        let source = arg(
            Arc::new(Float32Array::from(vec![f32::from_bits(bits)])),
            false,
        );
        let output = evaluate(&cast(vec![source], DataType::Float32));
        assert_eq!(
            output
                .pool()
                .array()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            bits
        );
    }
}
#[test]
fn newly_materialized_unsigned_negative_scale_and_nested_metadata_outputs_are_cv() {
    // The optimizer does not admit UInt32 root folding. Preserve both
    // independent properties: the cast owner converts Int32 to UInt32, then
    // the FE port materializes an exact CV through a valid Identity node.
    let target = FunctionValueType::new(DataType::UInt32, true);
    let source = i32(7);
    let converted = novarocks_execution::exec::expr::compiled_program::constant_node::evaluate_constant_node(
        novarocks_execution::exec::expr::compiled_program::constant_node::ConstantNode::Conversion(
            novarocks_execution::exec::expr::compiled_program::constant_node::ConstantConversion::Carrier(
                novarocks_execution::exec::expr::compiled_program::constant_node::ConstantCarrierStep {
                    target: &target,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: false,
                }
            )
        ),
        &[source.value], &target, &TestControl, CompilePhase::FunctionSpecialization,
    ).unwrap();
    let novarocks_execution::exec::expr::compiled_program::constant_node::ConstantNodeOutcome::Value(converted) = converted else {
        panic!("the existing Int32-to-UInt32 cast must produce a value");
    };
    let output = evaluate(&identity(typed_arg(converted, target)));
    assert_eq!(
        output
            .pool()
            .array()
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .unwrap()
            .value(0),
        7
    );
    let output = evaluate(&cast(
        vec![decimal(123, 10, -2)],
        DataType::Decimal128(10, -2),
    ));
    assert_eq!(decimal_value(&output), 123);
    let field = Arc::new(Field::new("source", DataType::Int32, true).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "provider.fact".into(),
            "kept".into(),
        )]),
    ));
    let source_type = DataType::List(field);
    let source = arg(arrow::array::new_null_array(&source_type, 1), true);
    let request = cast(vec![source.clone()], source_type.clone());
    let output = evaluate(&request);
    assert_eq!(output.value_type(), &request.result_type);
    assert_eq!(source.value.field().data_type(), &source_type);
    let DataType::List(source_field) = source.value.field().data_type() else {
        panic!("original source is a List");
    };
    assert_eq!(source_field.name(), "source");
    assert_eq!(
        source_field.metadata().get("provider.fact").unwrap(),
        "kept"
    );
    let DataType::List(output_field) = output.value_type().data_type() else {
        panic!("frozen execution result is a List");
    };
    assert_eq!(output_field.name(), "item");
    assert!(output_field.metadata().as_arc().is_none());
    assert!(output.value_type().nullable());
    assert!(output_field.is_nullable());
}
#[test]
fn checked_decimal_error_remains_ordinary_evaluation_while_policy_null_is_success() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let request = request(
            FixtureNode::BinaryOp(BinOp::Add, policy),
            vec![
                decimal(99_999_999_999_999_999_999_999_999_999_999_999_999, 38, 0),
                i64(1),
            ],
            DataType::Decimal128(38, 0),
        );
        let result = constant_evaluator().eval_scalar(&request, &TestControl);
        if policy == DecimalOverflowPolicy::OutputNull {
            assert!(
                result
                    .unwrap()
                    .unwrap()
                    .is_null_observed(CompilePhase::FunctionSpecialization, &TestControl)
                    .unwrap()
            );
        } else {
            let Err(SqlConstantEvaluationError::Source(source)) = result else {
                panic!("typed data evaluation source");
            };
            use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
            assert_eq!(source.failure_fact().class(), FailureClass::Data);
            assert!(
                source
                    .detail()
                    .contains("'add' operation involving decimal values overflows")
            );
            assert!(
                source
                    .downcast_ref::<novarocks_functions::RowDataError>()
                    .is_some()
            );
        }
    }
}

#[derive(Default)]
struct Control {
    checks: Mutex<Vec<u32>>,
    fail_at: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut checks = self.checks.lock().unwrap();
        let index = checks.len();
        checks.push(units);
        if let Some((at, cause)) = self.fail_at.clone()
            && at == index
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn assert_all_callbacks(
    request: &FoldRequest,
    expected: fn(&Result<Option<ConstantValue>, SqlConstantEvaluationError>),
) {
    let good = Control::default();
    let result = constant_evaluator().eval_scalar(request, &good);
    expected(&result);
    let trace = good.checks.lock().unwrap().clone();
    assert_eq!(trace[0], 0);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
            KernelFailure::ResourceExhausted,
        )),
    ] {
        for at in 0..trace.len() {
            let refusing = Control {
                fail_at: Some((at, cause.clone())),
                ..Default::default()
            };
            assert!(
                matches!(constant_evaluator().eval_scalar(request, &refusing), Err(SqlConstantEvaluationError::Control(actual)) if actual == cause)
            );
            assert_eq!(*refusing.checks.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn real_wide_prepared_call_keeps_entry_quantum_tail_and_all_three_original_causes() {
    let request = request(
        FixtureNode::Function {
            name: "coalesce".into(),
        },
        vec![i64(7); 320],
        DataType::Int64,
    );
    assert_all_callbacks(&request, |result| assert!(matches!(result, Ok(Some(_)))));
    let good = Control::default();
    constant_evaluator().eval_scalar(&request, &good).unwrap();
    assert!(good.checks.lock().unwrap().contains(&256));
}
#[test]
fn actual_metadata_decline_and_successful_output_pool_keep_all_callback_prefixes() {
    let metadata: std::collections::HashMap<String, String> = (0..255)
        .map(|i| (format!("provider.fact.{i}"), "value".into()))
        .chain([(NR_LOGICAL_TYPE_KEY.into(), "json".into())])
        .collect();
    let ty = DataType::List(Arc::new(
        Field::new("item", DataType::Utf8, true).with_metadata(metadata),
    ));
    let complete = json_list_type(&ty);
    let source = typed_arg(arrow::array::new_null_array(&ty, 1), complete.clone());
    assert_eq!(source.value.field().data_type(), &ty);
    let DataType::List(source_field) = source.value.field().data_type() else {
        panic!("original source is a List");
    };
    assert_eq!(source_field.metadata().len(), 256);
    assert_eq!(
        source_field.metadata().get(NR_LOGICAL_TYPE_KEY).unwrap(),
        "json"
    );
    assert_eq!(
        source.value_type.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    let mut request = cast(vec![source], complete.data_type().clone());
    request.result_type = complete;
    assert_all_callbacks(&request, |result| assert!(matches!(result, Ok(None))));
    assert_all_callbacks(
        &binary(BinOp::Add, vec![i32(1); 2], DataType::Int64),
        |result| assert!(matches!(result, Ok(Some(_)))),
    );
}
#[test]
fn ordinary_row_error_and_invalid_arithmetic_observe_exact_original_completion() {
    let request = request(
        FixtureNode::BinaryOp(BinOp::Add, DecimalOverflowPolicy::ReportError),
        vec![
            decimal(99_999_999_999_999_999_999_999_999_999_999_999_999, 38, 0),
            i64(1),
        ],
        DataType::Decimal128(38, 0),
    );
    assert_all_callbacks(&request, |result| {
        assert!(matches!(
            result,
            Err(SqlConstantEvaluationError::Source(source))
            if novarocks_execution_contract::failure::FailureProjection::failure_fact(source).class() == novarocks_execution_contract::failure::FailureClass::Data
        ))
    });
    let mut request = binary(BinOp::Div, vec![i32(1), i32(0)], DataType::Float64);
    request.result_type = request.result_type.with_nullable(false).unwrap();
    assert_all_callbacks(&request, assert_invalid_arithmetic);
}
#[test]
fn source_annotations_do_not_mint_a_type_resource_refusal() {
    let field =
        Field::new("item", DataType::Utf8, true).with_metadata(std::collections::HashMap::<
            String,
            String,
        >::from([(
            "provider.fact".into(),
            "x".repeat(novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES + 1),
        )]));
    let annotated = DataType::List(Arc::new(field));
    let target = FunctionValueType::new(annotated.clone(), true);
    let source = typed_arg(
        arrow::array::new_null_array(target.data_type(), 1),
        target.clone(),
    );
    let mut request = identity(source);
    request.result_type = target;
    let plain = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    );
    // Source annotations no longer establish execution resources or semantics.
    assert_eq!(request.result_type, plain);
    let DataType::List(normalized) = request.result_type.data_type() else {
        panic!("frozen result is a List");
    };
    assert!(normalized.metadata().as_arc().is_none());
    let DataType::List(original) = &annotated else {
        panic!("original source is a List");
    };
    assert_eq!(
        original.metadata()["provider.fact"].len(),
        novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES + 1
    );
    // Preserve every actual callback's first cause, including ResourceExhausted,
    // rather than inventing an annotation-driven execution validation boundary.
    assert_all_callbacks(&request, |result| assert!(matches!(result, Ok(Some(_)))));
}
#[test]
fn explicit_result_policy_refusal_is_typed_preparation_and_has_no_later_callback() {
    let mut request = binary(BinOp::Add, vec![i32(1); 2], DataType::Int64);
    request.constant_policy.max_rows = 0;
    let good = Control::default();
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &good),
        Err(SqlConstantEvaluationError::Constant(ConstantError::Limit(
            _
        )))
    ));
    let trace = good.checks.lock().unwrap().clone();
    let refusing = Control {
        fail_at: Some((trace.len(), CompileControlError::Cancelled)),
        ..Default::default()
    };
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &refusing),
        Err(SqlConstantEvaluationError::Constant(ConstantError::Limit(
            _
        )))
    ));
    assert_eq!(*refusing.checks.lock().unwrap(), trace);
}

#[test]
fn malformed_selected_utf8_bytes_decline_without_constructing_invalid_str() {
    use arrow::buffer::Buffer;
    for ty in [DataType::Utf8, DataType::LargeUtf8] {
        let offsets = if ty == DataType::Utf8 {
            Buffer::from_slice_ref([0i32, 1])
        } else {
            Buffer::from_slice_ref([0i64, 1])
        };
        // SAFETY: This deliberate invalid-UTF8 fixture is never read as &str.
        // The production guard examines only its validated offsets/raw bytes.
        let data = unsafe {
            arrow::array::ArrayData::builder(ty)
                .len(1)
                .add_buffer(offsets)
                .add_buffer(Buffer::from(vec![0xff]))
                .build_unchecked()
        };
        let output = arrow::array::make_array(data);
        let good = TestControl;
        let mut work =
            CompileCheckpoints::try_new(&good, CompilePhase::FunctionSpecialization).unwrap();
        assert!(!utf8_output_fits(&output, &mut work).unwrap());
        work.finish().unwrap();
    }
}

fn assert_regexp_count_literal_regex_syntax_source(error: &SqlConstantEvaluationError) {
    use novarocks_failure_contract::FailureProjection;
    let SqlConstantEvaluationError::Source(source) = error else {
        panic!("literal regexp_count preserves its typed source: {error:?}");
    };
    assert_eq!(
        source.failure_fact(),
        novarocks_functions::DataFailureReason::InvalidValue.failure_fact(),
        "literal regex syntax keeps its exact originating data fact"
    );
    let original = source
        .downcast_ref::<novarocks_functions::RowDataError>()
        .expect("the FE bridge retains the original selected row failure");
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(original);
    let mut has_regex_syntax = false;
    while let Some(current) = cause {
        if matches!(
            current.downcast_ref::<regex::Error>(),
            Some(regex::Error::Syntax(_))
        ) {
            has_regex_syntax = true;
            break;
        }
        cause = current.source();
    }
    assert!(
        has_regex_syntax,
        "the original regex::Error::Syntax remains in the source chain: {error:?}"
    );
}

#[test]
fn regexp_count_fold_receipt_reuses_literal_error_null_and_selected_pool_contract() {
    use novarocks_sql::compiler::FrozenFoldArgumentSource;
    use novarocks_type_contract::RegexpCountPatternSource as Source;
    let selected = pool(
        Arc::new(StringArray::from(vec!["a", "[0-9"])),
        FunctionValueType::new(DataType::Utf8, false),
    )
    .value(1)
    .unwrap();
    let make = |text_arg: FoldArg, pattern: FoldArg| {
        request(
            FixtureNode::Function {
                name: "regexp_count".into(),
            },
            vec![text_arg, pattern],
            DataType::Int64,
        )
    };
    let request = make(
        text("aaa"),
        FoldArg {
            value: selected.clone(),
            value_type: selected.value_type().clone(),
        },
    );
    let FrozenFoldNode::Call(call) = &request.kind else {
        panic!("real regexp call")
    };
    assert_eq!(
        call.source(),
        FrozenFoldArgumentSource::RegexpCountPattern(Source::NativeV1Utf8LiteralWhenPresent)
    );
    assert_eq!(request.args[1].value.ordinal(), 1);
    assert_eq!(call.arguments()[1].value.ordinal(), 1);
    assert!(Arc::ptr_eq(
        request.args[1].value.pool().array(),
        selected.pool().array()
    ));
    assert!(Arc::ptr_eq(
        call.arguments()[1].value.pool().array(),
        selected.pool().array()
    ));
    let error = constant_evaluator()
        .eval_scalar(&request, &TestControl)
        .unwrap_err();
    assert_regexp_count_literal_regex_syntax_source(&error);
    let SqlConstantEvaluationError::Source(source) = &error else {
        unreachable!("the exact typed source was checked above");
    };
    assert!(
        source
            .detail()
            .starts_with("Invalid regex expression: [0-9."),
        "{error:?}"
    );
    let null = || arg(Arc::new(StringArray::from(vec![None::<&str>])), true);
    assert_eq!(
        evaluate(&make(null(), text("[0-9"))).try_i64().unwrap(),
        None
    );
    assert_eq!(
        evaluate(&make(text("aaa"), null())).try_i64().unwrap(),
        None
    );
    assert_eq!(
        evaluate(&make(text("aaa"), text("a"))).try_i64().unwrap(),
        Some(3)
    );
    // A frozen receipt cannot be relabeled or attached to another function:
    // FrozenFoldCall fields are private, and SQL source-author tests own the
    // foreign-binding rejection. No mutated concat fixture is constructed.
}

#[test]
fn regexp_count_real_sql_fold_author_attaches_existing_emitted_source_before_error_decline() {
    use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId, ScanReadBudget};
    use novarocks_sql::compiler::*;
    use novarocks_type_contract::RegexpCountPatternSource as Source;
    struct Recording;
    static SEEN: Mutex<Vec<(Option<Source>, bool)>> = Mutex::new(Vec::new());
    impl SqlConstantEvaluator for Recording {
        fn eval_scalar(
            &self,
            request: &FoldRequest,
            control: &dyn PureCompileControl,
        ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
            let result = constant_evaluator().eval_scalar(request, control);
            if matches!(&request.kind, FrozenFoldNode::Call(call) if call.binding().function_id.as_str()=="builtin.scalar/regexp_count/v1")
            {
                let failed = if let Err(error) = &result {
                    assert_regexp_count_literal_regex_syntax_source(error);
                    let SqlConstantEvaluationError::Source(source) = error else {
                        unreachable!("the exact typed source was checked above");
                    };
                    assert!(source.detail().starts_with("Invalid regex expression:"));
                    true
                } else {
                    false
                };
                SEEN.lock()
                    .unwrap()
                    .push((match &request.kind {
                        FrozenFoldNode::Call(call) => match call.source() {
                            novarocks_sql::compiler::FrozenFoldArgumentSource::RegexpCountPattern(source) => Some(source),
                            _ => None,
                        }, _ => None,
                    }, failed));
            }
            result
        }
    }
    static RECORDER: Recording = Recording;
    SEEN.lock().unwrap().clear();
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let control = SqlCompileControl::unbounded();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([87; 16]).unwrap(),
        SqlStatementInput::sql("SELECT regexp_count('test string','[0-9')"),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: Default::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                ..Default::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        catalog.snapshot(),
        &RECORDER,
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
    )
    .try_into_completion()
    .unwrap();
    let owner = SqlCompiler::start(request, &control)
        .unwrap()
        .into_complete()
        .unwrap()
        .into_plan();
    let seen = SEEN.lock().unwrap();
    assert!(
        !seen.is_empty(),
        "ordinary SQL must exercise the real fold author"
    );
    assert!(
        seen.iter()
            .all(|row| *row == (Some(Source::NativeV1Utf8LiteralWhenPresent), true)),
        "the fold author must preserve the exact emitted source and typed decline: {seen:?}"
    );
    assert!(owner.plan().fragments().values().any(|fragment| fragment.expressions().iter().any(|(_,expression)| matches!(&expression.kind,novarocks_physical_plan::ExprKind::FunctionCall {function,..} if function.function_id.as_str()=="builtin.scalar/regexp_count/v1"))), "failed literal evaluation must retain the actual call, never fold to NULL");
}

#[test]
fn nominal_json_policy_decline_is_payload_independent_without_retagging() {
    let json_type =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    for input in [
        Some("{}"),
        Some("[]"),
        Some("null"),
        Some("128"),
        Some("2147483648"),
        Some("not-json"),
        None,
    ] {
        let argument = typed_arg(Arc::new(StringArray::from(vec![input])), json_type.clone());
        // The old fixture used a name-only JSON-to-Variant call that is not a
        // valid catalog binding. A valid authored Identity isolates the same
        // payload-independent nominal-domain decline without a fake call.
        let request = identity(argument);
        // The port declines the authored nominal domain for every payload;
        // it must neither retag the source nor fabricate a folded NULL.
        assert!(
            constant_evaluator()
                .eval_scalar(&request, &TestControl)
                .unwrap()
                .is_none()
        );
        assert_eq!(request.args[0].value.value_type(), &json_type);
    }
}

#[test]
fn call_prepared_result_nullability_is_checked_before_the_arena_nonnull_decline() {
    let mut request = request(
        FixtureNode::Function {
            name: "nullif".into(),
        },
        vec![i64(1); 2],
        DataType::Int64,
    );
    let FrozenFoldNode::Call(call) = &request.kind else {
        panic!("real nullif")
    };
    assert!(call.prepared_result_type().nullable());
    assert!(
        evaluate(&request)
            .is_null_observed(CompilePhase::FunctionSpecialization, &TestControl)
            .unwrap()
    );
    request.result_type = request.result_type.with_nullable(false).unwrap();
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Ok(None)
    ));
}
