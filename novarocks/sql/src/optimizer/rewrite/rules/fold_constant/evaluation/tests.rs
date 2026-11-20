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

use std::sync::Mutex;

use arrow::array::{BooleanArray, Int64Array};
use arrow::datatypes::DataType;
use novarocks_expression_contract::{BinaryOperator, ExprId, ExprKind};
use novarocks_type_contract::{CompileControlError, DecimalOverflowPolicy, FunctionValueType};

use super::*;
use crate::common::{BinOp, LiteralValue};
use crate::optimizer::rewrite::context::unbounded_rewrite_test_control;
use crate::optimizer::scalar::{HashableLiteral, ScalarNode};

fn context() -> RewriteContext<'static> {
    let mut ctx = RewriteContext::for_query(Vec::<String>::new());
    ctx.set_function_catalog(crate::functions::builtin_sql_function_catalog().snapshot());
    ctx.set_sealed_functions(
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
    );
    ctx
}

fn literal(arena: &mut ScalarArena, value: LiteralValue, ty: FunctionValueType) -> ScalarId {
    arena.intern(ScalarNode::Literal(HashableLiteral(value)), ty)
}

fn integer(arena: &mut ScalarArena, value: i64, data_type: DataType) -> ScalarId {
    literal(
        arena,
        LiteralValue::Int(value),
        FunctionValueType::new(data_type, false),
    )
}

fn binary(
    arena: &mut ScalarArena,
    op: BinOp,
    left: ScalarId,
    right: ScalarId,
    ty: FunctionValueType,
) -> ScalarId {
    arena.intern(
        ScalarNode::BinaryOp {
            op,
            left,
            right,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        ty,
    )
}

#[test]
fn candidate_and_two_constants_lower_to_one_root() {
    let mut arena = ScalarArena::new();
    let left = integer(&mut arena, 19, DataType::Int64);
    let right = integer(&mut arena, 23, DataType::Int64);
    let ty = FunctionValueType::new(DataType::Int64, true);
    let root = binary(&mut arena, BinOp::Add, left, right, ty);

    let evaluated = fold_node(root, &arena, &context()).unwrap();
    let source = evaluated.unit.unwrap();
    let unit = source.unit();
    assert!(unit.ports().is_empty());
    assert_eq!(unit.roots().len(), 1);
    assert_eq!(unit.definitions().len(), 3);
    let root = unit.root(source.root()).unwrap();
    let ExprKind::Binary {
        left,
        right,
        op: BinaryOperator::Add,
        ..
    } = unit.definition(root.definition).unwrap().kind
    else {
        panic!("expected the arithmetic candidate as the sole root")
    };
    for child in [left, right] {
        assert!(matches!(
            unit.definition(child).unwrap().kind,
            ExprKind::Constant(_)
        ));
    }
    let FoldNodeOutcome::Folded(value) = evaluated.outcome else {
        panic!("arithmetic candidate should fold")
    };
    let values = value
        .pool()
        .array()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(value.ordinal() as usize), 42);
}

#[test]
fn comparison_coercion_is_inside_the_boolean_root() {
    let mut arena = ScalarArena::new();
    let left = integer(&mut arena, 19, DataType::Int32);
    let right = integer(&mut arena, 23, DataType::Int64);
    let ty = FunctionValueType::new(DataType::Boolean, false);
    let input = binary(&mut arena, BinOp::Lt, left, right, ty);

    let evaluated = fold_node(input, &arena, &context()).unwrap();
    let source = evaluated.unit.unwrap();
    let unit = source.unit();
    let root = unit
        .definition(unit.root(source.root()).unwrap().definition)
        .unwrap();
    assert_eq!(root.ty.data_type(), &DataType::Boolean);
    let ExprKind::Binary {
        left,
        op: BinaryOperator::Lt,
        ..
    } = root.kind
    else {
        panic!("the comparison owns the root after child coercion")
    };
    assert!(matches!(
        unit.definition(left).unwrap().kind,
        ExprKind::Cast {
            target: DataType::Int64,
            ..
        }
    ));
    let FoldNodeOutcome::Folded(value) = evaluated.outcome else {
        panic!("coerced comparison should fold")
    };
    let values = value
        .pool()
        .array()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(values.value(value.ordinal() as usize));
}

#[test]
fn widened_nullable_root_does_not_admit_null_to_required_arena_type() {
    let mut arena = ScalarArena::new();
    let required = FunctionValueType::new(DataType::Boolean, false);
    let truth = literal(&mut arena, LiteralValue::Bool(true), required.clone());
    let null = literal(
        &mut arena,
        LiteralValue::Null,
        FunctionValueType::new(DataType::Boolean, true),
    );
    let input = binary(&mut arena, BinOp::And, truth, null, required);

    let evaluated = fold_node(input, &arena, &context()).unwrap();
    assert!(evaluated.unit.unwrap().value_type().nullable());
    assert!(!arena.value_type(input).nullable());
    assert!(matches!(evaluated.outcome, FoldNodeOutcome::Kept));
}

#[test]
fn row_data_error_keeps_the_compiled_candidate() {
    let mut arena = ScalarArena::new();
    let left = integer(&mut arena, 1, DataType::Int64);
    let zero = integer(&mut arena, 0, DataType::Int64);
    let input = binary(
        &mut arena,
        BinOp::Div,
        left,
        zero,
        FunctionValueType::new(DataType::Int64, true),
    );
    let evaluated = fold_node(input, &arena, &context()).unwrap();
    assert!(evaluated.unit.is_some());
    assert!(matches!(evaluated.outcome, FoldNodeOutcome::Kept));
}

#[derive(Debug)]
struct OuterFoldFailure(FailureClass);

impl std::fmt::Display for OuterFoldFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("division by zero")
    }
}

impl std::error::Error for OuterFoldFailure {}

impl FailureProjection for OuterFoldFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            self.0,
            novarocks_failure_contract::FailureReason::new(
                novarocks_failure_contract::domain::SQL,
                std::num::NonZeroU16::new(65000).unwrap(),
            ),
        )
    }
}

impl novarocks_failure_contract::OuterFailureProjection for OuterFoldFailure {}

#[test]
fn outer_failures_keep_the_exact_kernel_cause_even_when_classified_as_data() {
    // The same diagnostic can also appear in the row journal. Only that
    // channel permits the candidate to stay unfolded and compilation to continue.
    for class in [
        FailureClass::Data,
        FailureClass::Control,
        FailureClass::Resource,
        FailureClass::Invariant,
        FailureClass::Dependency,
        FailureClass::Representation,
        FailureClass::Unexpected,
    ] {
        let cause = KernelFailure::from_domain(OuterFoldFailure(class));
        let actual = evaluation_error(ExprEvaluationFailure::Kernel(cause.clone()));
        let SqlCompileError::CompilationSource(source) = actual else {
            panic!("outer evaluation failure lost its original cause: {actual:?}");
        };
        assert_eq!(source.downcast_ref::<KernelFailure>(), Some(&cause));
        assert_eq!(source.failure_fact(), cause.failure_fact());
        assert_eq!(source.failure_fact().class(), class);
    }
}

#[test]
fn malformed_literal_is_a_fatal_lowering_error() {
    let mut arena = ScalarArena::new();
    let wrong = literal(
        &mut arena,
        LiteralValue::Int(1),
        FunctionValueType::new(DataType::Boolean, false),
    );
    let other = integer(&mut arena, 2, DataType::Int64);
    let input = binary(
        &mut arena,
        BinOp::Add,
        wrong,
        other,
        FunctionValueType::new(DataType::Int64, false),
    );
    assert!(matches!(
        fold_node(input, &arena, &context()),
        Err(SqlCompileError::Compilation(_))
    ));
}

#[test]
fn only_typed_lowering_capabilities_and_structure_limits_are_optional() {
    // A prepared primitive capability refusal and unit construction limits
    // reach these branches before the compiler can create a program.
    let refused = ContractLoweringError::Semantics(
        crate::planner::distributed::build::PackageSemanticsError::UnsupportedCapability(
            "no prepared primitive".to_owned(),
        ),
    );
    assert!(lower_for_fold(Err(refused)).unwrap().is_none());
    for limit in [
        ExpressionControlFlowError::TooDeep,
        ExpressionControlFlowError::TooManyItems,
    ] {
        assert!(
            lower_for_fold(Err(ContractLoweringError::Unit(UnitError::Flow(limit))))
                .unwrap()
                .is_none()
        );
    }
    assert!(matches!(
        lower_for_fold(Err(ContractLoweringError::Unit(UnitError::Flow(
            ExpressionControlFlowError::InvalidControlShape,
        )))),
        Err(SqlCompileError::CompilationSource(_))
    ));
}

#[test]
fn capability_decline_preserves_noncapability_compile_errors() {
    assert!(
        compiled_for_fold(Err(ExprCompileError::UnsupportedExpression(ExprId::new(0))))
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        compiled_for_fold(Err(ExprCompileError::Control(
            CompileControlError::Cancelled
        ))),
        Err(SqlCompileError::Cancelled)
    ));
    assert!(matches!(
        compiled_for_fold(Err(ExprCompileError::Invalid("unit invariant"))),
        Err(SqlCompileError::CompilationSource(_))
    ));
}

#[derive(Default)]
struct RecordingControl {
    observations: Mutex<Vec<(CompilePhase, u32)>>,
    failure: Option<CompileControlError>,
}

impl PureCompileControl for RecordingControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        self.observations.lock().unwrap().push((phase, units));
        match &self.failure {
            Some(cause) => Err(cause.clone()),
            None => Ok(()),
        }
    }
}

#[test]
fn admitted_leaf_phase_checks_once_then_runs_its_body_once() {
    let control = RecordingControl::default();
    let mut host = FoldCallHost { control: &control };
    host.begin_call();
    let mut calls = 0;
    host.run_admitted(
        ScalarCallPhase::Body,
        ScalarCallRequestFacts::new(4096, 4),
        &mut || {
            assert_eq!(
                *control.observations.lock().unwrap(),
                [(CompilePhase::FunctionSpecialization, 1)]
            );
            calls += 1;
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(calls, 1);
    assert_eq!(
        *control.observations.lock().unwrap(),
        [(CompilePhase::FunctionSpecialization, 1)]
    );
}

#[test]
fn control_refusal_in_admission_aborts_without_running_the_body() {
    for (failure, expected) in [
        (CompileControlError::Cancelled, SqlCompileError::Cancelled),
        (
            CompileControlError::DeadlineExceeded,
            SqlCompileError::DeadlineExceeded,
        ),
        (
            CompileControlError::ResourceExhausted,
            SqlCompileError::ResourceExhausted,
        ),
    ] {
        let control = RecordingControl {
            failure: Some(failure),
            ..RecordingControl::default()
        };
        let mut host = FoldCallHost { control: &control };
        let cause = host
            .run_admitted(
                ScalarCallPhase::Body,
                ScalarCallRequestFacts::default(),
                &mut || panic!("a refused leaf phase cannot run"),
            )
            .unwrap_err();
        assert_eq!(
            *control.observations.lock().unwrap(),
            [(CompilePhase::FunctionSpecialization, 1)]
        );
        assert_eq!(evaluation_error(cause.into()), expected);
    }
}

#[test]
fn callback_request_is_a_fatal_invariant() {
    let control = unbounded_rewrite_test_control();
    let mut host = FoldCallHost { control };
    let cause = host.callback_request_facts(None).unwrap_err();
    let error = evaluation_error(cause.into());
    assert_eq!(
        error.source_failure().unwrap().failure_fact().class(),
        FailureClass::Invariant
    );
}
