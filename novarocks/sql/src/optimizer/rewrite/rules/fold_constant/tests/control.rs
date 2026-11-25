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
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};

use novarocks_functions::ConstantValue;
use novarocks_type_contract::{CompileControlError, PureCompileControl};

use super::{fixtures::*, *};
use crate::common::LiteralValue;
use crate::compiler::SqlFoldDependencyObserver;
use crate::optimizer::scalar::HashableLiteral;

#[derive(Default)]
struct TraceControl {
    steps: Mutex<Vec<(CompilePhase, u32)>>,
    reject: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for TraceControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut steps = self.steps.lock().unwrap();
        steps.push((phase, units));
        if let Some((ordinal, error)) = &self.reject
            && steps.len() == *ordinal
        {
            return Err(error.clone());
        }
        Ok(())
    }
}

fn apply_with_control(
    fixture: &Fixture,
    plan: OptExpr,
    control: &dyn PureCompileControl,
) -> Result<RewriteResult, SqlCompileError> {
    let mut ctx = RewriteContext::for_query_with_settings(
        Default::default(),
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        control,
    );
    install_inputs(&mut ctx, Rc::clone(&fixture.arena));
    FoldConstant.apply(plan, &mut ctx)
}

#[test]
fn fold_control_observes_real_nodes_cached_edges_and_final_publication() {
    let fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    let product = fixture.binary(BinOp::Mul, sum, sum);
    let trace = TraceControl::default();
    assert_int_literal(
        &fixture,
        project_expr(&changed(
            apply_with_control(&fixture, project(product), &trace).unwrap(),
        )),
        4,
    );
    let steps = trace.steps.into_inner().unwrap();
    for phase in [
        CompilePhase::Validate,
        CompilePhase::LowerProgram,
        CompilePhase::FunctionSpecialization,
    ] {
        let ordinal = steps
            .iter()
            .position(|&(observed, _)| observed == phase)
            .unwrap()
            + 1;
        let trace = TraceControl {
            reject: Some((ordinal, CompileControlError::Cancelled)),
            ..Default::default()
        };
        assert!(matches!(
            apply_with_control(&fixture, project(product), &trace),
            Err(SqlCompileError::Cancelled)
        ));
    }
    let trace = TraceControl {
        reject: Some((steps.len(), CompileControlError::DeadlineExceeded)),
        ..Default::default()
    };
    assert!(matches!(
        apply_with_control(&fixture, project(product), &trace),
        Err(SqlCompileError::DeadlineExceeded)
    ));
}

#[test]
fn fold_control_covers_no_evaluator_and_declined_or_failed_folds() {
    let fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let zero = fixture.int_literal(0);
    let miss = fixture.binary(BinOp::Div, one, zero);
    let trace = TraceControl::default();
    assert!(matches!(
        apply_with_control(&fixture, project(miss), &trace).unwrap(),
        RewriteResult::Unchanged
    ));
    let steps = trace.steps.into_inner().unwrap();
    let trace = TraceControl {
        reject: Some((steps.len(), CompileControlError::Cancelled)),
        ..Default::default()
    };
    assert!(matches!(
        apply_with_control(&fixture, project(miss), &trace),
        Err(SqlCompileError::Cancelled)
    ));
}

#[test]
fn folded_output_and_arena_do_not_retain_request_control() {
    struct LifetimeControl(Arc<()>);
    impl PureCompileControl for LifetimeControl {
        fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
            assert_eq!(Arc::strong_count(&self.0), 1);
            Ok(())
        }
    }

    let fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    let token = Arc::new(());
    let weak: Weak<()> = Arc::downgrade(&token);
    let control = LifetimeControl(token);
    let result = changed(apply_with_control(&fixture, project(sum), &control).unwrap());
    drop(control);
    assert!(weak.upgrade().is_none());
    assert_int_literal(&fixture, project_expr(&result), 2);
}

#[test]
fn evaluator_receives_original_phase_control_and_control_errors_never_decline() {
    struct RejectReceipt;
    impl SqlFoldDependencyObserver for RejectReceipt {
        fn observe_fold_unit_observed(
            &self,
            _source: &SqlAuthoredScalarUnit,
            result: Option<&ConstantValue>,
            control: &dyn PureCompileControl,
        ) -> Result<(), CompileControlError> {
            assert!(result.is_some());
            control.checkpoint(CompilePhase::FunctionSpecialization, u32::MAX)
        }
    }
    struct RejectReceiptWork;
    impl PureCompileControl for RejectReceiptWork {
        fn checkpoint(&self, _phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            if units == u32::MAX {
                return Err(CompileControlError::ResourceExhausted);
            }
            Ok(())
        }
    }
    let fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    let mut ctx = RewriteContext::for_query_with_settings(
        Default::default(),
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        &RejectReceiptWork,
    );
    install_inputs(&mut ctx, Rc::clone(&fixture.arena));
    ctx.set_fold_dependency_observer(Some(Arc::new(RejectReceipt)));
    for _attempt in 0..2 {
        assert!(matches!(
            FoldConstant.apply(project(sum), &mut ctx),
            Err(SqlCompileError::ResourceExhausted)
        ));
    }
}

fn check_attempt_prefixes(
    build: impl Fn() -> (Fixture, ScalarId),
    expected: impl FnOnce(&Fixture, Result<RewriteResult, SqlCompileError>),
    sample_wide: bool,
) -> Vec<(CompilePhase, u32)> {
    let (fixture, input) = build();
    let observe = TraceControl::default();
    expected(
        &fixture,
        apply_with_control(&fixture, project(input), &observe),
    );
    let trace = observe.steps.into_inner().unwrap();
    assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));

    let checkpoints = if sample_wide {
        // A wide compiled unit observes additional layout work per edge.
        // Sample the entry, work quantum, specialization tail and completion.
        let max = novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK;
        let quantum = trace.iter().position(|&(_, units)| units == max).unwrap();
        let tail = trace
            .iter()
            .rposition(|&(phase, units)| {
                phase == CompilePhase::FunctionSpecialization && (1..max).contains(&units)
            })
            .unwrap();
        vec![0, quantum, tail, trace.len() - 1]
    } else {
        (0..trace.len()).collect()
    };

    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
            novarocks_functions::KernelFailure::ResourceExhausted,
        )),
    ] {
        for index in &checkpoints {
            let ordinal = index + 1;
            let (fixture, input) = build();
            let refuse = TraceControl {
                reject: Some((ordinal, cause.clone())),
                ..Default::default()
            };
            let result = apply_with_control(&fixture, project(input), &refuse);
            assert!(
                matches!(result, Err(error) if error == SqlCompileError::from(cause.clone())),
                "lost {cause:?} at {ordinal}: {:?}",
                trace[ordinal - 1],
            );
            assert_eq!(refuse.steps.into_inner().unwrap(), trace[..ordinal]);
        }
    }
    trace
}

#[test]
fn real_wide_prepared_call_keeps_entry_quantum_tail_and_all_three_original_causes() {
    use novarocks_functions::{FunctionArgument, FunctionResultType};
    use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};

    let ty = FunctionValueType::new(DataType::Int64, false);
    let count = 320;
    let source = crate::functions::builtin_sql_function_catalog()
        .resolve_scalar_binding(
            "coalesce",
            &vec![
                FunctionArgument::Value {
                    value_type: ty,
                    constant: None
                };
                count
            ],
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
    let FunctionResultType::Scalar(result) = &source.selected.result_type else {
        panic!("the actual COALESCE owner returns a scalar");
    };
    let result = result.clone();
    let binding =
        crate::binding::SqlFunctionBinding::new(source, DecimalOverflowPolicy::OutputNull);
    let trace = check_attempt_prefixes(
        || {
            let fixture = Fixture::new();
            let seven = fixture.int_literal(7);
            let root = fixture.intern(
                ScalarNode::FunctionCall {
                    binding: binding.clone(),
                    name: "coalesce".into(),
                    args: vec![seven; count],
                    distinct: false,
                    volatility: FunctionVolatility::Immutable,
                },
                result.clone(),
            );
            (fixture, root)
        },
        |fixture, result| {
            assert_int_literal(fixture, project_expr(&changed(result.unwrap())), 7);
        },
        true,
    );
    assert!(trace.iter().any(|&(_, work)| work == 256));
    assert!(
        trace.iter().any(
            |&(phase, work)| phase == CompilePhase::FunctionSpecialization
                && (1..256).contains(&work)
        ),
        "the actual call still observes its function-specialization tail"
    );
}

#[test]
fn actual_metadata_decline_and_successful_output_pool_keep_all_callback_prefixes() {
    use arrow::datatypes::Field;
    use novarocks_type_contract::{
        FunctionValueType, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    let storage = ValueTypeStorageLifetime::pure();
    let json = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        storage.clone(),
    )
    .unwrap();
    let list = ValueTypeRef::try_new(ValueKind::List(json), true, storage.clone()).unwrap();
    let field = Field::new("item", DataType::LargeBinary, true).with_metadata([
        (novarocks_type_contract::NR_LOGICAL_TYPE_KEY, "json"),
        ("provider.fact", "positive source metadata"),
    ]);
    let layout = ValueLayout::try_new(&DataType::List(Arc::new(field)), storage).unwrap();
    let declaration = FunctionValueType::try_from_value(list, layout).unwrap();
    check_attempt_prefixes(
        || {
            let fixture = Fixture::new();
            let null = fixture.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Null)),
                declaration.clone(),
            );
            let root = fixture.binary_typed(BinOp::EqForNull, null, null, DataType::Boolean, false);
            (fixture, root)
        },
        |_, result| assert!(matches!(result, Ok(RewriteResult::Unchanged))),
        false,
    );
    check_attempt_prefixes(
        || {
            let fixture = Fixture::new();
            let one = fixture.int_literal(1);
            let root = fixture.binary(BinOp::Add, one, one);
            (fixture, root)
        },
        |fixture, result| assert_int_literal(fixture, project_expr(&changed(result.unwrap())), 2),
        false,
    );
}

#[test]
fn ordinary_row_error_and_invalid_arithmetic_observe_exact_original_completion() {
    use novarocks_type_contract::FunctionValueType;
    check_attempt_prefixes(
        || {
            let fixture = Fixture::new();
            let one = fixture.int_literal(1);
            let zero = fixture.int_literal(0);
            let root = fixture.binary(BinOp::Div, one, zero);
            (fixture, root)
        },
        |_, result| assert!(matches!(result, Ok(RewriteResult::Unchanged))),
        false,
    );
    check_attempt_prefixes(
        || {
            let fixture = Fixture::new();
            let malformed = fixture.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
                FunctionValueType::new(DataType::Boolean, false),
            );
            let one = fixture.int_literal(1);
            let root = fixture.binary(BinOp::Add, malformed, one);
            (fixture, root)
        },
        |_, result| {
            assert!(
                matches!(result, Err(SqlCompileError::Compilation(_))),
                "{result:?}"
            )
        },
        false,
    );
}
