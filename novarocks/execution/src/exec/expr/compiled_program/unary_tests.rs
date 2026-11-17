// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::exec::expr::compiled_program::checked_constants;
use arrow::array::ArrayRef;
use novarocks_functions::EvaluatedArgument;
use novarocks_local_program::{ProgramUseRef, StaticExprKind};
use novarocks_physical_plan::UnaryOperator;

#[derive(Clone, Copy)]
enum UnaryCase {
    IsNull,
    IsNotNull,
    PureAnd,
    PureOr,
    StateBoundary,
    NotFilter,
}

fn unary(builder: &mut FragmentBuilder, child: ExprId, negated: bool) -> ExprId {
    builder
        .add_expression(
            NodeId::new(0),
            FunctionValueType::new(DataType::Boolean, false),
            ExprKind::IsNull {
                expr: child,
                negated,
            },
        )
        .unwrap()
}

fn unary_program(case: UnaryCase) -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = catalogue(Shape::Decimal);
    let fragment_id = FragmentId::new(197);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let flag = ValueId::new(901);
    let decimal_value = ValueId::new(71);
    let bool_type = FunctionValueType::new(DataType::Boolean, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let integer = FunctionValueType::new(DataType::Int64, false);
    let columns = [(flag, bool_type.clone()), (decimal_value, decimal.clone())];
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = vec![];
    for (id, ty) in &columns {
        let expr = builder
            .add_expression(
                input,
                ty.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(ty.clone()), options().constants),
                ),
            )
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr { node: input, expr },
            })
            .unwrap();
        items.push((expr, *id));
    }
    builder
        .add_project(
            input,
            source,
            items.into_boxed_slice(),
            Box::from([flag, decimal_value]),
        )
        .unwrap();
    let flag_expr = if matches!(case, UnaryCase::IsNull | UnaryCase::IsNotNull) {
        None
    } else {
        Some(
            builder
                .add_expression(output, bool_type.clone(), ExprKind::Value(flag))
                .unwrap(),
        )
    };
    let mut authors = BTreeMap::new();
    let expression = if matches!(case, UnaryCase::NotFilter) {
        let actual_true = builder
            .add_expression(
                output,
                (FunctionValueType::new(DataType::Boolean, false)).clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::boolean(
                        &(FunctionValueType::new(DataType::Boolean, false)),
                        true,
                        options().constants,
                    ),
                ),
            )
            .unwrap();
        let conjunction = builder
            .add_expression(
                output,
                bool_type.clone(),
                ExprKind::Conjunction {
                    args: Box::from([flag_expr.unwrap(), actual_true]),
                },
            )
            .unwrap();
        builder
            .add_expression(
                output,
                bool_type.clone(),
                ExprKind::Unary {
                    op: UnaryOperator::Not,
                    expr: conjunction,
                },
            )
            .unwrap()
    } else {
        let value = builder
            .add_expression(output, decimal.clone(), ExprKind::Value(decimal_value))
            .unwrap();
        let digits = builder
            .add_expression(
                output,
                integer.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::i64(&(integer.clone()), -1, options().constants),
                ),
            )
            .unwrap();
        let round = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "round",
                vec![
                    argument(decimal, None),
                    integer_argument(integer.clone(), -1),
                ],
                ControlShape::Eager,
            ),
            vec![value, digits],
        );
        let test = unary(&mut builder, round, matches!(case, UnaryCase::IsNotNull));
        match case {
            UnaryCase::IsNull | UnaryCase::IsNotNull => test,
            UnaryCase::PureAnd => builder
                .add_expression(
                    output,
                    bool_type.clone(),
                    ExprKind::Conjunction {
                        args: Box::from([test, flag_expr.unwrap()]),
                    },
                )
                .unwrap(),
            UnaryCase::PureOr => builder
                .add_expression(
                    output,
                    bool_type.clone(),
                    ExprKind::Disjunction {
                        args: Box::from([test, flag_expr.unwrap()]),
                    },
                )
                .unwrap(),
            UnaryCase::StateBoundary => {
                let seed = builder
                    .add_expression(
                        output,
                        integer.clone(),
                        checked_constants::kind(
                            &mut source_pools,
                            checked_constants::i64(&(integer.clone()), 42, options().constants),
                        ),
                    )
                    .unwrap();
                let rand = call(
                    &mut builder,
                    &mut authors,
                    author(
                        &functions,
                        "rand",
                        vec![integer_argument(integer, 42)],
                        ControlShape::Eager,
                    ),
                    vec![seed],
                );
                let rand_null = unary(&mut builder, rand, false);
                builder
                    .add_expression(
                        output,
                        bool_type.clone(),
                        ExprKind::Disjunction {
                            args: Box::from([test, rand_null, flag_expr.unwrap()]),
                        },
                    )
                    .unwrap()
            }
            UnaryCase::NotFilter => unreachable!(),
        }
    };
    let mut result_fields = vec![];
    if matches!(case, UnaryCase::NotFilter) {
        builder
            .add_filter(output, input, Box::from([expression]))
            .unwrap();
        for (ordinal, (value, ty)) in columns.iter().enumerate() {
            result_fields.push(ResultField {
                domain: crate::test_result_domain::result_value_domain(ty),
                name: format!("input_{ordinal}").into(),
                alias: None,
                value: *value,
                ty: ty.clone(),
            });
        }
    } else {
        let result_type = if matches!(case, UnaryCase::IsNull | UnaryCase::IsNotNull) {
            FunctionValueType::new(DataType::Boolean, false)
        } else {
            bool_type.clone()
        };
        let result_value = builder
            .add_value(
                result_type.clone(),
                ValueOrigin::Expr {
                    node: output,
                    expr: expression,
                },
            )
            .unwrap();
        builder
            .add_project(
                output,
                input,
                Box::from([(expression, result_value)]),
                Box::from([result_value]),
            )
            .unwrap();
        result_fields.push(ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "unary_result".into(),
            alias: None,
            value: result_value,
            ty: result_type,
        });
    }
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&output].output.clone(),
        fields: result_fields.into_boxed_slice(),
    };
    compile_checked_fragment(&functions, fragment, &authors, result, source_pools)
}
fn unary_root(case: UnaryCase) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(2),
        role: if matches!(case, UnaryCase::NotFilter) {
            ProgramNodeExpressionRole::FilterPredicate { predicate: 0 }
        } else {
            ProgramNodeExpressionRole::ProjectOutput { expression: 0 }
        },
    }
}
fn evaluator(program: &Arc<LocalProgram>, case: UnaryCase) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(program.clone(), unary_root(case), &Control).unwrap()
}
fn unary_batch(
    program: &LocalProgram,
    flags: Vec<Option<bool>>,
    decimals: Vec<Option<i128>>,
) -> RecordBatch {
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(BooleanArray::from(flags)),
            Arc::new(
                Decimal128Array::from(decimals)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}
fn booleans(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn next_actual_rand(evaluator: &mut CompiledExpressionInstance) -> u64 {
    let rand = evaluator
        .instances
        .values_mut()
        .find(|instance| instance.contract().function_id().as_str() == "builtin.scalar/rand/v1")
        .unwrap();
    let seed = Arc::new(Int64Array::from(vec![42])) as ArrayRef;
    let arguments = [EvaluatedArgument::Scalar(&seed)];
    let output = rand
        .evaluate(
            Selection::all(1),
            &arguments,
            novarocks_functions::ScalarInvocationActivation::Activated,
            &Control,
        )
        .unwrap();
    output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
        .to_bits()
}
fn rand_instances(evaluator: &CompiledExpressionInstance) -> usize {
    evaluator
        .instances
        .values()
        .filter(|instance| instance.contract().function_id().as_str() == "builtin.scalar/rand/v1")
        .count()
}
fn max38() -> i128 {
    10_i128.pow(38) - 1
}

#[test]
fn actual_null_tests_distinguish_success_null_from_round_error_placeholder_on_sparse_rows() {
    for (case, expected) in [
        (UnaryCase::IsNull, vec![Some(false), Some(true), None]),
        (UnaryCase::IsNotNull, vec![Some(true), Some(false), None]),
    ] {
        let program = unary_program(case);
        let input = unary_batch(
            &program,
            vec![Some(true); 8],
            vec![
                Some(max38()),
                Some(25),
                Some(max38()),
                Some(max38()),
                None,
                Some(max38()),
                Some(max38()),
                Some(max38()),
            ],
        );
        let rows = [1, 4, 7];
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let mut instance = evaluator(&program, case);
        let output = instance.evaluate(&input, selection, &Control).unwrap();
        assert_eq!(booleans(&output), expected);
        assert_eq!(output.selection(), selection);
        assert_eq!(output.errors().len(), 1);
        assert_eq!(output.errors()[0].selected_ordinal(), 2);
        assert_eq!(output.selection().row(2), Some(7));
        assert!(output.errors()[0].message().contains("overflow"));
    }
}

#[test]
fn actual_pure_round_boolean_error_is_suppressed_only_by_later_required_decider() {
    for (case, flags, expected) in [
        (
            UnaryCase::PureAnd,
            vec![Some(false), Some(true), Some(true)],
            vec![Some(false), None, Some(true)],
        ),
        (
            UnaryCase::PureOr,
            vec![Some(true), Some(false), Some(false)],
            vec![Some(true), None, Some(true)],
        ),
    ] {
        let program = unary_program(case);
        let mut values = vec![Some(25); 10];
        values[2] = Some(max38());
        values[7] = Some(max38());
        values[9] = None;
        let mut flag_column = vec![None; 10];
        for (row, flag) in [2, 7, 9].into_iter().zip(flags) {
            flag_column[row] = flag;
        }
        let input = unary_batch(&program, flag_column, values);
        let rows = [2, 7, 9];
        let selection = Selection::try_sparse(10, &rows).unwrap();
        let mut instance = evaluator(&program, case);
        let output = instance.evaluate(&input, selection, &Control).unwrap();
        assert_eq!(booleans(&output), expected);
        assert_eq!(output.errors().len(), 1);
        assert_eq!(output.errors()[0].selected_ordinal(), 1);
        assert_eq!(output.selection().row(1), Some(7));
        assert!(output.errors()[0].message().contains("overflow"));
    }
}

#[test]
fn actual_rand_null_test_state_boundary_promotes_pending_and_skips_failed_parent_rows() {
    let program = unary_program(UnaryCase::StateBoundary);
    let mut evaluator = evaluator(&program, UnaryCase::StateBoundary);
    let input = unary_batch(
        &program,
        vec![Some(true); 9],
        vec![
            Some(25),
            Some(max38()),
            Some(25),
            Some(max38()),
            None,
            Some(max38()),
            Some(35),
            Some(max38()),
            Some(max38()),
        ],
    );
    let rows = [2, 4, 6, 8];
    let selection = Selection::try_sparse(9, &rows).unwrap();
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    assert_eq!(
        booleans(&output),
        vec![Some(true), Some(true), Some(true), None]
    );
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 3);
    assert_eq!(output.selection().row(3), Some(8));
    assert_eq!(rand_instances(&evaluator), 1);
    // Only original rows 2 and 6 reached the same real constant-seed instance.
    // Original row 4 decided TRUE before RAND; row 8 failed at its boundary.
    assert_eq!(next_actual_rand(&mut evaluator), SEED_42[2]);
    let resolved = program.checked().channels().expressions().resolved_calls();
    let snapshot = resolved.snapshot();
    let flow = &snapshot.flows()[&unary_root(UnaryCase::StateBoundary).arena()];
    let root_use = snapshot.bindings()[&unary_root(UnaryCase::StateBoundary)];
    let rand_null = flow.uses()[&root_use].arguments[1];
    let summary = evaluator.effects[&ProgramUseRef {
        arena: unary_root(UnaryCase::StateBoundary).arena(),
        use_id: rand_null,
    }];
    assert!(
        summary
            .for_use(flow.uses()[&rand_null].context)
            .unwrap()
            .has_instance_state
    );
    assert!(
        !summary
            .for_use(flow.uses()[&rand_null].context)
            .unwrap()
            .permits_boolean_reordering()
    );
}

#[test]
fn all_pending_round_errors_stop_before_rand_instantiation_and_do_not_advance_existing_state() {
    let program = unary_program(UnaryCase::StateBoundary);
    let mut evaluator = evaluator(&program, UnaryCase::StateBoundary);
    let failing = unary_batch(&program, vec![Some(true); 2], vec![Some(max38()); 2]);
    let output = evaluator
        .evaluate(&failing, Selection::all(2), &Control)
        .unwrap();
    assert_eq!(booleans(&output), vec![None, None]);
    assert_eq!(output.errors().len(), 2);
    assert_eq!(rand_instances(&evaluator), 0);
    let clean = unary_batch(&program, vec![Some(true)], vec![Some(25)]);
    evaluator
        .evaluate(&clean, Selection::all(1), &Control)
        .unwrap();
    assert_eq!(rand_instances(&evaluator), 1);
    let output = evaluator
        .evaluate(&failing, Selection::all(2), &Control)
        .unwrap();
    assert_eq!(output.errors().len(), 2);
    // One successful parent advanced the instance; both failing batches did not.
    assert_eq!(next_actual_rand(&mut evaluator), SEED_42[1]);
}

#[test]
fn actual_not_filter_requires_value_child_and_keeps_nullable_truth_semantics() {
    let program = unary_program(UnaryCase::NotFilter);
    let root = unary_root(UnaryCase::NotFilter);
    let resolved = program.checked().channels().expressions().resolved_calls();
    let snapshot = resolved.snapshot();
    let flow = &snapshot.flows()[&root.arena()];
    let parent = &flow.uses()[&snapshot.bindings()[&root]];
    assert_eq!(parent.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(parent.control, ControlShape::Eager);
    assert_eq!(
        flow.uses()[&parent.arguments[0]].context.demand,
        EvaluationDemand::Value
    );
    let definitions = &snapshot.roots().arenas()[&root.arena()];
    assert!(matches!(
        definitions.node(parent.definition).unwrap().kind(),
        StaticExprKind::Not(_)
    ));
    let child = &flow.uses()[&parent.arguments[0]];
    assert_eq!(child.control, ControlShape::Conjunction);
    assert!(matches!(
        definitions.node(child.definition).unwrap().kind(),
        StaticExprKind::NaryAnd { args } if args.len() == 2
    ));
    for argument in &child.arguments {
        assert_eq!(
            flow.uses()[argument].context.demand,
            EvaluationDemand::Value
        );
    }
    // If NOT propagated its TruthOnly demand into AND, the NULL child row
    // would become FALSE and NOT would incorrectly keep it as TRUE. The
    // actual TRUE literal is compiled to the original ConstantValue owner.
    let input = unary_batch(
        &program,
        vec![
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
        ],
        vec![None; 6],
    );
    let rows = [1, 3, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let output = evaluator(&program, UnaryCase::NotFilter)
        .evaluate(&input, selection, &Control)
        .unwrap();
    assert_eq!(booleans(&output), vec![Some(false), None, Some(true)]);
    assert!(output.errors().is_empty());
}

#[test]
fn every_actual_round_null_rand_boundary_callback_keeps_all_seven_causes_and_no_replay() {
    let program = unary_program(UnaryCase::StateBoundary);
    let input = unary_batch(
        &program,
        vec![Some(true); 320],
        (0..320)
            .map(|row| match row % 3 {
                0 => Some(max38()),
                1 => None,
                _ => Some(25),
            })
            .collect(),
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = evaluator(&program, UnaryCase::StateBoundary)
        .evaluate(&input, Selection::all(320), &recorder)
        .unwrap();
    assert_eq!(output.errors().len(), 107);
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let mut instance = evaluator(&program, UnaryCase::StateBoundary);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(instance.evaluate(&input,Selection::all(320),&control),Err(actual) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                instance.evaluate(&input, Selection::all(320), &after),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}
