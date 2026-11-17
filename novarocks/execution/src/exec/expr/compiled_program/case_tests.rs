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
use novarocks_local_program::StaticExprKind;

#[derive(Clone, Copy)]
enum CaseKind {
    Priority,
    NoElse,
    Random,
    DeferredRandom,
    ErrorThen,
}
fn literal_i64(builder: &mut FragmentBuilder, value: i64) -> ExprId {
    builder
        .add_expression(
            NodeId::new(0),
            FunctionValueType::new(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(value)),
        )
        .unwrap()
}
fn constant_rand(
    builder: &mut FragmentBuilder,
    authors: &mut BTreeMap<ExprId, Author>,
    functions: &PureEngineFunctionCatalog,
) -> ExprId {
    let ty = FunctionValueType::new(DataType::Int64, false);
    let seed = literal_i64(builder, 42);
    call(
        builder,
        authors,
        author(
            functions,
            "rand",
            vec![integer_argument(ty, 42)],
            ControlShape::Eager,
        ),
        vec![seed],
    )
}
fn rounded(
    builder: &mut FragmentBuilder,
    authors: &mut BTreeMap<ExprId, Author>,
    functions: &PureEngineFunctionCatalog,
    value: ExprId,
) -> ExprId {
    let digits = literal_i64(builder, -1);
    call(
        builder,
        authors,
        author(
            functions,
            "round",
            vec![
                argument(
                    FunctionValueType::new(DataType::Decimal128(38, 0), true),
                    None,
                ),
                integer_argument(FunctionValueType::new(DataType::Int64, false), -1),
            ],
            ControlShape::Eager,
        ),
        vec![value, digits],
    )
}
fn case_program(kind: CaseKind) -> Arc<LocalProgram> {
    let functions = catalogue(Shape::Decimal);
    let fragment_id = FragmentId::new(199);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let first = ValueId::new(901);
    let second = ValueId::new(3);
    let decimal_value = ValueId::new(71);
    let fallback = ValueId::new(72);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let columns = [
        (first, boolean.clone()),
        (second, boolean.clone()),
        (decimal_value, decimal.clone()),
        (fallback, decimal.clone()),
    ];
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = vec![];
    for (id, ty) in &columns {
        let expr = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
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
            columns
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
        .unwrap();
    let when_first = builder
        .add_expression(output, boolean.clone(), ExprKind::Value(first))
        .unwrap();
    let mut authors = BTreeMap::new();
    let (when_second, then_first, then_second, otherwise, result_type) = match kind {
        CaseKind::Priority | CaseKind::NoElse => {
            let when_second = builder
                .add_expression(output, boolean, ExprKind::Value(second))
                .unwrap();
            let first = literal_i64(&mut builder, 11);
            let second = literal_i64(&mut builder, 22);
            let otherwise = if matches!(kind, CaseKind::Priority) {
                Some(literal_i64(&mut builder, 33))
            } else {
                None
            };
            (
                when_second,
                first,
                second,
                otherwise,
                FunctionValueType::new(DataType::Int64, matches!(kind, CaseKind::NoElse)),
            )
        }
        CaseKind::Random | CaseKind::DeferredRandom => {
            let when_second = if matches!(kind, CaseKind::DeferredRandom) {
                let value = builder
                    .add_expression(output, decimal.clone(), ExprKind::Value(decimal_value))
                    .unwrap();
                let round = rounded(&mut builder, &mut authors, &functions, value);
                builder
                    .add_expression(
                        output,
                        FunctionValueType::new(DataType::Boolean, false),
                        ExprKind::IsNull {
                            expr: round,
                            negated: false,
                        },
                    )
                    .unwrap()
            } else {
                builder
                    .add_expression(output, boolean, ExprKind::Value(second))
                    .unwrap()
            };
            let first = constant_rand(&mut builder, &mut authors, &functions);
            let second = constant_rand(&mut builder, &mut authors, &functions);
            let otherwise = constant_rand(&mut builder, &mut authors, &functions);
            (
                when_second,
                first,
                second,
                Some(otherwise),
                FunctionValueType::new(DataType::Float64, true),
            )
        }
        CaseKind::ErrorThen => {
            let when_second = builder
                .add_expression(output, boolean, ExprKind::Value(second))
                .unwrap();
            let value = builder
                .add_expression(output, decimal.clone(), ExprKind::Value(decimal_value))
                .unwrap();
            let first = rounded(&mut builder, &mut authors, &functions, value);
            let fallback = builder
                .add_expression(output, decimal.clone(), ExprKind::Value(fallback))
                .unwrap();
            // Repeated definition is allowed, but the THEN and ELSE occurrences
            // must have distinct required-domain uses and independent mappings.
            (when_second, first, fallback, Some(fallback), decimal)
        }
    };
    let case = builder
        .add_expression(
            output,
            result_type.clone(),
            ExprKind::Case {
                operand: None,
                when_then: Box::from([(when_first, then_first), (when_second, then_second)]),
                else_expr: otherwise,
            },
        )
        .unwrap();
    let result_value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: output,
                expr: case,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(case, result_value)]),
            Box::from([result_value]),
        )
        .unwrap();
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
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "searched_case_result".into(),
            alias: None,
            value: result_value,
            ty: result_type,
        }]),
    };
    compile_checked_fragment(&functions, fragment, &authors, result)
}
fn case_batch(
    program: &LocalProgram,
    first: Vec<Option<bool>>,
    second: Vec<Option<bool>>,
    values: Vec<Option<i128>>,
) -> RecordBatch {
    let rows = first.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(BooleanArray::from(first)),
            Arc::new(BooleanArray::from(second)),
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(71); rows])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}
fn rand_bits(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<u64>> {
    output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .map(|value| value.map(f64::to_bits))
        .collect()
}
fn assert_case_domains(program: &LocalProgram, has_else: bool) {
    let resolved = program.checked().channels().expressions().resolved_calls();
    let snapshot = resolved.snapshot();
    let flow = &snapshot.flows()[&root().arena()];
    let invocation = &flow.uses()[&snapshot.bindings()[&root()]];
    assert_eq!(
        invocation.control,
        ControlShape::Case {
            simple: false,
            arms: 2,
            has_else
        }
    );
    let definitions = &snapshot.roots().arenas()[&root().arena()];
    assert!(
        matches!(definitions.node(invocation.definition).unwrap().kind(),StaticExprKind::Case {has_case_expr:false,has_else_expr,children} if *has_else_expr==has_else && children.len()==4+usize::from(has_else))
    );
    for (ordinal, child) in invocation.arguments.iter().enumerate() {
        let actual = &flow.uses()[child];
        assert_eq!(
            actual.context.demand,
            if ordinal < 4 && ordinal % 2 == 0 {
                EvaluationDemand::TruthOnly
            } else {
                EvaluationDemand::Value
            }
        );
        assert_ne!(actual.context.region, invocation.context.region);
        let expected = if ordinal == 4 {
            novarocks_type_contract::GuardKind::CaseElse
        } else if ordinal % 2 == 0 {
            novarocks_type_contract::GuardKind::CaseWhen {
                arm: (ordinal / 2) as u32,
            }
        } else {
            novarocks_type_contract::GuardKind::CaseThen {
                arm: (ordinal / 2) as u32,
            }
        };
        assert_eq!(
            flow.regions()[&actual.context.region].guard,
            Some(RegionGuard {
                owner: invocation.context.use_id,
                kind: expected
            })
        );
    }
}

#[test]
fn searched_case_first_true_wins_and_null_false_when_skip_with_sparse_source_rows() {
    let program = case_program(CaseKind::Priority);
    assert_case_domains(&program, true);
    let input = case_batch(
        &program,
        vec![
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
            None,
            Some(false),
        ],
        vec![
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
        ],
        vec![Some(25); 8],
    );
    let rows = [1, 3, 5, 6];
    let selection = Selection::try_sparse(8, &rows).unwrap();
    let output = instance(&program)
        .evaluate(&input, selection, &Control)
        .unwrap();
    assert_eq!(output.selection(), selection);
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(11), Some(22), Some(33), Some(33)]
    );
    assert!(output.errors().is_empty());
}

#[test]
fn searched_case_without_else_returns_successful_null_only_for_unmatched_rows() {
    let program = case_program(CaseKind::NoElse);
    assert_case_domains(&program, false);
    let input = case_batch(
        &program,
        vec![Some(true), None, Some(false)],
        vec![Some(true), None, Some(true)],
        vec![None; 3],
    );
    let output = instance(&program)
        .evaluate(&input, Selection::all(3), &Control)
        .unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(11), None, Some(22)]
    );
    assert!(output.errors().is_empty());
}

#[test]
fn searched_case_then_and_else_rng_instances_advance_only_actual_matches_across_batches() {
    let program = case_program(CaseKind::Random);
    let input = case_batch(
        &program,
        vec![
            None,
            Some(true),
            None,
            None,
            None,
            Some(false),
            None,
            None,
            None,
            Some(true),
        ],
        vec![
            None,
            Some(true),
            None,
            Some(true),
            None,
            Some(false),
            None,
            None,
            None,
            Some(true),
        ],
        vec![Some(25); 10],
    );
    let rows = [1, 3, 5, 9];
    let selection = Selection::try_sparse(10, &rows).unwrap();
    let mut evaluator = instance(&program);
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    assert_eq!(
        rand_bits(&output),
        vec![
            Some(SEED_42[0]),
            Some(SEED_42[0]),
            Some(SEED_42[0]),
            Some(SEED_42[1])
        ]
    );
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 3);
    let next = case_batch(
        &program,
        vec![Some(false), Some(true), Some(false)],
        vec![Some(true), Some(true), None],
        vec![Some(25); 3],
    );
    let output = evaluator
        .evaluate(&next, Selection::all(3), &Control)
        .unwrap();
    assert_eq!(
        rand_bits(&output),
        vec![Some(SEED_42[1]), Some(SEED_42[2]), Some(SEED_42[1])]
    );
    assert!(output.errors().is_empty());
}

#[test]
fn searched_case_later_round_when_is_required_only_for_remaining_rows_and_error_cannot_reach_else()
{
    let program = case_program(CaseKind::DeferredRandom);
    assert_case_domains(&program, true);
    let mut first = vec![Some(false); 8];
    first[1] = Some(true);
    first[3] = None;
    let mut values = vec![Some(25); 8];
    values[1] = Some(10_i128.pow(38) - 1);
    values[3] = None;
    values[7] = Some(10_i128.pow(38) - 1);
    let input = case_batch(&program, first, vec![None; 8], values);
    let rows = [1, 3, 5, 7];
    let selection = Selection::try_sparse(8, &rows).unwrap();
    let mut evaluator = instance(&program);
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    assert_eq!(
        rand_bits(&output),
        vec![Some(SEED_42[0]), Some(SEED_42[0]), Some(SEED_42[0]), None]
    );
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 3);
    assert_eq!(output.selection().row(3), Some(7));
    assert!(output.errors()[0].message().contains("overflow"));
    // Neither the first already-decided row nor the failed WHEN row may
    // advance ELSE. Each successful arm advanced its own actual instance once.
    let next = case_batch(
        &program,
        vec![Some(true), Some(false), Some(false)],
        vec![None; 3],
        vec![Some(10_i128.pow(38) - 1), None, Some(25)],
    );
    let output = evaluator
        .evaluate(&next, Selection::all(3), &Control)
        .unwrap();
    assert_eq!(rand_bits(&output), vec![Some(SEED_42[1]); 3]);
    assert!(output.errors().is_empty());
}

#[test]
fn searched_case_selected_then_round_error_is_terminal_and_inactive_then_overflow_is_skipped() {
    let program = case_program(CaseKind::ErrorThen);
    let input = case_batch(
        &program,
        vec![Some(true), Some(false), None, Some(true)],
        vec![Some(true), Some(true), Some(false), Some(true)],
        vec![
            Some(10_i128.pow(38) - 1),
            Some(10_i128.pow(38) - 1),
            Some(10_i128.pow(38) - 1),
            Some(25),
        ],
    );
    let output = instance(&program)
        .evaluate(&input, Selection::all(4), &Control)
        .unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(71), Some(71), Some(30)]
    );
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 0);
    assert!(output.errors()[0].message().contains("overflow"));
}

#[test]
fn searched_case_empty_selection_does_not_instantiate_any_then_else_or_when_kernel() {
    let program = case_program(CaseKind::DeferredRandom);
    let input = case_batch(
        &program,
        vec![Some(false); 4],
        vec![None; 4],
        vec![Some(10_i128.pow(38) - 1); 4],
    );
    let mut evaluator = instance(&program);
    let rows = [];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    assert!(output.values().is_empty());
    assert!(output.errors().is_empty());
    assert!(evaluator.instances.is_empty());
    let clean = case_batch(
        &program,
        vec![Some(true), Some(false), Some(false)],
        vec![None; 3],
        vec![Some(10_i128.pow(38) - 1), None, Some(25)],
    );
    let output = evaluator
        .evaluate(&clean, Selection::all(3), &Control)
        .unwrap();
    assert_eq!(rand_bits(&output), vec![Some(SEED_42[0]); 3]);
}

#[test]
fn every_actual_searched_case_callback_preserves_all_seven_causes_and_refuses_replay() {
    let program = case_program(CaseKind::DeferredRandom);
    let input = case_batch(
        &program,
        (0..320).map(|row| Some(row % 4 == 0)).collect(),
        vec![None; 320],
        (0..320)
            .map(|row| match row % 4 {
                0 | 1 => Some(10_i128.pow(38) - 1),
                2 => None,
                _ => Some(25),
            })
            .collect(),
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(&input, Selection::all(320), &recorder)
        .unwrap();
    assert_eq!(output.errors().len(), 80);
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(evaluator.evaluate(&input,Selection::all(320),&control),Err(actual) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(&input, Selection::all(320), &after),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn searched_case_all_rows_match_first_when_still_runs_then_before_empty_remaining_completion() {
    let program = case_program(CaseKind::Random);
    let input = case_batch(
        &program,
        vec![Some(true); 12],
        vec![Some(true); 12],
        vec![Some(10_i128.pow(38) - 1); 12],
    );
    let rows = [2, 7, 11];
    let selection = Selection::try_sparse(12, &rows).unwrap();
    let mut evaluator = instance(&program);
    // The first WHEN empties the pending row set. Its already matched THEN
    // remains required for every selected row; later WHEN/THEN/ELSE do not.
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    assert_eq!(output.selection(), selection);
    assert_eq!(
        rand_bits(&output),
        SEED_42[..3].iter().copied().map(Some).collect::<Vec<_>>()
    );
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 1);
    let next = case_batch(&program, vec![Some(true)], vec![Some(true)], vec![None]);
    let output = evaluator
        .evaluate(&next, Selection::all(1), &Control)
        .unwrap();
    assert_eq!(rand_bits(&output), vec![Some(SEED_42[3])]);
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 1);
}
