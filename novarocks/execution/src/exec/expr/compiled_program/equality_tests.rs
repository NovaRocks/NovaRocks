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
use novarocks_expression::ComparisonSite;
use novarocks_expression_contract::BinaryOperator;
use novarocks_type_contract::ExpressionUseId;

#[derive(Clone, Copy)]
enum EqualityCase {
    Eq,
    NotEq,
    Columns,
    RandomOperand,
    RandomLabels,
    ErrorOperand,
    ErrorLabel,
}
fn literal(
    builder: &mut FragmentBuilder,
    ty: FunctionValueType,
    value: novarocks_functions::ConstantValue,
    source_pools: &mut novarocks_physical_plan::ConstantPools,
) -> ExprId {
    builder
        .unit(NodeId::new(0))
        .unwrap()
        .add(None, ty, checked_constants::kind(source_pools, value))
        .unwrap()
}
fn random(
    builder: &mut FragmentBuilder,
    authors: &mut BTreeMap<ExprId, Author>,
    functions: &PureEngineFunctionCatalog,
    source_pools: &mut novarocks_physical_plan::ConstantPools,
) -> ExprId {
    let integer = FunctionValueType::new(DataType::Int64, false);
    let seed = literal(
        builder,
        integer.clone(),
        checked_constants::i64(&(integer.clone()), 42, options().constants),
        source_pools,
    );
    call(
        builder,
        authors,
        author(
            functions,
            "rand",
            vec![integer_argument(integer, 42)],
            ControlShape::Eager,
        ),
        vec![seed],
    )
}
fn round_value(
    builder: &mut FragmentBuilder,
    authors: &mut BTreeMap<ExprId, Author>,
    functions: &PureEngineFunctionCatalog,
    value: ExprId,
    source_pools: &mut novarocks_physical_plan::ConstantPools,
) -> ExprId {
    let integer = FunctionValueType::new(DataType::Int64, false);
    let digits = literal(
        builder,
        integer.clone(),
        checked_constants::i64(&(integer.clone()), -1, options().constants),
        source_pools,
    );
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
                integer_argument(integer, -1),
            ],
            ControlShape::Eager,
        ),
        vec![value, digits],
    )
}
fn equality_program(kind: EqualityCase) -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = catalogue(Shape::Decimal);
    let mut builder = FragmentBuilder::new(FragmentId::new(203));
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let floating = FunctionValueType::new(DataType::Float64, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let columns = [
        (ValueId::new(901), floating.clone()),
        (ValueId::new(71), floating.clone()),
        (ValueId::new(72), floating.clone()),
        (ValueId::new(3), decimal.clone()),
        (ValueId::new(99), decimal.clone()),
    ];
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = vec![];
    for (id, ty) in &columns {
        let expr = builder
            .unit(input)
            .unwrap()
            .add(
                None,
                ty.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(ty.clone()), options().constants),
                ),
            )
            .unwrap();
        let root = builder
            .unit(input)
            .unwrap()
            .root(expr, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr { node: input, root },
            })
            .unwrap();
        items.push((root, *id));
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
    let value = |builder: &mut FragmentBuilder, index: usize| {
        builder
            .unit(output)
            .unwrap()
            .add(
                None,
                columns[index].1.clone(),
                ExprKind::Value(columns[index].0),
            )
            .unwrap()
    };
    let mut authors = BTreeMap::new();
    let result_type = if matches!(kind, EqualityCase::Eq | EqualityCase::NotEq) {
        FunctionValueType::new(DataType::Boolean, true)
    } else {
        FunctionValueType::new(DataType::Int64, false)
    };
    let expr = if matches!(kind, EqualityCase::Eq | EqualityCase::NotEq) {
        let left = value(&mut builder, 0);
        let right = value(&mut builder, 1);
        builder
            .unit(output)
            .unwrap()
            .add(
                None,
                result_type.clone(),
                ExprKind::Binary {
                    op: if matches!(kind, EqualityCase::Eq) {
                        BinaryOperator::Eq
                    } else {
                        BinaryOperator::NotEq
                    },
                    left,
                    right,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: None,
                },
            )
            .unwrap()
    } else {
        let (operand, first, second) = match kind {
            EqualityCase::Columns => (
                value(&mut builder, 0),
                value(&mut builder, 1),
                value(&mut builder, 2),
            ),
            EqualityCase::RandomOperand => {
                let operand = random(&mut builder, &mut authors, &functions, &mut source_pools);
                let first = literal(
                    &mut builder,
                    floating.clone(),
                    checked_constants::f64_bits(
                        &(floating.clone()),
                        SEED_42[0],
                        options().constants,
                    ),
                    &mut source_pools,
                );
                let second = literal(
                    &mut builder,
                    (floating).clone(),
                    checked_constants::f64_bits(&(floating), SEED_42[1], options().constants),
                    &mut source_pools,
                );
                (operand, first, second)
            }
            EqualityCase::RandomLabels => (
                value(&mut builder, 0),
                random(&mut builder, &mut authors, &functions, &mut source_pools),
                random(&mut builder, &mut authors, &functions, &mut source_pools),
            ),
            EqualityCase::ErrorOperand => {
                let source = value(&mut builder, 3);
                let operand = round_value(
                    &mut builder,
                    &mut authors,
                    &functions,
                    source,
                    &mut source_pools,
                );
                (
                    operand,
                    value(&mut builder, 4),
                    literal(
                        &mut builder,
                        decimal.clone(),
                        checked_constants::null(&(decimal.clone()), options().constants),
                        &mut source_pools,
                    ),
                )
            }
            EqualityCase::ErrorLabel => {
                let operand = value(&mut builder, 3);
                let source = value(&mut builder, 4);
                let first = round_value(
                    &mut builder,
                    &mut authors,
                    &functions,
                    source,
                    &mut source_pools,
                );
                // The later label is a separate occurrence of the original
                // operand definition; no failed first label may reach it.
                (operand, first, operand)
            }
            EqualityCase::Eq | EqualityCase::NotEq => unreachable!(),
        };
        let then_first = literal(
            &mut builder,
            result_type.clone(),
            checked_constants::i64(&(result_type.clone()), 10, options().constants),
            &mut source_pools,
        );
        let then_second = literal(
            &mut builder,
            result_type.clone(),
            checked_constants::i64(&(result_type.clone()), 20, options().constants),
            &mut source_pools,
        );
        let otherwise = literal(
            &mut builder,
            result_type.clone(),
            checked_constants::i64(&(result_type.clone()), 30, options().constants),
            &mut source_pools,
        );
        builder
            .unit(output)
            .unwrap()
            .add(
                None,
                result_type.clone(),
                ExprKind::Case {
                    operand: Some(operand),
                    when_then: Box::from([(first, then_first), (second, then_second)]),
                    else_expr: Some(otherwise),
                },
            )
            .unwrap()
    };
    let expr = builder
        .unit(output)
        .unwrap()
        .root(expr, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let result_value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: output,
                root: expr,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(expr, result_value)]),
            Box::from([result_value]),
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: FragmentId::new(203),
        output: builder.construction_node(output).unwrap().output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "equality_result".into(),
            alias: None,
            value: result_value,
            ty: result_type,
        }]),
    };
    compile_checked_fragment(&functions, builder, output, &authors, result, source_pools)
}
fn batch(
    program: &LocalProgram,
    left: Vec<Option<f64>>,
    first: Vec<Option<f64>>,
    second: Vec<Option<f64>>,
    decimal: Vec<Option<i128>>,
    decimal_label: Vec<Option<i128>>,
) -> RecordBatch {
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(Float64Array::from(left)),
            Arc::new(Float64Array::from(first)),
            Arc::new(Float64Array::from(second)),
            Arc::new(
                Decimal128Array::from(decimal)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(decimal_label)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}
fn float_batch(
    program: &LocalProgram,
    left: Vec<Option<f64>>,
    first: Vec<Option<f64>>,
    second: Vec<Option<f64>>,
) -> RecordBatch {
    let rows = left.len();
    batch(
        program,
        left,
        first,
        second,
        vec![None; rows],
        vec![None; rows],
    )
}
fn integers(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<i64>> {
    output
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn root_use(program: &LocalProgram) -> ExpressionUseId {
    let reference = root(program);
    program
        .unit(reference.unit)
        .unwrap()
        .root(reference.root)
        .unwrap()
        .use_id
}
fn assert_case_recipes(program: &LocalProgram, ty: DataType) {
    let occurrence = root_use(program);
    for arm in 0..2 {
        let recipe = root_unit(program)
            .recipes()
            .comparison(ComparisonSite::CaseWhen { occurrence, arm })
            .unwrap();
        assert_eq!(recipe.left_type().data_type().clone(), ty);
        assert_eq!(recipe.right_type().data_type().clone(), ty);
    }
    assert!(
        root_unit(program)
            .recipes()
            .comparison(ComparisonSite::CaseWhen { occurrence, arm: 2 })
            .is_none()
    );
}

#[test]
fn ordinary_eq_and_not_eq_preserve_frozen_float_bits_and_nulls_in_sparse_rows() {
    // This is the accepted ordinary bit identity contract, not IEEE ==:
    // equal NaN payloads compare equal, and opposite signed zero differs.
    let nan_a = f64::from_bits(0x7ff8_0000_0000_0001);
    let nan_b = f64::from_bits(0x7ff8_0000_0000_0002);
    let left = vec![
        Some(99.0),
        Some(nan_a),
        Some(nan_a),
        Some(0.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
        Some(1.5),
        Some(1.5),
    ];
    let right = vec![
        Some(-99.0),
        Some(nan_a),
        Some(nan_b),
        Some(-0.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::INFINITY),
        None,
        None,
        Some(2.5),
    ];
    let rows = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    for kind in [EqualityCase::Eq, EqualityCase::NotEq] {
        let program = equality_program(kind);
        let recipe = root_unit(&program)
            .recipes()
            .comparison(ComparisonSite::Binary(root_use(&program)))
            .unwrap();
        assert_eq!(
            recipe.left_type(),
            &FunctionValueType::new(DataType::Float64, true)
        );
        assert_eq!(
            recipe.right_type(),
            &FunctionValueType::new(DataType::Float64, true)
        );
        let input = float_batch(&program, left.clone(), right.clone(), vec![None; 10]);
        let selection = Selection::try_sparse(10, &rows).unwrap();
        let output = instance(&program)
            .evaluate(&input, selection, &Control)
            .unwrap();
        let eq = [
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(false),
            None,
            None,
            Some(false),
        ];
        let expected = eq.map(|v| {
            v.map(|v| {
                if matches!(kind, EqualityCase::Eq) {
                    v
                } else {
                    !v
                }
            })
        });
        assert_eq!(output.selection(), selection);
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
        assert!(output.errors().is_empty());
    }
}

#[test]
fn simple_case_source_labels_keep_first_match_null_nonmatch_sparse_rows_and_recipe_occurrences() {
    let program = equality_program(EqualityCase::Columns);
    assert_case_recipes(&program, DataType::Float64);
    let input = float_batch(
        &program,
        vec![Some(99.0), Some(2.0), None, Some(4.0), Some(5.0), Some(6.0)],
        vec![Some(0.0), Some(2.0), None, Some(9.0), None, Some(0.0)],
        vec![Some(0.0), Some(2.0), None, Some(4.0), Some(5.0), Some(7.0)],
    );
    let rows = [1, 2, 3, 4, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let output = instance(&program)
        .evaluate(&input, selection, &Control)
        .unwrap();
    assert_eq!(output.selection(), selection);
    assert_eq!(
        integers(&output),
        vec![Some(10), Some(30), Some(20), Some(20), Some(30)]
    );
    assert!(output.errors().is_empty());
}

#[test]
fn simple_case_rand_operand_is_evaluated_once_per_selected_parent_across_all_when_arms() {
    let program = equality_program(EqualityCase::RandomOperand);
    assert_case_recipes(&program, DataType::Float64);
    let input = float_batch(&program, vec![None; 6], vec![None; 6], vec![None; 6]);
    let rows = [1, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let mut evaluator = instance(&program);
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    assert_eq!(integers(&output), vec![Some(10), Some(20)]);
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 1);
    let next = float_batch(&program, vec![None], vec![None], vec![None]);
    let output = evaluator
        .evaluate(&next, Selection::all(1), &Control)
        .unwrap();
    assert_eq!(integers(&output), vec![Some(30)]);
    assert_eq!(evaluator.instances.len(), 1);
}

#[test]
fn simple_case_rand_when_labels_only_advance_remaining_rows_with_independent_state() {
    let program = equality_program(EqualityCase::RandomLabels);
    assert_case_recipes(&program, DataType::Float64);
    let mut operand = vec![Some(-1.0); 9];
    operand[1] = Some(f64::from_bits(SEED_42[0]));
    operand[5] = Some(f64::from_bits(SEED_42[1]));
    operand[8] = Some(f64::from_bits(SEED_42[1]));
    let input = float_batch(&program, operand, vec![None; 9], vec![None; 9]);
    let rows = [1, 3, 5, 8];
    let selection = Selection::try_sparse(9, &rows).unwrap();
    let mut evaluator = instance(&program);
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    // WHEN1 samples r0..r3; only row1 matches. WHEN2 samples r0..r2
    // over the three remaining rows; only original row5 matches r1.
    assert_eq!(
        integers(&output),
        vec![Some(10), Some(30), Some(20), Some(30)]
    );
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 2);
    let next = float_batch(
        &program,
        vec![Some(f64::from_bits(SEED_42[3])), Some(-1.0)],
        vec![None; 2],
        vec![None; 2],
    );
    let output = evaluator
        .evaluate(&next, Selection::all(2), &Control)
        .unwrap();
    // WHEN1 now uses r4,r5; WHEN2 uses its own r3,r4.
    assert_eq!(integers(&output), vec![Some(20), Some(30)]);
    assert!(output.errors().is_empty());
}

#[test]
fn simple_case_round_operand_and_label_errors_are_terminal_without_null_match_or_fallthrough() {
    for kind in [EqualityCase::ErrorOperand, EqualityCase::ErrorLabel] {
        let program = equality_program(kind);
        assert_case_recipes(&program, DataType::Decimal128(38, 0));
        let rows = [1, 3, 5, 7];
        let mut source = vec![Some(25); 8];
        let mut label = vec![Some(30); 8];
        if matches!(kind, EqualityCase::ErrorOperand) {
            source[1] = None;
            source[5] = Some(10_i128.pow(38) - 1);
        } else {
            source = vec![Some(30); 8];
            source[1] = None;
            label[3] = Some(25);
            label[5] = Some(10_i128.pow(38) - 1);
            label[7] = None;
        }
        let input = batch(
            &program,
            vec![None; 8],
            vec![None; 8],
            vec![None; 8],
            source,
            label,
        );
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let output = instance(&program)
            .evaluate(&input, selection, &Control)
            .unwrap();
        assert_eq!(output.selection(), selection);
        assert_eq!(
            integers(&output),
            if matches!(kind, EqualityCase::ErrorOperand) {
                vec![Some(30), Some(10), None, Some(10)]
            } else {
                vec![Some(30), Some(10), None, Some(20)]
            }
        );
        assert_eq!(output.errors().len(), 1);
        assert_eq!(output.errors()[0].selected_ordinal(), 2);
        assert_eq!(output.selection().row(2), Some(5));
        assert!(output.errors()[0].message().contains("overflow"));
    }
}

#[test]
fn simple_case_empty_selection_does_not_initialize_operand_or_when_rand_instances() {
    for kind in [EqualityCase::RandomOperand, EqualityCase::RandomLabels] {
        let program = equality_program(kind);
        let input = float_batch(&program, vec![None; 4], vec![None; 4], vec![None; 4]);
        let mut evaluator = instance(&program);
        let rows = [];
        let output = evaluator
            .evaluate(&input, Selection::try_sparse(4, &rows).unwrap(), &Control)
            .unwrap();
        assert!(output.values().is_empty());
        assert!(output.errors().is_empty());
        assert!(evaluator.instances.is_empty());
        let next = float_batch(
            &program,
            vec![Some(f64::from_bits(SEED_42[0]))],
            vec![None],
            vec![None],
        );
        let output = evaluator
            .evaluate(&next, Selection::all(1), &Control)
            .unwrap();
        assert_eq!(integers(&output), vec![Some(10)]);
    }
}

#[test]
fn every_actual_simple_case_callback_preserves_all_seven_primary_failures_and_refuses_replay() {
    let program = equality_program(EqualityCase::ErrorLabel);
    let input = batch(
        &program,
        vec![None; 320],
        vec![None; 320],
        vec![None; 320],
        vec![Some(30); 320],
        (0..320)
            .map(|row| match row % 3 {
                0 => None,
                1 => Some(25),
                _ => Some(10_i128.pow(38) - 1),
            })
            .collect(),
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(&input, Selection::all(320), &recorder)
        .unwrap();
    assert_eq!(output.errors().len(), 106);
    assert_eq!(
        integers(&output),
        (0..320)
            .map(|row| match row % 3 {
                0 => Some(20),
                1 => Some(10),
                _ => None,
            })
            .collect::<Vec<_>>()
    );
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(evaluator.evaluate(&input, Selection::all(320), &control), Err(actual) if actual == cause)
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

#[path = "ordered_comparison_tests.rs"]
mod ordered_comparison_tests;
