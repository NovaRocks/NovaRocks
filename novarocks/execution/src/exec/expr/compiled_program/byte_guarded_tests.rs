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
use arrow::array::{ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};

fn carriers() -> [DataType; 4] {
    [
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::LargeBinary,
    ]
}

// Typed NULL definitions author the incoming layout; actual byte values below
// come from source channels, not a hypothetical string literal compiler.
fn byte_program(carrier: DataType, required_error: bool) -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = catalogue(Shape::Decimal);
    let mut builder = FragmentBuilder::new(FragmentId::new(201));
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let bytes = FunctionValueType::new(carrier, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let columns = [
        (ValueId::new(901), boolean.clone()),
        (ValueId::new(71), bytes.clone()),
        (ValueId::new(72), bytes.clone()),
        (ValueId::new(3), decimal.clone()),
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
    let mut authors = BTreeMap::new();
    let left = builder
        .unit(output)
        .unwrap()
        .add(None, bytes.clone(), ExprKind::Value(columns[1].0))
        .unwrap();
    let right = builder
        .unit(output)
        .unwrap()
        .add(None, bytes.clone(), ExprKind::Value(columns[2].0))
        .unwrap();
    let (condition, matched, otherwise) = if required_error {
        let value = builder
            .unit(output)
            .unwrap()
            .add(None, decimal.clone(), ExprKind::Value(columns[3].0))
            .unwrap();
        let digits = builder
            .unit(output)
            .unwrap()
            .add(
                None,
                (FunctionValueType::new(DataType::Int64, false)).clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::i64(
                        &(FunctionValueType::new(DataType::Int64, false)),
                        -1,
                        options().constants,
                    ),
                ),
            )
            .unwrap();
        let rounded = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "round",
                vec![
                    argument(decimal, None),
                    integer_argument(FunctionValueType::new(DataType::Int64, false), -1),
                ],
                ControlShape::Eager,
            ),
            vec![value, digits],
        );
        let condition = builder
            .unit(output)
            .unwrap()
            .add(
                None,
                FunctionValueType::new(DataType::Boolean, false),
                ExprKind::IsNull {
                    expr: rounded,
                    negated: false,
                },
            )
            .unwrap();
        (condition, left, right)
    } else {
        let flag = builder
            .unit(output)
            .unwrap()
            .add(None, boolean.clone(), ExprKind::Value(columns[0].0))
            .unwrap();
        let matched = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(boolean, None),
                    argument(bytes.clone(), None),
                    argument(bytes.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![flag, left, right],
        );
        let otherwise = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "coalesce",
                vec![argument(bytes.clone(), None), argument(bytes.clone(), None)],
                ControlShape::Coalesce,
            ),
            vec![left, right],
        );
        (flag, matched, otherwise)
    };
    let expr = builder
        .unit(output)
        .unwrap()
        .add(
            None,
            bytes.clone(),
            ExprKind::Case {
                operand: None,
                when_then: Box::from([(condition, matched)]),
                else_expr: Some(otherwise),
            },
        )
        .unwrap();
    let expr = builder
        .unit(output)
        .unwrap()
        .root(expr, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let result_value = builder
        .add_value(
            bytes.clone(),
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
        fragment: FragmentId::new(201),
        output: builder.construction_node(output).unwrap().output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&bytes),
            name: "byte_result".into(),
            alias: None,
            value: result_value,
            ty: bytes,
        }]),
    };
    compile_checked_fragment(&functions, builder, output, &authors, result, source_pools)
}

fn byte_array(ty: &DataType, values: &[Option<&[u8]>]) -> ArrayRef {
    match ty {
        DataType::Utf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| v.map(|b| std::str::from_utf8(b).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(
            values
                .iter()
                .map(|v| v.map(|b| std::str::from_utf8(b).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Binary => Arc::new(BinaryArray::from(values.to_vec())),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(values.to_vec())),
        _ => unreachable!("four offset carriers only"),
    }
}
fn byte_batch(
    program: &LocalProgram,
    flags: Vec<Option<bool>>,
    left: &[Option<&[u8]>],
    right: &[Option<&[u8]>],
    decimals: Vec<Option<i128>>,
) -> RecordBatch {
    let schema = program.graph().nodes()[1].output_layout().schema().clone();
    let ty = schema.field(1).data_type();
    // Nonempty prefix/suffix storage proves source array offsets are honoured.
    let sliced = |values: &[Option<&[u8]>]| {
        let mut backing = vec![Some(b"unused-prefix".as_slice())];
        backing.extend_from_slice(values);
        backing.push(Some(b"unused-suffix".as_slice()));
        byte_array(ty, &backing).slice(1, values.len())
    };
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BooleanArray::from(flags)),
            sliced(left),
            sliced(right),
            Arc::new(
                Decimal128Array::from(decimals)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}
fn bytes_of(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<Vec<u8>>> {
    let array = output.values();
    (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                return None;
            }
            let value: &[u8] = match array.data_type() {
                DataType::Utf8 => array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .as_bytes(),
                DataType::LargeUtf8 => array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
                    .value(row)
                    .as_bytes(),
                DataType::Binary => array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(row),
                DataType::LargeBinary => array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap()
                    .value(row),
                _ => unreachable!("four offset carriers only"),
            };
            Some(value.to_vec())
        })
        .collect()
}

#[test]
fn four_byte_carriers_nested_if_case_coalesce_keep_sparse_sliced_values_and_success_nulls() {
    for ty in carriers() {
        let program = byte_program(ty.clone(), false);
        let mut flags = vec![Some(false); 8];
        flags[1] = Some(true);
        flags[3] = None;
        flags[5] = Some(true);
        let mut left: Vec<Option<&[u8]>> = vec![Some(b"unselected"); 8];
        left[1] = Some(b"");
        left[3] = None;
        left[5] = None;
        left[7] = Some(if matches!(ty, DataType::Binary | DataType::LargeBinary) {
            b"\0\xff\x80"
        } else {
            "中\0e\u{301}😀".as_bytes()
        });
        let right: Vec<Option<&[u8]>> = vec![Some(b"fallback"); 8];
        let input = byte_batch(&program, flags, &left, &right, vec![None; 8]);
        let rows = [1, 3, 5, 7];
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let mut evaluator = instance(&program);
        let output = evaluator.evaluate(&input, selection, &Control).unwrap();
        assert_eq!(output.values().data_type(), &ty);
        assert_eq!(output.selection(), selection);
        assert_eq!(
            bytes_of(&output),
            vec![
                Some(vec![]),
                Some(b"fallback".to_vec()),
                None,
                left[7].map(<[u8]>::to_vec)
            ]
        );
        assert!(output.errors().is_empty());
        let next = byte_batch(
            &program,
            vec![Some(false), None, Some(true)],
            &[None, None, Some(b"next")],
            &[None, Some(b"last"), Some(b"unused")],
            vec![None; 3],
        );
        let output = evaluator
            .evaluate(&next, Selection::all(3), &Control)
            .unwrap();
        assert_eq!(
            bytes_of(&output),
            vec![None, Some(b"last".to_vec()), Some(b"next".to_vec())]
        );
        assert!(output.errors().is_empty());
    }
}

#[test]
fn four_byte_carriers_required_round_when_error_is_null_placeholder_without_branch_fallback() {
    for ty in carriers() {
        let program = byte_program(ty.clone(), true);
        let mut decimals = vec![Some(25); 8];
        decimals[1] = None;
        decimals[5] = Some(10_i128.pow(38) - 1);
        let input = byte_batch(
            &program,
            vec![None; 8],
            &[Some(b"then".as_slice()); 8],
            &[Some(b"else".as_slice()); 8],
            decimals,
        );
        let rows = [1, 3, 5, 7];
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let mut evaluator = instance(&program);
        let output = evaluator.evaluate(&input, selection, &Control).unwrap();
        assert_eq!(output.values().data_type(), &ty);
        assert_eq!(
            bytes_of(&output),
            vec![
                Some(b"then".to_vec()),
                Some(b"else".to_vec()),
                None,
                Some(b"else".to_vec())
            ]
        );
        assert_eq!(output.errors().len(), 1);
        assert_eq!(output.errors()[0].selected_ordinal(), 2);
        assert_eq!(output.selection().row(2), Some(5));
        assert!(output.errors()[0].message().contains("overflow"));
        let next = byte_batch(
            &program,
            vec![None],
            &[Some(b"clean")],
            &[Some(b"right")],
            vec![None],
        );
        let output = evaluator
            .evaluate(&next, Selection::all(1), &Control)
            .unwrap();
        assert_eq!(bytes_of(&output), vec![Some(b"clean".to_vec())]);
        assert!(output.errors().is_empty());
    }
}

#[test]
fn four_byte_carriers_empty_selection_skips_required_round_and_keeps_exact_empty_output() {
    for ty in carriers() {
        let program = byte_program(ty.clone(), true);
        let input = byte_batch(
            &program,
            vec![None; 3],
            &[None; 3],
            &[Some(b"fallback".as_slice()); 3],
            vec![Some(10_i128.pow(38) - 1); 3],
        );
        let mut evaluator = instance(&program);
        let rows = [];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let output = evaluator.evaluate(&input, selection, &Control).unwrap();
        assert_eq!(output.values().data_type(), &ty);
        assert_eq!(output.selection(), selection);
        assert!(output.values().is_empty());
        assert!(output.errors().is_empty());
        assert!(evaluator.instances.is_empty());
    }
}

#[test]
fn every_actual_byte_guarded_callback_preserves_seven_causes_and_refuses_replay() {
    let program = byte_program(DataType::Utf8, true);
    let input = byte_batch(
        &program,
        vec![None; 320],
        &[Some("中".as_bytes()); 320],
        &[Some(b"fallback".as_slice()); 320],
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
        bytes_of(&output),
        (0..320)
            .map(|row| match row % 3 {
                0 => Some("中".as_bytes().to_vec()),
                1 => Some(b"fallback".to_vec()),
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
