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

use super::*;
use crate::exec::expr::compiled_program::checked_constants;
use arrow::array::{UInt8Array, UInt16Array, UInt32Array, UInt64Array};
use arrow::record_batch::RecordBatchOptions;
use novarocks_type_contract::ExpressionEffects;

fn uints() -> [DataType; 4] {
    [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ]
}
fn profiles() -> Vec<(DataType, DataType)> {
    let all = [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ];
    all.iter()
        .flat_map(|a| {
            all.iter()
                .filter(move |b| uints().contains(a) || uints().contains(b))
                .map(move |b| (a.clone(), b.clone()))
        })
        .collect()
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum Expected {
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    F32(u32),
    F64(u64),
}
fn expected_integer(value: i128, target: &DataType) -> Option<Expected> {
    Some(match target {
        DataType::Boolean => Expected::Bool(value != 0),
        DataType::Int8 => Expected::Signed(i64::from(i8::try_from(value).ok()?)),
        DataType::Int16 => Expected::Signed(i64::from(i16::try_from(value).ok()?)),
        DataType::Int32 => Expected::Signed(i64::from(i32::try_from(value).ok()?)),
        DataType::Int64 => Expected::Signed(i64::try_from(value).ok()?),
        DataType::UInt8 => Expected::Unsigned(u64::from(u8::try_from(value).ok()?)),
        DataType::UInt16 => Expected::Unsigned(u64::from(u16::try_from(value).ok()?)),
        DataType::UInt32 => Expected::Unsigned(u64::from(u32::try_from(value).ok()?)),
        DataType::UInt64 => Expected::Unsigned(u64::try_from(value).ok()?),
        DataType::Float32 => Expected::F32((value as f32).to_bits()),
        DataType::Float64 => Expected::F64((value as f64).to_bits()),
        _ => panic!("closed scalar target"),
    })
}
fn actual(array: &ArrayRef) -> Vec<Option<Expected>> {
    macro_rules! values {
        ($a:ty,$f:expr) => {
            array
                .as_any()
                .downcast_ref::<$a>()
                .unwrap()
                .iter()
                .map(|v| v.map($f))
                .collect()
        };
    }
    match array.data_type() {
        DataType::Boolean => values!(BooleanArray, Expected::Bool),
        DataType::Int8 => values!(Int8Array, |v| Expected::Signed(i64::from(v))),
        DataType::Int16 => values!(Int16Array, |v| Expected::Signed(i64::from(v))),
        DataType::Int32 => values!(Int32Array, |v| Expected::Signed(i64::from(v))),
        DataType::Int64 => values!(Int64Array, Expected::Signed),
        DataType::UInt8 => values!(UInt8Array, |v| Expected::Unsigned(u64::from(v))),
        DataType::UInt16 => values!(UInt16Array, |v| Expected::Unsigned(u64::from(v))),
        DataType::UInt32 => values!(UInt32Array, |v| Expected::Unsigned(u64::from(v))),
        DataType::UInt64 => values!(UInt64Array, Expected::Unsigned),
        DataType::Float32 => values!(Float32Array, |v: f32| Expected::F32(v.to_bits())),
        DataType::Float64 => values!(Float64Array, |v: f64| Expected::F64(v.to_bits())),
        _ => panic!("closed scalar result"),
    }
}
fn source(carrier: &DataType) -> (ArrayRef, Vec<Option<i128>>) {
    let max = match carrier {
        DataType::UInt8 => i128::from(u8::MAX),
        DataType::UInt16 => i128::from(u16::MAX),
        DataType::UInt32 => i128::from(u32::MAX),
        DataType::UInt64 => i128::from(u64::MAX),
        DataType::Int8 => i128::from(i8::MAX),
        DataType::Int16 => i128::from(i16::MAX),
        DataType::Int32 => i128::from(i32::MAX),
        _ => i128::from(i64::MAX),
    };
    let values = if uints().contains(carrier) {
        vec![Some(0), Some(1), None, Some(max), Some(2), Some(max - 1)]
    } else {
        vec![Some(-1), Some(0), None, Some(max), Some(2), Some(1)]
    };
    let mut padded = vec![Some(9)];
    padded.extend(values.iter().copied());
    padded.push(Some(9));
    macro_rules! array {
        ($a:ty,$n:ty) => {
            Arc::new(<$a>::from(
                padded
                    .iter()
                    .map(|v| v.map(|v| <$n>::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    let array = match carrier {
        DataType::UInt8 => array!(UInt8Array, u8),
        DataType::UInt16 => array!(UInt16Array, u16),
        DataType::UInt32 => array!(UInt32Array, u32),
        DataType::UInt64 => array!(UInt64Array, u64),
        DataType::Int8 => array!(Int8Array, i8),
        DataType::Int16 => array!(Int16Array, i16),
        DataType::Int32 => array!(Int32Array, i32),
        DataType::Int64 => array!(Int64Array, i64),
        DataType::Boolean => Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(false),
            Some(false),
            Some(true),
        ])),
        DataType::Float32 => Arc::new(Float32Array::from(vec![
            Some(9.0),
            Some(-0.5),
            Some(1.9),
            None,
            Some(f32::NAN),
            Some(0.0),
            Some(f32::from_bits(0x5f800000)),
            Some(9.0),
        ])),
        DataType::Float64 => Arc::new(Float64Array::from(vec![
            Some(9.0),
            Some(-0.5),
            Some(1.9),
            None,
            Some(f64::NAN),
            Some(0.0),
            Some(f64::from_bits(0x43f0000000000000)),
            Some(9.0),
        ])),
        _ => panic!("closed source"),
    };
    let values = if carrier == &DataType::Boolean {
        vec![Some(0), Some(1), None, Some(1), Some(0), Some(0)]
    } else {
        values
    };
    (array.slice(1, 6), values)
}
fn input_array(
    program: &LocalProgram,
    array: ArrayRef,
    flags: Option<Vec<Option<bool>>>,
) -> RecordBatch {
    let n = array.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            array,
            Arc::new(Int64Array::from(vec![Some(42); n])),
            Arc::new(BooleanArray::from(
                flags.unwrap_or_else(|| vec![Some(true); n]),
            )),
        ],
    )
    .unwrap()
}
fn context(program: &LocalProgram) -> (ExpressionUseId, ExpressionEffectContext) {
    let u = cast_root_use(program);
    (u, root_unit(program).control_flow().uses()[&u].context)
}
#[test]
fn actual_compiled_unsigned_cast_all_seventy_two_profiles_preserve_sparse_slice_nulls_and_policies()
{
    let profiles = profiles();
    assert_eq!(profiles.len(), 72);
    for (from, to) in profiles {
        for allow in [false, true] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let source_type = FunctionValueType::new(from.clone(), true);
                let target_type = FunctionValueType::new(to.clone(), true);
                let program = compiled(
                    source_type.clone(),
                    target_type.clone(),
                    Source::Column,
                    Wrap::Bare,
                    policy,
                    allow,
                );
                let (u, c) = context(&program);
                let recipe = root_unit(&program).recipes().cast(u).unwrap();
                assert_eq!(recipe.source_type(), &source_type);
                assert_eq!(recipe.result_type(), &target_type);
                assert_eq!(recipe.allow_throw_exception(), allow);
                assert_eq!(recipe.policy(), policy);
                let is_float = matches!(from, DataType::Float32 | DataType::Float64);
                let mut effects = ExpressionEffects::PURE_VALUE;
                effects.may_raise_row_error = is_float && allow;
                assert_eq!(recipe.own_effects(c).for_use(c).unwrap(), effects);
                let (array, integers) = source(&from);
                let batch = input_array(&program, array, None);
                let rows = [0, 1, 2, 3, 5];
                // New floating profiles only target unsigned. These fixture values have
                // the independent old outcomes -0.5->0, 1.9->1, NULL, NaN failure, 2^64 failure.
                let expected = if is_float {
                    vec![
                        Some(Expected::Unsigned(0)),
                        Some(Expected::Unsigned(1)),
                        None,
                        None,
                        None,
                    ]
                } else {
                    rows.iter()
                        .map(|&r| integers[r].and_then(|v| expected_integer(v, &to)))
                        .collect()
                };
                let error_ordinals = if is_float && allow {
                    vec![3, 4]
                } else {
                    Vec::new()
                };
                let mut evaluator = instance(&program);
                for _ in 0..2 {
                    let result = evaluator
                        .evaluate(&batch, Selection::try_sparse(6, &rows).unwrap(), &Control)
                        .unwrap();
                    assert_eq!(result.selection().iter().collect::<Vec<_>>(), rows);
                    assert_eq!(result.values().data_type(), &to);
                    assert_eq!(actual(result.values()), expected);
                    assert_eq!(errors(&result), error_ordinals);
                }
                let result = evaluator
                    .evaluate(&batch, Selection::try_sparse(6, &[]).unwrap(), &Control)
                    .unwrap();
                assert!(result.values().is_empty());
                assert!(result.errors().is_empty());
                assert_eq!(result.values().data_type(), &to);
            }
        }
    }
}
#[test]
fn actual_compiled_unsigned_cast_u64_literal_constant_and_narrow_producers_do_not_use_i64_truth() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            for target in [
                DataType::UInt64,
                DataType::Float32,
                DataType::Float64,
                DataType::Boolean,
            ] {
                let program = compiled(
                    FunctionValueType::new(DataType::UInt64, false),
                    FunctionValueType::new(target.clone(), false),
                    Source::Constant,
                    Wrap::Bare,
                    policy,
                    allow,
                );
                let batch = input_array(
                    &program,
                    Arc::new(UInt64Array::from(vec![0, 1, u64::MAX])),
                    None,
                );
                let result = instance(&program)
                    .evaluate(&batch, Selection::all(3), &Control)
                    .unwrap();
                assert_eq!(
                    actual(result.values()),
                    vec![expected_integer(i128::from(u64::MAX), &target); 3]
                );
                assert!(result.errors().is_empty());
            }
            for target in uints() {
                let program = compiled(
                    FunctionValueType::new(DataType::UInt64, false),
                    FunctionValueType::new(target.clone(), true),
                    Source::Column,
                    Wrap::Bare,
                    policy,
                    allow,
                );
                let batch = input_array(
                    &program,
                    Arc::new(UInt64Array::from(vec![42, u64::MAX])),
                    None,
                );
                let result = instance(&program)
                    .evaluate(&batch, Selection::all(2), &Control)
                    .unwrap();
                assert_eq!(
                    actual(result.values()),
                    vec![
                        expected_integer(42, &target),
                        expected_integer(i128::from(u64::MAX), &target)
                    ]
                );
                assert!(result.errors().is_empty());
            }
        }
    }
    // Build a genuine UInt64 literal -> narrow unsigned CAST -> UInt64 CAST.
    // The intermediate permits NULL by its profile; it is not a retagged literal.
    for narrow in [DataType::UInt8, DataType::UInt16, DataType::UInt32] {
        let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
        let functions = catalogue(Shape::Decimal);
        let fid = FragmentId::new(213);
        let mut builder = FragmentBuilder::new(fid);
        let node = NodeId::new(0);
        builder
            .add_values(
                NodeId::new(u32::MAX),
                Box::from([Box::default()]),
                Box::default(),
            )
            .unwrap();
        let wide = FunctionValueType::new(DataType::UInt64, false);
        let intermediate = FunctionValueType::new(narrow.clone(), true);
        let output = FunctionValueType::new(DataType::UInt64, true);
        let literal = builder
            .unit(node)
            .unwrap()
            .add(
                None,
                (wide).clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::u64(&(wide), 42, options().constants),
                ),
            )
            .unwrap();
        let inner = builder
            .unit(node)
            .unwrap()
            .add(
                None,
                intermediate,
                ExprKind::Cast {
                    expr: literal,
                    target: narrow,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: allow_ref(),
                },
            )
            .unwrap();
        let expr = builder
            .unit(node)
            .unwrap()
            .add(
                None,
                output.clone(),
                ExprKind::Cast {
                    expr: inner,
                    target: DataType::UInt64,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: allow_ref(),
                },
            )
            .unwrap();
        let expr = builder
            .unit(node)
            .unwrap()
            .root(expr, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        let value = builder
            .add_value(output.clone(), ValueOrigin::Expr { node, root: expr })
            .unwrap();
        builder
            .add_project(
                node,
                NodeId::new(u32::MAX),
                Box::from([(expr, value)]),
                Box::from([value]),
            )
            .unwrap();
        let port = ResultPort {
            scalar_schema: None,
            fragment: fid,
            output: builder.construction_node(node).unwrap().output.clone(),
            fields: Box::from([ResultField {
                domain: crate::test_result_domain::result_value_domain(&output),
                name: "narrow_source".into(),
                alias: None,
                value,
                ty: output,
            }]),
        };
        let package = package(
            &functions,
            builder,
            node,
            &BTreeMap::new(),
            port,
            false,
            source_pools,
        );
        let program = compile(&functions, package, &Control).unwrap();
        let site = root(&program);
        let mut evaluator = CompiledExpressionInstance::try_new(
            program.unit(site.unit).unwrap().clone(),
            site.root,
            &Control,
        )
        .unwrap();
        let batch = RecordBatch::try_new_with_options(
            program.graph().nodes()[0].output_layout().schema().clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(6)),
        )
        .unwrap();
        let rows = [1, 4];
        for _ in 0..2 {
            let result = evaluator
                .evaluate(&batch, Selection::try_sparse(6, &rows).unwrap(), &Control)
                .unwrap();
            assert_eq!(
                actual(result.values()),
                vec![Some(Expected::Unsigned(42)); 2]
            );
            assert!(result.errors().is_empty());
        }
    }
}
#[test]
fn actual_compiled_unsigned_cast_fractional_negative_and_u64_float_boundaries_are_not_saturating() {
    for (source_type, array, expected) in [
        (
            DataType::Float64,
            Arc::new(Float64Array::from(vec![
                Some(-0.5),
                Some(-1.0),
                Some(f64::from_bits(0x43efffffffffffff)),
                Some(f64::from_bits(0x43f0000000000000)),
                None,
            ])) as ArrayRef,
            vec![
                Some(Expected::Unsigned(0)),
                None,
                Some(Expected::Unsigned(18446744073709549568)),
                None,
                None,
            ],
        ),
        (
            DataType::Float32,
            Arc::new(Float32Array::from(vec![
                Some(-0.5),
                Some(-1.0),
                Some(f32::from_bits(0x5f7fffff)),
                Some(f32::from_bits(0x5f800000)),
                None,
            ])) as ArrayRef,
            vec![
                Some(Expected::Unsigned(0)),
                None,
                Some(Expected::Unsigned(18446742974197923840)),
                None,
                None,
            ],
        ),
    ] {
        for allow in [false, true] {
            let program = compiled(
                FunctionValueType::new(source_type.clone(), true),
                FunctionValueType::new(DataType::UInt64, true),
                Source::Column,
                Wrap::Bare,
                DecimalOverflowPolicy::ReportError,
                allow,
            );
            let batch = input_array(&program, array.clone(), None);
            let result = instance(&program)
                .evaluate(&batch, Selection::all(5), &Control)
                .unwrap();
            assert_eq!(actual(result.values()), expected);
            assert_eq!(errors(&result), if allow { vec![1, 3] } else { vec![] });
            if allow {
                assert!(result.errors().iter().all(|e| {
                    e.message()
                        .contains("conflict with range of BIGINT UNSIGNED")
                }));
            }
        }
    }
}
#[test]
fn actual_compiled_unsigned_cast_required_add_and_round_errors_remain_terminal_under_guards() {
    for wrap in [Wrap::Bare, Wrap::Coalesce, Wrap::IsNull, Wrap::If] {
        let program = compiled(
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::UInt64, true),
            Source::Add,
            wrap,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let batch = input_array(
            &program,
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(i64::MAX),
                None,
                Some(-1),
                Some(i64::MAX),
            ])),
            Some(vec![None, Some(true), Some(true), Some(true), Some(false)]),
        );
        let rows = [1, 2, 3, 4];
        let mut evaluator = instance(&program);
        let (u, c) = context(&program);
        assert!(
            evaluator.effects[&u]
                .for_use(c)
                .unwrap()
                .may_raise_row_error
        );
        let result = evaluator
            .evaluate(&batch, Selection::try_sparse(5, &rows).unwrap(), &Control)
            .unwrap();
        assert_eq!(
            errors(&result),
            if matches!(wrap, Wrap::If) {
                vec![0]
            } else {
                vec![0, 3]
            }
        );
        assert_eq!(
            actual(result.values()),
            match wrap {
                Wrap::Bare => vec![None, None, Some(Expected::Unsigned(0)), None],
                Wrap::Coalesce => vec![
                    None,
                    Some(Expected::Unsigned(71)),
                    Some(Expected::Unsigned(0)),
                    None
                ],
                Wrap::IsNull => vec![
                    None,
                    Some(Expected::Bool(true)),
                    Some(Expected::Bool(false)),
                    None
                ],
                Wrap::If => vec![
                    None,
                    None,
                    Some(Expected::Unsigned(0)),
                    Some(Expected::Unsigned(71))
                ],
                _ => unreachable!(),
            }
        );
    }
    for allow in [false, true] {
        let (functions, p) =
            super::cast_float_tests::inherited_round_fixture_with_target(allow, DataType::UInt64);
        let program = compile(&functions, p, &Control).unwrap();
        let max = 10_i128.pow(38) - 1;
        let batch = RecordBatch::try_new(
            program.graph().nodes()[1].output_layout().schema().clone(),
            vec![
                Arc::new(Float64Array::from(vec![
                    Some(7.0),
                    Some(1.0),
                    None,
                    Some(-0.5),
                    Some(1.0),
                ])),
                Arc::new(
                    Decimal128Array::from(vec![Some(25), Some(max), None, Some(25), Some(max)])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        let rows = [1, 2, 3, 4];
        let result = instance(&program)
            .evaluate(&batch, Selection::try_sparse(5, &rows).unwrap(), &Control)
            .unwrap();
        assert_eq!(errors(&result), vec![0, 3]);
        assert_eq!(
            actual(result.values()),
            vec![None, None, Some(Expected::Unsigned(0)), None]
        );
        assert!(
            result
                .errors()
                .iter()
                .all(|e| e.message().contains("overflow"))
        );
    }
}
#[test]
fn actual_compiled_unsigned_cast_every_compile_and_runtime_callback_keeps_primary_prefix_and_failed_latch()
 {
    let (functions, p) = fixture(
        FunctionValueType::new(DataType::Float64, true),
        FunctionValueType::new(DataType::UInt64, true),
        Source::Column,
        Wrap::Bare,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
    let program = compile(&functions, p.clone(), &recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    for at in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = CompileCallbacks::new(cause.clone(), at);
            assert!(
                matches!(compile(&functions,p.clone(),&control),Err(FragmentCompileError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
    let batch = input_array(
        &program,
        Arc::new(Float64Array::from(
            (0..320)
                .map(|r| match r % 5 {
                    0 => None,
                    1 => Some(-0.5),
                    2 => Some(-1.0),
                    3 => Some(f64::NAN),
                    _ => Some(1.0),
                })
                .collect::<Vec<_>>(),
        )),
        None,
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let result = instance(&program)
        .evaluate(&batch, Selection::all(320), &recorder)
        .unwrap();
    assert!(!result.errors().is_empty());
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for at in 1..=trace.len() {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), at);
            let mut evaluator = instance(&program);
            assert!(
                matches!(evaluator.evaluate(&batch,Selection::all(320),&control),Err(actual) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
            let retry = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(&batch, Selection::all(320), &retry),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(retry.trace.lock().unwrap().is_empty());
        }
    }
}
