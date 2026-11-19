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

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, NullArray,
        StringArray, StructArray,
    },
    buffer::NullBuffer,
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_expression::{
    EvaluationInput, ExprEvaluationFailure,
    testkit::{
        CallbackControl, PassThroughCallHost, add_call,
        guarded_fixtures::{
            Fixture, GUARDED_CONTROL as Control, argument, author, causes, round_value,
            value_conversion::{
                array_common_coerced, conversion_program, converted, direct_program, largeint_type,
                nominal,
            },
        },
    },
};
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
    array_projection::normalize_execution_array,
};
use novarocks_type_contract::{
    ControlShape, FunctionArgumentType, FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueField,
    ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};
use std::sync::Arc;

fn input_batch(program: &Fixture, arrays: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(program.input_schema().clone(), arrays).unwrap()
}
fn wide(values: &[Option<i128>]) -> ArrayRef {
    let bytes = values
        .iter()
        .map(|value| value.map(i128::to_be_bytes))
        .collect::<Vec<_>>();
    Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            bytes
                .iter()
                .map(|value| value.as_ref().map(|value| value.as_slice())),
            16,
        )
        .unwrap(),
    )
}
fn wide_values(array: &ArrayRef) -> Vec<Option<i128>> {
    let array = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..array.len())
        .map(|row| {
            (!array.is_null(row)).then(|| i128::from_be_bytes(array.value(row).try_into().unwrap()))
        })
        .collect()
}
fn signed_values(array: &ArrayRef) -> Vec<Option<i64>> {
    macro_rules! read {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .unwrap()
                .iter()
                .map(|value| value.map(i64::from))
                .collect()
        };
    }
    match array.data_type() {
        DataType::Int8 => read!(Int8Array),
        DataType::Int16 => read!(Int16Array),
        DataType::Int32 => read!(Int32Array),
        DataType::Int64 => read!(Int64Array),
        _ => panic!("signed conversion target"),
    }
}
fn signed_array(carrier: &DataType, values: &[Option<i64>]) -> ArrayRef {
    macro_rules! make {
        ($ty:ty, $native:ty) => {
            Arc::new(<$ty>::from(
                values
                    .iter()
                    .map(|value| value.map(|value| <$native>::try_from(value).unwrap()))
                    .collect::<Vec<_>>(),
            ))
        };
    }
    match carrier {
        DataType::Int8 => make!(Int8Array, i8),
        DataType::Int16 => make!(Int16Array, i16),
        DataType::Int32 => make!(Int32Array, i32),
        DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
        _ => panic!("signed conversion source"),
    }
}
fn result_type(program: &Fixture) -> FunctionValueType {
    let root = program.unit.root(program.root).unwrap();
    let Some(FunctionArgumentType::Value(value_type)) =
        program.unit.types().get(root.definition.index())
    else {
        panic!("complete compiled output occurrence");
    };
    value_type.clone()
}

#[test]
fn actual_compiled_value_conversion_signed_four_widths_extend_exact_be16_at_sparse_addresses() {
    for (carrier, min, max) in [
        (DataType::Int8, i64::from(i8::MIN), i64::from(i8::MAX)),
        (DataType::Int16, i64::from(i16::MIN), i64::from(i16::MAX)),
        (DataType::Int32, i64::from(i32::MIN), i64::from(i32::MAX)),
        (DataType::Int64, i64::MIN, i64::MAX),
    ] {
        let program = direct_program(
            FunctionValueType::new(carrier.clone(), true),
            largeint_type(),
        );
        let array = signed_array(
            &carrier,
            &[
                Some(7),
                Some(min),
                None,
                Some(-1),
                Some(max),
                Some(0),
                Some(9),
            ],
        );
        let input = input_batch(&program, vec![array.slice(1, 5)]);
        let rows = [0, 1, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let mut evaluator = program.instance(&Control);
        for _ in 0..2 {
            let output = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(output.selection().iter().collect::<Vec<_>>(), rows);
            assert_eq!(
                wide_values(output.values()),
                vec![Some(i128::from(min)), None, Some(i128::from(max)), Some(0)]
            );
            assert!(output.errors().is_empty());
        }
        let empty = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(5, &[]).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(empty.values().is_empty());
        assert!(empty.errors().is_empty());
    }
}

#[test]
fn actual_compiled_value_conversion_signed_overflow_is_success_null_consumed_by_coalesce_and_isnull()
 {
    for (carrier, min, max) in [
        (DataType::Int8, i128::from(i8::MIN), i128::from(i8::MAX)),
        (DataType::Int16, i128::from(i16::MIN), i128::from(i16::MAX)),
        (DataType::Int32, i128::from(i32::MIN), i128::from(i32::MAX)),
        (DataType::Int64, i128::from(i64::MIN), i128::from(i64::MAX)),
    ] {
        let target = FunctionValueType::new(carrier.clone(), true);
        for null_test in [false, true] {
            let program = conversion_program(
                &if null_test {
                    vec![largeint_type()]
                } else {
                    vec![largeint_type(), target.clone()]
                },
                |functions, builder, authors, leaves, _source_pools| {
                    let narrowed = converted(
                        functions,
                        builder,
                        authors,
                        leaves[0],
                        largeint_type(),
                        target.clone(),
                    );
                    if null_test {
                        // IS NULL observes success NULL from range overflow, not row errors.
                        builder
                            .add(
                                None,
                                FunctionValueType::new(DataType::Boolean, false),
                                ExprKind::IsNull {
                                    expr: narrowed,
                                    negated: false,
                                },
                            )
                            .unwrap()
                    } else {
                        add_call(
                            builder,
                            authors,
                            author(
                                functions,
                                "coalesce",
                                vec![
                                    argument(target.clone(), None),
                                    argument(target.clone(), None),
                                ],
                                ControlShape::Coalesce,
                            ),
                            vec![narrowed, leaves[1]],
                        )
                    }
                },
            );
            let mut arrays = vec![wide(&[
                Some(0),
                Some(min - 1),
                Some(min),
                None,
                Some(max),
                Some(max + 1),
            ])];
            if !null_test {
                arrays.push(signed_array(&carrier, &[Some(7); 6]));
            }
            let input = input_batch(&program, arrays);
            let rows = [1, 2, 3, 4, 5];
            let output = program
                .instance(&Control)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    {
                        let selection = Selection::try_sparse(6, &rows).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert!(output.errors().is_empty());
            if null_test {
                assert_eq!(
                    output
                        .values()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(true), Some(false), Some(true), Some(false), Some(true)]
                );
            } else {
                assert_eq!(
                    signed_values(output.values()),
                    vec![
                        Some(7),
                        Some(i64::try_from(min).unwrap()),
                        Some(7),
                        Some(i64::try_from(max).unwrap()),
                        Some(7)
                    ]
                );
            }
        }
    }
}

#[test]
fn actual_compiled_value_conversion_largeint_float_direct_rounding_avoids_f64_intermediate() {
    let trap = (1_i128 << 80) + (1_i128 << 56) + 1;
    let values = [
        Some(0),
        Some(trap),
        Some(i128::MIN),
        None,
        Some(i128::MAX),
        Some(-trap),
    ];
    for carrier in [DataType::Float32, DataType::Float64] {
        let target = FunctionValueType::new(carrier.clone(), true);
        let program = direct_program(largeint_type(), target.clone());
        assert_eq!(result_type(&program), target);
        let input = input_batch(&program, vec![wide(&values)]);
        let rows = [1, 2, 3, 4, 5];
        let output = program
            .instance(&Control)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(6, &rows).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        if carrier == DataType::Float32 {
            let actual = output
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .map(|value| value.map(f32::to_bits))
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                vec![
                    Some(0x67800001),
                    Some(0xff000000),
                    None,
                    Some(0x7f000000),
                    Some(0xe7800001)
                ]
            );
            assert_eq!((trap as f64 as f32).to_bits(), 0x67800000);
        } else {
            let actual = output
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|value| value.map(f64::to_bits))
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                vec![
                    Some(0x44f0000010000000),
                    Some(0xc7e0000000000000),
                    None,
                    Some(0x47e0000000000000),
                    Some(0xc4f0000010000000)
                ]
            );
        }
    }
}

fn tagged_field(name: &str, carrier: DataType, tag: Option<&str>, field_id: &str) -> Arc<Field> {
    let mut metadata = std::collections::HashMap::from([
        ("PARQUET:field_id".to_owned(), field_id.to_owned()),
        ("provider.fact".to_owned(), "immutable-source".to_owned()),
    ]);
    if let Some(tag) = tag {
        metadata.insert(NR_LOGICAL_TYPE_KEY.to_owned(), tag.to_owned());
    }
    Arc::new(Field::new(name, carrier, true).with_metadata(metadata))
}

#[test]
fn actual_compiled_value_conversion_json_root_and_nested_struct_preserve_bytes_nulls_and_nominal_sibling()
 {
    let source = nominal(DataType::Utf8, ValueLogicalType::Json);
    let target = FunctionValueType::new(DataType::Utf8, true);
    let program = direct_program(source, target.clone());
    let original: ArrayRef = Arc::new(StringArray::from(vec![
        Some("unused"),
        Some("{\"a\":1}"),
        None,
        Some("bad-json-is-unchanged"),
        Some("suffix"),
    ]));
    let input = input_batch(&program, vec![original.slice(1, 3)]);
    let rows = [0, 1, 2];
    let output = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            {
                let selection = Selection::try_sparse(3, &rows).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(result_type(&program), target);
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("{\"a\":1}"), None, Some("bad-json-is-unchanged")]
    );

    let json = tagged_field("payload", DataType::Utf8, Some("json"), "17");
    let text = tagged_field("payload", DataType::Utf8, None, "17");
    let sibling = tagged_field("opaque", DataType::LargeBinary, Some("variant"), "29");
    let source_fields = vec![json, sibling.clone()];
    let target_fields = vec![text, sibling];
    let explicit_struct = |fields: &[Arc<Field>], payload: ValueKind| {
        let storage = ValueTypeStorageLifetime::pure();
        let payload = ValueTypeRef::try_new(payload, true, storage.clone()).unwrap();
        let sibling = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Variant),
            true,
            storage.clone(),
        )
        .unwrap();
        let value = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![
                    ValueField::new("payload", payload, storage.clone()),
                    ValueField::new("opaque", sibling, storage.clone()),
                ]
                .into_boxed_slice(),
            ),
            true,
            storage.clone(),
        )
        .unwrap();
        FunctionValueType::try_from_value(
            value,
            ValueLayout::try_new(&DataType::Struct(fields.to_vec().into()), storage).unwrap(),
        )
        .unwrap()
    };
    let source = explicit_struct(&source_fields, ValueKind::Nominal(ValueLogicalType::Json));
    let target = explicit_struct(&target_fields, ValueKind::String);
    let program = direct_program(source, target.clone());
    let original: ArrayRef = Arc::new(StructArray::new(
        source_fields.into(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("prefix"),
                Some("null"),
                Some("hidden"),
                None,
                Some("[1]"),
                Some("suffix"),
            ])),
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"prefix".as_slice()),
                Some(b"\x01".as_slice()),
                Some(b"hidden".as_slice()),
                Some(b"\x02".as_slice()),
                None,
                Some(b"suffix".as_slice()),
            ])),
        ],
        Some(NullBuffer::from(vec![true, true, false, true, true, true])),
    ));
    let normalized = normalize_execution_array(&original).unwrap();
    let input = input_batch(&program, vec![normalized.slice(1, 4)]);
    let rows = [0, 1, 3];
    let output = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            {
                let selection = Selection::try_sparse(4, &rows).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(result_type(&program), target);
    assert!(output.errors().is_empty());
    let array = output
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let DataType::Struct(expected_fields) = target.data_type() else {
        panic!("explicit Struct target");
    };
    assert_eq!(array.fields(), expected_fields);
    assert!(!array.is_null(0));
    assert!(array.is_null(1));
    assert!(!array.is_null(2));
    let text = array
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(text.value(0), "null");
    assert_eq!(text.value(2), "[1]");
    let opaque = array
        .column(1)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(opaque.value(0), b"\x01");
    assert!(opaque.is_null(2));
    assert_eq!(
        ValueLogicalType::Variant,
        result_type(&program).child_at(1).unwrap().logical_type()
    );
    assert!(
        array
            .fields()
            .iter()
            .all(|field| field.metadata().as_arc().is_none())
    );
    assert_eq!(target_fields[1].metadata()["PARQUET:field_id"], "29");
}

#[test]
fn actual_compiled_value_conversion_null_lift_preserves_nonencoded_target_type_sparse_empty_and_repeated_batches()
 {
    let struct_field = tagged_field("number", DataType::Int64, None, "43");
    for target in [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Struct(vec![struct_field].into()), true),
    ] {
        let program = direct_program(FunctionValueType::new(DataType::Null, true), target.clone());
        assert_eq!(result_type(&program), target);
        let input = input_batch(&program, vec![Arc::new(NullArray::new(6))]);
        let mut evaluator = program.instance(&Control);
        let rows = [1, 4];
        for _ in 0..2 {
            let output = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    {
                        let selection = Selection::try_sparse(6, &rows).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(output.selection().iter().collect::<Vec<_>>(), rows);
            assert_eq!(
                output.values().data_type().clone(),
                target.data_type().clone()
            );
            assert_eq!(output.values().null_count(), 2);
            assert!(output.errors().is_empty());
        }
        let output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(6, &[]).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.values().is_empty());
        assert_eq!(
            output.values().data_type().clone(),
            target.data_type().clone()
        );
    }
}

#[test]
fn actual_compiled_value_conversion_round_child_error_is_terminal_not_success_null_under_guard() {
    let integer = FunctionValueType::new(DataType::Int64, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let program = conversion_program(
        &[integer.clone(), decimal.clone()],
        |functions, builder, authors, leaves, source_pools| {
            let rounded = round_value(builder, authors, functions, leaves[1], source_pools);
            let null_test = builder
                .add(
                    None,
                    FunctionValueType::new(DataType::Boolean, false),
                    ExprKind::IsNull {
                        expr: rounded,
                        negated: false,
                    },
                )
                .unwrap();
            // Both branches are actual source values. The condition's ROUND error
            // must terminate the row before either branch or the conversion runs.
            let guarded = add_call(
                builder,
                authors,
                author(
                    functions,
                    "if",
                    vec![
                        argument(FunctionValueType::new(DataType::Boolean, false), None),
                        argument(integer.clone(), None),
                        argument(integer.clone(), None),
                    ],
                    ControlShape::If,
                ),
                vec![null_test, leaves[0], leaves[0]],
            );
            converted(
                functions,
                builder,
                authors,
                guarded,
                integer.clone(),
                largeint_type(),
            )
        },
    );
    let max38 = 10_i128.pow(38) - 1;
    let input = input_batch(
        &program,
        vec![
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(7),
                Some(-9),
                None,
                Some(11),
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(max38), Some(25), Some(max38), None, Some(35)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    );
    let rows = [1, 2, 3, 4];
    let output = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            {
                let selection = Selection::try_sparse(5, &rows).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        wide_values(output.values()),
        vec![Some(7), None, None, Some(11)]
    );
    assert_eq!(
        output
            .errors()
            .iter()
            .map(|error| error.selected_ordinal())
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert!(output.errors()[0].message().contains("overflow"));
}

#[test]
fn actual_compiled_value_conversion_every_selected_callback_preserves_primary_cause_and_failed_latch()
 {
    let program = direct_program(
        largeint_type(),
        FunctionValueType::new(DataType::Int8, true),
    );
    let input = input_batch(
        &program,
        vec![wide(
            &(0..320)
                .map(|row| {
                    if row % 11 == 0 {
                        None
                    } else {
                        Some(i128::from(row))
                    }
                })
                .collect::<Vec<_>>(),
        )],
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.errors().is_empty());
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    for index in 1..=trace.len() {
        for cause in causes("guarded-origin") {
            let mut evaluator = program.instance(&Control);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..index]);
            let retry = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
                    ScalarInvocationActivation::Activated,
                    &retry,
                    &mut PassThroughCallHost
                ),
                Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            assert!(retry.trace().is_empty());
        }
    }
}

#[test]
fn actual_compiled_largeint_decimal_keeps_exact_coefficients_and_frozen_report_error_rows() {
    use arrow::array::{Decimal128Array, Decimal256Array};
    use arrow::datatypes::i256;
    use std::str::FromStr;
    let source = wide(&[Some(i128::MAX), None, Some(i128::MIN), Some(42)]);
    let target = FunctionValueType::new(DataType::Decimal256(41, 2), true);
    let program = direct_program(largeint_type(), target.clone());
    assert_eq!(result_type(&program), target);
    let input = input_batch(&program, vec![source.clone()]);
    let rows = [0, 2, 3];
    let mut evaluator = program.instance(&Control);
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            {
                let selection = Selection::try_sparse(4, &rows).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.errors().is_empty());
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();
    assert_eq!(
        values.values().as_ref(),
        &[
            i256::from_str("17014118346046923173168730371588410572700").unwrap(),
            i256::from_str("-17014118346046923173168730371588410572800").unwrap(),
            i256::from_i128(4200),
        ]
    );
    let target = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let program = direct_program(largeint_type(), target);
    let input = input_batch(&program, vec![source]);
    let mut evaluator = program.instance(&Control);
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    // The original call claim retains ReportError;
    // successful SQL NULL remains distinct from each failed numeric row.
    assert_eq!(
        output
            .errors()
            .iter()
            .map(|error| error.selected_ordinal())
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert!(
        output
            .errors()
            .iter()
            .all(|error| error.message().contains("decimal overflows"))
    );
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![None, None, None, Some(42)]
    );
}

#[test]
fn nominal_largeint_text_actual_compiler_matches_v1_sparse_min_max_null_slice_empty() {
    let target = FunctionValueType::new(DataType::Utf8, true);
    let program = direct_program(largeint_type(), target.clone());
    assert_eq!(result_type(&program), target);
    let raw = wide(&[Some(0), Some(i128::MIN), Some(42), None, Some(i128::MAX)]).slice(1, 4);
    let batch = input_batch(&program, vec![raw.clone()]);
    let rows = [0, 1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let mut evaluator = program.instance(&Control);
    for _ in 0..2 {
        let output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output.values().to_data(),
            // Exact signed base-10 spelling of the declared 128-bit extremes.
            StringArray::from(vec![
                Some("-170141183460469231731687303715884105728"),
                Some("42"),
                None,
                Some("170141183460469231731687303715884105727")
            ])
            .to_data()
        );
    }
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            {
                let selection = Selection::try_sparse(4, &[]).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        output.values().to_data(),
        StringArray::from(Vec::<Option<&str>>::new()).to_data()
    );
}

fn array_common_round_frame_program(convert: bool) -> Fixture {
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    conversion_program(
        std::slice::from_ref(&decimal),
        |functions, builder, authors, leaves, source_pools| {
            let rounded = round_value(builder, authors, functions, leaves[0], source_pools);
            if !convert {
                return rounded;
            }
            array_common_coerced(
                functions,
                builder,
                authors,
                rounded,
                decimal.clone(),
                FunctionValueType::new(DataType::Float64, true),
            )
        },
    )
}

#[test]
fn actual_compiled_array_common_frame_keeps_upstream_round_data_and_original_sparse_rows() {
    let source_program = array_common_round_frame_program(false);
    let common_program = array_common_round_frame_program(true);
    assert_eq!(
        result_type(&source_program),
        FunctionValueType::new(DataType::Decimal128(38, 0), true)
    );
    assert_eq!(
        result_type(&common_program),
        FunctionValueType::new(DataType::Float64, true)
    );
    // Row0 is a real overflow outside the selected domain. Row2 is the only
    // demanded source error; row3 is an original SQL NULL, not a row error.
    let max38 = 10_i128.pow(38) - 1;
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(max38), Some(25), Some(max38), None, Some(35)])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    );
    let original = Arc::clone(&values);
    let source_batch = input_batch(&source_program, vec![Arc::clone(&values)]);
    let common_batch = input_batch(&common_program, vec![Arc::clone(&values)]);
    let rows = [1_usize, 2, 3, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let source_output = source_program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &source_batch,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let common_output = common_program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &common_batch,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(Arc::ptr_eq(&values, &original));
    assert_eq!(source_output.selection().iter().collect::<Vec<_>>(), rows);
    assert_eq!(common_output.selection().iter().collect::<Vec<_>>(), rows);
    assert_eq!(source_output.errors().len(), 1);
    assert_eq!(source_output.errors()[0].selected_ordinal(), 1);
    assert_eq!(
        source_output.errors()[0].reason(),
        novarocks_functions::DataFailureReason::NumericOutOfRange
    );
    assert_eq!(common_output.errors(), source_output.errors());
    let before = source_output
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let after = common_output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(before.value(0), 30);
    assert_eq!(before.value(3), 40);
    assert!(before.is_null(1));
    assert!(before.is_null(2));
    assert_eq!(after.value(0).to_bits(), 30.0_f64.to_bits());
    assert_eq!(after.value(3).to_bits(), 40.0_f64.to_bits());
    assert!(after.is_null(1));
    assert!(after.is_null(2));
    // ROUND's existing producer uses a bounded diagnostic, not a domain Error
    // object. Compare the complete real journal fact; do not invent cross-run
    // pointer identity for causes that do not carry a source object.
    assert!(std::error::Error::source(&source_output.errors()[0]).is_none());
    assert!(std::error::Error::source(&common_output.errors()[0]).is_none());
    // Decimal38 -> Double cannot create this overflow itself. Success here
    // requires Execution's real eager Frame to exclude the source-error row,
    // pass a clean argument to the new family, then restore the original row
    // ordinal/cause. ScalarEvaluationInstance alone must keep rejecting such
    // unresolved input journals with ArgumentRowErrors (KERNEL64).
}
