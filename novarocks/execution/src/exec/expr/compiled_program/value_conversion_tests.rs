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
use arrow::array::{
    ArrayRef, FixedSizeBinaryArray, Float32Array, Int8Array, Int16Array, Int32Array,
    LargeBinaryArray, NullArray, StringArray, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::Field;
use novarocks_functions::builtin::value_conversion::{
    VALUE_CONVERSION_NAME, value_conversion_definition,
};
use novarocks_type_contract::{
    NR_LOGICAL_TYPE_KEY, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
    ValueTypeStorageLifetime,
};

fn conversion_catalogue() -> PureEngineFunctionCatalog {
    let actual = catalogue(Shape::Decimal);
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut manifest = Vec::new();
    for (name, overloads, abi) in [
        (
            "if",
            &["(bool,any<T>,any<T>)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        (
            "coalesce",
            &["(any<T>...)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        ("round", &["dynamic-v1"][..], PureKernelAbi::ScalarV1),
    ] {
        builder
            .register(
                actual
                    .metadata()
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        for overload in overloads {
            manifest.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.scalar/{name}/{overload}"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi,
                },
                aggregate_state_format: None,
            });
        }
    }
    builder
        .register(value_conversion_definition().unwrap())
        .unwrap();
    // This independent installed manifest covers all nine real implementations
    // of the original hidden definition, without claiming a Server catalogue.
    for stem in [
        "json_text_same_structure",
        "opaque_to_binary_same_structure",
        "signed_to_largeint",
        "largeint_to_signed_null_overflow",
        "largeint_to_decimal_exact",
        "largeint_to_float_round",
        "largeint_to_utf8_text",
        "utf8_to_largeint_null_invalid",
        "null_to_typed_nullable",
    ] {
        manifest.push(InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/value_domain_conversion/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(format!(
                    "builtin.scalar/value_domain_conversion/{stem}/v1"
                ))
                .unwrap(),
                implementation: PureImplementationId::try_new(format!(
                    "builtin.scalar/value_domain_conversion/{stem}/selected-v1"
                ))
                .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        });
    }
    builder.seal_pure(manifest).unwrap()
}

fn nominal(carrier: DataType, domain: ValueLogicalType) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(carrier, true, domain).unwrap()
}
fn largeint_type() -> FunctionValueType {
    nominal(DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt)
}

// Only source-layout construction is local. The original shared flow author,
// FrozenPhysicalCalls, package validator and compiler author every invocation.
fn conversion_program(
    columns: &[FunctionValueType],
    build: impl FnOnce(
        &PureEngineFunctionCatalog,
        &mut FragmentBuilder,
        &mut BTreeMap<ExprId, Author>,
        &[ExprId],
    ) -> ExprId,
) -> Arc<LocalProgram> {
    let functions = conversion_catalogue();
    let id = FragmentId::new(229);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let mut builder = FragmentBuilder::new(id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = Vec::new();
    let mut values = Vec::new();
    for (ordinal, ty) in columns.iter().enumerate() {
        assert!(ty.nullable());
        let expr = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        let value = ValueId::new(901 + u32::try_from(ordinal).unwrap() * 71);
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr { node: input, expr },
            })
            .unwrap();
        items.push((expr, value));
        values.push(value);
    }
    builder
        .add_project(
            input,
            source,
            items.into_boxed_slice(),
            values.clone().into_boxed_slice(),
        )
        .unwrap();
    let leaves = columns
        .iter()
        .zip(values)
        .map(|(ty, value)| {
            builder
                .add_expression(output, ty.clone(), ExprKind::Value(value))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut authors = BTreeMap::new();
    let expression = build(&functions, &mut builder, &mut authors, &leaves);
    let result_type = builder.expressions().get(expression).unwrap().ty.clone();
    let value = builder
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
            Box::from([(expression, value)]),
            Box::from([value]),
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
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: id,
        output: fragment.nodes()[&output].output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "conversion_result".into(),
            alias: None,
            value,
            ty: result_type,
        }]),
    };
    compile_checked_fragment(&functions, fragment, &authors, result)
}

fn converted(
    functions: &PureEngineFunctionCatalog,
    builder: &mut FragmentBuilder,
    authors: &mut BTreeMap<ExprId, Author>,
    value: ExprId,
    source: FunctionValueType,
    target: FunctionValueType,
) -> ExprId {
    call(
        builder,
        authors,
        trusted_author(
            functions,
            VALUE_CONVERSION_NAME,
            vec![argument(source, None)],
            ControlShape::Eager,
            target,
        ),
        vec![value],
    )
}
fn direct_program(source: FunctionValueType, target: FunctionValueType) -> Arc<LocalProgram> {
    conversion_program(
        std::slice::from_ref(&source),
        |functions, builder, authors, leaves| {
            converted(
                functions,
                builder,
                authors,
                leaves[0],
                source.clone(),
                target,
            )
        },
    )
}
fn input_batch(program: &LocalProgram, arrays: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        arrays,
    )
    .unwrap()
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
fn result_type(program: &LocalProgram) -> FunctionValueType {
    FunctionValueType::try_from_typed_binding(
        program.graph().nodes()[2]
            .output_layout()
            .value_binding_at(0)
            .expect("complete frozen output occurrence")
            .clone(),
    )
    .unwrap()
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
        assert_eq!(result_type(&program), largeint_type());
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
        let mut evaluator = instance(&program);
        for _ in 0..2 {
            let output = evaluator.evaluate(&input, selection, &Control).unwrap();
            assert_eq!(output.selection().iter().collect::<Vec<_>>(), rows);
            assert_eq!(
                wide_values(output.values()),
                vec![Some(i128::from(min)), None, Some(i128::from(max)), Some(0)]
            );
            assert!(output.errors().is_empty());
        }
        let empty = evaluator
            .evaluate(&input, Selection::try_sparse(5, &[]).unwrap(), &Control)
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
                |functions, builder, authors, leaves| {
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
                            .add_expression(
                                NodeId::new(0),
                                FunctionValueType::new(DataType::Boolean, false),
                                ExprKind::IsNull {
                                    expr: narrowed,
                                    negated: false,
                                },
                            )
                            .unwrap()
                    } else {
                        call(
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
            let output = instance(&program)
                .evaluate(&input, Selection::try_sparse(6, &rows).unwrap(), &Control)
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
        let output = instance(&program)
            .evaluate(&input, Selection::try_sparse(6, &rows).unwrap(), &Control)
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
    let output = instance(&program)
        .evaluate(&input, Selection::try_sparse(3, &rows).unwrap(), &Control)
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
    let normalized = crate::exec::chunk::normalize_execution_array(&original).unwrap();
    let input = input_batch(&program, vec![normalized.slice(1, 4)]);
    let rows = [0, 1, 3];
    let output = instance(&program)
        .evaluate(&input, Selection::try_sparse(4, &rows).unwrap(), &Control)
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
            .all(|field| field.metadata().capacity() == 0)
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
        let mut evaluator = instance(&program);
        let rows = [1, 4];
        for _ in 0..2 {
            let output = evaluator
                .evaluate(&input, Selection::try_sparse(6, &rows).unwrap(), &Control)
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
            .evaluate(&input, Selection::try_sparse(6, &[]).unwrap(), &Control)
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
        |functions, builder, authors, leaves| {
            let scalar_integer = FunctionValueType::new(DataType::Int64, false);
            let digits = builder
                .add_expression(
                    NodeId::new(0),
                    scalar_integer.clone(),
                    ExprKind::Literal(LiteralValue::Int64(-1)),
                )
                .unwrap();
            let rounded = call(
                builder,
                authors,
                author(
                    functions,
                    "round",
                    vec![
                        argument(decimal.clone(), None),
                        integer_argument(scalar_integer, -1),
                    ],
                    ControlShape::Eager,
                ),
                vec![leaves[1], digits],
            );
            let null_test = builder
                .add_expression(
                    NodeId::new(0),
                    FunctionValueType::new(DataType::Boolean, false),
                    ExprKind::IsNull {
                        expr: rounded,
                        negated: false,
                    },
                )
                .unwrap();
            // Both branches are actual source values. The condition's ROUND error
            // must terminate the row before either branch or the conversion runs.
            let guarded = call(
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
    let output = instance(&program)
        .evaluate(&input, Selection::try_sparse(5, &rows).unwrap(), &Control)
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
    let output = instance(&program)
        .evaluate(&input, Selection::all(320), &recorder)
        .unwrap();
    assert!(output.errors().is_empty());
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(evaluator.evaluate(&input, Selection::all(320), &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            let retry = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(&input, Selection::all(320), &retry),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(retry.trace.lock().unwrap().is_empty());
        }
    }
}

#[path = "value_conversion_largeint_text_tests.rs"]
mod largeint_text_tests;

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
    let mut evaluator = instance(&program);
    let output = evaluator
        .evaluate(&input, Selection::try_sparse(4, &rows).unwrap(), &Control)
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
    let mut evaluator = instance(&program);
    let output = evaluator
        .evaluate(&input, Selection::all(4), &Control)
        .unwrap();
    // This fixture authors and freezes ReportError in compile_checked_fragment;
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
