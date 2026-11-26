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

//! Public text fixtures exercise node typing, compiled input shapes and values.

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    },
    datatypes::{DataType, Schema, i256},
    record_batch::RecordBatch,
};
use novarocks_expression::{
    testkit::{
        HarnessControl, constant_policy,
        expression_text::{
            CallInapplicability, CheckedRows, Consistency, EvaluationShape, Inapplicability,
            TextColumn, TextContext, TextError, TypedText, type_expression_text,
        },
    },
    typing::{TypingError, temporal_function_value_type},
};
use novarocks_expression_contract::{ExprKind, ValueId};
use novarocks_functions::{DataFailureReason, largeint};
use novarocks_type_contract::{
    AssignmentCommonTypeError, DecimalOverflowPolicy, FunctionValueType, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameterValue, SemanticParameters,
    TemporalPrecision, TemporalType, ValueLogicalType,
};
use std::sync::Arc;

const CONSTANT_SHAPE: EvaluationShape = EvaluationShape {
    port_count: 0,
    input_columns: 0,
    input_rows: 1,
};
const ALLOW_THROW: SemanticParameterRef = SemanticParameterRef {
    id: SemanticParameterId::new(0),
    expected_key: SemanticParameterKey::AllowThrowException,
};

fn fixture() -> Arc<novarocks_functions::PureEngineFunctionCatalog> {
    novarocks_functions::testkit::shared_installed_builtin_owner_catalogue()
}

fn parameters() -> SemanticParameters {
    SemanticParameters::try_new([(
        ALLOW_THROW.id,
        SemanticParameterValue::AllowThrowException(true),
    )])
    .unwrap()
}

fn context<'a>(
    functions: &'a novarocks_functions::PureEngineFunctionCatalog,
    parameters: &'a SemanticParameters,
) -> TextContext<'a> {
    TextContext {
        functions,
        constant_policy: constant_policy(),
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        parameters,
        allow_throw_exception: ALLOW_THROW,
    }
}

fn column(name: &str, id: u32, carrier: DataType, nullable: bool) -> TextColumn {
    TextColumn {
        name: name.into(),
        value: ValueId::new(id),
        value_type: FunctionValueType::new(carrier, nullable),
    }
}

fn batch(columns: &[TextColumn], values: Vec<ArrayRef>) -> RecordBatch {
    if columns.is_empty() {
        return RecordBatch::new_empty(Arc::new(Schema::empty()));
    }
    let fields = columns
        .iter()
        .map(|column| column.value_type.try_to_field(&column.name).unwrap())
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), values).unwrap()
}

fn compile<'a>(text: &str, columns: &[TextColumn], context: &TextContext<'a>) -> TypedText<'a> {
    type_expression_text(text, columns, context, &HarnessControl)
        .unwrap_or_else(|error| panic!("{text}: {error:?}"))
}

fn evaluate(expr: &TypedText<'_>, batch: &RecordBatch) -> CheckedRows {
    expr.check_rows(batch, &HarnessControl, &HarnessControl)
        .unwrap()
}

#[test]
fn collection_lambda_types_captures_and_replays_each_original_parent() {
    use arrow::{array::ListArray, datatypes::Int32Type};

    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let arrays = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
    ]);
    let columns = [
        column("items", 1, arrays.data_type().clone(), true),
        column("captured", 2, DataType::Int32, false),
        column("unused", 3, DataType::Int32, false),
    ];
    let data = batch(
        &columns,
        vec![
            Arc::new(arrays),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(Int32Array::from(vec![4, 5, 6])),
        ],
    );
    let typed = compile(
        "transform(items, x -> transform(items, y -> x + y + captured))",
        &columns,
        &context,
    );
    let [port] = typed.unit().ports() else {
        panic!("one input port");
    };
    assert_eq!(
        port.values.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [columns[0].value, columns[1].value],
    );
    let outcome = evaluate(&typed, &data);
    let Consistency::Compared { constant_shapes } = outcome.consistency else {
        panic!("immutable lambda bodies support row consistency");
    };
    assert_eq!(constant_shapes, vec![CONSTANT_SHAPE; 3]);
    assert!(outcome.batch.errors.is_empty());
    let rows = outcome
        .batch
        .values
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(rows.is_null(1));
    let values = |row| {
        let outer = rows.value(row);
        let inner = outer.as_any().downcast_ref::<ListArray>().unwrap();
        (0..inner.len())
            .map(|index| {
                let values = inner.value(index);
                values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(values(0), [vec![12, 13], vec![13, 14]]);
    assert_eq!(values(2), [vec![36]]);
}

#[test]
fn collection_lambda_rejects_unknown_or_non_boolean_predicate_body() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [column(
        "items",
        1,
        DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int32,
            true,
        ))),
        false,
    )];
    for body in ["NULL", "x", "BIGINT '0'"] {
        let result = type_expression_text(
            &format!("filter(items, x -> {body})"),
            &columns,
            &context,
            &HarnessControl,
        );
        assert!(
            matches!(
                result,
                Err(TextError::Typing {
                    cause: TypingError::Binding(
                        novarocks_functions::FunctionBindingError::NoMatchingOverload
                    ),
                    ..
                })
            ),
            "{body}"
        );
    }
}

fn assert_compared(result: &CheckedRows, columns: usize, rows: usize) {
    assert_eq!(
        result.batch.shape,
        EvaluationShape {
            port_count: 1,
            input_columns: columns,
            input_rows: rows
        }
    );
    let Consistency::Compared { constant_shapes } = &result.consistency else {
        panic!("expected comparison, got {:?}", result.consistency);
    };
    assert_eq!(constant_shapes, &vec![CONSTANT_SHAPE; rows]);
}

fn constant(text: &str, context: &TextContext<'_>) -> CheckedRows {
    let unit = compile(text, &[], context);
    assert!(unit.unit().ports().is_empty());
    let result = evaluate(&unit, &batch(&[], vec![]));
    assert_eq!(result.batch.shape, CONSTANT_SHAPE);
    assert!(result.batch.errors.is_empty());
    assert!(matches!(
        result.consistency,
        Consistency::NotApplicable(Inapplicability::NoInputColumns)
    ));
    result
}

#[test]
fn shared_call_keeps_port_order_and_repeated_references() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [
        column("x", 11, DataType::Int32, false),
        column("unused", 12, DataType::Boolean, false),
        column("y", 14, DataType::Int32, true),
    ];
    let typed = compile("COALESCE(y, NULLIF(x, x), abs(x))", &columns, &context);
    let [port] = typed.unit().ports() else {
        panic!("one ordered port");
    };
    assert_eq!(
        port.values.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [ValueId::new(11), ValueId::new(14)],
    );
    let occurrences = typed
        .unit()
        .definitions()
        .iter()
        .filter_map(|definition| match definition.kind {
            ExprKind::Value(id) => Some(id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        occurrences,
        [
            ValueId::new(14),
            ValueId::new(11),
            ValueId::new(11),
            ValueId::new(11)
        ]
    );
    let result = evaluate(
        &typed,
        &batch(
            &columns,
            vec![
                Arc::new(Int32Array::from(vec![-8, 3, -5])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
                Arc::new(Int32Array::from(vec![None, Some(7), None])),
            ],
        ),
    );
    assert_eq!(
        result.batch.shape,
        EvaluationShape {
            port_count: 1,
            input_columns: 2,
            input_rows: 3,
        }
    );
    assert!(matches!(
        result.consistency,
        Consistency::NotApplicable(Inapplicability::Call {
            function,
            reason: CallInapplicability::WholeInvocation,
        }) if function.as_str() == "builtin.scalar/nullif/v1"
    ));
    assert!(result.batch.errors.is_empty());
    assert_eq!(
        result.batch.values.to_data(),
        Int32Array::from(vec![8, 7, 5]).to_data()
    );
}

#[test]
fn logical_and_case_structures_preserve_values_and_nulls() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [
        column("b", 101, DataType::Boolean, true),
        column("x", 103, DataType::Int64, true),
    ];
    let rows = batch(
        &columns,
        vec![
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            Arc::new(Int64Array::from(vec![Some(-3), Some(11), None, None])),
        ],
    );
    let typed = compile(
        "CASE WHEN b AND (x IS NOT NULL AND TRUE) THEN -x WHEN b OR NULL THEN -2 ELSE 4 END",
        &columns,
        &context,
    );
    assert!(
        typed
            .unit()
            .definitions()
            .iter()
            .any(|node| matches!(&node.kind, ExprKind::Conjunction { args } if args.len() == 3))
    );
    let result = evaluate(&typed, &rows);
    assert_compared(&result, 2, 4);
    assert!(result.batch.errors.is_empty());
    assert_eq!(result.batch.result_type.data_type(), &DataType::Int64);
    assert_eq!(
        result.batch.values.to_data(),
        Int64Array::from(vec![3, 4, 4, -2]).to_data()
    );
    assert!(constant("NULL AND TRUE", &context).batch.values.is_null(0));
    assert!(
        constant("CASE WHEN NULL THEN 1 END", &context)
            .batch
            .values
            .is_null(0)
    );
    assert_eq!(
        constant("NULL IS NULL", &context).batch.values.to_data(),
        BooleanArray::from(vec![true]).to_data()
    );
}

#[test]
fn dedicated_extract_syntax_uses_its_catalogue_binding() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let source = "EXTRACT(YEAR FROM DATE '2025-09-02')";
    let typed = compile(source, &[], &context);
    assert!(matches!(
        &typed.unit().definitions().last().unwrap().kind,
        ExprKind::FunctionCall { function, .. }
            if function.function_id.as_str() == "builtin.scalar/year/v1"
    ));
    let result = constant(source, &context);
    assert_eq!(
        result.batch.values.to_data(),
        Int64Array::from(vec![2025]).to_data()
    );
}

#[test]
fn literal_types_and_cast_parameters_follow_the_checked_unit() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let cases: &[(&str, ArrayRef)] = &[
        ("'hello'", Arc::new(StringArray::from(vec!["hello"]))),
        (
            "X'036AFF'",
            Arc::new(BinaryArray::from(vec![&[3_u8, 106, 255][..]])),
        ),
        ("DATE '1970-01-03'", Arc::new(Date32Array::from(vec![2]))),
        (
            "12.30",
            Arc::new(
                Decimal128Array::from(vec![1230])
                    .with_precision_and_scale(4, 2)
                    .unwrap(),
            ),
        ),
        ("1.5E1", Arc::new(Float64Array::from(vec![15.0]))),
        (
            "2147483648",
            Arc::new(Int64Array::from(vec![2_147_483_648])),
        ),
    ];
    for (source, expected) in cases {
        let result = constant(source, &context);
        assert_eq!(
            result.batch.result_type.data_type(),
            expected.data_type(),
            "{source}"
        );
        assert!(!result.batch.result_type.nullable(), "{source}");
        assert_eq!(
            result.batch.values.to_data(),
            expected.to_data(),
            "{source}"
        );
    }
    let precision = TemporalPrecision::WHOLE_SECONDS;
    let temporal: &[(&str, TemporalType, ArrayRef)] = &[
        (
            "TIME '01:02:03'",
            TemporalType::Time(precision),
            Arc::new(Int64Array::from(vec![3_723_000_000_000_000])),
        ),
        (
            "TIMESTAMP '1970-01-02 00:00:00'",
            TemporalType::Timestamp(precision),
            Arc::new(TimestampMicrosecondArray::from(vec![86_400_000_000])),
        ),
    ];
    for (source, declaration, expected) in temporal {
        let typed = compile(source, &[], &context);
        let unit = typed.unit();
        let root = unit.definition(unit.roots()[0].definition).unwrap();
        let expected_type = temporal_function_value_type(*declaration, false).unwrap();
        assert_eq!(root.ty, expected_type, "{source}");
        assert!(matches!(root.kind, ExprKind::Constant(_)), "{source}");
        assert!(
            typed.environment().parameters.entries().is_empty(),
            "{source}"
        );

        let result = evaluate(&typed, &batch(&[], vec![]));
        assert_eq!(result.batch.shape, CONSTANT_SHAPE, "{source}");
        assert_eq!(result.batch.result_type, expected_type, "{source}");
        assert!(result.batch.errors.is_empty(), "{source}");
        assert_eq!(
            result.batch.values.to_data(),
            expected.to_data(),
            "{source}"
        );
    }
    let source = "CASE WHEN TRUE THEN 2147483648 ELSE 1 END";
    let typed = compile(source, &[], &context);
    assert!(typed.unit().definitions().iter().any(|definition| matches!(
        definition.kind,
        ExprKind::Cast {
            cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
            ..
        }
    )));
    assert!(typed.environment().parameters.require(ALLOW_THROW).is_err());
    assert_eq!(
        constant(source, &context).batch.values.to_data(),
        Int64Array::from(vec![2_147_483_648]).to_data(),
    );
}

#[test]
fn parser_column_migration_and_type_errors_keep_their_identity() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [column("x", 22, DataType::Utf8, false)];
    let type_text = |text| type_expression_text(text, &columns, &context, &HarnessControl);
    assert!(matches!(type_text("abs("), Err(TextError::Parse(_))));
    assert!(matches!(
        type_text("abs(unknown)"),
        Err(TextError::UnknownColumn { name, .. }) if name == "unknown"
    ));
    assert!(matches!(
        type_text("count(x)"),
        Err(TextError::NotTypedByLayer { construct, .. }) if construct == "count"
    ));
    assert!(matches!(
        type_text("x.y"),
        Err(TextError::Typing {
            cause: novarocks_expression::typing::TypingError::InvalidShape(
                "Field access requires a ROW value"
            ),
            ..
        })
    ));
    let conditional = type_text("CASE WHEN TRUE THEN x ELSE x END").unwrap();
    let values = Arc::new(StringArray::from(vec!["original value"]));
    let result = evaluate(&conditional, &batch(&columns, vec![values.clone()]));
    assert_eq!(
        result.batch.result_type,
        columns[0].value_type.with_nullable(true).unwrap()
    );
    assert_eq!(result.batch.values.to_data(), values.to_data());

    let trim = type_text("TRIM(x)").unwrap();
    let values = Arc::new(StringArray::from(vec!["  original value  "]));
    assert_eq!(
        evaluate(&trim, &batch(&columns, vec![values]))
            .batch
            .values
            .to_data(),
        StringArray::from(vec!["original value"]).to_data()
    );

    let cast = type_text("CAST(x AS BIGINT)").unwrap();
    let root = cast
        .unit()
        .definition(cast.unit().roots()[0].definition)
        .unwrap();
    assert_eq!(root.ty, FunctionValueType::new(DataType::Int64, false));
    assert!(matches!(
        type_text("CAST(x AS LARGEINT)"),
        Err(TextError::Typing {
            cause: TypingError::TypeMismatch(_),
            ..
        })
    ));
    assert!(matches!(
        type_text("TRIM('a' FROM x)"),
        Err(TextError::Typing {
            cause: TypingError::Binding(
                novarocks_functions::FunctionBindingError::UnavailableImplementation(overload)
            ),
            ..
        }) if overload.as_str() == "builtin.scalar/trim/trino-varchar-codepoints-v1"
    ));
    for source in [
        "-NULL",
        "NULL + NULL",
        "NULL - NULL",
        "NULL * NULL",
        "NULL / NULL",
        "NULL / 2e0",
    ] {
        assert!(
            matches!(
                type_text(source),
                Err(TextError::Typing {
                    cause: TypingError::Binding(
                        novarocks_functions::FunctionBindingError::AmbiguousOverload
                    ),
                    ..
                })
            ),
            "{source}"
        );
    }
    for (text, prefix) in [
        ("+NULL", "unary plus requires"),
        ("+DECIMAL '1.25'", "unary plus requires"),
        ("TRUE + 1", "operator + does not accept"),
        ("1 AND TRUE", "AND requires"),
        ("CASE WHEN 3 THEN 4 END", "CASE WHEN requires"),
    ] {
        assert!(
            matches!(
                type_text(text),
                Err(TextError::Typing { cause: TypingError::TypeMismatch(message), .. }) if message.starts_with(prefix)
            ),
            "{text}"
        );
    }
    let mixed_coalesce = type_text("COALESCE(1, 'text')").err();
    assert!(
        matches!(
            mixed_coalesce,
            Some(TextError::Typing {
                cause: TypingError::CommonType(AssignmentCommonTypeError::IncompatibleShape(
                    "expressions do not have a common Trino type"
                )),
                ..
            })
        ),
        "{mixed_coalesce:?}"
    );
}

#[test]
fn two_argument_if_preserves_the_selected_type_and_null_branch() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [
        column("condition", 1, DataType::Boolean, true),
        column("value", 2, DataType::Int32, false),
    ];
    let typed = compile("IF(condition, value)", &columns, &context);
    let input = batch(
        &columns,
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(Int32Array::from(vec![7, 8, 9])),
        ],
    );
    let result = evaluate(&typed, &input);
    assert!(result.batch.errors.is_empty());
    assert_eq!(
        result.batch.result_type,
        FunctionValueType::new(DataType::Int32, true)
    );
    assert_eq!(
        result.batch.values.to_data(),
        Int32Array::from(vec![Some(7), None, None]).to_data()
    );
}

#[test]
fn uuid_literals_and_explicit_casts_keep_the_exact_nominal_bytes() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let literal = "UUID 'F24F9B64-81FA-49D1-B74E-8C09A6E31C56'";
    let uuid = constant(literal, &context);
    assert!(uuid.batch.errors.is_empty());
    assert_eq!(
        uuid.batch.result_type.logical_type(),
        ValueLogicalType::Uuid
    );
    assert_eq!(
        uuid.batch.result_type.data_type(),
        &DataType::FixedSizeBinary(16)
    );
    let text = constant(&format!("CAST({literal} AS VARCHAR)"), &context);
    assert_eq!(
        text.batch.values.to_data(),
        StringArray::from(vec!["f24f9b64-81fa-49d1-b74e-8c09a6e31c56"]).to_data()
    );
    for sql in [
        format!("CAST(CAST({literal} AS VARBINARY) AS UUID)"),
        format!("CAST(CAST({literal} AS VARCHAR) AS UUID)"),
    ] {
        let roundtrip = constant(&sql, &context);
        assert!(roundtrip.batch.errors.is_empty(), "{sql}");
        assert_eq!(roundtrip.batch.result_type, uuid.batch.result_type);
        assert_eq!(
            roundtrip.batch.values.to_data(),
            uuid.batch.values.to_data()
        );
    }
    for invalid in [
        format!("CAST({literal} AS VARCHAR(35))"),
        "CAST(X'0001' AS UUID)".into(),
        "CAST('f24f9b6481fa49d1b74e8c09a6e31c56' AS UUID)".into(),
    ] {
        let result = evaluate(&compile(&invalid, &[], &context), &batch(&[], vec![]));
        let [error] = result.batch.errors.as_ref() else {
            panic!("expected one row error from {invalid}");
        };
        assert_eq!(
            error.reason(),
            DataFailureReason::Domain(
                novarocks_failure_contract::kernel_reason::INVALID_CAST_ARGUMENT
            )
        );
    }
}

#[test]
fn nullif_preserves_left_type_and_demands_only_present_rows() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [
        column("x", 1, DataType::Int32, true),
        column("d", 2, DataType::Int64, true),
    ];
    let input = batch(
        &columns,
        vec![
            Arc::new(Int32Array::from(vec![
                None,
                Some(7),
                Some(5),
                Some(7),
                Some(9),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(0),
                Some(0),
                Some(2),
                Some(2),
                None,
            ])),
        ],
    );
    let checked = evaluate(&compile("NULLIF(x, 10 / d)", &columns, &context), &input);
    assert_eq!(
        checked.batch.result_type,
        FunctionValueType::new(DataType::Int32, true)
    );
    assert_eq!(
        checked.batch.values.to_data(),
        Int32Array::from(vec![None, None, None, Some(7), Some(9)]).to_data()
    );
    let [error] = checked.batch.errors.as_ref() else {
        panic!("only the present left row with zero divisor fails");
    };
    assert_eq!(error.selected_ordinal(), 1);
    assert_eq!(error.reason(), DataFailureReason::DivisionByZero);
    let caught = evaluate(
        &compile("TRY(NULLIF(x, 10 / d))", &columns, &context),
        &input,
    );
    assert!(caught.batch.errors.is_empty());
    assert_eq!(
        caught.batch.values.to_data(),
        checked.batch.values.to_data()
    );
}

#[test]
fn nullif_evaluates_its_left_once_per_demanded_row() {
    use novarocks_functions::{KernelEvaluationControl, KernelFailure};
    use std::{sync::Mutex, time::Duration};

    #[derive(Default)]
    struct Waits(Mutex<Vec<Duration>>);

    impl KernelEvaluationControl for Waits {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }

        fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
            self.0.lock().unwrap().push(duration);
            Ok(())
        }
    }

    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [column("d", 1, DataType::Int64, true)];
    let input = batch(
        &columns,
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))],
    );
    let typed = compile("NULLIF(sleep(d), FALSE)", &columns, &context);
    let control = Waits::default();
    let checked = typed.check_rows(&input, &HarnessControl, &control).unwrap();
    assert!(checked.batch.errors.is_empty());
    assert_eq!(
        checked.batch.values.to_data(),
        BooleanArray::from(vec![Some(true), None, Some(true)]).to_data()
    );
    assert_eq!(
        control.0.into_inner().unwrap(),
        [Duration::from_secs(1), Duration::from_secs(2)]
    );
}

#[test]
fn nullif_nested_unknown_keeps_the_original_array() {
    use arrow::array::{ListArray, types};

    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let left: ArrayRef = Arc::new(ListArray::from_iter_primitive::<types::Int32Type, _, _>([
        Some(vec![Some(1), None]),
    ]));
    let right: ArrayRef = Arc::new(ListArray::from_iter_primitive::<types::Int64Type, _, _>([
        Some(vec![Some(1), None]),
    ]));
    let columns = [
        column("x", 1, left.data_type().clone(), false),
        column("y", 2, right.data_type().clone(), false),
    ];
    let input = batch(&columns, vec![left.clone(), right]);
    let checked = evaluate(&compile("NULLIF(x, y)", &columns, &context), &input);
    assert!(checked.batch.errors.is_empty());
    assert_eq!(checked.batch.values.to_data(), left.to_data());
}

#[test]
fn extrema_keep_selected_values_with_exact_width_and_nested_null_order() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let widest = constant(
        "greatest(BIGINT '9007199254740993', 9007199254740992)",
        &context,
    );
    assert_eq!(
        widest.batch.values.to_data(),
        Int64Array::from(vec![9007199254740993]).to_data()
    );
    let labels = [
        ("greatest(DOUBLE 'NaN', DOUBLE 'Infinity')", f64::INFINITY),
        ("least(DOUBLE 'NaN', DOUBLE '-Infinity')", f64::NEG_INFINITY),
        ("greatest(DOUBLE '-0.0', DOUBLE '0.0')", -0.0),
        ("least(DOUBLE '0.0', DOUBLE '-0.0')", 0.0),
    ];
    for (text, expected) in labels {
        let values = constant(text, &context).batch.values;
        let floats = values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(floats.value(0).to_bits(), expected.to_bits(), "{text}");
    }
    let values = constant("least(DOUBLE 'NaN', DOUBLE 'NaN')", &context)
        .batch
        .values;
    assert!(
        values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .is_nan()
    );

    let null = constant("greatest(NULL, NULL)", &context);
    assert_eq!(null.batch.values.data_type(), &DataType::Null);
    let fields = arrow::datatypes::Fields::from(vec![
        arrow::datatypes::Field::new("a", DataType::Float64, true),
        arrow::datatypes::Field::new("b", DataType::Int32, true),
    ]);
    let left = arrow::array::StructArray::new(
        fields.clone(),
        vec![
            Arc::new(Float64Array::from(vec![None])),
            Arc::new(Int32Array::from(vec![2])),
        ],
        None,
    );
    let right = arrow::array::StructArray::new(
        fields.clone(),
        vec![
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Int32Array::from(vec![3])),
        ],
        None,
    );
    let columns = [
        column("left_row", 1, DataType::Struct(fields.clone()), false),
        column("right_row", 2, DataType::Struct(fields), false),
    ];
    let expected = right.to_data();
    let input = batch(&columns, vec![Arc::new(left), Arc::new(right)]);
    let row = compile("least(left_row, right_row)", &columns, &context);
    assert_eq!(evaluate(&row, &input).batch.values.to_data(), expected);

    let arrays =
        arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Float64Type, _, _>([
            Some(vec![Some(f64::NAN)]),
            Some(vec![None]),
            None,
            Some(vec![Some(2.0)]),
        ]);
    let other =
        arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Float64Type, _, _>([
            Some(vec![Some(1.0)]),
            Some(vec![Some(1.0)]),
            Some(vec![Some(1.0)]),
            Some(vec![Some(1.0)]),
        ]);
    let columns = [
        column("left_value", 1, arrays.data_type().clone(), true),
        column("right_value", 2, other.data_type().clone(), true),
    ];
    let input = batch(&columns, vec![Arc::new(arrays), Arc::new(other)]);
    for (name, last) in [("greatest", 2.0), ("least", 1.0)] {
        let typed = compile(
            &format!("{name}(left_value, right_value)"),
            &columns,
            &context,
        );
        let result = evaluate(&typed, &input);
        assert!(result.batch.errors.is_empty());
        let expected =
            arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Float64Type, _, _>([
                Some(vec![Some(1.0)]),
                Some(vec![Some(1.0)]),
                None,
                Some(vec![Some(last)]),
            ]);
        assert_eq!(result.batch.values.to_data(), expected.to_data(), "{name}");
    }
}

#[test]
fn typeof_describes_exact_types_and_demands_the_selected_argument_rows() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    for (sql, expected) in [
        ("typeof(NULL)", "unknown"),
        ("typeof(INTEGER '7')", "integer"),
        ("typeof(REAL '1.5')", "real"),
        ("typeof('é中')", "varchar(2)"),
        ("typeof(CAST(NULL AS DECIMAL(5,2)))", "decimal(5,2)"),
        ("typeof(CAST(NULL AS TIMESTAMP(12)))", "timestamp(12)"),
        (
            "typeof(CAST(NULL AS ROW(a INTEGER, \"B\" VARCHAR(2))))",
            "row(\"a\" integer, \"B\" varchar(2))",
        ),
        (
            "typeof(CAST(NULL AS MAP(VARCHAR(3), ARRAY(DECIMAL(5,2)))))",
            "map(varchar(3), array(decimal(5,2)))",
        ),
        ("typeof(JSON 'null')", "json"),
    ] {
        let result = constant(sql, &context);
        assert_eq!(
            result.batch.result_type,
            FunctionValueType::new(DataType::Utf8, false)
        );
        assert_eq!(
            result.batch.values.to_data(),
            StringArray::from(vec![expected]).to_data(),
            "{sql}"
        );
    }

    let columns = [column("denominator", 1, DataType::Int32, true)];
    let input = batch(
        &columns,
        vec![Arc::new(Int32Array::from(vec![Some(2), Some(0), None]))],
    );
    let typed = compile("typeof(10 / denominator)", &columns, &context);
    let result = evaluate(&typed, &input);
    let [error] = result.batch.errors.as_ref() else {
        panic!("expected the original failure on the zero row");
    };
    assert_eq!(error.selected_ordinal(), 1);
    assert_eq!(error.reason(), DataFailureReason::DivisionByZero);
    assert_eq!(
        result.batch.values.to_data(),
        StringArray::from(vec![Some("integer"), None, Some("integer")]).to_data()
    );
}

#[test]
fn trino_json_literal_validation_precedes_try_conversion() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    for text in [
        "TRY_CAST(JSON '[1' AS VARCHAR)",
        "TRY(CAST(JSON '1e309' AS VARCHAR))",
    ] {
        assert!(
            matches!(
                type_expression_text(text, &[], &context, &HarnessControl),
                Err(TextError::Typing {
                    cause: TypingError::InvalidLiteral(_),
                    ..
                })
            ),
            "{text}"
        );
    }
    let converted = constant("TRY_CAST(JSON '1e2' AS VARCHAR)", &context);
    assert!(converted.batch.errors.is_empty());
    assert_eq!(
        converted.batch.result_type,
        FunctionValueType::new(DataType::Utf8, true)
    );
    assert_eq!(
        converted.batch.values.to_data(),
        StringArray::from(vec!["1.0E2"]).to_data()
    );
}

#[test]
fn negation_compares_nullable_integer_minima_at_the_original_row() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    macro_rules! check {
        ($array:ty, $native:ty, $carrier:expr, $name:literal) => {{
            let columns = [column("x", 4, $carrier, true)];
            let typed = compile("-x", &columns, &context);
            let input = Arc::new(<$array>::from(vec![
                Some(7),
                None,
                Some(<$native>::MIN),
                Some(-5),
            ]));
            let result = evaluate(&typed, &batch(&columns, vec![input]));
            assert_compared(&result, 1, 4);
            assert_eq!(result.batch.result_type, columns[0].value_type);
            assert_eq!(
                result.batch.values.to_data(),
                <$array>::from(vec![Some(-7), None, None, Some(5)]).to_data()
            );
            assert_overflow(&result, $name, <$native>::MIN);
        }};
    }
    check!(Int8Array, i8, DataType::Int8, "tinyint");
    check!(Int16Array, i16, DataType::Int16, "smallint");
    check!(Int32Array, i32, DataType::Int32, "integer");
    check!(Int64Array, i64, DataType::Int64, "bigint");

    let mut large = column("x", 23, DataType::FixedSizeBinary(16), true);
    large.value_type = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let columns = [large];
    let typed = compile("-x", &columns, &context);
    let result = evaluate(
        &typed,
        &batch(
            &columns,
            vec![largeint::array_from_i128(&[Some(7), None, Some(i128::MIN), Some(-5)]).unwrap()],
        ),
    );
    assert_compared(&result, 1, 4);
    assert_eq!(result.batch.result_type, columns[0].value_type);
    assert_eq!(
        result.batch.values.to_data(),
        largeint::array_from_i128(&[Some(-7), None, None, Some(5)])
            .unwrap()
            .to_data()
    );
    assert_overflow(&result, "largeint", i128::MIN);
}

#[test]
fn try_recovers_only_failed_rows_from_its_original_body() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [column("x", 4, DataType::Int32, false)];
    let input = batch(
        &columns,
        vec![Arc::new(Int32Array::from(vec![7, i32::MIN, -5]))],
    );
    let typed = compile("TRY(TRY(-x))", &columns, &context);
    let result = evaluate(&typed, &input);
    assert_compared(&result, 1, 3);
    assert!(result.batch.result_type.nullable());
    assert!(result.batch.errors.is_empty());
    assert_eq!(
        result.batch.values.to_data(),
        Int32Array::from(vec![Some(-7), None, Some(5)]).to_data(),
    );
    let result = constant("TRY(TRY(7))", &context);
    assert_eq!(
        result.batch.values.to_data(),
        Int32Array::from(vec![7]).to_data()
    );
}

#[test]
fn try_keeps_successes_and_nulls_around_invalid_array_subscripts() {
    use arrow::{array::ListArray, datatypes::Int32Type};
    use novarocks_failure_contract::kernel_reason::INVALID_FUNCTION_ARGUMENT;
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let arrays = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(11)]),
        Some(vec![Some(12)]),
        None,
        Some(vec![Some(14)]),
        Some(vec![Some(15)]),
        Some(vec![None]),
        Some(vec![Some(17)]),
    ]);
    let columns = [
        column("a", 1, arrays.data_type().clone(), true),
        column("i", 2, DataType::Int64, false),
    ];
    let input = batch(
        &columns,
        vec![
            Arc::new(arrays),
            Arc::new(Int64Array::from(vec![1, 0, 1, 2, -1, 1, 1])),
        ],
    );
    let bare = evaluate(&compile("a[i]", &columns, &context), &input);
    assert_eq!(
        bare.batch
            .errors
            .iter()
            .map(|error| (error.selected_ordinal(), error.reason()))
            .collect::<Vec<_>>(),
        [1, 3, 4].map(|ordinal| (
            ordinal,
            DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)
        )),
    );
    let try_cast = evaluate(
        &compile("TRY_CAST(a[i] AS BIGINT)", &columns, &context),
        &input,
    );
    assert_eq!(try_cast.batch.errors, bare.batch.errors);
    assert_eq!(
        try_cast.batch.values.to_data(),
        Int64Array::from(vec![Some(11), None, None, None, None, None, Some(17)]).to_data(),
    );
    let caught = evaluate(&compile("TRY(a[i])", &columns, &context), &input);
    assert!(caught.batch.errors.is_empty());
    assert_eq!(
        caught.batch.values.to_data(),
        Int32Array::from(vec![Some(11), None, None, None, None, None, Some(17)]).to_data(),
    );
}

fn assert_overflow(result: &CheckedRows, label: &str, minimum: impl std::fmt::Display) {
    let [error] = result.batch.errors.as_ref() else {
        panic!("one overflow row");
    };
    assert_eq!(error.selected_ordinal(), 2);
    assert_eq!(error.reason(), DataFailureReason::NumericOutOfRange);
    assert_eq!(
        error.message(),
        format!("{label} negation overflow: {minimum}")
    );
}

#[test]
fn negation_compares_both_float_widths_and_signed_zero() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    macro_rules! check {
        ($array:ty, $native:ty, $carrier:expr) => {{
            let columns = [column("x", 4, $carrier, true)];
            let typed = compile("-x", &columns, &context);
            let input = Arc::new(<$array>::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(<$native>::NAN),
                None,
                Some(-4.2),
            ]));
            let result = evaluate(&typed, &batch(&columns, vec![input]));
            assert_compared(&result, 1, 5);
            assert!(result.batch.errors.is_empty());
            assert_eq!(result.batch.result_type, columns[0].value_type);
            let values = result
                .batch
                .values
                .as_any()
                .downcast_ref::<$array>()
                .unwrap();
            assert_eq!(values.value(0).to_bits(), (-0.0 as $native).to_bits());
            assert_eq!(values.value(1).to_bits(), (0.0 as $native).to_bits());
            assert!(values.value(2).is_nan());
            assert!(values.is_null(3));
            assert_eq!(values.value(4), 4.2);
        }};
    }
    check!(Float32Array, f32, DataType::Float32);
    check!(Float64Array, f64, DataType::Float64);
    let literal = constant("-(0.0E0)", &context);
    assert_eq!(
        literal
            .batch
            .values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        (-0.0_f64).to_bits()
    );
}

#[test]
fn negation_compares_decimal_carriers_and_their_full_types() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let decimal = "146437688233000000000000000000000000000007"
        .parse::<i256>()
        .unwrap();
    let negative = "-146437688233000000000000000000000000000007"
        .parse::<i256>()
        .unwrap();
    for nullable in [false, true] {
        let middle128 = (!nullable).then_some(0);
        let middle256 = (!nullable).then_some(i256::ZERO);
        let cases: [(ArrayRef, ArrayRef); 2] = [
            (
                Arc::new(
                    Decimal128Array::from(vec![Some(12345), middle128])
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(-12345), middle128])
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                ),
            ),
            (
                Arc::new(
                    Decimal256Array::from(vec![Some(decimal), middle256])
                        .with_precision_and_scale(45, 3)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal256Array::from(vec![Some(negative), middle256])
                        .with_precision_and_scale(45, 3)
                        .unwrap(),
                ),
            ),
        ];
        for (input, expected) in cases {
            let columns = [column("x", 4, input.data_type().clone(), nullable)];
            let typed = compile("-x", &columns, &context);
            let result = evaluate(&typed, &batch(&columns, vec![input]));
            assert_compared(&result, 1, 2);
            assert!(result.batch.errors.is_empty());
            assert_eq!(result.batch.result_type, columns[0].value_type);
            assert_eq!(result.batch.values.to_data(), expected.to_data());
        }
    }
}

#[test]
fn compiled_rand_effect_makes_an_input_expression_inapplicable() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [column("b", 4, DataType::Boolean, false)];
    let typed = compile("CASE WHEN b THEN rand() ELSE 0.0E0 END", &columns, &context);
    let result = evaluate(
        &typed,
        &batch(
            &columns,
            vec![Arc::new(BooleanArray::from(vec![true, false]))],
        ),
    );
    assert_eq!(
        result.batch.shape,
        EvaluationShape {
            port_count: 1,
            input_columns: 1,
            input_rows: 2
        }
    );
    assert!(matches!(
        result.consistency,
        Consistency::NotApplicable(Inapplicability::Call { function, reason: CallInapplicability::Volatile })
        if function.as_str() == "builtin.scalar/rand/v1"
    ));
    assert!(result.batch.errors.is_empty());
    assert_eq!(
        result
            .batch
            .values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(1),
        0.0
    );
}

#[test]
fn input_constant_policy_reports_the_original_row_and_column() {
    let functions = fixture();
    let parameters = parameters();
    let mut context = context(&functions, &parameters);
    context.constant_policy.max_logical_elements = 1;
    let columns = [column(
        "x",
        4,
        DataType::List(Arc::new(arrow::datatypes::Field::new_list_field(
            DataType::Int32,
            true,
        ))),
        true,
    )];
    let typed = compile("x IS NOT NULL", &columns, &context);
    let input =
        arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([
            None,
            Some(vec![Some(23), Some(42), Some(42)]),
        ]);
    let result = evaluate(&typed, &batch(&columns, vec![Arc::new(input)]));
    assert_eq!(
        result.batch.values.to_data(),
        BooleanArray::from(vec![false, true]).to_data()
    );
    assert!(matches!(
        result.consistency,
        Consistency::NotApplicable(Inapplicability::InputConstant { row: 1, column, cause: novarocks_constant_contract::ConstantError::Limit(_) })
        if column == "x"
    ));
}

#[path = "expression_text/temporal_arithmetic.rs"]
mod temporal_arithmetic;

#[path = "expression_text/temporal_context.rs"]
mod temporal_context;
#[test]
fn standard_arithmetic_selects_the_exact_decimal_declarations() {
    use arrow::array::Decimal128Array;
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);

    let ratio = constant("DECIMAL '1.00' / DECIMAL '3.00'", &context);
    assert_eq!(ratio.batch.values.data_type(), &DataType::Decimal128(9, 6));
    assert_eq!(
        ratio
            .batch
            .values
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        333_333,
    );
    let ratio_with_null = constant("NULL / DECIMAL '3.00'", &context);
    assert_eq!(
        ratio_with_null.batch.values.data_type(),
        &DataType::Decimal128(9, 6)
    );
    assert!(ratio_with_null.batch.values.is_null(0));
    let null_modulo = constant("NULL % NULL", &context);
    assert_eq!(null_modulo.batch.values.data_type(), &DataType::Int8);
    assert!(null_modulo.batch.values.is_null(0));
}

#[test]
fn standard_arithmetic_uses_the_original_real_width_and_selected_rows() {
    use arrow::array::Float32Array;

    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [
        column("x", 5, DataType::Float32, true),
        column("y", 9, DataType::Float32, false),
    ];
    let input = batch(
        &columns,
        vec![
            Arc::new(Float32Array::from(vec![
                Some(16_777_216.0),
                Some(0.0),
                None,
            ])),
            Arc::new(Float32Array::from(vec![1.0, 0.0, 1.0])),
        ],
    );
    let checked = evaluate(&compile("(+x + y) - x", &columns, &context), &input);
    assert_compared(&checked, 2, 3);
    assert_eq!(
        checked.batch.values.to_data(),
        Float32Array::from(vec![Some(0.0), Some(0.0), None]).to_data(),
    );
    let checked = evaluate(&compile("x / y", &columns, &context), &input);
    let values = checked
        .batch
        .values
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(values.value(0), 16_777_216.0);
    assert!(values.value(1).is_nan());
    assert!(values.is_null(2));
    assert!(checked.batch.errors.is_empty());
}

#[test]
fn standard_mixed_arithmetic_rounds_decimals_directly_to_the_selected_ieee_width() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);

    let double = constant("DECIMAL '1801439850948188.9' + 0e0", &context);
    assert_eq!(
        double
            .batch
            .values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        0x4319_9999_9999_9974,
    );

    let columns = [
        column("r", 5, DataType::Float32, false),
        column("d", 17, DataType::Decimal128(38, 37), true),
    ];
    // The coefficient lies just above the first REAL midpoint around 1.0.
    // Rounding via DOUBLE would return the neighboring REAL value.
    let above_midpoint = 10_000_000_596_046_447_753_906_250_000_000_000_001;
    let values = Decimal128Array::from(vec![Some(above_midpoint), None])
        .with_precision_and_scale(38, 37)
        .unwrap();
    let typed = compile("r + d", &columns, &context);
    let coercion = novarocks_functions::builtin::value_conversion::OPERATOR_DECIMAL_FLOAT_COERCION;
    assert!(typed.unit().definitions().iter().any(|definition| matches!(
        &definition.kind,
        ExprKind::FunctionCall { function, .. } if function.overload.as_str() == coercion
    )));
    let input = batch(
        &columns,
        vec![
            Arc::new(Float32Array::from(vec![0.0, 0.0])),
            Arc::new(values),
        ],
    );
    let checked = evaluate(&typed, &input);
    assert_compared(&checked, 2, 2);
    let output = checked
        .batch
        .values
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(output.value(0).to_bits(), 0x3f80_0001);
    assert!(output.is_null(1));
    assert!(checked.batch.errors.is_empty());
}

#[test]
fn ordinary_and_null_safe_float_comparisons_preserve_distinct_relations() {
    use arrow::array::{BooleanArray, Float64Array};

    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [
        column("x", 5, DataType::Float64, true),
        column("y", 9, DataType::Float64, true),
    ];
    let input = batch(
        &columns,
        vec![
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(f64::NAN),
                Some(-0.0),
                None,
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(1.0),
                Some(0.0),
                None,
                Some(0.0),
            ])),
        ],
    );
    for (operator, expected) in [
        (
            "x = y",
            vec![Some(false), Some(false), Some(true), None, None],
        ),
        (
            "x < y",
            vec![Some(false), Some(false), Some(false), None, None],
        ),
        (
            "x IS NOT DISTINCT FROM y",
            vec![Some(true), Some(false), Some(true), Some(true), Some(false)],
        ),
        (
            "x IS DISTINCT FROM y",
            vec![
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                Some(true),
            ],
        ),
    ] {
        let checked = evaluate(&compile(operator, &columns, &context), &input);
        assert_eq!(
            checked.batch.values.to_data(),
            BooleanArray::from(expected).to_data(),
            "{operator}"
        );
    }
}

#[test]
fn integer_array_equality_keeps_nested_nulls_and_compares_mixed_width_elements() {
    use arrow::array::{ListArray, types};

    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let left: ArrayRef = Arc::new(ListArray::from_iter_primitive::<types::Int32Type, _, _>([
        Some(vec![Some(1), None]),
        Some(vec![None, Some(3)]),
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(1)]),
        Some(vec![None]),
        None,
    ]));
    let right: ArrayRef = Arc::new(ListArray::from_iter_primitive::<types::Int64Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(2), Some(4)]),
        Some(vec![Some(1), Some(2)]),
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
    ]));
    let columns = [
        column("lhs", 10, left.data_type().clone(), true),
        column("rhs", 20, right.data_type().clone(), false),
    ];
    let batch = batch(&columns, vec![left, right]);
    for (sql, expected) in [
        (
            "lhs = rhs",
            [
                None,
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                None,
            ],
        ),
        (
            "lhs <> rhs",
            [None, Some(true), Some(false), Some(true), Some(true), None],
        ),
    ] {
        let expression = compile(sql, &columns, &context);
        let checked = evaluate(&expression, &batch);
        assert!(checked.batch.errors.is_empty());
        let values = checked
            .batch
            .values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), expected, "{sql}");
    }
}

#[test]
fn map_concat_ordered_null_short_circuits_only_later_conversions() {
    use novarocks_failure_contract::kernel_reason::INVALID_CAST_ARGUMENT;
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    use arrow::{
        array::{MapArray, StructArray},
        buffer::OffsetBuffer,
        datatypes::Field,
    };
    let entries = StructArray::try_new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int32, true),
        ]
        .into(),
        vec![
            Arc::new(Int64Array::from(vec![9007199254740992, 9007199254740993])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
        None,
    )
    .unwrap();
    let collision: ArrayRef = Arc::new(
        MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        )
        .unwrap(),
    );
    let columns = [column(
        "collision",
        72,
        collision.data_type().clone(),
        false,
    )];
    let input = batch(&columns, vec![collision]);
    let null = "CAST(NULL AS MAP(DOUBLE, INTEGER))";

    let left_null = evaluate(
        &compile(
            &format!("map_concat({null}, collision)"),
            &columns,
            &context,
        ),
        &input,
    );
    assert!(left_null.batch.values.is_null(0));
    assert!(left_null.batch.errors.is_empty());

    let left_error = evaluate(
        &compile(
            &format!("map_concat(collision, {null})"),
            &columns,
            &context,
        ),
        &input,
    );
    assert!(left_error.batch.values.is_null(0));
    let [error] = left_error.batch.errors.as_ref() else {
        panic!("conversion of the demanded first argument must fail");
    };
    assert_eq!(
        error.reason(),
        DataFailureReason::Domain(INVALID_CAST_ARGUMENT)
    );
}

#[test]
fn nullif_composite_keeps_original_left_values_and_only_demands_present_rows() {
    use arrow::array::{ListArray, ListBuilder, StringBuilder, types::Int32Type};
    use novarocks_failure_contract::kernel_reason::INVALID_CAST_ARGUMENT;

    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let left: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
        None,
        Some(vec![Some(1), None]),
        Some(vec![Some(2)]),
        Some(vec![Some(3)]),
        Some(vec![Some(4)]),
    ]));
    let mut lists = ListBuilder::new(StringBuilder::new());
    for row in [&["bad"][..], &["1", "5"], &["2"], &["bad"], &["9"]] {
        for value in row {
            lists.values().append_value(*value);
        }
        lists.append(true);
    }
    let right: ArrayRef = Arc::new(lists.finish());
    let columns = [
        column("x", 1, left.data_type().clone(), true),
        column("txt", 2, right.data_type().clone(), false),
    ];
    let rows = batch(&columns, vec![left, right]);
    let result = evaluate(
        &compile("NULLIF(x, CAST(txt AS ARRAY(BIGINT)))", &columns, &context),
        &rows,
    );
    assert_eq!(result.batch.result_type, columns[0].value_type);
    assert_eq!(
        result.batch.values.to_data(),
        ListArray::from_iter_primitive::<Int32Type, _, _>([
            None,
            Some(vec![Some(1), None]),
            None,
            None,
            Some(vec![Some(4)]),
        ])
        .to_data(),
    );
    let [error] = result.batch.errors.as_ref() else {
        panic!("the invalid right child of the absent left row must not be demanded");
    };
    assert_eq!(error.selected_ordinal(), 3);
    assert_eq!(
        error.reason(),
        DataFailureReason::Domain(INVALID_CAST_ARGUMENT)
    );
}
