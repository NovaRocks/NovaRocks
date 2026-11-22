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
        Time64MicrosecondArray, TimestampMicrosecondArray,
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
    typing::TypingError,
};
use novarocks_expression_contract::{ExprKind, ValueId};
use novarocks_functions::{DataFailureReason, largeint};
use novarocks_type_contract::{
    DecimalOverflowPolicy, FunctionValueType, SemanticParameterId, SemanticParameterKey,
    SemanticParameterRef, SemanticParameterValue, SemanticParameters, ValueLogicalType,
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
    assert_compared(&result, 2, 3);
    assert!(result.batch.errors.is_empty());
    assert_eq!(
        result.batch.values.to_data(),
        Int64Array::from(vec![8, 7, 5]).to_data()
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
        Int32Array::from(vec![2025]).to_data()
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
    let temporal: &[(&str, ArrayRef)] = &[
        (
            "TIME '01:02:03'",
            Arc::new(Time64MicrosecondArray::from(vec![3_723_000_000])),
        ),
        (
            "TIMESTAMP '1970-01-02 00:00:00'",
            Arc::new(TimestampMicrosecondArray::from(vec![86_400_000_000])),
        ),
    ];
    for (source, expected) in temporal {
        let typed = compile(source, &[], &context);
        let unit = typed.unit();
        let root = unit.definition(unit.roots()[0].definition).unwrap();
        let expected_type = FunctionValueType::new(expected.data_type().clone(), true);
        assert_eq!(root.ty, expected_type, "{source}");
        let ExprKind::Cast {
            expr,
            target,
            allow_throw_exception,
            decimal_overflow_policy,
            array_failure_mode: None,
        } = &root.kind
        else {
            panic!("{source}: expected the original text CAST");
        };
        assert_eq!(target, expected.data_type(), "{source}");
        assert_eq!(*decimal_overflow_policy, context.decimal_overflow_policy);
        assert_eq!(*allow_throw_exception, ALLOW_THROW);
        assert_eq!(
            typed.environment().parameters.require(ALLOW_THROW).unwrap(),
            &SemanticParameterValue::AllowThrowException(true)
        );
        let child = unit.definition(*expr).unwrap();
        assert!(matches!(child.kind, ExprKind::Constant(_)));
        assert_eq!(child.ty, FunctionValueType::new(DataType::Utf8, false));

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
        ExprKind::Cast { allow_throw_exception, .. } if allow_throw_exception == ALLOW_THROW
    )));
    assert_eq!(
        typed.environment().parameters.require(ALLOW_THROW).unwrap(),
        &SemanticParameterValue::AllowThrowException(true)
    );
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
    for (text, name) in [
        ("TRIM(x)", "trim"),
        ("IF(TRUE, x, x)", "if"),
        ("count(x)", "count"),
        ("x.y", "qualified name or field reference"),
        ("CAST(x AS BIGINT)", "CAST"),
    ] {
        assert!(
            matches!(
                type_text(text),
                Err(TextError::NotTypedByLayer { construct, .. }) if construct == name
            ),
            "{text}"
        );
    }
    assert!(matches!(
        type_text("TRIM('a' FROM x)"),
        Err(TextError::UnsupportedSyntax(cause)) if cause.message == "explicit TRIM characters are not supported"
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
