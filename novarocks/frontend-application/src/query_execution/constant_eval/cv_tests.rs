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

//! Checked CV fixtures exercise the real staged FE evaluator, not a second
//! evaluator or a syntax readback adapter. Policies are finite test inputs.
use super::*;
use arrow::array::{Date32Array, Float32Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::Field;
use novarocks_functions::{ConstantError, ConstantPolicy};
use novarocks_sql::compiler::FoldArg;
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, NR_LOGICAL_TYPE_KEY, ValueLayout,
    ValueTypeStorageLifetime,
};
use std::sync::Mutex;

struct TestControl;
impl PureCompileControl for TestControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
fn pool(array: ArrayRef, ty: FunctionValueType) -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(Field::new(
            "literal",
            array.data_type().clone(),
            ty.nullable(),
        )),
        ty,
        array.to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &TestControl,
    )
    .unwrap()
}
fn arg(array: ArrayRef, nullable: bool) -> FoldArg {
    let ty = FunctionValueType::new(array.data_type().clone(), nullable);
    typed_arg(array, ty)
}
fn typed_arg(array: ArrayRef, ty: FunctionValueType) -> FoldArg {
    let source_type = array.data_type().clone();
    let value = pool(array, ty.clone()).value(0).unwrap();
    assert_eq!(value.field().data_type(), &source_type);
    assert_eq!(value.value_type(), &ty);
    FoldArg {
        value,
        value_type: ty,
    }
}
fn json_list_type(source_type: &DataType) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        storage.clone(),
    )
    .unwrap();
    let value = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    FunctionValueType::try_from_value(value, ValueLayout::try_new(source_type, storage).unwrap())
        .unwrap()
}
fn i32(value: i32) -> FoldArg {
    arg(Arc::new(Int32Array::from(vec![value])), false)
}
fn i64(value: i64) -> FoldArg {
    arg(Arc::new(Int64Array::from(vec![value])), false)
}
fn text(value: &str) -> FoldArg {
    arg(Arc::new(StringArray::from(vec![value])), false)
}
fn decimal(value: i128, precision: u8, scale: i8) -> FoldArg {
    arg(
        Arc::new(
            Decimal128Array::from(vec![value])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ),
        false,
    )
}
fn request(kind: FoldNodeKind, args: Vec<FoldArg>, result: DataType) -> FoldRequest {
    FoldRequest {
        regexp_count_pattern_source: None,
        kind,
        args,
        constant_policy: policy(),
        result_type: FunctionValueType::new(result, true),
    }
}
fn cast(args: Vec<FoldArg>, target: DataType) -> FoldRequest {
    request(
        FoldNodeKind::Cast(DecimalOverflowPolicy::OutputNull),
        args,
        target,
    )
}
fn binary(op: BinOp, args: Vec<FoldArg>, result: DataType) -> FoldRequest {
    request(
        FoldNodeKind::BinaryOp(op, DecimalOverflowPolicy::OutputNull),
        args,
        result,
    )
}
fn evaluate(request: &FoldRequest) -> ConstantValue {
    let value = constant_evaluator()
        .eval_scalar(request, &TestControl)
        .unwrap()
        .unwrap();
    assert!(
        value
            .value_type()
            .exactly_equals_observed::<ConstantError>(&request.result_type, || Ok(()))
            .unwrap()
    );
    assert_eq!(value.field().name(), "literal");
    assert_eq!(value.pool().resource_facts().rows, 1);
    value
}
fn decimal_value(value: &ConstantValue) -> i128 {
    value
        .pool()
        .array()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .value(value.ordinal() as usize)
}

#[test]
fn folds_int32_addition_without_an_integer_carrier_roundtrip() {
    let output = evaluate(&binary(BinOp::Add, vec![i32(1), i32(1)], DataType::Int32));
    assert_eq!(
        output
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        2
    );
}
#[test]
fn folds_date_format_and_utf8_to_date32_through_real_kernels() {
    let date = arg(Arc::new(Date32Array::from(vec![18262])), false);
    let output = evaluate(&request(
        FoldNodeKind::Function {
            name: "date_format".into(),
        },
        vec![date, text("%Y-%m-%d")],
        DataType::Utf8,
    ));
    assert_eq!(
        output
            .utf8_observed(CompilePhase::FunctionSpecialization, &TestControl)
            .unwrap(),
        Some("2020-01-01")
    );
    assert_eq!(
        evaluate(&cast(vec![text("2020-01-01")], DataType::Date32))
            .try_date32()
            .unwrap(),
        Some(18262)
    );
}
#[test]
fn folds_decimal_multiplication_and_checked_rounding_cast_with_exact_coefficients() {
    assert_eq!(
        decimal_value(&evaluate(&binary(
            BinOp::Mul,
            vec![decimal(125, 10, 2), decimal(400, 10, 2)],
            DataType::Decimal128(20, 4)
        ))),
        50000
    );
    assert_eq!(
        decimal_value(&evaluate(&cast(
            vec![decimal(99_999_999, 8, 3)],
            DataType::Decimal128(8, 2)
        ))),
        10_000_000
    );
    let overflow = evaluate(&cast(
        vec![decimal(99_999_999, 8, 3)],
        DataType::Decimal128(7, 2),
    ));
    assert!(
        overflow
            .is_null_observed(CompilePhase::FunctionSpecialization, &TestControl)
            .unwrap()
    );
}
#[test]
fn declines_unknown_and_unmapped_unary_shapes_without_a_fake_kernel() {
    for kind in [
        FoldNodeKind::Function {
            name: "no_such_novarocks_function".into(),
        },
        FoldNodeKind::UnaryOp(UnOp::Negate),
        FoldNodeKind::UnaryOp(UnOp::BitwiseNot),
    ] {
        assert!(matches!(
            constant_evaluator()
                .eval_scalar(&request(kind, vec![i32(1)], DataType::Int32), &TestControl),
            Ok(None)
        ));
    }
    let boolean = arg(
        Arc::new(arrow::array::BooleanArray::from(vec![true])),
        false,
    );
    assert_eq!(
        evaluate(&request(
            FoldNodeKind::UnaryOp(UnOp::Not),
            vec![boolean],
            DataType::Boolean
        ))
        .try_boolean()
        .unwrap(),
        Some(false)
    );
}
#[test]
fn division_by_zero_is_successful_typed_null_and_nonnullable_output_declines() {
    let mut request = binary(BinOp::Div, vec![i32(1), i32(0)], DataType::Float64);
    assert!(
        evaluate(&request)
            .is_null_observed(CompilePhase::FunctionSpecialization, &TestControl)
            .unwrap()
    );
    request.result_type = request.result_type.with_nullable(false).unwrap();
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Ok(None)
    ));
}
#[test]
fn exact_source_gate_rejects_carrier_domain_and_nullable_forgery() {
    let original = i32(7);
    for ty in [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Int64, false),
    ] {
        let mut forged = original.clone();
        forged.value_type = ty;
        assert!(matches!(
            constant_evaluator().eval_scalar(&cast(vec![forged], DataType::Int64), &TestControl),
            Err(SqlConstantEvaluationError::Constant(
                ConstantError::Invalid(_)
            ))
        ));
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let mut forged = text("{}");
    forged.value_type = json;
    assert!(matches!(
        constant_evaluator().eval_scalar(&cast(vec![forged], DataType::Utf8), &TestControl),
        Err(SqlConstantEvaluationError::Constant(
            ConstantError::Invalid(_)
        ))
    ));
}
#[test]
fn authored_nominal_and_nested_domains_decline_without_carrier_retagging() {
    for (ty, array) in [
        (
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
            Arc::new(StringArray::from(vec!["{}"])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                true,
                ValueLogicalType::Variant,
            )
            .unwrap(),
            Arc::new(arrow::array::LargeBinaryArray::from(vec![b"x".as_slice()])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(DataType::Binary, true, ValueLogicalType::Hll)
                .unwrap(),
            Arc::new(arrow::array::BinaryArray::from(vec![b"x".as_slice()])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                true,
                ValueLogicalType::Bitmap,
            )
            .unwrap(),
            Arc::new(arrow::array::BinaryArray::from(vec![b"x".as_slice()])) as ArrayRef,
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
            arrow::array::new_null_array(&DataType::FixedSizeBinary(16), 1),
        ),
    ] {
        let value = pool(array, ty.clone()).value(0).unwrap();
        assert!(matches!(
            constant_evaluator().eval_scalar(
                &cast(
                    vec![FoldArg {
                        value,
                        value_type: ty
                    }],
                    DataType::Utf8
                ),
                &TestControl
            ),
            Ok(None)
        ));
    }
    let child = Field::new("item", DataType::Utf8, true)
        .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "json".into())].into());
    let source_type = DataType::List(Arc::new(child));
    let ty = json_list_type(&source_type);
    let null = typed_arg(arrow::array::new_null_array(&source_type, 1), ty.clone());
    assert_eq!(null.value.field().data_type(), &source_type);
    assert_eq!(
        null.value_type.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    let mut request = cast(vec![null], ty.data_type().clone());
    request.result_type = ty;
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Ok(None)
    ));
}
#[test]
fn checked_type_refusal_and_later_source_forgery_are_not_hidden_by_domain_decline() {
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let value = pool(Arc::new(StringArray::from(vec!["{}"])), json.clone())
        .value(0)
        .unwrap();
    assert!(
        FunctionValueType::try_with_logical_type(DataType::Int64, false, ValueLogicalType::Json)
            .is_err()
    );
    // A malformed complete declaration cannot enter the evaluator. A later
    // legitimate JSON declaration with a forged source still fails before an
    // earlier argument's domain decline hides it.
    let mut invalid = i64(1);
    invalid.value_type = json.clone();
    let request = cast(
        vec![
            FoldArg {
                value,
                value_type: json,
            },
            invalid,
        ],
        DataType::Utf8,
    );
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Err(SqlConstantEvaluationError::Constant(_))
    ));
}
#[test]
fn exact_largeint_constant_and_nonzero_pool_ordinal_keep_nominal_identity() {
    let ty = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let value = i128::from(i64::MAX) + 17;
    let bytes = [
        1_i128.to_be_bytes(),
        value.to_be_bytes(),
        (-3_i128).to_be_bytes(),
    ];
    let array = Arc::new(
        arrow::array::FixedSizeBinaryArray::try_from_iter(bytes.iter().map(|v| v.as_slice()))
            .unwrap(),
    );
    let source = pool(array, ty.clone()).value(1).unwrap();
    let mut request = cast(
        vec![FoldArg {
            value: source,
            value_type: ty.clone(),
        }],
        ty.data_type().clone(),
    );
    request.result_type = ty;
    assert_eq!(evaluate(&request).try_largeint().unwrap(), Some(value));
    request.args[0].value_type = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &TestControl),
        Err(SqlConstantEvaluationError::Constant(_))
    ));
}
#[test]
fn float32_signaling_nan_and_signed_zero_never_widen_through_float64_syntax() {
    for bits in [
        0x7f80_0001,
        0xff80_0123,
        0x8000_0000,
        0x0000_0001,
        0x7f80_0000,
    ] {
        let source = arg(
            Arc::new(Float32Array::from(vec![f32::from_bits(bits)])),
            false,
        );
        let output = evaluate(&cast(vec![source], DataType::Float32));
        assert_eq!(
            output
                .pool()
                .array()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            bits
        );
    }
}
#[test]
fn newly_materialized_unsigned_negative_scale_and_nested_metadata_outputs_are_cv() {
    let output = evaluate(&cast(vec![i32(7)], DataType::UInt32));
    assert_eq!(
        output
            .pool()
            .array()
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .unwrap()
            .value(0),
        7
    );
    let output = evaluate(&cast(
        vec![decimal(123, 10, -2)],
        DataType::Decimal128(10, -2),
    ));
    assert_eq!(decimal_value(&output), 123);
    let field = Arc::new(
        Field::new("source", DataType::Int32, true)
            .with_metadata([("provider.fact".into(), "kept".into())].into()),
    );
    let source_type = DataType::List(field);
    let source = arg(arrow::array::new_null_array(&source_type, 1), true);
    let request = cast(vec![source.clone()], source_type.clone());
    let output = evaluate(&request);
    assert_eq!(output.value_type(), &request.result_type);
    assert_eq!(source.value.field().data_type(), &source_type);
    let DataType::List(source_field) = source.value.field().data_type() else {
        panic!("original source is a List");
    };
    assert_eq!(source_field.name(), "source");
    assert_eq!(
        source_field.metadata().get("provider.fact").unwrap(),
        "kept"
    );
    let DataType::List(output_field) = output.value_type().data_type() else {
        panic!("frozen execution result is a List");
    };
    assert_eq!(output_field.name(), "item");
    assert_eq!(output_field.metadata().capacity(), 0);
    assert!(output.value_type().nullable());
    assert!(output_field.is_nullable());
}
#[test]
fn checked_decimal_error_remains_ordinary_evaluation_while_policy_null_is_success() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let request = request(
            FoldNodeKind::BinaryOp(BinOp::Add, policy),
            vec![
                decimal(99_999_999_999_999_999_999_999_999_999_999_999_999, 38, 0),
                i64(1),
            ],
            DataType::Decimal128(38, 0),
        );
        let result = constant_evaluator().eval_scalar(&request, &TestControl);
        if policy == DecimalOverflowPolicy::OutputNull {
            assert!(
                result
                    .unwrap()
                    .unwrap()
                    .is_null_observed(CompilePhase::FunctionSpecialization, &TestControl)
                    .unwrap()
            );
        } else {
            let Err(SqlConstantEvaluationError::Evaluation(message)) = result else {
                panic!("ordinary runtime error");
            };
            assert!(message.contains("'add' operation involving decimal values overflows"));
        }
    }
}

#[derive(Default)]
struct Control {
    checks: Mutex<Vec<u32>>,
    fail_at: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut checks = self.checks.lock().unwrap();
        let index = checks.len();
        checks.push(units);
        if let Some((at, cause)) = self.fail_at
            && at == index
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn assert_all_callbacks(
    request: &FoldRequest,
    expected: fn(&Result<Option<ConstantValue>, SqlConstantEvaluationError>),
) {
    let good = Control::default();
    let result = constant_evaluator().eval_scalar(request, &good);
    expected(&result);
    let trace = good.checks.lock().unwrap().clone();
    assert_eq!(trace[0], 0);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..trace.len() {
            let refusing = Control {
                fail_at: Some((at, cause)),
                ..Default::default()
            };
            assert!(
                matches!(constant_evaluator().eval_scalar(request, &refusing), Err(SqlConstantEvaluationError::Control(actual)) if actual == cause)
            );
            assert_eq!(*refusing.checks.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn real_wide_argument_decline_keeps_entry_quantum_tail_and_all_three_original_causes() {
    let request = request(
        FoldNodeKind::Function {
            name: "no_such_novarocks_function".into(),
        },
        vec![i64(7); 320],
        DataType::Int64,
    );
    assert_all_callbacks(&request, |result| assert!(matches!(result, Ok(None))));
    let good = Control::default();
    constant_evaluator().eval_scalar(&request, &good).unwrap();
    assert!(good.checks.lock().unwrap().contains(&256));
}
#[test]
fn actual_metadata_decline_and_successful_output_pool_keep_all_callback_prefixes() {
    let metadata = (0..255)
        .map(|i| (format!("provider.fact.{i}"), "value".into()))
        .chain([(NR_LOGICAL_TYPE_KEY.into(), "json".into())])
        .collect();
    let ty = DataType::List(Arc::new(
        Field::new("item", DataType::Utf8, true).with_metadata(metadata),
    ));
    let complete = json_list_type(&ty);
    let source = typed_arg(arrow::array::new_null_array(&ty, 1), complete.clone());
    assert_eq!(source.value.field().data_type(), &ty);
    let DataType::List(source_field) = source.value.field().data_type() else {
        panic!("original source is a List");
    };
    assert_eq!(source_field.metadata().len(), 256);
    assert_eq!(
        source_field.metadata().get(NR_LOGICAL_TYPE_KEY).unwrap(),
        "json"
    );
    assert_eq!(
        source.value_type.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    let mut request = cast(vec![source], complete.data_type().clone());
    request.result_type = complete;
    assert_all_callbacks(&request, |result| assert!(matches!(result, Ok(None))));
    assert_all_callbacks(
        &binary(BinOp::Add, vec![i32(1), i32(1)], DataType::Int32),
        |result| assert!(matches!(result, Ok(Some(_)))),
    );
}
#[test]
fn ordinary_kernel_error_and_nonnullable_decline_observe_exact_original_completion() {
    let request = request(
        FoldNodeKind::BinaryOp(BinOp::Add, DecimalOverflowPolicy::ReportError),
        vec![
            decimal(99_999_999_999_999_999_999_999_999_999_999_999_999, 38, 0),
            i64(1),
        ],
        DataType::Decimal128(38, 0),
    );
    assert_all_callbacks(&request, |result| {
        assert!(matches!(
            result,
            Err(SqlConstantEvaluationError::Evaluation(_))
        ))
    });
    let mut request = binary(BinOp::Div, vec![i32(1), i32(0)], DataType::Float64);
    request.result_type = request.result_type.with_nullable(false).unwrap();
    assert_all_callbacks(&request, |result| assert!(matches!(result, Ok(None))));
}
#[test]
fn shared_type_resource_refusal_cannot_be_replaced_or_fail_open() {
    let field = Field::new("item", DataType::Utf8, true).with_metadata(
        [(
            "provider.fact".into(),
            "x".repeat(novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES + 1),
        )]
        .into(),
    );
    let annotated = DataType::List(Arc::new(field));
    let request = request(
        FoldNodeKind::Cast(DecimalOverflowPolicy::OutputNull),
        vec![],
        annotated.clone(),
    );
    let plain = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    );
    // Source annotations no longer establish execution resources or semantics.
    assert_eq!(request.result_type, plain);
    let DataType::List(normalized) = request.result_type.data_type() else {
        panic!("frozen result is a List");
    };
    assert_eq!(normalized.metadata().capacity(), 0);
    let DataType::List(original) = &annotated else {
        panic!("original source is a List");
    };
    assert_eq!(
        original.metadata()["provider.fact"].len(),
        novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES + 1
    );
    // Preserve every actual callback's first cause, including ResourceExhausted,
    // rather than inventing an annotation-driven execution validation boundary.
    assert_all_callbacks(&request, |result| assert!(matches!(result, Ok(None))));
}
#[test]
fn explicit_result_policy_refusal_is_typed_preparation_and_has_no_later_callback() {
    let mut request = binary(BinOp::Add, vec![i32(1), i32(1)], DataType::Int32);
    request.constant_policy.max_rows = 0;
    let good = Control::default();
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &good),
        Err(SqlConstantEvaluationError::Constant(ConstantError::Limit(
            _
        )))
    ));
    let trace = good.checks.lock().unwrap().clone();
    let refusing = Control {
        fail_at: Some((trace.len(), CompileControlError::Cancelled)),
        ..Default::default()
    };
    assert!(matches!(
        constant_evaluator().eval_scalar(&request, &refusing),
        Err(SqlConstantEvaluationError::Constant(ConstantError::Limit(
            _
        )))
    ));
    assert_eq!(*refusing.checks.lock().unwrap(), trace);
}

#[test]
fn malformed_selected_utf8_bytes_decline_without_constructing_invalid_str() {
    use arrow::buffer::Buffer;
    for ty in [DataType::Utf8, DataType::LargeUtf8] {
        let offsets = if ty == DataType::Utf8 {
            Buffer::from_slice_ref([0i32, 1])
        } else {
            Buffer::from_slice_ref([0i64, 1])
        };
        // SAFETY: This deliberate invalid-UTF8 fixture is never read as &str.
        // The production guard examines only its validated offsets/raw bytes.
        let data = unsafe {
            arrow::array::ArrayData::builder(ty)
                .len(1)
                .add_buffer(offsets)
                .add_buffer(Buffer::from(vec![0xff]))
                .build_unchecked()
        };
        let output = arrow::array::make_array(data);
        let good = TestControl;
        let mut work =
            CompileCheckpoints::try_new(&good, CompilePhase::FunctionSpecialization).unwrap();
        assert!(!utf8_output_fits(&output, &mut work).unwrap());
        work.finish().unwrap();
    }
}

#[test]
fn regexp_count_fold_receipt_reuses_literal_error_null_and_selected_pool_contract() {
    use novarocks_type_contract::RegexpCountPatternSource as Source;
    let selected = pool(
        Arc::new(StringArray::from(vec!["a", "[0-9"])),
        FunctionValueType::new(DataType::Utf8, false),
    )
    .value(1)
    .unwrap();
    let pattern = FoldArg {
        value: selected,
        value_type: FunctionValueType::new(DataType::Utf8, false),
    };
    let mut request = request(
        FoldNodeKind::Function {
            name: "regexp_count".into(),
        },
        vec![text("aaa"), pattern],
        DataType::Int64,
    );
    // The same raw pooled calculator input without emitted provenance remains
    // Dynamic. Installing the existing fact is the behavior-changing seam.
    assert_eq!(evaluate(&request).try_i64().unwrap(), None);
    request.regexp_count_pattern_source = Some(Source::NativeV1Utf8LiteralWhenPresent);
    let error = constant_evaluator()
        .eval_scalar(&request, &TestControl)
        .unwrap_err();
    assert!(
        matches!(error, SqlConstantEvaluationError::Evaluation(ref message) if message.starts_with("Invalid regex expression: [0-9.")),
        "{error:?}"
    );
    request.args[0] = arg(Arc::new(StringArray::from(vec![None::<&str>])), true);
    assert_eq!(evaluate(&request).try_i64().unwrap(), None);
    request.args[0] = text("aaa");
    request.args[1] = arg(Arc::new(StringArray::from(vec![None::<&str>])), true);
    assert_eq!(evaluate(&request).try_i64().unwrap(), None);
    request.args[1] = text("a");
    assert_eq!(evaluate(&request).try_i64().unwrap(), Some(3));
    request.kind = FoldNodeKind::Function {
        name: "concat".into(),
    };
    assert!(
        constant_evaluator()
            .eval_scalar(&request, &TestControl)
            .is_err(),
        "a foreign call cannot consume a regexp source fact"
    );
}

#[test]
fn regexp_count_real_sql_fold_author_attaches_existing_emitted_source_before_error_decline() {
    use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId, ScanReadBudget};
    use novarocks_sql::compiler::*;
    use novarocks_type_contract::RegexpCountPatternSource as Source;
    struct Recording;
    static SEEN: Mutex<Vec<(Option<Source>, bool)>> = Mutex::new(Vec::new());
    impl SqlConstantEvaluator for Recording {
        fn eval_scalar(
            &self,
            request: &FoldRequest,
            control: &dyn PureCompileControl,
        ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
            let result = constant_evaluator().eval_scalar(request, control);
            if matches!(&request.kind, FoldNodeKind::Function { name } if name.eq_ignore_ascii_case("regexp_count"))
            {
                let failed = matches!(&result, Err(SqlConstantEvaluationError::Evaluation(message)) if message.starts_with("Invalid regex expression:"));
                SEEN.lock()
                    .unwrap()
                    .push((request.regexp_count_pattern_source, failed));
            }
            result
        }
    }
    static RECORDER: Recording = Recording;
    SEEN.lock().unwrap().clear();
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let control = SqlCompileControl::unbounded();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([87; 16]).unwrap(),
        SqlStatementInput::sql("SELECT regexp_count('test string','[0-9')"),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: Default::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                ..Default::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        catalog.snapshot(),
        &RECORDER,
        policy(),
        SqlPhysicalEmissionMode::OriginalNativeV1,
        control.clone(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: 64,
            max_batch_bytes: 1 << 20,
        },
        DEFAULT_COMPLETION_LIMITS,
    )
    .try_into_completion()
    .unwrap();
    let owner = SqlCompiler::start(request, &control)
        .unwrap()
        .into_complete()
        .unwrap()
        .into_plan();
    let seen = SEEN.lock().unwrap();
    assert!(
        !seen.is_empty(),
        "ordinary SQL must exercise the real fold author"
    );
    assert!(
        seen.iter()
            .all(|row| *row == (Some(Source::NativeV1Utf8LiteralWhenPresent), true))
    );
    assert!(owner.plan().fragments().values().any(|fragment| fragment.expressions().iter().any(|(_,expression)| matches!(&expression.kind,novarocks_physical_plan::ExprKind::FunctionCall {function,..} if function.function_id.as_str()=="builtin.scalar/regexp_count/v1"))), "failed literal evaluation must retain the actual call, never fold to NULL");
}

#[test]
fn variant_typeof_preserves_the_staged_nominal_json_fold_decline_without_retagging() {
    let json_type =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    for input in [
        Some("{}"),
        Some("[]"),
        Some("null"),
        Some("128"),
        Some("2147483648"),
        Some("not-json"),
        None,
    ] {
        let argument = typed_arg(Arc::new(StringArray::from(vec![input])), json_type.clone());
        let request = request(
            FoldNodeKind::Function {
                name: "variant_typeof".into(),
            },
            vec![argument],
            DataType::Utf8,
        );
        // This adapter intentionally declines non-Physical domains. The real
        // typed Native call must remain, rather than a fabricated folded NULL.
        assert!(
            constant_evaluator()
                .eval_scalar(&request, &TestControl)
                .unwrap()
                .is_none()
        );
        assert_eq!(request.args[0].value.value_type(), &json_type);
    }
}
