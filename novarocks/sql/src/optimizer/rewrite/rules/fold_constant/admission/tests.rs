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
use crate::constant::test_constant_policy;
use crate::optimizer::test_optimizer_control;
use arrow::array::{
    Array, FixedSizeBinaryArray, Float32Array, Int32Array, StringArray, UInt32Array, make_array,
    new_null_array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::i256;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_type_contract::{
    CompileControlError, NR_LOGICAL_TYPE_KEY, ValueLayout, ValueTypeStorageLifetime,
};
use std::sync::Mutex;

fn admit(output: ArrayRef, ty: &FunctionValueType) -> Option<ConstantValue> {
    admit_result(output, ty, test_constant_policy(), test_optimizer_control()).unwrap()
}

fn value_type_supported(ty: &FunctionValueType) -> bool {
    let mut work = CompileCheckpoints::try_new(
        test_optimizer_control(),
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    let supported = fold_value_type_supported(ty, &mut work).unwrap();
    work.finish().unwrap();
    supported
}

fn nested_nominal_type(logical: ValueLogicalType, carrier: DataType) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let field = Field::new("item", carrier, true)
        .with_metadata([(NR_LOGICAL_TYPE_KEY, logical.metadata_value().unwrap())]);
    let ty = DataType::List(Arc::new(field));
    let item = ValueTypeRef::try_new(ValueKind::Nominal(logical), true, storage.clone()).unwrap();
    let value = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    FunctionValueType::try_from_value(value, ValueLayout::try_new(&ty, storage).unwrap()).unwrap()
}

#[test]
fn authored_nominal_and_nested_domains_decline_without_carrier_retagging() {
    for (carrier, logical) in [
        (DataType::LargeBinary, ValueLogicalType::Json),
        (DataType::LargeBinary, ValueLogicalType::Variant),
        (DataType::Binary, ValueLogicalType::Hll),
        (DataType::Binary, ValueLogicalType::Bitmap),
        (DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
    ] {
        let ty = FunctionValueType::try_with_logical_type(carrier.clone(), true, logical).unwrap();
        assert!(!value_type_supported(&ty), "{ty:?}");
        assert_eq!(ty.data_type(), &carrier);
        assert_eq!(ty.logical_type(), logical);
    }
    let list = nested_nominal_type(ValueLogicalType::Json, DataType::LargeBinary);
    assert!(!value_type_supported(&list));
    assert_eq!(
        list.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json,
    );
    let fixed_list = FunctionValueType::new(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::FixedSizeBinary(3),
            true,
        ))),
        true,
    );
    assert!(!value_type_supported(&fixed_list));
}

#[test]
fn exact_largeint_result_keeps_nominal_identity() {
    let carrier = DataType::FixedSizeBinary(16);
    let ty = FunctionValueType::try_with_logical_type(
        carrier.clone(),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    assert!(value_type_supported(&ty));
    assert!(!value_type_supported(&FunctionValueType::new(
        carrier.clone(),
        false
    )));
    assert!(!value_type_supported(&nested_nominal_type(
        ValueLogicalType::LargeInt,
        carrier
    )));
    let value = i128::from(i64::MAX) + 17;
    let bytes = [
        1_i128.to_be_bytes(),
        value.to_be_bytes(),
        (-3_i128).to_be_bytes(),
    ];
    let array =
        FixedSizeBinaryArray::try_from_iter(bytes.iter().map(|bytes| bytes.as_slice())).unwrap();
    let output = admit(Arc::new(array.slice(1, 1)), &ty).unwrap();
    assert_eq!(output.value_type(), &ty);
    assert_eq!(output.try_largeint().unwrap(), Some(value));
}

#[test]
fn float32_signaling_nan_and_signed_zero_never_widen_through_float64_syntax() {
    let ty = FunctionValueType::new(DataType::Float32, false);
    for bits in [
        0x7f80_0001,
        0xff80_0123,
        0x8000_0000,
        0x0000_0001,
        0x7f80_0000,
    ] {
        let output = admit(
            Arc::new(Float32Array::from(vec![f32::from_bits(bits)])),
            &ty,
        )
        .unwrap();
        let array = output
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(array.value(0).to_bits(), bits);
        assert_eq!(output.value_type(), &ty);
    }
}

#[test]
fn newly_materialized_unsigned_negative_scale_and_nested_metadata_outputs_are_cv() {
    // Unit evaluation supplies an already materialized carrier. This gate
    // preserves its exact layout, including carriers the optimizer never folds.
    let unsigned = FunctionValueType::new(DataType::UInt32, true);
    let value = admit(Arc::new(UInt32Array::from(vec![7])), &unsigned).unwrap();
    assert_eq!(value.value_type(), &unsigned);
    assert_eq!(
        value
            .pool()
            .array()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(0),
        7,
    );

    let decimal_type = FunctionValueType::new(DataType::Decimal128(10, -2), false);
    let decimal = Decimal128Array::from(vec![123])
        .with_precision_and_scale(10, -2)
        .unwrap();
    let value = admit(Arc::new(decimal), &decimal_type).unwrap();
    assert_eq!(value.value_type(), &decimal_type);
    assert_eq!(
        value
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        123,
    );

    let source_field = Field::new("source", DataType::Int32, true)
        .with_metadata([("provider.fact".to_owned(), "kept".to_owned())]);
    let source_type = DataType::List(Arc::new(source_field));
    let ty = FunctionValueType::new(source_type, true);
    let value = admit(new_null_array(ty.data_type(), 1), &ty).unwrap();
    assert_eq!(value.value_type(), &ty);
    let DataType::List(field) = value.value_type().data_type() else {
        panic!("expected the frozen List carrier");
    };
    assert_eq!(field.name(), "item");
    assert!(field.metadata().as_arc().is_none());
    assert!(field.is_nullable());
}

#[test]
fn source_annotations_do_not_mint_a_type_resource_refusal() {
    let field = Field::new("item", DataType::Utf8, true).with_metadata([(
        "provider.fact",
        "x".repeat(novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES + 1),
    )]);
    let annotated = DataType::List(Arc::new(field));
    let target = FunctionValueType::new(annotated, true);
    let plain = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    );
    assert_eq!(target, plain);
    assert!(value_type_supported(&target));
    let output = new_null_array(target.data_type(), 1);
    let policy = test_constant_policy();
    let recording = RecordingControl::default();
    let value = admit_result(output.clone(), &target, policy, &recording)
        .unwrap()
        .unwrap();
    assert_eq!(value.value_type(), &plain);
    let trace = recording.calls.into_inner().unwrap();
    assert_eq!(trace[0], (CompilePhase::FunctionSpecialization, 0));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::Source(SharedFailureCause::new(KernelFailure::ResourceExhausted)),
    ] {
        for index in 0..trace.len() {
            let refusing = RecordingControl {
                fail_at: Some((index, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                admit_result(output.clone(), &target, policy, &refusing).unwrap_err(),
                SqlCompileError::from(cause.clone()),
            );
            assert_eq!(refusing.calls.into_inner().unwrap(), trace[..=index]);
        }
    }
}

#[derive(Default)]
struct RecordingControl {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    fail_at: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for RecordingControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len();
        calls.push((phase, units));
        if let Some((at, cause)) = &self.fail_at
            && *at == index
        {
            return Err(cause.clone());
        }
        Ok(())
    }
}

#[test]
fn optional_fold_declines_values_that_exceed_the_constant_pool_limit() {
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let policy = novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
            max_retained_buffer_bytes: 256,
            ..test_constant_policy()
        },
    );
    let control = test_optimizer_control();
    let small = Arc::new(StringArray::from(vec!["March"])) as ArrayRef;
    let long_text = "t".repeat(2560);
    let long = Arc::new(StringArray::from(vec![long_text.as_str()])) as ArrayRef;
    assert!(admit_result(small, &ty, policy, control).unwrap().is_some());
    assert!(admit_result(long, &ty, policy, control).unwrap().is_none());
}

#[test]
fn malformed_selected_utf8_bytes_decline_without_constructing_invalid_str() {
    for ty in [DataType::Utf8, DataType::LargeUtf8] {
        let offsets = if ty == DataType::Utf8 {
            Buffer::from_slice_ref([0_i32, 1])
        } else {
            Buffer::from_slice_ref([0_i64, 1])
        };
        let target = FunctionValueType::new(ty.clone(), false);
        // SAFETY: This fixture deliberately violates UTF8 validity. The result
        // gate reads only the well-formed offsets and raw bytes.
        let data = unsafe {
            arrow::array::ArrayData::builder(ty)
                .len(1)
                .add_buffer(offsets)
                .add_buffer(Buffer::from(vec![0xff]))
                .build_unchecked()
        };
        assert!(admit(make_array(data), &target).is_none());
    }
}

#[test]
fn decimal_result_outside_its_precision_declines_folding() {
    let narrow128 = Decimal128Array::from(vec![100])
        .with_precision_and_scale(2, 0)
        .unwrap();
    let narrow256 = Decimal256Array::from(vec![i256::from_i128(100)])
        .with_precision_and_scale(2, 0)
        .unwrap();
    for output in [Arc::new(narrow128) as ArrayRef, Arc::new(narrow256)] {
        let ty = FunctionValueType::new(output.data_type().clone(), false);
        assert!(admit(output, &ty).is_none());
    }
}

#[test]
fn result_nullability_is_checked_against_the_arena_type() {
    let target = FunctionValueType::new(DataType::Int32, true);
    let array = Arc::new(Int32Array::from(vec![Some(3), None]));
    let evaluated = Arc::new(array.slice(1, 1));
    let constant = admit(evaluated.clone(), &target).unwrap();
    assert_eq!(constant.value_type(), &target);
    assert!(
        constant
            .is_null_observed(
                CompilePhase::FunctionSpecialization,
                test_optimizer_control()
            )
            .unwrap()
    );
    assert!(admit(evaluated, &target.with_nullable(false).unwrap()).is_none());
}

#[test]
fn evaluator_result_geometry_and_carrier_mismatch_are_fatal() {
    let target = FunctionValueType::new(DataType::Int32, false);
    for (output, expected) in [
        (
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            "constant folding produced 2 rows, expected exactly 1",
        ),
        (
            Arc::new(UInt32Array::from(vec![1])) as ArrayRef,
            "constant folding output differs from its frozen carrier",
        ),
    ] {
        let Err(SqlCompileError::CompilationSource(source)) = admit_result(
            output,
            &target,
            test_constant_policy(),
            test_optimizer_control(),
        ) else {
            panic!("the original arena result must not admit the invalid carrier");
        };
        assert_eq!(source.failure_fact().class(), FailureClass::Invariant);
        let Some(KernelFailure::InvalidProgram(diagnostic)) =
            source.downcast_ref::<KernelFailure>()
        else {
            panic!("the invalid output must retain its original program diagnostic");
        };
        assert_eq!(diagnostic.message(), expected);
    }
}
