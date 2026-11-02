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
use arrow_array::types::Int8Type;
use arrow_array::{
    ArrayRef, BinaryArray, Decimal128Array, DictionaryArray, FixedSizeBinaryArray, Float32Array,
    Int8Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, new_empty_array, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{Field, IntervalUnit, TimeUnit};
use std::sync::{Arc, Mutex};

fn failures() -> [KernelFailure; 7] {
    use crate::KernelDiagnostic;
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("original internal")),
        KernelFailure::Operational(KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
    ]
}
fn copy(mask: &BooleanArray, truthy: &ArrayRef, falsy: &ArrayRef) -> ArrayRef {
    preflight_zip(mask, truthy.as_ref(), falsy.as_ref(), |_| Ok(())).unwrap();
    // ArrayRef implements ordinary Datum, so neither side takes ScalarZipper.
    arrow_select::zip::zip(mask, truthy, falsy).unwrap()
}
fn byte_types() -> [DataType; 4] {
    [
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::LargeBinary,
    ]
}
fn bytes(ty: &DataType, values: &[&[u8]], valid: &[bool]) -> ArrayRef {
    assert_eq!(values.len(), valid.len());
    let mut payload = vec![];
    let mut offsets = vec![0usize];
    for value in values {
        payload.extend_from_slice(value);
        offsets.push(payload.len());
    }
    let nulls = Some(NullBuffer::from(valid.to_vec()));
    let data = Buffer::from(payload);
    match ty {
        DataType::Utf8 | DataType::Binary => {
            let offsets = OffsetBuffer::new(ScalarBuffer::from(
                offsets
                    .into_iter()
                    .map(|offset| i32::try_from(offset).unwrap())
                    .collect::<Vec<_>>(),
            ));
            if *ty == DataType::Utf8 {
                Arc::new(StringArray::new(offsets, data, nulls))
            } else {
                Arc::new(BinaryArray::new(offsets, data, nulls))
            }
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let offsets = OffsetBuffer::new(ScalarBuffer::from(
                offsets
                    .into_iter()
                    .map(|offset| i64::try_from(offset).unwrap())
                    .collect::<Vec<_>>(),
            ));
            if *ty == DataType::LargeUtf8 {
                Arc::new(LargeStringArray::new(offsets, data, nulls))
            } else {
                Arc::new(LargeBinaryArray::new(offsets, data, nulls))
            }
        }
        _ => panic!("fixture is an offset byte carrier"),
    }
}
fn byte_value(array: &dyn Array, row: usize) -> &[u8] {
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .as_bytes(),
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(row),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(row)
            .as_bytes(),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(row),
        _ => panic!("fixture is an offset byte carrier"),
    }
}

#[test]
fn zip_fixed_values_bits_validity_and_nonzero_slices_have_hand_oracles() {
    let mask = BooleanArray::new(
        arrow_buffer::BooleanBuffer::from(vec![true, false, true, true, false]),
        Some(NullBuffer::from(vec![true, true, false, true, true])),
    )
    .slice(1, 3);
    let truthy: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(999),
        Some(4),
        Some(5),
        None,
        Some(999),
    ]));
    let falsy: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(888),
        Some(-7),
        Some(-8),
        Some(-9),
        Some(888),
    ]));
    let output = copy(&mask, &truthy.slice(1, 3), &falsy.slice(1, 3));
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-7), Some(-8), None]
    );

    let mask = BooleanArray::from(vec![Some(true), None, Some(true)]);
    let truthy: ArrayRef = Arc::new(Float32Array::from(vec![
        f32::from_bits(0x7fa0_0001),
        0.0,
        -0.0,
    ]));
    let falsy: ArrayRef = Arc::new(Float32Array::from(vec![
        1.0,
        f32::from_bits(0xffc0_0042),
        1.0,
    ]));
    let output = copy(&mask, &truthy, &falsy);
    let array = output.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(
        (0..3)
            .map(|row| array.value(row).to_bits())
            .collect::<Vec<_>>(),
        vec![0x7fa0_0001, 0xffc0_0042, 0x8000_0000]
    );

    let truthy: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
    let falsy: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false), Some(true), None]));
    let output = copy(&mask, &truthy, &falsy);
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(true), Some(true), Some(false)]
    );

    let truthy: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(155), None, Some(-12)])
            .with_precision_and_scale(5, 2)
            .unwrap(),
    );
    let falsy: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(999), Some(122), Some(333)])
            .with_precision_and_scale(5, 2)
            .unwrap(),
    );
    let output = copy(&mask, &truthy, &falsy);
    assert_eq!(output.data_type(), &DataType::Decimal128(5, 2));
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(155), Some(122), Some(-12)]
    );

    let truthy: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter([b"abcd", b"efgh", b"ijkl"].into_iter()).unwrap(),
    );
    let falsy: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter([b"1234", b"5678", b"9012"].into_iter()).unwrap(),
    );
    let output = copy(&mask, &truthy, &falsy);
    let array = output
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(array.value(0), b"abcd");
    assert_eq!(array.value(1), b"5678");
    assert_eq!(array.value(2), b"ijkl");
}

#[test]
fn zip_all_flat_primitive_domains_empty_and_all_null_sources_preserve_full_type() {
    let mut types = vec![
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Decimal32(9, -2),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 0),
        DataType::Decimal256(76, 4),
        DataType::FixedSizeBinary(0),
        DataType::FixedSizeBinary(16),
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(unit, None));
        types.push(DataType::Timestamp(unit, Some("".into())));
        types.push(DataType::Timestamp(unit, Some("UTC".into())));
        types.push(DataType::Duration(unit));
    }
    types.extend(byte_types());
    for ty in types {
        let truthy = new_null_array(&ty, 3);
        let falsy = new_null_array(&ty, 3);
        for mask in [
            BooleanArray::from(vec![true; 3]),
            BooleanArray::from(vec![false; 3]),
            BooleanArray::from(vec![None; 3]),
        ] {
            let output = copy(&mask, &truthy, &falsy);
            assert!(novarocks_type_contract::arrow_data_types_exact(
                output.data_type(),
                &ty
            ));
            assert_eq!(output.len(), 3);
            assert_eq!(output.null_count(), 3);
        }
        let output = copy(
            &BooleanArray::from(Vec::<bool>::new()),
            &truthy.slice(0, 0),
            &falsy.slice(0, 0),
        );
        assert!(novarocks_type_contract::arrow_data_types_exact(
            output.data_type(),
            &ty
        ));
        assert!(output.is_empty());
    }
}

#[test]
fn zip_four_byte_carriers_copy_selected_hidden_null_spans_and_ignore_inactive_payload() {
    let ignored = vec![b'x'; 320 * 1024];
    let mask = BooleanArray::new(
        arrow_buffer::BooleanBuffer::from(vec![false, true, true, false, true, false]),
        Some(NullBuffer::from(vec![true, true, false, true, true, true])),
    )
    .slice(1, 4);
    for ty in byte_types() {
        let truthy = bytes(
            &ty,
            &[
                &ignored,
                "α".as_bytes(),
                b"wrong-hidden-mask",
                b"ignore",
                b"hidden-null",
                &ignored,
            ],
            &[true, true, true, true, false, true],
        )
        .slice(1, 4);
        let falsy = bytes(
            &ty,
            &[
                &ignored,
                b"ignore",
                "尾".as_bytes(),
                b"\0",
                b"ignore",
                &ignored,
            ],
            &[true; 6],
        )
        .slice(1, 4);
        let output = copy(&mask, &truthy, &falsy);
        let expected: [&[u8]; 4] = ["α".as_bytes(), "尾".as_bytes(), b"\0", b"hidden-null"];
        assert_eq!(output.to_data().buffers()[1].as_slice(), expected.concat());
        for (row, value) in expected.iter().enumerate() {
            assert_eq!(byte_value(output.as_ref(), row), *value);
            assert_eq!(output.is_null(row), row == 3);
        }
        for all in [true, false] {
            let output = copy(&BooleanArray::from(vec![all; 4]), &truthy, &falsy);
            let source = if all { &truthy } else { &falsy };
            for row in 0..4 {
                assert_eq!(
                    byte_value(output.as_ref(), row),
                    byte_value(source.as_ref(), row)
                );
                assert_eq!(output.is_null(row), source.is_null(row));
            }
        }
    }
    let truthy = bytes(&DataType::Binary, &[&[0xff, 0x80], b""], &[true; 2]);
    let falsy = bytes(&DataType::Binary, &[b"", &[0, 0xff]], &[true; 2]);
    let output = copy(&BooleanArray::from(vec![true, false]), &truthy, &falsy);
    assert_eq!(
        output.to_data().buffers()[1].as_slice(),
        &[0xff, 0x80, 0, 0xff]
    );
}

#[test]
fn zip_actual_mask_runs_match_independent_mutable_buffer_growth_oracles() {
    let a = vec![b'a'; 80];
    let b = vec![b'b'; 65];
    let truthy = bytes(&DataType::Utf8, &[&a, &b], &[true; 2]);
    let falsy = bytes(&DataType::Utf8, &[b"", &b], &[true; 2]);
    // Initial rows hint 2 rounds to 64. The 80-byte first run grows to128;
    // the second run requests145 bytes and doubles to256, not just round192.
    let output = copy(&BooleanArray::from(vec![true, false]), &truthy, &falsy);
    assert_eq!(
        output.to_data().buffers()[1].as_slice(),
        [a.as_slice(), b.as_slice()].concat()
    );
    assert_eq!(output.to_data().buffers()[1].capacity(), 256);
    let output = copy(&BooleanArray::from(vec![true, true]), &truthy, &falsy);
    assert_eq!(output.to_data().buffers()[1].capacity(), 192);
    let mut extent = ByteBufferExtent::new(2, false).unwrap();
    extent.extend(80, false).unwrap();
    extent.extend(65, false).unwrap();
    assert_eq!((extent.len, extent.capacity), (145, 256));
}

#[test]
fn zip_exact_carrier_shape_and_encoded_constructor_boundaries_refuse_explicitly() {
    let mask = BooleanArray::from(vec![true]);
    let i64s = Int64Array::from(vec![1]);
    let i32s = Int32Array::from(vec![1]);
    assert!(matches!(
        preflight_zip(&mask, &i64s, &i32s, |_| Ok(())),
        Err(CopyError::Invalid(_))
    ));
    assert!(matches!(
        preflight_zip(&mask, &i64s, &Int64Array::from(vec![1, 2]), |_| Ok(())),
        Err(CopyError::Invalid(_))
    ));
    let empty = BooleanArray::from(Vec::<bool>::new());
    assert!(matches!(
        preflight_zip(&empty, &i64s, &i64s, |_| Ok(())),
        Err(CopyError::Invalid(_))
    ));
    let none = TimestampMicrosecondArray::from(vec![0]);
    let some_empty = TimestampMicrosecondArray::from(vec![0]).with_timezone("");
    let utc = TimestampMicrosecondArray::from(vec![0]).with_timezone("UTC");
    for other in [&some_empty, &utc] {
        assert!(matches!(
            preflight_zip(&mask, &none, other, |_| Ok(())),
            Err(CopyError::Invalid(_))
        ));
    }
    let decimal3 = Decimal128Array::from(vec![1])
        .with_precision_and_scale(3, 0)
        .unwrap();
    let decimal4 = Decimal128Array::from(vec![1])
        .with_precision_and_scale(4, 0)
        .unwrap();
    assert!(matches!(
        preflight_zip(&mask, &decimal3, &decimal4, |_| Ok(())),
        Err(CopyError::Invalid(_))
    ));
    let dict = || {
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(Int32Array::from((0..64).collect::<Vec<_>>())),
        )
        .unwrap()
    };
    // Both dictionaries are real valid arrays. The initial scope refuses the
    // carrier rather than entering the library's 64+64 constructor concat.
    let left = dict();
    let right = dict();
    assert!(matches!(
        preflight_zip(&mask, &left, &right, |_| Ok(())),
        Err(CopyError::Unsupported(_))
    ));
    let empty_left = left.slice(0, 0);
    let empty_right = right.slice(0, 0);
    assert!(matches!(
        preflight_zip(&empty, &empty_left, &empty_right, |_| Ok(())),
        Err(CopyError::Unsupported(_))
    ));
    for ty in [
        DataType::Null,
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        DataType::Struct(vec![Arc::new(Field::new("child", DataType::Int64, true))].into()),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int16, false)),
            Arc::new(Field::new("values", DataType::Int64, true)),
        ),
    ] {
        let array = new_empty_array(&ty);
        assert!(
            matches!(preflight_zip(&empty, array.as_ref(), array.as_ref(), |_| Ok(())), Err(CopyError::Unsupported(actual)) if actual.as_str() == CopyError::unsupported(&ty).to_string())
        );
    }
}

#[test]
fn zip_static_layout_offset_and_growth_exact_boundaries_need_no_giant_allocations() {
    let maximum_aligned = (isize::MAX as usize / 64) * 64;
    assert_eq!(rounded_capacity(maximum_aligned).unwrap(), maximum_aligned);
    assert!(matches!(
        rounded_capacity(maximum_aligned + 1),
        Err(CopyError::Extent(_))
    ));
    assert!(matches!(
        rounded_capacity(usize::MAX),
        Err(CopyError::Extent(_))
    ));
    flat_extent(&DataType::Int64, maximum_aligned / 8).unwrap();
    assert!(matches!(
        flat_extent(&DataType::Int64, maximum_aligned / 8 + 1),
        Err(CopyError::Extent(_))
    ));
    flat_extent(&DataType::FixedSizeBinary(16), maximum_aligned / 16).unwrap();
    assert!(matches!(
        flat_extent(&DataType::FixedSizeBinary(16), maximum_aligned / 16 + 1),
        Err(CopyError::Extent(_))
    ));
    let invalid_width = flat_extent(&DataType::FixedSizeBinary(-1), 0).unwrap_err();
    assert!(matches!(invalid_width, CopyError::Invalid(_)));
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&invalid_width).class(),
        novarocks_failure_contract::FailureClass::Invariant,
    );
    assert!(matches!(
        flat_extent(&DataType::LargeUtf8, usize::MAX),
        Err(CopyError::Extent(_))
    ));
    let mut extent = ByteBufferExtent::new(0, false).unwrap();
    if usize::BITS > 32 {
        extent.extend(i32::MAX as usize, false).unwrap();
        assert_eq!(extent.len, i32::MAX as usize);
        assert!(matches!(extent.extend(1, false), Err(CopyError::Extent(_))));
    } else {
        // The offset is legal, but its 64-byte-rounded allocation exceeds the
        // 32-bit Rust Layout limit before that offset can be materialized.
        assert!(matches!(
            extent.extend(i32::MAX as usize, false),
            Err(CopyError::Extent(_))
        ));
    }
    let mut extent = ByteBufferExtent::new(0, false).unwrap();
    extent.extend(maximum_aligned, true).unwrap();
    assert!(matches!(extent.extend(1, true), Err(CopyError::Extent(_))));
    // Required final bytes fit, but the real old-capacity doubling does not.
    let half = (isize::MAX as usize).div_ceil(2);
    let mut extent = ByteBufferExtent::new(half, false).unwrap();
    assert!(matches!(
        extent.extend(half + 1, true),
        Err(CopyError::Extent(_))
    ));
}

#[test]
fn zip_every_small_callback_preserves_all_seven_first_causes_and_ordinary_tails() {
    let mask = BooleanArray::from(vec![Some(true), None, Some(false)]);
    let truthy = bytes(
        &DataType::Utf8,
        &[b"a", b"hidden", b"c"],
        &[true, false, true],
    );
    let falsy = bytes(&DataType::Utf8, &[b"d", b"e", b"f"], &[true; 3]);
    let wrong = Int64Array::from(vec![1, 2, 3]);
    let short = Int64Array::from(vec![1]);
    for right in [falsy.as_ref(), &wrong as &dyn Array, &short as &dyn Array] {
        let mut baseline = vec![];
        let result = preflight_zip(&mask, truthy.as_ref(), right, |opaque| {
            baseline.push(opaque);
            Ok(())
        });
        assert_eq!(result.is_ok(), right.data_type() == &DataType::Utf8);
        assert_eq!(baseline.first(), Some(&true));
        assert_eq!(baseline.last(), Some(&true));
        for primary in failures() {
            for refused in 0..baseline.len() {
                let mut actual = vec![];
                let result = preflight_zip(&mask, truthy.as_ref(), right, |opaque| {
                    actual.push(opaque);
                    if actual.len() == refused + 1 {
                        Err(primary.clone())
                    } else {
                        Ok(())
                    }
                });
                assert!(matches!(result, Err(CopyError::Control(cause)) if cause == primary));
                assert_eq!(actual, baseline[..=refused]);
            }
        }
    }
    // Unsupported is an ordinary finish too, with the original refusal primary.
    let array = new_empty_array(&DataType::Utf8View);
    let mut baseline = vec![];
    assert!(matches!(
        preflight_zip(
            &BooleanArray::from(Vec::<bool>::new()),
            array.as_ref(),
            array.as_ref(),
            |opaque| {
                baseline.push(opaque);
                Ok(())
            }
        ),
        Err(CopyError::Unsupported(_))
    ));
    assert_eq!(baseline.last(), Some(&true));
    for primary in failures() {
        for refused in 0..baseline.len() {
            let mut actual = vec![];
            assert!(matches!(
                preflight_zip(
                    &BooleanArray::from(Vec::<bool>::new()),
                    array.as_ref(),
                    array.as_ref(),
                    |opaque| {
                        actual.push(opaque);
                        if actual.len() == refused + 1 {
                            Err(primary.clone())
                        } else {
                            Ok(())
                        }
                    }
                ),
                Err(CopyError::Control(cause)) if cause == primary
            ));
            assert_eq!(actual, baseline[..=refused]);
        }
    }
}

#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl crate::KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        calls.push(units);
        if let Some((refused, primary)) = &self.refusal
            && *refused == at
        {
            return Err(primary.clone());
        }
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("zip preflight never waits")
    }
}

#[test]
fn zip_real_wide_mask_run_walk_emits_256_and_preserves_original_meter_prefix() {
    let truthy = Arc::new(StringArray::from(vec!["a"; 320])) as ArrayRef;
    let falsy = Arc::new(StringArray::from(vec!["b"; 320])) as ArrayRef;
    let mask = BooleanArray::from((0..320).map(|row| row % 2 == 0).collect::<Vec<_>>());
    let run = |control: &Control| {
        let mut work = crate::kernel_input::EvaluationCheckpoints::new(control);
        preflight_zip(&mask, truthy.as_ref(), falsy.as_ref(), |opaque| {
            if opaque { work.flush() } else { work.step() }
        })
    };
    let baseline = Control::default();
    run(&baseline).unwrap();
    let expected = baseline.calls.lock().unwrap().clone();
    let quantum = expected.iter().position(|units| *units == 256).unwrap();
    for primary in failures() {
        for refused in [0, quantum, expected.len() - 1] {
            let control = Control {
                calls: Mutex::new(vec![]),
                refusal: Some((refused, primary.clone())),
            };
            assert!(matches!(run(&control), Err(CopyError::Control(cause)) if cause == primary));
            assert_eq!(*control.calls.lock().unwrap(), expected[..=refused]);
        }
    }
    let actual = copy(&mask, &truthy, &falsy);
    let array = actual.as_any().downcast_ref::<StringArray>().unwrap();
    for row in 0..320 {
        assert_eq!(array.value(row), if row % 2 == 0 { "a" } else { "b" });
    }
}

#[test]
fn zip_long_nullable_masks_keep_row_values_across_source_dispatch_paths() {
    // Independent row oracle covers both the original MutableArrayData path
    // and Arrow 60's fragmented-mask interleave path, including sliced masks.
    for rows in [1023, 1024, 1025, 2048] {
        for fragmented in [false, true] {
            let mask_values = (0..rows + 2)
                .map(|row| {
                    (row % 7 != 0).then_some(if fragmented {
                        row % 2 == 0
                    } else {
                        row < rows / 2
                    })
                })
                .collect::<Vec<_>>();
            let mask = BooleanArray::from(mask_values).slice(1, rows);
            let left_values = (0..rows)
                .map(|row| (row % 11 != 0).then(|| format!("truthy-{row}")))
                .collect::<Vec<_>>();
            let right_values = (0..rows)
                .map(|row| (row % 13 != 0).then(|| format!("falsy-{row}")))
                .collect::<Vec<_>>();
            let expected = (0..rows)
                .map(|row| {
                    if mask.is_valid(row) && mask.value(row) {
                        left_values[row].clone()
                    } else {
                        right_values[row].clone()
                    }
                })
                .collect::<Vec<_>>();
            let left: ArrayRef = Arc::new(StringArray::from(left_values));
            let right: ArrayRef = Arc::new(StringArray::from(right_values));
            let actual = copy(&mask, &left, &right);
            let actual = actual.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(
                actual
                    .iter()
                    .map(|value| value.map(str::to_owned))
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }
}
