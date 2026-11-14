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

use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarEvaluationInstance, SelectedValues, Selection,
};
use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int8Array, Int16Array, Int32Array, Int64Array,
    builder::FixedSizeBinaryBuilder,
};
use arrow_schema::DataType;
use novarocks_type_contract::{CompilePhase, DecimalOverflowPolicy, ValueLogicalType};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const NAMES: [&str; 4] = ["bitand", "bitor", "bitxor", "bitnot"];

#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl Control {
    fn refusing(index: usize, error: KernelFailure) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            refusal: Some((index, error)),
        }
    }
    fn calls(&self) -> Vec<u32> {
        self.calls.lock().unwrap().clone()
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len();
        calls.push(units);
        if let Some((at, error)) = &self.refusal
            && *at == index
        {
            return Err(error.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("bitwise kernels must not wait");
    }
}
fn ty(dtype: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(dtype, nullable)
}
fn pair_types(dtype: DataType, nullable: bool) -> [FunctionValueType; 2] {
    [ty(dtype.clone(), nullable), ty(dtype, nullable)]
}
fn large_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn instance(name: &str, sources: &[FunctionValueType]) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        super::super::bitwise_owner::prepared_for_test(name, sources).unwrap(),
    )
    .unwrap()
}
fn large_array(values: &[Option<i128>]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        match value {
            Some(value) => builder.append_value(value.to_be_bytes()).unwrap(),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}
fn signed_values(array: &ArrayRef) -> Vec<Option<i128>> {
    macro_rules! primitive {
        ($array:ty) => {{
            let array = array.as_any().downcast_ref::<$array>().unwrap();
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| i128::from(array.value(row))))
                .collect()
        }};
    }
    match array.data_type() {
        DataType::Int8 => primitive!(Int8Array),
        DataType::Int16 => primitive!(Int16Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int64 => primitive!(Int64Array),
        DataType::FixedSizeBinary(16) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            (0..array.len())
                .map(|row| {
                    (!array.is_null(row))
                        .then(|| i128::from_be_bytes(array.value(row).try_into().unwrap()))
                })
                .collect()
        }
        dtype => panic!("unexpected bitwise output: {dtype:?}"),
    }
}
fn dense(name: &str, source: FunctionValueType, arrays: &[ArrayRef]) -> ArrayRef {
    let sources = vec![source.clone(); arrays.len()];
    let mut kernel = instance(name, &sources);
    assert!(kernel.contract().result_type().nullable());
    assert_eq!(
        kernel.contract().result_type().logical_type(),
        source.logical_type()
    );
    assert_eq!(
        kernel.contract().result_type().data_type(),
        source.data_type()
    );
    let args: Vec<_> = arrays.iter().map(EvaluatedArgument::Column).collect();
    let result = kernel
        .evaluate(Selection::all(arrays[0].len()), &args, &Control::default())
        .unwrap();
    assert!(result.errors().is_empty());
    result.into_parts().1
}
fn pair(name: &str, source: FunctionValueType, left: ArrayRef, right: ArrayRef) -> ArrayRef {
    if name == "bitnot" {
        dense(name, source, &[left])
    } else {
        dense(name, source, &[left, right])
    }
}

#[test]
fn all_twenty_actual_records_have_independent_signed_oracles_and_exact_output() {
    let profiles: [(FunctionValueType, ArrayRef, ArrayRef); 5] = [
        (
            ty(DataType::Int8, false),
            Arc::new(Int8Array::from(vec![-6])),
            Arc::new(Int8Array::from(vec![3])),
        ),
        (
            ty(DataType::Int16, false),
            Arc::new(Int16Array::from(vec![-6])),
            Arc::new(Int16Array::from(vec![3])),
        ),
        (
            ty(DataType::Int32, false),
            Arc::new(Int32Array::from(vec![-6])),
            Arc::new(Int32Array::from(vec![3])),
        ),
        (
            ty(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![-6])),
            Arc::new(Int64Array::from(vec![3])),
        ),
        (
            large_type(false),
            large_array(&[Some(-6)]),
            large_array(&[Some(3)]),
        ),
    ];
    let mut records = 0;
    for (name, expected) in [("bitand", 2), ("bitor", -5), ("bitxor", -7), ("bitnot", 5)] {
        for (source, left, right) in &profiles {
            let output = pair(name, source.clone(), left.clone(), right.clone());
            assert_eq!(output.data_type(), source.data_type());
            assert_eq!(signed_values(&output), [Some(expected)]);
            assert_eq!(output.null_count(), 0);
            records += 1;
        }
    }
    assert_eq!(records, 20);
}

#[test]
fn minima_maxima_and_sign_extension_never_overflow_same_width_bitwise_output() {
    let profiles: [(FunctionValueType, ArrayRef, ArrayRef, i128, i128); 4] = [
        (
            ty(DataType::Int8, false),
            Arc::new(Int8Array::from(vec![i8::MIN, i8::MAX, -1, 0])),
            Arc::new(Int8Array::from(vec![i8::MAX, -1, 0, -1])),
            -128,
            127,
        ),
        (
            ty(DataType::Int16, false),
            Arc::new(Int16Array::from(vec![i16::MIN, i16::MAX, -1, 0])),
            Arc::new(Int16Array::from(vec![i16::MAX, -1, 0, -1])),
            -32768,
            32767,
        ),
        (
            ty(DataType::Int32, false),
            Arc::new(Int32Array::from(vec![i32::MIN, i32::MAX, -1, 0])),
            Arc::new(Int32Array::from(vec![i32::MAX, -1, 0, -1])),
            -2147483648,
            2147483647,
        ),
        (
            ty(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX, -1, 0])),
            Arc::new(Int64Array::from(vec![i64::MAX, -1, 0, -1])),
            -9223372036854775808,
            9223372036854775807,
        ),
    ];
    for (source, left, right, min, max) in profiles {
        for (name, expected) in [
            ("bitand", [Some(0), Some(max), Some(0), Some(0)]),
            ("bitor", [Some(-1), Some(-1), Some(-1), Some(-1)]),
            ("bitxor", [Some(-1), Some(min), Some(-1), Some(-1)]),
            ("bitnot", [Some(max), Some(min), Some(0), Some(-1)]),
        ] {
            let output = pair(name, source.clone(), left.clone(), right.clone());
            assert_eq!(signed_values(&output), expected);
            assert_eq!(output.null_count(), 0);
        }
    }
}

#[test]
fn largeint_preserves_all_big_endian_bits_including_high_half_and_signed_extremes() {
    let pattern = 0x0102030405060708090a0b0c0d0e0f10_i128;
    let left = large_array(&[
        Some(i128::MIN),
        Some(i128::MAX),
        Some(-1),
        Some(0),
        Some(pattern),
    ]);
    let right = large_array(&[Some(i128::MAX), Some(-1), Some(0), Some(-1), Some(255)]);
    for (name, expected) in [
        (
            "bitand",
            [Some(0), Some(i128::MAX), Some(0), Some(0), Some(16)],
        ),
        (
            "bitor",
            [
                Some(-1),
                Some(-1),
                Some(-1),
                Some(-1),
                Some(0x0102030405060708090a0b0c0d0e0fff),
            ],
        ),
        (
            "bitxor",
            [
                Some(-1),
                Some(i128::MIN),
                Some(-1),
                Some(-1),
                Some(0x0102030405060708090a0b0c0d0e0fef),
            ],
        ),
        (
            "bitnot",
            [
                Some(i128::MAX),
                Some(i128::MIN),
                Some(0),
                Some(-1),
                Some(-0x0102030405060708090a0b0c0d0e0f11),
            ],
        ),
    ] {
        let output = pair(name, large_type(false), left.clone(), right.clone());
        assert_eq!(signed_values(&output), expected);
        let array = output
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(array.value(4), expected[4].unwrap().to_be_bytes());
    }
}

#[test]
fn real_binder_refuses_ghost_binary_bitnot_and_noninstalled_logical_domains() {
    let signed = ty(DataType::Int8, false);
    assert!(
        super::super::bitwise_owner::prepared_for_test("bitnot", &[signed.clone(), signed.clone()])
            .is_err()
    );
    assert!(super::super::bitwise_owner::prepared_for_test("bitnot", &[]).is_err());
    for name in NAMES {
        for bad in [
            ty(DataType::UInt64, false),
            ty(DataType::FixedSizeBinary(16), false),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
        ] {
            let args = if name == "bitnot" {
                vec![bad]
            } else {
                vec![bad.clone(), bad]
            };
            assert!(super::super::bitwise_owner::prepared_for_test(name, &args).is_err());
        }
        if name != "bitnot" {
            assert!(
                super::super::bitwise_owner::prepared_for_test(name, std::slice::from_ref(&signed))
                    .is_err()
            );
        }
    }
}

#[test]
fn sparse_dense_and_compact_arguments_map_independently_on_both_sides() {
    let rows = [1, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let left: ArrayRef = Arc::new(Int16Array::from(vec![
        None,
        Some(-6),
        None,
        None,
        Some(5),
        None,
    ]));
    let right: ArrayRef = Arc::new(Int16Array::from(vec![3, -3]));
    let compact =
        SelectedValues::try_new(selection, &DataType::Int16, right, Box::default()).unwrap();
    for (name, expected) in [
        ("bitand", [Some(2), Some(5)]),
        ("bitor", [Some(-5), Some(-3)]),
        ("bitxor", [Some(-7), Some(-8)]),
    ] {
        let mut kernel = instance(name, &pair_types(DataType::Int16, false));
        let args = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::SelectedColumn(&compact),
        ];
        let output = kernel
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(output.selection(), selection);
        assert_eq!(signed_values(output.values()), expected);
    }
    let right: ArrayRef = Arc::new(Int16Array::from(vec![
        None,
        Some(3),
        None,
        None,
        Some(-3),
        None,
    ]));
    let left: ArrayRef = Arc::new(Int16Array::from(vec![-6, 5]));
    let compact =
        SelectedValues::try_new(selection, &DataType::Int16, left, Box::default()).unwrap();
    for (name, expected) in [
        ("bitand", [Some(2), Some(5)]),
        ("bitor", [Some(-5), Some(-3)]),
        ("bitxor", [Some(-7), Some(-8)]),
    ] {
        let mut kernel = instance(name, &pair_types(DataType::Int16, false));
        let args = [
            EvaluatedArgument::SelectedColumn(&compact),
            EvaluatedArgument::Column(&right),
        ];
        assert_eq!(
            signed_values(
                kernel
                    .evaluate(selection, &args, &Control::default())
                    .unwrap()
                    .values()
            ),
            expected
        );
    }
    let mut not = instance("bitnot", &[ty(DataType::Int16, false)]);
    let args = [EvaluatedArgument::SelectedColumn(&compact)];
    assert_eq!(
        signed_values(
            not.evaluate(selection, &args, &Control::default())
                .unwrap()
                .values()
        ),
        [Some(5), Some(-6)]
    );
}

#[test]
fn scalar_broadcast_is_explicit_for_either_argument_and_not_for_one_row_columns() {
    let left: ArrayRef = Arc::new(Int8Array::from(vec![-6]));
    let right: ArrayRef = Arc::new(Int8Array::from(vec![3, -3]));
    for (name, expected) in [
        ("bitand", [Some(2), Some(-8)]),
        ("bitor", [Some(-5), Some(-1)]),
        ("bitxor", [Some(-7), Some(7)]),
    ] {
        let mut kernel = instance(name, &pair_types(DataType::Int8, false));
        let args = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Column(&right),
        ];
        assert_eq!(
            signed_values(
                kernel
                    .evaluate(Selection::all(2), &args, &Control::default())
                    .unwrap()
                    .values()
            ),
            expected
        );
        let mut kernel = instance(name, &pair_types(DataType::Int8, false));
        let args = [
            EvaluatedArgument::Column(&right),
            EvaluatedArgument::Scalar(&left),
        ];
        assert_eq!(
            signed_values(
                kernel
                    .evaluate(Selection::all(2), &args, &Control::default())
                    .unwrap()
                    .values()
            ),
            expected
        );
        let mut kernel = instance(name, &pair_types(DataType::Int8, false));
        let args = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&right),
        ];
        assert!(matches!(
            kernel.evaluate(Selection::all(2), &args, &Control::default()),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
    }
    let mut not = instance("bitnot", &[ty(DataType::Int8, false)]);
    assert_eq!(
        signed_values(
            not.evaluate(
                Selection::all(3),
                &[EvaluatedArgument::Scalar(&left)],
                &Control::default()
            )
            .unwrap()
            .values()
        ),
        [Some(5); 3]
    );
}

#[test]
fn null_is_strict_on_each_selected_side_without_row_error_or_narrowing_overflow() {
    let left: ArrayRef = Arc::new(Int32Array::from(vec![Some(-6), None, Some(5), None]));
    let right: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), Some(3), None, None]));
    for (name, first) in [("bitand", 2), ("bitor", -5), ("bitxor", -7)] {
        let output = pair(name, ty(DataType::Int32, true), left.clone(), right.clone());
        assert_eq!(signed_values(&output), [Some(first), None, None, None]);
    }
    assert_eq!(
        signed_values(&dense("bitnot", ty(DataType::Int32, true), &[left])),
        [Some(5), None, Some(-6), None]
    );
    let output = pair(
        "bitxor",
        large_type(true),
        large_array(&[None, Some(i128::MIN)]),
        large_array(&[Some(-1), None]),
    );
    assert_eq!(signed_values(&output), [None, None]);
}

#[test]
fn slice_offsets_and_duplicate_values_keep_partition_invariance_for_all_four_ops() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![999, -6, -6, i64::MIN, i64::MIN, 999]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![999, 3, 3, i64::MAX, i64::MAX, 999]));
    let left = left.slice(1, 4);
    let right = right.slice(1, 4);
    for name in NAMES {
        let whole = signed_values(&pair(
            name,
            ty(DataType::Int64, false),
            left.clone(),
            right.clone(),
        ));
        let arity = if name == "bitnot" { 1 } else { 2 };
        let mut kernel = instance(name, &vec![ty(DataType::Int64, false); arity]);
        let mut parts = Vec::new();
        for (start, len) in [(0, 1), (1, 2), (3, 1)] {
            let l = left.slice(start, len);
            let r = right.slice(start, len);
            let mut args = vec![EvaluatedArgument::Column(&l)];
            if arity == 2 {
                args.push(EvaluatedArgument::Column(&r));
            }
            parts.extend(signed_values(
                kernel
                    .evaluate(Selection::all(len), &args, &Control::default())
                    .unwrap()
                    .values(),
            ));
        }
        assert_eq!(whole, parts);
        assert_eq!(whole[0], whole[1]);
        assert_eq!(whole[2], whole[3]);
    }
}

fn pool(ty: FunctionValueType, array: ArrayRef) -> ConstantPool {
    let rows = u64::try_from(array.len()).unwrap();
    let policy = ConstantPolicy {
        max_rows: rows,
        max_array_nodes: 1,
        max_logical_elements: rows,
        max_retained_buffer_bytes: 1024,
        max_type_depth: 1,
        max_type_nodes: 1,
        max_dictionary_depth: 0,
        max_metadata_bytes: 1024,
        max_library_validation_work: 4096,
        max_library_validation_bytes: 8192,
    };
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("bitwise").unwrap()),
        ty,
        array.to_data(),
        policy,
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap()
}

#[test]
fn constant_pool_ordinals_are_independent_and_never_infer_largeint_from_binary() {
    let pool = pool(large_type(true), large_array(&[None, Some(-6), Some(3)]));
    let left = pool.value(1).unwrap();
    let right = pool.value(2).unwrap();
    assert!(Arc::ptr_eq(left.pool().array(), right.pool().array()));
    for (name, expected) in [("bitand", 2), ("bitor", -5), ("bitxor", -7), ("bitnot", 5)] {
        let arity = if name == "bitnot" { 1 } else { 2 };
        let mut kernel = instance(name, &vec![large_type(true); arity]);
        let mut args = vec![EvaluatedArgument::Constant(&left)];
        if arity == 2 {
            args.push(EvaluatedArgument::Constant(&right));
        }
        let output = kernel
            .evaluate(Selection::all(3), &args, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(signed_values(output.values()), [Some(expected); 3]);
    }
    let physical = self::pool(
        ty(DataType::FixedSizeBinary(16), false),
        large_array(&[Some(-6)]),
    );
    let impostor = physical.value(0).unwrap();
    let mut not = instance("bitnot", &[large_type(true)]);
    assert!(matches!(
        not.evaluate(
            Selection::all(1),
            &[EvaluatedArgument::Constant(&impostor)],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn selected_null_cannot_mask_wrong_carrier_cardinality_or_contradictory_nullability() {
    let left: ArrayRef = Arc::new(Int8Array::from(vec![None, Some(1)]));
    let short: ArrayRef = Arc::new(Int8Array::from(vec![1]));
    let wrong: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
    let null: ArrayRef = Arc::new(Int8Array::from(vec![None, Some(1)]));
    for right in [&short, &wrong, &null] {
        let mut kernel = instance(
            "bitand",
            &[ty(DataType::Int8, true), ty(DataType::Int8, false)],
        );
        let args = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(right),
        ];
        assert!(matches!(
            kernel.evaluate(Selection::all(2), &args, &Control::default()),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
        assert_eq!(
            kernel
                .evaluate(Selection::all(0), &[], &Control::default())
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
    }
    let mut not = instance("bitnot", &[ty(DataType::Int8, false)]);
    assert!(matches!(
        not.evaluate(
            Selection::all(2),
            &[EvaluatedArgument::Column(&left)],
            &Control::default()
        ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn controller_poison_and_foreign_compact_selection_remain_outer_errors() {
    let rows = [1];
    let other_rows = [2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let foreign = Selection::try_sparse(3, &other_rows).unwrap();
    let values: ArrayRef = Arc::new(Int8Array::from(vec![None]));
    let poisoned = SelectedValues::try_new(
        selection,
        &DataType::Int8,
        values.clone(),
        vec![crate::RowDataError::new(0, "child failure")].into_boxed_slice(),
    )
    .unwrap();
    let wrong =
        SelectedValues::try_new(foreign, &DataType::Int8, values.clone(), Box::default()).unwrap();
    for compact in [&poisoned, &wrong] {
        let mut kernel = instance("bitxor", &pair_types(DataType::Int8, true));
        let args = [
            EvaluatedArgument::Scalar(&values),
            EvaluatedArgument::SelectedColumn(compact),
        ];
        assert!(matches!(
            kernel.evaluate(selection, &args, &Control::default()),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
    }
    let mut not = instance("bitnot", &[ty(DataType::Int8, true)]);
    assert!(matches!(
        not.evaluate(
            selection,
            &[EvaluatedArgument::SelectedColumn(&poisoned)],
            &Control::default()
        ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    assert!(Selection::try_sparse(1, &[1]).is_err());
    let mut not = instance("bitnot", &[ty(DataType::Int8, true)]);
    assert!(matches!(
        not.evaluate(Selection::all(1), &[], &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn both_frozen_policies_are_identical_total_bitwise_operations_without_row_errors() {
    let left: ArrayRef = Arc::new(Int8Array::from(vec![i8::MIN, i8::MAX]));
    let right: ArrayRef = Arc::new(Int8Array::from(vec![-1, -1]));
    for (name, expected) in [
        ("bitand", [Some(-128), Some(127)]),
        ("bitor", [Some(-1), Some(-1)]),
        ("bitxor", [Some(127), Some(-128)]),
        ("bitnot", [Some(127), Some(-128)]),
    ] {
        let arity = if name == "bitnot" { 1 } else { 2 };
        let types = vec![ty(DataType::Int8, false); arity];
        for policy in [
            DecimalOverflowPolicy::ReportError,
            DecimalOverflowPolicy::OutputNull,
        ] {
            let prepared =
                super::super::bitwise_owner::prepared_for_test_with_policy(name, &types, policy)
                    .unwrap();
            let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            assert_eq!(kernel.contract().decimal_overflow_policy(), policy);
            let mut args = vec![EvaluatedArgument::Column(&left)];
            if arity == 2 {
                args.push(EvaluatedArgument::Column(&right));
            }
            let output = kernel
                .evaluate(Selection::all(2), &args, &Control::default())
                .unwrap();
            assert!(output.errors().is_empty());
            assert_eq!(signed_values(output.values()), expected);
            assert_eq!(output.values().null_count(), 0);
        }
    }
}

#[test]
fn empty_demand_skips_body_and_unreachable_nonnullable_null_values() {
    let value: ArrayRef = Arc::new(Int8Array::from(vec![None]));
    for name in NAMES {
        let arity = if name == "bitnot" { 1 } else { 2 };
        let mut kernel = instance(name, &vec![ty(DataType::Int8, false); arity]);
        let args = vec![EvaluatedArgument::Scalar(&value); arity];
        let control = Control::default();
        let output = kernel.evaluate(Selection::all(0), &args, &control).unwrap();
        assert!(output.values().is_empty());
        assert_eq!(output.values().data_type(), &DataType::Int8);
        assert!(output.errors().is_empty());
        assert_eq!(
            control.calls().iter().filter(|n| **n == 0).count(),
            arity + 1
        );
    }
}

#[test]
fn actual_entry_body_quantum_tail_and_publication_refusals_latch_all_four_instances() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![-6]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![3]));
    let selection = Selection::all(320);
    for name in NAMES {
        let arity = if name == "bitnot" { 1 } else { 2 };
        let types = vec![ty(DataType::Int64, true); arity];
        let mut args = vec![EvaluatedArgument::Scalar(&left)];
        if arity == 2 {
            args.push(EvaluatedArgument::Scalar(&right));
        }
        let mut baseline = instance(name, &types);
        let trace = Control::default();
        baseline.evaluate(selection, &args, &trace).unwrap();
        let calls = trace.calls();
        let body = calls
            .iter()
            .enumerate()
            .filter(|(_, n)| **n == 0)
            .nth(arity + 1)
            .unwrap()
            .0;
        let quantum = calls.iter().position(|n| *n == 256).unwrap();
        assert!(body < quantum);
        assert_eq!(calls[quantum + 1], 64);
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for at in [0, body, quantum, quantum + 1, calls.len() - 1] {
                let mut kernel = instance(name, &types);
                let control = Control::refusing(at, error.clone());
                assert_eq!(
                    kernel.evaluate(selection, &args, &control).unwrap_err(),
                    error
                );
                assert_eq!(control.calls().len(), at + 1);
                let after = Control::default();
                assert_eq!(
                    kernel.evaluate(selection, &args, &after).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.calls().is_empty());
            }
        }
    }
}

#[test]
fn allocation_representability_overflow_precedes_any_selected_row_read() {
    for source in [
        ty(DataType::Int8, true),
        ty(DataType::Int64, true),
        large_type(true),
    ] {
        let value: ArrayRef = match source.data_type() {
            DataType::Int8 => Arc::new(Int8Array::from(vec![None])),
            DataType::Int64 => Arc::new(Int64Array::from(vec![None])),
            _ => large_array(&[None]),
        };
        for name in NAMES {
            let arity = if name == "bitnot" { 1 } else { 2 };
            let mut kernel = instance(name, &vec![source.clone(); arity]);
            let args = vec![EvaluatedArgument::Scalar(&value); arity];
            let control = Control::default();
            assert_eq!(
                kernel
                    .evaluate(Selection::all(usize::MAX), &args, &control)
                    .unwrap_err(),
                KernelFailure::ResourceExhausted
            );
            assert!(control.calls().iter().all(|n| *n < 256));
            assert_eq!(
                control.calls().iter().filter(|n| **n == 0).count(),
                arity + 2
            );
        }
    }
}

mod shared_core;
