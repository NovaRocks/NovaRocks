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
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionValueType, ScalarEvaluationInstance,
    Selection,
};
use arrow_array::{Int8Array, Int16Array, Int32Array};
use novarocks_type_contract::{CompilePhase, DecimalOverflowPolicy};
use std::{sync::Mutex, time::Duration};

const NAMES: [&str; 3] = [
    "bit_shift_left",
    "bit_shift_right",
    "bit_shift_right_logical",
];

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
        panic!("shift kernels must not wait");
    }
}
fn ty(dtype: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(dtype, nullable)
}
fn large_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn instance(name: &str, sources: &[FunctionValueType; 2]) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        super::super::bit_shift_owner::prepared_for_test(name, sources).unwrap(),
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
        dtype => panic!("unexpected shift output: {dtype:?}"),
    }
}
fn dense(name: &str, source: FunctionValueType, left: ArrayRef, right: ArrayRef) -> ArrayRef {
    let sources = [source.clone(), ty(DataType::Int64, true)];
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
    let args = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&right),
    ];
    let output = kernel
        .evaluate(Selection::all(left.len()), &args, &Control::default())
        .unwrap();
    assert!(output.errors().is_empty());
    output.into_parts().1
}

#[test]
fn all_fifteen_installed_records_preserve_the_exact_output_domain() {
    let profiles: [(FunctionValueType, ArrayRef); 5] = [
        (
            ty(DataType::Int8, false),
            Arc::new(Int8Array::from(vec![12])),
        ),
        (
            ty(DataType::Int16, false),
            Arc::new(Int16Array::from(vec![12])),
        ),
        (
            ty(DataType::Int32, false),
            Arc::new(Int32Array::from(vec![12])),
        ),
        (
            ty(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![12])),
        ),
        (large_type(false), large_array(&[Some(12)])),
    ];
    let mut records = 0;
    for (name, expected) in [(NAMES[0], 48), (NAMES[1], 3), (NAMES[2], 3)] {
        for (source, left) in &profiles {
            let output = dense(
                name,
                source.clone(),
                left.clone(),
                Arc::new(Int64Array::from(vec![2])),
            );
            assert_eq!(output.data_type(), source.data_type());
            assert_eq!(signed_values(&output), [Some(expected)]);
            records += 1;
        }
    }
    assert_eq!(records, 15);
}

#[test]
fn narrow_left_shift_widens_to_i64_then_safe_narrows_instead_of_native_wrapping() {
    for (source, left, count) in [
        (
            ty(DataType::Int8, false),
            Arc::new(Int8Array::from(vec![1, 1, -1, -1])) as ArrayRef,
            8,
        ),
        (
            ty(DataType::Int16, false),
            Arc::new(Int16Array::from(vec![1, 1, -1, -1])) as ArrayRef,
            16,
        ),
        (
            ty(DataType::Int32, false),
            Arc::new(Int32Array::from(vec![1, 1, -1, -1])) as ArrayRef,
            32,
        ),
    ] {
        let counts: ArrayRef = Arc::new(Int64Array::from(vec![count, 64, count, 64]));
        assert_eq!(
            signed_values(&dense(NAMES[0], source, left, counts)),
            [None, Some(1), None, Some(-1)]
        );
    }
}

#[test]
fn negative_logical_right_narrowing_is_null_until_the_wide_result_fits() {
    let profiles: [(FunctionValueType, ArrayRef); 3] = [
        (
            ty(DataType::Int8, false),
            Arc::new(Int8Array::from(vec![-1; 4])),
        ),
        (
            ty(DataType::Int16, false),
            Arc::new(Int16Array::from(vec![-1; 4])),
        ),
        (
            ty(DataType::Int32, false),
            Arc::new(Int32Array::from(vec![-1; 4])),
        ),
    ];
    for (source, left) in profiles {
        let counts: ArrayRef = Arc::new(Int64Array::from(vec![1, 63, 64, -1]));
        assert_eq!(
            signed_values(&dense(
                NAMES[2],
                source.clone(),
                left.clone(),
                counts.clone()
            )),
            [None, Some(1), Some(-1), Some(1)]
        );
        assert_eq!(
            signed_values(&dense(NAMES[1], source, left, counts)),
            [Some(-1); 4]
        );
    }
}

#[test]
fn i64_counts_are_wrapped_u32_and_arithmetic_uses_all_sixty_four_bits() {
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![
        1,
        -1,
        -64,
        i64::MIN,
        i64::MAX,
        64,
        65,
    ]));
    let left: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX, 1, 7, 7, -1, 7, -8]));
    for (name, expected) in [
        (
            NAMES[0],
            vec![
                Some(-2),
                Some(i64::MIN as i128),
                Some(7),
                Some(7),
                Some(i64::MIN as i128),
                Some(7),
                Some(-16),
            ],
        ),
        (
            NAMES[1],
            vec![
                Some(4611686018427387903),
                Some(0),
                Some(7),
                Some(7),
                Some(-1),
                Some(7),
                Some(-4),
            ],
        ),
        (
            NAMES[2],
            vec![
                Some(4611686018427387903),
                Some(0),
                Some(7),
                Some(7),
                Some(1),
                Some(7),
                Some(9223372036854775804),
            ],
        ),
    ] {
        assert_eq!(
            signed_values(&dense(
                name,
                ty(DataType::Int64, false),
                left.clone(),
                counts.clone()
            )),
            expected
        );
    }
}

#[test]
fn largeint_uses_big_endian_bytes_and_full_128_bit_wrapping() {
    let left = large_array(&[
        Some(i128::MIN),
        Some(i128::MAX),
        Some(-1),
        Some(-1),
        Some(1),
        None,
    ]);
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(1),
        Some(127),
        Some(128),
        Some(-1),
        Some(1),
    ]));
    for (name, expected) in [
        (
            NAMES[0],
            vec![
                Some(0),
                Some(-2),
                Some(i128::MIN),
                Some(-1),
                Some(i128::MIN),
                None,
            ],
        ),
        (
            NAMES[1],
            vec![
                Some(-85070591730234615865843651857942052864),
                Some(85070591730234615865843651857942052863),
                Some(-1),
                Some(-1),
                Some(0),
                None,
            ],
        ),
        (
            NAMES[2],
            vec![
                Some(85070591730234615865843651857942052864),
                Some(85070591730234615865843651857942052863),
                Some(1),
                Some(-1),
                Some(0),
                None,
            ],
        ),
    ] {
        assert_eq!(
            signed_values(&dense(name, large_type(true), left.clone(), counts.clone())),
            expected
        );
    }
}

#[test]
fn fixed_binary_uuid_and_unsigned_are_not_inferred_as_numeric_domains() {
    for name in NAMES {
        for source in [
            ty(DataType::FixedSizeBinary(16), false),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
            ty(DataType::UInt64, false),
        ] {
            assert!(
                super::super::bit_shift_owner::prepared_for_test(
                    name,
                    &[source, ty(DataType::Int64, false)]
                )
                .is_err()
            );
        }
    }
}

#[test]
fn sparse_dense_and_compact_arguments_have_independent_selected_maps() {
    let rows = [1, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let left: ArrayRef = Arc::new(Int16Array::from(vec![
        None,
        Some(3),
        None,
        None,
        Some(5),
        None,
    ]));
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![2, 1]));
    let compact =
        SelectedValues::try_new(selection, &DataType::Int64, counts, Box::default()).unwrap();
    let args = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::SelectedColumn(&compact),
    ];
    let mut kernel = instance(
        NAMES[0],
        &[ty(DataType::Int16, false), ty(DataType::Int64, false)],
    );
    let output = kernel
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert_eq!(output.selection(), selection);
    assert_eq!(signed_values(output.values()), [Some(12), Some(10)]);

    let left: ArrayRef = Arc::new(Int16Array::from(vec![-8, -4]));
    let compact =
        SelectedValues::try_new(selection, &DataType::Int16, left, Box::default()).unwrap();
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![
        None,
        Some(1),
        None,
        None,
        Some(2),
        None,
    ]));
    let args = [
        EvaluatedArgument::SelectedColumn(&compact),
        EvaluatedArgument::Column(&counts),
    ];
    let mut kernel = instance(
        NAMES[1],
        &[ty(DataType::Int16, false), ty(DataType::Int64, false)],
    );
    assert_eq!(
        signed_values(
            kernel
                .evaluate(selection, &args, &Control::default())
                .unwrap()
                .values()
        ),
        [Some(-4), Some(-1)]
    );
}

#[test]
fn each_side_broadcast_and_selected_null_propagation_are_explicit() {
    let left: ArrayRef = Arc::new(Int8Array::from(vec![3]));
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]));
    let args = [
        EvaluatedArgument::Scalar(&left),
        EvaluatedArgument::Column(&counts),
    ];
    let mut kernel = instance(
        NAMES[0],
        &[ty(DataType::Int8, false), ty(DataType::Int64, true)],
    );
    assert_eq!(
        signed_values(
            kernel
                .evaluate(Selection::all(3), &args, &Control::default())
                .unwrap()
                .values()
        ),
        [Some(6), None, Some(12)]
    );
    let left: ArrayRef = Arc::new(Int8Array::from(vec![Some(-8), None, Some(-4)]));
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let args = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Scalar(&counts),
    ];
    let mut kernel = instance(
        NAMES[1],
        &[ty(DataType::Int8, true), ty(DataType::Int64, false)],
    );
    assert_eq!(
        signed_values(
            kernel
                .evaluate(Selection::all(3), &args, &Control::default())
                .unwrap()
                .values()
        ),
        [Some(-4), None, Some(-2)]
    );
}

#[test]
fn sliced_arrays_keep_offsets_and_duplicate_values_are_partition_invariant() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![999, -8, -8, 7, 7, 999]));
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![999, 1, 1, 64, 64, 999]));
    let left = left.slice(1, 4);
    let counts = counts.slice(1, 4);
    for name in NAMES {
        let source = ty(DataType::Int64, false);
        let whole = signed_values(&dense(name, source.clone(), left.clone(), counts.clone()));
        let mut kernel = instance(name, &[source, ty(DataType::Int64, false)]);
        let mut split = Vec::new();
        for (start, len) in [(0, 1), (1, 2), (3, 1)] {
            let l = left.slice(start, len);
            let r = counts.slice(start, len);
            let args = [EvaluatedArgument::Column(&l), EvaluatedArgument::Column(&r)];
            split.extend(signed_values(
                kernel
                    .evaluate(Selection::all(len), &args, &Control::default())
                    .unwrap()
                    .values(),
            ));
        }
        assert_eq!(whole, split);
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
        Arc::new(ty.try_to_field("shift").unwrap()),
        ty,
        array.to_data(),
        policy,
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap()
}

#[test]
fn constants_keep_nonzero_ordinals_and_exact_largeint_source_identity() {
    let lpool = pool(large_type(true), large_array(&[None, Some(-1), Some(12)]));
    let rpool = pool(
        ty(DataType::Int64, true),
        Arc::new(Int64Array::from(vec![None, Some(2), Some(127)])),
    );
    let left = lpool.value(1).unwrap();
    let right = rpool.value(2).unwrap();
    let args = [
        EvaluatedArgument::Constant(&left),
        EvaluatedArgument::Constant(&right),
    ];
    let mut kernel = instance(NAMES[2], &[large_type(true), ty(DataType::Int64, true)]);
    assert_eq!(
        signed_values(
            kernel
                .evaluate(Selection::all(3), &args, &Control::default())
                .unwrap()
                .values()
        ),
        [Some(1); 3]
    );
    assert_eq!(left.ordinal(), 1);
    assert_eq!(right.ordinal(), 2);

    let impostor = pool(
        ty(DataType::FixedSizeBinary(16), false),
        large_array(&[Some(12)]),
    );
    let wrong = impostor.value(0).unwrap();
    let args = [
        EvaluatedArgument::Constant(&wrong),
        EvaluatedArgument::Constant(&right),
    ];
    let mut kernel = instance(NAMES[0], &[large_type(true), ty(DataType::Int64, true)]);
    assert!(matches!(
        kernel.evaluate(Selection::all(1), &args, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn strict_null_cannot_hide_wrong_shape_carrier_or_selected_nonnull_violation() {
    let left: ArrayRef = Arc::new(Int8Array::from(vec![None, Some(1)]));
    let valid_count: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let wrong_count: ArrayRef = Arc::new(Int32Array::from(vec![1, 1]));
    for args in [
        [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&valid_count),
        ],
        [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&wrong_count),
        ],
    ] {
        let mut kernel = instance(
            NAMES[0],
            &[ty(DataType::Int8, true), ty(DataType::Int64, false)],
        );
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
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1)]));
    let args = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&counts),
    ];
    let mut kernel = instance(
        NAMES[0],
        &[ty(DataType::Int8, true), ty(DataType::Int64, false)],
    );
    assert!(matches!(
        kernel.evaluate(Selection::all(2), &args, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn compact_selection_mismatch_and_argument_cardinality_are_outer_errors() {
    let selected_rows = [1, 4];
    let wrong_rows = [0, 3];
    let selection = Selection::try_sparse(6, &selected_rows).unwrap();
    let wrong_selection = Selection::try_sparse(6, &wrong_rows).unwrap();
    let left: ArrayRef = Arc::new(Int8Array::from(vec![1, 2]));
    let compact =
        SelectedValues::try_new(wrong_selection, &DataType::Int8, left, Box::default()).unwrap();
    let count: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let args = [
        EvaluatedArgument::SelectedColumn(&compact),
        EvaluatedArgument::Scalar(&count),
    ];
    let types = [ty(DataType::Int8, false), ty(DataType::Int64, false)];
    let mut kernel = instance(NAMES[0], &types);
    assert!(matches!(
        kernel.evaluate(selection, &args, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let mut kernel = instance(NAMES[0], &types);
    assert!(matches!(
        kernel.evaluate(Selection::all(1), &args[..1], &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    assert!(Selection::try_sparse(1, &[1]).is_err());
    assert!(Selection::try_sparse(2, &[1, 1]).is_err());
}

#[test]
fn both_frozen_overflow_policies_return_safe_null_without_row_errors() {
    for name in NAMES {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let types = [ty(DataType::Int8, false), ty(DataType::Int64, false)];
            let prepared =
                super::super::bit_shift_owner::prepared_for_test_with_policy(name, &types, policy)
                    .unwrap();
            let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            assert_eq!(kernel.contract().decimal_overflow_policy(), policy);
            let left: ArrayRef = Arc::new(Int8Array::from(vec![1, -1]));
            let counts: ArrayRef = Arc::new(Int64Array::from(vec![8, 1]));
            let args = [
                EvaluatedArgument::Column(&left),
                EvaluatedArgument::Column(&counts),
            ];
            let output = kernel
                .evaluate(Selection::all(2), &args, &Control::default())
                .unwrap();
            assert!(output.errors().is_empty());
            let expected = match name {
                "bit_shift_left" => [None, Some(-2)],
                "bit_shift_right" => [Some(0), Some(-1)],
                "bit_shift_right_logical" => [Some(0), None],
                _ => unreachable!(),
            };
            assert_eq!(signed_values(output.values()), expected);
        }
    }
}

#[test]
fn empty_demand_does_not_enter_private_body_or_touch_unreachable_nulls() {
    let left: ArrayRef = Arc::new(Int8Array::from(vec![None]));
    let count: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    let args = [
        EvaluatedArgument::Scalar(&left),
        EvaluatedArgument::Scalar(&count),
    ];
    for name in NAMES {
        let mut kernel = instance(
            name,
            &[ty(DataType::Int8, false), ty(DataType::Int64, false)],
        );
        let control = Control::default();
        let result = kernel.evaluate(Selection::all(0), &args, &control).unwrap();
        assert!(result.values().is_empty());
        assert_eq!(result.values().data_type(), &DataType::Int8);
        assert!(result.errors().is_empty());
        assert_eq!(control.calls().iter().filter(|n| **n == 0).count(), 3);
    }
}

#[test]
fn actual_entry_body_256_tail_and_publication_refusals_poison_the_instance() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![12]));
    let count: ArrayRef = Arc::new(Int64Array::from(vec![2]));
    let args = [
        EvaluatedArgument::Scalar(&left),
        EvaluatedArgument::Scalar(&count),
    ];
    let types = [ty(DataType::Int64, true), ty(DataType::Int64, true)];
    let selection = Selection::all(320);
    for name in NAMES {
        let mut baseline = instance(name, &types);
        let trace = Control::default();
        baseline.evaluate(selection, &args, &trace).unwrap();
        let calls = trace.calls();
        let body = calls
            .iter()
            .enumerate()
            .filter(|(_, n)| **n == 0)
            .nth(3)
            .unwrap()
            .0;
        let interior = calls.iter().position(|n| *n == 256).unwrap();
        assert!(interior > body);
        assert_eq!(calls[interior + 1], 64);
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for at in [0, body, interior, interior + 1, calls.len() - 1] {
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
fn representability_overflow_is_rejected_before_allocation_and_any_row_work() {
    for source in [
        ty(DataType::Int8, true),
        ty(DataType::Int64, true),
        large_type(true),
    ] {
        let left: ArrayRef = match source.data_type() {
            DataType::Int8 => Arc::new(Int8Array::from(vec![None])),
            DataType::Int64 => Arc::new(Int64Array::from(vec![None])),
            _ => large_array(&[None]),
        };
        let count: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let args = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&count),
        ];
        for name in NAMES {
            let mut kernel = instance(name, &[source.clone(), ty(DataType::Int64, true)]);
            let control = Control::default();
            assert_eq!(
                kernel
                    .evaluate(Selection::all(usize::MAX), &args, &control)
                    .unwrap_err(),
                KernelFailure::ResourceExhausted
            );
            assert!(control.calls().iter().all(|n| *n < 256));
            assert_eq!(control.calls().iter().filter(|n| **n == 0).count(), 4);
        }
    }
}

#[test]
fn controller_must_resolve_child_row_errors_before_even_empty_or_null_demand() {
    let rows = [1];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let count: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    let poisoned = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        count,
        vec![crate::RowDataError::new(0, "child data failure")].into_boxed_slice(),
    )
    .unwrap();
    let left: ArrayRef = Arc::new(Int8Array::from(vec![None]));
    let args = [
        EvaluatedArgument::Scalar(&left),
        EvaluatedArgument::SelectedColumn(&poisoned),
    ];
    let mut kernel = instance(
        NAMES[0],
        &[ty(DataType::Int8, true), ty(DataType::Int64, true)],
    );
    assert!(matches!(
        kernel.evaluate(selection, &args, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    assert_eq!(
        kernel
            .evaluate(Selection::all(0), &[], &Control::default())
            .unwrap_err(),
        KernelFailure::InstanceFailed
    );

    let empty = SelectedValues::try_new(
        Selection::all(0),
        &DataType::Int64,
        Arc::new(Int64Array::from(Vec::<i64>::new())),
        Box::default(),
    )
    .unwrap();
    assert!(empty.errors().is_empty());
}

#[test]
fn signed_minimum_bits_wrap_only_at_the_widened_width() {
    let profiles: [(FunctionValueType, ArrayRef, i128); 4] = [
        (
            ty(DataType::Int8, false),
            Arc::new(Int8Array::from(vec![i8::MIN; 2])),
            -64,
        ),
        (
            ty(DataType::Int16, false),
            Arc::new(Int16Array::from(vec![i16::MIN; 2])),
            -16384,
        ),
        (
            ty(DataType::Int32, false),
            Arc::new(Int32Array::from(vec![i32::MIN; 2])),
            -1073741824,
        ),
        (
            ty(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![i64::MIN; 2])),
            -4611686018427387904,
        ),
    ];
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![1, 0]));
    for (source, left, half) in profiles {
        assert_eq!(
            signed_values(&dense(
                NAMES[1],
                source.clone(),
                left.clone(),
                counts.clone()
            )),
            [Some(half), Some(half * 2)]
        );
        let first = if source.data_type() == &DataType::Int64 {
            Some(0)
        } else {
            None
        };
        assert_eq!(
            signed_values(&dense(NAMES[0], source, left, counts.clone())),
            [first, Some(half * 2)]
        );
    }
}

mod shared_core;
