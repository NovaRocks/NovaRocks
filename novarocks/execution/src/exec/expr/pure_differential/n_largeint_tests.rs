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
//! Installed LARGEINT semantics vs original raw16 ABI. Complete declaration
//! metadata is separately checked by the preinstallation oracle. Maintained
//! by the N owner; source arrays/seeds only, no historical runtime inputs.
use super::aggregate::{
    AggregateDiffSpec, assert_n_largeint_encoded_matches_raw16, run_n_largeint_raw16_differential,
};
use super::generate::{InputGenerator, InputProfile};
use arrow::array::{
    Array, ArrayRef, BinaryArray, Int8Array, Int16Array, Int32Array, Int64Array, new_null_array,
};
use arrow::datatypes::DataType;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
fn value_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn limits(carrier: &DataType, values: &[Option<i64>]) -> ArrayRef {
    match carrier {
        DataType::Int8 => Arc::new(Int8Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as i8))
                .collect::<Vec<_>>(),
        )),
        DataType::Int16 => Arc::new(Int16Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as i16))
                .collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as i32))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
        _ => panic!("signed N limit fixture"),
    }
}
fn spec(
    name: &str,
    value_type: FunctionValueType,
    values: ArrayRef,
    limit_type: FunctionValueType,
    limit_values: ArrayRef,
) -> AggregateDiffSpec {
    AggregateDiffSpec::new(name)
        .typed_column(value_type, values)
        .typed_column(limit_type, limit_values)
        .invocation_data_errors()
}
#[test]
fn n_largeint_every_complete_declaration_matches_raw16_single_and_partitioned_values() {
    for name in ["min_n", "max_n"] {
        for nullable in [false, true] {
            for carrier in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
            ] {
                for limit_nullable in [false, true] {
                    for rows in [0_usize, 1, 17, 257] {
                        let ty = value_type(nullable);
                        let values = InputGenerator::new(0x1724 + rows as u64).column(
                            &ty,
                            rows,
                            &InputProfile::default(),
                        );
                        let s = spec(
                            name,
                            ty,
                            values,
                            FunctionValueType::new(carrier.clone(), limit_nullable),
                            limits(&carrier, &vec![Some(3); rows]),
                        )
                        .grouped((0..rows).map(|i| i % 3).collect(), 5)
                        .partitions(4, 0x1734);
                        let summary = run_n_largeint_raw16_differential(&s).unwrap();
                        assert_eq!(summary.matched_failures, 0);
                        assert_eq!(summary.rows, rows);
                        assert_eq!(summary.groups, 5);
                    }
                }
            }
        }
    }
}
#[test]
fn n_largeint_extrema_duplicates_all_null_and_empty_groups_match_raw16() {
    for name in ["min_n", "max_n"] {
        for n in [1_i64, 2, 8] {
            for all_null in [false, true] {
                let values = if all_null {
                    new_null_array(&DataType::FixedSizeBinary(16), 9)
                } else {
                    novarocks_functions::largeint::array_from_i128(&[
                        Some(i128::MIN),
                        Some(i128::MAX),
                        None,
                        Some(-3),
                        Some(0),
                        Some(i128::MAX),
                        Some(i128::MIN),
                        Some(7),
                        Some(-3),
                    ])
                    .unwrap()
                };
                let s = spec(
                    name,
                    value_type(true),
                    values,
                    FunctionValueType::new(DataType::Int64, true),
                    limits(&DataType::Int64, &vec![Some(n); 9]),
                )
                .grouped(vec![0, 0, 0, 1, 1, 1, 0, 1, 1], 4)
                .round_robin_partitions(3);
                assert_eq!(
                    run_n_largeint_raw16_differential(&s)
                        .unwrap()
                        .matched_failures,
                    0
                );
            }
        }
    }
}
#[test]
fn n_largeint_each_signed_limit_null_nonpositive_and_drift_is_full_original_data() {
    for name in ["min_n", "max_n"] {
        for carrier in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            for ls in [
                vec![None, None, None, None],
                vec![Some(0); 4],
                vec![Some(-1); 4],
                vec![Some(2), Some(3), Some(2), Some(3)],
            ] {
                let values = novarocks_functions::largeint::array_from_i128(&[
                    None,
                    Some(8),
                    Some(-2),
                    Some(i128::MAX),
                ])
                .unwrap();
                let s = spec(
                    name,
                    value_type(true),
                    values,
                    FunctionValueType::new(carrier.clone(), true),
                    limits(&carrier, &ls),
                )
                .round_robin_partitions(2);
                assert_eq!(
                    run_n_largeint_raw16_differential(&s)
                        .unwrap()
                        .matched_failures,
                    2
                );
            }
        }
    }
}
#[test]
fn n_largeint_constant_pool_values_and_limits_keep_actual_selected_addresses() {
    use arrow::datatypes::Field;
    use novarocks_functions::ConstantValue;
    for name in ["min_n", "max_n"] {
        let ty = value_type(false);
        let array = novarocks_functions::largeint::array_from_i128(&[Some(-9)]).unwrap();
        let value = ConstantValue::from_scalar_array(
            Arc::new(
                Field::new("value", DataType::FixedSizeBinary(16), false).with_metadata(
                    std::collections::HashMap::from([(
                        "nr_logical_type".into(),
                        "largeint".into(),
                    )]),
                ),
            ),
            ty,
            array.to_data(),
            super::constant_policy(),
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            &super::HarnessControl,
        )
        .unwrap();
        // Use an explicit positive limit rather than a random limit's semantics.
        let limit = ConstantValue::from_scalar_array(
            Arc::new(Field::new("limit", DataType::Int64, false)),
            FunctionValueType::new(DataType::Int64, false),
            Int64Array::from(vec![2_i64]).to_data(),
            super::constant_policy(),
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            &super::HarnessControl,
        )
        .unwrap();
        let s = AggregateDiffSpec::new(name)
            .constant(value)
            .constant(limit)
            .constant_rows(17)
            .invocation_data_errors()
            .round_robin_partitions(3);
        assert_eq!(
            run_n_largeint_raw16_differential(&s)
                .unwrap()
                .matched_failures,
            0
        );
    }
}
#[test]
fn n_largeint_original_codec_merge_intermediate_final_and_errors_match_raw16() {
    let mut valid = Vec::from(2_u32.to_le_bytes());
    valid.extend_from_slice(&2_u32.to_le_bytes());
    for v in [i128::MIN, i128::MAX] {
        valid.push(7);
        valid.extend_from_slice(&v.to_le_bytes());
    }
    let mut zero = Vec::from(0_u32.to_le_bytes());
    zero.extend_from_slice(&1_u32.to_le_bytes());
    zero.push(7);
    zero.extend_from_slice(&9_i128.to_le_bytes());
    // Each original scalar tag reaches the original decoder on both sides.
    // Non-LARGEINT leaves are legal intermediate payloads and retain the
    // original late final type error instead of an early profile rejection.
    let mut scalar_leaves = vec![
        vec![1, 1],
        [vec![2], (-9_i64).to_le_bytes().to_vec()].concat(),
        [vec![3], (-0.0_f64).to_bits().to_le_bytes().to_vec()].concat(),
        [vec![4], 3_u32.to_le_bytes().to_vec(), b"nxy".to_vec()].concat(),
        [vec![5], (-9_i32).to_le_bytes().to_vec()].concat(),
        [vec![6], (-9_i64).to_le_bytes().to_vec()].concat(),
        [vec![7], (-9_i128).to_le_bytes().to_vec()].concat(),
        [vec![11], 3_u32.to_le_bytes().to_vec(), b"-19".to_vec()].concat(),
    ];
    scalar_leaves.extend([
        vec![4, 1, 0, 0, 0, 255],
        vec![11, 1, 0, 0, 0, 255],
        [vec![11], 3_u32.to_le_bytes().to_vec(), b"bad".to_vec()].concat(),
    ]);
    let mut payloads = vec![valid.clone(), zero];
    for scalar in scalar_leaves {
        let mut payload = Vec::from(2_u32.to_le_bytes());
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.extend_from_slice(&scalar);
        payloads.push(payload);
    }
    let mut unknown = Vec::from(2_u32.to_le_bytes());
    unknown.extend_from_slice(&1_u32.to_le_bytes());
    unknown.push(255);
    let mut trailing = valid.clone();
    trailing.extend_from_slice(b"trailing");
    payloads.extend([unknown, trailing, vec![0_u8; 3]]);
    for name in ["min_n", "max_n"] {
        for payload in &payloads {
            let s = spec(
                name,
                value_type(true),
                novarocks_functions::largeint::array_from_i128(&[Some(1)]).unwrap(),
                FunctionValueType::new(DataType::Int64, true),
                limits(&DataType::Int64, &[Some(2)]),
            );
            let encoded: ArrayRef =
                Arc::new(BinaryArray::from(vec![Some(payload.as_slice()), None]));
            assert_n_largeint_encoded_matches_raw16(s, encoded);
        }
    }
}
