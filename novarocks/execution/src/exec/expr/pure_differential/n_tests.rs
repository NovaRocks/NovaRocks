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
//! Exact original N profiles, including non-total comparator sort branches.
use super::FloatComparison;
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::generate::{InputGenerator, InputProfile};
use arrow::array::{
    ArrayRef, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, new_empty_array,
    new_null_array,
};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
const ROWS: usize = 513;
fn profiles(nullable: bool) -> Vec<FunctionValueType> {
    let mut types = vec![
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Date32,
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some("UTC".into()), Some("Asia/Shanghai".into())] {
            types.push(DataType::Timestamp(unit, zone));
        }
    }
    for (precisions, wide) in [([9, 18, 38], false), ([40, 60, 76], true)] {
        for precision in precisions {
            for scale in [-2, 0, 2, precision as i8] {
                types.push(if wide {
                    DataType::Decimal256(precision, scale)
                } else {
                    DataType::Decimal128(precision, scale)
                });
            }
        }
    }
    let mut values = types
        .into_iter()
        .map(|ty| FunctionValueType::new(ty, nullable))
        .collect::<Vec<_>>();
    values.push(FunctionValueType::new(
        DataType::FixedSizeBinary(16),
        nullable,
    ));
    values
}

fn profile_values(source: &FunctionValueType, rows: usize) -> ArrayRef {
    if source.data_type == DataType::FixedSizeBinary(16) {
        // Genuine physical 16-byte carrier, with original endian/sign codec.
        // Binding stays Physical; this fixture does not assert logical LARGEINT.
        let pattern = [
            Some(i128::MIN),
            Some(i128::MAX),
            Some(-1),
            Some(0),
            Some(1),
            Some(1i128 << 64),
            Some(-(1i128 << 80)),
            None,
        ];
        return novarocks_types::largeint::array_from_i128(
            &(0..rows)
                .map(|row| pattern[row % pattern.len()].or_else(|| (!source.nullable).then_some(7)))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    }
    InputGenerator::new(1801).column(
        source,
        rows,
        &InputProfile::default().with_boundary_ratio(0.0),
    )
}
fn limits(ty: &DataType, rows: usize, n: i64) -> ArrayRef {
    match ty {
        DataType::Int8 => Arc::new(Int8Array::from(vec![n as i8; rows])),
        DataType::Int16 => Arc::new(Int16Array::from(vec![n as i16; rows])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![n as i32; rows])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![n; rows])),
        _ => unreachable!(),
    }
}
#[test]
fn pure_differential_n_every_complete_scalar_codec_profile_and_signed_limit_carrier() {
    for name in ["min_n", "max_n"] {
        for nullable in [false, true] {
            for source in profiles(nullable) {
                let values = profile_values(&source, ROWS);
                for limit in [
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                ] {
                    for limit_nullable in [false, true] {
                        let summary = assert_aggregate_matches_v1(
                            AggregateDiffSpec::new(name)
                                .typed_column(source.clone(), values.clone())
                                .typed_column(
                                    FunctionValueType::new(limit.clone(), limit_nullable),
                                    limits(&limit, ROWS, 7),
                                )
                                .grouped((0..ROWS).map(|row| row % 6).collect(), 8)
                                .partitions(7, 1811)
                                .float_comparison(FloatComparison::Exact),
                        );
                        assert_eq!(summary.matched_failures, 0);
                        assert!(summary.result_type.nullable);
                        // Empty groups remain non-null empty Lists.
                        assert_eq!(summary.null_results, 0);
                    }
                }
            }
        }
    }
}
#[test]
fn pure_differential_n_empty_all_null_values_and_constant_limits_keep_empty_lists() {
    for name in ["min_n", "max_n"] {
        for source in profiles(true)
            .into_iter()
            .chain([FunctionValueType::new(DataType::Null, true)])
        {
            for rows in [0, ROWS] {
                let values = if rows == 0 {
                    new_empty_array(&source.data_type)
                } else {
                    new_null_array(&source.data_type, rows)
                };
                let summary = assert_aggregate_matches_v1(
                    AggregateDiffSpec::new(name)
                        .typed_column(source.clone(), values)
                        .constant(super::constant(
                            FunctionValueType::new(DataType::Int64, false),
                            limits(&DataType::Int64, 1, 3),
                        ))
                        .partitions(7, 1812),
                );
                assert_eq!(summary.matched_failures, 0);
                assert_eq!(summary.null_results, 0);
            }
        }
    }
}
#[test]
fn pure_differential_n_nan_signed_zero_at_original_stable_sort_size_boundaries() {
    // These inputs deliberately do not define a total comparator. Keeping
    // original std stable sort also requires the original element layout and
    // Freeze specialization; this test must never be softened by filtering NaN.
    let mut matched_panics = 0;
    for name in ["min_n", "max_n"] {
        for rows in [16usize, 20, 21, 31, 32, 33, 63, 64, 65, 127, 128, 129, 513] {
            for n in [1, 7, 16, 20, 21, 31, 32, 33, 65, 129, 513] {
                let values: ArrayRef = Arc::new(Float64Array::from(
                    (0..rows)
                        .map(|row| match row % 7 {
                            0 => f64::from_bits(0x7ff8_0000_0000_0041 + row as u64),
                            1 => -0.0,
                            2 => 0.0,
                            3 => f64::INFINITY,
                            4 => f64::NEG_INFINITY,
                            _ => (row % 11) as f64,
                        })
                        .collect::<Vec<_>>(),
                ));
                let summary = assert_aggregate_matches_v1(
                    AggregateDiffSpec::new(name)
                        .typed_column(FunctionValueType::new(DataType::Float64, false), values)
                        .typed_column(
                            FunctionValueType::new(DataType::Int64, false),
                            limits(&DataType::Int64, rows, n),
                        )
                        .partitions(1, 1813)
                        .expected_panic_payload("user-provided comparison function does not correctly implement a total order")
                        .float_comparison(FloatComparison::Exact),
                );
                assert_eq!(summary.matched_failures, 0);
                matched_panics += summary.matched_panics;
            }
        }
    }
    assert!(
        matched_panics > 0,
        "freeze actual original non-total sort panic evidence"
    );
}

#[test]
fn pure_differential_n_logical_largeint_freezes_full_legacy_metadata_drift() {
    use super::DifferentialFailure;
    use super::aggregate::run_aggregate_differential;
    for name in ["min_n", "max_n"] {
        for nullable in [false, true] {
            let source = FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                nullable,
                ValueLogicalType::LargeInt,
            )
            .unwrap();
            for n in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
            ] {
                for limit_nullable in [false, true] {
                    for rows in [0usize, 8] {
                        let values = if rows == 0 {
                            new_empty_array(&source.data_type)
                        } else {
                            InputGenerator::new(1821).column(
                                &source,
                                rows,
                                &InputProfile::default(),
                            )
                        };
                        let limit = FunctionValueType::new(n.clone(), limit_nullable);
                        let spec = AggregateDiffSpec::new(name)
                            .typed_column(source.clone(), values)
                            .typed_column(limit.clone(), limits(&n, rows, 3));
                        let failure = run_aggregate_differential(&spec).unwrap_err();
                        match failure {
                            DifferentialFailure::LegacyUnavailable {
                                name: actual,
                                reason,
                            } => {
                                assert_eq!(actual, name);
                                let expected = format!(
                                    "bind aggregate `{name}`: aggregate `{name}` resolved signature drift: planned=ResolvedAggregateSignature {{ overload: AggregateOverloadIdentity(\"builtin.aggregate/{name}/derived-v1\"), argument_types: [FixedSizeBinary(16), {n:?}], intermediate_type: Binary, output_type: List(Field {{ data_type: FixedSizeBinary(16), nullable: true, metadata: {{\"nr_logical_type\": \"largeint\"}} }}), state_format: AggregateStateFormatId(\"novarocks/{name}/state-v1\") }}, local=ResolvedAggregateSignature {{ overload: AggregateOverloadIdentity(\"builtin.aggregate/{name}/derived-v1\"), argument_types: [FixedSizeBinary(16), {n:?}], intermediate_type: Binary, output_type: List(Field {{ data_type: FixedSizeBinary(16), nullable: true }}), state_format: AggregateStateFormatId(\"novarocks/{name}/state-v1\") }}"
                                );
                                assert_eq!(reason, expected);
                            }
                            other => panic!(
                                "expected frozen logical LARGEINT legacy metadata drift: {other}"
                            ),
                        }
                    }
                }
            }
        }
    }
}
