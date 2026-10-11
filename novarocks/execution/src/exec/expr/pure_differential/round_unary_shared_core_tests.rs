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
//! Generated unary ROUND witnesses before and after sharing the original arithmetic.
use super::{
    ScalarDiffSpec, assert_scalar_matches_v1, constant,
    generate::{InputGenerator, InputProfile, TextProfile},
};
use arrow::array::new_empty_array;
use arrow::datatypes::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};

const SEEDS: [u64; 3] = [0xC065E, 0xC065F, 0xC0660];
const ROWS: usize = 513;

fn carriers() -> [DataType; 13] {
    [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
    ]
}

fn policies() -> [DecimalOverflowPolicy; 2] {
    [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ]
}

fn check(spec: ScalarDiffSpec, rows: usize, seed: u64) {
    let expected = FunctionValueType::new(DataType::Int64, true);
    let summary = assert_scalar_matches_v1(
        spec.expect_result_type(expected.clone())
            .sparse_selections(7, seed),
    );
    assert_eq!(summary.overload.as_str(), "builtin.scalar/round/dynamic-v1");
    assert_eq!(summary.result_type, expected);
    assert_eq!(summary.rows, rows);
    assert_eq!(summary.selections, if rows > 1 { 8 } else { 1 });
    assert_eq!(summary.legacy_batch_errors, 0);
    assert_eq!(summary.attributed_row_errors, 0);
}

#[test]
fn c06_p1_round_unary_generated_columns_preserve_original_exact_results() {
    for seed in SEEDS {
        let mut generator = InputGenerator::new(seed);
        for carrier in carriers() {
            for nullable in [false, true] {
                let ty = FunctionValueType::new(carrier.clone(), nullable);
                let values = generator.column(
                    &ty,
                    ROWS,
                    &InputProfile::default().with_text(TextProfile::Numeric),
                );
                for policy in policies() {
                    check(
                        ScalarDiffSpec::new("round")
                            .typed_column(ty.clone(), values.clone())
                            .decimal_overflow(policy),
                        ROWS,
                        seed,
                    );
                }
            }
        }
    }
}

#[test]
fn c06_p1_round_unary_generated_constants_preserve_original_broadcast_results() {
    for seed in SEEDS {
        let mut generator = InputGenerator::new(seed);
        for carrier in carriers() {
            for nullable in [false, true] {
                let ty = FunctionValueType::new(carrier.clone(), nullable);
                let values = generator.column(
                    &ty,
                    ROWS,
                    &InputProfile::default().with_text(TextProfile::Numeric),
                );
                // Keep the original typed one-row pool admission and broadcast route.
                let value = constant(ty, values.slice(0, 1));
                for policy in policies() {
                    check(
                        ScalarDiffSpec::new("round")
                            .constant(value.clone())
                            .constant_rows(ROWS)
                            .decimal_overflow(policy),
                        ROWS,
                        seed,
                    );
                }
            }
        }
    }
}

#[test]
fn c06_p1_round_unary_typed_empty_columns_use_original_all_zero_rows() {
    for carrier in carriers() {
        for nullable in [false, true] {
            let ty = FunctionValueType::new(carrier.clone(), nullable);
            for policy in policies() {
                check(
                    ScalarDiffSpec::new("round")
                        .typed_column(ty.clone(), new_empty_array(&carrier))
                        .decimal_overflow(policy),
                    0,
                    SEEDS[0],
                );
            }
        }
    }
}

#[test]
fn c06_p1_round_unary_typed_constants_broadcast_to_zero_rows() {
    for seed in SEEDS {
        let mut generator = InputGenerator::new(seed);
        for carrier in carriers() {
            for nullable in [false, true] {
                let ty = FunctionValueType::new(carrier.clone(), nullable);
                let values = generator.column(
                    &ty,
                    1,
                    &InputProfile::default().with_text(TextProfile::Numeric),
                );
                let value = constant(ty, values);
                for policy in policies() {
                    check(
                        ScalarDiffSpec::new("round")
                            .constant(value.clone())
                            .constant_rows(0)
                            .decimal_overflow(policy),
                        0,
                        seed,
                    );
                }
            }
        }
    }
}
