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
//! Positive tests retain the immutable original profile domain and error text.
use super::aggregate::{
    AggregateDiffSpec, assert_aggregate_matches_v1, assert_encoded_aggregate_matches_v1,
};
use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use serde_json::{Value, json};
use std::sync::Arc;

fn record(
    records: &mut Vec<Value>,
    name: &str,
    columns: Vec<ArrayRef>,
    groups: &[usize],
    count: usize,
) {
    let types: Vec<_> = columns
        .iter()
        .map(|v| format!("{:?}", v.data_type()))
        .collect();
    let mut base = AggregateDiffSpec::new("mann_whitney_u_test")
        .invocation_data_errors()
        .grouped(groups.to_vec(), count);
    for column in columns {
        base = base.column(column);
    }
    for partitions in [1, 3] {
        assert_aggregate_matches_v1(base.clone().round_robin_partitions(partitions));
        records.push(json!({"name":name, "types":types, "partitions":partitions}));
    }
    // A separate seeded layout detects dependence on the transcript's fixed
    // round-robin order, using the same legacy and pure assignments.
    assert_aggregate_matches_v1(base.partitions(3, 0xA66));
}

fn boolean(values: Vec<Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from(values))
}
fn text(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn integers(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
#[test]
fn mann_whitney_owner_all_354_original_profiles_match_v1() {
    let mut records = Vec::new();
    let base = integers(vec![Some(1), Some(3), Some(2), Some(4), None, Some(-7)]);
    let treatment = integers(vec![Some(0), Some(-1), Some(0), Some(3), None, Some(0)]);
    let value_types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(20, 0),
        DataType::Decimal128(20, 2),
        DataType::Decimal128(38, -2),
    ];
    let treatment_types = [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ];
    let mut numeric = value_types
        .iter()
        .map(|ty| cast(base.as_ref(), ty).unwrap())
        .collect::<Vec<_>>();
    numeric.push(
        novarocks_functions::largeint::array_from_i128(&[
            Some(1),
            Some(3),
            Some(2),
            Some(4),
            None,
            Some(-(1_i128 << 72)),
        ])
        .unwrap(),
    );
    for (vi, values) in numeric.iter().enumerate() {
        for (ti, ty) in treatment_types.iter().enumerate() {
            let treatments = cast(treatment.as_ref(), ty).unwrap();
            for arity in 2..=4 {
                let mut columns = vec![values.clone(), treatments.clone()];
                if arity >= 3 {
                    columns.push(text(vec![Some("TwO-SiDeD"); 6]));
                }
                if arity == 4 {
                    columns
                        .push(cast(integers(vec![Some(0); 6]).as_ref(), &DataType::Int16).unwrap());
                }
                record(
                    &mut records,
                    &format!("numeric-{vi}-treatment-{ti}-arity-{arity}"),
                    columns,
                    &[0, 0, 0, 0, 0, 0],
                    2,
                );
            }
        }
    }
    record(
        &mut records,
        "grouped",
        vec![
            base.clone(),
            cast(treatment.as_ref(), &DataType::Boolean).unwrap(),
        ],
        &[0, 1, 2, 0, 1, 2],
        4,
    );
    record(
        &mut records,
        "empty",
        vec![
            Arc::new(Float64Array::from(Vec::<f64>::new())),
            boolean(vec![]),
        ],
        &[],
        2,
    );
    record(
        &mut records,
        "identical",
        vec![
            integers(vec![Some(1); 3]),
            boolean(vec![Some(false), Some(true), Some(false)]),
        ],
        &[0; 3],
        1,
    );
    record(
        &mut records,
        "single-sided",
        vec![
            integers(vec![Some(1), Some(2)]),
            boolean(vec![Some(false); 2]),
        ],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "nonfinite",
        vec![
            Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                1.0,
                2.0,
                3.0,
            ])),
            boolean(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                Some(true),
                Some(true),
            ]),
        ],
        &[0; 6],
        2,
    );
    record(
        &mut records,
        "all-null-values",
        vec![text(vec![None; 2]), boolean(vec![Some(false), Some(true)])],
        &[0; 2],
        2,
    );
    record(
        &mut records,
        "null-physical",
        vec![
            Arc::new(NullArray::new(2)),
            boolean(vec![Some(false), Some(true)]),
        ],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "nonnumeric-null-treatment",
        vec![text(vec![Some("x"), Some("y")]), boolean(vec![None; 2])],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "nonnumeric-data-error",
        vec![
            text(vec![Some("x"), None]),
            boolean(vec![Some(false), Some(true)]),
        ],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "unsupported-reader-null-treatment",
        vec![
            Arc::new(UInt64Array::from(vec![Some(1), None])),
            boolean(vec![None; 2]),
        ],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "unsupported-reader-null-value",
        vec![
            Arc::new(UInt64Array::from(vec![None::<u64>; 2])),
            boolean(vec![Some(false), Some(true)]),
        ],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "invalid-alternative-before-null",
        vec![
            text(vec![Some("x")]),
            boolean(vec![None]),
            text(vec![Some(" BAD ")]),
        ],
        &[0],
        1,
    );
    record(
        &mut records,
        "negative-continuity-before-null",
        vec![
            text(vec![None]),
            boolean(vec![None]),
            text(vec![None]),
            integers(vec![Some(-1)]),
        ],
        &[0],
        1,
    );
    record(
        &mut records,
        "later-parameters-ignored",
        vec![
            base.clone(),
            cast(treatment.as_ref(), &DataType::Boolean).unwrap(),
            text(vec![
                Some("less"),
                Some("invalid"),
                Some("greater"),
                None,
                Some("less"),
                Some("less"),
            ]),
            integers(vec![Some(0), Some(-1), Some(2), None, Some(0), Some(0)]),
        ],
        &[0; 6],
        1,
    );
    record(
        &mut records,
        "merge-parameters-mismatch",
        vec![
            base.clone(),
            cast(treatment.as_ref(), &DataType::Boolean).unwrap(),
            text(vec![
                Some("less"),
                Some("greater"),
                Some("less"),
                Some("less"),
                Some("greater"),
                Some("less"),
            ]),
        ],
        &[0; 6],
        1,
    );
    for alternative in ["two-sided", "less", "greater"] {
        for ty in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            record(
                &mut records,
                &format!("alternative-{alternative}-continuity-{ty:?}"),
                vec![
                    base.clone(),
                    cast(treatment.as_ref(), &DataType::Boolean).unwrap(),
                    text(vec![Some(alternative); 6]),
                    cast(integers(vec![Some(2); 6]).as_ref(), &ty).unwrap(),
                ],
                &[0; 6],
                1,
            );
        }
    }
    let frozen: Value = serde_json::from_str(include_str!(
        "../agg/functions/mann_whitney_original_contract.json"
    ))
    .unwrap();
    assert_eq!(records.len(), 354);
    let expected: Vec<_> = frozen["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| json!({"name":v["name"], "types":v["types"], "partitions":v["partitions"]}))
        .collect();
    assert_eq!(
        records, expected,
        "every frozen input profile must be represented"
    );
}

#[test]
fn mann_whitney_owner_four_original_codec_inputs_match_v1() {
    let frozen: Value = serde_json::from_str(include_str!(
        "../agg/functions/mann_whitney_original_contract.json"
    ))
    .unwrap();
    let entries = frozen["codec"].as_array().unwrap();
    assert_eq!(entries.len(), 4);
    for entry in entries {
        let bytes = hex::decode(entry["input"].as_str().unwrap()).unwrap();
        let input = Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())])) as ArrayRef;
        assert_encoded_aggregate_matches_v1(
            AggregateDiffSpec::new("mann_whitney_u_test")
                .invocation_data_errors()
                .column(Arc::new(Int64Array::from(vec![1])) as ArrayRef)
                .column(Arc::new(BooleanArray::from(vec![false])) as ArrayRef),
            input,
        );
    }
}

#[test]
fn mann_whitney_owner_exact_fixed16_logical_identities_keep_original_raw_i128() {
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    let values = novarocks_functions::largeint::array_from_i128(&[
        Some(i128::MIN),
        Some(-1),
        Some(0),
        Some(1),
        Some(i128::MAX),
        None,
    ])
    .unwrap();
    for logical in [
        ValueLogicalType::Physical,
        ValueLogicalType::LargeInt,
        ValueLogicalType::Uuid,
    ] {
        let source =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, logical)
                .unwrap();
        let result = assert_aggregate_matches_v1(
            AggregateDiffSpec::new("mann_whitney_u_test")
                .invocation_data_errors()
                .typed_column(source, values.clone())
                .column(boolean(vec![
                    Some(false),
                    Some(true),
                    Some(false),
                    Some(true),
                    Some(true),
                    None,
                ]))
                .grouped(vec![0, 0, 0, 0, 0, 0], 2),
        );
        assert_eq!(result.matched_failures, 0);
    }
}
#[test]
fn mann_whitney_owner_non_numeric_null_reachability_and_full_type_data_are_exact() {
    use arrow::datatypes::{Field, TimeUnit};
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    let mut sources = vec![
        FunctionValueType::new(DataType::UInt8, true),
        FunctionValueType::new(DataType::UInt16, true),
        FunctionValueType::new(DataType::UInt32, true),
        FunctionValueType::new(DataType::UInt64, true),
        FunctionValueType::new(DataType::Boolean, true),
        FunctionValueType::new(DataType::LargeUtf8, true),
        FunctionValueType::new(DataType::FixedSizeBinary(7), true),
        FunctionValueType::new(DataType::Date32, true),
        FunctionValueType::new(DataType::Decimal256(76, -2), true),
        FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Shanghai".into())),
            true,
        ),
        FunctionValueType::new(
            DataType::LargeList(Arc::new(Field::new(
                "long original field ".repeat(35),
                DataType::Utf8,
                true,
            ))),
            true,
        ),
    ];
    for (carrier, logical) in [
        (DataType::Utf8, ValueLogicalType::Json),
        (DataType::LargeBinary, ValueLogicalType::Variant),
        (DataType::Binary, ValueLogicalType::Hll),
        (DataType::Binary, ValueLogicalType::Bitmap),
        (DataType::Binary, ValueLogicalType::Object),
        (DataType::Binary, ValueLogicalType::Percentile),
    ] {
        sources.push(FunctionValueType::try_with_logical_type(carrier, true, logical).unwrap());
    }
    for source in sources {
        let values = new_null_array(&source.data_type, 2);
        for treatment in [
            boolean(vec![None; 2]),
            boolean(vec![Some(false), Some(true)]),
        ] {
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new("mann_whitney_u_test")
                    .invocation_data_errors()
                    .typed_column(source.clone(), values.clone())
                    .column(treatment)
                    .grouped(vec![0, 0], 2),
            );
        }
    }
    let list = Arc::new(ListArray::from_iter_primitive::<
        arrow::datatypes::Int64Type,
        _,
        _,
    >([
        Some(vec![Some(1), None, Some(3)]),
        Some(vec![]),
        None,
    ])) as ArrayRef;
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new("mann_whitney_u_test")
            .invocation_data_errors()
            .column(list)
            .column(boolean(vec![Some(false), Some(true), None]))
            .grouped(vec![0, 0, 0], 2),
    );
}
#[test]
fn mann_whitney_owner_seeded_numeric_boundaries_and_work_quantum_match_v1() {
    use super::generate::{InputGenerator, InputProfile};
    use novarocks_type_contract::FunctionValueType;
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(38, -2),
        DataType::Decimal128(38, 38),
    ] {
        let source = FunctionValueType::new(ty, true);
        let values = InputGenerator::new(517).column(&source, 513, &InputProfile::default());
        let treatments = boolean(
            (0..513)
                .map(|i| if i % 11 == 0 { None } else { Some(i % 2 == 0) })
                .collect(),
        );
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new("mann_whitney_u_test")
                .invocation_data_errors()
                .typed_column(source, values)
                .column(treatments)
                .grouped((0..513).map(|i| i % 3).collect(), 4)
                .partitions(3, 0x561),
        );
    }
}
