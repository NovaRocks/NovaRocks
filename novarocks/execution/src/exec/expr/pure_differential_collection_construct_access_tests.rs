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

//! Full concrete carrier matrices of the two generic collection overloads.
use super::generate::{InputGenerator, InputProfile};
use super::{
    FloatComparison, LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1, constant,
};
use arrow::array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, StructArray, UInt32Array, new_empty_array,
    new_null_array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_type_contract::{
    FunctionValueType, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
    ValueTypeStorageLifetime,
};
use std::sync::Arc;
const ROWS: usize = 513;
fn flat_profiles() -> Vec<FunctionValueType> {
    let mut p = vec![
        DataType::Null,
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
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Date32,
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 6),
    ]
    .into_iter()
    .map(|t| FunctionValueType::new(t, true))
    .collect::<Vec<_>>();
    for u in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        p.push(FunctionValueType::new(DataType::Timestamp(u, None), true))
    }
    for precision in [9u8, 18, 38] {
        for scale in [-2i8, 0, 2, precision as i8] {
            p.push(FunctionValueType::new(
                DataType::Decimal128(precision, scale),
                true,
            ));
        }
    }
    for precision in [40u8, 60, 76] {
        for scale in [-2i8, 0, 2, 6] {
            p.push(FunctionValueType::new(
                DataType::Decimal256(precision, scale),
                true,
            ));
        }
    }
    p.push(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    );
    p.push(
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    );
    p.push(
        FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            true,
            ValueLogicalType::Variant,
        )
        .unwrap(),
    );
    for l in [
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Object,
        ValueLogicalType::Percentile,
    ] {
        p.push(FunctionValueType::try_with_logical_type(DataType::Binary, true, l).unwrap())
    }
    p.push(FunctionValueType::new(DataType::FixedSizeBinary(16), true));
    p
}
fn values(t: &FunctionValueType, rows: usize, seed: u64) -> ArrayRef {
    if *t.data_type() == DataType::FixedSizeBinary(16) {
        return novarocks_types::largeint::array_from_i128(
            &(0..rows)
                .map(|r| {
                    if t.nullable() && r % 7 == 0 {
                        None
                    } else {
                        Some([i128::MIN, i128::MAX, -1, 0, 1][r % 5])
                    }
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
    }
    if t.logical_type() != ValueLogicalType::Physical {
        let physical = FunctionValueType::new(t.data_type().clone(), t.nullable());
        return InputGenerator::new(seed).column(&physical, rows, &InputProfile::default());
    }
    InputGenerator::new(seed).column(t, rows, &InputProfile::default())
}
fn nested() -> Vec<(FunctionValueType, ArrayRef)> {
    let mut result = Vec::new();
    for logical in [ValueLogicalType::Physical, ValueLogicalType::LargeInt] {
        let item = if logical == ValueLogicalType::Physical {
            FunctionValueType::new(DataType::Int32, true)
        } else {
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, logical)
                .unwrap()
        };
        let field = Arc::new(item.try_to_field("item").unwrap());
        let a: ArrayRef = Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(
                (0..=ROWS)
                    .map(|r| (r * 2) as i32)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            values(&item, ROWS * 2, 41),
            Some(NullBuffer::from(
                (0..ROWS).map(|r| r % 11 != 0).collect::<Vec<_>>(),
            )),
        ));
        let ty = FunctionValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::List(item.value_type().clone()),
                true,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
            ValueLayout::try_new(a.data_type(), ValueTypeStorageLifetime::pure()).unwrap(),
        )
        .unwrap();
        result.push((ty, a));
    }
    let fs = vec![
        Arc::new(Field::new("x", DataType::Int32, true)),
        Arc::new(Field::new("y", DataType::Utf8, true)),
    ];
    let a: ArrayRef = Arc::new(StructArray::new(
        fs.into(),
        vec![
            values(&FunctionValueType::new(DataType::Int32, true), ROWS, 43),
            values(&FunctionValueType::new(DataType::Utf8, true), ROWS, 44),
        ],
        Some(NullBuffer::from(
            (0..ROWS).map(|r| r % 13 != 0).collect::<Vec<_>>(),
        )),
    ));
    result.push((FunctionValueType::new(a.data_type().clone(), true), a));
    result
}
fn map(
    keys: ArrayRef,
    kt: &FunctionValueType,
    vals: ArrayRef,
    vt: &FunctionValueType,
    offsets: Vec<i32>,
    valid: Option<Vec<bool>>,
) -> ArrayRef {
    assert_eq!(
        keys.null_count(),
        0,
        "legal Map fixtures have required keys"
    );
    let k = kt.with_nullable(false).expect("valid required Map key");
    let mut v = vt.clone();
    v = v.with_nullable(true).expect("valid test root nullability");
    let f = vec![
        Arc::new(k.try_to_field("key").unwrap()),
        Arc::new(v.try_to_field("value").unwrap()),
    ];
    let entries = StructArray::new(f.into(), vec![keys, vals], None);
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(offsets.into()),
        entries,
        valid.map(NullBuffer::from),
        false,
    ))
}
fn map_type(
    array: &ArrayRef,
    key: &FunctionValueType,
    value: &FunctionValueType,
    nullable: bool,
) -> FunctionValueType {
    let key = key.with_nullable(false).unwrap();
    let value = value.with_nullable(true).unwrap();
    FunctionValueType::try_from_value(
        ValueTypeRef::try_new(
            ValueKind::Map {
                key: key.value_type().clone(),
                value: value.value_type().clone(),
            },
            nullable,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
        ValueLayout::try_new(array.data_type(), ValueTypeStorageLifetime::pure()).unwrap(),
    )
    .unwrap()
}
fn lookup_fixture(
    kt: &FunctionValueType,
    vt: &FunctionValueType,
    vals: ArrayRef,
) -> (ArrayRef, ArrayRef) {
    let key = values(kt, ROWS, 55);
    let required_key = values(&kt.with_nullable(false).unwrap(), ROWS, 55);
    let indices = UInt32Array::from_iter_values(
        (0..ROWS).flat_map(|r| [r as u32, ((r + 1) % ROWS) as u32, r as u32]),
    );
    let keys = take(required_key.as_ref(), &indices, None).unwrap();
    let value_indices = UInt32Array::from_iter_values((0..ROWS * 3).map(|r| (r % ROWS) as u32));
    let vals = take(vals.as_ref(), &value_indices, None).unwrap();
    (
        map(
            keys,
            kt,
            vals,
            vt,
            (0..=ROWS).map(|r| (r * 3) as i32).collect(),
            Some((0..ROWS).map(|r| r % 17 != 0).collect()),
        ),
        key,
    )
}
#[test]
fn pure_differential_array_literal_every_flat_domain_and_source_nullability() {
    for mut t in flat_profiles() {
        for nullable in [false, true] {
            if *t.data_type() == DataType::Null && !nullable {
                continue;
            }
            t = t
                .with_nullable(nullable)
                .expect("valid test root nullability");
            let a = values(&t, ROWS, 17);
            let b = values(&t, ROWS, 19);
            let s = assert_scalar_matches_v1(
                ScalarDiffSpec::new("__array_literal")
                    .typed_column(t.clone(), a)
                    .typed_column(t.clone(), b)
                    .sparse_selections(7, 71)
                    .float_comparison(FloatComparison::Exact),
            );
            assert!(!s.result_type.nullable());
            assert_eq!(s.legacy_batch_errors, 0);
            assert_eq!(s.attributed_row_errors, 0);
        }
    }
}
#[test]
fn pure_differential_array_literal_empty_null_nested_and_constant_materialization() {
    let s = assert_scalar_matches_v1(
        ScalarDiffSpec::new("__array_literal")
            .constant_rows(ROWS)
            .sparse_selections(7, 73),
    );
    assert!(!s.result_type.nullable());
    for (t, a) in nested() {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("__array_literal")
                .typed_column(t.clone(), a.clone())
                .typed_column(t, a)
                .sparse_selections(7, 74),
        );
    }
    for t in flat_profiles() {
        let a = values(&t, ROWS, 17);
        let c = constant(t.clone(), a.slice(0, 1));
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("__array_literal")
                .typed_column(t.clone(), a)
                .constant(c)
                .legacy_constants(LegacyConstantForm::Pool)
                .sparse_selections(7, 75),
        );
        for rows in [0, ROWS] {
            let a = if rows == 0 {
                new_empty_array(&t.data_type())
            } else {
                new_null_array(&t.data_type(), rows)
            };
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("__array_literal").typed_column(t.clone(), a),
            );
        }
    }
}
#[test]
fn pure_differential_map_element_at_all_sixteen_original_equality_domains() {
    let vt = FunctionValueType::new(DataType::Int32, true);
    let vals = values(&vt, ROWS, 76);
    for kt in flat_profiles().into_iter().filter(|t| {
        t.logical_type() == ValueLogicalType::Physical
            && matches!(
                t.data_type(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Utf8
                    | DataType::Date32
                    | DataType::Decimal128(..)
                    | DataType::Decimal256(..)
                    | DataType::Timestamp(..)
                    | DataType::FixedSizeBinary(16)
            )
    }) {
        let (m, k) = lookup_fixture(&kt, &vt, vals.clone());
        let s = assert_scalar_matches_v1(
            ScalarDiffSpec::new("__map_element_at")
                .typed_column(map_type(&m, &kt, &vt, true), m)
                .typed_column(kt, k)
                .sparse_selections(7, 77),
        );
        assert_eq!(s.attributed_row_errors, 0);
        assert!(s.result_type.nullable());
    }
}
#[test]
fn pure_differential_map_element_at_every_flat_and_nested_value_domain_null_probes_and_slices() {
    let kt = FunctionValueType::new(DataType::Int32, true);
    for (t, a) in flat_profiles()
        .into_iter()
        .filter(|t| *t.data_type() != DataType::Null)
        .map(|t| {
            let a = values(&t, ROWS, 78);
            (t, a)
        })
        .chain(nested())
    {
        let (m, k) = lookup_fixture(&kt, &t, a);
        let s = assert_scalar_matches_v1(
            ScalarDiffSpec::new("__map_element_at")
                .typed_column(map_type(&m, &kt, &t, true), m.slice(1, ROWS - 1))
                .typed_column(kt.clone(), k.slice(1, ROWS - 1))
                .sparse_selections(7, 79),
        );
        assert_eq!(s.attributed_row_errors, 0);
        assert_eq!(s.result_type.logical_type(), t.logical_type());
        assert!(
            s.result_type.same_semantics(&t),
            "Map lookup must preserve the complete nested value domain"
        );
    }
}
#[test]
fn pure_differential_map_element_at_original_unsupported_key_errors_at_required_rows() {
    let vt = FunctionValueType::new(DataType::Int32, true);
    let vals = values(&vt, ROWS, 80);
    for kt in flat_profiles().into_iter().filter(|t| {
        t.logical_type() == ValueLogicalType::Physical
            && matches!(
                t.data_type(),
                DataType::Null
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
            )
    }) {
        if kt.data_type() == &DataType::Null {
            // A NULL key domain cannot supply required Map keys. Refusal
            // belongs to complete binding admission before kernel execution.
            let carrier = DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Null, true),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            );
            let failure =
                ValueLayout::try_new(&carrier, ValueTypeStorageLifetime::pure()).unwrap_err();
            assert!(failure.to_string().contains("map key layout is nullable"));
            continue;
        }
        let (m, k) = lookup_fixture(&kt, &vt, vals.clone());
        let s = assert_scalar_matches_v1(
            ScalarDiffSpec::new("__map_element_at")
                .typed_column(map_type(&m, &kt, &vt, true), m)
                .typed_column(kt, k)
                .sparse_selections(7, 81),
        );
        assert!(s.legacy_batch_errors > 0);
        assert!(s.attributed_row_errors > 0);
    }
}
#[test]
fn pure_differential_map_element_at_empty_entries_and_constant_probe_are_real_nullable_profiles() {
    let kt = FunctionValueType::new(DataType::Int32, true);
    for vt in flat_profiles()
        .into_iter()
        .filter(|t| *t.data_type() != DataType::Null)
    {
        let m = map(
            new_empty_array(&kt.data_type()),
            &kt,
            new_empty_array(&vt.data_type()),
            &vt,
            vec![0; ROWS + 1],
            None,
        );
        let probe = Arc::new(Int32Array::from(vec![Some(7)])) as ArrayRef;
        let s = assert_scalar_matches_v1(
            ScalarDiffSpec::new("__map_element_at")
                .typed_column(map_type(&m, &kt, &vt, true), m)
                .constant(constant(kt.clone(), probe))
                .legacy_constants(LegacyConstantForm::Pool)
                .sparse_selections(7, 83),
        );
        assert!(s.null_results >= ROWS);
    }
}

#[test]
fn pure_differential_map_element_at_required_keys_and_independent_probe_nullability_and_domains() {
    let vt = FunctionValueType::new(DataType::Int32, true);
    for logical in [ValueLogicalType::Physical, ValueLogicalType::LargeInt] {
        for map_nullable in [false, true] {
            for key_nullable in [false, true] {
                let kt = FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    key_nullable,
                    logical,
                )
                .unwrap();
                let key = values(&kt, ROWS, 87);
                let required_key = values(&kt.with_nullable(false).unwrap(), ROWS, 87);
                let vals = values(&vt, ROWS, 88);
                let m = map(
                    required_key,
                    &kt,
                    vals,
                    &vt,
                    (0..=ROWS).map(|r| r as i32).collect(),
                    map_nullable.then(|| (0..ROWS).map(|r| r % 17 != 0).collect()),
                );
                let mt = map_type(&m, &kt, &vt, map_nullable);
                let s = assert_scalar_matches_v1(
                    ScalarDiffSpec::new("__map_element_at")
                        .typed_column(mt, m)
                        .typed_column(kt, key)
                        .sparse_selections(7, 89),
                );
                assert_eq!(s.attributed_row_errors, 0);
            }
        }
    }
}

#[test]
fn pure_differential_map_null_result_preserves_valid_null_array_storage() {
    let kt = FunctionValueType::new(DataType::Int32, true);
    let vt = FunctionValueType::new(DataType::Null, true);
    let m = map(
        Arc::new(Int32Array::from(vec![1])),
        &kt,
        Arc::new(arrow::array::NullArray::new(1)),
        &vt,
        vec![0, 1],
        None,
    );
    let summary = assert_scalar_matches_v1(
        ScalarDiffSpec::new("__map_element_at")
            .typed_column(map_type(&m, &kt, &vt, true), m)
            .column(Arc::new(Int32Array::from(vec![2]))),
    );
    assert_eq!(summary.attributed_row_errors, 0);
}
