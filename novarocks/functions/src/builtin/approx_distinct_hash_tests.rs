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
use arrow_array::{ArrayRef, NullArray, builder::FixedSizeBinaryBuilder, types::Int64Type};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{Field, Fields};
use novarocks_type_contract::{
    IntervalField, IntervalQualifier, PICOS_PER_SECOND, TemporalCarrier, TemporalMicros,
    TemporalPrecision, ValueLayout, ValueTypeStorageLifetime,
};

fn hash_at(ty: &FunctionValueType, array: &dyn Array, index: usize) -> Option<u64> {
    HashProfile::prepare(ty)
        .unwrap()
        .hash(array, index, &mut ScalarWork::new(None))
        .unwrap()
}

fn physical(array: &dyn Array) -> FunctionValueType {
    FunctionValueType::new(array.data_type().clone(), true)
}

fn map_rows() -> MapArray {
    let fields: Fields = vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]
    .into();
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![4, 1, 1, 4]));
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some("b"), None, None, Some("b")]));
    let entries = StructArray::new(fields.clone(), vec![keys, values], None);
    MapArray::new(
        Arc::new(Field::new("entries", DataType::Struct(fields), false)),
        OffsetBuffer::from_lengths([2, 2]),
        entries,
        None,
        false,
    )
}

// Trino 484 one-row checksum, interpreted as little-endian u64 and divided
// by its checksum prime modulo 2^64.
#[test]
fn approx_distinct_hash_operator_recipe_matches_trino_484_composite_vectors() {
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), None, Some(2)]),
        Some(vec![None, Some(1), Some(2)]),
        None,
    ]);
    let list_type = physical(&list);
    assert_eq!(hash_at(&list_type, &list, 0), Some(0x66aa_a1f9_d0cc_b605));
    assert_eq!(hash_at(&list_type, &list, 1), Some(0x30d6_d61c_f6d6_c6bb));
    assert_eq!(hash_at(&list_type, &list, 2), None);
    assert_eq!(
        hash_at(&list_type, &list.slice(1, 1), 0),
        Some(0x30d6_d61c_f6d6_c6bb)
    );

    let map = map_rows();
    let map_type = physical(&map);
    assert_eq!(hash_at(&map_type, &map, 0), Some(0xf30c_ee72_c008_e6a5));
    assert_eq!(hash_at(&map_type, &map, 1), Some(0xf30c_ee72_c008_e6a5));

    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(2), None]),
        Some(vec![Some(2), None]),
    ]));
    let map: ArrayRef = Arc::new(map);
    let null: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
    let columns: Fields = vec![
        Field::new("l", list.data_type().clone(), true),
        Field::new("m", map.data_type().clone(), true),
        Field::new("n", DataType::Int32, true),
    ]
    .into();
    let row = StructArray::new(columns, vec![list, map, null], None);
    let row_type = physical(&row);
    assert_eq!(hash_at(&row_type, &row, 0), Some(0xfc33_1e60_f5d8_db95));
    assert_eq!(hash_at(&row_type, &row, 1), Some(0xfc33_1e60_f5d8_db95));

    let nulls: ArrayRef = Arc::new(NullArray::new(2));
    let fields: Fields = vec![Field::new("n", DataType::Null, true)].into();
    let unknown = StructArray::new(fields, vec![nulls], None);
    assert_eq!(hash_at(&physical(&unknown), &unknown, 0), Some(31));

    let program = HashProfile::prepare(&row_type).unwrap();
    assert_ne!(
        program.transport_profile(),
        HashProfile::prepare(&map_type).unwrap().transport_profile()
    );
    assert_eq!(program, HashProfile::prepare(&row_type).unwrap());
}

fn temporal_type(temporal: TemporalType) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let ty = ValueTypeRef::try_new(ValueKind::Temporal(temporal), false, storage.clone()).unwrap();
    let layout = ValueLayout::try_from_normalized_arrow(temporal.carrier(), storage).unwrap();
    FunctionValueType::try_from_value(ty, layout).unwrap()
}

fn fixed_array(value: &[u8]) -> FixedSizeBinaryArray {
    let mut builder = FixedSizeBinaryBuilder::new(value.len() as i32);
    builder.append_value(value).unwrap();
    builder.finish()
}

fn hash_zoned_time(precision: TemporalPrecision, picos: i64, offset_minutes: i16) -> Option<u64> {
    let temporal = TemporalType::TimeWithTimeZone(precision);
    let value = TemporalValue::TimeWithTimeZone {
        picos,
        offset_minutes,
    }
    .to_carrier(temporal)
    .unwrap();
    let TemporalCarrier::ZonedTime(bytes) = value else {
        panic!("zoned TIME fixture")
    };
    hash_at(&temporal_type(temporal), &fixed_array(&bytes), 0)
}

#[test]
fn approx_distinct_hash_matches_trino_484_uuid_and_temporal_vectors() {
    let bigint = Int64Array::from(vec![-3]);
    assert_eq!(
        hash_at(&physical(&bigint), &bigint, 0),
        Some(0xdfb9_8331_207f_3823)
    );

    let uuid = fixed_array(&[
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]);
    let uuid_type = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    assert_eq!(hash_at(&uuid_type, &uuid, 0), Some(0xe4ea_697b_ba36_df9a));
    assert!(HashProfile::prepare(&physical(&uuid)).is_none());

    let time = TemporalType::Time(TemporalPrecision::NANOSECONDS);
    let time_array = Int64Array::from(vec![
        (12 * 3600 + 34 * 60 + 56) * PICOS_PER_SECOND + 123_456_789_000,
    ]);
    assert_eq!(
        hash_at(&temporal_type(time), &time_array, 0),
        Some(0xff28_0c63_5e48_c08c)
    );

    let local = (45 * 60 + 56) * PICOS_PER_SECOND;
    let nanos = local + 123_456_789_000;
    assert_eq!(
        hash_zoned_time(TemporalPrecision::NANOSECONDS, nanos, 150),
        Some(0x1a73_4051_4f61_9673)
    );
    assert_eq!(
        hash_zoned_time(
            TemporalPrecision::NANOSECONDS,
            normalized_time(nanos, 150),
            0
        ),
        Some(0x1a73_4051_4f61_9673)
    );
    assert_eq!(
        hash_zoned_time(TemporalPrecision::DEFAULT, local + 123_000_000_000, 150),
        Some(0x62e3_7033_0aae_c7a4)
    );

    let months = TemporalType::Interval(
        IntervalQualifier::new(
            IntervalField::Year,
            IntervalField::Month,
            1,
            TemporalPrecision::WHOLE_SECONDS,
        )
        .unwrap(),
    );
    let month_array = Int32Array::from(vec![-15]);
    assert_eq!(
        hash_at(&temporal_type(months), &month_array, 0),
        Some(0x8fdd_eef6_acaa_082a)
    );

    let days = TemporalType::Interval(
        IntervalQualifier::new(
            IntervalField::Day,
            IntervalField::Second,
            1,
            TemporalPrecision::MICROSECONDS,
        )
        .unwrap(),
    );
    let seconds = 26 * 3600 + 3 * 60 + 4;
    let micros = -(seconds * 1_000_000 + 123_000);
    let day_array = Int64Array::from(vec![micros]);
    assert_eq!(
        hash_at(&temporal_type(days), &day_array, 0),
        Some(0x6282_0be4_0081_e945)
    );

    // Independent long-interval operator formula: XXH64 over each
    // signed-long component, XORed. The fraction is Euclidean for a
    // negative interval and cannot be dropped at microsecond precision.
    let wide = TemporalType::Interval(
        IntervalQualifier::new(
            IntervalField::Day,
            IntervalField::Second,
            1,
            TemporalPrecision::PICOSECONDS,
        )
        .unwrap(),
    );
    let detail = TemporalMicros::new(micros - 1, 999_999).unwrap();
    let TemporalCarrier::Wide(bytes) = TemporalValue::IntervalDayTime(detail)
        .to_carrier(wide)
        .unwrap()
    else {
        panic!("wide interval fixture")
    };
    assert_eq!(
        hash_at(&temporal_type(wide), &fixed_array(&bytes), 0),
        Some(hash_long(micros - 1) ^ hash_long(999_999))
    );
}

#[test]
fn approx_distinct_hash_uses_trino_484_native_json_semantics() {
    let json = crate::json_value::test_array(&[
        Some("2.0"),
        Some("2"),
        Some("null"),
        None,
        Some(r#"{"a":1,"b":2}"#),
        Some(r#"{"b":2,"a":1}"#),
        Some(r#"{"a":1,"a":2}"#),
        Some(r#"{"a":2,"a":1}"#),
    ]);
    let ty = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let expected = [
        Some(0x3e),
        Some(0x3e),
        Some(0xb50b_02b9_8206_ab01),
        None,
        Some(0x6b33_932c_e9a4_0c9d),
        Some(0x6b33_932c_e9a4_0c9d),
        Some(0x5660_c149_4fbb_4580),
        Some(0x5660_c149_4fbb_4580),
    ];
    for (row, expected) in expected.into_iter().enumerate() {
        assert_eq!(hash_at(&ty, &json, row), expected);
    }
    assert_ne!(hash_at(&ty, &json, 2), hash_at(&physical(&json), &json, 2));

    let variant = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Variant,
    )
    .unwrap();
    assert!(HashProfile::prepare(&variant).is_none());
}

#[test]
fn approx_distinct_hash_accepts_the_native_json_depth_boundary() {
    let depth = crate::json_value::MAX_DEPTH;
    let text = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
    let json = crate::json_value::test_array(&[Some(&text)]);
    let ty = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    assert_eq!(hash_at(&ty, &json, 0), Some(31 * depth as u64));
}
