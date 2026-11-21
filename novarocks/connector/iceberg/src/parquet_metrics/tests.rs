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

use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use parquet::data_type::{ByteArray, FixedLenByteArray};
use parquet::file::metadata::{ColumnChunkMetaData, FileMetaData, RowGroupMetaData};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::{Statistics, ValueStatistics};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::SchemaDescriptor;
use serde_json::Value as Json;

use crate::file_reader::field_identity::ResolvedParquetLeaf;
use crate::iceberg::spec::NestedField;

use super::truncate::{binary_bound, string_bound};
use super::*;

fn schema(fields: &[(i32, PrimitiveType)]) -> Schema {
    Schema::builder()
        .with_fields(fields.iter().map(|(id, primitive)| {
            Arc::new(NestedField::optional(
                *id,
                format!("field_{id}"),
                Type::Primitive(primitive.clone()),
            ))
        }))
        .build()
        .unwrap()
}

fn leaf(field_id: i32) -> ResolvedParquetLeaf {
    ResolvedParquetLeaf {
        field_id: Some(field_id),
        repeated: false,
    }
}

fn footer(message: &str, groups: &[(i64, Vec<Option<Statistics>>)]) -> ParquetMetaData {
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(
        parse_message_type(message).unwrap(),
    )));
    let row_groups = groups
        .iter()
        .map(|(rows, stats)| {
            assert_eq!(schema.num_columns(), stats.len());
            let columns = stats
                .iter()
                .zip(schema.columns())
                .map(|(stats, column)| {
                    let mut builder = ColumnChunkMetaData::builder(column.clone())
                        .set_num_values(*rows)
                        .set_total_compressed_size(23);
                    if let Some(stats) = stats {
                        builder = builder.set_statistics(stats.clone());
                    }
                    builder.build().unwrap()
                })
                .collect();
            RowGroupMetaData::builder(schema.clone())
                .set_num_rows(*rows)
                .set_column_metadata(columns)
                .build()
                .unwrap()
        })
        .collect();
    // The metric author must use the sum of the row groups rather than the
    // file header's denormalized count.
    ParquetMetaData::new(
        FileMetaData::new(1, 0, None, None, schema, None),
        row_groups,
    )
}

fn metrics(
    metadata: &ParquetMetaData,
    leaves: &ResolvedParquetLeaves,
    schema: &Schema,
) -> FooterMetrics {
    analyze_footer_metrics(metadata, leaves, schema)
        .unwrap()
        .materialize()
        .unwrap()
}

#[test]
fn unknown_counts_missing_statistics_and_repeated_columns() {
    let metadata = footer(
        "message f { optional int32 a; optional int32 b; optional int32 c; optional int32 d; optional int32 e; }",
        &[
            (
                2,
                vec![Some(Statistics::int32(Some(-8), Some(3), None, Some(1), false)); 5],
            ),
            (
                2,
                vec![
                    Some(Statistics::int32(Some(1), Some(12), None, None, false)),
                    None,
                    Some(Statistics::int32(Some(1), Some(12), None, None, false)),
                    Some(Statistics::int32(Some(1), Some(12), None, None, false)),
                    Some(Statistics::int32(None, None, Some(1), None, false)),
                ],
            ),
        ],
    );
    let leaves = vec![
        leaf(20),
        leaf(21),
        ResolvedParquetLeaf {
            field_id: Some(22),
            repeated: true,
        },
        ResolvedParquetLeaf {
            field_id: None,
            repeated: false,
        },
        leaf(24),
    ];
    let table = schema(&[
        (20, PrimitiveType::Long),
        (21, PrimitiveType::Int),
        (22, PrimitiveType::Int),
    ]);
    let result = metrics(&metadata, &leaves, &table);
    assert_eq!(result.record_count, 4);
    assert_eq!(
        result.column_sizes,
        HashMap::from([(20, 46), (21, 46), (22, 46), (24, 46)])
    );
    assert_eq!(result.value_counts, HashMap::from([(20, 4)]));
    assert!(result.null_value_counts.is_empty());
    assert_eq!(result.lower_bounds, HashMap::from([(20, Datum::long(-8))]));
    assert_eq!(result.upper_bounds, HashMap::from([(20, Datum::long(12))]));
}

#[test]
fn all_null_groups_can_be_skipped_but_unbounded_non_null_values_cannot() {
    let table = schema(&[(1, PrimitiveType::Int)]);
    let leaves = vec![leaf(1)];
    let finite = Some(Statistics::int32(Some(-4), Some(7), None, Some(0), false));
    for nulls in [2, 1] {
        let metadata = footer(
            "message f { optional int32 a; }",
            &[
                (2, vec![finite.clone()]),
                (
                    2,
                    vec![Some(Statistics::int32(
                        None,
                        None,
                        None,
                        Some(nulls),
                        false,
                    ))],
                ),
            ],
        );
        let result = metrics(&metadata, &leaves, &table);
        assert_eq!(result.value_counts[&1], 4);
        assert_eq!(result.null_value_counts[&1], nulls);
        assert_eq!(result.lower_bounds.contains_key(&1), nulls == 2);
        assert_eq!(result.upper_bounds.contains_key(&1), nulls == 2);
    }
    let metadata = footer("message f { optional int32 a; }", &[]);
    let result = metrics(&metadata, &leaves, &table);
    assert_eq!(result.record_count, 0);
    assert_eq!(result.value_counts[&1], 0);
    assert_eq!(result.null_value_counts[&1], 0);
    assert!(
        result.column_sizes.is_empty()
            && result.lower_bounds.is_empty()
            && result.upper_bounds.is_empty()
    );
}

#[test]
fn int96_keeps_chunk_counts_and_never_bounds_even_without_a_target_column() {
    let table = schema(&[(1, PrimitiveType::Int)]);
    let leaves = vec![leaf(99)];
    for (second, expected_nulls) in [
        (None, None),
        (
            Some(Statistics::int96(None, None, None, Some(1), false)),
            Some(1),
        ),
    ] {
        let metadata = footer(
            "message f { optional int96 discarded = 99; }",
            &[
                (
                    2,
                    vec![Some(Statistics::int96(None, None, None, Some(0), false))],
                ),
                (1, vec![second]),
            ],
        );
        let result = metrics(&metadata, &leaves, &table);
        assert_eq!(result.column_sizes[&99], 46);
        assert_eq!(result.value_counts[&99], 3);
        assert_eq!(result.null_value_counts.get(&99).copied(), expected_nulls);
        assert!(result.lower_bounds.is_empty() && result.upper_bounds.is_empty());
    }
    let empty = footer("message f { optional int96 discarded = 99; }", &[]);
    let result = metrics(&empty, &leaves, &table);
    assert!(
        result.column_sizes.is_empty()
            && result.value_counts.is_empty()
            && result.null_value_counts.is_empty()
    );
}

#[test]
fn float_promotion_and_all_nan_proof_preserve_signed_zero_order() {
    let table = schema(&[(1, PrimitiveType::Double)]);
    let leaves = vec![leaf(1)];
    for nan_count in [Some(2), None] {
        let metadata = footer(
            "message f { optional float a; }",
            &[
                (
                    2,
                    vec![Some(Statistics::float(
                        Some(0.0),
                        Some(0.0),
                        None,
                        Some(0),
                        false,
                    ))],
                ),
                (
                    2,
                    vec![Some(Statistics::float(
                        Some(-0.0),
                        Some(-0.0),
                        None,
                        Some(0),
                        false,
                    ))],
                ),
                (
                    2,
                    vec![Some(Statistics::Float(
                        ValueStatistics::new(Some(f32::NAN), Some(f32::NAN), None, Some(0), false)
                            .with_nan_count(nan_count),
                    ))],
                ),
            ],
        );
        let result = metrics(&metadata, &leaves, &table);
        assert_eq!(result.value_counts[&1], 6);
        assert_eq!(result.null_value_counts[&1], 0);
        if nan_count.is_some() {
            assert_eq!(
                result.lower_bounds[&1].to_bytes().unwrap().as_ref(),
                &(-0.0f64).to_le_bytes()
            );
            assert_eq!(
                result.upper_bounds[&1].to_bytes().unwrap().as_ref(),
                &0.0f64.to_le_bytes()
            );
        } else {
            assert!(result.lower_bounds.is_empty() && result.upper_bounds.is_empty());
        }
    }
}

#[test]
fn converts_physical_numeric_and_temporal_statistics() {
    let message = "message f {
        optional boolean truth;
        optional int32 small (INT_8);
        optional int32 short (INT_16);
        optional int32 day (DATE);
        optional int32 time_ms (TIME(MILLIS,false));
        optional int64 time_us (TIME(MICROS,false));
        optional int64 time_ns (TIME(NANOS,false));
        optional int64 stamp_ms (TIMESTAMP(MILLIS,true));
        optional int64 stamp_us (TIMESTAMP(MICROS,false));
        optional int64 stamp_ns (TIMESTAMP(NANOS,true));
    }";
    let metadata = footer(
        message,
        &[(
            2,
            vec![
                Some(Statistics::boolean(
                    Some(false),
                    Some(true),
                    None,
                    Some(0),
                    false,
                )),
                Some(Statistics::int32(
                    Some(-120),
                    Some(127),
                    None,
                    Some(0),
                    true,
                )),
                Some(Statistics::int32(
                    Some(-32_768),
                    Some(32_767),
                    None,
                    Some(0),
                    false,
                )),
                Some(Statistics::int32(Some(-1), Some(4), None, Some(0), false)),
                Some(Statistics::int32(Some(3), Some(6), None, Some(0), false)),
                Some(Statistics::int64(Some(3), Some(6), None, Some(0), false)),
                Some(Statistics::int64(
                    Some(3_000),
                    Some(6_000),
                    None,
                    Some(0),
                    false,
                )),
                Some(Statistics::int64(Some(-3), Some(6), None, Some(0), false)),
                Some(Statistics::int64(Some(-3), Some(6), None, Some(0), false)),
                Some(Statistics::int64(Some(-3), Some(6), None, Some(0), false)),
            ],
        )],
    );
    let fields = [
        PrimitiveType::Boolean,
        PrimitiveType::Int,
        PrimitiveType::Long,
        PrimitiveType::Date,
        PrimitiveType::Time,
        PrimitiveType::Time,
        PrimitiveType::Time,
        PrimitiveType::Timestamptz,
        PrimitiveType::Timestamp,
        PrimitiveType::TimestamptzNs,
    ];
    let table = schema(&(1..).zip(fields).collect::<Vec<_>>());
    let result = metrics(&metadata, &(1..=10).map(leaf).collect(), &table);
    assert_eq!(
        result.lower_bounds,
        HashMap::from([
            (1, Datum::bool(false)),
            (2, Datum::int(-120)),
            (3, Datum::long(-32_768)),
            (4, Datum::date(-1)),
            (5, Datum::time_micros(3_000).unwrap()),
            (6, Datum::time_micros(3).unwrap()),
            (7, Datum::time_micros(3).unwrap()),
            (8, Datum::timestamptz_micros(-3_000)),
            (9, Datum::timestamp_micros(-3)),
            (10, Datum::timestamptz_nanos(-3)),
        ])
    );
    assert_eq!(result.upper_bounds[&7], Datum::time_micros(6).unwrap());
}

fn binary_stats(low: &[u8], high: &[u8], deprecated: bool) -> Option<Statistics> {
    Some(Statistics::byte_array(
        Some(ByteArray::from(low.to_vec())),
        Some(ByteArray::from(high.to_vec())),
        None,
        Some(0),
        deprecated,
    ))
}

fn fixed_stats(low: &[u8], high: &[u8]) -> Option<Statistics> {
    Some(Statistics::fixed_len_byte_array(
        Some(FixedLenByteArray::from(ByteArray::from(low.to_vec()))),
        Some(FixedLenByteArray::from(ByteArray::from(high.to_vec()))),
        None,
        Some(0),
        false,
    ))
}

#[test]
fn decimal_uses_signed_big_endian_shortest_bounds_for_all_physical_encodings() {
    let metadata = footer(
        "message f {
        optional int32 a (DECIMAL(8,2)); optional int64 b (DECIMAL(12,2));
        optional fixed_len_byte_array(6) c (DECIMAL(12,2)); optional binary d (DECIMAL(12,2));
    }",
        &[(
            2,
            vec![
                Some(Statistics::int32(
                    Some(-50111),
                    Some(40231),
                    None,
                    Some(0),
                    false,
                )),
                Some(Statistics::int64(
                    Some(-50111),
                    Some(40231),
                    None,
                    Some(0),
                    false,
                )),
                fixed_stats(&[255, 255, 255, 255, 60, 65], &[0, 0, 0, 0, 157, 39]),
                binary_stats(&[255, 60, 65], &[0, 157, 39], false),
            ],
        )],
    );
    let table = schema(
        &(1..=4)
            .map(|id| {
                (
                    id,
                    PrimitiveType::Decimal {
                        precision: 13,
                        scale: 2,
                    },
                )
            })
            .collect::<Vec<_>>(),
    );
    let result = metrics(&metadata, &(1..=4).map(leaf).collect(), &table);
    for id in 1..=4 {
        assert_eq!(
            result.lower_bounds[&id].to_bytes().unwrap().as_ref(),
            &[0xff, 0x3c, 0x41]
        );
        assert_eq!(
            result.upper_bounds[&id].to_bytes().unwrap().as_ref(),
            &[0x00, 0x9d, 0x27]
        );
    }
}

#[test]
fn bounds_use_primitive_order_ignore_old_binary_and_invalid_utf8() {
    let low_uuid = [0x7f; 16];
    let high_uuid = [0x80; 16];
    let metadata = footer(
        "message f {
        optional binary text (STRING); optional binary old_format;
        optional fixed_len_byte_array(20) fixed; optional fixed_len_byte_array(16) uuid (UUID);
    }",
        &[(
            2,
            vec![
                binary_stats(b"ok", b"\xff", false),
                binary_stats(b"a", b"z", true),
                fixed_stats(&[0x7f; 20], &[0x80; 20]),
                fixed_stats(&low_uuid, &high_uuid),
            ],
        )],
    );
    let table = schema(&[
        (1, PrimitiveType::String),
        (2, PrimitiveType::Binary),
        (3, PrimitiveType::Fixed(20)),
        (4, PrimitiveType::Uuid),
    ]);
    let result = metrics(&metadata, &(1..=4).map(leaf).collect(), &table);
    assert_eq!(result.value_counts.len(), 4);
    assert_eq!(result.lower_bounds.len(), 2);
    assert_eq!(result.upper_bounds.len(), 2);
    assert_eq!(
        result.lower_bounds[&3].to_bytes().unwrap().as_ref(),
        &[0x7f; 20]
    );
    assert_eq!(
        result.upper_bounds[&4].to_bytes().unwrap().as_ref(),
        &high_uuid
    );
}

#[test]
fn truncation_carries_back_and_uses_unicode_scalars() {
    let text = "💡".repeat(16);
    assert_eq!(
        string_bound(&text, true).unwrap().as_slice(),
        text.as_bytes()
    );
    let larger = format!("{text}α");
    assert_eq!(
        string_bound(&larger, false).unwrap().as_slice(),
        text.as_bytes()
    );
    assert_eq!(
        string_bound(&larger, true).unwrap().as_slice(),
        format!("{}💢", "💡".repeat(15)).as_bytes()
    );
    let gap = format!("{}\u{d7ff}a", "a".repeat(15));
    assert_eq!(
        string_bound(&gap, true).unwrap().as_slice(),
        format!("{}\u{e000}", "a".repeat(15)).as_bytes()
    );
    let carry = format!("a{}", "\u{10ffff}".repeat(16));
    assert_eq!(string_bound(&carry, true).unwrap().as_slice(), b"b");
    let overflow = "\u{10ffff}".repeat(17);
    assert!(string_bound(&overflow, true).is_none());
    assert_eq!(string_bound(&overflow, false).unwrap().as_slice().len(), 64);
    assert_eq!(
        binary_bound(&[0xff; 16], true).unwrap().as_slice(),
        &[0xff; 16]
    );
    assert!(binary_bound(&[0xff; 17], true).is_none());
    let mut binary = [0xff; 17];
    binary[7] = 0xfe;
    assert_eq!(binary_bound(&binary, true).unwrap().as_slice(), &[0xff; 8]);
    assert_eq!(
        binary_bound(&binary, false).unwrap().as_slice(),
        &binary[..16]
    );
}

#[test]
fn preflight_accounts_retained_map_tables_and_variable_bounds() {
    let metadata = footer(
        "message f { optional binary text (STRING); }",
        &[(2, vec![binary_stats(b"abc", b"xyz", false)])],
    );
    let table = schema(&[(1, PrimitiveType::String)]);
    let leaves = vec![leaf(1)];
    let plan = analyze_footer_metrics(&metadata, &leaves, &table).unwrap();
    let counts = fresh_table_layout::<i32, u64>(1)
        .unwrap()
        .request_bytes_upper_bound;
    let bounds = fresh_table_layout::<i32, Datum>(1)
        .unwrap()
        .request_bytes_upper_bound;
    assert_eq!(plan.retained_bytes(), 3 * counts + 2 * bounds + 6);
    let result = plan.materialize().unwrap();
    assert_eq!(result.lower_bounds[&1], Datum::string("abc"));
}

fn read_footer(encoded: &str) -> SerializedFileReader<Bytes> {
    let encoded: String = encoded.lines().collect();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .unwrap();
    SerializedFileReader::new(Bytes::from(bytes)).unwrap()
}

fn hex_bytes(string: &str) -> Vec<u8> {
    (0..string.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&string[index..index + 2], 16).unwrap())
        .collect()
}

#[test]
fn matches_p01_footer_references() {
    let reader = read_footer(include_str!(
        "../../../../../tests/sql/fixtures/iceberg-add-files/metrics.parquet.b64"
    ));
    let java: Json = serde_json::from_str(include_str!(
        "tests/fixtures/iceberg-java-1.12.0-metrics.json"
    ))
    .unwrap();
    let table = schema(&[
        (1, PrimitiveType::Long),
        (
            2,
            PrimitiveType::Decimal {
                precision: 12,
                scale: 2,
            },
        ),
        (3, PrimitiveType::String),
        (4, PrimitiveType::Binary),
        (7, PrimitiveType::Int),
        (8, PrimitiveType::String),
    ]);
    let leaves = [1, 2, 3, 4, 7, 8, 10, 11]
        .into_iter()
        .map(|id| ResolvedParquetLeaf {
            field_id: Some(id),
            repeated: id >= 10,
        })
        .collect();
    let actual = metrics(reader.metadata(), &leaves, &table);
    assert_eq!(actual.record_count, java["record_count"].as_u64().unwrap());
    for (actual, key) in [
        (&actual.column_sizes, "column_sizes"),
        (&actual.value_counts, "value_counts"),
        (&actual.null_value_counts, "null_value_counts"),
    ] {
        assert_eq!(serde_json::to_value(actual).unwrap(), java[key]);
    }
    for (actual, key) in [
        (&actual.lower_bounds, "lower_bounds_hex"),
        (&actual.upper_bounds, "upper_bounds_hex"),
    ] {
        let expected = java[key].as_object().unwrap();
        assert_eq!(actual.len(), expected.len());
        for (id, expected) in expected {
            let bound = &actual[&id.parse::<i32>().unwrap()];
            let bytes = hex_bytes(expected.as_str().unwrap());
            assert_eq!(
                *bound,
                Datum::try_from_bytes(&bytes, bound.data_type().clone()).unwrap()
            );
        }
    }

    let table = schema(&[(1, PrimitiveType::Int)]);
    let leaves = vec![leaf(1), leaf(99)];
    for (encoded, extra_null_count) in [
        (
            include_str!("tests/fixtures/embedded-extra-int96.parquet.b64"),
            None,
        ),
        (
            include_str!("tests/fixtures/embedded-extra-int96-known-nulls.parquet.b64"),
            Some(1),
        ),
    ] {
        let reader = read_footer(encoded);
        let result = metrics(reader.metadata(), &leaves, &table);
        assert_eq!(result.column_sizes, HashMap::from([(1, 160), (99, 113)]));
        assert_eq!(result.value_counts, HashMap::from([(1, 3), (99, 3)]));
        let mut expected_nulls = HashMap::from([(1, 0)]);
        if let Some(null_count) = extra_null_count {
            expected_nulls.insert(99, null_count);
        }
        assert_eq!(result.null_value_counts, expected_nulls);
        assert_eq!(result.lower_bounds, HashMap::from([(1, Datum::int(11))]));
        assert_eq!(result.upper_bounds, HashMap::from([(1, Datum::int(33))]));
    }
}
