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

//! Independent expectations follow Trino 483 JsonEncodingUtils.java:177-515,
//! TimeZoneKey.java:261-322 and VariantUtil.java:1228-1304.
use super::*;
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{Field, Schema, i256};
use novarocks_result_contract::{NamedRenderField, TrinoJsonFacts};
fn field(native_type: N, nullable: bool) -> RenderField {
    RenderField {
        native_type,
        nullable,
        presentation: P::TrinoValue,
    }
}
fn encoder(
    arrays: Vec<ArrayRef>,
    fields: Vec<RenderField>,
    facts: TrinoJsonFacts,
) -> ArrowTrinoJsonEncoder {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(
            arrays
                .iter()
                .zip(&fields)
                .enumerate()
                .map(|(i, (a, f))| Field::new(format!("c{i}"), a.data_type().clone(), f.nullable))
                .collect::<Vec<_>>(),
        )),
        arrays,
    )
    .unwrap();
    let columns = fields
        .into_iter()
        .enumerate()
        .map(|(i, f)| novarocks_result_contract::RenderColumn {
            source_ordinal: i as u32,
            source_slot: None,
            name: format!("c{i}"),
            field: f,
        })
        .collect();
    ArrowTrinoJsonEncoder::try_new(
        Arc::new(
            ClientRenderSchema::try_new(
                ClientPresentationFamily::TrinoJson(facts),
                columns,
                batch.num_columns(),
            )
            .unwrap(),
        ),
        batch,
    )
    .unwrap()
}
fn payloads(mut e: impl BoundedClientRowEncoder, segment: usize) -> Vec<String> {
    assert!(
        segment >= 5,
        "a row prefix and first payload byte require five bytes"
    );
    let mut body = vec![];
    let mut output = vec![0; segment];
    let mut stalled_turns = 0;
    loop {
        let turn = e.step(&mut output).unwrap();
        assert!(turn.emitted_bytes + turn.examined_bytes <= V::EMIT_BYTES_PER_TURN);
        assert!(turn.visited_cells <= V::CELLS_PER_TURN);
        if turn.emitted_bytes + turn.examined_bytes + turn.visited_cells == 0 {
            stalled_turns += 1;
            assert!(
                stalled_turns < 8,
                "encoder stalled despite a legal output buffer"
            );
        } else {
            stalled_turns = 0;
        }
        body.extend_from_slice(&output[..turn.emitted_bytes]);
        if turn.status == RenderTurnStatus::InputComplete {
            break;
        }
    }
    let mut rows = vec![];
    let mut at = 0;
    while at < body.len() {
        let n = u32::from_le_bytes(body[at..at + 4].try_into().unwrap()) as usize;
        rows.push(String::from_utf8(body[at + 4..at + 4 + n].to_vec()).unwrap());
        at += 4 + n;
    }
    rows
}
fn nested_decimal256_encoder(values: Vec<Option<i256>>, scale: i8) -> ArrowTrinoJsonEncoder {
    let len = i32::try_from(values.len()).unwrap();
    let values: ArrayRef = Arc::new(
        Decimal256Array::from(values)
            .with_precision_and_scale(38, scale)
            .unwrap(),
    );
    let list = Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, len])),
        values,
        None,
    ));
    encoder(
        vec![list],
        vec![field(
            N::List(Box::new(field(
                N::Decimal {
                    bits: 256,
                    precision: 38,
                    scale,
                },
                true,
            ))),
            false,
        )],
        TrinoJsonFacts::default(),
    )
}

#[test]
fn nested_decimal256_preserves_precision_scale_nulls_and_bounded_segments() {
    let maximum: i256 = "99999999999999999999999999999999999999".parse().unwrap();
    for segment in [5, 11, 1000] {
        assert_eq!(
            payloads(
                nested_decimal256_encoder(
                    vec![
                        Some(i256::ZERO),
                        Some(i256::from_i128(5)),
                        Some(i256::from_i128(-5)),
                        Some(maximum),
                        None,
                    ],
                    2,
                ),
                segment,
            ),
            vec![r#"[["0.00","0.05","-0.05","999999999999999999999999999999999999.99",null]]"#],
        );
        assert_eq!(
            payloads(
                nested_decimal256_encoder(vec![Some(i256::MINUS_ONE), Some(maximum)], 38),
                segment,
            ),
            vec![
                r#"[["-0.00000000000000000000000000000000000001","0.99999999999999999999999999999999999999"]]"#
            ],
        );
    }
}

#[test]
fn nested_decimal256_rejects_coefficients_outside_the_declared_precision() {
    for value in [
        "100000000000000000000000000000000000000"
            .parse::<i256>()
            .unwrap(),
        i256::MIN,
        i256::MAX,
    ] {
        let mut encoder = nested_decimal256_encoder(vec![Some(value)], 2);
        assert_eq!(
            encoder.step(&mut [0; 128]).unwrap_err().kind,
            E::InvalidValue,
        );
    }
}

#[test]
fn decimal256_support_remains_nested_only() {
    let array: ArrayRef = Arc::new(
        Decimal256Array::from_iter_values([i256::ONE])
            .with_precision_and_scale(38, 2)
            .unwrap(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "amount",
            array.data_type().clone(),
            false,
        )])),
        vec![array],
    )
    .unwrap();
    let schema = ClientRenderSchema::try_new(
        ClientPresentationFamily::TrinoJson(TrinoJsonFacts::default()),
        vec![novarocks_result_contract::RenderColumn {
            source_ordinal: 0,
            source_slot: None,
            name: "amount".into(),
            field: field(
                N::Decimal {
                    bits: 256,
                    precision: 38,
                    scale: 2,
                },
                false,
            ),
        }],
        1,
    )
    .unwrap();
    assert!(matches!(
        ArrowTrinoJsonEncoder::try_new(Arc::new(schema), batch),
        Err(RenderError {
            kind: E::UnsupportedPresentation,
            ..
        })
    ));
}

#[test]
fn scalar_type_values_and_nulls_follow_trino_483() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Int8Array::from(vec![Some(-8), None])),
        Arc::new(Int16Array::from(vec![Some(-16), None])),
        Arc::new(Int32Array::from(vec![Some(-32), None])),
        Arc::new(Int64Array::from(vec![Some(-64), None])),
        Arc::new(Float32Array::from(vec![Some(1.0), None])),
        Arc::new(Float64Array::from(vec![Some(-0.0), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(-5), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Arc::new(StringArray::from(vec![Some("a\"\n中文"), None])),
        Arc::new(BinaryArray::from(vec![Some([0, 255].as_slice()), None])),
        Arc::new(Date32Array::from(vec![Some(0), None])),
        Arc::new(StringArray::from(vec![Some("{\"k\":true}"), None])),
    ];
    let types = vec![
        N::Boolean,
        N::SignedInteger(8),
        N::SignedInteger(16),
        N::SignedInteger(32),
        N::SignedInteger(64),
        N::Float32,
        N::Float64,
        N::Decimal {
            bits: 128,
            precision: 10,
            scale: 2,
        },
        N::String,
        N::Binary,
        N::Date,
        N::Json,
    ];
    let rows = payloads(
        encoder(
            arrays,
            types.into_iter().map(|t| field(t, true)).collect(),
            TrinoJsonFacts::default(),
        ),
        13,
    );
    assert_eq!(
        rows[0],
        r#"[true,-8,-16,-32,-64,1.0,-0.0,"-0.05","a\"\n中文","AP8=","1970-01-01","{\"k\":true}"]"#
    );
    assert_eq!(
        rows[1],
        "[null,null,null,null,null,null,null,null,null,null,null,null]"
    );
}
#[test]
fn finite_and_nonfinite_floats_are_numbers_or_trino_strings() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(vec![
            1.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1e20,
            1e-5,
        ])),
        Arc::new(Float32Array::from(vec![
            1.0,
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            1e20,
            1e-5,
        ])),
    ];
    assert_eq!(
        payloads(
            encoder(
                arrays,
                vec![field(N::Float64, true), field(N::Float32, true)],
                TrinoJsonFacts::default()
            ),
            100
        ),
        vec![
            "[1.0,1.0]",
            "[\"NaN\",\"NaN\"]",
            "[\"Infinity\",\"Infinity\"]",
            "[\"-Infinity\",\"-Infinity\"]",
            "[1.0E20,1.0E20]",
            "[1.0E-5,1.0E-5]"
        ]
    );
}
#[test]
fn minimum_subnormals_use_java_nearest_two_digit_decimal() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(vec![f64::from_bits(1)])),
        Arc::new(Float32Array::from(vec![f32::from_bits(1)])),
    ];
    assert_eq!(
        payloads(
            encoder(
                arrays,
                vec![field(N::Float64, true), field(N::Float32, true)],
                TrinoJsonFacts::default()
            ),
            100
        ),
        vec!["[4.9E-324,1.4E-45]"]
    );
}

#[test]
fn temporal_units_rounding_and_timezones_are_type_owned() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Time32SecondArray::from(vec![3661])),
        Arc::new(Time32MillisecondArray::from(vec![3_661_123])),
        Arc::new(Time64MicrosecondArray::from(vec![3_661_123_456])),
        Arc::new(Time64NanosecondArray::from(vec![3_661_123_456_789])),
        Arc::new(TimestampSecondArray::from(vec![0])),
        Arc::new(TimestampMillisecondArray::from(vec![-1])),
        Arc::new(TimestampMicrosecondArray::from(vec![123_500])),
        Arc::new(TimestampNanosecondArray::from(vec![-123_500_000])),
    ];
    let units = [
        RenderTimeUnit::Second,
        RenderTimeUnit::Millisecond,
        RenderTimeUnit::Microsecond,
        RenderTimeUnit::Nanosecond,
    ];
    let fields = units
        .iter()
        .map(|u| field(N::Time { unit: *u }, true))
        .chain(units.iter().map(|u| {
            field(
                N::Timestamp {
                    unit: *u,
                    timezone: None,
                },
                true,
            )
        }))
        .collect::<Vec<_>>();
    assert_eq!(
        payloads(
            encoder(
                arrays.clone(),
                fields.clone(),
                TrinoJsonFacts {
                    parametric_datetime: true,
                    ..TrinoJsonFacts::default()
                }
            ),
            1000
        ),
        vec![
            r#"["01:01:01","01:01:01.123","01:01:01.123456","01:01:01.123456789","1970-01-01 00:00:00","1969-12-31 23:59:59.999","1970-01-01 00:00:00.123500","1969-12-31 23:59:59.876500000"]"#
        ]
    );
    assert_eq!(
        payloads(encoder(arrays, fields, TrinoJsonFacts::default()), 1000),
        vec![
            r#"["01:01:01.000","01:01:01.123","01:01:01.123","01:01:01.123","1970-01-01 00:00:00.000","1969-12-31 23:59:59.999","1970-01-01 00:00:00.124","1969-12-31 23:59:59.877"]"#
        ]
    );
    for (zone, expected) in [
        ("UTC", "1970-01-01 00:00:00.000000 UTC"),
        ("Z", "1970-01-01 00:00:00.000000 UTC"),
        ("Etc/UTC", "1970-01-01 00:00:00.000000 UTC"),
        ("+00:00", "1970-01-01 00:00:00.000000 UTC"),
        ("-00:00", "1970-01-01 00:00:00.000000 UTC"),
        ("+05:30", "1970-01-01 05:30:00.000000 +05:30"),
        ("Asia/Shanghai", "1970-01-01 08:00:00.000000 Asia/Shanghai"),
    ] {
        let array =
            Arc::new(TimestampMicrosecondArray::from(vec![0]).with_timezone(zone)) as ArrayRef;
        assert_eq!(
            payloads(
                encoder(
                    vec![array],
                    vec![field(
                        N::Timestamp {
                            unit: RenderTimeUnit::Microsecond,
                            timezone: Some(zone.into())
                        },
                        true
                    )],
                    TrinoJsonFacts {
                        parametric_datetime: true,
                        ..TrinoJsonFacts::default()
                    }
                ),
                1000
            ),
            vec![format!("[\"{expected}\"]")]
        );
    }
}
fn map(
    keys: ArrayRef,
    value: ArrayRef,
    key: RenderField,
    val: RenderField,
) -> (ArrayRef, RenderField) {
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", keys.data_type().clone(), key.nullable)),
            Arc::new(Field::new("value", value.data_type().clone(), val.nullable)),
        ]
        .into(),
        vec![keys, value],
        None,
    );
    let array = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, entries.len() as i32])),
        entries,
        None,
        false,
    ));
    (
        array,
        field(
            N::Map {
                key: Box::new(key),
                value: Box::new(val),
            },
            true,
        ),
    )
}
#[test]
fn nested_decimal256_map_keys_keep_their_declared_scale() {
    let keys: ArrayRef = Arc::new(
        Decimal256Array::from_iter_values([i256::from_i128(-5), i256::from_i128(1200)])
            .with_precision_and_scale(38, 2)
            .unwrap(),
    );
    let (array, render_field) = map(
        keys,
        Arc::new(Int32Array::from(vec![1, 2])),
        field(
            N::Decimal {
                bits: 256,
                precision: 38,
                scale: 2,
            },
            false,
        ),
        field(N::SignedInteger(32), false),
    );
    assert_eq!(
        payloads(
            encoder(vec![array], vec![render_field], TrinoJsonFacts::default()),
            5,
        ),
        vec![r#"[{"-0.05":1,"12.00":2}]"#],
    );
}
#[test]
fn nested_rows_arrays_and_map_keys_use_their_distinct_projections() {
    let (m, mf) = map(
        Arc::new(Int64Array::from(vec![2, 7])),
        Arc::new(StringArray::from(vec![Some("x"), None])),
        field(N::SignedInteger(64), false),
        field(N::String, true),
    );
    let list = Arc::new(ListArray::new(
        Arc::new(Field::new("item", m.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        m,
        None,
    ));
    let lf = field(N::List(Box::new(mf)), true);
    let array = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "values",
            list.data_type().clone(),
            true,
        ))]
        .into(),
        vec![list],
        None,
    ));
    let native = N::Struct(vec![NamedRenderField {
        name: "values".into(),
        field: lf,
    }]);
    assert_eq!(
        payloads(
            encoder(
                vec![array],
                vec![field(native, true)],
                TrinoJsonFacts::default()
            ),
            7
        ),
        vec![r#"[[[{"2":"x","7":null}]]]"#]
    );
    let (m, mf) = map(
        Arc::new(TimestampMicrosecondArray::from(vec![123_456])),
        Arc::new(Int32Array::from(vec![1])),
        field(
            N::Timestamp {
                unit: RenderTimeUnit::Microsecond,
                timezone: None,
            },
            false,
        ),
        field(N::SignedInteger(32), false),
    );
    assert_eq!(
        payloads(encoder(vec![m], vec![mf], TrinoJsonFacts::default()), 100),
        vec![r#"[{"1970-01-01 00:00:00.123456":1}]"#]
    );
    let (m, mf) = map(
        Arc::new(BinaryArray::from(vec![[0, 255].as_slice()])),
        Arc::new(Int32Array::from(vec![1])),
        field(N::Binary, false),
        field(N::SignedInteger(32), false),
    );
    assert_eq!(
        payloads(encoder(vec![m], vec![mf], TrinoJsonFacts::default()), 100),
        vec![r#"[{"AP8=":1}]"#]
    );
}
fn serialized(meta: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = ((meta.len() + value.len()) as u32).to_le_bytes().to_vec();
    out.extend(meta);
    out.extend(value);
    out
}
#[test]
fn variant_capabilities_and_nested_variants_preserve_json_or_binary() {
    let value = serialized(
        &[1, 0, 0],
        &[1 | (6 << 2), 0xe4, 0xb8, 0xad, 0xe6, 0x96, 0x87],
    );
    for (facts, expected) in [
        (TrinoJsonFacts::default(), r#"["\"中文\""]"#),
        (
            TrinoJsonFacts {
                variant: true,
                ..TrinoJsonFacts::default()
            },
            r#"["中文"]"#,
        ),
        (
            TrinoJsonFacts {
                variant_binary: true,
                ..TrinoJsonFacts::default()
            },
            r#"[{"metadata":"AQAA","value":"GeS4reaWhw=="}]"#,
        ),
    ] {
        assert_eq!(
            payloads(
                encoder(
                    vec![Arc::new(LargeBinaryArray::from(vec![value.as_slice()]))],
                    vec![field(N::Variant, true)],
                    facts
                ),
                11
            ),
            vec![expected]
        );
    }
    for (tag, expected) in [
        (12, r#"["1970-01-01 00:00:00.000001 UTC"]"#),
        (13, r#"["1970-01-01 00:00:00.000001"]"#),
        (17, r#"["00:00:00.000001"]"#),
        (18, r#"["1970-01-01 00:00:00.000000001 UTC"]"#),
        (19, r#"["1970-01-01 00:00:00.000000001"]"#),
    ] {
        let mut data = vec![tag << 2];
        data.extend(1i64.to_le_bytes());
        let value = serialized(&[1, 0, 0], &data);
        assert_eq!(
            payloads(
                encoder(
                    vec![Arc::new(LargeBinaryArray::from(vec![value.as_slice()]))],
                    vec![field(N::Variant, true)],
                    TrinoJsonFacts {
                        variant: true,
                        ..TrinoJsonFacts::default()
                    }
                ),
                100
            ),
            vec![expected]
        );
    }
}
#[test]
fn nested_variant_and_decimal_json_preserve_trino_text() {
    let text = serialized(
        &[1, 0, 0],
        &[1 | (6 << 2), 0xe4, 0xb8, 0xad, 0xe6, 0x96, 0x87],
    );
    let values = Arc::new(LargeBinaryArray::from(vec![text.as_slice()]));
    let list = Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        values,
        None,
    ));
    assert_eq!(
        payloads(
            encoder(
                vec![list],
                vec![field(N::List(Box::new(field(N::Variant, true))), true)],
                TrinoJsonFacts::default()
            ),
            11
        ),
        vec![r#"[["\"中文\""]]"#]
    );
    for (scale, unscaled, expected) in [(2u8, 1200i32, "12.00"), (8, 5, "5E-8"), (8, 0, "0E-8")] {
        let mut data = vec![8 << 2, scale];
        data.extend(unscaled.to_le_bytes());
        let value = serialized(&[1, 0, 0], &data);
        assert_eq!(
            payloads(
                encoder(
                    vec![Arc::new(LargeBinaryArray::from(vec![value.as_slice()]))],
                    vec![field(N::Variant, true)],
                    TrinoJsonFacts::default()
                ),
                100
            ),
            vec![format!("[\"{expected}\"]")]
        );
    }
    let meta = [1, 1, 0, 6, 0xe4, 0xb8, 0xad, 0xe6, 0x96, 0x87];
    let value = serialized(&meta, &[2, 1, 0, 0, 1, 0]);
    assert_eq!(
        payloads(
            encoder(
                vec![Arc::new(LargeBinaryArray::from(vec![value.as_slice()]))],
                vec![field(N::Variant, true)],
                TrinoJsonFacts {
                    variant: true,
                    ..TrinoJsonFacts::default()
                }
            ),
            11
        ),
        vec![r#"[{"中文":null}]"#]
    );
    assert_eq!(
        payloads(
            encoder(
                vec![Arc::new(NullArray::new(1))],
                vec![field(N::Null, true)],
                TrinoJsonFacts::default()
            ),
            11
        ),
        vec!["[null]"]
    );
}

#[test]
fn large_escaped_rows_keep_the_same_quantum_and_staging_bound() {
    let value = "\n中文".repeat(40_000);
    let mut e = encoder(
        vec![Arc::new(StringArray::from(vec![value.as_str()]))],
        vec![field(N::String, true)],
        TrinoJsonFacts::default(),
    );
    assert!(e.scratch_capacity_bytes() < 2 * 1024 * 1024);
    let mut out = [0; 4];
    for _ in 0..1000 {
        let turn = e.step(&mut out).unwrap();
        if turn.status == RenderTurnStatus::NeedsOutput {
            assert_eq!(turn.emitted_bytes, 0);
            break;
        }
    }
    let rows = payloads(e, 17);
    assert_eq!(rows[0], format!("[\"{}\"]", "\\n中文".repeat(40_000)));
    let mut e = encoder(
        vec![Arc::new(StringArray::from(vec!["a"]))],
        vec![field(N::String, true)],
        TrinoJsonFacts::default(),
    );
    e.cancel();
    assert_eq!(e.step(&mut [0; 100]).unwrap_err().kind, E::Cancelled);
}

#[test]
fn frozen_family_selects_wire_payload_without_presentation_fallback() {
    use crate::ClientRowEncoder;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            arrow::datatypes::DataType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(vec!["v"]))],
    )
    .unwrap();
    let column = |presentation| novarocks_result_contract::RenderColumn {
        source_ordinal: 0,
        source_slot: None,
        name: "v".into(),
        field: RenderField {
            native_type: N::String,
            presentation,
            nullable: true,
        },
    };
    let trino = Arc::new(
        ClientRenderSchema::try_new(
            ClientPresentationFamily::TrinoJson(TrinoJsonFacts::default()),
            vec![column(P::TrinoValue)],
            1,
        )
        .unwrap(),
    );
    assert!(ArrowMysqlTextEncoder::try_new(trino.clone(), batch.clone()).is_err());
    let cursor = ClientRowEncoder::try_new(trino, batch.clone()).unwrap();
    assert!(matches!(cursor, ClientRowEncoder::TrinoJson(_)));
    assert_eq!(payloads(cursor, 100), vec![r#"["v"]"#]);
    let mysql = Arc::new(
        ClientRenderSchema::try_new(
            ClientPresentationFamily::MysqlText,
            vec![column(P::ScalarText)],
            1,
        )
        .unwrap(),
    );
    assert!(ArrowTrinoJsonEncoder::try_new(mysql.clone(), batch.clone()).is_err());
    let mut cursor = ClientRowEncoder::try_new(mysql, batch).unwrap();
    let mut output = [0; 100];
    let turn = cursor.step(&mut output).unwrap();
    assert_eq!(&output[..turn.emitted_bytes], &[2, 0, 0, 0, 1, b'v']);
}

#[test]
fn nulls_of_every_supported_native_family_are_json_null() {
    use arrow::datatypes::DataType as D;
    let mut types = vec![
        (N::Null, D::Null),
        (N::Boolean, D::Boolean),
        (N::SignedInteger(8), D::Int8),
        (N::SignedInteger(16), D::Int16),
        (N::SignedInteger(32), D::Int32),
        (N::SignedInteger(64), D::Int64),
        (N::Float32, D::Float32),
        (N::Float64, D::Float64),
        (
            N::Decimal {
                bits: 128,
                precision: 10,
                scale: 2,
            },
            D::Decimal128(10, 2),
        ),
        (N::String, D::Utf8),
        (N::Binary, D::Binary),
        (N::Date, D::Date32),
        (N::Json, D::Utf8),
        (N::Variant, D::LargeBinary),
    ];
    for (unit, arrow) in [
        (RenderTimeUnit::Second, TimeUnit::Second),
        (RenderTimeUnit::Millisecond, TimeUnit::Millisecond),
        (RenderTimeUnit::Microsecond, TimeUnit::Microsecond),
        (RenderTimeUnit::Nanosecond, TimeUnit::Nanosecond),
    ] {
        types.push((
            N::Time { unit },
            if matches!(unit, RenderTimeUnit::Second | RenderTimeUnit::Millisecond) {
                D::Time32(arrow)
            } else {
                D::Time64(arrow)
            },
        ));
        types.push((
            N::Timestamp {
                unit,
                timezone: None,
            },
            D::Timestamp(arrow, None),
        ));
        types.push((
            N::Timestamp {
                unit,
                timezone: Some("UTC".into()),
            },
            D::Timestamp(arrow, Some("UTC".into())),
        ));
    }
    types.push((
        N::List(Box::new(field(N::String, true))),
        D::List(Arc::new(Field::new("item", D::Utf8, true))),
    ));
    let children = vec![
        Arc::new(Field::new("key", D::Utf8, false)),
        Arc::new(Field::new("value", D::Int32, true)),
    ];
    types.push((
        N::Map {
            key: Box::new(field(N::String, false)),
            value: Box::new(field(N::SignedInteger(32), true)),
        },
        D::Map(
            Arc::new(Field::new("entries", D::Struct(children.into()), false)),
            false,
        ),
    ));
    types.push((
        N::Struct(vec![NamedRenderField {
            name: "v".into(),
            field: field(N::String, true),
        }]),
        D::Struct(vec![Arc::new(Field::new("v", D::Utf8, true))].into()),
    ));
    let arrays = types
        .iter()
        .map(|(_, data_type)| new_null_array(data_type, 1))
        .collect();
    let fields = types
        .iter()
        .map(|(native, _)| field(native.clone(), true))
        .collect();
    let expected = format!("[{}]", vec!["null"; types.len()].join(","));
    assert_eq!(
        payloads(encoder(arrays, fields, TrinoJsonFacts::default()), 11),
        vec![expected]
    );
}
