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
use arrow::array::*;
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_result_contract::{
    ClientRenderSchema, ClientRowProfile, ClientRowStreamCursor, NamedRenderField,
    NativeRenderType as N, OpaqueRenderType, RenderColumn, RenderField, RenderPresentation as P,
    RenderTimeUnit as U, RootProfileV1 as V,
};
use novarocks_result_render::{
    ArrowMysqlTextEncoder, BoundedClientRowEncoder, RenderErrorKind as E, RenderTurnStatus,
};
use std::sync::Arc;
fn field(n: N, p: P, nullable: bool) -> RenderField {
    RenderField {
        native_type: n,
        presentation: p,
        nullable,
    }
}
fn input(arrays: Vec<ArrayRef>) -> RecordBatch {
    let fs = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(i.to_string(), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fs)), arrays).unwrap()
}
fn encoder(batch: RecordBatch, fields: Vec<RenderField>) -> ArrowMysqlTextEncoder {
    let cols = fields
        .into_iter()
        .enumerate()
        .map(|(i, field)| RenderColumn {
            source_ordinal: i as u32,
            source_slot: None,
            name: i.to_string(),
            field,
        })
        .collect();
    ArrowMysqlTextEncoder::try_new(
        Arc::new(
            ClientRenderSchema::try_new(
                novarocks_result_contract::ClientPresentationFamily::MysqlText,
                cols,
                batch.num_columns(),
            )
            .unwrap(),
        ),
        batch,
    )
    .unwrap()
}
fn string_encoder(values: Vec<&str>) -> ArrowMysqlTextEncoder {
    encoder(
        input(vec![Arc::new(StringArray::from(values))]),
        vec![field(N::String, P::ScalarText, true)],
    )
}
fn encode(mut e: ArrowMysqlTextEncoder, size: usize) -> (Vec<u8>, Vec<usize>) {
    let mut out = vec![0; size];
    let mut all = Vec::new();
    let mut lengths = Vec::new();
    let mut frontier = ClientRowStreamCursor::new();
    for _ in 0..2_000_000 {
        let t = e.step(&mut out).unwrap();
        assert!(t.emitted_bytes + t.examined_bytes <= V::EMIT_BYTES_PER_TURN);
        assert!(t.visited_cells <= V::CELLS_PER_TURN);
        if t.emitted_bytes != 0 {
            frontier = frontier
                .validate_body(
                    ClientRowProfile::try_new(V::SEGMENT_BYTES, V::ROW_PAYLOAD_BYTES).unwrap(),
                    &out[..t.emitted_bytes],
                )
                .unwrap()
                .after();
            all.extend_from_slice(&out[..t.emitted_bytes]);
            lengths.push(t.emitted_bytes);
        }
        if t.status == RenderTurnStatus::InputComplete {
            frontier.validate_end().unwrap();
            return (all, lengths);
        }
    }
    panic!("cursor did not finish")
}
fn row(payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(payload);
    out
}
fn cell(value: &[u8]) -> Vec<u8> {
    let mut out = if value.len() < 251 {
        vec![value.len() as u8]
    } else if value.len() <= 65535 {
        vec![0xfc, value.len() as u8, (value.len() >> 8) as u8]
    } else if value.len() <= 0xffffff {
        vec![
            0xfd,
            value.len() as u8,
            (value.len() >> 8) as u8,
            (value.len() >> 16) as u8,
        ]
    } else {
        let mut v = vec![0xfe];
        v.extend_from_slice(&(value.len() as u64).to_le_bytes());
        v
    };
    out.extend_from_slice(value);
    out
}
#[test]
fn scalar_exact_bytes() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![i64::MIN])),
        Arc::new(UInt64Array::from(vec![u64::MAX])),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(Float32Array::from(vec![1.25])),
        Arc::new(
            Decimal128Array::from(vec![-12345])
                .with_precision_and_scale(8, 3)
                .unwrap(),
        ),
        Arc::new(Date32Array::from(vec![-719560])),
        Arc::new(Time64MicrosecondArray::from(vec![93_784_000_005])),
        Arc::new(TimestampNanosecondArray::from(vec![1_999])),
    ];
    let fields = vec![
        field(N::SignedInteger(64), P::ScalarText, true),
        field(N::UnsignedInteger(64), P::ScalarText, true),
        field(N::Boolean, P::ScalarText, true),
        field(N::Float32, P::ScalarText, true),
        field(
            N::Decimal {
                bits: 128,
                precision: 8,
                scale: 3,
            },
            P::ScalarText,
            true,
        ),
        field(N::Date, P::ScalarText, true),
        field(
            N::Time {
                unit: U::Microsecond,
            },
            P::ScalarText,
            true,
        ),
        field(
            N::Timestamp {
                unit: U::Nanosecond,
                timezone: None,
            },
            P::TimestampUtcMicros,
            true,
        ),
    ];
    let mut payload = Vec::new();
    for s in [
        "-9223372036854775808",
        "18446744073709551615",
        "1",
        "1.25",
        "-12.345",
        "0000-00-00",
        "26:03:04.000005",
        "1970-01-01 00:00:00.000001",
    ] {
        payload.extend(cell(s.as_bytes()));
    }
    assert_eq!(
        encode(encoder(input(arrays), fields), 1 << 20).0,
        row(&payload)
    );
}
#[test]
fn small_threshold_and_lenenc_boundaries() {
    for total in [
        V::SMALL_ROW_BYTES - 1,
        V::SMALL_ROW_BYTES,
        V::SMALL_ROW_BYTES + 1,
    ] {
        let prefix = if total - 3 <= 65535 { 3 } else { 4 };
        let value = "x".repeat(total - prefix);
        let expected = row(&cell(value.as_bytes()));
        let (actual, lengths) = encode(string_encoder(vec![&value]), 1 << 20);
        assert_eq!(actual, expected);
        assert!(lengths.iter().all(|n| *n > 4));
    }
    for n in [0, 1, 250, 251, 65535, 65536, 0xffffff, 0x1000000] {
        let value = "x".repeat(n);
        assert_eq!(
            encode(string_encoder(vec![&value]), 65536).0,
            row(&cell(value.as_bytes()))
        );
    }
}
#[test]
fn prefix_quantum_and_short_continuation() {
    let value = "x".repeat(65534);
    for size in [5, 6, 7, 255, 256, 257, 65535, 65536, 65537, 1 << 20] {
        assert_eq!(
            encode(string_encoder(vec![&value, "", &value]), size).0,
            [
                row(&cell(value.as_bytes())),
                row(&cell(b"")),
                row(&cell(value.as_bytes()))
            ]
            .concat()
        );
    }
    for size in 0..5 {
        let mut e = string_encoder(vec!["x"]);
        let t = e.step(&mut vec![0; size]).unwrap();
        assert_eq!(t.emitted_bytes, 0);
        assert_eq!(t.status, RenderTurnStatus::NeedsOutput);
    }
    let mut e = string_encoder(vec![&value]);
    let mut out = vec![0; 5];
    loop {
        let t = e.step(&mut out).unwrap();
        if t.emitted_bytes != 0 {
            assert_eq!(t.emitted_bytes, 5);
            break;
        }
    }
    let t = e.step(&mut [0]).unwrap();
    assert_eq!(t.emitted_bytes, 1);
}
#[test]
fn nested_struct_json_time_timestamp_binary() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![r#"{"a":[1,{"b":null}]}"#])),
        Arc::new(Time64MicrosecondArray::from(vec![-1])),
        Arc::new(TimestampNanosecondArray::from(vec![1]).with_timezone("UTC")),
        Arc::new(BinaryArray::from(vec![b"a\xff\"".as_slice()])),
    ];
    let names = ["j", "t", "ts", "b"];
    let fs = arrays
        .iter()
        .zip(names)
        .map(|(a, n)| Arc::new(Field::new(n, a.data_type().clone(), true)))
        .collect::<Vec<_>>();
    let array = StructArray::new(fs.into(), arrays, None);
    let children = vec![
        NamedRenderField {
            name: "j".into(),
            field: field(N::Json, P::JsonText, true),
        },
        NamedRenderField {
            name: "t".into(),
            field: field(
                N::Time {
                    unit: U::Microsecond,
                },
                P::ScalarText,
                true,
            ),
        },
        NamedRenderField {
            name: "ts".into(),
            field: field(
                N::Timestamp {
                    unit: U::Nanosecond,
                    timezone: Some("UTC".into()),
                },
                P::TimestampContainerText,
                true,
            ),
        },
        NamedRenderField {
            name: "b".into(),
            field: field(N::Binary, P::ScalarText, true),
        },
    ];
    let expected = "{\"j\":'{\"a\":[1,{\"b\":null}]}',\"t\":\"-00:00:00.000001\",\"ts\":\"1970-01-01 00:00:00.000000001 UTC\",\"b\":\"a�\\\"\"}";
    assert_eq!(
        encode(
            encoder(
                input(vec![Arc::new(array)]),
                vec![field(N::Struct(children), P::MysqlContainer, true)]
            ),
            1 << 20
        )
        .0,
        row(&cell(expected.as_bytes()))
    );
}
#[test]
fn map_insertion_order_and_empty_list_null() {
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["z", "a"]));
    let vals: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("keys", DataType::Utf8, false)),
            Arc::new(Field::new("values", DataType::Int32, true)),
        ]
        .into(),
        vec![keys, vals],
        None,
    );
    let entry = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let map = MapArray::new(
        entry,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2])),
        entries,
        None,
        false,
    );
    let f = field(
        N::Map {
            key: Box::new(field(N::String, P::ScalarText, false)),
            value: Box::new(field(N::SignedInteger(32), P::ScalarText, true)),
        },
        P::MysqlContainer,
        true,
    );
    assert_eq!(
        encode(encoder(input(vec![Arc::new(map)]), vec![f]), 65536).0,
        [row(&cell(br#"{"z":1,"a":null}"#)), row(&cell(b"{}"))].concat()
    );
}
#[test]
fn repeated_occurrences_and_dictionary() {
    let d = DictionaryArray::<Int32Type>::from_iter(vec![Some("alpha"), None, Some("alpha")]);
    let batch = input(vec![Arc::new(d)]);
    let f = field(N::String, P::ScalarText, true);
    let cols = vec![
        RenderColumn {
            source_ordinal: 0,
            source_slot: None,
            name: "second".into(),
            field: f.clone(),
        },
        RenderColumn {
            source_ordinal: 0,
            source_slot: None,
            name: "first".into(),
            field: f,
        },
    ];
    let schema = Arc::new(
        ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
            cols,
            1,
        )
        .unwrap(),
    );
    let e = ArrowMysqlTextEncoder::try_new(schema, batch).unwrap();
    let expected = [
        row(&[cell(b"alpha"), cell(b"alpha")].concat()),
        row(&[0xfb, 0xfb]),
        row(&[cell(b"alpha"), cell(b"alpha")].concat()),
    ]
    .concat();
    assert_eq!(encode(e, 65536).0, expected);
}
#[test]
fn opaque_closed_and_binary_named_like_aggregate() {
    let batch = input(vec![Arc::new(BinaryArray::from(vec![b"x".as_slice()]))]);
    let f = field(N::Binary, P::ScalarText, true);
    let schema = Arc::new(
        ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
            vec![RenderColumn {
                source_ordinal: 0,
                source_slot: None,
                name: "bitmap_union".into(),
                field: f,
            }],
            1,
        )
        .unwrap(),
    );
    assert_eq!(
        encode(
            ArrowMysqlTextEncoder::try_new(schema, batch.clone()).unwrap(),
            65536
        )
        .0,
        row(&cell(b"x"))
    );
    assert_eq!(
        encode(
            encoder(
                batch,
                vec![field(
                    N::Opaque(OpaqueRenderType::Bitmap),
                    P::OpaqueNull,
                    true
                )]
            ),
            65536
        )
        .0,
        row(&[0xfb])
    );
}
#[test]
fn unsupported_and_cancel() {
    let mut e = encoder(
        input(vec![Arc::new(Time64MicrosecondArray::from(vec![-1]))]),
        vec![field(
            N::Time {
                unit: U::Microsecond,
            },
            P::ScalarText,
            true,
        )],
    );
    assert_eq!(e.step(&mut [0; 100]).unwrap_err().kind, E::InvalidValue);
    for json in [
        "",
        "[1,]",
        "{\"a\":}",
        "{\"a\":1,}",
        "01",
        "1e",
        "\"a\n\"",
        "null null",
    ] {
        let mut e = encoder(
            input(vec![Arc::new(StringArray::from(vec![json]))]),
            vec![field(N::Json, P::JsonText, true)],
        );
        let cause = e.step(&mut [0; 100]).unwrap_err();
        assert_eq!(cause.kind, E::InvalidJson, "{json}");
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(cause.failure_fact().class(), FailureClass::Data);
        assert_eq!(cause.failure_fact().reason().code(), 110);
    }
    let value = "x".repeat(1000000);
    let mut e = string_encoder(vec![&value]);
    let t = e.step(&mut [0; 100]).unwrap();
    assert_eq!(t.emitted_bytes, 0);
    assert!(t.examined_bytes <= 65536);
    e.cancel();
    assert_eq!(e.step(&mut [0; 100]).unwrap_err().kind, E::Cancelled);
}
fn variant(data: Vec<u8>, offset: i32) -> ArrowMysqlTextEncoder {
    let a = LargeBinaryArray::from(vec![data.as_slice()]);
    encoder(
        input(vec![Arc::new(a)]),
        vec![field(
            N::Variant,
            P::VariantJson {
                timezone_offset_seconds: offset,
            },
            true,
        )],
    )
}
fn serialized(meta: &[u8], value: &[u8]) -> Vec<u8> {
    let mut b = ((meta.len() + value.len()) as u32).to_le_bytes().to_vec();
    b.extend_from_slice(meta);
    b.extend_from_slice(value);
    b
}
#[test]
fn variant_ascii_unicode_key_value_frozen_offset() {
    let data = serialized(
        &[1, 0, 0],
        &[1 | (6 << 2), b'h', b'e', b'l', b'l', b'o', b'!'],
    );
    assert_eq!(
        encode(variant(data, 3600), 65536).0,
        row(&cell(b"\"hello!\""))
    );
    let mut v = vec![1 | (6 << 2)];
    v.extend("中文".as_bytes());
    let expected = "\"ä¸\u{ad}æ\u{96}\u{87}\"";
    assert_eq!(
        encode(variant(serialized(&[1, 0, 0], &v), 0), 65536).0,
        row(&cell(expected.as_bytes()))
    );
    let mut tag12 = vec![12 << 2];
    tag12.extend_from_slice(&1i64.to_le_bytes());
    assert_eq!(
        encode(variant(serialized(&[1, 0, 0], &tag12), 19800), 65536).0,
        row(&cell(b"\"1970-01-01 05:30:00.000001+05:30\""))
    );
    let meta = [1, 1, 0, 6, 0xe4, 0xb8, 0xad, 0xe6, 0x96, 0x87];
    let object = [2, 1, 0, 0, 1, 0];
    let expected = "{\"ä¸\u{ad}æ\u{96}\u{87}\":null}";
    assert_eq!(
        encode(variant(serialized(&meta, &object), 0), 65536).0,
        row(&cell(expected.as_bytes()))
    );
}
#[test]
fn variant_malformed_and_old_unsupported() {
    for value in [
        vec![18 << 2],
        vec![19 << 2],
        vec![7 << 2, 0],
        vec![2, 1, 0, 2, 1, 0],
    ] {
        let mut e = variant(serialized(&[1, 0, 0], &value), 0);
        let cause = e.step(&mut [0; 1000]).unwrap_err();
        assert_eq!(cause.kind, E::InvalidVariant);
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(cause.failure_fact().class(), FailureClass::Data);
        assert_eq!(cause.failure_fact().reason().code(), 111);
    }
    let data = serialized(&[1, 0, 0], &[0]);
    assert_eq!(
        encode(
            encoder(
                input(vec![Arc::new(LargeBinaryArray::from(vec![
                    data.as_slice()
                ]))]),
                vec![field(N::Variant, P::VariantSerializedBytes, true)]
            ),
            65536
        )
        .0,
        row(&cell(&data))
    );
}

#[test]
fn variant_date_does_not_use_date32_zero_sentinel() {
    let mut v = vec![11 << 2];
    v.extend_from_slice(&(-719223i32).to_le_bytes());
    assert_eq!(
        encode(variant(serialized(&[1, 0, 0], &v), 0), 65536).0,
        row(&cell(b"\"0000-11-01\""))
    );
    let mut v = vec![11 << 2];
    v.extend_from_slice(&(-719560i32).to_le_bytes());
    assert_eq!(
        encode(variant(serialized(&[1, 0, 0], &v), 0), 65536).0,
        row(&cell(b"\"-0001-11-30\""))
    );
}
#[test]
fn json_more_than_turn_cell_limit_keeps_exact_state() {
    let value = format!(
        "{{\"a\":[{}],\"b\":{{\"c\":true}}}}",
        vec!["0"; 4096].join(",")
    );
    assert_eq!(
        encode(
            encoder(
                input(vec![Arc::new(StringArray::from(vec![value.as_str()]))]),
                vec![field(N::Json, P::JsonText, true)]
            ),
            65536
        )
        .0,
        row(&cell(value.as_bytes()))
    );
}
#[test]
fn float_display_extreme_literal_bytes() {
    let values = [
        f64::MAX,
        f64::from_bits(1),
        -0.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    let expected = [
        format!("17976931348623157{}", "0".repeat(292)),
        format!("0.{}5", "0".repeat(323)),
        "-0".into(),
        "NaN".into(),
        "inf".into(),
        "-inf".into(),
    ];
    let bytes = expected
        .iter()
        .flat_map(|s| row(&cell(s.as_bytes())))
        .collect::<Vec<_>>();
    assert_eq!(
        encode(
            encoder(
                input(vec![Arc::new(Float64Array::from(values.to_vec()))]),
                vec![field(N::Float64, P::ScalarText, true)]
            ),
            65536
        )
        .0,
        bytes
    );
}
fn wrap_list(array: ArrayRef, render: RenderField) -> (ArrayRef, RenderField) {
    let child = Arc::new(Field::new(
        "item",
        array.data_type().clone(),
        render.nullable,
    ));
    let out = ListArray::new(
        child,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, array.len() as i32])),
        array,
        None,
    );
    (
        Arc::new(out),
        field(N::List(Box::new(render)), P::MysqlContainer, true),
    )
}
fn variant_arrays(mut inner: Vec<u8>, n: usize) -> Vec<u8> {
    for _ in 0..n {
        let w = if inner.len() < 256 { 1 } else { 2 };
        let mut out = vec![3 | (((w - 1) as u8) << 2), 1];
        out.extend(std::iter::repeat_n(0, w));
        out.extend_from_slice(&(inner.len() as u32).to_le_bytes()[..w]);
        out.extend(inner);
        inner = out;
    }
    inner
}
fn run_error(e: &mut ArrowMysqlTextEncoder) -> E {
    let mut output = vec![0; 65536];
    for _ in 0..200000 {
        match e.step(&mut output) {
            Err(e) => return e.kind,
            Ok(t) => {
                assert!(t.emitted_bytes + t.examined_bytes <= 65536);
                assert!(t.visited_cells <= 1024);
                assert_ne!(t.status, RenderTurnStatus::InputComplete, "expected error");
            }
        }
    }
    panic!("expected finite rejection")
}
#[test]
fn native_and_dynamic_depth_boundaries() {
    let base = Arc::new(Int32Array::from(vec![0])) as ArrayRef;
    let mut pair = (base, field(N::SignedInteger(32), P::ScalarText, true));
    for _ in 0..63 {
        pair = wrap_list(pair.0, pair.1);
    }
    let expected = format!("{}0{}", "[".repeat(63), "]".repeat(63));
    assert_eq!(
        encode(
            encoder(input(vec![pair.0.clone()]), vec![pair.1.clone()]),
            65536
        )
        .0,
        row(&cell(expected.as_bytes()))
    );
    let pair = wrap_list(pair.0, pair.1);
    assert!(
        ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
            vec![RenderColumn {
                source_ordinal: 0,
                source_slot: None,
                name: "x".into(),
                field: pair.1
            }],
            1
        )
        .is_err()
    );
    for (n, ok) in [(63, true), (64, false)] {
        let value = variant_arrays(vec![3, 0, 0], n);
        let mut e = variant(serialized(&[1, 0, 0], &value), 0);
        if ok {
            encode(e, 65536);
        } else {
            assert_eq!(run_error(&mut e), E::DepthLimit);
        }
    }
    for (n, ok) in [(62, true), (63, false)] {
        let value = serialized(&[1, 0, 0], &variant_arrays(vec![3, 0, 0], n));
        let pair = wrap_list(
            Arc::new(LargeBinaryArray::from(vec![value.as_slice()])),
            field(
                N::Variant,
                P::VariantJson {
                    timezone_offset_seconds: 0,
                },
                true,
            ),
        );
        let mut e = encoder(input(vec![pair.0]), vec![pair.1]);
        if ok {
            encode(e, 65536);
        } else {
            assert_eq!(run_error(&mut e), E::DepthLimit);
        }
    }
    for (n, ok) in [(64, true), (65, false)] {
        let text = format!("{}{}", "[".repeat(n), "]".repeat(n));
        let mut e = encoder(
            input(vec![Arc::new(StringArray::from(vec![text.as_str()]))]),
            vec![field(N::Json, P::JsonText, true)],
        );
        if ok {
            encode(e, 65536);
        } else {
            assert_eq!(run_error(&mut e), E::DepthLimit);
        }
    }
}
#[test]
fn nested_decimal256_and_largeint_extremes() {
    let values = Arc::new(
        Decimal256Array::from_iter_values(
            std::iter::once(
                "9999999999999999999999999999999999999999999999999999999999999999999999999999",
            )
            .map(|s| s.parse().unwrap()),
        )
        .with_precision_and_scale(76, 2)
        .unwrap(),
    ) as ArrayRef;
    let pair = wrap_list(
        values,
        field(
            N::Decimal {
                bits: 256,
                precision: 76,
                scale: 2,
            },
            P::ScalarText,
            true,
        ),
    );
    let expected = format!("[{}.99]", "9".repeat(74));
    assert_eq!(
        encode(encoder(input(vec![pair.0]), vec![pair.1]), 65536).0,
        row(&cell(expected.as_bytes()))
    );
    let large = FixedSizeBinaryArray::try_from_iter(
        [i128::MIN.to_be_bytes(), i128::MAX.to_be_bytes()].into_iter(),
    )
    .unwrap();
    let expected = [
        "-170141183460469231731687303715884105728",
        "170141183460469231731687303715884105727",
    ]
    .iter()
    .flat_map(|s| row(&cell(s.as_bytes())))
    .collect::<Vec<_>>();
    assert_eq!(
        encode(
            encoder(
                input(vec![Arc::new(large)]),
                vec![field(N::LargeInt, P::ScalarText, true)]
            ),
            65536
        )
        .0,
        expected
    );
}
#[test]
fn row_limit_exact_and_plus_one() {
    for (extra, ok) in [(0, true), (1, false)] {
        let text = "x".repeat(V::ROW_PAYLOAD_BYTES as usize - 9 + extra);
        let mut e = string_encoder(vec![&text]);
        if !ok {
            assert_eq!(run_error(&mut e), E::RowTooLarge);
            continue;
        }
        let mut output = vec![0; 65536];
        let mut frontier = ClientRowStreamCursor::new();
        let mut total = 0;
        loop {
            let t = e.step(&mut output).unwrap();
            assert!(t.examined_bytes + t.emitted_bytes <= 65536);
            if t.emitted_bytes != 0 {
                frontier = frontier
                    .validate_body(
                        ClientRowProfile::try_new(1 << 20, V::ROW_PAYLOAD_BYTES).unwrap(),
                        &output[..t.emitted_bytes],
                    )
                    .unwrap()
                    .after();
                total += t.emitted_bytes;
            }
            if t.status == RenderTurnStatus::InputComplete {
                break;
            }
        }
        assert_eq!(total, 4 + V::ROW_PAYLOAD_BYTES as usize);
        assert_eq!(frontier.completed_rows(), 1);
        frontier.validate_end().unwrap();
    }
}
#[test]
fn elements_reset_between_passes_and_sum_occurrences() {
    for (n, ok) in [
        (V::MAX_ELEMENTS_PER_ROW - 1, true),
        (V::MAX_ELEMENTS_PER_ROW, false),
    ] {
        let array = Arc::new(Int8Array::from(vec![0; n])) as ArrayRef;
        let pair = wrap_list(array, field(N::SignedInteger(8), P::ScalarText, true));
        let mut e = encoder(input(vec![pair.0]), vec![pair.1]);
        if ok {
            let mut out = vec![0; 65536];
            let mut rows = 0;
            loop {
                let t = e.step(&mut out).unwrap();
                assert!(t.examined_bytes + t.emitted_bytes <= 65536);
                assert!(t.visited_cells <= 1024);
                rows += t.completed_rows;
                if t.status == RenderTurnStatus::InputComplete {
                    break;
                }
            }
            assert_eq!(rows, 1);
        } else {
            assert_eq!(run_error(&mut e), E::ElementLimit);
        }
    }
    let array = Arc::new(Int8Array::from(vec![0; V::MAX_ELEMENTS_PER_ROW / 2])) as ArrayRef;
    let pair = wrap_list(array, field(N::SignedInteger(8), P::ScalarText, true));
    let batch = input(vec![pair.0]);
    let cols = (0..2)
        .map(|i| RenderColumn {
            source_ordinal: 0,
            source_slot: None,
            name: i.to_string(),
            field: pair.1.clone(),
        })
        .collect();
    let mut e = ArrowMysqlTextEncoder::try_new(
        Arc::new(
            ClientRenderSchema::try_new(
                novarocks_result_contract::ClientPresentationFamily::MysqlText,
                cols,
                1,
            )
            .unwrap(),
        ),
        batch,
    )
    .unwrap();
    assert_eq!(run_error(&mut e), E::ElementLimit);
}
#[test]
fn wide_columns_and_many_small_rows() {
    let count = 4096;
    let batch = input(vec![Arc::new(Int8Array::from(vec![7]))]);
    let cols = (0..count)
        .map(|i| RenderColumn {
            source_ordinal: 0,
            source_slot: None,
            name: i.to_string(),
            field: field(N::SignedInteger(8), P::ScalarText, true),
        })
        .collect();
    let e = ArrowMysqlTextEncoder::try_new(
        Arc::new(
            ClientRenderSchema::try_new(
                novarocks_result_contract::ClientPresentationFamily::MysqlText,
                cols,
                1,
            )
            .unwrap(),
        ),
        batch,
    )
    .unwrap();
    assert_eq!(encode(e, 65536).0, row(&[1, b'7'].repeat(count)));
    let values = vec!["x"; 1000];
    let (actual, segments) = encode(string_encoder(values), 1 << 20);
    assert_eq!(actual, row(&cell(b"x")).repeat(1000));
    assert!(segments.len() < 10);
}
#[test]
fn timestamp_signed_units_and_gregorian_years() {
    let tests: [(ArrayRef, RenderField, &str); 4] = [
        (
            Arc::new(TimestampMillisecondArray::from(vec![-1])),
            field(
                N::Timestamp {
                    unit: U::Millisecond,
                    timezone: None,
                },
                P::TimestampContainerText,
                true,
            ),
            "[\"1969-12-31 23:59:59.999\"]",
        ),
        (
            Arc::new(TimestampNanosecondArray::from(vec![-1])),
            field(
                N::Timestamp {
                    unit: U::Nanosecond,
                    timezone: None,
                },
                P::TimestampContainerText,
                true,
            ),
            "[\"1969-12-31 23:59:59.999999999\"]",
        ),
        (
            Arc::new(Date32Array::from(vec![-719561])),
            field(N::Date, P::ScalarText, true),
            "[\"-0001-11-29\"]",
        ),
        (
            Arc::new(TimestampSecondArray::from(vec![253402300800])),
            field(
                N::Timestamp {
                    unit: U::Second,
                    timezone: None,
                },
                P::TimestampContainerText,
                true,
            ),
            "[\"+10000-01-01 00:00:00\"]",
        ),
    ];
    for (a, f, expected) in tests {
        let pair = wrap_list(a, f);
        assert_eq!(
            encode(encoder(input(vec![pair.0]), vec![pair.1]), 65536).0,
            row(&cell(expected.as_bytes()))
        );
    }
    assert_eq!(
        encode(
            encoder(
                input(vec![Arc::new(Date32Array::from(vec![-719223]))]),
                vec![field(N::Date, P::ScalarText, true)]
            ),
            65536
        )
        .0,
        row(&cell(b"0000-11-01"))
    );
}
#[test]
fn frozen_offset_second_rounding_matches_legacy() {
    for (offset, expected) in [
        (30, "\"1970-01-01 00:00:30+00:01\""),
        (59, "\"1970-01-01 00:00:59+00:01\""),
        (-30, "\"1969-12-31 23:59:30-00:01\""),
        (-59, "\"1969-12-31 23:59:01-00:01\""),
    ] {
        let mut v = vec![12 << 2];
        v.extend_from_slice(&0i64.to_le_bytes());
        let actual = encode(variant(serialized(&[1, 0, 0], &v), offset), 65536).0;
        assert_eq!(actual, row(&cell(expected.as_bytes())));
        let dt = chrono::DateTime::from_timestamp(0, 0)
            .unwrap()
            .with_timezone(&chrono::FixedOffset::east_opt(offset).unwrap());
        assert!(expected.ends_with(&format!("{}\"", dt.format("%:z"))));
    }
}

// These byte oracles previously exercised the uncalled Arrow-field renderer.
// They now use the installed typed client renderer without source annotations.
#[test]
fn format_timestamp_microsecond_omits_zero_fraction() {
    let batch = input(vec![Arc::new(TimestampMicrosecondArray::from(vec![0]))]);
    let fields = vec![field(
        N::Timestamp {
            unit: U::Microsecond,
            timezone: None,
        },
        P::TimestampContainerText,
        true,
    )];
    assert_eq!(
        encode(encoder(batch, fields), 1024).0,
        row(&cell(b"1970-01-01 00:00:00"))
    );
}

#[test]
fn format_timestamp_microsecond_keeps_non_zero_fraction() {
    let batch = input(vec![Arc::new(TimestampMicrosecondArray::from(vec![1]))]);
    let fields = vec![field(
        N::Timestamp {
            unit: U::Microsecond,
            timezone: None,
        },
        P::TimestampContainerText,
        true,
    )];
    assert_eq!(
        encode(encoder(batch, fields), 1024).0,
        row(&cell(b"1970-01-01 00:00:00.000001"))
    );
}

#[test]
fn mysql_text_row_formats_time64_microsecond() {
    let batch = input(vec![Arc::new(Time64MicrosecondArray::from(vec![
        12_345_678,
    ]))]);
    let fields = vec![field(
        N::Time {
            unit: U::Microsecond,
        },
        P::ScalarText,
        true,
    )];
    assert_eq!(
        encode(encoder(batch, fields), 1024).0,
        row(&cell(b"00:00:12.345678"))
    );
}

#[test]
fn mysql_text_row_uses_declared_struct_json_children() {
    let array = StructArray::new(
        vec![Arc::new(Field::new("payload", DataType::Utf8, true))].into(),
        vec![Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#)])) as ArrayRef],
        None,
    );
    let declared = field(
        N::Struct(vec![NamedRenderField {
            name: "payload".into(),
            field: field(N::Json, P::JsonText, true),
        }]),
        P::MysqlContainer,
        true,
    );
    assert_eq!(
        encode(encoder(input(vec![Arc::new(array)]), vec![declared]), 1024).0,
        row(&cell(br#"{"payload":'{"a":1}'}"#)),
    );
}
