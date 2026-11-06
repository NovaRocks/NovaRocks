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
    NativeRenderType as N, RenderColumn, RenderField, RenderPresentation as P, RenderTimeUnit as U,
    RootProfileV1 as V,
};
use novarocks_result_render::{
    ArrowTrinoJsonEncoder, BoundedClientRowEncoder, RenderErrorKind as E, RenderTurnStatus,
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
fn encoder(batch: RecordBatch, fields: Vec<RenderField>) -> ArrowTrinoJsonEncoder {
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
    ArrowTrinoJsonEncoder::try_new(
        Arc::new(
            ClientRenderSchema::try_new(
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                    novarocks_result_contract::TrinoJsonFacts {
                        parametric_datetime: true,
                        variant: true,
                        variant_binary: false,
                    },
                ),
                cols,
                batch.num_columns(),
            )
            .unwrap(),
        ),
        batch,
    )
    .unwrap()
}
fn string_encoder(values: Vec<&str>) -> ArrowTrinoJsonEncoder {
    encoder(
        input(vec![Arc::new(StringArray::from(values))]),
        vec![field(N::String, P::TrinoValue, true)],
    )
}
fn encode(mut e: ArrowTrinoJsonEncoder, size: usize) -> (Vec<u8>, Vec<usize>) {
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
#[test]
fn small_threshold_keeps_exact_json_payload_boundaries() {
    for total in [
        V::SMALL_ROW_BYTES - 1,
        V::SMALL_ROW_BYTES,
        V::SMALL_ROW_BYTES + 1,
    ] {
        let value = "x".repeat(total - 4);
        let expected = row(format!("[\"{value}\"]").as_bytes());
        let (actual, lengths) = encode(string_encoder(vec![&value]), 1 << 20);
        assert_eq!(actual, expected);
        assert!(lengths.iter().all(|n| *n > 4));
    }
}
#[test]
fn prefix_quantum_and_short_continuation() {
    let value = "x".repeat(65534);
    for size in [5, 6, 7, 255, 256, 257, 65535, 65536, 65537, 1 << 20] {
        assert_eq!(
            encode(string_encoder(vec![&value, "", &value]), size).0,
            [
                row(format!("[\"{value}\"]").as_bytes()),
                row(b"[\"\"]"),
                row(format!("[\"{value}\"]").as_bytes())
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
            assert_eq!(
                &out,
                &[((value.len() + 4) as u32).to_le_bytes().as_slice(), b"["].concat()
            );
            break;
        }
    }
    let t = e.step(&mut [0]).unwrap();
    assert_eq!(t.emitted_bytes, 1);
}
#[test]
fn map_insertion_order_empty_map_and_null_value() {
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
            key: Box::new(field(N::String, P::TrinoValue, false)),
            value: Box::new(field(N::SignedInteger(32), P::TrinoValue, true)),
        },
        P::TrinoValue,
        true,
    );
    assert_eq!(
        encode(encoder(input(vec![Arc::new(map)]), vec![f]), 65536).0,
        [row(br#"[{"z":1,"a":null}]"#), row(b"[{}]")].concat()
    );
}
#[test]
fn repeated_occurrences_and_dictionary() {
    let d = DictionaryArray::<Int32Type>::from_iter(vec![Some("alpha"), None, Some("alpha")]);
    let batch = input(vec![Arc::new(d)]);
    let f = field(N::String, P::TrinoValue, true);
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
            novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    variant: true,
                    variant_binary: false,
                },
            ),
            cols,
            1,
        )
        .unwrap(),
    );
    let e = ArrowTrinoJsonEncoder::try_new(schema, batch).unwrap();
    let expected = [
        row(br#"["alpha","alpha"]"#),
        row(b"[null,null]"),
        row(br#"["alpha","alpha"]"#),
    ]
    .concat();
    assert_eq!(encode(e, 65536).0, expected);
}
#[test]
fn unsupported_and_cancel() {
    let mut e = encoder(
        input(vec![Arc::new(Time64MicrosecondArray::from(vec![-1]))]),
        vec![field(
            N::Time {
                unit: U::Microsecond,
            },
            P::TrinoValue,
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
            vec![field(N::Json, P::TrinoValue, true)],
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
fn variant(data: Vec<u8>) -> ArrowTrinoJsonEncoder {
    let a = LargeBinaryArray::from(vec![data.as_slice()]);
    encoder(
        input(vec![Arc::new(a)]),
        vec![field(N::Variant, P::TrinoValue, true)],
    )
}
fn serialized(meta: &[u8], value: &[u8]) -> Vec<u8> {
    let mut b = ((meta.len() + value.len()) as u32).to_le_bytes().to_vec();
    b.extend_from_slice(meta);
    b.extend_from_slice(value);
    b
}
#[test]
fn malformed_variant_payloads_keep_the_original_typed_data_cause() {
    for value in [
        vec![18 << 2],
        vec![19 << 2],
        vec![7 << 2, 0],
        vec![2, 1, 0, 2, 1, 0],
    ] {
        let mut e = variant(serialized(&[1, 0, 0], &value));
        let cause = e.step(&mut [0; 1000]).unwrap_err();
        assert_eq!(cause.kind, E::InvalidVariant);
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(cause.failure_fact().class(), FailureClass::Data);
        assert_eq!(cause.failure_fact().reason().code(), 111);
    }
}

#[test]
fn variant_dates_keep_proleptic_calendar_values() {
    let mut v = vec![11 << 2];
    v.extend_from_slice(&(-719223i32).to_le_bytes());
    assert_eq!(
        encode(variant(serialized(&[1, 0, 0], &v)), 65536).0,
        row(br#"["0000-11-01"]"#)
    );
    let mut v = vec![11 << 2];
    v.extend_from_slice(&(-719560i32).to_le_bytes());
    assert_eq!(
        encode(variant(serialized(&[1, 0, 0], &v)), 65536).0,
        row(br#"["-0001-11-30"]"#)
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
                vec![field(N::Json, P::TrinoValue, true)]
            ),
            65536
        )
        .0,
        row(format!("[\"{}\"]", value.replace('\\', "\\\\").replace('"', "\\\"")).as_bytes())
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
        field(N::List(Box::new(render)), P::TrinoValue, true),
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
fn run_error(e: &mut ArrowTrinoJsonEncoder) -> E {
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
    let mut pair = (base, field(N::SignedInteger(32), P::TrinoValue, true));
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
        row(format!("[{expected}]").as_bytes())
    );
    let pair = wrap_list(pair.0, pair.1);
    assert!(
        ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    variant: true,
                    variant_binary: true
                }
            ),
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
        let mut e = variant(serialized(&[1, 0, 0], &value));
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
            field(N::Variant, P::TrinoValue, true),
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
            vec![field(N::Json, P::TrinoValue, true)],
        );
        if ok {
            encode(e, 65536);
        } else {
            assert_eq!(run_error(&mut e), E::DepthLimit);
        }
    }
}
#[test]
fn row_limit_exact_and_plus_one() {
    for (extra, ok) in [(0, true), (1, false)] {
        let text = "x".repeat(V::ROW_PAYLOAD_BYTES as usize - 4 + extra);
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
        let pair = wrap_list(array, field(N::SignedInteger(8), P::TrinoValue, true));
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
    let pair = wrap_list(array, field(N::SignedInteger(8), P::TrinoValue, true));
    let batch = input(vec![pair.0]);
    let cols = (0..2)
        .map(|i| RenderColumn {
            source_ordinal: 0,
            source_slot: None,
            name: i.to_string(),
            field: pair.1.clone(),
        })
        .collect();
    let mut e = ArrowTrinoJsonEncoder::try_new(
        Arc::new(
            ClientRenderSchema::try_new(
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                    novarocks_result_contract::TrinoJsonFacts {
                        parametric_datetime: true,
                        variant: true,
                        variant_binary: false,
                    },
                ),
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
            field: field(N::SignedInteger(8), P::TrinoValue, true),
        })
        .collect();
    let e = ArrowTrinoJsonEncoder::try_new(
        Arc::new(
            ClientRenderSchema::try_new(
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                    novarocks_result_contract::TrinoJsonFacts {
                        parametric_datetime: true,
                        variant: true,
                        variant_binary: false,
                    },
                ),
                cols,
                1,
            )
            .unwrap(),
        ),
        batch,
    )
    .unwrap();
    assert_eq!(
        encode(e, 65536).0,
        row(format!("[{}]", vec!["7"; count].join(",")).as_bytes())
    );
    let values = vec!["x"; 1000];
    let (actual, segments) = encode(string_encoder(values), 1 << 20);
    assert_eq!(actual, row(br#"["x"]"#).repeat(1000));
    // The sole JSON protocol has its own quoting/traversal work. Preserve
    // batching under the fixed per-turn quantum rather than the retired
    // format's incidental number of segments.
    assert!(
        segments.len() < 1000,
        "small rows must make batched progress"
    );
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
                P::TrinoValue,
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
                P::TrinoValue,
                true,
            ),
            "[\"1969-12-31 23:59:59.999999999\"]",
        ),
        (
            Arc::new(Date32Array::from(vec![-719561])),
            field(N::Date, P::TrinoValue, true),
            "[\"-0001-11-29\"]",
        ),
        (
            Arc::new(TimestampSecondArray::from(vec![253402300800])),
            field(
                N::Timestamp {
                    unit: U::Second,
                    timezone: None,
                },
                P::TrinoValue,
                true,
            ),
            "[\"+10000-01-01 00:00:00\"]",
        ),
    ];
    for (a, f, expected) in tests {
        let pair = wrap_list(a, f);
        assert_eq!(
            encode(encoder(input(vec![pair.0]), vec![pair.1]), 65536).0,
            row(format!("[{expected}]").as_bytes())
        );
    }
    assert_eq!(
        encode(
            encoder(
                input(vec![Arc::new(Date32Array::from(vec![-719223]))]),
                vec![field(N::Date, P::TrinoValue, true)]
            ),
            65536
        )
        .0,
        row(br#"["0000-11-01"]"#)
    );
}
#[test]
fn trino_row_uses_declared_struct_json_children() {
    let array = StructArray::new(
        vec![Arc::new(Field::new("payload", DataType::Utf8, true))].into(),
        vec![Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#)])) as ArrayRef],
        None,
    );
    let declared = field(
        N::Struct(vec![NamedRenderField {
            name: "payload".into(),
            field: field(N::Json, P::TrinoValue, true),
        }]),
        P::TrinoValue,
        true,
    );
    assert_eq!(
        encode(encoder(input(vec![Arc::new(array)]), vec![declared]), 1024).0,
        row(br#"[["{\"a\":1}"]]"#),
    );
}
