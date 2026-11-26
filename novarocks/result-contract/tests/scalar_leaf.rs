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

//! Exact leaf bytes only. Fixture Vec allocations are not producer funding;
//! these tests do not install a source bridge, Host, collector or session.

use novarocks_result_contract::{
    BorrowedScalarLeaf as V, RootProfileV1, SCALAR_LEAF_HEADER_BYTES, ScalarField,
    ScalarLeafCursor, ScalarLeafError as E, ScalarLeafHeader, ScalarOpaqueType as O,
    ScalarProfileV1, ScalarSchema, ScalarTimestampUnit as U, ScalarValueType as T,
};

fn make_schema(value_type: T, nullable: bool) -> ScalarSchema {
    ScalarSchema::try_new(ScalarField {
        nullable,
        value_type,
    })
    .unwrap()
}

fn rejects(schema: &ScalarSchema, value: V<'_>, expected: E) {
    match ScalarLeafCursor::try_new(schema, value) {
        Ok(_) => panic!("invalid scalar leaf was accepted; expected {expected:?}"),
        Err(actual) => assert_eq!(actual, expected),
    }
}

fn variable_value<'a>(ty: &T, text: &'a str) -> V<'a> {
    match ty {
        T::String => V::String(text),
        T::Binary => V::Binary(text.as_bytes()),
        T::Variant => V::Variant(text.as_bytes()),
        T::Opaque(kind) => V::Opaque {
            kind: *kind,
            bytes: text.as_bytes(),
        },
        _ => unreachable!(),
    }
}

// Independent SCV1 declaration oracle: total/payload u32 LE, then semantic
// kind, flags, width u16 LE, precision, scale, temporal unit and opaque kind.
// The final four declaration bytes are reserved and must remain zero.
fn golden(declaration: [u8; 8], payload: &[u8]) -> Vec<u8> {
    let mut bytes = b"SCV1".to_vec();
    bytes.extend_from_slice(&(24u32 + payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&declaration);
    bytes.extend_from_slice(&[0, 0, 0, 0]);
    bytes.extend_from_slice(payload);
    bytes
}

fn emit(mut cursor: ScalarLeafCursor<'_>, chunk: usize) -> Vec<u8> {
    assert!(chunk > 0);
    let expected_len = cursor.encoded_len();
    let mut output = Vec::with_capacity(expected_len);
    let mut scratch = vec![0xa5; chunk];
    for _ in 0..=expected_len {
        let turn = cursor.step(&mut scratch);
        assert!(turn.emitted_bytes > 0 || turn.complete);
        assert!(turn.emitted_bytes <= chunk);
        assert!(turn.emitted_bytes <= RootProfileV1::EMIT_BYTES_PER_TURN);
        output.extend_from_slice(&scratch[..turn.emitted_bytes]);
        if turn.complete {
            assert_eq!(output.len(), expected_len);
            scratch.fill(0x5a);
            let finished = cursor.step(&mut scratch);
            assert!(finished.complete);
            assert_eq!(finished.emitted_bytes, 0);
            assert!(scratch.iter().all(|byte| *byte == 0x5a));
            return output;
        }
    }
    panic!("scalar cursor exceeded its finite output bound");
}

#[test]
fn exact_fixed_leaf_records_use_raw_little_endian_values() {
    let cases: Vec<(T, V<'_>, [u8; 8], Vec<u8>)> = vec![
        (
            T::Boolean,
            V::Boolean(true),
            [1, 0, 8, 0, 0, 0, 0, 0],
            vec![1],
        ),
        (
            T::Boolean,
            V::Boolean(false),
            [1, 0, 8, 0, 0, 0, 0, 0],
            vec![0],
        ),
        (
            T::SignedInteger(8),
            V::SignedInteger {
                bits: 8,
                value: -128,
            },
            [2, 0, 8, 0, 0, 0, 0, 0],
            vec![0x80],
        ),
        (
            T::SignedInteger(16),
            V::SignedInteger {
                bits: 16,
                value: -257,
            },
            [2, 0, 16, 0, 0, 0, 0, 0],
            vec![0xff, 0xfe],
        ),
        (
            T::SignedInteger(32),
            V::SignedInteger {
                bits: 32,
                value: 0x1234_5678,
            },
            [2, 0, 32, 0, 0, 0, 0, 0],
            vec![0x78, 0x56, 0x34, 0x12],
        ),
        (
            T::SignedInteger(64),
            V::SignedInteger {
                bits: 64,
                value: i64::MIN,
            },
            [2, 0, 64, 0, 0, 0, 0, 0],
            vec![0, 0, 0, 0, 0, 0, 0, 0x80],
        ),
        (
            T::LargeInt,
            V::LargeInt(i128::MIN),
            [3, 0, 128, 0, 0, 0, 0, 0],
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x80],
        ),
        (
            T::Date,
            V::Date(-1),
            [9, 0, 32, 0, 0, 0, 0, 0],
            vec![0xff; 4],
        ),
        (
            T::TimeMicros,
            V::TimeMicros(-1),
            [10, 0, 64, 0, 0, 0, 1, 0],
            vec![0xff; 8],
        ),
        (
            T::Timestamp {
                unit: U::Microsecond,
                timezone: Some("UTC".into()),
            },
            V::Timestamp {
                ticks: 1_000_001,
                unit: U::Microsecond,
            },
            [11, 0, 64, 0, 0, 0, 1, 0],
            vec![0x41, 0x42, 0x0f, 0, 0, 0, 0, 0],
        ),
        (
            T::Timestamp {
                unit: U::Nanosecond,
                timezone: None,
            },
            V::Timestamp {
                ticks: 1_000_000_001,
                unit: U::Nanosecond,
            },
            [11, 0, 64, 0, 0, 0, 2, 0],
            vec![1, 0xca, 0x9a, 0x3b, 0, 0, 0, 0],
        ),
    ];
    assert_eq!(SCALAR_LEAF_HEADER_BYTES, 24);
    for (ty, value, declaration, payload) in cases {
        let schema = make_schema(ty, false);
        let cursor = ScalarLeafCursor::try_new(&schema, value).unwrap();
        assert_eq!(cursor.rows(), 1);
        let expected = golden(declaration, &payload);
        assert_eq!(V::decode(&schema, &expected).unwrap(), value);
        assert_eq!(emit(cursor, 3), expected);
    }
}

#[test]
fn float_sign_nan_payload_and_infinity_bits_survive_without_text_conversion() {
    for (bits, expected) in [
        (0x8000_0000, [0, 0, 0, 0x80]),
        (0x7f80_0000, [0, 0, 0x80, 0x7f]),
        (0xff80_0000, [0, 0, 0x80, 0xff]),
        (0x7fc1_2345, [0x45, 0x23, 0xc1, 0x7f]),
        (0xff81_2345, [0x45, 0x23, 0x81, 0xff]),
    ] {
        let schema = make_schema(T::Float32, false);
        assert_eq!(
            emit(
                ScalarLeafCursor::try_new(&schema, V::Float32(bits)).unwrap(),
                1
            ),
            golden([4, 0, 32, 0, 0, 0, 0, 0], &expected)
        );
    }
    for (bits, expected) in [
        (0x8000_0000_0000_0000, [0, 0, 0, 0, 0, 0, 0, 0x80]),
        (0x7ff0_0000_0000_0000, [0, 0, 0, 0, 0, 0, 0xf0, 0x7f]),
        (0xfff0_0000_0000_0000, [0, 0, 0, 0, 0, 0, 0xf0, 0xff]),
        (
            0x7ff8_0123_4567_89ab,
            [0xab, 0x89, 0x67, 0x45, 0x23, 1, 0xf8, 0x7f],
        ),
        (
            0xfff0_0123_4567_89ab,
            [0xab, 0x89, 0x67, 0x45, 0x23, 1, 0xf0, 0xff],
        ),
    ] {
        let schema = make_schema(T::Float64, false);
        assert_eq!(
            emit(
                ScalarLeafCursor::try_new(&schema, V::Float64(bits)).unwrap(),
                5
            ),
            golden([5, 0, 64, 0, 0, 0, 0, 0], &expected)
        );
    }
}

#[test]
fn variable_bytes_preserve_utf8_binary_variant_and_each_opaque_identity() {
    let raw = [0, 0xff, 0x80, b'\'', b'\\'];
    let text = "雪\0'\\";
    let json = b"\x01\x80\x08\x01\x01v\x44\x03\xe9\x9b\xaa";
    for (ty, value, declaration, payload) in [
        (
            T::String,
            V::String(text),
            [7, 0, 0, 0, 0, 0, 0, 0],
            text.as_bytes(),
        ),
        (
            T::Json,
            V::Json(json),
            [18, 0, 0, 0, 0, 0, 0, 0],
            json.as_slice(),
        ),
        (
            T::Binary,
            V::Binary(&raw),
            [8, 0, 0, 0, 0, 0, 0, 0],
            raw.as_slice(),
        ),
        (
            T::Variant,
            V::Variant(&raw),
            [13, 0, 0, 0, 0, 0, 0, 0],
            raw.as_slice(),
        ),
    ] {
        let schema = make_schema(ty, false);
        assert_eq!(
            emit(ScalarLeafCursor::try_new(&schema, value).unwrap(), 1),
            golden(declaration, payload)
        );
    }
    for (kind, tag) in [
        (O::Hll, 1),
        (O::Bitmap, 2),
        (O::Object, 3),
        (O::Percentile, 4),
    ] {
        let schema = make_schema(T::Opaque(kind), false);
        assert_eq!(
            emit(
                ScalarLeafCursor::try_new(&schema, V::Opaque { kind, bytes: &raw }).unwrap(),
                2
            ),
            golden([14, 0, 0, 0, 0, 0, 0, tag], &raw)
        );
    }
}

#[test]
fn zero_output_never_advances_and_tiny_output_splits_every_declaration_boundary() {
    let payload = b"a\0b\xffc";
    let expected = golden([8, 0, 0, 0, 0, 0, 0, 0], payload);
    for size in [1, 2, 3, 7, 23, 24, 25, 64] {
        let schema = make_schema(T::Binary, false);
        let mut cursor = ScalarLeafCursor::try_new(&schema, V::Binary(payload)).unwrap();
        for _ in 0..4 {
            let turn = cursor.step(&mut []);
            assert_eq!(turn.emitted_bytes, 0);
            assert!(!turn.complete);
        }
        let mut first = [0xa5; 1];
        assert_eq!(cursor.step(&mut first).emitted_bytes, 1);
        assert_eq!(first, [b'S']);
        assert!(!cursor.step(&mut []).complete);
        let mut output = vec![first[0]];
        let mut piece = vec![0xa5; size];
        for _ in 0..expected.len() {
            let turn = cursor.step(&mut piece);
            output.extend_from_slice(&piece[..turn.emitted_bytes]);
            if turn.complete {
                break;
            }
        }
        assert_eq!(output, expected);
        let done = cursor.step(&mut []);
        assert!(done.complete);
        assert_eq!(done.emitted_bytes, 0);
    }
}

#[test]
fn exact_64k_record_obeys_turn_quantum_and_never_writes_output_suffix() {
    let payload: Vec<u8> = (0..ScalarProfileV1::RECORD_PAYLOAD_BYTES)
        .map(|i| (i % 251) as u8)
        .collect();
    let schema = make_schema(T::Binary, false);
    let mut cursor = ScalarLeafCursor::try_new(&schema, V::Binary(&payload)).unwrap();
    assert_eq!(cursor.encoded_len(), 65_536);
    let mut output = vec![0xa5; 70_000];
    let first = cursor.step(&mut output);
    assert_eq!(first.emitted_bytes, 65_536);
    assert!(first.complete);
    assert!(
        output[first.emitted_bytes..]
            .iter()
            .all(|byte| *byte == 0xa5)
    );
    let actual = output[..first.emitted_bytes].to_vec();
    output.fill(0x5a);
    let second = cursor.step(&mut output);
    assert_eq!(second.emitted_bytes, 0);
    assert!(second.complete);
    assert!(output.iter().all(|byte| *byte == 0x5a));
    assert_eq!(actual, golden([8, 0, 0, 0, 0, 0, 0, 0], &payload));
}

#[test]
fn every_variable_leaf_limit_is_inclusive_and_empty_value_is_present() {
    let exact = "x".repeat(65_512);
    let oversized = "x".repeat(65_513);
    for ty in [T::String, T::Binary, T::Variant, T::Opaque(O::Hll)] {
        let schema = make_schema(ty.clone(), false);
        let cursor = ScalarLeafCursor::try_new(&schema, variable_value(&ty, &exact)).unwrap();
        assert_eq!(cursor.rows(), 1);
        assert_eq!(cursor.encoded_len(), 65_536);
        rejects(&schema, variable_value(&ty, &oversized), E::ValueLimit);
        let empty = ScalarLeafCursor::try_new(&schema, variable_value(&ty, "")).unwrap();
        assert_eq!(empty.rows(), 1);
        let encoded = emit(empty, 3);
        assert_eq!(encoded.len(), 24);
        assert_eq!(encoded[13], 0, "empty bytes are neither NULL nor absent");
        assert_eq!(&encoded[8..12], &[0, 0, 0, 0]);
    }
}

#[test]
fn native_json_scalar_frame_preserves_its_marker_and_exact_record_limit() {
    let schema = make_schema(T::Json, false);
    let json_string = |len: usize| {
        // The tested lengths use exactly three ULEB128 bytes.
        let bytes = len - 5;
        let mut payload = vec![
            1,
            0x44,
            (bytes as u8 & 0x7f) | 0x80,
            ((bytes >> 7) as u8 & 0x7f) | 0x80,
            (bytes >> 14) as u8,
        ];
        payload.resize(len, b'x');
        payload
    };
    let exact = json_string(65_512);
    let over = json_string(65_513);
    assert_eq!(
        ScalarLeafCursor::try_new(&schema, V::Json(&exact))
            .unwrap()
            .encoded_len(),
        65_536
    );
    rejects(&schema, V::Json(&over), E::ValueLimit);
    rejects(&schema, V::Json(&[]), E::MalformedRecord);
    let json_null = [1, 0x20];
    assert_eq!(
        V::decode(&schema, &golden([18, 0, 0, 0, 0, 0, 0, 0], &json_null)).unwrap(),
        V::Json(&json_null)
    );
    for (tag, bytes) in [
        (12, json_null.as_slice()),
        (18, b"null".as_slice()),
        (18, &[1]),
        (18, &[1, 0x42]),
        (18, &[1, 0x80, 2, 0]),
        (18, &[1, 0x20, 0x20]),
        (18, &[1, 0xff]),
        (18, &[1, 0x44, 0x80, 0]),
    ] {
        assert_eq!(
            V::decode(&schema, &golden([tag, 0, 0, 0, 0, 0, 0, 0], bytes)),
            Err(E::MalformedRecord)
        );
    }
}

#[test]
fn absent_and_null_are_distinct_and_frozen_nullability_is_enforced() {
    for nullable in [false, true] {
        let schema = make_schema(T::SignedInteger(32), nullable);
        let absent = ScalarLeafCursor::try_new(&schema, V::NoRows).unwrap();
        assert_eq!(absent.rows(), 0);
        assert_eq!(emit(absent, 1), golden([2, 3, 32, 0, 0, 0, 0, 0], &[]));
        if nullable {
            let null = ScalarLeafCursor::try_new(&schema, V::Null).unwrap();
            assert_eq!(null.rows(), 1);
            assert_eq!(emit(null, 2), golden([2, 1, 32, 0, 0, 0, 0, 0], &[]));
        } else {
            rejects(&schema, V::Null, E::Nullability);
        }
    }
    let schema = make_schema(T::Null, false);
    let null = ScalarLeafCursor::try_new(&schema, V::Null).unwrap();
    assert_eq!(null.rows(), 1);
    assert_eq!(emit(null, 1), golden([0, 1, 0, 0, 0, 0, 0, 0], &[]));
    let absent = ScalarLeafCursor::try_new(&schema, V::NoRows).unwrap();
    assert_eq!(absent.rows(), 0);
    assert_eq!(emit(absent, 1), golden([0, 3, 0, 0, 0, 0, 0, 0], &[]));
}

#[test]
fn semantic_type_width_precision_unit_and_opaque_mismatches_refuse() {
    for (ty, value) in [
        (T::String, V::Json(&[1, 0x20])),
        (T::Json, V::String("same")),
        (T::Binary, V::Variant(b"same")),
        (T::Variant, V::Binary(b"same")),
        (T::Boolean, V::SignedInteger { bits: 8, value: 1 }),
        (T::SignedInteger(16), V::SignedInteger { bits: 8, value: 1 }),
        (
            T::LargeInt,
            V::Decimal128 {
                coefficient: 1,
                precision: 1,
                scale: 0,
            },
        ),
        (
            T::Decimal {
                bits: 128,
                precision: 9,
                scale: 2,
            },
            V::Decimal128 {
                coefficient: 1,
                precision: 8,
                scale: 2,
            },
        ),
        (
            T::Decimal {
                bits: 128,
                precision: 9,
                scale: 2,
            },
            V::Decimal128 {
                coefficient: 1,
                precision: 9,
                scale: 1,
            },
        ),
        (
            T::Timestamp {
                unit: U::Microsecond,
                timezone: Some("UTC".into()),
            },
            V::Timestamp {
                ticks: 1,
                unit: U::Nanosecond,
            },
        ),
        (
            T::Opaque(O::Bitmap),
            V::Opaque {
                kind: O::Hll,
                bytes: b"same",
            },
        ),
        (T::Date, V::TimeMicros(1)),
    ] {
        rejects(&make_schema(ty, true), value, E::Type);
    }
    for (bits, values) in [
        (8, [-129, 128]),
        (16, [-32769, 32768]),
        (32, [i64::from(i32::MIN) - 1, i64::from(i32::MAX) + 1]),
    ] {
        let schema = make_schema(T::SignedInteger(bits), false);
        for value in values {
            rejects(&schema, V::SignedInteger { bits, value }, E::InvalidValue);
        }
    }
    let container = make_schema(
        T::List(Box::new(ScalarField {
            nullable: true,
            value_type: T::Binary,
        })),
        true,
    );
    // Container values are written by ScalarRecordWriter; this cursor only
    // frames their header-only absent and null records.
    rejects(&container, V::Binary(b"[]"), E::UnsupportedContainer);
    for value in [V::Null, V::NoRows] {
        let cursor = ScalarLeafCursor::try_new(&container, value).unwrap();
        assert_eq!(cursor.encoded_len(), SCALAR_LEAF_HEADER_BYTES);
    }
}

#[test]
fn decimal128_precision_edges_preserve_unscaled_value_and_reject_min_without_overflow() {
    const MAX_38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;
    for (precision, scale, coefficient) in [
        (1, 0, 9),
        (1, 1, -9),
        (38, 38, MAX_38),
        (38, 0, -MAX_38),
        (38, 7, 0),
    ] {
        let schema = make_schema(
            T::Decimal {
                bits: 128,
                precision,
                scale,
            },
            false,
        );
        let value = V::Decimal128 {
            coefficient,
            precision,
            scale,
        };
        assert_eq!(
            emit(ScalarLeafCursor::try_new(&schema, value).unwrap(), 1),
            golden(
                [6, 0, 128, 0, precision, scale, 0, 0],
                &coefficient.to_le_bytes()
            )
        );
    }
    for (precision, coefficient) in [
        (1, 10),
        (1, -10),
        (38, MAX_38 + 1),
        (38, -MAX_38 - 1),
        (38, i128::MIN),
        (38, i128::MAX),
    ] {
        rejects(
            &make_schema(
                T::Decimal {
                    bits: 128,
                    precision,
                    scale: 0,
                },
                false,
            ),
            V::Decimal128 {
                coefficient,
                precision,
                scale: 0,
            },
            E::InvalidValue,
        );
    }
}

#[test]
fn decimal256_76_digit_signed_edges_and_min_have_exact_finite_limb_behavior() {
    // Independent integer fixtures for +/- (10^76 - 1), not codec arithmetic.
    let positive = [
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x95, 0x71, 0xf1, 0xa5, 0x75,
        0x77, 0x79, 0x29, 0x65, 0xe8, 0xab, 0xb4, 0x64, 0x07, 0xb5, 0x15, 0x99, 0x11, 0xa7, 0xcc,
        0x1b, 0x16,
    ];
    let negative = [
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x6a, 0x8e, 0x0e, 0x5a, 0x8a,
        0x88, 0x86, 0xd6, 0x9a, 0x17, 0x54, 0x4b, 0x9b, 0xf8, 0x4a, 0xea, 0x66, 0xee, 0x58, 0x33,
        0xe4, 0xe9,
    ];
    for (coefficient_le, scale) in [(positive, 76), (negative, 0), ([0; 32], 7)] {
        let schema = make_schema(
            T::Decimal {
                bits: 256,
                precision: 76,
                scale,
            },
            false,
        );
        assert_eq!(
            emit(
                ScalarLeafCursor::try_new(
                    &schema,
                    V::Decimal256 {
                        coefficient_le,
                        precision: 76,
                        scale
                    }
                )
                .unwrap(),
                3
            ),
            golden([6, 0, 0, 1, 76, scale, 0, 0], &coefficient_le)
        );
    }
    let mut ten_to_76 = positive;
    ten_to_76[..9].fill(0);
    ten_to_76[9] = 0x10;
    let mut minus_ten_to_76 = negative;
    minus_ten_to_76[0] = 0;
    let mut min = [0; 32];
    min[31] = 0x80;
    let mut max = [0xff; 32];
    max[31] = 0x7f;
    let wide_schema = make_schema(
        T::Decimal {
            bits: 256,
            precision: 76,
            scale: 0,
        },
        false,
    );
    for coefficient_le in [ten_to_76, minus_ten_to_76, min, max] {
        rejects(
            &wide_schema,
            V::Decimal256 {
                coefficient_le,
                precision: 76,
                scale: 0,
            },
            E::InvalidValue,
        );
    }
    let schema = make_schema(
        T::Decimal {
            bits: 256,
            precision: 1,
            scale: 0,
        },
        false,
    );
    for (low, fill) in [(9, 0), (0xf7, 0xff)] {
        let mut coefficient_le = [fill; 32];
        coefficient_le[0] = low;
        assert!(
            ScalarLeafCursor::try_new(
                &schema,
                V::Decimal256 {
                    coefficient_le,
                    precision: 1,
                    scale: 0
                }
            )
            .is_ok()
        );
    }
    for (low, fill) in [(10, 0), (0xf6, 0xff)] {
        let mut coefficient_le = [fill; 32];
        coefficient_le[0] = low;
        rejects(
            &schema,
            V::Decimal256 {
                coefficient_le,
                precision: 1,
                scale: 0,
            },
            E::InvalidValue,
        );
    }
}

#[test]
fn decoder_returns_raw_fixed_values_and_distinguishes_absent_from_null() {
    let cases = [
        (
            T::Float32,
            V::Float32(0x7fc1_2345),
            [4, 0, 32, 0, 0, 0, 0, 0],
            vec![0x45, 0x23, 0xc1, 0x7f],
        ),
        (
            T::Float64,
            V::Float64(0x8000_0000_0000_0000),
            [5, 0, 64, 0, 0, 0, 0, 0],
            vec![0, 0, 0, 0, 0, 0, 0, 0x80],
        ),
        (
            T::Decimal {
                bits: 128,
                precision: 2,
                scale: 2,
            },
            V::Decimal128 {
                coefficient: -99,
                precision: 2,
                scale: 2,
            },
            [6, 0, 128, 0, 2, 2, 0, 0],
            (-99i128).to_le_bytes().to_vec(),
        ),
        (
            T::Timestamp {
                unit: U::Nanosecond,
                timezone: Some("Asia/Shanghai".into()),
            },
            V::Timestamp {
                ticks: -1,
                unit: U::Nanosecond,
            },
            [11, 0, 64, 0, 0, 0, 2, 0],
            vec![0xff; 8],
        ),
    ];
    for (ty, expected, declaration, payload) in cases {
        let schema = make_schema(ty, false);
        assert_eq!(
            V::decode(&schema, &golden(declaration, &payload)).unwrap(),
            expected
        );
    }
    let schema = make_schema(T::Binary, true);
    assert_eq!(
        V::decode(&schema, &golden([8, 1, 0, 0, 0, 0, 0, 0], &[])).unwrap(),
        V::Null
    );
    assert_eq!(
        V::decode(&schema, &golden([8, 3, 0, 0, 0, 0, 0, 0], &[])).unwrap(),
        V::NoRows
    );
}

#[test]
fn decoder_variable_results_borrow_exact_original_payload_and_preserve_identity() {
    let text = "雪\0'\\";
    let raw = [0xff, 0, 0x80, b'\\'];
    let cases = [
        (T::String, [7, 0, 0, 0, 0, 0, 0, 0], text.as_bytes()),
        (T::Json, [18, 0, 0, 0, 0, 0, 0, 0], &[1, 0x20]),
        (T::Binary, [8, 0, 0, 0, 0, 0, 0, 0], raw.as_slice()),
        (T::Variant, [13, 0, 0, 0, 0, 0, 0, 0], raw.as_slice()),
        (T::Opaque(O::Hll), [14, 0, 0, 0, 0, 0, 0, 1], raw.as_slice()),
        (
            T::Opaque(O::Bitmap),
            [14, 0, 0, 0, 0, 0, 0, 2],
            raw.as_slice(),
        ),
        (
            T::Opaque(O::Object),
            [14, 0, 0, 0, 0, 0, 0, 3],
            raw.as_slice(),
        ),
        (
            T::Opaque(O::Percentile),
            [14, 0, 0, 0, 0, 0, 0, 4],
            raw.as_slice(),
        ),
    ];
    for (ty, declaration, payload) in cases {
        let record = golden(declaration, payload);
        let schema = make_schema(ty.clone(), false);
        let decoded = V::decode(&schema, &record).unwrap();
        let bytes = match (&ty, decoded) {
            (T::String, V::String(value)) => value.as_bytes(),
            (T::Json, V::Json(value))
            | (T::Binary, V::Binary(value))
            | (T::Variant, V::Variant(value)) => value,
            (T::Opaque(expected), V::Opaque { kind, bytes }) => {
                assert_eq!(kind, *expected);
                bytes
            }
            _ => panic!("decoder changed the frozen semantic domain"),
        };
        assert_eq!(bytes, payload);
        assert_eq!(
            bytes.as_ptr(),
            record[24..].as_ptr(),
            "decoded value copied its admitted payload"
        );
    }
}

#[test]
fn decoder_rejects_each_malformed_declaration_and_incomplete_or_extra_record() {
    let schema = make_schema(T::SignedInteger(32), true);
    let valid = golden([2, 0, 32, 0, 0, 0, 0, 0], &[0x78, 0x56, 0x34, 0x12]);
    for offset in [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
    ] {
        let mut malformed = valid.clone();
        malformed[offset] ^= 1;
        let expected = if matches!(offset, 10 | 11) {
            E::ValueLimit
        } else {
            E::MalformedRecord
        };
        assert_eq!(
            V::decode(&schema, &malformed),
            Err(expected),
            "declaration offset {offset}"
        );
    }
    for flags in [1, 2, 3, 4, 0x80, 0xff] {
        let mut malformed = valid.clone();
        malformed[13] = flags;
        assert_eq!(
            V::decode(&schema, &malformed),
            Err(E::MalformedRecord),
            "flags {flags} on nonempty payload"
        );
    }
    for length in 0..valid.len() {
        assert_eq!(
            V::decode(&schema, &valid[..length]),
            Err(E::MalformedRecord)
        );
    }
    let mut extra = valid;
    extra.push(0);
    assert_eq!(V::decode(&schema, &extra), Err(E::MalformedRecord));

    let string = golden([7, 0, 0, 0, 0, 0, 0, 0], b"same");
    assert_eq!(
        V::decode(&make_schema(T::Json, false), &string),
        Err(E::MalformedRecord)
    );
    let micros = golden([11, 0, 64, 0, 0, 0, 1, 0], &[0; 8]);
    let nanos_schema = make_schema(
        T::Timestamp {
            unit: U::Nanosecond,
            timezone: Some("UTC".into()),
        },
        false,
    );
    assert_eq!(V::decode(&nanos_schema, &micros), Err(E::MalformedRecord));
    let hll = golden([14, 0, 0, 0, 0, 0, 0, 1], b"same");
    assert_eq!(
        V::decode(&make_schema(T::Opaque(O::Bitmap), false), &hll),
        Err(E::MalformedRecord)
    );
    // Timezone is deliberately a fact of the paired immutable schema, not a
    // field in this record. These tests do not prove a source timezone bridge.
}

#[test]
fn decoder_checks_utf8_boolean_fixed_width_nullability_and_length_before_payload() {
    for (ty, declaration) in [
        (T::String, [7, 0, 0, 0, 0, 0, 0, 0]),
        (T::Json, [18, 0, 0, 0, 0, 0, 0, 0]),
    ] {
        assert_eq!(
            V::decode(
                &make_schema(ty, true),
                &golden(declaration, &[0xff, 0xc0, 0x80])
            ),
            Err(E::MalformedRecord)
        );
    }
    let boolean = make_schema(T::Boolean, false);
    for payload in [vec![], vec![2], vec![0, 1]] {
        assert_eq!(
            V::decode(&boolean, &golden([1, 0, 8, 0, 0, 0, 0, 0], &payload)),
            Err(E::MalformedRecord)
        );
    }
    let null = golden([1, 1, 8, 0, 0, 0, 0, 0], &[]);
    assert_eq!(V::decode(&boolean, &null), Err(E::Nullability));
    let absent = golden([1, 3, 8, 0, 0, 0, 0, 0], &[]);
    assert_eq!(V::decode(&boolean, &absent).unwrap(), V::NoRows);
    let invalid_null_type = golden([0, 0, 0, 0, 0, 0, 0, 0], &[]);
    assert_eq!(
        V::decode(&make_schema(T::Null, true), &invalid_null_type),
        Err(E::MalformedRecord)
    );
    let binary = make_schema(T::Binary, false);
    let exact = golden([8, 0, 0, 0, 0, 0, 0, 0], &vec![0xff; 65_512]);
    assert!(matches!(V::decode(&binary, &exact), Ok(V::Binary(bytes)) if bytes.len() == 65_512));
    let oversized = golden([8, 0, 0, 0, 0, 0, 0, 0], &vec![0; 65_513]);
    assert_eq!(V::decode(&binary, &oversized), Err(E::ValueLimit));
    assert_eq!(
        V::decode(&binary, &oversized[..24]),
        Err(E::ValueLimit),
        "declared length must refuse before accessing absent payload"
    );
}

#[test]
fn decoder_rechecks_decimal_coefficient_precision_and_signed_minimum() {
    let decimal128 = make_schema(
        T::Decimal {
            bits: 128,
            precision: 1,
            scale: 0,
        },
        false,
    );
    for coefficient in [10i128, -10, i128::MIN] {
        let record = golden([6, 0, 128, 0, 1, 0, 0, 0], &coefficient.to_le_bytes());
        assert_eq!(V::decode(&decimal128, &record), Err(E::InvalidValue));
    }
    let decimal256 = make_schema(
        T::Decimal {
            bits: 256,
            precision: 1,
            scale: 1,
        },
        false,
    );
    for (low, fill) in [(9, 0), (0xf7, 0xff)] {
        let mut coefficient_le = [fill; 32];
        coefficient_le[0] = low;
        let record = golden([6, 0, 0, 1, 1, 1, 0, 0], &coefficient_le);
        assert_eq!(
            V::decode(&decimal256, &record).unwrap(),
            V::Decimal256 {
                coefficient_le,
                precision: 1,
                scale: 1
            }
        );
    }
    for (low, fill) in [(10, 0), (0xf6, 0xff)] {
        let mut coefficient_le = [fill; 32];
        coefficient_le[0] = low;
        assert_eq!(
            V::decode(
                &decimal256,
                &golden([6, 0, 0, 1, 1, 1, 0, 0], &coefficient_le)
            ),
            Err(E::InvalidValue)
        );
    }
    let wide = make_schema(
        T::Decimal {
            bits: 256,
            precision: 76,
            scale: 0,
        },
        false,
    );
    let mut min = [0; 32];
    min[31] = 0x80;
    assert_eq!(
        V::decode(&wide, &golden([6, 0, 0, 1, 76, 0, 0, 0], &min)),
        Err(E::InvalidValue)
    );
}

#[test]
fn header_only_preflight_checks_complete_declarations_before_payload_assembly() {
    let binary = make_schema(T::Binary, false);
    let mut exact = golden([8, 0, 0, 0, 0, 0, 0, 0], &[]);
    exact[4..8].copy_from_slice(&65_536u32.to_le_bytes());
    exact[8..12].copy_from_slice(&65_512u32.to_le_bytes());
    let checked = ScalarLeafHeader::decode(&binary, &exact).unwrap();
    assert_eq!(checked.payload_bytes(), 65_512);
    assert_eq!(checked.record_bytes(), 65_536);
    assert_eq!(checked.rows(), 1);
    assert_eq!(
        V::decode(&binary, &exact),
        Err(E::MalformedRecord),
        "a header grants no completed value"
    );
    let mut too_large = exact.clone();
    too_large[4..8].copy_from_slice(&65_537u32.to_le_bytes());
    too_large[8..12].copy_from_slice(&65_513u32.to_le_bytes());
    assert_eq!(
        ScalarLeafHeader::decode(&binary, &too_large),
        Err(E::ValueLimit)
    );
    for bytes in [&exact[..23], &[0; 25][..]] {
        assert_eq!(
            ScalarLeafHeader::decode(&binary, bytes),
            Err(E::MalformedRecord)
        );
    }
    for offset in [0, 4, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23] {
        let mut malformed = exact.clone();
        malformed[offset] ^= 1;
        assert_eq!(
            ScalarLeafHeader::decode(&binary, &malformed),
            Err(E::MalformedRecord),
            "header preflight offset {offset}"
        );
    }
    let boolean = make_schema(T::Boolean, false);
    let valid = golden([1, 0, 8, 0, 0, 0, 0, 0], &[1]);
    let header = ScalarLeafHeader::decode(&boolean, &valid[..24]).unwrap();
    assert_eq!(header.payload_bytes(), 1);
    assert_eq!(header.record_bytes(), 25);
    for len in [0u32, 2] {
        let mut malformed = valid[..24].to_vec();
        malformed[4..8].copy_from_slice(&(24 + len).to_le_bytes());
        malformed[8..12].copy_from_slice(&len.to_le_bytes());
        assert_eq!(
            ScalarLeafHeader::decode(&boolean, &malformed),
            Err(E::MalformedRecord)
        );
    }
    let null = golden([1, 1, 8, 0, 0, 0, 0, 0], &[]);
    assert_eq!(
        ScalarLeafHeader::decode(&boolean, &null),
        Err(E::Nullability)
    );
    let nullable = make_schema(T::Boolean, true);
    let null = ScalarLeafHeader::decode(&nullable, &null).unwrap();
    assert_eq!(null.payload_bytes(), 0);
    assert_eq!(null.rows(), 1);
    let absent = golden([1, 3, 8, 0, 0, 0, 0, 0], &[]);
    let absent = ScalarLeafHeader::decode(&boolean, &absent).unwrap();
    assert_eq!(absent.payload_bytes(), 0);
    assert_eq!(absent.rows(), 0);
    for flags in [2, 4, 0xff] {
        assert_eq!(
            ScalarLeafHeader::decode(&boolean, &golden([1, flags, 8, 0, 0, 0, 0, 0], &[])),
            Err(E::MalformedRecord)
        );
    }
}

#[test]
fn copy_range_replays_exact_bytes_without_advancing_or_accepting_out_of_range() {
    let schema = make_schema(T::Binary, false);
    let payload = b"raw\0\xff'\\";
    let expected = golden([8, 0, 0, 0, 0, 0, 0, 0], payload);
    let cursor = ScalarLeafCursor::try_new(&schema, V::Binary(payload)).unwrap();
    for position in 0..=expected.len() {
        for capacity in [1, 2, 7, 24, 48] {
            let mut scratch = vec![0xa5; capacity];
            let first = cursor.copy_range(position, &mut scratch).unwrap();
            let len = capacity.min(expected.len() - position);
            assert_eq!(first.emitted_bytes, len);
            assert_eq!(first.complete, position + len == expected.len());
            assert_eq!(&scratch[..len], &expected[position..position + len]);
            assert!(scratch[len..].iter().all(|byte| *byte == 0xa5));
            let mut replay = vec![0xa5; capacity];
            assert_eq!(cursor.copy_range(position, &mut replay).unwrap(), first);
            assert_eq!(replay, scratch);
        }
    }
    for position in [expected.len() + 1, usize::MAX] {
        let mut output = [0xa5; 8];
        assert_eq!(
            cursor.copy_range(position, &mut output),
            Err(E::MalformedRecord)
        );
        assert_eq!(output, [0xa5; 8]);
    }
    let empty = cursor.copy_range(1, &mut []).unwrap();
    assert_eq!(empty.emitted_bytes, 0);
    assert!(!empty.complete);
    assert_eq!(
        emit(cursor, 3),
        expected,
        "range reads must leave the sequential cursor at offset zero"
    );
}
