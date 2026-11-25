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
use crate::KernelFailure;
use std::cell::Cell;
use std::convert::Infallible;
use std::rc::Rc;

#[derive(Default)]
struct TestWork {
    output: Vec<TestGrant>,
    live: Rc<Cell<usize>>,
    peak: usize,
    limit: usize,
    steps: usize,
    stop_at: Option<usize>,
}
struct TestGrant {
    bytes: usize,
    live: Rc<Cell<usize>>,
}
impl Drop for TestGrant {
    fn drop(&mut self) {
        self.live.set(self.live.get() - self.bytes);
    }
}

impl JsonWork for TestWork {
    type Reservation = TestGrant;

    fn step(&mut self) -> Result<(), KernelFailure> {
        self.steps += 1;
        if self.stop_at == Some(self.steps) {
            Err(KernelFailure::ResourceExhausted)
        } else {
            Ok(())
        }
    }

    fn reserve(&mut self, bytes: usize) -> Result<Self::Reservation, KernelFailure> {
        let next = self.live.get() + bytes;
        if self.limit != 0 && next > self.limit {
            return Err(KernelFailure::ResourceExhausted);
        }
        self.live.set(next);
        self.peak = self.peak.max(next);
        Ok(TestGrant {
            bytes,
            live: Rc::clone(&self.live),
        })
    }

    fn reserve_output(&mut self, bytes: usize) -> Result<(), KernelFailure> {
        let grant = self.reserve(bytes)?;
        self.output.push(grant);
        Ok(())
    }
}

fn valid(bytes: &[u8]) -> bool {
    Node::from_root(bytes).is_ok_and(|node| node.validate(|| Ok::<_, Infallible>(())).is_ok())
}

pub(crate) fn test_array(values: &[Option<&str>]) -> arrow_array::LargeBinaryArray {
    values
        .iter()
        .map(|value| value.map(|text| parse(text, &mut TestWork::default()).unwrap()))
        .collect()
}

fn text(raw: &str) -> String {
    let mut work = TestWork::default();
    let bytes = parse(raw, &mut work).unwrap();
    assert!(valid(&bytes), "{raw}");
    format(&bytes, &mut work).unwrap()
}

#[test]
fn layouts_preserve_compact_wire_facts() {
    let mut work = TestWork::default();
    assert_eq!(parse("null", &mut work).unwrap(), [1, 0x20]);
    assert_eq!(
        parse("12.99", &mut work).unwrap(),
        [1, 0x52, 4, 2, 2, 0x13, 0x05]
    );
    let array = parse("[12.99,5.49,100.00]", &mut work).unwrap();
    assert_eq!(array.len(), 22);
    let object = parse(r#"{"name":"abc","tags":["x","y"]}"#, &mut work).unwrap();
    assert_eq!(object.len(), 28);
    let bytes = parse("0.00", &mut work).unwrap();
    assert!(matches!(
        Node::from_root(&bytes).unwrap().kind().unwrap(),
        Kind::Scalar(Scalar::Decimal {
            precision: 2,
            scale: 2,
            coefficient: 0
        })
    ));
    assert_eq!(
        text("[-0.00,100.00,1e0,-0e0,5e-324,1e23]"),
        "[0.00,100.00,1.0,-0.0,4.9E-324,1.0E23]"
    );
}

#[test]
fn indexed_ranges_and_duplicate_matches_keep_original_order() {
    let mut work = TestWork::default();
    for count in 7..=9 {
        let members: Vec<_> = (0..count)
            .map(|i| {
                (
                    if i % 3 == 0 { "k" } else { "x" },
                    Input::Scalar(Scalar::Integer(i)),
                )
            })
            .collect();
        let bytes = encode(Input::Object(&members), &mut work).unwrap();
        assert!(valid(&bytes));
        assert_eq!(bytes[1] >> 5, if count < 8 { 4 } else { 6 });
        let Kind::Object(object) = Node::from_root(&bytes).unwrap().kind().unwrap() else {
            panic!("object");
        };
        assert_eq!(object.len(), count as usize);
        assert_eq!(
            object
                .entries()
                .map(|entry| entry.unwrap().key.unwrap())
                .collect::<Vec<_>>(),
            members.iter().map(|entry| entry.0).collect::<Vec<_>>()
        );
        let matches = object
            .lookup("k")
            .unwrap()
            .map(|node| match node.unwrap().kind().unwrap() {
                Kind::Scalar(Scalar::Integer(i)) => i,
                _ => panic!("integer match"),
            })
            .collect::<Vec<_>>();
        assert_eq!(matches, vec![0, 3, 6]);
    }
    assert_eq!(
        text(r#"{"k":0,"x":1,"y":2,"k":3,"a":4,"b":5,"c":6,"d":7,"e":8,"k":9}"#),
        r#"{"k":0,"x":1,"y":2,"k":3,"a":4,"b":5,"c":6,"d":7,"e":8,"k":9}"#
    );
    for count in [256, 257] {
        let values = vec![("a", Input::Null); count];
        let bytes = encode(Input::Object(&values), &mut work).unwrap();
        assert_eq!((bytes[1] >> 1) & 3, u8::from(count > 256));
        assert!(valid(&bytes));
    }
}

#[test]
fn lengths_and_offset_widths_are_minimal_at_boundaries() {
    let mut work = TestWork::default();
    for size in [127, 128] {
        let payload = " ".repeat(size);
        let bytes = encode(Input::Scalar(Scalar::String(&payload)), &mut work).unwrap();
        assert!(valid(&bytes));
        assert_eq!(
            &bytes[2..bytes.len() - size],
            if size < 128 { &[127][..] } else { &[128, 1] }
        );
    }

    for (size, code) in [(255, 0), (256, 1), (65_535, 1), (65_536, 2)] {
        let string_size = if size < 1_000 { size - 10 } else { size - 11 };
        let string = " ".repeat(string_size);
        let mut input = [Input::Null; 8];
        input[0] = Input::Scalar(Scalar::String(&string));
        let bytes = encode(Input::Array(&input), &mut work).unwrap();
        assert!(valid(&bytes));
        assert_eq!((bytes[1] >> 3) & 3, code);
        let Kind::Array(array) = Node::from_root(&bytes).unwrap().kind().unwrap() else {
            panic!("array");
        };
        assert!(
            matches!(array.get(0).unwrap().unwrap().value.kind().unwrap(), Kind::Scalar(Scalar::String(s)) if s.len() == string_size)
        );
        assert!(matches!(
            array.get(7).unwrap().unwrap().value.kind().unwrap(),
            Kind::Null
        ));
        assert!(array.get(8).unwrap().is_none());
    }
    #[cfg(target_pointer_width = "64")]
    {
        let low = super::codec::Layout::new(false, 8, 0xffff_ffff).unwrap();
        let high = super::codec::Layout::new(false, 8, 0x1_0000_0000).unwrap();
        assert_eq!(low.offset_width, 4);
        assert_eq!(high.offset_width, 8);
    }
}

#[test]
fn external_validation_rejects_noncanonical_tags_directories_and_extents() {
    let mut work = TestWork::default();
    for bytes in [
        &[0, 0x20][..],
        &[1, 0x21],
        &[1, 0x40],
        &[1, 0x43, 0],
        &[1, 0x44, 0x80, 0],
        &[1, 0x44, 0x80],
        &[1, 0x44, 1, 0xff],
        &[1, 0x20, 0x20],
        &[
            1, 0x60, 9, 8, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
        ],
        &[
            1, 0x44, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02,
        ],
    ] {
        assert!(!valid(bytes), "{bytes:?}");
    }
    let mut wrong_offsets = parse("[null,null,null,null,null,null,null,null]", &mut work).unwrap();
    wrong_offsets[6] = wrong_offsets[5];
    assert!(!valid(&wrong_offsets));
    let mut nonminimal = vec![1, 0xa8, 27, 8];
    for offset in 0_u16..9 {
        nonminimal.extend(offset.to_le_bytes());
    }
    nonminimal.extend([0x20; 8]);
    assert!(!valid(&nonminimal));
    let mut duplicate_ordinal = encode(Input::Object(&[("a", Input::Null); 8]), &mut work).unwrap();
    assert!(valid(&duplicate_ordinal));
    duplicate_ordinal[14] = duplicate_ordinal[13];
    assert!(!valid(&duplicate_ordinal));
}

#[test]
fn finite_coefficients_share_minimal_signed_bytes_without_losing_decimal_scale() {
    let mut work = TestWork::default();
    for (value, expected) in [
        (0, &[0][..]),
        (127, &[0x7f]),
        (128, &[0x80, 0]),
        (-128, &[0x80]),
        (-129, &[0x7f, 0xff]),
    ] {
        let bytes = encode(
            Input::Scalar(Scalar::Decimal {
                precision: 3,
                scale: 2,
                coefficient: value,
            }),
            &mut work,
        )
        .unwrap();
        assert_eq!(&bytes[5..], expected);
        assert!(valid(&bytes));
        let bytes = encode(
            Input::Scalar(Scalar::Number(Number::Finite {
                scale: 0,
                coefficient: expected,
            })),
            &mut work,
        )
        .unwrap();
        assert_eq!(&bytes[8..], expected);
        assert!(valid(&bytes));
    }
    for coefficient in [&[][..], &[0, 0], &[0x80, 0xff], &[0x17, 0]] {
        let mut decimal = vec![1, 0x52, 38, 0, coefficient.len() as u8];
        decimal.extend(coefficient);
        assert!(!valid(&decimal));
        let mut number = vec![1, 0x54, 0, 0, 0, 0, 0, coefficient.len() as u8];
        number.extend(coefficient);
        assert!(!valid(&number));
    }
    for (precision, scale, coefficient) in [(0, 0, 1), (39, 0, 1), (4, 5, 1), (2, 0, 100)] {
        let result = encode(
            Input::Scalar(Scalar::Decimal {
                precision,
                scale,
                coefficient,
            }),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(JsonError::Value(JsonValueError::Scalar))
        ));
    }
    for (scale, coefficient) in [(1, &[0][..]), (0, &[10]), (16_384, &[1])] {
        let result = encode(
            Input::Scalar(Scalar::Number(Number::Finite { scale, coefficient })),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(JsonError::Value(JsonValueError::Scalar))
        ));
    }
}

#[test]
fn shared_text_scanner_preserves_unicode_markers_numbers_and_strict_errors() {
    assert_eq!(
        text(
            r#"{"$serde_json::private::Number":"x","$serde_json::private::RawValue":"[]","😀":"\uD83D\uDE00"}"#
        ),
        r#"{"$serde_json::private::Number":"x","$serde_json::private::RawValue":"[]","😀":"😀"}"#
    );
    assert_eq!(
        text(r#"["a\u000b","\u007f","\\","\/","\t", "é"]"#),
        r#"["a\u000B","","\\","/","\t","é"]"#
    );
    let digits = "1".repeat(200);
    for (source, rendered) in [("4", "0"), ("5", "0")] {
        let expected = if source == "4" {
            format!("{digits}{rendered}")
        } else {
            format!("{}2{rendered}", &digits[..199])
        };
        assert_eq!(text(&format!("{digits}{source}")), expected);
    }
    let mut work = TestWork::default();
    for input in [
        "",
        "[] x",
        "[1,]",
        "[,1]",
        "01",
        "1.",
        "1E+",
        "1e9999",
        r#"{"a":1,}"#,
        r#""\uDE00""#,
        r#""\uD83Dx""#,
    ] {
        assert!(
            matches!(parse(input, &mut work), Err(JsonError::InvalidText(_))),
            "{input:?}"
        );
    }
    assert_eq!(java_double_decimal(1e23), Some((1, -23)));
    assert_eq!(java_double_decimal(f64::from_bits(1)), Some((49, 325)));
    assert_eq!(
        java_double_decimal(80_241_618_952_346.125),
        Some((8_024_161_895_234_612, 2))
    );
}

#[test]
fn maximum_depth_uses_one_resumable_cursor_and_original_scratch() {
    let input = format!("{}0{}", "[".repeat(MAX_DEPTH), "]".repeat(MAX_DEPTH));
    let mut work = TestWork::default();
    let bytes = parse(&input, &mut work).unwrap();
    assert!(valid(&bytes));
    assert_eq!(format(&bytes, &mut work).unwrap(), input);
    let mut direct: TextCursor<TestGrant> =
        TextCursor::with_preallocated_scratch(&mut work).unwrap();
    let peak = work.live.get();
    let mut text = Vec::new();
    let mut entered = 0;
    loop {
        match direct.advance(&bytes, &mut work).unwrap() {
            TextEvent::EnteredItem => entered += 1,
            TextEvent::Byte(byte) => text.push(byte),
            TextEvent::Done => break,
        }
    }
    assert_eq!(text, input.as_bytes());
    assert_eq!(entered, MAX_DEPTH + 1);
    assert_eq!(peak, work.live.get());
    direct.reset();
    assert_eq!(
        direct.advance(&[1, 0x20], &mut work).unwrap(),
        TextEvent::EnteredItem
    );
    let input = format!("[{input}]");
    assert!(matches!(parse(&input, &mut work), Err(JsonError::Depth)));
}

#[test]
fn work_and_allocation_failures_follow_actual_scratch_and_retained_output() {
    let input = format!("[{}]", "0,".repeat(40_000) + "0");
    let mut refused = TestWork {
        limit: 1024,
        ..Default::default()
    };
    assert!(matches!(
        parse(&input, &mut refused),
        Err(JsonError::Host(KernelFailure::ResourceExhausted))
    ));
    assert_eq!(refused.live.get(), 0);
    let mut stopped = TestWork {
        stop_at: Some(500),
        ..Default::default()
    };
    assert!(matches!(
        parse(&input, &mut stopped),
        Err(JsonError::Host(KernelFailure::ResourceExhausted))
    ));
    assert_eq!(stopped.live.get(), 0);
    let mut funded = TestWork::default();
    let bytes = parse(&input, &mut funded).unwrap();
    assert_eq!(funded.live.get(), bytes.capacity());
    let result = format(&bytes, &mut funded).unwrap();
    assert_eq!(funded.live.get(), bytes.capacity() + result.capacity());
    assert_eq!(result, input);
    drop(result);
    drop(bytes);
    funded.output.clear();
    assert_eq!(funded.live.get(), 0);
}

#[test]
fn typed_scalars_preserve_bits_temporal_coordinates_and_standard_text() {
    use novarocks_type_contract::{
        PICOS_PER_MINUTE, TemporalMicros, TemporalPrecision, TemporalType, TemporalValue,
        TimeZoneKey,
    };
    let mut work = TestWork::default();
    let clock = TemporalValue::TimeWithTimeZone {
        picos: 30 * PICOS_PER_MINUTE + 123_456_789_123,
        offset_minutes: 330,
    };
    let ty = TemporalType::TimeWithTimeZone(TemporalPrecision::PICOSECONDS);
    let time = Input::Scalar(Scalar::Temporal { ty, value: clock });
    let wall_ty = TemporalType::Timestamp(TemporalPrecision::new(3).unwrap());
    let wall = TemporalValue::Timestamp(TemporalMicros::from_picos(-877_000_000_000).unwrap());
    let timestamp = Input::Scalar(Scalar::Temporal {
        ty: wall_ty,
        value: wall,
    });
    let zone = TimeZoneKey::parse("America/New_York").unwrap();
    let instant = TemporalMicros::from_picos(1_727_755_200_123_456_789_123).unwrap();
    let zoned_ty = TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS);
    let zoned = TemporalValue::TimestampWithTimeZone { instant, zone };
    let payload = [
        Input::Scalar(Scalar::Date(19_782)),
        time,
        timestamp,
        Input::Scalar(Scalar::Temporal {
            ty: zoned_ty,
            value: zoned,
        }),
        Input::Scalar(Scalar::Double(f64::from_bits(0x7ff8_0000_0000_0001))),
        Input::Scalar(Scalar::Real(f32::NEG_INFINITY)),
        Input::Scalar(Scalar::Number(Number::PositiveInfinity)),
    ];
    let encoded = encode(Input::Array(&payload), &mut work).unwrap();
    assert!(valid(&encoded));
    let Kind::Array(array) = Node::from_root(&encoded).unwrap().kind().unwrap() else {
        panic!("array");
    };
    assert!(
        matches!(array.get(1).unwrap().unwrap().value.kind().unwrap(),
                     Kind::Scalar(Scalar::Temporal { ty: actual, value }) if actual == ty && value == clock)
    );
    assert!(
        matches!(array.get(2).unwrap().unwrap().value.kind().unwrap(),
                     Kind::Scalar(Scalar::Temporal { ty: actual, value }) if actual == wall_ty && value == wall)
    );
    assert!(
        matches!(array.get(3).unwrap().unwrap().value.kind().unwrap(),
                     Kind::Scalar(Scalar::Temporal { ty: actual, value }) if actual == zoned_ty && value == zoned)
    );
    assert!(
        matches!(array.get(4).unwrap().unwrap().value.kind().unwrap(),
                     Kind::Scalar(Scalar::Double(value)) if value.to_bits() == 0x7ff8_0000_0000_0001)
    );
    assert_eq!(
        format(&encoded, &mut work).unwrap(),
        r#"["2024-02-29","00:30:00.123456789123+05:30","1969-12-31 23:59:59.123","2024-10-01 00:00:00.123456789123 America/New_York","NaN","-Infinity","+Infinity"]"#
    );
}
