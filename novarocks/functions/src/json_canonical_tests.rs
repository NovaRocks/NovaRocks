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
use crate::UnaccountedAggregateStateAllocator;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Ok(())
    }
}

struct Build;
impl crate::json_value::JsonWork for Build {
    type Reservation = ();

    fn step(&mut self) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn reserve(&mut self, _: usize) -> Result<Self::Reservation, KernelFailure> {
        Ok(())
    }

    fn reserve_output(&mut self, _: usize) -> Result<(), KernelFailure> {
        Ok(())
    }
}

fn parse(source: &str) -> Vec<u8> {
    crate::json_value::parse(source, &mut Build).unwrap()
}

fn key_of(bytes: &[u8]) -> Vec<u8> {
    normalize_json_key(
        bytes,
        Arc::new(UnaccountedAggregateStateAllocator),
        &Control,
    )
    .unwrap()
    .as_bytes()
    .to_vec()
}

fn key(source: &str) -> Vec<u8> {
    key_of(&parse(source))
}

#[test]
fn trino_484_equality_examples() {
    for (left, right, equal) in [
        ("1.0", "1.00", true),
        ("1e2", "100", true),
        ("1e2", "100.0", true),
        ("100e0", "100", true),
        ("1.5e-7", "0.00000015", true),
        ("-0.0", "-0e0", true),
        ("-0", "0", true),
        ("1", "1.0", true),
        ("1e23", "100000000000000000000000", true),
        (r#"{"b":2,"a":1}"#, r#"{"a":1,"b":2}"#, true),
        ("[1,2]", "[2,1]", false),
        ("null", "null", true),
        ("null", r#""null""#, false),
        (r#""𐀀""#, r#""\ud800\udc00""#, true),
        (r#"{"a":1,"a":2}"#, r#"{"a":2e0,"a":1.00}"#, true),
        (r#"{"a":1,"a":2}"#, r#"{"a":2}"#, false),
        (r#"{"a":1,"a":2}"#, r#"{"a":1,"a":1}"#, false),
        (r#"{"a":1,"b":2}"#, r#"{"a":2,"b":1}"#, false),
        (
            r#"{"a":{"z":0,"z":3},"b":1,"a":{"y":2}}"#,
            r#"{"b":1,"a":{"y":2},"a":{"z":3e0,"z":0}}"#,
            true,
        ),
        (r#"{"ab":[1],"c":1}"#, r#"{"a":[1],"bc":1}"#, false),
    ] {
        assert_eq!(key(left) == key(right), equal, "{left} against {right}");
    }
}

#[test]
fn typed_real_widens_before_double_decimal_and_temporal_domains_stay_distinct() {
    use crate::json_value::{Input, Scalar, encode};
    use novarocks_type_contract::{TemporalPrecision, TemporalType, TemporalValue};
    let of = |scalar| key_of(&encode(Input::Scalar(scalar), &mut Build).unwrap());
    assert_eq!(of(Scalar::Double(0.1)), key("0.1"));
    assert_ne!(of(Scalar::Real(0.1)), key("0.1"));
    assert_eq!(of(Scalar::Real(f32::NAN)), of(Scalar::Double(f64::NAN)));
    assert_eq!(
        of(Scalar::Real(f32::INFINITY)),
        of(Scalar::Double(f64::INFINITY))
    );
    assert_ne!(
        of(Scalar::Real(f32::NEG_INFINITY)),
        of(Scalar::Double(f64::INFINITY))
    );
    assert_eq!(
        of(Scalar::Double(f64::from_bits(1))),
        key(&format!("0.{}49", "0".repeat(323)))
    );
    assert_ne!(of(Scalar::Date(19_782)), key(r#""2024-02-29""#));
    let time = |picos, offset_minutes| Scalar::Temporal {
        ty: TemporalType::TimeWithTimeZone(TemporalPrecision::new(12).unwrap()),
        value: TemporalValue::TimeWithTimeZone {
            picos,
            offset_minutes,
        },
    };
    const PER_MINUTE: i64 = 60_000_000_000_000;
    assert_eq!(
        of(time(30 * PER_MINUTE, 60)),
        of(time(1_410 * PER_MINUTE, 0))
    );
    assert_ne!(
        of(time(30 * PER_MINUTE, 60)),
        of(time(1_410 * PER_MINUTE + 1, 0))
    );
}

#[test]
fn malformed_native_payload_is_an_invariant_failure() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    for input in [b"null".as_slice(), b"\x01\x42\x02", b"\x01\x20x"] {
        let err = normalize_json_key(
            input,
            Arc::new(UnaccountedAggregateStateAllocator),
            &Control,
        )
        .unwrap_err();
        assert_eq!(err.failure_fact().class(), FailureClass::Invariant);
    }
}

#[test]
fn can_project_1024_containers_without_recursive_stack() {
    let input = format!("{}0{}", "[".repeat(1024), "]".repeat(1024));
    let other = format!("{}-0e0{}", "[".repeat(1024), "]".repeat(1024));
    assert_eq!(key(&input), key(&other));
    assert_ne!(key(&input), key(&input[1..input.len() - 1]));
}

struct Counted {
    bytes: AtomicUsize,
    refused: bool,
}
impl AggregateStateAllocator for Counted {
    fn allocate(&self, layout: std::alloc::Layout) -> Result<std::ptr::NonNull<u8>, KernelFailure> {
        if self.refused {
            return Err(KernelFailure::DeadlineExceeded);
        }
        let ptr = UnaccountedAggregateStateAllocator.allocate(layout)?;
        self.bytes.fetch_add(layout.size(), Ordering::SeqCst);
        Ok(ptr)
    }
    unsafe fn release(&self, block: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        self.bytes.fetch_sub(layout.size(), Ordering::SeqCst);
        unsafe { UnaccountedAggregateStateAllocator.release(block, layout) };
    }
}
#[test]
fn output_keeps_original_host_until_actual_drop_and_refusal_is_preserved() {
    let host = Arc::new(Counted {
        bytes: AtomicUsize::new(0),
        refused: false,
    });
    let document = parse(r#"{"b":[3,2,1],"a":"value"}"#);
    let value = normalize_json_key(&document, host.clone(), &Control).unwrap();
    assert!(host.bytes.load(Ordering::SeqCst) > 0);
    drop(value);
    assert_eq!(host.bytes.load(Ordering::SeqCst), 0);
    let host = Arc::new(Counted {
        bytes: AtomicUsize::new(0),
        refused: true,
    });
    assert!(matches!(
        normalize_json_key(&parse("1"), host.clone(), &Control),
        Err(KernelFailure::DeadlineExceeded)
    ));
    assert_eq!(host.bytes.load(Ordering::SeqCst), 0);
}
#[test]
fn cancellation_during_a_large_value_is_not_data_or_fallback() {
    struct Stop(AtomicUsize);
    impl KernelEvaluationControl for Stop {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            if self.0.fetch_add(1, Ordering::SeqCst) > 80 {
                Err(KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            Ok(())
        }
    }
    let text = parse(&format!("\"{}\"", "x".repeat(65_536)));
    assert!(matches!(
        normalize_json_key(
            &text,
            Arc::new(UnaccountedAggregateStateAllocator),
            &Stop(AtomicUsize::new(0))
        ),
        Err(KernelFailure::Cancelled)
    ));
}
