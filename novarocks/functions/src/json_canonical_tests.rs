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
fn key(s: &str) -> String {
    normalize_json_key(s, Arc::new(UnaccountedAggregateStateAllocator), &Control)
        .unwrap()
        .as_str()
        .to_owned()
}
#[test]
fn trino_483_canonical_examples() {
    for (source, expected) in [
        (r#"{"b":1,"a":2}"#, r#"{"a":2,"b":1}"#),
        (r#"{"a":1,"a":2}"#, r#"{"a":2}"#),
        ("1.0", "1.0"),
        ("1.00", "1.00"),
        ("1e2", "1E+2"),
        ("100.0", "100.0"),
        ("-0.0", "0.0"),
        ("-0", "0"),
        ("1.5e-7", "1.5E-7"),
        ("0.000001", "0.000001"),
        ("0.0000001", "1E-7"),
        ("1E400", "1E+400"),
        (
            "123456789012345678901234567890",
            "123456789012345678901234567890",
        ),
        (r#""é/\u0001""#, r#""é/\u0001""#),
        (r#" { "a" : [ 1 , 2 ] } "#, r#"{"a":[1,2]}"#),
        (
            r#"{"\uffff":1,"\ud800\udc00":2,"z":3,"é":4}"#,
            r#"{"z":3,"é":4,"\uD800\uDC00":2,"￿":1}"#,
        ),
        (r#""\ud800""#, r#""\uD800""#),
        (
            r#"{"$serde_json::private::RawValue":"1"}"#,
            r#"{"$serde_json::private::RawValue":"1"}"#,
        ),
    ] {
        assert_eq!(key(source), expected, "source {source}");
    }
}
#[test]
fn trino_483_equality_examples() {
    for (left, right, equal) in [
        ("1.0", "1.00", false),
        ("1e2", "100", false),
        ("1e2", "100.0", false),
        ("100e0", "100", true),
        ("1.5e-7", "0.00000015", true),
        ("-0.0", "0.0", true),
        ("1", "1.0", false),
        (r#"{"b":2,"a":1}"#, r#"{"a":1,"b":2}"#, true),
        ("[1,2]", "[2,1]", false),
        ("null", "null", true),
        (r#""𐀀""#, r#""\ud800\udc00""#, true),
        (r#"{"a":1,"a":2}"#, r#"{"a":2}"#, true),
    ] {
        assert_eq!(key(left) == key(right), equal, "{left} against {right}");
    }
}
#[test]
fn malformed_json_is_typed_data_without_text_fallback() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    for s in [
        "",
        "01",
        "-",
        "1.",
        "1e",
        "[1,]",
        "{\"a\":1,}",
        "[",
        "{} false",
        r#""\x""#,
        "NaN",
    ] {
        let err = normalize_json_key(s, Arc::new(UnaccountedAggregateStateAllocator), &Control)
            .unwrap_err();
        assert_eq!(err.failure_fact().class(), FailureClass::Data, "{s}");
    }
}
#[test]
fn iterative_depth_and_duplicate_objects_keep_no_private_quota() {
    let input = format!("{}0{}", "[".repeat(2048), "]".repeat(2048));
    assert_eq!(key(&input), input);
    assert_eq!(
        key(r#"{"a":{"z":0,"z":3},"b":1,"a":{"y":2}}"#),
        r#"{"a":{"y":2},"b":1}"#
    );
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
    let value = normalize_json_key(r#"{"b":[3,2,1],"a":"value"}"#, host.clone(), &Control).unwrap();
    assert!(host.bytes.load(Ordering::SeqCst) > 0);
    drop(value);
    assert_eq!(host.bytes.load(Ordering::SeqCst), 0);
    let host = Arc::new(Counted {
        bytes: AtomicUsize::new(0),
        refused: true,
    });
    assert!(matches!(
        normalize_json_key("1", host.clone(), &Control),
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
    let text = format!("\"{}\"", "x".repeat(2048));
    assert!(matches!(
        normalize_json_key(
            &text,
            Arc::new(UnaccountedAggregateStateAllocator),
            &Stop(AtomicUsize::new(0))
        ),
        Err(KernelFailure::Cancelled)
    ));
}

#[test]
fn overwritten_duplicate_values_still_validate_every_bigdecimal_representation() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    for source in [
        "1e2147483648",
        "0.1e-2147483647",
        r#"{"a":1e2147483648,"a":0}"#,
        r#"{"a":{"b":0.1e-2147483647},"a":0}"#,
        r#"{"a":[1e2147483648],"a":0}"#,
    ] {
        let error = normalize_json_key(
            source,
            Arc::new(UnaccountedAggregateStateAllocator),
            &Control,
        )
        .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Data, "{source}");
    }
    // The actual BigDecimal boundary remains accepted; scale is not a size quota.
    assert_eq!(key(r#"{"a":1e2147483647,"a":0}"#), r#"{"a":0}"#);
    assert_eq!(key(r#"{"a":{"b":0.1e-2147483646},"a":0}"#), r#"{"a":0}"#);
}
