// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;

#[test]
fn original_numeric_templates_have_source_capacity_and_output_goldens() {
    assert_eq!(usize::MAX.ilog10() + 1, 20);
    let max = usize::MAX;
    assert_eq!(format!("local_{max}_{max}").len(), 47);
    assert_eq!(
        original_format_string_request_bound(14, 47).unwrap(),
        FormatStringRequestFacts {
            allocation_requests_upper_bound: 3,
            request_bytes_upper_bound: 188,
            request_alignment: 1,
        }
    );
    assert_eq!(
        format!("map entries expected 2 struct fields, got {max}").len(),
        62
    );
    let arity = original_format_string_request_bound(84, 62).unwrap();
    assert_eq!(arity.allocation_requests_upper_bound, 1);
    assert_eq!(arity.request_bytes_upper_bound, 84);
}

#[test]
fn arbitrary_original_write_splits_stay_within_cumulative_request_bound() {
    // Exercise actual String append growth for both single-byte writes and
    // large segments, independently recording every changed capacity. The
    // bound must cover the sum, including the initial backing, not just the
    // last retained buffer. No allocator/grant observation is claimed here.
    for initial in [1, 7, 14, 54, 84] {
        for count in [0, 1, 7, 8, 47, 62, 127, 1025] {
            let facts = original_format_string_request_bound(initial, count).unwrap();
            for split in [1, 3, 31, 1025] {
                let mut destination = String::with_capacity(initial);
                let mut requests = 1;
                let mut bytes = destination.capacity();
                while destination.len() < count {
                    let old = destination.capacity();
                    let written = split.min(count - destination.len());
                    destination.push_str(&"x".repeat(written));
                    if destination.capacity() != old {
                        requests += 1;
                        bytes += destination.capacity();
                    }
                }
                assert!(requests <= facts.allocation_requests_upper_bound);
                assert!(bytes <= facts.request_bytes_upper_bound);
            }
        }
    }
}

#[test]
fn unauthored_initial_capacity_and_arithmetic_overflow_are_typed() {
    assert!(matches!(
        original_format_string_request_bound(0, 47),
        Err(ControlResourceError::SourceModel(_))
    ));
    assert_eq!(
        original_format_string_request_bound(14, usize::MAX),
        Err(resource())
    );
    assert_eq!(
        original_format_string_request_bound(usize::MAX, 1),
        Err(resource())
    );
    let max = isize::MAX as usize;
    let facts = original_format_string_request_bound(max / 2, max).unwrap();
    assert_eq!(facts.request_bytes_upper_bound, max * 2);
    assert_eq!(facts.allocation_requests_upper_bound, 2);
}
