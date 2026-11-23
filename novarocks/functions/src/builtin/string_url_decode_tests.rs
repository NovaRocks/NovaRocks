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

const ORACLES: [(&str, &str); 10] = [
    ("", ""),
    ("AZaz09-_.~+", "AZaz09-_.~+"),
    ("%20%2B%25%3F%26%3D%2F", " +%?&=/"),
    ("%00%09%0A%0D%7F", "\0\t\n\r\u{7f}"),
    ("%C3%A9%E4%B8%AD", "é中"),
    ("a%CC%81", "a\u{301}"),
    ("%F0%9F%91%A9%E2%80%8D%F0%9F%92%BB", "👩‍💻"),
    ("%252f", "%2f"),
    ("%21%24%27%28%29%2A%3B%2C", "!$'()*;,"),
    ("Straße+%C4%B0%CE%A3", "Straße+İΣ"),
];

#[test]
fn required_decode_uses_original_oracles_and_retains_small_utf8_source_with_global_index() {
    use novarocks_failure_contract::FailureProjection;
    use std::error::Error;
    for (input, expected) in ORACLES {
        let source: ArrayRef = Arc::new(StringArray::from(vec![input]));
        let output = evaluate_required(&source, 3).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(expected); 3]
        );
    }
    let invalid = "a".repeat(300) + "%FF";
    let source: ArrayRef = Arc::new(StringArray::from(vec![
        Some("%41"),
        None,
        Some(&invalid),
        Some("%GG"),
    ]));
    let weak = Arc::downgrade(&source);
    let failure = evaluate_required(&source, 4).unwrap_err();
    drop(source);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        failure.failure_fact(),
        crate::DataFailureReason::InvalidEncoding.failure_fact()
    );
    assert_eq!(
        failure.to_string(),
        "url_decode utf8 decode failed: invalid utf-8 sequence of 1 bytes from index 300"
    );
    let required = failure
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<RequiredDecodeFailure>()
        .unwrap();
    assert_eq!(required._ordinal, 2);
    let actual = required
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<std::str::Utf8Error>()
        .unwrap();
    assert_eq!(actual.valid_up_to(), 44);
    assert_eq!(actual.error_len(), Some(1));
    let source: ArrayRef = Arc::new(StringArray::from(vec!["%FF%GG"]));
    assert_eq!(
        evaluate_required(&source, 1).unwrap_err().to_string(),
        "decode string contains illegal hex chars: GG"
    );
}
