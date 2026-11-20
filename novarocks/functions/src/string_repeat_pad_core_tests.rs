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
fn shared_repeat_pad_original_payload_read_frontiers() {
    assert_eq!(
        repeat_original(repeat_plan(
            || panic!("negative repeat did not read payload"),
            -1
        )),
        Some(String::new())
    );
    let mut reads = 0;
    assert_eq!(
        repeat_original(repeat_plan(
            || {
                reads += 1;
                "x"
            },
            0
        )),
        Some(String::new())
    );
    assert_eq!(reads, 1);
    for count in [-1, 1_048_577, i64::MAX] {
        let mut out = Vec::new();
        pad_into(
            "x",
            count,
            || panic!("oversize/negative original PAD did not read padding"),
            true,
            OriginalPadProjection::new(&mut out),
        )
        .unwrap();
        assert_eq!(out, vec![None]);
    }
}
#[test]
fn shared_repeat_pad_original_unicode_and_independent_cap_outputs() {
    assert_eq!(
        repeat_original(repeat_plan(|| "", i64::MAX)),
        Some(String::new())
    );
    assert_eq!(
        repeat_original(repeat_plan(|| "é", 524_288)).unwrap().len(),
        1_048_576
    );
    assert_eq!(repeat_original(repeat_plan(|| "é", 524_289)), None);
    assert_eq!(repeat_original(repeat_plan(|| " ", -1)), None);
    assert_eq!(repeat_original(repeat_plan(|| " ", 0)), Some(String::new()));
    for (left, expected) in [(true, "🙂x🙂é中"), (false, "é中🙂x🙂")] {
        let mut out = Vec::new();
        pad_into(
            "é中",
            5,
            || "🙂x",
            left,
            OriginalPadProjection::new(&mut out),
        )
        .unwrap();
        assert_eq!(out, vec![Some(expected.to_owned())]);
    }
    let long = "🙂".repeat(262_145);
    let mut out = Vec::new();
    pad_into(
        &long,
        262_145,
        || "",
        true,
        OriginalPadProjection::new(&mut out),
    )
    .unwrap();
    assert_eq!(out, vec![None]);
}
