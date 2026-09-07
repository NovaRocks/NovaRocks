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

use std::env;
use std::fs;

use datasketches::theta::{CompactThetaSketch, ThetaUnionBuilder};

fn main() {
    let mut args = env::args().skip(1);
    let input_path = args
        .next()
        .expect("usage: verify-theta-interop INPUT EXPECTED [UNION_INPUT UNION_EXPECTED]");
    let expected = parse_expected(args.next(), "EXPECTED");
    let input = read_compact(&input_path);
    assert_estimate(&input_path, input.estimate(), expected);

    let mut union_estimate = None;
    if let Some(union_path) = args.next() {
        let union_expected = parse_expected(args.next(), "UNION_EXPECTED");
        let other = read_compact(&union_path);
        let mut union = ThetaUnionBuilder::default()
            .lg_k(12)
            .build()
            .expect("theta union");
        union.update(&input).expect("union input");
        union.update(&other).expect("union other input");
        let estimate = union.to_sketch(true).estimate();
        assert_estimate("union", estimate, union_expected);
        union_estimate = Some(estimate);
    }

    assert!(args.next().is_none(), "unexpected trailing arguments");
    match union_estimate {
        Some(estimate) => println!(
            "verified theta interop: input_estimate={} union_estimate={}",
            input.estimate(),
            estimate
        ),
        None => println!(
            "verified theta interop: input_estimate={}",
            input.estimate()
        ),
    }
}

fn read_compact(path: &str) -> CompactThetaSketch {
    let bytes = fs::read(path).unwrap_or_else(|error| panic!("read {path}: {error}"));
    CompactThetaSketch::deserialize(&bytes)
        .unwrap_or_else(|error| panic!("deserialize {path}: {error}"))
}

fn parse_expected(value: Option<String>, name: &str) -> f64 {
    value
        .unwrap_or_else(|| panic!("missing {name}"))
        .parse()
        .unwrap_or_else(|error| panic!("parse {name}: {error}"))
}

fn assert_estimate(label: &str, actual: f64, expected: f64) {
    assert_eq!(
        actual.to_bits(),
        expected.to_bits(),
        "{label} estimate: actual={actual}, expected={expected}"
    );
}
