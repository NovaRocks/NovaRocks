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

//! Records reassemble identically however the stream is cut, whole records
//! pass through without copies, and declared lengths are bounded before any
//! assembly buffer is reserved.

use novarocks_native_adapter::root_record_assembly::{
    RootRecordAssembly, RootRecordAssemblyError, RootRecordDomain,
};
use novarocks_native_adapter::root_statistics_codec::STATISTICS_HEADER_BYTES;

// Author two valid StatisticsArtifactV1 records directly from the documented
// framing contract. This assembly oracle does not depend on a producer cursor.
fn statistics_records() -> Vec<Vec<u8>> {
    [&b"aa"[..], &b"bbb"[..]]
        .into_iter()
        .map(|body| {
            let mut record = Vec::new();
            let record_bytes = STATISTICS_HEADER_BYTES + 4 + 1 + body.len();
            record.extend_from_slice(b"STA1");
            for word in [record_bytes as u32, 1, 1, body.len() as u32, 0] {
                record.extend_from_slice(&word.to_le_bytes());
            }
            record.extend_from_slice(&1_i32.to_le_bytes());
            record.extend_from_slice(b"x");
            record.extend_from_slice(body);
            record
        })
        .collect()
}

fn statistics_stream() -> Vec<u8> {
    statistics_records().concat()
}

fn records(stream: &[u8], cuts: &[usize]) -> Vec<Vec<u8>> {
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::Statistics, 1 << 20);
    let mut out = Vec::new();
    let mut start = 0;
    for &cut in cuts.iter().chain(std::iter::once(&stream.len())) {
        assembly
            .push::<std::convert::Infallible>(&stream[start..cut], |record| {
                out.push(record.to_vec());
                Ok(())
            })
            .unwrap();
        start = cut;
    }
    assembly.finish().unwrap();
    out
}

#[test]
fn every_cut_position_reassembles_the_same_records() {
    let expected = statistics_records();
    let stream = expected.concat();
    let whole = records(&stream, &[]);
    assert_eq!(
        whole, expected,
        "whole input retains the authored record boundaries"
    );
    assert_eq!(
        whole.len(),
        2,
        "exactly two data records and no Schema record"
    );
    assert_eq!(whole.concat(), stream);
    for cut in 1..stream.len() {
        assert_eq!(records(&stream, &[cut]), whole, "cut at {cut}");
    }
    let every_byte = (1..stream.len()).collect::<Vec<_>>();
    assert_eq!(records(&stream, &every_byte), whole);
}

#[test]
fn whole_records_in_one_body_are_not_copied() {
    let stream = statistics_stream();
    let range = stream.as_ptr_range();
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::Statistics, 1 << 20);
    assembly
        .push::<std::convert::Infallible>(&stream, |record| {
            assert!(range.contains(&record.as_ptr()), "record was copied");
            Ok(())
        })
        .unwrap();
    assert_eq!(assembly.retained_bytes(), 0);
}

#[test]
fn declared_length_is_bounded_before_assembly_reserves_it() {
    let stream = statistics_stream();
    let first = statistics_records()[0].len();
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::Statistics, first - 1);
    // The header arrives alone; the oversized declaration is refused at once.
    let error = assembly
        .push::<std::convert::Infallible>(&stream[..STATISTICS_HEADER_BYTES], |_| Ok(()))
        .unwrap_err();
    assert!(error.to_string().contains("assembly bound"), "{error}");
    assert!(assembly.retained_bytes() <= STATISTICS_HEADER_BYTES);
}

#[test]
fn end_inside_a_record_and_forged_headers_are_refused() {
    let stream = statistics_stream();
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::Statistics, 1 << 20);
    assembly
        .push::<std::convert::Infallible>(&stream[..statistics_records()[0].len() - 1], |_| Ok(()))
        .unwrap();
    assert!(assembly.finish().is_err());
    let mut forged = stream.clone();
    forged[0] = b'X';
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::Statistics, 1 << 20);
    assert!(
        assembly
            .push::<std::convert::Infallible>(&forged, |_| Ok(()))
            .is_err()
    );
    // A decoder refusal stops assembly with its own message.
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::Statistics, 1 << 20);
    let error = assembly
        .push(&stream, |_| Err("decoder refused".to_string()))
        .unwrap_err();
    assert!(
        matches!(error, RootRecordAssemblyError::Sink(message) if message == "decoder refused")
    );
}
