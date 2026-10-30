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

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_native_adapter::root_cow_selection_codec::{CowSelectionEncoder, CowSelectionTotals};
use novarocks_native_adapter::root_record_assembly::{
    RootRecordAssembly, RootRecordAssemblyError, RootRecordDomain,
};
use novarocks_result_render::RenderTurnStatus;

fn cow_stream() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_file", DataType::Utf8, false),
        Field::new("_pos", DataType::Int64, false),
    ]));
    let batch = |files: Vec<&str>, positions: Vec<i64>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(files)) as ArrayRef,
                Arc::new(Int64Array::from(positions)) as ArrayRef,
            ],
        )
        .unwrap()
    };
    let mut totals = CowSelectionTotals::default();
    let mut stream = Vec::new();
    for input in [
        batch(vec!["s3://a", "s3://bb"], vec![1, 2]),
        batch(vec!["s3://ccc"], vec![7]),
    ] {
        let mut encoder = CowSelectionEncoder::try_new(&input, totals, usize::MAX).unwrap();
        let mut output = vec![0_u8; 1 << 16];
        loop {
            let turn = encoder.step(&mut output);
            stream.extend_from_slice(&output[..turn.emitted_bytes]);
            if turn.status == RenderTurnStatus::InputComplete {
                break;
            }
        }
        totals = encoder.totals();
    }
    stream
}

fn records(stream: &[u8], cuts: &[usize]) -> Vec<Vec<u8>> {
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::CowSelection, 1 << 20);
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
    let stream = cow_stream();
    let whole = records(&stream, &[]);
    assert_eq!(whole.len(), 3, "one schema record and two batch records");
    assert_eq!(whole.concat(), stream);
    for cut in 1..stream.len() {
        assert_eq!(records(&stream, &[cut]), whole, "cut at {cut}");
    }
    let every_byte = (1..stream.len()).collect::<Vec<_>>();
    assert_eq!(records(&stream, &every_byte), whole);
}

#[test]
fn whole_records_in_one_body_are_not_copied() {
    let stream = cow_stream();
    let range = stream.as_ptr_range();
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::CowSelection, 1 << 20);
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
    let stream = cow_stream();
    let first = records(&stream, &[])[0].len();
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::CowSelection, first - 1);
    // The header arrives alone; the oversized declaration is refused at once.
    let error = assembly
        .push::<std::convert::Infallible>(&stream[..32], |_| Ok(()))
        .unwrap_err();
    assert!(error.to_string().contains("assembly bound"), "{error}");
    assert!(assembly.retained_bytes() <= 32);
}

#[test]
fn end_inside_a_record_and_forged_headers_are_refused() {
    let stream = cow_stream();
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::CowSelection, 1 << 20);
    assembly
        .push::<std::convert::Infallible>(&stream[..40], |_| Ok(()))
        .unwrap();
    assert!(assembly.finish().is_err());
    let mut forged = stream.clone();
    forged[0] = b'X';
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::CowSelection, 1 << 20);
    assert!(
        assembly
            .push::<std::convert::Infallible>(&forged, |_| Ok(()))
            .is_err()
    );
    // A decoder refusal stops assembly with its own message.
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::CowSelection, 1 << 20);
    let error = assembly
        .push(&stream, |_| Err("decoder refused".to_string()))
        .unwrap_err();
    assert!(
        matches!(error, RootRecordAssemblyError::Sink(message) if message == "decoder refused")
    );
}
