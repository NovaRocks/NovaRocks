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

use prost::Message;

use novarocks_proto::{common, filter};

fn roundtrip_message<M>(value: &M) -> M
where
    M: Message + Default,
{
    M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
}

fn sample_unique_id() -> common::UniqueId {
    common::UniqueId {
        hi: 0x1122_3344_5566_7788,
        lo: -0x0102_0304_0506_0708,
    }
}

fn sample_column(slot_id: i32, bytes: Vec<u8>) -> filter::Column {
    let data_size = bytes.len() as i64;
    filter::Column {
        slot_id,
        data_size,
        data: bytes,
    }
}

#[test]
fn lookup_request_with_multiple_columns_survives_proto_roundtrip() {
    let original = filter::LookupRequest {
        query_id: Some(sample_unique_id()),
        lookup_node_id: 7,
        request_tuple_id: 9,
        request_columns: vec![
            sample_column(11, vec![0x00, 0x01, 0x02, 0x03]),
            sample_column(12, vec![0xff, 0x80, 0x40, 0x00]),
        ],
    };

    assert_eq!(original, roundtrip_message(&original));
}

#[test]
fn lookup_response_with_status_and_column_survives_proto_roundtrip() {
    let original = filter::LookupResponse {
        status: Some(common::Status {
            code: 13,
            message: "lookup failed".to_string(),
        }),
        columns: vec![sample_column(21, vec![0x08, 0x96, 0x01, 0x00])],
    };

    assert_eq!(original, roundtrip_message(&original));
}

#[test]
fn column_preserves_opaque_bytes_across_proto_roundtrip() {
    let original = sample_column(31, vec![0x00, 0xff, 0x2a, 0x80, 0x7f, 0x00]);

    let decoded: filter::Column = roundtrip_message(&original);
    assert_eq!(original, decoded);
    assert_eq!(decoded.data, vec![0x00, 0xff, 0x2a, 0x80, 0x7f, 0x00]);
}
