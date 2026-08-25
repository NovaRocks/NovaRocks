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

use novarocks_proto::novarocks;

fn encoded_field_numbers<M: Message>(message: &M) -> Vec<u32> {
    let bytes = message.encode_to_vec();
    let mut fields = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let key = read_varint(&bytes, &mut offset);
        let field_number = (key >> 3) as u32;
        let wire_type = (key & 0x7) as u8;
        fields.push(field_number);
        match wire_type {
            0 => {
                let _ = read_varint(&bytes, &mut offset);
            }
            1 => offset += 8,
            2 => {
                let len = read_varint(&bytes, &mut offset) as usize;
                offset += len;
            }
            5 => offset += 4,
            other => panic!("unsupported wire type {other} in encoded proto"),
        }
    }
    fields
}

fn read_varint(bytes: &[u8], offset: &mut usize) -> u64 {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes
            .get(*offset)
            .unwrap_or_else(|| panic!("truncated varint at offset {}", *offset));
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
        assert!(shift < 64, "varint overflow");
    }
}

#[test]
fn fetch_result_response_uses_pre_release_reset_tags() {
    use novarocks::fetch_result_response::Status;

    assert_eq!(Status::ResultStatusUnspecified as i32, 0);
    assert_eq!(Status::Ready as i32, 1);
    assert_eq!(Status::NotReady as i32, 2);
    assert_eq!(Status::Eof as i32, 3);
    assert_eq!(Status::Error as i32, 4);

    let response = novarocks::FetchResultResponse {
        status: Status::Ready as i32,
        message: "ready".to_string(),
        result_arrow_ipc: b"NRX1".to_vec(),
        packet_seq: 9,
        eos: true,
    };
    let fields = encoded_field_numbers(&response);

    assert!(fields.contains(&1), "status must use reset tag 1");
    assert!(fields.contains(&2), "message must use reset tag 2");
    assert!(fields.contains(&3), "result_arrow_ipc must use reset tag 3");
    assert!(fields.contains(&4), "packet_seq must use reset tag 4");
    assert!(fields.contains(&5), "eos must use reset tag 5");
    assert!(
        !fields.contains(&6),
        "pre-release reset must not keep the old eos tag"
    );
}
