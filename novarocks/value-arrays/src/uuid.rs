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

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Encode canonical UUID bytes as lowercase, hyphenated ASCII.
pub fn format_uuid(bytes: [u8; 16]) -> [u8; 36] {
    let mut text = [b'-'; 36];
    let mut cursor = 0;
    for byte in bytes {
        if matches!(cursor, 8 | 13 | 18 | 23) {
            cursor += 1;
        }
        text[cursor] = HEX[usize::from(byte >> 4)];
        text[cursor + 1] = HEX[usize::from(byte & 0x0f)];
        cursor += 2;
    }
    text
}

/// Decode the canonical 36-byte UUID form, accepting ASCII hex in either case.
pub fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let text: &[u8; 36] = text.as_bytes().try_into().ok()?;
    let mut bytes = [0; 16];
    let mut cursor = 0;
    for byte in &mut bytes {
        if matches!(cursor, 8 | 13 | 18 | 23) {
            if text[cursor] != b'-' {
                return None;
            }
            cursor += 1;
        }
        let high = hex_nibble(text[cursor])?;
        let low = hex_nibble(text[cursor + 1])?;
        *byte = (high << 4) | low;
        cursor += 2;
    }
    Some(bytes)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
