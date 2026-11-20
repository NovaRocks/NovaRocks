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

const WIDTH: usize = 16;
const UTF8_CAPACITY: usize = WIDTH * 4;

#[derive(Clone, Copy)]
pub(super) enum BoundBytes<'a> {
    Borrowed(&'a [u8]),
    Inline {
        bytes: [u8; UTF8_CAPACITY],
        len: usize,
    },
}

impl BoundBytes<'_> {
    pub(super) fn inline(value: &[u8]) -> Self {
        let mut bytes = [0; UTF8_CAPACITY];
        bytes[..value.len()].copy_from_slice(value);
        Self::Inline {
            bytes,
            len: value.len(),
        }
    }

    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Inline { bytes, len } => &bytes[..*len],
        }
    }
}

pub(super) fn binary_bound(bytes: &[u8], upper: bool) -> Option<BoundBytes<'_>> {
    if bytes.len() <= WIDTH || !upper {
        return Some(BoundBytes::Borrowed(&bytes[..bytes.len().min(WIDTH)]));
    }
    let offset = bytes[..WIDTH].iter().rposition(|byte| *byte != u8::MAX)?;
    let mut successor = [0; WIDTH];
    successor[..offset].copy_from_slice(&bytes[..offset]);
    successor[offset] = bytes[offset] + 1;
    Some(BoundBytes::inline(&successor[..=offset]))
}

pub(super) fn string_bound(string: &str, upper: bool) -> Option<BoundBytes<'_>> {
    let Some((end, _)) = string.char_indices().nth(WIDTH) else {
        return Some(BoundBytes::Borrowed(string.as_bytes()));
    };
    let prefix = &string[..end];
    if !upper {
        return Some(BoundBytes::Borrowed(prefix.as_bytes()));
    }
    for (offset, character) in prefix.char_indices().rev() {
        let codepoint = match character as u32 {
            0xD7FF => 0xE000,
            value => value + 1,
        };
        if let Some(successor) = char::from_u32(codepoint) {
            let mut bytes = [0; UTF8_CAPACITY];
            bytes[..offset].copy_from_slice(&prefix.as_bytes()[..offset]);
            let len = successor.encode_utf8(&mut bytes[offset..]).len();
            return Some(BoundBytes::Inline {
                bytes,
                len: offset + len,
            });
        }
    }
    None
}
