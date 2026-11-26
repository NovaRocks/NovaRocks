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

//! Incremental UTF-8 validation for bounded output streams.
use crate::{RenderError, RenderErrorKind};
fn invalid_utf8() -> RenderError {
    RenderError {
        kind: RenderErrorKind::InvalidUtf8,
        output_ordinal: None,
        required: None,
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Utf8 {
    remaining: u8,
    first_min: u8,
    first_max: u8,
}
impl Utf8 {
    pub fn feed(&mut self, b: u8) -> Result<(), RenderError> {
        if self.remaining != 0 {
            if b < self.first_min || b > self.first_max {
                return Err(invalid_utf8());
            }
            self.remaining -= 1;
            self.first_min = 0x80;
            self.first_max = 0xbf;
            return Ok(());
        }
        (self.remaining, self.first_min, self.first_max) = match b {
            0..=0x7f => (0, 0, 0),
            0xc2..=0xdf => (1, 0x80, 0xbf),
            0xe0 => (2, 0xa0, 0xbf),
            0xe1..=0xec | 0xee..=0xef => (2, 0x80, 0xbf),
            0xed => (2, 0x80, 0x9f),
            0xf0 => (3, 0x90, 0xbf),
            0xf1..=0xf3 => (3, 0x80, 0xbf),
            0xf4 => (3, 0x80, 0x8f),
            _ => return Err(invalid_utf8()),
        };
        Ok(())
    }
    pub fn finish(self) -> Result<(), RenderError> {
        if self.remaining == 0 {
            Ok(())
        } else {
            Err(invalid_utf8())
        }
    }
}
