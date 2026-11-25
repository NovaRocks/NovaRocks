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

//! Canonical encoding primitives for versioned cross-owner facts.

use crate::{ValueTypeRef, write_value_type_identity_observed};
use std::convert::Infallible;

/// Encodes an ordered fact. Each fact author owns the order of its fields and
/// sorts any unordered input according to that fact's contract.
pub struct CanonicalFactWriter {
    bytes: Vec<u8>,
}

impl CanonicalFactWriter {
    pub fn new(domain: &'static str, format_version: u32) -> Self {
        let mut writer = Self { bytes: Vec::new() };
        writer.text(domain);
        writer.u32(format_version);
        writer
    }

    pub fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn i32(&mut self, value: i32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn boolean(&mut self, value: bool) {
        self.bytes.push(u8::from(value));
    }

    pub fn bytes(&mut self, value: &[u8]) {
        self.sequence(value.len());
        self.bytes.extend_from_slice(value);
    }

    pub fn text(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    pub fn sequence(&mut self, len: usize) {
        self.u64(len as u64);
    }

    pub fn value_type(&mut self, value: &ValueTypeRef) {
        let length_offset = self.bytes.len();
        self.u64(0);
        let start = self.bytes.len();
        let result: Result<(), Infallible> = write_value_type_identity_observed(value, |chunk| {
            self.bytes.extend_from_slice(chunk);
            Ok(())
        });
        match result {
            Ok(()) => {}
            Err(never) => match never {},
        }
        let length = (self.bytes.len() - start) as u64;
        self.bytes[length_offset..start].copy_from_slice(&length.to_be_bytes());
    }

    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}
