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

//! Client-facing query identity belongs to Query Application, not a transport.
use crate::session_control::ClientQueryId;
use novarocks_types::FrontendProcessId;
use std::sync::Mutex;

pub(crate) struct ClientQueryIdGenerator {
    suffix: [u8; 5],
    clock: Mutex<(i64, u32)>,
}
impl ClientQueryIdGenerator {
    pub(crate) fn new(process: FrontendProcessId) -> Self {
        let bytes = process.to_bytes();
        let mut bits = u32::from_be_bytes(bytes[12..16].try_into().expect("UUID tail"));
        let mut suffix = [0; 5];
        for byte in suffix.iter_mut().rev() {
            *byte = b"abcdefghijklmnopqrstuvwxyz234567"[(bits & 31) as usize];
            bits >>= 5;
        }
        Self {
            suffix,
            clock: Mutex::new((i64::MIN, 0)),
        }
    }
    fn reserve(clock: &mut (i64, u32), now: i64) -> Option<(i64, u32)> {
        if now > clock.0 {
            *clock = (now, 0);
        }
        // Match Trino QueryIdGenerator: never wrap or reuse a same-second ordinal.
        if clock.1 >= 99_999 {
            return None;
        }
        clock.1 += 1;
        Some(*clock)
    }
    pub(crate) fn mint(&self) -> ClientQueryId {
        let mut clock = self.clock.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            let now = chrono::Utc::now().timestamp();
            if let Some((second, ordinal)) = Self::reserve(&mut clock, now) {
                let timestamp = chrono::DateTime::from_timestamp(second, 0).expect("UTC time");
                return ClientQueryId(
                    format!(
                        "{}_{ordinal:05}_{}",
                        timestamp.format("%Y%m%d_%H%M%S"),
                        std::str::from_utf8(&self.suffix).expect("base32 suffix")
                    )
                    .into_boxed_str(),
                );
            }
            // Exhaustion or a backwards clock waits for a new second, without holding session state.
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ids_have_trino_shape_and_strictly_increasing_ordinals() {
        let generator = ClientQueryIdGenerator::new(FrontendProcessId::new_v7());
        let first = generator.mint();
        let second = generator.mint();
        assert_eq!(first.as_str().len(), 27);
        assert!(
            first
                .as_str()
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        );
        assert!(second.as_str() > first.as_str());
    }
    #[test]
    fn exhausted_or_reversed_clock_never_reuses_an_id() {
        let mut clock = (100, 99_998);
        assert_eq!(
            ClientQueryIdGenerator::reserve(&mut clock, 100),
            Some((100, 99_999))
        );
        assert_eq!(ClientQueryIdGenerator::reserve(&mut clock, 100), None);
        assert_eq!(ClientQueryIdGenerator::reserve(&mut clock, 99), None);
        assert_eq!(
            ClientQueryIdGenerator::reserve(&mut clock, 101),
            Some((101, 1))
        );
    }
}
