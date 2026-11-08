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

//! Seeded operation identity and reconnect jitter. No clock or random source is read.

use novarocks_execution_contract::TaskOperationId;

/// Initial random counter and original wall-clock timestamp supplied by the host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IdSeed {
    start: u128,
    start_wall_ms: u64,
}

impl IdSeed {
    /// The top counter bit is clear to preserve at least 2^73 values.
    pub const fn new(start: u128, start_wall_ms: u64) -> Self {
        assert!(
            start < (1u128 << 73),
            "operation seed exceeds 73 random bits"
        );
        assert!(
            start_wall_ms < (1u64 << 48),
            "operation timestamp exceeds UUIDv7 width"
        );
        Self {
            start,
            start_wall_ms,
        }
    }

    pub const fn counter(self) -> u128 {
        self.start
    }
    pub const fn wall_ms(self) -> u64 {
        self.start_wall_ms
    }

    /// Domain-separated deterministic mixing, independent of host HashMap seeds.
    pub fn jitter(self, participant: u128, generation: u64) -> u64 {
        let domain = splitmix64(participant as u64)
            ^ splitmix64((participant >> 64) as u64)
            ^ splitmix64(generation);
        splitmix64(self.start as u64 ^ splitmix64((self.start >> 64) as u64) ^ domain)
    }
}

#[derive(Debug)]
pub struct OperationIds {
    next: u128,
    wall_ms: u64,
}

impl OperationIds {
    pub const fn new(seed: IdSeed) -> Self {
        Self {
            next: seed.start,
            wall_ms: seed.start_wall_ms,
        }
    }

    /// RFC 9562 monotonic-random counter. Exhaustion is an invariant violation,
    /// never an ordinary product operation-count limit.
    pub fn issue(&mut self) -> TaskOperationId {
        assert!(
            self.next < (1u128 << 74),
            "operation identity counter exhausted"
        );
        let counter = self.next;
        self.next += 1;
        let value = ((self.wall_ms as u128) << 80)
            | (7u128 << 76)
            | ((counter >> 62) << 64)
            | (2u128 << 62)
            | (counter & ((1u128 << 62) - 1));
        TaskOperationId::try_from_bytes(value.to_be_bytes()).expect("constructed UUIDv7")
    }
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_counter_preserves_timestamp_version_and_variant_across_low_field_carry() {
        let seed = IdSeed::new((1u128 << 62) - 1, 1_794_000_000_000);
        let mut ids = OperationIds::new(seed);
        let before = ids.issue().to_bytes();
        let after = ids.issue().to_bytes();
        assert!(before < after);
        for bytes in [before, after] {
            assert_eq!(bytes[6] >> 4, 7);
            assert_eq!(bytes[8] >> 6, 2);
            assert_eq!(u128::from_be_bytes(bytes) >> 80, seed.wall_ms() as u128);
            assert!(TaskOperationId::try_from_bytes(bytes).is_ok());
        }
    }

    #[test]
    fn replaying_a_seed_replays_the_exact_operation_sequence() {
        let seed = IdSeed::new(0x123456789abcdef, 12345);
        let mut left = OperationIds::new(seed);
        let mut right = OperationIds::new(seed);
        let mut other = OperationIds::new(IdSeed::new(seed.counter() + 1000, seed.wall_ms()));
        for _ in 0..256 {
            let id = left.issue();
            assert_eq!(id, right.issue());
            assert_ne!(id, other.issue());
        }
        assert_eq!(seed.counter() >> 73, 0);
        assert_eq!(seed.jitter(42, 5), seed.jitter(42, 5));
        assert_ne!(seed.jitter(42, 5), seed.jitter(42, 6));
    }
}
