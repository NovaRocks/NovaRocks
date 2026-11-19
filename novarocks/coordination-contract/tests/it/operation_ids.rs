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

use novarocks_coordination_contract::{IdSeed, OperationIds};
use novarocks_execution_contract::TaskOperationId;

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
