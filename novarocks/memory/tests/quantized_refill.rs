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

use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};

#[test]
fn ancestor_capacity_refusal_retries_the_actual_shortfall() {
    let mut config = AuthorityConfig::new(28, 14, 14);
    config.top_up = TopUpPolicy::uniform(8);
    let authority = MemoryAuthority::new(config).unwrap();
    let group = authority
        .create_account(AccountKind::ResourceGroup, ExternalRef::from_u128(1))
        .unwrap();
    let work = group
        .create_child(AccountKind::Work, ExternalRef::from_u128(2))
        .unwrap();

    // The rounded 16-byte request cannot fit at the root. The 9-byte
    // obligation can, and exact mode must propagate through both ancestors.
    let grant = work.request_grant(9).unwrap();
    assert_eq!(grant.remaining_bytes(), 9);
    assert_eq!(work.committed_bytes(), 9);
    assert_eq!(group.committed_bytes(), 9);
    assert!(authority.snapshot().honours_capacity_bound());
}

#[test]
fn a_real_shortfall_is_still_refused_after_exact_retry() {
    let mut config = AuthorityConfig::new(28, 14, 14);
    config.top_up = TopUpPolicy::uniform(8);
    let authority = MemoryAuthority::new(config).unwrap();
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    assert!(work.request_grant(15).is_err());
    assert_eq!(work.committed_bytes(), 0);
    assert!(authority.snapshot().honours_capacity_bound());
}
