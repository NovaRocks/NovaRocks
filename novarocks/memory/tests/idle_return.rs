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
fn release_returns_idle_capacity_through_ancestors_before_query_exit() {
    let mut config = AuthorityConfig::new(128, 64, 64);
    config.top_up = TopUpPolicy::uniform(4);
    let authority = MemoryAuthority::new(config).unwrap();
    let group = authority
        .create_account(AccountKind::ResourceGroup, ExternalRef::from_u128(1))
        .unwrap();
    let a = group
        .create_child(AccountKind::Work, ExternalRef::from_u128(2))
        .unwrap();
    let b = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(3))
        .unwrap();
    let charge = a.request_grant(24).unwrap().fulfil(24).unwrap();
    assert!(b.request_grant(56).is_err());

    charge.release();
    assert!(a.committed_bytes() <= 4);
    assert!(group.committed_bytes() <= 8);
    assert_eq!(a.snapshot().live_bytes, 0);
    let other = b
        .request_grant(56)
        .expect("root capacity returned synchronously");
    assert_eq!(other.remaining_bytes(), 56);
    assert!(authority.snapshot().honours_capacity_bound());
}
