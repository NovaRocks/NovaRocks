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

use std::sync::{Arc, Barrier};

use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory::policy::LimitDimension;

#[test]
fn policy_install_racing_a_top_up_never_hides_committed_excess() {
    for _ in 0..100 {
        let mut config = AuthorityConfig::new(128, 64, 64);
        config.top_up = TopUpPolicy::uniform(4);
        let authority = MemoryAuthority::new(config).unwrap();
        let work = authority
            .create_account(AccountKind::Work, ExternalRef::from_u128(1))
            .unwrap();
        let start = Arc::new(Barrier::new(2));
        let requester = work.clone();
        let ready = Arc::clone(&start);
        let join = std::thread::spawn(move || {
            ready.wait();
            requester.request_grant(12)
        });
        start.wait();
        work.install_policy(8, LimitDimension::Work);
        let grant = join.join().unwrap();
        let snapshot = work.snapshot();
        assert_eq!(snapshot.policy_limit_bytes, Some(8));
        if grant.is_ok() {
            assert_eq!(snapshot.committed_bytes, 12);
            assert_eq!(snapshot.excess_bytes, 4);
            assert!(snapshot.growth_frozen);
        } else {
            assert_eq!(snapshot.excess_bytes, 0);
            assert!(snapshot.committed_bytes <= 8);
        }
        drop(grant);
    }
}
