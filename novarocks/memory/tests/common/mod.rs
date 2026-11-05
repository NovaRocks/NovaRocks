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

#![allow(dead_code)]
use novarocks_memory::*;
pub fn authority(bytes: u64) -> MemoryAuthority {
    let mut config = AuthorityConfig::new(bytes * 2, bytes, bytes);
    config.max_accounts = 32;
    config.metadata_budget_bytes = 16 * 1024;
    config.max_active_owners = 4;
    config.top_up = TopUpPolicy::uniform(1024);
    MemoryAuthority::new(config).unwrap()
}
pub fn work(a: &MemoryAuthority) -> Account {
    a.create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap()
}
pub fn exited() -> TeardownEvidence<'static> {
    TeardownEvidence {
        tasks_exited: true,
        operators_destroyed: true,
        io: &[],
        now_ns: 1,
    }
}
pub fn free(origin: FactToken, bytes: u64) {
    unsafe { origin.record_deallocation(bytes) }
}

pub fn reference(owner: &LaneShards) -> lane::RecordRef {
    owner
        .explicit_owner()
        .reference()
        .expect("covered test owner")
}
pub fn contains(owner: &LaneShards, reference: lane::RecordRef) -> bool {
    owner
        .snapshot()
        .records
        .iter()
        .flatten()
        .any(|shard| shard.reference == reference)
}
