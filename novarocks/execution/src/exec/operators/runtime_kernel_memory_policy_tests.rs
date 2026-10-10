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

use super::*;

#[test]
fn by_runtime_memory_policy_stopped_domains_outlive_the_active_owner_cap() {
    let mut config = AuthorityConfig::new(2 * 1024 * 1024, 1024 * 1024, 1024 * 1024);
    config.max_accounts = 4;
    config.max_active_owners = 1;
    config.metadata_budget_bytes = 64 * 1024;
    let authority = Arc::new(MemoryAuthority::new(config).unwrap());
    let binding = bound(authority.clone());
    let mut retained = Vec::new();
    for _ in 0..6 {
        // Original zero-payload domains still fund their real owner metadata.
        // A retained stopped owner has no active writer but occupies its slot.
        let domain = binding.account().create_domain(0).unwrap();
        domain.stop_producing().unwrap();
        retained.push(domain);
    }
    authority.request_maintenance(novarocks_memory::MaintenanceReason::ExplicitLocalReclaim);
    let wrong_bound = usize::try_from(config.max_active_owners).unwrap()
        + usize::try_from(config.max_accounts).unwrap();
    let partial = authority.maintain(wrong_bound);
    assert_eq!(partial.scanned, wrong_bound);
    assert!(partial.upper > wrong_bound);
    assert!(!partial.complete);
    let actual_bound = authority.maintenance_scan_bound().unwrap();
    assert!(actual_bound >= partial.upper);
    let full = authority.maintain(actual_bound);
    assert!(full.complete);
    assert_eq!(full.scanned, full.upper);
    // Holding their actual FundingDomain owners prevents metadata reclamation.
    assert_eq!(retained.len(), 6);
}

#[test]
fn by_runtime_memory_policy_complete_operation_uses_exact_same_authority_workset() {
    let binding = bound(authority(65536));
    let admitted = request_complete_operation(Some(&binding), 608);
    let KernelMemoryAdmission::Granted(ready) = admitted else {
        panic!("actual complete-operation request: {admitted:?}");
    };
    assert_eq!(ready.stock_bytes(), 608);
    assert_eq!(ready.threshold_bytes(), 0);
    assert_eq!(ready.domain().snapshot().authorized, 608);
    assert_eq!(
        ready.domain().lane().affiliation().id(),
        binding.account().id()
    );
    let allocator = AttributingAllocator::new(System);
    let mut journal = KernelMemoryJournal::default();
    let block = ready.run(&mut journal, || allocate(&allocator)).unwrap();
    let actual = journal.settlement.as_ref().unwrap();
    assert_eq!(actual.accepted_live, 608);
    assert_eq!(actual.debt, 0);
    assert_eq!(actual.next_step, Ok(()));
    assert_eq!(journal.stopped, Some(Ok(())));
    release(&allocator, block);
    assert_eq!(ready.domain().snapshot().live, 0);
}

#[test]
fn by_runtime_memory_policy_absent_capability_has_no_default_authority() {
    assert!(matches!(
        request_complete_operation(None, 608),
        KernelMemoryAdmission::MissingQueryMemory
    ));
}
