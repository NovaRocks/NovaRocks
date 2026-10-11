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
use novarocks_memory::{AuthorityConfig, CapacityError};
use novarocks_types::identity::AttemptId;
use novarocks_worker::query_context::QueryMemoryAccountError;

fn authority(max_accounts: u32) -> Arc<MemoryAuthority> {
    let mut cfg = AuthorityConfig::new(1 << 30, 1 << 29, 1 << 29);
    cfg.max_accounts = max_accounts;
    cfg.max_active_owners = 32;
    cfg.metadata_budget_bytes = 1 << 20;
    Arc::new(MemoryAuthority::new(cfg).unwrap())
}
fn execution(attempt: u64) -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(97201, 97202), AttemptId::new(attempt).unwrap()).unwrap()
}
fn admission(
    runtime: &NativeFragmentQueryRuntime,
    id: QueryExecutionId,
    limit: Option<i64>,
) -> Result<NativeFragmentAdmissionResources, String> {
    runtime.prepare_admission_execution(
        id,
        UniqueId::new(97203, 97204),
        Duration::from_secs(30),
        Duration::from_secs(30),
        limit,
        None,
    )
}
#[test]
fn mem_a1_v2_none_creates_real_account_without_local_policy_or_grant() {
    let authority = authority(32);
    let manager = QueryContextManager::new_for_test();
    let runtime = NativeFragmentQueryRuntime::new_for_test(manager, authority.clone());
    let before = authority.root().committed_bytes();
    let resources = admission(&runtime, execution(1), None).unwrap();
    let memory = resources
        .query_memory()
        .expect("native admission always binds query ownership");
    assert_eq!(memory.execution(), execution(1));
    assert_eq!(memory.account().snapshot().policy_limit_bytes, None);
    assert_eq!(memory.account().snapshot().granted_bytes, 0);
    assert_eq!(authority.live_accounts(), 2);
    assert_eq!(
        authority.root().committed_bytes(),
        before + novarocks_memory::ACCOUNT_METADATA_BYTES
    );
}
#[test]
fn mem_a1_v2_none_real_metadata_refusal_keeps_typed_cause_and_full_legacy_text() {
    let authority = authority(1);
    let manager = QueryContextManager::new_for_test();
    let runtime = NativeFragmentQueryRuntime::new_for_test(manager, authority.clone());
    let before = authority.root().committed_bytes();
    let error = runtime
        .prepare_admission_execution_typed(
            execution(1),
            UniqueId::new(97203, 97204),
            Duration::from_secs(30),
            Duration::from_secs(30),
            None,
            None,
        )
        .err()
        .expect("root occupies the real account registry");
    assert!(matches!(
        &error,
        NativeFragmentAdmissionError::MemoryAccount(QueryMemoryAccountError::Capacity(
            CapacityError::MetadataExhausted { .. }
        ))
    ));
    let full = error.to_string();
    assert!(
        full.starts_with("create query memory account: memory request refused: MetadataExhausted")
    );
    assert_eq!(admission(&runtime, execution(1), None).err().unwrap(), full);
    assert_eq!(authority.root().committed_bytes(), before);
    assert_eq!(authority.live_accounts(), 1);
}
#[test]
fn mem_a1_installed_account_is_carried_without_erasing_policy_or_granting_work() {
    let authority = authority(32);
    let manager = QueryContextManager::new_for_test();
    let runtime = NativeFragmentQueryRuntime::new_for_test(manager, authority.clone());
    let first = admission(&runtime, execution(1), Some(8192)).unwrap();
    let memory = first.query_memory().unwrap();
    let account = memory.account().id();
    assert_eq!(memory.account().snapshot().policy_limit_bytes, Some(8192));
    assert_eq!(memory.account().snapshot().granted_bytes, 0);
    let none = admission(&runtime, execution(1), None).unwrap();
    assert_eq!(none.query_memory().unwrap().account().id(), account);
    assert_eq!(
        none.query_memory()
            .unwrap()
            .account()
            .snapshot()
            .policy_limit_bytes,
        Some(8192)
    );
    let other = admission(&runtime, execution(2), None).unwrap();
    let other = other.query_memory().unwrap();
    assert_ne!(other.account().id(), account);
    assert_eq!(other.account().snapshot().policy_limit_bytes, None);
    assert_eq!(other.account().snapshot().granted_bytes, 0);
}
#[test]
fn mem_a1_worker_creation_keeps_actual_typed_metadata_cause_and_legacy_text() {
    let authority = authority(1);
    let manager = QueryContextManager::new_for_test();
    manager
        .ensure_native_context_execution(
            execution_key(execution(1)),
            false,
            Duration::from_secs(30),
            Duration::from_secs(30),
        )
        .unwrap();
    let error = manager
        .ensure_query_memory_account(execution_key(execution(1)), &authority)
        .unwrap_err();
    assert!(matches!(
        error,
        QueryMemoryAccountError::Capacity(CapacityError::MetadataExhausted { .. })
    ));
    assert_eq!(
        manager
            .ensure_query_account(execution_key(execution(1)), &authority)
            .unwrap_err(),
        error.to_string()
    );
    assert!(matches!(
        manager.ensure_query_memory_account(execution_key(execution(2)), &authority),
        Err(QueryMemoryAccountError::MissingContext)
    ));
}
#[test]
fn mem_a1_native_binder_checks_actual_worker_owner_attempt_and_authority() {
    let authority = authority(32);
    let manager = QueryContextManager::new_for_test();
    let runtime = NativeFragmentQueryRuntime::new_for_test(manager.clone(), authority.clone());
    admission(&runtime, execution(1), Some(8192)).unwrap();
    let owner = manager
        .query_memory_account_execution(execution_key(execution(1)))
        .unwrap();
    assert!(matches!(
        bind_query_memory(execution(2), authority.clone(), owner.clone()),
        Err(QueryMemoryBindingError::OwnerAttemptMismatch)
    ));
    assert!(matches!(
        bind_query_memory(execution(1), self::authority(32), owner.clone()),
        Err(QueryMemoryBindingError::Authority(CapacityError::Invalid {
            detail: "account belongs to another authority"
        }))
    ));
    assert_eq!(
        bind_query_memory(execution(1), authority, owner)
            .unwrap()
            .execution(),
        execution(1)
    );
}

#[test]
fn mem_a1_v2_none_then_some_then_none_keeps_the_real_context_owner_and_policy() {
    let authority = authority(32);
    let manager = QueryContextManager::new_for_test();
    let runtime = NativeFragmentQueryRuntime::new_for_test(manager, authority);
    let first = admission(&runtime, execution(1), None).unwrap();
    let id = first.query_memory().unwrap().account().id();
    let limited = admission(&runtime, execution(1), Some(8192)).unwrap();
    assert_eq!(limited.query_memory().unwrap().account().id(), id);
    assert_eq!(
        limited
            .query_memory()
            .unwrap()
            .account()
            .snapshot()
            .policy_limit_bytes,
        Some(8192)
    );
    let again = admission(&runtime, execution(1), None).unwrap();
    assert_eq!(again.query_memory().unwrap().account().id(), id);
    assert_eq!(
        again
            .query_memory()
            .unwrap()
            .account()
            .snapshot()
            .policy_limit_bytes,
        Some(8192)
    );
    assert_eq!(
        again
            .query_memory()
            .unwrap()
            .account()
            .snapshot()
            .granted_bytes,
        0
    );
}
#[test]
fn mem_a1_v2_context_removal_releases_only_after_the_last_actual_binding_drop() {
    let authority = authority(32);
    let manager = QueryContextManager::new_for_test();
    let runtime = NativeFragmentQueryRuntime::new_for_test(manager, authority.clone());
    let before = authority.root().committed_bytes();
    let resources = admission(&runtime, execution(1), None).unwrap();
    let held = resources.query_memory().unwrap().clone();
    drop(resources);
    assert!(runtime.retire_idle_execution(execution(1)));
    assert_eq!(authority.live_accounts(), 2);
    assert_eq!(held.account().snapshot().granted_bytes, 0);
    assert!(!held.account().is_retired());
    drop(held);
    assert_eq!(authority.live_accounts(), 1);
    assert_eq!(authority.root().committed_bytes(), before);
}
#[test]
fn mem_a1_v2_context_removal_is_not_funded_domain_teardown_evidence() {
    let authority = authority(32);
    let manager = QueryContextManager::new_for_test();
    let runtime = NativeFragmentQueryRuntime::new_for_test(manager, authority.clone());
    let resources = admission(&runtime, execution(1), None).unwrap();
    let domain = resources
        .query_memory()
        .unwrap()
        .account()
        .create_domain(1024)
        .unwrap();
    drop(resources);
    assert!(runtime.retire_idle_execution(execution(1)));
    assert!(!domain.lane().affiliation().is_retired());
    assert_eq!(domain.snapshot().authorized, 1024);
    // This is an inactive no-payload domain; ending it is genuine local exit.
    // It does not fabricate Work tasks/operators/I/O teardown evidence.
    domain.stop_producing().unwrap();
    drop(domain);
    assert!(authority.maintain(64).complete);
    assert_eq!(authority.live_accounts(), 1);
}

#[test]
fn native_fragment_query_early_type_binding_and_later_admission_consume_same_account() {
    let authority = authority(32);
    let manager = QueryContextManager::new_for_test();
    let runtime =
        NativeFragmentQueryRuntime::new_for_test(Arc::clone(&manager), Arc::clone(&authority));
    let execution = execution(37);
    let first = UniqueId::new(37, 1);
    let second = UniqueId::new(37, 2);
    let lease = runtime
        .register_fragment_execution(
            execution,
            first,
            Duration::from_secs(30),
            Duration::from_secs(30),
        )
        .unwrap();
    let early = runtime
        .prepare_query_memory_typed(
            execution,
            Duration::from_secs(30),
            Duration::from_secs(30),
            Some(1 << 20),
        )
        .unwrap();
    let account = early.binding().account().id();
    let other_lease = runtime
        .register_fragment_execution(
            execution,
            second,
            Duration::from_secs(30),
            Duration::from_secs(30),
        )
        .unwrap();
    let other = runtime
        .prepare_query_memory_typed(
            execution,
            Duration::from_secs(30),
            Duration::from_secs(30),
            None,
        )
        .unwrap();
    assert_eq!(other.binding().account().id(), account);
    assert_eq!(
        other.binding().account().snapshot().policy_limit_bytes,
        Some(1 << 20)
    );
    assert!(Arc::ptr_eq(other.binding().authority(), &authority));
    drop(other);
    drop(other_lease);
    assert_eq!(
        manager
            .native_execution_resource_snapshot()
            .active_fragments,
        1
    );
    assert_eq!(
        manager
            .query_memory_account_execution(execution_key(execution))
            .unwrap()
            .account()
            .id(),
        account
    );
    let admission = runtime.prepare_admission_with_memory(first, early, None);
    assert_eq!(admission.query_memory().unwrap().account().id(), account);
    assert_eq!(
        admission
            .query_memory()
            .unwrap()
            .account()
            .snapshot()
            .granted_bytes,
        0
    );
    drop(admission);
    drop(lease);
    assert_eq!(
        manager
            .native_execution_resource_snapshot()
            .active_fragments,
        0
    );
}

#[test]
fn native_fragment_query_refused_early_type_memory_rolls_back_only_its_original_lease() {
    let authority = authority(1);
    let manager = QueryContextManager::new_for_test();
    let runtime = NativeFragmentQueryRuntime::new_for_test(Arc::clone(&manager), authority);
    let execution = execution(38);
    let lease = runtime
        .register_fragment_execution(
            execution,
            UniqueId::new(38, 1),
            Duration::from_secs(30),
            Duration::from_secs(30),
        )
        .unwrap();
    assert!(matches!(
        runtime.prepare_query_memory_typed(
            execution,
            Duration::from_secs(30),
            Duration::from_secs(30),
            None
        ),
        Err(NativeFragmentAdmissionError::MemoryAccount(
            QueryMemoryAccountError::Capacity(CapacityError::MetadataExhausted { .. })
        ))
    ));
    drop(lease);
    let state = manager.native_execution_resource_snapshot();
    assert_eq!(state.active_fragments, 0);
    assert_eq!(state.active_contexts, 0);
}
