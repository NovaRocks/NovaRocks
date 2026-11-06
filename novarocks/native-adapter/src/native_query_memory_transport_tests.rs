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
use novarocks_types::{QueryId, identity::AttemptId};

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
    runtime: &NativeQueryContextRuntime,
    context: &Arc<QueryContext>,
    limit: Option<i64>,
) -> Result<NativeTaskAdmissionResources, String> {
    let memory = runtime
        .prepare_query_memory_typed(context, limit)
        .map_err(|error| error.to_string())?;
    Ok(runtime.prepare_admission_with_memory(UniqueId::new(97203, 97204), memory, None))
}
#[test]
fn mem_a1_v2_none_creates_real_account_without_local_policy_or_grant() {
    let authority = authority(32);
    let runtime = NativeQueryContextRuntime::new(authority.clone());
    let before = authority.root().committed_bytes();
    let context = runtime
        .establish_query_resources(execution(1), None)
        .unwrap();
    let resources = admission(&runtime, &context, None).unwrap();
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
    let runtime = NativeQueryContextRuntime::new(authority.clone());
    let before = authority.root().committed_bytes();
    let error = runtime
        .establish_query_resources(execution(1), None)
        .err()
        .expect("root occupies the real account registry");
    assert!(matches!(
        &error,
        NativeTaskAdmissionError::MemoryAccount(QueryMemoryAccountError::Capacity(
            CapacityError::MetadataExhausted { .. }
        ))
    ));
    let full = error.to_string();
    assert!(
        full.starts_with("create query memory account: memory request refused: MetadataExhausted")
    );
    assert_eq!(
        runtime
            .establish_query_resources(execution(1), None)
            .err()
            .unwrap()
            .to_string(),
        full
    );
    assert_eq!(authority.root().committed_bytes(), before);
    assert_eq!(authority.live_accounts(), 1);
}
#[test]
fn mem_a1_installed_account_is_carried_without_erasing_policy_or_granting_work() {
    let authority = authority(32);
    let runtime = NativeQueryContextRuntime::new(authority);
    let context = runtime
        .establish_query_resources(execution(1), Some(8192))
        .unwrap();
    let first = admission(&runtime, &context, Some(8192)).unwrap();
    let memory = first.query_memory().unwrap();
    let account = memory.account().id();
    assert_eq!(memory.account().snapshot().policy_limit_bytes, Some(8192));
    assert_eq!(memory.account().snapshot().granted_bytes, 0);
    let none = admission(&runtime, &context, None).unwrap();
    assert_eq!(none.query_memory().unwrap().account().id(), account);
    assert_eq!(
        none.query_memory()
            .unwrap()
            .account()
            .snapshot()
            .policy_limit_bytes,
        Some(8192)
    );
    let other_context = runtime
        .establish_query_resources(execution(2), None)
        .unwrap();
    let other = admission(&runtime, &other_context, None).unwrap();
    let other = other.query_memory().unwrap();
    assert_ne!(other.account().id(), account);
    assert_eq!(other.account().snapshot().policy_limit_bytes, None);
    assert_eq!(other.account().snapshot().granted_bytes, 0);
}
#[test]
fn mem_a1_owner_creation_keeps_actual_typed_metadata_cause_and_legacy_text() {
    let authority = authority(1);
    let runtime = NativeQueryContextRuntime::new(authority.clone());
    let error = runtime
        .establish_query_resources(execution(1), None)
        .err()
        .unwrap();
    let NativeTaskAdmissionError::MemoryAccount(original) = &error else {
        panic!("the real account owner must preserve its typed refusal")
    };
    assert!(matches!(
        original,
        QueryMemoryAccountError::Capacity(CapacityError::MetadataExhausted { .. })
    ));
    assert_eq!(error.to_string(), original.to_string());
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .is::<QueryMemoryAccountError>()
    );
    assert_eq!(
        authority.live_accounts(),
        1,
        "refused ownership leaves no replacement context or account"
    );
}
#[test]
fn mem_a1_native_binder_checks_actual_owner_attempt_and_authority() {
    let authority = authority(32);
    let runtime = NativeQueryContextRuntime::new(authority.clone());
    let context = runtime
        .establish_query_resources(execution(1), Some(8192))
        .unwrap();
    let owner = QueryMemoryAccount::new(
        context.execution(),
        context.memory_binding().account().clone(),
    );
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
    let runtime = NativeQueryContextRuntime::new(authority(32));
    let context = runtime
        .establish_query_resources(execution(1), None)
        .unwrap();
    let first = admission(&runtime, &context, None).unwrap();
    let id = first.query_memory().unwrap().account().id();
    let limited = admission(&runtime, &context, Some(8192)).unwrap();
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
    let again = admission(&runtime, &context, None).unwrap();
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
fn mem_a1_v2_owner_release_waits_for_actual_task_and_last_binding_drop() {
    let authority = authority(32);
    let runtime = NativeQueryContextRuntime::new(authority.clone());
    let before = authority.root().committed_bytes();
    let context = runtime
        .establish_query_resources(execution(1), None)
        .unwrap();
    let weak = Arc::downgrade(&context);
    let resources = admission(&runtime, &context, None).unwrap();
    let held = resources.query_memory().unwrap().clone();
    drop(context);
    assert_eq!(
        weak.upgrade().unwrap().active_tasks(),
        1,
        "the actual Task guard keeps its context"
    );
    drop(resources);
    assert!(
        weak.upgrade().is_none(),
        "the Task exit releases its original guard"
    );
    assert_eq!(authority.live_accounts(), 2);
    assert_eq!(held.account().snapshot().granted_bytes, 0);
    assert!(!held.account().is_retired());
    drop(held);
    assert_eq!(authority.live_accounts(), 1);
    assert_eq!(authority.root().committed_bytes(), before);
}
#[test]
fn mem_a1_v2_owner_release_is_not_funded_domain_teardown_evidence() {
    let authority = authority(32);
    let runtime = NativeQueryContextRuntime::new(authority.clone());
    let context = runtime
        .establish_query_resources(execution(1), None)
        .unwrap();
    let resources = admission(&runtime, &context, None).unwrap();
    let domain = resources
        .query_memory()
        .unwrap()
        .account()
        .create_domain(1024)
        .unwrap();
    drop(resources);
    drop(context);
    assert!(!domain.lane().affiliation().is_retired());
    assert_eq!(domain.snapshot().authorized, 1024);
    // Only this inactive no-payload domain's actual local exit is observed.
    domain.stop_producing().unwrap();
    drop(domain);
    assert!(authority.maintain(64).complete);
    assert_eq!(authority.live_accounts(), 1);
}
#[test]
fn native_fragment_query_early_type_binding_and_later_admission_consume_same_account() {
    let authority = authority(32);
    let runtime = NativeQueryContextRuntime::new(authority.clone());
    let context = runtime
        .establish_query_resources(execution(37), None)
        .unwrap();
    let tracker = runtime
        .type_construction_tracker(&context, Some(1 << 20))
        .unwrap();
    let early = runtime
        .prepare_query_memory_typed(&context, Some(1 << 20))
        .unwrap();
    let account = early.binding().account().id();
    let other = runtime.prepare_query_memory_typed(&context, None).unwrap();
    assert_eq!(other.binding().account().id(), account);
    assert_eq!(
        other.binding().account().snapshot().policy_limit_bytes,
        Some(1 << 20)
    );
    assert!(Arc::ptr_eq(other.binding().authority(), &authority));
    assert!(Arc::ptr_eq(&tracker, &context.tracker()));
    assert_eq!(context.active_tasks(), 2);
    drop(other);
    assert_eq!(context.active_tasks(), 1);
    assert_eq!(context.memory_binding().account().id(), account);
    let admission = runtime.prepare_admission_with_memory(UniqueId::new(37, 1), early, None);
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
    assert!(Arc::ptr_eq(&admission.query_mem_tracker(), &tracker));
    drop(admission);
    assert_eq!(context.active_tasks(), 0);
}
#[test]
fn native_fragment_query_refused_early_type_memory_does_not_create_a_task_guard() {
    let runtime = NativeQueryContextRuntime::new(authority(32));
    let context = runtime
        .establish_query_resources(execution(38), Some(1024))
        .unwrap();
    assert!(matches!(
        runtime.prepare_query_memory_typed(&context, Some(2048)),
        Err(NativeTaskAdmissionError::Limit(_))
    ));
    assert_eq!(context.active_tasks(), 0);
    assert_eq!(context.tracker().limit(), 1024);
    assert_eq!(
        context
            .memory_binding()
            .account()
            .snapshot()
            .policy_limit_bytes,
        Some(1024)
    );
    let original = runtime.prepare_query_memory_typed(&context, None).unwrap();
    assert_eq!(context.active_tasks(), 1);
    drop(original);
    assert_eq!(context.active_tasks(), 0);
}
