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
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef};
use novarocks_types::QueryId;
use novarocks_types::identity::{AttemptId, BackendProcessId, StageId, TaskId};
fn authority() -> Arc<MemoryAuthority> {
    let mut cfg = AuthorityConfig::new(1 << 30, 1 << 29, 1 << 29);
    cfg.max_accounts = 32;
    cfg.max_active_owners = 32;
    cfg.metadata_budget_bytes = 1 << 20;
    Arc::new(MemoryAuthority::new(cfg).unwrap())
}
fn execution(attempt: u64) -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(97301, 97302), AttemptId::new(attempt).unwrap()).unwrap()
}
fn identity(attempt: u64) -> TaskIdentity {
    TaskIdentity::new(
        execution(attempt),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}
#[test]
fn mem_a1_task_binding_preserves_full_attempt_and_exact_authority_without_grant() {
    let authority = authority();
    let other = self::authority();
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let before = authority.root().committed_bytes();
    let binding =
        QueryMemoryBinding::try_new(execution(1), authority.clone(), account.clone()).unwrap();
    assert_eq!(binding.validate_task(identity(1), &authority), Ok(()));
    assert_eq!(
        binding.validate_task(identity(2), &authority),
        Err(QueryMemoryBindingError::TaskAttemptMismatch)
    );
    assert_eq!(
        binding.validate_task(identity(1), &other),
        Err(QueryMemoryBindingError::Authority(CapacityError::Invalid {
            detail: "account belongs to another authority"
        }))
    );
    assert!(matches!(
        QueryMemoryBinding::try_new(execution(1), other, account.clone()),
        Err(QueryMemoryBindingError::Authority(
            CapacityError::Invalid { .. }
        ))
    ));
    assert_eq!(account.snapshot().granted_bytes, 0);
    assert_eq!(authority.root().committed_bytes(), before);
}
#[test]
fn mem_a1_driver_bindings_share_the_installed_account_and_never_mint_domains() {
    use crate::exec::pipeline::driver::PipelineDriverBindings;
    use crate::runtime::task::io::NoopTaskEventSink;
    let authority = authority();
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let binding =
        QueryMemoryBinding::try_new(execution(1), authority.clone(), account.clone()).unwrap();
    let before = authority.root().committed_bytes();
    let first = PipelineDriverBindings::new(Arc::new(NoopTaskEventSink))
        .with_query_memory(Some(binding.clone()));
    let second =
        PipelineDriverBindings::new(Arc::new(NoopTaskEventSink)).with_query_memory(Some(binding));
    // Constructing bindings retains account capabilities only, not grants.
    drop(first);
    drop(second);
    assert_eq!(authority.root().committed_bytes(), before);
    assert_eq!(account.snapshot().granted_bytes, 0);
}

#[test]
fn mem_a1_actual_runtime_builder_and_driver_transport_validate_identity_before_use() {
    use crate::exec::pipeline::driver::{PipelineDriver, PipelineDriverBindings};
    use crate::runtime::execution_runtime::test_execution_runtime;
    use crate::runtime::task::io::NoopTaskEventSink;
    use crate::runtime::task::runtime_state::{RuntimeStateInputs, build_runtime_state};
    let runtime = test_execution_runtime();
    let account = runtime
        .memory_authority()
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let binding = QueryMemoryBinding::try_new(
        execution(1),
        runtime.memory_authority().clone(),
        account.clone(),
    )
    .unwrap();
    let inputs = |task: Option<TaskIdentity>, query: Option<QueryId>| RuntimeStateInputs {
        query_options: None,
        query_id: query,
        task_key: None,
        backend_num: None,
        mem_tracker: None,
        runtime_filter_session: None,
        execution_runtime: Some(runtime.clone()),
        query_memory: Some(binding.clone()),
        task_identity: task,
        task_control: task.map(crate::runtime::task_control::test_support::control_for),
        #[cfg(novarocks_dev_controls)]
        exchange_test_observer: None,
    };
    assert_eq!(
        build_runtime_state(inputs(Some(identity(2)), Some(execution(1).query_id())))
            .unwrap_err()
            .to_string()
            .as_str(),
        "query memory task attempt mismatch"
    );
    assert_eq!(
        build_runtime_state(inputs(None, Some(execution(1).query_id())))
            .unwrap_err()
            .to_string()
            .as_str(),
        "query memory binding requires task identity"
    );
    assert_eq!(
        build_runtime_state(inputs(Some(identity(1)), Some(QueryId::new(1, 2))))
            .unwrap_err()
            .to_string()
            .as_str(),
        "query memory invocation query identity mismatch"
    );
    let state =
        build_runtime_state(inputs(Some(identity(1)), Some(execution(1).query_id()))).unwrap();
    assert_eq!(state.query_memory().unwrap().account().id(), account.id());
    assert_eq!(
        state
            .as_ref()
            .clone()
            .query_memory()
            .unwrap()
            .account()
            .id(),
        account.id()
    );
    let bindings = PipelineDriverBindings::new(Arc::new(NoopTaskEventSink))
        .with_query_memory(state.query_memory().cloned());
    assert_eq!(
        bindings.query_memory().unwrap().account().id(),
        account.id()
    );
    let driver =
        PipelineDriver::from_prepared(1, vec![], None, vec![], state.clone(), None, bindings);
    assert_eq!(driver.query_memory().unwrap().account().id(), account.id());
    let other = PipelineDriver::new(2, vec![], None, vec![], state, None);
    assert_eq!(other.query_memory().unwrap().account().id(), account.id());
    assert_eq!(account.snapshot().granted_bytes, 0);
}
