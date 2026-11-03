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

use crate::runtime::execution_runtime::test_execution_runtime;
use crate::runtime::query_memory::QueryMemoryBinding;
use crate::runtime::task::runtime_state::{RuntimeStateInputs, build_runtime_state};
use novarocks_execution_contract::TaskIdentity;
use novarocks_memory::{AccountKind, ExternalRef};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, BackendProcessId, QueryExecutionId, StageId, TaskId},
};

#[test]
fn by_runtime_memory_ready_actual_compiled_window_operator_borrows_prepared_task_capability() {
    let runtime = test_execution_runtime();
    let execution =
        QueryExecutionId::new(QueryId::new(80051, 80052), AttemptId::new(1).unwrap()).unwrap();
    let account = runtime
        .memory_authority()
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let binding = QueryMemoryBinding::try_new(
        execution,
        runtime.memory_authority().clone(),
        account.clone(),
    )
    .unwrap();
    let task = TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    let state = build_runtime_state(RuntimeStateInputs {
        query_options: None,
        query_id: Some(execution.query_id()),
        task_key: None,
        backend_num: None,
        mem_tracker: None,
        runtime_filter_session: None,
        execution_runtime: Some(runtime),
        query_memory: Some(binding.clone()),
        task_identity: Some(task),
        task_control: Some(crate::runtime::task_control::test_support::control_for(
            task,
        )),
        #[cfg(novarocks_dev_controls)]
        exchange_test_observer: None,
    })
    .unwrap();
    let catalog = installed_catalog();
    let shape = Shape {
        calls: vec![Call::window("row_number", &[])],
        ..Shape::default()
    };
    let program = compile(
        package(&[vec![Some(2)], vec![Some(1)]], 1, &shape, &catalog, 1),
        &catalog,
        1,
    );
    let node = program
        .graph()
        .nodes()
        .iter()
        .position(|node| matches!(node.kind(), ProgramNodeKind::Analytic { .. }))
        .unwrap();
    let factory = CompiledWindowProcessorFactory::try_new(
        program,
        ProgramNodeId::new(node),
        state.error_state(),
    )
    .unwrap();
    // This bridge verifies the original runtime memory binding only.
    // Schema preparation is checked separately with its actual creator.
    let before = binding.authority().root().committed_bytes();
    let installed = factory
        .bind_runtime_memory_for_test(&state)
        .unwrap()
        .unwrap();
    assert_eq!(installed.account().id(), account.id());
    assert_eq!(installed.execution(), execution);
    assert!(Arc::ptr_eq(installed.authority(), binding.authority()));
    assert_eq!(account.snapshot().granted_bytes, 0);
    assert_eq!(binding.authority().root().committed_bytes(), before);
    assert!(
        factory
            .bind_runtime_memory_for_test(&RuntimeState::default())
            .unwrap()
            .is_none()
    );
}
