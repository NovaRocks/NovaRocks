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

#![expect(
    clippy::disallowed_methods,
    reason = "Integration tests own and join their threads and runtime tasks; see ADR-0218."
)]

//! Native JSON selected results retain the original task's tracked allocation.
//! The legacy physical cohort has a separate I3 baseline disposition. Its
//! original source predates the release-policy change in 15d6d2bc96.
#[path = "support/shift_runtime_funding.rs"]
mod support;

use arrow::{
    array::{Array, LargeBinaryArray, StringArray},
    datatypes::DataType,
};
use novarocks_execution::runtime::{
    mem_tracker::{process_mem_tracker, query_tracker_label},
    query_memory::QueryMemoryBinding,
    scalar_memory::test_support::RuntimeScalarFrameForTest,
    task::ExecutionFailureCause,
};
use novarocks_functions::{
    DataFailureReason, KernelFailure,
    json_value::{Kind, Node},
};
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TopUpPolicy};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;
use support::{batch_for_arguments, program_for_arguments, runtime_for_names, site, task};

fn binding(query: i64) -> QueryMemoryBinding {
    let bytes = 16u64 << 20;
    let mut cfg = AuthorityConfig::new(bytes, bytes * 3 / 4, bytes / 4);
    cfg.max_accounts = 8;
    cfg.max_active_owners = 16;
    cfg.metadata_budget_bytes = 1 << 20;
    cfg.top_up = TopUpPolicy::uniform(1);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let execution = novarocks_types::identity::QueryExecutionId::new(
        novarocks_types::QueryId::new(37, query),
        novarocks_types::identity::AttemptId::new(1).unwrap(),
    )
    .unwrap();
    QueryMemoryBinding::try_new(execution, authority, account).unwrap()
}

#[test]
fn native_json_task_keeps_original_grant_until_last_backing_alias_on_another_thread() {
    let _ = novarocks_server_application::memory_observation::snapshot();
    let binding = binding(309);
    let runtime = runtime_for_names(&binding, &["json_parse"]);
    let program = program_for_arguments(
        "json_parse",
        &[FunctionValueType::new(DataType::Utf8, true)],
        &["json_parse"],
        &binding,
    );
    let query = binding.execution().query_id();
    let label = query_tracker_label(query.high(), query.low());
    let mut frame = RuntimeScalarFrameForTest::try_new_with_allocation_host(
        program.clone(),
        site(&program),
        runtime.clone(),
        task(&binding),
        Some(binding.clone()),
    )
    .unwrap();
    let tracker = process_mem_tracker()
        .children()
        .into_iter()
        .find(|tracker| tracker.label() == label)
        .expect("the original task created this query tracker");
    let input = batch_for_arguments(
        &program,
        vec![Arc::new(StringArray::from(vec![
            Some("null"),
            None,
            Some("[1,2]"),
        ]))],
    );
    let output = frame.evaluate(&input).unwrap();
    let json = output.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert!(matches!(
        Node::from_root(json.value(0)).unwrap().kind().unwrap(),
        Kind::Null
    ));
    assert!(json.is_null(1));
    assert!(matches!(
        Node::from_root(json.value(2)).unwrap().kind().unwrap(),
        Kind::Array(_)
    ));

    let mut aliases = [
        json.values().clone(),
        json.offsets().inner().inner().clone(),
        json.nulls().unwrap().buffer().clone(),
    ]
    .into_iter();
    let last = aliases.next().unwrap();
    drop(output);
    drop(input);
    drop(frame);
    drop(program);
    drop(runtime);
    let retained = tracker.current();
    assert!(
        retained > 0,
        "the task has exited and backing is still live"
    );

    for alias in aliases {
        drop(alias);
        assert_eq!(tracker.current(), retained);
    }
    let shared = last.clone();
    drop(last);
    assert_eq!(tracker.current(), retained);
    std::thread::spawn(move || drop(shared)).join().unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn native_json_task_publishes_typed_parse_data_and_requires_an_allocation_host() {
    let _ = novarocks_server_application::memory_observation::snapshot();
    let binding = binding(311);
    let runtime = runtime_for_names(&binding, &["json_parse"]);
    let program = program_for_arguments(
        "json_parse",
        &[FunctionValueType::new(DataType::Utf8, true)],
        &["json_parse"],
        &binding,
    );
    let input = batch_for_arguments(
        &program,
        vec![Arc::new(StringArray::from(vec!["1", "{]", "2"]))],
    );
    let mut frame = RuntimeScalarFrameForTest::try_new_with_allocation_host(
        program.clone(),
        site(&program),
        runtime.clone(),
        task(&binding),
        Some(binding.clone()),
    )
    .unwrap();
    let failure = frame.evaluate(&input).unwrap_err();
    let ExecutionFailureCause::RequiredRow(row) = failure.cause() else {
        panic!("expected original typed parse failure: {failure:?}");
    };
    assert_eq!(row.batch_row(), 1);
    assert!(matches!(row.error().reason(), DataFailureReason::Domain(_)));
    assert_eq!(row.error().message(), "Invalid JSON input");

    let mut unbound = RuntimeScalarFrameForTest::try_new(
        program.clone(),
        site(&program),
        runtime,
        task(&binding),
        Some(binding),
    )
    .unwrap();
    assert!(matches!(
        unbound.evaluate(&input).unwrap_err().cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        unbound.evaluate(&input).unwrap_err().cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
    ));
}
