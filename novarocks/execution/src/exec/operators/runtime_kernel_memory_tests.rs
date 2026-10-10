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

use super::RuntimeKernelControl;
use super::runtime_kernel_memory::*;
use crate::runtime::execution_runtime::test_execution_runtime;
use crate::runtime::fragment::runtime_state::{RuntimeStateInputs, build_runtime_state};
use crate::runtime::query_memory::QueryMemoryBinding;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_execution_contract::TaskIdentity;
use novarocks_memory::attribution::scope::{AmbientEntryObservation, AmbientExitObservation};
use novarocks_memory::attribution::{AttributingAllocator, binding};
use novarocks_memory::lane::RecordRef;
use novarocks_memory::{AccountKind, AuthorityConfig, CapacityError, ExternalRef, MemoryAuthority};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, BackendProcessId, QueryExecutionId, StageId, TaskId},
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    ptr::NonNull,
    sync::Arc,
};

fn execution() -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(80031, 80032), AttemptId::new(1).unwrap()).unwrap()
}
fn authority(bytes: u64) -> Arc<MemoryAuthority> {
    let mut cfg = AuthorityConfig::new(bytes * 2, bytes, bytes);
    cfg.max_accounts = 32;
    cfg.max_active_owners = 4;
    cfg.metadata_budget_bytes = 16 * 1024;
    cfg.top_up = novarocks_memory::TopUpPolicy::uniform(1024);
    Arc::new(MemoryAuthority::new(cfg).unwrap())
}
fn bound(authority: Arc<MemoryAuthority>) -> QueryMemoryBinding {
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    QueryMemoryBinding::try_new(execution(), authority, account).unwrap()
}
fn request_for_block() -> KernelMemoryRequest {
    // Exactly the existing wrapped allocator's 600-byte Layout plus its
    // actual eight-byte tail; not an Arrow operation or a schema envelope.
    KernelMemoryRequest {
        workset_bytes: 608,
        stock_bytes: 608,
        threshold_bytes: 0,
        maintenance_budget: 128,
    }
}
fn ready(binding: &QueryMemoryBinding) -> ReadyKernelMemory {
    match request(Some(binding), request_for_block()) {
        KernelMemoryAdmission::Granted(ready) => ready,
        other => panic!("real explicit test block request: {other:?}"),
    }
}
fn layout() -> Layout {
    Layout::from_size_align(600, 8).unwrap()
}
fn allocate(allocator: &AttributingAllocator<System>) -> NonNull<u8> {
    // SAFETY: the real local wrapped allocator receives a valid nonzero Layout.
    NonNull::new(unsafe { allocator.alloc(layout()) }).unwrap()
}
fn release(allocator: &AttributingAllocator<System>, block: NonNull<u8>) {
    // SAFETY: release exactly this allocator's block once with its original Layout.
    unsafe { allocator.dealloc(block.as_ptr(), layout()) };
}

#[test]
fn by_runtime_memory_ready_actual_task_state_installs_exact_account_without_grant() {
    let runtime = test_execution_runtime();
    let binding = bound(runtime.memory_authority().clone());
    let task = TaskIdentity::new(
        execution(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    let state = build_runtime_state(RuntimeStateInputs {
        query_options: None,
        query_id: Some(execution().query_id()),
        fragment_instance_id: None,
        backend_num: None,
        mem_tracker: None,
        runtime_filter_session: None,
        execution_runtime: Some(runtime),
        query_memory: Some(binding.clone()),
        task_identity: Some(task),
    })
    .unwrap();
    let before = binding.authority().root().committed_bytes();
    let mut control = RuntimeKernelControl::new(state.error_state());
    control.bind_runtime_memory(&state);
    let installed = control.query_memory().unwrap();
    assert_eq!(installed.execution(), binding.execution());
    assert_eq!(installed.account().id(), binding.account().id());
    assert!(Arc::ptr_eq(installed.authority(), binding.authority()));
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
    assert_eq!(binding.authority().root().committed_bytes(), before);
    control.bind_runtime_memory(&RuntimeState::default());
    assert!(control.query_memory().is_none());
    assert!(matches!(
        control.request_kernel_memory(request_for_block()),
        KernelMemoryAdmission::MissingQueryMemory
    ));
}
#[test]
fn by_runtime_memory_ready_real_grant_precedes_block_and_last_free_retains_origin() {
    let binding = bound(authority(65536));
    let ready = ready(&binding);
    assert_eq!(
        ready.domain().lane().affiliation().id(),
        binding.account().id()
    );
    let allocator = AttributingAllocator::new(System);
    let mut journal = KernelMemoryJournal::default();
    let block = ready
        .run(&mut journal, || {
            let block = allocate(&allocator);
            // SAFETY: the real wrapped allocator initialized this eight-byte tail.
            assert_eq!(
                unsafe { RecordRef::read(block.as_ptr().add(600)) },
                ready.domain().lane().reference()
            );
            block
        })
        .unwrap();
    assert_eq!(journal.observation.entry(), AmbientEntryObservation::Bound);
    assert_eq!(journal.observation.exit(), AmbientExitObservation::Restored);
    let receipt = journal.settlement.as_ref().unwrap();
    assert_eq!(receipt.accepted_live, 608);
    assert_eq!(receipt.debt, 0);
    assert_eq!(receipt.next_step, Ok(()));
    assert_eq!(journal.stopped, Some(Ok(())));
    assert_eq!(ready.domain().snapshot().live, 608);
    assert_eq!(binding::pending_bytes(), 0);
    release(&allocator, block);
    assert_eq!(ready.domain().snapshot().live, 0);
    ready.domain().settle();
}
#[test]
fn by_runtime_memory_ready_original_data_is_primary_and_no_fallible_footer() {
    let binding = bound(authority(65536));
    let ready = ready(&binding);
    let original = "original whole invocation error".repeat(100);
    let pointer = original.as_ptr();
    let calls = Cell::new(0);
    let mut journal = KernelMemoryJournal::default();
    let result = ready
        .run(&mut journal, || {
            calls.set(calls.get() + 1);
            Err::<(), _>(original)
        })
        .unwrap();
    let error = result.unwrap_err();
    assert_eq!(error.as_ptr(), pointer);
    assert_eq!(error, "original whole invocation error".repeat(100));
    assert_eq!(calls.get(), 1);
    assert!(journal.settlement.is_some());
    assert_eq!(journal.stopped, Some(Ok(())));
    // The Ready producer was stopped, so a second body cannot run.
    assert!(matches!(
        ready.run(&mut journal, || calls.set(99)),
        Err(novarocks_memory::CapacityError::Closed { .. })
    ));
    assert_eq!(calls.get(), 1);
}
#[test]
fn by_runtime_memory_ready_body_panic_settles_once_without_reclassifying_payload() {
    let binding = bound(authority(65536));
    let ready = ready(&binding);
    let mut journal = KernelMemoryJournal::default();
    let calls = Cell::new(0);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = ready.run(&mut journal, || {
            calls.set(calls.get() + 1);
            panic!("original stage panic");
        });
    }))
    .unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"original stage panic"));
    assert_eq!(calls.get(), 1);
    assert_eq!(journal.observation.exit(), AmbientExitObservation::Restored);
    assert!(journal.settlement.is_some());
    assert_eq!(journal.stopped, Some(Ok(())));
}
#[test]
fn by_runtime_memory_ready_pending_and_stock_refusal_keep_actual_nominal_receipts() {
    let binding = bound(authority(65536));
    // Another real query owns unused rights. The requested amount fits the
    // target query but needs the original bounded shared-capacity sweep.
    let other = bound(binding.authority().clone());
    let occupied = other.account().create_domain(40000).unwrap();
    let pending = request(
        Some(&binding),
        KernelMemoryRequest {
            workset_bytes: 40000,
            stock_bytes: 40000,
            threshold_bytes: 0,
            maintenance_budget: 0,
        },
    );
    match pending {
        KernelMemoryAdmission::SettlementPending(receipt) => assert!(!receipt.complete),
        other => panic!("zero-budget original bounded sweep must remain Pending: {other:?}"),
    }
    let refused = request(
        Some(&binding),
        KernelMemoryRequest {
            stock_bytes: 609,
            ..request_for_block()
        },
    );
    assert!(matches!(
        refused,
        KernelMemoryAdmission::Refused(CapacityError::Invalid {
            detail: "kernel stock exceeds its explicit source workset"
        })
    ));
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
    occupied.stop_producing().unwrap();
}
#[test]
fn by_runtime_memory_ready_abandoned_grant_stops_its_actual_producer() {
    let binding = bound(authority(65536));
    let ready = ready(&binding);
    let domain = ready.domain().clone();
    assert!(domain.snapshot().authorized >= 608);
    drop(ready);
    assert!(domain.snapshot().sealed);
    assert_eq!(domain.snapshot().authorized, 0);
    assert_eq!(domain.snapshot().live, 0);
}

#[test]
fn by_runtime_memory_ready_debt_and_policy_refusal_remain_secondary_to_original_data() {
    let binding = bound(authority(65536));
    let ready = ready(&binding);
    let allocator = AttributingAllocator::new(System);
    let actual = Layout::from_size_align(1200, 8).unwrap();
    let mut journal = KernelMemoryJournal::default();
    let original = "original Data before settlement";
    let result = ready
        .run(&mut journal, || {
            // Fault injection: this real block exceeds the admitted test envelope.
            // It proves honest debt/next-step reporting, not a valid Arrow invoice.
            // SAFETY: the real allocator receives this valid Layout; the exact
            // successful block is released once below with the same Layout.
            let block = NonNull::new(unsafe { allocator.alloc(actual) }).unwrap();
            binding
                .account()
                .install_policy(0, novarocks_memory::LimitDimension::Work);
            Err::<(), _>((original, block))
        })
        .unwrap();
    let (message, block) = result.unwrap_err();
    assert_eq!(message, original);
    assert!(std::ptr::eq(message.as_ptr(), original.as_ptr()));
    let receipt = journal.settlement.as_ref().unwrap();
    assert_eq!(receipt.accepted_live, 1208);
    assert_eq!(receipt.debt, 600);
    match &receipt.next_step {
        Err(CapacityError::QueryLimit(refusal)) => {
            assert_eq!(refusal.constraint_account, binding.account().id());
            assert_eq!(refusal.limit, 0);
            assert_eq!(
                refusal.dimension,
                Some(novarocks_memory::LimitDimension::Work)
            );
        }
        other => panic!("preserve exact policy cause: {other:?}"),
    }
    assert_eq!(journal.stopped, Some(Ok(())));
    // SAFETY: the exact same real block/Layout is freed once after observation.
    unsafe { allocator.dealloc(block.as_ptr(), actual) };
    assert_eq!(ready.domain().snapshot().live, 0);
}

#[path = "runtime_kernel_memory_policy_tests.rs"]
mod policy_tests;
