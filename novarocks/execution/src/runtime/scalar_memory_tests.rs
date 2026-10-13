// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

fn bound() -> QueryMemoryBinding {
    let mut cfg = AuthorityConfig::new(131072, 65536, 65536);
    cfg.max_accounts = 32;
    cfg.max_active_owners = 4;
    cfg.metadata_budget_bytes = 16 * 1024;
    cfg.top_up = novarocks_memory::TopUpPolicy::uniform(1024);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    bind(authority)
}
fn bind(authority: Arc<MemoryAuthority>) -> QueryMemoryBinding {
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let execution =
        QueryExecutionId::new(QueryId::new(37, 301), AttemptId::new(1).unwrap()).unwrap();
    QueryMemoryBinding::try_new(execution, authority, account).unwrap()
}
fn control(
    binding: Option<&QueryMemoryBinding>,
    error: Arc<RuntimeErrorState>,
) -> RuntimeKernelControl {
    let state = RuntimeState::default().with_query_memory(binding.cloned());
    let mut control = RuntimeKernelControl::new(error);
    control.bind_runtime_memory(&state);
    control
}
fn request(binding: &QueryMemoryBinding, peak: usize) -> KernelMemoryAdmission {
    kernel_memory::request_complete_operation(Some(binding), peak)
}

fn json_binding() -> QueryMemoryBinding {
    // A distinct explicit test budget covers JSONObject's complete cumulative
    // source requests, rather than the old integer wrapper's tiny fixture.
    let mut cfg = AuthorityConfig::new(256 << 20, 128 << 20, 128 << 20);
    cfg.max_accounts = 32;
    cfg.max_active_owners = 4;
    cfg.metadata_budget_bytes = 16 * 1024;
    cfg.top_up = novarocks_memory::TopUpPolicy::uniform(1024);
    bind(Arc::new(MemoryAuthority::new(cfg).unwrap()))
}
fn actual_json(
    types: &[arrow::datatypes::DataType],
    host: &Arc<dyn AggregateStateAllocator>,
) -> InvocationScalarEvaluationInstance {
    let types: Vec<_> = types
        .iter()
        .cloned()
        .map(|ty| novarocks_type_contract::FunctionValueType::new(ty, true))
        .collect();
    let novarocks_functions::PreparedPureKernel::ScalarInvocation(prepared) =
        actual_prepared("json_object", &types)
    else {
        panic!("actual installed whole-call owner")
    };
    InvocationScalarEvaluationInstance::instantiate_with_allocator(prepared, Some(host.clone()))
        .unwrap()
}
fn json_control(
    binding: Option<&QueryMemoryBinding>,
    label: &str,
) -> (
    RuntimeKernelControl,
    Arc<crate::runtime::mem_tracker::MemTracker>,
) {
    let tracker = crate::runtime::mem_tracker::MemTracker::new_root(label);
    let mut original = control(binding, Arc::default());
    original.bind_mem_tracker(tracker.clone());
    (original, tracker)
}
fn assert_completed_operation(body: &KernelMemoryJournal) {
    assert_eq!(body.observation.entry(), AmbientEntryObservation::Bound);
    assert_eq!(body.observation.exit(), AmbientExitObservation::Restored);
    assert_eq!(body.settlement.as_ref().unwrap().next_step, Ok(()));
    assert_eq!(body.stopped, Some(Ok(())));
}

#[test]
fn runtime_invocation_memory_real_pending_registers_and_releases_before_actual_json_body() {
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::DataType,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    let binding = json_binding();
    let other = bind(binding.authority().clone());
    // Leave only the explicit registry metadata budget; the complete JSON
    // body is larger and must qualify the genuine busy competing writer.
    let occupied_bytes = (128 << 20) - 16384;
    let occupied = other.account().create_domain(occupied_bytes).unwrap();
    let error = Arc::new(RuntimeErrorState::default());
    let mut control = super::contract_tests::control(Some(&binding), error.clone());
    let tracker = crate::runtime::mem_tracker::MemTracker::new_root("actual-pending");
    control.bind_mem_tracker(tracker.clone());
    let host = control.allocator().unwrap();
    let mut instance = actual_json(&[DataType::Utf8], &host);
    let key: ArrayRef = Arc::new(StringArray::from(vec!["key"]));
    let arguments = [EvaluatedArgument::Column(&key)];
    let waited = AtomicBool::new(false);
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let mut journal = RuntimeScalarMemoryJournal::default();
    std::thread::scope(|threads| {
        threads.spawn(|| {
            // The genuine competing writer stays on its own synchronous
            // thread. It cannot yield idle stock while qualification runs.
            let lease = occupied.activate(occupied_bytes, 0).unwrap();
            ready_tx.send(()).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while error.waiting_count() == 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "original Pending must register"
                );
                std::thread::yield_now();
            }
            waited.store(true, Ordering::Relaxed);
            lease.finish();
            occupied.stop_producing().unwrap();
        });
        ready_rx.recv().unwrap();
        let output = RuntimeScalarMemoryScope::new(&control, &mut journal)
            .evaluate_invocation(
                &mut instance,
                Selection::all(1),
                &arguments,
                ScalarInvocationActivation::Activated,
                Some(&host),
                &control,
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "{\"key\": null}"
        );
    });
    assert!(waited.load(Ordering::Relaxed));
    assert_completed_operation(journal.preparation.as_ref().unwrap());
    assert_completed_operation(&journal.body);
    assert!(!instance.is_failed());
    drop(instance);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn runtime_invocation_memory_activated_empty_requires_the_original_host_and_preserves_output() {
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::DataType,
    };
    let binding = json_binding();
    let (original, tracker) = json_control(Some(&binding), "actual-empty");
    let host = original.allocator().unwrap();
    let mut instance = actual_json(&[DataType::Utf8], &host);
    let empty: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    let arguments = [EvaluatedArgument::Column(&empty)];
    let missing = control(Some(&binding), Arc::default());
    let mut journal = RuntimeScalarMemoryJournal::default();
    assert!(matches!(
        RuntimeScalarMemoryScope::new(&missing, &mut journal).evaluate_invocation(
            &mut instance,
            Selection::all(0),
            &arguments,
            ScalarInvocationActivation::Activated,
            Some(&host),
            &missing
        ),
        Err(RuntimeScalarEvaluationFailure::Source(
            ScalarResourceError::Invariant(
                "whole-invocation runtime has no concrete allocation host"
            )
        ))
    ));
    assert!(!instance.is_failed());
    let output = RuntimeScalarMemoryScope::new(&original, &mut journal)
        .evaluate_invocation(
            &mut instance,
            Selection::all(0),
            &arguments,
            ScalarInvocationActivation::Activated,
            Some(&host),
            &original,
        )
        .unwrap();
    assert_eq!(output.values().len(), 0);
    assert_completed_operation(journal.preparation.as_ref().unwrap());
    assert_completed_operation(&journal.body);
    drop(output);
    drop(instance);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn runtime_invocation_memory_actual_json_two_grants_use_same_account_and_original_source() {
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::DataType,
    };
    let binding = json_binding();
    let (control, tracker) = json_control(Some(&binding), "actual\0µ-query");
    let host = control.allocator().unwrap();
    let mut instance = actual_json(&[DataType::Utf8, DataType::Utf8], &host);
    let key: ArrayRef = Arc::new(StringArray::from(vec!["key"]));
    let value: ArrayRef = Arc::new(StringArray::from(vec![
        "unused",
        "[1,2]",
        "unused",
        "{\"b\":2,\"a\":1}",
    ]));
    let arguments = [
        EvaluatedArgument::Scalar(&key),
        EvaluatedArgument::Column(&value),
    ];
    let selection = Selection::try_sparse(4, &[1, 3]).unwrap();
    let facts = instance
        .invocation_resource_profile()
        .unwrap()
        .requests_for(
            &instance,
            selection,
            &arguments,
            ScalarInvocationActivation::Activated,
            &control,
        )
        .unwrap();
    let callback = control
        .invocation_host(Some(&host))
        .unwrap()
        .limit_failure_request_facts()
        .unwrap();
    let mut journal = RuntimeScalarMemoryJournal::default();
    let output = RuntimeScalarMemoryScope::new(&control, &mut journal)
        .evaluate_invocation(
            &mut instance,
            selection,
            &arguments,
            ScalarInvocationActivation::Activated,
            Some(&host),
            &control,
        )
        .unwrap();
    let strings = output
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(strings.value(0), "{\"key\": [1, 2]}");
    assert_eq!(strings.value(1), "{\"key\": {\"a\": 1, \"b\": 2}}");
    assert_eq!(journal.preparation_workset_bytes, Some(520));
    assert_completed_operation(journal.preparation.as_ref().unwrap());
    assert_completed_operation(&journal.body);
    assert_eq!(
        journal.workset_bytes,
        Some(facts.bytes() + callback.requested_bytes + 8 * (facts.requests() + callback.requests))
    );
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
    assert!(!instance.is_failed());
    drop(output);
    drop(instance);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn runtime_invocation_memory_missing_binding_and_policy_refuse_before_instance_call() {
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::DataType,
    };
    for capacity in [false, true] {
        let binding = json_binding();
        if capacity {
            binding
                .account()
                .install_policy(0, novarocks_memory::LimitDimension::Work);
        }
        let (control, tracker) = json_control(capacity.then_some(&binding), "actual-query");
        let host = control.allocator().unwrap();
        let mut instance = actual_json(&[DataType::Utf8], &host);
        let key: ArrayRef = Arc::new(StringArray::from(vec!["key"]));
        let arguments = [EvaluatedArgument::Column(&key)];
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = RuntimeScalarMemoryScope::new(&control, &mut journal).evaluate_invocation(
            &mut instance,
            Selection::all(1),
            &arguments,
            ScalarInvocationActivation::Activated,
            Some(&host),
            &control,
        );
        assert!(
            matches!(
                &result,
                Err(RuntimeScalarEvaluationFailure::Host(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory
                ))
            ) && !capacity
                || matches!(
                    &result,
                    Err(RuntimeScalarEvaluationFailure::Host(
                        RuntimeScalarMemoryRefusal::Capacity(_)
                    ))
                ) && capacity
        );
        assert!(!instance.is_failed());
        assert_eq!(journal.workset_bytes, Some(520));
        assert!(journal.body.settlement.is_none() && journal.preparation.is_none());
        drop(result);
        drop(instance);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn runtime_invocation_memory_cached_birth_host_rebind_is_source_refusal_without_reinstantiation() {
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::DataType,
    };
    let binding = json_binding();
    let (original, tracker) = json_control(Some(&binding), "original");
    let host = original.allocator().unwrap();
    let mut instance = actual_json(&[DataType::Utf8], &host);
    let (rebound, other) = json_control(Some(&binding), "rebound");
    let key: ArrayRef = Arc::new(StringArray::from(vec!["key"]));
    let arguments = [EvaluatedArgument::Column(&key)];
    let mut journal = RuntimeScalarMemoryJournal::default();
    let result = RuntimeScalarMemoryScope::new(&rebound, &mut journal).evaluate_invocation(
        &mut instance,
        Selection::all(1),
        &arguments,
        ScalarInvocationActivation::Activated,
        Some(&host),
        &rebound,
    );
    assert!(matches!(
        result,
        Err(RuntimeScalarEvaluationFailure::Source(
            ScalarResourceError::Invariant(
                "whole-invocation instance allocation host differs from its runtime owner"
            )
        ))
    ));
    assert!(!instance.is_failed());
    assert_completed_operation(journal.preparation.as_ref().unwrap());
    assert!(journal.body.settlement.is_none());
    let output = RuntimeScalarMemoryScope::new(&original, &mut journal)
        .evaluate_invocation(
            &mut instance,
            Selection::all(1),
            &arguments,
            ScalarInvocationActivation::Activated,
            Some(&host),
            &original,
        )
        .unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "{\"key\": null}"
    );
    drop(output);
    drop(instance);
    assert_eq!(tracker.current(), 0);
    assert_eq!(other.current(), 0);
}

#[test]
fn runtime_invocation_memory_original_early_and_validate_only_paths_do_not_qualify_unused_host() {
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::DataType,
    };
    let binding = json_binding();
    let (birth, tracker) = json_control(Some(&binding), "birth");
    let host = birth.allocator().unwrap();
    let caller = control(Some(&binding), Arc::default());
    for activation in [
        ScalarInvocationActivation::Activated,
        ScalarInvocationActivation::ValidateOnly,
    ] {
        let mut instance = actual_json(&[DataType::Utf8], &host);
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = RuntimeScalarMemoryScope::new(&caller, &mut journal).evaluate_invocation(
            &mut instance,
            Selection::all(1),
            &[],
            activation,
            None,
            &caller,
        );
        let Err(RuntimeScalarEvaluationFailure::Kernel(KernelFailure::InvalidProgram(cause))) =
            result
        else {
            panic!("original early contract refusal")
        };
        if activation == ScalarInvocationActivation::ValidateOnly {
            assert_eq!(
                cause.message(),
                "nonempty scalar demand cannot be validation-only"
            );
        } else {
            assert_eq!(
                cause.message(),
                "evaluated scalar arguments differ from the exact call shape"
            );
        }
        assert!(instance.is_failed());
        assert_completed_operation(journal.preparation.as_ref().unwrap());
        assert_completed_operation(&journal.body);
    }
    let mut instance = actual_json(&[DataType::Utf8], &host);
    let empty: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    let arguments = [EvaluatedArgument::Column(&empty)];
    let mut journal = RuntimeScalarMemoryJournal::default();
    let output = RuntimeScalarMemoryScope::new(&caller, &mut journal)
        .evaluate_invocation(
            &mut instance,
            Selection::all(0),
            &arguments,
            ScalarInvocationActivation::ValidateOnly,
            None,
            &caller,
        )
        .unwrap();
    assert_eq!(output.values().len(), 0);
    assert_completed_operation(journal.preparation.as_ref().unwrap());
    assert_completed_operation(&journal.body);
    assert!(!instance.is_failed());
    drop(output);
    drop(instance);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn runtime_invocation_memory_whole_data_latches_and_failed_replay_reads_no_stopped_account() {
    use arrow::{
        array::{ArrayRef, LargeListArray, StringArray},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, Field},
    };
    let binding = json_binding();
    let (control, tracker) = json_control(Some(&binding), "actual-query");
    let host = control.allocator().unwrap();
    let unsupported: ArrayRef = Arc::new(LargeListArray::new(
        Arc::new(Field::new("long\0µ".repeat(100), DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 0])),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        None,
    ));
    let mut instance = actual_json(&[unsupported.data_type().clone()], &host);
    let arguments = [EvaluatedArgument::Scalar(&unsupported)];
    let mut journal = RuntimeScalarMemoryJournal::default();
    let result = RuntimeScalarMemoryScope::new(&control, &mut journal).evaluate_invocation(
        &mut instance,
        Selection::try_sparse(4, &[1, 3]).unwrap(),
        &arguments,
        ScalarInvocationActivation::Activated,
        Some(&host),
        &control,
    );
    let Err(RuntimeScalarEvaluationFailure::Data(data)) = result else {
        panic!("original full Data")
    };
    assert!(
        data.message()
            .starts_with("unsupported scalar type: LargeList")
    );
    assert!(data.message().len() > 512);
    assert_eq!(data.source_row(0), Some(1));
    assert_eq!(data.source_row(1), Some(3));
    assert!(instance.is_failed());
    assert_completed_operation(journal.preparation.as_ref().unwrap());
    assert_completed_operation(&journal.body);
    let error = Arc::new(RuntimeErrorState::default());
    error.set_error("stopped");
    let stopped = super::contract_tests::control(None, error);
    let replay = RuntimeScalarMemoryScope::new(&stopped, &mut journal).evaluate_invocation(
        &mut instance,
        Selection::all(usize::MAX),
        &[],
        ScalarInvocationActivation::Activated,
        None,
        &stopped,
    );
    assert!(matches!(
        replay,
        Err(RuntimeScalarEvaluationFailure::Kernel(
            KernelFailure::InstanceFailed
        ))
    ));
    assert!(
        journal.workset_bytes.is_none()
            && journal.preparation.is_none()
            && journal.body.settlement.is_none()
    );
    drop(data);
    drop(instance);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn runtime_scalar_memory_real_complete_grant_preserves_one_body_and_exact_receipts() {
    let binding = bound();
    let control = control(Some(&binding), Arc::default());
    assert!(Arc::ptr_eq(
        control.query_memory().unwrap().authority(),
        binding.authority()
    ));
    assert_eq!(
        control.query_memory().unwrap().account().id(),
        binding.account().id()
    );
    let mut journal = RuntimeScalarMemoryJournal::default();
    let calls = Cell::new(0);
    // This fixed block-envelope fixture tests host policy, not a shift invoice.
    let output = RuntimeScalarMemoryScope::new(&control, &mut journal)
        .run(
            608,
            || {
                calls.set(calls.get() + 1);
                Ok(73)
            },
            request,
        )
        .unwrap();
    assert_eq!(output, 73);
    assert_eq!(calls.get(), 1);
    assert_eq!(journal.workset_bytes, Some(608));
    assert_eq!(
        journal.body.observation.entry(),
        AmbientEntryObservation::Bound
    );
    assert_eq!(
        journal.body.observation.exit(),
        AmbientExitObservation::Restored
    );
    assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    assert_eq!(journal.body.stopped, Some(Ok(())));
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
}
#[test]
fn runtime_scalar_memory_missing_binding_and_stop_before_request_run_no_body() {
    for stopped in [false, true] {
        let error = Arc::new(RuntimeErrorState::default());
        if stopped {
            error.set_error("original runtime error".repeat(2048));
        }
        let control = control(None, error.clone());
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = RuntimeScalarMemoryScope::new(&control, &mut journal).run(
            608,
            || panic!("refused operation cannot run"),
            |_, _| panic!("absent binding cannot request"),
        );
        if stopped {
            assert_eq!(
                result,
                Err::<(), _>(RuntimeScalarEvaluationFailure::Kernel(
                    KernelFailure::Cancelled
                ))
            );
            assert_eq!(
                error.error().unwrap().to_string(),
                "original runtime error".repeat(2048)
            );
        } else {
            assert_eq!(
                result,
                Err::<(), _>(RuntimeScalarEvaluationFailure::Host(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory
                ))
            );
        }
        assert!(journal.body.settlement.is_none());
    }
}
#[test]
fn runtime_scalar_memory_stop_after_actual_grant_returns_unused_rights() {
    let binding = bound();
    let error = Arc::new(RuntimeErrorState::default());
    let control = control(Some(&binding), error.clone());
    let mut journal = RuntimeScalarMemoryJournal::default();
    let domain = RefCell::new(None);
    let result = RuntimeScalarMemoryScope::new(&control, &mut journal).run(
        608,
        || panic!("stopped grant cannot run"),
        |binding, peak| {
            let admission = request(binding, peak);
            let KernelMemoryAdmission::Granted(ready) = &admission else {
                panic!("real grant required");
            };
            *domain.borrow_mut() = Some(ready.domain().clone());
            error.set_error("stop during qualification");
            admission
        },
    );
    assert_eq!(
        result,
        Err::<(), _>(RuntimeScalarEvaluationFailure::Kernel(
            KernelFailure::Cancelled
        ))
    );
    let domain = domain.into_inner().unwrap();
    assert!(domain.snapshot().sealed);
    assert_eq!(domain.snapshot().authorized, 0);
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
    assert!(journal.body.settlement.is_none());
}
#[test]
fn runtime_scalar_memory_real_pending_wait_requalifies_without_body_replay() {
    let binding = bound();
    let other = bind(binding.authority().clone());
    let occupied = other.account().create_domain(40000).unwrap();
    let control = control(Some(&binding), Arc::default());
    let mut journal = RuntimeScalarMemoryJournal::default();
    let admissions = Cell::new(0);
    let calls = Cell::new(0);
    let output=RuntimeScalarMemoryScope::new(&control,&mut journal).run(40000,
        || { calls.set(calls.get()+1); Ok(91) },|binding,peak| {
            admissions.set(admissions.get()+1);
            if admissions.get()==1 {
                let admission=kernel_memory::request(Some(binding),kernel_memory::KernelMemoryRequest {
                    workset_bytes:peak as u64,stock_bytes:peak as u64,threshold_bytes:0,maintenance_budget:0,
                });
                assert!(matches!(&admission,KernelMemoryAdmission::SettlementPending(receipt) if !receipt.complete));
                assert_eq!(calls.get(),0);
                occupied.stop_producing().unwrap();
                admission
            } else { request(binding,peak) }
        }).unwrap();
    assert_eq!(output, 91);
    assert_eq!(admissions.get(), 2);
    assert_eq!(calls.get(), 1);
    assert!(!journal.pending.as_ref().unwrap().complete);
    assert_eq!(journal.body.stopped, Some(Ok(())));
}
#[test]
fn runtime_scalar_memory_pending_uses_original_registered_stop_notification() {
    for registered in [false, true] {
        let binding = bound();
        let other = bind(binding.authority().clone());
        let occupied = other.account().create_domain(40000).unwrap();
        let error = Arc::new(RuntimeErrorState::default());
        let control = control(Some(&binding), error.clone());
        let mut journal = RuntimeScalarMemoryJournal::default();
        std::thread::scope(|threads| {
            let notifier = registered.then(|| {
                threads.spawn(|| {
                    let deadline = std::time::Instant::now() + Duration::from_secs(5);
                    while error.waiting_count() == 0 {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "actual original wait must register"
                        );
                        std::thread::yield_now();
                    }
                    error.set_error("registered stop");
                })
            });
            let result = RuntimeScalarMemoryScope::new(&control, &mut journal).run(
                40000,
                || panic!("Pending cannot run the body"),
                |binding, peak| {
                    let admission = kernel_memory::request(
                        Some(binding),
                        kernel_memory::KernelMemoryRequest {
                            workset_bytes: peak as u64,
                            stock_bytes: peak as u64,
                            threshold_bytes: 0,
                            maintenance_budget: 0,
                        },
                    );
                    assert!(matches!(
                        &admission,
                        KernelMemoryAdmission::SettlementPending(_)
                    ));
                    if !registered {
                        error.set_error("stop before registration");
                    }
                    admission
                },
            );
            assert_eq!(
                result,
                Err::<(), _>(RuntimeScalarEvaluationFailure::Kernel(
                    KernelFailure::Cancelled
                ))
            );
            if let Some(notifier) = notifier {
                notifier.join().unwrap();
            }
        });
        assert!(journal.pending.is_some());
        assert!(journal.body.settlement.is_none());
        assert_eq!(binding.account().snapshot().granted_bytes, 0);
        occupied.stop_producing().unwrap();
    }
}
#[test]
fn runtime_scalar_memory_body_first_and_success_next_step_remain_nominal_and_disposed() {
    for body_error in [false, true] {
        let binding = bound();
        let control = control(Some(&binding), Arc::default());
        let mut journal = RuntimeScalarMemoryJournal::default();
        let domain = RefCell::new(None::<novarocks_memory::FundingDomain>);
        let calls = Cell::new(0);
        let result = RuntimeScalarMemoryScope::new(&control, &mut journal).run(
            608,
            || {
                calls.set(calls.get() + 1);
                // Real closed-next-step fault after activation, no fabricated receipt.
                domain.borrow().as_ref().unwrap().seal();
                if body_error {
                    Err(KernelFailure::InstanceFailed.into())
                } else {
                    Ok(73)
                }
            },
            |binding, peak| {
                let admission = request(binding, peak);
                let KernelMemoryAdmission::Granted(ready) = &admission else {
                    panic!("real grant required");
                };
                *domain.borrow_mut() = Some(ready.domain().clone());
                admission
            },
        );
        if body_error {
            assert_eq!(
                result,
                Err(RuntimeScalarEvaluationFailure::Kernel(
                    KernelFailure::InstanceFailed
                ))
            );
            assert!(journal.capacity.is_none());
        } else {
            assert!(
                matches!(result,Err(RuntimeScalarEvaluationFailure::Host(RuntimeScalarMemoryRefusal::Capacity(CapacityError::Closed{account}))) if account==binding.account().id())
            );
        }
        assert_eq!(calls.get(), 1);
        assert!(journal.observations.next_step_refused);
        assert_eq!(journal.body.stopped, Some(Ok(())));
        // A new successful leaf clears last facts, never completed disposition.
        journal.begin();
        RuntimeScalarMemoryScope::new(&control, &mut journal)
            .run(608, || Ok(()), request)
            .unwrap();
        assert!(journal.observations.next_step_refused);
        assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    }
}
#[test]
fn runtime_scalar_memory_real_account_policy_refusal_never_runs_body() {
    let binding = bound();
    binding
        .account()
        .install_policy(0, novarocks_memory::LimitDimension::Work);
    let control = control(Some(&binding), Arc::default());
    let mut journal = RuntimeScalarMemoryJournal::default();
    let result = RuntimeScalarMemoryScope::new(&control, &mut journal).run(
        608,
        || -> Result<(), RuntimeScalarEvaluationFailure> {
            panic!("policy refused body cannot run")
        },
        request,
    );
    let Err(RuntimeScalarEvaluationFailure::Host(RuntimeScalarMemoryRefusal::Capacity(
        CapacityError::ImpossibleRequest(refusal),
    ))) = result
    else {
        panic!("positive request exceeds the actual policy");
    };
    assert_eq!(refusal.request_account, binding.account().id());
    assert_eq!(refusal.constraint_account, binding.account().id());
    assert_eq!(refusal.limit, 0);
    assert!(journal.body.settlement.is_none());
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
}

fn actual_shift(name: &str, source: arrow::datatypes::DataType) -> ScalarEvaluationInstance {
    actual_typed_shift(
        name,
        novarocks_type_contract::FunctionValueType::new(source, true),
    )
}
fn actual_typed_shift(
    name: &str,
    source: novarocks_type_contract::FunctionValueType,
) -> ScalarEvaluationInstance {
    actual_scalar(
        name,
        &[
            source,
            novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                true,
            ),
        ],
    )
}
fn actual_scalar(
    name: &str,
    sources: &[novarocks_type_contract::FunctionValueType],
) -> ScalarEvaluationInstance {
    let novarocks_functions::PreparedPureKernel::Scalar(prepared) = actual_prepared(name, sources)
    else {
        panic!("actual ScalarV1 owner");
    };
    ScalarEvaluationInstance::instantiate(prepared).unwrap()
}
fn actual_prepared(
    name: &str,
    sources: &[novarocks_type_contract::FunctionValueType],
) -> novarocks_functions::PreparedPureKernel {
    use novarocks_functions::{
        CallArgumentUses, CallEffectInput, FunctionArgument, FunctionBindingRequest, FunctionKind,
        PureCallPreparation, ScopedExpressionEffects,
    };
    use novarocks_type_contract::{
        CallProofScope, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
        ExpressionEffectContext, ExpressionUseId, SemanticParameters,
    };
    let catalog = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let arguments = sources
        .iter()
        .cloned()
        .map(|ty| FunctionArgument::Value {
            value_type: ty,
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: arguments.len(),
    };
    let compile = &crate::exec::expr::pure_differential::HarnessControl;
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Scalar, request, compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let uses = (0..arguments.len())
        .map(|index| Some(ExpressionUseId::new(index as u32 + 1)))
        .collect::<Vec<_>>();
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(3),
        domain: EvaluationDomainId::new(1),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let prepared = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                context,
                argument_uses: CallArgumentUses::SelectedChannels(&uses),
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Unconditional,
            },
            selected.clone(),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            compile,
        )
        .unwrap();
    prepared.into_prepared()
}
#[test]
fn runtime_scalar_memory_actual_three_sources_use_same_wrapper_arguments_and_empty_path() {
    use arrow::{
        array::{Array, ArrayRef, Int64Array},
        datatypes::DataType,
    };
    for name in [
        "bit_shift_left",
        "bit_shift_right",
        "bit_shift_right_logical",
    ] {
        for rows in [0, 3] {
            let binding = bound();
            let control = control(Some(&binding), Arc::default());
            let mut instance = actual_shift(name, DataType::Int64);
            let values: ArrayRef =
                Arc::new(Int64Array::from(vec![Some(7), None, Some(-1)]).slice(0, rows));
            let counts: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 1]).slice(0, rows));
            let arguments = [
                EvaluatedArgument::Column(&values),
                EvaluatedArgument::Column(&counts),
            ];
            let selection = Selection::all(rows);
            let facts = instance
                .invocation_resource_profile()
                .unwrap()
                .requests_for(selection, &arguments)
                .unwrap();
            let mut journal = RuntimeScalarMemoryJournal::default();
            let selected = RuntimeScalarMemoryScope::new(&control, &mut journal)
                .evaluate(&mut instance, selection, &arguments, &control)
                .unwrap();
            assert_eq!(selected.values().len(), rows);
            assert_eq!(selected.values().data_type(), &DataType::Int64);
            assert_eq!(selected.values().null_count(), usize::from(rows != 0));
            assert_eq!(
                journal.workset_bytes,
                Some(facts.bytes() + 8 * facts.requests())
            );
            assert_eq!(
                journal.body.observation.entry(),
                AmbientEntryObservation::Bound
            );
            assert_eq!(journal.body.stopped, Some(Ok(())));
        }
    }
}
#[test]
fn runtime_scalar_memory_positive_source_errors_never_become_uncovered_or_latch_instance() {
    use arrow::{
        array::{ArrayRef, Int32Array, Int64Array},
        datatypes::DataType,
    };
    let binding = bound();
    let control = control(Some(&binding), Arc::default());
    let mut instance = actual_shift("bit_shift_left", DataType::Int64);
    let values: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let wrong = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&counts),
    ];
    let mut journal = RuntimeScalarMemoryJournal::default();
    let result = RuntimeScalarMemoryScope::new(&control, &mut journal).evaluate(
        &mut instance,
        Selection::all(1),
        &wrong,
        &control,
    );
    assert!(matches!(
        result,
        Err(RuntimeScalarEvaluationFailure::Source(
            ScalarResourceError::Invariant(_)
        ))
    ));
    assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
    let result = RuntimeScalarMemoryScope::new(&control, &mut journal).evaluate(
        &mut instance,
        Selection::all(usize::MAX),
        &[],
        &control,
    );
    assert!(matches!(
        result,
        Err(RuntimeScalarEvaluationFailure::Source(
            ScalarResourceError::Arithmetic
        ))
    ));
    assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
    // Host pre-body refusal leaves the scalar instance uncalled. The enclosing
    // compiled root must own its failed latch when the actual route is installed.
    let values: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let correct = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&counts),
    ];
    RuntimeScalarMemoryScope::new(&control, &mut journal)
        .evaluate(&mut instance, Selection::all(1), &correct, &control)
        .unwrap();
}

fn narrow_array(
    ty: &arrow::datatypes::DataType,
    values: Vec<Option<i64>>,
) -> arrow::array::ArrayRef {
    use arrow::{
        array::{Int8Array, Int16Array, Int32Array},
        datatypes::DataType,
    };
    match ty {
        DataType::Int8 => Arc::new(Int8Array::from(
            values
                .into_iter()
                .map(|v| v.map(|v| i8::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int16 => Arc::new(Int16Array::from(
            values
                .into_iter()
                .map(|v| v.map(|v| i16::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            values
                .into_iter()
                .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        _ => panic!("exact narrow fixture width"),
    }
}
fn narrow_rows(values: &arrow::array::ArrayRef) -> Vec<Option<i64>> {
    use arrow::{
        array::{Int8Array, Int16Array, Int32Array},
        datatypes::DataType,
    };
    match values.data_type() {
        DataType::Int8 => values
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int16 => values
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int32 => values
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        _ => panic!("exact narrow result width"),
    }
}

fn bitwise_array(
    ty: &arrow::datatypes::DataType,
    values: Vec<Option<i64>>,
) -> arrow::array::ArrayRef {
    if ty == &arrow::datatypes::DataType::Int64 {
        Arc::new(arrow::array::Int64Array::from(values))
    } else {
        narrow_array(ty, values)
    }
}
fn bitwise_rows(values: &arrow::array::ArrayRef) -> Vec<Option<i64>> {
    if values.data_type() == &arrow::datatypes::DataType::Int64 {
        values
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .iter()
            .collect()
    } else {
        narrow_rows(values)
    }
}

#[test]
fn runtime_scalar_memory_actual_bitwise_16_sources_keep_exact_arity_mapping_and_nulls() {
    use arrow::datatypes::DataType;
    use novarocks_type_contract::FunctionValueType;
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for (name, arity) in [("bitand", 2), ("bitor", 2), ("bitxor", 2), ("bitnot", 1)] {
            let types = vec![FunctionValueType::new(ty.clone(), true); arity];
            for (rows, sparse, scalar, compact) in [
                (0, false, false, false),
                (3, false, false, false),
                (3, true, false, false),
                (3, false, true, false),
                (3, true, false, true),
            ] {
                let binding = bound();
                let control = control(Some(&binding), Arc::default());
                let mut instance = actual_scalar(name, &types);
                assert_eq!(instance.contract().value_argument_types().len(), arity);
                let selection = if sparse {
                    Selection::try_sparse(3, &[0, 2]).unwrap()
                } else {
                    Selection::all(rows)
                };
                let values = bitwise_array(
                    &ty,
                    if scalar {
                        vec![Some(7)]
                    } else {
                        vec![Some(7), None, Some(-1)]
                            .into_iter()
                            .take(rows)
                            .collect()
                    },
                );
                let right = bitwise_array(
                    &ty,
                    vec![Some(3), Some(2), None]
                        .into_iter()
                        .take(rows)
                        .collect(),
                );
                let compact_left = compact.then(|| {
                    SelectedValues::try_new(
                        selection,
                        &ty,
                        bitwise_array(&ty, vec![Some(7), Some(-1)]),
                        Box::default(),
                    )
                    .unwrap()
                });
                let compact_right = compact.then(|| {
                    SelectedValues::try_new(
                        selection,
                        &ty,
                        bitwise_array(&ty, vec![Some(3), None]),
                        Box::default(),
                    )
                    .unwrap()
                });
                let left_argument = if compact {
                    EvaluatedArgument::SelectedColumn(compact_left.as_ref().unwrap())
                } else if scalar {
                    EvaluatedArgument::Scalar(&values)
                } else {
                    EvaluatedArgument::Column(&values)
                };
                let right_argument = if compact {
                    EvaluatedArgument::SelectedColumn(compact_right.as_ref().unwrap())
                } else {
                    EvaluatedArgument::Column(&right)
                };
                let args = [left_argument, right_argument];
                let arguments = &args[..arity];
                let facts = instance
                    .invocation_resource_profile()
                    .unwrap()
                    .requests_for(selection, arguments)
                    .unwrap();
                assert_eq!(
                    facts.requests(),
                    if rows == 0 {
                        6
                    } else if ty == DataType::Int64 {
                        7
                    } else {
                        12
                    }
                );
                let mut journal = RuntimeScalarMemoryJournal::default();
                let selected = RuntimeScalarMemoryScope::new(&control, &mut journal)
                    .evaluate(&mut instance, selection, arguments, &control)
                    .unwrap();
                let expected = match (name, scalar) {
                    ("bitand", false) => vec![Some(3), None, None],
                    ("bitor", false) => vec![Some(7), None, None],
                    ("bitxor", false) => vec![Some(4), None, None],
                    ("bitnot", false) => vec![Some(-8), None, Some(0)],
                    ("bitand", true) => vec![Some(3), Some(2), None],
                    ("bitor", true) => vec![Some(7), Some(7), None],
                    ("bitxor", true) => vec![Some(4), Some(5), None],
                    ("bitnot", true) => vec![Some(-8); 3],
                    _ => unreachable!(),
                };
                let expected = if sparse {
                    vec![expected[0], expected[2]]
                } else {
                    expected.into_iter().take(rows).collect()
                };
                assert_eq!(selected.values().data_type(), &ty);
                assert_eq!(bitwise_rows(selected.values()), expected);
                assert!(
                    selected
                        .selection()
                        .same_rows_observed(selection, || {
                            Ok::<(), std::convert::Infallible>(())
                        })
                        .unwrap()
                );
                assert!(selected.errors().is_empty());
                assert_eq!(
                    journal.workset_bytes,
                    Some(facts.bytes() + 8 * facts.requests())
                );
                assert_eq!(
                    journal.body.observation.entry(),
                    AmbientEntryObservation::Bound
                );
                assert_eq!(journal.body.stopped, Some(Ok(())));
                assert_eq!(journal.body.settlement.as_ref().unwrap().debt, 0);
            }
        }
    }
}

#[test]
fn runtime_scalar_memory_actual_bitwise_refusals_preserve_uncalled_instance_and_original_errors() {
    use arrow::datatypes::DataType;
    use novarocks_type_contract::FunctionValueType;
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for (name, arity) in [("bitand", 2), ("bitor", 2), ("bitxor", 2), ("bitnot", 1)] {
            let binding = bound();
            let funded = control(Some(&binding), Arc::default());
            let missing = control(None, Arc::default());
            let types = vec![FunctionValueType::new(ty.clone(), true); arity];
            let values = bitwise_array(&ty, vec![Some(7)]);
            let right = bitwise_array(&ty, vec![Some(3)]);
            let args = [
                EvaluatedArgument::Column(&values),
                EvaluatedArgument::Column(&right),
            ];
            let arguments = &args[..arity];
            let mut instance = actual_scalar(name, &types);
            let mut journal = RuntimeScalarMemoryJournal::default();
            assert_eq!(
                RuntimeScalarMemoryScope::new(&missing, &mut journal)
                    .evaluate(&mut instance, Selection::all(1), arguments, &missing)
                    .unwrap_err(),
                RuntimeScalarEvaluationFailure::Host(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory
                )
            );
            assert!(journal.body.settlement.is_none());
            let other_type = if ty == DataType::Int8 {
                DataType::Int16
            } else {
                DataType::Int8
            };
            let other = bitwise_array(&other_type, vec![Some(7)]);
            let mut wrong = args;
            wrong[arity - 1] = EvaluatedArgument::Column(&other);
            assert!(matches!(
                RuntimeScalarMemoryScope::new(&funded, &mut journal).evaluate(
                    &mut instance,
                    Selection::all(1),
                    &wrong[..arity],
                    &funded
                ),
                Err(RuntimeScalarEvaluationFailure::Source(
                    ScalarResourceError::Invariant(_)
                ))
            ));
            assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
            assert_eq!(binding.account().snapshot().granted_bytes, 0);
            assert_eq!(
                RuntimeScalarMemoryScope::new(&funded, &mut journal)
                    .evaluate(&mut instance, Selection::all(usize::MAX), &[], &funded)
                    .unwrap_err(),
                RuntimeScalarEvaluationFailure::Source(ScalarResourceError::Arithmetic)
            );
            assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
            RuntimeScalarMemoryScope::new(&funded, &mut journal)
                .evaluate(&mut instance, Selection::all(1), arguments, &funded)
                .unwrap();
            for wrong_arity in [false, true] {
                let unrelated = [EvaluatedArgument::Column(&other); 2];
                let invalid = if wrong_arity {
                    &unrelated[..if arity == 1 { 2 } else { 1 }]
                } else {
                    arguments
                };
                let selection = Selection::all(if wrong_arity { 1 } else { 2 });
                let expected = actual_scalar(name, &types)
                    .evaluate(selection, invalid, &funded)
                    .unwrap_err();
                let mut malformed = actual_scalar(name, &types);
                assert_eq!(
                    RuntimeScalarMemoryScope::new(&funded, &mut journal)
                        .evaluate(&mut malformed, selection, invalid, &funded)
                        .unwrap_err(),
                    RuntimeScalarEvaluationFailure::Kernel(expected)
                );
                assert!(journal.body.settlement.is_some());
                assert_eq!(
                    RuntimeScalarMemoryScope::new(&funded, &mut journal)
                        .evaluate(&mut malformed, Selection::all(1), arguments, &funded)
                        .unwrap_err(),
                    RuntimeScalarEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
                );
            }
        }
    }
}

#[test]
fn runtime_scalar_memory_actual_nine_narrow_sources_keep_original_mapping_and_successful_null() {
    use arrow::{
        array::{ArrayRef, Int64Array},
        datatypes::DataType,
    };
    for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
        for name in [
            "bit_shift_left",
            "bit_shift_right",
            "bit_shift_right_logical",
        ] {
            for (rows, sparse, scalar) in [
                (0, false, false),
                (3, false, false),
                (3, true, false),
                (3, false, true),
            ] {
                let binding = bound();
                let control = control(Some(&binding), Arc::default());
                let mut instance = actual_shift(name, ty.clone());
                let values = if scalar {
                    narrow_array(&ty, vec![Some(7)])
                } else {
                    narrow_array(&ty, vec![Some(7), None, Some(-1)]).slice(0, rows)
                };
                let counts: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 1]).slice(0, rows));
                let arguments = [
                    if scalar {
                        EvaluatedArgument::Scalar(&values)
                    } else {
                        EvaluatedArgument::Column(&values)
                    },
                    EvaluatedArgument::Column(&counts),
                ];
                let selection = if sparse {
                    Selection::try_sparse(3, &[0, 2]).unwrap()
                } else {
                    Selection::all(rows)
                };
                let facts = instance
                    .invocation_resource_profile()
                    .unwrap()
                    .requests_for(selection, &arguments)
                    .unwrap();
                assert_eq!(facts.requests(), if rows == 0 { 6 } else { 12 });
                let mut journal = RuntimeScalarMemoryJournal::default();
                let selected = RuntimeScalarMemoryScope::new(&control, &mut journal)
                    .evaluate(&mut instance, selection, &arguments, &control)
                    .unwrap();
                let expected = match (name, scalar) {
                    ("bit_shift_left", false) => vec![Some(14), None, Some(-2)],
                    ("bit_shift_right", false) => vec![Some(3), None, Some(-1)],
                    ("bit_shift_right_logical", false) => vec![Some(3), None, None],
                    ("bit_shift_left", true) => vec![Some(14), Some(28), Some(14)],
                    ("bit_shift_right" | "bit_shift_right_logical", true) => {
                        vec![Some(3), Some(1), Some(3)]
                    }
                    _ => unreachable!(),
                };
                let expected = if sparse {
                    vec![expected[0], expected[2]]
                } else {
                    expected.into_iter().take(rows).collect()
                };
                assert_eq!(selected.values().data_type(), &ty);
                assert_eq!(narrow_rows(selected.values()), expected);
                assert!(selected.errors().is_empty());
                assert_eq!(
                    journal.workset_bytes,
                    Some(facts.bytes() + 8 * facts.requests())
                );
                assert_eq!(
                    journal.body.observation.entry(),
                    AmbientEntryObservation::Bound
                );
                assert_eq!(journal.body.stopped, Some(Ok(())));
                assert_eq!(journal.body.settlement.as_ref().unwrap().debt, 0);
            }
        }
    }
}

#[test]
fn runtime_scalar_memory_actual_narrow_refusals_preserve_uncalled_instance_and_original_shape_error()
 {
    use arrow::{
        array::{ArrayRef, Int64Array},
        datatypes::DataType,
    };
    for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
        for name in [
            "bit_shift_left",
            "bit_shift_right",
            "bit_shift_right_logical",
        ] {
            let binding = bound();
            let funded = control(Some(&binding), Arc::default());
            let missing = control(None, Arc::default());
            let values = narrow_array(&ty, vec![Some(7)]);
            let counts: ArrayRef = Arc::new(Int64Array::from(vec![1]));
            let arguments = [
                EvaluatedArgument::Column(&values),
                EvaluatedArgument::Column(&counts),
            ];
            let mut instance = actual_shift(name, ty.clone());
            let mut journal = RuntimeScalarMemoryJournal::default();
            assert_eq!(
                RuntimeScalarMemoryScope::new(&missing, &mut journal)
                    .evaluate(&mut instance, Selection::all(1), &arguments, &missing)
                    .unwrap_err(),
                RuntimeScalarEvaluationFailure::Host(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory
                )
            );
            assert!(journal.body.settlement.is_none());
            let wrong = [EvaluatedArgument::Column(&counts), arguments[1]];
            assert!(matches!(
                RuntimeScalarMemoryScope::new(&funded, &mut journal).evaluate(
                    &mut instance,
                    Selection::all(1),
                    &wrong,
                    &funded
                ),
                Err(RuntimeScalarEvaluationFailure::Source(
                    ScalarResourceError::Invariant(_)
                ))
            ));
            assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
            assert_eq!(binding.account().snapshot().granted_bytes, 0);
            RuntimeScalarMemoryScope::new(&funded, &mut journal)
                .evaluate(&mut instance, Selection::all(1), &arguments, &funded)
                .unwrap();
            let mut original = actual_shift(name, ty.clone());
            let expected = original
                .evaluate(Selection::all(2), &arguments, &funded)
                .unwrap_err();
            let mut malformed = actual_shift(name, ty.clone());
            assert_eq!(
                RuntimeScalarMemoryScope::new(&funded, &mut journal)
                    .evaluate(&mut malformed, Selection::all(2), &arguments, &funded)
                    .unwrap_err(),
                RuntimeScalarEvaluationFailure::Kernel(expected)
            );
            assert!(journal.body.settlement.is_some());
            assert_eq!(
                RuntimeScalarMemoryScope::new(&funded, &mut journal)
                    .evaluate(&mut malformed, Selection::all(1), &arguments, &funded)
                    .unwrap_err(),
                RuntimeScalarEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
            );
        }
    }
}
#[test]
fn runtime_scalar_memory_uncovered_original_source_has_no_host_substitution() {
    use arrow::{
        array::{ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array},
        datatypes::DataType,
    };
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    let control = control(None, Arc::default());
    let source = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let mut instance = actual_typed_shift("bit_shift_left", source);
    assert!(instance.invocation_resource_profile().is_none());
    let mut builder = FixedSizeBinaryBuilder::with_capacity(1, 16);
    builder.append_value(7_i128.to_be_bytes()).unwrap();
    let values: ArrayRef = Arc::new(builder.finish());
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let arguments = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&counts),
    ];
    let mut journal = RuntimeScalarMemoryJournal::default();
    let selected = RuntimeScalarMemoryScope::new(&control, &mut journal)
        .evaluate(&mut instance, Selection::all(1), &arguments, &control)
        .unwrap();
    assert_eq!(
        selected
            .values()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        &14_i128.to_be_bytes()
    );
    assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
}

#[test]
fn runtime_scalar_memory_qualified_shortage_retains_actual_live_block_and_fresh_receipt() {
    use novarocks_memory::attribution::AttributingAllocator;
    use std::alloc::{GlobalAlloc, Layout, System};
    let binding = bound();
    let other = bind(binding.authority().clone());
    let KernelMemoryAdmission::Granted(occupied) = request(&other, 40008) else {
        panic!("real occupied grant");
    };
    let allocator = AttributingAllocator::new(System);
    let layout = Layout::from_size_align(40000, 8).unwrap();
    let mut observation = KernelMemoryJournal::default();
    let block = occupied
        .run(&mut observation, || {
            // SAFETY: valid nonzero Layout; the exact same block is freed below.
            let block = unsafe { allocator.alloc(layout) };
            assert!(!block.is_null());
            block
        })
        .unwrap();
    assert_eq!(
        observation.settlement.as_ref().unwrap().accepted_live,
        40008
    );
    let control = control(Some(&binding), Arc::default());
    let mut journal = RuntimeScalarMemoryJournal::default();
    let result = RuntimeScalarMemoryScope::new(&control, &mut journal).run(
        40000,
        || -> Result<(), RuntimeScalarEvaluationFailure> {
            panic!("qualified shortage cannot run body");
        },
        request,
    );
    let Err(RuntimeScalarEvaluationFailure::Host(RuntimeScalarMemoryRefusal::SharedShortage(
        receipt,
    ))) = result
    else {
        panic!("real completed coverage must preserve shortage");
    };
    assert!(receipt.coverage.complete);
    assert_eq!(receipt.refusal.requested, 40000);
    assert_eq!(receipt.refusal.request_account, binding.account().id());
    assert_eq!(
        receipt.refusal.capacity_revision,
        receipt.coverage.capacity_revision
    );
    assert!(
        binding
            .authority()
            .shortage_is_fresh(&receipt, Duration::from_secs(5))
    );
    assert_eq!(journal.shortage, Some(receipt));
    assert!(journal.body.settlement.is_none());
    // SAFETY: release the exact successfully allocated block with its Layout.
    unsafe {
        allocator.dealloc(block, layout);
    }
    assert_eq!(occupied.domain().snapshot().live, 0);
}
#[test]
fn runtime_scalar_memory_real_debt_stays_secondary_to_original_body_error() {
    use novarocks_memory::attribution::AttributingAllocator;
    use std::alloc::{GlobalAlloc, Layout, System};
    let binding = bound();
    let control = control(Some(&binding), Arc::default());
    let allocator = AttributingAllocator::new(System);
    let layout = Layout::from_size_align(1200, 8).unwrap();
    let block = Cell::new(std::ptr::null_mut());
    let mut journal = RuntimeScalarMemoryJournal::default();
    let result = RuntimeScalarMemoryScope::new(&control, &mut journal).run(
        608,
        || {
            // Deliberately underestimated block fixture: first-cause fault coverage,
            // never a legal shift source invoice or a manufactured allocation fact.
            // SAFETY: valid Layout; the successfully allocated block is freed below.
            block.set(unsafe { allocator.alloc(layout) });
            assert!(!block.get().is_null());
            binding
                .account()
                .install_policy(0, novarocks_memory::LimitDimension::Work);
            Err::<(), _>(RuntimeScalarEvaluationFailure::Kernel(
                KernelFailure::InstanceFailed,
            ))
        },
        request,
    );
    assert_eq!(
        result,
        Err(RuntimeScalarEvaluationFailure::Kernel(
            KernelFailure::InstanceFailed
        ))
    );
    let receipt = journal.body.settlement.as_ref().unwrap();
    assert_eq!(receipt.accepted_live, 1208);
    assert_eq!(receipt.debt, 600);
    assert!(matches!(
        receipt.next_step,
        Err(CapacityError::QueryLimit(_))
    ));
    assert!(journal.capacity.is_none());
    assert!(journal.observations.debt && journal.observations.next_step_refused);
    // SAFETY: free exactly the one successful block with its original Layout.
    unsafe {
        allocator.dealloc(block.get(), layout);
    }
}
#[test]
fn runtime_scalar_memory_presealed_ready_is_activation_refusal_not_entry_fault() {
    let binding = bound();
    let control = control(Some(&binding), Arc::default());
    let mut journal = RuntimeScalarMemoryJournal::default();
    let domain = RefCell::new(None);
    let result = RuntimeScalarMemoryScope::new(&control, &mut journal).run(
        608,
        || -> Result<(), RuntimeScalarEvaluationFailure> {
            panic!("presealed domain cannot activate");
        },
        |binding, peak| {
            let admission = request(binding, peak);
            let KernelMemoryAdmission::Granted(ready) = &admission else {
                panic!("real grant required");
            };
            *domain.borrow_mut() = Some(ready.domain().clone());
            ready.domain().seal();
            admission
        },
    );
    assert!(
        matches!(result,Err(RuntimeScalarEvaluationFailure::Host(RuntimeScalarMemoryRefusal::Capacity(CapacityError::Closed{account}))) if account==binding.account().id())
    );
    assert_eq!(
        journal.body.observation.entry(),
        AmbientEntryObservation::NotAttempted
    );
    assert!(!journal.observations.entry_refused);
    assert!(journal.body.settlement.is_none());
    let domain = domain.into_inner().unwrap();
    assert!(!domain.snapshot().active);
    assert_eq!(domain.snapshot().authorized, 0);
}
#[test]
fn runtime_scalar_memory_original_unwind_settles_and_disposes_without_replacing_panic() {
    let binding = bound();
    let control = control(Some(&binding), Arc::default());
    let mut journal = RuntimeScalarMemoryJournal::default();
    let domain = RefCell::new(None::<novarocks_memory::FundingDomain>);
    let calls = Cell::new(0);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = RuntimeScalarMemoryScope::new(&control, &mut journal).run(
            608,
            || -> Result<(), RuntimeScalarEvaluationFailure> {
                calls.set(calls.get() + 1);
                domain.borrow().as_ref().unwrap().seal();
                panic!("original synchronous body panic");
            },
            |binding, peak| {
                let admission = request(binding, peak);
                let KernelMemoryAdmission::Granted(ready) = &admission else {
                    panic!("real grant required");
                };
                *domain.borrow_mut() = Some(ready.domain().clone());
                admission
            },
        );
    }))
    .unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"original synchronous body panic")
    );
    assert_eq!(calls.get(), 1);
    assert_eq!(
        journal.body.observation.exit(),
        AmbientExitObservation::Restored
    );
    assert_eq!(journal.body.stopped, Some(Ok(())));
    assert!(journal.observations.next_step_refused);
    assert!(!domain.borrow().as_ref().unwrap().snapshot().active);
}
