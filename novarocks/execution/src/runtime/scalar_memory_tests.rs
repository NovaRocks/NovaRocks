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
    use novarocks_functions::{
        CallArgumentUses, CallEffectInput, FunctionArgument, FunctionBindingRequest, FunctionKind,
        PreparedPureKernel, PureCallPreparation, ScopedExpressionEffects,
    };
    use novarocks_type_contract::{
        CallProofScope, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
        ExpressionEffectContext, ExpressionUseId, FunctionValueType, SemanticParameters,
    };
    let catalog = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let arguments = [source, arrow::datatypes::DataType::Int64].map(|ty| FunctionArgument::Value {
        value_type: FunctionValueType::new(ty, true),
        constant: None,
    });
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: 2,
    };
    let compile = &crate::exec::expr::pure_differential::HarnessControl;
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Scalar, request, compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let uses = [Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))];
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
    let PreparedPureKernel::Scalar(prepared) = prepared.into_prepared() else {
        panic!("actual ScalarV1 shift owner");
    };
    ScalarEvaluationInstance::instantiate(prepared).unwrap()
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
#[test]
fn runtime_scalar_memory_uncovered_original_source_has_no_host_substitution() {
    use arrow::{
        array::{ArrayRef, Int32Array, Int64Array},
        datatypes::DataType,
    };
    let control = control(None, Arc::default());
    let mut instance = actual_shift("bit_shift_left", DataType::Int32);
    assert!(instance.invocation_resource_profile().is_none());
    let values: ArrayRef = Arc::new(Int32Array::from(vec![7]));
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
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        14
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
