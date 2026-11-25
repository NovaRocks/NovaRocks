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
use novarocks_expression::testkit::call_scalar;
use novarocks_functions::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, ScalarEvaluationInstance,
    ScalarResourceError, SelectedValues, Selection,
};
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

type CallFailure = novarocks_expression::ScalarCallFailure<RuntimeScalarMemoryRefusal>;

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
fn runtime_scalar_memory_admitted_phases_preserve_exact_receipts_on_original_account() {
    let binding = bound();
    let control = control(Some(&binding), Arc::default());
    let mut journal = RuntimeScalarMemoryJournal::default();
    let mut calls = Vec::with_capacity(2);

    {
        let mut host = RuntimeScalarMemoryScope::new(&control, &mut journal);
        host.begin_call();
        for (phase, bytes, requests) in [
            (ScalarCallPhase::Preparation, 512, 1),
            (ScalarCallPhase::Body, 1024, 2),
        ] {
            host.run_admitted(
                phase,
                ScalarCallRequestFacts::new(bytes, requests),
                &mut || {
                    calls.push(phase);
                    assert!(binding.account().snapshot().granted_bytes > 0);
                    Ok(())
                },
            )
            .unwrap();
        }
    }

    assert_eq!(calls, [ScalarCallPhase::Preparation, ScalarCallPhase::Body]);
    assert_eq!(journal.preparation_workset_bytes, Some(520));
    assert_eq!(journal.workset_bytes, Some(1040));
    for operation in [journal.preparation.as_ref().unwrap(), &journal.body] {
        assert_eq!(
            operation.observation.entry(),
            AmbientEntryObservation::Bound
        );
        assert_eq!(
            operation.observation.exit(),
            AmbientExitObservation::Restored
        );
        assert_eq!(operation.settlement.as_ref().unwrap().next_step, Ok(()));
        assert_eq!(operation.stopped, Some(Ok(())));
    }
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
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
            error.set_error(novarocks_functions::KernelFailure::Internal(
                novarocks_functions::KernelDiagnostic::new(
                    &("original runtime error".repeat(2048)),
                ),
            ));
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
                Err::<(), _>(CallFailure::Kernel(KernelFailure::Cancelled))
            );
            assert_eq!(
                error.error().unwrap().detail(),
                novarocks_failure_contract::BoundedDetail::truncating(
                    &"original runtime error".repeat(2048)
                )
                .as_str()
            );
            assert!(
                error.error().unwrap().detail().len()
                    <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES
            );
        } else {
            assert_eq!(
                result,
                Err::<(), _>(CallFailure::Host(
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
            error.set_error(novarocks_functions::KernelFailure::Internal(
                novarocks_functions::KernelDiagnostic::new("stop during qualification"),
            ));
            admission
        },
    );
    assert_eq!(
        result,
        Err::<(), _>(CallFailure::Kernel(KernelFailure::Cancelled))
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
                    error.set_error(novarocks_functions::KernelFailure::Internal(
                        novarocks_functions::KernelDiagnostic::new("registered stop"),
                    ));
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
                        error.set_error(novarocks_functions::KernelFailure::Internal(
                            novarocks_functions::KernelDiagnostic::new("stop before registration"),
                        ));
                    }
                    admission
                },
            );
            assert_eq!(
                result,
                Err::<(), _>(CallFailure::Kernel(KernelFailure::Cancelled))
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
                Err(CallFailure::Kernel(KernelFailure::InstanceFailed))
            );
            assert!(journal.capacity.is_none());
        } else {
            assert!(
                matches!(result,Err(CallFailure::Host(RuntimeScalarMemoryRefusal::Capacity(CapacityError::Closed{account}))) if account==binding.account().id())
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
        || -> Result<(), CallFailure> { panic!("policy refused body cannot run") },
        request,
    );
    let Err(CallFailure::Host(RuntimeScalarMemoryRefusal::Capacity(
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
    use novarocks_type_contract::FunctionValueType;
    actual_scalar(
        name,
        &[
            FunctionValueType::new(source, true),
            FunctionValueType::new(arrow::datatypes::DataType::Int32, true),
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
        CallProofScope, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
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
    let compile = &crate::exec::expr::test_support::HarnessControl;
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Scalar, request, compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let uses = (0..arguments.len())
        .map(|index| Some(ExpressionUseId::new(index as u32 + 1)))
        .collect::<Vec<_>>();
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(3),
        region: EvaluationRegionId::new(1),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let prepared = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                context: context.into(),
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
        array::{Array, ArrayRef, Int32Array, Int64Array},
        datatypes::DataType,
    };
    for name in [
        "bitwise_left_shift",
        "bitwise_right_shift_arithmetic",
        "bitwise_right_shift",
    ] {
        for rows in [0, 3] {
            let binding = bound();
            let control = control(Some(&binding), Arc::default());
            let mut instance = actual_shift(name, DataType::Int64);
            let values: ArrayRef =
                Arc::new(Int64Array::from(vec![Some(7), None, Some(-1)]).slice(0, rows));
            let counts: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 1]).slice(0, rows));
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
            let selected = call_scalar(
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                &mut instance,
                selection,
                &arguments,
                &control,
            )
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
    let mut instance = actual_shift("bitwise_left_shift", DataType::Int64);
    let values: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let counts: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let wrong = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&counts),
    ];
    let mut journal = RuntimeScalarMemoryJournal::default();
    let result = call_scalar(
        &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
        &mut instance,
        Selection::all(1),
        &wrong,
        &control,
    );
    assert!(matches!(
        result,
        Err(CallFailure::Source(ScalarResourceError::Invariant(_)))
    ));
    assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
    let result = call_scalar(
        &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
        &mut instance,
        Selection::all(usize::MAX),
        &[],
        &control,
    );
    assert!(matches!(
        result,
        Err(CallFailure::Source(ScalarResourceError::Arithmetic))
    ));
    assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
    // Host pre-body refusal leaves the scalar instance uncalled. The enclosing
    // compiled root must own its failed latch when the actual route is installed.
    let values: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let correct = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&counts),
    ];
    call_scalar(
        &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
        &mut instance,
        Selection::all(1),
        &correct,
        &control,
    )
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

#[test]
fn runtime_scalar_memory_actual_bitwise_bigint_sources_keep_exact_arity_mapping_and_nulls() {
    use arrow::{
        array::{ArrayRef, Int64Array},
        datatypes::DataType,
    };
    use novarocks_type_contract::FunctionValueType;
    let ty = DataType::Int64;
    for (name, arity) in [
        ("bitwise_and", 2),
        ("bitwise_or", 2),
        ("bitwise_xor", 2),
        ("bitwise_not", 1),
    ] {
        let types = vec![FunctionValueType::new(DataType::Int64, true); arity];
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
            let values: ArrayRef = Arc::new(Int64Array::from(if scalar {
                vec![Some(7)]
            } else {
                vec![Some(7), None, Some(-1)]
                    .into_iter()
                    .take(rows)
                    .collect::<Vec<_>>()
            }));
            let right: ArrayRef = Arc::new(Int64Array::from(
                vec![Some(3), Some(2), None]
                    .into_iter()
                    .take(rows)
                    .collect::<Vec<_>>(),
            ));
            let compact_left = compact.then(|| {
                SelectedValues::try_new(
                    selection,
                    &ty,
                    Arc::new(Int64Array::from(vec![Some(7), Some(-1)])),
                    Box::default(),
                )
                .unwrap()
            });
            let compact_right = compact.then(|| {
                SelectedValues::try_new(
                    selection,
                    &ty,
                    Arc::new(Int64Array::from(vec![Some(3), None])),
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
            assert_eq!(facts.requests(), if rows == 0 { 6 } else { 7 });
            let mut journal = RuntimeScalarMemoryJournal::default();
            let selected = call_scalar(
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                &mut instance,
                selection,
                arguments,
                &control,
            )
            .unwrap();
            let expected = match (name, scalar) {
                ("bitwise_and", false) => vec![Some(3), None, None],
                ("bitwise_or", false) => vec![Some(7), None, None],
                ("bitwise_xor", false) => vec![Some(4), None, None],
                ("bitwise_not", false) => vec![Some(-8), None, Some(0)],
                ("bitwise_and", true) => vec![Some(3), Some(2), None],
                ("bitwise_or", true) => vec![Some(7), Some(7), None],
                ("bitwise_xor", true) => vec![Some(4), Some(5), None],
                ("bitwise_not", true) => vec![Some(-8); 3],
                _ => unreachable!(),
            };
            let expected = if sparse {
                vec![expected[0], expected[2]]
            } else {
                expected.into_iter().take(rows).collect()
            };
            assert_eq!(selected.values().data_type(), &ty);
            assert_eq!(
                selected
                    .values()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(
                selected
                    .selection()
                    .same_rows_observed(selection, || { Ok::<(), std::convert::Infallible>(()) })
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

#[test]
fn runtime_scalar_memory_actual_bitwise_refusals_preserve_uncalled_instance_and_original_errors() {
    use arrow::{
        array::{ArrayRef, Int8Array, Int64Array},
        datatypes::DataType,
    };
    use novarocks_type_contract::FunctionValueType;
    for (name, arity) in [
        ("bitwise_and", 2),
        ("bitwise_or", 2),
        ("bitwise_xor", 2),
        ("bitwise_not", 1),
    ] {
        let binding = bound();
        let funded = control(Some(&binding), Arc::default());
        let missing = control(None, Arc::default());
        let types = vec![FunctionValueType::new(DataType::Int64, true); arity];
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(7)]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(3)]));
        let args = [
            EvaluatedArgument::Column(&values),
            EvaluatedArgument::Column(&right),
        ];
        let arguments = &args[..arity];
        let mut instance = actual_scalar(name, &types);
        let mut journal = RuntimeScalarMemoryJournal::default();
        assert_eq!(
            call_scalar(
                &mut RuntimeScalarMemoryScope::new(&missing, &mut journal),
                &mut instance,
                Selection::all(1),
                arguments,
                &missing
            )
            .unwrap_err(),
            CallFailure::Host(RuntimeScalarMemoryRefusal::MissingQueryMemory)
        );
        assert!(journal.body.settlement.is_none());
        let other: ArrayRef = Arc::new(Int8Array::from(vec![Some(7)]));
        let mut wrong = args;
        wrong[arity - 1] = EvaluatedArgument::Column(&other);
        assert!(matches!(
            call_scalar(
                &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
                &mut instance,
                Selection::all(1),
                &wrong[..arity],
                &funded
            ),
            Err(CallFailure::Source(ScalarResourceError::Invariant(_)))
        ));
        assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
        assert_eq!(binding.account().snapshot().granted_bytes, 0);
        assert_eq!(
            call_scalar(
                &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
                &mut instance,
                Selection::all(usize::MAX),
                &[],
                &funded
            )
            .unwrap_err(),
            CallFailure::Source(ScalarResourceError::Arithmetic)
        );
        assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
        call_scalar(
            &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
            &mut instance,
            Selection::all(1),
            arguments,
            &funded,
        )
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
            let actual = call_scalar(
                &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
                &mut malformed,
                selection,
                invalid,
                &funded,
            )
            .unwrap_err();
            let CallFailure::Kernel(actual) = actual else {
                panic!("the exact outer shape failure must survive the host");
            };
            assert_eq!(
                novarocks_failure_contract::FailureProjection::failure_fact(&actual),
                novarocks_failure_contract::FailureProjection::failure_fact(&expected)
            );
            assert_eq!(actual.to_string(), expected.to_string());
            assert!(journal.body.settlement.is_some());
            assert_eq!(
                call_scalar(
                    &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
                    &mut malformed,
                    Selection::all(1),
                    arguments,
                    &funded
                )
                .unwrap_err(),
                CallFailure::Kernel(KernelFailure::InstanceFailed)
            );
        }
    }
}

#[test]
fn runtime_scalar_memory_actual_nine_narrow_sources_keep_original_mapping_and_successful_null() {
    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::DataType,
    };
    for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
        for name in [
            "bitwise_left_shift",
            "bitwise_right_shift_arithmetic",
            "bitwise_right_shift",
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
                let counts: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 1]).slice(0, rows));
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
                let selected = call_scalar(
                    &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                    &mut instance,
                    selection,
                    &arguments,
                    &control,
                )
                .unwrap();
                let expected = match (name, scalar) {
                    ("bitwise_left_shift", false) => vec![Some(14), None, Some(-2)],
                    ("bitwise_right_shift_arithmetic", false) => vec![Some(3), None, Some(-1)],
                    ("bitwise_right_shift", false) => {
                        let positive = match ty {
                            DataType::Int8 => i64::from(i8::MAX),
                            DataType::Int16 => i64::from(i16::MAX),
                            DataType::Int32 => i64::from(i32::MAX),
                            _ => unreachable!("narrow source"),
                        };
                        vec![Some(3), None, Some(positive)]
                    }
                    ("bitwise_left_shift", true) => vec![Some(14), Some(28), Some(14)],
                    ("bitwise_right_shift_arithmetic" | "bitwise_right_shift", true) => {
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
        array::{ArrayRef, Int32Array, Int64Array},
        datatypes::DataType,
    };
    for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
        for name in [
            "bitwise_left_shift",
            "bitwise_right_shift_arithmetic",
            "bitwise_right_shift",
        ] {
            let binding = bound();
            let funded = control(Some(&binding), Arc::default());
            let missing = control(None, Arc::default());
            let values = narrow_array(&ty, vec![Some(7)]);
            let counts: ArrayRef = Arc::new(Int32Array::from(vec![1]));
            let arguments = [
                EvaluatedArgument::Column(&values),
                EvaluatedArgument::Column(&counts),
            ];
            let mut instance = actual_shift(name, ty.clone());
            let mut journal = RuntimeScalarMemoryJournal::default();
            assert_eq!(
                call_scalar(
                    &mut RuntimeScalarMemoryScope::new(&missing, &mut journal),
                    &mut instance,
                    Selection::all(1),
                    &arguments,
                    &missing
                )
                .unwrap_err(),
                CallFailure::Host(RuntimeScalarMemoryRefusal::MissingQueryMemory)
            );
            assert!(journal.body.settlement.is_none());
            let wrong_width: ArrayRef = Arc::new(Int64Array::from(vec![7]));
            let wrong = [EvaluatedArgument::Column(&wrong_width), arguments[1]];
            assert!(matches!(
                call_scalar(
                    &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
                    &mut instance,
                    Selection::all(1),
                    &wrong,
                    &funded
                ),
                Err(CallFailure::Source(ScalarResourceError::Invariant(_)))
            ));
            assert!(journal.workset_bytes.is_none() && journal.body.settlement.is_none());
            assert_eq!(binding.account().snapshot().granted_bytes, 0);
            call_scalar(
                &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
                &mut instance,
                Selection::all(1),
                &arguments,
                &funded,
            )
            .unwrap();
            let mut original = actual_shift(name, ty.clone());
            let expected = original
                .evaluate(Selection::all(2), &arguments, &funded)
                .unwrap_err();
            let mut malformed = actual_shift(name, ty.clone());
            let actual = call_scalar(
                &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
                &mut malformed,
                Selection::all(2),
                &arguments,
                &funded,
            )
            .unwrap_err();
            let CallFailure::Kernel(actual) = actual else {
                panic!("the original shape author must survive the host");
            };
            assert_eq!(
                novarocks_failure_contract::FailureProjection::failure_fact(&actual),
                novarocks_failure_contract::FailureProjection::failure_fact(&expected)
            );
            assert_eq!(actual.to_string(), expected.to_string());
            assert!(journal.body.settlement.is_some());
            assert_eq!(
                call_scalar(
                    &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
                    &mut malformed,
                    Selection::all(1),
                    &arguments,
                    &funded
                )
                .unwrap_err(),
                CallFailure::Kernel(KernelFailure::InstanceFailed)
            );
        }
    }
}
#[test]
fn runtime_scalar_memory_uncovered_original_source_has_no_host_substitution() {
    use arrow::{
        array::{ArrayRef, Int64Array},
        datatypes::DataType,
    };
    use novarocks_type_contract::FunctionValueType;
    let control = control(None, Arc::default());
    // ABS remains an installed scalar without the call-scoped memory profile.
    let mut instance = actual_scalar("abs", &[FunctionValueType::new(DataType::Int64, true)]);
    assert!(instance.invocation_resource_profile().is_none());
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(-14)]));
    let arguments = [EvaluatedArgument::Column(&values)];
    let mut journal = RuntimeScalarMemoryJournal::default();
    let selected = call_scalar(
        &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
        &mut instance,
        Selection::all(1),
        &arguments,
        &control,
    )
    .unwrap();
    assert_eq!(
        selected
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(14)]
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
        || -> Result<(), CallFailure> {
            panic!("qualified shortage cannot run body");
        },
        request,
    );
    let Err(CallFailure::Host(RuntimeScalarMemoryRefusal::SharedShortage(receipt))) = result else {
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
            Err::<(), _>(CallFailure::Kernel(KernelFailure::InstanceFailed))
        },
        request,
    );
    assert_eq!(
        result,
        Err(CallFailure::Kernel(KernelFailure::InstanceFailed))
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
        || -> Result<(), CallFailure> {
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
        matches!(result,Err(CallFailure::Host(RuntimeScalarMemoryRefusal::Capacity(CapacityError::Closed{account}))) if account==binding.account().id())
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
            || -> Result<(), CallFailure> {
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

#[test]
fn runtime_scalar_memory_original_domain_error_and_late_cancel_keep_real_settlement() {
    use novarocks_memory::attribution::AttributingAllocator;
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    use std::alloc::{GlobalAlloc, Layout, System};
    struct Release<'a>(&'a Cell<*mut u8>, &'a AttributingAllocator<System>, Layout);
    impl Drop for Release<'_> {
        fn drop(&mut self) {
            let pointer = self.0.replace(std::ptr::null_mut());
            if !pointer.is_null() {
                // SAFETY: free exactly the successful block with its original Layout.
                unsafe {
                    self.1.dealloc(pointer, self.2);
                }
            }
        }
    }
    let binding = bound();
    let stopped = Arc::new(RuntimeErrorState::default());
    let control = control(Some(&binding), stopped.clone());
    let allocator = AttributingAllocator::new(System);
    let layout = Layout::from_size_align(1200, 8).unwrap();
    let pointer = Cell::new(std::ptr::null_mut());
    let release = Release(&pointer, &allocator, layout);
    let domain = RefCell::new(None::<novarocks_memory::FundingDomain>);
    let expected = KernelFailure::from_domain(LayoutResourceError::SourceModel);
    let late_cancel: crate::runtime::task::ExecutionFailure = KernelFailure::Cancelled.into();
    let mut journal = RuntimeScalarMemoryJournal::default();
    let actual = RuntimeScalarMemoryScope::new(&control, &mut journal)
        .run(
            608,
            || {
                let mut work = novarocks_functions::EvaluationCheckpoints::new(&control);
                work.step()?;
                // An intentional source-bound fault exercises actual secondary
                // debt; it is not a valid complete-operation funding invoice.
                // SAFETY: Release owns the exact successful block and matching Layout.
                pointer.set(unsafe { allocator.alloc(layout) });
                assert!(!pointer.get().is_null());
                binding
                    .account()
                    .install_policy(0, novarocks_memory::LimitDimension::Work);
                let original: Result<(), KernelFailure> = Err(expected.clone());
                stopped.set_error(late_cancel.clone());
                work.finish_result(original).map_err(CallFailure::Kernel)
            },
            |binding, peak| {
                let admission = request(binding, peak);
                let KernelMemoryAdmission::Granted(ready) = &admission else {
                    panic!("real grant required");
                };
                *domain.borrow_mut() = Some(ready.domain().clone());
                admission
            },
        )
        .unwrap_err();
    // Shared source equality denotes the actual occurrence, not equal facts.
    assert_eq!(actual, CallFailure::Kernel(expected));
    assert!(stopped.error().unwrap().same_occurrence(&late_cancel));
    assert_eq!(
        KernelEvaluationControl::checkpoint(&control, 0),
        Err(KernelFailure::Cancelled)
    );
    assert_eq!(
        journal.body.observation.entry(),
        AmbientEntryObservation::Bound
    );
    assert_eq!(
        journal.body.observation.exit(),
        AmbientExitObservation::Restored
    );
    let receipt = journal.body.settlement.as_ref().unwrap();
    assert_eq!(receipt.accepted_live, 1208);
    assert_eq!(receipt.debt, 600);
    assert!(matches!(
        receipt.next_step,
        Err(CapacityError::QueryLimit(_))
    ));
    assert_eq!(journal.body.stopped, Some(Ok(())));
    assert!(journal.capacity.is_none());
    assert!(journal.observations.debt && journal.observations.next_step_refused);
    let domain = domain.into_inner().unwrap();
    assert!(!domain.snapshot().active);
    assert!(domain.snapshot().sealed);
    assert_eq!(domain.snapshot().live, 1208);
    drop(release);
    assert_eq!(domain.snapshot().live, 0);
    assert!(!domain.snapshot().active);
}

#[path = "scalar_memory_tests/invocation_custody.rs"]
mod invocation_custody;
#[path = "scalar_memory_tests/scalar_custody.rs"]
mod scalar_custody;

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

#[test]
fn original_occurrence_survives_late_cancel_after_real_charged_resource_settlement() {
    use crate::runtime::task::ExecutionFailure;
    use novarocks_memory::attribution::AttributingAllocator;
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    use std::alloc::{GlobalAlloc, Layout, System};
    struct Release<'a> {
        pointer: &'a Cell<*mut u8>,
        allocator: &'a AttributingAllocator<System>,
        layout: Layout,
    }
    impl Drop for Release<'_> {
        fn drop(&mut self) {
            let pointer = self.pointer.replace(std::ptr::null_mut());
            if !pointer.is_null() {
                // SAFETY: the one actual block is released exactly once
                // through its original allocator and original Layout.
                unsafe {
                    self.allocator.dealloc(pointer, self.layout);
                }
            }
        }
    }
    let binding = bound();
    let stopped = Arc::new(RuntimeErrorState::default());
    let runtime = control(Some(&binding), stopped.clone());
    let KernelMemoryAdmission::Granted(ready) = request(&binding, 608) else {
        panic!("the exact original body requires a real grant");
    };
    let domain = ready.domain().clone();
    let allocator = AttributingAllocator::new(System);
    let layout = Layout::from_size_align(600, 8).unwrap();
    let pointer = Cell::new(std::ptr::null_mut());
    let release = Release {
        pointer: &pointer,
        allocator: &allocator,
        layout,
    };
    let expected = KernelFailure::from_domain(LayoutResourceError::SourceModel);
    let late_cancel: ExecutionFailure = KernelFailure::Cancelled.into();
    let mut journal = KernelMemoryJournal::default();
    let result = ready
        .run(&mut journal, || {
            // SAFETY: valid nonzero Layout; Release owns the matching free.
            pointer.set(unsafe { allocator.alloc(layout) });
            assert!(!pointer.get().is_null());
            stopped.set_error(late_cancel.clone());
            Err::<(), _>(expected.clone())
        })
        .unwrap();
    // The actual writer has exited before the ordinary Frame footer.
    assert_eq!(journal.observation.entry(), AmbientEntryObservation::Bound);
    assert_eq!(journal.observation.exit(), AmbientExitObservation::Restored);
    assert_eq!(journal.settlement.as_ref().unwrap().accepted_live, 608);
    assert_eq!(journal.settlement.as_ref().unwrap().next_step, Ok(()));
    assert_eq!(journal.stopped, Some(Ok(())));
    assert!(!domain.snapshot().active);
    assert!(domain.snapshot().sealed);
    assert_eq!(domain.snapshot().live, 608);
    assert_eq!(result.unwrap_err(), expected);
    // Cancellation remains independently observable; it never rewrites
    // the already originating occurrence or the completed settlement.
    assert!(stopped.error().unwrap().same_occurrence(&late_cancel));
    assert_eq!(
        KernelEvaluationControl::checkpoint(&runtime, 0),
        Err(KernelFailure::Cancelled)
    );
    drop(ready);
    drop(release);
    assert_eq!(domain.snapshot().live, 0);
    assert!(!domain.snapshot().active);
}
