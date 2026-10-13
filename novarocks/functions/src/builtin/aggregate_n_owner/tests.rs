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

//! Public catalogue installation contracts; maintained by the N owner.
//! Source declarations are independent of runtime profile fixtures. No external
//! inputs, task capabilities, historical reports or cluster lifecycle are used.
use crate::builtin::catalogue::builtin_engine_function_catalog;
use crate::*;
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    SemanticParameters, ValueLogicalType,
};
use std::sync::Arc;
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
fn prepare(
    name: &str,
    value: FunctionValueType,
    limit: FunctionValueType,
    phase: AggregateKernelPhase,
) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
    let catalog = builtin_engine_function_catalog();
    let args = [
        FunctionArgument::Value {
            value_type: value,
            constant: None,
        },
        FunctionArgument::Value {
            value_type: limit,
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 2,
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Aggregate, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(51),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::default();
    let uses = [
        Some(ExpressionUseId::new(42)),
        Some(ExpressionUseId::new(43)),
    ];
    let state = FunctionValueType::new(DataType::Binary, true);
    let argument_uses = if phase.consumes_logical_arguments() {
        CallArgumentUses::SelectedChannels(&uses)
    } else {
        CallArgumentUses::AggregateMerge {
            phase,
            state_context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(61),
                domain: EvaluationDomainId::new(71),
                demand: EvaluationDemand::Value,
            },
            state_input_type: &state,
        }
    };
    catalog.prepare_fresh_selected(
        CallEffectInput {
            function_id: &bound.function_id,
            kind: FunctionKind::Aggregate,
            selected: &selected,
            request,
            argument_uses,
            context,
            parameters: &parameters,
            environment: &[],
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Domain(context.domain),
        },
        Arc::clone(&selected),
        PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::pure_value(context),
            options: AggregatePreparationOptions {
                phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_interpretation: None,
                state_input_type: (!phase.consumes_logical_arguments()).then_some(state.clone()),
            },
        },
        &Control,
    )
}
#[test]
fn public_n_owner_installs_every_exact_largeint_declaration_and_phase() {
    for name in ["min_n", "max_n"] {
        for nullable in [false, true] {
            for limit in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
            ] {
                for limit_nullable in [false, true] {
                    for phase in [
                        AggregateKernelPhase::Single,
                        AggregateKernelPhase::Partial,
                        AggregateKernelPhase::Intermediate,
                        AggregateKernelPhase::Final,
                    ] {
                        let value = FunctionValueType::try_with_logical_type(
                            DataType::FixedSizeBinary(16),
                            nullable,
                            ValueLogicalType::LargeInt,
                        )
                        .unwrap();
                        let prepared = prepare(
                            name,
                            value,
                            FunctionValueType::new(limit.clone(), limit_nullable),
                            phase,
                        )
                        .unwrap();
                        let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
                            panic!("aggregate ABI")
                        };
                        assert_eq!(handle.contract().phase(), phase);
                        assert_eq!(
                            handle.contract().call().selected().argument_types[0],
                            FunctionArgumentType::Value(
                                FunctionValueType::try_with_logical_type(
                                    DataType::FixedSizeBinary(16),
                                    nullable,
                                    ValueLogicalType::LargeInt
                                )
                                .unwrap()
                            )
                        );
                        let DataType::List(field) = &handle.contract().final_type().data_type
                        else {
                            panic!("List result")
                        };
                        assert_eq!(field.data_type(), &DataType::FixedSizeBinary(16));
                        assert!(field.is_nullable());
                        assert_eq!(
                            field.metadata(),
                            &std::collections::HashMap::from([(
                                "nr_logical_type".to_owned(),
                                "largeint".to_owned()
                            )])
                        );
                        assert_eq!(
                            handle.contract().intermediate_type(),
                            &FunctionValueType::new(DataType::Binary, true)
                        );
                        assert_eq!(
                            handle.contract().state_format().as_str(),
                            format!("novarocks/{name}/state-v1")
                        );
                        assert_eq!(
                            handle.memory_policy(),
                            AggregateStateMemoryPolicy::AllocationTracked
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn public_n_owner_preserves_physical_profile_and_explicit_uuid_refusal() {
    for name in ["min_n", "max_n"] {
        for phase in [
            AggregateKernelPhase::Single,
            AggregateKernelPhase::Partial,
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let prepared = prepare(
                name,
                FunctionValueType::new(DataType::FixedSizeBinary(16), true),
                FunctionValueType::new(DataType::Int64, true),
                phase,
            )
            .unwrap();
            let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
                panic!("aggregate ABI")
            };
            let DataType::List(field) = &handle.contract().final_type().data_type else {
                panic!("List result")
            };
            assert!(field.metadata().is_empty());
            let uuid = FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap();
            assert!(
                matches!(prepare(name, uuid, FunctionValueType::new(DataType::Int64, true), phase), Err(FunctionSpecializationFailure::Kernel(KernelFailure::InvalidProgram(d))) if d.message()==format!("builtin.aggregate/{name}/v1 has no installed min_n/max_n logical input profile for Uuid"))
            );
        }
    }
}

#[test]
fn public_n_state_column_runs_selected_single_and_partial_final_with_last_alias_custody() {
    use crate::builtin::aggregate_n_largeint_resources::tests::{Host, clean, control};
    use arrow_array::{Array, ArrayRef, Int64Array, ListArray};
    use std::num::NonZeroUsize;
    for name in ["min_n", "max_n"] {
        for partial in [false, true] {
            let host = Arc::new(Host::default());
            let child = {
                let value_type = FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    true,
                    ValueLogicalType::LargeInt,
                )
                .unwrap();
                let limit_type = FunctionValueType::new(DataType::Int64, true);
                let prepared = prepare(
                    name,
                    value_type.clone(),
                    limit_type.clone(),
                    if partial {
                        AggregateKernelPhase::Partial
                    } else {
                        AggregateKernelPhase::Single
                    },
                )
                .unwrap();
                let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
                    panic!("aggregate ABI")
                };
                let mut states = AggregateStateColumn::try_new(
                    handle.clone(),
                    Arc::clone(&host) as Arc<dyn AggregateStateAllocator>,
                    NonZeroUsize::new(2).unwrap(),
                )
                .unwrap();
                let ctl = control(&host, None);
                states.push(&ctl).unwrap();
                states.push(&ctl).unwrap();
                let input_values =
                    crate::largeint::array_from_i128(&[Some(999), Some(-7), Some(888), Some(3)])
                        .unwrap();
                let limits: ArrayRef = Arc::new(Int64Array::from(vec![2]));
                let args = [
                    EvaluatedArgument::Column(&input_values),
                    EvaluatedArgument::Scalar(&limits),
                ];
                let input = SelectedAggregateUpdateInput::try_new(
                    handle.contract(),
                    Selection::try_sparse(4, &[1, 3]).unwrap(),
                    &args,
                    &[],
                    &ctl,
                )
                .unwrap();
                {
                    let mut batch = states
                        .prepare_update_batch_evaluation(&[0, 0], input, &ctl)
                        .unwrap();
                    batch.run(&ctl).unwrap();
                    assert_eq!(batch.rows_processed(), 2);
                }
                let output = states.emit_evaluation(&[0, 1], 2, &ctl).unwrap();
                let output = if partial {
                    let final_prepared =
                        prepare(name, value_type, limit_type, AggregateKernelPhase::Final).unwrap();
                    let PreparedPureKernel::Aggregate(final_handle) = final_prepared.prepared()
                    else {
                        panic!("aggregate ABI")
                    };
                    let mut final_states = AggregateStateColumn::try_new(
                        final_handle.clone(),
                        Arc::clone(&host) as Arc<dyn AggregateStateAllocator>,
                        NonZeroUsize::new(2).unwrap(),
                    )
                    .unwrap();
                    final_states.push(&ctl).unwrap();
                    final_states.push(&ctl).unwrap();
                    let merge = SelectedAggregateMergeInput::try_new(
                        final_handle.contract(),
                        Selection::all(2),
                        EvaluatedArgument::Column(&output),
                        &ctl,
                    )
                    .unwrap();
                    {
                        let mut batch = final_states
                            .prepare_merge_batch_evaluation(&[0, 1], merge, &ctl)
                            .unwrap();
                        batch.run(&ctl).unwrap();
                        assert_eq!(batch.rows_processed(), 2);
                    }
                    final_states.emit_evaluation(&[0, 1], 2, &ctl).unwrap()
                } else {
                    output
                };
                let list = output.as_any().downcast_ref::<ListArray>().unwrap();
                assert!(!list.is_null(0));
                assert!(!list.is_null(1));
                assert_eq!(list.value_length(0), 2);
                assert_eq!(list.value_length(1), 0);
                let child = list
                    .values()
                    .as_any()
                    .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                    .unwrap();
                assert_eq!(i128::from_be_bytes(child.value(0).try_into().unwrap()), -7);
                assert_eq!(i128::from_be_bytes(child.value(1).try_into().unwrap()), 3);
                let DataType::List(field) = output.data_type() else {
                    panic!("List result")
                };
                assert_eq!(
                    field.metadata().get("nr_logical_type").map(String::as_str),
                    Some("largeint")
                );
                Arc::clone(list.values())
            };
            assert!(!host.ledger.lock().unwrap().loans.is_empty());
            std::thread::spawn(move || drop(child)).join().unwrap();
            clean(&host);
        }
    }
}

#[test]
fn private_dispatch_rejects_mixed_states_frames_and_emission_without_skipping_rows() {
    use crate::builtin::aggregate_n::NKernel as PhysicalKernel;
    use crate::builtin::aggregate_n_dispatch::NKernel;
    use crate::builtin::aggregate_n_largeint::NLargeIntKernel;
    use crate::builtin::aggregate_n_largeint_resources::tests::{Host, clean, control};
    use arrow_array::{ArrayRef, Int64Array};
    let host = Arc::new(Host::default());
    {
        let physical = prepare(
            "min_n",
            FunctionValueType::new(DataType::FixedSizeBinary(16), true),
            FunctionValueType::new(DataType::Int64, true),
            AggregateKernelPhase::Single,
        )
        .unwrap();
        let largeint = prepare(
            "min_n",
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
            FunctionValueType::new(DataType::Int64, true),
            AggregateKernelPhase::Single,
        )
        .unwrap();
        let PreparedPureKernel::Aggregate(p) = physical.prepared() else {
            panic!("aggregate ABI")
        };
        let PreparedPureKernel::Aggregate(l) = largeint.prepared() else {
            panic!("aggregate ABI")
        };
        let p = NKernel::Physical(Arc::new(PhysicalKernel {
            contract: Arc::clone(p.contract()),
            keep_smallest: true,
        }));
        let l = NKernel::LargeInt(Arc::new(
            NLargeIntKernel::try_new(Arc::clone(l.contract()), true, &Control).unwrap(),
        ));
        let ctl = control(&host, None);
        let authority = Arc::clone(&host) as Arc<dyn AggregateStateAllocator>;
        let mut ps = p
            .create_state_with_allocator(Some(Arc::clone(&authority)), &ctl)
            .unwrap();
        let mut ls = l
            .create_state_with_allocator(Some(Arc::clone(&authority)), &ctl)
            .unwrap();
        let values = crate::largeint::array_from_i128(&[Some(7)]).unwrap();
        let limits: ArrayRef = Arc::new(Int64Array::from(vec![2]));
        let args = [
            EvaluatedArgument::Column(&values),
            EvaluatedArgument::Scalar(&limits),
        ];
        let pi = SelectedAggregateUpdateInput::try_new(
            p.contract(),
            Selection::all(1),
            &args,
            &[],
            &ctl,
        )
        .unwrap();
        let li = SelectedAggregateUpdateInput::try_new(
            l.contract(),
            Selection::all(1),
            &args,
            &[],
            &ctl,
        )
        .unwrap();
        let pb = p
            .prepare_update_evaluation(pi, &[0], Some(Arc::clone(&authority)), &ctl)
            .unwrap();
        let lb = l
            .prepare_update_evaluation(li, &[0], Some(Arc::clone(&authority)), &ctl)
            .unwrap();
        for error in [
            p.update_row_evaluation(&mut ls, &pb, 0, &ctl),
            l.update_row_evaluation(&mut ps, &lb, 0, &ctl),
            p.update_row_evaluation(&mut ps, &lb, 0, &ctl),
            l.update_row_evaluation(&mut ls, &pb, 0, &ctl),
        ] {
            assert!(matches!(
                error,
                Err(EvaluationFailure::Kernel(KernelFailure::InvalidProgram(_)))
            ));
        }
        let context = AggregateEmissionContext::from_host(l.contract(), &[0], 1, Some(&authority));
        assert!(matches!(
            p.build_final(std::iter::once(&ls), &ctl),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert!(matches!(
            l.build_final_evaluation_with_context(std::iter::once(&ps), &context, &ctl),
            Err(EvaluationFailure::Kernel(KernelFailure::InvalidProgram(_)))
        ));
    }
    clean(&host);
}
