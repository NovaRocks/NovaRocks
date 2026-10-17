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

//! Public catalogue preparation, not a private replacement registration.
use super::*;
use crate::{
    FunctionArgument, FunctionValueType, PreparedPureKernel, PureCallPreparation,
    ScopedExpressionEffects,
};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters,
};

fn prepared(types: &[DataType]) -> Arc<dyn PreparedInvocationScalarKernel> {
    let types = types
        .iter()
        .map(|ty| FunctionValueType::new(ty.clone(), true))
        .collect::<Vec<_>>();
    prepared_typed(&types)
}
fn prepared_typed(types: &[FunctionValueType]) -> Arc<dyn PreparedInvocationScalarKernel> {
    prepared_typed_with_result_storage(types, None)
}
fn prepared_typed_with_result_storage(
    types: &[FunctionValueType],
    result_storage: Option<novarocks_type_contract::ValueTypeStorageLifetime>,
) -> Arc<dyn PreparedInvocationScalarKernel> {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let arguments = types
        .iter()
        .map(|value_type| FunctionArgument::Value {
            value_type: value_type.clone(),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    };
    let mut bound = catalog
        .resolve_bound_user(
            "array_sortby",
            FunctionKind::Scalar,
            request,
            crate::binding_test_control(),
        )
        .unwrap();
    if let Some(storage) = result_storage {
        let crate::FunctionResultType::Scalar(result) = &mut bound.selected.result_type else {
            panic!("array_sortby has a scalar result")
        };
        // Real canonical layout creation with a lifetime-only responsibility.
        // Semantic equality is still checked by the public selected owner.
        *result = FunctionValueType::try_from_value(
            result.value_type().clone(),
            novarocks_type_contract::ValueLayout::try_new(result.data_type(), storage).unwrap(),
        )
        .unwrap();
    }
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(771),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    };
    let uses = (0..types.len())
        .map(|i| Some(ExpressionUseId::new(772 + i as u32)))
        .collect::<Vec<_>>();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let prepared = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                request,
                argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
                context,
                parameters: &parameters,
                environment: &[],
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Domain(context.domain),
            },
            selected.clone(),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            crate::binding_test_control(),
        )
        .unwrap();
    let PreparedPureKernel::ScalarInvocation(prepared) = prepared.into_prepared() else {
        panic!("array_sortby must use its installed whole-invocation owner")
    };
    prepared
}
fn list(data_type: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", data_type, true)))
}
#[test]
fn array_sortby_public_owner_prepares_one_through_six_independently_typed_keys() {
    let keys = [
        DataType::Int64,
        DataType::Utf8,
        DataType::Boolean,
        DataType::Float32,
        DataType::Date32,
        DataType::Decimal128(7, 2),
    ];
    for count in 1..=6 {
        let mut types = vec![list(DataType::Utf8)];
        types.extend(keys.iter().take(count).cloned().map(list));
        let prepared = prepared(&types);
        assert_eq!(prepared.contract().value_argument_types().len(), count + 1);
        assert_eq!(prepared.contract().result_type().data_type(), &types[0]);
        assert_eq!(
            prepared.contract().effects().null_behavior,
            FunctionNullBehavior::CalledOnNull
        );
        assert_eq!(
            prepared.contract().effects().argument_control,
            ArgumentControl::Eager
        );
        assert!(prepared.invocation_resource_profile().is_none());
        assert_eq!(
            prepared.input_domain_policy(),
            crate::InvocationInputDomainPolicy::OriginalActualDomain
        );
    }
}
#[test]
fn array_sortby_public_signature_still_refuses_unordered_keys_and_outside_key_counts() {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    for types in [
        vec![list(DataType::Int64)],
        vec![list(DataType::Int64); 8],
        vec![list(DataType::Int64), list(DataType::Binary)],
    ] {
        let arguments = types
            .iter()
            .map(|data_type| FunctionArgument::Value {
                value_type: FunctionValueType::new(data_type.clone(), true),
                constant: None,
            })
            .collect::<Vec<_>>();
        assert!(
            catalog
                .resolve_bound_user(
                    "array_sortby",
                    FunctionKind::Scalar,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: arguments.len(),
                        expected_result_type: None
                    },
                    crate::binding_test_control()
                )
                .is_err()
        );
    }
}

use super::super::array_invocation_test_support::{Control, Host, causes};
use crate::{
    EvaluatedSourceLoan, InvocationScalarEvaluationInstance, ScalarInvocationActivation,
    ScalarInvocationFailure, ScalarInvocationScope, ScalarInvocationSource, Selection,
};
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, types::Int64Type};
use std::sync::Mutex;
fn empty(host: &Host) {
    host.released();
}
fn arrays(rows: Vec<Vec<i64>>) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
        rows.into_iter()
            .map(|row| Some(row.into_iter().map(Some).collect::<Vec<_>>())),
    ))
}
fn row(values: &ArrayRef, ordinal: usize) -> Vec<Option<i64>> {
    let list = values.as_any().downcast_ref::<ListArray>().unwrap();
    let row = list.value(ordinal);
    row.as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn instance(types: &[DataType], host: &Arc<Host>) -> InvocationScalarEvaluationInstance {
    InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared(types),
        Some(host.clone()),
    )
    .unwrap()
}
#[test]
fn array_sortby_public_actual_domain_success_projects_only_outer_demand_and_last_buffer_holds_sources()
 {
    let host = Host::with_limit(64 * 1024 * 1024);
    let source = arrays(vec![vec![10, 20], vec![30, 40], vec![50, 60]]);
    let key = arrays(vec![vec![2, 1], vec![1, 2], vec![2, 1]]);
    let weak = Arc::downgrade(&source);
    let mut kernel = instance(
        &[source.data_type().clone(), key.data_type().clone()],
        &host,
    );
    let actual_rows = [2, 4, 7];
    let demanded = [2, 7];
    let actual = Selection::try_sparse(8, &actual_rows).unwrap();
    let demand = Selection::try_sparse(8, &demanded).unwrap();
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, actual)),
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&key, actual)),
    ];
    let output = kernel
        .evaluate_scoped(
            ScalarInvocationScope::new(actual, demand),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), demanded);
    assert_eq!(row(output.values(), 0), vec![Some(20), Some(10)]);
    assert_eq!(row(output.values(), 1), vec![Some(60), Some(50)]);
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let data = list.values().to_data();
    let last = data.buffers()[0].clone();
    drop(data);
    drop(output);
    drop(kernel);
    drop(source);
    drop(key);
    assert!(
        weak.upgrade().is_some(),
        "the exact actual sources survive through the last output buffer"
    );
    assert!(host.live_blocks() > 0);
    drop(last);
    assert!(weak.upgrade().is_none());
    empty(&host);
}
#[test]
fn array_sortby_public_unselected_length_failure_retains_full_actual_source_and_latches() {
    let host = Host::with_limit(64 * 1024 * 1024);
    let source = arrays(vec![vec![10, 20], vec![30, 40], vec![50, 60]]);
    let key = arrays(vec![vec![2, 1], vec![1], vec![2, 1]]);
    let mut kernel = instance(
        &[source.data_type().clone(), key.data_type().clone()],
        &host,
    );
    let demanded = [2, 7];
    let actual_rows = [2, 4, 7];
    let actual = Selection::try_sparse(8, &actual_rows).unwrap();
    let scope = ScalarInvocationScope::new(actual, Selection::try_sparse(8, &demanded).unwrap());
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, actual)),
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&key, actual)),
    ];
    let failure = kernel
        .evaluate_scoped(
            scope,
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap_err();
    let ScalarInvocationFailure::Data(data) = &failure else {
        panic!("original size mismatch is whole Data")
    };
    assert_eq!(
        data.message(),
        "Expr evaluate meet error: Input arrays' size are not equal in array_sortby."
    );
    assert_eq!(data.batch_rows(), 8);
    assert_eq!(data.source_len(), 3);
    assert_eq!(
        (0..3)
            .map(|i| data.source_row(i).unwrap())
            .collect::<Vec<_>>(),
        vec![2, 4, 7]
    );
    assert!(matches!(
        kernel.evaluate_scoped(
            scope,
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default()
        ),
        Err(ScalarInvocationFailure::Kernel(
            KernelFailure::InstanceFailed
        ))
    ));
    drop(kernel);
    drop(failure);
    empty(&host);
}
#[test]
fn array_sortby_public_constant_uses_checked_nonzero_ordinal_and_actual_domain_broadcast() {
    let host = Host::with_limit(64 * 1024 * 1024);
    let source = arrays(vec![vec![10, 20], vec![30, 40], vec![50, 60]]);
    let key = arrays(vec![vec![1, 2], vec![2, 1]]);
    let ty = FunctionValueType::new(key.data_type().clone(), true);
    let field = Arc::new(ty.try_to_field("key").unwrap());
    let data = key.to_data();
    assert_eq!(data.len(), 2);
    assert_eq!(data.child_data().len(), 1);
    let child = &data.child_data()[0];
    assert_eq!(child.data_type(), &DataType::Int64);
    assert_eq!(child.len(), 4);
    assert!(data.nulls().is_none() && child.nulls().is_none());
    assert_eq!(data.buffers().len(), 1);
    assert_eq!(child.buffers().len(), 1);
    let buffers = [&data.buffers()[0], &child.buffers()[0]];
    assert_ne!(buffers[0].data_ptr(), buffers[1].data_ptr());
    // Read real Arrow allocation capacity, not the four selected Int64 bytes.
    // These two concrete independent buffers are the complete fixture geometry.
    let retained = buffers
        .iter()
        .map(|buffer| u64::try_from(buffer.capacity().max(buffer.len())).unwrap())
        .sum();
    let visited = buffers
        .iter()
        .map(|buffer| u64::try_from(buffer.len()).unwrap())
        .sum();
    let geometry = novarocks_constant_contract::RecursiveConstantResourceInput {
        buffer_count_upper_bound: 2,
        buffer_visits_bytes_upper_bound: visited,
        retained_buffer_capacity_bytes_upper_bound: retained,
        view_validation_bytes_upper_bound: 0,
        utf8_fallback_validation_bytes_upper_bound: 0,
    };
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        crate::binding_test_control(),
        CompilePhase::Validate,
    )
    .unwrap();
    let (facts, policy) =
        novarocks_constant_contract::preflight_recursive_pool_resources_input_derived_observed(
            field.as_ref(),
            &ty,
            [2, 4],
            geometry,
            &mut |_| Ok::<_, std::convert::Infallible>(()),
            &mut work,
        )
        .unwrap();
    work.finish().unwrap();
    assert_eq!(facts.retained_buffer_capacity_bytes, retained);
    assert!(
        retained > 8192,
        "the original 8KiB fixture quota did not cover Arrow's real default builder capacity"
    );
    assert!(
        facts.library_validation_bytes_upper_bound > 16384,
        "opaque validation includes the original diagnostic/header temporary envelope, separately from retained buffers"
    );
    eprintln!(
        "sortby checked Constant geometry={geometry:?}; derived facts={facts:?}; exact policy={policy:?}"
    );
    // Keep the original rejected byte gate as an independent negative. All
    // other limits come from the SAME existing owner, not a guessed larger cap.
    let mut too_small = policy;
    too_small.max_library_validation_bytes = 16384;
    assert!(matches!(
        crate::ConstantPool::try_new(
            field.clone(),
            ty.clone(),
            data.clone(),
            too_small,
            CompilePhase::Validate,
            crate::binding_test_control()
        ),
        Err(crate::ConstantError::Limit(
            "opaque Arrow validation byte limit exceeded"
        ))
    ));
    let pool = crate::ConstantPool::try_new(
        field,
        ty,
        data,
        policy,
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    assert_eq!(pool.resource_facts(), facts);
    let constant = pool.value(1).unwrap();
    let mut kernel = instance(
        &[source.data_type().clone(), key.data_type().clone()],
        &host,
    );
    let actual_rows = [1, 3, 5];
    let demanded = [1, 5];
    let actual = Selection::try_sparse(6, &actual_rows).unwrap();
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, actual)),
        ScalarInvocationSource::Constant(&constant),
    ];
    let output = kernel
        .evaluate_scoped(
            ScalarInvocationScope::new(actual, Selection::try_sparse(6, &demanded).unwrap()),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(row(output.values(), 0), vec![Some(20), Some(10)]);
    assert_eq!(row(output.values(), 1), vec![Some(60), Some(50)]);
    drop(output);
    drop(kernel);
    empty(&host);
}
#[test]
fn array_sortby_public_every_actual_control_refusal_keeps_all_seven_causes_and_no_replay() {
    let source = arrays(vec![vec![10, 20]]);
    let key = arrays(vec![vec![2, 1]]);
    let actual = Selection::all(1);
    let scope = ScalarInvocationScope::new(actual, actual);
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, actual)),
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&key, actual)),
    ];
    let types = [source.data_type().clone(), key.data_type().clone()];
    let host = Host::with_limit(64 * 1024 * 1024);
    let mut kernel = instance(&types, &host);
    let control = Control::default();
    drop(
        kernel
            .evaluate_scoped(
                scope,
                &sources,
                ScalarInvocationActivation::Activated,
                &control,
            )
            .unwrap(),
    );
    let calls = control.trace.lock().unwrap().len();
    assert!(calls > 0);
    drop(kernel);
    empty(&host);
    for cause in causes() {
        for stop in 0..calls {
            let host = Host::with_limit(64 * 1024 * 1024);
            let mut kernel = instance(&types, &host);
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(kernel.evaluate_scoped(scope,&sources,ScalarInvocationActivation::Activated,&control),
                Err(ScalarInvocationFailure::Kernel(actual)) if actual==cause)
            );
            assert!(matches!(
                kernel.evaluate_scoped(
                    scope,
                    &sources,
                    ScalarInvocationActivation::Activated,
                    &control
                ),
                Err(ScalarInvocationFailure::Kernel(
                    KernelFailure::InstanceFailed
                ))
            ));
            drop(kernel);
            empty(&host);
        }
    }
}
#[test]
fn array_sortby_public_refuses_a_missing_actual_host_before_invocation() {
    let result = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared(&[list(DataType::Int64), list(DataType::Int64)]),
        None,
    );
    assert!(matches!(result,Err(KernelFailure::InvalidProgram(cause))
        if cause.message()=="whole-invocation scalar requires an actual allocation host"));
}

#[test]
fn array_sortby_public_every_actual_host_wrapper_refusal_preserves_typed_cause_and_releases() {
    let source = arrays(vec![vec![10, 20]]);
    let key = arrays(vec![vec![2, 1]]);
    let actual = Selection::all(1);
    let scope = ScalarInvocationScope::new(actual, actual);
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, actual)),
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&key, actual)),
    ];
    let types = [source.data_type().clone(), key.data_type().clone()];
    let host = Host::with_limit(64 * 1024 * 1024);
    let mut kernel = instance(&types, &host);
    let before = host.attempts();
    let before_opaque = host.opaque_attempts();
    drop(
        kernel
            .evaluate_scoped(
                scope,
                &sources,
                ScalarInvocationActivation::Activated,
                &Control::default(),
            )
            .unwrap(),
    );
    let requests = [
        (false, host.attempts() - before),
        (true, host.opaque_attempts() - before_opaque),
    ];
    assert!(requests.iter().all(|(_, calls)| *calls > 0));
    drop(kernel);
    empty(&host);
    for cause in causes() {
        for (opaque, calls) in requests {
            for offset in 0..calls {
                let host = Host::with_limit(64 * 1024 * 1024);
                let mut kernel = instance(&types, &host);
                if opaque {
                    host.arm_opaque(offset, cause.clone());
                } else {
                    host.arm_allocation(offset, cause.clone());
                }
                assert!(
                    matches!(kernel.evaluate_scoped(scope,&sources,ScalarInvocationActivation::Activated,&Control::default()),
                    Err(ScalarInvocationFailure::Kernel(actual)) if actual==cause)
                );
                assert!(matches!(
                    kernel.evaluate_scoped(
                        scope,
                        &sources,
                        ScalarInvocationActivation::Activated,
                        &Control::default()
                    ),
                    Err(ScalarInvocationFailure::Kernel(
                        KernelFailure::InstanceFailed
                    ))
                ));
                drop(kernel);
                empty(&host);
            }
        }
    }
}
#[test]
fn array_sortby_public_each_independent_key_position_orders_previous_ties() {
    use arrow_array::StringArray;
    use arrow_buffer::OffsetBuffer;
    fn text(values: &[&str]) -> ArrayRef {
        let values: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0_i32, values.len() as i32].into()),
            values,
            None,
        ))
    }
    let source = arrays(vec![vec![10, 20, 30, 40]]);
    let actual = Selection::all(1);
    let scope = ScalarInvocationScope::new(actual, actual);
    for count in 1..=6 {
        let mut keys = (0..count - 1)
            .map(|index| {
                if index % 2 == 0 {
                    arrays(vec![vec![0, 0, 0, 0]])
                } else {
                    text(&["x", "x", "x", "x"])
                }
            })
            .collect::<Vec<_>>();
        keys.push(if count % 2 == 0 {
            text(&["b", "d", "a", "c"])
        } else {
            arrays(vec![vec![1, 3, 0, 2]])
        });
        let mut types = vec![source.data_type().clone()];
        types.extend(keys.iter().map(|key| key.data_type().clone()));
        let mut sources = vec![ScalarInvocationSource::Array(EvaluatedSourceLoan::new(
            &source, actual,
        ))];
        sources.extend(
            keys.iter()
                .map(|key| ScalarInvocationSource::Array(EvaluatedSourceLoan::new(key, actual))),
        );
        let host = Host::with_limit(64 * 1024 * 1024);
        let mut kernel = instance(&types, &host);
        let output = kernel
            .evaluate_scoped(
                scope,
                &sources,
                ScalarInvocationActivation::Activated,
                &Control::default(),
            )
            .unwrap();
        assert_eq!(
            row(output.values(), 0),
            vec![Some(30), Some(10), Some(40), Some(20)]
        );
        drop(output);
        drop(kernel);
        empty(&host);
    }
}

#[test]
fn array_sortby_public_full_data_formatter_preserves_all_seven_control_and_host_causes() {
    let source = arrays(vec![vec![10, 20], vec![30, 40], vec![50, 60]]);
    let key = arrays(vec![vec![2, 1], vec![1], vec![2, 1]]);
    let actual_rows = [2, 4, 7];
    let demanded = [2, 7];
    let actual = Selection::try_sparse(8, &actual_rows).unwrap();
    let scope = ScalarInvocationScope::new(actual, Selection::try_sparse(8, &demanded).unwrap());
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, actual)),
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&key, actual)),
    ];
    let types = [source.data_type().clone(), key.data_type().clone()];
    let host = Host::with_limit(64 * 1024 * 1024);
    let mut kernel = instance(&types, &host);
    let before = host.attempts();
    let before_opaque = host.opaque_attempts();
    let control = Control::default();
    let failure = kernel
        .evaluate_scoped(
            scope,
            &sources,
            ScalarInvocationActivation::Activated,
            &control,
        )
        .unwrap_err();
    let ScalarInvocationFailure::Data(data) = &failure else {
        panic!("unselected length mismatch must reach the original full Data formatter")
    };
    assert_eq!(
        data.message(),
        "Expr evaluate meet error: Input arrays' size are not equal in array_sortby."
    );
    assert_eq!(data.batch_rows(), 8);
    assert_eq!(data.source_len(), 3);
    assert_eq!(
        (0..3)
            .map(|i| data.source_row(i).unwrap())
            .collect::<Vec<_>>(),
        vec![2, 4, 7]
    );
    let requests = [
        host.attempts() - before,
        host.opaque_attempts() - before_opaque,
        control.trace.lock().unwrap().len(),
    ];
    assert!(requests.iter().all(|count| *count > 0));
    drop(failure);
    drop(kernel);
    empty(&host);
    for cause in causes() {
        for (kind, calls) in requests.into_iter().enumerate() {
            for offset in 0..calls {
                let host = Host::with_limit(64 * 1024 * 1024);
                let mut kernel = instance(&types, &host);
                match kind {
                    0 => host.arm_allocation(offset, cause.clone()),
                    1 => host.arm_opaque(offset, cause.clone()),
                    _ => (),
                }
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: (kind == 2).then(|| (offset, cause.clone())),
                };
                assert!(matches!(kernel.evaluate_scoped(scope,&sources,
                    ScalarInvocationActivation::Activated,&control),
                    Err(ScalarInvocationFailure::Kernel(actual)) if actual==cause));
                // Malformed reentry must not read sources or rerun the formatter.
                let next = Control::default();
                let attempts = host.attempts();
                let opaque_attempts = host.opaque_attempts();
                assert!(matches!(
                    kernel.evaluate_scoped(
                        scope,
                        &[],
                        ScalarInvocationActivation::Activated,
                        &next
                    ),
                    Err(ScalarInvocationFailure::Kernel(
                        KernelFailure::InstanceFailed
                    ))
                ));
                assert!(next.trace.lock().unwrap().is_empty());
                assert_eq!(host.attempts(), attempts);
                assert_eq!(host.opaque_attempts(), opaque_attempts);
                drop(kernel);
                empty(&host);
            }
        }
    }
}

#[test]
fn array_sortby_public_nested_bitmap_source_keeps_complete_type_bytes_and_last_alias() {
    use arrow_array::BinaryArray;
    use arrow_buffer::OffsetBuffer;
    use novarocks_type_contract::{
        ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let child: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&b"bitmap-three"[..]),
        None,
        Some(&b"bitmap-one"[..]),
        Some(&b"unselected-a"[..]),
        Some(&b"unselected-b"[..]),
        Some(&b"bitmap-x"[..]),
        Some(&b"bitmap-y"[..]),
        Some(&b"bitmap-z"[..]),
    ]));
    let source: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Binary, true)),
        OffsetBuffer::new(vec![0_i32, 3, 5, 8].into()),
        child.clone(),
        None,
    ));
    drop(child);
    let storage = ValueTypeStorageLifetime::pure();
    let bitmap = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Bitmap),
        true,
        storage.clone(),
    )
    .unwrap();
    let semantic = ValueTypeRef::try_new(ValueKind::List(bitmap), true, storage.clone()).unwrap();
    let source_type = FunctionValueType::try_from_value(
        semantic,
        ValueLayout::try_new(source.data_type(), storage).unwrap(),
    )
    .unwrap();
    let key = arrays(vec![vec![3, 2, 1], vec![2, 1], vec![1, 1, 0]]);
    let key_type = FunctionValueType::new(key.data_type().clone(), true);
    let prepared = prepared_typed(&[source_type.clone(), key_type.clone()]);
    assert_eq!(
        prepared
            .contract()
            .value_argument_types()
            .collect::<Vec<_>>(),
        vec![&source_type, &key_type]
    );
    assert_eq!(prepared.contract().result_type(), &source_type);
    assert_eq!(
        prepared
            .contract()
            .result_type()
            .child_at(0)
            .unwrap()
            .logical_type(),
        ValueLogicalType::Bitmap
    );
    assert_ne!(
        source_type,
        FunctionValueType::new(source.data_type().clone(), true)
    );
    let host = Host::with_limit(64 * 1024 * 1024);
    let mut kernel = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared.clone(),
        Some(host.clone()),
    )
    .unwrap();
    let weak_source = Arc::downgrade(&source);
    let weak_key = Arc::downgrade(&key);
    let actual_rows = [2, 4, 7];
    let demanded = [2, 7];
    let actual = Selection::try_sparse(8, &actual_rows).unwrap();
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, actual)),
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&key, actual)),
    ];
    let output = kernel
        .evaluate_scoped(
            ScalarInvocationScope::new(actual, Selection::try_sparse(8, &demanded).unwrap()),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), demanded);
    assert!(novarocks_type_contract::arrow_data_types_exact(
        output.values().data_type(),
        source_type.data_type()
    ));
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[0, 3, 6]);
    assert_eq!(list.null_count(), 0);
    let values = list
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    // These are moved opaque Bitmap bytes. The independent keys alone compare.
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![
            Some(&b"bitmap-one"[..]),
            None,
            Some(&b"bitmap-three"[..]),
            Some(&b"bitmap-z"[..]),
            Some(&b"bitmap-x"[..]),
            Some(&b"bitmap-y"[..]),
        ]
    );
    assert_eq!(prepared.contract().result_type(), &source_type);
    let data = list.values().to_data();
    let last = data.buffers()[1].clone();
    assert_eq!(
        last.as_slice(),
        b"bitmap-onebitmap-threebitmap-zbitmap-xbitmap-y"
    );
    drop(data);
    drop(output);
    drop(kernel);
    drop(prepared);
    drop(source);
    drop(key);
    assert!(weak_source.upgrade().is_some());
    assert!(weak_key.upgrade().is_some());
    assert!(host.live_blocks() > 0);
    assert_eq!(
        last.as_slice(),
        b"bitmap-onebitmap-threebitmap-zbitmap-xbitmap-y"
    );
    drop(last);
    assert!(weak_source.upgrade().is_none());
    assert!(weak_key.upgrade().is_none());
    empty(&host);
}

struct CanonicalSortByResultCreator(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl novarocks_type_contract::ValueTypeStorageHold for CanonicalSortByResultCreator {}
impl Drop for CanonicalSortByResultCreator {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[test]
fn array_sortby_public_canonical_result_creator_survives_the_last_output_alias() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let source = arrays(vec![vec![10, 20]]);
    let key = arrays(vec![vec![2, 1]]);
    let types = [
        FunctionValueType::new(source.data_type().clone(), true),
        FunctionValueType::new(key.data_type().clone(), true),
    ];
    let exits = Arc::new(AtomicUsize::new(0));
    let prepared = prepared_typed_with_result_storage(
        &types,
        Some(novarocks_type_contract::ValueTypeStorageLifetime::held(
            Arc::new(CanonicalSortByResultCreator(exits.clone())),
        )),
    );
    drop(types);
    let canonical = prepared.contract().result_type().clone();
    let DataType::List(canonical_field) = canonical.data_type() else {
        panic!("List result expected")
    };
    let DataType::List(actual_field) = source.data_type() else {
        panic!("List source expected")
    };
    assert_eq!(actual_field.as_ref(), canonical_field.as_ref());
    assert!(!Arc::ptr_eq(actual_field, canonical_field));
    let host = Host::with_limit(64 * 1024 * 1024);
    let mut kernel = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared.clone(),
        Some(host.clone()),
    )
    .unwrap();
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(1))),
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&key, Selection::all(1))),
    ];
    let output = kernel
        .evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(row(output.values(), 0), vec![Some(20), Some(10)]);
    let DataType::List(output_field) = output.values().data_type() else {
        panic!("List output expected")
    };
    assert!(Arc::ptr_eq(output_field, canonical_field));
    drop(canonical);
    drop(kernel);
    drop(prepared);
    assert_eq!(
        exits.load(Ordering::SeqCst),
        0,
        "output keeps its canonical type creator"
    );
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let data = list.values().to_data();
    let last = data.buffers()[0].clone();
    assert_eq!(
        i64::from_ne_bytes(last.as_slice()[..8].try_into().unwrap()),
        20
    );
    drop(data);
    drop(output);
    drop(source);
    drop(key);
    assert_eq!(
        exits.load(Ordering::SeqCst),
        0,
        "the last Buffer alias keeps the canonical creator"
    );
    assert!(host.live_blocks() > 0);
    assert_eq!(
        i64::from_ne_bytes(last.as_slice()[..8].try_into().unwrap()),
        20
    );
    drop(last);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    empty(&host);
}
