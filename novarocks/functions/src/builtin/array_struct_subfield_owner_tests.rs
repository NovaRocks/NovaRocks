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

//! Public catalogue -> selected implementation -> actual invocation host.
use super::array_invocation_source::retain_output;
use super::array_invocation_test_support::{Control, Host, causes};
use crate::*;
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, StringArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CallProofScope, CompilePhase, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
    ExpressionEffectContext, ExpressionUseId, SemanticParameters,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn constant(array: ArrayRef, ordinal: u32) -> ConstantValue {
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    ConstantPool::try_new(
        Arc::new(Field::new("names", ty.data_type().clone(), true)),
        ty,
        array.to_data(),
        super::catalogue::constant_binding_tests::policy(),
        CompilePhase::FunctionSpecialization,
        crate::binding_test_control(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn name(value: &str) -> ConstantValue {
    constant(Arc::new(StringArray::from(vec![value])), 0)
}
fn prepared(source: &ArrayRef, field: &ConstantValue) -> Arc<dyn PreparedInvocationScalarKernel> {
    prepared_typed(
        FunctionValueType::new(source.data_type().clone(), true),
        field,
    )
}
fn prepared_typed(
    source_type: FunctionValueType,
    field: &ConstantValue,
) -> Arc<dyn PreparedInvocationScalarKernel> {
    let arguments = [
        FunctionArgument::Value {
            value_type: source_type,
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, true),
            constant: Some(field.clone()),
        },
    ];
    prepared_arguments(&arguments)
}
fn prepared_arguments(arguments: &[FunctionArgument]) -> Arc<dyn PreparedInvocationScalarKernel> {
    prepared_arguments_with_result_storage(arguments, None)
}
fn prepared_arguments_with_result_storage(
    arguments: &[FunctionArgument],
    result_storage: Option<novarocks_type_contract::ValueTypeStorageLifetime>,
) -> Arc<dyn PreparedInvocationScalarKernel> {
    let catalogue = super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let request = FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    };
    let mut bound = catalogue
        .resolve_bound_user(
            "__array_struct_subfield",
            FunctionKind::Scalar,
            request,
            crate::binding_test_control(),
        )
        .unwrap();
    if let Some(storage) = result_storage {
        let FunctionResultType::Scalar(result) = &mut bound.selected.result_type else {
            panic!("projection has a scalar result")
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
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    };
    let uses = (0..arguments.len())
        .map(|i| (arguments.len() == 2).then_some(ExpressionUseId::new(42 + i as u32)))
        .collect::<Vec<_>>();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let specialization = catalogue
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
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                proof_scope: CallProofScope::Unconditional,
            },
            selected.clone(),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            crate::binding_test_control(),
        )
        .unwrap();
    let PreparedPureKernel::ScalarInvocation(prepared) = specialization.into_prepared() else {
        panic!("public dynamic-v1 did not install ScalarInvocationV1")
    };
    assert_eq!(
        prepared.input_domain_policy(),
        InvocationInputDomainPolicy::OriginalActualDomain
    );
    assert_eq!(
        prepared.contract().function_id().as_str(),
        "builtin.scalar/__array_struct_subfield/v1"
    );
    assert_eq!(
        prepared.contract().selected().overload.as_str(),
        "builtin.scalar/__array_struct_subfield/dynamic-v1"
    );
    assert!(prepared.invocation_resource_profile().is_none());
    prepared
}
fn instance(
    source: &ArrayRef,
    field: &ConstantValue,
    host: &Arc<Host>,
) -> InvocationScalarEvaluationInstance {
    InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared(source, field),
        Some(host.clone()),
    )
    .unwrap()
}
fn list(
    child: ArrayRef,
    struct_valid: Option<Vec<bool>>,
    offsets: Vec<i32>,
    list_valid: Option<Vec<bool>>,
) -> ArrayRef {
    let values: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "item",
            child.data_type().clone(),
            true,
        ))]
        .into(),
        vec![child],
        struct_valid.map(NullBuffer::from),
    ));
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        values,
        list_valid.map(NullBuffer::from),
    ))
}
fn ints(values: &[i32]) -> ArrayRef {
    list(
        Arc::new(Int32Array::from(values.to_vec())),
        None,
        (0..=values.len() as i32).collect(),
        None,
    )
}
fn rows(output: &SelectedValues<'_>) -> Vec<Vec<Option<i32>>> {
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    (0..list.len())
        .map(|row| {
            let array = list.value(row);
            let values = array.as_any().downcast_ref::<Int32Array>().unwrap();
            values.iter().collect()
        })
        .collect()
}
fn kernel(result: Result<SelectedValues<'_>, ScalarInvocationFailure>) -> KernelFailure {
    match result {
        Err(ScalarInvocationFailure::Kernel(cause)) => cause,
        other => panic!("expected typed Kernel, got {other:?}"),
    }
}
#[test]
fn public_array_projection_exact_actual_map_success_selects_only_demand() {
    let source = ints(&[10, 30, 50]);
    let field = name("item");
    let host = Host::with_limit(1 << 22);
    let actual = Selection::try_sparse(7, &[1, 3, 5]).unwrap();
    let demand = Selection::try_sparse(7, &[3, 5]).unwrap();
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, actual)),
        ScalarInvocationSource::Constant(&field),
    ];
    let mut owner = instance(&source, &field, &host);
    let output = owner
        .evaluate_scoped(
            ScalarInvocationScope::new(actual, demand),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), vec![3, 5]);
    assert_eq!(rows(&output), vec![vec![Some(30)], vec![Some(50)]]);
    assert!(output.errors().is_empty());
    drop(output);
    drop(owner);
    host.released();
}

#[test]
fn public_array_projection_wrong_raw_list_layout_is_rejected_without_reselection() {
    let source = ints(&[11, 22]);
    let list = source.as_any().downcast_ref::<ListArray>().unwrap();
    let malformed: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("entry", list.values().data_type().clone(), true)),
        list.offsets().clone(),
        list.values().clone(),
        list.nulls().cloned(),
    ));
    let field = name("item");
    let host = Host::with_limit(1 << 22);
    let mut owner = instance(&source, &field, &host);
    let selection = Selection::all(2);
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&malformed, selection)),
        ScalarInvocationSource::Constant(&field),
    ];
    assert_eq!(
        kernel(owner.evaluate_scoped(
            ScalarInvocationScope::new(selection, selection),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )),
        KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "evaluated argument differs from its exact selected type"
        )),
    );
    drop(owner);
    host.released();
}
#[test]
fn public_array_projection_checked_constant_ordinal_excludes_poison_pool_rows() {
    let source = ints(&[11, 22, 33]);
    let field = constant(
        Arc::new(StringArray::from(vec![
            None,
            Some("missing"),
            Some("item"),
            Some("Item"),
        ])),
        2,
    );
    let host = Host::with_limit(1 << 22);
    let mut owner = instance(&source, &field, &host);
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(3))),
        ScalarInvocationSource::Constant(&field),
    ];
    let output = owner
        .evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(
        rows(&output),
        vec![vec![Some(11)], vec![Some(22)], vec![Some(33)]]
    );
    drop(output);
    drop(owner);
    host.released();
}
#[test]
fn public_array_projection_original_null_struct_parent_offsets_and_slice_child_are_preserved() {
    let source = list(
        Arc::new(Int32Array::from(vec![7, 8, 9, 10, 11])),
        Some(vec![true, false, true, true, true]),
        vec![0, 2, 4, 5],
        Some(vec![true, false, true]),
    );
    let source = source.slice(1, 2); // The actual List carrier still has all five child structs.
    let field = name("item");
    let host = Host::with_limit(1 << 22);
    let mut owner = instance(&source, &field, &host);
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(2))),
        ScalarInvocationSource::Constant(&field),
    ];
    let output = owner
        .evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    let out = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(out.value_offsets(), &[2, 4, 5]);
    assert!(out.is_null(0));
    assert!(!out.is_null(1));
    let values = out.values().as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(7), None, Some(9), Some(10), Some(11)]
    );
    drop(output);
    drop(owner);
    host.released();
}
#[test]
fn public_array_projection_full_data_keeps_actual_domain_not_outer_demand() {
    let source = ints(&[10, 30, 50]);
    let field = name("item");
    let host = Host::with_limit(1 << 22);
    let actual = Selection::try_sparse(7, &[1, 3, 5]).unwrap();
    let demand = Selection::try_sparse(7, &[3, 5]).unwrap();
    // The real declared UTF8 argument carries a whole-operation business failure.
    let names: ArrayRef = Arc::new(StringArray::from(vec!["missing"; 3]));
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, actual)),
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&names, actual)),
    ];
    let mut owner = instance(&source, &field, &host);
    let data = match owner.evaluate_scoped(
        ScalarInvocationScope::new(actual, demand),
        &sources,
        ScalarInvocationActivation::Activated,
        &Control::default(),
    ) {
        Err(ScalarInvocationFailure::Data(data)) => data,
        other => panic!("expected original whole Data: {other:?}"),
    };
    assert_eq!(
        data.message(),
        "__array_struct_subfield field 'missing' does not exist"
    );
    assert_eq!(data.batch_rows(), 7);
    assert_eq!(data.source_len(), 3);
    assert_eq!(
        (0..3)
            .map(|i| data.source_row(i).unwrap())
            .collect::<Vec<_>>(),
        vec![1, 3, 5]
    );
    let control = Control::default();
    let attempts = host.attempts();
    let opaque = host.opaque_attempts();
    // Malformed reentry sources are not examined after the original Data latch.
    assert_eq!(
        kernel(owner.evaluate_scoped(
            ScalarInvocationScope::new(actual, demand),
            &[],
            ScalarInvocationActivation::Activated,
            &control
        )),
        KernelFailure::InstanceFailed
    );
    assert!(control.trace.lock().unwrap().is_empty());
    assert_eq!(host.attempts(), attempts);
    assert_eq!(host.opaque_attempts(), opaque);
    drop(owner);
    assert!(!data.message().is_empty());
    drop(data);
    host.released();
}
#[test]
fn public_array_projection_case_collision_uses_original_conditional_cast() {
    let fields = vec![
        Arc::new(Field::new("ID", DataType::Int32, true)),
        Arc::new(Field::new("id", DataType::Int64, true)),
    ]
    .into();
    let structs: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![123, i64::MAX, i64::MIN])),
        ],
        None,
    ));
    let source: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", structs.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 1, 2, 3].into()),
        structs,
        None,
    ));
    let field = name("id");
    let host = Host::with_limit(1 << 22);
    let mut owner = instance(&source, &field, &host);
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(3))),
        ScalarInvocationSource::Constant(&field),
    ];
    let out = owner
        .evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    // Binding's first ASCII-case-insensitive match is Int32; core's exact-byte
    // match is Int64. The original default safe cast produces NULL on overflow.
    assert_eq!(rows(&out), vec![vec![Some(123)], vec![None], vec![None]]);
    drop(out);
    drop(owner);
    host.released();
}
#[test]
fn public_array_projection_every_control_boundary_preserves_all_seven_causes_and_latch() {
    let source = ints(&[1, 2, 3]);
    let field = name("item");
    let scope = ScalarInvocationScope::new(Selection::all(3), Selection::all(3));
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(3))),
        ScalarInvocationSource::Constant(&field),
    ];
    let host = Host::with_limit(1 << 22);
    let mut owner = instance(&source, &field, &host);
    let control = Control::default();
    let out = owner
        .evaluate_scoped(
            scope,
            &sources,
            ScalarInvocationActivation::Activated,
            &control,
        )
        .unwrap();
    let count = control.trace.lock().unwrap().len();
    assert!(count > 5);
    drop(out);
    drop(owner);
    host.released();
    for cause in causes() {
        for at in 0..count {
            let host = Host::with_limit(1 << 22);
            let mut owner = instance(&source, &field, &host);
            let control = Control {
                trace: Default::default(),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(
                kernel(owner.evaluate_scoped(
                    scope,
                    &sources,
                    ScalarInvocationActivation::Activated,
                    &control
                )),
                cause
            );
            let again = Control::default();
            let attempts = host.attempts();
            let opaque = host.opaque_attempts();
            assert_eq!(
                kernel(owner.evaluate_scoped(
                    scope,
                    &[],
                    ScalarInvocationActivation::Activated,
                    &again
                )),
                KernelFailure::InstanceFailed
            );
            assert!(again.trace.lock().unwrap().is_empty());
            assert_eq!(host.attempts(), attempts);
            assert_eq!(host.opaque_attempts(), opaque);
            drop(owner);
            host.released();
        }
    }
}
#[test]
fn public_array_projection_every_actual_host_request_preserves_all_seven_causes() {
    let source = ints(&[1, 2, 3]);
    let field = name("item");
    let scope = ScalarInvocationScope::new(Selection::all(3), Selection::all(3));
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(3))),
        ScalarInvocationSource::Constant(&field),
    ];
    let host = Host::with_limit(1 << 22);
    let mut owner = instance(&source, &field, &host);
    let base = host.attempts();
    let opaque = host.opaque_attempts();
    let out = owner
        .evaluate_scoped(
            scope,
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    let count = host.attempts() - base;
    let ocount = host.opaque_attempts() - opaque;
    assert!(count > 0);
    assert!(ocount > 0);
    drop(out);
    drop(owner);
    host.released();
    for cause in causes() {
        for (is_opaque, count) in [(false, count), (true, ocount)] {
            for at in 0..count {
                let host = Host::with_limit(1 << 22);
                let mut owner = instance(&source, &field, &host);
                if is_opaque {
                    host.arm_opaque(at, cause.clone())
                } else {
                    host.arm_allocation(at, cause.clone())
                }
                assert_eq!(
                    kernel(owner.evaluate_scoped(
                        scope,
                        &sources,
                        ScalarInvocationActivation::Activated,
                        &Control::default()
                    )),
                    cause
                );
                let control = Control::default();
                let attempts = host.attempts();
                let opaque = host.opaque_attempts();
                assert_eq!(
                    kernel(owner.evaluate_scoped(
                        scope,
                        &[],
                        ScalarInvocationActivation::Activated,
                        &control
                    )),
                    KernelFailure::InstanceFailed
                );
                assert!(control.trace.lock().unwrap().is_empty());
                assert_eq!(host.attempts(), attempts);
                assert_eq!(host.opaque_attempts(), opaque);
                drop(owner);
                host.released();
            }
        }
    }
}
#[test]
fn public_array_projection_last_child_buffer_alias_keeps_real_source_owner_alive() {
    struct ActualSource(Arc<AtomicUsize>);
    impl Drop for ActualSource {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let host = Host::with_limit(1 << 22);
    let control = Control::default();
    let source = ints(&[11, 22]);
    let mut work = crate::kernel_input::EvaluationCheckpoints::new(&control);
    let source = retain_output(
        source,
        ActualSource(dropped.clone()),
        host.clone(),
        &mut work,
    )
    .unwrap();
    work.flush().unwrap();
    let field = name("item");
    let mut owner = instance(&source, &field, &host);
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(2))),
        ScalarInvocationSource::Constant(&field),
    ];
    let out = owner
        .evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            &sources,
            ScalarInvocationActivation::Activated,
            &control,
        )
        .unwrap();
    let list = out.values().as_any().downcast_ref::<ListArray>().unwrap();
    let alias = list.values().to_data().buffers()[0].clone();
    assert_eq!(alias.len(), 8);
    drop(out);
    drop(owner);
    drop(source);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(host.live_blocks() > 0);
    assert_eq!(
        i32::from_ne_bytes(alias.as_slice()[0..4].try_into().unwrap()),
        11
    );
    drop(alias);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    host.released();
}

#[test]
fn public_array_projection_called_on_null_checks_original_name_before_parent_mask() {
    let source = list(
        Arc::new(Int32Array::from(vec![11])),
        None,
        vec![0, 1],
        Some(vec![false]),
    );
    let field = name("item");
    let host = Host::with_limit(1 << 22);
    for names in [
        Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        Arc::new(StringArray::from(vec!["missing"])),
    ] {
        let mut owner = instance(&source, &field, &host);
        let sources = [
            ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(1))),
            ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&names, Selection::all(1))),
        ];
        let data = match owner.evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        ) {
            Err(ScalarInvocationFailure::Data(data)) => data,
            other => panic!("root NULL masked original Data: {other:?}"),
        };
        assert_eq!(
            data.message(),
            if names.is_null(0) {
                "__array_struct_subfield field-name argument must be non-null"
            } else {
                "__array_struct_subfield field 'missing' does not exist"
            }
        );
        drop(data);
        drop(owner);
        host.released();
    }
}
#[test]
fn public_array_projection_activated_empty_is_original_data_validate_only_does_not_enter() {
    let source = ints(&[]);
    let field = name("item");
    let host = Host::with_limit(1 << 22);
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(0))),
        ScalarInvocationSource::Constant(&field),
    ];
    let mut owner = instance(&source, &field, &host);
    let scope = ScalarInvocationScope::new(Selection::all(0), Selection::all(0));
    let output = owner
        .evaluate_scoped(
            scope,
            &sources,
            ScalarInvocationActivation::ValidateOnly,
            &Control::default(),
        )
        .unwrap();
    assert!(output.values().is_empty());
    drop(output);
    let data = match owner.evaluate_scoped(
        scope,
        &sources,
        ScalarInvocationActivation::Activated,
        &Control::default(),
    ) {
        Err(ScalarInvocationFailure::Data(data)) => data,
        other => panic!("activated empty lost original Data: {other:?}"),
    };
    assert_eq!(
        data.message(),
        "__array_struct_subfield field-name argument is empty"
    );
    assert_eq!(data.source_len(), 0);
    drop(data);
    drop(owner);
    host.released();
}
#[test]
fn public_array_projection_without_actual_host_has_named_refusal_and_no_fallback() {
    let source = ints(&[1]);
    let field = name("item");
    let error = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared(&source, &field),
        None,
    )
    .err()
    .expect("missing host was silently installed");
    assert!(matches!(error, KernelFailure::InvalidProgram(_)));
}
#[test]
fn public_array_projection_nested_nominal_child_identity_is_preserved() {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let child: ArrayRef = Arc::new(arrow_array::BinaryArray::from(vec![
        Some(&b"real-bitmap-bytes"[..]),
        None,
    ]));
    let source = list(child.clone(), None, vec![0, 1, 2], None);
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Bitmap),
        true,
        storage.clone(),
    )
    .unwrap();
    let structure = ValueTypeRef::try_new(
        ValueKind::Struct(Box::from([ValueField::new("item", item, storage.clone())])),
        true,
        storage.clone(),
    )
    .unwrap();
    let semantic =
        ValueTypeRef::try_new(ValueKind::List(structure), true, storage.clone()).unwrap();
    let ty = FunctionValueType::try_from_value(
        semantic,
        ValueLayout::try_new(source.data_type(), storage).unwrap(),
    )
    .unwrap();
    let field = name("item");
    let host = Host::with_limit(1 << 22);
    let prepared = prepared_typed(ty, &field);
    assert_eq!(
        prepared
            .contract()
            .result_type()
            .child_at(0)
            .unwrap()
            .logical_type(),
        ValueLogicalType::Bitmap
    );
    let mut owner = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared,
        Some(host.clone()),
    )
    .unwrap();
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(2))),
        ScalarInvocationSource::Constant(&field),
    ];
    let output = owner
        .evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.values().to_data(), child.to_data());
    drop(output);
    drop(owner);
    host.released();
}
#[test]
fn public_array_projection_nested_physical_children_use_original_take_complete_type() {
    use arrow_array::{BooleanArray, Decimal128Array, Float64Array, LargeStringArray, NullArray};
    let children: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("界"), None])),
        Arc::new(LargeStringArray::from(vec![Some("wide"), None])),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Float64Array::from(vec![Some(1.5), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(123i128), None])
                .with_precision_and_scale(12, 2)
                .unwrap(),
        ),
        Arc::new(NullArray::new(2)),
        ints(&[17, 19]),
        Arc::new(StructArray::new(
            vec![Arc::new(Field::new("deep", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![Some(23), None]))],
            None,
        )),
    ];
    for child in children {
        let source = list(child.clone(), None, vec![0, 1, 2], None);
        let field = name("item");
        let host = Host::with_limit(1 << 22);
        let mut owner = instance(&source, &field, &host);
        let sources = [
            ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(2))),
            ScalarInvocationSource::Constant(&field),
        ];
        let output = owner
            .evaluate_scoped(
                ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                &sources,
                ScalarInvocationActivation::Activated,
                &Control::default(),
            )
            .unwrap();
        let list = output
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.values().data_type(), child.data_type());
        assert_eq!(list.values().to_data(), child.to_data());
        drop(output);
        drop(owner);
        host.released();
    }
}

#[test]
fn public_array_projection_utf8_conditional_cast_keeps_default_safe_nulls() {
    let fields = vec![
        Arc::new(Field::new("Chosen", DataType::Int32, true)),
        Arc::new(Field::new("chosen", DataType::Utf8, true)),
    ]
    .into();
    let structures: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("123"), Some("invalid"), None])),
        ],
        None,
    ));
    let source: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", structures.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 1, 2, 3].into()),
        structures,
        None,
    ));
    let field = name("chosen");
    let host = Host::with_limit(1 << 22);
    let mut owner = instance(&source, &field, &host);
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(3))),
        ScalarInvocationSource::Constant(&field),
    ];
    let out = owner
        .evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(rows(&out), vec![vec![Some(123)], vec![None], vec![None]]);
    drop(out);
    drop(owner);
    host.released();
}
#[test]
fn public_array_projection_full_data_host_requests_do_not_replace_any_typed_cause() {
    let source = ints(&[1, 2, 3]);
    let field = name("item");
    let names: ArrayRef = Arc::new(StringArray::from(vec!["missing"; 3]));
    let scope = ScalarInvocationScope::new(Selection::all(3), Selection::all(3));
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(3))),
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&names, Selection::all(3))),
    ];
    let host = Host::with_limit(1 << 22);
    let mut owner = instance(&source, &field, &host);
    let base = host.attempts();
    let opaque = host.opaque_attempts();
    let control = Control::default();
    let data = match owner.evaluate_scoped(
        scope,
        &sources,
        ScalarInvocationActivation::Activated,
        &control,
    ) {
        Err(ScalarInvocationFailure::Data(data)) => data,
        other => panic!("expected full Data: {other:?}"),
    };
    assert_eq!(
        data.message(),
        "__array_struct_subfield field 'missing' does not exist"
    );
    let count = host.attempts() - base;
    let ocount = host.opaque_attempts() - opaque;
    let ccount = control.trace.lock().unwrap().len();
    assert!(count > 0);
    assert!(ocount > 0);
    drop(data);
    drop(owner);
    host.released();
    for cause in causes() {
        for kind in 0..3 {
            let count = match kind {
                0 => count,
                1 => ocount,
                _ => ccount,
            };
            for at in 0..count {
                let host = Host::with_limit(1 << 22);
                let mut owner = instance(&source, &field, &host);
                if kind == 0 {
                    host.arm_allocation(at, cause.clone())
                } else if kind == 1 {
                    host.arm_opaque(at, cause.clone())
                }
                let control = Control {
                    trace: Default::default(),
                    refusal: if kind == 2 {
                        Some((at, cause.clone()))
                    } else {
                        None
                    },
                };
                assert_eq!(
                    kernel(owner.evaluate_scoped(
                        scope,
                        &sources,
                        ScalarInvocationActivation::Activated,
                        &control
                    )),
                    cause
                );
                let next = Control::default();
                let attempts = host.attempts();
                let opaque = host.opaque_attempts();
                assert_eq!(
                    kernel(owner.evaluate_scoped(
                        scope,
                        &[],
                        ScalarInvocationActivation::Activated,
                        &next
                    )),
                    KernelFailure::InstanceFailed
                );
                assert!(next.trace.lock().unwrap().is_empty());
                assert_eq!(host.attempts(), attempts);
                assert_eq!(host.opaque_attempts(), opaque);
                drop(owner);
                host.released();
            }
        }
    }
}
fn run_child() -> ArrayRef {
    use arrow_array::{RunArray, types::Int32Type};
    Arc::new(
        RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 4]),
            &StringArray::from(vec!["first", "last"]),
        )
        .unwrap(),
    )
}
#[test]
fn original_core_run_null_indices_preserve_pinned_encoded_math() {
    use arrow_array::{RunArray, types::Int32Type};
    let child = run_child();
    let source = list(
        child.clone(),
        Some(vec![false, true, false, true]),
        vec![0, 1, 2, 3, 4],
        None,
    );
    let output_type = DataType::List(Arc::new(Field::new(
        "item",
        child.data_type().clone(),
        true,
    )));
    let names: ArrayRef = Arc::new(StringArray::from(vec!["item"; 4]));
    // This is the sole original core, with its original explicit encoded
    // output type. It is not a fabricated public selected encoding contract.
    let out =
        super::array_struct_subfield_core::project(&source, &names, Some(&output_type)).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 1, 2, 3, 4]);
    assert_eq!(list.values().data_type(), child.data_type());
    let run = list
        .values()
        .as_any()
        .downcast_ref::<RunArray<Int32Type>>()
        .unwrap();
    assert_eq!(run.run_ends().values(), &[3, 4]);
    assert_eq!(
        run.values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("first"), Some("last")]
    );
}
#[test]
fn public_array_projection_raw_run_encoding_is_rejected_before_leaf_and_latches() {
    let child = run_child();
    let source = list(
        child.clone(),
        Some(vec![false, true, false, true]),
        vec![0, 1, 2, 3, 4],
        None,
    );
    let field = name("item");
    let real = prepared(&source, &field);
    assert_ne!(
        real.contract()
            .value_argument_types()
            .next()
            .unwrap()
            .data_type(),
        source.data_type(),
        "ValueLayout canonicalization removes the runtime RunEnd wrapper"
    );
    assert_eq!(
        FunctionValueType::new(child.data_type().clone(), true).data_type(),
        &DataType::Utf8
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let observed: Arc<dyn PreparedInvocationScalarKernel> = Arc::new(ObservedEmptyPublicPrepared {
        inner: real,
        leaf_calls: calls.clone(),
    });
    let host = Host::with_limit(1 << 22);
    let mut owner = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        observed,
        Some(host.clone()),
    )
    .unwrap();
    let scope = ScalarInvocationScope::new(Selection::all(4), Selection::all(4));
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(4))),
        ScalarInvocationSource::Constant(&field),
    ];
    assert_eq!(
        kernel(owner.evaluate_scoped(
            scope,
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default()
        )),
        KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "evaluated argument differs from its exact selected type"
        ))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let after = Control::default();
    let before = host.attempts();
    let opaque = host.opaque_attempts();
    assert_eq!(
        kernel(owner.evaluate_scoped(scope, &[], ScalarInvocationActivation::Activated, &after)),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(host.attempts(), before);
    assert_eq!(host.opaque_attempts(), opaque);
    drop(owner);
    host.released();
}
#[test]
fn public_array_projection_original_sparse_union_null_indices_are_supported() {
    use arrow_array::UnionArray;
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::UnionFields;
    let child: ArrayRef = Arc::new(
        UnionArray::try_new(
            UnionFields::try_new(vec![5], vec![Field::new("item", DataType::Int32, true)]).unwrap(),
            ScalarBuffer::from(vec![5i8; 4]),
            None,
            vec![Arc::new(Int32Array::from(vec![11, 22, 33, 44]))],
        )
        .unwrap(),
    );
    let source = list(
        child.clone(),
        Some(vec![false, true, false, true]),
        vec![0, 1, 2, 3, 4],
        None,
    );
    let field = name("item");
    let host = Host::with_limit(1 << 22);
    let mut owner = instance(&source, &field, &host);
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(4))),
        ScalarInvocationSource::Constant(&field),
    ];
    let out = owner
        .evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    let list = out.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.values().data_type(), child.data_type());
    let union = list.values().as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(union.type_ids().as_ref(), &[5, 5, 5, 5]);
    assert_eq!(
        union
            .child(5)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(22), None, Some(44)]
    );
    drop(out);
    drop(owner);
    host.released();
}

#[test]
fn public_array_projection_extra_arity_has_no_arguments_and_original_full_count_data() {
    let source = ints(&[1]);
    let field = name("item");
    let host = Host::with_limit(1 << 22);
    let arguments = [
        FunctionArgument::Value {
            value_type: FunctionValueType::new(source.data_type().clone(), true),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, true),
            constant: Some(field),
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int32, false),
            constant: None,
        },
    ];
    let prepared = prepared_arguments(&arguments);
    assert_eq!(
        prepared.contract().effects().argument_control,
        novarocks_type_contract::ArgumentControl::NoArguments
    );
    let mut owner = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared,
        Some(host.clone()),
    )
    .unwrap();
    let data = match owner.evaluate_scoped(
        ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
        &[],
        ScalarInvocationActivation::Activated,
        &Control::default(),
    ) {
        Err(ScalarInvocationFailure::Data(data)) => data,
        other => panic!("arity entered children or lost Data: {other:?}"),
    };
    assert_eq!(
        data.message(),
        "array_struct_subfield expects 2 to 2 arguments, got 3"
    );
    drop(data);
    drop(owner);
    host.released();
}

struct CanonicalProjectionResultCreator(Arc<AtomicUsize>);
impl novarocks_type_contract::ValueTypeStorageHold for CanonicalProjectionResultCreator {}
impl Drop for CanonicalProjectionResultCreator {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn public_array_projection_canonical_result_creator_survives_the_last_output_alias() {
    let source = ints(&[11, 22]);
    let field = name("item");
    let arguments = [
        FunctionArgument::Value {
            value_type: FunctionValueType::new(source.data_type().clone(), true),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, true),
            constant: Some(field.clone()),
        },
    ];
    let exits = Arc::new(AtomicUsize::new(0));
    let prepared = prepared_arguments_with_result_storage(
        &arguments,
        Some(novarocks_type_contract::ValueTypeStorageLifetime::held(
            Arc::new(CanonicalProjectionResultCreator(exits.clone())),
        )),
    );
    drop(arguments);
    let canonical = prepared.contract().result_type().clone();
    let DataType::List(canonical_field) = canonical.data_type() else {
        panic!("List result expected")
    };
    let original = source.as_any().downcast_ref::<ListArray>().unwrap();
    let DataType::Struct(actual_fields) = original.values().data_type() else {
        panic!("Struct source expected")
    };
    // Equal raw Arrow fields are independently constructed; source custody is
    // not a substitute for this canonical result creator's responsibility.
    assert_eq!(actual_fields[0].as_ref(), canonical_field.as_ref());
    assert!(!Arc::ptr_eq(&actual_fields[0], canonical_field));
    let host = Host::with_limit(1 << 22);
    let mut owner = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared.clone(),
        Some(host.clone()),
    )
    .unwrap();
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(2))),
        ScalarInvocationSource::Constant(&field),
    ];
    let output = owner
        .evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            &sources,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(rows(&output), vec![vec![Some(11)], vec![Some(22)]]);
    let DataType::List(output_field) = output.values().data_type() else {
        panic!("List output expected")
    };
    assert!(Arc::ptr_eq(output_field, canonical_field));
    drop(canonical);
    drop(owner);
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
        i32::from_ne_bytes(last.as_slice()[..4].try_into().unwrap()),
        11
    );
    drop(data);
    drop(output);
    drop(source);
    drop(field);
    assert_eq!(
        exits.load(Ordering::SeqCst),
        0,
        "the last Buffer alias keeps the canonical creator"
    );
    assert!(host.live_blocks() > 0);
    assert_eq!(
        i32::from_ne_bytes(last.as_slice()[..4].try_into().unwrap()),
        11
    );
    drop(last);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    host.released();
}

/// A transparent observer around the REAL public prepared owner. Its inner Box
/// keeps its original exact host inline charge; no fake implementation/result
/// is registered and no allocator-free observer payload is passed as custody.
#[derive(Debug)]
struct ObservedEmptyPublicPrepared {
    inner: Arc<dyn PreparedInvocationScalarKernel>,
    leaf_calls: Arc<AtomicUsize>,
}
impl PreparedInvocationScalarKernel for ObservedEmptyPublicPrepared {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        self.inner.contract()
    }
    fn input_domain_policy(&self) -> InvocationInputDomainPolicy {
        self.inner.input_domain_policy()
    }
    fn invocation_resource_profile(&self) -> Option<WholeInvocationResourceProfile> {
        self.inner.invocation_resource_profile()
    }
    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<ObservedEmptyPublicInstance>()
            + self.inner.instance_retained_upper_bound()
    }
    fn instance_inline_allocation_bytes(&self) -> usize {
        std::mem::size_of::<ObservedEmptyPublicInstance>()
    }
    fn create_instance_with_allocator(
        &self,
        host: Arc<dyn AggregateStateAllocator>,
    ) -> Result<Box<dyn InvocationScalarKernelInstance>, KernelFailure> {
        // The ordinary outer runtime funds this observer Box. Preserve the real
        // delegated owner's separate original Box request on the SAME host.
        let bytes = self.inner.instance_inline_allocation_bytes();
        let mut inline = crate::opaque_memory::OpaqueRetainedCharge::try_new(host.clone())?;
        let mut reservation = if bytes == 0 {
            None
        } else {
            Some(inline.reserve_operation(bytes)?)
        };
        let inner = self.inner.create_instance_with_allocator(host)?;
        if let Some(reservation) = &mut reservation {
            inline.reconcile_under_reservation(bytes, reservation)?;
        }
        Ok(Box::new(ObservedEmptyPublicInstance {
            inner,
            _inner_inline: inline,
            leaf_calls: self.leaf_calls.clone(),
        }))
    }
}
struct ObservedEmptyPublicInstance {
    // The real instance exits before its original separate inline charge.
    inner: Box<dyn InvocationScalarKernelInstance>,
    _inner_inline: crate::opaque_memory::OpaqueRetainedCharge,
    leaf_calls: Arc<AtomicUsize>,
}
impl InvocationScalarKernelInstance for ObservedEmptyPublicInstance {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.inner.retained_bytes()
    }
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
        self.leaf_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.evaluate(input, control)
    }
    fn evaluate_scoped<'a>(
        &mut self,
        input: ScopedScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
        self.leaf_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.evaluate_scoped(input, control)
    }
}
fn observed_empty_public_instance(
    source: &ArrayRef,
    field: &ConstantValue,
    host: &Arc<Host>,
    exits: &Arc<AtomicUsize>,
    leaf_calls: &Arc<AtomicUsize>,
) -> (
    Arc<dyn PreparedInvocationScalarKernel>,
    InvocationScalarEvaluationInstance,
    FunctionValueType,
) {
    let arguments = [
        FunctionArgument::Value {
            value_type: FunctionValueType::new(source.data_type().clone(), true),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, true),
            constant: Some(field.clone()),
        },
    ];
    let real = prepared_arguments_with_result_storage(
        &arguments,
        Some(novarocks_type_contract::ValueTypeStorageLifetime::held(
            Arc::new(CanonicalProjectionResultCreator(exits.clone())),
        )),
    );
    drop(arguments);
    let canonical = real.contract().result_type().clone();
    let prepared: Arc<dyn PreparedInvocationScalarKernel> = Arc::new(ObservedEmptyPublicPrepared {
        inner: real,
        leaf_calls: leaf_calls.clone(),
    });
    let instance = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared.clone(),
        Some(host.clone()),
    )
    .unwrap();
    (prepared, instance, canonical)
}
#[test]
fn public_array_projection_validation_empty_keeps_independent_canonical_creator_without_calling_leaf()
 {
    let source = ints(&[]);
    let field = name("item");
    let host = Host::with_limit(1 << 22);
    let exits = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (prepared, mut instance, canonical) =
        observed_empty_public_instance(&source, &field, &host, &exits, &calls);
    let DataType::List(canonical_field) = canonical.data_type() else {
        panic!("List result expected")
    };
    let input = source.as_any().downcast_ref::<ListArray>().unwrap();
    let DataType::Struct(actual_fields) = input.values().data_type() else {
        panic!("Struct source expected")
    };
    assert_eq!(actual_fields[0].as_ref(), canonical_field.as_ref());
    assert!(!Arc::ptr_eq(&actual_fields[0], canonical_field));
    let scope = ScalarInvocationScope::new(Selection::all(0), Selection::all(0));
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(0))),
        ScalarInvocationSource::Constant(&field),
    ];
    let control = Control::default();
    let output = instance
        .evaluate_scoped(
            scope,
            &sources,
            ScalarInvocationActivation::ValidateOnly,
            &control,
        )
        .unwrap();
    assert!(output.values().is_empty());
    assert!(output.errors().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        !control.trace.lock().unwrap().is_empty(),
        "original scope/source validation still executes"
    );
    let DataType::List(output_field) = output.values().data_type() else {
        panic!("List output expected")
    };
    assert!(Arc::ptr_eq(output_field, canonical_field));
    // Empty validation must not bypass original input checks. A real wrong
    // carrier is rejected before the leaf/empty-construction path, then latches.
    let wrong: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
    let invalid_sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&wrong, Selection::all(0))),
        ScalarInvocationSource::Constant(&field),
    ];
    let before = host.attempts();
    let opaque = host.opaque_attempts();
    assert!(matches!(
        kernel(instance.evaluate_scoped(
            scope,
            &invalid_sources,
            ScalarInvocationActivation::ValidateOnly,
            &Control::default()
        )),
        KernelFailure::InvalidProgram(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(host.attempts(), before);
    assert_eq!(host.opaque_attempts(), opaque);
    let replay = Control {
        trace: Default::default(),
        refusal: Some((0, KernelFailure::Cancelled)),
    };
    assert_eq!(
        kernel(instance.evaluate_scoped(
            ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
            &[],
            ScalarInvocationActivation::ValidateOnly,
            &replay
        )),
        KernelFailure::InstanceFailed
    );
    assert!(replay.trace.lock().unwrap().is_empty());
    assert_eq!(host.attempts(), before);
    assert_eq!(host.opaque_attempts(), opaque);
    let empty_clone = output.values().clone();
    drop(canonical);
    drop(instance);
    drop(prepared);
    drop(output);
    drop(source);
    drop(field);
    drop(wrong);
    assert_eq!(
        exits.load(Ordering::SeqCst),
        0,
        "only the actual empty output clone retains its canonical creator"
    );
    // A List has a real one-element offsets buffer even with zero rows. The
    // child payload buffer is empty and cannot be used as a false last alias.
    let data = empty_clone.to_data();
    let offsets = data.buffers()[0].clone();
    assert_eq!(offsets.len(), 4);
    assert_eq!(
        i32::from_ne_bytes(offsets.as_slice()[..4].try_into().unwrap()),
        0
    );
    drop(data);
    drop(empty_clone);
    assert_eq!(
        exits.load(Ordering::SeqCst),
        0,
        "the real last offsets Buffer retains the canonical creator"
    );
    assert!(host.live_blocks() > 0);
    assert_eq!(
        i32::from_ne_bytes(offsets.as_slice()[..4].try_into().unwrap()),
        0
    );
    drop(offsets);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    host.released();
}
#[test]
fn public_array_projection_validation_empty_actual_host_refusals_keep_typed_cause_and_zero_leaf_replay()
 {
    let source = ints(&[]);
    let field = name("item");
    let scope = ScalarInvocationScope::new(Selection::all(0), Selection::all(0));
    let sources = [
        ScalarInvocationSource::Array(EvaluatedSourceLoan::new(&source, Selection::all(0))),
        ScalarInvocationSource::Constant(&field),
    ];
    // First establish that both request kinds exist on this REAL empty-result
    // custody path. Refusals below target their actual first request, not a
    // fabricated terminal value or a failure injected into the private owner.
    let host = Host::with_limit(1 << 22);
    let exits = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (prepared, mut instance, canonical) =
        observed_empty_public_instance(&source, &field, &host, &exits, &calls);
    let typed = host.attempts();
    let opaque = host.opaque_attempts();
    let output = instance
        .evaluate_scoped(
            scope,
            &sources,
            ScalarInvocationActivation::ValidateOnly,
            &Control::default(),
        )
        .unwrap();
    assert!(host.attempts() > typed);
    assert!(host.opaque_attempts() > opaque);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(output);
    drop(canonical);
    drop(instance);
    drop(prepared);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    host.released();
    for cause in causes() {
        for opaque in [false, true] {
            let host = Host::with_limit(1 << 22);
            let exits = Arc::new(AtomicUsize::new(0));
            let calls = Arc::new(AtomicUsize::new(0));
            let (prepared, mut instance, canonical) =
                observed_empty_public_instance(&source, &field, &host, &exits, &calls);
            if opaque {
                host.arm_opaque(0, cause.clone())
            } else {
                host.arm_allocation(0, cause.clone())
            }
            assert_eq!(
                kernel(instance.evaluate_scoped(
                    scope,
                    &sources,
                    ScalarInvocationActivation::ValidateOnly,
                    &Control::default()
                )),
                cause
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let typed = host.attempts();
            let opaque = host.opaque_attempts();
            let replay = Control::default();
            assert_eq!(
                kernel(instance.evaluate_scoped(
                    scope,
                    &[],
                    ScalarInvocationActivation::ValidateOnly,
                    &replay
                )),
                KernelFailure::InstanceFailed
            );
            assert!(replay.trace.lock().unwrap().is_empty());
            assert_eq!(host.attempts(), typed);
            assert_eq!(host.opaque_attempts(), opaque);
            drop(canonical);
            drop(instance);
            drop(prepared);
            assert_eq!(exits.load(Ordering::SeqCst), 1);
            host.released();
        }
    }
}
