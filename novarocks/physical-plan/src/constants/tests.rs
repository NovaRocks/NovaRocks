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

//! Sparse table address tests, not package, allocator or provider closure.
use super::*;
use arrow_array::types::Int8Type;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Float32Array, Int8Array, Int64Array, StringArray,
    new_null_array,
};
use arrow_schema::{DataType, Field};
use novarocks_constant_contract::ConstantPolicy;
use novarocks_type_contract::{CompilePhase, PureCompileControl, ValueLogicalType};
use std::sync::{Arc, Mutex};

struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self::at(None)
    }
    fn at(refusal: Option<(usize, CompileControlError)>) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            refusal,
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.events.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let index = events.len();
        events.push((phase, units));
        if let Some((at, cause)) = self.refusal.clone()
            && index == at
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
fn pool(array: ArrayRef, ty: FunctionValueType, field: Arc<Field>) -> ConstantPool {
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap()
}
fn plain(array: ArrayRef, nullable: bool) -> ConstantPool {
    let ty = FunctionValueType::new(array.data_type().clone(), nullable);
    let field = Arc::new(Field::new("literal", array.data_type().clone(), nullable));
    pool(array, ty, field)
}
fn reference(id: u32, ordinal: u32) -> ConstantReference {
    ConstantReference {
        pool: ConstantPoolId::new(id),
        ordinal,
    }
}
// The table never finishes a caller's encompassing scope. This concrete caller
// exposes a result only after its ordinary/successful completed tail.
fn completed<T>(
    control: &Control,
    action: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, ConstantReferenceError>,
) -> Result<T, ConstantReferenceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = action(&mut work);
    if matches!(&result, Err(ConstantReferenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn sparse_zero_and_max_ids_resolve_without_dense_storage_and_duplicate_replacement() {
    let original = plain(Arc::new(Int64Array::from(vec![3, 7])), false);
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(0), original.clone())
        .unwrap();
    table
        .insert(ConstantPoolId::new(u32::MAX), original.clone())
        .unwrap();
    assert_eq!(table.entries().len(), 2);
    assert_eq!(
        table
            .entries()
            .keys()
            .map(|id| id.get())
            .collect::<Vec<_>>(),
        [0, u32::MAX]
    );
    assert_eq!(
        table
            .insert(
                ConstantPoolId::new(0),
                plain(Arc::new(Int64Array::from(vec![99])), false)
            )
            .unwrap_err(),
        ConstantReferenceError::DuplicatePool(ConstantPoolId::new(0))
    );
    for id in [0, u32::MAX] {
        let value = completed(&Control::good(), |work| {
            table.resolve_observed(reference(id, 1), original.value_type(), work)
        })
        .unwrap();
        assert_eq!(value.try_i64().unwrap(), Some(7));
        assert_eq!(value.ordinal(), 1);
        assert!(Arc::ptr_eq(value.pool().array(), original.array()));
    }
}

#[test]
fn missing_pool_and_bad_ordinal_return_exact_errors_and_observed_ordinary_tails() {
    let backing = plain(Arc::new(Int64Array::from(vec![7])), false);
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(0), backing.clone())
        .unwrap();
    for (reference, expected) in [
        (
            reference(1, 0),
            ConstantReferenceError::MissingPool(ConstantPoolId::new(1)),
        ),
        (
            reference(0, 1),
            ConstantReferenceError::Constant(ConstantError::Invalid(
                "constant ordinal is outside its pool",
            )),
        ),
    ] {
        let baseline = Control::good();
        let error = completed(&baseline, |work| {
            table.resolve_observed(reference, backing.value_type(), work)
        })
        .unwrap_err();
        assert_eq!(error, expected);
        let trace = baseline.trace();
        assert!(trace.len() >= 2);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..trace.len() {
                let control = Control::at(Some((at, cause.clone())));
                assert_eq!(
                    completed(&control, |work| table.resolve_observed(
                        reference,
                        backing.value_type(),
                        work
                    ))
                    .unwrap_err(),
                    ConstantReferenceError::Control(cause.clone())
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

#[test]
#[allow(
    deprecated,
    reason = "Dictionary IDs/order are exact frozen public field facts."
)]
fn source_type_requires_exact_nullable_nested_metadata_and_nominal_identity() {
    let integer = plain(Arc::new(Int64Array::from(vec![7])), false);
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(0), integer.clone())
        .unwrap();
    for bad in [
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Int32, false),
    ] {
        assert_eq!(
            completed(&Control::good(), |w| table.resolve_observed(
                reference(0, 0),
                &bad,
                w
            ))
            .unwrap_err(),
            ConstantReferenceError::SourceTypeMismatch(reference(0, 0))
        );
    }
    let child = Arc::new(Field::new("child", DataType::Int64, true).with_metadata(
        std::collections::HashMap::<String, String>::from([("provider.id".into(), "a".into())]),
    ));
    let nested = FunctionValueType::new(DataType::List(child), true);
    let backing = plain(new_null_array(nested.data_type(), 1), true);
    table.insert(ConstantPoolId::new(1), backing).unwrap();
    for (index, bad) in [
        FunctionValueType::new(
            DataType::List(Arc::new(
                Field::new("child", DataType::Int64, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.id".into(),
                        "b".into(),
                    )]),
                ),
            )),
            true,
        ),
        FunctionValueType::new(
            DataType::List(Arc::new(
                Field::new("child", DataType::Int64, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.id".into(),
                        "a".into(),
                    )]),
                ),
            )),
            true,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        if index == 0 {
            completed(&Control::good(), |w| {
                table.resolve_observed(reference(1, 0), &bad, w)
            })
            .unwrap();
            continue;
        }
        assert_eq!(
            completed(&Control::good(), |w| table.resolve_observed(
                reference(1, 0),
                &bad,
                w
            ))
            .unwrap_err(),
            ConstantReferenceError::SourceTypeMismatch(reference(1, 0))
        );
    }
    let encoded = |id, ordered| {
        FunctionValueType::new(
            DataType::Struct(
                vec![Arc::new(Field::new_dict(
                    "encoded",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                    id,
                    ordered,
                ))]
                .into(),
            ),
            true,
        )
    };
    let exact = encoded(73, true);
    table
        .insert(
            ConstantPoolId::new(3),
            plain(new_null_array(exact.data_type(), 1), true),
        )
        .unwrap();
    for equivalent in [encoded(74, true), encoded(73, false)] {
        completed(&Control::good(), |w| {
            table.resolve_observed(reference(3, 0), &equivalent, w)
        })
        .unwrap();
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    table
        .insert(
            ConstantPoolId::new(2),
            pool(
                Arc::new(StringArray::from(vec!["x"])),
                json.clone(),
                Arc::new(json.try_to_field("literal").unwrap()),
            ),
        )
        .unwrap();
    assert_eq!(
        completed(&Control::good(), |w| table.resolve_observed(
            reference(2, 0),
            &FunctionValueType::new(DataType::Utf8, false),
            w
        ))
        .unwrap_err(),
        ConstantReferenceError::SourceTypeMismatch(reference(2, 0))
    );
}

#[test]
fn projection_retains_only_selected_sparse_pools_without_rebuilding_unused_backing() {
    let array: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(320_000), "kept".into()]));
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let field = Arc::new(ty.try_to_field("original").unwrap().with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "provider.fact".into(),
            "unchanged".into(),
        )]),
    ));
    let backing = pool(array, ty.clone(), field.clone());
    let mut table = ConstantPools::empty();
    table
        .insert(
            ConstantPoolId::new(0),
            plain(Arc::new(Int64Array::from(vec![99])), false),
        )
        .unwrap();
    table
        .insert(ConstantPoolId::new(u32::MAX), backing.clone())
        .unwrap();
    let projected = completed(&Control::good(), |w| {
        table.project_observed(
            [(reference(u32::MAX, 1), &ty), (reference(u32::MAX, 1), &ty)],
            w,
        )
    })
    .unwrap();
    assert_eq!(projected.entries().len(), 1);
    assert!(!projected.entries().contains_key(&ConstantPoolId::new(0)));
    let value = completed(&Control::good(), |w| {
        projected.resolve_observed(reference(u32::MAX, 1), &ty, w)
    })
    .unwrap();
    assert_eq!(value.try_utf8().unwrap(), Some("kept"));
    assert_eq!(value.ordinal(), 1);
    assert_eq!(value.pool().array().len(), 2);
    assert!(Arc::ptr_eq(value.pool().field_ref(), &field));
    assert!(Arc::ptr_eq(value.pool().array(), backing.array()));
}

#[test]
fn typed_null_and_dictionary_renumbered_values_retain_two_addressed_backings() {
    let a = plain(
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0]),
                Arc::new(StringArray::from(vec!["kept", "unused"])),
            )
            .unwrap(),
        ),
        false,
    );
    let b = plain(
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![1]),
                Arc::new(StringArray::from(vec!["other-unused", "kept"])),
            )
            .unwrap(),
        ),
        false,
    );
    let mut table = ConstantPools::empty();
    table.insert(ConstantPoolId::new(0), a.clone()).unwrap();
    table
        .insert(ConstantPoolId::new(u32::MAX), b.clone())
        .unwrap();
    let projected = completed(&Control::good(), |w| {
        table.project_observed(
            [
                (reference(0, 0), a.value_type()),
                (reference(u32::MAX, 0), b.value_type()),
            ],
            w,
        )
    })
    .unwrap();
    assert_eq!(
        projected.entries().len(),
        2,
        "value equality cannot collapse sparse addresses"
    );
    let l = completed(&Control::good(), |w| {
        projected.resolve_observed(reference(0, 0), a.value_type(), w)
    })
    .unwrap();
    let r = completed(&Control::good(), |w| {
        projected.resolve_observed(reference(u32::MAX, 0), b.value_type(), w)
    })
    .unwrap();
    assert!(
        l.equals_observed(&r, CompilePhase::Validate, &Control::good())
            .unwrap()
    );
    assert!(Arc::ptr_eq(l.pool().array(), a.array()));
    assert!(Arc::ptr_eq(r.pool().array(), b.array()));
    let null = plain(Arc::new(Int64Array::from(vec![None, Some(4)])), true);
    table.insert(ConstantPoolId::new(7), null.clone()).unwrap();
    let value = completed(&Control::good(), |w| {
        table.resolve_observed(reference(7, 0), null.value_type(), w)
    })
    .unwrap();
    assert!(
        value
            .is_null_observed(CompilePhase::Validate, &Control::good())
            .unwrap()
    );
    assert_eq!(value.value_type(), null.value_type());
}

#[test]
fn float32_raw_nan_payloads_and_signed_zero_survive_sparse_resolution() {
    let bits = [0x7f800001, 0x7fc00002, 0, 0x80000000];
    let backing = plain(
        Arc::new(Float32Array::from(bits.map(f32::from_bits).to_vec())),
        false,
    );
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(u32::MAX), backing.clone())
        .unwrap();
    for (ordinal, bits) in bits.into_iter().enumerate() {
        let value = completed(&Control::good(), |w| {
            table.resolve_observed(reference(u32::MAX, ordinal as u32), backing.value_type(), w)
        })
        .unwrap();
        let array = value
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(array.value(value.ordinal() as usize).to_bits(), bits);
    }
}

#[test]
fn source_metadata_is_retained_without_semantic_projection_copy_and_controls_preserve_prefix() {
    let child = Arc::new(
        Field::new("child", DataType::Int64, true).with_metadata(
            (0..320)
                .map(|i| (format!("provider.{i:04}"), "kept".into()))
                .collect::<std::collections::HashMap<String, String>>(),
        ),
    );
    let source_type = DataType::List(child);
    let ty = FunctionValueType::new(source_type.clone(), true);
    let backing = pool(
        new_null_array(&source_type, 1),
        ty.clone(),
        Arc::new(Field::new("actual.source", source_type, true)),
    );
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(u32::MAX), backing.clone())
        .unwrap();
    let baseline = Control::good();
    let projected = completed(&baseline, |w| {
        table.project_observed(
            [(reference(u32::MAX, 0), &ty), (reference(u32::MAX, 0), &ty)],
            w,
        )
    })
    .unwrap();
    assert_eq!(projected.entries().len(), 1);
    let trace = baseline.trace();
    let DataType::List(original_child) = backing.field().data_type() else {
        unreachable!()
    };
    assert_eq!(original_child.metadata().len(), 320);
    let DataType::List(layout_child) = ty.data_type() else {
        unreachable!()
    };
    assert!(layout_child.metadata().is_empty());
    assert!(trace.len() >= 2);
    assert!(trace.last().is_some_and(|(_, units)| *units > 0));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..trace.len() {
            let control = Control::at(Some((at, cause.clone())));
            assert_eq!(
                completed(&control, |w| table.project_observed(
                    [(reference(u32::MAX, 0), &ty), (reference(u32::MAX, 0), &ty)],
                    w
                ))
                .unwrap_err(),
                ConstantReferenceError::Control(cause.clone())
            );
            assert_eq!(control.trace(), trace[..=at]);
            assert_eq!(table.entries().len(), 1);
            assert!(Arc::ptr_eq(
                table.entries()[&ConstantPoolId::new(u32::MAX)].array(),
                backing.array()
            ));
        }
    }
}

// These construction helpers publish through real fragment, root-use and
// package validators. They never construct an unchecked executable package.
fn constant_fragment(
    id: u32,
    definitions: &[(ConstantReference, FunctionValueType)],
) -> crate::Fragment {
    let mut builder = crate::FragmentBuilder::new(crate::FragmentId::new(id));
    builder
        .add_values(
            crate::NodeId::new(99),
            Box::from([Box::default()]),
            Box::default(),
        )
        .unwrap();
    let mut assignments = Vec::new();
    let mut outputs = Vec::new();
    for (reference, ty) in definitions {
        let expression = builder
            .add_expression(
                crate::NodeId::new(8),
                ty.clone(),
                crate::ExprKind::Constant(*reference),
            )
            .unwrap();
        let value = builder
            .add_value(
                ty.clone(),
                crate::ValueOrigin::Expr {
                    node: crate::NodeId::new(8),
                    expr: expression,
                },
            )
            .unwrap();
        assignments.push((expression, value));
        outputs.push(value);
    }
    builder
        .add_project(
            crate::NodeId::new(8),
            crate::NodeId::new(99),
            assignments.into_boxed_slice(),
            outputs.into_boxed_slice(),
        )
        .unwrap();
    builder
        .finish_definition(
            crate::NodeId::new(8),
            crate::FragmentSink::Noop,
            crate::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap()
}
fn package_input(
    fragment: crate::Fragment,
    constants: ConstantPools,
) -> crate::FragmentPackageInput {
    use novarocks_type_contract::{
        ControlShape, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
        ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    };
    let roots = crate::PhysicalExpressionRoots::try_new(&fragment, &Control::good()).unwrap();
    let domain = EvaluationDomainId::new(17);
    let mut invocations = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(ordinal, (&site, root))| {
            let use_id = ExpressionUseId::new(ordinal as u32 + 42);
            invocations.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain,
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, use_id)
        })
        .collect();
    let flow = ExpressionControlFlow::<crate::ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let expression_uses =
        crate::PhysicalRootUses::try_new(&fragment, flow, bindings, &Control::good()).unwrap();
    let calls =
        crate::FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &Control::good())
            .unwrap();
    let pruning =
        crate::FrozenFragmentPruning::try_new(fragment.id(), vec![], &Control::good()).unwrap();
    crate::FragmentPackageInput {
        constants,
        version: crate::PlanVersionId::try_new([91; 16]).unwrap(),
        required: crate::RequiredContracts::default(),
        fragment,
        expression_uses,
        calls,
        pruning,
        cuts: crate::FragmentCuts::default(),
        result: None,
        parameters: novarocks_type_contract::SemanticParameters::try_new([]).unwrap(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
    }
}
fn plan_builder(fragments: &[crate::Fragment], constants: ConstantPools) -> crate::PlanBuilder {
    let mut builder = crate::PlanBuilder::new(crate::PlanVersionId::try_new([91; 16]).unwrap())
        .with_constant_pools(constants);
    for fragment in fragments {
        builder.add_fragment(fragment.clone()).unwrap();
    }
    builder
}

#[test]
fn actual_plan_and_package_publication_require_sparse_checked_closed_sources() {
    let original = plain(Arc::new(Int64Array::from(vec![9, 42, -7])), false);
    let ty = original.value_type().clone();
    let mut table = ConstantPools::empty();
    for id in [0, u32::MAX] {
        table
            .insert(ConstantPoolId::new(id), original.clone())
            .unwrap();
    }
    let fragment = constant_fragment(
        91,
        &[(reference(0, 1), ty.clone()), (reference(u32::MAX, 2), ty)],
    );
    let plan = plan_builder(std::slice::from_ref(&fragment), table.clone())
        .finish_observed(&Control::good())
        .unwrap();
    assert_eq!(plan.constants().entries().len(), 2);
    let package = crate::FragmentPackage::try_new(
        package_input(fragment.clone(), table.clone()),
        package_admission(),
        &Control::good(),
    )
    .unwrap();
    for id in [0, u32::MAX] {
        assert_eq!(
            plan.constants().entries()[&ConstantPoolId::new(id)].backing_identity(),
            original.backing_identity()
        );
        assert_eq!(
            package.constants().entries()[&ConstantPoolId::new(id)].backing_identity(),
            original.backing_identity()
        );
    }
    assert!(
        plan_builder(std::slice::from_ref(&fragment), table.clone())
            .finish()
            .is_err()
    );
    assert!(
        plan_builder(std::slice::from_ref(&fragment), ConstantPools::empty())
            .finish()
            .is_err()
    );
    table.insert(ConstantPoolId::new(17), original).unwrap();
    assert!(matches!(
        plan_builder(std::slice::from_ref(&fragment), table.clone())
            .finish_observed(&Control::good()),
        Err(crate::PlanConstructionError::Constants(
            ConstantReferenceError::UnusedPools
        ))
    ));
    assert!(matches!(
        crate::FragmentPackage::try_new(
            package_input(fragment, table),
            package_admission(),
            &Control::good()
        ),
        Err(crate::FragmentPackageError::Constant(
            ConstantReferenceError::UnusedPools
        ))
    ));
}

#[test]
fn actual_plan_and_package_publication_refuse_missing_ordinal_and_full_type_mismatch() {
    let original = plain(Arc::new(Int64Array::from(vec![9, 42, -7])), false);
    let ty = original.value_type().clone();
    let mut table = ConstantPools::empty();
    table.insert(ConstantPoolId::new(0), original).unwrap();
    for (reference, expected, failure) in [
        (reference(u32::MAX, 1), ty.clone(), 0),
        (reference(0, 3), ty, 1),
        (
            reference(0, 1),
            FunctionValueType::new(DataType::Int64, true),
            2,
        ),
    ] {
        let fragment = constant_fragment(92, &[(reference, expected)]);
        let plan_error = plan_builder(std::slice::from_ref(&fragment), table.clone())
            .finish_observed(&Control::good())
            .unwrap_err();
        let crate::PlanConstructionError::Constants(plan_error) = plan_error else {
            panic!("constant publication must refuse the source")
        };
        let package_error = crate::FragmentPackage::try_new(
            package_input(fragment, table.clone()),
            package_admission(),
            &Control::good(),
        )
        .unwrap_err();
        let crate::FragmentPackageError::Constant(package_error) = package_error else {
            panic!("constant package must refuse the source")
        };
        for error in [plan_error, package_error] {
            match failure {
                0 => assert_eq!(
                    error,
                    ConstantReferenceError::MissingPool(ConstantPoolId::new(u32::MAX))
                ),
                1 => assert!(matches!(
                    error,
                    ConstantReferenceError::Constant(ConstantError::Invalid(
                        "constant ordinal is outside its pool"
                    ))
                )),
                2 => assert_eq!(error, ConstantReferenceError::SourceTypeMismatch(reference)),
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn actual_peer_fragments_project_their_own_constants_without_global_local_charge() {
    // Compact Null stores no per-row buffers. Each real backing is locally
    // admissible, while the two backing row facts exceed the fragment profile.
    let rows = crate::resource::MAX_FRAGMENT_DYNAMIC_ITEMS / 2 + 100;
    let ty = FunctionValueType::new(DataType::Null, true);
    let mut explicit_policy = policy();
    explicit_policy.max_rows = rows as u64;
    explicit_policy.max_logical_elements = rows as u64;
    let make = || {
        ConstantPool::try_new(
            Arc::new(ty.try_to_field("compact-null").unwrap()),
            ty.clone(),
            arrow_array::NullArray::new(rows).to_data(),
            explicit_policy,
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap()
    };
    let mut table = ConstantPools::empty();
    table.insert(ConstantPoolId::new(0), make()).unwrap();
    table.insert(ConstantPoolId::new(u32::MAX), make()).unwrap();
    assert_eq!(
        table
            .entries()
            .values()
            .map(|pool| pool.resource_facts().rows)
            .sum::<u64>(),
        (2 * rows) as u64
    );
    let fragments = [
        constant_fragment(93, &[(reference(0, 1), ty.clone())]),
        constant_fragment(94, &[(reference(u32::MAX, 2), ty)]),
    ];
    let plan = plan_builder(&fragments, table.clone())
        .finish_observed(&Control::good())
        .unwrap();
    // Giving a local package the global table is a real resource/closure error;
    // extraction must project before the package's local envelope is applied.
    assert!(matches!(
        crate::FragmentPackage::try_new(
            package_input(fragments[0].clone(), table),
            package_admission(),
            &Control::good()
        ),
        Err(crate::FragmentPackageError::Constant(
            ConstantReferenceError::Structure(_)
        ))
    ));
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    for fragment in &fragments {
        let mut projected = ConstantPools::empty();
        let id = if fragment.id().get() == 93 {
            0
        } else {
            u32::MAX
        };
        projected
            .insert(
                ConstantPoolId::new(id),
                plan.constants().entries()[&ConstantPoolId::new(id)].clone(),
            )
            .unwrap();
        let input = package_input(fragment.clone(), projected);
        uses.insert(fragment.id(), input.expression_uses);
        calls.insert(fragment.id(), input.calls);
        pruning.insert(fragment.id(), input.pruning);
    }
    let packages = crate::extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &package_admissions(&plan),
        &Control::good(),
    )
    .unwrap();
    assert_eq!(packages.len(), 2);
    for (fragment, package) in packages {
        let id = if fragment.get() == 93 { 0 } else { u32::MAX };
        assert_eq!(package.constants().entries().len(), 1);
        assert_eq!(
            package.constants().entries()[&ConstantPoolId::new(id)].backing_identity(),
            plan.constants().entries()[&ConstantPoolId::new(id)].backing_identity()
        );
    }
}

#[test]
fn actual_publication_keeps_primary_control_every_callback_and_ordinary_tail() {
    let fields = (0..320)
        .map(|i| {
            let field = Arc::new(
                Field::new(format!("child-{i:04}"), DataType::Int64, false).with_metadata(
                    std::collections::HashMap::from([(
                        format!("provider.key.{i:04}"),
                        "keep".to_owned(),
                    )]),
                ),
            );
            (
                field,
                Arc::new(Int64Array::from(vec![9, 42, -7])) as ArrayRef,
            )
        })
        .collect::<Vec<_>>();
    let array = Arc::new(arrow_array::StructArray::from(fields)) as ArrayRef;
    let original = plain(array, false);
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(u32::MAX), original.clone())
        .unwrap();
    let fragment = constant_fragment(
        95,
        &[(reference(u32::MAX, 1), original.value_type().clone())],
    );
    let baseline = Control::good();
    plan_builder(std::slice::from_ref(&fragment), table.clone())
        .finish_observed(&baseline)
        .unwrap();
    let plan_trace = baseline.trace();
    assert!(plan_trace.iter().any(|(_, units)| *units == 256));
    let input = package_input(fragment.clone(), table.clone());
    let baseline = Control::good();
    crate::FragmentPackage::try_new(input.clone(), package_admission(), &baseline).unwrap();
    let package_trace = baseline.trace();
    assert!(package_trace.iter().any(|(_, units)| *units == 256));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..plan_trace.len() {
            let control = Control::at(Some((at, cause.clone())));
            assert!(
                matches!(plan_builder(std::slice::from_ref(&fragment), table.clone()).finish_observed(&control),
                Err(crate::PlanConstructionError::Constants(ConstantReferenceError::Control(actual))) if actual == cause)
            );
            assert_eq!(control.trace(), plan_trace[..=at]);
        }
        for at in 0..package_trace.len() {
            let control = Control::at(Some((at, cause.clone())));
            assert!(
                matches!(crate::FragmentPackage::try_new(input.clone(), package_admission(),  &control),
                Err(crate::FragmentPackageError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), package_trace[..=at]);
        }
    }
    // A genuine missing reference still finishes ordinary work; a refusal at
    // that tail takes precedence before any failed package can be published.
    let missing = constant_fragment(
        96,
        &[(
            reference(17, 1),
            FunctionValueType::new(DataType::Int64, false),
        )],
    );
    let input = package_input(missing, ConstantPools::empty());
    let baseline = Control::good();
    assert!(matches!(
        crate::FragmentPackage::try_new(input.clone(), package_admission(), &baseline),
        Err(crate::FragmentPackageError::Constant(
            ConstantReferenceError::MissingPool(_)
        ))
    ));
    let trace = baseline.trace();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..trace.len() {
            let control = Control::at(Some((at, cause.clone())));
            assert!(
                matches!(crate::FragmentPackage::try_new(input.clone(), package_admission(),  &control),
                Err(crate::FragmentPackageError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn actual_publication_observes_structure_success_and_ordinary_error_completion() {
    let original = plain(Arc::new(Int64Array::from(vec![9, 42, -7])), false);
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(17), original.clone())
        .unwrap();
    let fragment = constant_fragment(117, &[(reference(17, 1), original.value_type().clone())]);
    let author = |invalid_result: bool| {
        let mut builder = plan_builder(std::slice::from_ref(&fragment), table.clone());
        if invalid_result {
            // The source constants are valid. Only the actual plan's ResultPort
            // contradicts its Noop sink, so refusal comes from structure validation.
            builder
                .set_result_port(crate::ResultPort {
                    scalar_schema: None,
                    fragment: fragment.id(),
                    output: fragment.nodes()[&fragment.root()].output.clone(),
                    fields: Box::default(),
                })
                .unwrap();
        }
        builder
    };
    let valid = author(false).finish_observed(&Control::good()).unwrap();
    let constant_control = Control::good();
    validate_plan_constants_observed(&valid, &constant_control).unwrap();
    let constant_callbacks = constant_control.trace().len();
    for invalid_result in [false, true] {
        let baseline = Control::good();
        let result = author(invalid_result).finish_observed(&baseline);
        if invalid_result {
            let Err(crate::PlanConstructionError::Structure(error)) = result else {
                panic!("the actual result/sink contradiction must fail structure validation")
            };
            assert!(error.to_string().contains("result port has no result sink"));
        } else {
            let plan = result.unwrap();
            assert_eq!(
                plan.constants().entries()[&ConstantPoolId::new(17)].backing_identity(),
                original.backing_identity()
            );
        }
        let trace = baseline.trace();
        assert!(
            trace.len() > constant_callbacks,
            "publication must observe completed structure work after the constant gate"
        );
        assert_eq!(trace.last().unwrap().0, CompilePhase::Validate);
        // The final completion may report zero after an inner scope flush.
        // Its callback is still mandatory for both success and ordinary refusal.
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..trace.len() {
                let control = Control::at(Some((at, cause.clone())));
                assert!(matches!(author(invalid_result).finish_observed(&control),
                    Err(crate::PlanConstructionError::Constants(
                        ConstantReferenceError::Control(actual))) if actual == cause));
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

#[test]
fn optional_constant_projection_observes_nonconstant_prefixes_and_definition_gaps() {
    let original = plain(Arc::new(Int64Array::from(vec![9, 42, -7])), false);
    let ty = original.value_type();
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(u32::MAX), original.clone())
        .unwrap();
    let mut gap = vec![Some((reference(u32::MAX, 1), ty))];
    gap.extend(std::iter::repeat_n(None, 320));
    gap.push(Some((reference(u32::MAX, 2), ty)));
    for definitions in [vec![None; 320], gap] {
        let baseline = Control::good();
        let result = completed(&baseline, |work| {
            table.project_optional_references_observed(definitions.iter().copied(), work)
        })
        .unwrap();
        assert_eq!(
            result.entries().len(),
            usize::from(definitions[0].is_some())
        );
        if let Some(backing) = result.entries().get(&ConstantPoolId::new(u32::MAX)) {
            assert_eq!(backing.backing_identity(), original.backing_identity());
            assert_eq!(backing.value(1).unwrap().try_i64().unwrap(), Some(42));
            assert_eq!(backing.value(2).unwrap().try_i64().unwrap(), Some(-7));
        }
        let trace = baseline.trace();
        assert!(trace.iter().any(|(_, units)| *units == 256));
        assert!(
            trace
                .iter()
                .map(|(_, units)| u64::from(*units))
                .sum::<u64>()
                >= 320
        );
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..trace.len() {
                let control = Control::at(Some((at, cause.clone())));
                assert!(matches!(completed(&control, |work| {
                    table.project_optional_references_observed(definitions.iter().copied(), work)
                }), Err(ConstantReferenceError::Control(actual)) if actual == cause));
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

// Explicit small-fixture source invoice and independent property projection
// ceilings. These are test inputs, not a production default or MEM grant.
fn package_admission() -> crate::FragmentPackageAdmission {
    crate::FragmentPackageAdmission {
        plan_limits: crate::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: crate::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

fn package_admissions(
    plan: &crate::PhysicalPlan,
) -> std::collections::BTreeMap<crate::FragmentId, crate::FragmentPackageAdmission> {
    plan.fragments()
        .keys()
        .map(|id| (*id, package_admission()))
        .collect()
}

#[path = "collection_reference_tests.rs"]
mod collection_reference_tests;

#[test]
fn captured_source_preserves_sparse_address_null_and_original_field_backing() {
    let original = plain(Arc::new(Int64Array::from(vec![Some(7), None])), true);
    let mut table = ConstantPools::empty();
    for id in [0, u32::MAX] {
        table
            .insert(ConstantPoolId::new(id), original.clone())
            .unwrap();
        for ordinal in [0, 1] {
            let c = Control::good();
            let mut calls = 0;
            let value = completed(&c, |work| {
                table.resolve_source_captured_observed(
                    reference(id, ordinal),
                    &mut |value, _| {
                        calls += 1;
                        assert_eq!(value.ordinal(), ordinal);
                        assert!(Arc::ptr_eq(value.pool().field_ref(), original.field_ref()));
                        assert!(std::ptr::eq(value.value_type(), original.value_type()));
                        assert_eq!(c.trace(), [(CompilePhase::Validate, 0)]);
                        Ok::<_, ConstantReferenceError>(())
                    },
                    work,
                )
            })
            .unwrap();
            assert_eq!(calls, 1);
            assert_eq!(value.ordinal(), ordinal);
            assert!(Arc::ptr_eq(value.pool().field_ref(), original.field_ref()));
            let plain_control = Control::good();
            completed(&plain_control, |work| {
                table.resolve_source_observed(reference(id, ordinal), work)
            })
            .unwrap();
            assert_eq!(c.trace(), plain_control.trace());
        }
    }
}

#[test]
fn captured_source_refusal_precedes_pending_completed_lookup_and_has_no_footer() {
    let mut table = ConstantPools::empty();
    table
        .insert(
            ConstantPoolId::new(0),
            plain(Arc::new(Int64Array::from(vec![8, 9])), false),
        )
        .unwrap();
    for pending in [0, 254, 255] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = Control::at(Some((1, cause)));
            let result = completed(&c, |work| {
                for _ in 0..pending {
                    work.step()?;
                }
                table.resolve_source_captured_observed(
                    reference(0, 1),
                    &mut |value, _| {
                        assert_eq!(value.ordinal(), 1);
                        Err::<(), _>(ConstantReferenceError::Control(
                            CompileControlError::ResourceExhausted,
                        ))
                    },
                    work,
                )
            });
            assert!(matches!(
                result,
                Err(ConstantReferenceError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(c.trace(), [(CompilePhase::Validate, 0)]);
        }
    }
}

#[test]
fn captured_source_missing_and_ordinal_errors_keep_plain_observations_and_control_causes() {
    let mut table = ConstantPools::empty();
    table
        .insert(
            ConstantPoolId::new(0),
            plain(Arc::new(Int64Array::from(vec![8, 9])), false),
        )
        .unwrap();
    for address in [reference(0, 1), reference(0, 2), reference(u32::MAX, 0)] {
        let old = Control::good();
        let plain = completed(&old, |work| table.resolve_source_observed(address, work));
        let new = Control::good();
        let mut captures = 0;
        let received = completed(&new, |work| {
            table.resolve_source_captured_observed(
                address,
                &mut |_, _| {
                    captures += 1;
                    Ok::<_, ConstantReferenceError>(())
                },
                work,
            )
        });
        assert_eq!(plain.is_ok(), received.is_ok());
        if let (Err(left), Err(right)) = (&plain, &received) {
            assert_eq!(left, right);
        }
        assert_eq!(captures, usize::from(plain.is_ok()));
        assert_eq!(old.trace(), new.trace());
        let trace = new.trace();
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let c = Control::at(Some((stop, cause.clone())));
                let result = completed(&c, |work| {
                    table.resolve_source_captured_observed(
                        address,
                        &mut |_, _| Ok::<_, ConstantReferenceError>(()),
                        work,
                    )
                });
                assert!(
                    matches!(result, Err(ConstantReferenceError::Control(actual)) if actual == cause)
                );
                assert_eq!(c.trace(), trace[..=stop]);
            }
        }
    }
}

mod borrowed_validation_tests {
    include!("borrowed_validation_tests.rs");
}

#[test]
fn reference_admission_keeps_constant_policy_source_instead_of_relabeling_control() {
    use std::error::Error;
    let original = ConstantError::Limit("original constant owner policy");
    let error = ConstantReferenceError::from(original.clone());
    assert!(matches!(&error, ConstantReferenceError::Constant(_)));
    assert_eq!(
        error.source().unwrap().downcast_ref::<ConstantError>(),
        Some(&original)
    );
}
