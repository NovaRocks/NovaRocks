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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::{Fragment, PlanLimits, UnpivotConstant, ValueId};
use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};

fn lists(width: usize) -> ConstantPool {
    let mut builder = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Int32,
        false,
    )));
    for row in [
        vec![9999],
        (0..width).map(|i| i32::try_from(i).unwrap() - 2).collect(),
        vec![11],
    ] {
        for value in row {
            builder.values().append_value(value);
        }
        builder.append(true);
    }
    plain(Arc::new(builder.finish()), false)
}
fn maps() -> ConstantPool {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new())
        .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
        .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
    // The unused row is legal generic Map data, but violates Unpivot ordering.
    for row in [
        vec![("z", "unused"), ("a", "unused")],
        vec![("a", "λ\0"), ("z", "tail")],
        vec![("b", "v")],
    ] {
        for (key, value) in row {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).unwrap();
    }
    plain(Arc::new(builder.finish()), false)
}
fn table(backing: &ConstantPool, ids: &[u32]) -> ConstantPools {
    let mut pools = ConstantPools::empty();
    for id in ids {
        pools
            .insert(ConstantPoolId::new(*id), backing.clone())
            .unwrap();
    }
    pools
}
// A genuine checked construction fragment. The tests below invoke the actual
// mandatory constant-source gate, not a fabricated complete package.
fn collection_fragment(
    constants: &[UnpivotConstant],
    ty: &FunctionValueType,
) -> (Fragment, ValueId) {
    let mut builder = crate::FragmentBuilder::new(crate::FragmentId::new(91));
    let empty = crate::NodeId::new(u32::MAX);
    let project = crate::NodeId::new(0);
    let unpivot = crate::NodeId::new(901);
    builder
        .add_values(empty, Box::from([Box::default()]), Box::default())
        .unwrap();
    let integer = FunctionValueType::new(DataType::Int64, false);
    let expression = builder
        .add_expression(
            project,
            integer.clone(),
            crate::ExprKind::Literal(crate::LiteralValue::Int64(7)),
        )
        .unwrap();
    let input = builder
        .add_value(
            integer.clone(),
            crate::ValueOrigin::Expr {
                node: project,
                expr: expression,
            },
        )
        .unwrap();
    builder
        .add_project(
            project,
            empty,
            Box::from([(expression, input)]),
            Box::from([input]),
        )
        .unwrap();
    let value = builder
        .add_value(
            integer,
            crate::ValueOrigin::NodeOutput {
                node: unpivot,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let literal = builder
        .add_value(
            ty.clone(),
            crate::ValueOrigin::NodeOutput {
                node: unpivot,
                output_ordinal: 1,
            },
        )
        .unwrap();
    builder
        .add_row_rewriting(
            unpivot,
            project,
            Some(&BTreeMap::new()),
            Box::from([value, literal]),
            crate::NodeKind::Unpivot {
                spec: crate::UnpivotSpec {
                    passthrough: Box::default(),
                    value_output: value,
                    literal_outputs: Box::from([literal]),
                    mappings: constants
                        .iter()
                        .cloned()
                        .map(|constant| crate::UnpivotValueMapping {
                            input,
                            constants: Box::from([constant]),
                        })
                        .collect(),
                    max_output_rows: 1024,
                    max_output_bytes: 1 << 20,
                },
            },
        )
        .unwrap();
    let fragment = builder
        .finish_structure(
            unpivot,
            crate::FragmentSink::Noop,
            crate::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            PlanLimits::FROZEN,
            &Control::good(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    (fragment, literal)
}
fn gate(
    fragment: &Fragment,
    pools: &ConstantPools,
    limits: PlanLimits,
    control: &Control,
) -> Result<(), ConstantReferenceError> {
    validate_fragment_constants_observed(fragment, pools, true, limits, control)
}
fn changed_output(fragment: &Fragment, value: ValueId, ty: FunctionValueType) -> Fragment {
    // Deliberate source-gate input mutation; this is not whole-package admission.
    let mut parts = fragment.clone().into_parts();
    parts.values.get_mut(&value).unwrap().ty = ty;
    parts.into()
}

#[test]
fn collection_sparse_closure_keeps_each_used_alias_and_selected_nonzero_ordinal() {
    let backing = lists(2);
    let pools = table(&backing, &[0, u32::MAX, 17]);
    let constants = [
        UnpivotConstant::Int32List(reference(0, 1)),
        UnpivotConstant::Int32List(reference(u32::MAX, 2)),
    ];
    let (fragment, _) = collection_fragment(&constants, backing.value_type());
    // Direct collection references alone must select the observed publication
    // port, even when a caller omits the backing table entirely.
    let bare_error = plan_builder(std::slice::from_ref(&fragment), ConstantPools::empty())
        .finish()
        .unwrap_err();
    assert_eq!(bare_error.errors()[0].path(), "constants");
    assert_eq!(
        bare_error.errors()[0].message(),
        "checked constants and original call requests require caller-observed plan publication"
    );
    assert_eq!(
        gate(&fragment, &pools, PlanLimits::FROZEN, &Control::good()),
        Err(ConstantReferenceError::UnusedPools)
    );
    let projected = completed(&Control::good(), |work| {
        pools.project_fragment_observed(&fragment, work)
    })
    .unwrap();
    assert_eq!(
        projected
            .entries()
            .keys()
            .map(|id| id.get())
            .collect::<Vec<_>>(),
        [0, u32::MAX]
    );
    gate(&fragment, &projected, PlanLimits::FROZEN, &Control::good()).unwrap();
    completed(&Control::good(), |work| {
        for (id, ordinal, expected) in [(0, 1, vec![-2, -1]), (u32::MAX, 2, vec![11])] {
            let value = projected.resolve_source_observed(reference(id, ordinal), work)?;
            assert_eq!(value.ordinal(), ordinal);
            assert!(Arc::ptr_eq(value.pool().array(), backing.array()));
            let view = value
                .int32_list_observed(CompilePhase::Validate, work.control())?
                .unwrap();
            assert_eq!(view.len(), expected.len());
            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(view.item_observed(index, work)?, Some(expected));
            }
        }
        Ok(())
    })
    .unwrap();
    let (single, _) = collection_fragment(&constants[..1], backing.value_type());
    let one = completed(&Control::good(), |work| {
        pools.project_fragment_observed(&single, work)
    })
    .unwrap();
    assert_eq!(one.entries().len(), 1);
    gate(&single, &one, PlanLimits::FROZEN, &Control::good()).unwrap();
}

#[test]
#[allow(
    deprecated,
    reason = "Exact frozen dictionary field ID and ordering are source facts."
)]
fn collection_gate_preserves_complete_domains_and_rejects_wrong_nullable_profiles() {
    let backing = lists(2);
    let pools = table(&backing, &[0]);
    let constant = UnpivotConstant::Int32List(reference(0, 1));
    let (fragment, literal) = collection_fragment(&[constant], backing.value_type());
    let mut widened = backing.value_type().clone();
    widened = widened.with_nullable(true).unwrap();
    gate(
        &changed_output(&fragment, literal, widened),
        &pools,
        PlanLimits::FROZEN,
        &Control::good(),
    )
    .unwrap();
    // The constant gate permits root widening; structural output nullability
    // remains an independent obligation before complete package publication.
    let child = |metadata: bool, nullable: bool| {
        Arc::new(if metadata {
            Field::new("item", DataType::Int32, nullable).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "source.field".into(),
                    "changed".into(),
                )]),
            )
        } else {
            Field::new("item", DataType::Int32, nullable)
        })
    };
    for bad in [
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::List(child(false, true)), false),
    ] {
        assert_eq!(
            gate(
                &changed_output(&fragment, literal, bad),
                &pools,
                PlanLimits::FROZEN,
                &Control::good()
            ),
            Err(ConstantReferenceError::InvalidConsumer(
                "Unpivot collection source differs from its complete output domain"
            ))
        );
    }
    let decorated = FunctionValueType::new(DataType::List(child(true, false)), false);
    let decorated_output = changed_output(&fragment, literal, decorated.clone());
    // Provider annotations are discarded by the declared value author; they
    // neither change semantics nor survive in the normalized value layout.
    gate(
        &decorated_output,
        &pools,
        PlanLimits::FROZEN,
        &Control::good(),
    )
    .unwrap();
    crate::validation::validate_fragment_definition(&decorated_output).unwrap();
    // The same decoration on an actual admitted source is rejected by the
    // special source profile, even when the generic constant owner admits it.
    let field = Arc::new(Field::new(
        "decorated_source",
        DataType::List(child(true, false)),
        false,
    ));
    let mut builder = ListBuilder::new(Int32Builder::new()).with_field(child(true, false));
    builder.values().append_slice(&[7, 8]);
    builder.append(true);
    let array = builder.finish();
    let decorated_pool = ConstantPool::try_new(
        field,
        decorated,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let (decorated_source_fragment, _) = collection_fragment(
        &[UnpivotConstant::Int32List(reference(0, 0))],
        backing.value_type(),
    );
    assert_eq!(
        gate(
            &decorated_source_fragment,
            &table(&decorated_pool, &[0]),
            PlanLimits::FROZEN,
            &Control::good()
        ),
        Err(ConstantReferenceError::InvalidConsumer(
            "Unpivot collection source differs from its exact non-null special type"
        ))
    );
    let nullable = FunctionValueType::new(backing.value_type().data_type().clone(), true);
    let null_pool = plain(new_null_array(nullable.data_type(), 1), true);
    let nulls = table(&null_pool, &[0]);
    let (null_fragment, _) = collection_fragment(
        &[UnpivotConstant::Int32List(reference(0, 0))],
        backing.value_type(),
    );
    assert_eq!(
        gate(&null_fragment, &nulls, PlanLimits::FROZEN, &Control::good()),
        Err(ConstantReferenceError::InvalidConsumer(
            "Unpivot collection source differs from its exact non-null special type"
        ))
    );
    let wrong_profile = plain(Arc::new(Int64Array::from(vec![7])), false);
    assert_eq!(
        gate(
            &fragment,
            &table(&wrong_profile, &[0]),
            PlanLimits::FROZEN,
            &Control::good()
        ),
        Err(ConstantReferenceError::Constant(ConstantError::Invalid(
            "constant ordinal is outside its pool"
        )))
    );
    let (wrong_fragment, _) = collection_fragment(
        &[UnpivotConstant::Int32List(reference(0, 0))],
        backing.value_type(),
    );
    assert_eq!(
        gate(
            &wrong_fragment,
            &table(&wrong_profile, &[0]),
            PlanLimits::FROZEN,
            &Control::good()
        ),
        Err(ConstantReferenceError::InvalidConsumer(
            "Unpivot collection source differs from its exact non-null special type"
        ))
    );
    // Dictionary namespace/order belong to actual encoding. Normalized static
    // value bindings do not acquire that source identity.
    let encoded = |id, ordered| {
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new_dict(
                "item",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
                id,
                ordered,
            ))),
            true,
        )
    };
    let exact = encoded(73, true);
    let encoded_pool = plain(new_null_array(exact.data_type(), 2), true);
    let encoded_pools = table(&encoded_pool, &[u32::MAX]);
    for equivalent in [encoded(74, true), encoded(73, false)] {
        let fragment = constant_fragment(92, &[(reference(u32::MAX, 1), equivalent)]);
        gate(
            &fragment,
            &encoded_pools,
            PlanLimits::FROZEN,
            &Control::good(),
        )
        .unwrap();
    }
    let exact_fragment = constant_fragment(92, &[(reference(u32::MAX, 1), exact)]);
    gate(
        &exact_fragment,
        &encoded_pools,
        PlanLimits::FROZEN,
        &Control::good(),
    )
    .unwrap();
}

#[test]
fn selected_map_rows_preserve_payload_and_original_item_byte_limits() {
    let backing = maps();
    let pools = table(&backing, &[u32::MAX]);
    let selected = UnpivotConstant::Utf8Map(reference(u32::MAX, 1));
    let (fragment, _) = collection_fragment(std::slice::from_ref(&selected), backing.value_type());
    gate(&fragment, &pools, PlanLimits::FROZEN, &Control::good()).unwrap();
    completed(&Control::good(), |work| {
        let value = pools.resolve_source_observed(reference(u32::MAX, 1), work)?;
        let view = value
            .utf8_map_observed(CompilePhase::Validate, work.control())?
            .unwrap();
        assert_eq!(view.item_observed(0, work)?, (Some("a"), Some("λ\0")));
        assert_eq!(view.item_observed(1, work)?, (Some("z"), Some("tail")));
        let usage = unpivot_collection_usage_observed(
            &pools,
            &selected,
            Some(backing.value_type()),
            2,
            9,
            work,
        )?;
        assert_eq!((usage.items, usage.payload_bytes), (2, 9));
        Ok(())
    })
    .unwrap();
    for (items, bytes) in [(1, 9), (2, 8)] {
        assert_eq!(
            completed(&Control::good(), |work| unpivot_collection_usage_observed(
                &pools,
                &selected,
                Some(backing.value_type()),
                items,
                bytes,
                work
            ))
            .unwrap_err(),
            ConstantReferenceError::Control(CompileControlError::ResourceExhausted)
        );
    }
    let (unused_bad, _) = collection_fragment(
        &[UnpivotConstant::Utf8Map(reference(u32::MAX, 0))],
        backing.value_type(),
    );
    assert_eq!(
        gate(&unused_bad, &pools, PlanLimits::FROZEN, &Control::good()),
        Err(ConstantReferenceError::InvalidConsumer(
            "unpivot map keys must be non-empty and strictly increasing"
        ))
    );
}

fn resources(
    pools: &ConstantPools,
    fragment: Option<&Fragment>,
) -> crate::resource::CutResourceUsage {
    completed(&Control::good(), |work| {
        let mut resource = crate::resource::CutResourcePreflight::new();
        resource.add_constants_observed(pools, work)?;
        if let Some(fragment) = fragment {
            resource.add_unpivot_sources_observed(fragment, pools, PlanLimits::FROZEN, work)?;
        }
        let mut errors = crate::validation::ValidationContext::new();
        let usage = resource.validate("collection.resources", &mut errors);
        assert!(errors.is_empty());
        Ok(usage)
    })
    .unwrap()
}
#[test]
fn each_collection_reference_charges_selected_items_bytes_and_backing_once_at_exact_caps() {
    let backing = lists(2);
    let single = table(&backing, &[0]);
    let aliases = table(&backing, &[0, u32::MAX]);
    let (fragment, _) = collection_fragment(
        &[
            UnpivotConstant::Int32List(reference(0, 1)),
            UnpivotConstant::Int32List(reference(u32::MAX, 1)),
        ],
        backing.value_type(),
    );
    let one = resources(&single, None);
    let alias = resources(&aliases, None);
    let total = resources(&aliases, Some(&fragment));
    assert_eq!(alias.items, one.items + 1);
    assert_eq!(alias.bytes, one.bytes);
    assert_eq!(total.items, alias.items + 4);
    assert_eq!(total.bytes, alias.bytes + 16);
    gate(
        &fragment,
        &aliases,
        PlanLimits {
            unpivot_collection_items: 4,
            ..PlanLimits::FROZEN
        },
        &Control::good(),
    )
    .unwrap();
    assert_eq!(
        gate(
            &fragment,
            &aliases,
            PlanLimits {
                unpivot_collection_items: 3,
                ..PlanLimits::FROZEN
            },
            &Control::good()
        ),
        Err(ConstantReferenceError::Control(
            CompileControlError::ResourceExhausted
        ))
    );
    // Original dynamic envelope, with other real caller charges occupying its
    // remaining space. No test-only alternate wallet or policy is introduced.
    for item_axis in [false, true] {
        for extra in [0, 1] {
            let result = completed(&Control::good(), |work| {
                let mut resource = crate::resource::CutResourcePreflight::new();
                if item_axis {
                    resource.add_items(
                        crate::resource::MAX_FRAGMENT_DYNAMIC_ITEMS - total.items + extra,
                    );
                } else {
                    resource.add_bytes(
                        crate::resource::MAX_FRAGMENT_DYNAMIC_BYTES - total.bytes + extra,
                    );
                }
                resource.add_constants_observed(&aliases, work)?;
                resource.add_unpivot_sources_observed(
                    &fragment,
                    &aliases,
                    PlanLimits::FROZEN,
                    work,
                )?;
                let mut errors = crate::validation::ValidationContext::new();
                let actual = resource.validate("collection.resources", &mut errors);
                assert!(errors.is_empty());
                if item_axis {
                    assert_eq!(actual.items, crate::resource::MAX_FRAGMENT_DYNAMIC_ITEMS);
                } else {
                    assert_eq!(actual.bytes, crate::resource::MAX_FRAGMENT_DYNAMIC_BYTES);
                }
                Ok(())
            });
            if extra == 0 {
                result.unwrap();
            } else {
                assert_eq!(
                    result,
                    Err(ConstantReferenceError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                );
            }
        }
    }
}

#[test]
fn collection_gate_original_three_causes_stop_each_actual_prefix_including_ordinary_tails() {
    let backing = lists(2);
    let pools = table(&backing, &[0]);
    for address in [reference(0, 1), reference(u32::MAX, 1), reference(0, 3)] {
        let (fragment, _) =
            collection_fragment(&[UnpivotConstant::Int32List(address)], backing.value_type());
        let baseline = Control::good();
        let result = gate(&fragment, &pools, PlanLimits::FROZEN, &baseline);
        if address == reference(0, 1) {
            result.unwrap();
        } else if address.pool.get() == u32::MAX {
            assert_eq!(
                result,
                Err(ConstantReferenceError::MissingPool(ConstantPoolId::new(
                    u32::MAX
                )))
            );
        } else {
            assert_eq!(
                result,
                Err(ConstantReferenceError::Constant(ConstantError::Invalid(
                    "constant ordinal is outside its pool"
                )))
            );
        }
        let trace = baseline.trace();
        assert!(trace.len() >= 2);
        assert!(trace.last().unwrap().1 > 0);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..trace.len() {
                let control = Control::at(Some((at, cause.clone())));
                assert_eq!(
                    gate(&fragment, &pools, PlanLimits::FROZEN, &control),
                    Err(ConstantReferenceError::Control(cause.clone()))
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
    let wide = lists(320);
    let pools = table(&wide, &[0]);
    let (fragment, _) = collection_fragment(
        &[UnpivotConstant::Int32List(reference(0, 1))],
        wide.value_type(),
    );
    let baseline = Control::good();
    gate(&fragment, &pools, PlanLimits::FROZEN, &baseline).unwrap();
    let trace = baseline.trace();
    let quantum = trace
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("real selected items cross the quantum");
    for at in [0, quantum, trace.len() - 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::at(Some((at, cause.clone())));
            assert_eq!(
                gate(&fragment, &pools, PlanLimits::FROZEN, &control),
                Err(ConstantReferenceError::Control(cause))
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn actual_plan_extraction_publishes_direct_collection_closure_without_scalar_placeholders() {
    let backing = lists(2);
    let pools = table(&backing, &[0, u32::MAX]);
    let (fragment, _) = collection_fragment(
        &[
            UnpivotConstant::Int32List(reference(0, 1)),
            UnpivotConstant::Int32List(reference(u32::MAX, 2)),
        ],
        backing.value_type(),
    );
    assert!(
        fragment
            .expressions()
            .iter()
            .all(|(_, node)| !matches!(node.kind, crate::ExprKind::Constant(_)))
    );
    let plan = plan_builder(std::slice::from_ref(&fragment), pools.clone())
        .finish_observed(&Control::good())
        .unwrap();
    let input = package_input(fragment.clone(), pools);
    let uses = BTreeMap::from([(fragment.id(), input.expression_uses)]);
    let calls = BTreeMap::from([(fragment.id(), input.calls)]);
    let pruning = BTreeMap::from([(fragment.id(), input.pruning)]);
    let extract = |control: &Control| {
        crate::extract_fragment_packages(
            &plan,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &uses,
            &calls,
            &pruning,
            &package_admissions(&plan),
            control,
        )
    };
    let control = Control::good();
    let packages = extract(&control).unwrap();
    let package = &packages[&fragment.id()];
    assert_eq!(
        package
            .constants()
            .entries()
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [ConstantPoolId::new(0), ConstantPoolId::new(u32::MAX)]
    );
    for (id, ordinal, expected) in [(0, 1, -2), (u32::MAX, 2, 11)] {
        let source = completed(&Control::good(), |work| {
            package
                .constants()
                .resolve_source_observed(reference(id, ordinal), work)
        })
        .unwrap();
        assert!(Arc::ptr_eq(source.pool().array(), backing.array()));
        assert!(Arc::ptr_eq(source.pool().field_ref(), backing.field_ref()));
        assert_eq!(
            source
                .int32_list_observed(CompilePhase::Validate, &Control::good())
                .unwrap()
                .unwrap()
                .item(0, CompilePhase::Validate, &Control::good())
                .unwrap(),
            Some(expected)
        );
    }
    let trace = control.trace();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..trace.len() {
            let control = Control::at(Some((at, cause.clone())));
            assert!(
                matches!(extract(&control), Err(crate::FragmentPackageExtractionError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
