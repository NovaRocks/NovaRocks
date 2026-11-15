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

use super::FunctionValueType;
use crate::{ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime};
use arrow_schema::{DataType, Field};
use std::sync::Arc;

fn list(domain: ValueLogicalType, root_nullable: bool, child_nullable: bool) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let child =
        FunctionValueType::try_with_logical_type(DataType::Utf8, child_nullable, domain).unwrap();
    let value = ValueTypeRef::try_new(
        ValueKind::List(child.value_type().clone()),
        root_nullable,
        storage.clone(),
    )
    .unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, child_nullable))),
        storage,
    )
    .unwrap();
    FunctionValueType::try_from_value(value, layout).unwrap()
}

#[test]
fn nested_nominal_identity_survives_normalized_empty_metadata() {
    let json = list(ValueLogicalType::Json, false, false);
    let text = list(ValueLogicalType::Physical, false, false);
    assert_eq!(json.data_type(), text.data_type());
    assert!(!json.same_semantics(&text));
    assert!(!json.same_value_domain(&text));
    assert!(!json.fits_value_type(&text));
    assert_eq!(
        json.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    let DataType::List(item) = json.data_type() else {
        unreachable!()
    };
    assert!(item.metadata().is_empty());
}

#[test]
fn nullable_projection_changes_only_selected_root() {
    let original = list(ValueLogicalType::Json, false, false);
    let widened = original.with_nullable(true).unwrap();
    assert!(!original.nullable());
    assert!(widened.nullable());
    assert!(!widened.value_type().child_type_at(0).unwrap().nullable());
    assert!(original.same_value_domain(&widened));
    assert!(original.fits_value_type(&widened));
    assert!(!widened.fits_value_type(&original));
    assert!(!original.same_value_domain(&list(ValueLogicalType::Json, true, true)));
}

#[test]
fn declared_layout_constructor_does_not_read_nested_annotations() {
    let source = DataType::List(Arc::new(
        Field::new("provider_item", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                crate::NR_LOGICAL_TYPE_KEY.into(),
                "unknown-tag".into(),
            )]),
        ),
    ));
    let physical =
        FunctionValueType::try_with_logical_type(source, false, ValueLogicalType::Physical)
            .unwrap();
    assert_eq!(
        physical.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Physical
    );
    let DataType::List(item) = physical.data_type() else {
        unreachable!()
    };
    assert_eq!(item.name(), "item");
    assert!(item.metadata().is_empty());
}

#[test]
fn observed_clone_reports_only_two_handle_operations_and_first_refusal() {
    let value = list(ValueLogicalType::Json, false, true);
    let mut calls = 0;
    let cloned = value
        .clone_observed(|| {
            calls += 1;
            Ok::<(), &'static str>(())
        })
        .unwrap();
    assert_eq!(calls, 2);
    assert_eq!(value, cloned);
    for stop in [1, 2] {
        let mut calls = 0;
        let result = value.clone_observed(|| {
            calls += 1;
            if calls == stop {
                Err("original refusal")
            } else {
                Ok(())
            }
        });
        assert_eq!(result.unwrap_err(), "original refusal");
        assert_eq!(calls, stop);
    }
}

#[test]
fn borrowed_child_comparison_observes_first_refusal_without_projection() {
    let left = list(ValueLogicalType::Json, false, false);
    let right = list(ValueLogicalType::Json, true, true);
    let mut calls = 0;
    let result = FunctionValueType::borrowed_value_domains_equal_observed::<crate::ValueTypeError>(
        left.value_type().child_type_at(0).unwrap(),
        right.value_type().child_type_at(0).unwrap(),
        || {
            calls += 1;
            Err(crate::ValueTypeError::InvalidType("original refusal"))
        },
    );
    assert_eq!(
        result,
        Err(crate::ValueTypeError::InvalidType("original refusal"))
    );
    assert_eq!(calls, 1);
}

#[test]
fn checked_replay_exactness_accepts_callback_only_error_domain() {
    let value = list(ValueLogicalType::Json, true, false);
    let cloned = value.clone();
    assert!(
        value
            .exactly_equals_checked_observed(&cloned, || Ok::<(), &'static str>(()))
            .unwrap()
    );
    assert!(
        !value
            .exactly_equals_checked_observed(
                &list(ValueLogicalType::Physical, true, false),
                || Ok::<(), &'static str>(())
            )
            .unwrap()
    );
    let mut calls = 0;
    let result = value.exactly_equals_checked_observed(&cloned, || {
        calls += 1;
        Err("original replay refusal")
    });
    assert_eq!(result, Err("original replay refusal"));
    assert_eq!(calls, 1);
}

#[test]
fn physical_binding_comparison_ignores_only_root_nullability_and_keeps_original_refusal() {
    let original = list(ValueLogicalType::Json, false, false);
    for (other, expected) in [
        (list(ValueLogicalType::Json, true, false), true),
        (list(ValueLogicalType::Json, false, true), false),
        (list(ValueLogicalType::Physical, false, false), false),
    ] {
        let mut calls = 0;
        assert_eq!(
            original
                .same_physical_binding_ignoring_root_nullability_observed(&other, || {
                    calls += 1;
                    Ok::<(), &'static str>(())
                })
                .unwrap(),
            expected
        );
        for stop in 1..=calls {
            let mut seen = 0;
            let result =
                original.same_physical_binding_ignoring_root_nullability_observed(&other, || {
                    seen += 1;
                    assert!(seen <= stop, "observation after original refusal");
                    if seen == stop {
                        Err("original refusal")
                    } else {
                        Ok(())
                    }
                });
            assert_eq!(result, Err("original refusal"));
            assert_eq!(seen, stop);
        }
    }
    let narrow = list(ValueLogicalType::Physical, false, true);
    let wide = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
        true,
    );
    assert!(narrow.same_value_domain(&wide));
    assert!(
        !narrow
            .same_physical_binding_ignoring_root_nullability_observed(&wide, || Ok::<
                (),
                &'static str,
            >(()))
            .unwrap()
    );
}

fn nested_nullability_tree(nullable: bool) -> FunctionValueType {
    let map = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new(
                        "value",
                        DataType::List(Arc::new(Field::new("item", DataType::Int64, nullable))),
                        nullable,
                    )),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let union = DataType::Union(
        arrow_schema::UnionFields::try_new(
            [3],
            [Arc::new(Field::new("variant", DataType::Utf8, nullable))],
        )
        .unwrap(),
        arrow_schema::UnionMode::Dense,
    );
    FunctionValueType::new(
        DataType::Struct(
            vec![
                Arc::new(Field::new(
                    "payload",
                    DataType::List(Arc::new(Field::new("item", map, nullable))),
                    nullable,
                )),
                Arc::new(Field::new("choice", union, nullable)),
            ]
            .into(),
        ),
        nullable,
    )
}

#[test]
fn complete_binding_comparison_can_ignore_only_nullability_at_all_levels() {
    let required = nested_nullability_tree(false);
    let nullable = nested_nullability_tree(true);
    for (left, right) in [(&required, &nullable), (&nullable, &required)] {
        assert!(
            left.same_physical_binding_ignoring_nullability_observed(right, || {
                Ok::<(), &'static str>(())
            })
            .unwrap()
        );
        assert!(
            left.typed_binding()
                .same_physical_binding_ignoring_nullability_observed(
                    right.typed_binding(),
                    || Ok::<(), &'static str>(()),
                )
                .unwrap()
        );
        assert!(
            left.typed_binding()
                .borrowed()
                .same_physical_binding_ignoring_nullability_observed(
                    right.typed_binding().borrowed(),
                    || Ok::<(), &'static str>(()),
                )
                .unwrap()
        );
        assert!(
            !left
                .exactly_equals_checked_observed(right, || Ok::<(), &'static str>(()))
                .unwrap()
        );
        assert!(
            !left
                .same_physical_binding_ignoring_root_nullability_observed(right, || {
                    Ok::<(), &'static str>(())
                })
                .unwrap()
        );
    }
}

#[test]
fn ignoring_nullability_keeps_exact_layout_after_matching_logical_domains() {
    use arrow_schema::{TimeUnit, UnionFields, UnionMode};
    let item = || Arc::new(Field::new("item", DataType::Utf8, true));
    let map = |sorted| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            sorted,
        )
    };
    let union = |mode| {
        DataType::Union(
            UnionFields::try_new([3], [Field::new("v", DataType::Utf8, true)]).unwrap(),
            mode,
        )
    };
    // Each pair has the same complete logical domain. The rejected difference
    // belongs only to its frozen plain layout and cannot be inferred from it.
    for (left, right) in [
        (DataType::Utf8, DataType::LargeUtf8),
        (DataType::Utf8, DataType::Utf8View),
        (DataType::Binary, DataType::LargeBinary),
        (DataType::List(item()), DataType::LargeList(item())),
        (DataType::List(item()), DataType::ListView(item())),
        (map(false), map(true)),
        (union(UnionMode::Dense), union(UnionMode::Sparse)),
    ] {
        let left = FunctionValueType::new(left, false);
        let right = FunctionValueType::new(right, true);
        assert!(left.same_semantics_ignoring_nullability(&right));
        assert!(
            !left
                .same_physical_binding_ignoring_nullability_observed(&right, || {
                    Ok::<(), &'static str>(())
                })
                .unwrap(),
            "layout differs: {:?} and {:?}",
            left.data_type(),
            right.data_type()
        );
    }
    for (left, right) in [
        (DataType::FixedSizeBinary(4), DataType::FixedSizeBinary(8)),
        (
            DataType::FixedSizeList(item(), 2),
            DataType::FixedSizeList(item(), 3),
        ),
        (DataType::Decimal128(12, 2), DataType::Decimal128(13, 2)),
        (DataType::Decimal128(12, 2), DataType::Decimal128(12, 3)),
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ),
    ] {
        let left = FunctionValueType::new(left, false);
        let right = FunctionValueType::new(right, true);
        assert!(
            !left
                .same_physical_binding_ignoring_nullability_observed(&right, || {
                    Ok::<(), &'static str>(())
                })
                .unwrap()
        );
    }
}

#[test]
fn ignoring_nullability_keeps_nominal_domains_and_named_field_identity() {
    let json = list(ValueLogicalType::Json, false, false);
    let text = list(ValueLogicalType::Physical, true, true);
    assert!(
        !json
            .same_physical_binding_ignoring_nullability_observed(&text, || {
                Ok::<(), &'static str>(())
            })
            .unwrap()
    );
    let structure = |names: [&str; 2], nullable| {
        FunctionValueType::new(
            DataType::Struct(
                names
                    .map(|name| Arc::new(Field::new(name, DataType::Int64, nullable)))
                    .into(),
            ),
            nullable,
        )
    };
    let original = structure(["first", "second"], false);
    for other in [
        structure(["renamed", "second"], true),
        structure(["second", "first"], true),
    ] {
        assert!(
            !original
                .same_physical_binding_ignoring_nullability_observed(&other, || {
                    Ok::<(), &'static str>(())
                })
                .unwrap()
        );
    }
    let union = |id, name, nullable| {
        FunctionValueType::new(
            DataType::Union(
                arrow_schema::UnionFields::try_new(
                    [id],
                    [Field::new(name, DataType::Int64, nullable)],
                )
                .unwrap(),
                arrow_schema::UnionMode::Dense,
            ),
            nullable,
        )
    };
    let original = union(3, "first", false);
    for other in [union(4, "first", true), union(3, "second", true)] {
        assert!(
            !original
                .same_physical_binding_ignoring_nullability_observed(&other, || {
                    Ok::<(), &'static str>(())
                })
                .unwrap()
        );
    }
}

#[test]
fn ignoring_all_nullability_preserves_first_refusal_through_semantics_and_layout() {
    let original = nested_nullability_tree(false);
    let other = nested_nullability_tree(true);
    let mut calls = 0;
    assert!(
        original
            .typed_binding()
            .borrowed()
            .same_physical_binding_ignoring_nullability_observed(
                other.typed_binding().borrowed(),
                || {
                    calls += 1;
                    Ok::<(), &'static str>(())
                },
            )
            .unwrap()
    );
    for stop in 1..=calls {
        let mut seen = 0;
        let result = original
            .typed_binding()
            .borrowed()
            .same_physical_binding_ignoring_nullability_observed(
                other.typed_binding().borrowed(),
                || {
                    seen += 1;
                    assert!(seen <= stop, "observation after original refusal");
                    if seen == stop {
                        Err("original refusal")
                    } else {
                        Ok(())
                    }
                },
            );
        assert_eq!(result, Err("original refusal"));
        assert_eq!(seen, stop);
    }
}

fn checked_write_projection<'a>(
    source: &'a FunctionValueType,
    input: &'a FunctionValueType,
    storage: Option<&'a FunctionValueType>,
) -> Option<crate::CheckedWriteInputProjection<'a>> {
    crate::CheckedWriteInputProjection::try_new_observed(
        source.typed_binding().borrowed(),
        input.typed_binding().borrowed(),
        storage.map(|value| value.typed_binding().borrowed()),
        || Ok::<_, std::convert::Infallible>(()),
    )
    .unwrap()
}

#[test]
fn write_projection_needs_the_corresponding_storage_fixed_leaf() {
    let source = FunctionValueType::new(DataType::FixedSizeBinary(3), true);
    let input = FunctionValueType::new(DataType::Binary, false);
    let storage = FunctionValueType::new(DataType::FixedSizeBinary(4), false);
    let proof = checked_write_projection(&source, &input, Some(&storage)).unwrap();
    assert_eq!(
        proof.leaf_projection(),
        crate::WriteInputLeafProjection::FixedBytesToBinary { source_width: 3 }
    );
    assert!(std::ptr::eq(
        proof.source().value_type(),
        source.value_type()
    ));
    assert!(std::ptr::eq(proof.input().value_type(), input.value_type()));
    assert!(std::ptr::eq(
        proof.storage().unwrap().value_type(),
        storage.value_type()
    ));
    assert!(proof.child_at(0).is_none());
    assert!(checked_write_projection(&source, &input, None).is_none());
    assert!(checked_write_projection(&source, &input, Some(&input)).is_none());
    assert!(checked_write_projection(&input, &storage, Some(&input)).is_none());
    assert!(checked_write_projection(&input, &input, Some(&storage)).is_some());
    assert!(checked_write_projection(&storage, &storage, Some(&storage)).is_some());
    assert!(
        !source
            .same_physical_binding_ignoring_nullability_observed(&input, || Ok::<_, ()>(()))
            .unwrap()
    );
}

#[test]
fn write_projection_checks_each_nested_occurrence_and_all_storage_null_flags() {
    let tree = |fixed: DataType, root_null, child_null| {
        FunctionValueType::new(
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "payload",
                    DataType::List(Arc::new(Field::new("item", fixed, child_null))),
                    true,
                ))]
                .into(),
            ),
            root_null,
        )
    };
    let source = tree(DataType::FixedSizeBinary(9), true, true);
    let input = tree(DataType::Binary, false, false);
    let storage = tree(DataType::FixedSizeBinary(4), false, false);
    let proof = checked_write_projection(&source, &input, Some(&storage)).unwrap();
    let list = proof.child_at(0).unwrap();
    assert_eq!(
        list.leaf_projection(),
        crate::WriteInputLeafProjection::Exact
    );
    assert_eq!(
        list.child_at(0).unwrap().leaf_projection(),
        crate::WriteInputLeafProjection::FixedBytesToBinary { source_width: 9 }
    );
    assert!(list.child_at(1).is_none());
    assert!(
        checked_write_projection(
            &source,
            &input,
            Some(&tree(DataType::FixedSizeBinary(4), true, false))
        )
        .is_none()
    );
    assert!(
        checked_write_projection(
            &source,
            &input,
            Some(&tree(DataType::FixedSizeBinary(4), false, true))
        )
        .is_none()
    );

    // A neighbouring storage Binary field must not borrow another leaf's licence.
    let row = |first, second| {
        FunctionValueType::new(
            DataType::Struct(
                vec![
                    Arc::new(Field::new("licensed", first, false)),
                    Arc::new(Field::new("ordinary", second, false)),
                ]
                .into(),
            ),
            false,
        )
    };
    assert!(
        checked_write_projection(
            &row(DataType::FixedSizeBinary(3), DataType::FixedSizeBinary(3)),
            &row(DataType::Binary, DataType::Binary),
            Some(&row(DataType::FixedSizeBinary(4), DataType::Binary)),
        )
        .is_none()
    );
}

#[test]
fn write_projection_keeps_nominal_names_offsets_and_map_order_exact() {
    let input = FunctionValueType::new(DataType::Binary, false);
    for domain in [ValueLogicalType::Uuid, ValueLogicalType::LargeInt] {
        let nominal =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), false, domain)
                .unwrap();
        let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
        assert!(checked_write_projection(&nominal, &input, Some(&physical)).is_none());
        assert!(checked_write_projection(&physical, &input, Some(&nominal)).is_none());
    }
    let shape = |name: &str, leaf| {
        FunctionValueType::new(
            DataType::Struct(vec![Arc::new(Field::new(name, leaf, false))].into()),
            false,
        )
    };
    assert!(
        checked_write_projection(
            &shape("changed", DataType::FixedSizeBinary(3)),
            &shape("original", DataType::Binary),
            Some(&shape("original", DataType::FixedSizeBinary(4)))
        )
        .is_none()
    );
    let item = |leaf| Arc::new(Field::new("item", leaf, true));
    let source = FunctionValueType::new(
        DataType::LargeList(item(DataType::FixedSizeBinary(3))),
        false,
    );
    let target = FunctionValueType::new(DataType::List(item(DataType::Binary)), false);
    let storage = FunctionValueType::new(DataType::List(item(DataType::FixedSizeBinary(4))), false);
    assert!(checked_write_projection(&source, &target, Some(&storage)).is_none());
    let map = |leaf, sorted| {
        FunctionValueType::new(
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Int64, false)),
                            Arc::new(Field::new("value", leaf, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                sorted,
            ),
            false,
        )
    };
    let storage = map(DataType::FixedSizeBinary(4), false);
    let input = map(DataType::Binary, false);
    let source = map(DataType::FixedSizeBinary(3), false);
    let proof = checked_write_projection(&source, &input, Some(&storage)).unwrap();
    assert_eq!(
        proof.child_at(0).unwrap().leaf_projection(),
        crate::WriteInputLeafProjection::Exact
    );
    assert_eq!(
        proof.child_at(1).unwrap().leaf_projection(),
        crate::WriteInputLeafProjection::FixedBytesToBinary { source_width: 3 }
    );
    assert!(
        checked_write_projection(
            &map(DataType::FixedSizeBinary(3), true),
            &input,
            Some(&storage)
        )
        .is_none()
    );
}

#[test]
fn write_projection_exact_mode_reuses_only_the_original_null_relation() {
    let original = list(ValueLogicalType::Json, false, false);
    let nullable = list(ValueLogicalType::Json, true, true);
    assert!(checked_write_projection(&nullable, &original, None).is_some());
    let different = list(ValueLogicalType::Physical, true, true);
    assert!(checked_write_projection(&different, &original, None).is_none());
    let source = FunctionValueType::new(DataType::Decimal128(9, 2), false);
    let input = FunctionValueType::new(DataType::Decimal128(9, 3), false);
    assert!(checked_write_projection(&source, &input, Some(&input)).is_none());
}

#[test]
fn write_projection_observer_refusal_has_no_tail_and_needs_no_error_conversion() {
    let source = FunctionValueType::new(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::FixedSizeBinary(3),
            true,
        ))),
        true,
    );
    let input = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Binary, false))),
        false,
    );
    let storage = FunctionValueType::new(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::FixedSizeBinary(4),
            false,
        ))),
        false,
    );
    let mut count = 0;
    assert!(
        crate::CheckedWriteInputProjection::try_new_observed(
            source.typed_binding().borrowed(),
            input.typed_binding().borrowed(),
            Some(storage.typed_binding().borrowed()),
            || {
                count += 1;
                Ok::<_, &'static str>(())
            }
        )
        .unwrap()
        .is_some()
    );
    for stop in 1..=count {
        let mut calls = 0;
        let failure = crate::CheckedWriteInputProjection::try_new_observed(
            source.typed_binding().borrowed(),
            input.typed_binding().borrowed(),
            Some(storage.typed_binding().borrowed()),
            || {
                calls += 1;
                assert!(calls <= stop, "callback after original refusal");
                if calls == stop {
                    Err("original projection refusal")
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(failure, Err("original projection refusal")));
        assert_eq!(calls, stop);
    }
}

#[test]
fn write_projection_never_unwraps_other_byte_layouts_or_union_identity() {
    let input = FunctionValueType::new(DataType::Binary, false);
    let storage = FunctionValueType::new(DataType::FixedSizeBinary(4), false);
    for carrier in [DataType::LargeBinary, DataType::BinaryView] {
        let source = FunctionValueType::new(carrier, false);
        assert!(checked_write_projection(&source, &input, Some(&storage)).is_none());
    }
    let union = |id, ty| {
        FunctionValueType::new(
            DataType::Union(
                arrow_schema::UnionFields::try_new(
                    [id],
                    [Arc::new(Field::new("payload", ty, false))],
                )
                .unwrap(),
                arrow_schema::UnionMode::Dense,
            ),
            false,
        )
    };
    assert!(
        checked_write_projection(
            &union(7, DataType::FixedSizeBinary(3)),
            &union(8, DataType::Binary),
            Some(&union(8, DataType::FixedSizeBinary(4))),
        )
        .is_none()
    );
}
