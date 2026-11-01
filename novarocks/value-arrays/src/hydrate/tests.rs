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
use crate::{ByteLayout, arrow_raw, decode::ValueConversionContext};
use arrow_array::{
    DictionaryArray, FixedSizeListArray, Int32Array, LargeListArray, LargeStringArray, ListArray,
    MapArray, StringArray, StringViewArray, StructArray, types::Int32Type,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{Field, Fields};
use novarocks_failure_contract::RepresentationBoundary;
use novarocks_type_contract::{
    ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};
use std::{convert::Infallible, sync::Arc};

fn binding(target: DataType, nullable: bool) -> TypedValueBinding {
    let lifetime = ValueTypeStorageLifetime::pure();
    TypedValueBinding::try_new(
        ValueTypeRef::try_from_declared_arrow(
            &target,
            nullable,
            ValueLogicalType::Physical,
            lifetime.clone(),
        )
        .unwrap(),
        ValueLayout::try_new(&target, lifetime).unwrap(),
    )
    .unwrap()
}
fn dictionary(values: Vec<Option<&str>>, keys: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(keys),
            Arc::new(StringArray::from(values)),
        )
        .unwrap(),
    )
}
// ArrayData validation admits required dictionary fields whose physical keys
// have no NULL, while parent masks determine which logical values are required.
// Use the shared safe rebox adapter to author that existing carrier contract.
fn rebox(source: &ArrayRef, target: &DataType) -> ArrayRef {
    let children = match arrow_raw::structural_view(source).unwrap() {
        arrow_raw::StructuralRef::Struct { children, .. } => children.to_vec(),
        arrow_raw::StructuralRef::List32 { values, .. }
        | arrow_raw::StructuralRef::List64 { values, .. }
        | arrow_raw::StructuralRef::FixedSizeList { values, .. } => vec![values.clone()],
        arrow_raw::StructuralRef::Map { keys, values, .. } => vec![keys.clone(), values.clone()],
        arrow_raw::StructuralRef::Union { fields, array } => fields
            .iter()
            .map(|(id, _)| array.child(id).clone())
            .collect(),
        arrow_raw::StructuralRef::Leaf => panic!("container"),
    };
    arrow_raw::rebuild_structural(source, target, children).unwrap()
}
fn plan(source: &ArrayRef, binding: &TypedValueBinding) -> HydratePlan {
    HydratePlan::plan_observed(
        source,
        binding,
        &mut ValueConversionContext::default(),
        &mut |_, _| false,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .unwrap()
}
fn execute(plan: &HydratePlan, ctx: &mut ValueConversionContext) -> ArrayRef {
    plan.require_complete().unwrap();
    plan.execute_observed(
        ctx,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .unwrap()
}
fn strings(array: &ArrayRef) -> Vec<Option<String>> {
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        values.iter().map(|s| s.map(str::to_owned)).collect()
    } else if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        values.iter().map(|s| s.map(str::to_owned)).collect()
    } else {
        array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .iter()
            .map(|s| s.map(str::to_owned))
            .collect()
    }
}
#[test]
fn frozen_view_direct_and_shared_context_keep_key_and_value_nulls() {
    let input = dictionary(
        vec![None, Some("short"), Some("a longer adopted string")],
        vec![Some(0), None, Some(2), Some(1), Some(2)],
    );
    let binding = binding(DataType::Utf8View, true);
    let expected = vec![
        None,
        None,
        Some("a longer adopted string".to_owned()),
        Some("short".to_owned()),
        Some("a longer adopted string".to_owned()),
    ];
    let mut ctx = ValueConversionContext::new(1024 * 1024);
    let planned = HydratePlan::plan_observed(
        &input,
        &binding,
        &mut ctx,
        &mut |_, _| false,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .unwrap();
    let first = execute(&planned, &mut ctx);
    let again = HydratePlan::plan_observed(
        &input,
        &binding,
        &mut ctx,
        &mut |_, _| false,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .unwrap();
    let second = execute(&again, &mut ctx);
    assert_eq!(strings(&first), expected);
    assert_eq!(strings(&second), expected);
    assert_eq!(first.data_type(), &DataType::Utf8View);
    first.to_data().validate_full().unwrap();
    second.to_data().validate_full().unwrap();
    assert!(ctx.stats().cache_hits > 0);
    ctx.clear();
    assert_eq!(strings(&first), expected);
}
#[test]
fn large_offsets_target_uses_native_source_without_populating_view_cache() {
    let input: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(1), None, Some(0)]),
            Arc::new(LargeStringArray::from(vec![
                Some("longer original value"),
                Some("x"),
            ])),
        )
        .unwrap(),
    );
    let mut ctx = ValueConversionContext::default();
    let output = execute(&plan(&input, &binding(DataType::LargeUtf8, true)), &mut ctx);
    assert_eq!(
        strings(&output),
        vec![
            Some("x".to_owned()),
            None,
            Some("longer original value".to_owned())
        ]
    );
    assert_eq!(ctx.storage_facts().owned_descriptor_bytes, 0);
    assert_eq!(output.data_type(), &DataType::LargeUtf8);
}
#[test]
fn required_dictionary_under_null_struct_is_empty_and_reachable_null_is_rejected() {
    let child = dictionary(
        vec![None, Some("first"), Some("second")],
        vec![Some(0), Some(1), Some(2)],
    );
    let fields: Fields = vec![Arc::new(Field::new(
        "name",
        child.data_type().clone(),
        false,
    ))]
    .into();
    let input: ArrayRef = Arc::new(
        StructArray::try_new(
            fields,
            vec![child],
            Some(NullBuffer::from(vec![false, true, true])),
        )
        .unwrap(),
    );
    let target =
        DataType::Struct(vec![Arc::new(Field::new("name", DataType::Utf8View, false))].into());
    let output = execute(
        &plan(&input, &binding(target.clone(), true)),
        &mut ValueConversionContext::default(),
    );
    let fields = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        strings(fields.column(0)),
        vec![
            Some("".to_owned()),
            Some("first".to_owned()),
            Some("second".to_owned())
        ]
    );
    assert_eq!(fields.column(0).null_count(), 0);
    output.to_data().validate_full().unwrap();
    let invalid: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new(
                "name",
                input
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column(0)
                    .data_type()
                    .clone(),
                true,
            ))]
            .into(),
            vec![
                input
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column(0)
                    .clone(),
            ],
            None,
        )
        .unwrap(),
    );
    let actual_child = invalid
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0);
    let invalid = rebox(
        &invalid,
        &DataType::Struct(
            vec![Arc::new(Field::new(
                "name",
                actual_child.data_type().clone(),
                false,
            ))]
            .into(),
        ),
    );
    let result = HydratePlan::plan_observed(
        &invalid,
        &binding(target, true),
        &mut ValueConversionContext::default(),
        &mut |_, _| false,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    );
    assert!(matches!(
        result,
        Err(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::RequiredNull
        )))
    ));
}
#[test]
fn list_and_large_list_slices_keep_offset_domains_and_required_placeholders() {
    for large in [false, true] {
        let values = dictionary(
            vec![None, Some("visible")],
            vec![Some(0), Some(0), Some(0), Some(1), Some(0)],
        );
        let field = Arc::new(Field::new("item", values.data_type().clone(), true));
        let source: ArrayRef = if large {
            Arc::new(
                LargeListArray::try_new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(vec![1i64, 3, 4, 5])),
                    values,
                    Some(NullBuffer::from(vec![false, true, false])),
                )
                .unwrap(),
            )
        } else {
            Arc::new(
                ListArray::try_new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(vec![1i32, 3, 4, 5])),
                    values,
                    Some(NullBuffer::from(vec![false, true, false])),
                )
                .unwrap(),
            )
        };
        let physical_child = match arrow_raw::structural_view(&source).unwrap() {
            arrow_raw::StructuralRef::List32 { values, .. }
            | arrow_raw::StructuralRef::List64 { values, .. } => values,
            _ => panic!("list"),
        };
        let physical_field = Arc::new(Field::new(
            "item",
            physical_child.data_type().clone(),
            false,
        ));
        let physical_type = if large {
            DataType::LargeList(physical_field)
        } else {
            DataType::List(physical_field)
        };
        let source = rebox(&source, &physical_type).slice(0, 2);
        let target_field = Arc::new(Field::new("item", DataType::Utf8View, false));
        let target = if large {
            DataType::LargeList(target_field)
        } else {
            DataType::List(target_field)
        };
        let output = execute(
            &plan(&source, &binding(target, true)),
            &mut ValueConversionContext::default(),
        );
        let child = if large {
            output
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .values()
        } else {
            output
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
        };
        assert_eq!(
            strings(child),
            vec![
                Some("".to_owned()),
                Some("".to_owned()),
                Some("".to_owned()),
                Some("visible".to_owned()),
                Some("".to_owned())
            ]
        );
        assert!(output.is_null(0));
        assert!(!output.is_null(1));
        output.to_data().validate_full().unwrap();
    }
}
#[test]
fn fixed_size_slice_uses_actual_sliced_child_coordinates() {
    let values = dictionary(
        vec![None, Some("a"), Some("b")],
        vec![Some(0), Some(0), Some(1), Some(2), Some(0), Some(0)],
    );
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    let source: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            field,
            2,
            values,
            Some(NullBuffer::from(vec![false, true, false])),
        )
        .unwrap(),
    );
    let arrow_raw::StructuralRef::FixedSizeList { values, .. } =
        arrow_raw::structural_view(&source).unwrap()
    else {
        panic!("fixed list")
    };
    let physical_type = DataType::FixedSizeList(
        Arc::new(Field::new("item", values.data_type().clone(), false)),
        2,
    );
    let source = rebox(&source, &physical_type).slice(1, 2);
    let target =
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Utf8View, false)), 2);
    let output = execute(
        &plan(&source, &binding(target, true)),
        &mut ValueConversionContext::default(),
    );
    let values = output
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap()
        .values();
    assert_eq!(
        strings(values),
        vec![
            Some("a".to_owned()),
            Some("b".to_owned()),
            Some("".to_owned()),
            Some("".to_owned())
        ]
    );
    output.to_data().validate_full().unwrap();
}
#[test]
fn map_parent_null_masks_required_value_rows_and_preserves_physical_offsets() {
    let values = dictionary(
        vec![None, Some("a"), Some("b")],
        vec![Some(0), Some(0), Some(1), Some(2)],
    );
    let fields: Fields = vec![
        Arc::new(Field::new("key", DataType::Int32, false)),
        Arc::new(Field::new("value", values.data_type().clone(), true)),
    ]
    .into();
    let entries = StructArray::try_new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])), values],
        None,
    )
    .unwrap();
    let source: ArrayRef = Arc::new(
        MapArray::try_new(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 4])),
            entries,
            Some(NullBuffer::from(vec![false, true])),
            false,
        )
        .unwrap(),
    );
    let arrow_raw::StructuralRef::Map { values, .. } = arrow_raw::structural_view(&source).unwrap()
    else {
        panic!("map")
    };
    let physical_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", values.data_type().clone(), false)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let source = rebox(&source, &physical_type);
    let target = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Utf8View, false)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let output = execute(
        &plan(&source, &binding(target, true)),
        &mut ValueConversionContext::default(),
    );
    let map = output.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 2, 4]);
    assert_eq!(
        strings(map.values()),
        vec![
            Some("".to_owned()),
            Some("".to_owned()),
            Some("a".to_owned()),
            Some("b".to_owned())
        ]
    );
    output.to_data().validate_full().unwrap();
}
#[test]
fn capabilities_are_per_path_and_receive_the_actual_carrier() {
    let one = dictionary(vec![Some("x")], vec![Some(0)]);
    let fields: Fields = vec![
        Arc::new(Field::new("left", one.data_type().clone(), true)),
        Arc::new(Field::new("right", one.data_type().clone(), true)),
    ]
    .into();
    let input: ArrayRef =
        Arc::new(StructArray::try_new(fields, vec![one.clone(), one], None).unwrap());
    let target = DataType::Struct(
        vec![
            Arc::new(Field::new("left", DataType::Utf8View, true)),
            Arc::new(Field::new("right", DataType::Utf8View, true)),
        ]
        .into(),
    );
    let planned = HydratePlan::plan_observed(
        &input,
        &binding(target, true),
        &mut ValueConversionContext::default(),
        &mut |path, carrier| {
            assert!(matches!(carrier, DataType::Dictionary(..)));
            path == [HydratePathStep::StructField(0)]
        },
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .unwrap();
    let output = execute(&planned, &mut ValueConversionContext::default());
    let fields = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(matches!(
        fields.column(0).data_type(),
        DataType::Dictionary(..)
    ));
    assert_eq!(fields.column(1).data_type(), &DataType::Utf8View);
}
#[test]
fn growth_control_and_structure_refusals_preserve_original_caller_and_source() {
    let input = dictionary(vec![Some("a longer source value")], vec![Some(0)]);
    let planned = plan(&input, &binding(DataType::Utf8View, true));
    let mut context = ValueConversionContext::default();
    let result = planned.execute_observed(
        &mut context,
        &mut |_| Err("growth"),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    );
    assert!(matches!(result, Err(ValueFailure::Caller("growth"))));
    let result = planned.execute_observed(
        &mut context,
        &mut |_| Ok(()),
        &mut |_| Err("stop"),
        &mut |_| Ok(()),
    );
    assert!(matches!(result, Err(ValueFailure::Caller("stop"))));
    let result =
        planned.execute_observed(&mut context, &mut |_| Ok(()), &mut |_| Ok(()), &mut |_| {
            Err("structure")
        });
    assert!(matches!(result, Err(ValueFailure::Caller("structure"))));
    assert!(Arc::ptr_eq(planned.source(), &input));
    assert_eq!(
        strings(&execute(&planned, &mut context)),
        vec![Some("a longer source value".to_owned())]
    );
}
#[test]
fn metadata_backing_drives_rebuild_and_unchanged_payloads_share() {
    for has_metadata in [false, true] {
        let child: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let mut metadata = std::collections::HashMap::with_capacity(8);
        metadata.insert("temporary".to_owned(), "value".to_owned());
        if !has_metadata {
            metadata.clear();
        }
        let field = Arc::new(Field::new("n", DataType::Int32, false).with_metadata(metadata));
        let source: ArrayRef =
            Arc::new(StructArray::try_new(vec![field].into(), vec![child.clone()], None).unwrap());
        let target =
            DataType::Struct(vec![Arc::new(Field::new("n", DataType::Int32, false))].into());
        let output = execute(
            &plan(&source, &binding(target.clone(), false)),
            &mut ValueConversionContext::default(),
        );
        assert_eq!(Arc::ptr_eq(&source, &output), !has_metadata);
        let result = output.as_any().downcast_ref::<StructArray>().unwrap();
        // A real annotation rebuilds the parent, while both inputs retain the
        // exact immutable child payload. Cleared staging has no backing to remove.
        let original = child.as_any().downcast_ref::<Int32Array>().unwrap();
        let rebuilt = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(original.values().as_ptr(), rebuilt.values().as_ptr());
        let DataType::Struct(fields) = output.data_type() else {
            panic!("struct")
        };
        assert!(fields[0].metadata().as_arc().is_none());
        let second = execute(
            &plan(&output, &binding(target, false)),
            &mut ValueConversionContext::default(),
        );
        assert!(Arc::ptr_eq(&second, &output));
    }
}
#[test]
fn real_field_arithmetic_preserves_large_offsets_and_complete_top_row_prefixes() {
    let limit = i32::MAX as u128;
    let facts = tree::check_extent(ByteLayout::Utf8, limit - 3, 4, 4, 7)
        .unwrap()
        .unwrap();
    assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
    assert_eq!(facts.required, Some(i32::MAX as u64 + 1));
    assert_eq!(facts.representable_prefix, Some(7));
    let facts = tree::check_extent(ByteLayout::Utf8, 0, limit + 1, limit as u64 + 1, 0)
        .unwrap()
        .unwrap();
    assert_eq!(facts.boundary, RepresentationBoundary::MinimumUnit);
    assert!(
        tree::check_extent(ByteLayout::LargeUtf8, 0, limit + 1, limit as u64 + 1, 0)
            .unwrap()
            .is_none()
    );
    assert!(
        tree::check_extent(ByteLayout::Utf8View, u128::MAX - 100, 1, 1, 0)
            .unwrap()
            .is_none()
    );
    // Unreferenced huge dictionary values do not enter any row's measurements.
    assert!(
        tree::check_extent(ByteLayout::Utf8View, 0, 2, 2, 0)
            .unwrap()
            .is_none()
    );
    let first = tree::check_extent(ByteLayout::Utf8, limit - 10, 8, 8, 3).unwrap();
    let second = tree::check_extent(ByteLayout::Utf8, limit - 2, 8, 8, 3).unwrap();
    assert!(first.is_none());
    assert_eq!(second.unwrap().representable_prefix, Some(3));
}

#[test]
fn explicit_dictionary_layout_parameter_accepts_view_bottom_without_changing_engine_default() {
    let source: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), None]),
            Arc::new(StringViewArray::from(vec![
                Some("a longer view value"),
                None,
            ])),
        )
        .unwrap(),
    );
    let binding = binding(DataType::Utf8View, true);
    assert!(
        HydratePlan::plan_observed(
            &source,
            &binding,
            &mut ValueConversionContext::default(),
            &mut |_, _| false,
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok(()),
            &mut |_| Ok(())
        )
        .is_err()
    );
    let planned = HydratePlan::plan_with_options_observed(
        &source,
        &binding,
        HydrateOptions {
            dictionary_value_layout: Some(ByteLayout::Utf8View),
            ..HydrateOptions::default()
        },
        &mut ValueConversionContext::default(),
        &mut |_, _| false,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .unwrap();
    let output = execute(&planned, &mut ValueConversionContext::default());
    assert_eq!(
        strings(&output),
        vec![Some("a longer view value".to_owned()), None, None]
    );
    output.to_data().validate_full().unwrap();
}

#[test]
fn ancestor_struct_mask_controls_required_list_descendants_even_when_list_is_nonnull() {
    let values = dictionary(
        vec![None, Some("a"), Some("b")],
        vec![Some(0), Some(0), Some(1), Some(2)],
    );
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 4])),
            values,
            None,
        )
        .unwrap(),
    );
    let arrow_raw::StructuralRef::List32 { values, .. } =
        arrow_raw::structural_view(&list).unwrap()
    else {
        panic!("list")
    };
    let list = rebox(
        &list,
        &DataType::List(Arc::new(Field::new(
            "item",
            values.data_type().clone(),
            false,
        ))),
    );
    let fields: Fields = vec![Arc::new(Field::new(
        "items",
        list.data_type().clone(),
        false,
    ))]
    .into();
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            fields,
            vec![list],
            Some(NullBuffer::from(vec![false, true])),
        )
        .unwrap(),
    );
    let target = DataType::Struct(
        vec![Arc::new(Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8View, false))),
            false,
        ))]
        .into(),
    );
    let output = execute(
        &plan(&source, &binding(target, true)),
        &mut ValueConversionContext::default(),
    );
    let root = output.as_any().downcast_ref::<StructArray>().unwrap();
    let list = root.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        strings(list.values()),
        vec![
            Some("".to_owned()),
            Some("".to_owned()),
            Some("a".to_owned()),
            Some("b".to_owned())
        ]
    );
    assert!(root.is_null(0));
    assert!(!list.is_null(0));
    output.to_data().validate_full().unwrap();
}

#[test]
fn inactive_nullable_leaf_uses_null_with_legal_empty_descriptors() {
    let values = dictionary(
        vec![Some("hidden long source value"), Some("shown")],
        vec![Some(0), Some(1)],
    );
    let fields: Fields = vec![Arc::new(Field::new("s", values.data_type().clone(), true))].into();
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            fields,
            vec![values],
            Some(NullBuffer::from(vec![false, true])),
        )
        .unwrap(),
    );
    let target = DataType::Struct(vec![Arc::new(Field::new("s", DataType::Utf8View, true))].into());
    let output = execute(
        &plan(&source, &binding(target, true)),
        &mut ValueConversionContext::default(),
    );
    let root = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        strings(root.column(0)),
        vec![None, Some("shown".to_owned())]
    );
    output.to_data().validate_full().unwrap();
}

#[test]
fn planning_refusal_and_independent_required_validation_do_not_publish_or_change_source() {
    let source = dictionary(vec![Some("original")], vec![Some(0)]);
    let frozen = binding(DataType::Utf8View, true);
    let result = HydratePlan::plan_observed(
        &source,
        &frozen,
        &mut ValueConversionContext::default(),
        &mut |_, _| false,
        &mut |_| Err("plan storage"),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    );
    assert!(matches!(result, Err(ValueFailure::Caller("plan storage"))));
    let planned = plan(&source, &frozen);
    assert_eq!(
        strings(&execute(&planned, &mut ValueConversionContext::default())),
        vec![Some("original".to_owned())]
    );
    let null = dictionary(vec![None], vec![Some(0)]);
    assert!(matches!(
        validate_required_observed(
            &null,
            &binding(DataType::Utf8View, false),
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok(())
        ),
        Err(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::RequiredNull
        )))
    ));
}

#[test]
fn frozen_fixed_size_width_is_rejected_before_reboxing() {
    let values = dictionary(vec![Some("a"), Some("b")], vec![Some(0), Some(1)]);
    let source: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            2,
            values,
            None,
        )
        .unwrap(),
    );
    let frozen = binding(
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Utf8View, true)), 1),
        true,
    );
    let result = HydratePlan::plan_observed(
        &source,
        &frozen,
        &mut ValueConversionContext::default(),
        &mut |_, _| false,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    );
    assert!(matches!(
        result,
        Err(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::Layout
        )))
    ));
    assert_eq!(source.len(), 1);
}

#[test]
fn masked_required_null_key_is_not_dereferenced_and_becomes_valid_empty() {
    let values = dictionary(vec![Some("kept")], vec![None, Some(0)]);
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new("s", values.data_type().clone(), false))].into(),
            vec![values],
            Some(NullBuffer::from(vec![false, true])),
        )
        .unwrap(),
    );
    let frozen = binding(
        DataType::Struct(vec![Arc::new(Field::new("s", DataType::Utf8View, false))].into()),
        true,
    );
    let output = execute(
        &plan(&source, &frozen),
        &mut ValueConversionContext::default(),
    );
    let root = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        strings(root.column(0)),
        vec![Some("".to_owned()), Some("kept".to_owned())]
    );
    output.to_data().validate_full().unwrap();
}

fn projection_plan(
    source: &ArrayRef,
    target: &TypedValueBinding,
) -> Result<HydratePlan, ValueFailure<Infallible>> {
    HydratePlan::plan_with_options_observed(
        source,
        target,
        HydrateOptions {
            projection: HydrateProjection::FrozenNamesAndNullability,
            ..HydrateOptions::default()
        },
        &mut ValueConversionContext::default(),
        &mut |_, _| false,
        &mut |_| Ok(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
}
#[test]
fn explicit_signed_projection_preserves_values_and_strips_only_inactive_scalar_nulls() {
    let child: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new("old", DataType::Int32, true))].into(),
            vec![child],
            Some(NullBuffer::from(vec![true, false])),
        )
        .unwrap(),
    );
    let target = binding(
        DataType::Struct(vec![Arc::new(Field::new("new", DataType::Int32, false))].into()),
        true,
    );
    assert!(
        HydratePlan::plan_observed(
            &source,
            &target,
            &mut ValueConversionContext::default(),
            &mut |_, _| false,
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok(()),
            &mut |_| Ok(())
        )
        .is_err()
    );
    let output = execute(
        &projection_plan(&source, &target).unwrap(),
        &mut ValueConversionContext::default(),
    );
    let result = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(result.column(0).null_count(), 0);
    assert!(result.is_null(1));
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(output.data_type(), target.data_type());
    output.to_data().validate_full().unwrap();
    let invalid: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new("old", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![None::<i32>]))],
            None,
        )
        .unwrap(),
    );
    assert!(matches!(
        projection_plan(&invalid, &target),
        Err(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::RequiredNull
        )))
    ));
}
#[test]
fn signed_projection_list_required_byte_and_scalar_placeholders_share_reachability() {
    for string in [false, true] {
        let child: ArrayRef = if string {
            Arc::new(StringViewArray::from(vec![None, Some("shown")]))
        } else {
            Arc::new(Int32Array::from(vec![None, Some(8)]))
        };
        let leaf = child.data_type().clone();
        let source: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("old", leaf.clone(), true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
                child,
                Some(NullBuffer::from(vec![false, true])),
            )
            .unwrap(),
        );
        let frozen = binding(
            DataType::List(Arc::new(Field::new("item", leaf, false))),
            true,
        );
        let output = execute(
            &projection_plan(&source, &frozen).unwrap(),
            &mut ValueConversionContext::default(),
        );
        let list = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.is_null(0));
        assert_eq!(list.values().null_count(), 0);
        if string {
            assert_eq!(
                strings(list.values()),
                vec![Some("".to_owned()), Some("shown".to_owned())]
            );
        } else {
            assert_eq!(
                list.values()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(1),
                8
            );
        }
        output.to_data().validate_full().unwrap();
    }
}
#[test]
fn signed_projection_keeps_scalar_parameters_and_fixed_widths_exact() {
    let source: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![1]));
    assert!(projection_plan(&source, &binding(DataType::Int32, true)).is_err());
    let source: ArrayRef = Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_iter([b"abcd".as_slice()].into_iter()).unwrap(),
    );
    assert!(projection_plan(&source, &binding(DataType::Binary, true)).is_err());
}
#[test]
fn second_leaf_admission_refusal_precedes_every_output_materialization() {
    let child = dictionary(vec![Some("a longer payload")], vec![Some(0)]);
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![
                Arc::new(Field::new("left", child.data_type().clone(), true)),
                Arc::new(Field::new("right", child.data_type().clone(), true)),
            ]
            .into(),
            vec![child.clone(), child],
            None,
        )
        .unwrap(),
    );
    let frozen = binding(
        DataType::Struct(
            vec![
                Arc::new(Field::new("left", DataType::Utf8, true)),
                Arc::new(Field::new("right", DataType::Utf8, true)),
            ]
            .into(),
        ),
        true,
    );
    let planned = plan(&source, &frozen);
    let first = &planned.root.children[0];
    let first_count = crate::build::ValueBuilder::<allocator_api2::alloc::Global>::planned_layouts(
        ByteLayout::Utf8,
        first.source.len(),
        first.planned_payload,
        first.planned_regions,
    )
    .unwrap()
    .len();
    let mut requested = 0;
    let mut output_operations = 0;
    let result = planned.execute_observed(
        &mut ValueConversionContext::default(),
        &mut |_| {
            requested += 1;
            if requested == first_count + 1 {
                Err("second leaf")
            } else {
                Ok(())
            }
        },
        &mut |_| Ok(()),
        &mut |event| {
            if matches!(
                event,
                HydrateEvent::RebuildArray { .. }
                    | HydrateEvent::ValueStructure {
                        event: crate::control::ValueStructure::ByteArray { .. },
                        ..
                    }
            ) {
                output_operations += 1;
            }
            Ok(())
        },
    );
    assert!(matches!(result, Err(ValueFailure::Caller("second leaf"))));
    assert_eq!(output_operations, 0);
    assert!(Arc::ptr_eq(planned.source(), &source));
    let output = execute(&planned, &mut ValueConversionContext::default());
    let result = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        strings(result.column(0)),
        vec![Some("a longer payload".to_owned())]
    );
}

#[test]
fn repeated_dictionary_plans_real_utf8_prefix_without_allocating_logical_payload() {
    const ROWS: usize = 524_288;
    const BYTES: usize = 4096;
    let source: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; ROWS]),
            Arc::new(StringArray::from(vec!["x".repeat(BYTES)])),
        )
        .unwrap(),
    );
    assert!(source.get_buffer_memory_size() < 3 * 1024 * 1024);
    let planned = plan(&source, &binding(DataType::Utf8, true));
    assert_eq!(planned.rows(), (i32::MAX as usize) / BYTES);
    let facts = planned
        .require_complete()
        .unwrap_err()
        .representation()
        .unwrap();
    assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
    assert_eq!(facts.required, Some(2_147_483_648));
    assert_eq!(facts.extent_limit, Some(i32::MAX as u64));
    assert_eq!(facts.representable_prefix, Some(524_287));
    assert!(Arc::ptr_eq(planned.source(), &source));
}
#[test]
fn child_payload_limits_choose_one_common_complete_top_level_prefix() {
    const ROWS: usize = 262_144;
    let child = |bytes: usize| -> ArrayRef {
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0; ROWS]),
                Arc::new(StringArray::from(vec!["x".repeat(bytes)])),
            )
            .unwrap(),
        )
    };
    let left = child(4096);
    let right = child(8192);
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![
                Arc::new(Field::new("left", left.data_type().clone(), true)),
                Arc::new(Field::new("right", right.data_type().clone(), true)),
            ]
            .into(),
            vec![left, right],
            None,
        )
        .unwrap(),
    );
    let frozen = binding(
        DataType::Struct(
            vec![
                Arc::new(Field::new("left", DataType::Utf8, true)),
                Arc::new(Field::new("right", DataType::Utf8, true)),
            ]
            .into(),
        ),
        true,
    );
    let planned = plan(&source, &frozen);
    assert_eq!(planned.rows(), 262_143);
    assert_eq!(planned.root.children[0].source.len(), 262_143);
    assert_eq!(planned.root.children[1].source.len(), 262_143);
    let facts = planned
        .require_complete()
        .unwrap_err()
        .representation()
        .unwrap();
    assert_eq!(facts.representable_prefix, Some(262_143));
    assert_eq!(facts.required, Some(2_147_483_648));
    assert_eq!(
        planned.requirements().logical_bytes,
        262_143u128 * (4096 + 8192)
    );
}

#[test]
fn cached_view_plan_survives_owner_clear_and_execution_admits_each_declared_layout_once() {
    let source = dictionary(
        vec![Some("one longer source value"), Some("small")],
        vec![Some(0), Some(1), Some(0)],
    );
    let frozen = binding(DataType::Utf8View, true);
    let mut context = ValueConversionContext::new(1024 * 1024);
    let planned = HydratePlan::plan_observed(
        &source,
        &frozen,
        &mut context,
        &mut |_, _| false,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .unwrap();
    assert!(planned.requirements().held_plan_scratch_bytes > 0);
    context.clear();
    let stats = context.stats();
    let mut requested = vec![];
    let output = planned
        .execute_observed(
            &mut context,
            &mut |layout| {
                requested.push(layout);
                Ok::<_, Infallible>(())
            },
            &mut |_| Ok(()),
            &mut |_| Ok(()),
        )
        .unwrap();
    let expected = planned
        .requirements()
        .output
        .iter()
        .chain(&planned.requirements().execution_scratch)
        .copied()
        .filter(|layout| layout.size() != 0)
        .collect::<Vec<_>>();
    assert_eq!(requested, expected);
    assert_eq!(context.stats(), stats);
    assert_eq!(
        strings(&output),
        vec![
            Some("one longer source value".to_owned()),
            Some("small".to_owned()),
            Some("one longer source value".to_owned())
        ]
    );
    output.to_data().validate_full().unwrap();
}

#[test]
#[expect(
    deprecated,
    reason = "Legacy IPC dictionary IDs remain source encoding facts until Arrow removes their API."
)]
fn kept_dictionary_ipc_facts_and_payload_alias_survive_sibling_expansion() {
    let kept = dictionary(vec![Some("a longer retained payload")], vec![Some(0)]);
    let expanded = dictionary(vec![Some("expanded")], vec![Some(0)]);
    let fields: Fields = vec![
        Arc::new(Field::new_dict(
            "keep",
            kept.data_type().clone(),
            true,
            73,
            true,
        )),
        Arc::new(Field::new("expand", expanded.data_type().clone(), true)),
    ]
    .into();
    let source: ArrayRef =
        Arc::new(StructArray::try_new(fields, vec![kept.clone(), expanded], None).unwrap());
    let frozen = binding(
        DataType::Struct(
            vec![
                Arc::new(Field::new("keep", DataType::Utf8, true)),
                Arc::new(Field::new("expand", DataType::Utf8, true)),
            ]
            .into(),
        ),
        true,
    );
    let planned = HydratePlan::plan_observed(
        &source,
        &frozen,
        &mut ValueConversionContext::default(),
        &mut |path, _| path == [HydratePathStep::StructField(0)],
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .unwrap();
    let output = execute(&planned, &mut ValueConversionContext::default());
    let DataType::Struct(fields) = output.data_type() else {
        panic!("struct")
    };
    assert_eq!(fields[0].dict_id(), Some(73));
    assert_eq!(fields[0].dict_is_ordered(), Some(true));
    let array = output.as_any().downcast_ref::<StructArray>().unwrap();
    let original = kept
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap();
    let rebuilt = array
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap();
    assert_eq!(
        original
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .values()
            .as_ptr(),
        rebuilt
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .values()
            .as_ptr()
    );
    assert_eq!(strings(array.column(1)), vec![Some("expanded".to_owned())]);
    output.to_data().validate_full().unwrap();
}

#[test]
fn empty_dictionary_materialization_uses_planned_offsets_sentinel_and_view_geometry() {
    let source = dictionary(vec![], vec![]);
    for layout in [DataType::Utf8, DataType::Utf8View] {
        let planned = plan(&source, &binding(layout.clone(), true));
        let output = execute(&planned, &mut ValueConversionContext::default());
        assert_eq!(output.len(), 0);
        assert_eq!(output.data_type(), &layout);
        output.to_data().validate_full().unwrap();
        if let Some(array) = output.as_any().downcast_ref::<StringArray>() {
            assert_eq!(array.value_offsets(), &[0]);
        }
    }
}

#[test]
fn dense_and_sparse_union_occurrences_keep_encoding_demands_and_reachable_required_rows() {
    use arrow_array::{DictionaryArray, Int32Array, StringArray, UnionArray, types::Int32Type};
    use arrow_schema::{UnionFields, UnionMode};
    for dense in [false, true] {
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), Some(1), None]),
                Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
            )
            .unwrap(),
        );
        let number: ArrayRef = Arc::new(Int32Array::from(vec![7, 8, 9]));
        let fields = UnionFields::try_new(
            [1, 4],
            vec![
                Arc::new(Field::new("encoded", dictionary.data_type().clone(), true)),
                Arc::new(Field::new("number", DataType::Int32, false)),
            ],
        )
        .unwrap();
        let ids = if dense {
            vec![1i8, 4, 1]
        } else {
            vec![1i8, 1, 4]
        };
        let source: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields,
                ids.into(),
                dense.then(|| vec![0i32, 0, 1].into()),
                vec![dictionary.clone(), number.clone()],
            )
            .unwrap(),
        );
        let mode = if dense {
            UnionMode::Dense
        } else {
            UnionMode::Sparse
        };
        let target = DataType::Union(
            UnionFields::try_new(
                [1, 4],
                vec![
                    Arc::new(Field::new("encoded", DataType::Utf8, false)),
                    Arc::new(Field::new("number", DataType::Int32, false)),
                ],
            )
            .unwrap(),
            mode,
        );
        let declaration = binding(target.clone(), true);
        let mut context = ValueConversionContext::default();
        let mut demands = Vec::new();
        let planned = HydratePlan::plan_with_options_observed(
            &source,
            &declaration,
            HydrateOptions {
                projection: HydrateProjection::FrozenNamesAndNullability,
                dictionary_value_layout: None,
            },
            &mut context,
            &mut |path, _| {
                demands.push(path.to_vec());
                false
            },
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok(()),
            &mut |_| Ok(()),
        )
        .unwrap();
        planned.require_complete().unwrap();
        assert_eq!(demands, vec![vec![HydratePathStep::UnionField(1)]]);
        let mut copy_extents = 0usize;
        planned
            .visit_output_storage_observed(
                &mut |part| {
                    if let PlannedOutputStorage::Buffer { bytes, .. } = part {
                        copy_extents += bytes;
                    }
                    Ok::<_, Infallible>(())
                },
                &mut |_| Ok(()),
            )
            .unwrap();
        let output = execute(&planned, &mut context);
        output.to_data().validate_full().unwrap();
        assert_eq!(output.data_type(), &target);
        let union = output.as_any().downcast_ref::<UnionArray>().unwrap();
        let values = union
            .child(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some("alpha"), Some("beta"), Some("")]
        );
        assert!(
            Arc::ptr_eq(union.child(4), &number) || union.child(4).to_data() == number.to_data()
        );
        assert!(
            copy_extents
                >= values.value_data().len() + values.offsets().inner().len() * 4 + 3 * 4 + 3
        );
        assert!(matches!(dictionary.data_type(), DataType::Dictionary(..)));
    }
}

#[test]
fn target_requirements_predicate_keeps_original_promise_error_and_dictionary_null_rules() {
    let source = dictionary(vec![Some("kept"), None], vec![Some(0), Some(1), None]);
    let target = binding(DataType::Utf8View, false);
    assert!(
        !required_values_satisfied_observed(
            &source,
            &target,
            HydrateOptions::default(),
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok(()),
        )
        .unwrap()
    );
    assert!(matches!(
        validate_required_observed(
            &source,
            &target,
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok(()),
        ),
        Err(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::RequiredNull
        )))
    ));
    assert!(
        required_values_satisfied_observed(
            &source,
            &binding(DataType::Utf8View, true),
            HydrateOptions::default(),
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok(()),
        )
        .unwrap()
    );
    let error = required_values_satisfied_observed(
        &source,
        &target,
        HydrateOptions::default(),
        &mut |_| Err("original control"),
        &mut |_| Ok(()),
    )
    .unwrap_err();
    assert!(matches!(error, ValueFailure::Caller("original control")));
}

#[test]
fn target_requirements_use_the_same_active_parent_rule_without_masking_layout() {
    let field = Arc::new(Field::new("child", DataType::Int32, true));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![None::<i32>]));
    let target = binding(
        DataType::Struct(vec![Arc::new(Field::new("child", DataType::Int32, false))].into()),
        true,
    );
    let options = HydrateOptions {
        dictionary_value_layout: None,
        projection: HydrateProjection::FrozenNamesAndNullability,
    };
    for (active, expected) in [(false, true), (true, false)] {
        let source: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![Arc::clone(&field)].into(),
                vec![Arc::clone(&values)],
                Some(NullBuffer::from(vec![active])),
            )
            .unwrap(),
        );
        assert_eq!(
            required_values_satisfied_observed(
                &source,
                &target,
                options,
                &mut |_| Ok::<_, Infallible>(()),
                &mut |_| Ok(()),
            )
            .unwrap(),
            expected
        );
    }
    let wrong: ArrayRef = Arc::new(Int32Array::from(vec![None::<i32>]));
    assert!(matches!(
        required_values_satisfied_observed(
            &wrong,
            &target,
            options,
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok(()),
        ),
        Err(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::Layout
        )))
    ));
}
