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
use crate::ByteLayout;
use arrow_array::{
    BinaryArray, BinaryViewArray, DictionaryArray, Int32Array, LargeBinaryArray, LargeStringArray,
    ListArray, StringArray, StringViewArray, StructArray, types::Int32Type,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_type_contract::{
    ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};
use std::convert::Infallible;

fn binding(ty: DataType, nullable: bool) -> TypedValueBinding {
    let storage = ValueTypeStorageLifetime::pure();
    TypedValueBinding::try_new(
        ValueTypeRef::try_from_declared_arrow(
            &ty,
            nullable,
            ValueLogicalType::Physical,
            storage.clone(),
        )
        .unwrap(),
        ValueLayout::try_new(&ty, storage).unwrap(),
    )
    .unwrap()
}
fn plan(
    source: &ArrayRef,
    target: &TypedValueBinding,
    mode: HydrateProjection,
) -> Result<HydratePlan, ValueFailure<Infallible>> {
    HydratePlan::plan_with_options_observed(
        source,
        target,
        HydrateOptions {
            projection: mode,
            ..HydrateOptions::default()
        },
        &mut ValueConversionContext::default(),
        &mut |_, _| false,
        &mut |_| Ok(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
}
fn execute(plan: &HydratePlan) -> ArrayRef {
    plan.require_complete().unwrap();
    plan.execute_observed(
        &mut ValueConversionContext::default(),
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .unwrap()
}
fn binary_values(array: &ArrayRef) -> Vec<Option<Vec<u8>>> {
    match array.data_type() {
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(<[u8]>::to_vec))
            .collect(),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(<[u8]>::to_vec))
            .collect(),
        DataType::BinaryView => array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(<[u8]>::to_vec))
            .collect(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.as_bytes().to_vec()))
            .collect(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.as_bytes().to_vec()))
            .collect(),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.as_bytes().to_vec()))
            .collect(),
        _ => panic!("byte output"),
    }
}
#[test]
fn explicit_byte_layout_projection_preserves_sliced_binary_string_values_and_nulls() {
    let long_binary = vec![0xff; 150_017];
    let long_string = "文x".repeat(40_000);
    let binaries = [
        Some(b"outside".as_slice()),
        Some(long_binary.as_slice()),
        None,
        Some(b"".as_slice()),
        Some(b"tail".as_slice()),
    ];
    let strings = [
        Some("outside"),
        Some(long_string.as_str()),
        None,
        Some(""),
        Some("tail"),
    ];
    let sources: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(binaries.to_vec())),
        Arc::new(LargeBinaryArray::from(binaries.to_vec())),
        Arc::new(BinaryViewArray::from_iter(binaries)),
        Arc::new(StringArray::from(strings.to_vec())),
        Arc::new(LargeStringArray::from(strings.to_vec())),
        Arc::new(StringViewArray::from_iter(strings)),
    ];
    for source in sources {
        let source = source.slice(1, 3);
        let source_layout = ByteLayout::from_data_type(source.data_type()).unwrap();
        let targets = if source_layout.is_utf8() {
            [
                ByteLayout::Utf8,
                ByteLayout::LargeUtf8,
                ByteLayout::Utf8View,
            ]
        } else {
            [
                ByteLayout::Binary,
                ByteLayout::LargeBinary,
                ByteLayout::BinaryView,
            ]
        };
        for target_layout in targets {
            let target = binding(target_layout.data_type(), true);
            if source.data_type() != target.data_type() {
                assert!(plan(&source, &target, HydrateProjection::Exact).is_err());
                assert!(
                    plan(
                        &source,
                        &target,
                        HydrateProjection::FrozenNamesAndNullability
                    )
                    .is_err()
                );
            }
            let projected = plan(&source, &target, HydrateProjection::ExactByteLayouts).unwrap();
            let output = execute(&projected);
            assert_eq!(output.data_type(), target.data_type());
            assert_eq!(
                binary_values(&output),
                vec![
                    Some(if source_layout.is_utf8() {
                        long_string.as_bytes().to_vec()
                    } else {
                        long_binary.clone()
                    }),
                    None,
                    Some(vec![])
                ]
            );
            output.to_data().validate_full().unwrap();
        }
    }
}
#[test]
fn explicit_byte_layout_projection_keeps_names_null_declarations_domains_and_scalar_parameters_exact()
 {
    let child: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(b"binary".as_slice())]));
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new("payload", DataType::LargeBinary, true))].into(),
            vec![child.clone()],
            None,
        )
        .unwrap(),
    );
    let mode = HydrateProjection::ExactByteLayouts;
    for (name, nullable, ty) in [
        ("renamed", true, DataType::Binary),
        ("payload", false, DataType::Binary),
        ("payload", true, DataType::Utf8),
    ] {
        let target = binding(
            DataType::Struct(vec![Arc::new(Field::new(name, ty, nullable))].into()),
            true,
        );
        assert!(plan(&source, &target, mode).is_err());
    }
    let scalar: ArrayRef = Arc::new(
        arrow_array::Decimal128Array::from(vec![12])
            .with_precision_and_scale(8, 2)
            .unwrap(),
    );
    assert!(plan(&scalar, &binding(DataType::Decimal128(8, 3), false), mode).is_err());
    let fixed: ArrayRef = Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_iter([[0u8; 16]].into_iter()).unwrap(),
    );
    assert!(plan(&fixed, &binding(DataType::FixedSizeBinary(8), false), mode).is_err());
    assert!(plan(&child, &binding(DataType::Utf8, true), mode).is_err());
}
#[test]
fn explicit_byte_layout_projection_nested_active_ancestors_keep_only_reachable_payload() {
    let values: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(b"one".as_slice()),
        Some(b"hidden".as_slice()),
        None,
        Some(b"last".as_slice()),
    ]));
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true)));
    let lists: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::LargeBinary, true)),
            OffsetBuffer::new(vec![0, 1, 3, 4].into()),
            values,
            None,
        )
        .unwrap(),
    );
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new("items", list_type, true))].into(),
            vec![lists],
            Some(NullBuffer::from(vec![true, false, true])),
        )
        .unwrap(),
    );
    let target = binding(
        DataType::Struct(
            vec![Arc::new(Field::new(
                "items",
                DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
                true,
            ))]
            .into(),
        ),
        true,
    );
    let planned = plan(&source, &target, HydrateProjection::ExactByteLayouts).unwrap();
    assert_eq!(planned.requirements().logical_bytes, 7);
    let output = execute(&planned);
    assert_eq!(output.data_type(), target.data_type());
    let root = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(root.is_null(1));
    let list = root.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 1, 3, 4]);
    assert_eq!(
        binary_values(list.values()),
        vec![Some(b"one".to_vec()), None, None, Some(b"last".to_vec())]
    );
    output.to_data().validate_full().unwrap();
}
#[test]
fn explicit_byte_layout_projection_retains_dictionary_policy_and_target_width_plan() {
    let source: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), None]),
            Arc::new(StringArray::from(vec![Some("value"), None])),
        )
        .unwrap(),
    );
    let target = binding(DataType::Utf8View, true);
    let output = execute(&plan(&source, &target, HydrateProjection::ExactByteLayouts).unwrap());
    assert_eq!(
        binary_values(&output),
        vec![Some(b"value".to_vec()), None, None]
    );
    let wrong: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0)]),
            Arc::new(LargeStringArray::from(vec![Some("value")])),
        )
        .unwrap(),
    );
    assert!(plan(&wrong, &target, HydrateProjection::ExactByteLayouts).is_err());
    let options = HydrateOptions {
        projection: HydrateProjection::ExactByteLayouts,
        dictionary_value_layout: Some(ByteLayout::LargeUtf8),
    };
    let explicit = HydratePlan::plan_with_options_observed(
        &wrong,
        &target,
        options,
        &mut ValueConversionContext::default(),
        &mut |_, _| false,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(
        binary_values(&execute(&explicit)),
        vec![Some(b"value".to_vec())]
    );
    let bytes: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(b"a".as_slice()),
        None,
        Some(b"b".as_slice()),
    ]));
    let projected = plan(
        &bytes,
        &binding(DataType::Binary, true),
        HydrateProjection::ExactByteLayouts,
    )
    .unwrap();
    assert!(
        projected
            .requirements()
            .output
            .iter()
            .any(|l| l.size() == 4 * 4 && l.align() == align_of::<i32>())
    );
    assert_eq!(projected.requirements().logical_bytes, 2);
    execute(&projected).to_data().validate_full().unwrap();
}
#[test]
fn explicit_byte_layout_projection_preserves_original_growth_and_inside_value_control_refusals() {
    let payload = vec![0xff; 240_000];
    let source: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(payload.as_slice())]));
    let target = binding(DataType::Binary, true);
    let planned = plan(&source, &target, HydrateProjection::ExactByteLayouts).unwrap();
    let mut requests = 0;
    planned
        .execute_observed(
            &mut ValueConversionContext::default(),
            &mut |_| {
                requests += 1;
                Ok::<_, &'static str>(())
            },
            &mut |_| Ok(()),
            &mut |_| Ok(()),
        )
        .unwrap();
    assert!(requests > 0);
    for stop in 1..=requests {
        let mut seen = 0;
        let error = planned
            .execute_observed(
                &mut ValueConversionContext::default(),
                &mut |_| {
                    seen += 1;
                    if seen == stop { Err("creator") } else { Ok(()) }
                },
                &mut |_| Ok(()),
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert!(matches!(error, ValueFailure::Caller("creator")));
        assert_eq!(seen, stop);
    }
    let mut blocks = 0;
    let error = planned
        .execute_observed(
            &mut ValueConversionContext::default(),
            &mut |_| Ok::<_, &'static str>(()),
            &mut |units| {
                if units > 1 {
                    blocks += 1;
                    if blocks == 2 {
                        return Err("byte-copy-stop");
                    }
                }
                Ok(())
            },
            &mut |_| Ok(()),
        )
        .unwrap_err();
    assert!(matches!(error, ValueFailure::Caller("byte-copy-stop")));
    assert_eq!(blocks, 2);
}

fn assignment_plan(
    source: &ArrayRef,
    original: &TypedValueBinding,
    input: &TypedValueBinding,
    storage: &TypedValueBinding,
) -> Result<HydratePlan, ValueFailure<Infallible>> {
    let proof = novarocks_type_contract::CheckedWriteInputProjection::try_new_observed(
        original.borrowed(),
        input.borrowed(),
        Some(storage.borrowed()),
        || Ok::<_, Infallible>(()),
    )
    .unwrap()
    .expect("licensed assignment");
    HydratePlan::plan_assignment_projection_with_storage_observed(
        source,
        input,
        proof,
        &mut ValueConversionContext::default(),
        &ValueTypeStorageLifetime::pure(),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
}

#[test]
fn licensed_fixed_assignment_keeps_sliced_bytes_and_nulls_without_storage_width_validation() {
    let source: ArrayRef = Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [
                Some([9, 9, 9, 9]),
                Some([0, 255, 128, 1]),
                None,
                Some([2, 0, 254, 3]),
            ]
            .into_iter(),
            4,
        )
        .unwrap(),
    );
    let source = source.slice(1, 3);
    let original = binding(DataType::FixedSizeBinary(4), true);
    let input = binding(DataType::Binary, true);
    let storage = binding(DataType::FixedSizeBinary(8), true);
    let planned = assignment_plan(&source, &original, &input, &storage).unwrap();
    let output = execute(&planned);
    assert_eq!(
        binary_values(&output),
        vec![Some(vec![0, 255, 128, 1]), None, Some(vec![2, 0, 254, 3])]
    );
    output.to_data().validate_full().unwrap();
    for mode in [
        HydrateProjection::Exact,
        HydrateProjection::FrozenNamesAndNullability,
        HydrateProjection::ExactByteLayouts,
    ] {
        assert!(plan(&source, &input, mode).is_err());
    }
}

#[test]
fn licensed_fixed_assignment_observes_null_parent_and_list_slice_coordinates() {
    let child: ArrayRef = Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [
                Some([9, 9, 9, 9]),
                None,
                Some([0, 255, 128, 1]),
                Some([2, 0, 254, 3]),
            ]
            .into_iter(),
            4,
        )
        .unwrap(),
    );
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::FixedSizeBinary(4), true)),
            OffsetBuffer::new(vec![0i32, 1, 2, 4].into()),
            child,
            None,
        )
        .unwrap()
        .slice(1, 2),
    );
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new(
                "payload",
                list.data_type().clone(),
                true,
            ))]
            .into(),
            vec![list],
            Some(NullBuffer::from(vec![false, true])),
        )
        .unwrap(),
    );
    let nested = |ty, nullable| {
        DataType::Struct(
            vec![Arc::new(Field::new(
                "payload",
                DataType::List(Arc::new(Field::new("item", ty, nullable))),
                true,
            ))]
            .into(),
        )
    };
    let original = binding(nested(DataType::FixedSizeBinary(4), true), true);
    let input = binding(nested(DataType::Binary, false), true);
    let storage = binding(nested(DataType::FixedSizeBinary(8), false), true);
    let output = execute(&assignment_plan(&source, &original, &input, &storage).unwrap());
    let outer = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(outer.is_null(0));
    assert!(outer.is_valid(1));
    let lists = outer
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(lists.value_offsets(), &[1, 2, 4]);
    assert_eq!(
        binary_values(lists.values()),
        vec![
            Some(vec![]),
            Some(vec![]),
            Some(vec![0, 255, 128, 1]),
            Some(vec![2, 0, 254, 3])
        ]
    );
    output.to_data().validate_full().unwrap();

    let active: ArrayRef = Arc::new(
        StructArray::try_new(
            match source.data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => unreachable!(),
            },
            vec![
                source
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
    assert!(matches!(
        assignment_plan(&active, &original, &input, &storage),
        Err(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::RequiredNull
        )))
    ));
}

#[test]
fn licensed_fixed_assignment_keeps_caller_refusal_and_source_custody() {
    let source: ArrayRef = Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_iter([[0, 255, 128, 1]].into_iter()).unwrap(),
    );
    let weak = Arc::downgrade(&source);
    let original = binding(DataType::FixedSizeBinary(4), false);
    let input = binding(DataType::Binary, false);
    let storage = binding(DataType::FixedSizeBinary(8), false);
    let proof = novarocks_type_contract::CheckedWriteInputProjection::try_new_observed(
        original.borrowed(),
        input.borrowed(),
        Some(storage.borrowed()),
        || Ok::<_, u8>(()),
    )
    .unwrap()
    .unwrap();
    let result = HydratePlan::plan_assignment_projection_with_storage_observed(
        &source,
        &input,
        proof,
        &mut ValueConversionContext::default(),
        &ValueTypeStorageLifetime::pure(),
        &mut |_| Err(71u8),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    );
    assert!(matches!(result, Err(ValueFailure::Caller(71))));
    let result = HydratePlan::plan_assignment_projection_with_storage_observed(
        &source,
        &input,
        proof,
        &mut ValueConversionContext::default(),
        &ValueTypeStorageLifetime::pure(),
        &mut |_| Ok(()),
        &mut |_| Err(72u8),
        &mut |_| Ok(()),
    );
    assert!(matches!(result, Err(ValueFailure::Caller(72))));
    let result = HydratePlan::plan_assignment_projection_with_storage_observed(
        &source,
        &input,
        proof,
        &mut ValueConversionContext::default(),
        &ValueTypeStorageLifetime::pure(),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
        &mut |_| Err(73u8),
    );
    assert!(matches!(result, Err(ValueFailure::Caller(73))));
    let planned = assignment_plan(&source, &original, &input, &storage).unwrap();
    let result = planned.execute_observed(
        &mut ValueConversionContext::default(),
        &mut |_| Err(74u8),
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    );
    assert!(matches!(result, Err(ValueFailure::Caller(74))));
    drop(source);
    assert!(weak.upgrade().is_some());
    let output = execute(&planned);
    drop(planned);
    assert!(weak.upgrade().is_none());
    assert_eq!(binary_values(&output), vec![Some(vec![0, 255, 128, 1])]);
}

#[test]
fn licensed_fixed_assignment_uses_the_existing_binary_extent_boundary() {
    let limit = i32::MAX as u128;
    assert!(
        super::tree::check_extent(ByteLayout::Binary, limit - 4, 4, 4, 1)
            .unwrap()
            .is_none()
    );
    let crossing = super::tree::check_extent(ByteLayout::Binary, limit - 3, 4, 4, 1)
        .unwrap()
        .unwrap();
    assert_eq!(
        crossing.boundary,
        novarocks_failure_contract::RepresentationBoundary::SingleOutput
    );
    assert_eq!(crossing.representable_prefix, Some(1));
    let minimum = super::tree::check_extent(ByteLayout::Binary, 0, limit + 1, limit as u64 + 1, 0)
        .unwrap()
        .unwrap();
    assert_eq!(
        minimum.boundary,
        novarocks_failure_contract::RepresentationBoundary::MinimumUnit
    );
}

#[test]
fn licensed_fixed_assignment_keeps_map_mask_offsets_keys_and_sortedness() {
    let values: ArrayRef = Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some([9, 9, 9, 9]), None, Some([0, 255, 128, 1])].into_iter(),
            4,
        )
        .unwrap(),
    );
    let fields: arrow_schema::Fields = vec![
        Arc::new(Field::new("key", DataType::Int32, false)),
        Arc::new(Field::new("value", DataType::FixedSizeBinary(4), true)),
    ]
    .into();
    let entries = StructArray::try_new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30])), values],
        None,
    )
    .unwrap();
    let source: ArrayRef = Arc::new(
        arrow_array::MapArray::try_new(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(vec![0i32, 1, 2, 3].into()),
            entries,
            Some(NullBuffer::from(vec![true, false, true])),
            true,
        )
        .unwrap()
        .slice(1, 2),
    );
    let map = |ty, nullable| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, false)),
                        Arc::new(Field::new("value", ty, nullable)),
                    ]
                    .into(),
                ),
                false,
            )),
            true,
        )
    };
    let original = binding(map(DataType::FixedSizeBinary(4), true), true);
    let input = binding(map(DataType::Binary, false), true);
    let storage = binding(map(DataType::FixedSizeBinary(8), false), true);
    let output = execute(&assignment_plan(&source, &original, &input, &storage).unwrap());
    let result = output
        .as_any()
        .downcast_ref::<arrow_array::MapArray>()
        .unwrap();
    assert!(result.is_null(0));
    assert!(result.is_valid(1));
    assert_eq!(result.value_offsets(), &[1, 2, 3]);
    assert!(matches!(result.data_type(), DataType::Map(_, true)));
    assert_eq!(
        result
            .keys()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[10, 20, 30]
    );
    assert_eq!(
        binary_values(result.values()),
        vec![Some(vec![]), Some(vec![]), Some(vec![0, 255, 128, 1])]
    );
    output.to_data().validate_full().unwrap();
}

#[test]
fn licensed_fixed_assignment_does_not_invent_encoded_fixed_decoding() {
    let original = binding(DataType::FixedSizeBinary(4), false);
    let input = binding(DataType::Binary, false);
    let storage = binding(DataType::FixedSizeBinary(8), false);
    let values =
        arrow_array::FixedSizeBinaryArray::try_from_iter([[0, 255, 128, 1]].into_iter()).unwrap();
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), Arc::new(values.clone()))
            .unwrap(),
    );
    let ends = arrow_array::Int16Array::from(vec![1]);
    let run: ArrayRef = Arc::new(
        arrow_array::RunArray::<arrow_array::types::Int16Type>::try_new(&ends, &values).unwrap(),
    );
    for source in [dictionary, run] {
        assert!(matches!(
            assignment_plan(&source, &original, &input, &storage),
            Err(ValueFailure::Value(ValueError::Invariant(_)))
        ));
    }
    let wrong: ArrayRef = Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_iter([[1, 2, 3]].into_iter()).unwrap(),
    );
    assert!(matches!(
        assignment_plan(&wrong, &original, &input, &storage),
        Err(ValueFailure::Value(ValueError::Invariant(_)))
    ));
}
