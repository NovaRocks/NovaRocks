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

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

struct Control(AtomicBool);
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        assert!(
            !self.0.load(Ordering::SeqCst),
            "control after first host refusal"
        );
        Ok(())
    }
}

#[test]
fn input_derived_null_rows_are_independent_of_buffer_bytes() {
    let control = Control(AtomicBool::new(false));
    let field = Field::new("source", DataType::Null, true);
    let ty = FunctionValueType::new(DataType::Null, true);
    let input = FlatConstantResourceInput {
        rows: 1 << 40,
        buffer_count_upper_bound: 0,
        buffer_visits_bytes_upper_bound: 0,
        retained_buffer_capacity_bytes_upper_bound: 0,
        view_validation_bytes_upper_bound: 0,
    };
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let (facts, policy) = preflight_flat_pool_resources_input_derived_observed(
        &field,
        &ty,
        input,
        &mut |_| Ok::<(), ()>(()),
        &mut work,
    )
    .unwrap();
    assert_eq!(policy.max_rows, input.rows);
    assert_eq!(policy.max_logical_elements, input.rows);
    assert_eq!(policy.max_retained_buffer_bytes, 0);
    assert_eq!(
        preflight_flat_pool_resources(&field, &ty, input, policy, CompilePhase::Decode, &control,)
            .unwrap(),
        facts
    );
    assert!(matches!(
        preflight_flat_pool_resources(
            &field,
            &ty,
            input,
            ConstantPolicy {
                max_rows: input.rows - 1,
                ..policy
            },
            CompilePhase::Decode,
            &control,
        ),
        Err(ConstantError::Limit("constant row limit exceeded"))
    ));
    work.finish().unwrap();
}

#[test]
fn input_derived_recursive_profile_admits_actual_checked_values_without_a_second_formula() {
    use arrow_array::{Int8Array, ListArray};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    let control = Control(AtomicBool::new(false));
    let item = Arc::new(Field::new("item", DataType::Int8, true));
    let field = Arc::new(Field::new("source", DataType::List(item.clone()), true));
    let ty = FunctionValueType::new(field.data_type().clone(), true);
    let input = RecursiveConstantResourceInput {
        buffer_count_upper_bound: 4,
        buffer_visits_bytes_upper_bound: 100,
        retained_buffer_capacity_bytes_upper_bound: 512,
        view_validation_bytes_upper_bound: 0,
        utf8_fallback_validation_bytes_upper_bound: 0,
    };
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let (facts, policy) = preflight_recursive_pool_resources_input_derived_observed(
        &field,
        &ty,
        [2, 5],
        input,
        &mut |_| Ok::<(), ()>(()),
        &mut work,
    )
    .unwrap();
    assert_eq!(facts.array_nodes, 2);
    assert_eq!(facts.logical_elements_upper_bound, 12);
    assert_eq!(policy.max_type_depth, 2);
    assert_eq!(policy.max_type_nodes, 2);
    assert_eq!(
        preflight_recursive_pool_resources(
            &field,
            &ty,
            [2, 5],
            input,
            policy,
            CompilePhase::Decode,
            &control,
        )
        .unwrap(),
        facts
    );
    let array = ListArray::try_new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 5])),
        Arc::new(Int8Array::from(vec![1i8, 2, 3, 4, 5])),
        None,
    )
    .unwrap();
    let pool = ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy,
        CompilePhase::Decode,
        &control,
    )
    .unwrap();
    assert_eq!(pool.resource_facts().rows, 2);
    work.finish().unwrap();
}

#[test]
fn input_derived_preflight_keeps_first_host_payload_and_has_no_completed_tail() {
    let control = Control(AtomicBool::new(false));
    let field = Field::new("source", DataType::Null, true);
    let ty = FunctionValueType::new(DataType::Null, true);
    let cause = Arc::new("original resource refusal");
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let error = preflight_flat_pool_resources_input_derived_observed(
        &field,
        &ty,
        FlatConstantResourceInput {
            rows: 1,
            buffer_count_upper_bound: 0,
            buffer_visits_bytes_upper_bound: 0,
            retained_buffer_capacity_bytes_upper_bound: 0,
            view_validation_bytes_upper_bound: 0,
        },
        &mut |_| {
            assert!(!control.0.swap(true, Ordering::SeqCst));
            Err(cause.clone())
        },
        &mut work,
    )
    .unwrap_err();
    match error {
        ConstantCreationFailure::Creator(actual) => assert!(Arc::ptr_eq(&actual, &cause)),
        other => panic!("unexpected failure: {other:?}"),
    }
}

#[test]
fn input_derived_i64_pool_materializes_exact_nullable_rows() {
    use arrow_array::Int64Array;

    let control = Control(AtomicBool::new(false));
    let field = Arc::new(Field::new("source", DataType::Int64, true));
    let ty = FunctionValueType::new(DataType::Int64, true);
    let input = Int64Array::from_iter((0..4097).map(|row| (row % 11 != 0).then_some(row)));
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        input.to_data(),
        ConstantValidation::INPUT_DERIVED,
        CompilePhase::Decode,
        &control,
    )
    .unwrap();

    assert!(Arc::ptr_eq(pool.field_ref(), &field));
    assert_eq!(pool.value_type(), &ty);
    assert_eq!(pool.resource_facts().rows, 4097);
    for row in 0..4097 {
        let selected = pool.value(row).unwrap();
        assert_eq!(selected.ordinal(), row);
        assert_eq!(
            selected.try_i64().unwrap(),
            (row % 11 != 0).then_some(i64::from(row))
        );
    }
}

#[test]
fn input_derived_list_pool_materializes_backing_and_outer_null() {
    use arrow_array::{Int32Array, ListArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};

    let control = Control(AtomicBool::new(false));
    let item = Arc::new(Field::new("item", DataType::Int32, false));
    let field = Arc::new(Field::new("source", DataType::List(item.clone()), true));
    let ty = FunctionValueType::new(field.data_type().clone(), true);
    let input = ListArray::try_new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 65_537, 65_537])),
        Arc::new(Int32Array::from_iter_values(0..65_537)),
        Some(NullBuffer::from(vec![true, false])),
    )
    .unwrap();
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        input.to_data(),
        ConstantValidation::INPUT_DERIVED,
        CompilePhase::Decode,
        &control,
    )
    .unwrap();

    assert!(Arc::ptr_eq(pool.field_ref(), &field));
    assert_eq!(pool.value_type(), &ty);
    let array = pool.array().as_any().downcast_ref::<ListArray>().unwrap();
    assert!(array.is_valid(0));
    assert!(array.is_null(1));
    let child = array.value(0);
    let child = child.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(child.len(), 65_537);
    assert!(child.values().iter().copied().eq(0..65_537));
}

#[test]
fn input_derived_preflight_keeps_metadata_and_reports_actual_offset_extent() {
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};

    let control = Control(AtomicBool::new(false));
    let field = Field::new("source", DataType::Utf8, false)
        .with_metadata([("source".into(), "x".repeat(64 * 1024))].into());
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let validation = ConstantValidation::INPUT_DERIVED;
    assert!(matches!(
        preflight_scalar_construction(
            &field,
            &ty,
            1,
            false,
            validation,
            CompilePhase::Decode,
            &control,
        ),
        Err(ConstantError::Limit(
            "constant metadata byte limit exceeded"
        ))
    ));

    for (carrier, extent_limit) in [
        (DataType::Utf8, i32::MAX as u64),
        (DataType::Binary, i32::MAX as u64),
        (DataType::Utf8View, u32::MAX as u64),
        (DataType::LargeBinary, i64::MAX as u64),
    ] {
        let field = Field::new("source", carrier.clone(), false);
        let ty = FunctionValueType::new(carrier, false);
        let required = extent_limit + 1;
        let error = preflight_scalar_construction(
            &field,
            &ty,
            required,
            false,
            validation,
            CompilePhase::Decode,
            &control,
        )
        .unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        let representation = fact.representation().unwrap();
        assert_eq!(
            representation.boundary,
            RepresentationBoundary::SingleOutput
        );
        assert_eq!(representation.extent_limit, Some(extent_limit));
        assert_eq!(representation.required, Some(required));
        assert!(!error.is_local_allocation_failure());
    }
}

#[test]
fn input_derived_null_factory_rejects_excess_dictionary_depth_before_building() {
    let control = Control(AtomicBool::new(false));
    let carrier = (0..9).fold(DataType::Int64, |values, _| {
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(values))
    });
    let field = Field::new("dictionary", carrier.clone(), true);
    let ty = FunctionValueType::new(carrier, true);
    assert!(matches!(
        preflight_scalar_construction(
            &field,
            &ty,
            0,
            true,
            ConstantValidation::INPUT_DERIVED,
            CompilePhase::Decode,
            &control,
        ),
        Err(ConstantError::Limit(
            "constant dictionary depth limit exceeded"
        ))
    ));
}
