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
use arrow::{
    array::{
        Array, DictionaryArray, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray,
        StringArray, StructArray,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{Int32Type, Schema},
};
fn normalize_one(array: ArrayRef, target: Field) -> Result<RecordBatch, String> {
    let source = Arc::new(Schema::new(vec![Field::new(
        "source",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(source, vec![array]).unwrap();
    super::super::normalize_to_signed_selection(&Arc::new(Schema::new(vec![target])), &batch)
}
#[test]
fn signed_layout_accepts_nested_names_metadata_and_nullability() {
    let child: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("old", DataType::Int32, true)].into(),
        vec![child],
        Some(NullBuffer::from(vec![true, false])),
    ));
    let target =
        DataType::Struct(
            vec![Field::new("new", DataType::Int32, false).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "iceberg.id".into(),
                    "5".into(),
                )]),
            )]
            .into(),
        );
    let out = normalize_one(array, Field::new("root", target.clone(), true)).unwrap();
    assert_eq!(out.column(0).data_type(), &target);
    let structure = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(structure.is_null(1));
    assert_eq!(structure.column(0).null_count(), 0);
    assert_eq!(
        structure
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        7
    );
}
#[test]
fn signed_layout_required_null_is_rejected_at_root_or_visible_child() {
    assert!(
        normalize_one(
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Field::new("v", DataType::Int32, false)
        )
        .is_err()
    );
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("f", DataType::Int32, true)].into(),
        vec![Arc::new(Int32Array::from(vec![None::<i32>]))],
        None,
    ));
    let target = DataType::Struct(vec![Field::new("f", DataType::Int32, false)].into());
    assert!(normalize_one(array, Field::new("v", target, true)).is_err());
}
#[test]
fn signed_layout_null_list_parent_masks_required_element_null() {
    let array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("old", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
        Arc::new(Int32Array::from(vec![None, Some(8)])),
        Some(NullBuffer::from(vec![false, true])),
    ));
    let target = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
    let out = normalize_one(array, Field::new("v", target, true)).unwrap();
    let list = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(list.is_null(0));
    assert_eq!(list.values().null_count(), 0);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(1),
        8
    );
}
#[test]
fn signed_layout_visible_null_list_element_is_rejected() {
    let array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1])),
        Arc::new(Int32Array::from(vec![None::<i32>])),
        None,
    ));
    assert!(
        normalize_one(
            array,
            Field::new(
                "v",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                true
            )
        )
        .is_err()
    );
}
#[test]
fn signed_layout_dictionary_expands_exact_keys_and_value_nulls() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(1), None, Some(0), Some(2)]),
            Arc::new(StringArray::from(vec![Some("abc"), Some("x"), None])),
        )
        .unwrap(),
    );
    let out = normalize_one(dictionary.clone(), Field::new("v", DataType::Utf8, true)).unwrap();
    let strings = out
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        strings.iter().collect::<Vec<_>>(),
        vec![Some("x"), None, Some("abc"), None]
    );
    assert!(normalize_one(dictionary, Field::new("v", DataType::Utf8, false)).is_err());
}
#[test]
fn signed_layout_does_not_convert_values_or_fixed_widths() {
    assert!(
        normalize_one(
            Arc::new(Int64Array::from(vec![1])),
            Field::new("v", DataType::Int32, true)
        )
        .is_err()
    );
    let array: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter([b"abcd".as_slice()].into_iter()).unwrap());
    assert!(normalize_one(array, Field::new("v", DataType::Binary, true)).is_err());
    let decimal: ArrayRef = Arc::new(
        arrow::array::Decimal128Array::from(vec![123])
            .with_precision_and_scale(4, 2)
            .unwrap(),
    );
    assert!(normalize_one(decimal, Field::new("v", DataType::Decimal128(5, 2), true)).is_err());
}

#[test]
fn signed_layout_dictionary_extent_retains_representation_before_decode() {
    use novarocks_failure_contract::{
        FailureClass, FailureProjection, RepresentationBoundary, SharedFailureCause,
    };
    use std::error::Error;

    // The legal dictionary owns about 2 MiB of keys and one 4 KiB value.
    // Its repeated logical payload needs 2^31 bytes, one beyond UTF8's limit.
    const ROWS: usize = 524_288;
    const VALUE_BYTES: usize = 4096;
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; ROWS]),
            Arc::new(StringArray::from(vec!["x".repeat(VALUE_BYTES)])),
        )
        .unwrap(),
    );
    assert!(dictionary.get_buffer_memory_size() < 3 * 1024 * 1024);
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "source",
            dictionary.data_type().clone(),
            false,
        )])),
        vec![Arc::clone(&dictionary)],
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));

    // Exercise normalization directly as well as measurement: decode must refuse
    // before constructing a StringBuilder for the expanded 2 GiB payload.
    let error = super::normalize(&schema, &input).unwrap_err();
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Representation);
    assert!(fact.resource().is_none());
    let representation = fact.representation().unwrap();
    assert_eq!(
        representation.boundary,
        RepresentationBoundary::SingleOutput
    );
    assert_eq!(
        representation.required,
        Some(ROWS as u64 * VALUE_BYTES as u64)
    );
    assert_eq!(representation.extent_limit, Some(i32::MAX as u64));
    assert_eq!(representation.representable_prefix, Some((ROWS - 1) as u64));
    let shared = error
        .source()
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap();
    let original = shared
        .downcast_ref::<novarocks_value_arrays::ValueError>()
        .unwrap();
    assert_eq!(original.failure_fact(), fact);
    assert_eq!(shared.failure_fact(), fact);
    assert!(Arc::ptr_eq(input.column(0), &dictionary));
    assert_eq!(dictionary.len(), ROWS);
}

#[test]
fn signed_layout_allocation_keeps_actual_standard_reservation_source() {
    use novarocks_failure_contract::{
        FailureClass, FailureProjection, ResourceDimension, SharedFailureCause,
    };
    use novarocks_functions::allocation_failure::AllocationRefusal;
    use std::{collections::TryReserveError, error::Error};

    // This is a real standard container error, guaranteed without an allocator
    // call by the impossible capacity; it does not simulate an OOM condition.
    let mut values = Vec::<u64>::new();
    let original = values.try_reserve_exact(usize::MAX).unwrap_err();
    let original_message = original.to_string();
    let error = super::allocation(original);
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Resource);
    assert!(fact.representation().is_none());
    let resource = fact.resource().unwrap();
    assert_eq!(resource.dimension, ResourceDimension::Bytes);
    // std keeps its reserve failure kind and requested layout opaque.
    assert_eq!(resource.requested, None);
    assert_eq!(resource.required_growth, None);
    assert_eq!(resource.limit, None);
    assert_eq!(resource.live, None);
    assert_eq!(resource.committed, None);
    let shared = error
        .source()
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap();
    let refusal = shared
        .downcast_ref::<AllocationRefusal<TryReserveError>>()
        .unwrap();
    let retained = refusal
        .source()
        .unwrap()
        .downcast_ref::<TryReserveError>()
        .unwrap();
    assert!(std::ptr::eq(retained, refusal.original()));
    assert_eq!(retained.to_string(), original_message);
    assert_eq!(shared.failure_fact(), fact);
    assert!(values.is_empty());
    assert_eq!(values.capacity(), 0);
}

#[test]
fn signed_layout_arrow_assembly_keeps_actual_arrow_source_and_fact() {
    use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
    use std::error::Error;

    let original = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .unwrap_err();
    assert!(matches!(
        &original,
        arrow::error::ArrowError::InvalidArgumentError(_)
    ));
    let original_message = original.to_string();
    let error = super::arrow_assembly(original);
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Invariant);
    let shared = error
        .source()
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap();
    let assembly = shared.downcast_ref::<ArrowAssemblyFailure>().unwrap();
    let retained = assembly
        .source()
        .unwrap()
        .downcast_ref::<arrow::error::ArrowError>()
        .unwrap();
    assert!(std::ptr::eq(retained, &assembly.0));
    assert_eq!(retained.to_string(), original_message);
    assert_eq!(shared.failure_fact(), fact);
}

// This observer delegates every reservation to the actual Internal retention
// author. It neither supplies capacity nor replaces the returned byte receipt.
struct OriginalNormalizationBudget {
    retention: crate::query_execution::internal_result_cpu::InternalResultRetention,
    window: novarocks_workload_control::ResultWindowAlias,
    requests: std::sync::Mutex<Vec<(usize, bool, u64)>>,
}
impl novarocks_spi::connector::ConnectorPayloadByteBudget for OriginalNormalizationBudget {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> Result<novarocks_spi::connector::ConnectorPayloadRetentionGuard, ConnectorError> {
        let result = novarocks_spi::connector::ConnectorPayloadByteBudget::reserve_bytes(
            &self.retention,
            bytes,
        );
        self.requests.lock().unwrap().push((
            bytes,
            result.is_ok(),
            self.window.retained_payload_bytes(),
        ));
        result
    }
}
fn original_normalization_budget(
    limit: u64,
) -> (
    novarocks_workload_control::WorkloadControl,
    novarocks_workload_control::RootWork,
    novarocks_workload_control::ResultCapacityHandle,
    novarocks_workload_control::joint_admission::JointProgressDriver,
    Arc<OriginalNormalizationBudget>,
) {
    use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
    use novarocks_workload_control::{
        ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };
    let count = 8;
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let mut config = ResultCapacityConfig::V1;
    config.all_objects_bytes[2] = limit;
    let capacity = control.configure_result_capacity(config).unwrap();
    let (_, driver) = control
        .configure_responsibility(novarocks_workload_control::ResponsibilityConfig {
            work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control
        .configure_control_storage(novarocks_workload_control::ControlStorageConfig::new(
            count,
            &[],
        ))
        .unwrap();
    control.mark_ready().unwrap();
    // The original complete-window API is sufficient for this buffer owner
    // component. No executor, Native qualification or empty custody pin exists.
    let (root, window) = control
        .root_admission()
        .try_begin_root_with_result(
            WorkRequest::new(WorkClass::Management),
            ResultWindowClass::Internal,
        )
        .unwrap();
    let binding =
        QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
    let retention =
        crate::query_execution::internal_result_cpu::InternalResultRetention::try_new(&binding)
            .unwrap();
    let observation = Arc::new(OriginalNormalizationBudget {
        retention,
        window: binding.window_alias(),
        requests: std::sync::Mutex::new(Vec::new()),
    });
    drop((binding, window));
    (control, root, capacity, driver, observation)
}
fn paid_dictionary_for_normalization(
    budget: &Arc<OriginalNormalizationBudget>,
) -> novarocks_spi::connector::ConnectorRowMutationSourceBatch {
    use novarocks_spi::connector::{
        ConnectorPayloadRetentionGuard, ConnectorRowMutationSourceBuilder,
    };
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; 32]),
            Arc::new(StringArray::from(vec!["dictionary"])),
        )
        .unwrap(),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "input",
        dictionary.data_type().clone(),
        false,
    )]));
    let mut owner = ConnectorRowMutationSourceBuilder::try_new_with_guard(
        schema,
        ConnectorPayloadRetentionGuard::from_byte_budget(Arc::clone(budget)),
    )
    .unwrap();
    let mut columns = owner.children(1).unwrap();
    columns
        .push(owner.copy_data(0, &dictionary.to_data()).unwrap())
        .unwrap();
    owner.finish(32, columns).unwrap()
}

#[test]
fn normalization_growth_refuses_before_output_and_preserves_paid_dictionary_last_slice() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_spi::connector::ConnectorPayloadRetentionGuard;

    // SourceBuilder requests 128 key bytes + two independently rounded 64-byte
    // UTF8 buffers. This fixture deliberately leaves no normalization headroom.
    let (control, root, capacity, mut driver, budget) = original_normalization_budget(256);
    let source = paid_dictionary_for_normalization(&budget);
    assert_eq!(budget.window.retained_payload_bytes(), 256);
    let original_column = Arc::clone(source.batch().column(0));
    let original_data = original_column.to_data();
    let last_slice = original_data.child_data()[0].buffers()[1].slice(1);
    budget.requests.lock().unwrap().clear();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let declarations = super::fixture_declarations(&schema);
    let first_normalization_layout = {
        let mut planning = ValueConversionContext::new(0);
        let plan = CowSignedLayoutPlan::plan(
            &schema,
            source.batch(),
            &declarations,
            &mut planning,
            super::super::cast_footprint::TRANSFORM_BYTES,
            &mut |_| Ok(()),
        )
        .unwrap();

        plan.columns
            .iter()
            .flat_map(|column| {
                column
                    .requirements()
                    .output
                    .iter()
                    .chain(&column.requirements().execution_scratch)
            })
            .find(|layout| layout.size() != 0)
            .unwrap()
            .size()
    };
    let guard = ConnectorPayloadRetentionGuard::from_byte_budget(Arc::clone(&budget));
    let mut conversions = ValueConversionContext::new(0);
    let result = super::super::normalize_owned_to_signed_selection(
        &schema,
        &source,
        0,
        0,
        Some(&guard),
        &declarations,
        &mut conversions,
        None,
        &mut |_| Ok(()),
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("actual normalization growth must be refused before output"),
    };
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(error.failure_fact().class(), FailureClass::Resource);
    let requests = budget.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "first positive layout is refused before materialization or copy"
    );
    assert!(first_normalization_layout > 0);
    assert_eq!(
        requests[0].0, first_normalization_layout,
        "the refusal is the original normalization layout, not a later safe copy"
    );
    assert!(!requests[0].1);
    assert_eq!(requests[0].2, 256);
    drop(requests);
    assert_eq!(budget.window.retained_payload_bytes(), 256);
    assert!(Arc::ptr_eq(source.batch().column(0), &original_column));
    assert_eq!(source.batch().num_rows(), 32);
    conversions.clear();
    drop((source, original_column, original_data));
    assert_eq!(last_slice.as_slice(), b"ictionary");
    assert_eq!(budget.window.retained_payload_bytes(), 64);
    drop(last_slice);
    assert_eq!(budget.window.retained_payload_bytes(), 0);
    drop((guard, budget));
    root.business.release();
    root.owner.complete();
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    control.close_admission();
    let mut idle = false;
    for _ in 0..10_000 {
        if matches!(
            driver.step(),
            novarocks_workload_control::joint_admission::JointProgress::Idle(_)
        ) {
            idle = true;
            break;
        }
    }
    assert!(
        idle,
        "the original responsibility service must finish actual reaping"
    );
    assert!(control.shutdown().is_ok());
}

#[test]
fn normalization_pays_old_normalized_and_safe_copy_until_each_actual_backing_exits() {
    use novarocks_spi::connector::ConnectorPayloadRetentionGuard;

    let (control, root, capacity, mut driver, budget) = original_normalization_budget(64 * 1024);
    let source = paid_dictionary_for_normalization(&budget);
    let old_bytes = budget.window.retained_payload_bytes();
    assert_eq!(old_bytes, 256);
    let input_column = Arc::clone(source.batch().column(0));
    let input_data = input_column.to_data();
    let last_input_slice = input_data.child_data()[0].buffers()[1].slice(1);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let declarations = super::fixture_declarations(&schema);
    // Obtain the expected transient layouts from the exact original frozen
    // Hydrate plan; do not estimate a decoded dictionary with its wire length.
    let mut planning = ValueConversionContext::new(0);
    let plan = CowSignedLayoutPlan::plan(
        &schema,
        source.batch(),
        &declarations,
        &mut planning,
        super::super::cast_footprint::TRANSFORM_BYTES,
        &mut |_| Ok(()),
    )
    .unwrap();
    let transient: Vec<_> = plan
        .columns
        .iter()
        .flat_map(|column| {
            column
                .requirements()
                .output
                .iter()
                .chain(&column.requirements().execution_scratch)
        })
        .filter(|layout| layout.size() != 0)
        .map(|layout| layout.size())
        .collect();
    assert!(!transient.is_empty());
    let transient_bytes = transient.iter().sum::<usize>() as u64;
    drop(plan);
    planning.clear();
    budget.requests.lock().unwrap().clear();
    let guard = ConnectorPayloadRetentionGuard::from_byte_budget(Arc::clone(&budget));
    let mut conversions = ValueConversionContext::new(0);
    let output = super::super::normalize_owned_to_signed_selection(
        &schema,
        &source,
        0,
        0,
        Some(&guard),
        &declarations,
        &mut conversions,
        None,
        &mut |_| Ok(()),
    )
    .unwrap();
    let output_array = output
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(output_array.len(), 32);
    assert!(output_array.iter().all(|value| value == Some("dictionary")));
    let requests = budget.requests.lock().unwrap();
    assert!(requests.iter().all(|request| request.1));
    assert!(
        requests.len() > transient.len(),
        "independent safe-copy buffers are also paid"
    );
    assert_eq!(
        requests[..transient.len()]
            .iter()
            .map(|request| request.0)
            .collect::<Vec<_>>(),
        transient
    );
    let copy_bytes = requests[transient.len()..]
        .iter()
        .map(|request| request.0 as u64)
        .sum::<u64>();
    let mut held = old_bytes;
    for &(bytes, _, actual) in requests.iter() {
        held += bytes as u64;
        assert_eq!(
            actual, held,
            "all prior actual backing still coexists at formation"
        );
    }
    assert_eq!(held, old_bytes + transient_bytes + copy_bytes);
    // UTF8 copy_data copies offsets before values. The last exact receipt is
    // the values buffer; a slice must preserve that full receipt, not its len.
    let last_output_receipt = requests.last().unwrap().0 as u64;
    drop(requests);
    assert_eq!(
        budget.window.retained_payload_bytes(),
        old_bytes + copy_bytes
    );
    let output_column = Arc::clone(output.batch().column(0));
    let output_data = output_column.to_data();
    let last_output_slice = output_data.buffers()[1].slice_with_length(1, 1);
    conversions.clear();
    drop((source, input_column, input_data));
    assert_eq!(budget.window.retained_payload_bytes(), 64 + copy_bytes);
    drop((output, output_column, output_data));
    assert_eq!(
        budget.window.retained_payload_bytes(),
        64 + last_output_receipt
    );
    assert_eq!(last_input_slice.as_slice(), b"ictionary");
    assert_eq!(last_output_slice.as_slice(), b"i");
    drop(last_input_slice);
    assert_eq!(budget.window.retained_payload_bytes(), last_output_receipt);
    drop(last_output_slice);
    assert_eq!(budget.window.retained_payload_bytes(), 0);
    drop((guard, budget));
    root.business.release();
    root.owner.complete();
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    control.close_admission();
    let mut idle = false;
    for _ in 0..10_000 {
        if matches!(
            driver.step(),
            novarocks_workload_control::joint_admission::JointProgress::Idle(_)
        ) {
            idle = true;
            break;
        }
    }
    assert!(
        idle,
        "the original responsibility service must finish actual reaping"
    );
    assert!(control.shutdown().is_ok());
}
