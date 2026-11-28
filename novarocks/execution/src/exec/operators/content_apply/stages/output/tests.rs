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
use crate::{
    exec::{
        chunk::typed_fixture,
        pipeline::{
            operator::input_construction::try_runtime_input_construction_factory,
            pipeline::representation_construction_profile,
        },
    },
    runtime::{
        mem_tracker::MemTracker, runtime_state::RuntimeErrorState,
        tracked_state_allocator::expression_allocation_host,
    },
};
use arrow::{
    array::{StructArray, UInt64Array},
    buffer::NullBuffer,
    datatypes::{DataType, Field, Schema},
};
use novarocks_failure_contract::FailureProjection;
use novarocks_types::SlotId;
use novarocks_value_arrays::{ValueError, ValueViolation};

struct Control;

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("content output fixture must never sleep")
    }
}

#[test]
fn probe_projects_selected_permissive_rows_to_the_frozen_output() {
    let required = DataType::Struct(vec![Field::new("code", DataType::Int64, false)].into());
    let binding = FunctionValueType::new(required.clone(), true);
    let layout = typed_fixture::layout(
        Arc::new(Schema::new(vec![Field::new("payload", required, true)])),
        &[SlotId(1)],
        std::slice::from_ref(&binding),
    );
    let tracker = MemTracker::new_root("content probe output fixture");
    let factory = try_runtime_input_construction_factory(
        representation_construction_profile().unwrap(),
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    let slot = &schema.slots()[0];
    let source: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("code", DataType::Int64, true)].into(),
        vec![Arc::new(Int64Array::from(vec![Some(7), None, None]))],
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let take = |indices: &[u64]| {
        selected_copy::take_copy_in(
            Arc::clone(&source),
            CopyIndices::UInt64(Arc::new(UInt64Array::from(indices.to_vec()))),
            owner.storage_lifetime(),
            expression_allocation_host(tracker.clone()),
            &Control,
        )
        .expect("selected fixture rows")
        .into_values()
    };
    let mut conversions = ValueConversionContext::default();
    let selected = take(&[0, 1]);
    assert_ne!(selected.data_type(), slot.data_type());
    let projected = project_probe_output_column(
        selected,
        slot,
        0,
        owner.as_ref(),
        &Control,
        &mut conversions,
    )
    .unwrap();
    assert_eq!(projected.data_type(), slot.data_type());
    let chunk = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema.clone(),
        vec![projected.clone()],
        2,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap();
    let record = chunk.columns()[0]
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(record.is_null(1));
    let codes = record
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(codes.value(0), 7);
    let exact = project_probe_output_column(
        projected.clone(),
        slot,
        0,
        owner.as_ref(),
        &Control,
        &mut conversions,
    )
    .unwrap();
    assert!(Arc::ptr_eq(&projected, &exact));

    let failure = project_probe_output_column(
        take(&[2]),
        slot,
        0,
        owner.as_ref(),
        &Control,
        &mut conversions,
    )
    .expect_err("selected required child is NULL under a present parent");
    assert_eq!(
        failure.failure_fact(),
        ValueError::Invariant(ValueViolation::RequiredNull).failure_fact(),
    );
}
