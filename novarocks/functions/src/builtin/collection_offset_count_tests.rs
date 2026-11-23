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
//! Original shared offset body and classification/control-prefix evidence.
use super::*;
use crate::{KernelDiagnostic, KernelFailure, Selection};
use arrow_array::{Int32Array, ListArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
#[test]
fn offset_core_same_original_child_agnostic_null_and_slice_body() {
    let a = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0_i32, 2, 2, 4].into()),
        Arc::new(Int32Array::from(vec![None::<i32>; 4])),
        Some(NullBuffer::from(vec![true, true, false])),
    );
    let selection = Selection::all(3);
    let o = count_observed(
        &a,
        || Ok(a.value_offsets()),
        selection,
        |_, r| Ok::<_, KernelFailure>(r),
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(
        o.as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2), Some(0), None]
    );
    let b = a.slice(1, 2);
    let o = super::super::collection_cardinality_core::count_observed(
        &b,
        Selection::all(2),
        |_, r| Ok::<_, KernelFailure>(r),
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(
        o.as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0), None]
    );
}
#[test]
fn offset_core_raw_classification_follows_original_capacity_boundary() {
    let a = Int32Array::from(vec![1]);
    let mut trace = Vec::new();
    let error = super::super::collection_cardinality_core::count_observed(
        &a,
        Selection::all(1),
        |_, r| Ok::<_, KernelFailure>(r),
        &mut |event| {
            trace.push(matches!(event, CollectionObservation::OpaqueBoundary));
            Ok(())
        },
    )
    .unwrap_err();
    assert!(
        matches!(error,OffsetCountFailure::Operation(source) if source.to_string().contains("cardinality expects ARRAY or MAP, got Int32") && novarocks_failure_contract::FailureProjection::failure_fact(&source).class() == novarocks_failure_contract::FailureClass::Invariant)
    );
    assert_eq!(trace, vec![true, true]);
}
#[test]
fn offset_core_each_observed_boundary_preserves_seven_typed_causes_without_later_work() {
    let a = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0_i32, 2, 2].into()),
        Arc::new(Int32Array::from(vec![None::<i32>; 2])),
        Some(NullBuffer::from(vec![true, false])),
    );
    let mut good = Vec::new();
    count_observed(
        &a,
        || Ok(a.value_offsets()),
        Selection::all(2),
        |_, r| Ok::<_, KernelFailure>(r),
        &mut |e| {
            good.push(matches!(e, CollectionObservation::OpaqueBoundary));
            Ok(())
        },
    )
    .unwrap();
    for at in 0..good.len() {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("original internal")),
            KernelFailure::Operational(KernelDiagnostic::new("original operational")),
            KernelFailure::InstanceFailed,
        ] {
            let mut trace = Vec::new();
            let error = count_observed(
                &a,
                || Ok(a.value_offsets()),
                Selection::all(2),
                |_, r| Ok::<_, KernelFailure>(r),
                &mut |event| {
                    assert!(trace.len() <= at);
                    trace.push(matches!(event, CollectionObservation::OpaqueBoundary));
                    if trace.len() == at + 1 {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
            assert!(matches!(error,OffsetCountFailure::Control(actual) if actual==cause));
            assert_eq!(trace, good[..=at]);
        }
    }
}

#[test]
fn count_output_geometry_retains_actual_layout_source_without_inventing_quota() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let error = output_capacity(usize::MAX).unwrap_err();
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Representation);
    let representation = fact.representation().unwrap();
    assert_eq!(representation.required, None);
    assert_eq!(representation.extent_limit, Some(isize::MAX as u64));
    assert_eq!(representation.representable_prefix, None);
    assert!(fact.resource().is_none());
    let mut source: &(dyn std::error::Error + 'static) = &error;
    while !source.is::<std::alloc::LayoutError>() {
        source = source
            .source()
            .expect("actual standard layout error retained");
    }
}
