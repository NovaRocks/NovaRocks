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
//! Independent values retained before removing Execution differential oracles.
//! Test-only storage delegates exactly to Global; it grants no production scope.
use crate::aggregate_scalar::{
    AggScalarValue as V, ScalarStateAllocator, ScalarWork, TrackedAggScalarValue as T,
};
use allocator_api2::alloc::{AllocError, Allocator, Global};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, ListArray};
use arrow_schema::{DataType, Field};
use std::{alloc::Layout, ptr::NonNull, sync::Arc};
#[derive(Clone, Debug)]
struct TestAllocator;
// SAFETY: Both allocation and release delegate unchanged layouts and pointers
// to the same Global allocator. No ownership or pointer translation is added.
unsafe impl Allocator for TestAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global.allocate(layout)
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        unsafe { Global.deallocate(pointer, layout) }
    }
}
impl ScalarStateAllocator for TestAllocator {
    fn scalar_allocation_error<E: crate::allocation_failure::AllocationRefusalSource>(
        &self,
        operation: &str,
        source: E,
    ) -> crate::KernelFailure {
        crate::KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
            operation, source,
        ))
    }
}
#[test]
fn f3_any_value_first_non_null_has_handwritten_value_and_empty_oracles() {
    // Derivation: any_value in this engine selects the first present value in
    // update order; NULL contributes nothing and cannot overwrite the winner.
    use crate::builtin::aggregate_any_value_core::AnyValueState;
    let source: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(7), Some(2), None]));
    let mut state = AnyValueState::new(TestAllocator);
    let mut work = ScalarWork::new(None);
    assert!(state.output(&mut work).unwrap().is_none());
    for row in 0..source.len() {
        state.update_from_array(&source, row, &mut work).unwrap();
    }
    assert!(matches!(
        state.output(&mut work).unwrap(),
        Some(V::Int64(7))
    ));
}
#[test]
fn f3_n_physical_integer_limits_duplicates_and_nulls_have_handwritten_oracles() {
    // Derivation: sort [-2,7,7,9], retain two minima/maxima, preserve duplicates;
    // N's final presentation is ascending even for max_n.
    use crate::builtin::aggregate_n_core::NState;
    let source: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(7),
        None,
        Some(-2),
        Some(9),
        Some(7),
    ]));
    let limits: ArrayRef = Arc::new(Int32Array::from(vec![2]));
    for (smallest, expected) in [(true, [-2, 7]), (false, [7, 9])] {
        let mut state = NState::new(TestAllocator);
        let mut work = ScalarWork::new(None);
        for row in 0..source.len() {
            state
                .update_from_arrays(&source, row, &limits, 0, smallest, &mut work)
                .unwrap();
        }
        let V::List(values) = state.output(&mut work).unwrap() else {
            panic!("N output must be a list")
        };
        let values = values
            .into_iter()
            .map(|v| match v {
                Some(V::Int64(v)) => v,
                _ => panic!("integer N output"),
            })
            .collect::<Vec<_>>();
        assert_eq!(values, expected);
    }
}
#[test]
fn f3_array_aggregate_retains_order_duplicate_and_null_from_handwritten_input() {
    // Derivation: unordered ARRAY_AGG emits each input, including a NULL, in
    // update order. The expectation is literal, not another kernel invocation.
    use crate::builtin::aggregate_array_core::{
        ArrayAggConfig, ArrayAggKind, ArrayAggState, build_array, update_row,
    };
    let source: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None, Some(2), Some(7)]));
    let mut state = ArrayAggState::new(TestAllocator);
    let mut work = ScalarWork::new(None);
    let kind = ArrayAggKind::Array {
        distinct: false,
        ascending: &[],
        nulls_first: &[],
    };
    for row in 0..source.len() {
        update_row(
            &mut state,
            kind,
            std::iter::once((&source, row)),
            false,
            false,
            &mut work,
        )
        .unwrap();
    }
    let ty = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    let input_type = DataType::Int32;
    let spec = ArrayAggConfig {
        kind,
        output_type: &ty,
        intermediate_type: &ty,
        input_arg_type: Some(&input_type),
    };
    let out = build_array(&spec, std::iter::once(&state), false, &mut work).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_length(0), 4);
    let values = list.values().as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(7), None, Some(2), Some(7)]
    );
}
#[test]
fn f3_approx_top_k_uncontended_counters_have_exact_value_and_frequency_oracles() {
    // Derivation: with more counters than distinct keys, no eviction occurs:
    // value 7 contributes twice, value 2 once. Counts descend in final order.
    use crate::approx_top_k_core::{ApproxTopKState, sorted_top_entries_observed, update_one};
    let mut state = ApproxTopKState::new(TestAllocator);
    for value in [7, 2, 7] {
        update_one(&mut state, Some(T::Int64(value)), 1).unwrap();
    }
    let entries = sorted_top_entries_observed(&state, &mut ScalarWork::new(None)).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].1.count, 2);
    assert_eq!(entries[1].1.count, 1);
    assert!(matches!(entries[0].1.value.as_ref(), Some(T::Int64(7))));
    assert!(matches!(entries[1].1.value.as_ref(), Some(T::Int64(2))));
}
