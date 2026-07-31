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

//! Per-kind state combinator aggregate functions for IVM detail-state.
//!
//! Each kind family has two aggregate functions:
//!   - <kind>_state(args)                      -> VARBINARY (partial state from INSERT-only delta)
//!   - <kind>_state_signed(args, __op TINYINT) -> VARBINARY (with INSERT/DELETE sign)
//!
//! All produce VARBINARY columns with byte layout defined in
//! src/mv/aggregate_state/state_codec.rs

pub(super) mod approx_count_distinct;
pub(super) mod avg;
pub(super) mod bool_or_and;
pub(super) mod count;
pub(super) mod count_distinct;
pub(super) mod min_max;
pub(super) mod opaque_merge;
pub(super) mod sum;

#[cfg(test)]
mod tests {
    use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
    use std::ptr::NonNull;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BinaryArray};
    use arrow::datatypes::DataType;

    use crate::exec::expr::agg::functions::{AggSpec, AggStatePtr};
    use crate::exec::expr::function::mv_state::{
        approx_count_distinct_state_union, avg_state_union, bool_and_state_union,
        bool_or_state_union, count_distinct_state_union, count_state_union, max_state_union,
        min_state_union,
    };
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};
    use crate::mv::aggregate_state::state_codec::{
        MultisetEntry, encode_bool_state, encode_count_state, encode_multiset, encode_sum_int64,
    };

    type UnionFn = fn(&[u8], &[u8]) -> Result<Vec<u8>, String>;

    struct MergeCase {
        name: &'static str,
        states: Vec<Vec<u8>>,
        union: UnionFn,
    }

    struct StateCell {
        spec: AggSpec,
        ptr: NonNull<u8>,
        layout: Layout,
    }

    impl StateCell {
        fn new(spec: AggSpec) -> Self {
            let (size, align) = super::super::state_layout_for_kind(&spec.kind);
            let layout = Layout::from_size_align(size.max(1), align).unwrap();
            let raw = unsafe { alloc(layout) };
            if raw.is_null() {
                handle_alloc_error(layout);
            }
            let ptr = unsafe { NonNull::new_unchecked(raw) };
            super::super::init_state(&spec, ptr.as_ptr());
            Self { spec, ptr, layout }
        }

        fn ptr(&mut self) -> AggStatePtr {
            self.ptr.as_ptr() as AggStatePtr
        }

        fn update(&mut self, input: ArrayRef) {
            let rows = input.len();
            let input_slot = Some(input);
            let view = super::super::build_input_view(&self.spec, &input_slot).unwrap();
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::update_batch(&self.spec, 0, &state_ptrs, &view).unwrap();
        }

        fn merge(&mut self, input: ArrayRef) {
            let rows = input.len();
            let input_slot = Some(input);
            let view = super::super::build_merge_view(&self.spec, &input_slot).unwrap();
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::merge_batch(&self.spec, 0, &state_ptrs, &view).unwrap();
        }

        fn final_bytes(&mut self) -> Vec<u8> {
            let ptr = self.ptr();
            let out = super::super::build_array(&self.spec, 0, &[ptr], false).unwrap();
            let binary = out.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(binary.len(), 1);
            binary.value(0).to_vec()
        }
    }

    impl Drop for StateCell {
        fn drop(&mut self) {
            super::super::drop_state(&self.spec, self.ptr.as_ptr());
            unsafe {
                dealloc(self.ptr.as_ptr(), self.layout);
            }
        }
    }

    fn agg_func(name: &str) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Binary),
                input_arg_type: Some(DataType::Binary),
            }),
            ..Default::default()
        }
    }

    fn build_merge_spec(name: &str) -> AggSpec {
        super::super::build_spec_from_type(&agg_func(name), Some(&DataType::Binary), false).unwrap()
    }

    fn binary_input(states: &[Vec<u8>]) -> ArrayRef {
        Arc::new(BinaryArray::from_iter_values(
            states.iter().map(Vec::as_slice),
        )) as ArrayRef
    }

    fn int64_multiset(entries: &[(i64, i64)]) -> Vec<u8> {
        let entries = entries
            .iter()
            .map(|(key, count)| MultisetEntry {
                key_bytes: key.to_le_bytes().to_vec(),
                count: *count,
            })
            .collect::<Vec<_>>();
        encode_multiset(&entries, &DataType::Int64).unwrap()
    }

    fn merge_cases() -> Vec<MergeCase> {
        vec![
            MergeCase {
                name: "count_state_merge",
                states: vec![
                    encode_count_state(2),
                    Vec::new(),
                    encode_count_state(-1),
                    encode_count_state(4),
                ],
                union: count_state_union,
            },
            MergeCase {
                name: "avg_state_merge",
                states: vec![
                    encode_sum_int64(2, 30),
                    encode_sum_int64(-1, -10),
                    Vec::new(),
                    encode_sum_int64(3, 15),
                ],
                union: avg_state_union,
            },
            MergeCase {
                name: "min_state_merge",
                states: vec![
                    int64_multiset(&[(1, 2), (3, 1)]),
                    int64_multiset(&[(3, -1), (5, 4)]),
                    Vec::new(),
                ],
                union: min_state_union,
            },
            MergeCase {
                name: "max_state_merge",
                states: vec![
                    int64_multiset(&[(7, 1), (9, 2)]),
                    int64_multiset(&[(7, -1), (11, 3)]),
                    Vec::new(),
                ],
                union: max_state_union,
            },
            MergeCase {
                name: "bool_and_state_merge",
                states: vec![
                    encode_bool_state(2, 1),
                    encode_bool_state(-1, 3),
                    Vec::new(),
                ],
                union: bool_and_state_union,
            },
            MergeCase {
                name: "bool_or_state_merge",
                states: vec![
                    encode_bool_state(1, 4),
                    encode_bool_state(2, -1),
                    Vec::new(),
                ],
                union: bool_or_state_union,
            },
            MergeCase {
                name: "count_distinct_state_merge",
                states: vec![
                    int64_multiset(&[(1, 1), (2, 1)]),
                    int64_multiset(&[(2, -1), (3, 2)]),
                    Vec::new(),
                ],
                union: count_distinct_state_union,
            },
            MergeCase {
                name: "approx_count_distinct_state_merge",
                states: vec![
                    int64_multiset(&[(10, 1), (20, 1)]),
                    int64_multiset(&[(20, -1), (30, 3)]),
                    Vec::new(),
                ],
                union: approx_count_distinct_state_union,
            },
        ]
    }

    #[test]
    fn state_merge_remaining_kinds_update_batch_matches_sequential_union() {
        for case in merge_cases() {
            let expected = case
                .states
                .iter()
                .try_fold(Vec::new(), |acc, state| (case.union)(&acc, state))
                .unwrap();
            let mut cell = StateCell::new(build_merge_spec(case.name));

            cell.update(binary_input(&case.states));

            assert_eq!(
                cell.final_bytes(),
                expected,
                "{} update_batch should match sequential scalar union",
                case.name
            );
        }
    }

    #[test]
    fn state_merge_remaining_kinds_merge_batch_matches_sequential_union() {
        for case in merge_cases() {
            let expected = case
                .states
                .iter()
                .try_fold(Vec::new(), |acc, state| (case.union)(&acc, state))
                .unwrap();
            let mut cell = StateCell::new(build_merge_spec(case.name));

            cell.merge(binary_input(&case.states));

            assert_eq!(
                cell.final_bytes(),
                expected,
                "{} merge_batch should match sequential scalar union",
                case.name
            );
        }
    }
}
