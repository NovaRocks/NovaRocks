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

//! Approx-count-distinct state combinator for IMV.
//!
//! INVARIANT: the MV signed state MUST be an exact, invertible multiset
//! (aliased to `MinMaxState`). It must NEVER be replaced by an HLL/approximate
//! sketch — HLL is not invertible, so a `-1` (delete) under signed refresh
//! would corrupt the maintained count. Query-time approximate distinct lives
//! elsewhere (`agg::hll_raw` / `HistogramHllNdvAgg`) and must not leak here.

pub(in crate::exec::expr::agg::functions) use super::min_max::{
    MinMaxStateAgg as ApproxCountDistinctStateAgg,
    MinMaxStateSignedAgg as ApproxCountDistinctStateSignedAgg,
};

#[cfg(test)]
mod tests {
    use std::alloc::{Layout, alloc, dealloc};
    use std::ptr::NonNull;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BinaryArray, Int8Array, Int64Array, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field, Fields};

    use crate::exec::change_op::{CHANGE_OP_DELETE, CHANGE_OP_INSERT};
    use crate::exec::expr::agg::functions::AggStatePtr;
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

    fn agg_func(name: &str, input_arg_type: Option<DataType>) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Binary),
                input_arg_type,
            }),
            ..Default::default()
        }
    }

    fn build_spec(name: &str, input_type: &DataType) -> super::super::super::AggSpec {
        super::super::super::build_spec_from_type(
            &agg_func(name, Some(input_type.clone())),
            Some(input_type),
            false,
        )
        .unwrap()
    }

    fn signed_input_type(value_type: DataType) -> DataType {
        DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("value", value_type, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]))
    }

    fn signed_i64_input(values: Vec<Option<i64>>, ops: Vec<Option<i8>>) -> ArrayRef {
        let value_arr = Arc::new(Int64Array::from(values)) as ArrayRef;
        let op_arr = Arc::new(Int8Array::from(ops)) as ArrayRef;
        Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("value", DataType::Int64, true)),
                Arc::new(Field::new("op", DataType::Int8, true)),
            ]),
            vec![value_arr, op_arr],
            None::<NullBuffer>,
        )) as ArrayRef
    }

    struct StateCell {
        spec: super::super::super::AggSpec,
        ptr: NonNull<u8>,
        layout: Layout,
    }

    impl StateCell {
        fn new(spec: super::super::super::AggSpec) -> Self {
            let agg = super::super::super::resolve_by_kind(&spec.kind);
            let (size, align) = agg.state_layout_for(&spec.kind);
            let layout = Layout::from_size_align(size, align).unwrap();
            let ptr = NonNull::new(unsafe { alloc(layout) }).expect("aggregate state allocation");
            agg.init_state(&spec, ptr.as_ptr());
            Self { spec, ptr, layout }
        }

        fn ptr(&mut self) -> AggStatePtr {
            self.ptr.as_ptr() as AggStatePtr
        }

        fn update(&mut self, input: ArrayRef) {
            let rows = input.len();
            let input_slot = Some(input);
            let view = super::super::super::build_input_view(&self.spec, &input_slot).unwrap();
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::update_batch(&self.spec, 0, &state_ptrs, &view).unwrap();
        }

        fn final_bytes(&mut self) -> Vec<u8> {
            let ptr = self.ptr();
            let out = super::super::super::build_array(&self.spec, 0, &[ptr], false).unwrap();
            let binary = out.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(binary.len(), 1);
            binary.value(0).to_vec()
        }
    }

    impl Drop for StateCell {
        fn drop(&mut self) {
            super::super::super::drop_state(&self.spec, self.ptr.as_ptr());
            unsafe {
                dealloc(self.ptr.as_ptr(), self.layout);
            }
        }
    }

    fn state_bytes(name: &str, input_type: &DataType, input: ArrayRef) -> Vec<u8> {
        let mut cell = StateCell::new(build_spec(name, input_type));
        cell.update(input);
        cell.final_bytes()
    }

    #[test]
    fn approx_count_distinct_state_byte_equal_to_min_state() {
        let input = Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(3), None])) as ArrayRef;

        let approx_count_distinct = state_bytes(
            "approx_count_distinct_state",
            &DataType::Int64,
            input.clone(),
        );
        let min = state_bytes("min_state", &DataType::Int64, input);

        assert_eq!(
            approx_count_distinct, min,
            "ApproxCountDistinct state must be byte-identical to Min state on same input"
        );
    }

    #[test]
    fn approx_count_distinct_state_signed_byte_equal_to_min_state_signed() {
        let input_type = signed_input_type(DataType::Int64);
        let input = signed_i64_input(
            vec![Some(5), Some(5), Some(3)],
            vec![
                Some(CHANGE_OP_INSERT),
                Some(CHANGE_OP_DELETE),
                Some(CHANGE_OP_INSERT),
            ],
        );

        let approx_count_distinct = state_bytes(
            "approx_count_distinct_state_signed",
            &input_type,
            input.clone(),
        );
        let min = state_bytes("min_state_signed", &input_type, input);

        assert_eq!(
            approx_count_distinct, min,
            "ApproxCountDistinct signed state must be byte-identical to Min signed state on same input"
        );
    }

    #[test]
    fn approx_count_distinct_state_rejects_unsupported_binary_key() {
        let err = super::super::super::build_spec_from_type(
            &agg_func("approx_count_distinct_state", Some(DataType::Binary)),
            Some(&DataType::Binary),
            false,
        )
        .unwrap_err();

        assert!(err.contains("unsupported key type Binary"));
    }

    #[test]
    fn approx_count_distinct_state_signed_rejects_unsupported_nested_key() {
        let input_type = signed_input_type(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int64,
            true,
        ))));

        let err = super::super::super::build_spec_from_type(
            &agg_func(
                "approx_count_distinct_state_signed",
                Some(input_type.clone()),
            ),
            Some(&input_type),
            false,
        )
        .unwrap_err();

        assert!(err.contains("unsupported key type List"));
    }
}
