//! Average state combinator aggregate functions.

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::{AggInputView, AggKind, AggSpec, AggStatePtr, AggregateFunction};
use super::sum::{SumStateAgg, SumStateSignedAgg};

pub(in crate::exec::expr::agg::functions) struct AvgStateAgg;
pub(in crate::exec::expr::agg::functions) struct AvgStateSignedAgg;

impl AggregateFunction for AvgStateAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        SumStateAgg.build_spec_from_type(func, input_type, input_is_intermediate)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        SumStateAgg.state_layout_for(kind)
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        SumStateAgg.build_input_view(spec, array)
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        SumStateAgg.build_merge_view(spec, array)
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        SumStateAgg.init_state(spec, ptr)
    }

    fn drop_state(&self, spec: &AggSpec, ptr: *mut u8) {
        SumStateAgg.drop_state(spec, ptr)
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        SumStateAgg.update_batch(spec, offset, state_ptrs, input)
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        SumStateAgg.merge_batch(spec, offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        SumStateAgg.build_array(spec, offset, group_states, output_intermediate)
    }
}

impl AggregateFunction for AvgStateSignedAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        SumStateSignedAgg.build_spec_from_type(func, input_type, input_is_intermediate)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        SumStateSignedAgg.state_layout_for(kind)
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        SumStateSignedAgg.build_input_view(spec, array)
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        SumStateSignedAgg.build_merge_view(spec, array)
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        SumStateSignedAgg.init_state(spec, ptr)
    }

    fn drop_state(&self, spec: &AggSpec, ptr: *mut u8) {
        SumStateSignedAgg.drop_state(spec, ptr)
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        SumStateSignedAgg.update_batch(spec, offset, state_ptrs, input)
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        SumStateSignedAgg.merge_batch(spec, offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        SumStateSignedAgg.build_array(spec, offset, group_states, output_intermediate)
    }
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, Float64Array, Int8Array, Int64Array,
        StructArray,
    };
    use arrow::datatypes::{DataType, Field};

    use crate::connector::starrocks::managed::state_codec::encode_sum_decimal128;
    use crate::exec::change_op::{CHANGE_OP_DELETE, CHANGE_OP_INSERT};
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

    use super::super::super::{AggInputView, AggSpec, AggStatePtr, AggregateFunction};
    use super::super::sum::{SumDecimal128State, SumStateAgg, SumStateSignedAgg};
    use super::*;

    fn agg_func(name: &str) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Binary),
                input_arg_type: None,
            }),
        }
    }

    fn agg_func_with_arg(name: &str, input_arg_type: DataType) -> AggFunction {
        AggFunction {
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Binary),
                input_arg_type: Some(input_arg_type),
            }),
            ..agg_func(name)
        }
    }

    fn agg_func_with_signature(
        name: &str,
        output_type: DataType,
        intermediate_type: DataType,
        input_arg_type: Option<DataType>,
    ) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(intermediate_type),
                output_type: Some(output_type),
                input_arg_type,
            }),
        }
    }

    fn signed_type(value_type: DataType) -> DataType {
        DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("v", value_type, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]))
    }

    fn signed_int_input(values: Vec<Option<i64>>, ops: Vec<Option<i8>>) -> ArrayRef {
        assert_eq!(values.len(), ops.len());
        let value_arr = Arc::new(Int64Array::from(values)) as ArrayRef;
        let op_arr = Arc::new(Int8Array::from(ops)) as ArrayRef;
        Arc::new(StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new("v", DataType::Int64, true)),
                Arc::new(Field::new("op", DataType::Int8, true)),
            ]),
            vec![value_arr, op_arr],
            None,
        )) as ArrayRef
    }

    struct StateCell {
        spec: AggSpec,
        cell: Box<MaybeUninit<SumDecimal128State>>,
    }

    impl StateCell {
        fn new(spec: AggSpec) -> Self {
            let mut cell = Box::new(MaybeUninit::<SumDecimal128State>::uninit());
            let agg = super::super::super::resolve_by_kind(&spec.kind);
            agg.init_state(&spec, cell.as_mut_ptr() as *mut u8);
            Self { spec, cell }
        }

        fn ptr(&mut self) -> AggStatePtr {
            self.cell.as_mut_ptr() as AggStatePtr
        }

        fn update(&mut self, input: AggInputView<'_>, rows: usize) {
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::update_batch(&self.spec, 0, &state_ptrs, &input).unwrap();
        }

        fn merge(&mut self, input: ArrayRef) {
            let rows = input.len();
            let input_slot = Some(input);
            let view = super::super::super::build_merge_view(&self.spec, &input_slot).unwrap();
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::merge_batch(&self.spec, 0, &state_ptrs, &view).unwrap();
        }

        fn finalize(&mut self) -> Vec<u8> {
            let ptr = self.ptr();
            let out = super::super::super::build_array(&self.spec, 0, &[ptr], false).unwrap();
            let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(arr.len(), 1);
            assert!(!arr.is_null(0));
            arr.value(0).to_vec()
        }
    }

    impl Drop for StateCell {
        fn drop(&mut self) {
            super::super::super::drop_state(&self.spec, self.cell.as_mut_ptr() as *mut u8);
        }
    }

    fn bytes_for(
        agg: &dyn AggregateFunction,
        func: &AggFunction,
        input_type: &DataType,
        input: ArrayRef,
    ) -> Vec<u8> {
        let spec = agg
            .build_spec_from_type(func, Some(input_type), false)
            .unwrap();
        let mut cell = StateCell::new(spec);
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();
        cell.update(view, input_slot.as_ref().unwrap().len());
        cell.finalize()
    }

    #[test]
    fn avg_state_state_bytes_equal_sum_state_bytes() {
        let input = Arc::new(Int64Array::from(vec![Some(10), Some(20)])) as ArrayRef;
        let avg_bytes = bytes_for(
            &AvgStateAgg,
            &agg_func("avg_state"),
            &DataType::Int64,
            input.clone(),
        );
        let sum_bytes = bytes_for(
            &SumStateAgg,
            &agg_func("sum_state"),
            &DataType::Int64,
            input,
        );
        assert_eq!(avg_bytes, sum_bytes);
    }

    #[test]
    fn avg_state_signed_bytes_equal_sum_state_signed_bytes() {
        let input = signed_int_input(
            vec![Some(10), Some(5)],
            vec![Some(CHANGE_OP_INSERT), Some(CHANGE_OP_DELETE)],
        );
        let input_type = signed_type(DataType::Int64);
        let avg_bytes = bytes_for(
            &AvgStateSignedAgg,
            &agg_func("avg_state_signed"),
            &input_type,
            input.clone(),
        );
        let sum_bytes = bytes_for(
            &SumStateSignedAgg,
            &agg_func("sum_state_signed"),
            &input_type,
            input,
        );
        assert_eq!(avg_bytes, sum_bytes);
    }

    #[test]
    fn avg_state_decimal128_bytes_equal_sum_state_decimal128_bytes() {
        let input = Arc::new(
            Decimal128Array::from(vec![Some(1_000_000_i128), Some(2_000_000_i128)])
                .with_precision_and_scale(18, 6)
                .unwrap(),
        ) as ArrayRef;
        let input_type = DataType::Decimal128(18, 6);
        let avg_bytes = bytes_for(
            &AvgStateAgg,
            &agg_func("avg_state"),
            &input_type,
            input.clone(),
        );
        let sum_bytes = bytes_for(&SumStateAgg, &agg_func("sum_state"), &input_type, input);
        assert_eq!(avg_bytes, sum_bytes);
    }

    #[test]
    fn avg_state_merge_bytes_equal_sum_state_merge_bytes() {
        let avg_spec = AvgStateAgg
            .build_spec_from_type(
                &agg_func_with_arg("avg_state", DataType::Decimal128(18, 6)),
                Some(&DataType::Binary),
                true,
            )
            .unwrap();
        let sum_spec = SumStateAgg
            .build_spec_from_type(
                &agg_func_with_arg("sum_state", DataType::Decimal128(18, 6)),
                Some(&DataType::Binary),
                true,
            )
            .unwrap();
        let left = encode_sum_decimal128(1, 1_000_000);
        let right = encode_sum_decimal128(1, 2_000_000);
        let input =
            Arc::new(BinaryArray::from(vec![Some(&left[..]), Some(&right[..])])) as ArrayRef;

        let mut avg_cell = StateCell::new(avg_spec);
        avg_cell.merge(input.clone());
        let mut sum_cell = StateCell::new(sum_spec);
        sum_cell.merge(input);

        assert_eq!(avg_cell.finalize(), sum_cell.finalize());
    }

    #[test]
    fn avg_state_registration_resolves_binary_specs() {
        let spec = super::super::super::build_spec_from_type(
            &agg_func_with_arg("avg_state", DataType::Int64),
            Some(&DataType::Int64),
            false,
        )
        .unwrap();
        assert_eq!(spec.output_type, DataType::Binary);
        assert_eq!(spec.intermediate_type, DataType::Binary);

        let signed_ty = signed_type(DataType::Int64);
        let signed_spec = super::super::super::build_spec_from_type(
            &agg_func_with_arg("avg_state_signed", signed_ty.clone()),
            Some(&signed_ty),
            false,
        )
        .unwrap();
        assert_eq!(signed_spec.output_type, DataType::Binary);
        assert_eq!(signed_spec.intermediate_type, DataType::Binary);
    }

    #[test]
    fn avg_state_rejects_float_input() {
        let err = AvgStateAgg
            .build_spec_from_type(&agg_func("avg_state"), Some(&DataType::Float64), false)
            .unwrap_err();
        assert!(err.contains("sum_state does not support Float64"));

        let input = Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef;
        let input_slot = Some(input);
        let spec = AvgStateAgg
            .build_spec_from_type(&agg_func("avg_state"), Some(&DataType::Int64), false)
            .unwrap();
        let err: String = match AvgStateAgg.build_input_view(&spec, &input_slot) {
            Ok(_) => panic!("avg_state float input unexpectedly built a view"),
            Err(err) => err,
        };
        assert!(err.contains("sum_state does not support Float64"));
    }

    #[test]
    fn avg_state_reuses_strict_binary_signature_guard() {
        let output_err = crate::exec::expr::agg::spec::build_spec_from_type(
            &agg_func_with_signature(
                "avg_state",
                DataType::Utf8,
                DataType::Binary,
                Some(DataType::Int64),
            ),
            Some(&DataType::Int64),
            false,
        )
        .unwrap_err();
        assert!(output_err.contains("state combinator output_type must be Binary"));

        let intermediate_err = crate::exec::expr::agg::spec::build_spec_from_type(
            &agg_func_with_signature(
                "avg_state",
                DataType::Binary,
                DataType::Utf8,
                Some(DataType::Int64),
            ),
            Some(&DataType::Int64),
            false,
        )
        .unwrap_err();
        assert!(intermediate_err.contains("state combinator intermediate_type must be Binary"));
    }
}
