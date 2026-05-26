//! Count state combinator aggregate functions.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, Int8Array, StructArray};
use arrow::datatypes::DataType;

use crate::connector::starrocks::managed::state_codec::{decode_count_state, encode_count_state};
use crate::exec::node::aggregate::AggFunction;

use super::super::{AggInputView, AggKind, AggSpec, AggStatePtr, AggregateFunction};

pub(in crate::exec::expr::agg::functions) struct CountStateAgg;
pub(in crate::exec::expr::agg::functions) struct CountStateSignedAgg;

impl AggregateFunction for CountStateAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        build_count_state_spec(func, input_type, input_is_intermediate, AggKind::CountState)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        count_state_layout_for(kind)
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_count_state_input_view(spec, array)
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_count_state_merge_view(array)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        init_i64_state(ptr);
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        update_count_state(spec, offset, state_ptrs, input)
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        merge_count_state(offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        build_count_state_array(offset, group_states)
    }
}

impl AggregateFunction for CountStateSignedAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        build_count_state_spec(
            func,
            input_type,
            input_is_intermediate,
            AggKind::CountStateSigned,
        )
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        count_state_layout_for(kind)
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "count_state_signed input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_count_state_merge_view(array)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        init_i64_state(ptr);
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        update_count_state_signed(offset, state_ptrs, input)
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        merge_count_state(offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        build_count_state_array(offset, group_states)
    }
}

fn build_count_state_spec(
    func: &AggFunction,
    input_type: Option<&DataType>,
    input_is_intermediate: bool,
    kind: AggKind,
) -> Result<AggSpec, String> {
    let count_all = matches!(kind, AggKind::CountState) && input_type.is_none();
    if matches!(kind, AggKind::CountStateSigned) && !input_is_intermediate {
        let input_type =
            input_type.ok_or_else(|| "count_state_signed input type missing".to_string())?;
        let DataType::Struct(fields) = input_type else {
            return Err(format!(
                "count_state_signed expects struct input (value, change_op), got {:?}",
                input_type
            ));
        };
        if fields.len() != 2 {
            return Err(format!(
                "count_state_signed expects 2 arguments, got struct with {} fields",
                fields.len()
            ));
        }
        if fields[1].data_type() != &DataType::Int8 {
            return Err(format!(
                "count_state_signed change_op must be Int8, got {:?}",
                fields[1].data_type()
            ));
        }
    }

    Ok(AggSpec {
        kind,
        output_type: func
            .types
            .as_ref()
            .and_then(|t| t.output_type.clone())
            .unwrap_or(DataType::Binary),
        intermediate_type: func
            .types
            .as_ref()
            .and_then(|t| t.intermediate_type.clone())
            .unwrap_or(DataType::Binary),
        input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
        count_all,
    })
}

fn count_state_layout_for(kind: &AggKind) -> (usize, usize) {
    match kind {
        AggKind::CountState | AggKind::CountStateSigned => {
            (std::mem::size_of::<i64>(), std::mem::align_of::<i64>())
        }
        other => unreachable!("unexpected kind for count_state: {:?}", other),
    }
}

fn build_count_state_input_view<'a>(
    spec: &AggSpec,
    array: &'a Option<ArrayRef>,
) -> Result<AggInputView<'a>, String> {
    if spec.count_all {
        Ok(AggInputView::None)
    } else {
        let arr = array
            .as_ref()
            .ok_or_else(|| "count_state input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }
}

fn build_count_state_merge_view<'a>(
    array: &'a Option<ArrayRef>,
) -> Result<AggInputView<'a>, String> {
    let arr = array
        .as_ref()
        .ok_or_else(|| "count_state merge input missing".to_string())?;
    let binary = arr
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "count_state merge input must be BinaryArray".to_string())?;
    Ok(AggInputView::Binary(binary))
}

fn init_i64_state(ptr: *mut u8) {
    unsafe {
        std::ptr::write(ptr as *mut i64, 0);
    }
}

fn state_slot(base: AggStatePtr, offset: usize) -> *mut i64 {
    unsafe { (base as *mut u8).add(offset) as *mut i64 }
}

fn add_delta(slot: *mut i64, delta: i64, context: &str) -> Result<(), String> {
    unsafe {
        let current = *slot;
        let next = current
            .checked_add(delta)
            .ok_or_else(|| format!("{context} overflow while adding count delta"))?;
        *slot = next;
    }
    Ok(())
}

fn update_count_state(
    spec: &AggSpec,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::None => {
            for &base in state_ptrs {
                add_delta(state_slot(base, offset), 1, "count_state")?;
            }
            Ok(())
        }
        AggInputView::Any(array) => {
            if spec.count_all {
                for &base in state_ptrs {
                    add_delta(state_slot(base, offset), 1, "count_state")?;
                }
                return Ok(());
            }
            for (row, &base) in state_ptrs.iter().enumerate() {
                if !array.is_null(row) {
                    add_delta(state_slot(base, offset), 1, "count_state")?;
                }
            }
            Ok(())
        }
        _ => Err("count_state batch input type mismatch".to_string()),
    }
}

fn update_count_state_signed(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err("count_state_signed batch input type mismatch".to_string());
    };
    let struct_arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "count_state_signed expects struct input".to_string())?;
    if struct_arr.num_columns() != 2 {
        return Err(format!(
            "count_state_signed expects 2 arguments, got {}",
            struct_arr.num_columns()
        ));
    }
    let key_arr = struct_arr.column(0);
    let op_arr_ref = struct_arr.column(1);
    let op_arr = op_arr_ref
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| {
            format!(
                "count_state_signed change_op must be Int8, got {:?}",
                op_arr_ref.data_type()
            )
        })?;

    for (row, &base) in state_ptrs.iter().enumerate() {
        if struct_arr.is_null(row) || key_arr.is_null(row) {
            continue;
        }
        let delta = if op_arr.is_null(row) {
            0
        } else {
            match op_arr.value(row) {
                0 => 1,
                1 => -1,
                other => {
                    return Err(format!("unknown count_state_signed change_op: {other}"));
                }
            }
        };
        add_delta(state_slot(base, offset), delta, "count_state_signed")?;
    }
    Ok(())
}

fn merge_count_state(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Binary(array) = input else {
        return Err("count_state merge input type mismatch".to_string());
    };
    for (row, &base) in state_ptrs.iter().enumerate() {
        if array.is_null(row) {
            continue;
        }
        let delta = decode_count_state(array.value(row))?;
        add_delta(state_slot(base, offset), delta, "count_state merge")?;
    }
    Ok(())
}

fn build_count_state_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for &base in group_states {
        let count = unsafe { *state_slot(base, offset) };
        if count == 0 {
            builder.append_value([]);
        } else {
            builder.append_value(encode_count_state(count));
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BinaryArray, Int8Array, Int64Array, StructArray};
    use arrow::datatypes::{DataType, Field};

    use crate::connector::starrocks::managed::state_codec::{
        decode_count_state, encode_count_state,
    };
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

    use super::super::super::{AggKind, AggSpec, AggStatePtr, AggregateFunction};
    use super::*;

    fn count_func(name: &str) -> AggFunction {
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

    fn count_func_with_signature(
        name: &str,
        output_type: DataType,
        intermediate_type: DataType,
    ) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(intermediate_type),
                output_type: Some(output_type),
                input_arg_type: None,
            }),
        }
    }

    fn build_spec(name: &str, input_type: Option<&DataType>) -> AggSpec {
        CountStateAgg
            .build_spec_from_type(&count_func(name), input_type, false)
            .unwrap()
    }

    fn build_signed_spec(input_type: &DataType) -> AggSpec {
        CountStateSignedAgg
            .build_spec_from_type(&count_func("count_state_signed"), Some(input_type), false)
            .unwrap()
    }

    struct StateCell {
        spec: AggSpec,
        cell: Box<MaybeUninit<i64>>,
    }

    impl StateCell {
        fn new(spec: AggSpec) -> Self {
            let mut cell = Box::new(MaybeUninit::<i64>::uninit());
            let agg = super::super::super::resolve_by_kind(&spec.kind);
            agg.init_state(&spec, cell.as_mut_ptr() as *mut u8);
            Self { spec, cell }
        }

        fn ptr(&mut self) -> AggStatePtr {
            self.cell.as_mut_ptr() as AggStatePtr
        }

        fn update(&mut self, input: super::super::super::AggInputView<'_>, rows: usize) {
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::update_batch(&self.spec, 0, &state_ptrs, &input).unwrap();
        }

        fn try_update(
            &mut self,
            input: super::super::super::AggInputView<'_>,
            rows: usize,
        ) -> Result<(), String> {
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::update_batch(&self.spec, 0, &state_ptrs, &input)
        }

        fn merge(&mut self, input: ArrayRef) {
            let rows = input.len();
            let input_slot = Some(input);
            let view = super::super::super::build_merge_view(&self.spec, &input_slot).unwrap();
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::merge_batch(&self.spec, 0, &state_ptrs, &view).unwrap();
        }

        fn finalize(&mut self) -> ArrayRef {
            let ptr = self.ptr();
            super::super::super::build_array(&self.spec, 0, &[ptr], false).unwrap()
        }
    }

    impl Drop for StateCell {
        fn drop(&mut self) {
            super::super::super::drop_state(&self.spec, self.cell.as_mut_ptr() as *mut u8);
        }
    }

    fn final_bytes(out: &ArrayRef) -> &[u8] {
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(arr.len(), 1);
        assert!(!arr.is_null(0));
        arr.value(0)
    }

    fn final_count(out: &ArrayRef) -> i64 {
        decode_count_state(final_bytes(out)).unwrap()
    }

    fn signed_input(keys: Vec<Option<i64>>, ops: Vec<Option<i8>>) -> ArrayRef {
        assert_eq!(keys.len(), ops.len());
        let key_arr = Arc::new(Int64Array::from(keys)) as ArrayRef;
        let op_arr = Arc::new(Int8Array::from(ops)) as ArrayRef;
        Arc::new(StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new("k", DataType::Int64, true)),
                Arc::new(Field::new("op", DataType::Int8, true)),
            ]),
            vec![key_arr, op_arr],
            None,
        )) as ArrayRef
    }

    #[test]
    fn count_state_counts_non_null_rows() {
        let spec = build_spec("count_state", Some(&DataType::Int64));
        let mut cell = StateCell::new(spec);
        let input = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(3)])) as ArrayRef;
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 4);

        assert_eq!(final_count(&cell.finalize()), 3);
    }

    #[test]
    fn count_state_count_all_counts_every_row() {
        let spec = build_spec("count_state", None);
        assert!(matches!(spec.kind, AggKind::CountState));
        assert!(spec.count_all);
        let mut cell = StateCell::new(spec);

        cell.update(super::super::super::AggInputView::None, 4);

        assert_eq!(final_count(&cell.finalize()), 4);
    }

    #[test]
    fn count_state_zero_finalizes_empty_bytes() {
        let spec = build_spec("count_state", Some(&DataType::Int64));
        let mut cell = StateCell::new(spec);
        let input = Arc::new(Int64Array::from(vec![None, None])) as ArrayRef;
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 2);

        assert!(final_bytes(&cell.finalize()).is_empty());
    }

    #[test]
    fn count_state_signed_handles_insert_delete() {
        let input_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]));
        let spec = build_signed_spec(&input_type);
        let mut cell = StateCell::new(spec);
        let input = signed_input(
            vec![Some(1), Some(2), Some(3)],
            vec![Some(0), Some(1), Some(0)],
        );
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 3);

        assert_eq!(final_count(&cell.finalize()), 1);
    }

    #[test]
    fn count_state_signed_skips_null_key_and_null_op_zero() {
        let input_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]));
        let spec = build_signed_spec(&input_type);
        let mut cell = StateCell::new(spec);
        let input = signed_input(vec![Some(1), None, Some(3)], vec![Some(0), Some(0), None]);
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 3);

        assert_eq!(final_count(&cell.finalize()), 1);
    }

    #[test]
    fn count_state_signed_rejects_unknown_op() {
        let input_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]));
        let spec = build_signed_spec(&input_type);
        let mut cell = StateCell::new(spec);
        let input = signed_input(vec![Some(1)], vec![Some(2)]);
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        let err = cell.try_update(view, 1).unwrap_err();

        assert!(err.contains("unknown count_state_signed change_op"));
    }

    #[test]
    fn count_state_merge_decodes_binary_states() {
        let spec = build_spec("count_state", Some(&DataType::Binary));
        let mut cell = StateCell::new(spec);
        let two = encode_count_state(2);
        let neg_one = encode_count_state(-1);
        let input = Arc::new(BinaryArray::from(vec![
            Some(&two[..]),
            Some(&[][..]),
            Some(&neg_one[..]),
        ])) as ArrayRef;

        cell.merge(input);

        assert_eq!(final_count(&cell.finalize()), 1);
    }

    #[test]
    fn count_state_registration_resolves_binary_specs() {
        let count_spec =
            super::super::super::build_spec_from_type(&count_func("count_state"), None, false)
                .unwrap();
        assert!(matches!(count_spec.kind, AggKind::CountState));
        assert_eq!(count_spec.output_type, DataType::Binary);
        assert_eq!(count_spec.intermediate_type, DataType::Binary);

        let input_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]));
        let signed_spec = super::super::super::build_spec_from_type(
            &count_func("count_state_signed"),
            Some(&input_type),
            false,
        )
        .unwrap();
        assert!(matches!(signed_spec.kind, AggKind::CountStateSigned));
        assert_eq!(signed_spec.output_type, DataType::Binary);
        assert_eq!(signed_spec.intermediate_type, DataType::Binary);
    }

    #[test]
    fn count_state_rejects_utf8_type_signature() {
        let err = crate::exec::expr::agg::spec::build_spec_from_type(
            &count_func_with_signature("count_state", DataType::Utf8, DataType::Binary),
            Some(&DataType::Int64),
            false,
        )
        .unwrap_err();

        assert!(err.contains("state combinator output_type must be Binary"));
    }
}
