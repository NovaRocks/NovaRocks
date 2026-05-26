//! Boolean OR/AND state combinator aggregate functions.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Int8Array, StructArray,
};
use arrow::datatypes::DataType;

use crate::connector::starrocks::managed::state_codec::{decode_bool_state, encode_bool_state};
use crate::exec::change_op::{CHANGE_OP_DELETE, CHANGE_OP_INSERT};
use crate::exec::node::aggregate::AggFunction;

use super::super::{AggInputView, AggKind, AggSpec, AggStatePtr, AggregateFunction};

pub(in crate::exec::expr::agg::functions) struct BoolStateAgg;
pub(in crate::exec::expr::agg::functions) struct BoolStateSignedAgg;

#[derive(Default)]
struct BoolStateCounts {
    count_true: i64,
    count_false: i64,
}

impl AggregateFunction for BoolStateAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        build_bool_state_spec(func, input_type, input_is_intermediate, AggKind::BoolState)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        bool_state_layout_for(kind)
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "bool_state input missing".to_string())?;
        let bool_arr = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "bool_state input must be BooleanArray".to_string())?;
        Ok(AggInputView::Bool(bool_arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_bool_state_merge_view(array)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        init_bool_state(ptr);
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        update_bool_state(offset, state_ptrs, input)
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        merge_bool_state(offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        build_bool_state_array(offset, group_states)
    }
}

impl AggregateFunction for BoolStateSignedAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        build_bool_state_spec(
            func,
            input_type,
            input_is_intermediate,
            AggKind::BoolStateSigned,
        )
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        bool_state_layout_for(kind)
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "bool_state_signed input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_bool_state_merge_view(array)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        init_bool_state(ptr);
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        update_bool_state_signed(offset, state_ptrs, input)
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        merge_bool_state(offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        build_bool_state_array(offset, group_states)
    }
}

fn build_bool_state_spec(
    func: &AggFunction,
    input_type: Option<&DataType>,
    input_is_intermediate: bool,
    kind: AggKind,
) -> Result<AggSpec, String> {
    if !input_is_intermediate {
        let input_type = input_type.ok_or_else(|| match &kind {
            AggKind::BoolState => "bool_state input type missing".to_string(),
            AggKind::BoolStateSigned => "bool_state_signed input type missing".to_string(),
            other => format!("unexpected bool_state kind: {other:?}"),
        })?;
        match kind {
            AggKind::BoolState => {
                if input_type != &DataType::Boolean {
                    return Err(format!(
                        "bool_state expects Boolean input, got {input_type:?}"
                    ));
                }
            }
            AggKind::BoolStateSigned => validate_signed_input_type(input_type)?,
            other => unreachable!("unexpected kind for bool_state: {:?}", other),
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
        count_all: false,
    })
}

fn validate_signed_input_type(input_type: &DataType) -> Result<(), String> {
    let DataType::Struct(fields) = input_type else {
        return Err(format!(
            "bool_state_signed expects struct input (value, change_op), got {:?}",
            input_type
        ));
    };
    if fields.len() != 2 {
        return Err(format!(
            "bool_state_signed expects 2 arguments, got struct with {} fields",
            fields.len()
        ));
    }
    if fields[0].data_type() != &DataType::Boolean {
        return Err(format!(
            "bool_state_signed value must be Boolean, got {:?}",
            fields[0].data_type()
        ));
    }
    if fields[1].data_type() != &DataType::Int8 {
        return Err(format!(
            "bool_state_signed change_op must be Int8, got {:?}",
            fields[1].data_type()
        ));
    }
    Ok(())
}

fn bool_state_layout_for(kind: &AggKind) -> (usize, usize) {
    match kind {
        AggKind::BoolState | AggKind::BoolStateSigned => (
            std::mem::size_of::<BoolStateCounts>(),
            std::mem::align_of::<BoolStateCounts>(),
        ),
        other => unreachable!("unexpected kind for bool_state: {:?}", other),
    }
}

fn build_bool_state_merge_view<'a>(
    array: &'a Option<ArrayRef>,
) -> Result<AggInputView<'a>, String> {
    let arr = array
        .as_ref()
        .ok_or_else(|| "bool_state merge input missing".to_string())?;
    let binary = arr
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "bool_state merge input must be BinaryArray".to_string())?;
    Ok(AggInputView::Binary(binary))
}

fn init_bool_state(ptr: *mut u8) {
    unsafe {
        std::ptr::write(ptr as *mut BoolStateCounts, BoolStateCounts::default());
    }
}

fn state_slot(base: AggStatePtr, offset: usize) -> *mut BoolStateCounts {
    unsafe { (base as *mut u8).add(offset) as *mut BoolStateCounts }
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

fn add_bool_delta(
    state: *mut BoolStateCounts,
    value: bool,
    delta: i64,
    context: &str,
) -> Result<(), String> {
    unsafe {
        if value {
            add_delta(std::ptr::addr_of_mut!((*state).count_true), delta, context)
        } else {
            add_delta(std::ptr::addr_of_mut!((*state).count_false), delta, context)
        }
    }
}

fn update_bool_state(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Bool(array) = input else {
        return Err("bool_state batch input type mismatch".to_string());
    };
    for (row, &base) in state_ptrs.iter().enumerate() {
        if array.is_null(row) {
            continue;
        }
        add_bool_delta(state_slot(base, offset), array.value(row), 1, "bool_state")?;
    }
    Ok(())
}

fn update_bool_state_signed(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err("bool_state_signed batch input type mismatch".to_string());
    };
    let struct_arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "bool_state_signed expects struct input".to_string())?;
    if struct_arr.num_columns() != 2 {
        return Err(format!(
            "bool_state_signed expects 2 arguments, got {}",
            struct_arr.num_columns()
        ));
    }
    let value_arr_ref = struct_arr.column(0);
    let value_arr = value_arr_ref
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            format!(
                "bool_state_signed value must be Boolean, got {:?}",
                value_arr_ref.data_type()
            )
        })?;
    let op_arr_ref = struct_arr.column(1);
    let op_arr = op_arr_ref
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| {
            format!(
                "bool_state_signed change_op must be Int8, got {:?}",
                op_arr_ref.data_type()
            )
        })?;

    for (row, &base) in state_ptrs.iter().enumerate() {
        if struct_arr.is_null(row) || value_arr.is_null(row) {
            continue;
        }
        let delta = if op_arr.is_null(row) {
            0
        } else {
            match op_arr.value(row) {
                CHANGE_OP_INSERT => 1,
                CHANGE_OP_DELETE => -1,
                other => return Err(format!("unknown bool_state_signed change_op: {other}")),
            }
        };
        add_bool_delta(
            state_slot(base, offset),
            value_arr.value(row),
            delta,
            "bool_state_signed",
        )?;
    }
    Ok(())
}

fn merge_bool_state(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Binary(array) = input else {
        return Err("bool_state merge input type mismatch".to_string());
    };
    for (row, &base) in state_ptrs.iter().enumerate() {
        if array.is_null(row) {
            continue;
        }
        let (count_true, count_false) = decode_bool_state(array.value(row))?;
        let state = state_slot(base, offset);
        add_bool_delta(state, true, count_true, "bool_state merge")?;
        add_bool_delta(state, false, count_false, "bool_state merge")?;
    }
    Ok(())
}

fn build_bool_state_array(offset: usize, group_states: &[AggStatePtr]) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for &base in group_states {
        let state = unsafe { &*state_slot(base, offset) };
        if state.count_true == 0 && state.count_false == 0 {
            builder.append_value([]);
        } else {
            builder.append_value(encode_bool_state(state.count_true, state.count_false));
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, StructArray};
    use arrow::datatypes::{DataType, Field};
    use arrow_buffer::NullBufferBuilder;

    use crate::connector::starrocks::managed::state_codec::{decode_bool_state, encode_bool_state};
    use crate::exec::change_op::{CHANGE_OP_DELETE, CHANGE_OP_INSERT};
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

    use super::super::super::{AggInputView, AggKind, AggSpec, AggStatePtr, AggregateFunction};
    use super::*;

    fn bool_func(name: &str) -> AggFunction {
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

    fn build_spec(name: &str) -> AggSpec {
        BoolStateAgg
            .build_spec_from_type(&bool_func(name), Some(&DataType::Boolean), false)
            .unwrap()
    }

    fn build_signed_spec(name: &str) -> AggSpec {
        let input_type = signed_input_type();
        BoolStateSignedAgg
            .build_spec_from_type(&bool_func(name), Some(&input_type), false)
            .unwrap()
    }

    fn signed_input_type() -> DataType {
        DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("v", DataType::Boolean, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]))
    }

    struct StateCell {
        spec: AggSpec,
        cell: Box<MaybeUninit<BoolStateCounts>>,
    }

    impl StateCell {
        fn new(spec: AggSpec) -> Self {
            let mut cell = Box::new(MaybeUninit::<BoolStateCounts>::uninit());
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

        fn try_update(&mut self, input: AggInputView<'_>, rows: usize) -> Result<(), String> {
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

    fn final_counts(out: &ArrayRef) -> (i64, i64) {
        decode_bool_state(final_bytes(out)).unwrap()
    }

    fn signed_input(values: Vec<Option<bool>>, ops: Vec<Option<i8>>) -> ArrayRef {
        assert_eq!(values.len(), ops.len());
        let value_arr = Arc::new(BooleanArray::from(values)) as ArrayRef;
        let op_arr = Arc::new(Int8Array::from(ops)) as ArrayRef;
        Arc::new(StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new("v", DataType::Boolean, true)),
                Arc::new(Field::new("op", DataType::Int8, true)),
            ]),
            vec![value_arr, op_arr],
            None,
        )) as ArrayRef
    }

    fn signed_input_with_struct_nulls(
        values: Vec<Option<bool>>,
        ops: Vec<Option<i8>>,
        struct_valid: Vec<bool>,
    ) -> ArrayRef {
        assert_eq!(values.len(), ops.len());
        assert_eq!(values.len(), struct_valid.len());
        let value_arr = Arc::new(BooleanArray::from(values)) as ArrayRef;
        let op_arr = Arc::new(Int8Array::from(ops)) as ArrayRef;
        let mut struct_nulls = NullBufferBuilder::new(struct_valid.len());
        for valid in struct_valid {
            if valid {
                struct_nulls.append_non_null();
            } else {
                struct_nulls.append_null();
            }
        }
        Arc::new(StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new("v", DataType::Boolean, true)),
                Arc::new(Field::new("op", DataType::Int8, true)),
            ]),
            vec![value_arr, op_arr],
            struct_nulls.finish(),
        )) as ArrayRef
    }

    #[test]
    fn bool_or_state_counts_true_and_false() {
        let spec = build_spec("bool_or_state");
        let mut cell = StateCell::new(spec);
        let input = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            None,
            Some(true),
        ])) as ArrayRef;
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 5);

        assert_eq!(final_counts(&cell.finalize()), (2, 2));
    }

    #[test]
    fn bool_and_state_same_bytes_as_bool_or_state() {
        let input = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            None,
            Some(true),
        ])) as ArrayRef;

        let mut or_cell = StateCell::new(build_spec("bool_or_state"));
        let or_slot = Some(input.clone());
        let or_view = super::super::super::build_input_view(&or_cell.spec, &or_slot).unwrap();
        or_cell.update(or_view, 5);

        let mut and_cell = StateCell::new(build_spec("bool_and_state"));
        let and_slot = Some(input);
        let and_view = super::super::super::build_input_view(&and_cell.spec, &and_slot).unwrap();
        and_cell.update(and_view, 5);

        assert_eq!(
            final_bytes(&or_cell.finalize()),
            final_bytes(&and_cell.finalize())
        );
    }

    #[test]
    fn bool_or_state_signed_handles_insert_delete() {
        let spec = build_signed_spec("bool_or_state_signed");
        let mut cell = StateCell::new(spec);
        let input = signed_input(
            vec![Some(true), Some(false), Some(true)],
            vec![
                Some(CHANGE_OP_INSERT),
                Some(CHANGE_OP_INSERT),
                Some(CHANGE_OP_DELETE),
            ],
        );
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 3);

        assert_eq!(final_counts(&cell.finalize()), (0, 1));
    }

    #[test]
    fn bool_and_state_signed_same_bytes_as_bool_or_state_signed() {
        let input = signed_input(
            vec![Some(true), Some(false), Some(true)],
            vec![
                Some(CHANGE_OP_INSERT),
                Some(CHANGE_OP_INSERT),
                Some(CHANGE_OP_DELETE),
            ],
        );

        let mut or_cell = StateCell::new(build_signed_spec("bool_or_state_signed"));
        let or_slot = Some(input.clone());
        let or_view = super::super::super::build_input_view(&or_cell.spec, &or_slot).unwrap();
        or_cell.update(or_view, 3);

        let mut and_cell = StateCell::new(build_signed_spec("bool_and_state_signed"));
        let and_slot = Some(input);
        let and_view = super::super::super::build_input_view(&and_cell.spec, &and_slot).unwrap();
        and_cell.update(and_view, 3);

        assert_eq!(
            final_bytes(&or_cell.finalize()),
            final_bytes(&and_cell.finalize())
        );
    }

    #[test]
    fn bool_state_zero_finalizes_empty_bytes() {
        let spec = build_spec("bool_or_state");
        let mut cell = StateCell::new(spec);
        let input = Arc::new(BooleanArray::from(vec![None, None])) as ArrayRef;
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 2);

        assert!(final_bytes(&cell.finalize()).is_empty());
    }

    #[test]
    fn bool_state_signed_skips_null_value_and_null_op_zero() {
        let spec = build_signed_spec("bool_or_state_signed");
        let mut cell = StateCell::new(spec);
        let input = signed_input(
            vec![Some(true), None, Some(false)],
            vec![Some(CHANGE_OP_INSERT), Some(CHANGE_OP_INSERT), None],
        );
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 3);

        assert_eq!(final_counts(&cell.finalize()), (1, 0));
    }

    #[test]
    fn bool_state_signed_skips_struct_null_rows() {
        let spec = build_signed_spec("bool_or_state_signed");
        let mut cell = StateCell::new(spec);
        let input = signed_input_with_struct_nulls(
            vec![Some(true), Some(false), Some(true)],
            vec![
                Some(CHANGE_OP_INSERT),
                Some(CHANGE_OP_INSERT),
                Some(CHANGE_OP_DELETE),
            ],
            vec![true, false, true],
        );
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 3);

        assert_eq!(final_counts(&cell.finalize()), (0, 0));
    }

    #[test]
    fn bool_state_signed_rejects_unknown_op() {
        let spec = build_signed_spec("bool_or_state_signed");
        let mut cell = StateCell::new(spec);
        let input = signed_input(vec![Some(true)], vec![Some(2)]);
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        let err = cell.try_update(view, 1).unwrap_err();

        assert!(err.contains("unknown bool_state_signed change_op"));
    }

    #[test]
    fn bool_state_merge_decodes_binary_states() {
        let spec = build_spec("bool_or_state");
        let mut cell = StateCell::new(spec);
        let first = encode_bool_state(2, 1);
        let second = encode_bool_state(-1, 3);
        let input = Arc::new(BinaryArray::from(vec![
            Some(&first[..]),
            Some(&[][..]),
            Some(&second[..]),
        ])) as ArrayRef;

        cell.merge(input);

        assert_eq!(final_counts(&cell.finalize()), (1, 4));
    }

    #[test]
    fn bool_state_registration_resolves_binary_specs() {
        for (name, signed) in [
            ("bool_or_state", false),
            ("bool_and_state", false),
            ("bool_or_state_signed", true),
            ("bool_and_state_signed", true),
        ] {
            let input_type = if signed {
                signed_input_type()
            } else {
                DataType::Boolean
            };
            let spec = super::super::super::build_spec_from_type(
                &bool_func(name),
                Some(&input_type),
                false,
            )
            .unwrap();
            assert!(matches!(
                spec.kind,
                AggKind::BoolState | AggKind::BoolStateSigned
            ));
            assert_eq!(spec.output_type, DataType::Binary);
            assert_eq!(spec.intermediate_type, DataType::Binary);
            assert!(!spec.count_all);
        }
    }
}
