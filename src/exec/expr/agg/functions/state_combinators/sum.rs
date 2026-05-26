//! Sum state combinator aggregate functions.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Decimal128Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StructArray,
};
use arrow::datatypes::DataType;

use crate::connector::starrocks::managed::state_codec::{
    decode_sum_decimal128, decode_sum_int64, encode_sum_decimal128, encode_sum_int64,
};
use crate::exec::node::aggregate::AggFunction;

use super::super::{AggInputView, AggKind, AggSpec, AggStatePtr, AggregateFunction};

pub(in crate::exec::expr::agg::functions) struct SumStateAgg;
pub(in crate::exec::expr::agg::functions) struct SumStateSignedAgg;

#[derive(Default)]
pub(in crate::exec::expr::agg::functions) struct SumInt64State {
    row_count: i64,
    sum: i64,
}

#[derive(Default)]
pub(in crate::exec::expr::agg::functions) struct SumDecimal128State {
    row_count: i64,
    sum: i128,
}

#[cfg(test)]
type SumState = SumDecimal128State;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SumStateKind {
    Int64,
    Decimal128,
}

impl AggregateFunction for SumStateAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        build_sum_state_spec(func, input_type, input_is_intermediate, false)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        sum_state_layout_for(kind)
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_sum_state_input_view("sum_state", spec, array)
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_sum_state_merge_view("sum_state", array)
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        init_sum_state(&spec.kind, ptr);
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        update_sum_state("sum_state", spec, offset, state_ptrs, input)
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        merge_sum_state("sum_state", spec, offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        build_sum_state_array(spec, offset, group_states)
    }
}

impl AggregateFunction for SumStateSignedAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        build_sum_state_spec(func, input_type, input_is_intermediate, true)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        sum_state_layout_for(kind)
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_sum_state_input_view("sum_state_signed", spec, array)
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_sum_state_merge_view("sum_state_signed", array)
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        init_sum_state(&spec.kind, ptr);
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        update_sum_state("sum_state_signed", spec, offset, state_ptrs, input)
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        merge_sum_state("sum_state_signed", spec, offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        build_sum_state_array(spec, offset, group_states)
    }
}

fn build_sum_state_spec(
    func: &AggFunction,
    input_type: Option<&DataType>,
    input_is_intermediate: bool,
    signed: bool,
) -> Result<AggSpec, String> {
    let name = if signed {
        "sum_state_signed"
    } else {
        "sum_state"
    };
    let declared_arg_type = func.types.as_ref().and_then(|t| t.input_arg_type.as_ref());
    let state_kind = if input_is_intermediate {
        let arg_type = declared_arg_type
            .ok_or_else(|| format!("{name} merge requires original logical input type"))?;
        sum_state_kind_from_logical_input(name, arg_type, signed)?
    } else {
        let input_type = input_type.ok_or_else(|| format!("{name} input type missing"))?;
        sum_state_kind_from_logical_input(name, input_type, signed)?
    };

    let kind = match (signed, state_kind) {
        (false, SumStateKind::Int64) => AggKind::SumStateInt64,
        (false, SumStateKind::Decimal128) => AggKind::SumStateDecimal128,
        (true, SumStateKind::Int64) => AggKind::SumStateSignedInt64,
        (true, SumStateKind::Decimal128) => AggKind::SumStateSignedDecimal128,
    };

    Ok(AggSpec {
        kind,
        output_type: DataType::Binary,
        intermediate_type: DataType::Binary,
        input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
        count_all: false,
    })
}

fn sum_state_kind_from_logical_input(
    name: &str,
    input_type: &DataType,
    signed: bool,
) -> Result<SumStateKind, String> {
    if signed {
        let DataType::Struct(fields) = input_type else {
            return Err(format!(
                "{name} expects struct input (value, change_op), got {:?}",
                input_type
            ));
        };
        if fields.len() != 2 {
            return Err(format!(
                "{name} expects 2 arguments, got struct with {} fields",
                fields.len()
            ));
        }
        if fields[1].data_type() != &DataType::Int8 {
            return Err(format!(
                "{name} change_op must be Int8, got {:?}",
                fields[1].data_type()
            ));
        }
        return sum_state_kind_from_value_type(name, fields[0].data_type());
    }
    sum_state_kind_from_value_type(name, input_type)
}

fn sum_state_kind_from_value_type(
    name: &str,
    data_type: &DataType,
) -> Result<SumStateKind, String> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Ok(SumStateKind::Int64)
        }
        DataType::Decimal128(_, _) => Ok(SumStateKind::Decimal128),
        DataType::Float32 | DataType::Float64 => {
            Err(format!("{name} does not support {data_type:?} input"))
        }
        other => Err(format!("{name} unsupported input type: {other:?}")),
    }
}

fn sum_state_layout_for(kind: &AggKind) -> (usize, usize) {
    match kind {
        AggKind::SumStateInt64 | AggKind::SumStateSignedInt64 => (
            std::mem::size_of::<SumInt64State>(),
            std::mem::align_of::<SumInt64State>(),
        ),
        AggKind::SumStateDecimal128 | AggKind::SumStateSignedDecimal128 => (
            std::mem::size_of::<SumDecimal128State>(),
            std::mem::align_of::<SumDecimal128State>(),
        ),
        other => unreachable!("unexpected kind for sum_state: {:?}", other),
    }
}

fn build_sum_state_input_view<'a>(
    name: &str,
    spec: &AggSpec,
    array: &'a Option<ArrayRef>,
) -> Result<AggInputView<'a>, String> {
    let arr = array
        .as_ref()
        .ok_or_else(|| format!("{name} input missing"))?;
    match &spec.kind {
        AggKind::SumStateInt64 | AggKind::SumStateDecimal128 => {
            let actual = sum_state_kind_from_value_type(name, arr.data_type())?;
            ensure_expected_state_kind(name, &spec.kind, actual)?;
        }
        AggKind::SumStateSignedInt64 | AggKind::SumStateSignedDecimal128 => {
            let actual = sum_state_kind_from_logical_input(name, arr.data_type(), true)?;
            ensure_expected_state_kind(name, &spec.kind, actual)?;
        }
        other => return Err(format!("{name} input kind mismatch: {other:?}")),
    }
    Ok(AggInputView::Any(arr))
}

fn ensure_expected_state_kind(
    name: &str,
    kind: &AggKind,
    actual: SumStateKind,
) -> Result<(), String> {
    let expected = match kind {
        AggKind::SumStateInt64 | AggKind::SumStateSignedInt64 => SumStateKind::Int64,
        AggKind::SumStateDecimal128 | AggKind::SumStateSignedDecimal128 => SumStateKind::Decimal128,
        other => return Err(format!("{name} input kind mismatch: {other:?}")),
    };
    if expected != actual {
        return Err(format!(
            "{name} input type does not match selected state kind: expected {:?}, got {:?}",
            expected, actual
        ));
    }
    Ok(())
}

fn build_sum_state_merge_view<'a>(
    name: &str,
    array: &'a Option<ArrayRef>,
) -> Result<AggInputView<'a>, String> {
    let arr = array
        .as_ref()
        .ok_or_else(|| format!("{name} merge input missing"))?;
    let binary = arr
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| format!("{name} merge input must be BinaryArray"))?;
    Ok(AggInputView::Binary(binary))
}

fn init_sum_state(kind: &AggKind, ptr: *mut u8) {
    match kind {
        AggKind::SumStateInt64 | AggKind::SumStateSignedInt64 => unsafe {
            std::ptr::write(ptr as *mut SumInt64State, SumInt64State::default());
        },
        AggKind::SumStateDecimal128 | AggKind::SumStateSignedDecimal128 => unsafe {
            std::ptr::write(
                ptr as *mut SumDecimal128State,
                SumDecimal128State::default(),
            );
        },
        _ => {}
    }
}

fn int_state_slot(base: AggStatePtr, offset: usize) -> *mut SumInt64State {
    unsafe { (base as *mut u8).add(offset) as *mut SumInt64State }
}

fn decimal_state_slot(base: AggStatePtr, offset: usize) -> *mut SumDecimal128State {
    unsafe { (base as *mut u8).add(offset) as *mut SumDecimal128State }
}

fn add_int_delta(
    state: *mut SumInt64State,
    row_count_delta: i64,
    sum_delta: i64,
    context: &str,
) -> Result<(), String> {
    unsafe {
        (*state).row_count = (*state)
            .row_count
            .checked_add(row_count_delta)
            .ok_or_else(|| format!("{context} overflow while adding row_count delta"))?;
        (*state).sum = (*state)
            .sum
            .checked_add(sum_delta)
            .ok_or_else(|| format!("{context} overflow while adding sum delta"))?;
    }
    Ok(())
}

fn add_decimal_delta(
    state: *mut SumDecimal128State,
    row_count_delta: i64,
    sum_delta: i128,
    context: &str,
) -> Result<(), String> {
    unsafe {
        (*state).row_count = (*state)
            .row_count
            .checked_add(row_count_delta)
            .ok_or_else(|| format!("{context} overflow while adding row_count delta"))?;
        (*state).sum = (*state)
            .sum
            .checked_add(sum_delta)
            .ok_or_else(|| format!("{context} overflow while adding sum delta"))?;
    }
    Ok(())
}

fn update_sum_state(
    name: &str,
    spec: &AggSpec,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match spec.kind {
        AggKind::SumStateInt64 => update_sum_state_int(name, offset, state_ptrs, input),
        AggKind::SumStateDecimal128 => update_sum_state_decimal(name, offset, state_ptrs, input),
        AggKind::SumStateSignedInt64 => {
            update_sum_state_signed_int(name, offset, state_ptrs, input)
        }
        AggKind::SumStateSignedDecimal128 => {
            update_sum_state_signed_decimal(name, offset, state_ptrs, input)
        }
        _ => Err(format!("{name} update kind mismatch")),
    }
}

fn update_sum_state_int(
    name: &str,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err(format!("{name} batch input type mismatch"));
    };
    for (row, &base) in state_ptrs.iter().enumerate() {
        if let Some(value) = int_value_at(name, array, row)? {
            add_int_delta(int_state_slot(base, offset), 1, value, name)?;
        }
    }
    Ok(())
}

fn update_sum_state_decimal(
    name: &str,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err(format!("{name} batch input type mismatch"));
    };
    let decimal = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            format!(
                "{name} expects Decimal128 input, got {:?}",
                array.data_type()
            )
        })?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if decimal.is_null(row) {
            continue;
        }
        add_decimal_delta(
            decimal_state_slot(base, offset),
            1,
            decimal.value(row),
            name,
        )?;
    }
    Ok(())
}

fn update_sum_state_signed_int(
    name: &str,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let (struct_arr, value_arr, op_arr) = signed_parts(name, input)?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if struct_arr.is_null(row) {
            continue;
        }
        let Some(value) = int_value_at(name, value_arr, row)? else {
            continue;
        };
        let Some((row_count_delta, sign)) = signed_delta(name, op_arr, row)? else {
            continue;
        };
        let sum_delta = value
            .checked_mul(sign)
            .ok_or_else(|| format!("{name} overflow while applying change_op"))?;
        add_int_delta(
            int_state_slot(base, offset),
            row_count_delta,
            sum_delta,
            name,
        )?;
    }
    Ok(())
}

fn update_sum_state_signed_decimal(
    name: &str,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let (struct_arr, value_arr, op_arr) = signed_parts(name, input)?;
    let decimal = value_arr
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            format!(
                "{name} value must be Decimal128, got {:?}",
                value_arr.data_type()
            )
        })?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if struct_arr.is_null(row) || decimal.is_null(row) {
            continue;
        }
        let Some((row_count_delta, sign)) = signed_delta(name, op_arr, row)? else {
            continue;
        };
        let sum_delta = decimal
            .value(row)
            .checked_mul(sign as i128)
            .ok_or_else(|| format!("{name} overflow while applying change_op"))?;
        add_decimal_delta(
            decimal_state_slot(base, offset),
            row_count_delta,
            sum_delta,
            name,
        )?;
    }
    Ok(())
}

fn int_value_at(name: &str, array: &ArrayRef, row: usize) -> Result<Option<i64>, String> {
    macro_rules! typed_value {
        ($ty:ty) => {{
            let arr = array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| format!("failed to downcast {:?} input", array.data_type()))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(arr.value(row) as i64))
            }
        }};
    }
    match array.data_type() {
        DataType::Int8 => typed_value!(Int8Array),
        DataType::Int16 => typed_value!(Int16Array),
        DataType::Int32 => typed_value!(Int32Array),
        DataType::Int64 => typed_value!(Int64Array),
        DataType::Float32 | DataType::Float64 => Err(format!(
            "{name} does not support {:?} input",
            array.data_type()
        )),
        other => Err(format!("{name} unsupported input type: {other:?}")),
    }
}

fn signed_parts<'a>(
    name: &str,
    input: &'a AggInputView<'a>,
) -> Result<(&'a StructArray, &'a ArrayRef, &'a Int8Array), String> {
    let AggInputView::Any(array) = input else {
        return Err(format!("{name} batch input type mismatch"));
    };
    let struct_arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| format!("{name} expects struct input"))?;
    if struct_arr.num_columns() != 2 {
        return Err(format!(
            "{name} expects 2 arguments, got {}",
            struct_arr.num_columns()
        ));
    }
    let value_arr = struct_arr.column(0);
    let op_arr_ref = struct_arr.column(1);
    let op_arr = op_arr_ref
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| {
            format!(
                "{name} change_op must be Int8, got {:?}",
                op_arr_ref.data_type()
            )
        })?;
    Ok((struct_arr, value_arr, op_arr))
}

fn signed_delta(name: &str, op_arr: &Int8Array, row: usize) -> Result<Option<(i64, i64)>, String> {
    if op_arr.is_null(row) {
        return Ok(None);
    }
    match op_arr.value(row) {
        0 => Ok(Some((1, 1))),
        1 => Ok(Some((-1, -1))),
        other => Err(format!("unknown {name} change_op: {other}")),
    }
}

fn merge_sum_state(
    name: &str,
    spec: &AggSpec,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Binary(array) = input else {
        return Err(format!("{name} merge input type mismatch"));
    };
    for (row, &base) in state_ptrs.iter().enumerate() {
        if array.is_null(row) {
            continue;
        }
        match spec.kind {
            AggKind::SumStateInt64 | AggKind::SumStateSignedInt64 => {
                let (row_count, sum) = decode_sum_int64(array.value(row))?;
                add_int_delta(int_state_slot(base, offset), row_count, sum, name)?;
            }
            AggKind::SumStateDecimal128 | AggKind::SumStateSignedDecimal128 => {
                let (row_count, sum) = decode_sum_decimal128(array.value(row))?;
                add_decimal_delta(decimal_state_slot(base, offset), row_count, sum, name)?;
            }
            _ => return Err(format!("{name} merge kind mismatch")),
        }
    }
    Ok(())
}

fn build_sum_state_array(
    spec: &AggSpec,
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for &base in group_states {
        match spec.kind {
            AggKind::SumStateInt64 | AggKind::SumStateSignedInt64 => {
                let state = unsafe { &*int_state_slot(base, offset) };
                if state.row_count == 0 && state.sum == 0 {
                    builder.append_value([]);
                } else {
                    builder.append_value(encode_sum_int64(state.row_count, state.sum));
                }
            }
            AggKind::SumStateDecimal128 | AggKind::SumStateSignedDecimal128 => {
                let state = unsafe { &*decimal_state_slot(base, offset) };
                if state.row_count == 0 && state.sum == 0 {
                    builder.append_value([]);
                } else {
                    builder.append_value(encode_sum_decimal128(state.row_count, state.sum));
                }
            }
            _ => return Err("sum_state build array kind mismatch".to_string()),
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, StructArray,
    };
    use arrow::datatypes::{DataType, Field};
    use arrow_buffer::NullBufferBuilder;

    use crate::connector::starrocks::managed::state_codec::{
        decode_sum_decimal128, decode_sum_int64, encode_sum_decimal128, encode_sum_int64,
    };
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

    use super::super::super::{AggInputView, AggKind, AggSpec, AggStatePtr, AggregateFunction};
    use super::*;

    fn sum_func(name: &str) -> AggFunction {
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

    fn sum_func_with_arg(name: &str, input_arg_type: DataType) -> AggFunction {
        AggFunction {
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Binary),
                input_arg_type: Some(input_arg_type),
            }),
            ..sum_func(name)
        }
    }

    fn sum_func_with_signature(
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

    fn build_spec(input_type: &DataType) -> AggSpec {
        SumStateAgg
            .build_spec_from_type(&sum_func("sum_state"), Some(input_type), false)
            .unwrap()
    }

    fn build_signed_spec(input_type: &DataType) -> AggSpec {
        SumStateSignedAgg
            .build_spec_from_type(&sum_func("sum_state_signed"), Some(input_type), false)
            .unwrap()
    }

    fn signed_type(value_type: DataType) -> DataType {
        DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("v", value_type, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]))
    }

    struct StateCell {
        spec: AggSpec,
        cell: Box<MaybeUninit<SumState>>,
    }

    impl StateCell {
        fn new(spec: AggSpec) -> Self {
            let mut cell = Box::new(MaybeUninit::<SumState>::uninit());
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

    fn final_int_state(out: &ArrayRef) -> (i64, i64) {
        decode_sum_int64(final_bytes(out)).unwrap()
    }

    fn final_decimal_state(out: &ArrayRef) -> (i64, i128) {
        decode_sum_decimal128(final_bytes(out)).unwrap()
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

    fn signed_decimal_input(values: Vec<Option<i128>>, ops: Vec<Option<i8>>) -> ArrayRef {
        assert_eq!(values.len(), ops.len());
        let value_arr = Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(18, 6)
                .unwrap(),
        ) as ArrayRef;
        let op_arr = Arc::new(Int8Array::from(ops)) as ArrayRef;
        Arc::new(StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new("v", DataType::Decimal128(18, 6), true)),
                Arc::new(Field::new("op", DataType::Int8, true)),
            ]),
            vec![value_arr, op_arr],
            None,
        )) as ArrayRef
    }

    fn signed_int_input_with_struct_nulls(
        values: Vec<Option<i64>>,
        ops: Vec<Option<i8>>,
        struct_valid: Vec<bool>,
    ) -> ArrayRef {
        assert_eq!(values.len(), ops.len());
        assert_eq!(values.len(), struct_valid.len());
        let value_arr = Arc::new(Int64Array::from(values)) as ArrayRef;
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
                Arc::new(Field::new("v", DataType::Int64, true)),
                Arc::new(Field::new("op", DataType::Int8, true)),
            ]),
            vec![value_arr, op_arr],
            struct_nulls.finish(),
        )) as ArrayRef
    }

    #[test]
    fn sum_state_int64_skips_nulls() {
        let mut cell = StateCell::new(build_spec(&DataType::Int64));
        let input = Arc::new(Int64Array::from(vec![Some(10), None, Some(20)])) as ArrayRef;
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 3);

        assert_eq!(final_int_state(&cell.finalize()), (2, 30));
    }

    #[test]
    fn sum_state_signed_int64_handles_delete() {
        let mut cell = StateCell::new(build_signed_spec(&signed_type(DataType::Int64)));
        let input = signed_int_input(vec![Some(10), Some(5)], vec![Some(0), Some(1)]);
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 2);

        assert_eq!(final_int_state(&cell.finalize()), (0, 5));
    }

    #[test]
    fn sum_state_decimal128() {
        let mut cell = StateCell::new(build_spec(&DataType::Decimal128(18, 6)));
        let input = Arc::new(
            Decimal128Array::from(vec![Some(1_000_000_i128), Some(2_000_000_i128)])
                .with_precision_and_scale(18, 6)
                .unwrap(),
        ) as ArrayRef;
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 2);

        assert_eq!(final_decimal_state(&cell.finalize()), (2, 3_000_000));
    }

    #[test]
    fn sum_state_widens_int8_int16_int32() {
        let cases: Vec<(DataType, ArrayRef, (i64, i64))> = vec![
            (
                DataType::Int8,
                Arc::new(Int8Array::from(vec![Some(1_i8), Some(2_i8)])) as ArrayRef,
                (2, 3),
            ),
            (
                DataType::Int16,
                Arc::new(Int16Array::from(vec![Some(3_i16), Some(4_i16)])) as ArrayRef,
                (2, 7),
            ),
            (
                DataType::Int32,
                Arc::new(Int32Array::from(vec![Some(5_i32), Some(6_i32)])) as ArrayRef,
                (2, 11),
            ),
        ];

        for (ty, input, expected) in cases {
            let mut cell = StateCell::new(build_spec(&ty));
            let input_slot = Some(input);
            let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();
            cell.update(view, 2);
            assert_eq!(final_int_state(&cell.finalize()), expected);
        }
    }

    #[test]
    fn sum_state_signed_decimal_insert_delete() {
        let mut cell = StateCell::new(build_signed_spec(&signed_type(DataType::Decimal128(18, 6))));
        let input = signed_decimal_input(
            vec![Some(1_000_000_i128), Some(250_000_i128)],
            vec![Some(0), Some(1)],
        );
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 2);

        assert_eq!(final_decimal_state(&cell.finalize()), (0, 750_000));
    }

    #[test]
    fn sum_state_signed_skips_struct_null_value_null_and_null_op_zero() {
        let mut cell = StateCell::new(build_signed_spec(&signed_type(DataType::Int64)));
        let input = signed_int_input_with_struct_nulls(
            vec![Some(10), Some(20), None, Some(7)],
            vec![Some(0), Some(0), Some(0), None],
            vec![true, false, true, true],
        );
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 4);

        assert_eq!(final_int_state(&cell.finalize()), (1, 10));
    }

    #[test]
    fn sum_state_signed_rejects_unknown_op() {
        let mut cell = StateCell::new(build_signed_spec(&signed_type(DataType::Int64)));
        let input = signed_int_input(vec![Some(10)], vec![Some(2)]);
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        let err = cell.try_update(view, 1).unwrap_err();

        assert!(err.contains("unknown sum_state_signed change_op"));
    }

    #[test]
    fn sum_state_merge_decodes_binary_states() {
        let mut cell = StateCell::new(build_spec(&DataType::Int64));
        let pos = encode_sum_int64(2, 30);
        let neg = encode_sum_int64(-1, -5);
        let input = Arc::new(BinaryArray::from(vec![
            Some(&pos[..]),
            Some(&[][..]),
            Some(&neg[..]),
        ])) as ArrayRef;

        cell.merge(input);

        assert_eq!(final_int_state(&cell.finalize()), (1, 25));
    }

    #[test]
    fn sum_state_zero_finalizes_empty_bytes() {
        let mut cell = StateCell::new(build_spec(&DataType::Int64));
        let input = Arc::new(Int64Array::from(vec![None, None])) as ArrayRef;
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();

        cell.update(view, 2);

        assert!(final_bytes(&cell.finalize()).is_empty());
    }

    #[test]
    fn sum_state_registration_resolves_binary_specs() {
        let spec = super::super::super::build_spec_from_type(
            &sum_func_with_arg("sum_state", DataType::Int64),
            Some(&DataType::Int64),
            false,
        )
        .unwrap();
        assert!(matches!(spec.kind, AggKind::SumStateInt64));
        assert_eq!(spec.output_type, DataType::Binary);
        assert_eq!(spec.intermediate_type, DataType::Binary);

        let signed_ty = signed_type(DataType::Decimal128(18, 6));
        let signed_spec = super::super::super::build_spec_from_type(
            &sum_func_with_arg("sum_state_signed", signed_ty.clone()),
            Some(&signed_ty),
            false,
        )
        .unwrap();
        assert!(matches!(
            signed_spec.kind,
            AggKind::SumStateSignedDecimal128
        ));
        assert_eq!(signed_spec.output_type, DataType::Binary);
        assert_eq!(signed_spec.intermediate_type, DataType::Binary);
    }

    #[test]
    fn sum_state_rejects_non_binary_type_signature() {
        let output_err = crate::exec::expr::agg::spec::build_spec_from_type(
            &sum_func_with_signature(
                "sum_state",
                DataType::Float64,
                DataType::Binary,
                Some(DataType::Int64),
            ),
            Some(&DataType::Int64),
            false,
        )
        .unwrap_err();
        assert!(output_err.contains("state combinator output_type must be Binary"));

        let intermediate_err = crate::exec::expr::agg::spec::build_spec_from_type(
            &sum_func_with_signature(
                "sum_state_signed",
                DataType::Binary,
                DataType::Int64,
                Some(signed_type(DataType::Int64)),
            ),
            Some(&signed_type(DataType::Int64)),
            false,
        )
        .unwrap_err();
        assert!(intermediate_err.contains("state combinator intermediate_type must be Binary"));
    }

    #[test]
    fn sum_state_rejects_utf8_type_signature() {
        let output_err = crate::exec::expr::agg::spec::build_spec_from_type(
            &sum_func_with_signature(
                "sum_state",
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
            &sum_func_with_signature(
                "sum_state_signed",
                DataType::Binary,
                DataType::Utf8,
                Some(signed_type(DataType::Int64)),
            ),
            Some(&signed_type(DataType::Int64)),
            false,
        )
        .unwrap_err();
        assert!(intermediate_err.contains("state combinator intermediate_type must be Binary"));
    }

    #[test]
    fn sum_state_rejects_float_input() {
        let err = SumStateAgg
            .build_spec_from_type(&sum_func("sum_state"), Some(&DataType::Float64), false)
            .unwrap_err();
        assert!(err.contains("sum_state does not support Float64"));

        let input = Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef;
        let input_slot = Some(input);
        let spec = AggSpec {
            kind: AggKind::SumStateInt64,
            output_type: DataType::Binary,
            intermediate_type: DataType::Binary,
            input_arg_type: Some(DataType::Float64),
            count_all: false,
        };
        let err = match SumStateAgg.build_input_view(&spec, &input_slot) {
            Ok(_) => panic!("sum_state float input unexpectedly built a view"),
            Err(err) => err,
        };
        assert!(err.contains("sum_state does not support Float64"));
    }

    #[test]
    fn sum_state_merge_decimal_decodes_binary_states() {
        let mut cell = StateCell::new(build_spec(&DataType::Decimal128(18, 6)));
        let left = encode_sum_decimal128(1, 1_000_000);
        let right = encode_sum_decimal128(-1, -250_000);
        let input =
            Arc::new(BinaryArray::from(vec![Some(&left[..]), Some(&right[..])])) as ArrayRef;

        cell.merge(input);

        assert_eq!(final_decimal_state(&cell.finalize()), (0, 750_000));
    }

    #[test]
    fn sum_state_decimal_merge_spec_uses_preserved_logical_type() {
        let spec = super::super::super::build_spec_from_type(
            &sum_func_with_arg("sum_state", DataType::Decimal128(18, 6)),
            Some(&DataType::Binary),
            true,
        )
        .unwrap();
        assert!(matches!(spec.kind, AggKind::SumStateDecimal128));

        let mut cell = StateCell::new(spec);
        let left = encode_sum_decimal128(1, 1_000_000);
        let right = encode_sum_decimal128(1, 2_000_000);
        let input =
            Arc::new(BinaryArray::from(vec![Some(&left[..]), Some(&right[..])])) as ArrayRef;

        cell.merge(input);

        assert_eq!(final_decimal_state(&cell.finalize()), (2, 3_000_000));
    }

    #[test]
    fn sum_state_merge_spec_requires_preserved_logical_type() {
        let missing_err = super::super::super::build_spec_from_type(
            &sum_func("sum_state"),
            Some(&DataType::Binary),
            true,
        )
        .unwrap_err();
        assert!(missing_err.contains("sum_state merge requires original logical input type"));

        let binary_err = super::super::super::build_spec_from_type(
            &sum_func_with_arg("sum_state", DataType::Binary),
            Some(&DataType::Binary),
            true,
        )
        .unwrap_err();
        assert!(binary_err.contains("sum_state unsupported input type: Binary"));
    }
}
