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

//! ONE original unary computation for both selected owners and v1 shells.
//! Original Arrow cast/output allocation retains formal host scope obligations.
use crate::math_numeric::{
    MathNumericError, MathNumericObservation, NumericArrayView, cast_output_observed,
};
use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Float64Array, UInt64Array, types::Decimal128Type};
use arrow_array::{PrimitiveArray, builder::PrimitiveBuilder, types::ArrowPrimitiveType};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;
/// Frozen by the exact preparation owner, never reselected from a SQL name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NumericUnaryOp {
    Acos,
    Asin,
    Atan,
    Cbrt,
    Ceil,
    Cos,
    Degrees,
    Dlog1,
    Exp,
    Floor,
    Ln,
    Log10,
    Log2,
    Radians,
    Positive,
    Sin,
    Sqrt,
    Sign,
    Tan,
}
impl NumericUnaryOp {
    pub(super) const fn preserves_width(self) -> bool {
        matches!(self, Self::Ceil | Self::Floor | Self::Sign)
    }
    fn apply(self, value: f64) -> f64 {
        match self {
            Self::Acos => value.acos(),
            Self::Asin => value.asin(),
            Self::Atan => value.atan(),
            Self::Cbrt => value.cbrt(),
            Self::Ceil => value.ceil(),
            Self::Cos => value.cos(),
            Self::Degrees => value.to_degrees(),
            // Preserve the original addition/rounding; ln_1p differs at zero
            // and for small inputs and is not the accepted implementation.
            Self::Dlog1 => (1.0 + value).ln(),
            Self::Exp => value.exp(),
            Self::Floor => value.floor(),
            Self::Ln => value.ln(),
            Self::Log10 => value.log10(),
            Self::Log2 => value.log2(),
            Self::Radians => value.to_radians(),
            Self::Positive => value,
            Self::Sin => value.sin(),
            Self::Sign if value == 0.0 || value.is_nan() => value,
            Self::Sign => value.signum(),
            Self::Sqrt => value.sqrt(),
            Self::Tan => value.tan(),
        }
    }
}

enum UnaryArgument<'a> {
    Selected(EvaluatedArgument<'a>, &'a FunctionValueType),
    Legacy {
        array: &'a ArrayRef,
        batch_rows: usize,
    },
}
impl<'a> UnaryArgument<'a> {
    fn array(&self) -> &'a ArrayRef {
        match self {
            Self::Selected(arg, _) => arg.array(),
            Self::Legacy { array, .. } => array,
        }
    }
    fn row(&self, ordinal: usize, batch_row: usize) -> usize {
        match self {
            Self::Selected(arg, _) => arg.value_row(ordinal, batch_row),
            Self::Legacy { array, batch_rows } if array.len() == 1 && *batch_rows > 1 => 0,
            Self::Legacy { .. } => batch_row,
        }
    }
    fn check_row(&self, row: usize) -> Result<(), KernelFailure> {
        if let Self::Selected(_, source) = self {
            if row >= self.array().len() {
                return Err(internal(
                    "numeric unary selected row is outside its checked carrier",
                ));
            }
            if !source.nullable() && self.array().is_null(row) {
                return Err(internal(
                    "numeric unary non-null input contains selected SQL NULL",
                ));
            }
        }
        Ok(())
    }
}
enum UnaryOperation<F> {
    Transform(F),
    Positive,
}
fn compute_unary<F: Fn(f64) -> f64>(
    operation: UnaryOperation<F>,
    argument: UnaryArgument<'_>,
    view: Option<NumericArrayView<'_>>,
    selection: Selection<'_>,
    target: Option<&DataType>,
    target_nullable: Option<bool>,
    control: Option<&dyn KernelEvaluationControl>,
) -> Result<ArrayRef, MathNumericError> {
    if matches!(argument, UnaryArgument::Selected(..)) {
        output_capacity(selection.len())?;
    }
    let mut work = control.map(EvaluationCheckpoints::new);
    let out = match operation {
        UnaryOperation::Transform(func) => {
            let view = view.expect("numeric view is checked before the shared transform");
            let mut values = Vec::with_capacity(selection.len());
            for (ordinal, batch_row) in selection.iter().enumerate() {
                if let Some(work) = &mut work {
                    work.step()?;
                }
                let row = argument.row(ordinal, batch_row);
                argument.check_row(row)?;
                // Original read converts signed integers / decimals before
                // arithmetic, and filters only AFTER the original operation.
                let value = view.value_f64(row).and_then(|value| {
                    let value = func(value);
                    value.is_finite().then_some(value)
                });
                values.push(value);
            }
            if let Some(work) = &mut work {
                work.flush()?;
            }
            let out = Arc::new(Float64Array::from(values)) as ArrayRef;
            if let Some(work) = &mut work {
                work.flush()?;
            }
            out
        }
        UnaryOperation::Positive => {
            match &argument {
                // Original POSITIVE preserves its actual source and length,
                // rather than reading through f64 before Arrow conversion.
                UnaryArgument::Legacy { array, .. } => Arc::clone(array),
                UnaryArgument::Selected(_, _) => {
                    let mut identity = selection.len() == argument.array().len();
                    for (ordinal, batch_row) in selection.iter().enumerate() {
                        if let Some(work) = &mut work {
                            work.step()?;
                        }
                        let row = argument.row(ordinal, batch_row);
                        argument.check_row(row)?;
                        identity &= row == ordinal;
                    }
                    if identity {
                        Arc::clone(argument.array())
                    } else {
                        let mut addresses = Vec::with_capacity(selection.len());
                        for (ordinal, batch_row) in selection.iter().enumerate() {
                            if let Some(work) = &mut work {
                                work.step()?;
                            }
                            addresses.push(argument.row(ordinal, batch_row) as u64);
                        }
                        if let Some(work) = &mut work {
                            work.flush()?;
                        }
                        let addresses = UInt64Array::from(addresses);
                        let out =
                            arrow_select::take::take(argument.array().as_ref(), &addresses, None)
                                .map_err(MathNumericError::Gather)?;
                        if let Some(work) = &mut work {
                            work.flush()?;
                        }
                        out
                    }
                }
            }
        }
    };
    let out = cast_output_observed(out, target, &mut |observation| {
        if let Some(work) = &mut work {
            match observation {
                MathNumericObservation::Step => work.step(),
                MathNumericObservation::OpaqueBoundary => work.flush(),
            }
        } else {
            Ok(())
        }
    })?;
    if target_nullable == Some(false) && out.null_count() != 0 {
        return Err(internal("numeric unary successful NULL contradicts its result type").into());
    }
    if let Some(work) = work {
        work.finish()?;
    }
    Ok(out)
}
pub(super) fn evaluate_numeric_unary<'a>(
    op: NumericUnaryOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let [FunctionArgumentType::Value(source)] = input.contract().selected().argument_types.as_ref()
    else {
        return Err(invalid("numeric unary requires one exact value argument"));
    };
    let [argument] = input.arguments() else {
        return Err(invalid("numeric unary requires one evaluated argument"));
    };
    let target = input.contract().result_type();
    if source.logical_type() != ValueLogicalType::Physical
        || target.logical_type() != ValueLogicalType::Physical
        || target.nullable() != source.nullable()
        || argument.array().data_type() != source.data_type()
    {
        return Err(invalid(
            "numeric unary input or result differs from its exact profile",
        ));
    }
    let selection = input.selection();
    output_capacity(selection.len())?;
    let argument = UnaryArgument::Selected(*argument, source);
    let mut work = EvaluationCheckpoints::new(control);
    let values = if op.preserves_width() {
        evaluate_width_preserving(op, &argument, selection, target.data_type(), &mut work)?
    } else {
        if source.data_type() != &DataType::Float64 || target.data_type() != &DataType::Float64 {
            return Err(invalid("numeric unary requires its exact DOUBLE profile"));
        }
        Arc::new(unary_array::<arrow_array::types::Float64Type>(
            &argument,
            selection,
            &mut work,
            |value| op.apply(value),
        )?) as ArrayRef
    };
    work.finish()?;
    SelectedValues::try_new(
        input.selection(),
        target.data_type(),
        values,
        Box::default(),
    )
    .map_err(|_| internal("numeric unary compact output violates its selected contract"))
}
fn unary_array<T: ArrowPrimitiveType>(
    argument: &UnaryArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
    transform: impl Fn(T::Native) -> T::Native,
) -> Result<PrimitiveArray<T>, KernelFailure> {
    let source = argument
        .array()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| internal("numeric unary selected carrier cannot be downcast"))?;
    work.flush()?;
    crate::selected_copy::fixed_interleave_extent(&T::DATA_TYPE, selection.len())
        .map_err(crate::selected_copy::CopyError::into_kernel)?;
    let mut output = PrimitiveBuilder::<T>::with_capacity(selection.len());
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.row(ordinal, batch_row);
        argument.check_row(row)?;
        output.append_option((!source.is_null(row)).then(|| transform(source.value(row))));
    }
    work.flush()?;
    let result = output.finish();
    work.flush()?;
    Ok(result)
}

fn evaluate_width_preserving(
    op: NumericUnaryOp,
    argument: &UnaryArgument<'_>,
    selection: Selection<'_>,
    target: &DataType,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    use arrow_array::types::{Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type};
    let source = argument.array().data_type();
    if let DataType::Decimal128(precision, scale) = *source {
        if !(1..=38).contains(&precision) || !(0..=precision as i8).contains(&scale) {
            return Err(invalid(
                "numeric unary decimal input differs from the installed precision or scale",
            ));
        }
        let DataType::Decimal128(p, 0) = *target else {
            return Err(invalid("numeric unary decimal result requires scale zero"));
        };
        let expected = if op == NumericUnaryOp::Sign {
            1
        } else {
            super::signature::integral_decimal_precision(precision, scale)
                .expect("validated decimal declaration")
        };
        if p != expected {
            return Err(invalid(
                "numeric unary decimal result differs from the installed precision",
            ));
        }
        let factor = 10_i128.pow(scale as u32);
        let result = unary_array::<Decimal128Type>(argument, selection, work, |value| {
            let whole = value / factor;
            let fraction = value % factor;
            match op {
                NumericUnaryOp::Ceil => whole + i128::from(fraction > 0),
                NumericUnaryOp::Floor => whole - i128::from(fraction < 0),
                NumericUnaryOp::Sign => value.signum(),
                _ => unreachable!("typed decimal path is used for ceil, floor, and sign"),
            }
        })?
        .with_precision_and_scale(p, 0)
        .map_err(|_| internal("numeric unary cannot construct its declared DECIMAL result"))?;
        return Ok(Arc::new(result));
    }
    if target != source {
        return Err(invalid(
            "numeric unary result must preserve the source width",
        ));
    }
    macro_rules! integer {
        ($ty:ty) => {
            Arc::new(unary_array::<$ty>(argument, selection, work, |value| {
                if op == NumericUnaryOp::Sign {
                    value.signum()
                } else {
                    value
                }
            })?) as ArrayRef
        };
    }
    Ok(match source {
        DataType::Int8 => integer!(Int8Type),
        DataType::Int16 => integer!(Int16Type),
        DataType::Int32 => integer!(Int32Type),
        DataType::Int64 => integer!(Int64Type),
        DataType::Float32 => Arc::new(unary_array::<Float32Type>(
            argument,
            selection,
            work,
            |v| op.apply(f64::from(v)) as f32,
        )?),
        DataType::Float64 => Arc::new(unary_array::<Float64Type>(
            argument,
            selection,
            work,
            |v| op.apply(v),
        )?),
        _ => {
            return Err(invalid(
                "numeric unary carrier is outside the installed profile",
            ));
        }
    })
}

/// v1 already evaluated its child and supplies actual carrier / batch facts.
pub fn evaluate_legacy_numeric_unary(
    op: NumericUnaryOp,
    array: &ArrayRef,
    batch_rows: usize,
    target: Option<&DataType>,
) -> Result<ArrayRef, MathNumericError> {
    let operation = if op == NumericUnaryOp::Positive {
        UnaryOperation::Positive
    } else {
        UnaryOperation::Transform(move |value| op.apply(value))
    };
    let view = if op == NumericUnaryOp::Positive {
        None
    } else {
        Some(NumericArrayView::new(array).map_err(MathNumericError::Carrier)?)
    };
    compute_unary(
        operation,
        UnaryArgument::Legacy { array, batch_rows },
        view,
        Selection::all(batch_rows),
        target,
        None,
        None,
    )
}
/// The original generic unary entry is also used by ROUND/TRUNCATE shells.
/// Their arithmetic closures and operation selection remain with those owners.
pub fn evaluate_legacy_unary_f64<F: Fn(f64) -> f64>(
    array: &ArrayRef,
    batch_rows: usize,
    target: Option<&DataType>,
    func: F,
) -> Result<ArrayRef, MathNumericError> {
    let view = NumericArrayView::new(array).map_err(MathNumericError::Carrier)?;
    compute_unary(
        UnaryOperation::Transform(func),
        UnaryArgument::Legacy { array, batch_rows },
        Some(view),
        Selection::all(batch_rows),
        target,
        None,
        None,
    )
}
/// Check Rust allocation representability, never an application capacity grant.
fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = rows
        .checked_mul(8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    isize::try_from(values).map_err(|_| KernelFailure::ResourceExhausted)?;
    isize::try_from(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionValueType,
        ScalarEvaluationInstance, Selection,
    };
    use novarocks_type_contract::CompilePhase;
    use std::{sync::Mutex, time::Duration};

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl Control {
        fn refusing(index: usize, error: KernelFailure) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                refusal: Some((index, error)),
            }
        }
        fn calls(&self) -> Vec<u32> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let mut calls = self.calls.lock().unwrap();
            let index = calls.len();
            calls.push(units);
            if let Some((at, error)) = &self.refusal
                && *at == index
            {
                return Err(error.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("numeric unary kernels must never wait");
        }
    }
    fn instance(name: &str, source: FunctionValueType) -> ScalarEvaluationInstance {
        let prepared = super::super::numeric_unary_owner::prepared_for_test(name, source).unwrap();
        ScalarEvaluationInstance::instantiate(prepared).unwrap()
    }
    fn assert_float(value: f64, expected: f64) {
        if expected == 0.0 {
            assert_eq!(
                value.to_bits(),
                expected.to_bits(),
                "signed zero must retain the original operation's bits"
            );
        } else {
            assert!(
                (value - expected).abs() <= expected.abs().max(1.0) * 1e-14,
                "actual={value:?} expected={expected:?}"
            );
        }
    }
    fn assert_floats(array: &ArrayRef, expected: &[Option<f64>]) {
        let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                Some(expected) => {
                    assert!(!array.is_null(row));
                    assert_float(array.value(row), *expected);
                }
                None => assert!(array.is_null(row)),
            }
        }
    }

    #[test]
    fn sparse_column_compact_column_and_scalar_keep_explicit_row_mapping() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(6, &rows).unwrap();
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::NAN,
            4.0,
            -1.0,
            f64::INFINITY,
            9.0,
            -10.0,
        ]));
        let arguments = [EvaluatedArgument::Column(&array)];
        let mut column = instance("sqrt", FunctionValueType::new(DataType::Float64, false));
        let result = column
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_floats(result.values(), &[Some(2.0), Some(3.0)]);
        let compact_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(16.0), None]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Float64, compact_array, Box::default())
                .unwrap();
        let arguments = [EvaluatedArgument::SelectedColumn(&compact)];
        let mut compact_instance =
            instance("sqrt", FunctionValueType::new(DataType::Float64, true));
        assert_floats(
            compact_instance
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(4.0), None],
        );
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![25.0]));
        let arguments = [EvaluatedArgument::Scalar(&scalar)];
        let mut broadcast = instance("sqrt", FunctionValueType::new(DataType::Float64, false));
        assert_floats(
            broadcast
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(5.0), Some(5.0)],
        );
    }

    #[test]
    fn constant_nonzero_ordinal_broadcasts_without_copying_its_pool() {
        let ty = FunctionValueType::new(DataType::Float64, true);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![None, Some(-1.0), Some(9.0)]));
        // Explicit finite fixture policy only, not a production profile/grant.
        let policy = ConstantPolicy {
            max_rows: 3,
            max_array_nodes: 1,
            max_logical_elements: 3,
            max_retained_buffer_bytes: 1024,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            max_library_validation_bytes: 8192,
        };
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("unary-input").unwrap()),
            ty.clone(),
            array.to_data(),
            policy,
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let constant = pool.value(2).unwrap();
        let arguments = [EvaluatedArgument::Constant(&constant)];
        let mut instance = instance("sqrt", ty);
        let result = instance
            .evaluate(Selection::all(3), &arguments, &Control::default())
            .unwrap();
        assert_floats(result.values(), &[Some(3.0); 3]);
        assert!(Arc::ptr_eq(constant.pool().array(), pool.array()));
    }

    #[test]
    fn empty_selection_keeps_result_profile_and_skips_private_body() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        for (name, expected) in [("sqrt", DataType::Float64), ("floor", DataType::Float64)] {
            let mut instance = instance(name, FunctionValueType::new(DataType::Float64, true));
            let control = Control::default();
            let result = instance
                .evaluate(Selection::all(0), &arguments, &control)
                .unwrap();
            assert!(result.values().is_empty());
            assert_eq!(result.values().data_type(), &expected);
            assert!(result.errors().is_empty());
            assert_eq!(
                control.calls().iter().filter(|units| **units == 0).count(),
                2
            );
        }
    }

    #[test]
    fn entry_256_tail_and_output_publication_preserve_typed_refusal_without_replay() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![4.0]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        let source = FunctionValueType::new(DataType::Float64, true);
        let selection = Selection::all(320);
        let mut baseline = instance("sqrt", source.clone());
        let control = Control::default();
        baseline.evaluate(selection, &arguments, &control).unwrap();
        let calls = control.calls();
        let body_entry = calls
            .iter()
            .enumerate()
            .filter(|(_, units)| **units == 0)
            .nth(2)
            .unwrap()
            .0;
        let interior = calls.iter().position(|units| *units == 256).unwrap();
        assert!(body_entry < interior);
        assert_eq!(calls[interior + 1], 64);
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for at in [0, body_entry, interior, interior + 1, calls.len() - 1] {
                let mut instance = instance("sqrt", source.clone());
                let control = Control::refusing(at, error.clone());
                assert_eq!(
                    instance
                        .evaluate(selection, &arguments, &control)
                        .unwrap_err(),
                    error
                );
                assert_eq!(control.calls().len(), at + 1);
                assert_eq!(
                    instance
                        .evaluate(selection, &arguments, &Control::default())
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
            }
        }
    }

    #[test]
    fn capacity_overflow_refuses_before_builder_allocation_or_row_work() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        for name in ["sqrt", "ceil"] {
            let mut instance = instance(name, FunctionValueType::new(DataType::Float64, true));
            let control = Control::default();
            assert_eq!(
                instance
                    .evaluate(Selection::all(usize::MAX), &arguments, &control)
                    .unwrap_err(),
                KernelFailure::ResourceExhausted
            );
            assert!(control.calls().iter().all(|units| *units < 256));
            assert_eq!(
                control.calls().iter().filter(|units| **units == 0).count(),
                3
            );
        }
    }
}
