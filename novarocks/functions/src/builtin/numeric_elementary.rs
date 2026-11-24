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

//! Selected log/sign/constants computation for exact installed builtin owners.
//! Arrow builder allocation still requires formal host memory admission.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
    builder::{Float64Builder, Int64Builder},
    types::{Decimal128Type, validate_decimal_precision_and_scale},
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;

use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

/// Frozen by the exact owner, including the two distinct LOG arities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NumericElementaryOp {
    LogNatural,
    LogBase,
    Sign,
    E,
    Pi,
}
impl NumericElementaryOp {
    pub const fn arity(self) -> usize {
        match self {
            Self::LogNatural | Self::Sign => 1,
            Self::LogBase => 2,
            Self::E | Self::Pi => 0,
        }
    }
}

enum NumericInput<'a> {
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Decimal128(&'a Decimal128Array, i8),
    Null,
}
impl<'a> NumericInput<'a> {
    fn checked(array: &'a ArrayRef, data_type: &DataType) -> Result<Self, KernelFailure> {
        if array.data_type() != data_type {
            return Err(internal(
                "elementary numeric carrier differs from its checked argument",
            ));
        }
        match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => {}
            DataType::Decimal128(precision, scale) => {
                validate_decimal_precision_and_scale::<Decimal128Type>(*precision, *scale)
                    .map_err(|_| {
                        invalid("elementary numeric selected decimal parameters are invalid")
                    })?;
            }
            _ => {
                return Err(invalid(
                    "elementary numeric input is not an installed numeric profile",
                ));
            }
        }
        Self::from_actual_carrier(array).map_err(|_| {
            if matches!(data_type, DataType::Decimal128(..)) {
                internal("elementary numeric selected decimal cannot be downcast")
            } else {
                internal("elementary numeric selected carrier cannot be downcast")
            }
        })
    }

    /// The legacy carrier boundary has no selected metadata. Preserve its
    /// complete errors while sharing the reader and conversion with the owner.
    fn from_actual_carrier(array: &'a ArrayRef) -> Result<Self, NumericElementaryError> {
        macro_rules! downcast {
            ($array:ty, $variant:ident, $message:literal) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(Self::$variant)
                    .ok_or_else(|| {
                        NumericElementaryError::Carrier(
                            crate::math_numeric::NumericArrayFailure::new($message),
                        )
                    })
            };
        }
        match array.data_type() {
            DataType::Int8 => downcast!(Int8Array, Int8, "failed to downcast to Int8Array"),
            DataType::Int16 => downcast!(Int16Array, Int16, "failed to downcast to Int16Array"),
            DataType::Int32 => downcast!(Int32Array, Int32, "failed to downcast to Int32Array"),
            DataType::Int64 => downcast!(Int64Array, Int64, "failed to downcast to Int64Array"),
            DataType::Float32 => {
                downcast!(Float32Array, Float32, "failed to downcast to Float32Array")
            }
            DataType::Float64 => {
                downcast!(Float64Array, Float64, "failed to downcast to Float64Array")
            }
            DataType::Decimal128(_, scale) => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(|array| Self::Decimal128(array, *scale))
                .ok_or_else(|| {
                    NumericElementaryError::Carrier(crate::math_numeric::NumericArrayFailure::new(
                        "failed to downcast to Decimal128Array",
                    ))
                }),
            DataType::Null => Ok(Self::Null),
            other => Err(NumericElementaryError::Carrier(
                crate::math_numeric::NumericArrayFailure::new(format_args!(
                    "unsupported numeric type: {other:?}"
                )),
            )),
        }
    }
    fn value(&self, row: usize) -> Option<f64> {
        Some(match self {
            Self::Int8(array) => array.value(row) as f64,
            Self::Int16(array) => array.value(row) as f64,
            Self::Int32(array) => array.value(row) as f64,
            Self::Int64(array) => array.value(row) as f64,
            Self::Float32(array) => array.value(row) as f64,
            Self::Float64(array) => array.value(row),
            Self::Decimal128(array, scale) => {
                // Keep the original i128->f64 conversion before scaling,
                // including its precision loss and negative-scale behavior.
                (array.value(row) as f64) / 10_f64.powi(*scale as i32)
            }
            Self::Null => return None,
        })
    }
}

/// Mapping facts come from the selected ABI or the actual v1 carrier. Legacy
/// one-row broadcasting is explicit and never invents FunctionValueType facts.
enum ElementaryArgument<'a> {
    Selected(EvaluatedArgument<'a>),
    Legacy {
        array: &'a ArrayRef,
        batch_rows: usize,
    },
}
impl<'a> ElementaryArgument<'a> {
    fn array(&self) -> &'a ArrayRef {
        match self {
            Self::Selected(argument) => argument.array(),
            Self::Legacy { array, .. } => array,
        }
    }
    fn value_row(&self, ordinal: usize, batch_row: usize) -> usize {
        match self {
            Self::Selected(argument) => argument.value_row(ordinal, batch_row),
            Self::Legacy { array, batch_rows } if array.len() == 1 && *batch_rows > 1 => 0,
            Self::Legacy { .. } => batch_row,
        }
    }
}

struct NumericValue<'a> {
    argument: ElementaryArgument<'a>,
    view: NumericInput<'a>,
    nullable: Option<bool>,
}
impl<'a> NumericValue<'a> {
    fn checked(
        argument: EvaluatedArgument<'a>,
        value_type: &FunctionValueType,
        decimal: bool,
    ) -> Result<Self, KernelFailure> {
        if value_type.logical_type() != ValueLogicalType::Physical
            || (!decimal && matches!(value_type.data_type(), DataType::Decimal128(_, _)))
        {
            return Err(invalid(
                "elementary numeric input is not an exact installed profile",
            ));
        }
        let view = NumericInput::checked(argument.array(), value_type.data_type())?;
        Ok(Self {
            argument: ElementaryArgument::Selected(argument),
            view,
            nullable: Some(value_type.nullable()),
        })
    }
    fn legacy(array: &'a ArrayRef, batch_rows: usize) -> Result<Self, NumericElementaryError> {
        Ok(Self {
            argument: ElementaryArgument::Legacy { array, batch_rows },
            view: NumericInput::from_actual_carrier(array)?,
            nullable: None,
        })
    }
    fn value(&self, ordinal: usize, batch_row: usize) -> Result<Option<f64>, KernelFailure> {
        let row = self.argument.value_row(ordinal, batch_row);
        let array = self.argument.array();
        if self.nullable.is_some() && row >= array.len() {
            return Err(internal(
                "elementary numeric selected row is outside its checked carrier",
            ));
        }
        if array.is_null(row) {
            if self.nullable == Some(false) {
                return Err(internal(
                    "elementary numeric non-null input contains selected SQL NULL",
                ));
            }
            Ok(None)
        } else {
            Ok(self.view.value(row))
        }
    }
}

enum ElementaryInput<'a> {
    Constant(f64),
    Unary(NumericValue<'a>),
    Binary(NumericValue<'a>, NumericValue<'a>),
}
impl ElementaryInput<'_> {
    fn compute(
        &self,
        op: NumericElementaryOp,
        ordinal: usize,
        batch_row: usize,
    ) -> Result<Option<f64>, KernelFailure> {
        let result = match (self, op) {
            (Self::Constant(value), NumericElementaryOp::E | NumericElementaryOp::Pi) => {
                Some(*value)
            }
            (Self::Unary(value), NumericElementaryOp::LogNatural) => {
                value.value(ordinal, batch_row)?.map(f64::ln)
            }
            (Self::Unary(value), NumericElementaryOp::Sign) => {
                value.value(ordinal, batch_row)?.map(|value| {
                    if value > 0.0 {
                        1.0
                    } else if value < 0.0 {
                        -1.0
                    } else {
                        0.0
                    }
                })
            }
            (Self::Binary(base, value), NumericElementaryOp::LogBase) => {
                match (
                    base.value(ordinal, batch_row)?,
                    value.value(ordinal, batch_row)?,
                ) {
                    (Some(base), Some(value)) => Some(value.ln() / base.ln()),
                    _ => None,
                }
            }
            _ => {
                return Err(internal(
                    "elementary numeric operation differs from its prepared inputs",
                ));
            }
        };
        Ok(result)
    }
}

pub(super) fn evaluate_numeric_elementary<'a>(
    op: NumericElementaryOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let target = input.contract().result_type();
    if target.logical_type() != ValueLogicalType::Physical
        || target.data_type() != &DataType::Float64
    {
        return Err(invalid(
            "elementary numeric requires its exact Physical Float64 result",
        ));
    }
    if input.arguments().len() != op.arity()
        || input.contract().selected().argument_types.len() != op.arity()
    {
        return Err(invalid(
            "elementary numeric arguments differ from their exact operation arity",
        ));
    }
    let inputs = match (
        op,
        input.contract().selected().argument_types.as_ref(),
        input.arguments(),
    ) {
        (NumericElementaryOp::E, [], []) => ElementaryInput::Constant(std::f64::consts::E),
        (NumericElementaryOp::Pi, [], []) => ElementaryInput::Constant(std::f64::consts::PI),
        (
            NumericElementaryOp::LogNatural | NumericElementaryOp::Sign,
            [FunctionArgumentType::Value(source)],
            [argument],
        ) => {
            if (op == NumericElementaryOp::LogNatural && !target.nullable())
                || (op == NumericElementaryOp::Sign && target.nullable() != source.nullable())
            {
                return Err(invalid(
                    "elementary numeric result nullability differs from its exact profile",
                ));
            }
            ElementaryInput::Unary(NumericValue::checked(*argument, source, true)?)
        }
        (
            NumericElementaryOp::LogBase,
            [
                FunctionArgumentType::Value(base),
                FunctionArgumentType::Value(value),
            ],
            [left, right],
        ) if target.nullable() == (base.nullable() || value.nullable()) => ElementaryInput::Binary(
            NumericValue::checked(*left, base, false)?,
            NumericValue::checked(*right, value, false)?,
        ),
        _ => {
            return Err(invalid(
                "elementary numeric arguments differ from their exact arity or result profile",
            ));
        }
    };
    let selection = input.selection();
    let values = compute_numeric_elementary(
        op,
        inputs,
        selection,
        Some(target.nullable()),
        ElementaryProjection::Float64,
        Some(control),
    )?;
    SelectedValues::try_new(selection, target.data_type(), values, Box::default())
        .map_err(|_| internal("elementary numeric compact output violates its selected contract"))
}

/// Carrier diagnostics retain the original complete legacy text. Selected
/// contract and control failures retain their typed kernel failure.
#[derive(Debug)]
pub enum NumericElementaryError {
    Carrier(crate::math_numeric::NumericArrayFailure),
    Kernel(KernelFailure),
}
impl NumericElementaryError {
    pub fn into_kernel(self) -> KernelFailure {
        match self {
            Self::Kernel(cause) => cause,
            cause => KernelFailure::from_domain(cause),
        }
    }
}
impl std::fmt::Display for NumericElementaryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Carrier(cause) => cause.fmt(f),
            Self::Kernel(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for NumericElementaryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Carrier(cause) => Some(cause),
            Self::Kernel(cause) => Some(cause),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for NumericElementaryError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Carrier(cause) => cause.failure_fact(),
            Self::Kernel(cause) => cause.failure_fact(),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for NumericElementaryError {}

#[derive(Clone, Copy)]
enum ElementaryProjection {
    Float64,
    LegacySignInt64,
}
enum ElementaryBuilder {
    Float64(Float64Builder),
    SignInt64(Int64Builder),
}
impl ElementaryBuilder {
    fn new(projection: ElementaryProjection, rows: usize) -> Self {
        match projection {
            ElementaryProjection::Float64 => Self::Float64(Float64Builder::with_capacity(rows)),
            ElementaryProjection::LegacySignInt64 => {
                Self::SignInt64(Int64Builder::with_capacity(rows))
            }
        }
    }
    fn append(&mut self, value: Option<f64>) {
        match self {
            Self::Float64(builder) => builder.append_option(value),
            // SIGN computation returns exactly -1, +0 or +1. This projection
            // preserves the original Int64 source for subsequent legacy casts.
            Self::SignInt64(builder) => builder.append_option(value.map(|value| value as i64)),
        }
    }
    fn finish(self) -> ArrayRef {
        match self {
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::SignInt64(mut builder) => Arc::new(builder.finish()),
        }
    }
}

/// The one row loop, arithmetic and finite-result policy used by both shells.
/// None control/nullability means v1 supplied no selected contract or control;
/// it is not fabricated metadata and does not authorize allocation capacity.
fn compute_numeric_elementary(
    op: NumericElementaryOp,
    inputs: ElementaryInput<'_>,
    selection: Selection<'_>,
    nullable: Option<bool>,
    projection: ElementaryProjection,
    control: Option<&dyn KernelEvaluationControl>,
) -> Result<ArrayRef, KernelFailure> {
    output_capacity(selection.len())?;
    let mut builder = ElementaryBuilder::new(projection, selection.len());
    let mut work = control.map(EvaluationCheckpoints::new);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        if let Some(work) = &mut work {
            work.step()?;
        }
        let value = inputs.compute(op, ordinal, batch_row)?;
        if value.is_none() && nullable == Some(false) {
            return Err(internal(
                "elementary numeric successful NULL contradicts its result type",
            ));
        }
        builder.append(value);
    }
    let values = builder.finish();
    if let Some(work) = work {
        work.finish()?;
    }
    Ok(values)
}

/// Thin v1 entry after ExprArena evaluation: actual carrier and batch length
/// supply the original reader facts; all rows use the same pure computation.
/// Arrow output casting stays in the v1 shell with its original source type.
pub fn evaluate_legacy_numeric_elementary(
    op: NumericElementaryOp,
    arguments: &[ArrayRef],
    batch_rows: usize,
) -> Result<ArrayRef, NumericElementaryError> {
    let inputs = match (op, arguments) {
        (NumericElementaryOp::E, []) => ElementaryInput::Constant(std::f64::consts::E),
        (NumericElementaryOp::Pi, []) => ElementaryInput::Constant(std::f64::consts::PI),
        (NumericElementaryOp::LogNatural | NumericElementaryOp::Sign, [argument]) => {
            ElementaryInput::Unary(NumericValue::legacy(argument, batch_rows)?)
        }
        (NumericElementaryOp::LogBase, [base, value]) => ElementaryInput::Binary(
            NumericValue::legacy(base, batch_rows)?,
            NumericValue::legacy(value, batch_rows)?,
        ),
        _ => {
            return Err(NumericElementaryError::Kernel(internal(
                "elementary numeric operation differs from its prepared inputs",
            )));
        }
    };
    let projection = if op == NumericElementaryOp::Sign {
        ElementaryProjection::LegacySignInt64
    } else {
        ElementaryProjection::Float64
    };
    compute_numeric_elementary(
        op,
        inputs,
        Selection::all(batch_rows),
        None,
        projection,
        None,
    )
    .map_err(NumericElementaryError::Kernel)
}

/// Check Rust allocation representability; this never authorizes memory.
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
    use crate::{EvaluatedArgument, FunctionValueType, ScalarEvaluationInstance, Selection};
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
            panic!("binary numeric kernels must not wait");
        }
    }
    fn instance(name: &str, types: &[FunctionValueType]) -> ScalarEvaluationInstance {
        let prepared =
            super::super::numeric_elementary_owner::prepared_for_test(name, types).unwrap();
        ScalarEvaluationInstance::instantiate(prepared).unwrap()
    }

    #[test]
    fn e_pi_are_zero_argument_exact_bits_for_dense_and_sparse_outputs() {
        for (name, expected) in [("e", 0x4005bf0a8b145769u64), ("pi", 0x400921fb54442d18)] {
            let mut instance = instance(name, &[]);
            assert!(!instance.contract().result_type().nullable());
            let output = instance
                .evaluate(Selection::all(3), &[], &Control::default())
                .unwrap();
            assert!(output.errors().is_empty());
            let values = output
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(values.null_count(), 0);
            assert_eq!(
                values
                    .values()
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                [expected; 3]
            );
            let rows = [1, 7];
            let selection = Selection::try_sparse(10, &rows).unwrap();
            let output = instance
                .evaluate(selection, &[], &Control::default())
                .unwrap();
            assert_eq!(output.selection(), selection);
            assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                [expected; 2]
            );
        }
    }

    #[test]
    fn every_arity_empty_domain_skips_private_body_and_keeps_float_result() {
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        let source = FunctionValueType::new(DataType::Float64, true);
        for (name, arity) in [("log", 2), ("e", 0), ("pi", 0)] {
            let types = vec![source.clone(); arity];
            let arguments = vec![EvaluatedArgument::Scalar(&scalar); arity];
            let mut instance = instance(name, &types);
            let control = Control::default();
            let output = instance
                .evaluate(Selection::all(0), &arguments, &control)
                .unwrap();
            assert!(output.values().is_empty());
            assert_eq!(output.values().data_type(), &DataType::Float64);
            assert!(output.errors().is_empty());
            assert_eq!(
                control.calls().iter().filter(|units| **units == 0).count(),
                arity + 1
            );
        }
    }

    #[test]
    fn each_recipe_keeps_entry_256_tail_and_publication_refusals_typed_and_latched() {
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let source = FunctionValueType::new(DataType::Float64, true);
        let selection = Selection::all(320);
        for (name, arity) in [("log", 2), ("e", 0), ("pi", 0)] {
            let types = vec![source.clone(); arity];
            let arguments = vec![EvaluatedArgument::Scalar(&scalar); arity];
            let mut baseline = instance(name, &types);
            let control = Control::default();
            baseline.evaluate(selection, &arguments, &control).unwrap();
            let calls = control.calls();
            let body_entry = calls
                .iter()
                .enumerate()
                .filter(|(_, units)| **units == 0)
                .nth(arity + 1)
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
                    let mut instance = instance(name, &types);
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
    }

    #[test]
    fn capacity_overflow_refuses_before_allocation_or_actual_elementary_row_work() {
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let source = FunctionValueType::new(DataType::Float64, true);
        for (name, arity) in [("log", 2), ("e", 0), ("pi", 0)] {
            let types = vec![source.clone(); arity];
            let arguments = vec![EvaluatedArgument::Scalar(&scalar); arity];
            let mut instance = instance(name, &types);
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
                arity + 2
            );
        }
    }

    #[test]
    fn shared_legacy_core_keeps_raw_one_row_broadcast_and_sign_int64_projection() {
        for (value, expected) in [
            (Some(-0.0), Some(0)),
            (Some(f64::NAN), Some(0)),
            (Some(f64::INFINITY), Some(1)),
            (Some(f64::NEG_INFINITY), Some(-1)),
            (None, None),
        ] {
            let argument: ArrayRef = Arc::new(Float64Array::from(vec![value]));
            let output =
                evaluate_legacy_numeric_elementary(NumericElementaryOp::Sign, &[argument], 513)
                    .unwrap();
            assert_eq!(output.data_type(), &DataType::Int64);
            let output = output.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(output.len(), 513);
            assert!(output.iter().all(|actual| actual == expected));
        }
        let arguments: [ArrayRef; 2] = [
            Arc::new(Float64Array::from(vec![f64::INFINITY])),
            Arc::new(Float64Array::from(vec![0.5])),
        ];
        let output =
            evaluate_legacy_numeric_elementary(NumericElementaryOp::LogBase, &arguments, 513)
                .unwrap();
        let output = output.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(output.len(), 513);
        assert!(
            output
                .iter()
                .all(|actual| actual.unwrap().to_bits() == 0x8000000000000000)
        );
        let argument: ArrayRef = Arc::new(arrow_array::NullArray::new(1));
        let output =
            evaluate_legacy_numeric_elementary(NumericElementaryOp::LogNatural, &[argument], 513)
                .unwrap();
        assert_eq!(output.data_type(), &DataType::Float64);
        assert_eq!(output.len(), 513);
        assert_eq!(output.null_count(), 513);
    }
}
