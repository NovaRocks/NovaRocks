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
use arrow_array::{
    Array, ArrayRef, Float64Array, UInt64Array,
    types::{Decimal128Type, validate_decimal_precision_and_scale},
};
#[cfg(test)]
use arrow_array::{Decimal128Array, Float32Array, Int8Array, Int16Array, Int32Array, Int64Array};
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
    Tan,
}
impl NumericUnaryOp {
    pub(super) const fn returns_integer(self) -> bool {
        matches!(self, Self::Ceil | Self::Floor)
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
fn checked_view<'a>(
    array: &'a ArrayRef,
    source: &FunctionValueType,
) -> Result<NumericArrayView<'a>, KernelFailure> {
    if array.data_type() != source.data_type() {
        return Err(internal(
            "numeric unary carrier differs from its checked argument",
        ));
    }
    match source.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64 => {}
        DataType::Decimal128(precision, scale) => {
            validate_decimal_precision_and_scale::<Decimal128Type>(*precision, *scale)
                .map_err(|_| invalid("numeric unary selected decimal parameters are invalid"))?
        }
        _ => {
            return Err(invalid(
                "numeric unary input is not an installed numeric profile",
            ));
        }
    }
    NumericArrayView::new(array).map_err(KernelFailure::from_domain)
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
    let expected = if op.returns_integer() {
        DataType::Int64
    } else {
        DataType::Float64
    };
    if source.logical_type() != ValueLogicalType::Physical
        || target.logical_type() != ValueLogicalType::Physical
        || *target.data_type() != expected
    {
        return Err(invalid(
            "numeric unary logical or result type differs from its exact profile",
        ));
    }
    let view = checked_view(argument.array(), source)?;
    let operation = if op == NumericUnaryOp::Positive {
        UnaryOperation::Positive
    } else {
        UnaryOperation::Transform(move |value| op.apply(value))
    };
    let values = compute_unary(
        operation,
        UnaryArgument::Selected(*argument, source),
        Some(view),
        input.selection(),
        Some(target.data_type()),
        Some(target.nullable()),
        Some(control),
    )
    .map_err(MathNumericError::into_kernel)?;
    SelectedValues::try_new(
        input.selection(),
        target.data_type(),
        values,
        Box::default(),
    )
    .map_err(|_| internal("numeric unary compact output violates its selected contract"))
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
    fn dense(name: &str, source: FunctionValueType, array: ArrayRef) -> ArrayRef {
        let mut instance = instance(name, source);
        let arguments = [EvaluatedArgument::Column(&array)];
        let output = instance
            .evaluate(Selection::all(array.len()), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        output.into_parts().1
    }
    fn floats(name: &str, values: Vec<Option<f64>>) -> ArrayRef {
        dense(
            name,
            FunctionValueType::new(DataType::Float64, true),
            Arc::new(Float64Array::from(values)),
        )
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
    fn eighteen_real_operations_have_independent_finite_oracles() {
        use std::f64::consts::{FRAC_PI_2, FRAC_PI_3, FRAC_PI_4, FRAC_PI_6, PI};
        // Expected answers are identities/constants, not calls to the same
        // operation used by the production enum. libm last-bit identity is
        // not promised; signed zero has a separate exact-bit assertion.
        let cases = [
            ("acos", 0.5, FRAC_PI_3),
            ("asin", 0.5, FRAC_PI_6),
            ("atan", 1.0, FRAC_PI_4),
            ("cbrt", -8.0, -2.0),
            ("ceil", -1.2, -1.0),
            ("cos", PI, -1.0),
            ("degress", PI, 180.0),
            ("dlog1", 0.0, 0.0),
            ("exp", 0.0, 1.0),
            ("floor", -1.2, -2.0),
            ("ln", 1.0, 0.0),
            ("log10", 100.0, 2.0),
            ("log2", 8.0, 3.0),
            ("radians", 180.0, PI),
            ("positive", -2.0, -2.0),
            ("sin", FRAC_PI_2, 1.0),
            ("sqrt", 4.0, 2.0),
            ("tan", FRAC_PI_4, 1.0),
        ];
        assert_eq!(cases.len(), 18);
        for (name, value, expected) in cases {
            let array = floats(name, vec![Some(value), None]);
            if ["ceil", "floor"].contains(&name) {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(array.value(0), if name == "ceil" { -1 } else { -2 });
                assert!(array.is_null(1));
            } else {
                assert_floats(&array, &[Some(expected), None]);
            }
        }
    }

    #[test]
    fn eighteen_real_operations_keep_nonfinite_domain_and_signed_zero_rules() {
        use std::f64::consts::FRAC_PI_2;
        let nan = f64::from_bits(0xfff8123456789abc);
        let inf = f64::INFINITY;
        let cases = [
            (
                "acos",
                [2.0, nan, inf, -0.0],
                [None, None, None, Some(FRAC_PI_2)],
            ),
            (
                "asin",
                [-2.0, nan, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "atan",
                [nan, inf, -inf, -0.0],
                [None, Some(FRAC_PI_2), Some(-FRAC_PI_2), Some(-0.0)],
            ),
            (
                "cbrt",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "ceil",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(0.0)],
            ),
            ("cos", [nan, inf, -inf, -0.0], [None, None, None, Some(1.0)]),
            (
                "degress",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "dlog1",
                [-2.0, -1.0, nan, -0.0],
                [None, None, None, Some(0.0)],
            ),
            (
                "exp",
                [nan, inf, -inf, -0.0],
                [None, None, Some(0.0), Some(1.0)],
            ),
            (
                "floor",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(0.0)],
            ),
            ("ln", [0.0, -1.0, nan, inf], [None, None, None, None]),
            ("log10", [0.0, -1.0, nan, inf], [None, None, None, None]),
            ("log2", [0.0, -1.0, nan, inf], [None, None, None, None]),
            (
                "radians",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "positive",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "sin",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "sqrt",
                [-1.0, nan, inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "tan",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
        ];
        assert_eq!(cases.len(), 18);
        for (name, values, expected) in cases {
            let array = floats(name, values.into_iter().map(Some).collect());
            if ["ceil", "floor"].contains(&name) {
                assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![None, None, None, Some(0)]
                );
            } else {
                assert_floats(&array, &expected);
            }
        }
    }

    #[test]
    fn aliases_use_only_the_existing_supported_operations() {
        for (alias, canonical) in [
            ("ceiling", "ceil"),
            ("dceil", "ceil"),
            ("dfloor", "floor"),
            ("dexp", "exp"),
            ("dlog10", "log10"),
            ("dsqrt", "sqrt"),
        ] {
            let input = vec![Some(1.25), Some(-0.0), None];
            let left = floats(alias, input.clone());
            let right = floats(canonical, input);
            assert_eq!(left.to_data(), right.to_data(), "alias {alias}");
        }
    }

    #[test]
    fn all_seven_actual_numeric_profiles_preserve_input_conversion() {
        let profiles: [ArrayRef; 7] = [
            Arc::new(Int8Array::from(vec![Some(-2), None])),
            Arc::new(Int16Array::from(vec![Some(-2), None])),
            Arc::new(Int32Array::from(vec![Some(-2), None])),
            Arc::new(Int64Array::from(vec![Some(-2), None])),
            Arc::new(Float32Array::from(vec![Some(-2.0), None])),
            Arc::new(Float64Array::from(vec![Some(-2.0), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(-20), None])
                    .with_precision_and_scale(3, 1)
                    .unwrap(),
            ),
        ];
        for array in profiles {
            let source =
                FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true);
            let result = dense("atan", source, array);
            assert_floats(&result, &[Some(-1.1071487177940904), None]);
        }
    }

    #[test]
    fn decimal_scaling_large_precision_and_integer_rounding_remain_original() {
        for (precision, scale, raw, expected) in [
            (18, 3, 12345i128, 12.345_f64),
            (18, -2, -12345, -1234500.0),
            (38, 0, 99999999999999999999999999999999999999, 1e38),
            (38, 0, 9007199254740993, 9007199254740992.0),
        ] {
            let array: ArrayRef = Arc::new(
                Decimal128Array::from(vec![raw])
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            );
            let result = dense(
                "positive",
                FunctionValueType::from_legacy_declared_projection(
                    array.data_type().clone(),
                    false,
                ),
                array,
            );
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
                    .to_bits(),
                expected.to_bits()
            );
        }
        let result = dense(
            "positive",
            FunctionValueType::new(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![9007199254740993])),
        );
        assert_floats(&result, &[Some(9007199254740992.0)]);
    }

    #[test]
    fn dlog1_retains_add_then_log_rounding_instead_of_ln_1p() {
        let result = floats("dlog1", vec![Some(1e-20), Some(-1e-20), Some(-0.0)]);
        assert_floats(&result, &[Some(0.0), Some(0.0), Some(0.0)]);
    }

    #[test]
    fn ceil_floor_keep_arrow_safe_integer_boundary_instead_of_saturation() {
        for name in ["ceil", "floor"] {
            let result = dense(
                name,
                FunctionValueType::new(DataType::Int64, false),
                Arc::new(Int64Array::from(vec![i64::MAX, i64::MIN, 9007199254740993])),
            );
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, Some(i64::MIN), Some(9007199254740992)]
            );
            let result = floats(
                name,
                vec![
                    Some(9223372036854775808.0),
                    Some(-9223372036854775808.0),
                    Some(9223372036854774784.0),
                    Some(-9223372036854777856.0),
                ],
            );
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, Some(i64::MIN), Some(9223372036854774784), None]
            );
        }
    }

    #[test]
    fn positive_nonnullable_float_preserves_successful_null_and_minus_zero() {
        for array in [
            Arc::new(Float32Array::from(vec![f32::NAN, f32::INFINITY, -0.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![f64::NAN, f64::NEG_INFINITY, -0.0])) as ArrayRef,
        ] {
            let source = FunctionValueType::from_legacy_declared_projection(
                array.data_type().clone(),
                false,
            );
            let mut instance = instance("positive", source);
            assert!(
                instance.contract().result_type().nullable(),
                "the exact float owner must admit its existing sanitized NULL result"
            );
            let arguments = [EvaluatedArgument::Column(&array)];
            let result = instance
                .evaluate(Selection::all(3), &arguments, &Control::default())
                .unwrap();
            assert!(result.errors().is_empty());
            assert_floats(result.values(), &[None, None, Some(-0.0)]);
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
        let ty = FunctionValueType::new(DataType::Int64, true);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(-1), Some(9)]));
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
        for (name, expected) in [("sqrt", DataType::Float64), ("floor", DataType::Int64)] {
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
