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

//! Immutable selected arithmetic for exact frozen numeric value domains.
//! Preparation freezes both complete input domains, result, and authored policies. This
//! primitive neither allocates output arrays nor grants their memory ownership.

use arrow_array::{Array, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow_buffer::i256;
use arrow_schema::DataType;
#[path = "arithmetic_decimal.rs"]
mod decimal;
#[path = "arithmetic_float.rs"]
mod float;
#[path = "arithmetic_largeint.rs"]
mod largeint;
use novarocks_type_contract::{
    ArithmeticOperator, CompileCheckpoints, CompileControlError, CompilePhase,
    DecimalOverflowPolicy, ExpressionEffectContext, ExpressionEffects, FunctionValueType,
    PureCompileControl, ValueLogicalType, arithmetic_result_value_shape_with_op,
};
use std::{error::Error, fmt};

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, validate_type_observed};
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError,
    ScopedExpressionEffects,
};

/// Nominal source of a before-allocation canonical arithmetic type refusal.
#[derive(Debug)]
pub enum ArithmeticTypeCreationFailure<H> {
    Control(CompileControlError),
    Creator(H),
}

/// The original preparation error is separate from construction's exact
/// creator/control/resource cause; no typed host failure is erased or formatted.
#[derive(Debug)]
pub enum ArithmeticCreationFailure<H> {
    Prepare(ArithmeticPrepareError),
    Construction(
        novarocks_type_contract::ValueConstructionFailure<ArithmeticTypeCreationFailure<H>>,
    ),
}
impl<H> From<ArithmeticPrepareError> for ArithmeticCreationFailure<H> {
    fn from(error: ArithmeticPrepareError) -> Self {
        Self::Prepare(error)
    }
}
impl<H> From<CompileControlError> for ArithmeticCreationFailure<H> {
    fn from(error: CompileControlError) -> Self {
        Self::Prepare(error.into())
    }
}
impl<H> ArithmeticCreationFailure<H> {
    fn is_primary(&self) -> bool {
        use novarocks_type_contract::ValueConstructionFailure as Failure;
        match self {
            Self::Prepare(error) => error.control_error().is_some(),
            Self::Construction(
                Failure::Observer(_)
                | Failure::Layout(_)
                | Failure::Vec(_)
                | Failure::HashMap(_)
                | Failure::BTree(_),
            ) => true,
            Self::Construction(Failure::ValueType(_) | Failure::Rule(_)) => false,
        }
    }
}
fn pure_arithmetic_creation_failure(
    error: ArithmeticCreationFailure<std::convert::Infallible>,
) -> ArithmeticPrepareError {
    use novarocks_type_contract::ValueConstructionFailure as Failure;
    match error {
        ArithmeticCreationFailure::Prepare(error) => error,
        ArithmeticCreationFailure::Construction(Failure::Observer(
            ArithmeticTypeCreationFailure::Control(error),
        )) => error.into(),
        ArithmeticCreationFailure::Construction(Failure::Observer(
            ArithmeticTypeCreationFailure::Creator(never),
        )) => match never {},
        ArithmeticCreationFailure::Construction(
            Failure::Layout(_) | Failure::Vec(_) | Failure::HashMap(_) | Failure::BTree(_),
        ) => CompileControlError::ResourceExhausted.into(),
        ArithmeticCreationFailure::Construction(Failure::ValueType(_) | Failure::Rule(_)) => {
            ArithmeticPrepareError::TypeMismatch
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArithmeticPrepareError {
    Control(CompileControlError),
    Kernel(KernelFailure),
    Unsupported,
    TypeMismatch,
}
impl ArithmeticPrepareError {
    pub fn control_error(&self) -> Option<CompileControlError> {
        match self {
            Self::Control(cause) | Self::Kernel(KernelFailure::CompileControl(cause)) => {
                Some(cause.clone())
            }
            Self::Kernel(KernelFailure::Cancelled) => Some(CompileControlError::Cancelled),
            Self::Kernel(KernelFailure::DeadlineExceeded) => {
                Some(CompileControlError::DeadlineExceeded)
            }
            Self::Kernel(KernelFailure::ResourceExhausted) => {
                Some(CompileControlError::ResourceExhausted)
            }
            _ => None,
        }
    }
}
impl From<CompileControlError> for ArithmeticPrepareError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl fmt::Display for ArithmeticPrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Kernel(error) => error.fmt(f),
            Self::Unsupported => {
                f.write_str("arithmetic requires an implemented exact numeric value domain")
            }
            Self::TypeMismatch => {
                f.write_str("arithmetic result differs from its frozen nullable value type")
            }
        }
    }
}
impl Error for ArithmeticPrepareError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Kernel(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignedWidth {
    I8,
    I16,
    I32,
    I64,
}
impl SignedWidth {
    fn from_type(ty: &FunctionValueType) -> Option<Self> {
        if ty.logical_type() != ValueLogicalType::Physical {
            return None;
        }
        Self::from_carrier(ty.data_type())
    }
    fn from_carrier(carrier: &DataType) -> Option<Self> {
        Some(match carrier {
            DataType::Int8 => Self::I8,
            DataType::Int16 => Self::I16,
            DataType::Int32 => Self::I32,
            DataType::Int64 => Self::I64,
            _ => return None,
        })
    }
    fn validate(self, array: &dyn Array) -> bool {
        match self {
            Self::I8 => array.as_any().is::<Int8Array>(),
            Self::I16 => array.as_any().is::<Int16Array>(),
            Self::I32 => array.as_any().is::<Int32Array>(),
            Self::I64 => array.as_any().is::<Int64Array>(),
        }
    }
    fn read(self, array: &dyn Array, row: usize) -> Result<i64, KernelFailure> {
        macro_rules! read {
            ($array:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(|array| i64::from(array.value(row)))
                    .ok_or_else(|| {
                        internal("signed arithmetic carrier has a foreign array implementation")
                    })
            };
        }
        match self {
            Self::I8 => read!(Int8Array),
            Self::I16 => read!(Int16Array),
            Self::I32 => read!(Int32Array),
            Self::I64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|array| array.value(row))
                .ok_or_else(|| {
                    internal("signed arithmetic carrier has a foreign array implementation")
                }),
        }
    }
}

/// A row error uses the supplied left compact ordinal. The controller must
/// remap it when publishing into another output Selection; this is not a
/// source logical row number. No output vector or deferred replay is owned here.
#[derive(Clone, Debug, PartialEq)]
pub enum ArithmeticRowResult {
    Null,
    Signed(i64),
    Float(f64),
    LargeInt(i128),
    Decimal128(i128),
    Decimal256(i256),
    RowError(RowDataError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ArithmeticAlgorithm {
    Signed {
        left: SignedWidth,
        right: SignedWidth,
    },
    Decimal(decimal::DecimalArithmetic),
    LargeInt(largeint::LargeIntArithmetic),
    Float(float::FloatArithmetic),
}
impl ArithmeticAlgorithm {
    fn validate(&self, array: &dyn Array, left: bool) -> Result<(), KernelFailure> {
        match self {
            Self::Signed { left: l, right: r } => {
                if (if left { *l } else { *r }).validate(array) {
                    Ok(())
                } else {
                    Err(internal(
                        "signed arithmetic carrier has a foreign array implementation",
                    ))
                }
            }
            Self::Decimal(recipe) => {
                if left {
                    recipe.validate_left(array)
                } else {
                    recipe.validate_right(array)
                }
            }
            Self::LargeInt(recipe) => {
                if left {
                    recipe.validate_left(array)
                } else {
                    recipe.validate_right(array)
                }
            }
            Self::Float(_) => {
                unreachable!("floating arithmetic validates through its original CAST author")
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedArithmeticRecipe {
    operator: ArithmeticOperator,
    left: FunctionValueType,
    right: FunctionValueType,
    result: FunctionValueType,
    algorithm: ArithmeticAlgorithm,
    decimal_overflow_policy: DecimalOverflowPolicy,
    allow_throw_exception: bool,
}
impl PreparedArithmeticRecipe {
    pub fn try_new(
        operator: ArithmeticOperator,
        left: &FunctionValueType,
        right: &FunctionValueType,
        result: &FunctionValueType,
        decimal_policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ArithmeticPrepareError> {
        Self::try_new_core::<std::convert::Infallible>(
            operator,
            left,
            right,
            result,
            decimal_policy,
            allow_throw_exception,
            |_: &mut CompileCheckpoints<'_>| Ok(FunctionValueType::new(DataType::Float64, true)),
            control,
        )
        .map_err(pure_arithmetic_creation_failure)
    }

    /// The actual factory opens only the Float target's independent lifetime,
    /// after the original supported-input profile selected that construction.
    /// Other families create no unused target or scope.
    pub fn try_new_with_creator_in<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
        operator: ArithmeticOperator,
        left: &FunctionValueType,
        right: &FunctionValueType,
        result: &FunctionValueType,
        decimal_policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
        factory: &F,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ArithmeticCreationFailure<H>> {
        Self::try_new_core(
            operator,
            left,
            right,
            result,
            decimal_policy,
            allow_throw_exception,
            |work| float::canonical_target_with_factory(factory, work),
            control,
        )
    }

    fn try_new_core<H>(
        operator: ArithmeticOperator,
        left: &FunctionValueType,
        right: &FunctionValueType,
        result: &FunctionValueType,
        decimal_policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
        mut target: impl FnMut(
            &mut CompileCheckpoints<'_>,
        ) -> Result<FunctionValueType, ArithmeticCreationFailure<H>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ArithmeticCreationFailure<H>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let outcome: Result<Self, ArithmeticCreationFailure<H>> = (|| {
            for ty in [left, right, result] {
                validate_type_observed(ty, &mut work).map_err(ArithmeticPrepareError::Kernel)?;
            }
            // Both owning publication and this borrowed decision consume the
            // same original numeric result author. Preparation admits its
            // nullable result law without constructing discarded type nodes.
            let expected = arithmetic_result_value_shape_with_op(left, right, operator)
                .ok_or(ArithmeticPrepareError::Unsupported)?;
            work.step()?;
            let algorithm = if let (Some(left), Some(right)) =
                (SignedWidth::from_type(left), SignedWidth::from_type(right))
            {
                ArithmeticAlgorithm::Signed { left, right }
            } else if let Some(recipe) = float::FloatArithmetic::prepare(
                operator,
                left,
                right,
                decimal_policy,
                allow_throw_exception,
                &mut target,
                &mut work,
                control,
            )? {
                ArithmeticAlgorithm::Float(recipe)
            } else if let Some(recipe) = decimal::DecimalArithmetic::prepare(
                operator,
                left,
                right,
                (&expected.0, expected.1, true),
                decimal_policy,
                allow_throw_exception,
                &mut work,
            )? {
                ArithmeticAlgorithm::Decimal(recipe)
            } else if let Some(recipe) =
                largeint::LargeIntArithmetic::prepare(operator, left, right, &mut work)?
            {
                ArithmeticAlgorithm::LargeInt(recipe)
            } else {
                return Err(ArithmeticPrepareError::Unsupported.into());
            };
            work.step()?;
            let matches = result.nullable()
                && result.logical_type() == expected.1
                && result.data_type() == &expected.0;
            work.step()?;
            if !matches {
                return Err(ArithmeticPrepareError::TypeMismatch.into());
            }
            // All admitted carriers are now closed scalar types: these copies
            // contain no field graph, metadata, timezone or dynamic backing.
            let recipe = Self {
                operator,
                left: left.clone(),
                right: right.clone(),
                result: result.clone(),
                algorithm,
                decimal_overflow_policy: decimal_policy,
                allow_throw_exception,
            };
            work.step()?;
            Ok(recipe)
        })();
        if outcome
            .as_ref()
            .err()
            .is_some_and(ArithmeticCreationFailure::is_primary)
        {
            return outcome;
        }
        work.finish()?;
        outcome
    }
    pub fn operator(&self) -> ArithmeticOperator {
        self.operator
    }
    pub fn left_type(&self) -> &FunctionValueType {
        &self.left
    }
    pub fn right_type(&self) -> &FunctionValueType {
        &self.right
    }
    pub fn result_type(&self) -> &FunctionValueType {
        &self.result
    }
    pub fn decimal_overflow_policy(&self) -> DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }
    pub fn allow_throw_exception(&self) -> bool {
        self.allow_throw_exception
    }
    pub fn own_effects(&self, context: ExpressionEffectContext) -> ScopedExpressionEffects {
        let effects = match &self.algorithm {
            ArithmeticAlgorithm::Signed { left, right } => ExpressionEffects {
                may_raise_row_error: match self.operator {
                    ArithmeticOperator::Modulo => true,
                    ArithmeticOperator::Divide => false,
                    _ => *left == SignedWidth::I64 || *right == SignedWidth::I64,
                },
                ..ExpressionEffects::PURE_VALUE
            },
            ArithmeticAlgorithm::Decimal(recipe) => recipe.own_effects(),
            ArithmeticAlgorithm::LargeInt(recipe) => recipe.own_effects(),
            ArithmeticAlgorithm::Float(_) => ExpressionEffects::PURE_VALUE,
        };
        ScopedExpressionEffects::primitive(context, effects)
    }

    /// Validate both original addresses before strict SQL NULL. Inherited row
    /// errors must have been excluded by the controller; an unresolved compact
    /// error is rejected rather than misread as a successful NULL.
    pub fn evaluate_row(
        &self,
        left: EvaluatedArgument<'_>,
        left_ordinal: usize,
        left_logical_row: usize,
        right: EvaluatedArgument<'_>,
        right_ordinal: usize,
        right_logical_row: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArithmeticRowResult, KernelFailure> {
        if let ArithmeticAlgorithm::Float(recipe) = &self.algorithm {
            return recipe.evaluate_row(
                left,
                left_ordinal,
                left_logical_row,
                right,
                right_ordinal,
                right_logical_row,
                control,
            );
        }
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let outcome = (|| {
            let l = checked_row(
                left,
                left_ordinal,
                left_logical_row,
                &self.left,
                |array| self.algorithm.validate(array, true),
                &mut work,
            )?;
            let r = checked_row(
                right,
                right_ordinal,
                right_logical_row,
                &self.right,
                |array| self.algorithm.validate(array, false),
                &mut work,
            )?;
            let (Some(l), Some(r)) = (l, r) else {
                return Ok(ArithmeticRowResult::Null);
            };
            let output = match &self.algorithm {
                ArithmeticAlgorithm::Signed {
                    left: left_width,
                    right: right_width,
                } => {
                    let l = left_width.read(left.array().as_ref(), l)?;
                    work.step()?;
                    let r = right_width.read(right.array().as_ref(), r)?;
                    work.step()?;
                    use ArithmeticOperator::*;
                    let output = match self.operator {
                        Divide if r == 0 => ArithmeticRowResult::Null,
                        Divide => ArithmeticRowResult::Float((l as f64) / (r as f64)),
                        Modulo if r == 0 => ArithmeticRowResult::RowError(RowDataError::new(
                            left_ordinal,
                            "Divide by zero error",
                        )),
                        Modulo => ArithmeticRowResult::Signed(l.wrapping_rem(r)),
                        Add | Subtract | Multiply => {
                            let (value, symbol) = match self.operator {
                                Add => (l.checked_add(r), '+'),
                                Subtract => (l.checked_sub(r), '-'),
                                Multiply => (l.checked_mul(r), '*'),
                                _ => unreachable!("checked signed arithmetic operation"),
                            };
                            match value {
                                Some(value) => ArithmeticRowResult::Signed(value),
                                None => ArithmeticRowResult::RowError(RowDataError::new(
                                    left_ordinal,
                                    &format!(
                                        "Arithmetic overflow: Overflow happened on: {l:?} {symbol} {r:?}"
                                    ),
                                )),
                            }
                        }
                    };
                    work.step()?;
                    output
                }
                ArithmeticAlgorithm::Decimal(recipe) => recipe.evaluate_non_null(
                    left.array().as_ref(),
                    l,
                    right.array().as_ref(),
                    r,
                    left_ordinal,
                    &mut work,
                )?,
                ArithmeticAlgorithm::LargeInt(recipe) => recipe.evaluate_non_null(
                    left.array().as_ref(),
                    l,
                    right.array().as_ref(),
                    r,
                    left_ordinal,
                    &mut work,
                )?,
                ArithmeticAlgorithm::Float(_) => {
                    unreachable!("floating arithmetic uses its original CAST scalar entry")
                }
            };
            Ok(output)
        })();
        // EvaluationCheckpoints preserves any primary callback refusal, even
        // an Internal/Operational/InvalidProgram cause; it never rechecks it.
        work.finish()?;
        outcome
    }
}

pub(crate) fn checked_row(
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    logical_row: usize,
    expected: &FunctionValueType,
    validate: impl FnOnce(&dyn Array) -> Result<(), KernelFailure>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<usize>, KernelFailure> {
    if let Some(value) = argument.constant_value() {
        let actual = value.value_type();
        let matches = actual.logical_type() == expected.logical_type()
            && (!actual.nullable() || expected.nullable());
        work.step()?;
        if !matches {
            return Err(invalid(
                "arithmetic constant differs from its frozen value type",
            ));
        }
    }
    let array = argument.array();
    // All admitted arithmetic carriers are closed scalar types, so exact
    // equality here includes decimal precision/scale without a nested walk.
    let matches = array.data_type() == expected.data_type();
    work.step()?;
    if !matches {
        return Err(invalid(
            "arithmetic argument differs from its frozen numeric carrier",
        ));
    }
    let shape_matches = match argument {
        EvaluatedArgument::Scalar(array) => array.len() == 1,
        EvaluatedArgument::ConstantView(value) => value.has_original_shape(),
        EvaluatedArgument::SelectedColumn(values) => {
            values.selection().row(ordinal) == Some(logical_row)
        }
        _ => true,
    };
    work.step()?;
    if !shape_matches {
        return Err(invalid(
            "arithmetic argument has a foreign scalar or compact address",
        ));
    }
    if let EvaluatedArgument::SelectedColumn(values) = argument {
        // The checked errors are sorted and unique. Search only this actual
        // compact ordinal without scanning another row or building a bitmap.
        let (mut start, mut end) = (0, values.errors().len());
        while start < end {
            let middle = start + (end - start) / 2;
            let error_ordinal = values.errors()[middle].selected_ordinal();
            work.step()?;
            match error_ordinal.cmp(&ordinal) {
                std::cmp::Ordering::Less => start = middle + 1,
                std::cmp::Ordering::Greater => end = middle,
                std::cmp::Ordering::Equal => {
                    return Err(invalid(
                        "arithmetic argument contains an unresolved selected row error",
                    ));
                }
            }
        }
    }
    let row = argument.value_row(ordinal, logical_row);
    let in_bounds = row < array.len();
    work.step()?;
    if !in_bounds {
        return Err(invalid("arithmetic selected address is outside its array"));
    }
    let valid_implementation = validate(array.as_ref());
    work.step()?;
    valid_implementation?;
    let is_null = array.is_null(row);
    work.step()?;
    if is_null {
        return if expected.nullable() {
            Ok(None)
        } else {
            Err(invalid(
                "non-null arithmetic argument contains a selected NULL",
            ))
        };
    }
    Ok(Some(row))
}

#[cfg(test)]
#[path = "arithmetic_tests.rs"]
mod tests;
