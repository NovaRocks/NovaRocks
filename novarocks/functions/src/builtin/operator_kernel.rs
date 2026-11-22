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

use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, PrimitiveBuilder};
use arrow_array::types::{
    ArrowPrimitiveType, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type,
};
use arrow_array::{Array, ArrayRef, PrimitiveArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArithmeticOperator, ComparisonOperator, OperatorFunction, PureCompileControl,
};

use crate::arithmetic::{ArithmeticRowResult, SignedWidth, signed_arithmetic_row, trino_decimal};
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    DataFailureReason, EvaluatedArgument, FunctionBindingError, FunctionValueType,
    KernelEvaluationControl, KernelFailure, PreparedComparisonRecipe, PreparedScalarKernel,
    RowDataError, ScalarCallContract, ScalarCallInput, ScalarKernelInstance, SelectedValues,
};

#[derive(Debug)]
pub(super) struct BinaryOperatorKernel {
    contract: Arc<ScalarCallContract>,
    operation: Arc<Operation>,
}

#[derive(Debug)]
enum Operation {
    Signed(ArithmeticOperator, SignedWidth),
    Real(ArithmeticOperator),
    Double(ArithmeticOperator),
    Decimal(trino_decimal::DecimalArithmetic, [u128; 2]),
    Compare {
        operator: OperatorFunction,
        recipe: PreparedComparisonRecipe,
    },
}

impl BinaryOperatorKernel {
    pub(super) fn validate_comparison(
        operator: OperatorFunction,
        operands: [&FunctionValueType; 2],
        control: &dyn PureCompileControl,
    ) -> Result<bool, FunctionBindingError> {
        comparison_recipe(operator, operands, control).map(|recipe| recipe.nullable_result())
    }

    pub(super) fn prepare(
        operator: OperatorFunction,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut arguments = contract.value_argument_types();
        let (Some(left), Some(right), None) =
            (arguments.next(), arguments.next(), arguments.next())
        else {
            return Err(invalid("Binary operator expects two exact value arguments"));
        };
        drop(arguments);
        let operation = if let Some(op) = operator.arithmetic() {
            match (
                left.data_type(),
                right.data_type(),
                contract.result_type().data_type(),
            ) {
                (DataType::Float32, DataType::Float32, DataType::Float32) => Operation::Real(op),
                (DataType::Float64, DataType::Float64, DataType::Float64) => Operation::Double(op),
                (
                    DataType::Decimal128(p1, s1),
                    DataType::Decimal128(p2, s2),
                    DataType::Decimal128(..),
                ) => {
                    let (left, right) = ((*p1, *s1), (*p2, *s2));
                    Operation::Decimal(
                        trino_decimal::DecimalArithmetic::prepare(op, left, right),
                        [
                            10_u128.pow(u32::from(left.0)),
                            10_u128.pow(u32::from(right.0)),
                        ],
                    )
                }
                _ => {
                    let width = SignedWidth::from_type(contract.result_type())
                        .ok_or_else(|| invalid("Binary arithmetic has no exact implementation"))?;
                    Operation::Signed(op, width)
                }
            }
        } else {
            let recipe =
                comparison_recipe(operator, [left, right], control).map_err(
                    |error| match error {
                        FunctionBindingError::Control(error) => compile_failure(error),
                        _ => invalid("Binary comparison has no exact implementation"),
                    },
                )?;
            Operation::Compare { operator, recipe }
        };
        Ok(Self {
            contract,
            operation: Arc::new(operation),
        })
    }
}

fn comparison_recipe(
    operator: OperatorFunction,
    [left, right]: [&FunctionValueType; 2],
    control: &dyn PureCompileControl,
) -> Result<PreparedComparisonRecipe, FunctionBindingError> {
    let comparison = if operator.is_null_safe_comparison() {
        use DataType::*;
        if !matches!(
            left.data_type(),
            Null | Boolean
                | Int8
                | Int16
                | Int32
                | Int64
                | Float32
                | Float64
                | Utf8
                | Date32
                | Timestamp(_, None)
                | Decimal128(..)
                | Decimal256(..)
        ) {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        ComparisonOperator::Eq
    } else {
        operator
            .comparison()
            .ok_or(FunctionBindingError::NoMatchingOverload)?
    };
    PreparedComparisonRecipe::try_new(comparison, left, right, control).map_err(|error| {
        error.control_error().map_or(
            FunctionBindingError::NoMatchingOverload,
            FunctionBindingError::Control,
        )
    })
}

impl PreparedScalarKernel for BinaryOperatorKernel {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn invocation_resource_profile(&self) -> Option<crate::ScalarInvocationResourceProfile> {
        use crate::scalar_resources::PrimitiveBuilderOutput as Output;
        let output = match self.contract.result_type().data_type() {
            DataType::Boolean => Output::Boolean,
            DataType::Int8 => Output::Int8,
            DataType::Int16 => Output::Int16,
            DataType::Int32 => Output::Int32,
            DataType::Int64 => Output::Int64,
            DataType::Float32 => Output::Real,
            DataType::Float64 => Output::Double,
            DataType::Decimal128(..) => Output::Decimal128,
            _ => unreachable!("binary operator declares a funded primitive result"),
        };
        let row_errors = matches!(
            self.operation.as_ref(),
            Operation::Signed(..) | Operation::Decimal(..)
        );
        Some(crate::ScalarInvocationResourceProfile::primitive_builder(
            output, row_errors,
        ))
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<Instance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(Instance(Arc::clone(&self.operation))))
    }
}

struct Instance(Arc<Operation>);

impl ScalarKernelInstance for Instance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(0)?;
        let [left, right] = input.arguments() else {
            return Err(invalid("Binary operator expects two evaluated arguments"));
        };
        let mut work = EvaluationCheckpoints::new(control);
        let mut errors = Vec::new();
        let inputs = [*left, *right];
        let output = match self.0.as_ref() {
            Operation::Signed(operation, width) => {
                macro_rules! signed {
                    ($ty:ty) => {
                        arithmetic::<$ty>(input, inputs, &mut work, &mut errors, |l, r, ordinal| {
                            Ok(match signed_arithmetic_row(
                                *operation,
                                *width,
                                l.into(),
                                r.into(),
                                ordinal,
                            ) {
                                ArithmeticRowResult::Signed(value) => Ok(value as _),
                                ArithmeticRowResult::RowError(error) => Err(error),
                                _ => unreachable!(
                                    "signed arithmetic returns a signed coefficient or row error"
                                ),
                            })
                        })
                    };
                }
                match width {
                    SignedWidth::I8 => signed!(Int8Type),
                    SignedWidth::I16 => signed!(Int16Type),
                    SignedWidth::I32 => signed!(Int32Type),
                    SignedWidth::I64 => signed!(Int64Type),
                }
            }
            Operation::Real(operation) => {
                arithmetic::<Float32Type>(input, inputs, &mut work, &mut errors, |l, r, _| {
                    Ok(Ok(float(*operation, l, r)))
                })
            }
            Operation::Double(operation) => {
                arithmetic::<Float64Type>(input, inputs, &mut work, &mut errors, |l, r, _| {
                    Ok(Ok(float(*operation, l, r)))
                })
            }
            Operation::Decimal(recipe, [left_limit, right_limit]) => arithmetic::<Decimal128Type>(
                input,
                inputs,
                &mut work,
                &mut errors,
                |l, r, ordinal| {
                    if l.unsigned_abs() >= *left_limit || r.unsigned_abs() >= *right_limit {
                        return Err(internal(
                            "Binary arithmetic argument exceeds its declared decimal precision",
                        ));
                    }
                    Ok(recipe.evaluate(l, r).map_err(|reason| {
                        RowDataError::with_reason(
                            ordinal,
                            reason,
                            if reason == DataFailureReason::DivisionByZero {
                                "Division by zero"
                            } else {
                                "Decimal overflow"
                            },
                        )
                    }))
                },
            ),
            Operation::Compare { operator, recipe } => {
                let observed = crate::KernelControlObservation::new(control);
                match recipe.left_type().data_type() {
                    DataType::Float32 => compare_float::<Float32Type>(
                        input,
                        inputs,
                        *operator,
                        &mut work,
                        f32::is_nan,
                    ),
                    DataType::Float64 => compare_float::<Float64Type>(
                        input,
                        inputs,
                        *operator,
                        &mut work,
                        f64::is_nan,
                    ),
                    _ => comparison(input, &mut work, |ordinal, row, work| {
                        work.flush()?;
                        let equivalent = recipe
                            .compare_rows(*left, ordinal, row, *right, ordinal, row, &observed)?;
                        if operator.is_null_safe_comparison() {
                            let same = match equivalent {
                                Some(same) => same,
                                None => {
                                    let is_null = |arg: EvaluatedArgument<'_>| {
                                        arg.array().data_type() == &DataType::Null
                                            || arg.array().is_null(arg.value_row(ordinal, row))
                                    };
                                    is_null(*left) && is_null(*right)
                                }
                            };
                            Ok(Some(same ^ (*operator == OperatorFunction::Distinct)))
                        } else {
                            Ok(equivalent)
                        }
                    }),
                }
            }
        }?;
        work.flush()?;
        let output = SelectedValues::try_new_observed(
            input.selection(),
            input.contract().result_type().data_type(),
            output,
            errors.into_boxed_slice(),
            || work.step(),
        );
        work.finish_result(output)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

fn float<T>(operator: ArithmeticOperator, left: T, right: T) -> T
where
    T: std::ops::Add<Output = T>
        + std::ops::Sub<Output = T>
        + std::ops::Mul<Output = T>
        + std::ops::Div<Output = T>
        + std::ops::Rem<Output = T>,
{
    match operator {
        ArithmeticOperator::Add => left + right,
        ArithmeticOperator::Subtract => left - right,
        ArithmeticOperator::Multiply => left * right,
        ArithmeticOperator::Divide => left / right,
        ArithmeticOperator::Modulo => left % right,
    }
}

fn capacity(len: usize, width: usize) -> Result<(), KernelFailure> {
    len.checked_mul(width)
        .filter(|bytes| *bytes <= isize::MAX as usize)
        .map(|_| ())
        .ok_or(KernelFailure::ResourceExhausted)
}

fn carriers<'a, T: ArrowPrimitiveType>(
    inputs: [EvaluatedArgument<'a>; 2],
) -> Result<[&'a PrimitiveArray<T>; 2], KernelFailure> {
    let cast = |argument: EvaluatedArgument<'a>| {
        argument
            .array()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| internal("Binary operator primitive carrier cannot be downcast"))
    };
    Ok([cast(inputs[0])?, cast(inputs[1])?])
}

fn arithmetic<T: ArrowPrimitiveType>(
    input: ScalarCallInput<'_, '_>,
    inputs: [EvaluatedArgument<'_>; 2],
    work: &mut EvaluationCheckpoints<'_>,
    errors: &mut Vec<RowDataError>,
    mut evaluate: impl FnMut(
        T::Native,
        T::Native,
        usize,
    ) -> Result<Result<T::Native, RowDataError>, KernelFailure>,
) -> Result<ArrayRef, KernelFailure> {
    let [left, right] = carriers::<T>(inputs)?;
    let ty = input.contract().result_type().data_type().clone();
    let selection = input.selection();
    capacity(selection.len(), std::mem::size_of::<T::Native>())?;
    work.flush()?;
    let mut output = PrimitiveBuilder::<T>::with_capacity(selection.len()).with_data_type(ty);
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let l = inputs[0].value_row(ordinal, row);
        let r = inputs[1].value_row(ordinal, row);
        if left.is_null(l) || right.is_null(r) {
            output.append_null();
        } else {
            match evaluate(left.value(l), right.value(r), ordinal)? {
                Ok(value) => output.append_value(value),
                Err(error) => {
                    if errors.is_empty() {
                        errors
                            .try_reserve_exact(selection.len())
                            .map_err(|_| KernelFailure::ResourceExhausted)?;
                    }
                    errors.push(error);
                    output.append_null();
                }
            }
        }
    }
    work.flush()?;
    Ok(Arc::new(output.finish()))
}

fn comparison(
    input: ScalarCallInput<'_, '_>,
    work: &mut EvaluationCheckpoints<'_>,
    mut evaluate: impl FnMut(
        usize,
        usize,
        &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<bool>, KernelFailure>,
) -> Result<ArrayRef, KernelFailure> {
    let selection = input.selection();
    work.flush()?;
    let mut output = BooleanBuilder::with_capacity(selection.len());
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        output.append_option(evaluate(ordinal, row, work)?);
    }
    work.flush()?;
    Ok(Arc::new(output.finish()))
}

fn compare_float<T: ArrowPrimitiveType>(
    input: ScalarCallInput<'_, '_>,
    inputs: [EvaluatedArgument<'_>; 2],
    operator: OperatorFunction,
    work: &mut EvaluationCheckpoints<'_>,
    is_nan: impl Fn(T::Native) -> bool,
) -> Result<ArrayRef, KernelFailure>
where
    T::Native: PartialOrd,
{
    let [left, right] = carriers::<T>(inputs)?;
    comparison(input, work, |ordinal, row, _| {
        let l = inputs[0].value_row(ordinal, row);
        let r = inputs[1].value_row(ordinal, row);
        let (ln, rn) = (left.is_null(l), right.is_null(r));
        if operator.is_null_safe_comparison() {
            let same = if ln || rn {
                ln && rn
            } else {
                let (l, r) = (left.value(l), right.value(r));
                l == r || (is_nan(l) && is_nan(r))
            };
            return Ok(Some(same ^ (operator == OperatorFunction::Distinct)));
        }
        if ln || rn {
            return Ok(None);
        }
        let operator = operator.comparison().expect("ordinary floating comparison");
        Ok(Some(crate::comparison::ieee_float_comparison(
            operator,
            left.value(l),
            right.value(r),
        )))
    })
}
