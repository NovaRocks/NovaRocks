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
//! Exact native BitwiseNot recipe around the sole original bit-array authors.
//! Ordinary function binding/result metadata remains separate and unchanged.
use crate::arithmetic::ArithmeticPrepareError;
use crate::bit_array::{self, BitArrayObservation};
use crate::bit_numeric::BitwiseOp;
use crate::kernel_control::invalid;
use crate::{
    EvaluatedArgument, EvaluationCheckpoints, KernelEvaluationControl, KernelFailure,
    ScopedExpressionEffects,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ExpressionEffectContext, FunctionValueType,
    PureCompileControl, ValueLogicalType,
};
/// Source shapes implemented by the original signed BIGINT/LARGEINT bitnot path.
/// Unsupported UInt/PhysicalNull are explicit compile shape refusals in Exact
/// mode; this predicate never changes original SQL/native v1 admission.
pub fn native_bitnot_source_supported(source: &FunctionValueType) -> bool {
    matches!(
        (&source.logical_type(), source.data_type()),
        (
            ValueLogicalType::Physical,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
        ) | (ValueLogicalType::LargeInt, DataType::FixedSizeBinary(16))
    )
}
#[derive(Clone, Debug)]
pub struct PreparedNativeBitNotRecipe {
    source: FunctionValueType,
    result: FunctionValueType,
}
impl PreparedNativeBitNotRecipe {
    pub fn try_new(
        source: &FunctionValueType,
        result: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ArithmeticPrepareError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let outcome = (|| {
            crate::kernel_input::validate_type_observed(source, &mut work)
                .map_err(ArithmeticPrepareError::Kernel)?;
            crate::kernel_input::validate_type_observed(result, &mut work)
                .map_err(ArithmeticPrepareError::Kernel)?;
            work.step()?;
            if !native_bitnot_source_supported(source) {
                return Err(ArithmeticPrepareError::Kernel(invalid(&format!(
                    "native BitwiseNot unsupported source shape: {source:?}"
                ))));
            }
            if source.logical_type() != result.logical_type()
                || source.data_type() != result.data_type()
                || (source.nullable() && !result.nullable())
            {
                return Err(ArithmeticPrepareError::TypeMismatch);
            }
            work.step()?;
            Ok(Self {
                source: source.clone(),
                result: result.clone(),
            })
        })();
        if outcome
            .as_ref()
            .err()
            .is_some_and(|e: &ArithmeticPrepareError| e.control_error().is_some())
        {
            return outcome;
        }
        work.finish()?;
        outcome
    }
    pub fn source_type(&self) -> &FunctionValueType {
        &self.source
    }
    pub fn result_type(&self) -> &FunctionValueType {
        &self.result
    }
    pub fn own_effects(&self, context: ExpressionEffectContext) -> ScopedExpressionEffects {
        ScopedExpressionEffects::pure_value(context)
    }
    /// Current invocation addresses and inherited data errors are retained.
    pub fn evaluate_selected<'a>(
        &self,
        argument: EvaluatedArgument<'a>,
        selection: crate::Selection<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<crate::SelectedValues<'a>, KernelFailure> {
        let observed = crate::KernelControlObservation::new(control);
        observed.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(&observed);
        let outcome = (|| {
            if let EvaluatedArgument::SelectedColumn(values) = argument {
                if !values
                    .selection()
                    .same_rows_observed(selection, || work.step())?
                {
                    return Err(invalid(
                        "native BitwiseNot compact argument has a foreign selection",
                    ));
                }
            } else {
                argument.validate_shape_observed::<KernelFailure>(selection, || work.step())?;
            }
            if selection.is_empty() {
                // Shape/type obligations remain observable on an empty demand;
                // the existing validator inspects no inactive row in this domain.
                crate::kernel_input::validate_argument_observed(
                    argument,
                    selection,
                    &self.source,
                    &observed,
                )?;
            }
            let inherited = match argument {
                EvaluatedArgument::SelectedColumn(values) => values.errors(),
                _ => &[],
            };
            let mut next_error = inherited.iter().peekable();
            let mut rows = Vec::with_capacity(selection.len());
            let mut errors = Vec::new();
            work.flush()?;
            for ordinal in 0..selection.len() {
                let row = selection
                    .row(ordinal)
                    .ok_or_else(|| invalid("native BitwiseNot selection has a missing address"))?;
                work.step()?;
                if next_error
                    .peek()
                    .is_some_and(|error| error.selected_ordinal() == ordinal)
                {
                    let error = next_error
                        .next()
                        .ok_or_else(|| invalid("native BitwiseNot inherited error is missing"))?;
                    errors.push(error.clone());
                    rows.push(arrow_array::new_null_array(self.result.data_type(), 1));
                    work.step()?;
                } else {
                    work.flush()?;
                    rows.push(self.evaluate_row(argument, ordinal, row, &observed)?);
                    work.step()?;
                }
            }
            work.flush()?;
            let output = if rows.is_empty() {
                arrow_array::new_empty_array(self.result.data_type())
            } else {
                let mut arrays = Vec::with_capacity(rows.len());
                for array in &rows {
                    arrays.push(array.as_ref());
                    work.step()?;
                }
                work.flush()?;
                arrow_select::concat::concat(&arrays).map_err(|error| {
                    crate::kernel_control::internal(&format!(
                        "native BitwiseNot selected assembly: {error}"
                    ))
                })?
            };
            work.flush()?;
            crate::SelectedValues::try_new_observed(
                selection,
                self.result.data_type(),
                output,
                errors.into_boxed_slice(),
                || work.step(),
            )
        })();
        observed.finish(work.finish_result(outcome))
    }

    pub fn evaluate_row(
        &self,
        argument: EvaluatedArgument<'_>,
        ordinal: usize,
        logical_row: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let outcome = (|| {
            crate::arithmetic::checked_row(
                argument,
                ordinal,
                logical_row,
                &self.source,
                |array| {
                    let valid = match self.source.data_type() {
                        DataType::Int8 => array.as_any().is::<arrow_array::Int8Array>(),
                        DataType::Int16 => array.as_any().is::<arrow_array::Int16Array>(),
                        DataType::Int32 => array.as_any().is::<arrow_array::Int32Array>(),
                        DataType::Int64 => array.as_any().is::<arrow_array::Int64Array>(),
                        DataType::FixedSizeBinary(16) => {
                            array.as_any().is::<arrow_array::FixedSizeBinaryArray>()
                        }
                        _ => false,
                    };
                    if valid {
                        Ok(())
                    } else {
                        Err(invalid("native BitwiseNot has a foreign concrete operand"))
                    }
                },
                &mut work,
            )?;
            let row = argument.value_row(ordinal, logical_row);
            work.flush()?;
            let operand = argument.array().slice(row, 1);
            work.flush()?;
            let mut observer = |event| match event {
                BitArrayObservation::Step => work.step(),
                BitArrayObservation::OpaqueBoundary => work.flush(),
            };
            let answer = if self.source.logical_type() == ValueLogicalType::LargeInt {
                // The original reader is bounded to one already-demanded 16B
                // row; this does not assert or grant allocator capacity.
                observer(BitArrayObservation::OpaqueBoundary)?;
                let source = bit_array::to_i128_values(&operand, 0)
                    .map_err(crate::bit_array::BitArrayError::into_kernel)?;
                observer(BitArrayObservation::OpaqueBoundary)?;
                let values = bit_array::map_values_observed(
                    crate::Selection::all(1),
                    |_, row| Ok(source[row]),
                    |v| BitwiseOp::Not.apply_i128(v, 0),
                    &mut observer,
                )?;
                bit_array::cast_largeint_output_observed(
                    &values,
                    Some(self.result.data_type()),
                    &mut observer,
                )?
            } else {
                observer(BitArrayObservation::OpaqueBoundary)?;
                let source = bit_array::to_i64_array(&operand, 0)
                    .map_err(crate::bit_array::BitArrayError::into_kernel)?;
                observer(BitArrayObservation::OpaqueBoundary)?;
                let values = bit_array::map_values_observed(
                    crate::Selection::all(1),
                    |_, row| Ok((!source.is_null(row)).then(|| source.value(row))),
                    |v| BitwiseOp::Not.apply_i64(v, 0),
                    &mut observer,
                )?;
                bit_array::finish_i64_observed(
                    values,
                    Some(self.result.data_type()),
                    &mut observer,
                )?
            };
            // Valid admitted concrete signed/LARGEINT rows and matching output
            // metadata cannot hit original conversion/output data guards. A
            // forged ABI is never reinterpreted as a successful strict NULL.
            answer.map_err(crate::bit_array::BitArrayError::into_kernel)
        })();
        work.finish_result(outcome)
    }
}
