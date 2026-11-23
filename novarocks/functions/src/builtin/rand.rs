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

use arrow_array::{
    Array, ArrayRef, PrimitiveArray,
    builder::{Float64Builder, PrimitiveBuilder},
    types::{ArrowPrimitiveType, Int8Type, Int16Type, Int32Type, Int64Type},
};
use arrow_schema::DataType;
use novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT;
use novarocks_type_contract::ValueLogicalType;
use rand::{
    Rng, SeedableRng,
    rngs::{OsRng, StdRng},
};
use std::sync::Arc;

use crate::{
    DataFailureReason, EvaluatedArgument, FunctionArgumentType, KernelDiagnostic,
    KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput, ScalarKernelInstance,
    SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    selected_copy::{CopyError, fixed_interleave_extent},
};

pub(super) struct RandInstance {
    sequence: Option<StdRng>,
}
impl RandInstance {
    pub(super) const fn new() -> Self {
        Self { sequence: None }
    }
    pub(super) const fn retained_upper_bound() -> usize {
        std::mem::size_of::<Self>()
    }

    fn sequence(&mut self) -> Result<&mut StdRng, KernelFailure> {
        if self.sequence.is_none() {
            self.sequence = Some(StdRng::from_rng(OsRng).map_err(|_| {
                KernelFailure::Operational(KernelDiagnostic::new(
                    "RAND entropy source could not initialize its instance",
                ))
            })?);
        }
        self.sequence
            .as_mut()
            .ok_or_else(|| internal("RAND sequence was not initialized"))
    }

    fn bounded<T: ArrowPrimitiveType>(
        &mut self,
        selection: Selection<'_>,
        bound: EvaluatedArgument<'_>,
        nullable: bool,
        work: &mut EvaluationCheckpoints<'_>,
        errors: &mut Vec<RowDataError>,
    ) -> Result<ArrayRef, KernelFailure>
    where
        T::Native: Into<i64> + TryFrom<i64>,
    {
        let bounds = bound
            .array()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| internal("RAND bound differs from its checked integer carrier"))?;
        work.flush()?;
        let mut output = PrimitiveBuilder::<T>::with_capacity(selection.len());
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let address = bound.value_row(ordinal, row);
            if address >= bounds.len() {
                return Err(internal("RAND bound row is outside its checked carrier"));
            }
            if bounds.is_null(address) {
                if !nullable {
                    return Err(internal("RAND non-null bound contains a selected SQL NULL"));
                }
                output.append_null();
                continue;
            }
            let upper = bounds.value(address).into();
            if upper <= 0 {
                errors
                    .try_reserve(1)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                errors.push(RowDataError::with_reason(
                    ordinal,
                    DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
                    "Bound must be positive",
                ));
                output.append_null();
                continue;
            }
            let value = self.sequence()?.gen_range(0..upper);
            output.append_value(
                T::Native::try_from(value)
                    .map_err(|_| internal("RAND sample is outside its checked result width"))?,
            );
        }
        work.flush()?;
        Ok(Arc::new(output.finish()))
    }
}

impl ScalarKernelInstance for RandInstance {
    fn retained_bytes(&self) -> usize {
        Self::retained_upper_bound()
    }
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(0)?;
        let result_type = input.contract().result_type();
        let carrier = result_type.data_type();
        if result_type.logical_type() != ValueLogicalType::Physical {
            return Err(invalid("RAND requires a physical numeric result"));
        }
        let selection = input.selection();
        fixed_interleave_extent(carrier, selection.len()).map_err(CopyError::into_kernel)?;
        let mut errors = Vec::new();
        let mut work = EvaluationCheckpoints::new(control);
        let values = match (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        ) {
            ([], []) if carrier == &DataType::Float64 && !result_type.nullable() => {
                work.flush()?;
                let mut output = Float64Builder::with_capacity(selection.len());
                for _ in selection.iter() {
                    work.step()?;
                    output.append_value(self.sequence()?.r#gen::<f64>());
                }
                work.flush()?;
                Arc::new(output.finish()) as ArrayRef
            }
            ([FunctionArgumentType::Value(source)], [bound])
                if source.logical_type() == ValueLogicalType::Physical
                    && source.data_type() == carrier
                    && source.nullable() == result_type.nullable()
                    && bound.array().data_type() == carrier =>
            {
                let nullable = source.nullable();
                match carrier {
                    DataType::Int8 => self.bounded::<Int8Type>(
                        selection,
                        *bound,
                        nullable,
                        &mut work,
                        &mut errors,
                    )?,
                    DataType::Int16 => self.bounded::<Int16Type>(
                        selection,
                        *bound,
                        nullable,
                        &mut work,
                        &mut errors,
                    )?,
                    DataType::Int32 => self.bounded::<Int32Type>(
                        selection,
                        *bound,
                        nullable,
                        &mut work,
                        &mut errors,
                    )?,
                    DataType::Int64 => self.bounded::<Int64Type>(
                        selection,
                        *bound,
                        nullable,
                        &mut work,
                        &mut errors,
                    )?,
                    _ => return Err(invalid("RAND requires a signed integer bound")),
                }
            }
            _ => {
                return Err(invalid(
                    "RAND arguments differ from their checked signature",
                ));
            }
        };
        let selected = SelectedValues::try_new_observed(
            selection,
            carrier,
            values,
            errors.into_boxed_slice(),
            || work.step(),
        )?;
        work.finish()?;
        Ok(selected)
    }
}
