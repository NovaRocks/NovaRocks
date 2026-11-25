// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! NULLIF comparison setup and the compatibility entry for already evaluated inputs.
use novarocks_type_contract::{KeyRelation, OperatorFunction, PureCompileControl, TrinoCastMode};

#[derive(Clone, Debug)]
pub enum PreparedNullIfEquality {
    Ordinary(crate::PreparedComparisonRecipe),
    Composite(crate::PreparedCompositeComparisonRecipe),
}

/// Exact control recipe prepared from the verified left value and right comparison domain.
#[derive(Clone, Debug)]
pub struct PreparedNullIf {
    call: std::sync::Arc<crate::FunctionCallContract>,
    left_cast: crate::PreparedCastRecipe,
    equality: PreparedNullIfEquality,
}
impl PreparedNullIf {
    pub(crate) fn from_call(
        call: std::sync::Arc<crate::FunctionCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, crate::FunctionSpecializationFailure> {
        use crate::FunctionSpecializationFailure;
        let [
            FunctionArgumentType::Value(left),
            FunctionArgumentType::Value(right),
        ] = call.selected().argument_types.as_ref()
        else {
            return Err(FunctionSpecializationFailure::InvalidInput(
                "NULLIF requires two exact values",
            ));
        };
        let compare_left = right.with_nullable(left.nullable()).map_err(|error| {
            FunctionSpecializationFailure::Binding(crate::FunctionBindingError::ValueType(error))
        })?;
        let left_cast = crate::PreparedCastRecipe::try_new_trino(
            left,
            &compare_left,
            TrinoCastMode::Implicit,
            control,
        )
        .map_err(|error| {
            error.control_error().map_or(
                FunctionSpecializationFailure::InvalidInput(
                    "NULLIF left comparison conversion is unavailable",
                ),
                FunctionSpecializationFailure::Control,
            )
        })?;
        let equality = prepare_equality(&compare_left, right, control)
            .map_err(FunctionSpecializationFailure::Binding)?;
        Ok(Self {
            call,
            left_cast,
            equality,
        })
    }

    pub fn call(&self) -> &crate::FunctionCallContract {
        &self.call
    }
    pub fn left_cast(&self) -> &crate::PreparedCastRecipe {
        &self.left_cast
    }
    pub fn equality(&self) -> &PreparedNullIfEquality {
        &self.equality
    }
}

pub(super) fn prepare_equality(
    left: &crate::FunctionValueType,
    right: &crate::FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<PreparedNullIfEquality, crate::FunctionBindingError> {
    if crate::PreparedCompositeComparisonRecipe::is_composite(left) {
        return crate::PreparedCompositeComparisonRecipe::try_new(
            KeyRelation::Equal,
            left,
            right,
            control,
        )
        .map(PreparedNullIfEquality::Composite)
        .map_err(|error| {
            error.control_error().map_or(
                crate::FunctionBindingError::NoMatchingOverload,
                crate::FunctionBindingError::Control,
            )
        });
    }
    super::operator_kernel::comparison_recipe(OperatorFunction::Equal, [left, right], control)
        .map(PreparedNullIfEquality::Ordinary)
}

use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure, Selection,
    kernel_control::internal, kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, PrimitiveArray, StringArray,
    builder::BooleanBuilder,
    make_array,
    types::{
        ArrowPrimitiveType, Date32Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
        Int64Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    },
};
use arrow_buffer::NullBuffer;
use arrow_cast::{
    cast,
    display::{ArrayFormatter, FormatOptions},
};
use arrow_schema::{DataType, TimeUnit};
use std::{alloc::Layout, sync::Arc};

use crate::control_values::{AssemblyFailure, ConditionalTextExtent};
fn data_error(message: impl std::fmt::Display) -> KernelFailure {
    AssemblyFailure::contract(message).into_kernel()
}
fn reserve<T>(rows: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(output)
}
fn observe_bytes(text: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure> {
    for _ in text.as_bytes() {
        work.step()?;
    }
    Ok(())
}
/// V1 owns child demand. This entrypoint only casts already-evaluated inputs
/// with the original target inference and full, unbounded diagnostic strings.
pub fn evaluate_legacy(
    left: ArrayRef,
    right: ArrayRef,
    output_type: &DataType,
) -> Result<ArrayRef, KernelFailure> {
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    let result = (|| {
        let target = output_type.clone();
        let left = if left.data_type() != &target {
            work.flush()?;
            let value = cast(left.as_ref(), &target).map_err(|source| {
                AssemblyFailure::arrow(
                    format_args!(
                        "nullif failed to cast left {:?} -> {:?}",
                        left.data_type(),
                        target
                    ),
                    source,
                )
                .into_kernel()
            })?;
            work.flush()?;
            value
        } else {
            left
        };
        let right = if right.data_type() != &target {
            work.flush()?;
            let value = cast(right.as_ref(), &target).map_err(|source| {
                AssemblyFailure::arrow(
                    format_args!(
                        "nullif failed to cast right {:?} -> {:?}",
                        right.data_type(),
                        target
                    ),
                    source,
                )
                .into_kernel()
            })?;
            work.flush()?;
            value
        } else {
            right
        };
        evaluate_values(
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&right),
            Selection::all(left.len()),
            &mut work,
        )
    })();
    work.finish_result(result)
}

struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(internal("legacy nullif computation must not wait"))
    }
}

fn evaluate_values(
    left: EvaluatedArgument<'_>,
    right: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    match left.array().data_type() {
        DataType::Null => {
            work.flush()?;
            Ok(arrow_array::new_null_array(
                &DataType::Null,
                selection.len(),
            ))
        }
        DataType::Int8 => primitive::<Int8Type>(left, right, selection, work),
        DataType::Int16 => primitive::<Int16Type>(left, right, selection, work),
        DataType::Int32 => primitive::<Int32Type>(left, right, selection, work),
        DataType::Int64 => primitive::<Int64Type>(left, right, selection, work),
        DataType::Float32 => primitive::<Float32Type>(left, right, selection, work),
        DataType::Float64 => primitive::<Float64Type>(left, right, selection, work),
        DataType::Date32 => primitive::<Date32Type>(left, right, selection, work),
        DataType::Boolean => boolean(left, right, selection, work),
        DataType::Utf8 => string(left, right, selection, work),
        DataType::Decimal128(p, s) => decimal(left, right, *p, *s, selection, work),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => primitive::<TimestampSecondType>(left, right, selection, work),
            TimeUnit::Millisecond => {
                primitive::<TimestampMillisecondType>(left, right, selection, work)
            }
            TimeUnit::Microsecond => {
                primitive::<TimestampMicrosecondType>(left, right, selection, work)
            }
            TimeUnit::Nanosecond => {
                primitive::<TimestampNanosecondType>(left, right, selection, work)
            }
        },
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            generic_legacy(left.array(), right.array(), work)
        }
        other => Err(data_error(format_args!(
            "nullif unsupported type: {:?}",
            other
        ))),
    }
}
fn primitive<T: ArrowPrimitiveType>(
    left: EvaluatedArgument<'_>,
    right: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let l = left
        .array()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| data_error("nullif downcast failed"))?;
    let r = right
        .array()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| data_error("nullif downcast failed"))?;
    crate::selected_copy::guarded_interleave_extent(left.array().data_type(), selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut values = reserve::<Option<T::Native>>(selection.len(), work)?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let li = left.value_row(ordinal, row);
        let ri = right.value_row(ordinal, row);
        if l.is_null(li) {
            values.push(None);
            continue;
        }
        if r.is_null(ri) {
            values.push(Some(l.value(li)));
            continue;
        }
        values.push(if l.value(li) == r.value(ri) {
            None
        } else {
            Some(l.value(li))
        });
    }
    work.flush()?;
    // The concrete primitive family was checked above. Preserve its declared
    // timestamp timezone rather than dropping metadata during reconstruction.
    let output = Arc::new(
        PrimitiveArray::<T>::from_iter(values).with_data_type(left.array().data_type().clone()),
    ) as ArrayRef;
    work.flush()?;
    Ok(output)
}
fn boolean(
    left: EvaluatedArgument<'_>,
    right: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let l = left
        .array()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| data_error("nullif downcast failed"))?;
    let r = right
        .array()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| data_error("nullif downcast failed"))?;
    if matches!(
        (left, right),
        (EvaluatedArgument::Column(_), EvaluatedArgument::Column(_))
    ) && l.len() != r.len()
    {
        return Err(data_error("nullif boolean length mismatch"));
    }
    crate::selected_copy::guarded_interleave_extent(&DataType::Boolean, selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut builder = BooleanBuilder::with_capacity(selection.len());
    work.flush()?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let li = left.value_row(ordinal, row);
        let ri = right.value_row(ordinal, row);
        if l.is_null(li) {
            builder.append_null();
            continue;
        }
        if r.is_null(ri) {
            builder.append_value(l.value(li));
            continue;
        }
        if l.value(li) == r.value(ri) {
            builder.append_null();
        } else {
            builder.append_value(l.value(li));
        }
    }
    work.flush()?;
    let output = Arc::new(builder.finish()) as ArrayRef;
    work.flush()?;
    Ok(output)
}
fn string(
    left: EvaluatedArgument<'_>,
    right: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let l = left
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| data_error("nullif downcast failed"))?;
    let r = right
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| data_error("nullif downcast failed"))?;
    crate::selected_copy::guarded_interleave_extent(&DataType::Utf8, selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut out = reserve::<Option<String>>(selection.len(), work)?;
    let mut bytes = 0usize;
    let mut output_extent = ConditionalTextExtent::default();
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let li = left.value_row(ordinal, row);
        let ri = right.value_row(ordinal, row);
        if l.is_null(li) {
            out.push(None);
            continue;
        }
        let text = l.value(li);
        observe_bytes(text, work)?;
        let keep = if r.is_null(ri) {
            true
        } else {
            let other = r.value(ri);
            observe_bytes(other, work)?;
            work.flush()?;
            let unequal = text != other;
            work.flush()?;
            unequal
        };
        if !keep {
            out.push(None);
            continue;
        }
        output_extent.append(text.len())?;
        bytes = bytes
            .checked_add(text.len())
            .ok_or(KernelFailure::ResourceExhausted)?;
        Layout::array::<u8>(text.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let owned = text.to_string();
        work.flush()?;
        out.push(Some(owned));
    }
    crate::selected_copy::guarded_interleave_extent(&DataType::Utf8, selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let output = Arc::new(StringArray::from(out)) as ArrayRef;
    work.flush()?;
    Ok(output)
}
fn decimal(
    left: EvaluatedArgument<'_>,
    right: EvaluatedArgument<'_>,
    precision: u8,
    scale: i8,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let l = left
        .array()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| data_error("nullif downcast failed"))?;
    let r = right
        .array()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| data_error("nullif downcast failed"))?;
    crate::selected_copy::guarded_interleave_extent(left.array().data_type(), selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut values = reserve::<Option<i128>>(selection.len(), work)?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let li = left.value_row(ordinal, row);
        let ri = right.value_row(ordinal, row);
        if l.is_null(li) {
            values.push(None);
            continue;
        }
        if r.is_null(ri) {
            values.push(Some(l.value(li)));
            continue;
        }
        values.push(if l.value(li) == r.value(ri) {
            None
        } else {
            Some(l.value(li))
        });
    }
    work.flush()?;
    let output = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|source| AssemblyFailure::arrow("", source).into_kernel())?;
    work.flush()?;
    Ok(Arc::new(output))
}
/// Original display-based complex equality, used by full legacy inputs only.
/// Exact selected owners reject these carriers until their dedicated protocol.
fn generic_legacy(
    left: &ArrayRef,
    right: &ArrayRef,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let len = left.len();
    let fmt_opts = FormatOptions::default().with_null("\\N");
    work.flush()?;
    let left_fmt = ArrayFormatter::try_new(left.as_ref(), &fmt_opts)
        .map_err(|source| AssemblyFailure::arrow("nullif", source).into_kernel())?;
    let right_fmt = ArrayFormatter::try_new(right.as_ref(), &fmt_opts)
        .map_err(|source| AssemblyFailure::arrow("nullif", source).into_kernel())?;
    work.flush()?;
    let mut valid = reserve::<bool>(len, work)?;
    work.flush()?;
    valid.resize(len, true);
    work.flush()?;
    for (i, valid) in valid.iter_mut().enumerate().take(len) {
        work.step()?;
        if left.is_null(i) {
            *valid = false;
        } else if !right.is_null(i) {
            work.flush()?;
            let l_str = left_fmt.value(i).to_string();
            let r_str = right_fmt.value(i).to_string();
            work.flush()?;
            observe_bytes(&l_str, work)?;
            observe_bytes(&r_str, work)?;
            work.flush()?;
            if l_str == r_str {
                *valid = false;
            }
            work.flush()?;
        }
    }
    work.flush()?;
    let null_buffer = NullBuffer::from(valid);
    let data = left.to_data();
    let new_data = data
        .into_builder()
        .null_bit_buffer(Some(null_buffer.inner().inner().clone()))
        .build()
        .map_err(|source| AssemblyFailure::arrow("nullif", source).into_kernel())?;
    let output = make_array(new_data);
    work.flush()?;
    Ok(output)
}

#[cfg(test)]
mod tests;
