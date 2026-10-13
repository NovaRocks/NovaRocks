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

//! The original recursive aggregate scalar reader and output builder, shared
//! by legacy shells and pure owners through their exact state allocator.
use crate::kernel_input::EvaluationCheckpoints;
use crate::largeint;
use crate::{KernelDiagnostic, KernelFailure};
use allocator_api2::{alloc::Allocator, vec::Vec as ScalarVec};
use arrow_array::builder::*;
use arrow_array::*;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, i256};
use arrow_schema::{DataType, TimeUnit};
use std::sync::Arc;

pub trait ScalarStateAllocator:
    Allocator + Clone + std::fmt::Debug + Send + Sync + 'static
{
    fn scalar_allocation_error(&self, operation: &str) -> ScalarStateError;
}
#[derive(Debug)]
pub enum ScalarStateError {
    Legacy(String),
    OutputAllocation(String),
    Kernel(KernelFailure),
}
impl From<String> for ScalarStateError {
    fn from(error: String) -> Self {
        Self::Legacy(error)
    }
}
impl From<KernelFailure> for ScalarStateError {
    fn from(error: KernelFailure) -> Self {
        Self::Kernel(error)
    }
}
impl std::fmt::Display for ScalarStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(message) | Self::OutputAllocation(message) => f.write_str(message),
            Self::Kernel(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ScalarStateError {}
impl ScalarStateError {
    pub fn into_kernel_failure(self) -> KernelFailure {
        match self {
            Self::Kernel(error) => error,
            Self::OutputAllocation(_) => KernelFailure::ResourceExhausted,
            Self::Legacy(message) => KernelFailure::Operational(KernelDiagnostic::new(&message)),
        }
    }
}
pub struct ScalarWork<'control, 'scope> {
    work: Option<&'scope mut EvaluationCheckpoints<'control>>,
}
impl<'control, 'scope> ScalarWork<'control, 'scope> {
    pub fn new(work: Option<&'scope mut EvaluationCheckpoints<'control>>) -> Self {
        Self { work }
    }
    pub fn step(&mut self) -> Result<(), ScalarStateError> {
        if let Some(work) = &mut self.work {
            work.step()?;
        }
        Ok(())
    }
    pub fn flush(&mut self) -> Result<(), ScalarStateError> {
        if let Some(work) = &mut self.work {
            work.flush()?;
        }
        Ok(())
    }
}
pub fn scalar_bytes<A: ScalarStateAllocator>(
    allocator: A,
    bytes: &[u8],
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<u8, A>, ScalarStateError> {
    work.flush()?;
    let mut value = ScalarVec::new_in(allocator);
    value.try_reserve_exact(bytes.len()).map_err(|_| {
        value
            .allocator()
            .scalar_allocation_error("reserve aggregate byte value")
    })?;
    work.flush()?;
    for byte in bytes {
        value.push(*byte);
        work.step()?;
    }
    Ok(value)
}
#[derive(Clone)]
pub enum AggScalarValue {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
    Decimal256(i256),
    Binary(Vec<u8>),
    Struct(Vec<Option<AggScalarValue>>),
    Map(Vec<(Option<AggScalarValue>, Option<AggScalarValue>)>),
    List(Vec<Option<AggScalarValue>>),
}

impl std::fmt::Debug for AggScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        crate::aggregate_scalar_debug::fmt_scalar(self, f)
    }
}

/// Aggregate-state-owned scalar whose complete recursive heap graph uses the
/// query's exact aggregate allocator. Arrow output materialization converts
/// this value back to `AggScalarValue`; the tracked form never escapes the
/// aggregate state.
#[derive(Debug)]
pub enum TrackedAggScalarValue<A: ScalarStateAllocator> {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(ScalarVec<u8, A>),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
    Decimal256(i256),
    Binary(ScalarVec<u8, A>),
    Struct(ScalarVec<Option<TrackedAggScalarValue<A>>, A>),
    Map(
        ScalarVec<
            (
                Option<TrackedAggScalarValue<A>>,
                Option<TrackedAggScalarValue<A>>,
            ),
            A,
        >,
    ),
    List(ScalarVec<Option<TrackedAggScalarValue<A>>, A>),
}

pub fn aggregate_vec_with_capacity<T, A: ScalarStateAllocator>(
    allocator: &A,
    capacity: usize,
    operation: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<T, A>, ScalarStateError> {
    work.flush()?;
    let mut values = ScalarVec::new_in(allocator.clone());
    values
        .try_reserve_exact(capacity)
        .map_err(|_| values.allocator().scalar_allocation_error(operation))?;
    work.flush()?;
    Ok(values)
}

pub fn tracked_scalar_from_array<A: ScalarStateAllocator>(
    array: &ArrayRef,
    row: usize,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<TrackedAggScalarValue<A>>, ScalarStateError> {
    work.step()?;
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Null => return Ok(None),
        DataType::Boolean => TrackedAggScalarValue::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?
                .value(row),
        ),
        DataType::Int8 => TrackedAggScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?
                .value(row) as i64,
        ),
        DataType::Int16 => TrackedAggScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?
                .value(row) as i64,
        ),
        DataType::Int32 => TrackedAggScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?
                .value(row) as i64,
        ),
        DataType::Int64 => TrackedAggScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?
                .value(row),
        ),
        DataType::Float32 => TrackedAggScalarValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?
                .value(row) as f64,
        ),
        DataType::Float64 => TrackedAggScalarValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?
                .value(row),
        ),
        DataType::Utf8 => {
            let value = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?
                .value(row);
            TrackedAggScalarValue::Utf8(scalar_bytes(allocator.clone(), value.as_bytes(), work)?)
        }
        DataType::Binary => {
            let value = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?
                .value(row);
            TrackedAggScalarValue::Binary(scalar_bytes(allocator.clone(), value, work)?)
        }
        DataType::LargeBinary => {
            let value = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?
                .value(row);
            TrackedAggScalarValue::Binary(scalar_bytes(allocator.clone(), value, work)?)
        }
        DataType::Date32 => TrackedAggScalarValue::Date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?
                .value(row),
        ),
        DataType::Timestamp(unit, _) => {
            let value = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?
                    .value(row),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampMillisecondArray".to_string())?
                    .value(row),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampMicrosecondArray".to_string())?
                    .value(row),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampNanosecondArray".to_string())?
                    .value(row),
            };
            TrackedAggScalarValue::Timestamp(value)
        }
        DataType::Decimal128(_, _) => TrackedAggScalarValue::Decimal128(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?
                .value(row),
        ),
        DataType::Decimal256(_, _) => TrackedAggScalarValue::Decimal256(
            array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?
                .value(row),
        ),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
            TrackedAggScalarValue::Decimal128(largeint::value_at(array, row)?)
        }
        DataType::List(_) => {
            let array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "failed to downcast to ListArray".to_string())?;
            let offsets = array.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let mut values = aggregate_vec_with_capacity(
                allocator,
                end.saturating_sub(start),
                "reserve aggregate list scalar",
                work,
            )?;
            for index in start..end {
                work.step()?;
                values.push(tracked_scalar_from_array(
                    array.values(),
                    index,
                    allocator,
                    work,
                )?);
            }
            TrackedAggScalarValue::List(values)
        }
        DataType::Struct(fields) => {
            let array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
            let mut values = aggregate_vec_with_capacity(
                allocator,
                fields.len(),
                "reserve aggregate struct scalar",
                work,
            )?;
            for column in array.columns() {
                work.step()?;
                values.push(tracked_scalar_from_array(column, row, allocator, work)?);
            }
            TrackedAggScalarValue::Struct(values)
        }
        DataType::Map(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "failed to downcast to MapArray".to_string())?;
            let offsets = array.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let mut entries = aggregate_vec_with_capacity(
                allocator,
                end.saturating_sub(start),
                "reserve aggregate map scalar",
                work,
            )?;
            for index in start..end {
                work.step()?;
                entries.push((
                    tracked_scalar_from_array(array.keys(), index, allocator, work)?,
                    tracked_scalar_from_array(array.values(), index, allocator, work)?,
                ));
            }
            TrackedAggScalarValue::Map(entries)
        }
        other => return Err(format!("unsupported tracked scalar type: {other:?}").into()),
    };
    Ok(Some(value))
}

fn try_copy_bytes(
    bytes: &[u8],
    operation: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<u8>, ScalarStateError> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(bytes.len()).map_err(|_| {
        ScalarStateError::OutputAllocation(format!("ResourceExhausted: {operation}"))
    })?;
    for byte in bytes {
        copy.push(*byte);
        work.step()?;
    }
    Ok(copy)
}

pub fn tracked_scalar_to_output<A: ScalarStateAllocator>(
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<AggScalarValue, ScalarStateError> {
    work.step()?;
    Ok(match value {
        TrackedAggScalarValue::Bool(value) => AggScalarValue::Bool(*value),
        TrackedAggScalarValue::Int64(value) => AggScalarValue::Int64(*value),
        TrackedAggScalarValue::Float64(value) => AggScalarValue::Float64(*value),
        TrackedAggScalarValue::Utf8(value) => AggScalarValue::Utf8(
            String::from_utf8(try_copy_bytes(value, "copy aggregate UTF-8 output", work)?)
                .map_err(|error| error.to_string())?,
        ),
        TrackedAggScalarValue::Date32(value) => AggScalarValue::Date32(*value),
        TrackedAggScalarValue::Timestamp(value) => AggScalarValue::Timestamp(*value),
        TrackedAggScalarValue::Decimal128(value) => AggScalarValue::Decimal128(*value),
        TrackedAggScalarValue::Decimal256(value) => AggScalarValue::Decimal256(*value),
        TrackedAggScalarValue::Binary(value) => {
            AggScalarValue::Binary(try_copy_bytes(value, "copy aggregate binary output", work)?)
        }
        TrackedAggScalarValue::Struct(values) => {
            let mut output = Vec::new();
            output.try_reserve_exact(values.len()).map_err(|_| {
                ScalarStateError::OutputAllocation(
                    "ResourceExhausted: copy aggregate struct output".into(),
                )
            })?;
            for value in values {
                work.step()?;
                output.push(
                    value
                        .as_ref()
                        .map(|value| tracked_scalar_to_output(value, work))
                        .transpose()?,
                );
            }
            AggScalarValue::Struct(output)
        }
        TrackedAggScalarValue::Map(entries) => {
            let mut output = Vec::new();
            output.try_reserve_exact(entries.len()).map_err(|_| {
                ScalarStateError::OutputAllocation(
                    "ResourceExhausted: copy aggregate map output".into(),
                )
            })?;
            for (key, value) in entries {
                work.step()?;
                output.push((
                    key.as_ref()
                        .map(|value| tracked_scalar_to_output(value, work))
                        .transpose()?,
                    value
                        .as_ref()
                        .map(|value| tracked_scalar_to_output(value, work))
                        .transpose()?,
                ));
            }
            AggScalarValue::Map(output)
        }
        TrackedAggScalarValue::List(values) => {
            let mut output = Vec::new();
            output.try_reserve_exact(values.len()).map_err(|_| {
                ScalarStateError::OutputAllocation(
                    "ResourceExhausted: copy aggregate list output".into(),
                )
            })?;
            for value in values {
                work.step()?;
                output.push(
                    value
                        .as_ref()
                        .map(|value| tracked_scalar_to_output(value, work))
                        .transpose()?,
                );
            }
            AggScalarValue::List(output)
        }
    })
}

pub fn build_scalar_array(
    output_type: &DataType,
    values: Vec<Option<AggScalarValue>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, ScalarStateError> {
    build_scalar_array_impl(output_type, values, work, false)
}

/// Explicit selected callers observe the original nested LARGEINT builder.
/// Evaluation control is required; this interface grants no output memory.
pub fn build_scalar_array_with_largeint_observation(
    output_type: &DataType,
    values: Vec<Option<AggScalarValue>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, ScalarStateError> {
    if work.work.is_none() {
        return Err(crate::kernel_control::invalid(
            "observed LARGEINT scalar builder requires evaluation control",
        )
        .into());
    }
    build_scalar_array_impl(output_type, values, work, true)
}

fn build_scalar_array_impl(
    output_type: &DataType,
    values: Vec<Option<AggScalarValue>>,
    work: &mut ScalarWork<'_, '_>,
    observe_largeint: bool,
) -> Result<ArrayRef, ScalarStateError> {
    match output_type {
        DataType::Null => Ok(new_null_array(output_type, values.len())),
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Bool(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Boolean".to_string().into()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => {
            let mut builder = Int8Builder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Int64(v)) => {
                        let v = i8::try_from(v).map_err(|_| "int8 overflow".to_string())?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Int8".to_string().into()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Int64(v)) => {
                        let v = i16::try_from(v).map_err(|_| "int16 overflow".to_string())?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Int16".to_string().into()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Int64(v)) => {
                        let v = i32::try_from(v).map_err(|_| "int32 overflow".to_string())?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Int32".to_string().into()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Int64(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Int64".to_string().into()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Float64(v)) => builder.append_value(v as f32),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Float32".to_string().into()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Float64(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Float64".to_string().into()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Utf8(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Utf8".to_string().into()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Binary(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Binary".to_string().into()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeBinary => {
            let mut builder = LargeBinaryBuilder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Binary(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => {
                        return Err("scalar output type mismatch for LargeBinary"
                            .to_string()
                            .into());
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Date32(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Date32".to_string().into()),
                }
            }
            Ok(Arc::new(Date32Array::from(out)))
        }
        DataType::Timestamp(unit, tz) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Timestamp(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => {
                        return Err("scalar output type mismatch for Timestamp"
                            .to_string()
                            .into());
                    }
                }
            }
            let tz = tz.as_deref().map(|s| s.to_string());
            let array: ArrayRef = match unit {
                TimeUnit::Second => {
                    let array = TimestampSecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(array.with_timezone(tz))
                    } else {
                        Arc::new(array)
                    }
                }
                TimeUnit::Millisecond => {
                    let array = TimestampMillisecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(array.with_timezone(tz))
                    } else {
                        Arc::new(array)
                    }
                }
                TimeUnit::Microsecond => {
                    let array = TimestampMicrosecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(array.with_timezone(tz))
                    } else {
                        Arc::new(array)
                    }
                }
                TimeUnit::Nanosecond => {
                    let array = TimestampNanosecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(array.with_timezone(tz))
                    } else {
                        Arc::new(array)
                    }
                }
            };
            Ok(array)
        }
        DataType::Decimal128(precision, scale) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Decimal128(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => {
                        return Err("scalar output type mismatch for Decimal128"
                            .to_string()
                            .into());
                    }
                }
            }
            let array = Decimal128Array::from(out)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(array))
        }
        DataType::Decimal256(precision, scale) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Decimal256(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => {
                        return Err("scalar output type mismatch for Decimal256"
                            .to_string()
                            .into());
                    }
                }
            }
            let array = Decimal256Array::from(out)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(array))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Decimal128(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => {
                        return Err("scalar output type mismatch for LargeInt"
                            .to_string()
                            .into());
                    }
                }
            }
            if !observe_largeint {
                return largeint::array_from_i128(&out).map_err(ScalarStateError::from);
            }
            let result = largeint::array_from_i128_observed(&out, &mut |event| match event {
                largeint::LargeIntObservation::Step => work.step(),
                largeint::LargeIntObservation::OpaqueBoundary => work.flush(),
            })?;
            result.map_err(ScalarStateError::from)
        }
        DataType::List(item) => {
            let mut flat_values = Vec::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut current: i64 = 0;
            let mut nulls = NullBufferBuilder::new(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::List(items)) => {
                        current += i64::try_from(items.len())
                            .map_err(|_| "list item length overflow".to_string())?;
                        if current > i32::MAX as i64 {
                            return Err("list offset overflow".to_string().into());
                        }
                        // Count the owned inputs before the original Vec
                        // extension; its allocation/copy remains one library call.
                        for _ in &items {
                            work.step()?;
                        }
                        work.flush()?;
                        flat_values.extend(items);
                        work.flush()?;
                        offsets.push(current as i32);
                        nulls.append_non_null();
                    }
                    None => {
                        offsets.push(current as i32);
                        nulls.append_null();
                    }
                    _ => return Err("scalar output type mismatch for List".to_string().into()),
                }
            }
            let child =
                build_scalar_array_impl(item.data_type(), flat_values, work, observe_largeint)?;
            let out = ListArray::try_new(
                item.clone(),
                OffsetBuffer::new(offsets.into()),
                child,
                nulls.finish(),
            )
            .map_err(|e| format!("list output build failed: {}", e))?;
            Ok(Arc::new(out))
        }
        DataType::Struct(fields) => {
            let mut field_values: Vec<Vec<Option<AggScalarValue>>> =
                vec![Vec::with_capacity(values.len()); fields.len()];
            let mut nulls = NullBufferBuilder::new(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Struct(items)) => {
                        if items.len() != fields.len() {
                            return Err(format!(
                                "scalar output struct field count mismatch: expected {} got {}",
                                fields.len(),
                                items.len()
                            )
                            .into());
                        }
                        nulls.append_non_null();
                        for (idx, item) in items.into_iter().enumerate() {
                            work.step()?;
                            field_values[idx].push(item);
                        }
                    }
                    None => {
                        nulls.append_null();
                        for values in field_values.iter_mut() {
                            work.step()?;
                            values.push(None);
                        }
                    }
                    _ => return Err("scalar output type mismatch for Struct".to_string().into()),
                }
            }
            let mut columns = Vec::with_capacity(fields.len());
            for (field, values) in fields.iter().zip(field_values.into_iter()) {
                work.step()?;
                columns.push(build_scalar_array_impl(
                    field.data_type(),
                    values,
                    work,
                    observe_largeint,
                )?);
            }
            Ok(Arc::new(StructArray::new(
                fields.clone(),
                columns,
                nulls.finish(),
            )))
        }
        DataType::Map(field, ordered) => {
            let DataType::Struct(entry_fields) = field.data_type() else {
                return Err("scalar output MAP entries type must be STRUCT"
                    .to_string()
                    .into());
            };
            if entry_fields.len() != 2 {
                return Err("scalar output MAP entries must have 2 fields"
                    .to_string()
                    .into());
            }
            let mut key_values = Vec::<Option<AggScalarValue>>::new();
            let mut value_values = Vec::<Option<AggScalarValue>>::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut current: i64 = 0;
            let mut nulls = NullBufferBuilder::new(values.len());

            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Map(items)) => {
                        nulls.append_non_null();
                        for (k, v) in items {
                            work.step()?;
                            key_values.push(k);
                            value_values.push(v);
                            current += 1;
                            if current > i32::MAX as i64 {
                                return Err("map offset overflow".to_string().into());
                            }
                        }
                        offsets.push(current as i32);
                    }
                    None => {
                        nulls.append_null();
                        offsets.push(current as i32);
                    }
                    _ => return Err("scalar output type mismatch for Map".to_string().into()),
                }
            }

            let keys = build_scalar_array_impl(
                entry_fields[0].data_type(),
                key_values,
                work,
                observe_largeint,
            )?;
            let values = build_scalar_array_impl(
                entry_fields[1].data_type(),
                value_values,
                work,
                observe_largeint,
            )?;
            let entries = StructArray::new(entry_fields.clone(), vec![keys, values], None);
            let out = MapArray::try_new(
                field.clone(),
                OffsetBuffer::new(offsets.into()),
                entries,
                nulls.finish(),
                *ordered,
            )
            .map_err(|e| format!("map output build failed: {}", e))?;
            Ok(Arc::new(out))
        }
        other => Err(format!("unsupported scalar output type: {:?}", other).into()),
    }
}

pub fn compare_tracked_scalar_values<A: ScalarStateAllocator>(
    left: &TrackedAggScalarValue<A>,
    right: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<std::cmp::Ordering, ScalarStateError> {
    work.step()?;
    match (left, right) {
        (TrackedAggScalarValue::Bool(left), TrackedAggScalarValue::Bool(right)) => {
            Ok(left.cmp(right))
        }
        (TrackedAggScalarValue::Int64(left), TrackedAggScalarValue::Int64(right)) => {
            Ok(left.cmp(right))
        }
        (TrackedAggScalarValue::Float64(left), TrackedAggScalarValue::Float64(right)) => left
            .partial_cmp(right)
            .ok_or_else(|| "float comparison is not ordered".to_string().into()),
        (TrackedAggScalarValue::Utf8(left), TrackedAggScalarValue::Utf8(right))
        | (TrackedAggScalarValue::Binary(left), TrackedAggScalarValue::Binary(right)) => {
            for _ in left.iter().zip(right) {
                work.step()?;
            }
            work.flush()?;
            let ordering = left.as_slice().cmp(right.as_slice());
            work.flush()?;
            Ok(ordering)
        }
        (TrackedAggScalarValue::Date32(left), TrackedAggScalarValue::Date32(right)) => {
            Ok(left.cmp(right))
        }
        (TrackedAggScalarValue::Timestamp(left), TrackedAggScalarValue::Timestamp(right)) => {
            Ok(left.cmp(right))
        }
        (TrackedAggScalarValue::Decimal128(left), TrackedAggScalarValue::Decimal128(right)) => {
            Ok(left.cmp(right))
        }
        (TrackedAggScalarValue::Decimal256(left), TrackedAggScalarValue::Decimal256(right)) => {
            Ok(left.cmp(right))
        }
        (TrackedAggScalarValue::Struct(left), TrackedAggScalarValue::Struct(right))
        | (TrackedAggScalarValue::List(left), TrackedAggScalarValue::List(right)) => {
            compare_tracked_optional_slices(left, right, work)
        }
        (TrackedAggScalarValue::Map(left), TrackedAggScalarValue::Map(right)) => {
            for ((left_key, left_value), (right_key, right_value)) in left.iter().zip(right) {
                work.step()?;
                let ordering = compare_tracked_optional_values(left_key, right_key, work)?;
                if !ordering.is_eq() {
                    return Ok(ordering);
                }
                let ordering = compare_tracked_optional_values(left_value, right_value, work)?;
                if !ordering.is_eq() {
                    return Ok(ordering);
                }
            }
            Ok(left.len().cmp(&right.len()))
        }
        _ => Err("tracked scalar comparison type mismatch".to_string().into()),
    }
}

fn compare_tracked_optional_slices<A: ScalarStateAllocator>(
    left: &[Option<TrackedAggScalarValue<A>>],
    right: &[Option<TrackedAggScalarValue<A>>],
    work: &mut ScalarWork<'_, '_>,
) -> Result<std::cmp::Ordering, ScalarStateError> {
    work.step()?;
    for (left, right) in left.iter().zip(right) {
        work.step()?;
        let ordering = compare_tracked_optional_values(left, right, work)?;
        if !ordering.is_eq() {
            return Ok(ordering);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

fn compare_tracked_optional_values<A: ScalarStateAllocator>(
    left: &Option<TrackedAggScalarValue<A>>,
    right: &Option<TrackedAggScalarValue<A>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<std::cmp::Ordering, ScalarStateError> {
    work.step()?;
    match (left, right) {
        (None, None) => Ok(std::cmp::Ordering::Equal),
        (None, Some(_)) => Ok(std::cmp::Ordering::Less),
        (Some(_), None) => Ok(std::cmp::Ordering::Greater),
        (Some(left), Some(right)) => compare_tracked_scalar_values(left, right, work),
    }
}

pub fn tracked_scalar_heap_capacity<A: ScalarStateAllocator>(
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<usize, ScalarStateError> {
    work.step()?;
    let mut bytes = match value {
        TrackedAggScalarValue::Utf8(values) | TrackedAggScalarValue::Binary(values) => {
            values.capacity()
        }
        TrackedAggScalarValue::Struct(values) | TrackedAggScalarValue::List(values) => values
            .capacity()
            .checked_mul(std::mem::size_of::<Option<TrackedAggScalarValue<A>>>())
            .ok_or(crate::KernelFailure::ResourceExhausted)?,
        TrackedAggScalarValue::Map(values) => values
            .capacity()
            .checked_mul(std::mem::size_of::<(
                Option<TrackedAggScalarValue<A>>,
                Option<TrackedAggScalarValue<A>>,
            )>())
            .ok_or(crate::KernelFailure::ResourceExhausted)?,
        _ => 0,
    };
    match value {
        TrackedAggScalarValue::Struct(values) | TrackedAggScalarValue::List(values) => {
            for value in values {
                work.step()?;
                if let Some(value) = value {
                    bytes = bytes
                        .checked_add(tracked_scalar_heap_capacity(value, work)?)
                        .ok_or(crate::KernelFailure::ResourceExhausted)?;
                }
            }
        }
        TrackedAggScalarValue::Map(values) => {
            for (key, value) in values {
                work.step()?;
                for value in [key, value].into_iter().flatten() {
                    bytes = bytes
                        .checked_add(tracked_scalar_heap_capacity(value, work)?)
                        .ok_or(crate::KernelFailure::ResourceExhausted)?;
                }
            }
        }
        _ => {}
    }
    Ok(bytes)
}

/// The original owned scalar reader. Legacy callers keep their original
/// temporary materialization; selected callers borrow the same work scope.
/// Original untracked reader failures borrow the actual source carrier.
pub enum ScalarReadFailure<'a> {
    Static(&'static str),
    Unsupported(&'a DataType),
    Existing(&'a str),
}
impl std::fmt::Display for ScalarReadFailure<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(message) => f.write_str(message),
            Self::Existing(message) => f.write_str(message),
            Self::Unsupported(ty) => write!(f, "unsupported scalar type: {:?}", ty),
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum ScalarReadObservation {
    Step,
    OpaqueBoundary,
}
/// Requests name the actual original std allocation that follows, not a
/// placeholder allocation. A pure host admits it before that operation.
pub trait ScalarReadFailureSink {
    type Error;
    fn read_data(&mut self, failure: ScalarReadFailure<'_>) -> Self::Error;
    fn observe(&mut self, observation: ScalarReadObservation) -> Result<(), Self::Error>;
    fn reserve_scalar_copy(
        &mut self,
        elements: usize,
        width: usize,
        alignment: usize,
    ) -> Result<(), Self::Error>;
}
struct LegacyScalarReadSink<'borrow, 'control, 'scope> {
    work: &'borrow mut ScalarWork<'control, 'scope>,
}
impl ScalarReadFailureSink for LegacyScalarReadSink<'_, '_, '_> {
    type Error = ScalarStateError;
    fn read_data(&mut self, failure: ScalarReadFailure<'_>) -> ScalarStateError {
        ScalarStateError::Legacy(failure.to_string())
    }
    fn observe(&mut self, event: ScalarReadObservation) -> Result<(), ScalarStateError> {
        match event {
            ScalarReadObservation::Step => self.work.step(),
            ScalarReadObservation::OpaqueBoundary => self.work.flush(),
        }
    }
    fn reserve_scalar_copy(
        &mut self,
        _: usize,
        _: usize,
        _: usize,
    ) -> Result<(), ScalarStateError> {
        Ok(())
    }
}
pub fn scalar_from_array(
    array: &ArrayRef,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<AggScalarValue>, ScalarStateError> {
    scalar_from_array_with_failure(array, row, &mut LegacyScalarReadSink { work })
}
pub fn scalar_from_array_with_failure<F: ScalarReadFailureSink>(
    array: &ArrayRef,
    row: usize,
    sink: &mut F,
) -> Result<Option<AggScalarValue>, F::Error> {
    sink.observe(ScalarReadObservation::Step)?;
    match array.data_type() {
        DataType::Null => Ok(None),
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to BooleanArray",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Bool(arr.value(row))))
            }
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                sink.read_data(ScalarReadFailure::Static("failed to downcast to Int8Array"))
            })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row) as i64)))
            }
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                sink.read_data(ScalarReadFailure::Static(
                    "failed to downcast to Int16Array",
                ))
            })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row) as i64)))
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                sink.read_data(ScalarReadFailure::Static(
                    "failed to downcast to Int32Array",
                ))
            })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row) as i64)))
            }
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                sink.read_data(ScalarReadFailure::Static(
                    "failed to downcast to Int64Array",
                ))
            })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row))))
            }
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to Float32Array",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Float64(arr.value(row) as f64)))
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to Float64Array",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Float64(arr.value(row))))
            }
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to StringArray",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                for _ in arr.value(row).as_bytes() {
                    sink.observe(ScalarReadObservation::Step)?;
                }
                sink.observe(ScalarReadObservation::OpaqueBoundary)?;
                sink.reserve_scalar_copy(arr.value(row).len(), 1, 1)?;
                let value = arr.value(row).to_string();
                sink.observe(ScalarReadObservation::OpaqueBoundary)?;
                Ok(Some(AggScalarValue::Utf8(value)))
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to BinaryArray",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                for _ in arr.value(row) {
                    sink.observe(ScalarReadObservation::Step)?;
                }
                sink.observe(ScalarReadObservation::OpaqueBoundary)?;
                sink.reserve_scalar_copy(arr.value(row).len(), 1, 1)?;
                let value = arr.value(row).to_vec();
                sink.observe(ScalarReadObservation::OpaqueBoundary)?;
                Ok(Some(AggScalarValue::Binary(value)))
            }
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to LargeBinaryArray",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                for _ in arr.value(row) {
                    sink.observe(ScalarReadObservation::Step)?;
                }
                sink.observe(ScalarReadObservation::OpaqueBoundary)?;
                sink.reserve_scalar_copy(arr.value(row).len(), 1, 1)?;
                let value = arr.value(row).to_vec();
                sink.observe(ScalarReadObservation::OpaqueBoundary)?;
                Ok(Some(AggScalarValue::Binary(value)))
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to Date32Array",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Date32(arr.value(row))))
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        sink.read_data(ScalarReadFailure::Static(
                            "failed to downcast to TimestampSecondArray",
                        ))
                    })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
            TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        sink.read_data(ScalarReadFailure::Static(
                            "failed to downcast to TimestampMillisecondArray",
                        ))
                    })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
            TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        sink.read_data(ScalarReadFailure::Static(
                            "failed to downcast to TimestampMicrosecondArray",
                        ))
                    })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
            TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        sink.read_data(ScalarReadFailure::Static(
                            "failed to downcast to TimestampNanosecondArray",
                        ))
                    })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
        },
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to Decimal128Array",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Decimal128(arr.value(row))))
            }
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to Decimal256Array",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Decimal256(arr.value(row))))
            }
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to FixedSizeBinaryArray",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                let v = largeint::value_at(arr, row)
                    .map_err(|message| sink.read_data(ScalarReadFailure::Existing(&message)))?;
                Ok(Some(AggScalarValue::Decimal128(v)))
            }
        }
        DataType::List(_item) => {
            let arr = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                sink.read_data(ScalarReadFailure::Static("failed to downcast to ListArray"))
            })?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let offsets = arr.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = arr.values();
            sink.reserve_scalar_copy(
                end.saturating_sub(start),
                std::mem::size_of::<Option<AggScalarValue>>(),
                std::mem::align_of::<Option<AggScalarValue>>(),
            )?;
            let mut out = Vec::with_capacity(end.saturating_sub(start));
            for idx in start..end {
                sink.observe(ScalarReadObservation::Step)?;
                out.push(scalar_from_array_with_failure(values, idx, sink)?);
            }
            Ok(Some(AggScalarValue::List(out)))
        }
        DataType::Struct(fields) => {
            let arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    sink.read_data(ScalarReadFailure::Static(
                        "failed to downcast to StructArray",
                    ))
                })?;
            if arr.is_null(row) {
                return Ok(None);
            }
            sink.reserve_scalar_copy(
                fields.len(),
                std::mem::size_of::<Option<AggScalarValue>>(),
                std::mem::align_of::<Option<AggScalarValue>>(),
            )?;
            let mut out = Vec::with_capacity(fields.len());
            for col in arr.columns() {
                sink.observe(ScalarReadObservation::Step)?;
                out.push(scalar_from_array_with_failure(col, row, sink)?);
            }
            Ok(Some(AggScalarValue::Struct(out)))
        }
        DataType::Map(_, _) => {
            let arr = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                sink.read_data(ScalarReadFailure::Static("failed to downcast to MapArray"))
            })?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let offsets = arr.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let keys = arr.keys();
            let values = arr.values();
            sink.reserve_scalar_copy(
                end.saturating_sub(start),
                std::mem::size_of::<(Option<AggScalarValue>, Option<AggScalarValue>)>(),
                std::mem::align_of::<(Option<AggScalarValue>, Option<AggScalarValue>)>(),
            )?;
            let mut out = Vec::with_capacity(end.saturating_sub(start));
            for idx in start..end {
                sink.observe(ScalarReadObservation::Step)?;
                out.push((
                    scalar_from_array_with_failure(keys, idx, sink)?,
                    scalar_from_array_with_failure(values, idx, sink)?,
                ));
            }
            Ok(Some(AggScalarValue::Map(out)))
        }
        other => Err(sink.read_data(ScalarReadFailure::Unsupported(other))),
    }
}

#[cfg(test)]
#[path = "aggregate_scalar/largeint_observation_tests.rs"]
mod largeint_observation_tests;
