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
#[cfg(test)]
use crate::KernelDiagnostic;
use crate::KernelFailure;
use crate::kernel_input::EvaluationCheckpoints;
use crate::largeint;
use allocator_api2::{alloc::Allocator, vec::Vec as ScalarVec};
use arrow_array::builder::*;
use arrow_array::*;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, i256};
use arrow_schema::{DataType, TimeUnit};
use std::sync::Arc;
#[path = "aggregate_scalar/source_failure.rs"]
mod source_failure;
use source_failure::{Construction, ContractKind, OffsetLayout, OutputTally};

pub trait ScalarStateAllocator:
    Allocator + Clone + std::fmt::Debug + Send + Sync + 'static
{
    fn scalar_allocation_error<E: crate::allocation_failure::AllocationRefusalSource>(
        &self,
        operation: &str,
        source: E,
    ) -> crate::KernelFailure;
}
pub struct ScalarWork<'control, 'scope> {
    work: Option<&'scope mut EvaluationCheckpoints<'control>>,
}
impl<'control, 'scope> ScalarWork<'control, 'scope> {
    pub fn new(work: Option<&'scope mut EvaluationCheckpoints<'control>>) -> Self {
        Self { work }
    }
    pub fn step(&mut self) -> Result<(), KernelFailure> {
        if let Some(work) = &mut self.work {
            work.step()?;
        }
        Ok(())
    }
    pub fn flush(&mut self) -> Result<(), KernelFailure> {
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
) -> Result<ScalarVec<u8, A>, KernelFailure> {
    work.flush()?;
    let mut value = ScalarVec::new_in(allocator);
    value.try_reserve_exact(bytes.len()).map_err(|source| {
        value
            .allocator()
            .scalar_allocation_error("reserve aggregate byte value", source)
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

/// One recursively tracked key/value pair in a map aggregate value.
#[expect(
    type_alias_bounds,
    reason = "The allocator bound is required to form the tracked value types in the alias"
)]
pub type TrackedAggScalarMapEntry<A: ScalarStateAllocator> = (
    Option<TrackedAggScalarValue<A>>,
    Option<TrackedAggScalarValue<A>>,
);

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
    Map(ScalarVec<TrackedAggScalarMapEntry<A>, A>),
    List(ScalarVec<Option<TrackedAggScalarValue<A>>, A>),
}

pub fn aggregate_vec_with_capacity<T, A: ScalarStateAllocator>(
    allocator: &A,
    capacity: usize,
    operation: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<T, A>, KernelFailure> {
    work.flush()?;
    let mut values = ScalarVec::new_in(allocator.clone());
    values.try_reserve_exact(capacity).map_err(|source| {
        values
            .allocator()
            .scalar_allocation_error(operation, source)
    })?;
    work.flush()?;
    Ok(values)
}

pub fn tracked_scalar_from_array<A: ScalarStateAllocator>(
    array: &ArrayRef,
    row: usize,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<TrackedAggScalarValue<A>>, KernelFailure> {
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
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to BooleanArray",
                    )
                })?
                .value(row),
        ),
        DataType::Int8 => TrackedAggScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to Int8Array",
                    )
                })?
                .value(row) as i64,
        ),
        DataType::Int16 => TrackedAggScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to Int16Array",
                    )
                })?
                .value(row) as i64,
        ),
        DataType::Int32 => TrackedAggScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to Int32Array",
                    )
                })?
                .value(row) as i64,
        ),
        DataType::Int64 => TrackedAggScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to Int64Array",
                    )
                })?
                .value(row),
        ),
        DataType::Float32 => TrackedAggScalarValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to Float32Array",
                    )
                })?
                .value(row) as f64,
        ),
        DataType::Float64 => TrackedAggScalarValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to Float64Array",
                    )
                })?
                .value(row),
        ),
        DataType::Utf8 => {
            let value = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to StringArray",
                    )
                })?
                .value(row);
            TrackedAggScalarValue::Utf8(scalar_bytes(allocator.clone(), value.as_bytes(), work)?)
        }
        DataType::Binary => {
            let value = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to BinaryArray",
                    )
                })?
                .value(row);
            TrackedAggScalarValue::Binary(scalar_bytes(allocator.clone(), value, work)?)
        }
        DataType::LargeBinary => {
            let value = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to LargeBinaryArray",
                    )
                })?
                .value(row);
            TrackedAggScalarValue::Binary(scalar_bytes(allocator.clone(), value, work)?)
        }
        DataType::Date32 => TrackedAggScalarValue::Date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to Date32Array",
                    )
                })?
                .value(row),
        ),
        DataType::Timestamp(unit, _) => {
            let value = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        source_failure::contract(
                            ContractKind::Downcast,
                            "failed to downcast to TimestampSecondArray",
                        )
                    })?
                    .value(row),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        source_failure::contract(
                            ContractKind::Downcast,
                            "failed to downcast to TimestampMillisecondArray",
                        )
                    })?
                    .value(row),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        source_failure::contract(
                            ContractKind::Downcast,
                            "failed to downcast to TimestampMicrosecondArray",
                        )
                    })?
                    .value(row),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        source_failure::contract(
                            ContractKind::Downcast,
                            "failed to downcast to TimestampNanosecondArray",
                        )
                    })?
                    .value(row),
            };
            TrackedAggScalarValue::Timestamp(value)
        }
        DataType::Decimal128(_, _) => TrackedAggScalarValue::Decimal128(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to Decimal128Array",
                    )
                })?
                .value(row),
        ),
        DataType::Decimal256(_, _) => TrackedAggScalarValue::Decimal256(
            array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to Decimal256Array",
                    )
                })?
                .value(row),
        ),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to FixedSizeBinaryArray",
                    )
                })?;
            TrackedAggScalarValue::Decimal128(largeint::value_at(array, row)?)
        }
        DataType::List(_) => {
            let array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                source_failure::contract(ContractKind::Downcast, "failed to downcast to ListArray")
            })?;
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
                .ok_or_else(|| {
                    source_failure::contract(
                        ContractKind::Downcast,
                        "failed to downcast to StructArray",
                    )
                })?;
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
            let array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                source_failure::contract(ContractKind::Downcast, "failed to downcast to MapArray")
            })?;
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
        other => {
            return Err(source_failure::contract(
                ContractKind::TrackedType,
                format_args!("unsupported tracked scalar type: {other:?}"),
            ));
        }
    };
    Ok(Some(value))
}

/// Preserve the actual standard container error. Its stable API exposes no
/// allocation-failure kind or exact allocator request, so those remain unknown.
fn reserve_output<T>(
    output: &mut Vec<T>,
    additional: usize,
    operation: &str,
) -> Result<(), KernelFailure> {
    output.try_reserve_exact(additional).map_err(|source| {
        KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
            operation, source,
        ))
    })
}

fn try_copy_bytes(
    bytes: &[u8],
    operation: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<u8>, KernelFailure> {
    let mut copy = Vec::new();
    reserve_output(&mut copy, bytes.len(), operation)?;
    for byte in bytes {
        copy.push(*byte);
        work.step()?;
    }
    Ok(copy)
}

pub fn tracked_scalar_to_output<A: ScalarStateAllocator>(
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<AggScalarValue, KernelFailure> {
    work.step()?;
    Ok(match value {
        TrackedAggScalarValue::Bool(value) => AggScalarValue::Bool(*value),
        TrackedAggScalarValue::Int64(value) => AggScalarValue::Int64(*value),
        TrackedAggScalarValue::Float64(value) => AggScalarValue::Float64(*value),
        TrackedAggScalarValue::Utf8(value) => AggScalarValue::Utf8(
            String::from_utf8(try_copy_bytes(value, "copy aggregate UTF-8 output", work)?)
                .map_err(source_failure::utf8)?,
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
            reserve_output(&mut output, values.len(), "copy aggregate struct output")?;
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
            reserve_output(&mut output, entries.len(), "copy aggregate map output")?;
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
            reserve_output(&mut output, values.len(), "copy aggregate list output")?;
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
) -> Result<ArrayRef, KernelFailure> {
    build_scalar_array_impl(output_type, values, work, false)
}

/// Explicit selected callers observe the original nested LARGEINT builder.
/// Evaluation control is required; this interface grants no output memory.
pub fn build_scalar_array_with_largeint_observation(
    output_type: &DataType,
    values: Vec<Option<AggScalarValue>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, KernelFailure> {
    if work.work.is_none() {
        return Err(crate::kernel_control::invalid(
            "observed LARGEINT scalar builder requires evaluation control",
        ));
    }
    build_scalar_array_impl(output_type, values, work, true)
}

fn build_scalar_array_impl(
    output_type: &DataType,
    values: Vec<Option<AggScalarValue>>,
    work: &mut ScalarWork<'_, '_>,
    observe_largeint: bool,
) -> Result<ArrayRef, KernelFailure> {
    match output_type {
        DataType::Null => Ok(new_null_array(output_type, values.len())),
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Bool(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Boolean",
                        ));
                    }
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
                        let v = i8::try_from(v)
                            .map_err(|source| source_failure::whole_narrow("int8", source))?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Int8",
                        ));
                    }
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
                        let v = i16::try_from(v)
                            .map_err(|source| source_failure::whole_narrow("int16", source))?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Int16",
                        ));
                    }
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
                        let v = i32::try_from(v)
                            .map_err(|source| source_failure::whole_narrow("int32", source))?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Int32",
                        ));
                    }
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
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Int64",
                        ));
                    }
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
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Float32",
                        ));
                    }
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
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Float64",
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            let mut extent = OutputTally::new(OffsetLayout::Utf8);
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Utf8(v)) => {
                        extent.append(v.len())?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Utf8",
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            let mut extent = OutputTally::new(OffsetLayout::Binary);
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Binary(v)) => {
                        extent.append(v.len())?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Binary",
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeBinary => {
            let mut builder = LargeBinaryBuilder::new();
            let mut extent = OutputTally::new(OffsetLayout::LargeBinary);
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Binary(v)) => {
                        extent.append(v.len())?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for LargeBinary",
                        ));
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
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Date32",
                        ));
                    }
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
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Timestamp",
                        ));
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
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Decimal128",
                        ));
                    }
                }
            }
            let array = Decimal128Array::from(out)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|source| source_failure::construction(Construction::Decimal128, source))?;
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
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Decimal256",
                        ));
                    }
                }
            }
            let array = Decimal256Array::from(out)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|source| source_failure::construction(Construction::Decimal256, source))?;
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
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for LargeInt",
                        ));
                    }
                }
            }
            if !observe_largeint {
                return largeint::array_from_i128(&out).map_err(KernelFailure::from_domain);
            }
            largeint::array_from_i128_observed(&out, &mut |event| match event {
                largeint::LargeIntObservation::Step => work.step(),
                largeint::LargeIntObservation::OpaqueBoundary => work.flush(),
            })
        }
        DataType::List(item) => {
            let mut flat_values = Vec::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut extent = OutputTally::new(OffsetLayout::List);
            let mut nulls = NullBufferBuilder::new(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::List(items)) => {
                        extent.append(items.len())?;
                        // Count the owned inputs before the original Vec
                        // extension; its allocation/copy remains one library call.
                        for _ in &items {
                            work.step()?;
                        }
                        work.flush()?;
                        flat_values.extend(items);
                        work.flush()?;
                        offsets.push(extent.total() as i32);
                        nulls.append_non_null();
                    }
                    None => {
                        offsets.push(extent.total() as i32);
                        nulls.append_null();
                    }
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for List",
                        ));
                    }
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
            .map_err(|source| source_failure::construction(Construction::List, source))?;
            Ok(Arc::new(out))
        }
        DataType::Struct(fields) => {
            let output_rows = values.len();
            let mut field_values: Vec<Vec<Option<AggScalarValue>>> =
                vec![Vec::with_capacity(values.len()); fields.len()];
            let mut nulls = NullBufferBuilder::new(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Struct(items)) => {
                        if items.len() != fields.len() {
                            return Err(source_failure::contract(
                                ContractKind::StructWidth,
                                format_args!(
                                    "scalar output struct field count mismatch: expected {} got {}",
                                    fields.len(),
                                    items.len(),
                                ),
                            ));
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
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Struct",
                        ));
                    }
                }
            }
            let mut columns = Vec::with_capacity(fields.len());
            for (field, values) in fields.iter().zip(field_values) {
                work.step()?;
                columns.push(build_scalar_array_impl(
                    field.data_type(),
                    values,
                    work,
                    observe_largeint,
                )?);
            }
            Ok(Arc::new(
                StructArray::try_new_with_length(
                    fields.clone(),
                    columns,
                    nulls.finish(),
                    output_rows,
                )
                .map_err(|source| source_failure::construction(Construction::Struct, source))?,
            ))
        }
        DataType::Map(field, ordered) => {
            let DataType::Struct(entry_fields) = field.data_type() else {
                return Err(source_failure::contract(
                    ContractKind::MapEntries,
                    "scalar output MAP entries type must be STRUCT",
                ));
            };
            if entry_fields.len() != 2 {
                return Err(source_failure::contract(
                    ContractKind::MapEntries,
                    "scalar output MAP entries must have 2 fields",
                ));
            }
            let mut key_values = Vec::<Option<AggScalarValue>>::new();
            let mut value_values = Vec::<Option<AggScalarValue>>::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut extent = OutputTally::new(OffsetLayout::Map);
            let mut nulls = NullBufferBuilder::new(values.len());

            for value in values {
                work.step()?;
                match value {
                    Some(AggScalarValue::Map(items)) => {
                        extent.append(items.len())?;
                        nulls.append_non_null();
                        for (k, v) in items {
                            work.step()?;
                            key_values.push(k);
                            value_values.push(v);
                        }
                        offsets.push(extent.total() as i32);
                    }
                    None => {
                        nulls.append_null();
                        offsets.push(extent.total() as i32);
                    }
                    _ => {
                        return Err(source_failure::contract(
                            ContractKind::ValueType,
                            "scalar output type mismatch for Map",
                        ));
                    }
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
            let entries = StructArray::try_new(entry_fields.clone(), vec![keys, values], None)
                .map_err(|source| source_failure::construction(Construction::MapEntries, source))?;
            let out = MapArray::try_new(
                field.clone(),
                OffsetBuffer::new(offsets.into()),
                entries,
                nulls.finish(),
                *ordered,
            )
            .map_err(|source| source_failure::construction(Construction::Map, source))?;
            Ok(Arc::new(out))
        }
        other => Err(source_failure::contract(
            ContractKind::OutputType,
            format_args!("unsupported scalar output type: {:?}", other),
        )),
    }
}

/// Partial comparison is a value result, never a failed control observation.
/// Consumers own their policy for NaN and heterogeneous serialized values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarOrdering {
    Ordered(std::cmp::Ordering),
    UnorderedFloat,
    MismatchedKinds,
}
pub fn compare_tracked_scalar_values<A: ScalarStateAllocator>(
    left: &TrackedAggScalarValue<A>,
    right: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<std::cmp::Ordering, KernelFailure> {
    strict_scalar_ordering(
        compare_tracked_scalar_values_partial(left, right, work)?,
        "tracked scalar comparison type mismatch",
    )
}
pub(crate) fn strict_scalar_ordering(
    ordering: ScalarOrdering,
    mismatch: &'static str,
) -> Result<std::cmp::Ordering, KernelFailure> {
    match ordering {
        ScalarOrdering::Ordered(ordering) => Ok(ordering),
        ScalarOrdering::UnorderedFloat => Err(source_failure::whole_unordered_float()),
        ScalarOrdering::MismatchedKinds => {
            Err(source_failure::contract(ContractKind::Comparison, mismatch))
        }
    }
}

pub fn compare_tracked_scalar_values_partial<A: ScalarStateAllocator>(
    left: &TrackedAggScalarValue<A>,
    right: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarOrdering, KernelFailure> {
    work.step()?;
    match (left, right) {
        (TrackedAggScalarValue::Bool(left), TrackedAggScalarValue::Bool(right)) => {
            Ok(ScalarOrdering::Ordered(left.cmp(right)))
        }
        (TrackedAggScalarValue::Int64(left), TrackedAggScalarValue::Int64(right)) => {
            Ok(ScalarOrdering::Ordered(left.cmp(right)))
        }
        (TrackedAggScalarValue::Float64(left), TrackedAggScalarValue::Float64(right)) => Ok(left
            .partial_cmp(right)
            .map(ScalarOrdering::Ordered)
            .unwrap_or(ScalarOrdering::UnorderedFloat)),
        (TrackedAggScalarValue::Utf8(left), TrackedAggScalarValue::Utf8(right))
        | (TrackedAggScalarValue::Binary(left), TrackedAggScalarValue::Binary(right)) => {
            for _ in left.iter().zip(right) {
                work.step()?;
            }
            work.flush()?;
            let ordering = left.as_slice().cmp(right.as_slice());
            work.flush()?;
            Ok(ScalarOrdering::Ordered(ordering))
        }
        (TrackedAggScalarValue::Date32(left), TrackedAggScalarValue::Date32(right)) => {
            Ok(ScalarOrdering::Ordered(left.cmp(right)))
        }
        (TrackedAggScalarValue::Timestamp(left), TrackedAggScalarValue::Timestamp(right)) => {
            Ok(ScalarOrdering::Ordered(left.cmp(right)))
        }
        (TrackedAggScalarValue::Decimal128(left), TrackedAggScalarValue::Decimal128(right)) => {
            Ok(ScalarOrdering::Ordered(left.cmp(right)))
        }
        (TrackedAggScalarValue::Decimal256(left), TrackedAggScalarValue::Decimal256(right)) => {
            Ok(ScalarOrdering::Ordered(left.cmp(right)))
        }
        (TrackedAggScalarValue::Struct(left), TrackedAggScalarValue::Struct(right))
        | (TrackedAggScalarValue::List(left), TrackedAggScalarValue::List(right)) => {
            compare_tracked_optional_slices(left, right, work)
        }
        (TrackedAggScalarValue::Map(left), TrackedAggScalarValue::Map(right)) => {
            for ((left_key, left_value), (right_key, right_value)) in left.iter().zip(right) {
                work.step()?;
                let ordering = compare_tracked_optional_values(left_key, right_key, work)?;
                if !matches!(ordering, ScalarOrdering::Ordered(std::cmp::Ordering::Equal)) {
                    return Ok(ordering);
                }
                let ordering = compare_tracked_optional_values(left_value, right_value, work)?;
                if !matches!(ordering, ScalarOrdering::Ordered(std::cmp::Ordering::Equal)) {
                    return Ok(ordering);
                }
            }
            Ok(ScalarOrdering::Ordered(left.len().cmp(&right.len())))
        }
        _ => Ok(ScalarOrdering::MismatchedKinds),
    }
}

fn compare_tracked_optional_slices<A: ScalarStateAllocator>(
    left: &[Option<TrackedAggScalarValue<A>>],
    right: &[Option<TrackedAggScalarValue<A>>],
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarOrdering, KernelFailure> {
    work.step()?;
    for (left, right) in left.iter().zip(right) {
        work.step()?;
        let ordering = compare_tracked_optional_values(left, right, work)?;
        if !matches!(ordering, ScalarOrdering::Ordered(std::cmp::Ordering::Equal)) {
            return Ok(ordering);
        }
    }
    Ok(ScalarOrdering::Ordered(left.len().cmp(&right.len())))
}

fn compare_tracked_optional_values<A: ScalarStateAllocator>(
    left: &Option<TrackedAggScalarValue<A>>,
    right: &Option<TrackedAggScalarValue<A>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarOrdering, KernelFailure> {
    work.step()?;
    match (left, right) {
        (None, None) => Ok(ScalarOrdering::Ordered(std::cmp::Ordering::Equal)),
        (None, Some(_)) => Ok(ScalarOrdering::Ordered(std::cmp::Ordering::Less)),
        (Some(_), None) => Ok(ScalarOrdering::Ordered(std::cmp::Ordering::Greater)),
        (Some(left), Some(right)) => compare_tracked_scalar_values_partial(left, right, work),
    }
}

pub fn tracked_scalar_heap_capacity<A: ScalarStateAllocator>(
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<usize, KernelFailure> {
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
    Existing(&'a largeint::LargeIntCarrierFailure),
}
impl std::fmt::Display for ScalarReadFailure<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(message) => f.write_str(message),
            Self::Existing(source) => source.fmt(f),
            Self::Unsupported(ty) => write!(f, "unsupported scalar type: {:?}", ty),
        }
    }
}
/// Owned, bounded facts from this read boundary. The DataType graph stays at
/// the call site and a real LARGEINT source keeps its nominal error chain.
#[derive(Debug)]
pub enum ScalarReadContractFailure {
    Downcast(&'static str),
    Unsupported(novarocks_failure_contract::BoundedDetail),
    LargeInt(largeint::LargeIntCarrierFailure),
}
impl ScalarReadFailure<'_> {
    pub fn into_kernel(self) -> KernelFailure {
        let cause = match self {
            Self::Static(message) => ScalarReadContractFailure::Downcast(message),
            Self::Unsupported(ty) => ScalarReadContractFailure::Unsupported(
                novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
                    "unsupported scalar type: {ty:?}"
                )),
            ),
            Self::Existing(source) => ScalarReadContractFailure::LargeInt(source.clone()),
        };
        KernelFailure::from_domain(cause)
    }
}
impl std::fmt::Display for ScalarReadContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Downcast(message) => f.write_str(message),
            Self::Unsupported(detail) => f.write_str(detail.as_str()),
            Self::LargeInt(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for ScalarReadContractFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::LargeInt(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ScalarReadContractFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let code = match self {
            Self::Downcast(_) => 300,
            Self::Unsupported(_) => 301,
            Self::LargeInt(source) => return source.failure_fact(),
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ScalarReadContractFailure {}
#[derive(Clone, Copy, Debug)]
pub enum ScalarReadObservation {
    Step,
    OpaqueBoundary,
}
/// Requests name the actual original std allocation that follows, not a
/// placeholder allocation. A pure host admits it before that operation.
pub trait ScalarReadFailureSink {
    type Error;
    fn read_contract(&mut self, failure: ScalarReadFailure<'_>) -> Self::Error;
    fn observe(&mut self, observation: ScalarReadObservation) -> Result<(), Self::Error>;
    fn reserve_scalar_copy(
        &mut self,
        elements: usize,
        width: usize,
        alignment: usize,
    ) -> Result<(), Self::Error>;
}
struct UntrackedScalarReadSink<'borrow, 'control, 'scope> {
    work: &'borrow mut ScalarWork<'control, 'scope>,
}
impl ScalarReadFailureSink for UntrackedScalarReadSink<'_, '_, '_> {
    type Error = KernelFailure;
    fn read_contract(&mut self, failure: ScalarReadFailure<'_>) -> KernelFailure {
        failure.into_kernel()
    }
    fn observe(&mut self, event: ScalarReadObservation) -> Result<(), KernelFailure> {
        match event {
            ScalarReadObservation::Step => self.work.step(),
            ScalarReadObservation::OpaqueBoundary => self.work.flush(),
        }
    }
    fn reserve_scalar_copy(&mut self, _: usize, _: usize, _: usize) -> Result<(), KernelFailure> {
        Ok(())
    }
}
pub fn scalar_from_array(
    array: &ArrayRef,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<AggScalarValue>, KernelFailure> {
    scalar_from_array_with_failure(array, row, &mut UntrackedScalarReadSink { work })
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
                    sink.read_contract(ScalarReadFailure::Static(
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
                sink.read_contract(ScalarReadFailure::Static("failed to downcast to Int8Array"))
            })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row) as i64)))
            }
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                sink.read_contract(ScalarReadFailure::Static(
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
                sink.read_contract(ScalarReadFailure::Static(
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
                sink.read_contract(ScalarReadFailure::Static(
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
                    sink.read_contract(ScalarReadFailure::Static(
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
                    sink.read_contract(ScalarReadFailure::Static(
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
                    sink.read_contract(ScalarReadFailure::Static(
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
                    sink.read_contract(ScalarReadFailure::Static(
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
                    sink.read_contract(ScalarReadFailure::Static(
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
                    sink.read_contract(ScalarReadFailure::Static(
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
                        sink.read_contract(ScalarReadFailure::Static(
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
                        sink.read_contract(ScalarReadFailure::Static(
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
                        sink.read_contract(ScalarReadFailure::Static(
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
                        sink.read_contract(ScalarReadFailure::Static(
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
                    sink.read_contract(ScalarReadFailure::Static(
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
                    sink.read_contract(ScalarReadFailure::Static(
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
                    sink.read_contract(ScalarReadFailure::Static(
                        "failed to downcast to FixedSizeBinaryArray",
                    ))
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                let v = largeint::value_at(arr, row)
                    .map_err(|message| sink.read_contract(ScalarReadFailure::Existing(&message)))?;
                Ok(Some(AggScalarValue::Decimal128(v)))
            }
        }
        DataType::List(_item) => {
            let arr = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                sink.read_contract(ScalarReadFailure::Static("failed to downcast to ListArray"))
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
                    sink.read_contract(ScalarReadFailure::Static(
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
                sink.read_contract(ScalarReadFailure::Static("failed to downcast to MapArray"))
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
        other => Err(sink.read_contract(ScalarReadFailure::Unsupported(other))),
    }
}

#[cfg(test)]
#[path = "aggregate_scalar/largeint_observation_tests.rs"]
mod largeint_observation_tests;

#[cfg(test)]
mod output_allocation_source_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn scalar_output_actual_reserve_failure_keeps_source_and_unknown_physical_facts() {
        let mut output = Vec::<u8>::new();
        let failure =
            reserve_output(&mut output, usize::MAX, "reserve actual scalar output").unwrap_err();
        assert!(output.is_empty());
        assert_eq!(output.capacity(), 0);
        let mut source = failure.source();
        let mut found = false;
        while let Some(actual) = source {
            found |= actual.is::<std::collections::TryReserveError>();
            source = actual.source();
        }
        assert!(
            found,
            "actual TryReserveError must remain in the source chain"
        );
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().unwrap();
        assert_eq!(resource.requested, None);
        assert_eq!(resource.limit, None);
        assert_eq!(resource.constraint_scope, None);
    }
}

/// One checked Binary result construction. The caller still owns admission,
/// allocation attribution and custody; offset facts grant no memory capacity.
/// The original Arrow builder and its reset buffer keep their normal lifetime.
pub struct BinaryResultBuilder {
    builder: arrow_array::builder::BinaryBuilder,
    extent: source_failure::OutputTally,
    failure: Option<KernelFailure>,
}
impl Default for BinaryResultBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl BinaryResultBuilder {
    pub fn new() -> Self {
        Self {
            builder: arrow_array::builder::BinaryBuilder::new(),
            extent: source_failure::OutputTally::new(source_failure::OffsetLayout::Binary),
            failure: None,
        }
    }
    pub fn append_value(&mut self, value: impl AsRef<[u8]>) -> Result<(), KernelFailure> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        let value = value.as_ref();
        if let Err(cause) = self.extent.append(value.len()) {
            self.failure = Some(cause.clone());
            return Err(cause);
        }
        self.builder.append_value(value);
        Ok(())
    }
    pub fn append_null(&mut self) -> Result<(), KernelFailure> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        self.builder.append_null();
        Ok(())
    }
    pub fn finish(&mut self) -> Result<arrow_array::BinaryArray, KernelFailure> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        let result = self.builder.finish();
        self.extent = source_failure::OutputTally::new(source_failure::OffsetLayout::Binary);
        Ok(result)
    }
}
#[cfg(test)]
mod binary_result_builder_properties {
    use super::*;
    use arrow_array::Array;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn rejected_binary_extent_latches_actual_cause_without_publishing_a_partial_result() {
        let mut builder = BinaryResultBuilder::new();
        builder.extent.append(i32::MAX as usize).unwrap();
        let cause = builder.append_value([1u8]).unwrap_err();
        assert_eq!(cause.failure_fact().class(), FailureClass::Representation);
        assert_eq!(
            cause
                .failure_fact()
                .representation()
                .unwrap()
                .representable_prefix,
            None
        );
        assert_eq!(builder.append_value([]), Err(cause.clone()));
        assert_eq!(builder.append_null(), Err(cause.clone()));
        assert_eq!(builder.finish().unwrap_err(), cause);
    }
    #[test]
    fn binary_finish_resets_format_state_with_the_original_null_and_empty_value_behavior() {
        let mut builder = BinaryResultBuilder::new();
        builder.append_value([1, 2]).unwrap();
        builder.append_null().unwrap();
        builder.append_value([]).unwrap();
        let first = builder.finish().unwrap();
        assert_eq!(first.value(0), &[1, 2]);
        assert!(first.is_null(1));
        assert_eq!(first.value(2), &[] as &[u8]);
        assert_eq!(builder.extent.total(), 0);
        builder.append_value([3]).unwrap();
        let second = builder.finish().unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second.value(0), &[3]);
    }
}

/// Check an actual output span before appending. It grants neither capacity
/// nor an input prefix; each caller supplies its exact format and value span.
fn checked_output_offset(
    current: i32,
    additional: u128,
    layout: source_failure::OffsetLayout,
) -> Result<i32, KernelFailure> {
    if current < 0 {
        return Err(source_failure::contract(
            ContractKind::Offsets,
            "existing output offset is negative",
        ));
    }
    let mut tally = source_failure::OutputTally::new(layout);
    tally.append(current as usize)?;
    tally.append_u128(additional)?;
    i32::try_from(tally.total()).map_err(|_| {
        source_failure::contract(
            ContractKind::Offsets,
            "validated output extent does not fit Int32",
        )
    })
}
pub fn checked_list_output_offset(current: i32, additional: usize) -> Result<i32, KernelFailure> {
    checked_output_offset(
        current,
        additional as u128,
        source_failure::OffsetLayout::List,
    )
}
pub fn checked_map_output_offset(current: i32, additional: usize) -> Result<i32, KernelFailure> {
    checked_output_offset(
        current,
        additional as u128,
        source_failure::OffsetLayout::Map,
    )
}
pub fn checked_map_output_offset_u128(
    current: i32,
    additional: u128,
) -> Result<i32, KernelFailure> {
    checked_output_offset(current, additional, source_failure::OffsetLayout::Map)
}
pub fn checked_binary_output_offset(current: i32, additional: usize) -> Result<i32, KernelFailure> {
    checked_output_offset(
        current,
        additional as u128,
        source_failure::OffsetLayout::Binary,
    )
}
pub fn checked_utf8_output_offset(current: i32, additional: usize) -> Result<i32, KernelFailure> {
    checked_output_offset(
        current,
        additional as u128,
        source_failure::OffsetLayout::Utf8,
    )
}
/// The exact concatenated logical span may exceed pointer width before a
/// physical output is attempted. It uses the SAME List offset author.
pub fn checked_list_output_offset_u128(
    current: i32,
    additional: u128,
) -> Result<i32, KernelFailure> {
    checked_output_offset(current, additional, source_failure::OffsetLayout::List)
}
#[cfg(test)]
mod list_result_offset_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    #[test]
    fn list_result_span_retains_exact_unit_and_result_limits_without_input_prefix_credit() {
        assert_eq!(checked_list_output_offset(i32::MAX, 0).unwrap(), i32::MAX);
        for (current, additional, boundary) in [
            (i32::MAX - 1, 2, RepresentationBoundary::SingleOutput),
            (
                0,
                i32::MAX as usize + 1,
                RepresentationBoundary::MinimumUnit,
            ),
        ] {
            let failure = checked_list_output_offset(current, additional).unwrap_err();
            let fact = failure.failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            let facts = fact.representation().unwrap();
            assert_eq!(facts.boundary, boundary);
            assert_eq!(facts.required, Some(i32::MAX as u64 + 1));
            assert_eq!(facts.extent_limit, Some(i32::MAX as u64));
            assert_eq!(facts.representable_prefix, None);
        }
        assert_eq!(
            checked_list_output_offset(-1, 0)
                .unwrap_err()
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
    }
}

#[cfg(test)]
mod map_utf8_offset_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    #[test]
    fn actual_map_and_utf8_formats_keep_their_own_extents_without_row_credit() {
        for (check, layout) in [
            (
                checked_map_output_offset as fn(i32, usize) -> Result<i32, KernelFailure>,
                64,
            ),
            (checked_utf8_output_offset, 60),
        ] {
            assert_eq!(check(i32::MAX, 0).unwrap(), i32::MAX);
            for (current, incoming, boundary) in [
                (i32::MAX, 1, RepresentationBoundary::SingleOutput),
                (
                    0,
                    i32::MAX as usize + 1,
                    RepresentationBoundary::MinimumUnit,
                ),
            ] {
                let failure = check(current, incoming).unwrap_err();
                assert_eq!(failure.failure_fact().class(), FailureClass::Representation);
                let facts = failure.failure_fact().representation().unwrap();
                assert_eq!(facts.layout.code.get(), layout);
                assert_eq!(facts.boundary, boundary);
                assert_eq!(facts.required, Some(i32::MAX as u64 + 1));
                assert_eq!(facts.extent_limit, Some(i32::MAX as u64));
                assert_eq!(facts.representable_prefix, None);
            }
        }
    }
}

#[cfg(test)]
mod wide_list_output_properties {
    use super::*;
    use novarocks_failure_contract::{
        FailureClass, FailureEvidence, FailureProjection, RepresentationBoundary, domain,
    };
    #[test]
    fn actual_wide_spans_refuse_without_narrowing_or_fabricated_prefix() {
        assert_eq!(checked_list_output_offset_u128(2, 3).unwrap(), 5);
        for (current, additional, required, boundary) in [
            (
                i32::MAX - 1,
                2,
                Some(i32::MAX as u64 + 1),
                RepresentationBoundary::SingleOutput,
            ),
            (
                0,
                i32::MAX as u128 + 1,
                Some(i32::MAX as u64 + 1),
                RepresentationBoundary::MinimumUnit,
            ),
            (
                i32::MAX,
                u128::MAX,
                None,
                RepresentationBoundary::MinimumUnit,
            ),
        ] {
            let failure = checked_list_output_offset_u128(current, additional).unwrap_err();
            let fact = failure.failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            assert_eq!(fact.evidence(), FailureEvidence::Originating);
            assert_eq!(fact.reason().domain(), domain::KERNEL.get());
            assert_eq!(fact.reason().code(), 320);
            assert_eq!(fact.resource(), None);
            assert_eq!(fact.row_position(), None);
            let facts = fact.representation().unwrap();
            assert_eq!(facts.layout.domain, domain::KERNEL);
            assert_eq!(facts.layout.code.get(), 63);
            assert_eq!(facts.required, required);
            assert_eq!(facts.boundary, boundary);
            assert_eq!(facts.extent_limit, Some(i32::MAX as u64));
            assert_eq!(facts.representable_prefix, None);
        }
    }
}
