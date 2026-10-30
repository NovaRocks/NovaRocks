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

//! First construction of typed syntax-literal runs. Existing ConstantValue
//! backings never enter this author. Policy bounds shape each new pool; they
//! do not grant host memory or replace the original compile control.

use super::{
    decimal_coefficient_observed, syntax_float32_value, syntax_integer_value, validate_syntax_type,
};
use crate::common::LiteralValue;
use arrow::{
    array::ArrayData,
    buffer::{Buffer, MutableBuffer},
    datatypes::{DataType, Field, TimeUnit, ToByteSlice},
};
use novarocks_constant_contract::{
    ConstantError, ConstantPolicy, ConstantPool, FlatConstantResourceInput,
    preflight_flat_pool_resources,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueLogicalType,
};
use std::sync::Arc;

pub(crate) fn syntax_run_carrier(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
            | DataType::Timestamp(_, _)
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
    )
}

fn add(a: usize, b: usize) -> Result<usize, ConstantError> {
    a.checked_add(b)
        .ok_or(ConstantError::Limit("SQL literal run geometry overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, ConstantError> {
    a.checked_mul(b)
        .ok_or(ConstantError::Limit("SQL literal run geometry overflow"))
}
fn offset_width(ty: &DataType) -> Option<usize> {
    match ty {
        DataType::Utf8 | DataType::Binary => Some(4),
        DataType::LargeUtf8 | DataType::LargeBinary => Some(8),
        _ => None,
    }
}
fn width(ty: &DataType) -> Result<usize, ConstantError> {
    match ty {
        DataType::FixedSizeBinary(n) => usize::try_from(*n)
            .map_err(|_| ConstantError::Invalid("negative fixed binary literal width")),
        _ => ty
            .primitive_width()
            .ok_or(ConstantError::Invalid("SQL literal has no fixed carrier")),
    }
}
struct Geometry {
    validity: usize,
    offsets: usize,
    values: usize,
}
fn preflight(
    values: &[&LiteralValue],
    field: &Field,
    ty: &FunctionValueType,
    policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, ConstantError> {
    let rows = values.len();
    let validity = if ty.nullable() && *ty.data_type() != DataType::Null {
        add(rows, 7)? / 8
    } else {
        0
    };
    let offsets = offset_width(ty.data_type())
        .map(|n| mul(add(rows, 1)?, n))
        .transpose()?
        .unwrap_or(0);
    let mut payload = 0;
    for value in values {
        work.step()?;
        if matches!(value, LiteralValue::Null) && !ty.nullable() {
            return Err(ConstantError::Invalid(
                "NULL factory requires nullable exact type",
            ));
        }
        payload = add(
            payload,
            match value {
                LiteralValue::String(v) => v.len(),
                LiteralValue::Binary(v) => v.len(),
                _ => 0,
            },
        )?;
    }
    if offsets != 0 && offset_width(ty.data_type()) == Some(4) && payload > i32::MAX as usize {
        return Err(ConstantError::Limit(
            "SQL literal run exceeds its exact offset width",
        ));
    }
    let data = match ty.data_type() {
        DataType::Null => 0,
        DataType::Boolean => add(rows, 7)? / 8,
        _ if offsets != 0 => payload,
        _ => mul(rows, width(ty.data_type())?)?,
    };
    let backing = add(add(validity, offsets)?, data)?;
    let buffers = usize::from(validity != 0)
        + usize::from(offsets != 0)
        + usize::from(*ty.data_type() != DataType::Null);
    work.flush()?;
    let bounds = preflight_flat_pool_resources(
        field,
        ty,
        FlatConstantResourceInput {
            rows: rows as u64,
            buffer_count_upper_bound: buffers as u64,
            buffer_visits_bytes_upper_bound: backing as u64,
            retained_buffer_capacity_bytes_upper_bound: backing as u64,
            view_validation_bytes_upper_bound: 0,
        },
        policy,
        CompilePhase::FunctionSpecialization,
        work.control(),
    )?;
    // Safe ArrayData construction and the original pool admission each run
    // finite Arrow validation. Reuse that same envelope for both real passes.
    if bounds
        .library_validation_work_upper_bound
        .checked_mul(2)
        .is_none_or(|n| n > policy.max_library_validation_work)
    {
        return Err(ConstantError::Limit(
            "SQL literal run library work limit exceeded",
        ));
    }
    if bounds
        .library_validation_bytes_upper_bound
        .checked_mul(2)
        .is_none_or(|n| n > policy.max_library_validation_bytes)
    {
        return Err(ConstantError::Limit(
            "SQL literal run library byte limit exceeded",
        ));
    }
    work.flush()?;
    Ok(Geometry {
        validity,
        offsets,
        values: data,
    })
}
fn zeroed(n: usize, work: &mut CompileCheckpoints<'_>) -> Result<MutableBuffer, ConstantError> {
    // This exact-length constructor gives fixed carriers Arrow alignment
    // without a growing builder or a second temporary values allocation.
    work.flush()?;
    std::alloc::Layout::from_size_align(n, 64)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    let buffer = MutableBuffer::from_len_zeroed(n);
    work.flush()?;
    Ok(buffer)
}
fn copy(
    output: &mut [u8],
    input: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantError> {
    for (out, input) in output.chunks_mut(256).zip(input.chunks(256)) {
        out.copy_from_slice(input);
        for _ in input {
            work.step()?;
        }
    }
    Ok(())
}
fn fixed_bytes(
    value: &LiteralValue,
    ty: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<([u8; 32], usize), ConstantError> {
    let mut bytes = [0u8; 32];
    macro_rules! exact {
        ($value:expr) => {{
            let value = $value;
            let v = value.to_byte_slice();
            bytes[..v.len()].copy_from_slice(v);
            return Ok((bytes, v.len()));
        }};
    }
    match value {
        LiteralValue::Int(value) => {
            macro_rules! integer {
                ($native:ty) => {{
                    let v = syntax_integer_value::<$native>(*value)?;
                    exact!(v);
                }};
            }
            match ty.data_type() {
                DataType::Int8 => integer!(i8),
                DataType::Int16 => integer!(i16),
                DataType::Int32 | DataType::Date32 => integer!(i32),
                DataType::Int64
                | DataType::Date64
                | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
                | DataType::Timestamp(_, _) => integer!(i64),
                DataType::UInt8 => integer!(u8),
                DataType::UInt16 => integer!(u16),
                DataType::UInt32 => integer!(u32),
                DataType::UInt64 => integer!(u64),
                _ => Err(ConstantError::Invalid(
                    "SQL integer syntax requires its exact declared integer/date carrier",
                )),
            }
        }
        LiteralValue::Float(value) => match ty.data_type() {
            DataType::Float64 => exact!(*value),
            DataType::Float32 => {
                let v = syntax_float32_value(*value)?;
                exact!(v);
            }
            _ => Err(ConstantError::Invalid(
                "SQL float syntax requires its declared floating carrier",
            )),
        },
        LiteralValue::LargeInt(value)
            if ty.logical_type() == ValueLogicalType::LargeInt
                && *ty.data_type() == DataType::FixedSizeBinary(16) =>
        {
            bytes[..16].copy_from_slice(&value.to_be_bytes());
            Ok((bytes, 16))
        }
        LiteralValue::Decimal(text) => {
            let (p, s) = match ty.data_type() {
                DataType::Decimal32(p, s)
                | DataType::Decimal64(p, s)
                | DataType::Decimal128(p, s)
                | DataType::Decimal256(p, s) => (*p, *s),
                _ => {
                    return Err(ConstantError::Invalid(
                        "SQL decimal syntax requires its exact declared decimal carrier",
                    ));
                }
            };
            let value = decimal_coefficient_observed(text, p, s, work)?;
            macro_rules! decimal {
                ($native:ty) => {{
                    let v = value
                        .to_i128()
                        .and_then(|v| <$native>::try_from(v).ok())
                        .ok_or(ConstantError::Invalid(
                            "SQL decimal coefficient exceeds its declared width",
                        ))?;
                    exact!(v);
                }};
            }
            match ty.data_type() {
                DataType::Decimal32(_, _) => decimal!(i32),
                DataType::Decimal64(_, _) => decimal!(i64),
                DataType::Decimal128(_, _) => decimal!(i128),
                DataType::Decimal256(_, _) => exact!(value),
                _ => unreachable!(),
            }
        }
        _ => Err(ConstantError::Invalid(
            "SQL syntax differs from its exact declared carrier",
        )),
    }
}

/// Return one newly created pool and its consumed syntax prefix. A smaller
/// prefix is selected only for this new pool's numerical ConstantPolicy
/// envelope; source, cancellation and resource-control failures never retry.
pub(crate) fn admit_syntax_constant_run(
    values: &[&LiteralValue],
    ty: &FunctionValueType,
    policy: ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<(ConstantPool, usize), ConstantError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        validate_syntax_type(ty, &mut work)?;
        if values.is_empty() || !syntax_run_carrier(ty.data_type()) {
            return Err(ConstantError::Invalid(
                "SQL literal run requires nonempty supported syntax",
            ));
        }
        let field = Arc::new(Field::new("literal", ty.data_type().clone(), ty.nullable()));
        let max_rows = usize::try_from(policy.max_rows).unwrap_or(usize::MAX);
        let mut count = values.len().min(max_rows.max(1));
        let geometry = loop {
            match preflight(&values[..count], &field, ty, policy, &mut work) {
                Ok(geometry) => break geometry,
                Err(ConstantError::Limit(_)) if count > 1 => {
                    count /= 2;
                }
                Err(error) => return Err(error),
            }
        };
        let values = &values[..count];
        let mut validity = zeroed(geometry.validity, &mut work)?;
        let mut offsets = zeroed(geometry.offsets, &mut work)?;
        let mut data = zeroed(geometry.values, &mut work)?;
        let mut at = 0;
        for (row, value) in values.iter().enumerate() {
            work.step()?;
            let null = matches!(value, LiteralValue::Null);
            if !null && !validity.is_empty() {
                validity[row / 8] |= 1 << (row % 8);
            }
            if !null {
                if offset_width(ty.data_type()).is_some() {
                    let payload: &[u8] = match (value, ty.data_type()) {
                        (LiteralValue::String(value), DataType::Utf8 | DataType::LargeUtf8) => {
                            value.as_bytes()
                        }
                        (LiteralValue::Binary(value), DataType::Binary | DataType::LargeBinary) => {
                            value
                        }
                        _ => {
                            return Err(ConstantError::Invalid(
                                "SQL variable literal differs from its declared carrier",
                            ));
                        }
                    };
                    copy(&mut data[at..at + payload.len()], payload, &mut work)?;
                    at += payload.len();
                } else if *ty.data_type() == DataType::Boolean {
                    let LiteralValue::Bool(value) = value else {
                        return Err(ConstantError::Invalid(
                            "SQL Boolean literal requires its declared carrier",
                        ));
                    };
                    if *value {
                        data[row / 8] |= 1 << (row % 8);
                    }
                } else {
                    let (bytes, n) = fixed_bytes(value, ty, &mut work)?;
                    let expected = width(ty.data_type())?;
                    if n != expected {
                        return Err(ConstantError::Invalid(
                            "SQL literal differs from its declared fixed width",
                        ));
                    }
                    copy(&mut data[row * n..(row + 1) * n], &bytes[..n], &mut work)?;
                }
            }
            if let Some(n) = offset_width(ty.data_type()) {
                if n == 4 {
                    offsets[(row + 1) * n..(row + 2) * n]
                        .copy_from_slice(&(at as i32).to_ne_bytes());
                } else {
                    let offset = i64::try_from(at)
                        .map_err(|_| ConstantError::Limit("SQL literal offset exceeds i64"))?;
                    offsets[(row + 1) * n..(row + 2) * n].copy_from_slice(&offset.to_ne_bytes());
                }
            }
        }
        work.flush()?;
        let mut builder = ArrayData::builder(ty.data_type().clone()).len(count);
        if geometry.validity != 0 {
            builder = builder.null_bit_buffer(Some(Buffer::from(validity)));
        }
        if geometry.offsets != 0 {
            builder = builder.add_buffer(Buffer::from(offsets));
        }
        if *ty.data_type() != DataType::Null {
            builder = builder.add_buffer(Buffer::from(data));
        }
        let data = builder.build().map_err(ConstantError::arrow_validation)?;
        work.flush()?;
        let pool = ConstantPool::try_new(
            field,
            ty.clone(),
            data,
            policy,
            CompilePhase::FunctionSpecialization,
            work.control(),
        )?;
        work.flush()?;
        Ok((pool, count))
    })();
    work.finish_result(result, ConstantError::from)
}
