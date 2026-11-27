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
//! backings never enter this author. Each pool uses the longest consecutive
//! prefix its exact carrier and ordinal can represent.

use super::{
    decimal_coefficient_observed, syntax_float32_value, syntax_integer_value, validate_syntax_type,
};
use crate::common::LiteralValue;
use arrow::alloc::ALIGNMENT;
use arrow::{
    array::ArrayData,
    buffer::{Buffer, MutableBuffer},
    datatypes::{DataType, Field, TimeUnit, ToByteSlice},
};
use novarocks_constant_contract::{
    ConstantCreationFailure, ConstantError, ConstantPool, ConstantValidation,
    FlatConstantResourceInput, preflight_flat_pool_resources_input_derived_observed,
};
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use std::alloc::Layout;
use std::convert::Infallible;
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
        .ok_or_else(|| ConstantError::offset_extent(u64::MAX, isize::MAX as u64))
}

fn mul(a: usize, b: usize) -> Result<usize, ConstantError> {
    a.checked_mul(b)
        .ok_or_else(|| ConstantError::offset_extent(u64::MAX, isize::MAX as u64))
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
#[derive(Default)]
struct Geometry {
    validity: usize,
    offsets: usize,
    values: usize,
}

/// A scan of immutable typed cells. It owns only the extents of the prefix;
/// the actual rows stay borrowed in the source until the one materialization.
pub(crate) struct SyntaxLiteralPrefix<'ty> {
    ty: &'ty FunctionValueType,
    rows: usize,
    payload: usize,
    geometry: Geometry,
}

impl<'ty> SyntaxLiteralPrefix<'ty> {
    pub(crate) fn new(ty: &'ty FunctionValueType) -> Self {
        Self {
            ty,
            rows: 0,
            payload: 0,
            geometry: Geometry::default(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.rows
    }

    pub(crate) fn try_push(&mut self, literal: &LiteralValue) -> Result<bool, ConstantError> {
        if matches!(literal, LiteralValue::Null) && !self.ty.nullable() {
            return Err(ConstantError::Invalid(
                "NULL factory requires nullable exact type",
            ));
        }
        let ty = self.ty.data_type();
        let bytes = match (ty, literal) {
            (_, LiteralValue::Null) => 0,
            (DataType::Utf8 | DataType::LargeUtf8, LiteralValue::String(value)) => value.len(),
            (DataType::Binary | DataType::LargeBinary, LiteralValue::Binary(value)) => value.len(),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary,
                _,
            ) => {
                return Err(ConstantError::Invalid(
                    "SQL variable literal differs from its declared carrier",
                ));
            }
            _ => 0,
        };
        let next: Result<_, ConstantError> = (|| {
            let rows = add(self.rows, 1)?;
            let payload = add(self.payload, bytes)?;
            Ok((rows, payload, self.measure(rows, payload)?))
        })();
        match next {
            Ok((rows, payload, geometry)) => {
                self.rows = rows;
                self.payload = payload;
                self.geometry = geometry;
                Ok(true)
            }
            // Only the arithmetic in measure and the checked additions above
            // can reach this boundary. Data and control failures do not split.
            Err(error)
                if self.rows != 0
                    && error.failure_fact().class() == FailureClass::Representation =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    fn measure(&self, rows: usize, payload: usize) -> Result<Geometry, ConstantError> {
        let last_ordinal = rows - 1;
        if u32::try_from(last_ordinal).is_err() {
            return Err(ConstantError::offset_extent(
                last_ordinal as u64,
                u32::MAX as u64,
            ));
        }
        let ty = self.ty.data_type();
        let validity = if self.ty.nullable() && *ty != DataType::Null {
            rows.div_ceil(8)
        } else {
            0
        };
        let offsets = match offset_width(ty) {
            Some(n) => {
                let bound = if n == 4 {
                    i32::MAX as u64
                } else {
                    i64::MAX as u64
                };
                if payload as u64 > bound {
                    return Err(ConstantError::offset_extent(payload as u64, bound));
                }
                mul(add(rows, 1)?, n)?
            }
            None => 0,
        };
        let values = match ty {
            DataType::Null => 0,
            DataType::Boolean => rows.div_ceil(8),
            _ if offsets != 0 => payload,
            _ => mul(rows, width(ty)?)?,
        };
        for size in [validity, offsets, values] {
            Layout::from_size_align(size, ALIGNMENT).map_err(|_| {
                ConstantError::offset_extent(
                    size as u64,
                    ((isize::MAX as usize) & !(ALIGNMENT - 1)) as u64,
                )
            })?;
        }
        Ok(Geometry {
            validity,
            offsets,
            values,
        })
    }

    fn preflight(
        &self,
        field: &Field,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ConstantError> {
        let geometry = &self.geometry;
        let backing = add(add(geometry.validity, geometry.offsets)?, geometry.values)?;
        let buffers = usize::from(geometry.validity != 0)
            + usize::from(geometry.offsets != 0)
            + usize::from(*self.ty.data_type() != DataType::Null);
        preflight_flat_pool_resources_input_derived_observed(
            field,
            self.ty,
            FlatConstantResourceInput {
                rows: self.rows as u64,
                buffer_count_upper_bound: buffers as u64,
                buffer_visits_bytes_upper_bound: backing as u64,
                retained_buffer_capacity_bytes_upper_bound: backing as u64,
                view_validation_bytes_upper_bound: 0,
            },
            &mut |_| Ok::<_, Infallible>(()),
            work,
        )
        .map_err(|error| match error {
            ConstantCreationFailure::Constant(cause) => cause,
            ConstantCreationFailure::Creator(never) => match never {},
            ConstantCreationFailure::Resource(cause) => ConstantError::from_domain(cause),
            ConstantCreationFailure::SourceMetadata(cause) => ConstantError::from_domain(cause),
        })?;
        Ok(())
    }
}

fn zeroed(n: usize, work: &mut CompileCheckpoints<'_>) -> Result<MutableBuffer, ConstantError> {
    work.flush()?;
    let buffer = MutableBuffer::try_from_len_zeroed(n)
        .map_err(|error| ConstantError::arrow_buffer_allocation(n, error))?;
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

impl SyntaxLiteralPrefix<'_> {
    pub(crate) fn materialize<'a>(
        &self,
        values: impl ExactSizeIterator<Item = &'a LiteralValue>,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, ConstantError> {
        let ty = self.ty;
        let count = self.rows;
        let geometry = &self.geometry;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            validate_syntax_type(ty, &mut work)?;
            if count == 0 || count != values.len() || !syntax_run_carrier(ty.data_type()) {
                return Err(ConstantError::Invalid(
                    "SQL literal run differs from its checked prefix",
                ));
            }
            let field = Arc::new(Field::new("literal", ty.data_type().clone(), ty.nullable()));
            self.preflight(&field, &mut work)?;
            let mut validity = zeroed(geometry.validity, &mut work)?;
            let mut offsets = zeroed(geometry.offsets, &mut work)?;
            let mut data = zeroed(geometry.values, &mut work)?;
            let mut at = 0;
            for (row, value) in values.enumerate() {
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
                            (
                                LiteralValue::Binary(value),
                                DataType::Binary | DataType::LargeBinary,
                            ) => value,
                            _ => {
                                return Err(ConstantError::Invalid(
                                    "SQL variable literal differs from its declared carrier",
                                ));
                            }
                        };
                        let end = add(at, payload.len())?;
                        let range = data.get_mut(at..end).ok_or(ConstantError::Invalid(
                            "SQL literal differs from the checked prefix extent",
                        ))?;
                        copy(range, payload, &mut work)?;
                        at = end;
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
                        let offset = i64::try_from(at).map_err(|_| {
                            ConstantError::offset_extent(at as u64, i64::MAX as u64)
                        })?;
                        offsets[(row + 1) * n..(row + 2) * n]
                            .copy_from_slice(&offset.to_ne_bytes());
                    }
                }
            }
            if geometry.offsets != 0 && at != geometry.values {
                return Err(ConstantError::Invalid(
                    "SQL literal differs from the checked prefix extent",
                ));
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
                ConstantValidation::INPUT_DERIVED,
                CompilePhase::FunctionSpecialization,
                work.control(),
            )?;
            work.flush()?;
            Ok(pool)
        })();
        work.finish_result(result, ConstantError::from)
    }
}

#[cfg(test)]
pub(crate) fn admit_syntax_constant_run(
    values: &[&LiteralValue],
    ty: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<(ConstantPool, usize), ConstantError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let mut prefix = SyntaxLiteralPrefix::new(ty);
    for value in values {
        work.step()?;
        if !prefix.try_push(value)? {
            break;
        }
    }
    work.finish()?;
    let count = prefix.len();
    Ok((
        prefix.materialize(values[..count].iter().copied(), control)?,
        count,
    ))
}
