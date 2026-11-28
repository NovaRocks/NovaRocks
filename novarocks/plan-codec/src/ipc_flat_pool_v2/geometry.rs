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

//! Allocation-free geometry for the canonical checked pool's flat IPC body.
//! Payload ranges follow Arrow's flat writer: offsets are rebased but hidden
//! NULL payload remains in its contiguous span; view backing is never pruned.

use crate::{
    ipc_flat_batch_v2::{Layout, layout},
    physical_type_v2::TypeCodecError,
};
use arrow::array::ArrayData;
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::CompileCheckpoints;

#[derive(Clone, Copy)]
pub(crate) struct Geometry {
    pub rows: usize,
    pub buffers: usize,
    pub variadic: usize,
    pub views: bool,
    pub body_bytes: usize,
    pub payload_bytes: usize,
    pub values_start: usize,
    pub values_bytes: usize,
    pub offset_base: usize,
}
fn invalid() -> TypeCodecError {
    TypeCodecError::InvalidShape("flat pool writer extent is not representable")
}
pub(crate) fn add(a: usize, b: usize) -> Result<usize, TypeCodecError> {
    a.checked_add(b).ok_or_else(invalid)
}
pub(crate) fn mul(a: usize, b: usize) -> Result<usize, TypeCodecError> {
    a.checked_mul(b).ok_or_else(invalid)
}
pub(crate) fn aligned(n: usize) -> Result<usize, TypeCodecError> {
    Ok(add(n, 7)? & !7)
}
pub(crate) fn bit_bytes(n: usize) -> Result<usize, TypeCodecError> {
    Ok(add(n, 7)? / 8)
}
pub(crate) fn native_offset(
    bytes: &[u8],
    at: usize,
    width: usize,
) -> Result<usize, TypeCodecError> {
    let start = mul(at, width)?;
    let value = bytes.get(start..add(start, width)?).ok_or_else(invalid)?;
    let signed = match width {
        4 => i64::from(i32::from_ne_bytes(value.try_into().map_err(|_| invalid())?)),
        8 => i64::from_ne_bytes(value.try_into().map_err(|_| invalid())?),
        _ => return Err(invalid()),
    };
    usize::try_from(signed).map_err(|_| invalid())
}

pub(crate) fn inspect(
    pool: &ConstantPool,
    max_rows: usize,
    max_buffers: usize,
    max_body_bytes: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, TypeCodecError> {
    inspect_span(
        pool.data(),
        0,
        pool.data().len(),
        max_rows,
        max_buffers,
        max_body_bytes,
        work,
    )
}

pub(crate) fn inspect_span(
    data: &ArrayData,
    start: usize,
    rows: usize,
    max_rows: usize,
    max_buffers: usize,
    max_body_bytes: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, TypeCodecError> {
    inspect_span_core::<std::convert::Infallible>(
        data,
        start,
        rows,
        max_rows,
        max_buffers,
        max_body_bytes,
        None,
        work,
    )
    .map_err(ProjectionFailure::without_host)
}

use crate::host_projection_v2::ProjectionFailure;
type HostError<H> = ProjectionFailure<TypeCodecError, H>;
type GeometryCapture<'callback, H> = dyn FnMut(&Geometry) -> Result<(), HostError<H>> + 'callback;

pub(crate) fn inspect_span_core<H>(
    data: &ArrayData,
    start: usize,
    rows: usize,
    max_rows: usize,
    max_buffers: usize,
    max_body_bytes: usize,
    capture: Option<&mut GeometryCapture<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, HostError<H>> {
    inspect_span_policy_core(
        data,
        start,
        rows,
        Some((max_rows, max_buffers, max_body_bytes)),
        capture,
        work,
    )
}
pub(crate) fn inspect_span_pure<H>(
    data: &ArrayData,
    start: usize,
    rows: usize,
    capture: Option<&mut GeometryCapture<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, HostError<H>> {
    inspect_span_policy_core(data, start, rows, None, capture, work)
}
pub(crate) fn inspect_pure(
    pool: &ConstantPool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, TypeCodecError> {
    inspect_span_pure::<std::convert::Infallible>(pool.data(), 0, pool.data().len(), None, work)
        .map_err(ProjectionFailure::without_host)
}
fn inspect_span_policy_core<H>(
    data: &ArrayData,
    start: usize,
    rows: usize,
    receiving: Option<(usize, usize, usize)>,
    mut capture: Option<&mut GeometryCapture<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, HostError<H>> {
    let policy = crate::ipc_flat_pool_v2::WriterAdmissionPolicy(capture.is_some());
    let add = |a, b| policy.numeric(add(a, b));
    let aligned = |a| policy.numeric(aligned(a));
    let mul = |a, b| policy.numeric(mul(a, b));
    let bit_bytes = |a| policy.numeric(bit_bytes(a));
    if add(start, rows)? > data.len() {
        return Err((invalid()).into());
    }
    let offset = add(data.offset(), start)?;
    if receiving.is_some_and(|profile| rows > profile.0) || i64::try_from(rows).is_err() {
        return Err((if policy.0 {
            novarocks_type_contract::CompileControlError::ResourceExhausted.into()
        } else {
            TypeCodecError::InvalidShape("flat pool writer row envelope exceeded")
        })
        .into());
    }
    let kind = layout(data.data_type())?;
    let variadic = if matches!(kind, Layout::Views) {
        data.buffers().len().checked_sub(1).ok_or_else(invalid)?
    } else {
        0
    };
    let buffers = match kind {
        Layout::Null => 0,
        Layout::Offsets(_) => 3,
        Layout::Views => add(2, variadic)?,
        _ => 2,
    };
    if receiving.is_some_and(|profile| buffers > profile.1) || i64::try_from(variadic).is_err() {
        return Err((if policy.0 {
            novarocks_type_contract::CompileControlError::ResourceExhausted.into()
        } else {
            TypeCodecError::InvalidShape("flat pool writer buffer envelope exceeded")
        })
        .into());
    }
    if capture.is_none() {
        work.step()?;
    }
    let mut result = Geometry {
        rows,
        buffers,
        variadic,
        views: matches!(kind, Layout::Views),
        body_bytes: 0,
        payload_bytes: 0,
        values_start: 0,
        values_bytes: 0,
        offset_base: 0,
    };
    if let Some(capture) = capture.as_deref_mut() {
        capture(&result)?;
        work.step()?;
    }
    let mut charge = |bytes: usize| -> Result<(), HostError<H>> {
        result.body_bytes = add(result.body_bytes, aligned(bytes)?)?;
        result.payload_bytes = add(result.payload_bytes, bytes)?;
        if receiving.is_some_and(|profile| result.body_bytes > profile.2)
            || i64::try_from(result.body_bytes).is_err()
        {
            return Err((if policy.0 {
                novarocks_type_contract::CompileControlError::ResourceExhausted.into()
            } else {
                TypeCodecError::InvalidShape("flat pool writer body envelope exceeded")
            })
            .into());
        }
        if let Some(capture) = capture.as_deref_mut() {
            capture(&result)?;
        }
        work.step()?;
        Ok(())
    };
    if !matches!(kind, Layout::Null) {
        charge(bit_bytes(rows)?)?;
    }
    match kind {
        Layout::Null => (),
        Layout::Bits => {
            let bytes = bit_bytes(rows)?;
            // The canonical source was already validated, but retain checked
            // range arithmetic so this projection never assumes a wrapping sum.
            let last = add(offset, rows)?;
            if bit_bytes(last)? > data.buffers()[0].len() {
                return Err((invalid()).into());
            }
            charge(bytes)?;
            result.values_bytes = bytes;
        }
        Layout::Fixed(width) => {
            let start = mul(offset, width)?;
            let bytes = mul(rows, width)?;
            if add(start, bytes)? > data.buffers()[0].len() {
                return Err((invalid()).into());
            }
            charge(bytes)?;
            result.values_start = start;
            result.values_bytes = bytes;
        }
        Layout::Offsets(width) => {
            let offset_bytes = mul(add(rows, 1)?, width)?;
            let (base, end) = if rows == 0 {
                (0, 0)
            } else {
                let source = data.buffers()[0].as_slice();
                (
                    native_offset(source, offset, width)?,
                    native_offset(source, add(offset, rows)?, width)?,
                )
            };
            let bytes = end.checked_sub(base).ok_or_else(invalid)?;
            if end > data.buffers()[1].len() {
                return Err((invalid()).into());
            }
            charge(offset_bytes)?;
            charge(bytes)?;
            result.values_start = base;
            result.offset_base = base;
            result.values_bytes = bytes;
        }
        Layout::Views => {
            let start = mul(offset, 16)?;
            let bytes = mul(rows, 16)?;
            if add(start, bytes)? > data.buffers()[0].len() {
                return Err((invalid()).into());
            }
            charge(bytes)?;
            for buffer in data.buffers().iter().skip(1) {
                charge(buffer.len())?;
            }
            result.values_start = start;
            result.values_bytes = bytes;
        }
    }
    work.step()?;
    Ok(result)
}

pub(crate) fn inspect_with_host_in<H>(
    pool: &ConstantPool,
    max_rows: usize,
    max_buffers: usize,
    max_body_bytes: usize,
    capture: &mut dyn FnMut(&Geometry) -> Result<(), HostError<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, HostError<H>> {
    inspect_span_core(
        pool.data(),
        0,
        pool.data().len(),
        max_rows,
        max_buffers,
        max_body_bytes,
        Some(capture),
        work,
    )
}
