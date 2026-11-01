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

//! Representation facts for the original safe Arrow primitive default cast.
//! Conversion remains the one cast call in the complete-partition owner.

use crate::KernelFailure;
use crate::kernel_control::{compile_failure, invalid};
use arrow_array::{
    Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
use arrow_schema::DataType;
use novarocks_type_contract::{CompileCheckpoints, FunctionValueType, ValueLogicalType};
use std::alloc::Layout;

#[derive(Debug)]
pub(super) struct NumericDefaultRecipe {
    source: DataType,
    target: DataType,
}

impl NumericDefaultRecipe {
    /// None keeps the nonnumeric conversion path; Some(false) is the original
    /// unsupported nominal/nonnullable numeric profile.
    pub(super) fn profile_supported(
        source: &FunctionValueType,
        target: &FunctionValueType,
    ) -> Option<bool> {
        (width(source.data_type()).is_some() && width(target.data_type()).is_some()).then(|| {
            source.logical_type() == ValueLogicalType::Physical
                && target.logical_type() == ValueLogicalType::Physical
                && target.nullable()
        })
    }

    /// The caller owns type validation, the original checkpoint scope and its
    /// ordinary tail. This leaf neither converts nor retains a second FVT.
    pub(super) fn try_new(
        source: &FunctionValueType,
        target: &FunctionValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<Self>, KernelFailure> {
        let profile = Self::profile_supported(source, target);
        work.step().map_err(compile_failure)?;
        let Some(valid) = profile else {
            return Ok(None);
        };
        work.step().map_err(compile_failure)?;
        if !valid {
            return Err(invalid(
                "window numeric default requires Physical carriers and a nullable result",
            ));
        }
        let recipe = Self {
            source: source.data_type().clone(),
            target: target.data_type().clone(),
        };
        work.step().map_err(compile_failure)?;
        Ok(Some(recipe))
    }

    pub(super) fn source_class(&self, array: &dyn Array) -> bool {
        match self.source {
            DataType::Int8 => array.as_any().is::<Int8Array>(),
            DataType::Int16 => array.as_any().is::<Int16Array>(),
            DataType::Int32 => array.as_any().is::<Int32Array>(),
            DataType::Int64 => array.as_any().is::<Int64Array>(),
            DataType::Float32 => array.as_any().is::<Float32Array>(),
            DataType::Float64 => array.as_any().is::<Float64Array>(),
            _ => false,
        }
    }

    pub(super) fn preflight_output(&self, rows: usize) -> Result<(), KernelFailure> {
        Self::preflight_target(&self.target, rows)
    }

    pub(super) fn preflight_target(target: &DataType, rows: usize) -> Result<(), KernelFailure> {
        buffer_extents(target, rows).map(|_| ())
    }

    /// The returned primitive array's memory metric, including its concrete
    /// header and both buffer capacities. This excludes Arc/Bytes headers,
    /// gathered input, temporary coexistence and any host allocation grant.
    pub(super) fn retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure> {
        Self::retained_target_upper_bound(&self.target, rows)
    }

    pub(super) fn retained_target_upper_bound(
        target: &DataType,
        rows: usize,
    ) -> Result<usize, KernelFailure> {
        let (values, bitmap) = buffer_extents(target, rows)?;
        let header = match target {
            DataType::Int8 => std::mem::size_of::<Int8Array>(),
            DataType::Int16 => std::mem::size_of::<Int16Array>(),
            DataType::Int32 => std::mem::size_of::<Int32Array>(),
            DataType::Int64 => std::mem::size_of::<Int64Array>(),
            DataType::Float32 => std::mem::size_of::<Float32Array>(),
            DataType::Float64 => std::mem::size_of::<Float64Array>(),
            _ => return Err(invalid("window numeric default output carrier differs")),
        };
        header
            .checked_add(values)
            .and_then(|n| n.checked_add(bitmap))
            .ok_or(KernelFailure::ResourceExhausted)
    }
}

fn width(ty: &DataType) -> Option<usize> {
    match ty {
        DataType::Int8 => Some(1),
        DataType::Int16 => Some(2),
        DataType::Int32 | DataType::Float32 => Some(4),
        DataType::Int64 | DataType::Float64 => Some(8),
        _ => None,
    }
}

fn buffer_extents(ty: &DataType, rows: usize) -> Result<(usize, usize), KernelFailure> {
    let width =
        width(ty).ok_or_else(|| invalid("window numeric default output carrier differs"))?;
    let values = rows
        .checked_mul(width)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .ok_or(KernelFailure::ResourceExhausted)?
        / 8;
    // Arrow60.1.0 PrimitiveArray::unary_opt uses an exact native Vec for N
    // values and BooleanBufferBuilder for N validity bits. Rounding both
    // extents to 64 remains a conservative retained-byte upper, not the
    // actual values allocation layout. Identity reuses the gathered buffers.
    Ok((rounded_buffer(values)?, rounded_buffer(bitmap)?))
}

fn rounded_buffer(bytes: usize) -> Result<usize, KernelFailure> {
    let rounded = bytes
        .checked_add(63)
        .ok_or(KernelFailure::ResourceExhausted)?
        & !63;
    Layout::from_size_align(rounded, 64).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(rounded)
}

#[cfg(test)]
#[path = "window_default_numeric_tests.rs"]
mod tests;
