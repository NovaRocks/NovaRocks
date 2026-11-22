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

//! Exact selected CRC32 for the installed physical Utf8 profile.
//! Output capacity checks do not replace formal host memory admission.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
    builder::Int64Builder,
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;

use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

pub(super) fn evaluate_crc32<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let ([FunctionArgumentType::Value(source)], [argument]) = (
        input.contract().selected().argument_types.as_ref(),
        input.arguments(),
    ) else {
        return Err(invalid("crc32 requires exactly one checked value argument"));
    };
    let target = input.contract().result_type();
    if source.logical_type() != ValueLogicalType::Physical
        || source.data_type() != &DataType::Utf8
        || target.logical_type() != ValueLogicalType::Physical
        || target.data_type() != &DataType::Int64
        || !target.nullable()
    {
        return Err(invalid("crc32 differs from its exact installed profile"));
    }
    if argument.array().data_type() != &DataType::Utf8 {
        return Err(internal("crc32 carrier differs from its checked argument"));
    }
    argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal("crc32 selected Utf8 carrier cannot be downcast"))?;
    let selection = input.selection();
    output_capacity(selection.len())?;
    let output = evaluate_bytes(*argument, selection, source.nullable(), true, control)?;
    SelectedValues::try_new(selection, target.data_type(), output, Box::default())
        .map_err(|_| internal("crc32 compact output violates its selected contract"))
}

#[derive(Debug)]
struct CrcFailure(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for CrcFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for CrcFailure {}
impl novarocks_failure_contract::FailureProjection for CrcFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(360).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CrcFailure {}
fn carrier_failure(detail: impl std::fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(CrcFailure(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
enum Bytes<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
}
impl<'a> Bytes<'a> {
    fn from_array(array: &'a ArrayRef) -> Result<Self, KernelFailure> {
        let failure = |message: &str| carrier_failure(message);
        match array.data_type() {
            DataType::Utf8 => array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(Self::Utf8)
                .ok_or_else(|| failure("downcast StringArray failed")),
            DataType::LargeUtf8 => array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(Self::LargeUtf8)
                .ok_or_else(|| failure("downcast LargeStringArray failed")),
            DataType::Binary => array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .map(Self::Binary)
                .ok_or_else(|| failure("downcast BinaryArray failed")),
            DataType::LargeBinary => array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .map(Self::LargeBinary)
                .ok_or_else(|| failure("downcast LargeBinaryArray failed")),
            other => Err(carrier_failure(format_args!(
                "crc32 expects VARCHAR/BINARY input, got {:?}",
                other
            ))),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Utf8(a) => a.len(),
            Self::LargeUtf8(a) => a.len(),
            Self::Binary(a) => a.len(),
            Self::LargeBinary(a) => a.len(),
        }
    }
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(a) => a.is_null(row),
            Self::LargeUtf8(a) => a.is_null(row),
            Self::Binary(a) => a.is_null(row),
            Self::LargeBinary(a) => a.is_null(row),
        }
    }
    fn value(&self, row: usize) -> &[u8] {
        match self {
            Self::Utf8(a) => a.value(row).as_bytes(),
            Self::LargeUtf8(a) => a.value(row).as_bytes(),
            Self::Binary(a) => a.value(row),
            Self::LargeBinary(a) => a.value(row),
        }
    }
}
/// ONE original byte calculation and Int64 assembly for full and selected calls.
fn evaluate_bytes(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    nullable: bool,
    selected: bool,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    // Reject an invalid physical carrier before constructing result storage.
    let values = Bytes::from_array(argument.array())?;
    let mut builder = Int64Builder::with_capacity(selection.len());
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, batch_row);
        if selected && row >= values.len() {
            return Err(internal(
                "crc32 selected row is outside its checked carrier",
            ));
        }
        if values.is_null(row) {
            if !nullable {
                return Err(internal(
                    "crc32 non-null argument contains selected SQL NULL",
                ));
            }
            builder.append_null();
        } else {
            builder.append_value(crc32_zlib_observed(values.value(row), &mut work)? as i64);
        }
    }
    let output = Arc::new(builder.finish()) as ArrayRef;
    work.finish()?;
    Ok(output)
}
/// Direct arguments retain the same IEEE calculation and physical byte carriers.
pub fn evaluate_required(input: &ArrayRef) -> Result<ArrayRef, KernelFailure> {
    output_capacity(input.len())?;
    evaluate_bytes(
        EvaluatedArgument::Column(input),
        Selection::all(input.len()),
        true,
        false,
        &DirectControl,
    )
}
struct DirectControl;
impl KernelEvaluationControl for DirectControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(internal("direct CRC32 calculation must not wait"))
    }
}

pub(super) fn crc32_zlib_observed(
    data: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<u32, KernelFailure> {
    let mut crc = 0xffff_ffff_u32;
    for &byte in data {
        work.step()?;
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xedb8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    Ok(crc ^ 0xffff_ffff)
}

/// Allocation representability only; this does not authorize host memory.
fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = rows
        .checked_mul(8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    isize::try_from(values).map_err(|_| KernelFailure::ResourceExhausted)?;
    isize::try_from(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

#[cfg(test)]
#[path = "crc32_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "crc32_shared_tests.rs"]
mod shared_tests;
