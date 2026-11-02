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
//! One adjacent-subtraction author. Only arithmetic overflow is an explicit
//! row outcome; normalizers, format limits and SDK failures remain unmaskable.
use super::array_literal_core::CollectionObservation;
use super::collection_arrow::{self, ArrowSite};
use crate::KernelFailure;
use arrow_array::types::{ArrowPrimitiveType, Decimal128Type, Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, Decimal128Array, ListArray, PrimitiveArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_failure_contract::{FailureFact, FailureProjection, OuterFailureProjection};
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub enum Operation {
    Difference,
}
#[derive(Debug)]
pub enum DifferenceFailure<E> {
    Operation(KernelFailure),
    Caller(E),
}
impl<E> From<KernelFailure> for DifferenceFailure<E> {
    fn from(source: KernelFailure) -> Self {
        Self::Operation(source)
    }
}
/// The row belongs to this invocation's compact input, not the source batch.
#[derive(Debug)]
pub struct DifferenceRowError {
    pub row: usize,
}
impl std::fmt::Display for DifferenceRowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "array_difference integer subtraction overflow at row {}",
            self.row
        )
    }
}
impl std::error::Error for DifferenceRowError {}
impl FailureProjection for DifferenceRowError {
    fn failure_fact(&self) -> FailureFact {
        crate::DataFailureReason::NumericOutOfRange.failure_fact()
    }
}
// Explicit promotion is used only by the required invocation; the selected
// caller receives this nominal value outcome via its separate callback.
impl OuterFailureProjection for DifferenceRowError {}
fn reserve<T>(count: usize) -> Result<Vec<T>, KernelFailure> {
    let mut out = Vec::new();
    out.try_reserve_exact(count).map_err(|source| {
        KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
            "array_difference output",
            source,
        ))
    })?;
    Ok(out)
}
pub fn difference(
    arr: &ArrayRef,
    output_type: Option<&DataType>,
    chunk_len: usize,
) -> Result<ArrayRef, KernelFailure> {
    difference_observed(
        arr,
        output_type,
        chunk_len,
        &mut |_| Ok::<(), KernelFailure>(()),
        &mut |source| Err(KernelFailure::from_domain(source)),
    )
    .map_err(|source| match source {
        DifferenceFailure::Operation(source) | DifferenceFailure::Caller(source) => source,
    })
}
trait AdjacentValue: Copy {
    fn zero() -> Self;
    fn subtract(self, previous: Self) -> Option<Self>;
}
impl AdjacentValue for i64 {
    fn zero() -> Self {
        0
    }
    fn subtract(self, previous: Self) -> Option<Self> {
        self.checked_sub(previous)
    }
}
impl AdjacentValue for i128 {
    fn zero() -> Self {
        0
    }
    fn subtract(self, previous: Self) -> Option<Self> {
        self.checked_sub(previous)
    }
}
impl AdjacentValue for f64 {
    fn zero() -> Self {
        0.0
    }
    fn subtract(self, previous: Self) -> Option<Self> {
        Some(self - previous)
    }
}
struct OutputRows<E> {
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
    observe: E,
}
fn subtract_rows<T: ArrowPrimitiveType, E>(
    list: &ListArray,
    values: &PrimitiveArray<T>,
    rows: usize,
    total: usize,
    state: &mut OutputRows<&mut dyn FnMut(CollectionObservation) -> Result<(), E>>,
    on_row_error: &mut dyn FnMut(DifferenceRowError) -> Result<(), E>,
) -> Result<PrimitiveArray<T>, DifferenceFailure<E>>
where
    T::Native: AdjacentValue,
    PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
{
    let mut out = reserve::<Option<T::Native>>(total)?;
    for row in 0..rows {
        (state.observe)(CollectionObservation::Step).map_err(DifferenceFailure::Caller)?;
        let index = if list.len() == 1 { 0 } else { row };
        if list.is_null(index) {
            state.offsets.push(out.len() as i32);
            state.nulls.append_null();
            continue;
        }
        let start = list.value_offsets()[index] as usize;
        let end = list.value_offsets()[index + 1] as usize;
        let output_start = out.len();
        let mut failed = false;
        for index in start..end {
            (state.observe)(CollectionObservation::Step).map_err(DifferenceFailure::Caller)?;
            let value = if values.is_null(index) {
                None
            } else if index == start {
                Some(T::Native::zero())
            } else if values.is_null(index - 1) {
                None
            } else {
                let Some(value) = values.value(index).subtract(values.value(index - 1)) else {
                    out.truncate(output_start);
                    on_row_error(DifferenceRowError { row }).map_err(DifferenceFailure::Caller)?;
                    failed = true;
                    break;
                };
                Some(value)
            };
            out.push(value);
        }
        state.offsets.push(out.len() as i32);
        state.nulls.append(!failed);
    }
    Ok(PrimitiveArray::<T>::from(out))
}
pub fn difference_observed<E>(
    arr: &ArrayRef,
    output_type: Option<&DataType>,
    rows: usize,
    observe: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    on_row_error: &mut dyn FnMut(DifferenceRowError) -> Result<(), E>,
) -> Result<ArrayRef, DifferenceFailure<E>> {
    observe(CollectionObservation::OpaqueBoundary).map_err(DifferenceFailure::Caller)?;
    let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        collection_arrow::contract("array_difference requires its prepared List carrier")
    })?;
    if list.len() != rows && list.len() != 1 {
        return Err(collection_arrow::contract(
            "array_difference input length differs from invocation",
        )
        .into());
    }
    let field = match output_type {
        Some(DataType::List(field)) => field.clone(),
        Some(_) => {
            return Err(collection_arrow::contract("array_difference output must be List").into());
        }
        None => match list.data_type() {
            DataType::List(field) => field.clone(),
            _ => {
                return Err(collection_arrow::contract(
                    "array_difference source declaration differs",
                )
                .into());
            }
        },
    };
    let entries = collection_arrow::list_offset_entries(rows)?;
    let mut total = 0i32;
    for row in 0..rows {
        observe(CollectionObservation::Step).map_err(DifferenceFailure::Caller)?;
        let index = if list.len() == 1 { 0 } else { row };
        if !list.is_null(index) {
            let count = (list.value_offsets()[index + 1] - list.value_offsets()[index]) as usize;
            total = crate::aggregate_scalar::checked_list_output_offset(total, count)?;
        }
    }
    let values = if list.values().data_type() == field.data_type() {
        list.values().clone()
    } else if let (DataType::Decimal128(_, scale), DataType::Float64) =
        (list.values().data_type(), field.data_type())
    {
        // Preserve the original complete child conversion, including hidden
        // NULL payloads. Parent selection does not change this author's demand.
        observe(CollectionObservation::OpaqueBoundary).map_err(DifferenceFailure::Caller)?;
        let source = list
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| {
                collection_arrow::contract("array_difference decimal carrier differs")
            })?;
        Arc::new(crate::decimal_float_cast::decimal128_array_to_f64(
            source, *scale,
        )) as ArrayRef
    } else {
        observe(CollectionObservation::OpaqueBoundary).map_err(DifferenceFailure::Caller)?;
        arrow_cast::cast(list.values(), field.data_type()).map_err(|source| {
            collection_arrow::arrow_failure("array_difference", ArrowSite::Cast, source)
        })?
    };
    let mut state = OutputRows {
        offsets: reserve::<i32>(entries)?,
        nulls: NullBufferBuilder::new(rows),
        observe,
    };
    state.offsets.push(0);
    macro_rules! subtract {
        ($ty:ty) => {{
            let values = values
                .as_any()
                .downcast_ref::<PrimitiveArray<$ty>>()
                .ok_or_else(|| {
                    collection_arrow::contract(
                        "array_difference prepared primitive carrier differs",
                    )
                })?;
            subtract_rows::<$ty, E>(list, values, rows, total as usize, &mut state, on_row_error)?
        }};
    }
    let output: ArrayRef = match field.data_type() {
        DataType::Int64 => Arc::new(subtract!(Int64Type)),
        DataType::Float64 => Arc::new(subtract!(Float64Type)),
        DataType::Decimal128(precision, scale) => Arc::new(
            subtract!(Decimal128Type)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|source| {
                    collection_arrow::arrow_failure(
                        "array_difference decimal",
                        ArrowSite::Construct,
                        source,
                    )
                })?,
        ),
        _ => {
            return Err(collection_arrow::contract(
                "array_difference unsupported output element type",
            )
            .into());
        }
    };
    (state.observe)(CollectionObservation::OpaqueBoundary).map_err(DifferenceFailure::Caller)?;
    let output = ListArray::try_new(
        field,
        OffsetBuffer::new(state.offsets.into()),
        output,
        state.nulls.finish(),
    )
    .map_err(|source| {
        collection_arrow::arrow_failure("array_difference", ArrowSite::Construct, source)
    })?;
    (state.observe)(CollectionObservation::OpaqueBoundary).map_err(DifferenceFailure::Caller)?;
    Ok(Arc::new(output))
}
