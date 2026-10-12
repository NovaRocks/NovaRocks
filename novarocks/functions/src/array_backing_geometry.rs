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

//! Borrow actual immutable Arrow structure before its allocating to_data route.
//! These are table facts, not a value decoder, retained payload grant, SQL type
//! coercion, or a row-dependent string estimate.
use crate::KernelFailure;
use crate::kernel_control::invalid;
use crate::kernel_input::EvaluationCheckpoints;
use crate::source_custody_resources::{
    RequestSourceNode, RequestSummary, SourceCustodyRequestFacts, SourceNodeRole,
    SourceRequestPricing, summarize_source_node,
};
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, BinaryViewArray, DictionaryArray, FixedSizeListArray, LargeListArray,
    LargeListViewArray, ListArray, ListViewArray, MapArray, RunArray, StringViewArray, StructArray,
    UnionArray,
};
use arrow_schema::{DataType, UnionMode};
use std::alloc::Layout;

fn concrete<T: Array + 'static>(array: &dyn Array) -> Result<&T, KernelFailure> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        invalid("source backing geometry requires its actual built-in Arrow carrier")
    })
}
fn plus(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}

/// A borrowed node from the SAME source traversal. No ArrayData, payload,
/// metadata clone, or authorization is created by this loan.
pub(crate) struct BorrowedSourceNode<'a> {
    pub(crate) array: &'a dyn Array,
    pub(crate) data_buffers: usize,
    pub(crate) child_count: usize,
    pub(crate) copy_nulls: bool,
}
pub(crate) trait BorrowedSourceObservation<'source> {
    /// The observer owns any work or allocation it elects to perform. The
    /// source author adds neither a scope nor an extra success checkpoint.
    fn node(
        &mut self,
        node: BorrowedSourceNode<'source>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure>;
}

struct SourceGraphResult {
    legacy: Option<usize>,
    requests: Option<RequestSummary>,
    bufferless_root: Option<bool>,
}

fn actual_rebox_layout(array: &dyn Array) -> Result<Layout, KernelFailure> {
    use arrow_array::types::*;
    use arrow_array::*;
    use arrow_schema::{IntervalUnit, TimeUnit};
    let actual = array.as_any();
    macro_rules! primitive {
        ($ty:ty) => {
            actual.is::<PrimitiveArray<$ty>>()
        };
    }
    let valid = match array.data_type() {
        DataType::Null => actual.is::<NullArray>(),
        DataType::Boolean => actual.is::<BooleanArray>(),
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::UInt8 => primitive!(UInt8Type),
        DataType::UInt16 => primitive!(UInt16Type),
        DataType::UInt32 => primitive!(UInt32Type),
        DataType::UInt64 => primitive!(UInt64Type),
        DataType::Float16 => primitive!(Float16Type),
        DataType::Float32 => primitive!(Float32Type),
        DataType::Float64 => primitive!(Float64Type),
        DataType::Date32 => primitive!(Date32Type),
        DataType::Date64 => primitive!(Date64Type),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondType),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondType),
        DataType::Time32(_) | DataType::Time64(_) => false,
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondType),
        DataType::Duration(TimeUnit::Second) => primitive!(DurationSecondType),
        DataType::Duration(TimeUnit::Millisecond) => primitive!(DurationMillisecondType),
        DataType::Duration(TimeUnit::Microsecond) => primitive!(DurationMicrosecondType),
        DataType::Duration(TimeUnit::Nanosecond) => primitive!(DurationNanosecondType),
        DataType::Interval(IntervalUnit::YearMonth) => primitive!(IntervalYearMonthType),
        DataType::Interval(IntervalUnit::DayTime) => primitive!(IntervalDayTimeType),
        DataType::Interval(IntervalUnit::MonthDayNano) => primitive!(IntervalMonthDayNanoType),
        DataType::Decimal32(..) => primitive!(Decimal32Type),
        DataType::Decimal64(..) => primitive!(Decimal64Type),
        DataType::Decimal128(..) => primitive!(Decimal128Type),
        DataType::Decimal256(..) => primitive!(Decimal256Type),
        DataType::Utf8 => actual.is::<StringArray>(),
        DataType::LargeUtf8 => actual.is::<LargeStringArray>(),
        DataType::Binary => actual.is::<BinaryArray>(),
        DataType::LargeBinary => actual.is::<LargeBinaryArray>(),
        DataType::Utf8View => actual.is::<StringViewArray>(),
        DataType::BinaryView => actual.is::<BinaryViewArray>(),
        DataType::FixedSizeBinary(_) => actual.is::<FixedSizeBinaryArray>(),
        DataType::List(_) => actual.is::<ListArray>(),
        DataType::LargeList(_) => actual.is::<LargeListArray>(),
        DataType::ListView(_) => actual.is::<ListViewArray>(),
        DataType::LargeListView(_) => actual.is::<LargeListViewArray>(),
        DataType::FixedSizeList(..) => actual.is::<FixedSizeListArray>(),
        DataType::Struct(_) => actual.is::<StructArray>(),
        DataType::Map(..) => actual.is::<MapArray>(),
        // These branches already verify the precise key/index carrier in the
        // SAME child traversal below; as_any exposes its delegated header.
        DataType::Dictionary(..) | DataType::RunEndEncoded(..) => true,
        DataType::Union(..) => actual.is::<UnionArray>(),
    };
    if !valid {
        return Err(invalid(
            "source custody requires its actual built-in Arrow carrier",
        ));
    }
    Ok(Layout::for_value(actual))
}

/// Use ONE custody metadata arithmetic with borrowed actual table counts. The
/// returned bytes cover metadata construction only; existing input lease owners
/// retain the original payload independently and are never replaced by this sum.
fn source_metadata_graph<'source>(
    array: &'source dyn Array,
    work: &mut EvaluationCheckpoints<'_>,
    copy_nulls: bool,
    observer: &mut Option<&mut dyn BorrowedSourceObservation<'source>>,
    pricing: Option<&SourceRequestPricing>,
    role: SourceNodeRole,
) -> Result<SourceGraphResult, KernelFailure> {
    work.step()?;
    // Do not run unused legacy envelope arithmetic on the request route.
    let mut child_bytes = pricing.is_none().then_some(0usize);
    let mut child_requests = pricing.map(|_| RequestSummary::empty());
    let body = if pricing.is_some() {
        Some(actual_rebox_layout(array)?)
    } else {
        None
    };
    let buffers;
    let children;
    macro_rules! accumulate {
        ($result:expr) => {{
            let result = $result;
            if let Some(bytes) = &mut child_bytes {
                *bytes = plus(
                    *bytes,
                    result.legacy.expect("legacy source traversal amount"),
                )?;
            }
            if let Some(requests) = &mut child_requests {
                *requests =
                    requests.plus(result.requests.expect("source request traversal summary"))?;
            }
        }};
    }
    macro_rules! child {
        ($array:expr) => {{
            accumulate!(source_metadata_graph(
                $array.as_ref(),
                work,
                copy_nulls,
                observer,
                pricing,
                SourceNodeRole::Rebox,
            )?);
        }};
    }
    macro_rules! dictionary {
        ($key:ty) => {{
            child!(concrete::<DictionaryArray<$key>>(array)?.values());
        }};
    }
    match array.data_type() {
        DataType::Null => {
            buffers = 0;
            children = 0;
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            buffers = 2;
            children = 0;
        }
        DataType::Utf8View => {
            buffers = plus(1, concrete::<StringViewArray>(array)?.data_buffers().len())?;
            children = 0;
        }
        DataType::BinaryView => {
            buffers = plus(1, concrete::<BinaryViewArray>(array)?.data_buffers().len())?;
            children = 0;
        }
        DataType::List(_) => {
            buffers = 1;
            children = 1;
            child!(concrete::<ListArray>(array)?.values());
        }
        DataType::LargeList(_) => {
            buffers = 1;
            children = 1;
            child!(concrete::<LargeListArray>(array)?.values());
        }
        DataType::ListView(_) => {
            buffers = 2;
            children = 1;
            child!(concrete::<ListViewArray>(array)?.values());
        }
        DataType::LargeListView(_) => {
            buffers = 2;
            children = 1;
            child!(concrete::<LargeListViewArray>(array)?.values());
        }
        DataType::FixedSizeList(_, _) => {
            buffers = 0;
            children = 1;
            child!(concrete::<FixedSizeListArray>(array)?.values());
        }
        DataType::Struct(_) => {
            let columns = concrete::<StructArray>(array)?.columns();
            buffers = 0;
            children = columns.len();
            for column in columns {
                child!(column);
            }
        }
        DataType::Map(_, _) => {
            buffers = 1;
            children = 1;
            accumulate!(source_metadata_graph(
                concrete::<MapArray>(array)?.entries(),
                work,
                copy_nulls,
                observer,
                pricing,
                SourceNodeRole::MapEntries,
            )?);
        }
        DataType::Dictionary(key, _) => {
            buffers = 1;
            children = 1;
            match key.as_ref() {
                DataType::Int8 => dictionary!(Int8Type),
                DataType::Int16 => dictionary!(Int16Type),
                DataType::Int32 => dictionary!(Int32Type),
                DataType::Int64 => dictionary!(Int64Type),
                DataType::UInt8 => dictionary!(UInt8Type),
                DataType::UInt16 => dictionary!(UInt16Type),
                DataType::UInt32 => dictionary!(UInt32Type),
                DataType::UInt64 => dictionary!(UInt64Type),
                _ => return Err(invalid("source dictionary has no actual Arrow key carrier")),
            }
        }
        DataType::Union(fields, mode) => {
            let array = concrete::<UnionArray>(array)?;
            buffers = if *mode == UnionMode::Dense { 2 } else { 1 };
            children = fields.len();
            for (id, _) in fields.iter() {
                child!(array.child(id));
            }
        }
        DataType::RunEndEncoded(ends, _) => {
            buffers = 0;
            children = 2;
            // Arrow's run-end child is a primitive array with one data buffer
            // and no null bitmap. Values use their own actual dynamic structure.
            if let Some(bytes) = &mut child_bytes {
                *bytes =
                    crate::arrow_result_custody::custody_node_metadata_upper_bound(1, false, 0)?;
            }
            if let (Some(pricing), Some(requests)) = (pricing, child_requests.as_mut()) {
                // This implicit primitive child is part of the original Run
                // construction, not a temporary Array or additional decoder.
                let ends = summarize_source_node(
                    RequestSourceNode {
                        data_type: ends.data_type(),
                        data_buffers: 1,
                        child_count: 0,
                        nulls: None,
                        header_fields: 0,
                        body: None,
                        role: SourceNodeRole::RunEnds,
                    },
                    RequestSummary::empty(),
                    pricing,
                    work,
                )?;
                *requests = requests.plus(ends)?;
            }
            match ends.data_type() {
                DataType::Int16 => child!(concrete::<RunArray<Int16Type>>(array)?.values()),
                DataType::Int32 => child!(concrete::<RunArray<Int32Type>>(array)?.values()),
                DataType::Int64 => child!(concrete::<RunArray<Int64Type>>(array)?.values()),
                _ => return Err(invalid("source run-end has no actual Arrow index carrier")),
            }
        }
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(..)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(..)
        | DataType::Time64(..)
        | DataType::Duration(..)
        | DataType::Interval(..)
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => {
            buffers = 1;
            children = 0;
        }
    }
    if let Some(observer) = observer.as_deref_mut() {
        observer.node(
            BorrowedSourceNode {
                array,
                data_buffers: buffers,
                child_count: children,
                copy_nulls,
            },
            work,
        )?;
    }
    let legacy = match child_bytes {
        Some(child_bytes) => {
            let own = crate::arrow_result_custody::custody_typed_node_metadata_upper_bound(
                array.data_type(),
                buffers,
                copy_nulls || array.nulls().is_some(),
                children,
            )?;
            Some(plus(own, child_bytes)?)
        }
        None => None,
    };
    let requests = match (pricing, child_requests) {
        (Some(pricing), Some(child_requests)) => {
            let header_fields = match array.data_type() {
                DataType::Map(..) => concrete::<MapArray>(array)?.entries().columns().len(),
                _ => 0,
            };
            Some(summarize_source_node(
                RequestSourceNode {
                    data_type: array.data_type(),
                    data_buffers: buffers,
                    child_count: children,
                    nulls: array.nulls(),
                    header_fields,
                    body,
                    role,
                },
                child_requests,
                pricing,
                work,
            )?)
        }
        (None, None) => None,
        _ => {
            return Err(invalid(
                "source custody traversal has inconsistent request premises",
            ));
        }
    };
    let bufferless_root = pricing.map(|_| {
        let effective_nulls = array.nulls().is_some_and(|nulls| nulls.null_count() != 0);
        buffers == 0 && children == 0 && !effective_nulls
    });
    Ok(SourceGraphResult {
        legacy,
        requests,
        bufferless_root,
    })
}

pub(crate) fn source_metadata_bytes(
    array: &dyn Array,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    // Four envelopes cover pinned Arrow to_data transient table growth,
    // clone/into_builder/make_array coexistence, and custom-buffer metadata.
    // Apply it ONCE to the complete actual graph, not exponentially by depth.
    source_metadata_graph(array, work, false, &mut None, None, SourceNodeRole::Rebox)?
        .legacy
        .expect("legacy source traversal amount")
        .checked_mul(4)
        .ok_or(KernelFailure::ResourceExhausted)
}

/// The SAME actual source graph bounds copy metadata. Any copied node may gain
/// validity from actual nullable indices even when its source has no bitmap.
/// Dictionary/View children remain source-owned; counting their metadata does
/// not admit or charge their already-owned payload.
pub(crate) fn copy_metadata_bytes(
    array: &dyn Array,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    source_metadata_graph(array, work, true, &mut None, None, SourceNodeRole::Rebox)?
        .legacy
        .expect("legacy copy traversal amount")
        .checked_mul(4)
        .ok_or(KernelFailure::ResourceExhausted)
}

/// Explicit resource observation borrows the existing parent work. Legacy
/// entrypoints keep their original None path and original checkpoint trace.
pub(crate) fn observe_source_metadata<'source>(
    array: &'source dyn Array,
    copy_nulls: bool,
    work: &mut EvaluationCheckpoints<'_>,
    observer: &mut dyn BorrowedSourceObservation<'source>,
) -> Result<usize, KernelFailure> {
    source_metadata_graph(
        array,
        work,
        copy_nulls,
        &mut Some(observer),
        None,
        SourceNodeRole::Rebox,
    )?
    .legacy
    .expect("legacy observed traversal amount")
    .checked_mul(4)
    .ok_or(KernelFailure::ResourceExhausted)
}

#[cfg(test)]
#[path = "array_backing_geometry_observation_tests.rs"]
mod observation_tests;

pub(crate) fn source_request_facts(
    array: &dyn Array,
    pricing: &SourceRequestPricing,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<SourceCustodyRequestFacts, KernelFailure> {
    let result = source_metadata_graph(
        array,
        work,
        false,
        &mut None,
        Some(pricing),
        SourceNodeRole::Rebox,
    )?;
    result
        .requests
        .expect("source request traversal summary")
        .finish(
            pricing,
            result.bufferless_root.expect("source request root facts"),
        )
}
