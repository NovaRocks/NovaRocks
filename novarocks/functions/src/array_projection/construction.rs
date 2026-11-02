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

//! Arrow 60.1.0 locked-operation logical observations. Private Bytes owners,
//! ArrayData reconstruction and validation scratch are explicitly upper bounds,
//! never exact Layout invoices or retained foreign backing estimates.
//! Source anchors: arrow-select/take.rs::{take_bytes,take_nulls};
//! arrow-buffer/buffer/immutable.rs::Buffer::from_vec;
//! arrow-array/array/{struct_array,map_array,union_array,run_array}.rs;
//! arrow-data/data.rs::{ArrayData,ArrayDataBuilder}. The shared private-buffer
//! owner bound is the existing root_array_storage contract, not a new mirror.

use super::{ArrayNormalizationStep, ArrayProjectionFailure, ArrayProjectionViolation};

fn dictionary_values(source: &dyn Array) -> Option<&ArrayRef> {
    use arrow_array::types::{
        Int8Type, Int16Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    };
    macro_rules! values {
        ($key:ty) => {
            source
                .as_any()
                .downcast_ref::<DictionaryArray<$key>>()
                .map(DictionaryArray::values)
        };
    }
    let DataType::Dictionary(key, _) = source.data_type() else {
        return None;
    };
    match key.as_ref() {
        DataType::Int8 => values!(Int8Type),
        DataType::Int16 => values!(Int16Type),
        DataType::Int32 => values!(Int32Type),
        DataType::Int64 => values!(Int64Type),
        DataType::UInt8 => values!(UInt8Type),
        DataType::UInt16 => values!(UInt16Type),
        DataType::UInt32 => values!(UInt32Type),
        DataType::UInt64 => values!(UInt64Type),
        _ => None,
    }
}
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, DictionaryArray};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, FieldRef};
use std::alloc::Layout;

/// Original private immutable Buffer-owner bound, pinned by Execution's
/// counting-allocator root_capacity_introspection regression (pool disabled).
/// This is an opaque bound; it grants no capacity and is not an exact Layout.
pub const ARROW_BUFFER_OWNER_METADATA_BOUND: usize = 96;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArrayProjectionOpaqueBound {
    pub requests_upper: usize,
    pub bytes_upper: usize,
    pub work_upper: usize,
}
type Request<'a, E> = dyn FnMut(Layout) -> Result<(), E> + 'a;
type Opaque<'a, E> = dyn FnMut(ArrayProjectionOpaqueBound) -> Result<(), E> + 'a;
type Work<'a, E> = dyn FnMut(usize) -> Result<(), E> + 'a;

fn overflow() -> ArrayProjectionFailure {
    ArrayProjectionFailure::RequestArithmetic
}
fn add(a: usize, b: usize) -> Result<usize, ArrayProjectionFailure> {
    a.checked_add(b).ok_or_else(overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, ArrayProjectionFailure> {
    a.checked_mul(b).ok_or_else(overflow)
}
fn array<T, E: From<ArrayProjectionFailure>>(
    n: usize,
    request: &mut Request<'_, E>,
) -> Result<(), E> {
    if n != 0 {
        request(
            Layout::array::<T>(n).map_err(|source| ArrayProjectionFailure::ArrayLayout {
                count: n,
                width: std::mem::size_of::<T>(),
                source,
            })?,
        )?;
    }
    Ok(())
}
fn arc<T, E: From<ArrayProjectionFailure>>(request: &mut Request<'_, E>) -> Result<(), E> {
    request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
            .map_err(|error| E::from(ArrayProjectionFailure::Layout(error)))?,
    )
}
fn bitmap_bound(rows: usize) -> Result<usize, ArrayProjectionFailure> {
    // MutableBuffer's locked source rounds a nonzero bitmap payload to 64 B.
    let bytes = add(rows, 7)? / 8;
    mul(add(bytes, 63)? / 64, 64)
}
fn clone_parameters<E: From<ArrayProjectionFailure>>(
    ty: &DataType,
    request: &mut Request<'_, E>,
    work: &mut Work<'_, E>,
    depth: usize,
) -> Result<(), E> {
    work(1)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(E::from(ArrayProjectionFailure::ValueType(
            novarocks_type_contract::ValueTypeError::TooDeep,
        )));
    }
    if let DataType::Dictionary(key, value) = ty {
        request(Layout::new::<DataType>())?;
        request(Layout::new::<DataType>())?;
        clone_parameters(key, request, work, depth + 1)?;
        clone_parameters(value, request, work, depth + 1)?;
    }
    // Other container/scalar parameters share Arc-owned backing on DataType clone.
    Ok(())
}

pub fn observe_array_normalization_construction<E: From<ArrayProjectionFailure>>(
    step: ArrayNormalizationStep<'_>,
    request: &mut Request<'_, E>,
    opaque: &mut Opaque<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<(), E> {
    match step {
        ArrayNormalizationStep::ValueWork { units } => work(units),
        ArrayNormalizationStep::ValueStructure { event, .. } => {
            use novarocks_value_arrays::{ByteLayout, control::ValueStructure};
            match event {
                ValueStructure::SharedOwner(layout) => request(layout),
                ValueStructure::ByteArray {
                    layout,
                    new_buffer_owners,
                } => {
                    match layout {
                        ByteLayout::Utf8 => arc::<arrow_array::StringArray, E>(request)?,
                        ByteLayout::LargeUtf8 => arc::<arrow_array::LargeStringArray, E>(request)?,
                        ByteLayout::Binary => arc::<arrow_array::BinaryArray, E>(request)?,
                        ByteLayout::LargeBinary => {
                            arc::<arrow_array::LargeBinaryArray, E>(request)?
                        }
                        ByteLayout::Utf8View => arc::<arrow_array::StringViewArray, E>(request)?,
                        ByteLayout::BinaryView => arc::<arrow_array::BinaryViewArray, E>(request)?,
                    }
                    opaque(ArrayProjectionOpaqueBound {
                        requests_upper: new_buffer_owners,
                        bytes_upper: mul(new_buffer_owners, ARROW_BUFFER_OWNER_METADATA_BOUND)?,
                        work_upper: new_buffer_owners,
                    })
                }
            }
        }

        ArrayNormalizationStep::OpaqueOperationFinished => work(0),
        ArrayNormalizationStep::InspectType(_) => work(1),
        ArrayNormalizationStep::InspectConstructionWork { units } => work(units),
        ArrayNormalizationStep::InspectField { source, name } => {
            work(add(add(source.name().len(), name.len())?, 1)?)
        }
        ArrayNormalizationStep::InspectDictionaryRows { row_count, .. } => work(row_count),
        ArrayNormalizationStep::RebuildChunkScaffolds { channels } => work(channels),
        ArrayNormalizationStep::CreateRecordBatchWrapper => {
            arc::<arrow_array::RecordBatch, E>(request)
        }
        ArrayNormalizationStep::CreateChunkColumns { channels } => {
            work(channels)?;
            array::<ArrayRef, E>(channels, request)
        }
        ArrayNormalizationStep::RebuildField { target, name, .. } => {
            work(name.len())?;
            array::<u8, E>(name.len(), request)?;
            clone_parameters(target, request, work, 1)?;
            arc::<Field, E>(request)
        }
        ArrayNormalizationStep::RebuildType { source } => type_bound(source, opaque, work),
        ArrayNormalizationStep::RebuildArray { source, target, .. } => {
            array_bound(source, target, request, opaque, work, false)
        }
        ArrayNormalizationStep::SliceArray { source, .. } => {
            array_bound(source, source.data_type(), request, opaque, work, true)
        }
    }
}

fn type_bound<E: From<ArrayProjectionFailure>>(
    ty: &DataType,
    opaque: &mut Opaque<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<(), E> {
    work(1)?;
    if let DataType::Union(fields, _) = ty {
        // Arrow60.1 FromIterator unzips two Vecs before try_new's pair Vec.
        // Reuse the common source author, preserving the actual constructor
        // and distinguishing its opaque growth from the final public Arc.
        work(mul(fields.len(), 3)?)?;
        let requests =
            novarocks_type_contract::owned_resources::arrow_union::from_iterator_requests(
                fields.len(),
            )
            .map_err(|source| E::from(ArrayProjectionFailure::UnionFields(source)))?;
        return opaque(ArrayProjectionOpaqueBound {
            requests_upper: add(requests.scratch_requests_upper, 1)?,
            bytes_upper: add(requests.scratch_bytes_upper, requests.final_arc.size())?,
            work_upper: mul(fields.len(), 3)?,
        });
    }
    let (elements, width, extra) = match ty {
        DataType::Struct(fields) => (fields.len(), std::mem::size_of::<FieldRef>(), 1),
        DataType::Dictionary(_, _) => (2, std::mem::size_of::<DataType>(), 0),
        _ => (0, 0, 0),
    };
    if elements == 0 {
        return Ok(());
    }
    // Result-shunt/UnionFields collections can grow from Vec::new. Their
    // doubling capacities and the final Arc conversion are covered, rather
    // than claiming a foreign private collection has an exact layout.
    let bytes = mul(add(mul(elements.max(4), 4)?, extra)?, width)?;
    opaque(ArrayProjectionOpaqueBound {
        requests_upper: add(elements, 4)?,
        bytes_upper: add(bytes, mul(extra, ARROW_BUFFER_OWNER_METADATA_BOUND)?)?,
        work_upper: elements,
    })
}

fn datatype_nodes<E: From<ArrayProjectionFailure>>(
    ty: &DataType,
    depth: usize,
    work: &mut Work<'_, E>,
) -> Result<usize, E> {
    work(1)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(E::from(ArrayProjectionFailure::ValueType(
            novarocks_type_contract::ValueTypeError::TooDeep,
        )));
    }
    let mut sum = 1usize;
    let mut child = |ty: &DataType| -> Result<(), E> {
        sum = add(sum, datatype_nodes(ty, depth + 1, work)?)?;
        if sum > novarocks_type_contract::MAX_VALUE_TYPE_NODES {
            return Err(E::from(ArrayProjectionFailure::ValueType(
                novarocks_type_contract::ValueTypeError::TooManyNodes,
            )));
        }
        Ok(())
    };
    match ty {
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => child(f.data_type())?,
        DataType::Struct(fs) => {
            for f in fs {
                child(f.data_type())?;
            }
        }
        DataType::Union(fs, _) => {
            for (_, f) in fs.iter() {
                child(f.data_type())?;
            }
        }
        DataType::Dictionary(k, v) => {
            child(k)?;
            child(v)?;
        }
        DataType::RunEndEncoded(k, v) => {
            child(k.data_type())?;
            child(v.data_type())?;
        }
        _ => {}
    }
    Ok(sum)
}
fn array_bound<E: From<ArrayProjectionFailure>>(
    source: &ArrayRef,
    target: &DataType,
    request: &mut Request<'_, E>,
    opaque: &mut Opaque<'_, E>,
    work: &mut Work<'_, E>,
    slice: bool,
) -> Result<(), E> {
    use arrow_array::{FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray};
    work(1)?;
    if !slice {
        if super::requires_default_validated_container(target) {
            return metadata_fallback(source, target, opaque, work, false);
        }
        let mut scratch_rows = 0usize;
        let mut scratch_masks = 0usize;
        let mut inspect_required = |field: &Field, array: &ArrayRef| -> Result<(), E> {
            work(1)?;
            if !field.is_nullable() {
                if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
                    if dict.values().null_count() != 0 {
                        scratch_rows = add(scratch_rows, array.len())?;
                        scratch_masks = add(scratch_masks, 1)?;
                    }
                } else if matches!(
                    array.data_type(),
                    DataType::Union(_, _) | DataType::RunEndEncoded(_, _) | DataType::Null
                ) {
                    let mut nodes = 0;
                    let span = borrowed_array_span(array.as_ref(), 1, &mut nodes, work)?;
                    scratch_rows = add(scratch_rows, mul(nodes, span)?)?;
                    scratch_masks = add(scratch_masks, nodes)?;
                }
            }
            Ok(())
        };
        match target {
            DataType::Struct(fields) => {
                let actual = source
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        E::from(ArrayProjectionFailure::Contract(
                            ArrayProjectionViolation::PhysicalWrapper,
                        ))
                    })?;
                array::<ArrayRef, E>(fields.len(), request)?;
                arc::<StructArray, E>(request)?;
                for (field, child) in fields.iter().zip(actual.columns()) {
                    inspect_required(field, child)?;
                }
            }
            DataType::Map(entries, _) => {
                let actual = source.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                    E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::PhysicalWrapper,
                    ))
                })?;
                array::<ArrayRef, E>(2, request)?;
                arc::<StructArray, E>(request)?;
                arc::<MapArray, E>(request)?;
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::MapEntriesType,
                    )));
                };
                for (field, child) in fields.iter().zip(actual.entries().columns()) {
                    inspect_required(field, child)?;
                }
            }
            DataType::List(field) => {
                let actual = source.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::PhysicalWrapper,
                    ))
                })?;
                arc::<ListArray, E>(request)?;
                inspect_required(field, actual.values())?;
            }
            DataType::LargeList(field) => {
                let actual = source
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| {
                        E::from(ArrayProjectionFailure::Contract(
                            ArrayProjectionViolation::PhysicalWrapper,
                        ))
                    })?;
                arc::<LargeListArray, E>(request)?;
                inspect_required(field, actual.values())?;
            }
            DataType::FixedSizeList(field, _) => {
                let actual = source
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        E::from(ArrayProjectionFailure::Contract(
                            ArrayProjectionViolation::PhysicalWrapper,
                        ))
                    })?;
                arc::<FixedSizeListArray, E>(request)?;
                inspect_required(field, actual.values())?;
            }
            _ => return metadata_fallback(source, target, opaque, work, false),
        }
        if scratch_masks != 0 {
            opaque(ArrayProjectionOpaqueBound {
                requests_upper: mul(scratch_masks, 4)?,
                bytes_upper: add(
                    mul(
                        add(bitmap_bound(scratch_rows)?, mul(scratch_masks, 64)?)?,
                        4,
                    )?,
                    mul(scratch_masks, ARROW_BUFFER_OWNER_METADATA_BOUND)?,
                )?,
                work_upper: mul(scratch_rows, 4)?,
            })?;
        }
        return Ok(());
    }
    metadata_fallback(source, target, opaque, work, true)
}
fn metadata_fallback<E: From<ArrayProjectionFailure>>(
    source: &ArrayRef,
    target: &DataType,
    opaque: &mut Opaque<'_, E>,
    work: &mut Work<'_, E>,
    slice: bool,
) -> Result<(), E> {
    let nodes = datatype_nodes(source.data_type(), 1, work)?.max(datatype_nodes(target, 1, work)?);
    let span = borrowed_array_span(source.as_ref(), 1, &mut 0, work)?;
    // Slice visits its retained child wrappers; the ArrayData Union/REE branch
    // can repeatedly reconstruct child subtrees along their bounded depth.
    let factor = if slice {
        4
    } else {
        novarocks_type_contract::MAX_VALUE_TYPE_DEPTH + 2
    };
    let repeats = mul(nodes, factor)?;
    let node_bytes = add(
        mul(std::mem::size_of::<ArrayData>(), 8)?,
        mul(ARROW_BUFFER_OWNER_METADATA_BOUND, 8)?,
    )?;
    let null_scratch = mul(
        nodes,
        add(
            mul(bitmap_bound(span)?, 4)?,
            ARROW_BUFFER_OWNER_METADATA_BOUND,
        )?,
    )?;
    // Arrow60.1 validate_values now constructs a fixed 128-slot child-length
    // lookup for a Union root. The fallback's existing bounded constructor
    // repetitions conservatively cover that actual validation scratch too;
    // this is separate from ArrayData/wrapper metadata and bitmap scratch.
    let union_validation = if matches!(target, DataType::Union(..)) {
        mul(
            repeats,
            Layout::array::<Option<usize>>(128)
                .map_err(|source| ArrayProjectionFailure::ArrayLayout {
                    count: 128,
                    width: std::mem::size_of::<Option<usize>>(),
                    source,
                })?
                .size(),
        )?
    } else {
        0
    };
    opaque(ArrayProjectionOpaqueBound {
        requests_upper: add(
            mul(repeats, 12)?,
            if union_validation != 0 { repeats } else { 0 },
        )?,
        bytes_upper: add(
            add(mul(repeats, node_bytes)?, null_scratch)?,
            union_validation,
        )?,
        work_upper: add(
            add(repeats, mul(nodes, span)?)?,
            if union_validation != 0 {
                mul(repeats, 128)?
            } else {
                0
            },
        )?,
    })
}

fn borrowed_array_span<E: From<ArrayProjectionFailure>>(
    source: &dyn Array,
    depth: usize,
    nodes: &mut usize,
    work: &mut Work<'_, E>,
) -> Result<usize, E> {
    use arrow_array::types::{Int16Type, Int64Type};
    use arrow_array::{
        FixedSizeListArray, LargeListArray, ListArray, MapArray, RunArray, StructArray, UnionArray,
    };
    work(1)?;
    *nodes = add(*nodes, 1)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(E::from(ArrayProjectionFailure::ValueType(
            novarocks_type_contract::ValueTypeError::TooDeep,
        )));
    }
    if *nodes > novarocks_type_contract::MAX_VALUE_TYPE_NODES {
        return Err(E::from(ArrayProjectionFailure::ValueType(
            novarocks_type_contract::ValueTypeError::TooManyNodes,
        )));
    }
    let mut span = source.len();
    let mut child = |array: &dyn Array| -> Result<(), E> {
        span = span.max(borrowed_array_span(array, depth + 1, nodes, work)?);
        Ok(())
    };
    if let Some(array) = source.as_any().downcast_ref::<StructArray>() {
        for array in array.columns() {
            child(array.as_ref())?;
        }
    } else if let Some(array) = source.as_any().downcast_ref::<MapArray>() {
        child(array.entries())?;
    } else if let Some(array) = source.as_any().downcast_ref::<ListArray>() {
        child(array.values().as_ref())?;
    } else if let Some(array) = source.as_any().downcast_ref::<LargeListArray>() {
        child(array.values().as_ref())?;
    } else if let Some(array) = source.as_any().downcast_ref::<FixedSizeListArray>() {
        child(array.values().as_ref())?;
    } else if let Some(values) = dictionary_values(source) {
        child(values.as_ref())?;
    } else if let Some(array) = source.as_any().downcast_ref::<UnionArray>() {
        let DataType::Union(fields, _) = source.data_type() else {
            return Err(E::from(ArrayProjectionFailure::Contract(
                ArrayProjectionViolation::PhysicalWrapper,
            )));
        };
        for (id, _) in fields.iter() {
            child(array.child(id).as_ref())?;
        }
    } else if let Some(array) = source.as_any().downcast_ref::<RunArray<Int16Type>>() {
        child(array.values().as_ref())?;
    } else if let Some(array) = source.as_any().downcast_ref::<RunArray<Int32Type>>() {
        child(array.values().as_ref())?;
    } else if let Some(array) = source.as_any().downcast_ref::<RunArray<Int64Type>>() {
        child(array.values().as_ref())?;
    }
    Ok(span)
}

#[cfg(test)]
mod extent_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn impossible_array_backing_preserves_geometry_and_never_calls_the_host() {
        for count in [usize::MAX, isize::MAX as usize / 8 + 1] {
            let failure = array::<u64, ArrayProjectionFailure>(count, &mut |_| {
                panic!("an impossible Layout must not reach the host")
            })
            .unwrap_err();
            assert!(
                matches!(failure, ArrayProjectionFailure::ArrayLayout { count: actual, width: 8, .. } if actual == count)
            );
            let fact = failure.failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            assert!(fact.resource().is_none());
            let extent = fact.representation().unwrap();
            assert_eq!(extent.required, u64::try_from(count as u128 * 8).ok());
            assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
            assert_eq!(extent.representable_prefix, None);
            assert!(
                std::error::Error::source(&failure)
                    .unwrap()
                    .is::<std::alloc::LayoutError>()
            );
        }
        assert_eq!(
            ArrayProjectionFailure::RequestArithmetic
                .failure_fact()
                .class(),
            FailureClass::Resource
        );
        assert!(
            ArrayProjectionFailure::RequestArithmetic
                .failure_fact()
                .representation()
                .is_none()
        );
    }
}
