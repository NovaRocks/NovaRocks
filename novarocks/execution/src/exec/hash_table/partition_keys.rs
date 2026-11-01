// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Thin native tuple policy over the public value algorithms. This module owns
//! column combination and canonical numeric domains, never byte decoding or a
//! second byte hash. Ordinary exchange keeps its original partition mapping.

use std::{alloc::Layout, mem::size_of};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, ListArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::{
    ByteLayout, ValueError, ValueFailure, ValueViolation,
    access::ByteSource,
    control::ValueStructure,
    decode::{DecodedBytes, ValueConversionContext},
    hash_eq::{ByteHashAlgorithm, NullHash, hash_row_observed},
    reachability::ReachableRows,
};

use crate::{
    exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};

use super::hash::canonical_f64_bits;

const FNV_SEED: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x100_0000_01b3;
const BYTE_HASH: ByteHashAlgorithm = ByteHashAlgorithm::Fnv1a64 { seed: FNV_SEED };

/// A short borrowed constructor adapter carrying the input's original finite
/// buffer/type responsibility into the driver's conversion cache. No Chunk,
/// runtime, capacity grant or service is retained by the resulting storage.
pub(crate) struct SourceKeyOwner<'a> {
    original: &'a dyn InputConstructionOwner,
    storage: ValueTypeStorageLifetime,
}
pub(crate) fn source_key_owner<'a>(
    source: &crate::exec::chunk::Chunk,
    original: &'a dyn InputConstructionOwner,
) -> ExecutionResult<SourceKeyOwner<'a>> {
    let storage = source.source_storage_lifetime_observed::<ConstructionFailure>(
        original.storage_lifetime(),
        |layout| original.request(layout),
    )?;
    Ok(SourceKeyOwner { original, storage })
}
impl SourceKeyOwner<'_> {
    pub(crate) fn as_ref(&self) -> &dyn InputConstructionOwner {
        self
    }
}
impl InputConstructionOwner for SourceKeyOwner<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        self.original.request(layout)
    }
    fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
        self.original.observe_opaque_upper_bound(bytes)
    }
    fn observe(
        &self,
        occurrence: usize,
        path: &[crate::exec::chunk::ArrayPathStep],
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        self.original.observe(occurrence, path, step)
    }
}

pub(crate) struct TupleHashBatch {
    hashes: Vec<Option<u64>>,
    _storage: ValueTypeStorageLifetime,
}
impl TupleHashBatch {
    pub(crate) fn as_slice(&self) -> &[Option<u64>] {
        &self.hashes
    }
}

fn invariant(violation: ValueViolation) -> ExecutionFailure {
    ExecutionFailure::from_domain(ValueError::Invariant(violation))
}
fn original_failure(error: ValueFailure<ExecutionFailure>) -> ExecutionFailure {
    match error {
        ValueFailure::Value(error) => ExecutionFailure::from_domain(error),
        ValueFailure::Caller(error) => error,
    }
}
fn request<T>(owner: &dyn InputConstructionOwner, count: usize) -> ExecutionResult<()> {
    owner.request(Layout::array::<T>(count).map_err(|_| {
        ConstructionFailure::from(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?)?;
    Ok(())
}
fn structure(owner: &dyn InputConstructionOwner, value: ValueStructure) -> ExecutionResult<()> {
    owner.observe(
        0,
        &[],
        crate::exec::chunk::ArrayNormalizationStep::ValueStructure {
            path: &[],
            event: value,
        },
    )?;
    Ok(())
}

fn combine(accumulator: u64, value: u64) -> u64 {
    (accumulator ^ value).wrapping_mul(FNV_PRIME)
}

/// One base hash per selected logical row. Excluded rows remain None and their
/// key values are never inspected. Numeric canonicalization and two nested
/// native combination steps preserve NativeExchangeV1's base tuple algorithm;
/// the caller explicitly applies InstanceV1 or DriverV1 after this operation.
/// The exact signed types are required even when a dictionary stores a key.
pub(crate) fn native_tuple_hashes_observed(
    arrays: &[ArrayRef],
    key_types: &[FunctionValueType],
    selected: &ReachableRows<'_>,
    conversions: &mut ValueConversionContext,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<TupleHashBatch> {
    checkpoint()?;
    if arrays.is_empty() || arrays.len() != key_types.len() {
        return Err(invariant(ValueViolation::Layout));
    }
    request::<Option<u64>>(owner, selected.len())?;
    let mut hashes = Vec::new();
    hashes.try_reserve_exact(selected.len()).map_err(|source| {
        ExecutionFailure::from_domain(ValueError::Allocation {
            requested: selected.len().saturating_mul(size_of::<Option<u64>>()),
            source,
        })
    })?;
    hashes.resize(selected.len(), None);
    selected
        .for_each(&mut |row| {
            hashes[row] = Some(FNV_SEED);
            checkpoint()
        })
        .map_err(original_failure)?;
    let storage = owner.storage_lifetime();
    for (array, ty) in arrays.iter().zip(key_types) {
        if array.len() != selected.len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        if needs_nested_normalization(array, ty) {
            let normalized = normalize_nested_selected(
                array,
                ty,
                selected,
                conversions,
                owner,
                checkpoint,
                novarocks_value_arrays::hydrate::HydrateOptions::default(),
            )?;
            request::<Option<u64>>(owner, normalized.rows.len())?;
            let mut compact = normalized
                .rows
                .iter()
                .map(|&row| hashes[row])
                .collect::<Vec<_>>();
            append_native_column(
                &normalized.array,
                ty,
                &ReachableRows::all(normalized.rows.len()),
                &mut compact,
                conversions,
                owner,
                checkpoint,
            )?;
            for (&row, hash) in normalized.rows.iter().zip(compact) {
                hashes[row] = hash;
            }
        } else {
            append_native_column(
                array,
                ty,
                selected,
                &mut hashes,
                conversions,
                owner,
                checkpoint,
            )?;
        }
    }
    checkpoint()?;
    Ok(TupleHashBatch {
        hashes,
        _storage: storage,
    })
}

fn append_native_column(
    array: &ArrayRef,
    ty: &FunctionValueType,
    selected: &ReachableRows<'_>,
    hashes: &mut [Option<u64>],
    conversions: &mut ValueConversionContext,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    if array.len() != selected.len() {
        return Err(invariant(ValueViolation::Bounds));
    }
    let storage = owner.storage_lifetime();
    let dictionary = matches!(array.data_type(), DataType::Dictionary(..));
    if !dictionary && array.data_type() != ty.data_type() {
        return Err(invariant(ValueViolation::Layout));
    }
    let byte_layout = ByteLayout::from_data_type(ty.data_type());
    if dictionary && !byte_layout.is_some_and(ByteLayout::is_utf8) {
        return Err(invariant(ValueViolation::Dictionary));
    }
    if let Some(layout) = byte_layout {
        let source = ByteSource::parse(array, layout, None)
            .map_err(ExecutionFailure::from_domain)?
            .with_storage(&storage);
        let decoded = conversions
            .decode_structured_observed(
                &source,
                selected,
                &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| checkpoint(),
                &mut |value| structure(owner, value),
            )
            .map_err(original_failure)?;
        for (row, hash) in hashes.iter_mut().enumerate() {
            if row.is_multiple_of(256) {
                checkpoint()?;
            }
            let Some(accumulator) = hash.as_mut() else {
                continue;
            };
            let value = decoded.value(row).map_err(ExecutionFailure::from_domain)?;
            if value.is_none() && !ty.nullable() {
                return Err(invariant(ValueViolation::RequiredNull));
            }
            let leaf = if value.is_none() {
                None
            } else {
                Some(
                    hash_row_observed(
                        &decoded,
                        row,
                        BYTE_HASH,
                        NullHash::PreserveSeed,
                        &mut |_| checkpoint(),
                    )
                    .map_err(original_failure)?,
                )
            };
            let component = combine(FNV_SEED, leaf.unwrap_or(0));
            *accumulator = combine(*accumulator, component);
        }
    } else if matches!(array.data_type(), DataType::List(field)
            if matches!(field.data_type(), DataType::Utf8 | DataType::Int32))
    {
        if !ty.nullable() {
            selected
                .for_each(&mut |row| {
                    if array.is_null(row) {
                        Err(invariant(ValueViolation::RequiredNull))
                    } else {
                        Ok(())
                    }
                })
                .map_err(original_failure)?;
        }
        hash_native_list(array, selected, hashes, conversions, owner, checkpoint)?;
    } else {
        for (row, hash) in hashes.iter_mut().enumerate() {
            if row.is_multiple_of(256) {
                checkpoint()?;
            }
            let Some(accumulator) = hash.as_mut() else {
                continue;
            };
            if normalized_root_is_null(array.as_ref(), row) && !ty.nullable() {
                return Err(invariant(ValueViolation::RequiredNull));
            }
            let leaf = primitive_hash(array, row, checkpoint)?;
            *accumulator = combine(*accumulator, combine(FNV_SEED, leaf.unwrap_or(0)));
        }
    }
    Ok(())
}

/// Only for physical roots after encoded byte sources have been decoded by
/// their original author. Arrow NullArray has no validity buffer, so is_null
/// alone does not express the logical NULL of that carrier.
pub(crate) fn normalized_root_is_null(array: &dyn arrow::array::Array, row: usize) -> bool {
    matches!(array.data_type(), DataType::Null) || array.is_null(row)
}

struct NormalizedNested {
    array: ArrayRef,
    rows: Vec<usize>,
    row_map: Vec<usize>,
}
fn needs_nested_normalization(array: &ArrayRef, ty: &FunctionValueType) -> bool {
    array.data_type() != ty.data_type()
        && matches!(
            ty.data_type(),
            DataType::List(_) | DataType::Struct(_) | DataType::Map(..)
        )
}
fn normalize_nested_selected(
    array: &ArrayRef,
    ty: &FunctionValueType,
    selected: &ReachableRows<'_>,
    conversions: &mut ValueConversionContext,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
    options: novarocks_value_arrays::hydrate::HydrateOptions,
) -> ExecutionResult<NormalizedNested> {
    use novarocks_functions::selected_copy::{self, CopyConstructionError, CopyIndexView};
    use novarocks_value_arrays::hydrate::HydratePlan;
    request::<usize>(owner, selected.len())?;
    request::<u32>(owner, selected.len())?;
    request::<usize>(owner, selected.len())?;
    let mut row_map = vec![usize::MAX; selected.len()];
    let mut rows = Vec::new();
    let mut indices = Vec::new();
    rows.try_reserve_exact(selected.len()).map_err(|source| {
        ExecutionFailure::from_domain(ValueError::Allocation {
            requested: selected.len().saturating_mul(size_of::<usize>()),
            source,
        })
    })?;
    indices
        .try_reserve_exact(selected.len())
        .map_err(|source| {
            ExecutionFailure::from_domain(ValueError::Allocation {
                requested: selected.len().saturating_mul(size_of::<u32>()),
                source,
            })
        })?;
    selected
        .for_each(&mut |row| {
            checkpoint()?;
            indices.push(u32::try_from(row).map_err(|_| invariant(ValueViolation::Bounds))?);
            row_map[row] = rows.len();
            rows.push(row);
            Ok(())
        })
        .map_err(original_failure)?;
    let mut request = |layout| owner.request(layout).map_err(ExecutionFailure::from);
    let mut opaque = |bytes| {
        owner
            .observe_opaque_upper_bound(bytes)
            .map_err(ExecutionFailure::from)
    };
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        owner.storage_lifetime(),
        &mut request,
        &mut opaque,
    );
    // This is one selected batch projection, not one Arrow publication per key.
    selected_copy::preflight_primitive_vec_adoption_construction_observed(
        &mut creation,
        &mut |_| checkpoint(),
    )
    .map_err(|error| match error {
        CopyConstructionError::Creator(cause) => cause,
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
    })?;
    let indices = arrow::array::UInt32Array::from(indices);
    selected_copy::preflight_take_construction_unified_observed(
        array.as_ref(),
        CopyIndexView::UInt32(&indices),
        &mut creation,
        &mut |_| checkpoint(),
    )
    .map_err(|error| match error {
        CopyConstructionError::Creator(cause) => cause,
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
    })?;
    let compact = arrow::compute::take(array.as_ref(), &indices, None).map_err(|error| {
        ExecutionFailure::from(
            novarocks_functions::array_projection::ArrayProjectionFailure::Arrow {
                operation:
                    novarocks_functions::array_projection::ArrayProjectionOperation::RebuildArray,
                source: error,
            },
        )
    })?;
    let storage = owner.storage_lifetime();
    let mut observe = |event: novarocks_value_arrays::hydrate::HydrateEvent<'_>| {
        novarocks_functions::array_projection::observe_value_hydration(event, &mut |step| {
            owner.observe(0, &[], step).map_err(ExecutionFailure::from)
        })
    };
    let mut growth = |layout| owner.request(layout).map_err(ExecutionFailure::from);
    let plan = HydratePlan::plan_with_options_and_storage_observed(
        &compact,
        ty.typed_binding(),
        options,
        conversions,
        &storage,
        &mut |_, _| false,
        &mut growth,
        &mut |_| checkpoint(),
        &mut observe,
    )
    .map_err(original_failure)?;
    plan.require_complete()
        .map_err(ExecutionFailure::from_domain)?;
    let array = plan
        .execute_observed(
            conversions,
            &mut growth,
            &mut |_| checkpoint(),
            &mut observe,
        )
        .map_err(original_failure)?;
    checkpoint()?;
    Ok(NormalizedNested {
        array,
        rows,
        row_map,
    })
}

fn primitive_hash(
    array: &ArrayRef,
    row: usize,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Option<u64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    macro_rules! hash {
        ($array:ty, $bytes:expr) => {{
            let value = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?
                .value(row);
            let bytes = $bytes(value);
            Some(BYTE_HASH.hash_bytes(&bytes))
        }};
    }
    Ok(match array.data_type() {
        DataType::Null => None,
        DataType::Boolean => Some(u64::from(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?
                .value(row),
        )),
        // Native canonicalization widens signed integers and floats before
        // hashing. No temporary Arrow cast array is needed for a scalar loan.
        DataType::Int8 => hash!(Int8Array, |v: i8| i64::from(v).to_le_bytes()),
        DataType::Int16 => hash!(Int16Array, |v: i16| i64::from(v).to_le_bytes()),
        DataType::Int32 => hash!(Int32Array, |v: i32| i64::from(v).to_le_bytes()),
        DataType::Int64 => hash!(Int64Array, |v: i64| v.to_le_bytes()),
        DataType::Float32 => hash!(Float32Array, |v: f32| canonical_f64_bits(f64::from(v))
            .to_le_bytes()),
        DataType::Float64 => hash!(Float64Array, |v: f64| canonical_f64_bits(v).to_le_bytes()),
        DataType::Date32 => hash!(Date32Array, |v: i32| v.to_le_bytes()),
        DataType::Timestamp(TimeUnit::Second, _) => {
            hash!(TimestampSecondArray, |v: i64| v.to_le_bytes())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            hash!(TimestampMillisecondArray, |v: i64| v.to_le_bytes())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            hash!(TimestampMicrosecondArray, |v: i64| v.to_le_bytes())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            hash!(TimestampNanosecondArray, |v: i64| v.to_le_bytes())
        }
        DataType::Decimal128(..) => hash!(Decimal128Array, |v: i128| v.to_le_bytes()),
        DataType::Decimal256(..) => {
            hash!(Decimal256Array, |v: arrow::datatypes::i256| v.to_le_bytes())
        }
        DataType::FixedSizeBinary(16) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?;
            let bytes: [u8; 16] = array
                .value(row)
                .try_into()
                .map_err(|_| invariant(ValueViolation::Layout))?;
            Some(BYTE_HASH.hash_bytes(&i128::from_be_bytes(bytes).to_le_bytes()))
        }
        // Nested grouping serialization remains its existing consumer author;
        // this adapter never introduces another nested value representation.
        DataType::List(_) | DataType::Struct(_) | DataType::Map(..) => {
            super::key_builder::canonical_group_key_fnv_hash_observed(array, row, checkpoint)?
        }
        _ => return Err(invariant(ValueViolation::Carrier)),
    })
}

// The historical native algorithm specializes these two list domains. Keep
// that policy while borrowing all SQL byte interpretation from value-arrays.
fn hash_native_list(
    array: &ArrayRef,
    selected: &ReachableRows<'_>,
    hashes: &mut [Option<u64>],
    conversions: &mut ValueConversionContext,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    let list = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| invariant(ValueViolation::Carrier))?;
    let children = list.values();
    let storage = owner.storage_lifetime();
    let decoded = if children.data_type() == &DataType::Utf8 {
        let reachable = ReachableRows::List32 {
            parent: selected,
            offsets: list.value_offsets(),
            validity: list.nulls(),
            child_len: children.len(),
        };
        let source = ByteSource::parse(children, ByteLayout::Utf8, None)
            .map_err(ExecutionFailure::from_domain)?
            .with_storage(&storage);
        Some(
            conversions
                .decode_structured_observed(
                    &source,
                    &reachable,
                    &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                    &mut |_| checkpoint(),
                    &mut |value| structure(owner, value),
                )
                .map_err(original_failure)?,
        )
    } else {
        None
    };
    for (row, hash) in hashes.iter_mut().enumerate() {
        if row.is_multiple_of(256) {
            checkpoint()?;
        }
        let Some(accumulator) = hash.as_mut() else {
            continue;
        };
        checkpoint()?;
        let leaf = if list.is_null(row) {
            0
        } else {
            let start = list.value_offsets()[row] as usize;
            let end = list.value_offsets()[row + 1] as usize;
            let mut value = combine(
                FNV_SEED,
                BYTE_HASH.hash_bytes(&((end - start) as u64).to_le_bytes()),
            );
            for child in start..end {
                if child.is_multiple_of(256) {
                    checkpoint()?;
                }
                let part = if let Some(decoded) = &decoded {
                    if decoded
                        .value(child)
                        .map_err(ExecutionFailure::from_domain)?
                        .is_none()
                    {
                        0
                    } else {
                        hash_row_observed(
                            decoded,
                            child,
                            BYTE_HASH,
                            NullHash::PreserveSeed,
                            &mut |_| checkpoint(),
                        )
                        .map_err(original_failure)?
                    }
                } else {
                    let values = children
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| invariant(ValueViolation::Carrier))?;
                    if values.is_null(child) {
                        0
                    } else {
                        BYTE_HASH.hash_bytes(&values.value(child).to_le_bytes())
                    }
                };
                value = combine(value, part);
            }
            value
        };
        *accumulator = combine(*accumulator, combine(FNV_SEED, leaf));
    }
    Ok(())
}

/// Source-backed byte views plus the original canonical primitive/nested
/// encoder. Tuple framing belongs to this consumer; retained keys own only
/// their canonical bytes, never a newly published Arrow array per key.
/// Owns only prepared columns and their original finite custody. It can move
/// into an immutable input page without keeping references into that page.
pub(crate) struct CanonicalTupleStorage {
    bytes: Vec<Option<DecodedBytes>>,
    normalized: Vec<Option<NormalizedNested>>,
    _storage: ValueTypeStorageLifetime,
}
impl CanonicalTupleStorage {
    pub(crate) fn view<'a>(&'a self, arrays: &'a [ArrayRef]) -> CanonicalTupleView<'a> {
        CanonicalTupleView {
            arrays,
            storage: self,
        }
    }
    pub(crate) fn decoded_component(&self, column: usize) -> Option<&DecodedBytes> {
        self.bytes.get(column).and_then(Option::as_ref)
    }
}
pub(crate) struct CanonicalTupleView<'a> {
    arrays: &'a [ArrayRef],
    storage: &'a CanonicalTupleStorage,
}
pub(crate) struct CanonicalTupleBatch<'a> {
    arrays: &'a [ArrayRef],
    storage: CanonicalTupleStorage,
}
impl CanonicalTupleBatch<'_> {
    pub(crate) fn into_storage(self) -> CanonicalTupleStorage {
        self.storage
    }
    pub(crate) fn string_component(
        &self,
        column: usize,
        row: usize,
    ) -> ExecutionResult<Option<Option<&str>>> {
        self.storage.view(self.arrays).string_component(column, row)
    }
    #[cfg(test)]
    pub(crate) fn encode_row(
        &self,
        row: usize,
        allocator: super::allocation::AggregateAllocator,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<super::allocation::AggregateVec<u8>> {
        self.storage
            .view(self.arrays)
            .encode_row(row, allocator, checkpoint)
    }
}
impl<'a> CanonicalTupleView<'a> {
    pub(crate) fn physical_component(
        &self,
        column: usize,
    ) -> ExecutionResult<(&'a ArrayRef, Option<&'a [usize]>)> {
        let array = self
            .arrays
            .get(column)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        Ok(
            match self.storage.normalized.get(column).and_then(Option::as_ref) {
                Some(normalized) => (&normalized.array, Some(&normalized.row_map)),
                None => (array, None),
            },
        )
    }

    /// Borrow an already decoded SQL STRING component for bounded diagnostics.
    /// The outer Option distinguishes non-STRING columns from logical NULL.
    pub(crate) fn string_component(
        &self,
        column: usize,
        row: usize,
    ) -> ExecutionResult<Option<Option<&'a str>>> {
        match self.storage.bytes.get(column) {
            Some(Some(decoded)) if decoded.is_utf8() => decoded
                .string(row)
                .map(Some)
                .map_err(ExecutionFailure::from_domain),
            Some(_) => Ok(None),
            None => Err(invariant(ValueViolation::Bounds)),
        }
    }

    #[cfg(test)]
    pub(crate) fn row_has_null(&self, row: usize) -> ExecutionResult<bool> {
        self.row_has_null_observed(row, &|| Ok(()))
    }

    pub(crate) fn row_has_null_observed(
        &self,
        row: usize,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<bool> {
        checkpoint()?;
        for (column, ((array, decoded), normalized)) in self
            .arrays
            .iter()
            .zip(&self.storage.bytes)
            .zip(&self.storage.normalized)
            .enumerate()
        {
            if column.is_multiple_of(256) {
                checkpoint()?;
            }
            let (array, row) = if let Some(normalized) = normalized {
                (
                    &normalized.array,
                    *normalized
                        .row_map
                        .get(row)
                        .filter(|&&row| row != usize::MAX)
                        .ok_or_else(|| invariant(ValueViolation::Bounds))?,
                )
            } else {
                (array, row)
            };
            let null = match decoded {
                Some(decoded) => decoded
                    .value(row)
                    .map_err(ExecutionFailure::from_domain)?
                    .is_none(),
                None => normalized_root_is_null(array.as_ref(), row),
            };
            if null {
                return Ok(true);
            }
        }
        checkpoint()?;
        Ok(false)
    }

    #[cfg(test)]
    pub(crate) fn encode_row(
        &self,
        row: usize,
        allocator: super::allocation::AggregateAllocator,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<super::allocation::AggregateVec<u8>> {
        let mut key = super::allocation::AggregateVec::new_in(allocator.clone());
        for column in 0..self.arrays.len() {
            self.append_component(column, row, &mut key, &allocator, checkpoint)?;
        }
        Ok(key)
    }

    /// The same canonical author can encode one complex component. Local
    /// component hashing therefore never serializes neighboring STRING values
    /// or bypasses their V dictionary cache.
    pub(crate) fn encode_component_row(
        &self,
        column: usize,
        row: usize,
        allocator: super::allocation::AggregateAllocator,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<super::allocation::AggregateVec<u8>> {
        let mut key = super::allocation::AggregateVec::new_in(allocator.clone());
        self.append_component(column, row, &mut key, &allocator, checkpoint)?;
        Ok(key)
    }

    fn append_component(
        &self,
        column: usize,
        row: usize,
        key: &mut super::allocation::AggregateVec<u8>,
        allocator: &super::allocation::AggregateAllocator,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        let array = self
            .arrays
            .get(column)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        let decoded = self
            .storage
            .bytes
            .get(column)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        let normalized = self
            .storage
            .normalized
            .get(column)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        let (array, row) = if let Some(normalized) = normalized {
            (
                &normalized.array,
                *normalized
                    .row_map
                    .get(row)
                    .filter(|&&row| row != usize::MAX)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?,
            )
        } else {
            (array, row)
        };
        checkpoint()?;
        if let Some(decoded) = decoded {
            let value = decoded.value(row).map_err(ExecutionFailure::from_domain)?;
            let count = value.map_or(1, |bytes| 9usize.saturating_add(bytes.len()));
            key.try_reserve_exact(count).map_err(|source| {
                allocator.allocation_error("reserve canonical tuple bytes", source)
            })?;
            if let Some(bytes) = value {
                key.push(1);
                key.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                for part in bytes.chunks(64 * 1024) {
                    checkpoint()?;
                    key.extend_from_slice(part);
                }
            } else {
                key.push(0);
            }
        } else {
            super::key_builder::append_canonical_nullable_tracked_observed(
                array, row, key, checkpoint,
            )?;
        }
        Ok(())
    }
}

pub(crate) fn prepare_canonical_tuple<'a>(
    arrays: &'a [ArrayRef],
    key_types: &[FunctionValueType],
    selected: &ReachableRows<'_>,
    conversions: &mut ValueConversionContext,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<CanonicalTupleBatch<'a>> {
    prepare_canonical_tuple_with_options(
        arrays,
        key_types,
        selected,
        conversions,
        owner,
        checkpoint,
        novarocks_value_arrays::hydrate::HydrateOptions::default(),
    )
}

/// A local equality index explicitly projects same-domain byte leaves to its
/// own frozen layout. Versioned distribution callers retain the default path.
pub(crate) fn prepare_canonical_tuple_with_byte_layout_projection<'a>(
    arrays: &'a [ArrayRef],
    key_types: &[FunctionValueType],
    selected: &ReachableRows<'_>,
    conversions: &mut ValueConversionContext,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<CanonicalTupleBatch<'a>> {
    prepare_canonical_tuple_with_options(
        arrays,
        key_types,
        selected,
        conversions,
        owner,
        checkpoint,
        novarocks_value_arrays::hydrate::HydrateOptions {
            projection: novarocks_value_arrays::hydrate::HydrateProjection::ExactByteLayouts,
            ..Default::default()
        },
    )
}

fn prepare_canonical_tuple_with_options<'a>(
    arrays: &'a [ArrayRef],
    key_types: &[FunctionValueType],
    selected: &ReachableRows<'_>,
    conversions: &mut ValueConversionContext,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
    options: novarocks_value_arrays::hydrate::HydrateOptions,
) -> ExecutionResult<CanonicalTupleBatch<'a>> {
    if arrays.len() != key_types.len() || arrays.is_empty() {
        return Err(invariant(ValueViolation::Layout));
    }
    request::<Option<DecodedBytes>>(owner, arrays.len())?;
    let mut bytes = Vec::new();
    request::<Option<NormalizedNested>>(owner, arrays.len())?;
    let mut normalized = Vec::with_capacity(arrays.len());
    bytes.try_reserve_exact(arrays.len()).map_err(|source| {
        ExecutionFailure::from_domain(ValueError::Allocation {
            requested: arrays
                .len()
                .saturating_mul(size_of::<Option<DecodedBytes>>()),
            source,
        })
    })?;
    let storage = owner.storage_lifetime();
    for (array, ty) in arrays.iter().zip(key_types) {
        checkpoint()?;
        if array.len() != selected.len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        let nested = if needs_nested_normalization(array, ty) {
            Some(normalize_nested_selected(
                array,
                ty,
                selected,
                conversions,
                owner,
                checkpoint,
                options,
            )?)
        } else {
            None
        };
        let decoded = if let Some(layout) = ByteLayout::from_data_type(ty.data_type()) {
            let source = ByteSource::parse(array, layout, None)
                .map_err(ExecutionFailure::from_domain)?
                .with_storage(&storage);
            Some(
                conversions
                    .decode_structured_observed(
                        &source,
                        selected,
                        &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                        &mut |_| checkpoint(),
                        &mut |value| structure(owner, value),
                    )
                    .map_err(original_failure)?,
            )
        } else {
            if array.data_type() != ty.data_type() && nested.is_none() {
                return Err(invariant(ValueViolation::Layout));
            }
            None
        };
        if let Some(decoded) = &decoded {
            // Batch binding performs the only physical byte-layout dispatch;
            // required-NULL checking then uses V's typed selected visitor.
            let materialized;
            let rows = match selected {
                ReachableRows::Selection(rows) => *rows,
                _ => {
                    materialized = selected
                        .materialize_observed(
                            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                            &mut |_| checkpoint(),
                        )
                        .map_err(original_failure)?;
                    &materialized
                }
            };
            let prepared = novarocks_value_arrays::batch::PreparedBytes::new_observed(
                decoded,
                rows,
                &mut |_| checkpoint(),
            )
            .map_err(original_failure)?;
            novarocks_value_arrays::batch::selected::visit_selected(
                &prepared,
                &mut |_, _, value| {
                    if value.is_none() && !ty.nullable() {
                        return Err(invariant(ValueViolation::RequiredNull));
                    }
                    Ok(())
                },
                &mut |_| checkpoint(),
            )
            .map_err(original_failure)?;
        } else {
            selected
                .for_each(&mut |row| {
                    if normalized_root_is_null(array.as_ref(), row) && !ty.nullable() {
                        return Err(invariant(ValueViolation::RequiredNull));
                    }
                    checkpoint()
                })
                .map_err(original_failure)?;
        }
        bytes.push(decoded);
        normalized.push(nested);
    }
    Ok(CanonicalTupleBatch {
        arrays,
        storage: CanonicalTupleStorage {
            bytes,
            normalized,
            _storage: storage,
        },
    })
}

/// Applicability is an explicit consumer policy. A matched target may never
/// acquire the NULL grouping identity, even if an outer join made it nullable.
pub(crate) fn require_non_null_keys(
    arrays: &[ArrayRef],
    key_types: &[FunctionValueType],
    selected: &ReachableRows<'_>,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    if arrays.len() != key_types.len() {
        return Err(invariant(ValueViolation::Layout));
    }
    for (array, ty) in arrays.iter().zip(key_types) {
        if array.len() != selected.len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        let source = ByteLayout::from_data_type(ty.data_type())
            .map(|layout| ByteSource::parse(array, layout, None))
            .transpose()
            .map_err(ExecutionFailure::from_domain)?;
        selected
            .for_each(&mut |row| {
                checkpoint()?;
                let null = if let Some(source) = &source {
                    source
                        .value(row)
                        .map_err(ExecutionFailure::from_domain)?
                        .is_none()
                } else {
                    array.is_null(row)
                };
                if null {
                    Err(invariant(ValueViolation::RequiredNull))
                } else {
                    Ok(())
                }
            })
            .map_err(original_failure)?;
    }
    Ok(())
}

/// Pure fixture declarations retain the actual encoded field representation;
/// the logical value binding always comes from the explicit frozen types.
#[cfg(test)]
pub(crate) fn typed_encoded_tuple_fixture(
    names: &[&str],
    slots: &[novarocks_types::SlotId],
    types: &[FunctionValueType],
    columns: Vec<ArrayRef>,
    rows: usize,
) -> crate::exec::chunk::Chunk {
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use std::sync::Arc;
    assert_eq!(names.len(), types.len());
    assert_eq!(slots.len(), types.len());
    assert_eq!(columns.len(), types.len());
    let pure = ValueTypeStorageLifetime::pure();
    let declared = names
        .iter()
        .zip(slots)
        .zip(types)
        .zip(&columns)
        .map(|(((name, slot), ty), column)| {
            ChunkSlotSchema::try_new_typed(
                *slot,
                Arc::new(arrow::datatypes::Field::new(
                    *name,
                    column.data_type().clone(),
                    ty.nullable(),
                )),
                ty.typed_binding().clone(),
                None,
                pure.clone(),
            )
            .unwrap()
        })
        .collect();
    let schema = Arc::new(ChunkSchema::try_new_typed(declared, pure).unwrap());
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("encoded tuple fixture construction"),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    owner
        .observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::CreateChunkColumns {
                channels: columns.len(),
            },
        )
        .unwrap();
    Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        columns,
        rows,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::mem_tracker::MemTracker;
    use arrow::array::{
        DictionaryArray, Int32Builder, LargeStringArray, ListBuilder, StringArray, StringBuilder,
        StringViewArray, StructArray,
    };
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_failure_contract::FailureProjection;
    use novarocks_value_arrays::hash_eq::{NullEquality, equal_row_observed};
    use std::sync::Arc;

    fn owner() -> Box<dyn InputConstructionOwner> {
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            MemTracker::new_root("native tuple construction"),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        )
        .unwrap();
        let declaration = crate::exec::chunk::typed_fixture::layout(
            Arc::new(arrow::datatypes::Schema::empty()),
            &[],
            &[],
        );
        let schema = factory.create_compiled_chunk_schema(&declaration).unwrap();
        factory.begin_typed_construction(&schema).unwrap()
    }
    fn hashes(array: ArrayRef, ty: FunctionValueType, selected: &[usize]) -> Vec<Option<u64>> {
        let len = array.len();
        let owner = owner();
        native_tuple_hashes_observed(
            &[array],
            &[ty],
            &ReachableRows::indices(len, selected),
            &mut ValueConversionContext::default(),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap()
        .hashes
    }
    #[test]
    fn native_integer_and_list_policy_matches_frozen_independent_vectors() {
        let expected = vec![
            Some(0xba40_0326_6bee_b280),
            Some(0x8444_774a_16bf_1347),
            Some(0xbae5_6b09_101e_acff),
            Some(0xc37b_be04_8b3c_40b7),
        ];
        for array in [
            Arc::new(Int32Array::from(vec![7, 8, 0, -1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![7, 8, 0, -1])) as ArrayRef,
        ] {
            let ty = FunctionValueType::new(array.data_type().clone(), false);
            assert_eq!(hashes(array, ty, &[0, 1, 2, 3]), expected);
        }
        let mut ints = ListBuilder::new(Int32Builder::new());
        ints.values().append_value(1);
        ints.values().append_null();
        ints.values().append_value(2);
        ints.append(true);
        let ints = Arc::new(ints.finish()) as ArrayRef;
        assert_eq!(
            hashes(
                ints.clone(),
                FunctionValueType::new(ints.data_type().clone(), false),
                &[0]
            ),
            vec![Some(0xeb8e_49fb_b320_0ba4)]
        );
        let mut strings = ListBuilder::new(StringBuilder::new());
        strings.values().append_value("alpha");
        strings.values().append_value("beta");
        strings.append(true);
        let strings = Arc::new(strings.finish()) as ArrayRef;
        assert_eq!(
            hashes(
                strings.clone(),
                FunctionValueType::new(strings.data_type().clone(), false),
                &[0]
            ),
            vec![Some(0x28fb_b7b3_98da_887e)]
        );
    }
    #[test]
    fn public_byte_equality_native_hash_and_tuple_identity_agree_across_carriers() {
        let values = vec![Some("alpha"), None, Some("beta")];
        let plain = Arc::new(StringArray::from(values.clone())) as ArrayRef;
        let carriers = vec![
            (plain.clone(), ByteLayout::Utf8),
            (
                Arc::new(LargeStringArray::from(values.clone())) as ArrayRef,
                ByteLayout::LargeUtf8,
            ),
            (
                Arc::new(StringViewArray::from(values)) as ArrayRef,
                ByteLayout::Utf8View,
            ),
            (
                Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from(vec![Some(0), None, Some(1)]),
                        Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
                    )
                    .unwrap(),
                ) as ArrayRef,
                ByteLayout::Utf8,
            ),
        ];
        let owner = owner();
        let state_owner = MemTracker::new_root("tuple identity bytes");
        let allocator = super::super::allocation::AggregateAllocator::new(state_owner.clone());
        let mut context = ValueConversionContext::default();
        let expected = hashes(
            plain.clone(),
            FunctionValueType::new(DataType::Utf8, true),
            &[0, 1, 2],
        );
        let base_source = ByteSource::parse(&plain, ByteLayout::Utf8, None).unwrap();
        let decoded_base = context
            .decode_structured_observed(
                &base_source,
                &ReachableRows::all(3),
                &mut |_| Ok::<_, ExecutionFailure>(()),
                &mut |_| Ok(()),
                &mut |_| Ok(()),
            )
            .unwrap();
        let mut reference = None;
        for (array, layout) in carriers {
            let source = ByteSource::parse(&array, layout, None).unwrap();
            let decoded = context
                .decode_structured_observed(
                    &source,
                    &ReachableRows::all(3),
                    &mut |_| Ok::<_, ExecutionFailure>(()),
                    &mut |_| Ok(()),
                    &mut |_| Ok(()),
                )
                .unwrap();
            for row in 0..3 {
                assert_eq!(
                    equal_row_observed(
                        &decoded_base,
                        row,
                        &decoded,
                        row,
                        NullEquality::Equal,
                        &mut |_| Ok::<_, ExecutionFailure>(())
                    )
                    .unwrap(),
                    Some(true)
                );
            }
            let ty = FunctionValueType::new(layout.data_type(), true);
            assert_eq!(hashes(array.clone(), ty.clone(), &[0, 1, 2]), expected);
            let arrays = [array];
            let tuples = prepare_canonical_tuple(
                &arrays,
                &[ty],
                &ReachableRows::all(3),
                &mut context,
                owner.as_ref(),
                &|| Ok(()),
            )
            .unwrap();
            let key = tuples.encode_row(0, allocator.clone(), &|| Ok(())).unwrap();
            if let Some(reference) = &reference {
                assert_eq!(key.as_slice(), reference);
            } else {
                reference = Some(key.to_vec());
            }
        }
        assert_eq!(state_owner.current(), 0);
    }
    #[test]
    fn excluded_required_dictionary_value_is_never_read_and_selected_null_refuses() {
        let array = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1]),
                Arc::new(StringArray::from(vec![Some("reachable"), None])) as ArrayRef,
            )
            .unwrap(),
        ) as ArrayRef;
        let ty = FunctionValueType::new(DataType::Utf8, false);
        let selected = hashes(array.clone(), ty.clone(), &[0]);
        assert!(selected[0].is_some());
        assert_eq!(selected[1], None);
        let owner = owner();
        let error = native_tuple_hashes_observed(
            std::slice::from_ref(&array),
            std::slice::from_ref(&ty),
            &ReachableRows::all(2),
            &mut ValueConversionContext::default(),
            owner.as_ref(),
            &|| Ok(()),
        )
        .err()
        .unwrap();
        assert_eq!(
            error.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Invariant
        );
        assert!(
            require_non_null_keys(
                &[array],
                &[ty],
                &ReachableRows::indices(2, &[0]),
                &|| Ok(())
            )
            .is_ok()
        );
    }
    #[test]
    fn nested_dictionary_is_materialized_only_after_selected_batch_projection() {
        let dictionary = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1]),
                Arc::new(StringArray::from(vec![Some("reachable"), None])) as ArrayRef,
            )
            .unwrap(),
        ) as ArrayRef;
        let actual_field = Arc::new(Field::new("value", dictionary.data_type().clone(), false));
        // Arrow's convenience StructArray constructor rejects logical NULL
        // behind a non-null dictionary key. Its safe ArrayData author admits
        // this physically valid carrier, as required_dictionary_nulls does.
        let data = arrow::array::ArrayData::builder(DataType::Struct(vec![actual_field].into()))
            .len(dictionary.len())
            .child_data(vec![dictionary.to_data()])
            .build()
            .unwrap();
        data.validate_full().unwrap();
        let array = Arc::new(StructArray::from(data)) as ArrayRef;
        let frozen =
            DataType::Struct(vec![Arc::new(Field::new("value", DataType::Utf8, false))].into());
        let ty = FunctionValueType::new(frozen, false);
        let result = hashes(array, ty, &[0]);
        assert!(result[0].is_some());
        assert_eq!(result[1], None);
    }
    #[test]
    fn original_control_failure_survives_the_adapter_boundary() {
        let owner = owner();
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Operational(
            novarocks_functions::KernelDiagnostic::new("tuple control origin"),
        ));
        let error = native_tuple_hashes_observed(
            &[Arc::new(Int32Array::from(vec![7]))],
            &[FunctionValueType::new(DataType::Int32, false)],
            &ReachableRows::all(1),
            &mut ValueConversionContext::default(),
            owner.as_ref(),
            &|| Err(original.clone()),
        )
        .err()
        .unwrap();
        assert_eq!(error, original);
    }
    #[test]
    fn conversion_cache_keeps_original_source_funding_until_actual_cache_clear() {
        use arrow::{datatypes::Schema, record_batch::RecordBatch};
        use novarocks_types::SlotId;
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let mut source = crate::exec::chunk::typed_fixture::chunk(
            RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef],
            )
            .unwrap(),
            &[SlotId::new(1)],
            &[FunctionValueType::new(DataType::Utf8, false)],
        );
        let funding = MemTracker::new_root("original tuple source funding");
        source.transfer_to(&funding);
        let original_bytes = funding.current();
        assert!(original_bytes > 0);
        let mut context = ValueConversionContext::default();
        let hashes = {
            let original = owner();
            let owner = source_key_owner(&source, original.as_ref()).unwrap();
            let dictionary = Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![0, 1]),
                    source.columns()[0].clone(),
                )
                .unwrap(),
            ) as ArrayRef;
            native_tuple_hashes_observed(
                &[dictionary],
                &[FunctionValueType::new(DataType::Utf8, false)],
                &ReachableRows::all(2),
                &mut context,
                owner.as_ref(),
                &|| Ok(()),
            )
            .unwrap()
        };
        drop(source);
        assert_eq!(funding.current(), original_bytes);
        drop(hashes);
        assert_eq!(
            funding.current(),
            original_bytes,
            "the cache still owns decoded aliases and original finite funding"
        );
        context.clear();
        assert_eq!(funding.current(), 0);
    }
    #[test]
    fn sparse_excluded_prefix_observes_stop_without_reading_required_null_keys() {
        use std::cell::Cell;
        let owner = owner();
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Operational(
            novarocks_functions::KernelDiagnostic::new("sparse excluded prefix stop"),
        ));
        let mut values = vec![None; 4096];
        values[4095] = Some(7);
        let numeric = Arc::new(Int32Array::from(values)) as ArrayRef;
        let selected = ReachableRows::indices(4096, &[4095]);
        let mut hashes = vec![None; 4096];
        hashes[4095] = Some(FNV_SEED);
        let calls = Cell::new(0usize);
        let error = append_native_column(
            &numeric,
            &FunctionValueType::new(DataType::Int32, false),
            &selected,
            &mut hashes,
            &mut ValueConversionContext::default(),
            owner.as_ref(),
            &|| {
                calls.set(calls.get() + 1);
                if calls.get() == 3 {
                    Err(original.clone())
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert_eq!(calls.get(), 3);
        assert!(error.same_occurrence(&original));
        assert_eq!(hashes[4095], Some(FNV_SEED));
        assert!(hashes[..4095].iter().all(Option::is_none));
        append_native_column(
            &numeric,
            &FunctionValueType::new(DataType::Int32, false),
            &selected,
            &mut hashes,
            &mut ValueConversionContext::default(),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        assert_ne!(hashes[4095], Some(FNV_SEED));
        assert!(hashes[..4095].iter().all(Option::is_none));
        hashes[4095] = Some(FNV_SEED);

        let mut builder = ListBuilder::new(Int32Builder::new());
        for _ in 0..4095 {
            builder.append(false);
        }
        builder.values().append_value(7);
        builder.append(true);
        let list = Arc::new(builder.finish()) as ArrayRef;
        calls.set(0);
        let error = hash_native_list(
            &list,
            &selected,
            &mut hashes,
            &mut ValueConversionContext::default(),
            owner.as_ref(),
            &|| {
                calls.set(calls.get() + 1);
                if calls.get() == 2 {
                    Err(original.clone())
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(error.same_occurrence(&original));
        assert_eq!(hashes[4095], Some(FNV_SEED));
        assert!(hashes[..4095].iter().all(Option::is_none));
    }

    #[test]
    fn actual_typed_nested_tuple_hash_and_state_encoding_observe_the_original_stop() {
        use arrow::array::{Int64Builder, ListBuilder};
        use std::cell::Cell;
        let mut nested = ListBuilder::new(Int64Builder::new());
        for value in 0..4096 {
            nested.values().append_value(value);
        }
        nested.append(true);
        let array = std::sync::Arc::new(nested.finish()) as ArrayRef;
        let ty = FunctionValueType::new(array.data_type().clone(), false);
        let arrays = [array];
        let types = [ty];
        let owner = owner();
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Operational(
            novarocks_functions::KernelDiagnostic::new("actual typed nested tuple stop"),
        ));
        let calls = Cell::new(0usize);
        let failure = native_tuple_hashes_observed(
            &arrays,
            &types,
            &ReachableRows::all(1),
            &mut ValueConversionContext::default(),
            owner.as_ref(),
            &|| {
                calls.set(calls.get() + 1);
                if calls.get() == 6 {
                    Err(original.clone())
                } else {
                    Ok(())
                }
            },
        )
        .err()
        .unwrap();
        assert_eq!(failure, original);
        let tuples = prepare_canonical_tuple(
            &arrays,
            &types,
            &ReachableRows::all(1),
            &mut ValueConversionContext::default(),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let allocator = super::super::allocation::AggregateAllocator::new(MemTracker::new_root(
            "actual nested tuple encoding stop",
        ));
        calls.set(0);
        let failure = tuples
            .encode_row(0, allocator, &|| {
                calls.set(calls.get() + 1);
                if calls.get() == 4 {
                    Err(original.clone())
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert_eq!(failure, original);
    }
}
