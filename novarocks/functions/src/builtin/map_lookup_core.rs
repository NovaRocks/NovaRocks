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
//! Original map key equality, reverse search, take and NULL repair author.
use super::array_literal_core::CollectionObservation;
use super::collection_arrow::{ArrowSite, arrow_failure, cast_output, contract};
use crate::KernelFailure;
use arrow_array::{Array, ArrayRef, BooleanArray, MapArray, UInt32Array};
use arrow_schema::DataType;
use arrow_select::take::take;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, domain,
    kernel_reason::MAP_KEY_INDETERMINATE,
};
use novarocks_value_arrays::{
    ValueFailure,
    hash_eq::{KeyLookup, KeyRows, PreparedKeyOperations},
};

/// This existing frozen Map has an uninhabited key. Its value type remains
/// independent and complete, including nominal domains and nullability. It
/// cannot contain an entry without violating the ordinary non-null key rule.
/// This is a complete-type fact, independent of literal origin or row bytes.
pub fn has_uninhabited_map_key(value: &crate::FunctionValueType) -> bool {
    use novarocks_type_contract::{ValueKind, ValueLogicalType};
    if value.logical_type() != ValueLogicalType::Physical
        || !has_uninhabited_map_key_carrier(value.data_type())
    {
        return false;
    }
    let ValueKind::Map { key, .. } = value.value_type().kind() else {
        return false;
    };
    !key.nullable() && matches!(key.kind(), ValueKind::Scalar(DataType::Null))
}

/// Signature-carrier constraint only; executable selection also checks the
/// complete semantic declaration through `has_uninhabited_map_key`.
pub(super) fn has_uninhabited_map_key_carrier(value: &DataType) -> bool {
    let DataType::Map(entries, _) = value else {
        return false;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return false;
    };
    fields.len() == 2
        && !entries.is_nullable()
        && fields[0].data_type() == &DataType::Null
        && !fields[0].is_nullable()
}

pub fn row_index(row: usize, len: usize) -> usize {
    if len == 1 { 0 } else { row }
}
#[derive(Debug)]
pub enum MapLookupFailure<E> {
    Operation(KernelFailure),
    Control(E),
}
pub struct MapLookupInputs<'a> {
    pub map: &'a MapArray,
    key_arr: &'a ArrayRef,
    check_arr: Option<&'a BooleanArray>,
}
impl<'a> MapLookupInputs<'a> {
    /// Original carrier checks, in their original order after all child evals.
    pub fn new(
        map_arr: &'a ArrayRef,
        key_arr: &'a ArrayRef,
        check_arr: Option<&'a ArrayRef>,
    ) -> Result<Self, KernelFailure> {
        let map = map_arr.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            contract(format_args!(
                "element_at expects MapArray, got {:?}",
                map_arr.data_type()
            ))
        })?;
        let check_arr = check_arr
            .map(|a| {
                a.as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| contract("element_at check flag must be BOOLEAN"))
            })
            .transpose()?;
        Ok(Self {
            map,
            key_arr,
            check_arr,
        })
    }
    /// This is the v1 array-layout contract, not a guess about selected mappings.
    pub fn validate_direct_lengths(&self) -> Result<(), KernelFailure> {
        if self.key_arr.len() != 1 && self.key_arr.len() != self.map.len() {
            return Err(contract(format_args!(
                "element_at key length mismatch: map rows={}, key rows={}",
                self.map.len(),
                self.key_arr.len()
            )));
        }
        if let Some(flags) = self.check_arr
            && flags.len() != 1
            && flags.len() != self.map.len()
        {
            return Err(contract(format_args!(
                "element_at check flag length mismatch: map rows={}, check rows={}",
                self.map.len(),
                flags.len()
            )));
        }
        Ok(())
    }
    pub fn direct_rows(&self, row: usize) -> LookupRows {
        LookupRows {
            map: row,
            key: row_index(row, self.key_arr.len()),
            check: self.check_arr.map(|flags| row_index(row, flags.len())),
        }
    }
}
#[derive(Clone, Copy)]
pub struct LookupRows {
    pub map: usize,
    pub key: usize,
    pub check: Option<usize>,
}
fn observe<E>(
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    event: CollectionObservation,
) -> Result<(), MapLookupFailure<E>> {
    observer(event).map_err(MapLookupFailure::Control)
}
/// Each caller supplies its real argument mapping, never a type-derived default.
pub fn lookup_observed<E>(
    inputs: &MapLookupInputs<'_>,
    key_operations: &PreparedKeyOperations,
    selection: crate::Selection<'_>,
    mut rows: impl FnMut(usize, usize) -> Result<LookupRows, E>,
    output_type: Option<&DataType>,
    mut before_take: impl FnMut(&dyn Array, &UInt32Array) -> Result<(), E>,
    mut row_error: impl FnMut(usize, LookupRowFailure) -> Result<(), KernelFailure>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, MapLookupFailure<E>> {
    let map = inputs.map;
    let key_arr = inputs.key_arr;
    let check_arr = inputs.check_arr;
    let keys = map.keys();
    let values = map.values();
    let offsets = map.value_offsets();
    if values.is_empty() {
        for (ordinal, row) in selection.iter().enumerate() {
            observe(observer, CollectionObservation::Step)?;
            let row = rows(ordinal, row).map_err(MapLookupFailure::Control)?;
            validate_address(inputs, row).map_err(MapLookupFailure::Operation)?;
            if let Some(flags) = check_arr {
                let check_idx = row.check.expect("validated flag address");
                let strict = !flags.is_null(check_idx) && flags.value(check_idx);
                if strict && !map.is_null(row.map) && !is_null_probe(key_arr, row.key) {
                    row_error(ordinal, LookupRowFailure::MissingKey)
                        .map_err(MapLookupFailure::Operation)?;
                }
            }
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let out = arrow_array::new_null_array(values.data_type(), selection.len());
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let out =
            cast_output(out, output_type, "element_at").map_err(MapLookupFailure::Operation)?;
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        return Ok(out);
    }
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    super::string_failure::array_layout::<Option<u32>>(selection.len())
        .map_err(MapLookupFailure::Operation)?;
    let mut indices = Vec::new();
    indices
        .try_reserve_exact(selection.len())
        .map_err(|source| {
            MapLookupFailure::Operation(super::string_failure::reservation::<Option<u32>>(
                selection.len(),
                source,
            ))
        })?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let probe = key_operations
        .bind_observed(key_arr, KeyRows::All, &mut |steps| {
            key_work(observer, steps)
        })
        .map_err(key_failure)?;
    for (ordinal, row) in selection.iter().enumerate() {
        observe(observer, CollectionObservation::Step)?;
        let row = rows(ordinal, row).map_err(MapLookupFailure::Control)?;
        validate_address(inputs, row).map_err(MapLookupFailure::Operation)?;
        if map.is_null(row.map) || is_null_probe(key_arr, row.key) {
            indices.push(None);
            continue;
        }
        let strict = row.check.is_some_and(|index| {
            let flags = check_arr.expect("validated flag address");
            flags.is_valid(index) && flags.value(index)
        });
        let candidates = offsets[row.map] as usize..offsets[row.map + 1] as usize;
        let keys = key_operations
            .bind_observed(keys, KeyRows::Range(candidates), &mut |steps| {
                key_work(observer, steps)
            })
            .map_err(key_failure)?;
        let value = match key_operations
            .lookup_observed(&keys, &probe, row.key, &mut |_| Ok(()), &mut |steps| {
                key_work(observer, steps)
            })
            .map_err(key_failure)?
        {
            KeyLookup::Found(index) => Some(index as u32),
            KeyLookup::Missing => {
                if strict {
                    row_error(ordinal, LookupRowFailure::MissingKey)
                        .map_err(MapLookupFailure::Operation)?;
                }
                None
            }
            KeyLookup::Indeterminate => {
                row_error(ordinal, LookupRowFailure::Indeterminate)
                    .map_err(MapLookupFailure::Operation)?;
                None
            }
        };
        indices.push(value);
    }
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let indices = UInt32Array::from(indices);
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    before_take(values.as_ref(), &indices).map_err(MapLookupFailure::Control)?;
    let out = take(values.as_ref(), &indices, None).map_err(|source| {
        MapLookupFailure::Operation(arrow_failure("element_at", ArrowSite::Take, source))
    })?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out = apply_indices_nulls(&out, &indices);
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out = cast_output(out, output_type, "element_at").map_err(MapLookupFailure::Operation)?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok(out)
}
fn key_work<E>(
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    steps: usize,
) -> Result<(), E> {
    for _ in 0..steps {
        observer(CollectionObservation::Step)?;
    }
    Ok(())
}

fn key_failure<E>(error: ValueFailure<E>) -> MapLookupFailure<E> {
    match error {
        ValueFailure::Caller(error) => MapLookupFailure::Control(error),
        ValueFailure::Value(error) => {
            MapLookupFailure::Operation(KernelFailure::from_domain(error))
        }
    }
}

fn is_null_probe(key: &ArrayRef, row: usize) -> bool {
    key.data_type() == &DataType::Null || key.is_null(row)
}

fn apply_indices_nulls(out: &ArrayRef, indices: &UInt32Array) -> ArrayRef {
    // NullArray already represents NULL at every position and cannot carry a
    // physical validity buffer. The lookup still ran before this storage repair.
    if out.data_type() == &DataType::Null {
        return out.clone();
    }
    let Some(idx_nulls) = indices.nulls() else {
        return out.clone();
    };
    if idx_nulls.null_count() == 0 {
        return out.clone();
    }
    let combined_nulls = match out.nulls() {
        Some(existing) => arrow_buffer::NullBuffer::union(Some(existing), Some(idx_nulls))
            .expect("combined null buffer is always Some"),
        None => idx_nulls.clone(),
    };
    let data = out.to_data().into_builder().nulls(Some(combined_nulls));
    arrow_array::make_array(unsafe { data.build_unchecked() })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LookupRowFailure {
    MissingKey,
    Indeterminate,
}
impl LookupRowFailure {
    pub fn message(&self) -> &str {
        match self {
            Self::MissingKey => "Key not present in map",
            Self::Indeterminate => "Map key comparison is indeterminate",
        }
    }
}
impl std::fmt::Display for LookupRowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for LookupRowFailure {}
#[derive(Debug)]
struct RequiredLookupRow {
    _ordinal: Option<usize>,
    cause: LookupRowFailure,
}
impl std::fmt::Display for RequiredLookupRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for RequiredLookupRow {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
impl FailureProjection for LookupRowFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::MissingKey => lookup_fact(FailureClass::Data, 377),
            Self::Indeterminate => FailureFact::new(FailureClass::Data, MAP_KEY_INDETERMINATE),
        }
    }
}
impl FailureProjection for RequiredLookupRow {
    fn failure_fact(&self) -> FailureFact {
        self.cause.failure_fact()
    }
}
impl novarocks_failure_contract::OuterFailureProjection for RequiredLookupRow {}
pub fn required_row_error(ordinal: usize, cause: LookupRowFailure) -> KernelFailure {
    KernelFailure::from_domain(RequiredLookupRow {
        _ordinal: Some(ordinal),
        cause,
    })
}
fn lookup_fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
    )
}
fn validate_address(inputs: &MapLookupInputs<'_>, rows: LookupRows) -> Result<(), KernelFailure> {
    if rows.map >= inputs.map.len() || rows.key >= inputs.key_arr.len() {
        return Err(contract(
            "map lookup demanded row is outside its physical carriers",
        ));
    }
    match (inputs.check_arr, rows.check) {
        (None, None) => Ok(()),
        (Some(flags), Some(index)) if index < flags.len() => Ok(()),
        _ => Err(contract(
            "map lookup flag address differs from its prepared input",
        )),
    }
}
