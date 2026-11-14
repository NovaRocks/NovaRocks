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

//! Metadata-free execution wrappers over the unchanged actual array buffers.
//!
//! This representation operation authors no logical domain, decodes no runtime
//! encoding and grants no capacity. The binding owner remains responsible for
//! its immutable semantic contract and every newly constructed wrapper.

use std::{borrow::Cow, sync::Arc};

use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, LargeListArray, LargeListViewArray,
    ListArray, ListViewArray, MapArray, StructArray, UnionArray, make_array,
};
use arrow_schema::{DataType, Field, FieldRef, Fields, UnionFields};
use novarocks_type_contract::{ArrowTypeDepth, ArrowTypeProfile};

mod failure;
pub use failure::{
    ArrayProjectionFailure, ArrayProjectionOperation, ArrayProjectionViolation, StringOffsetWidth,
};

mod construction;
pub use construction::{
    ARROW_BUFFER_OWNER_METADATA_BOUND, ArrayProjectionOpaqueBound,
    observe_array_normalization_construction,
};

mod required_dictionary_nulls;
mod typed_nulls;
pub use required_dictionary_nulls::validate_required_dictionary_children;
pub use typed_nulls::{
    validate_dictionary_rows_observed, validate_encoded_keys_and_root_nulls_observed,
};

mod value_hydration;
pub use value_hydration::{normalize_typed_array_plain_observed, observe_value_hydration};

/// An accurate structural occurrence within one actual array channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrayPathStep {
    ListItem,
    StructField(usize),
    MapKey,
    MapValue,
    UnionField(i8),
    DictionaryValue,
    RunEndValue,
}

/// Borrowed control/resource observation before the corresponding construction.
///
/// Type/field events describe only new structure. Array events precede any
/// child vector, ArrayData extraction or wrapper allocation for that array.
/// Buffer backing is reused, never estimated or re-authorized by these events.
#[derive(Clone, Copy, Debug)]
pub enum ArrayNormalizationStep<'a> {
    /// Value traversal checks control without spending a metadata work quota.
    ValueWork {
        units: usize,
    },
    ValueStructure {
        path: &'a [ArrayPathStep],
        event: novarocks_value_arrays::control::ValueStructure,
    },
    /// Work on channels; owned hydration admits its Vec separately.
    RebuildChunkScaffolds {
        channels: usize,
    },
    /// One actual fresh Vec<ArrayRef> in a slice construction.
    CreateChunkColumns {
        channels: usize,
    },
    /// One actual temporary Vec<ArrayPathStep>, admitted before allocation.
    CreateProjectionPath {
        steps: usize,
    },
    /// The sole new immutable Arc<RecordBatch> holder, before allocation.
    CreateRecordBatchWrapper,
    SliceArray {
        source: &'a ArrayRef,
        offset: usize,
        length: usize,
    },
    InspectDictionaryRows {
        source: &'a ArrayRef,
        row_start: usize,
        row_count: usize,
    },
    InspectType(&'a DataType),
    InspectConstructionWork {
        units: usize,
    },
    OpaqueOperationFinished,
    InspectField {
        source: &'a Field,
        name: &'a str,
    },
    RebuildType {
        source: &'a DataType,
    },
    RebuildField {
        source: &'a Field,
        target: &'a DataType,
        name: &'a str,
    },
    RebuildArray {
        path: &'a [ArrayPathStep],
        source: &'a ArrayRef,
        target: &'a DataType,
    },
}

pub fn normalize_execution_array(array: &ArrayRef) -> Result<ArrayRef, ArrayProjectionFailure> {
    normalize_execution_array_observed::<ArrayProjectionFailure, _>(array, |_| Ok(()))
}

/// Normalize an actual field representation without interpreting annotations.
pub fn normalize_execution_field_observed<E, F>(
    field: &FieldRef,
    mut observe: F,
) -> Result<FieldRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    normalization_preflight_observed(
        field.data_type(),
        ArrowTypeDepth::ROOT,
        &mut 0,
        &mut |source| observe(ArrayNormalizationStep::InspectType(source)),
    )?;
    normalize_field(field, field.name(), &mut observe)
}

/// Normalize under the actual caller's control and construction responsibility.
///
/// Rejected construction leaves the original array intact. Already normalized
/// input returns its original Arc after borrowed type inspection, without
/// extracting ArrayData or constructing another type/array tree.
pub fn normalize_execution_array_observed<E, F>(
    array: &ArrayRef,
    mut observe: F,
) -> Result<ArrayRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    normalization_preflight_observed(
        array.data_type(),
        ArrowTypeDepth::ROOT,
        &mut 0,
        &mut |source| observe(ArrayNormalizationStep::InspectType(source)),
    )?;
    let target = normalize_type(array.data_type(), &mut observe)?;
    match target {
        Cow::Borrowed(_) => Ok(Arc::clone(array)),
        Cow::Owned(target) => rebuild_array_observed(array, &target, &mut observe),
    }
}

fn normalization_preflight_observed<E: From<ArrayProjectionFailure>>(
    source: &DataType,
    depth: ArrowTypeDepth,
    nodes: &mut usize,
    observe: &mut impl FnMut(&DataType) -> Result<(), E>,
) -> Result<(), E> {
    observe(source)?;
    depth
        .check(ArrowTypeProfile::Value)
        .map_err(|error| E::from(ArrayProjectionFailure::ValueType(error)))?;
    *nodes = nodes.checked_add(1).ok_or_else(|| {
        E::from(ArrayProjectionFailure::ValueType(
            novarocks_type_contract::ValueTypeError::TooManyNodes,
        ))
    })?;
    if *nodes > novarocks_type_contract::MAX_VALUE_TYPE_NODES {
        return Err(E::from(ArrayProjectionFailure::ValueType(
            novarocks_type_contract::ValueTypeError::TooManyNodes,
        )));
    }
    let mut child = |ty: &DataType, child_depth: Result<ArrowTypeDepth, _>| {
        normalization_preflight_observed(
            ty,
            child_depth.map_err(|error| E::from(ArrayProjectionFailure::ValueType(error)))?,
            nodes,
            observe,
        )
    };
    match source {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => child(field.data_type(), depth.value_child())?,
        DataType::Map(field, _) => child(field.data_type(), depth.map_entries())?,
        DataType::Struct(fields) => {
            for field in fields {
                child(field.data_type(), depth.value_child())?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                child(field.data_type(), depth.value_child())?;
            }
        }
        DataType::Dictionary(keys, values) => {
            child(keys, depth.encoding_child())?;
            child(values, depth.encoding_child())?;
        }
        DataType::RunEndEncoded(ends, values) => {
            child(ends.data_type(), depth.encoding_child())?;
            child(values.data_type(), depth.encoding_child())?;
        }
        _ => {}
    }
    Ok(())
}

/// Borrowed canonical-representation validation for already-authored schemas.
/// Canonical execution metadata has no Arc backing. Arrow 60 represents
/// empty metadata as None, including after removal and conversion.
pub fn execution_field_is_normalized(field: &Field) -> Result<bool, ArrayProjectionFailure> {
    execution_field_is_normalized_observed(field, &mut |_| Ok(()))
}

pub fn execution_field_is_normalized_observed<E: From<ArrayProjectionFailure>>(
    field: &Field,
    work: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    work(1)?;
    normalization_preflight_observed(field.data_type(), ArrowTypeDepth::ROOT, &mut 0, &mut |_| {
        work(1)
    })?;
    Ok(field.metadata().as_arc().is_none()
        && !needs_normalization(field.data_type(), &mut |step| match step {
            ArrayNormalizationStep::InspectField { source, name } => {
                let units = source
                    .name()
                    .len()
                    .checked_add(name.len())
                    .and_then(|n| n.checked_add(1))
                    .ok_or_else(|| E::from(ArrayProjectionFailure::RequestArithmetic))?;
                work(units)
            }
            _ => work(1),
        })?)
}

fn field_needs_normalization<E, F>(field: &Field, name: &str, observe: &mut F) -> Result<bool, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    observe(ArrayNormalizationStep::InspectField {
        source: field,
        name,
    })?;
    let nested = needs_normalization(field.data_type(), observe)?;
    Ok(field.metadata().as_arc().is_some() || field.name() != name || nested)
}

fn needs_normalization<E, F>(source: &DataType, observe: &mut F) -> Result<bool, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    observe(ArrayNormalizationStep::InspectType(source))?;
    let mut changed = false;
    match source {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            changed = field_needs_normalization(field, "item", observe)?;
        }
        DataType::Struct(fields) => {
            for field in fields {
                changed |= field_needs_normalization(field, field.name(), observe)?;
            }
        }
        DataType::Map(entries, _) => {
            changed |= entries.metadata().as_arc().is_some() || entries.name() != "entries";
            observe(ArrayNormalizationStep::InspectType(entries.data_type()))?;
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::MapEntriesType,
                )));
            };
            if fields.len() != 2 {
                return Err(E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::MapEntriesArity,
                )));
            }
            changed |= field_needs_normalization(&fields[0], "key", observe)?;
            changed |= field_needs_normalization(&fields[1], "value", observe)?;
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                changed |= field_needs_normalization(field, field.name(), observe)?;
            }
        }
        DataType::Dictionary(keys, values) => {
            changed |= needs_normalization(keys, observe)?;
            changed |= needs_normalization(values, observe)?;
        }
        DataType::RunEndEncoded(ends, values) => {
            changed |= field_needs_normalization(ends, ends.name(), observe)?;
            changed |= field_needs_normalization(values, values.name(), observe)?;
        }
        _ => {}
    }
    Ok(changed)
}

fn normalize_type<'a, E, F>(source: &'a DataType, observe: &mut F) -> Result<Cow<'a, DataType>, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'b> FnMut(ArrayNormalizationStep<'b>) -> Result<(), E>,
{
    if !needs_normalization(source, observe)? {
        return Ok(Cow::Borrowed(source));
    }
    observe(ArrayNormalizationStep::RebuildType { source })?;
    let target = match source {
        DataType::List(field) => DataType::List(normalize_field(field, "item", observe)?),
        DataType::LargeList(field) => DataType::LargeList(normalize_field(field, "item", observe)?),
        DataType::ListView(field) => DataType::ListView(normalize_field(field, "item", observe)?),
        DataType::LargeListView(field) => {
            DataType::LargeListView(normalize_field(field, "item", observe)?)
        }
        DataType::FixedSizeList(field, length) => {
            DataType::FixedSizeList(normalize_field(field, "item", observe)?, *length)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| normalize_field(field, field.name(), observe))
                .collect::<Result<Fields, E>>()?,
        ),
        DataType::Map(entries, ordered) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::MapEntriesType,
                )));
            };
            if fields.len() != 2 {
                return Err(E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::MapEntriesArity,
                )));
            }
            observe(ArrayNormalizationStep::RebuildType {
                source: entries.data_type(),
            })?;
            let entries_type = DataType::Struct(
                vec![
                    normalize_field(&fields[0], "key", observe)?,
                    normalize_field(&fields[1], "value", observe)?,
                ]
                .into(),
            );
            let entries = rebuild_field(entries, &entries_type, "entries", observe)?;
            DataType::Map(entries, *ordered)
        }
        DataType::Union(fields, mode) => {
            let fields = fields
                .iter()
                .map(|(id, field)| Ok((id, normalize_field(field, field.name(), observe)?)))
                .collect::<Result<UnionFields, E>>()?;
            DataType::Union(fields, *mode)
        }
        DataType::Dictionary(keys, values) => DataType::Dictionary(
            Box::new(normalize_type(keys, observe)?.into_owned()),
            Box::new(normalize_type(values, observe)?.into_owned()),
        ),
        DataType::RunEndEncoded(ends, values) => DataType::RunEndEncoded(
            normalize_field(ends, ends.name(), observe)?,
            normalize_field(values, values.name(), observe)?,
        ),
        _ => {
            return Err(E::from(ArrayProjectionFailure::Contract(
                ArrayProjectionViolation::ScalarChanged,
            )));
        }
    };
    Ok(Cow::Owned(target))
}

fn normalize_field<E, F>(source: &FieldRef, name: &str, observe: &mut F) -> Result<FieldRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    let target = normalize_type(source.data_type(), observe)?;
    if matches!(target, Cow::Borrowed(_))
        && source.metadata().as_arc().is_none()
        && source.name() == name
    {
        return Ok(Arc::clone(source));
    }
    rebuild_field(source, target.as_ref(), name, observe)
}

pub fn rebuild_field<E, F>(
    source: &Field,
    target: &DataType,
    name: &str,
    observe: &mut F,
) -> Result<FieldRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    rebuild_field_with_nullability(source, target, name, source.is_nullable(), observe)
}

#[allow(
    deprecated,
    reason = "Actual IPC dictionary descriptors belong to the source field."
)]
fn rebuild_field_with_nullability<E, F>(
    source: &Field,
    target: &DataType,
    name: &str,
    nullable: bool,
    observe: &mut F,
) -> Result<FieldRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    observe(ArrayNormalizationStep::RebuildField {
        source,
        target,
        name,
    })?;
    let field = if matches!(target, DataType::Dictionary(..)) {
        Field::new_dict(
            name,
            target.clone(),
            nullable,
            source.dict_id().ok_or_else(|| {
                E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::MissingDictionaryId,
                ))
            })?,
            source.dict_is_ordered().ok_or_else(|| {
                E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::MissingDictionaryOrder,
                ))
            })?,
        )
    } else {
        Field::new(name, target.clone(), nullable)
    };
    Ok(Arc::new(field))
}

/// Merge only physical child NULL declarations of normalized execution
/// carriers. Complete value bindings remain with the caller. Neither source
/// data nor semantic requiredness is inferred from these Arrow fields.
pub fn merge_execution_carriers_observed<'a, E, F>(
    left: &'a DataType,
    right: &DataType,
    observe: &mut F,
) -> Result<Cow<'a, DataType>, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'b> FnMut(ArrayNormalizationStep<'b>) -> Result<(), E>,
{
    for source in [left, right] {
        normalization_preflight_observed(source, ArrowTypeDepth::ROOT, &mut 0, &mut |source| {
            observe(ArrayNormalizationStep::InspectType(source))
        })?;
    }
    merge_carrier_type(left, right, observe)
}

#[allow(
    deprecated,
    reason = "Actual IPC dictionary descriptors remain exact carrier facts."
)]
fn merge_carrier_field<E, F>(
    left: &FieldRef,
    right: &FieldRef,
    observe: &mut F,
) -> Result<FieldRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    observe(ArrayNormalizationStep::InspectField {
        source: left,
        name: right.name(),
    })?;
    if left.name() != right.name() || !left.metadata().is_empty() || !right.metadata().is_empty() {
        return Err(E::from(ArrayProjectionFailure::Contract(
            ArrayProjectionViolation::ScalarChanged,
        )));
    }
    if matches!(left.data_type(), DataType::Dictionary(..))
        && (left.dict_id() != right.dict_id() || left.dict_is_ordered() != right.dict_is_ordered())
    {
        return Err(E::from(ArrayProjectionFailure::Contract(
            ArrayProjectionViolation::ScalarChanged,
        )));
    }
    let target = merge_carrier_type(left.data_type(), right.data_type(), observe)?;
    let nullable = left.is_nullable() || right.is_nullable();
    if matches!(target, Cow::Borrowed(_)) && nullable == left.is_nullable() {
        return Ok(Arc::clone(left));
    }
    rebuild_field_with_nullability(left, target.as_ref(), left.name(), nullable, observe)
}

enum CarrierMergeError<E> {
    Caller(E),
    ValueType(novarocks_type_contract::ValueTypeError),
}
impl<E> From<novarocks_type_contract::ValueTypeError> for CarrierMergeError<E> {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}

fn merge_carrier_type<'a, E, F>(
    left: &'a DataType,
    right: &DataType,
    observe: &mut F,
) -> Result<Cow<'a, DataType>, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'b> FnMut(ArrayNormalizationStep<'b>) -> Result<(), E>,
{
    let same = novarocks_type_contract::arrow_data_types_exact_with_profile_observed::<
        CarrierMergeError<E>,
    >(left, right, ArrowTypeProfile::Value, || {
        observe(ArrayNormalizationStep::InspectConstructionWork { units: 1 })
            .map_err(CarrierMergeError::Caller)
    })
    .map_err(|error| match error {
        CarrierMergeError::Caller(error) => error,
        CarrierMergeError::ValueType(error) => E::from(ArrayProjectionFailure::ValueType(error)),
    })?;
    if same {
        return Ok(Cow::Borrowed(left));
    }
    observe(ArrayNormalizationStep::RebuildType { source: left })?;
    let invalid = || {
        E::from(ArrayProjectionFailure::Contract(
            ArrayProjectionViolation::ScalarChanged,
        ))
    };
    let target = match (left, right) {
        (DataType::List(a), DataType::List(b)) => {
            DataType::List(merge_carrier_field(a, b, observe)?)
        }
        (DataType::LargeList(a), DataType::LargeList(b)) => {
            DataType::LargeList(merge_carrier_field(a, b, observe)?)
        }
        (DataType::ListView(a), DataType::ListView(b)) => {
            DataType::ListView(merge_carrier_field(a, b, observe)?)
        }
        (DataType::LargeListView(a), DataType::LargeListView(b)) => {
            DataType::LargeListView(merge_carrier_field(a, b, observe)?)
        }
        (DataType::FixedSizeList(a, al), DataType::FixedSizeList(b, bl)) if al == bl => {
            DataType::FixedSizeList(merge_carrier_field(a, b, observe)?, *al)
        }
        (DataType::Struct(a), DataType::Struct(b)) if a.len() == b.len() => DataType::Struct(
            a.iter()
                .zip(b)
                .map(|(a, b)| merge_carrier_field(a, b, observe))
                .collect::<Result<Fields, E>>()?,
        ),
        (DataType::Map(a, ao), DataType::Map(b, bo)) if ao == bo => {
            for entries in [a, b] {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(invalid());
                };
                if entries.is_nullable() || fields.len() != 2 || fields[0].is_nullable() {
                    return Err(invalid());
                }
            }
            DataType::Map(merge_carrier_field(a, b, observe)?, *ao)
        }
        (DataType::Union(a, am), DataType::Union(b, bm)) if am == bm && a.len() == b.len() => {
            let fields = a
                .iter()
                .zip(b.iter())
                .map(|((aid, a), (bid, b))| {
                    if aid != bid {
                        return Err(invalid());
                    }
                    Ok((aid, merge_carrier_field(a, b, observe)?))
                })
                .collect::<Result<UnionFields, E>>()?;
            DataType::Union(fields, *am)
        }
        (DataType::Dictionary(ak, av), DataType::Dictionary(bk, bv)) if ak == bk => {
            DataType::Dictionary(
                ak.clone(),
                Box::new(merge_carrier_type(av, bv, observe)?.into_owned()),
            )
        }
        (DataType::RunEndEncoded(ae, av), DataType::RunEndEncoded(be, bv)) => {
            DataType::RunEndEncoded(
                merge_carrier_field(ae, be, observe)?,
                merge_carrier_field(av, bv, observe)?,
            )
        }
        _ => return Err(invalid()),
    };
    Ok(Cow::Owned(target))
}

fn physical<T: 'static, E: From<ArrayProjectionFailure>>(array: &ArrayRef) -> Result<&T, E> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        E::from(ArrayProjectionFailure::Contract(
            ArrayProjectionViolation::PhysicalWrapper,
        ))
    })
}

pub fn rebuild_array_observed<E, F>(
    source: &ArrayRef,
    target: &DataType,
    observe: &mut F,
) -> Result<ArrayRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    rebuild_array_at(source, target, observe, &[], ArrowTypeDepth::ROOT)
}

fn rebuild_array_at<E, F>(
    source: &ArrayRef,
    target: &DataType,
    observe: &mut F,
    path: &[ArrayPathStep],
    depth: ArrowTypeDepth,
) -> Result<ArrayRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    depth
        .check(ArrowTypeProfile::Value)
        .map_err(|error| E::from(ArrayProjectionFailure::ValueType(error)))?;
    // Arrow equality does not compare backing capacity of empty metadata maps.
    // Reusing such a wrapper would retain the external map allocation even
    // though its entries compare equal to the canonical empty representation.
    if source.data_type() == target && !needs_normalization(source.data_type(), observe)? {
        return Ok(Arc::clone(source));
    }
    observe(ArrayNormalizationStep::RebuildArray {
        path,
        source,
        target,
    })?;
    if requires_default_validated_container(target) {
        // Metadata/role rebuilding must not add a high-level whole-values
        // logical NULL rule to a safely validated source representation.
        // The typed ingress owner checks required logical values with all
        // active ancestor masks; this operation authors no semantic promise.
        let result = rebuild_default_validated_container(source, target, observe, path, depth)?;
        observe(ArrayNormalizationStep::OpaqueOperationFinished)?;
        return Ok(result);
    }
    let invalid = |error: arrow_schema::ArrowError| {
        E::from(ArrayProjectionFailure::Arrow {
            operation: ArrayProjectionOperation::RebuildArray,
            source: error,
        })
    };
    let array: ArrayRef = match target {
        DataType::Struct(fields) => {
            let source = physical::<StructArray, E>(source)?;
            let mut children = Vec::with_capacity(fields.len());
            for (index, (child, field)) in source.columns().iter().zip(fields).enumerate() {
                children.push(rebuild_array_child(
                    child,
                    field.data_type(),
                    observe,
                    path,
                    depth,
                    ArrayPathStep::StructField(index),
                )?);
            }
            Arc::new(
                StructArray::try_new_with_length(
                    fields.clone(),
                    children,
                    source.nulls().cloned(),
                    source.len(),
                )
                .map_err(invalid)?,
            )
        }
        DataType::List(field) => {
            let source = physical::<ListArray, E>(source)?;
            let values = rebuild_array_child(
                source.values(),
                field.data_type(),
                observe,
                path,
                depth,
                ArrayPathStep::ListItem,
            )?;
            Arc::new(
                ListArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )
                .map_err(invalid)?,
            )
        }
        DataType::LargeList(field) => {
            let source = physical::<LargeListArray, E>(source)?;
            let values = rebuild_array_child(
                source.values(),
                field.data_type(),
                observe,
                path,
                depth,
                ArrayPathStep::ListItem,
            )?;
            Arc::new(
                LargeListArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )
                .map_err(invalid)?,
            )
        }
        DataType::ListView(field) => {
            let source = physical::<ListViewArray, E>(source)?;
            let values = rebuild_array_child(
                source.values(),
                field.data_type(),
                observe,
                path,
                depth,
                ArrayPathStep::ListItem,
            )?;
            Arc::new(
                ListViewArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    source.sizes().clone(),
                    values,
                    source.nulls().cloned(),
                )
                .map_err(invalid)?,
            )
        }
        DataType::LargeListView(field) => {
            let source = physical::<LargeListViewArray, E>(source)?;
            let values = rebuild_array_child(
                source.values(),
                field.data_type(),
                observe,
                path,
                depth,
                ArrayPathStep::ListItem,
            )?;
            Arc::new(
                LargeListViewArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    source.sizes().clone(),
                    values,
                    source.nulls().cloned(),
                )
                .map_err(invalid)?,
            )
        }
        DataType::FixedSizeList(field, length) => {
            let source = physical::<FixedSizeListArray, E>(source)?;
            let values = rebuild_array_child(
                source.values(),
                field.data_type(),
                observe,
                path,
                depth,
                ArrayPathStep::ListItem,
            )?;
            Arc::new(
                FixedSizeListArray::try_new_with_length(
                    Arc::clone(field),
                    *length,
                    values,
                    source.nulls().cloned(),
                    source.len(),
                )
                .map_err(invalid)?,
            )
        }
        DataType::Map(entries, ordered) => {
            let source = physical::<MapArray, E>(source)?;
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::MapEntriesType,
                )));
            };
            let children = source
                .entries()
                .columns()
                .iter()
                .zip(fields)
                .enumerate()
                .map(|(index, (child, field))| {
                    rebuild_array_child(
                        child,
                        field.data_type(),
                        observe,
                        path,
                        depth,
                        if index == 0 {
                            ArrayPathStep::MapKey
                        } else {
                            ArrayPathStep::MapValue
                        },
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let entries_array = StructArray::try_new_with_length(
                fields.clone(),
                children,
                source.entries().nulls().cloned(),
                source.entries().len(),
            )
            .map_err(invalid)?;
            Arc::new(
                MapArray::try_new(
                    Arc::clone(entries),
                    source.offsets().clone(),
                    entries_array,
                    source.nulls().cloned(),
                    *ordered,
                )
                .map_err(invalid)?,
            )
        }
        DataType::Union(fields, _) => {
            // Arrow's validated Union constructor rebuilds child wrappers
            // through ArrayData. It preserves their buffers, but its public
            // API does not retain the supplied child Arc identities.
            let source = physical::<UnionArray, E>(source)?;
            let children = fields
                .iter()
                .map(|(id, field)| {
                    rebuild_array_child(
                        source.child(id),
                        field.data_type(),
                        observe,
                        path,
                        depth,
                        ArrayPathStep::UnionField(id),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Arc::new(
                UnionArray::try_new(
                    fields.clone(),
                    source.type_ids().clone(),
                    source.offsets().cloned(),
                    children,
                )
                .map_err(invalid)?,
            )
        }
        DataType::Dictionary(keys, values) => {
            macro_rules! dictionary {
                ($key:ty) => {
                    rebuild_dictionary::<$key, E, F>(source, values, observe, path, depth)?
                };
            }
            match keys.as_ref() {
                DataType::Int8 => dictionary!(Int8Type),
                DataType::Int16 => dictionary!(Int16Type),
                DataType::Int32 => dictionary!(Int32Type),
                DataType::Int64 => dictionary!(Int64Type),
                DataType::UInt8 => dictionary!(UInt8Type),
                DataType::UInt16 => dictionary!(UInt16Type),
                DataType::UInt32 => dictionary!(UInt32Type),
                DataType::UInt64 => dictionary!(UInt64Type),
                _ => {
                    return Err(E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::DictionaryKeyType,
                    )));
                }
            }
        }
        DataType::RunEndEncoded(_, values) => {
            // Arrow's public validated RunArray constructor fixes its role
            // names/nullability. Preserve the actual encoding fields instead
            // through the default validated ArrayData builder. This retains
            // buffers but does not promise values' existing ArrayRef identity.
            let data = source.to_data();
            let children = data.child_data();
            if children.len() != 2 {
                return Err(E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::RunEndChildren,
                )));
            }
            let values_array = make_array(children[1].clone());
            let normalized_values = rebuild_array_child(
                &values_array,
                values.data_type(),
                observe,
                path,
                depth,
                ArrayPathStep::RunEndValue,
            )?;
            let children = vec![children[0].clone(), normalized_values.to_data()];
            let data = data
                .into_builder()
                .data_type(target.clone())
                .child_data(children)
                .build()
                .map_err(invalid)?;
            make_array(data)
        }
        _ => {
            return Err(E::from(ArrayProjectionFailure::Contract(
                ArrayProjectionViolation::ScalarRebuild,
            )));
        }
    };
    observe(ArrayNormalizationStep::OpaqueOperationFinished)?;
    Ok(array)
}

pub(crate) fn requires_default_validated_container(target: &DataType) -> bool {
    novarocks_value_arrays::storage::requires_default_validated_container(target)
}

fn rebuild_default_validated_container<E, F>(
    source: &ArrayRef,
    target: &DataType,
    observe: &mut F,
    path: &[ArrayPathStep],
    depth: ArrowTypeDepth,
) -> Result<ArrayRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    let data = source.to_data();
    let mut children = Vec::with_capacity(data.child_data().len());
    match target {
        DataType::Struct(fields) => {
            let array = physical::<StructArray, E>(source)?;
            for (index, (child, field)) in array.columns().iter().zip(fields).enumerate() {
                children.push(
                    rebuild_array_child(
                        child,
                        field.data_type(),
                        observe,
                        path,
                        depth,
                        ArrayPathStep::StructField(index),
                    )?
                    .to_data(),
                );
            }
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            let values = match source.data_type() {
                DataType::List(_) => physical::<ListArray, E>(source)?.values(),
                DataType::LargeList(_) => physical::<LargeListArray, E>(source)?.values(),
                DataType::FixedSizeList(_, _) => {
                    physical::<FixedSizeListArray, E>(source)?.values()
                }
                _ => {
                    return Err(E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::PhysicalWrapper,
                    )));
                }
            };
            children.push(
                rebuild_array_child(
                    values,
                    field.data_type(),
                    observe,
                    path,
                    depth,
                    ArrayPathStep::ListItem,
                )?
                .to_data(),
            );
        }
        DataType::Map(entries, _) => {
            let array = physical::<MapArray, E>(source)?;
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::MapEntriesType,
                )));
            };
            let mut entries_children = Vec::with_capacity(2);
            for (index, (child, field)) in array.entries().columns().iter().zip(fields).enumerate()
            {
                entries_children.push(
                    rebuild_array_child(
                        child,
                        field.data_type(),
                        observe,
                        path,
                        depth,
                        if index == 0 {
                            ArrayPathStep::MapKey
                        } else {
                            ArrayPathStep::MapValue
                        },
                    )?
                    .to_data(),
                );
            }
            children.push(
                array
                    .entries()
                    .to_data()
                    .into_builder()
                    .data_type(entries.data_type().clone())
                    .child_data(entries_children)
                    .build()
                    .map_err(|error| {
                        E::from(ArrayProjectionFailure::Arrow {
                            operation: ArrayProjectionOperation::RebuildMapEntries,
                            source: error,
                        })
                    })?,
            );
        }
        _ => {
            return Err(E::from(ArrayProjectionFailure::Contract(
                ArrayProjectionViolation::UnsupportedContainer,
            )));
        }
    }
    // This branch shares all payload/null/offset buffers. Arrow's public
    // make_array reconstructs child wrappers; their Arc identity is not shared.
    let data = data
        .into_builder()
        .data_type(target.clone())
        .child_data(children)
        .build()
        .map_err(|error| {
            E::from(ArrayProjectionFailure::Arrow {
                operation: ArrayProjectionOperation::RebuildArray,
                source: error,
            })
        })?;
    Ok(make_array(data))
}

fn rebuild_array_child<E, F>(
    source: &ArrayRef,
    target: &DataType,
    observe: &mut F,
    path: &[ArrayPathStep],
    depth: ArrowTypeDepth,
    child: ArrayPathStep,
) -> Result<ArrayRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    // The actual path can include encoding and Map scaffolding beyond the
    // logical contract. Its temporary storage follows the observed path length,
    // never a second local depth/wrapper quota or a derived fixed capacity.
    let depth = match child {
        ArrayPathStep::MapKey | ArrayPathStep::MapValue => {
            depth.map_entries().and_then(ArrowTypeDepth::value_child)
        }
        ArrayPathStep::DictionaryValue | ArrayPathStep::RunEndValue => depth.encoding_child(),
        _ => depth.value_child(),
    }
    .map_err(|error| E::from(ArrayProjectionFailure::ValueType(error)))?;
    depth
        .check(ArrowTypeProfile::Value)
        .map_err(|error| E::from(ArrayProjectionFailure::ValueType(error)))?;
    let steps = path
        .len()
        .checked_add(1)
        .ok_or_else(|| E::from(ArrayProjectionFailure::RequestArithmetic))?;
    let layout = std::alloc::Layout::array::<ArrayPathStep>(steps).map_err(|source| {
        E::from(ArrayProjectionFailure::ArrayLayout {
            count: steps,
            width: std::mem::size_of::<ArrayPathStep>(),
            source,
        })
    })?;
    observe(ArrayNormalizationStep::InspectConstructionWork {
        units: layout.size(),
    })?;
    observe(ArrayNormalizationStep::CreateProjectionPath { steps })?;
    let mut next = Vec::with_capacity(steps);
    next.extend_from_slice(path);
    next.push(child);
    rebuild_array_at(source, target, observe, &next, depth)
}

fn rebuild_dictionary<K, E, F>(
    source: &ArrayRef,
    target_values: &DataType,
    observe: &mut F,
    path: &[ArrayPathStep],
    depth: ArrowTypeDepth,
) -> Result<ArrayRef, E>
where
    K: ArrowDictionaryKeyType,
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    let source = physical::<DictionaryArray<K>, E>(source)?;
    let values = rebuild_array_child(
        source.values(),
        target_values,
        observe,
        path,
        depth,
        ArrayPathStep::DictionaryValue,
    )?;
    Ok(Arc::new(
        DictionaryArray::<K>::try_new(source.keys().clone(), values).map_err(|error| {
            E::from(ArrayProjectionFailure::Arrow {
                operation: ArrayProjectionOperation::RebuildDictionary,
                source: error,
            })
        })?,
    ))
}

#[cfg(test)]
mod tests {
    use arrow_array::{BinaryArray, Int32Array, Int64Array, RunArray, StringArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::UnionMode;

    use super::*;

    fn annotated(name: &str, data_type: DataType, nullable: bool) -> FieldRef {
        Arc::new(
            Field::new(name, data_type, nullable).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([
                ("nr_logical_type".into(), "uninterpreted".into()),
                ("PARQUET:field_id".into(), "17".into()),
                (
                    "ARROW:extension:name".into(),
                    "arrow.parquet.variant".into(),
                ),
            ])),
        )
    }

    fn assert_clean(data_type: &DataType) {
        let field = |field: &Field| {
            assert!(field.metadata().as_arc().is_none());
            assert_clean(field.data_type());
        };
        match data_type {
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _) => {
                assert_eq!(item.name(), "item");
                field(item);
            }
            DataType::Struct(fields) => {
                for child in fields {
                    field(child);
                }
            }
            DataType::Map(entries, _) => {
                assert_eq!(entries.name(), "entries");
                field(entries);
                let DataType::Struct(fields) = entries.data_type() else {
                    panic!("Struct expected");
                };
                assert_eq!(fields[0].name(), "key");
                assert_eq!(fields[1].name(), "value");
            }
            DataType::Union(fields, _) => {
                for (_, child) in fields.iter() {
                    field(child);
                }
            }
            DataType::Dictionary(keys, values) => {
                assert_clean(keys);
                assert_clean(values);
            }
            DataType::RunEndEncoded(ends, values) => {
                field(ends);
                field(values);
            }
            _ => {}
        }
    }

    #[test]
    fn cleared_hashmap_becomes_no_metadata_backing_and_reuses_canonical_source() {
        let mut metadata = std::collections::HashMap::with_capacity(32);
        metadata.insert("external".into(), "source".into());
        metadata.clear();
        assert!(metadata.capacity() >= 32);
        let field = Arc::new(Field::new("payload", DataType::Utf8, true).with_metadata(metadata));
        assert!(field.metadata().is_empty());
        assert!(field.metadata().as_arc().is_none());
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let source: ArrayRef = Arc::new(StructArray::new(
            vec![field.clone()].into(),
            vec![values.clone()],
            None,
        ));
        let normalized = normalize_execution_array(&source).unwrap();
        assert!(Arc::ptr_eq(&source, &normalized));
        let normalized_struct = normalized.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(Arc::ptr_eq(&values, normalized_struct.column(0)));
        assert!(Arc::ptr_eq(&field, &normalized_struct.fields()[0]));
        assert_clean(normalized.data_type());
        assert!(field.metadata().as_arc().is_none());
        assert_buffer_sharing(&source.to_data(), &normalized.to_data());
        let clean = normalize_execution_array(&normalized).unwrap();
        assert!(Arc::ptr_eq(&normalized, &clean));
    }

    fn assert_buffer_sharing(before: &arrow_data::ArrayData, after: &arrow_data::ArrayData) {
        assert_eq!(before.len(), after.len());
        assert_eq!(before.offset(), after.offset());
        assert_eq!(before.buffers().len(), after.buffers().len());
        for (left, right) in before.buffers().iter().zip(after.buffers()) {
            assert_eq!(left.as_ptr(), right.as_ptr());
            assert_eq!(left.len(), right.len());
        }
        if let (Some(left), Some(right)) = (before.nulls(), after.nulls()) {
            assert_eq!(left.buffer().as_ptr(), right.buffer().as_ptr());
        }
        assert_eq!(before.child_data().len(), after.child_data().len());
        for (left, right) in before.child_data().iter().zip(after.child_data()) {
            assert_buffer_sharing(left, right);
        }
    }

    #[test]
    fn nested_list_and_struct_keep_slices_nulls_payload_and_unchanged_siblings() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("bbb"),
            Some("c"),
        ]));
        let list: ArrayRef = Arc::new(
            ListArray::try_new(
                annotated("element", DataType::Utf8, true),
                OffsetBuffer::new(vec![0i32, 1, 2, 4].into()),
                Arc::clone(&strings),
                Some(NullBuffer::from(vec![true, false, true])),
            )
            .unwrap(),
        );
        let integers: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let source: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![
                    annotated("text", list.data_type().clone(), true),
                    Arc::new(Field::new("stable", DataType::Int64, false)),
                ]
                .into(),
                vec![list, integers],
                Some(NullBuffer::from(vec![false, true, true])),
            )
            .unwrap(),
        );
        let source = source.slice(1, 2);
        let output = normalize_execution_array(&source).unwrap();
        assert_clean(output.data_type());
        assert_buffer_sharing(&source.to_data(), &output.to_data());
        let original = source.as_any().downcast_ref::<StructArray>().unwrap();
        let normalized = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(Arc::ptr_eq(original.column(1), normalized.column(1)));
        let list = normalized
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(Arc::ptr_eq(list.values(), &strings));
        assert!(list.is_null(0));
        assert_eq!(list.value_length(1), 2);
        assert!(Arc::ptr_eq(
            &output,
            &normalize_execution_array(&output).unwrap()
        ));
    }

    #[test]
    fn map_roles_normalize_without_replacing_key_or_value_arrays() {
        let keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let entries = StructArray::try_new(
            vec![
                annotated("k", DataType::Int32, false),
                annotated("v", DataType::Utf8, true),
            ]
            .into(),
            vec![Arc::clone(&keys), Arc::clone(&values)],
            None,
        )
        .unwrap();
        let source: ArrayRef = Arc::new(
            MapArray::try_new(
                annotated("pairs", entries.data_type().clone(), false),
                OffsetBuffer::new(vec![0i32, 0, 1, 2].into()),
                entries,
                Some(NullBuffer::from(vec![false, true, true])),
                true,
            )
            .unwrap(),
        );
        let output = normalize_execution_array(&source).unwrap();
        assert_clean(output.data_type());
        assert_buffer_sharing(&source.to_data(), &output.to_data());
        let map = output.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(Arc::ptr_eq(map.keys(), &keys));
        assert!(Arc::ptr_eq(map.values(), &values));
        assert!(map.is_null(0));
        assert_eq!(map.value_length(1), 1);
    }

    #[test]
    #[allow(deprecated)]
    fn dictionary_encodings_and_field_dictionary_descriptors_remain_actual() {
        let payload: ArrayRef = Arc::new(StringArray::from(vec!["a", "bbb"]));
        let nested: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![annotated("text", DataType::Utf8, false)].into(),
                vec![payload],
                None,
            )
            .unwrap(),
        );
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(1), None, Some(0)]),
                nested,
            )
            .unwrap(),
        );
        let field = Arc::new(
            Field::new_dict("codes", dictionary.data_type().clone(), true, 42, true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "source".into(),
                    "kept externally".into(),
                )]),
            ),
        );
        let source: ArrayRef =
            Arc::new(StructArray::try_new(vec![field].into(), vec![dictionary], None).unwrap());
        let output = normalize_execution_array(&source).unwrap();
        assert_clean(output.data_type());
        assert_buffer_sharing(&source.to_data(), &output.to_data());
        let structure = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(structure.fields()[0].dict_id(), Some(42));
        assert_eq!(structure.fields()[0].dict_is_ordered(), Some(true));
        let dictionary = structure
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dictionary.keys().value(0), 1);
        assert!(dictionary.is_null(1));
        assert_eq!(dictionary.values().len(), 2);
    }

    #[test]
    fn run_end_encoding_and_its_slice_coordinates_are_preserved() {
        let values: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![annotated("v", DataType::Utf8, true)].into(),
                vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
                None,
            )
            .unwrap(),
        );
        let source: ArrayRef = Arc::new(
            RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 5]), values.as_ref()).unwrap(),
        );
        let source = source.slice(1, 3);
        let output = normalize_execution_array(&source).unwrap();
        assert_clean(output.data_type());
        assert!(matches!(output.data_type(), DataType::RunEndEncoded(..)));
        assert_buffer_sharing(&source.to_data(), &output.to_data());
        let run = output
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();
        assert_eq!(run.run_ends().offset(), 1);
        assert_eq!(run.len(), 3);
    }

    #[test]
    fn union_mode_ids_names_and_unchanged_child_buffers_survive_normalization() {
        let stable: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let nested: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![annotated("inner", DataType::Utf8, false)].into(),
                vec![Arc::new(StringArray::from(vec!["a"]))],
                None,
            )
            .unwrap(),
        );
        let fields = UnionFields::try_new(
            [1, 3],
            [
                Arc::new(Field::new("integer", DataType::Int64, false)),
                annotated("record", nested.data_type().clone(), false),
            ],
        )
        .unwrap();
        let source: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields,
                ScalarBuffer::from(vec![1i8, 3]),
                Some(ScalarBuffer::from(vec![0i32, 0])),
                vec![Arc::clone(&stable), nested],
            )
            .unwrap(),
        );
        let output = normalize_execution_array(&source).unwrap();
        assert_clean(output.data_type());
        assert_buffer_sharing(&source.to_data(), &output.to_data());
        let DataType::Union(fields, UnionMode::Dense) = output.data_type() else {
            panic!("dense Union expected");
        };
        assert_eq!(
            fields
                .iter()
                .map(|(id, field)| (id, field.name().as_str()))
                .collect::<Vec<_>>(),
            [(1, "integer"), (3, "record")]
        );
        let original = source.as_any().downcast_ref::<UnionArray>().unwrap();
        let normalized = output.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_buffer_sharing(&original.child(1).to_data(), &normalized.child(1).to_data());
    }

    #[test]
    fn variant_annotation_is_removed_but_metadata_payload_column_is_preserved() {
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b"metadata"[..])]));
        let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b"value"[..])]));
        let variant: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![
                    Arc::new(Field::new("metadata", DataType::Binary, false)),
                    Arc::new(Field::new("value", DataType::Binary, false)),
                ]
                .into(),
                vec![Arc::clone(&metadata), Arc::clone(&value)],
                None,
            )
            .unwrap(),
        );
        let source: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![annotated("variant", variant.data_type().clone(), false)].into(),
                vec![Arc::clone(&variant)],
                None,
            )
            .unwrap(),
        );
        let output = normalize_execution_array(&source).unwrap();
        assert_clean(output.data_type());
        let output = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(Arc::ptr_eq(output.column(0), &variant));
        let variant = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(variant.fields()[0].name(), "metadata");
        assert!(Arc::ptr_eq(variant.column(0), &metadata));
        assert!(Arc::ptr_eq(variant.column(1), &value));
    }

    #[test]
    fn callbacks_precede_new_structure_and_failure_keeps_the_input() {
        let source: ArrayRef = Arc::new(
            ListArray::try_new(
                annotated("element", DataType::Utf8, false),
                OffsetBuffer::new(vec![0i32, 1].into()),
                Arc::new(StringArray::from(vec!["a"])),
                None,
            )
            .unwrap(),
        );
        let mut fields = 0;
        let mut arrays = 0;
        let output =
            normalize_execution_array_observed::<crate::KernelFailure, _>(&source, |step| {
                match step {
                    ArrayNormalizationStep::RebuildField { target, name, .. } => {
                        assert_eq!(target, &DataType::Utf8);
                        assert_eq!(name, "item");
                        fields += 1;
                    }
                    ArrayNormalizationStep::RebuildArray { .. } => arrays += 1,
                    _ => {}
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(fields, 1);
        assert_eq!(arrays, 1);
        assert!(
            normalize_execution_array_observed::<crate::KernelFailure, _>(&source, |step| {
                if matches!(step, ArrayNormalizationStep::RebuildField { .. }) {
                    Err(crate::KernelFailure::ResourceExhausted)
                } else {
                    Ok(())
                }
            })
            .is_err()
        );
        assert_eq!(
            source
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_length(0),
            1
        );
        let mut constructions = 0;
        let retained =
            normalize_execution_array_observed::<crate::KernelFailure, _>(&output, |step| {
                if !matches!(
                    step,
                    ArrayNormalizationStep::InspectType(_)
                        | ArrayNormalizationStep::InspectField { .. }
                        | ArrayNormalizationStep::InspectConstructionWork { .. }
                        | ArrayNormalizationStep::OpaqueOperationFinished
                ) {
                    constructions += 1;
                }
                Ok(())
            })
            .unwrap();
        assert!(Arc::ptr_eq(&output, &retained));
        assert_eq!(constructions, 0);
    }

    #[test]
    fn independently_authored_role_labels_normalize_to_the_same_actual_shape() {
        let array = |role: &str| -> ArrayRef {
            Arc::new(
                LargeListArray::try_new(
                    annotated(role, DataType::Int64, false),
                    OffsetBuffer::new(vec![0i64, 1, 3].into()),
                    Arc::new(Int64Array::from(vec![7, 8, 9])),
                    Some(NullBuffer::from(vec![true, false])),
                )
                .unwrap(),
            )
        };
        let left = array("element");
        let right = array("value");
        assert_ne!(left.data_type(), right.data_type());
        let left_output = normalize_execution_array(&left).unwrap();
        let right_output = normalize_execution_array(&right).unwrap();
        assert_eq!(left_output.data_type(), right_output.data_type());
        assert_clean(left_output.data_type());
        assert_buffer_sharing(&left.to_data(), &left_output.to_data());
        assert_buffer_sharing(&right.to_data(), &right_output.to_data());
        assert!(!Arc::ptr_eq(&left_output, &right_output));
    }
}

#[cfg(test)]
mod typed_source_tests {
    use super::*;
    use crate::{KernelDiagnostic, KernelFailure};
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn excessive_actual_type_depth_retains_its_type_contract_reason() {
        let mut ty = DataType::Int32;
        for _ in 0..=novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
            ty = DataType::List(Arc::new(Field::new("item", ty, true)));
        }
        let field = Arc::new(Field::new("root", ty, true));
        let failure =
            normalize_execution_field_observed::<ArrayProjectionFailure, _>(&field, |_| Ok(()))
                .unwrap_err();
        assert!(matches!(
            failure,
            ArrayProjectionFailure::ValueType(novarocks_type_contract::ValueTypeError::TooDeep)
        ));
        assert_eq!(
            failure.failure_fact(),
            novarocks_type_contract::ValueTypeError::TooDeep.failure_fact()
        );
        assert_eq!(failure.failure_fact().class(), FailureClass::Resource);
    }
    #[test]
    fn all_original_callback_causes_stop_before_construction_without_relabeling() {
        let source = Arc::new(
            Field::new("external-role", DataType::Int32, false).with_metadata(
                std::collections::HashMap::from([("external".into(), "value".into())]),
            ),
        );
        for original in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("original internal")),
            KernelFailure::Operational(KernelDiagnostic::new("original operational")),
            KernelFailure::from(novarocks_type_contract::ValueTypeError::TooManyNodes),
        ] {
            let mut refused = false;
            let result = normalize_execution_field_observed::<KernelFailure, _>(&source, |step| {
                assert!(!refused, "no callback may run after the original refusal");
                if matches!(step, ArrayNormalizationStep::RebuildField { .. }) {
                    refused = true;
                    Err(original.clone())
                } else {
                    Ok(())
                }
            });
            assert!(refused);
            assert_eq!(result.unwrap_err(), original);
            assert_eq!(source.metadata().get("external").unwrap(), "value");
        }
    }
}

#[cfg(test)]
mod projection_path_tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_buffer::OffsetBuffer;
    use std::cell::{Cell, RefCell};

    fn encoded_source(wrappers: usize) -> (ArrayRef, *const u8) {
        let values = Arc::new(Int32Array::from(vec![7]));
        let payload = values.values().inner().as_ptr();
        let mut source: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("external-role", DataType::Int32, false)),
                OffsetBuffer::new(vec![0i32, 1].into()),
                values,
                None,
            )
            .unwrap(),
        );
        for _ in 0..wrappers {
            source = Arc::new(
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), source).unwrap(),
            );
        }
        (source, payload)
    }

    #[test]
    fn projection_paths_beyond_the_removed_wrapper_bound_use_actual_observed_storage() {
        // Logical depth is two; encodings do not introduce another quota.
        // This also crosses the removed 126-entry local projection path cap.
        let wrappers = 128;
        let (source, payload) = encoded_source(wrappers);
        let largest_path = Cell::new(0);
        let actual_path_layouts = RefCell::new(Vec::new());
        let output =
            normalize_execution_array_observed::<ArrayProjectionFailure, _>(&source, |step| {
                if let ArrayNormalizationStep::CreateProjectionPath { steps } = step {
                    largest_path.set(largest_path.get().max(steps));
                    // Use the same actual construction author, not a second estimate
                    // or an observer that silently authorizes the path allocation.
                    observe_array_normalization_construction(
                        step,
                        &mut |layout| {
                            actual_path_layouts.borrow_mut().push(layout);
                            Ok(())
                        },
                        &mut |_| Ok(()),
                        &mut |_| Ok(()),
                    )?;
                } else {
                    observe_array_normalization_construction(
                        step,
                        &mut |_| Ok(()),
                        &mut |_| Ok(()),
                        &mut |_| Ok(()),
                    )?;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(largest_path.get(), wrappers + 1);
        assert!(actual_path_layouts.borrow().iter().any(|layout| {
            *layout == std::alloc::Layout::array::<ArrayPathStep>(wrappers + 1).unwrap()
        }));
        let mut current = &output;
        for _ in 0..wrappers {
            current = current
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap()
                .values();
        }
        let list = current.as_any().downcast_ref::<ListArray>().unwrap();
        let DataType::List(field) = list.data_type() else {
            panic!("List remains the original value representation")
        };
        assert_eq!(field.name(), "item");
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            7
        );
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .inner()
                .as_ptr(),
            payload
        );
    }

    #[test]
    fn path_storage_first_refusal_has_no_tail_and_keeps_the_original_source() {
        let (source, _) = encoded_source(2);
        let original = crate::KernelFailure::Operational(crate::KernelDiagnostic::new(
            "original projection path refusal",
        ));
        let mut refused = false;
        let result =
            normalize_execution_array_observed::<crate::KernelFailure, _>(&source, |step| {
                assert!(
                    !refused,
                    "no work or construction follows the first original refusal"
                );
                if matches!(step, ArrayNormalizationStep::CreateProjectionPath { .. }) {
                    refused = true;
                    Err(original.clone())
                } else {
                    Ok(())
                }
            });
        assert!(refused);
        assert_eq!(result.unwrap_err(), original);
        let outer = source
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let inner = outer
            .values()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let DataType::List(field) = inner.values().data_type() else {
            panic!("original List")
        };
        assert_eq!(field.name(), "external-role");
    }
}
