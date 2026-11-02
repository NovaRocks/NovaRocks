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

use super::{
    HydrateEvent, HydrateObserver, HydrateOptions, HydrateProjection, child, invalid,
    tree::child_rows,
};
use crate::{
    ByteLayout, ValueError, ValueFailure, ValueViolation,
    access::ByteSource,
    arrow_raw::{self, StructuralRef},
    control::Work,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use novarocks_type_contract::{BorrowedTypedValueBinding, TypedValueBinding};

/// Validate original encoded keys independently of materialization, then enforce
/// required values only under active ancestors. No metadata authors semantics,
/// no decoded wrapper is needed, and inactive NULL keys are never dereferenced.
pub fn validate_required_observed<E>(
    source: &ArrayRef,
    binding: &TypedValueBinding,
    work: &mut Work<'_, E>,
    observe: &mut HydrateObserver<'_, E>,
) -> Result<(), ValueFailure<E>> {
    validate_with_options(source, binding, HydrateOptions::default(), work, observe)
}
/// Check the original value promise using the caller's exact frozen projection.
pub fn validate_required_with_options_observed<E>(
    source: &ArrayRef,
    binding: &TypedValueBinding,
    options: HydrateOptions,
    work: &mut Work<'_, E>,
    observe: &mut HydrateObserver<'_, E>,
) -> Result<(), ValueFailure<E>> {
    validate_with_options(source, binding, options, work, observe)
}

/// Observe a target requirement without treating it as an input value promise.
/// The same logical-NULL traversal keeps dictionary and active-parent rules.
/// Invalid structure/keys and caller failures remain their original errors.
pub fn required_values_satisfied_observed<E>(
    source: &ArrayRef,
    binding: &TypedValueBinding,
    options: HydrateOptions,
    work: &mut Work<'_, E>,
    observe: &mut HydrateObserver<'_, E>,
) -> Result<bool, ValueFailure<E>> {
    validate_structure(source, binding, options, work, observe)?;
    let mut satisfied = true;
    let mut missing = || {
        satisfied = false;
        Ok(())
    };
    for row in 0..source.len() {
        validate_row(source, binding.borrowed(), options, row, work, &mut missing)?;
    }
    Ok(satisfied)
}

pub(super) fn validate_with_options<E>(
    source: &ArrayRef,
    binding: &TypedValueBinding,
    options: HydrateOptions,
    work: &mut Work<'_, E>,
    observe: &mut HydrateObserver<'_, E>,
) -> Result<(), ValueFailure<E>> {
    validate_structure(source, binding, options, work, observe)?;
    let mut missing = || Err(ValueError::Invariant(ValueViolation::RequiredNull).into());
    for row in 0..source.len() {
        validate_row(source, binding.borrowed(), options, row, work, &mut missing)?;
    }
    Ok(())
}

fn validate_structure<E>(
    source: &ArrayRef,
    binding: &TypedValueBinding,
    options: HydrateOptions,
    work: &mut Work<'_, E>,
    observe: &mut HydrateObserver<'_, E>,
) -> Result<(), ValueFailure<E>> {
    enum MatchError<E> {
        Caller(E),
        Contract,
    }
    impl<E> From<novarocks_type_contract::ValueTypeError> for MatchError<E> {
        fn from(_: novarocks_type_contract::ValueTypeError) -> Self {
            Self::Contract
        }
    }
    let matches = if options.dictionary_value_layout.is_some()
        || options.projection != HydrateProjection::Exact
    {
        parameterized_shape(source, binding.borrowed(), options, work)?
    } else {
        novarocks_type_contract::value_layout_matches_carrier_observed(
            binding.layout(),
            source.data_type(),
            |units| work(units).map_err(MatchError::Caller),
        )
        .map_err(|error| match error {
            MatchError::Caller(error) => ValueFailure::Caller(error),
            MatchError::Contract => ValueError::Invariant(ValueViolation::Layout).into(),
        })?
    };
    if !matches {
        return Err(ValueError::Invariant(ValueViolation::Layout).into());
    }
    inspect(source, work, observe, 0)?;
    Ok(())
}

fn inspect<E>(
    source: &ArrayRef,
    work: &mut Work<'_, E>,
    observe: &mut HydrateObserver<'_, E>,
    depth: usize,
) -> Result<(), ValueFailure<E>> {
    if depth >= novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(ValueError::Invariant(ValueViolation::Depth).into());
    }
    work(1).map_err(ValueFailure::Caller)?;
    observe(HydrateEvent::InspectType(source.data_type())).map_err(ValueFailure::Caller)?;
    if matches!(source.data_type(), DataType::Dictionary(..)) {
        let (keys, values) = arrow_raw::source_dictionary(source)?;
        for row_start in (0..keys.len()).step_by(1024) {
            let row_count = (keys.len() - row_start).min(1024);
            observe(HydrateEvent::InspectDictionaryRows {
                source,
                row_start,
                row_count,
            })
            .map_err(ValueFailure::Caller)?;
            for row in row_start..row_start + row_count {
                work(1).map_err(ValueFailure::Caller)?;
                if keys.is_null(row) {
                    continue;
                }
                let index = usize::try_from(keys.value(row))
                    .map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?;
                if index >= values.len() {
                    return Err(ValueError::Invariant(ValueViolation::Bounds).into());
                }
            }
        }
    }
    match arrow_raw::structural_view(source)? {
        StructuralRef::Leaf => {}
        StructuralRef::Struct { fields, children } => {
            for (field, input) in fields.iter().zip(children) {
                observe(HydrateEvent::InspectField(field)).map_err(ValueFailure::Caller)?;
                inspect(input, work, observe, depth + 1)?;
            }
        }
        StructuralRef::Union { fields, array } => {
            for (id, field) in fields.iter() {
                observe(HydrateEvent::InspectField(field)).map_err(ValueFailure::Caller)?;
                inspect(array.child(id), work, observe, depth + 1)?;
            }
        }
        StructuralRef::List32 { field, values, .. }
        | StructuralRef::List64 { field, values, .. }
        | StructuralRef::FixedSizeList { field, values, .. } => {
            observe(HydrateEvent::InspectField(field)).map_err(ValueFailure::Caller)?;
            inspect(values, work, observe, depth + 1)?;
        }
        StructuralRef::Map {
            entries_field,
            keys,
            values,
            ..
        } => {
            observe(HydrateEvent::InspectField(entries_field)).map_err(ValueFailure::Caller)?;
            if let DataType::Struct(fields) = entries_field.data_type() {
                for field in fields {
                    observe(HydrateEvent::InspectField(field)).map_err(ValueFailure::Caller)?;
                }
            }
            inspect(keys, work, observe, depth + 1)?;
            inspect(values, work, observe, depth + 1)?;
        }
    }
    Ok(())
}
fn validate_row<E>(
    source: &ArrayRef,
    binding: BorrowedTypedValueBinding<'_>,
    options: HydrateOptions,
    row: usize,
    work: &mut Work<'_, E>,
    missing: &mut dyn FnMut() -> Result<(), ValueFailure<E>>,
) -> Result<(), ValueFailure<E>> {
    work(1).map_err(ValueFailure::Caller)?;
    let is_null = if matches!(source.data_type(), DataType::Dictionary(..)) {
        let layout = ByteLayout::from_data_type(binding.data_type()).ok_or_else(invalid)?;
        let bytes = ByteSource::parse(source, layout, options.dictionary_value_layout)?;
        match bytes.mapped_row(row)? {
            None => true,
            Some(index) => bytes.values().is_null(index),
        }
    } else {
        source.is_null(row)
    };
    if is_null {
        if !binding.value_type().nullable() {
            return missing();
        }
        return Ok(());
    }
    child_rows(source, row, &mut |index, start, end| {
        let input = match arrow_raw::structural_view(source)? {
            StructuralRef::Struct { children, .. } => children.get(index).ok_or_else(invalid)?,
            StructuralRef::Union { fields, array } => {
                array.child(fields.iter().nth(index).ok_or_else(invalid)?.0)
            }
            StructuralRef::List32 { values, .. }
            | StructuralRef::List64 { values, .. }
            | StructuralRef::FixedSizeList { values, .. } => values,
            StructuralRef::Map { keys, values, .. } => {
                if index == 0 {
                    keys
                } else {
                    values
                }
            }
            StructuralRef::Leaf => return Err(invalid().into()),
        };
        let binding = child(binding, index)?;
        for row in start..end {
            validate_row(input, binding, options, row, work, missing)?;
        }
        Ok(())
    })
}

// Primitive override changes only dictionary-value storage. Byte projection
// is an explicit plain-leaf operation; only the signed names/NULL mode relaxes
// those declarations. All other scalar and container parameters stay exact.
fn parameterized_shape<E>(
    source: &ArrayRef,
    binding: BorrowedTypedValueBinding<'_>,
    options: HydrateOptions,
    work: &mut Work<'_, E>,
) -> Result<bool, ValueFailure<E>> {
    fn field<E>(
        expected: &arrow_schema::Field,
        actual: &arrow_schema::Field,
        named: bool,
        options: HydrateOptions,
        depth: usize,
        work: &mut Work<'_, E>,
    ) -> Result<bool, ValueFailure<E>> {
        work(1).map_err(ValueFailure::Caller)?;
        if options.projection != HydrateProjection::FrozenNamesAndNullability
            && expected.is_nullable() != actual.is_nullable()
        {
            return Ok(false);
        }
        if named && options.projection != HydrateProjection::FrozenNamesAndNullability {
            if expected.name().len() != actual.name().len() {
                return Ok(false);
            }
            for (a, b) in expected
                .name()
                .as_bytes()
                .chunks(1024)
                .zip(actual.name().as_bytes().chunks(1024))
            {
                work(a.len()).map_err(ValueFailure::Caller)?;
                if a != b {
                    return Ok(false);
                }
            }
        }
        ty(
            expected.data_type(),
            actual.data_type(),
            options,
            depth,
            work,
        )
    }
    fn ty<E>(
        expected: &DataType,
        actual: &DataType,
        options: HydrateOptions,
        depth: usize,
        work: &mut Work<'_, E>,
    ) -> Result<bool, ValueFailure<E>> {
        work(1).map_err(ValueFailure::Caller)?;
        if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
            return Err(ValueError::Invariant(ValueViolation::Depth).into());
        }
        if let DataType::Dictionary(keys, values) = actual {
            let dictionary_layout = options.dictionary_value_layout.or_else(|| {
                novarocks_type_contract::dictionary_value_layout(expected)
                    .as_ref()
                    .and_then(ByteLayout::from_data_type)
            });
            return Ok(keys.as_ref() == &DataType::Int32
                && ByteLayout::from_data_type(expected).is_some_and(ByteLayout::is_utf8)
                && dictionary_layout.is_some_and(|layout| {
                    layout.is_utf8() && &layout.data_type() == values.as_ref()
                }));
        }
        if options.projection == HydrateProjection::ExactByteLayouts
            && let (Some(target), Some(source)) = (
                ByteLayout::from_data_type(expected),
                ByteLayout::from_data_type(actual),
            )
        {
            // This is representation projection, never a text/binary cast.
            return Ok(target.is_utf8() == source.is_utf8());
        }
        match (expected, actual) {
            (DataType::Struct(a), DataType::Struct(b)) if a.len() == b.len() => {
                for (a, b) in a.iter().zip(b) {
                    if !field(a, b, true, options, depth + 1, work)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (DataType::Union(a, am), DataType::Union(b, bm)) if am == bm && a.len() == b.len() => {
                for ((ai, a), (bi, b)) in a.iter().zip(b.iter()) {
                    if ai != bi || !field(a, b, true, options, depth + 1, work)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (DataType::List(a), DataType::List(b))
            | (DataType::LargeList(a), DataType::LargeList(b)) => {
                field(a, b, false, options, depth + 1, work)
            }
            (DataType::FixedSizeList(a, aw), DataType::FixedSizeList(b, bw)) if aw == bw => {
                field(a, b, false, options, depth + 1, work)
            }
            (DataType::Map(a, asort), DataType::Map(b, bsort))
                if asort == bsort
                    && (options.projection == HydrateProjection::FrozenNamesAndNullability
                        || a.is_nullable() == b.is_nullable()) =>
            {
                let (DataType::Struct(af), DataType::Struct(bf)) = (a.data_type(), b.data_type())
                else {
                    return Ok(false);
                };
                if af.len() != 2 || bf.len() != 2 {
                    return Ok(false);
                }
                Ok(field(&af[0], &bf[0], false, options, depth + 2, work)?
                    && field(&af[1], &bf[1], false, options, depth + 2, work)?)
            }
            _ => {
                enum ShapeError<E> {
                    Caller(E),
                    Contract,
                }
                impl<E> From<novarocks_type_contract::ValueTypeError> for ShapeError<E> {
                    fn from(_: novarocks_type_contract::ValueTypeError) -> Self {
                        Self::Contract
                    }
                }
                novarocks_type_contract::data_type_layout_matches_carrier_observed(
                    expected,
                    actual,
                    |n| work(n).map_err(ShapeError::Caller),
                )
                .map_err(|error| match error {
                    ShapeError::Caller(error) => ValueFailure::Caller(error),
                    ShapeError::Contract => ValueError::Invariant(ValueViolation::Layout).into(),
                })
            }
        }
    }
    ty(binding.data_type(), source.data_type(), options, 1, work)
}
