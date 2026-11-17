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

//! ARRAY-constructor-only common declarations from original complete FVTs.
//! This declares a result; it grants no conversion or installed kernel capability.
//! No outer target, AST, ConstantValue, SQL mode or runtime value enters this API.
use crate::value_construction::ValueConstruction;
use crate::{
    FunctionValueType, ValueConstructionFailure, ValueConstructionRequest, ValueKind, ValueLayout,
    ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};
use arrow_schema::{DataType, Field};
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrayCommonRuleFailure {
    IncompatibleDomains,
    IncompatibleScalarTypes,
    UnreviewedHeterogeneousFamily,
    NonnullableUnknown,
    MissingListChild,
    UnreviewedLayoutReconciliation,
}
#[derive(Debug)]
pub enum ArrayCommonTypeFailure<E> {
    Construction(ValueConstructionFailure<E>),
    Rule(ArrayCommonRuleFailure),
}
impl<E> From<ValueConstructionFailure<E>> for ArrayCommonTypeFailure<E> {
    fn from(cause: ValueConstructionFailure<E>) -> Self {
        Self::Construction(cause)
    }
}
fn step<E>(creation: &mut ValueConstruction<'_, E>) -> Result<(), ArrayCommonTypeFailure<E>> {
    if let Some(observer) = creation.observer() {
        observer(ValueConstructionRequest::Work(1)).map_err(|cause| {
            ArrayCommonTypeFailure::Construction(ValueConstructionFailure::Observer(cause))
        })?;
    }
    Ok(())
}
fn rule<E>(reason: ArrayCommonRuleFailure) -> ArrayCommonTypeFailure<E> {
    ArrayCommonTypeFailure::Rule(reason)
}
fn unknown(value: &FunctionValueType) -> bool {
    value.logical_type() == ValueLogicalType::Physical
        && matches!(value.value_type().kind(), ValueKind::Scalar(DataType::Null))
}
fn check_unknown<E>(value: &FunctionValueType) -> Result<(), ArrayCommonTypeFailure<E>> {
    if unknown(value) && !value.nullable() {
        return Err(rule(ArrayCommonRuleFailure::NonnullableUnknown));
    }
    Ok(())
}

/// The constructor owns a non-NULL List result with a nullable item. Original
/// source handles remain unchanged and available to their conversion author.
pub fn array_constructor_result_with_construction<E>(
    original_elements: &[&FunctionValueType],
    storage: ValueTypeStorageLifetime,
    observer: Option<&mut dyn FnMut(ValueConstructionRequest) -> Result<(), E>>,
) -> Result<FunctionValueType, ArrayCommonTypeFailure<E>> {
    let mut creation = ValueConstruction::new(observer);
    step(&mut creation)?;
    let mut item = if let Some((first, remaining)) = original_elements.split_first() {
        // Visit the first source's actual children too; a one-argument call
        // must not bypass semantic checks that a duplicate occurrence sees.
        let mut common = pair(first, first, storage.clone(), &mut creation)?;
        for original in remaining {
            step(&mut creation)?;
            common = pair(&common, original, storage.clone(), &mut creation)?;
        }
        common
    } else {
        FunctionValueType::try_with_logical_type_with_construction(
            DataType::Null,
            true,
            ValueLogicalType::Physical,
            storage.clone(),
            creation.observer(),
        )?
    };
    item = item.with_nullable_with_construction(true, storage.clone(), creation.observer())?;
    list_result(&item, false, storage, &mut creation)
}

/// Declare one List from an explicit complete child type. This shares the
/// constructor's original Field/layout creators without implying an ARRAY
/// expression, a common-type rule, or any conversion/runtime capability.
pub fn list_value_type_with_construction<E>(
    child: &FunctionValueType,
    nullable: bool,
    storage: ValueTypeStorageLifetime,
    observer: Option<&mut dyn FnMut(ValueConstructionRequest) -> Result<(), E>>,
) -> Result<FunctionValueType, ArrayCommonTypeFailure<E>> {
    let mut creation = ValueConstruction::new(observer);
    list_result(child, nullable, storage, &mut creation)
}

fn pair<E>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    storage: ValueTypeStorageLifetime,
    creation: &mut ValueConstruction<'_, E>,
) -> Result<FunctionValueType, ArrayCommonTypeFailure<E>> {
    step(creation)?;
    check_unknown(left)?;
    check_unknown(right)?;
    let nullable = left.nullable() || right.nullable();
    if unknown(left) || unknown(right) {
        let source = if unknown(left) { right } else { left };
        return source
            .with_nullable_with_construction(nullable, storage, creation.observer())
            .map_err(Into::into);
    }
    // Lists must visit their actual semantic children even when Arrow types
    // compare equal. An explicit JSON/String CAST exception is not a common rule.
    if matches!(
        (left.value_type().kind(), right.value_type().kind()),
        (ValueKind::List(_), ValueKind::List(_))
    ) {
        let a = left
            .child_at_with_construction(0, storage.clone(), creation.observer())?
            .ok_or_else(|| rule(ArrayCommonRuleFailure::MissingListChild))?;
        let b = right
            .child_at_with_construction(0, storage.clone(), creation.observer())?
            .ok_or_else(|| rule(ArrayCommonRuleFailure::MissingListChild))?;
        let child = pair(&a, &b, storage.clone(), creation)?;
        if left
            .same_physical_binding_ignoring_root_nullability_observed(right, || step(creation))?
        {
            return left
                .with_nullable_with_construction(nullable, storage, creation.observer())
                .map_err(Into::into);
        }
        let (DataType::List(a_field), DataType::List(b_field)) =
            (left.data_type(), right.data_type())
        else {
            return Err(rule(ArrayCommonRuleFailure::UnreviewedLayoutReconciliation));
        };
        // Source fields remain in their original source declarations. A new
        // common layout has no authority to discard/reinterpret format facts.
        if !a_field.metadata().is_empty()
            || !b_field.metadata().is_empty()
            || a_field.name() != b_field.name()
        {
            return Err(rule(ArrayCommonRuleFailure::UnreviewedLayoutReconciliation));
        }
        return list_result(&child, nullable, storage, creation);
    }
    if left.same_physical_binding_ignoring_root_nullability_observed(right, || step(creation))? {
        // Exact existing identity also preserves homogeneous row/map/temporal
        // declarations. This is not an unreviewed heterogeneous promotion.
        return left
            .with_nullable_with_construction(nullable, storage, creation.observer())
            .map_err(Into::into);
    }
    if left.logical_type() != right.logical_type() {
        return Err(rule(ArrayCommonRuleFailure::IncompatibleDomains));
    }
    if left.logical_type() != ValueLogicalType::Physical {
        return Err(rule(ArrayCommonRuleFailure::IncompatibleDomains));
    }
    let carrier = numeric_common(left.data_type(), right.data_type()).map_err(rule::<E>)?;
    FunctionValueType::try_with_logical_type_with_construction(
        carrier,
        nullable,
        ValueLogicalType::Physical,
        storage,
        creation.observer(),
    )
    .map_err(Into::into)
}

fn list_result<E>(
    child: &FunctionValueType,
    nullable: bool,
    storage: ValueTypeStorageLifetime,
    creation: &mut ValueConstruction<'_, E>,
) -> Result<FunctionValueType, ArrayCommonTypeFailure<E>> {
    step(creation)?;
    let child_carrier = creation.cloned_data_type(child.data_type())?;
    // These are the actual original Field/name creators, using the same
    // construction events as the neutral coercion/layout authors.
    creation.name("item".len())?;
    let field = Field::new("item", child_carrier, child.nullable());
    creation.exact(|| crate::owned_resources::layout::arc_layout(Layout::new::<Field>()))?;
    let carrier = DataType::List(Arc::new(field));
    let layout =
        ValueLayout::try_new_with_construction(&carrier, storage.clone(), creation.observer())?;
    let value = ValueTypeRef::try_new_with_construction(
        ValueKind::List(child.value_type().clone()),
        nullable,
        storage,
        creation.observer(),
    )?;
    FunctionValueType::try_from_value(value, layout)
        .map_err(|cause| ArrayCommonTypeFailure::Construction(cause.into()))
}
fn signed(t: &DataType) -> Option<(u8, u8)> {
    match t {
        DataType::Int8 => Some((8, 3)),
        DataType::Int16 => Some((16, 5)),
        DataType::Int32 => Some((32, 10)),
        DataType::Int64 => Some((64, 19)),
        _ => None,
    }
}
fn decimal(t: &DataType) -> Option<(u8, i8)> {
    if let Some((_, digits)) = signed(t) {
        return Some((digits, 0));
    }
    match t {
        DataType::Decimal128(p, s) if (1..=38).contains(p) && *s >= 0 && *s <= *p as i8 => {
            Some((*p, *s))
        }
        _ => None,
    }
}
fn numeric(t: &DataType) -> bool {
    signed(t).is_some()
        || matches!(t, DataType::Float32 | DataType::Float64)
        || decimal(t).is_some()
}
fn numeric_common(a: &DataType, b: &DataType) -> Result<DataType, ArrayCommonRuleFailure> {
    if !numeric(a) || !numeric(b) {
        return Err(
            if matches!(a, DataType::Utf8 | DataType::LargeUtf8)
                || matches!(b, DataType::Utf8 | DataType::LargeUtf8)
            {
                ArrayCommonRuleFailure::IncompatibleScalarTypes
            } else {
                ArrayCommonRuleFailure::UnreviewedHeterogeneousFamily
            },
        );
    }
    if matches!(a, DataType::Float64) || matches!(b, DataType::Float64) {
        return Ok(DataType::Float64);
    }
    if matches!(a, DataType::Float32) || matches!(b, DataType::Float32) {
        return Ok(DataType::Float32);
    }
    if let (Some((a, _)), Some((b, _))) = (signed(a), signed(b)) {
        return Ok(match a.max(b) {
            8 => DataType::Int8,
            16 => DataType::Int16,
            32 => DataType::Int32,
            64 => DataType::Int64,
            _ => unreachable!("reviewed signed width"),
        });
    }
    let (ap, as_) = decimal(a).expect("reviewed non-float numeric");
    let (bp, bs) = decimal(b).expect("reviewed non-float numeric");
    let scale = as_.max(bs);
    let precision = ((i16::from(ap) - i16::from(as_)).max(i16::from(bp) - i16::from(bs))
        + i16::from(scale))
    .min(38) as u8;
    Ok(DataType::Decimal128(precision, scale))
}

/// Whether a reviewed numeric source can be implicitly coerced to this ARRAY
/// common target. This is a type fact, not a conversion capability.
pub fn array_common_numeric_coercion(source: &DataType, target: &DataType) -> bool {
    numeric_common(source, target).is_ok_and(|common| &common == target)
}

/// Check one frozen ARRAY common coercion without constructing or retyping a
/// source. Explicit JSON/text CAST exceptions never enter this relation.
pub fn array_common_coercion_types_valid_observed<E>(
    source: &FunctionValueType,
    target: &FunctionValueType,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    fn visit<E>(
        source: &ValueTypeRef,
        source_layout: &DataType,
        target: &ValueTypeRef,
        target_layout: &DataType,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        if source.nullable() && !target.nullable() {
            return Ok(false);
        }
        if matches!(source.kind(), ValueKind::Scalar(DataType::Null)) {
            return Ok(source.nullable() && target.nullable());
        }
        if let (
            ValueKind::List(source_item),
            DataType::List(source_field),
            ValueKind::List(target_item),
            DataType::List(target_field),
        ) = (source.kind(), source_layout, target.kind(), target_layout)
        {
            return visit(
                source_item,
                source_field.data_type(),
                target_item,
                target_field.data_type(),
                observe,
            );
        }
        if crate::function::typed_value::compatible(
            source,
            target,
            crate::function::typed_value::Nullability::Fits,
            observe,
        )? && crate::schema::arrow_validated_data_types_exact_observed(
            source_layout,
            target_layout,
            &mut *observe,
        )? {
            return Ok(true);
        }
        Ok(matches!(
            (source.kind(), target.kind()),
            (ValueKind::Scalar(_), ValueKind::Scalar(_))
        ) && array_common_numeric_coercion(source_layout, target_layout))
    }
    visit(
        source.value_type(),
        source.data_type(),
        target.value_type(),
        target.data_type(),
        &mut observe,
    )
}

#[cfg(test)]
#[path = "array_common_tests.rs"]
mod tests;
