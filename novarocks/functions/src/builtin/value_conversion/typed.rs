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

use crate::{FunctionBindingError, FunctionValueType};
use arrow_schema::DataType;
use novarocks_type_contract::{ValueKind, ValueTypeRef};

fn child_layout(layout: &DataType, index: usize) -> Option<&DataType> {
    match layout {
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            (index == 0).then_some(item.data_type())
        }
        DataType::Struct(fields) => fields.get(index).map(|field| field.data_type()),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) => fields.get(index).map(|field| field.data_type()),
            _ => None,
        },
        _ => None,
    }
}

fn has_nominal(value: &ValueTypeRef) -> bool {
    matches!(value.kind(), ValueKind::Nominal(_) | ValueKind::Temporal(_))
        || (0..value.child_count()).any(|i| value.child_type_at(i).is_some_and(has_nominal))
}

pub(super) fn domains_preserved(a: &ValueTypeRef, b: &ValueTypeRef) -> bool {
    if a.temporal().is_some() || b.temporal().is_some() {
        return a.temporal() == b.temporal();
    }
    if !has_nominal(a) && !has_nominal(b) {
        return true;
    }
    if a.domain() != b.domain() || a.child_count() != b.child_count() {
        return false;
    }
    (0..a.child_count()).all(|i| match (a.child_type_at(i), b.child_type_at(i)) {
        (Some(a), Some(b)) => domains_preserved(a, b),
        _ => false,
    })
}

#[derive(Clone, Copy)]
pub(super) enum SameShapeConversion {
    JsonText,
    OpaqueBinary,
}
impl SameShapeConversion {
    fn domain_pair(
        self,
        source: novarocks_type_contract::ValueLogicalType,
        target: novarocks_type_contract::ValueLogicalType,
    ) -> Option<bool> {
        use novarocks_type_contract::ValueLogicalType as D;
        match self {
            Self::JsonText => super::json_domain_pair(source, target),
            Self::OpaqueBinary if source == target => Some(false),
            Self::OpaqueBinary
                if matches!(
                    source,
                    D::Hll | D::Bitmap | D::Object | D::Percentile | D::Variant
                ) && target == D::Physical =>
            {
                Some(true)
            }
            _ => None,
        }
    }
    fn changed_kind(self) -> ValueKind {
        match self {
            Self::JsonText => ValueKind::String,
            Self::OpaqueBinary => ValueKind::Binary,
        }
    }
}

/// The same borrowed matcher serves pure selection and covered validation.
/// No metadata map, field clone, dictionary identity or authority is consulted.
pub(super) fn same_shape_pair(
    source: &FunctionValueType,
    target: &FunctionValueType,
    mode: SameShapeConversion,
    observe: &mut impl FnMut() -> Result<(), FunctionBindingError>,
) -> Result<Option<bool>, FunctionBindingError> {
    same_shape_node(
        source.value_type(),
        source.data_type(),
        target.value_type(),
        target.data_type(),
        true,
        mode,
        observe,
    )
}

pub(super) fn same_shape_intermediate_with_construction<E>(
    source: &FunctionValueType,
    target: &FunctionValueType,
    mode: SameShapeConversion,
    creation: &mut super::super::scalar_construction::ScalarConstruction<'_, E>,
) -> Result<FunctionValueType, super::super::scalar_construction::OwnerFailure<E>> {
    use novarocks_type_contract::ValueField;
    let no_match = || FunctionBindingError::NoMatchingOverload;
    let Some(changed) = mode.domain_pair(source.logical_type(), target.logical_type()) else {
        return Err(no_match().into());
    };
    let storage = creation.storage();
    let kind = if changed {
        if matches!(mode, SameShapeConversion::OpaqueBinary)
            && !matches!(target.value_type().kind(), ValueKind::Binary)
        {
            return Err(no_match().into());
        }
        mode.changed_kind()
    } else {
        match (source.value_type().kind(), target.value_type().kind()) {
            (ValueKind::List(_), ValueKind::List(_)) => ValueKind::List(
                same_shape_intermediate_with_construction(
                    &creation.child(source, 0)?.ok_or_else(no_match)?,
                    &creation.child(target, 0)?.ok_or_else(no_match)?,
                    mode,
                    creation,
                )?
                .value_type()
                .clone(),
            ),
            (
                ValueKind::FixedSizeList { length, .. },
                ValueKind::FixedSizeList { .. } | ValueKind::List(_),
            ) => ValueKind::FixedSizeList {
                item: same_shape_intermediate_with_construction(
                    &creation.child(source, 0)?.ok_or_else(no_match)?,
                    &creation.child(target, 0)?.ok_or_else(no_match)?,
                    mode,
                    creation,
                )?
                .value_type()
                .clone(),
                length: *length,
            },
            (ValueKind::Struct(a), ValueKind::Struct(b)) if a.len() == b.len() => {
                let by_name = a
                    .iter()
                    .all(|field| b.iter().any(|other| field.name() == other.name()));
                creation.collected_box::<ValueField>(a.len())?;
                let fields = a
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        let right = if by_name {
                            b.iter()
                                .position(|other| other.name() == field.name())
                                .ok_or_else(no_match)?
                        } else {
                            index
                        };
                        let item = same_shape_intermediate_with_construction(
                            &creation.child(source, index)?.ok_or_else(no_match)?,
                            &creation.child(target, right)?.ok_or_else(no_match)?,
                            mode,
                            creation,
                        )?;
                        Ok(ValueField::from_borrowed_name_with_construction(
                            field.name(),
                            item.value_type().clone(),
                            storage.clone(),
                            creation.observer(),
                        )?)
                    })
                    .collect::<Result<Vec<_>, super::super::scalar_construction::OwnerFailure<E>>>(
                    )?;
                ValueKind::Struct(fields.into())
            }
            (ValueKind::Map { .. }, ValueKind::Map { .. }) => ValueKind::Map {
                key: same_shape_intermediate_with_construction(
                    &creation.child(source, 0)?.ok_or_else(no_match)?,
                    &creation.child(target, 0)?.ok_or_else(no_match)?,
                    mode,
                    creation,
                )?
                .value_type()
                .clone(),
                value: same_shape_intermediate_with_construction(
                    &creation.child(source, 1)?.ok_or_else(no_match)?,
                    &creation.child(target, 1)?.ok_or_else(no_match)?,
                    mode,
                    creation,
                )?
                .value_type()
                .clone(),
            },
            _ if domains_preserved(source.value_type(), target.value_type()) => {
                return Ok(source.clone());
            }
            _ => return Err(no_match().into()),
        }
    };
    let value = ValueTypeRef::try_new_with_construction(
        kind,
        source.nullable(),
        storage,
        creation.observer(),
    )?;
    Ok(FunctionValueType::try_from_value(
        value,
        source.typed_binding().layout().clone(),
    )?)
}

fn same_shape_node(
    a: &ValueTypeRef,
    al: &DataType,
    b: &ValueTypeRef,
    bl: &DataType,
    root: bool,
    mode: SameShapeConversion,
    observe: &mut impl FnMut() -> Result<(), FunctionBindingError>,
) -> Result<Option<bool>, FunctionBindingError> {
    observe()?;
    if !root && a.nullable() != b.nullable() {
        return Ok(None);
    }
    let domain = |value: &ValueTypeRef| match value.kind() {
        ValueKind::Nominal(domain) => *domain,
        _ => novarocks_type_contract::ValueLogicalType::Physical,
    };
    let Some(mut changed) = mode.domain_pair(domain(a), domain(b)) else {
        return Ok(None);
    };
    match (a.kind(), b.kind(), al, bl) {
        (ValueKind::List(_), ValueKind::List(_), DataType::List(_), DataType::List(_))
        | (
            ValueKind::List(_),
            ValueKind::List(_),
            DataType::LargeList(_),
            DataType::LargeList(_),
        ) => {}
        (
            ValueKind::FixedSizeList { length: a, .. },
            ValueKind::FixedSizeList { length: b, .. },
            DataType::FixedSizeList(_, al),
            DataType::FixedSizeList(_, bl),
        ) if a == b && al == bl => {}
        (ValueKind::Struct(a), ValueKind::Struct(b), DataType::Struct(_), DataType::Struct(_))
            if a.len() == b.len() =>
        {
            for (a, b) in a.iter().zip(b) {
                observe()?;
                if a.name().len() != b.name().len() {
                    return Ok(None);
                }
                for (a, b) in a.name().bytes().zip(b.name().bytes()) {
                    observe()?;
                    if a != b {
                        return Ok(None);
                    }
                }
            }
        }
        (
            ValueKind::Map { .. },
            ValueKind::Map { .. },
            DataType::Map(_, a),
            DataType::Map(_, b),
        ) if a == b => {}
        _ => {
            let same =
                novarocks_type_contract::arrow_data_types_exact_observed(al, bl, &mut *observe)?;
            return Ok(same.then_some(changed));
        }
    }
    if a.child_count() != b.child_count() {
        return Ok(None);
    }
    for index in 0..a.child_count() {
        observe()?;
        let (Some(a), Some(al), Some(b), Some(bl)) = (
            a.child_type_at(index),
            child_layout(al, index),
            b.child_type_at(index),
            child_layout(bl, index),
        ) else {
            return Ok(None);
        };
        let Some(child) = same_shape_node(a, al, b, bl, false, mode, observe)? else {
            return Ok(None);
        };
        changed |= child;
    }
    Ok(Some(changed))
}
