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

use std::sync::Arc;

use crate::{ValueKind, ValueTypeError, ValueTypeRef};
use arrow_schema::{DataType, Field, UnionFields};

#[derive(Clone, Copy)]
pub(crate) enum Nullability {
    Fits,
    IgnoreRoot,
    Exact,
    IgnoreAll,
}

pub(crate) fn compatible<E>(
    left: &ValueTypeRef,
    right: &ValueTypeRef,
    nulls: Nullability,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    observe()?;
    match nulls {
        Nullability::Fits if left.nullable() && !right.nullable() => return Ok(false),
        Nullability::Exact if left.nullable() != right.nullable() => return Ok(false),
        _ => {}
    }
    let children = match nulls {
        Nullability::IgnoreRoot => Nullability::Exact,
        other => other,
    };
    let names_equal =
        |a: &str, b: &str, observe: &mut dyn FnMut() -> Result<(), E>| -> Result<bool, E> {
            observe()?;
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.bytes().zip(b.bytes()) {
                observe()?;
                if a != b {
                    return Ok(false);
                }
            }
            Ok(true)
        };
    Ok(match (left.kind(), right.kind()) {
        (ValueKind::Scalar(a), ValueKind::Scalar(b)) => {
            crate::schema::arrow_validated_data_types_exact_observed(a, b, observe)?
        }
        (ValueKind::String, ValueKind::String) | (ValueKind::Binary, ValueKind::Binary) => true,
        (ValueKind::Nominal(a), ValueKind::Nominal(b)) => {
            observe()?;
            a == b
        }
        (ValueKind::List(a), ValueKind::List(b)) => compatible(a, b, children, observe)?,
        (
            ValueKind::FixedSizeList {
                item: a,
                length: al,
            },
            ValueKind::FixedSizeList {
                item: b,
                length: bl,
            },
        ) => {
            observe()?;
            al == bl && compatible(a, b, children, observe)?
        }
        (ValueKind::Struct(a), ValueKind::Struct(b)) => {
            observe()?;
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                if !names_equal(a.name(), b.name(), observe)?
                    || !compatible(a.value_type(), b.value_type(), children, observe)?
                {
                    return Ok(false);
                }
            }
            true
        }
        (ValueKind::Map { key: ak, value: av }, ValueKind::Map { key: bk, value: bv }) => {
            compatible(ak, bk, children, observe)? && compatible(av, bv, children, observe)?
        }
        (ValueKind::Union(a), ValueKind::Union(b)) => {
            observe()?;
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                observe()?;
                if a.type_id != b.type_id
                    || !names_equal(a.field.name(), b.field.name(), observe)?
                    || !compatible(
                        a.field.value_type(),
                        b.field.value_type(),
                        children,
                        observe,
                    )?
                {
                    return Ok(false);
                }
            }
            true
        }
        _ => false,
    })
}

/// Temporary legacy production projection. The layout supplies physical
/// structure, the complete semantic tree supplies every nominal annotation.
pub(super) fn legacy_field(
    value: &ValueTypeRef,
    layout: &DataType,
    name: String,
) -> Result<Field, ValueTypeError> {
    let data_type = match (value.kind(), layout) {
        (ValueKind::List(child), DataType::List(field)) => DataType::List(Arc::new(legacy_field(
            child,
            field.data_type(),
            field.name().clone(),
        )?)),
        (ValueKind::List(child), DataType::LargeList(field)) => DataType::LargeList(Arc::new(
            legacy_field(child, field.data_type(), field.name().clone())?,
        )),
        (ValueKind::List(child), DataType::ListView(field)) => DataType::ListView(Arc::new(
            legacy_field(child, field.data_type(), field.name().clone())?,
        )),
        (ValueKind::List(child), DataType::LargeListView(field)) => {
            DataType::LargeListView(Arc::new(legacy_field(
                child,
                field.data_type(),
                field.name().clone(),
            )?))
        }
        (ValueKind::FixedSizeList { item, .. }, DataType::FixedSizeList(field, length)) => {
            DataType::FixedSizeList(
                Arc::new(legacy_field(item, field.data_type(), field.name().clone())?),
                *length,
            )
        }
        (ValueKind::Struct(children), DataType::Struct(fields)) => DataType::Struct(
            children
                .iter()
                .zip(fields)
                .map(|(child, field)| {
                    legacy_field(child.value_type(), field.data_type(), child.name().into())
                        .map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        (ValueKind::Map { key, value }, DataType::Map(entries, sorted)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(ValueTypeError::InvalidType(
                    "typed map projection entries are not a struct",
                ));
            };
            let fields = vec![
                legacy_field(key, fields[0].data_type(), "key".into())?,
                legacy_field(value, fields[1].data_type(), "value".into())?,
            ];
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(fields.into()),
                    false,
                )),
                *sorted,
            )
        }
        (ValueKind::Union(children), DataType::Union(fields, mode)) => {
            let projected = children
                .iter()
                .zip(fields.iter())
                .map(|(child, (_, field))| {
                    legacy_field(
                        child.field.value_type(),
                        field.data_type(),
                        child.field.name().into(),
                    )
                    .map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?;
            DataType::Union(
                UnionFields::try_new(children.iter().map(|child| child.type_id), projected)
                    .map_err(|_| {
                        ValueTypeError::InvalidType("typed union projection has invalid identities")
                    })?,
                *mode,
            )
        }
        _ => layout.clone(),
    };
    let field = Field::new(name, data_type, value.nullable());
    Ok(match value.kind() {
        ValueKind::Nominal(logical) => field.with_metadata(std::collections::HashMap::from([(
            crate::NR_LOGICAL_TYPE_KEY.into(),
            logical
                .metadata_value()
                .ok_or(ValueTypeError::InvalidType(
                    "nominal type has no legacy projection tag",
                ))?
                .into(),
        )])),
        _ => field,
    })
}
