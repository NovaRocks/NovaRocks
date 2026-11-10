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

use crate::iceberg::spec::{
    Literal, PrimitiveLiteral, PrimitiveType, Schema, Struct, TableMetadata, Type,
};

/// A historical type failure never replaces its creating caller's control cause.
#[derive(Debug)]
pub(crate) enum PartitionSourceTypeFailure<E> {
    Semantic(String),
    Original(E),
}
impl<E> From<String> for PartitionSourceTypeFailure<E> {
    fn from(message: String) -> Self {
        Self::Semantic(message)
    }
}

pub(crate) fn partition_source_types(
    metadata: &TableMetadata,
    query: &Schema,
    source_id: i32,
) -> Result<(PrimitiveType, PrimitiveType), String> {
    partition_source_types_checked(metadata, query, source_id, || {
        Ok::<(), std::convert::Infallible>(())
    })
    .map_err(|cause| match cause {
        PartitionSourceTypeFailure::Semantic(message) => message,
        PartitionSourceTypeFailure::Original(never) => match never {},
    })
}

/// One exact retained-type policy shared by read domains and metadata authors.
/// Primitive declarations own no variable-size backing; each schema lookup is
/// observed before entry and caller failures retain their original identity.
pub(crate) fn partition_source_types_checked<E>(
    metadata: &TableMetadata,
    query: &Schema,
    source_id: i32,
    mut active: impl FnMut() -> Result<(), E>,
) -> Result<(PrimitiveType, PrimitiveType), PartitionSourceTypeFailure<E>> {
    let mut historical = None;
    for schema in metadata.schemas_iter() {
        active().map_err(PartitionSourceTypeFailure::Original)?;
        let Some(source) = schema.field_by_id(source_id) else {
            continue;
        };
        let Type::Primitive(ty) = source.field_type.as_ref() else {
            return Err(format!(
                "partition source {} is not primitive in retained schema {}",
                source_id,
                schema.schema_id()
            )
            .into());
        };
        historical = Some(match historical {
            None => ty.clone(),
            Some(previous) => legal_partition_type_join(metadata.format_version(), &previous, ty)
                .ok_or_else(|| {
                format!(
                    "partition source {} has incompatible retained types {previous:?} and {ty:?}",
                    source_id
                )
            })?,
        });
    }
    let historical = historical.ok_or_else(|| {
        format!(
            "partition source {} has no retained schema type evidence",
            source_id
        )
    })?;
    active().map_err(PartitionSourceTypeFailure::Original)?;
    let resolved = match query.field_by_id(source_id) {
        Some(source) => {
            let Type::Primitive(ty) = source.field_type.as_ref() else {
                return Err(format!(
                    "partition source {} is not primitive in the query schema",
                    source_id
                )
                .into());
            };
            legal_partition_type_join(metadata.format_version(), &historical, ty).ok_or_else(
                || {
                    format!(
                        "query partition source {} conflicts with retained schema types",
                        source_id
                    )
                },
            )?;
            ty.clone()
        }
        None => historical.clone(),
    };
    Ok((resolved, historical))
}

pub(crate) fn legal_partition_type_join(
    version: crate::iceberg::spec::FormatVersion,
    a: &PrimitiveType,
    b: &PrimitiveType,
) -> Option<PrimitiveType> {
    use PrimitiveType::*;
    if a == b {
        return Some(a.clone());
    }
    match (a, b) {
        (Int, Long) | (Long, Int) => Some(Long),
        (Float, Double) | (Double, Float) => Some(Double),
        (Date, Timestamp) | (Timestamp, Date)
            if version == crate::iceberg::spec::FormatVersion::V3 =>
        {
            Some(Timestamp)
        }
        (Date, TimestampNs) | (TimestampNs, Date)
            if version == crate::iceberg::spec::FormatVersion::V3 =>
        {
            Some(TimestampNs)
        }
        (
            Decimal {
                precision: p,
                scale: s,
            },
            Decimal {
                precision: q,
                scale: t,
            },
        ) if s == t => Some(Decimal {
            precision: (*p).max(*q),
            scale: *s,
        }),
        _ => None,
    }
}

pub(crate) fn resolved_partition_values(
    values: &Struct,
    partition_type: &crate::iceberg::spec::StructType,
) -> Struct {
    Struct::from_iter(
        values
            .fields()
            .iter()
            .zip(partition_type.fields())
            .map(|(value, field)| match (value, field.field_type.as_ref()) {
                (Some(Literal::Primitive(value)), Type::Primitive(target)) => Some(
                    Literal::Primitive(resolved_partition_primitive(value, target).into_owned()),
                ),
                _ => value.clone(),
            }),
    )
}

/// The admitted lossless partition promotions; unrelated literals remain exact
/// and are rejected by their final typed consumer rather than coerced here.
pub(crate) fn resolved_partition_primitive<'a>(
    value: &'a PrimitiveLiteral,
    target: &PrimitiveType,
) -> std::borrow::Cow<'a, PrimitiveLiteral> {
    match (value, target) {
        (PrimitiveLiteral::Int(value), PrimitiveType::Long) => {
            std::borrow::Cow::Owned(PrimitiveLiteral::Long(i64::from(*value)))
        }
        (PrimitiveLiteral::Float(value), PrimitiveType::Double) => {
            std::borrow::Cow::Owned(PrimitiveLiteral::Double(f64::from(value.0).into()))
        }
        _ => std::borrow::Cow::Borrowed(value),
    }
}
