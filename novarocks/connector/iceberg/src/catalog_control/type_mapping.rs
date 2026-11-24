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

use std::sync::Arc;

use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection, connector_reason};
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorDataType, ConnectorDefaultValue, ConnectorError,
    ConnectorErrorKind,
};
use novarocks_type_contract::TemporalType;

use crate::iceberg::spec::{
    FormatVersion, ListType, Literal, MapType, NestedField, PrimitiveLiteral, PrimitiveType,
    StructType, Type,
};

pub(crate) fn schema_fields(
    columns: &[ConnectorColumnDefinition],
    format_version: FormatVersion,
) -> Result<Vec<Arc<NestedField>>, ConnectorError> {
    let mut next_id = i32::try_from(columns.len())
        .map_err(|_| invalid("too many Iceberg columns"))?
        .checked_add(1)
        .ok_or_else(|| invalid("too many Iceberg columns"))?;
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let id = i32::try_from(index + 1).map_err(|_| invalid("too many Iceberg columns"))?;
            column_field(id, column, format_version, &mut next_id).map(Arc::new)
        })
        .collect()
}

pub(crate) fn column_field(
    id: i32,
    column: &ConnectorColumnDefinition,
    format_version: FormatVersion,
    next_id: &mut i32,
) -> Result<NestedField, ConnectorError> {
    let field_type = iceberg_type(&column.data_type, format_version, next_id)?;
    let mut field = NestedField::new(
        id,
        column.name.as_ref(),
        field_type.clone(),
        !column.nullable,
    );
    if let Some(default) = &column.default {
        let literal = if let ConnectorDefaultValue::Temporal { data_type, value } = default {
            let ConnectorDataType::Temporal(requested) = column.data_type else {
                return Err(invalid("temporal default requires a temporal column"));
            };
            Some(crate::default_value::temporal_default_literal(
                *data_type,
                *value,
                requested,
                &field_type,
            )?)
        } else {
            default_literal(default, &field_type).map_err(invalid)?
        };
        if let Some(literal) = literal {
            field = field
                .with_initial_default(literal.clone())
                .with_write_default(literal);
        }
    }
    Ok(field)
}

pub(crate) fn iceberg_type(
    data_type: &ConnectorDataType,
    format_version: FormatVersion,
    next_id: &mut i32,
) -> Result<Type, ConnectorError> {
    let primitive = |value| Ok(Type::Primitive(value));
    match data_type {
        ConnectorDataType::Boolean => primitive(PrimitiveType::Boolean),
        ConnectorDataType::TinyInt | ConnectorDataType::SmallInt | ConnectorDataType::Int => {
            primitive(PrimitiveType::Int)
        }
        ConnectorDataType::BigInt => primitive(PrimitiveType::Long),
        ConnectorDataType::LargeInt => primitive(PrimitiveType::Fixed(
            u64::try_from(novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
                .expect("positive LargeInt width"),
        )),
        ConnectorDataType::Float => primitive(PrimitiveType::Float),
        ConnectorDataType::Double => primitive(PrimitiveType::Double),
        ConnectorDataType::Decimal { precision, scale } => {
            if *scale < 0
                || u8::try_from(*scale)
                    .ok()
                    .is_some_and(|scale| scale > *precision)
            {
                return Err(invalid(format!("invalid DECIMAL({precision},{scale})")));
            }
            Type::decimal(
                u32::from(*precision),
                u32::try_from(*scale).unwrap_or_default(),
            )
            .map_err(|error| {
                invalid(format!(
                    "invalid Iceberg DECIMAL({precision},{scale}): {error}"
                ))
            })
        }
        ConnectorDataType::String
        | ConnectorDataType::BoundedCharacter(_)
        | ConnectorDataType::Json => primitive(PrimitiveType::String),
        ConnectorDataType::Binary | ConnectorDataType::Bitmap | ConnectorDataType::Hll => {
            primitive(PrimitiveType::Binary)
        }
        ConnectorDataType::Date => primitive(PrimitiveType::Date),
        ConnectorDataType::DateTime => primitive(PrimitiveType::Timestamp),
        ConnectorDataType::DateTimeNs => primitive(PrimitiveType::TimestampNs),
        ConnectorDataType::Time => primitive(PrimitiveType::Time),
        ConnectorDataType::Temporal(declaration) => {
            let nanos =
                format_version == FormatVersion::V3 && !declaration.precision().fits_microseconds();
            primitive(match declaration {
                TemporalType::Time(_) => PrimitiveType::Time,
                TemporalType::Timestamp(_) if nanos => PrimitiveType::TimestampNs,
                TemporalType::Timestamp(_) => PrimitiveType::Timestamp,
                TemporalType::TimestampWithTimeZone(_) if nanos => PrimitiveType::TimestamptzNs,
                TemporalType::TimestampWithTimeZone(_) => PrimitiveType::Timestamptz,
                TemporalType::TimeWithTimeZone(_) | TemporalType::Interval(_) => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "Iceberg tables do not support TIME WITH TIME ZONE or INTERVAL columns",
                    ));
                }
            })
        }
        ConnectorDataType::Variant => Ok(Type::Variant(crate::iceberg::spec::VariantType)),
        ConnectorDataType::Array(element) => {
            let element_id = allocate_id(next_id)?;
            let element_type = iceberg_type(element, format_version, next_id)?;
            Ok(Type::List(ListType::new(Arc::new(
                NestedField::list_element(element_id, element_type, false),
            ))))
        }
        ConnectorDataType::Map(key, value) => {
            let key_id = allocate_id(next_id)?;
            let key_type = iceberg_type(key, format_version, next_id)?;
            let value_id = allocate_id(next_id)?;
            let value_type = iceberg_type(value, format_version, next_id)?;
            Ok(Type::Map(MapType::new(
                Arc::new(NestedField::map_key_element(key_id, key_type)),
                Arc::new(NestedField::map_value_element(value_id, value_type, false)),
            )))
        }
        ConnectorDataType::Struct(fields) => {
            // Design: ADR-0265 (docs/adr/ADR-0265-row-declarations-and-canonical-map-keys.md)
            for (index, field) in fields.iter().enumerate() {
                if field.name.is_empty() {
                    return Err(anonymous_row_field());
                }
                if fields[..index]
                    .iter()
                    .any(|previous| equal_row_field_names(&previous.name, &field.name))
                {
                    return Err(duplicate_row_field_name());
                }
            }
            // New-table publication normalizes IDs with the pinned SDK: assign
            // all sibling IDs before descending into any sibling's children.
            let ids = fields
                .iter()
                .map(|_| allocate_id(next_id))
                .collect::<Result<Vec<_>, _>>()?;
            let fields = fields
                .iter()
                .zip(ids)
                .map(|(field, id)| {
                    let field_type = iceberg_type(&field.data_type, format_version, next_id)?;
                    Ok(Arc::new(NestedField::new(
                        id,
                        field.name.as_ref(),
                        field_type,
                        !field.nullable,
                    )))
                })
                .collect::<Result<Vec<_>, ConnectorError>>()?;
            Ok(Type::Struct(StructType::new(fields)))
        }
    }
}

pub(super) fn equal_row_field_names(left: &str, right: &str) -> bool {
    left.chars()
        .flat_map(char::to_lowercase)
        .eq(right.chars().flat_map(char::to_lowercase))
}

pub(super) fn anonymous_row_field() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "Iceberg schema requires names for all ROW fields",
    )
    .with_public_failure_detail()
}

#[derive(Debug)]
struct DuplicateRowFieldName;

impl std::fmt::Display for DuplicateRowFieldName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Iceberg schema contains duplicate ROW field names")
    }
}

impl std::error::Error for DuplicateRowFieldName {}

impl FailureProjection for DuplicateRowFieldName {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Data,
            connector_reason::ICEBERG_DUPLICATE_ROW_FIELD_NAME,
        )
    }
}

pub(super) fn duplicate_row_field_name() -> ConnectorError {
    ConnectorError::from_domain(ConnectorErrorKind::InvalidRequest, DuplicateRowFieldName)
        .with_public_failure_detail()
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn allocate_id(next_id: &mut i32) -> Result<i32, ConnectorError> {
    let id = *next_id;
    *next_id = next_id
        .checked_add(1)
        .ok_or_else(|| invalid("Iceberg field ID space exhausted"))?;
    Ok(id)
}

/// An empty-collection default arrives as the literal text the statement wrote,
/// because the neutral default vocabulary has no collection variant. Only the
/// empty collection is admitted, matching what the read path can materialize.
fn empty_collection_default(text: &str, field_type: &Type) -> Result<Option<Literal>, String> {
    let trimmed: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    match field_type {
        Type::List(_) if trimmed == "[]" => Ok(Some(Literal::List(Vec::new()))),
        Type::Map(_) if trimmed == "{}" => {
            Ok(Some(Literal::Map(crate::iceberg::spec::Map::default())))
        }
        Type::List(_) | Type::Map(_) => Err(format!(
            "only an empty collection default is supported for Iceberg type {field_type}"
        )),
        _ => Ok(None),
    }
}

pub(crate) fn default_literal(
    value: &ConnectorDefaultValue,
    field_type: &Type,
) -> Result<Option<Literal>, String> {
    if let ConnectorDefaultValue::String(text) = value
        && matches!(field_type, Type::List(_) | Type::Map(_))
    {
        return empty_collection_default(text, field_type);
    }
    let primitive = match (value, field_type) {
        (ConnectorDefaultValue::Null, _) => return Ok(None),
        (ConnectorDefaultValue::Bool(value), Type::Primitive(PrimitiveType::Boolean)) => {
            PrimitiveLiteral::Boolean(*value)
        }
        (ConnectorDefaultValue::Int(value), Type::Primitive(PrimitiveType::Int)) => {
            PrimitiveLiteral::Int(i32::try_from(*value).map_err(|_| "INT default is out of range")?)
        }
        (ConnectorDefaultValue::Int(value), Type::Primitive(PrimitiveType::Long)) => {
            PrimitiveLiteral::Long(*value)
        }
        (ConnectorDefaultValue::Float(value), Type::Primitive(PrimitiveType::Float)) => {
            PrimitiveLiteral::Float(ordered_float::OrderedFloat(*value as f32))
        }
        (ConnectorDefaultValue::Float(value), Type::Primitive(PrimitiveType::Double)) => {
            PrimitiveLiteral::Double(ordered_float::OrderedFloat(*value))
        }
        (
            ConnectorDefaultValue::Decimal { unscaled, scale },
            Type::Primitive(PrimitiveType::Decimal {
                scale: expected_scale,
                ..
            }),
        ) if u32::try_from(*scale).ok() == Some(*expected_scale) => {
            PrimitiveLiteral::Int128(*unscaled)
        }
        (ConnectorDefaultValue::String(value), Type::Primitive(PrimitiveType::String)) => {
            PrimitiveLiteral::String(value.to_string())
        }
        (ConnectorDefaultValue::Binary(value), Type::Primitive(PrimitiveType::Binary)) => {
            PrimitiveLiteral::Binary(value.to_vec())
        }
        (ConnectorDefaultValue::Binary(value), Type::Primitive(PrimitiveType::Fixed(width)))
            if usize::try_from(*width).ok() == Some(value.len()) =>
        {
            PrimitiveLiteral::Binary(value.to_vec())
        }
        (ConnectorDefaultValue::Date(value), Type::Primitive(PrimitiveType::Date)) => {
            PrimitiveLiteral::Int(*value)
        }
        (ConnectorDefaultValue::DateTime(value), Type::Primitive(PrimitiveType::Timestamp))
        | (ConnectorDefaultValue::DateTime(value), Type::Primitive(PrimitiveType::TimestampNs))
        | (ConnectorDefaultValue::DateTime(value), Type::Primitive(PrimitiveType::Time)) => {
            PrimitiveLiteral::Long(*value)
        }
        _ => {
            return Err(format!(
                "connector default does not match Iceberg type {field_type}"
            ));
        }
    };
    Ok(Some(Literal::Primitive(primitive)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_spi::connector::ConnectorStructField;
    use novarocks_type_contract::TemporalPrecision;

    #[test]
    fn temporal_defaults_use_the_version_precision_once() {
        use novarocks_functions::temporal_value::parse_timestamp_literal;
        let parsed = parse_timestamp_literal("1970-01-01 00:00:00.0000004995 UTC").unwrap();
        let column = ConnectorColumnDefinition {
            name: "written_at".into(),
            data_type: ConnectorDataType::Temporal(TemporalType::TimestampWithTimeZone(
                TemporalPrecision::PICOSECONDS,
            )),
            nullable: true,
            default: Some(ConnectorDefaultValue::Temporal {
                data_type: parsed.ty,
                value: parsed.value,
            }),
        };
        let v2 = schema_fields(std::slice::from_ref(&column), FormatVersion::V2).unwrap();
        let v3 = schema_fields(std::slice::from_ref(&column), FormatVersion::V3).unwrap();
        assert_eq!(v2[0].initial_default, Some(Literal::long(0)));
        assert_eq!(v3[0].initial_default, Some(Literal::long(500)));
        assert_eq!(v3[0].write_default, v3[0].initial_default);
    }

    #[test]
    fn temporal_projection_uses_one_recursive_storage_version() {
        let columns = [ConnectorColumnDefinition {
            name: "payload".into(),
            data_type: ConnectorDataType::Struct(vec![
                ConnectorStructField {
                    name: "clock".into(),
                    data_type: ConnectorDataType::Temporal(TemporalType::Time(
                        TemporalPrecision::PICOSECONDS,
                    )),
                    nullable: false,
                },
                ConnectorStructField {
                    name: "instant".into(),
                    data_type: ConnectorDataType::Temporal(TemporalType::TimestampWithTimeZone(
                        TemporalPrecision::PICOSECONDS,
                    )),
                    nullable: true,
                },
            ]),
            nullable: true,
            default: None,
        }];
        for (version, expected) in [
            (FormatVersion::V1, PrimitiveType::Timestamptz),
            (FormatVersion::V2, PrimitiveType::Timestamptz),
            (FormatVersion::V3, PrimitiveType::TimestamptzNs),
        ] {
            let fields = schema_fields(&columns, version).unwrap();
            let Type::Struct(payload) = fields[0].field_type.as_ref() else {
                panic!("struct");
            };
            assert_eq!(
                *payload.fields()[0].field_type,
                Type::Primitive(PrimitiveType::Time)
            );
            assert_eq!(*payload.fields()[1].field_type, Type::Primitive(expected));
            assert_eq!(payload.fields()[0].id, 2);
            assert!(payload.fields()[0].required);
            assert!(!payload.fields()[1].required);
        }
        let forbidden = ConnectorDataType::Temporal(TemporalType::TimeWithTimeZone(
            TemporalPrecision::MICROSECONDS,
        ));
        assert_eq!(
            iceberg_type(&forbidden, FormatVersion::V3, &mut 1)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported,
        );
    }

    #[test]
    fn nested_row_schema_requires_present_and_unique_sibling_names() {
        let schema = |second: &str| {
            let field = |name: &str| ConnectorStructField {
                name: name.into(),
                data_type: ConnectorDataType::Int,
                nullable: true,
            };
            [ConnectorColumnDefinition {
                name: "payload".into(),
                data_type: ConnectorDataType::Array(Box::new(ConnectorDataType::Struct(vec![
                    field("Item"),
                    field(second),
                ]))),
                nullable: true,
                default: None,
            }]
        };
        let anonymous = schema_fields(&schema(""), FormatVersion::V3).unwrap_err();
        assert_eq!(anonymous.kind(), ConnectorErrorKind::Unsupported);
        let duplicate = schema_fields(&schema("item"), FormatVersion::V3).unwrap_err();
        assert_eq!(
            duplicate.failure_fact(),
            FailureFact::new(
                FailureClass::Data,
                connector_reason::ICEBERG_DUPLICATE_ROW_FIELD_NAME,
            ),
        );
        assert!(schema_fields(&schema("detail"), FormatVersion::V3).is_ok());
    }

    #[test]
    fn connector_schema_mapping_preserves_nested_nullability_and_unique_ids() {
        let columns = vec![ConnectorColumnDefinition {
            name: "payload".into(),
            data_type: ConnectorDataType::Struct(vec![ConnectorStructField {
                name: "items".into(),
                data_type: ConnectorDataType::Array(Box::new(ConnectorDataType::LargeInt)),
                nullable: false,
            }]),
            nullable: true,
            default: None,
        }];
        let fields = schema_fields(&columns, FormatVersion::V2).expect("schema fields");
        assert_eq!(fields[0].id, 1);
        assert!(!fields[0].required);
        let Type::Struct(struct_type) = fields[0].field_type.as_ref() else {
            panic!("expected struct");
        };
        assert!(struct_type.fields()[0].required);
        let Type::List(list_type) = struct_type.fields()[0].field_type.as_ref() else {
            panic!("expected list");
        };
        assert_ne!(struct_type.fields()[0].id, list_type.element_field.id);
        assert_eq!(
            list_type.element_field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Fixed(16))
        );
    }

    #[test]
    fn connector_defaults_are_checked_against_the_authoritative_type() {
        let mismatch = default_literal(
            &ConnectorDefaultValue::Binary(Bytes::from_static(b"abc")),
            &Type::Primitive(PrimitiveType::Fixed(16)),
        )
        .expect_err("fixed default width mismatch");
        assert!(mismatch.contains("does not match"), "{mismatch}");

        let decimal = ConnectorDataType::Decimal {
            precision: 10,
            scale: -1,
        };
        assert!(iceberg_type(&decimal, FormatVersion::V2, &mut 1).is_err());
    }
}
