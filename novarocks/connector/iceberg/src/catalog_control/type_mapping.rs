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

use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorDataType, ConnectorDefaultValue,
};

use crate::iceberg::spec::{
    ListType, Literal, MapType, NestedField, PrimitiveLiteral, PrimitiveType, StructType, Type,
};

pub(crate) fn schema_fields(
    columns: &[ConnectorColumnDefinition],
) -> Result<Vec<Arc<NestedField>>, String> {
    let mut next_id = i32::try_from(columns.len())
        .map_err(|_| "too many Iceberg columns".to_string())?
        .checked_add(1)
        .ok_or_else(|| "too many Iceberg columns".to_string())?;
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let id =
                i32::try_from(index + 1).map_err(|_| "too many Iceberg columns".to_string())?;
            column_field(id, column, &mut next_id).map(Arc::new)
        })
        .collect()
}

pub(crate) fn column_field(
    id: i32,
    column: &ConnectorColumnDefinition,
    next_id: &mut i32,
) -> Result<NestedField, String> {
    let field_type = iceberg_type(&column.data_type, next_id)?;
    let mut field = NestedField::new(
        id,
        column.name.as_ref(),
        field_type.clone(),
        !column.nullable,
    );
    if let Some(default) = &column.default
        && let Some(literal) = default_literal(default, &field_type)?
    {
        field = field
            .with_initial_default(literal.clone())
            .with_write_default(literal);
    }
    Ok(field)
}

pub(crate) fn iceberg_type(
    data_type: &ConnectorDataType,
    next_id: &mut i32,
) -> Result<Type, String> {
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
                return Err(format!("invalid DECIMAL({precision},{scale})"));
            }
            Type::decimal(
                u32::from(*precision),
                u32::try_from(*scale).unwrap_or_default(),
            )
            .map_err(|error| format!("invalid Iceberg DECIMAL({precision},{scale}): {error}"))
        }
        ConnectorDataType::String | ConnectorDataType::Json => primitive(PrimitiveType::String),
        ConnectorDataType::Binary | ConnectorDataType::Bitmap | ConnectorDataType::Hll => {
            primitive(PrimitiveType::Binary)
        }
        ConnectorDataType::Date => primitive(PrimitiveType::Date),
        ConnectorDataType::DateTime => primitive(PrimitiveType::Timestamp),
        ConnectorDataType::DateTimeNs => primitive(PrimitiveType::TimestampNs),
        ConnectorDataType::Time => primitive(PrimitiveType::Time),
        ConnectorDataType::Variant => Ok(Type::Variant(crate::iceberg::spec::VariantType)),
        ConnectorDataType::Array(element) => {
            let element_id = allocate_id(next_id)?;
            let element_type = iceberg_type(element, next_id)?;
            Ok(Type::List(ListType::new(Arc::new(
                NestedField::list_element(element_id, element_type, false),
            ))))
        }
        ConnectorDataType::Map(key, value) => {
            let key_id = allocate_id(next_id)?;
            let key_type = iceberg_type(key, next_id)?;
            let value_id = allocate_id(next_id)?;
            let value_type = iceberg_type(value, next_id)?;
            Ok(Type::Map(MapType::new(
                Arc::new(NestedField::map_key_element(key_id, key_type)),
                Arc::new(NestedField::map_value_element(value_id, value_type, false)),
            )))
        }
        ConnectorDataType::Struct(fields) => {
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
                    let field_type = iceberg_type(&field.data_type, next_id)?;
                    Ok(Arc::new(NestedField::new(
                        id,
                        field.name.as_ref(),
                        field_type,
                        !field.nullable,
                    )))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(Type::Struct(StructType::new(fields)))
        }
    }
}

fn allocate_id(next_id: &mut i32) -> Result<i32, String> {
    let id = *next_id;
    *next_id = next_id
        .checked_add(1)
        .ok_or_else(|| "Iceberg field ID space exhausted".to_string())?;
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
            aggregation: None,
            default: None,
        }];
        let fields = schema_fields(&columns).expect("schema fields");
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
        assert!(iceberg_type(&decimal, &mut 1).is_err());
    }
}
