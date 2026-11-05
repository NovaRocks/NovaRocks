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

//! Standard Iceberg view types, following Trino 483's TypeConverter.
//! SQL view types carry no non-null promise. Table declarations keep their
//! independent required/nominal rules; no view summary can recover a type.

use crate::iceberg::spec::{
    ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_connector_contract::PureProviderCompileError;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorViewColumn, ConnectorViewDefinition,
    read_stack::ConnectorReadSchemaCreation,
};
use novarocks_type_contract::{
    FunctionValueType, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
};
use std::sync::Arc;

type Failure = PureProviderCompileError<ConnectorError>;
fn unsupported_failure() -> Failure {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "Iceberg view type has no supported Trino 483 mapping",
    )
    .into()
}
fn unsupported<T>() -> Result<T, Failure> {
    Err(unsupported_failure())
}
fn arc_text(
    source: &str,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Arc<str>, Failure> {
    let text = creation.text(source)?;
    creation.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(
            std::alloc::Layout::array::<u8>(text.len()).map_err(|_| resource())?,
        )
        .map_err(|_| resource())?,
    )?;
    creation.work(text.len())?;
    Ok(text.into())
}
fn resource() -> Failure {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Iceberg view type exceeds its original structural bounds",
    )
    .into()
}
fn visit(
    depth: usize,
    nodes: &mut usize,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), Failure> {
    creation.work(1)?;
    *nodes = nodes.checked_add(1).ok_or_else(resource)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH
        || *nodes > novarocks_type_contract::MAX_VALUE_TYPE_NODES
    {
        return Err(resource());
    }
    Ok(())
}
fn next_id(next: &mut i32) -> Result<i32, Failure> {
    let value = *next;
    *next = next.checked_add(1).ok_or_else(resource)?;
    Ok(value)
}

fn nested_field(
    id: i32,
    name: String,
    field_type: Type,
    required: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Arc<NestedField>, Failure> {
    creation.request(std::alloc::Layout::new::<Type>())?;
    creation.work(1)?;
    creation.arc(NestedField {
        id,
        name,
        required,
        field_type: Box::new(field_type),
        doc: None,
        initial_default: None,
        write_default: None,
    })
}

pub(super) fn schema(
    definition: &ConnectorViewDefinition,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Schema, Failure> {
    let mut next = 1;
    let mut fields = creation.reserve::<Arc<NestedField>>(definition.columns.len())?;
    for column in definition.columns.iter() {
        let ty = write_type(
            column.value_type.value_type(),
            &mut next,
            1,
            &mut 0,
            creation,
        )?;
        let id = next_id(&mut next)?;
        let name = creation.text(&column.name)?;
        // IcebergUtil.schemaFromViewColumns uses required top-level fields;
        // this never grants the SQL result a non-null promise.
        fields.push(nested_field(id, name, ty, true, creation)?);
    }
    Schema::builder().with_fields(fields).build().map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "invalid standard Iceberg view schema",
        )
        .into()
    })
}
fn write_type(
    value: &ValueTypeRef,
    next: &mut i32,
    depth: usize,
    nodes: &mut usize,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Type, Failure> {
    visit(depth, nodes, creation)?;
    let primitive = |ty| Type::Primitive(ty);
    // The persistent declaration records SQL value types. Actual Arrow layout
    // remains with the analyzed body; it grants no additional SQL type fact.
    Ok(match value.kind() {
        ValueKind::String => primitive(PrimitiveType::String),
        ValueKind::Binary => primitive(PrimitiveType::Binary),
        ValueKind::Nominal(ValueLogicalType::Uuid) => primitive(PrimitiveType::Uuid),
        ValueKind::Nominal(ValueLogicalType::Variant) => {
            Type::Variant(crate::iceberg::spec::VariantType)
        }
        ValueKind::Scalar(ty) => match ty {
            DataType::Boolean => primitive(PrimitiveType::Boolean),
            DataType::Int32 => primitive(PrimitiveType::Int),
            DataType::Int64 => primitive(PrimitiveType::Long),
            DataType::Float32 => primitive(PrimitiveType::Float),
            DataType::Float64 => primitive(PrimitiveType::Double),
            DataType::Decimal128(p, s) if *p > 0 && *p <= 38 && *s >= 0 && *s <= *p as i8 => {
                Type::decimal(u32::from(*p), u32::try_from(*s).map_err(|_| resource())?)
                    .map_err(|_| resource())?
            }
            DataType::Date32 => primitive(PrimitiveType::Date),
            DataType::Time64(TimeUnit::Microsecond) => primitive(PrimitiveType::Time),
            DataType::Timestamp(TimeUnit::Microsecond, None) => primitive(PrimitiveType::Timestamp),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                primitive(PrimitiveType::TimestampNs)
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == "UTC" => {
                primitive(PrimitiveType::Timestamptz)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, Some(zone)) if zone.as_ref() == "UTC" => {
                primitive(PrimitiveType::TimestamptzNs)
            }
            _ => return unsupported(),
        },
        ValueKind::List(child) => {
            let id = next_id(next)?;
            let ty = write_type(child, next, depth + 1, nodes, creation)?;
            let name = creation.text("element")?;
            Type::List(ListType::new(nested_field(id, name, ty, false, creation)?))
        }
        ValueKind::Map { key, value } => {
            let key_id = next_id(next)?;
            let value_id = next_id(next)?;
            let key = write_type(key, next, depth + 1, nodes, creation)?;
            let value = write_type(value, next, depth + 1, nodes, creation)?;
            let key_name = creation.text("key")?;
            let key = nested_field(key_id, key_name, key, true, creation)?;
            let value_name = creation.text("value")?;
            let value = nested_field(value_id, value_name, value, false, creation)?;
            Type::Map(MapType::new(key, value))
        }
        ValueKind::Struct(children) => {
            let mut fields = creation.reserve::<Arc<NestedField>>(children.len())?;
            for (index, child) in children.iter().enumerate() {
                if child.name().is_empty() {
                    return unsupported();
                }
                for previous in children.iter().take(index) {
                    creation.work(
                        previous
                            .name()
                            .len()
                            .checked_add(child.name().len())
                            .ok_or_else(resource)?,
                    )?;
                    if previous
                        .name()
                        .chars()
                        .flat_map(char::to_lowercase)
                        .eq(child.name().chars().flat_map(char::to_lowercase))
                    {
                        return unsupported();
                    }
                }
                let id = next_id(next)?;
                let ty = write_type(child.value_type(), next, depth + 1, nodes, creation)?;
                let name = creation.text(child.name())?;
                fields.push(nested_field(id, name, ty, false, creation)?);
            }
            Type::Struct(StructType::new(fields))
        }
        _ => return unsupported(),
    })
}

pub(super) fn columns(
    schema: &Schema,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorViewColumn>, Failure> {
    let mut columns = creation.reserve::<ConnectorViewColumn>(schema.as_struct().fields().len())?;
    for field in schema.as_struct().fields() {
        let (layout, semantic) = read_type(&field.field_type, true, 1, &mut 0, creation)?;
        creation.request(ValueLayout::storage_allocation_layout().map_err(|_| resource())?)?;
        let storage = creation.storage();
        let layout = ValueLayout::try_from_normalized_arrow_observed(layout, storage, |units| {
            creation.work(units).map_err(LayoutFailure::Caller)
        })
        .map_err(LayoutFailure::into_failure)?;
        let value_type = FunctionValueType::try_from_value(semantic, layout)
            .map_err(|_| unsupported_failure())?;
        columns.push(ConnectorViewColumn {
            name: arc_text(&field.name, creation)?,
            value_type,
        });
    }
    Ok(columns)
}
fn read_type(
    declaration: &Type,
    nullable: bool,
    depth: usize,
    nodes: &mut usize,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(DataType, ValueTypeRef), Failure> {
    visit(depth, nodes, creation)?;
    let storage = creation.storage();
    let (layout, kind) = match declaration {
        Type::Primitive(primitive) => {
            let ty = match primitive {
                PrimitiveType::Fixed(_) => DataType::Binary,
                PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
                    let unit = if matches!(primitive, PrimitiveType::Timestamptz) {
                        TimeUnit::Microsecond
                    } else {
                        TimeUnit::Nanosecond
                    };
                    DataType::Timestamp(unit, Some(arc_text("UTC", creation)?))
                }
                _ => crate::schema_mapping::typed_value::primitive_layout(primitive, None)?,
            };
            let kind = match primitive {
                PrimitiveType::Uuid => ValueKind::Nominal(ValueLogicalType::Uuid),
                PrimitiveType::String => ValueKind::String,
                PrimitiveType::Binary | PrimitiveType::Fixed(_) => ValueKind::Binary,
                _ => ValueKind::Scalar(ty.clone()),
            };
            (ty, kind)
        }
        Type::Variant(_) => (
            DataType::LargeBinary,
            ValueKind::Nominal(ValueLogicalType::Variant),
        ),
        Type::Struct(fields) => {
            let mut layout = creation.reserve::<Arc<Field>>(fields.fields().len())?;
            let mut semantic = creation.reserve::<ValueField>(fields.fields().len())?;
            for field in fields.fields() {
                let (ty, child) = read_type(&field.field_type, true, depth + 1, nodes, creation)?;
                let field_name = creation.text(&field.name)?;
                layout.push(creation.arc(Field::new(field_name, ty, true))?);
                let name = creation.text(&field.name)?.into_boxed_str();
                creation
                    .request(ValueField::storage_allocation_layout().map_err(|_| resource())?)?;
                semantic.push(ValueField::new(name, child, storage.clone()));
            }
            (
                DataType::Struct(creation.arrow_fields(layout)?),
                ValueKind::Struct(semantic.into_boxed_slice()),
            )
        }
        Type::List(list) => {
            let (ty, child) = read_type(
                &list.element_field.field_type,
                true,
                depth + 1,
                nodes,
                creation,
            )?;
            let name = creation.text("item")?;
            (
                DataType::List(creation.arc(Field::new(name, ty, true))?),
                ValueKind::List(child),
            )
        }
        Type::Map(map) => {
            let (key_type, key) =
                read_type(&map.key_field.field_type, false, depth + 1, nodes, creation)?;
            let (value_type, value) = read_type(
                &map.value_field.field_type,
                true,
                depth + 1,
                nodes,
                creation,
            )?;
            let mut fields = creation.reserve::<Arc<Field>>(2)?;
            let key_name = creation.text("key")?;
            fields.push(creation.arc(Field::new(key_name, key_type, false))?);
            let value_name = creation.text("value")?;
            fields.push(creation.arc(Field::new(value_name, value_type, true))?);
            let entries = creation.arrow_fields(fields)?;
            let name = creation.text("entries")?;
            (
                DataType::Map(
                    creation.arc(Field::new(name, DataType::Struct(entries), false))?,
                    false,
                ),
                ValueKind::Map { key, value },
            )
        }
    };
    creation.request(ValueTypeRef::storage_allocation_layout().map_err(|_| resource())?)?;
    let semantic =
        ValueTypeRef::try_new(kind, nullable, storage).map_err(|_| unsupported_failure())?;
    Ok((layout, semantic))
}

enum LayoutFailure {
    Caller(Failure),
    Source,
}
impl From<novarocks_type_contract::ExecutionValueTypeError> for LayoutFailure {
    fn from(_: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Source
    }
}
impl LayoutFailure {
    fn into_failure(self) -> Failure {
        match self {
            Self::Caller(cause) => cause,
            Self::Source => unsupported_failure(),
        }
    }
}
