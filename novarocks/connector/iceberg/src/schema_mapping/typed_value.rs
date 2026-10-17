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

//! Complete values from explicit Iceberg declarations. Arrow annotations are
//! an outward format projection and supply none of these semantic decisions.

use crate::iceberg::spec::{PrimitiveType, Type};
use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_connector_contract::{ConnectorError, ConnectorErrorKind, PureProviderCompileError};
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use novarocks_type_contract::{
    FunctionValueType, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
};
use std::sync::Arc;

pub(crate) type Failure = PureProviderCompileError<ConnectorError>;
pub(crate) struct DeclaredValue {
    pub(crate) source_layout: DataType,
    pub(crate) binding: FunctionValueType,
}
fn invalid(message: &'static str) -> Failure {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message).into()
}
fn resource() -> Failure {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Iceberg complete value construction exceeds its finite source bounds",
    )
    .into()
}

/// Source offset choices remain the existing SQL-read mapping: LIST uses
/// 32-bit offsets, ordinary BINARY is Binary, VARIANT is LargeBinary, and UTC
/// storage timestamps use the existing timezone-free SQL execution carrier.
pub(crate) fn from_declaration(
    declaration: &Type,
    nullable: bool,
    nominal: Option<ValueLogicalType>,
    integer: Option<crate::scalar_integer_domain::ScalarIntegerDomain>,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<DeclaredValue, Failure> {
    from_declaration_with_nominals(
        declaration,
        nullable,
        nominal,
        integer,
        &Default::default(),
        creation,
    )
}

pub(crate) fn from_field(
    field: &crate::iceberg::spec::NestedField,
    nullable: bool,
    nominals: &super::nominal_declaration::NominalSourceDeclarations,
    integer: Option<crate::scalar_integer_domain::ScalarIntegerDomain>,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<DeclaredValue, Failure> {
    from_declaration_with_nominals(
        &field.field_type,
        nullable,
        nominals.get(&field.id).map(|domain| domain.logical_type()),
        integer,
        nominals,
        creation,
    )
}

fn from_declaration_with_nominals(
    declaration: &Type,
    nullable: bool,
    nominal: Option<ValueLogicalType>,
    integer: Option<crate::scalar_integer_domain::ScalarIntegerDomain>,
    nominals: &super::nominal_declaration::NominalSourceDeclarations,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<DeclaredValue, Failure> {
    let (source_layout, layout, semantic) = author(
        declaration,
        nullable,
        nominal,
        integer,
        nominals,
        1,
        &mut 0,
        creation,
    )?;
    creation.request(ValueLayout::storage_allocation_layout().map_err(|_| resource())?)?;
    let storage = creation.storage();
    let layout = ValueLayout::try_from_normalized_arrow_observed(layout, storage, |units| {
        creation.work(units).map_err(LayoutFailure::Caller)
    })
    .map_err(LayoutFailure::into_failure)?;
    creation.work(1)?;
    let binding = FunctionValueType::try_from_value(semantic, layout).map_err(|_| {
        invalid("Iceberg complete declaration differs from its exact execution layout")
    })?;
    Ok(DeclaredValue {
        source_layout,
        binding,
    })
}

fn author(
    declaration: &Type,
    nullable: bool,
    nominal: Option<ValueLogicalType>,
    integer: Option<crate::scalar_integer_domain::ScalarIntegerDomain>,
    nominals: &super::nominal_declaration::NominalSourceDeclarations,
    depth: usize,
    nodes: &mut usize,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(DataType, DataType, ValueTypeRef), Failure> {
    creation.work(1)?;
    *nodes = nodes.checked_add(1).ok_or_else(resource)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH
        || *nodes > novarocks_type_contract::MAX_VALUE_TYPE_NODES
    {
        return Err(resource());
    }
    let storage = creation.storage();
    let (source, layout, kind) = match declaration {
        Type::Primitive(_) | Type::Variant(_) => {
            let (data_type, domain) = match declaration {
                Type::Primitive(primitive) => (
                    primitive_layout(primitive, integer)?,
                    nominal.unwrap_or(match primitive {
                        PrimitiveType::Uuid => ValueLogicalType::Uuid,
                        _ => ValueLogicalType::Physical,
                    }),
                ),
                Type::Variant(_) => {
                    if integer.is_some() {
                        return Err(invalid(
                            "Iceberg narrow integer domain targets non-INT storage",
                        ));
                    }
                    // This is the existing SQL declaration carrier, not an SDK
                    // physical Variant/shredding conversion or writer capability.
                    (
                        DataType::LargeBinary,
                        nominal.unwrap_or(ValueLogicalType::Variant),
                    )
                }
                _ => unreachable!("scalar declaration arm"),
            };
            domain.validate_carrier(&data_type).map_err(|_| {
                invalid("Iceberg source domain conflicts with its declared primitive carrier")
            })?;
            let kind = match domain {
                ValueLogicalType::Physical => match data_type {
                    DataType::Utf8 => ValueKind::String,
                    DataType::Binary => ValueKind::Binary,
                    _ => ValueKind::Scalar(data_type.clone()),
                },
                domain => ValueKind::Nominal(domain),
            };
            (data_type.clone(), data_type, kind)
        }
        Type::Struct(fields) => {
            if nominal.is_some() || integer.is_some() {
                return Err(invalid("Iceberg scalar declaration targets a struct"));
            }
            let mut source = creation.reserve::<Arc<Field>>(fields.fields().len())?;
            let mut layouts = creation.reserve::<Arc<Field>>(fields.fields().len())?;
            let mut semantic = creation.reserve::<ValueField>(fields.fields().len())?;
            for field in fields.fields() {
                let (source_type, layout_type, child) = author(
                    &field.field_type,
                    !field.required,
                    nominals.get(&field.id).map(|domain| domain.logical_type()),
                    None,
                    nominals,
                    depth + 1,
                    nodes,
                    creation,
                )?;
                let source_name = creation.text(&field.name)?;
                source.push(creation.arc(Field::new(source_name, source_type, !field.required))?);
                let layout_name = creation.text(&field.name)?;
                layouts.push(creation.arc(Field::new(
                    layout_name,
                    layout_type,
                    !field.required,
                ))?);
                let name = creation.text(&field.name)?.into_boxed_str();
                creation
                    .request(ValueField::storage_allocation_layout().map_err(|_| resource())?)?;
                creation.work(1)?;
                semantic.push(ValueField::new(name, child, storage.clone()));
            }
            (
                DataType::Struct(creation.arrow_fields(source)?),
                DataType::Struct(creation.arrow_fields(layouts)?),
                ValueKind::Struct(semantic.into_boxed_slice()),
            )
        }
        Type::List(list) => {
            if nominal.is_some() || integer.is_some() {
                return Err(invalid("Iceberg scalar declaration targets a list"));
            }
            let item = &list.element_field;
            let (source_type, layout_type, child) = author(
                &item.field_type,
                !item.required,
                nominals.get(&item.id).map(|domain| domain.logical_type()),
                None,
                nominals,
                depth + 1,
                nodes,
                creation,
            )?;
            let source_name = creation.text(&item.name)?;
            let source_field =
                creation.arc(Field::new(source_name, source_type, !item.required))?;
            let layout_name = creation.text("item")?;
            let layout_field =
                creation.arc(Field::new(layout_name, layout_type, !item.required))?;
            (
                DataType::List(source_field),
                DataType::List(layout_field),
                ValueKind::List(child),
            )
        }
        Type::Map(map) => {
            if nominal.is_some() || integer.is_some() {
                return Err(invalid("Iceberg scalar declaration targets a map"));
            }
            let mut sources = creation.reserve::<Arc<Field>>(2)?;
            let mut layouts = creation.reserve::<Arc<Field>>(2)?;
            let (key_source, key_layout, key) = author(
                &map.key_field.field_type,
                !map.key_field.required,
                nominals
                    .get(&map.key_field.id)
                    .map(|domain| domain.logical_type()),
                None,
                nominals,
                depth + 2,
                nodes,
                creation,
            )?;
            let (value_source, value_layout, value) = author(
                &map.value_field.field_type,
                !map.value_field.required,
                nominals
                    .get(&map.value_field.id)
                    .map(|domain| domain.logical_type()),
                None,
                nominals,
                depth + 2,
                nodes,
                creation,
            )?;
            for (field, source_type, layout_type, role) in [
                (&map.key_field, key_source, key_layout, "key"),
                (&map.value_field, value_source, value_layout, "value"),
            ] {
                let source_name = creation.text(&field.name)?;
                sources.push(creation.arc(Field::new(
                    source_name,
                    source_type,
                    !field.required,
                ))?);
                let layout_name = creation.text(role)?;
                layouts.push(creation.arc(Field::new(
                    layout_name,
                    layout_type,
                    !field.required,
                ))?);
            }
            let source_entries = DataType::Struct(creation.arrow_fields(sources)?);
            let layout_entries = DataType::Struct(creation.arrow_fields(layouts)?);
            let source_name = creation.text("entries")?;
            let source_entries = creation.arc(Field::new(source_name, source_entries, false))?;
            let layout_name = creation.text("entries")?;
            let layout_entries = creation.arc(Field::new(layout_name, layout_entries, false))?;
            (
                DataType::Map(source_entries, false),
                DataType::Map(layout_entries, false),
                ValueKind::Map { key, value },
            )
        }
    };
    creation.request(ValueTypeRef::storage_allocation_layout().map_err(|_| resource())?)?;
    creation.work(1)?;
    let semantic = ValueTypeRef::try_new(kind, nullable, storage)
        .map_err(|_| invalid("Iceberg source has an invalid complete semantic declaration"))?;
    Ok((source, layout, semantic))
}

pub(crate) fn primitive_layout(
    primitive: &PrimitiveType,
    integer: Option<crate::scalar_integer_domain::ScalarIntegerDomain>,
) -> Result<DataType, Failure> {
    if let Some(integer) = integer {
        if primitive != &PrimitiveType::Int {
            return Err(invalid(
                "Iceberg narrow integer domain targets non-INT storage",
            ));
        }
        return Ok(integer.data_type());
    }
    Ok(match primitive {
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::Long => DataType::Int64,
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Decimal { precision, scale } => DataType::Decimal128(
            u8::try_from(*precision)
                .map_err(|_| invalid("Iceberg decimal precision exceeds its carrier"))?,
            i8::try_from(*scale)
                .map_err(|_| invalid("Iceberg decimal scale exceeds its carrier"))?,
        ),
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs => {
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        }
        PrimitiveType::String => DataType::Utf8,
        PrimitiveType::Uuid => DataType::FixedSizeBinary(16),
        PrimitiveType::Fixed(width) => DataType::FixedSizeBinary(
            i32::try_from(*width)
                .map_err(|_| invalid("Iceberg fixed width exceeds its exact carrier"))?,
        ),
        PrimitiveType::Binary => DataType::Binary,
    })
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
            Self::Source => invalid("Iceberg normalized execution layout is invalid"),
        }
    }
}
