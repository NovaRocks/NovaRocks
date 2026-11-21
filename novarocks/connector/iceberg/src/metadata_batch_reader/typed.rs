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

//! Provider-owned Iceberg metadata-table output schemas: which columns a
//! metadata-table alias exposes, and the Arrow schema its rows are shaped to.

//! The metadata alias author explicitly declares physical values at every
//! level. Generic Arrow attributes establish no nominal value domain.

use super::*;
use novarocks_connector_contract::{ConnectorReadExecutionSchema, PureProviderCompileError};
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::{
    FunctionValueType, ValueField, ValueKind, ValueLayout, ValueTypeRef,
};

type Failure = PureProviderCompileError<ConnectorError>;
fn bad() -> Failure {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "metadata alias declaration cannot author this physical value",
    )
    .into()
}
fn extent() -> Failure {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "metadata alias declaration exceeds its finite source model",
    )
    .into()
}
#[derive(Debug)]
enum LayoutError {
    Caller(Failure),
    Source,
}
impl From<novarocks_type_contract::ExecutionValueTypeError> for LayoutError {
    fn from(_: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Source
    }
}
impl LayoutError {
    fn failure(self) -> Failure {
        match self {
            Self::Caller(cause) => cause,
            Self::Source => bad(),
        }
    }
}
fn declaration(
    ty: &DataType,
    nullable: bool,
    depth: usize,
    nodes: &mut usize,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(DataType, ValueTypeRef), Failure> {
    creation.work(1)?;
    *nodes = nodes.checked_add(1).ok_or_else(extent)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH
        || *nodes > novarocks_type_contract::MAX_VALUE_TYPE_NODES
    {
        return Err(extent());
    }
    let (layout, kind) = match ty {
        DataType::Struct(fields) => {
            let mut layouts = creation.reserve(fields.len())?;
            let mut values = creation.reserve(fields.len())?;
            for field in fields {
                let (ty, value) = declaration(
                    field.data_type(),
                    field.is_nullable(),
                    depth + 1,
                    nodes,
                    creation,
                )?;
                let name = creation.text(field.name())?;
                layouts.push(creation.arc(Field::new(name, ty, field.is_nullable()))?);
                let name = creation.text(field.name())?.into_boxed_str();
                creation.request(ValueField::storage_allocation_layout().map_err(|_| extent())?)?;
                creation.work(1)?;
                values.push(ValueField::new(name, value, creation.storage()));
            }
            (
                DataType::Struct(creation.arrow_fields(layouts)?),
                ValueKind::Struct(values.into_boxed_slice()),
            )
        }
        DataType::List(field) => {
            let (ty, value) = declaration(
                field.data_type(),
                field.is_nullable(),
                depth + 1,
                nodes,
                creation,
            )?;
            let name = creation.text("item")?;
            let field = creation.arc(Field::new(name, ty, field.is_nullable()))?;
            (DataType::List(field), ValueKind::List(value))
        }
        DataType::Map(entries, ordered) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(bad());
            };
            if fields.len() != 2 {
                return Err(bad());
            }
            let (key_ty, key) =
                declaration(fields[0].data_type(), false, depth + 2, nodes, creation)?;
            let (value_ty, value) = declaration(
                fields[1].data_type(),
                fields[1].is_nullable(),
                depth + 2,
                nodes,
                creation,
            )?;
            let mut fields = creation.reserve(2)?;
            let name = creation.text("key")?;
            fields.push(creation.arc(Field::new(name, key_ty, false))?);
            let name = creation.text("value")?;
            fields.push(creation.arc(Field::new(name, value_ty, value.nullable()))?);
            let entries = DataType::Struct(creation.arrow_fields(fields)?);
            let name = creation.text("entries")?;
            (
                DataType::Map(creation.arc(Field::new(name, entries, false))?, *ordered),
                ValueKind::Map { key, value },
            )
        }
        DataType::Utf8 => (DataType::Utf8, ValueKind::String),
        DataType::Binary => (DataType::Binary, ValueKind::Binary),
        ty if !ty.is_nested() => {
            creation.work(1)?;
            (ty.clone(), ValueKind::Scalar(ty.clone()))
        }
        _ => return Err(bad()),
    };
    creation.request(ValueTypeRef::storage_allocation_layout().map_err(|_| extent())?)?;
    creation.work(1)?;
    let value = ValueTypeRef::try_new(kind, nullable, creation.storage()).map_err(|_| bad())?;
    Ok((layout, value))
}
pub(super) fn value(
    ty: &DataType,
    nullable: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<FunctionValueType, Failure> {
    let (layout, value) = declaration(ty, nullable, 1, &mut 0, creation)?;
    creation.request(ValueLayout::storage_allocation_layout().map_err(|_| extent())?)?;
    let layout =
        ValueLayout::try_from_normalized_arrow_observed(layout, creation.storage(), |units| {
            creation.work(units).map_err(LayoutError::Caller)
        })
        .map_err(LayoutError::failure)?;
    FunctionValueType::try_from_value(value, layout).map_err(|_| bad())
}
pub(super) fn schema(
    columns: &[MetadataOutputColumn],
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadExecutionSchema, Failure> {
    let mut fields = creation.reserve(columns.len())?;
    let mut values = creation.reserve(columns.len())?;
    for column in columns {
        let value = match column.temporal_declaration() {
            Some(source) => {
                crate::schema_mapping::typed_value::from_declaration(
                    source,
                    column.nullable(),
                    None,
                    None,
                    creation,
                )?
                .binding
            }
            None => column.value_type().clone_observed(|| creation.work(1))?,
        };
        let name = creation.text(column.name())?;
        fields.push(creation.arc(Field::new(
            name,
            value.data_type().clone(),
            value.nullable(),
        ))?);
        values.push(value);
    }
    let fields = creation.arrow_fields(fields)?;
    let schema = creation.arc(Schema::new(fields))?;
    let storage = creation.storage();
    let (request, work) = creation.observers();
    ConnectorReadExecutionSchema::try_new_observed(
        schema,
        values.into_boxed_slice(),
        storage,
        |layout| request(layout).map_err(Failure::Control),
        |units| work(units).map_err(Failure::Control),
    )
}
