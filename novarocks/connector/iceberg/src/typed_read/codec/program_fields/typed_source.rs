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

//! Provider-authored complete public read facts, retaining exact creators.

use super::*;
use crate::iceberg::spec::{PrimitiveType, Type};
use novarocks_connector_contract::{
    ConnectorReadExecutionSchema, ConnectorSourceFieldFacts, ConnectorSourceInitialDefault,
    ConnectorSourceSchemaFacts, ConnectorSourceSchemaShape, MAX_CONNECTOR_RECIPE_COLUMNS,
};
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use std::{alloc::Layout, collections::HashMap};

type CreationFailure = PureProviderCompileError<ConnectorError>;
fn invalid_creation(message: &'static str) -> CreationFailure {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message).into()
}
fn resource_creation() -> CreationFailure {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Iceberg public source construction exceeds its finite source bounds",
    )
    .into()
}

pub(super) fn publish(
    relation: &IcebergRuntimeRelation,
    columns: &[&IcebergColumnHandle],
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadPublicSchema, CreationFailure> {
    creation.work(1)?;
    if columns.len() > MAX_CONNECTOR_RECIPE_COLUMNS {
        return Err(resource_creation());
    }
    let (schema, nominal, table, source_bytes) = match relation {
        IcebergRuntimeRelation::Table(table) => {
            creation.work(table.table_schema_json().len())?;
            let schema = table.parse_table_schema()?;
            creation.work(1)?;
            (
                Some(schema),
                Some(table.nominal_declarations()),
                Some(table),
                table.table_schema_json().len(),
            )
        }
        IcebergRuntimeRelation::ChangeWindow(window) => {
            creation.work(window.table_schema_json().len())?;
            let schema = window.parse_table_schema()?;
            creation.work(1)?;
            (
                Some(schema),
                Some(window.nominal_declarations()),
                None,
                window.table_schema_json().len(),
            )
        }
        IcebergRuntimeRelation::TableExecute(execute) => match execute.procedure_handle() {
            Some(IcebergTableExecuteProcedureHandle::Optimize(optimize)) => {
                let table = optimize.table_handle();
                creation.work(table.table_schema_json().len())?;
                let schema = table.parse_table_schema()?;
                creation.work(1)?;
                (
                    Some(schema),
                    Some(table.nominal_declarations()),
                    Some(table),
                    table.table_schema_json().len(),
                )
            }
            Some(IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(_)) => {
                (None, None, None, 0)
            }
            None => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "Iceberg table execute has no source declaration",
                )
                .into());
            }
        },
        _ => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg relation needs its own complete source author",
            )
            .into());
        }
    };
    let storage = creation.storage();
    let mut source_fields = creation.reserve::<Arc<Field>>(columns.len())?;
    let mut execution_fields = creation.reserve::<Arc<Field>>(columns.len())?;
    let mut bindings = creation.reserve::<FunctionValueType>(columns.len())?;
    let mut facts_count = 0usize;
    for column in columns {
        let field = resolve(schema.as_ref(), column, creation)?;
        facts_count = facts_count
            .checked_add(count_fields(
                field.map(|f| f.field_type.as_ref()),
                1,
                creation,
            )?)
            .ok_or_else(resource_creation)?;
    }
    let mut facts = creation.reserve::<ConnectorSourceFieldFacts>(facts_count)?;
    let mut path = creation.reserve::<u32>(novarocks_type_contract::MAX_VALUE_TYPE_DEPTH + 1)?;
    for (ordinal, column) in columns.iter().enumerate() {
        let source = resolve(schema.as_ref(), column, creation)?;
        let owned;
        let source = match source {
            Some(source) => source,
            None => {
                creation.work(column.type_json().len())?;
                let ty = parse_type(column.type_json(), "type_json")?;
                creation.work(1)?;
                let name = creation.text(column.base_column_identity().name())?;
                creation.request(Layout::new::<Type>())?;
                creation.work(1)?;
                owned = NestedField {
                    id: column.base_field_id(),
                    name,
                    required: !column.nullable(),
                    field_type: Box::new(ty),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                };
                &owned
            }
        };
        validate_column_declaration(source, column, schema.as_ref(), table, relation, creation)?;
        let empty = Default::default();
        let declared = crate::schema_mapping::typed_value::from_field(
            source,
            column.nullable(),
            nominal.unwrap_or(&empty),
            column.scalar_integer_domain(),
            creation,
        )?;
        let source_name = creation.text(&source.name)?;
        source_fields.push(creation.arc(Field::new(
            source_name,
            declared.source_layout,
            column.nullable(),
        ))?);
        let execution_name = creation.text(&source.name)?;
        execution_fields.push(creation.arc(Field::new(
            execution_name,
            declared.binding.data_type().clone(),
            column.nullable(),
        ))?);
        bindings.push(declared.binding);
        path.clear();
        path.push(u32::try_from(ordinal).map_err(|_| resource_creation())?);
        append_facts(source, &mut path, &mut facts, source_bytes, creation)?;
    }
    let source_fields = creation.arrow_fields(source_fields)?;
    let source_schema = creation.arc(arrow::datatypes::Schema::new(source_fields))?;
    let shape =
        ConnectorSourceSchemaShape::try_new_observed(source_schema, storage.clone(), |units| {
            creation.work(units)
        })?;
    let (request, work) = creation.observers();
    let source = ConnectorSourceSchemaFacts::try_new_observed(
        shape,
        HashMap::new(),
        facts.into_boxed_slice(),
        storage.clone(),
        |layout| request(layout).map_err(CreationFailure::Control),
        |units| work(units).map_err(CreationFailure::Control),
    )?;
    let fields = creation.arrow_fields(execution_fields)?;
    let schema = creation.arc(arrow::datatypes::Schema::new(fields))?;
    let (request, work) = creation.observers();
    let execution = ConnectorReadExecutionSchema::try_new_observed(
        schema,
        bindings.into_boxed_slice(),
        storage.clone(),
        |layout| request(layout).map_err(CreationFailure::Control),
        |units| work(units).map_err(CreationFailure::Control),
    )?;
    let (request, work) = creation.observers();
    ConnectorReadPublicSchema::try_new_typed_observed(
        &execution,
        &source,
        storage,
        |layout| request(layout).map_err(CreationFailure::Control),
        |units| work(units).map_err(CreationFailure::Control),
    )
}

fn resolve<'a>(
    schema: Option<&'a Schema>,
    column: &IcebergColumnHandle,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Option<&'a NestedField>, CreationFailure> {
    if IcebergMetadataColumn::from_field_id(column.base_field_id()).is_some()
        || column.base_field_id() == ICEBERG_CHANGE_OP_FIELD_ID
        || schema.is_none()
    {
        creation.work(1)?;
        return Ok(None);
    }
    let schema = schema.ok_or_else(|| invalid_creation("Iceberg source schema is absent"))?;
    let mut current = None;
    for field in schema.as_struct().fields() {
        creation.work(1)?;
        if field.id == column.base_field_id() {
            current = Some(field.as_ref());
            break;
        }
    }
    let mut field = current
        .ok_or_else(|| invalid_creation("Iceberg read ID is absent from its frozen schema"))?;
    for id in column.field_id_path() {
        let Type::Struct(children) = field.field_type.as_ref() else {
            return Err(invalid_creation(
                "Iceberg read dereference requires an authoritative struct",
            ));
        };
        let mut child = None;
        for candidate in children.fields() {
            creation.work(1)?;
            if candidate.id == *id {
                child = Some(candidate.as_ref());
                break;
            }
        }
        field = child.ok_or_else(|| {
            invalid_creation("Iceberg read dereference is absent from its frozen source")
        })?;
    }
    Ok(Some(field))
}

fn count_fields(
    ty: Option<&Type>,
    depth: usize,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<usize, CreationFailure> {
    creation.work(1)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(resource_creation());
    }
    let mut count = 1usize;
    match ty {
        Some(Type::Struct(fields)) => {
            for field in fields.fields() {
                count = count
                    .checked_add(count_fields(Some(&field.field_type), depth + 1, creation)?)
                    .ok_or_else(resource_creation)?;
            }
        }
        Some(Type::List(list)) => {
            count += count_fields(Some(&list.element_field.field_type), depth + 1, creation)?
        }
        Some(Type::Map(map)) => {
            count += count_fields(Some(&map.key_field.field_type), depth + 2, creation)?;
            count += count_fields(Some(&map.value_field.field_type), depth + 2, creation)?;
        }
        _ => {}
    }
    if count > novarocks_type_contract::MAX_VALUE_TYPE_NODES {
        return Err(resource_creation());
    }
    Ok(count)
}

fn validate_column_declaration(
    source: &NestedField,
    column: &IcebergColumnHandle,
    schema: Option<&Schema>,
    table: Option<&IcebergTableHandle>,
    relation: &IcebergRuntimeRelation,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), CreationFailure> {
    if let Some(metadata) = IcebergMetadataColumn::from_field_id(column.base_field_id()) {
        let rewrite_name = if matches!(relation, IcebergRuntimeRelation::TableExecute(execute)
            if matches!(execute.procedure_handle(), Some(IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(_))))
        {
            REWRITE_POSITION_DELETE_OUTPUT_COLUMNS
                .iter()
                .find_map(|(name, expected)| (*expected == metadata).then_some(*name))
        } else {
            Some(metadata.column_name())
        };
        if !column.is_base_column()
            || rewrite_name != Some(column.base_column_identity().name())
            || source.field_type.as_ref() != &Type::Primitive(metadata.declared_type())
            || metadata == IcebergMetadataColumn::IsDeleted
        {
            return Err(invalid_creation(
                "Iceberg metadata column differs from its real source author",
            ));
        }
        if let Some(cow) = table.and_then(IcebergTableHandle::frozen_cow_source) {
            let mut found = false;
            for expected in cow.metadata_columns() {
                creation.work(1)?;
                if expected == column {
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(invalid_creation(
                    "Iceberg metadata column differs from its original COW source",
                ));
            }
        }
    } else if source.id == ICEBERG_CHANGE_OP_FIELD_ID {
        if !matches!(relation, IcebergRuntimeRelation::ChangeWindow(_))
            || !column.is_base_column()
            || column.base_column_identity().name()
                != crate::typed_read::change_window::ICEBERG_CHANGE_OP_COLUMN
            || column.nullable()
            || source.field_type.as_ref() != &Type::Primitive(PrimitiveType::Int)
        {
            return Err(invalid_creation(
                "Iceberg change operation column differs from its explicit source",
            ));
        }
    } else if let Some(schema) = schema {
        let mut root = None;
        for field in schema.as_struct().fields() {
            creation.work(1)?;
            if field.id == column.base_field_id() {
                root = Some(field.as_ref());
                break;
            }
        }
        let mut field =
            root.ok_or_else(|| invalid_creation("Iceberg column has no root source identity"))?;
        let base_field = field;
        validate_identity(field, column.base_column_identity(), creation)?;
        let mut nullable = !field.required;
        for id in column.field_id_path() {
            let Type::Struct(fields) = field.field_type.as_ref() else {
                return Err(invalid_creation(
                    "Iceberg source projection has a non-struct ancestor",
                ));
            };
            let mut child = None;
            for candidate in fields.fields() {
                creation.work(1)?;
                if candidate.id == *id {
                    child = Some(candidate.as_ref());
                    break;
                }
            }
            field =
                child.ok_or_else(|| invalid_creation("Iceberg source projection ID is absent"))?;
            nullable |= !field.required;
        }
        creation.work(1)?;
        if column.nullable() != nullable {
            return Err(invalid_creation(
                "Iceberg column NULL contract differs from its full source path",
            ));
        }
        creation.work(column.base_type_json().len())?;
        let base = parse_type(column.base_type_json(), "base_type_json")?;
        creation.work(1)?;
        if &base != base_field.field_type.as_ref() {
            return Err(invalid_creation(
                "Iceberg base declaration differs from its authoritative source",
            ));
        }
    }
    if let IcebergRuntimeRelation::ChangeWindow(window) = relation {
        let mut found = source.id == ICEBERG_CHANGE_OP_FIELD_ID;
        for expected in window.columns() {
            creation.work(1)?;
            if expected == column {
                found = true;
                break;
            }
        }
        if !found {
            return Err(invalid_creation(
                "Iceberg column differs from its exact frozen change window",
            ));
        }
    }
    creation.work(column.type_json().len())?;
    let declared = parse_type(column.type_json(), "type_json")?;
    creation.work(1)?;
    if &declared != source.field_type.as_ref() {
        return Err(invalid_creation(
            "Iceberg private type differs from its authoritative source",
        ));
    }
    if let Some(table) = table {
        let expected = table
            .scalar_integer_domains()
            .get(&column.base_field_id())
            .copied();
        if column.scalar_integer_domain() != expected {
            return Err(invalid_creation(
                "Iceberg integer source declaration was not preserved",
            ));
        }
    }
    Ok(())
}

fn validate_identity(
    source: &NestedField,
    identity: &ColumnIdentity,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), CreationFailure> {
    creation.work(source.name.len())?;
    creation.work(1)?;
    let kind = match source.field_type.as_ref() {
        Type::Primitive(_) | Type::Variant(_) => {
            crate::typed_read::ColumnIdentityCategory::Primitive
        }
        Type::Struct(_) => crate::typed_read::ColumnIdentityCategory::Struct,
        Type::List(_) => crate::typed_read::ColumnIdentityCategory::Array,
        Type::Map(_) => crate::typed_read::ColumnIdentityCategory::Map,
    };
    if identity.field_id() != source.id
        || identity.name() != source.name
        || identity.category() != kind
    {
        return Err(invalid_creation(
            "Iceberg column identity differs from its explicit source tree",
        ));
    }
    match source.field_type.as_ref() {
        Type::Primitive(_) | Type::Variant(_) => {
            if !identity.children().is_empty() {
                return Err(invalid_creation(
                    "Iceberg primitive identity has source children",
                ));
            }
        }
        Type::Struct(fields) => {
            if identity.children().len() != fields.fields().len() {
                return Err(invalid_creation("Iceberg identity child counts differ"));
            }
            for (field, identity) in fields.fields().iter().zip(identity.children()) {
                validate_identity(field, identity, creation)?;
            }
        }
        Type::List(list) => {
            if identity.children().len() != 1 {
                return Err(invalid_creation(
                    "Iceberg list identity child count differs",
                ));
            }
            validate_identity(&list.element_field, &identity.children()[0], creation)?;
        }
        Type::Map(map) => {
            if identity.children().len() != 2 {
                return Err(invalid_creation("Iceberg map identity child counts differ"));
            }
            validate_identity(&map.key_field, &identity.children()[0], creation)?;
            validate_identity(&map.value_field, &identity.children()[1], creation)?;
        }
    }
    Ok(())
}

fn append_facts(
    source: &NestedField,
    path: &mut Vec<u32>,
    facts: &mut Vec<ConnectorSourceFieldFacts>,
    source_bytes: usize,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), CreationFailure> {
    creation.work(1)?;
    let storage = creation.storage();
    let mut owned_path = creation.reserve::<u32>(path.len())?;
    creation.work(std::mem::size_of_val(path.as_slice()))?;
    owned_path.extend_from_slice(path);
    let initial_default = if let Some(default) = &source.initial_default {
        // The SDK literal codec is an original finite source operation. Caller
        // control observes its input bound before entry and its exit afterwards.
        creation.work(source_bytes)?;
        let value = default
            .clone()
            .try_into_json(&source.field_type)
            .map_err(|_| {
                invalid_creation("Iceberg authoritative initial default cannot be encoded")
            })?;
        let json = value.to_string();
        creation.work(1)?;
        if json.len() > novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES {
            return Err(resource_creation());
        }
        let payload = Layout::array::<u8>(json.len()).map_err(|_| resource_creation())?;
        creation.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(payload)
                .map_err(|_| resource_creation())?,
        )?;
        creation.work(json.len())?;
        Some(ConnectorSourceInitialDefault::json(
            Arc::<str>::from(json.as_str()),
            storage.clone(),
        ))
    } else {
        None
    };
    let (attrs, attrs_retained) = source_attributes(source, creation)?;
    let (request, work) = creation.observers();
    let fact = ConnectorSourceFieldFacts::try_new_observed(
        owned_path.into_boxed_slice(),
        Some(source.id),
        initial_default,
        attrs,
        &[PARQUET_FIELD_ID_META_KEY, ICEBERG_INITIAL_DEFAULT_META_KEY],
        storage,
        attrs_retained,
        |layout| request(layout).map_err(CreationFailure::Control),
        |units| work(units).map_err(CreationFailure::Control),
    )?;
    facts.push(fact);
    match source.field_type.as_ref() {
        Type::Struct(fields) => {
            for (ordinal, field) in fields.fields().iter().enumerate() {
                path.push(u32::try_from(ordinal).map_err(|_| resource_creation())?);
                append_facts(field, path, facts, source_bytes, creation)?;
                path.pop();
            }
        }
        Type::List(list) => {
            path.push(0);
            append_facts(&list.element_field, path, facts, source_bytes, creation)?;
            path.pop();
        }
        Type::Map(map) => {
            path.push(0);
            for (ordinal, field) in [&map.key_field, &map.value_field].into_iter().enumerate() {
                path.push(ordinal as u32);
                append_facts(field, path, facts, source_bytes, creation)?;
                path.pop();
            }
            path.pop();
        }
        _ => {}
    }
    Ok(())
}

fn source_attributes(
    source: &NestedField,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(HashMap<String, String>, usize), CreationFailure> {
    let mut input = [None, None, None];
    let mut count = 0;
    if let Some(doc) = &source.doc {
        input[count] = Some(("doc", doc.as_str()));
        count += 1;
    }
    if matches!(source.field_type.as_ref(), Type::Variant(_)) {
        input[count] = Some(("ARROW:extension:name", "arrow.parquet.variant"));
        count += 1;
        // The pinned Iceberg SDK's VariantExtensionType serializes no metadata.
        // Preserve that actual source absence instead of fabricating an empty key.
    }
    let mut attrs = HashMap::new();
    let layout = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
        String,
        String,
    >(count)
    .map_err(|_| resource_creation())?;
    let mut retained = layout.layout.map_or(0, |layout| layout.size());
    if let Some(layout) = layout.layout {
        creation.request(layout)?;
    }
    let key_bytes = input.iter().flatten().map(|(key, _)| key.len()).sum();
    let max_key = input
        .iter()
        .flatten()
        .map(|(key, _)| key.len())
        .max()
        .unwrap_or(0);
    creation.work(
        novarocks_type_contract::owned_resources::hashmap::fresh_string_table_work_upper_bound(
            count, key_bytes, max_key,
        )
        .map_err(|_| resource_creation())?,
    )?;
    attrs.try_reserve(count).map_err(|_| resource_creation())?;
    for (key, value) in input.into_iter().flatten() {
        if value.len() > novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES {
            return Err(resource_creation());
        }
        retained = retained
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or_else(resource_creation)?;
        let key = creation.text(key)?;
        let value = creation.text(value)?;
        creation.work(1)?;
        attrs.insert(key, value);
    }
    Ok((attrs, retained))
}
