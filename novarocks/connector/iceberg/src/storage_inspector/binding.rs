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

//! Resolves MV source paths against the table observation that authored SQL's schema.

use std::collections::BTreeMap;
use std::mem::size_of;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorOperationControl, ConnectorProviderBindingKey, ConnectorRequestContext,
    ConnectorTableMetadata, MvSourceBindingFact, MvSourceBindingObservation, MvSourceFieldBinding,
    MvSourceFieldKind, MvSourceFieldPath, MvSourceObjectKind, MvSourcePathBinding,
};
use novarocks_type_contract::CanonicalFactWriter;

use crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::{
    IcebergObservedSourceReadFacts, frozen_current_source_schema,
};
use crate::iceberg::spec::{
    Literal, NestedField, Schema, StructType, TableMetadata, TableProperties, Type,
};
use crate::metadata::TableMetadataProjection;
use crate::scalar_integer_domain::ScalarIntegerDomain;
use crate::schema_mapping::nominal_declaration::{
    NominalSourceDomain, TableNominalDocument, is_retired_read_field,
};
use crate::source_type::project_source_type;

use super::{
    IcebergStorageInspector, corrupt, exact_schema_version, iceberg_object_id_from_uuid, invalid,
    reserve_bytes,
};

impl IcebergStorageInspector {
    pub fn observe_source_binding(
        &self,
        exact_lease: &ConnectorControlPlanningPin,
        metadata: &ConnectorTableMetadata,
        paths: &[MvSourceFieldPath],
        context: ConnectorRequestContext,
    ) -> Result<MvSourceBindingObservation, ConnectorError> {
        context.original_operation_control().check_active()?;
        let owner = ConnectorProviderBindingKey {
            instance_id: exact_lease.binding().descriptor().instance_id.clone(),
            incarnation: exact_lease.binding().incarnation(),
        };
        if metadata.identity.instance_id != owner.instance_id
            || metadata.table.owner() != &owner.instance_id
        {
            return Err(invalid("MV source binding belongs to another connector"));
        }
        let original = metadata.source_read_facts().ok_or_else(|| {
            unsupported("MV source binding requires the original Iceberg table observation")
        })?;
        original.validate_owner(&owner)?;
        let original =
            original.value::<IcebergObservedSourceReadFacts>(owner.instance_id.as_str())?;
        let identifier = original.access.identifier();
        if identifier.namespace().to_url_string() != metadata.identity.namespace.as_ref()
            || identifier.name() != metadata.identity.table.as_ref()
        {
            return Err(invalid(
                "MV source binding does not identify the observed Iceberg base table",
            ));
        }
        let table = original.access.metadata();
        let schema = match original.projection {
            TableMetadataProjection::Current => table.current_schema().clone(),
            TableMetadataProjection::SnapshotTo(snapshot) => {
                crate::change_planning::resolved_window_schema(table, snapshot)?
            }
            TableMetadataProjection::CurrentSnapshotTo(snapshot) => {
                frozen_current_source_schema(table, snapshot)?
            }
        };
        if metadata.version.as_ref() != Some(&exact_schema_version(schema.schema_id())) {
            return Err(corrupt(
                "MV source binding differs from its observed Iceberg schema",
            ));
        }

        project_source_binding(owner.instance_id, table, &schema, paths, &context)
    }
}

struct ResolvedPath<'a> {
    fields: Vec<&'a NestedField>,
    ids: Vec<i32>,
}

impl<'a> ResolvedPath<'a> {
    fn leaf(&self) -> &'a NestedField {
        self.fields[self.fields.len() - 1]
    }
}

fn project_source_binding(
    catalog_instance: ConnectorInstanceId,
    table: &TableMetadata,
    schema: &Schema,
    paths: &[MvSourceFieldPath],
    context: &ConnectorRequestContext,
) -> Result<MvSourceBindingObservation, ConnectorError> {
    let control = context.original_operation_control();
    control.check_active()?;
    let mut budget = 0;
    let object_id = iceberg_object_id_from_uuid(table.uuid().to_string())?;
    reserve_bytes(context, &mut budget, object_id.as_bytes().len())?;
    let managed = crate::document_storage::keyed::view::management_marker(table)
        .map_err(crate::document_storage::observation::keyed_read_error)?;
    let object_kind = if managed
        .as_ref()
        .is_some_and(|marker| marker.kind() == "materialized-view")
    {
        MvSourceObjectKind::MaterializedView
    } else {
        MvSourceObjectKind::Table
    };
    let mut object = writer("novarocks.iceberg.mv-source-object");
    object.text(catalog_instance.as_str());
    object.bytes(object_id.as_bytes());
    object.u32(match object_kind {
        MvSourceObjectKind::Table => 1,
        MvSourceObjectKind::MaterializedView => 2,
    });

    let mut resolved = Vec::new();
    let mut bindings = Vec::new();
    let mut by_ids = BTreeMap::new();
    for path in paths {
        let selected = resolve_path(schema.as_struct(), path, &control, context, &mut budget)?;
        let leaf = selected.leaf();
        let leaf_kind = field_kind(&leaf.field_type);
        if leaf_kind != MvSourceFieldKind::Scalar && !path.whole_subtree {
            return Err(invalid(
                "MV source container access must name a whole subtree",
            ));
        }
        let leaf_type = project_source_type(&leaf.field_type, &control, |bytes| {
            reserve_bytes(context, &mut budget, bytes)
        })?;
        let fields = selected
            .fields
            .iter()
            .map(|field| MvSourceFieldBinding {
                provider_field_id: Bytes::copy_from_slice(&field.id.to_be_bytes()),
                required: field.required,
                kind: field_kind(&field.field_type),
            })
            .collect();
        by_ids.insert(selected.ids.clone(), resolved.len());
        resolved.push(selected);
        bindings.push(MvSourcePathBinding {
            fields,
            leaf_type,
            whole_subtree: leaf_kind != MvSourceFieldKind::Scalar,
        });
    }

    let mut field_paths = writer("novarocks.iceberg.mv-source-paths");
    let mut source_types = writer("novarocks.iceberg.mv-source-types");
    field_paths.sequence(by_ids.len());
    source_types.sequence(by_ids.len());
    for index in by_ids.values().copied() {
        control.check_active()?;
        let path = &resolved[index];
        let binding = &bindings[index];
        write_ids(&mut field_paths, &path.ids);
        write_ids(&mut source_types, &path.ids);
        source_types.boolean(binding.whole_subtree);
        for field in &binding.fields {
            source_types.boolean(field.required);
            source_types.u32(match field.kind {
                MvSourceFieldKind::Scalar => 1,
                MvSourceFieldKind::Struct => 2,
                MvSourceFieldKind::List => 3,
                MvSourceFieldKind::Map => 4,
            });
        }
        let bytes = binding.leaf_type.canonical_bytes();
        reserve_bytes(context, &mut budget, bytes.len())?;
        source_types.bytes(&bytes);
    }

    let declarations = read_declarations(
        table,
        by_ids.values().map(|index| &resolved[*index]),
        &control,
        context,
        &mut budget,
    )?;
    let mut finish = |writer: CanonicalFactWriter| {
        let bytes = writer.finish();
        reserve_bytes(context, &mut budget, bytes.len())?;
        Ok::<_, ConnectorError>(MvSourceBindingFact::from_canonical_bytes(bytes))
    };

    Ok(MvSourceBindingObservation {
        catalog_instance,
        object_id,
        object_kind,
        paths: bindings,
        object_fact: finish(object)?,
        path_fact: finish(field_paths)?,
        type_fact: finish(source_types)?,
        read_declarations_fact: finish(declarations)?,
    })
}

fn resolve_path<'a>(
    root: &'a StructType,
    path: &MvSourceFieldPath,
    control: &dyn ConnectorOperationControl,
    context: &ConnectorRequestContext,
    budget: &mut usize,
) -> Result<ResolvedPath<'a>, ConnectorError> {
    let Some((leaf, ancestors)) = path.fields.split_last() else {
        return Err(invalid("MV source field path is empty"));
    };
    reserve_bytes(context, budget, size_of::<MvSourcePathBinding>())?;
    let mut fields = Vec::new();
    let mut ids = Vec::new();
    let mut structure = root;
    for coordinate in ancestors.iter().chain(std::iter::once(leaf)) {
        control.check_active()?;
        // One retained header and its encoded ID, plus the private lookup IDs.
        reserve_bytes(
            context,
            budget,
            size_of::<MvSourceFieldBinding>() + size_of::<i32>() * 3,
        )?;
        let ordinal = usize::try_from(coordinate.ordinal)
            .map_err(|_| invalid("MV source field ordinal cannot index the observed schema"))?;
        let field = structure
            .fields()
            .get(ordinal)
            .filter(|field| field.name == coordinate.name)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "MV source field path does not match the original Iceberg schema",
                )
            })?;
        fields.push(field.as_ref());
        ids.push(field.id);
        if fields.len() < path.fields.len() {
            let Type::Struct(children) = field.field_type.as_ref() else {
                return Err(unsupported("MV source paths descend only through structs"));
            };
            structure = children;
        }
    }
    Ok(ResolvedPath { fields, ids })
}

fn field_kind(kind: &Type) -> MvSourceFieldKind {
    match kind {
        Type::Primitive(_) | Type::Variant(_) => MvSourceFieldKind::Scalar,
        Type::Struct(_) => MvSourceFieldKind::Struct,
        Type::List(_) => MvSourceFieldKind::List,
        Type::Map(_) => MvSourceFieldKind::Map,
    }
}

fn writer(domain: &'static str) -> CanonicalFactWriter {
    CanonicalFactWriter::new(domain, 1)
}

fn write_ids(writer: &mut CanonicalFactWriter, ids: &[i32]) {
    writer.sequence(ids.len());
    for id in ids {
        writer.i32(*id);
    }
}

fn read_declarations<'a>(
    table: &TableMetadata,
    paths: impl Iterator<Item = &'a ResolvedPath<'a>> + Clone,
    control: &dyn ConnectorOperationControl,
    context: &ConnectorRequestContext,
    budget: &mut usize,
) -> Result<CanonicalFactWriter, ConnectorError> {
    let mut consumed = BTreeMap::new();
    for path in paths.clone() {
        add_consumed_subtree(path.leaf(), &mut consumed, control, context, budget)?;
    }

    let mut fact = writer("novarocks.iceberg.mv-source-read-declarations");
    let name_mapping = table
        .properties()
        .get(TableProperties::PROPERTY_DEFAULT_NAME_MAPPING);
    fact.boolean(name_mapping.is_some());
    if let Some(name_mapping) = name_mapping {
        control.check_active()?;
        reserve_bytes(context, budget, name_mapping.len())?;
        let canonical =
            crate::schema_mapping::canonical_name_mapping(name_mapping).map_err(corrupt)?;
        fact.text(&canonical);
    }
    let declarations = if consumed.is_empty() {
        TableNominalDocument::default()
    } else {
        TableNominalDocument::from_metadata_checked(table, &|_| control.check_active())?
    };
    let fields = consumed.iter().filter(|(id, field)| {
        declarations.nominals.contains_key(*id)
            || declarations.integers.contains_key(*id)
            || field.initial_default.is_some()
    });
    fact.sequence(fields.clone().count());
    for (id, field) in fields {
        control.check_active()?;
        fact.i32(*id);
        fact.u32(match declarations.nominals.get(id) {
            None => 0,
            Some(NominalSourceDomain::Hll) => 1,
            Some(NominalSourceDomain::Bitmap) => 2,
            Some(NominalSourceDomain::LargeInt) => 3,
        });
        fact.u32(match declarations.integers.get(id) {
            None => 0,
            Some(ScalarIntegerDomain::Int8) => 1,
            Some(ScalarIntegerDomain::Int16) => 2,
        });
        fact.boolean(is_retired_read_field(*id, &declarations.nominals));
        write_typed_default(
            &mut fact,
            &field.field_type,
            field.initial_default.as_ref(),
            context,
            budget,
        )?;
    }

    // When an enclosing struct is absent from an old file its initial
    // default also supplies the selected leaf. Other sibling values in that
    // default do not participate unless the whole enclosing struct is used.
    let ancestor_defaults = paths.clone().flat_map(|path| {
        path.fields[..path.fields.len() - 1]
            .iter()
            .enumerate()
            .filter(|(_, field)| field.initial_default.is_some())
            .map(move |(index, field)| (path, index, *field))
    });
    fact.sequence(ancestor_defaults.clone().count());
    for (path, index, field) in ancestor_defaults {
        control.check_active()?;
        write_ids(&mut fact, &path.ids);
        fact.i32(field.id);
        let literal = project_ancestor_default(field, &path.fields[index + 1..], control)?;
        write_typed_default(&mut fact, &path.leaf().field_type, literal, context, budget)?;
    }
    Ok(fact)
}

fn add_consumed_subtree<'a>(
    field: &'a NestedField,
    consumed: &mut BTreeMap<i32, &'a NestedField>,
    control: &dyn ConnectorOperationControl,
    context: &ConnectorRequestContext,
    budget: &mut usize,
) -> Result<(), ConnectorError> {
    control.check_active()?;
    if consumed.insert(field.id, field).is_some() {
        return Ok(());
    }
    reserve_bytes(context, budget, size_of::<(i32, &NestedField)>())?;
    match field.field_type.as_ref() {
        Type::Struct(children) => {
            for child in children.fields() {
                add_consumed_subtree(child, consumed, control, context, budget)?;
            }
        }
        Type::List(list) => {
            add_consumed_subtree(&list.element_field, consumed, control, context, budget)?;
        }
        Type::Map(map) => {
            add_consumed_subtree(&map.key_field, consumed, control, context, budget)?;
            add_consumed_subtree(&map.value_field, consumed, control, context, budget)?;
        }
        Type::Primitive(_) | Type::Variant(_) => {}
    }
    Ok(())
}

fn project_ancestor_default<'a>(
    mut field: &'a NestedField,
    descendants: &[&'a NestedField],
    control: &dyn ConnectorOperationControl,
) -> Result<Option<&'a Literal>, ConnectorError> {
    let mut value = field.initial_default.as_ref();
    for next in descendants {
        control.check_active()?;
        let (Some(Literal::Struct(record)), Type::Struct(structure)) =
            (value, field.field_type.as_ref())
        else {
            return if value.is_none() {
                Ok(None)
            } else {
                Err(corrupt(
                    "MV source struct initial default differs from its declared type",
                ))
            };
        };
        let ordinal = structure
            .fields()
            .iter()
            .position(|member| member.id == next.id)
            .ok_or_else(|| corrupt("MV source initial default path left the declared struct"))?;
        value = record
            .fields()
            .get(ordinal)
            .ok_or_else(|| corrupt("MV source struct initial default has a missing field"))?
            .as_ref();
        field = next;
    }
    Ok(value)
}

fn write_typed_default(
    fact: &mut CanonicalFactWriter,
    field_type: &Type,
    literal: Option<&Literal>,
    context: &ConnectorRequestContext,
    budget: &mut usize,
) -> Result<(), ConnectorError> {
    // The actual field type is independently sealed in A3. Use Iceberg's
    // typed literal encoder for A4, with field IDs and JSON values only.
    let control = context.original_operation_control();
    fact.boolean(literal.is_some());
    if let Some(literal) = literal {
        control.check_active()?;
        let value = literal
            .clone()
            .try_into_json(field_type)
            .map_err(|cause| corrupt(format!("encode typed MV source initial default: {cause}")))?;
        if value.is_null() {
            return Err(unsupported(
                "MV source initial default has no Iceberg JSON value",
            ));
        }
        let encoded = serde_json::to_vec(&value)
            .map_err(|cause| corrupt(format!("encode MV source initial default JSON: {cause}")))?;
        reserve_bytes(context, budget, encoded.len())?;
        fact.bytes(&encoded);
    }
    Ok(())
}

fn unsupported(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests;
