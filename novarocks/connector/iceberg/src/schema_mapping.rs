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

//! Provider-owned Parquet field-identity and Iceberg name-mapping helpers.

use std::collections::HashSet;
use std::sync::Arc;

type ListFieldWrapper = Box<dyn FnOnce(Arc<Field>) -> DataType>;

pub(crate) mod nominal_declaration;
pub(crate) mod typed_value;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
pub use novarocks_types::value::variant::is_variant_struct_data_type;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY;
use crate::iceberg::spec::{MappedField, NameMapping};
use crate::scan_model::{IcebergSchemaDef, IcebergSchemaFieldDef};

/// Convert one frozen Iceberg schema into the SQL read carrier exposed by the
/// Provider. iceberg-rust's generic Arrow mapping intentionally chooses wider
/// physical representations for several primitives; NovaRocks binds these
/// exact logical carriers at admission and must reproduce them at begin-scan.
pub fn sql_read_schema_from_iceberg(
    iceberg_schema: &crate::iceberg::spec::Schema,
) -> Result<SchemaRef, String> {
    let arrow_schema = crate::iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
        .map_err(|error| format!("convert Iceberg schema to Arrow: {error}"))?;
    if arrow_schema.fields().len() != iceberg_schema.as_struct().fields().len() {
        return Err("Iceberg schema field count does not match its Arrow carrier".to_string());
    }
    let fields = arrow_schema
        .fields()
        .iter()
        .zip(iceberg_schema.as_struct().fields())
        .map(|(field, iceberg_field)| sql_read_field(field.as_ref(), iceberg_field).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        arrow_schema.metadata().clone(),
    )))
}

fn sql_read_field(
    field: &Field,
    iceberg_field: &crate::iceberg::spec::NestedField,
) -> Result<Field, String> {
    use crate::iceberg::spec::{PrimitiveType, Type};
    use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType};

    // Storage authors the domain. Equal-width FIXED and UUID, or a binary
    // carrier and VARIANT, do not confer the same semantic identity.
    let logical = match iceberg_field.field_type.as_ref() {
        Type::Primitive(PrimitiveType::Uuid) => ValueLogicalType::Uuid,
        Type::Variant(_) => ValueLogicalType::Variant,
        _ => ValueLogicalType::Physical,
    };
    if let Some(label) = field.metadata().get(NR_LOGICAL_TYPE_KEY) {
        let declared = ValueLogicalType::from_metadata_value(label).map_err(|error| {
            format!(
                "Iceberg field {} has invalid logical metadata: {error}",
                field.name()
            )
        })?;
        if declared != logical {
            return Err(format!(
                "Iceberg field {} logical metadata conflicts with its storage type",
                field.name()
            ));
        }
    }
    let data_type = sql_read_data_type(
        field.data_type(),
        iceberg_field.field_type.as_ref(),
        field.name(),
    )?;
    logical.validate_carrier(&data_type).map_err(|error| {
        format!(
            "Iceberg field {} has incompatible logical carrier: {error}",
            field.name()
        )
    })?;
    let mut metadata = field.metadata().clone();
    if let Some(label) = logical.metadata_value() {
        metadata.insert(NR_LOGICAL_TYPE_KEY, label);
    }
    // Clone the actual field, including provider annotations and dictionary
    // identity; replace only the carrier already authored by this read owner.
    Ok(field
        .clone()
        .with_data_type(data_type)
        .with_metadata(metadata))
}

fn sql_read_data_type(
    arrow_type: &DataType,
    iceberg_type: &crate::iceberg::spec::Type,
    path: &str,
) -> Result<DataType, String> {
    use crate::iceberg::spec::{PrimitiveType, Type};
    use arrow::datatypes::TimeUnit;

    match iceberg_type {
        Type::Primitive(PrimitiveType::Binary) => Ok(DataType::Binary),
        Type::Variant(_) => Ok(DataType::LargeBinary),
        Type::Primitive(PrimitiveType::Timestamptz) => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        Type::Primitive(PrimitiveType::TimestamptzNs) => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        Type::Primitive(_) => Ok(arrow_type.clone()),
        Type::Struct(iceberg_struct) => {
            let DataType::Struct(arrow_fields) = arrow_type else {
                return Err(format!(
                    "Iceberg struct {path} has incompatible Arrow carrier {arrow_type:?}"
                ));
            };
            if arrow_fields.len() != iceberg_struct.fields().len() {
                return Err(format!(
                    "Iceberg struct {path} field count does not match its Arrow carrier"
                ));
            }
            let fields = arrow_fields
                .iter()
                .zip(iceberg_struct.fields())
                .map(|(field, iceberg_field)| {
                    sql_read_field(field.as_ref(), iceberg_field).map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        Type::List(iceberg_list) => {
            let (field, wrap): (&Field, ListFieldWrapper) = match arrow_type {
                DataType::List(field) => (field.as_ref(), Box::new(DataType::List)),
                DataType::LargeList(field) => (field.as_ref(), Box::new(DataType::LargeList)),
                DataType::FixedSizeList(field, size) => {
                    let size = *size;
                    (
                        field.as_ref(),
                        Box::new(move |field| DataType::FixedSizeList(field, size)),
                    )
                }
                _ => {
                    return Err(format!(
                        "Iceberg list {path} has incompatible Arrow carrier {arrow_type:?}"
                    ));
                }
            };
            Ok(wrap(Arc::new(sql_read_field(
                field,
                &iceberg_list.element_field,
            )?)))
        }
        Type::Map(iceberg_map) => {
            let DataType::Map(entries, sorted) = arrow_type else {
                return Err(format!(
                    "Iceberg map {path} has incompatible Arrow carrier {arrow_type:?}"
                ));
            };
            let DataType::Struct(arrow_fields) = entries.data_type() else {
                return Err(format!("Iceberg map {path} has non-struct Arrow entries"));
            };
            if arrow_fields.len() != 2 {
                return Err(format!(
                    "Iceberg map {path} does not have key/value entries"
                ));
            }
            // The formal Iceberg source requires a present key. Preserve this
            // exact NULL contract in the Arrow format projection; downstream
            // value layout normalization cannot authorize nullable map keys.
            let fields = vec![
                Arc::new(
                    sql_read_field(arrow_fields[0].as_ref(), &iceberg_map.key_field)?
                        .with_nullable(!iceberg_map.key_field.required),
                ),
                Arc::new(sql_read_field(
                    arrow_fields[1].as_ref(),
                    &iceberg_map.value_field,
                )?),
            ];
            let entries = Arc::new(
                entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(fields.into())),
            );
            Ok(DataType::Map(entries, *sorted))
        }
    }
}

/// Validates and canonically encodes an Iceberg name mapping.
///
/// Name mappings are physical Iceberg schema facts.  Keeping validation and
/// canonical serialization with the provider prevents catalog mutation and
/// write planning callers from each accepting a slightly different mapping.
pub fn canonical_name_mapping(raw: &str) -> Result<String, String> {
    encode_name_mapping(&parse_name_mapping(raw)?)
}

pub(crate) fn parse_name_mapping(raw: &str) -> Result<NameMapping, String> {
    let value: serde_json::Value = crate::metadata_json::from_str(raw)
        .map_err(|error| format!("decode schema.name-mapping.default: {error}"))?;
    validate_name_mapping_json(&value)?;
    serde_json::from_value(value)
        .map_err(|error| format!("decode schema.name-mapping.default: {error}"))
}

pub(crate) fn encode_name_mapping(mapping: &NameMapping) -> Result<String, String> {
    serde_json::to_string(mapping)
        .map_err(|error| format!("encode canonical schema.name-mapping.default: {error}"))
}

fn validate_name_mapping_json(value: &serde_json::Value) -> Result<(), String> {
    fn visit(fields: &serde_json::Value, ids: &mut HashSet<i64>) -> Result<(), String> {
        let fields = fields
            .as_array()
            .ok_or_else(|| "Iceberg name mapping root/fields must be an array".to_string())?;
        let mut sibling_aliases = HashSet::new();
        for field in fields {
            let object = field
                .as_object()
                .ok_or_else(|| "Iceberg name mapping field must be an object".to_string())?;
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "field-id" | "names" | "fields"))
            {
                return Err("Iceberg name mapping contains an unknown field".to_string());
            }
            if let Some(value) = object.get("field-id")
                && !value.is_null()
            {
                let id = value.as_i64().ok_or_else(|| {
                    "Iceberg name mapping field-id must be an integer".to_string()
                })?;
                if id <= 0 || !ids.insert(id) {
                    return Err(format!(
                        "Iceberg name mapping has duplicate or invalid ID {id}"
                    ));
                }
            }
            let names = object
                .get("names")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "Iceberg name mapping names must be an array".to_string())?;
            for name in names {
                let name = name
                    .as_str()
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| "Iceberg name mapping alias must be nonempty".to_string())?;
                if !sibling_aliases.insert(name.to_string()) {
                    return Err(format!("Iceberg name mapping has duplicate alias {name}"));
                }
            }
            if let Some(children) = object.get("fields")
                && !children.is_null()
            {
                visit(children, ids)?;
            }
        }
        Ok(())
    }

    let mut ids = HashSet::new();
    visit(value, &mut ids)
}

pub fn schema_field_id_coverage(schema: &SchemaRef) -> Result<(usize, usize), String> {
    schema
        .fields()
        .iter()
        .try_fold((0usize, 0usize), |(identified, total), field| {
            let (field_identified, field_total) = field_id_coverage(field.as_ref())?;
            Ok::<_, String>((identified + field_identified, total + field_total))
        })
}

pub fn apply_name_mapping_to_schema(
    schema: &SchemaRef,
    name_mapping: &NameMapping,
) -> Result<SchemaRef, String> {
    apply_name_mapping(schema, name_mapping, |_, _| {})
}

/// Resolve the logical paths of an ID-less file. The observer sees the
/// result at each path we looked up, including an unmatched field at the
/// boundary of an ignored subtree.
pub(crate) fn apply_name_mapping(
    schema: &SchemaRef,
    name_mapping: &NameMapping,
    mut observe: impl FnMut(&[&str], Option<i32>),
) -> Result<SchemaRef, String> {
    let mut resolver = NameMappingResolver {
        observe: &mut observe,
        used_ids: HashSet::new(),
    };
    let mut path = Vec::new();
    let fields = schema
        .fields()
        .iter()
        .map(|field| resolver.resolve(field, field.name(), name_mapping.fields().iter(), &mut path))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// Re-annotate a read output schema with the provider-owned field facts carried
/// by the frozen Iceberg schema: parquet field IDs and each column's
/// spec-encoded initial default. iceberg-rust's Arrow conversion drops initial
/// defaults, so without this a data file written before `ADD COLUMN ... DEFAULT`
/// reads back as NULL instead of the column's default.
///
/// A field with no frozen counterpart is returned unchanged: metadata
/// pseudo-columns own their own identity, and a frozen row-mutation source
/// projects an older snapshot schema whose columns may since have been renamed.
pub fn annotate_read_schema_from_scan_model(
    output_schema: &SchemaRef,
    iceberg_schema: &IcebergSchemaDef,
) -> Result<SchemaRef, String> {
    let fields = output_schema
        .fields()
        .iter()
        .map(|field| {
            let Some(frozen) = iceberg_schema
                .fields
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(field.name()))
            else {
                return Ok(field.as_ref().clone());
            };
            apply_scan_field_id_recursive(field.as_ref(), frozen)
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        output_schema.metadata().clone(),
    )))
}

fn apply_scan_field_id_recursive(
    field: &Field,
    frozen: &IcebergSchemaFieldDef,
) -> Result<Field, String> {
    let mut metadata = field.metadata().clone();
    metadata.insert(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        frozen.field_id.to_string(),
    );
    if let Some(default) = frozen.initial_default_json.as_ref() {
        metadata.insert(
            ICEBERG_INITIAL_DEFAULT_META_KEY.to_string(),
            default.clone(),
        );
    }
    let data_type = match field.data_type() {
        // Parquet VARIANT is one Iceberg primitive field whose physical Arrow
        // carrier is a Struct. Its metadata/value children are encoding
        // details, not independently identified Iceberg nested fields.
        data_type if frozen.is_variant => data_type.clone(),
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|child| scan_child_field(child.as_ref(), &frozen.children))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        DataType::List(child) => DataType::List(Arc::new(scan_list_child(child.as_ref(), frozen)?)),
        DataType::LargeList(child) => {
            DataType::LargeList(Arc::new(scan_list_child(child.as_ref(), frozen)?))
        }
        DataType::FixedSizeList(child, size) => {
            DataType::FixedSizeList(Arc::new(scan_list_child(child.as_ref(), frozen)?), *size)
        }
        DataType::Map(entries, sorted) => {
            let DataType::Struct(children) = entries.data_type() else {
                return Err(format!(
                    "Iceberg MAP column {} has non-struct entries",
                    field.name()
                ));
            };
            if children.len() != 2 || frozen.children.len() != 2 {
                return Err(format!(
                    "Iceberg MAP column {} has incompatible frozen fields",
                    field.name()
                ));
            }
            let children = children
                .iter()
                .zip(frozen.children.iter())
                .map(|(child, frozen)| apply_scan_field_id_recursive(child.as_ref(), frozen))
                .collect::<Result<Vec<_>, _>>()?;
            let entries = Field::new(
                entries.name(),
                DataType::Struct(children.into()),
                entries.is_nullable(),
            )
            .with_metadata(entries.metadata().clone());
            DataType::Map(Arc::new(entries), *sorted)
        }
        data_type => data_type.clone(),
    };
    Ok(Field::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata))
}

fn scan_child_field(
    field: &Field,
    frozen_children: &[IcebergSchemaFieldDef],
) -> Result<Field, String> {
    let frozen = frozen_children
        .iter()
        .find(|candidate| candidate.name == field.name().as_str())
        .ok_or_else(|| {
            format!(
                "Iceberg nested writer field {} is missing frozen metadata",
                field.name()
            )
        })?;
    apply_scan_field_id_recursive(field, frozen)
}

fn scan_list_child(field: &Field, frozen: &IcebergSchemaFieldDef) -> Result<Field, String> {
    let child = frozen.children.first().ok_or_else(|| {
        format!(
            "Iceberg list writer field {} is missing frozen element metadata",
            field.name()
        )
    })?;
    apply_scan_field_id_recursive(field, child)
}

struct NameMappingResolver<'a, F> {
    observe: &'a mut F,
    used_ids: HashSet<i32>,
}

impl<F: FnMut(&[&str], Option<i32>)> NameMappingResolver<'_, F> {
    fn resolve<'a, 'm>(
        &mut self,
        field: &'a Field,
        segment: &'a str,
        mappings: impl Iterator<Item = &'m MappedField>,
        path: &mut Vec<&'a str>,
    ) -> Result<Field, String> {
        path.push(segment);
        let mapped = mapped_field_for_name(mappings, segment)?;
        let field_id = mapped.and_then(MappedField::field_id);
        (self.observe)(path, field_id);
        let resolved = match (mapped, field_id) {
            (Some(mapped), Some(id)) => {
                if !self.used_ids.insert(id) {
                    return Err(format!(
                        "Iceberg name mapping resolves multiple physical fields to ID {id}"
                    ));
                }
                let data_type = self.resolve_children(field, mapped.fields(), path)?;
                let mut metadata = field.metadata().clone();
                metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
                field
                    .clone()
                    .with_data_type(data_type)
                    .with_metadata(metadata)
            }
            _ => field.clone(),
        };
        path.pop();
        Ok(resolved)
    }

    fn resolve_children<'a>(
        &mut self,
        field: &'a Field,
        mappings: &[Arc<MappedField>],
        path: &mut Vec<&'a str>,
    ) -> Result<DataType, String> {
        let mut resolve = |child: &'a Field, name: &'a str| {
            self.resolve(child, name, mappings.iter().map(Arc::as_ref), path)
                .map(Arc::new)
        };
        Ok(match field.data_type() {
            data_type if is_variant_struct_data_type(data_type) => data_type.clone(),
            DataType::Struct(children) => DataType::Struct(
                children
                    .iter()
                    .map(|child| resolve(child, child.name()))
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            ),
            DataType::List(child) => DataType::List(resolve(child, "element")?),
            DataType::LargeList(child) => DataType::LargeList(resolve(child, "element")?),
            DataType::FixedSizeList(child, size) => {
                DataType::FixedSizeList(resolve(child, "element")?, *size)
            }
            DataType::Map(entries, sorted) => {
                let DataType::Struct(entry_fields) = entries.data_type() else {
                    return Err(format!(
                        "Iceberg mapped map field {} has non-struct entries",
                        field.name()
                    ));
                };
                let [key, value] = entry_fields.as_ref() else {
                    return Err(format!(
                        "Iceberg mapped map field {} must have key and value entries",
                        field.name()
                    ));
                };
                let key = resolve(key, "key")?;
                let value = resolve(value, "value")?;
                let entries = entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(vec![key, value].into()));
                DataType::Map(Arc::new(entries), *sorted)
            }
            data_type => data_type.clone(),
        })
    }
}

fn mapped_field_for_name<'a>(
    mappings: impl Iterator<Item = &'a MappedField>,
    name: &str,
) -> Result<Option<&'a MappedField>, String> {
    let mut matches =
        mappings.filter(|mapped| mapped.names().iter().any(|candidate| candidate == name));
    let mapped = matches.next();
    if matches.next().is_some() {
        return Err(format!(
            "Iceberg name mapping contains duplicate aliases for physical field {name}"
        ));
    }
    Ok(mapped)
}

fn field_id_coverage(field: &Field) -> Result<(usize, usize), String> {
    let identified = usize::from(field_id_for_arrow_field(field)?.is_some());
    if is_variant_struct_data_type(field.data_type()) {
        return Ok((identified, 1));
    }
    let (children_identified, children_total) = match field.data_type() {
        DataType::Struct(children) => {
            children
                .iter()
                .try_fold((0usize, 0usize), |(identified, total), child| {
                    let (child_identified, child_total) = field_id_coverage(child.as_ref())?;
                    Ok::<_, String>((identified + child_identified, total + child_total))
                })?
        }
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            field_id_coverage(child.as_ref())?
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(children) = entries.data_type() else {
                return Err(format!("map field {} has non-struct entries", field.name()));
            };
            children
                .iter()
                .try_fold((0usize, 0usize), |(identified, total), child| {
                    let (child_identified, child_total) = field_id_coverage(child.as_ref())?;
                    Ok::<_, String>((identified + child_identified, total + child_total))
                })?
        }
        _ => (0, 0),
    };
    Ok((identified + children_identified, 1 + children_total))
}

pub fn field_id_for_arrow_field(field: &Field) -> Result<Option<i32>, String> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .map(|value| {
            value.parse::<i32>().map_err(|error| {
                format!(
                    "invalid Iceberg field ID metadata for column {}: {error}",
                    field.name()
                )
            })
        })
        .transpose()
}

pub fn unidentified_fields_are_only_opaque_variants(schema: &SchemaRef) -> Result<bool, String> {
    let mut found_unidentified_variant = false;
    for field in schema.fields() {
        if is_variant_struct_data_type(field.data_type()) {
            if field_id_for_arrow_field(field)?.is_none() {
                found_unidentified_variant = true;
            }
            continue;
        }
        let (identified, total) = field_id_coverage(field.as_ref())?;
        if identified != total {
            return Ok(false);
        }
    }
    Ok(found_unidentified_variant)
}

#[cfg(test)]
mod tests;

pub(crate) mod catalog_value;

/// Author the default mapping from Iceberg's logical schema, including its
/// canonical element/key/value roles. Names are individual path components.
#[allow(dead_code, reason = "ADD FILES publication consumes this fact in P08")]
pub(crate) fn name_mapping_from_schema(schema: &crate::iceberg::spec::Schema) -> NameMapping {
    use crate::iceberg::spec::{NestedField, Type};

    fn field_mapping(field: &NestedField, name: &str) -> MappedField {
        let children = match field.field_type.as_ref() {
            Type::Struct(structure) => structure
                .fields()
                .iter()
                .map(|child| field_mapping(child, &child.name))
                .collect(),
            Type::List(list) => vec![field_mapping(&list.element_field, "element")],
            Type::Map(map) => vec![
                field_mapping(&map.key_field, "key"),
                field_mapping(&map.value_field, "value"),
            ],
            Type::Primitive(_) | Type::Variant(_) => Vec::new(),
        };
        MappedField::new(Some(field.id), vec![name.to_owned()], children)
    }

    NameMapping::new(
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field_mapping(field, &field.name))
            .collect(),
    )
}

/// Resolve a recorded path in a later mapping with the same ancestor rule
/// as file binding. An unmapped ancestor hides its entire physical subtree.
#[allow(dead_code, reason = "ADD FILES publication consumes this fact in P08")]
pub(crate) fn mapped_path_id(
    mapping: &NameMapping,
    path: &[String],
) -> Result<Option<i32>, String> {
    let Some((name, rest)) = path.split_first() else {
        return Ok(None);
    };
    let mut entry = mapped_field_for_name(mapping.fields().iter(), name)?;
    for segment in rest {
        let Some(parent) = entry.filter(|parent| parent.field_id().is_some()) else {
            return Ok(None);
        };
        entry = mapped_field_for_name(parent.fields().iter().map(Arc::as_ref), segment)?;
    }
    Ok(entry.and_then(MappedField::field_id))
}
