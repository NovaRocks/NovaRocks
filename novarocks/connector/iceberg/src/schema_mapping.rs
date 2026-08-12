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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
pub use novarocks_types::value::variant::is_variant_struct_data_type;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY;
use crate::iceberg::spec::{MappedField, NameMapping};
use crate::row_lineage_synth::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use crate::scan_model::{IcebergSchemaDef, IcebergSchemaFieldDef};

/// Validates and canonically encodes an Iceberg name mapping.
///
/// Name mappings are physical Iceberg schema facts.  Keeping validation and
/// canonical serialization with the provider prevents catalog mutation and
/// write planning callers from each accepting a slightly different mapping.
pub fn canonical_name_mapping(raw: &str) -> Result<String, String> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| format!("decode schema.name-mapping.default: {error}"))?;
    validate_name_mapping_json(&value)?;
    let mapping: NameMapping = serde_json::from_value(value)
        .map_err(|error| format!("decode schema.name-mapping.default: {error}"))?;
    serde_json::to_string(&mapping)
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
            let id = object
                .get("field-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "Iceberg name mapping field-id is required".to_string())?;
            if id <= 0 || !ids.insert(id) {
                return Err(format!(
                    "Iceberg name mapping has duplicate or invalid ID {id}"
                ));
            }
            let names = object
                .get("names")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "Iceberg name mapping names must be an array".to_string())?;
            if names.is_empty() {
                return Err("Iceberg name mapping names must not be empty".to_string());
            }
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
    let mappings = name_mapping
        .fields()
        .iter()
        .cloned()
        .map(Arc::new)
        .collect::<Vec<_>>();
    let fields = schema
        .fields()
        .iter()
        .map(|field| map_field_id_recursive(field.as_ref(), &mappings))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// Re-annotate a generic native writer schema with the frozen Iceberg field-ID
/// tree carried by a provider write handle.  The generic carrier owns Arrow
/// types; Iceberg owns physical field identity and defaults.
pub fn annotate_schema_from_scan_model(
    input_schema: &SchemaRef,
    iceberg_schema: &IcebergSchemaDef,
) -> Result<SchemaRef, String> {
    let fields = input_schema
        .fields()
        .iter()
        .map(|field| {
            if is_write_virtual_column(field.name()) {
                return Ok(field.as_ref().clone());
            }
            if let Some(field_id) = reserved_row_lineage_field_id(field)? {
                let mut metadata = field.metadata().clone();
                metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
                return Ok(Field::new(
                    field.name(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
                .with_metadata(metadata));
            }
            let frozen = iceberg_schema
                .fields
                .iter()
                .find(|candidate| candidate.name == field.name().as_str())
                .ok_or_else(|| {
                    format!(
                        "Iceberg writer column {} is missing its frozen schema field",
                        field.name()
                    )
                })?;
            apply_scan_field_id_recursive(field.as_ref(), frozen)
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
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

fn is_write_virtual_column(name: &str) -> bool {
    matches!(name, "_file" | "_pos" | "__change_op") || name.starts_with("__nr_var_")
}

fn reserved_row_lineage_field_id(field: &Field) -> Result<Option<i32>, String> {
    let field_id = if field.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL) {
        ICEBERG_RESERVED_FIELD_ID_ROW_ID
    } else if field
        .name()
        .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
    {
        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
    } else {
        return Ok(None);
    };
    if field.data_type() != &DataType::Int64 {
        return Err(format!(
            "Iceberg reserved row-lineage column {} expects Int64, got {:?}",
            field.name(),
            field.data_type()
        ));
    }
    Ok(Some(field_id))
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

fn map_field_id_recursive(field: &Field, mappings: &[Arc<MappedField>]) -> Result<Field, String> {
    let mapped = mapped_field_for_name(mappings, field.name())?;
    let field_id = mapped.field_id().ok_or_else(|| {
        format!(
            "Iceberg name mapping entry for {} does not contain a field ID",
            field.name()
        )
    })?;
    let mut metadata = field.metadata().clone();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    let data_type = match field.data_type() {
        data_type if is_variant_struct_data_type(data_type) => data_type.clone(),
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|child| map_field_id_recursive(child.as_ref(), mapped.fields()))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        DataType::List(child) => DataType::List(Arc::new(map_field_id_recursive(
            child.as_ref(),
            mapped.fields(),
        )?)),
        DataType::LargeList(child) => DataType::LargeList(Arc::new(map_field_id_recursive(
            child.as_ref(),
            mapped.fields(),
        )?)),
        DataType::FixedSizeList(child, size) => DataType::FixedSizeList(
            Arc::new(map_field_id_recursive(child.as_ref(), mapped.fields())?),
            *size,
        ),
        DataType::Map(entries, sorted) => {
            let DataType::Struct(entry_fields) = entries.data_type() else {
                return Err(format!(
                    "Iceberg mapped map field {} has non-struct entries",
                    field.name()
                ));
            };
            if entry_fields.len() != 2 {
                return Err(format!(
                    "Iceberg mapped map field {} must have key and value entries",
                    field.name()
                ));
            }
            let key = map_field_id_recursive(entry_fields[0].as_ref(), mapped.fields())?;
            let value = map_field_id_recursive(entry_fields[1].as_ref(), mapped.fields())?;
            let entries = Field::new(
                entries.name(),
                DataType::Struct(vec![key, value].into()),
                entries.is_nullable(),
            )
            .with_metadata(entries.metadata().clone());
            DataType::Map(Arc::new(entries), *sorted)
        }
        data_type => data_type.clone(),
    };
    Ok(Field::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata))
}

fn mapped_field_for_name<'a>(
    mappings: &'a [Arc<MappedField>],
    name: &str,
) -> Result<&'a MappedField, String> {
    let mut matches = mappings
        .iter()
        .filter(|mapped| mapped.names().iter().any(|candidate| candidate == name));
    let mapped = matches
        .next()
        .ok_or_else(|| format!("Iceberg name mapping does not contain physical field {name}"))?;
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
mod tests {
    use super::canonical_name_mapping;

    #[test]
    fn canonical_name_mapping_is_strict_and_provider_owned() {
        assert_eq!(
            canonical_name_mapping(r#"[{"names":["legacy_id"],"field-id":1}]"#)
                .expect("canonical mapping"),
            r#"[{"field-id":1,"names":["legacy_id"]}]"#,
        );
        assert!(
            canonical_name_mapping(r#"[{"field-id":1,"names":["id"],"credential":"secret"}]"#)
                .is_err()
        );
        assert!(
            canonical_name_mapping(
                r#"[
                {"field-id":1,"names":["left"],"fields":[{"field-id":2,"names":["id"]}]},
                {"field-id":3,"names":["right"],"fields":[{"field-id":4,"names":["id"]}]}
            ]"#
            )
            .is_ok()
        );
    }
}
