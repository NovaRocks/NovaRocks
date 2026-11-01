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
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::{
        ICEBERG_INITIAL_DEFAULT_META_KEY, annotate_read_schema_from_scan_model,
        apply_scan_field_id_recursive, canonical_name_mapping, sql_read_field,
        sql_read_schema_from_iceberg,
    };
    use crate::iceberg::spec::{
        ListType, MapType, NestedField, PrimitiveType, Schema as IcebergSchema, StructType, Type,
    };
    use crate::scan_model::IcebergSchemaFieldDef;

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

    #[test]
    fn metadata_value_struct_annotation_preserves_real_child_ids() {
        let source = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::optional(
                    1,
                    "payload",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(
                            2,
                            "metadata",
                            Type::Primitive(PrimitiveType::Binary),
                        )
                        .into(),
                        NestedField::optional(3, "value", Type::Primitive(PrimitiveType::Binary))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let arrow = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(
                vec![
                    Field::new("metadata", DataType::Binary, true),
                    Field::new("value", DataType::Binary, true),
                ]
                .into(),
            ),
            true,
        )]));
        let frozen = crate::schema_facts::iceberg_schema_def(&source);
        let output = annotate_read_schema_from_scan_model(&arrow, &frozen).unwrap();
        let DataType::Struct(children) = output.field(0).data_type() else {
            panic!("struct");
        };
        assert_eq!(children[0].metadata()[PARQUET_FIELD_ID_META_KEY], "2");
        assert_eq!(children[1].metadata()[PARQUET_FIELD_ID_META_KEY], "3");
    }

    #[test]
    fn frozen_variant_annotation_keeps_physical_children_opaque() {
        let field = Field::new(
            "v",
            DataType::Struct(
                vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("value", DataType::LargeBinary, false),
                ]
                .into(),
            ),
            true,
        );
        let frozen = IcebergSchemaFieldDef {
            is_variant: true,
            field_id: 7,
            name: "v".to_string(),
            initial_default: None,
            write_default: None,
            initial_default_json: None,
            write_default_json: None,
            children: Vec::new(),
        };

        let annotated =
            apply_scan_field_id_recursive(&field, &frozen).expect("annotate variant field");
        assert_eq!(
            annotated.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"7".to_string())
        );
        assert_eq!(annotated.data_type(), field.data_type());
        let DataType::Struct(children) = annotated.data_type() else {
            panic!("variant remains a struct carrier");
        };
        assert!(children.iter().all(|child| child.metadata().is_empty()));
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn real_storage_schema_authors_uuid_variant_and_distinct_fixed_domains() {
        use novarocks_type_contract::{ValueLogicalType, field_logical_type};
        let iceberg = IcebergSchema::builder()
            .with_schema_id(8)
            .with_fields(vec![
                NestedField::required(11, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
                NestedField::optional(12, "fixed", Type::Primitive(PrimitiveType::Fixed(16)))
                    .into(),
                NestedField::optional(
                    13,
                    "variant",
                    Type::Variant(crate::iceberg::spec::VariantType),
                )
                .into(),
                NestedField::required(14, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
            ])
            .build()
            .unwrap();
        let schema = sql_read_schema_from_iceberg(&iceberg).unwrap();
        for (ordinal, carrier, logical, nullable, id) in [
            (
                0,
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Uuid,
                false,
                11,
            ),
            (
                1,
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Physical,
                true,
                12,
            ),
            (
                2,
                DataType::LargeBinary,
                ValueLogicalType::Variant,
                true,
                13,
            ),
            (3, DataType::Binary, ValueLogicalType::Physical, false, 14),
        ] {
            let field = schema.field(ordinal);
            assert_eq!(field.data_type(), &carrier);
            assert_eq!(field.is_nullable(), nullable);
            assert_eq!(field_logical_type(field).unwrap(), logical);
            assert_eq!(
                field.metadata().get(PARQUET_FIELD_ID_META_KEY),
                Some(&id.to_string())
            );
        }
    }

    #[test]
    fn recursive_storage_fields_preserve_uuid_and_variant_in_read_schema() {
        use novarocks_type_contract::{ValueLogicalType, field_logical_type};
        let iceberg = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(
                    1,
                    "record",
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::required(
                            2,
                            "identity",
                            Type::Primitive(PrimitiveType::Uuid),
                        )),
                        Arc::new(NestedField::optional(
                            3,
                            "payload",
                            Type::Variant(crate::iceberg::spec::VariantType),
                        )),
                    ])),
                )
                .into(),
                NestedField::optional(
                    4,
                    "identities",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        5,
                        Type::Primitive(PrimitiveType::Uuid),
                        false,
                    )))),
                )
                .into(),
                NestedField::optional(
                    6,
                    "objects",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::map_key_element(
                            7,
                            Type::Primitive(PrimitiveType::Uuid),
                        )),
                        Arc::new(NestedField::map_value_element(
                            8,
                            Type::Variant(crate::iceberg::spec::VariantType),
                            true,
                        )),
                    )),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let schema = sql_read_schema_from_iceberg(&iceberg).unwrap();
        let DataType::Struct(record) = schema.field(0).data_type() else {
            panic!("record")
        };
        let DataType::List(items) = schema.field(1).data_type() else {
            panic!("items")
        };
        let DataType::Map(entries, _) = schema.field(2).data_type() else {
            panic!("map")
        };
        let DataType::Struct(map) = entries.data_type() else {
            panic!("entries")
        };
        for (field, logical, id) in [
            (record[0].as_ref(), ValueLogicalType::Uuid, 2),
            (record[1].as_ref(), ValueLogicalType::Variant, 3),
            (items.as_ref(), ValueLogicalType::Uuid, 5),
            (map[0].as_ref(), ValueLogicalType::Uuid, 7),
            (map[1].as_ref(), ValueLogicalType::Variant, 8),
        ] {
            assert_eq!(field_logical_type(field).unwrap(), logical);
            assert_eq!(
                field.metadata().get(PARQUET_FIELD_ID_META_KEY),
                Some(&id.to_string())
            );
        }
        assert!(!record[0].is_nullable());
        assert!(record[1].is_nullable());
        assert!(items.is_nullable());
        // The frozen Iceberg Map declares a required key independently of its
        // generic Arrow projection; execution must retain that declaration.
        assert!(!map[0].is_nullable());
        assert!(!map[1].is_nullable());
        let frozen = crate::schema_facts::iceberg_schema_def(&iceberg);
        let annotated = annotate_read_schema_from_scan_model(&schema, &frozen).unwrap();
        assert!(novarocks_type_contract::arrow_schemas_exact(
            &schema, &annotated
        ));
    }

    #[test]
    fn authored_logical_metadata_conflicts_and_wrong_uuid_carriers_fail() {
        use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType, field_logical_type};
        let uuid = NestedField::optional(11, "u", Type::Primitive(PrimitiveType::Uuid));
        for field in [
            Field::new("u", DataType::Utf8, true),
            Field::new("u", DataType::FixedSizeBinary(15), true),
            Field::new("u", DataType::FixedSizeBinary(16), true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    NR_LOGICAL_TYPE_KEY.into(),
                    "largeint".into(),
                )]),
            ),
            Field::new("u", DataType::FixedSizeBinary(16), true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    NR_LOGICAL_TYPE_KEY.into(),
                    "unknown".into(),
                )]),
            ),
        ] {
            assert!(sql_read_field(&field, &uuid).is_err());
        }
        let valid = Field::new("u", DataType::FixedSizeBinary(16), true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                NR_LOGICAL_TYPE_KEY.into(),
                "uuid".into(),
            )]),
        );
        assert_eq!(
            field_logical_type(&sql_read_field(&valid, &uuid).unwrap()).unwrap(),
            ValueLogicalType::Uuid
        );
        let fixed = NestedField::optional(12, "u", Type::Primitive(PrimitiveType::Fixed(16)));
        assert!(sql_read_field(&valid, &fixed).is_err());
        let binary = NestedField::optional(13, "u", Type::Primitive(PrimitiveType::Binary));
        let forged_variant = Field::new("u", DataType::LargeBinary, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                NR_LOGICAL_TYPE_KEY.into(),
                "variant".into(),
            )]),
        );
        assert!(sql_read_field(&forged_variant, &binary).is_err());
        let variant =
            NestedField::optional(14, "u", Type::Variant(crate::iceberg::spec::VariantType));
        assert!(sql_read_field(&valid, &variant).is_err());
        // An unlabelled LargeBinary field itself does not author VARIANT.
        let plain = sql_read_field(
            &Field::new("u", DataType::LargeBinary, true),
            &NestedField::optional(15, "u", Type::Primitive(PrimitiveType::Binary)),
        )
        .unwrap();
        assert_eq!(
            field_logical_type(&plain).unwrap(),
            ValueLogicalType::Physical
        );
    }

    #[test]
    #[allow(deprecated)]
    fn read_field_projection_preserves_provider_metadata_and_dictionary_facts() {
        use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, arrow_fields_exact};
        let source = Field::new("u", DataType::FixedSizeBinary(16), false).with_metadata(
            std::collections::HashMap::<String, String>::from([
                (PARQUET_FIELD_ID_META_KEY.into(), "91".into()),
                ("provider.annotation".into(), "opaque-source-fact".into()),
                (
                    ICEBERG_INITIAL_DEFAULT_META_KEY.into(),
                    "\"00112233-4455-6677-8899-aabbccddeeff\"".into(),
                ),
            ]),
        );
        let uuid = NestedField::required(91, "u", Type::Primitive(PrimitiveType::Uuid));
        let mut metadata = source.metadata().clone();
        metadata.insert(NR_LOGICAL_TYPE_KEY.to_string(), "uuid".to_string());
        let projected = sql_read_field(&source, &uuid).unwrap();
        assert!(arrow_fields_exact(
            &projected,
            &source.with_metadata(metadata)
        ));
        let dictionary = Field::new_dict(
            "codes",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true,
            72,
            true,
        )
        .with_metadata(std::collections::HashMap::<String, String>::from([(
            "provider.annotation".into(),
            "kept".into(),
        )]));
        let storage = NestedField::optional(72, "codes", Type::Primitive(PrimitiveType::String));
        assert!(arrow_fields_exact(
            &sql_read_field(&dictionary, &storage).unwrap(),
            &dictionary
        ));
    }

    #[test]
    fn frozen_iceberg_primitives_use_exact_sql_read_carriers() {
        let iceberg = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
                NestedField::optional(
                    2,
                    "variant",
                    Type::Variant(crate::iceberg::spec::VariantType),
                )
                .into(),
                NestedField::required(
                    3,
                    "timestamptz",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )
                .into(),
            ])
            .build()
            .expect("Iceberg schema");

        let schema = sql_read_schema_from_iceberg(&iceberg).expect("SQL read schema");
        assert_eq!(schema.field(0).data_type(), &DataType::Binary);
        assert_eq!(schema.field(1).data_type(), &DataType::LargeBinary);
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn frozen_nested_iceberg_primitives_use_exact_sql_read_carriers() {
        let iceberg = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(
                    1,
                    "record",
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                        2,
                        "payload",
                        Type::Primitive(PrimitiveType::Binary),
                    ))])),
                )
                .into(),
                NestedField::optional(
                    3,
                    "items",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        4,
                        Type::Primitive(PrimitiveType::Binary),
                        false,
                    )))),
                )
                .into(),
                NestedField::optional(
                    5,
                    "attributes",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::map_key_element(
                            6,
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::map_value_element(
                            7,
                            Type::Variant(crate::iceberg::spec::VariantType),
                            false,
                        )),
                    )),
                )
                .into(),
            ])
            .build()
            .expect("Iceberg schema");

        let schema = sql_read_schema_from_iceberg(&iceberg).expect("SQL read schema");
        let DataType::Struct(record) = schema.field(0).data_type() else {
            panic!("record is a struct")
        };
        assert_eq!(record[0].data_type(), &DataType::Binary);
        let DataType::List(items) = schema.field(1).data_type() else {
            panic!("items is a list")
        };
        assert_eq!(items.data_type(), &DataType::Binary);
        let DataType::Map(entries, _) = schema.field(2).data_type() else {
            panic!("attributes is a map")
        };
        let DataType::Struct(entries) = entries.data_type() else {
            panic!("map entries are a struct")
        };
        assert_eq!(entries[1].data_type(), &DataType::LargeBinary);
    }
}

pub(crate) mod catalog_value;
