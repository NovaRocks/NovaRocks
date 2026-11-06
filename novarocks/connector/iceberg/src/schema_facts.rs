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

//! Provider-owned projections of Iceberg schema and row-lineage semantics.

use crate::iceberg::spec::{FormatVersion, NestedField, Schema, TableMetadata, Type};
use crate::scan_model::{IcebergSchemaDef, IcebergSchemaFieldDef};

/// Converts Iceberg field-ID/default facts into the frozen provider schema
/// representation carried inside provider-private scan and write payloads.
pub fn iceberg_schema_def(schema: &Schema) -> IcebergSchemaDef {
    IcebergSchemaDef {
        fields: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| iceberg_field_def(field.as_ref()))
            .collect(),
    }
}

fn iceberg_field_def(field: &NestedField) -> IcebergSchemaFieldDef {
    let initial_default_json = field.initial_default.as_ref().and_then(|literal| {
        literal
            .clone()
            .try_into_json(field.field_type.as_ref())
            .ok()
            .map(|json| json.to_string())
    });
    let write_default_json = field.write_default.as_ref().and_then(|literal| {
        literal
            .clone()
            .try_into_json(field.field_type.as_ref())
            .ok()
            .map(|json| json.to_string())
    });
    IcebergSchemaFieldDef {
        is_variant: matches!(field.field_type.as_ref(), Type::Variant(_)),
        field_id: field.id,
        name: field.name.clone(),
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
        initial_default_json,
        write_default_json,
        children: iceberg_type_children(field.field_type.as_ref()),
    }
}

fn iceberg_type_children(ty: &Type) -> Vec<IcebergSchemaFieldDef> {
    match ty {
        Type::Struct(struct_ty) => struct_ty
            .fields()
            .iter()
            .map(|field| iceberg_field_def(field.as_ref()))
            .collect(),
        Type::List(list_ty) => vec![iceberg_field_def(list_ty.element_field.as_ref())],
        Type::Map(map_ty) => vec![
            iceberg_field_def(map_ty.key_field.as_ref()),
            iceberg_field_def(map_ty.value_field.as_ref()),
        ],
        Type::Primitive(_) | Type::Variant(_) => Vec::new(),
    }
}

/// Iceberg V3 always exposes row-lineage columns, independently of properties.
// Design: ADR-0219 (docs/adr/ADR-0219-iceberg-v3-lineage-and-planned-duplicate-match-errors.md)
pub fn row_lineage_enabled(metadata: &TableMetadata) -> bool {
    matches!(metadata.format_version(), FormatVersion::V3)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::iceberg::spec::{
        NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, TableMetadataBuilder,
    };

    use super::*;

    fn metadata(
        format_version: FormatVersion,
        properties: HashMap<String, String>,
    ) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///schema-facts-test".to_string(),
            format_version,
            properties,
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn row_lineage_depends_only_on_format_version() {
        assert!(row_lineage_enabled(&metadata(
            FormatVersion::V3,
            HashMap::new()
        )));
        assert!(row_lineage_enabled(&metadata(
            FormatVersion::V3,
            HashMap::from([("write.row-lineage".to_string(), "false".to_string())]),
        )));
        assert!(!row_lineage_enabled(&metadata(
            FormatVersion::V2,
            HashMap::new()
        )));
        assert!(!row_lineage_enabled(&metadata(
            FormatVersion::V2,
            HashMap::from([("write.row-lineage".to_string(), "true".to_string())]),
        )));
    }
}
