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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::exec::row_position::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use crate::runtime::descriptor_snapshot::{DescriptorIcebergSchema, DescriptorIcebergSchemaField};

const VIRTUAL_COUNT_COLUMN: &str = "___count___";
pub const ICEBERG_INITIAL_DEFAULT_META_KEY: &str = "novarocks.iceberg.initial_default";

#[derive(Clone, Debug)]
pub struct IcebergArrowColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct IcebergSchemaFieldDescriptor {
    pub name: String,
    pub field_id: Option<i32>,
    pub children: Vec<IcebergSchemaFieldDescriptor>,
    pub initial_default_json: Option<String>,
}

#[derive(Clone, Debug)]
pub struct IcebergSchemaDescriptor {
    pub fields: Vec<IcebergSchemaFieldDescriptor>,
}

#[derive(Clone, Debug)]
pub struct IcebergTableColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct IcebergPartitionInfo {
    pub source_column_name: String,
    pub partition_column_name: String,
    pub transform_expr: String,
}

#[derive(Clone, Debug)]
pub struct IcebergTableDescriptor {
    pub columns: Vec<IcebergTableColumn>,
    pub iceberg_schema: Option<IcebergSchemaDescriptor>,
    pub equality_delete_schema: Option<IcebergSchemaDescriptor>,
    pub partition_info: Vec<IcebergPartitionInfo>,
    pub current_snapshot_id: Option<i64>,
    pub serialized_metadata: Option<String>,
}

pub fn build_full_output_schema(iceberg: &IcebergTableDescriptor) -> Result<SchemaRef, String> {
    if let Some(schema) = iceberg.iceberg_schema.as_ref() {
        let mut fields = Vec::with_capacity(schema.fields.len());
        for schema_field in &schema.fields {
            let col = iceberg
                .columns
                .iter()
                .find(|c| c.name == schema_field.name)
                .ok_or_else(|| {
                    format!(
                        "iceberg schema field {} missing column descriptor",
                        schema_field.name
                    )
                })?;
            let field = Field::new(col.name.clone(), col.data_type.clone(), col.nullable);
            let field = apply_field_id_recursive(field, schema_field)?;
            fields.push(field);
        }
        return Ok(Arc::new(Schema::new(fields)));
    }

    let mut fields = Vec::with_capacity(iceberg.columns.len());
    for col in &iceberg.columns {
        fields.push(Field::new(
            col.name.clone(),
            col.data_type.clone(),
            col.nullable,
        ));
    }

    Ok(Arc::new(Schema::new(fields)))
}

pub fn build_projected_output_schema(
    iceberg: &IcebergTableDescriptor,
    columns: &[IcebergArrowColumn],
) -> Result<Option<SchemaRef>, String> {
    let Some(schema) = iceberg.iceberg_schema.as_ref() else {
        return Ok(None);
    };
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        if column.name == VIRTUAL_COUNT_COLUMN {
            fields.push(Field::new(column.name.clone(), DataType::Boolean, false));
            continue;
        }
        if let Some(field) = build_reserved_row_lineage_projected_field(column)? {
            fields.push(field);
            continue;
        }
        let schema_field = schema
            .fields
            .iter()
            .find(|field| field.name == column.name)
            .ok_or_else(|| {
                format!(
                    "iceberg projected column {} missing schema field descriptor",
                    column.name
                )
            })?;
        let field = Field::new(
            column.name.clone(),
            column.data_type.clone(),
            column.nullable,
        );
        let field = apply_field_id_recursive(field, schema_field)?;
        fields.push(field);
    }
    Ok(Some(Arc::new(Schema::new(fields))))
}

pub(crate) fn build_projected_output_schema_from_descriptor(
    iceberg_schema: Option<&DescriptorIcebergSchema>,
    columns: &[IcebergArrowColumn],
) -> Result<Option<SchemaRef>, String> {
    let Some(schema) = iceberg_schema else {
        return Ok(None);
    };
    let schema_fields = schema
        .fields
        .as_ref()
        .ok_or_else(|| "iceberg schema missing fields".to_string())?;
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        if column.name == VIRTUAL_COUNT_COLUMN {
            fields.push(Field::new(column.name.clone(), DataType::Boolean, false));
            continue;
        }
        if let Some(field) = build_reserved_row_lineage_projected_field(column)? {
            fields.push(field);
            continue;
        }
        let schema_field = schema_fields
            .iter()
            .find(|field| field.name.as_deref() == Some(column.name.as_str()))
            .ok_or_else(|| {
                format!(
                    "iceberg projected column {} missing schema field descriptor",
                    column.name
                )
            })?;
        let field = Field::new(
            column.name.clone(),
            column.data_type.clone(),
            column.nullable,
        );
        let field = apply_descriptor_field_id_recursive(field, schema_field)?;
        fields.push(field);
    }
    Ok(Some(Arc::new(Schema::new(fields))))
}

fn build_reserved_row_lineage_projected_field(
    column: &IcebergArrowColumn,
) -> Result<Option<Field>, String> {
    if column
        .name
        .eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
    {
        if !matches!(column.data_type, DataType::Int8) {
            return Err(format!(
                "iceberg internal column {} expects Int8, got {:?}",
                column.name, column.data_type
            ));
        }
        return Ok(Some(Field::new(
            column.name.clone(),
            DataType::Int8,
            column.nullable,
        )));
    }

    let field_id = if column.name.eq_ignore_ascii_case(ICEBERG_ROW_ID_COL) {
        ICEBERG_RESERVED_FIELD_ID_ROW_ID
    } else if column
        .name
        .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
    {
        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
    } else {
        return Ok(None);
    };
    if !matches!(column.data_type, DataType::Int64) {
        return Err(format!(
            "iceberg reserved row-lineage column {} expects Int64, got {:?}",
            column.name, column.data_type
        ));
    }
    let mut meta = std::collections::HashMap::new();
    meta.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    Ok(Some(
        Field::new(column.name.clone(), DataType::Int64, column.nullable).with_metadata(meta),
    ))
}

pub fn apply_field_id_recursive(
    field: Field,
    schema_field: &IcebergSchemaFieldDescriptor,
) -> Result<Field, String> {
    let field_id = schema_field
        .field_id
        .ok_or_else(|| format!("iceberg schema field {} missing field_id", field.name()))?;
    let mut meta = field.metadata().clone();
    meta.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    if let Some(json) = schema_field.initial_default_json.as_ref() {
        meta.insert(ICEBERG_INITIAL_DEFAULT_META_KEY.to_string(), json.clone());
    }
    let data_type = match field.data_type() {
        DataType::Struct(children) => {
            let schema_children = &schema_field.children;
            if children.len() != schema_children.len() {
                return Err(format!(
                    "iceberg schema children mismatch for {}: fields={} schema_fields={}",
                    field.name(),
                    children.len(),
                    schema_children.len()
                ));
            }
            let mut new_children = Vec::with_capacity(children.len());
            for (child, schema_child) in children.iter().zip(schema_children.iter()) {
                let new_child = apply_field_id_recursive(child.as_ref().clone(), schema_child)?;
                new_children.push(new_child);
            }
            DataType::Struct(new_children.into())
        }
        DataType::List(child) => {
            let schema_children = &schema_field.children;
            if schema_children.len() != 1 {
                return Err(format!(
                    "iceberg schema list field {} should have 1 child, got {}",
                    field.name(),
                    schema_children.len()
                ));
            }
            let new_child = apply_field_id_recursive(child.as_ref().clone(), &schema_children[0])?;
            DataType::List(Arc::new(new_child))
        }
        DataType::Map(entries, sorted) => {
            let schema_children = &schema_field.children;
            if schema_children.len() != 2 {
                return Err(format!(
                    "iceberg schema map field {} should have 2 children, got {}",
                    field.name(),
                    schema_children.len()
                ));
            }
            let entries_field = entries.as_ref();
            let entry_fields = match entries_field.data_type() {
                DataType::Struct(fields) => fields,
                _ => {
                    return Err(format!(
                        "iceberg map field {} has non-struct entries",
                        field.name()
                    ));
                }
            };
            if entry_fields.len() != 2 {
                return Err(format!(
                    "iceberg map field {} entries should have 2 fields",
                    field.name()
                ));
            }
            let key_field =
                apply_field_id_recursive(entry_fields[0].as_ref().clone(), &schema_children[0])?;
            let value_field =
                apply_field_id_recursive(entry_fields[1].as_ref().clone(), &schema_children[1])?;
            let entries_struct = DataType::Struct(vec![key_field, value_field].into());
            let entries_field = Field::new(
                entries_field.name(),
                entries_struct,
                entries_field.is_nullable(),
            );
            DataType::Map(Arc::new(entries_field), *sorted)
        }
        other => other.clone(),
    };
    Ok(Field::new(field.name(), data_type, field.is_nullable()).with_metadata(meta))
}

fn apply_descriptor_field_id_recursive(
    field: Field,
    schema_field: &DescriptorIcebergSchemaField,
) -> Result<Field, String> {
    let field_id = schema_field
        .field_id
        .ok_or_else(|| format!("iceberg schema field {} missing field_id", field.name()))?;
    let mut meta = field.metadata().clone();
    meta.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    if let Some(json) = schema_field.initial_default_json.as_ref() {
        meta.insert(ICEBERG_INITIAL_DEFAULT_META_KEY.to_string(), json.clone());
    }
    let data_type = match field.data_type() {
        DataType::Struct(children) => {
            let schema_children = schema_field
                .children
                .as_ref()
                .ok_or_else(|| format!("iceberg schema field {} missing children", field.name()))?;
            if children.len() != schema_children.len() {
                return Err(format!(
                    "iceberg schema children mismatch for {}: fields={} schema_fields={}",
                    field.name(),
                    children.len(),
                    schema_children.len()
                ));
            }
            let mut new_children = Vec::with_capacity(children.len());
            for (child, schema_child) in children.iter().zip(schema_children.iter()) {
                let new_child =
                    apply_descriptor_field_id_recursive(child.as_ref().clone(), schema_child)?;
                new_children.push(new_child);
            }
            DataType::Struct(new_children.into())
        }
        DataType::List(child) => {
            let schema_children = schema_field
                .children
                .as_ref()
                .ok_or_else(|| format!("iceberg schema field {} missing children", field.name()))?;
            if schema_children.len() != 1 {
                return Err(format!(
                    "iceberg schema list field {} should have 1 child, got {}",
                    field.name(),
                    schema_children.len()
                ));
            }
            let new_child =
                apply_descriptor_field_id_recursive(child.as_ref().clone(), &schema_children[0])?;
            DataType::List(Arc::new(new_child))
        }
        DataType::Map(entries, sorted) => {
            let schema_children = schema_field
                .children
                .as_ref()
                .ok_or_else(|| format!("iceberg schema field {} missing children", field.name()))?;
            if schema_children.len() != 2 {
                return Err(format!(
                    "iceberg schema map field {} should have 2 children, got {}",
                    field.name(),
                    schema_children.len()
                ));
            }
            let entries_field = entries.as_ref();
            let entry_fields = match entries_field.data_type() {
                DataType::Struct(fields) => fields,
                _ => {
                    return Err(format!(
                        "iceberg map field {} has non-struct entries",
                        field.name()
                    ));
                }
            };
            if entry_fields.len() != 2 {
                return Err(format!(
                    "iceberg map field {} entries should have 2 fields",
                    field.name()
                ));
            }
            let key_field = apply_descriptor_field_id_recursive(
                entry_fields[0].as_ref().clone(),
                &schema_children[0],
            )?;
            let value_field = apply_descriptor_field_id_recursive(
                entry_fields[1].as_ref().clone(),
                &schema_children[1],
            )?;
            let entries_struct = DataType::Struct(vec![key_field, value_field].into());
            let entries_field = Field::new(
                entries_field.name(),
                entries_struct,
                entries_field.is_nullable(),
            );
            DataType::Map(Arc::new(entries_field), *sorted)
        }
        other => other.clone(),
    };
    Ok(Field::new(field.name(), data_type, field.is_nullable()).with_metadata(meta))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    fn schema_field(
        name: &str,
        field_id: i32,
        children: Vec<IcebergSchemaFieldDescriptor>,
    ) -> IcebergSchemaFieldDescriptor {
        IcebergSchemaFieldDescriptor {
            name: name.to_string(),
            field_id: Some(field_id),
            children,
            initial_default_json: None,
        }
    }

    #[test]
    fn apply_field_id_recursive_writes_initial_default_metadata() {
        let mut schema_field = schema_field("c", 1, Vec::new());
        schema_field.initial_default_json = Some("5".to_string());

        let field = Field::new("c", DataType::Int32, true);
        let updated = apply_field_id_recursive(field, &schema_field).expect("apply");
        assert_eq!(
            updated.metadata().get(ICEBERG_INITIAL_DEFAULT_META_KEY),
            Some(&"5".to_string())
        );
    }

    #[test]
    fn apply_field_id_recursive_omits_initial_default_when_absent() {
        let schema_field = schema_field("c", 1, Vec::new());
        let field = Field::new("c", DataType::Int32, true);
        let updated = apply_field_id_recursive(field, &schema_field).expect("apply");
        assert_eq!(
            updated.metadata().get(ICEBERG_INITIAL_DEFAULT_META_KEY),
            None
        );
    }

    #[test]
    fn projected_schema_accepts_internal_change_op_column() {
        let iceberg = IcebergTableDescriptor {
            columns: Vec::new(),
            iceberg_schema: Some(IcebergSchemaDescriptor {
                fields: vec![schema_field("id", 1, Vec::new())],
            }),
            equality_delete_schema: None,
            partition_info: Vec::new(),
            current_snapshot_id: None,
            serialized_metadata: None,
        };

        let projected = build_projected_output_schema(
            &iceberg,
            &[
                IcebergArrowColumn {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                IcebergArrowColumn {
                    name: crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
                    data_type: DataType::Int8,
                    nullable: false,
                },
            ],
        )
        .expect("projected schema")
        .expect("schema");

        let change_op = projected
            .field_with_name(crate::exec::change_op::CHANGE_OP_COLUMN)
            .expect("change op field");
        assert_eq!(change_op.data_type(), &DataType::Int8);
        assert!(!change_op.is_nullable());
        assert!(
            !change_op
                .metadata()
                .contains_key(parquet::arrow::PARQUET_FIELD_ID_META_KEY),
            "__change_op is synthetic and must not claim an Iceberg field id"
        );
    }

    #[test]
    fn full_schema_uses_domain_columns_without_thrift_conversion() {
        let iceberg = IcebergTableDescriptor {
            columns: vec![IcebergTableColumn {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            iceberg_schema: Some(IcebergSchemaDescriptor {
                fields: vec![schema_field("id", 11, Vec::new())],
            }),
            equality_delete_schema: None,
            partition_info: Vec::new(),
            current_snapshot_id: None,
            serialized_metadata: None,
        };

        let schema = build_full_output_schema(&iceberg).expect("schema");

        assert_eq!(schema.fields().len(), 1);
        let field = schema.field(0);
        assert_eq!(field.name(), "id");
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(!field.is_nullable());
        assert_eq!(
            field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"11".to_string())
        );
    }
}
