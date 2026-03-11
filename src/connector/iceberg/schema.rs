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

use crate::descriptors;
use crate::lower::type_lowering::arrow_type_from_desc;

const VIRTUAL_COUNT_COLUMN: &str = "___count___";

#[derive(Clone, Debug)]
pub struct IcebergArrowColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

pub fn build_full_output_schema(iceberg: &descriptors::TIcebergTable) -> Result<SchemaRef, String> {
    let columns = iceberg
        .columns
        .as_ref()
        .ok_or_else(|| "iceberg table missing columns".to_string())?;
    if let Some(schema) = iceberg.iceberg_schema.as_ref() {
        let schema_fields = schema
            .fields
            .as_ref()
            .ok_or_else(|| "iceberg schema missing fields".to_string())?;
        let mut fields = Vec::with_capacity(schema_fields.len());
        for schema_field in schema_fields {
            let name = schema_field
                .name
                .as_ref()
                .ok_or_else(|| "iceberg schema field missing name".to_string())?;
            let col = columns
                .iter()
                .find(|c| &c.column_name == name)
                .ok_or_else(|| {
                    format!("iceberg schema field {} missing column descriptor", name)
                })?;
            let dtype = col
                .type_desc
                .as_ref()
                .and_then(arrow_type_from_desc)
                .ok_or_else(|| format!("iceberg column {} missing type_desc", col.column_name))?;
            let nullable = col.is_allow_null.unwrap_or(true);
            let field = Field::new(col.column_name.clone(), dtype, nullable);
            let field = apply_field_id_recursive(field, schema_field)?;
            fields.push(field);
        }
        return Ok(Arc::new(Schema::new(fields)));
    }

    let mut fields = Vec::with_capacity(columns.len());
    for col in columns {
        let dtype = col
            .type_desc
            .as_ref()
            .and_then(arrow_type_from_desc)
            .ok_or_else(|| format!("iceberg column {} missing type_desc", col.column_name))?;
        let nullable = col.is_allow_null.unwrap_or(true);
        fields.push(Field::new(col.column_name.clone(), dtype, nullable));
    }

    Ok(Arc::new(Schema::new(fields)))
}

pub fn build_projected_output_schema(
    iceberg: &descriptors::TIcebergTable,
    columns: &[IcebergArrowColumn],
) -> Result<Option<SchemaRef>, String> {
    let Some(schema) = iceberg.iceberg_schema.as_ref() else {
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
        let field = apply_field_id_recursive(field, schema_field)?;
        fields.push(field);
    }
    Ok(Some(Arc::new(Schema::new(fields))))
}

pub fn apply_field_id_recursive(
    field: Field,
    schema_field: &descriptors::TIcebergSchemaField,
) -> Result<Field, String> {
    let field_id = schema_field
        .field_id
        .ok_or_else(|| format!("iceberg schema field {} missing field_id", field.name()))?;
    let mut meta = field.metadata().clone();
    meta.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
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
                let new_child = apply_field_id_recursive(child.as_ref().clone(), schema_child)?;
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
            let new_child = apply_field_id_recursive(child.as_ref().clone(), &schema_children[0])?;
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
