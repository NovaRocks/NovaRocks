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

//! Provider-owned reconstruction of a DATA writer context from frozen handle facts.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef as ArrowSchemaRef};

use crate::access_binding::IcebergReadBinding;
use crate::commit::data_writer::{PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY, StagedWriteContext};
use crate::commit::write_io::build_staged_file_io;
use crate::iceberg::spec::{
    ListType, MapType, NestedField, PartitionSpec, PrimitiveType, SortOrder, StructType,
    TableMetadata, TableMetadataBuilder, Transform, Type, UnboundPartitionSpec,
};
use crate::scan_model::IcebergSchemaDef;
use crate::schema_mapping::annotate_schema_from_scan_model;

/// Secret-free DATA writer facts decoded from one exact-generation handle.
#[derive(Clone, Debug)]
pub struct FrozenDataWriteFacts {
    pub table_location: String,
    pub data_location: String,
    pub target_partition_spec_id: i32,
    pub partition_source_column_names: Vec<String>,
    pub partition_column_names: Vec<String>,
    pub transform_exprs: Vec<String>,
    pub data_input_schema: IcebergSchemaDef,
    pub parquet_row_group_size_bytes: Option<u64>,
}

/// Build the provider writer context exclusively from the sealed handle and
/// local execution binding.  No FE plan, credentials, or Core runtime state is
/// reconstructed here.
pub fn staged_write_context_from_frozen_facts(
    binding: &IcebergReadBinding,
    input_schema: &ArrowSchemaRef,
    facts: FrozenDataWriteFacts,
) -> Result<StagedWriteContext, String> {
    let annotated_schema = annotate_schema_from_scan_model(input_schema, &facts.data_input_schema)?;
    let writer_schema = Arc::new(iceberg_schema_from_arrow_schema(annotated_schema.as_ref())?);
    let annotated_schema = scalar_integer_storage_schema(&annotated_schema);
    let metadata = build_target_table_metadata(&facts, writer_schema.as_ref())?;
    let file_io = build_staged_file_io(binding, &facts.data_location)?;
    StagedWriteContext::from_parts_with_partition_spec_id(
        metadata,
        file_io,
        writer_schema,
        annotated_schema,
        facts.target_partition_spec_id,
    )
}

/// Iceberg INT has a four-byte storage carrier even when the signed SQL input
/// is TINYINT or SMALLINT. The writer's existing reannotation performs the
/// lossless conversion; the Parquet schema must describe the same carrier.
pub(super) fn scalar_integer_storage_schema(schema: &ArrowSchemaRef) -> ArrowSchemaRef {
    fn field(field: &Field) -> Arc<Field> {
        Arc::new(field.clone().with_data_type(data_type(field.data_type())))
    }
    fn data_type(value: &DataType) -> DataType {
        match value {
            DataType::Int8 | DataType::Int16 => DataType::Int32,
            DataType::Struct(fields) => {
                DataType::Struct(fields.iter().map(|value| field(value)).collect())
            }
            DataType::List(element) => DataType::List(field(element)),
            DataType::Map(entries, sorted) => DataType::Map(field(entries), *sorted),
            other => other.clone(),
        }
    }
    Arc::new(arrow::datatypes::Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|value| field(value))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

fn build_target_table_metadata(
    facts: &FrozenDataWriteFacts,
    writer_schema: &crate::iceberg::spec::Schema,
) -> Result<TableMetadata, String> {
    let partition_spec = build_staged_partition_spec(
        writer_schema,
        facts.target_partition_spec_id,
        &facts.partition_source_column_names,
        &facts.partition_column_names,
        &facts.transform_exprs,
    )?;
    let mut properties = std::collections::HashMap::new();
    properties.insert("write.data.path".to_string(), facts.data_location.clone());
    if let Some(value) = facts.parquet_row_group_size_bytes {
        properties.insert(
            PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY.to_string(),
            value.to_string(),
        );
    }
    TableMetadataBuilder::new(
        writer_schema.clone(),
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        facts.table_location.clone(),
        crate::iceberg::spec::FormatVersion::V2,
        properties,
    )
    .map_err(|error| format!("build staged Iceberg table metadata: {error}"))?
    .add_current_schema(writer_schema.clone())
    .map_err(|error| format!("add staged Iceberg writer schema: {error}"))?
    .add_default_partition_spec(partition_spec)
    .map_err(|error| format!("add staged Iceberg partition spec: {error}"))?
    .build()
    .map_err(|error| format!("finalize staged Iceberg table metadata: {error}"))
    .and_then(|built| {
        retag_default_partition_spec_id(
            built.metadata,
            facts.target_partition_spec_id,
            &facts.partition_column_names,
        )
    })
}

fn iceberg_schema_from_arrow_schema(
    schema: &Schema,
) -> Result<crate::iceberg::spec::Schema, String> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| iceberg_nested_field_from_arrow_field(field.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    crate::iceberg::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .map_err(|error| format!("build staged Iceberg writer schema: {error}"))
}

fn iceberg_nested_field_from_arrow_field(
    field: &Field,
) -> Result<crate::iceberg::spec::NestedFieldRef, String> {
    let field_id = crate::schema_mapping::field_id_for_arrow_field(field)?.ok_or_else(|| {
        format!(
            "Iceberg writer field {} is missing parquet field ID metadata",
            field.name()
        )
    })?;
    Ok(Arc::new(NestedField::new(
        field_id,
        field.name(),
        iceberg_type_from_arrow_type(field.data_type())?,
        !field.is_nullable(),
    )))
}

fn iceberg_type_from_arrow_type(data_type: &DataType) -> Result<Type, String> {
    use arrow::datatypes::TimeUnit;

    let primitive = match data_type {
        DataType::Boolean => Some(PrimitiveType::Boolean),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Some(PrimitiveType::Int),
        DataType::Int64 => Some(PrimitiveType::Long),
        DataType::Float32 => Some(PrimitiveType::Float),
        DataType::Float64 => Some(PrimitiveType::Double),
        DataType::Decimal128(precision, scale) => Some(PrimitiveType::Decimal {
            precision: (*precision).into(),
            scale: u32::try_from(*scale).map_err(|_| {
                format!("Iceberg writer decimal scale {scale} cannot convert to u32")
            })?,
        }),
        DataType::Date32 => Some(PrimitiveType::Date),
        DataType::Time64(TimeUnit::Microsecond) => Some(PrimitiveType::Time),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some(PrimitiveType::Timestamp),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Some(PrimitiveType::Timestamptz),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(PrimitiveType::TimestampNs),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => Some(PrimitiveType::TimestamptzNs),
        DataType::Utf8 | DataType::LargeUtf8 => Some(PrimitiveType::String),
        DataType::Binary => Some(PrimitiveType::Binary),
        DataType::LargeBinary => Some(PrimitiveType::Variant),
        DataType::FixedSizeBinary(size) => {
            Some(PrimitiveType::Fixed(u64::try_from(*size).map_err(
                |_| format!("Iceberg writer fixed binary width {size} cannot convert to u64"),
            )?))
        }
        _ => None,
    };
    if let Some(primitive) = primitive {
        return Ok(Type::Primitive(primitive));
    }
    match data_type {
        DataType::Struct(fields) => Ok(Type::Struct(StructType::new(
            fields
                .iter()
                .map(|field| iceberg_nested_field_from_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::List(element) => Ok(Type::List(ListType::new(
            iceberg_nested_field_from_arrow_field(element.as_ref())?,
        ))),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(format!(
                    "Iceberg MAP entries field must be Struct, got {:?}",
                    entries.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "Iceberg MAP entries Struct must have 2 fields, got {}",
                    fields.len()
                ));
            }
            Ok(Type::Map(MapType::new(
                iceberg_nested_field_from_arrow_field(fields[0].as_ref())?,
                iceberg_nested_field_from_arrow_field(fields[1].as_ref())?,
            )))
        }
        other => Err(format!(
            "unsupported Arrow type for staged Iceberg writer schema: {other:?}"
        )),
    }
}

fn build_staged_partition_spec(
    schema: &crate::iceberg::spec::Schema,
    partition_spec_id: i32,
    source_column_names: &[String],
    partition_column_names: &[String],
    transform_exprs: &[String],
) -> Result<UnboundPartitionSpec, String> {
    if source_column_names.len() != partition_column_names.len()
        || source_column_names.len() != transform_exprs.len()
    {
        return Err(format!(
            "Iceberg writer partition metadata mismatch: sources={} names={} transforms={}",
            source_column_names.len(),
            partition_column_names.len(),
            transform_exprs.len()
        ));
    }
    let mut builder = UnboundPartitionSpec::builder().with_spec_id(partition_spec_id);
    for ((source_name, partition_name), transform_expr) in source_column_names
        .iter()
        .zip(partition_column_names.iter())
        .zip(transform_exprs.iter())
    {
        let field = schema
            .field_by_name_case_insensitive(source_name)
            .ok_or_else(|| {
                format!(
                    "Iceberg writer partition source column {source_name} is missing from schema"
                )
            })?;
        builder = builder
            .add_partition_field(
                field.id,
                partition_name,
                parse_partition_transform(transform_expr)?,
            )
            .map_err(|error| format!("build staged Iceberg partition field: {error}"))?;
    }
    Ok(builder.build())
}

fn parse_partition_transform(raw: &str) -> Result<Transform, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "identity" => Ok(Transform::Identity),
        "year" => Ok(Transform::Year),
        "month" => Ok(Transform::Month),
        "day" => Ok(Transform::Day),
        "hour" => Ok(Transform::Hour),
        "void" => Ok(Transform::Void),
        _ => {
            if let Some(width) = parse_transform_arg(&normalized, "bucket")? {
                return Ok(Transform::Bucket(width));
            }
            if let Some(width) = parse_transform_arg(&normalized, "truncate")? {
                return Ok(Transform::Truncate(width));
            }
            Err(format!(
                "unsupported Iceberg partition transform for writer: {raw}"
            ))
        }
    }
}

fn parse_transform_arg(raw: &str, name: &str) -> Result<Option<u32>, String> {
    let Some(rest) = raw.strip_prefix(name) else {
        return Ok(None);
    };
    let Some(rest) = rest
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
    else {
        return Err(format!(
            "Iceberg partition transform {raw} must use {name}[N] syntax"
        ));
    };
    let value = rest.parse::<u32>().map_err(|error| {
        format!("Iceberg partition transform {raw} has invalid numeric argument: {error}")
    })?;
    if value == 0 {
        return Err(format!(
            "Iceberg partition transform {raw} requires a positive numeric argument"
        ));
    }
    Ok(Some(value))
}

fn retag_default_partition_spec_id(
    metadata: TableMetadata,
    target_spec_id: i32,
    partition_column_names: &[String],
) -> Result<TableMetadata, String> {
    let mut value = serde_json::to_value(metadata)
        .map_err(|error| format!("serialize staged Iceberg table metadata: {error}"))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| "staged Iceberg table metadata must serialize to an object".to_string())?;
    object.insert(
        "default-spec-id".to_string(),
        serde_json::Value::from(target_spec_id),
    );
    let specs = object
        .get_mut("partition-specs")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| "staged Iceberg table metadata is missing partition-specs".to_string())?;
    let index = specs
        .iter()
        .position(|spec| partition_spec_names_match(spec, partition_column_names))
        .ok_or_else(|| {
            format!(
                "staged Iceberg metadata is missing partition fields {partition_column_names:?}"
            )
        })?;
    let mut spec = specs[index].clone();
    spec.as_object_mut()
        .ok_or_else(|| "staged Iceberg partition spec must be an object".to_string())?
        .insert(
            "spec-id".to_string(),
            serde_json::Value::from(target_spec_id),
        );
    *specs = vec![spec];
    let metadata: TableMetadata = serde_json::from_value(value)
        .map_err(|error| format!("deserialize staged Iceberg table metadata: {error}"))?;
    let spec = metadata
        .partition_spec_by_id(target_spec_id)
        .ok_or_else(|| {
            format!("staged Iceberg metadata failed to retain partition spec {target_spec_id}")
        })?;
    if metadata.default_partition_spec_id() != target_spec_id
        || spec
            .fields()
            .iter()
            .map(|field| &field.name)
            .ne(partition_column_names.iter())
    {
        return Err(
            "staged Iceberg metadata partition spec does not match frozen handle".to_string(),
        );
    }
    Ok(metadata)
}

fn partition_spec_names_match(spec: &serde_json::Value, names: &[String]) -> bool {
    let Some(fields) = spec
        .as_object()
        .and_then(|object| object.get("fields"))
        .and_then(serde_json::Value::as_array)
    else {
        return names.is_empty();
    };
    fields.len() == names.len()
        && fields.iter().zip(names).all(|(field, expected)| {
            field
                .as_object()
                .and_then(|object| object.get("name"))
                .and_then(serde_json::Value::as_str)
                == Some(expected.as_str())
        })
}

#[cfg(test)]
#[path = "frozen_write_tests.rs"]
mod tests;
