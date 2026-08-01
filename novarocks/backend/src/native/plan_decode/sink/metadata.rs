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

use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::{Field, Schema};
use iceberg::spec::TableMetadata;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::Compression;

use super::super::error::NativeFragmentLeafDecodeError;
use crate::native::type_decode::decode_type;
use novarocks::connector::iceberg::delete_file::IcebergFileFormat;
use novarocks::connector::iceberg::schema::{
    IcebergSchemaDescriptor, IcebergSchemaFieldDescriptor, IcebergTableColumn,
    IcebergTableDescriptor,
};
use novarocks::connector::iceberg::sink_plan::{IcebergSinkMode, IcebergSinkObjectStoreConfig};
use novarocks::exec::row_position::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use novarocks::fs::object_store_credentials::{
    ObjectStoreCredentials, ObjectStoreCredentialsSource,
};
use novarocks::protocol::common::error::ProtocolErrorKind;
use novarocks_protocol::plan;

pub(crate) fn iceberg_table_descriptor_from_native(
    table: &plan::IcebergTableInfo,
    target_columns: &[plan::ColumnDef],
    mode: IcebergSinkMode,
) -> Result<IcebergTableDescriptor, NativeFragmentLeafDecodeError> {
    let schema = table.schema.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "iceberg",
            "native Iceberg write sink target schema missing",
        )
        .append_field("schema")
    })?;
    let iceberg_schema = IcebergSchemaDescriptor {
        fields: schema
            .fields
            .iter()
            .map(iceberg_schema_field_descriptor_from_native)
            .collect(),
    };
    let columns = target_columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            column_def_to_table_column(column).map_err(|error| {
                error
                    .prepend_index(index)
                    .prepend_field("columns")
                    .prepend_field("target_table")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let equality_delete_schema =
        (mode == IcebergSinkMode::EqualityDeletes).then_some(IcebergSchemaDescriptor {
            fields: iceberg_schema
                .fields
                .iter()
                .filter(|field| columns.iter().any(|column| column.name == field.name))
                .cloned()
                .collect(),
        });
    Ok(IcebergTableDescriptor {
        columns,
        iceberg_schema: Some(iceberg_schema),
        equality_delete_schema,
        partition_info: Vec::new(),
        current_snapshot_id: table.current_snapshot_id,
        serialized_metadata: table.serialized_metadata.clone(),
    })
}

fn iceberg_schema_field_descriptor_from_native(
    field: &plan::IcebergSchemaFieldDef,
) -> IcebergSchemaFieldDescriptor {
    IcebergSchemaFieldDescriptor {
        name: field.name.clone(),
        field_id: Some(field.field_id),
        children: field
            .children
            .iter()
            .map(iceberg_schema_field_descriptor_from_native)
            .collect(),
        initial_default_json: field.initial_default_json.clone(),
    }
}

fn column_def_to_table_column(
    column: &plan::ColumnDef,
) -> Result<IcebergTableColumn, NativeFragmentLeafDecodeError> {
    let data_type = column
        .data_type
        .as_ref()
        .ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "data_type",
                format!("native Iceberg column {} missing data_type", column.name),
            )
        })
        .and_then(|wire| {
            decode_type(wire).map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "data_type",
                    error,
                )
            })
        })?;
    Ok(IcebergTableColumn {
        name: column.name.clone(),
        data_type,
        nullable: column.nullable,
    })
}

pub(crate) fn parse_target_table_metadata(
    iceberg: &IcebergTableDescriptor,
    mode: IcebergSinkMode,
) -> Result<Option<TableMetadata>, NativeFragmentLeafDecodeError> {
    let serialized = match mode {
        IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors => {
            Some(iceberg.serialized_metadata.as_ref().ok_or_else(|| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::MissingField,
                    "serialized_metadata",
                    format!(
                        "native Iceberg {:?} sink requires serialized target table metadata",
                        mode
                    ),
                )
            })?)
        }
        IcebergSinkMode::Data | IcebergSinkMode::EqualityDeletes => {
            iceberg.serialized_metadata.as_ref()
        }
    };
    let Some(serialized) = serialized else {
        return Ok(None);
    };
    serde_json::from_str::<TableMetadata>(serialized)
        .map(Some)
        .map_err(|e| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "serialized_metadata",
                format!(
                    "parse native Iceberg {:?} target metadata failed: {e}",
                    mode
                ),
            )
        })
}

pub(crate) fn iceberg_table_location(serialized_metadata: Option<&str>) -> Option<String> {
    let serialized = serialized_metadata?;
    let value = serde_json::from_str::<serde_json::Value>(serialized).ok()?;
    value
        .get("location")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

pub(super) fn arrow_field_id(field: &Field) -> Result<i32, NativeFragmentLeafDecodeError> {
    let raw = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "field_id",
                format!(
                    "native Iceberg sink field {} is missing parquet field id metadata",
                    field.name()
                ),
            )
        })?;
    raw.parse::<i32>().map_err(|e| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "field_id",
            format!(
                "native Iceberg sink field {} has invalid parquet field id {raw}: {e}",
                field.name()
            ),
        )
    })
}

pub(super) fn schema_has_reserved_row_lineage_columns(
    schema: &Schema,
) -> Result<bool, NativeFragmentLeafDecodeError> {
    let mut has_row_id = false;
    let mut has_last_updated = false;
    for field in schema.fields() {
        if field.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL) {
            has_row_id = matches!(arrow_field_id(field), Ok(ICEBERG_RESERVED_FIELD_ID_ROW_ID));
        } else if field
            .name()
            .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
        {
            has_last_updated = matches!(
                arrow_field_id(field),
                Ok(ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
            );
        }
    }
    Ok(has_row_id && has_last_updated)
}

pub(crate) fn resolve_native_sink_s3_config(
    data_location: &str,
    cloud_properties: &HashMap<String, String>,
) -> Result<Option<IcebergSinkObjectStoreConfig>, NativeFragmentLeafDecodeError> {
    if !novarocks_fs::is_object_store_location_parse_only(data_location).map_err(|e| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "data_location",
            format!("parse native Iceberg sink data_location {data_location}: {e}"),
        )
    })? {
        return Ok(None);
    }
    let (bucket, _data_root) = novarocks_fs::parse_object_store_path_parse_only(data_location)
        .map_err(|e| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "data_location",
                format!(
                    "parse native Iceberg sink object-store data_location {data_location}: {e}"
                ),
            )
        })?;
    if cloud_properties.is_empty() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "cloud_properties",
            format!(
                "native Iceberg sink object-store path requires cloud_properties: data_location={data_location}"
            ),
        ));
    }
    let cloud_properties = cloud_properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let credentials = ObjectStoreCredentials::from_aws_s3_properties(
        ObjectStoreCredentialsSource::IcebergSinkCloudProperties,
        &cloud_properties,
    )
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "cloud_properties",
            error,
        )
    })?;
    Ok(Some(IcebergSinkObjectStoreConfig::from_credentials(
        bucket,
        credentials,
    )))
}

pub(crate) fn validate_iceberg_sink_file_format(
    file_format: &str,
) -> Result<(IcebergFileFormat, String), NativeFragmentLeafDecodeError> {
    if !file_format.eq_ignore_ascii_case("parquet") {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::Unsupported,
            "file_format",
            format!(
                "native Iceberg sink does not support {file_format} files; only Parquet is supported"
            ),
        ));
    }
    Ok((IcebergFileFormat::Parquet, file_format.to_string()))
}

pub(super) fn map_native_compression(
    value: i32,
) -> Result<Compression, NativeFragmentLeafDecodeError> {
    let compression = plan::IcebergWriteFileCompression::try_from(value).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "compression",
            format!("unknown native IcebergWriteFileCompression value {value}"),
        )
    })?;
    match compression {
        plan::IcebergWriteFileCompression::Snappy => Ok(Compression::SNAPPY),
        plan::IcebergWriteFileCompression::Unspecified => {
            Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidEnum,
                "compression",
                "native Iceberg write file compression is unspecified",
            ))
        }
    }
}
