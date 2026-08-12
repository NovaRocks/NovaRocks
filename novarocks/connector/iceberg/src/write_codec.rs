// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Provider-owned canonical payloads for Iceberg write handles and reports.

use std::collections::BTreeMap;

use arrow::datatypes::{DataType, SchemaRef};
use base64::Engine;
use bytes::Bytes;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use serde::{Deserialize, Serialize};

use novarocks_spi::connector::{
    CONNECTOR_WRITE_CONTRACT_VERSION, ConnectorCommittedVersion, ConnectorStagedReport,
    ConnectorStagedReportSummary, ConnectorWriteReceipt, ConnectorWriterIdentity,
    ConnectorWriterTerminalState,
};

use crate::commit::report::{
    IcebergColumnStats, IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
};
use crate::commit::{DeletionVector, EqualityDeleteColumn};
use crate::delete_file::IcebergFileContent;
use crate::delete_file::IcebergFileFormat;
use crate::iceberg::spec::TableMetadata;
use crate::scan_model::{IcebergSchemaDef, IcebergSchemaFieldDef};
use crate::write_descriptor::{IcebergPartitionDescriptor, IcebergPartitionValueDescriptor};
use crate::write_descriptor::{decode_partition_descriptor, encode_partition_descriptor};

pub const ICEBERG_WRITE_PAYLOAD_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergWriteHandleMode {
    Data,
    EqualityDeletes,
    PositionDeletes,
    DeletionVectors,
}

#[derive(Clone, Debug)]
pub struct IcebergWriteHandleInput {
    pub mode: IcebergWriteHandleMode,
    pub table_location: String,
    pub data_location: String,
    pub target_partition_spec_id: i32,
    pub target_snapshot_id: Option<i64>,
    pub file_format: IcebergFileFormat,
    pub report_file_format: String,
    pub compression: Compression,
    pub equality_delete_columns: Vec<EqualityDeleteColumn>,
    pub row_lineage_data: bool,
    pub partition_source_column_names: Vec<String>,
    pub partition_column_names: Vec<String>,
    pub transform_exprs: Vec<String>,
    pub data_input_schema: Option<IcebergSchemaDef>,
    pub position_delete_binding: Option<IcebergPositionDeleteBinding>,
    pub position_delete_partitions: Vec<IcebergPositionDeletePartitionInput>,
}

#[derive(Clone, Debug)]
pub struct IcebergPositionDeleteBinding {
    pub output_column_names: Vec<String>,
    pub partition_source_column_names: Vec<String>,
    pub partition_column_names: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct IcebergPositionDeletePartitionInput {
    pub data_file_path: String,
    pub partition_path: String,
    pub null_fingerprint: String,
    pub partition_spec_id: i32,
    pub descriptor: IcebergPartitionDescriptor,
    pub existing_deletion_vector_payload: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct IcebergDecodedWriteHandle {
    pub mode: IcebergWriteHandleMode,
    pub table_location: String,
    pub data_location: String,
    pub target_partition_spec_id: i32,
    pub target_snapshot_id: Option<i64>,
    pub file_format: IcebergFileFormat,
    pub report_file_format: String,
    pub compression: Compression,
    pub equality_delete_columns: Vec<IcebergDecodedEqualityDeleteColumn>,
    pub row_lineage_data: bool,
    pub partition_source_column_names: Vec<String>,
    pub partition_column_names: Vec<String>,
    pub transform_exprs: Vec<String>,
    pub data_input_schema: Option<IcebergSchemaDef>,
    pub position_delete_binding: Option<IcebergPositionDeleteBinding>,
    pub position_delete_partitions: BTreeMap<String, IcebergPositionDeletePartition>,
}

#[derive(Clone, Debug)]
pub struct IcebergDecodedEqualityDeleteColumn {
    pub name: String,
    pub field_id: i32,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct IcebergPositionDeletePartition {
    pub partition_path: String,
    pub null_fingerprint: String,
    pub partition_spec_id: i32,
    pub descriptor: IcebergPartitionDescriptor,
    pub existing_deletion_vector_payload: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct IcebergPositionDeleteHandle {
    pub mode: IcebergWriteHandleMode,
    pub data_location: String,
    pub report_file_format: String,
    pub compression: Compression,
    pub partitions: BTreeMap<String, IcebergPositionDeletePartition>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ModeV1 {
    Data,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FileFormatV1 {
    Parquet,
    Puffin,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IcebergFileContentV1 {
    Data,
    PositionDeletes,
    EqualityDeletes,
}

impl From<IcebergFileContent> for IcebergFileContentV1 {
    fn from(value: IcebergFileContent) -> Self {
        match value {
            IcebergFileContent::Data => Self::Data,
            IcebergFileContent::PositionDeletes => Self::PositionDeletes,
            IcebergFileContent::EqualityDeletes => Self::EqualityDeletes,
        }
    }
}

impl From<IcebergFileContentV1> for IcebergFileContent {
    fn from(value: IcebergFileContentV1) -> Self {
        match value {
            IcebergFileContentV1::Data => Self::Data,
            IcebergFileContentV1::PositionDeletes => Self::PositionDeletes,
            IcebergFileContentV1::EqualityDeletes => Self::EqualityDeletes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HandleV1 {
    version: u32,
    mode: ModeV1,
    table_location: String,
    data_location: String,
    target_partition_spec_id: i32,
    target_snapshot_id: Option<i64>,
    file_format: FileFormatV1,
    report_file_format: String,
    compression: String,
    equality_delete_columns: Vec<EqualityDeleteColumnV1>,
    row_lineage_data: bool,
    partition_source_column_names: Vec<String>,
    partition_column_names: Vec<String>,
    transform_exprs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data_input_schema: Option<Vec<SchemaFieldV1>>,
    position_delete_binding: Option<PositionDeleteBindingV1>,
    position_delete_partitions: Vec<PositionDeletePartitionV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EqualityDeleteColumnV1 {
    name: String,
    field_id: i32,
    data_type: String,
    nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaFieldV1 {
    field_id: i32,
    name: String,
    initial_default_json: Option<String>,
    write_default_json: Option<String>,
    children: Vec<SchemaFieldV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PositionDeleteBindingV1 {
    output_column_names: Vec<String>,
    partition_source_column_names: Vec<String>,
    partition_column_names: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PositionDeletePartitionV1 {
    data_file_path: String,
    partition_path: String,
    null_fingerprint: String,
    partition_spec_id: i32,
    values: Vec<PartitionValueV1>,
    existing_deletion_vector_payload_base64: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PartitionValueV1 {
    is_null: bool,
    datum_base64: Option<String>,
}

impl From<&IcebergSchemaFieldDef> for SchemaFieldV1 {
    fn from(field: &IcebergSchemaFieldDef) -> Self {
        Self {
            field_id: field.field_id,
            name: field.name.clone(),
            initial_default_json: field.initial_default_json.clone(),
            write_default_json: field.write_default_json.clone(),
            children: field.children.iter().map(Self::from).collect(),
        }
    }
}

impl From<&SchemaFieldV1> for IcebergSchemaFieldDef {
    fn from(field: &SchemaFieldV1) -> Self {
        Self {
            field_id: field.field_id,
            name: field.name.clone(),
            initial_default: None,
            write_default: None,
            initial_default_json: field.initial_default_json.clone(),
            write_default_json: field.write_default_json.clone(),
            children: field.children.iter().map(Self::from).collect(),
        }
    }
}

pub fn encode_write_handle(input: &IcebergWriteHandleInput) -> Result<Bytes, String> {
    validate_secret_free_text("table location", &input.table_location)?;
    validate_secret_free_text("data location", &input.data_location)?;
    for transform in &input.transform_exprs {
        validate_secret_free_text("partition transform", transform)?;
    }
    let partitions = input
        .position_delete_partitions
        .iter()
        .map(|partition| {
            validate_secret_free_text("referenced data file", &partition.data_file_path)?;
            if let Some(payload) = &partition.existing_deletion_vector_payload {
                DeletionVector::from_iceberg_payload(payload)
                    .map_err(|error| format!("decode frozen Iceberg deletion vector: {error}"))?;
            }
            Ok(PositionDeletePartitionV1 {
                data_file_path: partition.data_file_path.clone(),
                partition_path: partition.partition_path.clone(),
                null_fingerprint: partition.null_fingerprint.clone(),
                partition_spec_id: partition.partition_spec_id,
                values: partition
                    .descriptor
                    .values
                    .iter()
                    .map(|value| PartitionValueV1 {
                        is_null: value.is_null,
                        datum_base64: value.datum_bytes.as_ref().map(base64_encode),
                    })
                    .collect(),
                existing_deletion_vector_payload_base64: partition
                    .existing_deletion_vector_payload
                    .as_ref()
                    .map(base64_encode),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    canonical_json(&HandleV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        mode: match input.mode {
            IcebergWriteHandleMode::Data => ModeV1::Data,
            IcebergWriteHandleMode::EqualityDeletes => ModeV1::EqualityDeletes,
            IcebergWriteHandleMode::PositionDeletes => ModeV1::PositionDeletes,
            IcebergWriteHandleMode::DeletionVectors => ModeV1::DeletionVectors,
        },
        table_location: input.table_location.clone(),
        data_location: input.data_location.clone(),
        target_partition_spec_id: input.target_partition_spec_id,
        target_snapshot_id: input.target_snapshot_id,
        file_format: match input.file_format {
            IcebergFileFormat::Parquet => FileFormatV1::Parquet,
            IcebergFileFormat::Puffin => FileFormatV1::Puffin,
            IcebergFileFormat::Unknown => FileFormatV1::Unknown,
        },
        report_file_format: input.report_file_format.clone(),
        compression: encode_compression(input.compression),
        equality_delete_columns: input
            .equality_delete_columns
            .iter()
            .map(|column| EqualityDeleteColumnV1 {
                name: column.name.clone(),
                field_id: column.field_id,
                data_type: format!("{:?}", column.data_type),
                nullable: column.nullable,
            })
            .collect(),
        row_lineage_data: input.row_lineage_data,
        partition_source_column_names: input.partition_source_column_names.clone(),
        partition_column_names: input.partition_column_names.clone(),
        transform_exprs: input.transform_exprs.clone(),
        data_input_schema: input
            .data_input_schema
            .as_ref()
            .map(|schema| schema.fields.iter().map(SchemaFieldV1::from).collect()),
        position_delete_binding: input.position_delete_binding.as_ref().map(|binding| {
            PositionDeleteBindingV1 {
                output_column_names: binding.output_column_names.clone(),
                partition_source_column_names: binding.partition_source_column_names.clone(),
                partition_column_names: binding.partition_column_names.clone(),
            }
        }),
        position_delete_partitions: partitions,
    })
}

pub fn decode_write_handle(bytes: &[u8]) -> Result<IcebergDecodedWriteHandle, String> {
    let payload: HandleV1 = decode_json(bytes, "writer handle")?;
    if payload.version != ICEBERG_WRITE_PAYLOAD_VERSION {
        return Err(format!(
            "unsupported Iceberg writer handle payload version {}; expected {}",
            payload.version, ICEBERG_WRITE_PAYLOAD_VERSION
        ));
    }
    validate_secret_free_text("table location", &payload.table_location)?;
    validate_secret_free_text("data location", &payload.data_location)?;
    for transform in &payload.transform_exprs {
        validate_secret_free_text("partition transform", transform)?;
    }
    ensure_canonical_json(bytes, &payload, "writer handle")?;
    let mut partitions = BTreeMap::new();
    for partition in payload.position_delete_partitions {
        validate_secret_free_text("referenced data file", &partition.data_file_path)?;
        let existing = partition
            .existing_deletion_vector_payload_base64
            .as_deref()
            .map(|value| base64_decode(value, "frozen deletion vector"))
            .transpose()?;
        if let Some(payload) = &existing {
            DeletionVector::from_iceberg_payload(payload)
                .map_err(|error| format!("decode frozen Iceberg deletion vector: {error}"))?;
        }
        let descriptor = IcebergPartitionDescriptor { values: partition.values.into_iter().enumerate().map(|(index, value)| match (value.is_null, value.datum_base64) { (true, None) => Ok(IcebergPartitionValueDescriptor { is_null: true, datum_bytes: None }), (false, Some(value)) => Ok(IcebergPartitionValueDescriptor { is_null: false, datum_bytes: Some(base64_decode(&value, "position-delete partition datum")?) }), (true, Some(_)) => Err(format!("Iceberg position-delete partition descriptor value {index} is null but carries a payload")), (false, None) => Err(format!("Iceberg position-delete partition descriptor value {index} is non-null but has no payload")) }).collect::<Result<Vec<_>, String>>()? };
        if partitions
            .insert(
                partition.data_file_path,
                IcebergPositionDeletePartition {
                    partition_path: partition.partition_path,
                    null_fingerprint: partition.null_fingerprint,
                    partition_spec_id: partition.partition_spec_id,
                    descriptor,
                    existing_deletion_vector_payload: existing,
                },
            )
            .is_some()
        {
            return Err(
                "Iceberg position-delete handle contains duplicate data-file paths".to_string(),
            );
        }
    }
    Ok(IcebergDecodedWriteHandle {
        mode: match payload.mode {
            ModeV1::Data => IcebergWriteHandleMode::Data,
            ModeV1::EqualityDeletes => IcebergWriteHandleMode::EqualityDeletes,
            ModeV1::PositionDeletes => IcebergWriteHandleMode::PositionDeletes,
            ModeV1::DeletionVectors => IcebergWriteHandleMode::DeletionVectors,
        },
        table_location: payload.table_location,
        data_location: payload.data_location,
        target_partition_spec_id: payload.target_partition_spec_id,
        target_snapshot_id: payload.target_snapshot_id,
        file_format: match payload.file_format {
            FileFormatV1::Parquet => IcebergFileFormat::Parquet,
            FileFormatV1::Puffin => IcebergFileFormat::Puffin,
            FileFormatV1::Unknown => IcebergFileFormat::Unknown,
        },
        report_file_format: payload.report_file_format,
        compression: decode_compression(&payload.compression)?,
        equality_delete_columns: payload
            .equality_delete_columns
            .into_iter()
            .map(|column| IcebergDecodedEqualityDeleteColumn {
                name: column.name,
                field_id: column.field_id,
                data_type: column.data_type,
                nullable: column.nullable,
            })
            .collect(),
        row_lineage_data: payload.row_lineage_data,
        partition_source_column_names: payload.partition_source_column_names,
        partition_column_names: payload.partition_column_names,
        transform_exprs: payload.transform_exprs,
        data_input_schema: payload.data_input_schema.map(|fields| IcebergSchemaDef {
            fields: fields.iter().map(IcebergSchemaFieldDef::from).collect(),
        }),
        position_delete_binding: payload.position_delete_binding.map(|binding| {
            IcebergPositionDeleteBinding {
                output_column_names: binding.output_column_names,
                partition_source_column_names: binding.partition_source_column_names,
                partition_column_names: binding.partition_column_names,
            }
        }),
        position_delete_partitions: partitions,
    })
}

pub fn write_handle_mode(bytes: &[u8]) -> Result<IcebergWriteHandleMode, String> {
    Ok(decode_write_handle(bytes)?.mode)
}

/// Decode and validate the unpartitioned equality-delete facts against the
/// BE-local Arrow input schema.  The raw handle deliberately carries names,
/// field IDs and type renderings only; Arrow values never cross this boundary.
pub fn equality_delete_handle_from_payload(
    bytes: &[u8],
    input_schema: SchemaRef,
) -> Result<(String, i32, Vec<EqualityDeleteColumn>), String> {
    let payload = decode_write_handle(bytes)?;
    if payload.mode != IcebergWriteHandleMode::EqualityDeletes {
        return Err("Iceberg connector writer mode is not supported by the equality-delete execution adapter".to_string());
    }
    if !payload.partition_source_column_names.is_empty()
        || !payload.partition_column_names.is_empty()
        || !payload.transform_exprs.is_empty()
        || payload.equality_delete_columns.is_empty()
        || payload.equality_delete_columns.len() != input_schema.fields().len()
    {
        return Err("Iceberg connector equality-delete handle does not match the unpartitioned fragment input".to_string());
    }
    let columns = payload
        .equality_delete_columns
        .iter()
        .zip(input_schema.fields())
        .map(|(expected, actual)| {
            if expected.name != actual.name().as_str()
                || expected.data_type != format!("{:?}", actual.data_type())
                || expected.nullable != actual.is_nullable()
            {
                return Err(format!(
                    "Iceberg equality-delete handle column `{}` does not match fragment input `{}`",
                    expected.name,
                    actual.name()
                ));
            }
            Ok(EqualityDeleteColumn {
                name: expected.name.clone(),
                field_id: expected.field_id,
                data_type: actual.data_type().clone(),
                nullable: actual.is_nullable(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok((
        payload.data_location,
        payload.target_partition_spec_id,
        columns,
    ))
}

/// Decode frozen position-delete or deletion-vector facts and validate the
/// mandatory `_file UTF8, _pos INT64` prefix before a BE creates files.
pub fn position_delete_handle_from_payload(
    bytes: &[u8],
    input_schema: &SchemaRef,
) -> Result<IcebergPositionDeleteHandle, String> {
    let payload = decode_write_handle(bytes)?;
    if !matches!(
        payload.mode,
        IcebergWriteHandleMode::PositionDeletes | IcebergWriteHandleMode::DeletionVectors
    ) {
        return Err("Iceberg connector writer mode is not supported by the position-delete execution adapter".to_string());
    }
    if input_schema.fields().len() < 2
        || input_schema.fields()[0].name() != "_file"
        || input_schema.fields()[0].data_type() != &DataType::Utf8
        || input_schema.fields()[1].name() != "_pos"
        || input_schema.fields()[1].data_type() != &DataType::Int64
    {
        return Err("Iceberg position-delete writer requires (_file UTF8, _pos INT64) as its first two input columns".to_string());
    }
    Ok(IcebergPositionDeleteHandle {
        mode: payload.mode,
        data_location: payload.data_location,
        report_file_format: payload.report_file_format,
        compression: payload.compression,
        partitions: payload.position_delete_partitions,
    })
}

fn encode_compression(compression: Compression) -> String {
    match compression {
        Compression::UNCOMPRESSED => "UNCOMPRESSED".to_string(),
        Compression::SNAPPY => "SNAPPY".to_string(),
        Compression::GZIP(level) => format!("GZIP:{}", level.compression_level()),
        Compression::LZO => "LZO".to_string(),
        Compression::BROTLI(level) => format!("BROTLI:{}", level.compression_level()),
        Compression::LZ4 => "LZ4".to_string(),
        Compression::ZSTD(level) => format!("ZSTD:{}", level.compression_level()),
        Compression::LZ4_RAW => "LZ4_RAW".to_string(),
    }
}
fn decode_compression(value: &str) -> Result<Compression, String> {
    match value {
        "UNCOMPRESSED" => Ok(Compression::UNCOMPRESSED),
        "SNAPPY" => Ok(Compression::SNAPPY),
        "LZO" => Ok(Compression::LZO),
        "LZ4" => Ok(Compression::LZ4),
        "LZ4_RAW" => Ok(Compression::LZ4_RAW),
        value => {
            let (codec, level) = value.split_once(':').ok_or_else(|| {
                format!("unsupported Iceberg connector writer compression {value}")
            })?;
            match codec {
                "GZIP" => {
                    GzipLevel::try_new(level.parse::<u32>().map_err(|error| {
                        format!("invalid Iceberg GZIP compression level: {error}")
                    })?)
                    .map(Compression::GZIP)
                    .map_err(|error| format!("invalid Iceberg GZIP compression level: {error}"))
                }
                "BROTLI" => BrotliLevel::try_new(level.parse::<u32>().map_err(|error| {
                    format!("invalid Iceberg BROTLI compression level: {error}")
                })?)
                .map(Compression::BROTLI)
                .map_err(|error| format!("invalid Iceberg BROTLI compression level: {error}")),
                "ZSTD" => {
                    ZstdLevel::try_new(level.parse::<i32>().map_err(|error| {
                        format!("invalid Iceberg ZSTD compression level: {error}")
                    })?)
                    .map(Compression::ZSTD)
                    .map_err(|error| format!("invalid Iceberg ZSTD compression level: {error}"))
                }
                _ => Err(format!(
                    "unsupported Iceberg connector writer compression {value}"
                )),
            }
        }
    }
}
fn canonical_json<T: Serialize>(value: &T) -> Result<Bytes, String> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| format!("encode canonical Iceberg write payload failed: {error}"))
}
fn decode_json<T: for<'de> Deserialize<'de>>(payload: &[u8], subject: &str) -> Result<T, String> {
    serde_json::from_slice(payload)
        .map_err(|error| format!("decode Iceberg {subject} payload failed: {error}"))
}
fn ensure_canonical_json<T: Serialize>(
    payload: &[u8],
    value: &T,
    subject: &str,
) -> Result<(), String> {
    if canonical_json(value)?.as_ref() != payload {
        return Err(format!(
            "Iceberg {subject} payload is not canonical JSON v1"
        ));
    }
    Ok(())
}
fn base64_encode(value: impl AsRef<[u8]>) -> String {
    base64::engine::general_purpose::STANDARD.encode(value)
}
fn base64_decode(value: &str, subject: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| format!("decode Iceberg {subject} base64 failed: {error}"))
}
fn validate_secret_free_text(subject: &str, value: &str) -> Result<(), String> {
    if value.contains('\0') {
        return Err(format!("Iceberg {subject} contains a NUL byte"));
    }
    if let Ok(url) = url::Url::parse(value) {
        if !url.username().is_empty() || url.password().is_some() {
            return Err(format!("Iceberg {subject} must not embed credentials"));
        }
        for (key, _) in url.query_pairs() {
            if matches!(
                key.to_ascii_lowercase().as_str(),
                "access_key"
                    | "access_key_id"
                    | "secret"
                    | "secret_key"
                    | "session_token"
                    | "token"
            ) {
                return Err(format!("Iceberg {subject} must not embed credentials"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod handle_tests {
    use super::*;

    fn data_handle() -> IcebergWriteHandleInput {
        IcebergWriteHandleInput {
            mode: IcebergWriteHandleMode::Data,
            table_location: "s3://warehouse/db/t".to_string(),
            data_location: "s3://warehouse/db/t/data".to_string(),
            target_partition_spec_id: 0,
            target_snapshot_id: Some(7),
            file_format: IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::SNAPPY,
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            partition_source_column_names: Vec::new(),
            partition_column_names: Vec::new(),
            transform_exprs: Vec::new(),
            data_input_schema: None,
            position_delete_binding: None,
            position_delete_partitions: Vec::new(),
        }
    }

    #[test]
    fn handle_is_canonical_and_round_trips() {
        let first = encode_write_handle(&data_handle()).expect("encode first");
        let second = encode_write_handle(&data_handle()).expect("encode second");
        assert_eq!(first, second);
        let decoded = decode_write_handle(&first).expect("decode");
        assert_eq!(decoded.mode, IcebergWriteHandleMode::Data);
        assert_eq!(decoded.target_snapshot_id, Some(7));
        assert!(decode_write_handle(br#"{\"version\":1}"#).is_err());
    }

    #[test]
    fn handle_rejects_embedded_credentials() {
        let mut input = data_handle();
        input.data_location = "s3://key:secret@warehouse/db/t/data".to_string();
        assert!(encode_write_handle(&input).is_err());
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergStagedReportsPayloadV1 {
    version: u32,
    reports: Vec<IcebergWriterReportV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergWriterReportV1 {
    file: IcebergWrittenFileReportV1,
    is_overwrite: Option<bool>,
    is_rewrite: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergWrittenFileReportV1 {
    path: String,
    format: String,
    content: IcebergFileContentV1,
    record_count: i64,
    file_size_in_bytes: i64,
    partition: IcebergPartitionReportV1,
    split_offsets: Option<Vec<i64>>,
    column_stats: Option<IcebergColumnStatsV1>,
    referenced_data_file: Option<String>,
    first_row_id: Option<i64>,
    equality_ids: Option<Vec<i32>>,
    key_metadata_base64: Option<String>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
    cardinality: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergPartitionReportV1 {
    partition_path: String,
    null_fingerprint: String,
    partition_spec_id: i32,
    values: Vec<IcebergPartitionValueV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergPartitionValueV1 {
    is_null: bool,
    datum_base64: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergColumnStatsV1 {
    column_sizes: BTreeMap<i32, i64>,
    value_counts: BTreeMap<i32, i64>,
    null_value_counts: BTreeMap<i32, i64>,
    nan_value_counts: BTreeMap<i32, i64>,
    lower_bounds_base64: BTreeMap<i32, String>,
    upper_bounds_base64: BTreeMap<i32, String>,
}

impl IcebergWriterReportV1 {
    fn from_report(report: &IcebergWriterReport, metadata: &TableMetadata) -> Result<Self, String> {
        let file = &report.file;
        validate_secret_free_text("staged file path", &file.path)?;
        if let Some(path) = &file.referenced_data_file {
            validate_secret_free_text("referenced data file", path)?;
        }
        let descriptor = encode_partition_descriptor(
            &file.partition.partition_values,
            file.partition.partition_spec_id,
            metadata,
        )
        .map_err(|error| format!("encode Iceberg partition descriptor failed: {error}"))?;
        Ok(Self {
            file: IcebergWrittenFileReportV1 {
                path: file.path.clone(),
                format: file.format.clone(),
                content: file.content.into(),
                record_count: file.record_count,
                file_size_in_bytes: file.file_size_in_bytes,
                partition: IcebergPartitionReportV1 {
                    partition_path: file.partition.partition_path.clone(),
                    null_fingerprint: file.partition.null_fingerprint.clone(),
                    partition_spec_id: file.partition.partition_spec_id,
                    values: descriptor
                        .values
                        .into_iter()
                        .map(|value| IcebergPartitionValueV1 {
                            is_null: value.is_null,
                            datum_base64: value.datum_bytes.map(base64_encode),
                        })
                        .collect(),
                },
                split_offsets: file.split_offsets.clone(),
                column_stats: file
                    .column_stats
                    .as_ref()
                    .map(IcebergColumnStatsV1::from_stats),
                referenced_data_file: file.referenced_data_file.clone(),
                first_row_id: file.first_row_id,
                equality_ids: file.equality_ids.clone(),
                key_metadata_base64: file.key_metadata.as_ref().map(|value| base64_encode(value)),
                content_offset: file.content_offset,
                content_size_in_bytes: file.content_size_in_bytes,
                cardinality: file.cardinality,
            },
            is_overwrite: report.is_overwrite,
            is_rewrite: report.is_rewrite,
        })
    }

    fn into_report(self, metadata: &TableMetadata) -> Result<IcebergWriterReport, String> {
        validate_secret_free_text("staged file path", &self.file.path)?;
        if let Some(path) = &self.file.referenced_data_file {
            validate_secret_free_text("referenced data file", path)?;
        }
        let values = self
            .file
            .partition
            .values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let datum_bytes = match (value.is_null, value.datum_base64) {
                    (true, None) => None,
                    (true, Some(_)) => {
                        return Err(format!(
                            "Iceberg partition descriptor value {index} is null but carries a payload"
                        ));
                    }
                    (false, Some(value)) => Some(base64_decode(&value, "partition datum")?),
                    (false, None) => {
                        return Err(format!(
                            "Iceberg partition descriptor value {index} is non-null but has no payload"
                        ));
                    }
                };
                Ok(IcebergPartitionValueDescriptor {
                    is_null: value.is_null,
                    datum_bytes,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let partition_spec_id = self.file.partition.partition_spec_id;
        let partition_values = decode_partition_descriptor(
            Some(IcebergPartitionDescriptor { values }),
            partition_spec_id,
            metadata,
        )
        .map_err(|error| format!("decode Iceberg partition descriptor failed: {error}"))?;
        Ok(IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: self.file.path,
                format: self.file.format,
                content: self.file.content.into(),
                record_count: self.file.record_count,
                file_size_in_bytes: self.file.file_size_in_bytes,
                partition: IcebergPartitionReport {
                    partition_path: self.file.partition.partition_path,
                    null_fingerprint: self.file.partition.null_fingerprint,
                    partition_spec_id,
                    partition_values,
                },
                split_offsets: self.file.split_offsets,
                column_stats: self
                    .file
                    .column_stats
                    .map(IcebergColumnStatsV1::into_stats)
                    .transpose()?,
                referenced_data_file: self.file.referenced_data_file,
                first_row_id: self.file.first_row_id,
                equality_ids: self.file.equality_ids,
                key_metadata: self
                    .file
                    .key_metadata_base64
                    .as_deref()
                    .map(|value| base64_decode(value, "key metadata"))
                    .transpose()?,
                content_offset: self.file.content_offset,
                content_size_in_bytes: self.file.content_size_in_bytes,
                cardinality: self.file.cardinality,
            },
            is_overwrite: self.is_overwrite,
            is_rewrite: self.is_rewrite,
        })
    }

    fn from_unpartitioned_equality_delete_report(
        report: &IcebergWriterReport,
    ) -> Result<Self, String> {
        let file = &report.file;
        if file.content != IcebergFileContent::EqualityDeletes
            || !file.partition.partition_values.fields().is_empty()
            || !file.partition.partition_path.is_empty()
            || !file.partition.null_fingerprint.is_empty()
        {
            return Err(
                "Iceberg unpartitioned equality-delete report has non-equality or partitioned facts"
                    .to_string(),
            );
        }
        validate_secret_free_text("staged file path", &file.path)?;
        Ok(Self {
            file: IcebergWrittenFileReportV1 {
                path: file.path.clone(),
                format: file.format.clone(),
                content: IcebergFileContentV1::EqualityDeletes,
                record_count: file.record_count,
                file_size_in_bytes: file.file_size_in_bytes,
                partition: IcebergPartitionReportV1 {
                    partition_path: String::new(),
                    null_fingerprint: String::new(),
                    partition_spec_id: file.partition.partition_spec_id,
                    values: Vec::new(),
                },
                split_offsets: file.split_offsets.clone(),
                column_stats: file
                    .column_stats
                    .as_ref()
                    .map(IcebergColumnStatsV1::from_stats),
                referenced_data_file: None,
                first_row_id: None,
                equality_ids: file.equality_ids.clone(),
                key_metadata_base64: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            },
            is_overwrite: report.is_overwrite,
            is_rewrite: report.is_rewrite,
        })
    }
}

impl IcebergColumnStatsV1 {
    fn from_stats(stats: &IcebergColumnStats) -> Self {
        Self {
            column_sizes: stats.column_sizes.clone(),
            value_counts: stats.value_counts.clone(),
            null_value_counts: stats.null_value_counts.clone(),
            nan_value_counts: stats.nan_value_counts.clone(),
            lower_bounds_base64: stats
                .lower_bounds
                .iter()
                .map(|(field_id, value)| (*field_id, base64_encode(value)))
                .collect(),
            upper_bounds_base64: stats
                .upper_bounds
                .iter()
                .map(|(field_id, value)| (*field_id, base64_encode(value)))
                .collect(),
        }
    }

    fn into_stats(self) -> Result<IcebergColumnStats, String> {
        Ok(IcebergColumnStats {
            column_sizes: self.column_sizes,
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
            nan_value_counts: self.nan_value_counts,
            lower_bounds: self
                .lower_bounds_base64
                .into_iter()
                .map(|(field_id, value)| {
                    base64_decode(&value, "lower bound").map(|value| (field_id, value))
                })
                .collect::<Result<_, _>>()?,
            upper_bounds: self
                .upper_bounds_base64
                .into_iter()
                .map(|(field_id, value)| {
                    base64_decode(&value, "upper bound").map(|value| (field_id, value))
                })
                .collect::<Result<_, _>>()?,
        })
    }
}

/// Encode one logical writer's Iceberg file facts.  Multiple files are kept in
/// one logical payload and can then be bounded/framed by `ConnectorStagedReport`.
pub fn encode_writer_reports(
    reports: &[IcebergWriterReport],
    metadata: &TableMetadata,
) -> Result<Bytes, String> {
    let payload = IcebergStagedReportsPayloadV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        reports: reports
            .iter()
            .map(|report| IcebergWriterReportV1::from_report(report, metadata))
            .collect::<Result<_, _>>()?,
    };
    canonical_json(&payload)
}

pub fn decode_writer_reports(
    payload: &[u8],
    metadata: &TableMetadata,
) -> Result<Vec<IcebergWriterReport>, String> {
    let decoded: IcebergStagedReportsPayloadV1 = decode_json(payload, "staged report")?;
    if decoded.version != ICEBERG_WRITE_PAYLOAD_VERSION {
        return Err(format!(
            "unsupported Iceberg staged report payload version {}; expected {}",
            decoded.version, ICEBERG_WRITE_PAYLOAD_VERSION
        ));
    }
    ensure_canonical_json(payload, &decoded, "staged report")?;
    decoded
        .reports
        .into_iter()
        .map(|report| report.into_report(metadata))
        .collect()
}

pub fn staged_report_from_iceberg_reports(
    writer: ConnectorWriterIdentity,
    state: ConnectorWriterTerminalState,
    summary: ConnectorStagedReportSummary,
    reports: &[IcebergWriterReport],
    metadata: &TableMetadata,
) -> Result<ConnectorStagedReport, String> {
    let payload = encode_writer_reports(reports, metadata)?;
    ConnectorStagedReport::try_new(
        writer,
        CONNECTOR_WRITE_CONTRACT_VERSION,
        state,
        summary,
        payload,
    )
    .map_err(|error| format!("build Iceberg connector staged report failed: {error}"))
}

/// Encode reports for the BE-only unpartitioned equality-delete adapter.  Its
/// empty partition descriptor is fully self-describing and is checked again
/// against the FE-owned table metadata while decoding for commit.
pub fn staged_report_from_unpartitioned_equality_delete_reports(
    writer: ConnectorWriterIdentity,
    state: ConnectorWriterTerminalState,
    summary: ConnectorStagedReportSummary,
    reports: &[IcebergWriterReport],
) -> Result<ConnectorStagedReport, String> {
    let payload = IcebergStagedReportsPayloadV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        reports: reports
            .iter()
            .map(IcebergWriterReportV1::from_unpartitioned_equality_delete_report)
            .collect::<Result<_, _>>()?,
    };
    ConnectorStagedReport::try_new(
        writer,
        CONNECTOR_WRITE_CONTRACT_VERSION,
        state,
        summary,
        canonical_json(&payload)?,
    )
    .map_err(|error| format!("build Iceberg equality-delete staged report failed: {error}"))
}

/// Minimal file facts emitted by the BE-only position-delete adapter.  The
/// partition descriptor was frozen by the FE in the opaque writer handle, so
/// the BE can stage a report without loading table metadata or a catalog.
pub struct IcebergPositionDeleteStagedFile<'a> {
    pub path: &'a str,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub split_offsets: Option<Vec<i64>>,
    pub column_stats: Option<IcebergColumnStats>,
    pub referenced_data_file: String,
    pub partition: &'a IcebergPositionDeletePartition,
    pub format: &'a str,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub cardinality: Option<i64>,
}

/// Encode position-delete reports without table metadata.  The FE control
/// adapter replays the descriptors against the authoritative metadata before
/// committing, which is the only point that materializes `Struct` values.
pub fn staged_report_from_position_delete_files(
    writer: ConnectorWriterIdentity,
    state: ConnectorWriterTerminalState,
    summary: ConnectorStagedReportSummary,
    files: &[IcebergPositionDeleteStagedFile<'_>],
) -> Result<ConnectorStagedReport, String> {
    let reports = files
        .iter()
        .map(|file| {
            validate_secret_free_text("staged file path", file.path)?;
            validate_secret_free_text("referenced data file", &file.referenced_data_file)?;
            Ok(IcebergWriterReportV1 {
                file: IcebergWrittenFileReportV1 {
                    path: file.path.to_string(),
                    format: file.format.to_string(),
                    content: IcebergFileContentV1::PositionDeletes,
                    record_count: file.record_count,
                    file_size_in_bytes: file.file_size_in_bytes,
                    partition: IcebergPartitionReportV1 {
                        partition_path: file.partition.partition_path.clone(),
                        null_fingerprint: file.partition.null_fingerprint.clone(),
                        partition_spec_id: file.partition.partition_spec_id,
                        values: file
                            .partition
                            .descriptor
                            .values
                            .iter()
                            .map(|value| IcebergPartitionValueV1 {
                                is_null: value.is_null,
                                datum_base64: value.datum_bytes.as_ref().map(base64_encode),
                            })
                            .collect(),
                    },
                    split_offsets: file.split_offsets.clone(),
                    column_stats: file
                        .column_stats
                        .as_ref()
                        .map(IcebergColumnStatsV1::from_stats),
                    referenced_data_file: Some(file.referenced_data_file.clone()),
                    first_row_id: None,
                    equality_ids: None,
                    key_metadata_base64: None,
                    content_offset: file.content_offset,
                    content_size_in_bytes: file.content_size_in_bytes,
                    cardinality: file.cardinality,
                },
                is_overwrite: None,
                is_rewrite: None,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let payload = IcebergStagedReportsPayloadV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        reports,
    };
    ConnectorStagedReport::try_new(
        writer,
        CONNECTOR_WRITE_CONTRACT_VERSION,
        state,
        summary,
        canonical_json(&payload)?,
    )
    .map_err(|error| format!("build Iceberg position-delete staged report failed: {error}"))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergWriteReceiptV1 {
    version: u32,
    snapshot_id: i64,
}

pub fn encode_write_receipt(snapshot_id: i64) -> Result<Bytes, String> {
    canonical_json(&IcebergWriteReceiptV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        snapshot_id,
    })
}

pub fn decode_write_receipt(payload: &[u8]) -> Result<i64, String> {
    let decoded: IcebergWriteReceiptV1 = decode_json(payload, "write receipt")?;
    if decoded.version != ICEBERG_WRITE_PAYLOAD_VERSION {
        return Err(format!(
            "unsupported Iceberg write receipt version {}; expected {}",
            decoded.version, ICEBERG_WRITE_PAYLOAD_VERSION
        ));
    }
    ensure_canonical_json(payload, &decoded, "write receipt")?;
    Ok(decoded.snapshot_id)
}

pub fn connector_write_receipt(
    snapshot_id: i64,
    resulting_row_count: Option<u64>,
) -> Result<ConnectorWriteReceipt, String> {
    connector_write_receipt_with_partitioning(snapshot_id, resulting_row_count, None)
}

pub fn connector_write_receipt_with_partitioning(
    snapshot_id: i64,
    resulting_row_count: Option<u64>,
    committed_partitioning: Option<novarocks_spi::connector::ConnectorCommittedPartitioning>,
) -> Result<ConnectorWriteReceipt, String> {
    let payload = encode_write_receipt(snapshot_id)?;
    let committed_version = ConnectorCommittedVersion::try_new(payload.clone(), Some(snapshot_id))
        .map_err(|error| format!("build Iceberg connector committed version failed: {error}"))?;
    match committed_partitioning {
        Some(partitioning) => ConnectorWriteReceipt::try_new_with_committed_facts_and_partitioning(
            payload,
            committed_version,
            resulting_row_count,
            partitioning,
        ),
        None => ConnectorWriteReceipt::try_new_with_committed_facts(
            payload,
            committed_version,
            resulting_row_count,
        ),
    }
    .map_err(|error| format!("build Iceberg connector write receipt failed: {error}"))
}
