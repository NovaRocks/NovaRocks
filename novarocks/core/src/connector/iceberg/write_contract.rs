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

//! Iceberg's provider-private v1 payloads for the connector write contract.
//!
//! These payloads deliberately describe only stable, secret-free facts.  The
//! execution binding supplies catalog clients and object-store credentials
//! locally; neither can cross a native fragment boundary.

use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::SchemaRef;
use base64::Engine;
use bytes::Bytes;
use iceberg::spec::TableMetadata;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use serde::{Deserialize, Serialize};

use novarocks_spi::connector::{
    CONNECTOR_WRITE_CONTRACT_VERSION, ConnectorCommittedVersion, ConnectorExecutionBindingKey,
    ConnectorStagedReport, ConnectorStagedReportSummary, ConnectorWriteReceipt,
    ConnectorWriterHandle, ConnectorWriterIdentity, ConnectorWriterTerminalState,
};

use super::commit::DeletionVector;
use super::delete_file::{IcebergFileContent, IcebergFileFormat};
use super::report::{
    IcebergColumnStats, IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
    partition_path_from_struct,
};
use super::scan_model::{IcebergSchemaDef, IcebergSchemaFieldDef};
use super::sink_plan::{
    IcebergSinkMode, IcebergSinkObjectStoreConfig, IcebergSinkPlan, PositionDeleteDataFilePartition,
};
use super::write_descriptor::{
    IcebergPartitionDescriptor, IcebergPartitionValueDescriptor, decode_partition_descriptor,
    encode_partition_descriptor,
};
use crate::sql::planner::distributed::write::sink::{
    IcebergWriteFileCompression, IcebergWriteSinkMode, IcebergWriteSinkSpec,
};

pub(crate) const ICEBERG_WRITE_PAYLOAD_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IcebergSinkModeV1 {
    Data,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

impl From<IcebergSinkMode> for IcebergSinkModeV1 {
    fn from(value: IcebergSinkMode) -> Self {
        match value {
            IcebergSinkMode::Data => Self::Data,
            IcebergSinkMode::PositionDeletes => Self::PositionDeletes,
            IcebergSinkMode::DeletionVectors => Self::DeletionVectors,
            IcebergSinkMode::EqualityDeletes => Self::EqualityDeletes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IcebergFileFormatV1 {
    Parquet,
    Puffin,
    Unknown,
}

impl From<IcebergFileFormat> for IcebergFileFormatV1 {
    fn from(value: IcebergFileFormat) -> Self {
        match value {
            IcebergFileFormat::Parquet => Self::Parquet,
            IcebergFileFormat::Puffin => Self::Puffin,
            IcebergFileFormat::Unknown => Self::Unknown,
        }
    }
}

impl From<IcebergFileFormatV1> for IcebergFileFormat {
    fn from(value: IcebergFileFormatV1) -> Self {
        match value {
            IcebergFileFormatV1::Parquet => Self::Parquet,
            IcebergFileFormatV1::Puffin => Self::Puffin,
            IcebergFileFormatV1::Unknown => Self::Unknown,
        }
    }
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

/// Secret-free subset of an [`IcebergSinkPlan`] transported inside a generic
/// [`ConnectorWriterHandle`].  Schemas and expression programs remain generic
/// plan inputs, while object-store credentials remain in the local binding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergWriteHandlePayloadV1 {
    version: u32,
    mode: IcebergSinkModeV1,
    table_location: String,
    data_location: String,
    target_partition_spec_id: i32,
    target_snapshot_id: Option<i64>,
    file_format: IcebergFileFormatV1,
    report_file_format: String,
    compression: String,
    equality_delete_columns: Vec<IcebergEqualityDeleteColumnV1>,
    row_lineage_data: bool,
    partition_source_column_names: Vec<String>,
    partition_column_names: Vec<String>,
    transform_exprs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data_input_schema: Option<Vec<IcebergWriterSchemaFieldV1>>,
    position_delete_binding: Option<IcebergPositionDeleteBindingV1>,
    position_delete_partitions: Vec<IcebergPositionDeletePartitionV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergEqualityDeleteColumnV1 {
    name: String,
    field_id: i32,
    data_type: String,
    nullable: bool,
}

/// Iceberg field-ID descriptor carried inside a DATA writer handle. The
/// generic native carrier transports Arrow values and ordinal selection, but
/// deliberately does not import provider schema DTOs. This provider-private,
/// secret-free descriptor restores the field IDs on the BE-local Arrow schema
/// before Parquet staging.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergWriterSchemaFieldV1 {
    field_id: i32,
    name: String,
    initial_default_json: Option<String>,
    write_default_json: Option<String>,
    children: Vec<IcebergWriterSchemaFieldV1>,
}

impl From<&IcebergSchemaFieldDef> for IcebergWriterSchemaFieldV1 {
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

impl From<&IcebergWriterSchemaFieldV1> for IcebergSchemaFieldDef {
    fn from(field: &IcebergWriterSchemaFieldV1) -> Self {
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergPositionDeleteBindingV1 {
    output_column_names: Vec<String>,
    partition_source_column_names: Vec<String>,
    partition_column_names: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergPositionDeletePartitionV1 {
    data_file_path: String,
    partition_path: String,
    null_fingerprint: String,
    partition_spec_id: i32,
    values: Vec<IcebergPartitionValueV1>,
    existing_deletion_vector_payload_base64: Option<String>,
}

impl IcebergWriteHandlePayloadV1 {
    fn from_sink_plan(plan: &IcebergSinkPlan) -> Result<Self, String> {
        validate_secret_free_text("table location", &plan.table_location)?;
        validate_secret_free_text("data location", &plan.data_location)?;
        for transform in &plan.transform_exprs {
            validate_secret_free_text("partition transform", transform)?;
        }
        Ok(Self {
            version: ICEBERG_WRITE_PAYLOAD_VERSION,
            mode: plan.mode.into(),
            table_location: plan.table_location.clone(),
            data_location: plan.data_location.clone(),
            target_partition_spec_id: plan.target_partition_spec_id,
            target_snapshot_id: plan.target_snapshot_id,
            file_format: plan.file_format.into(),
            report_file_format: plan.report_file_format.clone(),
            compression: encode_parquet_compression(plan.compression),
            equality_delete_columns: plan
                .equality_delete_columns
                .iter()
                .map(|column| IcebergEqualityDeleteColumnV1 {
                    name: column.name.clone(),
                    field_id: column.field_id,
                    data_type: format!("{:?}", column.data_type),
                    nullable: column.nullable,
                })
                .collect(),
            row_lineage_data: plan.row_lineage_data,
            partition_source_column_names: plan.partition_source_column_names.clone(),
            partition_column_names: plan.partition_column_names.clone(),
            transform_exprs: plan.transform_exprs.clone(),
            data_input_schema: None,
            position_delete_binding: plan.position_delete_binding.as_ref().map(|binding| {
                IcebergPositionDeleteBindingV1 {
                    output_column_names: binding.output_column_names.clone(),
                    partition_source_column_names: binding.partition_source_column_names.clone(),
                    partition_column_names: binding.partition_column_names.clone(),
                }
            }),
            position_delete_partitions: Vec::new(),
        })
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != ICEBERG_WRITE_PAYLOAD_VERSION {
            return Err(format!(
                "unsupported Iceberg writer handle payload version {}; expected {}",
                self.version, ICEBERG_WRITE_PAYLOAD_VERSION
            ));
        }
        validate_secret_free_text("table location", &self.table_location)?;
        validate_secret_free_text("data location", &self.data_location)?;
        for transform in &self.transform_exprs {
            validate_secret_free_text("partition transform", transform)?;
        }
        Ok(())
    }

    fn annotate_data_input_schema(&self, input_schema: SchemaRef) -> Result<SchemaRef, String> {
        let Some(fields) = self.data_input_schema.as_ref() else {
            return Ok(input_schema);
        };
        let columns = input_schema
            .fields()
            .iter()
            .map(|field| super::schema::IcebergArrowColumn {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            })
            .collect::<Vec<_>>();
        super::schema::build_projected_output_schema_from_scan_model(
            &IcebergSchemaDef {
                fields: fields.iter().map(IcebergSchemaFieldDef::from).collect(),
            },
            &columns,
        )
    }
}

/// Encode the non-sensitive facts from a legacy sink plan in deterministic,
/// compact JSON.  `serde_json::Map` is ordered by default and all map-bearing
/// report fields below use `BTreeMap`, so equal facts always produce equal
/// bytes and therefore equal SPI digests.
pub(crate) fn encode_sink_plan_handle_payload(plan: &IcebergSinkPlan) -> Result<Bytes, String> {
    canonical_json(&IcebergWriteHandlePayloadV1::from_sink_plan(plan)?)
}

/// Build the secret-free data-file writer template directly from the FE-owned
/// distributed sink specification.  Ordinary DATA and row-lineage DATA both
/// stage Iceberg data files; delete modes require their own provider adapters.
pub(crate) fn encode_data_sink_spec_handle_payload(
    spec: &IcebergWriteSinkSpec,
) -> Result<Bytes, String> {
    if !matches!(
        spec.mode,
        IcebergWriteSinkMode::Data | IcebergWriteSinkMode::RowLineageData
    ) {
        return Err("only Iceberg data-file sinks can use the data writer template".to_string());
    }
    let serialized = spec.iceberg.serialized_metadata.as_deref().ok_or_else(|| {
        "Iceberg DATA writer template requires serialized table metadata".to_string()
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized)
        .map_err(|error| format!("decode Iceberg DATA writer table metadata: {error}"))?;
    let partition_spec = metadata
        .partition_spec_by_id(spec.target_partition_spec_id)
        .ok_or_else(|| {
            format!(
                "Iceberg DATA writer template references unknown partition spec {}",
                spec.target_partition_spec_id
            )
        })?;
    let mut partition_source_column_names = Vec::with_capacity(partition_spec.fields().len());
    let mut partition_column_names = Vec::with_capacity(partition_spec.fields().len());
    let mut transform_exprs = Vec::with_capacity(partition_spec.fields().len());
    for field in partition_spec.fields() {
        let source = metadata
            .current_schema()
            .field_by_id(field.source_id)
            .ok_or_else(|| {
                format!(
                    "Iceberg DATA writer partition field {} has unknown source column {}",
                    field.name, field.source_id
                )
            })?;
        partition_source_column_names.push(source.name.clone());
        partition_column_names.push(field.name.clone());
        transform_exprs.push(field.transform.to_string());
    }
    let compression = match spec.compression {
        IcebergWriteFileCompression::Snappy => "SNAPPY",
    };
    canonical_json(&IcebergWriteHandlePayloadV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        mode: IcebergSinkModeV1::Data,
        table_location: spec.table_location.clone(),
        data_location: spec.data_location.clone(),
        target_partition_spec_id: spec.target_partition_spec_id,
        target_snapshot_id: spec.iceberg.current_snapshot_id,
        file_format: IcebergFileFormatV1::Parquet,
        report_file_format: spec.file_format.to_ascii_lowercase(),
        compression: compression.to_string(),
        equality_delete_columns: Vec::new(),
        row_lineage_data: spec.mode == IcebergWriteSinkMode::RowLineageData,
        partition_source_column_names,
        partition_column_names,
        transform_exprs,
        data_input_schema: Some(
            spec.iceberg
                .schema
                .fields
                .iter()
                .map(IcebergWriterSchemaFieldV1::from)
                .collect(),
        ),
        position_delete_binding: None,
        position_delete_partitions: Vec::new(),
    })
}

/// Build the secret-free equality-delete handle from the FE-owned sink spec.
/// C1 intentionally admits only the existing unpartitioned equality-delete
/// path; partitioned delete semantics require a later provider adapter.
pub(crate) fn encode_equality_delete_sink_spec_handle_payload(
    spec: &IcebergWriteSinkSpec,
    columns: &[super::commit::EqualityDeleteColumn],
) -> Result<Bytes, String> {
    if spec.mode != IcebergWriteSinkMode::EqualityDeletes {
        return Err("only Iceberg equality-delete sinks can use this writer template".to_string());
    }
    if columns.is_empty() {
        return Err("Iceberg equality-delete writer template requires columns".to_string());
    }
    let serialized = spec.iceberg.serialized_metadata.as_deref().ok_or_else(|| {
        "Iceberg equality-delete writer template requires serialized table metadata".to_string()
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized)
        .map_err(|error| format!("decode Iceberg equality-delete table metadata: {error}"))?;
    if !metadata.default_partition_spec().is_unpartitioned() {
        return Err(
            "Iceberg connector equality-delete writer template supports only unpartitioned tables"
                .to_string(),
        );
    }
    canonical_json(&IcebergWriteHandlePayloadV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        mode: IcebergSinkModeV1::EqualityDeletes,
        table_location: spec.table_location.clone(),
        data_location: spec.data_location.clone(),
        target_partition_spec_id: spec.target_partition_spec_id,
        target_snapshot_id: spec.iceberg.current_snapshot_id,
        file_format: IcebergFileFormatV1::Parquet,
        report_file_format: spec.file_format.to_ascii_lowercase(),
        compression: "SNAPPY".to_string(),
        equality_delete_columns: columns
            .iter()
            .map(|column| IcebergEqualityDeleteColumnV1 {
                name: column.name.clone(),
                field_id: column.field_id,
                data_type: format!("{:?}", column.data_type),
                nullable: column.nullable,
            })
            .collect(),
        row_lineage_data: false,
        partition_source_column_names: Vec::new(),
        partition_column_names: Vec::new(),
        transform_exprs: Vec::new(),
        data_input_schema: None,
        position_delete_binding: None,
        position_delete_partitions: Vec::new(),
    })
}

pub(crate) fn decode_sink_plan_handle_payload(
    payload: &[u8],
) -> Result<IcebergWriteHandlePayloadV1, String> {
    let decoded: IcebergWriteHandlePayloadV1 = decode_canonical_json(payload, "writer handle")?;
    decoded.validate()?;
    ensure_canonical_json(payload, &decoded, "writer handle")?;
    Ok(decoded)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergWriteHandleMode {
    Data,
    EqualityDeletes,
    PositionDeletes,
    DeletionVectors,
}

pub(crate) fn write_handle_mode(payload: &[u8]) -> Result<IcebergWriteHandleMode, String> {
    Ok(match decode_sink_plan_handle_payload(payload)?.mode {
        IcebergSinkModeV1::Data => IcebergWriteHandleMode::Data,
        IcebergSinkModeV1::EqualityDeletes => IcebergWriteHandleMode::EqualityDeletes,
        IcebergSinkModeV1::PositionDeletes => IcebergWriteHandleMode::PositionDeletes,
        IcebergSinkModeV1::DeletionVectors => IcebergWriteHandleMode::DeletionVectors,
    })
}

fn encode_parquet_compression(compression: Compression) -> String {
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

fn decode_parquet_compression(value: &str) -> Result<Compression, String> {
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
                "GZIP" => level
                    .parse::<u32>()
                    .map_err(|error| format!("invalid Iceberg GZIP compression level: {error}"))
                    .and_then(|level| {
                        GzipLevel::try_new(level)
                            .map(Compression::GZIP)
                            .map_err(|error| {
                                format!("invalid Iceberg GZIP compression level: {error}")
                            })
                    }),
                "BROTLI" => level
                    .parse::<u32>()
                    .map_err(|error| format!("invalid Iceberg BROTLI compression level: {error}"))
                    .and_then(|level| {
                        BrotliLevel::try_new(level)
                            .map(Compression::BROTLI)
                            .map_err(|error| {
                                format!("invalid Iceberg BROTLI compression level: {error}")
                            })
                    }),
                "ZSTD" => level
                    .parse::<i32>()
                    .map_err(|error| format!("invalid Iceberg ZSTD compression level: {error}"))
                    .and_then(|level| {
                        ZstdLevel::try_new(level)
                            .map(Compression::ZSTD)
                            .map_err(|error| {
                                format!("invalid Iceberg ZSTD compression level: {error}")
                            })
                    }),
                _ => Err(format!(
                    "unsupported Iceberg connector writer compression {value}"
                )),
            }
        }
    }
}

/// Reconstruct the strictly data-file subset of an Iceberg sink plan from an
/// opaque handle and BE-local storage binding. The generic carrier supplies
/// Arrow batches that have already been projected by the fragment plan, so no
/// FE expression arena, catalog client, or credential is required here.
///
/// Delete modes intentionally fail closed until their provider execution
/// adapters preserve the same staged-file semantics.
pub(crate) fn data_sink_plan_from_handle_payload(
    payload: &[u8],
    input_schema: SchemaRef,
    object_store_s3: Option<IcebergSinkObjectStoreConfig>,
) -> Result<IcebergSinkPlan, String> {
    let payload = decode_sink_plan_handle_payload(payload)?;
    if payload.mode != IcebergSinkModeV1::Data {
        return Err(
            "Iceberg connector writer mode is not supported by the data-file execution adapter"
                .to_string(),
        );
    }
    let compression = decode_parquet_compression(&payload.compression)?;
    let input_schema = payload.annotate_data_input_schema(input_schema)?;
    Ok(IcebergSinkPlan {
        mode: IcebergSinkMode::Data,
        table_location: payload.table_location,
        data_location: payload.data_location,
        target_partition_spec_id: payload.target_partition_spec_id,
        target_table_metadata: None,
        target_snapshot_id: payload.target_snapshot_id,
        position_delete_data_file_partitions: Default::default(),
        position_delete_data_file_partition_index_input: None,
        object_store_s3,
        file_format: payload.file_format.into(),
        report_file_format: payload.report_file_format,
        compression,
        output_schema: input_schema.clone(),
        target_schema: input_schema,
        equality_delete_columns: Vec::new(),
        row_lineage_data: payload.row_lineage_data,
        output_exprs: Vec::new(),
        partition_exprs: Vec::new(),
        partition_source_column_names: payload.partition_source_column_names,
        partition_column_names: payload.partition_column_names,
        transform_exprs: payload.transform_exprs,
        position_delete_binding: None,
    })
}

/// Reconstruct the unpartitioned equality-delete facts needed by the BE-only
/// writer.  The actual Arrow types remain the generic sink input schema; the
/// opaque handle only pins their Iceberg field IDs and rejects a stale or
/// differently-shaped fragment before it can create a delete file.
pub(crate) fn equality_delete_handle_from_payload(
    payload: &[u8],
    input_schema: SchemaRef,
) -> Result<(String, i32, Vec<super::commit::EqualityDeleteColumn>), String> {
    let payload = decode_sink_plan_handle_payload(payload)?;
    if payload.mode != IcebergSinkModeV1::EqualityDeletes {
        return Err(
            "Iceberg connector writer mode is not supported by the equality-delete execution adapter"
                .to_string(),
        );
    }
    if !payload.partition_source_column_names.is_empty()
        || !payload.partition_column_names.is_empty()
        || !payload.transform_exprs.is_empty()
    {
        return Err(
            "Iceberg connector equality-delete execution supports only unpartitioned tables"
                .to_string(),
        );
    }
    if payload.equality_delete_columns.is_empty() {
        return Err("Iceberg connector equality-delete handle has no equality columns".to_string());
    }
    if payload.equality_delete_columns.len() != input_schema.fields().len() {
        return Err(format!(
            "Iceberg equality-delete handle has {} columns but fragment input has {}",
            payload.equality_delete_columns.len(),
            input_schema.fields().len()
        ));
    }
    let columns = payload
        .equality_delete_columns
        .iter()
        .zip(input_schema.fields().iter())
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
            Ok(super::commit::EqualityDeleteColumn {
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

/// Provider-private facts used by the BE position-delete and deletion-vector
/// staging adapters.  The FE freezes this index at the target snapshot; the
/// BE never opens a catalog or infers a partition from a newer snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IcebergPositionDeleteHandle {
    pub(crate) mode: IcebergWriteHandleMode,
    pub(crate) data_location: String,
    pub(crate) report_file_format: String,
    pub(crate) compression: Compression,
    pub(crate) partitions: BTreeMap<String, IcebergPositionDeletePartition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IcebergPositionDeletePartition {
    pub(crate) partition_path: String,
    pub(crate) null_fingerprint: String,
    pub(crate) partition_spec_id: i32,
    pub(crate) descriptor: IcebergPartitionDescriptor,
    /// Iceberg's canonical DV payload as read by the FE from the frozen target
    /// snapshot.  It is a data fact, not a credential or process-local handle.
    pub(crate) existing_deletion_vector_payload: Option<Vec<u8>>,
}

/// Encode the FE-frozen data-file partition lookup in a canonical writer
/// handle.  The caller must pass the exact target snapshot index; payload
/// bounds are enforced by `ConnectorWriterHandle`/`ConnectorWritePlan` before
/// the result reaches a BE.
pub(crate) fn encode_position_delete_sink_handle_payload(
    spec: &IcebergWriteSinkSpec,
    metadata: &TableMetadata,
    partitions: &HashMap<String, PositionDeleteDataFilePartition>,
) -> Result<Bytes, String> {
    encode_position_delete_handle_payload(spec, metadata, partitions, None)
}

/// Encode a deletion-vector writer handle.  Existing positions are read by
/// the FE against the planned snapshot and serialized as canonical Iceberg DV
/// payloads, so the BE never resolves prior delete files or table metadata.
pub(crate) fn encode_deletion_vector_sink_handle_payload(
    spec: &IcebergWriteSinkSpec,
    metadata: &TableMetadata,
    partitions: &HashMap<String, PositionDeleteDataFilePartition>,
    existing_vectors: &HashMap<String, DeletionVector>,
) -> Result<Bytes, String> {
    encode_position_delete_handle_payload(spec, metadata, partitions, Some(existing_vectors))
}

fn encode_position_delete_handle_payload(
    spec: &IcebergWriteSinkSpec,
    metadata: &TableMetadata,
    partitions: &HashMap<String, PositionDeleteDataFilePartition>,
    existing_vectors: Option<&HashMap<String, DeletionVector>>,
) -> Result<Bytes, String> {
    if !matches!(
        spec.mode,
        IcebergWriteSinkMode::PositionDeletes | IcebergWriteSinkMode::DeletionVectors
    ) {
        return Err(
            "only Iceberg position-delete or deletion-vector sinks can use this writer template"
                .to_string(),
        );
    }
    let mut encoded_partitions = partitions
        .iter()
        .map(|(data_file_path, partition)| {
            validate_secret_free_text("referenced data file", data_file_path)?;
            let partition_spec = metadata
                .partition_spec_by_id(partition.partition_spec_id)
                .ok_or_else(|| {
                    format!(
                        "Iceberg position-delete handle references unknown partition spec {}",
                        partition.partition_spec_id
                    )
                })?;
            let (partition_path, null_fingerprint) =
                partition_path_from_struct(&partition.partition_values, partition_spec)?;
            let descriptor = encode_partition_descriptor(
                &partition.partition_values,
                partition.partition_spec_id,
                metadata,
            )
            .map_err(|error| format!("encode Iceberg position-delete partition: {error}"))?;
            let existing_deletion_vector_payload_base64 = existing_vectors
                .and_then(|vectors| vectors.get(data_file_path))
                .map(|vector| vector.to_iceberg_payload())
                .transpose()
                .map_err(|error| format!("encode frozen Iceberg deletion vector: {error}"))?
                .map(base64_encode);
            Ok(IcebergPositionDeletePartitionV1 {
                data_file_path: data_file_path.clone(),
                partition_path,
                null_fingerprint,
                partition_spec_id: partition.partition_spec_id,
                values: descriptor
                    .values
                    .into_iter()
                    .map(|value| IcebergPartitionValueV1 {
                        is_null: value.is_null,
                        datum_base64: value.datum_bytes.map(base64_encode),
                    })
                    .collect(),
                existing_deletion_vector_payload_base64,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    encoded_partitions.sort_by(|left, right| left.data_file_path.cmp(&right.data_file_path));
    if encoded_partitions
        .windows(2)
        .any(|pair| pair[0].data_file_path == pair[1].data_file_path)
    {
        return Err(
            "Iceberg position-delete handle contains duplicate data-file paths".to_string(),
        );
    }
    if existing_vectors.is_some() && spec.mode != IcebergWriteSinkMode::DeletionVectors {
        return Err("frozen deletion vectors require a deletion-vector sink".to_string());
    }
    let compression = match spec.compression {
        IcebergWriteFileCompression::Snappy => "SNAPPY",
    };
    canonical_json(&IcebergWriteHandlePayloadV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        mode: match spec.mode {
            IcebergWriteSinkMode::PositionDeletes => IcebergSinkModeV1::PositionDeletes,
            IcebergWriteSinkMode::DeletionVectors => IcebergSinkModeV1::DeletionVectors,
            _ => unreachable!("validated position-delete writer mode"),
        },
        table_location: spec.table_location.clone(),
        data_location: spec.data_location.clone(),
        target_partition_spec_id: spec.target_partition_spec_id,
        target_snapshot_id: spec.iceberg.current_snapshot_id,
        file_format: IcebergFileFormatV1::Parquet,
        report_file_format: spec.file_format.to_ascii_lowercase(),
        compression: compression.to_string(),
        equality_delete_columns: Vec::new(),
        row_lineage_data: false,
        partition_source_column_names: Vec::new(),
        partition_column_names: Vec::new(),
        transform_exprs: Vec::new(),
        data_input_schema: None,
        position_delete_binding: None,
        position_delete_partitions: encoded_partitions,
    })
}

pub(crate) fn position_delete_handle_from_payload(
    payload: &[u8],
    input_schema: &arrow::datatypes::SchemaRef,
) -> Result<IcebergPositionDeleteHandle, String> {
    let payload = decode_sink_plan_handle_payload(payload)?;
    if !matches!(
        payload.mode,
        IcebergSinkModeV1::PositionDeletes | IcebergSinkModeV1::DeletionVectors
    ) {
        return Err(
            "Iceberg connector writer mode is not supported by the position-delete execution adapter"
                .to_string(),
        );
    }
    if input_schema.fields().len() < 2
        || input_schema.fields()[0].name()
            != crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_FILE_COLUMN
        || input_schema.fields()[0].data_type() != &arrow::datatypes::DataType::Utf8
        || input_schema.fields()[1].name()
            != crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_POS_COLUMN
        || input_schema.fields()[1].data_type() != &arrow::datatypes::DataType::Int64
    {
        return Err(
            "Iceberg position-delete writer requires (_file UTF8, _pos INT64) as its first two input columns"
                .to_string(),
        );
    }
    let compression = decode_parquet_compression(&payload.compression)
        .map_err(|error| format!("unsupported Iceberg position-delete compression: {error}"))?;
    let mut partitions = BTreeMap::new();
    for partition in payload.position_delete_partitions {
        validate_secret_free_text("referenced data file", &partition.data_file_path)?;
        let existing_deletion_vector_payload = partition
            .existing_deletion_vector_payload_base64
            .as_deref()
            .map(|value| base64_decode(value, "frozen deletion vector"))
            .transpose()?;
        if let Some(payload) = &existing_deletion_vector_payload {
            DeletionVector::from_iceberg_payload(payload)
                .map_err(|error| format!("decode frozen Iceberg deletion vector: {error}"))?;
        }
        let descriptor = IcebergPartitionDescriptor {
            values: partition
                .values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    let datum_bytes = match (value.is_null, value.datum_base64) {
                        (true, None) => None,
                        (true, Some(_)) => {
                            return Err(format!(
                                "Iceberg position-delete partition descriptor value {index} is null but carries a payload"
                            ));
                        }
                        (false, Some(value)) => {
                            Some(base64_decode(&value, "position-delete partition datum")?)
                        }
                        (false, None) => {
                            return Err(format!(
                                "Iceberg position-delete partition descriptor value {index} is non-null but has no payload"
                            ));
                        }
                    };
                    Ok(IcebergPartitionValueDescriptor {
                        is_null: value.is_null,
                        datum_bytes,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
        };
        if partitions
            .insert(
                partition.data_file_path,
                IcebergPositionDeletePartition {
                    partition_path: partition.partition_path,
                    null_fingerprint: partition.null_fingerprint,
                    partition_spec_id: partition.partition_spec_id,
                    descriptor,
                    existing_deletion_vector_payload,
                },
            )
            .is_some()
        {
            return Err(
                "Iceberg position-delete handle contains duplicate data-file paths".to_string(),
            );
        }
    }
    Ok(IcebergPositionDeleteHandle {
        mode: match payload.mode {
            IcebergSinkModeV1::PositionDeletes => IcebergWriteHandleMode::PositionDeletes,
            IcebergSinkModeV1::DeletionVectors => IcebergWriteHandleMode::DeletionVectors,
            _ => unreachable!("validated position-delete writer mode"),
        },
        data_location: payload.data_location,
        report_file_format: payload.report_file_format,
        compression,
        partitions,
    })
}

pub(crate) fn writer_handle_from_sink_plan(
    owner: ConnectorExecutionBindingKey,
    writer: ConnectorWriterIdentity,
    plan: &IcebergSinkPlan,
) -> Result<ConnectorWriterHandle, String> {
    let payload = encode_sink_plan_handle_payload(plan)?;
    ConnectorWriterHandle::try_new(owner, writer, ICEBERG_WRITE_PAYLOAD_VERSION, payload)
        .map_err(|error| format!("build Iceberg connector writer handle failed: {error}"))
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
pub(crate) fn encode_writer_reports(
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

pub(crate) fn decode_writer_reports(
    payload: &[u8],
    metadata: &TableMetadata,
) -> Result<Vec<IcebergWriterReport>, String> {
    let decoded: IcebergStagedReportsPayloadV1 = decode_canonical_json(payload, "staged report")?;
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

pub(crate) fn staged_report_from_iceberg_reports(
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
pub(crate) fn staged_report_from_unpartitioned_equality_delete_reports(
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
pub(crate) struct IcebergPositionDeleteStagedFile<'a> {
    pub(crate) path: &'a str,
    pub(crate) record_count: i64,
    pub(crate) file_size_in_bytes: i64,
    pub(crate) split_offsets: Option<Vec<i64>>,
    pub(crate) column_stats: Option<IcebergColumnStats>,
    pub(crate) referenced_data_file: String,
    pub(crate) partition: &'a IcebergPositionDeletePartition,
    pub(crate) format: &'a str,
    pub(crate) content_offset: Option<i64>,
    pub(crate) content_size_in_bytes: Option<i64>,
    pub(crate) cardinality: Option<i64>,
}

/// Encode position-delete reports without table metadata.  The FE control
/// adapter replays the descriptors against the authoritative metadata before
/// committing, which is the only point that materializes `Struct` values.
pub(crate) fn staged_report_from_position_delete_files(
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

pub(crate) fn encode_write_receipt(snapshot_id: i64) -> Result<Bytes, String> {
    canonical_json(&IcebergWriteReceiptV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        snapshot_id,
    })
}

pub(crate) fn decode_write_receipt(payload: &[u8]) -> Result<i64, String> {
    let decoded: IcebergWriteReceiptV1 = decode_canonical_json(payload, "write receipt")?;
    if decoded.version != ICEBERG_WRITE_PAYLOAD_VERSION {
        return Err(format!(
            "unsupported Iceberg write receipt version {}; expected {}",
            decoded.version, ICEBERG_WRITE_PAYLOAD_VERSION
        ));
    }
    ensure_canonical_json(payload, &decoded, "write receipt")?;
    Ok(decoded.snapshot_id)
}

pub(crate) fn connector_write_receipt(snapshot_id: i64) -> Result<ConnectorWriteReceipt, String> {
    let payload = encode_write_receipt(snapshot_id)?;
    let committed_version = ConnectorCommittedVersion::try_new(payload.clone(), Some(snapshot_id))
        .map_err(|error| format!("build Iceberg connector committed version failed: {error}"))?;
    ConnectorWriteReceipt::try_new_with_committed_version(payload, committed_version)
        .map_err(|error| format!("build Iceberg connector write receipt failed: {error}"))
}

fn canonical_json<T: Serialize>(value: &T) -> Result<Bytes, String> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| format!("encode canonical Iceberg write payload failed: {error}"))
}

fn decode_canonical_json<T: for<'de> Deserialize<'de>>(
    payload: &[u8],
    subject: &str,
) -> Result<T, String> {
    serde_json::from_slice(payload)
        .map_err(|error| format!("decode Iceberg {subject} payload failed: {error}"))
}

fn ensure_canonical_json<T: Serialize>(
    payload: &[u8],
    value: &T,
    subject: &str,
) -> Result<(), String> {
    let canonical = canonical_json(value)?;
    if canonical.as_ref() != payload {
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
            let normalized = key.to_ascii_lowercase();
            if matches!(
                normalized.as_str(),
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
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorWriteExecutionId,
        ConnectorWriteOperationId,
    };
    use parquet::basic::Compression;

    use super::*;
    use crate::connector::iceberg::sink_plan::IcebergSinkObjectStoreConfig;

    fn metadata() -> TableMetadata {
        iceberg::spec::TableMetadataBuilder::from_table_creation(
            iceberg::TableCreation::builder()
                .name("t".to_string())
                .location("file:///warehouse/db/t".to_string())
                .schema(
                    iceberg::spec::Schema::builder()
                        .with_schema_id(1)
                        .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                            1,
                            "id",
                            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                        ))])
                        .build()
                        .expect("schema"),
                )
                .partition_spec(iceberg::spec::PartitionSpec::unpartition_spec())
                .format_version(iceberg::spec::FormatVersion::V2)
                .build(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    fn report(content: IcebergFileContent, path: &str) -> IcebergWriterReport {
        IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: path.to_string(),
                format: if content == IcebergFileContent::PositionDeletes {
                    "puffin".to_string()
                } else {
                    "parquet".to_string()
                },
                content,
                record_count: 3,
                file_size_in_bytes: 42,
                partition: IcebergPartitionReport {
                    partition_path: String::new(),
                    null_fingerprint: String::new(),
                    partition_spec_id: 0,
                    partition_values: iceberg::spec::Struct::empty(),
                },
                split_offsets: Some(vec![4, 12]),
                column_stats: Some(IcebergColumnStats {
                    column_sizes: BTreeMap::from([(1, 42)]),
                    value_counts: BTreeMap::from([(1, 3)]),
                    null_value_counts: BTreeMap::new(),
                    nan_value_counts: BTreeMap::new(),
                    lower_bounds: BTreeMap::from([(1, vec![1, 0, 0, 0])]),
                    upper_bounds: BTreeMap::from([(1, vec![3, 0, 0, 0])]),
                }),
                referenced_data_file: (content == IcebergFileContent::PositionDeletes)
                    .then(|| "file:///warehouse/db/t/data/base.parquet".to_string()),
                first_row_id: None,
                equality_ids: (content == IcebergFileContent::EqualityDeletes).then(|| vec![1]),
                key_metadata: Some(vec![9, 8, 7]),
                content_offset: (content == IcebergFileContent::PositionDeletes).then_some(9),
                content_size_in_bytes: (content == IcebergFileContent::PositionDeletes)
                    .then_some(33),
                cardinality: (content == IcebergFileContent::PositionDeletes).then_some(3),
            },
            is_overwrite: Some(false),
            is_rewrite: Some(false),
        }
    }

    fn sink_plan_with_credentials() -> IcebergSinkPlan {
        let schema = Arc::new(Schema::empty());
        IcebergSinkPlan {
            mode: IcebergSinkMode::Data,
            table_location: "s3://warehouse/table".to_string(),
            data_location: "s3://warehouse/table/data".to_string(),
            target_partition_spec_id: 0,
            target_table_metadata: None,
            target_snapshot_id: None,
            position_delete_data_file_partitions: Default::default(),
            position_delete_data_file_partition_index_input: None,
            object_store_s3: Some(IcebergSinkObjectStoreConfig {
                endpoint: "http://minio:9000".to_string(),
                bucket: "warehouse".to_string(),
                access_key_id: "test-access-key".to_string(),
                access_key_secret: "test-access-secret".to_string(),
                session_token: Some("test-session-token".to_string()),
                region: None,
                enable_path_style_access: None,
                retry_max_times: None,
                retry_min_delay_ms: None,
                retry_max_delay_ms: None,
                timeout_ms: None,
                io_timeout_ms: None,
            }),
            file_format: IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::UNCOMPRESSED,
            output_schema: Arc::clone(&schema),
            target_schema: schema,
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            output_exprs: Vec::new(),
            partition_exprs: Vec::new(),
            partition_source_column_names: Vec::new(),
            partition_column_names: Vec::new(),
            transform_exprs: Vec::new(),
            position_delete_binding: None,
        }
    }

    fn writer_identity() -> ConnectorWriterIdentity {
        let binding_key = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg.test").expect("instance"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        ConnectorWriterIdentity::new(
            operation_id,
            novarocks_spi::connector::ConnectorWriteCohortId::primary(operation_id),
            ConnectorWriteExecutionId::new([2; 16], 3),
            [4; 16],
            5,
            6,
            0,
            binding_key,
        )
    }

    #[test]
    fn staged_report_payload_is_deterministic_and_round_trips_data_and_delete_facts() {
        let metadata = metadata();
        let reports = vec![
            report(
                IcebergFileContent::Data,
                "file:///warehouse/db/t/data/a.parquet",
            ),
            report(
                IcebergFileContent::PositionDeletes,
                "file:///warehouse/db/t/delete/a.puffin",
            ),
        ];

        let first = encode_writer_reports(&reports, &metadata).expect("encode first");
        let second = encode_writer_reports(&reports, &metadata).expect("encode second");
        assert_eq!(
            first, second,
            "identical facts need identical canonical bytes"
        );
        let first_staged = staged_report_from_iceberg_reports(
            writer_identity(),
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            &reports,
            &metadata,
        )
        .expect("build first staged report");
        let second_staged = staged_report_from_iceberg_reports(
            writer_identity(),
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            &reports,
            &metadata,
        )
        .expect("build second staged report");
        assert_eq!(
            first_staged.payload_digest(),
            second_staged.payload_digest()
        );
        let decoded = decode_writer_reports(&first, &metadata).expect("decode");
        assert_eq!(decoded.len(), 2);
        assert_eq!(
            (decoded[0].file.path.as_str(), decoded[0].file.content),
            (
                "file:///warehouse/db/t/data/a.parquet",
                IcebergFileContent::Data
            )
        );
        assert_eq!(
            (decoded[1].file.path.as_str(), decoded[1].file.content),
            (
                "file:///warehouse/db/t/delete/a.puffin",
                IcebergFileContent::PositionDeletes
            )
        );
        assert!(
            String::from_utf8(first.to_vec())
                .expect("utf8")
                .contains("\"version\":1")
        );
    }

    #[test]
    fn position_delete_report_replays_frozen_partition_facts_at_control() {
        let metadata = metadata();
        let partition = IcebergPositionDeletePartition {
            partition_path: String::new(),
            null_fingerprint: String::new(),
            partition_spec_id: metadata.default_partition_spec_id(),
            descriptor: IcebergPartitionDescriptor { values: Vec::new() },
            existing_deletion_vector_payload: None,
        };
        let staged = staged_report_from_position_delete_files(
            writer_identity(),
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary {
                input_rows: 2,
                staged_bytes: 17,
                artifact_count: 1,
            },
            &[IcebergPositionDeleteStagedFile {
                path: "file:///warehouse/db/t/data/delete.parquet",
                record_count: 2,
                file_size_in_bytes: 17,
                split_offsets: None,
                column_stats: None,
                referenced_data_file: "file:///warehouse/db/t/data/source.parquet".to_string(),
                partition: &partition,
                format: "parquet",
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            }],
        )
        .expect("stage position delete");
        let reports = decode_writer_reports(staged.payload(), &metadata).expect("control decode");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].file.content, IcebergFileContent::PositionDeletes);
        assert_eq!(
            reports[0].file.referenced_data_file.as_deref(),
            Some("file:///warehouse/db/t/data/source.parquet")
        );
        assert!(
            reports[0]
                .file
                .partition
                .partition_values
                .fields()
                .is_empty()
        );
    }

    #[test]
    fn handle_payload_drops_object_store_credentials() {
        let payload =
            encode_sink_plan_handle_payload(&sink_plan_with_credentials()).expect("encode");
        let payload = String::from_utf8(payload.to_vec()).expect("json");
        assert!(!payload.contains("test-access-key"));
        assert!(!payload.contains("test-access-secret"));
        assert!(!payload.contains("test-session-token"));
        decode_sink_plan_handle_payload(payload.as_bytes()).expect("decode");
    }

    #[test]
    fn data_execution_reconstruction_uses_only_the_local_storage_binding() {
        let mut original = sink_plan_with_credentials();
        original.compression = Compression::SNAPPY;
        let payload = encode_sink_plan_handle_payload(&original).expect("encode handle");
        let local = IcebergSinkObjectStoreConfig {
            endpoint: "http://be-local-minio:9000".to_string(),
            bucket: "warehouse".to_string(),
            access_key_id: "be-local-access".to_string(),
            access_key_secret: "be-local-secret".to_string(),
            session_token: None,
            region: None,
            enable_path_style_access: None,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        };
        let reconstructed = data_sink_plan_from_handle_payload(
            &payload,
            Arc::new(Schema::empty()),
            Some(local.clone()),
        )
        .expect("reconstruct data writer plan");
        assert_eq!(reconstructed.mode, IcebergSinkMode::Data);
        assert_eq!(reconstructed.object_store_s3, Some(local));

        original.mode = IcebergSinkMode::EqualityDeletes;
        let delete_payload = encode_sink_plan_handle_payload(&original).expect("encode delete");
        assert!(
            data_sink_plan_from_handle_payload(&delete_payload, Arc::new(Schema::empty()), None,)
                .is_err()
        );
    }

    #[test]
    fn data_execution_reconstructs_compat_zstd_compression() {
        let mut original = sink_plan_with_credentials();
        original.compression = Compression::ZSTD(ZstdLevel::default());
        let payload = encode_sink_plan_handle_payload(&original).expect("encode handle");
        let reconstructed = data_sink_plan_from_handle_payload(
            &payload,
            Arc::new(Schema::empty()),
            original.object_store_s3.clone(),
        )
        .expect("reconstruct ZSTD writer plan");
        assert_eq!(reconstructed.compression, original.compression);
    }

    #[test]
    fn data_sink_spec_handle_restores_iceberg_field_ids_on_generic_input() {
        let mut spec =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::distributed::write::sink::test_support::unpartitioned_metadata_json(),
        );
        let payload = encode_data_sink_spec_handle_payload(&spec).expect("encode data handle");
        let raw_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        let plan = data_sink_plan_from_handle_payload(&payload, raw_schema, None)
            .expect("reconstruct annotated data plan");
        assert_eq!(
            plan.output_schema.fields()[0]
                .metadata()
                .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn row_lineage_data_handle_preserves_the_data_writer_contract() {
        let mut spec =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        spec.mode = IcebergWriteSinkMode::RowLineageData;
        spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::distributed::write::sink::test_support::unpartitioned_metadata_json(),
        );
        spec.iceberg.schema.fields.extend([
            IcebergSchemaFieldDef {
                field_id: crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                name: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            },
            IcebergSchemaFieldDef {
                field_id:
                    crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                name: crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            },
        ]);
        let payload = encode_data_sink_spec_handle_payload(&spec).expect("encode lineage data");
        let raw_schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new(
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                arrow::datatypes::DataType::Int64,
                false,
            ),
            arrow::datatypes::Field::new(
                crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
                arrow::datatypes::DataType::Int64,
                true,
            ),
        ]));
        let plan = data_sink_plan_from_handle_payload(&payload, raw_schema, None)
            .expect("reconstruct lineage data plan");
        assert!(plan.row_lineage_data);
        assert_eq!(
            plan.output_schema.fields()[1]
                .metadata()
                .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                .map(|value| value.parse::<i32>().expect("field id")),
            Some(crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID)
        );
    }

    #[test]
    fn equality_delete_handle_requires_the_exact_fragment_schema() {
        let mut plan = sink_plan_with_credentials();
        plan.mode = IcebergSinkMode::EqualityDeletes;
        plan.equality_delete_columns = vec![super::super::commit::EqualityDeleteColumn {
            name: "id".to_string(),
            field_id: 7,
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        }];
        let payload = encode_sink_plan_handle_payload(&plan).expect("encode equality handle");
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        let (location, spec_id, columns) =
            equality_delete_handle_from_payload(&payload, schema).expect("decode equality handle");
        assert_eq!(location, "s3://warehouse/table/data");
        assert_eq!(spec_id, 0);
        assert_eq!(columns[0].field_id, 7);

        let wrong_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "other",
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        assert!(equality_delete_handle_from_payload(&payload, wrong_schema).is_err());
    }

    #[test]
    fn unpartitioned_equality_delete_report_round_trips_with_control_metadata() {
        let metadata = metadata();
        let reports = vec![report(
            IcebergFileContent::EqualityDeletes,
            "file:///warehouse/db/t/delete/equality.parquet",
        )];
        let staged = staged_report_from_unpartitioned_equality_delete_reports(
            writer_identity(),
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            &reports,
        )
        .expect("stage equality report");
        let decoded = decode_writer_reports(staged.payload(), &metadata).expect("decode report");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].file.content, IcebergFileContent::EqualityDeletes);
        assert!(
            decoded[0]
                .file
                .partition
                .partition_values
                .fields()
                .is_empty()
        );
    }

    #[test]
    fn position_delete_handle_round_trips_frozen_partition_index() {
        let metadata = metadata();
        let mut spec =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        spec.mode = IcebergWriteSinkMode::PositionDeletes;
        spec.iceberg.current_snapshot_id = metadata.current_snapshot_id();
        let data_file = "file:///warehouse/db/t/data/00001.parquet".to_string();
        let index = HashMap::from([(
            data_file.clone(),
            PositionDeleteDataFilePartition {
                partition_spec_id: metadata.default_partition_spec_id(),
                partition_values: iceberg::spec::Struct::empty(),
            },
        )]);
        let payload =
            encode_position_delete_sink_handle_payload(&spec, &metadata, &index).expect("encode");
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("_file", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("_pos", arrow::datatypes::DataType::Int64, false),
        ]));
        let decoded = position_delete_handle_from_payload(&payload, &schema).expect("decode");
        assert_eq!(decoded.data_location, spec.data_location);
        assert_eq!(decoded.partitions.len(), 1);
        assert_eq!(
            decoded.partitions[&data_file].partition_spec_id,
            metadata.default_partition_spec_id()
        );
        assert!(decoded.partitions[&data_file].descriptor.values.is_empty());

        let schema_with_route_column = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("_file", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("_pos", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("region", arrow::datatypes::DataType::Utf8, false),
        ]));
        position_delete_handle_from_payload(&payload, &schema_with_route_column)
            .expect("accept route-only columns after position-delete fields");

        let invalid_schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("path", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
        ]));
        assert!(position_delete_handle_from_payload(&payload, &invalid_schema).is_err());
    }

    #[test]
    fn deletion_vector_handle_carries_only_frozen_canonical_vector_facts() {
        let metadata = metadata();
        let mut spec =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        spec.mode = IcebergWriteSinkMode::DeletionVectors;
        let data_file = "file:///warehouse/db/t/data/00001.parquet".to_string();
        let partitions = HashMap::from([(
            data_file.clone(),
            PositionDeleteDataFilePartition {
                partition_spec_id: metadata.default_partition_spec_id(),
                partition_values: iceberg::spec::Struct::empty(),
            },
        )]);
        let mut vector = DeletionVector::new();
        vector.insert(3).expect("position");
        vector.insert(9).expect("position");
        let payload = encode_deletion_vector_sink_handle_payload(
            &spec,
            &metadata,
            &partitions,
            &HashMap::from([(data_file.clone(), vector)]),
        )
        .expect("encode");
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("_file", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("_pos", arrow::datatypes::DataType::Int64, false),
        ]));
        let decoded = position_delete_handle_from_payload(&payload, &schema).expect("decode");
        assert_eq!(decoded.mode, IcebergWriteHandleMode::DeletionVectors);
        let vector = DeletionVector::from_iceberg_payload(
            decoded.partitions[&data_file]
                .existing_deletion_vector_payload
                .as_deref()
                .expect("frozen vector"),
        )
        .expect("decode frozen vector");
        assert!(vector.contains(3));
        assert!(vector.contains(9));
    }

    #[test]
    fn malformed_or_noncanonical_payload_is_rejected() {
        let metadata = metadata();
        let malformed = br#"{\"version\":1,\"reports\":[{"#;
        assert!(decode_writer_reports(malformed, &metadata).is_err());

        let canonical = encode_writer_reports(
            &[report(
                IcebergFileContent::Data,
                "file:///warehouse/db/t/data/a.parquet",
            )],
            &metadata,
        )
        .expect("encode");
        let pretty = serde_json::to_vec_pretty(
            &serde_json::from_slice::<serde_json::Value>(&canonical).expect("value"),
        )
        .expect("pretty");
        assert!(decode_writer_reports(&pretty, &metadata).is_err());

        let mut invalid_base64: serde_json::Value =
            serde_json::from_slice(&canonical).expect("value");
        invalid_base64["reports"][0]["file"]["key_metadata_base64"] =
            serde_json::Value::String("not base64!".to_string());
        let invalid_base64 = serde_json::to_vec(&invalid_base64).expect("canonical json");
        assert!(decode_writer_reports(&invalid_base64, &metadata).is_err());
    }

    #[test]
    fn receipt_payload_is_canonical_and_round_trips() {
        let payload = encode_write_receipt(42).expect("encode");
        assert_eq!(decode_write_receipt(&payload).expect("decode"), 42);
        assert_eq!(
            decode_write_receipt(connector_write_receipt(42).expect("receipt").payload())
                .expect("decode connector receipt"),
            42
        );
    }
}
