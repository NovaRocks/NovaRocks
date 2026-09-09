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

use std::collections::BTreeSet;
use std::mem::size_of;

use bytes::Bytes;
use novarocks_spi::connector::read_stack::{
    ConnectorReadSplitFacts, ConnectorSplit, ConnectorTableHandle, SchemaTableName,
};
use novarocks_spi::connector::{
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorDecodeContext, ConnectorFieldPath,
    ConnectorPrivateDecoder, ConnectorPrivateEncoder,
};
use prost::Message;

use crate::domain::{
    PaimonBinaryTableStats, PaimonBucketMode, PaimonColumn, PaimonDataCompression, PaimonDataFile,
    PaimonDataFileFacts, PaimonDeletionFile, PaimonMergeEngine, PaimonReadView, PaimonRowRange,
    PaimonSplit, PaimonTable,
};
use crate::schema::PaimonDataType;

use super::dto;

const MAX_PRIVATE_READ_BYTES: usize = 16 * 1024 * 1024;

struct StrictSchema {
    name: &'static str,
    fields: &'static [(u32, u8)],
    repeated: &'static [u32],
    children: &'static [(u32, &'static StrictSchema)],
}

static OPTIONAL_I64_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.file.stats.null_count",
    fields: &[(1, 0)],
    repeated: &[],
    children: &[],
};
static STATS_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.file.stats",
    fields: &[(1, 2), (2, 2), (3, 2)],
    repeated: &[3],
    children: &[(3, &OPTIONAL_I64_SCHEMA)],
};
static STRING_LIST_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.file.string_list",
    fields: &[(1, 2)],
    repeated: &[1],
    children: &[],
};
static FILE_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.file",
    fields: &[
        (1, 2),
        (2, 0),
        (3, 0),
        (4, 0),
        (5, 0),
        (6, 0),
        (7, 0),
        (8, 0),
        (9, 2),
        (10, 2),
        (11, 2),
        (12, 2),
        (13, 2),
        (14, 0),
        (15, 0),
        (16, 2),
        (17, 0),
        (18, 2),
        (19, 2),
        (20, 0),
        (21, 2),
    ],
    repeated: &[13],
    children: &[
        (11, &STATS_SCHEMA),
        (12, &STATS_SCHEMA),
        (18, &STRING_LIST_SCHEMA),
        (21, &STRING_LIST_SCHEMA),
    ],
};
static DELETION_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.deletion_file",
    fields: &[(1, 2), (2, 0), (3, 0), (4, 0)],
    repeated: &[],
    children: &[],
};
static OPTIONAL_DELETION_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.optional_deletion_file",
    fields: &[(1, 2)],
    repeated: &[],
    children: &[(1, &DELETION_SCHEMA)],
};
static DELETION_LIST_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.deletion_files",
    fields: &[(1, 2)],
    repeated: &[1],
    children: &[(1, &OPTIONAL_DELETION_SCHEMA)],
};
static ROW_RANGE_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.row_range",
    fields: &[(1, 0), (2, 0)],
    repeated: &[],
    children: &[],
};
static ROW_RANGE_LIST_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split.row_ranges",
    fields: &[(1, 2)],
    repeated: &[1],
    children: &[(1, &ROW_RANGE_SCHEMA)],
};
static SPLIT_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_split",
    fields: &[
        (1, 0),
        (2, 0),
        (3, 2),
        (4, 0),
        (5, 2),
        (6, 0),
        (7, 0),
        (8, 2),
        (9, 0),
        (10, 2),
        (11, 2),
        (12, 0),
    ],
    repeated: &[5],
    children: &[
        (5, &FILE_SCHEMA),
        (10, &DELETION_LIST_SCHEMA),
        (11, &ROW_RANGE_LIST_SCHEMA),
    ],
};
static DATA_TYPE_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_column.data_type",
    fields: &[(1, 0), (2, 0), (3, 0), (4, 0)],
    repeated: &[],
    children: &[],
};
static COLUMN_SCHEMA: StrictSchema = StrictSchema {
    name: "paimon_read_column",
    fields: &[(1, 0), (2, 2), (3, 2), (4, 0), (5, 0)],
    repeated: &[],
    children: &[(3, &DATA_TYPE_SCHEMA)],
};

#[derive(Clone, Copy, Debug, Default)]
pub struct PaimonReadWireCodec;

impl ConnectorPrivateEncoder<PaimonTable> for PaimonReadWireCodec {
    fn encode_private(&self, value: &PaimonTable) -> Result<Bytes, ConnectorCodecError> {
        let name = value.schema_table_name();
        Ok(Bytes::from(
            dto::PaimonTablePayload {
                schema_name: name.schema_name().to_string(),
                table_name: name.table_name().to_string(),
                table_location: value.location().to_string(),
                merge_engine: encode_merge(value.merge_engine()) as i32,
                bucket_mode: encode_bucket(value.bucket_mode()) as i32,
                primary_key_field_ids: value.primary_key_field_ids().to_vec(),
                partition_field_ids: value.partition_field_ids().to_vec(),
            }
            .encode_to_vec(),
        ))
    }
}

impl ConnectorPrivateDecoder<PaimonTable> for PaimonReadWireCodec {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<PaimonTable, ConnectorCodecError> {
        let raw = decode_root::<dto::PaimonTablePayload>(
            payload,
            context,
            "paimon_read_table",
            &[1, 2, 3, 4, 5],
            &[6, 7],
        )?;
        let value = PaimonTable::try_new(
            SchemaTableName::try_new(&raw.schema_name, &raw.table_name).map_err(domain_error)?,
            &raw.table_location,
            decode_merge(raw.merge_engine)?,
            decode_bucket(raw.bucket_mode)?,
            raw.primary_key_field_ids,
            raw.partition_field_ids,
        )
        .map_err(domain_error)?;
        charge(context, payload.len(), size_of::<PaimonTable>())?;
        Ok(value)
    }
}

impl ConnectorPrivateEncoder<PaimonColumn> for PaimonReadWireCodec {
    fn encode_private(&self, value: &PaimonColumn) -> Result<Bytes, ConnectorCodecError> {
        Ok(Bytes::from(
            dto::PaimonColumnPayload {
                field_id: Some(value.field_id()),
                name: value.name().to_string(),
                data_type: Some(encode_type(value.data_type())),
                nullable: Some(value.nullable()),
                output_ordinal: Some(value.output_ordinal()),
            }
            .encode_to_vec(),
        ))
    }
}

impl ConnectorPrivateDecoder<PaimonColumn> for PaimonReadWireCodec {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<PaimonColumn, ConnectorCodecError> {
        let raw = decode_strict_root::<dto::PaimonColumnPayload>(payload, context, &COLUMN_SCHEMA)?;
        let data_type = raw
            .data_type
            .as_ref()
            .ok_or_else(|| missing("paimon_read_column.data_type"))?;
        let value = PaimonColumn::try_new(
            raw.field_id
                .ok_or_else(|| missing("paimon_read_column.field_id"))?,
            &raw.name,
            decode_type(data_type)?,
            raw.nullable
                .ok_or_else(|| missing("paimon_read_column.nullable"))?,
            raw.output_ordinal
                .ok_or_else(|| missing("paimon_read_column.output_ordinal"))?,
        )
        .map_err(domain_error)?;
        charge(
            context,
            payload.len(),
            size_of::<PaimonColumn>() + raw.name.len(),
        )?;
        Ok(value)
    }
}

impl ConnectorPrivateEncoder<PaimonReadView> for PaimonReadWireCodec {
    fn encode_private(&self, value: &PaimonReadView) -> Result<Bytes, ConnectorCodecError> {
        Ok(Bytes::from(
            dto::PaimonReadViewPayload {
                table_location: value.table_location().to_string(),
                snapshot_id: value.snapshot_id(),
                schema_id: Some(value.schema_id()),
                schema_fingerprint: value.schema_fingerprint().to_vec(),
                read_recipe_digest: value.read_recipe_digest().to_vec(),
                sequence_field_id: value.sequence_field_id(),
            }
            .encode_to_vec(),
        ))
    }
}

impl ConnectorPrivateDecoder<PaimonReadView> for PaimonReadWireCodec {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<PaimonReadView, ConnectorCodecError> {
        let raw = decode_root::<dto::PaimonReadViewPayload>(
            payload,
            context,
            "paimon_read_view",
            &[1, 2, 3, 4, 5, 6],
            &[],
        )?;
        let schema_fingerprint = fixed_32(
            &raw.schema_fingerprint,
            "paimon_read_view.schema_fingerprint",
        )?;
        let read_recipe_digest = fixed_32(
            &raw.read_recipe_digest,
            "paimon_read_view.read_recipe_digest",
        )?;
        let value = PaimonReadView::try_new(
            &raw.table_location,
            raw.snapshot_id,
            raw.schema_id
                .ok_or_else(|| missing("paimon_read_view.schema_id"))?,
            schema_fingerprint,
            read_recipe_digest,
            raw.sequence_field_id,
        )
        .map_err(domain_error)?;
        charge(
            context,
            payload.len(),
            size_of::<PaimonReadView>() + raw.table_location.len(),
        )?;
        Ok(value)
    }
}

impl ConnectorPrivateEncoder<PaimonSplit> for PaimonReadWireCodec {
    fn encode_private(&self, value: &PaimonSplit) -> Result<Bytes, ConnectorCodecError> {
        let encoded = dto::PaimonReadSplitPayload {
            snapshot_id: Some(value.snapshot_id()),
            schema_id: Some(value.schema_id()),
            partition: value.partition().to_vec(),
            bucket: Some(value.bucket()),
            files: value.files().iter().map(encode_file).collect(),
            contains_delete_rows: Some(value.contains_delete_rows()),
            partition_arity: Some(value.partition_arity()),
            bucket_path: value.bucket_path().to_string(),
            total_buckets: Some(value.total_buckets()),
            data_deletion_files: value.data_deletion_files().map(|values| {
                dto::PaimonDeletionFilesPayload {
                    values: values
                        .iter()
                        .map(|value| dto::PaimonOptionalDeletionFilePayload {
                            value: value.as_ref().map(encode_deletion_file),
                        })
                        .collect(),
                }
            }),
            row_ranges: value
                .row_ranges()
                .map(|values| dto::PaimonRowRangesPayload {
                    values: values
                        .iter()
                        .map(|value| dto::PaimonRowRangePayload {
                            from: Some(value.from()),
                            to: Some(value.to()),
                        })
                        .collect(),
                }),
            raw_convertible: Some(value.raw_convertible()),
        }
        .encode_to_vec();
        if encoded.len() > MAX_PRIVATE_READ_BYTES {
            return Err(capacity(
                "paimon_read_split",
                "Paimon private payload exceeds 16 MiB",
            ));
        }
        Ok(Bytes::from(encoded))
    }
}

impl PaimonReadWireCodec {
    pub fn decode_split_private(
        &self,
        payload: &[u8],
        facts: &ConnectorReadSplitFacts,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<PaimonSplit, ConnectorCodecError> {
        if !facts.remotely_accessible() || !facts.addresses().is_empty() {
            return Err(invalid(
                "paimon_read_split.facts",
                "Paimon object-store split must be remotely accessible without node addresses",
            ));
        }
        let raw =
            decode_strict_root::<dto::PaimonReadSplitPayload>(payload, context, &SPLIT_SCHEMA)?;
        let files = raw
            .files
            .iter()
            .map(decode_file)
            .collect::<Result<Vec<_>, _>>()?;
        let split = PaimonSplit::try_new(
            raw.snapshot_id
                .ok_or_else(|| missing("paimon_read_split.snapshot_id"))?,
            raw.schema_id
                .ok_or_else(|| missing("paimon_read_split.schema_id"))?,
            raw.partition_arity
                .ok_or_else(|| missing("paimon_read_split.partition_arity"))?,
            raw.partition,
            raw.bucket
                .ok_or_else(|| missing("paimon_read_split.bucket"))?,
            &raw.bucket_path,
            raw.total_buckets
                .ok_or_else(|| missing("paimon_read_split.total_buckets"))?,
            files,
            raw.data_deletion_files
                .map(decode_deletion_files)
                .transpose()?,
            raw.row_ranges.map(decode_row_ranges).transpose()?,
            raw.raw_convertible
                .ok_or_else(|| missing("paimon_read_split.raw_convertible"))?,
            raw.contains_delete_rows
                .ok_or_else(|| missing("paimon_read_split.contains_delete_rows"))?,
            facts.split_weight(),
        )
        .map_err(domain_error)?;
        if split.retained_size_in_bytes() != facts.retained_size_in_bytes() {
            return Err(invalid(
                "paimon_read_split.facts.retained_size",
                "Paimon split retained size does not match the public carrier",
            ));
        }
        charge(
            context,
            payload.len(),
            usize::try_from(split.retained_size_in_bytes()).unwrap_or(usize::MAX),
        )?;
        Ok(split)
    }
}

fn encode_file(value: &PaimonDataFile) -> dto::PaimonDataFilePayload {
    let facts = value.facts();
    dto::PaimonDataFilePayload {
        file_name: facts.file_name.clone(),
        file_size: Some(value.file_size()),
        schema_id: Some(value.schema_id()),
        level: Some(value.level()),
        min_sequence_number: Some(value.min_sequence_number()),
        max_sequence_number: Some(value.max_sequence_number()),
        row_count: Some(value.row_count()),
        compression: encode_compression(value.compression()) as i32,
        min_key: facts.min_key.clone(),
        max_key: facts.max_key.clone(),
        key_stats: Some(encode_stats(&facts.key_stats)),
        value_stats: Some(encode_stats(&facts.value_stats)),
        extra_files: facts.extra_files.clone(),
        creation_time_millis: facts.creation_time_millis,
        delete_row_count: facts.delete_row_count,
        embedded_index: facts.embedded_index.clone(),
        file_source: facts.file_source,
        value_stats_cols: facts.value_stats_cols.as_ref().map(|values| {
            dto::PaimonStringListPayload {
                values: values.clone(),
            }
        }),
        external_path: facts.external_path.clone(),
        first_row_id: facts.first_row_id,
        write_cols: facts
            .write_cols
            .as_ref()
            .map(|values| dto::PaimonStringListPayload {
                values: values.clone(),
            }),
    }
}

fn decode_file(raw: &dto::PaimonDataFilePayload) -> Result<PaimonDataFile, ConnectorCodecError> {
    PaimonDataFile::try_new(PaimonDataFileFacts {
        file_name: raw.file_name.clone(),
        file_size: raw
            .file_size
            .ok_or_else(|| missing("paimon_read_split.file.file_size"))?,
        row_count: raw
            .row_count
            .ok_or_else(|| missing("paimon_read_split.file.row_count"))?,
        min_key: raw.min_key.clone(),
        max_key: raw.max_key.clone(),
        key_stats: decode_stats(
            raw.key_stats
                .as_ref()
                .ok_or_else(|| missing("paimon_read_split.file.key_stats"))?,
        )?,
        value_stats: decode_stats(
            raw.value_stats
                .as_ref()
                .ok_or_else(|| missing("paimon_read_split.file.value_stats"))?,
        )?,
        min_sequence_number: raw
            .min_sequence_number
            .ok_or_else(|| missing("paimon_read_split.file.min_sequence_number"))?,
        max_sequence_number: raw
            .max_sequence_number
            .ok_or_else(|| missing("paimon_read_split.file.max_sequence_number"))?,
        schema_id: raw
            .schema_id
            .ok_or_else(|| missing("paimon_read_split.file.schema_id"))?,
        level: raw
            .level
            .ok_or_else(|| missing("paimon_read_split.file.level"))?,
        extra_files: raw.extra_files.clone(),
        creation_time_millis: raw.creation_time_millis,
        delete_row_count: raw.delete_row_count,
        embedded_index: raw.embedded_index.clone(),
        file_source: raw.file_source,
        value_stats_cols: raw
            .value_stats_cols
            .as_ref()
            .map(|values| values.values.clone()),
        external_path: raw.external_path.clone(),
        first_row_id: raw.first_row_id,
        write_cols: raw.write_cols.as_ref().map(|values| values.values.clone()),
        compression: decode_compression(raw.compression)?,
    })
    .map_err(domain_error)
}

fn encode_stats(value: &PaimonBinaryTableStats) -> dto::PaimonBinaryTableStatsPayload {
    dto::PaimonBinaryTableStatsPayload {
        min_values: value.min_values().to_vec(),
        max_values: value.max_values().to_vec(),
        null_counts: value
            .null_counts()
            .iter()
            .map(|value| dto::PaimonOptionalInt64Payload { value: *value })
            .collect(),
    }
}

fn decode_stats(
    raw: &dto::PaimonBinaryTableStatsPayload,
) -> Result<PaimonBinaryTableStats, ConnectorCodecError> {
    PaimonBinaryTableStats::try_new(
        raw.min_values.clone(),
        raw.max_values.clone(),
        raw.null_counts.iter().map(|value| value.value).collect(),
    )
    .map_err(domain_error)
}

fn encode_deletion_file(value: &PaimonDeletionFile) -> dto::PaimonDeletionFilePayload {
    dto::PaimonDeletionFilePayload {
        path: value.path().to_string(),
        offset: Some(value.offset()),
        length: Some(value.length()),
        cardinality: value.cardinality(),
    }
}

fn decode_deletion_files(
    raw: dto::PaimonDeletionFilesPayload,
) -> Result<Vec<Option<PaimonDeletionFile>>, ConnectorCodecError> {
    raw.values
        .into_iter()
        .map(|value| {
            value
                .value
                .map(|value| {
                    PaimonDeletionFile::try_new(
                        &value.path,
                        value
                            .offset
                            .ok_or_else(|| missing("paimon_read_split.deletion_file.offset"))?,
                        value
                            .length
                            .ok_or_else(|| missing("paimon_read_split.deletion_file.length"))?,
                        value.cardinality,
                    )
                    .map_err(domain_error)
                })
                .transpose()
        })
        .collect()
}

fn decode_row_ranges(
    raw: dto::PaimonRowRangesPayload,
) -> Result<Vec<PaimonRowRange>, ConnectorCodecError> {
    raw.values
        .into_iter()
        .map(|value| {
            PaimonRowRange::try_new(
                value
                    .from
                    .ok_or_else(|| missing("paimon_read_split.row_range.from"))?,
                value
                    .to
                    .ok_or_else(|| missing("paimon_read_split.row_range.to"))?,
            )
            .map_err(domain_error)
        })
        .collect()
}

fn encode_type(value: PaimonDataType) -> dto::PaimonDataTypePayload {
    use dto::PaimonTypeKind as K;
    let (kind, precision, scale, timestamp_precision) = match value {
        PaimonDataType::Boolean => (K::Boolean, None, None, None),
        PaimonDataType::Int8 => (K::Int8, None, None, None),
        PaimonDataType::Int16 => (K::Int16, None, None, None),
        PaimonDataType::Int32 => (K::Int32, None, None, None),
        PaimonDataType::Int64 => (K::Int64, None, None, None),
        PaimonDataType::Float32 => (K::Float32, None, None, None),
        PaimonDataType::Float64 => (K::Float64, None, None, None),
        PaimonDataType::Decimal128 { precision, scale } => (
            K::Decimal128,
            Some(u32::from(precision)),
            Some(u32::from(scale)),
            None,
        ),
        PaimonDataType::Utf8 => (K::Utf8, None, None, None),
        PaimonDataType::Binary => (K::Binary, None, None, None),
        PaimonDataType::Date32 => (K::Date32, None, None, None),
        PaimonDataType::TimestampMillis { precision } => {
            (K::TimestampMillis, None, None, Some(u32::from(precision)))
        }
        PaimonDataType::TimestampMicros { precision } => {
            (K::TimestampMicros, None, None, Some(u32::from(precision)))
        }
    };
    dto::PaimonDataTypePayload {
        kind: kind as i32,
        decimal_precision: precision,
        decimal_scale: scale,
        timestamp_precision,
    }
}

fn decode_type(raw: &dto::PaimonDataTypePayload) -> Result<PaimonDataType, ConnectorCodecError> {
    use dto::PaimonTypeKind as K;
    let kind = K::try_from(raw.kind).map_err(|_| {
        invalid(
            "paimon_read_column.data_type.kind",
            "unknown Paimon type kind",
        )
    })?;
    let no_parameters = || {
        if raw.decimal_precision.is_none()
            && raw.decimal_scale.is_none()
            && raw.timestamp_precision.is_none()
        {
            Ok(())
        } else {
            Err(invalid(
                "paimon_read_column.data_type",
                "Paimon type has inconsistent parameters",
            ))
        }
    };
    let plain = |value| {
        no_parameters()?;
        Ok(value)
    };
    match kind {
        K::Boolean => plain(PaimonDataType::Boolean),
        K::Int8 => plain(PaimonDataType::Int8),
        K::Int16 => plain(PaimonDataType::Int16),
        K::Int32 => plain(PaimonDataType::Int32),
        K::Int64 => plain(PaimonDataType::Int64),
        K::Float32 => plain(PaimonDataType::Float32),
        K::Float64 => plain(PaimonDataType::Float64),
        K::Utf8 => plain(PaimonDataType::Utf8),
        K::Binary => plain(PaimonDataType::Binary),
        K::Date32 => plain(PaimonDataType::Date32),
        K::Decimal128 => PaimonDataType::decimal(
            u8::try_from(
                raw.decimal_precision
                    .ok_or_else(|| missing("paimon_read_column.data_type.decimal_precision"))?,
            )
            .map_err(|_| {
                invalid(
                    "paimon_read_column.data_type.decimal_precision",
                    "precision overflows",
                )
            })?,
            u8::try_from(
                raw.decimal_scale
                    .ok_or_else(|| missing("paimon_read_column.data_type.decimal_scale"))?,
            )
            .map_err(|_| {
                invalid(
                    "paimon_read_column.data_type.decimal_scale",
                    "scale overflows",
                )
            })?,
        )
        .map_err(domain_error),
        K::TimestampMillis | K::TimestampMicros => {
            if raw.decimal_precision.is_some() || raw.decimal_scale.is_some() {
                return Err(invalid(
                    "paimon_read_column.data_type",
                    "timestamp carries decimal parameters",
                ));
            }
            let precision = u8::try_from(
                raw.timestamp_precision
                    .ok_or_else(|| missing("paimon_read_column.data_type.timestamp_precision"))?,
            )
            .map_err(|_| {
                invalid(
                    "paimon_read_column.data_type.timestamp_precision",
                    "precision overflows",
                )
            })?;
            let value = PaimonDataType::timestamp(precision).map_err(domain_error)?;
            if (kind == K::TimestampMillis)
                != matches!(value, PaimonDataType::TimestampMillis { .. })
            {
                return Err(invalid(
                    "paimon_read_column.data_type.kind",
                    "timestamp carrier does not match precision",
                ));
            }
            Ok(value)
        }
        K::Unspecified => Err(invalid(
            "paimon_read_column.data_type.kind",
            "Paimon type kind is required",
        )),
    }
}

fn encode_merge(value: PaimonMergeEngine) -> dto::PaimonMergeEngine {
    match value {
        PaimonMergeEngine::AppendOnly => dto::PaimonMergeEngine::AppendOnly,
        PaimonMergeEngine::Deduplicate => dto::PaimonMergeEngine::Deduplicate,
    }
}
fn decode_merge(value: i32) -> Result<PaimonMergeEngine, ConnectorCodecError> {
    match dto::PaimonMergeEngine::try_from(value).ok() {
        Some(dto::PaimonMergeEngine::AppendOnly) => Ok(PaimonMergeEngine::AppendOnly),
        Some(dto::PaimonMergeEngine::Deduplicate) => Ok(PaimonMergeEngine::Deduplicate),
        _ => Err(invalid(
            "paimon_read_table.merge_engine",
            "unsupported Paimon merge engine",
        )),
    }
}
fn encode_bucket(value: PaimonBucketMode) -> dto::PaimonBucketMode {
    match value {
        PaimonBucketMode::Unbucketed => dto::PaimonBucketMode::Unbucketed,
        PaimonBucketMode::Fixed => dto::PaimonBucketMode::Fixed,
        PaimonBucketMode::Dynamic => dto::PaimonBucketMode::Dynamic,
    }
}
fn decode_bucket(value: i32) -> Result<PaimonBucketMode, ConnectorCodecError> {
    match dto::PaimonBucketMode::try_from(value).ok() {
        Some(dto::PaimonBucketMode::Unbucketed) => Ok(PaimonBucketMode::Unbucketed),
        Some(dto::PaimonBucketMode::Fixed) => Ok(PaimonBucketMode::Fixed),
        Some(dto::PaimonBucketMode::Dynamic) => Ok(PaimonBucketMode::Dynamic),
        _ => Err(invalid(
            "paimon_read_table.bucket_mode",
            "unsupported Paimon bucket mode",
        )),
    }
}
fn encode_compression(value: PaimonDataCompression) -> dto::PaimonDataCompression {
    match value {
        PaimonDataCompression::Uncompressed => dto::PaimonDataCompression::Uncompressed,
        PaimonDataCompression::Snappy => dto::PaimonDataCompression::Snappy,
        PaimonDataCompression::Zstd => dto::PaimonDataCompression::Zstd,
        PaimonDataCompression::Lz4Raw => dto::PaimonDataCompression::Lz4Raw,
    }
}
fn decode_compression(value: i32) -> Result<PaimonDataCompression, ConnectorCodecError> {
    match dto::PaimonDataCompression::try_from(value).ok() {
        Some(dto::PaimonDataCompression::Uncompressed) => Ok(PaimonDataCompression::Uncompressed),
        Some(dto::PaimonDataCompression::Snappy) => Ok(PaimonDataCompression::Snappy),
        Some(dto::PaimonDataCompression::Zstd) => Ok(PaimonDataCompression::Zstd),
        Some(dto::PaimonDataCompression::Lz4Raw) => Ok(PaimonDataCompression::Lz4Raw),
        _ => Err(invalid(
            "paimon_read_split.file.compression",
            "unsupported Paimon compression",
        )),
    }
}

fn decode_strict_root<M: Message + Default>(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    schema: &'static StrictSchema,
) -> Result<M, ConnectorCodecError> {
    if payload.len() > MAX_PRIVATE_READ_BYTES {
        return Err(capacity(
            schema.name,
            "Paimon private payload exceeds 16 MiB",
        ));
    }
    context.ledger().charge_raw(payload.len())?;
    scan_strict_message(payload, context, schema, 0)?;
    M::decode(payload).map_err(|error| {
        invalid(
            schema.name,
            format!("malformed Paimon private protobuf: {error}"),
        )
    })
}

fn scan_strict_message(
    mut input: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    schema: &'static StrictSchema,
    depth: usize,
) -> Result<(), ConnectorCodecError> {
    context.ledger().check_depth(depth)?;
    let mut seen = BTreeSet::new();
    while !input.is_empty() {
        context.ledger().charge_items(1)?;
        let key = read_varint(&mut input, schema.name)?;
        let field = u32::try_from(key >> 3).map_err(|_| malformed(schema.name))?;
        let wire = u8::try_from(key & 7).map_err(|_| malformed(schema.name))?;
        let Some((_, expected_wire)) = schema.fields.iter().find(|(number, _)| *number == field)
        else {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root(schema.name).field(format!("field_{field}")),
                ConnectorCodecErrorKind::UnknownField,
                "Paimon private payload contains an unknown nested field",
            ));
        };
        if wire != *expected_wire {
            return Err(invalid(
                schema.name,
                format!(
                    "Paimon private field {field} uses wire type {wire}, expected {expected_wire}"
                ),
            ));
        }
        if !schema.repeated.contains(&field) && !seen.insert(field) {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root(schema.name).field(format!("field_{field}")),
                ConnectorCodecErrorKind::DuplicateField,
                "Paimon private payload repeats a singular nested field",
            ));
        }
        if let Some((_, child)) = schema.children.iter().find(|(number, _)| *number == field) {
            if wire != 2 {
                return Err(malformed(schema.name));
            }
            let len = usize::try_from(read_varint(&mut input, schema.name)?)
                .map_err(|_| malformed(schema.name))?;
            let nested = take(&mut input, len, schema.name)?;
            scan_strict_message(nested, context, child, depth + 1)?;
        } else {
            skip_value(&mut input, wire, context, schema.name)?;
        }
    }
    Ok(())
}

fn decode_root<M: Message + Default>(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    name: &'static str,
    singular: &[u32],
    repeated: &[u32],
) -> Result<M, ConnectorCodecError> {
    if payload.len() > MAX_PRIVATE_READ_BYTES {
        return Err(capacity(name, "Paimon private payload exceeds 16 MiB"));
    }
    context.ledger().charge_raw(payload.len())?;
    scan_root(payload, context, name, singular, repeated)?;
    M::decode(payload)
        .map_err(|error| invalid(name, format!("malformed Paimon private protobuf: {error}")))
}

fn scan_root(
    mut input: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    name: &'static str,
    singular: &[u32],
    repeated: &[u32],
) -> Result<(), ConnectorCodecError> {
    let mut seen = BTreeSet::new();
    while !input.is_empty() {
        context.ledger().charge_items(1)?;
        let key = read_varint(&mut input, name)?;
        let field = u32::try_from(key >> 3).map_err(|_| malformed(name))?;
        let wire = u8::try_from(key & 7).map_err(|_| malformed(name))?;
        if field == 0 || (!singular.contains(&field) && !repeated.contains(&field)) {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root(name).field(format!("field_{field}")),
                ConnectorCodecErrorKind::UnknownField,
                "Paimon private payload contains an unknown field",
            ));
        }
        if singular.contains(&field) && !seen.insert(field) {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root(name).field(format!("field_{field}")),
                ConnectorCodecErrorKind::DuplicateField,
                "Paimon private payload repeats a singular field",
            ));
        }
        skip_value(&mut input, wire, context, name)?;
    }
    Ok(())
}

fn skip_value(
    input: &mut &[u8],
    wire: u8,
    context: &mut ConnectorDecodeContext<'_>,
    name: &'static str,
) -> Result<(), ConnectorCodecError> {
    match wire {
        0 => read_varint(input, name).map(|_| ()),
        1 => take(input, 8, name).map(|_| ()),
        2 => {
            let len = usize::try_from(read_varint(input, name)?).map_err(|_| malformed(name))?;
            context.ledger().charge_scalar(len)?;
            take(input, len, name).map(|_| ())
        }
        5 => take(input, 4, name).map(|_| ()),
        _ => Err(malformed(name)),
    }
}
fn read_varint(input: &mut &[u8], name: &'static str) -> Result<u64, ConnectorCodecError> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let Some((&byte, rest)) = input.split_first() else {
            return Err(malformed(name));
        };
        *input = rest;
        if shift == 63 && byte > 1 {
            return Err(malformed(name));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(malformed(name))
}
fn take<'a>(
    input: &mut &'a [u8],
    len: usize,
    name: &'static str,
) -> Result<&'a [u8], ConnectorCodecError> {
    if input.len() < len {
        return Err(malformed(name));
    }
    let (value, rest) = input.split_at(len);
    *input = rest;
    Ok(value)
}
fn malformed(name: &'static str) -> ConnectorCodecError {
    invalid(name, "malformed Paimon private protobuf structure")
}
fn charge(
    context: &mut ConnectorDecodeContext<'_>,
    raw: usize,
    concrete: usize,
) -> Result<(), ConnectorCodecError> {
    context
        .ledger()
        .charge_retained(concrete.saturating_add(raw.saturating_mul(2)))
}
fn fixed_32(value: &[u8], path: &'static str) -> Result<[u8; 32], ConnectorCodecError> {
    value
        .try_into()
        .map_err(|_| invalid(path, "digest must contain exactly 32 bytes"))
}
fn missing(path: &'static str) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root(path),
        ConnectorCodecErrorKind::MissingField,
        "required Paimon field is missing",
    )
}
fn invalid(path: &'static str, detail: impl AsRef<str>) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root(path),
        ConnectorCodecErrorKind::InvalidValue,
        detail,
    )
}
fn capacity(path: &'static str, detail: &'static str) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root(path),
        ConnectorCodecErrorKind::Capacity,
        detail,
    )
}
fn domain_error(error: impl std::fmt::Display) -> ConnectorCodecError {
    invalid("paimon_payload", error.to_string())
}
