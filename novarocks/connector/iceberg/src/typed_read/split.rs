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

//! One schedulable byte range of one Iceberg data file.
//!
//! A split is self-contained: it carries its full applicable delete closure,
//! its partition values, its statistics domain, and its sequence facts, so a
//! worker never consults the catalog, a remote dictionary, or a sibling split
//! to read it. That costs bounded duplication of delete descriptors and buys
//! a scheduler with no cross-split lifetime to manage.

use std::sync::Arc;

use crate::wire::dto;
use novarocks_proto_codec::connector_read::{
    MAX_AFFINITY_KEY_BYTES, MAX_DELETES_PER_SPLIT, MAX_ENCRYPTION_MATERIAL_BYTES,
    MAX_EQUALITY_FIELD_IDS, MAX_JSON_BYTES, MAX_PATH_BYTES,
};
use novarocks_spi::connector::read_stack::{ConnectorSplit, HostAddress, SplitWeight, TupleDomain};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::iceberg::spec::DataFileFormat;

use super::column_handle::{
    IcebergColumnHandle, decode_tuple_domain, encode_tuple_domain, invalid, unsupported,
};

/// Trino's default `iceberg.minimum-assigned-split-weight`.
pub const DEFAULT_MINIMUM_ASSIGNED_SPLIT_WEIGHT: f64 = 0.05;

/// The physical format of one Iceberg data or delete file.
///
/// This field is the only format authority in the read stack. A path suffix is
/// never inspected: an Iceberg file's format is a manifest fact, and a table
/// may legally name a Parquet file anything at all.
///
/// `Puffin` is a delete-artifact container that addresses a deletion vector by
/// offset and size inside it. A data file is never Puffin.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergFileFormat {
    Orc,
    Parquet,
    Avro,
    Puffin,
}

impl IcebergFileFormat {
    /// Map any manifest file format onto the read contract's closed set.
    pub const fn from_manifest_format(format: DataFileFormat) -> Self {
        match format {
            DataFileFormat::Orc => Self::Orc,
            DataFileFormat::Parquet => Self::Parquet,
            DataFileFormat::Avro => Self::Avro,
            DataFileFormat::Puffin => Self::Puffin,
        }
    }

    /// Map the manifest format of a *data* file.
    ///
    /// Puffin is rejected here rather than carried: it names a delete artifact,
    /// and a data file that claims it is malformed planning input.
    pub fn from_data_file_format(format: DataFileFormat) -> Result<Self, ConnectorError> {
        match Self::from_manifest_format(format) {
            format @ (Self::Orc | Self::Parquet | Self::Avro) => Ok(format),
            Self::Puffin => Err(unsupported(
                "an iceberg data file is never in the puffin delete-artifact format",
            )),
        }
    }

    fn to_proto(self) -> dto::IcebergFileFormat {
        match self {
            Self::Orc => dto::IcebergFileFormat::Orc,
            Self::Parquet => dto::IcebergFileFormat::Parquet,
            Self::Avro => dto::IcebergFileFormat::Avro,
            Self::Puffin => dto::IcebergFileFormat::Puffin,
        }
    }

    fn from_proto(raw: i32) -> Result<Self, ConnectorError> {
        let format = dto::IcebergFileFormat::try_from(raw)
            .map_err(|_| invalid("unknown iceberg file format"))?;
        match format {
            dto::IcebergFileFormat::Unspecified => {
                Err(invalid("iceberg file format must be specified"))
            }
            dto::IcebergFileFormat::Orc => Ok(Self::Orc),
            dto::IcebergFileFormat::Parquet => Ok(Self::Parquet),
            dto::IcebergFileFormat::Avro => Ok(Self::Avro),
            dto::IcebergFileFormat::Puffin => Ok(Self::Puffin),
        }
    }
}

/// What a delete file deletes by.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergDeleteFileContent {
    PositionDeletes,
    EqualityDeletes,
}

impl IcebergDeleteFileContent {
    fn to_proto(self) -> dto::IcebergDeleteFileContent {
        match self {
            Self::PositionDeletes => dto::IcebergDeleteFileContent::PositionDeletes,
            Self::EqualityDeletes => dto::IcebergDeleteFileContent::EqualityDeletes,
        }
    }

    fn from_proto(raw: i32) -> Result<Self, ConnectorError> {
        let content = dto::IcebergDeleteFileContent::try_from(raw)
            .map_err(|_| invalid("unknown iceberg delete file content"))?;
        match content {
            dto::IcebergDeleteFileContent::Unspecified => {
                Err(invalid("iceberg delete file content must be specified"))
            }
            dto::IcebergDeleteFileContent::PositionDeletes => Ok(Self::PositionDeletes),
            dto::IcebergDeleteFileContent::EqualityDeletes => Ok(Self::EqualityDeletes),
        }
    }
}

/// Parquet modular-encryption material.
///
/// The type exists so the contract has a place for it; this stack implements
/// no modular encryption, and both split production and reader admission
/// reject a non-empty value. `Debug` redacts every byte so key material can
/// never reach a log.
#[derive(Clone, Eq, PartialEq)]
pub struct ParquetFileDecryptionData {
    key_metadata: Vec<u8>,
    aad_prefix: Vec<u8>,
}

impl ParquetFileDecryptionData {
    pub fn try_new(key_metadata: Vec<u8>, aad_prefix: Vec<u8>) -> Result<Self, ConnectorError> {
        if key_metadata.len() > MAX_ENCRYPTION_MATERIAL_BYTES
            || aad_prefix.len() > MAX_ENCRYPTION_MATERIAL_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg parquet decryption material exceeds the hard limit",
            ));
        }
        Ok(Self {
            key_metadata,
            aad_prefix,
        })
    }

    pub fn key_metadata(&self) -> &[u8] {
        &self.key_metadata
    }

    pub fn aad_prefix(&self) -> &[u8] {
        &self.aad_prefix
    }

    fn retained_size_in_bytes(&self) -> usize {
        self.key_metadata.len() + self.aad_prefix.len()
    }

    fn to_proto(&self) -> dto::ParquetFileDecryptionData {
        dto::ParquetFileDecryptionData {
            key_metadata: self.key_metadata.clone(),
            aad_prefix: self.aad_prefix.clone(),
        }
    }

    fn from_proto(raw: &dto::ParquetFileDecryptionData) -> Result<Self, ConnectorError> {
        Self::try_new(raw.key_metadata.clone(), raw.aad_prefix.clone())
    }
}

impl std::fmt::Debug for ParquetFileDecryptionData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetFileDecryptionData")
            .field("key_metadata", &"<redacted>")
            .field("aad_prefix", &"<redacted>")
            .finish()
    }
}

/// The exact facts one delete descriptor carries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergDeleteFileParams {
    pub content: IcebergDeleteFileContent,
    pub path: String,
    pub format: IcebergFileFormat,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    /// Equality field IDs in table-schema order.
    pub equality_field_ids: Vec<i32>,
    pub row_position_lower_bound: Option<i64>,
    pub row_position_upper_bound: Option<i64>,
    pub data_sequence_number: i64,
    /// Puffin deletion vector only.
    pub content_offset: Option<i64>,
    /// Puffin deletion vector only.
    pub content_size_in_bytes: Option<i64>,
    /// Iceberg manifest identity of the data file this delete artifact applies to.
    pub referenced_data_file: Option<String>,
    pub decryption_data: Option<ParquetFileDecryptionData>,
}

/// One delete file that applies to the data file a split reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergDeleteFile {
    content: IcebergDeleteFileContent,
    path: Arc<str>,
    format: IcebergFileFormat,
    record_count: i64,
    file_size_in_bytes: i64,
    equality_field_ids: Vec<i32>,
    row_position_lower_bound: Option<i64>,
    row_position_upper_bound: Option<i64>,
    data_sequence_number: i64,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
    referenced_data_file: Option<Arc<str>>,
    decryption_data: Option<ParquetFileDecryptionData>,
}

impl IcebergDeleteFile {
    pub fn try_new(params: IcebergDeleteFileParams) -> Result<Self, ConnectorError> {
        let IcebergDeleteFileParams {
            content,
            path,
            format,
            record_count,
            file_size_in_bytes,
            equality_field_ids,
            row_position_lower_bound,
            row_position_upper_bound,
            data_sequence_number,
            content_offset,
            content_size_in_bytes,
            referenced_data_file,
            decryption_data,
        } = params;

        if path.is_empty() || path.len() > MAX_PATH_BYTES {
            return Err(invalid(
                "iceberg delete file path must be non-empty and bounded",
            ));
        }
        if let Some(referenced_data_file) = &referenced_data_file
            && (referenced_data_file.is_empty() || referenced_data_file.len() > MAX_PATH_BYTES)
        {
            return Err(invalid(
                "iceberg delete referenced data file must be non-empty and bounded",
            ));
        }
        if record_count < 0 || file_size_in_bytes < 0 || data_sequence_number < 0 {
            return Err(invalid(
                "iceberg delete file counts, sizes, and sequence numbers must be nonnegative",
            ));
        }
        if equality_field_ids.len() > MAX_EQUALITY_FIELD_IDS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg equality field id count exceeds the hard limit",
            ));
        }
        match content {
            IcebergDeleteFileContent::PositionDeletes => {
                if !equality_field_ids.is_empty() {
                    return Err(invalid(
                        "an iceberg position-delete file must not carry equality field ids",
                    ));
                }
            }
            IcebergDeleteFileContent::EqualityDeletes => {
                if equality_field_ids.is_empty() {
                    return Err(invalid(
                        "an iceberg equality-delete file requires equality field ids",
                    ));
                }
                if equality_field_ids.iter().any(|field_id| *field_id <= 0) {
                    return Err(invalid("iceberg equality field ids must be positive"));
                }
                if content_offset.is_some() || content_size_in_bytes.is_some() {
                    return Err(invalid(
                        "an iceberg equality-delete file has no puffin content range",
                    ));
                }
            }
        }
        // A content range is a pair: an offset without a size cannot be read,
        // and a size without an offset has no anchor.
        if content_offset.is_some() != content_size_in_bytes.is_some() {
            return Err(invalid(
                "an iceberg puffin content range requires both an offset and a size",
            ));
        }
        // A deletion vector is a blob addressed inside a Puffin container, so
        // the format and the content range imply each other exactly.
        if (format == IcebergFileFormat::Puffin) != content_offset.is_some() {
            return Err(invalid(
                "an iceberg puffin delete file requires a content range, and only puffin has one",
            ));
        }
        for value in [content_offset, content_size_in_bytes] {
            if value.is_some_and(|value| value < 0) {
                return Err(invalid(
                    "iceberg puffin content offsets and sizes must be nonnegative",
                ));
            }
        }
        if let (Some(lower), Some(upper)) = (row_position_lower_bound, row_position_upper_bound)
            && lower > upper
        {
            return Err(invalid(
                "iceberg delete row-position lower bound is above its upper bound",
            ));
        }
        for value in [row_position_lower_bound, row_position_upper_bound] {
            if value.is_some_and(|value| value < 0) {
                return Err(invalid("iceberg delete row positions must be nonnegative"));
            }
        }

        Ok(Self {
            content,
            path: Arc::from(path.as_str()),
            format,
            record_count,
            file_size_in_bytes,
            equality_field_ids,
            row_position_lower_bound,
            row_position_upper_bound,
            data_sequence_number,
            content_offset,
            content_size_in_bytes,
            referenced_data_file: referenced_data_file.map(Arc::from),
            decryption_data,
        })
    }

    pub const fn content(&self) -> IcebergDeleteFileContent {
        self.content
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub const fn format(&self) -> IcebergFileFormat {
        self.format
    }

    pub const fn record_count(&self) -> i64 {
        self.record_count
    }

    pub const fn file_size_in_bytes(&self) -> i64 {
        self.file_size_in_bytes
    }

    pub fn equality_field_ids(&self) -> &[i32] {
        &self.equality_field_ids
    }

    pub const fn row_position_lower_bound(&self) -> Option<i64> {
        self.row_position_lower_bound
    }

    pub const fn row_position_upper_bound(&self) -> Option<i64> {
        self.row_position_upper_bound
    }

    pub const fn data_sequence_number(&self) -> i64 {
        self.data_sequence_number
    }

    pub const fn content_offset(&self) -> Option<i64> {
        self.content_offset
    }

    pub const fn content_size_in_bytes(&self) -> Option<i64> {
        self.content_size_in_bytes
    }

    pub fn referenced_data_file(&self) -> Option<&str> {
        self.referenced_data_file.as_deref()
    }

    pub const fn decryption_data(&self) -> Option<&ParquetFileDecryptionData> {
        self.decryption_data.as_ref()
    }

    fn retained_size_in_bytes(&self) -> usize {
        size_of::<Self>()
            + self.path.len()
            + self
                .referenced_data_file
                .as_ref()
                .map_or(0, |path| path.len())
            + self.equality_field_ids.len() * size_of::<i32>()
            + self
                .decryption_data
                .as_ref()
                .map_or(0, ParquetFileDecryptionData::retained_size_in_bytes)
    }

    pub fn to_proto(&self) -> dto::IcebergDeleteFile {
        dto::IcebergDeleteFile {
            content: self.content.to_proto() as i32,
            path: self.path.to_string(),
            format: self.format.to_proto() as i32,
            record_count: self.record_count,
            file_size_in_bytes: self.file_size_in_bytes,
            equality_field_ids: self.equality_field_ids.clone(),
            row_position_lower_bound: self.row_position_lower_bound,
            row_position_upper_bound: self.row_position_upper_bound,
            data_sequence_number: self.data_sequence_number,
            content_offset: self.content_offset,
            content_size_in_bytes: self.content_size_in_bytes,
            referenced_data_file: self.referenced_data_file.as_ref().map(ToString::to_string),
            decryption_data: self
                .decryption_data
                .as_ref()
                .map(ParquetFileDecryptionData::to_proto),
        }
    }

    pub fn from_proto(raw: &dto::IcebergDeleteFile) -> Result<Self, ConnectorError> {
        Self::try_new(IcebergDeleteFileParams {
            content: IcebergDeleteFileContent::from_proto(raw.content)?,
            path: raw.path.clone(),
            format: IcebergFileFormat::from_proto(raw.format)?,
            record_count: raw.record_count,
            file_size_in_bytes: raw.file_size_in_bytes,
            equality_field_ids: raw.equality_field_ids.clone(),
            row_position_lower_bound: raw.row_position_lower_bound,
            row_position_upper_bound: raw.row_position_upper_bound,
            data_sequence_number: raw.data_sequence_number,
            content_offset: raw.content_offset,
            content_size_in_bytes: raw.content_size_in_bytes,
            referenced_data_file: raw.referenced_data_file.clone(),
            decryption_data: raw
                .decryption_data
                .as_ref()
                .map(ParquetFileDecryptionData::from_proto)
                .transpose()?,
        })
    }
}

/// The exact facts one Iceberg data split carries.
#[derive(Clone, Debug)]
pub struct IcebergSplitParams {
    pub path: String,
    pub start: i64,
    pub length: i64,
    pub file_size: i64,
    pub file_record_count: i64,
    pub file_format: IcebergFileFormat,
    pub partition_spec_id: i32,
    pub partition_data_json: String,
    /// The complete applicable delete closure, in planner order.
    pub deletes: Vec<IcebergDeleteFile>,
    pub file_statistics_domain: TupleDomain<IcebergColumnHandle>,
    pub data_sequence_number: Option<i64>,
    pub file_first_row_id: Option<i64>,
    pub decryption_data: Option<ParquetFileDecryptionData>,
    pub split_weight: SplitWeight,
    /// A co-location hint, never an identity.
    pub affinity_key: Option<String>,
}

/// One byte range of one Iceberg data file.
#[derive(Clone, Debug)]
pub struct IcebergSplit {
    path: Arc<str>,
    start: i64,
    length: i64,
    file_size: i64,
    file_record_count: i64,
    file_format: IcebergFileFormat,
    partition_spec_id: i32,
    partition_data_json: Arc<str>,
    deletes: Vec<IcebergDeleteFile>,
    file_statistics_domain: TupleDomain<IcebergColumnHandle>,
    data_sequence_number: Option<i64>,
    file_first_row_id: Option<i64>,
    decryption_data: Option<ParquetFileDecryptionData>,
    split_weight: SplitWeight,
    affinity_key: Option<Arc<str>>,
    retained_size_in_bytes: u64,
}

impl IcebergSplit {
    pub fn try_new(params: IcebergSplitParams) -> Result<Self, ConnectorError> {
        let IcebergSplitParams {
            path,
            start,
            length,
            file_size,
            file_record_count,
            file_format,
            partition_spec_id,
            partition_data_json,
            deletes,
            file_statistics_domain,
            data_sequence_number,
            file_first_row_id,
            decryption_data,
            split_weight,
            affinity_key,
        } = params;

        if path.is_empty() || path.len() > MAX_PATH_BYTES {
            return Err(invalid("iceberg split path must be non-empty and bounded"));
        }
        if file_format == IcebergFileFormat::Puffin {
            return Err(invalid(
                "an iceberg data split is never in the puffin delete-artifact format",
            ));
        }
        if start < 0 || length < 0 || file_size < 0 || file_record_count < 0 {
            return Err(invalid(
                "iceberg split offsets, lengths, and counts must be nonnegative",
            ));
        }
        // The range must live inside the file: an overflowing end would let a
        // reader ask an object store for bytes that provably do not exist.
        let end = start
            .checked_add(length)
            .ok_or_else(|| invalid("iceberg split byte range overflows"))?;
        if end > file_size {
            return Err(invalid("iceberg split byte range exceeds its file size"));
        }
        if partition_data_json.is_empty() || partition_data_json.len() > MAX_JSON_BYTES {
            return Err(invalid(
                "iceberg split partition data json must be non-empty and bounded",
            ));
        }
        if deletes.len() > MAX_DELETES_PER_SPLIT {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg split delete count exceeds the hard limit",
            ));
        }
        if data_sequence_number.is_some_and(|value| value < 0) {
            return Err(invalid(
                "iceberg split data sequence number must be nonnegative",
            ));
        }
        if file_first_row_id.is_some_and(|value| value < 0) {
            return Err(invalid("iceberg split first row id must be nonnegative"));
        }
        if let Some(affinity_key) = affinity_key.as_deref()
            && (affinity_key.is_empty() || affinity_key.len() > MAX_AFFINITY_KEY_BYTES)
        {
            return Err(invalid(
                "iceberg split affinity key must be non-empty and bounded when present",
            ));
        }

        let mut split = Self {
            path: Arc::from(path.as_str()),
            start,
            length,
            file_size,
            file_record_count,
            file_format,
            partition_spec_id,
            partition_data_json: Arc::from(partition_data_json.as_str()),
            deletes,
            file_statistics_domain,
            data_sequence_number,
            file_first_row_id,
            decryption_data,
            split_weight,
            affinity_key: affinity_key.map(|key| Arc::from(key.as_str())),
            retained_size_in_bytes: 0,
        };
        split.retained_size_in_bytes = split.compute_retained_size_in_bytes();
        Ok(split)
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub const fn start(&self) -> i64 {
        self.start
    }

    pub const fn length(&self) -> i64 {
        self.length
    }

    pub const fn file_size(&self) -> i64 {
        self.file_size
    }

    pub const fn file_record_count(&self) -> i64 {
        self.file_record_count
    }

    pub const fn file_format(&self) -> IcebergFileFormat {
        self.file_format
    }

    pub const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub fn partition_data_json(&self) -> &str {
        &self.partition_data_json
    }

    pub fn deletes(&self) -> &[IcebergDeleteFile] {
        &self.deletes
    }

    pub const fn file_statistics_domain(&self) -> &TupleDomain<IcebergColumnHandle> {
        &self.file_statistics_domain
    }

    pub const fn data_sequence_number(&self) -> Option<i64> {
        self.data_sequence_number
    }

    pub const fn file_first_row_id(&self) -> Option<i64> {
        self.file_first_row_id
    }

    pub const fn decryption_data(&self) -> Option<&ParquetFileDecryptionData> {
        self.decryption_data.as_ref()
    }

    /// Whether this split covers its whole data file.
    pub const fn is_whole_file(&self) -> bool {
        self.start == 0 && self.length == self.file_size
    }

    fn compute_retained_size_in_bytes(&self) -> u64 {
        let mut retained = size_of::<Self>()
            + self.path.len()
            + self.partition_data_json.len()
            + self.affinity_key.as_ref().map_or(0, |key| key.len())
            + self
                .decryption_data
                .as_ref()
                .map_or(0, ParquetFileDecryptionData::retained_size_in_bytes);
        for delete in &self.deletes {
            retained += delete.retained_size_in_bytes();
        }
        if let Some(domains) = self.file_statistics_domain.domains() {
            for (column, domain) in domains {
                retained += size_of::<IcebergColumnHandle>()
                    + column.base_type_json().len()
                    + column.type_json().len()
                    + size_of_val(column.field_id_path());
                for range in domain.values().ranges() {
                    retained += range.low().value().map_or(
                        0,
                        novarocks_spi::connector::read_stack::ConnectorValue::payload_bytes,
                    ) + range.high().value().map_or(
                        0,
                        novarocks_spi::connector::read_stack::ConnectorValue::payload_bytes,
                    );
                }
            }
        }
        retained as u64
    }

    pub fn to_proto(&self) -> dto::IcebergSplit {
        dto::IcebergSplit {
            path: self.path.to_string(),
            start: self.start,
            length: self.length,
            file_size: self.file_size,
            file_record_count: self.file_record_count,
            file_format: self.file_format.to_proto() as i32,
            partition_spec_id: self.partition_spec_id,
            partition_data_json: self.partition_data_json.to_string(),
            deletes: self
                .deletes
                .iter()
                .map(IcebergDeleteFile::to_proto)
                .collect(),
            file_statistics_domain: Some(encode_tuple_domain(&self.file_statistics_domain)),
            data_sequence_number: self.data_sequence_number,
            file_first_row_id: self.file_first_row_id,
            decryption_data: self
                .decryption_data
                .as_ref()
                .map(ParquetFileDecryptionData::to_proto),
        }
    }

    pub fn from_proto(
        raw: &dto::IcebergSplit,
        split_weight: SplitWeight,
        affinity_key: Option<String>,
    ) -> Result<Self, ConnectorError> {
        let mut deletes = Vec::with_capacity(raw.deletes.len());
        for delete in &raw.deletes {
            deletes.push(IcebergDeleteFile::from_proto(delete)?);
        }
        let file_statistics_domain = raw
            .file_statistics_domain
            .as_ref()
            .ok_or_else(|| invalid("iceberg split requires a file statistics domain"))?;

        Self::try_new(IcebergSplitParams {
            path: raw.path.clone(),
            start: raw.start,
            length: raw.length,
            file_size: raw.file_size,
            file_record_count: raw.file_record_count,
            file_format: IcebergFileFormat::from_proto(raw.file_format)?,
            partition_spec_id: raw.partition_spec_id,
            partition_data_json: raw.partition_data_json.clone(),
            deletes,
            file_statistics_domain: decode_tuple_domain(file_statistics_domain)?,
            data_sequence_number: raw.data_sequence_number,
            file_first_row_id: raw.file_first_row_id,
            decryption_data: raw
                .decryption_data
                .as_ref()
                .map(ParquetFileDecryptionData::from_proto)
                .transpose()?,
            split_weight,
            affinity_key,
        })
    }
}

impl ConnectorSplit for IcebergSplit {
    fn is_remotely_accessible(&self) -> bool {
        true
    }

    fn addresses(&self) -> &[HostAddress] {
        &[]
    }

    fn affinity_key(&self) -> Option<&str> {
        self.affinity_key.as_deref()
    }

    fn split_weight(&self) -> SplitWeight {
        self.split_weight
    }

    fn retained_size_in_bytes(&self) -> u64 {
        self.retained_size_in_bytes
    }
}

/// The scheduling parameters one split weight is computed against.
#[derive(Clone, Copy, Debug)]
pub struct IcebergSplitWeightParameters {
    pub target_split_size: u64,
    pub minimum_assigned_split_weight: f64,
}

impl IcebergSplitWeightParameters {
    pub fn try_new(
        target_split_size: u64,
        minimum_assigned_split_weight: f64,
    ) -> Result<Self, ConnectorError> {
        if target_split_size == 0 {
            return Err(invalid("iceberg target split size must be positive"));
        }
        if !minimum_assigned_split_weight.is_finite()
            || minimum_assigned_split_weight <= 0.0
            || minimum_assigned_split_weight > 1.0
        {
            return Err(invalid(
                "iceberg minimum assigned split weight must be in (0, 1]",
            ));
        }
        Ok(Self {
            target_split_size,
            minimum_assigned_split_weight,
        })
    }
}

/// Trino's Iceberg split-weight formula.
///
/// The data weight is the range's share of one target-sized split. Position
/// deletes double it once, because the whole delete set is read alongside the
/// range regardless of how many files it spans. Equality deletes scale it by
/// their total record count, because every row of the range is matched against
/// every equality row. The result is clamped so a tiny range is never free and
/// a heavily deleted one never outweighs a full standard split.
pub fn iceberg_split_weight(
    length: i64,
    deletes: &[IcebergDeleteFile],
    parameters: IcebergSplitWeightParameters,
) -> Result<SplitWeight, ConnectorError> {
    if length < 0 {
        return Err(invalid("iceberg split length must be nonnegative"));
    }
    let data_weight = length as f64 / parameters.target_split_size as f64;
    let mut weight = data_weight;
    if deletes
        .iter()
        .any(|delete| delete.content() == IcebergDeleteFileContent::PositionDeletes)
    {
        weight += data_weight;
    }
    let equality_delete_records: i64 = deletes
        .iter()
        .filter(|delete| delete.content() == IcebergDeleteFileContent::EqualityDeletes)
        .map(IcebergDeleteFile::record_count)
        .sum();
    weight += equality_delete_records as f64 * data_weight;
    SplitWeight::from_proportion(weight.clamp(parameters.minimum_assigned_split_weight, 1.0))
}

#[cfg(test)]
pub(super) mod tests {
    use novarocks_spi::connector::read_stack::STANDARD_SPLIT_WEIGHT_RAW;

    use super::*;

    pub(in crate::typed_read) fn position_delete(path: &str) -> IcebergDeleteFile {
        IcebergDeleteFile::try_new(IcebergDeleteFileParams {
            content: IcebergDeleteFileContent::PositionDeletes,
            path: path.to_string(),
            format: IcebergFileFormat::Parquet,
            record_count: 3,
            file_size_in_bytes: 64,
            equality_field_ids: Vec::new(),
            row_position_lower_bound: Some(0),
            row_position_upper_bound: Some(2),
            data_sequence_number: 5,
            content_offset: None,
            content_size_in_bytes: None,
            referenced_data_file: None,
            decryption_data: None,
        })
        .expect("position delete")
    }

    pub(in crate::typed_read) fn equality_delete(
        path: &str,
        record_count: i64,
    ) -> IcebergDeleteFile {
        IcebergDeleteFile::try_new(IcebergDeleteFileParams {
            content: IcebergDeleteFileContent::EqualityDeletes,
            path: path.to_string(),
            format: IcebergFileFormat::Parquet,
            record_count,
            file_size_in_bytes: 128,
            equality_field_ids: vec![1],
            row_position_lower_bound: None,
            row_position_upper_bound: None,
            data_sequence_number: 6,
            content_offset: None,
            content_size_in_bytes: None,
            referenced_data_file: None,
            decryption_data: None,
        })
        .expect("equality delete")
    }

    fn split_params(path: &str, start: i64, length: i64, file_size: i64) -> IcebergSplitParams {
        IcebergSplitParams {
            path: path.to_string(),
            start,
            length,
            file_size,
            file_record_count: 100,
            file_format: IcebergFileFormat::Parquet,
            partition_spec_id: 0,
            partition_data_json: "{}".to_string(),
            deletes: Vec::new(),
            file_statistics_domain: TupleDomain::all(),
            data_sequence_number: Some(4),
            file_first_row_id: Some(1000),
            decryption_data: None,
            split_weight: SplitWeight::STANDARD,
            affinity_key: Some(path.to_string()),
        }
    }

    #[test]
    fn a_split_rejects_a_range_outside_its_file() {
        assert!(IcebergSplit::try_new(split_params("a.parquet", 0, 10, 10)).is_ok());
        assert!(IcebergSplit::try_new(split_params("a.parquet", 5, 10, 10)).is_err());
        assert!(IcebergSplit::try_new(split_params("a.parquet", -1, 1, 10)).is_err());
        assert!(IcebergSplit::try_new(split_params("", 0, 1, 10)).is_err());
    }

    #[test]
    fn the_neutral_envelope_is_remote_and_addressless() {
        let split = IcebergSplit::try_new(split_params("a.parquet", 0, 10, 10)).expect("split");
        assert!(split.is_remotely_accessible());
        assert!(ConnectorSplit::addresses(&split).is_empty());
        assert_eq!(ConnectorSplit::affinity_key(&split), Some("a.parquet"));
        assert!(ConnectorSplit::retained_size_in_bytes(&split) > 0);
        assert!(split.is_whole_file());
    }

    #[test]
    fn delete_descriptors_reject_contradictory_content_facts() {
        let mut params = IcebergDeleteFileParams {
            content: IcebergDeleteFileContent::EqualityDeletes,
            path: "d.parquet".to_string(),
            format: IcebergFileFormat::Parquet,
            record_count: 1,
            file_size_in_bytes: 1,
            equality_field_ids: Vec::new(),
            row_position_lower_bound: None,
            row_position_upper_bound: None,
            data_sequence_number: 1,
            content_offset: None,
            content_size_in_bytes: None,
            referenced_data_file: None,
            decryption_data: None,
        };
        assert!(IcebergDeleteFile::try_new(params.clone()).is_err());

        params.equality_field_ids = vec![1];
        assert!(IcebergDeleteFile::try_new(params.clone()).is_ok());

        params.content_offset = Some(4);
        assert!(IcebergDeleteFile::try_new(params.clone()).is_err());

        let mut position = params.clone();
        position.content = IcebergDeleteFileContent::PositionDeletes;
        position.equality_field_ids = Vec::new();
        position.content_offset = Some(4);
        position.content_size_in_bytes = None;
        assert!(IcebergDeleteFile::try_new(position.clone()).is_err());

        // A content range only exists inside a Puffin container.
        position.content_size_in_bytes = Some(8);
        assert!(IcebergDeleteFile::try_new(position.clone()).is_err());
        position.format = IcebergFileFormat::Puffin;
        assert!(IcebergDeleteFile::try_new(position.clone()).is_ok());
        position.content_offset = None;
        position.content_size_in_bytes = None;
        assert!(IcebergDeleteFile::try_new(position.clone()).is_err());

        position.format = IcebergFileFormat::Parquet;
        position.content_offset = None;
        position.content_size_in_bytes = None;
        position.row_position_lower_bound = Some(9);
        position.row_position_upper_bound = Some(2);
        assert!(IcebergDeleteFile::try_new(position).is_err());
    }

    #[test]
    fn split_weight_follows_the_trino_formula_and_its_clamp() {
        let parameters =
            IcebergSplitWeightParameters::try_new(100, DEFAULT_MINIMUM_ASSIGNED_SPLIT_WEIGHT)
                .expect("parameters");

        // A half-target range is half a standard split, rounded up.
        assert_eq!(
            iceberg_split_weight(50, &[], parameters)
                .expect("weight")
                .raw_value(),
            STANDARD_SPLIT_WEIGHT_RAW / 2
        );

        // Position deletes add the data weight once, however many there are.
        let one_position =
            iceberg_split_weight(25, &[position_delete("p0.parquet")], parameters).expect("weight");
        let two_positions = iceberg_split_weight(
            25,
            &[position_delete("p0.parquet"), position_delete("p1.parquet")],
            parameters,
        )
        .expect("weight");
        assert_eq!(one_position.raw_value(), 50);
        assert_eq!(one_position, two_positions);

        // Equality deletes scale the data weight by their total record count:
        // a quarter-target range plus two equality rows is 0.25 + 2 * 0.25.
        assert_eq!(
            iceberg_split_weight(25, &[equality_delete("e0.parquet", 2)], parameters)
                .expect("weight")
                .raw_value(),
            75
        );
        // Record counts sum across equality delete files.
        assert_eq!(
            iceberg_split_weight(
                25,
                &[
                    equality_delete("e0.parquet", 1),
                    equality_delete("e1.parquet", 1)
                ],
                parameters,
            )
            .expect("weight")
            .raw_value(),
            75
        );

        // Clamped below by the minimum and above by one standard split.
        assert_eq!(
            iceberg_split_weight(1, &[], parameters)
                .expect("weight")
                .raw_value(),
            5
        );
        assert_eq!(
            iceberg_split_weight(1_000, &[], parameters)
                .expect("weight")
                .raw_value(),
            STANDARD_SPLIT_WEIGHT_RAW
        );

        assert!(IcebergSplitWeightParameters::try_new(0, 0.05).is_err());
        assert!(IcebergSplitWeightParameters::try_new(100, 0.0).is_err());
        assert!(IcebergSplitWeightParameters::try_new(100, 1.5).is_err());
    }

    #[test]
    fn splits_round_trip_through_the_private_wire_value() {
        let mut params = split_params("s3://bucket/a.parquet", 0, 128, 256);
        params.deletes = vec![
            position_delete("p0.parquet"),
            equality_delete("e0.parquet", 4),
        ];
        params.split_weight = SplitWeight::try_from_raw(37).expect("weight");
        let split = IcebergSplit::try_new(params).expect("split");

        let encoded = split.to_proto();
        let decoded = IcebergSplit::from_proto(
            &encoded,
            ConnectorSplit::split_weight(&split),
            ConnectorSplit::affinity_key(&split).map(ToOwned::to_owned),
        )
        .expect("decoded");
        assert_eq!(decoded.path(), split.path());
        assert_eq!(decoded.start(), split.start());
        assert_eq!(decoded.length(), split.length());
        assert_eq!(decoded.deletes(), split.deletes());
        assert_eq!(decoded.file_format(), split.file_format());
        assert_eq!(decoded.data_sequence_number(), split.data_sequence_number());
        assert_eq!(decoded.file_first_row_id(), split.file_first_row_id());
        assert_eq!(
            ConnectorSplit::split_weight(&decoded),
            ConnectorSplit::split_weight(&split)
        );
        assert_eq!(
            ConnectorSplit::affinity_key(&decoded),
            ConnectorSplit::affinity_key(&split)
        );
    }

    #[test]
    fn an_unspecified_private_wire_split_is_rejected() {
        let split = IcebergSplit::try_new(split_params("a.parquet", 0, 10, 10)).expect("split");
        let mut raw = split.to_proto();
        raw.file_format = dto::IcebergFileFormat::Unspecified as i32;
        assert!(IcebergSplit::from_proto(&raw, SplitWeight::STANDARD, None).is_err());
    }

    #[test]
    fn decryption_material_never_reaches_a_debug_rendering() {
        let material =
            ParquetFileDecryptionData::try_new(vec![1, 2, 3], vec![4, 5]).expect("decryption data");
        let rendered = format!("{material:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains('1'));
    }

    #[test]
    fn puffin_is_a_delete_artifact_and_never_a_data_file_format() {
        assert_eq!(
            IcebergFileFormat::from_data_file_format(DataFileFormat::Parquet).expect("parquet"),
            IcebergFileFormat::Parquet
        );
        assert_eq!(
            IcebergFileFormat::from_manifest_format(DataFileFormat::Puffin),
            IcebergFileFormat::Puffin
        );
        let error = IcebergFileFormat::from_data_file_format(DataFileFormat::Puffin)
            .expect_err("puffin is not a data-file format");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);

        let mut params = split_params("dv.puffin", 0, 10, 10);
        params.file_format = IcebergFileFormat::Puffin;
        assert!(IcebergSplit::try_new(params).is_err());
    }
}
