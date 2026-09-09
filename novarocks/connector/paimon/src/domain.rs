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
use std::sync::Arc;

use novarocks_spi::connector::provider::ProviderReadTypes;
use novarocks_spi::connector::read_stack::{
    ColumnHandle, ConnectorSplit, ConnectorTableHandle, SchemaTableName, SplitWeight,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::schema::PaimonDataType;

pub const MAX_PAIMON_COLUMNS: usize = 16_384;
pub const MAX_PAIMON_FILES_PER_SPLIT: usize = 4_096;
pub const MAX_PAIMON_LOCATION_BYTES: usize = 16 * 1024;
pub const MAX_PAIMON_NESTED_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_PAIMON_ROW_RANGES: usize = 1_000_000;
pub const MAX_PAIMON_SPLIT_RETAINED_BYTES: u64 = 12 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PaimonMergeEngine {
    AppendOnly,
    Deduplicate,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PaimonBucketMode {
    Unbucketed,
    Fixed,
    Dynamic,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PaimonDataCompression {
    Uncompressed,
    Snappy,
    Zstd,
    Lz4Raw,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PaimonColumn {
    field_id: i32,
    name: Arc<str>,
    data_type: PaimonDataType,
    nullable: bool,
    output_ordinal: u32,
}

impl PaimonColumn {
    pub fn try_new(
        field_id: i32,
        name: impl AsRef<str>,
        data_type: PaimonDataType,
        nullable: bool,
        output_ordinal: u32,
    ) -> Result<Self, ConnectorError> {
        let name = name.as_ref();
        if field_id < 0 || name.is_empty() || name.len() > 1_024 {
            return Err(invalid("Paimon column identity is invalid or unbounded"));
        }
        Ok(Self {
            field_id,
            name: Arc::from(name),
            data_type,
            nullable,
            output_ordinal,
        })
    }

    pub const fn field_id(&self) -> i32 {
        self.field_id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub const fn data_type(&self) -> PaimonDataType {
        self.data_type
    }
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
    pub const fn output_ordinal(&self) -> u32 {
        self.output_ordinal
    }
}

impl ColumnHandle for PaimonColumn {}

#[derive(Clone, Debug)]
pub struct PaimonTable {
    name: SchemaTableName,
    location: Arc<str>,
    merge_engine: PaimonMergeEngine,
    bucket_mode: PaimonBucketMode,
    primary_key_field_ids: Arc<[i32]>,
    partition_field_ids: Arc<[i32]>,
}

impl PaimonTable {
    pub fn try_new(
        name: SchemaTableName,
        location: impl AsRef<str>,
        merge_engine: PaimonMergeEngine,
        bucket_mode: PaimonBucketMode,
        primary_key_field_ids: Vec<i32>,
        partition_field_ids: Vec<i32>,
    ) -> Result<Self, ConnectorError> {
        let location = bounded_location(location.as_ref())?;
        validate_field_ids(&primary_key_field_ids)?;
        validate_field_ids(&partition_field_ids)?;
        if merge_engine == PaimonMergeEngine::AppendOnly && !primary_key_field_ids.is_empty() {
            return Err(invalid(
                "append-only Paimon table cannot declare primary keys",
            ));
        }
        if merge_engine == PaimonMergeEngine::Deduplicate && primary_key_field_ids.is_empty() {
            return Err(invalid("deduplicate Paimon table requires primary keys"));
        }
        Ok(Self {
            name,
            location,
            merge_engine,
            bucket_mode,
            primary_key_field_ids: Arc::from(primary_key_field_ids),
            partition_field_ids: Arc::from(partition_field_ids),
        })
    }

    pub fn location(&self) -> &str {
        &self.location
    }
    pub const fn merge_engine(&self) -> PaimonMergeEngine {
        self.merge_engine
    }
    pub const fn bucket_mode(&self) -> PaimonBucketMode {
        self.bucket_mode
    }
    pub fn primary_key_field_ids(&self) -> &[i32] {
        &self.primary_key_field_ids
    }
    pub fn partition_field_ids(&self) -> &[i32] {
        &self.partition_field_ids
    }
}

impl ConnectorTableHandle for PaimonTable {
    fn schema_table_name(&self) -> &SchemaTableName {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaimonReadView {
    table_location: Arc<str>,
    snapshot_id: Option<i64>,
    schema_id: i64,
    schema_fingerprint: [u8; 32],
    read_recipe_digest: [u8; 32],
    sequence_field_id: Option<i32>,
}

impl PaimonReadView {
    pub fn try_new(
        table_location: impl AsRef<str>,
        snapshot_id: Option<i64>,
        schema_id: i64,
        schema_fingerprint: [u8; 32],
        read_recipe_digest: [u8; 32],
        sequence_field_id: Option<i32>,
    ) -> Result<Self, ConnectorError> {
        if snapshot_id.is_some_and(|id| id < 0)
            || schema_id < 0
            || sequence_field_id.is_some_and(|id| id < 0)
        {
            return Err(invalid("Paimon frozen view has a negative identity"));
        }
        Ok(Self {
            table_location: bounded_location(table_location.as_ref())?,
            snapshot_id,
            schema_id,
            schema_fingerprint,
            read_recipe_digest,
            sequence_field_id,
        })
    }

    pub fn table_location(&self) -> &str {
        &self.table_location
    }
    pub const fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }
    pub const fn schema_id(&self) -> i64 {
        self.schema_id
    }
    pub const fn schema_fingerprint(&self) -> &[u8; 32] {
        &self.schema_fingerprint
    }
    pub const fn read_recipe_digest(&self) -> &[u8; 32] {
        &self.read_recipe_digest
    }
    pub const fn sequence_field_id(&self) -> Option<i32> {
        self.sequence_field_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaimonBinaryTableStats {
    min_values: Arc<[u8]>,
    max_values: Arc<[u8]>,
    null_counts: Arc<[Option<i64>]>,
}

impl PaimonBinaryTableStats {
    pub fn try_new(
        min_values: Vec<u8>,
        max_values: Vec<u8>,
        null_counts: Vec<Option<i64>>,
    ) -> Result<Self, ConnectorError> {
        if min_values.len() > MAX_PAIMON_NESTED_BYTES
            || max_values.len() > MAX_PAIMON_NESTED_BYTES
            || null_counts.len() > MAX_PAIMON_COLUMNS
            || null_counts.iter().flatten().any(|count| *count < 0)
        {
            return Err(invalid(
                "Paimon binary table statistics are invalid or unbounded",
            ));
        }
        Ok(Self {
            min_values: Arc::from(min_values),
            max_values: Arc::from(max_values),
            null_counts: Arc::from(null_counts),
        })
    }
    pub fn min_values(&self) -> &[u8] {
        &self.min_values
    }
    pub fn max_values(&self) -> &[u8] {
        &self.max_values
    }
    pub fn null_counts(&self) -> &[Option<i64>] {
        &self.null_counts
    }
    fn retained_size(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.min_values.len())
            .saturating_add(self.max_values.len())
            .saturating_add(
                self.null_counts
                    .len()
                    .saturating_mul(size_of::<Option<i64>>()),
            )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaimonDataFileFacts {
    pub file_name: String,
    pub file_size: u64,
    pub row_count: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub key_stats: PaimonBinaryTableStats,
    pub value_stats: PaimonBinaryTableStats,
    pub min_sequence_number: i64,
    pub max_sequence_number: i64,
    pub schema_id: i64,
    pub level: i32,
    pub extra_files: Vec<String>,
    pub creation_time_millis: Option<i64>,
    pub delete_row_count: Option<u64>,
    pub embedded_index: Option<Vec<u8>>,
    pub file_source: Option<i32>,
    pub value_stats_cols: Option<Vec<String>>,
    pub external_path: Option<String>,
    pub first_row_id: Option<i64>,
    pub write_cols: Option<Vec<String>>,
    pub compression: PaimonDataCompression,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaimonDataFile {
    facts: PaimonDataFileFacts,
}

impl PaimonDataFile {
    pub fn try_new(facts: PaimonDataFileFacts) -> Result<Self, ConnectorError> {
        if facts.file_name.is_empty()
            || facts.file_name.len() > MAX_PAIMON_LOCATION_BYTES
            || facts.file_size > i64::MAX as u64
            || facts.row_count > i64::MAX as u64
            || facts.schema_id < 0
            || facts.level < 0
            || facts.min_sequence_number < 0
            || facts.max_sequence_number < facts.min_sequence_number
            || facts
                .delete_row_count
                .is_some_and(|count| count > facts.row_count || count > i64::MAX as u64)
            || facts.first_row_id.is_some_and(|id| id < 0)
            || facts.min_key.len() > MAX_PAIMON_NESTED_BYTES
            || facts.max_key.len() > MAX_PAIMON_NESTED_BYTES
            || facts
                .key_stats
                .null_counts()
                .iter()
                .flatten()
                .any(|count| *count as u64 > facts.row_count)
            || facts
                .value_stats
                .null_counts()
                .iter()
                .flatten()
                .any(|count| *count as u64 > facts.row_count)
            || facts
                .embedded_index
                .as_ref()
                .is_some_and(|v| v.len() > MAX_PAIMON_NESTED_BYTES)
        {
            return Err(invalid("Paimon data file facts are invalid or unbounded"));
        }
        validate_strings(&facts.extra_files, MAX_PAIMON_FILES_PER_SPLIT)?;
        validate_optional_strings(&facts.value_stats_cols, MAX_PAIMON_COLUMNS)?;
        validate_optional_strings(&facts.write_cols, MAX_PAIMON_COLUMNS)?;
        if let Some(path) = &facts.external_path {
            bounded_location(path)?;
        }
        Ok(Self { facts })
    }
    pub fn facts(&self) -> &PaimonDataFileFacts {
        &self.facts
    }
    pub fn file_name(&self) -> &str {
        &self.facts.file_name
    }
    pub const fn file_size(&self) -> u64 {
        self.facts.file_size
    }
    pub const fn schema_id(&self) -> i64 {
        self.facts.schema_id
    }
    pub const fn level(&self) -> i32 {
        self.facts.level
    }
    pub const fn min_sequence_number(&self) -> i64 {
        self.facts.min_sequence_number
    }
    pub const fn max_sequence_number(&self) -> i64 {
        self.facts.max_sequence_number
    }
    pub const fn row_count(&self) -> u64 {
        self.facts.row_count
    }
    pub const fn compression(&self) -> PaimonDataCompression {
        self.facts.compression
    }
    fn retained_size(&self) -> usize {
        let facts = &self.facts;
        size_of::<Self>()
            .saturating_add(facts.file_name.len())
            .saturating_add(facts.min_key.len())
            .saturating_add(facts.max_key.len())
            .saturating_add(facts.key_stats.retained_size())
            .saturating_add(facts.value_stats.retained_size())
            .saturating_add(strings_retained(&facts.extra_files))
            .saturating_add(facts.embedded_index.as_ref().map_or(0, Vec::len))
            .saturating_add(optional_strings_retained(&facts.value_stats_cols))
            .saturating_add(facts.external_path.as_ref().map_or(0, String::len))
            .saturating_add(optional_strings_retained(&facts.write_cols))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaimonDeletionFile {
    path: Arc<str>,
    offset: u64,
    length: u64,
    cardinality: Option<u64>,
}

impl PaimonDeletionFile {
    pub fn try_new(
        path: impl AsRef<str>,
        offset: u64,
        length: u64,
        cardinality: Option<u64>,
    ) -> Result<Self, ConnectorError> {
        if length == 0 {
            return Err(invalid("Paimon deletion file length must be positive"));
        }
        if offset > i64::MAX as u64
            || length > i64::MAX as u64
            || cardinality.is_some_and(|value| value > i64::MAX as u64)
        {
            return Err(invalid(
                "Paimon deletion file facts exceed the SDK integer domain",
            ));
        }
        Ok(Self {
            path: bounded_location(path.as_ref())?,
            offset,
            length,
            cardinality,
        })
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub const fn offset(&self) -> u64 {
        self.offset
    }
    pub const fn length(&self) -> u64 {
        self.length
    }
    pub const fn cardinality(&self) -> Option<u64> {
        self.cardinality
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaimonRowRange {
    from: i64,
    to: i64,
}
impl PaimonRowRange {
    pub fn try_new(from: i64, to: i64) -> Result<Self, ConnectorError> {
        if from > to {
            return Err(invalid("Paimon row range start exceeds end"));
        }
        Ok(Self { from, to })
    }
    pub const fn from(&self) -> i64 {
        self.from
    }
    pub const fn to(&self) -> i64 {
        self.to
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaimonSplit {
    snapshot_id: i64,
    schema_id: i64,
    partition_arity: i32,
    partition: Arc<[u8]>,
    bucket: i32,
    bucket_path: Arc<str>,
    total_buckets: i32,
    files: Arc<[PaimonDataFile]>,
    data_deletion_files: Option<Arc<[Option<PaimonDeletionFile>]>>,
    row_ranges: Option<Arc<[PaimonRowRange]>>,
    raw_convertible: bool,
    contains_delete_rows: bool,
    weight: SplitWeight,
}

impl PaimonSplit {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        snapshot_id: i64,
        schema_id: i64,
        partition_arity: i32,
        partition: Vec<u8>,
        bucket: i32,
        bucket_path: impl AsRef<str>,
        total_buckets: i32,
        files: Vec<PaimonDataFile>,
        data_deletion_files: Option<Vec<Option<PaimonDeletionFile>>>,
        row_ranges: Option<Vec<PaimonRowRange>>,
        raw_convertible: bool,
        contains_delete_rows: bool,
        weight: SplitWeight,
    ) -> Result<Self, ConnectorError> {
        if snapshot_id == -1
            || schema_id < 0
            || partition_arity < 0
            || bucket < 0
            || total_buckets == 0
            || total_buckets < -1
            || (total_buckets > 0 && bucket >= total_buckets)
            || files.len() > MAX_PAIMON_FILES_PER_SPLIT
        {
            return Err(invalid("Paimon split facts are invalid or unbounded"));
        }
        let minimum_partition_bytes = (((i64::from(partition_arity) + 71) / 64) * 8)
            .saturating_add(i64::from(partition_arity).saturating_mul(8));
        if !(partition_arity == 0 && partition.is_empty())
            && i64::try_from(partition.len()).unwrap_or(i64::MAX) < minimum_partition_bytes
        {
            return Err(invalid("Paimon partition BinaryRow body is truncated"));
        }
        if data_deletion_files.is_some() {
            return Err(unsupported(
                "Paimon deletion-vector reads are unsupported in PAI-1",
            ));
        }
        if row_ranges.is_some() {
            return Err(unsupported(
                "Paimon row-range reads are unsupported in PAI-1",
            ));
        }
        if !contains_delete_rows
            && files
                .iter()
                .any(|file| file.facts.delete_row_count.is_some_and(|count| count > 0))
        {
            return Err(invalid("Paimon split contradicts its delete-row summary"));
        }
        let value = Self {
            snapshot_id,
            schema_id,
            partition_arity,
            partition: Arc::from(partition),
            bucket,
            bucket_path: bounded_location(bucket_path.as_ref())?,
            total_buckets,
            files: Arc::from(files),
            data_deletion_files: data_deletion_files.map(Arc::from),
            row_ranges: row_ranges.map(Arc::from),
            raw_convertible,
            contains_delete_rows,
            weight,
        };
        if value.retained_size_in_bytes() > MAX_PAIMON_SPLIT_RETAINED_BYTES {
            return Err(invalid(
                "Paimon split retained size exceeds its private carrier budget",
            ));
        }
        Ok(value)
    }
    pub const fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
    pub const fn schema_id(&self) -> i64 {
        self.schema_id
    }
    pub const fn partition_arity(&self) -> i32 {
        self.partition_arity
    }
    pub fn partition(&self) -> &[u8] {
        &self.partition
    }
    pub const fn bucket(&self) -> i32 {
        self.bucket
    }
    pub fn bucket_path(&self) -> &str {
        &self.bucket_path
    }
    pub const fn total_buckets(&self) -> i32 {
        self.total_buckets
    }
    pub fn files(&self) -> &[PaimonDataFile] {
        &self.files
    }
    pub fn data_deletion_files(&self) -> Option<&[Option<PaimonDeletionFile>]> {
        self.data_deletion_files.as_deref()
    }
    pub fn row_ranges(&self) -> Option<&[PaimonRowRange]> {
        self.row_ranges.as_deref()
    }
    pub const fn raw_convertible(&self) -> bool {
        self.raw_convertible
    }
    pub const fn contains_delete_rows(&self) -> bool {
        self.contains_delete_rows
    }
}

impl ConnectorSplit for PaimonSplit {
    fn split_weight(&self) -> SplitWeight {
        self.weight
    }
    fn retained_size_in_bytes(&self) -> u64 {
        let mut retained = size_of::<Self>()
            .saturating_add(self.partition.len())
            .saturating_add(self.bucket_path.len());
        retained = self.files.iter().fold(retained, |sum, file| {
            sum.saturating_add(file.retained_size())
        });
        if let Some(deletions) = &self.data_deletion_files {
            retained = retained.saturating_add(
                deletions
                    .len()
                    .saturating_mul(size_of::<Option<PaimonDeletionFile>>()),
            );
            retained = deletions
                .iter()
                .flatten()
                .fold(retained, |sum, file| sum.saturating_add(file.path.len()));
        }
        if let Some(ranges) = &self.row_ranges {
            retained =
                retained.saturating_add(ranges.len().saturating_mul(size_of::<PaimonRowRange>()));
        }
        u64::try_from(retained).unwrap_or(u64::MAX)
    }
}

pub struct PaimonReadTypes;

impl ProviderReadTypes for PaimonReadTypes {
    type Table = PaimonTable;
    type Column = PaimonColumn;
    type ReadView = PaimonReadView;
    type Split = PaimonSplit;
}

fn validate_strings(values: &[String], max_items: usize) -> Result<(), ConnectorError> {
    if values.len() > max_items
        || values
            .iter()
            .any(|value| value.is_empty() || value.len() > MAX_PAIMON_LOCATION_BYTES)
    {
        return Err(invalid("Paimon string vector is invalid or unbounded"));
    }
    Ok(())
}

fn validate_optional_strings(
    values: &Option<Vec<String>>,
    max_items: usize,
) -> Result<(), ConnectorError> {
    if let Some(values) = values {
        validate_strings(values, max_items)?;
    }
    Ok(())
}

fn strings_retained(values: &[String]) -> usize {
    values.iter().fold(
        values.len().saturating_mul(size_of::<String>()),
        |sum, value| sum.saturating_add(value.len()),
    )
}

fn optional_strings_retained(values: &Option<Vec<String>>) -> usize {
    values.as_ref().map_or(0, |values| strings_retained(values))
}

fn bounded_location(value: &str) -> Result<Arc<str>, ConnectorError> {
    if value.is_empty() || value.len() > MAX_PAIMON_LOCATION_BYTES {
        return Err(invalid("Paimon location must be non-empty and bounded"));
    }
    Ok(Arc::from(value))
}

fn validate_field_ids(ids: &[i32]) -> Result<(), ConnectorError> {
    if ids.len() > MAX_PAIMON_COLUMNS
        || ids.iter().any(|id| *id < 0)
        || ids.iter().copied().collect::<BTreeSet<_>>().len() != ids.len()
    {
        return Err(invalid(
            "Paimon field IDs must be unique, non-negative and bounded",
        ));
    }
    Ok(())
}

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn unsupported(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}
