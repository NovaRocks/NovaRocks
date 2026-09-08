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

//! Iceberg system relations, their frozen worker reference, and the `$files`
//! split source.
//!
//! A system relation is read from one immutable metadata file, not from the
//! catalog. The frontend freezes the exact metadata file location, the table
//! UUID, and -- when the relation is snapshot-scoped -- the selected snapshot
//! ID; the selected backend loads that file through its local connector
//! binding and verifies both identities before it emits a single row. It never
//! re-resolves the table, never falls back to the current snapshot, and never
//! accepts frontend-serialized rows: either of those would make the answer
//! depend on when the read happened rather than on what was planned.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::wire::dto;
use novarocks_proto_codec::connector_read::{
    MAX_JSON_BYTES, MAX_PATH_BYTES, MAX_SPLITS_PER_ASSIGNMENT,
};
use novarocks_spi::connector::read_stack::{
    ConnectorSplit, ConnectorSplitBatch, ConnectorSplitSource, DynamicFilterSnapshot, HostAddress,
    SchemaTableName, SplitWeight, SystemTableDistribution,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::iceberg::spec::{
    ManifestContentType, ManifestFile, PartitionSpec, Schema, TableMetadata,
};

use super::column_handle::{IcebergColumnHandle, corrupt, invalid, unsupported};
use super::table_handle::MAX_PARTITION_SPECS;

/// Maximum number of manifests one `$files` relation may enumerate.
///
/// A pinned snapshot's manifest list is already bounded by the writer, but the
/// reference arrives over a wire: a bound here keeps a malformed plan from
/// materializing an unbounded split list on a backend.
pub const MAX_FILES_TABLE_MANIFESTS: usize = 1_000_000;

/// The worker-visible Iceberg system relations.
///
/// The set is deliberately closed and deliberately small. `ALL_ENTRIES` and
/// `ALL_MANIFESTS` are absent because they aggregate across snapshots, which
/// contradicts a single pinned metadata reference; `PARTITIONS` is absent
/// because it is a view over `FILES` rather than a relation of its own (see
/// its own reference kind. There is no unknown variant: a relation this
/// stack cannot name is a planning error, not a value to carry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergSystemTableType {
    Files,
    Entries,
    Snapshots,
    History,
    Refs,
    Manifests,
    /// The aggregation over the rows `Files` produces for the same pinned
    /// snapshot. It is a relation of its own rather than a `Files` reference a
    /// worker is told to aggregate: a `Files` reference reaching a backend
    /// reads as the un-aggregated relation, one row per data file instead of
    /// one row per partition.
    Partitions,
}

/// How one system relation actually runs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergSystemTableExecution {
    /// Enumerated by [`FilesTableSplitSource`] and spread over every admitted
    /// backend.
    DistributedSplits,
    /// Run once, on exactly one selected backend, as a direct page source.
    ///
    /// Trino calls this `SINGLE_COORDINATOR`; Native selects one backend
    /// instead of running the relation on the coordinator. Either way there is
    /// no split: a synthetic one-row split would only invent a scheduling
    /// object with no byte range and no work to divide.
    SingleBackendDirectPageSource,
}

impl IcebergSystemTableType {
    /// The SQL suffix that names this relation.
    pub const fn suffix(self) -> &'static str {
        match self {
            Self::Files => "$files",
            Self::Entries => "$entries",
            Self::Snapshots => "$snapshots",
            Self::History => "$history",
            Self::Refs => "$refs",
            Self::Manifests => "$manifests",
            Self::Partitions => "$partitions",
        }
    }

    /// Resolve a SQL relation suffix to a worker relation.
    ///
    /// `$entries` resolves straight to [`Self::Entries`]: the old
    /// `LogicalIcebergMetadata` alias is gone, and reintroducing an alias would
    /// give one relation two names on the wire.
    pub fn try_from_suffix(suffix: &str) -> Result<Self, ConnectorError> {
        match suffix {
            "$files" => Ok(Self::Files),
            "$entries" => Ok(Self::Entries),
            "$snapshots" => Ok(Self::Snapshots),
            "$history" => Ok(Self::History),
            "$refs" => Ok(Self::Refs),
            "$manifests" => Ok(Self::Manifests),
            // `$partitions` exists, but only as a frontend-local view over this
            // same pinned snapshot's `$files`. Naming it here would create a
            // second aggregation owner on the worker side.
            "$partitions" => Err(unsupported(
                "iceberg $partitions is a frontend-local view over $files, not a worker relation",
            )),
            // Relations Iceberg defines and this stack does not implement. They
            // fail as unsupported rather than as unknown so the message stays
            // stable if one is added later.
            "$all_entries"
            | "$all_manifests"
            | "$all_data_files"
            | "$all_delete_files"
            | "$all_files"
            | "$position_deletes"
            | "$metadata_log_entries" => Err(unsupported(format!(
                "iceberg system relation {suffix} is not supported"
            ))),
            other => Err(invalid(format!("unknown iceberg system relation {other}"))),
        }
    }

    pub const fn execution(self) -> IcebergSystemTableExecution {
        match self {
            // One manifest is one independent unit of parseable bytes, so the
            // relation divides cleanly across backends.
            Self::Files => IcebergSystemTableExecution::DistributedSplits,
            // These five read only the metadata file and its manifest list.
            // There is nothing to divide, and dividing it would mean reading
            // the same metadata file on every backend.
            // `$partitions` joins them: it walks every manifest of the
            // snapshot, but it must see all of them at once to aggregate, so
            // there is nothing to divide either.
            Self::Entries
            | Self::Snapshots
            | Self::History
            | Self::Refs
            | Self::Manifests
            | Self::Partitions => IcebergSystemTableExecution::SingleBackendDirectPageSource,
        }
    }

    pub const fn distribution(self) -> SystemTableDistribution {
        match self.execution() {
            IcebergSystemTableExecution::DistributedSplits => SystemTableDistribution::AllNodes,
            IcebergSystemTableExecution::SingleBackendDirectPageSource => {
                SystemTableDistribution::SingleCoordinator
            }
        }
    }

    /// Whether this relation is scheduled through a split source at all.
    pub const fn produces_splits(self) -> bool {
        match self.execution() {
            IcebergSystemTableExecution::DistributedSplits => true,
            IcebergSystemTableExecution::SingleBackendDirectPageSource => false,
        }
    }

    fn to_proto(self) -> dto::IcebergSystemTableType {
        match self {
            Self::Files => dto::IcebergSystemTableType::Files,
            Self::Entries => dto::IcebergSystemTableType::Entries,
            Self::Snapshots => dto::IcebergSystemTableType::Snapshots,
            Self::History => dto::IcebergSystemTableType::History,
            Self::Refs => dto::IcebergSystemTableType::Refs,
            Self::Manifests => dto::IcebergSystemTableType::Manifests,
            Self::Partitions => dto::IcebergSystemTableType::Partitions,
        }
    }

    fn from_proto(raw: i32) -> Result<Self, ConnectorError> {
        let value = dto::IcebergSystemTableType::try_from(raw)
            .map_err(|_| invalid("unknown iceberg system table type"))?;
        match value {
            dto::IcebergSystemTableType::Unspecified => {
                Err(invalid("iceberg system table type must be specified"))
            }
            dto::IcebergSystemTableType::Files => Ok(Self::Files),
            dto::IcebergSystemTableType::Entries => Ok(Self::Entries),
            dto::IcebergSystemTableType::Snapshots => Ok(Self::Snapshots),
            dto::IcebergSystemTableType::History => Ok(Self::History),
            dto::IcebergSystemTableType::Refs => Ok(Self::Refs),
            dto::IcebergSystemTableType::Manifests => Ok(Self::Manifests),
            dto::IcebergSystemTableType::Partitions => Ok(Self::Partitions),
        }
    }
}

/// The exact facts one system-relation reference is frozen from.
#[derive(Clone, Debug)]
pub struct IcebergSystemTableReferenceParams {
    pub schema_table_name: SchemaTableName,
    pub system_table_type: IcebergSystemTableType,
    /// The exact immutable metadata file the backend must load.
    pub metadata_file_location: String,
    /// The table UUID the loaded metadata must declare.
    pub table_uuid: String,
    /// The selected snapshot, for relations that are snapshot-scoped.
    pub snapshot_id: Option<i64>,
}

/// One frozen reference to one immutable Iceberg metadata file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergSystemTableReference {
    schema_table_name: SchemaTableName,
    system_table_type: IcebergSystemTableType,
    metadata_file_location: Arc<str>,
    /// Stored in the canonical lowercase hyphenated form so that comparing it
    /// against loaded metadata is a string equality and not a shape question.
    table_uuid: Arc<str>,
    snapshot_id: Option<i64>,
}

impl IcebergSystemTableReference {
    pub fn try_new(params: IcebergSystemTableReferenceParams) -> Result<Self, ConnectorError> {
        let IcebergSystemTableReferenceParams {
            schema_table_name,
            system_table_type,
            metadata_file_location,
            table_uuid,
            snapshot_id,
        } = params;

        if metadata_file_location.is_empty() || metadata_file_location.len() > MAX_PATH_BYTES {
            return Err(invalid(
                "iceberg metadata file location must be non-empty and bounded",
            ));
        }
        if metadata_file_location.ends_with('/') {
            return Err(invalid(
                "iceberg metadata file location must name a file, not a directory",
            ));
        }
        // Parsing here is the fail-fast point: a malformed UUID could never
        // match loaded metadata, and finding that out on a backend would waste
        // a scheduled task to learn a fact the frontend already had.
        let table_uuid = uuid::Uuid::parse_str(&table_uuid)
            .map_err(|error| invalid(format!("iceberg table uuid is invalid: {error}")))?;
        if snapshot_id.is_none()
            && matches!(
                system_table_type,
                IcebergSystemTableType::Files | IcebergSystemTableType::Partitions
            )
        {
            // `$files` reports the files of one snapshot, and `$partitions`
            // aggregates those same files. Without a selected snapshot there is
            // no manifest list to walk, and picking the current one on the
            // backend is exactly the fallback this reference exists to forbid.
            return Err(invalid(format!(
                "iceberg {} requires a selected snapshot id",
                system_table_type.suffix()
            )));
        }

        Ok(Self {
            schema_table_name,
            system_table_type,
            metadata_file_location: Arc::from(metadata_file_location.as_str()),
            table_uuid: Arc::from(table_uuid.hyphenated().to_string().as_str()),
            snapshot_id,
        })
    }

    pub const fn schema_table_name(&self) -> &SchemaTableName {
        &self.schema_table_name
    }

    pub const fn system_table_type(&self) -> IcebergSystemTableType {
        self.system_table_type
    }

    pub fn metadata_file_location(&self) -> &str {
        &self.metadata_file_location
    }

    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }

    pub const fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    /// Confirm that metadata loaded from [`Self::metadata_file_location`] is
    /// the metadata this reference was frozen against.
    ///
    /// A mismatch is corrupt data, not a retryable condition: the location is
    /// immutable, so a different UUID means the location was reused by another
    /// table, and a missing snapshot means the file is not the one planning
    /// read. Falling back to the current snapshot in either case would answer
    /// a different question than the one that was planned.
    pub fn verify_loaded_metadata(&self, metadata: &TableMetadata) -> Result<(), ConnectorError> {
        let loaded_uuid = metadata.uuid().hyphenated().to_string();
        if loaded_uuid != *self.table_uuid {
            return Err(corrupt(format!(
                "iceberg metadata file {} declares table uuid {loaded_uuid}, not the frozen {}",
                self.metadata_file_location, self.table_uuid
            )));
        }
        if let Some(snapshot_id) = self.snapshot_id
            && metadata.snapshot_by_id(snapshot_id).is_none()
        {
            return Err(corrupt(format!(
                "iceberg metadata file {} does not contain the frozen snapshot {snapshot_id}",
                self.metadata_file_location
            )));
        }
        Ok(())
    }

    pub fn to_proto(&self) -> dto::IcebergSystemTableReference {
        dto::IcebergSystemTableReference {
            schema_table_name: Some(dto::SchemaTableName {
                schema_name: self.schema_table_name.schema_name().to_string(),
                table_name: self.schema_table_name.table_name().to_string(),
            }),
            system_table_type: self.system_table_type.to_proto() as i32,
            metadata_file_location: self.metadata_file_location.to_string(),
            table_uuid: self.table_uuid.to_string(),
            snapshot_id: self.snapshot_id,
        }
    }

    pub fn from_proto(raw: &dto::IcebergSystemTableReference) -> Result<Self, ConnectorError> {
        let schema_table_name = raw.schema_table_name.as_ref().ok_or_else(|| {
            invalid("iceberg system table reference requires a schema table name")
        })?;
        Self::try_new(IcebergSystemTableReferenceParams {
            schema_table_name: SchemaTableName::try_new(
                &schema_table_name.schema_name,
                &schema_table_name.table_name,
            )?,
            system_table_type: IcebergSystemTableType::from_proto(raw.system_table_type)?,
            metadata_file_location: raw.metadata_file_location.clone(),
            table_uuid: raw.table_uuid.clone(),
            snapshot_id: raw.snapshot_id,
        })
    }
}
/// What one manifest tracks.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TrinoManifestContent {
    Data,
    Deletes,
}

impl TrinoManifestContent {
    pub const fn from_manifest_content_type(content: ManifestContentType) -> Self {
        match content {
            ManifestContentType::Data => Self::Data,
            ManifestContentType::Deletes => Self::Deletes,
        }
    }

    /// The Iceberg spec's numeric code, which is what `$files.content` reports.
    pub const fn code(self) -> i32 {
        match self {
            Self::Data => 0,
            Self::Deletes => 1,
        }
    }

    fn from_code(raw: i32) -> Result<Self, ConnectorError> {
        match raw {
            0 => Ok(Self::Data),
            1 => Ok(Self::Deletes),
            other => Err(invalid(format!(
                "unknown iceberg manifest content code {other}"
            ))),
        }
    }
}

/// The exact facts one manifest descriptor carries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoManifestFileParams {
    pub path: String,
    pub length: i64,
    pub partition_spec_id: i32,
    pub content: TrinoManifestContent,
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    pub added_files_count: Option<i32>,
    pub existing_files_count: Option<i32>,
    pub deleted_files_count: Option<i32>,
    pub added_rows_count: Option<i64>,
    pub existing_rows_count: Option<i64>,
    pub deleted_rows_count: Option<i64>,
    pub first_row_id: Option<i64>,
    /// Encryption material. This stack implements none, so a non-empty value
    /// is rejected by both producer and consumer.
    pub key_metadata: Vec<u8>,
}

/// One manifest of a pinned snapshot's manifest list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoManifestFile {
    path: Arc<str>,
    length: i64,
    partition_spec_id: i32,
    content: TrinoManifestContent,
    sequence_number: i64,
    min_sequence_number: i64,
    added_snapshot_id: i64,
    added_files_count: Option<i32>,
    existing_files_count: Option<i32>,
    deleted_files_count: Option<i32>,
    added_rows_count: Option<i64>,
    existing_rows_count: Option<i64>,
    deleted_rows_count: Option<i64>,
    first_row_id: Option<i64>,
}

impl TrinoManifestFile {
    pub fn try_new(params: TrinoManifestFileParams) -> Result<Self, ConnectorError> {
        let TrinoManifestFileParams {
            path,
            length,
            partition_spec_id,
            content,
            sequence_number,
            min_sequence_number,
            added_snapshot_id,
            added_files_count,
            existing_files_count,
            deleted_files_count,
            added_rows_count,
            existing_rows_count,
            deleted_rows_count,
            first_row_id,
            key_metadata,
        } = params;

        if path.is_empty() || path.len() > MAX_PATH_BYTES {
            return Err(invalid(
                "iceberg manifest path must be non-empty and bounded",
            ));
        }
        if length < 0 {
            return Err(invalid("iceberg manifest length must be nonnegative"));
        }
        if sequence_number < 0 || min_sequence_number < 0 {
            return Err(invalid(
                "iceberg manifest sequence numbers must be nonnegative",
            ));
        }
        for count in [added_files_count, existing_files_count, deleted_files_count] {
            if count.is_some_and(|value| value < 0) {
                return Err(invalid("iceberg manifest file counts must be nonnegative"));
            }
        }
        for count in [added_rows_count, existing_rows_count, deleted_rows_count] {
            if count.is_some_and(|value| value < 0) {
                return Err(invalid("iceberg manifest row counts must be nonnegative"));
            }
        }
        if first_row_id.is_some_and(|value| value < 0) {
            return Err(invalid("iceberg manifest first row id must be nonnegative"));
        }
        if !key_metadata.is_empty() {
            return Err(unsupported(
                "iceberg encrypted manifest key metadata is not supported by the connector read stack",
            ));
        }

        Ok(Self {
            path: Arc::from(path.as_str()),
            length,
            partition_spec_id,
            content,
            sequence_number,
            min_sequence_number,
            added_snapshot_id,
            added_files_count,
            existing_files_count,
            deleted_files_count,
            added_rows_count,
            existing_rows_count,
            deleted_rows_count,
            first_row_id,
        })
    }

    /// Freeze one entry of a loaded manifest list.
    pub fn from_manifest_file(manifest: &ManifestFile) -> Result<Self, ConnectorError> {
        let count =
            |value: Option<u32>, what: &'static str| -> Result<Option<i32>, ConnectorError> {
                value
                    .map(|value| {
                        i32::try_from(value).map_err(|_| {
                            ConnectorError::new(
                                ConnectorErrorKind::ResourceExhausted,
                                format!("iceberg manifest {what} exceeds Int32"),
                            )
                        })
                    })
                    .transpose()
            };
        let rows =
            |value: Option<u64>, what: &'static str| -> Result<Option<i64>, ConnectorError> {
                value
                    .map(|value| {
                        i64::try_from(value).map_err(|_| {
                            ConnectorError::new(
                                ConnectorErrorKind::ResourceExhausted,
                                format!("iceberg manifest {what} exceeds Int64"),
                            )
                        })
                    })
                    .transpose()
            };

        Self::try_new(TrinoManifestFileParams {
            path: manifest.manifest_path.clone(),
            length: manifest.manifest_length,
            partition_spec_id: manifest.partition_spec_id,
            content: TrinoManifestContent::from_manifest_content_type(manifest.content),
            sequence_number: manifest.sequence_number,
            min_sequence_number: manifest.min_sequence_number,
            added_snapshot_id: manifest.added_snapshot_id,
            added_files_count: count(manifest.added_files_count, "added files count")?,
            existing_files_count: count(manifest.existing_files_count, "existing files count")?,
            deleted_files_count: count(manifest.deleted_files_count, "deleted files count")?,
            added_rows_count: rows(manifest.added_rows_count, "added rows count")?,
            existing_rows_count: rows(manifest.existing_rows_count, "existing rows count")?,
            deleted_rows_count: rows(manifest.deleted_rows_count, "deleted rows count")?,
            first_row_id: rows(manifest.first_row_id, "first row id")?,
            key_metadata: manifest.key_metadata.clone().unwrap_or_default(),
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub const fn length(&self) -> i64 {
        self.length
    }

    pub const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub const fn content(&self) -> TrinoManifestContent {
        self.content
    }

    pub const fn sequence_number(&self) -> i64 {
        self.sequence_number
    }

    pub const fn min_sequence_number(&self) -> i64 {
        self.min_sequence_number
    }

    pub const fn added_snapshot_id(&self) -> i64 {
        self.added_snapshot_id
    }

    pub const fn added_files_count(&self) -> Option<i32> {
        self.added_files_count
    }

    pub const fn existing_files_count(&self) -> Option<i32> {
        self.existing_files_count
    }

    pub const fn deleted_files_count(&self) -> Option<i32> {
        self.deleted_files_count
    }

    pub const fn added_rows_count(&self) -> Option<i64> {
        self.added_rows_count
    }

    pub const fn existing_rows_count(&self) -> Option<i64> {
        self.existing_rows_count
    }

    pub const fn deleted_rows_count(&self) -> Option<i64> {
        self.deleted_rows_count
    }

    pub const fn first_row_id(&self) -> Option<i64> {
        self.first_row_id
    }

    fn retained_size_in_bytes(&self) -> usize {
        size_of::<Self>() + self.path.len()
    }

    pub fn to_proto(&self) -> dto::TrinoManifestFile {
        dto::TrinoManifestFile {
            path: self.path.to_string(),
            length: self.length,
            partition_spec_id: self.partition_spec_id,
            content: self.content.code(),
            sequence_number: self.sequence_number,
            min_sequence_number: self.min_sequence_number,
            added_snapshot_id: self.added_snapshot_id,
            added_files_count: self.added_files_count,
            existing_files_count: self.existing_files_count,
            deleted_files_count: self.deleted_files_count,
            added_rows_count: self.added_rows_count,
            existing_rows_count: self.existing_rows_count,
            deleted_rows_count: self.deleted_rows_count,
            first_row_id: self.first_row_id,
            // An encrypted manifest never reaches this point, so the wire field
            // is always empty rather than carrying redacted material.
            key_metadata: Vec::new(),
        }
    }

    pub fn from_proto(raw: &dto::TrinoManifestFile) -> Result<Self, ConnectorError> {
        Self::try_new(TrinoManifestFileParams {
            path: raw.path.clone(),
            length: raw.length,
            partition_spec_id: raw.partition_spec_id,
            content: TrinoManifestContent::from_code(raw.content)?,
            sequence_number: raw.sequence_number,
            min_sequence_number: raw.min_sequence_number,
            added_snapshot_id: raw.added_snapshot_id,
            added_files_count: raw.added_files_count,
            existing_files_count: raw.existing_files_count,
            deleted_files_count: raw.deleted_files_count,
            added_rows_count: raw.added_rows_count,
            existing_rows_count: raw.existing_rows_count,
            deleted_rows_count: raw.deleted_rows_count,
            first_row_id: raw.first_row_id,
            key_metadata: raw.key_metadata.clone(),
        })
    }
}

/// The exact facts one `$files` split carries.
#[derive(Clone, Debug)]
pub struct FilesTableSplitParams {
    pub manifest: TrinoManifestFile,
    pub table_schema_json: String,
    /// The frozen `$files` output schema. It is not an Iceberg schema, so it is
    /// carried as opaque-to-Iceberg but well-formed JSON.
    pub metadata_table_schema_json: String,
    /// Every partition spec the manifests of this snapshot reference.
    pub partition_spec_jsons: BTreeMap<i32, String>,
    /// The schema-derived partition ROW type, absent for an unpartitioned
    /// table.
    pub partition_column_type_json: Option<String>,
    /// The schema-derived lower/upper bounds ROW type, absent when the frozen
    /// projection needs no bounds.
    pub bounds_column_type_json: Option<String>,
    /// Typed extension point only. This stack implements no manifest
    /// encryption, so a present value is rejected by producer and consumer.
    pub encryption_key_id: Option<String>,
}

/// One manifest of the pinned snapshot's `$files` relation.
///
/// One manifest produces exactly one split. A manifest is a single Avro file
/// read start to finish; cutting it into byte ranges would make every range
/// re-read the header, and merging manifests would make one split's failure
/// retry another manifest's work.
#[derive(Clone, Debug)]
pub struct FilesTableSplit {
    manifest: TrinoManifestFile,
    table_schema_json: Arc<str>,
    metadata_table_schema_json: Arc<str>,
    partition_spec_jsons: Arc<BTreeMap<i32, String>>,
    partition_column_type_json: Option<Arc<str>>,
    bounds_column_type_json: Option<Arc<str>>,
    retained_size_in_bytes: u64,
}

impl FilesTableSplit {
    pub fn try_new(params: FilesTableSplitParams) -> Result<Self, ConnectorError> {
        let FilesTableSplitParams {
            manifest,
            table_schema_json,
            metadata_table_schema_json,
            partition_spec_jsons,
            partition_column_type_json,
            bounds_column_type_json,
            encryption_key_id,
        } = params;

        validate_shared_files_table_facts(
            &table_schema_json,
            &metadata_table_schema_json,
            &partition_spec_jsons,
            partition_column_type_json.as_deref(),
            bounds_column_type_json.as_deref(),
            encryption_key_id.as_deref(),
        )?;
        // The manifest names the spec that decodes its partition values; a
        // split that cannot decode them would produce a wrong `$files.partition`
        // column rather than fail.
        if !partition_spec_jsons.contains_key(&manifest.partition_spec_id()) {
            return Err(invalid(format!(
                "iceberg $files split has no partition spec json for spec id {}",
                manifest.partition_spec_id()
            )));
        }

        let mut split = Self {
            manifest,
            table_schema_json: Arc::from(table_schema_json.as_str()),
            metadata_table_schema_json: Arc::from(metadata_table_schema_json.as_str()),
            partition_spec_jsons: Arc::new(partition_spec_jsons),
            partition_column_type_json: partition_column_type_json
                .map(|json| Arc::from(json.as_str())),
            bounds_column_type_json: bounds_column_type_json.map(|json| Arc::from(json.as_str())),
            retained_size_in_bytes: 0,
        };
        split.retained_size_in_bytes = split.compute_retained_size_in_bytes();
        Ok(split)
    }

    /// A sibling split of the same relation that differs only in its manifest.
    ///
    /// Every split of one `$files` scan shares the same frozen schemas; sharing
    /// them by reference keeps a thousand-manifest snapshot from copying the
    /// table schema a thousand times, and re-validating them per manifest would
    /// only re-prove the same fact.
    pub fn with_manifest(&self, manifest: TrinoManifestFile) -> Result<Self, ConnectorError> {
        if !self
            .partition_spec_jsons
            .contains_key(&manifest.partition_spec_id())
        {
            return Err(invalid(format!(
                "iceberg $files split has no partition spec json for spec id {}",
                manifest.partition_spec_id()
            )));
        }
        let mut split = Self {
            manifest,
            table_schema_json: Arc::clone(&self.table_schema_json),
            metadata_table_schema_json: Arc::clone(&self.metadata_table_schema_json),
            partition_spec_jsons: Arc::clone(&self.partition_spec_jsons),
            partition_column_type_json: self.partition_column_type_json.clone(),
            bounds_column_type_json: self.bounds_column_type_json.clone(),
            retained_size_in_bytes: 0,
        };
        split.retained_size_in_bytes = split.compute_retained_size_in_bytes();
        Ok(split)
    }

    pub const fn manifest(&self) -> &TrinoManifestFile {
        &self.manifest
    }

    pub fn table_schema_json(&self) -> &str {
        &self.table_schema_json
    }

    pub fn metadata_table_schema_json(&self) -> &str {
        &self.metadata_table_schema_json
    }

    pub fn partition_spec_jsons(&self) -> &BTreeMap<i32, String> {
        &self.partition_spec_jsons
    }

    pub fn partition_column_type_json(&self) -> Option<&str> {
        self.partition_column_type_json.as_deref()
    }

    pub fn bounds_column_type_json(&self) -> Option<&str> {
        self.bounds_column_type_json.as_deref()
    }

    /// Always absent: manifest encryption is rejected at construction.
    pub const fn encryption_key_id(&self) -> Option<&str> {
        None
    }

    pub fn parse_partition_spec(&self, spec_id: i32) -> Result<PartitionSpec, ConnectorError> {
        let spec_json = self.partition_spec_jsons.get(&spec_id).ok_or_else(|| {
            invalid(format!(
                "iceberg partition spec id {spec_id} is not carried by this $files split"
            ))
        })?;
        parse_partition_spec(spec_json)
    }

    fn compute_retained_size_in_bytes(&self) -> u64 {
        let mut retained = size_of::<Self>()
            + self.manifest.retained_size_in_bytes()
            + self.table_schema_json.len()
            + self.metadata_table_schema_json.len()
            + self
                .partition_column_type_json
                .as_ref()
                .map_or(0, |json| json.len())
            + self
                .bounds_column_type_json
                .as_ref()
                .map_or(0, |json| json.len());
        for spec_json in self.partition_spec_jsons.values() {
            retained += size_of::<i32>() + spec_json.len();
        }
        retained as u64
    }

    pub fn to_proto(&self) -> dto::FilesTableSplit {
        dto::FilesTableSplit {
            manifest: Some(self.manifest.to_proto()),
            table_schema_json: self.table_schema_json.to_string(),
            metadata_table_schema_json: self.metadata_table_schema_json.to_string(),
            partition_spec_jsons: (*self.partition_spec_jsons).clone(),
            partition_column_type_json: self
                .partition_column_type_json
                .as_ref()
                .map(|json| json.to_string()),
            bounds_column_type_json: self
                .bounds_column_type_json
                .as_ref()
                .map(|json| json.to_string()),
            encryption_key_id: None,
        }
    }

    pub fn from_proto(raw: &dto::FilesTableSplit) -> Result<Self, ConnectorError> {
        let manifest = raw
            .manifest
            .as_ref()
            .ok_or_else(|| invalid("iceberg $files split requires a manifest"))?;
        Self::try_new(FilesTableSplitParams {
            manifest: TrinoManifestFile::from_proto(manifest)?,
            table_schema_json: raw.table_schema_json.clone(),
            metadata_table_schema_json: raw.metadata_table_schema_json.clone(),
            partition_spec_jsons: raw.partition_spec_jsons.clone(),
            partition_column_type_json: raw.partition_column_type_json.clone(),
            bounds_column_type_json: raw.bounds_column_type_json.clone(),
            encryption_key_id: raw.encryption_key_id.clone(),
        })
    }
}

impl ConnectorSplit for FilesTableSplit {
    fn is_remotely_accessible(&self) -> bool {
        true
    }

    fn addresses(&self) -> &[HostAddress] {
        &[]
    }

    fn affinity_key(&self) -> Option<&str> {
        None
    }

    fn split_weight(&self) -> SplitWeight {
        // One manifest is one unit of work: manifests are written to a similar
        // size, and their row counts are not known until they are read.
        SplitWeight::STANDARD
    }

    fn retained_size_in_bytes(&self) -> u64 {
        self.retained_size_in_bytes
    }
}

/// The exact facts one `$files` split source is built from.
#[derive(Clone, Debug)]
pub struct FilesTableSplitSourceParams {
    pub reference: IcebergSystemTableReference,
    /// Every manifest of the pinned snapshot's manifest list, in list order.
    pub manifests: Vec<TrinoManifestFile>,
    pub table_schema_json: String,
    pub metadata_table_schema_json: String,
    pub partition_spec_jsons: BTreeMap<i32, String>,
    pub partition_column_type_json: Option<String>,
    pub bounds_column_type_json: Option<String>,
    pub encryption_key_id: Option<String>,
}

/// A lazily advancing enumerator over one pinned snapshot's manifests.
#[derive(Debug)]
pub struct FilesTableSplitSource {
    /// The fully validated first split. Every later split is this one with
    /// another manifest. `None` only when the pinned snapshot lists no
    /// manifests at all, which is a legal empty relation.
    template: Option<FilesTableSplit>,
    template_emitted: bool,
    manifests: VecDeque<TrinoManifestFile>,
    closed: bool,
    exhausted: bool,
}

impl FilesTableSplitSource {
    pub fn try_new(params: FilesTableSplitSourceParams) -> Result<Self, ConnectorError> {
        let FilesTableSplitSourceParams {
            reference,
            manifests,
            table_schema_json,
            metadata_table_schema_json,
            partition_spec_jsons,
            partition_column_type_json,
            bounds_column_type_json,
            encryption_key_id,
        } = params;

        if reference.system_table_type() != IcebergSystemTableType::Files {
            return Err(invalid(format!(
                "iceberg {} runs as a direct page source and has no split source",
                reference.system_table_type().suffix()
            )));
        }
        if manifests.len() > MAX_FILES_TABLE_MANIFESTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg $files manifest count exceeds the hard limit",
            ));
        }

        let mut manifests = VecDeque::from(manifests);
        let template = match manifests.pop_front() {
            Some(manifest) => Some(FilesTableSplit::try_new(FilesTableSplitParams {
                manifest,
                table_schema_json,
                metadata_table_schema_json,
                partition_spec_jsons,
                partition_column_type_json,
                bounds_column_type_json,
                encryption_key_id,
            })?),
            None => {
                // Validate the shared facts even with nothing to enumerate: a
                // malformed frozen schema is a planning bug, and letting an
                // empty snapshot hide it would only surface it on the next
                // commit, under a different query.
                validate_shared_files_table_facts(
                    &table_schema_json,
                    &metadata_table_schema_json,
                    &partition_spec_jsons,
                    partition_column_type_json.as_deref(),
                    bounds_column_type_json.as_deref(),
                    encryption_key_id.as_deref(),
                )?;
                None
            }
        };
        let exhausted = template.is_none();
        Ok(Self {
            template,
            template_emitted: false,
            manifests,
            closed: false,
            exhausted,
        })
    }
}

impl ConnectorSplitSource for FilesTableSplitSource {
    type Split = FilesTableSplit;
    type Column = IcebergColumnHandle;

    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &DynamicFilterSnapshot<Self::Column>,
    ) -> Result<ConnectorSplitBatch<Self::Split>, ConnectorError> {
        if max_size == 0 {
            return Err(invalid("connector split batch size must be positive"));
        }
        if self.closed || self.exhausted {
            return Ok(ConnectorSplitBatch::finished());
        }
        // A `$files` scan has no Iceberg column statistics to prune against.
        // The one fact worth acting on needs none: an unsatisfiable predicate
        // provably selects no rows.
        if dynamic_filter.current_predicate().is_none() {
            self.exhausted = true;
            return Ok(ConnectorSplitBatch::finished());
        }

        let Some(template) = self.template.as_ref() else {
            self.exhausted = true;
            return Ok(ConnectorSplitBatch::finished());
        };
        let max_size = max_size.min(MAX_SPLITS_PER_ASSIGNMENT);
        let mut produced = Vec::with_capacity(max_size);
        if !self.template_emitted {
            produced.push(template.clone());
            self.template_emitted = true;
        }
        while produced.len() < max_size {
            let Some(manifest) = self.manifests.pop_front() else {
                break;
            };
            produced.push(template.with_manifest(manifest)?);
        }

        let no_more_splits = self.manifests.is_empty();
        Ok(ConnectorSplitBatch::new(produced, no_more_splits))
    }

    fn is_finished(&self) -> bool {
        self.closed || self.exhausted || (self.template_emitted && self.manifests.is_empty())
    }

    /// Idempotent. A batch already returned by value cannot be retracted.
    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.manifests.clear();
        Ok(())
    }
}

/// Validate the facts every split of one `$files` scan shares.
fn validate_shared_files_table_facts(
    table_schema_json: &str,
    metadata_table_schema_json: &str,
    partition_spec_jsons: &BTreeMap<i32, String>,
    partition_column_type_json: Option<&str>,
    bounds_column_type_json: Option<&str>,
    encryption_key_id: Option<&str>,
) -> Result<(), ConnectorError> {
    if encryption_key_id.is_some() {
        return Err(unsupported(
            "iceberg manifest encryption is not supported by the connector read stack",
        ));
    }
    if table_schema_json.is_empty() || table_schema_json.len() > MAX_JSON_BYTES {
        return Err(invalid(
            "iceberg table schema json must be non-empty and bounded",
        ));
    }
    serde_json::from_str::<Schema>(table_schema_json)
        .map_err(|error| invalid(format!("iceberg table schema json is invalid: {error}")))?;
    parse_json_value("iceberg metadata table schema", metadata_table_schema_json)?;
    if partition_spec_jsons.len() > MAX_PARTITION_SPECS {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "iceberg partition spec count exceeds the hard limit",
        ));
    }
    for (declared_spec_id, spec_json) in partition_spec_jsons {
        let spec = parse_partition_spec(spec_json)?;
        if spec.spec_id() != *declared_spec_id {
            return Err(invalid(format!(
                "iceberg partition spec json declares spec id {} under key {declared_spec_id}",
                spec.spec_id()
            )));
        }
    }
    if let Some(json) = partition_column_type_json {
        parse_json_value("iceberg partition column type", json)?;
    }
    if let Some(json) = bounds_column_type_json {
        parse_json_value("iceberg bounds column type", json)?;
    }
    Ok(())
}

fn parse_json_value(what: &'static str, json: &str) -> Result<(), ConnectorError> {
    if json.is_empty() || json.len() > MAX_JSON_BYTES {
        return Err(invalid(format!(
            "{what} json must be non-empty and bounded"
        )));
    }
    serde_json::from_str::<serde_json::Value>(json)
        .map_err(|error| invalid(format!("{what} json is invalid: {error}")))?;
    Ok(())
}

fn parse_partition_spec(spec_json: &str) -> Result<PartitionSpec, ConnectorError> {
    if spec_json.is_empty() || spec_json.len() > MAX_JSON_BYTES {
        return Err(invalid(
            "iceberg partition spec json must be non-empty and bounded",
        ));
    }
    serde_json::from_str::<PartitionSpec>(spec_json)
        .map_err(|error| invalid(format!("iceberg partition spec json is invalid: {error}")))
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::read_stack::TupleDomain;

    use super::super::table_handle::tests::{identity_partition_spec, partitioned_schema};
    use super::*;

    fn reference(system_table_type: IcebergSystemTableType) -> IcebergSystemTableReference {
        IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            system_table_type,
            metadata_file_location: "s3://warehouse/db/t/metadata/00007-abc.metadata.json"
                .to_string(),
            table_uuid: "9d1f4c1e-6a1f-4a0b-9c3a-0f2b6d5e7a11".to_string(),
            snapshot_id: Some(11),
        })
        .expect("reference")
    }

    fn manifest(path: &str) -> TrinoManifestFile {
        TrinoManifestFile::try_new(TrinoManifestFileParams {
            path: path.to_string(),
            length: 4096,
            partition_spec_id: 7,
            content: TrinoManifestContent::Data,
            sequence_number: 3,
            min_sequence_number: 1,
            added_snapshot_id: 11,
            added_files_count: Some(2),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(200),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            first_row_id: Some(0),
            key_metadata: Vec::new(),
        })
        .expect("manifest")
    }

    fn split_source_params(manifests: Vec<TrinoManifestFile>) -> FilesTableSplitSourceParams {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        let mut partition_spec_jsons = BTreeMap::new();
        partition_spec_jsons.insert(
            spec.spec_id(),
            serde_json::to_string(&spec).expect("spec json"),
        );
        FilesTableSplitSourceParams {
            reference: reference(IcebergSystemTableType::Files),
            manifests,
            table_schema_json: serde_json::to_string(&schema).expect("schema json"),
            metadata_table_schema_json: r#"{"columns":["content","file_path"]}"#.to_string(),
            partition_spec_jsons,
            partition_column_type_json: Some(r#"{"type":"struct"}"#.to_string()),
            bounds_column_type_json: None,
            encryption_key_id: None,
        }
    }

    fn drain(source: &mut FilesTableSplitSource) -> Vec<FilesTableSplit> {
        let snapshot = DynamicFilterSnapshot::<IcebergColumnHandle>::all_complete();
        let mut splits = Vec::new();
        loop {
            let batch = source.next_batch(16, &snapshot).expect("batch");
            let no_more = batch.no_more_splits();
            splits.extend(batch.into_splits());
            if no_more {
                break;
            }
        }
        splits
    }

    #[test]
    fn the_worker_relation_set_is_closed_and_rejects_unknown_and_unspecified() {
        for (raw, expected) in [
            (1, IcebergSystemTableType::Files),
            (2, IcebergSystemTableType::Entries),
            (3, IcebergSystemTableType::Snapshots),
            (4, IcebergSystemTableType::History),
            (5, IcebergSystemTableType::Refs),
            (6, IcebergSystemTableType::Manifests),
            (7, IcebergSystemTableType::Partitions),
        ] {
            assert_eq!(
                IcebergSystemTableType::from_proto(raw).expect("known type"),
                expected
            );
            assert_eq!(expected.to_proto() as i32, raw);
        }
        assert!(IcebergSystemTableType::from_proto(0).is_err());
        assert!(IcebergSystemTableType::from_proto(8).is_err());
        assert!(IcebergSystemTableType::from_proto(-1).is_err());
    }

    #[test]
    fn entries_resolves_directly_and_partitions_is_not_a_worker_relation() {
        assert_eq!(
            IcebergSystemTableType::try_from_suffix("$entries").expect("entries"),
            IcebergSystemTableType::Entries
        );
        let partitions =
            IcebergSystemTableType::try_from_suffix("$partitions").expect_err("not a worker type");
        assert_eq!(partitions.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(
            IcebergSystemTableType::try_from_suffix("$all_entries")
                .expect_err("aggregating relation")
                .kind(),
            ConnectorErrorKind::Unsupported
        );
        assert_eq!(
            IcebergSystemTableType::try_from_suffix("$nope")
                .expect_err("unknown relation")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn only_files_is_distributed_and_the_other_five_build_no_split() {
        assert_eq!(
            IcebergSystemTableType::Files.distribution(),
            SystemTableDistribution::AllNodes
        );
        assert!(IcebergSystemTableType::Files.produces_splits());
        for relation in [
            IcebergSystemTableType::Entries,
            IcebergSystemTableType::Snapshots,
            IcebergSystemTableType::History,
            IcebergSystemTableType::Refs,
            IcebergSystemTableType::Manifests,
        ] {
            assert_eq!(
                relation.distribution(),
                SystemTableDistribution::SingleCoordinator
            );
            assert_eq!(
                relation.execution(),
                IcebergSystemTableExecution::SingleBackendDirectPageSource
            );
            assert!(!relation.produces_splits());
            // A direct page source relation has no split source at all; asking
            // for one is a planning error, not an empty enumeration.
            let mut params = split_source_params(vec![manifest("m0.avro")]);
            params.reference = reference(relation);
            assert!(FilesTableSplitSource::try_new(params).is_err());
        }
    }

    #[test]
    fn files_produces_exactly_one_split_per_manifest() {
        let manifests = vec![
            manifest("m0.avro"),
            manifest("m1.avro"),
            manifest("m2.avro"),
        ];
        let mut source =
            FilesTableSplitSource::try_new(split_source_params(manifests)).expect("source");
        let splits = drain(&mut source);
        assert_eq!(splits.len(), 3);
        let paths = splits
            .iter()
            .map(|split| split.manifest().path().to_string())
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["m0.avro", "m1.avro", "m2.avro"]);
        assert!(source.is_finished());
        // Every split repeats the same frozen schemas so a worker needs nothing
        // but the split itself.
        for split in &splits {
            assert_eq!(split.table_schema_json(), splits[0].table_schema_json());
            assert!(split.parse_partition_spec(7).is_ok());
            assert!(ConnectorSplit::retained_size_in_bytes(split) > 0);
            assert_eq!(ConnectorSplit::split_weight(split), SplitWeight::STANDARD);
            assert!(ConnectorSplit::affinity_key(split).is_none());
        }
    }

    #[test]
    fn a_snapshot_with_no_manifests_is_an_empty_relation_not_a_synthetic_split() {
        let mut source =
            FilesTableSplitSource::try_new(split_source_params(Vec::new())).expect("source");
        assert!(source.is_finished());
        assert!(drain(&mut source).is_empty());
    }

    #[test]
    fn a_metadata_uuid_or_snapshot_mismatch_fails_closed() {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            spec,
            crate::iceberg::spec::SortOrder::builder()
                .build_unbound()
                .expect("sort order"),
            "s3://warehouse/db/t".to_string(),
            crate::iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;

        let frozen = IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            system_table_type: IcebergSystemTableType::Snapshots,
            metadata_file_location: "s3://warehouse/db/t/metadata/00007-abc.metadata.json"
                .to_string(),
            table_uuid: metadata.uuid().hyphenated().to_string(),
            snapshot_id: None,
        })
        .expect("reference");
        assert!(frozen.verify_loaded_metadata(&metadata).is_ok());

        // A location reused by another table must never silently answer.
        let wrong_uuid = reference(IcebergSystemTableType::Snapshots);
        let error = wrong_uuid
            .verify_loaded_metadata(&metadata)
            .expect_err("uuid mismatch");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);

        // A snapshot that is absent must not fall back to the current one.
        let missing_snapshot =
            IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
                schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
                system_table_type: IcebergSystemTableType::Snapshots,
                metadata_file_location: "s3://warehouse/db/t/metadata/00007-abc.metadata.json"
                    .to_string(),
                table_uuid: metadata.uuid().hyphenated().to_string(),
                snapshot_id: Some(4242),
            })
            .expect("reference");
        assert_eq!(
            missing_snapshot
                .verify_loaded_metadata(&metadata)
                .expect_err("snapshot mismatch")
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn a_reference_rejects_a_malformed_identity_or_an_unpinned_files_relation() {
        let mut params = IcebergSystemTableReferenceParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            system_table_type: IcebergSystemTableType::Files,
            metadata_file_location: "s3://warehouse/db/t/metadata/00007-abc.metadata.json"
                .to_string(),
            table_uuid: "9d1f4c1e-6a1f-4a0b-9c3a-0f2b6d5e7a11".to_string(),
            snapshot_id: Some(11),
        };
        assert!(IcebergSystemTableReference::try_new(params.clone()).is_ok());

        params.table_uuid = "not-a-uuid".to_string();
        assert!(IcebergSystemTableReference::try_new(params.clone()).is_err());
        params.table_uuid = "9d1f4c1e-6a1f-4a0b-9c3a-0f2b6d5e7a11".to_string();

        params.metadata_file_location = "s3://warehouse/db/t/metadata/".to_string();
        assert!(IcebergSystemTableReference::try_new(params.clone()).is_err());
        params.metadata_file_location =
            "s3://warehouse/db/t/metadata/00007-abc.metadata.json".to_string();

        // `$files` reports one snapshot's files; without one there is nothing
        // to walk and no legal fallback. `$partitions` aggregates those same
        // files, so it needs one for the same reason.
        params.snapshot_id = None;
        assert!(IcebergSystemTableReference::try_new(params.clone()).is_err());
        params.system_table_type = IcebergSystemTableType::Partitions;
        assert!(IcebergSystemTableReference::try_new(params.clone()).is_err());
        params.system_table_type = IcebergSystemTableType::Refs;
        assert!(IcebergSystemTableReference::try_new(params).is_ok());
    }

    #[test]
    fn an_encrypted_manifest_is_stably_unsupported() {
        let error = TrinoManifestFile::try_new(TrinoManifestFileParams {
            path: "m0.avro".to_string(),
            length: 1,
            partition_spec_id: 7,
            content: TrinoManifestContent::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 11,
            added_files_count: None,
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            first_row_id: None,
            key_metadata: vec![1, 2, 3],
        })
        .expect_err("encrypted manifest");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);

        let mut params = split_source_params(vec![manifest("m0.avro")]);
        params.encryption_key_id = Some("key-1".to_string());
        assert_eq!(
            FilesTableSplitSource::try_new(params)
                .expect_err("encrypted relation")
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }

    #[test]
    fn a_reference_and_a_files_split_round_trip_through_the_wire() {
        for relation in [
            IcebergSystemTableType::Files,
            IcebergSystemTableType::Entries,
            IcebergSystemTableType::Snapshots,
            IcebergSystemTableType::History,
            IcebergSystemTableType::Refs,
            IcebergSystemTableType::Manifests,
        ] {
            let frozen = reference(relation);
            let decoded = IcebergSystemTableReference::from_proto(&frozen.to_proto())
                .expect("decoded reference");
            assert_eq!(decoded, frozen);
        }

        let mut source = FilesTableSplitSource::try_new(split_source_params(vec![
            manifest("m0.avro"),
            manifest("m1.avro"),
        ]))
        .expect("source");
        for split in drain(&mut source) {
            let decoded = FilesTableSplit::from_proto(&split.to_proto()).expect("decoded split");
            assert_eq!(decoded.manifest(), split.manifest());
            assert_eq!(decoded.table_schema_json(), split.table_schema_json());
            assert_eq!(
                decoded.metadata_table_schema_json(),
                split.metadata_table_schema_json()
            );
            assert_eq!(decoded.partition_spec_jsons(), split.partition_spec_jsons());
            assert_eq!(
                decoded.partition_column_type_json(),
                split.partition_column_type_json()
            );
            assert!(decoded.encryption_key_id().is_none());
        }
    }

    #[test]
    fn a_manifest_naming_an_unknown_partition_spec_fails_closed() {
        let mut params = split_source_params(vec![manifest("m0.avro")]);
        params.partition_spec_jsons.clear();
        assert!(FilesTableSplitSource::try_new(params).is_err());

        let mut source =
            FilesTableSplitSource::try_new(split_source_params(vec![manifest("m0.avro")]))
                .expect("source");
        let split = drain(&mut source).remove(0);
        let mut foreign = manifest("m1.avro");
        foreign = TrinoManifestFile::try_new(TrinoManifestFileParams {
            path: foreign.path().to_string(),
            length: foreign.length(),
            partition_spec_id: 99,
            content: foreign.content(),
            sequence_number: foreign.sequence_number(),
            min_sequence_number: foreign.min_sequence_number(),
            added_snapshot_id: foreign.added_snapshot_id(),
            added_files_count: foreign.added_files_count(),
            existing_files_count: foreign.existing_files_count(),
            deleted_files_count: foreign.deleted_files_count(),
            added_rows_count: foreign.added_rows_count(),
            existing_rows_count: foreign.existing_rows_count(),
            deleted_rows_count: foreign.deleted_rows_count(),
            first_row_id: foreign.first_row_id(),
            key_metadata: Vec::new(),
        })
        .expect("manifest");
        assert!(split.with_manifest(foreign).is_err());
    }

    #[test]
    fn an_unsatisfiable_dynamic_filter_finishes_without_a_split() {
        let mut source =
            FilesTableSplitSource::try_new(split_source_params(vec![manifest("m0.avro")]))
                .expect("source");
        let snapshot = DynamicFilterSnapshot::<IcebergColumnHandle>::new(TupleDomain::none(), true);
        let batch = source.next_batch(16, &snapshot).expect("batch");
        assert!(batch.is_empty());
        assert!(batch.no_more_splits());
        assert!(source.is_finished());
    }
}
