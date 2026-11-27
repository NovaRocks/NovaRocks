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
    SchemaTableName, SplitWeight, SystemTableUnitHint,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::iceberg::spec::{
    FormatVersion, ManifestContentType, ManifestFile, PartitionSpec, Schema, TableMetadata,
};

use crate::partition_row::UnifiedPartitionRow;

use super::column_handle::{IcebergColumnHandle, corrupt, invalid, unsupported};
use super::table_handle::MAX_PARTITION_SPECS;

/// Maximum number of manifests one `$files` or `$entries` relation may enumerate.
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

    /// Placement hint only; every relation is enumerated through splits.
    pub const fn unit_hint(self) -> SystemTableUnitHint {
        match self {
            Self::Files | Self::Entries => SystemTableUnitHint::ManyUnits,
            Self::Snapshots | Self::History | Self::Refs | Self::Manifests => {
                SystemTableUnitHint::SingleUnit
            }
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

pub(crate) fn require_supported_manifests_format(
    format_version: FormatVersion,
) -> Result<(), ConnectorError> {
    if format_version == FormatVersion::V1 {
        return Err(unsupported(
            "Iceberg $manifests is unsupported for format-version 1 tables because iceberg-rust 0.11 cannot read v1 manifest list file counts written by Iceberg Java. Support resumes in B6c-2 with the upstream field-ID-based manifest list reader.",
        ));
    }
    Ok(())
}

/// One immutable metadata relation and its scan-wide declaration inputs.
#[derive(Clone, Debug)]
pub struct SystemTableScan {
    reference: IcebergSystemTableReference,
    facts: Arc<SystemTableScanFacts>,
}

#[derive(Debug)]
struct SystemTableScanFacts {
    format_version: FormatVersion,
    table_schema: Schema,
    specs: BTreeMap<i32, PartitionSpec>,
    partitions: UnifiedPartitionRow,
    columns: Vec<IcebergColumnHandle>,
    source_schema: arrow::datatypes::SchemaRef,
    metric_fields: Box<[super::system_page_source::StatisticsField]>,
    metric_shape_bytes: u128,
    table_has_variant_history: bool,
}

impl SystemTableScan {
    pub(crate) fn from_metadata(
        reference: IcebergSystemTableReference,
        metadata: &TableMetadata,
    ) -> Result<Self, ConnectorError> {
        reference.verify_loaded_metadata(metadata)?;
        Self::from_facts(
            reference,
            metadata.format_version(),
            metadata.current_schema().as_ref().clone(),
            metadata
                .partition_specs_iter()
                .map(|spec| spec.as_ref().clone()),
            crate::manifest::table_has_variant_history(metadata),
        )
    }

    pub(crate) fn from_facts(
        reference: IcebergSystemTableReference,
        format_version: FormatVersion,
        table_schema: Schema,
        specs: impl IntoIterator<Item = PartitionSpec>,
        table_has_variant_history: bool,
    ) -> Result<Self, ConnectorError> {
        let mut by_id = BTreeMap::new();
        for spec in specs {
            if by_id.insert(spec.spec_id(), spec).is_some() {
                return Err(invalid(
                    "iceberg system scan has a duplicate partition spec",
                ));
            }
            if by_id.len() > MAX_PARTITION_SPECS {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "iceberg partition spec count exceeds the hard limit",
                ));
            }
        }
        let partitions =
            if reference.system_table_type().unit_hint() == SystemTableUnitHint::ManyUnits {
                let specs: Vec<_> = by_id.values().collect();
                UnifiedPartitionRow::of_checked(&table_schema, &specs, || {
                    Ok::<_, std::convert::Infallible>(())
                })
                .map_err(|error| match error {
                    crate::partition_row::PartitionRowError::Semantic(error) => error,
                    crate::partition_row::PartitionRowError::Original(never) => match never {},
                })?
            } else {
                UnifiedPartitionRow {
                    fields: Vec::new(),
                    slots_by_spec: BTreeMap::new(),
                }
            };
        let (columns, source_schema) = super::system_page_source::freeze_system_scan_declaration(
            reference.system_table_type(),
            &table_schema,
            &partitions,
        )?;
        let metric_fields =
            if reference.system_table_type().unit_hint() == SystemTableUnitHint::ManyUnits {
                super::system_page_source::freeze_metric_fields(&table_schema)?
            } else {
                Box::new([])
            };
        let metric_shape_bytes = metric_fields
            .iter()
            .map(|field| 512 + field.name.len() as u128 * 4)
            .sum();
        Ok(Self {
            reference,
            facts: Arc::new(SystemTableScanFacts {
                format_version,
                table_schema,
                specs: by_id,
                partitions,
                columns,
                source_schema,
                metric_fields,
                metric_shape_bytes,
                table_has_variant_history,
            }),
        })
    }

    pub const fn reference(&self) -> &IcebergSystemTableReference {
        &self.reference
    }

    pub const fn system_table_type(&self) -> IcebergSystemTableType {
        self.reference.system_table_type()
    }

    pub fn format_version(&self) -> FormatVersion {
        self.facts.format_version
    }

    pub fn table_schema(&self) -> &Schema {
        &self.facts.table_schema
    }

    pub fn partition_spec(&self, spec_id: i32) -> Option<&PartitionSpec> {
        self.facts.specs.get(&spec_id)
    }

    pub(crate) fn partition_specs(&self) -> &BTreeMap<i32, PartitionSpec> {
        &self.facts.specs
    }

    pub(crate) fn partitions(&self) -> &UnifiedPartitionRow {
        &self.facts.partitions
    }

    pub(crate) fn columns(&self) -> &[IcebergColumnHandle] {
        &self.facts.columns
    }

    pub(crate) fn source_schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.facts.source_schema
    }

    pub(crate) fn project_columns(
        &self,
        columns: &[IcebergColumnHandle],
    ) -> Result<Vec<usize>, ConnectorError> {
        columns
            .iter()
            .map(|column| {
                self.facts
                    .columns
                    .iter()
                    .position(|expected| expected == column)
                    .ok_or_else(|| {
                        invalid(
                            "iceberg system projection differs from its frozen column declaration",
                        )
                    })
            })
            .collect()
    }

    pub(super) fn metric_fields(&self) -> &[super::system_page_source::StatisticsField] {
        &self.facts.metric_fields
    }

    pub(super) fn metric_shape_bytes(&self) -> u128 {
        self.facts.metric_shape_bytes
    }

    pub fn table_has_variant_history(&self) -> bool {
        self.facts.table_has_variant_history
    }

    pub(crate) fn validate_manifest(
        &self,
        split: &SystemManifestSplit,
    ) -> Result<&PartitionSpec, ConnectorError> {
        if split.reference() != self.reference() {
            return Err(invalid(
                "iceberg manifest work unit differs from its frozen scan reference",
            ));
        }
        let spec_id = split.manifest().partition_spec_id();
        self.partition_spec(spec_id).ok_or_else(|| {
            invalid(format!(
                "iceberg manifest work unit names unknown frozen partition spec {spec_id}"
            ))
        })
    }

    #[cfg(test)]
    pub(crate) fn empty_for_test(reference: IcebergSystemTableReference) -> Self {
        Self::from_facts(
            reference,
            FormatVersion::V2,
            Schema::builder().build().unwrap(),
            [],
            false,
        )
        .expect("empty system scan")
    }
}

/// A manifest is one unit of work in a frozen files or entries relation.
#[derive(Clone, Debug)]
pub struct SystemManifestSplit {
    reference: IcebergSystemTableReference,
    manifest: TrinoManifestFile,
}

impl SystemManifestSplit {
    pub fn try_new(
        reference: IcebergSystemTableReference,
        manifest: TrinoManifestFile,
    ) -> Result<Self, ConnectorError> {
        if !matches!(
            reference.system_table_type(),
            IcebergSystemTableType::Files | IcebergSystemTableType::Entries
        ) || reference.snapshot_id().is_none()
        {
            return Err(invalid(
                "iceberg manifest split requires a files or entries reference with a snapshot",
            ));
        }
        Ok(Self {
            reference,
            manifest,
        })
    }

    pub const fn reference(&self) -> &IcebergSystemTableReference {
        &self.reference
    }

    pub const fn manifest(&self) -> &TrinoManifestFile {
        &self.manifest
    }

    #[cfg(test)]
    pub fn to_proto(&self) -> dto::SystemManifestSplit {
        dto::SystemManifestSplit {
            reference: Some(self.reference.to_proto()),
            manifest: Some(self.manifest.to_proto()),
        }
    }

    #[cfg(test)]
    pub fn from_proto(raw: &dto::SystemManifestSplit) -> Result<Self, ConnectorError> {
        Self::try_new(
            IcebergSystemTableReference::from_proto(
                raw.reference
                    .as_ref()
                    .ok_or_else(|| invalid("iceberg manifest split requires a frozen reference"))?,
            )?,
            TrinoManifestFile::from_proto(
                raw.manifest
                    .as_ref()
                    .ok_or_else(|| invalid("iceberg manifest split requires a manifest"))?,
            )?,
        )
    }
}

impl ConnectorSplit for SystemManifestSplit {
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
        (size_of::<Self>()
            + self.reference.metadata_file_location().len()
            + self.reference.schema_table_name().schema_name().len()
            + self.reference.schema_table_name().table_name().len()
            + self.reference.table_uuid().len()
            + self.manifest.path().len()) as u64
    }
}

/// One indivisible metadata-relation read, carrying the exact scan reference.
#[derive(Clone, Debug)]
pub struct SystemRelationSplit {
    reference: IcebergSystemTableReference,
}

impl SystemRelationSplit {
    pub fn try_new(reference: IcebergSystemTableReference) -> Result<Self, ConnectorError> {
        if matches!(
            reference.system_table_type(),
            IcebergSystemTableType::Files | IcebergSystemTableType::Entries
        ) {
            return Err(invalid(
                "iceberg files and entries require manifest work units",
            ));
        }
        Ok(Self { reference })
    }

    pub const fn reference(&self) -> &IcebergSystemTableReference {
        &self.reference
    }
}

impl ConnectorSplit for SystemRelationSplit {
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
        SplitWeight::STANDARD
    }
    fn retained_size_in_bytes(&self) -> u64 {
        (size_of::<Self>()
            + self.reference.metadata_file_location().len()
            + self.reference.schema_table_name().schema_name().len()
            + self.reference.schema_table_name().table_name().len()) as u64
    }
}

/// A lazily advancing enumerator over one pinned snapshot's manifests.
#[derive(Debug)]
pub struct SystemManifestSplitSource {
    scan: SystemTableScan,
    manifests: VecDeque<TrinoManifestFile>,
    closed: bool,
    exhausted: bool,
}

impl SystemManifestSplitSource {
    pub fn try_new(
        scan: &SystemTableScan,
        manifests: Vec<TrinoManifestFile>,
    ) -> Result<Self, ConnectorError> {
        let reference = scan.reference();
        if reference.system_table_type().unit_hint() != SystemTableUnitHint::ManyUnits {
            return Err(invalid(
                "iceberg manifest source requires a files or entries reference",
            ));
        }
        if reference.snapshot_id().is_none() && !manifests.is_empty() {
            return Err(invalid(
                "iceberg manifest source without a snapshot must have no manifests",
            ));
        }
        if manifests.len() > MAX_FILES_TABLE_MANIFESTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg $files manifest count exceeds the hard limit",
            ));
        }
        let source = Self {
            scan: scan.clone(),
            exhausted: manifests.is_empty(),
            manifests: manifests.into(),
            closed: false,
        };
        if let Some(first) = source.manifests.front() {
            source.validate_spec(first)?;
        }
        Ok(source)
    }

    fn validate_spec(&self, manifest: &TrinoManifestFile) -> Result<(), ConnectorError> {
        let spec_id = manifest.partition_spec_id();
        if self.scan.partition_spec(spec_id).is_none() {
            return Err(invalid(format!(
                "iceberg manifest work unit names unknown frozen partition spec {spec_id}"
            )));
        }
        Ok(())
    }
}

impl ConnectorSplitSource for SystemManifestSplitSource {
    type Split = SystemManifestSplit;
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

        let max_size = max_size.min(MAX_SPLITS_PER_ASSIGNMENT);
        let mut produced = Vec::with_capacity(max_size.min(self.manifests.len()));
        while produced.len() < max_size {
            let Some(manifest) = self.manifests.pop_front() else {
                break;
            };
            self.validate_spec(&manifest)?;
            produced.push(SystemManifestSplit::try_new(
                self.scan.reference().clone(),
                manifest,
            )?);
        }

        let no_more_splits = self.manifests.is_empty();
        Ok(ConnectorSplitBatch::new(produced, no_more_splits))
    }

    fn is_finished(&self) -> bool {
        self.closed || self.exhausted || self.manifests.is_empty()
    }

    /// Idempotent. A batch already returned by value cannot be retracted.
    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.manifests.clear();
        Ok(())
    }
}

pub(crate) fn parse_table_schema(schema_json: &str) -> Result<Schema, ConnectorError> {
    if schema_json.is_empty() || schema_json.len() > MAX_JSON_BYTES {
        return Err(invalid(
            "iceberg table schema json must be non-empty and bounded",
        ));
    }
    crate::metadata_json::from_str(schema_json)
        .map_err(|error| invalid(format!("iceberg table schema json is invalid: {error}")))
}

pub(crate) fn parse_partition_spec(spec_json: &str) -> Result<PartitionSpec, ConnectorError> {
    if spec_json.is_empty() || spec_json.len() > MAX_JSON_BYTES {
        return Err(invalid(
            "iceberg partition spec json must be non-empty and bounded",
        ));
    }
    crate::metadata_json::from_str(spec_json)
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

    fn scan(reference: IcebergSystemTableReference) -> SystemTableScan {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        SystemTableScan::from_facts(reference, FormatVersion::V2, schema, [spec], true)
            .expect("system scan")
    }

    fn split_source(
        manifests: Vec<TrinoManifestFile>,
    ) -> Result<SystemManifestSplitSource, ConnectorError> {
        SystemManifestSplitSource::try_new(
            &scan(reference(IcebergSystemTableType::Files)),
            manifests,
        )
    }

    fn drain(source: &mut SystemManifestSplitSource) -> Vec<SystemManifestSplit> {
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
        ] {
            assert_eq!(
                IcebergSystemTableType::from_proto(raw).expect("known type"),
                expected
            );
            assert_eq!(expected.to_proto() as i32, raw);
        }
        assert!(IcebergSystemTableType::from_proto(0).is_err());
        assert!(IcebergSystemTableType::from_proto(7).is_err());
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
    fn manifest_relations_use_many_units_and_singleton_relations_use_one() {
        for kind in [
            IcebergSystemTableType::Files,
            IcebergSystemTableType::Entries,
        ] {
            assert_eq!(kind.unit_hint(), SystemTableUnitHint::ManyUnits);
            let mut source = SystemManifestSplitSource::try_new(
                &scan(reference(kind)),
                vec![manifest("m0.avro"), manifest("m1.avro")],
            )
            .unwrap();
            let splits = drain(&mut source);
            assert_eq!(splits.len(), 2);
            assert!(
                splits
                    .iter()
                    .all(|split| split.reference() == &reference(kind))
            );
        }
        for kind in [
            IcebergSystemTableType::Snapshots,
            IcebergSystemTableType::History,
            IcebergSystemTableType::Refs,
            IcebergSystemTableType::Manifests,
        ] {
            assert_eq!(kind.unit_hint(), SystemTableUnitHint::SingleUnit);
            let split = SystemRelationSplit::try_new(reference(kind)).unwrap();
            assert_eq!(split.reference(), &reference(kind));
            assert!(
                SystemManifestSplitSource::try_new(
                    &scan(reference(kind)),
                    vec![manifest("m0.avro")],
                )
                .is_err()
            );
        }
    }

    #[test]
    fn files_produces_exactly_one_split_per_manifest() {
        let manifests = vec![
            manifest("m0.avro"),
            manifest("m1.avro"),
            manifest("m2.avro"),
        ];
        let mut source = split_source(manifests).expect("source");
        let splits = drain(&mut source);
        assert_eq!(splits.len(), 3);
        let paths = splits
            .iter()
            .map(|split| split.manifest().path().to_string())
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["m0.avro", "m1.avro", "m2.avro"]);
        assert!(source.is_finished());
        let scan = scan(reference(IcebergSystemTableType::Files));
        for split in &splits {
            assert!(scan.validate_manifest(split).is_ok());
            assert!(ConnectorSplit::retained_size_in_bytes(split) > 0);
            assert_eq!(ConnectorSplit::split_weight(split), SplitWeight::STANDARD);
            assert!(ConnectorSplit::affinity_key(split).is_none());
        }
    }

    #[test]
    fn files_and_entries_without_a_snapshot_are_valid_empty_sources() {
        for kind in [
            IcebergSystemTableType::Files,
            IcebergSystemTableType::Entries,
        ] {
            let mut raw = reference(kind).to_proto();
            raw.snapshot_id = None;
            let reference = IcebergSystemTableReference::from_proto(&raw).unwrap();
            let mut source =
                SystemManifestSplitSource::try_new(&scan(reference), Vec::new()).unwrap();
            assert!(source.is_finished());
            assert!(drain(&mut source).is_empty());
        }
    }

    #[test]
    fn a_snapshot_with_no_manifests_is_an_empty_relation_not_a_synthetic_split() {
        let mut source = split_source(Vec::new()).expect("source");
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
    fn a_reference_rejects_malformed_identity_and_allows_snapshotless_manifest_relations() {
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

        // A snapshotless files/entries reference denotes zero manifest units;
        // it does not select a later snapshot. The temporary provider-owned
        // partitions relation still needs a snapshot until P06 removes it.
        params.snapshot_id = None;
        assert!(IcebergSystemTableReference::try_new(params.clone()).is_ok());
        params.system_table_type = IcebergSystemTableType::Entries;
        assert!(IcebergSystemTableReference::try_new(params.clone()).is_ok());
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

        let mut source =
            split_source(vec![manifest("m0.avro"), manifest("m1.avro")]).expect("source");
        for split in drain(&mut source) {
            let decoded =
                SystemManifestSplit::from_proto(&split.to_proto()).expect("decoded split");
            assert_eq!(decoded.manifest(), split.manifest());

            assert_eq!(decoded.reference(), split.reference());
            assert_eq!(
                decoded.retained_size_in_bytes(),
                split.retained_size_in_bytes()
            );
        }
    }

    #[test]
    fn a_manifest_naming_an_unknown_partition_spec_fails_closed() {
        let scan = scan(reference(IcebergSystemTableType::Files));
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
        assert!(SystemManifestSplitSource::try_new(&scan, vec![foreign.clone()]).is_err());
        let split = SystemManifestSplit::try_new(scan.reference().clone(), foreign).unwrap();
        assert_eq!(
            scan.validate_manifest(&split).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn an_unsatisfiable_dynamic_filter_finishes_without_a_split() {
        let mut source = split_source(vec![manifest("m0.avro")]).expect("source");
        let snapshot = DynamicFilterSnapshot::<IcebergColumnHandle>::new(TupleDomain::none(), true);
        let batch = source.next_batch(16, &snapshot).expect("batch");
        assert!(batch.is_empty());
        assert!(batch.no_more_splits());
        assert!(source.is_finished());
    }
}
