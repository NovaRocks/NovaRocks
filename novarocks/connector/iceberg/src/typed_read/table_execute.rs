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

//! `ALTER TABLE ... EXECUTE` procedures and the one distributed rewrite that
//! is not a scan.
//!
//! Trino's eight Iceberg procedures are carried unchanged. Exactly one of
//! them, `OPTIMIZE`, reads data through an ordinary [`super::split::IcebergSplit`];
//! the other seven are coordinator work over metadata and carry no procedure
//! handle at all.
//!
//! NovaRocks adds exactly one extension, `REWRITE_POSITION_DELETE_FILES`. It
//! reads Puffin deletion vectors -- not table rows -- and writes a new
//! position-delete file, so it is its own procedure with its own split and its
//! own page source. Dressing it up as `OPTIMIZE`, or routing it through an
//! ordinary scan with a read-purpose branch, would put a delete-artifact
//! reader behind a code path whose every other caller reads data files.

use std::sync::Arc;

use crate::wire::dto;
use novarocks_proto_codec::connector_read::{
    MAX_DELETES_PER_SPLIT, MAX_JSON_BYTES, MAX_PATH_BYTES,
};
use novarocks_spi::connector::read_stack::{
    ConnectorSplit, ConnectorTableExecuteHandle as ConnectorTableExecuteHandleMarker, HostAddress,
    SchemaTableName, SplitWeight,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use super::column_handle::invalid;
use super::schema_binding::IcebergMetadataColumn;
use super::split::{IcebergDeleteFile, IcebergDeleteFileContent, IcebergFileFormat};
use super::table_handle::IcebergTableHandle;

/// The exact output contract of the rewrite-position-delete page source.
///
/// These are the two columns an Iceberg position-delete file holds, in the
/// order the writer expects them. The page source produces nothing else: it is
/// re-encoding delete positions, not projecting table columns.
///
/// The physical field IDs retain Iceberg's metadata-column identity. The wire
/// and Arrow names are deliberately procedure-specific: they are the public
/// `REWRITE_POSITION_DELETE_FILES` result contract, not ordinary `_file` /
/// `_pos` scan metadata aliases.
pub const REWRITE_POSITION_DELETE_OUTPUT_COLUMNS: [(&str, IcebergMetadataColumn); 2] = [
    ("file_path", IcebergMetadataColumn::Path),
    ("pos", IcebergMetadataColumn::RowPosition),
];

/// Length of a SHA-256 digest rendered as lowercase hex.
const DIGEST_HEX_LEN: usize = 64;

/// Every `ALTER TABLE ... EXECUTE` procedure this connector answers to.
///
/// The first eight are Trino's. The ninth is the single NovaRocks extension,
/// named explicitly rather than folded into `OPTIMIZE`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergProcedureId {
    Optimize,
    OptimizeManifests,
    DropExtendedStats,
    RollbackToSnapshot,
    ExpireSnapshots,
    RemoveOrphanFiles,
    AddFiles,
    AddFilesFromTable,
    RewritePositionDeleteFiles,
}

/// Where a procedure's work happens, and what it reads while it happens.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergProcedureExecution {
    /// Metadata-only work the coordinator performs itself. No split, no
    /// worker, no procedure handle.
    Coordinator,
    /// Distributed over ordinary data splits: the procedure rewrites table
    /// rows, so it reads them the same way a scan does.
    DistributedDataSplits,
    /// Distributed over
    /// [`IcebergRewritePositionDeleteFilesSplit`]: the procedure reads delete
    /// artifacts, never table rows.
    DistributedRewritePositionDeleteSplits,
}

impl IcebergProcedureId {
    pub const fn execution(self) -> IcebergProcedureExecution {
        match self {
            Self::Optimize => IcebergProcedureExecution::DistributedDataSplits,
            Self::RewritePositionDeleteFiles => {
                IcebergProcedureExecution::DistributedRewritePositionDeleteSplits
            }
            Self::OptimizeManifests
            | Self::DropExtendedStats
            | Self::RollbackToSnapshot
            | Self::ExpireSnapshots
            | Self::RemoveOrphanFiles
            | Self::AddFiles
            | Self::AddFilesFromTable => IcebergProcedureExecution::Coordinator,
        }
    }

    /// Whether a worker-visible procedure handle exists for this procedure.
    ///
    /// A coordinator-executed procedure has nothing to send: inventing an
    /// empty handle for it would make "no handle" and "handle with no facts"
    /// two ways to say the same thing.
    pub const fn has_procedure_handle(self) -> bool {
        match self.execution() {
            IcebergProcedureExecution::Coordinator => false,
            IcebergProcedureExecution::DistributedDataSplits
            | IcebergProcedureExecution::DistributedRewritePositionDeleteSplits => true,
        }
    }

    fn to_proto(self) -> dto::IcebergProcedureId {
        match self {
            Self::Optimize => dto::IcebergProcedureId::Optimize,
            Self::OptimizeManifests => dto::IcebergProcedureId::OptimizeManifests,
            Self::DropExtendedStats => dto::IcebergProcedureId::DropExtendedStats,
            Self::RollbackToSnapshot => dto::IcebergProcedureId::RollbackToSnapshot,
            Self::ExpireSnapshots => dto::IcebergProcedureId::ExpireSnapshots,
            Self::RemoveOrphanFiles => dto::IcebergProcedureId::RemoveOrphanFiles,
            Self::AddFiles => dto::IcebergProcedureId::AddFiles,
            Self::AddFilesFromTable => dto::IcebergProcedureId::AddFilesFromTable,
            Self::RewritePositionDeleteFiles => dto::IcebergProcedureId::RewritePositionDeleteFiles,
        }
    }

    fn from_proto(raw: i32) -> Result<Self, ConnectorError> {
        let value = dto::IcebergProcedureId::try_from(raw)
            .map_err(|_| invalid("unknown iceberg procedure id"))?;
        match value {
            dto::IcebergProcedureId::Unspecified => {
                Err(invalid("iceberg procedure id must be specified"))
            }
            dto::IcebergProcedureId::Optimize => Ok(Self::Optimize),
            dto::IcebergProcedureId::OptimizeManifests => Ok(Self::OptimizeManifests),
            dto::IcebergProcedureId::DropExtendedStats => Ok(Self::DropExtendedStats),
            dto::IcebergProcedureId::RollbackToSnapshot => Ok(Self::RollbackToSnapshot),
            dto::IcebergProcedureId::ExpireSnapshots => Ok(Self::ExpireSnapshots),
            dto::IcebergProcedureId::RemoveOrphanFiles => Ok(Self::RemoveOrphanFiles),
            dto::IcebergProcedureId::AddFiles => Ok(Self::AddFiles),
            dto::IcebergProcedureId::AddFilesFromTable => Ok(Self::AddFilesFromTable),
            dto::IcebergProcedureId::RewritePositionDeleteFiles => {
                Ok(Self::RewritePositionDeleteFiles)
            }
        }
    }
}

/// `OPTIMIZE`: rewrite small data files of one pinned snapshot into larger
/// ones.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergOptimizeHandle {
    table_handle: IcebergTableHandle,
    min_file_size_bytes: u64,
}

impl IcebergOptimizeHandle {
    pub fn try_new(
        table_handle: IcebergTableHandle,
        min_file_size_bytes: u64,
    ) -> Result<Self, ConnectorError> {
        // The pinned snapshot is what the rewrite reads and what its commit
        // will be validated against; without one there is nothing to optimize.
        if table_handle.snapshot_id().is_none() {
            return Err(invalid("iceberg optimize requires a pinned snapshot"));
        }
        Ok(Self {
            table_handle,
            min_file_size_bytes,
        })
    }

    pub const fn table_handle(&self) -> &IcebergTableHandle {
        &self.table_handle
    }

    pub const fn min_file_size_bytes(&self) -> u64 {
        self.min_file_size_bytes
    }

    fn to_proto(&self) -> dto::IcebergOptimizeHandle {
        dto::IcebergOptimizeHandle {
            table_handle: Some(self.table_handle.to_proto()),
            min_file_size_bytes: self.min_file_size_bytes,
        }
    }

    fn from_proto(raw: &dto::IcebergOptimizeHandle) -> Result<Self, ConnectorError> {
        let table_handle = raw
            .table_handle
            .as_ref()
            .ok_or_else(|| invalid("iceberg optimize handle requires a table handle"))?;
        Self::try_new(
            IcebergTableHandle::from_proto(table_handle)?,
            raw.min_file_size_bytes,
        )
    }
}

/// The immutable external content this rewrite was frozen against.
///
/// This is the only content digest anywhere in the typed read stack, and it
/// earns its place: the rewrite artifact is a real, separately written,
/// immutable object, so naming it by location alone would let a replacement
/// written at the same location be read as the same plan. A page source or an
/// ordinary scan produces no digest at all -- there is no external object to
/// name, and a digest of a payload would only be a second identity for the
/// facts the payload already spells out.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IcebergRewriteArtifactContentId {
    artifact_location: Arc<str>,
    artifact_digest_hex: Arc<str>,
}

impl IcebergRewriteArtifactContentId {
    pub fn try_new(
        artifact_location: impl AsRef<str>,
        artifact_digest_hex: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        let artifact_location = artifact_location.as_ref();
        if artifact_location.is_empty() || artifact_location.len() > MAX_PATH_BYTES {
            return Err(invalid(
                "iceberg rewrite artifact location must be non-empty and bounded",
            ));
        }
        if artifact_location.ends_with('/') {
            return Err(invalid(
                "iceberg rewrite artifact location must not end with a separator",
            ));
        }
        Ok(Self {
            artifact_location: Arc::from(artifact_location),
            artifact_digest_hex: Arc::from(validate_digest_hex(
                artifact_digest_hex.as_ref(),
                "artifact",
            )?),
        })
    }

    pub fn artifact_location(&self) -> &str {
        &self.artifact_location
    }

    pub fn artifact_digest_hex(&self) -> &str {
        &self.artifact_digest_hex
    }

    fn to_proto(&self) -> dto::IcebergRewriteArtifactContentId {
        dto::IcebergRewriteArtifactContentId {
            artifact_location: self.artifact_location.to_string(),
            artifact_digest_hex: self.artifact_digest_hex.to_string(),
        }
    }

    fn from_proto(raw: &dto::IcebergRewriteArtifactContentId) -> Result<Self, ConnectorError> {
        Self::try_new(&raw.artifact_location, &raw.artifact_digest_hex)
    }
}

/// `REWRITE_POSITION_DELETE_FILES`: re-encode the position deletes of one
/// exact frozen rewrite group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergRewritePositionDeleteFilesHandle {
    table_handle: IcebergTableHandle,
    artifact: IcebergRewriteArtifactContentId,
    group_digest_hex: Arc<str>,
}

impl IcebergRewritePositionDeleteFilesHandle {
    pub fn try_new(
        table_handle: IcebergTableHandle,
        artifact: IcebergRewriteArtifactContentId,
        group_digest_hex: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        if table_handle.snapshot_id().is_none() {
            return Err(invalid(
                "iceberg rewrite position delete files requires a pinned snapshot",
            ));
        }
        Ok(Self {
            table_handle,
            artifact,
            group_digest_hex: Arc::from(validate_digest_hex(group_digest_hex.as_ref(), "group")?),
        })
    }

    pub const fn table_handle(&self) -> &IcebergTableHandle {
        &self.table_handle
    }

    pub const fn artifact(&self) -> &IcebergRewriteArtifactContentId {
        &self.artifact
    }

    /// The exact group inside the artifact this procedure instance owns.
    pub fn group_digest_hex(&self) -> &str {
        &self.group_digest_hex
    }

    fn to_proto(&self) -> dto::IcebergRewritePositionDeleteFilesHandle {
        dto::IcebergRewritePositionDeleteFilesHandle {
            table_handle: Some(self.table_handle.to_proto()),
            artifact: Some(self.artifact.to_proto()),
            group_digest_hex: self.group_digest_hex.to_string(),
        }
    }

    fn from_proto(
        raw: &dto::IcebergRewritePositionDeleteFilesHandle,
    ) -> Result<Self, ConnectorError> {
        let table_handle = raw.table_handle.as_ref().ok_or_else(|| {
            invalid("iceberg rewrite position delete files handle requires a table handle")
        })?;
        let artifact = raw.artifact.as_ref().ok_or_else(|| {
            invalid("iceberg rewrite position delete files handle requires an artifact")
        })?;
        Self::try_new(
            IcebergTableHandle::from_proto(table_handle)?,
            IcebergRewriteArtifactContentId::from_proto(artifact)?,
            &raw.group_digest_hex,
        )
    }
}

/// The typed procedure handle, present exactly for the two distributed
/// procedures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergTableExecuteProcedureHandle {
    Optimize(IcebergOptimizeHandle),
    RewritePositionDeleteFiles(IcebergRewritePositionDeleteFilesHandle),
}

impl IcebergTableExecuteProcedureHandle {
    pub const fn procedure_id(&self) -> IcebergProcedureId {
        match self {
            Self::Optimize(_) => IcebergProcedureId::Optimize,
            Self::RewritePositionDeleteFiles(_) => IcebergProcedureId::RewritePositionDeleteFiles,
        }
    }
}

/// The exact facts one table-execute handle is frozen from.
#[derive(Clone, Debug)]
pub struct IcebergTableExecuteHandleParams {
    pub schema_table_name: SchemaTableName,
    pub procedure_id: IcebergProcedureId,
    pub table_location: String,
    /// Present exactly for `OPTIMIZE` and `REWRITE_POSITION_DELETE_FILES`.
    pub procedure_handle: Option<IcebergTableExecuteProcedureHandle>,
}

/// One `ALTER TABLE ... EXECUTE` invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergTableExecuteHandle {
    schema_table_name: SchemaTableName,
    procedure_id: IcebergProcedureId,
    table_location: Arc<str>,
    procedure_handle: Option<IcebergTableExecuteProcedureHandle>,
}

impl IcebergTableExecuteHandle {
    pub fn try_new(params: IcebergTableExecuteHandleParams) -> Result<Self, ConnectorError> {
        let IcebergTableExecuteHandleParams {
            schema_table_name,
            procedure_id,
            table_location,
            procedure_handle,
        } = params;

        if table_location.is_empty() || table_location.len() > MAX_PATH_BYTES {
            return Err(invalid(
                "iceberg table location must be non-empty and bounded",
            ));
        }
        match (&procedure_handle, procedure_id.has_procedure_handle()) {
            (Some(handle), true) => {
                // The id and the handle are two statements of the same fact.
                // Letting them disagree would let a worker run one procedure
                // while scheduling reported another.
                if handle.procedure_id() != procedure_id {
                    return Err(invalid(format!(
                        "iceberg procedure handle does not match procedure id {procedure_id:?}"
                    )));
                }
            }
            (None, false) => {}
            (Some(_), false) => {
                return Err(invalid(format!(
                    "iceberg procedure {procedure_id:?} runs on the coordinator and carries no procedure handle"
                )));
            }
            (None, true) => {
                return Err(invalid(format!(
                    "iceberg procedure {procedure_id:?} requires a typed procedure handle"
                )));
            }
        }

        Ok(Self {
            schema_table_name,
            procedure_id,
            table_location: Arc::from(table_location.as_str()),
            procedure_handle,
        })
    }

    pub const fn procedure_id(&self) -> IcebergProcedureId {
        self.procedure_id
    }

    pub fn table_location(&self) -> &str {
        &self.table_location
    }

    pub const fn procedure_handle(&self) -> Option<&IcebergTableExecuteProcedureHandle> {
        self.procedure_handle.as_ref()
    }

    pub const fn execution(&self) -> IcebergProcedureExecution {
        self.procedure_id.execution()
    }

    pub fn to_proto(&self) -> dto::IcebergTableExecuteHandle {
        dto::IcebergTableExecuteHandle {
            schema_table_name: Some(dto::SchemaTableName {
                schema_name: self.schema_table_name.schema_name().to_string(),
                table_name: self.schema_table_name.table_name().to_string(),
            }),
            procedure_id: self.procedure_id.to_proto() as i32,
            table_location: self.table_location.to_string(),
            procedure_handle: self.procedure_handle.as_ref().map(|handle| match handle {
                IcebergTableExecuteProcedureHandle::Optimize(optimize) => {
                    dto::iceberg_table_execute_handle::ProcedureHandle::Optimize(
                        optimize.to_proto(),
                    )
                }
                IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(rewrite) => {
                    dto::iceberg_table_execute_handle::ProcedureHandle::RewritePositionDeleteFiles(
                        rewrite.to_proto(),
                    )
                }
            }),
        }
    }

    pub fn from_proto(raw: &dto::IcebergTableExecuteHandle) -> Result<Self, ConnectorError> {
        let schema_table_name = raw
            .schema_table_name
            .as_ref()
            .ok_or_else(|| invalid("iceberg table execute handle requires a schema table name"))?;
        let procedure_handle = match raw.procedure_handle.as_ref() {
            None => None,
            Some(dto::iceberg_table_execute_handle::ProcedureHandle::Optimize(optimize)) => {
                Some(IcebergTableExecuteProcedureHandle::Optimize(
                    IcebergOptimizeHandle::from_proto(optimize)?,
                ))
            }
            Some(
                dto::iceberg_table_execute_handle::ProcedureHandle::RewritePositionDeleteFiles(
                    rewrite,
                ),
            ) => Some(
                IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(
                    IcebergRewritePositionDeleteFilesHandle::from_proto(rewrite)?,
                ),
            ),
        };
        Self::try_new(IcebergTableExecuteHandleParams {
            schema_table_name: SchemaTableName::try_new(
                &schema_table_name.schema_name,
                &schema_table_name.table_name,
            )?,
            procedure_id: IcebergProcedureId::from_proto(raw.procedure_id)?,
            table_location: raw.table_location.clone(),
            procedure_handle,
        })
    }
}

impl ConnectorTableExecuteHandleMarker for IcebergTableExecuteHandle {
    fn schema_table_name(&self) -> &SchemaTableName {
        &self.schema_table_name
    }
}

/// The exact facts one rewrite-position-delete split carries.
#[derive(Clone, Debug)]
pub struct IcebergRewritePositionDeleteFilesSplitParams {
    pub data_file_path: String,
    pub data_file_size: i64,
    pub partition_spec_id: i32,
    pub partition_data_json: String,
    /// The exact Puffin deletion vectors selected by the frozen artifact.
    pub selected_position_deletes: Vec<IcebergDeleteFile>,
    pub split_weight: SplitWeight,
}

/// One data file plus the deletion vectors whose positions are being
/// re-encoded for it.
#[derive(Clone, Debug)]
pub struct IcebergRewritePositionDeleteFilesSplit {
    data_file_path: Arc<str>,
    data_file_size: i64,
    partition_spec_id: i32,
    partition_data_json: Arc<str>,
    selected_position_deletes: Vec<IcebergDeleteFile>,
    split_weight: SplitWeight,
    retained_size_in_bytes: u64,
}

impl IcebergRewritePositionDeleteFilesSplit {
    pub fn try_new(
        params: IcebergRewritePositionDeleteFilesSplitParams,
    ) -> Result<Self, ConnectorError> {
        let IcebergRewritePositionDeleteFilesSplitParams {
            data_file_path,
            data_file_size,
            partition_spec_id,
            partition_data_json,
            selected_position_deletes,
            split_weight,
        } = params;

        if data_file_path.is_empty() || data_file_path.len() > MAX_PATH_BYTES {
            return Err(invalid(
                "iceberg rewrite data file path must be non-empty and bounded",
            ));
        }
        if data_file_size < 0 {
            return Err(invalid(
                "iceberg rewrite data file size must be nonnegative",
            ));
        }
        if partition_data_json.is_empty() || partition_data_json.len() > MAX_JSON_BYTES {
            return Err(invalid(
                "iceberg rewrite partition data json must be non-empty and bounded",
            ));
        }
        if selected_position_deletes.is_empty() {
            return Err(invalid(
                "iceberg rewrite position delete split requires at least one selected delete",
            ));
        }
        if selected_position_deletes.len() > MAX_DELETES_PER_SPLIT {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg rewrite selected delete count exceeds the hard limit",
            ));
        }
        for delete in &selected_position_deletes {
            // A V2 Parquet position-delete file has no addressed content range,
            // so re-encoding it would mean reading a whole file to find the
            // rows of one data file. This rewrite exists for deletion vectors.
            if delete.content() != IcebergDeleteFileContent::PositionDeletes
                || delete.format() != IcebergFileFormat::Puffin
            {
                return Err(invalid(format!(
                    "iceberg rewrite selected delete {} is not a puffin deletion vector",
                    delete.path()
                )));
            }
            // `IcebergDeleteFile` already ties Puffin to a content range; this
            // re-states the requirement the rewrite itself depends on.
            if delete.content_offset().is_none() || delete.content_size_in_bytes().is_none() {
                return Err(invalid(format!(
                    "iceberg rewrite selected delete {} has no puffin content range",
                    delete.path()
                )));
            }
            if delete.referenced_data_file() != Some(data_file_path.as_str()) {
                return Err(invalid(format!(
                    "iceberg rewrite selected delete {} does not belong to data file {data_file_path}",
                    delete.path()
                )));
            }
        }

        let retained_size_in_bytes = (size_of::<Self>()
            + data_file_path.len()
            + partition_data_json.len()
            + selected_position_deletes
                .iter()
                .map(|delete| size_of::<IcebergDeleteFile>() + delete.path().len())
                .sum::<usize>()) as u64;
        Ok(Self {
            data_file_path: Arc::from(data_file_path.as_str()),
            data_file_size,
            partition_spec_id,
            partition_data_json: Arc::from(partition_data_json.as_str()),
            selected_position_deletes,
            split_weight,
            retained_size_in_bytes,
        })
    }

    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    pub const fn data_file_size(&self) -> i64 {
        self.data_file_size
    }

    pub const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub fn partition_data_json(&self) -> &str {
        &self.partition_data_json
    }

    pub fn selected_position_deletes(&self) -> &[IcebergDeleteFile] {
        &self.selected_position_deletes
    }

    pub fn to_proto(&self) -> dto::IcebergRewritePositionDeleteFilesSplit {
        dto::IcebergRewritePositionDeleteFilesSplit {
            data_file_path: self.data_file_path.to_string(),
            data_file_size: self.data_file_size,
            partition_spec_id: self.partition_spec_id,
            partition_data_json: self.partition_data_json.to_string(),
            selected_position_deletes: self
                .selected_position_deletes
                .iter()
                .map(IcebergDeleteFile::to_proto)
                .collect(),
        }
    }

    pub fn from_proto(
        raw: &dto::IcebergRewritePositionDeleteFilesSplit,
        split_weight: SplitWeight,
    ) -> Result<Self, ConnectorError> {
        let mut selected_position_deletes = Vec::with_capacity(raw.selected_position_deletes.len());
        for delete in &raw.selected_position_deletes {
            selected_position_deletes.push(IcebergDeleteFile::from_proto(delete)?);
        }
        Self::try_new(IcebergRewritePositionDeleteFilesSplitParams {
            data_file_path: raw.data_file_path.clone(),
            data_file_size: raw.data_file_size,
            partition_spec_id: raw.partition_spec_id,
            partition_data_json: raw.partition_data_json.clone(),
            selected_position_deletes,
            split_weight,
        })
    }
}

impl ConnectorSplit for IcebergRewritePositionDeleteFilesSplit {
    fn is_remotely_accessible(&self) -> bool {
        true
    }

    fn addresses(&self) -> &[HostAddress] {
        &[]
    }

    fn affinity_key(&self) -> Option<&str> {
        Some(&self.data_file_path)
    }

    fn split_weight(&self) -> SplitWeight {
        self.split_weight
    }

    fn retained_size_in_bytes(&self) -> u64 {
        self.retained_size_in_bytes
    }
}

fn validate_digest_hex(value: &str, what: &'static str) -> Result<String, ConnectorError> {
    if value.len() != DIGEST_HEX_LEN
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(format!(
            "iceberg rewrite {what} digest must be 64 lowercase hex characters"
        )));
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::super::split::{IcebergDeleteFileParams, tests::position_delete};
    use super::super::table_handle::tests::partitioned_handle;
    use super::*;

    const ARTIFACT_DIGEST: &str =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const GROUP_DIGEST: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    fn artifact() -> IcebergRewriteArtifactContentId {
        IcebergRewriteArtifactContentId::try_new(
            "s3://warehouse/db/t/_rewrite/0199",
            ARTIFACT_DIGEST,
        )
        .expect("artifact content id")
    }

    fn deletion_vector(path: &str) -> IcebergDeleteFile {
        IcebergDeleteFile::try_new(IcebergDeleteFileParams {
            content: IcebergDeleteFileContent::PositionDeletes,
            path: path.to_string(),
            format: IcebergFileFormat::Puffin,
            record_count: 12,
            file_size_in_bytes: 4096,
            equality_field_ids: Vec::new(),
            row_position_lower_bound: Some(0),
            row_position_upper_bound: Some(99),
            data_sequence_number: 9,
            content_offset: Some(64),
            content_size_in_bytes: Some(256),
            referenced_data_file: Some("s3://warehouse/db/t/data/a.parquet".to_string()),
            decryption_data: None,
        })
        .expect("deletion vector")
    }

    fn optimize_handle() -> IcebergTableExecuteHandle {
        IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            procedure_id: IcebergProcedureId::Optimize,
            table_location: "s3://warehouse/db/t".to_string(),
            procedure_handle: Some(IcebergTableExecuteProcedureHandle::Optimize(
                IcebergOptimizeHandle::try_new(partitioned_handle(), 64 * 1024 * 1024)
                    .expect("optimize handle"),
            )),
        })
        .expect("table execute handle")
    }

    fn rewrite_handle() -> IcebergTableExecuteHandle {
        IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            procedure_id: IcebergProcedureId::RewritePositionDeleteFiles,
            table_location: "s3://warehouse/db/t".to_string(),
            procedure_handle: Some(
                IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(
                    IcebergRewritePositionDeleteFilesHandle::try_new(
                        partitioned_handle(),
                        artifact(),
                        GROUP_DIGEST,
                    )
                    .expect("rewrite handle"),
                ),
            ),
        })
        .expect("table execute handle")
    }

    fn rewrite_split(
        deletes: Vec<IcebergDeleteFile>,
    ) -> Result<IcebergRewritePositionDeleteFilesSplit, ConnectorError> {
        IcebergRewritePositionDeleteFilesSplit::try_new(
            IcebergRewritePositionDeleteFilesSplitParams {
                data_file_path: "s3://warehouse/db/t/data/a.parquet".to_string(),
                data_file_size: 8192,
                partition_spec_id: 7,
                partition_data_json: "{}".to_string(),
                selected_position_deletes: deletes,
                split_weight: SplitWeight::STANDARD,
            },
        )
    }

    #[test]
    fn the_procedure_set_is_closed_and_rejects_unknown_and_unspecified() {
        for (raw, expected) in [
            (1, IcebergProcedureId::Optimize),
            (2, IcebergProcedureId::OptimizeManifests),
            (3, IcebergProcedureId::DropExtendedStats),
            (4, IcebergProcedureId::RollbackToSnapshot),
            (5, IcebergProcedureId::ExpireSnapshots),
            (6, IcebergProcedureId::RemoveOrphanFiles),
            (7, IcebergProcedureId::AddFiles),
            (8, IcebergProcedureId::AddFilesFromTable),
            (9, IcebergProcedureId::RewritePositionDeleteFiles),
        ] {
            assert_eq!(
                IcebergProcedureId::from_proto(raw).expect("known procedure"),
                expected
            );
            assert_eq!(expected.to_proto() as i32, raw);
        }
        assert!(IcebergProcedureId::from_proto(0).is_err());
        assert!(IcebergProcedureId::from_proto(10).is_err());
    }

    #[test]
    fn only_optimize_reuses_an_ordinary_data_split() {
        assert_eq!(
            IcebergProcedureId::Optimize.execution(),
            IcebergProcedureExecution::DistributedDataSplits
        );
        assert_eq!(
            IcebergProcedureId::RewritePositionDeleteFiles.execution(),
            IcebergProcedureExecution::DistributedRewritePositionDeleteSplits
        );
        for coordinator in [
            IcebergProcedureId::OptimizeManifests,
            IcebergProcedureId::DropExtendedStats,
            IcebergProcedureId::RollbackToSnapshot,
            IcebergProcedureId::ExpireSnapshots,
            IcebergProcedureId::RemoveOrphanFiles,
            IcebergProcedureId::AddFiles,
            IcebergProcedureId::AddFilesFromTable,
        ] {
            assert_eq!(
                coordinator.execution(),
                IcebergProcedureExecution::Coordinator
            );
            assert!(!coordinator.has_procedure_handle());
        }
        assert_eq!(
            optimize_handle().execution(),
            IcebergProcedureExecution::DistributedDataSplits
        );
        assert_eq!(
            rewrite_handle().execution(),
            IcebergProcedureExecution::DistributedRewritePositionDeleteSplits
        );
    }

    #[test]
    fn the_procedure_handle_is_present_exactly_for_the_two_distributed_procedures() {
        // A coordinator procedure carries none.
        let coordinator = IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            procedure_id: IcebergProcedureId::ExpireSnapshots,
            table_location: "s3://warehouse/db/t".to_string(),
            procedure_handle: None,
        })
        .expect("coordinator handle");
        assert!(coordinator.procedure_handle().is_none());

        // Attaching one to a coordinator procedure is a planning error.
        assert!(
            IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
                schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
                procedure_id: IcebergProcedureId::ExpireSnapshots,
                table_location: "s3://warehouse/db/t".to_string(),
                procedure_handle: Some(IcebergTableExecuteProcedureHandle::Optimize(
                    IcebergOptimizeHandle::try_new(partitioned_handle(), 1).expect("optimize"),
                )),
            })
            .is_err()
        );
        // So is omitting one from a distributed procedure.
        assert!(
            IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
                schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
                procedure_id: IcebergProcedureId::Optimize,
                table_location: "s3://warehouse/db/t".to_string(),
                procedure_handle: None,
            })
            .is_err()
        );
        // And so is an id that names a different procedure than the handle.
        assert!(
            IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
                schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
                procedure_id: IcebergProcedureId::Optimize,
                table_location: "s3://warehouse/db/t".to_string(),
                procedure_handle: Some(
                    IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(
                        IcebergRewritePositionDeleteFilesHandle::try_new(
                            partitioned_handle(),
                            artifact(),
                            GROUP_DIGEST,
                        )
                        .expect("rewrite handle"),
                    ),
                ),
            })
            .is_err()
        );
    }

    #[test]
    fn table_execute_handles_round_trip_through_the_private_wire_value() {
        for handle in [optimize_handle(), rewrite_handle()] {
            let decoded =
                IcebergTableExecuteHandle::from_proto(&handle.to_proto()).expect("decoded handle");
            assert_eq!(decoded, handle);
        }

        let coordinator = IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            procedure_id: IcebergProcedureId::RemoveOrphanFiles,
            table_location: "s3://warehouse/db/t".to_string(),
            procedure_handle: None,
        })
        .expect("coordinator handle");
        let decoded =
            IcebergTableExecuteHandle::from_proto(&coordinator.to_proto()).expect("decoded handle");
        assert_eq!(decoded, coordinator);
        assert!(decoded.procedure_handle().is_none());
    }

    #[test]
    fn an_artifact_content_id_names_exact_immutable_external_content() {
        assert_eq!(artifact().artifact_digest_hex(), ARTIFACT_DIGEST);
        assert_eq!(
            artifact().artifact_location(),
            "s3://warehouse/db/t/_rewrite/0199"
        );
        // A directory is not an object, an uppercase digest is a different
        // rendering, and a short digest is not a SHA-256 at all.
        assert!(
            IcebergRewriteArtifactContentId::try_new(
                "s3://warehouse/db/t/_rewrite/",
                ARTIFACT_DIGEST
            )
            .is_err()
        );
        assert!(
            IcebergRewriteArtifactContentId::try_new("s3://a", ARTIFACT_DIGEST.to_uppercase())
                .is_err()
        );
        assert!(IcebergRewriteArtifactContentId::try_new("s3://a", "abcd").is_err());
        assert!(IcebergRewriteArtifactContentId::try_new("", ARTIFACT_DIGEST).is_err());
        assert!(
            IcebergRewritePositionDeleteFilesHandle::try_new(
                partitioned_handle(),
                artifact(),
                "not-hex"
            )
            .is_err()
        );
    }

    #[test]
    fn a_rewrite_split_accepts_only_puffin_deletion_vectors() {
        let split = rewrite_split(vec![deletion_vector("dv.puffin")]).expect("split");
        assert_eq!(split.selected_position_deletes().len(), 1);
        assert_eq!(
            split.selected_position_deletes()[0].content_offset(),
            Some(64)
        );

        // A V2 Parquet position-delete file has no addressed content range, so
        // it is not something this rewrite can re-encode.
        let error =
            rewrite_split(vec![position_delete("d.parquet")]).expect_err("parquet position delete");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        // An equality delete deletes by value, not by position.
        let error = rewrite_split(vec![super::super::split::tests::equality_delete(
            "e.parquet",
            2,
        )])
        .expect_err("equality delete");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        let split = rewrite_split(vec![deletion_vector("dv.puffin")]).expect("split");
        let mut missing_identity = split.to_proto();
        missing_identity.selected_position_deletes[0].referenced_data_file = None;
        assert!(
            IcebergRewritePositionDeleteFilesSplit::from_proto(
                &missing_identity,
                SplitWeight::STANDARD,
            )
            .is_err()
        );
        let mut foreign_identity = split.to_proto();
        foreign_identity.selected_position_deletes[0].referenced_data_file =
            Some("s3://warehouse/db/t/data/other.parquet".to_string());
        assert!(
            IcebergRewritePositionDeleteFilesSplit::from_proto(
                &foreign_identity,
                SplitWeight::STANDARD,
            )
            .is_err()
        );
        // And a split with nothing selected has no work to do.
        assert!(rewrite_split(Vec::new()).is_err());
    }

    #[test]
    fn the_rewrite_page_source_emits_exactly_the_data_file_and_the_position() {
        assert_eq!(
            REWRITE_POSITION_DELETE_OUTPUT_COLUMNS.map(|(name, _)| name),
            ["file_path", "pos"]
        );
    }

    #[test]
    fn a_rewrite_split_round_trips_and_is_keyed_by_its_data_file() {
        let split = rewrite_split(vec![deletion_vector("dv.puffin")]).expect("split");
        assert_eq!(
            ConnectorSplit::affinity_key(&split),
            Some("s3://warehouse/db/t/data/a.parquet")
        );
        assert!(ConnectorSplit::retained_size_in_bytes(&split) > 0);

        let raw = split.to_proto();
        let decoded = IcebergRewritePositionDeleteFilesSplit::from_proto(
            &raw,
            ConnectorSplit::split_weight(&split),
        )
        .expect("decoded split");
        assert_eq!(decoded.to_proto(), split.to_proto());
        assert_eq!(decoded.data_file_path(), split.data_file_path());
    }

    #[test]
    fn a_distributed_procedure_requires_a_pinned_snapshot() {
        let unpinned = {
            let schema = super::super::table_handle::tests::partitioned_schema();
            let spec = super::super::table_handle::tests::identity_partition_spec(&schema);
            let mut params =
                super::super::table_handle::tests::table_handle_params(&schema, Some(&spec));
            params.snapshot_id = None;
            IcebergTableHandle::try_new(params).expect("handle")
        };
        assert!(IcebergOptimizeHandle::try_new(unpinned.clone(), 1).is_err());
        assert!(
            IcebergRewritePositionDeleteFilesHandle::try_new(unpinned, artifact(), GROUP_DIGEST)
                .is_err()
        );
    }
}
