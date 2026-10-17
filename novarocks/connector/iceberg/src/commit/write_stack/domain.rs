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

//! Iceberg's concrete write domain for the provider-neutral write stack.
//!
//! Three values live here, and each one owns its cross-field validation in its
//! constructor so an invalid value cannot be built at all:
//!
//! * [`IcebergCommitHandle`] is the frontend-only write session. It carries the
//!   sealed logical target set, the frozen table/ref/base facts, and the single
//!   terminal state a commit may reach.
//! * [`IcebergWriterHandle`] is one logical write recipe. It carries table
//!   format facts and — for a delete branch — *exact references* to the old
//!   delete artifacts a writer must re-read. It never carries a credential and
//!   never carries an already-read bulk object such as a materialized deletion
//!   vector.
//! * [`IcebergCommitFragment`] describes exactly one artifact: one written data
//!   file, one position-delete file, or one deletion vector. It is not a report
//!   document and carries no writer identity.

use crate::commit::model::EntryIdentity;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use novarocks_spi::connector::write_stack::{ConnectorManagedPublicationShape, WriteTargetOrdinal};
use novarocks_spi::connector::{
    ConnectorDocumentPublicationDeclaration, ConnectorError, ConnectorErrorKind,
    ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationTechnique,
    ConnectorWriteAdmissionPurpose, ConnectorWriteIntent,
};
use parquet::basic::Compression;

use crate::commit::CommitOpKind;
use crate::commit::report::IcebergColumnStats;
use crate::commit::write_stack::copy_on_write::IcebergCowBranchInput;
use crate::commit::write_stack::old_delete::IcebergOldDeleteMergeTarget;
use crate::delete_file::IcebergFileFormat;
use crate::write_descriptor::IcebergPartitionDescriptor;

pub(crate) fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

pub(crate) fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

/// Reject a value that is empty, carries a NUL byte, or embeds a credential.
///
/// A handle, a fragment, and an old-delete reference are all values the
/// frontend hands to a backend, so a location that smuggles a secret through a
/// URL userinfo or query parameter must never be constructible.
pub(crate) fn validate_location(subject: &str, value: &str) -> Result<(), ConnectorError> {
    if value.is_empty() {
        return Err(invalid(format!("Iceberg {subject} must not be empty")));
    }
    if value.contains('\0') {
        return Err(invalid(format!("Iceberg {subject} contains a NUL byte")));
    }
    if let Ok(url) = url::Url::parse(value) {
        if !url.username().is_empty() || url.password().is_some() {
            return Err(invalid(format!(
                "Iceberg {subject} must not embed credentials"
            )));
        }
        for (key, _) in url.query_pairs() {
            if [
                "access_key",
                "access_key_id",
                "secret",
                "secret_key",
                "session_token",
                "token",
            ]
            .iter()
            .any(|candidate| key.eq_ignore_ascii_case(candidate))
            {
                return Err(invalid(format!(
                    "Iceberg {subject} must not embed credentials"
                )));
            }
        }
    }
    Ok(())
}

/// Which physical branch one logical write target drives.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergWriteBranch {
    /// New data files, optionally carrying Iceberg row lineage.
    Data,
    /// Parquet position-delete files.
    PositionDelete,
    /// Puffin deletion vectors, one blob per referenced data file.
    DeletionVector,
    /// Parquet equality-delete files, matched on column values rather than on
    /// row position.
    EqualityDelete,
}

impl IcebergWriteBranch {
    /// Whether this branch supersedes the pre-existing delete artifacts of the
    /// data files it writes for, and therefore owns their old-delete merge.
    ///
    /// An equality delete does not: it appends a delete file that names no data
    /// file, retires nothing, and merges nothing. Reporting it here would give
    /// it an old-delete merge it has no data file to perform.
    pub const fn writes_deletes(self) -> bool {
        matches!(self, Self::PositionDelete | Self::DeletionVector)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::PositionDelete => "position-delete",
            Self::DeletionVector => "deletion-vector",
            Self::EqualityDelete => "equality-delete",
        }
    }
}

/// Every SQL-level write shape Iceberg supports, mapped onto one commit op.
///
/// This enum is provider-private on purpose: the neutral write stack never
/// learns that Iceberg distinguishes a dynamic partition overwrite from a
/// static one, or a copy-on-write mutation from a merge-on-read one.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergWriteFlavor {
    /// `INSERT INTO`.
    Append,
    /// `INSERT OVERWRITE` replacing the whole target.
    Overwrite,
    /// `INSERT OVERWRITE` replacing only the touched partitions.
    PartitionOverwrite,
    /// Merge-on-read row mutation staging Parquet position deletes.
    RowMutationPositionDelete,
    /// Merge-on-read row mutation staging Puffin deletion vectors.
    RowMutationDeletionVector,
    /// `ALTER TABLE ... ADD EQUALITY DELETE`: append Parquet equality-delete
    /// files matched on column values.
    EqualityDelete,
    /// Copy-on-write row mutation rewriting whole data files.
    RowMutationCopyOnWrite,
    /// `CREATE TABLE AS SELECT` publishing into a staged table.
    StagedCreate,
    /// Materialized-view refresh publication.
    ManagedPublication,
    /// Distributed compaction / rewrite of a frozen data file set.
    DistributedRewrite,
    /// Distributed repack of a frozen position-delete artifact set.
    ///
    /// It is a separate flavor from [`Self::DistributedRewrite`] rather than a
    /// flag on it because the two seal different physical branches: this one
    /// republishes deletion vectors and never writes a data file, and the other
    /// republishes data files and never writes a delete artifact. Keeping them
    /// apart is what lets `allowed_session_branches` refuse each one's branch on
    /// the other.
    DistributedRewritePositionDeletes,
    /// Provider-owned maintenance such as `TRUNCATE`.
    TableMaintenance,
}

impl IcebergWriteFlavor {
    /// Preserve the admitted operation family through catalog publication.
    pub(crate) const fn catalog_operation(self) -> crate::catalog::admission::CatalogOperation {
        use crate::catalog::admission::CatalogOperation;
        match self {
            Self::Append => CatalogOperation::Append,
            Self::Overwrite | Self::PartitionOverwrite => CatalogOperation::Overwrite,
            Self::RowMutationPositionDelete | Self::RowMutationDeletionVector => {
                CatalogOperation::RowMutation
            }
            Self::EqualityDelete => CatalogOperation::RowDelta,
            Self::RowMutationCopyOnWrite => CatalogOperation::CopyOnWrite,
            Self::StagedCreate => CatalogOperation::CreateTable(
                crate::catalog::CatalogCreateIntent::CreateTableAsSelect,
            ),
            Self::ManagedPublication => CatalogOperation::PublishDocuments,
            Self::DistributedRewrite => CatalogOperation::RewriteDataFiles,
            Self::DistributedRewritePositionDeletes => CatalogOperation::RewritePositionDeletes,
            Self::TableMaintenance => CatalogOperation::Truncate,
        }
    }

    /// The single external commit op this flavor performs.
    ///
    /// The mapping is the one the provider already commits today: a
    /// deletion-vector mutation lands on `RowDeltaDvFromFiles` because a Puffin
    /// artifact makes the delta file-based, and a distributed rewrite lands on
    /// `SelectedRewrite` because its input file set is frozen at planning.
    pub const fn commit_op_kind(self) -> CommitOpKind {
        match self {
            Self::Append | Self::StagedCreate | Self::ManagedPublication => {
                CommitOpKind::FastAppend
            }
            Self::Overwrite => CommitOpKind::Overwrite,
            Self::PartitionOverwrite => CommitOpKind::OverwritePartitions,
            Self::RowMutationPositionDelete => CommitOpKind::RowDelta,
            Self::RowMutationDeletionVector => CommitOpKind::RowDeltaDvFromFiles,
            // The same op a Parquet position-delete row mutation commits: the
            // delta is file-based only when the artifacts are Puffin, and an
            // equality delete's are not.
            Self::EqualityDelete => CommitOpKind::RowDelta,
            Self::RowMutationCopyOnWrite => CommitOpKind::CowUpdate,
            // Both rewrites commit as one selected replacement of a frozen
            // input set; what differs is which half of that set they replace,
            // and the commit reads that off the session's flavor.
            Self::DistributedRewrite | Self::DistributedRewritePositionDeletes => {
                CommitOpKind::SelectedRewrite
            }
            Self::TableMaintenance => CommitOpKind::Truncate,
        }
    }

    /// Whether this flavor is one of the two distributed rewrites.
    ///
    /// The two share every session-level rule but their branch set: both freeze
    /// one input per branch, both may repeat a branch, both skip the external
    /// write fence, and both terminate an empty prepared set without a commit.
    pub const fn is_distributed_rewrite(self) -> bool {
        matches!(
            self,
            Self::DistributedRewrite | Self::DistributedRewritePositionDeletes
        )
    }

    /// The frozen input half a rewrite of this flavor replaces.
    ///
    /// `None` on every flavor that is not a rewrite. It is read off the flavor
    /// rather than off the staged artifacts, because a branch whose writer
    /// produced nothing must still retire its frozen inputs.
    pub(crate) const fn selected_rewrite_kind(
        self,
    ) -> Option<crate::commit::selected_rewrite::SelectedRewriteKind> {
        use crate::commit::selected_rewrite::SelectedRewriteKind;
        match self {
            Self::DistributedRewrite => Some(SelectedRewriteKind::Data),
            Self::DistributedRewritePositionDeletes => Some(SelectedRewriteKind::PositionDeletes),
            _ => None,
        }
    }

    /// The physical branches this flavor may seal as logical targets, in the
    /// exact order their ordinals are assigned.
    pub fn branches(self) -> &'static [IcebergWriteBranch] {
        match self {
            Self::Append
            | Self::Overwrite
            | Self::PartitionOverwrite
            | Self::RowMutationCopyOnWrite
            | Self::StagedCreate
            | Self::ManagedPublication
            | Self::DistributedRewrite
            | Self::TableMaintenance => &[IcebergWriteBranch::Data],
            Self::RowMutationPositionDelete => {
                &[IcebergWriteBranch::Data, IcebergWriteBranch::PositionDelete]
            }
            Self::RowMutationDeletionVector => {
                &[IcebergWriteBranch::Data, IcebergWriteBranch::DeletionVector]
            }
            // An equality delete writes delete files and nothing else. Sealing
            // a data branch beside it would give SQL somewhere to send rows the
            // statement never produces.
            Self::EqualityDelete => &[IcebergWriteBranch::EqualityDelete],
            // A position-delete rewrite reads old deletion vectors and writes
            // replacements for them; it never produces a table row, so sealing
            // a data branch beside it would give SQL somewhere to send rows the
            // operation never produces.
            Self::DistributedRewritePositionDeletes => &[IcebergWriteBranch::DeletionVector],
        }
    }

    /// Whether this flavor may reach `finish_write` with no fragment at all.
    ///
    /// Every flavor may: a zero-row `INSERT` still commits an empty snapshot,
    /// and a mutation matching no row still needs its session terminated. The
    /// method exists to make that decision explicit rather than incidental.
    pub const fn accepts_empty_prepared_set(self) -> bool {
        true
    }

    /// Whether one sealed session may seal the same physical branch more than
    /// once.
    ///
    /// Only the two distributed rewrites and a copy-on-write mutation may.
    /// Their logical targets are all one branch kind — one per frozen rewrite
    /// group, one per rewritten data file — and the ordinal, not the branch, is
    /// what tells them apart. Every other flavor keeps the stricter rule,
    /// because a repeated delete branch would make the old-delete merge owner
    /// ambiguous. A position-delete rewrite repeats one and stays unambiguous
    /// for a different reason: `prove_unique_delete_owner` still runs, and its
    /// groups own disjoint data files, so no two branches claim one file.
    pub const fn seals_one_target_per_branch(self) -> bool {
        !matches!(
            self,
            Self::DistributedRewrite
                | Self::DistributedRewritePositionDeletes
                | Self::RowMutationCopyOnWrite
        )
    }

    /// Whether this flavor's write must be serialized behind the distributed
    /// external write fence.
    ///
    /// A distributed rewrite must not be: it is arbitrated by the ordinary
    /// Iceberg base-state compare and swap that the frozen publication runner already
    /// performs, and taking the fence would serialize it against ordinary DML
    /// it does not conflict with. Every other flavor keeps the fence, because a
    /// DML write that skipped it would lose that protection.
    pub const fn requires_external_write_fence(self) -> bool {
        !self.is_distributed_rewrite()
    }
}

/// The application-document publication declaration frozen by `begin_write`.
///
/// The application documents are the durable truth and
/// are supplied only to `finish_write`, while this declaration keeps the
/// admission-time target, base, technique, shape, and repartition decision
/// available for exact validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergDocumentPublicationFacts {
    declaration: ConnectorDocumentPublicationDeclaration,
    shape: ConnectorManagedPublicationShape,
}

impl IcebergDocumentPublicationFacts {
    pub const fn new(
        declaration: ConnectorDocumentPublicationDeclaration,
        shape: ConnectorManagedPublicationShape,
    ) -> Self {
        Self { declaration, shape }
    }

    pub const fn declaration(&self) -> &ConnectorDocumentPublicationDeclaration {
        &self.declaration
    }

    pub const fn shape(&self) -> ConnectorManagedPublicationShape {
        self.shape
    }

    pub const fn technique(&self) -> ConnectorManagedPublicationTechnique {
        self.declaration.technique()
    }

    pub const fn empty_input(&self) -> ConnectorManagedPublicationEmptyInputDisposition {
        self.declaration.empty_input()
    }

    pub const fn connector_intent(&self) -> ConnectorWriteIntent {
        match self.technique() {
            ConnectorManagedPublicationTechnique::Full => ConnectorWriteIntent::Overwrite,
            ConnectorManagedPublicationTechnique::Incremental
            | ConnectorManagedPublicationTechnique::MetadataOnly => ConnectorWriteIntent::Append,
        }
    }

    pub const fn commit_op_kind(&self, delete_branch: Option<IcebergWriteBranch>) -> CommitOpKind {
        publication_commit_op_kind(self.technique(), delete_branch)
    }
}

const fn publication_commit_op_kind(
    technique: ConnectorManagedPublicationTechnique,
    delete_branch: Option<IcebergWriteBranch>,
) -> CommitOpKind {
    match (technique, delete_branch) {
        // A full refresh never seals a delete branch:
        // `plan_document_publication_branches` refuses a full refresh with a
        // change-stream shape, because a commit that replaces every live
        // row has no prior row for a change event to supersede.
        (ConnectorManagedPublicationTechnique::Full, _) => CommitOpKind::Overwrite,
        // A metadata-only publication writes no files and seals no delete
        // branch: it exists to move the watermark, and the rows it publishes
        // are the ones already there.
        (ConnectorManagedPublicationTechnique::MetadataOnly, _) => CommitOpKind::FastAppend,
        (ConnectorManagedPublicationTechnique::Incremental, None)
        | (ConnectorManagedPublicationTechnique::Incremental, Some(IcebergWriteBranch::Data)) => {
            CommitOpKind::FastAppend
        }
        (
            ConnectorManagedPublicationTechnique::Incremental,
            Some(IcebergWriteBranch::DeletionVector),
        ) => IcebergWriteFlavor::RowMutationDeletionVector.commit_op_kind(),
        (
            ConnectorManagedPublicationTechnique::Incremental,
            Some(IcebergWriteBranch::PositionDelete),
        ) => IcebergWriteFlavor::RowMutationPositionDelete.commit_op_kind(),
        // `sealed_delete_branch` only ever reports a branch that supersedes
        // the deletes of a data file, and an equality delete does not; a
        // publication cannot seal one either, because
        // `allowed_session_branches` never offers it.
        (
            ConnectorManagedPublicationTechnique::Incremental,
            Some(IcebergWriteBranch::EqualityDelete),
        ) => IcebergWriteFlavor::EqualityDelete.commit_op_kind(),
    }
}

/// The physical branches one sealed session may own.
///
/// Every flavor but one answers this from itself. A managed publication cannot:
/// its branch structure follows the shape its caller declared, so a `Data`
/// publication owns only its data branch while a `RowMutation` one owns the
/// branches a row mutation needs. Deriving it from the sealed publication facts
/// keeps the planner's check and the handle's reading the same source, instead
/// of a second provider flavor that would say the same thing twice.
///
/// A row-mutation publication is admitted for the union of the delete branches,
/// not for one of them: which delete branch it seals is the branch planner's
/// decision from the signed input, and the sealed session still refuses to
/// repeat a branch, so the union cannot admit two delete owners.
pub fn allowed_session_branches(
    flavor: IcebergWriteFlavor,
    publication_shape: Option<ConnectorManagedPublicationShape>,
) -> &'static [IcebergWriteBranch] {
    const ROW_MUTATION_BRANCHES: &[IcebergWriteBranch] = &[
        IcebergWriteBranch::Data,
        IcebergWriteBranch::PositionDelete,
        IcebergWriteBranch::DeletionVector,
    ];
    match publication_shape {
        Some(ConnectorManagedPublicationShape::RowMutation) => ROW_MUTATION_BRANCHES,
        _ => flavor.branches(),
    }
}

/// The exact input files one distributed-rewrite branch replaces.
///
/// A rewrite names its inputs at planning time: the frozen base snapshot says
/// which data files and which attached delete artifacts the new files supersede.
/// Freezing them here is what keeps the commit's replaced set a planning fact
/// instead of something recovered from whatever the writers happened to produce
/// — a writer that produced nothing for its group must still retire that
/// group's inputs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IcebergFrozenRewriteBranchInput {
    data_paths: BTreeSet<EntryIdentity>,
    delete_paths: BTreeSet<EntryIdentity>,
}

impl IcebergFrozenRewriteBranchInput {
    /// An empty input is admissible: a rewrite that found nothing to rewrite
    /// still seals one branch so its session can terminate. An empty *path* is
    /// not, because it would name no file while claiming to replace one.
    pub fn try_new(
        data_paths: BTreeSet<EntryIdentity>,
        delete_paths: BTreeSet<EntryIdentity>,
    ) -> Result<Self, ConnectorError> {
        for path in data_paths.iter().chain(&delete_paths) {
            path.validate()
                .map_err(|error| invalid(error.to_string()))?;
            validate_location("rewrite input path", path.path())?;
        }
        if data_paths
            .iter()
            .any(|entry| !matches!(entry, EntryIdentity::DataFile { .. }))
            || delete_paths
                .iter()
                .any(|entry| matches!(entry, EntryIdentity::DataFile { .. }))
            || data_paths.iter().any(|data| {
                delete_paths
                    .iter()
                    .any(|delete| data.path() == delete.path())
            })
        {
            return Err(invalid(
                "Iceberg rewrite branch input names one path as both data and delete",
            ));
        }
        Ok(Self {
            data_paths,
            delete_paths,
        })
    }

    pub const fn data_paths(&self) -> &BTreeSet<EntryIdentity> {
        &self.data_paths
    }

    pub const fn delete_paths(&self) -> &BTreeSet<EntryIdentity> {
        &self.delete_paths
    }

    pub fn is_empty(&self) -> bool {
        self.data_paths.is_empty() && self.delete_paths.is_empty()
    }
}

/// What an *empty* prepared write set means for one sealed session.
///
/// The decision stays inside the provider on purpose. A generic frontend that
/// guessed from "there were no fragments" would have to invent a policy for a
/// publication that asked to abort, and would turn a rewrite with nothing to
/// rewrite into a failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergEmptyWriteDecision {
    /// Publish the empty write as an ordinary snapshot. This is what a zero-row
    /// `INSERT` has always done.
    Commit,
    /// Terminate the session with no external commit at all. The target ref
    /// keeps the head the session was frozen against.
    SkipExternalCommit,
}

impl IcebergWriteFlavor {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Overwrite => "overwrite",
            Self::PartitionOverwrite => "partition-overwrite",
            Self::RowMutationPositionDelete => "row-mutation-position-delete",
            Self::RowMutationDeletionVector => "row-mutation-deletion-vector",
            Self::RowMutationCopyOnWrite => "row-mutation-copy-on-write",
            Self::StagedCreate => "staged-create",
            Self::ManagedPublication => "managed-publication",
            Self::DistributedRewrite => "distributed-rewrite",
            Self::DistributedRewritePositionDeletes => "distributed-rewrite-position-deletes",
            Self::TableMaintenance => "table-maintenance",
            Self::EqualityDelete => "equality-delete",
        }
    }

    /// The neutral write intent this flavor presents to admission.
    pub const fn connector_intent(self) -> ConnectorWriteIntent {
        match self {
            Self::Append | Self::StagedCreate | Self::ManagedPublication => {
                ConnectorWriteIntent::Append
            }
            Self::Overwrite | Self::DistributedRewrite | Self::TableMaintenance => {
                ConnectorWriteIntent::Overwrite
            }
            Self::PartitionOverwrite => ConnectorWriteIntent::PartitionOverwrite,
            // A position-delete rewrite stages delete artifacts, so it presents
            // the same neutral intent every other delete-staging write does.
            Self::RowMutationPositionDelete
            | Self::RowMutationDeletionVector
            | Self::RowMutationCopyOnWrite
            | Self::DistributedRewritePositionDeletes
            | Self::EqualityDelete => ConnectorWriteIntent::RowDelta,
        }
    }
}

/// The exact table generation one write session is frozen against.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergWriteTableFacts {
    table_uuid: String,
    namespace: String,
    table_name: String,
    table_location: String,
    data_location: String,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: i64,
    schema_id: i32,
    default_partition_spec_id: i32,
    format_version: u8,
}

impl IcebergWriteTableFacts {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        table_uuid: String,
        namespace: String,
        table_name: String,
        table_location: String,
        data_location: String,
        target_ref: String,
        base_snapshot_id: Option<i64>,
        base_sequence_number: i64,
        schema_id: i32,
        default_partition_spec_id: i32,
        format_version: u8,
    ) -> Result<Self, ConnectorError> {
        if table_uuid.is_empty() {
            return Err(invalid("Iceberg write session requires a table UUID"));
        }
        if namespace.is_empty() || table_name.is_empty() {
            return Err(invalid(
                "Iceberg write session requires a namespace and a table name",
            ));
        }
        if target_ref.is_empty() || target_ref.chars().any(char::is_control) {
            return Err(invalid(
                "Iceberg write session target ref must be non-empty and control-free",
            ));
        }
        validate_location("table location", &table_location)?;
        validate_location("data location", &data_location)?;
        if base_sequence_number < 0 {
            return Err(invalid(
                "Iceberg write session base sequence number must not be negative",
            ));
        }
        if !(1..=3).contains(&format_version) {
            return Err(invalid(
                "Iceberg write session format version must be 1, 2, or 3",
            ));
        }
        Ok(Self {
            table_uuid,
            namespace,
            table_name,
            table_location,
            data_location,
            target_ref,
            base_snapshot_id,
            base_sequence_number,
            schema_id,
            default_partition_spec_id,
            format_version,
        })
    }

    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
    pub fn table_location(&self) -> &str {
        &self.table_location
    }
    pub fn data_location(&self) -> &str {
        &self.data_location
    }
    pub fn target_ref(&self) -> &str {
        &self.target_ref
    }
    pub const fn base_snapshot_id(&self) -> Option<i64> {
        self.base_snapshot_id
    }
    pub const fn base_sequence_number(&self) -> i64 {
        self.base_sequence_number
    }
    pub const fn schema_id(&self) -> i32 {
        self.schema_id
    }
    pub const fn default_partition_spec_id(&self) -> i32 {
        self.default_partition_spec_id
    }
    pub const fn format_version(&self) -> u8 {
        self.format_version
    }
}

/// Physical output settings frozen for one logical writer.
#[derive(Clone, Debug)]
pub struct IcebergWriterOutput {
    file_format: IcebergFileFormat,
    compression: Compression,
    parquet_row_group_size_bytes: Option<u64>,
}

impl IcebergWriterOutput {
    pub fn try_new(
        file_format: IcebergFileFormat,
        compression: Compression,
        parquet_row_group_size_bytes: Option<u64>,
    ) -> Result<Self, ConnectorError> {
        if file_format == IcebergFileFormat::Unknown {
            return Err(invalid(
                "Iceberg writer output requires an exact file format",
            ));
        }
        if let Some(size) = parquet_row_group_size_bytes
            && (size == 0 || file_format != IcebergFileFormat::Parquet)
        {
            return Err(invalid(
                "Iceberg Parquet row group size requires a positive value on a Parquet writer",
            ));
        }
        Ok(Self {
            file_format,
            compression,
            parquet_row_group_size_bytes,
        })
    }

    pub const fn file_format(&self) -> IcebergFileFormat {
        self.file_format
    }
    pub const fn compression(&self) -> Compression {
        self.compression
    }
    pub const fn parquet_row_group_size_bytes(&self) -> Option<u64> {
        self.parquet_row_group_size_bytes
    }
}

/// Complete provider schema JSON; its allocation retains the original COW holder.
#[derive(Clone, Debug)]
pub(crate) enum IcebergDataSchema {
    Owned(String),
    CowShared(Arc<IcebergCowTargetSchemaBacking>),
}
#[derive(Debug)]
pub(crate) struct IcebergCowTargetSchemaBacking {
    schema: String,
    _original: novarocks_spi::connector::ConnectorPayloadRetentionGuard,
}
impl IcebergCowTargetSchemaBacking {
    pub(crate) fn from_checked(
        schema: String,
        original: novarocks_spi::connector::ConnectorPayloadRetentionGuard,
    ) -> Arc<Self> {
        Arc::new(Self {
            schema,
            _original: original,
        })
    }
}
impl IcebergDataSchema {
    pub(crate) fn schema(&self) -> &str {
        match self {
            Self::Owned(schema) => schema,
            Self::CowShared(owner) => &owner.schema,
        }
    }
}

/// Complete frozen target schema and partition facts for the data branch.
#[derive(Clone, Debug)]
pub struct IcebergDataBranchRecipe {
    target_schema: Option<IcebergDataSchema>,
    partition_source_column_names: Vec<String>,
    partition_column_names: Vec<String>,
    transform_exprs: Vec<String>,
    /// Descriptive only. Input lineage columns select the reserved definitions.
    row_lineage: bool,
    _cow_original: Option<novarocks_spi::connector::ConnectorPayloadRetentionGuard>,
}
impl IcebergDataBranchRecipe {
    pub fn try_new(
        target_schema: Option<String>,
        partition_source_column_names: Vec<String>,
        partition_column_names: Vec<String>,
        transform_exprs: Vec<String>,
        row_lineage: bool,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_schema_backing(
            target_schema.map(IcebergDataSchema::Owned),
            partition_source_column_names,
            partition_column_names,
            transform_exprs,
            row_lineage,
        )
    }
    pub(crate) fn try_new_cow(
        target_schema: Arc<IcebergCowTargetSchemaBacking>,
        partition_source_column_names: Vec<String>,
        partition_column_names: Vec<String>,
        transform_exprs: Vec<String>,
        row_lineage: bool,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_schema_backing(
            Some(IcebergDataSchema::CowShared(target_schema)),
            partition_source_column_names,
            partition_column_names,
            transform_exprs,
            row_lineage,
        )
    }
    fn try_new_schema_backing(
        target_schema: Option<IcebergDataSchema>,
        partition_source_column_names: Vec<String>,
        partition_column_names: Vec<String>,
        transform_exprs: Vec<String>,
        row_lineage: bool,
    ) -> Result<Self, ConnectorError> {
        if partition_source_column_names.len() != partition_column_names.len()
            || partition_column_names.len() != transform_exprs.len()
        {
            return Err(invalid(
                "Iceberg data writer partition sources, names, and transforms must be parallel",
            ));
        }
        for transform in &transform_exprs {
            validate_location("partition transform", transform)?;
        }
        let unique = partition_column_names.iter().collect::<BTreeSet<_>>();
        if unique.len() != partition_column_names.len() {
            return Err(invalid(
                "Iceberg data writer repeats a partition column name",
            ));
        }
        let original = match &target_schema {
            Some(IcebergDataSchema::CowShared(owner)) => Some(owner._original.clone()),
            _ => None,
        };
        Ok(Self {
            target_schema,
            partition_source_column_names,
            partition_column_names,
            transform_exprs,
            row_lineage,
            _cow_original: original,
        })
    }
    pub fn target_schema_json(&self) -> Option<&str> {
        self.target_schema.as_ref().map(IcebergDataSchema::schema)
    }
    pub fn partition_source_column_names(&self) -> &[String] {
        &self.partition_source_column_names
    }
    pub fn partition_column_names(&self) -> &[String] {
        &self.partition_column_names
    }
    pub fn transform_exprs(&self) -> &[String] {
        &self.transform_exprs
    }
    pub const fn row_lineage(&self) -> bool {
        self.row_lineage
    }
}

/// One logical write recipe, copied to every physical writer placement.
///
/// A delete branch carries `old_deletes`: exact, bounded *references* to the
/// artifacts a writer must re-read through its own query-leased storage. The
/// FE deliberately does not read them.
#[derive(Clone, Debug)]
pub struct IcebergWriterHandle {
    branch: IcebergWriteBranch,
    table: IcebergWriteTableFacts,
    output: IcebergWriterOutput,
    data: Option<IcebergDataBranchRecipe>,
    old_deletes: BTreeMap<String, IcebergOldDeleteMergeTarget>,
    equality: Option<IcebergEqualityDeleteRecipe>,
}

impl IcebergWriterHandle {
    /// Build the data branch's recipe.
    pub fn try_new_data(
        table: IcebergWriteTableFacts,
        output: IcebergWriterOutput,
        data: IcebergDataBranchRecipe,
    ) -> Result<Self, ConnectorError> {
        if output.file_format() != IcebergFileFormat::Parquet {
            return Err(invalid("Iceberg data writer must produce Parquet"));
        }
        Ok(Self {
            branch: IcebergWriteBranch::Data,
            table,
            output,
            data: Some(data),
            old_deletes: BTreeMap::new(),
            equality: None,
        })
    }

    /// Build the equality-delete branch's recipe.
    ///
    /// It freezes no old-delete reference and needs no base snapshot: the file
    /// it writes names no data file and supersedes no existing artifact, so
    /// there is nothing for it to merge.
    pub fn try_new_equality_delete(
        table: IcebergWriteTableFacts,
        output: IcebergWriterOutput,
        equality: IcebergEqualityDeleteRecipe,
    ) -> Result<Self, ConnectorError> {
        if output.file_format() != IcebergFileFormat::Parquet {
            return Err(invalid(
                "Iceberg equality-delete writer must produce Parquet",
            ));
        }
        Ok(Self {
            branch: IcebergWriteBranch::EqualityDelete,
            table,
            output,
            data: None,
            old_deletes: BTreeMap::new(),
            equality: Some(equality),
        })
    }

    /// Build a delete branch's recipe from its frozen old-delete references.
    ///
    /// Every merge target must name the data file it keys, and a deletion
    /// vector branch must write Puffin while a position-delete branch must
    /// write Parquet. Both rules are structural, so neither can be bypassed by
    /// a caller assembling the parts by hand.
    pub fn try_new_delete(
        branch: IcebergWriteBranch,
        table: IcebergWriteTableFacts,
        output: IcebergWriterOutput,
        old_deletes: Vec<IcebergOldDeleteMergeTarget>,
    ) -> Result<Self, ConnectorError> {
        let expected_format = match branch {
            IcebergWriteBranch::PositionDelete => IcebergFileFormat::Parquet,
            IcebergWriteBranch::DeletionVector => IcebergFileFormat::Puffin,
            IcebergWriteBranch::Data | IcebergWriteBranch::EqualityDelete => {
                return Err(invalid(format!(
                    "Iceberg {} branch cannot be built as a position-delete branch",
                    branch.as_str()
                )));
            }
        };
        if output.file_format() != expected_format {
            return Err(invalid(format!(
                "Iceberg {} branch must produce {expected_format:?}",
                branch.as_str()
            )));
        }
        let base_snapshot_id = table.base_snapshot_id();
        let mut frozen = BTreeMap::new();
        for target in old_deletes {
            if Some(target.base_snapshot_id()) != base_snapshot_id {
                return Err(invalid(
                    "Iceberg old-delete merge target does not name the session's frozen base snapshot",
                ));
            }
            if frozen
                .insert(target.data_file_path().to_string(), target)
                .is_some()
            {
                return Err(invalid(
                    "Iceberg delete writer handle repeats a referenced data file",
                ));
            }
        }
        Ok(Self {
            branch,
            table,
            output,
            data: None,
            old_deletes: frozen,
            equality: None,
        })
    }

    pub const fn branch(&self) -> IcebergWriteBranch {
        self.branch
    }
    pub const fn table(&self) -> &IcebergWriteTableFacts {
        &self.table
    }
    pub const fn output(&self) -> &IcebergWriterOutput {
        &self.output
    }
    pub fn data(&self) -> Option<&IcebergDataBranchRecipe> {
        self.data.as_ref()
    }

    /// The match key an equality-delete branch writes its file on, present
    /// exactly on that branch.
    pub const fn equality(&self) -> Option<&IcebergEqualityDeleteRecipe> {
        self.equality.as_ref()
    }

    /// The exact old-delete artifacts a writer must re-read, keyed by the data
    /// file each one belongs to.
    pub const fn old_deletes(&self) -> &BTreeMap<String, IcebergOldDeleteMergeTarget> {
        &self.old_deletes
    }

    /// The data files this handle owns exclusively for the old-delete merge.
    pub fn owned_data_files(&self) -> impl Iterator<Item = &str> {
        self.old_deletes.keys().map(String::as_str)
    }
}

/// A partition, as one staged artifact reports it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergArtifactPartition {
    partition_path: String,
    null_fingerprint: String,
    partition_spec_id: i32,
    descriptor: IcebergPartitionDescriptor,
}

impl IcebergArtifactPartition {
    pub fn try_new(
        partition_path: String,
        null_fingerprint: String,
        partition_spec_id: i32,
        descriptor: IcebergPartitionDescriptor,
    ) -> Result<Self, ConnectorError> {
        if partition_spec_id < 0 {
            return Err(invalid(
                "Iceberg artifact partition spec id must not be negative",
            ));
        }
        if partition_path.contains('\0') || null_fingerprint.contains('\0') {
            return Err(invalid(
                "Iceberg artifact partition path contains a NUL byte",
            ));
        }
        for (index, value) in descriptor.values.iter().enumerate() {
            match (value.is_null, value.datum_bytes.as_ref()) {
                (true, Some(_)) => {
                    return Err(corrupt(format!(
                        "Iceberg partition descriptor value {index} is null but carries a payload"
                    )));
                }
                (false, None) => {
                    return Err(corrupt(format!(
                        "Iceberg partition descriptor value {index} is non-null but has no payload"
                    )));
                }
                _ => {}
            }
        }
        Ok(Self {
            partition_path,
            null_fingerprint,
            partition_spec_id,
            descriptor,
        })
    }

    pub fn partition_path(&self) -> &str {
        &self.partition_path
    }
    pub fn null_fingerprint(&self) -> &str {
        &self.null_fingerprint
    }
    pub const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }
    pub const fn descriptor(&self) -> &IcebergPartitionDescriptor {
        &self.descriptor
    }
}

/// The physical metrics one staged artifact reports.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IcebergArtifactMetrics {
    record_count: u64,
    file_size_in_bytes: u64,
    split_offsets: Vec<i64>,
    column_stats: Option<IcebergColumnStats>,
}

impl IcebergArtifactMetrics {
    pub fn try_new(
        record_count: u64,
        file_size_in_bytes: u64,
        split_offsets: Vec<i64>,
        column_stats: Option<IcebergColumnStats>,
    ) -> Result<Self, ConnectorError> {
        if file_size_in_bytes == 0 {
            return Err(invalid(
                "Iceberg staged artifact must report a positive file size",
            ));
        }
        i64::try_from(record_count)
            .map_err(|_| invalid("Iceberg staged artifact record count overflows i64"))?;
        let size = i64::try_from(file_size_in_bytes)
            .map_err(|_| invalid("Iceberg staged artifact file size overflows i64"))?;
        let mut previous: Option<i64> = None;
        for offset in &split_offsets {
            if *offset < 0 || *offset >= size {
                return Err(invalid(
                    "Iceberg staged artifact split offset falls outside the file",
                ));
            }
            if previous.is_some_and(|last| *offset <= last) {
                return Err(invalid(
                    "Iceberg staged artifact split offsets must strictly ascend",
                ));
            }
            previous = Some(*offset);
        }
        if let Some(stats) = &column_stats {
            for counts in [
                &stats.column_sizes,
                &stats.value_counts,
                &stats.null_value_counts,
                &stats.nan_value_counts,
            ] {
                if counts.values().any(|value| *value < 0) {
                    return Err(corrupt(
                        "Iceberg staged artifact column statistic count is negative",
                    ));
                }
            }
        }
        Ok(Self {
            record_count,
            file_size_in_bytes,
            split_offsets,
            column_stats,
        })
    }

    pub const fn record_count(&self) -> u64 {
        self.record_count
    }
    pub const fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }
    pub fn split_offsets(&self) -> &[i64] {
        &self.split_offsets
    }
    pub fn column_stats(&self) -> Option<&IcebergColumnStats> {
        self.column_stats.as_ref()
    }
}

/// The bounded byte range one Puffin blob occupies inside its file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IcebergContentRange {
    offset: i64,
    size_in_bytes: i64,
}

impl IcebergContentRange {
    pub fn try_new(offset: i64, size_in_bytes: i64) -> Result<Self, ConnectorError> {
        if offset < 0 {
            return Err(invalid("Iceberg content range offset must not be negative"));
        }
        if size_in_bytes <= 0 {
            return Err(invalid("Iceberg content range size must be positive"));
        }
        offset
            .checked_add(size_in_bytes)
            .ok_or_else(|| invalid("Iceberg content range end overflows i64"))?;
        Ok(Self {
            offset,
            size_in_bytes,
        })
    }

    pub const fn offset(&self) -> i64 {
        self.offset
    }
    pub const fn size_in_bytes(&self) -> i64 {
        self.size_in_bytes
    }
    pub const fn end(&self) -> i64 {
        self.offset + self.size_in_bytes
    }
}

/// One written data file.
#[derive(Clone, Debug)]
pub struct IcebergDataFileArtifact {
    path: String,
    file_format: IcebergFileFormat,
    partition: IcebergArtifactPartition,
    metrics: IcebergArtifactMetrics,
    first_row_id: Option<i64>,
}

impl IcebergDataFileArtifact {
    pub fn try_new(
        path: String,
        file_format: IcebergFileFormat,
        partition: IcebergArtifactPartition,
        metrics: IcebergArtifactMetrics,
        first_row_id: Option<i64>,
    ) -> Result<Self, ConnectorError> {
        validate_location("staged data file", &path)?;
        if file_format != IcebergFileFormat::Parquet {
            return Err(invalid("Iceberg staged data file must be Parquet"));
        }
        if let Some(first_row_id) = first_row_id
            && first_row_id < 0
        {
            return Err(invalid(
                "Iceberg staged data file row lineage must not be negative",
            ));
        }
        Ok(Self {
            path,
            file_format,
            partition,
            metrics,
            first_row_id,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub const fn file_format(&self) -> IcebergFileFormat {
        self.file_format
    }
    pub const fn partition(&self) -> &IcebergArtifactPartition {
        &self.partition
    }
    pub const fn metrics(&self) -> &IcebergArtifactMetrics {
        &self.metrics
    }
    pub const fn first_row_id(&self) -> Option<i64> {
        self.first_row_id
    }
}

/// One written Parquet position-delete file.
#[derive(Clone, Debug)]
pub struct IcebergPositionDeleteFileArtifact {
    path: String,
    partition: IcebergArtifactPartition,
    metrics: IcebergArtifactMetrics,
    referenced_data_file: String,
    merged_old_references: Vec<EntryIdentity>,
}

impl IcebergPositionDeleteFileArtifact {
    pub fn try_new(
        path: String,
        partition: IcebergArtifactPartition,
        metrics: IcebergArtifactMetrics,
        referenced_data_file: String,
        merged_old_references: Vec<EntryIdentity>,
    ) -> Result<Self, ConnectorError> {
        validate_location("staged position-delete file", &path)?;
        validate_location("referenced data file", &referenced_data_file)?;
        if metrics.record_count() == 0 {
            return Err(invalid(
                "Iceberg staged position-delete file must delete at least one row",
            ));
        }
        validate_merged_references(&merged_old_references)?;
        Ok(Self {
            path,
            partition,
            metrics,
            referenced_data_file,
            merged_old_references,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub const fn partition(&self) -> &IcebergArtifactPartition {
        &self.partition
    }
    pub const fn metrics(&self) -> &IcebergArtifactMetrics {
        &self.metrics
    }
    pub fn referenced_data_file(&self) -> &str {
        &self.referenced_data_file
    }
    /// The exact old artifacts this file superseded, in sorted order.
    pub fn merged_old_references(&self) -> &[EntryIdentity] {
        &self.merged_old_references
    }
}

/// One column an equality-delete file matches rows on.
///
/// The name and field identity of one equality key; types belong to field_schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergEqualityDeleteColumnFacts {
    name: String,
    field_id: i32,
}
impl IcebergEqualityDeleteColumnFacts {
    pub fn try_new(name: String, field_id: i32) -> Result<Self, ConnectorError> {
        if name.is_empty() {
            return Err(invalid("Iceberg equality-delete column requires a name"));
        }
        if field_id < 0 {
            return Err(invalid(format!(
                "Iceberg equality-delete column `{name}` requires a nonnegative field id"
            )));
        }
        Ok(Self { name, field_id })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub const fn field_id(&self) -> i32 {
        self.field_id
    }
}

/// Complete provider schema projection in equality-key input order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergEqualityDeleteRecipe {
    columns: Vec<IcebergEqualityDeleteColumnFacts>,
    field_schema_json: String,
}
impl IcebergEqualityDeleteRecipe {
    pub fn try_new(
        columns: Vec<IcebergEqualityDeleteColumnFacts>,
        field_schema_json: String,
    ) -> Result<Self, ConnectorError> {
        if columns.is_empty() {
            return Err(invalid(
                "Iceberg equality delete matches on at least one column",
            ));
        }
        let mut seen = BTreeSet::new();
        for column in &columns {
            if !seen.insert(column.field_id()) {
                return Err(invalid(format!(
                    "Iceberg equality-delete columns repeat field id {}",
                    column.field_id()
                )));
            }
        }
        Ok(Self {
            columns,
            field_schema_json,
        })
    }
    pub fn columns(&self) -> &[IcebergEqualityDeleteColumnFacts] {
        &self.columns
    }
    pub fn field_schema_json(&self) -> &str {
        &self.field_schema_json
    }
    pub fn field_ids(&self) -> Vec<i32> {
        self.columns
            .iter()
            .map(IcebergEqualityDeleteColumnFacts::field_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

/// One written Parquet equality-delete file.
///
/// It names no data file: Iceberg applies an equality delete to every row whose
/// equality-column values match, across the data files the delete's sequence
/// number covers. That is also why it supersedes nothing and merges nothing.
#[derive(Clone, Debug)]
pub struct IcebergEqualityDeleteFileArtifact {
    path: String,
    partition: IcebergArtifactPartition,
    metrics: IcebergArtifactMetrics,
    equality_field_ids: Vec<i32>,
}

impl IcebergEqualityDeleteFileArtifact {
    pub fn try_new(
        path: String,
        partition: IcebergArtifactPartition,
        metrics: IcebergArtifactMetrics,
        equality_field_ids: Vec<i32>,
    ) -> Result<Self, ConnectorError> {
        validate_location("staged equality-delete file", &path)?;
        if metrics.record_count() == 0 {
            return Err(invalid(
                "Iceberg staged equality-delete file must carry at least one match row",
            ));
        }
        if equality_field_ids.is_empty() {
            return Err(invalid(
                "Iceberg staged equality-delete file matches on at least one field id",
            ));
        }
        // Iceberg reads these back as a set, so a duplicate is meaningless and
        // an unstable order would make the same file look different on replay.
        let mut previous: Option<i32> = None;
        for field_id in &equality_field_ids {
            if *field_id < 0 {
                return Err(invalid(
                    "Iceberg staged equality-delete field id must be nonnegative",
                ));
            }
            if previous.is_some_and(|previous| previous >= *field_id) {
                return Err(invalid(
                    "Iceberg staged equality-delete field ids must be sorted and unique",
                ));
            }
            previous = Some(*field_id);
        }
        Ok(Self {
            path,
            partition,
            metrics,
            equality_field_ids,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub const fn partition(&self) -> &IcebergArtifactPartition {
        &self.partition
    }
    pub const fn metrics(&self) -> &IcebergArtifactMetrics {
        &self.metrics
    }
    pub fn equality_field_ids(&self) -> &[i32] {
        &self.equality_field_ids
    }
}

/// One written Puffin deletion vector.
#[derive(Clone, Debug)]
pub struct IcebergDeletionVectorArtifact {
    path: String,
    partition: IcebergArtifactPartition,
    metrics: IcebergArtifactMetrics,
    referenced_data_file: String,
    content_range: IcebergContentRange,
    cardinality: u64,
    merged_old_references: Vec<EntryIdentity>,
}

impl IcebergDeletionVectorArtifact {
    pub fn try_new(
        path: String,
        partition: IcebergArtifactPartition,
        metrics: IcebergArtifactMetrics,
        referenced_data_file: String,
        content_range: IcebergContentRange,
        cardinality: u64,
        merged_old_references: Vec<EntryIdentity>,
    ) -> Result<Self, ConnectorError> {
        validate_location("staged deletion vector", &path)?;
        validate_location("referenced data file", &referenced_data_file)?;
        if cardinality == 0 {
            return Err(invalid(
                "Iceberg staged deletion vector must delete at least one row",
            ));
        }
        if cardinality != metrics.record_count() {
            return Err(corrupt(
                "Iceberg staged deletion vector cardinality differs from its record count",
            ));
        }
        let size = i64::try_from(metrics.file_size_in_bytes())
            .map_err(|_| invalid("Iceberg staged deletion vector file size overflows i64"))?;
        if content_range.end() > size {
            return Err(corrupt(
                "Iceberg staged deletion vector blob range extends past its Puffin file",
            ));
        }
        validate_merged_references(&merged_old_references)?;
        Ok(Self {
            path,
            partition,
            metrics,
            referenced_data_file,
            content_range,
            cardinality,
            merged_old_references,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub const fn partition(&self) -> &IcebergArtifactPartition {
        &self.partition
    }
    pub const fn metrics(&self) -> &IcebergArtifactMetrics {
        &self.metrics
    }
    pub fn referenced_data_file(&self) -> &str {
        &self.referenced_data_file
    }
    pub const fn content_range(&self) -> IcebergContentRange {
        self.content_range
    }
    pub const fn cardinality(&self) -> u64 {
        self.cardinality
    }
    pub fn merged_old_references(&self) -> &[EntryIdentity] {
        &self.merged_old_references
    }
}

fn validate_merged_references(references: &[EntryIdentity]) -> Result<(), ConnectorError> {
    let mut previous: Option<&EntryIdentity> = None;
    for reference in references {
        reference
            .validate()
            .map_err(|error| corrupt(error.to_string()))?;
        validate_location("merged old delete reference", reference.path())?;
        if matches!(reference, EntryIdentity::DataFile { .. }) {
            return Err(corrupt(
                "Merged old-delete reference must identify a delete entry",
            ));
        }
        if previous.is_some_and(|last| last >= reference) {
            return Err(corrupt(
                "Iceberg merged old-delete references must be sorted and unique",
            ));
        }
        previous = Some(reference);
    }
    Ok(())
}

/// The one artifact a commit fragment describes.
#[derive(Clone, Debug)]
pub enum IcebergCommitArtifact {
    DataFile(IcebergDataFileArtifact),
    PositionDeleteFile(IcebergPositionDeleteFileArtifact),
    DeletionVector(IcebergDeletionVectorArtifact),
    EqualityDeleteFile(IcebergEqualityDeleteFileArtifact),
}

/// Exactly one staged Iceberg artifact.
///
/// A fragment carries no writer identity, no attempt id, and no aggregate
/// summary: those are properties of an execution, not of an artifact, and
/// putting them here would make two identical artifacts look different.
#[derive(Clone, Debug)]
pub struct IcebergCommitFragment {
    artifact: IcebergCommitArtifact,
}

impl IcebergCommitFragment {
    pub const fn new(artifact: IcebergCommitArtifact) -> Self {
        Self { artifact }
    }

    pub fn data_file(artifact: IcebergDataFileArtifact) -> Self {
        Self::new(IcebergCommitArtifact::DataFile(artifact))
    }

    pub fn position_delete_file(artifact: IcebergPositionDeleteFileArtifact) -> Self {
        Self::new(IcebergCommitArtifact::PositionDeleteFile(artifact))
    }

    pub fn deletion_vector(artifact: IcebergDeletionVectorArtifact) -> Self {
        Self::new(IcebergCommitArtifact::DeletionVector(artifact))
    }

    pub fn equality_delete_file(artifact: IcebergEqualityDeleteFileArtifact) -> Self {
        Self::new(IcebergCommitArtifact::EqualityDeleteFile(artifact))
    }

    pub const fn artifact(&self) -> &IcebergCommitArtifact {
        &self.artifact
    }

    /// The logical identity is unique across one prepared write set.
    pub fn entry_identity(&self) -> EntryIdentity {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(file) => EntryIdentity::DataFile {
                path: file.path().to_string(),
            },
            IcebergCommitArtifact::PositionDeleteFile(file) => EntryIdentity::DeleteFile {
                path: file.path().to_string(),
            },
            IcebergCommitArtifact::EqualityDeleteFile(file) => EntryIdentity::DeleteFile {
                path: file.path().to_string(),
            },
            IcebergCommitArtifact::DeletionVector(file) => EntryIdentity::DeletionVector {
                path: file.path().to_string(),
                offset: file.content_range().offset(),
                length: file.content_range().size_in_bytes(),
                referenced_data_file: file.referenced_data_file().to_string(),
            },
        }
    }

    /// The physical object location; distinct DV entries may share it.
    pub fn path(&self) -> &str {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(file) => file.path(),
            IcebergCommitArtifact::PositionDeleteFile(file) => file.path(),
            IcebergCommitArtifact::DeletionVector(file) => file.path(),
            IcebergCommitArtifact::EqualityDeleteFile(file) => file.path(),
        }
    }

    pub fn partition(&self) -> &IcebergArtifactPartition {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(file) => file.partition(),
            IcebergCommitArtifact::PositionDeleteFile(file) => file.partition(),
            IcebergCommitArtifact::DeletionVector(file) => file.partition(),
            IcebergCommitArtifact::EqualityDeleteFile(file) => file.partition(),
        }
    }

    pub fn metrics(&self) -> &IcebergArtifactMetrics {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(file) => file.metrics(),
            IcebergCommitArtifact::PositionDeleteFile(file) => file.metrics(),
            IcebergCommitArtifact::DeletionVector(file) => file.metrics(),
            IcebergCommitArtifact::EqualityDeleteFile(file) => file.metrics(),
        }
    }

    /// The data file a delete artifact supersedes deletes for, if any.
    pub fn referenced_data_file(&self) -> Option<&str> {
        match &self.artifact {
            // A data file supersedes nothing, and an equality delete names no
            // data file at all: it matches on column values across every file
            // its sequence number covers.
            IcebergCommitArtifact::DataFile(_) | IcebergCommitArtifact::EqualityDeleteFile(_) => {
                None
            }
            IcebergCommitArtifact::PositionDeleteFile(file) => Some(file.referenced_data_file()),
            IcebergCommitArtifact::DeletionVector(file) => Some(file.referenced_data_file()),
        }
    }

    pub fn merged_old_references(&self) -> &[EntryIdentity] {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(_) | IcebergCommitArtifact::EqualityDeleteFile(_) => {
                &[]
            }
            IcebergCommitArtifact::PositionDeleteFile(file) => file.merged_old_references(),
            IcebergCommitArtifact::DeletionVector(file) => file.merged_old_references(),
        }
    }

    /// The branch that may legally produce this artifact.
    pub const fn branch(&self) -> IcebergWriteBranch {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(_) => IcebergWriteBranch::Data,
            IcebergCommitArtifact::PositionDeleteFile(_) => IcebergWriteBranch::PositionDelete,
            IcebergCommitArtifact::DeletionVector(_) => IcebergWriteBranch::DeletionVector,
            IcebergCommitArtifact::EqualityDeleteFile(_) => IcebergWriteBranch::EqualityDelete,
        }
    }
}

/// One sealed logical target inside a begin session.
///
/// A delete branch's map keys are every old data file routed to it, and each
/// value is the sorted set of old delete artifacts the session froze for that
/// file. An empty value means "this data file has no old deletes" — a decision,
/// not a read result.
#[derive(Clone, Debug)]
pub struct IcebergSealedWriteTarget {
    ordinal: WriteTargetOrdinal,
    branch: IcebergWriteBranch,
    owned_data_files: BTreeMap<String, Vec<EntryIdentity>>,
}

impl IcebergSealedWriteTarget {
    pub const fn new(
        ordinal: WriteTargetOrdinal,
        branch: IcebergWriteBranch,
        owned_data_files: BTreeMap<String, Vec<EntryIdentity>>,
    ) -> Self {
        Self {
            ordinal,
            branch,
            owned_data_files,
        }
    }

    pub const fn ordinal(&self) -> WriteTargetOrdinal {
        self.ordinal
    }
    pub const fn branch(&self) -> IcebergWriteBranch {
        self.branch
    }
    pub const fn owned_data_files(&self) -> &BTreeMap<String, Vec<EntryIdentity>> {
        &self.owned_data_files
    }
    pub fn data_files(&self) -> impl Iterator<Item = &String> {
        self.owned_data_files.keys()
    }
}

/// The frontend-only write session identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IcebergWriteSessionId([u8; 16]);

impl IcebergWriteSessionId {
    pub fn new() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl Default for IcebergWriteSessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for IcebergWriteSessionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", uuid::Uuid::from_bytes(self.0))
    }
}

/// The frontend-only Iceberg write session.
///
/// It is never `Clone`: exactly one owner may finish, abort, or reconcile it.
/// Its terminal state lives behind a mutex because the neutral contract borrows
/// the handle on every terminal call.
#[derive(Debug)]
pub struct IcebergCommitHandle {
    session_id: IcebergWriteSessionId,
    table: IcebergWriteTableFacts,
    flavor: IcebergWriteFlavor,
    purpose: ConnectorWriteAdmissionPurpose,
    base_version_digest: Option<[u8; 32]>,
    document_publication: Option<IcebergDocumentPublicationFacts>,
    targets: Vec<IcebergSealedWriteTarget>,
    delete_owner: BTreeMap<String, WriteTargetOrdinal>,
    /// Managed publications retain the same exact merge facts already frozen
    /// for their BE writers. This is session-local evidence for logical row
    /// changes, never a serialized second source of delete identities.
    frozen_delete_merge_targets:
        Option<BTreeMap<WriteTargetOrdinal, BTreeMap<String, IcebergOldDeleteMergeTarget>>>,
    /// The frozen metadata of a staged target, present exactly on a
    /// staged-create session. It stands in for the catalog load every other
    /// flavor performs, and the seal reads its schema and partition spec to
    /// interpret the artifacts the backends staged.
    staged_metadata: Option<Arc<crate::iceberg::spec::TableMetadata>>,
    /// Source facts remain pinned across retries, independently of latest M.
    /// This is a provider-private runtime value, never a serialized handle.
    source_metadata: Option<Arc<crate::iceberg::spec::TableMetadata>>,
    /// The frozen input file set of every sealed rewrite branch, in ordinal
    /// order. Present exactly on a distributed rewrite, which is the one flavor
    /// whose commit replaces files it named before any writer ran.
    rewrite_inputs: Vec<IcebergFrozenRewriteBranchInput>,
    /// What every sealed copy-on-write branch replaces, in ordinal order.
    /// Present exactly on a copy-on-write mutation.
    ///
    /// It stays on the session because the single external commit needs it and
    /// nothing on the data plane carries it: a writer produces replacement
    /// rows, and which old file those rows supersede — together with the row
    /// ids inside it that were matched — is a planning fact the session froze
    /// before any writer ran.
    copy_on_write: Vec<IcebergCowBranchInput>,
    /// The partition replacement this session's single external commit applies
    /// ahead of its snapshot. Present only on a managed publication that was
    /// admitted with one.
    repartition: Option<crate::commit::write_stack::repartition::IcebergPreparedRepartition>,
    /// Exact collect-on-write artifact identities frozen for each logical
    /// target. These remain provider-private session facts; only the cloneable
    /// aggregate requirements are exposed to FE planning.
    statistics_expectations:
        BTreeMap<WriteTargetOrdinal, Vec<novarocks_spi::connector::StatisticsArtifactIdentity>>,
    document_records: std::sync::Mutex<Option<BTreeMap<String, String>>>,
    terminal_finalization: std::sync::Mutex<novarocks_spi::connector::ExternalMutationFinalization>,
    recovery_evidence:
        Arc<std::sync::Mutex<Option<novarocks_spi::connector::ExternalMutationEvidence>>>,
    state: std::sync::Mutex<IcebergWriteSessionState>,
}

/// Everything a sealed session carries besides its identity, its table
/// generation and its logical targets.
///
/// It is one value rather than a parameter list because these facts are
/// cross-validated against each other and against the flavor: a publication
/// technique, frozen staged metadata and a frozen rewrite input set are each
/// admissible on exactly one flavor, and stating them together is what makes
/// that checkable in one place.
#[derive(Clone, Debug)]
pub struct IcebergSessionFacts {
    pub purpose: ConnectorWriteAdmissionPurpose,
    pub base_version_digest: Option<[u8; 32]>,
    /// Present exactly on an application-document publication flavor.
    pub document_publication: Option<IcebergDocumentPublicationFacts>,
    /// Present exactly on the staged-create flavor.
    pub staged_metadata: Option<Arc<crate::iceberg::spec::TableMetadata>>,
    /// Present exactly on the distributed-rewrite flavor, one entry per sealed
    /// branch and in the same order.
    pub rewrite_inputs: Vec<IcebergFrozenRewriteBranchInput>,
    /// Present exactly on the copy-on-write flavor, one entry per sealed branch
    /// and in the same order.
    pub copy_on_write: Vec<IcebergCowBranchInput>,
    /// Present only on a managed publication that replaces the target's default
    /// partitioning in the same external commit that publishes its rows.
    pub(crate) repartition:
        Option<crate::commit::write_stack::repartition::IcebergPreparedRepartition>,
}

impl IcebergSessionFacts {
    /// The facts of a session that carries none of the flavor-specific ones:
    /// no publication, no staged target, nothing frozen to rewrite.
    pub const fn ordinary(
        purpose: ConnectorWriteAdmissionPurpose,
        base_version_digest: Option<[u8; 32]>,
    ) -> Self {
        Self {
            purpose,
            base_version_digest,
            document_publication: None,
            staged_metadata: None,
            rewrite_inputs: Vec::new(),
            copy_on_write: Vec::new(),
            repartition: None,
        }
    }
}

/// The terminal state a write session may reach. It mirrors the provider's
/// existing commit verdicts exactly: a session never reports a commit it did
/// not observe, and an unknown outcome stays unknown until reconciliation
/// resolves it.
#[derive(Clone, Debug)]
pub enum IcebergWriteSessionState {
    /// No external effect has happened yet.
    Active,
    /// A commit is in flight. A second terminal call must not start another.
    Committing,
    /// The external commit is proven to have happened.
    KnownCommitted { snapshot_id: Option<i64> },
    /// The prepared write set was sealed into a receipt and no external commit
    /// was attempted. Only a staged-create session reaches this: the single
    /// external effect belongs to the publication that owns the staged target,
    /// so this session is finished without ever having touched the catalog.
    Sealed,
    /// The external commit is proven not to have happened.
    KnownUncommitted { message: String },
    /// The external outcome is unknown and staged files were deliberately left
    /// in place for reconciliation.
    CommitUnknown {
        message: String,
        staging_dir: String,
    },
}

impl IcebergCommitHandle {
    /// Seal a begin session.
    ///
    /// Target ordinals must be dense from zero and each branch must appear at
    /// most once, because a duplicate branch would make the delete-owner route
    /// ambiguous.
    pub fn try_new(
        session_id: IcebergWriteSessionId,
        table: IcebergWriteTableFacts,
        flavor: IcebergWriteFlavor,
        purpose: ConnectorWriteAdmissionPurpose,
        base_version_digest: Option<[u8; 32]>,
        targets: Vec<IcebergSealedWriteTarget>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_sealed(
            session_id,
            table,
            flavor,
            IcebergSessionFacts::ordinary(purpose, base_version_digest),
            targets,
        )
    }

    /// Seal a begin session together with the facts only its own flavor
    /// carries.
    ///
    /// Each of those facts is admissible on exactly one flavor: a session that
    /// carried a publication technique it does not publish under would decide
    /// its commit op from a fact its own flavor contradicts, and one carrying a
    /// frozen rewrite input set it does not replace would retire live files no
    /// writer superseded.
    pub fn try_new_sealed(
        session_id: IcebergWriteSessionId,
        table: IcebergWriteTableFacts,
        flavor: IcebergWriteFlavor,
        facts: IcebergSessionFacts,
        targets: Vec<IcebergSealedWriteTarget>,
    ) -> Result<Self, ConnectorError> {
        let IcebergSessionFacts {
            purpose,
            base_version_digest,
            document_publication,
            staged_metadata,
            rewrite_inputs,
            copy_on_write,
            repartition,
        } = facts;
        if document_publication.is_some() && flavor != IcebergWriteFlavor::ManagedPublication {
            return Err(invalid(format!(
                "Iceberg {} write session cannot carry application-document publication facts",
                flavor.as_str()
            )));
        }
        // The two directions are both errors, and for the same reason: a staged
        // session with no frozen metadata has nothing to interpret its
        // artifacts against, and any other flavor carrying staged metadata
        // would have two disagreeing sources for its target's schema.
        if staged_metadata.is_some() != (flavor == IcebergWriteFlavor::StagedCreate) {
            return Err(invalid(format!(
                "Iceberg {} write session must carry frozen staged metadata exactly when it stages a create",
                flavor.as_str()
            )));
        }
        // The rewrite input set is per branch, so a session holding a different
        // number of entries than it sealed branches could not say which files
        // any one branch replaces. The two directions are both errors for the
        // same reason as the staged metadata above.
        if rewrite_inputs.is_empty() == flavor.is_distributed_rewrite() {
            return Err(invalid(format!(
                "Iceberg {} write session must carry a frozen rewrite input per branch exactly when it rewrites a frozen file set",
                flavor.as_str()
            )));
        }
        // A partition replacement changes what the target *is*, and only a
        // managed publication is admitted to do that. Every other flavor
        // carrying one would apply a spec change its own caller never asked
        // for, in a commit that is not the publication's.
        if repartition.is_some() && flavor != IcebergWriteFlavor::ManagedPublication {
            return Err(invalid(format!(
                "Iceberg {} write session cannot carry an atomic partition replacement",
                flavor.as_str()
            )));
        }
        // A copy-on-write session's branch count follows the files its match
        // selection touched, so both directions are errors: a copy-on-write
        // session with no frozen branch would have nothing to replace, and any
        // other flavor carrying one would retire a live file no writer
        // superseded.
        if copy_on_write.is_empty() == (flavor == IcebergWriteFlavor::RowMutationCopyOnWrite) {
            return Err(invalid(format!(
                "Iceberg {} write session must carry a frozen copy-on-write branch per target exactly when it rewrites matched files",
                flavor.as_str()
            )));
        }
        if !copy_on_write.is_empty() && copy_on_write.len() != targets.len() {
            return Err(invalid(format!(
                "Iceberg {} write session froze {} copy-on-write branches for {} sealed targets",
                flavor.as_str(),
                copy_on_write.len(),
                targets.len()
            )));
        }
        if !rewrite_inputs.is_empty() && rewrite_inputs.len() != targets.len() {
            return Err(invalid(format!(
                "Iceberg {} write session froze {} rewrite inputs for {} sealed branches",
                flavor.as_str(),
                rewrite_inputs.len(),
                targets.len()
            )));
        }
        let ordinals = targets
            .iter()
            .map(IcebergSealedWriteTarget::ordinal)
            .collect::<Vec<_>>();
        novarocks_spi::connector::write_stack::validate_dense_target_ordinals(&ordinals)?;
        let mut branches = BTreeSet::new();
        for target in &targets {
            if !branches.insert(target.branch()) && flavor.seals_one_target_per_branch() {
                return Err(invalid(
                    "Iceberg write session repeats a physical write branch",
                ));
            }
        }
        let publication_shape = document_publication
            .as_ref()
            .map(IcebergDocumentPublicationFacts::shape);
        let allowed = allowed_session_branches(flavor, publication_shape)
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if !branches.is_subset(&allowed) {
            return Err(invalid(format!(
                "Iceberg {} write session seals a branch the flavor does not own",
                flavor.as_str()
            )));
        }
        let delete_owner = super::planning::prove_unique_delete_owner(&targets)?;
        Ok(Self {
            session_id,
            table,
            flavor,
            purpose,
            base_version_digest,
            document_publication,
            targets,
            delete_owner,
            frozen_delete_merge_targets: None,
            staged_metadata,
            source_metadata: None,
            rewrite_inputs,
            copy_on_write,
            repartition,
            statistics_expectations: BTreeMap::new(),
            document_records: std::sync::Mutex::new(None),
            terminal_finalization: std::sync::Mutex::new(
                novarocks_spi::connector::ExternalMutationFinalization::Complete,
            ),
            recovery_evidence: Arc::new(std::sync::Mutex::new(None)),
            state: std::sync::Mutex::new(IcebergWriteSessionState::Active),
        })
    }

    pub(crate) fn with_frozen_delete_merge_targets(
        mut self,
        targets: BTreeMap<WriteTargetOrdinal, BTreeMap<String, IcebergOldDeleteMergeTarget>>,
    ) -> Result<Self, ConnectorError> {
        if self.document_publication.is_none() || self.frozen_delete_merge_targets.is_some() {
            return Err(invalid(
                "Iceberg logical row facts require one managed publication owner",
            ));
        }
        let mut identities = BTreeMap::new();
        for (ordinal, files) in &targets {
            let mut entries = BTreeMap::new();
            for (path, target) in files {
                if path != target.data_file_path()
                    || Some(target.base_snapshot_id()) != self.table.base_snapshot_id()
                {
                    return Err(invalid(
                        "Iceberg logical row facts differ from their frozen data-file target",
                    ));
                }
                entries.insert(
                    path.clone(),
                    target
                        .references()
                        .iter()
                        .map(|reference| reference.entry_identity())
                        .collect::<Vec<_>>(),
                );
            }
            identities.insert(*ordinal, entries);
        }
        if identities != self.frozen_old_references() {
            return Err(invalid(
                "Iceberg logical row facts differ from the sealed old-delete identities",
            ));
        }
        self.frozen_delete_merge_targets = Some(targets);
        Ok(self)
    }

    pub(crate) fn frozen_delete_merge_targets(
        &self,
    ) -> Option<&BTreeMap<WriteTargetOrdinal, BTreeMap<String, IcebergOldDeleteMergeTarget>>> {
        self.frozen_delete_merge_targets.as_ref()
    }

    pub(crate) fn with_source_metadata(
        mut self,
        metadata: Arc<crate::iceberg::spec::TableMetadata>,
    ) -> Result<Self, ConnectorError> {
        let source = metadata.snapshot_for_ref(self.table.target_ref());
        let mut uuid_buffer = [0u8; 36];
        let uuid = metadata.uuid().hyphenated().encode_lower(&mut uuid_buffer);
        if self.flavor == IcebergWriteFlavor::StagedCreate
            || self.source_metadata.is_some()
            || uuid != self.table.table_uuid()
            || source.map(|snapshot| snapshot.snapshot_id()) != self.table.base_snapshot_id()
            || source.map_or(0, |snapshot| snapshot.sequence_number())
                != self.table.base_sequence_number()
        {
            return Err(invalid(
                "Pinned Iceberg source metadata contradicts the admitted session",
            ));
        }
        self.source_metadata = Some(metadata);
        Ok(self)
    }

    pub(crate) fn source_metadata(&self) -> Option<&crate::iceberg::spec::TableMetadata> {
        self.source_metadata.as_deref()
    }

    pub(crate) fn with_statistics_expectations(
        mut self,
        expectations: BTreeMap<
            WriteTargetOrdinal,
            Vec<novarocks_spi::connector::StatisticsArtifactIdentity>,
        >,
    ) -> Result<Self, ConnectorError> {
        let targets = self
            .targets
            .iter()
            .map(IcebergSealedWriteTarget::ordinal)
            .collect::<BTreeSet<_>>();
        for (target, identities) in &expectations {
            if !targets.contains(target) {
                return Err(invalid(
                    "Iceberg write statistics expectation names an unsealed target",
                ));
            }
            let unique = identities.iter().collect::<BTreeSet<_>>();
            if unique.len() != identities.len() {
                return Err(invalid(
                    "Iceberg write statistics expectation repeats an artifact identity",
                ));
            }
        }
        self.statistics_expectations = expectations;
        Ok(self)
    }

    /// The frozen input files this session's commit replaces, unioned across
    /// every sealed branch.
    ///
    /// The union, not the per-branch sets, is what the commit needs: one
    /// snapshot replaces the whole planned set at once. Reading it off the
    /// session keeps it a planning fact — a branch whose writer produced
    /// nothing still retires its inputs, which is exactly what makes a rewrite
    /// that compacted rows away correct.
    pub fn frozen_rewrite_input(&self) -> IcebergFrozenRewriteBranchInput {
        let mut data_paths = BTreeSet::new();
        let mut delete_paths = BTreeSet::new();
        for input in &self.rewrite_inputs {
            data_paths.extend(input.data_paths().iter().cloned());
            delete_paths.extend(input.delete_paths().iter().cloned());
        }
        IcebergFrozenRewriteBranchInput {
            data_paths,
            delete_paths,
        }
    }

    pub const fn session_id(&self) -> IcebergWriteSessionId {
        self.session_id
    }
    pub const fn table(&self) -> &IcebergWriteTableFacts {
        &self.table
    }
    pub const fn flavor(&self) -> IcebergWriteFlavor {
        self.flavor
    }
    pub const fn purpose(&self) -> ConnectorWriteAdmissionPurpose {
        self.purpose
    }
    pub const fn base_version_digest(&self) -> Option<[u8; 32]> {
        self.base_version_digest
    }
    pub const fn document_publication(&self) -> Option<&IcebergDocumentPublicationFacts> {
        self.document_publication.as_ref()
    }

    pub(crate) fn bind_document_records(
        &self,
        records: BTreeMap<String, String>,
    ) -> Result<(), ConnectorError> {
        if self.document_publication.is_none() || records.is_empty() {
            return Err(invalid(
                "keyed publication requires a document session and at least one record",
            ));
        }
        let mut bound = self
            .document_records
            .lock()
            .map_err(|_| corrupt("keyed record lock is poisoned"))?;
        match &*bound {
            Some(existing) if existing != &records => Err(invalid(
                "write session is already bound to different keyed record bytes",
            )),
            Some(_) => Ok(()),
            None => {
                *bound = Some(records);
                Ok(())
            }
        }
    }
    pub(crate) fn document_records(
        &self,
    ) -> Result<Option<BTreeMap<String, String>>, ConnectorError> {
        self.document_records
            .lock()
            .map(|records| records.clone())
            .map_err(|_| corrupt("keyed record lock is poisoned"))
    }
    pub(crate) fn document_records_digest(&self) -> Result<Option<[u8; 32]>, ConnectorError> {
        use sha2::{Digest, Sha256};
        self.document_records()?
            .map(|records| {
                let mut digest = Sha256::new();
                for (key, value) in records {
                    for bytes in [key.as_bytes(), value.as_bytes()] {
                        let length = u32::try_from(bytes.len())
                            .map_err(|_| invalid("keyed record exceeds digest length framing"))?;
                        digest.update(length.to_be_bytes());
                        digest.update(bytes);
                    }
                }
                Ok(digest.finalize().into())
            })
            .transpose()
    }

    /// The partition replacement this session's commit applies, present only on
    /// a managed publication admitted with one.
    ///
    /// It carries the prospective metadata every writer already wrote under, so
    /// the commit interprets the staged artifacts against the same spec that
    /// produced them rather than against the one the table still has.
    pub(crate) const fn repartition(
        &self,
    ) -> Option<&crate::commit::write_stack::repartition::IcebergPreparedRepartition> {
        self.repartition.as_ref()
    }

    /// The frozen metadata a staged-create session interprets its artifacts
    /// against. Absent on every other flavor, which loads its target instead.
    pub fn staged_metadata(&self) -> Option<&crate::iceberg::spec::TableMetadata> {
        self.staged_metadata.as_deref()
    }

    pub fn targets(&self) -> &[IcebergSealedWriteTarget] {
        &self.targets
    }

    /// What every sealed copy-on-write branch replaces, in ordinal order.
    ///
    /// Empty on every other flavor. The commit joins entry `i` with the
    /// fragments the backends staged for write target `i`, which is the only
    /// key involved: no cohort, operation, or attempt identity takes part.
    pub(crate) fn copy_on_write(&self) -> &[IcebergCowBranchInput] {
        &self.copy_on_write
    }

    /// The single external commit op this session performs.
    ///
    /// A managed publication decides it from its technique — a full refresh
    /// replaces, an incremental one appends — and every other session decides
    /// it from its flavor alone.
    pub fn commit_op_kind(&self) -> CommitOpKind {
        match &self.document_publication {
            Some(publication) => publication.commit_op_kind(self.sealed_delete_branch()),
            None => self.flavor.commit_op_kind(),
        }
    }

    /// The delete branch this session sealed, if it sealed one.
    ///
    /// At most one exists: `seals_one_target_per_branch` forbids repeating a
    /// branch for every flavor that can own a delete branch at all, so the
    /// first match is the only one.
    fn sealed_delete_branch(&self) -> Option<IcebergWriteBranch> {
        self.targets
            .iter()
            .map(IcebergSealedWriteTarget::branch)
            .find(|branch| branch.writes_deletes())
    }

    /// Whether this session must be serialized behind the distributed external
    /// write fence.
    ///
    /// Read off the sealed session rather than re-derived, so the exemption a
    /// rewrite relies on is stated once and cannot drift from what the session
    /// actually is.
    pub const fn requires_external_write_fence(&self) -> bool {
        self.flavor.requires_external_write_fence()
    }

    /// What an empty prepared write set means for this session.
    ///
    /// Two sessions terminate without any external commit: a managed
    /// publication whose caller declared `AbortWithoutExternalCommit`, and a
    /// distributed rewrite that found nothing to rewrite. Neither is a failure,
    /// and neither is derivable from "there were no fragments" alone — the
    /// first depends on a disposition only the publication carries, and the
    /// second differs from a zero-row `INSERT`, which still publishes.
    pub fn empty_write_decision(&self) -> IcebergEmptyWriteDecision {
        if self.flavor.is_distributed_rewrite() {
            return IcebergEmptyWriteDecision::SkipExternalCommit;
        }
        let disposition = self
            .document_publication
            .as_ref()
            .map(IcebergDocumentPublicationFacts::empty_input);
        match disposition {
            Some(ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit) => {
                IcebergEmptyWriteDecision::SkipExternalCommit
            }
            _ => IcebergEmptyWriteDecision::Commit,
        }
    }

    /// The proven route from an old data file routed to a delete branch to its
    /// single owning logical target. It is a superset of the files that
    /// actually need a merge, because a file with no old deletes still needs a
    /// unique writer.
    pub const fn delete_owner(&self) -> &BTreeMap<String, WriteTargetOrdinal> {
        &self.delete_owner
    }

    /// The frozen old-delete references, keyed by target and data file.
    ///
    /// `finish_write` uses this to prove each staged artifact superseded
    /// exactly the references the session froze.
    pub fn frozen_old_references(
        &self,
    ) -> BTreeMap<WriteTargetOrdinal, BTreeMap<String, Vec<EntryIdentity>>> {
        self.targets
            .iter()
            .filter(|target| target.branch().writes_deletes())
            .map(|target| (target.ordinal(), target.owned_data_files().clone()))
            .collect()
    }

    pub fn expected_targets(&self) -> Vec<WriteTargetOrdinal> {
        self.targets
            .iter()
            .map(IcebergSealedWriteTarget::ordinal)
            .collect()
    }

    pub(crate) const fn statistics_expectations(
        &self,
    ) -> &BTreeMap<WriteTargetOrdinal, Vec<novarocks_spi::connector::StatisticsArtifactIdentity>>
    {
        &self.statistics_expectations
    }

    pub fn branch_of(&self, ordinal: WriteTargetOrdinal) -> Option<IcebergWriteBranch> {
        self.targets
            .iter()
            .find(|target| target.ordinal() == ordinal)
            .map(IcebergSealedWriteTarget::branch)
    }

    /// The staging directory this session owns, used for recovery evidence.
    pub fn staging_dir(&self) -> String {
        format!(
            "{}/_staging/{}",
            self.table.data_location().trim_end_matches('/'),
            self.session_id
        )
    }

    pub fn state(&self) -> Result<IcebergWriteSessionState, ConnectorError> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| corrupt("Iceberg write session state lock is poisoned"))
    }

    /// Claim the single commit attempt this session allows.
    ///
    /// A second claim fails: exactly one external snapshot commit may be
    /// attempted per session, and a caller that already saw a terminal verdict
    /// must not silently start a second one.
    pub fn begin_commit(&self) -> Result<(), ConnectorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("Iceberg write session state lock is poisoned"))?;
        match &*state {
            IcebergWriteSessionState::Active => {
                *state = IcebergWriteSessionState::Committing;
                Ok(())
            }
            IcebergWriteSessionState::Committing => Err(invalid(
                "Iceberg write session already has a commit in flight",
            )),
            IcebergWriteSessionState::KnownCommitted { .. } => {
                Err(invalid("Iceberg write session is already known committed"))
            }
            IcebergWriteSessionState::Sealed => Err(invalid(
                "Iceberg write session is already sealed for its publication",
            )),
            IcebergWriteSessionState::KnownUncommitted { .. } => Err(invalid(
                "Iceberg write session is already known uncommitted",
            )),
            IcebergWriteSessionState::CommitUnknown { .. } => Err(invalid(
                "Iceberg write session outcome is unknown and requires reconciliation",
            )),
        }
    }

    pub(crate) fn retain_finalization(
        &self,
        result: novarocks_spi::connector::ExternalMutationFinalization,
    ) {
        *self
            .terminal_finalization
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = result;
    }

    pub(crate) fn terminal_finalization(
        &self,
    ) -> novarocks_spi::connector::ExternalMutationFinalization {
        self.terminal_finalization
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn recovery_evidence_slot(
        &self,
    ) -> Arc<std::sync::Mutex<Option<novarocks_spi::connector::ExternalMutationEvidence>>> {
        Arc::clone(&self.recovery_evidence)
    }

    pub(crate) fn retain_recovery_evidence(
        &self,
        evidence: novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<(), ConnectorError> {
        if evidence.operation_id().to_bytes() != self.session_id.to_bytes() {
            return Err(invalid(
                "Iceberg recovery evidence names a different session",
            ));
        }
        *self
            .recovery_evidence
            .lock()
            .map_err(|_| invalid("Iceberg recovery evidence state poisoned"))? = Some(evidence);
        Ok(())
    }

    pub(crate) fn recovery_evidence(
        &self,
    ) -> Result<novarocks_spi::connector::ExternalMutationEvidence, ConnectorError> {
        self.recovery_evidence
            .lock()
            .map_err(|_| invalid("Iceberg recovery evidence state poisoned"))?
            .clone()
            .ok_or_else(|| {
                invalid("Iceberg unknown session has no complete checked recovery evidence")
            })
    }

    pub fn settle(&self, terminal: IcebergWriteSessionState) -> Result<(), ConnectorError> {
        if matches!(
            terminal,
            IcebergWriteSessionState::Active | IcebergWriteSessionState::Committing
        ) {
            return Err(invalid(
                "Iceberg write session cannot settle into a non-terminal state",
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("Iceberg write session state lock is poisoned"))?;
        *state = terminal;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::write_stack::test_support::{
        dv_artifact, sample_metrics, sample_partition, table_facts,
    };

    #[test]
    fn every_flavor_maps_to_one_commit_op_and_owns_its_branches() {
        for flavor in [
            IcebergWriteFlavor::Append,
            IcebergWriteFlavor::Overwrite,
            IcebergWriteFlavor::PartitionOverwrite,
            IcebergWriteFlavor::RowMutationPositionDelete,
            IcebergWriteFlavor::RowMutationDeletionVector,
            IcebergWriteFlavor::RowMutationCopyOnWrite,
            IcebergWriteFlavor::StagedCreate,
            IcebergWriteFlavor::ManagedPublication,
            IcebergWriteFlavor::DistributedRewrite,
            IcebergWriteFlavor::TableMaintenance,
        ] {
            let branches = flavor.branches();
            assert!(!branches.is_empty(), "{} has no branch", flavor.as_str());
            assert_eq!(
                branches[0],
                IcebergWriteBranch::Data,
                "{} must open its data branch first",
                flavor.as_str()
            );
            // A commit op is a total function of the flavor; calling it must
            // never panic and must never be inferred from a fragment.
            let _ = flavor.commit_op_kind();
            let _ = flavor.connector_intent();
            assert!(flavor.accepts_empty_prepared_set());
        }
        assert_eq!(
            IcebergWriteFlavor::RowMutationDeletionVector.branches(),
            &[IcebergWriteBranch::Data, IcebergWriteBranch::DeletionVector]
        );
        // Two flavors deliberately own no data branch: they write delete
        // artifacts and nothing else, so sealing one would give SQL somewhere
        // to send rows the statement never produces.
        for (flavor, branches) in [
            (
                IcebergWriteFlavor::EqualityDelete,
                &[IcebergWriteBranch::EqualityDelete][..],
            ),
            (
                IcebergWriteFlavor::DistributedRewritePositionDeletes,
                &[IcebergWriteBranch::DeletionVector][..],
            ),
        ] {
            assert_eq!(flavor.branches(), branches, "{}", flavor.as_str());
            let _ = flavor.commit_op_kind();
            let _ = flavor.connector_intent();
            assert!(flavor.accepts_empty_prepared_set());
        }
    }

    #[test]
    fn a_location_that_embeds_a_credential_is_rejected() {
        assert!(validate_location("table location", "s3://bucket/warehouse").is_ok());
        assert!(validate_location("table location", "").is_err());
        assert!(validate_location("table location", "s3://a\0b").is_err());
        assert!(validate_location("table location", "s3://key:secret@bucket/x").is_err());
        assert!(
            validate_location("table location", "s3://bucket/x?session_token=abc").is_err(),
            "a query-string credential must not survive into a handle"
        );
    }

    #[test]
    fn artifact_metrics_reject_a_zero_size_or_unsorted_split_offsets() {
        assert!(IcebergArtifactMetrics::try_new(1, 0, Vec::new(), None).is_err());
        assert!(IcebergArtifactMetrics::try_new(1, 100, vec![0, 40, 80], None).is_ok());
        assert!(IcebergArtifactMetrics::try_new(1, 100, vec![0, 80, 40], None).is_err());
        assert!(IcebergArtifactMetrics::try_new(1, 100, vec![0, 100], None).is_err());
        assert!(IcebergArtifactMetrics::try_new(1, 100, vec![-1], None).is_err());
    }

    #[test]
    fn a_deletion_vector_artifact_must_agree_with_its_own_blob_range() {
        assert!(dv_artifact("s3://b/dv.puffin", "s3://b/a.parquet", 3, 1024, 0, 64).is_ok());
        assert_eq!(
            dv_artifact("s3://b/dv.puffin", "s3://b/a.parquet", 0, 1024, 0, 64)
                .expect_err("empty vector")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            dv_artifact("s3://b/dv.puffin", "s3://b/a.parquet", 3, 32, 0, 64)
                .expect_err("blob past end of file")
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn a_deletion_vector_cardinality_must_equal_its_record_count() {
        let partition = sample_partition();
        let metrics = sample_metrics(3, 1024);
        let error = IcebergDeletionVectorArtifact::try_new(
            "s3://b/dv.puffin".to_string(),
            partition,
            metrics,
            "s3://b/a.parquet".to_string(),
            IcebergContentRange::try_new(0, 64).expect("range"),
            4,
            Vec::new(),
        )
        .expect_err("cardinality disagreement");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn merged_old_references_must_be_sorted_and_unique() {
        let build = |references: Vec<EntryIdentity>| {
            IcebergPositionDeleteFileArtifact::try_new(
                "s3://b/d.parquet".to_string(),
                sample_partition(),
                sample_metrics(2, 512),
                "s3://b/a.parquet".to_string(),
                references,
            )
        };
        assert!(
            build(vec![
                EntryIdentity::DeleteFile {
                    path: "s3://b/1".to_string()
                },
                EntryIdentity::DeleteFile {
                    path: "s3://b/2".to_string()
                }
            ])
            .is_ok()
        );
        assert!(
            build(vec![
                EntryIdentity::DeleteFile {
                    path: "s3://b/2".to_string()
                },
                EntryIdentity::DeleteFile {
                    path: "s3://b/1".to_string()
                }
            ])
            .is_err()
        );
        assert!(
            build(vec![
                EntryIdentity::DeleteFile {
                    path: "s3://b/1".to_string()
                },
                EntryIdentity::DeleteFile {
                    path: "s3://b/1".to_string()
                }
            ])
            .is_err()
        );
    }

    #[test]
    fn merged_vector_identity_keeps_range_and_referenced_data_file() {
        let first = EntryIdentity::DeletionVector {
            path: "s3://b/shared.puffin".to_string(),
            offset: 4,
            length: 64,
            referenced_data_file: "s3://b/a.parquet".to_string(),
        };
        let mut second = first.clone();
        if let EntryIdentity::DeletionVector { offset, .. } = &mut second {
            *offset = 80;
        }
        assert!(validate_merged_references(&[first.clone(), second.clone()]).is_ok());
        assert!(validate_merged_references(&[first.clone(), first.clone()]).is_err());
        assert!(
            validate_merged_references(&[EntryIdentity::DataFile {
                path: "s3://b/a.parquet".to_string()
            }])
            .is_err()
        );
        if let EntryIdentity::DeletionVector { length, .. } = &mut second {
            *length = 0;
        }
        assert!(validate_merged_references(&[first, second]).is_err());
    }

    #[test]
    fn a_session_seals_dense_ordinals_and_refuses_a_repeated_branch() {
        let target = |ordinal: u32, branch| {
            IcebergSealedWriteTarget::new(
                WriteTargetOrdinal::try_new(ordinal).expect("ordinal"),
                branch,
                BTreeMap::new(),
            )
        };
        assert!(
            IcebergCommitHandle::try_new(
                IcebergWriteSessionId::new(),
                table_facts(),
                IcebergWriteFlavor::RowMutationDeletionVector,
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
                None,
                vec![
                    target(0, IcebergWriteBranch::Data),
                    target(1, IcebergWriteBranch::DeletionVector),
                ],
            )
            .is_ok()
        );
        assert!(
            IcebergCommitHandle::try_new(
                IcebergWriteSessionId::new(),
                table_facts(),
                IcebergWriteFlavor::RowMutationDeletionVector,
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
                None,
                vec![
                    target(0, IcebergWriteBranch::Data),
                    target(1, IcebergWriteBranch::Data),
                ],
            )
            .is_err(),
            "a repeated branch makes the delete route ambiguous"
        );
        assert!(
            IcebergCommitHandle::try_new(
                IcebergWriteSessionId::new(),
                table_facts(),
                IcebergWriteFlavor::Append,
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
                None,
                vec![
                    target(0, IcebergWriteBranch::Data),
                    target(1, IcebergWriteBranch::DeletionVector),
                ],
            )
            .is_err(),
            "an append flavor does not own a deletion-vector branch"
        );
        assert!(
            IcebergCommitHandle::try_new(
                IcebergWriteSessionId::new(),
                table_facts(),
                IcebergWriteFlavor::Append,
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
                None,
                vec![target(1, IcebergWriteBranch::Data)],
            )
            .is_err(),
            "ordinals must be dense from zero"
        );
    }

    #[test]
    fn a_session_allows_exactly_one_commit_attempt() {
        let handle = IcebergCommitHandle::try_new(
            IcebergWriteSessionId::new(),
            table_facts(),
            IcebergWriteFlavor::Append,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
            None,
            vec![IcebergSealedWriteTarget::new(
                WriteTargetOrdinal::try_new(0).expect("ordinal"),
                IcebergWriteBranch::Data,
                BTreeMap::new(),
            )],
        )
        .expect("session");
        assert!(matches!(
            handle.state().expect("state"),
            IcebergWriteSessionState::Active
        ));
        handle.begin_commit().expect("first attempt");
        assert!(handle.begin_commit().is_err());
        handle
            .settle(IcebergWriteSessionState::KnownCommitted {
                snapshot_id: Some(7),
            })
            .expect("settle");
        assert!(handle.begin_commit().is_err());
        assert!(handle.settle(IcebergWriteSessionState::Active).is_err());
    }
}
