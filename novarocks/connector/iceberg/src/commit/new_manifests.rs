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

//! Operation-owned additions and their reusable manifest descriptors.

// Design: ADR-0239 (docs/adr/ADR-0239-iceberg-compact-receiving-and-rolling-manifests.md)

use std::alloc::Layout;
use std::collections::{BTreeMap, LinkedList};
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};

use crate::iceberg::spec::{
    FormatVersion, ManifestContentType, ManifestFile, ManifestStatus, PartitionSpec, StructType,
    TableMetadata,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::source::ManifestSource;

use super::manifest_encoding::{ManifestWriteSource, SourcedManifestEntry};
use super::model::{AddedContent, CommitOperation};
use super::operation::IcebergCommitOperation;
use super::staging::{
    ManifestEntryWrite, StagedView, write_manifest_with_sources, write_sourced_manifest,
};
use super::write_stack::codec::CompactFragmentCodec;
use super::write_stack::control::publication::{
    rewrite_file_backing, written_file_with_historical_partition,
};
use super::write_stack::domain::IcebergCommitFragment;
use super::write_stack::receiving::compact::{
    AddedCounters, AdditionKind, CompactAddition, CompactEntry,
};
use super::write_stack::receiving::{AdditionGroup, ReceivedCompactFiles};

mod registered;
pub(crate) use registered::RegisteredPaths;
pub(in crate::commit) use registered::decoded_file_clone_bytes;
pub(crate) use registered::{RegisteredFile, RegisteredNewFiles};

pub(crate) enum StageNewFiles {
    Pending(Arc<PendingNewFiles>),
    Registered(RegisteredNewFiles),
    Writing,
    Written(WrittenNewManifests),
}

pub(crate) struct PendingNewFiles {
    pub(crate) received: ReceivedCompactFiles,
    pub(crate) source: Arc<TableMetadata>,
    pub(crate) codec: CompactFragmentCodec,
}

pub(crate) struct WrittenNewManifests {
    pub(crate) manifests: Vec<ManifestFile>,
    pub(crate) entries: usize,
    version: FormatVersion,
    counts: AddedCounters,
    registered: bool,
}

impl WrittenNewManifests {
    pub(super) fn is_registered(&self) -> bool {
        self.registered
    }
}

fn invariant(message: impl Into<String>) -> Error {
    let message = message.into();
    Error::new(ErrorKind::Unexpected, message.clone())
        .with_source(ConnectorError::new(ConnectorErrorKind::Internal, message))
}

pub(crate) fn commit_validation_error(message: impl Into<String>) -> Error {
    commit_validation_error_for(CommitOperation::Write, message)
}

pub(super) fn commit_validation_error_for(
    operation: CommitOperation,
    message: impl Into<String>,
) -> Error {
    let message = message.into();
    let failure = super::conflict::validation_conflict(operation, message.clone());
    Error::new(ErrorKind::DataInvalid, message).with_source(failure)
}

pub(crate) fn validate_position_format(source: FormatVersion, base: FormatVersion) -> Result<()> {
    if source == FormatVersion::V2 && base >= FormatVersion::V3 {
        return Err(commit_validation_error(format!(
            "Must use DVs for position deletes in V{}: writer used V2 position deletes",
            base as u8,
        )));
    }
    Ok(())
}

/// Keep the soft rolling estimate separate from a proven encoded minimum.
#[derive(Clone, Copy, Default)]
struct EntrySize {
    lower: u64,
    layout: u64,
}

impl EntrySize {
    fn checked_add(self, entry: Self) -> Result<Self> {
        let overflow = || invariant("Iceberg manifest estimate overflow");
        Ok(Self {
            lower: self.lower.checked_add(entry.lower).ok_or_else(overflow)?,
            layout: self.layout.checked_add(entry.layout).ok_or_else(overflow)?,
        })
    }
}

/// Ratio of actual length to the input estimate. Compare cross-products to
/// keep threshold decisions independent of floating point.
#[derive(Clone, Copy)]
struct Calibration {
    actual: u64,
    layout: u64,
    observed: bool,
}

impl Default for Calibration {
    fn default() -> Self {
        Self {
            actual: 1,
            layout: 1,
            observed: false,
        }
    }
}

impl Calibration {
    fn reaches(self, layout: u64, target: u64) -> bool {
        u128::from(layout) * u128::from(self.actual) >= u128::from(target) * u128::from(self.layout)
    }

    fn observe(&mut self, actual: u64, size: EntrySize) -> Result<()> {
        if size.lower == 0 || actual < size.lower {
            return Err(invariant(
                "Iceberg manifest length is below its encoded lower bound",
            ));
        }
        if !self.observed
            || u128::from(actual) * u128::from(self.layout)
                < u128::from(self.actual) * u128::from(size.layout)
        {
            *self = Self {
                actual,
                layout: size.layout,
                observed: true,
            };
        }
        Ok(())
    }
}

pub(crate) async fn prepare_added_manifests(
    operation: &IcebergCommitOperation,
    view: &StagedView<'_>,
    stage: usize,
    snapshot_id: i64,
    historical: Option<&BTreeMap<i32, crate::iceberg::spec::StructType>>,
) -> Result<(Vec<ManifestFile>, AddedCounters)> {
    if view.stage_key() != stage
        || view.artifacts().attempt_token().operation() != operation.token()
    {
        return Err(invariant(
            "Iceberg compact stage belongs to another preparation",
        ));
    }
    let version = view.metadata().format_version();
    let (written, verify_coverage) = match operation.take_stage_new_files(stage)? {
        StageNewFiles::Pending(pending) => {
            let pending = Arc::try_unwrap(pending)
                .map_err(|_| invariant("Iceberg compact stage still has an observer"))?;
            (
                write_pending(view, snapshot_id, pending, historical).await?,
                true,
            )
        }
        StageNewFiles::Registered(registered) => {
            if historical.is_some() {
                return Err(invariant(
                    "Iceberg registered stage can only prepare an append",
                ));
            }
            (write_registered(view, snapshot_id, registered).await?, true)
        }
        StageNewFiles::Written(written)
            if written.version == FormatVersion::V1 && version != FormatVersion::V1 =>
        {
            (upgrade_v1(view, snapshot_id, written).await?, true)
        }
        StageNewFiles::Written(written) => {
            if written.counts.position_deletes.files != 0 {
                validate_position_format(written.version, version)?;
            }
            tracing::debug!(operation = ?operation.token(), stage, snapshot_id,
                manifests = written.manifests.len(), "Reusing new Iceberg manifests");
            (written, false)
        }
        StageNewFiles::Writing => {
            return Err(invariant(
                "Iceberg compact stage preparation was already claimed",
            ));
        }
    };
    let descriptors = written.manifests.clone();
    let counts = written.counts;
    operation.finish_stage_new_files(stage, snapshot_id, written, verify_coverage)?;
    Ok((descriptors, counts))
}

async fn write_pending(
    view: &StagedView<'_>,
    snapshot: i64,
    pending: PendingNewFiles,
    historical: Option<&BTreeMap<i32, StructType>>,
) -> Result<WrittenNewManifests> {
    let version = view.metadata().format_version();
    let rewrite_sequence = view.rewrite_source_sequence()?;
    let target_bytes = manifest_target_bytes(view)?;
    let partition_replace = view
        .partition_replace()
        .map(|selection| {
            crate::typed_boundary::ExactMvTargetPartitions::check(&pending.source, selection)
        })
        .transpose()
        .map_err(project_failure)?;
    let ReceivedCompactFiles {
        groups,
        author,
        counts,
        v2_targets,
    } = pending.received;
    drop(v2_targets);
    if counts.position_deletes.files != 0 {
        validate_position_format(pending.source.format_version(), version)?;
    }
    let total = counts
        .data
        .files
        .checked_add(counts.position_deletes.files)
        .and_then(|files| files.checked_add(counts.equality_deletes.files))
        .and_then(|files| usize::try_from(files).ok())
        .ok_or_else(|| invariant("Compact manifest entry count overflow"))?;
    let mut manifests = Vec::new();
    for (group, entries) in groups {
        let AdditionGroup::Additions {
            spec: spec_id,
            kind,
        } = group
        else {
            if historical.is_none() {
                return Err(invariant("Compact append received a delete effect"));
            }
            continue;
        };
        let partition_type = if kind == AdditionKind::PositionDelete {
            Some(
                historical
                    .and_then(|partitions| partitions.get(&spec_id))
                    .ok_or_else(|| {
                        invariant("Compact position output lacks its historical partition type")
                    })?,
            )
        } else {
            None
        };
        let author = author
            .as_ref()
            .ok_or_else(|| invariant("Compact additions have no original byte author"))?;
        let spec = pending
            .source
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| invariant("Compact addition lost its admitted partition spec"))?;
        let input = RollingInput::Compact {
            entries,
            codec: &pending.codec,
            author,
            partition_type,
            rewrite_sequence,
        };
        let output = RollingOutput {
            view,
            source: &pending.source,
            snapshot,
            spec,
            content: kind.manifest_content(),
            partition_replace: partition_replace.as_ref(),
        };
        write_rolled(input, &output, target_bytes, &mut manifests).await?;
    }
    Ok(WrittenNewManifests {
        manifests,
        entries: total,
        version,
        counts,
        registered: false,
    })
}

async fn write_registered(
    view: &StagedView<'_>,
    snapshot: i64,
    mut registered: RegisteredNewFiles,
) -> Result<WrittenNewManifests> {
    if view.partition_replace().is_some() {
        return Err(invariant(
            "Partition replacement must use its original compact additions",
        ));
    }
    let counts = registered.counts();
    let source = &registered.source;
    let spec = source.default_partition_spec();
    if view.metadata().uuid() != source.uuid()
        || view.metadata().partition_spec_by_id(spec.spec_id()) != Some(spec)
    {
        return Err(invariant(
            "Iceberg registration lost its admitted table or partition spec",
        ));
    }
    let total = registered.entries.len();
    let input = RollingInput::Registered(std::mem::take(&mut registered.entries).into_iter());
    let output = RollingOutput {
        view,
        source,
        snapshot,
        spec,
        content: ManifestContentType::Data,
        partition_replace: None,
    };
    let mut manifests = Vec::new();
    // RegisteredNewFiles still holds the vector's capacity receipt. It
    // survives even as the writer retires each consumed prefix.
    write_rolled(input, &output, manifest_target_bytes(view)?, &mut manifests).await?;
    Ok(WrittenNewManifests {
        manifests,
        entries: total,
        version: view.metadata().format_version(),
        counts,
        registered: true,
    })
}

fn manifest_target_bytes(view: &StagedView<'_>) -> Result<u64> {
    view.preparation()
        .merge_policy
        .map(|policy| policy.target_bytes)
        .map_err(|message| Error::new(ErrorKind::DataInvalid, message))
}

enum RollingInput<'a> {
    Compact {
        entries: LinkedList<CompactEntry>,
        codec: &'a CompactFragmentCodec,
        author: &'a ConnectorPayloadRetentionGuard,
        partition_type: Option<&'a StructType>,
        rewrite_sequence: Option<i64>,
    },
    Registered(std::vec::IntoIter<super::RegisteredFile>),
}

struct RegisteredBatch<'a> {
    entries: std::slice::IterMut<'a, super::RegisteredFile>,
    spec_id: i32,
}

impl Iterator for RegisteredBatch<'_> {
    type Item = Result<ManifestEntryWrite>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.entries.next()?;
        Some(entry.take_for_manifest(self.spec_id))
    }
}

struct RollingOutput<'a, 'view> {
    view: &'a StagedView<'view>,
    source: &'a TableMetadata,
    snapshot: i64,
    spec: &'a PartitionSpec,
    content: ManifestContentType,
    partition_replace: Option<&'a crate::typed_boundary::ExactMvTargetPartitions<'a>>,
}

impl RollingInput<'_> {
    fn next_batch(&self, calibration: Calibration, target: u64) -> Result<(usize, EntrySize)> {
        match self {
            Self::Compact { entries, .. } => batch_size(
                entries.iter().map(|entry| {
                    let addition = entry.addition.as_ref().expect("grouped data addition");
                    let lower = u64::try_from(addition.encoded_lower_bound)
                        .map_err(|_| invariant("Iceberg manifest entry estimate overflow"))?;
                    Ok(EntrySize {
                        lower,
                        layout: lower,
                    })
                }),
                calibration,
                target,
            ),
            Self::Registered(entries) => batch_size(
                entries.as_slice().iter().map(|entry| Ok(entry.size)),
                calibration,
                target,
            ),
        }
    }

    async fn write_batch(
        &mut self,
        count: usize,
        output: &RollingOutput<'_, '_>,
    ) -> Result<ManifestFile> {
        let view = output.view;
        match self {
            Self::Compact {
                entries,
                codec,
                author,
                partition_type,
                rewrite_sequence,
            } => {
                let remaining = entries.split_off(count);
                let batch = std::mem::replace(entries, remaining);
                let descriptor = if let Some(sequence) = *rewrite_sequence {
                    if output.content != ManifestContentType::Data {
                        return Err(invariant(
                            "Iceberg rewrite output must use its frozen data partition",
                        ));
                    }
                    // The manifest list allocates a new range from the table watermark.
                    output
                        .write_rewrite_data(&batch, codec, author, sequence)
                        .await?
                } else {
                    let values = batch
                        .iter()
                        .map(|entry| {
                            view.artifacts().check_active()?;
                            decode_addition(
                                output.source,
                                codec,
                                author,
                                entry,
                                *partition_type,
                                output.partition_replace,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if let Some(partition_type) = partition_type {
                        let source = Arc::new(ManifestWriteSource::for_position_output(
                            output.source.current_schema().clone(),
                            output.spec.clone(),
                            (*partition_type).clone(),
                        ));
                        let sourced = values
                            .into_iter()
                            .map(|entry| SourcedManifestEntry {
                                source: Arc::clone(&source),
                                entry,
                            })
                            .collect();
                        write_sourced_manifest(
                            view.artifacts(),
                            view.metadata(),
                            output.snapshot,
                            output.spec,
                            ManifestContentType::Deletes,
                            sourced,
                            &[],
                        )
                        .await?
                    } else {
                        output
                            .write_current_partition(values.into_iter().map(Ok))
                            .await?
                    }
                };
                // Compact inputs and their receipts survive the actual write.
                drop(batch);
                Ok(descriptor)
            }
            Self::Registered(entries) => {
                let batch = RegisteredBatch {
                    entries: entries.as_mut_slice()[..count].iter_mut(),
                    spec_id: output.spec.spec_id(),
                };
                let descriptor = output.write_current_partition(batch).await?;
                // The SDK consumed each moved file. Its receipt and any
                // remaining unused slot are retired with the original prefix.
                entries.by_ref().take(count).for_each(drop);
                Ok(descriptor)
            }
        }
    }
}

impl RollingOutput<'_, '_> {
    async fn write_rewrite_data(
        &self,
        batch: &LinkedList<CompactEntry>,
        codec: &CompactFragmentCodec,
        author: &ConnectorPayloadRetentionGuard,
        sequence: i64,
    ) -> Result<ManifestFile> {
        let slots = Layout::array::<PaidRewriteFile>(batch.len()).map_err(rewrite_resource)?;
        let _slots = reserve_rewrite_backing(author, slots.size())?;
        let mut files = Vec::new();
        files
            .try_reserve_exact(batch.len())
            .map_err(rewrite_resource)?;
        let sequence =
            (self.view.metadata().format_version() != FormatVersion::V1).then_some(sequence);
        let mut partition_bytes = 0;
        for entry in batch {
            self.view.artifacts().check_active()?;
            let addition = entry.addition.as_ref().expect("grouped data addition");
            let (fragment, decoded) = decode_compact_file(codec, author, addition)?;
            let backing = rewrite_file_backing(&fragment, self.source).map_err(project_failure)?;
            // The decoded source stays paid while the projection creates its
            // owned path, partition, metric maps and bound values.
            let file_receipt = reserve_rewrite_backing(author, backing.file)?;
            let written = written_file_with_historical_partition(
                &fragment,
                self.source,
                Some(self.source.default_partition_type()),
            )
            .map_err(project_failure)?;
            drop(fragment);
            drop(decoded);

            // DataFileBuilder::build clones its owned fields. That copy and
            // the builder coexist until finish_addition returns.
            let builder_receipt = reserve_rewrite_backing(author, backing.file)?;
            let entry = finish_addition(written, self.source, addition, None, sequence)?;
            drop(builder_receipt);
            files.push(PaidRewriteFile {
                entry: Some(entry),
                _file: file_receipt,
            });
            partition_bytes = partition_bytes.max(backing.partition);
        }
        // The fixed manifest encoder validates one projected partition at a
        // time, while the complete batch of file values is still live.
        let _partition = (partition_bytes != 0)
            .then(|| reserve_rewrite_backing(author, partition_bytes))
            .transpose()?;
        let batch = PaidRewriteBatch(files.iter_mut());
        let result = self.write_current_partition(batch).await;
        // The iterator moves each DataFile into the SDK's writer. Its receipt
        // remains here until the writer completes or its future has exited.
        drop(files);
        result
    }

    async fn write_current_partition(
        &self,
        entries: impl Iterator<Item = Result<ManifestEntryWrite>>,
    ) -> Result<ManifestFile> {
        let encoding = (
            self.view.metadata().format_version(),
            self.snapshot,
            self.source.current_schema().clone(),
            self.spec.clone(),
            self.content,
        );
        write_manifest_with_sources(self.view.artifacts(), encoding, entries, &[]).await
    }
}

fn batch_size(
    entries: impl Iterator<Item = Result<EntrySize>>,
    calibration: Calibration,
    target: u64,
) -> Result<(usize, EntrySize)> {
    let mut size = EntrySize::default();
    let mut count = 0;
    for entry in entries {
        size = size.checked_add(entry?)?;
        count += 1;
        if calibration.reaches(size.layout, target) {
            break;
        }
    }
    Ok((count, size))
}

async fn write_rolled(
    mut input: RollingInput<'_>,
    output: &RollingOutput<'_, '_>,
    target: u64,
    manifests: &mut Vec<ManifestFile>,
) -> Result<()> {
    let mut calibration = Calibration::default();
    loop {
        let (entries, size) = input.next_batch(calibration, target)?;
        if entries == 0 {
            return Ok(());
        }
        let descriptor = input.write_batch(entries, output).await?;
        let actual = u64::try_from(descriptor.manifest_length)
            .map_err(|_| invariant("Negative written manifest length"))?;
        calibration.observe(actual, size)?;
        tracing::debug!(
            operation = ?output.view.artifacts().attempt_token().operation(), stage = output.view.stage_key(),
            snapshot = output.snapshot, spec_id = output.spec.spec_id(), entries,
            lower = size.lower, layout = size.layout, actual, calibration_numerator = calibration.actual,
            calibration_denominator = calibration.layout, "Wrote rolled Iceberg manifest"
        );
        manifests.push(descriptor);
    }
}

struct PaidRewriteFile {
    entry: Option<ManifestEntryWrite>,
    _file: ConnectorPayloadRetentionGuard,
}

struct PaidRewriteBatch<'a>(std::slice::IterMut<'a, PaidRewriteFile>);

impl Iterator for PaidRewriteBatch<'_> {
    type Item = Result<ManifestEntryWrite>;

    fn next(&mut self) -> Option<Self::Item> {
        let file = self.0.next()?;
        Some(Ok(file
            .entry
            .take()
            .expect("rewrite manifest entry is written once")))
    }
}

fn rewrite_resource(error: impl std::fmt::Display) -> Error {
    project_failure(ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        error.to_string(),
    ))
}

fn project_failure(error: ConnectorError) -> Error {
    Error::new(ErrorKind::DataInvalid, error.message()).with_source(error)
}

fn reserve_rewrite_backing(
    author: &ConnectorPayloadRetentionGuard,
    bytes: usize,
) -> Result<ConnectorPayloadRetentionGuard> {
    author.reserve_owned_bytes(bytes).map_err(project_failure)
}

fn decode_compact_file(
    codec: &CompactFragmentCodec,
    author: &ConnectorPayloadRetentionGuard,
    addition: &CompactAddition,
) -> Result<(IcebergCommitFragment, ConnectorPayloadRetentionGuard)> {
    codec.decode(&addition.payload, author).map_err(|error| {
        Error::new(
            ErrorKind::Unexpected,
            "Could not decode paid compact addition",
        )
        .with_source(error)
    })
}

fn decode_addition(
    source: &TableMetadata,
    codec: &CompactFragmentCodec,
    author: &ConnectorPayloadRetentionGuard,
    entry: &CompactEntry,
    historical: Option<&StructType>,
    partition_replace: Option<&crate::typed_boundary::ExactMvTargetPartitions<'_>>,
) -> Result<ManifestEntryWrite> {
    let addition = entry.addition.as_ref().expect("grouped data addition");
    let (fragment, _decoded) = decode_compact_file(codec, author, addition)?;
    let written = written_file_with_historical_partition(&fragment, source, historical)
        .map_err(project_failure)?;
    if let Some(selection) = partition_replace
        && (written.content != crate::iceberg::spec::DataContentType::Data
            || selection.matches(written.partition_spec_id, &written.partition_values)
                != Some(true))
    {
        return Err(commit_validation_error_for(
            CommitOperation::RefreshMaterializedView,
            "Partition replacement appended a file outside the exact selection",
        ));
    }
    drop(fragment);
    finish_addition(written, source, addition, historical, None)
}

fn finish_addition(
    written: super::WrittenFile,
    source: &TableMetadata,
    addition: &CompactAddition,
    historical: Option<&StructType>,
    rewrite_sequence: Option<i64>,
) -> Result<ManifestEntryWrite> {
    if written.path != addition.object_path.as_ref()
        || written.partition_spec_id != addition.partition_spec_id
    {
        return Err(invariant(
            "Decoded compact addition differs from its admitted identity",
        ));
    }
    if historical.is_some()
        && (written.content != crate::iceberg::spec::DataContentType::PositionDeletes
            || !written.lower_bounds.is_empty()
            || !written.upper_bounds.is_empty())
    {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "Historical position output cannot carry table-column bounds",
        ));
    }
    let file = super::data_file::into_written_file(written)?;
    let added = match rewrite_sequence {
        Some(sequence) => {
            use super::model::SeqField;
            let expected = if source.format_version() == FormatVersion::V1 {
                SeqField::Inherit
            } else {
                SeqField::Explicit(sequence)
            };
            if addition.data_sequence != expected {
                return Err(invariant(
                    "Iceberg compact rewrite changed its source sequence",
                ));
            }
            AddedContent::rewritten_data(file, addition.partition_spec_id, sequence)
        }
        None => AddedContent::new_logical_data(file, addition.partition_spec_id),
    }?;
    Ok(ManifestEntryWrite::Added(added))
}

async fn upgrade_v1(
    view: &StagedView<'_>,
    snapshot: i64,
    mut written: WrittenNewManifests,
) -> Result<WrittenNewManifests> {
    let version = view.metadata().format_version();
    let rewrite_sequence = view.rewrite_source_sequence()?;
    let mut rewritten = Vec::with_capacity(written.manifests.len());
    for prior in written.manifests {
        view.artifacts().check_active()?;
        let source = ManifestSource::pending(snapshot, &prior);
        let raw = source.load_sized(view.artifacts().file_io()).await?;
        source.check_header(raw.metadata())?;
        if raw.metadata().format_version != FormatVersion::V1
            || prior.content != ManifestContentType::Data
        {
            return Err(invariant("Expected an operation-owned V1 data manifest"));
        }
        let (entries, header) = raw.into_parts();
        let entries = entries.into_iter().map(|entry| {
            if entry.status() != ManifestStatus::Added || entry.snapshot_id() != Some(snapshot) {
                return Err(invariant(
                    "A pending V1 manifest contains a non-stage entry",
                ));
            }
            let entry = Arc::unwrap_or_clone(entry);
            // The V1 representation has no explicit age field. OPTIMIZE
            // restores its frozen E sequence when the winning format has one.
            let added = match rewrite_sequence {
                Some(sequence) => {
                    AddedContent::rewritten_data(entry.data_file, prior.partition_spec_id, sequence)
                }
                None => AddedContent::new_logical_data(entry.data_file, prior.partition_spec_id),
            }?;
            Ok(ManifestEntryWrite::Added(added))
        });
        let encoding = (
            version,
            snapshot,
            header.schema,
            header.partition_spec,
            prior.content,
        );
        rewritten.push(
            write_manifest_with_sources(
                view.artifacts(),
                encoding,
                entries,
                std::slice::from_ref(&prior.manifest_path),
            )
            .await?,
        );
    }
    written.manifests = rewritten;
    written.version = version;
    Ok(written)
}

#[cfg(test)]
pub(in crate::commit) mod tests;
