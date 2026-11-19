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

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};

use crate::iceberg::spec::{
    FormatVersion, ManifestContentType, ManifestFile, ManifestStatus, TableMetadata,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::source::ManifestSource;

use super::manifest_encoding::{ManifestWriteSource, SourcedManifestEntry};
use super::model::AddedContent;
use super::operation::IcebergCommitOperation;
use super::staging::{
    ManifestEntryWrite, StagedView, write_manifest_with_sources, write_sourced_manifest,
};
use super::write_stack::codec::CompactFragmentCodec;
use super::write_stack::control::publication::written_file_with_historical_partition;
use super::write_stack::receiving::compact::{AddedCounters, CompactEntry};
use super::write_stack::receiving::{AdditionGroup, ReceivedCompactFiles};

pub(crate) enum StageNewFiles {
    Pending(Arc<PendingNewFiles>),
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
}

fn invariant(message: impl Into<String>) -> Error {
    let message = message.into();
    Error::new(ErrorKind::Unexpected, message.clone())
        .with_source(ConnectorError::new(ConnectorErrorKind::Internal, message))
}

/// Ratio of actual length to the input's encoded lower bound. Compare
/// cross-products to keep threshold decisions independent of floating point.
#[derive(Clone, Copy)]
struct Calibration {
    actual: u64,
    lower: u64,
    observed: bool,
}

impl Default for Calibration {
    fn default() -> Self {
        Self {
            actual: 1,
            lower: 1,
            observed: false,
        }
    }
}

impl Calibration {
    fn reaches(self, lower: u64, target: u64) -> bool {
        u128::from(lower) * u128::from(self.actual) >= u128::from(target) * u128::from(self.lower)
    }

    fn observe(&mut self, actual: u64, lower: u64) -> Result<()> {
        if lower == 0 || actual < lower {
            return Err(invariant(
                "Iceberg manifest length is below its encoded lower bound",
            ));
        }
        if !self.observed
            || u128::from(actual) * u128::from(self.lower)
                < u128::from(self.actual) * u128::from(lower)
        {
            *self = Self {
                actual,
                lower,
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
        StageNewFiles::Written(written)
            if written.version == FormatVersion::V1 && version != FormatVersion::V1 =>
        {
            (upgrade_v1(view, snapshot_id, written).await?, true)
        }
        StageNewFiles::Written(written) => {
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
    historical: Option<&BTreeMap<i32, crate::iceberg::spec::StructType>>,
) -> Result<WrittenNewManifests> {
    let version = view.metadata().format_version();
    let policy = view
        .preparation()
        .merge_policy
        .map_err(|message| Error::new(ErrorKind::DataInvalid, message))?;
    let ReceivedCompactFiles {
        groups,
        author,
        counts,
        v2_targets,
    } = pending.received;
    drop(v2_targets);
    let total = counts
        .data
        .files
        .checked_add(counts.position_deletes.files)
        .and_then(|files| usize::try_from(files).ok())
        .ok_or_else(|| invariant("Compact manifest entry count overflow"))?;
    let mut manifests = Vec::new();
    for (group, mut entries) in groups {
        let AdditionGroup::Additions {
            spec: spec_id,
            deletes,
        } = group
        else {
            if historical.is_none() {
                return Err(invariant("Compact append received a delete effect"));
            }
            continue;
        };
        let partition_type = if deletes {
            if version == FormatVersion::V3 && pending.source.format_version() == FormatVersion::V2
            {
                return Err(invariant(
                    "A new V3 delete manifest cannot add V2 position-delete files",
                ));
            }
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
        let mut calibration = Calibration::default();
        while !entries.is_empty() {
            let mut lower = 0u64;
            let mut count = 0;
            for entry in &entries {
                let addition = entry.addition.as_ref().expect("grouped data addition");
                let bytes = u64::try_from(addition.encoded_lower_bound)
                    .map_err(|_| invariant("Iceberg manifest entry estimate overflow"))?;
                lower = lower
                    .checked_add(bytes)
                    .ok_or_else(|| invariant("Iceberg manifest estimate overflow"))?;
                count += 1;
                if calibration.reaches(lower, policy.target_bytes) {
                    break;
                }
            }
            let remaining = entries.split_off(count);
            let batch = std::mem::replace(&mut entries, remaining);
            let values = batch
                .iter()
                .map(|entry| {
                    view.artifacts().check_active()?;
                    decode_addition(
                        &pending.source,
                        &pending.codec,
                        author,
                        entry,
                        partition_type,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let descriptor = if let Some(partition_type) = partition_type {
                let source = Arc::new(ManifestWriteSource::for_position_output(
                    pending.source.current_schema().clone(),
                    spec.as_ref().clone(),
                    partition_type.clone(),
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
                    snapshot,
                    spec,
                    ManifestContentType::Deletes,
                    sourced,
                    &[],
                )
                .await?
            } else {
                let encoding = (
                    version,
                    snapshot,
                    pending.source.current_schema().clone(),
                    spec.as_ref().clone(),
                    ManifestContentType::Data,
                );
                write_manifest_with_sources(
                    view.artifacts(),
                    encoding,
                    values.into_iter().map(Ok),
                    &[],
                )
                .await?
            };
            let actual = u64::try_from(descriptor.manifest_length)
                .map_err(|_| invariant("Negative written manifest length"))?;
            calibration.observe(actual, lower)?;
            tracing::debug!(operation = ?view.artifacts().attempt_token().operation(), stage = view.stage_key(), snapshot,
                spec_id, entries = batch.len(), lower, actual, calibration_numerator = calibration.actual,
                calibration_denominator = calibration.lower, "Wrote rolled Iceberg manifest");
            manifests.push(descriptor);
            // The compact inputs and their receipts live through the actual write.
            drop(batch);
        }
    }
    Ok(WrittenNewManifests {
        manifests,
        entries: total,
        version,
        counts,
    })
}

fn decode_addition(
    source: &TableMetadata,
    codec: &CompactFragmentCodec,
    author: &ConnectorPayloadRetentionGuard,
    entry: &CompactEntry,
    historical: Option<&crate::iceberg::spec::StructType>,
) -> Result<ManifestEntryWrite> {
    let addition = entry.addition.as_ref().expect("grouped data addition");
    let (fragment, _decoded) = codec.decode(&addition.payload, author).map_err(|error| {
        Error::new(
            ErrorKind::Unexpected,
            "Could not decode paid compact addition",
        )
        .with_source(error)
    })?;
    let written = written_file_with_historical_partition(&fragment, source, historical)
        .map_err(|error| Error::new(ErrorKind::DataInvalid, error.message()).with_source(error))?;
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
    drop(fragment);
    let file = super::data_file::into_written_file(written)?;
    Ok(ManifestEntryWrite::Added(AddedContent::new_logical_data(
        file,
        addition.partition_spec_id,
    )?))
}

async fn upgrade_v1(
    view: &StagedView<'_>,
    snapshot: i64,
    mut written: WrittenNewManifests,
) -> Result<WrittenNewManifests> {
    let version = view.metadata().format_version();
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
            // V1's decoded zero is physical format behavior. Reauthor the
            // original addition with inheritance for the winning publication.
            Ok(ManifestEntryWrite::Added(AddedContent::new_logical_data(
                entry.data_file,
                prior.partition_spec_id,
            )?))
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
