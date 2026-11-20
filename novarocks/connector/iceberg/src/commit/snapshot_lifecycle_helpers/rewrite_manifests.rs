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

//! Rewrite manifest layout in one `operation=replace` snapshot. Explicit
//! EXECUTE clusters and rolls data manifests. Legacy callers merge
//! manifests of the same spec and content.
//!
//! Spec: docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §5.
//!
//! Key properties:
//! * snapshot.sequence_number = last_sequence_number + 1 (catalog invariant —
//!   iceberg-rs strictly increases snapshot seq per commit). The per-entry
//!   data_sequence_number / file_sequence_number inside merged manifests are
//!   preserved unchanged.
//! * v3 row-lineage fields (first_row_id, referenced_data_file, etc.) round-trip via
//!   ManifestEntry's public fields
//! * DELETED entries are dropped from merged manifests
//! * ADDED + EXISTING entries become EXISTING in the merged manifest

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

use super::super::manifest_encoding::{
    ManifestWriteSource, SourcedManifestEntry, descriptor_owned_bytes, encoding_basis,
};
use super::super::model::ManifestRewriteReuse;
use super::super::model::{ArtifactWriter, FrozenEntry};
use super::super::staging::{ManifestEntryWrite, RollingDataManifest, write_sourced_manifest};
use crate::iceberg::spec::{
    DataFile, Literal, ManifestContentType, ManifestFile, Operation, PartitionSpec,
    PrimitiveLiteral, PrimitiveType, Snapshot, Summary, TableMetadata, Type,
};
use crate::iceberg::{TableRequirement, TableUpdate};
#[cfg(test)]
use crate::iceberg::{
    io::FileIO,
    spec::{DataFileBuilder, FormatVersion, ManifestEntry, ManifestStatus, ManifestWriterBuilder},
};
use crate::manifest::source::ManifestSource;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorManifestRewriteMetrics, ConnectorRequestContext,
};

fn finalize_snapshot_summary(
    mut properties: HashMap<String, String>,
    previous: Option<&Summary>,
) -> Result<HashMap<String, String>, crate::iceberg::Error> {
    const TOTALS: [(&str, &str, &str); 6] = [
        ("total-data-files", "added-data-files", "deleted-data-files"),
        (
            "total-delete-files",
            "added-delete-files",
            "removed-delete-files",
        ),
        ("total-records", "added-records", "deleted-records"),
        ("total-files-size", "added-files-size", "removed-files-size"),
        (
            "total-position-deletes",
            "added-position-deletes",
            "removed-position-deletes",
        ),
        (
            "total-equality-deletes",
            "added-equality-deletes",
            "removed-equality-deletes",
        ),
    ];
    for (total, added, removed) in TOTALS {
        let base = match previous {
            None => 0,
            Some(summary) => match summary
                .additional_properties
                .get(total)
                .and_then(|value| value.parse::<u64>().ok())
            {
                Some(value) => value,
                None => continue,
            },
        };
        let added = properties
            .get(added)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let removed = properties
            .get(removed)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        properties.insert(
            total.to_string(),
            base.saturating_add(added)
                .saturating_sub(removed)
                .to_string(),
        );
    }
    properties.insert("engine-name".to_string(), "novarocks".to_string());
    properties.insert(
        "engine-version".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
    );
    if let Some(previous) = previous {
        for (key, value) in &previous.additional_properties {
            if key.starts_with(crate::document_storage::keyed::keys::RECORD_PREFIX) {
                crate::document_storage::keyed::record::decode_record(key, value)
                    .map_err(keyed_record_error)?;
                properties.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(properties)
}

fn keyed_record_error(error: novarocks_spi::connector::ConnectorError) -> crate::iceberg::Error {
    crate::iceberg::Error::new(
        crate::iceberg::ErrorKind::DataInvalid,
        format!("rewrite Iceberg document manifest: {error}"),
    )
}

/// Stable byte encoding for `ManifestContentType` used as `BTreeMap` key.
/// Data → 0, Deletes → 1. This ensures deterministic iteration order.
fn content_type_key(c: ManifestContentType) -> u8 {
    match c {
        ManifestContentType::Data => 0,
        ManifestContentType::Deletes => 1,
    }
}

/// Spec §5.2 Step 2: group manifest list entries by (partition_spec_id, content_type).
///
/// Uses `BTreeMap<(i32, u8), ...>` so iteration order is deterministic across
/// runs: spec_id ascending, then content_type by encoded byte (Data=0,
/// Deletes=1). This guarantees that the order of entries in the new manifest
/// list — and the order of physical-delete calls — is consistent.
pub(crate) fn group_manifests_by_spec_and_content(
    manifests: &[ManifestFile],
) -> BTreeMap<(i32, u8), Vec<ManifestFile>> {
    let mut groups: BTreeMap<(i32, u8), Vec<ManifestFile>> = BTreeMap::new();
    for m in manifests {
        let key = (m.partition_spec_id, content_type_key(m.content));
        groups.entry(key).or_default().push(m.clone());
    }
    groups
}

/// Merge all entries from a group of manifest files into one new manifest.
/// Drops DELETED entries (spec §5.2 Step 3). Sets remaining entries' status
/// to EXISTING (round-tripping snapshot_id, sequence_number, file_sequence_number
/// and all DataFile v3 row-lineage fields via ManifestWriter::add_existing_file).
async fn merge_manifest_group(
    artifacts: &dyn ArtifactWriter,
    metadata: &crate::iceberg::spec::TableMetadata,
    parent: &Snapshot,
    group: &[ManifestFile],
    snapshot_id: i64,
) -> crate::iceberg::Result<ManifestFile> {
    let first = group
        .first()
        .ok_or_else(|| rewrite_error("Manifest merge requires a nonempty input group"))?;
    let spec_id = first.partition_spec_id;
    let content = first.content;
    let spec = metadata
        .partition_spec_by_id(spec_id)
        .ok_or_else(|| rewrite_error("Manifest merge references a missing partition spec"))?;
    let mut entries = Vec::new();
    for descriptor in group {
        if descriptor.partition_spec_id != spec_id || descriptor.content != content {
            return Err(rewrite_error(
                "Manifest merge input group has conflicting spec or content identities",
            ));
        }
        artifacts.check_active()?;
        let source =
            ManifestSource::committed(parent.snapshot_id(), parent.manifest_list(), descriptor);
        let raw = source.load(artifacts.file_io()).await?;
        let encoding = Arc::new(ManifestWriteSource::from_header(raw.metadata())?);
        for bound in source.bind(&raw)? {
            artifacts.check_active()?;
            let entry = bound?;
            if entry.raw.is_alive() {
                entries.push(SourcedManifestEntry {
                    source: Arc::clone(&encoding),
                    entry: ManifestEntryWrite::Existing {
                        file: entry.raw.data_file().clone(),
                        frozen: FrozenEntry::from_bound_entry(&entry)?,
                    },
                });
            }
        }
    }
    write_sourced_manifest(
        artifacts,
        metadata,
        snapshot_id,
        spec,
        content,
        entries,
        &group
            .iter()
            .map(|manifest| manifest.manifest_path.clone())
            .collect::<Vec<_>>(),
    )
    .await
}

#[cfg(test)]
fn carried_identity(
    entry: &ManifestEntry,
) -> Result<(i64, i64, Option<i64>), crate::iceberg::Error> {
    let snapshot = entry
        .snapshot_id
        .ok_or_else(|| rewrite_error("Carried manifest entry has no original snapshot identity"))?;
    let sequence = entry
        .sequence_number
        .filter(|sequence| *sequence >= 0)
        .ok_or_else(|| rewrite_error("Carried manifest entry has no actual data sequence"))?;
    if entry
        .file_sequence_number
        .is_some_and(|sequence| sequence < 0)
    {
        return Err(rewrite_error(
            "Carried manifest entry has a negative file sequence",
        ));
    }
    Ok((snapshot, sequence, entry.file_sequence_number))
}

const MANIFEST_TARGET_SIZE: &str = "commit.manifest.target-size-bytes";
const DEFAULT_MANIFEST_TARGET: u64 = 8 * 1024 * 1024;
const MAX_MANIFEST_GROUPS: u128 = 200;

const MANIFESTS_REPLACED: &str = "manifests-replaced";
const MANIFESTS_CREATED: &str = "manifests-created";
const MANIFESTS_KEPT: &str = "manifests-kept";
const ENTRIES_PROCESSED: &str = "entries-processed";

fn target_size(metadata: &TableMetadata) -> crate::iceberg::Result<u64> {
    metadata
        .properties()
        .get(MANIFEST_TARGET_SIZE)
        .map_or(Ok(DEFAULT_MANIFEST_TARGET), |value| {
            value
                .parse::<i64>()
                .ok()
                .filter(|size| *size > 0)
                .map(|size| size as u64)
                .ok_or_else(|| rewrite_error("Manifest target size must be a positive integer"))
        })
}

fn manifest_groups<'a>(
    sources: impl IntoIterator<Item = &'a ManifestFile>,
    target: u64,
) -> crate::iceberg::Result<Option<usize>> {
    let (count, total) = sources
        .into_iter()
        .try_fold((0, 0u128), |(count, total), manifest| {
            u64::try_from(manifest.manifest_length)
                .ok()
                .filter(|length| *length > 0)
                .map(|length| (count + 1, total + u128::from(length)))
                .ok_or_else(|| rewrite_error("Data manifest has invalid length"))
        })?;
    if count == 0 || (count == 1 && total < u128::from(target)) {
        return Ok(None);
    }
    Ok(Some(
        total.div_ceil(u128::from(target)).min(MAX_MANIFEST_GROUPS) as usize,
    ))
}

/// Same-type keys with Iceberg Java's order and equality. Strings compare by
/// Unicode scalar value, fixed/binary by unsigned bytes, and UUID words by
/// signed Java long. Java float order distinguishes signed zeros and orders
/// every NaN after positive infinity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ClusterValue<'a> {
    Signed(i128),
    Float(u64),
    Bytes(Cow<'a, [u8]>),
    Uuid(i64, i64),
}

impl ClusterValue<'_> {
    fn into_owned(self) -> ClusterValue<'static> {
        match self {
            Self::Signed(value) => ClusterValue::Signed(value),
            Self::Float(value) => ClusterValue::Float(value),
            Self::Bytes(value) => ClusterValue::Bytes(Cow::Owned(value.into_owned())),
            Self::Uuid(high, low) => ClusterValue::Uuid(high, low),
        }
    }

    fn owned_bytes(&self) -> u64 {
        let payload = match self {
            Self::Bytes(bytes) => bytes.len() as u64,
            _ => 0,
        };
        // Accounts for sparse B-tree nodes and the final boundary vector.
        4 * std::mem::size_of::<Self>() as u64 + payload
    }
}

fn java_float_key(value: f64) -> u64 {
    if value.is_nan() {
        return u64::MAX;
    }
    let bits = value.to_bits();
    if bits & (1 << 63) != 0 {
        !bits
    } else {
        bits ^ (1 << 63)
    }
}

struct PartitionClusterField<'a> {
    id: i32,
    primitive: &'a PrimitiveType,
}

impl PartitionClusterField<'_> {
    fn project<'a>(
        &self,
        ordinal: Option<usize>,
        file: &'a DataFile,
    ) -> crate::iceberg::Result<Option<ClusterValue<'a>>> {
        let Some(ordinal) = ordinal else {
            return Ok(None);
        };
        let Some(literal) = file
            .partition()
            .fields()
            .get(ordinal)
            .ok_or_else(|| rewrite_error("Manifest partition is missing a declared field"))?
        else {
            return Ok(None);
        };
        let Literal::Primitive(value) = literal else {
            return Err(rewrite_error("Clustering partition field is not primitive"));
        };
        use PrimitiveLiteral::*;
        // Historical int and float fields may have their lossless schema promotion.
        let key = match value {
            Int(value) if *self.primitive == PrimitiveType::Long => {
                ClusterValue::Signed(i128::from(*value))
            }
            Float(value) if *self.primitive == PrimitiveType::Double => {
                ClusterValue::Float(java_float_key(f64::from(value.0)))
            }
            value if self.primitive.compatible(value) => match value {
                Boolean(value) => ClusterValue::Signed(i128::from(*value)),
                Int(value) => ClusterValue::Signed(i128::from(*value)),
                Long(value) => ClusterValue::Signed(i128::from(*value)),
                Int128(value) => ClusterValue::Signed(*value),
                UInt128(value) => ClusterValue::Uuid((*value >> 64) as i64, *value as i64),
                Float(value) => ClusterValue::Float(java_float_key(f64::from(value.0))),
                Double(value) => ClusterValue::Float(java_float_key(value.0)),
                String(value) => ClusterValue::Bytes(Cow::Borrowed(value.as_bytes())),
                Binary(value) => ClusterValue::Bytes(Cow::Borrowed(value)),
                AboveMax | BelowMin => {
                    return Err(rewrite_error("Unbounded partition clustering value"));
                }
            },
            _ => {
                return Err(rewrite_error(
                    "Manifest partition does not match the current clustering type",
                ));
            }
        };
        Ok(Some(key))
    }
}

#[derive(Default)]
struct PartitionGroups {
    boundaries: Vec<(ClusterValue<'static>, usize)>,
    owned_bytes: u64,
}

impl PartitionGroups {
    fn from_values(
        values: BTreeSet<ClusterValue<'static>>,
        groups: usize,
        mut check_active: impl FnMut() -> crate::iceberg::Result<()>,
    ) -> crate::iceberg::Result<Self> {
        let count = values.len() as u128;
        let mut result = Self::default();
        let mut previous = 0;
        for (rank, value) in values.into_iter().enumerate() {
            check_active()?;
            let group = (rank as u128 * groups as u128 / count) as usize;
            if group > previous {
                result.owned_bytes += value.owned_bytes();
                result.boundaries.push((value, group));
                previous = group;
            }
        }
        Ok(result)
    }

    fn group(&self, value: Option<ClusterValue<'_>>) -> usize {
        value.map_or(0, |value| {
            let index = self
                .boundaries
                .partition_point(|(boundary, _)| boundary <= &value);
            index
                .checked_sub(1)
                .map_or(0, |index| self.boundaries[index].1)
        })
    }
}

/// A reservation is a peak over the source descriptors, retained outputs,
/// partition keys and all live lane buffers. The same statement still owns
/// the high-water reservation, while rolling releases each actual buffer.
struct RewriteBacking<'a> {
    request: Option<&'a ConnectorRequestContext>,
    base: u64,
    admitted: u64,
}

impl RewriteBacking<'_> {
    async fn admit(&mut self, extra: u64) -> crate::iceberg::Result<()> {
        let peak = self
            .base
            .checked_add(extra)
            .ok_or_else(|| rewrite_error("Manifest rewrite backing overflow"))?;
        if let Some(request) = self.request.filter(|_| peak > self.admitted) {
            request
                .reserve_owned_metadata(std::num::NonZeroU64::new(peak).expect("positive peak"))
                .await
                .map_err(|error| {
                    rewrite_error("Original manifest backing admission refused").with_source(error)
                })?;
            self.admitted = peak;
        }
        Ok(())
    }
}

fn add_backing(left: u64, right: u64) -> crate::iceberg::Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| rewrite_error("Manifest rewrite backing overflow"))
}

async fn partition_groups(
    artifacts: &dyn ArtifactWriter,
    metadata: &TableMetadata,
    parent: &Snapshot,
    sources: &[ManifestFile],
    field: &PartitionClusterField<'_>,
    groups: usize,
    backing: &mut RewriteBacking<'_>,
) -> crate::iceberg::Result<PartitionGroups> {
    let mut values = BTreeSet::new();
    let mut bytes = 0;
    for descriptor in sources {
        artifacts.check_active()?;
        let spec = data_spec(metadata, descriptor)?;
        let ordinal = spec
            .fields()
            .iter()
            .position(|candidate| field.id == candidate.field_id);
        if ordinal.is_none() {
            continue;
        }
        let source =
            ManifestSource::committed(parent.snapshot_id(), parent.manifest_list(), descriptor);
        let raw = source
            .load_sized(artifacts.file_io())
            .await
            .map_err(|error| rewrite_read_error(error, &descriptor.manifest_path))?;
        for entry in source.bind(&raw)? {
            artifacts.check_active()?;
            let entry = entry?;
            if entry.raw.is_alive()
                && let Some(key) = field.project(ordinal, entry.raw.data_file())?
                && !values.contains(&key)
            {
                bytes = add_backing(bytes, key.owned_bytes())?;
                backing.admit(bytes).await?;
                values.insert(key.into_owned());
            }
        }
    }
    PartitionGroups::from_values(values, groups, || artifacts.check_active())
}

fn data_spec<'a>(
    metadata: &'a TableMetadata,
    descriptor: &ManifestFile,
) -> crate::iceberg::Result<&'a PartitionSpec> {
    metadata
        .partition_spec_by_id(descriptor.partition_spec_id)
        .map(AsRef::as_ref)
        .ok_or_else(|| {
            let missing = crate::catalog_control::maintenance_error::invalid_metadata(format!(
                "Partition spec {} does not exist for manifest: {}",
                descriptor.partition_spec_id, descriptor.manifest_path,
            ));
            rewrite_error("Missing Iceberg partition spec").with_source(missing)
        })
}

struct DataManifestRewrite<'a> {
    artifacts: &'a dyn ArtifactWriter,
    metadata: &'a TableMetadata,
    parent: &'a Snapshot,
    snapshot_id: i64,
    target: u64,
    backing: RewriteBacking<'a>,
}

impl DataManifestRewrite<'_> {
    async fn write(
        mut self,
        sources: Vec<ManifestFile>,
        groups: usize,
        encoding_basis: Vec<u8>,
    ) -> crate::iceberg::Result<ManifestRewriteReuse> {
        let mut lanes = BTreeMap::<(usize, i32), RollingDataManifest>::new();
        let mut outputs = Vec::new();
        let mut processed_entries = 0u64;
        let mut active_bytes = 0;
        let mut output_bytes = 0;

        let field = if groups > 1 && !self.metadata.default_partition_spec().is_unpartitioned() {
            let field = &self.metadata.default_partition_type().fields()[0];
            let Type::Primitive(primitive) = field.field_type.as_ref() else {
                return Err(rewrite_error(
                    "Current first partition field is not primitive",
                ));
            };
            Some(PartitionClusterField {
                id: field.id,
                primitive,
            })
        } else {
            None
        };
        let partition_groups = if let Some(field) = &field {
            partition_groups(
                self.artifacts,
                self.metadata,
                self.parent,
                &sources,
                field,
                groups,
                &mut self.backing,
            )
            .await?
        } else {
            PartitionGroups::default()
        };
        self.backing.base = add_backing(self.backing.base, partition_groups.owned_bytes)?;

        for descriptor in &sources {
            self.artifacts.check_active()?;
            let spec = data_spec(self.metadata, descriptor)?;
            let ordinal = field.as_ref().and_then(|field| {
                spec.fields()
                    .iter()
                    .position(|candidate| field.id == candidate.field_id)
            });
            let source = ManifestSource::committed(
                self.parent.snapshot_id(),
                self.parent.manifest_list(),
                descriptor,
            );
            let raw = source
                .load_sized(self.artifacts.file_io())
                .await
                .map_err(|error| rewrite_read_error(error, &descriptor.manifest_path))?;
            let encoding = Arc::new(ManifestWriteSource::from_header(raw.metadata())?);
            let processed_before = processed_entries;
            for entry in source.bind(&raw)? {
                self.artifacts.check_active()?;
                let entry = entry?;
                if !entry.raw.is_alive() {
                    continue;
                }
                let file = entry.raw.data_file();
                let value = field
                    .as_ref()
                    .map(|field| field.project(ordinal, file))
                    .transpose()?
                    .flatten();
                let group = partition_groups.group(value);
                let key = (group, spec.spec_id());
                let (encoded_bytes, entry_bytes) = RollingDataManifest::entry_sizes(file)?;
                let new_source = lanes.get(&key).and_then(|lane| lane.replaces.last())
                    != Some(&descriptor.manifest_path);
                let source_bytes = if new_source {
                    2 * (std::mem::size_of::<String>() + descriptor.manifest_path.len()) as u64
                } else {
                    0
                };
                let entry_bytes = add_backing(entry_bytes, source_bytes)?;
                active_bytes = add_backing(active_bytes, entry_bytes)?;
                self.backing
                    .admit(add_backing(active_bytes, output_bytes)?)
                    .await?;
                let lane = lanes.entry(key).or_default();
                if new_source {
                    lane.replaces.push(descriptor.manifest_path.clone());
                }
                lane.owned_bytes = add_backing(lane.owned_bytes, entry_bytes)?;
                lane.push(
                    SourcedManifestEntry {
                        source: Arc::clone(&encoding),
                        entry: ManifestEntryWrite::Existing {
                            file: file.clone(),
                            frozen: FrozenEntry::from_bound_entry(&entry)?,
                        },
                    },
                    encoded_bytes,
                );
                processed_entries = processed_entries
                    .checked_add(1)
                    .ok_or_else(|| rewrite_error("Manifest rewrite entry count overflow"))?;
                if lane.reaches(self.target) {
                    let lane = lanes.remove(&key).expect("active manifest lane");
                    active_bytes -= lane.owned_bytes;
                    self.write_lane(lane, spec, active_bytes, &mut output_bytes, &mut outputs)
                        .await?;
                }
            }
            if let (Some(added), Some(existing)) = (
                descriptor.added_files_count,
                descriptor.existing_files_count,
            ) && u64::from(added) + u64::from(existing) != processed_entries - processed_before
            {
                return Err(rewrite_error(
                    "Data manifest live file count differs from its descriptor",
                ));
            }
        }
        for ((_, spec_id), lane) in lanes {
            let spec = self.metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
                rewrite_error("Manifest lane references a missing partition spec")
            })?;
            active_bytes -= lane.owned_bytes;
            self.write_lane(lane, spec, active_bytes, &mut output_bytes, &mut outputs)
                .await?;
        }
        Ok(ManifestRewriteReuse {
            sources,
            outputs,
            encoding_basis,
            processed_entries,
        })
    }

    async fn write_lane(
        &mut self,
        lane: RollingDataManifest,
        spec: &PartitionSpec,
        active_bytes: u64,
        output_bytes: &mut u64,
        outputs: &mut Vec<ManifestFile>,
    ) -> crate::iceberg::Result<()> {
        // Source paths stay with the operation ledger after the lane is released.
        let ledger_bytes = lane.replaces.iter().try_fold(0, |bytes, path| {
            add_backing(
                bytes,
                2 * (std::mem::size_of::<String>() + path.len()) as u64,
            )
        })?;
        let output = lane
            .write(self.artifacts, self.metadata, self.snapshot_id, spec)
            .await?;
        let descriptor_bytes = descriptor_owned_bytes(std::slice::from_ref(&output))?;
        *output_bytes = add_backing(
            *output_bytes,
            add_backing(ledger_bytes, 4 * descriptor_bytes)?,
        )?;
        self.backing
            .admit(add_backing(active_bytes, *output_bytes)?)
            .await?;
        self.artifacts.check_active()?;
        outputs.push(output);
        Ok(())
    }
}

fn rewrite_read_error(error: crate::iceberg::Error, path: &str) -> crate::iceberg::Error {
    use std::error::Error as _;
    let mut cause = error.source();
    while let Some(source) = cause {
        if source
            .downcast_ref::<ConnectorError>()
            .is_some_and(|error| error.kind() == ConnectorErrorKind::NotFound)
        {
            let missing = crate::catalog_control::maintenance_error::invalid_metadata(format!(
                "Manifest file does not exist: {path}"
            ));
            return rewrite_error("Missing Iceberg rewrite manifest").with_source(missing);
        }
        cause = source.source();
    }
    error
}

pub(crate) fn execute_metrics(
    properties: &HashMap<String, String>,
) -> crate::iceberg::Result<ConnectorManifestRewriteMetrics> {
    let count = |key| {
        properties
            .get(key)
            .and_then(|count| count.parse().ok())
            .ok_or_else(|| rewrite_error("Frozen rewrite summary lacks an exact manifest metric"))
    };
    Ok(ConnectorManifestRewriteMetrics {
        rewritten: count(MANIFESTS_REPLACED)?,
        added: count(MANIFESTS_CREATED)?,
        kept: count(MANIFESTS_KEPT)?,
        processed_entries: count(ENTRIES_PROCESSED)?,
    })
}

pub(crate) struct RewriteManifestsPreparer {
    request: Option<ConnectorRequestContext>,
    sql_execute: bool,
}
impl RewriteManifestsPreparer {
    pub(crate) fn for_request(request: ConnectorRequestContext, sql_execute: bool) -> Self {
        Self {
            request: Some(request),
            sql_execute,
        }
    }
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self {
            request: None,
            sql_execute: false,
        }
    }
}

#[async_trait::async_trait]
impl crate::commit::staging::Preparer for RewriteManifestsPreparer {
    async fn prepare(
        &self,
        view: &crate::commit::staging::StagedView<'_>,
        intent: &crate::commit::model::OperationIntent,
    ) -> crate::iceberg::Result<crate::commit::staging::PreparedChange> {
        use crate::commit::staging::PreparedChange;
        let metadata = view.metadata();
        view.artifacts().check_active()?;
        let carry = crate::commit::slot_change::carry_records_and_closure(metadata)?;
        let parent = metadata.snapshot_for_ref(intent.target_ref());
        let no_op = || PreparedChange {
            requirements: vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: intent.target_ref().to_owned(),
                snapshot_id: parent.map(|snapshot| snapshot.snapshot_id()),
            }],
            updates: Vec::new(),
        };
        let Some(parent) = parent else {
            return Ok(no_op());
        };
        let manifest_list =
            crate::manifest::load_manifest_list(parent, view.artifacts().file_io(), metadata)
                .await
                .map_err(|error| {
                    if self.sql_execute {
                        rewrite_read_error(error, parent.manifest_list())
                    } else {
                        error
                    }
                })?;
        view.artifacts().check_active()?;
        let latest = manifest_list.entries();
        let selected_data = || {
            latest
                .iter()
                .filter(|manifest| manifest.content == ManifestContentType::Data)
        };
        let layout = if self.sql_execute {
            let mut data = selected_data();
            let Some(first) = data.next() else {
                return Ok(no_op());
            };
            let target = target_size(metadata)?;
            let Some(groups) = manifest_groups(std::iter::once(first).chain(data), target)? else {
                return Ok(no_op());
            };
            Some((target, groups))
        } else {
            None
        };
        let mut encoding_basis = encoding_basis(metadata)?;
        if let Some((target, _)) = layout {
            encoding_basis.extend_from_slice(&target.to_be_bytes());
            encoding_basis.extend_from_slice(&metadata.default_partition_spec_id().to_be_bytes());
        }

        // Preserve the original owner and peak for descriptor, marker and
        // retry-cache backing. The EXECUTE path also counts live lane buffers.
        use novarocks_spi::connector::{
            MAX_CONNECTOR_METADATA_MAINTENANCE_MARKER_BYTES,
            MAX_CONNECTOR_METADATA_MAINTENANCE_PROVIDER_PAYLOAD_BYTES,
        };
        let old = view
            .artifacts()
            .rewrite_reuse_backing_bytes(view.stage_key())?;
        let base = descriptor_owned_bytes(latest)?
            .checked_add(old)
            .and_then(|bytes| bytes.checked_add(encoding_basis.capacity() as u64))
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| {
                bytes.checked_add(
                    2 * (MAX_CONNECTOR_METADATA_MAINTENANCE_PROVIDER_PAYLOAD_BYTES
                        + MAX_CONNECTOR_METADATA_MAINTENANCE_MARKER_BYTES)
                        as u64,
                )
            })
            .ok_or_else(|| rewrite_error("Manifest descriptor backing overflow"))?;
        let mut backing = RewriteBacking {
            request: self.request.as_ref(),
            base,
            admitted: 0,
        };
        backing.admit(0).await?;

        let cached =
            view.artifacts()
                .find_reusable_rewrite(view.stage_key(), &encoding_basis, latest)?;
        let (snapshot_id, reusable) = if let Some(cache) = cached {
            (view.stage_snapshot_id()?, cache)
        } else {
            let (snapshot_id, cache) = if let Some((target, groups)) = layout {
                let snapshot_id = view.stage_snapshot_id()?;
                let rewrite = DataManifestRewrite {
                    artifacts: view.artifacts(),
                    metadata,
                    parent,
                    snapshot_id,
                    target,
                    backing,
                };
                let cache = rewrite
                    .write(selected_data().cloned().collect(), groups, encoding_basis)
                    .await?;
                (snapshot_id, cache)
            } else {
                let groups = group_manifests_by_spec_and_content(latest);
                if groups.values().all(|group| group.len() <= 1) {
                    return Ok(no_op());
                }
                let snapshot_id = view.stage_snapshot_id()?;
                let mut sources = Vec::new();
                let mut outputs = Vec::new();
                for group in groups.values().filter(|group| group.len() > 1) {
                    let output = merge_manifest_group(
                        view.artifacts(),
                        metadata,
                        parent,
                        group,
                        snapshot_id,
                    )
                    .await?;
                    view.artifacts().check_active()?;
                    sources.extend(group.iter().cloned());
                    outputs.push(output);
                }
                let cache = ManifestRewriteReuse {
                    sources,
                    outputs,
                    encoding_basis,
                    processed_entries: 0,
                };
                (snapshot_id, cache)
            };
            view.artifacts()
                .remember_reusable_rewrite(view.stage_key(), cache.clone())?;
            (snapshot_id, cache)
        };

        let selected: BTreeSet<_> = reusable
            .sources
            .iter()
            .map(|manifest| manifest.manifest_path.as_str())
            .collect();
        let kept = latest
            .iter()
            .filter(|manifest| !selected.contains(manifest.manifest_path.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let (manifests, properties) = if self.sql_execute {
            let properties = HashMap::from([
                (
                    MANIFESTS_REPLACED.to_owned(),
                    reusable.sources.len().to_string(),
                ),
                (
                    MANIFESTS_CREATED.to_owned(),
                    reusable.outputs.len().to_string(),
                ),
                (MANIFESTS_KEPT.to_owned(), kept.len().to_string()),
                (
                    ENTRIES_PROCESSED.to_owned(),
                    reusable.processed_entries.to_string(),
                ),
            ]);
            let mut manifests = reusable.outputs;
            manifests.extend(kept);
            (manifests, properties)
        } else {
            let properties = HashMap::from([
                (
                    "replaced-manifests-count".to_owned(),
                    reusable.sources.len().to_string(),
                ),
                (
                    "added-manifests-count".to_owned(),
                    reusable.outputs.len().to_string(),
                ),
            ]);
            let mut manifests = kept;
            manifests.extend(reusable.outputs);
            (manifests, properties)
        };
        let properties = finalize_snapshot_summary(properties, Some(parent.summary()))?;
        let mut change = crate::commit::overwrite::prepare_snapshot_change(
            view,
            intent,
            snapshot_id,
            Operation::Replace,
            manifests,
            properties,
            false,
        )
        .await?;
        if !carry.properties.is_empty() {
            change.updates.push(TableUpdate::SetProperties {
                updates: carry.properties,
            });
        }
        crate::document_storage::keyed::write::trace_updates(
            metadata.properties(),
            &change.updates,
            carry
                .summary
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum(),
        );
        Ok(change)
    }
}

fn rewrite_error(message: &str) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message)
}

#[cfg(test)]
#[path = "rewrite_manifests/tests/keyed_record.rs"]
mod keyed_record_tests;

#[cfg(test)]
#[path = "rewrite_manifests/tests/row_id.rs"]
mod row_id_rewrite_tests;

#[cfg(test)]
#[path = "rewrite_manifests/tests/clustered.rs"]
mod clustered_tests;
