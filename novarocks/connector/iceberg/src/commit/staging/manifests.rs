// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::invalid;
use crate::commit::manifest_encoding::{
    EncodedManifestEntry, ManifestWriteEncoding, SourcedManifestEntry,
};
use crate::commit::model::{
    AddedContent, ArtifactKind, ArtifactWriter, FrozenEntry, ObjectIdentity,
};
use crate::iceberg::Result;
use crate::iceberg::spec::{
    DataFile, FormatVersion, Literal, ManifestContentType, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, PartitionSpec, PrimitiveLiteral, SchemaRef, TableMetadata,
};

pub enum ManifestEntryWrite {
    Added(AddedContent),
    Existing { file: DataFile, frozen: FrozenEntry },
    Deleted { file: DataFile, frozen: FrozenEntry },
}

impl ManifestEntryWrite {
    pub(crate) fn file(&self) -> &DataFile {
        match self {
            Self::Added(added) => added.file(),
            Self::Existing { file, .. } | Self::Deleted { file, .. } => file,
        }
    }
}

/// Write one attempt-owned manifest using a caller's already known encoding.
pub async fn write_manifest(
    artifacts: &dyn ArtifactWriter,
    version: FormatVersion,
    snapshot_id: i64,
    schema: SchemaRef,
    spec: PartitionSpec,
    content: ManifestContentType,
    entries: impl IntoIterator<Item = ManifestEntryWrite>,
) -> Result<ManifestFile> {
    write_manifest_with_sources(
        artifacts,
        (version, snapshot_id, schema, spec, content),
        entries.into_iter().map(Ok),
        &[],
    )
    .await
}

pub(crate) async fn write_manifest_with_sources(
    artifacts: &dyn ArtifactWriter,
    (version, snapshot_id, schema, spec, content): (
        FormatVersion,
        i64,
        SchemaRef,
        PartitionSpec,
        ManifestContentType,
    ),
    entries: impl IntoIterator<Item = Result<ManifestEntryWrite>>,
    replaces: &[String],
) -> Result<ManifestFile> {
    let encoding = ManifestWriteEncoding::fixed(version, schema, spec)?;
    let entries = entries
        .into_iter()
        .map(|entry| encoding.encode(entry?, None));
    write_encoded_manifest(
        artifacts,
        version,
        snapshot_id,
        &encoding,
        content,
        entries,
        replaces,
    )
    .await
}

pub(crate) async fn write_sourced_manifest(
    artifacts: &dyn ArtifactWriter,
    metadata: &TableMetadata,
    snapshot_id: i64,
    spec: &PartitionSpec,
    content: ManifestContentType,
    entries: Vec<SourcedManifestEntry>,
    replaces: &[String],
) -> Result<ManifestFile> {
    let encoding =
        ManifestWriteEncoding::choose(metadata, spec, &entries, || artifacts.check_active())?;
    let entries = entries
        .into_iter()
        .map(|entry| encoding.encode(entry.entry, Some(&entry.source)));
    write_encoded_manifest(
        artifacts,
        metadata.format_version(),
        snapshot_id,
        &encoding,
        content,
        entries,
        replaces,
    )
    .await
}

/// Buffer one data-manifest lane until its estimated Avro length reaches the
/// table's target. Source encoding remains the original sourced writer's job.
#[derive(Default)]
pub(crate) struct RollingDataManifest {
    entries: Vec<SourcedManifestEntry>,
    pub(crate) replaces: Vec<String>,
    pub(crate) owned_bytes: u64,
    encoded_bytes: u64,
}

impl RollingDataManifest {
    pub(crate) fn entry_sizes(file: &DataFile) -> Result<(u64, u64)> {
        let variable_size = |value: &PrimitiveLiteral| match value {
            PrimitiveLiteral::String(value) => value.len(),
            PrimitiveLiteral::Binary(value) => value.len(),
            _ => 0,
        };
        let mut payload = file.file_path().len();
        let mut add = |bytes: usize| -> Result<()> {
            payload = payload
                .checked_add(bytes)
                .ok_or_else(|| invalid("Manifest entry backing overflow"))?;
            Ok(())
        };
        add(file.key_metadata().map_or(0, <[u8]>::len))?;
        add(std::mem::size_of_val(
            file.split_offsets().unwrap_or_default(),
        ))?;
        for partition in file.partition().fields() {
            add(20)?;
            if let Some(Literal::Primitive(value)) = partition {
                add(variable_size(value))?;
            }
        }
        for counts in [
            file.column_sizes(),
            file.value_counts(),
            file.null_value_counts(),
            file.nan_value_counts(),
        ] {
            add(counts.len().saturating_mul(20))?;
        }
        for value in file
            .lower_bounds()
            .values()
            .chain(file.upper_bounds().values())
        {
            add(20)?;
            add(variable_size(value.literal()))?;
        }
        let encoded = 48 + payload as u64;
        // Includes vector spare capacity, heap payloads and the temporary
        // copies held by the sourced encoder, SDK writer and artifact author.
        let owned = encoded
            .checked_add(std::mem::size_of::<SourcedManifestEntry>() as u64)
            .and_then(|bytes| bytes.checked_mul(8))
            .ok_or_else(|| invalid("Manifest entry backing overflow"))?;
        Ok((encoded, owned))
    }

    pub(crate) fn push(&mut self, entry: SourcedManifestEntry, encoded_bytes: u64) {
        self.encoded_bytes += encoded_bytes;
        self.entries.push(entry);
    }

    pub(crate) fn reaches(&self, target: u64) -> bool {
        self.encoded_bytes >= target
    }

    pub(crate) async fn write(
        self,
        artifacts: &dyn ArtifactWriter,
        metadata: &TableMetadata,
        snapshot_id: i64,
        spec: &PartitionSpec,
    ) -> Result<ManifestFile> {
        write_sourced_manifest(
            artifacts,
            metadata,
            snapshot_id,
            spec,
            ManifestContentType::Data,
            self.entries,
            &self.replaces,
        )
        .await
    }
}

pub(super) async fn write_encoded_manifest(
    artifacts: &dyn ArtifactWriter,
    version: FormatVersion,
    snapshot_id: i64,
    encoding: &ManifestWriteEncoding,
    content: ManifestContentType,
    entries: impl IntoIterator<Item = Result<EncodedManifestEntry>>,
    replaces: &[String],
) -> Result<ManifestFile> {
    artifacts.check_active()?;
    let object = artifacts.allocate(ArtifactKind::Manifest)?;
    let output = artifacts.file_io().new_output(object.path())?;
    let builder = ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        encoding.schema.clone(),
        encoding.spec.clone(),
    );
    let mut writer = match (version, content) {
        (FormatVersion::V1, ManifestContentType::Data) => builder.build_v1(),
        (FormatVersion::V1, ManifestContentType::Deletes) => {
            return Err(invalid("V1 tables cannot contain delete manifests"));
        }
        (FormatVersion::V2, ManifestContentType::Data) => builder.build_v2_data(),
        (FormatVersion::V2, ManifestContentType::Deletes) => builder.build_v2_deletes(),
        (FormatVersion::V3, ManifestContentType::Data) => builder.build_v3_data(),
        (FormatVersion::V3, ManifestContentType::Deletes) => builder.build_v3_deletes(),
    };
    let mut added_paths = Vec::new();
    let mut existing_paths = Vec::new();
    for entry in entries {
        artifacts.check_active()?;
        match entry? {
            EncodedManifestEntry::Added {
                file,
                data_sequence,
            } => {
                added_paths.push(file.file_path().to_owned());
                writer.add_file(file, data_sequence.manifest_writer_value())?;
            }
            EncodedManifestEntry::Existing { file, facts } => {
                existing_paths.push(file.file_path().to_owned());
                writer.add_existing_file(
                    file,
                    facts
                        .added_snapshot_id
                        .ok_or_else(|| invalid("Carried entry has no original added snapshot"))?,
                    facts
                        .data_sequence
                        .ok_or_else(|| invalid("Carried entry has no assigned data sequence"))?,
                    facts.file_sequence,
                )?;
            }
            EncodedManifestEntry::Deleted { file, facts } => {
                writer.add_delete_file(
                    file,
                    facts
                        .data_sequence
                        .ok_or_else(|| invalid("Deleted entry has no assigned data sequence"))?,
                    facts.file_sequence,
                )?;
            }
        }
    }
    let descriptor = writer.write_manifest_file().await?;
    artifacts.record_manifest(&object, &added_paths, &existing_paths, replaces)?;
    Ok(descriptor)
}

pub struct ManifestListOutput {
    pub object: ObjectIdentity,
    /// Exactly the range consumed by the successful writer, not a prebudget.
    pub row_range: Option<(u64, u64)>,
}

pub async fn write_manifest_list(
    artifacts: &dyn ArtifactWriter,
    metadata: &TableMetadata,
    snapshot_id: i64,
    parent: Option<i64>,
    manifests: impl IntoIterator<Item = ManifestFile>,
) -> Result<ManifestListOutput> {
    artifacts.check_active()?;
    let object = artifacts.allocate(ArtifactKind::ManifestList)?;
    let output = artifacts.file_io().new_output(object.path())?;
    let manifests: Vec<_> = manifests.into_iter().collect();
    let paths: Vec<_> = manifests
        .iter()
        .map(|manifest| manifest.manifest_path.clone())
        .collect();
    let first = metadata.next_row_id();
    let mut writer = match metadata.format_version() {
        FormatVersion::V1 => ManifestListWriter::v1(output.writer().await?, snapshot_id, parent),
        FormatVersion::V2 => ManifestListWriter::v2(
            output.writer().await?,
            snapshot_id,
            parent,
            metadata.next_sequence_number(),
        ),
        FormatVersion::V3 => ManifestListWriter::v3(
            output.writer().await?,
            snapshot_id,
            parent,
            metadata.next_sequence_number(),
            Some(first),
        ),
    };
    writer.add_manifests(manifests.into_iter())?;
    let row_range = writer
        .next_row_id()
        .map(|next| {
            next.checked_sub(first)
                .map(|count| (first, count))
                .ok_or_else(|| invalid("Manifest list row-ID counter moved backwards"))
        })
        .transpose()?;
    writer.close().await?;
    artifacts.record_manifest_list(&object, snapshot_id, &paths)?;
    artifacts.check_active()?;
    Ok(ManifestListOutput { object, row_range })
}
