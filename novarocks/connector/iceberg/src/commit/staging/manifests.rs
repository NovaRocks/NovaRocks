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
    AddedContent, ArtifactClass, ArtifactKind, ArtifactWriter, FrozenEntry, ObjectIdentity,
};
use crate::iceberg::Result;
use crate::iceberg::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, PartitionSpec, SchemaRef, TableMetadata,
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
    let encoding = ManifestWriteEncoding::fixed(version, schema, spec)?;
    let entries = entries
        .into_iter()
        .map(|entry| encoding.encode(entry, None));
    write_encoded_manifest(
        artifacts,
        ArtifactClass::Attempt,
        version,
        snapshot_id,
        &encoding,
        content,
        entries,
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
) -> Result<ManifestFile> {
    write_sourced_manifest_with_class(
        artifacts,
        ArtifactClass::Attempt,
        metadata,
        snapshot_id,
        spec,
        content,
        entries,
    )
    .await
}

pub(crate) async fn write_sourced_manifest_with_class(
    artifacts: &dyn ArtifactWriter,
    class: ArtifactClass,
    metadata: &TableMetadata,
    snapshot_id: i64,
    spec: &PartitionSpec,
    content: ManifestContentType,
    entries: Vec<SourcedManifestEntry>,
) -> Result<ManifestFile> {
    let encoding =
        ManifestWriteEncoding::choose(metadata, spec, &entries, || artifacts.check_active())?;
    let entries = entries
        .into_iter()
        .map(|entry| encoding.encode(entry.entry, Some(&entry.source)));
    write_encoded_manifest(
        artifacts,
        class,
        metadata.format_version(),
        snapshot_id,
        &encoding,
        content,
        entries,
    )
    .await
}

async fn write_encoded_manifest(
    artifacts: &dyn ArtifactWriter,
    class: ArtifactClass,
    version: FormatVersion,
    snapshot_id: i64,
    encoding: &ManifestWriteEncoding,
    content: ManifestContentType,
    entries: impl IntoIterator<Item = Result<EncodedManifestEntry>>,
) -> Result<ManifestFile> {
    artifacts.check_active()?;
    let object = artifacts.allocate(class, ArtifactKind::Manifest)?;
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
    for entry in entries {
        artifacts.check_active()?;
        match entry? {
            EncodedManifestEntry::Added {
                file,
                data_sequence,
            } => {
                writer.add_file(file, data_sequence.manifest_writer_value())?;
            }
            EncodedManifestEntry::Existing { file, facts } => {
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
    writer.write_manifest_file().await
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
    let object = artifacts.allocate(ArtifactClass::Attempt, ArtifactKind::ManifestList)?;
    let output = artifacts.file_io().new_output(object.path())?;
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
    artifacts.check_active()?;
    Ok(ManifestListOutput { object, row_range })
}
