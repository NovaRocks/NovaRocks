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
use crate::commit::model::{
    AddedContent, ArtifactClass, ArtifactKind, ArtifactWriter, EntryIdentity, FrozenEntry,
    ObjectIdentity,
};
use crate::iceberg::Result;
use crate::iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestFile,
    ManifestListWriter, ManifestStatus, ManifestWriterBuilder, PartitionSpec, SchemaRef,
    TableMetadata,
};

/// Explicit file IDs never consume an inherited manifest cursor. Historical
/// unassigned manifests preserve None until the manifest list assigns a range.
pub struct FirstRowIdInheritance {
    next: Option<i64>,
}
impl FirstRowIdInheritance {
    pub fn new(manifest_first_row_id: Option<i64>) -> Self {
        Self {
            next: manifest_first_row_id,
        }
    }
    pub fn resolve(&mut self, file: &DataFile, status: ManifestStatus) -> Result<Option<i64>> {
        if let Some(id) = file.first_row_id() {
            return Ok(Some(id));
        }
        if status == ManifestStatus::Deleted || file.content_type() != DataContentType::Data {
            return Ok(None);
        }
        let Some(first) = self.next else {
            return Ok(None);
        };
        let count = i64::try_from(file.record_count())
            .map_err(|_| invalid("File row count exceeds row-ID range"))?;
        self.next = Some(
            first
                .checked_add(count)
                .ok_or_else(|| invalid("Manifest inherited row-ID range overflow"))?,
        );
        Ok(Some(first))
    }
}

pub enum ManifestEntryWrite {
    Added(AddedContent),
    Existing { file: DataFile, frozen: FrozenEntry },
    Deleted { file: DataFile, frozen: FrozenEntry },
}

/// Write one attempt-owned manifest. Existing logical rows retain their actual
/// sequence and row identity; added physical replacements inherit file sequence.
pub async fn write_manifest(
    artifacts: &dyn ArtifactWriter,
    version: FormatVersion,
    snapshot_id: i64,
    schema: SchemaRef,
    spec: PartitionSpec,
    content: ManifestContentType,
    entries: impl IntoIterator<Item = ManifestEntryWrite>,
) -> Result<ManifestFile> {
    artifacts.check_active()?;
    let object = artifacts.allocate(ArtifactClass::Attempt, ArtifactKind::Manifest)?;
    let output = artifacts.file_io().new_output(object.path())?;
    let builder = ManifestWriterBuilder::new(output, Some(snapshot_id), None, schema, spec.clone());
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
        match entry {
            ManifestEntryWrite::Added(added) => {
                if added.partition_spec_id() != spec.spec_id() {
                    return Err(invalid(
                        "Added content partition spec does not match its manifest",
                    ));
                }
                writer.add_file(
                    added.file().clone(),
                    added.data_sequence().manifest_writer_value(),
                )?;
            }
            ManifestEntryWrite::Existing { file, frozen } => {
                let file = carry_file(file, &frozen, spec.spec_id())?;
                let facts = frozen.facts();
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
            ManifestEntryWrite::Deleted { file, frozen } => {
                let file = carry_file(file, &frozen, spec.spec_id())?;
                writer.add_delete_file(
                    file,
                    frozen
                        .facts()
                        .data_sequence
                        .ok_or_else(|| invalid("Deleted entry has no assigned data sequence"))?,
                    frozen.facts().file_sequence,
                )?;
            }
        }
    }
    writer.write_manifest_file().await
}

fn carry_file(file: DataFile, frozen: &FrozenEntry, spec_id: i32) -> Result<DataFile> {
    let facts = frozen.facts();
    if EntryIdentity::try_from(&file)? != *frozen.identity()
        || facts.partition_spec_id != spec_id
        || facts.partition != *file.partition()
        || facts.record_count != file.record_count()
    {
        return Err(invalid(
            "Carried data file does not match frozen entry facts",
        ));
    }
    crate::commit::data_file::clone_data_file_with_first_row_id(&file, spec_id, facts.first_row_id)
        .map_err(invalid)
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
        FormatVersion::V1 => ManifestListWriter::v1(output, snapshot_id, parent),
        FormatVersion::V2 => {
            ManifestListWriter::v2(output, snapshot_id, parent, metadata.next_sequence_number())
        }
        FormatVersion::V3 => ManifestListWriter::v3(
            output,
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
