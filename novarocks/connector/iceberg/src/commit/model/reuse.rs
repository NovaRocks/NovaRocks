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

//! Immutable manifest dependencies and the operation-owned lookup index.

use super::{ArtifactLedger, ObjectIdentity, invalid};
use crate::iceberg::Result;
use crate::iceberg::spec::{ManifestFile, StatisticsFile};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum AddedManifestAuthor {
    IntentData,
    #[cfg(test)]
    EntryGroups,
    #[cfg(test)]
    PhysicalRewrite,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ManifestReuseBasis {
    Added {
        stage: usize,
        author: AddedManifestAuthor,
        ordinal: usize,
        encoding: Vec<u8>,
        paths_digest: [u8; 32],
    },
    Filtered {
        stage: usize,
        source: ManifestFile,
        removed_ordinals: Vec<usize>,
        encoding: Vec<u8>,
    },
    Merged {
        stage: usize,
        inputs: Vec<(bool, ManifestFile)>,
        encoding: Vec<u8>,
    },
}
impl ManifestReuseBasis {
    pub fn added<'a>(
        stage: usize,
        author: AddedManifestAuthor,
        ordinal: usize,
        encoding: Vec<u8>,
        paths: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let mut hash = Sha256::new();
        for path in paths {
            hash.update((path.len() as u64).to_be_bytes());
            hash.update(path.as_bytes());
        }
        Self::Added {
            stage,
            author,
            ordinal,
            encoding,
            paths_digest: hash.finalize().into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ManifestRewriteReuse {
    pub sources: Vec<ManifestFile>,
    pub outputs: Vec<ManifestFile>,
    pub encoding_basis: Vec<u8>,
    pub processed_entries: u64,
}
impl ManifestRewriteReuse {
    pub fn owned_backing_bytes(&self) -> Result<u64> {
        crate::commit::manifest_encoding::descriptor_owned_bytes(&self.sources)?
            .checked_add(crate::commit::manifest_encoding::descriptor_owned_bytes(
                &self.outputs,
            )?)
            .and_then(|value| value.checked_add(self.encoding_basis.capacity() as u64))
            .ok_or_else(|| invalid("Manifest reuse backing overflow"))
    }
}

/// The source's physical registration. Blob properties belong to the fresh
/// snapshot registration. They do not identify the immutable Puffin object.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct StatisticsSourceIdentity {
    path: String,
    file_size_in_bytes: i64,
    file_footer_size_in_bytes: i64,
    // Opaque encryption metadata may contain key material.
    key_metadata: Option<String>,
}

impl From<&StatisticsFile> for StatisticsSourceIdentity {
    fn from(source: &StatisticsFile) -> Self {
        Self {
            path: source.statistics_path.clone(),
            file_size_in_bytes: source.file_size_in_bytes,
            file_footer_size_in_bytes: source.file_footer_size_in_bytes,
            key_metadata: source.key_metadata.clone(),
        }
    }
}

/// The exact source and output identity required by the statistics writer.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct StatisticsReuseBasis {
    pub(crate) stage: usize,
    pub(crate) source: StatisticsSourceIdentity,
    pub(crate) stage_snapshot_id: i64,
}

/// Facts about the operation-owned Puffin object after its actual close.
/// A new round authors its own registration using the current source metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReusableStatisticsFile {
    pub(crate) object: ObjectIdentity,
    pub(crate) snapshot_id: i64,
    pub(crate) file_size_in_bytes: i64,
    pub(crate) file_footer_size_in_bytes: i64,
}

fn puffin_framing_fits(file_size: i64, footer_size: i64) -> bool {
    // The file begins with a four-byte magic. The footer has its own opening
    // magic and ends with three four-byte fields (length, flags, and magic).
    const FILE_HEADER_BYTES: i64 = 4;
    const FOOTER_FRAMING_BYTES: i64 = 16;
    footer_size >= FOOTER_FRAMING_BYTES
        && file_size
            .checked_sub(FILE_HEADER_BYTES)
            .is_some_and(|after_header| footer_size <= after_header)
}

#[derive(Default)]
pub struct ManifestReuseIndex {
    keyed: HashMap<ManifestReuseBasis, ManifestFile>,
    rewrites: BTreeMap<usize, ManifestRewriteReuse>,
    statistics: BTreeMap<usize, (StatisticsReuseBasis, ReusableStatisticsFile)>,
}
impl ManifestReuseIndex {
    pub(crate) fn find_statistics(
        &mut self,
        ledger: &ArtifactLedger,
        basis: &StatisticsReuseBasis,
    ) -> Result<Option<ReusableStatisticsFile>> {
        let Some((recorded, output)) = self.statistics.get(&basis.stage) else {
            return Ok(None);
        };
        if recorded != basis {
            return Ok(None);
        }
        if ledger.is_retired(&output.object) {
            self.statistics.remove(&basis.stage);
            return Ok(None);
        }
        ledger.check_reusable_statistics(&output.object)?;
        Ok(Some(output.clone()))
    }

    pub(crate) fn remember_statistics(
        &mut self,
        ledger: &ArtifactLedger,
        basis: StatisticsReuseBasis,
        output: ReusableStatisticsFile,
    ) -> Result<()> {
        if output.snapshot_id != basis.stage_snapshot_id
            || output.object.path() == basis.source.path
        {
            return Err(invalid(
                "Iceberg statistics copy has a different stage or source identity",
            ));
        }
        if !puffin_framing_fits(
            basis.source.file_size_in_bytes,
            basis.source.file_footer_size_in_bytes,
        ) || !puffin_framing_fits(output.file_size_in_bytes, output.file_footer_size_in_bytes)
        {
            return Err(invalid("Iceberg statistics copy has inconsistent extents"));
        }
        ledger.check_reusable_statistics(&output.object)?;
        self.statistics.insert(basis.stage, (basis, output));
        Ok(())
    }

    pub fn find(
        &mut self,
        ledger: &ArtifactLedger,
        basis: &ManifestReuseBasis,
    ) -> Result<Option<ManifestFile>> {
        let Some(output) = self.keyed.get(basis) else {
            return Ok(None);
        };
        let object = ObjectIdentity::new(output.manifest_path.clone())?;
        if ledger.is_retired(&object) {
            self.keyed.remove(basis);
            return Ok(None);
        }
        ledger.check_reusable_manifest(&object)?;
        Ok(Some(output.clone()))
    }
    pub fn remember(
        &mut self,
        ledger: &ArtifactLedger,
        basis: ManifestReuseBasis,
        output: ManifestFile,
    ) -> Result<()> {
        ledger.check_reusable_manifest(&ObjectIdentity::new(output.manifest_path.clone())?)?;
        self.keyed.insert(basis, output);
        Ok(())
    }
    pub fn rewrite_backing_bytes(&self, stage: usize) -> Result<u64> {
        self.rewrites
            .get(&stage)
            .map(ManifestRewriteReuse::owned_backing_bytes)
            .transpose()
            .map(|v| v.unwrap_or(0))
    }
    pub fn find_rewrite(
        &mut self,
        ledger: &ArtifactLedger,
        stage: usize,
        encoding: &[u8],
        latest: &[ManifestFile],
    ) -> Result<Option<ManifestRewriteReuse>> {
        let Some(cache) = self.rewrites.get(&stage) else {
            return Ok(None);
        };
        for output in &cache.outputs {
            let object = ObjectIdentity::new(output.manifest_path.clone())?;
            if ledger.is_retired(&object) {
                self.rewrites.remove(&stage);
                return Ok(None);
            }
            ledger.check_reusable_manifest(&object)?;
        }
        if cache.encoding_basis != encoding {
            return Ok(None);
        }
        let latest: HashSet<_> = latest.iter().collect();
        if !cache.sources.iter().all(|source| latest.contains(source)) {
            return Ok(None);
        }
        Ok(Some(cache.clone()))
    }
    pub fn remember_rewrite(
        &mut self,
        ledger: &ArtifactLedger,
        stage: usize,
        result: ManifestRewriteReuse,
    ) -> Result<()> {
        for output in &result.outputs {
            ledger.check_reusable_manifest(&ObjectIdentity::new(output.manifest_path.clone())?)?;
        }
        self.rewrites.insert(stage, result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::model::{ArtifactKind, ArtifactRecord, AttemptToken, OperationToken};
    use crate::iceberg::spec::ManifestContentType;
    use novarocks_spi::connector::ConnectorWriteOperationId;

    fn descriptor(path: &str) -> ManifestFile {
        ManifestFile {
            manifest_path: path.into(),
            manifest_length: 100,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: -1,
            min_sequence_number: -1,
            added_snapshot_id: 11,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(3),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }
    fn ledger() -> (ArtifactLedger, AttemptToken) {
        let operation = OperationToken::from_write(ConnectorWriteOperationId::from_bytes([42; 16]));
        (
            ArtifactLedger::new(operation),
            AttemptToken::new(operation, 0),
        )
    }
    fn register(ledger: &mut ArtifactLedger, attempt: AttemptToken, path: &str) -> ManifestFile {
        let object = ObjectIdentity::new(path).unwrap();
        ledger
            .register(ArtifactRecord::produced(
                object.clone(),
                attempt,
                ArtifactKind::Manifest,
            ))
            .unwrap();
        ledger.confirm_written_for_test(&object).unwrap();
        descriptor(path)
    }
    #[test]
    fn added_inputs_preserve_author_order_and_unambiguous_path_boundaries() {
        let basis = |stage, author, ordinal, encoding, paths| {
            ManifestReuseBasis::added(stage, author, ordinal, encoding, paths)
        };
        let original = basis(
            0,
            AddedManifestAuthor::IntentData,
            1,
            vec![1],
            vec!["ab", "c"],
        );
        for changed in [
            basis(
                1,
                AddedManifestAuthor::IntentData,
                1,
                vec![1],
                vec!["ab", "c"],
            ),
            basis(
                0,
                AddedManifestAuthor::EntryGroups,
                1,
                vec![1],
                vec!["ab", "c"],
            ),
            basis(
                0,
                AddedManifestAuthor::IntentData,
                2,
                vec![1],
                vec!["ab", "c"],
            ),
            basis(
                0,
                AddedManifestAuthor::IntentData,
                1,
                vec![2],
                vec!["ab", "c"],
            ),
            basis(
                0,
                AddedManifestAuthor::IntentData,
                1,
                vec![1],
                vec!["c", "ab"],
            ),
            basis(
                0,
                AddedManifestAuthor::IntentData,
                1,
                vec![1],
                vec!["a", "bc"],
            ),
        ] {
            assert_ne!(original, changed);
        }
    }
    #[test]
    fn reuse_requires_written_live_objects_and_keeps_the_original_ledger() {
        let (mut ledger, attempt) = ledger();
        let mut index = ManifestReuseIndex::default();
        let output = register(&mut ledger, attempt, "s3://bucket/output.avro");
        let basis = ManifestReuseBasis::added(
            0,
            AddedManifestAuthor::IntentData,
            0,
            vec![1],
            ["s3://bucket/data.parquet"],
        );
        index
            .remember(&ledger, basis.clone(), output.clone())
            .unwrap();
        assert_eq!(index.find(&ledger, &basis).unwrap(), Some(output.clone()));
        ledger
            .retire(&ObjectIdentity::new(output.manifest_path).unwrap())
            .unwrap();
        assert!(index.find(&ledger, &basis).unwrap().is_none());
        assert_eq!(
            ledger.records().count(),
            1,
            "Index eviction does not remove the physical responsibility"
        );
    }
    #[test]
    fn merge_and_filter_compare_complete_descriptors_and_order() {
        let source = descriptor("s3://bucket/source.avro");
        let other = descriptor("s3://bucket/other.avro");
        let original = ManifestReuseBasis::Merged {
            stage: 0,
            inputs: vec![(true, source.clone()), (false, other.clone())],
            encoding: vec![1],
        };
        for inputs in [
            vec![(false, source.clone()), (false, other.clone())],
            vec![(false, other.clone()), (true, source.clone())],
            vec![
                (
                    true,
                    ManifestFile {
                        first_row_id: Some(50),
                        ..source.clone()
                    },
                ),
                (false, other.clone()),
            ],
            vec![
                (
                    true,
                    ManifestFile {
                        sequence_number: 7,
                        ..source.clone()
                    },
                ),
                (false, other),
            ],
        ] {
            assert_ne!(
                original,
                ManifestReuseBasis::Merged {
                    stage: 0,
                    inputs,
                    encoding: vec![1]
                }
            );
        }
        let filter = ManifestReuseBasis::Filtered {
            stage: 0,
            source: source.clone(),
            removed_ordinals: vec![1, 4],
            encoding: vec![1],
        };
        assert_ne!(
            filter,
            ManifestReuseBasis::Filtered {
                stage: 0,
                source,
                removed_ordinals: vec![1, 5],
                encoding: vec![1]
            }
        );
    }
    #[test]
    fn rewrite_keeps_its_previous_selection_when_a_new_manifest_is_appended() {
        let (mut ledger, attempt) = ledger();
        let mut index = ManifestReuseIndex::default();
        let source = descriptor("s3://bucket/source.avro");
        let output = register(&mut ledger, attempt, "s3://bucket/merged.avro");
        index
            .remember_rewrite(
                &ledger,
                0,
                ManifestRewriteReuse {
                    sources: vec![source.clone()],
                    outputs: vec![output.clone()],
                    encoding_basis: vec![1],
                    processed_entries: 5,
                },
            )
            .unwrap();
        let latest = [descriptor("s3://bucket/concurrent.avro"), source.clone()];
        let reused = index
            .find_rewrite(&ledger, 0, &[1], &latest)
            .unwrap()
            .unwrap();
        assert_eq!(reused.outputs.as_slice(), std::slice::from_ref(&output));
        assert_eq!(reused.processed_entries, 5);
        let mut replaced = source.clone();
        replaced.sequence_number += 1;
        assert!(
            index
                .find_rewrite(&ledger, 0, &[1], &[replaced])
                .unwrap()
                .is_none()
        );
        assert!(
            index
                .find_rewrite(&ledger, 0, &[2], &latest)
                .unwrap()
                .is_none()
        );
        assert!(
            index
                .find_rewrite(&ledger, 0, &[1], &latest[..1])
                .unwrap()
                .is_none()
        );
        ledger
            .retire(&ObjectIdentity::new(output.manifest_path).unwrap())
            .unwrap();
        assert!(
            index
                .find_rewrite(&ledger, 0, &[1], &latest)
                .unwrap()
                .is_none()
        );
        assert_eq!(ledger.records().count(), 1);
    }
}
