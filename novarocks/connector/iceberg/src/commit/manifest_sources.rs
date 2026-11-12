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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::commit::model::{EntryIdentity, FrozenEntry};
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFileFormat, ManifestContentType, ManifestFile, ManifestList, Snapshot,
    TableMetadata,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::binding::BoundEntry;
use crate::manifest::source::ManifestSource;

/// One observed list, retaining only descriptors between manifest reads.
pub(crate) struct SnapshotSources<'a> {
    snapshot: &'a Snapshot,
    descriptors: Vec<ManifestFile>,
    list_ordinals: Vec<usize>,
}

impl<'a> SnapshotSources<'a> {
    pub(crate) async fn load(
        metadata: &TableMetadata,
        snapshot: &'a Snapshot,
        io: &FileIO,
        active: impl Fn() -> Result<()>,
    ) -> Result<Self> {
        active()?;
        let list = crate::manifest::load_manifest_list(snapshot, io, metadata).await?;
        active()?;
        Self::from_completed_list(snapshot, &list, active)
    }

    pub(crate) fn from_completed_list(
        snapshot: &'a Snapshot,
        list: &ManifestList,
        active: impl Fn() -> Result<()>,
    ) -> Result<Self> {
        let mut seen = HashMap::new();
        let mut indices = Vec::new();
        for (index, descriptor) in list.entries().iter().enumerate() {
            active()?;
            if let Some(previous) = seen.insert(&descriptor.manifest_path, descriptor) {
                if previous != descriptor {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Snapshot {} has conflicting descriptors for manifest {}",
                            snapshot.snapshot_id(),
                            descriptor.manifest_path,
                        ),
                    ));
                }
            } else {
                indices.push(index);
            }
        }
        let descriptors = indices
            .iter()
            .map(|index| list.entries()[*index].clone())
            .collect();
        Ok(Self {
            snapshot,
            descriptors,
            list_ordinals: indices,
        })
    }

    pub(crate) fn from_indexed_descriptors(
        snapshot: &'a Snapshot,
        descriptors: impl IntoIterator<Item = (usize, ManifestFile)>,
    ) -> Self {
        let (list_ordinals, descriptors) = descriptors.into_iter().unzip();
        Self {
            snapshot,
            descriptors,
            list_ordinals,
        }
    }

    pub(crate) fn source_index_for_list_ordinal(&self, ordinal: usize) -> Option<usize> {
        self.list_ordinals.binary_search(&ordinal).ok()
    }

    pub(crate) fn into_indexed_descriptors(self) -> BTreeMap<usize, ManifestFile> {
        self.list_ordinals
            .into_iter()
            .zip(self.descriptors)
            .collect()
    }

    pub(crate) fn sources(&self) -> impl Iterator<Item = ManifestSource<'_>> {
        self.descriptors.iter().map(|descriptor| {
            ManifestSource::committed(
                self.snapshot.snapshot_id(),
                self.snapshot.manifest_list(),
                descriptor,
            )
        })
    }

    /// Callback receives borrowed entries in physical order. A completed fold
    /// leaves only the facts the caller explicitly copied out.
    pub(crate) async fn fold(
        &self,
        io: &FileIO,
        active: impl Fn() -> Result<()>,
        select: impl Fn(&ManifestFile) -> bool,
        mut entry: impl FnMut(BoundEntry<'_>) -> Result<()>,
    ) -> Result<()> {
        self.fold_indexed(io, active, select, |_, bound| entry(bound))
            .await
    }

    async fn fold_indexed(
        &self,
        io: &FileIO,
        active: impl Fn() -> Result<()>,
        select: impl Fn(&ManifestFile) -> bool,
        mut entry: impl FnMut(usize, BoundEntry<'_>) -> Result<()>,
    ) -> Result<()> {
        for (index, source) in self.sources().enumerate() {
            active()?;
            if !select(source.descriptor) {
                continue;
            }
            let raw = source.load(io).await?;
            active()?;
            for bound in source.bind(&raw)? {
                active()?;
                entry(index, bound?)?;
            }
        }
        active()
    }

    /// Complete live identity inventory, holding locators instead of SDK files.
    pub(crate) async fn live_entries(
        &self,
        io: &FileIO,
        active: impl Fn() -> Result<()>,
    ) -> Result<BTreeMap<EntryIdentity, LocatedEntry>> {
        let mut entries = BTreeMap::new();
        self.fold_indexed(
            io,
            active,
            |_| true,
            |source_index, entry| {
                if entry.raw.is_alive() {
                    let located = LocatedEntry::from_bound(source_index, &entry)?;
                    let identity = located.frozen.identity().clone();
                    if entries.insert(identity.clone(), located).is_some() {
                        return Err(duplicate_identity(&identity));
                    }
                }
                Ok(())
            },
        )
        .await?;
        Ok(entries)
    }

    /// P lookup scans all data sources because paths have no partition
    /// constraint. Only facts for matching paths escape the source fold.
    pub(crate) async fn live_data_matches(
        &self,
        paths: &BTreeSet<&str>,
        io: &FileIO,
        active: impl Fn() -> Result<()>,
    ) -> Result<BTreeSet<String>> {
        let mut matches = BTreeSet::new();
        if paths.is_empty() {
            active()?;
            return Ok(matches);
        }
        self.fold(
            io,
            active,
            |descriptor| descriptor.content == ManifestContentType::Data,
            |entry| {
                if entry.raw.is_alive() && paths.contains(entry.raw.file_path()) {
                    if entry.raw.content_type() != DataContentType::Data {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Data manifest contains a non-data entry",
                        ));
                    }
                    let identity = EntryIdentity::try_from(entry.raw.data_file())?;
                    if !matches.insert(identity.path().to_owned()) {
                        return Err(duplicate_identity(&identity));
                    }
                }
                Ok(())
            },
        )
        .await?;
        Ok(matches)
    }
}

/// A compact location with all the facts needed to revalidate a selection.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LocatedEntry {
    pub(crate) frozen: FrozenEntry,
    pub(crate) ordinal: usize,
    pub(crate) source_index: usize,
    pub(crate) content_type: DataContentType,
    pub(crate) file_format: DataFileFormat,
    pub(crate) logical_size: u64,
}

impl LocatedEntry {
    pub(crate) fn from_bound(source_index: usize, entry: &BoundEntry<'_>) -> Result<Self> {
        let frozen = FrozenEntry::from_bound_entry(entry)?;
        let file = entry.raw.data_file();
        let logical_size = match frozen.identity() {
            EntryIdentity::DeletionVector { length, .. } => {
                u64::try_from(*length).map_err(|_| {
                    Error::new(ErrorKind::DataInvalid, "Negative deletion vector length")
                })?
            }
            _ => file.file_size_in_bytes(),
        };
        Ok(Self {
            frozen,
            ordinal: entry.ordinal,
            source_index,
            content_type: file.content_type(),
            file_format: file.file_format(),
            logical_size,
        })
    }
}

pub(crate) fn duplicate_identity(identity: &EntryIdentity) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!("Duplicate live Iceberg entry identity: {identity:?}"),
    )
}
