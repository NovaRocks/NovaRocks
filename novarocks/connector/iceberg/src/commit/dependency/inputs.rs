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

//! Manifest input retains logical identities, including multiple DV blobs in one Puffin.

use std::collections::BTreeMap;

use super::super::model::{ArtifactWriter, EntryIdentity, FrozenEntry};
use super::{HistoryGap, HistoryWindow, history_window};
use crate::iceberg::spec::{DataContentType, DataFile, ManifestFile, SnapshotRef, TableMetadata};
use crate::iceberg::{Error, ErrorKind, Result};

#[derive(Clone, Debug)]
pub struct LiveEntry {
    pub frozen: FrozenEntry,
    pub file: DataFile,
}

pub type LiveSet = BTreeMap<EntryIdentity, LiveEntry>;

pub struct ValidationInputs<'a> {
    pub metadata: &'a TableMetadata,
    pub parent: Option<i64>,
    io: &'a dyn ArtifactWriter,
    live: Option<LiveSet>,
}

impl<'a> ValidationInputs<'a> {
    pub fn new(
        metadata: &'a TableMetadata,
        parent: Option<i64>,
        io: &'a dyn ArtifactWriter,
    ) -> Self {
        Self {
            metadata,
            parent,
            io,
            live: None,
        }
    }

    pub async fn live_set(&mut self) -> Result<&LiveSet> {
        if self.live.is_none() {
            let mut live = LiveSet::new();
            if let Some(parent) = self.parent {
                let snapshot = self.metadata.snapshot_by_id(parent).ok_or_else(|| {
                    invalid(format!(
                        "Target ref snapshot {parent} is absent from metadata"
                    ))
                })?;
                for (identity, entry) in self.snapshot_entries(snapshot, false).await? {
                    if live.insert(identity.clone(), entry).is_some() {
                        return Err(invalid(format!(
                            "Duplicate live Iceberg entry identity: {identity:?}"
                        )));
                    }
                }
            }
            self.live = Some(live);
        }
        Ok(self.live.as_ref().expect("live set initialized"))
    }

    pub fn history(&self, start: Option<i64>) -> std::result::Result<HistoryWindow, HistoryGap> {
        history_window(self.metadata, start, self.parent)
    }

    /// Reads only manifests written by this snapshot. Inherited manifests are not new history.
    pub async fn own_snapshot_entries(
        &self,
        snapshot: &SnapshotRef,
    ) -> Result<Vec<(EntryIdentity, LiveEntry)>> {
        self.snapshot_entries(snapshot, true).await
    }

    async fn snapshot_entries(
        &self,
        snapshot: &SnapshotRef,
        own_only: bool,
    ) -> Result<Vec<(EntryIdentity, LiveEntry)>> {
        self.io.check_active()?;
        let list = snapshot
            .load_manifest_list(self.io.file_io(), self.metadata)
            .await?;
        self.io.check_active()?;
        let mut entries = Vec::new();
        for manifest in list.entries() {
            if own_only && manifest.added_snapshot_id != snapshot.snapshot_id() {
                continue;
            }
            entries.extend(self.manifest_entries(manifest, own_only).await?);
        }
        Ok(entries)
    }

    async fn manifest_entries(
        &self,
        manifest: &ManifestFile,
        include_deleted: bool,
    ) -> Result<Vec<(EntryIdentity, LiveEntry)>> {
        self.io.check_active()?;
        let loaded = manifest.load_manifest(self.io.file_io()).await?;
        self.io.check_active()?;
        let mut next_row = manifest
            .first_row_id
            .map(i64::try_from)
            .transpose()
            .map_err(|_| invalid("Manifest first row ID exceeds i64"))?;
        let mut out = Vec::new();
        for entry in loaded.entries() {
            let file = entry.data_file();
            let inherited = if entry.is_alive() && file.content_type() == DataContentType::Data {
                let inherited = file.first_row_id().or(next_row);
                if let Some(next) = next_row.as_mut().filter(|_| file.first_row_id().is_none()) {
                    *next = next
                        .checked_add(
                            i64::try_from(file.record_count())
                                .map_err(|_| invalid("Manifest row count exceeds i64"))?,
                        )
                        .ok_or_else(|| invalid("Manifest row ID inheritance overflows"))?;
                }
                inherited
            } else {
                None
            };
            if !include_deleted && !entry.is_alive() {
                continue;
            }
            let frozen =
                FrozenEntry::from_manifest_entry(entry, manifest.partition_spec_id, inherited)?;
            out.push((
                frozen.identity().clone(),
                LiveEntry {
                    frozen,
                    file: file.clone(),
                },
            ));
        }
        Ok(out)
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}
