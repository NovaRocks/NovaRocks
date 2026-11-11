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

use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use super::super::manifest_sources::duplicate_identity;
use super::super::manifest_sources::{LocatedEntry, SnapshotSources};
#[cfg(test)]
use super::super::model::FrozenEntry;
use super::super::model::{ArtifactWriter, EntryIdentity};
#[cfg(test)]
use super::{HistoryGap, HistoryWindow, history_window};
#[cfg(test)]
use crate::iceberg::spec::DataFile;
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::{Error, ErrorKind, Result};

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct LiveEntry {
    pub frozen: FrozenEntry,
    pub file: DataFile,
}

#[cfg(test)]
pub type LiveSet = BTreeMap<EntryIdentity, LiveEntry>;

pub struct ValidationInputs<'a> {
    pub metadata: &'a TableMetadata,
    pub parent: Option<i64>,
    io: &'a dyn ArtifactWriter,
    #[cfg(test)]
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
            #[cfg(test)]
            live: None,
        }
    }

    pub(crate) async fn live_data_matches(
        &self,
        paths: &BTreeSet<&str>,
    ) -> Result<BTreeSet<String>> {
        self.io.check_active()?;
        if paths.is_empty() {
            return Ok(BTreeSet::new());
        }
        let Some(parent) = self.parent else {
            return Ok(BTreeSet::new());
        };
        let snapshot = self.metadata.snapshot_by_id(parent).ok_or_else(|| {
            invalid(format!(
                "Target ref snapshot {parent} is absent from metadata"
            ))
        })?;
        let sources = SnapshotSources::load(self.metadata, snapshot, self.io.file_io(), || {
            self.io.check_active()
        })
        .await?;
        sources
            .live_data_matches(paths, self.io.file_io(), || self.io.check_active())
            .await
    }

    pub(crate) async fn compact_live_entries(
        &self,
    ) -> Result<BTreeMap<EntryIdentity, LocatedEntry>> {
        self.io.check_active()?;
        let Some(parent) = self.parent else {
            return Ok(BTreeMap::new());
        };
        let snapshot = self.metadata.snapshot_by_id(parent).ok_or_else(|| {
            invalid(format!(
                "Target ref snapshot {parent} is absent from metadata"
            ))
        })?;
        let sources = SnapshotSources::load(self.metadata, snapshot, self.io.file_io(), || {
            self.io.check_active()
        })
        .await?;
        sources
            .live_entries(self.io.file_io(), || self.io.check_active())
            .await
    }

    #[cfg(test)]
    pub async fn live_set(&mut self) -> Result<&LiveSet> {
        if self.live.is_none() {
            let mut live = LiveSet::new();
            if let Some(parent) = self.parent {
                let snapshot = self.metadata.snapshot_by_id(parent).ok_or_else(|| {
                    invalid(format!(
                        "Target ref snapshot {parent} is absent from metadata"
                    ))
                })?;
                let sources =
                    SnapshotSources::load(self.metadata, snapshot, self.io.file_io(), || {
                        self.io.check_active()
                    })
                    .await?;
                sources
                    .fold(
                        self.io.file_io(),
                        || self.io.check_active(),
                        |_| true,
                        |entry| {
                            if entry.raw.is_alive() {
                                let frozen = FrozenEntry::from_bound_entry(&entry)?;
                                let identity = frozen.identity().clone();
                                if live
                                    .insert(
                                        identity.clone(),
                                        LiveEntry {
                                            frozen,
                                            file: entry.raw.data_file().clone(),
                                        },
                                    )
                                    .is_some()
                                {
                                    return Err(duplicate_identity(&identity));
                                }
                            }
                            Ok(())
                        },
                    )
                    .await?;
            }
            self.live = Some(live);
        }
        Ok(self.live.as_ref().expect("live set initialized"))
    }

    #[cfg(test)]
    pub fn history(&self, start: Option<i64>) -> std::result::Result<HistoryWindow, HistoryGap> {
        history_window(self.metadata, start, self.parent)
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}
