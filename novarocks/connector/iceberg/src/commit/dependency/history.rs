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

//! History is an exact ancestor interval, never a sequence-number approximation.

use std::collections::BTreeSet;

use crate::commit::manifest_sources::SnapshotSources;
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, ManifestContentType, ManifestStatus, Operation, SnapshotRef, TableMetadata,
};
use crate::iceberg::{Error, ErrorKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryGap {
    StartExpired(i64),
    SnapshotMissing(i64),
    Cycle(i64),
    StartNotAncestor { start: i64, parent: Option<i64> },
}

#[derive(Debug)]
pub struct HistoryWindow {
    start: Option<i64>,
    parent: Option<i64>,
    /// Newest first; excludes the operation's start snapshot.
    snapshots: Vec<SnapshotRef>,
}

impl HistoryWindow {
    pub fn endpoints(&self) -> (Option<i64>, Option<i64>) {
        (self.start, self.parent)
    }
    pub fn snapshots(&self) -> &[SnapshotRef] {
        &self.snapshots
    }
    pub fn filtered<'a>(
        &'a self,
        operations: &'a [Operation],
    ) -> impl Iterator<Item = &'a SnapshotRef> {
        self.snapshots
            .iter()
            .filter(move |snapshot| operations.contains(&snapshot.summary().operation))
    }
}

pub fn history_window(
    metadata: &TableMetadata,
    start: Option<i64>,
    parent: Option<i64>,
) -> Result<HistoryWindow, HistoryGap> {
    if let Some(start) = start
        && metadata.snapshot_by_id(start).is_none()
    {
        return Err(HistoryGap::StartExpired(start));
    }
    let mut next = parent;
    let mut visited = BTreeSet::new();
    let mut snapshots = Vec::new();
    while next != start {
        let Some(id) = next else {
            return Err(HistoryGap::StartNotAncestor {
                start: start.expect("unequal absent endpoints"),
                parent,
            });
        };
        if !visited.insert(id) {
            return Err(HistoryGap::Cycle(id));
        }
        let snapshot = metadata
            .snapshot_by_id(id)
            .ok_or(HistoryGap::SnapshotMissing(id))?;
        snapshots.push(snapshot.clone());
        next = snapshot.parent_snapshot_id();
    }
    Ok(HistoryWindow {
        start,
        parent,
        snapshots,
    })
}

#[derive(Debug)]
pub enum HistoryObservationError {
    Gap(HistoryGap),
    Read(Error),
}

#[derive(Debug)]
pub struct PhysicalAddedData {
    pub window: HistoryWindow,
    pub paths: BTreeSet<String>,
}

/// Read only manifests newly written by snapshots in the exact ancestor window.
/// Physical data paths include REPLACE. A manifest-only rewrite carries existing
/// entries and cannot add a path to this result.
pub async fn physical_added_data(
    metadata: &TableMetadata,
    start: Option<i64>,
    parent: Option<i64>,
    io: &FileIO,
    active: impl Fn() -> crate::iceberg::Result<()>,
) -> Result<PhysicalAddedData, HistoryObservationError> {
    active().map_err(HistoryObservationError::Read)?;
    let window = history_window(metadata, start, parent).map_err(HistoryObservationError::Gap)?;
    let window_ids: BTreeSet<_> = window.snapshots().iter().map(|s| s.snapshot_id()).collect();
    let mut paths = BTreeSet::new();
    for snapshot in window.snapshots() {
        let sources = SnapshotSources::load(metadata, snapshot, io, &active)
            .await
            .map_err(HistoryObservationError::Read)?;
        // With no known E, the oldest snapshot has to prove an actual
        // initial live set. A disconnected import may look parentless while
        // its retained manifests name an owner outside this history.
        let initial_root = start.is_none() && snapshot.parent_snapshot_id().is_none();
        if initial_root {
            for source in sources.sources() {
                let descriptor = source.descriptor;
                if descriptor.content == ManifestContentType::Data
                    && descriptor.added_snapshot_id != snapshot.snapshot_id()
                    && (descriptor.has_added_files() || descriptor.has_existing_files())
                {
                    return Err(HistoryObservationError::Read(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "History root {} cannot prove data from manifest {}",
                            snapshot.snapshot_id(),
                            descriptor.manifest_path
                        ),
                    )));
                }
            }
        }
        sources
            .fold(
                io,
                &active,
                |descriptor| {
                    descriptor.added_snapshot_id == snapshot.snapshot_id()
                        && descriptor.content == ManifestContentType::Data
                },
                |entry| {
                    if initial_root
                        && entry.raw.is_alive()
                        && (entry.raw.status != ManifestStatus::Added
                            || entry.sequences.snapshot_id != Some(snapshot.snapshot_id()))
                    {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "History root {} cannot prove the physical addition of {}",
                                snapshot.snapshot_id(),
                                entry.raw.file_path()
                            ),
                        ));
                    }
                    if entry.raw.status == ManifestStatus::Added
                        && entry.raw.content_type() == DataContentType::Data
                        && entry
                            .sequences
                            .snapshot_id
                            .is_some_and(|id| window_ids.contains(&id))
                    {
                        paths.insert(entry.raw.file_path().to_owned());
                    }
                    Ok(())
                },
            )
            .await
            .map_err(HistoryObservationError::Read)?;
    }
    active().map_err(HistoryObservationError::Read)?;
    Ok(PhysicalAddedData { window, paths })
}
