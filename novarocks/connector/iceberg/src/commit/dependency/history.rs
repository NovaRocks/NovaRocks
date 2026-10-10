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

use crate::iceberg::spec::{Operation, SnapshotRef, TableMetadata};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryGap {
    StartExpired(i64),
    SnapshotMissing(i64),
    StartNotAncestor { start: i64, parent: Option<i64> },
    Cycle(i64),
}

#[derive(Debug)]
pub struct HistoryWindow {
    /// Newest first; excludes the operation's start snapshot.
    snapshots: Vec<SnapshotRef>,
}

impl HistoryWindow {
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
    if let Some(start) = start {
        if metadata.snapshot_by_id(start).is_none() {
            return Err(HistoryGap::StartExpired(start));
        }
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
    Ok(HistoryWindow { snapshots })
}
