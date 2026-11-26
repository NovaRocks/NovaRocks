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

//! Iceberg's parent links and the format-defined effect of each snapshot.

use crate::iceberg::spec::{Operation, Snapshot, TableMetadata};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotDataEffect {
    Preserves,
    Adds,
    Removes,
    MayAddAndRemove,
}

pub(crate) struct SnapshotHistory<'a> {
    metadata: &'a TableMetadata,
    next_id: Option<i64>,
}

pub(crate) struct SnapshotHistoryStep<'a> {
    pub(crate) snapshot: &'a Snapshot,
    pub(crate) data_effect: SnapshotDataEffect,
}

impl<'a> SnapshotHistory<'a> {
    pub(crate) const fn new(metadata: &'a TableMetadata, descendant: i64) -> Self {
        Self {
            metadata,
            next_id: Some(descendant),
        }
    }

    /// Check this identity before asking for the next body. A parent link can
    /// prove arrival at an ancestor whose snapshot body has expired.
    pub(crate) const fn next_id(&self) -> Option<i64> {
        self.next_id
    }
}

impl<'a> Iterator for SnapshotHistory<'a> {
    type Item = SnapshotHistoryStep<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let snapshot = self.metadata.snapshot_by_id(self.next_id.take()?)?;
        self.next_id = snapshot.parent_snapshot_id();
        let data_effect = match snapshot.summary().operation {
            Operation::Replace => SnapshotDataEffect::Preserves,
            Operation::Append => SnapshotDataEffect::Adds,
            Operation::Delete => SnapshotDataEffect::Removes,
            Operation::Overwrite => SnapshotDataEffect::MayAddAndRemove,
        };
        Some(SnapshotHistoryStep {
            snapshot,
            data_effect,
        })
    }
}
