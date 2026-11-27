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

use novarocks_spi::connector::ConnectorError;

use crate::iceberg::spec::{Manifest, ManifestEntry, ManifestStatus};
use crate::manifest::binding::{EntrySequences, RowIdBinding};
use crate::typed_read::system_table::{IcebergSystemTableType, TrinoManifestFile};

use super::corrupt;

pub(super) struct EntryView<'a> {
    pub(super) entry: &'a ManifestEntry,
    pub(super) ordinal: usize,
    pub(super) sequences: EntrySequences,
    pub(super) first_row_id: Option<i64>,
}

#[derive(Clone, Copy)]
pub(super) struct EntryPositionFact {
    ordinal: usize,
    sequences: EntrySequences,
    first_row_id: Option<i64>,
}

impl EntryView<'_> {
    pub(super) fn position(&self) -> EntryPositionFact {
        EntryPositionFact {
            ordinal: self.ordinal,
            sequences: self.sequences,
            first_row_id: self.first_row_id,
        }
    }
}

impl EntryPositionFact {
    pub(super) fn view(self, manifest: &Manifest) -> EntryView<'_> {
        EntryView {
            entry: &manifest.entries()[self.ordinal],
            ordinal: self.ordinal,
            sequences: self.sequences,
            first_row_id: self.first_row_id,
        }
    }
}

/// Cursor state for one authorized manifest. The source and position remain
/// distinct so a page can borrow several entries from the same decoded input.
pub(super) struct EntryPosition {
    next: usize,
    files: bool,
    row_ids: RowIdBinding,
}

impl EntryPosition {
    pub(super) fn new(kind: IcebergSystemTableType, manifest: &TrinoManifestFile) -> Self {
        Self {
            next: 0,
            files: kind == IcebergSystemTableType::Files,
            row_ids: RowIdBinding::new(manifest.first_row_id(), false),
        }
    }

    pub(super) fn has_entry(&self, source: &Manifest) -> bool {
        self.next < source.entries().len()
    }

    /// One physical entry spends work even if it is a tombstone filtered out
    /// of Files. Entries retains it and reports the original file facts.
    pub(super) fn take<'a>(
        &mut self,
        source: &'a Manifest,
        descriptor: &TrinoManifestFile,
    ) -> Result<Option<EntryView<'a>>, ConnectorError> {
        let ordinal = self.next;
        let Some(entry) = source.entries().get(ordinal) else {
            return Ok(None);
        };
        self.next += 1;
        let status = entry.status();
        let file = entry.data_file();
        let first_row_id = if self.files {
            self.row_ids
                .resolve(file, status)
                .map_err(|error| corrupt(error.to_string()))?
        } else {
            file.first_row_id()
        };
        if self.files && status == ManifestStatus::Deleted {
            return Ok(None);
        }
        Ok(Some(EntryView {
            entry,
            ordinal,
            sequences: EntrySequences::resolve_from(
                entry,
                descriptor.added_snapshot_id(),
                descriptor.sequence_number(),
                false,
            ),
            first_row_id,
        }))
    }
}
