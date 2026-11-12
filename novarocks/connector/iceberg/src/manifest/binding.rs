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

use std::slice::Iter;

use crate::iceberg::spec::{
    DataContentType, DataFile, ManifestEntry, ManifestEntryRef, ManifestFile, ManifestStatus,
    UNASSIGNED_SEQUENCE_NUMBER,
};
use crate::iceberg::{Error, ErrorKind, Result};

use super::source::ManifestSource;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EntrySequences {
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) data_sequence: Option<i64>,
    pub(crate) file_sequence: Option<i64>,
}

impl EntrySequences {
    pub(crate) fn resolve(entry: &ManifestEntry, descriptor: &ManifestFile, pending: bool) -> Self {
        Self::resolve_from(
            entry,
            descriptor.added_snapshot_id,
            descriptor.sequence_number,
            pending,
        )
    }

    pub(crate) fn resolve_from(
        entry: &ManifestEntry,
        added_snapshot_id: i64,
        manifest_sequence: i64,
        pending: bool,
    ) -> Self {
        // The initial sequence (zero) applies to V1 entries regardless of status.
        let inherits = entry.status == ManifestStatus::Added || manifest_sequence == 0;
        let assigned = (inherits && manifest_sequence != UNASSIGNED_SEQUENCE_NUMBER)
            .then_some(manifest_sequence);
        let resolve = |value: Option<i64>| {
            if pending && value == Some(UNASSIGNED_SEQUENCE_NUMBER) {
                assigned
            } else {
                value.or(assigned)
            }
        };
        Self {
            snapshot_id: entry.snapshot_id.or(Some(added_snapshot_id)),
            data_sequence: resolve(entry.sequence_number),
            file_sequence: resolve(entry.file_sequence_number),
        }
    }
}

/// Apply row lineage in physical order before selecting entries.
pub(crate) struct RowIdBinding {
    next: Option<i64>,
    pending: bool,
}

impl RowIdBinding {
    pub(crate) fn new(first_row_id: Option<i64>, pending: bool) -> Self {
        Self {
            next: first_row_id,
            pending,
        }
    }

    pub(crate) fn resolve(
        &mut self,
        file: &DataFile,
        status: ManifestStatus,
    ) -> Result<Option<i64>> {
        let Some(first) = self.next else {
            return Ok(if self.pending {
                file.first_row_id()
            } else {
                None
            });
        };
        if file.first_row_id().is_some()
            || status == ManifestStatus::Deleted
            || file.content_type() != DataContentType::Data
        {
            return Ok(file.first_row_id());
        }
        let count = i64::try_from(file.record_count()).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                "File row count exceeds row-ID range",
            )
        })?;
        self.next = Some(first.checked_add(count).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Manifest inherited row-ID range overflow",
            )
        })?);
        Ok(Some(first))
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BoundEntry<'a> {
    pub(crate) source: ManifestSource<'a>,
    pub(crate) ordinal: usize,
    pub(crate) raw: &'a ManifestEntry,
    pub(crate) sequences: EntrySequences,
    pub(crate) first_row_id: Option<i64>,
}

pub(crate) struct BoundEntries<'a> {
    source: ManifestSource<'a>,
    entries: std::iter::Enumerate<Iter<'a, ManifestEntryRef>>,
    row_ids: RowIdBinding,
}

impl<'a> BoundEntries<'a> {
    pub(super) fn new(source: ManifestSource<'a>, entries: &'a [ManifestEntryRef]) -> Result<Self> {
        let first_row_id = source
            .descriptor
            .first_row_id
            .map(i64::try_from)
            .transpose()
            .map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Manifest first row ID exceeds row-ID range",
                )
            })?;
        Ok(Self {
            source,
            entries: entries.iter().enumerate(),
            row_ids: RowIdBinding::new(first_row_id, source.origin.is_pending()),
        })
    }
}

impl<'a> Iterator for BoundEntries<'a> {
    type Item = Result<BoundEntry<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let (ordinal, raw) = self.entries.next()?;
        let first_row_id = self.row_ids.resolve(raw.data_file(), raw.status);
        Some(first_row_id.map(|first_row_id| BoundEntry {
            source: self.source,
            ordinal,
            raw,
            sequences: EntrySequences::resolve(
                raw,
                self.source.descriptor,
                self.source.origin.is_pending(),
            ),
            first_row_id,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::model::FrozenEntry;
    use crate::iceberg::spec::{
        DataFileBuilder, DataFileFormat, FormatVersion, Manifest, ManifestContentType,
        ManifestMetadata, PartitionSpec, Schema,
    };
    use std::sync::Arc;

    fn entry(name: &str, status: ManifestStatus, count: u64, row_id: Option<i64>) -> ManifestEntry {
        ManifestEntry::builder()
            .status(status)
            .data_file(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(format!("s3://bucket/{name}.parquet"))
                    .file_format(DataFileFormat::Parquet)
                    .record_count(count)
                    .file_size_in_bytes(100)
                    .first_row_id(row_id)
                    .build()
                    .unwrap(),
            )
            .build()
    }

    fn descriptor(first_row_id: Option<u64>) -> ManifestFile {
        ManifestFile {
            manifest_path: "s3://bucket/m.avro".into(),
            manifest_length: 100,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 6,
            min_sequence_number: 4,
            added_snapshot_id: 11,
            added_files_count: Some(2),
            existing_files_count: Some(1),
            deleted_files_count: Some(1),
            added_rows_count: Some(60),
            existing_rows_count: Some(25),
            deleted_rows_count: Some(60),
            partitions: None,
            key_metadata: None,
            first_row_id,
        }
    }

    #[test]
    fn one_raw_manifest_binds_lineage_before_selection() {
        let mut existing = entry("x", ManifestStatus::Existing, 25, Some(800));
        existing.snapshot_id = Some(7);
        existing.sequence_number = Some(4);
        existing.file_sequence_number = Some(5);
        let raw = Manifest::new(
            ManifestMetadata::builder()
                .schema(Arc::new(Schema::builder().build().unwrap()))
                .schema_id(0)
                .partition_spec(PartitionSpec::unpartition_spec())
                .format_version(FormatVersion::V3)
                .content(ManifestContentType::Data)
                .build(),
            vec![
                existing,
                entry("a", ManifestStatus::Added, 50, None),
                entry("deleted", ManifestStatus::Deleted, 60, None),
                entry("b", ManifestStatus::Added, 10, None),
            ],
        );
        let with_range = descriptor(Some(1000));
        let entries = ManifestSource::committed(11, "list", &with_range)
            .bind(&raw)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|e| (e.ordinal, e.first_row_id))
                .collect::<Vec<_>>(),
            [(0, Some(800)), (1, Some(1000)), (2, None), (3, Some(1050))]
        );
        assert_eq!(entries[0].sequences.data_sequence, Some(4));
        assert_eq!(entries[3].sequences.file_sequence, Some(6));
        assert_eq!(entries[3].sequences.snapshot_id, Some(11));

        let without_range = descriptor(None);
        let committed = ManifestSource::committed(11, "legacy-list", &without_range)
            .bind(&raw)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(committed.first_row_id, None);
        assert_eq!(
            FrozenEntry::from_bound_entry(&committed)
                .unwrap()
                .facts()
                .first_row_id,
            None
        );
        assert_eq!(committed.raw.data_file().first_row_id(), Some(800));
        assert_eq!(raw.entries()[3].sequence_number, None);
        assert_eq!(raw.entries()[3].data_file().first_row_id(), None);

        let mut pending = descriptor(None);
        pending.sequence_number = UNASSIGNED_SEQUENCE_NUMBER;
        let mut entries = ManifestSource::pending(11, &pending).bind(&raw).unwrap();
        assert_eq!(entries.next().unwrap().unwrap().first_row_id, Some(800));
        let added = entries.next().unwrap().unwrap();
        assert_eq!(added.sequences.data_sequence, None);
        assert_eq!(added.sequences.file_sequence, None);
    }

    #[test]
    fn inherited_row_id_overflow_is_an_error() {
        let mut row_ids = RowIdBinding::new(Some(i64::MAX), false);
        let added = entry("a", ManifestStatus::Added, 1, None);
        assert!(row_ids.resolve(added.data_file(), added.status).is_err());
    }
}
