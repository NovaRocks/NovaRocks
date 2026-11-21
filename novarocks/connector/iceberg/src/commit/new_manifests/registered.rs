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

//! Admission and sizing of caller-managed data files before rolling writes.

use std::alloc::Layout;
use std::mem::size_of;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};
use novarocks_type_contract::owned_resources::{hashmap::fresh_table_layout, layout::arc_layout};

use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Datum, PrimitiveLiteral, TableMetadata,
};
use crate::iceberg::{Error, ErrorKind, Result};

use super::super::model::{AddedContent, ObjectIdentity};
use super::super::write_stack::receiving::compact::{
    AddedCounters, PhysicalAdditionCounts, manifest_entry_encoded_lower_bound,
};
use super::{EntrySize, ManifestEntryWrite, invariant};

/// One paid input. The path pin is shared with the caller's candidate index.
/// The file pin covers the independent SDK DataFile backing consumed by the writer.
pub(crate) struct RegisteredFile {
    file: Option<DataFile>,
    path: ObjectIdentity,
    pub(super) size: EntrySize,
    path_retention: ConnectorPayloadRetentionGuard,
    _file_retention: ConnectorPayloadRetentionGuard,
}

impl RegisteredFile {
    pub(crate) fn try_new(
        file: DataFile,
        path: ObjectIdentity,
        path_retention: ConnectorPayloadRetentionGuard,
        file_retention: ConnectorPayloadRetentionGuard,
    ) -> Result<Self> {
        let mut entry = Self {
            file: Some(file),
            path,
            size: EntrySize::default(),
            path_retention,
            _file_retention: file_retention,
        };
        let file = entry.file.as_ref().expect("original registered file");
        if file.content_type() != DataContentType::Data
            || file.file_format() != DataFileFormat::Parquet
            || file.file_path() != entry.path.path()
            || !file.partition().fields().is_empty()
            || file.first_row_id().is_some()
        {
            return Err(invariant("Invalid Iceberg registered file admission"));
        }
        entry.size = registered_entry_size(file)?;
        Ok(entry)
    }

    pub(crate) fn path(&self) -> &str {
        self.path.path()
    }

    pub(super) fn take_for_manifest(&mut self, spec_id: i32) -> Result<ManifestEntryWrite> {
        let file = self
            .file
            .take()
            .ok_or_else(|| invariant("Iceberg registered file was already consumed"))?;
        Ok(ManifestEntryWrite::Added(AddedContent::new_logical_data(
            file, spec_id,
        )?))
    }
}

pub(crate) struct RegisteredNewFiles {
    pub(super) source: Arc<TableMetadata>,
    pub(super) entries: Vec<RegisteredFile>,
    _entries_retention: ConnectorPayloadRetentionGuard,
    counts: AddedCounters,
    byte_author: ConnectorPayloadRetentionGuard,
}

impl RegisteredNewFiles {
    pub(crate) fn try_new(
        source: Arc<TableMetadata>,
        entries: Vec<RegisteredFile>,
        entries_retention: ConnectorPayloadRetentionGuard,
        byte_author: ConnectorPayloadRetentionGuard,
    ) -> Result<Self> {
        let mut files = Self {
            source,
            entries,
            _entries_retention: entries_retention,
            counts: AddedCounters::default(),
            byte_author,
        };
        if !files.source.default_partition_spec().is_unpartitioned() {
            return Err(invariant(
                "Iceberg registration requires a captured unpartitioned spec",
            ));
        }
        if files
            .entries
            .windows(2)
            .any(|pair| pair[0].path >= pair[1].path)
        {
            return Err(invariant(
                "Iceberg registered files must be in unique path order",
            ));
        }
        for entry in &files.entries {
            let file = entry
                .file
                .as_ref()
                .ok_or_else(|| invariant("Iceberg registration has a consumed input"))?;
            files.counts = files
                .counts
                .checked_add(AddedCounters {
                    data: PhysicalAdditionCounts {
                        files: 1,
                        rows: file.record_count(),
                        bytes: file.file_size_in_bytes(),
                    },
                    ..Default::default()
                })
                .map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Iceberg registered file counters overflow",
                    )
                    .with_source(error)
                })?;
        }
        Ok(files)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn counts(&self) -> AddedCounters {
        self.counts
    }

    pub(crate) fn candidate_index(&self, path: &str) -> Option<usize> {
        self.entries
            .binary_search_by(|entry| entry.path.path().cmp(path))
            .ok()
    }

    pub(crate) fn candidate_path(&self, index: usize) -> Option<&str> {
        self.entries.get(index).map(|entry| entry.path.path())
    }

    pub(in crate::commit) fn retain_paths(&self) -> Result<Arc<RegisteredPaths>> {
        let entries = self
            .entries
            .iter()
            .map(|entry| (&entry.path, &entry.path_retention));
        RegisteredPaths::try_new(entries, &self.byte_author)
    }
}

struct RegisteredPath {
    identity: ObjectIdentity,
    _retained: ConnectorPayloadRetentionGuard,
}

/// Immutable paths used by manifest observations. They are never deletion targets.
pub(crate) struct RegisteredPaths {
    paths: Vec<RegisteredPath>,
    author: ConnectorPayloadRetentionGuard,
    _backing: ConnectorPayloadRetentionGuard,
}

impl RegisteredPaths {
    fn try_new<'a>(
        entries: impl ExactSizeIterator<Item = (&'a ObjectIdentity, &'a ConnectorPayloadRetentionGuard)>,
        author: &ConnectorPayloadRetentionGuard,
    ) -> Result<Arc<Self>> {
        let root_layout =
            arc_layout(Layout::new::<Self>()).map_err(|error| size_error(error.to_string()))?;
        let bytes = entries
            .len()
            .checked_mul(size_of::<RegisteredPath>())
            .and_then(|bytes| bytes.checked_add(root_layout.size()))
            .ok_or_else(|| size_error("Iceberg registered path index extent overflow"))?;
        let backing = reserve(author, bytes, "Iceberg registered path index")?;
        let mut paths = Vec::new();
        paths
            .try_reserve_exact(entries.len())
            .map_err(|error| size_error(error.to_string()))?;
        for (identity, retention) in entries {
            paths.push(RegisteredPath {
                identity: identity.clone(),
                _retained: clone_pin(retention)?,
            });
        }
        Ok(Arc::new(Self {
            paths,
            author: clone_pin(author)?,
            _backing: backing,
        }))
    }

    pub(crate) fn len(&self) -> usize {
        self.paths.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub(crate) fn get(&self, index: usize) -> Option<&str> {
        self.paths.get(index).map(|entry| entry.identity.path())
    }

    pub(crate) fn index(&self, path: &str) -> Option<usize> {
        self.paths
            .binary_search_by(|entry| entry.identity.path().cmp(path))
            .ok()
    }

    pub(in crate::commit) fn reserve(
        &self,
        bytes: usize,
        label: &'static str,
    ) -> Result<ConnectorPayloadRetentionGuard> {
        reserve(&self.author, bytes, label)
    }
}

fn clone_pin(guard: &ConnectorPayloadRetentionGuard) -> Result<ConnectorPayloadRetentionGuard> {
    guard.try_clone().map_err(|error| {
        Error::new(
            ErrorKind::Unexpected,
            "Iceberg registered input retention pin clone failed",
        )
        .with_source(error)
    })
}

fn reserve(
    author: &ConnectorPayloadRetentionGuard,
    bytes: usize,
    label: &'static str,
) -> Result<ConnectorPayloadRetentionGuard> {
    author
        .reserve_owned_bytes(bytes)
        .map_err(|error| Error::new(ErrorKind::Unexpected, label).with_source(error))
}

/// Allocation request for a Provider copy of a registered file just decoded
/// from an operation-owned manifest. The SDK creates its maps with capacity and
/// only inserts entries. There are no tombstones or aliased mutable sources.
pub(in crate::commit) fn decoded_file_clone_bytes(file: &DataFile) -> Result<usize> {
    let mut total = 0usize;
    let mut add = |bytes| -> Result<()> {
        total = total
            .checked_add(bytes)
            .ok_or_else(|| size_error("Iceberg registered DataFile clone extent overflow"))?;
        Ok(())
    };
    add(file.file_path().len())?;
    for metrics in [
        file.column_sizes(),
        file.value_counts(),
        file.null_value_counts(),
        file.nan_value_counts(),
    ] {
        add(fresh_table_layout::<i32, u64>(metrics.capacity())
            .map_err(|error| size_error(error.to_string()))?
            .layout
            .map_or(0, |layout| layout.size()))?;
    }
    for bounds in [file.lower_bounds(), file.upper_bounds()] {
        add(fresh_table_layout::<i32, Datum>(bounds.capacity())
            .map_err(|error| size_error(error.to_string()))?
            .layout
            .map_or(0, |layout| layout.size()))?;
        for value in bounds.values() {
            match value.literal() {
                PrimitiveLiteral::String(text) => add(text.len())?,
                PrimitiveLiteral::Binary(bytes) => add(bytes.len())?,
                _ => {}
            }
        }
    }
    if let Some(offsets) = file.split_offsets() {
        add(Layout::array::<i64>(offsets.len())
            .map_err(|error| size_error(error.to_string()))?
            .size())?;
    }
    add(file.key_metadata().map_or(0, |value| value.len()))?;
    Ok(total)
}

fn size_error(message: impl Into<String>) -> Error {
    let message = message.into();
    Error::new(ErrorKind::Unexpected, message.clone()).with_source(ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        message,
    ))
}

fn registered_entry_size(file: &DataFile) -> Result<EntrySize> {
    let path = file.file_path();
    let format = "PARQUET".len();
    let lower = manifest_entry_encoded_lower_bound(path, format).map_err(|error| {
        Error::new(ErrorKind::Unexpected, "Iceberg registered entry extent").with_source(error)
    })?;
    let mut layout = lower;
    let mut add = |bytes| -> Result<()> {
        layout = layout
            .checked_add(bytes)
            .ok_or_else(|| size_error("Iceberg registered entry layout estimate overflow"))?;
        Ok(())
    };
    // The lower bound omits optional fields and varint growth. Count their
    // Avro representation for layout only. This estimate is not a budget.
    add(avro_unsigned_bytes(path.len() as u64).saturating_sub(1))?;
    add(avro_unsigned_bytes(file.record_count()).saturating_sub(1))?;
    add(avro_unsigned_bytes(file.file_size_in_bytes()).saturating_sub(1))?;
    for metrics in [
        file.column_sizes(),
        file.value_counts(),
        file.null_value_counts(),
        file.nan_value_counts(),
    ] {
        add(2 + avro_unsigned_bytes(metrics.len() as u64))?;
        for (id, value) in metrics {
            add(avro_int_bytes(*id) + avro_unsigned_bytes(*value))?;
        }
    }
    for bounds in [file.lower_bounds(), file.upper_bounds()] {
        add(2 + avro_unsigned_bytes(bounds.len() as u64))?;
        for (id, datum) in bounds {
            let bytes = serialized_bound_bytes(datum)?;
            add(avro_int_bytes(*id) + avro_unsigned_bytes(bytes as u64))?;
            add(bytes)?;
        }
    }
    if let Some(offsets) = file.split_offsets() {
        add(2 + avro_unsigned_bytes(offsets.len() as u64))?;
        for offset in offsets {
            add(avro_signed_bytes(*offset))?;
        }
    }
    Ok(EntrySize {
        lower: u64::try_from(lower)
            .map_err(|_| size_error("Iceberg entry lower bound width overflow"))?,
        layout: u64::try_from(layout)
            .map_err(|_| size_error("Iceberg entry layout width overflow"))?,
    })
}

fn avro_int_bytes(value: i32) -> usize {
    avro_signed_bytes(i64::from(value))
}

fn avro_signed_bytes(value: i64) -> usize {
    let zigzag = ((value as u64) << 1) ^ ((value >> 63) as u64);
    (u64::BITS - zigzag.leading_zeros()).max(1).div_ceil(7) as usize
}

fn avro_unsigned_bytes(value: u64) -> usize {
    // The SDK validates the unsigned value against its signed storage width.
    // An out-of-range value can still be sized without overflowing this estimate.
    avro_signed_bytes(i64::try_from(value).unwrap_or(i64::MAX))
}

fn serialized_bound_bytes(datum: &Datum) -> Result<usize> {
    Ok(match datum.literal() {
        PrimitiveLiteral::Boolean(_) => 1,
        PrimitiveLiteral::Int(_) | PrimitiveLiteral::Float(_) => 4,
        PrimitiveLiteral::Long(_) | PrimitiveLiteral::Double(_) => 8,
        PrimitiveLiteral::UInt128(_) => 16,
        PrimitiveLiteral::String(value) => value.len(),
        PrimitiveLiteral::Binary(value) => value.len(),
        PrimitiveLiteral::Int128(value) => {
            let bytes = value.to_be_bytes();
            let redundant_sign_bytes = bytes
                .windows(2)
                .take_while(|pair| {
                    (pair[0] == 0 && pair[1] < 0x80) || (pair[0] == 0xff && pair[1] >= 0x80)
                })
                .count();
            bytes.len() - redundant_sign_bytes
        }
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
            return Err(invariant(
                "Iceberg registered bounds must have a binary encoding",
            ));
        }
    })
}
