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

//! Compact provider-owned inputs to the first publication preparation.

use novarocks_spi::connector::write_stack::{
    ConnectorCommitFragment, ConnectorWriterPhysicalContext,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};

use crate::commit::model::{EntryIdentity, SeqField};
use crate::iceberg::spec::ManifestContentType;
use crate::row_lineage_synth::{
    ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, ICEBERG_RESERVED_FIELD_ID_ROW_ID,
};
use crate::wire::dto::IcebergPartitionValueDescriptor as EncodedPartitionValue;

use super::super::codec::encode_compact_addition;
use super::super::domain::{
    IcebergArtifactMetrics, IcebergArtifactPartition, IcebergCommitArtifact, IcebergCommitFragment,
    IcebergDeleteTargetLocator, IcebergFinalDeleteEffect, IcebergFinalDeleteKind,
    IcebergWriteTableFacts, corrupt, invalid,
};
use super::super::runtime::IcebergWriteAdapter;

pub(crate) struct CompactEntry {
    pub(crate) addition: Option<CompactAddition>,
    pub(crate) delete_effect: Option<CompactDeleteEffect>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Stores the arrival counters alongside the paid input for custody assertions."
        )
    )]
    pub(crate) counts: AddedCounters,
    // Field order returns the original payment after the retained backing.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The receipt is returned by dropping the entry after actual write exit."
        )
    )]
    pub(crate) receipt: CompactEntryReceipt,
}

pub(crate) struct CompactAddition {
    pub(crate) object_path: std::sync::Arc<str>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the proven original writer identity through publication."
        )
    )]
    pub(crate) writer: Option<ConnectorWriterPhysicalContext>,
    pub(crate) partition_spec_id: i32,
    pub(crate) kind: AdditionKind,
    pub(crate) encoded_lower_bound: usize,
    pub(crate) data_sequence: SeqField,
    /// A rewrite marker is the smallest explicitly stored source row ID.
    pub(crate) first_row_id: Option<i64>,
    pub(crate) payload: Box<[u8]>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum AdditionKind {
    Data,
    PositionDelete,
    EqualityDelete,
}

impl AdditionKind {
    pub(crate) const fn manifest_content(self) -> ManifestContentType {
        match self {
            Self::Data => ManifestContentType::Data,
            Self::PositionDelete | Self::EqualityDelete => ManifestContentType::Deletes,
        }
    }
}

pub(crate) struct CompactDeleteEffect {
    pub(crate) target: IcebergDeleteTargetLocator,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Preserves the exact logical effect alongside the physical artifact."
        )
    )]
    pub(crate) newly_deleted_rows: u64,
    pub(crate) kind: CompactDeleteKind,
}

pub(crate) enum CompactDeleteKind {
    RemoveDataFile {
        partition: IcebergArtifactPartition,
        old_exclusive_deletes: Box<[EntryIdentity]>,
    },
    ReplacePositions {
        partition: Box<IcebergArtifactPartition>,
        merged_old_references: Vec<EntryIdentity>,
    },
}

pub(crate) struct CompactEntryReceipt {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The carried original decode quote is observable at custody tests."
        )
    )]
    pub(crate) original_bytes: usize,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The distinct top-up amount is observable at custody tests."
        )
    )]
    pub(crate) additional_bytes: usize,
    _original: ConnectorPayloadRetentionGuard,
    _additional: Option<ConnectorPayloadRetentionGuard>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PhysicalAdditionCounts {
    pub(crate) files: u64,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

impl PhysicalAdditionCounts {
    fn checked_add(self, rhs: Self) -> Result<Self, ConnectorError> {
        Ok(Self {
            files: checked_counter(self.files, rhs.files)?,
            rows: checked_counter(self.rows, rhs.rows)?,
            bytes: checked_counter(self.bytes, rhs.bytes)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AddedCounters {
    pub(crate) data: PhysicalAdditionCounts,
    pub(crate) position_deletes: PhysicalAdditionCounts,
    pub(crate) equality_deletes: PhysicalAdditionCounts,
    /// Only an original final effect reports newly deleted visible rows.
    pub(crate) newly_deleted_rows: u64,
}

impl AddedCounters {
    pub(crate) fn checked_add(self, rhs: Self) -> Result<Self, ConnectorError> {
        Ok(Self {
            data: self.data.checked_add(rhs.data)?,
            position_deletes: self.position_deletes.checked_add(rhs.position_deletes)?,
            equality_deletes: self.equality_deletes.checked_add(rhs.equality_deletes)?,
            newly_deleted_rows: checked_counter(self.newly_deleted_rows, rhs.newly_deleted_rows)?,
        })
    }
}

fn checked_counter(left: u64, right: u64) -> Result<u64, ConnectorError> {
    left.checked_add(right)
        .ok_or_else(|| corrupt("Iceberg compact entry counter overflow"))
}

fn sum_bytes(bytes: impl IntoIterator<Item = usize>) -> Result<usize, ConnectorError> {
    bytes.into_iter().try_fold(0usize, |total, bytes| {
        total.checked_add(bytes).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg compact entry retained size overflow",
            )
        })
    })
}

/// All independently quoted backing for one already admitted fragment.
/// Completing a record can install index backing while these guards are live.
#[must_use]
pub(crate) struct PreparedCompactEntry {
    // Observe identity only. The typed pointee inside the neutral slot's Arc
    // has a stable address even when the caller moves the neutral handle.
    original: *const IcebergCommitFragment,
    original_bytes: usize,
    payload_bytes: usize,
    data_sequence: SeqField,
    first_row_id: Option<i64>,
    counts: AddedCounters,
    additional_bytes: usize,
    additional: Option<ConnectorPayloadRetentionGuard>,
    overlap: Option<ConnectorPayloadRetentionGuard>,
}

impl CompactEntry {
    /// Reserve for an already admitted fragment before any replacement or new
    /// index backing is installed. A refusal leaves the original value paid.
    pub(crate) fn prepare(
        adapter: &IcebergWriteAdapter,
        original: &ConnectorCommitFragment,
        index_bytes: usize,
        author: &ConnectorPayloadRetentionGuard,
        rewrite: Option<&IcebergWriteTableFacts>,
    ) -> Result<PreparedCompactEntry, ConnectorError> {
        let (fragment, original_bytes) = adapter.measured_commit_fragment(original)?;
        let (data_sequence, first_row_id) = addition_lineage(fragment, rewrite)?;
        let payload_bytes = fragment.compact_payload_bytes().ok_or_else(|| {
            invalid("Iceberg artifact has no admitted compact addition projection")
        })?;
        let counts = addition_counts(fragment)?;
        let replacement_partition = match fragment.artifact() {
            IcebergCommitArtifact::FinalDelete(effect) => match effect.kind() {
                IcebergFinalDeleteKind::ReplacePositions(vector) => sum_bytes([
                    std::mem::size_of::<IcebergArtifactPartition>(),
                    partition_copy_bytes(vector.partition())?,
                ])?,
                IcebergFinalDeleteKind::RemoveDataFile { .. } => 0,
            },
            _ => 0,
        };
        let effect_bytes = match fragment.artifact() {
            IcebergCommitArtifact::FinalDelete(effect) => effect_backing_bytes(effect)?,
            _ => 0,
        };
        let object_bytes = fragment
            .new_object_path()
            .map(object_path_bytes)
            .transpose()?
            .unwrap_or(0);
        let retained_bytes = sum_bytes([
            retained_entry_node_bytes(),
            payload_bytes,
            object_bytes,
            effect_bytes,
            replacement_partition,
            index_bytes,
        ])?;
        let scratch_bytes = if payload_bytes == 0 {
            0
        } else {
            // The original and DTO partition descriptor vectors coexist
            // during the move. The metric maps and referenced paths move.
            let partition = fragment
                .partition()
                .ok_or_else(|| corrupt("Iceberg compact addition has no artifact partition"))?;
            sum_bytes(
                partition
                    .descriptor()
                    .values
                    .iter()
                    .map(|_| std::mem::size_of::<EncodedPartitionValue>()),
            )?
        };
        let additional_bytes = retained_bytes.saturating_sub(original_bytes);
        let additional = reserve_if_occupied(author, additional_bytes)?;
        let overlap = sum_bytes([
            payload_bytes,
            object_bytes,
            index_bytes,
            replacement_partition,
            scratch_bytes,
        ])?
        .saturating_sub(additional_bytes);
        let overlap = reserve_if_occupied(author, overlap)?;
        Ok(PreparedCompactEntry {
            original: std::ptr::from_ref(fragment),
            original_bytes,
            payload_bytes,
            data_sequence,
            first_row_id,
            counts,
            additional_bytes,
            additional,
            overlap,
        })
    }

    #[cfg(test)]
    pub(in crate::commit) fn from_arrival(
        adapter: &IcebergWriteAdapter,
        original: &mut Option<ConnectorCommitFragment>,
        index_bytes: usize,
        author: &ConnectorPayloadRetentionGuard,
    ) -> Result<Self, ConnectorError> {
        let fragment = original
            .as_ref()
            .ok_or_else(|| invalid("empty compact arrival"))?;
        Self::prepare(adapter, fragment, index_bytes, author, None)?.finish(adapter, original)
    }
}

impl PreparedCompactEntry {
    pub(crate) fn counts(&self) -> AddedCounters {
        self.counts
    }

    /// Move the exact prepared fragment and its original decode receipt. The
    /// caller retains any paid index backing only for this prepared record.
    pub(crate) fn finish(
        &mut self,
        adapter: &IcebergWriteAdapter,
        original: &mut Option<ConnectorCommitFragment>,
    ) -> Result<CompactEntry, ConnectorError> {
        let fragment = original
            .as_ref()
            .ok_or_else(|| invalid("empty compact arrival"))?;
        let (fragment, original_bytes) = adapter.measured_commit_fragment(fragment)?;
        if !std::ptr::eq(self.original, fragment) || original_bytes != self.original_bytes {
            return Err(invalid(
                "compact arrival differs from its original paid fragment",
            ));
        }
        // Replacement backing can now allocate under the same paid window.
        let addition = fragment
            .new_object_path()
            .map(|path| addition_metadata(fragment, path))
            .transpose()?;
        let (fragment, original_receipt) = adapter.take_measured_commit_fragment(original)?;
        let (artifact, delete_effect) = split_effect(fragment.into_artifact());
        let addition = match (addition, artifact) {
            (Some(addition), Some(artifact)) => Some(CompactAddition {
                object_path: addition.object_path,
                writer: addition.writer,
                partition_spec_id: addition.partition_spec_id,
                kind: addition.kind,
                encoded_lower_bound: addition.encoded_lower_bound,
                data_sequence: self.data_sequence,
                first_row_id: self.first_row_id,
                payload: encode_compact_addition(artifact, self.payload_bytes)?,
            }),
            (None, None) => None,
            _ => {
                return Err(corrupt(
                    "Iceberg compact artifact and delete effect disagree",
                ));
            }
        };
        let receipt = CompactEntryReceipt {
            original_bytes: self.original_bytes,
            additional_bytes: self.additional_bytes,
            _original: original_receipt,
            _additional: self.additional.take(),
        };
        drop(self.overlap.take());
        Ok(CompactEntry {
            addition,
            delete_effect,
            counts: self.counts,
            receipt,
        })
    }
}

fn retained_entry_node_bytes() -> usize {
    // LinkedList owns two pointer-sized links with the paid entry at the node.
    std::mem::size_of::<(CompactEntry, [usize; 2])>()
}

fn object_path_bytes(path: &str) -> Result<usize, ConnectorError> {
    // The owned path is an Arc<str> so the ledger can adopt its allocation.
    let header = std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>();
    std::alloc::Layout::array::<u8>(path.len())
        .and_then(|bytes| header.extend(bytes))
        .map(|(allocation, _)| allocation.pad_to_align().size())
        .map_err(|_| corrupt("Iceberg compact object path layout overflow"))
}

fn reserve_if_occupied(
    author: &ConnectorPayloadRetentionGuard,
    bytes: usize,
) -> Result<Option<ConnectorPayloadRetentionGuard>, ConnectorError> {
    if bytes == 0 {
        Ok(None)
    } else {
        author.reserve_owned_bytes(bytes).map(Some)
    }
}

struct AdditionMetadata {
    object_path: std::sync::Arc<str>,
    writer: Option<ConnectorWriterPhysicalContext>,
    partition_spec_id: i32,
    kind: AdditionKind,
    encoded_lower_bound: usize,
}

pub(crate) fn manifest_entry_encoded_lower_bound(
    path: &str,
    format_bytes: usize,
) -> Result<usize, ConnectorError> {
    // Both manifest encodings write a status, a file path and format with
    // length prefixes, a record count and a file size. Each scalar/prefix
    // consumes at least one byte. Optional/format-specific fields add no bound.
    sum_bytes([path.len(), format_bytes, 5])
}

fn addition_metadata(
    fragment: &IcebergCommitFragment,
    path: &str,
) -> Result<AdditionMetadata, ConnectorError> {
    let kind = match fragment.artifact() {
        IcebergCommitArtifact::DataFile(_) => AdditionKind::Data,
        IcebergCommitArtifact::EqualityDeleteFile(_) => AdditionKind::EqualityDelete,
        IcebergCommitArtifact::PositionDeleteFile(_)
        | IcebergCommitArtifact::DeletionVector(_)
        | IcebergCommitArtifact::FinalDelete(_) => AdditionKind::PositionDelete,
    };
    let is_vector = matches!(
        fragment.artifact(),
        IcebergCommitArtifact::DeletionVector(_)
            | IcebergCommitArtifact::FinalDelete(IcebergFinalDeleteEffect {
                kind: IcebergFinalDeleteKind::ReplacePositions(_),
                ..
            })
    );
    let partition_spec_id = fragment
        .partition()
        .ok_or_else(|| corrupt("Iceberg compact addition has no artifact partition"))?
        .partition_spec_id();
    let format_bytes = if is_vector {
        "PUFFIN".len()
    } else {
        "PARQUET".len()
    };
    let encoded_lower_bound = manifest_entry_encoded_lower_bound(path, format_bytes)?;
    Ok(AdditionMetadata {
        object_path: path.into(),
        writer: fragment.creation().map(|receipt| *receipt.physical()),
        partition_spec_id,
        kind,
        encoded_lower_bound,
    })
}

fn addition_lineage(
    fragment: &IcebergCommitFragment,
    rewrite: Option<&IcebergWriteTableFacts>,
) -> Result<(SeqField, Option<i64>), ConnectorError> {
    let IcebergCommitArtifact::DataFile(file) = fragment.artifact() else {
        if rewrite.is_some() {
            return Err(corrupt("Iceberg rewrite received a non-data addition"));
        }
        return Ok((SeqField::Inherit, None));
    };
    let Some(table) = rewrite else {
        return Ok((SeqField::Inherit, file.first_row_id()));
    };
    let data_sequence = if table.format_version() == 1 {
        SeqField::Inherit
    } else {
        // IcebergWriteTableFacts has already checked the frozen sequence.
        SeqField::Explicit(table.base_sequence_number())
    };
    let first_row_id = if table.format_version() == 3 {
        Some(certify_preserved_row_id(
            file.metrics(),
            file.first_row_id(),
        )?)
    } else {
        None
    };
    Ok((data_sequence, first_row_id))
}

fn long_bound(
    bounds: &std::collections::BTreeMap<i32, Vec<u8>>,
    field: i32,
) -> Result<i64, ConnectorError> {
    let bytes = bounds
        .get(&field)
        .ok_or_else(|| corrupt("Preserved rewrite is missing a stored lineage bound"))?;
    let bytes: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| corrupt("Preserved rewrite stored lineage bound is not an Iceberg Long"))?;
    let value = i64::from_le_bytes(bytes);
    if value < 0 {
        return Err(corrupt(
            "Preserved rewrite stored lineage bound is negative",
        ));
    }
    Ok(value)
}

pub(crate) fn certify_preserved_row_id(
    metrics: &IcebergArtifactMetrics,
    marker: Option<i64>,
) -> Result<i64, ConnectorError> {
    let stats = metrics
        .column_stats()
        .ok_or_else(|| corrupt("Preserved rewrite output is missing physical row-id statistics"))?;
    if stats
        .null_value_counts
        .get(&ICEBERG_RESERVED_FIELD_ID_ROW_ID)
        != Some(&0)
    {
        return Err(corrupt(
            "Preserved rewrite requires a proven non-null stored row-id column",
        ));
    }
    let first_row_id = long_bound(&stats.lower_bounds, ICEBERG_RESERVED_FIELD_ID_ROW_ID)?;
    if stats
        .null_value_counts
        .get(&ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
        != Some(&0)
    {
        return Err(corrupt(
            "Preserved rewrite requires proven non-null stored last-updated sequences",
        ));
    }
    let last_updated_min = long_bound(
        &stats.lower_bounds,
        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    )?;
    let last_updated_max = long_bound(
        &stats.upper_bounds,
        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    )?;
    if last_updated_min > last_updated_max {
        return Err(corrupt(
            "Preserved rewrite stored last-updated sequence bounds are reversed",
        ));
    }
    if marker.is_some_and(|marker| marker != first_row_id) {
        return Err(corrupt(
            "Preserved rewrite first-row-id marker differs from the physical row-id minimum",
        ));
    }
    Ok(first_row_id)
}

fn addition_counts(fragment: &IcebergCommitFragment) -> Result<AddedCounters, ConnectorError> {
    let mut counts = AddedCounters::default();
    let vector = match fragment.artifact() {
        IcebergCommitArtifact::DataFile(file) => {
            counts.data = PhysicalAdditionCounts {
                files: 1,
                rows: file.metrics().record_count(),
                bytes: file.metrics().file_size_in_bytes(),
            };
            return Ok(counts);
        }
        IcebergCommitArtifact::PositionDeleteFile(file) => {
            counts.position_deletes = PhysicalAdditionCounts {
                files: 1,
                rows: file.metrics().record_count(),
                bytes: file.metrics().file_size_in_bytes(),
            };
            return Ok(counts);
        }
        IcebergCommitArtifact::EqualityDeleteFile(file) => {
            counts.equality_deletes = PhysicalAdditionCounts {
                files: 1,
                rows: file.metrics().record_count(),
                bytes: file.metrics().file_size_in_bytes(),
            };
            return Ok(counts);
        }
        IcebergCommitArtifact::DeletionVector(vector) => vector,
        IcebergCommitArtifact::FinalDelete(effect) => {
            counts.newly_deleted_rows = effect.newly_deleted_rows();
            match effect.kind() {
                IcebergFinalDeleteKind::ReplacePositions(vector) => vector,
                IcebergFinalDeleteKind::RemoveDataFile { .. } => return Ok(counts),
            }
        }
    };
    counts.position_deletes = PhysicalAdditionCounts {
        files: 1,
        rows: vector.cardinality(),
        bytes: u64::try_from(vector.content_range().size_in_bytes())
            .map_err(|_| corrupt("Iceberg compact DV size is negative"))?,
    };
    Ok(counts)
}

pub(super) fn partition_copy_bytes(
    partition: &IcebergArtifactPartition,
) -> Result<usize, ConnectorError> {
    let values = &partition.descriptor().values;
    let slots = values
        .len()
        .checked_mul(std::mem::size_of::<
            crate::write_descriptor::IcebergPartitionValueDescriptor,
        >())
        .ok_or_else(|| corrupt("Iceberg compact partition geometry overflow"))?;
    sum_bytes(
        [
            partition.partition_path().len(),
            partition.null_fingerprint().len(),
            slots,
        ]
        .into_iter()
        .chain(
            values
                .iter()
                .map(|value| value.datum_bytes.as_ref().map_or(0, Vec::len)),
        ),
    )
}

pub(super) fn position_target_index_bytes(
    path: &str,
    partition: &IcebergArtifactPartition,
) -> Result<usize, ConnectorError> {
    sum_bytes([
        std::mem::size_of::<(String, IcebergArtifactPartition)>(),
        path.len(),
        partition_copy_bytes(partition)?,
    ])
}

fn effect_backing_bytes(effect: &IcebergFinalDeleteEffect) -> Result<usize, ConnectorError> {
    let extra = match effect.kind() {
        IcebergFinalDeleteKind::RemoveDataFile {
            partition,
            old_exclusive_deletes,
        } => sum_bytes([
            partition.partition_path.capacity(),
            partition.null_fingerprint.capacity(),
            partition
                .descriptor
                .values
                .capacity()
                .checked_mul(std::mem::size_of::<
                    crate::write_descriptor::IcebergPartitionValueDescriptor,
                >())
                .ok_or_else(|| corrupt("Iceberg compact partition geometry overflow"))?,
            sum_bytes(
                partition
                    .descriptor
                    .values
                    .iter()
                    .map(|value| value.datum_bytes.as_ref().map_or(0, Vec::capacity)),
            )?,
            references_backing_bytes(old_exclusive_deletes, old_exclusive_deletes.len())?,
        ])?,
        IcebergFinalDeleteKind::ReplacePositions(vector) => {
            let refs = &vector.merged_old_references;
            references_backing_bytes(refs, refs.capacity())?
        }
    };
    sum_bytes([
        effect.target.manifest_list.capacity(),
        effect.target.manifest_path.capacity(),
        effect.target.data_file_path.capacity(),
        extra,
    ])
}

fn references_backing_bytes(
    refs: &[EntryIdentity],
    capacity: usize,
) -> Result<usize, ConnectorError> {
    let slots = capacity
        .checked_mul(std::mem::size_of::<EntryIdentity>())
        .ok_or_else(|| corrupt("Iceberg compact delete reference geometry overflow"))?;
    sum_bytes(
        std::iter::once(slots).chain(refs.iter().flat_map(|identity| match identity {
            EntryIdentity::DataFile { path } | EntryIdentity::DeleteFile { path } => {
                [path.capacity(), 0]
            }
            EntryIdentity::DeletionVector {
                path,
                referenced_data_file,
                ..
            } => [path.capacity(), referenced_data_file.capacity()],
        })),
    )
}

fn split_effect(
    artifact: IcebergCommitArtifact,
) -> (Option<IcebergCommitArtifact>, Option<CompactDeleteEffect>) {
    let IcebergCommitArtifact::FinalDelete(effect) = artifact else {
        return (Some(artifact), None);
    };
    let IcebergFinalDeleteEffect {
        target,
        kind,
        newly_deleted_rows,
    } = effect;
    let (addition, kind) = match kind {
        IcebergFinalDeleteKind::RemoveDataFile {
            partition,
            old_exclusive_deletes,
        } => (
            None,
            CompactDeleteKind::RemoveDataFile {
                partition,
                old_exclusive_deletes,
            },
        ),
        IcebergFinalDeleteKind::ReplacePositions(vector) => {
            let mut vector = *vector;
            let merged_old_references = std::mem::take(&mut vector.merged_old_references);
            let partition = Box::new(vector.partition().clone());
            (
                Some(IcebergCommitArtifact::DeletionVector(vector)),
                CompactDeleteKind::ReplacePositions {
                    partition,
                    merged_old_references,
                },
            )
        }
    };
    (
        addition,
        Some(CompactDeleteEffect {
            target,
            kind,
            newly_deleted_rows: newly_deleted_rows.get(),
        }),
    )
}

#[cfg(test)]
mod tests;
