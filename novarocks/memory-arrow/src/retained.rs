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

//! Immutable backing lineage carried with each retained Arrow output.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow::array::RecordBatch;
use arrow_buffer::Buffer;
use novarocks_memory::ids::AccountId;
use novarocks_memory::{CapacityError, ReservationLease};

use crate::backing::{Backing, BackingCollector, BackingError, BackingProvenance};
use crate::domain::{CensusRoot, RetentionDomain};

const ARC_HEADER_BYTES: usize = 2 * size_of::<usize>();

#[derive(Debug)]
pub enum RetainError {
    Backing(BackingError),
    Capacity(CapacityError),
    MetadataOverflow,
    DifferentAuthority,
}

impl fmt::Display for RetainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backing(error) => error.fmt(f),
            Self::Capacity(error) => error.fmt(f),
            Self::MetadataOverflow => write!(f, "retention metadata size overflowed"),
            Self::DifferentAuthority => write!(f, "retention source belongs to another authority"),
        }
    }
}

impl std::error::Error for RetainError {}

impl From<BackingError> for RetainError {
    fn from(value: BackingError) -> Self {
        Self::Backing(value)
    }
}

impl From<CapacityError> for RetainError {
    fn from(value: CapacityError) -> Self {
        Self::Capacity(value)
    }
}

#[derive(Debug)]
struct Entry {
    buffer: Option<Buffer>,
    capacity: u64,
    metadata_bytes: u64,
    base: usize,
    payer: AccountId,
    census: Arc<CensusRoot>,
    lease: Option<ReservationLease>,
}

impl Entry {
    fn new(
        backing: Backing,
        payer: AccountId,
        metadata_bytes: u64,
        census: Arc<CensusRoot>,
        lease: ReservationLease,
    ) -> Self {
        let base = backing.base();
        let capacity = backing.capacity();
        census.entries.fetch_add(1, Ordering::AcqRel);
        census.data_bytes.fetch_add(capacity, Ordering::AcqRel);
        census
            .metadata_bytes
            .fetch_add(metadata_bytes, Ordering::AcqRel);
        Self {
            buffer: Some(backing.buffer().clone()),
            capacity,
            metadata_bytes,
            base,
            payer,
            census,
            lease: Some(lease),
        }
    }

    fn take_settlement(&mut self) -> ReservationLease {
        drop(self.buffer.take());
        self.lease.take().expect("entry already settled")
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        // The allocation must be released before its capacity is returned.
        drop(self.buffer.take());
        drop(self.lease.take());
        self.census.entries.fetch_sub(1, Ordering::AcqRel);
        self.census
            .data_bytes
            .fetch_sub(self.capacity, Ordering::AcqRel);
        self.census
            .metadata_bytes
            .fetch_sub(self.metadata_bytes, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct EntryRef(Option<Arc<Entry>>);

impl EntryRef {
    fn new(entry: Entry) -> Self {
        Self(Some(Arc::new(entry)))
    }

    fn entry(&self) -> &Entry {
        self.0.as_ref().expect("entry reference already released")
    }

    fn key(&self) -> usize {
        Arc::as_ptr(self.0.as_ref().expect("entry reference already released")) as usize
    }

    fn take_last(mut self) -> Option<Entry> {
        Arc::into_inner(self.0.take().expect("entry reference already released"))
    }
}

impl Clone for EntryRef {
    fn clone(&self) -> Self {
        Self(Some(
            self.0
                .as_ref()
                .expect("entry reference already released")
                .clone(),
        ))
    }
}

impl Drop for EntryRef {
    fn drop(&mut self) {
        if let Some(entry) = self.0.take() {
            drop(Arc::into_inner(entry));
        }
    }
}

#[derive(Debug)]
struct LineageSet {
    entries: Vec<EntryRef>,
    metadata_lease: Option<ReservationLease>,
    metadata_bytes: u64,
    metadata_payer: AccountId,
    authority_key: usize,
    census: Arc<CensusRoot>,
}

impl LineageSet {
    fn new(
        entries: Vec<EntryRef>,
        metadata_lease: ReservationLease,
        metadata_bytes: u64,
        metadata_payer: AccountId,
        authority_key: usize,
        census: Arc<CensusRoot>,
    ) -> Self {
        census.sets.fetch_add(1, Ordering::AcqRel);
        census
            .metadata_bytes
            .fetch_add(metadata_bytes, Ordering::AcqRel);
        Self {
            entries,
            metadata_lease: Some(metadata_lease),
            metadata_bytes,
            metadata_payer,
            authority_key,
            census,
        }
    }
}

#[derive(Default)]
struct SettlementGuard {
    by_leaf: HashMap<usize, ReservationLease>,
}

impl SettlementGuard {
    fn add(&mut self, lease: ReservationLease) {
        let key = lease.leaf_key();
        if let Some(existing) = self.by_leaf.get_mut(&key) {
            existing
                .merge(lease)
                .unwrap_or_else(|_| panic!("settlement debit overflow or leaf mismatch"));
        } else {
            self.by_leaf.insert(key, lease);
        }
    }
}

impl Drop for LineageSet {
    fn drop(&mut self) {
        // into_inner selects exactly one final owner even when last exits race.
        // Entry's Drop is the fallback if a temporary reference survives this
        // set; it always drops the backing before the debit.
        let mut settlements = SettlementGuard::default();
        for entry in self.entries.drain(..) {
            if let Some(mut entry) = entry.take_last() {
                settlements.add(entry.take_settlement());
                drop(entry);
            }
        }
        if let Some(lease) = self.metadata_lease.take() {
            settlements.add(lease);
        }
        drop(settlements);
        self.census.sets.fetch_sub(1, Ordering::AcqRel);
        self.census
            .metadata_bytes
            .fetch_sub(self.metadata_bytes, Ordering::AcqRel);
    }
}

/// An Arrow value and the private immutable lineage that pays for its backing.
#[derive(Debug)]
pub struct Retained<T> {
    payload: Option<T>,
    lineage: Option<Arc<LineageSet>>,
}

impl<T> Retained<T> {
    pub fn payload(&self) -> &T {
        self.payload
            .as_ref()
            .expect("retained payload already dropped")
    }

    /// A fork shares the exact lineage object and does no capacity admission.
    pub fn fork(&self) -> Self
    where
        T: Clone,
    {
        Self {
            payload: Some(self.payload().clone()),
            lineage: self.lineage.clone(),
        }
    }

    pub fn data_bytes(&self) -> u64 {
        self.lineage
            .as_ref()
            .expect("retained lineage already dropped")
            .entries
            .iter()
            .map(|entry| entry.entry().capacity)
            .sum()
    }

    /// Holder exposure of entry and set metadata. Shared entries appear in
    /// each holder's exposure, so callers must not sum this across forks.
    pub fn metadata_bytes(&self) -> u64 {
        let lineage = self.lineage.as_ref().expect("retained lineage missing");
        lineage.metadata_bytes
            + lineage
                .entries
                .iter()
                .map(|entry| entry.entry().metadata_bytes)
                .sum::<u64>()
    }

    pub(crate) fn authority_key(&self) -> usize {
        self.lineage
            .as_ref()
            .expect("retained lineage missing")
            .authority_key
    }

    pub(crate) fn paid_entirely_by(&self, account: AccountId) -> bool {
        let lineage = self.lineage.as_ref().expect("retained lineage missing");
        lineage.metadata_payer == account
            && lineage
                .entries
                .iter()
                .all(|entry| entry.entry().payer == account)
    }
}

impl<T> Drop for Retained<T> {
    fn drop(&mut self) {
        // The payload can own the last Arrow alias. Keep lineage alive until
        // after payload destruction even if another set exits concurrently.
        drop(self.payload.take());
        drop(self.lineage.take());
    }
}

impl RetentionDomain {
    pub fn retain_many(
        &self,
        outputs: Vec<RecordBatch>,
        provenance: &BackingProvenance,
    ) -> Result<Vec<Retained<RecordBatch>>, RetainError> {
        self.publish(&[], outputs, provenance)
    }

    pub fn retain(
        &self,
        output: RecordBatch,
        provenance: &BackingProvenance,
    ) -> Result<Retained<RecordBatch>, RetainError> {
        Ok(self.retain_many(vec![output], provenance)?.remove(0))
    }

    pub fn derive_many(
        &self,
        sources: &[&Retained<RecordBatch>],
        outputs: Vec<RecordBatch>,
        provenance: &BackingProvenance,
    ) -> Result<Vec<Retained<RecordBatch>>, RetainError> {
        self.publish(sources, outputs, provenance)
    }

    pub fn derive(
        &self,
        sources: &[&Retained<RecordBatch>],
        output: RecordBatch,
        provenance: &BackingProvenance,
    ) -> Result<Retained<RecordBatch>, RetainError> {
        Ok(self
            .derive_many(sources, vec![output], provenance)?
            .remove(0))
    }

    fn publish(
        &self,
        sources: &[&Retained<RecordBatch>],
        outputs: Vec<RecordBatch>,
        provenance: &BackingProvenance,
    ) -> Result<Vec<Retained<RecordBatch>>, RetainError> {
        let authority_key = self.authority_key();
        let mut source_by_base: HashMap<usize, Vec<EntryRef>> = HashMap::new();
        for source in sources {
            let lineage = source.lineage.as_ref().expect("retained lineage missing");
            if lineage.authority_key != authority_key {
                return Err(RetainError::DifferentAuthority);
            }
            for entry in &lineage.entries {
                let candidates = source_by_base.entry(entry.entry().base).or_default();
                if !candidates.iter().any(|seen| seen.key() == entry.key()) {
                    candidates.push(entry.clone());
                }
            }
        }

        let mut collector = BackingCollector::new();
        let output_indices: Vec<_> = outputs
            .iter()
            .map(|output| collector.collect_batch(output, provenance))
            .collect::<Result<_, _>>()?;
        let (backings, _) = collector.finish().into_parts();
        let mut source_matches = Vec::with_capacity(backings.len());
        let mut new_data_bytes = 0u64;
        let mut new_count = 0usize;
        for backing in &backings {
            let matches = source_by_base
                .get(&backing.base())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            if let Some(mismatch) = matches
                .iter()
                .find(|entry| entry.entry().capacity != backing.capacity())
            {
                return Err(RetainError::Backing(BackingError::ConflictingCapacity {
                    base: backing.base(),
                    first: mismatch.entry().capacity,
                    second: backing.capacity(),
                }));
            }
            if matches.is_empty() {
                new_data_bytes = new_data_bytes
                    .checked_add(backing.capacity())
                    .ok_or(RetainError::MetadataOverflow)?;
                new_count += 1;
            }
            source_matches.push(matches.to_vec());
        }

        let entry_metadata = metadata_bytes::<Entry>(new_count)?;
        let entry_unit = metadata_bytes::<Entry>(1)?;
        let mut set_metadata = Vec::with_capacity(outputs.len());
        for indices in &output_indices {
            let members = indices
                .iter()
                .try_fold(0usize, |total, index| {
                    total.checked_add(source_matches[*index].len().max(1))
                })
                .ok_or(RetainError::MetadataOverflow)?;
            set_metadata.push(
                metadata_bytes::<LineageSet>(1)?
                    .checked_add(array_bytes::<EntryRef>(members)?)
                    .ok_or(RetainError::MetadataOverflow)?,
            );
        }
        let metadata_total = set_metadata.iter().try_fold(entry_metadata, |sum, bytes| {
            sum.checked_add(*bytes).ok_or(RetainError::MetadataOverflow)
        })?;
        let debit = new_data_bytes
            .checked_add(metadata_total)
            .ok_or(RetainError::MetadataOverflow)?;
        let mut lease = self.leaf().try_grow(debit)?;

        let mut entries = Vec::with_capacity(backings.len());
        for (backing, matches) in backings.into_iter().zip(source_matches) {
            if matches.is_empty() {
                let bytes = backing.capacity() + entry_unit;
                let portion = lease.split_off(bytes);
                entries.push(vec![EntryRef::new(Entry::new(
                    backing,
                    self.leaf_id(),
                    entry_unit,
                    self.census_root(),
                    portion,
                ))]);
            } else {
                entries.push(matches);
            }
        }

        let mut retained = Vec::with_capacity(outputs.len());
        for ((payload, indices), set_bytes) in
            outputs.into_iter().zip(output_indices).zip(set_metadata)
        {
            let mut seen = HashSet::new();
            let member_capacity = indices.iter().map(|index| entries[*index].len()).sum();
            let mut members = Vec::with_capacity(member_capacity);
            for index in indices {
                for entry in &entries[index] {
                    let key = entry.key();
                    if seen.insert(key) {
                        members.push(entry.clone());
                    }
                }
            }
            retained.push(Retained {
                payload: Some(payload),
                lineage: Some(Arc::new(LineageSet::new(
                    members,
                    lease.split_off(set_bytes),
                    set_bytes,
                    self.leaf_id(),
                    authority_key,
                    self.census_root(),
                ))),
            });
        }
        debug_assert_eq!(lease.bytes(), 0);
        Ok(retained)
    }
}

fn array_bytes<T>(count: usize) -> Result<u64, RetainError> {
    count
        .checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(RetainError::MetadataOverflow)
}

fn metadata_bytes<T>(count: usize) -> Result<u64, RetainError> {
    let per_object = size_of::<T>()
        .checked_add(ARC_HEADER_BYTES)
        .ok_or(RetainError::MetadataOverflow)?;
    array_bytes::<u8>(
        per_object
            .checked_mul(count)
            .ok_or(RetainError::MetadataOverflow)?,
    )
}
