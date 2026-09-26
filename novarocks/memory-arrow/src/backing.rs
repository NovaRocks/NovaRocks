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

//! Local collection of immutable Arrow allocations before retention admission.
//!
//! Identity is the allocation base only while the collected `Buffer` references
//! are alive. This module does not establish ownership across separate calls or
//! supply a process-wide paid-backing index. Arrow 58.2 reports the length
//! supplied to `Buffer::from_custom_allocation` as its `capacity`, even though
//! the owner may retain a larger allocation. Its deallocation kind is private.
//! Consequently only a caller that can prove standard Arrow provenance may
//! authorize capacity collection. Unverified input is rejected, including
//! empty custom buffers whose owners cannot be inspected.

use std::collections::{HashMap, HashSet};
use std::fmt;

use arrow::array::{ArrayData, RecordBatch};
use arrow_buffer::Buffer;

const INLINE_LIMIT: usize = 16;

/// Evidence supplied by a boundary that knows how every input buffer was made.
/// A bare Arrow `Buffer` or `RecordBatch` does not supply this evidence.
#[derive(Clone, Copy, Debug)]
pub struct BackingProvenance {
    standard_arrow: bool,
}

impl BackingProvenance {
    /// A source with unknown allocation owners; collecting from it fails.
    pub const fn unknown() -> Self {
        Self {
            standard_arrow: false,
        }
    }

    /// Attests that every buffer passed with this evidence has standard Arrow
    /// allocation provenance, with `Buffer::capacity` equal to its full stable
    /// allocation size. The caller must establish this from the construction or
    /// decoding boundary, not infer it from the buffer's visible length or
    /// reported capacity.
    ///
    /// # Safety
    ///
    /// Passing a custom owner under this attestation can undercharge retained
    /// memory. It violates the memory authority's capacity invariant.
    pub unsafe fn trusted_standard_arrow() -> Self {
        Self {
            standard_arrow: true,
        }
    }
}

/// One strongly held allocation and its full capacity, not its visible length.
#[derive(Debug)]
pub struct Backing {
    buffer: Buffer,
    capacity: u64,
}

impl Backing {
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    pub fn base(&self) -> usize {
        self.buffer.data_ptr().as_ptr() as usize
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BackingError {
    /// Arrow hides the full allocation size of an externally owned buffer.
    UnknownProvenance {
        base: usize,
        visible_len: usize,
    },
    /// Two views of one live base disagreed about its full capacity.
    ConflictingCapacity {
        base: usize,
        first: u64,
        second: u64,
    },
    CapacityOverflow,
}

impl fmt::Display for BackingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownProvenance { base, visible_len } => write!(
                f,
                "unknown Arrow backing provenance at {base:#x} with {visible_len} visible bytes"
            ),
            Self::ConflictingCapacity {
                base,
                first,
                second,
            } => write!(
                f,
                "Arrow backing at {base:#x} has conflicting capacities {first} and {second}"
            ),
            Self::CapacityOverflow => write!(f, "Arrow backing capacity sum overflowed"),
        }
    }
}

impl std::error::Error for BackingError {}

/// Distinct backings from one local import or derivation.
#[derive(Debug)]
pub struct BackingCollection {
    backings: Vec<Backing>,
    total_capacity: u64,
}

impl BackingCollection {
    pub fn backings(&self) -> &[Backing] {
        &self.backings
    }

    pub fn total_capacity(&self) -> u64 {
        self.total_capacity
    }

    pub fn into_parts(self) -> (Vec<Backing>, u64) {
        (self.backings, self.total_capacity)
    }
}

/// Collects all outputs of one import/derive together, preserving first-seen
/// traversal order. Each output receives indices into the finished collection.
#[derive(Default)]
pub struct BackingCollector {
    backings: Vec<Backing>,
    by_base: Option<HashMap<usize, usize>>,
    total_capacity: u64,
}

impl BackingCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the index of a nonempty backing, or `None` for an empty buffer.
    /// A failed call leaves the collector unsuitable for publication; discard it.
    pub fn collect_buffer(
        &mut self,
        buffer: &Buffer,
        provenance: &BackingProvenance,
    ) -> Result<Option<usize>, BackingError> {
        self.insert(buffer, provenance)
    }

    /// Returns unique backing indices used by this output, in traversal order.
    pub fn collect_batch(
        &mut self,
        batch: &RecordBatch,
        provenance: &BackingProvenance,
    ) -> Result<Vec<usize>, BackingError> {
        let mut indices = OutputIndices::default();
        for column in batch.columns() {
            self.collect_data_into(&column.to_data(), provenance, &mut indices)?;
        }
        Ok(indices.values)
    }

    pub fn collect_array_data(
        &mut self,
        data: &ArrayData,
        provenance: &BackingProvenance,
    ) -> Result<Vec<usize>, BackingError> {
        let mut indices = OutputIndices::default();
        self.collect_data_into(data, provenance, &mut indices)?;
        Ok(indices.values)
    }

    pub fn finish(self) -> BackingCollection {
        BackingCollection {
            backings: self.backings,
            total_capacity: self.total_capacity,
        }
    }

    fn collect_data_into(
        &mut self,
        data: &ArrayData,
        provenance: &BackingProvenance,
        indices: &mut OutputIndices,
    ) -> Result<(), BackingError> {
        for buffer in data.buffers() {
            if let Some(index) = self.insert(buffer, provenance)? {
                indices.push(index);
            }
        }
        if let Some(nulls) = data.nulls()
            && let Some(index) = self.insert(nulls.inner().inner(), provenance)?
        {
            indices.push(index);
        }
        for child in data.child_data() {
            self.collect_data_into(child, provenance, indices)?;
        }
        Ok(())
    }

    fn insert(
        &mut self,
        buffer: &Buffer,
        provenance: &BackingProvenance,
    ) -> Result<Option<usize>, BackingError> {
        let capacity =
            u64::try_from(buffer.capacity()).map_err(|_| BackingError::CapacityOverflow)?;
        let base = buffer.data_ptr().as_ptr() as usize;
        if !provenance.standard_arrow {
            return Err(BackingError::UnknownProvenance {
                base,
                visible_len: buffer.len(),
            });
        }
        if capacity == 0 {
            return if buffer.is_empty() {
                Ok(None)
            } else {
                Err(BackingError::UnknownProvenance {
                    base,
                    visible_len: buffer.len(),
                })
            };
        }

        let prior = self
            .by_base
            .as_ref()
            .and_then(|map| map.get(&base).copied())
            .or_else(|| {
                (self.by_base.is_none()).then(|| {
                    self.backings
                        .iter()
                        .position(|backing| backing.base() == base)
                })?
            });
        if let Some(index) = prior {
            let first = self.backings[index].capacity;
            if first != capacity {
                return Err(BackingError::ConflictingCapacity {
                    base,
                    first,
                    second: capacity,
                });
            }
            return Ok(Some(index));
        }

        let next_total = checked_add(self.total_capacity, capacity)?;
        let index = self.backings.len();
        self.backings.push(Backing {
            buffer: buffer.clone(),
            capacity,
        });
        self.total_capacity = next_total;
        if self.by_base.is_none() && self.backings.len() > INLINE_LIMIT {
            self.by_base = Some(
                self.backings
                    .iter()
                    .enumerate()
                    .map(|(index, backing)| (backing.base(), index))
                    .collect(),
            );
        } else if let Some(map) = &mut self.by_base {
            map.insert(base, index);
        }
        Ok(Some(index))
    }
}

fn checked_add(left: u64, right: u64) -> Result<u64, BackingError> {
    left.checked_add(right)
        .ok_or(BackingError::CapacityOverflow)
}

#[derive(Default)]
struct OutputIndices {
    values: Vec<usize>,
    seen: Option<HashSet<usize>>,
}

impl OutputIndices {
    fn push(&mut self, index: usize) {
        if let Some(seen) = &mut self.seen {
            if seen.insert(index) {
                self.values.push(index);
            }
        } else if !self.values.contains(&index) {
            self.values.push(index);
            if self.values.len() > INLINE_LIMIT {
                self.seen = Some(self.values.iter().copied().collect());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_sum_rejects_overflow() {
        assert_eq!(
            checked_add(u64::MAX, 1),
            Err(BackingError::CapacityOverflow)
        );
    }

    #[test]
    fn conflicting_capacity_is_rejected_before_publication() {
        let buffer = Buffer::from_vec(vec![1u8; 8]);
        let mut collector = BackingCollector::new();
        let provenance = unsafe { BackingProvenance::trusted_standard_arrow() };
        collector.collect_buffer(&buffer, &provenance).unwrap();
        collector.backings[0].capacity += 1;
        assert!(matches!(
            collector.collect_buffer(&buffer, &provenance),
            Err(BackingError::ConflictingCapacity { .. })
        ));
    }
}
