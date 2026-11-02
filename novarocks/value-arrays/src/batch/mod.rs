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

//! Prepared byte batches with caller-owned results and synchronous control.
//! Preparing binds storage once; kernels preserve logical coordinates and NULL
//! policy while the decoded owner keeps the actual Arrow sources alive.
//! New kernels write selection-ordinal results into caller storage. A running
//! hash state is an algorithm-specific object, distinct from its final hash.
//!
//! ```
//! use std::{convert::Infallible, sync::Arc};
//! use arrow_array::{ArrayRef, StringArray};
//! use novarocks_value_arrays::{ByteLayout, access::ByteSource,
//!     decode::ValueConversionContext, reachability::ReachableRows,
//!     batch::{PreparedBytes, hash::{hash_into, Fnv1a64, HashOptions, HashSeed, HashScratch}},
//!     hash_eq::NullHash};
//! let array: ArrayRef = Arc::new(StringArray::from(vec![Some("hello"), None]));
//! let source = ByteSource::parse(&array, ByteLayout::Utf8, None).unwrap();
//! let rows = ReachableRows::all(2).materialize(&mut |_| Ok::<_, Infallible>(())).unwrap();
//! let decoded = ValueConversionContext::default().decode(&source, &ReachableRows::all(2)).unwrap();
//! let input = PreparedBytes::new(&decoded, &rows).unwrap();
//! let mut output = [0; 2];
//! hash_into(&input, &Fnv1a64,
//!     HashOptions { seeds: HashSeed::Uniform(14695981039346656037), nulls: NullHash::Value(99) },
//!     &mut output, &mut HashScratch::new(),
//!     &mut |_| Ok::<_, Infallible>(()), &mut |_| Ok::<_, Infallible>(())).unwrap();
//! assert_eq!(output, [0xa430d84680aabd0b, 99]);
//! ```
pub mod compare;
pub mod hash;
mod prepared;
pub mod selected;
pub use prepared::{OwnedBytesPage, OwnedPreparedBytes, PreparedBytes};

pub(crate) const ROW_BLOCK: usize = 1024;
pub(crate) const BYTE_BLOCK: usize = 64 * 1024;

/// Control at bounded row blocks, including all-NULL and empty-value batches.
pub(crate) struct RowWork<'a, W: ?Sized> {
    work: &'a mut W,
    pending: usize,
    pending_bytes: usize,
}
impl<'a, W: ?Sized> RowWork<'a, W> {
    pub(crate) fn new(work: &'a mut W) -> Self {
        Self {
            work,
            pending: 0,
            pending_bytes: 0,
        }
    }
    pub(crate) fn row<E>(&mut self) -> Result<(), crate::ValueFailure<E>>
    where
        W: FnMut(usize) -> Result<(), E>,
    {
        self.pending += 1;
        if self.pending == ROW_BLOCK {
            self.flush()?;
        }
        Ok(())
    }
    pub(crate) fn observe_metadata<E>(&mut self, count: usize) -> Result<(), E>
    where
        W: FnMut(usize) -> Result<(), E>,
    {
        // Metadata blocks are already bounded by the selection owner. Keep
        // their observations distinct from per-value byte accumulation.
        (self.work)(count)
    }
    pub(crate) fn bytes<E>(&mut self, count: usize) -> Result<(), crate::ValueFailure<E>>
    where
        W: FnMut(usize) -> Result<(), E>,
    {
        let mut remaining = count;
        while remaining != 0 {
            let step = remaining.min(BYTE_BLOCK - self.pending_bytes);
            self.pending_bytes += step;
            remaining -= step;
            if self.pending_bytes == BYTE_BLOCK {
                let bytes = std::mem::take(&mut self.pending_bytes);
                (self.work)(bytes).map_err(crate::ValueFailure::Caller)?;
            }
        }
        Ok(())
    }
    pub(crate) fn flush<E>(&mut self) -> Result<(), crate::ValueFailure<E>>
    where
        W: FnMut(usize) -> Result<(), E>,
    {
        if self.pending_bytes != 0 {
            let count = std::mem::take(&mut self.pending_bytes);
            (self.work)(count).map_err(crate::ValueFailure::Caller)?;
        }
        if self.pending != 0 {
            let count = std::mem::take(&mut self.pending);
            (self.work)(count).map_err(crate::ValueFailure::Caller)?;
        }
        Ok(())
    }
}

// Each arm instantiates the complete loop for one concrete mapping; no row
// performs an enum or dynamic-dispatch mapping selection.
macro_rules! with_mapping {
    ($input:expr, |$mapping:ident| $body:block) => {{
        match $input.mapping() {
            crate::decode::RowMapping::Identity { .. } => {
                let $mapping = $crate::batch::prepared::Identity;
                $body
            }
            crate::decode::RowMapping::Constant { index, .. } => {
                let $mapping = $crate::batch::prepared::Constant(*index);
                $body
            }
            crate::decode::RowMapping::Dictionary(keys) => {
                let $mapping = $crate::batch::prepared::Dictionary(keys);
                $body
            }
        }
    }};
}
pub(crate) use with_mapping;
