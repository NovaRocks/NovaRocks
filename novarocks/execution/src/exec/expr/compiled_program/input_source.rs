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

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::{Chunk, ChunkSourceAuthorityRef};

/// One synchronous input. The Chunk variant structurally pairs its own batch
/// and attached owners, without cloning either or accepting another batch.
#[derive(Clone, Copy)]
pub(crate) enum CompiledInputLoan<'a> {
    RecordBatch(&'a RecordBatch),
    Chunk(&'a Chunk),
    #[cfg(test)]
    Observed {
        input: &'a CompiledInputLoan<'a>,
        observer: &'a SlotSourceObserver,
    },
}

impl<'a> CompiledInputLoan<'a> {
    pub(crate) fn from_chunk(chunk: &'a Chunk) -> Self {
        Self::Chunk(chunk)
    }

    #[cfg(test)]
    pub(crate) fn observed<'b>(
        &'b self,
        observer: &'b SlotSourceObserver,
    ) -> CompiledInputLoan<'b> {
        CompiledInputLoan::Observed {
            input: self,
            observer,
        }
    }

    pub(crate) fn batch(self) -> &'a RecordBatch {
        match self {
            Self::RecordBatch(batch) => batch,
            Self::Chunk(chunk) => &chunk.batch,
            #[cfg(test)]
            Self::Observed { input, .. } => input.batch(),
        }
    }

    /// Called only after the original Frame has checked the lexical input
    /// node/role. An absent ordinal remains the original Slot error.
    pub(crate) fn checked_column(self, ordinal: usize) -> Option<BorrowedInputColumn<'a>> {
        #[cfg(test)]
        if let Self::Observed { input, observer } = self {
            let column = input.checked_column(ordinal)?;
            observer(column);
            return Some(column);
        }
        let array = self.batch().columns().get(ordinal)?;
        let source = match self {
            Self::RecordBatch(_) => BorrowedInputSource::NoChunkProvided,
            #[cfg(test)]
            Self::Observed { .. } => {
                unreachable!("observed input returned its original checked column")
            }
            Self::Chunk(chunk) => {
                let authority = chunk.source_authority();
                if authority.is_attached() {
                    BorrowedInputSource::Attached(authority)
                } else {
                    BorrowedInputSource::NoAttachedAuthority
                }
            }
        };
        Some(BorrowedInputColumn { array, source })
    }
}

impl std::ops::Deref for CompiledInputLoan<'_> {
    type Target = RecordBatch;
    fn deref(&self) -> &Self::Target {
        self.batch()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum BorrowedInputSource<'a> {
    NoChunkProvided,
    NoAttachedAuthority,
    #[allow(
        dead_code,
        reason = "Prepared Slot source view; leaf source consumption is a later slice"
    )]
    Attached(ChunkSourceAuthorityRef<'a>),
}

/// A view of the original checked column and its factual source. It never
/// becomes an owned Frame result or changes the current leaf argument ABI.
#[derive(Clone, Copy)]
pub(crate) struct BorrowedInputColumn<'a> {
    array: &'a ArrayRef,
    source: BorrowedInputSource<'a>,
}

impl<'a> BorrowedInputColumn<'a> {
    pub(crate) fn into_parts(self) -> (&'a ArrayRef, BorrowedInputSource<'a>) {
        (self.array, self.source)
    }
}

#[cfg(test)]
pub(crate) type SlotSourceObserver = dyn for<'a> Fn(BorrowedInputColumn<'a>) + Send + Sync;
