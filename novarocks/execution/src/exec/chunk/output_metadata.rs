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

//! Retain an admitted original output column container through its real free.
//! This is metadata, not Arrow payload/source authority. A later clone, slice
//! or concatenation creates its own container request; sharing this keeper
//! only conservatively extends the original paid container's lifetime.

use std::alloc::Layout;
use std::fmt;
use std::sync::Arc;

use arrow::array::ArrayRef;
use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
use novarocks_functions::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, ScalarResourceError, SourceBackingOwner, source_custody_prefix_facts,
};

struct ColumnMetadataCharge {
    columns: usize,
    _charge: OpaqueRetainedCharge,
}

/// Clone shares the existing typed cold owner without allocating. A retaining
/// container must destroy its RecordBatch before destroying this keeper.
#[derive(Clone)]
pub(crate) struct OutputColumnMetadata {
    owner: SourceBackingOwner<ColumnMetadataCharge>,
}

fn model_failure(error: ScalarResourceError) -> KernelFailure {
    match error {
        ScalarResourceError::Arithmetic => KernelFailure::ResourceExhausted,
        ScalarResourceError::SourceModel(detail) | ScalarResourceError::Invariant(detail) => {
            KernelFailure::InvalidProgram(KernelDiagnostic::new(detail))
        }
    }
}

impl OutputColumnMetadata {
    /// Admit the exact request BEFORE the caller allocates its std column Vec.
    /// The caller uses try_reserve_exact, moves that Vec into the original
    /// batch, and keeps this value declared before unpublished data.
    pub(crate) fn try_new(
        columns: usize,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        let layout =
            Layout::array::<ArrayRef>(columns).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let _prefix = charge.reserve_operation(source_custody_prefix_facts().requests().bytes())?;
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let _facts = SourceBackingOwner::<ColumnMetadataCharge>::owned_request_facts()
            .map_err(model_failure)?;
        let mut reservation = charge.reserve_operation(layout.size())?;
        charge.reconcile_under_reservation(layout.size(), &mut reservation)?;
        drop(reservation);
        let owner = SourceBackingOwner::try_from_owned_direct(
            ColumnMetadataCharge {
                columns,
                _charge: charge,
            },
            host,
            &mut work,
        )?;
        work.finish()?;
        Ok(Self { owner })
    }

    pub(crate) fn columns(&self) -> usize {
        self.owner.borrowed().columns
    }

    /// Known typed holder requests, not an ordinary std Arc layout or an
    /// inspection of the shared host/tracker governance ancestry.
    pub(crate) fn holder_bytes(&self) -> Result<usize, ScalarResourceError> {
        Ok(
            SourceBackingOwner::<ColumnMetadataCharge>::owned_request_facts()?
                .typed()
                .bytes(),
        )
    }
}

impl fmt::Debug for OutputColumnMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutputColumnMetadata")
            .field("columns", &self.columns())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "output_metadata_tests.rs"]
mod tests;
