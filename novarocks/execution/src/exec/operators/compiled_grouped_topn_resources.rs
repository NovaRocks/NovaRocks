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

//! Public Arrow work budgets and custody of newly restored grouped key arrays.
//!
//! The invoice is a policy estimate for a public library call, not a proof of
//! Arrow's private capacities or a physical peak. Candidate/state containers
//! use their original exact Layout allocator separately. Returned key stock,
//! typed source holders and GLOBAL wrapping metadata have separate charges.

use std::sync::Arc;

use arrow::array::ArrayRef;
use novarocks_functions::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use novarocks_functions::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, PreparedSourceCustody, ScalarResourceError, SourceBackingOwner,
    source_custody_prefix_facts,
};

/// Declared public work, never inferred private allocation geometry.
#[derive(Clone, Copy)]
pub(super) struct ArrowWorkShape {
    pub(super) rows: usize,
    pub(super) cells: usize,
    pub(super) payload_bytes: usize,
    pub(super) fields: usize,
    pub(super) buffers: usize,
}

/// GroupedTopNArrowBudgetV1 from the accepted plan. Overflow is a fatal
/// resource refusal; it cannot turn into a zero budget or a row data error.
fn invoice(shape: ArrowWorkShape) -> Result<usize, KernelFailure> {
    let checked = || {
        16_384usize
            .checked_add(64usize.checked_mul(shape.rows.checked_add(shape.cells)?)?)?
            .checked_add(2usize.checked_mul(shape.payload_bytes)?)?
            .checked_add(2_048usize.checked_mul(shape.fields)?)?
            .checked_add(1_024usize.checked_mul(shape.buffers)?)
    };
    checked().ok_or(KernelFailure::ResourceExhausted)
}

/// Kept alive until the actual public operation and its temporary results
/// have returned/dropped. This same host also adopts its new returned stock.
pub(super) struct ArrowWorkBudget {
    host: Arc<dyn AggregateStateAllocator>,
    _reservation: OpaqueReservation,
}
impl ArrowWorkBudget {
    pub(super) fn admit(
        host: Arc<dyn AggregateStateAllocator>,
        shape: ArrowWorkShape,
    ) -> Result<Self, KernelFailure> {
        let authority = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let reservation = authority.reserve_operation(invoice(shape)?)?;
        Ok(Self {
            host,
            _reservation: reservation,
        })
    }
}

/// The actual array is destroyed before its stock charge. SourceBackingOwner
/// and the original Arrow custody author retain this owner through the last
/// Array/Buffer alias, including aliases transferred to another thread.
struct KeyArrayBacking {
    array: ArrayRef,
    _stock: OpaqueRetainedCharge,
}

fn source_model_failure(error: ScalarResourceError) -> KernelFailure {
    match error {
        ScalarResourceError::Arithmetic => KernelFailure::ResourceExhausted,
        ScalarResourceError::SourceModel(detail) | ScalarResourceError::Invariant(detail) => {
            KernelFailure::InvalidProgram(KernelDiagnostic::new(detail))
        }
    }
}

/// Move a NEW array from the audited public Arrow restoration/normalization
/// into the existing source custody author. Callers must keep `work_budget`
/// alive over creation and this adoption, and must not leave raw external
/// aliases to the unwrapped result. This function owns that raw result.
///
/// Observed returned stock is admitted after the library call. It is not an
/// assertion of exact admission before Arrow's internal creation. The source
/// prefix is paid before callbacks/flushes; typed holder Layouts are then
/// charged by the same host.allocate, never duplicated as opaque bytes.
pub(super) fn retain_new_key_array(
    array: ArrayRef,
    work_budget: &ArrowWorkBudget,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    let host = &work_budget.host;
    let authority = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    let _prefix = authority.reserve_operation(source_custody_prefix_facts().requests().bytes())?;
    let mut work = EvaluationCheckpoints::new(control);
    work.flush()?;
    // Pure request facts qualify the original direct holder's model. The
    // actual two typed Layout requests are paid inside its original author.
    let _owned = SourceBackingOwner::<KeyArrayBacking>::owned_request_facts()
        .map_err(source_model_failure)?;
    let mut stock = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    let bytes = array.get_array_memory_size();
    work.flush()?;
    let mut stock_reservation = stock.reserve_operation(bytes)?;
    stock.reconcile_under_reservation(bytes, &mut stock_reservation)?;
    drop(stock_reservation);
    let source = SourceBackingOwner::try_from_owned_direct(
        KeyArrayBacking {
            array,
            _stock: stock,
        },
        Arc::clone(host),
        &mut work,
    )?;
    let prepared = PreparedSourceCustody::prepare(&source.borrowed().array, &mut work)?;
    // GLOBAL metadata differs from typed backing; retain transfers only its
    // own retained portion, and the reservation returns actual scratch.
    let mut metadata = authority.reserve_operation(prepared.request_facts().global_peak_upper())?;
    let output = prepared.retain(&source, &mut metadata, &mut work)?;
    work.finish()?;
    Ok(output)
}

#[cfg(test)]
#[path = "compiled_grouped_topn_resources_tests.rs"]
mod tests;
