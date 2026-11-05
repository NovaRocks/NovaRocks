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

//! The single-reader compiled ordered Scan composition. The original
//! conjunction, row evaluator, mask creator and source lifecycle remain owners.

use crate::exec::arrow_work::{ArrowWorkBudget, ArrowWorkShape};
use crate::exec::chunk::Chunk;
use crate::exec::expr::compiled_program::input_source::CompiledInputLoan;
use crate::exec::operators::compiled_expression::{
    CompiledFilterBody, CompiledFilterProcessorFactory, RuntimeKernelControl,
};
use crate::exec::operators::runtime_filter::{
    CompiledOrderedScanEvaluator, CompiledOrderedScanFactory,
};
use crate::exec::pipeline::operator::InputConstructionFactory;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::{OperatorProfiles, ProfileUnit};
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::ExecutionResult;
use crate::runtime::task::io::TaskEventSink;
use crate::runtime_filter::OrderedTopNAllocationHost;
use arrow::array::{Array, BooleanArray};
use novarocks_functions::KernelFailure;
use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
use std::sync::Arc;

#[derive(Clone)]
pub(in crate::exec::operators::scan) struct CompiledOrderedOutputFactory {
    pub ordered: CompiledOrderedScanFactory,
    pub residual: Option<CompiledFilterProcessorFactory>,
    pub host: OrderedTopNAllocationHost,
    pub control: RuntimeKernelControl,
}
impl CompiledOrderedOutputFactory {
    pub(super) fn for_driver(&self) -> CompiledOrderedOutputEvaluator {
        CompiledOrderedOutputEvaluator {
            ordered: self.ordered.for_driver(),
            residual: self.residual.as_ref().map(|factory| factory.create_body()),
            host: self.host.clone(),
            control: self.control.clone(),
        }
    }
}
pub(in crate::exec::operators::scan) struct CompiledOrderedOutputEvaluator {
    ordered: CompiledOrderedScanEvaluator,
    residual: Option<CompiledFilterBody>,
    host: OrderedTopNAllocationHost,
    control: RuntimeKernelControl,
}
impl CompiledOrderedOutputEvaluator {
    pub(super) fn bind_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        self.ordered.bind_mem_tracker(tracker);
    }
    pub(super) fn bind_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(Arc::clone(&factory));
        self.ordered.bind_construction_factory(factory);
    }
    pub(super) fn bind(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        self.ordered.bind(state)
    }
    pub(super) fn apply(
        &mut self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
        sink: &Arc<dyn TaskEventSink>,
    ) -> ExecutionResult<Option<Chunk>> {
        let chunk = if let Some(residual) = &mut self.residual {
            let rows = chunk.len();
            // This public operation budget encloses the original conjunction
            // and the synchronous Boolean result's stock adoption. It does not
            // certify Arrow's private allocation peak.
            let _work = ArrowWorkBudget::admit(
                self.host.scalar_allocator(),
                ArrowWorkShape {
                    rows,
                    cells: rows,
                    payload_bytes: 0,
                    fields: 1,
                    buffers: 2,
                },
            )?;
            struct ResidualMask {
                truth: BooleanArray,
                _stock: Option<OpaqueRetainedCharge>,
            }
            let mut mask = ResidualMask {
                truth: residual.evaluate(CompiledInputLoan::from_chunk(&chunk), &self.control)?,
                _stock: None,
            };
            let mut stock = OpaqueRetainedCharge::try_new(self.host.scalar_allocator())?;
            let bytes = mask
                .truth
                .values()
                .inner()
                .capacity()
                .checked_add(
                    mask.truth
                        .nulls()
                        .map_or(0, |nulls| nulls.buffer().capacity()),
                )
                .ok_or(KernelFailure::ResourceExhausted)?;
            let mut reservation = stock.reserve_operation(bytes)?;
            stock.reconcile_under_reservation(bytes, &mut reservation)?;
            drop(reservation);
            mask._stock = Some(stock);
            let owner = self.control.construction_owner(chunk.chunk_schema())?;
            let result = chunk.filter_ordered_scan_controlled(
                &mask.truth,
                self.host.scalar_allocator(),
                &self.control,
                owner.as_ref(),
            )?;
            if let Some(profiles) = profiles {
                profiles.common.counter_add(
                    super::SCAN_CONJUNCT_INPUT_ROWS,
                    ProfileUnit::Unit,
                    i64::try_from(rows).unwrap_or(i64::MAX),
                );
                profiles.common.counter_add(
                    super::SCAN_CONJUNCT_OUTPUT_ROWS,
                    ProfileUnit::Unit,
                    i64::try_from(result.as_ref().map_or(0, Chunk::len)).unwrap_or(i64::MAX),
                );
            }
            let Some(chunk) = result else {
                return Ok(None);
            };
            chunk
        } else {
            chunk
        };
        // Polling happens inside apply, only after the complete residual has
        // published any required-row error. Empty survivors never imply EOS.
        self.ordered.apply(chunk, sink)
    }
    pub(super) fn restrict_limit(
        &mut self,
        chunk: Chunk,
        limit: Option<usize>,
        rows_before: usize,
    ) -> ExecutionResult<Option<Chunk>> {
        let Some(limit) = limit else {
            return Ok(Some(chunk));
        };
        let remaining = limit.saturating_sub(rows_before);
        if remaining == 0 {
            return Ok(None);
        }
        if chunk.len() <= remaining {
            return Ok(Some(chunk));
        }
        let owner = self.control.construction_owner(chunk.chunk_schema())?;
        chunk
            .slice_ordered_scan_controlled(
                0,
                remaining,
                self.host.scalar_allocator(),
                &self.control,
                owner.as_ref(),
            )
            .map(Some)
    }
}
