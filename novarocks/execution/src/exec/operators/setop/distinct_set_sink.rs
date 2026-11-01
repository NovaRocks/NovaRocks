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

//! Generic sink side for DISTINCT set-operation stage ingestion.
//!
//! Responsibilities:
//! - Feeds stage-specific rows into shared distinct set-operation membership tracking.
//! - Signals stage completion for downstream result materialization.
//!
//! Key exported interfaces:
//! - Types: `DistinctSetSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::operators::compiled_expression::RuntimeKernelControl;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, OperatorLifecycle,
};
use crate::runtime::fragment::ExecutionResult;

use std::marker::PhantomData;
use std::sync::Arc;

use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::{PreparedInput, RawInput},
    shape::{SinkKernel, SinkStep, Transform},
    step::{AdmissionNext, NextStep, WaitFact, WaitReason},
    step_context::StepContext,
};
use crate::runtime::mem_tracker::MemTracker;

use super::distinct_set_shared::{DistinctSetSemantics, DistinctSetSharedState};

pub struct DistinctSetSinkFactory<S: DistinctSetSemantics> {
    name: String,
    stage: usize,
    state: DistinctSetSharedState<S>,
}

impl<S: DistinctSetSemantics> OperatorFactory for DistinctSetSinkFactory<S> {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(DistinctSetSinkOperator::<S> {
            name: self.name.clone(),
            stage: self.stage,
            state: self.state.clone(),
            mem_tracker: None,
            construction: None,
            control: None,
            _semantics: PhantomData,
        }))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct DistinctSetSinkOperator<S: DistinctSetSemantics> {
    name: String,
    stage: usize,
    state: DistinctSetSharedState<S>,
    mem_tracker: Option<Arc<MemTracker>>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    control: Option<RuntimeKernelControl>,
    _semantics: PhantomData<S>,
}

impl<S: DistinctSetSemantics> OperatorLifecycle for DistinctSetSinkOperator<S> {
    fn name(&self) -> &str {
        &self.name
    }

    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        if let Some(control) = self.control.as_mut() {
            control.bind_construction_factory(Arc::clone(&factory));
        }
        self.construction = Some(factory);
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        if let Some(control) = self.control.as_mut() {
            control.bind_mem_tracker(Arc::clone(&tracker));
        }
        self.mem_tracker = Some(tracker);
    }
    fn bind_runtime_state(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) -> ExecutionResult<()> {
        let mut control = RuntimeKernelControl::new(state.error_state());
        control.bind_runtime_memory(state);
        if let Some(tracker) = &self.mem_tracker {
            control.bind_mem_tracker(Arc::clone(tracker));
        }
        if let Some(factory) = &self.construction {
            control.bind_construction_factory(Arc::clone(factory));
        }
        self.control = Some(control);
        Ok(())
    }
}
impl<S: DistinctSetSemantics> InputEncodingPolicy for DistinctSetSinkOperator<S> {}
impl<S: DistinctSetSemantics> SinkKernel for DistinctSetSinkOperator<S> {
    fn admission(
        &self,
        _: &mut StepContext<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let observable = self.state.controller().observable();
        let observed_generation = observable.generation();
        if self.state.controller().is_stage_ready(self.stage) {
            return Ok(None);
        }
        Ok(Some(AdmissionNext::Wait(WaitFact {
            reason: WaitReason::Dependency,
            observable,
            observed_generation,
            deadline: None,
        })))
    }
    fn consume(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        if !chunk.is_empty() {
            ctx.checkpoint()?;
            let control = self
                .control
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            control.checkpoint_execution()?;
            let owner = control.construction_owner(chunk.chunk_schema())?;
            let tracker = self
                .mem_tracker
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let result = if self.stage == 0 {
                self.state.ingest_build(
                    &chunk,
                    Arc::clone(tracker),
                    owner.as_ref(),
                    control,
                    ctx.value_conversions(),
                )
            } else {
                self.state.ingest_probe(
                    self.stage,
                    &chunk,
                    Arc::clone(tracker),
                    owner.as_ref(),
                    control,
                    ctx.value_conversions(),
                )
            };
            result.map_err(|failure| control.project_required_failure(failure))?;
        }
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
    fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
        self.state.finish_stage(self.stage)?;
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}
