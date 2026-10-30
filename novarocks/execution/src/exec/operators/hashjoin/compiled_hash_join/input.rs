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

//! One installed E input. A hash input evaluates its original roots once per
//! page and captures in the shared original domains. NL appends payload only.
//! Both seal their own physical pages on real input EOS.
//! No index/CSR construction runs on this input's finish continuation.

use super::super::{
    build_capture::{EvaluatedJoinKeyPage, EvaluatedKeyProjection, InputRunWriter},
    build_finalize::JoinBuildFrame,
    build_pages::B3RunId,
    build_state::build_contract,
    join_completion::{RemainingDemand, funded_vec},
};
use super::frame::CompiledJoinFrame;
use super::*;
use crate::exec::{
    operators::compiled_expression::evaluate_chunk_all,
    pipeline::{
        operator::{ExitFailure, ExitObservation, OperatorLifecycle},
        shape::{ControlStep, SinkKernel, SinkStep},
    },
};
use novarocks_functions::KernelEvaluationControl;

pub(crate) struct CompiledJoinInputSinkFactory {
    name: String,
    declaration: Arc<CompiledJoinFrame>,
    error: Arc<RuntimeErrorState>,
    producers: Option<Arc<NativeRuntimeFilterProducerFactory>>,
}
impl CompiledJoinInputSinkFactory {
    pub(crate) fn new(
        declaration: Arc<CompiledJoinFrame>,
        error: Arc<RuntimeErrorState>,
        producers: Option<Arc<NativeRuntimeFilterProducerFactory>>,
    ) -> Self {
        Self {
            name: format!("COMPILED_JOIN_INPUT (node={})", declaration.node().index()),
            declaration,
            error,
            producers,
        }
    }
}
impl OperatorFactory for CompiledJoinInputSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn is_sink(&self) -> bool {
        true
    }
    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        if usize::try_from(dop).ok() != Some(self.declaration.participants.inputs) {
            return Err(build_contract(
                "join input pipeline differs from its actual installed E",
            ));
        }
        Ok(())
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let slot = usize::try_from(driver_id)
            .ok()
            .filter(|&slot| slot < self.declaration.participants.inputs);
        let mut invalid = self.bind_pipeline_dop(dop).err().or_else(|| {
            slot.is_none()
                .then(|| build_contract("join input driver has no installed E slot"))
        });
        let producers = self.producers.as_ref().and_then(|factory| {
            match factory.create_for_driver(dop, driver_id) {
                Ok(set) => Some(set),
                Err(cause) => {
                    invalid.get_or_insert(cause);
                    None
                }
            }
        });
        Box::new(Transform::hydrating(JoinInputSink {
            name: self.name.clone(),
            declaration: Arc::clone(&self.declaration),
            frame: None,
            slot,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            error: Arc::clone(&self.error),
            construction: None,
            schema: None,
            tracker: None,
            prepare_failure: invalid,
            writer: None,
            values: None,
            scopes: None,
            scope_sites: Vec::new(),
            scope_bindings: Vec::new(),
            conversions: ValueConversionContext::default(),
            producers,
            producer_count: self
                .producers
                .as_ref()
                .map(|factory| factory.local_partition_count()),
            normal: false,
            exited: false,
            exit_failure: None,
            _prepared_storage: None,
        }))
    }
}
struct JoinInputSink {
    name: String,
    declaration: Arc<CompiledJoinFrame>,
    frame: Option<Arc<JoinBuildFrame>>,
    slot: Option<usize>,
    control: RuntimeKernelControl,
    error: Arc<RuntimeErrorState>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    schema: Option<ChunkSchemaRef>,
    tracker: Option<Arc<MemTracker>>,
    prepare_failure: Option<ExecutionFailure>,
    writer: Option<InputRunWriter>,
    values: Option<Vec<CompiledExpressionInstance>>,
    scopes: Option<Vec<CompiledExpressionInstance>>,
    scope_sites: Vec<ProgramExpressionRootSite>,
    scope_bindings: Vec<FunctionValueType>,
    conversions: ValueConversionContext,
    producers: Option<NativeRuntimeFilterProducerSet>,
    producer_count: Option<u32>,
    normal: bool,
    exited: bool,
    exit_failure: Option<ExecutionFailure>,
    _prepared_storage: Option<ValueTypeStorageLifetime>,
}
impl JoinInputSink {
    fn validate_payload(&self, chunk: &Chunk) -> ExecutionResult<()> {
        use novarocks_functions::{
            EvaluatedArgument, Selection, validate_evaluated_argument_observed,
        };
        use novarocks_local_program::{ProgramChannelLayoutRole, ProgramChannelSite};
        use novarocks_type_contract::arrow_fields_exact_observed;
        let actual = chunk.record_batch().schema();
        let frozen = self.declaration.build_layout().schema();
        self.control.checkpoint(0)?;
        if actual.metadata() != frozen.metadata() || actual.fields().len() != frozen.fields().len()
        {
            return Err(build_contract(
                "nested-loop payload differs from its frozen build layout",
            ));
        }
        for (actual, frozen) in actual.fields().iter().zip(frozen.fields()) {
            if !arrow_fields_exact_observed(actual, frozen, || self.control.checkpoint(1))? {
                return Err(build_contract(
                    "nested-loop payload field differs from its frozen declaration",
                ));
            }
        }
        for (column, array) in chunk.columns().iter().enumerate() {
            let ordinal = u32::try_from(column).map_err(|_| {
                build_contract("join build channel exceeds its frozen u32 representation")
            })?;
            let binding = self
                .declaration
                .program
                .checked()
                .channels()
                .channel_type(ProgramChannelSite::Layout {
                    node: self.declaration.node(),
                    role: ProgramChannelLayoutRole::JoinRight,
                    ordinal,
                })
                .ok_or_else(|| {
                    build_contract("nested-loop payload has no original signed channel")
                })?;
            validate_evaluated_argument_observed(
                EvaluatedArgument::Column(array),
                Selection::all(chunk.len()),
                binding,
                &self.control,
            )?;
        }
        Ok(())
    }
    fn frame(&self) -> ExecutionResult<&Arc<JoinBuildFrame>> {
        if let Some(cause) = &self.prepare_failure {
            return Err(cause.clone());
        }
        self.frame
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }
    fn fail(&self, cause: ExecutionFailure) -> ExecutionFailure {
        let cause = self.control.project_required_failure(cause);
        self.frame.as_ref().map_or(cause.clone(), |frame| {
            self.slot.map_or_else(
                || frame.fail(cause.clone()),
                |slot| frame.fail_input(slot, cause.clone()),
            )
        })
    }
    fn end_owned(&mut self) -> ExecutionResult<()> {
        if self.exited {
            return Ok(());
        }
        let mut reason = RuntimeFilterProducerFailure::UpstreamUnavailable;
        let disposition: ExecutionResult<()> = (|| {
            if let Some(frame) = &self.frame {
                if self.error.is_stopped() {
                    self.fail(
                        self.error
                            .error()
                            .unwrap_or_else(|| KernelFailure::Cancelled.into()),
                    );
                    reason = RuntimeFilterProducerFailure::ExecutionFailed;
                }
                if !self.normal && frame.completion.check().is_ok() {
                    if frame.completion.remaining_demand()? != RemainingDemand::Released {
                        let cause = self.fail(build_contract(
                            "join input exited before its normal sealed EOS",
                        ));
                        self.exit_failure.get_or_insert(cause);
                        reason = RuntimeFilterProducerFailure::ExecutionFailed;
                    } else {
                        frame
                            .inputs
                            .withdraw_input_if_pending(self.slot.ok_or_else(|| {
                                build_contract("join input exit has no installed E slot")
                            })?)?;
                    }
                }
            }
            Ok(())
        })();
        // Always destroy actual partial data, including when disposition
        // itself failed. An exit diagnostic is never proof of this destruction.
        self.writer = None;
        self.values = None;
        self.scopes = None;
        self.conversions.clear();
        let rf = self
            .producers
            .as_mut()
            .map_or(Ok(()), |set| set.fail(reason));
        self.exited = true;
        match (disposition, rf) {
            (Err(first), Err(secondary)) => {
                self.error.report_cleanup(&secondary);
                Err(first)
            }
            (first, second) => first.and(second),
        }
    }
}
impl OperatorLifecycle for JoinInputSink {
    fn name(&self) -> &str {
        &self.name
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        self.tracker = Some(tracker);
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(Arc::clone(&factory));
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(cause) = &self.prepare_failure {
            return Err(cause.clone());
        }
        if self.frame.is_some() {
            return Ok(());
        }
        let result: ExecutionResult<()> = (|| {
            let schema = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?
                .create_compiled_chunk_schema(self.declaration.build_layout())?;
            let owner = self.control.construction_owner(&schema)?;
            let tracker = self
                .tracker
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let work = BulkWork {
                owner: owner.as_ref(),
                allocator: AggregateAllocator::new(Arc::clone(tracker)),
                host: self
                    .control
                    .allocator()
                    .ok_or(ConstructionFailure::MissingOwner)?,
                control: &self.control,
            };
            let frame = self.declaration.prepare(&work)?;
            if self.declaration.payload_only() {
                if self.producers.is_some() {
                    return Err(build_contract(
                        "nested-loop payload input cannot publish a keyed runtime filter",
                    ));
                }
                self.schema = Some(schema);
                self.frame = Some(frame);
                return Ok(());
            }
            let ProgramNodeKind::Join { condition, .. } =
                self.declaration.program.graph().nodes()[self.declaration.node().index()].kind()
            else {
                return Err(build_contract("join input has no original condition"));
            };
            let mut sites = funded_vec(owner.as_ref(), condition.scope_keys().len())?;
            let mut bindings = funded_vec(owner.as_ref(), condition.scope_keys().len())?;
            for ordinal in 0..condition.scope_keys().len() {
                let key = u32::try_from(ordinal).map_err(|_| {
                    build_contract("join scope root ordinal exceeds its frozen u32 representation")
                })?;
                let site = ProgramExpressionRootSite::Node {
                    node: self.declaration.node(),
                    role: ProgramNodeExpressionRole::NaaScopeBuildKey { key },
                };
                bindings.push(root_value_type(&self.declaration.program, site)?);
                sites.push(site);
            }
            self._prepared_storage = Some(owner.storage_lifetime());
            self.scope_sites = sites;
            self.scope_bindings = bindings;
            self.schema = Some(schema);
            self.frame = Some(frame);
            Ok(())
        })();
        if let Err(cause) = &result {
            self.prepare_failure = Some(self.control.project_required_failure(cause.clone()));
        }
        result.map_err(|cause| self.control.project_required_failure(cause))
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn activate(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
        let frame = self.frame()?;
        let schema = self
            .schema
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = self.control.construction_owner(schema)?;
        let tracker = self
            .tracker
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let work = BulkWork {
            owner: owner.as_ref(),
            allocator: AggregateAllocator::new(Arc::clone(tracker)),
            host: self
                .control
                .allocator()
                .ok_or(ConstructionFailure::MissingOwner)?,
            control: &self.control,
        };
        frame.activate(&work).map_err(|cause| frame.fail(cause))?;
        if let (Some(producers), Some(count)) = (&mut self.producers, self.producer_count) {
            producers.bind(count)?;
        }
        Ok(())
    }
    fn cancel(&mut self) {
        if self.error.is_stopped()
            && let Some(frame) = &self.frame
        {
            frame.fail(
                self.error
                    .error()
                    .unwrap_or_else(|| KernelFailure::Cancelled.into()),
            );
        }
    }
    fn on_driver_failure(&mut self) {
        self.cancel();
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.end_owned()
    }
    fn observe_exit(&mut self) -> ExitObservation {
        let mut observation = ExitObservation::exited();
        observation.failure = self.exit_failure.take().map(ExitFailure::Required);
        observation
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.end_owned()
    }
}
impl InputEncodingPolicy for JoinInputSink {}
impl SinkKernel for JoinInputSink {
    fn admission(
        &self,
        _: &mut StepContext<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        if let Some(cause) = &self.prepare_failure {
            return Err(cause.clone());
        }
        Ok(None)
    }
    fn control_observable(&self) -> Option<Arc<crate::runtime::observable::Observable>> {
        self.frame
            .as_ref()
            .map(|frame| frame.completion.observable())
    }
    fn poll_control(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        let frame = self.frame()?;
        frame.completion.check()?;
        if frame.completion.remaining_demand()? == RemainingDemand::Released {
            Ok(ControlStep::Withdrawn)
        } else {
            Ok(ControlStep::Active)
        }
    }
    fn consume(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len())
            .map_err(|_| build_contract("join input row count exceeds its original u64 counter"))?;
        let result: ExecutionResult<()> = (|| {
            if self.normal {
                return Err(build_contract("join input accepted data after normal EOS"));
            }
            let frame = Arc::clone(self.frame()?);
            frame.completion.check()?;
            let schema = self
                .schema
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let owner = self.control.construction_owner(schema)?;
            let tracker = self
                .tracker
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let host = self
                .control
                .allocator()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let work = BulkWork {
                owner: owner.as_ref(),
                allocator: AggregateAllocator::new(Arc::clone(tracker)),
                host,
                control: &self.control,
            };
            if self.declaration.payload_only() {
                self.validate_payload(&chunk)?;
                if self.writer.is_none() {
                    self.writer = Some(InputRunWriter::new(
                        &frame.domains,
                        B3RunId::from_input_slot(
                            self.slot.ok_or_else(|| {
                                build_contract("join input has no installed E slot")
                            })?,
                            self.declaration.participants.inputs,
                        )?,
                        self.declaration.requires_payload(),
                        &work,
                    )?);
                }
                self.writer
                    .as_mut()
                    .expect("actual payload E writer")
                    .push_payload(&chunk, &work)?;
                return Ok(());
            }
            let plan = self.declaration.hash_plan()?;
            instances(
                &mut self.values,
                &self.declaration.program,
                &plan.build_sites,
                &self.control,
            )?;
            let mut values = funded_vec(owner.as_ref(), plan.build_sites.len())?;
            for (instance, site) in self
                .values
                .as_mut()
                .expect("actual value instances")
                .iter_mut()
                .zip(&plan.build_sites)
            {
                values.push(evaluate_chunk_all(instance, *site, &chunk, &self.control)?);
            }
            instances(
                &mut self.scopes,
                &self.declaration.program,
                &self.scope_sites,
                &self.control,
            )?;
            let mut scopes = funded_vec(owner.as_ref(), self.scope_sites.len())?;
            for (instance, site) in self
                .scopes
                .as_mut()
                .expect("actual scope instances")
                .iter_mut()
                .zip(&self.scope_sites)
            {
                scopes.push(evaluate_chunk_all(instance, *site, &chunk, &self.control)?);
            }
            if self.writer.is_none() {
                self.writer = Some(InputRunWriter::new(
                    &frame.domains,
                    B3RunId::from_input_slot(
                        self.slot
                            .ok_or_else(|| build_contract("join input has no installed E slot"))?,
                        self.declaration.participants.inputs,
                    )?,
                    self.declaration.requires_payload(),
                    &work,
                )?);
            }
            // Capture construction performs source continuation exactly once.
            // Do not wrap this work in a second source_key_owner.
            let values_projection = EvaluatedKeyProjection {
                arrays: &values,
                bindings: &plan.build_key_bindings,
            };
            let page = if matches!(
                &frame.domains,
                super::super::build_state::ProjectionDomains::Ordinary { .. }
            ) {
                EvaluatedJoinKeyPage::Ordinary(values_projection)
            } else {
                EvaluatedJoinKeyPage::Naa {
                    scope: EvaluatedKeyProjection {
                        arrays: &scopes,
                        bindings: &self.scope_bindings,
                    },
                    values: values_projection,
                }
            };
            self.writer
                .as_mut()
                .expect("actual E writer")
                .push_evaluated(&chunk, page, &mut self.conversions, &work)?;
            self.conversions.clear();
            if let Some(producers) = &mut self.producers {
                producers.submit(&values)?;
            }
            Ok(())
        })();
        if let Err(cause) = result {
            let cause = self.fail(cause);
            ctx.report_failure(cause.clone());
            return Err(cause);
        }
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
        if let Some(cause) = &self.prepare_failure {
            return Err(cause.clone());
        }
        if self.normal {
            return Ok(SinkStep::new(NextStep::Finished, 0));
        }
        if !ctx.work().try_consume(1) {
            return Ok(SinkStep::new(NextStep::Yield, 0));
        }
        let result: ExecutionResult<()> = (|| {
            let frame = Arc::clone(self.frame()?);
            let schema = self
                .schema
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let owner = self.control.construction_owner(schema)?;
            let tracker = self
                .tracker
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let work = BulkWork {
                owner: owner.as_ref(),
                allocator: AggregateAllocator::new(Arc::clone(tracker)),
                host: self
                    .control
                    .allocator()
                    .ok_or(ConstructionFailure::MissingOwner)?,
                control: &self.control,
            };
            if self.writer.is_none() {
                self.writer = Some(InputRunWriter::new(
                    &frame.domains,
                    B3RunId::from_input_slot(
                        self.slot.ok_or_else(|| {
                            build_contract("join input finish has no installed E slot")
                        })?,
                        self.declaration.participants.inputs,
                    )?,
                    self.declaration.requires_payload(),
                    &work,
                )?);
            }
            let receipt = self.writer.as_mut().expect("actual E writer").seal(&work)?;
            if let Some(producers) = &mut self.producers {
                producers.finish()?;
            }
            frame.inputs.normal_input(
                self.slot
                    .ok_or_else(|| build_contract("join input finish has no installed E slot"))?,
                receipt,
            )?;
            self.writer = None;
            self.normal = true;
            Ok(())
        })();
        if let Err(cause) = result {
            let cause = self.fail(cause);
            ctx.report_failure(cause.clone());
            return Err(cause);
        }
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}
