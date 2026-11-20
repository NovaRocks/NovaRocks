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

use super::{
    OperatorPorts, SetOpDeclaration, contract, frame::capture_work, installed_dop, installed_slot,
    wait_fact,
};
use crate::{
    exec::{
        hash_table::{
            key_index::{BulkMatch, CaptureBuilder, EvaluatedPage, KeyInput, PreparedKeyBatch},
            partition_keys::source_key_owner,
        },
        pipeline::{
            driver_step::DriverStep,
            input::{PreparedInput, RawInput},
            operator::{
                InputConstructionFactory, InputEncodingPolicy, Operator, OperatorLifecycle,
            },
            operator_factory::OperatorFactory,
            shape::{ControlStep, SinkKernel, SinkStep, Transform},
            step::{AdmissionNext, NextStep},
        },
        shared_stage::{funded_vec, stage_completion::RemainingDemand},
    },
    runtime::{
        mem_tracker::MemTracker,
        observable::Observable,
        profile::{CounterRef, OperatorProfiles},
        runtime_state::{RuntimeErrorState, RuntimeState},
        task::{ExecutionFailure, ExecutionResult},
    },
};
use arrow::datatypes::DataType;
use novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK;
use novarocks_value_arrays::{
    ByteLayout, ValueFailure, decode::ValueConversionContext, reachability::ReachableRows,
};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum Role {
    Build,
    Probe { lane: usize, offset: usize },
}

pub(crate) struct SetOpInputFactory {
    name: String,
    declaration: Arc<SetOpDeclaration>,
    error: Arc<RuntimeErrorState>,
    role: Role,
    dop: usize,
}

impl SetOpInputFactory {
    pub(crate) fn build(declaration: Arc<SetOpDeclaration>, error: Arc<RuntimeErrorState>) -> Self {
        Self {
            name: format!("COMPILED_SET_BUILD (node={})", declaration.node.index()),
            dop: declaration.participants.inputs,
            declaration,
            error,
            role: Role::Build,
        }
    }

    pub(crate) fn probe(
        declaration: Arc<SetOpDeclaration>,
        error: Arc<RuntimeErrorState>,
        lane: usize,
        offset: usize,
        dop: usize,
    ) -> Self {
        Self {
            name: format!(
                "COMPILED_SET_PROBE (node={}, input={})",
                declaration.node.index(),
                lane + 1
            ),
            declaration,
            error,
            role: Role::Probe { lane, offset },
            dop,
        }
    }
}

impl OperatorFactory for SetOpInputFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn is_sink(&self) -> bool {
        true
    }
    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        installed_dop(dop, self.dop)
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let slot = installed_slot(dop, driver_id, self.dop).and_then(|local| match self.role {
            Role::Build => Ok(local),
            Role::Probe { offset, .. } => offset
                .checked_add(local)
                .filter(|&slot| slot < self.declaration.participants.probes)
                .ok_or_else(|| contract("set operation probe position is outside its roster")),
        });
        Box::new(Transform::hydrating(InputSink {
            name: self.name.clone(),
            ports: OperatorPorts::new(self.declaration.clone(), self.error.clone()),
            role: self.role,
            slot,
            capture: None,
            conversions: ValueConversionContext::default(),
            finished: false,
            exited: false,
            hits: None,
        }))
    }
}

struct InputSink {
    name: String,
    ports: OperatorPorts,
    role: Role,
    slot: ExecutionResult<usize>,
    capture: Option<CaptureBuilder>,
    conversions: ValueConversionContext,
    finished: bool,
    exited: bool,
    hits: Option<CounterRef>,
}

impl InputSink {
    fn retain_failure(&self, cause: ExecutionFailure) -> ExecutionFailure {
        let Ok(prep) = self.ports.prepared() else {
            return cause;
        };
        let first = match (self.role, &self.slot) {
            (Role::Build, Ok(slot)) => prep.frame.inputs.fail_input(*slot, cause),
            (Role::Probe { .. }, Ok(slot)) => prep.frame.completion.fail_probe(*slot, cause),
            _ => cause,
        };
        prep.frame.fail(first)
    }

    fn report(&self, ctx: &DriverStep<'_>, cause: ExecutionFailure) -> ExecutionFailure {
        self.ports
            .report(ctx, cause, |cause| self.retain_failure(cause))
    }

    fn release(&mut self) {
        self.capture = None;
        self.conversions.clear();
    }

    fn exit(&mut self) -> ExecutionResult<()> {
        if self.exited {
            return Ok(());
        }
        let result = (|| {
            let Ok(prep) = self.ports.prepared() else {
                return Ok(());
            };
            if let Some(cause) = self.ports.error.error() {
                self.retain_failure(cause);
            }
            if self.finished || prep.frame.completion.check().is_err() {
                return Ok(());
            }
            let slot = self.slot.clone()?;
            match self.role {
                Role::Build => {
                    if prep.frame.completion.remaining_demand()? == RemainingDemand::Needed {
                        return Err(self.retain_failure(contract(
                            "set operation build exited before sealing required input",
                        )));
                    }
                    prep.frame.inputs.withdraw_input_if_pending(slot)?;
                }
                Role::Probe { .. } => {
                    prep.frame.completion.withdraw_if_pending(slot)?;
                }
            }
            Ok(())
        })();
        self.release();
        self.exited = true;
        result
    }
}

impl OperatorLifecycle for InputSink {
    fn name(&self) -> &str {
        &self.name
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.ports.set_tracker(tracker);
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.ports.set_factory(factory);
    }
    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        if matches!(self.role, Role::Probe { .. }) {
            self.hits = Some(profiles.unique.add_unit_counter("SetOpProbeHits"));
        }
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.ports.bind_runtime(state);
        Ok(())
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        self.slot.clone()?;
        self.ports.prepare()
    }
    fn activate(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
        let result = (|| {
            self.ports.activate()?;
            if matches!(self.role, Role::Build) {
                let prep = self.ports.prepared()?;
                let owner = self.ports.control.construction_owner(&prep.schema)?;
                let work = self.ports.work(owner.as_ref())?;
                self.capture = Some(prep.frame.domain.begin_capture(&capture_work(&work))?);
            }
            Ok(())
        })();
        result.map_err(|cause| self.retain_failure(cause))
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.exit()
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.exit()
    }
    fn cancel(&mut self) {
        if let Some(cause) = self.ports.error.error() {
            self.retain_failure(cause);
        }
    }
    fn on_driver_failure(&mut self) {
        self.cancel();
    }
}

impl InputEncodingPolicy for InputSink {
    fn retains_encoded_column(&self, _: novarocks_types::SlotId, carrier: &DataType) -> bool {
        matches!(carrier, DataType::Dictionary(_, values)
            if ByteLayout::from_data_type(values).is_some())
    }
}

impl SinkKernel for InputSink {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        self.ports.observable()
    }

    fn poll_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        let result = (|| {
            let completion = &self.ports.prepared()?.frame.completion;
            if let Some(cause) = self.ports.error.error() {
                return Err(cause);
            }
            Ok(
                if !self.finished && completion.remaining_demand()? == RemainingDemand::Released {
                    ControlStep::Withdrawn
                } else {
                    ControlStep::Active
                },
            )
        })();
        result.map_err(|cause| self.report(ctx, cause))
    }

    fn admission(
        &self,
        ctx: &mut DriverStep<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let result = (|| {
            if matches!(self.role, Role::Build) {
                return Ok(None);
            }
            let completion = &self.ports.prepared()?.frame.completion;
            let observable = completion.observable();
            let generation = observable.generation();
            if completion.published()?.is_some() {
                Ok(None)
            } else {
                Ok(Some(AdmissionNext::Wait(wait_fact(observable, generation))))
            }
        })();
        result.map_err(|cause| self.report(ctx, cause))
    }

    fn consume(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let result = (|| {
            ctx.checkpoint()?;
            let chunk = input.into_uncovered_chunk()?;
            let prep = self.ports.prepared()?;
            let expected = prep.schema.slots();
            let actual = chunk.chunk_schema().slots();
            if !chunk.chunk_schema().is_typed() || actual.len() != expected.len() {
                return Err(contract(
                    "set operation input has the wrong compiled layout",
                ));
            }
            for (actual, expected) in actual.iter().zip(expected) {
                ctx.checkpoint()?;
                if actual.slot_id() != expected.slot_id()
                    || actual.typed_binding().value_type() != expected.typed_binding().value_type()
                {
                    return Err(contract("set operation input lost its normalized channel"));
                }
            }
            let rows = chunk.len();
            if rows == 0 || prep.frame.completion.remaining_demand()? == RemainingDemand::Released {
                return Ok(SinkStep::new(NextStep::NeedInput, 0));
            }
            let owner = self.ports.control.construction_owner(&prep.schema)?;
            let source = source_key_owner(&chunk, owner.as_ref())?;
            let work = self.ports.work(source.as_ref())?;
            let checkpoint = || work.control.checkpoint(0).map_err(ExecutionFailure::from);
            let readable = ReachableRows::all(rows)
                .materialize_observed(
                    &mut |layout| work.owner.request(layout).map_err(ExecutionFailure::from),
                    &mut |_| checkpoint(),
                )
                .map_err(|cause| match cause {
                    ValueFailure::Value(cause) => ExecutionFailure::from_domain(cause),
                    ValueFailure::Caller(cause) => cause,
                })?;
            let mut occurrences = funded_vec(work.owner, rows)?;
            for row in 0..rows {
                if row.is_multiple_of(MAX_UNOBSERVED_COMPILE_WORK as usize) {
                    checkpoint()?;
                }
                occurrences.push(row);
            }
            let spec = prep.frame.domain.spec();
            match self.role {
                Role::Build => {
                    self.capture
                        .as_mut()
                        .ok_or_else(|| contract("set operation build has no original capture"))?
                        .push_evaluated(
                            EvaluatedPage {
                                arrays: chunk.columns(),
                                bindings: spec.types(),
                                readable: &readable,
                                occurrences: &occurrences,
                            },
                            &mut self.conversions,
                            &capture_work(&work),
                        )?;
                }
                Role::Probe { lane, .. } => {
                    let published = prep.frame.completion.published()?.ok_or_else(|| {
                        contract("set operation probe accepted rows before publication")
                    })?;
                    let input = KeyInput::new_for_spec(
                        spec,
                        chunk.columns(),
                        spec.types(),
                        &readable,
                        &occurrences,
                        &mut self.conversions,
                        work.owner,
                        &checkpoint,
                    )?;
                    let prepared = PreparedKeyBatch::from_captured(
                        spec,
                        &input,
                        work.allocator.clone(),
                        &checkpoint,
                    )?;
                    let found = published.artifact.keys.view.probe(
                        &prepared,
                        None,
                        work.owner,
                        work.control,
                    )?;
                    let mut hits = 0;
                    for (row, found) in found.matches().iter().enumerate() {
                        if row.is_multiple_of(MAX_UNOBSERVED_COMPILE_WORK as usize) {
                            checkpoint()?;
                        }
                        match *found {
                            BulkMatch::Found(key) => {
                                published.artifact.mark(lane, key)?;
                                hits += 1;
                            }
                            BulkMatch::Miss => {}
                            BulkMatch::Unmatchable => {
                                return Err(contract(
                                    "set operation NULL-equal key was unmatchable",
                                ));
                            }
                        }
                    }
                    if let Some(counter) = &self.hits {
                        counter.add(hits);
                    }
                }
            }
            Ok(SinkStep::new(NextStep::NeedInput, rows as u64))
        })();
        result.map_err(|cause| self.report(ctx, cause))
    }

    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<SinkStep> {
        let result = (|| {
            let prep = self.ports.prepared()?;
            let completion = &prep.frame.completion;
            if completion.remaining_demand()? == RemainingDemand::Released {
                return Ok(SinkStep::new(NextStep::Finished, 0));
            }
            let slot = self.slot.clone()?;
            match self.role {
                Role::Build => {
                    let owner = self.ports.control.construction_owner(&prep.schema)?;
                    let work = self.ports.work(owner.as_ref())?;
                    let capture = self.capture.as_mut().ok_or_else(|| {
                        contract("set operation build EOS has no original capture")
                    })?;
                    prep.frame
                        .inputs
                        .normal_input(slot, capture.seal(&capture_work(&work))?)?;
                }
                Role::Probe { .. } => {
                    // Probe batches require publication at admission. An empty
                    // driver can reach EOS earlier, and T checks both facts.
                    completion.normal_after_output(slot)?;
                }
            }
            self.finished = true;
            self.release();
            Ok(SinkStep::new(NextStep::Finished, 0))
        })();
        result.map_err(|cause| self.report(ctx, cause))
    }
}
