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
    OperatorPorts, SetOpDeclaration, artifact::Artifact, contract, installed_dop, installed_slot,
    wait,
};
use crate::{
    exec::{
        chunk::Chunk,
        pipeline::{
            driver_step::DriverStep,
            operator::{
                ConstructionFailure, InputConstructionFactory, Operator, OperatorLifecycle,
            },
            operator_factory::OperatorFactory,
            shape::{ControlStep, Source, SourceKernel},
            step::{NextStep, StepResult},
        },
        shared_stage::{
            funded_vec,
            stage_completion::RemainingDemand,
            tail_ranges::{TailClaim, TailObligation},
        },
    },
    runtime::{
        mem_tracker::MemTracker,
        observable::Observable,
        profile::{CounterRef, OperatorProfiles},
        runtime_state::{RuntimeErrorState, RuntimeState},
        task::{ExecutionFailure, ExecutionResult},
    },
};
use std::{num::NonZeroUsize, sync::Arc};

// Cooperative selection/export granularity shared with the join tail's batches.
pub(super) const TAIL_WORK_ROWS: NonZeroUsize = NonZeroUsize::new(512).unwrap();

pub(crate) struct SetOpTailFactory {
    name: String,
    declaration: Arc<SetOpDeclaration>,
    error: Arc<RuntimeErrorState>,
}

impl SetOpTailFactory {
    pub(crate) fn new(declaration: Arc<SetOpDeclaration>, error: Arc<RuntimeErrorState>) -> Self {
        Self {
            name: format!("COMPILED_SET_TAIL (node={})", declaration.node.index()),
            declaration,
            error,
        }
    }
}
impl OperatorFactory for SetOpTailFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn is_source(&self) -> bool {
        true
    }
    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        installed_dop(dop, self.declaration.participants.tails)
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let position = installed_slot(dop, driver_id, self.declaration.participants.tails);
        Box::new(Source::new(TailSource {
            name: self.name.clone(),
            prepare_failure: position.err(),
            ports: OperatorPorts::new(self.declaration.clone(), self.error.clone()),
            range: None,
            position: 0,
            done: false,
            emitted: None,
        }))
    }
}

struct TailSource {
    name: String,
    ports: OperatorPorts,
    prepare_failure: Option<ExecutionFailure>,
    range: Option<TailObligation<Artifact>>,
    position: usize,
    done: bool,
    emitted: Option<CounterRef>,
}

impl TailSource {
    fn fail(&mut self, cause: ExecutionFailure) -> ExecutionFailure {
        let cause = if let Ok(prep) = self.ports.prepared() {
            prep.frame.fail(cause)
        } else {
            cause
        };
        if let Some(mut range) = self.range.take() {
            range.fail(cause.clone());
        }
        cause
    }
    fn report(&mut self, ctx: &DriverStep<'_>, cause: ExecutionFailure) -> ExecutionFailure {
        let cause = self.fail(ctx.locate(self.ports.control.project_required_failure(cause)));
        ctx.report_failure(cause.clone());
        cause
    }
    fn withdraw(&mut self) -> ExecutionResult<()> {
        if let Some(range) = self.range.as_mut() {
            range.withdraw_after_demand_released()?;
        }
        self.range = None;
        self.done = true;
        Ok(())
    }
    fn exit(&mut self) -> ExecutionResult<()> {
        if let Some(cause) = self.ports.error.error() {
            self.fail(cause);
        }
        if let Some(range) = self.range.as_ref() {
            return match range.check() {
                Ok(RemainingDemand::Released) => self.withdraw(),
                Ok(RemainingDemand::Needed) => Err(self.fail(contract(
                    "set operation tail exited before transferring a required key range",
                ))),
                Err(cause) => {
                    self.fail(cause);
                    Ok(())
                }
            };
        }
        Ok(())
    }
}

impl OperatorLifecycle for TailSource {
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
        self.emitted = Some(profiles.unique.add_unit_counter("SetOpOutputKeys"));
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.ports.bind_runtime(state);
        Ok(())
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(cause) = &self.prepare_failure {
            return Err(cause.clone());
        }
        self.ports.prepare()
    }
    fn activate(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
        self.ports.activate()
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.exit()
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.exit()
    }
    fn cancel(&mut self) {
        if let Some(cause) = self.ports.error.error() {
            self.fail(cause);
        }
    }
    fn on_driver_failure(&mut self) {
        self.cancel();
    }
}

impl SourceKernel for TailSource {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        self.ports.observable()
    }
    fn poll_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        let result = (|| {
            ctx.checkpoint()?;
            let completion = &self.ports.prepared()?.frame.completion;
            Ok(
                if completion.remaining_demand()? == RemainingDemand::Released {
                    ControlStep::Runnable
                } else {
                    ControlStep::Active
                },
            )
        })();
        result.map_err(|cause| self.report(ctx, cause))
    }

    fn poll_output(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        let result = self.output(ctx);
        result.map_err(|cause| self.report(ctx, cause))
    }
}

impl TailSource {
    fn output(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if self.done {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        let prepared = self.ports.prepared()?;
        let completion = &prepared.frame.completion;
        let observable = completion.observable();
        let generation = observable.generation();
        if completion.remaining_demand()? == RemainingDemand::Released {
            self.withdraw()?;
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        if let Some(range) = self.range.as_mut()
            && self.position == range.rows().end
        {
            // Re-entry proves the preceding page was accepted by the Handoff.
            range.complete_after_output()?;
            self.range = None;
        }
        let owner = self.ports.control.construction_owner(&prepared.schema)?;
        let work = self.ports.work(owner.as_ref())?;
        if self.range.is_none() {
            match prepared.frame.claim_tail(&work)? {
                TailClaim::Pending => {
                    return Ok(StepResult::new(None, wait(observable, generation), 0));
                }
                TailClaim::Unavailable => {
                    return Err(contract(
                        "set operation tail is missing a required normal probe contribution",
                    ));
                }
                TailClaim::Withdrawn | TailClaim::Finished => {
                    self.done = true;
                    return Ok(StepResult::new(None, NextStep::Finished, 0));
                }
                TailClaim::Range(range) => {
                    self.position = range.rows().start;
                    self.range = Some(range);
                }
            }
        }
        let range = self
            .range
            .as_ref()
            .expect("original set operation tail obligation");
        let data = range.artifact();
        let allowance = ctx.work().remaining().min(
            novarocks_spi::connector::ENGINE_OUTPUT_POLICY
                .max_rows
                .get(),
        ) as usize;
        if allowance == 0 {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let end = self
            .position
            .saturating_add(allowance)
            .min(range.rows().end);
        let (partition, base, ids) = data.keys.partition_prefix(self.position..end)?;
        let count = ids.len();
        if !ctx.work().try_consume(count as u32) {
            return Err(contract("set operation tail lost its installed turn work"));
        }
        let columns = data.keys.view.export_partition_range(
            partition,
            ids,
            |id| data.survives(base + id, work.control),
            data.keys.view.spec().types(),
            &work,
        )?;
        self.position += count;
        let rows = columns
            .arrays()
            .first()
            .ok_or_else(|| contract("set operation output has no comparison columns"))?
            .len();
        if rows == 0 {
            return Ok(StepResult::new(None, NextStep::Runnable, 0));
        }
        let mut output = funded_vec(owner.as_ref(), columns.arrays().len())?;
        output.extend_from_slice(columns.arrays());
        let chunk = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            prepared.schema.clone(),
            output,
            rows,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )?;
        if let Some(counter) = &self.emitted {
            counter.add(i64::try_from(rows).unwrap_or(i64::MAX));
        }
        Ok(StepResult::new(
            Some(chunk),
            NextStep::Runnable,
            rows as u64,
        ))
    }
}
