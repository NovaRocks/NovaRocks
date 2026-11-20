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
    OperatorPorts, SetOpDeclaration, contract,
    frame::{Phase, SetOpFrame, is_cancellation},
    installed_dop, installed_slot, wait,
};
use crate::{
    exec::{
        hash_table::key_index::BulkWork,
        pipeline::{
            driver_step::{DriverStep, TurnWork},
            operator::{
                ExitFailure, ExitObservation, FinishWatch, InputConstructionFactory, Operator,
                OperatorLifecycle,
            },
            operator_factory::OperatorFactory,
            shape::{ControlStep, Source, SourceKernel},
            step::{NextStep, StepResult},
        },
        shared_stage::{
            key_bitmap::{BitmapFreeze, BitmapInitClaim, InitObligation, InitProgress},
            key_build_worker::{KeyBuildWorker, KeyWorkerStep},
            stage_completion::RemainingDemand,
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
use std::sync::Arc;

pub(crate) struct SetOpFinalizeFactory {
    name: String,
    declaration: Arc<SetOpDeclaration>,
    error: Arc<RuntimeErrorState>,
}

impl SetOpFinalizeFactory {
    pub(crate) fn new(declaration: Arc<SetOpDeclaration>, error: Arc<RuntimeErrorState>) -> Self {
        Self {
            name: format!("COMPILED_SET_FINALIZE (node={})", declaration.node.index()),
            declaration,
            error,
        }
    }
}
impl OperatorFactory for SetOpFinalizeFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn is_source(&self) -> bool {
        true
    }
    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        installed_dop(dop, self.declaration.participants.finalizers.get())
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let position = installed_slot(
            dop,
            driver_id,
            self.declaration.participants.finalizers.get(),
        );
        Box::new(Source::new(FinalizeSource {
            name: self.name.clone(),
            prepare_failure: position.err(),
            ports: OperatorPorts::new(self.declaration.clone(), self.error.clone()),
            worker: None,
            exit_wait: false,
            exit_failure: None,
            key_count: None,
        }))
    }
}

struct FinalizeSource {
    name: String,
    ports: OperatorPorts,
    prepare_failure: Option<ExecutionFailure>,
    worker: Option<FinalizeWorker>,
    exit_wait: bool,
    exit_failure: Option<ExecutionFailure>,
    key_count: Option<CounterRef>,
}

impl FinalizeSource {
    fn retain_failure(&self, cause: ExecutionFailure) -> ExecutionFailure {
        match self.ports.prepared() {
            Ok(prep) => prep.frame.fail(cause),
            Err(_) => cause,
        }
    }

    fn cleanup(&mut self) -> ExecutionResult<bool> {
        let Some(worker) = self.worker.as_mut() else {
            return Ok(true);
        };
        let prep = self.ports.prepared()?;
        if let Some(cause) = self.ports.error.error() {
            prep.frame.fail(cause);
        }
        if prep.frame.completion.check().is_err() || prep.frame.completion.published()?.is_some() {
            self.worker = None;
            return Ok(true);
        }
        if !worker.retire(&prep.frame)? {
            return Ok(false);
        }
        self.worker = None;
        Ok(true)
    }
}

impl OperatorLifecycle for FinalizeSource {
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
        self.key_count = Some(profiles.unique.add_unit_counter("SetOpBuildKeys"));
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
        self.ports.activate()?;
        let prepared = self.ports.prepared()?;
        let owner = self.ports.control.construction_owner(&prepared.schema)?;
        let work = self.ports.work(owner.as_ref())?;
        self.worker = Some(FinalizeWorker::new(&work));
        Ok(())
    }
    fn cancel(&mut self) {
        if let Some(cause) = self.ports.error.error() {
            self.retain_failure(cause);
        }
    }
    fn on_driver_failure(&mut self) {
        self.cancel();
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.exit_wait = !self.cleanup().map_err(|cause| self.retain_failure(cause))?;
        Ok(())
    }
    fn observe_exit(&mut self) -> ExitObservation {
        if self.exit_wait {
            match self.cleanup() {
                Ok(exited) => self.exit_wait = !exited,
                Err(cause) => {
                    self.exit_failure = Some(self.retain_failure(cause));
                    self.worker = None;
                    self.exit_wait = false;
                }
            }
        }
        let mut observation = if self.exit_wait {
            ExitObservation::pending(FinishWatch::Notify(
                self.ports
                    .observable()
                    .expect("pending original set operation frame"),
            ))
        } else {
            ExitObservation::exited()
        };
        observation.failure = self.exit_failure.take().map(ExitFailure::Required);
        observation
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
}

impl SourceKernel for FinalizeSource {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        self.ports.observable()
    }
    fn poll_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        let result = (|| {
            ctx.checkpoint()?;
            let completion = &self.ports.prepared()?.frame.completion;
            Ok(
                if completion.remaining_demand()? == RemainingDemand::Released
                    || completion.published()?.is_some()
                {
                    ControlStep::Runnable
                } else {
                    ControlStep::Active
                },
            )
        })();
        result.map_err(|cause| {
            self.ports
                .report(ctx, cause, |cause| self.retain_failure(cause))
        })
    }

    fn poll_output(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        let result = (|| {
            let prepared = self.ports.prepared()?;
            let owner = self.ports.control.construction_owner(&prepared.schema)?;
            let work = self.ports.work(owner.as_ref())?;
            let worker = self
                .worker
                .as_mut()
                .ok_or_else(|| contract("set operation finalizer has no original worker"))?;
            let (next, keys) = worker.step(&prepared.frame, ctx.work(), &work)?;
            if let (Some(count), Some(counter)) = (keys, &self.key_count) {
                counter.set(i64::try_from(count).unwrap_or(i64::MAX));
            }
            Ok(StepResult::new(None, next, 0))
        })();
        result.map_err(|cause| {
            self.ports
                .report(ctx, cause, |cause| self.retain_failure(cause))
        })
    }
}

pub(super) struct FinalizeWorker {
    keys: KeyBuildWorker,
    keys_finished: bool,
    bitmap: Option<InitObligation>,
}

impl FinalizeWorker {
    pub(super) fn new(work: &BulkWork<'_>) -> Self {
        Self {
            keys: KeyBuildWorker::new(work, contract),
            keys_finished: false,
            bitmap: None,
        }
    }

    #[cfg(test)]
    pub(super) fn worked_on_keys(&self) -> bool {
        self.keys.key_work_facts().1 > 0
    }

    #[cfg(test)]
    pub(super) fn settled_key_build(&self) -> bool {
        self.keys_finished
    }

    pub(super) fn retire(&mut self, frame: &SetOpFrame) -> ExecutionResult<bool> {
        if !frame.retire()? {
            return Ok(false);
        }
        if let Some(mut obligation) = self.bitmap.take() {
            obligation.retire()?;
        }
        self.keys.release_retired();
        Ok(true)
    }

    pub(super) fn step(
        &mut self,
        frame: &SetOpFrame,
        turn: &mut TurnWork,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<(NextStep, Option<usize>)> {
        let observable = frame.completion.observable();
        let generation = observable.generation();
        let result = self.step_active(frame, turn, work);
        match result {
            Err(cause)
                if is_cancellation(&cause)
                    && matches!(
                        frame.observe(self.keys_finished)?,
                        Phase::Retiring | Phase::Retired
                    ) =>
            {
                let next = if self.retire(frame)? {
                    NextStep::Finished
                } else {
                    wait(observable, generation)
                };
                Ok((next, None))
            }
            Ok((None, count)) => Ok((wait(observable, generation), count)),
            Ok((Some(next), count)) => Ok((next, count)),
            Err(cause) => Err(cause),
        }
    }

    fn step_active(
        &mut self,
        frame: &SetOpFrame,
        turn: &mut TurnWork,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<(Option<NextStep>, Option<usize>)> {
        if frame.completion.remaining_demand()? == RemainingDemand::Released {
            return Ok((self.retire(frame)?.then_some(NextStep::Finished), None));
        }
        let changed = frame.completion.observable();
        match frame.observe(self.keys_finished)? {
            Phase::Published => Ok((Some(NextStep::Finished), None)),
            Phase::Retiring | Phase::Retired => {
                Ok((self.retire(frame)?.then_some(NextStep::Finished), None))
            }
            Phase::Failed(cause) => Err(cause),
            Phase::Collecting => {
                if !turn.try_consume(1) {
                    return Ok((Some(NextStep::Yield), None));
                }
                Ok((frame.begin_keys(work)?.then_some(NextStep::Runnable), None))
            }
            Phase::WaitingForKeyWorkers => Ok((None, None)),
            Phase::Keys { graph, .. } => {
                let next = match self.keys.step(&graph, &changed, turn, work)? {
                    KeyWorkerStep::Finished => {
                        drop(graph);
                        self.keys_finished = true;
                        frame.finish_keys(work)?.then_some(NextStep::Runnable)
                    }
                    KeyWorkerStep::Progressed => Some(NextStep::Runnable),
                    KeyWorkerStep::Blocked => None,
                    KeyWorkerStep::Yielded => Some(NextStep::Yield),
                };
                Ok((next, None))
            }
            Phase::Bitmap(bitmap) => {
                if !turn.try_consume(1) {
                    return Ok((Some(NextStep::Yield), None));
                }
                if self.bitmap.is_none() {
                    match bitmap.init.claim(work.owner, work.control)? {
                        BitmapInitClaim::Range(lease) => self.bitmap = Some(lease),
                        BitmapInitClaim::Pending => return Ok((None, None)),
                        BitmapInitClaim::Retired => return Ok((Some(NextStep::Runnable), None)),
                        BitmapInitClaim::Finished => {
                            return match bitmap.init.try_freeze()? {
                                BitmapFreeze::Ready(bits) => {
                                    Ok((Some(NextStep::Runnable), frame.publish(bits, work)?))
                                }
                                BitmapFreeze::Pending | BitmapFreeze::Frozen => Ok((None, None)),
                                BitmapFreeze::Retired => Ok((Some(NextStep::Runnable), None)),
                            };
                        }
                    }
                }
                let allowance = turn.remaining();
                if allowance == 0 {
                    return Ok((Some(NextStep::Yield), None));
                }
                if !turn.try_consume(allowance) {
                    return Err(contract(
                        "set operation bitmap lost its installed turn work",
                    ));
                }
                let lease = self
                    .bitmap
                    .as_mut()
                    .expect("original bitmap init obligation");
                let next = match lease.initialize(allowance as usize, work.control)? {
                    InitProgress::Initialized => {
                        lease.settle()?;
                        self.bitmap = None;
                        NextStep::Runnable
                    }
                    InitProgress::Retired => {
                        lease.retire()?;
                        self.bitmap = None;
                        NextStep::Runnable
                    }
                    InitProgress::Progressed => NextStep::Runnable,
                    InitProgress::Yielded => NextStep::Yield,
                };
                Ok((Some(next), None))
            }
        }
    }
}
