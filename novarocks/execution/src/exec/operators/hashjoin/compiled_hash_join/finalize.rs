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

//! Driver-polled F source. It emits no rows and ends at the original NoopSink.
//! Dependencies return the original generation-bearing WaitFact; CPU exhaustion
//! yields under the same driver turn, retaining exact K/CSR/bitmap cursors.

use super::super::{
    build_finalize::{FinalizeStep, FinalizeWorker, JoinBuildFrame},
    build_state::build_contract,
};
use super::frame::CompiledJoinFrame;
use super::*;
use crate::exec::pipeline::{
    operator::{ExitFailure, ExitObservation, FinishWatch, OperatorLifecycle},
    shape::{ControlStep, Source, SourceKernel},
};
use novarocks_functions::KernelEvaluationControl;

pub(crate) struct CompiledJoinFinalizeSourceFactory {
    name: String,
    frame: Arc<CompiledJoinFrame>,
    error: Arc<RuntimeErrorState>,
}
impl CompiledJoinFinalizeSourceFactory {
    pub(crate) fn new(frame: Arc<CompiledJoinFrame>, error: Arc<RuntimeErrorState>) -> Self {
        Self {
            name: format!("COMPILED_JOIN_FINALIZE (node={})", frame.node().index()),
            frame,
            error,
        }
    }
}
impl OperatorFactory for CompiledJoinFinalizeSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn is_source(&self) -> bool {
        true
    }
    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        if usize::try_from(dop).ok() != Some(self.frame.participants.finalizers.get()) {
            return Err(build_contract(
                "join finalizer pipeline differs from its actual installed F",
            ));
        }
        Ok(())
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let invalid = self.bind_pipeline_dop(dop).err().or_else(|| {
            (!usize::try_from(driver_id)
                .is_ok_and(|id| id < self.frame.participants.finalizers.get()))
            .then(|| build_contract("join finalizer driver has no installed F slot"))
        });
        Box::new(Source::new(JoinFinalizeSource {
            name: self.name.clone(),
            declaration: Arc::clone(&self.frame),
            frame: None,
            error: Arc::clone(&self.error),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            construction: None,
            schema: None,
            tracker: None,
            worker: Some(FinalizeWorker::new()),
            prepare_failure: invalid,
            exit_wait: false,
            exit_failure: None,
            profiles: None,
            key_counters: None,
            reports_partitions: driver_id == 0,
            partitions_recorded: false,
        }))
    }
}
struct JoinFinalizeSource {
    name: String,
    declaration: Arc<CompiledJoinFrame>,
    frame: Option<Arc<JoinBuildFrame>>,
    error: Arc<RuntimeErrorState>,
    control: RuntimeKernelControl,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    schema: Option<ChunkSchemaRef>,
    tracker: Option<Arc<MemTracker>>,
    worker: Option<FinalizeWorker>,
    prepare_failure: Option<ExecutionFailure>,
    exit_wait: bool,
    exit_failure: Option<ExecutionFailure>,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    key_counters: Option<[crate::runtime::profile::CounterRef; 3]>,
    // Count the shared artifact once in the original additive task projection.
    // This diagnostic role grants no scheduling or construction authority.
    reports_partitions: bool,
    partitions_recorded: bool,
}
impl JoinFinalizeSource {
    fn record_key_work(&mut self) {
        if let (Some(worker), Some(counters)) = (&self.worker, &self.key_counters) {
            let (calls, progress) = worker.key_work_facts();
            counters[0].set(i64::try_from(calls).unwrap_or(i64::MAX));
            counters[1].set(i64::try_from(progress).unwrap_or(i64::MAX));
            counters[2].set(i64::from(progress != 0));
        }
        if self.partitions_recorded {
            return;
        }
        let (Some(profile), Some(frame)) = (&self.profiles, &self.frame) else {
            return;
        };
        let published = match frame.completion.published() {
            Ok(Some(data)) => data,
            _ => return,
        };
        let record_partition = |name, direct_name, partitions: usize, direct: bool| {
            profile.unique.add_info_string(name, partitions.to_string());
            if self.reports_partitions {
                profile
                    .unique
                    .add_unit_counter(name)
                    .set(i64::try_from(partitions).unwrap_or(i64::MAX));
                profile
                    .unique
                    .add_unit_counter(direct_name)
                    .set(i64::from(direct));
            }
        };
        use super::super::build_finalize::BuildLookup;
        match &published.artifact.lookup {
            BuildLookup::PayloadOnly => profile.unique.add_info_string("JoinLookup", "PayloadOnly"),
            BuildLookup::Ordinary(full) => {
                record_partition(
                    "JoinFullPartitions",
                    "JoinFullDirect",
                    full.view.partition_count(),
                    full.view.is_direct(),
                );
            }
            BuildLookup::Naa { group, full, .. } => {
                record_partition(
                    "JoinFullPartitions",
                    "JoinFullDirect",
                    full.view.partition_count(),
                    full.view.is_direct(),
                );
                if let Some(group) = group {
                    record_partition(
                        "JoinGroupPartitions",
                        "JoinGroupDirect",
                        group.view.partition_count(),
                        group.view.is_direct(),
                    );
                } else {
                    profile
                        .unique
                        .add_info_string("JoinGroupPartitions", "none");
                }
            }
        }
        self.partitions_recorded = true;
    }

    fn frame(&self) -> ExecutionResult<&Arc<JoinBuildFrame>> {
        if let Some(cause) = &self.prepare_failure {
            return Err(cause.clone());
        }
        self.frame
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }
    fn cleanup(&mut self) -> ExecutionResult<bool> {
        let Some(frame) = &self.frame else {
            self.worker = None;
            return Ok(true);
        };
        if self.error.is_stopped() {
            frame.fail(
                self.error
                    .error()
                    .unwrap_or_else(|| KernelFailure::Cancelled.into()),
            );
            self.worker = None;
            return Ok(true);
        }
        if frame.completion.check().is_err() {
            // Required work reported its original cause before this cleanup.
            // There is no asynchronous task behind these synchronous cursors.
            self.worker = None;
            return Ok(true);
        }
        let Some(worker) = &mut self.worker else {
            return Ok(true);
        };
        match worker.finish_exit(frame) {
            Ok(true) => {
                self.worker = None;
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(cause) => {
                let first = frame.fail(cause);
                self.worker = None;
                self.exit_failure.get_or_insert(first);
                Ok(true)
            }
        }
    }
}
impl OperatorLifecycle for JoinFinalizeSource {
    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.key_counters = Some([
            profiles.unique.add_unit_counter("JoinKAdvanceCalls"),
            profiles.unique.add_unit_counter("JoinKProgressSteps"),
            profiles.unique.add_unit_counter("JoinKProgressWorkers"),
        ]);
        self.profiles = Some(profiles);
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(Arc::clone(&factory));
        self.construction = Some(factory);
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        self.tracker = Some(tracker);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(cause) = &self.prepare_failure {
            return Err(cause.clone());
        }
        if self.frame.is_some() {
            return Ok(());
        }
        let result: ExecutionResult<()> = (|| {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let schema = factory.create_compiled_chunk_schema(self.declaration.build_layout())?;
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
            self.schema = Some(schema);
            self.frame = Some(frame);
            Ok(())
        })();
        if let Err(cause) = &result {
            self.prepare_failure = Some(cause.clone());
        }
        result
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
        frame.activate(&work).map_err(|cause| frame.fail(cause))
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
        self.exit_wait = !self.cleanup()?;
        Ok(())
    }
    fn observe_exit(&mut self) -> ExitObservation {
        if self.exit_wait {
            match self.cleanup() {
                Ok(true) => self.exit_wait = false,
                Ok(false) => {}
                Err(cause) => {
                    self.exit_failure.get_or_insert(cause);
                    self.worker = None;
                    self.exit_wait = false;
                }
            }
        }
        let mut observation = if self.exit_wait {
            ExitObservation::pending(FinishWatch::Notify(
                self.frame
                    .as_ref()
                    .expect("pending original frame")
                    .completion
                    .observable(),
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
impl SourceKernel for JoinFinalizeSource {
    fn control_observable(&self) -> Option<Arc<crate::runtime::observable::Observable>> {
        self.frame
            .as_ref()
            .map(|frame| frame.completion.observable())
    }
    fn poll_control(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        let frame = self.frame()?;
        self.control
            .checkpoint(0)
            .map_err(|cause| self.control.project_required_failure(cause.into()))?;
        frame.completion.check()?;
        if frame.completion.remaining_demand()?
            == super::super::join_completion::RemainingDemand::Released
            || frame.completion.published()?.is_some()
        {
            Ok(ControlStep::Runnable)
        } else {
            Ok(ControlStep::Active)
        }
    }
    fn poll_output(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        let frame = Arc::clone(self.frame()?);
        let observable = frame.completion.observable();
        let observed_generation = observable.generation();
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
        let result = self
            .worker
            .as_mut()
            .ok_or_else(|| build_contract("join finalizer polled after its real exit"))?
            .step(&frame, ctx.work(), &work);
        self.record_key_work();
        let step = match result {
            Ok(step) => step,
            Err(cause) => {
                let cause = self.control.project_required_failure(cause);
                ctx.report_failure(cause.clone());
                return Err(cause);
            }
        };
        let next = match step {
            FinalizeStep::Finished | FinalizeStep::Retired => NextStep::Finished,
            FinalizeStep::Progressed => NextStep::Runnable,
            FinalizeStep::Yielded => NextStep::Yield,
            FinalizeStep::Blocked if observable.generation() != observed_generation => {
                NextStep::Runnable
            }
            FinalizeStep::Blocked => NextStep::Wait(WaitFact {
                reason: WaitReason::Dependency,
                observable,
                observed_generation,
                deadline: None,
            }),
        };
        Ok(StepResult::new(None, next, 0))
    }
}
