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

//! A real source owns both input continuations. No side-polling in a unary
//! Transform can prevent the independent writer source from being pulled.
use super::*;
use crate::exec::operators::UnionAllSourceFactory;
use crate::exec::operators::logical_change_summary::{
    SourceBoundSummaryInput, SummaryCoverage, SummaryFailure, SummaryRead, summary_failure,
};
use crate::exec::operators::setop::union_all_shared::UnionAllSharedState;
use crate::exec::pipeline::operator::ConstructionFailure;
use crate::exec::pipeline::{
    input::{RawInput, derived_raw_input},
    shape::{ReceiverPreparation, ReceiverSlot},
    step::InputResult,
};
use crate::runtime::observable::ObserverSubscription;
use std::sync::Mutex;

pub(crate) struct LogicalTableFinishFactory {
    core: TableFinishOperatorFactory,
    writer: UnionAllSharedState,
    summary: Mutex<Option<SourceBoundSummaryInput>>,
}
impl TableFinishOperatorFactory {
    pub(crate) fn with_logical_input(
        self,
        writer: UnionAllSharedState,
        summary: SourceBoundSummaryInput,
    ) -> LogicalTableFinishFactory {
        LogicalTableFinishFactory {
            core: self,
            writer,
            summary: Mutex::new(Some(summary)),
        }
    }
}
impl OperatorFactory for LogicalTableFinishFactory {
    fn name(&self) -> &str {
        &self.core.name
    }
    fn is_source(&self) -> bool {
        true
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let mut failure = None;
        let summary = if dop == 1 && driver_id == 0 {
            self.summary
                .lock()
                .expect("logical finish binding lock")
                .take()
        } else {
            None
        };
        if summary.is_none() {
            failure = Some(finish_contract(
                FinishContractSite::Binding,
                "logical finish requires exactly one owner of both inputs",
            ));
        }
        let observable = Arc::new(Observable::new());
        let writer = UnionAllSourceFactory::new(self.writer.clone(), -1).create(1, 0);
        Box::new(Source::new(LogicalFinish {
            core: self.core.create_kernel(1, 0),
            core_next: NextStep::Runnable,
            writer: ChildStep::new(writer, observable.clone()),
            writer_state: self.writer.clone(),
            writer_pending: None,
            writer_admission: None,
            writer_end: false,
            receiver: ReceiverSlot::new(HydrateInput::default()),
            summary,
            summary_wait: None,
            summary_end: false,
            coverage: None,
            observable,
            subscriptions: Vec::new(),
            cursor: 0,
            construction: None,
            tracker: None,
            failure,
            #[cfg(novarocks_dev_controls)]
            diagnostics: None,
        }))
    }
}
struct LogicalFinish {
    core: TableFinishOperator,
    core_next: NextStep,
    writer: ChildStep,
    writer_state: UnionAllSharedState,
    writer_pending: Option<RawInput>,
    writer_admission: Option<AdmissionNext>,
    writer_end: bool,
    receiver: ReceiverSlot<HydrateInput>,
    summary: Option<SourceBoundSummaryInput>,
    summary_wait: Option<WaitFact>,
    summary_end: bool,
    coverage: Option<SummaryCoverage>,
    observable: Arc<Observable>,
    subscriptions: Vec<ObserverSubscription>,
    cursor: usize,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    tracker: Option<Arc<MemTracker>>,
    failure: Option<ExecutionFailure>,
    #[cfg(novarocks_dev_controls)]
    diagnostics: Option<crate::runtime::writer_diagnostics::WriterDiagnostics>,
}
fn runnable(next: &NextStep) -> bool {
    match next {
        NextStep::Runnable | NextStep::Yield => true,
        NextStep::Wait(wait) => wait.is_ready(),
        _ => false,
    }
}
impl LogicalFinish {
    #[cfg(novarocks_dev_controls)]
    fn emit_diagnostic(&self, event: &str, service: Option<usize>) {
        if let Some(scope) = &self.diagnostics {
            scope.emit(event, || format!(
                "service={service:?} cursor={} writer_end={} summary_end={} writer_pending={} receiver_occupied={} core_next={:?} summary_wait={:?}",
                self.cursor, self.writer_end, self.summary_end, self.writer_pending.is_some(), self.receiver.is_occupied(), self.core_next, self.summary_wait
            ));
        }
    }
    fn writer_ready(&self) -> bool {
        if self.writer_end {
            return false;
        }
        if self.writer.is_complete() && self.writer_pending.is_none() {
            return true;
        }
        !self.receiver.is_occupied()
            && matches!(self.core_next, NextStep::NeedInput)
            && self
                .writer_admission
                .as_ref()
                .is_none_or(|a| !matches!(a,AdmissionNext::Wait(w) if !w.is_ready()))
            && (self.writer_pending.is_some() || self.writer.runnable())
    }
    fn summary_ready(&self) -> bool {
        !self.summary_end
            && (self.summary_wait.as_ref().is_none_or(WaitFact::is_ready)
                || self
                    .summary
                    .as_ref()
                    .is_some_and(SourceBoundSummaryInput::ready))
    }
    fn can_finalize(&self) -> bool {
        self.writer_end
            && self.summary_end
            && !self.receiver.is_occupied()
            && self.writer_pending.is_none()
            && matches!(self.core_next, NextStep::NeedInput)
    }
    fn core_ready(&self) -> bool {
        (self.receiver.is_occupied()
            && !matches!(&self.core_next, NextStep::Wait(wait) if !wait.is_ready()))
            || runnable(&self.core_next)
            || self.can_finalize()
    }
    fn poll_children(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<()> {
        ctx.checkpoint()?;
        if matches!(self.writer.poll_control(ctx)?, ControlStep::Withdrawn)
            && !self.writer_state.normal_input_exhausted()
        {
            return Err(summary_failure(SummaryFailure::WithdrawnWriter));
        }
        if matches!(
            TransformKernel::poll_control(&mut self.core, ctx)?,
            ControlStep::Runnable
        ) {
            self.core_next = NextStep::Runnable;
        }
        Ok(())
    }
    fn service_writer(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<()> {
        if self.writer_pending.is_none() && !self.writer.is_complete() {
            self.writer_pending = self.writer.advance(ctx)?.map(derived_raw_input);
        }
        if let Some(input) = self.writer_pending.take() {
            match self.receiver.try_accept(ctx, input, &self.core) {
                InputResult::Accepted(result) => {
                    result?;
                    self.writer_admission = None;
                }
                InputResult::Deferred { input, next } => {
                    self.writer_pending = Some(input);
                    self.writer_admission = Some(next);
                }
                InputResult::Rejected { input, failure } => {
                    self.writer_pending = Some(input);
                    return Err(failure);
                }
            }
        }
        if self.writer.is_complete() && self.writer_pending.is_none() {
            if !self.writer_state.normal_input_exhausted() {
                return Err(summary_failure(SummaryFailure::WithdrawnWriter));
            }
            self.writer_end = true;
        }
        Ok(())
    }
    fn service_summary(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<()> {
        match self
            .summary
            .as_mut()
            .ok_or(ConstructionFailure::MissingOwner)?
            .poll(ctx)?
        {
            SummaryRead::Summary(summary) => {
                self.summary_wait = None;
                self.coverage
                    .as_mut()
                    .ok_or(ConstructionFailure::MissingOwner)?
                    .accept(summary)?;
            }
            SummaryRead::Pending(wait) => self.summary_wait = Some(wait),
            SummaryRead::End => {
                self.summary_end = true;
                self.summary_wait = None;
            }
        }
        Ok(())
    }
    fn service_core(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<Option<Chunk>> {
        let step = if self.receiver.is_occupied() {
            match self.receiver.prepare(ctx, &self.core)? {
                ReceiverPreparation::Ready(input) => {
                    TransformKernel::apply(&mut self.core, ctx, input)?
                }
                ReceiverPreparation::Pending(next) => {
                    self.core_next = next;
                    return Ok(None);
                }
                ReceiverPreparation::Empty => return Ok(None),
            }
        } else if self.can_finalize() {
            ctx.checkpoint()?;
            self.core.logical_rows = Some(
                self.coverage
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?
                    .finish()?,
            );
            TransformKernel::finish(&mut self.core, ctx)?
        } else {
            TransformKernel::drive(&mut self.core, ctx)?
        };
        self.core_next = step.next;
        Ok(step.output)
    }
    fn next(&self, generation: u64) -> NextStep {
        if matches!(self.core_next, NextStep::Finished) {
            return NextStep::Finished;
        }
        if self.writer_ready() || self.summary_ready() || self.core_ready() {
            return NextStep::Runnable;
        }
        let deadline = self
            .writer
            .wait()
            .and_then(|w| w.deadline)
            .into_iter()
            .chain(match &self.core_next {
                NextStep::Wait(w) => w.deadline,
                _ => None,
            })
            .chain(self.summary_wait.as_ref().and_then(|w| w.deadline))
            .chain(match &self.writer_admission {
                Some(AdmissionNext::Wait(w)) => w.deadline,
                _ => None,
            })
            .min_by_key(|d| d.at());
        NextStep::Wait(WaitFact {
            reason: WaitReason::Input,
            observable: self.observable.clone(),
            observed_generation: generation,
            deadline,
        })
    }
}
impl SourceKernel for LogicalFinish {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.observable.clone())
    }
    fn poll_control(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        self.poll_children(ctx)?;
        Ok(
            if self.writer_ready() || self.summary_ready() || self.core_ready() {
                ControlStep::Runnable
            } else {
                ControlStep::Active
            },
        )
    }
    fn poll_output(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        ctx.checkpoint()?;
        let generation = self.observable.generation();
        self.poll_children(ctx)?;
        let mut output = None;
        for offset in 0..3 {
            let service = (self.cursor + offset) % 3;
            let ready = match service {
                0 => self.writer_ready(),
                1 => self.summary_ready(),
                _ => self.core_ready(),
            };
            if !ready {
                continue;
            }
            // Do not spend a parent coordination unit before giving a child
            // its quantum. A one-unit turn can execute one actual child.
            if ctx.work().remaining() == 0 {
                return Ok(StepResult::new(None, NextStep::Yield, 0));
            }
            self.cursor = (service + 1) % 3;
            #[cfg(novarocks_dev_controls)]
            self.emit_diagnostic("logical_finish_service_enter", Some(service));
            match service {
                0 => self.service_writer(ctx)?,
                1 => {
                    if !ctx.work().try_consume(1) {
                        return Ok(StepResult::new(None, NextStep::Yield, 0));
                    }
                    self.service_summary(ctx)?;
                }
                _ => output = self.service_core(ctx)?,
            }
            #[cfg(novarocks_dev_controls)]
            self.emit_diagnostic("logical_finish_service_returned", Some(service));
            break;
        }
        let next = self.next(generation);
        Ok(StepResult::new(
            output,
            if ctx.work().remaining() == 0 && matches!(next, NextStep::Runnable) {
                NextStep::Yield
            } else {
                next
            },
            0,
        ))
    }
}
impl OperatorLifecycle for LogicalFinish {
    fn name(&self) -> &str {
        &self.core.name
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.receiver
            .preparation_mut()
            .install_tracker(tracker.clone());
        self.core
            .set_mem_tracker(MemTracker::new_child("FinishCore", &tracker));
        self.writer
            .set_mem_tracker(MemTracker::new_child("WriterResultsInput", &tracker));
        self.tracker = Some(tracker);
    }
    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        self.core.set_profiles(profiles.clone());
        self.writer.set_profiles(OperatorProfiles::new(
            profiles.operator.child("WriterResultsInput"),
        ));
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.receiver
            .preparation_mut()
            .install_factory(factory.clone());
        self.core.set_construction_factory(factory.clone());
        self.writer.set_construction_factory(factory.clone());
        self.construction = Some(factory);
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        #[cfg(novarocks_dev_controls)]
        {
            self.diagnostics =
                crate::runtime::writer_diagnostics::WriterDiagnostics::from_runtime(state);
            if let Some(summary) = &mut self.summary {
                summary.bind_diagnostics(self.diagnostics.clone());
            }
        }
        self.writer_state.bind_runtime(state)?;
        self.writer.bind_runtime_state(state)?;
        self.core.bind_runtime_state(state)
    }
    fn set_fragment_event_sink(
        &mut self,
        sink: Arc<dyn crate::runtime::fragment::io::FragmentEventSink>,
    ) {
        self.writer.set_fragment_event_sink(sink.clone());
        self.core.set_fragment_event_sink(sink);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let summary = self
            .summary
            .as_mut()
            .ok_or(ConstructionFailure::MissingOwner)?;
        summary.prepare(
            self.construction
                .as_deref()
                .ok_or(ConstructionFailure::MissingOwner)?,
            self.tracker
                .clone()
                .ok_or(ConstructionFailure::MissingOwner)?,
        )?;
        self.coverage = Some(SummaryCoverage::new(
            summary.sources()?,
            self.tracker
                .clone()
                .ok_or(ConstructionFailure::MissingOwner)?,
        )?);
        for source in [summary.observable(), self.core.source_observable.clone()] {
            let target = Arc::downgrade(&self.observable);
            self.subscriptions.push(source.subscribe(Arc::new(move || {
                if let Some(target) = target.upgrade() {
                    target.notify_observers();
                }
            })));
        }
        self.core.prepare()?;
        self.writer.prepare()
    }
    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.core.activate(state)?;
        self.writer.activate(state)
    }
    fn cancel(&mut self) {
        self.writer.cancel();
        self.core.cancel();
        self.receiver.discard();
        self.writer_pending = None;
    }
    fn on_driver_failure(&mut self) {
        self.cancel();
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        let writer = self.writer.begin_exit();
        let core = self.core.begin_exit();
        self.receiver.discard();
        self.writer_pending = None;
        writer.and(core)
    }
    fn visit_exits(
        &mut self,
        visit: &mut dyn FnMut(crate::exec::pipeline::operator::ExitObservation),
    ) {
        self.writer.visit_exits(visit);
        self.core.visit_exits(visit);
    }
    fn close(&mut self) -> ExecutionResult<()> {
        let writer = self.writer.close();
        let core = self.core.close();
        writer.and(core)
    }
}
