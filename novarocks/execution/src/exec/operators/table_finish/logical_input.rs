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
use novarocks_result_contract::{PerKind, SummaryKind, SummaryValue};
use std::sync::Mutex;

pub(crate) struct LogicalTableFinishFactory {
    core: TableFinishOperatorFactory,
    writer: UnionAllSharedState,
    summaries: Mutex<Option<PerKind<SourceBoundSummaryInput>>>,
}
impl TableFinishOperatorFactory {
    pub(crate) fn with_logical_input(
        self,
        writer: UnionAllSharedState,
        summaries: PerKind<SourceBoundSummaryInput>,
    ) -> LogicalTableFinishFactory {
        LogicalTableFinishFactory {
            core: self,
            writer,
            summaries: Mutex::new(Some(summaries)),
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
        let summaries = if dop == 1 && driver_id == 0 {
            self.summaries
                .lock()
                .expect("logical finish binding lock")
                .take()
        } else {
            None
        };
        if summaries.is_none() {
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
            summaries: summaries.unwrap_or_else(PerKind::empty).map(|_, input| {
                FinishSummaryInput {
                    input,
                    wait: None,
                    end: false,
                    coverage: None,
                }
            }),
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
struct FinishSummaryInput {
    input: SourceBoundSummaryInput,
    wait: Option<WaitFact>,
    end: bool,
    coverage: Option<SummaryCoverage>,
}
impl FinishSummaryInput {
    fn ready(&self) -> bool {
        !self.end && (self.wait.as_ref().is_none_or(WaitFact::is_ready) || self.input.ready())
    }
}
// Design: ADR-0218 (docs/adr/ADR-0218-typed-task-summaries-and-per-kind-completion.md)
struct LogicalFinish {
    core: TableFinishOperator,
    core_next: NextStep,
    writer: ChildStep,
    writer_state: UnionAllSharedState,
    writer_pending: Option<RawInput>,
    writer_admission: Option<AdmissionNext>,
    writer_end: bool,
    receiver: ReceiverSlot<HydrateInput>,
    summaries: PerKind<FinishSummaryInput>,
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
                self.cursor, self.writer_end, self.summaries.iter().all(|(_, input)| input.end), self.writer_pending.is_some(), self.receiver.is_occupied(), self.core_next, self.summaries.iter().map(|(kind, input)| (kind, &input.wait)).collect::<Vec<_>>()
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
        self.summaries.iter().any(|(_, input)| input.ready())
    }
    fn can_finalize(&self) -> bool {
        self.writer_end
            && self.summaries.iter().all(|(_, input)| input.end)
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
    fn poll_children(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<()> {
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
    fn service_writer(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<()> {
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
    fn service_summary(
        &mut self,
        kind: SummaryKind,
        ctx: &mut DriverStep<'_>,
    ) -> ExecutionResult<()> {
        let input = self
            .summaries
            .get_mut(kind)
            .ok_or(ConstructionFailure::MissingOwner)?;
        match input.input.poll(ctx)? {
            SummaryRead::Summary(summary) => {
                input.wait = None;
                input
                    .coverage
                    .as_mut()
                    .ok_or(ConstructionFailure::MissingOwner)?
                    .accept(summary)?;
            }
            SummaryRead::Pending(wait) => input.wait = Some(wait),
            SummaryRead::End => {
                input.end = true;
                input.wait = None;
            }
        }
        Ok(())
    }
    fn service_core(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<Option<Chunk>> {
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
            for (_, input) in self.summaries.iter() {
                match input
                    .coverage
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?
                    .finish()?
                {
                    SummaryValue::WriteCounts(counts) => self.core.logical_rows = Some(counts),
                    SummaryValue::DeleteVerification(counts) => {
                        self.core.delete_verification = Some(counts)
                    }
                }
            }
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
            .chain(
                self.summaries
                    .iter()
                    .filter_map(|(_, input)| input.wait.as_ref().and_then(|w| w.deadline)),
            )
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
    fn poll_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        self.poll_children(ctx)?;
        Ok(
            if self.writer_ready() || self.summary_ready() || self.core_ready() {
                ControlStep::Runnable
            } else {
                ControlStep::Active
            },
        )
    }
    fn poll_output(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        ctx.checkpoint()?;
        let generation = self.observable.generation();
        self.poll_children(ctx)?;
        let mut output = None;
        let services = self.summaries.iter().count() + 2;
        for offset in 0..services {
            let service = (self.cursor + offset) % services;
            let kind = service
                .checked_sub(1)
                .and_then(|index| self.summaries.iter().nth(index).map(|(kind, _)| kind));
            let ready = if service == 0 {
                self.writer_ready()
            } else if let Some(kind) = kind {
                self.summaries
                    .get(kind)
                    .is_some_and(FinishSummaryInput::ready)
            } else {
                self.core_ready()
            };
            if !ready {
                continue;
            }
            // Do not spend a parent coordination unit before giving a child
            // its quantum. A one-unit turn can execute one actual child.
            if ctx.work().remaining() == 0 {
                return Ok(StepResult::new(None, NextStep::Yield, 0));
            }
            self.cursor = (service + 1) % services;
            #[cfg(novarocks_dev_controls)]
            self.emit_diagnostic("logical_finish_service_enter", Some(service));
            if service == 0 {
                self.service_writer(ctx)?;
            } else if let Some(kind) = kind {
                if !ctx.work().try_consume(1) {
                    return Ok(StepResult::new(None, NextStep::Yield, 0));
                }
                self.service_summary(kind, ctx)?;
            } else {
                output = self.service_core(ctx)?;
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
            for (_, input) in self.summaries.iter_mut() {
                input.input.bind_diagnostics(self.diagnostics.clone());
            }
        }
        self.writer_state.bind_runtime(state)?;
        self.writer.bind_runtime_state(state)?;
        self.core.bind_runtime_state(state)
    }
    fn set_task_event_sink(&mut self, sink: Arc<dyn crate::runtime::task::io::TaskEventSink>) {
        self.writer.set_task_event_sink(sink.clone());
        self.core.set_task_event_sink(sink);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        for (_, input) in self.summaries.iter_mut() {
            input.input.prepare(
                self.construction
                    .as_deref()
                    .ok_or(ConstructionFailure::MissingOwner)?,
                self.tracker
                    .clone()
                    .ok_or(ConstructionFailure::MissingOwner)?,
            )?;
            input.coverage = Some(SummaryCoverage::new(
                input.input.sources()?,
                self.tracker
                    .clone()
                    .ok_or(ConstructionFailure::MissingOwner)?,
            )?);
            let target = Arc::downgrade(&self.observable);
            self.subscriptions
                .push(input.input.observable().subscribe(Arc::new(move || {
                    if let Some(target) = target.upgrade() {
                        target.notify_observers();
                    }
                })));
        }
        let target = Arc::downgrade(&self.observable);
        self.subscriptions
            .push(self.core.source_observable.subscribe(Arc::new(move || {
                if let Some(target) = target.upgrade() {
                    target.notify_observers();
                }
            })));
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
